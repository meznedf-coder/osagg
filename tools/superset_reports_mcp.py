"""Small MCP server for Superset scheduled reports (Superset 6.1's MCP service has none).

Tools: list_reports, create_report. A report e-mails a dashboard or a chart on a
schedule: PNG screenshot inline (or PDF / CSV attachment, or the chart data as an HTML
table), with a subject and an HTML description at the top of the e-mail. It uses
Superset's REST API (/api/v1/report/) with a service account; the screenshots are taken
by Superset's Celery worker (headless browser configured in superset_config.py).

Started by the agent over stdio (no network port):
    MCP_URL="http://127.0.0.1:5008/mcp,superset_reports_mcp.py" python superset_agent.py ...

Environment: SUPERSET_URL (default http://127.0.0.1:8088), SUPERSET_USER,
SUPERSET_PASSWORD.

CSV / TEXT reports read the chart's saved query context, which charts created through
Superset 6.1's MCP service do not have ("Chart has no query context saved"); create_report
then stores one built from the chart's parameters (the query the chart shows).
"""

from __future__ import annotations

import json
import os
from typing import Any, Literal

import httpx
from fastmcp import FastMCP
from pydantic import BaseModel, Field

BASE = os.environ.get("SUPERSET_URL", "http://127.0.0.1:8088").rstrip("/")
mcp = FastMCP("superset-reports")


def _session() -> httpx.Client:
    c = httpx.Client(base_url=BASE, timeout=60)
    r = c.post("/api/v1/security/login", json={
        "username": os.environ["SUPERSET_USER"], "password": os.environ["SUPERSET_PASSWORD"],
        "provider": "db", "refresh": True})
    r.raise_for_status()
    c.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
    r = c.get("/api/v1/security/csrf_token/")
    r.raise_for_status()
    c.headers["X-CSRFToken"] = r.json()["result"]
    c.headers["Referer"] = BASE
    return c


def _query_context(params: dict[str, Any], ds_id: int) -> dict[str, Any]:
    """The query context Superset's front end would save for a table / XY / big-number chart."""
    metrics = list(params.get("metrics") or ([params["metric"]] if params.get("metric") else []))
    grain = params.get("time_grain_sqla")
    if params.get("query_mode") == "raw":
        columns: list[Any] = list(params.get("all_columns") or [])
        metrics = []
    else:
        columns = list(params.get("groupby") or [])
        x = params.get("x_axis")
        if x and x not in columns:
            columns.insert(0, {"columnType": "BASE_AXIS", "sqlExpression": x, "label": x,
                               "expressionType": "SQL", "timeGrain": grain} if grain else x)
    filters, time_range = [], params.get("time_range") or "No filter"
    for f in params.get("adhoc_filters") or []:
        if f.get("expressionType") != "SIMPLE" or f.get("clause", "WHERE") != "WHERE":
            continue
        if f.get("operator") == "TEMPORAL_RANGE":
            time_range = f.get("comparator") or time_range
        else:
            filters.append({"col": f["subject"], "op": f["operator"], "val": f.get("comparator")})
    sort = params.get("timeseries_limit_metric")
    orderby = ([[sort, not params.get("order_desc", True)]] if sort
               else [] if params.get("x_axis") or not metrics      # time order
               else [[metrics[0], False]])                          # the table chart's default
    return {"datasource": {"id": ds_id, "type": "table"}, "force": False,
            "result_format": "json", "result_type": "full",
            "form_data": {**params, "datasource": f"{ds_id}__table"},
            "queries": [{"columns": columns, "metrics": metrics, "orderby": orderby,
                         "row_limit": params.get("row_limit") or 1000, "filters": filters,
                         "time_range": time_range,
                         "extras": {"time_grain_sqla": grain, "having": "", "where": ""}}]}


def _ensure_query_context(c: httpx.Client, chart_id: int) -> None:
    chart = c.get(f"/api/v1/chart/{chart_id}").json()["result"]
    if chart.get("query_context"):
        return
    params = json.loads(chart.get("params") or "{}")
    ds_id = int(str(params.get("datasource", "")).split("__")[0] or 0) or \
        c.get(f"/api/v1/chart/?q=(filters:!((col:id,opr:eq,value:{chart_id})),columns:!(datasource_id))"
              ).json()["result"][0]["datasource_id"]
    c.put(f"/api/v1/chart/{chart_id}", json={
        "query_context": json.dumps(_query_context(params, ds_id)),
        "query_context_generation": True}).raise_for_status()


class ReportRequest(BaseModel):
    name: str = Field(description="Report name (unique)")
    dashboard_id: int | None = Field(default=None, description="Dashboard to send (or chart_id)")
    chart_id: int | None = Field(default=None, description="Chart to send (or dashboard_id)")
    report_format: Literal["PNG", "PDF", "CSV", "TEXT"] = Field(
        default="PNG", description="PNG: screenshot in the e-mail body; PDF: attachment; CSV: chart "
                                   "data attached (charts only); TEXT: chart data as a table in the body")
    crontab: str = Field(default="0 8 * * 1-5", description="Schedule, cron syntax (Mon-Fri 08:00)")
    timezone: str = Field(default="Europe/Paris")
    email_recipients: list[str] = Field(description="E-mail addresses")
    email_subject: str | None = Field(default=None, description="E-mail subject (default: report name)")
    description_html: str | None = Field(
        default=None, description="Text at the top of the e-mail. Allowed HTML: p, b, strong, i, em, "
                                  "ul, ol, li, br, a, blockquote, code, div, table (no headings)")
    custom_width: int | None = Field(default=None, description="Screenshot width in px (e.g. 1600)")
    active: bool = True


@mcp.tool
def list_reports() -> list[dict]:
    """List scheduled reports and alerts: name, type, target, format, schedule, last state."""
    c = _session()
    try:
        res = c.get("/api/v1/report/?q=(page_size:100)").json()["result"]
        return [{"id": r["id"], "name": r["name"], "type": r["type"], "active": r["active"],
                 "crontab": r["crontab"], "timezone": r.get("timezone"),
                 "last_state": r.get("last_state"), "recipients": r.get("recipients")} for r in res]
    finally:
        c.close()


@mcp.tool
def create_report(request: ReportRequest) -> dict:
    """Schedule an e-mail report of a dashboard or a chart (PNG screenshot in the e-mail,
    PDF/CSV attachment or data table), with subject and HTML description."""
    if (request.dashboard_id is None) == (request.chart_id is None):
        return {"error": "give exactly one of dashboard_id or chart_id"}
    c = _session()
    try:
        me = c.get("/api/v1/me/").json()["result"]
        body = {
            "type": "Report", "name": request.name, "active": request.active,
            "crontab": request.crontab, "timezone": request.timezone,
            "report_format": request.report_format, "owners": [me["id"]],
            "description": request.description_html or "", "log_retention": 90,
            "working_timeout": 600,
            "recipients": [{"type": "Email", "recipient_config_json": {
                "target": ", ".join(request.email_recipients)}}],
        }
        if request.email_subject:
            body["email_subject"] = request.email_subject
        if request.custom_width:
            body["custom_width"] = request.custom_width
        if request.dashboard_id is not None:
            body["dashboard"] = request.dashboard_id
        else:
            body["chart"] = request.chart_id
            body["force_screenshot"] = request.report_format == "PNG"
            if request.report_format in ("CSV", "TEXT"):
                _ensure_query_context(c, request.chart_id)
        r = c.post("/api/v1/report/", json=body)
        if r.status_code >= 400:
            return {"error": r.status_code, "detail": r.text[:1000]}
        return {"id": r.json()["id"], "name": request.name, "schedule": request.crontab,
                "format": request.report_format, "list_url": f"{BASE}/report/list/"}
    finally:
        c.close()


if __name__ == "__main__":
    mcp.run()          # stdio
