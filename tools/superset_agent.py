"""Chat with Superset through MCP servers, using a local OpenAI-compatible LLM.

    python superset_agent.py "Which applications had the most failed jobs yesterday?"
    python superset_agent.py            # interactive

The LLM (e.g. llama.cpp `llama-server --jinja`) chooses the tools; this script
runs them on the MCP servers (Superset's `superset mcp run`, and optionally
superset_alerts_mcp.py) and loops until the model answers. It needs only the Superset
virtualenv (fastmcp + httpx).

Environment: LLM_URL (default http://127.0.0.1:8080/v1), LLM_MODEL (default: the
first model of the server), LLM_API_KEY (if the server wants one), MCP_URL (comma
separated: http URLs, or .py files started over stdio; default
http://127.0.0.1:5008/mcp), AGENT_TOOLS (comma-separated allowlist, "*" for all),
AGENT_THINKING (1: let the model reason before each step; default 0, much faster: with a
local model a reasoning step can generate 150-600 tokens at ~12 tokens/s).

Chart guard: Superset 6.1's MCP service logs why a chart config is invalid but answers only
"An error occurred", and saves one more chart at every accepted retry. Before a chart tool
call reaches it, this script validates the config with Superset's own schema (when run in
the Superset virtualenv) and against the dataset's columns and saved metrics, and returns
the exact error to the model. Before a save, the chart is previewed without saving (the
preview of a saved chart ignores its filters): a failing or empty chart is not saved and
the model is told why. A second save of the same chart name becomes update_chart. After a
save, date comparisons on the chart's time column become Superset's time range
("start : end") through the REST API (SUPERSET_URL / SUPERSET_USER / SUPERSET_PASSWORD):
the MCP service saves them as plain filters, which dashboards drop on the time column.
"""

from __future__ import annotations

import asyncio
import copy
import datetime as dt
import json
import os
import re
import sys
import time
from contextlib import AsyncExitStack
from typing import Any

import httpx
from fastmcp import Client

LLM_URL = os.environ.get("LLM_URL", "http://127.0.0.1:8080/v1").rstrip("/")
MCP_URLS = [u.strip() for u in os.environ.get(
    "MCP_URL", "http://127.0.0.1:5008/mcp,http://127.0.0.1:5009/mcp").split(",") if u.strip()]
DEFAULT_TOOLS = [
    # Superset's MCP service
    "list_datasets", "get_dataset_info", "execute_sql", "list_databases", "get_chart_type_schema",
    "generate_chart", "update_chart", "list_charts", "get_chart_info", "generate_dashboard",
    "add_chart_to_existing_dashboard", "list_dashboards", "get_dashboard_info",
    # superset_tools_mcp.py
    "describe_data", "export_excel", "chart_from_sql", "chart_image", "send_email", "list_reports",
    "create_report", "promql_query", "check_health", "list_alerts",
    # this script
    "show_chart",
]
TOOLS = os.environ.get("AGENT_TOOLS", ",".join(DEFAULT_TOOLS)).split(",")
MAX_STEPS = int(os.environ.get("AGENT_MAX_STEPS", "16"))
MAX_TOOL_CHARS = int(os.environ.get("AGENT_MAX_TOOL_CHARS", "8000"))
THINKING = os.environ.get("AGENT_THINKING", "0") == "1"
# a fixed "now" (replays and demos on a copy of old data), e.g. AGENT_NOW="2026-09-24 23:30"
AGENT_NOW = os.environ.get("AGENT_NOW", "").strip()


def _now() -> dt.datetime:
    return dt.datetime.fromisoformat(AGENT_NOW) if AGENT_NOW else dt.datetime.now()

SYSTEM = """You are a data assistant connected to Apache Superset through tools.
Answer with facts from the tools, never invent numbers or ids. Answer in the user's language.

How to work:
- Understand the data first: describe_data (topic = the words of the question) gives what
  each field means, its synonyms and typical values, how the indices relate (join fields)
  and the time range of the data. If the data ends before "now", say so and use its last
  days. Datasets and their ids: list_datasets / get_dataset_info.
- execute_sql runs SQL on a database id. The OpenSearch database uses osagg: DuckDB-style
  SQL, table = index name, field names are case sensitive and must be double-quoted
  ("APPLICATION", "@timestamp_date"). GROUP BY / aggregates run inside OpenSearch, so
  aggregate as much as possible and always add a LIMIT to row lists.
- Business dates: "POSITION_DATE" (yyyymmdd), "POSITION_LABEL" (D, D-1, W-1, Y-1...)
  and "POSITION_TIME" (execution time moved onto the D-1 position date) are columns.
  Filter labels with "POSITION_LABEL" IN ('D-1', 'W-1').
- Timestamps are local time (Europe/Paris); now is given below.
- To build charts: check the fields with get_chart_type_schema, then call generate_chart
  once with save_chart=true and the requested chart_name; a tool error tells you exactly
  what to fix. Use only the dataset's column names. COUNT(*) is the dataset's saved metric
  "count": {"name": "count", "saved_metric": true}. Ratios, percentiles and conditional
  counts cannot be written in a chart: use the dataset's saved metrics (get_dataset_info
  lists them with their description), or say it is not possible. To change a saved chart
  call update_chart with its id instead of creating another one.
- Chart filters are fixed values (no rolling time range): turn "last 7 days" into dates
  from "now". If a saved chart returns no rows, its filters exclude every document: fix
  them (a POSITION_LABEL filter and a date filter must not contradict each other).
- Charts cannot compare with an earlier period (no time comparison). To compare position
  dates, use X = POSITION_TIME and group by POSITION_LABEL, filtered on the labels.
- Metrics (Prometheus / Mimir) are tables of their own database (promagg): one table per
  metric; columns ts, one column per label, value, and for counters rate / increase.
  Always filter ts on a time range and GROUP BY a time bucket (DATE_TRUNC('hour', ts)).
  Counters: SUM(rate) = per second, SUM(increase) = count; gauges: AVG(value), MIN(value),
  MAX(value); histograms (*_bucket tables): HISTOGRAM_QUANTILE(0.95, SUM(RATE(value)));
  a condition on one label only: SUM(rate) FILTER (WHERE mode <> 'idle'). Which servers
  had samples in a window: GROUP BY node with COUNT(*) (SELECT DISTINCT reads the label
  index, which may list servers that stopped earlier). A metrics database over several
  tenants has a column __tenant_id__: GROUP BY __tenant_id__, node keeps them apart.
  describe_data gives example SQL and how metric labels match index fields (label node = "NODE").
- Investigations ("why did the jobs fail", "was a server saturated"): 1) find where and
  when on the jobs index (execute_sql: failed jobs by "NODE" or "APPLICATION" and hour),
  2) call check_health for that time window with entities = the servers / applications
  found (it lists CPU, memory, disk, OOM kills, outages, queues, HTTP errors, latency and
  licences with from-to and worst value), 3) answer with the problems that match the
  failures (server, check, from-to, worst value) and say what was not found.
  promql_query runs any other PromQL; list_alerts shows the alerts firing now.
- Report requests ("failing jobs today", "jobs by application"...): run the SQL
  (execute_sql), then answer in the chat with one or two sentences and a Markdown table
  (at most 30 rows); for a ranking or a time series also call show_chart and put its
  output in the answer. Only when the user asks for it:
  JSON -> answer with the rows as a ```json block only; a file or Excel extract ->
  export_excel (give the file path; only there a row list may join the big index with
  small ones, e.g. jobs with their application's TEAM); an image of data -> chart_from_sql
  (a SELECT: label column then value columns; bar = ranking, line = time series), an image
  of a saved Superset chart -> chart_image(chart_id); an e-mail now -> send_email
  (body_markdown, sql for a table, chart_sqls or image_paths for images, excel_sql or
  attach_paths for the Excel file); a recurring e-mail -> create_report.
- Say exactly what the tools did: if an image, a file or an e-mail could not be made, say
  so; never claim what a tool result does not show.
- Keep answers short and give the SQL you ran."""


def inline_refs(schema: dict) -> dict:
    """Replace $ref by the referenced $defs entry (local models read plain schemas better)."""
    defs = schema.get("$defs", {})

    def walk(node: Any, depth: int = 0) -> Any:
        if isinstance(node, dict):
            if "$ref" in node and depth < 8:
                name = node["$ref"].rsplit("/", 1)[-1]
                return walk(copy.deepcopy(defs.get(name, {})), depth + 1)
            return {k: walk(v, depth) for k, v in node.items() if k != "$defs"}
        if isinstance(node, list):
            return [walk(v, depth) for v in node]
        return node

    return walk(schema)


def tool_text(result: Any, limit: int | None = MAX_TOOL_CHARS) -> str:
    parts = [getattr(c, "text", None) or str(c) for c in getattr(result, "content", None) or []]
    text = "\n".join(parts) if parts else json.dumps(getattr(result, "data", None), default=str)
    return text if limit is None or len(text) <= limit else text[:limit] + "\n...[truncated]"


CHART_TOOLS = ("generate_chart", "update_chart", "update_chart_preview")
REF_FIELDS = {"name", "column_name", "label", "dtype", "aggregate", "saved_metric"}
# accepted by Superset 6.1's MCP schema, but its chart queries only run these:
RUNNABLE_AGGREGATES = {"SUM", "COUNT", "AVG", "MIN", "MAX", "COUNT_DISTINCT"}


def _short_error(ex: Exception) -> str:
    """The useful lines of a pydantic error (field path and message)."""
    lines = [ln.strip() for ln in str(ex).splitlines()[1:]]
    return "\n".join(ln for ln in lines if ln and not ln.startswith("For further information"))[:1500]


def _sort_entries(values: list) -> list:
    """Table sort_by "col DESC" / "-col" / "col" -> Superset's ["col", ascending] entries
    (the MCP service copies sort_by verbatim, and the table ignores other forms)."""
    out = []
    for v in values:
        if isinstance(v, (list, tuple)) and len(v) == 2 and isinstance(v[0], str):
            out.append(json.dumps([v[0], bool(v[1])]))      # Superset's own [column, ascending]
            continue
        if not isinstance(v, str) or v.lstrip().startswith("["):
            out.append(v)
            continue
        text = v.strip()
        desc = text.startswith("-") or text.upper().endswith(" DESC")
        col = text.lstrip("-").strip()
        for suffix in (" DESC", " ASC", " desc", " asc"):
            if col.endswith(suffix):
                col = col[: -len(suffix)].strip()
        out.append(json.dumps([col, not desc]))
    return out


def time_range_fix(params: dict) -> dict | None:
    """Move >=, >, <=, < filters on the chart's time column (granularity_sqla) into its
    TEMPORAL_RANGE filter; None when there is nothing to move."""
    col = params.get("granularity_sqla")
    filters = params.get("adhoc_filters") or []
    moved = [f for f in filters if f.get("expressionType") == "SIMPLE" and f.get("subject") == col
             and f.get("operator") in (">=", ">", "<=", "<")]
    if not col or not moved:
        return None
    start = next((f["comparator"] for f in moved if f["operator"] in (">=", ">")), "")
    end = next((f["comparator"] for f in moved if f["operator"] in ("<=", "<")), "")
    time_range = f"{start} : {end}"
    kept = [f for f in filters if f not in moved]
    temporal = [f for f in kept if f.get("operator") == "TEMPORAL_RANGE" and f.get("subject") == col]
    if temporal:
        temporal[0]["comparator"] = time_range
    else:
        kept.append({"clause": "WHERE", "expressionType": "SIMPLE", "subject": col,
                     "operator": "TEMPORAL_RANGE", "comparator": time_range})
    return {**params, "adhoc_filters": kept, "time_range": time_range}


class SupersetRest:
    """Superset's REST API with the agent's account (only for fixes the MCP service lacks)."""

    def __init__(self) -> None:
        self.base = os.environ.get("SUPERSET_URL", "").rstrip("/")
        self.client: httpx.Client | None = None

    def _login(self) -> httpx.Client:
        c = httpx.Client(base_url=self.base, timeout=60)
        r = c.post("/api/v1/security/login", json={
            "username": os.environ["SUPERSET_USER"], "password": os.environ["SUPERSET_PASSWORD"],
            "provider": "db", "refresh": True})
        r.raise_for_status()
        c.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        c.headers["X-CSRFToken"] = c.get("/api/v1/security/csrf_token/").json()["result"]
        c.headers["Referer"] = self.base
        return c

    def request(self, method: str, path: str, **kw: Any) -> Any:
        if not (self.base and os.environ.get("SUPERSET_USER") and os.environ.get("SUPERSET_PASSWORD")):
            return None
        for _ in range(2):
            if self.client is None:
                self.client = self._login()
            r = self.client.request(method, path, **kw)
            if r.status_code != 401:
                r.raise_for_status()
                return r.json() if r.content else None
            self.client = None                      # token expired
        return None

    def fix_time_range(self, chart_id: int) -> str | None:
        chart = self.request("GET", f"/api/v1/chart/{chart_id}")
        if not chart:
            return None
        fixed = time_range_fix(json.loads(chart["result"].get("params") or "{}"))
        if fixed is None:
            return None
        self.request("PUT", f"/api/v1/chart/{chart_id}", json={"params": json.dumps(fixed)})
        return fixed["time_range"]


def _refs(node: Any, path: str = "") -> list[tuple[str, dict]]:
    """Column references ({"name": ...}) and filters ({"column": ...}) in a chart config."""
    out: list[tuple[str, dict]] = []
    if isinstance(node, dict):
        if isinstance(node.get("name"), str) or isinstance(node.get("column"), str):
            out.append((path or "config", node))
        for k, v in node.items():
            if k not in ("x_axis", "y_axis", "legend"):
                out.extend(_refs(v, f"{path}.{k}" if path else k))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            out.extend(_refs(v, f"{path}[{i}]"))
    return out


class ChartGuard:
    """Checks chart tool calls before Superset's MCP service sees them (see module doc)."""

    def __init__(self, agent: "SupersetAgent") -> None:
        self.agent = agent
        self.datasets: dict[str, tuple[set[str], set[str]]] = {}
        self.saved: dict[str, int] = {}
        self.dataset_of: dict[int, Any] = {}
        self.rest = SupersetRest()
        try:
            from superset.mcp_service.chart.schemas import parse_chart_config  # noqa: PLC0415

            self.parse = parse_chart_config
        except Exception:  # pylint: disable=broad-except  (not in the Superset virtualenv)
            self.parse = None

    async def _dataset(self, ident: Any) -> tuple[set[str], set[str]] | None:
        key = str(ident)
        if key not in self.datasets:
            client = self.agent.routes.get("get_dataset_info")
            if client is None:
                return None
            try:
                res = await client.call_tool("get_dataset_info", {"request": {"identifier": ident}},
                                             raise_on_error=False)
                info = json.loads(tool_text(res, limit=None))
                self.datasets[key] = ({c["column_name"] for c in info.get("columns") or []},
                                      {m["metric_name"] for m in info.get("metrics") or []})
            except Exception:  # pylint: disable=broad-except
                return None
        return self.datasets[key]

    def _check(self, config: dict, cols: set[str] | None, metrics: set[str] | None) -> list[str]:
        """Fields, aggregates, and (dataset known) column and metric names."""
        errors = []
        for where, ref in _refs(config):
            if isinstance(ref.get("column"), str) and "name" not in ref:        # a filter
                if cols is not None and ref["column"] not in cols:
                    errors.append(f"{where}: unknown column '{ref['column']}'")
                continue
            name = ref["name"]
            extra = sorted(set(ref) - REF_FIELDS)
            if extra:
                errors.append(f"{where}: {', '.join(extra)} is not a chart field (a column takes only "
                              "name, label, aggregate or saved_metric; SQL expressions are not possible)")
            agg = str(ref.get("aggregate") or "").upper()
            if agg and agg not in RUNNABLE_AGGREGATES:
                errors.append(f"{where}: Superset cannot run {agg} in these charts (only "
                              f"{', '.join(sorted(RUNNABLE_AGGREGATES))}); use a saved metric of the "
                              f"dataset if one fits ({', '.join(sorted(metrics or [])) or 'none'}), "
                              "otherwise say it is not possible")
            if cols is None:
                continue
            if ref.get("saved_metric"):
                if name not in metrics:
                    errors.append(f"{where}: '{name}' is not a saved metric (saved metrics: "
                                  f"{', '.join(sorted(metrics))})")
            elif name in cols:
                continue
            elif name in metrics:
                errors.append(f'{where}: "{name}" is a saved metric: use {{"name": "{name}", '
                              '"saved_metric": true}')
            elif name.lower() in ("count", "*", "count(*)", "rows", "jobs"):
                errors.append(f"{where}: there is no column '{name}'; COUNT(*) is "
                              + ('{"name": "count", "saved_metric": true}' if "count" in metrics
                                 else "COUNT of a column that is never empty"))
            else:
                errors.append(f"{where}: unknown column '{name}' (columns: "
                              f"{', '.join(sorted(cols))[:800]})")
        return errors

    async def _dry_run(self, dataset_id: Any, config: dict) -> str | None:
        """Preview without saving: an error for the model, or None when the chart has rows."""
        client = self.agent.routes.get("generate_chart")
        if client is None:
            return None
        res = await client.call_tool("generate_chart", {"request": {
            "dataset_id": dataset_id, "config": config, "save_chart": False,
            "preview_formats": ["table"]}}, raise_on_error=False)
        try:
            data = json.loads(tool_text(res, limit=None))
        except ValueError:
            return None
        if data.get("success") is False or data.get("error"):
            return json.dumps({"success": False, "error": "the chart query fails, nothing was saved",
                               "details": data.get("error")}, ensure_ascii=False, default=str)[:3000]
        if ((data.get("previews") or {}).get("table") or {}).get("row_count") == 0:
            return json.dumps({"success": False, "error": "the chart returns NO ROWS with these filters, "
                               "nothing was saved: use dates that exist in the data, and labels "
                               "that agree with the dates, then call again"})
        return None

    async def before(self, name: str, args: dict) -> tuple[str, dict, str | None]:
        """(tool, arguments) to call instead, or an error to return to the model."""
        req = args.get("request", args)
        if name not in CHART_TOOLS or not isinstance(req, dict):
            return name, args, None
        config = req.get("config")
        if isinstance(config, dict) and config.get("chart_type") == "table":
            for key in ("sort_by", "order_by_cols", "order_by"):         # the schema's aliases
                if isinstance(config.get(key), list):
                    config[key] = _sort_entries(config[key])
        if isinstance(config, dict):
            errors = []
            if self.parse is not None:
                try:
                    self.parse(config)
                except Exception as ex:  # pylint: disable=broad-except
                    errors.append(_short_error(ex))
            ds = req.get("dataset_id") if name == "generate_chart" else \
                self.dataset_of.get(req.get("identifier"))
            known = await self._dataset(ds) if ds is not None and not errors else None
            x = config.get("x") if isinstance(config.get("x"), dict) else {}
            if known and x.get("name") == "ts" and {"ts", "value"} <= known[0] and not config.get("time_grain"):
                config["time_grain"] = "PT1H"      # metrics (promagg): time buckets, never raw samples
            if not errors:
                errors += self._check(config, *(known or (None, None)))
            if errors:
                return name, args, json.dumps({"success": False, "error": "invalid chart config: "
                                               "fix these points and call the tool again",
                                               "details": errors}, ensure_ascii=False)
            saving = (name == "generate_chart" and req.get("save_chart")) or (
                name == "update_chart" and req.get("generate_preview") is False)
            target = ds if name == "generate_chart" else self.dataset_of.get(req.get("identifier"))
            if saving and target is not None:
                problem = await self._dry_run(target, config)
                if problem:
                    return name, args, problem
        chart_name = req.get("chart_name")
        if name == "generate_chart" and req.get("save_chart") and chart_name in self.saved:
            return "update_chart", {"request": {"identifier": self.saved[chart_name], "config": config,
                                                "chart_name": chart_name, "generate_preview": False}}, None
        return name, args, None

    def after(self, name: str, args: dict, content: str) -> str:
        """Remember the charts saved during this question (name -> id, id -> dataset)."""
        if name not in CHART_TOOLS:
            return content
        try:
            data = json.loads(content)
        except ValueError:
            return content
        chart = data.get("chart") or {}
        if chart.get("id") and chart.get("slice_name"):
            self.saved[chart["slice_name"]] = chart["id"]
            ds = args.get("request", args).get("dataset_id")
            if ds is not None:
                self.dataset_of[chart["id"]] = ds
            try:
                time_range = self.rest.fix_time_range(chart["id"])
            except Exception:  # pylint: disable=broad-except
                time_range = None
            if time_range:
                content += f"\n(the date filters were saved as the chart's time range: {time_range})"
        return content


def show_chart(title: str, labels: list[str], values: list[float], unit: str = "") -> str:
    """Bar chart as text, for the chat."""
    pairs = [(str(lb), float(v)) for lb, v in zip(labels, values) if v is not None][:40]
    if not pairs:
        return "(no data)"
    top = max(abs(v) for _, v in pairs) or 1.0
    width = max(len(lb) for lb, _ in pairs)
    lines = [title]
    for lb, v in pairs:
        bar = "\u2588" * max(1, round(abs(v) / top * 40)) if v else ""
        lines.append(f"{lb.ljust(width)} {bar} {v:,.6g}{(' ' + unit) if unit else ''}")
    return "\n".join(lines)


def _table_if_missing(answer: str, trace: list[dict]) -> str:
    """A ranking shown as a text chart (show_chart) without its Markdown table: append the table
    of the SQL result behind it (report answers carry both)."""
    if "|" in answer and re.search(r"^\s*\|.+\|\s*$", answer, re.M):
        return ""
    if not any(t.get("called", t["tool"]) == "show_chart" for t in trace):
        return ""
    for t in reversed(trace):
        if t.get("called", t["tool"]) != "execute_sql":
            continue
        try:
            rows = json.loads(t["result"]).get("rows") or []
        except (ValueError, TypeError, AttributeError):
            continue
        if not rows or len(rows) > 30 or not isinstance(rows[0], dict):
            continue
        cols = list(rows[0])
        out = ["", "| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
        for r in rows:
            out.append("| " + " | ".join(_cell(r.get(c)) for c in cols) + " |")
        return "\n".join(out)
    return ""


def _chart_if_missing(answer: str, trace: list[dict]) -> str:
    """A ranking given as a table without its text chart: append the chart of the SQL result
    (one label column and one number column, at most 30 rows)."""
    if "\u2588" in answer or not re.search(r"^\s*\|.+\|\s*$", answer, re.M):
        return ""
    if any(w in answer.lower() for w in ("```json",)):
        return ""
    for t in reversed(trace):
        if t.get("called", t["tool"]) != "execute_sql":
            continue
        try:
            rows = json.loads(t["result"]).get("rows") or []
        except (ValueError, TypeError, AttributeError):
            continue
        if not rows or len(rows) > 30 or not isinstance(rows[0], dict) or len(rows[0]) != 2:
            return ""
        label_col, value_col = list(rows[0])
        if not all(isinstance(r.get(value_col), (int, float)) and r.get(value_col) is not None for r in rows):
            return ""
        chart = show_chart(value_col, [str(r.get(label_col)) for r in rows], [r[value_col] for r in rows])
        return "\n\n```\n" + chart + "\n```"
    return ""


def _cell(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:,.2f}" if abs(v) < 1e15 else str(v)
    if isinstance(v, int):
        return f"{v:,}"
    return "" if v is None else str(v).replace("|", "/")


def _claims_check(answer: str, trace: list[dict]) -> str:
    """Correct an answer that claims e-mail content the send_email result does not show."""
    notes = []
    sent = []
    for t in trace:
        if t.get("called", t["tool"]) != "send_email":
            continue
        try:
            res = json.loads(t["result"])
        except (ValueError, TypeError):
            continue
        if res.get("sent_to"):
            sent.append(res)
    for res in sent[-1:]:                          # the e-mail the answer is about
        low = answer.lower()
        if not res.get("images") and re.search(r"\b(image|chart|graph)", low):
            notes.append("the e-mail was sent without an image (send_email: images 0)")
        if not res.get("attached") and re.search(r"attach|excel|xlsx", low):
            notes.append("the e-mail was sent without an attachment")
    return ("\n\n(Check: " + "; ".join(dict.fromkeys(notes)) + ".)") if notes else ""


LOCAL_TOOLS = {
    "show_chart": (show_chart, {
        "description": "Draw a bar chart as text for the chat answer (labels and values of a ranking or "
                       "a time series). Put the returned text in the answer inside a ``` block.",
        "parameters": {"type": "object", "required": ["title", "labels", "values"], "properties": {
            "title": {"type": "string"}, "labels": {"type": "array", "items": {"type": "string"}},
            "values": {"type": "array", "items": {"type": "number"}}, "unit": {"type": "string"}}}}),
}


class SupersetAgent:
    def __init__(self, tools: list[str] | None = None, system_extra: str = "",
                 verbose: bool = True) -> None:
        self.allow = tools or TOOLS
        self.system_extra = system_extra
        self.verbose = verbose
        self.routes: dict[str, Client] = {}
        self.schemas: dict[str, dict] = {}
        self.tools: list[dict] = []
        self.guard = ChartGuard(self)

    async def __aenter__(self) -> "SupersetAgent":
        async def quiet(_message: Any) -> None:     # server log notifications
            return None

        self._stack = AsyncExitStack()
        self.http = await self._stack.enter_async_context(httpx.AsyncClient(timeout=900))
        self.model = os.environ.get("LLM_MODEL") or (
            await self.http.get(f"{LLM_URL}/models")).json()["data"][0]["id"]
        for name, (_fn, spec) in LOCAL_TOOLS.items():
            if "*" in self.allow or name in self.allow:
                self.schemas[name] = spec["parameters"]
                self.tools.append({"type": "function", "function": {"name": name, **spec}})
        for url in MCP_URLS:
            target: Any = url
            if url.endswith(".py"):            # local MCP server over stdio, with our environment
                from fastmcp.client.transports import PythonStdioTransport

                target = PythonStdioTransport(url, python_cmd=sys.executable, env=dict(os.environ))
            client = await self._stack.enter_async_context(Client(target, log_handler=quiet))
            for t in await client.list_tools():
                self.routes[t.name] = client          # the guard may call tools the model is not given
                if "*" not in self.allow and t.name not in self.allow:
                    continue
                self.schemas[t.name] = inline_refs(t.inputSchema)
                self.tools.append({"type": "function", "function": {
                    "name": t.name, "description": (t.description or "")[:1500],
                    "parameters": self.schemas[t.name]}})
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self._stack.aclose()

    async def _chat(self, messages: list) -> dict:
        headers = ({"Authorization": f"Bearer {os.environ['LLM_API_KEY']}"}
                   if os.environ.get("LLM_API_KEY") else {})
        body = {"model": self.model, "messages": messages, "tools": self.tools,
                "tool_choice": "auto", "temperature": 0.2}
        if not THINKING:            # Qwen3.x / llama.cpp --jinja: no reasoning tokens
            body["chat_template_kwargs"] = {"enable_thinking": False}
        r = await self.http.post(f"{LLM_URL}/chat/completions", headers=headers, json=body)
        r.raise_for_status()
        msg = r.json()["choices"][0]["message"]
        if not (msg.get("content") or "").strip() and not msg.get("tool_calls"):
            # seen with llama.cpp after a while: one empty token for every request
            raise RuntimeError("LLM returned an empty answer (server degraded? restart it)")
        return msg

    async def ask(self, question: str) -> tuple[str, list[dict]]:
        """Answer one question; returns the answer and the trace of tool calls."""
        self.guard.saved = {}
        failed: dict[str, str] = {}                 # identical calls that already failed
        charts: list[str] = []                      # show_chart outputs, kept in the answer
        emailed: list[str] = []                     # at most one e-mail per request
        messages: list[dict] = [
            {"role": "system", "content": SYSTEM + self.system_extra},
            {"role": "user", "content": f"(Now: {_now():%A %Y-%m-%d %H:%M}.)\n{question}"}]
        trace: list[dict] = []
        for _ in range(MAX_STEPS):
            msg = await self._chat(messages)
            messages.append({k: v for k, v in msg.items() if k in ("role", "content", "tool_calls")})
            calls = msg.get("tool_calls") or []
            if not calls:
                answer = (msg.get("content") or "").strip()
                missing = [c for c in charts if c.splitlines()[-1].strip() not in answer]
                if missing:
                    answer += "".join(f"\n\n```\n{c}\n```" for c in missing)
                answer += _table_if_missing(answer, trace)
                answer += _chart_if_missing(answer, trace)
                return answer + _claims_check(answer, trace), trace
            for tc in calls:
                name = tc["function"]["name"]
                try:
                    args = json.loads(tc["function"].get("arguments") or "{}")
                except json.JSONDecodeError:
                    args = {}
                props = self.schemas.get(name, {}).get("properties", {})
                if list(props) == ["request"] and "request" not in args:
                    args = {"request": args}          # the model skipped the wrapper
                if self.verbose:
                    print(f"  -> {name} {json.dumps(args, ensure_ascii=False)[:300]}", file=sys.stderr)
                t0 = time.time()
                called, call_args, content = (await self.guard.before(name, args) if name in self.schemas
                                              else (name, args, f"unknown tool {name}"))
                if content is not None and self.verbose:
                    print(f"     guard: {content[:300]}", file=sys.stderr)
                call_key = name + json.dumps(args, sort_keys=True, default=str)
                if content is None and name == "send_email" and emailed:
                    content = (f"An e-mail was already sent for this request ({emailed[0]}): do not send "
                               "another one; tell the user what it contained.")
                elif content is None and call_key in failed:
                    content = (f"You already made exactly this call and it failed: {failed[call_key][:600]}. "
                               "Change the arguments as the error says, or answer the user.")
                elif content is None and name in LOCAL_TOOLS:
                    try:
                        content = LOCAL_TOOLS[name][0](**args)
                        if name == "show_chart":
                            charts.append(content)
                    except Exception as ex:  # pylint: disable=broad-except
                        content = f"tool error: {ex}"
                if content is None:
                    client = self.routes.get(called)
                    try:
                        content = (tool_text(await client.call_tool(called, call_args, raise_on_error=False),
                                             limit=None) if client else f"unknown tool {called}")
                    except Exception as ex:  # pylint: disable=broad-except
                        content = f"tool error: {ex}"
                    content = self.guard.after(called, call_args, content)
                    if called != name:
                        content = f"(a chart with this name was already saved: {called} was used)\n" + content
                if re.search(r'"(error|success)":\s*("|false)|^(error|tool error|unknown tool)', content[:300]):
                    failed[call_key] = content
                elif called == "send_email" and '"sent_to"' in content:
                    emailed.append(content[:300])
                if len(content) > MAX_TOOL_CHARS:
                    content = content[:MAX_TOOL_CHARS] + "\n...[truncated]"
                trace.append({"tool": name, "called": called, "args": args,
                              "seconds": round(time.time() - t0, 1), "result": content[:4000]})
                messages.append({"role": "tool", "tool_call_id": tc.get("id", name), "content": content})
        return "(stopped after too many tool calls)", trace


async def main() -> None:
    async with SupersetAgent() as agent:
        questions = [" ".join(sys.argv[1:])] if len(sys.argv) > 1 else None
        while True:
            q = questions.pop(0) if questions else input("\nquestion> ").strip()
            if not q:
                break
            answer, _trace = await agent.ask(q)
            print(answer)
            if questions is not None and not questions:
                break


if __name__ == "__main__":
    asyncio.run(main())
