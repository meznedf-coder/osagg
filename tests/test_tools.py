"""The AI tools' own logic, without Superset or an LLM: SQL checks of the extracts, file
names and paths, e-mail line lengths, charts drawn from query results, and the agent's
chart guard helpers and answer checks."""

from __future__ import annotations

import json
import re
import os
import sys

import pytest

pytest.importorskip("fastmcp")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

import superset_agent as agent  # noqa: E402
import superset_tools_mcp as tools  # noqa: E402


def test_extracts_are_one_select_with_a_bounded_limit():
    sql, limit = tools._check_select('SELECT "A" FROM "idx" WHERE "B" = 1', 1000)
    assert limit == 1001 and sql.endswith("LIMIT 1001")
    sql, limit = tools._check_select('SELECT "A" FROM "idx" LIMIT 50', 1000)
    assert limit == 50 and sql.endswith("LIMIT 50")
    _sql, limit = tools._check_select('SELECT "A" FROM "idx" LIMIT 5000000', 1000)
    assert limit == 1001
    for bad in ('DELETE FROM "idx"', 'SELECT 1; SELECT 2', 'INSERT INTO t SELECT 1', 'DROP TABLE x'):
        with pytest.raises(tools.ToolError):
            tools._check_select(bad, 10)


def test_file_names_and_paths_stay_in_the_export_folder(tmp_path, monkeypatch):
    name = tools._file_name("../../etc/passwd failed jobs.xlsx", "extract", "xlsx")
    assert "/" not in name and re.fullmatch(r"passwd_failed_jobs-[0-9a-f]{6}\.xlsx", name)
    monkeypatch.setattr(tools, "EXPORT_DIR", str(tmp_path))
    (tmp_path / "chart-1.png").write_bytes(b"png")
    assert tools._export_file("chart-1.png", (".png",)) == str(tmp_path / "chart-1.png")
    assert tools._export_file("/elsewhere/chart-1.png", (".png",)) == str(tmp_path / "chart-1.png")
    for bad in ("../secret.png", "chart-1.exe", "missing.png"):
        with pytest.raises(tools.ToolError):
            tools._export_file(bad, (".png",))


def test_email_lines_fit_smtp():
    html = "<table>" + "".join(f"<tr><td>{'x' * 50} {i}</td></tr>" for i in range(200)) + "</table>"
    html += "<p>" + " ".join(["word"] * 1000) + "</p>"
    assert max(len(line) for line in tools._short_lines(html).split("\n")) <= 998


def test_charts_from_query_results():
    labels, data = tools._series(["app", "failed"], [("BILLING", 577), ("ORDERS", 224)])
    assert labels == ["BILLING", "ORDERS"] and data == {"failed": [577.0, 224.0]}
    labels, data = tools._series(["hour", "env", "jobs"], [("00", "PROD", 5), ("00", "DEV", 1), ("01", "PROD", 7)])
    assert labels == ["00", "01"] and data == {"PROD": [5.0, 7.0], "DEV": [1.0, None]}
    for kind in ("bar", "line"):
        svg = tools._svg(kind, "Failed <jobs>", ["app", "failed"], [("BILLING", 577), ("ORDERS", 224)], "", 800, 400)
        assert svg.startswith("<svg") and svg.endswith("</svg>") and "Failed &lt;jobs&gt;" in svg
    assert tools._ticks(577 * 1.08)[:3] == [0.0, 200.0, 400.0]


def test_claims_about_an_email_are_checked():
    trace = [{"tool": "send_email", "result": json.dumps({"sent_to": ["a@b.c"], "images": 0, "attached": []})}]
    note = agent._claims_check("Sent with a bar chart image and the Excel attached.", trace)
    assert "without an image" in note and "without an attachment" in note
    ok = [{"tool": "send_email", "result": json.dumps({"sent_to": ["a@b.c"], "images": 1, "attached": ["x.xlsx"]})}]
    assert agent._claims_check("Sent with a bar chart image and the Excel attached.", ok) == ""


def test_guard_helpers():
    assert agent._sort_entries([["t", False], "POSITION_DATE DESC", "APP"]) == [
        '["t", false]', '["POSITION_DATE", false]', '["APP", true]']
    fixed = agent.time_range_fix({"granularity_sqla": "ts", "adhoc_filters": [
        {"expressionType": "SIMPLE", "subject": "ts", "operator": ">=", "comparator": "2026-09-19"},
        {"expressionType": "SIMPLE", "subject": "ts", "operator": "<", "comparator": "2026-09-25"},
        {"expressionType": "SIMPLE", "subject": "APP", "operator": "IN", "comparator": ["A"]}]})
    assert fixed["time_range"] == "2026-09-19 : 2026-09-25"
    assert [f["operator"] for f in fixed["adhoc_filters"]] == ["IN", "TEMPORAL_RANGE"]
    assert agent.time_range_fix({"granularity_sqla": "ts", "adhoc_filters": []}) is None


def test_files_are_found_by_their_short_id_and_escaped_sql_is_read(tmp_path, monkeypatch):
    monkeypatch.setattr(tools, "EXPORT_DIR", str(tmp_path))
    (tmp_path / "Failed_jobs_by_app-a3f9c2.png").write_bytes(b"png")
    for ref in ("a3f9c2", "Failed_jobs_by_app-a3f9c2.png", "Failed_Jobs-a3f9c2.png", "/x/y/whatever-a3f9c2.png"):
        assert tools._export_file(ref, (".png",)).endswith("Failed_jobs_by_app-a3f9c2.png"), ref
    with pytest.raises(tools.ToolError):
        tools._export_file("b0b0b0", (".png",))
    sql, _ = tools._check_select('SELECT\\n  \\"A\\"\\nFROM \\"idx\\"', 10)
    assert sql == 'SELECT "A" FROM "idx" LIMIT 11'


def test_email_images_go_where_the_text_puts_them_and_tables_are_styled():
    import markdown

    body = markdown.markdown("Intro\n\n![Failed jobs](chart.png)\n\n![Extra](x.png)\n\n| app | n |\n|---|---|\n| A | 1,234 |",
                             extensions=["tables"])
    text, rest = tools._place_images(tools._style_tables(body), ["chart1"])
    assert 'src="cid:chart1"' in text and "x.png" not in text and "chart.png" not in text and rest == []
    assert "text-align:right\">1,234" in text and "border-collapse" in text
    text, rest = tools._place_images("<p>no image</p>", ["chart1", "chart2"])
    assert rest == ["chart1", "chart2"]


class _Zone:
    def local(self, ms):
        import datetime as dt

        return dt.datetime(1970, 1, 1) + dt.timedelta(milliseconds=ms)


class _Conn:
    zone = _Zone()


class _S:
    def __init__(self, labels, points):
        self.labels, self.points = labels, points


def test_health_breaches_need_the_minimum_duration():
    step = 60_000
    pts = [(i * step, v) for i, v in enumerate([10, 95, 96, 97, 20, 99, 10])]
    found = tools._breaches(_Conn(), [_S({"node": "a"}, pts)], "above", 90, 3 * step, step)
    assert len(found) == 1 and found[0]["minutes"] == 3 and found[0]["worst"] == 97
    assert len(tools._breaches(_Conn(), [_S({"node": "a"}, pts)], "above", 90, 0, step)) == 2
    low = [(i * step, v) for i, v in enumerate([50, 3, 2, 50])]
    b = tools._breaches(_Conn(), [_S({"node": "b"}, low)], "below", 5, 0, step)
    assert b[0]["worst"] == 2 and b[0]["labels"] == {"node": "b"}


def test_health_checks_keep_tenants_apart():
    """Database over several tenants (tenant=a|b): the same server name in two tenants stays
    two servers, and every breach says its tenant."""
    assert tools._per_tenant('100 * (1 - avg by (node, region) (rate(m{mode="idle"}[5m])))') == \
        '100 * (1 - avg by (__tenant_id__, node, region) (rate(m{mode="idle"}[5m])))'
    assert tools._per_tenant("sum by(application) (a) / sum by (application) (b)") == \
        "sum by (__tenant_id__, application) (a) / sum by (__tenant_id__, application) (b)"
    assert tools._per_tenant("histogram_quantile(0.95, sum by (le) (rate(x[5m])))") == \
        "histogram_quantile(0.95, sum by (__tenant_id__, le) (rate(x[5m])))"
    assert tools._per_tenant("sum by () (x)") == "sum by (__tenant_id__) (x)"
    assert tools._per_tenant("a / on (node) group_left b") == "a / on (__tenant_id__, node) group_left b"
    for same in ("sum by (__tenant_id__, a) (x)", "sum without (le) (x)", "100 * a / b", 'up{job="node"}'):
        assert tools._per_tenant(same) == same


def test_chart_time_filters_become_the_time_range():
    """Charts saved by Superset's MCP service keep ts >= / < as plain filters, which dashboards
    ignore: the tools' fix (for any agent) and the agent's own give the same time range."""
    params = {"granularity_sqla": "ts", "x_axis": "ts", "adhoc_filters": [
        {"clause": "WHERE", "expressionType": "SIMPLE", "subject": "ts", "operator": ">=", "comparator": "2026-09-27 07:00:00"},
        {"clause": "WHERE", "expressionType": "SIMPLE", "subject": "ts", "operator": "<", "comparator": "2026-09-27 08:30:00"},
        {"clause": "WHERE", "expressionType": "SIMPLE", "subject": "ts", "operator": "TEMPORAL_RANGE", "comparator": "No filter"},
        {"clause": "WHERE", "expressionType": "SIMPLE", "subject": "__tenant_id__", "operator": "IN", "comparator": ["fed-b"]}]}
    fixed = tools._time_range_fix(json.loads(json.dumps(params)))
    assert fixed["time_range"] == "2026-09-27 07:00:00 : 2026-09-27 08:30:00"
    assert [(f["subject"], f["operator"]) for f in fixed["adhoc_filters"]] == [("ts", "TEMPORAL_RANGE"), ("__tenant_id__", "IN")]
    assert fixed == agent.time_range_fix(json.loads(json.dumps(params)))
    assert tools._time_range_fix({"granularity_sqla": "ts", "adhoc_filters": []}) is None


def test_metric_range_bisects_the_series_index():
    start, end = 0, 30 * 86_400_000
    first, last = 5 * 86_400_000 + 3_600_000 * 7, 20 * 86_400_000

    class Client:
        calls = 0

        def series(self, match, a, b, limit=None):
            Client.calls += 1
            return [{"__name__": "m"}] if a < last and b > first else []

    conn = _Conn()
    conn.client = Client()
    lo, hi = tools._metric_range(conn, '{__name__="m"}', start, end)
    assert abs(lo - first) <= 3_600_000 and abs(hi - last) <= 3_600_000 and Client.calls < 30


def test_ranking_answers_get_their_table():
    trace = [{"tool": "execute_sql", "result": json.dumps({"rows": [{"APPLICATION": "BILLING", "failed": 577},
                                                                    {"APPLICATION": "ORDERS", "failed": 224}]})},
             {"tool": "show_chart", "result": "BILLING ████ 577"}]
    table = agent._table_if_missing("BILLING leads with 577 failures.", trace)
    assert "| APPLICATION | failed |" in table and "| BILLING | 577 |" in table
    assert agent._table_if_missing("| a | b |\n|---|---|\n| x | 1 |", trace) == ""
    assert agent._table_if_missing("no chart here", trace[:1]) == ""


def test_ranking_tables_get_their_chart():
    trace = [{"tool": "execute_sql", "result": json.dumps({"rows": [{"APPLICATION": "BILLING", "failed": 577},
                                                                    {"APPLICATION": "ORDERS", "failed": 224}]})}]
    answer = "Failed jobs:\n\n| APPLICATION | failed |\n|---|---|\n| BILLING | 577 |\n| ORDERS | 224 |"
    chart = agent._chart_if_missing(answer, trace)
    assert "█" in chart and "BILLING" in chart
    assert agent._chart_if_missing(answer + "\n██ 577", trace) == ""
    wide = [{"tool": "execute_sql", "result": json.dumps({"rows": [{"a": "x", "b": 1, "c": 2}]})}]
    assert agent._chart_if_missing(answer, wide) == ""
