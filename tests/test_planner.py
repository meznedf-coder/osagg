"""Offline planner tests (no OpenSearch needed): check the generated DSL."""

from __future__ import annotations

import datetime as dt

import glob
import json
import os
import sys
from zoneinfo import ZoneInfo

import pytest
import sqlglot

from osagg import duck
from osagg.metadata import ID_FIELD, TableMeta, fields_from_mapping
from osagg.planner import AggScan, DocScan, Planner, Settings

from generic_mapping import MAPPING

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INDEX = "jobs"


def meta() -> TableMeta:
    fields = fields_from_mapping({INDEX: {"mappings": MAPPING}})
    fields["_id"] = ID_FIELD
    return TableMeta(INDEX, [INDEX], fields)


@pytest.fixture(scope="module")
def planner():
    m = meta()
    con = duck.locked_session("Europe/Paris")

    def const_eval(node):
        return con.execute("SELECT " + node.sql(dialect="duckdb")).fetchone()[0]

    def lookup(name):
        return m if name in (INDEX, "jobs-archive") else None

    def make(**kw):
        return Planner(lookup, Settings(tz=ZoneInfo("Europe/Paris"), **kw), const_eval)

    return make


def plan(planner, sql, **kw):
    return planner(**kw).plan(sqlglot.parse_one(sql, read="duckdb"))


def dsl(scan) -> str:
    return json.dumps(scan.describe(), sort_keys=True)


def test_not_in_has_exists_guard(planner):
    p = plan(planner, f'SELECT COUNT(*) FROM "{INDEX}" WHERE "POOL" NOT IN (\'A\', \'B\')')
    q = p.scans[0].query
    assert q == {"bool": {"filter": [{"exists": {"field": "POOL"}},
                                     {"bool": {"must_not": [{"terms": {"POOL": ["A", "B"]}}]}}]}}


def test_time_range_in_the_connection_timezone(planner):
    p = plan(planner, f'SELECT COUNT(*) FROM "{INDEX}" WHERE "@timestamp_date" >= TIMESTAMP \'2026-03-29 03:00:00\'')
    rng = p.scans[0].query["range"]["@timestamp_date"]
    # 03:00 CEST (first hour of summer time) == 01:00 UTC: an ISO instant with its offset, as people write it
    assert rng == {"gte": "2026-03-29T03:00:00.000+02:00", "format": "strict_date_optional_time"}
    assert dt.datetime.fromisoformat(rng["gte"]).timestamp() * 1000 == 1774746000000


def test_date_histogram_tz_and_two_level_merge(planner):
    p = plan(planner, f'SELECT DATE_TRUNC(\'day\', "@timestamp_date") d, COUNT(*) FROM "{INDEX}" GROUP BY 1')
    s = p.scans[0]
    assert s.mode == "direct"
    src = s.keys[0][1].source["date_histogram"]
    assert src["calendar_interval"] == "1d" and src["time_zone"] == "Europe/Paris"
    # sub-day buckets in a DST zone are merged in DuckDB (repeated hour in October)
    p = plan(planner, f'SELECT DATE_TRUNC(\'hour\', "@timestamp_date") h, COUNT(*) FROM "{INDEX}" GROUP BY 1')
    assert p.scans[0].mode == "two-level"


def test_group_others_is_two_level(planner):
    sql = (f'SELECT CASE WHEN "APPLICATION" IN (\'BILLING\') THEN "APPLICATION" ELSE \'Others\' END AS app, '
           f'COUNT(*) FROM "{INDEX}" GROUP BY 1')
    p = plan(planner, sql)
    assert p.scans[0].mode == "two-level"
    assert [k.source for _, k in p.scans[0].keys] == [{"terms": {"field": "APPLICATION", "missing_bucket": True}}]
    assert "GROUP BY" in p.residual.sql(dialect="duckdb")


def test_case_sum_becomes_filter_subagg(planner):
    sql = f'SELECT SUM(CASE WHEN "STATUS_INFO" = \'FAILED\' THEN 1 ELSE 0 END) FROM "{INDEX}"'
    s = plan(planner, sql).scans[0]
    assert s.mode == "global"
    assert s.aggs == {"a0_f": {"filter": {"term": {"STATUS_INFO": "FAILED"}}}}


def test_text_field_uses_keyword_subfield(planner):
    s = plan(planner, f'SELECT "ERROR_EXCEPTION", COUNT(*) FROM "{INDEX}" GROUP BY 1').scans[0]
    assert s.keys[0][1].source["terms"]["field"] == "ERROR_EXCEPTION.keyword"


def test_virtual_dataset_is_flattened(planner):
    sql = (f'SELECT "APPLICATION", COUNT(*) AS c FROM (SELECT * FROM "{INDEX}" WHERE "SEVERITY" = \'HIGH\') '
           f'AS virtual_table WHERE "STATUS_INFO" = \'FAILED\' GROUP BY "APPLICATION"')
    p = plan(planner, sql)
    assert len(p.scans) == 1 and isinstance(p.scans[0], AggScan)
    assert p.scans[0].query == {"bool": {"filter": [{"term": {"SEVERITY": "HIGH"}},
                                                     {"term": {"STATUS_INFO": "FAILED"}}]}}


def test_early_termination_for_filter_values(planner):
    s = plan(planner, f'SELECT "ERROR_DESCRIPTION" FROM "{INDEX}" GROUP BY 1 ORDER BY 1 LIMIT 1000').scans[0]
    assert s.stop_after == 1000
    assert s.keys[0][1].source["terms"]["order"] == "asc"


def test_topn_mode(planner):
    sql = f'SELECT "NODE", SUM("JOB_DURATION_d") s FROM "{INDEX}" GROUP BY 1 ORDER BY s DESC LIMIT 10'
    assert plan(planner, sql).scans[0].mode == "direct"
    s = plan(planner, sql, topn="approx").scans[0]
    assert s.mode == "topn" and s.topn["order"][0] == {"a0_sum": "desc"}


def test_topn_with_limit_0_asks_opensearch_for_one_bucket(planner):
    sql = f'SELECT "NODE", SUM("JOB_DURATION_d") s FROM "{INDEX}" GROUP BY 1 ORDER BY s DESC LIMIT 0'
    s = plan(planner, sql, topn="approx").scans[0]
    assert s.mode == "topn" and s.topn["size"] == 1       # size 0: "[terms] failed to parse field [size]"


def test_raw_rows_push_sort_and_limit(planner):
    s = plan(planner, f'SELECT * FROM "{INDEX}" WHERE "APPLICATION" = \'ORDERS\' ORDER BY "@timestamp_date" DESC LIMIT 50').scans[0]
    assert isinstance(s, DocScan) and s.limit == 50
    assert s.sort == [{"@timestamp_date": {"order": "desc", "missing": "_last"}}]


def _fixtures():
    out = []
    for path in sorted(glob.glob(os.path.join(ROOT, "tests", "fixtures", "superset_sql", "*.sql"))):
        for i, stmt in enumerate(s for s in open(path).read().split("\n;\n") if s.strip()):
            out.append(pytest.param(stmt, id=f"{os.path.basename(path)[:-4]}#{i}"))
    return out


@pytest.mark.parametrize("sql", _fixtures())
def test_superset_fixture_fully_pushed_down(planner, sql):
    p = plan(planner, sql)
    for scan in p.scans:
        assert isinstance(scan, AggScan) or scan.limit is not None, scan.notes
        assert "FULL SCAN" not in " ".join(scan.notes)


@pytest.mark.parametrize("agg", ["SUM", "AVG", "STDDEV_SAMP", "VAR_POP"])
def test_numeric_aggregate_on_text_field_fails_fast_with_advice(planner, agg):
    """SUM("RUN_ID") on a keyword: no fallback scan, the message says to COUNT."""
    from osagg.errors import ProgrammingError

    sql = (f'SELECT DATE_TRUNC(\'hour\', "@timestamp_date") AS t, {agg}("APPLICATION") AS m '
           f'FROM "{INDEX}" WHERE "APPLICATION" IN (\'PAYROLL\') GROUP BY 1')
    with pytest.raises(ProgrammingError) as ei:
        plan(planner, sql)
    msg = str(ei.value)
    assert '"APPLICATION" is a text field' in msg and "COUNT(*)" in msg


def test_counting_documents_is_pushed_down(planner):
    for metric in ("COUNT(*)", 'COUNT("APPLICATION")', 'COUNT(DISTINCT "APPLICATION")'):
        p = plan(planner, f'SELECT DATE_TRUNC(\'hour\', "@timestamp_date") AS t, {metric} AS m '
                          f'FROM "{INDEX}" WHERE "APPLICATION" IN (\'PAYROLL\') GROUP BY 1')
        s = p.scans[0]       # hourly buckets in a DST zone: two-level (the repeated hour is merged)
        assert s.kind == "aggregation" and s.mode in ("direct", "two-level"), metric
        assert not any("FULL SCAN" in n for n in s.notes), s.notes


def test_document_pages_with_different_column_types_are_concatenated():
    import pyarrow as pa

    from osagg.executor import concat_pages

    ints = pa.table({"a": pa.array([1, 2], pa.int64()), "b": pa.array(["x", "y"])})
    floats = pa.table({"a": pa.array([1e30], pa.float64()), "b": pa.array(["z"])})
    t = concat_pages([ints, floats])
    assert t.num_rows == 3 and pa.types.is_floating(t.schema.field("a").type)
    texts = pa.table({"a": pa.array(["n/a"]), "b": pa.array(["w"])})
    t = concat_pages([ints, texts])
    assert t.column("a").to_pylist() == ["1", "2", "n/a"]


def test_a_query_of_many_aggregates_plans_in_linear_time(planner, monkeypatch):
    """Thirty aggregates in one grouped query (a comparison tool reading every measure at once): each one's key is
    computed a few times, not once per aggregate already seen (0.2.12: about 460 keys for 30 aggregates)."""
    from osagg import planner as P

    calls = []
    real = P._key
    monkeypatch.setattr(P, "_key", lambda node: calls.append(1) or real(node))
    aggs = ", ".join(f"SUM(\"JOB_DURATION_d\") FILTER (WHERE \"SEVERITY\" = 'S{i}') AS a{i}" for i in range(30))
    p = plan(planner, f'SELECT "APPLICATION", COUNT(*) AS n, {aggs} FROM jobs GROUP BY "APPLICATION"')
    assert isinstance(p.scans[0], AggScan) and len(calls) <= 10 * 32      # (0.2.12: more than 700)
    again = plan(planner, 'SELECT "APPLICATION", COUNT(*) AS n, COUNT(*) AS m, SUM("JOB_DURATION_d") AS s FROM jobs GROUP BY 1')
    assert isinstance(again.scans[0], AggScan)                      # (the same aggregate twice is still one)
