"""Fields mapped in ways that changed the counts (no OpenSearch needed: the generated DSL and the
metadata). Business dates stored as dates (yyyyMMdd, basic_date, or "2026-10-01" in OpenSearch's default
format) are calendar days read in UTC, whatever the connection's zone; a number like 20261001 is read
with the field's own format; a field the indices of a pattern map differently is compared per group of
indices, each with its own exact field and type (one index's ".keyword" sub-field, missing in the others,
made their documents match nothing: a count of 10 for 1,210)."""

from __future__ import annotations

import datetime as dt
import json
from zoneinfo import ZoneInfo

import pytest
import sqlglot

from osagg import duck
from osagg.errors import ProgrammingError
from osagg.metadata import (ID_FIELD, TableMeta, fields_from_mapping, format_is_date_only,
                            probe_midnight_dates, strptime_pattern)
from osagg.planner import Planner, Settings


def ms(day: str, tz: str = "UTC") -> int:
    d = dt.datetime.fromisoformat(day).replace(tzinfo=ZoneInfo(tz))
    return int(d.timestamp() * 1000)


def table(per_index: dict[str, dict]) -> TableMeta:
    resp = {ix: {"mappings": {"properties": props}} for ix, props in per_index.items()}
    fields = fields_from_mapping(resp)
    fields["_id"] = ID_FIELD
    return TableMeta("jobs-*", sorted(per_index), fields)


def planner(meta: TableMeta, tz: str = "Europe/Paris") -> Planner:
    con = duck.locked_session(tz)
    return Planner(lambda name: meta if name == "jobs-*" else None, Settings(tz=ZoneInfo(tz)),
                   lambda node: con.execute("SELECT " + node.sql(dialect="duckdb")).fetchone()[0])


def query_of(meta: TableMeta, where: str, tz: str = "Europe/Paris") -> dict:
    p = planner(meta, tz).plan(sqlglot.parse_one(f'SELECT COUNT(*) FROM "jobs-*" WHERE {where}', read="duckdb"))
    return p.scans[0].query


DAYS = {"jobs-a": {"POSITION_DATE": {"type": "date", "format": "yyyyMMdd"}, "STATUS_INFO": {"type": "keyword"},
                   "@timestamp_date": {"type": "date"}, "RUNS": {"type": "long"}}}


def test_the_formats_that_say_a_day():
    for fmt, day_only, pattern in (("yyyyMMdd", True, "%Y%m%d"), ("basic_date", True, "%Y%m%d"),
                                   ("yyyyMMdd||epoch_millis", True, "%Y%m%d"), ("strict_date", True, "%Y-%m-%d"),
                                   ("dd/MM/yyyy", True, "%d/%m/%Y"), (None, False, None),
                                   ("strict_date_optional_time||epoch_millis", False, None),
                                   ("yyyy-MM-dd HH:mm:ss", False, None), ("epoch_millis", False, None),
                                   ("yyyy-MM-dd'T'HH:mm", False, None), ("date_time", False, None)):
        assert (format_is_date_only(fmt), strptime_pattern(fmt)) == (day_only, pattern), fmt


@pytest.mark.parametrize("tz", ["Europe/Paris", "America/New_York", "Asia/Tokyo"])
def test_a_business_date_stored_as_a_date_is_a_day_in_any_zone(tz):
    """POSITION_DATE = 20261001 was read as epoch milliseconds (1970), '2026-10-01' as midnight in the
    connection's zone (22:00 UTC the day before in Paris): both counted nothing."""
    meta = table(DAYS)
    day = ms("2026-10-01")
    eq = {"range": {"POSITION_DATE": {"gte": day, "lte": day, "format": "epoch_millis"}}}
    for lit in ("20261001", "'20261001'", "'2026-10-01'", "DATE '2026-10-01'"):
        assert query_of(meta, f'"POSITION_DATE" = {lit}', tz) == eq, lit
    assert query_of(meta, "\"POSITION_DATE\" <= '2026-09-30'", tz) == {
        "range": {"POSITION_DATE": {"lte": ms("2026-09-30"), "format": "epoch_millis"}}}
    assert query_of(meta, "\"POSITION_DATE\" > 20260930", tz) == {
        "range": {"POSITION_DATE": {"gt": ms("2026-09-30"), "format": "epoch_millis"}}}
    # a timestamp keeps the connection's zone
    assert query_of(meta, "\"@timestamp_date\" >= '2026-10-01'", tz) == {
        "range": {"@timestamp_date": {"gte": ms("2026-10-01", tz), "format": "epoch_millis"}}}


def test_a_day_grouped_and_shown_at_midnight():
    meta = table(DAYS)
    p = planner(meta, "America/New_York").plan(sqlglot.parse_one(
        'SELECT CAST("POSITION_DATE" AS DATE) AS d, COUNT(*) FROM "jobs-*" GROUP BY 1', read="duckdb"))
    body = json.dumps(p.scans[0].describe())
    assert '"time_zone": "UTC"' in body                     # not New York: the day stays the day


def test_a_number_or_a_text_where_a_type_cannot_take_it_says_so():
    meta = table(DAYS)
    with pytest.raises(ProgrammingError, match="is a number"):
        query_of(meta, "\"RUNS\" = DATE '2026-10-01'")
    with pytest.raises(ProgrammingError, match="is a number"):
        query_of(meta, "\"RUNS\" = '2026-10-01'")
    with pytest.raises(ProgrammingError, match="is not a day"):
        query_of(meta, '"POSITION_DATE" = 123')


MIXED = {"jobs-a": {"STATUS_INFO": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "POSITION_DATE": {"type": "date", "format": "yyyyMMdd"}},
         "jobs-b": {"STATUS_INFO": {"type": "keyword"}, "POSITION_DATE": {"type": "keyword"}},
         "jobs-c": {"STATUS_INFO": {"type": "keyword"}, "POSITION_DATE": {"type": "long"}}}


def test_a_field_the_indices_map_differently_keeps_each_view():
    meta = table(MIXED)
    st = meta.fields["STATUS_INFO"]
    assert st.agg_field is None and st.sql_type == "VARCHAR"
    assert [(ix, v.agg_field) for ix, v in st.variants] == [(("jobs-a",), "STATUS_INFO.keyword"),
                                                            (("jobs-b", "jobs-c"), "STATUS_INFO")]
    pd = meta.fields["POSITION_DATE"]
    assert pd.os_type == "mixed" and [v.os_type for _, v in pd.variants] == ["date", "keyword", "long"]


def test_each_group_of_indices_is_compared_as_it_maps_the_field():
    meta = table(MIXED)
    q = query_of(meta, "\"STATUS_INFO\" = 'KO'")
    assert q == {"bool": {"should": [
        {"bool": {"filter": [{"terms": {"_index": ["jobs-a"]}}, {"term": {"STATUS_INFO.keyword": "KO"}}]}},
        {"bool": {"filter": [{"terms": {"_index": ["jobs-b", "jobs-c"]}}, {"term": {"STATUS_INFO": "KO"}}]}}],
        "minimum_should_match": 1}}
    q = json.dumps(query_of(meta, '"POSITION_DATE" = 20261001'))
    day = ms("2026-10-01")
    assert f'"gte": {day}, "lte": {day}' in q and '"term": {"POSITION_DATE": "20261001"}' in q \
        and '"term": {"POSITION_DATE": 20261001}' in q                # the date, the keyword, the long
    with pytest.raises(ProgrammingError, match="is a number"):         # the long cannot be a date
        query_of(meta, "\"POSITION_DATE\" = DATE '2026-10-01'")


def test_grouping_on_text_here_keyword_there_reads_each_index_exact_field():
    meta = table(MIXED)
    p = planner(meta).plan(sqlglot.parse_one('SELECT "STATUS_INFO", COUNT(*) FROM "jobs-*" GROUP BY 1',
                                             read="duckdb"))
    src = p.scans[0].keys[0][1].source["terms"]
    assert src["script"]["params"]["fields"] == ["STATUS_INFO.keyword", "STATUS_INFO"]
    assert src["value_type"] == "string"


def test_conditions_applied_per_index_are_not_compared_again_in_duckdb():
    meta = table(MIXED)
    p = planner(meta).plan(sqlglot.parse_one('SELECT "POSITION_DATE" FROM "jobs-*" WHERE "POSITION_DATE" = 20261001',
                                             read="duckdb"))
    assert "WHERE" not in p.residual.sql(dialect="duckdb").upper()  # the raw values have no single type


class FakeTransport:
    kind = "direct"

    def __init__(self, keys: dict[str, list[int]]):
        self.keys, self.bodies = keys, []

    def search(self, index, body, timeout=None):
        self.bodies.append(body)
        aggs = {}
        for name, agg in body["aggs"].items():
            if name == "s":
                aggs["s"] = {k: {"buckets": [{"key": v} for v in self.keys[a["terms"]["field"]]]}
                             for k, a in agg["aggs"].items()}
            else:
                field = next(iter(agg.values()))["field"]
                vals = self.keys[field]
                aggs[name] = {"value": min(vals) if name.startswith("mn") else max(vals)}
        return {"aggregations": aggs}


def test_a_default_format_date_holding_only_days_is_read_as_days():
    meta = table({"cob": {"COB_DATE": {"type": "date"}, "TS": {"type": "date"}}})
    day = ms("2026-09-23")
    t = FakeTransport({"COB_DATE": [day, day + 86_400_000, day + 2 * 86_400_000],
                       "TS": [day, day + 3_600_000, day + 86_400_000]})
    fields = probe_midnight_dates(t, "cob", meta.fields)
    assert fields["COB_DATE"].date_only and not fields["TS"].date_only


def test_the_probe_of_a_field_mapped_differently_is_per_group():
    meta = table({"a": {"D": {"type": "date"}}, "b": {"D": {"type": "keyword"}}})
    day = ms("2026-09-23")
    t = FakeTransport({"D": [day, day, day + 86_400_000]})
    fields = probe_midnight_dates(t, "a,b", meta.fields)
    assert t.bodies[0]["query"] == {"terms": {"_index": ["a"]}}
    assert [v.date_only for _, v in fields["D"].variants] == [True, False]


def _planner_with(meta: TableMeta, **kw) -> Planner:
    con = duck.locked_session("Europe/Paris")
    return Planner(lambda name: meta if name == "jobs-*" else None, Settings(tz=ZoneInfo("Europe/Paris"), **kw),
                   lambda node: con.execute("SELECT " + node.sql(dialect="duckdb")).fetchone()[0])


def test_count_distinct_is_exact_unless_asked_for_an_estimate():
    """COUNT(DISTINCT x) was a cardinality sketch (an estimate above 3,000 values). Now a sketch while it is
    exact; reaching its threshold, the query is counted again with x as a key (DuckDB counts the keys);
    count_distinct=approx keeps the estimate."""
    from osagg.executor import NotExact, _check_exact

    meta = table(DAYS)
    sql = sqlglot.parse_one('SELECT COUNT(DISTINCT "STATUS_INFO") FROM "jobs-*"', read="duckdb")
    first = _planner_with(meta).plan(sql)
    assert [n for n in first.scans[0].aggs] == ["a0_xcard"]
    with pytest.raises(NotExact):
        _check_exact(first.scans[0], [{"doc_count": 9, "a0_xcard": {"value": 3000}}])
    _check_exact(first.scans[0], [{"doc_count": 9, "a0_xcard": {"value": 2999}}])      # exact below
    exact = _planner_with(meta, exact_distinct=True).plan(sql)
    assert [n for n, _ in exact.scans[0].keys] == ["k0"] and not exact.scans[0].aggs
    assert 'COUNT(DISTINCT "k0")' in exact.residual.sql(dialect="duckdb")
    approx = _planner_with(meta, count_distinct="approx").plan(sql)
    assert [n for n in approx.scans[0].aggs] == ["a0_card"]
    grouped = _planner_with(meta, exact_distinct=True).plan(sqlglot.parse_one(
        'SELECT "STATUS_INFO", COUNT(DISTINCT "RUNS"), COUNT(*) FROM "jobs-*" GROUP BY 1', read="duckdb"))
    assert len(grouped.scans[0].keys) == 2 and "SUM(" in grouped.residual.sql(dialect="duckdb")
