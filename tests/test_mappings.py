"""Fields mapped in ways that changed the counts (no OpenSearch needed: the generated DSL and the
metadata). Business dates stored as dates (yyyyMMdd, basic_date, or "2026-10-01" in OpenSearch's default
format) are calendar days read in UTC, whatever the connection's zone; a number like 20261001 is read
with the field's own format; a field the indices of a pattern map differently is compared per group of
indices, each with its own exact field and type (one index's ".keyword" sub-field, missing in the others,
made their documents match nothing: a count of 10 for 1,210)."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
from zoneinfo import ZoneInfo

import pytest
import sqlglot

from osagg import duck
from osagg.errors import ProgrammingError
from osagg.metadata import (ID_FIELD, TableMeta, fields_from_mapping, format_is_date_only,
                            probe_long_values, probe_midnight_dates, strptime_pattern)
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
        self.keys, self.bodies, self.targets = keys, [], []

    def search(self, index, body, timeout=None):
        self.bodies.append(body)
        self.targets.append(index)
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
    assert t.targets == ["a"]          # only the indices that map it as a date: elsewhere a text fails the shards
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


LONG = {"jobs-a": {"MSG": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 32}}},
                   "RUNS": {"type": "long"}}}
A_LONG = "java.lang.IllegalStateException: pricing grid is empty"     # 55 characters: not in MSG.keyword


def long_table(probed: bool = True) -> TableMeta:
    meta = table(LONG)
    if probed:
        meta.fields["MSG"] = dataclasses.replace(meta.fields["MSG"], long_values=True)
    return meta


def plan_of(meta: TableMeta, sql: str):
    return planner(meta).plan(sqlglot.parse_one(sql, read="duckdb"))


class CountingTransport:
    kind = "direct"

    def __init__(self, counts: list[int]):
        self.counts, self.bodies = counts, []

    def search(self, index, body, timeout=None):
        self.bodies.append(body)
        return {"aggregations": {n: {"doc_count": c} for n, c in zip(body["aggs"], self.counts)}}


def test_a_text_whose_keyword_has_ignore_above_is_probed_for_longer_values():
    """OpenSearch's dynamic mapping gives text fields a keyword with ignore_above 256: a longer value (an
    error description, a stack trace) has the text but no keyword, which every exact filter, group and
    count read."""
    meta = table(LONG)
    assert meta.fields["MSG"].exact_max == 32 and not meta.fields["MSG"].long_values
    t = CountingTransport([3])
    probed = probe_long_values(t, "jobs-*", meta.fields)
    assert probed["MSG"].long_values
    assert t.bodies[0]["aggs"]["l0"] == {"filter": {"bool": {"filter": [{"exists": {"field": "MSG"}}],
                                                             "must_not": [{"exists": {"field": "MSG.keyword"}}]}}}
    assert not probe_long_values(CountingTransport([0]), "jobs-*", meta.fields)["MSG"].long_values


def test_values_longer_than_the_keyword_are_read_from_the_documents():
    meta = long_table()
    # equal to a long value: the documents whose text holds it, compared whole in DuckDB
    p = plan_of(meta, f"SELECT COUNT(*) FROM \"jobs-*\" WHERE \"MSG\" = '{A_LONG}'")
    assert p.scans[0].kind == "documents" and "match_phrase" in json.dumps(p.scans[0].query)
    assert '"must_not": [{"exists": {"field": "MSG.keyword"}}]' in json.dumps(p.scans[0].query)
    assert "WHERE" in p.residual.sql(dialect="duckdb").upper()
    # equal to a short value: the keyword holds every one, counted by OpenSearch
    p = plan_of(meta, "SELECT COUNT(*) FROM \"jobs-*\" WHERE \"MSG\" = 'OK'")
    assert p.scans[0].kind == "aggregation" and p.scans[0].query == {"term": {"MSG.keyword": "OK"}}
    # grouped: the long values are not keys of the keyword
    p = plan_of(meta, 'SELECT "MSG", COUNT(*) FROM "jobs-*" GROUP BY 1')
    assert p.scans[0].kind == "documents"
    # LIKE: the keyword's matches and every long row, DuckDB decides
    p = plan_of(meta, "SELECT COUNT(*) FROM \"jobs-*\" WHERE \"MSG\" LIKE '%pricing%'")
    q = json.dumps(p.scans[0].query)
    assert p.scans[0].kind == "documents" and "wildcard" in q and "must_not" in q
    # COUNT(DISTINCT) of every value: from the documents
    assert plan_of(meta, 'SELECT COUNT(DISTINCT "MSG") FROM "jobs-*"').scans[0].kind == "documents"


def test_null_and_count_of_a_text_include_its_long_values_probed_or_not():
    either = {"bool": {"should": [{"exists": {"field": "MSG.keyword"}}, {"exists": {"field": "MSG"}}],
                       "minimum_should_match": 1}}
    for probed in (True, False):
        meta = long_table(probed)
        assert query_of(meta, '"MSG" IS NULL') == {"bool": {"must_not": [either]}}
        p = plan_of(meta, 'SELECT COUNT("MSG") FROM "jobs-*"')
        assert p.scans[0].kind == "aggregation" and p.scans[0].aggs == {"a0_has": {"filter": either}}


def test_a_long_literal_is_looked_for_even_before_the_probe_saw_one():
    """The metadata (and its probe) is kept 5 minutes: a long value is still found the minute it arrives."""
    meta = long_table(probed=False)
    p = plan_of(meta, f"SELECT COUNT(*) FROM \"jobs-*\" WHERE \"MSG\" IN ('OK', '{A_LONG}')")
    q = json.dumps(p.scans[0].query)
    assert p.scans[0].kind == "documents" and '"term": {"MSG.keyword": "OK"}' in q and "match_phrase" in q
    # ignore_above counts UTF-16 units as Java does: 20 emoji are 40 of them
    p = plan_of(meta, "SELECT COUNT(*) FROM \"jobs-*\" WHERE \"MSG\" = '" + "\U0001F600" * 20 + "'")
    assert p.scans[0].kind == "documents"
    # not probed, nothing long asked: the keyword as before
    assert plan_of(meta, 'SELECT "MSG", COUNT(*) FROM "jobs-*" GROUP BY 1').scans[0].kind == "aggregation"


def test_a_text_long_in_some_indices_of_a_pattern_is_probed_and_compared_per_group():
    meta = table({"jobs-a": LONG["jobs-a"], "jobs-b": {"MSG": {"type": "keyword"}}})
    t = CountingTransport([2])
    fields = probe_long_values(t, "jobs-*", meta.fields)
    assert {"terms": {"_index": ["jobs-a"]}} in t.bodies[0]["aggs"]["l0"]["filter"]["bool"]["filter"]
    assert fields["MSG"].long_values and [v.long_values for _, v in fields["MSG"].variants] == [True, False]
    meta.fields["MSG"] = fields["MSG"]
    p = plan_of(meta, f"SELECT COUNT(*) FROM \"jobs-*\" WHERE \"MSG\" = '{A_LONG}'")
    q = json.dumps(p.scans[0].query)
    assert p.scans[0].kind == "documents" and '"_index": ["jobs-a"]' in q and '"_index": ["jobs-b"]' in q
    assert plan_of(meta, 'SELECT "MSG", COUNT(*) FROM "jobs-*" GROUP BY 1').scans[0].kind == "documents"


def test_an_answer_from_part_of_the_shards_is_an_error():
    """OpenSearch answers 200 with what the shards it could read hold when others fail: counts from part of
    the data, nothing in the answer says so but _shards."""
    from osagg.transport import PartialResults, check_complete

    ok = {"_shards": {"total": 5, "successful": 5, "skipped": 2, "failed": 0}, "timed_out": False}
    assert check_complete(ok, "jobs-*") is ok
    failed = {"_shards": {"total": 5, "successful": 4, "failed": 1, "failures": [
        {"shard": 3, "index": "jobs-b", "reason": {"type": "node_not_connected_exception", "reason": "gone"}}]}}
    with pytest.raises(PartialResults, match="1 of the 5 shards of jobs-\\* failed: node_not_connected") as ex:
        check_complete(failed, "jobs-*")
    assert not ex.value.busy
    busy = {"_shards": {"total": 2, "failed": 1, "failures": [{"reason": {
        "type": "es_rejected_execution_exception", "reason": "rejected execution"}}]}}
    with pytest.raises(PartialResults) as ex:
        check_complete(busy, "jobs-*")
    assert ex.value.busy
    with pytest.raises(PartialResults, match="timed out"):
        check_complete({"_shards": {"total": 1, "failed": 0}, "timed_out": True}, "jobs-*")


def test_the_direct_transport_asks_for_all_shards_and_retries_a_busy_one(monkeypatch):
    from osagg import transport as T

    monkeypatch.setattr(T.time, "sleep", lambda s: None)
    answers = [{"_shards": {"total": 2, "failed": 1, "failures": [{"reason": {
        "type": "es_rejected_execution_exception", "reason": "queue full"}}]}, "hits": {}},
               {"_shards": {"total": 2, "failed": 0}, "hits": {"total": {"value": 7}}}]
    seen = []

    class Client:
        def search(self, **kw):
            seen.append(kw["params"])
            return answers.pop(0)

    t = T.DirectTransport()
    t.client = Client()
    assert t.search("jobs-*", {"size": 0})["hits"]["total"]["value"] == 7
    assert len(seen) == 2 and all(p["allow_partial_search_results"] == "false" for p in seen)


def test_a_point_in_time_is_created_on_every_shard_or_not_at_all():
    """Deep reads page through a point in time: one created on part of the shards answers every page with
    "0 failed" while documents are missing."""
    from osagg import transport as T

    seen, closed = [], []

    class Client:
        def __init__(self, answer):
            self.answer = answer

        def create_pit(self, **kw):
            seen.append(kw["params"])
            return self.answer

        def delete_pit(self, body):
            closed.append(body)

    t = T.DirectTransport()
    t.client = Client({"pit_id": "p1", "_shards": {"total": 3, "successful": 3, "failed": 0}})
    assert t.open_pit("jobs-*") == "p1" and seen[0]["allow_partial_pit_creation"] == "false"
    t.client = Client({"pit_id": "p2", "_shards": {"total": 3, "successful": 2, "failed": 1, "failures": [
        {"reason": {"type": "node_disconnected_exception", "reason": "gone"}}]}})
    with pytest.raises(T.PartialResults, match="1 of the 3 shards"):
        t.open_pit("jobs-*")
    assert closed == [{"pit_id": ["p2"]}]
