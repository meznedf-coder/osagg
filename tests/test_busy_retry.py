"""OpenSearch answers 429 / circuit_breaking_exception when it is short of heap for a
moment (several dashboard charts at once on a small node): osagg retries with a back-off,
and gives up with the original error. No cluster needed."""

from __future__ import annotations

import pytest
from opensearchpy.exceptions import RequestError, TransportError

import osagg
from osagg import transport


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr(transport, "BUSY_RETRY_DELAYS", (0.0, 0.0, 0.0))


def breaker():
    return TransportError(429, "circuit_breaking_exception",
                          {"error": {"type": "circuit_breaking_exception", "reason": "[parent] Data too large"}})


def test_retried_until_opensearch_has_room():
    calls = []

    def search():
        calls.append(1)
        if len(calls) < 3:
            raise breaker()
        return {"hits": {}}

    assert transport.DirectTransport.__new__(transport.DirectTransport)._call(search) == {"hits": {}}
    assert len(calls) == 3


def test_gives_up_with_the_error():
    calls = []

    def search():
        calls.append(1)
        raise breaker()

    with pytest.raises(osagg.OperationalError, match="Data too large"):
        transport.DirectTransport.__new__(transport.DirectTransport)._call(search)
    assert len(calls) == 1 + len(transport.BUSY_RETRY_DELAYS)


def test_bad_queries_are_not_retried():
    calls = []

    def search():
        calls.append(1)
        raise RequestError(400, "parsing_exception", {"error": "unknown field"})

    with pytest.raises(osagg.ProgrammingError):
        transport.DirectTransport.__new__(transport.DirectTransport)._call(search)
    assert len(calls) == 1


def test_composite_pages_shrink_when_opensearch_is_short_of_memory():
    """A page refused by the circuit breaker is asked again in smaller pieces; the result
    is complete (paging goes on from the same after_key)."""
    import pyarrow as pa  # noqa: F401

    from osagg.executor import MIN_PAGE, Executor
    from osagg.planner import AggScan, OutCol
    from osagg.translate import GroupKey

    rows = [{"key": {"k0": f"v{i:05d}"}, "doc_count": 1} for i in range(3000)]
    sizes = []

    class Fake:
        def max_buckets(self):
            return 65535

        def search(self, index, body, timeout=None):
            comp = body["aggs"]["g"]["composite"]
            sizes.append(comp["size"])
            if comp["size"] > 1000:
                raise osagg.OperationalError("OpenSearch error: circuit_breaking_exception: [parent] "
                                             "Data too large")
            start = 0
            if comp.get("after"):
                start = next(i for i, r in enumerate(rows) if r["key"] == comp["after"]) + 1
            page = rows[start:start + comp["size"]]
            g = {"buckets": page}
            if page:
                g["after_key"] = page[-1]["key"]
            return {"took": 1, "aggregations": {"g": g}}

    key = GroupKey(source={"terms": {"field": "k"}}, sql_type="VARCHAR", kind="terms")
    scan = AggScan(table="t", index="i", query={"match_all": {}}, keys=[("k0", key)], aggs={},
                   columns=[OutCol("k", "VARCHAR", lambda b: b["key"]["k0"])], mode="direct")
    table, stats = Executor(Fake(), __import__("zoneinfo").ZoneInfo("UTC"), page_size=50_000).run(scan)
    assert table.num_rows == 3000 and table.column("k")[2999].as_py() == "v02999"
    assert sizes[:4] == [50_000, 12_500, 3_125, 781] and min(sizes) >= MIN_PAGE
