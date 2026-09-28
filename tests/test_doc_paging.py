"""Raw-document reads, offline with a fake transport: page by page, no cap with a LIMIT."""

from __future__ import annotations

from zoneinfo import ZoneInfo

import pytest

from osagg.errors import PushdownError
from osagg.executor import Executor
from osagg.metadata import Field
from osagg.planner import DocScan
from osagg.transport import Transport

FIELDS = [Field("APP", "keyword", "VARCHAR", "APP", "APP"),
          Field("N", "long", "BIGINT", "N", "N")]


class FakeTransport(Transport):
    kind = "direct"

    def __init__(self, n_docs: int) -> None:
        self.docs = [{"_id": str(i), "_source": {"APP": f"a{i % 7}", "N": i}, "sort": [i]}
                     for i in range(n_docs)]
        self.sizes: list[int] = []

    def search(self, index, body, timeout=None):
        start = body.get("search_after", [-1])[0] + 1
        page = self.docs[start:start + body["size"]]
        self.sizes.append(len(page))
        return {"took": 1, "hits": {"hits": page}, "pit_id": "pit"}

    def count(self, index, query):
        return len(self.docs)

    def open_pit(self, index, keep_alive="2m"):
        return "pit"

    def close_pit(self, pit_id):
        return None


def run(n_docs, limit, **kw):
    tr = FakeTransport(n_docs)
    ex = Executor(tr, ZoneInfo("Europe/Paris"), doc_page_size=1000, **kw)
    table, stats = ex.run(DocScan("t", "idx", {"match_all": {}}, FIELDS, None, limit))
    return table, stats, tr


def test_limit_above_the_scan_cap_is_read_page_by_page():
    table, stats, tr = run(7_500, limit=7_500, max_scan_rows=1_000)
    assert table.num_rows == 7_500 and stats.rows == 7_500
    assert tr.sizes == [1000] * 7 + [500]                 # 8 pages of at most doc_page_size
    assert table.column("N").to_pylist() == list(range(7_500))


def test_optional_max_rows_caps_a_limit():
    table, _, _ = run(5_000, limit=5_000, max_rows=2_500)
    assert table.num_rows == 2_500


def test_without_limit_the_scan_cap_applies():
    with pytest.raises(PushdownError, match="max_scan_rows"):
        run(5_000, limit=None, max_scan_rows=1_000)
    table, _, _ = run(900, limit=None, max_scan_rows=1_000)
    assert table.num_rows == 900
