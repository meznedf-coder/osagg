"""Run scans against OpenSearch and turn the results into Arrow tables."""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc

from osagg.errors import OperationalError, PushdownError
from osagg.metadata import Field
from osagg.planner import AggScan, DocScan
from osagg.transport import Transport

logger = logging.getLogger(__name__)
MIN_PAGE = 500          # smallest composite page when OpenSearch is short of memory


def _short_of_memory(ex: Exception) -> bool:
    return any(s in str(ex) for s in ("circuit_breaking_exception", "Data too large"))

TRINO_MAX_DOCS = 10_000   # index.max_result_window; raw_query has no point-in-time

ARROW_TYPES = {
    "VARCHAR": pa.string(),
    "BIGINT": pa.int64(),
    "INTEGER": pa.int64(),
    "SMALLINT": pa.int64(),
    "TINYINT": pa.int64(),
    "DOUBLE": pa.float64(),
    "BOOLEAN": pa.bool_(),
    "TIMESTAMP": pa.timestamp("us"),
    "DATE": pa.date32(),
}


@dataclass
class ScanStats:
    table: str
    kind: str
    index: str
    requests: int = 0
    rows: int = 0
    os_took_ms: int = 0
    wall_ms: float = 0.0
    pages: list[int] = field(default_factory=list)


class Executor:
    def __init__(self, transport: Transport, tz: ZoneInfo, *, page_size: int = 50_000,
                 max_buckets_total: int = 2_000_000, max_scan_rows: int = 500_000,
                 max_rows: int = 0, doc_page_size: int = 10_000,
                 request_timeout: float = 300.0) -> None:
        self.transport = transport
        self.tz = tz
        self.page_size = page_size
        self.max_buckets_total = max_buckets_total
        self.max_scan_rows = max_scan_rows      # documents read without a LIMIT
        self.max_rows = max_rows                # optional cap on SELECT ... LIMIT n (0: none)
        self.doc_page_size = doc_page_size
        self.request_timeout = request_timeout

    # ------------------------------------------------------------------ #
    def run(self, scan: AggScan | DocScan) -> tuple[pa.Table, ScanStats]:
        t0 = time.perf_counter()
        if isinstance(scan, AggScan):
            table, stats = self._run_agg(scan)
        else:
            table, stats = self._run_docs(scan)
        stats.wall_ms = (time.perf_counter() - t0) * 1000
        logger.info("osagg scan %s on %s: %s rows, %s request(s), OpenSearch %sms, wall %.0fms",
                    stats.kind, stats.index, stats.rows, stats.requests, stats.os_took_ms,
                    stats.wall_ms)
        return table, stats

    # ------------------------------------------------------------------ #
    def _run_agg(self, scan: AggScan) -> tuple[pa.Table, ScanStats]:
        stats = ScanStats(scan.table, f"aggregation/{scan.mode}", scan.index)
        if scan.mode == "global":
            need_total = True
            body = {"size": 0, "track_total_hits": need_total, "query": scan.query}
            if scan.aggs:
                body["aggs"] = scan.aggs
            res = self.transport.search(scan.index, body, timeout=self.request_timeout)
            stats.requests += 1
            stats.os_took_ms += int(res.get("took", 0))
            bucket = {"doc_count": res["hits"]["total"]["value"], **(res.get("aggregations") or {})}
            buckets = [bucket]
        elif scan.mode == "topn":
            buckets = self._run_topn(scan, stats)
        else:
            max_buckets = self.transport.max_buckets()
            per_page = max(1, min(self.page_size, (max_buckets - 100) // (1 + scan.bucket_aggs)))
            if scan.heavy:
                # cardinality / percentiles keep sketches per bucket: keep pages small
                per_page = min(per_page, max(500, 10_000 // scan.heavy))
            if scan.stop_after is not None and scan.stop_dst_key is None:
                per_page = max(1, min(per_page, scan.stop_after))
            elif scan.stop_after is not None:
                per_page = max(scan.stop_after, min(per_page, 1000))
            comp: dict[str, Any] = {"size": per_page,
                                    "sources": [{n: k.source} for n, k in scan.keys]}
            agg_body: dict[str, Any] = {"composite": comp}
            if scan.aggs:
                agg_body["aggs"] = scan.aggs
            body = {"size": 0, "track_total_hits": False, "query": scan.query,
                    "aggs": {"g": agg_body}}
            buckets = []
            after = None
            while True:
                if after is not None:
                    comp["after"] = after
                try:
                    res = self.transport.search(scan.index, body, timeout=self.request_timeout)
                except OperationalError as ex:
                    # the cluster is short of heap: ask for the same page in smaller pieces
                    if not _short_of_memory(ex) or per_page <= MIN_PAGE:
                        raise
                    per_page = comp["size"] = max(MIN_PAGE, per_page // 4)
                    logger.warning("osagg: OpenSearch short of memory, pages of %d groups", per_page)
                    stats.requests += 1
                    continue
                stats.requests += 1
                stats.os_took_ms += int(res.get("took", 0))
                g = res["aggregations"]["g"]
                page = g.get("buckets", [])
                stats.pages.append(len(page))
                buckets.extend(page)
                if scan.stop_after is not None and len(buckets) >= scan.stop_after and (
                        scan.stop_dst_key is None
                        or self._dst_groups_complete(buckets, scan.stop_after, scan.stop_dst_key)):
                    break
                if len(buckets) > self.max_buckets_total:
                    raise PushdownError(
                        f"The query produces more than {self.max_buckets_total:,} groups. Add "
                        "filters, use fewer / lower-cardinality GROUP BY columns or a coarser "
                        "time grain.")
                after = g.get("after_key")
                if not page or after is None or len(page) < per_page:
                    break
        stats.rows = len(buckets)
        arrays = {}
        for col in scan.columns:
            values = [col.extract(b) for b in buckets]
            arrays[col.name] = _to_arrow(values, col.sql_type)
        return pa.table(arrays), stats

    def _dst_groups_complete(self, buckets: list[dict], n: int, key: str) -> bool:
        """The first n buckets (ordered by a local-time key) are complete groups when none
        of them is in the repeated autumn hour, or when the scan went an hour past them."""
        firsts = [b["key"].get(key) for b in buckets[:n]]
        if any(v is None for v in firsts):
            return False
        if not any(_ambiguous(v, self.tz) for v in firsts):
            return True
        last = buckets[-1]["key"].get(key)
        return last is not None and last >= max(firsts) + 3_600_000

    def _run_topn(self, scan: AggScan, stats: ScanStats) -> list[dict]:
        """Two-phase top-N.

        1. candidates: terms aggregation ordered by the metric (one pass, shard_size
           heuristics - may under-count terms spread over shards)
        2. exact values: the same aggregations restricted to the candidate keys, so
           every returned value is exact; only membership at the boundary is
           approximate.
        """
        from osagg.planner import NULL_KEY

        kname = scan.keys[0][0]
        terms = dict(scan.topn)
        field = terms["field"]
        size = terms["size"]
        order_path = next(iter(terms["order"][0]))
        order_aggs = {k: v for k, v in scan.aggs.items() if order_path.split(">")[0].split(".")[0] == k}
        p1 = dict(terms, size=min(10_000, 5 * size))
        body = {"size": 0, "track_total_hits": False, "query": scan.query,
                "aggs": {"g": {"terms": p1, **({"aggs": order_aggs} if order_aggs else {})}}}
        res = self.transport.search(scan.index, body, timeout=self.request_timeout)
        stats.requests += 1
        stats.os_took_ms += int(res.get("took", 0))
        cands = [b["key"] for b in res["aggregations"]["g"]["buckets"]]
        if not cands:
            return []
        real = [c for c in cands if c != NULL_KEY]
        parts = []
        if real:
            parts.append({"terms": {field: real}})
        if len(real) != len(cands):
            parts.append({"bool": {"must_not": [{"exists": {"field": field}}]}})
        restrict = parts[0] if len(parts) == 1 else {"bool": {"should": parts, "minimum_should_match": 1}}
        query = {"bool": {"filter": [scan.query, restrict]}}
        p2 = {"field": field, "size": len(cands), "shard_size": len(cands)}
        if "missing" in terms:
            p2["missing"] = terms["missing"]
        body = {"size": 0, "track_total_hits": False, "query": query,
                "aggs": {"g": {"terms": p2, **({"aggs": scan.aggs} if scan.aggs else {})}}}
        res = self.transport.search(scan.index, body, timeout=self.request_timeout)
        stats.requests += 1
        stats.os_took_ms += int(res.get("took", 0))
        out = []
        for b in res["aggregations"]["g"]["buckets"]:
            key = b.get("key")
            if key == NULL_KEY:
                key = None
            out.append({**b, "key": {kname: key}})
        return out

    # ------------------------------------------------------------------ #
    def _run_docs(self, scan: DocScan) -> tuple[pa.Table, ScanStats]:
        stats = ScanStats(scan.table, "documents", scan.index)
        fields = scan.fields
        src = [f.source_path for f in fields if f.source_path and not f.is_date]
        dates = [f for f in fields if f.is_date]
        base: dict[str, Any] = {"query": scan.query, "track_total_hits": False}
        base["_source"] = src if src else False
        if dates:
            base["docvalue_fields"] = [{"field": f.agg_field, "format": "epoch_millis"} for f in dates]

        target = scan.limit
        if target is None:
            total = self.transport.count(scan.index, scan.query)
            stats.requests += 1
            if total > self.max_scan_rows:
                raise PushdownError(
                    f"This query needs {total:,} raw documents from OpenSearch, above the safety cap "
                    f"of {self.max_scan_rows:,} rows. It could not be fully pushed down "
                    f"({'; '.join(scan.notes) or 'raw rows requested'}). Add a LIMIT, narrow the "
                    "filters, or rewrite it so that GROUP BY / aggregates can be pushed down "
                    "(see EXPLAIN); the cap is the max_scan_rows parameter of the connection.")
            target = total
        elif self.max_rows and target > self.max_rows:
            target = self.max_rows
            scan.notes.append(f"row cap: only the first {self.max_rows:,} rows are fetched")

        if self.transport.kind == "trino" and target > TRINO_MAX_DOCS:
            raise PushdownError(
                f"Through Trino, at most {TRINO_MAX_DOCS:,} raw documents can be read per query "
                f"(this one needs {target:,}). Add a LIMIT, or make the query aggregate so it can be "
                "pushed down, or use the direct OpenSearch transport.")
        # each page becomes Arrow columns right away: 500,000 rows never sit in memory as JSON
        tables: list[pa.Table] = []
        n_rows = 0
        if target == 0:
            pass
        elif target <= self.doc_page_size:
            body = dict(base, size=target)
            if scan.sort:
                body["sort"] = scan.sort
            res = self.transport.search(scan.index, body, timeout=self.request_timeout)
            stats.requests += 1
            stats.os_took_ms += int(res.get("took", 0))
            page = res["hits"]["hits"]
            if page:
                tables.append(self._page_table(fields, page))
                n_rows += len(page)
        else:
            pit = self.transport.open_pit(scan.index)
            if pit is None:
                raise PushdownError("deep pagination requires point-in-time support")
            try:
                search_after = None
                while n_rows < target:
                    body = dict(base, size=min(self.doc_page_size, target - n_rows),
                                pit={"id": pit, "keep_alive": "2m"},
                                sort=(scan.sort or []) + [{"_shard_doc": "asc"}])
                    if search_after is not None:
                        body["search_after"] = search_after
                    res = self.transport.search(scan.index, body, timeout=self.request_timeout)
                    stats.requests += 1
                    stats.os_took_ms += int(res.get("took", 0))
                    page = res["hits"]["hits"]
                    if not page:
                        break
                    tables.append(self._page_table(fields, page))
                    n_rows += len(page)
                    search_after = page[-1]["sort"]
                    pit = res.get("pit_id", pit)
            finally:
                self.transport.close_pit(pit)
        stats.rows = n_rows
        if not tables:
            return self._page_table(fields, []), stats
        return (tables[0] if len(tables) == 1 else concat_pages(tables)), stats

    def _page_table(self, fields: list[Field], hits: list[dict]) -> pa.Table:
        arrays = {}
        for f in fields:
            arrays[f.name] = self._doc_column(f, hits)
        if not fields:
            # e.g. SELECT COUNT(*) FROM idx WHERE <non-pushable>: need row count only
            arrays["__row"] = pa.array([1] * len(hits), pa.int64())
        return pa.table(arrays)

    def _doc_column(self, f: Field, hits: list[dict]) -> pa.Array:
        if f.name == "_id":
            return pa.array([h["_id"] for h in hits], pa.string())
        if f.is_date:
            ms = []
            for h in hits:
                v = (h.get("fields") or {}).get(f.agg_field)
                if not v:
                    ms.append(None)
                else:
                    x = v[0]
                    ms.append(int(x) if isinstance(x, (int, float)) or str(x).lstrip("-").isdigit()
                              else int(float(x)))
            arr = pa.array(ms, pa.int64()).cast(pa.timestamp("ms", tz="UTC"))
            arr = arr.cast(pa.timestamp("ms", tz=str(self.tz)))
            return pc.local_timestamp(arr).cast(pa.timestamp("us"))
        path = f.source_path or f.name
        values = [_source_get(h.get("_source") or {}, path) for h in hits]
        return _to_arrow([_normalize(v, f) for v in values], f.sql_type)


def _source_get(src: dict, path: str) -> Any:
    if path in src:
        return src[path]
    cur: Any = src
    parts = path.split(".")
    for i, p in enumerate(parts):
        if isinstance(cur, dict):
            if p in cur:
                cur = cur[p]
                continue
            rest = ".".join(parts[i:])
            if rest in cur:
                return cur[rest]
            return None
        if isinstance(cur, list):
            vals = [_source_get(x, ".".join(parts[i:])) for x in cur if isinstance(x, dict)]
            vals = [v for v in vals if v is not None]
            return vals or None
        return None
    return cur


def _normalize(v: Any, f: Field) -> Any:
    if v is None:
        return None
    if isinstance(v, list):
        if f.sql_type == "VARCHAR":
            if len(v) == 1:
                v = v[0]
            else:
                return json.dumps(v, ensure_ascii=False, default=str)
        else:
            v = v[0] if v else None
            if v is None:
                return None
    if f.sql_type == "VARCHAR":
        if isinstance(v, (dict,)):
            return json.dumps(v, ensure_ascii=False, default=str)
        return v if isinstance(v, str) else (str(v).lower() if isinstance(v, bool) else str(v))
    if f.sql_type == "BOOLEAN":
        if isinstance(v, str):
            return v.lower() == "true"
        return bool(v)
    if f.is_integer:
        try:
            return int(v)
        except (TypeError, ValueError):
            return None
    if f.sql_type == "DOUBLE":
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    return v


def _ambiguous(epoch_ms: Any, tz: ZoneInfo) -> bool:
    """Local wall time of this instant also occurs one hour earlier or later (DST fold)."""
    try:
        wall = dt.datetime.fromtimestamp(int(epoch_ms) / 1000, tz).replace(tzinfo=None)
    except (TypeError, ValueError, OverflowError):
        return True
    return wall.replace(tzinfo=tz, fold=0).utcoffset() != wall.replace(tzinfo=tz, fold=1).utcoffset()


def concat_pages(tables: list[pa.Table]) -> pa.Table:
    """Concatenate per-page tables. A column whose type differs between pages (an integer
    page that overflowed to float, values that only parse as text) is widened."""
    try:
        return pa.concat_tables(tables, promote_options="permissive")
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError):
        pass
    for name in tables[0].column_names:
        types = {t.schema.field(name).type for t in tables}
        if len(types) > 1:
            numeric = all(pa.types.is_integer(x) or pa.types.is_floating(x) or pa.types.is_null(x)
                          for x in types)
            target = pa.float64() if numeric else pa.string()
            tables = [t.set_column(t.schema.get_field_index(name), name, t[name].cast(target))
                      for t in tables]
    return pa.concat_tables(tables)


def _to_arrow(values: list[Any], sql_type: str) -> pa.Array:
    typ = ARROW_TYPES.get(sql_type, pa.string())
    try:
        return pa.array(values, typ)
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
        if sql_type in ("BIGINT", "INTEGER", "SMALLINT", "TINYINT"):
            return pa.array([None if v is None else float(v) for v in values], pa.float64())
        return pa.array([None if v is None else str(v) for v in values], pa.string())


def local_now(tz: ZoneInfo) -> dt.datetime:
    return dt.datetime.now(tz).replace(tzinfo=None)
