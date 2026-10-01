"""PEP 249 (DB-API 2.0) interface of osagg."""

from __future__ import annotations

import datetime as dt
import decimal
import fnmatch
import json
import logging
import re
import threading
import time
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

import sqlglot
from sqlglot import exp

from osagg import duck
from osagg.errors import (  # noqa: F401  (re-exported)
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    PushdownError,
    Warning,
)
from osagg.executor import Executor, NotExact
from osagg import calendar
from osagg.metadata import CACHE, TableMeta, matches_any, visible_tables, with_label_column
from osagg.planner import Plan, Planner, Settings
from osagg.transport import DirectTransport, Transport, TrinoTransport

logger = logging.getLogger(__name__)

_ENUM_CACHE: dict[tuple, tuple[float, list | None]] = {}

apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"

# type objects
STRING = "VARCHAR"
NUMBER = "DOUBLE"
DATETIME = "TIMESTAMP"
BINARY = "BLOB"
ROWID = "VARCHAR"


def _bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).lower() in ("1", "true", "yes", "on")


def connect(host: str = "localhost", port: int | None = None, user: str | None = None,
            password: str | None = None, **kwargs: Any) -> "Connection":
    """Open a connection.

    Keyword arguments (all optional):
      scheme / use_ssl, verify_certs, ca_certs, client_cert, client_key, url_prefix,
      timezone (default UTC), transport ("direct" | "trino"),
      trino_host, trino_port, trino_user, trino_password, trino_catalog, trino_schema,
      trino_http_scheme, max_scan_rows, max_rows, max_buckets_total, page_size,
      cardinality_precision, request_timeout, tables (extra index patterns, comma sep).
    """
    return Connection(host=host, port=port, user=user, password=password, **kwargs)


class Connection:
    def __init__(self, host: str = "localhost", port: int | None = None, user: str | None = None,
                 password: str | None = None, **kw: Any) -> None:
        self.cfg = dict(kw)
        self.closed = False
        tz_name = kw.get("timezone") or kw.get("tz") or "UTC"
        try:
            self.tz = ZoneInfo(tz_name)
        except Exception as ex:  # pylint: disable=broad-except
            raise InterfaceError(f"unknown time zone {tz_name!r}") from ex
        self.tz_name = tz_name
        self.max_scan_rows = int(kw.get("max_scan_rows", 500_000))   # reads without a LIMIT
        self.max_rows = int(kw.get("max_rows", 0))                    # SELECT ... LIMIT n: no cap
        self.max_buckets_total = int(kw.get("max_buckets_total", 2_000_000))
        self.page_size = int(kw.get("page_size", 50_000))
        self.doc_page_size = int(kw.get("doc_page_size", 10_000))
        self.cardinality_precision = int(kw.get("cardinality_precision", 3_000))
        self.percentile_compression = int(kw.get("percentile_compression", 500))
        self.topn = str(kw.get("topn", "exact")).lower()
        self.enum_max_values = int(kw.get("enum_max_values", 10_000))
        # date fields whose values are all at 00:00 UTC are calendar days (read in UTC): checked once
        # per table (metadata.probe_midnight_dates); date_probe=false leaves it to their format alone
        self.date_probe = str(kw.get("date_probe", "true")).lower() in ("1", "true", "yes")
        # COUNT(DISTINCT): exact (a sketch while it is exact, else the values as keys), or approx (sketches)
        self.count_distinct = "approx" if str(kw.get("count_distinct", "exact")).lower() == "approx" else "exact"
        self.enum_cache_ttl = float(kw.get("enum_cache_ttl", 60))
        self.join_max_keys = int(kw.get("join_max_keys", 100_000))
        # row lists over a join (one big index + small ones): off, set only by extract tools
        self.lookup_joins = str(kw.get("lookup_joins", "false")).lower() in ("1", "true", "yes")
        # business-date label column (business calendar): POSITION_LABEL computed from POSITION_DATE
        src = str(kw.get("label_source", "POSITION_DATE")).strip()
        self.label_source = None if src.lower() in ("", "none", "off", "false") else src
        self.label_column = str(kw.get("label_column", "POSITION_LABEL")).strip()
        self.label_time_source = str(kw.get("label_time_source", "@timestamp_date")).strip()
        self.label_time_column = str(kw.get("label_time_column", "POSITION_TIME")).strip()
        self.label_date_format = str(kw.get("label_date_format", "%Y%m%d")).strip()
        hh, mm = str(kw.get("label_cutoff", "14:00")).split(":")
        self.label_cutoff = dt.time(int(hh), int(mm))
        self.label_years = _bool(kw.get("label_years"), True)
        # "now" of the calendar: the Superset process clock (like datetime.now() in
        # superset_config.py) unless label_timezone names a zone; label_now pins it (tests)
        ltz = str(kw.get("label_timezone", "local")).strip()
        try:
            self.label_tz = None if ltz.lower() in ("", "local") else ZoneInfo(ltz)
        except Exception as ex:  # pylint: disable=broad-except
            raise InterfaceError(f"unknown label time zone {ltz!r}") from ex
        self.label_now = dt.datetime.fromisoformat(kw["label_now"]) if kw.get("label_now") else None
        self.request_timeout = float(kw.get("request_timeout", 300))
        self.extra_tables = [t for t in str(kw.get("tables", "")).split(",") if t]
        self.include_hidden = _bool(kw.get("include_hidden"), False)
        kind = kw.get("transport", "direct")
        if kind == "trino":
            self.transport: Transport = TrinoTransport(
                host=kw.get("trino_host") or host,
                port=int(kw.get("trino_port") or port or 8080),
                user=kw.get("trino_user") or user or "osagg",
                password=kw.get("trino_password") or password,
                catalog=kw.get("trino_catalog", "opensearch"),
                schema=kw.get("trino_schema", "default"),
                http_scheme=kw.get("trino_http_scheme", "http"),
                verify=_bool(kw.get("verify_certs"), True),
                request_timeout=self.request_timeout,
                max_buckets=int(kw.get("max_buckets", 65535)),
            )
            self.cache_key = f"trino://{host}:{port}/{kw.get('trino_catalog', 'opensearch')}"
        else:
            scheme = kw.get("scheme") or ("https" if _bool(kw.get("use_ssl")) else "http")
            self.transport = DirectTransport(
                host=host, port=int(port or 9200), user=user, password=password,
                use_ssl=scheme == "https", verify_certs=_bool(kw.get("verify_certs"), True),
                ca_certs=kw.get("ca_certs"), client_cert=kw.get("client_cert"),
                client_key=kw.get("client_key"), request_timeout=self.request_timeout,
                http_compress=_bool(kw.get("http_compress"), True),
                url_prefix=kw.get("url_prefix", ""),
            )
            self.cache_key = f"{scheme}://{user or ''}@{host}:{port}"
        self._lock = threading.Lock()

    # PEP 249 --------------------------------------------------------------
    def close(self) -> None:
        if not self.closed:
            self.transport.close()
            self.closed = True

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def cursor(self) -> "Cursor":
        if self.closed:
            raise InterfaceError("connection is closed")
        return Cursor(self)

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def ping(self) -> bool:
        """Contact OpenSearch for real (credentials, TLS, permissions) with a cheap
        call that the least-privilege role allows. Used by Superset's "Test connection"."""
        if self.transport.kind == "trino":
            self.transport.list_tables()
        else:
            self.transport.list_tables(self.extra_tables or None)
        return True

    # metadata -------------------------------------------------------------
    def list_tables(self) -> list[str]:
        # tables=<patterns>: list only those (least-privilege accounts) and expose the
        # patterns themselves as tables (e.g. batch-jobs-* over daily indices)
        tables = CACHE.list_tables(self.cache_key, self.transport, self.extra_tables or None)
        return visible_tables(tables, self.extra_tables, self.include_hidden)

    def table_meta(self, name: str) -> TableMeta | None:
        patterns = self.extra_tables or None
        if patterns and any(fnmatch.fnmatchcase(name, p) for p in patterns):
            # inside a configured pattern: the mapping request tells whether it exists
            return self._with_virtual(CACHE.table(self.cache_key, self.transport, name,
                                                  date_probe=self.date_probe))
        tables = CACHE.list_tables(self.cache_key, self.transport, patterns)
        if not matches_any(name, tables) and name not in self.extra_tables:
            CACHE.invalidate()
            tables = CACHE.list_tables(self.cache_key, self.transport, patterns)
            if not matches_any(name, tables) and name not in self.extra_tables:
                return None
        return self._with_virtual(CACHE.table(self.cache_key, self.transport, name, date_probe=self.date_probe))

    def _with_virtual(self, meta: TableMeta | None) -> TableMeta | None:
        """Expose the business-date label column when the index has the source field."""
        if meta is None:
            return None
        return with_label_column(meta, self.label_source, self.label_column,
                                 self.label_time_source, self.label_time_column,
                                 self.label_date_format)

    def label_key(self) -> str:
        """Calendar state for one query: '<today>/<session date>/<Y|->'."""
        now = self.label_now or dt.datetime.now(dt.timezone.utc)
        if now.tzinfo is not None:      # wall clock in the label zone (or the local one)
            now = (now.astimezone(self.label_tz) if self.label_tz else now.astimezone()) \
                .replace(tzinfo=None)
        return calendar.asof_key(now, self.label_cutoff, self.label_years)


class Cursor:
    arraysize = 1000

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.description: list[tuple] | None = None
        self.rowcount = -1
        self._rows: list[tuple] = []
        self._pos = 0
        self.closed = False
        self.last_plan: Plan | None = None
        self.last_stats: list[Any] = []
        self.query_id: str | None = None

    # PEP 249 --------------------------------------------------------------
    def close(self) -> None:
        self.closed = True
        self._rows = []

    def setinputsizes(self, sizes: Any) -> None:
        pass

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        pass

    def executemany(self, operation: str, seq_of_parameters: Iterable[Any]) -> None:
        raise NotSupportedError("executemany is not supported (read-only engine)")

    def fetchone(self) -> tuple | None:
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        size = size or self.arraysize
        out = self._rows[self._pos:self._pos + size]
        self._pos += len(out)
        return out

    def fetchall(self) -> list[tuple]:
        out = self._rows[self._pos:]
        self._pos = len(self._rows)
        return out

    def __iter__(self):
        return iter(self.fetchall())

    # execution ------------------------------------------------------------
    def execute(self, operation: str, parameters: Any = None) -> "Cursor":
        if self.closed:
            raise InterfaceError("cursor is closed")
        sql = _bind(operation, parameters)
        self._rows, self._pos, self.description, self.rowcount = [], 0, None, -1
        stripped = sql.strip().rstrip(";").strip()
        m = re.match(r"(?is)^explain(\s+analyze)?\s+(.*)$", stripped)
        if m:
            return self._explain(m.group(2), analyze=bool(m.group(1)))
        if re.match(r"(?is)^show\s+tables\b", stripped):
            self._set_result([("name", "VARCHAR")], [(t,) for t in self.connection.list_tables()])
            return self
        m = re.match(r'(?is)^(?:describe|desc)\s+(?:"?default"?\.)?"?([^"]+)"?$', stripped)
        if m and not stripped.lower().startswith(("describe select", "desc select")):
            meta = self.connection.table_meta(m.group(1))
            if meta is None:
                raise ProgrammingError(f'Table "{m.group(1)}" does not exist')
            rows = [(f.name, f.sql_type, _virtual_desc(f) if f.virtual else f.os_type,
                     f.agg_field or "") for f in meta.fields.values()]
            self._set_result([("column_name", "VARCHAR"), ("column_type", "VARCHAR"),
                              ("opensearch_type", "VARCHAR"), ("aggregatable_field", "VARCHAR")], rows)
            return self
        try:
            statements = [s for s in sqlglot.parse(sql, read="duckdb") if s is not None]
        except sqlglot.errors.ParseError as ex:
            raise ProgrammingError(f"SQL syntax error: {ex}") from ex
        if not statements:
            raise ProgrammingError("empty statement")
        for stmt in statements:
            self._execute_one(stmt)
        return self

    def _settings(self, exact_distinct: bool = False) -> Settings:
        c = self.connection
        return Settings(tz=c.tz, max_scan_rows=c.max_scan_rows, max_buckets_total=c.max_buckets_total,
                        count_distinct=c.count_distinct, exact_distinct=exact_distinct,
                        cardinality_precision=c.cardinality_precision,
                        percentile_compression=c.percentile_compression, topn=c.topn,
                        enum_max_values=c.enum_max_values, label_key=c.label_key(),
                        join_max_keys=c.join_max_keys, lookup_joins=c.lookup_joins)

    def _planner(self, const_con, exact_distinct: bool = False) -> Planner:
        def const_eval(node: exp.Expression) -> Any:
            try:
                row = const_con.execute("SELECT " + node.sql(dialect="duckdb")).fetchone()
            except Exception as ex:  # pylint: disable=broad-except
                from osagg.translate import Untranslatable

                raise Untranslatable(f"cannot evaluate constant {node.sql()}: {ex}") from ex
            return row[0]

        def const_query(sql: str) -> list[tuple]:
            return const_con.execute(sql).fetchall()

        return Planner(self.connection.table_meta, self._settings(exact_distinct), const_eval,
                       enumerate_values=self._enumerate_values, const_query=const_query,
                       estimate_keys=self._estimate_keys, count_docs=self._count_docs)

    def _estimate_keys(self, index: str, query: dict, fields: list[str]) -> int:
        """Approximate number of distinct join-key combinations (product of cardinalities).

        A first pass reads at most join_max_keys + 1 documents per shard: a high-cardinality
        key of a huge index is refused from that sample, without a pass over the index."""
        aggs = {f"k{i}": {"cardinality": {"field": f, "precision_threshold": 1000}}
                for i, f in enumerate(fields)}
        cap = self.connection.join_max_keys
        body = {"size": 0, "track_total_hits": False, "query": query, "aggs": aggs}
        n = 0
        for extra in ({"terminate_after": cap + 1}, {}):
            res = self.connection.transport.search(index, {**body, **extra})
            n = 1
            for i in range(len(fields)):
                n *= max(1, int(res["aggregations"][f"k{i}"]["value"]))
            if n > cap or not res.get("terminated_early"):
                break
        return n

    def _count_docs(self, index: str, query: dict, cap: int) -> int:
        """Matching documents, counted up to cap + 1 only (cheap on huge indices)."""
        res = self.connection.transport.search(index, {"size": 0, "track_total_hits": cap + 1,
                                                       "query": query})
        return int(res["hits"]["total"]["value"])

    def _push_join_keys(self, scan: Any, con: Any, done: dict) -> None:
        """Filter a join side by the join keys of the sides aggregated before it."""
        from osagg.joins import refusal, terms_filter
        from osagg.translate import q_and

        c = self.connection
        j = scan.join
        cap = 2 * c.join_max_keys
        room = getattr(c.transport, "max_body_chars", None)
        filters = []
        for p in j["push_from"]:
            src = done[(j["group"], p["src"])]
            rows = con.execute(f'SELECT DISTINCT "{p["col"]}" FROM ({src.join["sql"]}) AS __osagg_k '
                               f'WHERE "{p["col"]}" IS NOT NULL LIMIT {cap + 1}').fetchall()
            if len(rows) > cap:
                scan.notes.append(f"join: more than {cap:,} keys of {p['src_index']}, not pushed")
                continue
            values = [float(v) if isinstance(v, decimal.Decimal) else v for (v,) in rows]
            flt = terms_filter(p["field"], values)
            if room is not None:
                room -= len(json.dumps(flt, default=str))
                if room < 0:
                    scan.notes.append(f"join: keys of {p['src_index']} too long for the transport, "
                                      "not pushed")
                    continue
            filters.append(flt)
            scan.notes.append(f"join: {len(values):,} key(s) of {p['src_index']} pushed on {p['field']}")
        if filters:
            scan.query = q_and([scan.query] + filters)
        elif j["fed"]:
            # it was only bounded by the keys it should have received
            n = self._estimate_keys(scan.index, scan.query, j["key_fields"])
            if n > c.join_max_keys:
                raise refusal(f"{scan.index} has about {n:,} distinct join keys under its filters "
                              f"(more than join_max_keys={c.join_max_keys:,}) and the keys of the other "
                              "indices could not be pushed to it")

    def _enumerate_values(self, index: str, query: dict, f: Any, limit: int) -> list | None:
        """Distinct values of one field under `query` (None when more than `limit`).

        Cached for `enum_cache_ttl` seconds: every chart of a dashboard that filters on
        the same label asks the same question."""
        ttl = self.connection.enum_cache_ttl
        key = (self.connection.cache_key, index, f.agg_field or f.name, limit,
               json.dumps(query, sort_keys=True, default=str))
        if ttl > 0:
            hit = _ENUM_CACHE.get(key)
            if hit is not None and hit[0] > time.monotonic():
                return hit[1]
        values = self._enumerate_values_uncached(index, query, f, limit)
        if ttl > 0:
            if len(_ENUM_CACHE) > 512:
                _ENUM_CACHE.clear()
            _ENUM_CACHE[key] = (time.monotonic() + ttl, values)
        return values

    def _enumerate_values_uncached(self, index: str, query: dict, f: Any, limit: int) -> list | None:
        source: dict[str, Any] = {"terms": {"field": f.agg_field, "missing_bucket": True}}
        if f.agg_field is None and f.variants:          # text here, keyword there: each index's exact field
            from osagg.translate import _variant_key

            key = _variant_key(f)
            if key is None:
                return None
            source = key.source
        comp: dict[str, Any] = {"size": min(limit + 1, 10_000), "sources": [{"v": source}]}
        body = {"size": 0, "track_total_hits": False, "query": query,
                "aggs": {"e": {"composite": comp}}}
        values: list[Any] = []
        while True:
            res = self.connection.transport.search(index, body)
            agg = res["aggregations"]["e"]
            page = agg.get("buckets", [])
            values.extend(b["key"]["v"] for b in page)
            if len(values) > limit:
                return None
            after = agg.get("after_key")
            if not page or after is None or len(page) < comp["size"]:
                return values
            comp["after"] = after

    def _check_statement(self, stmt: exp.Expression) -> None:
        if not isinstance(stmt, (exp.Query, exp.Select, exp.Union, exp.Intersect, exp.Except)):
            raise NotSupportedError(
                f"Only SELECT queries are supported (got {type(stmt).__name__.upper()})")

    def plan(self, stmt: exp.Expression, exact_distinct: bool = False) -> Plan:
        self._check_statement(stmt)
        const_con = duck.locked_session(self.connection.tz_name)
        try:
            return self._planner(const_con, exact_distinct).plan(stmt)
        finally:
            const_con.close()

    def _execute_one(self, stmt: exp.Expression) -> None:
        t0 = time.perf_counter()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("osagg SQL: %s", stmt.sql(dialect="duckdb"))
        try:
            plan = self.plan(stmt)
        except PushdownError:
            if not _no_rows_by_construction(stmt) or not self._answer_without_reading(stmt):
                raise
            return
        self.last_plan = plan
        c = self.connection
        executor = Executor(c.transport, c.tz, page_size=c.page_size,
                            max_buckets_total=c.max_buckets_total, max_scan_rows=c.max_scan_rows,
                            max_rows=c.max_rows,
                            doc_page_size=c.doc_page_size, request_timeout=c.request_timeout)
        con = duck.new_session(c.tz_name)
        try:
            try:
                self._run_scans(plan, executor, con)
            except NotExact as ex:
                # a COUNT(DISTINCT) sketch reached its threshold (an estimate above): counted exactly
                logger.info("osagg: %s: counted again exactly", ex)
                con.close()
                con = duck.new_session(c.tz_name)
                plan = self.last_plan = self.plan(stmt, exact_distinct=True)
                plan.notes.append(f"{ex}: counted exactly (its values as keys)")
                self._run_scans(plan, executor, con)
            duck.lock(con)
            sql = plan.residual.sql(dialect="duckdb")
            logger.debug("osagg residual SQL: %s", sql)
            try:
                cur = con.execute(sql)
            except Exception as ex:  # pylint: disable=broad-except
                raise ProgrammingError(f"{ex}") from ex
            desc = cur.description or []
            rows = cur.fetchall()
        finally:
            con.close()
        cols = [(d[0], _type_name(d[1])) for d in desc]
        self._set_result(cols, [_fix_row(r) for r in rows])
        logger.info("osagg query: %d scan(s), %d row(s), %.0f ms", len(plan.scans), len(rows),
                    (time.perf_counter() - t0) * 1000)

    def _run_scans(self, plan: Plan, executor: Executor, con: Any) -> None:
        self.last_stats = []
        done: dict[tuple, Any] = {}
        for scan in _run_order(plan.scans):
            join = getattr(scan, "join", None)
            if join and join["push_from"]:
                self._push_join_keys(scan, con, done)
            table, stats = executor.run(scan)
            con.register(scan.table, table)
            self.last_stats.append(stats)
            if join:
                done[(join["group"], join["id"])] = scan

    def _answer_without_reading(self, stmt: exp.Expression) -> bool:
        """LIMIT 0 / WHERE FALSE (Superset's column-type probes): run the query in DuckDB on
        empty tables typed like the indices, so e.g. a raw-row join that osagg refuses to
        execute can still describe its columns. Nothing is read from OpenSearch."""
        c = self.connection
        ctes = {cte.alias_or_name for cte in stmt.find_all(exp.CTE)}
        con = duck.new_session(c.tz_name)
        try:
            con.execute('CREATE SCHEMA IF NOT EXISTS "default"')
            done: set[str] = set()
            for t in stmt.find_all(exp.Table):
                name = t.name
                if not name or name in done or (not t.args.get("db") and name in ctes):
                    continue
                meta = c.table_meta(name)
                if meta is None:
                    return False
                q = '"' + name.replace('"', '""') + '"'
                cols = ", ".join('"' + f.name.replace('"', '""') + '" ' + f.sql_type
                                 for f in meta.fields.values())
                con.execute(f'CREATE TABLE "default".{q} ({cols})')
                con.execute(f'CREATE VIEW main.{q} AS SELECT * FROM "default".{q}')
                done.add(name)
            duck.lock(con)
            cur = con.execute(stmt.sql(dialect="duckdb"))
            desc = cur.description or []
            rows = cur.fetchall()
        except Exception:  # pylint: disable=broad-except
            logger.debug("osagg: no-read answer failed", exc_info=True)
            return False
        finally:
            con.close()
        self.last_plan = Plan(scans=[], residual=stmt,
                              notes=["no rows by construction: answered from the mappings"])
        self.last_stats = []
        self._set_result([(d[0], _type_name(d[1])) for d in desc], [_fix_row(r) for r in rows])
        return True

    def _set_result(self, cols: Sequence[tuple[str, str]], rows: list[tuple]) -> None:
        self.description = [(name, typ, None, None, None, None, True) for name, typ in cols]
        self._rows = rows
        self._pos = 0
        self.rowcount = len(rows)

    # EXPLAIN ----------------------------------------------------------------
    def _explain(self, sql: str, analyze: bool) -> "Cursor":
        try:
            stmt = sqlglot.parse_one(sql, read="duckdb")
        except sqlglot.errors.ParseError as ex:
            raise ProgrammingError(f"SQL syntax error: {ex}") from ex
        rows: list[tuple] = []
        if analyze:
            self._execute_one(stmt)
            plan = self.last_plan
            stats = {s.table: s for s in self.last_stats}
        else:
            plan = self.plan(stmt)
            stats = {}
        lines: list[str] = []
        for i, scan in enumerate(_run_order(plan.scans), 1):
            st = stats.get(scan.table)
            mode = getattr(scan, "mode", "")
            lines.append(f"-- {i}. OpenSearch {scan.kind}{(' (' + mode + ')') if mode else ''} "
                         f"on {scan.index}")
            if st is not None:
                lines.append(f"--    {st.rows} row(s), {st.requests} request(s), OpenSearch "
                             f"{st.os_took_ms} ms, wall {st.wall_ms:.0f} ms")
            for note in scan.notes:
                lines.append(f"--    note: {note}")
            lines.extend(json.dumps(scan.describe(), indent=2, default=str).splitlines())
            lines.append("")
        lines.append(f"-- {len(plan.scans) + 1}. DuckDB, on the rows returned above")
        lines.extend(plan.residual.sql(dialect="duckdb", pretty=True).splitlines())
        # one line per row reads well in SQL Lab's result grid (like PostgreSQL EXPLAIN);
        # indentation uses no-break spaces because HTML grids collapse normal ones
        rows = []
        for line in lines:
            stripped = line.lstrip(" ")
            rows.append(("\u00a0" * (len(line) - len(stripped)) + stripped,))
        self._set_result([("plan", "VARCHAR")], rows)
        return self


def _is_false(node: exp.Expression) -> bool:
    while isinstance(node, exp.Paren):
        node = node.this
    if isinstance(node, exp.Boolean):
        return node.this is False
    if isinstance(node, exp.And):
        return _is_false(node.this) or _is_false(node.expression)
    if isinstance(node, (exp.EQ, exp.NEQ)) and isinstance(node.this, exp.Literal) \
            and isinstance(node.expression, exp.Literal):
        same = node.this.this == node.expression.this and node.this.is_string == node.expression.is_string
        return same if isinstance(node, exp.NEQ) else not same
    return False


def _no_rows_by_construction(stmt: exp.Expression) -> bool:
    """LIMIT 0, or an outer WHERE FALSE without scalar subqueries: the answer does not
    depend on the documents."""
    if not isinstance(stmt, exp.Select):
        return False
    limit = stmt.args.get("limit")
    value = limit.expression if isinstance(limit, exp.Limit) else None
    if isinstance(value, exp.Literal) and not value.is_string and str(value.this) == "0":
        return True
    where = stmt.args.get("where")
    if where is None or not _is_false(where.this):
        return False
    return all(isinstance(s.parent, (exp.From, exp.Join)) for s in stmt.find_all(exp.Subquery))


def _run_order(scans: list) -> list:
    """Join sides in their planned order: each may receive the keys of earlier ones."""
    return sorted(scans, key=lambda s: (1, s.join["group"], s.join["rank"])
                  if getattr(s, "join", None) else (0, "", 0))


def _virtual_desc(f: Any) -> str:
    kind, *args = f.virtual.split(":")
    if kind == "shift":
        return f"computed: {args[1]} moved onto the D-1 position date ({args[0]})"
    return f"computed: label of {args[0]}"


def _type_name(t: Any) -> str:
    name = str(t).upper()
    if name in ("HUGEINT", "UHUGEINT", "UBIGINT"):
        return "BIGINT"
    if name.startswith("TIMESTAMP WITH TIME ZONE"):
        return "TIMESTAMP WITH TIME ZONE"
    return name


def _fix_row(row: tuple) -> tuple:
    # HUGEINT arrives as python int already; nothing to do except exotic types
    return tuple(float(v) if isinstance(v, decimal.Decimal) and v.as_tuple().exponent < -12 else v
                 for v in row)


def _quote(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float, decimal.Decimal)):
        return str(v)
    if isinstance(v, dt.datetime):
        return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    if isinstance(v, dt.date):
        return f"DATE '{v.isoformat()}'"
    if isinstance(v, (list, tuple)):
        return "(" + ", ".join(_quote(x) for x in v) + ")"
    return "'" + str(v).replace("'", "''") + "'"


def _bind(sql: str, params: Any) -> str:
    if params is None:
        return sql
    if isinstance(params, dict):
        if not params:
            return sql
        out = re.sub(r"%\(([^)]+)\)s", lambda m: _quote(params[m.group(1)]), sql)
    else:
        seq = list(params)
        if not seq:
            return sql
        it = iter(seq)
        out = re.sub(r"%s", lambda m: _quote(next(it)), sql)
    return out.replace("%%", "%")
