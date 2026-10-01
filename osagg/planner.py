"""Query planner: split a SQL statement into OpenSearch scans + a DuckDB residual.

For every ``SELECT`` whose ``FROM`` is an OpenSearch table (index, alias or
pattern) the planner produces one *scan*:

``AggScan``  (GROUP BY / aggregates / DISTINCT)
    *direct*      every group key and aggregate maps to OpenSearch: one
                  composite aggregation, one output row per bucket.
    *two-level*   some group keys / WHERE conjuncts are arbitrary expressions
                  (CASE ... 'Others', UPPER(x), EXTRACT(hour ...), ...): the
                  columns they use become composite keys, aggregates are
                  pushed as decomposable partials (sum/count/min/max) and
                  DuckDB re-aggregates the (small) bucket table.
``DocScan``  plain row queries (samples, drill-to-detail, SQL Lab): filters,
             sort and limit are pushed down; a hard row cap protects workers.

The SELECT is then rewritten to read from the scan's (registered) table, so
DuckDB only evaluates what is left: ORDER BY, LIMIT, HAVING, arithmetic on
aggregates, window functions, joins between scans, etc.
"""

from __future__ import annotations

import datetime as dt
import itertools
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable

from sqlglot import exp

from osagg import calendar
from osagg.errors import ProgrammingError, PushdownError
from osagg.metadata import SORTABLE_FORMATS, Field, TableMeta
from osagg.translate import (
    zone_of,
    MATCH_ALL,
    MATCH_NONE,
    AggSpec,
    Ctx,
    GroupKey,
    NotNumeric,
    Untranslatable,
    aggregate,
    _interval_parts,
    group_key,
    is_aggregate,
    is_constant,
    predicate,
    q_and,
    q_exists,
    q_not,
    q_or,
    sort_script,
)

logger = logging.getLogger(__name__)

SCAN_PREFIX = "__osagg_"
# sqlglot >= 28 names the FROM clause "from_", older versions "from"
FROM_KEY = "from_" if "from_" in exp.Select.arg_types else "from"
NULL_KEY = "\u0000osagg-null\u0000"


# --------------------------------------------------------------------------- #
# plan objects
# --------------------------------------------------------------------------- #
@dataclass
class OutCol:
    name: str
    sql_type: str
    extract: Callable[[dict], Any]


@dataclass
class AggScan:
    table: str                    # registered DuckDB table name
    index: str                    # OpenSearch index / alias / pattern
    query: dict
    keys: list[tuple[str, GroupKey]]
    aggs: dict[str, dict]
    columns: list[OutCol]
    mode: str                     # direct | two-level | global
    stop_after: int | None = None  # early termination (rows) when order allows it
    stop_dst_key: str | None = None  # two-level DST merge: first key, a local timestamp
    bucket_aggs: int = 0
    notes: list[str] = field(default_factory=list)
    heavy: int = 0
    topn: dict | None = None
    join: dict | None = None      # side of a join (osagg.joins): order, keys to receive

    kind = "aggregation"

    def describe(self) -> dict:
        body: dict[str, Any] = {"size": 0, "query": self.query}
        if self.mode == "global":
            body["aggs"] = self.aggs
        elif self.mode == "topn":
            body["aggs"] = {"g": {"terms": self.topn, **({"aggs": self.aggs} if self.aggs else {})}}
        else:
            comp: dict[str, Any] = {"size": "<page>", "sources": [{n: k.source} for n, k in self.keys]}
            body["aggs"] = {"g": {"composite": comp, **({"aggs": self.aggs} if self.aggs else {})}}
        return body


@dataclass
class DocScan:
    table: str
    index: str
    query: dict
    fields: list[Field]
    sort: list[dict] | None
    limit: int | None             # rows to fetch (None: all matching, subject to max_scan_rows)
    notes: list[str] = field(default_factory=list)
    join: dict | None = None      # side of a row join (extracts only, osagg.joins)

    kind = "documents"

    def describe(self) -> dict:
        body: dict[str, Any] = {"query": self.query,
                                "_source": [f.source_path for f in self.fields if f.source_path]}
        if self.sort:
            body["sort"] = self.sort
        body["size"] = self.limit if self.limit is not None else "<all, capped>"
        return body


@dataclass
class Plan:
    scans: list[AggScan | DocScan]
    residual: exp.Expression
    notes: list[str] = field(default_factory=list)


@dataclass
class Settings:
    tz: Any
    max_scan_rows: int = 500_000
    max_buckets_total: int = 2_000_000
    cardinality_precision: int = 3_000
    percentile_compression: int = 500
    topn: str = "exact"            # "approx": ORDER BY <aggregate> LIMIT n via a terms aggregation
    enum_max_values: int = 10_000  # single-column filters: enumerate up to this many values
    label_key: str | None = None   # business calendar state of this query (osagg.calendar)
    join_max_keys: int = 100_000   # joins: distinct join-key values allowed per index
    lookup_joins: bool = False     # row lists over a join (one big index + small ones): extracts only


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _strip(node: exp.Expression) -> exp.Expression:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _key(node: exp.Expression) -> str:
    """Structural key used to match identical expressions (qualifiers removed)."""
    node = _strip(node).copy()
    for col in node.find_all(exp.Column):
        col.set("table", None)
        col.set("db", None)
        col.set("catalog", None)
    return node.sql(dialect="duckdb", normalize=True)


def conjuncts(node: exp.Expression | None) -> list[exp.Expression]:
    if node is None:
        return []
    node = _strip(node)
    if isinstance(node, exp.And):
        return conjuncts(node.this) + conjuncts(node.expression)
    return [node]


def and_all(parts: list[exp.Expression]) -> exp.Expression | None:
    if not parts:
        return None
    out = parts[0]
    for p in parts[1:]:
        out = exp.and_(out, p, copy=False)
    return out


def _contains_agg(node: exp.Expression) -> bool:
    for n in node.walk():
        if isinstance(n, exp.Window):
            continue
        if is_aggregate(n) and not n.find_ancestor(exp.Window):
            return True
    return False


def _aggs_in(node: exp.Expression) -> list[exp.Expression]:
    """Outermost aggregate calls (not inside window specs)."""
    out: list[exp.Expression] = []

    def visit(n: exp.Expression) -> None:
        if isinstance(n, exp.Window):
            # aggregates inside OVER(...) args are window functions, but their
            # arguments may contain real aggregates (SUM(SUM(x)) OVER ()).
            inner = n.this
            if inner is not None:
                for child in inner.iter_expressions():
                    visit(child)
            return
        if is_aggregate(n):
            out.append(n)
            return
        for child in n.iter_expressions():
            visit(child)

    visit(node)
    return out


def _has_window(select: exp.Select) -> bool:
    return any(True for _ in select.find_all(exp.Window))


# --------------------------------------------------------------------------- #
# planner
# --------------------------------------------------------------------------- #
class Planner:
    def __init__(self, lookup_table: Callable[[str], TableMeta | None], settings: Settings,
                 const_eval: Callable[[exp.Expression], Any],
                 enumerate_values: Callable[[str, dict, Field, int], list | None] | None = None,
                 const_query: Callable[[str], list[tuple]] | None = None,
                 estimate_keys: Callable[[str, dict, list[str]], int] | None = None,
                 count_docs: Callable[[str, dict, int], int] | None = None) -> None:
        self.lookup_table = lookup_table
        self.estimate_keys = estimate_keys
        self.count_docs = count_docs
        self.settings = settings
        self.const_eval = const_eval
        self.enumerate_values = enumerate_values
        self.const_query = const_query
        self._ids = itertools.count(1)
        self._ctx_meta: TableMeta | None = None
        self._join_memo: dict[tuple, int] = {}

    # ------------------------------------------------------------------ #
    def plan(self, stmt: exp.Expression) -> Plan:
        stmt = stmt.copy()
        cte_names = {cte.alias_or_name for cte in stmt.find_all(exp.CTE)}
        self._cte_names = cte_names
        self._flatten_derived_tables(stmt)
        stmt = self._rewrite_joins(stmt)
        scans: list[AggScan | DocScan] = []
        notes: list[str] = []

        # deepest SELECTs first
        selects = [(s.depth, i, s) for i, s in enumerate(stmt.find_all(exp.Select))]
        selects.sort(key=lambda t: (-t[0], t[1]))
        for _depth, _i, select in selects:
            os_table = self._os_table(select)
            if os_table is None:
                self._check_joins(select)
                continue
            table_node, meta = os_table
            new_select, scan = self._plan_select(select, table_node, meta)
            join = select.meta.get("osagg_join")
            if join is not None:
                from osagg.joins import refusal

                if scan.kind != ("documents" if join.get("rows") else "aggregation"):
                    raise refusal(f"{meta.name} cannot be aggregated in OpenSearch here "
                                  f"({'; '.join(scan.notes) or 'raw documents needed'})")
                scan.join = {**join, "sql": new_select.sql(dialect="duckdb")}
                scan.notes.append(join["note"])
            scans.append(scan)
            notes.extend(scan.notes)
            if select is stmt:
                stmt = new_select
            else:
                select.replace(new_select)
        return Plan(scans=scans, residual=stmt, notes=notes)

    # ------------------------------------------------------------------ #
    def _table_meta(self, table: exp.Table) -> TableMeta | None:
        name = table.name
        if not name or name.startswith(SCAN_PREFIX):
            return None
        if not table.args.get("db") and name in self._cte_names:
            return None
        db = table.text("db")
        if db and db not in ("default",):
            raise ProgrammingError(f'Unknown schema "{db}" (only "default" exists)')
        return self.lookup_table(name)

    def _os_table(self, select: exp.Select) -> tuple[exp.Table, TableMeta] | None:
        from_ = select.args.get(FROM_KEY)
        if from_ is None:
            return None
        src = from_.this
        if not isinstance(src, exp.Table):
            return None
        meta = self._table_meta(src)
        if meta is None:
            return None
        if select.args.get("joins"):
            raise PushdownError(
                "JOIN on an OpenSearch table is not supported: aggregate or filter each side in "
                "a subquery first, e.g. SELECT ... FROM (SELECT k, COUNT(*) c FROM idx GROUP BY k) a "
                "JOIN (...) b ON ...")
        if select.args.get("laterals"):
            raise PushdownError("LATERAL on an OpenSearch table is not supported")
        return src, meta

    def _check_joins(self, select: exp.Select) -> None:
        for j in select.args.get("joins") or []:
            t = j.this
            if isinstance(t, exp.Table) and self._table_meta(t) is not None:
                raise PushdownError(
                    "JOIN with a raw OpenSearch table is not supported: wrap it in an aggregating "
                    "subquery")

    # ------------------------------------------------------------------ #
    # joins of indices: aggregated in OpenSearch, grouped rows joined (osagg.joins)
    # ------------------------------------------------------------------ #
    def _rewrite_joins(self, stmt: exp.Expression) -> exp.Expression:
        from osagg.joins import rewrite_join

        for select in list(stmt.find_all(exp.Select)):
            joins = select.args.get("joins")
            from_ = select.args.get(FROM_KEY)
            if not joins or from_ is None or not isinstance(from_.this, exp.Table):
                continue
            meta = self._table_meta(from_.this)
            if meta is None:
                continue
            if any(not isinstance(j.this, exp.Table) or self._table_meta(j.this) is None for j in joins):
                continue    # index JOIN subquery: refused later with the usual message
            new = rewrite_join(select, from_.this, meta, joins, self._table_meta,
                               self._join_count if self.count_docs else None,
                               self._join_estimate if self.estimate_keys else None,
                               self.settings.join_max_keys, f"j{next(self._ids)}",
                               allow_rows=self.settings.lookup_joins)
            if select is stmt:
                stmt = new
            else:
                select.replace(new)
        return stmt

    def _where_query(self, probe: exp.Select) -> tuple[str, dict]:
        """(index, OpenSearch query) of SELECT ... FROM index WHERE ..., as planned."""
        table = probe.args[FROM_KEY].this
        meta = self._table_meta(table)
        probe = probe.copy()
        self._pd_dates = {}
        ctx = self._ctx(table, meta)
        self._expand_virtual(probe, ctx)
        pushed: list[dict] = []
        residual = []
        where = probe.args.get("where")
        for c in conjuncts(where.this if where is not None else None):
            try:
                pushed.append(predicate(c, ctx).true)
            except Untranslatable as ex:
                residual.append((c, ex))
        self._enumerate_residual(residual, ctx, meta, pushed, [], "not counted")
        return meta.name, (q_and(pushed) if pushed else MATCH_ALL)

    def _join_count(self, probe: exp.Select) -> int:
        index, query = self._where_query(probe)
        key = ("count", index, json.dumps(query, sort_keys=True, default=str))
        if key not in self._join_memo:
            self._join_memo[key] = self.count_docs(index, query, self.settings.join_max_keys)
        return self._join_memo[key]

    def _join_estimate(self, probe: exp.Select, fields: list[Field]) -> int:
        index, query = self._where_query(probe)
        names = [f.agg_field for f in fields]
        key = ("keys", index, json.dumps(query, sort_keys=True, default=str), tuple(names))
        if key not in self._join_memo:
            self._join_memo[key] = self.estimate_keys(index, query, names)
        return self._join_memo[key]

    # ------------------------------------------------------------------ #
    # virtual datasets: SELECT ... FROM (SELECT <cols> FROM idx WHERE ...) AS t
    # ------------------------------------------------------------------ #
    def _flatten_derived_tables(self, stmt: exp.Expression) -> None:
        changed = True
        while changed:
            changed = False
            for outer in list(stmt.find_all(exp.Select)):
                from_ = outer.args.get(FROM_KEY)
                if from_ is None or outer.args.get("joins") or outer.args.get("laterals"):
                    continue
                sub = from_.this
                if not isinstance(sub, exp.Subquery) or not isinstance(sub.this, exp.Select):
                    continue
                inner = sub.this
                if self._os_table_simple(inner) is None:
                    continue
                if self._merge(outer, sub, inner):
                    changed = True
                    break

    def _os_table_simple(self, inner: exp.Select) -> exp.Table | None:
        from_ = inner.args.get(FROM_KEY)
        if from_ is None or not isinstance(from_.this, exp.Table):
            return None
        if self._table_meta(from_.this) is None:
            return None
        joins = inner.args.get("joins") or []
        if any(not (isinstance(j.this, exp.Table) and self._table_meta(j.this) is not None)
               for j in joins):
            return None
        if joins and any(isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star))
                         for e in inner.expressions):
            return None      # SELECT * over a join: names would be ambiguous
        for arg in ("laterals", "group", "having", "limit", "offset", "distinct",
                    "qualify", "with", "windows", "prewhere"):
            if inner.args.get(arg):
                return None
        if _has_window(inner) or any(_contains_agg(e) for e in inner.expressions):
            return None
        return from_.this

    def _merge(self, outer: exp.Select, sub: exp.Subquery, inner: exp.Select) -> bool:
        alias = sub.alias_or_name
        star = any(isinstance(e, exp.Star) for e in inner.expressions)
        proj: dict[str, exp.Expression] = {}
        order: list[str] = []
        for e in inner.expressions:
            if isinstance(e, exp.Star):
                continue
            name = e.alias_or_name
            if not name:
                return False
            proj[name] = e.this if isinstance(e, exp.Alias) else e
            order.append(name)

        # every outer column must resolve (ORDER BY / HAVING may name an output alias)
        out_aliases = {e.alias for e in outer.expressions if isinstance(e, exp.Alias)}

        def alias_ref(col: exp.Column) -> bool:
            return (not col.table and col.name in out_aliases
                    and col.find_ancestor(exp.Order, exp.Having, exp.Qualify) is not None)

        for col in outer.find_all(exp.Column):
            if col.find_ancestor(exp.Subquery, exp.Select) is not outer and col.find_ancestor(exp.Select) is not outer:
                continue
            if col.table and col.table != alias:
                return False
            if col.name not in proj and not star and not alias_ref(col):
                return False

        def subst(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Column) and not isinstance(node.this, exp.Star) \
                    and (not node.table or node.table == alias) \
                    and node.find_ancestor(exp.Select) is outer and not alias_ref(node):
                repl = proj.get(node.name)
                if repl is not None:
                    new = repl.copy()
                    if isinstance(new, (exp.Binary, exp.Connector, exp.Not, exp.Case)):
                        new = exp.Paren(this=new)
                    return new
                new = node.copy()
                new.set("table", None)
                return new
            return node

        # projections: substitute, keep output names, expand SELECT *
        new_exprs: list[exp.Expression] = []
        for e in list(outer.expressions):
            if isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star)):
                if star:
                    new_exprs.append(exp.Star())
                for name in order:
                    new_exprs.append(exp.alias_(proj[name].copy(), name, quoted=True))
                continue
            out_name = e.alias_or_name if isinstance(e, (exp.Alias, exp.Column)) else None
            e2 = e.transform(subst, copy=False)
            if isinstance(e, exp.Column) and out_name and not (
                    isinstance(e2, exp.Column) and e2.name == out_name):
                e2 = exp.alias_(e2, out_name, quoted=True)
            new_exprs.append(e2)
        outer.set("expressions", new_exprs)

        for key in ("where", "group", "having", "order", "qualify"):
            val = outer.args.get(key)
            if val is None:
                continue
            if isinstance(val, list):
                outer.set(key, [v.transform(subst, copy=False) for v in val])
            else:
                outer.set(key, val.transform(subst, copy=False))

        outer.set(FROM_KEY, inner.args[FROM_KEY].copy())
        if inner.args.get("joins"):
            outer.set("joins", [j.copy() for j in inner.args["joins"]])
        w = [c for c in [inner.args.get("where"), outer.args.get("where")] if c is not None]
        merged = and_all([c.this.copy() if isinstance(c, exp.Where) else c.copy() for c in w])
        outer.set("where", exp.Where(this=merged) if merged is not None else None)
        return True

    # ------------------------------------------------------------------ #
    def _ctx(self, table: exp.Table, meta: TableMeta) -> Ctx:
        quals = {table.name}
        if table.alias:
            quals.add(table.alias)
        return Ctx(meta=meta, tz=self.settings.tz, qualifiers=quals, const_eval=self.const_eval,
                   cardinality_precision=self.settings.cardinality_precision,
                   percentile_compression=self.settings.percentile_compression)

    def _new_table(self) -> str:
        return f"{SCAN_PREFIX}{next(self._ids)}"

    def _check_columns(self, select: exp.Select, ctx: Ctx, table: exp.Table) -> None:
        for col in select.find_all(exp.Column):
            if col.find_ancestor(exp.Select) is not select:
                continue
            if isinstance(col.this, exp.Star):
                continue
            if col.table and col.table not in ctx.qualifiers:
                continue
            if ctx.field_of(col) is None and not self._is_alias_ref(select, col):
                avail = ", ".join(list(ctx.meta.fields)[:60])
                raise ProgrammingError(
                    f'Column "{col.name}" does not exist in "{table.name}". Available: {avail}')

    @staticmethod
    def _is_alias_ref(select: exp.Select, col: exp.Column) -> bool:
        if col.table:
            return False
        aliases = {e.alias for e in select.expressions if isinstance(e, exp.Alias)}
        if col.name not in aliases:
            return False
        # alias references are legal in ORDER BY / GROUP BY / HAVING (DuckDB)
        return col.find_ancestor(exp.Order, exp.Group, exp.Having, exp.Qualify) is not None

    # ------------------------------------------------------------------ #
    # virtual columns: POSITION_LABEL -> osagg_label("POSITION_DATE", '<as-of>')
    # ------------------------------------------------------------------ #
    def _expand_virtual(self, select: exp.Select, ctx: Ctx) -> None:
        """Replace the columns computed by osagg (the business-date label, the execution
        time moved onto the D-1 position date) by their definition on stored fields, in place. Filters on the label then become
        terms filters on the date (see _label_filter), GROUP BY on it groups on the
        date in OpenSearch and labels the (few) buckets in DuckDB."""
        virtual = [f for f in ctx.meta.fields.values() if f.virtual]
        if not virtual:
            return
        key = self.settings.label_key
        if key is None:
            raise ProgrammingError("label columns need the calendar state (Settings.label_key)")
        aliases = {e.alias for e in select.expressions if isinstance(e, exp.Alias)}

        def definition(f: Field) -> exp.Expression:
            kind, *args = f.virtual.split(":")
            opts = f.virtual_opts or {"source": args[0], "kind": "keyword", "format": "%Y%m%d"}
            ymd = _yyyymmdd(opts)
            if kind == "shift":
                # POSITION_TIME = DATE_TRUNC('minute', "@timestamp_date")
                #                 + INTERVAL (<days to the D-1 position date>) DAY
                # (to the minute: grouping on it without a time grain stays bounded)
                ts = opts.get("time") or args[1]
                days = exp.Anonymous(this="osagg_shift", expressions=[ymd, exp.Literal.string(key)])
                minute = exp.TimestampTrunc(this=exp.column(ts, quoted=True),
                                            unit=exp.Var(this="MINUTE"))
                return exp.Add(this=minute, expression=exp.Interval(
                    this=exp.Paren(this=days), unit=exp.Var(this="DAY")))
            return exp.Anonymous(this="osagg_label", expressions=[ymd, exp.Literal.string(key)])

        def tx(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Column) or isinstance(node.this, exp.Star) \
                    or node.find_ancestor(exp.Select) is not select:
                return node
            f = ctx.field_of(node)
            if f is None or not f.virtual:
                return node
            if not node.table and node.name in aliases \
                    and node.find_ancestor(exp.Order, exp.Qualify) is not None:
                return node          # ORDER BY <output alias>
            return definition(f)

        exprs: list[exp.Expression] = []
        for e in select.expressions:
            if isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star)):
                # SELECT *: the stored columns come from the scan, the label is computed
                exprs.append(e)
                qual = e.table if isinstance(e, exp.Column) else ""
                if not qual or qual in ctx.qualifiers:
                    exprs.extend(exp.alias_(definition(f), f.name, quoted=True) for f in virtual)
                continue
            if isinstance(e, exp.Column):
                f = ctx.field_of(e)
                if f is not None and f.virtual:
                    exprs.append(exp.alias_(definition(f), e.name, quoted=True))
                    continue
            exprs.append(e.transform(tx, copy=False))
        select.set("expressions", exprs)
        for arg in ("where", "group", "having", "order", "qualify", "distinct"):
            val = select.args.get(arg)
            if val is not None:
                select.set(arg, val.transform(tx, copy=False))

    def _label_filter(self, c: exp.Expression, ctx: Ctx, base_query: dict,
                      notes: list[str]) -> dict | None:
        """osagg_label(date, key) = 'D-1' / IN ('D-1', 'W-1', 'Y-1'): the dates that carry
        these labels are known from the calendar, no OpenSearch round trip is needed.
        "D" also covers every later date: those are listed with a range-restricted
        enumeration (normally empty)."""
        c = _strip(c)
        if isinstance(c, exp.EQ):
            lhs, rhs = _strip(c.this), _strip(c.expression)
            if isinstance(lhs, exp.Literal):
                lhs, rhs = rhs, lhs
            values = [rhs]
        elif isinstance(c, exp.In) and not c.args.get("query") and not c.args.get("unnest"):
            lhs, values = _strip(c.this), [_strip(v) for v in c.expressions]
        else:
            return None
        if not (isinstance(lhs, exp.Anonymous) and lhs.name.lower() == "osagg_label"
                and len(lhs.expressions) == 2 and isinstance(lhs.expressions[1], exp.Literal)
                and lhs.expressions[1].is_string):
            return None
        src = _date_source(lhs.expressions[0], ctx)
        if src is None:
            return None
        f, kind, fmt = src
        if not all(isinstance(v, exp.Null) or (isinstance(v, exp.Literal) and v.is_string)
                   for v in values):
            return None
        key = lhs.expressions[1].this
        labels = sorted({v.this for v in values if isinstance(v, exp.Literal)})
        dates = calendar.dates_for_labels(labels, key)
        tail = calendar.open_ended_from(labels, key)
        extra: list[str] = []
        tail_query: dict | None = None
        if tail is not None and kind == "date":
            tail_query = predicate(exp.GTE(this=exp.column(f.name, quoted=True),
                                           expression=_ts_literal(_ymd_date(tail))), ctx).true
        elif tail is not None:
            if self.enumerate_values is None or fmt not in SORTABLE_FORMATS:
                return None
            since = predicate(exp.GTE(this=exp.column(f.name, quoted=True),
                                      expression=exp.Literal.string(_ymd_date(tail).strftime(fmt))), ctx).true
            later = self.enumerate_values(ctx.meta.name, q_and([base_query, since]), f,
                                          self.settings.enum_max_values)
            if later is None:
                return None
            extra = [d for d in (_to_ymd(v, fmt) for v in later)
                     if d is not None and calendar.label(d, key) in labels]
        dates = sorted(set(dates) | set(extra))
        prev = self._pd_dates.get(f.name)
        self._pd_dates[f.name] = set(dates) if prev is None else prev & set(dates)
        today, session = key.split("/")[:2]
        notes.append(f"label filter {', '.join(labels)} (today {today}, session date {session}) "
                     f"-> {f.name} in {len(dates)} date(s){' or later' if tail_query else ''} "
                     "pushed down")
        parts = [self._dates_query(f, kind, fmt, dates, ctx)] if dates else []
        if tail_query is not None:
            parts.append(tail_query)
        return (parts[0] if len(parts) == 1 else q_or(parts)) if parts else MATCH_NONE

    def _dates_query(self, f: Field, kind: str, fmt: str, dates: list[str], ctx: Ctx) -> dict:
        """Documents whose position date is one of `dates` (yyyymmdd)."""
        if kind == "keyword":
            return _in_values(f, [_ymd_date(d).strftime(fmt) for d in dates], ctx)
        # date field: one range per run of consecutive days
        days = sorted(_ymd_date(d) for d in dates)
        runs: list[list[dt.date]] = []
        for d in days:
            if runs and d == runs[-1][1] + dt.timedelta(days=1):
                runs[-1][1] = d
            else:
                runs.append([d, d])
        col = exp.column(f.name, quoted=True)
        alts = [exp.and_(exp.GTE(this=col.copy(), expression=_ts_literal(a)),
                         exp.LT(this=col.copy(), expression=_ts_literal(b + dt.timedelta(days=1))))
                for a, b in runs]
        expr = alts[0]
        for a in alts[1:]:
            expr = exp.or_(expr, a)
        return predicate(expr, ctx).true

    MAX_TIME_DATES = 200   # position dates a POSITION_TIME range may span (2 clauses each)

    def _position_time_filter(self, c: exp.Expression, ctx: Ctx, base_query: dict,
                              notes: list[str]) -> dict | None:
        """POSITION_TIME >= / > / < / <= / BETWEEN <constant>: for each position date P
        in play, "@timestamp_date" <op> <constant> - shift(P) days, pushed as one bool
        should. The dates come from the label filter of the same query (no request),
        otherwise from the listed POSITION_DATE values."""
        c = _strip(c)
        ops = {exp.GTE: exp.GTE, exp.GT: exp.GT, exp.LT: exp.LT, exp.LTE: exp.LTE, exp.EQ: exp.EQ}
        flip = {exp.GTE: exp.LTE, exp.GT: exp.LT, exp.LT: exp.GT, exp.LTE: exp.GTE, exp.EQ: exp.EQ}
        if isinstance(c, exp.Between):
            parts = [(exp.GTE, c.this, c.args.get("low")), (exp.LTE, c.this, c.args.get("high"))]
        elif type(c) in ops:
            lhs, rhs = c.this, c.expression
            if _shifted_time(lhs) is None and _shifted_time(rhs) is not None:
                parts = [(flip[type(c)], rhs, lhs)]
            else:
                parts = [(type(c), lhs, rhs)]
        else:
            return None
        spec = None
        bounds: list[tuple[type, Any]] = []
        for op, side, value in parts:
            sh = _shifted_time(side)
            if sh is None or value is None or not is_constant(value):
                return None
            if spec is not None and (sh[0], sh[2]) != (spec[0], spec[2]):
                return None
            spec = sh
            try:
                v = self.const_eval(value)
            except Untranslatable:
                return None
            if isinstance(v, str):
                try:
                    v = dt.datetime.fromisoformat(v)
                except ValueError:
                    return None
            if isinstance(v, dt.date) and not isinstance(v, dt.datetime):
                v = dt.datetime.combine(v, dt.time())
            if not isinstance(v, dt.datetime):
                return None
            bounds.append((op, v.replace(tzinfo=None)))
        ts_name, ymd_expr, key = spec
        picked = _date_source(ymd_expr, ctx)
        if picked is None:
            return None
        src, kind, fmt = picked
        dates = self._pd_dates.get(src.name)
        if dates is None:
            if self.enumerate_values is None or kind == "date":
                return None
            values = self.enumerate_values(ctx.meta.name, base_query, src,
                                           self.MAX_TIME_DATES * 5)
            if values is None:
                return None
            dates = {d for d in (_to_ymd(v, fmt) for v in values) if d is not None}
            self._pd_dates[src.name] = dates   # the other bound of the range reuses them
        shifts = {d: calendar.shift_days(d, key) for d in dates}
        shifts = {d: n for d, n in shifts.items() if n is not None}
        if len(shifts) > self.MAX_TIME_DATES:
            raise ProgrammingError(
                f"A time range on {spec_label(spec)} spans {len(shifts)} position dates: filter "
                f"the position label as well (e.g. IN ('D-1', 'W-1', 'W-2')).")
        if not shifts:
            notes.append("range on the aligned time: no position date in play, nothing to read")
            return MATCH_NONE
        alts = []
        for d, n in sorted(shifts.items()):
            col = exp.column(src.name, quoted=True)
            day = _ymd_date(d)
            conds: list[exp.Expression] = (
                [exp.EQ(this=col, expression=exp.Literal.string(day.strftime(fmt)))]
                if kind == "keyword" else
                [exp.GTE(this=col, expression=_ts_literal(day)),
                 exp.LT(this=col.copy(), expression=_ts_literal(day + dt.timedelta(days=1)))])
            possible = True
            for op, v in bounds:
                for cop, bound in _minute_bounds(op, v - dt.timedelta(days=n)):
                    if cop is None:
                        possible = False
                        break
                    lit = exp.cast(exp.Literal.string(bound.isoformat(sep=" ")),
                                   exp.DataType.Type.TIMESTAMP)
                    conds.append(cop(this=exp.column(ts_name, quoted=True), expression=lit))
            if possible:
                alts.append(and_all(conds))
        if not alts:
            notes.append("aligned time range matches no minute: nothing to read")
            return MATCH_NONE
        expr = alts[0]
        for a in alts[1:]:
            expr = exp.or_(expr, a, copy=False)
        try:
            q = predicate(expr, ctx).true
        except Untranslatable:
            return None
        notes.append(f"aligned time range -> {ts_name} ranges for {len(shifts)} position date(s) "
                     f"pushed down")
        return q

    # ------------------------------------------------------------------ #
    # single-column filters that OpenSearch cannot express: enumerate the column
    # ------------------------------------------------------------------ #
    def _enumerate_residual(self, preds, ctx: Ctx, meta: TableMeta, pushed: list[dict],
                            notes: list[str], where_label: str) -> list[exp.Expression]:
        """Try to turn each non-translatable WHERE term that depends on one keyword
        column (a CASE mapping, a Python function such as a business-date label,
        LOWER/SUBSTRING...) into `column IN (<values for which it is true>)`.

        The distinct values of the column (under the other, pushed filters) are
        listed with one small aggregation, the term is evaluated on each of them
        in DuckDB, and the equivalent terms filter is pushed to OpenSearch."""
        out: list[exp.Expression] = []
        # label filters first: they tell which position dates a POSITION_TIME range spans
        preds = sorted(preds, key=lambda p: 0 if any(
            isinstance(n, exp.Anonymous) and n.name.lower() == "osagg_label"
            for n in p[0].walk()) else 1)
        for c, why in preds:
            base = q_and(pushed) if pushed else MATCH_ALL
            q = self._label_filter(c, ctx, base, notes)
            if q is None:
                q = self._position_time_filter(c, ctx, base, notes)
            if q is None and self.enumerate_values is not None and self.const_query is not None:
                try:
                    q = self._enumerate_one(c, ctx, meta, base, notes)
                except Untranslatable:
                    q = None
            if q is not None:
                pushed.append(q)
            else:
                out.append(c)
                notes.append(f"WHERE term {where_label}: {c.sql(dialect='duckdb')} ({why})")
        return out

    def _enumerate_one(self, c: exp.Expression, ctx: Ctx, meta: TableMeta, base_query: dict,
                       notes: list[str]) -> dict | None:
        if any(isinstance(n, (exp.Select, exp.Subquery, exp.Window, exp.AggFunc))
               for n in c.walk()):
            return None
        cols = [col for col in c.find_all(exp.Column) if not isinstance(col.this, exp.Star)]
        fields = {}
        for col in cols:
            f = ctx.field_of(col)
            if f is None:
                return None
            fields[f.name] = f
        if len(fields) != 1:
            return None
        f = next(iter(fields.values()))
        if (f.agg_field is None and not _same_kind_text(f)) or f.is_date or f.name == "_id":
            return None
        values = self.enumerate_values(meta.name, base_query, f, self.settings.enum_max_values)
        if values is None:
            return None
        if not values:
            notes.append(f"filter on {f.name}: no value in range, nothing to read")
            return MATCH_NONE

        def lit(v: Any) -> str:
            if v is None:
                return "NULL"
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"
            if isinstance(v, (int, float)):
                return repr(v)
            return "'" + str(v).replace("'", "''") + "'"

        sub = c.copy()
        sub = sub.transform(lambda n: exp.column("__osagg_v")
                            if isinstance(n, exp.Column) and ctx.field_of(n) is not None else n)
        rows_sql = ", ".join(f"({lit(v)})" for v in values)
        sql = (f"SELECT __osagg_v FROM (VALUES {rows_sql}) AS __osagg_t(__osagg_v) "
               f"WHERE {sub.sql(dialect='duckdb')}")
        try:
            matched = [r[0] for r in self.const_query(sql)]
        except Exception as ex:  # pylint: disable=broad-except
            raise Untranslatable(f"cannot evaluate {c.sql()} on {f.name} values: {ex}") from ex
        non_null = [v for v in matched if v is not None]
        label_fn = [n for n in c.walk() if isinstance(n, exp.Anonymous) and n.name.lower() == "osagg_label"]
        picked = _date_source(label_fn[0].expressions[0], ctx) if label_fn and label_fn[0].expressions else None
        if picked is not None and picked[0].name == f.name:
            ymd = {d for d in (_to_ymd(v, picked[2]) for v in non_null) if d is not None}
            prev = self._pd_dates.get(f.name)
            self._pd_dates[f.name] = ymd if prev is None else prev & ymd
        parts: list[dict] = []
        if non_null:
            parts.append(_in_values(f, non_null, ctx))
        if len(non_null) != len(matched):
            parts.append(q_not(q_exists(f)))
        notes.append(f"filter {c.sql(dialect='duckdb')[:120]} -> {f.name} IN "
                     f"({len(non_null)} of {len(values)} value(s)) pushed down")
        return q_or(parts) if parts else MATCH_NONE

    def _plan_select(self, select: exp.Select, table: exp.Table, meta: TableMeta):
        self._pd_dates: dict[str, set[str]] = {}   # position dates kept by label filters
        ctx = self._ctx(table, meta)
        self._check_columns(select, ctx, table)
        self._expand_virtual(select, ctx)
        is_agg = bool(select.args.get("group")) or any(_contains_agg(e) for e in select.expressions) \
            or bool(select.args.get("having"))
        distinct = select.args.get("distinct")
        if not is_agg and distinct is not None and not distinct.args.get("on") \
                and not any(isinstance(e, exp.Star) for e in select.expressions):
            # SELECT DISTINCT a, b  ==  SELECT a, b ... GROUP BY a, b
            select = select.copy()
            select.set("distinct", None)
            select.set("group", exp.Group(expressions=[
                (e.this if isinstance(e, exp.Alias) else e).copy() for e in select.expressions]))
            is_agg = True
        if is_agg:
            return self._plan_agg(select, table, meta, ctx)
        return self._plan_docs(select, table, meta, ctx)

    # ------------------------------------------------------------------ #
    # aggregations
    # ------------------------------------------------------------------ #
    def _group_exprs(self, select: exp.Select) -> list[exp.Expression]:
        group = select.args.get("group")
        if group is None:
            return []
        if group.args.get("rollup") or group.args.get("cube") or group.args.get("grouping_sets"):
            raise PushdownError("ROLLUP / CUBE / GROUPING SETS are not supported")
        if group.args.get("all"):
            return [(e.this if isinstance(e, exp.Alias) else e).copy()
                    for e in select.expressions if not _contains_agg(e)]
        projs = select.expressions
        out = []
        for g in group.expressions:
            g0 = _strip(g)
            if isinstance(g0, exp.Literal) and not g0.is_string:
                idx = int(g0.this) - 1
                if not 0 <= idx < len(projs):
                    raise ProgrammingError(f"GROUP BY position {idx + 1} is not in select list")
                p = projs[idx]
                out.append((p.this if isinstance(p, exp.Alias) else p).copy())
                continue
            if isinstance(g0, exp.Column) and not g0.table and self._ctx_meta is not None \
                    and self._ctx_meta.resolve(g0.name) is None:
                # not a table column: GROUP BY <select alias>
                hit = [p for p in projs if isinstance(p, exp.Alias) and p.alias == g0.name]
                if hit:
                    out.append(hit[0].this.copy())
                    continue
            out.append(g.copy())
        return out

    def _plan_agg(self, select: exp.Select, table: exp.Table, meta: TableMeta, ctx: Ctx):
        notes: list[str] = []
        self._ctx_meta = meta
        group_exprs = self._group_exprs(select)

        # WHERE
        pushed: list[dict] = []
        residual_preds: list[exp.Expression] = []
        done: list[exp.Expression] = []
        where = select.args.get("where")
        for c in conjuncts(where.this if where is not None else None):
            try:
                pushed.append(predicate(c, ctx).true)
                done.append(c)
            except Untranslatable as ex:
                residual_preds.append((c, ex))
        residual_preds = self._enumerate_residual(residual_preds, ctx, meta, pushed, notes,
                                                  "evaluated after aggregation")
        query = q_and(pushed) if pushed else MATCH_ALL

        # aggregate calls anywhere in the select
        agg_nodes: list[exp.Expression] = []
        for part in list(select.expressions) + [select.args.get("having"), select.args.get("order"),
                                                select.args.get("qualify")]:
            if part is None:
                continue
            for a in _aggs_in(part):
                if _key(a) not in {_key(x) for x in agg_nodes}:
                    agg_nodes.append(a)

        # group keys
        direct_keys: list[tuple[exp.Expression, GroupKey | None]] = [
            (g, group_key(g, ctx)) for g in group_exprs]
        all_direct = all(k is not None for _, k in direct_keys) and not residual_preds

        specs: dict[str, AggSpec] = {}
        failed: list[str] = []
        for i, a in enumerate(agg_nodes):
            try:
                specs[_key(a)] = aggregate(a, ctx, f"a{i}")
            except NotNumeric as ex:      # invalid on any engine: say so before reading anything
                raise ProgrammingError(str(ex)) from ex
            except Untranslatable as ex:
                failed.append(f"{a.sql(dialect='duckdb')}: {ex}")

        decomposable = not failed and all(specs[_key(a)].decomposable for a in agg_nodes)
        dst_merge = all_direct and self._needs_dst_merge([k for _, k in direct_keys])
        if all_direct and not failed and not (dst_merge and decomposable):
            if dst_merge:
                notes.append("sub-day buckets in a DST time zone: the repeated hour at the end of "
                             "summer time is returned as two rows (non-decomposable aggregate)")
            if not group_exprs:
                return self._global_agg(select, table, meta, ctx, query, agg_nodes, specs, notes)
            return self._direct_agg(select, meta, ctx, query, direct_keys, agg_nodes, specs, notes)

        # ---- two-level: push the columns used by non-translatable parts as keys
        if decomposable:
            if dst_merge:
                notes.append("sub-day buckets in a DST time zone: buckets of the repeated hour "
                             "are merged in DuckDB")
            try:
                new, scan = self._two_level_agg(select, meta, ctx, query, group_exprs, direct_keys,
                                                residual_preds, agg_nodes, specs, notes)
            except Untranslatable as ex:
                notes.append(f"two-level pushdown impossible: {ex}")
            else:
                if dst_merge and select.args.get("order") is None and scan.keys:
                    # LIMIT n without ORDER BY: any n complete groups will do; a bucket can
                    # only merge with another in the repeated autumn hour (see executor)
                    first = scan.keys[0][1]
                    n = self._early_stop(select, scan.keys, {})
                    if n is not None and first.field is not None and first.field.is_date:
                        scan.stop_after = n
                        scan.stop_dst_key = scan.keys[0][0]
                return new, scan
        elif not failed and not residual_preds:
            # non-decomposable aggregates: accept ES offsets for shifted weeks (approximate
            # on DST days) rather than scanning documents
            ctx.allow_shift_offset = True
            keys2 = [(g, group_key(g, ctx)) for g in group_exprs]
            if all(k is not None for _, k in keys2):
                notes.append("shifted week buckets use an OpenSearch offset (boundaries may be "
                             "off by one hour on DST days)")
                return self._direct_agg(select, meta, ctx, query, keys2, agg_nodes, specs, notes)
        if failed:
            notes.extend(f"aggregate not pushable: {f}" for f in failed)
        else:
            notes.append("non-decomposable aggregate over non-pushable group keys / filters")
        return self._fallback_scan(select, table, meta, ctx, pushed, notes, done)

    def _needs_dst_merge(self, keys: list[GroupKey | None]) -> bool:
        """Sub-day buckets in a zone with DST: the repeated local hour yields two buckets
        with the same wall-clock key, which SQL (naive local timestamps) merges."""
        if not _has_dst(self.settings.tz):
            return False
        for k in keys:
            if k is None:
                continue
            if k.kind == "date_histogram":
                body = k.source["date_histogram"]
                if "fixed_interval" in body or body.get("calendar_interval") in ("1s", "1m", "1h"):
                    return True
            elif k.kind == "terms" and k.field is not None and k.field.is_date:
                return True
        return False

    def _global_agg(self, select, table, meta, ctx, query, agg_nodes, specs, notes):
        name = self._new_table()
        cols: list[OutCol] = []
        aggs: dict[str, dict] = {}
        repl: dict[str, str] = {}
        bucket_aggs = 0
        for i, a in enumerate(agg_nodes):
            spec = specs[_key(a)]
            aggs.update(spec.aggs)
            bucket_aggs += spec.bucket_aggs
            cname = f"a{i}"
            cols.append(OutCol(cname, spec.sql_type, spec.extract))
            repl[_key(a)] = cname
        scan = AggScan(table=name, index=meta.name, query=query, keys=[], aggs=aggs, columns=cols,
                       mode="global", bucket_aggs=bucket_aggs, notes=notes)
        new = self._rewrite(select, name, {}, repl, drop_group=True)
        return new, scan

    def _direct_agg(self, select, meta, ctx, query, direct_keys, agg_nodes, specs, notes):
        name = self._new_table()
        cols: list[OutCol] = []
        keys: list[tuple[str, GroupKey]] = []
        key_repl: dict[str, str] = {}
        seen: dict[str, str] = {}
        for i, (g, k) in enumerate(direct_keys):
            gk = _key(g)
            if gk in seen:
                continue
            kname = f"k{i}"
            seen[gk] = kname
            keys.append((kname, k))
            cols.append(OutCol(kname, k.sql_type, _key_extractor(kname, k, ctx)))
            key_repl[gk] = kname
        aggs: dict[str, dict] = {}
        agg_repl: dict[str, str] = {}
        bucket_aggs = 0
        for i, a in enumerate(agg_nodes):
            spec = specs[_key(a)]
            aggs.update(spec.aggs)
            bucket_aggs += spec.bucket_aggs
            cname = f"a{i}"
            cols.append(OutCol(cname, spec.sql_type, spec.extract))
            agg_repl[_key(a)] = cname
        stop_after = self._early_stop(select, keys, key_repl)
        scan = AggScan(table=name, index=meta.name, query=query, keys=keys, aggs=aggs, columns=cols,
                       mode="direct", stop_after=stop_after, bucket_aggs=bucket_aggs, notes=notes,
                       heavy=sum(specs[_key(a)].heavy for a in agg_nodes))
        if stop_after is None and self.settings.topn == "approx":
            self._try_topn(select, scan, agg_nodes, specs)
        new = self._rewrite(select, name, key_repl, agg_repl, drop_group=True)
        return new, scan

    def _try_topn(self, select: exp.Select, scan: AggScan, agg_nodes, specs) -> None:
        """ORDER BY <aggregate> [DESC] LIMIT n on one terms key: a single terms
        aggregation ordered by the metric (approximate on multi-shard indices,
        one pass instead of paging every group)."""
        if len(scan.keys) != 1 or select.args.get("having") is not None or _has_window(select) \
                or select.args.get("distinct") is not None or select.args.get("qualify"):
            return
        kname, key = scan.keys[0]
        if key.kind != "terms" or key.field is None or key.field.is_date:
            return
        limit, order = select.args.get("limit"), select.args.get("order")
        if limit is None or order is None:
            return
        try:
            n = int(limit.expression.this)
            off = int(select.args["offset"].expression.this) if select.args.get("offset") else 0
        except (AttributeError, ValueError, TypeError):
            return
        if n + off > 10_000:
            return
        first = order.expressions[0]
        target = first.this
        aliases = {e.alias: e.this for e in select.expressions if isinstance(e, exp.Alias)}
        if isinstance(target, exp.Column) and not target.table and target.name in aliases:
            target = aliases[target.name]
        spec = specs.get(_key(target))
        if spec is None or spec.order_path is None:
            return
        size = max(1, n + off)          # LIMIT 0 (Superset reading a dataset's columns): OpenSearch refuses size 0
        terms: dict[str, Any] = {"field": key.source["terms"]["field"], "size": size,
                                 "shard_size": max(1_000, 10 * size),
                                 "order": [{spec.order_path: "desc" if first.args.get("desc") else "asc"},
                                           {"_key": "asc"}]}
        if key.field.sql_type == "VARCHAR":
            terms["missing"] = NULL_KEY
        scan.mode = "topn"
        scan.topn = terms
        scan.notes.append(f"approximate top-{size}: terms aggregation ordered by {spec.order_path} "
                          f"(shard_size {terms['shard_size']})")

    def _early_stop(self, select: exp.Select, keys, key_repl) -> int | None:
        """LIMIT n with no ORDER BY (or ORDER BY exactly the composite key order):
        stop paging after n (+offset) buckets."""
        limit = select.args.get("limit")
        if limit is None or select.args.get("having") is not None or select.args.get("qualify"):
            return None
        try:
            n = int(limit.expression.this) if isinstance(limit, exp.Limit) else None
        except (AttributeError, ValueError, TypeError):
            return None
        if n is None:
            return None
        off = 0
        offset = select.args.get("offset")
        if offset is not None:
            try:
                off = int(offset.expression.this)
            except (AttributeError, ValueError, TypeError):
                return None
        if select.args.get("distinct") is not None or _has_window(select):
            return None
        order = select.args.get("order")
        if order is None:
            return n + off
        # ORDER BY must be a prefix of the keys (in composite source order)
        aliases = {e.alias: e.this for e in select.expressions if isinstance(e, exp.Alias)}
        key_names = [k for k, _ in keys]
        projs = select.expressions
        for pos, o in enumerate(order.expressions):
            target = _strip(o.this)
            if isinstance(target, exp.Literal) and not target.is_string:
                idx = int(target.this) - 1          # ORDER BY <position>
                if not 0 <= idx < len(projs):
                    return None
                target = projs[idx].this if isinstance(projs[idx], exp.Alias) else projs[idx]
            if isinstance(target, exp.Column) and not target.table and target.name in aliases:
                target = aliases[target.name]
            kname = key_repl.get(_key(target))
            if kname is None or pos >= len(key_names) or key_names[pos] != kname:
                return None
            src = dict(keys)[kname].source
            body = next(iter(src.values()))
            body["order"] = "desc" if o.args.get("desc") else "asc"
            # composite: null bucket first for asc unless told otherwise; match SQL
            body["missing_order"] = "first" if o.args.get("nulls_first") else "last"
        return n + off

    def _two_level_agg(self, select, meta, ctx, query, group_exprs, direct_keys, residual_preds,
                       agg_nodes, specs, notes):
        # base keys: directly translatable group expressions + columns referenced by the rest
        base: list[tuple[exp.Expression, GroupKey]] = []
        base_keys: dict[str, str] = {}

        def add_key(expr: exp.Expression, k: GroupKey) -> None:
            kk = _key(expr)
            if kk in base_keys:
                return
            base_keys[kk] = f"k{len(base)}"
            base.append((expr, k))

        for g, k in direct_keys:
            if k is not None:
                add_key(g, k)
        needs_cols: list[exp.Column] = []

        def own_columns(node: exp.Expression) -> list[exp.Column]:
            # columns of this SELECT only (skip nested subqueries, already planned)
            return [c for c in node.find_all(exp.Column)
                    if c.find_ancestor(exp.Select, exp.Subquery) in (None, select)
                    and not isinstance(c.this, exp.Star)]

        for g, k in direct_keys:
            if k is None:
                needs_cols.extend(own_columns(g))
        for p in residual_preds:
            needs_cols.extend(own_columns(p))
        date_grains: dict[str, str] = {}
        for col in needs_cols:
            f = ctx.field_of(col)
            if f is None:
                raise Untranslatable(f"unknown column {col.sql()}")
            if f.is_date:
                # timestamps are pushed as a date_histogram at the grain the expression needs
                g = _required_grain(col)
                if g is None:
                    raise Untranslatable(f"expression over raw timestamp column {f.name} "
                                         "(use DATE_TRUNC / TIME_BUCKET / EXTRACT / strftime)")
                prev = date_grains.get(f.name)
                if prev is None or GRAINS.index(g) < GRAINS.index(prev):
                    date_grains[f.name] = g
                continue
            k = group_key(exp.column(f.name, quoted=True), ctx)
            if k is None:
                raise Untranslatable(f"column {f.name} cannot be used as a group key")
            add_key(exp.column(f.name, quoted=True), k)
        for fname, g in date_grains.items():
            trunc = exp.TimestampTrunc(this=exp.column(fname, quoted=True), unit=exp.Var(this=g))
            k = group_key(trunc, ctx)
            if k is None:
                raise Untranslatable(f"cannot bucket {fname} by {g}")
            add_key(exp.column(fname, quoted=True), k)
        if not base and group_exprs:
            raise Untranslatable("no pushable group keys")

        name = self._new_table()
        cols: list[OutCol] = []
        keys: list[tuple[str, GroupKey]] = []
        for expr, k in base:
            kname = base_keys[_key(expr)]
            keys.append((kname, k))
            cols.append(OutCol(kname, k.sql_type, _key_extractor(kname, k, ctx)))
        aggs: dict[str, dict] = {}
        agg_repl: dict[str, exp.Expression] = {}
        bucket_aggs = 0
        for i, a in enumerate(agg_nodes):
            spec = specs[_key(a)]
            aggs.update(spec.aggs)
            bucket_aggs += spec.bucket_aggs
            pnames = []
            for j, (ptype, pex) in enumerate(spec.partials):
                pname = f"a{i}_{j}"
                cols.append(OutCol(pname, ptype, pex))
                pnames.append(f'"{pname}"')
            combined = spec.combine.format(*pnames)
            agg_repl[_key(a)] = combined
        notes.append("two-level aggregation: OpenSearch groups by base columns, DuckDB re-aggregates")
        scan = AggScan(table=name, index=meta.name, query=query, keys=keys, aggs=aggs, columns=cols,
                       mode="two-level", bucket_aggs=bucket_aggs, notes=notes,
                       heavy=sum(specs[_key(a)].heavy for a in agg_nodes))

        # residual: original select over the bucket table; columns -> key columns
        col_repl: dict[str, str] = {}
        for expr, _k in base:
            col_repl[_key(expr)] = base_keys[_key(expr)]
        new = select.copy()
        new.set("where", None)
        from sqlglot import parse_one

        def tx(node: exp.Expression) -> exp.Expression:
            if not _replaceable(node) or isinstance(node, (exp.Select, exp.Subquery)):
                return node
            if node.find_ancestor(exp.Subquery) is not None:
                return node
            kk = _key(node)
            if kk in agg_repl:
                return exp.Paren(this=parse_one(agg_repl[kk], read="duckdb"))
            if kk in col_repl and not node.find_ancestor(exp.Window):
                return exp.column(col_repl[kk], quoted=True)
            if isinstance(node, exp.Column) and not isinstance(node.this, exp.Star):
                f = ctx.field_of(node)
                if f is not None:
                    kn = col_repl.get(_key(exp.column(f.name, quoted=True)))
                    if kn is not None:
                        return exp.column(kn, quoted=True)
            return node

        for key in ("expressions", "group", "having", "order", "qualify"):
            val = new.args.get(key)
            if val is None:
                continue
            if isinstance(val, list):
                new.set(key, [v.transform(tx) for v in val])
            else:
                new.set(key, val.transform(tx))
        new.set("expressions", _preserve_names(select.expressions, new.expressions))
        resid = [p.transform(tx) for p in residual_preds]
        w = and_all(resid)
        new.set("where", exp.Where(this=w) if w is not None else None)
        self._replace_from(new, name)
        return new, scan

    def _fallback_scan(self, select, table, meta, ctx, pushed, notes, done=()):
        """Fetch the needed columns of matching documents (capped) and let DuckDB do it all."""
        fields = self._needed_fields(select, ctx)
        name = self._new_table()
        query = q_and(pushed) if pushed else MATCH_ALL
        notes.append(f"FULL SCAN: {len(fields)} column(s) of matching documents are fetched and "
                     f"aggregated in DuckDB (cap {self.settings.max_scan_rows:,} rows)")
        scan = DocScan(table=name, index=meta.name, query=query, fields=fields, sort=None, limit=None,
                       notes=notes)
        new = select.copy()
        # remove pushed conjuncts? keep all: re-evaluating them is harmless and exact (but on a
        # field the indices map with different types, which DuckDB cannot compare as one type)
        _drop_mixed(new, done, ctx)
        self._replace_from(new, name, table.alias)
        return new, scan

    # ------------------------------------------------------------------ #
    # plain documents
    # ------------------------------------------------------------------ #
    def _needed_fields(self, select: exp.Select, ctx: Ctx) -> list[Field]:
        if any(isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star))
               for e in select.expressions):
            return [f for f in ctx.meta.fields.values() if not f.virtual]
        names: list[str] = []
        for col in select.find_all(exp.Column):
            if col.find_ancestor(exp.Select) is not select or isinstance(col.this, exp.Star):
                continue
            f = ctx.field_of(col)
            if f is not None and not f.virtual and f.name not in names:
                names.append(f.name)
        if not names:  # SELECT COUNT(*) FROM t with fallback, SELECT 1 FROM t
            return []
        return [ctx.meta.fields.get(n) or ctx.meta.resolve(n) for n in names]

    def _plan_docs(self, select: exp.Select, table: exp.Table, meta: TableMeta, ctx: Ctx):
        notes: list[str] = []
        fields = self._needed_fields(select, ctx)
        pushed: list[dict] = []
        residual = []
        done: list[exp.Expression] = []
        where = select.args.get("where")
        for c in conjuncts(where.this if where is not None else None):
            try:
                pushed.append(predicate(c, ctx).true)
                done.append(c)
            except Untranslatable as ex:
                residual.append((c, ex))
        residual = self._enumerate_residual(residual, ctx, meta, pushed, notes, "evaluated in DuckDB")
        query = q_and(pushed) if pushed else MATCH_ALL

        # ORDER BY pushdown
        sort: list[dict] | None = None
        order = select.args.get("order")
        order_ok = True
        if order is not None:
            sort = []
            aliases = {e.alias: e.this for e in select.expressions if isinstance(e, exp.Alias)}
            projs = select.expressions
            for o in order.expressions:
                t = _strip(o.this)
                if isinstance(t, exp.Literal) and not t.is_string:      # ORDER BY <position>
                    idx = int(t.this) - 1
                    if not 0 <= idx < len(projs) or isinstance(projs[idx], exp.Star):
                        order_ok = False
                        break
                    t = _strip(projs[idx].this if isinstance(projs[idx], exp.Alias) else projs[idx])
                elif isinstance(t, exp.Column) and not t.table and t.name in aliases:
                    t = _strip(aliases[t.name])
                desc, nulls_first = bool(o.args.get("desc")), bool(o.args.get("nulls_first"))
                f = ctx.field_of(t)
                if f is not None and f.agg_field is not None:
                    sort.append({f.agg_field: {"order": "desc" if desc else "asc",
                                               "missing": "_first" if nulls_first else "_last"}})
                    continue
                try:
                    sort.append(sort_script(t, ctx, desc, nulls_first))
                except Untranslatable as ex:
                    order_ok = False
                    notes.append(f"ORDER BY evaluated in DuckDB: {ex}")
                    break
            if not order_ok:
                sort = None

        limit_rows: int | None = None
        limit = select.args.get("limit")
        if limit is not None and not residual and order_ok and select.args.get("distinct") is None \
                and not _has_window(select) and select.args.get("qualify") is None:
            try:
                n = int(limit.expression.this)
                off = 0
                if select.args.get("offset") is not None:
                    off = int(select.args["offset"].expression.this)
                limit_rows = n + off
            except (AttributeError, ValueError, TypeError):
                limit_rows = None
        name = self._new_table()
        scan = DocScan(table=name, index=meta.name, query=query, fields=fields, sort=sort,
                       limit=limit_rows, notes=notes)
        new = select.copy()
        _drop_mixed(new, done, ctx)
        self._replace_from(new, name, table.alias)
        return new, scan

    # ------------------------------------------------------------------ #
    # rewriting
    # ------------------------------------------------------------------ #
    @staticmethod
    def _replace_from(select: exp.Select, name: str, alias: str | None = None) -> None:
        tbl = exp.Table(this=exp.to_identifier(name))
        if alias:
            tbl.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))
        select.set(FROM_KEY, exp.From(this=tbl))

    def _rewrite(self, select: exp.Select, name: str, key_repl: dict[str, str],
                 agg_repl: dict[str, str], drop_group: bool) -> exp.Select:
        new = select.copy()
        new.set("where", None)

        def tx(node: exp.Expression) -> exp.Expression:
            if not _replaceable(node):
                return node
            kk = _key(node)
            if kk in agg_repl:
                return exp.column(agg_repl[kk], quoted=True)
            if kk in key_repl and not isinstance(node, exp.Literal):
                return exp.column(key_repl[kk], quoted=True)
            return node

        for key in ("expressions", "having", "order", "qualify"):
            val = new.args.get(key)
            if val is None:
                continue
            if isinstance(val, list):
                new.set(key, [v.transform(tx) for v in val])
            else:
                new.set(key, val.transform(tx))
        new.set("expressions", _preserve_names(select.expressions, new.expressions))
        if drop_group:
            group = new.args.get("group")
            new.set("group", None)
            having = new.args.get("having")
            if having is not None:
                new.set("having", None)
                new.set("where", exp.Where(this=having.this))
            del group
        self._replace_from(new, name)
        return new


def _replaceable(node: exp.Expression) -> bool:
    """Only value expressions may be swapped for scan columns - never identifiers,
    aliases, literals, stars or ORDER BY wrappers."""
    if not isinstance(node, exp.Expression):
        return False
    if isinstance(node, (exp.Identifier, exp.Star, exp.Literal, exp.Ordered, exp.Alias,
                         exp.TableAlias, exp.Var, exp.DataType, exp.Interval)):
        return False
    if node.arg_key == "alias":
        return False
    return True


def _in_values(f: Field, values: list[Any], ctx: Ctx | None = None) -> dict:
    uniq = list(dict.fromkeys(values))
    if f.variants and ctx is not None:               # mapped differently across the indices: per index
        lits = [exp.Literal.string(str(v)) if isinstance(v, str) else exp.Literal.number(v) for v in uniq]
        return predicate(exp.In(this=exp.column(f.name, quoted=True), expressions=lits), ctx).true
    fld = f.agg_field
    if len(uniq) == 1:
        return {"term": {fld: uniq[0]}}
    return {"terms": {fld: uniq}}


def _same_kind_text(f: Field) -> bool:
    """A field the indices map as text here, keyword there (each with an exact field): one kind of value."""
    return bool(f.variants) and all(v.sql_type == "VARCHAR" and v.agg_field for _, v in f.variants)


def _has_dst(tz) -> bool:
    try:
        y = dt.datetime.now().year
        a = dt.datetime(y, 1, 15, tzinfo=tz).utcoffset()
        b = dt.datetime(y, 7, 15, tzinfo=tz).utcoffset()
        return a != b
    except Exception:  # pylint: disable=broad-except
        return True


GRAINS = ["SECOND", "MINUTE", "HOUR", "DAY", "MONTH", "QUARTER", "YEAR"]
_PART_GRAIN = {
    "YEAR": "YEAR", "YEARS": "YEAR", "YYYY": "YEAR", "ISOYEAR": "DAY", "QUARTER": "QUARTER",
    "MONTH": "MONTH", "MONTHS": "MONTH", "WEEK": "DAY", "WEEKS": "DAY", "WEEKOFYEAR": "DAY",
    "DAY": "DAY", "DAYS": "DAY", "DOW": "DAY", "ISODOW": "DAY", "DOY": "DAY", "DAYOFWEEK": "DAY",
    "DAYOFYEAR": "DAY", "DAYOFMONTH": "DAY", "YEARWEEK": "DAY", "HOUR": "HOUR", "HOURS": "HOUR",
    "MINUTE": "MINUTE", "MINUTES": "MINUTE", "SECOND": "SECOND", "SECONDS": "SECOND",
    "DECADE": "YEAR", "CENTURY": "YEAR", "MILLENNIUM": "YEAR", "ERA": "YEAR",
}
_FN_GRAIN_NAMES = {
    "Year": "YEAR", "Quarter": "QUARTER", "Month": "MONTH", "Week": "DAY", "WeekOfYear": "DAY",
    "DayOfWeek": "DAY", "DayOfWeekIso": "DAY", "DayOfMonth": "DAY", "DayOfYear": "DAY",
    "Dayname": "DAY", "Monthname": "MONTH", "Hour": "HOUR", "Minute": "MINUTE", "Second": "SECOND",
}
# classes differ between sqlglot versions (Superset 6.0 pins 27.x, 6.1 pins 28.x)
_FN_GRAIN = {getattr(exp, n): g for n, g in _FN_GRAIN_NAMES.items() if hasattr(exp, n)}
_ANON_GRAIN = {n.upper(): g for n, g in _FN_GRAIN_NAMES.items()}
_ANON_GRAIN.update({"DAY": "DAY", "ISODOW": "DAY", "DAYOFMONTH": "DAY", "YEARWEEK": "DAY",
                    "ISOYEAR": "DAY", "WEEKDAY": "DAY", "EPOCH": "SECOND"})
_STRFTIME_GRAIN = [
    ("SECOND", "SsfTXcgn"), ("MINUTE", "MR"), ("HOUR", "HIpkl"),
    ("DAY", "dejaAwuUWVxDFG"), ("MONTH", "mbBh"), ("YEAR", "Yy"),
]


def _required_grain(col: exp.Column) -> str | None:
    """Coarsest date_histogram grain on which the expression around `col` is constant."""
    parent = col.parent
    while isinstance(parent, exp.Paren):
        parent = parent.parent
    shift_grain = None
    while isinstance(parent, (exp.Add, exp.Sub)) and isinstance(parent.expression, exp.Interval):
        unit = parent.expression.args.get("unit")
        uname = (unit.name if unit is not None else "").upper().rstrip("S")
        g = {"DAY": "DAY", "WEEK": "DAY", "HOUR": "HOUR", "MINUTE": "MINUTE",
             "SECOND": "SECOND", "MONTH": "DAY", "YEAR": "DAY"}.get(uname)
        if g is None:
            return None
        shift_grain = g if shift_grain is None or GRAINS.index(g) < GRAINS.index(shift_grain) else shift_grain
        parent = parent.parent
        while isinstance(parent, exp.Paren):
            parent = parent.parent
    base = _grain_of_parent(parent)
    if base is None or shift_grain is None:
        return base
    return base if GRAINS.index(base) < GRAINS.index(shift_grain) else shift_grain


def _shifted_time(node: exp.Expression) -> tuple[str, str, str] | None:
    """DATE_TRUNC('minute', "ts") + INTERVAL (osagg_shift("POSITION_DATE", 'key')) DAY
    -> (ts, date field, key)."""
    node = _strip(node)
    if not isinstance(node, exp.Add) or not isinstance(node.expression, exp.Interval):
        return None
    ts, iv = _strip(node.this), node.expression
    if isinstance(ts, (exp.TimestampTrunc, exp.DateTrunc)):
        unit = ts.args.get("unit")
        if unit is None or unit.name.strip("'\"").upper() != "MINUTE":
            return None
        ts = _strip(ts.this)
    fn = _strip(iv.this) if iv.this is not None else None
    unit = iv.args.get("unit")
    if not (isinstance(ts, exp.Column) and isinstance(fn, exp.Anonymous)
            and fn.name.lower() == "osagg_shift" and len(fn.expressions) == 2
            and isinstance(fn.expressions[1], exp.Literal) and unit is not None
            and unit.name.upper() == "DAY"):
        return None
    return ts.name, fn.expressions[0], fn.expressions[1].this


def _yyyymmdd(opts: dict) -> exp.Expression:
    """SQL giving the position date as 'yyyymmdd' from its stored field."""
    col = exp.column(opts["source"], quoted=True)
    ymd = exp.Literal.string("%Y%m%d")
    if opts.get("kind") == "date":
        return exp.TimeToStr(this=col, format=ymd)
    fmt = opts.get("format") or "%Y%m%d"
    if fmt == "%Y%m%d":
        return col
    parsed = exp.Anonymous(this="try_strptime", expressions=[col, exp.Literal.string(fmt)])
    return exp.TimeToStr(this=parsed, format=ymd)


def _mixed_types(node: exp.Expression, ctx: Ctx) -> bool:
    """The condition reads a field the indices map with different types (a date here, a keyword there)."""
    for col in node.find_all(exp.Column):
        f = ctx.field_of(col)
        if f is not None and f.variants and len({v.sql_type for _, v in f.variants}) > 1:
            return True
    return False


def _drop_mixed(new: exp.Select, done: Any, ctx: Ctx) -> None:
    """The conditions OpenSearch applied exactly, per index, on fields the indices map with different
    types, are not evaluated again in DuckDB: their raw values there have no single type."""
    where = new.args.get("where")
    drop = {c.sql() for c in done or () if _mixed_types(c, ctx)}
    if where is None or not drop:
        return
    w = and_all([c for c in conjuncts(where.this) if c.sql() not in drop])
    new.set("where", exp.Where(this=w) if w is not None else None)


def _date_source(node: exp.Expression, ctx: Ctx) -> tuple[Field, str, str] | None:
    """Inverse of _yyyymmdd: (field, kind, format) behind a position-date expression."""
    node = _strip(node)
    if isinstance(node, exp.Column):
        f = ctx.field_of(node)
        if f is not None and (f.agg_field is not None or _same_kind_text(f)) and not f.virtual \
                and f.sql_type == "VARCHAR":
            return f, "keyword", "%Y%m%d"
        return None
    if isinstance(node, exp.TimeToStr):
        fmt = node.args.get("format")
        if not (isinstance(fmt, exp.Literal) and fmt.this == "%Y%m%d"):
            return None
        inner = _strip(node.this)
        if isinstance(inner, exp.Column):
            f = ctx.field_of(inner)
            if f is not None and f.is_date and not f.virtual:
                return f, "date", "%Y%m%d"
        elif isinstance(inner, exp.Anonymous) and inner.name.lower() == "try_strptime" \
                and len(inner.expressions) == 2 and isinstance(inner.expressions[1], exp.Literal):
            col = _strip(inner.expressions[0])
            f = ctx.field_of(col) if isinstance(col, exp.Column) else None
            if f is not None and (f.agg_field is not None or _same_kind_text(f)) and f.sql_type == "VARCHAR":
                return f, "keyword", inner.expressions[1].this
    return None


def _ymd_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y%m%d").date()


def _to_ymd(value: Any, fmt: str) -> str | None:
    """A stored position-date value -> 'yyyymmdd' (None if it is not a date)."""
    if not isinstance(value, str):
        return None
    try:
        return dt.datetime.strptime(value, fmt).strftime("%Y%m%d")
    except ValueError:
        return None


def _ts_literal(day: dt.date) -> exp.Expression:
    return exp.cast(exp.Literal.string(f"{day.isoformat()} 00:00:00"), exp.DataType.Type.TIMESTAMP)


def _minute_bounds(op: type, v: dt.datetime) -> list[tuple[type | None, dt.datetime]]:
    """DATE_TRUNC('minute', ts) <op> v, as conditions on the raw ts."""
    floor = v.replace(second=0, microsecond=0)
    ceil = floor if v == floor else floor + dt.timedelta(minutes=1)
    nxt = floor + dt.timedelta(minutes=1)
    if op is exp.GTE:
        return [(exp.GTE, ceil)]
    if op is exp.GT:
        return [(exp.GTE, nxt)]
    if op is exp.LT:
        return [(exp.LT, ceil)]
    if op is exp.LTE:
        return [(exp.LT, nxt)]
    if v != floor:                       # EQ on a value inside a minute: never true
        return [(None, v)]
    return [(exp.GTE, floor), (exp.LT, nxt)]


def spec_label(spec: tuple) -> str:
    return f'the aligned time ("{spec[0]}" moved onto the D-1 position date)'


_BIN_GRAIN = {"SECOND": "SECOND", "MINUTE": "MINUTE", "HOUR": "HOUR", "DAY": "DAY",
              "WEEK": "DAY", "MONTH": "MONTH", "QUARTER": "MONTH", "YEAR": "MONTH"}


def _grain_of_parent(parent: exp.Expression | None) -> str | None:
    # TIME_BUCKET(INTERVAL 'n unit', col): the buckets start on unit boundaries
    date_bin = getattr(exp, "DateBin", None)
    iv = None
    if date_bin is not None and isinstance(parent, date_bin):
        iv = parent.this
    elif isinstance(parent, exp.Anonymous) and parent.name.upper() == "TIME_BUCKET" \
            and parent.expressions:
        iv = parent.expressions[0]
    if iv is not None:
        parts = _interval_parts(iv)
        return _BIN_GRAIN.get(parts[1].rstrip("S")) if parts else None
    if isinstance(parent, exp.Extract):
        return _PART_GRAIN.get(parent.this.name.upper())
    for cls, g in _FN_GRAIN.items():
        if isinstance(parent, cls):
            return g
    if isinstance(parent, exp.Anonymous) and parent.name.upper() in ("DATE_PART", "DATEPART") \
            and parent.expressions and isinstance(parent.expressions[0], exp.Literal):
        return _PART_GRAIN.get(parent.expressions[0].this.upper())
    if isinstance(parent, exp.Anonymous) and parent.name.upper() in _ANON_GRAIN:
        return _ANON_GRAIN[parent.name.upper()]
    if isinstance(parent, (exp.TimestampTrunc, exp.DateTrunc)):
        unit = parent.args.get("unit")
        name = (unit.name if unit is not None else "").strip("'\"").upper()
        return "DAY" if name == "WEEK" else _PART_GRAIN.get(name)
    if isinstance(parent, exp.Cast) and parent.to.this == exp.DataType.Type.DATE:
        return "DAY"
    if isinstance(parent, exp.TimeToStr):
        fmt = parent.args.get("format")
        if isinstance(fmt, exp.Literal):
            directives = set(re.findall(r"%-?([A-Za-z])", fmt.this))
            for grain, chars in _STRFTIME_GRAIN:
                if directives & set(chars):
                    return grain
            return "YEAR"
    return None


def _preserve_names(orig: list[exp.Expression], new: list[exp.Expression]) -> list[exp.Expression]:
    """Keep the output column names of unaliased projections after rewriting."""
    out = []
    for o, n in zip(orig, new):
        if isinstance(o, (exp.Alias, exp.Star)) or (isinstance(o, exp.Column) and isinstance(o.this, exp.Star)):
            out.append(n)
            continue
        if n is o or _key(n) == _key(o):
            out.append(n)
            continue
        name = o.name if isinstance(o, exp.Column) else o.sql(dialect="duckdb")
        out.append(exp.alias_(n, name, quoted=True))
    return out


def _key_extractor(kname: str, k: GroupKey, ctx: Ctx) -> Callable[[dict], Any]:
    f = k.field
    tz = zone_of(f, ctx.tz)
    if k.kind == "date_histogram":
        shift = k.shift
        as_date = k.as_date

        def ex(b: dict) -> Any:
            v = b["key"][kname]
            if v is None:
                return None
            t = (dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(milliseconds=v)) \
                .astimezone(tz).replace(tzinfo=None)
            if shift:
                t = t + shift
            return t.date() if as_date else t

        return ex
    if k.kind == "histogram":
        return lambda b: b["key"][kname]
    if f is not None and f.is_date:
        def exd(b: dict) -> Any:
            v = b["key"][kname]
            if v is None:
                return None
            return (dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(milliseconds=v)) \
                .astimezone(tz).replace(tzinfo=None)
        return exd
    if f is not None and f.is_integer:
        return lambda b: None if b["key"][kname] is None else int(b["key"][kname])
    if f is not None and f.sql_type == "DOUBLE":
        return lambda b: None if b["key"][kname] is None else float(b["key"][kname])
    if f is not None and f.sql_type == "BOOLEAN":
        return lambda b: None if b["key"][kname] is None else bool(b["key"][kname])
    return lambda b: b["key"][kname]
