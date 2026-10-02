"""Joins of OpenSearch indices, pushed down by eager aggregation.

OpenSearch cannot join. For an aggregating query such as

    SELECT b."TEAM", COUNT(*), SUM(a."JOB_DURATION_d")
    FROM jobs a JOIN apps b ON a."APPLICATION" = b."APPLICATION"
    WHERE a."STATUS_INFO" = 'FAILED' GROUP BY 1

each index is aggregated in OpenSearch by its own group columns plus its join keys
(with its own filters), and DuckDB joins the grouped rows. Every group carries its
document count, so aggregates are combined exactly; with n indices the multiplicity of
a group is the product of the other indices' group counts:

    COUNT(*)   = SUM(cnt_a * cnt_b)        SUM(a.x)  = SUM(sum_a_x * cnt_b)
    COUNT(a.x) = SUM(cnt_a_x * cnt_b)      MIN/MAX   = MIN/MAX of the group values
    AVG(a.x)   = SUM(sum_a_x * cnt_b) / SUM(cnt_a_x * cnt_b)

Huge indices: what grows is the number of distinct join keys, not the number of
documents. The indices are aggregated one after the other, smallest first (document
count under the filters), and the join keys found so far are pushed to the next ones as
a `terms` filter, so a huge index is only grouped for the keys that can match. Every
index must be bounded: at most join_max_keys matching documents, or keys received from
an index aggregated before it, or at most join_max_keys distinct join keys (cardinality
aggregation). Otherwise the query is refused before anything is read. Keys are only
pushed where that cannot drop rows of the result (not into the preserved side of a LEFT
JOIN). Anything that would need raw documents (row lists, COUNT DISTINCT or
percentiles across the join, conditions mixing indices, keys of different types) is
refused with the reason.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from sqlglot import exp

from osagg.errors import PushdownError

TERMS_MAX = 65_536        # values per terms clause (index.max_terms_count)
COMPARISONS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)
HUGEINT = exp.DataType.build("HUGEINT", dialect="duckdb")   # counts of huge x huge joins


class JoinRefused(PushdownError):
    pass


def refusal(why: str) -> JoinRefused:
    return JoinRefused(f"This JOIN cannot run in OpenSearch: {why}. osagg joins indices on equal "
                       "fields in aggregating queries (each index is grouped in OpenSearch, the "
                       "grouped rows are joined); it never pulls raw documents for a join.")


def terms_filter(agg_field: str, values: list) -> dict:
    """values IN (...) as terms clauses of at most TERMS_MAX values."""
    if len(values) <= TERMS_MAX:
        return {"terms": {agg_field: values}}
    return {"bool": {"should": [{"terms": {agg_field: values[i:i + TERMS_MAX]}}
                                for i in range(0, len(values), TERMS_MAX)],
                     "minimum_should_match": 1}}


def _unparen(e: exp.Expression) -> exp.Expression:
    while isinstance(e, (exp.Paren, exp.Cast)):
        e = e.this
    return e


def _null_rejecting(c: exp.Expression) -> bool:
    """Never true on a NULL-extended row: a comparison on a bare column, IS NOT NULL..."""
    c = _unparen(c)
    if isinstance(c, exp.Or):
        return _null_rejecting(c.this) and _null_rejecting(c.expression)
    if isinstance(c, exp.Not):
        inner = _unparen(c.this)
        return isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null) \
            and isinstance(_unparen(inner.this), exp.Column)
    if isinstance(c, COMPARISONS):
        return isinstance(_unparen(c.this), exp.Column) or isinstance(_unparen(c.expression), exp.Column)
    if isinstance(c, (exp.In, exp.Like, exp.ILike, exp.Between)):
        return isinstance(_unparen(c.this), exp.Column)
    return False


def _lookup_rows(select: exp.Select, tabs: list, edges: list, side_of: Callable[[exp.Column], int],
                 count_docs: Callable[[exp.Select], int] | None, max_keys: int,
                 group_id: str) -> exp.Select:
    """Row list over a join, for extracts only: one index may be big, every other one must have
    at most max_keys matching documents (read whole); their join keys are pushed into the big
    one as terms filters, and ORDER BY / LIMIT too when that is exact."""
    from osagg.planner import FROM_KEY, _preserve_names, and_all

    if any(isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star))
           for e in select.expressions):
        raise refusal("list the columns of a row list over a join (no SELECT *)")
    limit = select.args.get("limit")
    limit_value = limit.expression if isinstance(limit, exp.Limit) else None
    if not (isinstance(limit_value, exp.Literal) and not limit_value.is_string):
        raise refusal("a row list over a join needs a LIMIT")
    n = len(tabs)
    out_aliases = {e.alias for e in select.expressions if isinstance(e, exp.Alias)}

    def alias_ref(c: exp.Column) -> bool:
        return not c.table and c.name in out_aliases and c.find_ancestor(exp.Order, exp.Qualify) is not None

    values: list[dict[str, str]] = [{} for _ in range(n)]      # field -> column of its subquery

    def value(t: int, name: str) -> str:
        if name not in values[t]:
            values[t][name] = f"__v{t}_{len(values[t])}"
        return values[t][name]

    for part in [*select.expressions, select.args.get("order"), select.args.get("qualify")]:
        for c in (part.find_all(exp.Column) if part is not None else []):
            if not isinstance(c.this, exp.Star) and not alias_ref(c):
                value(side_of(c), c.name)
    for p, cp, i, ci in edges:
        value(p, cp.name)
        value(i, ci.name)

    def probe(t: int) -> exp.Select:
        sub = exp.Select(expressions=[exp.Literal.number(1)])
        sub.set(FROM_KEY, exp.From(this=tabs[t].table.copy()))
        w = and_all([c.copy() for c in tabs[t].conds])
        if w is not None:
            sub.set("where", exp.Where(this=w))
        return sub

    docs = [count_docs(probe(t)) if count_docs is not None else 0 for t in range(n)]
    big = [t for t in range(n) if docs[t] > max_keys]
    if len(big) > 1:
        raise refusal("a row list over a join needs every index but one to have at most "
                      f"join_max_keys={max_keys:,} matching documents ("
                      + ", ".join(tabs[t].table.name for t in big) + " have more): add filters")
    drive = big[0] if big else max(range(n), key=lambda t: (docs[t], -t))

    def pushable(src: int) -> list[tuple[str, Any]]:
        """Key columns of src whose values may filter the big index (never into the preserved
        side of a LEFT JOIN; same-typed non-date fields)."""
        out = []
        for p, cp, i, ci in edges:
            if (p, i) == (src, drive):
                src_col, dst_col = cp, ci
            elif (p, i) == (drive, src) and not tabs[src].optional:
                src_col, dst_col = ci, cp
            else:
                continue
            fs, fd = tabs[src].meta.resolve(src_col.name), tabs[drive].meta.resolve(dst_col.name)
            if not fd.is_date and fs.sql_type == fd.sql_type:
                out.append((values[src][src_col.name], fd))
        return out

    # exact LIMIT pushdown: each other index joined straight to the big one on one field, as the
    # optional side of a LEFT JOIN or as an INNER JOIN whose keys filter the big index, and an
    # ORDER BY on the big index only: every big row then gives at least one result row
    order = select.args.get("order")
    order_items = order.expressions if order is not None else []
    push_limit = (not tabs[drive].optional and not select.args.get("distinct")
                  and not select.args.get("qualify") and all(
                      len([e for e in edges if t in (e[0], e[2])]) == 1
                      and any(drive in (e[0], e[2]) for e in edges if t in (e[0], e[2]))
                      and (tabs[t].optional or pushable(t))
                      for t in range(n) if t != drive)
                  and all(any(True for _ in o.find_all(exp.Column))
                          and all(not isinstance(c.this, exp.Star) and not alias_ref(c)
                                  and side_of(c) == drive for c in o.find_all(exp.Column))
                          for o in order_items))
    offset = select.args.get("offset")
    offset_value = offset.expression if isinstance(offset, exp.Offset) else None
    if offset is not None and not (isinstance(offset_value, exp.Literal) and not offset_value.is_string):
        push_limit = False

    ranks = sorted((t for t in range(n) if t != drive), key=lambda t: (docs[t], t)) + [drive]

    def side_select(t: int) -> exp.Select:
        tab = tabs[t]
        sub = exp.Select(expressions=[exp.alias_(exp.column(f, table=tab.name, quoted=True), a, quoted=True)
                                      for f, a in values[t].items()])
        sub.set(FROM_KEY, exp.From(this=tab.table.copy()))
        w = and_all([c.copy() for c in tab.conds])
        if w is not None:
            sub.set("where", exp.Where(this=w))
        rank = ranks.index(t)
        pushes = []
        if t == drive:
            pushes = [{"src": s, "src_index": tabs[s].table.name, "col": c, "field": f.agg_field}
                      for s in ranks[:-1] for c, f in pushable(s)]
            if push_limit:
                if order_items:
                    sub.set("order", order.copy())
                total = int(limit_value.this) + (int(offset_value.this) if offset_value is not None else 0)
                sub.set("limit", exp.Limit(expression=exp.Literal.number(total)))
        note = (f"join (rows): {docs[t]:,} matching document(s), read whole" if t != drive else
                "join (rows): the big index" + (", receives the keys of " + ", ".join(dict.fromkeys(
                    p["src_index"] for p in pushes)) if pushes else "")
                + (", ORDER BY / LIMIT pushed" if push_limit else ""))
        sub.meta["osagg_join"] = {"group": group_id, "id": t, "rank": rank, "push_from": pushes,
                                  "fed": False, "rows": True, "key_fields": [], "note": note}
        return sub

    def col(t: int, name: str) -> exp.Column:
        return exp.column(values[t][name], table=f"__j{t}", quoted=True)

    outer = select.copy()
    outer.set(FROM_KEY, exp.From(this=exp.Subquery(
        this=side_select(0), alias=exp.TableAlias(this=exp.to_identifier("__j0")))))
    new_joins = []
    for t in range(1, n):
        on = and_all([exp.EQ(this=col(p, cp.name), expression=col(i, ci.name))
                      for p, cp, i, ci in edges if i == t])
        new_joins.append(exp.Join(this=exp.Subquery(this=side_select(t),
                                                    alias=exp.TableAlias(this=exp.to_identifier(f"__j{t}"))),
                                  on=on, side="LEFT" if tabs[t].optional else None))
    outer.set("joins", new_joins)
    outer.set("where", None)

    def tx(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column) and not isinstance(node.this, exp.Star) and not alias_ref(node):
            return col(side_of(node), node.name)
        return node

    for arg in ("expressions", "order", "qualify"):
        val = outer.args.get(arg)
        if val is None:
            continue
        outer.set(arg, [v.transform(tx) for v in val] if isinstance(val, list) else val.transform(tx))
    outer.set("expressions", _preserve_names(select.expressions, outer.expressions))
    return outer


@dataclass
class _Tab:
    table: exp.Table
    meta: Any
    name: str                     # alias, or index name
    optional: bool = False        # right side of a LEFT JOIN
    conds: list = field(default_factory=list)
    keys: list = field(default_factory=list)       # its join-key columns
    parts: list = field(default_factory=list)      # projections of its grouped subquery
    ngroup: int = 0


def rewrite_join(select: exp.Select, first: exp.Table, first_meta: Any, joins: list[exp.Join],
                 lookup: Callable[[exp.Table], Any],
                 count_docs: Callable[[exp.Select], int] | None,
                 estimate_keys: Callable[[exp.Select, list[Any]], int] | None,
                 max_keys: int, group_id: str, allow_rows: bool = False) -> exp.Select:
    """Return an equivalent SELECT over aggregated subqueries (see module doc).

    count_docs(probe) -> matching documents, capped just above max_keys; estimate_keys(
    probe, fields) -> distinct key combinations; probe is SELECT 1 FROM index WHERE <its
    conditions>. Each subquery carries meta["osagg_join"] (order, keys to receive)."""
    from osagg.planner import FROM_KEY, _aggs_in, _key, _preserve_names, _strip, and_all, conjuncts

    # ---- indices, in join order (a RIGHT JOIN of two indices becomes a LEFT JOIN)
    tabs = [_Tab(first, first_meta, first.alias_or_name)]
    ons: list[tuple[int, exp.Expression | None, list]] = []
    for j in joins:
        kind = (j.args.get("kind") or "").upper()
        side = (j.args.get("side") or "").upper()
        if kind in ("CROSS", "SEMI", "ANTI") or side == "FULL" or (j.args.get("on") is None
                                                                  and not j.args.get("using")):
            raise refusal(f"{(side or kind or 'CROSS')} JOIN is not supported")
        if not isinstance(j.this, exp.Table):
            raise refusal("a joined relation must be an index")
        meta = lookup(j.this)
        if meta is None:
            raise refusal(f"{j.this.sql()} is not an OpenSearch index")
        if side == "RIGHT":
            if len(joins) > 1:
                raise refusal("RIGHT JOIN with more than two indices (write it as a LEFT JOIN)")
            tabs = [_Tab(j.this, meta, j.this.alias_or_name), _Tab(first, first_meta, tabs[0].name, True)]
        else:
            tabs.append(_Tab(j.this, meta, j.this.alias_or_name, optional=side == "LEFT"))
        ons.append((len(tabs) - 1, j.args.get("on"), j.args.get("using") or []))
    if len({t.name for t in tabs}) != len(tabs):
        raise refusal("give each index a different alias")
    using_cols: dict[str, int] = {}       # unqualified USING column -> its (left-most) index

    def side_of(col: exp.Column) -> int:
        if col.table:
            for i, t in enumerate(tabs):
                if col.table == t.name:
                    return i
            raise refusal(f"unknown table alias {col.table}")
        if col.name in using_cols:
            return using_cols[col.name]
        found = [i for i, t in enumerate(tabs) if t.meta.resolve(col.name) is not None]
        if len(found) != 1:
            raise refusal(f'column "{col.name}" is {"in several indices: qualify it" if found else "unknown"}')
        return found[0]

    def sides(node: exp.Expression) -> set[int]:
        return {side_of(c) for c in node.find_all(exp.Column) if not isinstance(c.this, exp.Star)}

    def skey(node: exp.Expression) -> str:
        """Structural key that keeps a.x and b.x apart."""
        return _key(node) + "|" + ",".join(str(s) for s in sorted(sides(node)))

    def restrict(t: int, c: exp.Expression, where: str) -> None:
        """A filter on the rows of the join result that only reads index t."""
        if tabs[t].optional:
            if not _null_rejecting(c):
                raise refusal(f"{where} condition {c.sql(dialect='duckdb')} on the right index of a "
                              "LEFT JOIN")
            tabs[t].optional = False          # it rejects NULLs: that LEFT JOIN is an INNER JOIN
        tabs[t].conds.append(c)

    # ---- join keys and per-index conditions
    edges: list[tuple[int, exp.Column, int, exp.Column]] = []     # (earlier, col, joined, col)
    for i, on, using in ons:
        for ident in using:
            prev = [p for p in range(i) if tabs[p].meta.resolve(ident.name) is not None]
            if not prev or tabs[i].meta.resolve(ident.name) is None:
                raise refusal(f'USING ("{ident.name}"): no such field on both sides')
            using_cols.setdefault(ident.name, prev[0])
            edges.append((prev[0], exp.column(ident.name, table=tabs[prev[0]].name, quoted=True),
                          i, exp.column(ident.name, table=tabs[i].name, quoted=True)))
        for c in conjuncts(on):
            c = _strip(c)
            if isinstance(c, exp.EQ) and isinstance(_strip(c.this), exp.Column) \
                    and isinstance(_strip(c.expression), exp.Column):
                l, r = _strip(c.this), _strip(c.expression)
                sl, sr = side_of(l), side_of(r)
                if i in (sl, sr) and sl != sr and min(sl, sr) < i:
                    edges.append((sr, r, i, l) if sl == i else (sl, l, i, r))
                    continue
            s = sides(c)
            if len(s) > 1:
                raise refusal(f"ON condition {c.sql(dialect='duckdb')} is not an equality between a "
                              "field of the joined index and a field of an earlier one")
            t = s.pop() if s else i
            if t == i:
                tabs[i].conds.append(c)           # filters the joined index before the join
            elif tabs[i].optional:
                raise refusal("a condition on a preserved index in the ON clause of a LEFT JOIN")
            else:
                restrict(t, c, "ON")
    for i in range(1, len(tabs)):
        if not any(e[2] == i for e in edges):
            raise refusal(f"no equality between {tabs[i].name} and an earlier index")
    for p, cp, i, ci in edges:
        fp, fi = tabs[p].meta.resolve(cp.name), tabs[i].meta.resolve(ci.name)
        if fp is None or fi is None or fp.agg_field is None or fi.agg_field is None:
            raise refusal(f"join fields {cp.name} / {ci.name} must be aggregatable fields")
        kinds = {("t" if f.is_date else "n" if f.is_numeric else "s") for f in (fp, fi)}
        if len(kinds) != 1:
            raise refusal(f"join fields {cp.name} ({fp.sql_type}) and {ci.name} ({fi.sql_type}) "
                          "have different types")

    where = select.args.get("where")
    for c in conjuncts(where.this if where is not None else None):
        s = sides(c)
        if len(s) > 1:
            raise refusal(f"WHERE condition {c.sql(dialect='duckdb')} mixes several indices")
        restrict(s.pop() if s else 0, c, "WHERE")

    # ---- groups and aggregates
    group = select.args.get("group")
    projs = select.expressions
    group_exprs: list[exp.Expression] = []
    for g in (group.expressions if group is not None else []):
        g0 = _strip(g)
        if isinstance(g0, exp.Literal) and not g0.is_string:
            p = projs[int(g0.this) - 1]
            g0 = p.this if isinstance(p, exp.Alias) else p
        elif isinstance(g0, exp.Column) and not g0.table and g0.name not in using_cols:
            hit = [p for p in projs if isinstance(p, exp.Alias) and p.alias == g0.name]
            if hit and all(t.meta.resolve(g0.name) is None for t in tabs):
                g0 = hit[0].this
        group_exprs.append(g0)
    agg_nodes: list[exp.Expression] = []
    agg_keys: set[str] = set()         # (each key once: a query of thirty aggregates plans in a blink)
    for part in list(projs) + [select.args.get("having"), select.args.get("order")]:
        if part is not None:
            for a in _aggs_in(part):
                key = _key(a)
                if key not in agg_keys:
                    agg_keys.add(key)
                    agg_nodes.append(a)
    if not group_exprs and not agg_nodes:
        if not allow_rows:
            raise refusal("the query returns rows, not aggregates (add GROUP BY / aggregates); a row "
                          "list over a join is only possible in an extract")
        return _lookup_rows(select, tabs, edges, side_of, count_docs, max_keys, group_id)

    gcols: dict[str, tuple[int, str]] = {}
    for n, g in enumerate(group_exprs):
        s = sides(g)
        if len(s) > 1:
            raise refusal(f"GROUP BY {g.sql(dialect='duckdb')} mixes several indices")
        t = s.pop() if s else 0
        gcols[skey(g)] = (t, f"__g{t}_{n}")
        tabs[t].parts.append(exp.alias_(g.copy(), f"__g{t}_{n}", quoted=True))
    keycol: dict[tuple[int, str], str] = {}                 # (index, field) -> key column
    for p, cp, i, ci in edges:
        for t, c in ((p, cp), (i, ci)):
            if (t, c.name) not in keycol:
                keycol[(t, c.name)] = f"__k{t}_{len(tabs[t].keys)}"
                tabs[t].keys.append(c)
                tabs[t].parts.append(exp.alias_(c.copy(), keycol[(t, c.name)], quoted=True))
    for t, tab in enumerate(tabs):
        tab.ngroup = len(tab.parts)
        tab.parts.append(exp.alias_(exp.Count(this=exp.Star()), f"__c{t}", quoted=True))

    def col(t: int, name: str) -> exp.Column:
        return exp.column(name, table=f"__j{t}", quoted=True)

    def mult(t: int) -> exp.Expression:
        c = exp.cast(col(t, f"__c{t}"), HUGEINT)
        return exp.func("COALESCE", c, exp.Literal.number(1)) if tabs[t].optional else c

    def product(ts: list[int]) -> exp.Expression | None:
        out = None
        for t in ts:
            out = mult(t) if out is None else exp.Mul(this=out, expression=mult(t))
        return out

    def total(t: int, name: str) -> exp.Expression:
        others = product([u for u in range(len(tabs)) if u != t])
        v = col(t, name)
        return exp.Sum(this=v if others is None else exp.Mul(this=v, expression=others))

    combined: dict[str, exp.Expression] = {}
    for n, a in enumerate(agg_nodes):
        if isinstance(a, exp.Count) and (a.this is None or isinstance(a.this, exp.Star)):
            combined[skey(a)] = exp.func("COALESCE", exp.Sum(this=product(list(range(len(tabs))))),
                                         exp.Literal.number(0))
            continue
        if isinstance(a, exp.Count) and isinstance(a.this, exp.Distinct):
            raise refusal("COUNT(DISTINCT ...) cannot be combined across a join")
        if not isinstance(a, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)):
            raise refusal(f"{a.sql(dialect='duckdb')} cannot be combined across a join "
                          "(use COUNT, SUM, AVG, MIN or MAX)")
        s = sides(a)
        if len(s) != 1:
            raise refusal(f"{a.sql(dialect='duckdb')} must use the fields of one index")
        t = s.pop()
        arg = a.this.copy()
        if isinstance(a, (exp.Min, exp.Max)):
            name = f"__m{t}_{n}"
            tabs[t].parts.append(exp.alias_(type(a)(this=arg), name, quoted=True))
            combined[skey(a)] = type(a)(this=col(t, name))
            continue
        sname, nname = f"__s{t}_{n}", f"__n{t}_{n}"
        if isinstance(a, (exp.Sum, exp.Avg)):
            tabs[t].parts.append(exp.alias_(exp.Sum(this=arg), sname, quoted=True))
        if isinstance(a, (exp.Count, exp.Avg)):
            tabs[t].parts.append(exp.alias_(exp.Count(this=arg.copy()), nname, quoted=True))
        if isinstance(a, exp.Sum):
            combined[skey(a)] = total(t, sname)
        elif isinstance(a, exp.Count):
            combined[skey(a)] = exp.func("COALESCE", total(t, nname), exp.Literal.number(0))
        else:
            combined[skey(a)] = exp.Div(this=total(t, sname), expression=exp.func(
                "NULLIF", total(t, nname), exp.Literal.number(0)))

    # ---- order of the aggregations: every index bounded, keys shrink the next ones
    def pushable(src: int, dst: int) -> list[tuple[str, Any]]:
        """(key column of src, field of dst) whose values may filter dst: never into the
        preserved side of a LEFT JOIN, only same-typed non-date fields."""
        out = []
        for p, cp, i, ci in edges:
            if (p, i) == (src, dst):
                src_col, dst_col = cp, ci             # into a later index (inner or optional)
            elif (p, i) == (dst, src) and not tabs[src].optional:
                src_col, dst_col = ci, cp             # into an earlier one through an INNER JOIN
            else:
                continue
            fs, fd = tabs[src].meta.resolve(src_col.name), tabs[dst].meta.resolve(dst_col.name)
            if not fd.is_date and fs.sql_type == fd.sql_type:
                out.append((keycol[(src, src_col.name)], fd))
        return out

    def probe(t: int) -> exp.Select:
        sub = exp.Select(expressions=[exp.Literal.number(1)])
        sub.set(FROM_KEY, exp.From(this=tabs[t].table.copy()))
        w = and_all([c.copy() for c in tabs[t].conds])
        if w is not None:
            sub.set("where", exp.Where(this=w))
        return sub

    docs = [count_docs(probe(t)) if count_docs is not None else 0 for t in range(len(tabs))]
    order: list[int] = []
    why: dict[int, str] = {}
    remaining = list(range(len(tabs)))
    while remaining:
        small = [t for t in remaining if docs[t] <= max_keys]
        fed = [t for t in remaining if any(pushable(s, t) for s in order)]
        if small:
            t = min(small, key=lambda u: (docs[u], u))
            why[t] = f"{docs[t]:,} matching document(s)"
        elif fed:
            t = fed[0]
            why[t] = "fed"
        else:
            est: dict[int, int] = {}
            for t in remaining:
                fields = [tabs[t].meta.resolve(c.name) for c in tabs[t].keys]
                est[t] = estimate_keys(probe(t), fields) if estimate_keys is not None else 0
                if est[t] <= max_keys:
                    break
            else:
                t = min(est, key=lambda u: est[u])
                raise refusal(
                    f"{tabs[t].table.name} has about {est[t]:,} distinct join keys under its filters "
                    f"(more than join_max_keys={max_keys:,}) and no smaller index can filter it by "
                    "its keys; add filters (a position date, a time range...) on one of the indices, "
                    "or join on a lower-cardinality field")
            why[t] = f"~{est[t]:,} distinct join keys"
        order.append(t)
        remaining.remove(t)

    def side_select(t: int) -> exp.Select:
        tab = tabs[t]
        sub = exp.Select(expressions=tab.parts)
        sub.set(FROM_KEY, exp.From(this=tab.table.copy()))
        w = and_all([c.copy() for c in tab.conds])
        if w is not None:
            sub.set("where", exp.Where(this=w))
        sub.set("group", exp.Group(expressions=[exp.Literal.number(k + 1) for k in range(tab.ngroup)]))
        rank = order.index(t)
        pushes = [{"src": s, "src_index": tabs[s].table.name, "col": c, "field": f.agg_field}
                  for s in order[:rank] for c, f in pushable(s, t)]
        if pushes:
            note = ("join: receives the join keys of " + ", ".join(dict.fromkeys(
                p["src_index"] for p in pushes)) + " (terms filter)")
        else:
            note = f"join: {why[t]}"
        sub.meta["osagg_join"] = {
            "group": group_id, "id": t, "rank": rank, "push_from": pushes, "fed": why[t] == "fed",
            "key_fields": [tab.meta.resolve(c.name).agg_field for c in tab.keys], "note": note}
        return sub

    outer = select.copy()
    outer.set(FROM_KEY, exp.From(this=exp.Subquery(
        this=side_select(0), alias=exp.TableAlias(this=exp.to_identifier("__j0")))))
    new_joins = []
    for t in range(1, len(tabs)):
        on = and_all([exp.EQ(this=col(p, keycol[(p, cp.name)]), expression=col(i, keycol[(i, ci.name)]))
                      for p, cp, i, ci in edges if i == t])
        new_joins.append(exp.Join(this=exp.Subquery(this=side_select(t),
                                                    alias=exp.TableAlias(this=exp.to_identifier(f"__j{t}"))),
                                  on=on, side="LEFT" if tabs[t].optional else None))
    outer.set("joins", new_joins)
    outer.set("where", None)
    out_aliases = {e.alias for e in select.expressions if isinstance(e, exp.Alias)}

    def tx(node: exp.Expression) -> exp.Expression:
        if isinstance(node, (exp.Literal, exp.Identifier, exp.Star, exp.DataType, exp.Var)):
            return node
        if isinstance(node, exp.Column) and not node.table and node.name in out_aliases \
                and node.find_ancestor(exp.Order, exp.Having, exp.Qualify) is not None:
            return node                   # ORDER BY / HAVING <output alias>
        try:
            k = skey(node)
        except JoinRefused:
            k = None                      # e.g. an output alias inside an expression
        if k in combined and node.find_ancestor(exp.Window) is None:
            return exp.Paren(this=combined[k].copy())
        if k in gcols:
            return col(*gcols[k])
        if isinstance(node, exp.Column) and not isinstance(node.this, exp.Star):
            raise refusal(f'column "{node.name}" is neither grouped nor aggregated')
        return node

    for arg in ("expressions", "having", "order", "qualify"):
        val = outer.args.get(arg)
        if val is None:
            continue
        if isinstance(val, list):
            outer.set(arg, [v.transform(tx) for v in val])
        else:
            outer.set(arg, val.transform(tx))
    outer.set("expressions", _preserve_names(select.expressions, outer.expressions))
    if group is not None:
        outer.set("group", exp.Group(expressions=[col(*gcols[skey(g)]) for g in group_exprs]))
    return outer
