"""SQL expression (sqlglot AST) -> OpenSearch DSL translation.

Three kinds of translation live here:

* predicates  -> query DSL (exact SQL three-valued logic, see ``Pred``)
* group keys  -> composite aggregation sources
* aggregates  -> metric (sub-)aggregations + bucket value extractors

Everything returns ``None`` (or raises ``Untranslatable``) when an expression
cannot be expressed in OpenSearch; the planner then evaluates it in DuckDB.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import math
import re
from dataclasses import dataclass, field
from typing import Any, Callable
from zoneinfo import ZoneInfo

from sqlglot import exp

from osagg.errors import ProgrammingError
from osagg.metadata import Field, TableMeta

EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


class Untranslatable(Exception):
    """Expression cannot be pushed down to OpenSearch."""


class Candidate(Untranslatable):
    """No exact OpenSearch query: `query` keeps a superset of the rows (every row the condition keeps), the
    condition itself is evaluated in DuckDB on the documents' values (a text longer than its keyword)."""

    def __init__(self, message: str, query: dict) -> None:
        super().__init__(message)
        self.query = query


class NotNumeric(Untranslatable):
    """SUM / AVG / STDDEV / VARIANCE of a text field: not valid SQL anywhere, so there is
    no point in fetching documents to compute it; the message tells what to use."""


def _require_numeric(agg: exp.Expression, f: Field) -> None:
    if f.is_numeric:
        return
    fn = type(agg).sql_name()
    if f.sql_type == "VARCHAR":
        raise NotNumeric(
            f'{fn}("{f.name}"): "{f.name}" is a text field (OpenSearch {f.os_type}), it cannot be '
            f'summed or averaged. To count documents use COUNT(*); COUNT("{f.name}") counts the '
            f'documents that have a value and COUNT(DISTINCT "{f.name}") the distinct values. '
            "All of them run inside OpenSearch, on any number of documents.")
    raise Untranslatable(f"{fn}({f.name}): not a numeric field")


# --------------------------------------------------------------------------- #
# context
# --------------------------------------------------------------------------- #
@dataclass
class Ctx:
    meta: TableMeta
    tz: ZoneInfo
    qualifiers: set[str]                       # table name / alias usable as column qualifier
    const_eval: Callable[[exp.Expression], Any]  # evaluates column-free expressions (DuckDB)
    cardinality_precision: int = 3000
    percentile_compression: int = 500
    allow_shift_offset: bool = False           # Sunday weeks via ES offset (approximate on DST days)
    overrides: dict | None = None              # a field as one group of indices maps it (variants)
    distinct_mode: str = "exact"               # COUNT(DISTINCT): exact (default) or approx (sketches)
    exact_distinct: bool = False               # COUNT(DISTINCT) counted exactly (its values as keys)

    def field_of(self, node: exp.Expression) -> Field | None:
        if isinstance(node, exp.Column):
            if node.table and node.table not in self.qualifiers:
                return None
            f = self.meta.resolve(node.name)
            if f is not None and self.overrides and f.name in self.overrides:
                return self.overrides[f.name]
            return f
        return None

    def with_fields(self, views: dict[str, Field]) -> "Ctx":
        return dataclasses.replace(self, overrides={**(self.overrides or {}), **views})


UTC = ZoneInfo("UTC")


def zone_of(f: Field | None, tz: ZoneInfo) -> ZoneInfo:
    """The zone a date field's values and literals are read in: UTC for a date-only field (calendar
    days, stored at 00:00 UTC: '2026-10-01' is that day whatever the connection's zone), else the
    connection's zone."""
    return UTC if f is not None and f.date_only else tz


def _as_day(value: Any, f: Field) -> dt.date | None:
    """A literal written as the date-only field writes its days: 20261001 or '20261001' for yyyyMMdd,
    '01/10/2026' for dd/MM/yyyy (OpenSearch parses them with the field's format; Discover's filters
    too). None: not in that form (ISO strings and dates are read as they are)."""
    pattern = f.date_pattern
    if pattern is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not value.is_integer():
            return None
        text = str(int(value))
    elif isinstance(value, str):
        text = value.strip()
    else:
        return None
    try:
        return dt.datetime.strptime(text, pattern).date()
    except ValueError:
        return None


def columns_in(node: exp.Expression) -> list[exp.Column]:
    return list(node.find_all(exp.Column))


def is_constant(node: exp.Expression) -> bool:
    """Column-free, subquery-free, aggregate-free scalar expression."""
    for n in node.walk():
        if isinstance(n, (exp.Column, exp.Subquery, exp.Select, exp.AggFunc, exp.Window,
                          exp.Star, exp.Placeholder, exp.Parameter)):
            return False
        if isinstance(n, exp.Anonymous) and n.name.lower() in ("random", "uuid", "gen_random_uuid"):
            return False
    return True


# --------------------------------------------------------------------------- #
# values
# --------------------------------------------------------------------------- #
def to_utc_micros(value: Any, tz: ZoneInfo) -> int:
    """Convert a literal (datetime/date/str) to epoch microseconds.

    Naive datetimes are wall-clock times in the connection time zone.
    """
    if isinstance(value, str):
        s = value.strip().replace("Z", "+00:00")
        try:
            value = dt.datetime.fromisoformat(s)
        except ValueError as ex:
            raise Untranslatable(f"not a timestamp literal: {value!r}") from ex
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=tz)
        delta = value - EPOCH
        return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    if isinstance(value, dt.date):
        return to_utc_micros(dt.datetime(value.year, value.month, value.day), tz)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # numeric literal compared with a date column: DuckDB would reject it;
        # we interpret it as epoch milliseconds
        return int(value * 1000)
    raise Untranslatable(f"not a timestamp literal: {value!r}")


def literal_value(node: exp.Expression, ctx: Ctx) -> Any:
    """Python value of a constant expression (raises Untranslatable)."""
    if isinstance(node, exp.Paren):
        return literal_value(node.this, ctx)
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        txt = node.this
        try:
            return int(txt)
        except ValueError:
            return float(txt)
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Cast) and isinstance(node.this, exp.Literal) and node.this.is_string:
        t = node.to.this
        txt = node.this.this.strip()
        try:
            if t in (exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPNTZ,
                     exp.DataType.Type.DATETIME):
                return dt.datetime.fromisoformat(txt)
            if t == exp.DataType.Type.DATE:
                return dt.date.fromisoformat(txt[:10])
        except ValueError:
            pass
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal) and not node.this.is_string:
        v = literal_value(node.this, ctx)
        return -v
    if is_constant(node):
        return ctx.const_eval(node)
    raise Untranslatable(f"not a constant: {node.sql()}")


def coerce_for_field(value: Any, f: Field, ctx: Ctx) -> Any:
    """Convert a Python literal to the representation used in OpenSearch queries."""
    if value is None:
        return None
    if f.is_date:
        if f.date_only:                      # a calendar day: 20261001, '20261001', '2026-10-01', in UTC
            day = _as_day(value, f)
            if day is not None:
                value = day
            elif isinstance(value, (int, float)) and not isinstance(value, bool) \
                    and "epoch_millis" not in (f.date_format or "strict_date_optional_time||epoch_millis"):
                raise ProgrammingError(f'{value!r} is not a day of "{f.name}" (a date in format '
                                       f'{f.date_format}): write it as the field does, or as \'YYYY-MM-DD\'')
        return to_utc_micros(value, zone_of(f, ctx.tz))  # micros; callers convert to ms bounds
    if f.sql_type == "BOOLEAN":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str) and value.lower() in ("true", "false", "t", "f", "1", "0"):
            return value.lower() in ("true", "t", "1")
        raise Untranslatable(f"not a boolean: {value!r}")
    if f.is_numeric:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                try:
                    return float(value)
                except ValueError as ex:
                    raise ProgrammingError(f'"{f.name}" is a number ({f.os_type}): {value!r} is not one') from ex
        if isinstance(value, (dt.date, dt.datetime)):
            raise ProgrammingError(f'"{f.name}" is a number ({f.os_type}), not a date: compare it with a '
                                   f"number (a day written 20261001 is the number 20261001)")
        return value
    # keyword-ish
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat(sep=" ") if isinstance(value, dt.datetime) else value.isoformat()
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


# --------------------------------------------------------------------------- #
# predicates
# --------------------------------------------------------------------------- #
MATCH_ALL: dict = {"match_all": {}}
MATCH_NONE: dict = {"match_none": {}}


def q_and(parts: list[dict]) -> dict:
    parts = [p for p in parts if p != MATCH_ALL]
    if any(p == MATCH_NONE for p in parts):
        return MATCH_NONE
    if not parts:
        return MATCH_ALL
    if len(parts) == 1:
        return parts[0]
    flat: list[dict] = []
    for p in parts:
        b = p.get("bool")
        if b is not None and set(b) == {"filter"}:
            flat.extend(b["filter"])
        else:
            flat.append(p)
    # merge ranges on the same field: {gte: a} AND {lt: b} -> {gte: a, lt: b}
    merged: list[dict] = []
    ranges: dict[str, dict] = {}
    for p in flat:
        r = p.get("range") if len(p) == 1 else None
        if r is not None and len(r) == 1:
            fld, body = next(iter(r.items()))
            prev = ranges.get(fld)
            if prev is not None and prev.get("format") == body.get("format") \
                    and not (set(prev) & set(body)) - {"format"}:
                prev.update(body)
                continue
            body = dict(body)
            ranges[fld] = body
            merged.append({"range": {fld: body}})
            continue
        merged.append(p)
    if len(merged) == 1:
        return merged[0]
    return {"bool": {"filter": merged}}


def q_or(parts: list[dict]) -> dict:
    parts = [p for p in parts if p != MATCH_NONE]
    if any(p == MATCH_ALL for p in parts):
        return MATCH_ALL
    if not parts:
        return MATCH_NONE
    if len(parts) == 1:
        return parts[0]
    return {"bool": {"should": parts, "minimum_should_match": 1}}


def q_not(q: dict) -> dict:
    if q == MATCH_ALL:
        return MATCH_NONE
    if q == MATCH_NONE:
        return MATCH_ALL
    return {"bool": {"must_not": [q]}}


def q_exists(f: Field) -> dict:
    if f.name == "_id":
        return MATCH_ALL
    if f.is_text and f.exact_max and f.agg_field and f.agg_field != f.name and not f.variants:
        # the keyword holds the values up to exact_max characters, the text every value
        return q_or([{"exists": {"field": f.agg_field}}, {"exists": {"field": f.name}}])
    return {"exists": {"field": f.agg_field or f.name}}


def _long_rows(f: Field) -> dict:
    """The documents whose value is longer than the field's keyword (the text without the keyword)."""
    return {"bool": {"filter": [{"exists": {"field": f.name}}], "must_not": [{"exists": {"field": f.agg_field}}]}}


def _too_long(f: Field, v: Any) -> bool:
    """A value the keyword of this text field cannot hold (ignore_above counts UTF-16 units, as Java does)."""
    return bool(f.is_text and f.exact_max and isinstance(v, str)
                and len(v) > f.exact_max // 2 and len(v.encode("utf-16-le")) // 2 > f.exact_max)


def _long_match(f: Field, longs: list[str]) -> dict:
    """The documents whose text may be one of these long values (DuckDB compares the whole value). Probed or
    not, the keyword never holds them: a field that gets its first long value is right at once."""
    return q_and([_long_rows(f), q_or([{"match_phrase": {f.name: {"query": v, "zero_terms_query": "all"}}}
                                       for v in longs])])


@dataclass
class Pred:
    """A predicate compiled for SQL three-valued logic.

    ``true``  - documents where the SQL predicate is TRUE
    ``false`` - documents where the SQL predicate is FALSE (NULL rows excluded)
    WHERE keeps rows where the predicate is TRUE, so ``NOT p`` keeps ``p.false``.
    """

    true: dict
    false: dict


def _exact_field(f: Field) -> str:
    if f.name == "_id":
        return "_id"
    if f.agg_field is None:
        raise Untranslatable(f'"{f.name}" is a text field without keyword sub-field; '
                             "exact comparisons cannot be pushed down")
    return f.agg_field


def _leaf(true_q: dict, fields: list[Field]) -> Pred:
    guard = q_and([q_exists(f) for f in fields])
    return Pred(true=true_q, false=q_and([guard, q_not(true_q)]))


def _ms_bounds(op: str, micros: int) -> tuple[str, int] | None:
    """Translate `field OP value` with microsecond value to an epoch-ms bound."""
    lo = micros // 1000                       # floor
    hi = -((-micros) // 1000)                 # ceil
    if op == "gte":
        return "gte", hi
    if op == "gt":
        return "gt", lo
    if op == "lte":
        return "lte", lo
    if op == "lt":
        return "lt", hi
    raise AssertionError(op)


def _range(f: Field, bounds: dict[str, Any]) -> dict:
    body = dict(bounds)
    if f.is_date:
        body["format"] = "epoch_millis"
    return {"range": {_exact_field(f): body}}


FLIP = {"gt": "lt", "gte": "lte", "lt": "gt", "lte": "gte"}
CMP_OPS = {exp.GT: "gt", exp.GTE: "gte", exp.LT: "lt", exp.LTE: "lte"}


def _cmp(f: Field, op: str, value: Any, ctx: Ctx) -> dict:
    v = coerce_for_field(value, f, ctx)
    if v is None:
        return MATCH_NONE
    if f.is_date:
        op2, ms = _ms_bounds(op, v)
        return _range(f, {op2: ms})
    return _range(f, {op: v})


def _eq(f: Field, value: Any, ctx: Ctx) -> dict:
    v = coerce_for_field(value, f, ctx)
    if v is None:
        return MATCH_NONE
    if _too_long(f, v):
        raise Candidate(f"{f.name}: a value longer than its keyword's {f.exact_max} characters",
                        _long_match(f, [v]))
    if f.is_date:
        if v % 1000:
            return MATCH_NONE  # stored with ms precision
        return _range(f, {"gte": v // 1000, "lte": v // 1000})
    if f.name == "_id":
        return {"ids": {"values": [str(v)]}}
    return {"term": {_exact_field(f): v}}


def _in(f: Field, values: list[Any], ctx: Ctx) -> dict:
    vals = [coerce_for_field(v, f, ctx) for v in values]
    vals = [v for v in vals if v is not None]
    if not vals:
        return MATCH_NONE
    longs = [v for v in vals if _too_long(f, v)]
    if longs:
        shorts = [v for v in vals if not _too_long(f, v)]
        raise Candidate(f"{f.name}: a value longer than its keyword's {f.exact_max} characters",
                        q_or([_long_match(f, longs)] + [{"term": {_exact_field(f): v}} for v in shorts]))
    if f.is_date:
        return q_or([_eq(f, v_raw, ctx) for v_raw in values if v_raw is not None])
    if f.name == "_id":
        return {"ids": {"values": [str(v) for v in vals]}}
    # dedupe, keep order
    seen = []
    for v in vals:
        if v not in seen:
            seen.append(v)
    if len(seen) == 1:
        return {"term": {_exact_field(f): seen[0]}}
    return {"terms": {_exact_field(f): seen}}


def like_to_wildcard(pattern: str, escape: str | None) -> tuple[str, str]:
    """SQL LIKE pattern -> (kind, value) where kind in term|prefix|wildcard."""
    out = []
    literal_only = True
    i = 0
    n = len(pattern)
    while i < n:
        ch = pattern[i]
        if escape and ch == escape and i + 1 < n:
            nxt = pattern[i + 1]
            out.append(("lit", nxt))
            i += 2
            continue
        if ch == "%":
            out.append(("any", None))
            literal_only = False
        elif ch == "_":
            out.append(("one", None))
            literal_only = False
        else:
            out.append(("lit", ch))
        i += 1
    if literal_only:
        return "term", "".join(c for _, c in out)
    # prefix: literal chars followed by a single trailing %
    if out and out[-1][0] == "any" and all(k == "lit" for k, _ in out[:-1]):
        return "prefix", "".join(c for _, c in out[:-1])
    buf = []
    for kind, ch in out:
        if kind == "any":
            buf.append("*")
        elif kind == "one":
            buf.append("?")
        else:
            buf.append("\\" + ch if ch in "*?\\" else ch)
    return "wildcard", "".join(buf)


def _like(f: Field, pattern: Any, escape: str | None, insensitive: bool) -> dict:
    if not isinstance(pattern, str):
        raise Untranslatable("LIKE pattern must be a string literal")
    if f.long_values:                                  # the long values are not in the keyword
        short = _like(dataclasses.replace(f, long_values=False), pattern, escape, insensitive)
        raise Candidate(f"{f.name}: LIKE also over values longer than its keyword's {f.exact_max} characters",
                        q_or([short, _long_rows(f)]))
    fld = _exact_field(f)
    kind, val = like_to_wildcard(pattern, escape)
    if kind == "term":
        body = {"value": val}
    elif kind == "prefix":
        if val == "":
            return q_exists(f)
        body = {"value": val}
    else:
        body = {"value": val}
    if insensitive:
        body["case_insensitive"] = True
    return {kind: {fld: body}}


def _strip(node: exp.Expression) -> exp.Expression:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _date_cast_col(node: exp.Expression, ctx: Ctx) -> Field | None:
    """CAST(date_col AS DATE) / DATE_TRUNC('day', date_col)."""
    node = _strip(node)
    if isinstance(node, exp.Cast) and node.to.this == exp.DataType.Type.DATE:
        f = ctx.field_of(node.this)
        if f is not None and f.is_date:
            return f
    if isinstance(node, (exp.TimestampTrunc, exp.DateTrunc)):
        unit = _unit_name(node)
        if unit == "DAY":
            f = ctx.field_of(node.this)
            if f is not None and f.is_date:
                return f
    return None


def _day_range(f: Field, op: str, value: Any, ctx: Ctx) -> dict:
    """Predicate on the local calendar day of a date column."""
    if isinstance(value, str):
        value = dt.date.fromisoformat(value[:10])
    if isinstance(value, dt.datetime):
        if value.time() != dt.time(0):
            if op == "eq":
                return MATCH_NONE
            # compare with a timestamp: day >= 10:00 means day >= next day
            value = value.date() + (dt.timedelta(days=1) if op in ("gte", "gt") else dt.timedelta(0))
            if op == "gt":
                op = "gte"
            elif op == "lte":
                op = "lt"
        else:
            value = value.date()
    if not isinstance(value, dt.date):
        raise Untranslatable("date comparison with non-date literal")
    zone = zone_of(f, ctx.tz)
    start = to_utc_micros(value, zone) // 1000
    nxt = to_utc_micros(value + dt.timedelta(days=1), zone) // 1000
    if op == "eq":
        return _range(f, {"gte": start, "lt": nxt})
    if op == "gte":
        return _range(f, {"gte": start})
    if op == "gt":
        return _range(f, {"gte": nxt})
    if op == "lt":
        return _range(f, {"lt": start})
    if op == "lte":
        return _range(f, {"lt": nxt})
    raise AssertionError(op)


def _per_index(node: exp.Expression, ctx: Ctx) -> Pred | None:
    """A condition on fields the indices map differently, compiled for each group of indices with
    their own view of those fields (exact field, type, date format), each part restricted to its
    indices: the documents of every index are compared as that index maps them (as Discover's
    filters are), never through another index's exact field."""
    mixed: dict[str, Field] = {}
    for col in columns_in(node):
        f = ctx.field_of(col)
        if f is not None and f.variants and f.name not in (ctx.overrides or {}):
            mixed[f.name] = f
    if not mixed:
        return None
    cells: dict[tuple, list[str]] = {}
    for index in ctx.meta.indices:
        key = tuple(next((v for ix, v in f.variants if index in ix), f.variants[0][1]) for f in mixed.values())
        cells.setdefault(key, []).append(index)          # an index without the field: NULL there, as any view
    trues, falses, wider = [], [], None
    for key, indices in cells.items():
        only = {"terms": {"_index": indices}}
        try:
            p = predicate(node, ctx.with_fields(dict(zip(mixed, key))))
        except Candidate as ex:                       # these indices hold values longer than their keyword
            wider = ex
            trues.append(q_and([only, ex.query]))
            continue
        trues.append(q_and([only, p.true]))
        falses.append(q_and([only, p.false]))
    if wider is not None:                             # the rows it keeps are among these; DuckDB decides
        raise Candidate(str(wider), q_or(trues))
    return Pred(true=q_or(trues), false=q_or(falses))


def predicate(node: exp.Expression, ctx: Ctx) -> Pred:
    """Compile a WHERE-clause expression. Raises Untranslatable."""
    node = _strip(node)
    if not isinstance(node, (exp.And, exp.Or, exp.Not)):
        split = _per_index(node, ctx)
        if split is not None:
            return split

    if isinstance(node, exp.And):
        a, b = predicate(node.this, ctx), predicate(node.expression, ctx)
        return Pred(true=q_and([a.true, b.true]), false=q_or([a.false, b.false]))
    if isinstance(node, exp.Or):
        try:
            a, b = predicate(node.this, ctx), predicate(node.expression, ctx)
        except Candidate as ex:                        # a superset of one side is none of the OR
            raise Untranslatable(str(ex)) from ex
        return Pred(true=q_or([a.true, b.true]), false=q_and([a.false, b.false]))
    if isinstance(node, exp.Not):
        try:
            p = predicate(node.this, ctx)
        except Candidate as ex:                        # nor of its negation
            raise Untranslatable(str(ex)) from ex
        return Pred(true=p.false, false=p.true)
    if is_constant(node):
        v = ctx.const_eval(node)
        if v is None:
            return Pred(true=MATCH_NONE, false=MATCH_NONE)
        return Pred(true=MATCH_ALL, false=MATCH_NONE) if v else Pred(true=MATCH_NONE, false=MATCH_ALL)

    # bare boolean column: WHERE "RELAUNCHED"
    f = ctx.field_of(node)
    if f is not None:
        if f.sql_type != "BOOLEAN":
            raise Untranslatable(f"non-boolean column used as predicate: {f.name}")
        return _leaf({"term": {_exact_field(f): True}}, [f])

    if isinstance(node, exp.Is):
        f = ctx.field_of(_strip(node.this))
        rhs = node.expression
        if f is None:
            raise Untranslatable(node.sql())
        if isinstance(rhs, exp.Null):
            ex = q_exists(f)
            return Pred(true=q_not(ex), false=ex)
        if isinstance(rhs, exp.Boolean) and f.sql_type == "BOOLEAN":
            t = {"term": {_exact_field(f): bool(rhs.this)}}
            # IS TRUE / IS FALSE never yield NULL
            return Pred(true=t, false=q_not(t))
        raise Untranslatable(node.sql())

    if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.NullSafeEQ,
                         exp.NullSafeNEQ)):
        left, right = _strip(node.this), _strip(node.expression)
        fl, fr = ctx.field_of(left), ctx.field_of(right)
        if fl is None and fr is not None:
            left, right, fl = right, left, fr
            flipped = True
        else:
            flipped = False
        if fl is None:
            # expressions such as CAST(ts AS DATE) = DATE '...'
            day_f = _date_cast_col(left, ctx) or (_date_cast_col(right, ctx) if not flipped else None)
            if day_f is not None:
                if _date_cast_col(left, ctx) is None:
                    left, right = right, left
                    flipped = not flipped
                value = literal_value(right, ctx)
                op = {exp.EQ: "eq", exp.GT: "gt", exp.GTE: "gte", exp.LT: "lt", exp.LTE: "lte"}.get(type(node))
                if op is None:
                    raise Untranslatable(node.sql())
                if flipped and op in FLIP:
                    op = FLIP[op]
                return _leaf(_day_range(day_f, op, value, ctx), [day_f])
            lowered = _case_fold(left, right, node, ctx)
            if lowered is not None:
                return lowered
            raise Untranslatable(node.sql())
        if ctx.field_of(right) is not None:
            raise Untranslatable("column-to-column comparison")
        value = literal_value(right, ctx)
        if isinstance(node, exp.NullSafeEQ):
            if value is None:
                ex = q_exists(fl)
                return Pred(true=q_not(ex), false=ex)
            t = _eq(fl, value, ctx)
            return Pred(true=t, false=q_not(t))
        if isinstance(node, exp.NullSafeNEQ):
            if value is None:
                ex = q_exists(fl)
                return Pred(true=ex, false=q_not(ex))
            try:
                t = _eq(fl, value, ctx)
            except Candidate as cand:
                raise Untranslatable(str(cand)) from cand
            return Pred(true=q_not(t), false=t)
        if value is None:  # comparison with NULL is never TRUE nor FALSE
            return Pred(true=MATCH_NONE, false=MATCH_NONE)
        if isinstance(node, exp.EQ):
            return _leaf(_eq(fl, value, ctx), [fl])
        if isinstance(node, exp.NEQ):
            try:
                p = _leaf(_eq(fl, value, ctx), [fl])
            except Candidate as cand:
                raise Untranslatable(str(cand)) from cand
            return Pred(true=p.false, false=p.true)
        op = CMP_OPS[type(node)]
        if flipped:
            op = FLIP[op]
        return _leaf(_cmp(fl, op, value, ctx), [fl])

    if isinstance(node, exp.In):
        if node.args.get("query") is not None or node.args.get("unnest") is not None:
            raise Untranslatable("IN (subquery)")
        f = ctx.field_of(_strip(node.this))
        if f is None:
            day_f = _date_cast_col(node.this, ctx)
            if day_f is not None:
                vals = [literal_value(v, ctx) for v in node.expressions]
                return _leaf(q_or([_day_range(day_f, "eq", v, ctx) for v in vals if v is not None]), [day_f])
            raise Untranslatable(node.sql())
        vals = [literal_value(v, ctx) for v in node.expressions]
        has_null = any(v is None for v in vals)
        t = _in(f, vals, ctx)
        p = _leaf(t, [f])
        if has_null:  # x NOT IN (..., NULL) is never TRUE
            p = Pred(true=p.true, false=MATCH_NONE)
        return p

    if isinstance(node, exp.Between):
        f = ctx.field_of(_strip(node.this))
        if f is None:
            raise Untranslatable(node.sql())
        lo, hi = literal_value(node.args["low"], ctx), literal_value(node.args["high"], ctx)
        if lo is None or hi is None:
            raise Untranslatable("BETWEEN NULL")
        return _leaf(q_and([_cmp(f, "gte", lo, ctx), _cmp(f, "lte", hi, ctx)]), [f])

    like_node = node
    escape = None
    if isinstance(like_node, exp.Escape):
        escape = literal_value(like_node.expression, ctx)
        like_node = like_node.this
    if isinstance(like_node, (exp.Like, exp.ILike)):
        f = ctx.field_of(_strip(like_node.this))
        if f is None:
            raise Untranslatable(node.sql())
        pat = literal_value(like_node.expression, ctx)
        if pat is None:
            return Pred(true=MATCH_NONE, false=MATCH_NONE)
        return _leaf(_like(f, pat, escape, isinstance(like_node, exp.ILike)), [f])

    if isinstance(node, (exp.StartsWith,)):
        f = ctx.field_of(_strip(node.this))
        if f is None:
            raise Untranslatable(node.sql())
        pat = literal_value(node.expression, ctx)
        return _leaf({"prefix": {_exact_field(f): {"value": str(pat)}}}, [f])

    raise Untranslatable(node.sql())


def _case_fold(left: exp.Expression, right: exp.Expression, node: exp.Expression, ctx: Ctx) -> Pred | None:
    """LOWER(col) = 'abc' / UPPER(col) = 'ABC' -> case-insensitive term."""
    if not isinstance(node, (exp.EQ, exp.NEQ)):
        return None
    if isinstance(left, (exp.Lower, exp.Upper)):
        f = ctx.field_of(_strip(left.this))
        if f is None or f.sql_type != "VARCHAR":
            return None
        v = literal_value(right, ctx)
        if v is None:
            return Pred(true=MATCH_NONE, false=MATCH_NONE)
        v = str(v)
        norm = v.lower() if isinstance(left, exp.Lower) else v.upper()
        t = {"term": {_exact_field(f): {"value": v, "case_insensitive": True}}} if norm == v else MATCH_NONE
        p = _leaf(t, [f])
        return p if isinstance(node, exp.EQ) else Pred(true=p.false, false=p.true)
    return None


# --------------------------------------------------------------------------- #
# group keys
# --------------------------------------------------------------------------- #
CAL_UNITS = {
    "SECOND": "1s", "MINUTE": "1m", "HOUR": "1h", "DAY": "1d", "WEEK": "1w",
    "MONTH": "1M", "QUARTER": "1q", "YEAR": "1y",
}
FIXED_UNITS = {
    "SECOND": "s", "SECONDS": "s", "MINUTE": "m", "MINUTES": "m", "HOUR": "h", "HOURS": "h",
    "DAY": "d", "DAYS": "d",
}
UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _unit_name(node: exp.Expression) -> str | None:
    unit = node.args.get("unit")
    if unit is None:
        return None
    name = unit.name if isinstance(unit, (exp.Var, exp.Literal)) else unit.sql()
    name = name.strip("'\"").upper()
    if name.endswith("S") and name[:-1] in CAL_UNITS:
        name = name[:-1]
    return name


def _interval_parts(node: exp.Expression) -> tuple[int, str] | None:
    """INTERVAL '10' MINUTES -> (10, 'MINUTES')."""
    node = _strip(node)
    if not isinstance(node, exp.Interval):
        return None
    this = node.this
    unit = node.args.get("unit")
    if this is None:
        return None
    txt = this.name if isinstance(this, exp.Literal) else this.sql()
    txt = txt.strip()
    if unit is None:
        parts = txt.split()
        if len(parts) != 2:
            return None
        txt, uname = parts
    else:
        uname = unit.name if isinstance(unit, exp.Var) else unit.sql()
    try:
        n = int(float(txt))
    except ValueError:
        return None
    return n, uname.upper()


def _interval_timedelta(node: exp.Expression) -> dt.timedelta | None:
    p = _interval_parts(node)
    if p is None:
        return None
    n, unit = p
    u = unit.rstrip("S") if unit not in ("S",) else unit
    mult = {"SECOND": 1, "MINUTE": 60, "HOUR": 3600, "DAY": 86400, "WEEK": 7 * 86400}.get(u)
    if mult is None:
        return None
    return dt.timedelta(seconds=n * mult)


@dataclass
class GroupKey:
    """One composite source."""

    source: dict                      # composite source body, e.g. {"terms": {...}}
    sql_type: str                     # output SQL type
    kind: str                         # terms | date_histogram | histogram
    shift: dt.timedelta = dt.timedelta(0)   # added to date_histogram keys (Sunday weeks...)
    as_date: bool = False             # CAST(ts AS DATE)
    field: Field | None = None


def group_key(node: exp.Expression, ctx: Ctx) -> GroupKey | None:
    node = _strip(node)
    f = ctx.field_of(node)
    if f is not None:
        if f.long_values:
            return None                                  # long values are not in the keyword: from the documents
        if f.name != "_id" and f.agg_field is None and f.variants:
            return _variant_key(f)
        if f.name == "_id" or f.agg_field is None:
            return None
        return GroupKey({"terms": {"field": f.agg_field, "missing_bucket": True}}, f.sql_type,
                        "terms", field=f)

    # DATE_TRUNC(unit, col [+ INTERVAL]) [+/- INTERVAL]
    outer_shift = dt.timedelta(0)
    core = node
    if isinstance(core, (exp.Add, exp.Sub)):
        td = _interval_timedelta(core.expression)
        if td is not None:
            outer_shift = td if isinstance(core, exp.Add) else -td
            core = _strip(core.this)
    if isinstance(core, (exp.TimestampTrunc, exp.DateTrunc)):
        unit = _unit_name(core)
        inner = _strip(core.this)
        inner_shift = dt.timedelta(0)
        if isinstance(inner, (exp.Add, exp.Sub)):
            td = _interval_timedelta(inner.expression)
            if td is None:
                return None
            inner_shift = td if isinstance(inner, exp.Add) else -td
            inner = _strip(inner.this)
        f = ctx.field_of(inner)
        if f is None or not f.is_date or unit not in CAL_UNITS:
            return None
        body: dict[str, Any] = {"field": f.agg_field, "time_zone": str(zone_of(f, ctx.tz)),
                                "missing_bucket": True}
        if unit == "SECOND":
            body["fixed_interval"] = "1s"
        else:
            body["calendar_interval"] = CAL_UNITS[unit]
        if inner_shift:
            if not ctx.allow_shift_offset:
                # ES applies offsets as fixed milliseconds, which is wrong by one hour on DST
                # days: let the planner bucket by day and roll up in DuckDB instead.
                return None
            # DATE_TRUNC(u, c + s) == bucket(c, offset=-s) + s
            secs = int(-inner_shift.total_seconds())
            body["offset"] = f"{secs}s" if secs < 0 else f"+{secs}s"
        return GroupKey({"date_histogram": body}, "TIMESTAMP", "date_histogram",
                        shift=inner_shift + outer_shift, field=f)
    if outer_shift:
        return None

    # TIME_BUCKET(INTERVAL 'n unit', col)
    if isinstance(node, exp.DateBin):
        iv = _interval_parts(node.this)
        f = ctx.field_of(_strip(node.expression))
        if iv is None or f is None or not f.is_date:
            return None
        if node.args.get("zone") is not None or node.args.get("origin") is not None:
            return None
        n, unit = iv
        u = FIXED_UNITS.get(unit)
        if u is None or n <= 0:
            return None
        secs = n * UNIT_SECONDS[u]
        if 86400 % secs != 0:
            return None  # DuckDB origin (2000-01-03) alignment only matches for day divisors
        body = {"field": f.agg_field, "fixed_interval": f"{n}{u}", "time_zone": str(zone_of(f, ctx.tz)),
                "missing_bucket": True}
        return GroupKey({"date_histogram": body}, "TIMESTAMP", "date_histogram", field=f)

    # CAST(col AS DATE)
    if isinstance(node, exp.Cast) and node.to.this == exp.DataType.Type.DATE:
        f = ctx.field_of(_strip(node.this))
        if f is None or not f.is_date:
            return None
        body = {"field": f.agg_field, "calendar_interval": "1d", "time_zone": str(zone_of(f, ctx.tz)),
                "missing_bucket": True}
        return GroupKey({"date_histogram": body}, "DATE", "date_histogram", as_date=True, field=f)

    # FLOOR(col / n) * n  -> histogram
    if isinstance(node, exp.Mul):
        a, b = _strip(node.this), _strip(node.expression)
        if isinstance(a, exp.Floor) and isinstance(b, exp.Literal) and not b.is_string:
            inner = _strip(a.this)
            if isinstance(inner, exp.Div) and isinstance(_strip(inner.expression), exp.Literal):
                f = ctx.field_of(_strip(inner.this))
                d = literal_value(_strip(inner.expression), ctx)
                m = literal_value(b, ctx)
                if f is not None and f.is_numeric and d == m and d and d > 0:
                    return GroupKey({"histogram": {"field": f.agg_field, "interval": d,
                                                   "missing_bucket": True}},
                                    "DOUBLE", "histogram", field=f)
    return None


VARIANT_KEY = ("for (String n : params.fields) { if (doc.containsKey(n)) { def d = doc[n]; "
               "return d.size() == 0 ? null : d.value; } } return null;")


def _variant_key(f: Field) -> GroupKey | None:
    """GROUP BY a field the indices map with different exact fields of one type (text with a keyword
    sub-field here, keyword there): each document's value from the exact field its index has."""
    views = [v for _, v in f.variants]
    if len({v.sql_type for v in views}) != 1 or any(v.agg_field is None for v in views):
        return None                                   # different types: no common value to group on
    fields = sorted(dict.fromkeys(v.agg_field for v in views), key=lambda n: -n.count("."))
    script = {"lang": "painless", "source": VARIANT_KEY, "params": {"fields": fields}}
    body: dict[str, Any] = {"script": script, "missing_bucket": True}
    if views[0].sql_type == "VARCHAR":
        body["value_type"] = "string"
    return GroupKey({"terms": body}, views[0].sql_type, "terms", field=f)


# --------------------------------------------------------------------------- #
# aggregates
# --------------------------------------------------------------------------- #
@dataclass
class AggSpec:
    """A pushed-down aggregate.

    ``aggs``: named sub-aggregations to add to the bucket.
    ``extract``: bucket -> python value.
    ``partials``: for two-level plans, list of (sql_type, extractor) partial
    columns and a template combining them (``{0}``, ``{1}`` ... placeholders).
    """

    sql_type: str
    aggs: dict[str, dict]
    extract: Callable[[dict], Any]
    decomposable: bool = False
    partials: list[tuple[str, Callable[[dict], Any]]] = field(default_factory=list)
    combine: str = ""
    bucket_aggs: int = 0          # number of bucket-creating sub-aggs (filter/terms), for max_buckets
    heavy: int = 0                # memory-heavy sub-aggs (cardinality, percentiles): smaller pages
    order_path: str | None = None  # terms "order" path for approximate top-N
    distinct_of: Any = None       # COUNT(DISTINCT x) counted exactly: x becomes a key, DuckDB counts it


def _val(name: str) -> Callable[[dict], Any]:
    def ex(b: dict) -> Any:
        v = b[name]["value"]
        return v

    return ex


def _num_field(node: exp.Expression, ctx: Ctx) -> Field:
    f = ctx.field_of(_strip(node))
    if f is None:
        raise Untranslatable(f"aggregate argument is not a column: {node.sql()}")
    return f


def _unwrap_filter(agg: exp.Expression) -> tuple[exp.Expression, exp.Expression | None]:
    if isinstance(agg, exp.Filter):
        where = agg.expression
        cond = where.this if isinstance(where, exp.Where) else where
        return agg.this, cond
    return agg, None


def _case_filter(arg: exp.Expression) -> tuple[exp.Expression, exp.Expression | None] | None:
    """CASE WHEN cond THEN x [ELSE NULL|0] END -> (x, cond)."""
    arg = _strip(arg)
    if not isinstance(arg, exp.Case) or arg.this is not None:
        return None
    ifs = arg.args.get("ifs") or []
    if len(ifs) != 1:
        return None
    default = arg.args.get("default")
    return ifs[0].args["true"], ifs[0].this if default is None or _is_zero_or_null(default) else None


def _is_zero_or_null(node: exp.Expression) -> bool:
    node = _strip(node)
    if isinstance(node, exp.Null):
        return True
    if isinstance(node, exp.Literal) and not node.is_string:
        try:
            return float(node.this) == 0.0
        except ValueError:
            return False
    return False


def _is_one(node: exp.Expression) -> bool:
    node = _strip(node)
    return isinstance(node, exp.Literal) and not node.is_string and node.this in ("1", "1.0")


def _date_value(v: Any, tz: ZoneInfo) -> Any:
    if v is None:
        return None
    return (EPOCH + dt.timedelta(milliseconds=v)).astimezone(tz).replace(tzinfo=None)


def aggregate(node: exp.Expression, ctx: Ctx, name: str) -> AggSpec:
    """Translate an aggregate call. ``name`` prefixes the sub-aggregation names."""
    agg, cond = _unwrap_filter(node)
    agg = _strip(agg)

    # COUNT_IF(cond)
    if isinstance(agg, exp.CountIf):
        if cond is not None:
            raise Untranslatable("COUNT_IF with FILTER")
        cond = agg.this
        agg = exp.Count(this=exp.Star())

    # SUM/COUNT/AVG/MIN/MAX over CASE WHEN cond THEN x [ELSE 0|NULL] END
    post: Callable[[Any], Any] | None = None
    if isinstance(agg, (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)) and cond is None:
        arg = agg.this
        if arg is not None and not isinstance(arg, (exp.Star, exp.Distinct)):
            cf = _case_filter(arg)
            if cf is not None and cf[1] is not None:
                value, c = cf
                default = _strip(arg).args.get("default")
                else_null = default is None or isinstance(_strip(default), exp.Null)
                if isinstance(agg, exp.Sum):
                    # ELSE 0: non-matching rows add 0 -> result is 0 (not NULL) when none match
                    cond = c
                    if _is_one(value):
                        agg = exp.Count(this=exp.Star())
                        if else_null:
                            post = lambda v: None if not v else v  # noqa: E731
                    else:
                        agg = exp.Sum(this=value)
                        if not else_null:
                            post = lambda v: 0 if v is None else v  # noqa: E731
                elif else_null:
                    cond = c
                    if isinstance(agg, exp.Count) and _is_non_null_literal(value):
                        agg = exp.Count(this=exp.Star())
                    else:
                        agg = agg.__class__(this=value)

    spec = _plain_aggregate(agg, ctx, name)
    if cond is not None:
        spec = _wrap_filter(spec, cond, ctx, name)
    if post is not None:
        spec = _map_spec(spec, post)
    return spec


def _is_non_null_literal(node: exp.Expression) -> bool:
    node = _strip(node)
    return isinstance(node, (exp.Literal, exp.Boolean))


def _map_spec(spec: AggSpec, fn: Callable[[Any], Any]) -> AggSpec:
    ex0 = spec.extract
    partials = [(t, (lambda b, _p=p: fn(_p(b)))) for t, p in spec.partials]
    return AggSpec(spec.sql_type, spec.aggs, lambda b: fn(ex0(b)), spec.decomposable, partials,
                   spec.combine, spec.bucket_aggs, spec.heavy)


def _wrap_filter(spec: AggSpec, cond: exp.Expression, ctx: Ctx, name: str) -> AggSpec:
    """Put the aggregation inside a `filter` sub-aggregation."""
    q = predicate(cond, ctx).true
    fname = f"{name}_f"
    inner = spec
    body: dict[str, Any] = {"filter": q}
    if inner.aggs:
        body["aggs"] = inner.aggs

    def ex(b: dict, _inner=inner) -> Any:
        return _inner.extract(b[fname])

    partials = [(t, (lambda b, _p=p: _p(b[fname]))) for t, p in inner.partials]
    return AggSpec(inner.sql_type, {fname: body}, ex, inner.decomposable, partials, inner.combine,
                   bucket_aggs=inner.bucket_aggs + 1, heavy=inner.heavy)


def _plain_aggregate(agg: exp.Expression, ctx: Ctx, name: str) -> AggSpec:
    # COUNT(*), COUNT(1), COUNT(col), COUNT(DISTINCT col)
    if isinstance(agg, exp.Count):
        arg = agg.this
        if arg is None or isinstance(arg, exp.Star) or (_is_one(arg) if arg is not None else False):
            ex = lambda b: b["doc_count"]  # noqa: E731
            return AggSpec("BIGINT", {}, ex, True, [("BIGINT", ex)], "SUM({0})", order_path="_count")
        if isinstance(arg, exp.Distinct):
            exprs = arg.expressions
            if len(exprs) != 1:
                raise Untranslatable("COUNT(DISTINCT a, b)")
            f = _num_field(exprs[0], ctx)
            if (f.agg_field is None and not f.variants) or f.long_values:
                raise Untranslatable(f"COUNT(DISTINCT {f.name}): not every value is aggregatable")
            if ctx.exact_distinct and ctx.distinct_mode != "approx":
                # exact: the values become keys of the buckets, DuckDB counts them
                return AggSpec("BIGINT", {}, lambda b: None, True, [], "", distinct_of=exprs[0])
            if f.agg_field is None:
                raise Untranslatable(f"COUNT(DISTINCT {f.name}): mapped differently across the indices")
            # a sketch, exact below its precision threshold: "_xcard" ones are checked (executor) and the
            # query counted again exactly when one reaches it (dbapi); "_card" (approx mode): an estimate
            n = f"{name}_card" if ctx.distinct_mode == "approx" else f"{name}_xcard"
            body = {"cardinality": {"field": f.agg_field,
                                    "precision_threshold": ctx.cardinality_precision}}
            return AggSpec("BIGINT", {n: body}, _val(n), heavy=1, order_path=n)
        f = _num_field(arg, ctx)
        if f.name == "_id":
            ex = lambda b: b["doc_count"]  # noqa: E731
            return AggSpec("BIGINT", {}, ex, True, [("BIGINT", ex)], "SUM({0})")
        if f.long_values or (f.is_text and f.exact_max and f.agg_field and f.agg_field != f.name):
            n = f"{name}_has"                            # rows with a value, longer than the keyword included
            ex = lambda b, _n=n: b[_n]["doc_count"]  # noqa: E731
            return AggSpec("BIGINT", {n: {"filter": q_exists(f)}}, ex, True, [("BIGINT", ex)], "SUM({0})",
                           bucket_aggs=1)
        if f.agg_field is None:
            raise Untranslatable(f"COUNT({f.name}): field is not aggregatable")
        n = f"{name}_vc"
        ex = _val(n)
        return AggSpec("BIGINT", {n: {"value_count": {"field": f.agg_field}}}, ex, True,
                       [("BIGINT", ex)], "SUM({0})", order_path=n)

    if isinstance(agg, exp.ApproxDistinct):
        f = _num_field(agg.this, ctx)
        if f.agg_field is None:
            raise Untranslatable(f"APPROX_COUNT_DISTINCT({f.name}): field is not aggregatable")
        n = f"{name}_card"
        body = {"cardinality": {"field": f.agg_field, "precision_threshold": ctx.cardinality_precision}}
        return AggSpec("BIGINT", {n: body}, _val(n), heavy=1)

    if isinstance(agg, (exp.Sum, exp.Avg)):
        f = _num_field(agg.this, ctx)
        _require_numeric(agg, f)
        s, c = f"{name}_sum", f"{name}_cnt"
        aggs = {s: {"sum": {"field": f.agg_field}}, c: {"value_count": {"field": f.agg_field}}}
        is_int = f.is_integer

        def sum_ex(b: dict) -> Any:
            if b[c]["value"] == 0:
                return None  # SQL: SUM over no non-NULL values is NULL
            v = b[s]["value"]
            return int(round(v)) if is_int else v

        def cnt_ex(b: dict) -> Any:
            return b[c]["value"]

        if isinstance(agg, exp.Sum):
            return AggSpec("BIGINT" if is_int else "DOUBLE", aggs, sum_ex, True,
                           [("BIGINT" if is_int else "DOUBLE", sum_ex)], "SUM({0})", order_path=s)

        def avg_ex(b: dict) -> Any:
            n = b[c]["value"]
            return None if n == 0 else b[s]["value"] / n

        return AggSpec("DOUBLE", aggs, avg_ex, True,
                       [("DOUBLE", lambda b: None if b[c]["value"] == 0 else b[s]["value"]),
                        ("BIGINT", cnt_ex)],
                       "SUM({0}) / NULLIF(SUM({1}), 0)")

    if isinstance(agg, (exp.Min, exp.Max)):
        f = _num_field(agg.this, ctx)
        if f.agg_field is None:
            raise Untranslatable(f"{agg.key.upper()}({f.name}): field is not aggregatable")
        kind = "min" if isinstance(agg, exp.Min) else "max"
        fn = kind.upper()
        if f.is_numeric or f.is_date or f.sql_type == "BOOLEAN":
            n = f"{name}_{kind}"
            tz = zone_of(f, ctx.tz)
            if f.is_date:
                ex = lambda b, _n=n: _date_value(b[_n]["value"], tz)  # noqa: E731
                typ = "TIMESTAMP"
            elif f.sql_type == "BOOLEAN":
                ex = lambda b, _n=n: None if b[_n]["value"] is None else bool(b[_n]["value"])  # noqa: E731
                typ = "BOOLEAN"
            elif f.is_integer:
                ex = lambda b, _n=n: None if b[_n]["value"] is None else int(b[_n]["value"])  # noqa: E731
                typ = "BIGINT"
            else:
                ex = _val(n)
                typ = "DOUBLE"
            return AggSpec(typ, {n: {kind: {"field": f.agg_field}}}, ex, True, [(typ, ex)],
                           f"{fn}({{0}})", order_path=n)
        # keyword MIN/MAX: terms agg of size 1 ordered by key
        n = f"{name}_{kind}"
        body = {"terms": {"field": f.agg_field, "size": 1,
                          "order": {"_key": "asc" if kind == "min" else "desc"}}}

        def kex(b: dict, _n=n) -> Any:
            bk = b[_n]["buckets"]
            return bk[0]["key"] if bk else None

        return AggSpec("VARCHAR", {n: body}, kex, True, [("VARCHAR", kex)], f"{fn}({{0}})",
                       bucket_aggs=1)

    # percentiles / median
    if isinstance(agg, (exp.Median, exp.PercentileCont, exp.PercentileDisc, exp.ApproxQuantile,
                        exp.WithinGroup)):
        if isinstance(agg, exp.WithinGroup):
            inner = agg.this
            order = agg.expression
            if not isinstance(inner, (exp.PercentileCont, exp.PercentileDisc)) or not isinstance(order, exp.Order):
                raise Untranslatable(agg.sql())
            col = order.expressions[0].this
            q = literal_value(inner.this, ctx)
        elif isinstance(agg, exp.Median):
            col, q = agg.this, 0.5
        elif isinstance(agg, exp.ApproxQuantile):
            col = agg.this
            q = literal_value(agg.args.get("quantile"), ctx)
        else:
            col = agg.this
            qn = agg.expression
            if isinstance(qn, exp.Order):
                # DuckDB form of WITHIN GROUP: QUANTILE_CONT(x, 0.95 ORDER BY x)
                qn = qn.this
            q = literal_value(qn, ctx) if qn is not None else 0.5
        f = _num_field(col, ctx)
        if not f.is_numeric or not isinstance(q, (int, float)) or not 0 <= q <= 1:
            raise Untranslatable(agg.sql())
        n = f"{name}_pct"
        body = {"percentiles": {"field": f.agg_field, "percents": [q * 100], "keyed": False,
                                "tdigest": {"compression": ctx.percentile_compression}}}

        def pex(b: dict, _n=n) -> Any:
            vals = b[_n]["values"]
            return vals[0]["value"] if vals else None

        return AggSpec("DOUBLE", {n: body}, pex, heavy=1)

    if isinstance(agg, (exp.Stddev, exp.StddevSamp, exp.StddevPop, exp.Variance, exp.VariancePop)):
        f = _num_field(agg.this, ctx)
        _require_numeric(agg, f)
        n = f"{name}_xs"
        key = {
            exp.Stddev: "std_deviation_sampling", exp.StddevSamp: "std_deviation_sampling",
            exp.StddevPop: "std_deviation_population", exp.Variance: "variance_sampling",
            exp.VariancePop: "variance_population",
        }[type(agg)]

        def sex(b: dict, _n=n, _k=key) -> Any:
            x = b[_n]
            cnt = x.get("count", 0)
            if cnt == 0 or (cnt == 1 and "sampling" in _k):
                return None
            v = x.get(_k)
            return None if v is None or (isinstance(v, float) and math.isnan(v)) else v

        return AggSpec("DOUBLE", {n: {"extended_stats": {"field": f.agg_field}}}, sex)

    raise Untranslatable(f"aggregate not supported for pushdown: {agg.sql()}")


def is_aggregate(node: exp.Expression) -> bool:
    return isinstance(node, (exp.AggFunc, exp.Filter, exp.WithinGroup)) and not isinstance(node, exp.Window)


# --------------------------------------------------------------------------- #
# painless sort scripts (raw-document ORDER BY on arithmetic expressions)
# --------------------------------------------------------------------------- #
def sort_script(node: exp.Expression, ctx: Ctx, desc: bool, nulls_first: bool) -> dict:
    """ORDER BY <arithmetic over numeric columns> -> _script sort.

    SQL arithmetic propagates NULL: if any referenced value is missing the row
    sorts as NULL, placed by +/- infinity according to NULLS FIRST / LAST.
    Division follows IEEE rules like DuckDB (x/0 = inf, 0/0 = NaN).
    """
    fields: list[str] = []

    def go(n: exp.Expression) -> str:
        n = _strip_parens(n)
        f = ctx.field_of(n)
        if f is not None:
            if not f.is_numeric or f.agg_field is None:
                raise Untranslatable(f"cannot sort by an expression over {f.name}")
            if f.agg_field not in fields:
                fields.append(f.agg_field)
            return f"((double) v{fields.index(f.agg_field)})"
        if isinstance(n, exp.Literal) and not n.is_string:
            return f"({float(n.this)!r})"
        if isinstance(n, exp.Neg):
            return f"(-{go(n.this)})"
        if isinstance(n, exp.Cast) and n.to.is_type(*exp.DataType.NUMERIC_TYPES):
            return go(n.this)
        ops = {exp.Add: "+", exp.Sub: "-", exp.Mul: "*", exp.Div: "/"}
        for cls, op in ops.items():
            if type(n) is cls:
                return f"({go(n.this)} {op} {go(n.expression)})"
        raise Untranslatable(f"cannot sort by {n.sql()} in OpenSearch")

    body = go(node)
    if not fields:
        raise Untranslatable("constant sort key")
    nullv = "Double.POSITIVE_INFINITY" if desc == nulls_first else "Double.NEGATIVE_INFINITY"
    lines = [f"def v{i} = doc['{f}'].size() == 0 ? null : doc['{f}'].value;" for i, f in enumerate(fields)]
    lines.append("if (" + " || ".join(f"v{i} == null" for i in range(len(fields))) + f") return {nullv};")
    lines.append(f"return {body};")
    return {"_script": {"type": "number", "order": "desc" if desc else "asc",
                        "script": {"lang": "painless", "source": " ".join(lines)}}}


def _strip_parens(node: exp.Expression) -> exp.Expression:
    while isinstance(node, exp.Paren):
        node = node.this
    return node
