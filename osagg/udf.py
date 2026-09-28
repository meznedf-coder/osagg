"""User-defined SQL functions backed by Python (business calendars, label mappings...).

Register them once, e.g. in ``superset_config.py``::

    import osagg
    osagg.register_function("position_label", convert_date_or_label, ["VARCHAR"], "VARCHAR")

They become callable in every osagg query (SQL Lab, calculated columns, metrics,
filters). They run inside DuckDB on the small, already aggregated rows, and in the
planner when an expression is a constant (``position_days('W-1')``). A filter that
depends on a single keyword column, e.g. ``position_label("POSITION_DATE") IN ('D-1')``,
is turned into a ``terms`` filter on that column and pushed down to OpenSearch.
"""

from __future__ import annotations

import threading
from typing import Any, Callable

import duckdb

_LOCK = threading.Lock()
_REGISTRY: dict[str, tuple[Callable[..., Any], list[str], str, bool]] = {}


def register_function(name: str, func: Callable[..., Any], parameters: list[str],
                      return_type: str, *, errors_as_null: bool = True) -> None:
    """Make `func` callable as `name(...)` in osagg SQL.

    parameters / return_type are DuckDB type names ("VARCHAR", "BIGINT", "DOUBLE",
    "DATE", "TIMESTAMP", "BOOLEAN"). NULL arguments give NULL without calling the
    function. With errors_as_null, an exception raised by the function gives NULL
    instead of failing the whole query.
    """
    if not name.isidentifier():
        raise ValueError(f"invalid function name: {name!r}")
    with _LOCK:
        _REGISTRY[name.lower()] = (func, list(parameters), return_type, errors_as_null)


def unregister_function(name: str) -> None:
    with _LOCK:
        _REGISTRY.pop(name.lower(), None)


def registered() -> list[str]:
    with _LOCK:
        return sorted(_REGISTRY)


def apply(con: duckdb.DuckDBPyConnection) -> None:
    """Register the built-in and user functions on a DuckDB connection."""
    from osagg import calendar

    # osagg_label(<yyyymmdd>, '<today>/<session>/<Y|->') -> 'D-1' / 'W-2' / 'Y-1' / NULL
    con.create_function("osagg_label", calendar.label, [duckdb.sqltype("VARCHAR")] * 2,
                        duckdb.sqltype("VARCHAR"), null_handling="special",
                        exception_handling="return_null")
    # osagg_shift(<yyyymmdd>, key) -> days from that position date to the D-1 position date
    con.create_function("osagg_shift", calendar.shift_days, [duckdb.sqltype("VARCHAR")] * 2,
                        duckdb.sqltype("INTEGER"), null_handling="special",
                        exception_handling="return_null")
    with _LOCK:
        items = list(_REGISTRY.items())
    for name, (func, params, ret, errors_as_null) in items:
        con.create_function(
            name, _null_in_null_out(func), [duckdb.sqltype(p) for p in params], duckdb.sqltype(ret),
            null_handling="special",
            exception_handling="return_null" if errors_as_null else "throw",
        )


def _null_in_null_out(func: Callable[..., Any]) -> Callable[..., Any]:
    # "special" NULL handling lets the function return None; NULL arguments still give NULL
    def call(*args: Any) -> Any:
        if any(a is None for a in args):
            return None
        return func(*args)

    return call
