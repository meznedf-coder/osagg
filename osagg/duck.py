"""Embedded DuckDB used for the residual part of a query.

Each query gets a fresh in-memory database. After the scan results are
registered, external access (files, network, extensions, ATTACH, COPY) is
disabled and the configuration is locked, so SQL typed in SQL Lab cannot
reach the Superset host.
"""

from __future__ import annotations

import duckdb

from osagg import udf


def new_session(tz: str, threads: int = 2, memory_limit: str = "1GB") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(
        ":memory:",
        config={
            "threads": threads,
            "memory_limit": memory_limit,
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
        },
    )
    con.execute(f"SET TimeZone = '{tz}'")
    udf.apply(con)
    return con


def lock(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")


def locked_session(tz: str) -> duckdb.DuckDBPyConnection:
    con = new_session(tz, threads=1, memory_limit="256MB")
    lock(con)
    return con
