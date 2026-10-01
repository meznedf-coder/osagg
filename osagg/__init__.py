"""osagg - OpenSearch aggregation-pushdown SQL engine for Apache Superset.

Superset (or any DB-API / SQLAlchemy client) sends plain SQL. osagg parses it,
pushes filters, GROUP BY and aggregate functions down to OpenSearch as
composite aggregations (paged), and only evaluates what is left (ordering,
limits, post-aggregation expressions, HAVING, ...) in an embedded, locked-down
DuckDB on the small aggregated result.
"""

from osagg.dbapi import (  # noqa: F401
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
    apilevel,
    connect,
    paramstyle,
    threadsafety,
)

from osagg.udf import register_function, registered, unregister_function  # noqa: E402,F401

__version__ = "0.2.8"
