"""SQLAlchemy dialect ``osagg://`` (SQLAlchemy 1.4 and 2.0).

SQL is compiled PostgreSQL/DuckDB style (double-quoted identifiers), which is
what osagg parses. Reflection reads OpenSearch mappings.

URL:  osagg://[user[:password]@]host[:port]/[default]?timezone=Europe/Paris&...
      osagg+https://...            (TLS)
      osagg://trino-host:8080/?transport=trino&trino_catalog=opensearch&...
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgresql.base import (
    PGCompiler,
    PGDDLCompiler,
    PGIdentifierPreparer,
    PGTypeCompiler,
)
from sqlalchemy.engine import default

SQL_TYPES = {
    "VARCHAR": sqltypes.String,
    "BIGINT": sqltypes.BigInteger,
    "INTEGER": sqltypes.Integer,
    "SMALLINT": sqltypes.SmallInteger,
    "TINYINT": sqltypes.SmallInteger,
    "DOUBLE": sqltypes.Float,
    "BOOLEAN": sqltypes.Boolean,
    "TIMESTAMP": sqltypes.TIMESTAMP,
    "DATE": sqltypes.Date,
}


class OsaggIdentifierPreparer(PGIdentifierPreparer):
    """Quote everything that is not a plain lower-case identifier (index names
    contain dashes, fields start with @ or contain dots)."""

    def _requires_quotes(self, value: str) -> bool:
        return True if value != value.lower() else super()._requires_quotes(value)


class OpenSearchAggDialect(default.DefaultDialect):
    name = "osagg"
    driver = "rest"
    supports_statement_cache = True
    default_paramstyle = "pyformat"

    statement_compiler = PGCompiler
    ddl_compiler = PGDDLCompiler
    type_compiler = PGTypeCompiler
    preparer = OsaggIdentifierPreparer

    supports_alter = False
    supports_sequences = False
    supports_native_boolean = True
    supports_native_decimal = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_multivalues_insert = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    postfetch_lastrowid = False
    implicit_returning = False
    _backslash_escapes = False
    supports_smallserial = False
    supports_identity_columns = False
    supports_native_enum = False

    # DB-API module ---------------------------------------------------------
    @classmethod
    def dbapi(cls):  # SQLAlchemy 1.4
        import osagg.dbapi as module

        return module

    @classmethod
    def import_dbapi(cls):  # SQLAlchemy 2.0
        import osagg.dbapi as module

        return module

    def create_connect_args(self, url) -> tuple[list[Any], dict[str, Any]]:
        kwargs: dict[str, Any] = dict(url.query)
        kwargs["host"] = url.host or "localhost"
        if url.port:
            kwargs["port"] = url.port
        if url.username:
            kwargs["user"] = url.username
        if url.password:
            kwargs["password"] = url.password
        if url.drivername.endswith("+https"):
            kwargs.setdefault("scheme", "https")
        return [], kwargs

    # no transactions / server introspection -------------------------------
    def initialize(self, connection) -> None:
        self.server_version_info = (1, 0)
        self.default_schema_name = "default"
        self.default_isolation_level = "AUTOCOMMIT"

    def _get_server_version_info(self, connection):
        return (1, 0)

    def _get_default_schema_name(self, connection):
        return "default"

    def get_isolation_level(self, dbapi_connection):
        return "AUTOCOMMIT"

    def set_isolation_level(self, dbapi_connection, level):
        pass

    def get_default_isolation_level(self, dbapi_conn):
        return "AUTOCOMMIT"

    def do_rollback(self, dbapi_connection) -> None:
        pass

    def do_commit(self, dbapi_connection) -> None:
        pass

    def do_ping(self, dbapi_connection) -> bool:
        # a real round trip to OpenSearch (SELECT 1 would only exercise DuckDB)
        return self._raw(dbapi_connection).ping()

    # reflection ------------------------------------------------------------
    @staticmethod
    def _raw(connection):
        """Dig the osagg DB-API connection out of SQLAlchemy wrappers."""
        from osagg.dbapi import Connection

        obj = connection
        for _ in range(6):
            if isinstance(obj, Connection):
                return obj
            nxt = getattr(obj, "dbapi_connection", None)
            if nxt is None or nxt is obj:
                nxt = getattr(obj, "connection", None)
            if nxt is None or nxt is obj:
                break
            obj = nxt
        raise RuntimeError("not an osagg connection")

    def get_schema_names(self, connection, **kw) -> list[str]:
        return ["default"]

    def has_schema(self, connection, schema_name, **kw) -> bool:
        return schema_name in (None, "default")

    def get_table_names(self, connection, schema=None, **kw) -> list[str]:
        return self._raw(connection).list_tables()

    def has_table(self, connection, table_name, schema=None, **kw) -> bool:
        return self._raw(connection).table_meta(table_name) is not None

    def get_view_names(self, connection, schema=None, **kw) -> list[str]:
        return []

    def get_materialized_view_names(self, connection, schema=None, **kw) -> list[str]:
        return []

    def get_temp_table_names(self, connection, schema=None, **kw) -> list[str]:
        return []

    def get_temp_view_names(self, connection, schema=None, **kw) -> list[str]:
        return []

    def get_sequence_names(self, connection, schema=None, **kw) -> list[str]:
        return []

    def get_columns(self, connection, table_name, schema=None, **kw) -> list[dict[str, Any]]:
        meta = self._raw(connection).table_meta(table_name)
        if meta is None:
            from sqlalchemy.exc import NoSuchTableError

            raise NoSuchTableError(table_name)
        cols = []
        for f in meta.fields.values():
            typ = SQL_TYPES.get(f.sql_type, sqltypes.String)
            comment = f"OpenSearch {f.os_type}"
            if f.virtual and f.virtual.startswith("shift:"):
                _k, src, ts = f.virtual.split(":", 2)
                comment = (f"{ts} moved onto the D-1 position date (+7 days for W-1, +14 for "
                           f"W-2...): X-axis to overlay position dates, with {src} labels as "
                           "dimension")
            elif f.virtual:
                src = f.virtual.split(":", 1)[1]
                comment = (f"Business-date label of {src} computed by osagg (D, D-x, W-x, Y-x); "
                           f"filters on it are pushed down as {src} filters")
            elif f.agg_field and f.agg_field != f.name:
                comment += f" (aggregations use {f.agg_field})"
            elif f.agg_field is None and f.name != "_id":
                comment += " (not aggregatable)"
            cols.append({"name": f.name, "type": typ(), "nullable": True, "default": None,
                         "autoincrement": False, "comment": comment})
        return cols

    def get_pk_constraint(self, connection, table_name, schema=None, **kw) -> dict[str, Any]:
        return {"constrained_columns": [], "name": None}

    def get_primary_keys(self, connection, table_name, schema=None, **kw) -> list[str]:
        return []

    def get_foreign_keys(self, connection, table_name, schema=None, **kw) -> list[dict]:
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw) -> list[dict]:
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw) -> list[dict]:
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kw) -> list[dict]:
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kw) -> dict[str, Any]:
        return {"text": None}

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        return None


class OpenSearchAggHttpsDialect(OpenSearchAggDialect):
    driver = "https"
