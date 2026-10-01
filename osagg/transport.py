"""How osagg talks to OpenSearch.

Two transports share one interface so the SQL -> DSL translation never cares
how requests are sent:

* ``DirectTransport``: Superset workers call the OpenSearch REST API directly
  (opensearch-py). Supports composite paging, point-in-time + search_after.
* ``TrinoTransport``: every search request is sent through Trino's OpenSearch
  connector ``raw_query`` table function (for sites where Trino is the only
  gateway to OpenSearch). Composite paging still works (one Trino query per
  page). Raw-document scans are delegated to Trino's regular table scan.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from osagg.errors import OperationalError, ProgrammingError

logger = logging.getLogger(__name__)


# OpenSearch answers 429 (circuit breaker, full search queue) when it is short of heap or
# threads for a moment: it means "retry later". Several dashboard charts at once can do it.
BUSY_RETRY_DELAYS = (0.5, 1.5, 4.0)


def _busy(ex: Exception) -> bool:
    text = f"{getattr(ex, 'error', '')} {getattr(ex, 'info', '')}"
    return getattr(ex, "status_code", None) == 429 or any(
        s in text for s in ("circuit_breaking_exception", "Data too large", "rejected_execution_exception"))


class PartialResults(OperationalError):
    """OpenSearch answered from some of the shards only."""

    def __init__(self, message: str, busy: bool) -> None:
        super().__init__(message)
        self.busy = busy


def check_complete(res: dict, index: str) -> dict:
    """OpenSearch answers 200 with what the shards it could read hold when others fail (a node gone, a shard
    search rejected or out of memory, a search timeout): counts and sums from part of the data, and nothing
    in the answer says so but _shards / timed_out. Such an answer is an error here, never a result."""
    shards = res.get("_shards") or {}
    failed = int(shards.get("failed") or 0)
    if not failed and not res.get("timed_out"):
        return res
    reasons = []
    for fail in shards.get("failures") or []:
        r = fail.get("reason") or {}
        why = f"{r.get('type')}: {r.get('reason')}" if isinstance(r, dict) else str(r)
        if why not in reasons:
            reasons.append(why)
    busy = bool(reasons) and all(any(s in w for s in ("rejected_execution", "circuit_breaking", "Data too large"))
                                 for w in reasons)
    if failed:
        what = f"{failed} of the {shards.get('total', '?')} shards of {index} failed"
    else:
        what = f"the search of {index} timed out"
    raise PartialResults(f"OpenSearch answered from part of the data only ({what}"
                         + (f": {'; '.join(reasons[:3])[:600]}" if reasons else "")
                         + "). No result is computed from part of the data: run the query again, and if this "
                         "persists check the cluster's health (GET _cluster/health).", busy)


class Transport:
    """Interface."""

    kind = "base"

    def search(self, index: str, body: dict, timeout: float | None = None) -> dict:
        raise NotImplementedError

    def count(self, index: str, query: dict | None) -> int:
        body = {"size": 0, "track_total_hits": True}
        if query:
            body["query"] = query
        res = self.search(index, body)
        return int(res["hits"]["total"]["value"])

    def msearch(self, indices: list[str], bodies: list[dict]) -> list[dict]:
        """Several searches (one per index and body), each answer complete or an error."""
        return [self.search(i, b) for i, b in zip(indices, bodies)]

    def open_pit(self, index: str, keep_alive: str = "2m") -> str | None:
        return None

    def close_pit(self, pit_id: str) -> None:
        return None

    def get_mapping(self, index: str) -> dict:
        raise NotImplementedError

    def list_tables(self, patterns: list[str] | None = None) -> list[tuple[str, str]]:
        """Return [(name, kind)] with kind in index|alias|data_stream."""
        raise NotImplementedError

    def max_buckets(self) -> int:
        return 65535

    def close(self) -> None:
        return None


class DirectTransport(Transport):
    kind = "direct"

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        user: str | None = None,
        password: str | None = None,
        use_ssl: bool = False,
        verify_certs: bool = True,
        ca_certs: str | None = None,
        client_cert: str | None = None,
        client_key: str | None = None,
        request_timeout: float = 120.0,
        http_compress: bool = True,
        url_prefix: str = "",
    ) -> None:
        from opensearchpy import OpenSearch  # local import: optional dependency

        kwargs: dict[str, Any] = {
            "hosts": [{"host": host, "port": int(port), "url_prefix": url_prefix}],
            "use_ssl": use_ssl,
            "verify_certs": verify_certs,
            "ssl_show_warn": False,
            "timeout": request_timeout,
            "http_compress": http_compress,
            "max_retries": 2,
            "retry_on_timeout": False,
        }
        if user:
            kwargs["http_auth"] = (user, password or "")
        if ca_certs:
            kwargs["ca_certs"] = ca_certs
        if client_cert:
            kwargs["client_cert"] = client_cert
        if client_key:
            kwargs["client_key"] = client_key
        self.request_timeout = request_timeout
        self.client = OpenSearch(**kwargs)
        self._max_buckets: int | None = None

    def _call(self, fn, *args, **kwargs):
        from opensearchpy.exceptions import (
            ConnectionError as OSConnectionError,
            RequestError,
            TransportError,
        )

        for attempt in range(len(BUSY_RETRY_DELAYS) + 1):
            try:
                return fn(*args, **kwargs)
            except RequestError as ex:  # 400: bad query
                if _busy(ex) and attempt < len(BUSY_RETRY_DELAYS):
                    self._wait(ex, attempt)
                    continue
                raise ProgrammingError(f"OpenSearch rejected the request: {_os_error(ex)}") from ex
            except OSConnectionError as ex:
                raise OperationalError(f"Cannot reach OpenSearch: {ex}") from ex
            except TransportError as ex:
                if _busy(ex) and attempt < len(BUSY_RETRY_DELAYS):
                    self._wait(ex, attempt)
                    continue
                raise OperationalError(f"OpenSearch error: {_os_error(ex)}") from ex
        raise AssertionError("unreachable")

    @staticmethod
    def _wait(ex: Exception, attempt: int) -> None:
        delay = BUSY_RETRY_DELAYS[attempt]
        logger.warning("osagg: OpenSearch is busy (%s), retrying in %.1f s", _os_error(ex)[:160], delay)
        time.sleep(delay)

    def search(self, index: str, body: dict, timeout: float | None = None) -> dict:
        t0 = time.perf_counter()
        # a failing shard fails the request rather than leaving its documents out (checked below as well)
        params = {"request_timeout": timeout or self.request_timeout, "allow_partial_search_results": "false"}
        for attempt in range(len(BUSY_RETRY_DELAYS) + 1):
            if "pit" in body:  # PIT searches must not name the index
                res = self._call(self.client.search, body=body, params=params)
            else:
                res = self._call(self.client.search, index=index, body=body, params=params)
            try:
                check_complete(res, index)
                break
            except PartialResults as ex:
                if not ex.busy or attempt == len(BUSY_RETRY_DELAYS):
                    raise
                self._wait(ex, attempt)
        logger.debug("search %s took=%sms wall=%.0fms", index, res.get("took"),
                     (time.perf_counter() - t0) * 1000)
        return res

    def msearch(self, indices: list[str], bodies: list[dict]) -> list[dict]:
        """Several searches in one request (the metadata probes): each answer checked as a search's."""
        if not bodies:
            return []
        lines: list[dict] = []
        for index, body in zip(indices, bodies):
            lines += [{"index": index}, body]
        # (_msearch takes no allow_partial_search_results: each answer's _shards is checked below)
        res = self._call(self.client.msearch, body=lines, params={"request_timeout": self.request_timeout})
        out = []
        for index, r in zip(indices, res.get("responses") or []):
            if "error" in r:
                raise OperationalError(f"OpenSearch error on {index}: {r['error']}")
            out.append(check_complete(r, index))
        if len(out) != len(bodies):
            raise OperationalError("OpenSearch answered fewer searches than were sent")
        return out

    def open_pit(self, index: str, keep_alive: str = "2m") -> str | None:
        # created on every shard or not at all: a point in time missing shards would page through part of
        # the documents with every later page answering "0 failed"
        res = self._call(self.client.create_pit, index=index,
                         params={"keep_alive": keep_alive, "allow_partial_pit_creation": "false"})
        try:
            check_complete(res, index)
        except PartialResults:
            if res.get("pit_id"):
                self.close_pit(res["pit_id"])
            raise
        return res.get("pit_id")

    def close_pit(self, pit_id: str) -> None:
        try:
            self.client.delete_pit(body={"pit_id": [pit_id]})
        except Exception:  # pylint: disable=broad-except
            logger.debug("could not delete PIT", exc_info=True)

    def get_mapping(self, index: str) -> dict:
        return self._call(self.client.indices.get_mapping, index=index,
                          params={"expand_wildcards": "open", "ignore_unavailable": "true"})

    def list_tables(self, patterns: list[str] | None = None) -> list[tuple[str, str]]:
        """Indices, aliases and data streams, optionally restricted to `patterns`.

        Uses GET _resolve/index/<patterns>, which the plain `read` privilege allows,
        so a least-privilege service account needs no cluster permission.
        """
        from opensearchpy.exceptions import AuthorizationException, NotFoundError

        target = ",".join(patterns) if patterns else "*"
        try:
            res = self.client.indices.resolve_index(name=target, params={"expand_wildcards": "open"})
        except AuthorizationException as ex:
            if patterns:
                logger.warning("osagg: no permission to resolve %s; exposing the patterns only", target)
                return [(p, "pattern") for p in patterns]
            raise OperationalError(
                "The OpenSearch account is not allowed to list all indices. Add "
                "tables=<index or pattern>[,<more>] to the connection URI (the indices it may "
                "read).") from ex
        except NotFoundError:
            return [(p, "pattern") for p in patterns or []]
        except Exception as ex:  # pylint: disable=broad-except
            raise OperationalError(f"Cannot list OpenSearch indices: {_os_error(ex)}") from ex
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        for kind, key in (("index", "indices"), ("alias", "aliases"), ("data_stream", "data_streams")):
            for item in res.get(key, []):
                name = item.get("name")
                if not name or name in seen or "hidden" in (item.get("attributes") or []):
                    continue
                seen.add(name)
                out.append((name, kind))
        return out

    def max_buckets(self) -> int:
        if self._max_buckets is None:
            try:
                res = self.client.cluster.get_settings(
                    params={"include_defaults": "true", "flat_settings": "true"})
                val = None
                for section in ("transient", "persistent", "defaults"):
                    val = res.get(section, {}).get("search.max_buckets") or val
                    if res.get(section, {}).get("search.max_buckets"):
                        break
                self._max_buckets = int(val or 65535)
            except Exception:  # pylint: disable=broad-except
                self._max_buckets = 65535
        return self._max_buckets

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:  # pylint: disable=broad-except
            pass


class TrinoTransport(Transport):
    """Send OpenSearch DSL through Trino's ``raw_query`` table function."""

    kind = "trino"
    max_body_chars = 800_000      # the DSL travels inside the SQL text (query.max-length 1M)

    def max_buckets(self) -> int:
        return self._max_buckets

    def __init__(
        self,
        host: str,
        port: int = 8080,
        user: str = "osagg",
        password: str | None = None,
        catalog: str = "opensearch",
        schema: str = "default",
        http_scheme: str = "http",
        verify: bool | str = True,
        request_timeout: float = 120.0,
        max_buckets: int = 65535,
    ) -> None:
        import trino  # local import: optional dependency

        self._max_buckets = max_buckets

        auth = None
        if password:
            auth = trino.auth.BasicAuthentication(user, password)
        self.catalog = catalog
        self.schema = schema
        self.request_timeout = request_timeout
        self.conn = trino.dbapi.connect(
            host=host, port=int(port), user=user, auth=auth, catalog=catalog,
            schema=schema, http_scheme=http_scheme, verify=verify,
            request_timeout=request_timeout, source="osagg",
        )

    def _query(self, sql: str) -> list[tuple]:
        import trino

        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        except trino.exceptions.TrinoUserError as ex:
            raise ProgrammingError(f"Trino rejected the request: {ex.message}") from ex
        except trino.exceptions.Error as ex:  # pragma: no cover - network errors
            raise OperationalError(f"Trino error: {ex}") from ex
        finally:
            cur.close()

    @staticmethod
    def _lit(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def search(self, index: str, body: dict, timeout: float | None = None) -> dict:
        if "pit" in body:
            raise ProgrammingError("point-in-time is not available through Trino raw_query")
        sql = (
            f"SELECT result FROM TABLE({self.catalog}.system.raw_query("
            f"schema => {self._lit(self.schema)}, index => {self._lit(index)}, "
            f"query => {self._lit(json.dumps(body, separators=(',', ':')))}))"
        )
        t0 = time.perf_counter()
        rows = self._query(sql)
        res = json.loads(rows[0][0])
        logger.debug("trino raw_query %s took=%sms wall=%.0fms", index, res.get("took"),
                     (time.perf_counter() - t0) * 1000)
        if "error" in res:
            raise ProgrammingError(f"OpenSearch rejected the request: {res['error']}")
        return check_complete(res, index)

    def trino_query(self, sql: str) -> tuple[list[tuple], list[str]]:
        """Run a plain Trino query (used for delegated raw-document scans)."""
        import trino

        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            names = [d[0] for d in cur.description]
            return rows, names
        except trino.exceptions.TrinoUserError as ex:
            raise ProgrammingError(f"Trino rejected the request: {ex.message}") from ex
        finally:
            cur.close()

    def list_tables(self, patterns: list[str] | None = None) -> list[tuple[str, str]]:
        rows = self._query(f"SHOW TABLES FROM {self.catalog}.{self._ident(self.schema)}")
        return [(r[0], "index") for r in rows]

    @staticmethod
    def _ident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def get_mapping(self, index: str) -> dict:
        """Build a pseudo mapping from Trino's column metadata.

        Trino lower-cases column names, but OpenSearch field names are case
        sensitive, so the original names are recovered by sampling documents
        through raw_query (and probing with `exists` for fields not seen).
        Keyword and text both look like VARCHAR: the metadata layer probes
        aggregatability afterwards (see metadata.probe_text_fields).
        """
        rows = self._query(
            f"SELECT column_name, data_type FROM {self.catalog}.information_schema.columns "
            f"WHERE table_schema = {self._lit(self.schema)} AND table_name = {self._lit(index.lower())} "
            f"ORDER BY ordinal_position"
        )
        if not rows:
            return {}
        names = self._original_names(index, [r[0] for r in rows])
        props: dict[str, Any] = {}
        for name, dtype in rows:
            if name in ("_id", "_score", "_source"):
                continue
            if dtype.lower().startswith("row"):
                continue  # objects: not supported through Trino (use the direct transport)
            real = names.get(name)
            if real is None:
                logger.warning("osagg/trino: cannot find the OpenSearch name of column %s.%s", index, name)
                continue
            props[real] = {"type": _trino_to_os_type(dtype), "_trino_type": dtype}
        return {index: {"mappings": {"properties": props}}}

    def _original_names(self, index: str, lower_names: list[str]) -> dict[str, str]:
        found: dict[str, str] = {}

        def walk(doc: Any, prefix: str = "") -> None:
            if isinstance(doc, dict):
                for k, v in doc.items():
                    if isinstance(v, dict):
                        walk(v, f"{prefix}{k}.")
                    else:
                        found.setdefault(f"{prefix}{k}".lower(), f"{prefix}{k}")

        for sort in ("asc", "desc"):
            try:
                res = self.search(index, {"size": 500, "track_total_hits": False,
                                          "sort": [{"_doc": sort}]})
                for h in res["hits"]["hits"]:
                    walk(h.get("_source") or {})
            except Exception:  # pylint: disable=broad-except
                logger.debug("sampling failed", exc_info=True)
        out: dict[str, str] = {}
        for low in lower_names:
            if low in found:
                out[low] = found[low]
                continue
            for cand in (low.upper(), low):
                try:
                    res = self.search(index, {"size": 0, "track_total_hits": 1,
                                              "query": {"exists": {"field": cand}}})
                    if res["hits"]["total"]["value"] > 0:
                        out[low] = cand
                        break
                except Exception:  # pylint: disable=broad-except
                    continue
        return out


def _trino_to_os_type(dtype: str) -> str:
    d = dtype.lower()
    if d.startswith("varchar"):
        return "keyword"  # verified lazily (text vs keyword)
    if d in ("bigint",):
        return "long"
    if d in ("integer",):
        return "integer"
    if d in ("smallint",):
        return "short"
    if d in ("tinyint",):
        return "byte"
    if d in ("double",):
        return "double"
    if d in ("real",):
        return "float"
    if d == "boolean":
        return "boolean"
    if d.startswith("timestamp"):
        return "date"
    if d.startswith("row"):
        return "object"
    return "keyword"


def _os_error(ex: Exception) -> str:
    info = getattr(ex, "info", None)
    if isinstance(info, dict):
        err = info.get("error", info)
        if isinstance(err, dict):
            root = err.get("root_cause") or []
            reason = err.get("reason", "")
            if root and isinstance(root, list):
                r0 = root[0]
                return f"{r0.get('type')}: {r0.get('reason')}" + (f" ({reason})" if reason and reason != r0.get("reason") else "")
            caused = err.get("caused_by")
            if caused:
                return f"{err.get('type')}: {reason} / caused by {caused.get('type')}: {caused.get('reason')}"
            return f"{err.get('type')}: {reason}"
        return str(err)
    return str(ex)
