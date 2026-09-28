"""Index / alias metadata: OpenSearch mappings -> SQL columns.

Every mapped leaf field becomes a column (object sub-fields use their dotted
path). ``text`` fields expose their keyword multi-field (``.keyword``) as the
field used for aggregations, sorting and exact filters, so users never have to
know about sub-fields. ``nested`` fields are skipped (they cannot be grouped
without nested aggregations).
"""

from __future__ import annotations

import dataclasses
import fnmatch
import hashlib
import json
import logging
import os
import re
import tempfile
import threading
import time
from dataclasses import dataclass, field

from osagg.errors import ProgrammingError
from osagg.transport import Transport

logger = logging.getLogger(__name__)

INT_TYPES = {"long": "BIGINT", "integer": "INTEGER", "short": "SMALLINT", "byte": "TINYINT",
             "unsigned_long": "BIGINT"}
FLOAT_TYPES = {"double", "float", "half_float", "scaled_float"}
KEYWORDISH = {"keyword", "constant_keyword", "wildcard", "ip", "version"}
TEXTISH = {"text", "match_only_text"}
DATE_TYPES = {"date", "date_nanos"}
SKIP_TYPES = {"nested", "geo_point", "geo_shape", "binary", "knn_vector", "rank_feature",
              "rank_features", "completion", "join", "percolator", "flat_object", "xy_point",
              "xy_shape", "semantic", "sparse_vector", "search_as_you_type", "token_count",
              "histogram", "alias_unresolved", "integer_range", "long_range", "float_range",
              "double_range", "date_range", "ip_range", "star_tree", "derived"}


@dataclass(frozen=True)
class Field:
    name: str                 # SQL column name (dotted path for object sub-fields)
    os_type: str              # OpenSearch field type
    sql_type: str             # VARCHAR / BIGINT / DOUBLE / BOOLEAN / TIMESTAMP ...
    agg_field: str | None     # field for aggs / sort / exact term queries (None: not aggregatable)
    source_path: str | None = None  # path in _source (None for pseudo columns)
    is_text: bool = False     # analysed text field
    virtual: str | None = None  # "label:<date field>" / "shift:<date field>:<time field>"
    virtual_opts: dict | None = None  # computed columns: source, kind (keyword|date), format, time

    @property
    def is_date(self) -> bool:
        return self.sql_type == "TIMESTAMP"

    @property
    def is_numeric(self) -> bool:
        return self.sql_type in ("BIGINT", "INTEGER", "SMALLINT", "TINYINT", "DOUBLE")

    @property
    def is_integer(self) -> bool:
        return self.sql_type in ("BIGINT", "INTEGER", "SMALLINT", "TINYINT")


ID_FIELD = Field("_id", "_id", "VARCHAR", agg_field=None, source_path=None)


@dataclass
class TableMeta:
    name: str
    indices: list[str]
    fields: dict[str, Field] = field(default_factory=dict)

    def resolve(self, name: str) -> Field | None:
        f = self.fields.get(name)
        if f is not None:
            return f
        # explicit sub-field reference such as "ERROR_EXCEPTION.keyword"
        if "." in name:
            parent, _, sub = name.rpartition(".")
            pf = self.fields.get(parent)
            if pf is not None and pf.agg_field == name:
                return Field(name, "keyword", "VARCHAR", agg_field=name, source_path=pf.source_path)
        lowered = [f for n, f in self.fields.items() if n.lower() == name.lower()]
        if len(lowered) == 1:
            return lowered[0]
        return None

    def date_fields(self) -> list[Field]:
        return [f for f in self.fields.values() if f.is_date]


SORTABLE_FORMATS = ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d")


def resolve_source(spec: str | None, table: str, fields: dict[str, Field],
                   default_format: str = "%Y%m%d") -> tuple[Field, str] | None:
    """Pick the field named by `spec` for this table.

    spec: comma-separated items, tried in order; "pattern:field" applies only to tables
    matching the pattern (fnmatch), "field" to every table; "field=format" gives the
    strftime format of a keyword date (default `default_format`)."""
    if not spec:
        return None
    ruled: list[str] = []
    others: list[str] = []
    for item in (i.strip() for i in spec.split(",")):
        if not item:
            continue
        if ":" in item:
            pattern, item = (x.strip() for x in item.split(":", 1))
            if fnmatch.fnmatchcase(table, pattern):
                ruled.append(item)
        else:
            others.append(item)
    for item in ruled + others:
        name, _, fmt = item.partition("=")
        f = fields.get(name.strip())
        if f is not None and not f.virtual:
            return f, (fmt.strip() or default_format)
    return None


def with_label_column(meta: TableMeta, source: str | None, column: str,
                      time_source: str | None = None, time_column: str | None = None,
                      date_format: str = "%Y%m%d") -> TableMeta:
    """Add the business-date label column computed from the position date field chosen by
    `source` (see resolve_source; a keyword date in `date_format` or a date field), and
    the aligned execution time `time_column` (`time_source` moved onto the D-1 position
    date). Indices without such a field, or with a stored column of the same name, are
    unchanged."""
    picked = resolve_source(source, meta.name, meta.fields, date_format)
    if picked is None:
        return meta
    src, fmt = picked
    if src.is_date:
        kind = "date"
    elif src.sql_type == "VARCHAR" and src.agg_field is not None:
        kind = "keyword"
    else:
        return meta
    opts = {"source": src.name, "kind": kind, "format": fmt}
    fields = dict(meta.fields)
    if column and meta.resolve(column) is None:
        fields[column] = Field(column, "virtual", "VARCHAR", agg_field=None, source_path=None,
                               virtual=f"label:{src.name}", virtual_opts=opts)
    ts_pick = resolve_source(time_source, meta.name, meta.fields)
    ts = ts_pick[0] if ts_pick else None
    if time_column and ts is not None and ts.is_date and meta.resolve(time_column) is None:
        fields[time_column] = Field(time_column, "virtual", "TIMESTAMP", agg_field=None,
                                    source_path=None, virtual=f"shift:{src.name}:{ts.name}",
                                    virtual_opts={**opts, "time": ts.name})
    if len(fields) == len(meta.fields):
        return meta
    return TableMeta(name=meta.name, indices=meta.indices, fields=fields)


def _walk(props: dict, prefix: str, out: dict[str, Field], aliases: list[tuple[str, str]]) -> None:
    for name, spec in props.items():
        path = f"{prefix}{name}"
        ftype = spec.get("type")
        if ftype is None and "properties" in spec:  # object
            _walk(spec["properties"], f"{path}.", out, aliases)
            continue
        if ftype == "object":
            _walk(spec.get("properties", {}), f"{path}.", out, aliases)
            continue
        if ftype == "alias":
            aliases.append((path, spec.get("path", "")))
            continue
        if ftype in SKIP_TYPES or ftype is None:
            continue
        if ftype in KEYWORDISH:
            out[path] = Field(path, ftype, "VARCHAR", agg_field=path, source_path=path)
        elif ftype in TEXTISH:
            sub = None
            for sname, sspec in (spec.get("fields") or {}).items():
                if sspec.get("type") in ("keyword", "wildcard", "constant_keyword"):
                    sub = f"{path}.{sname}"
                    break
            fielddata = bool(spec.get("fielddata"))
            out[path] = Field(path, ftype, "VARCHAR", agg_field=sub or (path if fielddata else None),
                              source_path=path, is_text=True)
        elif ftype in INT_TYPES:
            out[path] = Field(path, ftype, INT_TYPES[ftype], agg_field=path, source_path=path)
        elif ftype in FLOAT_TYPES:
            out[path] = Field(path, ftype, "DOUBLE", agg_field=path, source_path=path)
        elif ftype == "boolean":
            out[path] = Field(path, ftype, "BOOLEAN", agg_field=path, source_path=path)
        elif ftype in DATE_TYPES:
            out[path] = Field(path, ftype, "TIMESTAMP", agg_field=path, source_path=path)
        else:
            logger.debug("skipping field %s of unsupported type %s", path, ftype)


def fields_from_mapping(mapping_response: dict) -> dict[str, Field]:
    """Merge the mappings of all indices in a GET _mapping response."""
    merged: dict[str, Field] = {}
    for index_name in sorted(mapping_response):
        mappings = mapping_response[index_name].get("mappings", {})
        props = mappings.get("properties", {})
        out: dict[str, Field] = {}
        aliases: list[tuple[str, str]] = []
        _walk(props, "", out, aliases)
        for alias_name, target in aliases:
            t = out.get(target)
            if t is not None:
                out[alias_name] = Field(alias_name, t.os_type, t.sql_type,
                                        agg_field=alias_name if t.agg_field == target else t.agg_field,
                                        source_path=t.source_path, is_text=t.is_text)
        for name, f in out.items():
            prev = merged.get(name)
            if prev is None:
                merged[name] = f
            elif prev.sql_type != f.sql_type or prev.agg_field != f.agg_field:
                # conflicting definitions across indices: keep the most permissive view
                logger.warning("field %s has conflicting mappings (%s vs %s)", name, prev, f)
                if prev.sql_type != f.sql_type:
                    merged[name] = Field(name, "keyword", "VARCHAR", agg_field=None,
                                         source_path=f.source_path)
    return dict(sorted(merged.items()))


class MetadataCache:
    """Per-process cache of table metadata (mappings change rarely)."""

    def __init__(self, ttl: float = 300.0) -> None:
        self.ttl = ttl
        self._lock = threading.Lock()
        self._tables: dict[tuple[str, str], tuple[float, TableMeta]] = {}
        self._lists: dict[str, tuple[float, list[tuple[str, str]]]] = {}

    def list_tables(self, key: str, transport: Transport,
                    patterns: list[str] | None = None) -> list[tuple[str, str]]:
        now = time.monotonic()
        ckey = f"{key}|{','.join(patterns or [])}"
        with self._lock:
            hit = self._lists.get(ckey)
            if hit and hit[0] > now:
                return hit[1]
        tables = transport.list_tables(patterns) if patterns else transport.list_tables()
        with self._lock:
            self._lists[ckey] = (now + self.ttl, tables)
        return tables

    def table(self, key: str, transport: Transport, name: str, include_id: bool = True) -> TableMeta:
        now = time.monotonic()
        with self._lock:
            hit = self._tables.get((key, name))
            if hit and hit[0] > now:
                return hit[1]
        slow = getattr(transport, "kind", "") == "trino"
        cached = _disk_load(key, name) if slow else None
        if cached is not None:
            indices, fields = cached
        else:
            resp = transport.get_mapping(name)
            if not resp:
                raise ProgrammingError(f'Table "{name}" does not exist (no OpenSearch index, alias '
                                       f"or pattern matches it)")
            indices = sorted(resp)
            fields = fields_from_mapping(resp)
            if slow:
                # discovery through Trino samples documents and probes fields (seconds):
                # share the result between Superset processes through a small disk cache
                fields = probe_text_fields(transport, name, fields)
                _disk_save(key, name, indices, fields)
        if include_id:
            fields["_id"] = ID_FIELD
        meta = TableMeta(name=name, indices=indices, fields=fields)
        with self._lock:
            self._tables[(key, name)] = (now + self.ttl, meta)
        return meta

    def invalidate(self) -> None:
        with self._lock:
            self._tables.clear()
            self._lists.clear()


CACHE = MetadataCache()

DISK_TTL = float(os.environ.get("OSAGG_METADATA_DISK_TTL", 86400))


def _disk_path(key: str, name: str) -> str:
    base = os.environ.get("OSAGG_CACHE_DIR") or os.path.join(tempfile.gettempdir(), "osagg-metadata")
    digest = hashlib.sha256(f"{key}|{name}".encode()).hexdigest()[:32]
    return os.path.join(base, f"{digest}.json")


def _disk_load(key: str, name: str) -> tuple[list[str], dict[str, Field]] | None:
    path = _disk_path(key, name)
    try:
        if time.time() - os.path.getmtime(path) > DISK_TTL:
            return None
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return list(data["indices"]), {f["name"]: Field(**f) for f in data["fields"]}
    except (OSError, ValueError, KeyError, TypeError):
        return None


def _disk_save(key: str, name: str, indices: list[str], fields: dict[str, Field]) -> None:
    path = _disk_path(key, name)
    try:
        os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
        tmp = f"{path}.{os.getpid()}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"table": name, "indices": indices,
                       "fields": [dataclasses.asdict(f) for f in fields.values()]}, fh)
        os.replace(tmp, path)
    except OSError:
        logger.debug("cannot write metadata cache %s", path, exc_info=True)


def visible_tables(tables: list[tuple[str, str]], extra_patterns: list[str] | None = None,
                   include_hidden: bool = False) -> list[str]:
    names = []
    for name, _kind in tables:
        if not include_hidden and name.startswith("."):
            continue
        names.append(name)
    for pat in extra_patterns or []:
        if pat not in names:
            names.append(pat)
    return sorted(names)


def matches_any(name: str, tables: list[tuple[str, str]]) -> bool:
    if any(ch in name for ch in "*?"):
        return any(fnmatch.fnmatchcase(t, name) for t, _ in tables)
    return any(t == name for t, _ in tables)


_TEXT_ERR = re.compile(r"fielddata=true on \[([^\]]+)\]|Fielddata is disabled on \[?([^\] ]+)\]?")


def probe_text_fields(transport: Transport, index: str, fields: dict[str, Field]) -> dict[str, Field]:
    """Through Trino, keyword and text both look like VARCHAR. Find text fields by
    asking OpenSearch for a tiny terms aggregation on every VARCHAR field: a text
    field fails with a fielddata error naming it; then look for a .keyword sub-field."""
    candidates = [f.name for f in fields.values() if f.sql_type == "VARCHAR"]
    text: set[str] = set()
    for _ in range(len(candidates) + 1):
        todo = [c for c in candidates if c not in text]
        if not todo:
            break
        # cheap at any index size: one document per shard (terminate_after) and map
        # execution (no global ordinals); text fields fail when the aggregator is built
        body = {"size": 0, "track_total_hits": False, "terminate_after": 1,
                "aggs": {f"p{i}": {"terms": {"field": c, "size": 1, "execution_hint": "map"}}
                         for i, c in enumerate(todo)}}
        try:
            transport.search(index, body)
            break
        except Exception as ex:  # pylint: disable=broad-except
            m = _TEXT_ERR.search(str(ex))
            name = (m.group(1) or m.group(2)) if m else None
            if not name or name not in fields or name in text:
                logger.warning("text-field probe failed on %s: %s", index, ex)
                break
            text.add(name)
    out = dict(fields)
    for name in text:
        f = fields[name]
        sub = f"{name}.keyword"
        agg = None
        try:
            res = transport.search(index, {"size": 0, "track_total_hits": 1,
                                           "query": {"exists": {"field": sub}}})
            if res["hits"]["total"]["value"] > 0:
                agg = sub
        except Exception:  # pylint: disable=broad-except
            agg = None
        out[name] = Field(name, "text", "VARCHAR", agg_field=agg, source_path=f.source_path,
                          is_text=True)
    return out
