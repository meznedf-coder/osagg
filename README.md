# osagg: OpenSearch aggregation pushdown for Apache Superset

`osagg` lets Superset treat an OpenSearch index (or alias, or pattern) as a normal
**physical dataset** while every `GROUP BY` / aggregate runs **inside OpenSearch**.
Superset keeps generating plain SQL; osagg compiles it to OpenSearch composite
aggregations (paged), and only the small aggregated result is post-processed
by an embedded, locked-down DuckDB (ordering, limits, ratios, HAVING, window
functions, joins, …).

```
Superset (charts, dashboards, filters, alerts, SQL Lab)
   │  plain SQL (physical dataset = OpenSearch index)
   ▼
osagg  ── parse (sqlglot) ── plan ──┬─ filters      → bool query (exact SQL NULL semantics)
                                    ├─ GROUP BY     → composite sources (terms / date_histogram / histogram)
                                    ├─ aggregates   → sum / value_count / min / max / cardinality /
                                    │                 percentiles / extended_stats / filter sub-aggs
                                    └─ raw rows     → search + sort + size, PIT + search_after
   ▲                                      │
   └──── DuckDB (in-memory, no file / network / extension access) runs the rest
```

It is packaged as a normal Python wheel with:

* a DB-API 2.0 driver (`osagg.connect`)
* a SQLAlchemy 1.4/2.0 dialect (`osagg://`)
* a Superset DB engine spec (time grains, date literals, series-limit strategy),
  registered through the `superset.db_engine_specs` entry point

Tested with Superset 6.1 (running lab: charts, dashboards, SQL Lab, drill-to-detail,
alerts), and with the Superset 6.0.0 and 5.0.0 packages (engine spec loaded by
Superset's own loader, columns and queries through Superset's `Database` model, full
test suite under their pinned sqlglot 27/26 and pyarrow 16/14). Python 3.11,
OpenSearch 3.8, Trino 483.

## Install (pip only)

Full, tested runbook (OpenSearch role, pip online / offline wheelhouse, verification,
troubleshooting): [docs/DEPLOY.md](docs/DEPLOY.md).

```bash
PY=$(head -1 "$(command -v superset)" | cut -c3-)          # the Python that runs Superset
$PY -m pip install --upgrade osagg-0.2.9-py3-none-any.whl   # deps: duckdb, opensearch-py, Events
# offline: $PY -m pip install --upgrade --no-index --find-links ./wheelhouse osagg
$PY -m pip install trino                                     # only for transport=trino
```

Nothing to add to `superset_config.py`: the wheel registers the engine and its SQL
dialect (DuckDB grammar) through Superset's entry points. Restart the web server and
the Celery workers, then **Settings → Database connections → + Database →
OpenSearch (aggregation pushdown)**.

## Connection URI

Direct (Superset workers call the OpenSearch REST API):

```
osagg://USER:PASSWORD@opensearch-host:9200/default?timezone=Europe/Paris
osagg+https://USER:PASSWORD@opensearch-host:9200/default?timezone=Europe/Paris&ca_certs=/etc/ssl/os-ca.pem
```

Through Trino (Trino stays the only gateway; each OpenSearch request is sent
with `opensearch.system.raw_query`, composite pages included):

```
osagg://TRINO_USER@trino-host:8080/default?transport=trino&trino_catalog=opensearch&timezone=Europe/Paris
osagg://TRINO_USER:PWD@trino-host:8443/default?transport=trino&trino_http_scheme=https&timezone=Europe/Paris
```

| parameter | default | meaning |
|---|---|---|
| `timezone` | `UTC` | wall-clock zone of all TIMESTAMP values (filters, buckets, results) |
| `max_scan_rows` | `500000` | cap for reads without a LIMIT (aggregations that cannot be pushed down, SELECT without LIMIT) |
| `max_rows` | `0` | optional cap for `SELECT … LIMIT n` (default: none, rows are read page by page) |
| `max_buckets_total` | `2000000` | cap on groups returned by one aggregation |
| `page_size` | `50000` | composite page size (also bounded by the cluster `search.max_buckets`) |
| `topn` | `exact` | `approx`: `ORDER BY <aggregate> LIMIT n` on one key uses a two-phase terms aggregation (exact values, approximate membership at the boundary) |
| `count_distinct` | `exact` | `approx`: `COUNT(DISTINCT)` returns the cardinality estimate above `cardinality_precision` (the former behaviour) |
| `cardinality_precision` | `3000` | `precision_threshold` of the cardinality sketch: below it the count is exact; at it, `count_distinct=exact` counts the values as keys |
| `percentile_compression` | `500` | TDigest compression for MEDIAN / PERCENTILE_CONT (≈0.3 % max error) |
| `date_probe` | `true` | `false`: date fields in the default format are not checked for calendar days (values all at 00:00 UTC) |
| `request_timeout` | `300` | seconds per OpenSearch request |
| `join_max_keys` | `100000` | joins: an index that no other index filters may have at most this many matching documents or distinct join keys (see Joins of indices) |
| `label_source`, `label_date_format`, `label_time_source` | `POSITION_DATE`, `%Y%m%d`, `@timestamp_date` | where the business-date labels come from, per index (see Business-date labels) |
| `tables` | – | indices / aliases / patterns to show as tables, e.g. `tables=batch-jobs-*` (required for least-privilege accounts, see docs/DEPLOY.md) |
| `scheme`, `verify_certs`, `ca_certs`, `client_cert`, `client_key`, `url_prefix` | – | TLS / proxy options (direct) |
| `transport`, `trino_catalog`, `trino_schema`, `trino_user`, `trino_password`, `trino_http_scheme`, `max_buckets` | – | Trino transport |

## What is pushed down

| SQL | OpenSearch |
|---|---|
| `=`, `<>`, `IN`, `NOT IN`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM` | term / terms / range / exists, with `exists` guards so negations keep SQL NULL semantics |
| `LIKE`, `ILIKE`, `NOT LIKE`, `starts_with` | prefix / wildcard (`case_insensitive`) |
| `AND` / `OR` / `NOT`, constant sub-expressions (`now() - INTERVAL '1 hour'`) | bool query, constants folded |
| `LOWER(col) = 'x'`, `CAST(ts AS DATE) = DATE '…'` | case-insensitive term, day range in the time zone |
| `GROUP BY col` | composite `terms` (`missing_bucket` → NULL group) |
| `DATE_TRUNC('second'…'year', ts)`, `TIME_BUCKET(INTERVAL 'n minutes/hours', ts)`, `CAST(ts AS DATE)` | `date_histogram` with `time_zone` |
| `FLOOR(x / n) * n` | `histogram` |
| `COUNT(*)`, `COUNT(col)`, `SUM`, `AVG`, `MIN`, `MAX` (numeric, date, boolean, keyword) | doc_count, value_count, sum, min, max, terms(size 1) |
| `COUNT(DISTINCT col)` | cardinality while it is exact (below `cardinality_precision`); at it, the values become keys of the buckets and DuckDB counts them (exact) |
| `APPROX_COUNT_DISTINCT` | cardinality (an estimate above `cardinality_precision`) |
| `MEDIAN`, `QUANTILE_CONT`, `PERCENTILE_CONT … WITHIN GROUP` | percentiles (TDigest: an estimate, ≈0.3 % with the default compression) |
| `STDDEV_SAMP/POP`, `VAR_SAMP/POP` | extended_stats |
| `SUM(CASE WHEN c THEN x ELSE 0 END)`, `agg(x) FILTER (WHERE c)`, `COUNT_IF(c)` | `filter` sub-aggregation |
| any other GROUP BY / WHERE expression over grouped columns (`CASE … 'Others'`, `UPPER`, `COALESCE`, `EXTRACT(hour …)`, `strftime`, Sunday weeks) | **two-level**: OpenSearch groups by the underlying columns (timestamps at the needed grain), DuckDB re-aggregates the buckets (sum / count / min / max / avg) |
| `ORDER BY key LIMIT n` / `LIMIT n` | composite stops after n buckets |
| `SELECT … WHERE … ORDER BY col LIMIT n OFFSET m` | search with sort / size, point-in-time + `search_after` beyond 10 000 |
| virtual datasets `SELECT … FROM (SELECT * FROM idx WHERE …) AS virtual_table` | flattened into one pushed-down query |
| `JOIN` / `LEFT JOIN` / `USING` of two or more indices on equal fields, in aggregating queries | each index grouped by its join keys and GROUP BY columns in OpenSearch, keys of the smaller indices pushed to the bigger ones as `terms`; DuckDB joins the grouped rows (see Joins of indices) |

| any other filter on **one** keyword column (CASE mappings, `LOWER`/`SUBSTRING`, `POSITION_LABEL LIKE 'W-%'`, Python functions) | the column's distinct values are listed (one small aggregation), the filter is evaluated on them, and `terms` on that column is pushed down |
| `ORDER BY <column>` / `ORDER BY <position>` / arithmetic expressions on raw rows | sort / painless script sort, pushed down with LIMIT |

Everything else (ORDER BY, LIMIT, HAVING, arithmetic on aggregates, window
functions, CTEs, UNION, the join of grouped rows) runs in DuckDB on the aggregated
rows.

If a query cannot be pushed down (e.g. a non-decomposable aggregate over an
arbitrary expression), osagg fetches the needed columns of the matching
documents **only if** there are at most `max_scan_rows` of them; otherwise it
fails with a message explaining why, instead of pulling billions of rows.

## Business-date labels (POSITION_LABEL)

Indices with a keyword `POSITION_DATE` (yyyymmdd) get a computed column
`POSITION_LABEL`: `D`, `D-x`, `W-x`, `Y-x` relative to the session date (weekends →
Friday, weekdays before 14:00 → previous business day). Filters on it are rewritten to
`POSITION_DATE`: `=` / `IN` become a `terms` filter on the dates computed from the
calendar (no extra request), other predicates go through the single-column value
enumeration, and `GROUP BY` groups on the date in OpenSearch. URI parameters:
`label_timezone` (default: the host clock), `label_cutoff` (`14:00`), `label_years`
(`true`), `label_source` / `label_column` (`POSITION_DATE` / `POSITION_LABEL`,
`label_source=none` turns it off). See docs/DEPLOY.md, step 5b.

The position date may have another name or form per index: `label_source` is a
comma-separated list tried in order, `pattern:FIELD` items applying only to the indices
matching the pattern, `FIELD=format` giving the format of a keyword date. Date fields
work too. For example `risk-*:COB_DATE=%Y-%m-%d,POS_DAY,POSITION_DATE`: `risk-*`
indices use their keyword `COB_DATE` (yyyy-mm-dd), the others a date field `POS_DAY`
or the keyword `POSITION_DATE`. `label_time_source` picks the execution time the same
way. Indices without any of these fields simply have no label columns.

`POSITION_TIME` is the execution time (`@timestamp_date`) moved onto the D-1 position
date (+7 days for W-1, +14 for W-2…): use it as the X-axis with `POSITION_LABEL` as
dimension to overlay position dates on one timeline. Time buckets on it group by
position date and minute in OpenSearch; time ranges on it become one `@timestamp_date`
range per position date.

Other Python functions can be exposed to osagg SQL with
`osagg.register_function(name, func, ["VARCHAR"], "VARCHAR")`; they run in DuckDB on
grouped values, and single-column filters built on them are pushed down as terms.

## Joins of indices

OpenSearch cannot join, but an aggregating query over joined indices still runs
without reading documents:

```sql
SELECT c."SITE", b."TIER", COUNT(*), SUM(a."JOB_DURATION_d")
FROM "batch-jobs" a
JOIN "apps" b ON a."APPLICATION" = b."APPLICATION" AND a."ENVIRONMENT_TYPE" = b."ENVIRONMENT_TYPE"
LEFT JOIN "teams" c ON b."TEAM" = c."TEAM"
WHERE a."STATUS_INFO" = 'FAILED' AND a."POSITION_LABEL" = 'D-1'
GROUP BY 1, 2
```

* Each index is aggregated in OpenSearch with its own filters, by its join keys and its
  own GROUP BY columns; every group keeps its document count. DuckDB joins the grouped
  rows and combines COUNT / SUM / AVG / MIN / MAX exactly (a group counts as many times
  as it matches on the other side).
* Huge indices: the indices are aggregated smallest first (documents matching their
  filters), and the join keys found so far are pushed to the next ones as a `terms`
  filter. Two billion-document indices joined on a job id, with a filter keeping 5,000
  jobs on one side, are both grouped for those 5,000 ids only. Keys are never pushed
  into the preserved side of a LEFT JOIN.
* Every index must be bounded: at most `join_max_keys` (100,000) matching documents,
  or keys received from an index aggregated before it, or at most `join_max_keys`
  distinct join keys (cardinality aggregation, on a sample first). Otherwise the query
  is refused before anything is read, with the reason.
* Refused, never answered by reading documents: row lists over a join (no aggregate),
  `COUNT(DISTINCT …)` or percentiles across the join, conditions mixing indices
  (`a.x > b.y`), join conditions other than equalities, `FULL` / `CROSS` joins, join
  fields of different types.
* In Superset: SQL Lab, or a virtual dataset that lists its columns
  (`SELECT a."APPLICATION", a."JOB_DURATION_d", b."TEAM" FROM … JOIN …`); charts on
  it aggregate through the join. `EXPLAIN` shows the order of the indices, each
  request and the keys each index receives.

## AI agent and tools (Superset 6.1 MCP)

`tools/` holds an AI agent for Superset 6.1's MCP service with a local OpenAI-compatible
LLM, and a second MCP service for what Superset's lacks (docs/DEPLOY.md, sections 10-11):

* `superset_agent.py` — asks the data dictionary first, runs SQL through osagg, answers
  report requests in the chat (summary, table, text chart) and gives JSON, an Excel file,
  a chart image or an e-mail when asked; it validates chart configs before Superset's MCP
  service sees them (no duplicate or broken charts).
* `superset_tools_mcp.py` — `describe_data` (field meanings, synonyms, values,
  relationships, glossary from `catalog.yaml`), `export_excel` (row lists may join a big
  index with small ones here only), `chart_image`, `send_email`, `create_report`.

Everything installs from one offline bundle (`scripts/make_bundle.sh`, `deploy/bundle/`).

## EXPLAIN

In SQL Lab: `EXPLAIN SELECT …` shows every OpenSearch request (DSL) and the
residual DuckDB SQL; `EXPLAIN ANALYZE SELECT …` also runs it and reports rows,
requests and OpenSearch time per scan.

## Semantics worth knowing

* Results are exact, with these exceptions: percentiles (MEDIAN, QUANTILE_CONT,
  PERCENTILE_CONT are TDigest estimates), `APPROX_COUNT_DISTINCT`, and
  `count_distinct=approx` / `topn=approx` when you ask for them.
* An OpenSearch answer from part of the shards (a failed or timed-out shard) is
  an error, never a result: OpenSearch itself answers HTTP 200 with what the
  other shards hold.
* Timestamps are naive wall-clock values in `timezone`. In zones with DST,
  sub-day buckets of the repeated autumn hour are merged, like SQL on local time.
* Date fields whose format has no time of day (`yyyyMMdd`, `basic_date`, …) and
  default-format date fields holding only midnights are calendar days in UTC,
  whatever `timezone`; `= 20261001` is read in the field's own format.
* A field the indices of a pattern map differently (text with `.keyword` here,
  keyword there; date here, keyword there) is compared in each group of indices
  as that group maps it, as Discover's filters do.
* Multi-valued fields: grouping counts a document once per value (OpenSearch
  terms semantics, like `UNNEST`); `col = 'x'` matches if any value matches;
  `COUNT(col)` of a keyword or number counts its values and `SUM` adds them all. Raw rows return
  multi-valued keyword fields as JSON text and the first value of a number.
* `text` fields are grouped / compared exactly through their keyword sub-field
  (`field.keyword`); text fields without one cannot be grouped. Values longer
  than the keyword's `ignore_above` (256 in the dynamic mapping) are not in it:
  where a field has some, they are read from the documents.
* `nested` fields are not exposed.
* Through Trino, raw-document queries are limited to 10 000 rows per query
  (no point-in-time through `raw_query`); aggregations have no such limit.

## Development

```bash
pip install -e .[test,trino]
pytest tests/                                 # offline: no OpenSearch needed
```

The offline tests check the generated OpenSearch requests (including SQL captured from
Superset 6.1 charts, which must be pushed down completely), the business-date labels
against a reference implementation, paging, retries and TLS options. The integration
tests (every query through a real OpenSearch compared with DuckDB over the same documents,
both transports, forced paging) run on an internal lab and are not published.
