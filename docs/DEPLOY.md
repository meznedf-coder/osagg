# osagg deployment runbook — pip only (Superset → OpenSearch and Prometheus / Mimir, no Docker, no Trino)

Everything below was tested on 25–27 Sep 2026: Superset 6.1.0 installed with pip in a
fresh virtualenv (Python 3.11), osagg and promagg installed into it with pip (online,
offline from the wheelhouse, as an upgrade from 0.1.0, and the 0.2.6 bundle as an offline
upgrade); the lab Superset 6.1 (web server, Celery worker and beat) runs the same wheels
against OpenSearch 3.8 and Grafana Mimir 3.2.1. Commands are copy-paste ready; replace the
values in `<...>`.

**Short path: the bundle.** `osagg-0.2.8-promagg-0.2.2-bundle-py311-linux-x86_64.tar.gz`
(38 MB, offline; also in two parts under 30 MB) holds everything: osagg (OpenSearch),
promagg (Prometheus / Mimir), the packages of Superset 6.1's MCP service, the AI agent and
its tools service (data dictionary, Excel extracts, chart images, e-mails, reports,
PromQL, health checks), config snippets, systemd units and this runbook. Its `INSTALL.txt`
is the whole procedure:

| Part | Commands | Then |
|---|---|---|
| A. osagg + promagg (required) | `./install.sh $PY` | restart web server, workers, beat |
| B. MCP services + AI agent | `./install.sh $PY --mcp`; paste `config/superset_config_mcp.py`; install `systemd/superset-mcp.service` and `systemd/superset-tools.service`; fill `ai/tools.env`, `ai/catalog.yaml`, `ai/agent.env` | `$PY ai/superset_agent.py "question"` |
| C. E-mail reports | paste `config/superset_config_reports.py`; Chromium on the workers | Alerts & Reports, or ask the agent |
| D. Prometheus / Mimir metrics | database URI `promagg+https://...` (section 12); metrics and health checks in `ai/catalog.yaml`; `superset_tools_mcp.py push-metrics` | charts on metrics; the agent investigates with check_health |

(`$PY` = the Python of Superset, step 2.) What changed: `CHANGES.md`; SQL of promagg:
`promagg-README.md`.
The sections below explain each step and were all tested.

## 0. Prerequisites

| Item | Requirement |
|---|---|
| Superset | 6.x (or 5.0) installed with pip, Python 3.10–3.12. |
| Platform | Linux x86-64 with glibc ≥ 2.28 for the offline wheelhouse (RHEL/Rocky 8+, Debian 11+, Ubuntu 20.04+). Other platforms: online install from PyPI or your pip mirror. |
| Network | The Superset host (web server and Celery workers) reaches OpenSearch's HTTPS port (usually 9200). |
| OpenSearch | 1.x, 2.x or 3.x. Point-in-time paging needs 2.4+. |
| Account | A service account with the role in step 1. |
| CA | Optional: the PEM file of the CA that signed the OpenSearch certificates, readable by the user that runs Superset (e.g. `/etc/superset/opensearch-ca.pem`). Without it, use `verify_certs=false` (step 4). |

Quick network check from the Superset host:

```bash
curl -s --cacert /etc/superset/opensearch-ca.pem -u 'svc_superset:<password>' \
  'https://<opensearch-host>:9200/_resolve/index/<index-or-pattern>'
# expected: {"indices":[{"name":"<index>",...}],"aliases":[...],"data_streams":[]}
# no CA file: replace  --cacert /etc/superset/opensearch-ca.pem  by  -k
```

## 1. OpenSearch: role and service account (least privilege, tested)

The service account needs **no cluster permission**. Replace the pattern by the
indices / aliases Superset may read (the pattern must cover aliases too).

```http
PUT _plugins/_security/api/roles/superset_osagg
{
  "cluster_permissions": [],
  "index_permissions": [{
    "index_patterns": ["batch-jobs*"],
    "allowed_actions": ["read", "indices:admin/mappings/get"]
  }]
}

PUT _plugins/_security/api/internalusers/svc_superset
{ "password": "<strong password>" }

PUT _plugins/_security/api/rolesmapping/superset_osagg
{ "users": ["svc_superset"] }
```

What each permission covers: `read` = searches, aggregations, point-in-time,
`_resolve/index` (table list); `indices:admin/mappings/get` = column list.
Optional: `cluster:monitor/state` lets osagg read `search.max_buckets`; without it
osagg assumes 65,535 (set `page_size` if you raised the cluster setting).
An LDAP / AD technical account can be mapped to the same role instead of an internal user.

## 2. Install the wheel with pip

Files (either one):

* offline, everything: `osagg-0.2.8-promagg-0.2.2-bundle-py311-linux-x86_64.tar.gz` (Python 3.11):
  `./install.sh $PY` runs the offline command below;
* offline: `osagg-0.2.8-wheelhouse-py3XX-linux-x86_64.tar.gz` for your Python version
  (`py310`, `py311` or `py312`, about 22 MB): `osagg`, `duckdb`, `opensearch-py`, `Events`;
* online: `osagg-0.2.8-py3-none-any.whl` alone, when the host can reach PyPI or your
  pip mirror (pip downloads the three dependencies).

```bash
# 1. The Python that runs Superset (its virtualenv); run the rest as the owner of that virtualenv
head -1 "$(command -v superset)"           # e.g. #!/opt/superset/venv/bin/python3.11
PY=/opt/superset/venv/bin/python3.11       # what the line above printed, without "#!"
$PY --version                              # 3.11 -> the py311 archive

# 2a. Offline: extract the archive and install from its wheelhouse folder
tar xzf osagg-0.2.8-wheelhouse-py311-linux-x86_64.tar.gz
$PY -m pip install --upgrade --no-index --find-links ./wheelhouse osagg

# 2b. Or online (PyPI or company mirror)
$PY -m pip install --upgrade osagg-0.2.8-py3-none-any.whl

# 3. Check
$PY -m pip show osagg | head -2            # Version: 0.2.8
```

Already on 0.2.x: only the new wheel is needed, even offline:
`$PY -m pip install --upgrade --no-index osagg-0.2.8-py3-none-any.whl`

If `superset` is not on your PATH, its path is in the `ExecStart=` line of your Superset
service (`systemctl cat <superset service>`).

pip adds four packages (`osagg`, `duckdb`, `opensearch-py` 3.0.0, `Events`) and changes
nothing else: Superset already has `sqlglot`, `pyarrow` and `sqlalchemy`, and
`pip check` stays clean. The same command upgrades 0.1.0. On Superset 5.0 add
`"opensearch-py==2.6.0"` at the end of the command.

Then **restart Superset**: the web server, the Celery worker(s) and Celery beat, the way
you start them (systemd units, supervisor, or the `gunicorn` / `celery` commands), e.g.
`sudo systemctl restart superset superset-worker superset-beat`.

Install it everywhere Superset runs queries: the web server and every Celery worker
(alerts, reports, async SQL Lab). Repeat step 2 on each host if they are separate.

**Fresh pip install of Superset 6.1.0:** `pip install apache-superset==6.1.0` now pulls
Flask-Caching 2.5.1, with which Superset 6.1.0 does not start (`SupersetMetastoreCache.__init__()
got an unexpected keyword argument 'ignore_delete_many_errors'`). Pin it:
`pip install "apache-superset==6.1.0" "flask-caching==2.3.1"` (tested 27 Sep 2026; `install.sh`
prints a warning when it finds 2.5 or later). Existing installations are not affected.

## 3. `superset_config.py`

**Nothing is required.** The wheel registers its engine and its SQL dialect itself
(tested without any osagg line: SQL Lab, charts, dashboards, native filters, alerts;
the registration also works on Superset 6.0 and 5.0).

Upgrading from 0.1.0, where the labels came from your calendar functions: keep
`superset_config.py` unchanged until the dataset is switched over, in the order given in
step 9.

Optional, for exact top-N charts over huge cardinalities: `SUPERSET_WEBSERVER_TIMEOUT = 180`
(and the same value for gunicorn's `--timeout`).

## 4. Create the database connection

UI: **Settings → Database Connections → + Database →** select
**"OpenSearch (aggregation pushdown)"** (it appears once the wheel is installed)
→ SQLAlchemy URI:

```
osagg+https://svc_superset:<url-encoded password>@<opensearch-host>:9200/default?timezone=Europe/Paris&tables=batch-jobs*&topn=approx&ca_certs=/etc/superset/opensearch-ca.pem
```

Without the CA file, replace `ca_certs=…` by `verify_certs=false`:

```
osagg+https://svc_superset:<url-encoded password>@<opensearch-host>:9200/default?timezone=Europe/Paris&tables=batch-jobs*&topn=approx&verify_certs=false
```

* `osagg+https` = TLS. Use `osagg://` only for plain-HTTP clusters.
* `verify_certs=false`: the traffic stays encrypted (HTTPS), but the server's certificate
  is not checked, so osagg cannot tell the real cluster from an impostor on the network.
  Switch to `ca_certs=` once you have the CA certificate (the OpenSearch team can give
  it: it is public, not a secret).
* Special characters in the password must be percent-encoded:
  `@`→`%40`, `:`→`%3A`, `/`→`%2F`, `#`→`%23`, `?`→`%3F`, `%`→`%25`, `&`→`%26`.
* Instead of URI parameters you may use **Advanced → Other → Engine parameters**:
  `{"connect_args": {"ca_certs": "/etc/superset/opensearch-ca.pem", "tables": "batch-jobs*", "timezone": "Europe/Paris", "topn": "approx"}}`
  (both forms were tested; without the CA file use `"verify_certs": false` instead of
  `"ca_certs"`).
* **Advanced → SQL Lab:** expose in SQL Lab ✓, allow DML ✗, CTAS/CVAS ✗.

Press **Test connection**. It makes a real authenticated call:

| Result | Meaning |
|---|---|
| "Connection looks good!" | OK |
| `AuthenticationException(401, 'Unauthorized')` | wrong user / password (or password not URL-encoded) |
| `CERTIFICATE_VERIFY_FAILED` | `ca_certs` missing or wrong file; no CA file: `verify_certs=false` |
| "not allowed to list all indices. Add tables=…" | restricted account: add `tables=<pattern>` |
| `no permissions for [indices:admin/mappings/get]` | add that action to the role |

API alternative (automation):

```bash
curl -X POST https://<superset>/api/v1/database/ -H "Authorization: Bearer <token>" \
  -H "X-CSRFToken: <csrf>" -H "Content-Type: application/json" -d '{
  "database_name": "OpenSearch (osagg)",
  "sqlalchemy_uri": "osagg+https://svc_superset:<pw>@<host>:9200/default?timezone=Europe/Paris&tables=batch-jobs*&topn=approx&ca_certs=/etc/superset/opensearch-ca.pem",
  "expose_in_sqllab": true, "allow_dml": false}'
```

### URI / connect_args parameters

| parameter | default | use |
|---|---|---|
| `timezone` | `UTC` | wall-clock zone of all timestamps: set `Europe/Paris` |
| `tables` | – | comma-separated indices / aliases / patterns to show; required for restricted accounts |
| `topn` | `exact` | `approx` = two-phase top-N for `ORDER BY <metric> LIMIT n` (values exact) |
| `ca_certs`, `client_cert`, `client_key` | – | TLS files: CA to trust, client certificate and key |
| `verify_certs` | `true` | `false` = HTTPS without certificate checking (when you have no CA file) |
| `max_rows` | `0` (none) | optional cap for `SELECT … LIMIT n`: without it, the n rows are read page by page (10,000 per request), whatever n is |
| `max_scan_rows` | 500000 | cap for reads **without** a LIMIT: aggregations that cannot run in OpenSearch, `SELECT` without LIMIT |
| `max_buckets_total` | 2000000 | cap on groups returned by one aggregation |
| `page_size` | 50000 | composite page size (≤ cluster `search.max_buckets`) |
| `request_timeout` | 300 | seconds per OpenSearch request |
| `cardinality_precision`, `percentile_compression` | 3000, 500 | accuracy of COUNT DISTINCT / percentiles |
| `url_prefix` | – | when OpenSearch sits behind a proxy path |
| `enum_max_values`, `enum_cache_ttl` | 10000, 60 | single-column filters (labels, CASE mappings) are turned into terms filters when the column has at most this many values; value lists are cached this many seconds |
| `label_timezone` | `local` | clock of the business-date labels (step 5b): `local` = the Superset host clock, like `datetime.now()`; or a zone such as `Europe/Paris` |
| `label_cutoff` | `14:00` | session cut-off of the labels |
| `label_years` | `true` | `Y-x` labels; `false` = exactly the old functions (that date reads `W-52`) |
| `label_source`, `label_column` | `POSITION_DATE`, `POSITION_LABEL` | field(s) the label is computed from, tried in order, per index (`pattern:FIELD=format`, step 5b), and the column name; `label_source=none` turns it off |
| `label_date_format` | `%Y%m%d` | format of keyword position dates without `=format` |
| `label_time_source`, `label_time_column` | `@timestamp_date`, `POSITION_TIME` | execution time moved onto the D-1 position date (same rules as `label_source`), and its column name |
| `join_max_keys` | 100000 | joins (step 5c): an index that no other index filters may have at most this many matching documents or distinct join keys |

## 5. Create the dataset

**Datasets → + Dataset →** database = the connection, schema = `default`, table =
`batch-jobs` (or the alias / pattern) → *Create dataset and create chart*.

Then **Edit dataset**:

* Columns: tick *Is temporal* on `@timestamp_date` and make it the default datetime.
* Metrics (reusable in every chart), for example:

| metric | SQL expression |
|---|---|
| `jobs` | `COUNT(*)` |
| `failed_jobs` | `SUM(CASE WHEN "STATUS_INFO" = 'FAILED' THEN 1 ELSE 0 END)` |
| `failure_rate` | `SUM(CASE WHEN "STATUS_INFO" = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)` |
| `avg_duration` | `AVG("JOB_DURATION_d")` |
| `cpu_cost` | `SUM("CPU_COST_H_d")` |
| `max_jobs` | `MAX("MAX_JOBS_COUNT_d")` |
| `nodes` | `COUNT(DISTINCT "NODE")` |

Field names are case sensitive and keep their OpenSearch spelling (quote them).

## 5b. Business-date labels (D, D-x, W-x, Y-x): built in

Every index with a keyword `POSITION_DATE` (yyyymmdd) gets an extra column
`POSITION_LABEL`, computed by osagg with your calendar rules. Nothing to install or
configure.

* Session date: Saturday and Sunday → Friday; Monday–Friday before 14:00 → the previous
  business day; otherwise today.
* `Y-x`: x × 364 days before the session date (same weekday, 52 weeks earlier). Your
  functions had no Y rule; `label_years=false` in the URI gives exactly the old
  labels (that date then reads `W-52`).
* `W-x`: x × 7 days before the session date.
* `D-x`: x = number of weekdays from the date to yesterday; `D` for today, later dates,
  and a weekend just before today.

Use it like any other column: group by, filters, native filter, drill to detail, SQL
Lab. osagg turns it into `POSITION_DATE` conditions:

| You use | OpenSearch receives |
|---|---|
| `POSITION_LABEL IN ('D-1', 'W-1')` (native filter, chart filter, SQL) | `terms` on `POSITION_DATE` with the matching dates, computed from the calendar: no extra request |
| `POSITION_LABEL = 'D'` | the same, plus one small lookup of the dates after today + 7 |
| `LIKE 'W-%'`, `NOT IN`, `IS NULL` | one small aggregation listing the `POSITION_DATE` values (cached 60 s), then `terms` |
| `GROUP BY POSITION_LABEL` | composite aggregation on `POSITION_DATE`; DuckDB labels the buckets and merges them |
| samples, drill to detail, `SELECT *` | the label is computed on the returned rows |

Dataset (**Edit dataset**):

1. **Columns**: if you created a calculated column `POSITION_LABEL` before, delete it,
   then **Sync columns from source**: `POSITION_LABEL` (VARCHAR) appears. New datasets
   have it right away.
2. **Metrics**: `latest_position_date` = `MAX("POSITION_DATE")`. Native filter:
   *Value* on `POSITION_LABEL`, **Sort by metric** `latest_position_date`, descending:
   the list reads D-1, D-2, …, W-1, … .
3. **Settings → Cache timeout**: `300` (see below).

Check in SQL Lab:

```sql
EXPLAIN
SELECT "APPLICATION", COUNT(*) AS jobs
FROM "batch-jobs"
WHERE "POSITION_LABEL" IN ('D-1', 'W-1')
GROUP BY 1
```

The plan shows `label filter D-1, W-1 (today 20260925, session date 20260924) ->
POSITION_DATE IN (2 date(s)) pushed down` and `"terms": {"POSITION_DATE": ["20260917",
"20260924"]}`.

**Compare position dates on one timeline (D-1 vs W-1 vs W-2).** Every index with
`POSITION_DATE` and `@timestamp_date` also gets `POSITION_TIME` (TIMESTAMP): the execution
time moved onto the D-1 position date, +7 days for W-1, +14 for W-2, +1 (or +3 on Monday)
for D-2… Each position date keeps its own documents, even when one day executes two or
three position dates. Tested in the lab, chart "Jobs by position date: D-1 vs W-1 / W-2":

1. **Line Chart**, X-axis `POSITION_TIME`, time grain 10 minute, any metrics.
2. **Dimensions**: `POSITION_LABEL`.
3. **Filters**: `POSITION_LABEL` in `D-1`, `W-1`, `W-2` (any labels work).
4. Optional, to crop to the session: time range on `POSITION_TIME` → **Advanced**:
   `DATEADD(DATETIME("yesterday"), 14, hour)` to `DATEADD(DATETIME("today"), 10, hour)`.

One line per position label. OpenSearch receives `terms` on the three position dates and,
for the time range, one `@timestamp_date` range per position date (moved back by its
shift), then groups by position date and 1-minute buckets; DuckDB moves the buckets and
draws the lines. A time range on `POSITION_TIME` needs a `POSITION_LABEL` filter (or at most
200 position dates in play). After upgrading, **Sync columns from source** once so the
dataset lists `POSITION_TIME` (Superset marks it temporal).

**D and D-1 against the average of W-1..W-4.** Use a **Mixed Chart** (two queries on
the same X-axis). Tested in a lab dashboard "Position dates - D, D-1 vs W-x":

1. **X-axis** `POSITION_TIME`, time grain 10 minute.
2. **Query A**: metric `COUNT(*)` (or any), dimension `POSITION_LABEL`, filter
   `POSITION_LABEL` in `D`, `D-1`. This gives one line per label.
3. **Query B**: metric *Custom SQL* `COUNT(*) / 4.0` labelled `Avg W-1..W-4`, no
   dimension, filter `POSITION_LABEL` in `W-1`, `W-2`, `W-3`, `W-4`. This gives one line:
   the four weeks added up per 10 minutes and divided by 4 (a missing week counts as 0).
4. Customize: both series *Line*, Query B on the primary Y axis.

Other metrics in Query B:

* sums: divide by 4, e.g. `SUM("CPU_COST_H_d") / 4.0` or
  `SUM(CASE WHEN "STATUS_INFO" = 'FAILED' THEN 1 ELSE 0 END) / 4.0`;
* averages: the same metric, e.g. `AVG("JOB_DURATION_d")`, which averages all the jobs of
  the four weeks;
* MIN / MAX: the lowest / highest over the four weeks.

The W-x follow your session rule: before 14:00 they are the weeks of D-1's session, and
after 14:00 the weeks of D's session. On the dashboard, a time range filter on
`POSITION_TIME` (e.g. `DATEADD(DATETIME("yesterday"), 14, hour)` to
`DATEADD(DATETIME("today"), 10, hour)`) crops every chart to the session.

To compare execution windows instead (whatever the position date), use Superset's
**Time comparison** on `@timestamp_date` (`1 week ago`, `2 weeks ago`).

Position date under another name, form or type (connection option `label_source`,
set in **Advanced → Other → Engine parameters** to avoid URL-encoding `%` and `:`):

```json
{"connect_args": {"label_source": "risk-*:COB_DATE=%Y-%m-%d,POS_DAY,POSITION_DATE",
                  "label_time_source": "risk-*:EXEC_TS,@timestamp_date"}}
```

* Items are tried in order; `risk-*:COB_DATE` applies only to the indices matching
  `risk-*` (such rules go first), a bare name to every index.
* `COB_DATE=%Y-%m-%d`: a keyword date in that format (`%Y%m%d` by default, or
  `label_date_format`). With formats that sort like dates (`%Y%m%d`, `%Y-%m-%d`,
  `%Y/%m/%d`, `%Y.%m.%d`) a filter on `D` looks up the later dates with a range; other
  formats (e.g. `%d/%m/%Y`) work too, through one small aggregation listing the
  position-date values (cached 60 s).
* A `date` field (here `POS_DAY`) works too: label filters become day ranges.
* An index without any of the fields has no `POSITION_LABEL` / `POSITION_TIME`; all its
  other columns work as usual. `label_source=none` turns labels off everywhere.
* Tested in the lab with the same documents stored three ways (keyword yyyymmdd,
  keyword yyyy-mm-dd, date field): identical results for group by, `IN`, `=`, `LIKE`,
  `POSITION_TIME` overlays and row lists.

Clock and cache:

* "Now" is the clock of the Superset host, as with `datetime.now()` in your old
  `superset_config.py`. If the Superset hosts do not run in the business time zone, add
  `&label_timezone=Europe/Paris` to the URI.
* The labels move at 14:00 and at midnight, but Superset's chart cache key does not
  change then. A 300 s cache timeout on the dataset (or on the database) means labels
  are at most 5 minutes late; **Force refresh** on a dashboard is always exact.

## 5c. Joins of indices

osagg can join indices when the query aggregates: each index is grouped in OpenSearch
(with its own filters, by its join keys and its own GROUP BY columns, every group keeping
its document count) and DuckDB joins the grouped rows, combining COUNT / SUM / AVG / MIN
/ MAX exactly. Two indices or more, `JOIN`, `LEFT JOIN`, `RIGHT JOIN` (two indices),
`USING`, on one field or several.

In Superset, create a **virtual dataset** that lists its columns (not `SELECT *`):

```sql
SELECT a."@timestamp_date", a."APPLICATION", a."STATUS_INFO", a."JOB_DURATION_d",
       b."TEAM", b."TIER"
FROM "batch-jobs" a
JOIN "applications" b ON a."APPLICATION" = b."APPLICATION"
                     AND a."ENVIRONMENT_TYPE" = b."ENVIRONMENT_TYPE"
```

Save it without running it (SQL Lab: **Save ▾ → Save dataset**, or paste the SQL in
**Edit dataset → Source → Virtual**): running it would list raw rows, which osagg refuses
for a join. Superset reads the dataset's columns with a `LIMIT 0` probe that osagg
answers from the index mappings, without any OpenSearch request. Charts on it (COUNT(*) by
`TEAM`, SUM of durations per `TIER` over time…) run as aggregations through the join;
filters on either side go to that index. Aggregating queries also work directly in SQL Lab.

Huge indices: what matters is the number of distinct join keys, not the number of
documents. osagg aggregates the indices smallest first and pushes the keys found so far
to the next ones as a `terms` filter, so a billion-document index joined with a filtered
one (e.g. the failed jobs of D-1) is grouped only for the matching keys. Every index must
be bounded: at most `join_max_keys` (100,000) matching documents, or keys received from
an index aggregated before it, or at most `join_max_keys` distinct join keys (checked on
a sample of `join_max_keys` documents per shard first, then with a cardinality
aggregation). Otherwise the query is refused before anything is read: add a filter
(position date, time range, status…) on one of the indices, or join on a
lower-cardinality field. Keys are never pushed into the preserved side of a `LEFT JOIN`.

`EXPLAIN` shows the order of the indices, each request and the keys each index receives,
e.g. `join: 10 key(s) of applications pushed on APPLICATION`.

Refused (osagg never reads documents to join): row lists over a join (no aggregate),
`COUNT(DISTINCT …)` or percentiles across the join, conditions mixing indices
(`a.x > b.y`), join conditions other than equalities of fields, `FULL` / `CROSS` joins,
join fields of different types. The message says which.

## 6. Verify

SQL Lab, database = the connection:

```sql
EXPLAIN
SELECT "APPLICATION", COUNT(*) AS jobs, AVG("JOB_DURATION_d") AS avg_s
FROM "batch-jobs"
WHERE "@timestamp_date" >= now() - INTERVAL '1 day'
GROUP BY 1 ORDER BY avg_s DESC
```

The first line must read `-- 1. OpenSearch aggregation (direct)`: GROUP BY runs in
OpenSearch. Run it without `EXPLAIN` to get the rows. Then build one timeseries
chart (time grain 10 minute) and one table sorted by a metric.

Logs (`osagg` logger, INFO, in the Superset log): one line per request, e.g.
`osagg scan aggregation/direct on <index>: 80 rows, 1 request(s), OpenSearch 312ms`.

## 7. Operating notes

* Aggregations run inside OpenSearch on any number of documents: COUNT, SUM, AVG, MIN,
  MAX, COUNT DISTINCT, percentiles, per time bucket or over the whole range (Big Number),
  with filters and group by. No row cap applies to them. On the 10M-document lab index,
  SUM/AVG/MIN/MAX/COUNT per 10 minutes over a week takes 0.5 s, and MAX/MIN/AVG/SUM/COUNT
  over all 10M documents 2.4 s, on a Raspberry Pi. MIN and MAX of a text field with
  millions of distinct values (an ID) per time bucket are much heavier: prefer numeric fields.
* Row lists (`SELECT` of fields, table charts in raw mode, CSV export, drill to detail)
  are read page by page, 10,000 documents per request, up to the query's LIMIT with no
  osagg cap (150,000 rows took 26 s in the lab, 281 MB in the Superset process). Superset
  itself stops at `SQL_MAX_ROW` (100,000 by default; 500,000 for table charts with server
  pagination); raise it in `superset_config.py` if you need more.
* Sorting: `ORDER BY` on time buckets, keys and metrics (SUM, AVG, MAX, MIN, COUNT,
  COUNT DISTINCT, ratios) is exact; raw-row sorts on columns and arithmetic
  expressions are pushed to OpenSearch (script sort). 39 ordered tests cover it.
* Exact top-N on a very high-cardinality field pages through every group; with
  `topn=approx` values stay exact and only the last ranks may differ.
* `COUNT(DISTINCT)` and percentiles are approximate (HyperLogLog++, TDigest).
* Row level security in Superset adds WHERE clauses that are pushed down.
  All Superset users query with the service account's OpenSearch permissions.
* OpenSearch side at scale: `eager_global_ordinals` on grouped keyword fields,
  refresh interval ≥ 30 s, time-based indices behind an alias, request cache on.

## 8. Troubleshooting

| Symptom | Fix |
|---|---|
| `No module named pip` | `$PY -m ensurepip --upgrade`, then step 2 again |
| pip tries to reach the internet | offline, use `--no-index --find-links ./wheelhouse` (step 2a) |
| `No matching distribution found for duckdb` (offline) | the archive does not match your Python (`$PY --version`: take the py310 / py311 / py312 archive) or your platform (Linux x86-64 only): use the online command (2b) |
| Engine missing in "+ Database" list | osagg is not in the Python that runs the web server (`head -1 "$(command -v superset)"`), or Superset was not restarted; Superset logs "Unable to load Superset DB engine spec" |
| Alerts / reports: "Could not load database driver" | the Celery worker runs from another virtualenv or host: install there too and restart it |
| No `POSITION_LABEL` column | the index has no keyword `POSITION_DATE`, or the dataset was not synced (**Sync columns from source**) |
| Labels change a few minutes after 14:00 | chart cache (step 5b); set `label_timezone` if the servers run in UTC |
| Test connection errors | see the table in step 4 |
| `Column "x" does not exist` | field names are case sensitive: `"APPLICATION"`, not `application` |
| `... more than 2,000,000 groups` | add filters, a coarser time grain, or raise `max_buckets_total` |
| `This JOIN cannot run in OpenSearch: …` | the reason follows (step 5c): add a filter on one index, aggregate instead of listing rows, or join on equal fields of the same type |
| `SUM("X"): "X" is a text field …` | SUM / AVG need a numeric field; to count documents use `COUNT(*)` or `COUNT("X")` |
| `... needs N raw documents ... above the safety cap` | a query without LIMIT that could not run in OpenSearch: add a LIMIT, make the aggregate pushable (see EXPLAIN), or raise `max_scan_rows` |
| Row lists stop at 100,000 rows | that is Superset's `SQL_MAX_ROW` (default 100,000; table charts with *server pagination* go to 500,000): add `SQL_MAX_ROW = 500000` to `superset_config.py` |
| Charts time out | `SUPERSET_WEBSERVER_TIMEOUT`, gunicorn `--timeout`, `request_timeout`; use `topn=approx` for top-N tables |
| Day boundaries off by 1–2 h | set `timezone=Europe/Paris` |
| Alerts never run | Celery worker and beat must be running and have osagg; the metadata DB must be PostgreSQL or MySQL |

## 9. Upgrade and rollback

Upgrade from 0.1.0 (labels from your calendar functions), in this order so that
dashboards keep working at every step:

1. Step 2 (`pip install --upgrade …`) and restart Superset. Leave `superset_config.py` as
   it is: the old calculated column still calls your former label functions.
2. In each dataset that has a calculated column `POSITION_LABEL`: **Edit dataset →
   Columns**, delete it, then **Sync columns from source**. The built-in `POSITION_LABEL`
   appears under the same name, so charts and native filters that use it keep working
   (checked in the lab with the label table chart and the native filter).
3. Once no dataset, chart or saved query uses them any more, delete from
   `superset_config.py` your former calendar functions, the `osagg.register_function(...)`
   lines, their `JINJA_CONTEXT_ADDONS` entries and the `SQLGLOT_DIALECTS_EXTENSIONS` line.
   Restart Superset.

Later upgrades: step 2 with the new file, then restart Superset.

Rollback: delete the database connection (and its datasets / charts) in Superset, then

```bash
$PY -m pip uninstall -y osagg        # duckdb, opensearch-py and Events may stay
```

and restart Superset. osagg never writes to OpenSearch.

## 10. AI agent through Superset's MCP service (optional)

Superset **6.1** includes an MCP server (`superset mcp run`, not in 6.0 or 5.0). It has
no chat window inside Superset (section 13 adds one, supagent): an AI agent connects to it and uses 24 tools, including
`list_datasets`, `get_dataset_info`, `execute_sql`, `list_dashboards`, `get_chart_data`,
`generate_chart`, `generate_dashboard` and `generate_explore_link`. Tested in a lab with
a local model (Qwen3.6-35B-A3B served by llama.cpp's OpenAI-compatible API).

1. Install the MCP extra in Superset's virtualenv:
   `$PY -m pip install "fastmcp>=3.1.0,<4.0"`. Offline, use the MCP wheelhouse
   (Python 3.11, glibc ≥ 2.17):
   `$PY -m pip install --no-index --find-links ./wheelhouse-mcp "fastmcp>=3.1.0,<4.0"`.
   It only adds packages; `pip check` stays clean.
2. `superset_config.py` (settings only, no functions):

   ```python
   MCP_DEV_USERNAME = "mcp_agent"        # Superset user the agent acts as (required)
   SUPERSET_WEBSERVER_ADDRESS = "https://<superset>"          # links in answers
   WEBDRIVER_BASEURL_USER_FRIENDLY = "https://<superset>/"
   MCP_TOOL_SEARCH_CONFIG = {"enabled": False}   # local models: plain tool list
   ```

   Create `mcp_agent` in Superset with only the roles the agent needs (e.g. Gamma plus
   access to the OpenSearch database and its datasets). The agent sees and changes
   exactly what that user can (RBAC is on by default).
3. Run it next to Superset, on localhost:
   `superset mcp run --host 127.0.0.1 --port 5008` (systemd unit:
   `systemd/superset-mcp.service` of the bundle). The endpoint is
   `http://127.0.0.1:5008/mcp` (streamable HTTP). Before exposing it to other hosts,
   turn on JWT auth (`MCP_AUTH_ENABLED = True`, `MCP_JWT_ALGORITHM = "HS256"`,
   `MCP_JWT_SECRET = "<secret>"`) or put it behind an authenticating proxy.
4. Agent with the local LLM: `ai/superset_agent.py` of the bundle (or `tools/` in the
   repository); it needs only the Superset virtualenv. Settings in `ai/agent.env`
   (LLM URL, the two MCP endpoints, the agent's Superset account; chmod 600):

   ```bash
   set -a; . /opt/superset/ai/agent.env; set +a
   $PY /opt/superset/ai/superset_agent.py \
     "Top 5 applications by failed jobs on D-1, with the failure rate"
   ```

   Without a question it is interactive. With the tools service (10b) in `MCP_URL` it
   also explains the data, extracts to Excel, sends chart images and e-mails, and
   schedules e-mail reports (section 11).

   The model reads the dataset columns, writes the SQL (pushed down by osagg) and
   answers with a table and the SQL it ran. Each question took about 2 minutes with the lab's
   local LLM.
   Any other MCP client (Claude Desktop, Open WebUI…) can use the same
   endpoint. `AGENT_THINKING=0` (default) turns off the model's reasoning tokens: on the lab's LLM
   a reasoning step generated 150–600 tokens at about 12 tokens/s.
5. Charts built by an agent. Superset 6.1's MCP service (`generate_chart`) is the weak
   spot with a local model, whatever the SQL engine:
   * An invalid chart config is answered "An error occurred"; the reason (e.g. "Unknown
     field 'sort_desc' — did you mean 'sort_by'?") only goes to the MCP service's log.
     The model retries blindly, and every accepted retry saves one more chart.
   * `COUNT(*)` is the dataset's saved metric `count` (`{"name": "count",
     "saved_metric": true}`); `COUNT` of a column named `count` saves a broken chart.
   * No time comparison, no rolling time range (only fixed-value filters), no native
     filters on dashboards. Compare position dates with `POSITION_TIME` and
     `POSITION_LABEL` instead of a time comparison.
   * Charts are saved without a query context: they display, but CSV / text reports and
     the chart-data API need one (`create_report` of the tools service adds it for CSV / TEXT
     reports).
   * The preview of a saved chart ignores its filters and time grain.

   Give the dataset saved metrics for what a chart cannot write (ratios, percentiles,
   conditional counts): `ai/add_saved_metrics.py <dataset id>` adds `failed_jobs`,
   `failure_rate_pct`, `p95_duration_s` and `median_duration_s` to a jobs dataset; the
   agent is told to use saved metrics.

   `tools/superset_agent.py` guards the chart tools: it validates each config with
   Superset's own schema and against the dataset's columns and saved metrics and returns
   the exact error to the model; refuses the aggregates Superset cannot run (MEDIAN,
   PERCENTILE, STDDEV, VAR) and fields it would drop; previews the chart without saving
   it first (a failing or empty chart is not saved); turns a second save of the same
   chart name into `update_chart`; saves table sorts as `["column", ascending]` (another
   form breaks the whole dashboard) and date limits as the chart's time range (dashboards
   drop plain filters on the time column, `SUPERSET_URL` / `SUPERSET_USER` /
   `SUPERSET_PASSWORD` needed). An empty LLM answer is reported as an error.
6. The LLM server. With llama.cpp on a GPU whose memory several models share (an iGPU
   for example), a big model can degrade silently into one empty token per answer when
   another model grows after it started: restart `llama-server` and start it last.

## 10b. Tools for the agent: data dictionary, Excel extracts, images, e-mails

`superset_tools_mcp.py` is a second MCP service, next to Superset's, for what Superset's
MCP service does not do. It runs in Superset's app context (same `superset_config.py`,
database connections, SMTP and screenshot settings) and acts as one Superset user
(`TOOLS_USER`, e.g. `mcp_agent`) with that user's permissions. Settings: `ai/tools.env`.
Service: `systemd/superset-tools.service`, on `127.0.0.1:5009` only.

| Tool | What it does |
|---|---|
| `describe_data` | what each field means, units, synonyms, typical values and ranges, the time range of the data, how the indices relate (join fields), the business glossary and the saved metrics; the agent calls it before writing SQL or a chart |
| `export_excel` | a SELECT to an .xlsx file in `EXPORT_DIR` (typed cells, frozen header, filters, a sheet with the query); optionally e-mailed |
| `chart_from_sql` | PNG bar or line chart drawn from the result of a SELECT (no Superset chart needed; headless Chromium) |
| `chart_image` | PNG of a saved chart or of an explore link, taken by Superset's report browser |
| `send_email` | an e-mail now: text, a data table, chart images, an Excel attachment (at most one per request) |
| `list_reports`, `create_report` | recurring e-mail reports of a dashboard or a chart (by id or title) |

**Data dictionary.** Write `ai/catalog.yaml`: per index (name as in Superset) its
description, time field, relationships to other indices (join fields) and, per field, a
description, unit and synonyms; plus a glossary of business terms (failed job, D-1...).
Values, ranges, document counts and time ranges are profiled automatically once a day on
a sample (one small request per index). `superset_tools_mcp.py push-descriptions` copies
the field descriptions into the Superset dataset columns (`--labels` also sets the column
labels, which relabels existing charts).

**Excel extracts.** One SELECT only; the user's access is checked; at most
`EXPORT_MAX_ROWS` rows (the file says when rows were cut). Only here may a row list join
the big index with small ones (each at most `join_max_keys` matching documents), e.g. the
failed jobs with their application's team: the small index is read whole and its keys
filter the big one (osagg option `lookup_joins`, off everywhere else). Measured in the lab:
8,390 rows of a jobs x applications extract in 3.5 s. Files older than `EXPORT_KEEP_DAYS`
are deleted; serve `EXPORT_DIR` (`EXPORT_BASE_URL`) or let the agent e-mail the file.

**E-mails.** `send_email` and `export_excel(email_to=...)` use Superset's SMTP settings
(section 11). `EMAIL_ALLOWED_DOMAINS` restricts the recipients; files over
`EMAIL_MAX_ATTACH_MB` are linked, not attached. Files made by the tools are named
`<name>-<id>.<ext>`; the agent passes the 6-character id (images, attachments) from one
tool to the next.

Tested with a local LLM (lab scenarios, each with a deterministic check):
what the data means (dictionary), a failing-jobs report in the chat (table and text
chart), JSON matching the SQL result, an Excel extract of the failed jobs with their
application's team and tier (row count = `COUNT(*)` through osagg), and one e-mail with
summary, table, bar chart image and the Excel file attached.

## 11. Scheduled e-mail reports with chart screenshots

Superset's **Alerts & Reports** sends a dashboard or a chart on a schedule: a PNG
screenshot in the e-mail body, a PDF, a CSV or a text table. It runs in the Celery worker
and beat, which need an SMTP server and, for PNG / PDF, a headless Chromium with its
chromedriver on the worker host. osagg needs nothing more: the charts query OpenSearch
through it as usual. Tested in the lab (Superset 6.1, Chromium 149 on a Raspberry Pi).

1. `superset_config.py` (settings only):

   ```python
   from superset.tasks.types import FixedExecutor

   FEATURE_FLAGS = {"ALERT_REPORTS": True}
   SMTP_HOST = "<company SMTP relay>"
   SMTP_PORT = 25
   SMTP_STARTTLS = True
   SMTP_SSL = False
   SMTP_USER = ""
   SMTP_PASSWORD = ""
   SMTP_MAIL_FROM = "superset@<company>"
   EMAIL_REPORTS_SUBJECT_PREFIX = "[Superset] "
   ALERT_REPORTS_EXECUTORS = [FixedExecutor("reports_bot")]   # user whose view is captured
   WEBDRIVER_TYPE = "chrome"
   WEBDRIVER_BASEURL = "http://127.0.0.1:8088/"                # how the worker reaches Superset
   WEBDRIVER_BASEURL_USER_FRIENDLY = "https://<superset>/"    # links in the e-mail
   WEBDRIVER_OPTION_ARGS = ["--headless=new", "--disable-gpu", "--disable-dev-shm-usage",
                            "--hide-scrollbars", "--no-sandbox"]
   WEBDRIVER_WINDOW = {"dashboard": (1600, 3600), "slice": (1600, 1000), "pixel_density": 1}
   ```

   `reports_bot` is a Superset user that can see the reported dashboards (Gamma plus the
   datasets). The default window (1600 × 2000) cuts long dashboards: raise the height.
2. Chromium and chromedriver on every worker host:
   * With root: `sudo apt install chromium chromium-driver` (Debian / Ubuntu), or the
     distribution's `chromium` with the matching `chromedriver`.
   * Without root: `apt-get download` the `chromium`, `chromium-common`,
     `chromium-driver` packages and the libraries they need, unpack each with
     `dpkg-deb -x <file>.deb ~/opt/chromium-root`, and write two wrappers
     `~/opt/chromium/chromium` and `~/opt/chromium/chromedriver` that set `LD_LIBRARY_PATH`
     (the unpacked library folders) and `FONTCONFIG_FILE`, then `exec` the unpacked binary,
     e.g. for Chromium:

     ```sh
     #!/bin/sh
     R=$HOME/opt/chromium-root
     export LD_LIBRARY_PATH="$R/usr/lib/<arch>-linux-gnu:$R/lib/<arch>-linux-gnu:$R/usr/lib/chromium"
     export FONTCONFIG_FILE="$HOME/opt/chromium/fonts.conf"
     exec "$R/usr/lib/chromium/chromium" --no-sandbox "$@"
     ```

     Put `~/opt/chromium` first in the worker's `PATH` (a systemd drop-in:
     `[Service]` / `Environment=PATH=%h/opt/chromium:/usr/local/bin:/usr/bin:/bin`)
     and point Superset at the driver:
     `WEBDRIVER_CONFIGURATION = {"options": {"capabilities": {}, "preferences": {}}, "service": {"executable_path": "/home/<user>/opt/chromium/chromedriver", "log_output": "/dev/null", "service_args": [], "port": 0, "env": {}}}`.
     Do not set `binary_location` there: Superset 6.1 passes it to `WebDriver()`, which
     fails. Check the libraries with `~/opt/chromium/chromium --headless=new --dump-dom about:blank`
     (a missing `.so` is named in the error).
3. Restart the worker and beat. Create the report: **Alerts & Reports → + Report**,
   dashboard or chart, format PNG, schedule (cron and time zone), recipients.
   An MCP agent can also create them (`create_report` of the tools service, section 10b).
4. Test without waiting for the schedule: set it to `* * * * *`, open the report's
   **Log** (Success, or the error), then set the schedule back.

| Symptom in the report log | Fix |
|---|---|
| `cannot find Chrome binary`, `chrome not reachable` | Chromium not on the worker's `PATH`, or a library missing (run the wrapper by hand) |
| `DevToolsActivePort file doesn't exist` | keep `--no-sandbox` and `--disable-dev-shm-usage` |
| screenshot of the login page | `WEBDRIVER_BASEURL` must be reachable from the worker; the executor user must exist and see the dashboard |
| dashboard cut at the bottom | raise the `dashboard` height of `WEBDRIVER_WINDOW` |

For tests without an SMTP relay, any local SMTP sink will do (e.g.
`python -m aiosmtpd -n -l 127.0.0.1:8025`, with `SMTP_HOST = "127.0.0.1"`, `SMTP_PORT = 8025`).
In production, use the company relay.

## 12. Prometheus / Grafana Mimir metrics (promagg)

promagg is the same idea as osagg for metrics: Superset sends SQL, promagg turns it into
PromQL, Mimir aggregates (billions of samples) and returns the small result. One SQL table
per metric; columns `ts`, one per label, `value`, and for counters `rate` / `increase`.
Full SQL reference: the promagg README (in the bundle, `promagg-README.md`).

**Install.** `./install.sh $PY` (step A1) installs promagg with osagg; nothing in
`superset_config.py`; restart the web server, workers and beat. Dependencies are already
in Superset (sqlglot, duckdb, pyarrow, urllib3, SQLAlchemy).

**Access in Mimir.** promagg only calls the read API under `/prometheus/api/v1/`
(`query`, `query_range`, `series`, `labels`, `label/<name>/values`, `metadata`,
`status/buildinfo`, and `alerts` / `rules` for the agent). Give Superset a read-only
account on the query-frontend (or the gateway in front of it) and the tenant id.

```bash
# check from the Superset host (tenant, TLS, credentials as in production)
curl -s -H 'X-Scope-OrgID: <tenant>' -u '<user>:<password>' \
  'https://<mimir>/prometheus/api/v1/label/__name__/values' | head -c 300
```

**Connection.** Settings > Database Connections > + Database > "Prometheus / Mimir
(PromQL pushdown)" > SQLALCHEMY URI:

```
promagg+https://<user>:<password>@<mimir-host>:443/prometheus?tenant=<tenant>&timezone=Europe/Paris
promagg+https://bearer:<token>@<mimir-host>:443/prometheus?tenant=<tenant>&timezone=Europe/Paris
promagg://<mimir-query-frontend>:8080/prometheus?tenant=<tenant>&timezone=Europe/Paris
```

**Several tenants in one connection.** With tenant federation enabled in Mimir
(`-tenant-federation.enabled=true` on the query-frontends and queriers), put the tenants in
the URI separated by `|` (or `%7C`): `...?tenant=team-a|team-b&timezone=Europe/Paris`. Every
table then has a column `__tenant_id__` (added by Mimir): charts can split or filter by
tenant, sums add up across tenants, and `GROUP BY __tenant_id__, node` keeps a server name
used in two tenants apart. `check_health` keeps the tenants apart by itself and names the
tenant of each breach; `list_alerts` asks the ruler of each tenant. Without federation,
Mimir refuses the connection ("too many tenant IDs") and the error says how to enable it.
Measured in the lab (report, "Many tenants"): the cost follows the data read, about the sum over
the tenants (a day at 5-minute steps: 0.2 s for one tenant, 2.2 s for 50 small ones, 4.9 s with
the 2.9-billion-sample tenant added; cached 0.1 s); 400 tenant IDs in one header (4 KB) work.
Mimir settings: `-tenant-federation.max-concurrent` (per-tenant queries at a time, default 16),
`-tenant-federation.max-tenants` (no limit by default). A gateway must accept the header: nginx
takes 8 KB per header line by default (`large_client_header_buffers`), about 700 tenant IDs of
10 characters.

**Through a gateway (TLS, passwords, tokens, client certificates).** The same URI forms work
through nginx or another gateway in front of Mimir (tested: `tests/test_gateway.py`, and the
Superset of the lab through each form):

```
promagg+https://superset:<password>@<gateway>:443/prometheus?tenant=team-a|team-b&ca_certs=/etc/superset/gw-ca.pem
promagg+https://bearer:<token>@<gateway>:443/prometheus?tenant=team-a|team-b&ca_certs=/etc/superset/gw-ca.pem
promagg+https://<gateway>:443/prometheus?tenant=team-a|team-b&client_cert=/etc/superset/client.pem&client_key=/etc/superset/client.key&ca_certs=/etc/superset/gw-ca.pem
promagg+https://superset:<password>@<gateway>:443/prometheus      (the gateway sets the account's tenants)
```

The certificate and key files must be readable by the web server and the Celery workers. The
gateway must pass `X-Scope-OrgID` unchanged and allow the account every tenant of the list;
alerts and rules are asked tenant by tenant (the ruler API takes one), which a gateway that
replaces the client's tenant header cannot serve. A pass-through gateway with a tenant list per
account, in nginx (the check runs after authentication: a wrong password is 401, a tenant
outside the list 403):

```nginx
map "$remote_user:$http_x_scope_orgid" $tenants_ok {
    default 0;
    "~^superset:(team-a|team-b|team-c)(\|(team-a|team-b|team-c))*$" 1;
}
map $tenants_ok $read_to { 1 mimir; default denied; }
upstream mimir  { server mimir-query-frontend:8080; keepalive 16; }
upstream denied { server 127.0.0.1:9448; }   # server { listen 127.0.0.1:9448; return 403 "tenants not allowed\n"; }
server {
    listen 443 ssl;
    ssl_certificate /etc/nginx/gw.pem;  ssl_certificate_key /etc/nginx/gw.key;
    auth_basic "mimir";  auth_basic_user_file /etc/nginx/mimir.htpasswd;
    location /prometheus/config/ { return 403; }              # read-only for Superset
    location /prometheus/ {
        proxy_pass http://$read_to;
        proxy_set_header X-Scope-OrgID $http_x_scope_orgid;
        proxy_http_version 1.1;  proxy_set_header Connection "";
        proxy_read_timeout 360s;                             # above Mimir's -querier.timeout
    }
}
```

"Test connection" and SQL Lab show why a connection is refused: certificate not trusted
(`ca_certs`), wrong credentials (401), tenants not allowed (403, with the list), client
certificate missing or refused, file not found.

Useful options (all in the URI): `schema_window=7d` (where metric names, labels and
dashboard filter values are looked up), `default_range=24h` (queries without a time
filter), `tables=node_*,batch_*` (metrics to show), `counters=node_vmstat_*` (counters
without a `_total` suffix), `verify_certs`, `ca_certs`, `request_timeout=120`,
`concurrency=4`. "Test connection" makes a real call (URL, credentials, tenant).

**Datasets and saved metrics from the catalog.** Declare the metrics in
`ai/catalog.yaml` (section `metrics`: description, unit, synonyms, label meanings,
example SQL and `saved_metrics`), then:

```bash
set -a; . /opt/superset/ai/tools.env; set +a
$PY /opt/superset/ai/superset_tools_mcp.py push-metrics
# dataset 6 node_cpu_seconds_total (created): cpu_busy_pct, cpu_busy_cores
# dataset 12 batch_jobs_total (created): jobs, failed_jobs, failure_rate_pct ...
```

Saved metrics are what charts built by the agent (Superset's MCP service) can use:
`cpu_busy_pct = 100 * SUM(rate) FILTER (WHERE mode <> 'idle') / SUM(rate)`,
`failure_rate_pct`, `p95_duration_s = HISTOGRAM_QUANTILE(0.95, SUM(RATE(value)))`...
The same catalog tells the agent how metric labels match index fields
(`label_relationships`: `node` = `"NODE"` of the jobs index) and defines the health
checks.

**Charts.** Time column `ts`; choose a time grain (minute, hour, day). "Original value"
gives automatic buckets of about 500 points over the chart's range (samples are not
grouped one by one). Label filters, series limits, dashboard native filters (values read
from the label index), time comparison, Big Number, tables and alerts work as with any
database. For arithmetic between metrics
(memory used %), a virtual dataset with `promql('...')` gives one table.

**Agent tools** (tools service, same catalog): `promql_query` (any PromQL, summarized
series), `check_health` (the `checks` of the catalog over a window: CPU saturation,
memory pressure, OOM kills, disk full, servers down, queue backlog, HTTP 5xx, latency,
licences; returns each breach with server / application, from, to, worst value) and
`list_alerts` (alerts firing now and alerting rules of the Mimir ruler). `promql_query`
and `check_health` need database access in Superset for `TOOLS_USER`; ranges are capped
at `PROMQL_MAX_RANGE_DAYS` (31). `fix_chart_time_range(chart_id)`: for agents that build charts with
Superset's MCP service directly: date filters on the time column (`ts >= ...`, `ts < ...`) are
saved as plain filters, which the chart page applies but dashboards ignore; the tool moves them
into the chart's time range (the agent of the bundle does it by itself). It edits the chart
with the tools service's Superset account (`SUPERSET_USER` of `tools.env`): that account must be
allowed to edit charts saved by the account the agent uses with Superset's MCP service (an owner
of the chart, or an Admin).

**Performance.** Measured in the lab on 2.9 billion samples (16,726 series, 15 s, 30 days,
Mimir 3.2.1 monolithic with memcached caches on S3): see the report. The work is done by
Mimir; promagg sends one PromQL query per aggregate and run of buckets and receives groups
x buckets rows. Mimir's results cache keeps queries whose times are multiples of the step
(minute, hour, 5-minute buckets); local-time days are not cached by Mimir (Superset's
chart cache still is). For dashboards over months on many series, use recording rules:
their series are tables like any metric.

**Experimental PromQL functions.** `MAD_OVER_TIME` (mad_over_time) is an experimental PromQL
function: enable it for the tenant with `-query-frontend.enabled-promql-experimental-functions`
(`mad_over_time` or `all`), otherwise Mimir refuses the query with that reason.

**Limits (refused with the reason):** quantiles of raw samples, conditions on sample values
inside aggregates (use HAVING or `promql('m > 90')`), joins of metric tables row by row (aggregate each in a subquery, or `promql()`), rate-like functions summed
across several buckets of one group (hour-of-day). Native histograms: use `promql()`.

## 13. The agent inside Superset (supagent)

supagent puts the agent into Superset: a chat page (menu *AI agent*), a data dictionary
learned every day and kept in Superset's database, and admin settings with the LLM
authentication (none, a fixed token, or the company middleware's short-lived tokens). It
replaces the separate agent and tools services of sections 10 and 10b for users who work in Superset. They
keep working for other agents: `superset supagent mcp` serves the same tools over MCP.

```bash
$PY -m pip install supagent-0.5.1-py3-none-any.whl     # or the latest supagent release (see its guide)
# superset_config.py, one line:
#   from supagent import init_app as FLASK_APP_MUTATOR
superset supagent init                                  # tables supagent_*, role "AI Agent"
superset supagent grant <user>...
superset supagent settings --set llm.base_url=... --set llm.auth=middleware ...   # see the guide
superset supagent import-catalog catalog.yaml
superset supagent learn
# restart the web server, the Celery workers and beat
```

The agent acts with the permissions of the user who asks: it only reads the databases,
datasets, charts and dashboards that user may use in Superset, and the dictionary shows only
those databases. Everything is in the supagent guide (`supagent-guide.html` / `.pdf` in the
supagent bundle).
