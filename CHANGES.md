# Changes

## 0.2.13 — 3 Oct 2026

**A query of many aggregates plans in linear time.** The planner compared each aggregate of a query with every
aggregate before it, writing both as SQL each time: thirty aggregates in one grouped query (a tool that reads every
measure of a table at once, each with its own `FILTER (WHERE ...)`) cost about 760 such writings and 100 ms of
planning, more than OpenSearch took to answer. Each aggregate's key is now computed once per use, and no copy of
the expression is made when there is no table qualifier to remove: the same query plans in a third of the time,
and a call of a hundred such queries (supagent 0.9 `compare_groups`) takes 4 to 6 seconds instead of 9 to 10. The
plans and the results are unchanged.

## 0.2.12 — 2 Oct 2026

**A fresh Superset 6.1.0 from PyPI needs three pins.** `pip install apache-superset==6.1.0` today pulls Flask-Caching
2.5 (Superset 6.1.0 does not start with it), Flask-Limiter 4, which no longer brings `rich` (`superset db upgrade`
fails), and no `cachetools` (the login page fails). DEPLOY.md, section 2, gives the line
`pip install "apache-superset==6.1.0" "flask-caching==2.3.1" "flask-limiter<4" cachetools` (tested on an empty
PostgreSQL database); `install.sh` warns about each one it finds missing. No code change: osagg 0.2.11 and 0.2.12
read the same.

## 0.2.11 — 2 Oct 2026

**EXPLAIN names the indices a table reads, and the ones of the same family it misses.** Each table of the query
gets a line with the indices it reads (the first and the last ones) and their documents, and a WARNING when indices
of the same name (apart from `-`, `_` and `.`) exist that it does not read, with their documents and the pattern
that reads them all. The production case of 2 October: a table on an alias put on the monthly indices with a
wildcard, which did not take the index created on 1 October, so the alias had 2 documents of that day where
Discover, on the index pattern, had 215. The warning also says to put the alias in the index template; see
DEPLOY.md, *An alias for every new index* (checked on OpenSearch 3.8: a legacy template is ignored when a
composable one matches). Direct transport only; EXPLAIN only (a query's own rows are unchanged).

## 0.2.10 — 1 Oct 2026

**`POSITION_LABEL = 'D'` is one position date, today's.** It also asked for the next seven days and every
later date (`terms` with 8 dates and an open range), because the production rule (D-x = the weekdays from the
date to yesterday) calls every date after today "D". A date after today is now `D+1`, `D+2`... (calendar days),
so `D` is today's position date alone (and, on a Monday, the weekend before it, which no weekday follows).

**Time bounds read like yours.** A range on a date is written as ISO instants with their offset in the
connection's zone (`"gte": "2026-09-30T21:00:58.251+02:00", "lt": "2026-10-01T21:00:58.253+02:00",
"format": "strict_date_optional_time"`; calendar days in UTC): the same instants as the epoch milliseconds
before, readable in EXPLAIN.

**EXPLAIN fits on the screen.** One row per OpenSearch request (compact JSON) instead of one row per line of
indented JSON: about 10 rows for a count with three conditions instead of 45. `EXPLAIN VERBOSE` keeps the
indented form.

## 0.2.9 — 1 Oct 2026

**Faster metadata (0.2.8 could take tens of seconds on big indices).** 0.2.8 looked for text values longer than
their keyword by filtering the documents that have the text but not the keyword: every document of the
pattern was read for each text field, at every load of a table's metadata (every 5 minutes in each Superset
process). It now compares two counts per field (documents with the text, with the keyword), which OpenSearch
answers from its index statistics: 20 ms instead of 223 ms per field on a lab index of 10 million documents,
and the cost no longer grows with the number of documents. All the counts go in one request.

**EXPLAIN says what to check first** when a count differs from Discover's: the version of osagg, the transport
and the time zone, then, for each index pattern, how each field the query names is read there (keyword, text
with its exact sub-field, a date's format and whether it holds calendar days, a field mapped differently in
each group of indices, values longer than the keyword).

## 0.2.8 — 1 Oct 2026

Counts that were silently wrong, found while reviewing a production count far below what Discover
showed for the same filters. Each was reproduced on a lab cluster with the truth computed from the
generated documents, and each has a test that fails on 0.2.7.

**A field the indices of a pattern map differently.** If one index mapped a field as text with a
`.keyword` sub-field and another mapped it as keyword, osagg filtered on `<field>.keyword` everywhere.
The second index's documents then matched nothing: 10 counted instead of 1,210. Every condition on such
a field is now built per group of indices, each with its own exact field and type. GROUP BY reads each
index's exact field. The business-date label (`POSITION_LABEL`) computed from such a field also counted
only the first group of indices: 5 instead of 155.

**Business dates mapped as dates** (`yyyyMMdd`, `basic_date`, ...).
- `= 20261001` was read as epoch milliseconds (1970).
- `'2026-10-01'` was read as midnight in the connection's zone, which in Paris is 22:00 UTC the day
  before.
- Both counted nothing, and ranges gained or lost a day.

Such a field is now a calendar day in UTC, and its literals are read in the field's own format.
Default-format date fields whose values are all at midnight UTC are detected once per table and read the
same way (`date_probe=false` turns this off). A literal a field cannot hold now raises a clear error
instead of matching nothing.

**COUNT(DISTINCT) is exact.** It was a cardinality sketch: above 3,000 values it returned an estimate
without saying so (56,171 for 55,653). The sketch is still used while it is exact. When it reaches its
threshold, the query is counted again with the values as keys. `count_distinct=approx` keeps the
estimate.

**Text values longer than their keyword.** OpenSearch's dynamic mapping gives text fields a keyword
sub-field with `ignore_above: 256`, and a longer value (an error description, a stack trace) is not in
that keyword. Such values were grouped under NULL, and `=`, `<>`, `LIKE`, `IN`, `COUNT(col)`,
`COUNT(DISTINCT)` and `IS NULL` missed them.
- osagg now counts these documents once per table, and where needed reads the field from the documents.
- `=`, `IN`, `IS NULL` and `COUNT(col)` find a long value at once, even one that arrives after the table
  was read.
- GROUP BY and `LIKE` notice the first long value of a field when the metadata is refreshed, within 5
  minutes.

**An answer from part of the shards is an error.** When a shard fails (a node gone, a rejected or
out-of-memory shard search, a script error in some indices), OpenSearch still answers HTTP 200 with what
the other shards hold. osagg returned those partial counts. Searches now ask for all shards
(`allow_partial_search_results=false`), and any answer with failed shards or a timeout raises an error
naming the shards and the reason; busy shards are retried first. This applies through Trino as well.
In the lab, OpenSearch counted 1,200 of 1,210 documents with one shard failed, and 0.2.7 returned 1,200.
Points in time (deep reads beyond 10,000 rows) are created on every shard or not at all
(`allow_partial_pit_creation=false`): one created on part of the shards would answer every page with no
failed shard while documents are missing.

**Bundle.** Carries promagg 0.2.2 (was 0.2.1): exact increases per time bucket with Mimir's anchored
ranges, and a backend warning or a native histogram sample is an error instead of a silently incomplete
result (see `promagg-README.md`).

**Upgrading.** A chart whose answers were silently partial (for example an index of a pattern whose
mapping makes a shard fail) now shows an error naming the shards and the reason instead of a smaller
number. That error is the place to fix the mapping or the query.

## 0.2.7 — 30 Sep 2026

**Top-N with LIMIT 0.** A query ordering its groups by an aggregate with `LIMIT 0` (what
Superset runs to read the columns of a virtual dataset, for example `... GROUP BY
"APPLICATION" ORDER BY "FAILED_JOBS" DESC`) asked OpenSearch for a terms aggregation of size
0, which it refuses (`[terms] failed to parse field [size]`): saving such a dataset failed.
It now asks for one bucket and returns no rows.

**Bundle.** Carries promagg 0.2.1 (was 0.1.0). Its tools service and agent are unchanged:
every tool they offer (describe_data, export_excel, chart_image, chart_from_sql, send_email,
promql_query, check_health, list_alerts, list_reports, create_report, fix_chart_time_range)
is also in supagent, the agent inside Superset, which serves them over MCP with
`superset supagent mcp` (DEPLOY.md section 13).

## Tools and bundle — 27 Sep 2026 (osagg 0.2.6 unchanged, promagg 0.1.0 added)

**Prometheus / Mimir metrics.** The bundle adds promagg (see `promagg-README.md` and
DEPLOY.md section 12): Superset charts, dashboards, SQL Lab and alerts on Mimir, with the
aggregation done by Mimir.

**Agent tools for metrics** (`superset_tools_mcp.py`):

* `describe_data` also describes the metrics: one table per metric, labels and their values,
  how labels match index fields (`node` = `"NODE"`), example SQL, saved metrics, health checks.
* `promql_query` — any PromQL on the metrics database, summarized per series.
* `check_health` — the health checks of `catalog.yaml` (CPU saturation, memory pressure,
  OOM kills, disk full, servers down, queue backlog, HTTP 5xx, latency, licences, job
  failure rate) over a time window: each breach with server / application, from, to and
  worst value.
* `list_alerts` — alerts firing now and alerting rules of the Mimir ruler.
* `push-metrics` command — `catalog.yaml` metrics -> Superset datasets and saved metrics.
* `chart_from_sql`, `export_excel` and `send_email` (SQL attachments) pick the database from
  the SQL (metric tables -> the metrics database), so the model does not have to name it.
* `describe_data` answers names the model guessed (`cpu_usage`) with the tables of that topic;
  `promql_query` explains empty results (unknown metric, label values, time range).

* Metrics databases over several Mimir tenants (`tenant=a|b`): `check_health` keeps
  `__tenant_id__` in its groupings (the same server name in two tenants stays apart, each
  breach names its tenant), `list_alerts` asks each tenant's ruler, `describe_data` tells
  the agent about the tenant column.
* With several metrics databases, the tools default to the catalog's (`metrics.database`), not
  the first one found.
* Health checks keep `__tenant_id__` in their groupings whatever the connection says (tenants
  set by a gateway too).
* `fix_chart_time_range` — for agents that save charts through Superset's MCP service: the
  chart's date filters become its time range (dashboards ignore plain filters on the time
  column).
* DEPLOY.md: gateway set-ups (TLS, password, token, client certificate, tenants set by the
  gateway) with an nginx example.

**Agent.** Knows the metric tables and how to investigate (failed jobs by server and
hour on the jobs index, then check_health on those servers and hours); charts on metric
datasets get an hourly grain when none is given; when the question asks for a table (or a
chart) and the model gave only the other, the answer is completed from the same SQL result;
`AGENT_NOW` pins "today" for tests on old data.

## 0.2.6 — 26 Sep 2026

**Row lists over a join, for extracts only.** With the connection option `lookup_joins`
(off by default; the Excel tool sets it), a row list may join one big index with small
ones (each at most `join_max_keys` matching documents, read whole): their join keys are
pushed into the big index as `terms` filters, a LIMIT is required, and ORDER BY / LIMIT go
into the big index when that is exact. Charts, SQL Lab and the agent's queries still refuse
row joins.

**Tools for AI agents** (`superset_tools_mcp.py`, a localhost MCP service next to
Superset's own, in Superset's app context, acting as one Superset user):

* `describe_data` — data dictionary: what each field means, units, synonyms, typical
  values and ranges (profiled daily on a sample), time range of the data, relationships
  between indices, business glossary, saved metrics. Written in `catalog.yaml`;
  `push-descriptions` copies the descriptions into the Superset dataset columns, so the UI
  and any MCP client see them.
* `export_excel` — a SELECT to an .xlsx on the server (typed cells, frozen header, filters,
  a sheet with the query), up to `EXPORT_MAX_ROWS`, optionally e-mailed; SELECT only,
  access checked as the user, retention of the files.
* `chart_from_sql` — PNG bar or line chart drawn from a SELECT's result, no Superset chart
  needed (the model only writes SQL).
* `chart_image` — PNG of a saved chart or an explore link, with Superset's report browser.
* `send_email` — an e-mail now: text, a data table, chart images, an Excel attachment,
  through Superset's SMTP settings (recipient domains can be restricted).
* `list_reports` / `create_report` — recurring e-mail reports, dashboard or chart by id or
  by title.

**Agent.** Reads the data dictionary before writing SQL or charts; answers report
requests in the chat with a summary, a table and a text chart, and gives JSON, an Excel
file, an image or an e-mail when asked; a call that already failed is not repeated, at
most one e-mail is sent per request, and an answer claiming an image or an attachment the
e-mail does not have is corrected.

## 0.2.5 — 26 Sep 2026

**Joins of indices, pushed down.** `JOIN`, `LEFT JOIN`, `RIGHT JOIN` (two indices) and
`USING`, on one or several equal fields, between two or more indices, in aggregating
queries (COUNT / SUM / AVG / MIN / MAX per group):

* each index is grouped in OpenSearch with its own filters; DuckDB joins the grouped
  rows and combines the aggregates exactly;
* huge indices: indices are aggregated smallest first and the join keys found so far are
  pushed to the next ones as a `terms` filter (a billion-document index joined with a
  filtered one is grouped only for the matching keys);
* every index must be bounded (new URI option `join_max_keys`, default 100000: matching
  documents, keys received, or distinct join keys from a sampled estimate), otherwise the
  query is refused before anything is read, with the reason;
* refused: row lists over a join, COUNT DISTINCT / percentiles across a join, conditions
  mixing indices, non-equality or FULL / CROSS joins;
* Superset virtual datasets over a join can be saved: their `LIMIT 0` / `WHERE false`
  column probes are answered from the mappings (the engine spec now probes with LIMIT 0).

**Busy clusters.** A search answered 429 / `circuit_breaking_exception` is retried after
0.5, 1.5 and 4 s; if a composite page is still refused, it is asked again in pieces four
times smaller (down to 500 groups). On the 2.5 GB lab node, dashboards with ten charts
no longer fail when they load at once.

## 0.2.4 — 26 Sep 2026

**Position date under other names and forms.** `label_source` picks the field per index:
`pattern:FIELD=format` items tried in order (`risk-*:COB_DATE=%Y-%m-%d,POSITION_DATE`),
keyword dates in any strftime format (`label_date_format`, default `%Y%m%d`), or `date`
fields. `label_time_source` picks the execution time the same way. Indices without such
a field simply have no `POSITION_LABEL` / `POSITION_TIME`.

**`POSITION_TIME` to the minute** (seconds dropped), so time buckets line up across
position dates.

**`LIMIT` without `ORDER BY` on sub-day buckets** stops early without splitting the
repeated autumn DST hour.

## Upgrading from 0.2.2

* Nothing to change in Superset: install the new wheel and restart (INSTALL.txt, step A).
* Differences you may notice: `POSITION_TIME` values have no seconds; a busy cluster makes
  a chart slower instead of failing at once; `COUNT(*)` over an empty join is 0; the
  column probe of virtual datasets is `LIMIT 0` (answered without reading).

## Tools (repository `tools/`, not in the wheel)

* `superset_agent.py` — AI agent for Superset 6.1's MCP service with a local
  OpenAI-compatible LLM. It guards the chart tools: configs are validated with Superset's
  own schema and against the dataset's columns and saved metrics, the exact error goes
  back to the model, a chart is previewed before it is saved (a failing or empty chart is
  not saved), a second save of the same name updates the chart, aggregates Superset cannot
  run are refused, table sorts and date limits are saved in the form Superset's dashboards
  read. `AGENT_THINKING=0` (default) turns reasoning tokens off. An empty LLM answer is an
  error.
* `superset_reports_mcp.py` — MCP tools `list_reports` / `create_report`: scheduled
  e-mail reports of dashboards and charts (PNG in the e-mail, PDF, CSV, text table); CSV
  and text reports store the chart's query context when the chart has none. (0.2.6: part
  of `superset_tools_mcp.py`.)
