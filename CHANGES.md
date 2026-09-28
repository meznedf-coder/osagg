# Changes

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
