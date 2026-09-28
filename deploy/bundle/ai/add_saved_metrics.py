"""Add saved metrics to a Superset dataset, for charts that an AI agent builds.

Charts built through Superset's MCP service take a column with an aggregate (SUM,
COUNT, AVG, MIN, MAX, COUNT_DISTINCT) or a saved metric: ratios, percentiles and
conditional counts need saved metrics. These are for a batch-jobs dataset (edit METRICS for other
datasets). Existing metrics are kept; a metric with the same name is not replaced.

    set -a; . agent.env; set +a
    $PY add_saved_metrics.py <dataset id>
"""

from __future__ import annotations

import os
import sys

import httpx

METRICS = [
    ("failed_jobs", "failed jobs", "SUM(CASE WHEN \"STATUS_INFO\" = 'FAILED' THEN 1 ELSE 0 END)",
     "Number of jobs with STATUS_INFO = 'FAILED'"),
    ("failure_rate_pct", "failure rate %",
     "100.0 * SUM(CASE WHEN \"STATUS_INFO\" = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*)",
     "Failed jobs * 100 / all jobs"),
    ("p95_duration_s", "p95 duration (s)", "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY \"JOB_DURATION_d\")",
     "95th percentile of JOB_DURATION_d, in seconds"),
    ("median_duration_s", "median duration (s)", "MEDIAN(\"JOB_DURATION_d\")",
     "Median of JOB_DURATION_d, in seconds"),
]


def main() -> None:
    ds_id = int(sys.argv[1])
    base = os.environ.get("SUPERSET_URL", "http://127.0.0.1:8088").rstrip("/")
    c = httpx.Client(base_url=base, timeout=60)
    r = c.post("/api/v1/security/login", json={"username": os.environ["SUPERSET_USER"],
               "password": os.environ["SUPERSET_PASSWORD"], "provider": "db", "refresh": True})
    r.raise_for_status()
    c.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
    c.headers["X-CSRFToken"] = c.get("/api/v1/security/csrf_token/").json()["result"]
    c.headers["Referer"] = base
    ds = c.get(f"/api/v1/dataset/{ds_id}").json()["result"]
    keep = ("id", "metric_name", "expression", "verbose_name", "description", "d3format", "metric_type")
    metrics = [{k: m[k] for k in keep if m.get(k) is not None} for m in ds["metrics"]]
    have = {m["metric_name"] for m in metrics}
    added = [name for name, *_ in METRICS if name not in have]
    metrics += [{"metric_name": n, "verbose_name": v, "expression": e, "description": d}
                for n, v, e, d in METRICS if n not in have]
    c.put(f"/api/v1/dataset/{ds_id}?override_columns=false", json={"metrics": metrics}).raise_for_status()
    print(f"dataset {ds_id}: added {', '.join(added) or 'nothing (already there)'}")


if __name__ == "__main__":
    main()
