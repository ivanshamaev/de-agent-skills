---
name: aiops-query-cost-analyzer
description: AIOps autonomous query cost analyzer — Trino system.runtime.query_history cost SQL (CPU time/peak memory/bytes scanned), ClickHouse system.query_log top expensive queries, Spark UI cost attribution by user/job/DAG, BigQuery INFORMATION_SCHEMA.JOBS cost analysis, LLM-driven query rewrite recommendations (missing partition filters/broadcast hints/aggregation pushdown), cost anomaly detection (user over daily budget), automated query tagging, chargeback report by team/project
---

# AIOps Query Cost Analyzer

## When to Use

- Identifying which queries consume the most resources on Trino/ClickHouse/Spark
- Automatically recommending query optimizations based on execution patterns
- Enforcing per-user or per-team query cost budgets
- Generating cost chargeback reports broken down by team/project/DAG
- Detecting query cost regressions after code deployments

---

## Trino Query Cost Analysis

```sql
-- Top 20 most expensive queries (CPU + memory) in last 24h
SELECT
    query_id,
    user,
    source,                          -- dbt, airflow, jupyter, etc.
    state,
    CAST(error_code AS VARCHAR)       AS error_code,
    DATE_TRUNC('hour', created)       AS query_hour,
    ROUND(cpu_time_millis / 1000.0 / 60, 1) AS cpu_minutes,
    ROUND(peak_memory_bytes / 1e9, 2) AS peak_memory_gb,
    ROUND(cumulative_memory / 1e12, 3) AS cumulative_memory_tb_ms,
    ROUND(input_bytes / 1e9, 2)       AS input_gb,
    ROUND(input_rows / 1e6, 1)        AS input_rows_M,
    wall_time_millis / 1000           AS wall_seconds,
    -- Cost proxy: CPU minutes × $0.10 + GB scanned × $0.001
    ROUND(cpu_time_millis / 1000.0 / 60 * 0.10 + input_bytes / 1e9 * 0.001, 4) AS estimated_cost_usd,
    SUBSTR(query, 1, 200)             AS query_preview
FROM system.runtime.queries
WHERE created >= NOW() - INTERVAL '24' HOUR
  AND state = 'FINISHED'
ORDER BY estimated_cost_usd DESC
LIMIT 20;

-- Cost by user/team over past 7 days
SELECT
    user,
    source,
    COUNT(*) AS query_count,
    ROUND(SUM(cpu_time_millis) / 1000.0 / 3600, 1)  AS cpu_hours,
    ROUND(SUM(input_bytes) / 1e12, 2)                AS input_TB,
    ROUND(SUM(peak_memory_bytes) / 1e9, 1)           AS peak_mem_gb,
    ROUND(
        SUM(cpu_time_millis / 1000.0 / 60 * 0.10 + input_bytes / 1e9 * 0.001), 2
    )                                                  AS estimated_cost_usd,
    APPROX_PERCENTILE(wall_time_millis / 1000.0, 0.95) AS p95_wall_seconds
FROM system.runtime.queries
WHERE created >= NOW() - INTERVAL '7' DAY
  AND state = 'FINISHED'
GROUP BY user, source
ORDER BY estimated_cost_usd DESC;

-- Queries without partition filters (full-table scans)
SELECT
    query_id,
    user,
    input_rows,
    input_bytes,
    query
FROM system.runtime.queries
WHERE created >= NOW() - INTERVAL '24' HOUR
  AND state = 'FINISHED'
  AND input_rows > 100000000       -- scanned > 100M rows
  AND query NOT LIKE '%partition_date%'
  AND query NOT LIKE '%WHERE%'
ORDER BY input_bytes DESC
LIMIT 10;
```

---

## ClickHouse Query Cost Analysis

```sql
-- Top expensive queries in last 24h
SELECT
    user,
    query_kind,
    round(query_duration_ms / 1000, 2)    AS duration_sec,
    formatReadableSize(memory_usage)       AS memory,
    formatReadableSize(read_bytes)         AS read_bytes,
    formatReadableQuantity(read_rows)      AS read_rows,
    round(memory_usage / 1e9, 2)          AS memory_gb,
    arrayStringConcat(tables, ', ')        AS tables_accessed,
    -- Cost proxy
    round(query_duration_ms / 1000 * 0.001 + read_bytes / 1e9 * 0.001, 5) AS cost_usd,
    LEFT(query, 300)                       AS query_preview
FROM system.query_log
WHERE type = 'QueryFinish'
  AND event_time >= now() - INTERVAL 24 HOUR
ORDER BY memory_usage DESC
LIMIT 20;

-- Users exceeding daily memory budget (> 100 GB total)
SELECT
    user,
    COUNT()                                 AS query_count,
    formatReadableSize(SUM(memory_usage))   AS total_memory,
    formatReadableSize(SUM(read_bytes))     AS total_read_bytes,
    round(SUM(query_duration_ms) / 3600000, 2) AS total_hours
FROM system.query_log
WHERE type = 'QueryFinish'
  AND event_time >= toStartOfDay(now())
GROUP BY user
HAVING SUM(memory_usage) > 100 * 1e9   -- 100 GB
ORDER BY SUM(memory_usage) DESC;

-- Slow queries by table (join with table stats for context)
SELECT
    arrayJoin(tables)                       AS table_name,
    COUNT()                                 AS query_count,
    median(query_duration_ms)               AS median_ms,
    quantile(0.95)(query_duration_ms)       AS p95_ms,
    formatReadableSize(median(read_bytes))  AS median_scan
FROM system.query_log
WHERE type = 'QueryFinish'
  AND event_time >= now() - INTERVAL 7 DAY
GROUP BY table_name
ORDER BY p95_ms DESC
LIMIT 20;
```

---

## LLM-Driven Query Rewrite Agent

```python
import anthropic
import re

client = anthropic.Anthropic()

QUERY_OPTIMIZER_SYSTEM = """You are a Trino/ClickHouse SQL query optimizer.
Analyze the provided query and its execution stats, then suggest specific rewrites to reduce cost.

Focus on:
1. Missing partition filters (add WHERE partition_date = ...)
2. Full table scans without predicate pushdown
3. Missing broadcast hints for small tables (< 100 MB)
4. GROUP BY before JOIN (reduce rows before joining)
5. APPROX_DISTINCT instead of COUNT(DISTINCT) where exactness not required
6. Unnecessary columns in SELECT *
7. Missing LIMIT on exploratory queries

Respond in JSON:
{
  "issues": [{"type": "string", "description": "string", "severity": "high|medium|low"}],
  "rewritten_query": "string",
  "estimated_cost_reduction_pct": number,
  "explanation": "string"
}"""


def analyze_and_rewrite_query(
    query: str,
    execution_stats: dict,
    engine: str = "trino",
) -> dict:
    """Use LLM to analyze a slow/expensive query and suggest rewrites."""

    user_message = f"""
Engine: {engine}
Execution stats:
- CPU time: {execution_stats.get('cpu_minutes', 'N/A')} minutes
- Data scanned: {execution_stats.get('input_gb', 'N/A')} GB
- Peak memory: {execution_stats.get('peak_memory_gb', 'N/A')} GB
- Wall time: {execution_stats.get('wall_seconds', 'N/A')} seconds
- Rows scanned: {execution_stats.get('input_rows_M', 'N/A')}M

Query:
```sql
{query}
```

Analyze this query and suggest cost-reducing rewrites.
"""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=QUERY_OPTIMIZER_SYSTEM,
        messages=[{"role": "user", "content": user_message}],
    )

    text = response.content[0].text
    # Extract JSON from response
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        import json
        return json.loads(json_match.group())
    return {"raw_response": text}


def batch_analyze_expensive_queries(
    queries: list[dict],   # [{query_id, query, cpu_minutes, input_gb, peak_memory_gb, user}]
    top_n: int = 10,
) -> list[dict]:
    """Analyze top N most expensive queries and generate optimization report."""
    # Sort by cost
    sorted_queries = sorted(queries, key=lambda q: q.get("input_gb", 0), reverse=True)[:top_n]

    results = []
    for q in sorted_queries:
        analysis = analyze_and_rewrite_query(
            query=q["query"],
            execution_stats=q,
        )
        results.append({
            "query_id": q.get("query_id"),
            "user": q.get("user"),
            "original_cost_gb": q.get("input_gb"),
            "analysis": analysis,
        })

    return results
```

---

## Cost Anomaly Detection

```python
import numpy as np
from datetime import datetime, timedelta

def detect_query_cost_anomalies(
    daily_costs: dict[str, list[float]],   # {user: [daily_cost_usd_30d]}
    alert_threshold_sigma: float = 3.0,
) -> list[dict]:
    """
    Detect users/teams with anomalous query cost today vs their 30-day baseline.
    """
    alerts = []
    for user, costs in daily_costs.items():
        if len(costs) < 7:
            continue

        baseline = np.array(costs[:-1])  # all but today
        today = costs[-1]

        mean = np.mean(baseline)
        std = np.std(baseline)
        if std == 0:
            continue

        z = (today - mean) / std
        if z > alert_threshold_sigma:
            alerts.append({
                "user": user,
                "today_cost_usd": round(today, 2),
                "baseline_mean_usd": round(mean, 2),
                "z_score": round(z, 2),
                "severity": "critical" if z > 5.0 else "warning",
                "message": f"Query cost anomaly: {user} spent ${today:.2f} today vs ${mean:.2f} baseline (z={z:.1f}σ)",
            })

    return sorted(alerts, key=lambda a: a["z_score"], reverse=True)
```

---

## Cost Chargeback Report

```python
def generate_chargeback_report(
    trino_query_stats: list[dict],
    team_mapping: dict[str, str],   # {user: team}
    month: str,                     # "2024-01"
) -> str:
    """Generate monthly cost chargeback report by team."""
    from collections import defaultdict

    team_costs = defaultdict(lambda: {"queries": 0, "cpu_hours": 0, "input_TB": 0, "cost_usd": 0.0})

    for q in trino_query_stats:
        team = team_mapping.get(q["user"], "unknown")
        team_costs[team]["queries"] += 1
        team_costs[team]["cpu_hours"] += q.get("cpu_minutes", 0) / 60
        team_costs[team]["input_TB"] += q.get("input_gb", 0) / 1000
        team_costs[team]["cost_usd"] += q.get("estimated_cost_usd", 0)

    lines = [
        f"# Query Cost Chargeback Report — {month}",
        "",
        "| Team | Queries | CPU Hours | Data Scanned (TB) | Estimated Cost |",
        "|------|---------|-----------|-------------------|----------------|",
    ]
    total_cost = 0
    for team, stats in sorted(team_costs.items(), key=lambda x: x[1]["cost_usd"], reverse=True):
        lines.append(
            f"| {team} | {stats['queries']:,} | {stats['cpu_hours']:.1f} | "
            f"{stats['input_TB']:.2f} | ${stats['cost_usd']:.2f} |"
        )
        total_cost += stats["cost_usd"]

    lines += ["", f"**Total**: ${total_cost:.2f}"]
    return "\n".join(lines)
```

---

## Anti-Patterns

1. **Cost analysis only on failed queries** — expensive queries often succeed but consume disproportionate resources; analyze all finished queries, not just errors.
2. **No query tagging** — `SELECT *` from user "john" is impossible to attribute; enforce `SET SESSION query_tag = 'dag=etl_orders,task=load_orders'` in all pipeline queries.
3. **Optimizing the wrong queries** — a 10-second query run 10,000× per day costs more than a 10-minute query run once; optimize by total cost, not worst single query.
4. **LLM rewrites without testing** — auto-applying LLM-suggested rewrites without running EXPLAIN first can change semantics; require human review for rewrite approval.
5. **Chargeback without optimization help** — showing teams their costs without providing optimization guidance creates finger-pointing; pair chargeback with self-service optimization tips.

---

## References

- Trino system tables: `trino.io/docs/current/connector/system.html`
- ClickHouse query_log: `clickhouse.com/docs/en/operations/system-tables/query_log`
- BigQuery INFORMATION_SCHEMA.JOBS: `cloud.google.com/bigquery/docs/information-schema-jobs`
- Related skills: `[[de-cost-optimization]]`, `[[trino-iceberg]]`, `[[clickhouse-olap]]`, `[[aiops-infrastructure-anomaly-detection]]`
