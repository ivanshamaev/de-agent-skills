---
name: starrocks-ai-query-autotuner
description: StarRocks AI query autotuner — autonomous SQL optimization agent workflow (EXPLAIN COSTS → analyze → recommend), materialized view recommendation from slow query log, index recommendation (bitmap/bloom filter), partition pruning diagnosis, join order hints generation, statistics staleness detection and auto-ANALYZE trigger, slow query pattern classification, query rewrite suggestions
---

# StarRocks AI Query Autotuner

## When to Use

- Automatically diagnose slow StarRocks queries without manual EXPLAIN reading
- Recommend materialized views based on slow query patterns
- Detect missing statistics and trigger ANALYZE automatically
- Generate query hints (LEADING, JOIN_METHOD) for bad join plans
- Classify query anti-patterns (full table scan, Cartesian join, no partition filter)

---

## Autotuner Agent Workflow

```
Input: slow query SQL or query_id
  │
  ├── 1. Fetch EXPLAIN COSTS plan
  ├── 2. Classify anti-patterns (full scan / bad join / no stats)
  ├── 3. Analyze statistics freshness (SHOW ANALYZE STATUS)
  ├── 4. Check colocate group membership
  ├── 5. Generate recommendations
  │       ├── MV suggestion
  │       ├── Index suggestion (bitmap/bloom)
  │       ├── Query hint (LEADING/JOIN_METHOD)
  │       └── Rewrite with partition filter
  └── 6. Output: prioritized action list
```

---

## Step 1: Fetch and Parse EXPLAIN COSTS

```python
import pymysql
import re
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class PlanNode:
    node_type: str
    output_rows: float
    cost: float
    table: Optional[str] = None
    partition_info: Optional[str] = None
    is_full_scan: bool = False


def get_explain_costs(host: str, sql: str) -> str:
    conn = pymysql.connect(host=host, port=9030, user="root", db="sales")
    cursor = conn.cursor()
    cursor.execute(f"EXPLAIN COSTS {sql}")
    rows = cursor.fetchall()
    conn.close()
    return "\n".join(r[0] for r in rows)


def parse_explain_plan(plan_text: str) -> List[PlanNode]:
    """Extract key nodes from EXPLAIN COSTS output."""
    nodes = []
    lines = plan_text.split("\n")

    for line in lines:
        line = line.strip()

        # Detect OlapScanNode (table scan)
        if "OlapScanNode" in line or "SCAN" in line.upper():
            table_match = re.search(r'TABLE:\s*(\w+)', line, re.IGNORECASE)
            partition_match = re.search(r'partitions=(\d+)/(\d+)', line)
            rows_match = re.search(r'cardinality=(\d+)', line)

            is_full_scan = False
            if partition_match:
                scanned = int(partition_match.group(1))
                total = int(partition_match.group(2))
                is_full_scan = (scanned == total and total > 1)

            nodes.append(PlanNode(
                node_type="SCAN",
                output_rows=float(rows_match.group(1)) if rows_match else 0,
                cost=0.0,
                table=table_match.group(1) if table_match else None,
                partition_info=f"{partition_match.group(1)}/{partition_match.group(2)}" if partition_match else None,
                is_full_scan=is_full_scan,
            ))

        # Detect join types
        elif "CrossJoin" in line or "CROSS JOIN" in line.upper():
            nodes.append(PlanNode(node_type="CROSS_JOIN", output_rows=0, cost=0))

        elif "HashJoin" in line or "HASH_JOIN" in line.upper():
            rows_match = re.search(r'cardinality=(\d+)', line)
            nodes.append(PlanNode(
                node_type="HASH_JOIN",
                output_rows=float(rows_match.group(1)) if rows_match else 0,
                cost=0,
            ))

    return nodes
```

---

## Step 2: Anti-Pattern Classifier

```python
@dataclass
class QueryIssue:
    severity: str      # "critical", "warning", "info"
    category: str      # "full_scan", "missing_stats", "bad_join", "no_partition_filter", "cartesian"
    description: str
    recommendation: str


def classify_issues(plan_nodes: List[PlanNode], plan_text: str) -> List[QueryIssue]:
    issues = []

    # Full table scans
    for node in plan_nodes:
        if node.node_type == "SCAN" and node.is_full_scan:
            issues.append(QueryIssue(
                severity="critical",
                category="full_scan",
                description=f"Full partition scan on table {node.table}: {node.partition_info} partitions scanned",
                recommendation=f"Add WHERE clause on partition column of {node.table} (e.g., WHERE dt = '...')"
            ))

    # Cartesian join
    if any(n.node_type == "CROSS_JOIN" for n in plan_nodes):
        issues.append(QueryIssue(
            severity="critical",
            category="cartesian",
            description="Cartesian (CROSS) join detected — exponential row explosion",
            recommendation="Add explicit JOIN ON condition or WHERE clause to eliminate cross join"
        ))

    # Very large intermediate results
    for node in plan_nodes:
        if node.node_type == "HASH_JOIN" and node.output_rows > 1_000_000_000:
            issues.append(QueryIssue(
                severity="warning",
                category="bad_join",
                description=f"Hash join produces {node.output_rows/1e9:.1f}B rows — possible fanout",
                recommendation="Verify join cardinality; add filters before join; consider AGGREGATE or LIMIT upstream"
            ))

    # No partition filter (heuristic: check if plan mentions all partitions)
    if re.search(r'partitions=(\d+)/\1', plan_text):  # scanned == total
        issues.append(QueryIssue(
            severity="warning",
            category="no_partition_filter",
            description="Table scan reads all partitions",
            recommendation="Add equality or range filter on the partition column (e.g., WHERE dt >= '2024-01-01')"
        ))

    return issues
```

---

## Step 3: Statistics Staleness Check

```python
from datetime import datetime, timedelta


def check_stale_statistics(
    host: str,
    db: str,
    tables: List[str],
    max_age_hours: int = 24,
) -> List[dict]:
    conn = pymysql.connect(host=host, port=9030, user="root", db=db)
    cursor = conn.cursor()

    stale = []
    for table in tables:
        cursor.execute(f"""
            SELECT TableName, Status, StartTime, EndTime
            FROM _statistics_.analyze_jobs
            WHERE DbName = '{db}' AND TableName = '{table}'
            ORDER BY StartTime DESC
            LIMIT 1
        """)
        row = cursor.fetchone()

        if not row:
            stale.append({"table": table, "reason": "no_statistics"})
        else:
            status = row[1]
            end_time = row[3]
            if isinstance(end_time, str):
                end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            age_hours = (datetime.utcnow() - end_time).total_seconds() / 3600

            if status != "SUCCESS" or age_hours > max_age_hours:
                stale.append({
                    "table": table,
                    "reason": f"stale ({age_hours:.1f}h old, status={status})",
                })

    conn.close()
    return stale
```

---

## Step 4: Materialized View Recommendation

```python
def recommend_materialized_view(sql: str, slow_tables: List[str]) -> str:
    """Generate a MV DDL recommendation based on query pattern."""
    # Detect GROUP BY + aggregation pattern
    group_by_match = re.search(r'GROUP BY\s+(.+?)(?:ORDER BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
    select_match   = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)

    if not group_by_match:
        return "No GROUP BY detected — MV not recommended for this query pattern"

    group_cols = group_by_match.group(1).strip()
    table_name = slow_tables[0] if slow_tables else "your_table"

    mv_suggestion = f"""
-- Recommended Materialized View:
-- This MV pre-aggregates the GROUP BY pattern from the slow query.
-- Refresh every hour for near-real-time analytics.

CREATE MATERIALIZED VIEW {table_name}_mv_auto
DISTRIBUTED BY HASH({group_cols.split(',')[0].strip()}) BUCKETS 8
REFRESH ASYNC EVERY (INTERVAL 1 HOUR)
AS
{sql.rstrip(';')};

-- After creation, verify the optimizer uses the MV:
-- EXPLAIN {sql}
-- Look for: MaterializedViewScanNode instead of OlapScanNode
"""
    return mv_suggestion
```

---

## Step 5: Query Rewrite with Hints

```python
def add_join_hints(sql: str, join_order: List[str], join_method: str = "BROADCAST") -> str:
    """Inject LEADING and JOIN_METHOD hints into a SELECT statement."""
    leading = ", ".join(join_order)
    hint = f"/*+ LEADING({leading}) JOIN_METHOD({join_order[0]}, {join_order[1]}, {join_method}) */"

    # Insert hint after SELECT
    rewritten = re.sub(
        r'^SELECT\s+',
        f'SELECT {hint} ',
        sql.strip(),
        count=1,
        flags=re.IGNORECASE,
    )
    return rewritten


def add_partition_filter(sql: str, table: str, partition_col: str, ds: str) -> str:
    """Add a partition filter if missing."""
    if partition_col.lower() not in sql.lower():
        filter_clause = f"{table}.{partition_col} = '{ds}'"
        if "WHERE" in sql.upper():
            sql = re.sub(r'\bWHERE\b', f"WHERE {filter_clause} AND", sql, count=1, flags=re.IGNORECASE)
        else:
            sql = re.sub(r'\bGROUP BY\b', f"WHERE {filter_clause} GROUP BY", sql, count=1, flags=re.IGNORECASE)
    return sql
```

---

## Step 6: Full Autotuner Report

```python
def autotune_query(host: str, db: str, sql: str) -> str:
    """Full autotuner: analyze query, return prioritized recommendations."""
    report_lines = ["## StarRocks Query Autotuner Report", ""]

    # Get plan
    plan_text = get_explain_costs(host, sql)
    plan_nodes = parse_explain_plan(plan_text)

    # Classify issues
    issues = classify_issues(plan_nodes, plan_text)
    if issues:
        report_lines.append("### Issues Found")
        for issue in sorted(issues, key=lambda i: {"critical": 0, "warning": 1, "info": 2}[i.severity]):
            report_lines.append(f"- [{issue.severity.upper()}] {issue.description}")
            report_lines.append(f"  → {issue.recommendation}")
        report_lines.append("")

    # Check statistics
    tables = [n.table for n in plan_nodes if n.table]
    if tables:
        stale = check_stale_statistics(host, db, tables)
        if stale:
            report_lines.append("### Stale Statistics (auto-ANALYZE recommended)")
            for s in stale:
                report_lines.append(f"- {s['table']}: {s['reason']}")
                report_lines.append(f"  → Run: ANALYZE TABLE {db}.{s['table']} WITH ASYNC MODE")
            report_lines.append("")

    # MV recommendation
    slow_tables = [n.table for n in plan_nodes if n.is_full_scan and n.table]
    if slow_tables and "GROUP BY" in sql.upper():
        mv = recommend_materialized_view(sql, slow_tables)
        report_lines.append("### Materialized View Recommendation")
        report_lines.append(mv)

    if not issues and not stale:
        report_lines.append("### Result: No issues detected")
        report_lines.append("Query plan looks healthy.")

    return "\n".join(report_lines)
```

---

## Slow Query Log Analysis

```sql
-- Find top 10 slowest queries from StarRocks query log
SELECT
    QueryId,
    StartTime,
    EndTime,
    QueryTime,
    State,
    LEFT(Statement, 200) AS sql_preview
FROM information_schema.be_cloud_native_compactions
-- For query history, use:
SHOW PROCESSLIST;

-- Or query FE audit log directly:
-- /data/starrocks/fe/log/fe.audit.log
-- Lines with "Query"
```

Python: parse FE audit log for slow queries:

```python
import re
from datetime import datetime

def parse_fe_audit_log(log_path: str, min_query_time_ms: int = 5000):
    """Extract slow queries from StarRocks FE audit log."""
    slow = []
    pattern = re.compile(
        r'QueryDetail\|(?P<ts>[^|]+)\|(?P<user>[^|]+)\|(?P<db>[^|]+)\|'
        r'(?P<time>\d+)\|(?P<rows>\d+)\|(?P<bytes>\d+)\|(?P<state>\w+)\|'
        r'(?P<sql>.+)'
    )
    with open(log_path) as f:
        for line in f:
            m = pattern.search(line)
            if m and int(m.group("time")) >= min_query_time_ms:
                slow.append({
                    "ts": m.group("ts"),
                    "user": m.group("user"),
                    "db": m.group("db"),
                    "time_ms": int(m.group("time")),
                    "state": m.group("state"),
                    "sql": m.group("sql")[:500],
                })
    return sorted(slow, key=lambda x: x["time_ms"], reverse=True)
```

---

## Anti-Patterns

1. **Running EXPLAIN COSTS on production during peak hours** — EXPLAIN compiles the query plan; light overhead but avoids running for complex sub-queries during peak load.
2. **Auto-applying MV recommendations without testing** — MVs consume storage and refresh resources; always test query improvement before creating in production.
3. **Adding LEADING hints without verifying cardinality** — wrong join order hint makes the plan worse; validate with EXPLAIN COSTS before deploying hint.
4. **Triggering ANALYZE synchronously in autotuner** — blocks; always use `WITH ASYNC MODE` and check status separately.
5. **Classifying issues from plan text only** — partition_info in EXPLAIN shows runtime skipping, not always visible in text; always verify with actual query timing.

---

## References

- EXPLAIN COSTS: `docs.starrocks.io/docs/administration/management/Query_management/Cost_based_optimizer/`
- Query hints: `docs.starrocks.io/docs/sql-reference/sql-statements/hint/`
- Materialized Views: `docs.starrocks.io/docs/using_starrocks/Materialized_view/`
- Related skills: `[[starrocks-explain-plan]]`, `[[starrocks-cbo]]`, `[[starrocks-query-optimizer]]`, `[[starrocks-materialized-views]]`
