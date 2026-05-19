---
name: starrocks-cbo
description: StarRocks cost-based optimizer — ANALYZE TABLE (full/sample/histogram/predicate columns), AUTO ANALYZE configuration, statistics storage (_statistics_ database), SHOW ANALYZE STATUS, stale statistics detection, cardinality estimation failures, CBO join hints (LEADING/broadcast/shuffle), EXPLAIN COSTS reading, multi-column statistics (3.5+)
---

# StarRocks Cost-Based Optimizer (CBO)

## When to Use

Activate this skill when:
- Wrong join strategy chosen (broadcast when shuffle is better, or vice versa)
- Bad cardinality estimates visible in `EXPLAIN COSTS` (estimated 100 rows, actual 10M)
- Post-bulk-load statistics refresh needed
- Slow queries that should be fast given the schema
- Setting up statistics collection schedule for production

---

## CBO Architecture

```
┌───────────────────────────────────────────────────────────┐
│                  Query Optimizer                           │
│                                                           │
│  SQL → Parser → Analyzer → Optimizer → Physical Plan      │
│                                ↕                          │
│              Statistics (column_statistics,               │
│              histogram_statistics, row_count)             │
│                                                           │
│  Cost model: selectivity × row_count × I/O cost           │
│  → choose: join order, join strategy, agg strategy        │
└───────────────────────────────────────────────────────────┘
```

The CBO uses per-column statistics to estimate:
- **Row count** after filters (selectivity)
- **NDV** (Number of Distinct Values) for join cardinality
- **Null fraction** for IS NULL predicate cost
- **Min/Max** for range predicate pruning

---

## ANALYZE TABLE

### Full Statistics Collection

```sql
-- Collect stats on all columns (full table scan)
ANALYZE TABLE orders;

-- Collect specific columns only
ANALYZE TABLE orders (order_id, customer_id, created_at, status);

-- Async mode (returns immediately, runs in background)
ANALYZE TABLE orders WITH ASYNC MODE;

-- Sync mode (blocks until complete — use for ETL pipelines)
ANALYZE TABLE orders WITH SYNC MODE;
```

### Sampled Statistics (faster, slightly less accurate)

```sql
-- Sample with row count
ANALYZE SAMPLE TABLE orders PROPERTIES(
    "statistic_sample_collect_rows" = "5000000"
);

-- Sample by ratio (0.1 = 10% of rows)
ANALYZE SAMPLE TABLE orders PROPERTIES(
    "sample_ratio" = "0.1"
);
```

### Histogram Statistics

Histograms improve cardinality estimation for skewed columns (e.g., `status` has 90% 'delivered', 5% 'cancelled').

```sql
-- Build histogram with 64 buckets (default)
ANALYZE TABLE orders UPDATE HISTOGRAM ON status, region WITH SYNC MODE;

-- Custom bucket count (more buckets = more accuracy, more memory)
ANALYZE TABLE orders UPDATE HISTOGRAM ON amount
    WITH 128 BUCKETS
    PROPERTIES("histogram_sample_ratio" = "0.2");

-- Drop histogram (revert to basic stats)
ANALYZE TABLE orders DROP HISTOGRAM ON status;
```

Histogram parameters:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `histogram_buckets_size` | 64 | Number of equi-height buckets |
| `histogram_mcv_size` | 100 | Most-common-value entries |
| `histogram_sample_ratio` | 0.1 | Fraction of rows sampled |

### Predicate Columns (StarRocks 3.5+)

Automatically identify and collect stats only on frequently-filtered columns:

```sql
-- Collect stats only on columns seen in WHERE clauses
ANALYZE TABLE orders PREDICATE COLUMNS WITH ASYNC MODE;
```

### Partition-Level Statistics

```sql
-- Analyze specific partition only
ANALYZE TABLE orders PARTITION (p20240101) WITH SYNC MODE;

-- Useful after daily partition load (only refresh new partition)
ANALYZE TABLE orders PARTITION (p{{ ds_nodash }}) WITH ASYNC MODE;
```

---

## AUTO ANALYZE Configuration

Configure via FE parameters (set in `fe.conf` or dynamically):

```sql
-- Enable/disable auto analyze globally
ADMIN SET FRONTEND CONFIG("enable_collect_full_statistic" = "true");

-- Health threshold: re-analyze when > 80% of rows changed
ADMIN SET FRONTEND CONFIG("statistic_auto_collect_ratio" = "0.8");

-- Maintenance window (avoids peak hours)
ADMIN SET FRONTEND CONFIG("statistic_auto_analyze_start_time" = "02:00:00");
ADMIN SET FRONTEND CONFIG("statistic_auto_analyze_end_time" = "06:00:00");

-- Small table: re-analyze if any change (interval = 0 seconds)
ADMIN SET FRONTEND CONFIG("statistic_auto_collect_small_table_interval" = "0");

-- Large table: re-analyze every 12 hours max
ADMIN SET FRONTEND CONFIG("statistic_auto_collect_large_table_interval" = "43200");

-- Threshold between "small" and "large" table
ADMIN SET FRONTEND CONFIG("statistic_auto_collect_small_table_size" = "5368709120");  -- 5GB
```

Check current settings:
```sql
SHOW FRONTEND CONFIG LIKE "%statistic%";
```

---

## Monitoring Statistics Collection

### SHOW ANALYZE STATUS

```sql
-- All recent analyze jobs
SHOW ANALYZE STATUS;

-- Filter by table
SHOW ANALYZE STATUS WHERE `Table` = 'orders';

-- Failed jobs only
SHOW ANALYZE STATUS WHERE `Status` = 'FAILED';
```

Key columns:
| Column | Description |
|--------|-------------|
| `Id` | Task ID |
| `Database` | Database name |
| `Table` | Table name |
| `Columns` | Columns analyzed |
| `Type` | FULL / SAMPLE / HISTOGRAM |
| `Schedule` | ONCE (manual) / SCHEDULE (automatic) |
| `Status` | PENDING / RUNNING / SUCCESS / FAILED |
| `StartTime` | When collection started |
| `EndTime` | When collection finished |
| `Reason` | Failure message if FAILED |

### Kill a Running Analyze Job

```sql
-- Get job ID from SHOW ANALYZE STATUS
KILL ANALYZE <job_id>;
```

---

## Statistics Storage Tables

Statistics live in the `_statistics_` database:

```sql
-- Check statistics freshness per table
SELECT
    table_name,
    column_name,
    row_count,
    data_size,
    ndv,
    null_count,
    update_time
FROM _statistics_.column_statistics
WHERE db_id = (SELECT db_id FROM information_schema.schemata WHERE schema_name = 'sales')
ORDER BY update_time ASC;

-- Find stale statistics (not updated in 24h on tables with > 1M rows)
SELECT
    t.table_name,
    cs.update_time,
    t.table_rows,
    TIMESTAMPDIFF(HOUR, cs.update_time, NOW()) AS hours_stale
FROM information_schema.tables t
JOIN (
    SELECT table_name, MIN(update_time) AS update_time
    FROM _statistics_.column_statistics
    GROUP BY table_name
) cs ON t.table_name = cs.table_name
WHERE t.table_schema = 'sales'
  AND t.table_rows > 1000000
  AND TIMESTAMPDIFF(HOUR, cs.update_time, NOW()) > 24
ORDER BY hours_stale DESC;
```

---

## Detecting Cardinality Estimation Errors

Use `EXPLAIN COSTS` to spot bad estimates:

```sql
EXPLAIN COSTS
SELECT s.region, COUNT(*) AS cnt, SUM(o.amount) AS revenue
FROM orders o
JOIN stores s ON o.store_id = s.store_id
WHERE o.created_at >= '2024-01-01'
GROUP BY s.region;
```

Look for in output:
```
OlapScanNode [orders]
  cardinality: 150000000     ← estimated
  actualRows: 2000000000     ← if very different → stale stats
```

A mismatch of **>10×** indicates stale or missing statistics.

### Common Cardinality Failures

| Problem | Symptom | Fix |
|---------|---------|-----|
| Stale stats after bulk load | Estimated rows << actual rows | `ANALYZE TABLE t WITH SYNC MODE` |
| Skewed column (90% same value) | Wrong selectivity for equality filter | `UPDATE HISTOGRAM ON skewed_col` |
| No stats on new column | `cardinality=-1` in EXPLAIN | `ANALYZE TABLE t (new_col)` |
| External table (Iceberg/Hive) | CBO guesses default stats | `ANALYZE TABLE ext_table WITH SYNC MODE` |
| High-NDV string column | Join cardinality overestimated | `UPDATE HISTOGRAM ON string_col` |

---

## CBO Optimizer Hints

Use hints to override the CBO when statistics are wrong or when you know the optimal plan:

### Join Order Hint

```sql
-- Force join order: process t1 first, then t2, then t3
SELECT /*+ LEADING(t1 t2 t3) */ *
FROM orders t1
JOIN customers t2 ON t1.customer_id = t2.customer_id
JOIN stores t3 ON t1.store_id = t3.store_id;
```

### Join Strategy Hints

```sql
-- Force broadcast join (replicate dim_store to all BEs)
SELECT /*+ JOIN(dim_store BROADCAST) */
    o.order_id, s.region
FROM orders o
JOIN dim_store s ON o.store_id = s.store_id;

-- Force shuffle join (repartition both sides)
SELECT /*+ JOIN(orders SHUFFLE) JOIN(customers SHUFFLE) */
    o.order_id, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
```

### Per-Query Session Variables

```sql
-- Override join reorder limit (default 4 tables)
SELECT /*+ SET_VAR(cbo_max_reorder_node=8) */
    ...
FROM t1 JOIN t2 JOIN t3 JOIN t4 JOIN t5 JOIN t6;

-- Disable CBO table prune for debugging
SELECT /*+ SET_VAR(enable_cbo_table_prune=false) */ ...
FROM orders;

-- Allow optimizer more time (complex queries)
SELECT /*+ SET_VAR(new_planner_optimize_timeout=10000) */ ...
```

### Session-Level Variable Override

```sql
-- For current session
SET cbo_max_reorder_node = 8;
SET enable_predicate_reorder = true;

-- Check current optimizer settings
SHOW VARIABLES LIKE '%cbo%';
SHOW VARIABLES LIKE '%optimizer%';
```

---

## EXPLAIN COSTS Reading

```sql
EXPLAIN COSTS SELECT customer_id, SUM(amount) FROM orders WHERE created_at >= '2024-01-01' GROUP BY customer_id;
```

Sample output with annotations:
```
PLAN FRAGMENT 0
  OUTPUT EXPRS: customer_id | sum(amount)
  PARTITION: UNPARTITIONED

  RESULT SINK

  4:AGGREGATE (merge finalize)
  |  output: sum(4: amount)         ← global merge agg
  |  group by: 1: customer_id
  |  cardinality: 5000000           ← estimated output groups
  |  cost: {CPU: 5000000, Memory: 200000000, ...}
  |
  3:EXCHANGE
  |  distribution type: HASH[1: customer_id]
  |  cardinality: 150000000
  |
  PLAN FRAGMENT 1 ... (per BE)
    2:AGGREGATE (update serialize)
    |  output: sum(4: amount)       ← local pre-agg on each BE
    |  cardinality: 5000000
    |
    1:OlapScanNode
       TABLE: orders
       PREAGGREGATION: ON
       PREDICATES: created_at >= '2024-01-01'
       partitions=30/365             ← partition pruning working!
       rollup: orders
       tabletRatio=128/128
       cardinality: 150000000        ← CBO estimate from stats
       cost: {CPU: 150000000, Memory: 0, I/O: 6000000000}
```

Key things to check:
1. `partitions=N/total` — partition pruning working
2. `cardinality` — does it match reality?
3. Join type in `HashJoinNode`: BROADCAST / PARTITIONED / COLOCATE
4. `PREAGGREGATION: ON` — pre-agg at scan level for Aggregate Key tables
5. `cost` — CPU-heavy vs I/O-heavy determines bottleneck

---

## Statistics Maintenance Schedule

Recommended Airflow DAG for production:

```python
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

@dag(schedule="0 3 * * *", start_date=datetime(2024, 1, 1), catchup=False)
def starrocks_stats_maintenance():

    @task
    def analyze_high_traffic_tables():
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        tables = [
            "sales.orders",
            "sales.order_items",
            "sales.customers",
        ]
        for table in tables:
            hook.run(f"ANALYZE TABLE {table} WITH ASYNC MODE")

    @task
    def analyze_after_daily_load(ds=None):
        """Analyze only today's partition after load."""
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        partition = f"p{ds.replace('-', '')}"
        hook.run(
            f"ANALYZE TABLE sales.events PARTITION ({partition}) "
            "WITH ASYNC MODE"
        )

    @task
    def check_stale_stats():
        """Alert on tables with stale stats."""
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        result = hook.get_records("""
            SELECT table_name, MIN(update_time) AS oldest_stat
            FROM _statistics_.column_statistics
            WHERE db_name = 'sales'
            GROUP BY table_name
            HAVING TIMESTAMPDIFF(HOUR, MIN(update_time), NOW()) > 24
        """)
        if result:
            stale = [r[0] for r in result]
            raise ValueError(f"Stale stats on: {stale}")

    analyze_high_traffic_tables() >> analyze_after_daily_load() >> check_stale_stats()

dag = starrocks_stats_maintenance()
```

---

## DROP STATS (Force Re-Collect)

```sql
-- Drop all stats for a table (will be re-collected by auto-analyze)
DROP STATS orders;

-- Drop histogram only
ANALYZE TABLE orders DROP HISTOGRAM ON status;
```

---

## Multi-Column Statistics (StarRocks 3.5+)

For correlated columns where single-column stats underestimate selectivity:

```sql
-- Collect joint NDV for (region, category) combination
ANALYZE TABLE orders MULTIPLE COLUMNS (region, product_category)
    WITH ASYNC MODE;

-- Check if multi-column stats exist
SELECT * FROM _statistics_.multi_column_statistics
WHERE table_name = 'orders';
```

Use when: queries filter on two correlated columns and CBO picks wrong join strategy.

---

## Anti-Patterns

1. **Never running ANALYZE after bulk loads** — CBO uses stale row counts, picks wrong join strategy; add ANALYZE step to every ETL pipeline.

2. **Running ANALYZE FULL TABLE on 10B-row tables hourly** — use `ANALYZE SAMPLE` or partition-level analyze instead.

3. **Ignoring `cardinality=-1` in EXPLAIN** — means no stats at all; CBO uses hardcoded defaults which are almost always wrong.

4. **Over-using hints instead of fixing stats** — hints are hard to maintain; fix the root cause (stats freshness) first.

5. **No statistics collection window** — running ANALYZE during peak query hours causes I/O contention; set `statistic_auto_analyze_start_time` to off-peak hours.

6. **Not collecting histograms on skewed columns** — a column with 95% one value needs a histogram for correct selectivity; basic NDV stats will be wrong.

7. **External table stats never collected** — Iceberg/Hive tables have no automatic stats collection; manually ANALYZE after partition adds.

---

## References

- StarRocks CBO docs: `docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/`
- ANALYZE TABLE reference: `docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ANALYZE_TABLE/`
- Statistics best practices: `docs.starrocks.io/docs/best_practices/`
- Related skills: `[[starrocks-explain-plan]]`, `[[starrocks-query-optimizer]]`, `[[starrocks-join-optimization]]`
