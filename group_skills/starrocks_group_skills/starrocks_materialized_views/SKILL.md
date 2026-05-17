---
name: starrocks-materialized-views
description: StarRocks materialized views — synchronous MV (rollup/aggregate acceleration), asynchronous MV (CREATE MATERIALIZED VIEW REFRESH ASYNC/MANUAL/SCHEDULED, partition-aware refresh, query rewrite, nested MVs, MV on Iceberg/Hive external tables), SHOW MATERIALIZED VIEWS, ALTER/REFRESH/DROP MV, transparent query rewrite via EXPLAIN, use_mv hint
---

# StarRocks — Materialized Views

## When to Use

Load this skill when the user needs to:
- Pre-aggregate large fact tables for BI dashboards (daily/hourly roll-ups)
- Accelerate ad-hoc analytical queries with transparent query rewrite
- Speed up lakehouse queries on Iceberg or Hive external tables without moving data
- Create rollup indexes on a base table to reduce scan and aggregation cost
- Schedule incremental or partition-aware MV refreshes for near-real-time data
- Debug why a query is not rewriting to an existing MV
- Manage MV lifecycle: refresh, alter refresh schedule, cancel, drop
- Build nested MVs (MV on top of another MV) for multi-level pre-aggregation

---

## Two Types of Materialized Views

StarRocks has two fundamentally different MV mechanisms. Choose the right one before writing any DDL.

| Dimension | Synchronous MV (Rollup) | Asynchronous MV |
|---|---|---|
| Creation syntax | `CREATE MATERIALIZED VIEW mv AS SELECT ...` (no `REFRESH` clause) | `CREATE MATERIALIZED VIEW mv REFRESH [ASYNC\|MANUAL\|SCHEDULED] AS SELECT ...` |
| Update mechanism | Updated atomically on every write to the base table | Refreshed on a schedule, manually, or triggered by base-table events |
| Latency | Always consistent with base table | Eventually consistent; controlled by refresh interval |
| Scope | Single base table only; simple GROUP BY aggregations | Multi-table joins, subqueries, window functions, expressions |
| Storage location | Stored as a hidden rollup index inside the base tablet | Stored as an independent table in the database |
| Query rewrite | Automatic; transparent to the planner | Automatic when `enable_materialized_view_rewrite = true`; always consistent check is implicit |
| DDL complexity | Simpler; fewer properties | Full DDL with partitioning, distribution, ORDER BY, PROPERTIES |
| External table support | No | Yes (Iceberg, Hive, JDBC, etc.) |
| Nested MVs | No | Yes (MV built on another asynchronous MV) |

**Rule of thumb:**
- Use a **synchronous MV** when the query can be expressed as a single-table aggregate and you need zero-lag consistency.
- Use an **asynchronous MV** for everything else: multi-table joins, external catalogs, complex expressions, lakehouse acceleration, or when eventual consistency is acceptable.

---

## Synchronous MV (Rollup)

A synchronous MV is a pre-computed rollup index stored alongside the base table's tablets. It is maintained automatically by every `INSERT`, `UPDATE`, `DELETE`, and `LOAD` on the base table.

### Constraints

- Must reference a **single internal base table** (no joins, no subqueries).
- Aggregate functions allowed: `SUM`, `MIN`, `MAX`, `COUNT`, `BITMAP_UNION`, `HLL_UNION`, `PERCENTILE_UNION`.
- `GROUP BY` columns must be a subset of the base table's key columns (for Duplicate Key / Aggregate Key models).
- Cannot contain `WHERE`, `HAVING`, `LIMIT`, `ORDER BY` in the MV definition.
- Cannot use window functions.
- The primary key model (`PRIMARY KEY`) does not support synchronous MVs.

### Basic Aggregate Rollup

```sql
-- Base table: orders with a Duplicate Key model
CREATE TABLE orders (
    order_id     BIGINT       NOT NULL,
    user_id      INT          NOT NULL,
    order_date   DATE         NOT NULL,
    region       VARCHAR(32)  NOT NULL,
    amount       DECIMAL(18,2) NOT NULL,
    quantity     INT          NOT NULL
)
DUPLICATE KEY(order_id)
PARTITION BY RANGE(order_date)(
    PARTITION p2026_01 VALUES [('2026-01-01'), ('2026-02-01')),
    PARTITION p2026_02 VALUES [('2026-02-01'), ('2026-03-01'))
)
DISTRIBUTED BY HASH(order_id) BUCKETS 32
PROPERTIES ("replication_num" = "3");

-- Synchronous MV: daily revenue per user
CREATE MATERIALIZED VIEW mv_orders_user_daily
AS
SELECT
    user_id,
    order_date,
    SUM(amount)   AS total_amount,
    SUM(quantity) AS total_quantity,
    COUNT(*)      AS order_count
FROM orders
GROUP BY user_id, order_date;
```

The `CREATE MATERIALIZED VIEW` statement submits a background schema-change job. The MV is built asynchronously while the table remains writable.

### Monitor Creation Progress

```sql
-- Check whether the rollup job has finished
SHOW ALTER TABLE ROLLUP FROM sales_db;
```

Key columns in the output:

| Column | Meaning |
|---|---|
| `JobId` | Unique job identifier |
| `TableName` | Base table name |
| `IndexName` | MV name (= rollup index name) |
| `State` | `PENDING`, `WAITING_TXN`, `RUNNING`, `FINISHED`, `CANCELLED` |
| `Msg` | Error message when State = CANCELLED |
| `Progress` | Percentage complete, e.g. `50%` |
| `Timeout` | Job timeout in seconds |

```sql
-- Wait for a specific MV only
SHOW ALTER TABLE ROLLUP FROM sales_db
WHERE IndexName = 'mv_orders_user_daily';
```

### Automatic Query Rewrite for Synchronous MVs

Once `State = FINISHED`, the planner automatically rewrites qualifying queries to scan the rollup:

```sql
-- Query without any hint — planner rewrites to mv_orders_user_daily
SELECT user_id, order_date, SUM(amount)
FROM orders
GROUP BY user_id, order_date;
```

Run `EXPLAIN` to confirm which rollup is chosen:

```sql
EXPLAIN SELECT user_id, order_date, SUM(amount)
FROM orders
GROUP BY user_id, order_date;
```

Look for `rollup: mv_orders_user_daily` in the `OlapScanNode` section of the plan. If you see `rollup: orders` the base table is being scanned.

### Multiple Rollups for Different Query Patterns

```sql
-- Rollup for region-level summary
CREATE MATERIALIZED VIEW mv_orders_region_daily
AS
SELECT
    region,
    order_date,
    SUM(amount)   AS total_amount,
    COUNT(*)      AS order_count
FROM orders
GROUP BY region, order_date;

-- Rollup for overall daily totals (highest compression)
CREATE MATERIALIZED VIEW mv_orders_daily
AS
SELECT
    order_date,
    SUM(amount)   AS total_amount,
    COUNT(*)      AS order_count
FROM orders
GROUP BY order_date;
```

The planner picks the most selective rollup that satisfies the query. Prefer fewer rollups with well-chosen dimensions over many fine-grained ones to limit write amplification.

### Dropping a Synchronous MV

```sql
DROP MATERIALIZED VIEW mv_orders_user_daily ON orders;
```

The `ON <base_table>` clause is required for synchronous MVs because the MV name is scoped to the base table's rollup index list.

---

## Asynchronous MV — Creation DDL

### Full Syntax Reference

```sql
CREATE MATERIALIZED VIEW <mv_name>
[COMMENT '<comment>']
[PARTITION BY
    { <partition_col>
    | date_trunc('<granularity>', <partition_col>) }
]
[DISTRIBUTED BY HASH(<col>[, <col>...]) [BUCKETS <n>]]
[ORDER BY (<col>[, <col>...])]
[REFRESH
    { ASYNC                                          -- trigger on base-table change
    | ASYNC START('<datetime>') EVERY(INTERVAL <n> { HOUR | DAY | ... })
    | MANUAL                                         -- only on explicit REFRESH command
    }
]
[PROPERTIES (
    "replication_num"                = "<n>",
    "partition_refresh_number"       = "<n>",
    "excluded_trigger_tables"        = "<tbl>[,<tbl>]",
    "auto_refresh_partitions_limit"  = "<n>",
    "mv_rewrite_staleness_second"    = "<n>",
    "colocate_with"                  = "<colocate_group>"
)]
AS
<select_statement>;
```

### REFRESH Modes

| Mode | When to use |
|---|---|
| `REFRESH ASYNC` | Trigger refresh automatically whenever any base table has new data imported. Best for streaming or near-real-time ingestion where data arrives continuously. |
| `REFRESH ASYNC START('2026-01-01 00:00:00') EVERY(INTERVAL 1 HOUR)` | Scheduled periodic refresh. Use for daily/hourly BI dashboards where eventual consistency is acceptable. |
| `REFRESH MANUAL` | Refresh only when explicitly called with `REFRESH MATERIALIZED VIEW`. Use when the pipeline orchestrator (Airflow, etc.) controls the refresh timing. |

### PROPERTIES Keys

| Key | Default | Purpose |
|---|---|---|
| `replication_num` | cluster default | Number of data replicas for the MV table |
| `partition_refresh_number` | `1` | How many partitions to refresh in a single transaction. Increase to trade memory for throughput. |
| `excluded_trigger_tables` | `""` | Comma-separated list of base tables whose writes should NOT trigger an async refresh. Useful for dimension tables that change rarely. |
| `auto_refresh_partitions_limit` | `-1` (unlimited) | Maximum number of partitions to refresh in one async trigger run. Cap this to protect BE memory on large MVs. |
| `mv_rewrite_staleness_second` | `0` | Seconds of allowed staleness for query rewrite. When > 0, the planner will rewrite even if the MV is slightly behind the base table — trades freshness for lower query latency. |
| `colocate_with` | `""` | Assign the MV to a colocate group to enable local joins with other tables in the same group. |

---

## Asynchronous MV — Examples

### Single-Table Aggregate MV (Hourly Schedule)

```sql
CREATE MATERIALIZED VIEW mv_orders_hourly_agg
COMMENT 'Hourly order aggregates for the revenue dashboard'
PARTITION BY date_trunc('day', order_date)
DISTRIBUTED BY HASH(region) BUCKETS 16
ORDER BY (region, order_date)
REFRESH ASYNC START('2026-01-01 01:00:00') EVERY(INTERVAL 1 HOUR)
PROPERTIES (
    "replication_num"               = "3",
    "partition_refresh_number"      = "3",
    "auto_refresh_partitions_limit" = "7"
)
AS
SELECT
    date_trunc('hour', order_ts)      AS order_hour,
    order_date,
    region,
    SUM(amount)                        AS total_amount,
    COUNT(*)                           AS order_count,
    COUNT(DISTINCT user_id)            AS unique_users
FROM orders
GROUP BY
    date_trunc('hour', order_ts),
    order_date,
    region;
```

### Multi-Table Join MV

```sql
CREATE MATERIALIZED VIEW mv_order_item_daily
COMMENT 'Denormalized order-item fact for item-level dashboards'
PARTITION BY date_trunc('day', o.order_date)
DISTRIBUTED BY HASH(o.order_id) BUCKETS 32
REFRESH ASYNC START('2026-01-01 02:00:00') EVERY(INTERVAL 1 DAY)
PROPERTIES (
    "replication_num"               = "3",
    "partition_refresh_number"      = "1",
    "auto_refresh_partitions_limit" = "14"
)
AS
SELECT
    o.order_date,
    o.region,
    o.user_id,
    i.product_id,
    i.category,
    SUM(i.unit_price * i.quantity) AS revenue,
    SUM(i.quantity)                AS units_sold,
    COUNT(DISTINCT o.order_id)     AS order_count
FROM orders          AS o
INNER JOIN order_items AS i ON o.order_id = i.order_id
GROUP BY
    o.order_date,
    o.region,
    o.user_id,
    i.product_id,
    i.category;
```

### Manual Refresh MV (Orchestrator-Controlled)

```sql
CREATE MATERIALIZED VIEW mv_monthly_cohort
COMMENT 'Monthly user cohort retention — refreshed by Airflow after ETL'
DISTRIBUTED BY HASH(cohort_month) BUCKETS 8
REFRESH MANUAL
AS
SELECT
    date_trunc('month', first_order_date) AS cohort_month,
    DATE_DIFF('month', first_order_date, activity_date) AS months_since_first,
    COUNT(DISTINCT user_id)               AS retained_users
FROM user_activity_fact
GROUP BY
    date_trunc('month', first_order_date),
    DATE_DIFF('month', first_order_date, activity_date);
```

Trigger from Airflow after the upstream DAG completes:

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

refresh_mv = SQLExecuteQueryOperator(
    task_id="refresh_mv_monthly_cohort",
    conn_id="starrocks_default",
    sql="REFRESH MATERIALIZED VIEW mv_monthly_cohort;",
)
```

---

## Partition-Aware Refresh

Partition-aware refresh is the key to efficient incremental maintenance of asynchronous MVs. When the MV is partitioned on the same column (or a time-truncated version) as the base table, StarRocks refreshes only the partitions whose source data has changed since the last refresh.

### How Partition Mapping Works

1. The MV `PARTITION BY` expression must map deterministically to a base table partition column.
2. StarRocks tracks a refresh timestamp per MV partition.
3. On each refresh trigger, it compares base-table partition modification times against MV partition refresh times and builds a minimal refresh set.

```sql
-- Base table partitioned by order_date (DATE)
CREATE TABLE orders_partitioned (
    order_id   BIGINT        NOT NULL,
    order_date DATE          NOT NULL,
    region     VARCHAR(32),
    amount     DECIMAL(18,2)
)
DUPLICATE KEY(order_id)
PARTITION BY RANGE(order_date)(
    START ("2025-01-01") END ("2027-01-01") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(order_id) BUCKETS 32;

-- MV maps each day partition in orders_partitioned
-- to the same day partition in the MV
CREATE MATERIALIZED VIEW mv_daily_revenue
PARTITION BY order_date                        -- direct column mapping
DISTRIBUTED BY HASH(region) BUCKETS 16
REFRESH ASYNC START('2026-01-01 00:30:00') EVERY(INTERVAL 1 DAY)
PROPERTIES (
    "replication_num"               = "3",
    "partition_refresh_number"      = "3",
    "auto_refresh_partitions_limit" = "30"
)
AS
SELECT
    order_date,
    region,
    SUM(amount)  AS total_amount,
    COUNT(*)     AS order_count
FROM orders_partitioned
GROUP BY order_date, region;
```

### Truncated Partition Mapping

When the base table is partitioned at finer granularity (e.g., hourly) but the MV should be partitioned at day level:

```sql
-- Base table: partitioned by event_dt at HOUR granularity
CREATE MATERIALIZED VIEW mv_events_daily
PARTITION BY date_trunc('day', event_dt)       -- coarsen hour -> day
DISTRIBUTED BY HASH(user_id) BUCKETS 32
REFRESH ASYNC EVERY(INTERVAL 1 HOUR)
AS
SELECT
    date_trunc('day', event_dt)  AS event_day,
    user_id,
    event_type,
    COUNT(*)                     AS event_count
FROM raw_events
GROUP BY
    date_trunc('day', event_dt),
    user_id,
    event_type;
```

Supported granularities for `date_trunc` in `PARTITION BY`: `'hour'`, `'day'`, `'month'`, `'quarter'`, `'year'`.

### Inspect Partition Refresh State

```sql
-- See each MV partition and its last refresh time
SHOW PARTITIONS FROM mv_daily_revenue;
```

Key columns:

| Column | Meaning |
|---|---|
| `PartitionName` | Partition name, e.g. `p20260501` |
| `VisibleVersionTime` | Timestamp when this partition was last successfully refreshed |
| `DataSize` | Compressed data size stored in the partition |
| `IsTemp` | Whether this is a temporary partition |

Check which partitions are stale:

```sql
SELECT
    PartitionName,
    VisibleVersionTime,
    DataSize
FROM information_schema.partitions
WHERE table_name = 'mv_daily_revenue'
  AND table_schema = 'sales_db'
ORDER BY PartitionName;
```

---

## Query Rewrite

Query rewrite is the mechanism by which the StarRocks planner transparently substitutes a scan of one or more base tables with a scan of a pre-computed MV, without any change to the user query.

### Enabling Query Rewrite

Query rewrite is controlled by a session variable (enabled by default in StarRocks 3.x):

```sql
-- Confirm current setting
SHOW VARIABLES LIKE 'enable_materialized_view_rewrite';

-- Enable for the current session (default: true in SR 3.x)
SET enable_materialized_view_rewrite = true;

-- Disable rewrite for debugging (forces base table scan)
SET enable_materialized_view_rewrite = false;
```

Enable globally (FE config, `fe.conf`):

```
enable_materialized_view_rewrite = true
```

### Rewrite Eligibility Conditions

For the planner to rewrite a query to use an asynchronous MV, **all** of the following must be true:

1. **Output columns**: Every column requested by the query can be derived from MV output columns (direct reference or computable expression).
2. **Aggregation compatibility**: If the query aggregates, the MV must have pre-aggregated at the same or coarser granularity using the same or compatible aggregate functions. `SUM` in MV satisfies `SUM` in query; `COUNT(DISTINCT ...)` in MV does NOT rewrite a simple `COUNT(*)`.
3. **Filter subsumption**: The MV's `WHERE` clause (if any) must subsume the query's filter — i.e., every row the query needs is present in the MV.
4. **Join equivalence**: For multi-table MVs, the join structure (tables, join keys, join types) must match the query's join.
5. **Freshness**: If `mv_rewrite_staleness_second = 0` (default), the MV must not be stale (last refresh must cover all partitions the query touches). Increase `mv_rewrite_staleness_second` to relax this requirement.
6. **MV state**: The MV must be in `ACTIVE` state. An MV enters `INACTIVE` state if the base table is dropped or altered incompatibly.

### Confirming Rewrite with EXPLAIN

```sql
EXPLAIN
SELECT order_date, region, SUM(amount) AS total_amount, COUNT(*) AS order_count
FROM orders_partitioned
WHERE order_date BETWEEN '2026-04-01' AND '2026-04-30'
GROUP BY order_date, region;
```

A rewritten plan shows `mv_daily_revenue` in the scan node:

```
PLAN FRAGMENT 0
  OUTPUT EXPRS: 1: order_date | 4: region | 5: total_amount | 6: order_count
  ...
  1: OlapScanNode
       TABLE: mv_daily_revenue
       PREAGGREGATION: ON
       PREDICATES: 1: order_date >= '2026-04-01', 1: order_date <= '2026-04-30'
       partitions=30/365
       rollup: mv_daily_revenue
       ...
```

Without rewrite, you would see `TABLE: orders_partitioned`.

### Verbose EXPLAIN to Diagnose Rewrite Misses

```sql
EXPLAIN VERBOSE
SELECT order_date, region, SUM(amount) AS total_amount
FROM orders_partitioned
GROUP BY order_date, region;
```

`EXPLAIN VERBOSE` includes a `MaterializedView` section that lists each candidate MV, whether it was chosen, and the reason it was rejected if not selected. Common rejection reasons:

| Rejection reason | Fix |
|---|---|
| `MV is not active` | Check `SHOW MATERIALIZED VIEWS`; run `ALTER MATERIALIZED VIEW mv ACTIVE` if needed |
| `MV output column not found` | Add missing columns or expressions to the MV SELECT list |
| `MV is stale` | Run `REFRESH MATERIALIZED VIEW mv` or increase `mv_rewrite_staleness_second` |
| `Aggregation not compatible` | Ensure the MV aggregates at the same or coarser level as the query |
| `Filter not subsumed` | The MV has a WHERE clause that excludes rows the query needs; widen or remove the MV filter |

### Force or Disable Rewrite with Query Hints

```sql
-- Force rewrite to a specific MV (query hint)
SELECT /*+ SET_VAR(enable_materialized_view_rewrite=true) */
    order_date,
    region,
    SUM(amount)
FROM orders_partitioned
GROUP BY order_date, region;

-- Disable rewrite for a single query (debugging)
SELECT /*+ SET_VAR(enable_materialized_view_rewrite=false) */
    order_date,
    region,
    SUM(amount)
FROM orders_partitioned
GROUP BY order_date, region;

-- Direct MV hint (StarRocks 3.2+): force planner to use a specific MV
SELECT /*+ MATERIALIZED_VIEW(mv_daily_revenue) */
    order_date,
    region,
    SUM(amount)
FROM orders_partitioned
GROUP BY order_date, region;
```

### Staleness-Tolerant Rewrite

For dashboards where slightly stale data is acceptable, allow the planner to rewrite even when the MV has not yet refreshed the latest base-table partitions:

```sql
ALTER MATERIALIZED VIEW mv_daily_revenue
SET ("mv_rewrite_staleness_second" = "3600");
```

With `mv_rewrite_staleness_second = 3600`, the planner rewrites to the MV as long as the stale partitions are no older than 1 hour. Partitions outside that window fall back to direct base-table scan.

---

## MV on External Tables (Iceberg / Hive)

Asynchronous MVs can be created over external catalog tables (Iceberg, Hive, JDBC). The MV is stored as an internal StarRocks table and refreshed from the external source.

### Prerequisites

```sql
-- Create an Iceberg external catalog (done once by admin)
CREATE EXTERNAL CATALOG iceberg_prod
PROPERTIES (
    "type"                        = "iceberg",
    "iceberg.catalog.type"        = "hive",
    "hive.metastore.uris"         = "thrift://metastore-host:9083",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region"               = "us-east-1"
);
```

### Create MV on Iceberg Table

```sql
-- Reference the external table with catalog.db.table notation
CREATE MATERIALIZED VIEW mv_iceberg_daily_sales
COMMENT 'Daily sales aggregated from Iceberg lakehouse'
PARTITION BY date_trunc('day', event_date)
DISTRIBUTED BY HASH(store_id) BUCKETS 16
REFRESH MANUAL
PROPERTIES (
    "replication_num"               = "3",
    "partition_refresh_number"      = "2",
    "auto_refresh_partitions_limit" = "30"
)
AS
SELECT
    date_trunc('day', event_date)  AS event_day,
    store_id,
    product_category,
    SUM(sale_amount)               AS total_sales,
    COUNT(*)                       AS transaction_count
FROM iceberg_prod.warehouse.sales_events
GROUP BY
    date_trunc('day', event_date),
    store_id,
    product_category;
```

### Refresh from Airflow after Iceberg Compaction

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

# Refresh only the last 7 days of partitions
refresh_iceberg_mv = SQLExecuteQueryOperator(
    task_id="refresh_mv_iceberg_sales",
    conn_id="starrocks_default",
    sql="""
        REFRESH MATERIALIZED VIEW mv_iceberg_daily_sales
        PARTITION START ('{{ macros.ds_add(ds, -6) }}') END ('{{ tomorrow_ds }}');
    """,
)
```

### MV on Hive External Table

```sql
CREATE EXTERNAL CATALOG hive_legacy
PROPERTIES (
    "type"               = "hive",
    "hive.metastore.uris"= "thrift://hive-metastore:9083"
);

CREATE MATERIALIZED VIEW mv_hive_user_profile
DISTRIBUTED BY HASH(user_id) BUCKETS 32
REFRESH ASYNC START('2026-01-01 03:00:00') EVERY(INTERVAL 1 DAY)
AS
SELECT
    u.user_id,
    u.signup_date,
    u.country,
    COUNT(DISTINCT o.order_id) AS lifetime_orders,
    SUM(o.amount)              AS lifetime_revenue
FROM hive_legacy.default.users   AS u
LEFT JOIN hive_legacy.default.orders AS o ON u.user_id = o.user_id
GROUP BY u.user_id, u.signup_date, u.country;
```

Query rewrite works for MVs on external tables: queries referencing `iceberg_prod.warehouse.sales_events` can rewrite to `mv_iceberg_daily_sales` when the MV satisfies all rewrite conditions.

---

## Nested MVs

A nested MV is an asynchronous MV defined over another asynchronous MV as its base "table". StarRocks 3.x supports this for multi-level pre-aggregation.

### Use Case

Hourly raw events → daily MV → monthly MV. Each level aggregates further, so the monthly dashboard never scans raw data.

### Example: Two-Level Aggregation

```sql
-- Level 1: daily MV on raw events
CREATE MATERIALIZED VIEW mv_events_daily_agg
PARTITION BY event_day
DISTRIBUTED BY HASH(user_id) BUCKETS 32
REFRESH ASYNC EVERY(INTERVAL 1 HOUR)
PROPERTIES ("replication_num" = "3")
AS
SELECT
    DATE(event_ts)   AS event_day,
    user_id,
    event_type,
    COUNT(*)         AS event_count,
    SUM(value)       AS total_value
FROM raw_events
GROUP BY DATE(event_ts), user_id, event_type;

-- Level 2: monthly MV on the daily MV
CREATE MATERIALIZED VIEW mv_events_monthly_agg
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
REFRESH ASYNC EVERY(INTERVAL 1 DAY)
PROPERTIES ("replication_num" = "3")
AS
SELECT
    date_trunc('month', event_day) AS event_month,
    user_id,
    event_type,
    SUM(event_count)               AS event_count,
    SUM(total_value)               AS total_value
FROM mv_events_daily_agg           -- references the Level 1 MV
GROUP BY date_trunc('month', event_day), user_id, event_type;
```

### Refresh Dependency Order

StarRocks automatically detects the dependency chain when `REFRESH ASYNC` triggers are used. However, for `REFRESH MANUAL` or orchestrated pipelines, refresh in bottom-up order:

```sql
-- Step 1: refresh Level 1 MV first
REFRESH MATERIALIZED VIEW mv_events_daily_agg;

-- Step 2: refresh Level 2 MV after Level 1 is complete
REFRESH MATERIALIZED VIEW mv_events_monthly_agg;
```

In Airflow, chain the tasks explicitly:

```python
refresh_daily  >> refresh_monthly
```

### Nested MV Rewrite

The planner can rewrite a monthly-granularity query directly to `mv_events_monthly_agg` without touching `mv_events_daily_agg` or `raw_events`, provided all rewrite conditions are met.

---

## MV Management Commands

### List All MVs

```sql
-- All MVs in the current database
SHOW MATERIALIZED VIEWS;

-- MVs in a specific database
SHOW MATERIALIZED VIEWS IN sales_db;

-- Filter by name pattern
SHOW MATERIALIZED VIEWS IN sales_db LIKE 'mv_orders%';
```

Columns returned:

| Column | Description |
|---|---|
| `id` | Internal MV object ID |
| `name` | MV name |
| `database_name` | Database the MV belongs to |
| `text` | MV definition SQL (the AS SELECT ...) |
| `rows` | Approximate row count |
| `refresh_type` | `ASYNC`, `MANUAL`, or `SYNC` |
| `is_active` | `true` / `false` — `false` means rewrite is disabled |
| `last_refresh_start_time` | Timestamp of the most recent refresh start |
| `last_refresh_finished_time` | Timestamp of the most recent refresh completion |
| `last_refresh_duration` | Duration of the last refresh in seconds |
| `last_refresh_state` | `SUCCESS`, `FAILED`, `RUNNING`, or `PENDING` |
| `inactive_reason` | Reason for `is_active = false`, e.g. base table altered |

### Query MV Metadata from information_schema

```sql
-- Programmatic MV inventory
SELECT
    TABLE_NAME        AS mv_name,
    REFRESH_TYPE,
    IS_ACTIVE,
    LAST_REFRESH_START_TIME,
    LAST_REFRESH_FINISHED_TIME,
    LAST_REFRESH_STATE,
    INACTIVE_REASON
FROM information_schema.materialized_views
WHERE TABLE_SCHEMA = 'sales_db'
ORDER BY LAST_REFRESH_START_TIME DESC;
```

### Alter Refresh Schedule

```sql
-- Change from manual to hourly async
ALTER MATERIALIZED VIEW mv_monthly_cohort
REFRESH ASYNC EVERY(INTERVAL 1 HOUR);

-- Change to daily at 02:00 UTC
ALTER MATERIALIZED VIEW mv_order_item_daily
REFRESH ASYNC START('2026-01-01 02:00:00') EVERY(INTERVAL 1 DAY);

-- Pause automated refresh (switch to manual)
ALTER MATERIALIZED VIEW mv_order_item_daily
REFRESH MANUAL;

-- Update properties without changing refresh mode
ALTER MATERIALIZED VIEW mv_daily_revenue
SET (
    "partition_refresh_number"      = "5",
    "auto_refresh_partitions_limit" = "60",
    "mv_rewrite_staleness_second"   = "1800"
);

-- Rename an MV
ALTER MATERIALIZED VIEW mv_daily_revenue RENAME mv_daily_revenue_v2;

-- Reactivate an inactive MV (e.g., after base table schema fix)
ALTER MATERIALIZED VIEW mv_daily_revenue ACTIVE;
```

### Manually Trigger Refresh

```sql
-- Full refresh (all partitions)
REFRESH MATERIALIZED VIEW mv_daily_revenue;

-- Partition range refresh (only 2026-05)
REFRESH MATERIALIZED VIEW mv_daily_revenue
PARTITION START ('2026-05-01') END ('2026-06-01');

-- Force refresh even if StarRocks thinks data is fresh
REFRESH MATERIALIZED VIEW mv_daily_revenue FORCE;

-- Partition range force refresh
REFRESH MATERIALIZED VIEW mv_daily_revenue
PARTITION START ('2026-05-01') END ('2026-06-01') FORCE;
```

### Cancel a Running Refresh

```sql
-- Cancel in-progress refresh job
CANCEL REFRESH MATERIALIZED VIEW mv_daily_revenue;
```

### Drop an Asynchronous MV

```sql
-- Drop the MV and all its data
DROP MATERIALIZED VIEW mv_daily_revenue;

-- Drop only if it exists (idempotent)
DROP MATERIALIZED VIEW IF EXISTS mv_daily_revenue;
```

---

## Production Example — Daily Sales Dashboard MV

This end-to-end example shows the complete workflow for a production BI dashboard accelerated by a partition-aware async MV.

### Base Tables

```sql
-- Fact table: high-volume orders
CREATE TABLE fct_orders (
    order_id      BIGINT         NOT NULL,
    order_date    DATE           NOT NULL,
    region        VARCHAR(32)    NOT NULL,
    channel       VARCHAR(32)    NOT NULL,
    user_id       INT            NOT NULL,
    store_id      INT            NOT NULL,
    amount        DECIMAL(18,2)  NOT NULL,
    quantity      INT            NOT NULL
)
PRIMARY KEY(order_id)
PARTITION BY RANGE(order_date)(
    START ("2025-01-01") END ("2027-01-01") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(order_id) BUCKETS 64
PROPERTIES (
    "replication_num"            = "3",
    "enable_persistent_index"    = "true"
);

-- Dimension: store
CREATE TABLE dim_store (
    store_id    INT           NOT NULL,
    store_name  VARCHAR(128),
    city        VARCHAR(64),
    country     VARCHAR(32)
)
PRIMARY KEY(store_id)
DISTRIBUTED BY HASH(store_id) BUCKETS 4
PROPERTIES ("replication_num" = "3");
```

### Asynchronous MV with Partition-Aware Refresh

```sql
CREATE MATERIALIZED VIEW mv_sales_dashboard_daily
COMMENT 'Daily KPIs for the sales BI dashboard — refreshed nightly at 01:00 UTC'
PARTITION BY order_date
DISTRIBUTED BY HASH(region, channel) BUCKETS 16
ORDER BY (order_date, region, channel)
REFRESH ASYNC START('2026-01-01 01:00:00') EVERY(INTERVAL 1 DAY)
PROPERTIES (
    "replication_num"               = "3",
    "partition_refresh_number"      = "7",
    "auto_refresh_partitions_limit" = "90",
    "mv_rewrite_staleness_second"   = "7200"
)
AS
SELECT
    o.order_date,
    o.region,
    o.channel,
    s.country,
    s.city,
    COUNT(DISTINCT o.user_id)     AS unique_buyers,
    COUNT(DISTINCT o.order_id)    AS order_count,
    SUM(o.amount)                 AS gross_revenue,
    SUM(o.quantity)               AS units_sold,
    SUM(o.amount) / COUNT(DISTINCT o.order_id) AS avg_order_value
FROM fct_orders  AS o
INNER JOIN dim_store AS s ON o.store_id = s.store_id
GROUP BY
    o.order_date,
    o.region,
    o.channel,
    s.country,
    s.city;
```

### Verify Creation

```sql
-- Monitor the initial build
SHOW MATERIALIZED VIEWS IN sales_db LIKE 'mv_sales_dashboard_daily';

-- Check partitions are populated
SHOW PARTITIONS FROM mv_sales_dashboard_daily;
```

### Confirm Query Rewrite

```sql
-- Typical dashboard query
EXPLAIN
SELECT
    order_date,
    region,
    channel,
    SUM(gross_revenue) AS revenue,
    SUM(order_count)   AS orders
FROM mv_sales_dashboard_daily
WHERE order_date BETWEEN '2026-04-01' AND '2026-04-30'
GROUP BY order_date, region, channel
ORDER BY order_date, revenue DESC;
```

Expected plan fragment (rewritten):

```
PLAN FRAGMENT 0
  OUTPUT EXPRS: order_date | region | channel | revenue | orders
  ...
  OlapScanNode
    TABLE: mv_sales_dashboard_daily
    PREAGGREGATION: ON
    PREDICATES: order_date >= '2026-04-01', order_date <= '2026-04-30'
    partitions=30/730
    rollup: mv_sales_dashboard_daily
```

The key indicators are:
- `TABLE: mv_sales_dashboard_daily` — MV is being scanned, not the base tables.
- `partitions=30/730` — partition pruning is active (30 partitions out of 730 total).
- `PREAGGREGATION: ON` — the aggregate pushdown is using the pre-computed columns.

When a user query directly references the base table `fct_orders` with the same aggregation pattern, the planner rewrites it to `mv_sales_dashboard_daily` transparently:

```sql
-- User query hitting fct_orders — will rewrite to mv_sales_dashboard_daily
EXPLAIN
SELECT
    o.order_date,
    o.region,
    o.channel,
    s.country,
    COUNT(DISTINCT o.order_id)    AS order_count,
    SUM(o.amount)                 AS gross_revenue
FROM fct_orders  AS o
INNER JOIN dim_store AS s ON o.store_id = s.store_id
WHERE o.order_date BETWEEN '2026-04-01' AND '2026-04-30'
GROUP BY o.order_date, o.region, o.channel, s.country;
```

```
  OlapScanNode
    TABLE: mv_sales_dashboard_daily     <-- transparent rewrite
    PREDICATES: order_date >= '2026-04-01', order_date <= '2026-04-30'
    partitions=30/730
```

### Incremental Partition Refresh from Airflow

```python
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum

@dag(
    dag_id="refresh_mv_sales_dashboard",
    schedule="0 1 * * *",              # 01:00 UTC daily
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["starrocks", "mv", "sales"],
)
def refresh_mv_sales_dashboard():

    refresh_mv = SQLExecuteQueryOperator(
        task_id="refresh_mv_partition",
        conn_id="starrocks_default",
        sql="""
            REFRESH MATERIALIZED VIEW mv_sales_dashboard_daily
            PARTITION START ('{{ macros.ds_add(ds, -1) }}') END ('{{ ds }}');
        """,
    )

    check_mv = SQLExecuteQueryOperator(
        task_id="check_mv_state",
        conn_id="starrocks_default",
        sql="""
            SELECT last_refresh_state
            FROM information_schema.materialized_views
            WHERE table_name   = 'mv_sales_dashboard_daily'
              AND table_schema = 'sales_db';
        """,
    )

    refresh_mv >> check_mv

refresh_mv_sales_dashboard()
```

---

## MV vs Query Cache vs Partition Pruning

Use this table to decide which acceleration technique fits each scenario.

| Scenario | Recommended technique | Reason |
|---|---|---|
| Dashboard always queries the same aggregation over the last N days | **Asynchronous MV** | Pre-computation eliminates repeated aggregation; rewrite is transparent |
| Ad-hoc queries with highly variable filters and GROUP BY columns | **Query cache** (`enable_query_cache = true`) | MV can't be defined for every possible query shape; cache caches result sets |
| Table has hundreds of partitions but queries always filter to a small date range | **Partition pruning** (design) | Ensure partition column is used directly in WHERE; no MV needed if scan is already small |
| Single-table aggregate query, always consistent with base table | **Synchronous MV (rollup)** | Zero-lag; automatically maintained on every write |
| Multi-table join result, slightly stale is OK | **Asynchronous MV** | Scheduled refresh; rewrite handles the join materialization |
| Iceberg/Hive lakehouse; avoid repeated remote metadata + data reads | **Async MV on external table** | Pulls data into StarRocks storage; subsequent queries are local |
| One-off heavy computation, result used once | **No MV** — use a temp table or `CREATE TABLE AS SELECT` | MV maintenance overhead is wasted on a single-use result |
| Query is fast enough after partition pruning (< 200 ms) | **No MV** | Adding an MV incurs write amplification and refresh cost without meaningful gain |

---

## Anti-Patterns

### 1. Over-Creating Synchronous MVs

**Problem:** Each synchronous MV is a rollup maintained on every write. Creating 20 rollups on a high-write fact table multiplies write I/O by 20x.

**Rule:** Keep synchronous MVs to 3–5 per table. Cover the highest-frequency query patterns only.

### 2. Refresh Interval Too Aggressive

**Problem:** Setting `EVERY(INTERVAL 5 MINUTE)` on a large multi-table MV causes refresh jobs to queue behind each other, consuming BE memory and causing slow refreshes that starve query resources.

**Fix:** Match the refresh interval to the business SLA (SLA = 1 hour → `EVERY(INTERVAL 1 HOUR)`). For very large MVs, use `REFRESH MANUAL` and trigger from Airflow with proper task timeout enforcement.

### 3. Rewrite Silently Not Used

**Problem:** The MV exists but queries still scan base tables. No error is raised — the optimizer just falls back.

**Diagnosis steps:**
```sql
-- Step 1: confirm MV is active
SHOW MATERIALIZED VIEWS LIKE 'mv_sales_dashboard_daily';
-- is_active must be 'true'

-- Step 2: check last refresh state
SELECT last_refresh_state, last_refresh_finished_time, inactive_reason
FROM information_schema.materialized_views
WHERE table_name = 'mv_sales_dashboard_daily';

-- Step 3: verbose explain to see rejection reason
EXPLAIN VERBOSE
SELECT ... FROM fct_orders JOIN dim_store ...;
-- Look for the MaterializedView section and RewriteFail reason
```

Common fixes: reactivate with `ALTER MATERIALIZED VIEW mv ACTIVE`, run `REFRESH MATERIALIZED VIEW mv`, or add missing columns to the MV SELECT list.

### 4. Missing Partition Column in MV SELECT

**Problem:** Defining `PARTITION BY order_date` in the MV DDL but not including `order_date` in the SELECT list causes partition-aware refresh to fail.

**Rule:** Any column referenced in `PARTITION BY`, `DISTRIBUTED BY`, or `ORDER BY` must appear explicitly in the SELECT list of the MV query.

### 5. Stale Data Undetected in Production

**Problem:** Downstream BI tools query the MV without knowing the last refresh time. A refresh failure goes undetected for hours.

**Fix:** Add a data freshness check to your dashboard or monitoring pipeline:

```sql
SELECT
    last_refresh_finished_time,
    TIMESTAMPDIFF(MINUTE, last_refresh_finished_time, NOW()) AS minutes_stale,
    last_refresh_state
FROM information_schema.materialized_views
WHERE table_name = 'mv_sales_dashboard_daily';
```

Alert when `minutes_stale > threshold` or `last_refresh_state != 'SUCCESS'`.

### 6. Using COUNT(DISTINCT) in Async MV and Expecting Rewrite of COUNT(DISTINCT)

**Problem:** An MV with `COUNT(DISTINCT user_id) AS unique_users` cannot transparently rewrite a query that also requests `COUNT(DISTINCT user_id)` unless the MV stores the exact result for the exact grouping. The planner cannot reuse a `COUNT(DISTINCT)` from a coarser grouping to answer a finer-grained query.

**Fix:** For approximate distinct counts at multiple granularities, use `BITMAP_UNION(TO_BITMAP(user_id))` in the MV and `BITMAP_UNION_COUNT(...)` in the query. Bitmap aggregates are re-aggregatable across groupings.

```sql
-- MV with bitmap for reusable distinct counts
CREATE MATERIALIZED VIEW mv_orders_bitmap_daily
PARTITION BY order_date
DISTRIBUTED BY HASH(region) BUCKETS 16
REFRESH ASYNC EVERY(INTERVAL 1 HOUR)
AS
SELECT
    order_date,
    region,
    channel,
    BITMAP_UNION(TO_BITMAP(user_id)) AS user_id_bitmap,
    COUNT(*)                          AS order_count,
    SUM(amount)                       AS total_amount
FROM fct_orders
GROUP BY order_date, region, channel;

-- Query: distinct users per region (rewrite to MV possible)
SELECT
    region,
    BITMAP_UNION_COUNT(user_id_bitmap) AS unique_users
FROM mv_orders_bitmap_daily
WHERE order_date BETWEEN '2026-04-01' AND '2026-04-30'
GROUP BY region;
```

### 7. Dropping Base Table Without Dropping MV First

**Problem:** Dropping a base table leaves the MV in `INACTIVE` state with a broken reference. Inactive MVs are never used for query rewrite and waste storage.

**Rule:** Always drop dependent MVs before dropping or renaming base tables:

```sql
DROP MATERIALIZED VIEW IF EXISTS mv_daily_revenue;
DROP TABLE fct_orders;
```

### 8. Refreshing All Partitions When Only Recent Data Changed

**Problem:** Calling `REFRESH MATERIALIZED VIEW mv` without a `PARTITION START/END` clause triggers a full refresh of all partitions, consuming unnecessary compute and time.

**Fix:** For time-series MVs, always specify the partition window that covers changed data:

```sql
-- Refresh only yesterday and today
REFRESH MATERIALIZED VIEW mv_sales_dashboard_daily
PARTITION START ('{{ yesterday }}') END ('{{ today }}');
```

---

## References

- StarRocks 3.x Documentation — Materialized Views Overview: `https://docs.starrocks.io/docs/using_starrocks/Materialized_view/`
- StarRocks 3.x Documentation — Synchronous Materialized Views: `https://docs.starrocks.io/docs/using_starrocks/Materialized_view-single_table/`
- StarRocks 3.x Documentation — Asynchronous Materialized Views: `https://docs.starrocks.io/docs/using_starrocks/async_mv/Materialized_view/`
- StarRocks 3.x Documentation — Query Rewrite with MVs: `https://docs.starrocks.io/docs/using_starrocks/query_rewrite_with_materialized_views/`
- StarRocks 3.x Documentation — CREATE MATERIALIZED VIEW (DDL reference): `https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/`
- StarRocks 3.x Documentation — External Catalogs (Iceberg, Hive): `https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog/`
- `skills/trino_iceberg/SKILL.md` — Iceberg table design, partition transforms, and maintenance patterns relevant when the MV source is an Iceberg external catalog.
- `skills/airflow_dags/SKILL.md` — Airflow DAG authoring for orchestrating MV refresh pipelines.
