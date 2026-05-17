---
name: starrocks-schema-evolution
description: StarRocks schema evolution — ALTER TABLE ADD/DROP/MODIFY/REORDER COLUMN, column type widening rules, adding columns to Aggregate/Unique/Primary Key tables, schema change types (direct vs linked vs sorted), SHOW ALTER TABLE COLUMN progress, rollback safety, backward-compatible patterns, dbt model evolution, zero-downtime migration checklist
---

# StarRocks — Schema Evolution

## When to Use

Load this skill when the user needs to:
- Add new metric or dimension columns to an existing StarRocks table
- Widen a column type (INT → BIGINT, VARCHAR(64) → VARCHAR(256), FLOAT → DOUBLE)
- Rename or reorder columns without breaking downstream queries
- Drop obsolete columns safely
- Monitor an in-progress schema change job and cancel it if needed
- Execute zero-downtime schema migrations in production
- Handle schema drift in dbt models backed by StarRocks tables
- Backfill a new column with a default value across partitioned tables

---

## Schema Change Types in StarRocks

StarRocks 3.x classifies every `ALTER TABLE COLUMN` job into one of three internal change types. The type determines cost (latency, I/O, resource usage) and rollback safety.

### Direct Schema Change (Instant / Metadata Only)

Metadata-level operation — no tablet data is rewritten.

| Operation | Detail |
|---|---|
| ADD COLUMN (nullable, no default) | New column added to schema metadata; existing rows read `NULL` |
| ADD COLUMN with DEFAULT on Primary Key tables | Metadata-only if the default is a constant; rows return the constant without rewrite |
| MODIFY COLUMN comment | Pure metadata, zero data I/O |
| Rename column (`MODIFY COLUMN … RENAME`) | Metadata-only in StarRocks 3.2+; older versions require a linked change |

**Impact:** Commits in seconds. No progress in `SHOW ALTER TABLE COLUMN` beyond FINISHED. Safe to run during peak hours.

### Linked Schema Change (Background Backfill)

A background job rewrites affected column data incrementally, tablet by tablet.

| Operation | Detail |
|---|---|
| ADD COLUMN with non-null DEFAULT (Duplicate/Aggregate/Unique Key) | New column values backfilled from the constant; old replicas updated in place |
| MODIFY COLUMN type widening (INT → BIGINT, VARCHAR(32) → VARCHAR(128)) | Each row is re-encoded with the wider type; compacted on next merge |
| DROP COLUMN (non-key) | Column is removed from each tablet data file during the next compaction cycle |

**Impact:** Background job runs while the table stays fully readable and writable. Ingestion continues. Duration scales with data volume. Monitor with `SHOW ALTER TABLE COLUMN`.

### Sorted Schema Change (Full Data Rewrite)

Requires full tablet rewrite because the physical sort order changes.

| Operation | Detail |
|---|---|
| ADD COLUMN to key prefix / change sort key (`ORDER BY`) | All tablet data rewritten to establish new sort order |
| Reorder key columns (`REORDER COLUMNS`) | Physical row order changes — full rewrite |
| Changing the `DISTRIBUTED BY` expression | Not allowed via ALTER; requires `CREATE TABLE … AS SELECT` |

**Impact:** High I/O, proportional to total table size. Table remains available but with degraded write performance. **Plan a maintenance window** for large tables.

---

## ADD COLUMN

### Basic Syntax

```sql
-- Add a single nullable column (direct change — instant)
ALTER TABLE orders
ADD COLUMN is_fraud BOOLEAN NULL COMMENT 'Fraud flag from ML model';

-- Add a column with a default value at a specific position
ALTER TABLE orders
ADD COLUMN discount_amount DECIMAL(12, 2) DEFAULT 0.00
AFTER order_amount
COMMENT 'Promotional discount applied at order time';

-- Add a column as the first non-key column
ALTER TABLE events
ADD COLUMN ingest_version TINYINT DEFAULT 1 FIRST;
```

### Adding Multiple Columns in One ALTER

Batching columns into a single `ALTER TABLE` statement is strongly preferred: StarRocks executes them as one schema-change job, minimising the number of tablet rewrites.

```sql
ALTER TABLE fact_sales
ADD COLUMN region_code     VARCHAR(8)     DEFAULT ''   AFTER country_code,
ADD COLUMN channel         VARCHAR(32)    DEFAULT 'unknown' AFTER region_code,
ADD COLUMN gross_margin    DECIMAL(18, 4) NULL         AFTER revenue,
ADD COLUMN is_promo        BOOLEAN        DEFAULT FALSE AFTER gross_margin;
```

### Aggregate Key Table — Specifying Aggregate Function

For Aggregate Key tables, every non-key column must carry an aggregate function. When adding a column you must include it in the `ALTER TABLE` statement.

```sql
-- Table definition (reference):
-- CREATE TABLE agg_daily_metrics (
--     dt DATE, user_id BIGINT,                  -- key columns
--     session_cnt BIGINT SUM, ...               -- value columns
-- ) AGGREGATE KEY(dt, user_id)

ALTER TABLE agg_daily_metrics
ADD COLUMN ad_clicks BIGINT SUM DEFAULT 0
AFTER session_cnt
COMMENT 'Ad click count, summed during compaction';

-- HLL and BITMAP aggregate columns
ALTER TABLE agg_daily_metrics
ADD COLUMN uv_hll HLL HLL_UNION DEFAULT HLL_EMPTY() AFTER ad_clicks;

ALTER TABLE agg_daily_metrics
ADD COLUMN active_users_bitmap BITMAP BITMAP_UNION DEFAULT BITMAP_EMPTY() AFTER uv_hll;
```

> Omitting the aggregate function on a new value column raises `ERROR 1064: Aggregate function is required for value column`.

### Unique Key Table

Unique Key tables store the latest row per key (REPLACE semantics). Adding a value column follows standard syntax — no aggregate function required, but `NULL` or `DEFAULT` is strongly recommended so existing rows have a defined value.

```sql
ALTER TABLE dim_customer
ADD COLUMN loyalty_tier   VARCHAR(16) DEFAULT 'bronze' AFTER signup_date,
ADD COLUMN gdpr_deleted   BOOLEAN     DEFAULT FALSE    AFTER loyalty_tier;
```

### Primary Key Table

Primary Key tables support full DML. Any new column must be either `NULL` or carry a `DEFAULT` value; otherwise existing rows cannot be read consistently.

```sql
ALTER TABLE orders_pk
ADD COLUMN fulfillment_sla_hours SMALLINT  DEFAULT 48   AFTER shipping_method,
ADD COLUMN last_modified_at      DATETIME  NULL         AFTER fulfillment_sla_hours;
```

For Primary Key tables, StarRocks 3.1+ treats constant-default ADD COLUMN as a direct (instant) change — no background rewrite occurs.

---

## DROP COLUMN

### Syntax

```sql
-- Drop a single non-key column
ALTER TABLE fact_sales
DROP COLUMN legacy_promo_code;

-- Drop multiple columns in one job
ALTER TABLE fact_sales
DROP COLUMN legacy_promo_code,
DROP COLUMN deprecated_flag,
DROP COLUMN tmp_etl_batch;
```

### Constraints

| Constraint | Explanation |
|---|---|
| Cannot drop key columns | Key columns (DUPLICATE KEY / AGGREGATE KEY / UNIQUE KEY / PRIMARY KEY prefix) define sort order and deduplication — they are structural |
| Cannot drop the last non-key column | A table must have at least one value column |
| Cannot drop a column referenced by a synchronous materialized view | The MV will be invalidated; drop or refresh the MV first |
| Cannot drop a column used in a generated column expression | Remove or alter the generated column first |

### Immediate vs Deferred Removal

Logically, the column disappears from `DESCRIBE` and query planning immediately after the job reaches `FINISHED` state. Physically, column data is removed from tablet files during the next background compaction. Disk space is reclaimed lazily — do not rely on DROP COLUMN for immediate storage reclamation on large tables.

### Synchronous MV Impact

```sql
-- Identify MVs that reference a column before dropping it
SHOW CREATE MATERIALIZED VIEW mv_order_summary\G

-- If the MV references the column you plan to drop:
DROP MATERIALIZED VIEW IF EXISTS mv_order_summary;

-- Then proceed with the column drop
ALTER TABLE fact_sales DROP COLUMN old_status_code;

-- Recreate the MV if needed, excluding the dropped column
CREATE MATERIALIZED VIEW mv_order_summary
AS SELECT order_date, user_id, SUM(amount) AS total_amount
   FROM fact_sales
   GROUP BY order_date, user_id;
```

---

## MODIFY COLUMN (Type Widening)

### Allowed Widenings

| From | To | Notes |
|---|---|---|
| `TINYINT` | `SMALLINT`, `INT`, `BIGINT`, `LARGEINT` | Signed integer widening |
| `SMALLINT` | `INT`, `BIGINT`, `LARGEINT` | |
| `INT` | `BIGINT`, `LARGEINT` | Most common migration |
| `BIGINT` | `LARGEINT` | |
| `FLOAT` | `DOUBLE` | Precision preserved |
| `VARCHAR(n)` | `VARCHAR(m)` where m > n | Length widening only |
| `CHAR(n)` | `VARCHAR(m)` where m ≥ n | CHAR → VARCHAR is allowed |
| `DATE` | `DATETIME` | Promotes date to datetime at midnight |

### Forbidden Modifications

| Operation | Reason |
|---|---|
| Type narrowing (BIGINT → INT) | Would silently truncate existing data |
| Changing key column type | Key columns define physical sort and distribution — changing type requires a full table rebuild |
| DECIMAL precision/scale change | StarRocks treats scale changes as incompatible |
| Any type change that alters sort semantics | Would invalidate the sorted tablet structure |
| Changing aggregate function on Aggregate Key column | Changing SUM → MAX semantics without rebuilding history is undefined behaviour |

### Syntax

```sql
-- Widen INT to BIGINT
ALTER TABLE fact_orders
MODIFY COLUMN order_id BIGINT NOT NULL COMMENT 'Widened from INT';

-- Widen VARCHAR
ALTER TABLE dim_product
MODIFY COLUMN description VARCHAR(1024) NULL;

-- FLOAT → DOUBLE
ALTER TABLE sensor_readings
MODIFY COLUMN temperature DOUBLE NULL;

-- DATE → DATETIME (adds time component, always 00:00:00 for existing rows)
ALTER TABLE events
MODIFY COLUMN event_date DATETIME NOT NULL;
```

> When widening a `NOT NULL` column you must repeat `NOT NULL` in the new definition; StarRocks does not inherit nullability automatically.

### MODIFY COLUMN on Aggregate Key

You may widen the type of a value column, but the aggregate function must be restated and must not change:

```sql
ALTER TABLE agg_daily_metrics
MODIFY COLUMN session_cnt BIGINT SUM DEFAULT 0;
-- SUM must match the original aggregate function
```

---

## REORDER COLUMNS

`ORDER BY` controls the prefix index (sort key) StarRocks builds per tablet. Reordering changes physical row layout — this is always a sorted schema change (full rewrite).

```sql
-- Original ORDER BY: (event_date, event_ts, user_id)
-- New intent: query by (user_id, event_date) most frequently

ALTER TABLE page_events
ORDER BY (user_id, event_date, event_ts);
```

For Primary Key tables the sort key is always identical to the primary key — reordering is not supported unless you are changing the entire primary key definition (which requires `CREATE TABLE … AS SELECT`).

For Duplicate Key and Unique Key tables with an explicit `ORDER BY` clause (StarRocks 3.0+), you can change the sort key independently from the key columns:

```sql
-- Duplicate Key table: key columns unchanged, sort order changed
ALTER TABLE ods.page_events
ORDER BY (user_id, event_date, event_ts, event_id);
-- event_id is still in DUPLICATE KEY() but moved to the end of sort
```

---

## Monitoring Schema Changes

### SHOW ALTER TABLE COLUMN

```sql
-- Show all schema change jobs for the current database
SHOW ALTER TABLE COLUMN;

-- Filter to a specific table
SHOW ALTER TABLE COLUMN
WHERE TableName = 'fact_orders';

-- Include finished/cancelled jobs (default shows recent N jobs)
SHOW ALTER TABLE COLUMN
WHERE TableName = 'fact_orders'
ORDER BY CreateTime DESC
LIMIT 20;
```

Key columns in the output:

| Column | Description |
|---|---|
| `JobId` | Unique job identifier |
| `TableName` | Target table |
| `CreateTime` | When the job was submitted |
| `FinishedTime` | Completion time (NULL while running) |
| `SchemaChangeType` | `DIRECT` / `LINKED` / `SORT` |
| `State` | See state machine below |
| `Progress` | Percentage of tablets completed (RUNNING state only) |
| `Msg` | Error message if CANCELLED |

### Job State Machine

```
PENDING → WAITING_TXN → RUNNING → FINISHED
                   ↓              ↓
               CANCELLED      CANCELLED
```

| State | Meaning |
|---|---|
| `PENDING` | Job submitted; waiting for the FE leader to schedule |
| `WAITING_TXN` | Waiting for in-flight load transactions to commit before starting |
| `RUNNING` | Background tablet rewrite in progress; `Progress` field updates |
| `FINISHED` | All tablets rewritten; schema is live |
| `CANCELLED` | Job was cancelled or failed; original schema intact |

### Polling a Job in Shell

```bash
# Poll until the job finishes or is cancelled
watch -n 5 "mysql -h sr-fe -P 9030 -u root -e \
  \"SHOW ALTER TABLE COLUMN WHERE TableName = 'fact_orders'\" 2>/dev/null \
  | awk '{print \$1, \$7, \$8, \$9}'"
```

### Cancel a Running Job

```sql
-- Cancel by table name (cancels the active job on that table)
CANCEL ALTER TABLE COLUMN ON fact_orders;

-- Verify cancellation
SHOW ALTER TABLE COLUMN
WHERE TableName = 'fact_orders'
ORDER BY CreateTime DESC
LIMIT 1;
-- State should be CANCELLED; original schema is restored
```

Cancellation is safe: StarRocks keeps the original tablet data intact during the job. On cancellation the new tablet copies are discarded.

---

## Zero-Downtime Patterns

### Pattern 1 — Adding Nullable Columns (Zero Downtime, Instant)

No downtime. The column becomes available immediately after the DDL commits.

```sql
-- Add nullable column — direct schema change
ALTER TABLE fact_orders
ADD COLUMN predicted_churn_score FLOAT NULL COMMENT 'ML model output, backfilled nightly';
```

Downstream queries that don't reference the column are unaffected. New writes can immediately populate the column. Old rows return `NULL` until a backfill job sets values.

### Pattern 2 — Adding Columns with DEFAULT (Near-Zero Downtime)

Reads return the DEFAULT value instantly; background job fills tablets at rest.

```sql
ALTER TABLE fact_orders
ADD COLUMN order_channel VARCHAR(32) DEFAULT 'web' AFTER user_id;
```

Table remains fully readable and writable throughout. The backfill is tablet-granular and low-priority. In StarRocks 3.1+ on Primary Key tables, this is a direct (instant) change — no background job at all.

### Pattern 3 — Type Widening (Near-Zero Downtime)

Background rewrite, but the table stays online.

```sql
-- Step 1: Widen the column
ALTER TABLE fact_orders
MODIFY COLUMN order_amount DECIMAL(18, 4) NULL;

-- Step 2: Monitor until FINISHED
SHOW ALTER TABLE COLUMN WHERE TableName = 'fact_orders';

-- Step 3: Update downstream DDL/models after FINISHED
```

Queries continue to work during the rewrite because StarRocks dual-reads old tablets (old type) and new tablets (new type) transparently until all tablets finish.

### Pattern 4 — Sort Key Change (Requires Maintenance Window)

Full rewrite. Plan for:
- Duration: estimate ~30 min per 100 GB of tablet data on typical hardware
- Increased I/O on BE nodes during the job
- Temporarily degraded ingest performance

```sql
-- Pre-maintenance: notify dependent services, pause high-volume loaders if possible

-- Execute sort key change
ALTER TABLE ods.clickstream
ORDER BY (user_id, event_date, event_ts);

-- Monitor
SHOW ALTER TABLE COLUMN WHERE TableName = 'clickstream';

-- Post-maintenance: verify row counts match, spot-check queries
SELECT COUNT(*) FROM ods.clickstream;
SELECT user_id, COUNT(*) FROM ods.clickstream
WHERE event_date = CURRENT_DATE()
GROUP BY user_id LIMIT 10;
```

### Pattern 5 — Expand-Contract for Breaking Changes

Use when the column type change is not directly allowed (e.g., changing DECIMAL scale, renaming a column in older StarRocks versions).

```sql
-- Phase 1: Expand — add the new column alongside the old one
ALTER TABLE dim_product
ADD COLUMN price_usd DECIMAL(18, 4) NULL AFTER price;

-- Phase 2: Backfill — populate the new column
UPDATE dim_product
SET price_usd = CAST(price AS DECIMAL(18, 4))
WHERE price_usd IS NULL;

-- (or use an INSERT OVERWRITE / Flink/Spark backfill job for large tables)

-- Phase 3: Migrate writers — update all ETL jobs to write price_usd
-- Phase 4: Migrate readers — update all dashboards/BI to read price_usd
-- Phase 5: Contract — drop the old column once readers/writers are fully migrated
ALTER TABLE dim_product DROP COLUMN price;

-- Phase 6: (Optional) Rename if desired — in StarRocks 3.2+
ALTER TABLE dim_product MODIFY COLUMN price_usd RENAME price;
```

---

## dbt Model Evolution

### StarRocks + dbt Materializations

dbt interacts with StarRocks schema evolution through two mechanisms:
1. **`on_schema_change`** — what dbt does when the target table schema differs from the model SQL
2. **Manual `ALTER TABLE`** — executed outside dbt to pre-stage structural changes

### `on_schema_change` Config

```yaml
# dbt_project.yml or model config block
models:
  my_project:
    marts:
      +materialized: incremental
      +on_schema_change: append_new_columns   # recommended for StarRocks

# In-model config (overrides project-level)
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}
```

| Value | Behaviour |
|---|---|
| `ignore` (default) | Runs the model; columns added to SQL are silently ignored if table already exists |
| `append_new_columns` | Issues `ALTER TABLE ADD COLUMN` for new columns in the SELECT; drops nothing |
| `sync_all_columns` | Adds new columns AND drops columns removed from the SELECT — **use with caution** |
| `fail` | Raises an error if the schema differs; forces explicit migration |

**Recommended default for StarRocks:** `append_new_columns`. StarRocks handles the ALTER TABLE ADD COLUMN as a direct or linked change, keeping the table online. `sync_all_columns` is dangerous because it will `DROP COLUMN` if you remove a field from your SELECT — downstream consumers may break.

### Adding a Column to a dbt-Managed Table

```sql
-- Option A: Let dbt handle it (append_new_columns)
-- 1. Add the column to the model SELECT list with a default/null expression
-- 2. Run: dbt run --select my_model
-- dbt issues: ALTER TABLE my_model ADD COLUMN new_col TYPE ...

-- Option B: Pre-stage the column manually (recommended for large tables)
-- 1. Run ALTER TABLE manually to add column with DEFAULT
ALTER TABLE marts.fact_orders
ADD COLUMN fulfillment_days SMALLINT DEFAULT NULL;

-- 2. Update the dbt model SQL to SELECT the column
-- 3. Run: dbt run --select fact_orders
-- dbt detects column already exists — no ALTER issued, just populates data
```

### Full-Refresh Triggers

A `dbt run --full-refresh` will DROP and CREATE the table from scratch. Use this when:
- A sort key change is needed (not expressible via incremental ALTER)
- Aggregate function on a value column must change
- A column type needs to narrow (not allowed by ALTER)

```bash
# Full refresh — drops the table and rebuilds
dbt run --select fact_orders --full-refresh

# Confirm the table was recreated
dbt show --select fact_orders --limit 5
```

> On large tables, a full refresh can be expensive. Pre-coordinate with downstream consumers.

### dbt Snapshot Schema Evolution

When evolving a snapshot table (SCD2 target), dbt does NOT support `ALTER TABLE` on snapshot tables — it always does a full rebuild. Run with `--full-refresh` and accept the rebuild cost, or manually add columns to the snapshot table and let dbt populate them on the next run.

```bash
dbt snapshot --select scd_dim_customer --full-refresh
```

---

## Backward-Compatible Checklist

Run through this checklist before executing any `ALTER TABLE` in production:

1. **Identify all readers** — query `information_schema.tables` and check BI tools, Flink jobs, Spark jobs, and application queries for references to the target table and column.

2. **Identify synchronous MVs** — run `SHOW CREATE MATERIALIZED VIEW` for each MV on the table; verify the column being dropped/modified is not referenced.

3. **Check active load transactions** — `SHOW PROC '/transactions/<db>'`; the schema change job will wait in `WAITING_TXN` until in-flight loads commit. High-frequency loaders can delay the job start.

4. **Estimate job duration** — for LINKED and SORT changes, estimate duration from table size:
   ```sql
   SELECT SUM(data_size) / 1024 / 1024 / 1024 AS size_gb
   FROM information_schema.partitions
   WHERE table_name = 'fact_orders' AND table_schema = 'marts';
   ```

5. **Test on a dev/staging clone** — execute the ALTER on a representative sample table first; verify `SHOW ALTER TABLE COLUMN` completes with FINISHED.

6. **Backup partition manifest** — for partitioned tables, snapshot `SHOW PARTITIONS FROM <table>` output before the change.

7. **Confirm type widening is allowed** — cross-check the FROM→TO pair against the allowed widening table above. If it is not listed, use the expand-contract pattern instead.

8. **Verify DEFAULT values are semantically correct** — a DEFAULT of `0` on a revenue column may corrupt aggregations; `NULL` is safer when the value is genuinely unknown.

9. **Coordinate writer schema** — if loaders use a schema-aware format (Avro, Protobuf), update producer schemas before or simultaneously with the ALTER.

10. **Have a rollback plan** — for LINKED changes, `CANCEL ALTER TABLE COLUMN ON <table>` is safe and leaves the original schema intact. For SORT changes, prepare a `CREATE TABLE … AS SELECT` restore script.

---

## Anti-Patterns

### Dropping Columns with Active Downstream Queries

```sql
-- ANTI-PATTERN: Drop immediately without coordination
ALTER TABLE fact_orders DROP COLUMN legacy_status;
-- Dashboards and Flink jobs reading legacy_status will fail with
-- "Unknown column 'legacy_status'" immediately after the job finishes
```

**Fix:** Use expand-contract. Keep the column, make new readers use the replacement column, then drop after a safe migration window.

### Changing Aggregate Function on a Live Table

```sql
-- ANTI-PATTERN: Changing SUM → MAX on a column that already has aggregated data
ALTER TABLE agg_metrics MODIFY COLUMN revenue BIGINT MAX DEFAULT 0;
-- StarRocks will reject this: aggregate function cannot be changed on Aggregate Key column
-- Even if accepted in future versions, historical data was summed — using MAX now is meaningless
```

**Fix:** Add a new column with the new aggregate function; backfill from source data, not from the aggregated column.

### Sort Key Change Without Maintenance Window

```sql
-- ANTI-PATTERN: Changing ORDER BY on a 2 TB table during business hours
ALTER TABLE ods.page_events ORDER BY (user_id, event_date, event_ts);
-- Full tablet rewrite for 2 TB → multi-hour I/O spike
-- BE nodes may OOM or throttle, impacting concurrent queries
```

**Fix:** Schedule during off-peak. Pre-scale BE nodes. Use resource groups to cap the ALTER job's CPU/IO if supported.

### Multiple Sequential ALTER TABLE Jobs on the Same Table

```sql
-- ANTI-PATTERN: Running ALTERs serially
ALTER TABLE fact_orders ADD COLUMN col_a BIGINT DEFAULT 0;
-- (wait)
ALTER TABLE fact_orders ADD COLUMN col_b VARCHAR(32) DEFAULT '';
-- (wait)
ALTER TABLE fact_orders ADD COLUMN col_c BOOLEAN DEFAULT FALSE;
-- 3 separate background jobs = 3x the I/O
```

**Fix:** Batch all column additions into a single `ALTER TABLE` statement to execute as one job.

### Using `on_schema_change='sync_all_columns'` in dbt Without Review

```yaml
# ANTI-PATTERN
+on_schema_change: sync_all_columns
```

If a developer removes a column from a dbt model SELECT during refactoring, `sync_all_columns` will issue `DROP COLUMN` on the live table — breaking any consumer that was still reading that column.

**Fix:** Use `append_new_columns` as the default; issue explicit `DROP COLUMN` ALTER statements as a deliberate, reviewed step.

### Widening a Key Column

```sql
-- ANTI-PATTERN: Trying to widen a key column directly
ALTER TABLE fact_orders MODIFY COLUMN order_id BIGINT NOT NULL;
-- order_id is in DUPLICATE KEY() → StarRocks rejects this
-- ERROR: Do not support change key column
```

**Fix:** Use `CREATE TABLE new_table AS SELECT CAST(order_id AS BIGINT) AS order_id, ... FROM fact_orders`, then swap via `ALTER TABLE RENAME`.

---

## Output Expectations

When performing schema evolution tasks:
- Always state which schema change type (DIRECT / LINKED / SORT) the operation will trigger
- Include the full `ALTER TABLE` statement with explicit column positions (AFTER / FIRST) to make the change deterministic
- For Aggregate Key tables, always include the aggregate function in ADD/MODIFY statements
- Include the `SHOW ALTER TABLE COLUMN` monitoring query
- For SORT changes, include duration estimate guidance based on table size
- For dbt-managed tables, specify the `on_schema_change` value and whether a `--full-refresh` is needed

---

## References to Consult When Needed

- StarRocks 3.x Documentation: `ALTER TABLE` — https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_TABLE/
- StarRocks 3.x Documentation: Schema Change Overview — https://docs.starrocks.io/docs/table_design/schema_change/
- StarRocks 3.x Documentation: Primary Key table DML — https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/
- dbt StarRocks adapter: https://github.com/StarRocks/starrocks/tree/main/contrib/starrocks-connector-for-dbt
