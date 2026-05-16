---
name: vertica
description: Use when writing, reviewing, debugging, or optimizing SQL for Vertica — covering DDL (CREATE/ALTER/DROP TABLE, columns, projections, segmentation, partitions), DML (INSERT, UPDATE, DELETE, MERGE, TRUNCATE, COPY), CRUD patterns, and Vertica-specific performance guidance including encoding, segmentation keys, partition pruning, and query optimization.
---

# Vertica SQL Engineer

## When to Use

Use this skill when:
- The user is writing or reviewing SQL for Vertica (HPE Vertica / Micro Focus Vertica)
- The task involves DDL: creating, altering, or dropping tables, projections, schemas
- The task involves DML: INSERT, UPDATE, DELETE, MERGE, TRUNCATE, or COPY
- The user needs guidance on Vertica-specific concepts: projections, segmentation, encoding, partitioning
- The user asks about CRUD patterns, data update strategies, or upsert patterns in Vertica

## Core Concepts Unique to Vertica

- **Columnar storage**: data is stored column-by-column; column encoding and compression matter significantly.
- **Projections**: the physical storage layer. Every table has a super projection; additional covering projections can be created to optimize specific queries. A query can only be answered if a covering projection exists.
- **Segmentation**: how rows are distributed across nodes using a hash or unsegmented. The segmentation key is the Vertica equivalent of a distribution key.
- **Partition Expression**: a partition clause divides data within each node into segments for pruning and ROS container management.
- **ROS / WOS**: Vertica writes new data to Write Optimized Store (WOS) in memory and flushes to Read Optimized Store (ROS) on disk via the Tuple Mover.
- **Epoch / snapshot isolation**: Vertica uses epoch-based MVCC. DELETE and UPDATE mark rows logically deleted; the Tuple Mover purges them.

---

## DDL: Schemas

```sql
CREATE SCHEMA IF NOT EXISTS marketing;
DROP SCHEMA marketing CASCADE;   -- drops all objects inside
```

---

## DDL: CREATE TABLE

### Minimal table

```sql
CREATE TABLE marketing.events (
    event_id    BIGINT       NOT NULL,
    user_id     BIGINT       NOT NULL,
    event_type  VARCHAR(64)  NOT NULL,
    amount      NUMERIC(18,2),
    event_ts    TIMESTAMP    NOT NULL,
    created_at  TIMESTAMP    DEFAULT NOW()
);
```

### Full production table with segmentation and partitioning

```sql
CREATE TABLE dwh.fact_orders (
    order_id     BIGINT        NOT NULL,
    user_id      BIGINT        NOT NULL,
    order_date   DATE          NOT NULL,
    status       VARCHAR(32)   NOT NULL,
    amount       NUMERIC(18,2) NOT NULL,
    currency     CHAR(3)       NOT NULL DEFAULT 'USD',
    created_at   TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP
)
ORDER BY order_date, user_id       -- sort order within projection
SEGMENTED BY HASH(order_id) ALL NODES  -- distribution key
PARTITION BY order_date::DATE
    GROUP BY CALENDAR_HIERARCHY_DAY(order_date::DATE, 3, 12);
```

Key design rules:
- `ORDER BY` in `CREATE TABLE` sets the default sort order of the super projection; use columns that appear most in range predicates.
- `SEGMENTED BY HASH(col)` distributes rows across nodes; pick a high-cardinality column that appears in joins and group-bys.
- `UNSEGMENTED ALL NODES` for small dimension tables that are broadcast.
- `PARTITION BY` enables partition pruning and range-limited DELETE; keep partition granularity coarse enough to avoid excessive ROS containers (month or year is safer than day for very large tables).

### LIKE — copy structure

```sql
CREATE TABLE staging.fact_orders_load
    LIKE dwh.fact_orders INCLUDING PROJECTIONS;
```

`LIKE` copies columns, constraints, and optionally projections. Use it for staging tables.

### CREATE TABLE AS SELECT (CTAS)

```sql
CREATE TABLE tmp.monthly_revenue AS
SELECT
    DATE_TRUNC('month', order_date) AS month,
    currency,
    SUM(amount)                      AS revenue
FROM dwh.fact_orders
WHERE order_date >= DATE '2026-01-01'
GROUP BY 1, 2;
```

CTAS creates a table and populates it in one statement. The resulting table has no projections beyond the super projection unless you create them separately.

---

## DDL: Data Types

| Category    | Types                                                                 |
|-------------|-----------------------------------------------------------------------|
| Integer     | `TINYINT`, `SMALLINT`, `INTEGER` / `INT`, `BIGINT`                   |
| Decimal     | `NUMERIC(p,s)` / `DECIMAL(p,s)`, `FLOAT` / `DOUBLE PRECISION`        |
| String      | `CHAR(n)`, `VARCHAR(n)`, `LONG VARCHAR`, `UUID`                      |
| Date/Time   | `DATE`, `TIME`, `TIMETZ`, `TIMESTAMP`, `TIMESTAMPTZ`, `INTERVAL`     |
| Boolean     | `BOOLEAN`                                                             |
| Binary      | `BINARY(n)`, `VARBINARY(n)`, `LONG VARBINARY`                        |

Use `BIGINT` for IDs by default. Use `NUMERIC` (not `FLOAT`) for monetary values. Prefer `TIMESTAMP` over `TIMESTAMPTZ` unless time-zone awareness is explicitly required.

---

## DDL: ALTER TABLE

### Add a column

```sql
ALTER TABLE dwh.fact_orders ADD COLUMN region VARCHAR(64);
ALTER TABLE dwh.fact_orders ADD COLUMN is_refunded BOOLEAN DEFAULT FALSE NOT NULL;
```

After `ADD COLUMN`, new rows get the default. Existing rows see `NULL` unless a `DEFAULT` is specified and a `NOT NULL` constraint is added.

### Drop a column

```sql
ALTER TABLE dwh.fact_orders DROP COLUMN region;
ALTER TABLE dwh.fact_orders DROP COLUMN region CASCADE; -- also drops dependent projections
```

Dropping a column that is part of a projection requires `CASCADE` or dropping the projection first.

### Rename a column

```sql
ALTER TABLE dwh.fact_orders RENAME COLUMN region TO sales_region;
```

### Modify column type or default

```sql
ALTER TABLE dwh.fact_orders ALTER COLUMN status SET DEFAULT 'pending';
ALTER TABLE dwh.fact_orders ALTER COLUMN status DROP DEFAULT;
ALTER TABLE dwh.fact_orders ALTER COLUMN status SET NOT NULL;
ALTER TABLE dwh.fact_orders ALTER COLUMN notes DROP NOT NULL;
```

Vertica does not support `ALTER COLUMN ... TYPE` for changing an existing column's data type in place — use a staged rename-and-recreate approach if needed.

### Rename a table

```sql
ALTER TABLE staging.fact_orders_load RENAME TO fact_orders_v2;
```

### Set partition expression

```sql
ALTER TABLE dwh.fact_orders
    PARTITION BY order_date::DATE
    GROUP BY CALENDAR_HIERARCHY_DAY(order_date::DATE, 3, 12);
```

---

## DDL: DROP TABLE

```sql
DROP TABLE IF EXISTS staging.fact_orders_load;
DROP TABLE staging.fact_orders_load CASCADE; -- also drops dependent objects
```

---

## DDL: Projections

Vertica projections are the physical storage objects. Every table gets a default super projection. Create additional projections to cover specific query patterns.

### Create a covering projection

```sql
CREATE PROJECTION dwh.fact_orders_by_user (
    user_id,
    order_date,
    amount,
    status
)
AS
SELECT
    user_id,
    order_date,
    amount,
    status
FROM dwh.fact_orders
ORDER BY user_id, order_date
SEGMENTED BY HASH(user_id) ALL NODES;
```

After creating a projection, refresh it:

```sql
SELECT MAKE_AHM_NOW();
SELECT START_REFRESH();
```

Or wait for the Tuple Mover to populate it, or refresh manually:

```sql
SELECT REFRESH('dwh.fact_orders');
```

### Drop a projection

```sql
DROP PROJECTION dwh.fact_orders_by_user;
```

---

## DML: INSERT

### Single-row insert

```sql
INSERT INTO dwh.fact_orders (order_id, user_id, order_date, status, amount, currency)
VALUES (1001, 42, DATE '2026-05-01', 'completed', 199.99, 'USD');
```

### Multi-row insert

```sql
INSERT INTO dwh.fact_orders (order_id, user_id, order_date, status, amount, currency)
VALUES
    (1002, 43, DATE '2026-05-02', 'pending',   49.00, 'EUR'),
    (1003, 44, DATE '2026-05-02', 'completed', 99.50, 'USD');
```

### INSERT ... SELECT

```sql
INSERT INTO dwh.fact_orders (order_id, user_id, order_date, status, amount, currency)
SELECT
    order_id,
    user_id,
    order_date::DATE,
    COALESCE(status, 'unknown'),
    amount,
    currency
FROM staging.fact_orders_load
WHERE amount > 0;
```

### COPY — bulk load (preferred for large volumes)

```sql
COPY staging.fact_orders_load (order_id, user_id, order_date, status, amount, currency)
FROM '/data/orders/2026-05-01.csv'
DELIMITER ','
ENCLOSED BY '"'
SKIP 1
DIRECT
REJECTMAX 100
EXCEPTIONS '/tmp/load_exceptions.txt';
```

- `DIRECT`: bypasses WOS and writes directly to ROS — preferred for large loads.
- `REJECTMAX n`: allow at most `n` rejected rows before aborting.
- Use `COPY FROM STDIN` for programmatic loading from application code.

---

## DML: UPDATE

```sql
UPDATE dwh.fact_orders
SET
    status     = 'refunded',
    updated_at = NOW()
WHERE order_id = 1001;
```

### UPDATE with JOIN (using a subquery)

Vertica does not support `UPDATE ... FROM ... JOIN` syntax directly. Use a correlated subquery or a scalar subquery:

```sql
UPDATE dwh.fact_orders o
SET status = (
    SELECT r.new_status
    FROM staging.order_status_updates r
    WHERE r.order_id = o.order_id
)
WHERE EXISTS (
    SELECT 1
    FROM staging.order_status_updates r
    WHERE r.order_id = o.order_id
);
```

Performance note: large `UPDATE` operations are expensive in Vertica because they logically delete old rows and insert new ones. For bulk updates affecting a large fraction of a table, prefer `MERGE` or staging + truncate + re-insert.

---

## DML: DELETE

### Simple delete

```sql
DELETE FROM dwh.fact_orders
WHERE order_date < DATE '2023-01-01';
```

### Partition-limited delete (fast path)

```sql
DELETE FROM dwh.fact_orders
WHERE order_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31';
```

When the `WHERE` clause aligns with the partition expression, Vertica can drop entire ROS containers rather than marking individual rows deleted — significantly faster.

### Truncate (delete all rows)

```sql
TRUNCATE TABLE staging.fact_orders_load;
```

`TRUNCATE` is much faster than `DELETE` with no `WHERE` clause. It is not transaction-safe in the same way as `DELETE` — it drops ROS containers immediately.

### Purging deleted rows

Logically deleted rows are purged by the Tuple Mover on its schedule. To force immediate purging:

```sql
SELECT PURGE_TABLE('dwh.fact_orders');
SELECT PURGE_PARTITION('dwh.fact_orders', '2026-01-01', '2026-01-31');
```

---

## DML: MERGE (upsert)

`MERGE` is the standard Vertica upsert pattern. Use it to apply changes from a staging table into a target table.

### Standard upsert

```sql
MERGE INTO dwh.fact_orders AS target
USING staging.fact_orders_load AS source
    ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET
        target.status     = source.status,
        target.amount     = source.amount,
        target.updated_at = NOW()
WHEN NOT MATCHED THEN
    INSERT (order_id, user_id, order_date, status, amount, currency, created_at)
    VALUES (source.order_id, source.user_id, source.order_date,
            source.status, source.amount, source.currency, NOW());
```

### Insert-only merge (when no update needed)

```sql
MERGE INTO dwh.dim_users AS target
USING staging.dim_users_delta AS source
    ON target.user_id = source.user_id
WHEN NOT MATCHED THEN
    INSERT (user_id, email, country, created_at)
    VALUES (source.user_id, source.email, source.country, NOW());
```

### Merge with delete

```sql
MERGE INTO dwh.fact_orders AS target
USING staging.fact_orders_deletes AS source
    ON target.order_id = source.order_id
WHEN MATCHED THEN DELETE;
```

### Merge with conditional update

```sql
MERGE INTO dwh.fact_orders AS target
USING staging.fact_orders_load AS source
    ON target.order_id = source.order_id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
    UPDATE SET
        target.status     = source.status,
        target.amount     = source.amount,
        target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (order_id, user_id, order_date, status, amount, currency, created_at)
    VALUES (source.order_id, source.user_id, source.order_date,
            source.status, source.amount, source.currency, NOW());
```

`MERGE` rules:
- The `ON` clause must uniquely identify each target row; duplicate matches cause errors.
- Deduplicate the source before `MERGE` if the source can have multiple rows per key.
- `WHEN MATCHED THEN DELETE` removes the matched target row.
- Multiple `WHEN MATCHED` and `WHEN NOT MATCHED` branches are supported; Vertica evaluates them in order.

---

## Data Update Strategies

Choose the right strategy based on data volume and update pattern:

### 1. Row-level UPDATE / DELETE
Use for small targeted changes (single row or narrow filter).

```sql
UPDATE dwh.dim_users SET country = 'DE' WHERE user_id = 42;
DELETE FROM dwh.fact_orders WHERE order_id = 9999;
```

### 2. MERGE from staging
Use for incremental loads where rows can be new or changed. This is the standard ETL pattern.

```sql
-- Load delta to staging
TRUNCATE TABLE staging.fact_orders_load;
COPY staging.fact_orders_load FROM '/data/orders/delta.csv' DELIMITER ',' DIRECT;

-- Apply to target
MERGE INTO dwh.fact_orders AS t
USING (
    SELECT order_id, user_id, order_date, status, amount, currency,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
    FROM staging.fact_orders_load
) AS s ON t.order_id = s.order_id AND s.rn = 1
WHEN MATCHED THEN UPDATE SET
    t.status = s.status, t.amount = s.amount, t.updated_at = NOW()
WHEN NOT MATCHED THEN INSERT (order_id, user_id, order_date, status, amount, currency)
    VALUES (s.order_id, s.user_id, s.order_date, s.status, s.amount, s.currency);
```

Always deduplicate the staging source on the business key before or inside the `USING` clause.

### 3. Partition swap (large bulk reload)
Use when reloading an entire partition (e.g., a month) more efficiently than UPDATE/MERGE.

```sql
-- Load new data for the partition into a staging table
CREATE TABLE tmp.fact_orders_202601 LIKE dwh.fact_orders;
COPY tmp.fact_orders_202601 FROM '/data/orders/2026-01/*.csv' DELIMITER ',' DIRECT;

-- Swap the partition
ALTER TABLE dwh.fact_orders
    SWAP PARTITION BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
    WITH tmp.fact_orders_202601;

-- Drop the staging table
DROP TABLE tmp.fact_orders_202601;
```

### 4. TRUNCATE + INSERT (full table reload)
Use when rebuilding dimension tables or complete fact reloads.

```sql
TRUNCATE TABLE dwh.dim_users;
INSERT INTO dwh.dim_users SELECT * FROM staging.dim_users_full;
```

### 5. CREATE TABLE AS SELECT + RENAME (atomic full rebuild)
Use for full rebuilds where downtime must be minimized.

```sql
CREATE TABLE dwh.dim_users_new AS
SELECT * FROM staging.dim_users_full;

ALTER TABLE dwh.dim_users RENAME TO dim_users_old;
ALTER TABLE dwh.dim_users_new RENAME TO dim_users;
DROP TABLE dwh.dim_users_old;
```

---

## DML: SELECT — Query Basics

### Basic query structure

```sql
SELECT
    o.order_date,
    u.country,
    SUM(o.amount)    AS revenue,
    COUNT(DISTINCT o.order_id) AS order_count
FROM dwh.fact_orders o
JOIN dwh.dim_users u
    ON o.user_id = u.user_id
WHERE o.order_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
  AND o.status = 'completed'
GROUP BY o.order_date, u.country
ORDER BY o.order_date, revenue DESC;
```

### CTEs

```sql
WITH completed_orders AS (
    SELECT order_id, user_id, order_date, amount
    FROM dwh.fact_orders
    WHERE status = 'completed'
      AND order_date >= DATE '2026-01-01'
),
user_revenue AS (
    SELECT
        o.user_id,
        u.country,
        SUM(o.amount) AS revenue
    FROM completed_orders o
    JOIN dwh.dim_users u ON o.user_id = u.user_id
    GROUP BY o.user_id, u.country
)
SELECT country, SUM(revenue) AS total_revenue
FROM user_revenue
GROUP BY country
ORDER BY total_revenue DESC
LIMIT 20;
```

### Window functions

```sql
SELECT
    user_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY user_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    ROW_NUMBER() OVER (
        PARTITION BY user_id
        ORDER BY order_date DESC NULLS LAST
    ) AS recency_rank
FROM dwh.fact_orders
WHERE status = 'completed';
```

### Deduplication with ROW_NUMBER

```sql
SELECT order_id, user_id, order_date, amount
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC NULLS LAST
        ) AS rn
    FROM staging.fact_orders_load
) t
WHERE rn = 1;
```

### Date and time filtering

```sql
-- Prefer direct date comparisons for partition pruning
WHERE order_date >= DATE '2026-01-01'
  AND order_date <  DATE '2026-02-01'

-- Avoid wrapping partition columns in functions
-- BAD: WHERE DATE_TRUNC('month', order_date) = DATE '2026-01-01'
-- GOOD: WHERE order_date >= DATE '2026-01-01' AND order_date < DATE '2026-02-01'
```

Useful Vertica date functions:

```sql
DATE_TRUNC('month', order_date)
ADD_MONTHS(order_date, 3)
DATEDIFF('day', start_date, end_date)
TIMESTAMPDIFF('hour', start_ts, end_ts)
NOW()
CURRENT_DATE
TO_DATE('2026-05-01', 'YYYY-MM-DD')
TO_TIMESTAMP('2026-05-01 12:00:00', 'YYYY-MM-DD HH24:MI:SS')
```

### String functions

```sql
LOWER(status)
UPPER(country)
TRIM(email)
LTRIM(s), RTRIM(s)
SUBSTR(s, start, length)
REGEXP_LIKE(email, '^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$')
REGEXP_SUBSTR(description, '[0-9]+')
SPLIT_PART(path, '/', 2)
REPLACE(s, 'old', 'new')
LENGTH(s)
```

### NULL handling

```sql
COALESCE(amount, 0)
NVL(status, 'unknown')      -- Vertica alias for COALESCE with 2 args
NULLIFZERO(amount)           -- returns NULL if 0
ZEROIFNULL(amount)           -- returns 0 if NULL
DECODE(status, NULL, 'n/a', status)
```

### Type casting

```sql
CAST(amount AS VARCHAR(20))
amount::VARCHAR(20)          -- Vertica shorthand
TO_CHAR(order_date, 'YYYY-MM-DD')
TO_NUMBER('123.45', '999.99')
```

---

## Query Performance Guidance

### Use partition pruning

Always filter on the partition column using direct comparisons, not function-wrapped columns:

```sql
WHERE order_date >= DATE '2026-01-01'
  AND order_date <  DATE '2026-04-01'
```

### Check query plan

```sql
EXPLAIN SELECT ...;
```

Look for:
- `GROUP BY PIPELINED`: good, no sort needed.
- `RESEGMENT`: data is being redistributed across nodes for a join; check if segmentation keys match join keys.
- `BROADCAST`: small table is broadcast to all nodes; expected for small dimensions.
- Full-table scans without partition pruning on large partitioned tables: add a partition filter.
- Missing covering projections: Vertica uses the best available projection; create a covering one for hot queries.

### ANALYZE_STATISTICS

Keep statistics current for the query optimizer:

```sql
SELECT ANALYZE_STATISTICS('dwh.fact_orders');
SELECT ANALYZE_STATISTICS('dwh.fact_orders', 'order_date');
```

### Hints

```sql
-- Force use of a specific projection
SELECT /*+ PROJECTION(dwh.fact_orders_by_user) */ user_id, SUM(amount)
FROM dwh.fact_orders
GROUP BY user_id;
```

---

## Useful System Queries

```sql
-- List tables in a schema
SELECT table_name FROM v_catalog.tables WHERE table_schema = 'dwh';

-- List columns
SELECT column_name, data_type, is_nullable, column_default
FROM v_catalog.columns
WHERE table_schema = 'dwh' AND table_name = 'fact_orders'
ORDER BY ordinal_position;

-- List projections
SELECT projection_name, is_super_projection, is_up_to_date
FROM v_catalog.projections
WHERE projection_schema = 'dwh' AND anchor_table_name = 'fact_orders';

-- List partitions
SELECT partition_key, ros_count, ros_row_count
FROM v_monitor.partitions
WHERE table_schema = 'dwh' AND table_name = 'fact_orders'
ORDER BY partition_key;

-- Check table row counts
SELECT table_schema, table_name, row_count
FROM v_monitor.table_storage
WHERE table_schema = 'dwh'
ORDER BY row_count DESC;

-- Recent load history
SELECT table_name, rows_accepted, rows_rejected, load_start, load_duration_ms
FROM v_monitor.load_streams
ORDER BY load_start DESC
LIMIT 20;

-- Active sessions and running queries
SELECT session_id, user_name, current_statement, is_active
FROM v_monitor.sessions
WHERE is_active = TRUE;
```

---

## Anti-Patterns

Do not:
- Use `SELECT *` in production queries or INSERT-SELECT.
- Wrap partition columns in functions inside `WHERE` — this disables partition pruning.
- Run large `UPDATE` on most rows of a large table — use MERGE + staging or partition swap instead.
- Forget to deduplicate the source before `MERGE` — duplicate source keys cause runtime errors.
- Create many narrow projections — each projection is a full copy; balance query coverage with storage cost.
- Run `TRUNCATE` when transactional safety matters — prefer `DELETE` with a condition.
- Ignore `PURGE_TABLE` / `PURGE_PARTITION` on tables with heavy DELETE/UPDATE churn — deleted rows persist on disk until purged and inflate storage.
- Use `ORDER BY` in subqueries or CTEs unless absolutely required — it forces unnecessary sorts.
- Use `FLOAT` for monetary or precision-critical values — use `NUMERIC(p,s)`.
- Build large literal `IN (...)` lists — use a join or a temp table instead.
- Omit the segmentation key in joins and group-bys — mismatched segmentation triggers expensive `RESEGMENT` operations.

---

## Output Expectations

When producing Vertica SQL:
- Return valid Vertica SQL, not generic ANSI SQL or PostgreSQL.
- Use explicit column lists in `INSERT`, `SELECT`, and `MERGE`.
- Prefer `MERGE` for upserts over conditional INSERT/UPDATE logic.
- Explain segmentation, partition, and projection choices briefly when they affect correctness or performance.
- Mention when a write strategy (partition swap, full reload) is more appropriate than row-level DML.
- Include `ANALYZE_STATISTICS` or `EXPLAIN` recommendations when performance depends on data distribution.
- Call out `PURGE_TABLE` needs when heavy DELETE/UPDATE is used.
