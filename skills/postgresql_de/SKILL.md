---
name: postgresql-data-engineering
description: PostgreSQL for data engineering — declarative partitioning (RANGE/LIST/HASH/pg_partman), index types (B-Tree/BRIN/GIN/GIST/partial/covering), COPY bulk load, EXPLAIN ANALYZE plan reading, autovacuum tuning, window functions, JSONB, CTEs, LATERAL joins, and bulk-load patterns (UNLOGGED/pg_bulkload)
---

# PostgreSQL for Data Engineering

## When to Use

Load this skill when the task involves:
- Designing or maintaining large PostgreSQL tables with **declarative partitioning**
- Choosing or creating **indexes** (B-Tree, BRIN, GIN, GIST, partial, covering)
- Bulk-loading data with **COPY**, unlogged tables, or pg_bulkload
- Diagnosing slow queries with **EXPLAIN / EXPLAIN ANALYZE**
- Tuning **autovacuum** to prevent table bloat
- Writing advanced SQL: **window functions**, **JSONB**, **CTEs**, **LATERAL** joins
- Any DE pipeline that reads/writes PostgreSQL as a source or sink

---

## 1. Declarative Partitioning

### Strategy Selection

| Strategy | Use case | Partition key |
|----------|----------|---------------|
| RANGE    | Time-series, sequential IDs | `event_date`, `created_at`, `id` |
| LIST     | Low-cardinality categorical | `region`, `status`, `tenant_id` |
| HASH     | Even spread, no natural range | `user_id`, `order_id` |

### RANGE — time-series events table

```sql
-- Parent table (holds no data itself)
CREATE TABLE events (
    event_id    BIGINT       NOT NULL,
    event_date  DATE         NOT NULL,
    source      TEXT         NOT NULL,
    payload     JSONB,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
) PARTITION BY RANGE (event_date);

-- Monthly child partitions
CREATE TABLE events_2024_01
    PARTITION OF events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE events_2024_02
    PARTITION OF events
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- Default partition catches out-of-range rows (prevents insert errors)
CREATE TABLE events_default
    PARTITION OF events DEFAULT;

-- Index on every partition (inherit automatically in PG 11+)
CREATE INDEX ON events (event_date);
CREATE INDEX ON events (source, event_date);
```

### LIST — multi-tenant or regional sharding

```sql
CREATE TABLE orders (
    order_id   BIGINT       NOT NULL,
    region     TEXT         NOT NULL,
    amount     NUMERIC(12,2),
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
) PARTITION BY LIST (region);

CREATE TABLE orders_eu   PARTITION OF orders FOR VALUES IN ('EU', 'DE', 'FR', 'NL');
CREATE TABLE orders_us   PARTITION OF orders FOR VALUES IN ('US', 'CA', 'MX');
CREATE TABLE orders_apac PARTITION OF orders FOR VALUES IN ('AU', 'SG', 'JP', 'IN');
CREATE TABLE orders_default PARTITION OF orders DEFAULT;
```

### HASH — even distribution when no natural range exists

```sql
CREATE TABLE user_events (
    user_id    BIGINT NOT NULL,
    event_type TEXT   NOT NULL,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY HASH (user_id);

-- 8 buckets
CREATE TABLE user_events_0 PARTITION OF user_events FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE user_events_1 PARTITION OF user_events FOR VALUES WITH (MODULUS 8, REMAINDER 1);
-- ... repeat through REMAINDER 7
```

### Attach / Detach

```sql
-- Detach a partition for archival (non-blocking in PG 14+ with CONCURRENTLY)
ALTER TABLE events DETACH PARTITION events_2022_01 CONCURRENTLY;

-- Re-attach an existing table as a partition
ALTER TABLE events ATTACH PARTITION events_2024_03
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
```

### Partition Pruning

```sql
-- Confirm pruning is enabled
SHOW enable_partition_pruning;  -- must be 'on'

-- Pruning happens at planning time for literals:
EXPLAIN SELECT * FROM events WHERE event_date = '2024-01-15';
-- Output should show only events_2024_01, not all partitions

-- For runtime parameters, enable_runtime_partition_pruning must be on (default on PG 13+)
```

### pg_partman — automated partition management

```sql
-- Install extension
CREATE EXTENSION pg_partman SCHEMA partman;

-- Create a time-based partition set (monthly, pre-create 3 future partitions)
SELECT partman.create_parent(
    p_parent_table  => 'public.events',
    p_control       => 'event_date',
    p_type          => 'range',
    p_interval      => 'monthly',
    p_premake       => 3,
    p_start_partition => '2024-01-01'
);

-- Run maintenance (create new partitions, drop expired ones)
-- Typically called from pg_cron every hour:
SELECT partman.run_maintenance('public.events');

-- Retention: drop partitions older than 12 months
UPDATE partman.part_config
SET    retention = '12 months',
       retention_keep_table = FALSE
WHERE  parent_table = 'public.events';
```

---

## 2. Index Types and Strategy

### B-Tree — the default, works for equality and range queries

```sql
-- Simple column
CREATE INDEX CONCURRENTLY idx_orders_created_at
    ON orders (created_at DESC);

-- Composite: put equality columns first, range column last
CREATE INDEX CONCURRENTLY idx_orders_region_created
    ON orders (region, created_at DESC);

-- Covering index (INCLUDE) — enables index-only scans for wide SELECTs
CREATE INDEX CONCURRENTLY idx_orders_region_amount
    ON orders (region, created_at DESC)
    INCLUDE (order_id, amount);
-- SELECT order_id, amount FROM orders WHERE region='EU' ORDER BY created_at DESC
-- now never touches the heap
```

### BRIN — Block Range INdex, ideal for naturally-ordered time-series

```sql
-- BRIN is tiny (~100x smaller than B-Tree) but requires physical correlation
-- Works well when rows are appended in timestamp order (logs, events, IoT)
CREATE INDEX CONCURRENTLY idx_events_created_brin
    ON events USING BRIN (created_at)
    WITH (pages_per_range = 32);   -- default 128; smaller = more precise, larger index

-- Verify correlation before choosing BRIN:
SELECT attname, correlation
FROM   pg_stats
WHERE  tablename = 'events' AND attname = 'created_at';
-- correlation close to 1.0 or -1.0 → BRIN is appropriate
```

### GIN — inverted index for JSONB, arrays, and full-text search

```sql
-- Default jsonb_ops: supports @>, ?, ?|, ?&, @?  (flexible, larger index)
CREATE INDEX CONCURRENTLY idx_events_payload_gin
    ON events USING GIN (payload);

-- jsonb_path_ops: supports only @> and @@  (faster, 30–50% smaller)
-- Use when queries only check containment and you have a stable schema
CREATE INDEX CONCURRENTLY idx_events_payload_path_gin
    ON events USING GIN (payload jsonb_path_ops);

-- GIN on array column
CREATE INDEX CONCURRENTLY idx_product_tags_gin
    ON products USING GIN (tags);
-- Supports: WHERE tags @> ARRAY['electronics','sale']
```

### Partial index — index only the rows you query

```sql
-- Index only active orders — dramatically smaller, faster to maintain
CREATE INDEX CONCURRENTLY idx_orders_active_created
    ON orders (created_at DESC)
    WHERE status = 'ACTIVE';

-- Index only non-null values
CREATE INDEX CONCURRENTLY idx_events_source_notnull
    ON events (source)
    WHERE source IS NOT NULL;
```

### GIST — geometry, ranges, nearest-neighbor

```sql
-- Range overlap queries
CREATE INDEX CONCURRENTLY idx_bookings_period_gist
    ON bookings USING GIST (tstzrange(start_at, end_at));

-- Find overlapping bookings
SELECT * FROM bookings
WHERE  tstzrange(start_at, end_at) && tstzrange('2024-06-01','2024-06-30');
```

### Index maintenance best practices

```sql
-- Always use CONCURRENTLY to avoid table lock in production
CREATE INDEX CONCURRENTLY ...;

-- Check index bloat
SELECT indexrelname,
       pg_size_pretty(pg_relation_size(indexrelid)) AS idx_size,
       idx_scan,
       idx_tup_read,
       idx_tup_fetch
FROM   pg_stat_user_indexes
WHERE  relname = 'events'
ORDER  BY pg_relation_size(indexrelid) DESC;

-- Rebuild a bloated index online
REINDEX INDEX CONCURRENTLY idx_events_payload_gin;
```

---

## 3. COPY Command — Bulk Load and Export

### COPY FROM — load data into PostgreSQL

```sql
-- CSV with header
COPY events (event_id, event_date, source, payload)
FROM '/data/events_2024_01.csv'
WITH (
    FORMAT CSV,
    HEADER TRUE,
    DELIMITER ',',
    QUOTE '"',
    NULL ''
);

-- Compressed file via shell (server must be able to read the pipe)
COPY events FROM PROGRAM 'gunzip -c /data/events.csv.gz'
WITH (FORMAT CSV, HEADER TRUE);

-- Binary mode — fastest, no encoding overhead; not human-readable
COPY events TO '/data/events_binary.bin' WITH (FORMAT BINARY);
COPY events FROM '/data/events_binary.bin' WITH (FORMAT BINARY);
```

### COPY TO — export from PostgreSQL

```sql
-- Export a query result
COPY (
    SELECT event_id, event_date, source, payload->>'user_id' AS user_id
    FROM   events
    WHERE  event_date >= '2024-01-01'
      AND  event_date <  '2024-02-01'
) TO '/data/export_jan.csv'
WITH (FORMAT CSV, HEADER TRUE, DELIMITER '\t');
```

### Client-side \copy (psql meta-command)

```sql
-- Runs on the client, no superuser needed, reads local files
\copy events (event_id, event_date, source) FROM 'local_file.csv' CSV HEADER
\copy (SELECT * FROM events LIMIT 1000) TO 'sample.csv' CSV HEADER
```

### COPY vs INSERT performance

| Method | ~rows/sec | Notes |
|--------|-----------|-------|
| Single-row INSERT | ~5 K | Max overhead, one WAL record per row |
| Multi-row INSERT (1000/batch) | ~50–200 K | Good for application code |
| COPY (logged) | ~200–800 K | Best general-purpose bulk load |
| COPY + UNLOGGED table | ~1–3 M | No WAL writes; data lost on crash |
| pg_bulkload | >3 M | Bypass shared_buffers; requires extension |

---

## 4. EXPLAIN / EXPLAIN ANALYZE

### Reading a query plan

```sql
-- Always use ANALYZE BUFFERS for real diagnostics; VERBOSE adds column detail
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT TEXT)
SELECT  o.order_id, o.amount, c.name
FROM    orders o
JOIN    customers c ON c.customer_id = o.customer_id
WHERE   o.region = 'EU'
  AND   o.created_at >= now() - INTERVAL '7 days';
```

Sample output interpretation:

```
Hash Join  (cost=1234.56..5678.90 rows=1500 width=48)
           (actual time=12.345..67.890 rows=1423 loops=1)
  Buffers: shared hit=1200 read=340
  ->  Seq Scan on orders_eu  (cost=0..3000 rows=5000 ...)
        Filter: (created_at >= ...)
        Rows Removed by Filter: 3577
  ->  Hash  (cost=800..800 rows=34456 ...)
        Buckets: 65536  Batches: 1  Memory Usage: 2048kB
        ->  Seq Scan on customers  (cost=0..800 ...)
```

Key fields:

| Field | Meaning |
|-------|---------|
| `cost=X..Y` | Planner estimate: startup cost .. total cost (arbitrary units) |
| `rows=N` (plan) | Planner estimate of output rows |
| `rows=N` (actual) | Actual rows returned |
| `loops=N` | Node executed N times (multiply actual by loops for total work) |
| `shared hit` | Pages served from `shared_buffers` (fast) |
| `shared read` | Pages fetched from disk or OS cache (slow) |
| `Rows Removed by Filter` | Rows read but discarded — suggests a missing index |

### Scan type guide

| Scan type | When chosen | What to check |
|-----------|-------------|---------------|
| **Seq Scan** | No usable index, or low selectivity | Add index if `Rows Removed by Filter` is high |
| **Index Scan** | High selectivity, random access | Normal; watch `shared read` for I/O cost |
| **Index Only Scan** | All columns in INCLUDE or index | Ideal; verify `Heap Fetches=0` |
| **Bitmap Heap Scan** | Medium selectivity | Normal; `Recheck Cond` rows indicate bloat |

### Common slow-query patterns

```sql
-- Pattern 1: Stale statistics → huge row estimate mismatch
-- Fix: ANALYZE table; or lower autovacuum_analyze_scale_factor per table

-- Pattern 2: Function on indexed column disables index
-- BAD:
WHERE DATE_TRUNC('day', created_at) = '2024-01-15'
-- GOOD (use range instead):
WHERE created_at >= '2024-01-15' AND created_at < '2024-01-16'
-- Or create an expression index:
CREATE INDEX ON events (DATE_TRUNC('day', created_at));

-- Pattern 3: Implicit type cast prevents index use
-- BAD (event_id is BIGINT, literal is TEXT):
WHERE event_id = '12345'
-- GOOD:
WHERE event_id = 12345

-- Pattern 4: JIT overhead on short queries
-- If JIT Time is large fraction of execution time, disable per session:
SET jit = off;
-- Or raise the threshold globally:
-- jit_above_cost = 500000  (default 100000)
```

### Diagnosing with pg_stat_statements

```sql
-- Top 10 slowest queries by total time
SELECT query,
       calls,
       total_exec_time::NUMERIC(12,2) AS total_ms,
       mean_exec_time::NUMERIC(10,2)  AS mean_ms,
       rows
FROM   pg_stat_statements
ORDER  BY total_exec_time DESC
LIMIT  10;
```

---

## 5. Autovacuum Tuning

### How bloat happens

Every UPDATE or DELETE leaves dead tuples. PostgreSQL uses MVCC, meaning old row versions are not immediately removed. Autovacuum reclaims them. Without proper tuning, large write-heavy tables accumulate bloat that slows scans and wastes disk.

### Key parameters

```sql
-- Global defaults (postgresql.conf)
autovacuum_vacuum_scale_factor    = 0.2    -- trigger at 20% dead tuples (too high for large tables)
autovacuum_vacuum_threshold       = 50     -- minimum dead tuples to trigger vacuum
autovacuum_analyze_scale_factor   = 0.1
autovacuum_analyze_threshold      = 50

autovacuum_vacuum_cost_limit      = 200    -- total I/O cost budget per pass
autovacuum_vacuum_cost_delay      = 2ms    -- pause after exhausting budget
autovacuum_max_workers            = 3      -- parallel autovacuum workers
```

### Per-table tuning for high-write tables

```sql
-- A 10M-row orders table: default fires at 2M dead tuples — far too late
ALTER TABLE orders SET (
    autovacuum_vacuum_scale_factor   = 0.01,   -- trigger at 1%  (100K rows)
    autovacuum_vacuum_threshold      = 1000,
    autovacuum_analyze_scale_factor  = 0.005,  -- analyze at 0.5%
    autovacuum_vacuum_cost_limit     = 1000,   -- more aggressive I/O budget
    autovacuum_vacuum_cost_delay     = 2        -- ms; 0 = unlimited (risky on HDD)
);

-- Small hot lookup table: keep almost always fresh
ALTER TABLE sessions SET (
    autovacuum_vacuum_scale_factor  = 0.0,
    autovacuum_vacuum_threshold     = 500,
    autovacuum_vacuum_cost_limit    = 800
);
```

### Monitor bloat and vacuum activity

```sql
-- Dead tuple ratio per table
SELECT relname,
       n_dead_tup,
       n_live_tup,
       ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_pct,
       last_autovacuum,
       last_autoanalyze
FROM   pg_stat_user_tables
ORDER  BY n_dead_tup DESC
LIMIT  20;

-- Physical bloat estimate
SELECT relname,
       pg_size_pretty(pg_total_relation_size(oid))           AS total_size,
       pg_size_pretty(pg_relation_size(oid))                  AS table_size,
       pg_size_pretty(pg_total_relation_size(oid)
                      - pg_relation_size(oid))                AS index_size
FROM   pg_class
WHERE  relkind = 'r'
ORDER  BY pg_total_relation_size(oid) DESC
LIMIT  20;
```

### Manual vacuum / analyze

```sql
-- Reclaim space and update statistics (safe in production, no table lock)
VACUUM ANALYZE orders;

-- FULL reclaims disk space but holds AccessExclusiveLock — avoid on live tables
VACUUM FULL orders;  -- only during maintenance windows

-- Freeze old XIDs to prevent transaction ID wraparound
VACUUM FREEZE ANALYZE events_2022_01;
```

---

## 6. Window Functions — Advanced Patterns

### Running totals, lag/lead, rank

```sql
SELECT
    order_id,
    region,
    amount,
    -- Running sum within region, ordered by date
    SUM(amount) OVER (
        PARTITION BY region
        ORDER BY created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    -- Previous row value
    LAG(amount, 1, 0) OVER (PARTITION BY region ORDER BY created_at) AS prev_amount,
    -- Dense rank (no gaps on ties)
    DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS rank_in_region
FROM orders;
```

### ROWS vs RANGE vs GROUPS frame modes

```sql
-- ROWS: physical row count — precise, deterministic
SUM(amount) OVER (
    PARTITION BY region ORDER BY created_at
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW   -- rolling 7-row window
)

-- RANGE: logical value range — includes all ties at boundaries
SUM(amount) OVER (
    PARTITION BY region ORDER BY created_at
    RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW  -- rolling 7-day window
)

-- GROUPS (PostgreSQL 11+): peer-group count — useful for ranked groupings
RANK() OVER (
    ORDER BY amount
    GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING
)
```

### FILTER clause inside window aggregates

```sql
-- Count only orders where amount > 1000, alongside total count
SELECT
    region,
    created_at::DATE,
    COUNT(*)                          AS total_orders,
    COUNT(*) FILTER (WHERE amount > 1000) AS large_orders,
    SUM(amount) FILTER (WHERE status = 'COMPLETED') AS completed_revenue
FROM   orders
GROUP  BY region, created_at::DATE;
```

### Named WINDOW clause — avoid duplication

```sql
SELECT
    region, order_id, amount,
    SUM(amount)  OVER w   AS running_total,
    AVG(amount)  OVER w   AS running_avg,
    COUNT(*)     OVER w   AS running_count,
    FIRST_VALUE(amount) OVER w AS first_in_window
FROM   orders
WINDOW w AS (PARTITION BY region ORDER BY created_at ROWS UNBOUNDED PRECEDING);
```

### Percentile and nth_value patterns

```sql
-- Continuous percentile (interpolated)
SELECT region,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) AS p95_amount
FROM   orders
GROUP  BY region;

-- nth_value with frame boundary
SELECT order_id, region, amount,
       NTH_VALUE(amount, 2) OVER (
           PARTITION BY region
           ORDER BY amount DESC
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS second_highest
FROM   orders;
```

---

## 7. JSONB Operations

### Core operators

```sql
-- -> returns JSONB child  (object key or array index)
SELECT payload -> 'user' -> 'id'  FROM events;          -- nested JSONB

-- ->> returns TEXT (use for comparisons and output)
SELECT payload ->> 'event_type'  FROM events;

-- #>  path as array → JSONB
SELECT payload #> '{user,address,city}'  FROM events;

-- #>> path as array → TEXT
SELECT payload #>> '{user,address,city}'  FROM events;

-- @> containment — uses GIN index
SELECT * FROM events WHERE payload @> '{"event_type": "purchase"}';
SELECT * FROM events WHERE payload @> '{"user": {"tier": "premium"}}';

-- ? key existence
SELECT * FROM events WHERE payload ? 'error_code';

-- ?| any key
SELECT * FROM events WHERE payload ?| ARRAY['error_code', 'warning_code'];

-- ?& all keys
SELECT * FROM events WHERE payload ?& ARRAY['user', 'event_type', 'ts'];
```

### jsonb_each, jsonb_each_text — expand keys into rows

```sql
-- Flatten top-level keys into (key, value) pairs
SELECT e.event_id, kv.key, kv.value
FROM   events e,
       jsonb_each(e.payload) AS kv
WHERE  e.event_date = '2024-01-15';
```

### jsonb_agg — aggregate rows back into JSON

```sql
-- Build a JSON array of objects grouped by region
SELECT region,
       jsonb_agg(
           jsonb_build_object(
               'order_id', order_id,
               'amount',   amount,
               'status',   status
           ) ORDER BY created_at DESC
       ) AS orders_json
FROM   orders
WHERE  created_at >= now() - INTERVAL '1 day'
GROUP  BY region;
```

### jsonb_set and update patterns

```sql
-- Update a single nested key without replacing the whole document
UPDATE events
SET    payload = jsonb_set(payload, '{user, tier}', '"gold"', TRUE)
WHERE  event_id = 12345;

-- Remove a key
UPDATE events
SET    payload = payload - 'debug_info'
WHERE  event_date < '2024-01-01';

-- Concatenate / merge two JSONB objects (right wins on key conflict)
UPDATE events
SET    payload = payload || '{"processed": true, "version": 2}'
WHERE  event_type = 'import';
```

### JSONB path queries (PostgreSQL 12+)

```sql
-- jsonpath: find events where any item in a cart array costs > 100
SELECT event_id
FROM   events
WHERE  payload @? '$.cart[*] ? (@.price > 100)';

-- jsonb_path_query — returns matching elements
SELECT event_id,
       jsonb_path_query_array(payload, '$.cart[*].sku') AS skus
FROM   events
WHERE  payload @? '$.cart[*]';
```

---

## 8. CTEs, WITH RECURSIVE, and LATERAL Joins

### Basic CTE — readable decomposition

```sql
-- CTEs are inlined by default in PG 12+ (same as subquery)
WITH recent_orders AS (
    SELECT order_id, customer_id, amount, region
    FROM   orders
    WHERE  created_at >= now() - INTERVAL '30 days'
),
customer_totals AS (
    SELECT customer_id, SUM(amount) AS total_30d
    FROM   recent_orders
    GROUP  BY customer_id
)
SELECT c.name, ct.total_30d
FROM   customers c
JOIN   customer_totals ct USING (customer_id)
WHERE  ct.total_30d > 10000
ORDER  BY ct.total_30d DESC;
```

### MATERIALIZED CTE — force separate execution

```sql
-- Force materialisation when CTE is expensive and referenced multiple times
WITH MATERIALIZED daily_stats AS (
    SELECT event_date,
           COUNT(*)          AS event_count,
           SUM(payload->>'amount')::NUMERIC AS revenue
    FROM   events
    WHERE  event_date >= CURRENT_DATE - 90
    GROUP  BY event_date
)
SELECT d.event_date,
       d.event_count,
       d.revenue,
       AVG(d.revenue) OVER (ORDER BY d.event_date ROWS 6 PRECEDING) AS revenue_7d_avg
FROM   daily_stats d
ORDER  BY event_date;
-- Without MATERIALIZED, the aggregate runs twice; with it, runs once.
```

### WITH RECURSIVE — hierarchical / graph traversal

```sql
-- Org chart: find all reports under a given manager
WITH RECURSIVE org_tree AS (
    -- Anchor: direct reports of manager 42
    SELECT employee_id, manager_id, name, 1 AS depth
    FROM   employees
    WHERE  manager_id = 42

    UNION ALL

    -- Recursive: one level deeper
    SELECT e.employee_id, e.manager_id, e.name, ot.depth + 1
    FROM   employees e
    JOIN   org_tree ot ON e.manager_id = ot.employee_id
    WHERE  ot.depth < 10   -- depth guard prevents infinite loops
)
SELECT * FROM org_tree ORDER BY depth, name;
```

```sql
-- Generate a calendar series (no recursive table needed with generate_series,
-- but shown for completeness)
WITH RECURSIVE date_series AS (
    SELECT '2024-01-01'::DATE AS dt
    UNION ALL
    SELECT dt + 1 FROM date_series WHERE dt < '2024-12-31'
)
SELECT dt FROM date_series;
-- Prefer generate_series() for dates; WITH RECURSIVE for graph/tree structures
```

### LATERAL join — correlated subquery as table

```sql
-- Top-3 most recent orders per customer (efficient with index on customer_id, created_at)
SELECT c.customer_id, c.name, top_orders.*
FROM   customers c
CROSS  JOIN LATERAL (
    SELECT order_id, amount, created_at
    FROM   orders o
    WHERE  o.customer_id = c.customer_id
    ORDER  BY o.created_at DESC
    LIMIT  3
) AS top_orders;

-- LATERAL with function
SELECT u.user_id, stats.*
FROM   users u
CROSS  JOIN LATERAL compute_user_stats(u.user_id, now() - INTERVAL '30 days') AS stats;
```

---

## 9. Bulk Load Patterns

### Pattern 1 — COPY into a permanent logged table (general purpose)

```sql
BEGIN;

-- Disable triggers if safe (e.g., no FK checks needed during load)
ALTER TABLE events DISABLE TRIGGER ALL;

COPY events (event_id, event_date, source, payload)
FROM '/data/events_batch.csv'
WITH (FORMAT CSV, HEADER TRUE);

ALTER TABLE events ENABLE TRIGGER ALL;

COMMIT;
```

### Pattern 2 — UNLOGGED staging table trick

```sql
-- Step 1: create an unlogged staging copy (no WAL → ~3–5x faster writes)
CREATE UNLOGGED TABLE events_stage (LIKE events INCLUDING DEFAULTS);

-- Step 2: bulk-load into staging
COPY events_stage FROM '/data/events_batch.csv' WITH (FORMAT CSV, HEADER TRUE);

-- Step 3: validate / transform in staging
DELETE FROM events_stage WHERE event_id IS NULL OR event_date IS NULL;

-- Step 4: insert from staging into main table
INSERT INTO events SELECT * FROM events_stage
ON CONFLICT (event_id) DO NOTHING;

-- Step 5: clean up
DROP TABLE events_stage;
-- Note: events_stage is truncated to empty automatically if the server crashes
```

### Pattern 3 — Disable indexes + COPY + rebuild

```sql
-- For initial historical loads of hundreds of millions of rows
-- Step 1: drop non-primary indexes (keep PK/UNIQUE if needed for uniqueness)
DROP INDEX CONCURRENTLY idx_events_created_brin;
DROP INDEX CONCURRENTLY idx_events_payload_gin;

-- Step 2: load data (now much faster without index maintenance)
COPY events FROM '/data/historical_events.csv' WITH (FORMAT CSV, HEADER TRUE);

-- Step 3: rebuild indexes (CONCURRENTLY avoids table lock)
CREATE INDEX CONCURRENTLY idx_events_created_brin
    ON events USING BRIN (created_at) WITH (pages_per_range = 32);
CREATE INDEX CONCURRENTLY idx_events_payload_gin
    ON events USING GIN (payload jsonb_path_ops);

-- Step 4: update statistics
ANALYZE events;
```

### Pattern 4 — Session-level settings for maximum load throughput

```sql
-- Set in the loading session (reverts when session closes)
SET maintenance_work_mem = '2GB';        -- faster index builds
SET max_wal_size = '8GB';               -- reduce checkpoint frequency
SET synchronous_commit = off;            -- async WAL flush (risk: lose ~1s of data on crash)
SET wal_buffers = '64MB';               -- write-ahead log buffer

-- Then run COPY ...
```

### Pattern 5 — pg_bulkload (extreme throughput, extension required)

```bash
# pg_bulkload bypasses shared_buffers and WAL; fastest possible ingestion
# Install: apt-get install pg_bulkload  or  pgxn install pg_bulkload

pg_bulkload \
  -d mydb \
  -U postgres \
  -i /data/events_batch.csv \
  -O events \
  -o "TYPE=CSV" \
  -o "SKIP=1" \
  -o "DELIMITER=," \
  -o "FILTER=events_filter_func"
```

```sql
-- After pg_bulkload, always rebuild indexes and run ANALYZE
REINDEX TABLE events;
ANALYZE events;
```

### Partition-aware bulk load

```sql
-- For partitioned tables: load directly into the specific child partition
-- to avoid partition routing overhead
COPY events_2024_01 FROM '/data/events_2024_01.csv' WITH (FORMAT CSV, HEADER TRUE);
COPY events_2024_02 FROM '/data/events_2024_02.csv' WITH (FORMAT CSV, HEADER TRUE);
```

---

## Anti-Patterns

| Anti-pattern | Problem | Fix |
|-------------|---------|-----|
| `SELECT *` on wide partitioned table | Reads all partitions, all columns | Explicit column list + `WHERE` on partition key |
| `WHERE DATE_TRUNC('month', ts) = '2024-01-01'` | Function on column disables index | Use range: `ts >= '2024-01-01' AND ts < '2024-02-01'` |
| Autovacuum with default scale_factor on 100M+ row tables | Vacuum fires at 20M dead tuples; severe bloat | `ALTER TABLE ... SET (autovacuum_vacuum_scale_factor = 0.01)` |
| `VACUUM FULL` in production | Holds `AccessExclusiveLock` for hours | Use `pg_repack` extension for online defragmentation |
| COPY without a transaction | Partial load on error leaves dirty state | Wrap `COPY` in `BEGIN/COMMIT` |
| GIN index on every JSONB column | GIN indexes are 60–80% of table size; slow writes | Index only frequently-queried paths; use `jsonb_path_ops` for containment-only |
| UNLOGGED table as permanent store | Data lost on server crash, not replicated | Use only for staging/temp workloads |
| Recursive CTE without depth guard | Infinite loop on cyclic graph data | Add `WHERE depth < N` guard |
| Ignoring `shared read` in BUFFERS output | Misses I/O bottleneck; relies on warm cache | Tune `shared_buffers`; check `effective_cache_size` |
| `NOT MATERIALIZED` hint on multi-reference CTE | Expensive subquery runs multiple times | Use `MATERIALIZED` when CTE is referenced 2+ times and is costly |

---

## References to Consult When Needed

- [PostgreSQL 16 Documentation — Table Partitioning](https://www.postgresql.org/docs/16/ddl-partitioning.html)
- [PostgreSQL 16 Documentation — EXPLAIN](https://www.postgresql.org/docs/16/sql-explain.html)
- [PostgreSQL 16 Documentation — Populating a Database (bulk load)](https://www.postgresql.org/docs/16/populate.html)
- [PostgreSQL 16 Documentation — Index Types](https://www.postgresql.org/docs/16/indexes-types.html)
- [PostgreSQL 16 Documentation — WITH Queries](https://www.postgresql.org/docs/16/queries-with.html)
- [PostgreSQL 16 Documentation — JSON Types](https://www.postgresql.org/docs/16/datatype-json.html)
- [PostgreSQL 16 Documentation — Autovacuum](https://www.postgresql.org/docs/16/routine-vacuuming.html)
- [pg_partman GitHub](https://github.com/pgpartman/pg_partman) — partition management extension
- [pganalyze — Understanding GIN Indexes](https://pganalyze.com/blog/gin-index)
- [EDB — 7 Best Practice Tips for PostgreSQL Bulk Data Loading](https://www.enterprisedb.com/blog/7-best-practice-tips-postgresql-bulk-data-loading)
- [Cybertec — PostgreSQL Bulk Loading](https://www.cybertec-postgresql.com/en/postgresql-bulk-loading-huge-amounts-of-data/)
- [Percona — Tuning Autovacuum in PostgreSQL](https://www.percona.com/blog/tuning-autovacuum-in-postgresql-and-autovacuum-internals/)
- [Crunchy Data — Indexing JSONB in Postgres](https://www.crunchydata.com/blog/indexing-jsonb-in-postgres)
