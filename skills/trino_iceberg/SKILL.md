---
name: trino_iceberg
description: Use when writing, optimizing, or maintaining Apache Iceberg tables with Trino — covering table DDL, all ALTER TABLE operations, partition transforms, bucketing, sorted tables, DML (INSERT/UPDATE/DELETE/MERGE), EXPLAIN plan reading, join optimization, statistics with ANALYZE, table maintenance (optimize/expire_snapshots/remove_orphan_files), schema evolution, time travel, and metadata table diagnostics.
---

# Trino + Apache Iceberg Engineer

## When to Use

Use this skill when:
- Creating or modifying Iceberg tables in Trino (DDL, partitioning, bucketing, table properties)
- Writing or tuning DML: INSERT, UPDATE, DELETE, MERGE, TRUNCATE on Iceberg tables
- Optimizing query performance: reading EXPLAIN plans, configuring join strategies, enabling pushdowns
- Running or scheduling table maintenance: compaction, snapshot expiration, orphan file cleanup
- Diagnosing performance problems using `$partitions`, `$files`, `$snapshots` metadata tables
- Implementing schema evolution, time travel, or snapshot rollback
- Configuring the Iceberg connector for a new Trino cluster

---

## Core Workflow

### Starting from scratch: new table

1. Choose partition granularity from volume guidelines (see Partitioning section).
2. Decide on bucketing if a high-cardinality join/filter column exists (see Bucketing section).
3. Pick `sorted_by` columns: the highest-selectivity filter columns and join keys.
4. Create the table with `format='PARQUET'`, `format_version=2`.
5. Load data, then run `ANALYZE` on join and filter columns.
6. Verify the plan with `EXPLAIN (TYPE DISTRIBUTED)`.

### Diagnosing a slow query

1. Run `EXPLAIN (TYPE DISTRIBUTED)` — identify join type, exchange type, scan rows.
2. Run `SHOW STATS FOR <table>` — check if statistics exist.
3. Check `"$partitions"` — look for partition skew or small-file accumulation.
4. Run `ANALYZE` if NDV / row counts are missing.
5. Adjust partitioning, bucketing, `sorted_by`, or session join properties if plan is still suboptimal.

### Regular maintenance cycle

```
Daily   → OPTIMIZE on partitions written in the last 1–2 days
Weekly  → EXPIRE_SNAPSHOTS + REMOVE_ORPHAN_FILES + OPTIMIZE_MANIFESTS
After optimize → ANALYZE to refresh statistics
```

---

## Table Creation

### Column data types

| Category | Types |
|---|---|
| Integer | `BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER` / `INT`, `BIGINT` |
| Floating point | `REAL` / `FLOAT`, `DOUBLE` |
| Fixed precision | `DECIMAL(precision, scale)` — use for monetary values |
| String | `VARCHAR` (unbounded), `VARCHAR(n)` (max n chars), `CHAR(n)` (fixed, space-padded) |
| Binary | `VARBINARY` |
| Date / time | `DATE`, `TIME`, `TIME(p) WITH TIME ZONE`, `TIMESTAMP`, `TIMESTAMP(p) WITH TIME ZONE` |
| Complex | `ARRAY(element_type)`, `MAP(key_type, value_type)`, `ROW(field_name field_type, ...)` |
| Other | `UUID`, `JSON`, `IPADDRESS` |

Prefer `TIMESTAMP(6) WITH TIME ZONE` over bare `TIMESTAMP` — Iceberg stores time zone offset and avoids daylight saving ambiguity.

### Full CREATE TABLE syntax

```sql
CREATE TABLE iceberg_catalog.mydb.events (
    event_id    BIGINT        NOT NULL,
    event_type  VARCHAR       NOT NULL,
    user_id     BIGINT,
    region      VARCHAR,
    amount      DECIMAL(18,2),
    tags        ARRAY(VARCHAR),
    metadata    MAP(VARCHAR, VARCHAR),
    address     ROW(city VARCHAR, country CHAR(2)),
    event_ts    TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    created_at  TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
COMMENT 'Raw clickstream events'
WITH (
    format                         = 'PARQUET',
    format_version                 = 2,
    partitioning                   = ARRAY['region', 'day(event_ts)'],
    sorted_by                      = ARRAY['user_id', 'event_type'],
    write_target_data_file_size_bytes = 536870912,  -- 512 MB per file
    location                       = 's3://my-bucket/warehouse/mydb/events'
);
```

### All WITH table properties

| Property | Values / Default | Description |
|---|---|---|
| `format` | `'PARQUET'` ★ / `'ORC'` / `'AVRO'` | Data file format. Use PARQUET for analytics. |
| `format_version` | `1` / `2` ★ | Format v2 required for full ACID (UPDATE/DELETE/MERGE). |
| `partitioning` | `ARRAY[...]` | Partition spec (transforms). See Partitioning section. |
| `sorted_by` | `ARRAY[...]` | Sort columns within each data file. |
| `location` | URI string | Storage path. Defaults to catalog warehouse path. |
| `write_target_data_file_size_bytes` | integer, default `1073741824` (1 GB) | Max data file size before rolling to a new file. |
| `orc_bloom_filter_columns` | `ARRAY['col1', 'col2']` | ORC-only: enables Bloom filters for these columns. |
| `orc_bloom_filter_fpp` | `0.0–1.0`, default `0.05` | ORC Bloom filter false-positive probability. |
| `parquet_bloom_filter_columns` | `ARRAY['col1', 'col2']` | Parquet: Bloom filters for equality / IN predicates. |

★ = recommended default.

### Connector defaults (`etc/catalog/iceberg.properties`)

```properties
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
iceberg.target-max-file-size=1GB
iceberg.sorted-writing-enabled=true
iceberg.table-statistics-enabled=true
iceberg.file-system-cache.enabled=true
iceberg.file-system-cache.directory=/tmp/trino-cache
iceberg.file-system-cache.max-size=100GB
```

Set these once globally; omit `format` and compression from individual table DDL unless overriding.

### CREATE TABLE AS SELECT (CTAS)

```sql
-- Create and populate in one step; inherits column types from the SELECT
CREATE TABLE iceberg_catalog.mydb.events_daily
WITH (
    format         = 'PARQUET',
    format_version = 2,
    partitioning   = ARRAY['dt']
) AS
SELECT
    date_trunc('day', event_ts) AS dt,
    event_type,
    region,
    count(*)        AS event_count,
    sum(amount)     AS total_amount
FROM iceberg_catalog.mydb.events
GROUP BY 1, 2, 3;
```

CTAS creates a new snapshot; run `ANALYZE` afterwards if the table will be used in joins.

### CREATE TABLE LIKE

```sql
-- Copy column definitions and table properties; does NOT copy data
CREATE TABLE iceberg_catalog.mydb.events_staging
LIKE iceberg_catalog.mydb.events
INCLUDING PROPERTIES;   -- copies format, partitioning, sorted_by, etc.
```

Use `EXCLUDING PROPERTIES` to copy only columns without the partition spec (useful for staging tables that shouldn't be partitioned the same way).

### External / registered table

Point to existing Iceberg metadata already on object storage:

```sql
CREATE TABLE iceberg_catalog.mydb.events_external (
    event_id   BIGINT,
    event_ts   TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    location = 's3://existing-bucket/path/to/table'
);
```

Trino will read the existing `metadata/` directory at that location and register it.

---

## Partitioning

### Partition transforms

| Transform | Syntax | Use case |
|---|---|---|
| Identity | `region` | Low-cardinality categorical columns |
| Day | `day(ts)` | Daily pipelines — most common temporal choice |
| Hour | `hour(ts)` | High-frequency event streams (> 5 GB/day) |
| Month | `month(ts)` | Low-volume data (< 100 MB/day) |
| Year | `year(ts)` | Archival or very sparse data |
| Bucket | `bucket(N, col)` | High-cardinality IDs — see Bucketing section |
| Truncate | `truncate(N, col)` | String prefix or integer range partitioning |

**Hidden partitioning**: users query the original column (`event_ts`, `user_id`). Trino rewrites predicates into the corresponding partition transform internally. The partition column never leaks into the schema.

### Partition sizing guideline

| Daily data volume | Recommended granularity |
|---|---|
| < 100 MB | `month(ts)` |
| 100 MB – 5 GB | `day(ts)` |
| 5 GB – 50 GB | `hour(ts)` |
| > 50 GB | `hour(ts)` + `bucket(N, high_card_col)` |

Target **256 MB – 1 GB per data file** after compaction.

### Combined partition specs

```sql
-- Temporal + categorical
partitioning = ARRAY['day(event_ts)', 'region']

-- Temporal + bucket for even distribution per day
partitioning = ARRAY['day(event_ts)', 'bucket(32, user_id)']

-- Truncate for string prefix partitioning
partitioning = ARRAY['truncate(2, country_code)', 'month(event_ts)']

-- Identity on enum
partitioning = ARRAY['status', 'day(created_at)']
```

### How partition pruning works

1. The optimizer extracts predicates on partition columns from the WHERE clause.
2. Each predicate is translated to the partition transform and matched against manifest file metadata.
3. Manifest files whose partition range doesn't match are skipped without reading any data.
4. Within surviving manifests, per-file min/max statistics skip individual data files.

```sql
-- This predicate prunes day(event_ts) partitions — only Jan files are read
SELECT * FROM events
WHERE event_ts >= TIMESTAMP '2024-01-01 00:00:00 UTC'
  AND event_ts <  TIMESTAMP '2024-02-01 00:00:00 UTC';

-- Combined pruning: day + region both applied
SELECT * FROM events
WHERE event_ts >= TIMESTAMP '2024-01-15 00:00:00 UTC'
  AND region = 'us-east-1';
```

**Pruning works for**: equality (`=`), ranges (`>`, `<`, `BETWEEN`), `IN` lists, `IS NULL` / `IS NOT NULL`.
**Pruning does NOT work for**: expressions on columns (`date_trunc('day', event_ts) = DATE '2024-01-15'` — use the raw column), OR across partition columns.

### Verify pruning in EXPLAIN

```sql
EXPLAIN (TYPE IO, FORMAT JSON)
SELECT count(*) FROM events
WHERE event_ts >= TIMESTAMP '2024-01-15 00:00:00 UTC'
  AND region = 'us-east-1';
-- Look for: "partitionsCount": N  (low N means pruning worked)
-- and "estimatedSizeInBytes" matching only the selected partitions
```

### Inspect current partition spec

```sql
-- File distribution per partition
SELECT partition, file_count, record_count,
       total_size / 1024 / 1024 AS total_mb,
       total_size / NULLIF(file_count, 0) / 1024 / 1024 AS avg_file_mb
FROM iceberg_catalog.mydb."events$partitions"
ORDER BY total_size DESC;

-- Table properties including current partition spec
SELECT * FROM iceberg_catalog.mydb."events$properties";
```

### Evolving partitioning (no rewrite)

```sql
-- Finer granularity going forward; existing files keep old layout
ALTER TABLE events
SET PROPERTIES partitioning = ARRAY['region', 'hour(event_ts)'];

-- Add a bucket dimension to an existing temporal partition
ALTER TABLE events
SET PROPERTIES partitioning = ARRAY['day(event_ts)', 'bucket(32, user_id)'];

-- Remove partitioning entirely (unpartitioned going forward)
ALTER TABLE events
SET PROPERTIES partitioning = ARRAY[];
```

Old files are not rewritten and remain queryable under both layouts.

---

## Bucketing

### What bucketing does

`bucket(N, col)` hashes the column value and assigns it to one of N bucket numbers. Bucket number is stored in partition metadata. At query time, equality predicates on the bucketed column are translated to specific bucket numbers — only manifest files for those buckets are read.

```
row with user_id=12345
  → hash(12345) mod 32 = 17
  → written to bucket 17 file
  → query WHERE user_id = 12345 → reads only bucket 17 file
```

### When to use bucketing

Use `bucket(N, col)` when:
- The column has **high cardinality** (millions+ distinct values: user_id, order_id, session_id).
- Queries frequently **filter by equality or IN-list** on that column.
- A temporal partition alone would create too many tiny files per partition, or without bucketing each partition file would be too large.
- The table participates in **joins** on that column — co-located bucketed tables joined on the same column avoid network shuffle.

Do **not** use bucketing on low-cardinality columns (< 1000 distinct values) — use identity partitioning or `truncate` instead.

### Choosing bucket count N

Start with: `N = ceil(expected_partition_size_GB / 0.5)` — aiming for ~512 MB per bucket file per partition.

| Partition size after load | Suggested N |
|---|---|
| < 4 GB/day | 8 |
| 4–16 GB/day | 32 |
| 16–64 GB/day | 64 |
| 64–256 GB/day | 128 |
| > 256 GB/day | 256+ |

**N must be a power of 2** — Iceberg's hash function relies on this for consistent distribution. Changing N later requires a full table rewrite, so choose carefully upfront.

### Create a bucketed table

```sql
-- Pure bucket partitioning (no temporal — suited for lookup tables)
CREATE TABLE iceberg_catalog.mydb.user_profiles (
    user_id     BIGINT NOT NULL,
    email       VARCHAR,
    country     CHAR(2),
    created_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format         = 'PARQUET',
    format_version = 2,
    partitioning   = ARRAY['bucket(64, user_id)'],
    sorted_by      = ARRAY['user_id']
);

-- Combined temporal + bucket (most common production pattern)
CREATE TABLE iceberg_catalog.mydb.events (
    event_id    BIGINT,
    user_id     BIGINT,
    event_ts    TIMESTAMP(6) WITH TIME ZONE,
    event_type  VARCHAR,
    payload     VARCHAR
)
WITH (
    format         = 'PARQUET',
    format_version = 2,
    partitioning   = ARRAY['day(event_ts)', 'bucket(32, user_id)'],
    sorted_by      = ARRAY['user_id', 'event_type']
);
```

### Bucket pruning mechanics

Trino translates equality and IN predicates into bucket numbers and skips all other bucket files:

```sql
-- Equality → looks up exactly bucket hash(42) mod 32
SELECT * FROM events WHERE user_id = 42;

-- IN list → reads only the buckets for each value in the list
SELECT * FROM events WHERE user_id IN (42, 1001, 9999);

-- Range predicate → cannot compute bucket set → reads ALL buckets (no pruning)
SELECT * FROM events WHERE user_id BETWEEN 1 AND 1000;
```

Verify bucket pruning via EXPLAIN IO:
```sql
EXPLAIN (TYPE IO, FORMAT JSON)
SELECT * FROM events WHERE user_id = 42 AND event_ts >= TIMESTAMP '2024-01-01 00:00:00 UTC';
-- Expected: partitionsCount = 1 (only the bucket containing user_id=42 within the day partition)
```

### Bucket pruning through joins (dynamic partition pruning)

When a bucketed table is joined to a small filtered dimension, Trino builds a Bloom filter from the dimension side and pushes it to the bucket scan — further reducing files read at runtime:

```sql
-- Trino builds a Bloom filter from the WHERE on dim_users and pushes it
-- to the events scan, skipping bucket files not containing matching user_ids
SELECT e.event_type, count(*)
FROM events e
JOIN dim_users u ON e.user_id = u.user_id
WHERE u.country = 'US'
GROUP BY 1;
```

EXPLAIN shows `DynamicFilterSource` on the build side and `DynamicFilter` applied to the scan — this is working as expected.

### Adding bucketing to an existing table

```sql
-- Affects future writes and OPTIMIZE only; existing files are not changed
ALTER TABLE events
SET PROPERTIES partitioning = ARRAY['day(event_ts)', 'bucket(32, user_id)'];
```

After changing the partition spec: run `OPTIMIZE` to compact and re-bucket existing files, then `ANALYZE`.

### Changing bucket count

There is no in-place bucket count change — existing files are tied to the original bucket mapping. To change N:

```sql
-- 1. Create new table with desired bucket count
CREATE TABLE events_v2 AS SELECT * FROM events;  -- CTAS with new WITH properties

-- 2. Swap alias / rename (coordinate with consumers)
ALTER TABLE events RENAME TO events_old;
ALTER TABLE events_v2 RENAME TO events;
```

---

## ALTER TABLE: Full Reference

### Column operations

```sql
-- Add a nullable column (visible as NULL for existing rows)
ALTER TABLE events ADD COLUMN session_id VARCHAR;

-- Add with NOT NULL constraint (only safe if table is empty or you provide a DEFAULT)
ALTER TABLE events ADD COLUMN is_bot BOOLEAN NOT NULL DEFAULT false;

-- Add a column with a comment
ALTER TABLE events ADD COLUMN campaign_id BIGINT COMMENT 'Ad campaign identifier';

-- Add a column and position it explicitly
ALTER TABLE events ADD COLUMN channel VARCHAR FIRST;
ALTER TABLE events ADD COLUMN channel VARCHAR AFTER event_type;

-- Drop a column (data in existing files is preserved but invisible to queries)
ALTER TABLE events DROP COLUMN legacy_field;

-- Rename a column (tracked by column ID — safe for time travel reads)
ALTER TABLE events RENAME COLUMN user_id TO account_id;

-- Widen a column type (only safe promotions allowed)
ALTER TABLE events ALTER COLUMN click_count SET DATA TYPE BIGINT;   -- INT → BIGINT
ALTER TABLE events ALTER COLUMN score      SET DATA TYPE DOUBLE;    -- FLOAT → DOUBLE

-- Change nullability
ALTER TABLE events ALTER COLUMN session_id SET NOT NULL;
ALTER TABLE events ALTER COLUMN session_id DROP NOT NULL;

-- Set or change default value
ALTER TABLE events ALTER COLUMN status SET DEFAULT 'active';
ALTER TABLE events ALTER COLUMN status DROP DEFAULT;

-- Reorder an existing column
ALTER TABLE events ALTER COLUMN session_id FIRST;
ALTER TABLE events ALTER COLUMN session_id AFTER event_id;

-- Add or change column comment
ALTER TABLE events ALTER COLUMN user_id COMMENT 'Internal account ID';
```

#### Nested column operations (ROW fields)

```sql
-- Add a subfield to an existing ROW column
ALTER TABLE events ADD COLUMN address.zip_code VARCHAR;

-- Drop a subfield
ALTER TABLE events DROP COLUMN address.zip_code;

-- Rename a subfield
ALTER TABLE events RENAME COLUMN address.city TO address.city_name;
```

#### Allowed type promotions

| From | To | Safe |
|---|---|---|
| `TINYINT` | `SMALLINT`, `INTEGER`, `BIGINT` | Yes |
| `SMALLINT` | `INTEGER`, `BIGINT` | Yes |
| `INTEGER` | `BIGINT` | Yes |
| `FLOAT` / `REAL` | `DOUBLE` | Yes |
| `DECIMAL(p, s)` | `DECIMAL(p', s)` where `p' > p` | Yes |
| `VARCHAR(n)` | `VARCHAR(m)` where `m > n` or unbounded `VARCHAR` | Yes |
| Any type | Incompatible type | No — requires table rebuild |

### Table property changes

```sql
-- Change partition spec (in-place; old files keep old layout)
ALTER TABLE events SET PROPERTIES partitioning = ARRAY['day(event_ts)', 'bucket(32, user_id)'];

-- Remove partitioning
ALTER TABLE events SET PROPERTIES partitioning = ARRAY[];

-- Change or add sorted_by (affects future writes and OPTIMIZE)
ALTER TABLE events SET PROPERTIES sorted_by = ARRAY['user_id', 'event_type'];

-- Remove sorting
ALTER TABLE events SET PROPERTIES sorted_by = ARRAY[];

-- Upgrade format version (v1 → v2 enables full ACID)
ALTER TABLE events SET PROPERTIES format_version = 2;

-- Change target file size for writes (bytes)
ALTER TABLE events SET PROPERTIES write_target_data_file_size_bytes = 268435456;  -- 256 MB

-- Enable Parquet Bloom filters on specific columns
ALTER TABLE events SET PROPERTIES parquet_bloom_filter_columns = ARRAY['user_id', 'session_id'];

-- Enable ORC Bloom filters
ALTER TABLE events SET PROPERTIES orc_bloom_filter_columns = ARRAY['user_id'];
ALTER TABLE events SET PROPERTIES orc_bloom_filter_fpp     = 0.01;
```

### Table rename

```sql
ALTER TABLE iceberg_catalog.mydb.events RENAME TO iceberg_catalog.mydb.events_v2;
```

### Table comment

```sql
COMMENT ON TABLE events IS 'Raw clickstream events — partitioned by day and user bucket';
COMMENT ON COLUMN events.user_id IS 'Internal account identifier';
```

### Change table owner

```sql
ALTER TABLE events SET AUTHORIZATION USER 'new_owner';
ALTER TABLE events SET AUTHORIZATION ROLE 'data_engineering';
```

### Table procedures (via ALTER TABLE EXECUTE)

These are covered in detail in Table Maintenance, but listed here for completeness:

```sql
ALTER TABLE events EXECUTE optimize(file_size_threshold => '100MB');
ALTER TABLE events EXECUTE expire_snapshots(retention_threshold => '14d', retain_last => 10);
ALTER TABLE events EXECUTE remove_orphan_files(retention_threshold => '14d');
ALTER TABLE events EXECUTE optimize_manifests;
```

### Drop table

```sql
-- Drops table metadata AND data files
DROP TABLE iceberg_catalog.mydb.events;

-- Safe drop (no error if table doesn't exist)
DROP TABLE IF EXISTS iceberg_catalog.mydb.events;
```

---

## Sorted Tables

`sorted_by` pre-sorts rows by the given columns **within each data file**. Benefits:
- Tighter min/max bounds in manifests → more file skipping for selective predicates.
- Higher compression ratios (similar values are adjacent).

A 1.4B-row event table sorted on `(user_id, event_type)` shrank from 8.09 GB / 3,619 files to 2.40 GB / 1,074 files (73% reduction).

**Rule**: set `sorted_by` on columns most used in `WHERE` equality filters and join predicates. Primary sort column should be the one with the highest cardinality in those filters.

```sql
-- Set at creation
CREATE TABLE events (...) WITH (
    partitioning = ARRAY['day(event_ts)'],
    sorted_by    = ARRAY['user_id', 'event_type']
);

-- Add to existing table (affects future writes and OPTIMIZE)
ALTER TABLE events SET PROPERTIES sorted_by = ARRAY['user_id', 'event_type'];
```

After adding `sorted_by`, run `OPTIMIZE` to compact and re-sort existing files, then `ANALYZE`.

---

## EXPLAIN: Reading the Plan

```sql
-- Distributed plan: stages, join types, exchanges
EXPLAIN (TYPE DISTRIBUTED) SELECT ...;

-- IO plan: scan row estimates used by optimizer (JSON for tooling)
EXPLAIN (TYPE IO, FORMAT JSON) SELECT ...;

-- Actual runtime stats (runs the query)
EXPLAIN ANALYZE SELECT ...;
```

### Key signals

| Signal | Meaning | Action |
|---|---|---|
| `InnerJoin[BROADCAST]` | Small table replicated to all workers | Expected for small dims; verify size estimate |
| `InnerJoin[PARTITIONED]` | Both sides hash-shuffled | Normal for large–large joins |
| `LocalExchange[GATHER]` | Single-threaded aggregation | Check partition key or add parallelism |
| `ScanFilterProject` with high estimated rows | Poor pruning or missing stats | Add partition predicate; run ANALYZE |
| `DynamicFilterSource` | Build-side Bloom filter pushed to scan | Good — reduces probe scan at runtime |
| low `partitionsCount` in IO plan | Partition pruning working | Confirm expected count |
| high `partitionsCount` despite filter | Predicate not pruning | Check column is a partition transform; avoid expressions |

---

## Statistics (ANALYZE)

Manifest files automatically track min/max per file — no ANALYZE needed for file-level skipping. ANALYZE additionally computes table-level row counts, null fractions, and **NDV** (required for join cardinality estimation and join reordering).

```sql
-- Full table
ANALYZE iceberg_catalog.mydb.events;

-- Specific columns only (faster, target join and filter columns)
ANALYZE iceberg_catalog.mydb.events
  WITH (columns = ARRAY['user_id', 'event_type', 'region']);

-- Single partition (incremental refresh)
ANALYZE iceberg_catalog.mydb.events
  WITH (partitioning = ARRAY['dt=2024-01-15']);

-- Inspect current stats
SHOW STATS FOR iceberg_catalog.mydb.events;
```

Run ANALYZE after: initial load, bulk DML > 10% of table, OPTIMIZE, or when EXPLAIN shows wrong row estimates.

---

## Join Optimization

```sql
SET SESSION join_reordering_strategy       = 'AUTOMATIC';   -- CBO reorders with stats
SET SESSION join_distribution_type         = 'AUTOMATIC';   -- CBO chooses broadcast vs partitioned
SET SESSION join_max_broadcast_table_size  = '200MB';       -- default 100 MB
```

Force broadcast when CBO underestimates the build side:
```sql
SELECT /*+ BROADCAST(small_dim) */ f.*, d.name
FROM large_fact f
JOIN small_dim d ON f.dim_id = d.id;
```

Enable adaptive join reordering (corrects estimates mid-execution):
```properties
fault-tolerant-execution-enabled=true
fault-tolerant-execution-adaptive-query-planning-enabled=true
```

---

## DML

### INSERT

```sql
INSERT INTO events SELECT * FROM staging_events;
INSERT INTO events (event_id, event_type, user_id, event_ts) VALUES (1, 'click', 42, CURRENT_TIMESTAMP);
```

### UPDATE (copy-on-write)

Trino rewrites affected data files entirely. Align the WHERE predicate with partition columns to minimize rewritten files:

```sql
UPDATE users
SET status = 'inactive'
WHERE region = 'us-east-1'
  AND last_login_ts < TIMESTAMP '2023-01-01 00:00:00 UTC';
```

### DELETE

```sql
DELETE FROM events WHERE dt = DATE '2024-01-14';                    -- partition-aligned: rewrites minimal files
DELETE FROM events WHERE event_type = 'test' AND dt < DATE '2024-01-01';
```

### MERGE — canonical patterns

**Conditional upsert:**
```sql
MERGE INTO users t
USING user_updates s ON t.user_id = s.user_id
WHEN MATCHED AND s.updated_at > t.updated_at
  THEN UPDATE SET email = s.email, status = s.status, updated_at = s.updated_at
WHEN NOT MATCHED
  THEN INSERT (user_id, email, status, updated_at)
       VALUES (s.user_id, s.email, s.status, s.updated_at);
```

**SCD Type 2:**
```sql
MERGE INTO customers t
USING customer_updates s
  ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET is_current = false, expiry_date = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, effective_date, expiry_date, is_current)
  VALUES (s.customer_id, s.name, s.email, s.updated_at, null, true);
```

**Insert-only (dedup load):**
```sql
MERGE INTO target t
USING staging s ON t.id = s.id
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val, s.updated_at);
```

### TRUNCATE

```sql
TRUNCATE TABLE events;  -- instant; creates empty snapshot without file rewrites
```

---

## Table Maintenance

Run in this order. Never skip `REMOVE_ORPHAN_FILES` after `EXPIRE_SNAPSHOTS`.

### 1. OPTIMIZE (compaction)

```sql
ALTER TABLE events EXECUTE optimize(file_size_threshold => '100MB');

-- Scope to recent partitions
ALTER TABLE events EXECUTE optimize
WHERE event_ts >= CURRENT_TIMESTAMP - INTERVAL '2' DAY;

-- Scope by file modification time
ALTER TABLE events EXECUTE optimize
WHERE "$file_modified_time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY;
```

### 2. EXPIRE_SNAPSHOTS

```sql
ALTER TABLE events EXECUTE expire_snapshots(
    retention_threshold          => '14d',
    retain_last                  => 10,
    clean_expired_metadata_files => true
);
```

### 3. REMOVE_ORPHAN_FILES

`retention_threshold` must be ≥ your cluster's `query.max-run-time`:

```sql
ALTER TABLE events EXECUTE remove_orphan_files(retention_threshold => '14d');
```

### 4. OPTIMIZE_MANIFESTS

```sql
ALTER TABLE events EXECUTE optimize_manifests;
```

### 5. ANALYZE (after any optimize)

```sql
ANALYZE iceberg_catalog.mydb.events;
```

### Recommended schedule

```sql
-- Daily: compact recent partitions only
ALTER TABLE events EXECUTE optimize
WHERE event_ts >= CURRENT_TIMESTAMP - INTERVAL '2' DAY;

-- Weekly:
ALTER TABLE events EXECUTE expire_snapshots(
    retention_threshold => '14d', retain_last => 10,
    clean_expired_metadata_files => true
);
ALTER TABLE events EXECUTE remove_orphan_files(retention_threshold => '14d');
ALTER TABLE events EXECUTE optimize_manifests;
ANALYZE iceberg_catalog.mydb.events;
```

---

## Time Travel and Rollback

```sql
-- Query a past snapshot by ID
SELECT * FROM events FOR VERSION AS OF 1234567890;

-- Query the state at a timestamp
SELECT * FROM events FOR TIMESTAMP AS OF TIMESTAMP '2024-01-14 12:00:00 UTC';

-- Daily delta
SELECT new.cnt - old.cnt AS delta
FROM (SELECT count(*) AS cnt FROM events) new,
     (SELECT count(*) AS cnt FROM events
      FOR TIMESTAMP AS OF CURRENT_TIMESTAMP - INTERVAL '1' DAY) old;

-- Roll back to a snapshot (creates a new snapshot)
CALL iceberg_catalog.system.rollback_to_snapshot(
    schema_name => 'mydb', table_name => 'events', snapshot_id => 1234567890
);
CALL iceberg_catalog.system.rollback_to_timestamp(
    schema_name => 'mydb', table_name => 'events',
    timestamp   => TIMESTAMP '2024-01-14 12:00:00.000 UTC'
);
```

List snapshots:
```sql
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg_catalog.mydb."events$snapshots"
ORDER BY committed_at DESC LIMIT 10;
```

---

## Metadata Tables

| Metadata table | Contents |
|---|---|
| `"t$snapshots"` | Snapshot log: IDs, timestamps, operations |
| `"t$history"` | Schema/partition/property change log |
| `"t$files"` | All data files in current snapshot with sizes and column stats |
| `"t$manifests"` | Manifest files: paths, lengths, partition summaries |
| `"t$partitions"` | Per-partition: file count, record count, total size |
| `"t$refs"` | Named branches and tags |
| `"t$properties"` | Table properties key/value |

### Diagnostic queries

```sql
-- Partitions with too many small files → candidates for OPTIMIZE
SELECT partition, file_count,
       total_size / 1024 / 1024 AS total_mb,
       total_size / NULLIF(file_count, 0) / 1024 / 1024 AS avg_file_mb
FROM iceberg_catalog.mydb."events$partitions"
WHERE total_size / NULLIF(file_count, 0) < 64 * 1024 * 1024
ORDER BY file_count DESC;

-- Largest partitions
SELECT partition, record_count,
       total_size / 1024.0 / 1024 / 1024 AS total_gb
FROM iceberg_catalog.mydb."events$partitions"
ORDER BY total_size DESC LIMIT 20;

-- Snapshot growth rate
SELECT committed_at, operation, added_files_count, deleted_files_count
FROM iceberg_catalog.mydb."events$snapshots"
ORDER BY committed_at DESC LIMIT 20;
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| `SELECT *` on wide tables | Reads all columns; defeats columnar pushdown | Enumerate required columns |
| No ANALYZE after large loads | Default row estimates → bad join order | Run ANALYZE after every bulk DML |
| OPTIMIZE without ANALYZE after | Statistics reflect old file layout | Always run ANALYZE after OPTIMIZE |
| EXPIRE_SNAPSHOTS without REMOVE_ORPHAN_FILES | Orphan files accumulate | Always pair the two procedures |
| `retention_threshold` < longest query | Running queries read deleted files | Set threshold ≥ `query.max-run-time` |
| `hour(ts)` on low-volume tables | Millions of tiny files | Use `day(ts)` or `month(ts)` |
| Bucket count N not a power of 2 | Uneven distribution | Always use powers of 2: 8, 16, 32, 64, 128... |
| Bucket count too high | Too many small files per partition | Aim for ~512 MB per bucket file |
| Range predicate on bucketed column | No bucket pruning — reads all buckets | Use equality or IN; for ranges, add a temporal partition |
| MERGE/UPDATE without partition predicate | Full-table COW rewrite | Include partition column in WHERE |
| AVRO for analytics tables | Row-oriented; no column skipping | Use PARQUET for all analytics tables |
| Skipping OPTIMIZE indefinitely | Small files accumulate → slow manifest scans | Run daily on recent partitions |
| Changing bucket count in-place | Not possible — old files use old bucket mapping | CTAS into new table with desired N, then rename |
| Using expressions on partition columns in WHERE | Prevents predicate pushdown to partition pruning | Use raw column: `event_ts >= ...` not `date_trunc('day', event_ts) = ...` |

---

## Output Expectations

When working on Trino + Iceberg tasks:
- Show the `EXPLAIN (TYPE DISTRIBUTED)` excerpt highlighting the problem (join type, estimated rows, exchange type).
- Propose specific `CREATE TABLE` DDL or `ALTER TABLE SET PROPERTIES` with explicit `partitioning`, `sorted_by`, and `format_version`.
- For bucketing questions: state the chosen N, explain why, show how to verify pruning with `EXPLAIN (TYPE IO)`.
- Name maintenance commands in order and explain why order matters.
- State when `ANALYZE` is needed and which columns to include.
- Provide the `$partitions` diagnostic query to validate the fix after implementation.

---

## References

- Local deep-dive spec: `docs/specs/trino_iceberg_performance_optimization.md`
- Trino optimizer: https://trino.io/docs/current/optimizer.html
- Trino Iceberg connector: https://trino.io/docs/current/connector/iceberg.html
