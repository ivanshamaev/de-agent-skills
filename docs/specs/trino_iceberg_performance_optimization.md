# Trino Iceberg Performance and Optimization

Source: https://trino.io/docs/current/optimizer.html, https://trino.io/docs/current/connector/iceberg.html, and Starburst blog series on Apache Iceberg.

---

## Table of Contents

1. [Apache Iceberg Architecture Overview](#1-apache-iceberg-architecture-overview)
2. [Iceberg vs Hive: Why the Switch](#2-iceberg-vs-hive-why-the-switch)
3. [Trino Optimizer Fundamentals](#3-trino-optimizer-fundamentals)
4. [Cost-Based Optimization and Statistics](#4-cost-based-optimization-and-statistics)
5. [Join Strategies and Distribution](#5-join-strategies-and-distribution)
6. [Pushdown Optimizations](#6-pushdown-optimizations)
7. [Adaptive Query Execution](#7-adaptive-query-execution)
8. [Iceberg Connector Configuration](#8-iceberg-connector-configuration)
9. [Partitioning and Partition Pruning](#9-partitioning-and-partition-pruning)
10. [Sorted Tables](#10-sorted-tables)
11. [File Formats and Compression](#11-file-formats-and-compression)
12. [Table Statistics with ANALYZE](#12-table-statistics-with-analyze)
13. [DML: INSERT, UPDATE, DELETE, MERGE](#13-dml-insert-update-delete-merge)
14. [Table Maintenance](#14-table-maintenance)
15. [Schema Evolution](#15-schema-evolution)
16. [Time Travel and Rollback](#16-time-travel-and-rollback)
17. [Metadata Tables](#17-metadata-tables)
18. [Materialized Views](#18-materialized-views)
19. [Anti-Patterns and Best Practices](#19-anti-patterns-and-best-practices)
20. [Quick Reference](#20-quick-reference)

---

## 1. Apache Iceberg Architecture Overview

### Metadata Hierarchy

Iceberg tables use a three-level metadata hierarchy stored entirely in object storage. There are no external metastore locks or server-side file listings.

```
catalog (Hive Metastore / Glue / REST / Nessie)
  └── table metadata file  (table-metadata-<uuid>.json)
        ├── schema history
        ├── partition spec history
        ├── snapshot log
        └── manifest list  (snap-<snapshot_id>-<attempt_id>-<commit_uuid>.avro)
              └── manifest file  (...)
                    └── data file references  (Parquet / ORC / Avro)
```

**Key properties:**
- Each commit produces a new **snapshot** — an immutable pointer to a complete table state.
- Manifest files record data file paths, partition values, and column-level statistics (row count, null count, min/max bounds).
- Snapshot isolation is inherent: readers see a consistent view without blocking writers.

### Three-Layer Filtering

At query time Trino applies filtering in three layers before reading any row data:

1. **Partition pruning** — eliminate manifest files whose partition values don't match the predicate.
2. **Manifest metadata filtering** — skip individual data files using file-level min/max statistics stored in manifests.
3. **Parquet/ORC page-level filtering** — skip row groups or stripes using column statistics embedded in the data files.

All three layers work together: a well-partitioned, sorted table with collected statistics can achieve orders-of-magnitude scan reduction.

---

## 2. Iceberg vs Hive: Why the Switch

| Capability | Hive (ORC/Parquet on HDFS/S3) | Apache Iceberg |
|---|---|---|
| ACID transactions | Limited (Hive ACID, NameNode-heavy) | Full ACID via snapshot commits |
| File listing performance | Slow: O(n) metastore/object-store listings | Fast: O(1) snapshot pointer lookup |
| Partition evolution | Requires table rebuild | In-place partition spec change |
| Schema evolution | Rename/add columns only; type changes risky | Add, drop, rename, reorder, widen types safely |
| Time travel | Not supported | FOR VERSION AS OF / FOR TIMESTAMP AS OF |
| Hidden partitioning | No (partition columns exposed to users) | Yes (transforms invisible to queries) |
| Concurrent writers | Not safe | Optimistic concurrency with retry |
| Column statistics | Post-hoc MSCK REPAIR | Embedded in manifest files per data file |

**Performance headline**: Iceberg eliminates the slow `LIST` calls that Hive requires for every query planning phase. A Hive table with millions of partitions triggers millions of object-store API calls per query; Iceberg reads a single snapshot manifest list.

---

## 3. Trino Optimizer Fundamentals

Trino's optimizer operates as a cost-based rule engine. It applies a sequence of logical and physical transformation rules to the query plan and selects the lowest-cost alternative.

### Optimizer Pipeline

```
SQL Text
  → Parser → AST
  → Analyzer (semantic validation, type resolution)
  → Logical Plan
  → Rule-based transformations (predicate pushdown, projection pruning, …)
  → Cost estimation (row counts, data sizes, NDV)
  → Physical plan selection (join order, join type, exchange type)
  → Stage/fragment scheduling
  → Execution
```

### Key Session Properties

```sql
-- Enable/disable cost-based optimizer
SET SESSION optimizer_use_cbo = true;

-- Join reordering strategy
SET SESSION join_reordering_strategy = 'AUTOMATIC';  -- default

-- Join distribution type
SET SESSION join_distribution_type = 'AUTOMATIC';   -- default

-- Max broadcast table size (default 100 MB)
SET SESSION join_max_broadcast_table_size = '200MB';
```

---

## 4. Cost-Based Optimization and Statistics

### Statistics Used by the Optimizer

| Statistic | Description | Source |
|---|---|---|
| Row count | Estimated number of rows after filters | Table stats / ANALYZE |
| Data size | Estimated bytes after filters | Table stats / ANALYZE |
| Null fraction | Fraction of NULLs per column | ANALYZE |
| NDV (distinct values) | Number of distinct values per column | ANALYZE |
| Min / Max | Column value bounds | Manifest files (automatic) |

### Enabling Statistics Collection

```sql
-- Collect full column statistics (triggers a full scan)
ANALYZE iceberg_catalog.mydb.events;

-- Collect stats for specific columns only
ANALYZE iceberg_catalog.mydb.events
  WITH (columns = ARRAY['event_type', 'user_id', 'region']);

-- Collect stats for a specific partition
ANALYZE iceberg_catalog.mydb.events
  WITH (partitioning = ARRAY['region=us-east-1']);
```

Statistics are stored in Iceberg table metadata and are invalidated automatically when new snapshots are created by DML. Re-run ANALYZE after large loads.

### Inspecting Statistics

```sql
SHOW STATS FOR iceberg_catalog.mydb.events;

-- Result columns: column_name, data_size, distinct_values_count,
--                 nulls_fraction, row_count, low_value, high_value
```

---

## 5. Join Strategies and Distribution

### Join Reordering

Controlled by `join_reordering_strategy`:

| Value | Behavior |
|---|---|
| `AUTOMATIC` | CBO reorders joins using statistics; falls back to source order if stats unavailable |
| `ELIMINATE_CROSS_JOINS` | Reorders only to eliminate cross joins; does not do full CBO reorder |
| `NONE` | Joins executed in the order written in SQL |

`AUTOMATIC` is the default and recommended setting when table statistics are collected.

### Join Distribution Types

| Type | When Used | Mechanics |
|---|---|---|
| `BROADCAST` | Small build side | Build-side table replicated to all workers; no shuffle of probe side |
| `PARTITIONED` | Large build side | Both sides hash-partitioned and shuffled by join key |
| `AUTOMATIC` | Default | CBO chooses based on estimated sizes |

**Broadcast threshold**: controlled by `join_max_broadcast_table_size` (default `100MB`). If the build side estimated size is below this threshold, CBO may choose BROADCAST.

```sql
-- Force broadcast for a specific join
SELECT /*+ BROADCAST(small_dim) */ *
FROM large_fact f
JOIN small_dim d ON f.dim_id = d.id;
```

### Practical Tips

- Collect statistics on all join columns with ANALYZE.
- Place the largest table first in multi-way joins when using `NONE` strategy (left-deep plan).
- Use `EXPLAIN` to verify join types chosen:

```sql
EXPLAIN (TYPE DISTRIBUTED)
SELECT ...;
```

---

## 6. Pushdown Optimizations

Trino pushes operations down to the connector layer when the connector supports it. The Iceberg connector supports all major pushdown types.

### Predicate Pushdown

Filter conditions are pushed into the scan operator and used for:
- **Partition pruning**: predicates on partition columns skip entire manifest files.
- **File-level filtering**: min/max bounds in manifests skip individual data files.
- **Page-level filtering**: Parquet/ORC row group statistics skip internal pages.

```sql
-- Partition predicate — Trino pushes dt='2024-01-15' into scan
SELECT count(*) FROM events WHERE dt = DATE '2024-01-15';

-- Range predicate on sorted column — skips row groups via min/max
SELECT * FROM events WHERE event_ts BETWEEN TIMESTAMP '2024-01-15 00:00:00'
                                        AND TIMESTAMP '2024-01-15 01:00:00';
```

### Projection Pushdown

Only requested columns are read from Parquet/ORC files. Never use `SELECT *` in production queries — it reads all columns including wide or nested ones.

### Dereference Pushdown

For nested (struct) columns, Trino pushes field dereference into the scan so only the required sub-fields are read from Parquet.

```sql
-- Only the 'address.city' sub-field is read, not the whole 'address' struct
SELECT user_id, address.city FROM users WHERE address.country = 'US';
```

### Aggregation Pushdown

Simple aggregations (`COUNT`, `SUM`, `MIN`, `MAX`) can be partially pushed to the connector, reducing data shuffled across the network.

### Limit / TopN Pushdown

`LIMIT` and `ORDER BY ... LIMIT` (TopN) are pushed to scan operators so each worker returns only the required rows before any global merge.

### Join Pushdown

When both sides of a join reside in the same Iceberg catalog, the connector can execute the join closer to the data. This is catalog-specific and may not always be triggered.

---

## 7. Adaptive Query Execution

### Fault-Tolerant Execution

Enable fault-tolerant execution (FTE) for large, long-running queries. FTE checkpoints intermediate results so tasks can be retried individually on failure.

```properties
# trino coordinator config.properties
fault-tolerant-execution-enabled=true
```

### Adaptive Join Reordering (AQE)

When FTE is enabled, Trino can re-evaluate join order after collecting runtime statistics from the first stages:

```properties
fault-tolerant-execution-adaptive-query-planning-enabled=true
```

This lets the planner correct initial estimates when statistics were absent or stale. The plan may be revised mid-execution based on actual cardinalities observed.

### Dynamic Filtering

Trino builds a runtime Bloom filter from the build side of a join and pushes it to the probe-side scan. This further reduces rows read from data files during execution.

Dynamic filtering works transparently — no configuration needed. It is most effective when:
- The build side is small (after any predicate filtering).
- The probe side is large and scanned from Iceberg files.

---

## 8. Iceberg Connector Configuration

### Catalog Properties (`etc/catalog/iceberg.properties`)

```properties
connector.name=iceberg

# Metastore type: hive, glue, rest, nessie, jdbc
iceberg.catalog.type=hive

# File format for new tables (PARQUET recommended)
iceberg.file-format=PARQUET

# Compression codec (ZSTD for best compression/speed ratio)
iceberg.compression-codec=ZSTD

# Target max data file size before rolling to a new file (default 1 GB)
iceberg.target-max-file-size=1GB

# Enable sorted writing (requires sorted_by table property)
iceberg.sorted-writing-enabled=true

# Metadata cache TTL (reduce for high-write workloads)
iceberg.metadata-cache-enabled=true
iceberg.table-statistics-enabled=true

# Split size for reads
iceberg.split-size=128MB

# File system caching (local SSD)
iceberg.file-system-cache.enabled=true
iceberg.file-system-cache.directory=/tmp/trino-cache
iceberg.file-system-cache.max-size=100GB
```

### Table-Level Properties

```sql
CREATE TABLE iceberg_catalog.mydb.events (
    event_id    BIGINT,
    event_type  VARCHAR,
    user_id     BIGINT,
    region      VARCHAR,
    event_ts    TIMESTAMP(6) WITH TIME ZONE,
    payload     VARCHAR
)
WITH (
    format           = 'PARQUET',
    partitioning     = ARRAY['region', 'day(event_ts)'],
    sorted_by        = ARRAY['user_id', 'event_type'],
    format_version   = 2,
    location         = 's3://my-bucket/warehouse/mydb/events'
);
```

---

## 9. Partitioning and Partition Pruning

### Partition Transforms

Iceberg supports computed partition transforms — the transform is applied to the column value at write time; the original column stays unchanged for queries.

| Transform | Syntax | Use Case |
|---|---|---|
| Identity | `region` | Low-cardinality categorical columns |
| Year | `year(ts)` | Date-range queries on year boundaries |
| Month | `month(ts)` | Monthly reporting |
| Day | `day(ts)` | Daily data pipelines |
| Hour | `hour(ts)` | High-frequency event streams |
| Bucket | `bucket(N, col)` | High-cardinality columns (user_id, order_id) |
| Truncate | `truncate(N, col)` | String prefix or integer range |

**Hidden partitioning**: Users query the original column (`event_ts`). Trino rewrites the predicate into the appropriate partition transform internally. No partition column leaks into the schema.

```sql
-- User writes natural predicate; Trino prunes partitions automatically
SELECT * FROM events
WHERE event_ts >= TIMESTAMP '2024-01-01 00:00:00 UTC'
  AND event_ts <  TIMESTAMP '2024-02-01 00:00:00 UTC';
```

### Partition Sizing Guidelines

| Data volume per period | Recommended granularity |
|---|---|
| < 100 MB/day | `month(ts)` or `year(ts)` |
| 100 MB – 5 GB/day | `day(ts)` |
| 5 GB – 50 GB/day | `hour(ts)` |
| > 50 GB/day | `hour(ts)` + `bucket(N, high_card_col)` |

Too many small files per partition harm performance. Target **128 MB – 1 GB per data file** after compaction.

### Partition Evolution

Change partition spec without rewriting existing data:

```sql
-- Switch from monthly to daily partitioning going forward
ALTER TABLE events
SET PROPERTIES partitioning = ARRAY['region', 'day(event_ts)'];
```

Old files retain the old partition layout; new writes use the new spec. Trino handles both layouts transparently.

### Checking Current Partition Spec

```sql
SELECT * FROM iceberg_catalog.mydb."events$partitions";
```

---

## 10. Sorted Tables

### Overview

Sorted tables write data files with rows pre-sorted by specified columns within each partition. This enables:
- **Better compression**: similar values are adjacent → higher RLE/dictionary compression ratios.
- **More aggressive file skipping**: the min/max bounds in manifests become tighter, skipping more files for selective point queries.

### Benchmark Results (Starburst)

A 1.4-billion-row event dataset:
- Without sorting: 8.09 GB, 3,619 data files
- With sorting on `(user_id, event_type)`: 2.40 GB, 1,074 files (**73% size reduction**)

Queries filtering on `user_id` saw proportionally fewer files scanned.

### Configuration

```sql
-- Set sorted_by at table creation
CREATE TABLE events (...)
WITH (
    partitioning = ARRAY['day(event_ts)'],
    sorted_by    = ARRAY['user_id', 'event_type']
);

-- Add sorting to an existing table (affects future writes only)
ALTER TABLE events
SET PROPERTIES sorted_by = ARRAY['user_id', 'event_type'];

-- Enable globally in connector config
-- iceberg.sorted-writing-enabled=true
```

### Interaction with OPTIMIZE

After running `OPTIMIZE`, compacted files respect the `sorted_by` property — compaction re-sorts rows. Always run `ANALYZE` after optimize to refresh statistics.

---

## 11. File Formats and Compression

### Format Comparison

| Format | Read Performance | Write Overhead | Recommended Use |
|---|---|---|---|
| PARQUET | Best (columnar, vectorized) | Medium | Default for analytics |
| ORC | Good (columnar, vectorized) | Medium | Hive compatibility |
| AVRO | Poor for analytics | Low | Streaming/CDC ingestion only |

**Use PARQUET for all analytical workloads.**

### Compression Codecs

| Codec | Compression Ratio | CPU Cost | Recommended Use |
|---|---|---|---|
| `ZSTD` | Best | Low-Medium | Default recommendation |
| `SNAPPY` | Good | Very Low | High-throughput, CPU-constrained |
| `GZIP` | Excellent | High | Cold storage / archival |
| `NONE` | None | None | Debugging only |

```sql
-- Table-level compression
CREATE TABLE events (...) WITH (format = 'PARQUET', orc_compression_codec = 'ZSTD');

-- Or set globally in connector properties
-- iceberg.compression-codec=ZSTD
```

---

## 12. Table Statistics with ANALYZE

### When to Run ANALYZE

- After initial data load.
- After large `INSERT` / `MERGE` operations (> 10% of table size changed).
- After `OPTIMIZE` (file sizes changed; statistics may be stale).
- Before complex multi-join queries where the optimizer chose a bad plan.

### Incremental Statistics

Use partition-level ANALYZE to limit scan scope:

```sql
-- Only re-analyze the most recent day's partition
ANALYZE iceberg_catalog.mydb.events
WITH (partitioning = ARRAY['dt=2024-01-15']);
```

### Automatic Statistics from Manifests

Min/max bounds per data file are always written automatically by Iceberg — no ANALYZE needed for file skipping. ANALYZE additionally computes:
- Table-level row counts
- Column-level null fractions
- NDV (number of distinct values) per column

NDV is required for join cardinality estimation and join reordering.

---

## 13. DML: INSERT, UPDATE, DELETE, MERGE

### INSERT

```sql
-- Append rows
INSERT INTO events SELECT * FROM staging_events;

-- Insert with explicit columns
INSERT INTO events (event_id, event_type, user_id, event_ts)
VALUES (1, 'click', 42, CURRENT_TIMESTAMP);
```

### UPDATE

Iceberg implements UPDATE as copy-on-write by default: affected rows are rewritten into new data files; old files are preserved in the previous snapshot.

```sql
UPDATE users
SET status = 'inactive'
WHERE last_login_ts < TIMESTAMP '2023-01-01 00:00:00 UTC';
```

### DELETE

```sql
-- Delete matching rows (rewrites affected files)
DELETE FROM events WHERE event_type = 'test' AND dt < DATE '2024-01-01';

-- Partition-aligned delete is most efficient — rewrites minimal files
DELETE FROM events WHERE dt = DATE '2024-01-14';
```

### MERGE (Upsert)

MERGE is the workhorse for SCD patterns and CDC pipelines.

**Insert-only (new records from staging):**
```sql
MERGE INTO target t
USING staging s ON t.id = s.id
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val, s.updated_at);
```

**Conditional upsert:**
```sql
MERGE INTO users t
USING user_updates s ON t.user_id = s.user_id
WHEN MATCHED AND s.updated_at > t.updated_at
  THEN UPDATE SET email = s.email, updated_at = s.updated_at
WHEN NOT MATCHED
  THEN INSERT (user_id, email, updated_at)
       VALUES (s.user_id, s.email, s.updated_at);
```

**SCD Type 2 (history tracking):**
```sql
MERGE INTO customers t
USING customer_updates s ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET is_current = false, expiry_date = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, effective_date, expiry_date, is_current)
  VALUES (s.customer_id, s.name, s.email, s.updated_at, null, true);
```

### TRUNCATE

Instantly drops all data without rewriting — creates a new snapshot pointing to an empty manifest list.

```sql
TRUNCATE TABLE events;
```

### Copy-on-Write vs Merge-on-Read

Iceberg format version 2 supports **merge-on-read** (MOR) for UPDATE/DELETE via delete files (position delete and equality delete). MOR reduces write amplification but increases read cost — reads must merge delete files.

Trino currently uses **copy-on-write** (COW) by default, which rewrites affected data files. COW is better for read-heavy workloads.

---

## 14. Table Maintenance

### Overview

Regular maintenance prevents query performance degradation caused by:
- Small files (many files → many split plannings, manifest lookups)
- Stale snapshots (growing manifest lists)
- Orphan files (failed writes leaving unreferenced files)
- Bloated manifests (too many data file entries per manifest)

### OPTIMIZE (File Compaction)

Rewrites small data files into larger ones within each partition. Respects `sorted_by` if set.

```sql
-- Compact all files below the default threshold (target-max-file-size)
ALTER TABLE events EXECUTE optimize;

-- Compact only files smaller than a specific threshold
ALTER TABLE events EXECUTE optimize(file_size_threshold => '100MB');

-- Scope to recent partitions only (most impactful for incremental pipelines)
ALTER TABLE events EXECUTE optimize
WHERE event_ts >= CURRENT_TIMESTAMP - INTERVAL '7' DAY;

-- Filter by file modification time (targets recently written small files)
ALTER TABLE events EXECUTE optimize
WHERE "$file_modified_time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY;
```

Run `ANALYZE` after `OPTIMIZE` to refresh statistics to reflect new file boundaries.

### EXPIRE_SNAPSHOTS

Removes snapshot metadata and the data files exclusively referenced by those snapshots. Does not delete files still referenced by retained snapshots.

```sql
-- Retain last 7 days of snapshots
ALTER TABLE events EXECUTE expire_snapshots(retention_threshold => '7d');

-- Retain at least the last N snapshots regardless of age
ALTER TABLE events EXECUTE expire_snapshots(
    retention_threshold     => '7d',
    retain_last             => 5
);

-- Also clean expired metadata files (recommended)
ALTER TABLE events EXECUTE expire_snapshots(
    retention_threshold => '7d',
    clean_expired_metadata_files => true
);
```

**Important**: Always run `REMOVE_ORPHAN_FILES` after `EXPIRE_SNAPSHOTS` — expired snapshots may leave orphan files not cleaned by expiration alone.

### REMOVE_ORPHAN_FILES

Deletes data files in the table location that are not referenced by any current snapshot.

```sql
ALTER TABLE events EXECUTE remove_orphan_files(retention_threshold => '7d');
```

Set `retention_threshold` to at least as long as your longest-running query to avoid deleting files being read by concurrent queries.

### OPTIMIZE_MANIFESTS

Rewrites manifest files to reduce their count and ensure each manifest covers a balanced number of data files.

```sql
ALTER TABLE events EXECUTE optimize_manifests;
```

Run this after heavy writes that created many small manifests.

### Recommended Maintenance Schedule

```sql
-- Daily: compact recent partitions
ALTER TABLE events EXECUTE optimize
WHERE event_ts >= CURRENT_TIMESTAMP - INTERVAL '2' DAY;

-- Weekly: expire old snapshots and clean orphans together
ALTER TABLE events EXECUTE expire_snapshots(
    retention_threshold => '14d',
    retain_last => 10,
    clean_expired_metadata_files => true
);
ALTER TABLE events EXECUTE remove_orphan_files(retention_threshold => '14d');

-- Weekly: rebalance manifests after heavy writes
ALTER TABLE events EXECUTE optimize_manifests;

-- After any optimize: refresh statistics
ANALYZE iceberg_catalog.mydb.events;
```

### Automating Maintenance

Maintenance can be driven by an Airflow DAG or a Python script using the Trino REST API. A schedule table approach:

```sql
-- Maintenance configuration table
CREATE TABLE iceberg_maintenance_schedule (
    table_name              VARCHAR,
    last_optimized_at       TIMESTAMP,
    last_expired_at         TIMESTAMP,
    optimize_frequency_days INTEGER,
    expire_frequency_days   INTEGER,
    retention_days          INTEGER
);
```

Query this table in a maintenance DAG to determine which tables are due.

---

## 15. Schema Evolution

Iceberg supports safe in-place schema changes. Existing data files are never rewritten; the schema change is recorded in table metadata.

### Supported Changes

```sql
-- Add a new column (nullable by default)
ALTER TABLE events ADD COLUMN session_id VARCHAR;

-- Add a column with a default value for existing rows
ALTER TABLE events ADD COLUMN is_bot BOOLEAN DEFAULT false;

-- Drop a column (data in files is preserved but invisible)
ALTER TABLE events DROP COLUMN legacy_field;

-- Rename a column
ALTER TABLE events RENAME COLUMN user_id TO account_id;

-- Change column type (widening only: INT → BIGINT, FLOAT → DOUBLE)
ALTER TABLE events ALTER COLUMN click_count SET DATA TYPE BIGINT;

-- Reorder columns
ALTER TABLE events ALTER COLUMN session_id FIRST;
ALTER TABLE events ALTER COLUMN session_id AFTER event_id;
```

### Type Promotion Rules

| From | To | Safe? |
|---|---|---|
| INT | BIGINT | Yes |
| FLOAT | DOUBLE | Yes |
| DECIMAL(p, s) | DECIMAL(p', s') where p' ≥ p | Yes |
| VARCHAR(n) | VARCHAR(m) where m ≥ n | Yes |
| Any type | Incompatible type | No — requires table rebuild |

### Column IDs

Iceberg tracks columns by numeric ID, not name. Renaming a column does not break queries using the old name in time-travel reads — the ID mapping resolves correctly.

---

## 16. Time Travel and Rollback

### Query Historical Snapshots

```sql
-- By snapshot ID
SELECT * FROM events FOR VERSION AS OF 1234567890;

-- By timestamp (resolves to the snapshot at or before that time)
SELECT * FROM events FOR TIMESTAMP AS OF TIMESTAMP '2024-01-14 12:00:00 UTC';

-- Compare current vs yesterday
SELECT new.count - old.count AS daily_delta
FROM (SELECT count(*) AS count FROM events) new,
     (SELECT count(*) AS count
      FROM events FOR TIMESTAMP AS OF CURRENT_TIMESTAMP - INTERVAL '1' DAY) old;
```

### List Snapshots

```sql
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg_catalog.mydb."events$snapshots"
ORDER BY committed_at DESC
LIMIT 10;
```

### Rollback to a Previous Snapshot

```sql
-- Roll back the table to a specific snapshot (creates a new snapshot)
CALL iceberg_catalog.system.rollback_to_snapshot(
    schema_name   => 'mydb',
    table_name    => 'events',
    snapshot_id   => 1234567890
);

-- Roll back to a timestamp
CALL iceberg_catalog.system.rollback_to_timestamp(
    schema_name => 'mydb',
    table_name  => 'events',
    timestamp   => TIMESTAMP '2024-01-14 12:00:00.000 UTC'
);
```

### Use Cases

| Use Case | Approach |
|---|---|
| Audit past data state | `FOR TIMESTAMP AS OF` |
| Undo accidental DELETE/UPDATE | `rollback_to_snapshot` |
| Incremental processing (new rows since last run) | Compare snapshots in `$snapshots` |
| Reproducing a historical report | `FOR VERSION AS OF` |
| Debugging data quality regressions | Time travel + diff query |

---

## 17. Metadata Tables

Access Iceberg metadata by appending a suffix to the table name in double quotes.

```sql
-- Snapshot history
SELECT * FROM iceberg_catalog.mydb."events$snapshots";

-- Table change log (schema, partition, property changes)
SELECT * FROM iceberg_catalog.mydb."events$history";

-- All data files in current snapshot with sizes and statistics
SELECT * FROM iceberg_catalog.mydb."events$files";

-- Manifest files
SELECT * FROM iceberg_catalog.mydb."events$manifests";

-- Partition-level statistics
SELECT * FROM iceberg_catalog.mydb."events$partitions";

-- Named references (branches and tags)
SELECT * FROM iceberg_catalog.mydb."events$refs";

-- Properties
SELECT * FROM iceberg_catalog.mydb."events$properties";
```

### Useful Diagnostic Queries

```sql
-- Find partitions with too many small files (candidates for OPTIMIZE)
SELECT partition, file_count, record_count,
       total_size / 1024 / 1024 AS total_mb,
       total_size / NULLIF(file_count, 0) / 1024 / 1024 AS avg_file_mb
FROM iceberg_catalog.mydb."events$partitions"
WHERE total_size / NULLIF(file_count, 0) < 64 * 1024 * 1024  -- avg < 64 MB
ORDER BY file_count DESC;

-- Find largest partitions
SELECT partition, record_count,
       total_size / 1024 / 1024 / 1024 AS total_gb
FROM iceberg_catalog.mydb."events$partitions"
ORDER BY total_size DESC
LIMIT 20;

-- Count files per snapshot to detect snapshot bloat
SELECT snapshot_id, added_files_count, deleted_files_count, existing_files_count
FROM iceberg_catalog.mydb."events$snapshots"
ORDER BY committed_at DESC;
```

---

## 18. Materialized Views

Materialized views pre-compute and store query results as Iceberg tables. Trino can transparently rewrite eligible queries to use the materialized view.

```sql
-- Create a materialized view
CREATE MATERIALIZED VIEW iceberg_catalog.mydb.daily_event_counts
WITH (
    partitioning = ARRAY['dt']
) AS
SELECT date_trunc('day', event_ts) AS dt,
       event_type,
       region,
       count(*) AS event_count
FROM iceberg_catalog.mydb.events
GROUP BY 1, 2, 3;

-- Refresh the materialized view
REFRESH MATERIALIZED VIEW iceberg_catalog.mydb.daily_event_counts;

-- Drop
DROP MATERIALIZED VIEW iceberg_catalog.mydb.daily_event_counts;
```

Transparent rewriting activates when a query's projection and filter can be answered entirely from the materialized view's data.

---

## 19. Anti-Patterns and Best Practices

### Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Too many small partitions | Millions of files → slow manifest scans, planner overhead | Use coarser transforms (`day` instead of `hour`) or `bucket()` |
| No ANALYZE after large loads | Optimizer uses default estimates → bad join order / distribution | Run `ANALYZE` after loads > 10% of table size |
| SELECT * on wide tables | Reads all columns → defeats columnar projection pushdown | Enumerate required columns explicitly |
| MERGE/UPDATE on poorly partitioned data | Full-table file rewrites | Align update predicates with partition columns |
| Skipping OPTIMIZE indefinitely | Thousands of small files accumulate → increasing query latency | Schedule weekly compaction |
| Running EXPIRE_SNAPSHOTS without REMOVE_ORPHAN_FILES | Orphan files accumulate on storage | Always pair the two procedures |
| Setting retention_threshold < longest query duration | Active queries read files that orphan cleanup deletes | Set threshold ≥ `query_max_run_time` |
| Bucket count too high or too low | Too high → too many files; too low → skew | Start with `bucket(N)` where N yields ~256 MB–1 GB per file |
| Using AVRO for analytics tables | Row-oriented → full row reads, no column pruning | Use PARQUET for all analytical tables |
| Forgetting to run ANALYZE after OPTIMIZE | Stale statistics; optimizer may pick worse plans after compaction | ANALYZE immediately after OPTIMIZE |
| Mixing COW and MOR assumptions | Unexpected query patterns | Understand that Trino uses COW; MOR awareness needed for format_version=2 tables with delete files |

### Best Practices Checklist

- [ ] Use `PARQUET` + `ZSTD` for all new tables.
- [ ] Set `format_version = 2` for full ACID support.
- [ ] Choose partition granularity so each partition contains 1–10 data files averaging 256 MB–1 GB.
- [ ] Add `sorted_by` for columns frequently used in point-query filters and join predicates.
- [ ] Run `ANALYZE` after initial loads and after bulk DML.
- [ ] Schedule weekly `OPTIMIZE` + `EXPIRE_SNAPSHOTS` + `REMOVE_ORPHAN_FILES`.
- [ ] Set `join_reordering_strategy = 'AUTOMATIC'` and ensure statistics exist.
- [ ] Use partition-aligned predicates in `UPDATE`, `DELETE`, and `OPTIMIZE` to limit file rewrites.
- [ ] Monitor `$partitions` and `$files` metadata tables for small-file accumulation.
- [ ] Set `iceberg.file-system-cache.enabled = true` with a local SSD directory for frequently scanned tables.
- [ ] Enable `fault-tolerant-execution-adaptive-query-planning-enabled` for long-running ETL jobs.

---

## 20. Quick Reference

### Session Properties for Performance Tuning

```sql
SET SESSION join_reordering_strategy = 'AUTOMATIC';
SET SESSION join_distribution_type = 'AUTOMATIC';
SET SESSION join_max_broadcast_table_size = '200MB';
SET SESSION query_max_memory = '10GB';
```

### Connector Properties Summary

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

### Partition Transform Quick Reference

```sql
-- Temporal
partitioning = ARRAY['year(created_at)']
partitioning = ARRAY['month(created_at)']
partitioning = ARRAY['day(created_at)']
partitioning = ARRAY['hour(created_at)']

-- Hash bucketing (high-cardinality columns)
partitioning = ARRAY['bucket(64, user_id)']

-- String prefix
partitioning = ARRAY['truncate(4, country_code)']

-- Combined
partitioning = ARRAY['region', 'day(event_ts)', 'bucket(32, user_id)']
```

### Maintenance Command Reference

```sql
-- Compaction
ALTER TABLE t EXECUTE optimize(file_size_threshold => '100MB');
ALTER TABLE t EXECUTE optimize WHERE dt >= CURRENT_DATE - INTERVAL '7' DAY;

-- Snapshot expiration
ALTER TABLE t EXECUTE expire_snapshots(
    retention_threshold => '14d',
    retain_last => 10,
    clean_expired_metadata_files => true
);

-- Orphan cleanup
ALTER TABLE t EXECUTE remove_orphan_files(retention_threshold => '14d');

-- Manifest rebalancing
ALTER TABLE t EXECUTE optimize_manifests;

-- Statistics refresh
ANALYZE iceberg_catalog.mydb.t;
```

### Time Travel Quick Reference

```sql
-- By snapshot ID
SELECT * FROM t FOR VERSION AS OF <snapshot_id>;

-- By timestamp
SELECT * FROM t FOR TIMESTAMP AS OF TIMESTAMP '2024-01-14 12:00:00 UTC';

-- Rollback
CALL catalog.system.rollback_to_snapshot('schema', 'table', <snapshot_id>);
CALL catalog.system.rollback_to_timestamp('schema', 'table',
    TIMESTAMP '2024-01-14 12:00:00.000 UTC');
```

### EXPLAIN Usage

```sql
-- Logical plan
EXPLAIN SELECT ...;

-- Distributed plan (shows stages, exchanges, join types)
EXPLAIN (TYPE DISTRIBUTED) SELECT ...;

-- IO plan (shows scan statistics used by optimizer)
EXPLAIN (TYPE IO, FORMAT JSON) SELECT ...;

-- Actual execution plan with runtime stats
EXPLAIN ANALYZE SELECT ...;
```
