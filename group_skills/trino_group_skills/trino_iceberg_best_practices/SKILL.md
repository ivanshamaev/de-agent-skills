---
name: trino-iceberg-best-practices
description: Production Apache Iceberg best practices with Trino — hidden partitioning (day/month/year/hour/bucket/truncate transforms), partition evolution, sorted tables, file compaction (optimize/expire_snapshots/remove_orphan_files), snapshot management, metadata tables ($snapshots/$files/$partitions/$history), schema evolution, time travel queries, MERGE/UPDATE/DELETE DML, ANALYZE for CBO, Iceberg catalog types (HMS/Glue/REST/Nessie), small file prevention, Bloom filters
---

# Trino Iceberg Best Practices

## When to Use

- Designing or reviewing Iceberg table DDL for production workloads
- Troubleshooting slow Iceberg queries (missing partition pruning, small files, stale snapshots)
- Planning Iceberg table maintenance jobs (compaction, snapshot expiry)
- Migrating Hive tables to Iceberg format
- Implementing SCD2, MERGE, or CDC patterns on Iceberg

---

## Table DDL: Production Template

```sql
CREATE TABLE iceberg.silver.orders (
    order_id     BIGINT        NOT NULL,
    customer_id  BIGINT        NOT NULL,
    order_date   DATE          NOT NULL,
    status       VARCHAR       NOT NULL,
    amount       DECIMAL(18,2),
    region       VARCHAR,
    updated_at   TIMESTAMP(6)  NOT NULL
)
WITH (
    format            = 'PARQUET',         -- preferred: columnar + compression
    compression_codec = 'ZSTD',            -- best ratio/speed trade-off
    format_version    = 2,                 -- required for row-level deletes (UPDATE/DELETE/MERGE)
    partitioning      = ARRAY['day(order_date)', 'region'],
    sorted_by         = ARRAY['customer_id', 'order_date'],  -- within each partition file
    location          = 's3://data-lake/silver/orders/'
);
```

---

## Partition Transforms (Hidden Partitioning)

Iceberg hidden partitioning: partition values are derived from column values using transforms. Clients never need to specify partition values — Trino prunes automatically.

| Transform | DDL Syntax | Column Type | Pruning Trigger |
|-----------|-----------|-------------|----------------|
| Identity | `'region'` | Any | `WHERE region = 'US'` |
| Year | `'year(order_date)'` | DATE / TIMESTAMP | `WHERE YEAR(order_date) = 2024` ✗, use range ✓ |
| Month | `'month(order_date)'` | DATE / TIMESTAMP | Range predicate on date |
| Day | `'day(order_date)'` | DATE / TIMESTAMP | `WHERE order_date = DATE '2024-01-15'` |
| Hour | `'hour(event_time)'` | TIMESTAMP | `WHERE event_time >= ... AND event_time < ...` |
| Bucket | `'bucket(customer_id, 32)'` | INT/BIGINT/VARCHAR | Exact equality on customer_id |
| Truncate | `'truncate(sku, 4)'` | VARCHAR | Prefix match on sku |

**Critical rule**: Always use **range predicates** (not function calls) on date/timestamp partition columns:

```sql
-- GOOD: partition pruning works
WHERE order_date >= DATE '2024-01-01' AND order_date < DATE '2024-02-01'

-- BAD: disables partition pruning
WHERE DATE_TRUNC('month', order_date) = DATE '2024-01-01'
WHERE YEAR(order_date) = 2024
```

---

## Partition Evolution

Iceberg supports adding/changing/removing partition specs without rewriting existing data.

```sql
-- Add a new partition dimension to existing table
ALTER TABLE iceberg.silver.orders
SET PROPERTIES partitioning = ARRAY['day(order_date)', 'region', 'bucket(customer_id, 16)'];

-- Remove partition (data still physically partitioned, just not pruned by this column)
ALTER TABLE iceberg.silver.orders
SET PROPERTIES partitioning = ARRAY['day(order_date)'];
```

Old and new data coexist with different partition schemes — Trino handles this transparently.

---

## Sorted Tables

`sorted_by` applies within each Parquet/ORC file, enabling min/max skipping and better compression.

```sql
-- Orders sorted by customer_id within each day partition
-- → filters on customer_id skip row groups efficiently
CREATE TABLE iceberg.silver.orders (...)
WITH (
    partitioning = ARRAY['day(order_date)'],
    sorted_by    = ARRAY['customer_id ASC', 'updated_at DESC']
);

-- Disable sorted writing for bulk insert performance (then re-enable)
SET SESSION sorted_writing_enabled = false;
INSERT INTO iceberg.silver.orders SELECT * FROM staging;
SET SESSION sorted_writing_enabled = true;
```

---

## DML: INSERT / UPDATE / DELETE / MERGE

```sql
-- INSERT (append new rows)
INSERT INTO iceberg.silver.orders
SELECT order_id, customer_id, order_date, status, amount, region, updated_at
FROM iceberg.bronze.orders_raw
WHERE ingested_date = CURRENT_DATE;

-- UPDATE (requires format_version = 2)
UPDATE iceberg.silver.orders
SET status = 'cancelled'
WHERE order_id = 12345;

-- DELETE
DELETE FROM iceberg.silver.orders
WHERE order_date < DATE '2020-01-01';    -- partition-level delete (fast with identity partitioning)

-- MERGE: upsert pattern (SCD1 / CDC)
MERGE INTO iceberg.silver.orders t
USING (
    SELECT order_id, customer_id, order_date, status, amount, region, updated_at
    FROM iceberg.bronze.orders_raw
    WHERE ingested_date = CURRENT_DATE
) s ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
    UPDATE SET status = s.status, amount = s.amount, updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, order_date, status, amount, region, updated_at)
    VALUES (s.order_id, s.customer_id, s.order_date, s.status, s.amount, s.region, s.updated_at);
```

---

## Table Maintenance

Run these as scheduled Airflow tasks. Always run in sequence: optimize → expire_snapshots → remove_orphan_files.

### OPTIMIZE (File Compaction)

Merges small files into target size (default 100MB, recommended 128MB–512MB):

```sql
-- Full table compaction
ALTER TABLE iceberg.silver.orders EXECUTE optimize(file_size_threshold => '128MB');

-- Partition-specific compaction (more targeted, less I/O)
ALTER TABLE iceberg.silver.orders EXECUTE optimize(file_size_threshold => '128MB')
WHERE order_date >= DATE '2024-01-01' AND order_date < DATE '2024-02-01';

-- Verify compaction result
SELECT partition, file_count, total_size, record_count
FROM iceberg.silver."orders$partitions"
ORDER BY file_count DESC
LIMIT 20;
```

**When to run**: daily for append-heavy tables; after bulk MERGE/UPDATE.

### OPTIMIZE_MANIFESTS

Clusters manifest files to improve metadata read performance:

```sql
ALTER TABLE iceberg.silver.orders EXECUTE optimize_manifests;
```

**When to run**: after many small commits (streaming ingestion), or when manifest count exceeds 1000.

### EXPIRE_SNAPSHOTS

Removes old snapshots and their associated data files:

```sql
-- Keep snapshots for 7 days, retain at least 1
ALTER TABLE iceberg.silver.orders EXECUTE expire_snapshots(
    retention_threshold => '7d',
    retain_last => 2
);

-- Shorter retention for staging tables
ALTER TABLE iceberg.bronze.orders_raw EXECUTE expire_snapshots(
    retention_threshold => '2d',
    retain_last => 1
);
```

**Warning**: do not expire snapshots while time travel queries are in progress.

### REMOVE_ORPHAN_FILES

Deletes files not referenced by any snapshot:

```sql
ALTER TABLE iceberg.silver.orders EXECUTE remove_orphan_files(
    retention_threshold => '7d'
);
```

**Always run after** `expire_snapshots`, and ensure retention_threshold matches.

---

## Snapshot Management and Time Travel

```sql
-- View snapshot history
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg.silver."orders$snapshots"
ORDER BY committed_at DESC
LIMIT 10;

-- View table history (including replacements)
SELECT * FROM iceberg.silver."orders$history"
ORDER BY made_current_at DESC;

-- Time travel: query past snapshot by ID
SELECT COUNT(*) FROM iceberg.silver.orders
FOR VERSION AS OF 8954597067493422955;

-- Time travel: query past snapshot by timestamp
SELECT * FROM iceberg.silver.orders
FOR TIMESTAMP AS OF TIMESTAMP '2024-06-01 00:00:00 UTC';

-- Rollback to snapshot (use after bad write)
ALTER TABLE iceberg.silver.orders EXECUTE rollback_to_snapshot(8954597067493422955);
```

---

## Metadata Tables

```sql
-- File-level statistics (detect small files)
SELECT file_path, file_size_in_bytes,
       record_count,
       ROUND(file_size_in_bytes / 1024.0 / 1024, 1) AS size_mb
FROM iceberg.silver."orders$files"
WHERE file_size_in_bytes < 10 * 1024 * 1024   -- files < 10MB
ORDER BY file_size_in_bytes
LIMIT 50;

-- Partition stats (find hot/cold partitions)
SELECT partition, record_count, file_count,
       ROUND(total_size / 1024.0 / 1024 / 1024, 2) AS total_gb,
       ROUND(CAST(record_count AS DOUBLE) / file_count) AS avg_rows_per_file
FROM iceberg.silver."orders$partitions"
ORDER BY file_count DESC
LIMIT 20;

-- Manifest list (check commit frequency)
SELECT manifest_path, partition_spec_id, added_files_count, existing_files_count
FROM iceberg.silver."orders$manifests"
LIMIT 20;

-- Active refs (branches, tags)
SELECT name, type, snapshot_id
FROM iceberg.silver."orders$refs";
```

---

## Schema Evolution

Iceberg schema evolution is safe — existing files do not need to be rewritten.

```sql
-- Add column (new files include it; old files return NULL)
ALTER TABLE iceberg.silver.orders ADD COLUMN promo_code VARCHAR;

-- Rename column
ALTER TABLE iceberg.silver.orders RENAME COLUMN promo_code TO coupon_code;

-- Drop column (logically hidden; data remains in old files)
ALTER TABLE iceberg.silver.orders DROP COLUMN coupon_code;

-- Widen numeric type (safe: INT → BIGINT, FLOAT → DOUBLE)
-- Not supported via ALTER directly; use CTAS migration for type changes
```

---

## Bloom Filter Indexes

Enable Bloom filters on high-cardinality equality filter columns:

```sql
CREATE TABLE iceberg.silver.orders (
    order_id    BIGINT,
    customer_id BIGINT,
    ...
)
WITH (
    format = 'PARQUET',
    parquet_bloom_filter_columns = ARRAY['order_id', 'customer_id']  -- equality filters
);

-- For ORC tables
CREATE TABLE iceberg.silver.events (...)
WITH (
    format = 'ORC',
    orc_bloom_filter_columns = ARRAY['event_id', 'session_id'],
    orc_bloom_filter_fpp = 0.01    -- false positive probability (0.01 = 1%)
);
```

---

## Statistics for CBO

```sql
-- Full statistics
ANALYZE iceberg.silver.orders;

-- Targeted statistics (cheaper, effective for join/filter optimization)
ANALYZE iceberg.silver.orders
WITH (columns = ARRAY['customer_id', 'order_date', 'status', 'region']);

-- Schedule ANALYZE after bulk loads via Airflow
-- Run ANALYZE on partition-filtered data if table is very large
```

---

## Hive → Iceberg Migration

```sql
-- In-place migration using Trino procedure (zero-copy)
CALL iceberg.system.migrate(
    schema_name => 'legacy',
    table_name  => 'orders_hive',
    recursive_directory => 'true'
);

-- Verify migrated table
SELECT COUNT(*) FROM iceberg.legacy.orders_hive;
SELECT * FROM iceberg.legacy."orders_hive$snapshots";
```

---

## Anti-Patterns

1. **`format_version = 1`** — version 1 does not support row-level deletes (UPDATE/DELETE/MERGE); always use `format_version = 2` for mutable tables.
2. **Identity partition on timestamp column** — creates one partition per timestamp value (millions of partitions); use `day()` or `hour()` transforms instead.
3. **Running OPTIMIZE without EXPIRE_SNAPSHOTS afterward** — OPTIMIZE creates new snapshots; without expiry, metadata grows unbounded.
4. **Applying function to partition column in WHERE** — `YEAR(order_date) = 2024` disables Iceberg partition pruning; rewrite as `order_date >= '2024-01-01' AND order_date < '2025-01-01'`.
5. **Streaming tiny commits without batch compaction** — Kafka→Iceberg pipelines writing every 30s create thousands of 1KB files; compact hourly to 128MB target files.
6. **MERGE without `format_version = 2`** — Iceberg v1 tables don't support equality delete files required for MERGE; upgrading format_version requires a CTAS migration.
7. **Missing ANALYZE after data loads** — stale stats cause CBO to choose suboptimal join strategies; schedule `ANALYZE` after large bulk inserts.

---

## References

- Iceberg connector: `trino.io/docs/current/connector/iceberg.html`
- Apache Iceberg spec: `iceberg.apache.org/docs/latest/`
- Related skills: `[[trino-query-optimization]]`, `[[trino-explain-plan-review]]`, `[[trino-airflow-lakehouse-pipelines]]`, `[[trino-file-layout-optimization]]`
