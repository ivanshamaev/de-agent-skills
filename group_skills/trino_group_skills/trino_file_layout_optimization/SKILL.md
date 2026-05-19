---
name: trino-file-layout-optimization
description: Trino data file layout optimization for Iceberg — Parquet vs ORC file format selection, target file size tuning (iceberg.target-max-file-size), row group size, Parquet/ORC column encoding choices, Bloom filter indexes, sorted_by for min/max skipping, small file detection via $files metadata table, OPTIMIZE compaction strategies, partition design impact on file count, Z-order equivalent via sorted_by, split sizing and parallelism (iceberg.minimum-assigned-split-weight), write parallelism tuning
---

# Trino File Layout Optimization

## When to Use

- Queries are slow due to excessive split count (too many small files)
- Table scans read too much data (poor column/row pruning)
- Writing performance is slow due to too many partitions per writer
- Tuning Parquet/ORC encoding for specific column distributions
- Deciding between Parquet and ORC for a workload

---

## Format Selection: Parquet vs ORC

| Dimension | Parquet | ORC |
|-----------|---------|-----|
| Read performance | Excellent | Excellent |
| Write speed | Fast | Fast |
| Nested types (MAP/ARRAY/STRUCT) | Better | Good |
| Bloom filter support | ✓ | ✓ |
| Row-level statistics | Row group level | Stripe level |
| Ecosystem compatibility | Wider (Spark, Flink, Arrow) | Narrower (Hive-optimized) |
| Default in Trino Iceberg | **Yes** | No |

**Rule**: use **Parquet** as default. Use ORC only if migrating from Hive workloads or if ORC-specific index features (stripe statistics) are required.

---

## Target File Size

Trino writes files up to `iceberg.target-max-file-size` before opening a new one. Too small = many files (slow metadata), too large = low write parallelism.

```properties
# etc/catalog/iceberg.properties
# Default: 1GB — too large for most workloads
iceberg.target-max-file-size=512MB
```

**Recommended target sizes by layer:**

| Layer | Table Type | Target File Size | Rationale |
|-------|-----------|-----------------|-----------|
| Bronze | Append-heavy | 128MB | Frequent small writes, compact later |
| Silver | Incremental | 256MB | Balance between write speed and read perf |
| Gold | Full refresh | 512MB | Full rebuild, maximize row group density |
| Archive | Read-heavy | 1GB | Minimize file count for cold scans |

---

## Parquet Row Group Tuning

Row group size controls how much data is buffered in memory before writing one row group. Larger row groups = better compression + better statistics = better skip rate.

```sql
-- Set target row group size via session property
SET SESSION parquet_writer_block_size = '256MB';   -- default: 128MB

-- Validate row group sizes in written files:
SELECT
    file_path,
    ROUND(file_size_in_bytes / 1024.0 / 1024, 1)    AS file_mb,
    record_count,
    ROUND(CAST(record_count AS DOUBLE) /
          (file_size_in_bytes / (128 * 1024 * 1024) + 1)) AS est_rows_per_rg
FROM iceberg.silver."orders$files"
ORDER BY file_size_in_bytes DESC
LIMIT 20;
```

---

## Sorted Files for Min/Max Skipping

`sorted_by` sorts rows within each Parquet/ORC file by the specified columns. Queries filtering on the sort key can skip entire row groups whose min/max range doesn't overlap the filter.

```sql
-- Table sorted by customer_id within each day partition
CREATE TABLE iceberg.silver.orders (
    order_id    BIGINT,
    customer_id BIGINT,
    order_date  DATE,
    amount      DECIMAL(18,2),
    status      VARCHAR
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['day(order_date)'],
    sorted_by    = ARRAY['customer_id']    -- rows sorted within each file
);

-- This query now skips 90%+ of row groups if customer distribution is uniform:
SELECT * FROM iceberg.silver.orders
WHERE order_date = DATE '2024-06-01'
  AND customer_id BETWEEN 1000000 AND 1000100;
```

**Sorting cost**: sorted writes are 20-40% slower due to in-memory sorting. Disable for bulk loads, re-enable after:

```sql
SET SESSION sorted_writing_enabled = false;
-- ... bulk INSERT ...
SET SESSION sorted_writing_enabled = true;
-- ... then OPTIMIZE to compact and re-sort
ALTER TABLE iceberg.silver.orders EXECUTE optimize(file_size_threshold => '256MB');
```

---

## Bloom Filter Indexes

Bloom filters provide O(1) existence checks for equality predicates. Effective for high-cardinality columns where min/max skipping is ineffective (e.g., UUIDs, order IDs).

```sql
-- Parquet Bloom filter
CREATE TABLE iceberg.silver.events (
    event_id    UUID,
    session_id  VARCHAR,
    user_id     BIGINT,
    event_time  TIMESTAMP(6),
    event_type  VARCHAR
)
WITH (
    format = 'PARQUET',
    parquet_bloom_filter_columns = ARRAY['event_id', 'session_id'],
    -- false positive probability: 0.01 = 1% (lower = larger bloom filter)
    compression_codec = 'ZSTD'
);

-- ORC Bloom filter
CREATE TABLE iceberg.silver.events_orc (...)
WITH (
    format                  = 'ORC',
    orc_bloom_filter_columns = ARRAY['event_id', 'session_id'],
    orc_bloom_filter_fpp     = 0.01
);
```

**When Bloom filters help**: `WHERE event_id = 'abc-123'` with no good min/max range → Bloom filter skips 99% of row groups.
**When they don't help**: range predicates (`BETWEEN`), `LIKE`, `IN` with many values.

---

## Small File Detection and Compaction

```sql
-- Find tables with excessive small files
SELECT
    partition,
    file_count,
    record_count,
    ROUND(total_size / 1024.0 / 1024 / 1024, 3)          AS total_gb,
    ROUND(total_size / 1024.0 / 1024 / NULLIF(file_count, 0), 1) AS avg_file_mb
FROM iceberg.silver."orders$partitions"
WHERE file_count > 100                  -- partitions with > 100 files
   OR (total_size / NULLIF(file_count, 0)) < 10 * 1024 * 1024  -- avg < 10MB
ORDER BY file_count DESC
LIMIT 20;

-- Identify the worst offenders across all tables (run for each table)
SELECT
    file_path,
    ROUND(file_size_in_bytes / 1024.0 / 1024, 1) AS size_mb,
    record_count
FROM iceberg.silver."orders$files"
WHERE file_size_in_bytes < 5 * 1024 * 1024      -- files < 5MB
ORDER BY file_size_in_bytes
LIMIT 50;
```

### Targeted Compaction

```sql
-- Compact a specific date partition (avoid full table compaction on large tables)
ALTER TABLE iceberg.silver.orders
EXECUTE optimize(file_size_threshold => '256MB')
WHERE order_date >= DATE '2024-01-01' AND order_date < DATE '2024-02-01';

-- Compact by region (identity partition)
ALTER TABLE iceberg.silver.orders
EXECUTE optimize(file_size_threshold => '256MB')
WHERE region = 'EU';

-- Full table compaction (use only for small/medium tables)
ALTER TABLE iceberg.silver.orders
EXECUTE optimize(file_size_threshold => '256MB');
```

---

## Split Weight and Parallelism

Trino assigns splits (Parquet/ORC files or row groups) to tasks. The `minimum-assigned-split-weight` controls the minimum fraction of work per split.

```properties
# etc/catalog/iceberg.properties
# Default: 0.05 — very small files still get their own split
# Increase to merge tiny splits together:
iceberg.minimum-assigned-split-weight=0.1
```

**Write parallelism**: controlled by `iceberg.max-partitions-per-writer`. Too few = bottleneck. Too many = too many tiny files.

```properties
# Default: 100 partitions per writer thread
iceberg.max-partitions-per-writer=100
```

For tables with many partitions (e.g., 365 days × 50 regions = 18,250 partitions), increase:

```properties
iceberg.max-partitions-per-writer=300
```

---

## Partition Design Impact on Files

**Too fine-grained partitioning** creates many small files:

```sql
-- BAD: partition by HOUR on low-volume table
-- 24 hours × 365 days = 8,760 partitions; each partition may have only 1MB
CREATE TABLE iceberg.silver.low_volume_events (...)
WITH (partitioning = ARRAY['hour(event_time)']);  -- too fine

-- GOOD: partition by DAY — larger files, same partition pruning for daily queries
CREATE TABLE iceberg.silver.low_volume_events (...)
WITH (partitioning = ARRAY['day(event_time)']);
```

**Too coarse-grained partitioning** causes large files that miss row-level pruning:

```sql
-- BAD: no partitioning on a 1TB table — full scan for every date filter
CREATE TABLE iceberg.silver.huge_events (...);

-- GOOD: partition by day for 1TB/day table
CREATE TABLE iceberg.silver.huge_events (...)
WITH (partitioning = ARRAY['day(event_time)']);
```

**Target**: 128MB–1GB files per partition. Adjust partition granularity to achieve this.

---

## File Layout Health Check SQL

```sql
-- Comprehensive health check for a table
WITH stats AS (
    SELECT
        COUNT(*)                                                  AS total_files,
        SUM(record_count)                                         AS total_rows,
        SUM(file_size_in_bytes) / 1024.0 / 1024 / 1024           AS total_gb,
        AVG(file_size_in_bytes) / 1024.0 / 1024                  AS avg_file_mb,
        MIN(file_size_in_bytes) / 1024.0 / 1024                  AS min_file_mb,
        MAX(file_size_in_bytes) / 1024.0 / 1024                  AS max_file_mb,
        COUNT(*) FILTER (WHERE file_size_in_bytes < 10*1024*1024) AS small_files_lt_10mb,
        COUNT(*) FILTER (WHERE file_size_in_bytes > 512*1024*1024) AS large_files_gt_512mb
    FROM iceberg.silver."orders$files"
)
SELECT
    total_files,
    total_rows,
    ROUND(total_gb, 2)          AS total_gb,
    ROUND(avg_file_mb, 1)       AS avg_file_mb,
    ROUND(min_file_mb, 1)       AS min_file_mb,
    ROUND(max_file_mb, 1)       AS max_file_mb,
    small_files_lt_10mb,
    large_files_gt_512mb,
    CASE
        WHEN avg_file_mb < 10   THEN '🔴 CRITICAL: too many small files'
        WHEN avg_file_mb < 64   THEN '🟡 WARNING: files undersized'
        WHEN avg_file_mb > 1024 THEN '🟡 WARNING: files oversized'
        ELSE '🟢 OK'
    END AS health_status
FROM stats;
```

---

## Anti-Patterns

1. **Not setting `sorted_by` on the primary filter column** — without sorted files, Parquet row-group min/max skipping only eliminates a random ~20% of data; with sort, filter selectivity can reach 95%+.
2. **Bloom filters on low-cardinality columns** — Bloom filters add overhead per row group; on `status VARCHAR` with 5 values, they waste space and don't help — use range predicates instead.
3. **`iceberg.target-max-file-size=1GB` on frequently updated tables** — large files require reading and rewriting 1GB per UPDATE/DELETE row; use 128–256MB for mutable tables.
4. **Partition by UUID or hash column** — creates as many partitions as rows; use `bucket(id, N)` transform instead for hash-based partitioning.
5. **Running OPTIMIZE without EXPIRE_SNAPSHOTS** — OPTIMIZE creates new snapshot files; the old small files remain until expire_snapshots removes them; always run the full maintenance sequence.

---

## References

- Iceberg connector properties: `trino.io/docs/current/connector/iceberg.html`
- Parquet format: `parquet.apache.org/docs/`
- Related skills: `[[trino-iceberg-best-practices]]`, `[[trino-query-optimization]]`, `[[trino-airflow-lakehouse-pipelines]]`
