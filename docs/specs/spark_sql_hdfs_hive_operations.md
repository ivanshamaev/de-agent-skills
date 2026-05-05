# Spark SQL HDFS and Hive Operations

This reference is for agents working with Spark SQL on top of Hive Metastore and HDFS-backed data. Use it for production DDL, partition management, HDFS inspection, safe writes, backfills, table repair, metadata drift, file layout, data profiling, and operational debugging.

The agent must treat Hive metadata and HDFS files as two related but separate systems:

- Hive Metastore stores table definitions, partition metadata, locations, schema, properties, and ownership metadata.
- HDFS stores physical directories and files.
- Spark plans queries from catalog metadata, file listings, statistics, and runtime configuration.
- A query can be logically correct and still read the wrong files if metadata is stale or paths are inconsistent.
- A write can succeed as a Spark job and still be unsafe if overwrite scope, partition metadata, or file layout is wrong.

## Operating Principles

When working with Spark SQL over HDFS/Hive:

- Prefer catalog tables over direct HDFS paths for production reads and writes.
- Inspect both Hive metadata and HDFS state before changing production tables.
- Make partition ranges explicit for reads, writes, repairs, and backfills.
- Never perform a broad `INSERT OVERWRITE` unless the task is an intentional full rebuild.
- Do not rely on `SELECT *` for production writes.
- Keep partition columns directly filterable for pruning.
- Use columnar formats such as Parquet or ORC for large analytical tables.
- Avoid many tiny files and high-cardinality partitions.
- Use staged writes and validation for risky replacements.
- Refresh or repair metadata when files are created, removed, or moved outside Spark.
- State assumptions about managed/external tables, ownership, location, and overwrite behavior.

## Context to Collect

Before DDL, repair, overwrite, or debugging work, collect:

- Spark version and Hive support mode.
- Catalog/metastore name and database.
- Table format: Hive Parquet/ORC, text, Avro, Delta, Iceberg, Hudi, or custom SerDe.
- Managed vs external table.
- Table location and partition locations.
- Partition columns and their data types.
- Expected partition directory layout.
- Current table schema and target schema.
- Data volume, file count, and largest partitions.
- Compression codec and file format.
- Write mode: append, static overwrite, dynamic partition overwrite, CTAS, staged swap, or table-format transaction.
- Effective user, HDFS ACLs, and warehouse permissions.
- Whether data was written by Spark, Hive, DistCp, Flume, streaming, Sqoop, or another system.
- Whether downstream consumers read through Hive, Spark, Presto/Trino, Impala, or direct paths.

## Initial Inspection Checklist

Run catalog inspection first:

```sql
USE raw;

SHOW TABLES LIKE 'events';
SHOW CREATE TABLE raw.events;
DESCRIBE FORMATTED raw.events;
DESCRIBE EXTENDED raw.events;
SHOW TBLPROPERTIES raw.events;
SHOW COLUMNS IN raw.events;
SHOW PARTITIONS raw.events;
```

For a specific partition:

```sql
DESCRIBE EXTENDED raw.events PARTITION (event_date = '2026-05-05', country = 'US');
```

Then inspect HDFS:

```bash
hdfs dfs -ls /warehouse/raw/events
hdfs dfs -ls /warehouse/raw/events/event_date=2026-05-05
hdfs dfs -du -s -h /warehouse/raw/events/event_date=2026-05-05
hdfs dfs -count -h /warehouse/raw/events/event_date=2026-05-05
hdfs dfs -test -d /warehouse/raw/events/event_date=2026-05-05
```

Use both views. Metastore can list partitions that no longer exist on HDFS, and HDFS can contain partitions that are invisible to Hive.

## Managed vs External Tables

Managed and external tables have different lifecycle risk.

Managed table:

- Spark/Hive owns table metadata and usually owns table data under the warehouse path.
- Dropping the table can delete data, depending on catalog/version/settings.
- Use only when the platform should manage data lifecycle.

External table:

- Hive owns metadata only.
- Data lives at an explicit `LOCATION`.
- Dropping the table usually removes metadata, not files.
- Preferred for raw/landing zones and shared HDFS locations.

Inspect table type:

```sql
DESCRIBE FORMATTED raw.events;
SHOW TBLPROPERTIES raw.events;
```

Create an external Parquet table:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS raw.events (
    event_id STRING,
    user_id STRING,
    event_time TIMESTAMP,
    event_type STRING,
    amount DECIMAL(18, 2),
    ingestion_ts TIMESTAMP
)
PARTITIONED BY (event_date DATE, country STRING)
STORED AS PARQUET
LOCATION 'hdfs:///warehouse/raw/events'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'owner' = 'data-platform'
);
```

Create a managed analytical table:

```sql
CREATE TABLE IF NOT EXISTS mart.daily_revenue (
    country STRING,
    revenue DECIMAL(18, 2),
    processed_at TIMESTAMP
)
PARTITIONED BY (event_date DATE)
STORED AS ORC;
```

Agent rule: before any `DROP TABLE`, `TRUNCATE TABLE`, or full overwrite, explicitly identify whether the table is managed or external and where data lives.

## Table Locations

Always verify table and partition locations:

```sql
DESCRIBE FORMATTED raw.events;
DESCRIBE EXTENDED raw.events PARTITION (event_date = '2026-05-05', country = 'US');
```

Common cases:

- Table location is `hdfs:///warehouse/raw/events`.
- Partition location is inherited as `.../event_date=2026-05-05/country=US`.
- Partition location is custom and points elsewhere.
- Metastore location uses `hdfs://nameservice1/...` while jobs read `hdfs:///...`.
- Data was moved on HDFS but partition metadata still points to old paths.

When location and physical files disagree:

```bash
hdfs dfs -ls hdfs:///warehouse/raw/events/event_date=2026-05-05/country=US
hdfs dfs -du -s -h hdfs:///warehouse/raw/events/event_date=2026-05-05/country=US
```

Do not assume table root plus Hive-style partition path when partitions have explicit custom locations.

## HDFS Path Conventions

Recommended Hive-style partition layout:

```text
hdfs:///warehouse/raw/events/
  event_date=2026-05-05/
    country=US/
      part-00000-....snappy.parquet
```

Rules:

- Use stable table roots.
- Keep partition directory names exactly aligned with partition column names.
- Avoid spaces and unusual characters in HDFS paths.
- Avoid manual data placement under temporary directories inside table roots.
- Do not mix unrelated schemas under the same table root.
- Do not write non-data marker files that confuse downstream readers, except standard commit markers accepted by the platform.
- Keep raw, staging, mart, and temporary paths separate.

## File Formats

Preferred production formats:

- Parquet for general analytical workloads.
- ORC for Hive-heavy environments or workloads optimized for ORC.
- Delta/Iceberg/Hudi when table transactions, upserts, schema evolution, or snapshot management are required.

Avoid for large curated tables:

- CSV.
- JSON.
- Uncompressed text.
- Mixed-format directories.

Parquet table:

```sql
CREATE EXTERNAL TABLE raw.events_parquet (
    event_id STRING,
    user_id STRING,
    event_time TIMESTAMP,
    event_type STRING,
    amount DECIMAL(18, 2)
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///warehouse/raw/events_parquet';
```

ORC table:

```sql
CREATE TABLE mart.user_daily_metrics (
    user_id STRING,
    event_count BIGINT,
    revenue DECIMAL(18, 2)
)
PARTITIONED BY (event_date DATE)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'ZLIB');
```

Text/CSV external table for landing only:

```sql
CREATE EXTERNAL TABLE landing.events_csv (
    event_id STRING,
    user_id STRING,
    raw_event_time STRING,
    raw_amount STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///warehouse/landing/events_csv';
```

For CSV/JSON landing, parse and write typed curated Parquet/ORC as soon as possible.

## Partition Design

Partition columns should reduce scans without creating too many directories.

Good partition candidates:

- Event date.
- Ingestion date.
- Hour for very large time-series data when queries commonly filter by hour.
- Country, region, tenant, or business domain only when cardinality is manageable and filters are common.

Bad partition candidates:

- `user_id`.
- `request_id`.
- `uuid`.
- High-cardinality transaction ids.
- Fields with unbounded growth.

Rules:

- Partition by columns commonly used in `WHERE`.
- Keep partition cardinality manageable.
- Avoid deep partition trees unless query patterns need them.
- Avoid partitions with only a few tiny files.
- Keep partition column types consistent across DDL, SQL filters, and directory values.
- Do not derive the partition filter through a function on the partition column.

Partition pruning pattern:

```sql
SELECT
    event_date,
    country,
    COUNT(*) AS events
FROM raw.events
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
  AND country IN ('US', 'CA')
GROUP BY event_date, country;
```

Bad pattern:

```sql
WHERE DATE_TRUNC('MONTH', event_date) = DATE '2026-05-01'
```

Use ranges:

```sql
WHERE event_date >= DATE '2026-05-01'
  AND event_date <  DATE '2026-06-01'
```

## Static and Dynamic Partitions

Static partition insert:

```sql
INSERT OVERWRITE TABLE mart.daily_revenue
PARTITION (event_date = DATE '2026-05-05')
SELECT
    country,
    SUM(amount) AS revenue,
    CURRENT_TIMESTAMP() AS processed_at
FROM raw.events
WHERE event_date = DATE '2026-05-05'
GROUP BY country;
```

Dynamic partition insert:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.daily_revenue
PARTITION (event_date)
SELECT
    country,
    SUM(amount) AS revenue,
    CURRENT_TIMESTAMP() AS processed_at,
    event_date
FROM raw.events
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date, country;
```

Rules:

- Static partition overwrite replaces the specified partition.
- Dynamic partition overwrite replaces partitions present in the write result.
- Dynamic overwrite is safe only when the result includes exactly the intended partitions.
- Always filter the source range for dynamic overwrite jobs.
- Always validate the touched partitions after writing.
- For Hive syntax, partition columns appear last in the `SELECT` for dynamic partition inserts.

## Adding Partitions

Targeted add for known paths:

```sql
ALTER TABLE raw.events ADD IF NOT EXISTS
    PARTITION (event_date = '2026-05-05', country = 'US')
    LOCATION 'hdfs:///warehouse/raw/events/event_date=2026-05-05/country=US';
```

Multiple targeted adds:

```sql
ALTER TABLE raw.events ADD IF NOT EXISTS
    PARTITION (event_date = '2026-05-05', country = 'US')
    LOCATION 'hdfs:///warehouse/raw/events/event_date=2026-05-05/country=US'
    PARTITION (event_date = '2026-05-05', country = 'CA')
    LOCATION 'hdfs:///warehouse/raw/events/event_date=2026-05-05/country=CA';
```

Use targeted partition adds when:

- You know exactly which partitions arrived.
- You want to avoid expensive recursive discovery.
- Partition count is large.
- Repairing the whole table would be too slow.

## Repairing Partition Metadata

Bulk repair:

```sql
MSCK REPAIR TABLE raw.events;
```

Spark-compatible form on some platforms:

```sql
REPAIR TABLE raw.events;
```

Then refresh Spark metadata:

```sql
REFRESH TABLE raw.events;
```

Use repair when:

- Files were written outside Hive/Spark table APIs.
- Many partitions exist on HDFS but are missing from the metastore.
- A bulk backfill produced many Hive-style partition directories.

Avoid blind repair when:

- The table has millions of partitions.
- HDFS directory layout contains junk or temporary directories.
- Partition locations are custom and not under the table root.
- The repair operation will exceed metastore or namenode capacity.

Before bulk repair, inspect:

```bash
hdfs dfs -ls /warehouse/raw/events | head
hdfs dfs -count -h /warehouse/raw/events
```

Agent rule: prefer targeted `ALTER TABLE ADD PARTITION` for known partition ranges in production. Use `MSCK REPAIR TABLE` as an operational recovery tool, not as a default daily pattern for huge tables.

## Dropping Partitions

Drop partition metadata:

```sql
ALTER TABLE raw.events DROP IF EXISTS PARTITION (event_date = '2026-05-05', country = 'US');
```

Important:

- On external tables, dropping a partition usually removes metadata only, not files.
- On managed tables, behavior can vary; verify platform semantics before assuming files remain.
- If files must be deleted, delete them deliberately after validation and approval.

HDFS deletion is destructive. Do not suggest it casually:

```bash
hdfs dfs -rm -r /warehouse/raw/events/event_date=2026-05-05/country=US
```

Before any deletion:

```bash
hdfs dfs -du -s -h /warehouse/raw/events/event_date=2026-05-05/country=US
hdfs dfs -ls /warehouse/raw/events/event_date=2026-05-05/country=US
```

Agent rule: when asked to remove data, distinguish metadata removal from physical file deletion and ask for confirmation if deletion is not explicitly requested.

## Renaming or Moving Partitions

Avoid moving production partition directories manually. Prefer rewriting the intended partition through the table API.

If a partition location must be changed:

1. Validate new HDFS path exists and contains expected files.
2. Add or alter partition metadata to point to the new location.
3. Refresh table metadata.
4. Validate row counts.
5. Remove old metadata/path only after confirmation.

Example:

```sql
ALTER TABLE raw.events PARTITION (event_date = '2026-05-05', country = 'US')
SET LOCATION 'hdfs:///warehouse/raw/events_recovered/event_date=2026-05-05/country=US';

REFRESH TABLE raw.events;
```

Check:

```sql
SELECT COUNT(*)
FROM raw.events
WHERE event_date = DATE '2026-05-05'
  AND country = 'US';
```

## Refreshing Spark Metadata

Spark can cache table metadata and file listings.

Use:

```sql
REFRESH TABLE raw.events;
```

For temporary views:

```sql
REFRESH TABLE temp_view_name;
```

When cache is involved:

```sql
UNCACHE TABLE raw.events;
REFRESH TABLE raw.events;
```

Use refresh when:

- Files changed outside the current Spark session.
- Partitions were added or dropped by another engine.
- You see `FileNotFoundException` after overwrite.
- A query reads stale file lists.

Refresh does not add missing Hive partitions by itself. Use `ALTER TABLE ADD PARTITION` or `MSCK REPAIR TABLE` for metastore partition registration.

## HDFS Inspection Commands

Common inspection:

```bash
hdfs dfs -ls /warehouse/raw/events
hdfs dfs -ls -R /warehouse/raw/events/event_date=2026-05-05 | head
hdfs dfs -du -h /warehouse/raw/events/event_date=2026-05-05
hdfs dfs -du -s -h /warehouse/raw/events/event_date=2026-05-05
hdfs dfs -count -h /warehouse/raw/events/event_date=2026-05-05
hdfs dfs -stat '%n %b %o %r %y' /warehouse/raw/events/event_date=2026-05-05
hdfs dfs -test -e /warehouse/raw/events/event_date=2026-05-05
hdfs dfs -test -d /warehouse/raw/events/event_date=2026-05-05
hdfs dfs -test -f /warehouse/raw/events/event_date=2026-05-05/_SUCCESS
```

Permissions:

```bash
hdfs dfs -ls -d /warehouse/raw/events
hdfs dfs -getfacl /warehouse/raw/events
hdfs dfs -getfacl /warehouse/raw/events/event_date=2026-05-05
```

Disk usage:

```bash
hdfs dfs -du -s -h /warehouse/raw/events/*
hdfs dfs -count -q -h /warehouse/raw/events
```

Small-file diagnosis:

```bash
hdfs dfs -count -h /warehouse/raw/events/event_date=2026-05-05
hdfs dfs -ls /warehouse/raw/events/event_date=2026-05-05 | head
```

Use HDFS commands when:

- Metastore says a partition exists but Spark returns zero rows.
- Spark fails with file not found.
- Partition repair does not discover expected data.
- Query is slow because of file count or layout.
- Permissions differ between table root and partition directories.

## Direct Path Reads

Prefer catalog reads:

```sql
SELECT user_id, event_date, amount
FROM raw.events
WHERE event_date = DATE '2026-05-05';
```

Use direct path reads only for diagnostics, recovery, or intentionally path-based data:

```sql
SELECT
    user_id,
    event_time,
    amount
FROM parquet.`hdfs:///warehouse/raw/events/event_date=2026-05-05/country=US`;
```

For ORC:

```sql
SELECT *
FROM orc.`hdfs:///warehouse/mart/user_daily_metrics/event_date=2026-05-05`;
```

Rules:

- Direct path reads bypass Hive partition metadata.
- Direct path reads still depend on physical schema and file format.
- Direct path reads can hide metastore problems instead of fixing them.
- Do not hard-code direct paths in production business logic unless the dataset is intentionally path-managed.

## Safe Writes

Before any production write:

- Inspect target DDL and location.
- Verify table type: managed vs external.
- Verify target partition columns and order.
- Verify source partition filters.
- Verify output schema and column order.
- Decide append vs overwrite intentionally.
- Decide static vs dynamic partition overwrite intentionally.
- Validate expected touched partitions.
- Plan rollback or recovery.

Recommended session settings:

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.sources.partitionOverwriteMode = dynamic;
SET spark.sql.storeAssignmentPolicy = ANSI;
```

Append:

```sql
INSERT INTO TABLE raw.events
PARTITION (event_date)
SELECT
    event_id,
    user_id,
    event_time,
    event_type,
    amount,
    ingestion_ts,
    event_date
FROM staging.events_valid
WHERE event_date = DATE '2026-05-05';
```

Partition overwrite:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.daily_revenue
PARTITION (event_date)
SELECT
    country,
    SUM(amount) AS revenue,
    CURRENT_TIMESTAMP() AS processed_at,
    event_date
FROM raw.events
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date, country;
```

Post-write validation:

```sql
SELECT
    event_date,
    COUNT(*) AS rows,
    COUNT(DISTINCT country) AS countries,
    SUM(revenue) AS revenue
FROM mart.daily_revenue
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date
ORDER BY event_date;
```

## Staged Replacement

Use staged replacement when a write is risky, full-table, or non-atomic.

Pattern:

1. Write to a staging table or staging path.
2. Validate schema, row counts, partitions, metrics, and file layout.
3. Swap metadata or rename tables if the platform supports it.
4. Keep the previous version until downstream validation completes.
5. Clean old data after retention/approval.

CTAS staging:

```sql
CREATE TABLE mart.daily_revenue_new
USING PARQUET
PARTITIONED BY (event_date)
AS
SELECT
    country,
    SUM(amount) AS revenue,
    CURRENT_TIMESTAMP() AS processed_at,
    event_date
FROM raw.events
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY event_date, country;
```

Controlled rename:

```sql
ALTER TABLE mart.daily_revenue RENAME TO mart.daily_revenue_old;
ALTER TABLE mart.daily_revenue_new RENAME TO mart.daily_revenue;
```

Risks:

- Rename semantics differ by catalog and table type.
- External table locations may not move.
- Object storage rename can be expensive or non-atomic, but this file focuses on HDFS where rename is usually metadata-level in the namenode.
- Downstream engines may cache metadata.

Always validate after the swap:

```sql
REFRESH TABLE mart.daily_revenue;

SELECT MIN(event_date), MAX(event_date), COUNT(*)
FROM mart.daily_revenue;
```

## Backfills

Backfills must be bounded and repeatable.

Rules:

- Use the same transformation logic as the daily pipeline when possible.
- Process explicit partition ranges.
- Write only touched partitions.
- Validate each partition or batch of partitions.
- Avoid repairing or refreshing the whole table when a targeted operation is enough.
- Track backfilled ranges in orchestration metadata.
- Do not mix historical backfill output with live daily output without a conflict policy.

Backfill pattern:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.user_daily_metrics
PARTITION (event_date)
SELECT
    user_id,
    COUNT(*) AS event_count,
    SUM(amount) AS revenue,
    event_date
FROM raw.events
WHERE event_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
GROUP BY event_date, user_id;
```

Validation:

```sql
SELECT
    event_date,
    COUNT(*) AS rows,
    COUNT(DISTINCT user_id) AS users
FROM mart.user_daily_metrics
WHERE event_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
GROUP BY event_date
ORDER BY event_date;
```

## Data Profiling

Profile unknown data before joins, overwrites, repairs, or schema changes.

Partition coverage:

```sql
SELECT
    event_date,
    COUNT(*) AS rows,
    MIN(event_time) AS min_event_time,
    MAX(event_time) AS max_event_time
FROM raw.events
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date
ORDER BY event_date;
```

Null profile:

```sql
SELECT
    COUNT(*) AS rows,
    COUNT(*) FILTER (WHERE event_id IS NULL) AS null_event_ids,
    COUNT(*) FILTER (WHERE user_id IS NULL) AS null_user_ids,
    COUNT(*) FILTER (WHERE amount IS NULL) AS null_amounts
FROM raw.events
WHERE event_date = DATE '2026-05-05';
```

Key skew:

```sql
SELECT
    user_id,
    COUNT(*) AS rows
FROM raw.events
WHERE event_date = DATE '2026-05-05'
GROUP BY user_id
ORDER BY rows DESC
LIMIT 50;
```

Duplicate keys:

```sql
SELECT
    event_id,
    COUNT(*) AS rows
FROM raw.events
WHERE event_date = DATE '2026-05-05'
GROUP BY event_id
HAVING COUNT(*) > 1
LIMIT 100;
```

## Dirty Data Handling

For raw landing tables, parse and quarantine deliberately.

Typed parsing:

```sql
WITH parsed AS (
    SELECT
        raw_event_id AS event_id,
        raw_user_id AS user_id,
        TRY_CAST(raw_event_time AS TIMESTAMP) AS event_time,
        TRY_CAST(raw_amount AS DECIMAL(18, 2)) AS amount,
        ingestion_date
    FROM landing.events_raw
    WHERE ingestion_date = DATE '2026-05-05'
),
valid AS (
    SELECT *
    FROM parsed
    WHERE event_id IS NOT NULL
      AND event_time IS NOT NULL
),
invalid AS (
    SELECT *
    FROM parsed
    WHERE event_id IS NULL
       OR event_time IS NULL
)
SELECT * FROM valid;
```

Rules:

- Use `TRY_CAST` when dirty values are expected and should be quarantined.
- Use strict `CAST` when dirty values should fail the pipeline.
- Keep invalid records with a reason when regulatory or business requirements require traceability.
- Do not silently replace null keys with sentinel strings.
- Do not parse timestamps without an explicit timezone policy.

## Schema Evolution

Before changing schema:

- Inspect existing DDL.
- Identify all downstream consumers.
- Determine whether the change is additive, type-widening, rename, or breaking.
- Understand whether table format and readers support schema evolution.
- Validate old and new partitions with mixed schemas.

Add column:

```sql
ALTER TABLE mart.daily_revenue ADD COLUMNS (
    net_revenue DECIMAL(18, 2) COMMENT 'Revenue after refunds'
);
```

Rename column support varies by catalog and Spark version. Avoid assuming it is safe for Hive Parquet/ORC tables.

Type changes can be dangerous:

- `INT` to `BIGINT` is usually safer than `BIGINT` to `INT`.
- `DOUBLE` to `DECIMAL` can change values.
- `STRING` to typed date/timestamp requires parsing and validation.
- Parquet physical schemas across partitions can conflict.

After schema changes:

```sql
REFRESH TABLE mart.daily_revenue;
DESCRIBE FORMATTED mart.daily_revenue;
SELECT * FROM mart.daily_revenue WHERE event_date = DATE '2026-05-05' LIMIT 10;
```

## Statistics and Optimizer Metadata

Spark relies on table and column statistics when available.

Compute table stats:

```sql
ANALYZE TABLE raw.events COMPUTE STATISTICS;
```

Compute column stats:

```sql
ANALYZE TABLE raw.events COMPUTE STATISTICS FOR COLUMNS event_date, user_id, country;
```

Inspect stats:

```sql
DESCRIBE EXTENDED raw.events;
DESCRIBE EXTENDED raw.events user_id;
```

Refresh stats when:

- Large backfills complete.
- Partitions are replaced.
- Compaction changes file sizes.
- Join/filter columns change distribution.
- Optimizer chooses bad broadcast or join strategy.

Do not trust `EXPLAIN COST` when statistics are missing or stale.

## File Layout and Small Files

Small files hurt Spark planning, HDFS namenode pressure, and scan performance.

Healthy analytical file sizes are often:

- 128-512 MB for general Parquet/ORC tables.
- Larger for very large sequential scan-heavy fact tables.
- Smaller only for low-volume partitions or latency-sensitive ingestion.

Diagnose small files:

```bash
hdfs dfs -count -h /warehouse/mart/daily_revenue/event_date=2026-05-05
hdfs dfs -du -h /warehouse/mart/daily_revenue/event_date=2026-05-05 | head
```

SQL-side row distribution:

```sql
SELECT
    event_date,
    country,
    COUNT(*) AS rows
FROM mart.daily_revenue_source
WHERE event_date = DATE '2026-05-05'
GROUP BY event_date, country
ORDER BY rows DESC;
```

Compaction with CTAS or overwrite:

```sql
CREATE TABLE tmp.daily_revenue_compacted
USING PARQUET
PARTITIONED BY (event_date)
AS
SELECT *
FROM mart.daily_revenue
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05';
```

Then validate and replace targeted partitions or perform a controlled table swap.

Rules:

- Do not compact by reading the entire table when only a few partitions are unhealthy.
- Do not force one output file for large partitions.
- Use `REBALANCE` when available and AQE is enabled for better output distribution.
- Use table-format optimize/compaction features for Delta/Iceberg/Hudi rather than manual file operations.

## Bucketing and Sorting

Hive bucketing can help some joins, but only when platform behavior is well understood.

Use bucketing only when:

- Join keys are stable and frequent.
- Both sides are bucketed compatibly.
- The engine respects bucket metadata.
- File counts and bucket counts are maintained.
- The operational cost is justified.

Example:

```sql
CREATE TABLE mart.user_events_bucketed (
    event_date DATE,
    user_id STRING,
    event_count BIGINT
)
CLUSTERED BY (user_id) INTO 1024 BUCKETS
STORED AS PARQUET;
```

Agent rule: do not recommend bucketing as a first-line fix for a single slow query. Prefer pruning, filtering, aggregation, stats, and skew handling first.

## Querying HDFS-Backed Tables Safely

Use partition filters:

```sql
SELECT
    event_date,
    COUNT(*) AS rows
FROM raw.events
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date;
```

Avoid full-table scans:

```sql
SELECT COUNT(*)
FROM raw.events;
```

If a full-table count is required, state that it will scan all data and may be expensive.

Avoid global sorts:

```sql
SELECT *
FROM raw.events
WHERE event_date = DATE '2026-05-05'
ORDER BY event_time;
```

Prefer bounded diagnostics:

```sql
SELECT *
FROM raw.events
WHERE event_date = DATE '2026-05-05'
ORDER BY event_time
LIMIT 100;
```

## Filtering by Another Dataset

Do not collect large key lists into `IN (...)`. Model the filter as a relation.

Use `LEFT SEMI JOIN`:

```sql
WITH selected_users AS (
    SELECT DISTINCT user_id
    FROM mart.campaign_users
    WHERE campaign_date = DATE '2026-05-05'
),
events AS (
    SELECT
        event_date,
        user_id,
        event_id,
        amount
    FROM raw.events
    WHERE event_date = DATE '2026-05-05'
)
SELECT
    e.event_date,
    e.user_id,
    e.event_id,
    e.amount
FROM events e
LEFT SEMI JOIN selected_users s
    ON e.user_id = s.user_id;
```

Use `LEFT ANTI JOIN` for exclusion:

```sql
SELECT e.*
FROM raw.events e
LEFT ANTI JOIN fraud.blocked_users b
    ON e.user_id = b.user_id
WHERE e.event_date = DATE '2026-05-05';
```

## Join Cases on Hive/HDFS Data

Large fact to small dimension:

```sql
SELECT /*+ BROADCAST(d) */
    f.event_date,
    f.user_id,
    d.country,
    f.amount
FROM raw.events f
JOIN dim.users_current d
    ON f.user_id = d.user_id
WHERE f.event_date = DATE '2026-05-05';
```

Use broadcast only when the dimension is genuinely small and stats are trustworthy.

Large fact to large fact:

```sql
WITH events_a AS (
    SELECT user_id, event_date, event_id
    FROM raw.events
    WHERE event_date = DATE '2026-05-05'
),
orders_a AS (
    SELECT user_id, order_date, order_id, amount
    FROM raw.orders
    WHERE order_date = DATE '2026-05-05'
)
SELECT
    e.user_id,
    COUNT(DISTINCT e.event_id) AS events,
    COUNT(DISTINCT o.order_id) AS orders,
    SUM(o.amount) AS order_amount
FROM events_a e
JOIN orders_a o
    ON e.user_id = o.user_id
GROUP BY e.user_id;
```

Rules:

- Filter both sides by partitions.
- Project only required columns.
- Check skew on join keys.
- Check whether join cardinality is expected.
- Avoid accidental many-to-many joins.

## Permissions and Ownership

Permission issues can appear as read failures, write failures, or missing data.

Inspect:

```bash
hdfs dfs -ls -d /warehouse/raw/events
hdfs dfs -getfacl /warehouse/raw/events
hdfs dfs -getfacl /warehouse/raw/events/event_date=2026-05-05
```

Common issues:

- Spark job runs as a service user different from the interactive user.
- Partition directories are created with wrong owner/group.
- ACLs differ across partitions.
- Staging directory is writable but final table directory is not.
- Warehouse database path has different permissions from table path.

Agent rule: before recommending retries for permission errors, verify effective user, table location, partition location, and HDFS ACLs.

## Common Failure Modes

### Partition Exists in Hive but Files Are Missing

Symptoms:

- Query returns zero rows.
- Query fails with `FileNotFoundException`.
- `SHOW PARTITIONS` lists the partition.
- HDFS path does not exist or is empty.

Checks:

```sql
SHOW PARTITIONS raw.events;
DESCRIBE EXTENDED raw.events PARTITION (event_date = '2026-05-05');
```

```bash
hdfs dfs -ls /warehouse/raw/events/event_date=2026-05-05
```

Fix options:

- Restore files from backup.
- Drop partition metadata if data should not exist.
- Alter partition location if files moved.
- Refresh Spark metadata after repair.

### Files Exist but Hive Does Not See the Partition

Symptoms:

- Direct path read returns rows.
- Table query returns zero rows for that partition.
- HDFS has Hive-style partition directory.
- `SHOW PARTITIONS` does not list it.

Fix:

```sql
ALTER TABLE raw.events ADD IF NOT EXISTS
    PARTITION (event_date = '2026-05-05')
    LOCATION 'hdfs:///warehouse/raw/events/event_date=2026-05-05';

REFRESH TABLE raw.events;
```

For many partitions:

```sql
MSCK REPAIR TABLE raw.events;
REFRESH TABLE raw.events;
```

### Stale Spark File Listing

Symptoms:

- Another job overwrote files.
- Current Spark session still tries old files.
- `FileNotFoundException` references deleted part files.

Fix:

```sql
UNCACHE TABLE raw.events;
REFRESH TABLE raw.events;
```

Then rerun the query.

### Wrong Partition Type

Symptoms:

- Partition pruning does not work as expected.
- `event_date` is stored as `STRING`, but SQL uses date literals.
- Directory names do not match type expectations.

Inspect:

```sql
DESCRIBE FORMATTED raw.events;
SHOW PARTITIONS raw.events;
```

Fix:

- Use consistent typed predicates.
- Consider rebuilding table metadata with correct partition types.
- Avoid mixing string and date partition semantics.

### Small-File Incident

Symptoms:

- Query planning is slow.
- Namenode pressure increases.
- Spark UI shows many tiny input tasks.
- HDFS partition has thousands or millions of small files.

Fix options:

- Compact affected partitions.
- Adjust upstream shuffle/output partition count.
- Use AQE coalescing.
- Use table-format compaction where available.
- Avoid streaming micro-batches writing directly to analytical table without compaction.

### Schema Mismatch Across Partitions

Symptoms:

- Query fails reading a subset of partitions.
- Old partitions have different Parquet schema.
- A column has conflicting physical types.

Checks:

```sql
SELECT *
FROM raw.events
WHERE event_date = DATE '2026-05-05'
LIMIT 10;
```

Compare old and new partitions by direct reads if needed.

Fix options:

- Reprocess affected partitions.
- Write a compatibility view with explicit casts when safe.
- Rebuild table with consistent schema.
- Avoid blindly setting permissive schema merge flags on critical pipelines.

## EXPLAIN for HDFS/Hive Tables

Use:

```sql
EXPLAIN FORMATTED
SELECT
    user_id,
    SUM(amount) AS revenue
FROM raw.events
WHERE event_date = DATE '2026-05-05'
GROUP BY user_id;
```

Check:

- `FileScan parquet` or `FileScan orc`.
- `Location`.
- `PartitionFilters`.
- `PushedFilters`.
- `ReadSchema`.
- `Exchange`.
- `BroadcastHashJoin` or `SortMergeJoin`.
- `HashAggregate` partial and final.

Healthy scan example:

```text
FileScan parquet raw.events
PartitionFilters: [isnotnull(event_date), (event_date = 2026-05-05)]
PushedFilters: [IsNotNull(user_id)]
ReadSchema: struct<user_id:string,amount:decimal(18,2)>
```

Unhealthy scan:

```text
PartitionFilters: []
ReadSchema: struct<all columns...>
```

If partition filters are missing, fix the query before tuning cluster resources.

## Cache and Temporary Views

Temporary views do not persist data:

```sql
CREATE OR REPLACE TEMP VIEW filtered_events AS
SELECT *
FROM raw.events
WHERE event_date = DATE '2026-05-05';
```

Cache only reused bounded data:

```sql
CACHE TABLE filtered_events;
SELECT COUNT(*) FROM filtered_events;
```

Uncache:

```sql
UNCACHE TABLE filtered_events;
```

Rules:

- Do not cache raw PB-scale tables.
- Do not rely on cache after underlying files change.
- Refresh or uncache when debugging stale results.
- Prefer materialized partitioned intermediate tables for expensive reusable production stages.

## Data Quality Validation

Before and after writes, validate:

- Row counts by partition.
- Null rates for key fields.
- Duplicate business keys.
- Min/max timestamps.
- Metric totals.
- Join coverage.
- Partition list and HDFS file count.
- Output schema and column order.

Row count by partition:

```sql
SELECT
    event_date,
    COUNT(*) AS rows
FROM mart.daily_revenue
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date
ORDER BY event_date;
```

Duplicate check:

```sql
SELECT
    event_date,
    country,
    COUNT(*) AS rows
FROM mart.daily_revenue
WHERE event_date = DATE '2026-05-05'
GROUP BY event_date, country
HAVING COUNT(*) > 1;
```

Reconciliation:

```sql
WITH source AS (
    SELECT event_date, SUM(amount) AS source_revenue
    FROM raw.events
    WHERE event_date = DATE '2026-05-05'
      AND event_type = 'purchase'
    GROUP BY event_date
),
target AS (
    SELECT event_date, SUM(revenue) AS target_revenue
    FROM mart.daily_revenue
    WHERE event_date = DATE '2026-05-05'
    GROUP BY event_date
)
SELECT
    s.event_date,
    s.source_revenue,
    t.target_revenue,
    s.source_revenue - t.target_revenue AS diff
FROM source s
JOIN target t
    ON s.event_date = t.event_date;
```

## Recovery Patterns

### Restore Missing Partition Metadata

If HDFS files exist:

```sql
ALTER TABLE raw.events ADD IF NOT EXISTS
    PARTITION (event_date = '2026-05-05')
    LOCATION 'hdfs:///warehouse/raw/events/event_date=2026-05-05';

REFRESH TABLE raw.events;
```

### Remove Bad Partition Metadata

If metadata points to bad or missing data:

```sql
ALTER TABLE raw.events DROP IF EXISTS PARTITION (event_date = '2026-05-05');
REFRESH TABLE raw.events;
```

Then restore or rewrite the partition.

### Rewrite a Bad Partition

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.daily_revenue
PARTITION (event_date)
SELECT
    country,
    SUM(amount) AS revenue,
    CURRENT_TIMESTAMP() AS processed_at,
    event_date
FROM raw.events
WHERE event_date = DATE '2026-05-05'
GROUP BY event_date, country;
```

Validate immediately.

## Anti-Patterns

Do not:

- Query huge Hive tables without partition filters.
- Use `SELECT *` in production writes.
- Use broad `INSERT OVERWRITE TABLE target SELECT ...` for incremental work.
- Run `MSCK REPAIR TABLE` daily on massive partitioned tables when targeted adds are available.
- Delete HDFS paths when the user asked only to drop metadata.
- Assume external table drops delete files.
- Assume managed table drops preserve files.
- Hard-code direct HDFS paths in business logic without a reason.
- Mix Parquet and ORC files under one table location.
- Store many schemas under one HDFS table root.
- Partition by high-cardinality IDs.
- Create millions of tiny files.
- Use `DISTINCT` to hide duplicate partitions or join explosions.
- Ignore stale metadata after out-of-band writes.
- Trust `EXPLAIN COST` with missing statistics.
- Change table schema without checking downstream consumers.
- Retry permission errors without checking ACLs and effective user.

## Agent Review Checklist

When reviewing Spark SQL over HDFS/Hive, check:

- Is the table managed or external?
- Is the table location known?
- Are partition locations inherited or custom?
- Does the query use direct partition filters?
- Are partition column types used correctly?
- Are source and target partition ranges bounded?
- Is overwrite static or dynamic, and is that intentional?
- Are partition columns projected correctly for dynamic insert?
- Are table schema and output column order compatible?
- Are metastore partitions synchronized with HDFS?
- Are HDFS paths real and readable by the effective user?
- Is `REFRESH TABLE` needed after out-of-band changes?
- Are file counts and file sizes healthy?
- Are table and column statistics fresh enough?
- Is a full scan, full repair, or full overwrite truly required?
- Are post-write validation queries present?
- Is rollback or recovery possible?

## Incident Playbook

If a query returns zero rows unexpectedly:

1. Check the partition filter and partition type.
2. Run `SHOW PARTITIONS`.
3. Run `DESCRIBE EXTENDED ... PARTITION`.
4. Check HDFS path existence.
5. Try a direct path read for diagnosis.
6. Add/repair/refresh metadata as needed.

If a query fails with `FileNotFoundException`:

1. Check whether another job overwrote or deleted files.
2. Run `REFRESH TABLE`.
3. Uncache the table if cached.
4. Check partition metadata locations.
5. Check HDFS existence for referenced files.
6. Rerun only after metadata is consistent.

If a write overwrote too much:

1. Stop downstream publication if possible.
2. Identify touched partitions from logs and HDFS modification times.
3. Restore from backup, snapshot, or previous table version if available.
4. Re-register restored partitions.
5. Validate counts and metrics.
6. Fix overwrite mode and partition filters before rerun.

If repair is slow or stuck:

1. Check partition count and HDFS directory breadth.
2. Prefer targeted partition adds for the required range.
3. Check metastore load.
4. Check HDFS namenode load.
5. Avoid repeated concurrent repairs.

## Enterprise Defaults

When context is missing, assume:

- HDFS/Hive tables are large.
- Full scans and full overwrites are unsafe.
- Partition metadata may be stale after out-of-band writes.
- External tables should not be treated as disposable.
- Managed table lifecycle must be verified before drop/truncate.
- Targeted partition operations are preferred over whole-table repair.
- Post-write validation is required.
- HDFS file layout matters for query performance.

