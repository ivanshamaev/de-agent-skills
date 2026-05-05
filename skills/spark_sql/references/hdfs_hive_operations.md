# HDFS and Hive Operations for Spark SQL

Use this reference when working with Spark SQL over Hive Metastore and HDFS: table DDL, partition metadata, direct HDFS inspection, session settings, safe overwrites, profiling, and dirty data.

## Hive Metastore Inspection

```sql
SHOW CREATE TABLE raw.events;
SHOW TBLPROPERTIES raw.events;
SHOW COLUMNS IN raw.events;
DESCRIBE FORMATTED raw.events;
SHOW PARTITIONS raw.events;
DESCRIBE EXTENDED raw.events PARTITION (event_date = '2026-01-01');
```

Use these before changing table definitions, repairing partitions, or debugging mismatches between Spark results and HDFS files.

## Partition Metadata

Prefer targeted partition registration for known paths:

```sql
ALTER TABLE raw.events ADD IF NOT EXISTS
    PARTITION (event_date = '2026-01-01', country = 'US')
    LOCATION 'hdfs:///warehouse/raw/events/event_date=2026-01-01/country=US';
```

Use bulk repair only when many partitions need discovery:

```sql
MSCK REPAIR TABLE raw.events;
REFRESH TABLE raw.events;
ALTER TABLE raw.events DROP IF EXISTS PARTITION (event_date = '2025-01-01');
```

`MSCK REPAIR` updates metastore partition metadata from the directory tree and can be slow on large partition counts. `REFRESH TABLE` refreshes Spark's cached metadata/file listings.

## DDL on HDFS

External tables keep HDFS data independent from table metadata:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS raw.events (
    user_id STRING,
    event_time TIMESTAMP,
    event_type STRING,
    amount DECIMAL(18, 2)
)
PARTITIONED BY (event_date DATE, country STRING)
STORED AS PARQUET
LOCATION 'hdfs:///warehouse/raw/events'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');
```

Managed tables let Spark/Hive own the table lifecycle:

```sql
CREATE TABLE IF NOT EXISTS dwh.daily_revenue (
    country STRING,
    revenue DECIMAL(18, 2)
)
PARTITIONED BY (event_date DATE)
STORED AS ORC
LOCATION 'hdfs:///warehouse/dwh/daily_revenue';
```

Use external tables for raw/landing data where dropping metadata must not delete files. Use managed tables only when the platform should own cleanup.

## Direct HDFS Inspection

Use HDFS commands when metastore metadata and disk state disagree:

```bash
hdfs dfs -du -s -h /warehouse/raw/events/event_date=2026-01-01
hdfs dfs -ls /warehouse/raw/events/event_date=2026-01-01
hdfs dfs -test -d /warehouse/raw/events/event_date=2026-01-01
```

Small-file diagnosis: inspect `hdfs dfs -ls` output and compare file counts/sizes against the target file size, usually 128-512MB for analytical columnar tables.

## Session Settings

Recommend `SET` statements before running risky writes or heavy queries:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.shuffle.partitions = 400;
SET spark.sql.autoBroadcastJoinThreshold = -1;
SET spark.sql.storeAssignmentPolicy = ANSI;
```

Use dynamic partition overwrite only when the write query projects the partition columns correctly. Static overwrite can remove broader partition ranges than intended.

## Data Profiling

Profile unknown data before designing joins, overwrites, or deduplication:

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT user_id) AS unique_users,
    MIN(event_date) AS min_date,
    MAX(event_date) AS max_date,
    COUNT(*) FILTER (WHERE user_id IS NULL) AS null_user_ids
FROM raw.events
WHERE event_date = DATE '2026-01-01';
```

Check partition coverage and key skew:

```sql
SELECT event_date, COUNT(*) AS row_count
FROM raw.events
WHERE event_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
GROUP BY event_date
ORDER BY event_date;

SELECT user_id, COUNT(*) AS cnt
FROM raw.events
WHERE event_date = DATE '2026-01-01'
GROUP BY user_id
ORDER BY cnt DESC
LIMIT 20;
```

## Safe and Idempotent Writes

Partition-scoped overwrite:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE dwh.daily_revenue
PARTITION (event_date)
SELECT
    country,
    SUM(amount) AS revenue,
    event_date
FROM filtered_events
GROUP BY event_date, country;
```

Validate after writing:

```sql
SELECT COUNT(*) FROM dwh.daily_revenue WHERE event_date = DATE '2026-01-01';
```

For full replacement, prefer atomic table-format operations when available. Otherwise use CTAS to a new table and a controlled rename/drop sequence:

```sql
CREATE TABLE dwh.daily_revenue_new USING PARQUET AS SELECT ...;
ALTER TABLE dwh.daily_revenue RENAME TO dwh.daily_revenue_old;
ALTER TABLE dwh.daily_revenue_new RENAME TO dwh.daily_revenue;
DROP TABLE dwh.daily_revenue_old;
```

## Dirty Data Patterns

Deduplicate staging input deterministically:

```sql
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, event_time
            ORDER BY _ingestion_time DESC
        ) AS rn
    FROM raw.events_staging
    WHERE event_date = DATE '2026-01-01'
)
SELECT user_id, event_time, event_type, amount, event_date
FROM deduped
WHERE rn = 1;
```

Make null-key behavior explicit and parse dirty types safely:

```sql
SELECT
    e.user_id,
    COALESCE(u.country, 'UNKNOWN') AS country,
    e.amount
FROM raw.events e
LEFT JOIN dwh.users u
    ON e.user_id = u.user_id
   AND e.user_id IS NOT NULL;

SELECT
    user_id,
    TRY_CAST(raw_amount AS DECIMAL(18, 2)) AS amount
FROM raw.events_raw;
```

## Debugging Workflow

1. Run `EXPLAIN FORMATTED` before executing a large query.
2. Sample or narrow by partition before a full scan.
3. Run suspicious CTEs in isolation with `LIMIT`.
4. Count rows at each stage to catch join explosions or filter mistakes.
5. Only write after validating schema, counts, partitions, and overwrite mode.
