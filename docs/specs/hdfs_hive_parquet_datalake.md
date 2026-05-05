# HDFS Hive Parquet Data Lake with Spark SQL

This specification describes how to design, build, operate, and review an enterprise data lake on HDFS using Hive Metastore, Spark SQL, and partitioned Parquet data. It is written for agents that must produce production-grade guidance, DDL, SQL pipelines, operational checks, and review feedback for large-scale data platforms.

The target architecture assumes:

- HDFS is the primary storage layer.
- Hive Metastore is the catalog and metadata layer.
- Spark SQL is the main processing and transformation engine.
- Data is stored as Parquet in partitioned directory layouts.
- Tables are usually external Hive tables over HDFS locations.
- Workloads range from daily batch processing to large historical backfills.
- Data volume can grow from terabytes to petabytes.

## Agent Contract

When designing or modifying an HDFS/Hive/Parquet data lake, the agent must:

- Treat metadata, physical files, and SQL semantics as separate concerns that must stay aligned.
- Prefer explicit HDFS locations, Hive databases, table ownership, partition columns, and schema contracts.
- Design for incremental processing, bounded backfills, partition pruning, and safe overwrites.
- Avoid full-table scans, full-table overwrites, and full-table repairs unless explicitly required.
- Keep raw data immutable where possible.
- Write curated data in typed Parquet tables with predictable partitioning.
- Use Spark SQL built-ins, Hive-compatible DDL, and table operations instead of ad hoc filesystem manipulation.
- Validate every production write with row counts, partition checks, schema checks, and reconciliation metrics.
- Call out assumptions about managed vs external tables, retention, security, schema evolution, and downstream engines.

## Design Goals

A production HDFS data lake should provide:

- Durable storage for raw and curated data.
- Clear separation between landing, raw, refined, and serving zones.
- Hive catalog discoverability.
- Efficient Spark SQL reads through partition pruning and column pruning.
- Reliable incremental writes.
- Repeatable backfills.
- Auditable data lineage.
- Controlled schema evolution.
- Reasonable file sizes and HDFS namenode load.
- Security through HDFS ACLs, Kerberos, Ranger/Sentry or platform equivalents.
- Operational recovery for bad partitions, stale metadata, and failed writes.

## Non-Goals

Do not design the lake as:

- A dumping ground of unrelated files under one HDFS directory.
- A system where production tables are read only through direct paths.
- A set of untyped CSV/JSON tables used directly by marts.
- A platform where every job owns its own incompatible layout.
- A pipeline that depends on manual HDFS deletes for correctness.
- A model where full rebuild is the default daily processing mode.

## Reference Architecture

Use a layered lake model:

```text
/data/lake/
  landing/
  raw/
  refined/
  mart/
  sandbox/
  tmp/
  quarantine/
```

Recommended Hive databases:

```sql
CREATE DATABASE IF NOT EXISTS landing
LOCATION 'hdfs:///data/lake/landing';

CREATE DATABASE IF NOT EXISTS raw
LOCATION 'hdfs:///data/lake/raw';

CREATE DATABASE IF NOT EXISTS refined
LOCATION 'hdfs:///data/lake/refined';

CREATE DATABASE IF NOT EXISTS mart
LOCATION 'hdfs:///data/lake/mart';

CREATE DATABASE IF NOT EXISTS quarantine
LOCATION 'hdfs:///data/lake/quarantine';
```

Layer responsibilities:

- `landing`: files as received from source systems, often immutable and close to source format.
- `raw`: typed, minimally transformed, source-aligned Parquet tables.
- `refined`: cleaned, deduplicated, conformed, domain-oriented tables.
- `mart`: consumption-ready fact, aggregate, and dimensional tables.
- `quarantine`: invalid or rejected records with parse or validation reasons.
- `tmp`: job-scoped temporary data with TTL.
- `sandbox`: non-production exploration with limited retention and no production dependencies.

## HDFS Directory Standards

Use stable, predictable table roots:

```text
hdfs:///data/lake/raw/events/
hdfs:///data/lake/refined/user_events/
hdfs:///data/lake/mart/daily_revenue/
```

Use Hive-style partitions:

```text
hdfs:///data/lake/raw/events/
  event_date=2026-05-05/
    source_system=web/
      part-00000-....snappy.parquet
```

Rules:

- One logical table per HDFS root.
- Do not mix schemas or formats under one table root.
- Do not write unrelated temporary files under production table roots.
- Do not use personal user directories for production outputs.
- Keep partition directory names identical to partition column names.
- Avoid spaces and unusual characters in paths.
- Use `_tmp`, `_staging`, or `/data/lake/tmp` for job staging, not hidden directories inside table roots.
- Use HDFS snapshots or backup policy for critical zones when available.

## Hive Table Strategy

Use external tables for most lake data:

- Raw data must not be deleted by dropping metadata.
- HDFS lifecycle is controlled by platform retention policies.
- Several engines may read the same data.
- Recovery often requires re-registering metadata over existing files.

Use managed tables only when:

- The platform explicitly owns the full table lifecycle.
- Drop/truncate semantics are understood.
- Data is not shared outside the owning catalog.

Agent rule: before any destructive operation, identify whether the table is external or managed.

External Parquet table pattern:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS raw.events (
    event_id STRING,
    user_id STRING,
    event_time TIMESTAMP,
    event_type STRING,
    amount DECIMAL(18, 2),
    ingestion_ts TIMESTAMP,
    source_file STRING
)
PARTITIONED BY (event_date DATE, source_system STRING)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/raw/events'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'lake.layer' = 'raw',
    'lake.owner' = 'data-platform',
    'lake.retention' = '365d'
);
```

Serving mart table pattern:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS mart.daily_revenue (
    country STRING,
    revenue DECIMAL(18, 2),
    order_count BIGINT,
    processed_at TIMESTAMP
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/mart/daily_revenue'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'lake.layer' = 'mart',
    'lake.owner' = 'analytics-platform'
);
```

## Naming Standards

Use stable names:

- Database names reflect lake zones: `landing`, `raw`, `refined`, `mart`, `quarantine`.
- Table names are lowercase snake_case.
- Partition columns are lowercase snake_case.
- Timestamp columns end with `_time`, `_ts`, or `_at` according to local convention.
- Date partition columns use `_date`.
- Raw source lineage fields use names such as `source_system`, `source_file`, `ingestion_ts`, `batch_id`.

Avoid:

- Abbreviations that are not domain standards.
- Table names with dates.
- Environment names inside table names when databases or catalogs already separate environments.
- Partition columns named generically as `dt` unless this is an established platform standard.

## Parquet Standards

Parquet is the default format for typed analytical data.

Rules:

- Use Snappy compression by default for balanced CPU and IO.
- Use ZSTD only when the platform supports it and CPU tradeoffs are accepted.
- Store money and exact numeric metrics as `DECIMAL`, not `DOUBLE`.
- Store identifiers as `STRING` unless numeric behavior is required.
- Avoid nested schemas in curated marts unless downstream engines support them well.
- Avoid raw JSON strings in refined and mart layers.
- Keep schema consistent across partitions.
- Do not mix Parquet files with other formats in the same table location.

Recommended table properties:

```sql
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'parquet.enable.summary-metadata' = 'false'
)
```

Use platform defaults when they are centrally managed.

## Partitioning Strategy

Partitioning should reduce scan cost without overloading the metastore or HDFS namenode.

Common partition columns:

- `event_date`
- `ingestion_date`
- `snapshot_date`
- `business_date`
- `country` or `region` only when cardinality is controlled
- `source_system` when source-specific reads are common

Avoid partitioning by:

- `user_id`
- `account_id`
- `request_id`
- `event_id`
- `uuid`
- Any high-cardinality or unbounded field

Date partition pattern:

```sql
PARTITIONED BY (event_date DATE)
```

Date plus low-cardinality domain:

```sql
PARTITIONED BY (event_date DATE, source_system STRING)
```

Rules:

- Partition columns must appear in common query filters.
- Partition values must be predictable and validated.
- Partition count must remain operationally manageable.
- Do not create partitions with only a few tiny files unless low volume is expected.
- Prefer one or two partition levels for most tables.
- Use hour partitions only for very large data or latency-driven workloads.

## Table Layer Design

### Landing Layer

Landing stores source-arrival data with minimal changes.

Characteristics:

- Often text, JSON, Avro, or raw Parquet from source.
- Partitioned by `ingestion_date` and optionally `source_system`.
- Immutable after arrival.
- Used for replay and audit, not for direct business analytics.

Example external table for raw CSV landing:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS landing.orders_csv (
    raw_order_id STRING,
    raw_user_id STRING,
    raw_order_time STRING,
    raw_amount STRING,
    raw_country STRING
)
PARTITIONED BY (ingestion_date DATE, source_system STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///data/lake/landing/orders_csv';
```

Agent rule: convert landing data to typed Parquet before using it in refined or mart pipelines.

### Raw Layer

Raw stores source-aligned, typed Parquet.

Characteristics:

- Minimal transformation.
- Schema is explicit.
- Records preserve source identifiers and ingestion metadata.
- Partitioned by event date or ingestion date.
- Suitable for replay, debugging, and building refined data.

Example:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS raw.orders (
    order_id STRING,
    user_id STRING,
    order_time TIMESTAMP,
    amount DECIMAL(18, 2),
    country STRING,
    ingestion_ts TIMESTAMP,
    source_file STRING,
    batch_id STRING
)
PARTITIONED BY (order_date DATE, source_system STRING)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/raw/orders';
```

### Refined Layer

Refined stores cleaned and conformed data.

Characteristics:

- Deduplicated where required.
- Business keys are standardized.
- Dimensions are conformed.
- Dirty records are excluded or quarantined.
- Data quality checks are enforced.

Example:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS refined.orders_clean (
    order_id STRING,
    user_id STRING,
    order_time TIMESTAMP,
    amount DECIMAL(18, 2),
    country STRING,
    is_refund BOOLEAN,
    ingestion_ts TIMESTAMP
)
PARTITIONED BY (order_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/refined/orders_clean';
```

### Mart Layer

Mart stores consumption-ready data.

Characteristics:

- Fact tables, dimensions, snapshots, and aggregates.
- Clear grain and owner.
- Stable schema contract.
- Optimized for common analytical queries.
- Strict write validation.

Example:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS mart.country_daily_revenue (
    country STRING,
    gross_revenue DECIMAL(18, 2),
    refund_amount DECIMAL(18, 2),
    net_revenue DECIMAL(18, 2),
    order_count BIGINT,
    processed_at TIMESTAMP
)
PARTITIONED BY (order_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/mart/country_daily_revenue';
```

## Spark SQL Session Baseline

Recommended baseline for large Spark SQL jobs:

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.sources.partitionOverwriteMode = dynamic;
SET spark.sql.storeAssignmentPolicy = ANSI;
```

Tune shuffle partitions from data size:

```sql
-- Example only. Size this from post-shuffle data volume.
SET spark.sql.shuffle.partitions = 4000;
```

Broadcast control:

```sql
-- Disable when stale Hive stats cause unsafe broadcasts.
SET spark.sql.autoBroadcastJoinThreshold = -1;
```

Agent rule: settings do not replace good table design, partition pruning, and correct joins.

## Ingestion Pattern: Landing to Raw

Convert raw source files into typed partitioned Parquet.

Example parse and write:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE raw.orders
PARTITION (order_date, source_system)
SELECT
    raw_order_id AS order_id,
    raw_user_id AS user_id,
    TRY_CAST(raw_order_time AS TIMESTAMP) AS order_time,
    TRY_CAST(raw_amount AS DECIMAL(18, 2)) AS amount,
    raw_country AS country,
    CURRENT_TIMESTAMP() AS ingestion_ts,
    INPUT_FILE_NAME() AS source_file,
    '${batch_id}' AS batch_id,
    TO_DATE(TRY_CAST(raw_order_time AS TIMESTAMP)) AS order_date,
    source_system
FROM landing.orders_csv
WHERE ingestion_date = DATE '2026-05-05'
  AND source_system = 'orders_api'
  AND raw_order_id IS NOT NULL;
```

Quarantine invalid records:

```sql
INSERT INTO TABLE quarantine.orders_invalid
PARTITION (ingestion_date, source_system)
SELECT
    raw_order_id,
    raw_user_id,
    raw_order_time,
    raw_amount,
    raw_country,
    CASE
        WHEN raw_order_id IS NULL THEN 'missing_order_id'
        WHEN TRY_CAST(raw_order_time AS TIMESTAMP) IS NULL THEN 'invalid_order_time'
        WHEN TRY_CAST(raw_amount AS DECIMAL(18, 2)) IS NULL THEN 'invalid_amount'
        ELSE 'unknown'
    END AS invalid_reason,
    CURRENT_TIMESTAMP() AS quarantined_at,
    ingestion_date,
    source_system
FROM landing.orders_csv
WHERE ingestion_date = DATE '2026-05-05'
  AND source_system = 'orders_api'
  AND (
      raw_order_id IS NULL
      OR TRY_CAST(raw_order_time AS TIMESTAMP) IS NULL
      OR TRY_CAST(raw_amount AS DECIMAL(18, 2)) IS NULL
  );
```

Rules:

- Keep landing immutable.
- Convert to typed Parquet in raw.
- Preserve lineage fields.
- Quarantine bad records with reasons.
- Validate partition counts after write.

## Refined Transform Pattern

Build cleaned tables from raw using bounded partition ranges.

Deduplicate deterministically:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE refined.orders_clean
PARTITION (order_date)
WITH source_orders AS (
    SELECT
        order_id,
        user_id,
        order_time,
        amount,
        country,
        ingestion_ts,
        order_date
    FROM raw.orders
    WHERE order_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
),
deduped AS (
    SELECT
        order_id,
        user_id,
        order_time,
        amount,
        country,
        ingestion_ts,
        order_date
    FROM (
        SELECT
            source_orders.*,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY ingestion_ts DESC NULLS LAST
            ) AS rn
        FROM source_orders
    ) x
    WHERE rn = 1
)
SELECT
    order_id,
    user_id,
    order_time,
    amount,
    country,
    amount < CAST(0 AS DECIMAL(18, 2)) AS is_refund,
    ingestion_ts,
    order_date
FROM deduped;
```

Rules:

- Filter partitions at the source.
- Deduplicate with stable ordering.
- Do not hide duplicates with `DISTINCT`.
- Keep output partition columns explicit.
- Use dynamic partition overwrite only for touched partitions.

## Mart Build Pattern

Build consumption-ready aggregates from refined tables.

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.country_daily_revenue
PARTITION (order_date)
SELECT
    country,
    SUM(CASE WHEN is_refund = false THEN amount ELSE CAST(0 AS DECIMAL(18, 2)) END) AS gross_revenue,
    SUM(CASE WHEN is_refund = true THEN amount ELSE CAST(0 AS DECIMAL(18, 2)) END) AS refund_amount,
    SUM(amount) AS net_revenue,
    COUNT(*) AS order_count,
    CURRENT_TIMESTAMP() AS processed_at,
    order_date
FROM refined.orders_clean
WHERE order_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY order_date, country;
```

Rules:

- Keep mart grain explicit.
- Aggregate by the minimal key set.
- Write only affected partitions.
- Validate source-to-target reconciliation.
- Avoid expensive `COUNT(DISTINCT)` unless the metric requires exact distinct counts.

## Incremental Processing

Default daily processing should be incremental.

Pattern:

- Determine processing date or date range.
- Read only required partitions.
- Recompute a rolling window when late data is expected.
- Overwrite only touched partitions.
- Validate touched partitions.
- Record job metadata: range, input counts, output counts, and status.

Rolling window example:

```sql
WHERE order_date BETWEEN DATE '${process_date}' - INTERVAL 6 DAYS
                     AND DATE '${process_date}'
```

Use rolling windows when:

- Source data arrives late.
- Upstream systems correct previous days.
- Deduplication depends on records from a recent range.
- Business metrics can change after the original event date.

Do not use rolling recompute as an excuse to scan the full table.

## Backfill Strategy

Backfills must be planned as bounded production changes.

Rules:

- Use the same SQL logic as daily processing when possible.
- Process explicit date ranges.
- Split large ranges into batches.
- Avoid concurrent writes to the same partitions.
- Validate each batch.
- Track completed ranges in orchestration metadata.
- Keep old data recoverable until validation completes.

Backfill example:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.country_daily_revenue
PARTITION (order_date)
SELECT
    country,
    SUM(CASE WHEN is_refund = false THEN amount ELSE CAST(0 AS DECIMAL(18, 2)) END) AS gross_revenue,
    SUM(CASE WHEN is_refund = true THEN amount ELSE CAST(0 AS DECIMAL(18, 2)) END) AS refund_amount,
    SUM(amount) AS net_revenue,
    COUNT(*) AS order_count,
    CURRENT_TIMESTAMP() AS processed_at,
    order_date
FROM refined.orders_clean
WHERE order_date BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
GROUP BY order_date, country;
```

After each backfill batch:

```sql
SELECT
    order_date,
    COUNT(*) AS rows,
    SUM(net_revenue) AS net_revenue
FROM mart.country_daily_revenue
WHERE order_date BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
GROUP BY order_date
ORDER BY order_date;
```

## Safe Write Policy

Before every production write, verify:

- Target table exists.
- Target table is external or managed as expected.
- Target location is correct.
- Source range is bounded.
- Output schema matches table schema.
- Partition columns are projected correctly.
- Write mode is explicit.
- Overwrite scope is intentional.
- Validation queries are ready.
- Rollback path exists.

Dynamic partition overwrite:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;
```

Use only when the result contains exactly the partitions intended for replacement.

Static partition overwrite:

```sql
INSERT OVERWRITE TABLE mart.country_daily_revenue
PARTITION (order_date = DATE '2026-05-05')
SELECT
    country,
    SUM(CASE WHEN is_refund = false THEN amount ELSE CAST(0 AS DECIMAL(18, 2)) END) AS gross_revenue,
    SUM(CASE WHEN is_refund = true THEN amount ELSE CAST(0 AS DECIMAL(18, 2)) END) AS refund_amount,
    SUM(amount) AS net_revenue,
    COUNT(*) AS order_count,
    CURRENT_TIMESTAMP() AS processed_at
FROM refined.orders_clean
WHERE order_date = DATE '2026-05-05'
GROUP BY country;
```

Agent rule: do not recommend full table overwrite for daily or rolling jobs.

## Staged Write Pattern

Use staged writes for high-risk replacements.

Workflow:

1. Write output into a staging table or staging path.
2. Validate schema, counts, metrics, and file layout.
3. Swap metadata or replace partitions in a controlled way.
4. Refresh table metadata.
5. Keep previous version until validation passes.

Staging table:

```sql
CREATE TABLE IF NOT EXISTS tmp.country_daily_revenue_stage (
    country STRING,
    gross_revenue DECIMAL(18, 2),
    refund_amount DECIMAL(18, 2),
    net_revenue DECIMAL(18, 2),
    order_count BIGINT,
    processed_at TIMESTAMP
)
PARTITIONED BY (order_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/tmp/country_daily_revenue_stage';
```

Load stage:

```sql
INSERT OVERWRITE TABLE tmp.country_daily_revenue_stage
PARTITION (order_date)
SELECT
    country,
    gross_revenue,
    refund_amount,
    net_revenue,
    order_count,
    processed_at,
    order_date
FROM mart.country_daily_revenue_candidate;
```

Staged replacement is especially useful for:

- Full historical rebuilds.
- Schema migrations.
- Major metric logic changes.
- Recovery from corrupted partitions.

## Partition Metadata Management

If Spark writes through Hive tables, partition metadata is usually updated automatically.

If files are written outside Hive table APIs, register partitions:

```sql
ALTER TABLE raw.orders ADD IF NOT EXISTS
    PARTITION (order_date = '2026-05-05', source_system = 'orders_api')
    LOCATION 'hdfs:///data/lake/raw/orders/order_date=2026-05-05/source_system=orders_api';
```

Bulk repair:

```sql
MSCK REPAIR TABLE raw.orders;
REFRESH TABLE raw.orders;
```

Rules:

- Prefer targeted `ALTER TABLE ADD PARTITION` for known partition ranges.
- Use `MSCK REPAIR TABLE` for recovery or bulk discovery, not as the default daily pattern for huge tables.
- Run `REFRESH TABLE` when the current Spark session may have stale metadata.
- Inspect partition locations when data and metadata disagree.

Drop metadata only:

```sql
ALTER TABLE raw.orders DROP IF EXISTS
    PARTITION (order_date = '2026-05-05', source_system = 'orders_api');
```

Do not confuse dropping partition metadata with deleting HDFS files.

## HDFS Operations

Use HDFS commands for inspection, not as the primary data pipeline API.

Inspect table root:

```bash
hdfs dfs -ls /data/lake/raw/orders
hdfs dfs -du -s -h /data/lake/raw/orders
hdfs dfs -count -h /data/lake/raw/orders
```

Inspect partition:

```bash
hdfs dfs -ls /data/lake/raw/orders/order_date=2026-05-05/source_system=orders_api
hdfs dfs -du -s -h /data/lake/raw/orders/order_date=2026-05-05/source_system=orders_api
hdfs dfs -count -h /data/lake/raw/orders/order_date=2026-05-05/source_system=orders_api
```

Inspect permissions:

```bash
hdfs dfs -ls -d /data/lake/raw/orders
hdfs dfs -getfacl /data/lake/raw/orders
```

Rules:

- Do not manually delete production paths unless deletion is explicitly requested and validated.
- Do not move partition directories as a normal correction pattern.
- Prefer rewriting partitions through Spark SQL.
- Use HDFS snapshots or backup restore when recovering deleted data.

## File Size and Compaction

Small files are a major data lake failure mode.

Target file sizes:

- 128-512 MB for most Parquet analytical tables.
- 512 MB-1 GB for very large sequential fact scans if the platform supports it.
- Smaller only for low-volume partitions where overhead is acceptable.

Diagnose small files:

```bash
hdfs dfs -count -h /data/lake/mart/country_daily_revenue/order_date=2026-05-05
hdfs dfs -du -h /data/lake/mart/country_daily_revenue/order_date=2026-05-05 | head
```

Compaction by partition:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;
SET spark.sql.shuffle.partitions = 200;

INSERT OVERWRITE TABLE mart.country_daily_revenue
PARTITION (order_date)
SELECT
    country,
    gross_revenue,
    refund_amount,
    net_revenue,
    order_count,
    processed_at,
    order_date
FROM mart.country_daily_revenue
WHERE order_date = DATE '2026-05-05';
```

Rules:

- Compact affected partitions, not the entire table.
- Do not force one file for large partitions.
- Adjust shuffle partitions based on output size.
- Schedule compaction for streaming or micro-batch outputs.
- Monitor file count per partition.

## Statistics and Optimizer Metadata

Maintain statistics for important tables:

```sql
ANALYZE TABLE raw.orders COMPUTE STATISTICS;
ANALYZE TABLE raw.orders COMPUTE STATISTICS FOR COLUMNS order_date, user_id, country;
```

Use stats for:

- Join planning.
- Broadcast decisions.
- Cost-based optimization.
- Understanding table size.

Refresh stats after:

- Large backfills.
- Major partition overwrites.
- Compaction.
- Schema changes.
- Data distribution changes.

Inspect:

```sql
DESCRIBE EXTENDED raw.orders;
DESCRIBE EXTENDED raw.orders user_id;
```

Agent rule: do not trust `EXPLAIN COST` when statistics are absent or stale.

## Query Design for the Lake

Always preserve partition pruning:

```sql
SELECT
    order_date,
    country,
    SUM(amount) AS revenue
FROM refined.orders_clean
WHERE order_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY order_date, country;
```

Avoid hiding partition columns:

```sql
WHERE TO_DATE(order_time) = DATE '2026-05-05'
```

Use both partition and timestamp filters when needed:

```sql
WHERE order_date = DATE '2026-05-05'
  AND order_time >= TIMESTAMP '2026-05-05 00:00:00'
  AND order_time <  TIMESTAMP '2026-05-06 00:00:00'
```

Avoid global sorts on large lake tables:

```sql
ORDER BY order_time
```

Use bounded diagnostics:

```sql
SELECT *
FROM refined.orders_clean
WHERE order_date = DATE '2026-05-05'
ORDER BY order_time
LIMIT 100;
```

## Join Design

For large HDFS-backed tables:

- Filter both sides by partitions.
- Project only needed columns.
- Verify join key types.
- Verify key uniqueness where required.
- Use `LEFT SEMI JOIN` for existence.
- Use `LEFT ANTI JOIN` for exclusion.
- Broadcast only genuinely small dimensions.
- Check skew before large joins.

Fact to dimension:

```sql
SELECT /*+ BROADCAST(u) */
    o.order_date,
    o.order_id,
    o.user_id,
    u.country,
    o.amount
FROM refined.orders_clean o
JOIN refined.users_current u
    ON o.user_id = u.user_id
WHERE o.order_date = DATE '2026-05-05';
```

Existence filter:

```sql
SELECT o.*
FROM refined.orders_clean o
LEFT SEMI JOIN mart.active_users u
    ON o.user_id = u.user_id
WHERE o.order_date = DATE '2026-05-05';
```

Exclusion filter:

```sql
SELECT o.*
FROM refined.orders_clean o
LEFT ANTI JOIN mart.blocked_users b
    ON o.user_id = b.user_id
WHERE o.order_date = DATE '2026-05-05';
```

## Data Quality Gates

Every production layer should have quality gates.

Raw layer checks:

- Parsed row count vs landing row count.
- Invalid row count.
- Required fields present.
- Partition values valid.
- Source lineage fields populated.

Refined layer checks:

- Duplicate business keys.
- Null key rates.
- Referential coverage.
- Deduplication winner logic.
- Timestamp and date consistency.

Mart layer checks:

- Row counts by partition.
- Metric reconciliation.
- Duplicate grain checks.
- Null dimension rates.
- Schema conformance.

Examples:

```sql
SELECT
    order_date,
    COUNT(*) AS rows,
    COUNT(*) FILTER (WHERE order_id IS NULL) AS null_order_ids,
    COUNT(*) FILTER (WHERE user_id IS NULL) AS null_user_ids
FROM raw.orders
WHERE order_date = DATE '2026-05-05'
GROUP BY order_date;
```

Duplicate grain check:

```sql
SELECT
    order_date,
    country,
    COUNT(*) AS rows
FROM mart.country_daily_revenue
WHERE order_date = DATE '2026-05-05'
GROUP BY order_date, country
HAVING COUNT(*) > 1;
```

Reconciliation:

```sql
WITH source AS (
    SELECT order_date, SUM(amount) AS source_net_revenue
    FROM refined.orders_clean
    WHERE order_date = DATE '2026-05-05'
    GROUP BY order_date
),
target AS (
    SELECT order_date, SUM(net_revenue) AS target_net_revenue
    FROM mart.country_daily_revenue
    WHERE order_date = DATE '2026-05-05'
    GROUP BY order_date
)
SELECT
    s.order_date,
    s.source_net_revenue,
    t.target_net_revenue,
    s.source_net_revenue - t.target_net_revenue AS diff
FROM source s
JOIN target t
    ON s.order_date = t.order_date;
```

## Schema Evolution Policy

Schema changes must be governed.

Safe changes:

- Add nullable columns.
- Add derived columns to marts with downstream approval.
- Widen compatible numeric types when readers support it.

Risky changes:

- Rename columns.
- Drop columns.
- Change partition columns.
- Change physical type across existing Parquet partitions.
- Convert `STRING` to `TIMESTAMP` without validation.
- Change decimal precision/scale without downstream checks.

Add column:

```sql
ALTER TABLE mart.country_daily_revenue ADD COLUMNS (
    average_order_value DECIMAL(18, 2)
);
```

After schema change:

```sql
REFRESH TABLE mart.country_daily_revenue;
DESCRIBE FORMATTED mart.country_daily_revenue;
SELECT *
FROM mart.country_daily_revenue
WHERE order_date = DATE '2026-05-05'
LIMIT 10;
```

Agent rule: never assume Hive Parquet schema evolution is safe across all downstream engines.

## Security and Governance

HDFS data lake security must be designed from the start.

Controls:

- Kerberos or platform authentication.
- HDFS ACLs by lake zone and table path.
- Ranger/Sentry or equivalent SQL authorization.
- Database and table ownership.
- PII classification and masking policy.
- Audit logs for reads and writes.
- Separate service users for production jobs.
- No secrets in SQL text, logs, or table properties.

HDFS ACL inspection:

```bash
hdfs dfs -getfacl /data/lake/raw/orders
hdfs dfs -getfacl /data/lake/mart/country_daily_revenue
```

Rules:

- Raw PII should not be copied into marts unless needed.
- Quarantine data may contain sensitive raw values and needs restricted access.
- Sandbox data must not become a production dependency.
- Temporary paths need retention cleanup.

## Retention and Lifecycle

Define retention by zone:

- Landing: retained for replay according to source and compliance policy.
- Raw: retained long enough for reprocessing and audit.
- Refined: retained according to business history needs.
- Mart: retained according to consumer and cost requirements.
- Quarantine: retained according to debugging, compliance, and privacy requirements.
- Tmp: short TTL, usually days.
- Sandbox: short TTL or owner-managed.

Do not delete HDFS data manually without:

- Owner approval.
- Partition/path verification.
- Backup or snapshot awareness.
- Downstream impact review.
- Metadata cleanup plan.

For external tables, metadata deletion and physical deletion are separate operations.

## Observability

Every production pipeline should record:

- Job name.
- Batch id.
- Processing range.
- Source table and target table.
- Input row counts by partition.
- Output row counts by partition.
- Invalid/quarantine counts.
- Runtime and Spark application id.
- HDFS output path.
- Touched partitions.
- Validation status.

Recommended operational table:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS mart.pipeline_run_audit (
    job_name STRING,
    batch_id STRING,
    source_table STRING,
    target_table STRING,
    process_start_date DATE,
    process_end_date DATE,
    input_rows BIGINT,
    output_rows BIGINT,
    invalid_rows BIGINT,
    spark_application_id STRING,
    status STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
)
PARTITIONED BY (run_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/mart/pipeline_run_audit';
```

## EXPLAIN and Performance Review

Use `EXPLAIN FORMATTED` before expensive jobs:

```sql
EXPLAIN FORMATTED
SELECT
    order_date,
    country,
    SUM(amount) AS revenue
FROM refined.orders_clean
WHERE order_date = DATE '2026-05-05'
GROUP BY order_date, country;
```

Check:

- `FileScan parquet`
- `PartitionFilters`
- `PushedFilters`
- `ReadSchema`
- `Exchange`
- `BroadcastHashJoin`
- `SortMergeJoin`
- `HashAggregate`
- `Window`
- Unexpected `CartesianProduct`

Healthy scan:

```text
PartitionFilters: [isnotnull(order_date), (order_date = 2026-05-05)]
ReadSchema: struct<country:string,amount:decimal(18,2)>
```

Unhealthy scan:

```text
PartitionFilters: []
ReadSchema: struct<all columns...>
```

If a large table scan lacks partition filters, fix the SQL before tuning cluster resources.

## Disaster Recovery

Plan recovery for:

- Bad overwrite.
- Missing partitions.
- Corrupted Parquet files.
- Stale metastore metadata.
- Accidental HDFS deletion.
- Schema mismatch.
- Failed backfill.

Recovery tools:

- HDFS snapshots.
- Table backups.
- Reprocessing from landing or raw.
- Re-registering partitions.
- Dropping bad metadata.
- Rewriting affected partitions.
- Restoring previous mart partitions.

Missing partition metadata:

```sql
ALTER TABLE raw.orders ADD IF NOT EXISTS
    PARTITION (order_date = '2026-05-05', source_system = 'orders_api')
    LOCATION 'hdfs:///data/lake/raw/orders/order_date=2026-05-05/source_system=orders_api';

REFRESH TABLE raw.orders;
```

Bad partition rewrite:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.country_daily_revenue
PARTITION (order_date)
SELECT
    country,
    gross_revenue,
    refund_amount,
    net_revenue,
    order_count,
    CURRENT_TIMESTAMP() AS processed_at,
    order_date
FROM tmp.country_daily_revenue_recovered
WHERE order_date = DATE '2026-05-05';
```

## Environment Separation

Separate development, staging, and production:

```text
hdfs:///data/dev/lake/
hdfs:///data/stage/lake/
hdfs:///data/prod/lake/
```

or by database:

```sql
CREATE DATABASE raw_dev LOCATION 'hdfs:///data/dev/lake/raw';
CREATE DATABASE raw_stage LOCATION 'hdfs:///data/stage/lake/raw';
CREATE DATABASE raw_prod LOCATION 'hdfs:///data/prod/lake/raw';
```

Rules:

- Do not point dev tables at prod HDFS paths.
- Do not write staging output into production table roots.
- Keep service accounts and ACLs environment-specific.
- Use production-like partitioning and schema in staging.

## Common Anti-Patterns

Avoid:

- One HDFS directory containing many unrelated datasets.
- Production tables without Hive metadata.
- Hive tables without explicit `LOCATION`.
- Full scans for daily pipelines.
- Full table overwrite for incremental pipelines.
- `SELECT *` in production writes.
- Partitioning by high-cardinality identifiers.
- Millions of tiny Parquet files.
- Mixed file formats in one table root.
- Manual HDFS deletes as part of normal job logic.
- Running `MSCK REPAIR TABLE` over massive tables as a daily default.
- Storing money in `DOUBLE`.
- Treating landing CSV/JSON as the curated analytical layer.
- Changing schema without downstream review.
- Ignoring HDFS ACLs and effective job user.
- Trusting successful Spark job status without data validation.

## Build Checklist

Before creating a new lake table:

- Choose the lake layer.
- Choose database and table name.
- Define owner and consumers.
- Define HDFS root.
- Choose external vs managed table.
- Define schema and data types.
- Define partition columns.
- Define retention.
- Define quality checks.
- Define write mode.
- Define backfill strategy.
- Define recovery path.
- Define permissions.
- Define file size targets.
- Define statistics refresh policy.

## Review Checklist

When reviewing an HDFS/Hive/Parquet data lake design:

- Are layers clearly separated?
- Are table roots stable and explicit?
- Are tables external or managed intentionally?
- Is Parquet used for curated layers?
- Are partition columns aligned with query patterns?
- Are partition cardinalities manageable?
- Is incremental processing bounded?
- Are overwrite semantics safe?
- Are metadata repair and refresh procedures defined?
- Are file sizes and compaction addressed?
- Are schema evolution rules defined?
- Are data quality gates present?
- Are HDFS ACLs and table grants addressed?
- Is lineage captured?
- Is recovery possible after bad writes?
- Are downstream consumers protected from breaking changes?

## Enterprise Defaults

When context is missing, use these defaults:

- Use external Hive tables over explicit HDFS locations.
- Use Parquet with Snappy compression.
- Partition large event/fact tables by date.
- Keep landing immutable.
- Convert landing data to typed raw Parquet.
- Build refined and mart layers through Spark SQL.
- Process bounded partition ranges.
- Use dynamic partition overwrite only for carefully filtered results.
- Validate after every write.
- Prefer targeted partition registration over whole-table repair.
- Avoid high-cardinality partitions and small files.
- Maintain table and column statistics for important tables.
- Treat HDFS deletion as a separate, explicit operational action.

