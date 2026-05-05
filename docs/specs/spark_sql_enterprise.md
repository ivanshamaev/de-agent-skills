# Spark SQL Enterprise Specification

This specification is for agents that write, review, debug, and optimize Spark SQL in production data platforms: Hive Metastore, HDFS, S3/ADLS/GCS, Parquet/ORC, Delta Lake, Iceberg, Hudi, and lakehouse tables. Use it for TB-PB scale pipelines where a bad query can waste cluster hours, miss an SLA, corrupt partitions, or publish incorrect marts.

## Agent Contract

When working with Spark SQL, the agent must think like a production data engineer:

- First clarify data semantics, volume, SLA, table format, catalog, partitions, keys, uniqueness, watermark, late data, write mode, and downstream consumers.
- Do not suggest quick SQL hacks that hide data problems: `DISTINCT` after a join explosion, random `LIMIT`, broad `INSERT OVERWRITE`, unbounded `ORDER BY`, or Python UDFs instead of SQL built-ins.
- Write SQL that can be maintained: explicit CTEs, explicit columns, explicit join types, explicit partition filters, and explicit deduplication.
- For expensive queries, always reason about scan volume, pruning, shuffle, skew, join strategy, file layout, metadata, and statistics.
- For production writes, always reason about idempotency, overwrite scope, atomicity, retry safety, schema evolution, and post-write validation.
- If important context is missing, state assumptions explicitly. Do not pretend that partitioning, key uniqueness, or table format are known.

## Production Context to Collect

Before designing or optimizing a query, collect at least:

- Spark version and whether Adaptive Query Execution is enabled.
- Table format: Hive Parquet/ORC, Delta, Iceberg, Hudi, or direct external path.
- Catalog/metastore: Hive Metastore, Glue, Unity Catalog, Nessie, or another catalog.
- Storage: HDFS, S3, ADLS, or GCS. Object storage changes the cost of listing, renames, and commit protocols.
- Data volume: rows, bytes, daily increment, and largest partitions.
- Partition columns and types: date, hour, country, business domain, and similar.
- Bucketing, clustering, Z-order, or sort order, if present.
- Primary/business keys, uniqueness guarantees, and null behavior.
- Expected output grain: exactly what one output row represents.
- Incremental semantics: full rebuild, rolling window, late arriving data, CDC, or SCD.
- Write mode: append, insert overwrite partition, merge/upsert, or replace table.
- Consumer contract: schema, partitions, SLA, freshness, and backfill expectations.

## Default Spark SQL Settings

Do not copy settings blindly. Recommend them as a starting point that depends on the cluster and data volume.

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.sources.partitionOverwriteMode = dynamic;
SET spark.sql.storeAssignmentPolicy = ANSI;
```

For heavy shuffles:

```sql
-- Target roughly 128-512 MB of data per post-shuffle partition.
SET spark.sql.shuffle.partitions = 4000;
```

For broadcast joins:

```sql
-- Use only with verified statistics and executor memory headroom.
SET spark.sql.autoBroadcastJoinThreshold = 52428800;

-- Disable when stale statistics cause dangerous broadcasts.
SET spark.sql.autoBroadcastJoinThreshold = -1;
```

For dynamic partition writes:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;
```

Dynamic overwrite is safe only when the `SELECT` projects partition columns correctly and the query is bounded to the intended partition range.

## Query Shape

Production SQL must be readable and verifiable:

- Use CTEs for data stages: `source_filtered`, `deduped`, `aggregated`, `enriched`, `final`.
- Do not use `SELECT *` in production queries, except for short interactive diagnostics.
- Every derived field must have an explicit alias.
- Qualify all columns with table aliases when more than one relation is present.
- Use typed literals: `DATE '2026-05-05'`, `TIMESTAMP '2026-05-05 00:00:00'`, `DECIMAL(18,2)`.
- Do not mix cleaning, joining, aggregation, and final formatting in one huge `SELECT`.
- Keep the grain of every CTE clear. If a CTE changes grain, its name and `GROUP BY` or window should make that obvious.

Good skeleton:

```sql
WITH source_filtered AS (
    SELECT
        event_date,
        user_id,
        event_id,
        event_time,
        event_type,
        amount
    FROM raw.events
    WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
      AND event_type IN ('purchase', 'refund')
),
deduped AS (
    SELECT
        event_date,
        user_id,
        event_id,
        event_time,
        event_type,
        amount
    FROM (
        SELECT
            sf.*,
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY event_time DESC NULLS LAST
            ) AS rn
        FROM source_filtered sf
    ) x
    WHERE rn = 1
),
daily_user_metrics AS (
    SELECT
        event_date,
        user_id,
        COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchase_cnt,
        SUM(amount) FILTER (WHERE event_type = 'purchase') AS gross_revenue,
        SUM(amount) FILTER (WHERE event_type = 'refund') AS refund_amount
    FROM deduped
    GROUP BY event_date, user_id
)
SELECT
    event_date,
    user_id,
    purchase_cnt,
    gross_revenue,
    refund_amount
FROM daily_user_metrics;
```

## Partition Pruning

At petabyte scale, missing partition pruning is usually a critical defect.

Rules:

- Filter partition columns directly in `WHERE`.
- Do not wrap partition columns in functions.
- Do not compare a partition column to an expression of another type without an explicit typed literal.
- If a table is partitioned by `event_date`, filtering on `event_time` does not replace filtering on `event_date`.
- For rolling windows, bound both the input range and the overwrite target to the same range.

Good:

```sql
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
```

Bad:

```sql
WHERE TO_DATE(event_time) BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
```

If timestamp logic is required:

```sql
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
  AND event_time >= TIMESTAMP '2026-05-01 00:00:00'
  AND event_time <  TIMESTAMP '2026-05-06 00:00:00'
```

## Projection and Predicate Pushdown

For columnar storage, cost is often driven by how many files and columns are read:

- Select only required columns in early CTEs.
- Push filters as early as semantics allow, especially before joins and aggregations.
- Avoid UDFs and complex expressions in `WHERE` when they block pushdown.
- Check `FileScan`, `ReadSchema`, `PartitionFilters`, and `PushedFilters` in `EXPLAIN FORMATTED`.
- For Parquet/ORC, prefer typed columns over JSON string blobs.

## Joins

Joins are the most common source of Spark SQL production incidents.

Mandatory rules:

- Always specify the join type: `INNER JOIN`, `LEFT JOIN`, `LEFT SEMI JOIN`, `LEFT ANTI JOIN`, `FULL OUTER JOIN`.
- Do not use `RIGHT JOIN`; swap table order and use `LEFT JOIN`.
- Before joining, verify the grain and key uniqueness of both sides.
- Filter and project large inputs before joining.
- If a dimension is not unique by key, deduplicate it deterministically before joining.
- Do not add `DISTINCT` after a join until you have explained why duplicates appeared.
- Use `LEFT SEMI JOIN` for existence checks.
- Use `LEFT ANTI JOIN` for exclusion checks instead of `NOT IN` when nulls are possible.
- For range joins and non-equi joins, evaluate cardinality explosion separately.
- Use null-safe equality `<=>` only when business semantics require `NULL = NULL`.

Deduplicating a dimension before join:

```sql
WITH users_one_row AS (
    SELECT
        user_id,
        country,
        segment
    FROM (
        SELECT
            u.*,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY updated_at DESC NULLS LAST, ingestion_ts DESC NULLS LAST
            ) AS rn
        FROM dwh.users_dim u
        WHERE is_deleted = false
    ) x
    WHERE rn = 1
)
SELECT
    e.event_date,
    e.user_id,
    u.country,
    e.amount
FROM raw.events e
LEFT JOIN users_one_row u
    ON e.user_id = u.user_id
WHERE e.event_date = DATE '2026-05-05';
```

Existence check:

```sql
SELECT
    e.event_id,
    e.user_id
FROM raw.events e
LEFT SEMI JOIN dwh.active_users u
    ON e.user_id = u.user_id
WHERE e.event_date = DATE '2026-05-05';
```

Exclusion:

```sql
SELECT
    e.event_id,
    e.user_id
FROM raw.events e
LEFT ANTI JOIN fraud.blocked_users b
    ON e.user_id = b.user_id
WHERE e.event_date = DATE '2026-05-05';
```

### Broadcast Joins

Broadcast only truly small relations:

- Check table statistics, physical file size, and cardinality.
- The broadcast side must fit in executor memory with headroom.
- Do not broadcast tables that may grow without a guardrail.
- Use hints only when the optimizer is wrong or statistics are incomplete.
- If broadcast causes OOM, disable auto broadcast and fix statistics.

```sql
SELECT /*+ BROADCAST(d) */
    f.event_date,
    f.user_id,
    d.country
FROM fact.events f
JOIN dim.users_current d
    ON f.user_id = d.user_id
WHERE f.event_date = DATE '2026-05-05';
```

### Sort-Merge Joins

Sort-merge join is normal for two large inputs, but expensive:

- Reduce both sides before the join.
- Align join key types.
- Check skew on join keys.
- Consider pre-aggregation.
- Treat bucketing/clustering as a platform-level design, not a local SQL trick.

### Skewed Joins

Signs of skew:

- A few tasks run much longer than the rest.
- In Spark UI, a few tasks read gigabytes of shuffle while most read megabytes.
- Top keys account for a huge share of rows.

Diagnostics:

```sql
SELECT
    join_key,
    COUNT(*) AS cnt
FROM large_table
WHERE event_date = DATE '2026-05-05'
GROUP BY join_key
ORDER BY cnt DESC
LIMIT 50;
```

Preferred fixes, in order:

- Filter unnecessary rows before the join.
- Aggregate the heavy side before the join if semantics allow it.
- Enable AQE skew join optimization.
- Split hot keys and normal keys into separate branches.
- Use salting only deliberately and document it.

Split hot keys pattern:

```sql
WITH hot_keys AS (
    SELECT join_key
    FROM large_table
    WHERE event_date = DATE '2026-05-05'
    GROUP BY join_key
    HAVING COUNT(*) > 100000000
),
normal_rows AS (
    SELECT l.*
    FROM large_table l
    LEFT ANTI JOIN hot_keys h
        ON l.join_key = h.join_key
    WHERE l.event_date = DATE '2026-05-05'
),
hot_rows AS (
    SELECT l.*
    FROM large_table l
    INNER JOIN hot_keys h
        ON l.join_key = h.join_key
    WHERE l.event_date = DATE '2026-05-05'
)
SELECT ...
FROM normal_rows n
JOIN dim_table d
    ON n.join_key = d.join_key
UNION ALL
SELECT ...
FROM hot_rows h
JOIN dim_table d
    ON h.join_key = d.join_key;
```

## Aggregations

Rules:

- Group by the minimal key set needed for the output grain.
- Aggregate before joining when it reduces data volume and preserves semantics.
- Use `COUNT(*)` for row counts.
- Use `COUNT(col)` only when intentionally excluding nulls.
- Use `FILTER (WHERE ...)` for conditional metrics.
- Use `GROUPING SETS`, `ROLLUP`, or `CUBE` instead of multiple full scans when several grains are needed.
- Do not perform a global `ORDER BY` before aggregation.
- Check for partial and final aggregate operators in the physical plan.

Conditional aggregation:

```sql
SELECT
    event_date,
    country,
    COUNT(*) AS events,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    SUM(amount) FILTER (WHERE event_type = 'purchase') AS revenue
FROM fact.events
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date, country;
```

Multiple grains in one pass:

```sql
SELECT
    event_date,
    country,
    device_type,
    SUM(amount) AS revenue,
    GROUPING_ID(event_date, country, device_type) AS grouping_id
FROM fact.events
WHERE event_date = DATE '2026-05-05'
GROUP BY GROUPING SETS (
    (event_date, country, device_type),
    (event_date, country),
    (event_date)
);
```

## Windows

Window functions often cause shuffle and sort.

Rules:

- Always specify `PARTITION BY`, except for genuinely small global data.
- Always specify deterministic `ORDER BY` for `ROW_NUMBER`, `RANK`, `FIRST_VALUE`, and `LAST_VALUE`.
- Add a tie-breaker: ingestion timestamp, version, event id, or another stable field.
- Explicitly specify frames for running totals and analytic metrics.
- Do not run windows over raw petabyte data when you can aggregate earlier.
- Do not use `ORDER BY rand()` on large data.
- Reuse identical window specifications so Spark can reuse sort/shuffle work.

Deterministic latest row:

```sql
SELECT
    user_id,
    status,
    effective_from
FROM (
    SELECT
        user_id,
        status,
        effective_from,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY effective_from DESC NULLS LAST, ingestion_ts DESC NULLS LAST
        ) AS rn
    FROM dwh.user_status_history
    WHERE snapshot_date <= DATE '2026-05-05'
) x
WHERE rn = 1;
```

Running metric:

```sql
SUM(amount) OVER (
    PARTITION BY user_id
    ORDER BY event_time ASC NULLS LAST, event_id ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS running_amount
```

## Transformations

### Schema Contracts

Every production transformation must have an input and output contract:

- Explicit column list.
- Explicit types for money, IDs, timestamps, and dates.
- Explicit nullability semantics.
- Explicit timezone semantics for timestamps.
- Explicit behavior for dirty values.

Final contract example:

```sql
SELECT
    CAST(event_date AS DATE) AS event_date,
    CAST(user_id AS STRING) AS user_id,
    CAST(country AS STRING) AS country,
    CAST(revenue AS DECIMAL(18, 2)) AS revenue,
    CAST(processed_at AS TIMESTAMP) AS processed_at
FROM final_metrics;
```

### Dirty Data

Rules:

- Use `TRY_CAST` when dirty values are expected and should become null or be quarantined.
- Use strict `CAST` when dirty values should fail the pipeline.
- Create a quarantine dataset for bad rows when data loss is not acceptable.
- Do not replace null with `UNKNOWN` without downstream semantics.
- Do not change timezones implicitly.

```sql
WITH parsed AS (
    SELECT
        raw_id,
        TRY_CAST(raw_amount AS DECIMAL(18, 2)) AS amount,
        TRY_CAST(raw_event_time AS TIMESTAMP) AS event_time
    FROM landing.events_raw
),
valid AS (
    SELECT *
    FROM parsed
    WHERE amount IS NOT NULL
      AND event_time IS NOT NULL
),
invalid AS (
    SELECT *
    FROM parsed
    WHERE amount IS NULL
       OR event_time IS NULL
)
SELECT * FROM valid;
```

### Deduplication

Deduplication must be deterministic:

- Specify the business key.
- Specify winner ordering.
- Specify why losing records are discarded.
- Do not use plain `DISTINCT` or nondeterministic deduplication when you need the latest or authoritative record.

```sql
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY business_id
            ORDER BY source_priority ASC, updated_at DESC NULLS LAST, ingestion_ts DESC NULLS LAST
        ) AS rn
    FROM staging.entity_updates
    WHERE ingestion_date = DATE '2026-05-05'
)
SELECT
    business_id,
    attr_1,
    attr_2,
    updated_at
FROM ranked
WHERE rn = 1;
```

### Slowly Changing Dimensions

For SCD2:

- Verify non-overlapping intervals.
- Use half-open intervals: `[valid_from, valid_to)`.
- Join facts to dimensions by business key and event timestamp/date.
- Do not use `BETWEEN` for inclusive ends when `valid_to` is the boundary of the next version.

```sql
SELECT
    f.event_id,
    f.user_id,
    d.segment
FROM fact.events f
LEFT JOIN dim.user_scd2 d
    ON f.user_id = d.user_id
   AND f.event_time >= d.valid_from
   AND f.event_time <  d.valid_to
WHERE f.event_date = DATE '2026-05-05';
```

## Writes and Overwrites

Production writes are the highest-risk part of Spark SQL.

Before writing, the agent must verify:

- Target table exists and its schema is expected.
- Partition columns are present in the output.
- Write mode matches the task.
- Overwrite is bounded to the intended range.
- Query is idempotent on retry.
- File count and target file size will not create a small-file incident.
- Post-write validation exists.
- Rollback or atomic table-format operation exists.

Partition-scoped overwrite:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.daily_revenue
PARTITION (event_date)
SELECT
    country,
    SUM(amount) AS revenue,
    event_date
FROM fact.events
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date, country;
```

Dangerous:

```sql
INSERT OVERWRITE TABLE mart.daily_revenue
SELECT ...
FROM fact.events;
```

If the table format supports atomic operations, prefer:

- Delta: `MERGE INTO`, `REPLACE WHERE`, transaction log.
- Iceberg: `MERGE INTO`, partition overwrite, snapshot rollback.
- Hudi: upsert with record key and precombine key.

For Hive-style tables without atomic guarantees, use staged writes:

1. Write to a staging path or table.
2. Validate counts, schema, and partitions.
3. Atomically switch metadata if the platform supports it.
4. Clean old data only after confirmation.

## File Layout

At PB scale, file layout matters as much as SQL.

Rules:

- Use Parquet/ORC/Delta/Iceberg/Hudi for analytics.
- Avoid CSV/JSON in core mart and fact layers.
- Target file size is usually 128-512 MB, sometimes 1 GB for very large sequential scans.
- Avoid millions of small files.
- Choose partition columns based on common filters and manageable cardinality.
- Do not partition by `user_id`, `request_id`, `uuid`, or other high-cardinality fields.
- On object storage, account for expensive listing and do not assume rename is cheap.
- For Delta/Iceberg/Hudi, use compaction, optimize, vacuum, and snapshot expiration according to platform policy.

Before writing, estimate output cardinality:

```sql
SELECT
    event_date,
    country,
    COUNT(*) AS rows
FROM final_dataset
GROUP BY event_date, country
ORDER BY rows DESC;
```

## Statistics and Metadata

The optimizer depends on metadata:

- Refresh table statistics for large managed tables.
- Refresh column statistics for join and filter keys.
- Check for stale statistics after large backfills or compaction.
- For Hive external partitions, synchronize the metastore with physical layout.
- Do not trust the plan if statistics are missing or clearly wrong.

```sql
ANALYZE TABLE fact.events COMPUTE STATISTICS;
ANALYZE TABLE fact.events COMPUTE STATISTICS FOR COLUMNS event_date, user_id, country;

DESCRIBE FORMATTED fact.events;
SHOW PARTITIONS fact.events;
SHOW TBLPROPERTIES fact.events;
```

## EXPLAIN and Plan Review

Before running an expensive query:

```sql
EXPLAIN FORMATTED
SELECT ...;

EXPLAIN COST
SELECT ...;
```

In the plan, look for:

- `FileScan`: which tables are read, and whether `PartitionFilters`, `PushedFilters`, and `ReadSchema` are present.
- `Exchange`: shuffle boundary; check keys and partition count.
- `BroadcastHashJoin`: verify the broadcast side is small.
- `SortMergeJoin`: verify that a large-large join is expected.
- `BroadcastNestedLoopJoin`: almost always an alarm.
- `CartesianProduct`: allowed only for explicitly bounded tiny inputs.
- `HashAggregate`: check for partial and final aggregation.
- `Window`: check for large sort/shuffle.
- `Sort`: check for unnecessary global sort.
- `CollectLimit`: verify it is not masking a full upstream scan.

If `EXPLAIN` shows a full scan of a large table without a partition filter, stop and fix the query.

## Shuffle Management

Shuffle is expensive but unavoidable for joins, groups, and windows.

Rules:

- Reduce data before shuffle: filter, projection, pre-aggregation.
- Set `spark.sql.shuffle.partitions` from post-shuffle data size, not habit.
- With AQE, set partitions with headroom and let coalescing reduce tiny partitions.
- Do not use `ORDER BY` for distributed results when `SORT BY` or downstream-local sorting is enough.
- Use `DISTRIBUTE BY` and `CLUSTER BY` only when you understand the downstream layout.
- Do not force one output file for large data.

Guidelines:

- 128 MB per shuffle partition: more parallelism and lower memory pressure.
- 256-512 MB per shuffle partition: less overhead, but requires stable memory.
- Too many tiny partitions create scheduler overhead.
- Too few huge partitions create spills and stragglers.

## Caching and Materialization

Cache is not a universal optimization.

Use cache or a materialized temp table only when:

- The same expensive subquery is reused multiple times.
- The data fits in cluster memory/disk with other workloads considered.
- Cache is explicitly cleared after use.

Do not cache:

- One-off CTEs.
- Raw PB-scale tables.
- High-churn data where stale cache is dangerous.

For expensive reusable stages, a curated intermediate partitioned table is often better than ephemeral cache.

## Incremental Processing

Enterprise pipelines should usually be incremental:

- Define watermark and late arrival policy.
- Process a bounded partition range.
- Overwrite only touched partitions.
- For CDC, use deterministic merge keys and ordering columns.
- For backfills, use the same code path as the daily run when possible.
- Log processed range, input counts, output counts, and bad records.

Rolling recompute pattern:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.user_daily_metrics
PARTITION (event_date)
SELECT
    user_id,
    COUNT(*) AS events,
    SUM(amount) AS revenue,
    event_date
FROM fact.events
WHERE event_date BETWEEN DATE '2026-04-29' AND DATE '2026-05-05'
GROUP BY event_date, user_id;
```

## Data Quality Checks

After transform/write, check:

- Row counts by partition.
- Null rates for key fields.
- Duplicate counts by business key.
- Referential coverage after joins.
- Metric reconciliation with source.
- Min/max dates and timestamps.
- Schema and type conformance.
- Distribution drift for important dimensions.

Templates:

```sql
SELECT
    event_date,
    COUNT(*) AS rows,
    COUNT(DISTINCT user_id) AS users,
    COUNT(*) FILTER (WHERE user_id IS NULL) AS null_user_id_rows
FROM mart.user_daily_metrics
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date
ORDER BY event_date;
```

Duplicate check:

```sql
SELECT
    event_date,
    user_id,
    COUNT(*) AS cnt
FROM mart.user_daily_metrics
WHERE event_date = DATE '2026-05-05'
GROUP BY event_date, user_id
HAVING COUNT(*) > 1
LIMIT 100;
```

Join coverage:

```sql
SELECT
    COUNT(*) AS rows,
    COUNT(*) FILTER (WHERE d.user_id IS NULL) AS unmatched_rows
FROM fact.events f
LEFT JOIN dim.users_current d
    ON f.user_id = d.user_id
WHERE f.event_date = DATE '2026-05-05';
```

## Null Semantics

Spark SQL null behavior must be explicit:

- `NULL = NULL` returns unknown, not true.
- Use `<=>` for null-safe equality.
- `NOT IN` with nulls in the subquery can produce surprising results; prefer `LEFT ANTI JOIN`.
- `COUNT(col)` does not count nulls.
- `SUM` over all-null rows returns null; use `COALESCE` if you need zero.
- Specify `NULLS FIRST` or `NULLS LAST` in window sorts.

```sql
COALESCE(SUM(amount), CAST(0 AS DECIMAL(18, 2))) AS revenue
```

## Time and Timezones

Rules:

- Store event time and processing time as separate fields.
- Do not derive `event_date` from timestamp without explicit timezone semantics.
- Use half-open intervals for timestamp filters.
- Do not use `BETWEEN` for timestamp day windows when the upper bound should be exclusive.
- Document session timezone when results depend on it.

```sql
WHERE event_time >= TIMESTAMP '2026-05-05 00:00:00'
  AND event_time <  TIMESTAMP '2026-05-06 00:00:00'
```

## Security and Governance

Enterprise SQL must account for governance:

- Do not expose PII without a clear need.
- Do not broadly copy raw PII into mart layers.
- Apply masking or tokenization policies when the platform provides them.
- Check owner, location, and grants before creating a production table.
- Do not use personal scratch paths for production outputs.
- Use TTL or cleanup policy for temporary debug tables.

## Anti-Patterns

Forbidden or suspicious patterns:

- `SELECT *` in production mart/fact writes.
- `DISTINCT` after joins without investigating cardinality.
- `ORDER BY` at PB scale without `LIMIT` or a downstream requirement.
- `COUNT(DISTINCT high_cardinality_key)` on huge data without cost evaluation.
- Cartesian joins without explicitly tiny bounded inputs.
- Broadcast hints on tables of unknown size.
- Function wrappers around partition columns in `WHERE`.
- Full-table `INSERT OVERWRITE` instead of partition-scoped overwrite.
- `NOT IN` when nulls are possible.
- Window without `PARTITION BY` on large data.
- Nondeterministic `ROW_NUMBER`.
- UDFs for logic expressible with built-in SQL functions.
- Reading direct paths instead of catalog tables without a reason.
- Writing millions of small files.
- Blindly increasing executor memory instead of analyzing plans and stage metrics.

## Optimization Decision Order

Optimize in this order:

1. Fix semantics: grain, keys, nulls, duplicates.
2. Reduce scan: partition pruning, predicate pushdown, column pruning.
3. Reduce data before shuffle: projection, filters, pre-aggregation.
4. Fix join strategy: uniqueness, broadcast, sort-merge, semi/anti join.
5. Handle skew: AQE, split hot keys, salting.
6. Tune shuffle partitions and AQE.
7. Fix file layout: compaction, partitioning, clustering.
8. Refresh statistics and metadata.
9. Change cluster resources only after the above.

## Review Checklist

When reviewing Spark SQL, the agent must check:

- Is there an explicit bounded input range?
- Does partition pruning work?
- Is `SELECT *` absent from the production path?
- Is the output grain clear?
- Do all joins have explicit types and correct keys?
- Are dimension tables unique or deterministically deduplicated?
- Is there any join explosion hidden by `DISTINCT`?
- Are there null traps: `NOT IN`, `COUNT(col)`, outer join filters in `WHERE`?
- Are windows deterministic and framed correctly?
- Do aggregations use the minimal `GROUP BY`?
- Is the write mode safe and idempotent?
- Are partition columns projected correctly?
- Is there post-write validation?
- Are file count and layout reasonable?
- Are statistics and metadata fresh enough for the optimizer?
- Is there a diagnostic plan using `EXPLAIN FORMATTED` and Spark UI?

## Incident Playbook

If a query suddenly becomes slow:

1. Compare input partitions and file count with the previous successful run.
2. Check whether a partition filter disappeared.
3. Check stale or missing statistics.
4. Check join strategy changes in `EXPLAIN`.
5. Check top skew keys.
6. Check small files and metadata listing time.
7. Check object storage/HDFS errors and retry storms.
8. Check cluster contention and executor failures.

If results become incorrect:

1. Check input range and late arriving data.
2. Check duplicates by source keys.
3. Check join cardinality.
4. Check null behavior after outer joins.
5. Check timezone/date derivation.
6. Check overwrite scope and touched partitions.
7. Compare counts by partition before and after.

## Enterprise Defaults

If the user has not provided context, use conservative defaults:

- Treat tables as large until proven otherwise.
- Do not perform full scans or full overwrites without an explicit requirement.
- Prefer partition-scoped incremental processing.
- Prefer SQL built-ins and table-format operations.
- Prefer deterministic deduplication.
- Prefer `LEFT SEMI` and `LEFT ANTI` for existence and exclusion.
- Prefer `EXPLAIN FORMATTED` before running expensive queries.
- Prefer post-write validation over trusting successful job status.

