---
name: spark_sql
description: Use when writing, reviewing, debugging, or optimizing production Spark SQL for Hive/lakehouse/HDFS tables, including CTE-heavy queries, joins, windows, partition pruning, insert/overwrite semantics, query hints, statistics, EXPLAIN plans, AQE, skew, materialization, and SQL performance diagnostics.
---

# Spark SQL Engineer

## When to Use

Use this skill when:
- The user asks for Spark SQL, Hive-compatible SQL, lakehouse SQL, or SQL queries executed by `spark.sql`
- The task can be expressed more clearly in SQL than PySpark DataFrame code
- You need to review or optimize joins, aggregations, windows, partitions, writes, or query plans
- The data volume is large enough that shuffles, skew, HDFS/file layout, and partition pruning matter

Prefer the `pyspark_etl` skill for DataFrame-heavy pipeline code. Use this skill for SQL-first answers.

## Core Workflow

1. Clarify the Spark version, table format/catalog, HDFS/storage location, table sizes, partition columns, unique keys, write mode, and expected output schema.
2. Start with a readable SQL shape using CTEs and explicit column lists.
3. Push filters and projections as early as semantics allow.
4. Verify join keys, key uniqueness, join type, and null behavior before adding hints.
5. For expensive or suspicious queries, recommend `EXPLAIN FORMATTED` or `EXPLAIN COST`.
6. If optimizer behavior matters, check table statistics with `DESCRIBE EXTENDED` and refresh them with `ANALYZE TABLE` when appropriate.
7. Call out assumptions about partition pruning, skew, overwrite scope, and table-format-specific features.

## Query Structure

Use CTEs for complex logic, with names that describe data state:

```sql
WITH filtered_events AS (
    SELECT
        user_id,
        event_time,
        event_date,
        amount
    FROM raw.events
    WHERE event_date >= DATE '2026-01-01'
      AND event_type = 'purchase'
),
daily_revenue AS (
    SELECT
        event_date,
        user_id,
        SUM(amount) AS revenue
    FROM filtered_events
    GROUP BY event_date, user_id
)
SELECT
    event_date,
    user_id,
    revenue
FROM daily_revenue;
```

Rules:
- Avoid `SELECT *` in production queries.
- Put one selected expression per line in non-trivial queries.
- Use explicit aliases for derived columns.
- Keep CTEs purposeful; do not create a CTE for every tiny expression.
- Prefer typed literals such as `DATE '2026-01-01'` when the target type matters.

## Filtering and Projection

- Filter partition columns directly in `WHERE` to enable partition pruning.
- Avoid wrapping partition columns in functions inside predicates.
- Select only columns required by downstream CTEs.
- Keep complex predicates readable and grouped with parentheses.

```sql
WHERE event_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
  AND country IN ('US', 'CA')
```

Prefer this over:

```sql
WHERE TO_DATE(event_time) = DATE '2026-01-01'
```

when `event_date` is already a partition column.

## Joins

- Always make join type explicit: `INNER JOIN`, `LEFT JOIN`, `LEFT SEMI JOIN`, `LEFT ANTI JOIN`.
- Prefer `LEFT JOIN` over `RIGHT JOIN` by swapping table order.
- Use aliases and qualify columns when more than one table is present.
- Filter and project large inputs before joining.
- Aggregate before joining when it reduces data volume and preserves semantics.
- Use `LEFT SEMI JOIN` for existence checks and `LEFT ANTI JOIN` for exclusion checks.
- Do not use `DISTINCT` to hide join explosions; fix source duplicates or join keys intentionally.

```sql
WITH users_dim AS (
    SELECT
        user_id,
        country
    FROM dwh.users
    WHERE is_current = TRUE
),
joined AS (
    SELECT
        e.event_date,
        u.country,
        e.amount
    FROM filtered_events e
    LEFT JOIN users_dim u
        ON e.user_id = u.user_id
)
SELECT
    event_date,
    country,
    SUM(amount) AS revenue
FROM joined
GROUP BY event_date, country;
```

## Aggregations and Windows

- Group by the minimal keys needed for the result.
- Use `COUNT(*)` for row counts and `COUNT(col)` only when null exclusion is intended.
- Specify deterministic ordering for `ROW_NUMBER`, `RANK`, `FIRST_VALUE`, and `LAST_VALUE`.
- Specify window frames for cumulative or analytic calculations.
- Control null ordering explicitly with `NULLS FIRST` or `NULLS LAST`.

```sql
WITH ranked_events AS (
    SELECT
        user_id,
        event_time,
        event_id,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY event_time DESC NULLS LAST, event_id DESC
        ) AS rn
    FROM raw.events
)
SELECT
    user_id,
    event_time,
    event_id
FROM ranked_events
WHERE rn = 1;
```

For running totals:

```sql
SUM(amount) OVER (
    PARTITION BY user_id
    ORDER BY event_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS running_amount
```

## Performance and Optimizer Guidance

- Prefer Parquet/ORC/Delta/Iceberg-backed tables over row-oriented formats for analytical workloads.
- Keep catalog/table statistics current for important managed tables.
- Use `EXPLAIN FORMATTED` for readable physical plans and `EXPLAIN COST` when statistics are available.
- Watch for `Exchange`, `BroadcastHashJoin`, `SortMergeJoin`, full scans, missing partition filters, and unexpected Cartesian products.
- Rely on Adaptive Query Execution when available, but still write good join predicates and pruning filters.
- AQE can coalesce shuffle partitions, adjust join strategy from runtime statistics, and optimize skewed joins.
- Cache only reused intermediate tables/views, and uncache them when they are no longer needed.

Diagnostics:

```sql
EXPLAIN FORMATTED
SELECT ...;

EXPLAIN COST
SELECT ...;

ANALYZE TABLE dwh.fact_events COMPUTE STATISTICS;
ANALYZE TABLE dwh.fact_events COMPUTE STATISTICS FOR COLUMNS user_id, event_date;
DESCRIBE EXTENDED dwh.fact_events;
```

## Error Handling and Debugging

When a Spark SQL query fails, identify whether the failure is analysis-time, planning-time, read-time, or runtime.

Common failure patterns:
- `AnalysisException`: check unresolved columns/tables, ambiguous column names after joins, missing functions, unsupported SQL syntax, invalid casts, and table-format-specific commands.
- `OutOfMemoryError` or `GC overhead limit exceeded`: inspect Spark UI stage metrics for spill, peak memory, shuffle read/write, large broadcasts, wide rows, and oversized groups/windows.
- `Task failed N times`: compare task durations and shuffle read sizes in the Stage View; a few very slow tasks usually means skew, bad input splits, or executor-local resource pressure.
- `FileNotFoundException` on HDFS: if partitions were added or removed outside Spark, use `MSCK REPAIR TABLE` or `ALTER TABLE ADD/DROP PARTITION` for catalog partition metadata; use `REFRESH TABLE` when Spark metadata or file listings are stale.
- Permission or path errors: verify the effective user, HDFS ACLs, table location, and whether the query reads a table or a direct path.

Debugging checklist:
- Run `EXPLAIN FORMATTED` and look for full scans, missing `PartitionFilters`, unexpected `Exchange`, and wrong join strategy.
- Reduce the query to the smallest failing CTE and validate schemas with `DESCRIBE TABLE`.
- Check Spark UI SQL and Stage tabs before changing configs.
- Prefer a query or data-layout fix before increasing executor memory or shuffle partitions.

## HDFS and Partitioned Tables

Use SQL against catalog tables when possible instead of hard-coded HDFS paths. If direct paths are necessary, make them explicit and stable:

```sql
SELECT user_id, event_date, amount
FROM parquet.`hdfs:///warehouse/raw/events/event_date=2026-01-01`;
```

HDFS layout rules:
- Prefer columnar files such as Parquet or ORC on HDFS.
- Avoid many tiny files; compact upstream data or use `REBALANCE` before writes when AQE is enabled.
- Keep partition directories consistent with Hive-style naming, for example `event_date=2026-01-01/country=US`.
- Partition by low/medium-cardinality predicates that are common in queries, usually dates or business domains.
- Do not partition by high-cardinality IDs such as `user_id` unless the table design explicitly requires it.
- Avoid recursive scans over broad HDFS roots; query a table or a narrow path.
- After out-of-band HDFS writes to partitioned Hive tables, refresh metadata with `MSCK REPAIR TABLE`, `ALTER TABLE ADD PARTITION`, or the catalog-specific repair command.
- Use `REFRESH TABLE` when Spark has stale metadata for files or partitions.

For partitioned tables, always preserve partition pruning:

```sql
SELECT
    event_date,
    country,
    SUM(amount) AS revenue
FROM raw.events
WHERE event_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-07'
GROUP BY event_date, country;
```

Avoid predicates that hide partition columns:

```sql
-- Avoid when event_date is the partition column.
WHERE DATE_TRUNC('MONTH', event_date) = DATE '2026-01-01'
```

Prefer range predicates:

```sql
WHERE event_date >= DATE '2026-01-01'
  AND event_date < DATE '2026-02-01'
```

## Filtering from Other Datasets

When a large HDFS-backed fact table must be filtered by another dataset, avoid collecting keys into a huge `IN (...)` list. Model the filter dataset as a relation and let Spark optimize the join.

Use `LEFT SEMI JOIN` for key-based filtering:

```sql
WITH selected_users AS (
    SELECT DISTINCT user_id
    FROM mart.campaign_users
    WHERE campaign_date = DATE '2026-01-05'
),
events AS (
    SELECT user_id, event_date, event_type, amount
    FROM raw.events
    WHERE event_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-07'
)
SELECT
    e.user_id,
    e.event_date,
    e.event_type,
    e.amount
FROM events e
LEFT SEMI JOIN selected_users u
    ON e.user_id = u.user_id;
```

For exclusion, use `LEFT ANTI JOIN`:

```sql
SELECT e.user_id, e.event_date, e.amount
FROM events e
LEFT ANTI JOIN blocked_users b
    ON e.user_id = b.user_id;
```

Guidelines for complex dataset-driven filters:
- Apply partition filters on the large fact table even when another dataset controls the selection.
- Deduplicate the filter dataset only on the join keys, not with broad `SELECT DISTINCT *`.
- If the filter dataset is small and statistics are missing, consider a `BROADCAST` hint after validating size.
- If the filter dataset also contains date ranges, join on both business key and partition/date range to preserve pruning opportunities.
- For partitioned fact tables joined to filtered dimension tables, check `EXPLAIN FORMATTED` for partition filters and dynamic partition pruning behavior.
- Materialize a reused complex filter dataset as a temp view/table when it is referenced by multiple heavy queries.

## Skew Handling

Diagnose skew before adding manual workarounds:
- In Spark UI Stage View, compare task duration, input size, shuffle read, spill, and records per task.
- In SQL plans, look for large joins/aggregations on low-cardinality or hot keys.
- Check key distribution with a bounded aggregation on the suspected key.
- AQE skew join handling is controlled by `spark.sql.adaptive.skewJoin.enabled` and related thresholds such as `spark.sql.adaptive.skewJoin.skewedPartitionFactor` and `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`.

Manual salting can help before or beyond AQE when one join key dominates. Salt only the large/skewed side and expand the small side by the same salt range:

```sql
WITH skewed_events AS (
    SELECT
        user_id,
        event_date,
        amount,
        CAST(PMOD(HASH(event_id), 16) AS INT) AS salt
    FROM raw.events
    WHERE event_date = DATE '2026-01-01'
),
user_salts AS (
    SELECT EXPLODE(SEQUENCE(0, 15)) AS salt
),
salted_users AS (
    SELECT
        u.user_id, u.country, s.salt
    FROM dwh.users u
    CROSS JOIN user_salts s
)
SELECT
    e.event_date,
    u.country,
    SUM(e.amount) AS revenue
FROM skewed_events e
JOIN salted_users u
    ON e.user_id = u.user_id
   AND e.salt = u.salt
GROUP BY e.event_date, u.country;
```

Use salting sparingly: it increases data volume on the expanded side and should be removed when AQE/data layout fixes are enough.

## Materialization

Materialize when a complex intermediate result is reused, expensive to recompute, or needed to isolate failures.

Use a temp view for readability within one Spark session; it is logical and does not by itself write data:

```sql
CREATE OR REPLACE TEMP VIEW enriched_events AS
SELECT
    e.event_date, e.user_id, u.country, e.amount
FROM raw.events e
LEFT JOIN dwh.users u
    ON e.user_id = u.user_id;
```

Use CTAS to persist an expensive intermediate result to disk, especially across sessions or jobs:

```sql
CREATE TABLE tmp.enriched_events
USING PARQUET
PARTITIONED BY (event_date)
AS
SELECT
    event_date, user_id, country, amount
FROM enriched_events;
```

Use `CACHE TABLE` when the result is reused several times in the same application and fits in cluster memory. Prefer CTAS when the result is too large, reused by other jobs, or should survive executor loss/session end.

Use `CACHE TABLE enriched_events` before repeated reads and `UNCACHE TABLE enriched_events` after the last reuse. `UNCACHE TABLE` releases cached blocks from executor memory, reducing GC pressure for later stages.

## Hints

Use hints only when you have evidence the optimizer lacks good information.

Join hints:

```sql
SELECT /*+ BROADCAST(u) */
    e.user_id,
    u.country
FROM raw.events e
JOIN dwh.users u
    ON e.user_id = u.user_id;
```

Partitioning hints for output shape:

```sql
SELECT /*+ REBALANCE(event_date) */
    event_date,
    country,
    revenue
FROM daily_revenue;
```

Guidelines:
- `BROADCAST` is for genuinely small relations.
- `MERGE` can request sort-merge joins for large sortable inputs.
- `SHUFFLE_HASH` may help when per-partition build sides are small enough.
- `REBALANCE` is useful before writes to reduce tiny or oversized files, and depends on AQE.
- Hints are suggestions, not guarantees; Spark may ignore unsupported strategies.

## Writes and DML

Be explicit about write semantics, columns, partitions, and overwrite scope.

```sql
INSERT OVERWRITE TABLE dwh.daily_revenue
PARTITION (event_date = DATE '2026-01-01')
SELECT
    country,
    SUM(amount) AS revenue
FROM filtered_events
WHERE event_date = DATE '2026-01-01'
GROUP BY country;
```

Prefer column lists or `BY NAME` when schema order may drift:

```sql
INSERT INTO dwh.daily_revenue (event_date, country, revenue)
SELECT
    event_date,
    country,
    revenue
FROM daily_revenue;
```

Use `MERGE INTO`, `UPDATE`, or `DELETE` only when the configured table format/catalog supports row-level operations, such as Delta, Iceberg, or Hudi with the needed Spark extensions. Mention this dependency in generated code or review comments.

For production writes, call out:
- Append vs overwrite vs replace-where behavior
- Static vs dynamic partition overwrite
- Idempotency and retry safety
- Late-arriving data policy
- Expected output file count and partition cardinality

## Anti-Patterns

Do not:
- Use `SELECT *` in production queries or writes
- Omit partition filters on large partitioned tables
- Scan broad HDFS roots instead of catalog tables, narrow paths, or partition-pruned predicates
- Use huge literal `IN` lists from another dataset instead of joins or semi joins
- Use `DISTINCT` as a bandage for incorrect joins
- Use `ORDER BY` globally unless the final result truly requires total ordering
- Write broad `INSERT OVERWRITE` statements without an intentional overwrite scope
- Broadcast large or unknown-size tables
- Cross join unless explicitly requested and bounded
- Put Python/Scala UDF logic into SQL when built-in Spark SQL functions can express it
- Depend on nondeterministic deduplication without a stable `ORDER BY`
- Add salting without proving skew and bounding the salt factor
- Cache large one-time intermediates instead of writing a controlled CTAS or avoiding materialization
- Hide type conversions; cast explicitly where correctness depends on type

## Example Query

```sql
WITH purchases AS (
    SELECT
        user_id,
        event_date,
        amount
    FROM raw.events
    WHERE event_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
      AND event_type = 'purchase'
),
users_dim AS (
    SELECT
        user_id,
        country
    FROM dwh.users
    WHERE is_current = TRUE
),
revenue AS (
    SELECT
        p.event_date,
        u.country,
        SUM(p.amount) AS revenue
    FROM purchases p
    LEFT JOIN users_dim u
        ON p.user_id = u.user_id
    GROUP BY p.event_date, u.country
)
SELECT
    event_date,
    country,
    revenue
FROM revenue;
```

## Output Expectations

When producing Spark SQL:
- Return valid Spark SQL, not generic warehouse SQL
- Use explicit columns, aliases, join types, and write semantics
- Prefer readable CTEs for multi-step logic
- Explain performance-sensitive choices briefly
- Mention when a feature depends on Spark version, table format, catalog, or lakehouse extensions
- Preserve HDFS partition pruning and avoid small-file-heavy output layouts
- Include a debugging path for failures, skew, and memory pressure when relevant
- Suggest `EXPLAIN`, `ANALYZE TABLE`, or runtime metric checks when performance depends on data distribution

## References to Consult When Needed

- Apache Spark SQL Performance Tuning: https://spark.apache.org/docs/latest/sql-performance-tuning.html
- Apache Spark SQL Syntax: https://spark.apache.org/docs/latest/sql-ref-syntax.html
- Apache Spark SQL Hints: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html
- Apache Spark SQL EXPLAIN: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html
- Apache Spark SQL ANALYZE TABLE: https://spark.apache.org/docs/latest/sql-ref-syntax-aux-analyze-table.html
