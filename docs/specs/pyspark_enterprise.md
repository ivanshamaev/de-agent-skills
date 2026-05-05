# PySpark Enterprise Specification

This specification is for agents that design, write, review, debug, and optimize production PySpark pipelines at TB-PB scale. It applies to Spark DataFrame jobs, Spark SQL embedded in PySpark, lakehouse writes, Hive/HDFS integrations, and cloud object storage platforms. Treat it as an instruction file for real enterprise data engineering work, not as a beginner tutorial.

## Agent Contract

When working with PySpark, the agent must behave like a production data engineer:

- First clarify source and target formats, data volume, SLA, partitioning, keys, schema contract, incremental semantics, late data, write mode, and downstream consumers.
- Keep transformations in Spark DataFrame or Spark SQL APIs. Avoid RDDs unless the problem truly needs low-level distributed control.
- Prefer explicit schemas, explicit column selection, explicit join types, explicit write modes, and explicit partition ranges.
- Optimize for correctness first: grain, keys, null behavior, duplicates, idempotency, and overwrite scope.
- Optimize performance by reducing scans, projections, shuffles, skew, small files, and driver pressure before changing cluster size.
- Never hide data problems with `.distinct()`, `.dropDuplicates()`, `.limit()`, or broad `try/except`.
- Never use pandas, `collect()`, `toPandas()`, or Python row loops on unbounded distributed data.
- If important context is missing, state assumptions explicitly.

## Production Context to Collect

Before designing or changing a PySpark job, collect:

- Spark version and cluster manager: YARN, Kubernetes, Databricks, EMR, Glue, Synapse, or another platform.
- Table format: Parquet, ORC, Delta, Iceberg, Hudi, Hive table, JDBC source, Kafka, or raw files.
- Storage: HDFS, S3, ADLS, GCS, local dev, or another filesystem.
- Catalog: Hive Metastore, Glue, Unity Catalog, Iceberg catalog, Nessie, or direct paths.
- Data volume: total bytes, daily increment, number of files, largest partitions, expected growth.
- Partition columns and filter patterns.
- Join keys, business keys, uniqueness guarantees, and null behavior.
- Expected output grain: exactly what one output row represents.
- Incremental mode: append, rolling recompute, CDC merge, SCD, full rebuild, or backfill.
- Late arriving data and watermark policy.
- Schema evolution policy and compatibility requirements.
- SLA, retry behavior, job ownership, and observability requirements.

## Standard Imports

Use consistent imports:

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F, types as T, Window as W
```

Rules:

- Use `F`, `T`, and `W` aliases consistently.
- Avoid wildcard imports from `pyspark.sql.functions`.
- Import `DataFrame` for type hints in transformation functions.
- Keep platform-specific imports isolated from pure transformation logic.

## Function Shape

Production transformation code should be composable and testable:

- Prefer pure functions that accept and return `DataFrame`.
- Keep IO at job boundaries; keep transformation logic in separate functions.
- Give each transformation a clear input and output grain.
- Use explicit `select()` as a schema contract at boundaries.
- Keep functions small enough to review, but do not split every expression into trivial functions.
- Avoid mutating shared global state.

Recommended shape:

```python
def build_daily_user_metrics(events: DataFrame, users: DataFrame) -> DataFrame:
    filtered_events = (
        events
        .select(
            "event_date",
            "user_id",
            "event_id",
            "event_time",
            "event_type",
            "amount",
        )
        .filter(F.col("event_date").between(F.lit("2026-05-01"), F.lit("2026-05-05")))
    )

    users_one_row = deduplicate_users(users)

    return (
        filtered_events
        .join(users_one_row, on="user_id", how="left")
        .groupBy("event_date", "user_id", "country")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("amount"))).alias("revenue"),
        )
        .select(
            "event_date",
            "user_id",
            "country",
            "event_count",
            "revenue",
        )
    )
```

## Spark Session and Runtime Settings

Do not cargo-cult settings. Explain why each setting is needed.

Common production baseline:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.storeAssignmentPolicy", "ANSI")
```

Shuffle partitions:

```python
# Target roughly 128-512 MB per post-shuffle partition.
spark.conf.set("spark.sql.shuffle.partitions", "4000")
```

Broadcast threshold:

```python
# Use only when statistics and executor memory are trustworthy.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)

# Disable when stale stats cause dangerous broadcasts.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

Rules:

- Prefer AQE when available, but still design good joins and partitions.
- Size shuffle partitions from post-shuffle data size, not habit.
- Do not fix semantic or skew issues by only increasing executor memory.
- Be careful with settings in shared notebooks or long-lived sessions; reset or isolate job settings.

## Schema Contracts

At enterprise scale, schema drift is a production risk.

Rules:

- Provide explicit schemas for recurring CSV, JSON, text, and semi-structured inputs.
- Use explicit `select()` for output schema and column order.
- Cast important fields intentionally: IDs, money, timestamps, dates, booleans.
- Use `DecimalType` for money and exact numeric metrics where precision matters.
- Avoid relying on `inferSchema` in recurring production jobs.
- Avoid ambiguous stringly typed payloads in curated layers.
- Make nullability expectations explicit in validation and tests.

Explicit schema example:

```python
events_schema = T.StructType(
    [
        T.StructField("event_id", T.StringType(), nullable=False),
        T.StructField("user_id", T.StringType(), nullable=True),
        T.StructField("event_time", T.TimestampType(), nullable=True),
        T.StructField("event_date", T.DateType(), nullable=False),
        T.StructField("event_type", T.StringType(), nullable=True),
        T.StructField("amount", T.DecimalType(18, 2), nullable=True),
    ]
)
```

Output contract example:

```python
result = result.select(
    F.col("event_date").cast(T.DateType()).alias("event_date"),
    F.col("user_id").cast(T.StringType()).alias("user_id"),
    F.col("country").cast(T.StringType()).alias("country"),
    F.col("revenue").cast(T.DecimalType(18, 2)).alias("revenue"),
    F.current_timestamp().alias("processed_at"),
)
```

## Read Patterns

Prefer table reads for governed production data:

```python
events = spark.table("fact.events")
users = spark.table("dim.users_current")
```

Prefer columnar formats for large analytical workloads:

```python
events = spark.read.parquet("s3://company-lake/fact/events/")
```

For CSV/JSON, use explicit schemas:

```python
raw_events = (
    spark.read
    .schema(events_schema)
    .option("mode", "PERMISSIVE")
    .json("s3://company-lake/landing/events/date=2026-05-05/")
)
```

Rules:

- Avoid recursive broad path reads unless the source layout is intentionally designed for it.
- Prefer partition-aware table reads over manually listing many paths.
- Push partition filters immediately after read when table API cannot push them automatically.
- Avoid reading millions of tiny files directly into core transformations; compact or stage first.
- For JDBC sources, always use partitioned reads for large tables.

JDBC pattern:

```python
source = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "public.orders")
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("partitionColumn", "order_id")
    .option("lowerBound", "1")
    .option("upperBound", "1000000000")
    .option("numPartitions", "200")
    .load()
)
```

Do not run single-connection JDBC reads for large production tables.

## Column and Expression Style

Use readable, optimizer-friendly DataFrame expressions:

- Prefer built-in `pyspark.sql.functions` over UDFs.
- Prefer `select()` for multiple renames, casts, and derived columns.
- Use `.withColumn()` for one or two local additions, not schema-wide transformations.
- Use aliases to disambiguate joined DataFrames.
- Use `F.col()` when columns are ambiguous or expressions are complex.
- Do not build SQL expressions by unsafe string concatenation.
- Keep complex predicates named when it improves readability.

Good:

```python
is_purchase = F.col("event_type") == F.lit("purchase")
has_amount = F.col("amount").isNotNull()

purchases = (
    events
    .select("event_date", "user_id", "event_type", "amount")
    .filter(is_purchase & has_amount)
)
```

Avoid chaining many `withColumn()` calls:

```python
clean = raw.select(
    F.col("event_id").cast("string").alias("event_id"),
    F.col("user_id").cast("string").alias("user_id"),
    F.to_date("event_time").alias("event_date"),
    F.col("amount").cast(T.DecimalType(18, 2)).alias("amount"),
)
```

## Filter and Projection Pushdown

Reduce data early:

- Select only columns needed downstream.
- Filter partition columns directly.
- Avoid wrapping partition columns in functions.
- Push filters before joins and aggregations when semantics allow.
- Avoid UDFs in filters on large scans.
- Use `.explain("formatted")` to confirm `PartitionFilters`, `PushedFilters`, and `ReadSchema`.

Good:

```python
events = (
    spark.table("fact.events")
    .select("event_date", "user_id", "event_id", "event_type", "amount")
    .filter(F.col("event_date").between(F.lit("2026-05-01"), F.lit("2026-05-05")))
)
```

Bad:

```python
events = spark.table("fact.events").filter(F.to_date("event_time") == F.lit("2026-05-05"))
```

If timestamp logic is required, keep both partition and timestamp filters:

```python
events = events.filter(
    (F.col("event_date") == F.lit("2026-05-05"))
    & (F.col("event_time") >= F.lit("2026-05-05 00:00:00").cast("timestamp"))
    & (F.col("event_time") < F.lit("2026-05-06 00:00:00").cast("timestamp"))
)
```

## Joins

Joins are the most common source of correctness and performance incidents.

Mandatory rules:

- Always pass `how=` explicitly.
- Prefer `left` over `right` by swapping DataFrame order.
- Verify grain and uniqueness on both sides before joining.
- Deduplicate dimensions deterministically before joining if they are not unique.
- Filter and project both sides before large joins.
- Use aliases and explicit `select()` after joins to avoid duplicate ambiguous columns.
- Do not use `.distinct()` or `.dropDuplicates()` to hide join explosions.
- Use `left_semi` for existence checks.
- Use `left_anti` for exclusion checks.
- Treat null join key behavior explicitly.

Basic pattern:

```python
events_a = events.alias("events")
users_a = users.alias("users")

enriched = (
    events_a
    .join(users_a, on=F.col("events.user_id") == F.col("users.user_id"), how="left")
    .select(
        F.col("events.event_date"),
        F.col("events.event_id"),
        F.col("events.user_id"),
        F.col("users.country").alias("user_country"),
        F.col("events.amount"),
    )
)
```

When joining by same column names, `on=["user_id"]` can be cleaner:

```python
enriched = (
    events
    .join(users.select("user_id", "country"), on=["user_id"], how="left")
    .select("event_date", "event_id", "user_id", "country", "amount")
)
```

### Dimension Deduplication

Deduplicate with deterministic ordering:

```python
def deduplicate_users(users: DataFrame) -> DataFrame:
    w = (
        W.partitionBy("user_id")
        .orderBy(
            F.col("updated_at").desc_nulls_last(),
            F.col("ingestion_ts").desc_nulls_last(),
        )
    )

    return (
        users
        .filter(F.col("is_deleted") == F.lit(False))
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("user_id", "country", "segment")
    )
```

### Existence and Exclusion

Use semi and anti joins:

```python
active_events = events.join(active_users.select("user_id"), on=["user_id"], how="left_semi")

allowed_events = events.join(blocked_users.select("user_id"), on=["user_id"], how="left_anti")
```

Prefer these over collecting keys to the driver or using `isin()` with large lists.

### Broadcast Joins

Broadcast only truly small relations:

```python
users_dim = users.select("user_id", "country")

enriched = events.join(F.broadcast(users_dim), on=["user_id"], how="left")
```

Rules:

- Verify physical size and row count before broadcasting.
- Broadcast side must fit in executor memory with headroom.
- Do not broadcast growing dimensions without guardrails.
- Use broadcast hints only when the optimizer lacks correct statistics.
- If jobs fail with broadcast OOM, disable auto broadcast and refresh stats.

### Sort-Merge Joins

Sort-merge join is expected for large-large joins:

- Reduce both sides before joining.
- Align key types exactly.
- Avoid joining on derived expressions when you can materialize the derived key earlier.
- Check skew on join keys.
- Consider pre-aggregation when it preserves semantics.
- Do not assume repartitioning both sides by key always helps; inspect the physical plan.

### Range and Non-Equi Joins

Range joins can explode cardinality:

- Bound both sides by partition/date.
- Validate expected matches per left row.
- Prefer SCD2 half-open intervals.
- Consider bucketing, indexing, or table-format features if range joins are core workload.

SCD2 pattern:

```python
joined = (
    facts.alias("f")
    .join(
        dim.alias("d"),
        (F.col("f.user_id") == F.col("d.user_id"))
        & (F.col("f.event_time") >= F.col("d.valid_from"))
        & (F.col("f.event_time") < F.col("d.valid_to")),
        how="left",
    )
)
```

## Skew Handling

Skew is normal at PB scale. Do not ignore straggler tasks.

Signs:

- A few tasks run far longer than the rest.
- A few tasks read much more shuffle data than others.
- Top keys dominate the dataset.
- Broadcast or sort-merge join performance changes dramatically across dates.

Diagnose key skew:

```python
top_keys = (
    events
    .filter(F.col("event_date") == F.lit("2026-05-05"))
    .groupBy("user_id")
    .count()
    .orderBy(F.col("count").desc())
    .limit(50)
)
```

Preferred fixes, in order:

- Filter unnecessary rows before the join.
- Aggregate before joining when semantics allow.
- Enable AQE skew join optimization.
- Split hot keys from normal keys and process separately.
- Use salting only when you can prove it preserves correctness.

Hot key split:

```python
hot_keys = (
    events
    .filter(F.col("event_date") == F.lit("2026-05-05"))
    .groupBy("user_id")
    .count()
    .filter(F.col("count") > F.lit(100000000))
    .select("user_id")
)

normal_events = events.join(hot_keys, on=["user_id"], how="left_anti")
hot_events = events.join(hot_keys, on=["user_id"], how="inner")
```

Salting guidance:

- Salt only the skewed side and replicate the small side intentionally when needed.
- Keep salt cardinality bounded.
- Remove helper salt columns before final output.
- Document why the salted join is semantically equivalent.

## Aggregations

Aggregation rules:

- Group by the minimal key set required by the output grain.
- Aggregate before joining when it reduces data and preserves semantics.
- Use `F.count("*")` for row counts.
- Use `F.count("col")` only when intentionally excluding nulls.
- Prefer conditional aggregations over repeated scans.
- Avoid `countDistinct` on huge high-cardinality keys without cost evaluation.
- Use approximate aggregations only when the metric contract allows approximation.

Conditional aggregation:

```python
metrics = (
    events
    .groupBy("event_date", "country")
    .agg(
        F.count("*").alias("events"),
        F.sum(F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias("purchases"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("amount"))).alias("revenue"),
    )
)
```

Approximation, only when allowed:

```python
daily_users = (
    events
    .groupBy("event_date")
    .agg(F.approx_count_distinct("user_id", rsd=0.01).alias("approx_users"))
)
```

Do not sort globally before aggregation.

## Windows

Window functions create shuffle and sort.

Rules:

- Always specify `partitionBy`, except for genuinely small global data.
- Always specify deterministic ordering for `row_number`, `rank`, `first`, and `last`.
- Add tie-breakers such as ingestion timestamp, version, or event id.
- Specify frames for running totals.
- Reduce data before applying windows.
- Avoid windows over raw PB-scale data when pre-aggregation is possible.
- Do not use random ordering on large data.

Latest row pattern:

```python
w_latest = (
    W.partitionBy("business_id")
    .orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("ingestion_ts").desc_nulls_last(),
    )
)

latest = (
    updates
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
```

Running metric:

```python
w_running = (
    W.partitionBy("user_id")
    .orderBy(F.col("event_time").asc_nulls_last(), F.col("event_id").asc())
    .rowsBetween(W.unboundedPreceding, W.currentRow)
)

events_with_running = events.withColumn("running_amount", F.sum("amount").over(w_running))
```

Never use `W.partitionBy()` with no columns on large data. It moves all rows into one logical partition.

## Repartitioning, Coalesce, and Output Layout

Partitioning decisions affect shuffle, parallelism, and file count.

Rules:

- Use `repartition()` when you need a full shuffle for parallelism, join/group keys, range distribution, or output layout.
- Use `coalesce()` only to reduce partitions without a full shuffle.
- Do not use `repartition(1)` or `coalesce(1)` for large data.
- Do not partition output by high-cardinality fields such as `user_id`, `uuid`, `request_id`.
- Choose output partition columns by common query predicates and manageable cardinality.
- Target file size is usually 128-512 MB, sometimes larger for sequential scan-heavy fact tables.
- Estimate output rows and files by partition before writing.

Examples:

```python
# Improve output distribution by partition columns before write.
result = result.repartition("event_date", "country")
```

```python
# Reduce small output for a bounded small dataset only.
small_result = small_result.coalesce(20)
```

Do not repartition blindly before every write. Inspect output file counts and stage metrics.

## Writes and Overwrites

Production writes are high risk.

Before writing, verify:

- Target table/path exists or creation is intentional.
- Output schema matches the contract.
- Partition columns are present and correct.
- Write mode is explicit.
- Overwrite scope is bounded.
- The job is idempotent on retry.
- File count will be reasonable.
- Post-write validation exists.
- Rollback or atomic table-format operation exists.

Table write:

```python
(
    result
    .write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("event_date")
    .saveAsTable("mart.daily_revenue")
)
```

Dynamic partition overwrite:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    result
    .write
    .mode("overwrite")
    .insertInto("mart.daily_revenue")
)
```

Be careful: dynamic overwrite is safe only when `result` contains exactly the partitions intended for replacement.

Path write:

```python
(
    result
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet("s3://company-lake/mart/daily_revenue/")
)
```

Avoid unbounded broad path overwrite unless the job is an intentional full rebuild.

## Lakehouse Table Operations

Use table-format semantics when available.

Delta:

- Prefer `MERGE INTO` for upserts.
- Prefer partition predicates or `replaceWhere` for scoped replacement.
- Use optimize/compaction policies for small files.
- Use transaction history and rollback features when appropriate.

Iceberg:

- Prefer table writes and partition overwrite operations over manual path manipulation.
- Use snapshot isolation and rollback features.
- Maintain manifests and expire snapshots according to platform policy.

Hudi:

- Define record key and precombine key carefully.
- Understand copy-on-write vs merge-on-read tradeoffs.
- Plan compaction and clustering.

Agent rule: do not implement lakehouse atomic behavior by manually deleting and renaming object storage paths.

## Dirty Data and Quarantine

Production pipelines must handle bad data deliberately.

Rules:

- Use strict casts when dirty values should fail the pipeline.
- Use safe parsing and quarantine when dirty values are expected.
- Separate valid and invalid records when data loss is unacceptable.
- Add parse error columns or validation reasons for quarantine datasets.
- Do not silently replace missing keys with sentinel strings.

Example:

```python
parsed = raw.select(
    "raw_id",
    F.col("raw_amount").cast(T.DecimalType(18, 2)).alias("amount"),
    F.to_timestamp("raw_event_time").alias("event_time"),
)

valid = parsed.filter(F.col("amount").isNotNull() & F.col("event_time").isNotNull())
invalid = parsed.filter(F.col("amount").isNull() | F.col("event_time").isNull())
```

If using ANSI mode, invalid casts may fail instead of returning null. Choose behavior intentionally.

## Null Semantics

Null behavior must be explicit:

- `==` with null does not behave like regular equality.
- Use `.eqNullSafe()` only when business semantics require `NULL = NULL`.
- `count(col)` excludes nulls; `count("*")` counts rows.
- Aggregations over all-null values may return null.
- Be careful filtering after left joins; a `WHERE` filter on right-side columns can turn a left join into an inner join.
- Use `isNull()` and `isNotNull()` explicitly.

Null-safe join:

```python
joined = left.join(
    right,
    left["nullable_key"].eqNullSafe(right["nullable_key"]),
    how="inner",
)
```

Left join filter pattern:

```python
joined = (
    events.alias("e")
    .join(users.alias("u"), on=["user_id"], how="left")
    .filter((F.col("u.is_active") == F.lit(True)) | F.col("u.user_id").isNull())
)
```

## Time and Timezones

Time bugs are common in enterprise pipelines.

Rules:

- Store event time, ingestion time, and processing time as separate fields.
- Do not derive date partitions from timestamps without explicit timezone semantics.
- Use half-open intervals for timestamp windows.
- Avoid `between` for timestamp day ranges when the upper bound should be exclusive.
- Document session timezone when results depend on it.

Pattern:

```python
events = events.filter(
    (F.col("event_time") >= F.lit("2026-05-05 00:00:00").cast("timestamp"))
    & (F.col("event_time") < F.lit("2026-05-06 00:00:00").cast("timestamp"))
)
```

Prefer explicit partition dates plus timestamp filters for large tables.

## UDF, Pandas UDF, and Arrow

Python execution is expensive in Spark.

Rules:

- Prefer built-in Spark SQL functions.
- Avoid Python UDFs for logic expressible with `pyspark.sql.functions`.
- If custom vectorized logic is unavoidable, consider pandas UDFs with explicit return types.
- Keep UDFs deterministic and side-effect free.
- Do not perform network calls, database writes, or filesystem writes inside UDFs.
- Do not use UDFs in filters or join keys unless there is no alternative.
- Remember that Arrow helps data exchange, not distributed correctness.
- `toPandas()` still collects to the driver and must be bounded.

Bad:

```python
@F.udf("string")
def normalize_country(value: str) -> str:
    return value.strip().upper() if value else None
```

Good:

```python
normalized = df.select(F.upper(F.trim("country")).alias("country"))
```

## Driver Safety

The driver is not a data processing engine.

Avoid on unbounded data:

- `collect()`
- `toPandas()`
- `count()` repeated in loops
- `show()` on huge intermediate DataFrames as diagnostics for production
- `df.rdd.map(...).collect()`
- Building large Python lists and passing them to `isin()`
- Reading partition lists into the driver when the catalog can filter them

Allowed when bounded:

```python
sample_rows = (
    df
    .filter(F.col("event_date") == F.lit("2026-05-05"))
    .limit(100)
    .collect()
)
```

For diagnostics, prefer aggregated summaries over raw collection.

## Caching and Checkpointing

Cache is not a universal optimization.

Use cache or persist only when:

- The same expensive DataFrame is reused multiple times.
- The data fits in cluster memory/disk with other workloads considered.
- You trigger materialization deliberately.
- You unpersist when done.

```python
base = expensive_transform(events).persist()
base.count()

try:
    metric_a = build_metric_a(base)
    metric_b = build_metric_b(base)
finally:
    base.unpersist()
```

Use checkpointing when lineage becomes too long or iterative algorithms need lineage truncation:

```python
spark.sparkContext.setCheckpointDir("s3://company-tmp/checkpoints/job_name/")
checkpointed = df.checkpoint(eager=True)
```

Do not cache raw PB-scale tables or one-off DataFrames.

## Observability and Data Quality

Successful Spark job status does not prove correct data.

Capture and validate:

- Input row counts by partition.
- Output row counts by partition.
- Null rates for key columns.
- Duplicate counts by business key.
- Join coverage and unmatched rates.
- Metric reconciliation against source.
- Min/max dates and timestamps.
- Bad record counts.
- Output file counts and approximate file sizes.

Examples:

```python
partition_counts = (
    result
    .groupBy("event_date")
    .agg(
        F.count("*").alias("rows"),
        F.countDistinct("user_id").alias("users"),
        F.sum(F.when(F.col("user_id").isNull(), 1).otherwise(0)).alias("null_user_rows"),
    )
)
```

Duplicate check:

```python
duplicates = (
    result
    .groupBy("event_date", "user_id")
    .count()
    .filter(F.col("count") > 1)
)
```

Join coverage:

```python
coverage = (
    events.alias("e")
    .join(users.alias("u"), on=["user_id"], how="left")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("u.user_id").isNull(), 1).otherwise(0)).alias("unmatched_rows"),
    )
)
```

## Explain and Plan Review

For expensive jobs, inspect plans:

```python
df.explain("formatted")
df.explain(mode="cost")
```

Look for:

- `FileScan`: verify partition filters, pushed filters, and read schema.
- `Exchange`: shuffle boundary; check keys and partition counts.
- `BroadcastHashJoin`: verify broadcast side is small.
- `SortMergeJoin`: expected for large-large joins.
- `BroadcastNestedLoopJoin`: usually a serious warning.
- `CartesianProduct`: almost always a bug unless explicitly tiny and bounded.
- `HashAggregate`: check partial and final aggregates.
- `Window`: check for large sort and shuffle.
- `Sort`: check for unnecessary global ordering.

If the plan scans a large partitioned table without partition filters, stop and fix the query.

## Testing Strategy

Test transformation logic with small DataFrames:

- Normal rows.
- Null keys and null measures.
- Duplicate business keys.
- Missing dimension rows.
- Multiple dimension rows per key.
- Late arriving rows.
- Schema changes.
- Boundary timestamps and dates.

Keep pure transformations easy to test:

```python
def test_build_daily_revenue(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            ("2026-05-05", "u1", "purchase", 10.0),
            ("2026-05-05", "u1", "refund", -2.0),
        ],
        ["event_date", "user_id", "event_type", "amount"],
    )

    users = spark.createDataFrame(
        [("u1", "US")],
        ["user_id", "country"],
    )

    result = build_daily_revenue(events, users)

    assert result.count() == 1
```

For production confidence, combine unit tests with integration tests on representative partitions.

## Incremental Processing

Prefer bounded incremental processing:

- Define processing range explicitly.
- Use partition filters on input.
- Overwrite only touched partitions.
- Keep daily runs and backfills on the same code path when possible.
- Track processed partitions in metadata or orchestration.
- Make retries idempotent.
- Handle late data with rolling recompute or merge semantics.

Rolling recompute:

```python
events_window = events.filter(
    F.col("event_date").between(F.lit("2026-04-29"), F.lit("2026-05-05"))
)

result = build_user_daily_metrics(events_window, users)
```

CDC merge requires deterministic keys and ordering:

- Record key.
- Operation type.
- Ordering/precombine column.
- Delete semantics.
- Tie-breaker for same timestamp/version.

## File Layout and Small Files

Small files can dominate runtime at scale.

Rules:

- Prefer columnar formats for curated data.
- Avoid writing millions of tiny files.
- Compact landing or streaming outputs before heavy analytics.
- Repartition by output partition columns only when it improves distribution.
- Use platform compaction features for Delta/Iceberg/Hudi.
- Monitor file count per partition.
- Avoid high-cardinality partition columns.
- Account for expensive listing on object storage.

Estimate partition sizes before write:

```python
sizes = (
    result
    .groupBy("event_date", "country")
    .count()
    .orderBy(F.col("count").desc())
)
```

## Streaming Considerations

For Structured Streaming:

- Define checkpoint location explicitly and keep it stable.
- Use event-time watermarks when aggregating late data.
- Make sinks idempotent or transactional.
- Avoid unbounded state.
- Monitor state store size and processing delay.
- Use `foreachBatch` carefully; batch logic must be idempotent.
- Do not change checkpointed query schema casually.

Example:

```python
query = (
    stream_df
    .withWatermark("event_time", "2 days")
    .groupBy(F.window("event_time", "1 day"), "user_id")
    .agg(F.count("*").alias("events"))
    .writeStream
    .format("delta")
    .option("checkpointLocation", "s3://company-checkpoints/user_daily_events/")
    .outputMode("append")
    .start("s3://company-lake/mart/user_daily_events/")
)
```

## Security and Governance

Enterprise PySpark code must account for governance:

- Do not copy PII into marts without a clear requirement.
- Apply masking, tokenization, or access policies provided by the platform.
- Do not write production data to personal scratch paths.
- Avoid logging secrets, full connection strings, tokens, or PII values.
- Use secret managers for credentials.
- Set table owner, location, and grants through platform-approved mechanisms.
- Clean temporary debug outputs with TTL policy.

## Anti-Patterns

Forbidden or suspicious patterns:

- `collect()`, `toPandas()`, or large `show()` calls on unbounded data.
- Python loops over rows.
- RDD conversions for normal ETL.
- UDFs where built-in functions work.
- Chaining dozens of `withColumn()` calls.
- `distinct()` or `dropDuplicates()` after joins without root-cause analysis.
- Global `orderBy()` on large data without a real requirement.
- `repartition(1)` or `coalesce(1)` for large data.
- High-cardinality output partitions.
- Full path overwrite for incremental jobs.
- Join keys with mismatched types.
- Broadcast hints on tables of unknown size.
- Unbounded JDBC reads through one connection.
- Storing money as `DoubleType`.
- Silent schema inference in production.
- Swallowing exceptions and publishing partial data.

## Optimization Decision Order

Optimize in this order:

1. Fix semantics: output grain, keys, nulls, duplicates, and late data.
2. Reduce scan: partition pruning, predicate pushdown, column pruning.
3. Reduce data before shuffle: filters, projection, pre-aggregation.
4. Fix join strategy: uniqueness, broadcast, semi/anti joins, sort-merge.
5. Handle skew: AQE, split hot keys, salting when justified.
6. Tune shuffle partitions and AQE.
7. Fix output layout: partitioning, target file sizes, compaction.
8. Refresh catalog/table statistics.
9. Increase cluster resources only after the above.

## Review Checklist

When reviewing PySpark code, the agent must check:

- Are transformation functions separated from IO?
- Are input and output schemas explicit?
- Is the input range bounded for large sources?
- Does partition pruning work?
- Are projections pushed early?
- Are join types explicit?
- Are join keys typed consistently?
- Are dimensions unique or deterministically deduplicated?
- Is any join explosion hidden by `.distinct()` or `.dropDuplicates()`?
- Are null semantics explicit?
- Are windows partitioned, ordered, and framed correctly?
- Are aggregations at the intended grain?
- Are UDFs avoided or justified?
- Is driver collection bounded?
- Is write mode explicit and safe?
- Are partition columns present in output?
- Is overwrite scope bounded and idempotent?
- Is output file count controlled?
- Are data quality checks present?
- Is there an `explain` or Spark UI diagnostic path for performance-sensitive jobs?

## Incident Playbook

If a job suddenly becomes slow:

1. Compare input file count, partition count, and bytes with the previous successful run.
2. Check whether partition filters disappeared.
3. Inspect `df.explain("formatted")` for plan changes.
4. Check Spark UI for shuffle, spills, skew, and stragglers.
5. Check whether broadcast strategy changed.
6. Check small files and object storage listing time.
7. Check stale statistics.
8. Check cluster contention and executor failures.
9. Check recent schema or data distribution changes.

If output is wrong:

1. Verify input range and late arriving data.
2. Check duplicate source keys.
3. Check join cardinality.
4. Check null behavior after outer joins.
5. Check timezone and date derivation.
6. Check overwrite scope and touched partitions.
7. Compare row counts and metrics by partition before and after.

## Enterprise Defaults

When context is missing, use conservative defaults:

- Treat data as large until proven otherwise.
- Do not full-scan or full-overwrite without an explicit requirement.
- Prefer incremental partition-scoped processing.
- Prefer table APIs and catalog metadata over direct paths.
- Prefer DataFrame/Spark SQL built-ins over UDFs.
- Prefer deterministic deduplication.
- Prefer `left_semi` and `left_anti` for existence and exclusion checks.
- Prefer post-write validation over trusting successful job completion.
- Prefer explain plans and Spark UI evidence over guesswork.

