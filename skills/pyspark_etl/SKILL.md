---
name: pyspark_etl
description: Use when designing, implementing, reviewing, or optimizing production PySpark ETL/DataFrame pipelines at GB-TB+ scale, including schemas, joins, partitioning, window functions, writes, UDF avoidance, and Spark performance diagnostics.
---

# PySpark ETL Engineer

## When to Use

Use this skill when:
- Writing or reviewing PySpark ETL pipelines
- Transforming large datasets with Spark DataFrames or Spark SQL
- Optimizing joins, aggregations, partitioning, window functions, or writes
- Debugging Spark performance issues such as shuffles, skew, small files, or slow stages

Do not use pandas for distributed ETL unless the user explicitly asks for pandas API on Spark or a local-only sample.

## Core Workflow

1. Clarify source/target formats, data volume, partition columns, incremental semantics, schema evolution, and SLA.
2. Keep transformations in Spark DataFrame/Spark SQL APIs; avoid RDDs unless the problem truly requires low-level control.
3. Establish a schema contract at input/output boundaries with explicit schemas and `select`.
4. Push projection and filters early to enable column pruning, predicate pushdown, and partition pruning.
5. Minimize wide transformations: joins, groupBy, distinct, repartition, orderBy, and window operations.
6. For expensive jobs, inspect plans with `df.explain("formatted")` or `df.explain(mode="cost")` when table statistics are available.
7. Include focused tests for transformation logic using small DataFrames; test nulls, duplicates, missing keys, and schema shape.

## Standard Imports

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F, types as T, Window as W
```

Use these aliases consistently. Avoid introducing alternate aliases for `functions`, `types`, or `Window`.

## Read Patterns

Prefer columnar formats for ETL:

```python
events = spark.read.parquet("/data/raw/events/")
```

For CSV/JSON, provide an explicit schema in production:

```python
schema = T.StructType(
    [
        T.StructField("user_id", T.StringType(), nullable=False),
        T.StructField("event_time", T.TimestampType(), nullable=True),
        T.StructField("amount", T.DecimalType(18, 2), nullable=True),
    ]
)

events = (
    spark.read
    .schema(schema)
    .option("header", "true")
    .csv("/data/raw/events/")
)
```

Avoid `inferSchema` for recurring production jobs. It is slower and can change behavior when new files arrive.

## Transformation Style

Prefer column names as strings in Spark functions when unambiguous:

```python
df = df.select(F.lower("country").alias("country"))
```

Use `F.col()` when disambiguating joined DataFrames or when it improves readability:

```python
events = events.alias("events")
users = users.alias("users")

joined = events.join(users, on="user_id", how="left")
joined = joined.select(
    F.col("events.user_id"),
    F.col("users.country").alias("user_country"),
)
```

Use `select` as a schema contract near the start or end of a transform:

```python
clean_events = raw_events.select(
    "user_id",
    F.to_date("event_time").alias("event_date"),
    F.col("amount").cast(T.DecimalType(18, 2)).alias("amount"),
)
```

Keep complex logic named:

```python
is_purchase = F.col("event_type") == F.lit("purchase")
has_amount = F.col("amount").isNotNull()

purchases = events.filter(is_purchase & has_amount)
```

Keep chains short and grouped by operation type. Split long or mixed chains into named steps:

```python
events = (
    events
    .select("user_id", "event_time", "event_type", "amount")
    .filter(F.col("event_time") >= F.lit("2026-01-01"))
)

events = events.withColumn("event_date", F.to_date("event_time"))

result = (
    events
    .join(users, on="user_id", how="left")
    .groupBy("country", "event_date")
    .agg(F.sum("amount").alias("revenue"))
)
```

Use `.withColumn()` for one added computed column. Prefer a single `.select()` for renames, casts, and multiple derived output columns.

## Joins

- Always pass `how=` explicitly.
- Prefer `left` joins over `right` joins by swapping DataFrame order.
- Broadcast only genuinely small dimensions, and verify size/cardinality assumptions.
- Use aliases to disambiguate columns after joins.
- Do not use `.distinct()` or `.dropDuplicates()` to hide join explosions; fix the join keys or deduplicate the input intentionally.
- For large joins, check join type, join keys, partitioning, skew, and whether table/file statistics are available.

```python
users_dim = F.broadcast(users.select("user_id", "country"))

enriched = events.join(users_dim, on="user_id", how="left")
```

## Aggregations and Windows

Aggregate before joining when it reduces data volume and preserves semantics:

```python
daily_revenue = (
    purchases
    .groupBy("event_date", "country")
    .agg(F.sum("amount").alias("revenue"))
)
```

Always specify an explicit window frame:

```python
w = (
    W.partitionBy("user_id")
    .orderBy(F.asc_nulls_last("event_time"))
    .rowsBetween(W.unboundedPreceding, W.currentRow)
)

events = events.withColumn("running_amount", F.sum("amount").over(w))
```

Never use `W.partitionBy()` with no columns; it moves all data into a single partition. Use `agg()` for global aggregates.

## Partitioning, Skew, and AQE

- Repartition only when it is needed for parallelism, join/group keys, range distribution, or output layout.
- Use `coalesce()` only to reduce partitions without a full shuffle, commonly before controlled small outputs.
- Choose output partition columns by query predicates and manageable cardinality; avoid high-cardinality partition columns that create many small files.
- Watch for skew by checking stage task durations, shuffle read sizes, and key distributions.
- Rely on Adaptive Query Execution when available, but still design good joins and partitioning. AQE can coalesce shuffle partitions, convert join strategies at runtime, and optimize skewed joins.
- For table-backed pipelines, keep statistics current when the catalog supports it; missing statistics can lead to poor join plans.

Useful diagnostics:

```python
df.explain("formatted")
df.explain(mode="cost")
```

## UDF and Arrow Guidance

- Prefer built-in `pyspark.sql.functions` over Python UDFs.
- If custom Python logic is unavoidable, consider `pandas_udf` or Arrow-optimized UDFs and declare return types explicitly.
- Keep UDF logic deterministic and side-effect free.
- Remember that `toPandas()` and `collect()` bring data to the driver even when Arrow is enabled; only use them on bounded small data.
- Tune Arrow batch size only when memory pressure or very wide rows make it necessary.

## Write Patterns

Prefer columnar output:

```python
(
    result
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet("/data/dwh/revenue/")
)
```

For production tables, be explicit about:
- Write mode and overwrite scope
- Partition columns and expected file count
- Schema evolution policy
- Idempotency and retry behavior
- Late-arriving data handling

Avoid unbounded `overwrite` on broad paths unless the job is intentionally rebuilding the full dataset.

## Anti-Patterns

Do not:
- Call `collect()`, `toPandas()`, or `.show()` on unbounded large DataFrames
- Use Python loops over rows instead of Spark transformations
- Use UDFs when built-in Spark functions can express the logic
- Chain many `.withColumn()` calls for schema-wide transformations
- Use `orderBy()` globally unless the final result truly requires total ordering
- Write high-cardinality partitions or uncontrolled tiny files
- Ignore ambiguous duplicate column names after joins
- Use empty strings or sentinel values like `"NA"` for missing data; use `F.lit(None)` and proper types
- Leave commented-out code or unexplained magic literals in production ETL

## Example Pipeline

```python
def build_daily_revenue(events: DataFrame, users: DataFrame) -> DataFrame:
    purchases = (
        events
        .select("user_id", "event_time", "event_type", "amount")
        .filter(F.col("event_type") == F.lit("purchase"))
    )

    purchases = purchases.withColumn("event_date", F.to_date("event_time"))

    users_dim = F.broadcast(users.select("user_id", "country"))

    return (
        purchases
        .join(users_dim, on="user_id", how="left")
        .groupBy("event_date", "country")
        .agg(F.sum("amount").alias("revenue"))
        .select("event_date", "country", "revenue")
    )
```

## Output Expectations

When producing PySpark ETL code:
- Include missing imports
- Return readable, production-oriented DataFrame code
- Prefer explicit schemas, explicit join types, and explicit write semantics
- Explain performance-sensitive choices briefly
- Call out assumptions about data volume, key uniqueness, skew, partitioning, and overwrite behavior
- Suggest `explain`, tests, or metric checks when performance or correctness depends on runtime data characteristics

## References to Consult When Needed

- Apache Spark SQL Performance Tuning: https://spark.apache.org/docs/latest/sql-performance-tuning.html
- PySpark Arrow and pandas UDF guide: https://spark.apache.org/docs/3.5.5/api/python/user_guide/sql/arrow_pandas.html
- PySpark API docs for pandas UDFs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html
