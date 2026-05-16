---
name: delta-lake
description: Delta Lake — table DDL (CREATE/PARTITIONED BY/LOCATION/TBLPROPERTIES), DML (INSERT/UPDATE/DELETE/MERGE), MERGE patterns (upsert/SCD2/CDC), schema evolution (mergeSchema/overwriteSchema/ALTER TABLE), OPTIMIZE compaction, Z-ORDER BY data skipping, VACUUM, Time Travel (VERSION AS OF/TIMESTAMP AS OF), RESTORE, DESCRIBE HISTORY, shallow/deep clone, Change Data Feed, streaming read/write, deletion vectors, table properties
---

# Delta Lake

## When to Use

Load this skill when the user needs to:
- Create and manage Delta tables (DDL, partitioning, table properties)
- Write DML: INSERT, UPDATE, DELETE, MERGE (upsert, SCD2, CDC)
- Compact files (OPTIMIZE), apply data skipping (Z-ORDER BY)
- Vacuum stale files, time travel, restore snapshots
- Evolve schemas without breaking existing readers
- Read Delta tables in streaming or batch mode

---

## SparkSession Setup

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("delta-pipeline")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
```

---

## Table DDL

### CREATE TABLE

```sql
-- Managed table (Hive/Unity catalog manages location)
CREATE TABLE silver.orders (
    order_id    BIGINT      NOT NULL,
    user_id     BIGINT      NOT NULL,
    amount      DECIMAL(18,2),
    status      STRING,
    event_time  TIMESTAMP,
    dt          DATE        GENERATED ALWAYS AS (CAST(event_time AS DATE)),
    _ingest_ts  TIMESTAMP
)
USING DELTA
PARTITIONED BY (dt)
TBLPROPERTIES (
    'delta.minReaderVersion'              = '2',
    'delta.minWriterVersion'              = '5',
    'delta.columnMapping.mode'            = 'name',    -- allows DROP/RENAME columns
    'delta.autoOptimize.optimizeWrite'    = 'true',    -- right-size files on write
    'delta.autoOptimize.autoCompact'      = 'true',    -- background compaction
    'delta.deletedFileRetentionDuration'  = 'interval 7 days',
    'delta.logRetentionDuration'          = 'interval 30 days',
    'delta.dataSkippingNumIndexedCols'    = '32'
);

-- External (unmanaged) table — survives DROP TABLE
CREATE TABLE silver.orders
USING DELTA
LOCATION 's3a://datalake/silver/orders/'
PARTITIONED BY (dt);
```

### CREATE TABLE AS SELECT (CTAS)

```sql
CREATE TABLE silver.orders_clean
USING DELTA
PARTITIONED BY (dt)
AS
SELECT order_id, user_id, CAST(amount AS DECIMAL(18,2)), status,
       event_time, CAST(event_time AS DATE) AS dt
FROM bronze.orders_raw
WHERE order_id IS NOT NULL;
```

### Python API

```python
from delta.tables import DeltaTable
from pyspark.sql.types import *

DeltaTable.createIfNotExists(spark) \
    .tableName("silver.orders") \
    .addColumn("order_id",   LongType(),         nullable=False) \
    .addColumn("user_id",    LongType(),         nullable=False) \
    .addColumn("amount",     DoubleType()) \
    .addColumn("status",     StringType()) \
    .addColumn("event_time", TimestampType()) \
    .addColumn("dt",         DateType(),
               generatedAlwaysAs="CAST(event_time AS DATE)") \
    .partitionedBy("dt") \
    .property("delta.autoOptimize.autoCompact", "true") \
    .execute()
```

---

## DML

### INSERT / APPEND

```python
# DataFrame append
df.write.format("delta").mode("append").saveAsTable("silver.orders")
df.write.format("delta").mode("append").save("s3a://datalake/silver/orders/")

# SQL INSERT
spark.sql("""
    INSERT INTO silver.orders
    SELECT order_id, user_id, amount, status, event_time
    FROM bronze.orders_raw
    WHERE dt = '2024-01-15'
""")
```

### Partition Overwrite

```python
# Static overwrite — replaces ALL partitions
df.write.format("delta").mode("overwrite").save(path)

# Dynamic overwrite — replaces only partitions present in df
df.write.format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .saveAsTable("silver.orders")

# replaceWhere — overwrite a predicate-defined range
df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "dt = '2024-01-15'") \
    .save(path)
```

### UPDATE

```python
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "silver.orders")

# Update specific rows
dt.update(
    condition="status = 'pending' AND event_time < current_timestamp() - INTERVAL 7 DAYS",
    set={"status": "'expired'", "_ingest_ts": "current_timestamp()"},
)
```

```sql
UPDATE silver.orders
SET status = 'expired', _ingest_ts = current_timestamp()
WHERE status = 'pending' AND event_time < current_timestamp() - INTERVAL 7 DAYS;
```

### DELETE

```python
dt.delete("status = 'cancelled' AND dt < '2023-01-01'")
```

```sql
DELETE FROM silver.orders
WHERE status = 'cancelled' AND dt < '2023-01-01';
```

### MERGE — Upsert Pattern

```python
from delta.tables import DeltaTable

source = spark.table("staging.orders_latest")
target = DeltaTable.forName(spark, "silver.orders")

(
    target.alias("t")
    .merge(source.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdate(set={
        "status":     "s.status",
        "amount":     "s.amount",
        "event_time": "s.event_time",
        "_ingest_ts": "current_timestamp()",
    })
    .whenNotMatchedInsert(values={
        "order_id":   "s.order_id",
        "user_id":    "s.user_id",
        "amount":     "s.amount",
        "status":     "s.status",
        "event_time": "s.event_time",
        "_ingest_ts": "current_timestamp()",
    })
    .execute()
)
```

```sql
MERGE INTO silver.orders AS t
USING (
    SELECT order_id, user_id, amount, status, event_time
    FROM staging.orders_latest
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
    t.status     = s.status,
    t.amount     = s.amount,
    t.event_time = s.event_time,
    t._ingest_ts = current_timestamp()
WHEN NOT MATCHED THEN INSERT (order_id, user_id, amount, status, event_time, _ingest_ts)
    VALUES (s.order_id, s.user_id, s.amount, s.status, s.event_time, current_timestamp());
```

### MERGE — SCD Type 2

```python
from pyspark.sql.functions import current_timestamp, lit, expr

# Source: new/changed dimension records
new_data = spark.table("staging.customers_latest")

# Separate into: records that changed + new records (via union trick)
staged = new_data.selectExpr(
    "customer_id", "name", "email", "city",
    "md5(concat_ws('|', name, email, city)) AS row_hash",
)

# Step 1: Expire changed current records
(
    DeltaTable.forName(spark, "silver.dim_customers").alias("t")
    .merge(staged.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true AND t.row_hash != s.row_hash")
    .whenMatchedUpdate(set={
        "is_current":  "false",
        "valid_to":    "current_timestamp()",
    })
    .execute()
)

# Step 2: Insert new current versions
(
    new_data.alias("s")
    .join(
        spark.table("silver.dim_customers").filter("is_current = true").alias("t"),
        "s.customer_id = t.customer_id", "left_anti"  # not in current → new
    )
    .union(
        new_data.alias("s").join(
            spark.table("silver.dim_customers").filter("is_current = false").alias("t"),
            expr("s.customer_id = t.customer_id AND s.row_hash != t.row_hash")
        ).select("s.*")
    )
    .withColumn("is_current", lit(True))
    .withColumn("valid_from", current_timestamp())
    .withColumn("valid_to", lit(None).cast("timestamp"))
    .write.format("delta").mode("append").saveAsTable("silver.dim_customers")
)
```

### MERGE — CDC (Change Data Capture)

```python
from pyspark.sql.functions import col

# CDC source: rows with op_type = 'I' / 'U' / 'D'
cdc_df = spark.table("staging.orders_cdc")

# Keep only last operation per key
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

latest_cdc = (
    cdc_df
    .withColumn("rn", row_number().over(
        Window.partitionBy("order_id").orderBy(col("cdc_ts").desc())
    ))
    .filter("rn = 1")
    .drop("rn")
)

(
    DeltaTable.forName(spark, "silver.orders").alias("t")
    .merge(latest_cdc.alias("s"), "t.order_id = s.order_id")
    .whenMatchedDelete(condition="s.op_type = 'D'")
    .whenMatchedUpdate(
        condition="s.op_type IN ('U', 'I')",
        set={"status": "s.status", "amount": "s.amount", "event_time": "s.cdc_ts"},
    )
    .whenNotMatchedInsert(
        condition="s.op_type != 'D'",
        values={
            "order_id":   "s.order_id",
            "user_id":    "s.user_id",
            "amount":     "s.amount",
            "status":     "s.status",
            "event_time": "s.cdc_ts",
        },
    )
    # Delete from target rows no longer in source (full refresh only)
    # .whenNotMatchedBySourceDelete()
    .execute()
)
```

---

## Schema Evolution

```python
# Append new columns automatically
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save(path)

# Replace entire schema on overwrite
df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(path)

# Session-level auto-merge (for MERGE INTO schema evolution)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

```sql
-- Add column
ALTER TABLE silver.orders ADD COLUMN discount DECIMAL(10,2) AFTER amount;

-- Rename column (requires columnMapping.mode = 'name')
ALTER TABLE silver.orders RENAME COLUMN user_id TO customer_id;

-- Drop column (requires columnMapping.mode = 'name')
ALTER TABLE silver.orders DROP COLUMN legacy_field;

-- Change type (widening only: INT → LONG, FLOAT → DOUBLE, etc.)
ALTER TABLE silver.orders ALTER COLUMN amount TYPE DOUBLE;

-- Add comment
ALTER TABLE silver.orders ALTER COLUMN status COMMENT 'Order lifecycle status';
```

**Column mapping** (`delta.columnMapping.mode = 'name'`) must be enabled for RENAME and DROP. Enable on existing table:

```sql
ALTER TABLE silver.orders
SET TBLPROPERTIES (
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);
```

---

## OPTIMIZE & Z-ORDER BY

```sql
-- Compact small files in a specific partition
OPTIMIZE silver.orders WHERE dt = '2024-01-15';

-- Compact + cluster by frequently filtered columns (max 4 cols)
OPTIMIZE silver.orders WHERE dt >= '2024-01-01'
ZORDER BY (status, user_id);
```

```python
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "silver.orders")

# Compaction only
dt.optimize().executeCompaction()

# Compaction + Z-ORDER on a partition
dt.optimize().where("dt = '2024-01-15'").executeZOrderBy("status", "user_id")
```

**Z-ORDER tips:**
- Use on columns with high cardinality that appear in `WHERE` / `JOIN` predicates.
- Max 4 columns (effectiveness degrades beyond that).
- Re-run after significant data ingestion; not automatic.
- Combine with partition pruning: filter on partition column first, Z-ORDER for within-partition skipping.

**Auto Optimize** (set once, runs automatically):

```sql
ALTER TABLE silver.orders
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',  -- right-sizes files during write
    'delta.autoOptimize.autoCompact'   = 'true'   -- background compaction after write
);
```

---

## VACUUM

Removes files no longer referenced by any table version.

```sql
-- Dry run — see what would be deleted
VACUUM silver.orders DRY RUN;

-- Delete files older than 7 days (default retention)
VACUUM silver.orders;

-- Custom retention (minimum 168 hours = 7 days enforced by default)
VACUUM silver.orders RETAIN 240 HOURS;
```

```python
dt = DeltaTable.forName(spark, "silver.orders")
dt.vacuum(retentionHours=240)

# Bypass minimum retention (DANGEROUS — disables safety check)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
dt.vacuum(retentionHours=0)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
```

**Rule**: never VACUUM with retention < active streaming query lag. Streaming readers hold references to old files; VACUUM removes them → StreamingQueryException.

---

## Time Travel

```sql
-- Query by version
SELECT * FROM silver.orders VERSION AS OF 42;

-- Query by timestamp
SELECT * FROM silver.orders TIMESTAMP AS OF '2024-01-15 10:00:00';

-- Show full history
DESCRIBE HISTORY silver.orders;
DESCRIBE HISTORY silver.orders LIMIT 5;

-- Compare two versions
SELECT a.order_id, a.status AS old_status, b.status AS new_status
FROM silver.orders VERSION AS OF 10 a
JOIN silver.orders VERSION AS OF 20 b ON a.order_id = b.order_id
WHERE a.status != b.status;
```

```python
# Read historical version
df_old = (
    spark.read.format("delta")
    .option("versionAsOf", 10)
    .load("s3a://datalake/silver/orders/")
)

# Read by timestamp
df_ts = (
    spark.read.format("delta")
    .option("timestampAsOf", "2024-01-15")
    .table("silver.orders")
)

# Get history as DataFrame
dt = DeltaTable.forName(spark, "silver.orders")
history = dt.history()        # all versions
history = dt.history(10)      # last 10 versions
history.select("version", "timestamp", "operation", "operationParameters").show()
```

---

## RESTORE TABLE

```sql
-- Rollback to version
RESTORE TABLE silver.orders TO VERSION AS OF 15;

-- Rollback to timestamp
RESTORE TABLE silver.orders TO TIMESTAMP AS OF '2024-01-14 00:00:00';
```

```python
dt.restoreToVersion(15)
dt.restoreToTimestamp("2024-01-14T00:00:00")
```

RESTORE creates a new table version (not a destructive operation) — old version is preserved in history.

---

## Shallow & Deep Clone

```sql
-- Shallow clone: metadata copy, references same data files
-- Fast, zero data copy — use for testing, dev environments
CREATE TABLE dev.orders_test
SHALLOW CLONE silver.orders;

-- Clone at specific version
CREATE TABLE dev.orders_snapshot
SHALLOW CLONE silver.orders VERSION AS OF 100;

-- Deep clone: full data copy — use for full isolation or cross-region migration
CREATE TABLE archive.orders_2023
DEEP CLONE silver.orders
LOCATION 's3a://archive/orders_2023/'
WHERE dt < '2024-01-01';
```

**Shallow clone warning**: if source files are VACUUMed, the shallow clone breaks. Use deep clone for long-term archives.

---

## Change Data Feed (CDF)

Exposes row-level changes (insert/update_preimage/update_postimage/delete) as a queryable stream.

```sql
-- Enable on table
ALTER TABLE silver.orders
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

```python
# Batch read of changes between versions
changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .option("endingVersion", 10)    # or endingTimestamp
    .table("silver.orders")
)
# Extra columns: _change_type, _commit_version, _commit_timestamp

# Streaming CDF consumer
cdf_stream = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", "latest")
    .table("silver.orders")
)
```

CDF is useful for:
- Propagating changes downstream without full table scans
- Incremental Gold layer rebuilds
- Audit trails

---

## Streaming Read / Write

```python
# Read Delta as stream — picks up new commits automatically
stream = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 10)          # process 10 files per micro-batch
    .option("ignoreChanges", "true")           # ignore UPDATE/DELETE (append-only semantic)
    .table("silver.orders")
)

# Write stream to Delta
(
    stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3a://checkpoints/orders-gold/")
    .option("mergeSchema", "true")
    .trigger(processingTime="1 minute")
    .start("s3a://datalake/gold/orders/")
)
```

---

## Table Properties Reference

| Property | Default | Description |
|---|---|---|
| `delta.appendOnly` | `false` | Disallow UPDATE/DELETE/OVERWRITE |
| `delta.autoOptimize.autoCompact` | `false` | Background compaction after writes |
| `delta.autoOptimize.optimizeWrite` | `false` | Right-size files during write |
| `delta.columnMapping.mode` | `none` | `name` enables RENAME/DROP columns |
| `delta.dataSkippingNumIndexedCols` | 32 | Columns tracked for min/max statistics |
| `delta.deletedFileRetentionDuration` | 7 days | VACUUM default retention |
| `delta.enableChangeDataFeed` | `false` | Enable CDF row-level change tracking |
| `delta.logRetentionDuration` | 30 days | Transaction log retention |
| `delta.minReaderVersion` | `1` | Min protocol for readers (`2` for column mapping) |
| `delta.minWriterVersion` | `2` | Min protocol for writers (`5` for column mapping) |
| `delta.targetFileSize` | 128 MB | Target file size for OPTIMIZE |

---

## DESCRIBE Commands

```sql
-- Partition info, file counts, size, schema
DESCRIBE DETAIL silver.orders;

-- Column types, partitioning, metadata
DESCRIBE TABLE silver.orders;
DESCRIBE TABLE EXTENDED silver.orders;

-- Check table properties
SHOW TBLPROPERTIES silver.orders;
```

```python
DeltaTable.forName(spark, "silver.orders").detail().show(vertical=True)
```

---

## Maintenance Schedule (Airflow Example)

```python
from airflow.sdk import dag, task
import pendulum

@dag(
    schedule="0 3 * * 0",  # weekly on Sunday 3am
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["delta", "maintenance"],
)
def delta_maintenance():

    @task
    def optimize_silver_tables():
        from pyspark.sql import SparkSession
        from delta.tables import DeltaTable
        spark = SparkSession.builder.getOrCreate()
        tables = ["orders", "customers", "products"]
        for tbl in tables:
            DeltaTable.forName(spark, f"silver.{tbl}") \
                .optimize().executeZOrderBy("status", "user_id")

    @task
    def vacuum_all_tables():
        from pyspark.sql import SparkSession
        from delta.tables import DeltaTable
        spark = SparkSession.builder.getOrCreate()
        for tbl in ["bronze.orders_raw", "silver.orders", "gold.order_summary"]:
            DeltaTable.forName(spark, tbl).vacuum(retentionHours=168)

    optimize_silver_tables() >> vacuum_all_tables()

delta_maintenance()
```

---

## Best Practices

1. **Enable `autoOptimize.optimizeWrite=true`** — prevents small file problem at write time without extra jobs.
2. **Run OPTIMIZE + ZORDER BY weekly** on partitions written in the last 7 days; Z-ORDER by query predicates.
3. **Set `delta.columnMapping.mode=name`** from table creation — required for RENAME/DROP later; costly to add after data exists.
4. **Never VACUUM below 7-day retention** while streaming jobs are active — they hold references to old files.
5. **Use `replaceWhere` instead of full overwrite** for partition updates — preserves history and is faster.
6. **Enable CDF only on tables you need it on** — CDF doubles write volume (pre + post images).
7. **Set `logRetentionDuration=30 days`** to allow time travel and streaming re-reads.
8. **For MERGE, deduplicate source first** — duplicate keys in source cause non-deterministic behavior.
9. **Use generated columns for partition keys** (`CAST(event_time AS DATE)`) — partition pruning works automatically.
10. **Use shallow clone for dev/test environments** — zero cost, instant, safe for schema testing.

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Writing millions of tiny files | Read performance degrades; S3 LIST overhead | Enable `autoOptimize.optimizeWrite` + schedule OPTIMIZE |
| VACUUM with retention < streaming lag | Streaming reader fails — files deleted while in use | VACUUM retention ≥ max streaming checkpoint interval |
| Z-ORDER on > 4 columns | Effectiveness diminishes beyond 4; OPTIMIZE takes longer | Choose top 2-3 highest-selectivity predicates |
| OVERWRITE without `replaceWhere` or `dynamic` | Deletes ALL partitions, not just the target | Use `partitionOverwriteMode=dynamic` or `replaceWhere` |
| MERGE without partition filter | Full table scan on target for every merge | Add `WHERE dt = current_date()` in MERGE or add partition predicate |
| Enabling CDF on all tables | CDF doubles storage writes | Enable only on tables with downstream CDC consumers |
| `overwriteSchema=true` on live table | Breaks concurrent readers mid-schema-change | Use schema evolution (`mergeSchema=true`) or schedule downtime |
| Sharing table path across multiple writers | Concurrent writes corrupt `_delta_log` | Each table has exactly one writer; use MERGE for concurrent upserts |
| No `checkpointLocation` on streaming writer | Restart re-processes from beginning | Always set persistent checkpoint path for streaming |

---

## References to Consult When Needed

- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [Delta Lake Batch Reads & Writes](https://docs.delta.io/latest/delta-batch.html)
- [Delta Lake Update/Delete/Merge](https://docs.delta.io/latest/delta-update.html)
- [Delta Lake Optimizations](https://docs.delta.io/latest/optimizations-oss.html)
- [Delta Lake Utility Operations](https://docs.delta.io/latest/delta-utility.html)
- [Delta Lake Streaming](https://docs.delta.io/latest/delta-streaming.html)
