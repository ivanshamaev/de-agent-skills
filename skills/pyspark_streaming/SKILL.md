---
name: pyspark-structured-streaming
description: PySpark Structured Streaming — streaming sources (Kafka/file/rate), output modes (append/complete/update), triggers (ProcessingTime/AvailableNow/Once/Continuous), watermarks, event-time windows (tumbling/sliding/session), stateful aggregations, deduplication, foreachBatch, stream-stream joins, checkpointing, fault tolerance, RocksDB state store, Kafka source/sink options, production patterns
---

# PySpark Structured Streaming

## When to Use

Load this skill when the user needs to:
- Build streaming pipelines with PySpark (Kafka → Iceberg, Kafka → Delta, file watching, etc.)
- Configure watermarks and event-time windowing
- Handle late data, deduplication, and stateful aggregations
- Write to Kafka, Delta Lake, Iceberg, or other sinks from a stream
- Debug streaming queries, monitor lag, and tune checkpointing

---

## Core Model

```
Streaming = unbounded table that grows row by row
           ┌─────────────────────────────┐
Source ──► │  Infinite Input Table       │
           │  (new rows = new events)    │
           └────────────┬────────────────┘
                        │ trigger
                        ▼
                 Incremental Query
                        │
                        ▼
           ┌─────────────────────────────┐
           │  Result Table               │  Output mode determines
           │  (updated every trigger)    │  what gets written to sink
           └─────────────────────────────┘
```

Every `StreamingQuery` maintains a **checkpoint** directory — offsets read, state snapshots, committed sink batches. Restart resumes from exactly this point → **exactly-once delivery** when sink is idempotent.

---

## SparkSession Setup

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("orders-streaming")
    # Kafka connector (Maven coordinate)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")
    # RocksDB state store for large stateful workloads
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    # Adaptive query execution
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

---

## Sources

### Kafka Source

```python
from pyspark.sql.functions import col, from_json, schema_of_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# Schema for JSON value
order_schema = StructType([
    StructField("order_id",   LongType(),      False),
    StructField("user_id",    LongType(),      False),
    StructField("amount",     StringType(),    True),
    StructField("status",     StringType(),    True),
    StructField("event_time", TimestampType(), True),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "orders")                    # or subscribePattern for regex
    .option("startingOffsets", "latest")              # latest | earliest | JSON offset map
    .option("maxOffsetsPerTrigger", 50_000)           # rate-limit per micro-batch
    .option("failOnDataLoss", "false")                # don't fail if topic is compacted
    .option("kafka.session.timeout.ms", "30000")
    .option("kafka.max.poll.records", "500")
    .option("includeHeaders", "true")                 # expose Kafka headers column
    .load()
)

# Kafka DataFrame schema: key(binary), value(binary), topic, partition, offset, timestamp, headers
orders = (
    raw
    .selectExpr("CAST(key AS STRING) AS order_key", "CAST(value AS STRING) AS json_value",
                "timestamp AS kafka_ts", "partition", "offset")
    .withColumn("data", from_json(col("json_value"), order_schema))
    .select("order_key", "kafka_ts", "offset", "partition", "data.*")
)
```

**Kafka source options:**

| Option | Default | Notes |
|--------|---------|-------|
| `subscribe` | — | Comma-separated topic list |
| `subscribePattern` | — | Java regex over topic names |
| `assign` | — | JSON `{"topic":[0,1,2]}` for specific partitions |
| `startingOffsets` | `latest` (stream) / `earliest` (batch) | |
| `maxOffsetsPerTrigger` | unlimited | Cap throughput per trigger |
| `minOffsetsPerTrigger` | 0 | Minimum before trigger fires |
| `maxTriggerDelay` | 15m | Max wait even if minOffsets not met |
| `failOnDataLoss` | `true` | Set `false` for compacted topics |
| `kafka.group.id` | auto | Override Spark's managed consumer group |

### File Source (Auto-Ingest)

```python
# Watch a directory for new Parquet files — triggers on new files only
file_stream = (
    spark.readStream
    .schema(order_schema)
    .option("maxFilesPerTrigger", 10)    # process up to 10 files per batch
    .option("latestFirst", "false")
    .parquet("s3a://datalake/landing/orders/")
)
```

### Rate Source (Testing / Benchmarking)

```python
rate_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1000)
    .option("numPartitions", 4)
    .load()
)
# Columns: timestamp (event-time), value (monotonically increasing long)
```

---

## Output Modes

| Mode | When to Use | Constraints |
|------|-------------|-------------|
| `append` | Row written once, never updated | Requires watermark for windowed aggregations; no aggregation on unbounded data |
| `update` | Only changed rows written each trigger | Aggregations allowed; no sort |
| `complete` | Full result table written each trigger | Aggregations only; **do not use for large result tables** |

```python
# append — for raw event logging, windowed aggs with watermark
query = stream.writeStream.outputMode("append").format("delta").start(path)

# update — for aggregations that need live updates (dashboards)
query = stream.writeStream.outputMode("update").format("console").start()

# complete — for global aggregations (total counts, global topN)
query = stream.writeStream.outputMode("complete").format("memory").queryName("totals").start()
```

---

## Triggers

```python
from pyspark.sql.streaming import Trigger

# ProcessingTime — fixed micro-batch interval
.trigger(processingTime="30 seconds")
.trigger(processingTime="1 minute")

# AvailableNow — process all available data, then stop (replaces One-time trigger)
# Use for scheduled batch-style streaming jobs
.trigger(availableNow=True)

# Once — deprecated in favor of AvailableNow (same semantics, single batch)
.trigger(once=True)

# Continuous — experimental low-latency mode (~1ms); limited operations
.trigger(continuous="1 second")
```

**Rule**: use `availableNow=True` for Airflow-orchestrated streaming jobs (bounded run). Use `processingTime` for always-on streaming.

---

## Watermarks & Event-Time Windows

### Watermark Mechanics

```
Watermark = max(event_time seen) - watermark_delay

Window closes (and output emitted in append mode) when:
  watermark > window_end_time

Late records with event_time < watermark are DROPPED.
```

```python
from pyspark.sql.functions import col, window, avg, count, sum as spark_sum

# Define watermark on event-time column
windowed_agg = (
    orders
    .withWatermark("event_time", "10 minutes")   # tolerate up to 10 min late arrivals
    .groupBy(
        window(col("event_time"), "5 minutes"),  # tumbling window
        col("status"),
    )
    .agg(
        count("*").alias("cnt"),
        spark_sum("amount").alias("total_amount"),
    )
)
```

### Tumbling Window

```python
# Non-overlapping, fixed-size windows
from pyspark.sql.functions import window

df.groupBy(window("event_time", "10 minutes")).agg(count("*"))
# Window column: window.start, window.end
```

### Sliding Window

```python
# Overlapping windows — duration=10min, slide=5min
df.groupBy(window("event_time", "10 minutes", "5 minutes")).agg(count("*"))
```

### Session Window

```python
# Variable-size windows — gap closes after inactivity
from pyspark.sql.functions import session_window

df.groupBy(
    session_window("event_time", "5 minutes"),  # new session after 5 min silence
    col("user_id"),
).agg(count("*").alias("events_in_session"))
```

### Output Mode with Watermarks

| Combination | Supported | Notes |
|---|---|---|
| Windowed agg + watermark + `append` | ✅ | Results emitted once window is closed past watermark |
| Windowed agg + watermark + `update` | ✅ | Partial results emitted each trigger, state trimmed by watermark |
| Windowed agg + **no** watermark + `append` | ❌ | Error: append requires watermark for bounded state |
| Any agg + `complete` | ✅ | Full table every trigger; watermark not required but state never trimmed |

---

## Streaming Deduplication

```python
# Deduplicate within watermark window
# Records with same (order_id, event_time) within 1 hour are deduplicated
deduplicated = (
    orders
    .withWatermark("event_time", "1 hour")
    .dropDuplicates(["order_id", "event_time"])  # dedup keys
)

# Without watermark: dedup across entire stream (state grows unbounded — only for small key spaces)
deduplicated_global = orders.dropDuplicates(["order_id"])
```

---

## foreachBatch — Custom Sink Logic

The most powerful pattern: process each micro-batch as a static DataFrame. Enables multi-sink writes, upserts, and custom transformations.

```python
from delta.tables import DeltaTable

def upsert_to_delta(batch_df, batch_id):
    """Merge streaming micro-batch into Delta Lake table."""
    # batch_df is a standard static DataFrame
    batch_df.persist()   # cache if reusing multiple times

    # Deduplicate within batch first (micro-batch may have duplicates)
    deduped = (
        batch_df
        .dropDuplicates(["order_id"])
        .filter(col("order_id").isNotNull())
    )

    target = DeltaTable.forPath(spark, "s3a://datalake/silver/orders")
    (
        target.alias("t")
        .merge(deduped.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdate(set={
            "status":     "s.status",
            "amount":     "s.amount",
            "updated_at": "s.event_time",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    batch_df.unpersist()

query = (
    orders
    .writeStream
    .outputMode("update")           # update mode with foreachBatch
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "s3a://checkpoints/orders-silver/")
    .trigger(processingTime="1 minute")
    .start()
)
```

### foreachBatch: Multi-Sink Write

```python
def write_to_multiple_sinks(batch_df, batch_id):
    batch_df.persist()

    # Sink 1: Delta Lake silver
    batch_df.write.format("delta").mode("append").save("s3a://silver/orders/")

    # Sink 2: Kafka output topic (transform + forward)
    (
        batch_df
        .selectExpr("CAST(order_id AS STRING) AS key",
                    "to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("topic", "orders-enriched")
        .save()
    )

    # Sink 3: Metrics table (aggregation)
    (
        batch_df
        .groupBy("status")
        .agg(count("*").alias("cnt"))
        .write.format("delta").mode("append").save("s3a://gold/order_metrics/")
    )

    batch_df.unpersist()

query = (
    orders.writeStream
    .foreachBatch(write_to_multiple_sinks)
    .option("checkpointLocation", "s3a://checkpoints/orders-multi/")
    .start()
)
```

---

## Kafka Sink

```python
# Write stream to Kafka topic
(
    orders
    .selectExpr(
        "CAST(order_id AS STRING) AS key",
        "to_json(struct(order_id, user_id, amount, status, event_time)) AS value",
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("topic", "orders-processed")
    .option("kafka.acks", "all")
    .option("kafka.enable.idempotence", "true")
    .option("checkpointLocation", "s3a://checkpoints/orders-kafka-sink/")
    .trigger(processingTime="30 seconds")
    .start()
)
```

---

## Delta Lake / Iceberg Sink

```python
# Delta Lake — append
(
    orders.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3a://checkpoints/orders/")
    .option("mergeSchema", "true")           # auto schema evolution
    .partitionBy("dt")                       # partition by date column
    .trigger(processingTime="5 minutes")
    .start("s3a://datalake/silver/orders/")
)

# Iceberg — append via foreachBatch (recommended for Iceberg + Spark)
def write_to_iceberg(batch_df, batch_id):
    batch_df.writeTo("silver.orders").append()

(
    orders.writeStream
    .foreachBatch(write_to_iceberg)
    .option("checkpointLocation", "s3a://checkpoints/orders-iceberg/")
    .trigger(processingTime="5 minutes")
    .start()
)
```

---

## Stream–Stream Joins

```python
from pyspark.sql.functions import expr

orders  = spark.readStream.format("kafka").option(...).load()
payments = spark.readStream.format("kafka").option(...).load()

# Both streams need watermarks for state cleanup
orders_wm  = orders.withWatermark("order_time", "1 hour")
payments_wm = payments.withWatermark("payment_time", "30 minutes")

joined = orders_wm.join(
    payments_wm,
    expr("""
        orders.order_id = payments.order_id AND
        payments.payment_time BETWEEN
            orders.order_time AND
            orders.order_time + INTERVAL 30 MINUTES
    """),
    "inner",   # inner | leftOuter | full (limited support)
)
```

**Global watermark rule**: the combined watermark = min(stream_A_watermark, stream_B_watermark). A slow stream delays the other's state cleanup. Set `spark.sql.streaming.multipleWatermarkPolicy=max` (drops data from slow stream) only when latency > data completeness.

---

## Checkpointing & Fault Tolerance

```python
# Checkpoint = commit log of: source offsets + state snapshots + sink commits
# Location MUST be persistent storage (HDFS, S3, GCS — not local fs in cluster mode)

(
    stream.writeStream
    .option("checkpointLocation", "s3a://checkpoints/my-job/v1/")
    # Change checkpoint path = restart from scratch with new state
    # Keep same path = resume from last committed batch
    .start()
)
```

**Checkpoint directory layout:**
```
checkpoints/my-job/v1/
  commits/          # committed batch IDs
  offsets/          # source offsets per batch
  sources/          # source metadata
  state/            # stateful operator snapshots (RocksDB files)
  metadata          # query metadata
```

**WAL (Write-Ahead Log)**: Spark writes source offsets *before* processing. On restart, it re-reads from the last uncommitted offset → **at-least-once** for sources + **idempotent sink** = **exactly-once** end-to-end.

```python
# Schema changes: after changing schema, must either:
# 1. Migrate state (complex) — use same checkpoint with mergeSchema
# 2. Start fresh — change checkpoint path or delete state/ dir only
```

---

## Managing Streaming Queries

```python
# Start query and keep reference
query = stream.writeStream.format("delta").option(...).start()

# Check status
print(query.status)
# {'message': 'Processing new data', 'isDataAvailable': True, 'isTriggerActive': True}

# Last progress (metrics, watermark, lag)
import json
print(json.dumps(query.lastProgress, indent=2))
# {
#   "batchId": 42,
#   "numInputRows": 15000,
#   "inputRowsPerSecond": 1250.0,
#   "processedRowsPerSecond": 2100.0,
#   "eventTime": {"max": "2024-01-15T10:30:00", "watermark": "2024-01-15T10:20:00"},
#   "stateOperators": [{"numRowsTotal": 42000, "numRowsDroppedByWatermark": 120}],
#   "sources": [{"description": "KafkaV2[..orders..]", "endOffset": {"orders": {"0": 8150}}}]
# }

# Block until termination
query.awaitTermination()
query.awaitTermination(timeout=300)   # timeout in seconds

# Stop gracefully
query.stop()

# List all active queries
spark.streams.active

# Restart on failure pattern
while True:
    try:
        query.awaitTermination()
        break
    except Exception as e:
        logger.error(f"Query failed: {e}")
        query = stream.writeStream.option(...).start()
```

---

## RocksDB State Store (Production Stateful Pipelines)

Default in-memory state store exhausts executor memory for large key spaces. RocksDB offloads state to local SSD.

```python
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
)
# Block cache (default 8 MB — increase for large state)
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.blockCacheSizeMB", "512")
# Write buffer (default 64 MB)
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.writeBufferSizeMB", "128")
```

---

## Production Pipeline Pattern — Kafka → Silver Delta Lake

```python
import pendulum
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, DoubleType
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("orders-kafka-to-silver")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
            "io.delta:delta-spark_2.13:3.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    .getOrCreate()
)

ORDER_SCHEMA = StructType([
    StructField("order_id",   LongType(),      False),
    StructField("user_id",    LongType(),      False),
    StructField("amount",     DoubleType(),    True),
    StructField("status",     StringType(),    True),
    StructField("event_time", TimestampType(), True),
])

CHECKPOINT = "s3a://checkpoints/orders-silver/"
SILVER_PATH = "s3a://datalake/silver/orders/"
KAFKA_SERVERS = "broker1:9092,broker2:9092"

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 100_000)
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw
    .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts",
                "partition", "offset")
    .withColumn("d", from_json(col("json_str"), ORDER_SCHEMA))
    .select("kafka_ts", "partition", "offset", "d.*")
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("dt", to_date("event_time"))
    .filter(col("order_id").isNotNull())
)

def merge_to_silver(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    deduped = batch_df.dropDuplicates(["order_id"])

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        target = DeltaTable.forPath(spark, SILVER_PATH)
        (
            target.alias("t")
            .merge(deduped.alias("s"), "t.order_id = s.order_id")
            .whenMatchedUpdate(condition="s.event_time > t.event_time", set={
                "status": "s.status", "amount": "s.amount",
                "event_time": "s.event_time", "_ingest_ts": "s._ingest_ts",
            })
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        deduped.write.format("delta").mode("overwrite").partitionBy("dt").save(SILVER_PATH)

query = (
    parsed.writeStream
    .outputMode("update")
    .foreachBatch(merge_to_silver)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="1 minute")
    .start()
)
query.awaitTermination()
```

---

## Best Practices

1. **Always set `checkpointLocation`** on persistent storage (S3/HDFS). Without it, a restart reprocesses from beginning.
2. **Use `foreachBatch`** for any non-trivial sink: upserts, multi-sink writes, custom error handling, Delta MERGE.
3. **Use `withWatermark` + `dropDuplicates`** together for deduplication with bounded state.
4. **Set `maxOffsetsPerTrigger`** to cap batch size and keep processing latency predictable.
5. **Enable RocksDB state store** for stateful aggregations with large key spaces (> 1M keys).
6. **Monitor `stateOperators[].numRowsDroppedByWatermark`** — high numbers = watermark delay too short for your late data distribution.
7. **Trigger `availableNow=True`** for Airflow-scheduled jobs — processes all backlog and terminates cleanly.
8. **Persist batch_df** at the start of `foreachBatch` if using it more than once (multiple sinks).
9. **Separate checkpoint paths per query** — never share checkpoints between different queries.
10. **Schema evolution**: use `mergeSchema=true` on Delta sink; restart with new checkpoint path for breaking schema changes.
11. **Backpressure**: Structured Streaming has no built-in backpressure — control via `maxOffsetsPerTrigger` and horizontal scaling.

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| No checkpoint location | Full replay on restart, duplicated writes | Always set persistent `checkpointLocation` |
| `outputMode("complete")` on large stream | Full result table written every trigger — OOM | Use `append` + watermark or `update` |
| Windowed agg without watermark in `append` mode | AnalysisException at runtime | Add `withWatermark()` before `groupBy(window(...))` |
| Collecting results with `df.collect()` inside `foreachBatch` | Brings all data to driver | Never collect; always write via distributed APIs |
| Local file path for checkpoint | Fails in cluster mode (executor ≠ driver filesystem) | Use HDFS/S3/GCS paths |
| Missing `dropDuplicates` in `foreachBatch` | Kafka at-least-once delivery causes duplicates in Delta | Dedup on natural key before merge |
| `trigger(once=True)` on large backlog | Processes entire backlog in a single batch — OOM | Use `trigger(availableNow=True)` instead |
| Stream-stream join without time constraint | State grows unbounded | Always add event-time constraint in join condition |
| Same checkpoint path for different queries | Corrupted checkpoint metadata | One checkpoint path per unique query |
| Using `mapPartitions` / `foreach` instead of `foreachBatch` | Per-record overhead, no access to SparkSession | Use `foreachBatch` for batch semantics |

---

## References to Consult When Needed

- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Spark + Kafka Integration](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html)
- [Databricks Watermarking Deep Dive](https://www.databricks.com/blog/2022/08/22/feature-deep-dive-watermarking-apache-spark-structured-streaming.html)
- [Delta Lake Streaming](https://docs.delta.io/latest/delta-streaming.html)
