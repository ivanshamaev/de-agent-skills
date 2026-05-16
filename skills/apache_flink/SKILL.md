---
name: apache-flink
description: Apache Flink — streaming architecture (JobManager/TaskManager/slots/parallelism), PyFlink Table API and SQL (Kafka/filesystem DDL, windowing TUMBLE/HOP/SESSION, aggregations), DataStream API (KeyedStream, stateful operations ValueState/MapState, process functions), event-time vs processing-time, watermark strategies, checkpointing (exactly-once, RocksDB state backend), Kafka source/sink (delivery guarantees), backpressure, savepoints, deployment (standalone/YARN/Kubernetes)
---

# Apache Flink

## When to Use

Load this skill when the user needs to:
- Build low-latency stateful streaming pipelines (CDC, real-time aggregations, event-driven processing)
- Use event-time processing, watermarks, and out-of-order event handling
- Write PyFlink SQL or Table API pipelines with Kafka sources/sinks
- Implement stateful operations (session detection, deduplication, windowed aggregation)
- Configure checkpointing, state backends, and exactly-once semantics
- Deploy Flink jobs on YARN, Kubernetes, or standalone clusters

---

## Architecture

```
Client ──► JobManager (master)
              ├─ JobGraph compilation
              ├─ Task scheduling
              ├─ Checkpoint coordination
              └─ REST API (:8081)

TaskManagers (workers) — N instances
  └─ Task Slots (parallelism units)
       └─ Subtask (one parallel instance of an operator)

Parallelism: each operator runs as P subtasks (default = slot count)
             P per operator can be set independently

State Backend:
  ├─ HashMapStateBackend (default) — JVM heap, fast, size-limited
  └─ EmbeddedRocksDBStateBackend  — local SSD, unbounded state, slower reads
```

---

## Setup

### Install (PyFlink)

```bash
pip install apache-flink==1.20.0
# Requires Java 11 on PATH
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### Maven / Gradle coordinates for JARs

```
# Kafka connector (add to flink/lib/ or via --jars)
org.apache.flink:flink-connector-kafka:3.3.0-1.20
org.apache.flink:flink-sql-connector-kafka:3.3.0-1.20   # Table API

# Iceberg Flink runtime
org.apache.iceberg:iceberg-flink-runtime-1.20:1.7.0

# Avro
org.apache.flink:flink-avro:1.20.0
```

---

## Table API & SQL (Recommended for ETL / Analytics)

### Environment Setup

```python
from pyflink.table import TableEnvironment, EnvironmentSettings

# Streaming mode
t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

# Batch mode (bounded sources)
t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

# Parallelism
t_env.get_config().set("parallelism.default", "4")

# Checkpoint (required for production)
t_env.get_config().set("execution.checkpointing.interval", "60000")       # 60s
t_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")
t_env.get_config().set("execution.checkpointing.timeout", "300000")        # 5 min
t_env.get_config().set("execution.checkpointing.min-pause", "30000")       # 30s between checkpoints
t_env.get_config().set("state.backend", "rocksdb")
t_env.get_config().set("state.checkpoints.dir", "s3://flink-checkpoints/my-job/")
t_env.get_config().set("state.savepoints.dir", "s3://flink-savepoints/")

# Add connector JAR at runtime
t_env.get_config().set(
    "pipeline.jars",
    "file:///opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar"
)
```

### Kafka Source — DDL

```python
t_env.execute_sql("""
CREATE TABLE orders_kafka (
    order_id    BIGINT,
    user_id     BIGINT,
    amount      DOUBLE,
    status      STRING,
    event_time  TIMESTAMP_LTZ(3),   -- Kafka record timestamp (milliseconds)

    -- Metadata columns
    kafka_offset   BIGINT  METADATA FROM 'offset'    VIRTUAL,
    kafka_part     INT     METADATA FROM 'partition'  VIRTUAL,
    kafka_ts       TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,

    -- Event-time watermark: tolerate 30s late arrivals
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
)
WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'orders',
    'properties.bootstrap.servers'  = 'broker:9092',
    'properties.group.id'           = 'flink-orders-consumer',
    'scan.startup.mode'             = 'earliest-offset',
    'value.format'                  = 'json',
    'value.json.ignore-parse-errors' = 'true'
)
""")
```

**Kafka source options:**

| Option | Description |
|--------|-------------|
| `scan.startup.mode` | `group-offsets` / `earliest-offset` / `latest-offset` / `timestamp` / `specific-offsets` |
| `scan.startup.timestamp-millis` | Epoch ms when `scan.startup.mode=timestamp` |
| `properties.*` | Pass any Kafka consumer property via this prefix |
| `value.format` | `json` / `avro` / `avro-confluent` / `debezium-json` / `canal-json` / `csv` |
| `key.format` | Format for message key |
| `key.fields` | Semicolon-separated key fields |

### Kafka Source — Debezium CDC Format

```python
t_env.execute_sql("""
CREATE TABLE customers_cdc (
    customer_id  BIGINT,
    name         STRING,
    email        STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
)
WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'cdc.public.customers',
    'properties.bootstrap.servers'  = 'broker:9092',
    'properties.group.id'           = 'flink-cdc-consumer',
    'scan.startup.mode'             = 'earliest-offset',
    'value.format'                  = 'debezium-json'
)
""")
-- Flink automatically applies INSERT/UPDATE/DELETE semantics from Debezium envelope
```

### Kafka Sink — DDL

```python
t_env.execute_sql("""
CREATE TABLE orders_enriched_kafka (
    order_id    BIGINT,
    user_id     BIGINT,
    amount      DOUBLE,
    status      STRING,
    region      STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
)
WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'orders-enriched',
    'properties.bootstrap.servers'  = 'broker:9092',
    'key.format'                    = 'json',
    'key.fields'                    = 'order_id',
    'value.format'                  = 'json',
    'sink.delivery-guarantee'       = 'exactly-once',
    'sink.transactional-id-prefix'  = 'flink-orders-sink',
    'sink.parallelism'              = '4'
)
""")
```

**Sink delivery guarantees:**

| Mode | Semantics | Requires |
|------|-----------|----------|
| `none` | Best effort | — |
| `at-least-once` (default) | No loss, possible duplicates | — |
| `exactly-once` | No loss, no duplicates | Kafka transactions, `transactional-id-prefix` |

### Filesystem Source / Sink

```python
t_env.execute_sql("""
CREATE TABLE orders_parquet (
    order_id    BIGINT,
    user_id     BIGINT,
    amount      DOUBLE,
    status      STRING,
    dt          STRING
)
PARTITIONED BY (dt)
WITH (
    'connector' = 'filesystem',
    'path'      = 's3://datalake/bronze/orders/',
    'format'    = 'parquet',
    'sink.rolling-policy.rollover-interval'   = '10 min',
    'sink.rolling-policy.check-interval'      = '1 min',
    'sink.partition-commit.trigger'           = 'partition-time',
    'sink.partition-commit.delay'             = '1 h',
    'sink.partition-commit.policy.kind'       = 'success-file'
)
""")
```

### Iceberg Sink

```python
t_env.execute_sql("""
CREATE CATALOG iceberg_catalog WITH (
    'type'         = 'iceberg',
    'catalog-type' = 'rest',
    'uri'          = 'http://iceberg-rest:8181',
    'warehouse'    = 's3://datalake/'
)
""")

t_env.execute_sql("USE CATALOG iceberg_catalog")
t_env.execute_sql("USE silver")

# Write stream to Iceberg table
t_env.execute_sql("""
INSERT INTO silver.orders
SELECT order_id, user_id, amount, status, event_time
FROM orders_kafka
""")
```

---

## Windowing (SQL)

### TUMBLE — Non-overlapping fixed windows

```sql
-- Count orders per 5-minute tumbling window, per status
SELECT
    window_start,
    window_end,
    status,
    COUNT(*) AS cnt,
    SUM(amount) AS total_amount
FROM TABLE(
    TUMBLE(TABLE orders_kafka, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
)
GROUP BY window_start, window_end, status;
```

### HOP (Sliding) — Overlapping windows

```sql
-- 10-min windows sliding every 5 min (each event appears in 2 windows)
SELECT
    window_start,
    window_end,
    user_id,
    COUNT(*) AS cnt
FROM TABLE(
    HOP(TABLE orders_kafka, DESCRIPTOR(event_time),
        INTERVAL '5' MINUTES,   -- slide
        INTERVAL '10' MINUTES)  -- size
)
GROUP BY window_start, window_end, user_id;
```

### SESSION — Inactivity-based windows

```sql
-- User session window: new session after 10 min of inactivity
SELECT
    window_start,
    window_end,
    user_id,
    COUNT(*) AS events,
    SUM(amount) AS session_revenue
FROM TABLE(
    SESSION(TABLE orders_kafka, DESCRIPTOR(event_time),
            PARTITION BY user_id,
            GAP => INTERVAL '10' MINUTES)
)
GROUP BY window_start, window_end, user_id;
```

### Cumulate Window — Running totals within period

```sql
-- Running total within each day, updated every 10 min
SELECT
    window_start, window_end,
    status,
    COUNT(*) AS cnt
FROM TABLE(
    CUMULATE(TABLE orders_kafka, DESCRIPTOR(event_time),
             INTERVAL '10' MINUTES,  -- step
             INTERVAL '1' DAY)       -- max size
)
GROUP BY window_start, window_end, status;
```

---

## Watermarks

### In DDL

```python
t_env.execute_sql("""
CREATE TABLE events (
    user_id    BIGINT,
    event_time TIMESTAMP_LTZ(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (...)
""")

-- Bounded out-of-orderness: advance watermark = max_event_time - 10s
-- Any event with ts < watermark is "late" and may be dropped
```

### In DataStream API

```python
from pyflink.datastream import WatermarkStrategy
from pyflink.common.watermark_strategy import Duration

# Bounded out-of-orderness (10 second tolerance)
watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(10))
    .with_timestamp_assigner(
        lambda event, _: event.event_time_ms  # return epoch ms
    )
    .with_idle_source_timeout(Duration.of_minutes(5))  # advance watermark if source goes idle
)
```

---

## DataStream API (Stateful / Low-Level)

### Basic Pipeline

```python
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(4)
env.get_checkpoint_config().enable_checkpointing(60_000)  # 60s interval
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# Add connector JAR
env.add_jars("file:///opt/flink/lib/flink-connector-kafka-3.3.0-1.20.jar")

# Kafka source
source = (
    KafkaSource.builder()
    .set_bootstrap_servers("broker:9092")
    .set_topics("orders")
    .set_group_id("flink-datastream-consumer")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

stream = env.from_source(
    source=source,
    watermark_strategy=WatermarkStrategy.no_watermarks(),
    source_name="orders-kafka",
)
```

### KeyedStream + Stateful ProcessFunction

```python
from pyflink.datastream import KeyedProcessFunction, TimerService
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types
import json

class OrderSessionDetector(KeyedProcessFunction):
    """Detects order sessions: emits session summary after 10 min inactivity per user."""

    SESSION_TIMEOUT_MS = 10 * 60 * 1000  # 10 minutes

    def open(self, runtime_context):
        # State: last seen event time per user
        self.last_event_state = runtime_context.get_state(
            ValueStateDescriptor("last_event_ts", Types.LONG())
        )
        # State: order count in current session
        self.order_count_state = runtime_context.get_state(
            ValueStateDescriptor("order_count", Types.INT())
        )

    def process_element(self, value, ctx: KeyedProcessFunction.Context, out):
        order = json.loads(value)
        current_ts = ctx.timestamp()

        # Update state
        self.last_event_state.update(current_ts)
        count = self.order_count_state.value() or 0
        self.order_count_state.update(count + 1)

        # Register/reset timer for session expiry
        ctx.timer_service().register_event_time_timer(
            current_ts + self.SESSION_TIMEOUT_MS
        )

    def on_timer(self, timestamp, ctx: KeyedProcessFunction.OnTimerContext, out):
        last_ts = self.last_event_state.value()
        if last_ts is None:
            return
        # Timer fired after inactivity — emit session summary
        if timestamp >= last_ts + self.SESSION_TIMEOUT_MS:
            user_id = ctx.get_current_key()
            count = self.order_count_state.value() or 0
            out.collect(json.dumps({
                "user_id": user_id,
                "orders_in_session": count,
                "session_end_ts": timestamp,
            }))
            # Clear state for next session
            self.last_event_state.clear()
            self.order_count_state.clear()

# Apply:
sessions = (
    stream
    .map(lambda x: x, output_type=Types.STRING())
    .key_by(lambda x: json.loads(x)["user_id"])
    .process(OrderSessionDetector(), output_type=Types.STRING())
)
sessions.print()
env.execute("order-session-detector")
```

### Windowed Aggregation (DataStream API)

```python
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows, EventTimeSessionWindows
from pyflink.common import Time

keyed = (
    stream
    .key_by(lambda x: json.loads(x)["status"])
)

# Tumbling 5-minute event-time window
windowed = (
    keyed
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .reduce(lambda a, b: json.dumps({
        "status": json.loads(a)["status"],
        "count": json.loads(a).get("count", 1) + json.loads(b).get("count", 1),
    }))
)

# Sliding 10-min window, slide 5 min
sliding = keyed.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(5)))

# Session window with 10-min gap
session = keyed.window(EventTimeSessionWindows.with_gap(Time.minutes(10)))
```

### State Types

```python
from pyflink.datastream.state import (
    ValueStateDescriptor,
    MapStateDescriptor,
    ListStateDescriptor,
    ReducingStateDescriptor,
    AggregatingStateDescriptor,
)
from pyflink.common import Types

# ValueState — single value per key
self.counter = runtime_context.get_state(
    ValueStateDescriptor("counter", Types.LONG())
)

# MapState — key-value map per key
self.window_counts = runtime_context.get_map_state(
    MapStateDescriptor("window_counts", Types.STRING(), Types.LONG())
)

# ListState — list per key
self.events = runtime_context.get_list_state(
    ListStateDescriptor("events", Types.STRING())
)

# Accessing state:
value = self.counter.value()      # None if not set
self.counter.update(value + 1)
self.counter.clear()              # remove from state backend
```

---

## Checkpointing & Fault Tolerance

```python
from pyflink.datastream import CheckpointingMode, RestartStrategies

# In StreamExecutionEnvironment
env.get_checkpoint_config().enable_checkpointing(60_000)   # interval ms
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_checkpoint_timeout(300_000)
env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
env.get_checkpoint_config().enable_externalized_checkpoints(
    ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION  # keep checkpoint on cancel
)

# Restart strategy
env.set_restart_strategy(
    RestartStrategies.fixed_delay_restart(
        restart_attempts=3,
        delay_between_attempts=10_000,  # 10 seconds
    )
)
# Or exponential backoff:
env.set_restart_strategy(
    RestartStrategies.exponential_delay_restart(
        initial_backoff=10_000,
        max_backoff=600_000,
        backoff_multiplier=2.0,
        reset_backoff_threshold=3_600_000,
        jitter_factor=0.1,
    )
)
```

```python
# In Table API (config keys)
t_env.get_config().set("execution.checkpointing.interval",     "60000")
t_env.get_config().set("execution.checkpointing.mode",         "EXACTLY_ONCE")
t_env.get_config().set("execution.checkpointing.timeout",      "300000")
t_env.get_config().set("state.backend",                        "rocksdb")
t_env.get_config().set("state.checkpoints.dir",                "s3://checkpoints/job/")
t_env.get_config().set("state.savepoints.dir",                 "s3://savepoints/")
```

### State Backends

| Backend | Storage | Use Case |
|---------|---------|----------|
| `hashmap` (default) | JVM heap | Small state (< millions of keys) |
| `rocksdb` | Local SSD + checkpoints to S3/HDFS | Large state (billions of keys), production |

### Savepoints (Versioned Checkpoints)

```bash
# Trigger savepoint while job runs
flink savepoint <job-id> s3://savepoints/my-job/manual-sp-1

# Cancel job and create savepoint atomically
flink cancel --withSavepoint s3://savepoints/my-job/ <job-id>

# Resume from savepoint (allows code changes)
flink run -s s3://savepoints/my-job/savepoint-xyz my_job.jar

# List running jobs
flink list

# Get job overview
curl http://jobmanager:8081/jobs/<job-id>
```

---

## SQL Streaming Pipeline — End-to-End Example

```python
from pyflink.table import TableEnvironment, EnvironmentSettings

t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
t_env.get_config().set("parallelism.default", "4")
t_env.get_config().set("execution.checkpointing.interval", "60000")
t_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")
t_env.get_config().set("state.backend", "rocksdb")
t_env.get_config().set("state.checkpoints.dir", "s3://checkpoints/orders-agg/")

# Kafka source
t_env.execute_sql("""
CREATE TABLE orders_source (
    order_id   BIGINT,
    user_id    BIGINT,
    amount     DOUBLE,
    status     STRING,
    event_time TIMESTAMP_LTZ(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'orders',
    'properties.bootstrap.servers' = 'broker:9092',
    'properties.group.id'          = 'flink-orders-agg',
    'scan.startup.mode'            = 'latest-offset',
    'value.format'                 = 'json'
)
""")

# Kafka sink
t_env.execute_sql("""
CREATE TABLE orders_agg_sink (
    window_start TIMESTAMP_LTZ(3),
    window_end   TIMESTAMP_LTZ(3),
    status       STRING,
    cnt          BIGINT,
    total_amount DOUBLE
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'orders-agg-5min',
    'properties.bootstrap.servers' = 'broker:9092',
    'value.format'                 = 'json'
)
""")

# Windowed aggregation query
statement_set = t_env.create_statement_set()
statement_set.add_insert_sql("""
INSERT INTO orders_agg_sink
SELECT
    window_start,
    window_end,
    status,
    COUNT(*) AS cnt,
    SUM(amount) AS total_amount
FROM TABLE(
    TUMBLE(TABLE orders_source, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
)
GROUP BY window_start, window_end, status
""")

# Submit multiple inserts as a single job
job_client = statement_set.execute().get_job_client()
print(f"Job ID: {job_client.get_job_id()}")
```

---

## Deployment

### Standalone / Docker

```yaml
# docker-compose.yml
version: "3.9"
services:
  jobmanager:
    image: flink:1.20-scala_2.12-java11
    ports: ["8081:8081"]
    command: jobmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
        state.backend: rocksdb
        state.checkpoints.dir: s3://checkpoints/
        s3.endpoint: http://minio:9000
        s3.access-key: minioadmin
        s3.secret-key: minioadmin
        s3.path.style.access: true

  taskmanager:
    image: flink:1.20-scala_2.12-java11
    depends_on: [jobmanager]
    command: taskmanager
    scale: 2
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
        taskmanager.memory.process.size: 2048m
```

### Submit Job

```bash
# Submit Python job
flink run --python my_job.py \
  --pyFiles utils.py \
  -p 4 \
  -Dexecution.checkpointing.interval=60000

# Submit fat JAR
flink run -c com.example.MyJob my_job.jar --arg1 value1

# Submit with savepoint
flink run -s s3://savepoints/my-job/savepoint-xyz my_job.jar

# Kubernetes operator (KubernetesApplicationClusterEntrypoint)
flink run-application -t kubernetes-application \
  -Dkubernetes.cluster-id=my-flink-job \
  -Dkubernetes.container.image=my-flink:1.20 \
  local:///opt/flink/usrlib/my_job.jar
```

---

## Monitoring & Backpressure

```bash
# REST API — check job status
curl http://jobmanager:8081/jobs/overview

# Per-vertex backpressure status (0.0 = none, 1.0 = fully backpressured)
curl http://jobmanager:8081/jobs/<job-id>/vertices/<vertex-id>/backpressure

# Checkpoint statistics
curl http://jobmanager:8081/jobs/<job-id>/checkpoints

# Task manager metrics
curl http://jobmanager:8081/taskmanagers/<tm-id>/metrics
```

**Backpressure causes:**
- Slow sink (DB/Kafka writes slower than source produces)
- Heavy state access (RocksDB compaction, large value reads)
- GC pressure (increase task manager heap)
- CPU saturation (increase parallelism)

**Backpressure fixes:**
1. Increase sink parallelism independently: `stream.sink(...).setParallelism(8)`
2. Enable RocksDB for state-heavy operators
3. Increase `taskmanager.memory.managed.fraction` for RocksDB
4. Scale out task managers

---

## Best Practices

1. **Always enable checkpointing** with EXACTLY_ONCE and persistent storage (S3/HDFS) — without it, failures cause full replay.
2. **Use RocksDB state backend** for any job with large state (> 1M keys per operator).
3. **Declare WATERMARK in DDL** rather than programmatically — it integrates with the SQL optimizer.
4. **Use `INTERVAL '30' SECOND` watermark** as default; tune based on actual late-arrival distribution from source metrics.
5. **Set `with_idle_source_timeout`** — prevents watermark stalling when a Kafka partition receives no data.
6. **Prefer Table API / SQL** over DataStream for ETL pipelines — optimizer handles parallelism and state efficiently.
7. **Use savepoints for upgrades** — savepoints survive code changes; checkpoints may not.
8. **Separate source and sink parallelism** — Kafka source parallelism = number of topic partitions; sink can differ.
9. **Monitor checkpoint duration** — if checkpoint takes > 50% of checkpoint interval, state is too large or RocksDB needs tuning.
10. **Use `statement_set.execute()`** to submit multiple INSERT statements as a single Flink job (shared state, shared backpressure).

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| No checkpointing | Full state loss on restart; reprocesses from beginning | Enable with EXACTLY_ONCE and persistent dir |
| HashMapStateBackend with large state | OOM on task manager heap | Switch to RocksDB state backend |
| Missing watermark on event-time windows | Windows never close; unbounded state accumulation | Add `WATERMARK FOR event_time AS ...` in DDL |
| Watermark too tight (0s) | All out-of-order events dropped as "late" | Use at least 5-30s depending on source latency characteristics |
| Not setting `with_idle_source_timeout` | Watermark freezes when Kafka partition is idle | Set idle timeout ≥ expected max gap between events |
| One global parallelism for all operators | Kafka consumer bottleneck = partition count; sink may be slower | Set per-operator parallelism |
| Missing `NOT ENFORCED` on PRIMARY KEY | Flink requires it for upsert-capable tables | `PRIMARY KEY (id) NOT ENFORCED` |
| Using `.print()` in production | Single-threaded, no fault tolerance, no parallelism | Use proper Kafka/filesystem/Iceberg sink |
| Sharing savepoint path across jobs | One job's savepoint overwrites another's | Unique path per job |
| No restart strategy | Single failure kills the job permanently | Set fixed-delay or exponential-backoff restart |

---

## References to Consult When Needed

- [Apache Flink Documentation 1.20](https://nightlies.apache.org/flink/flink-docs-release-1.20/)
- [PyFlink Table API Tutorial](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/python/table_api_tutorial/)
- [Flink SQL Kafka Connector](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/kafka/)
- [Flink Windowing](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/)
- [Flink State & Fault Tolerance](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/stateful-stream-processing/)
- [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.10/)
