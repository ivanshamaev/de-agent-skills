---
name: starrocks-routine-load-kafka
description: StarRocks Routine Load for Kafka — CREATE ROUTINE LOAD DDL (all PROPERTIES/KAFKA clause parameters), JSON/CSV/Avro format config, desired_concurrent_number tuning, SHOW ROUTINE LOAD columns, PAUSE/RESUME/ALTER/STOP, consumer lag monitoring, error log analysis, exactly-once semantics, Schema Registry for Avro, Kafka SASL/SSL config, idempotent upsert to Primary Key table
---

# StarRocks Routine Load — Kafka Integration

## When to Use

- Continuous Kafka → StarRocks ingestion (sub-minute latency)
- CDC event stream landing into Primary Key tables
- Real-time metrics accumulation into Aggregate Key tables
- Replace Kafka Connect StarRocks sink (Routine Load is native, no JVM)

Not for: batch file ingestion (Broker Load), push from application code (Stream Load).

---

## Architecture

```
Kafka Topic
  [Partition 0] ──┐
  [Partition 1] ──┤── Routine Load Job (FE-managed)
  [Partition 2] ──┘        │
                            ├── Task 1 → BE 1
                            ├── Task 2 → BE 2
                            └── Task 3 → BE 3
                                    │
                               StarRocks Table
```

- FE assigns tasks to BEs; each task consumes one or more Kafka partitions
- Exactly-once: offset committed only after tablet commit
- Max concurrency = min(alive BEs, partition count, `desired_concurrent_number`)

---

## CREATE ROUTINE LOAD — Full Syntax

```sql
CREATE ROUTINE LOAD db_name.job_name ON table_name
[LOAD PROPERTIES]
PROPERTIES (
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "10",
    "max_batch_rows" = "200000",
    "max_error_number" = "1000",
    "max_filter_ratio" = "0.01",
    "format" = "json",
    "jsonpaths" = "[\"$.order_id\",\"$.customer_id\",\"$.amount\",\"$.created_at\"]",
    "columns" = "order_id,customer_id,amount,created_at",
    "strict_mode" = "true",
    "timezone" = "UTC"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka1:9092,kafka2:9092,kafka3:9092",
    "kafka_topic" = "orders_cdc",
    "kafka_partitions" = "0,1,2",
    "kafka_offsets" = "OFFSET_END,OFFSET_END,OFFSET_END",
    "property.group.id" = "starrocks_orders_consumer"
);
```

---

## PROPERTIES Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `desired_concurrent_number` | 3 | Target task concurrency (≤ partition count, ≤ alive BEs) |
| `max_batch_interval` | 10 | Max seconds between commits |
| `max_batch_rows` | 200000 | Max rows per task before commit |
| `max_batch_size` | 100MB | Max bytes per task |
| `max_error_number` | 0 | Max parse errors before pausing job |
| `max_filter_ratio` | 0 | Max fraction of filtered rows (0=fail on any error) |
| `format` | csv | `csv` / `json` / `avro` |
| `jsonpaths` | — | JSON field paths: `["$.field1","$.field2"]` |
| `columns` | all | Column mapping / expressions |
| `where` | — | Row filter expression |
| `strict_mode` | false | Reject type mismatches |
| `timezone` | `UTC` | Datetime parsing timezone |
| `partial_update` | false | Partial column update (PK table) |
| `strip_outer_array` | false | JSON: strip outer `[...]` |
| `json_root` | — | JSON: root path expression |

---

## KAFKA Clause Reference

| Parameter | Description |
|-----------|-------------|
| `kafka_broker_list` | Comma-separated `host:port` list |
| `kafka_topic` | Source topic name |
| `kafka_partitions` | Comma-separated partition IDs, or omit for all |
| `kafka_offsets` | Per-partition offsets: `OFFSET_BEGINNING`, `OFFSET_END`, or specific offset |
| `property.group.id` | Consumer group ID |
| `property.kafka_default_offsets` | Default offset for new partitions |
| `confluent.schema.registry.url` | Required for Avro format |
| `property.security.protocol` | `SASL_PLAINTEXT` / `SASL_SSL` / `SSL` |
| `property.sasl.mechanism` | `PLAIN` / `SCRAM-SHA-256` / `SCRAM-SHA-512` |
| `property.sasl.username` | SASL username |
| `property.sasl.password` | SASL password |

---

## Format Examples

### CSV from Kafka

```sql
CREATE ROUTINE LOAD sales.csv_job ON events
PROPERTIES (
    "desired_concurrent_number" = "4",
    "format" = "csv",
    "column_separator" = "|",
    "columns" = "event_id,user_id,event_type,ts",
    "max_filter_ratio" = "0.001"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "app_events",
    "property.group.id" = "sr_events_consumer"
);
```

### JSON — Nested Fields

```sql
CREATE ROUTINE LOAD sales.json_job ON orders
PROPERTIES (
    "desired_concurrent_number" = "3",
    "format" = "json",
    "jsonpaths" = "[\"$.id\",\"$.payload.customer_id\",\"$.payload.total\",\"$.meta.ts\"]",
    "columns" = "order_id,customer_id,total,created_at",
    "strip_outer_array" = "false"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "order_events"
);
```

### Avro with Schema Registry

```sql
CREATE ROUTINE LOAD sales.avro_job ON orders
PROPERTIES (
    "desired_concurrent_number" = "3",
    "format" = "avro",
    "columns" = "order_id,customer_id,amount,created_at"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "orders_avro",
    "confluent.schema.registry.url" = "http://schema-registry:8081",
    "property.group.id" = "sr_avro_consumer"
);
```

### SASL/SSL Authentication

```sql
CREATE ROUTINE LOAD sales.secure_job ON orders
PROPERTIES ("format" = "json", "jsonpaths" = "[\"$.id\",\"$.amount\"]")
FROM KAFKA (
    "kafka_broker_list" = "kafka:9093",
    "kafka_topic" = "secure_orders",
    "property.security.protocol" = "SASL_SSL",
    "property.sasl.mechanism" = "SCRAM-SHA-256",
    "property.sasl.username" = "starrocks",
    "property.sasl.password" = "secret123",
    "property.ssl.ca.location" = "/etc/ssl/certs/kafka-ca.pem"
);
```

---

## Upsert into Primary Key Table (CDC Pattern)

For CDC events where each message represents an upsert:

```sql
-- Table: PRIMARY KEY(order_id) with enable_persistent_index=true
CREATE ROUTINE LOAD sales.cdc_orders ON orders
PROPERTIES (
    "desired_concurrent_number" = "4",
    "format" = "json",
    "jsonpaths" = "[\"$.before\",\"$.after\",\"$.op\"]",
    -- Map CDC envelope fields
    "columns" = "before_json,after_json,op,\
                 order_id=get_json_int(after_json,'$.order_id'),\
                 customer_id=get_json_int(after_json,'$.customer_id'),\
                 status=get_json_string(after_json,'$.status'),\
                 amount=get_json_double(after_json,'$.amount')",
    -- Filter: only process insert/update (ignore delete for append table)
    "where" = "op IN ('c','u','r')"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "postgres.sales.orders"
);
```

For delete handling on Primary Key table, use Flink StarRocks connector (supports DELETE semantics) or process via Stream Load with custom logic.

---

## Managing Routine Load Jobs

```sql
-- View all jobs in database
SHOW ROUTINE LOAD FROM sales;

-- View specific job with full details
SHOW ROUTINE LOAD FOR sales.cdc_orders\G

-- View running tasks (partition → BE assignment)
SHOW ROUTINE LOAD TASK FROM sales WHERE jobname = 'cdc_orders';

-- Pause (reversible)
PAUSE ROUTINE LOAD FOR sales.cdc_orders;

-- Resume
RESUME ROUTINE LOAD FOR sales.cdc_orders;

-- Modify (must pause first)
ALTER ROUTINE LOAD FOR sales.cdc_orders
PROPERTIES("desired_concurrent_number" = "6");

-- Permanent stop (cannot resume)
STOP ROUTINE LOAD FOR sales.cdc_orders;
```

---

## SHOW ROUTINE LOAD Key Columns

```sql
SHOW ROUTINE LOAD FOR sales.cdc_orders\G
```

| Column | Description |
|--------|-------------|
| `State` | NEED_SCHEDULE / RUNNING / PAUSED / STOPPED / CANCELLED |
| `DataSourceProperties` | Current Kafka offsets per partition |
| `CustomProperties` | Job configuration |
| `Statistic` | Rows loaded/filtered, error count, bytes consumed |
| `Progress` | Per-partition committed offsets |
| `TimestampProgress` | Per-partition committed message timestamps |
| `ReasonOfStateChanged` | Why job paused/stopped |
| `ErrorLogUrls` | URLs to fetch error samples |
| `TrackingSQL` | SQL to query load history |

---

## Consumer Lag Monitoring

Routine Load does not expose lag directly. Calculate lag using Kafka tools + StarRocks offsets:

```python
from confluent_kafka.admin import AdminClient
from confluent_kafka import TopicPartition, Consumer
import pymysql

def get_routine_load_lag(
    kafka_brokers: str,
    topic: str,
    sr_host: str,
    job_name: str,
    db: str,
) -> dict[int, int]:
    """Returns {partition: lag_messages}"""
    # Get high watermarks from Kafka
    admin = AdminClient({"bootstrap.servers": kafka_brokers})
    consumer = Consumer({
        "bootstrap.servers": kafka_brokers,
        "group.id": "__lag_checker__",
    })
    metadata = admin.list_topics(topic)
    partitions = list(metadata.topics[topic].partitions.keys())
    tps = [TopicPartition(topic, p) for p in partitions]
    high_watermarks = {
        p: consumer.get_watermark_offsets(tp)[1]
        for p, tp in zip(partitions, tps)
    }
    consumer.close()

    # Get committed offsets from StarRocks
    conn = pymysql.connect(host=sr_host, port=9030, user="root", db=db)
    cursor = conn.cursor()
    cursor.execute(f"SHOW ROUTINE LOAD FOR {db}.{job_name}")
    row = dict(zip([d[0] for d in cursor.description], cursor.fetchone()))
    conn.close()

    import json
    progress = json.loads(row["Progress"].split(": ", 1)[1])  # {"0": "12345", ...}
    committed = {int(k): int(v) for k, v in progress.items()}

    lag = {
        p: high_watermarks.get(p, 0) - committed.get(p, 0)
        for p in partitions
    }
    return lag

lag = get_routine_load_lag(
    "kafka:9092", "orders_cdc", "sr-fe", "cdc_orders", "sales"
)
for partition, messages_behind in lag.items():
    print(f"Partition {partition}: {messages_behind} messages behind")
    if messages_behind > 100000:
        print(f"  WARNING: high lag on partition {partition}!")
```

---

## Error Diagnosis

When job pauses with `ReasonOfStateChanged: ErrorTooMany`:

```bash
# Fetch error samples
SHOW ROUTINE LOAD FOR sales.cdc_orders\G
# Get ErrorLogUrls from output

curl "http://be-host:8040/api/_load_error_log?file=error_log_xxx" | head -100
```

Common error causes:
| Error | Cause | Fix |
|-------|-------|-----|
| `Arithmetic exception` | Value overflow (INT → BIGINT) | Fix schema or use MODIFY COLUMN |
| `jsonpaths parse error` | JSON field not found | Check message format, update jsonpaths |
| `Null value in non-null column` | Missing required field | Add `WHERE` filter or allow NULL |
| `Data quality error exceeded` | `max_error_number` hit | Check source data; increase threshold |
| `Offset out of range` | Kafka retention expired | Reset offsets: ALTER ROUTINE LOAD with new offsets |

Reset Kafka offsets after retention expiry:
```sql
PAUSE ROUTINE LOAD FOR sales.cdc_orders;

ALTER ROUTINE LOAD FOR sales.cdc_orders
FROM KAFKA (
    "kafka_offsets" = "OFFSET_BEGINNING,OFFSET_BEGINNING,OFFSET_BEGINNING"
);

RESUME ROUTINE LOAD FOR sales.cdc_orders;
```

---

## Tuning desired_concurrent_number

```
actual_concurrency = min(
    kafka_partition_count,
    alive_BE_count,
    desired_concurrent_number,
    max_routine_load_task_concurrent_num  -- FE config, default 5
)
```

- Set `desired_concurrent_number` = number of Kafka partitions for full parallelism
- For many topics on same cluster: reduce per-job concurrency, increase `max_routine_load_task_concurrent_num`
- More concurrency = higher throughput but more memory per BE

---

## Anti-Patterns

1. **`max_error_number=0` with noisy source** — job pauses on first bad message; set to 1000 for CDC streams.
2. **One job per partition** — unnecessary; one job handles all partitions with `desired_concurrent_number` tasks.
3. **Not monitoring lag** — Routine Load looks healthy while falling behind; add lag alerting.
4. **Wrong `kafka_offsets` on create** — `OFFSET_BEGINNING` for replay, `OFFSET_END` for live start; default is OFFSET_END.
5. **`strict_mode=false`** — type mismatches silently truncate strings to NULL; enable strict in production.
6. **No consumer group ID** — StarRocks uses a generated ID; set explicit `property.group.id` for external monitoring.
7. **`max_batch_interval` too low** — very frequent small commits increases FE load; ≥ 10s is reasonable.

---

## References

- Routine Load docs: `docs.starrocks.io/docs/loading/RoutineLoad/`
- Kafka connector: `docs.starrocks.io/docs/loading/Kafka-connector-starrocks/`
- Related skills: `[[starrocks-stream-load]]`, `[[starrocks-cdc-pipeline]]`, `[[starrocks-realtime-modeling]]`
