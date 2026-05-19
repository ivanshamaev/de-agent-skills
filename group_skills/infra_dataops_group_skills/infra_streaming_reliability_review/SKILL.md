---
name: infra-streaming-reliability-review
description: Streaming pipeline reliability — exactly-once semantics (idempotent producer/transactional API/read_committed/Kafka Streams EOS), at-least-once patterns with idempotent consumers, dead letter queue (DLQ) design with Kafka Connect SMT/custom error handler, poison message handling, reprocessing strategy, watermarks and late data handling, checkpoint/savepoint for Flink/Spark Streaming, consumer group rebalance storm prevention, end-to-end latency SLO monitoring, streaming reliability checklist
---

# Streaming Pipeline Reliability Review

## When to Use

- Designing a streaming pipeline that requires exactly-once or at-least-once guarantees
- Implementing DLQ (Dead Letter Queue) for poison message handling
- Reviewing a Kafka Streams or Flink pipeline for reliability gaps
- Setting up reprocessing workflows after downstream failures
- Tuning consumer group stability to prevent rebalance storms

---

## Delivery Semantics Decision Matrix

```
At-most-once:   fire-and-forget, no retries, tolerate data loss
                → metrics aggregation, non-critical event logging

At-least-once:  producer retries + idempotent consumer logic
                → most ETL pipelines (with INSERT OVERWRITE or upsert)

Exactly-once:   Kafka transactions OR idempotent consumer writes
                → financial transactions, inventory updates, CDC pipelines
```

---

## Exactly-Once with Kafka Transactions

```python
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition

def process_orders_exactly_once(
    input_topic: str,
    output_topic: str,
    consumer_group: str,
    transactional_id: str,
):
    # Producer with transactions
    producer = Producer({
        'bootstrap.servers': 'kafka:9092',
        'transactional.id': transactional_id,   # unique per producer instance
        'enable.idempotence': True,              # implied by transactional.id
        'acks': 'all',
        'max.in.flight.requests.per.connection': 5,
    })

    # Consumer: read_committed skips uncommitted/aborted messages
    consumer = Consumer({
        'bootstrap.servers': 'kafka:9092',
        'group.id': consumer_group,
        'isolation.level': 'read_committed',     # skip aborted transactions
        'enable.auto.commit': False,             # manual offset commit in transaction
        'auto.offset.reset': 'earliest',
    })

    producer.init_transactions()
    consumer.subscribe([input_topic])

    while True:
        messages = consumer.consume(num_messages=100, timeout=1.0)
        if not messages:
            continue

        producer.begin_transaction()
        try:
            for msg in messages:
                if msg.error():
                    if msg.error().code() == KafkaError.PARTITION_EOF:
                        continue
                    raise Exception(f"Consumer error: {msg.error()}")

                # Process
                result = transform(msg.value())

                # Write output within transaction
                producer.produce(output_topic, key=msg.key(), value=result)

            # Commit offsets in the same transaction as output
            offsets = {
                TopicPartition(m.topic(), m.partition(), m.offset() + 1)
                for m in messages
            }
            producer.send_offsets_to_transaction(offsets, consumer.consumer_group_metadata())
            producer.commit_transaction()

        except Exception as e:
            producer.abort_transaction()
            raise
```

---

## Exactly-Once with Kafka Streams

```java
// Kafka Streams: processing.guarantee=exactly_once_v2
Properties props = new Properties();
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-processor");

// exactly_once_v2 (Kafka 2.5+) is more efficient than exactly_once
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

// Commit interval (lower = lower latency, higher overhead)
props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
```

---

## At-Least-Once with Idempotent Consumer

```python
# When full EOS is too expensive, use idempotent writes at consumer side
from sqlalchemy import create_engine, text

def process_and_upsert(messages: list, engine):
    """Idempotent consumer: safe to replay any message."""
    for msg in messages:
        event = deserialize(msg.value())

        # Upsert by natural key: replaying the same message is safe
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO orders (order_id, customer_id, amount, updated_at)
                VALUES (:order_id, :customer_id, :amount, :updated_at)
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    amount = EXCLUDED.amount,
                    updated_at = EXCLUDED.updated_at
                WHERE orders.updated_at < EXCLUDED.updated_at
            """), event)

    consumer.commit()  # only after successful batch write
```

---

## Dead Letter Queue (DLQ) Design

### Kafka Connect DLQ

```json
{
  "name": "postgres-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "topics": "orders-validated",
    "connection.url": "jdbc:postgresql://db:5432/warehouse",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "orders-validated-dlq",
    "errors.deadletterqueue.topic.replication.factor": "3",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }
}
```

### Custom DLQ Handler (Python)

```python
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

@dataclass
class DLQMessage:
    original_topic: str
    original_partition: int
    original_offset: int
    original_key: bytes | None
    original_value: bytes
    error_class: str
    error_message: str
    retry_count: int
    failed_at: str
    pipeline: str


def send_to_dlq(
    producer,
    dlq_topic: str,
    original_msg,
    error: Exception,
    retry_count: int = 0,
    pipeline: str = "unknown",
):
    """Route failed message to DLQ with full context."""
    dlq_record = DLQMessage(
        original_topic=original_msg.topic(),
        original_partition=original_msg.partition(),
        original_offset=original_msg.offset(),
        original_key=original_msg.key(),
        original_value=original_msg.value(),
        error_class=type(error).__name__,
        error_message=str(error),
        retry_count=retry_count,
        failed_at=datetime.now(timezone.utc).isoformat(),
        pipeline=pipeline,
    )

    producer.produce(
        topic=dlq_topic,
        key=original_msg.key(),
        value=json.dumps(asdict(dlq_record)).encode(),
        headers={
            "x-dead-letter-origin-topic": original_msg.topic().encode(),
            "x-dead-letter-error-class": type(error).__name__.encode(),
            "x-dead-letter-retry-count": str(retry_count).encode(),
        },
    )
    producer.flush()
```

### DLQ Reprocessing

```python
# Reprocess DLQ messages after fixing the bug
def reprocess_dlq(
    dlq_topic: str,
    target_topic: str,
    filter_error_class: str | None = None,
    max_messages: int = 10000,
):
    """Re-route DLQ messages back to original topic for reprocessing."""
    consumer = create_consumer(group_id=f"dlq-reprocessor-{int(time.time())}")
    producer = create_producer()

    consumer.assign([TopicPartition(dlq_topic, 0)])  # assign all partitions

    reprocessed = 0
    skipped = 0

    while reprocessed + skipped < max_messages:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            break

        dlq_record = json.loads(msg.value())

        if filter_error_class and dlq_record["error_class"] != filter_error_class:
            skipped += 1
            continue

        # Re-produce original message to original topic
        producer.produce(
            topic=target_topic,
            key=dlq_record.get("original_key", "").encode() if dlq_record.get("original_key") else None,
            value=dlq_record["original_value"].encode(),
        )
        reprocessed += 1

    producer.flush()
    return {"reprocessed": reprocessed, "skipped": skipped}
```

---

## Flink Checkpointing for Reliability

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Enable checkpointing every 60 seconds
env.enableCheckpointing(60_000);

CheckpointConfig config = env.getCheckpointConfig();

// Exactly-once mode (default)
config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// Tolerate 2 consecutive checkpoint failures before job fails
config.setTolerableCheckpointFailureNumber(2);

// Minimum time between checkpoints (prevent checkpoint overlap)
config.setMinPauseBetweenCheckpoints(30_000);

// Checkpoint must complete within 10 min
config.setCheckpointTimeout(600_000);

// Keep last 2 completed checkpoints for manual recovery
config.setMaxConcurrentCheckpoints(1);
config.enableExternalizedCheckpoints(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
);

// RocksDB state backend for large state (> 1 GB)
env.setStateBackend(new EmbeddedRocksDBStateBackend(true)); // incremental checkpoints
env.getCheckpointConfig().setCheckpointStorage("s3://my-flink-checkpoints/prod/");
```

---

## Rebalance Storm Prevention

```python
# Consumer config to prevent rebalance cascades
consumer_config = {
    # Cooperative sticky: members keep their partitions during rebalance
    # (no stop-the-world pause)
    'partition.assignment.strategy': 'cooperative-sticky',

    # Don't join group until cluster is stable
    'group.instance.id': f'worker-{HOSTNAME}',  # static membership

    # Long poll interval prevents "slow consumer" rebalance trigger
    'max.poll.interval.ms': 600000,   # 10 min for long-running batch operations

    # Healthy session: detect real failures, not GC pauses
    'session.timeout.ms': 60000,
    'heartbeat.interval.ms': 10000,
}
```

```bash
# Monitor rebalance frequency (high rebalance = instability)
# In Prometheus:
kafka_coordinator_group_completed_rebalances_total

# Alert: more than 5 rebalances per hour per group
- alert: KafkaConsumerRebalanceStorm
  expr: |
    rate(kafka_coordinator_group_completed_rebalances_total[1h]) > 5
  labels:
    severity: warning
  annotations:
    summary: "Frequent consumer rebalances: {{ $labels.group }}"
```

---

## End-to-End Latency Monitoring

```python
# Inject timestamp header at producer side
from datetime import datetime, timezone
import struct

def produce_with_timestamp(producer, topic: str, key: bytes, value: bytes):
    ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    producer.produce(
        topic=topic,
        key=key,
        value=value,
        headers={"x-produce-timestamp-ms": struct.pack(">q", ts_ms)},
    )

# Measure E2E latency at consumer side
def measure_e2e_latency(msg) -> float:
    headers = dict(msg.headers() or [])
    if b"x-produce-timestamp-ms" not in headers:
        return -1.0
    produce_ts = struct.unpack(">q", headers[b"x-produce-timestamp-ms"])[0]
    consume_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    return (consume_ts - produce_ts) / 1000.0   # seconds

# Expose as Prometheus histogram
from prometheus_client import Histogram
e2e_latency = Histogram(
    "streaming_e2e_latency_seconds",
    "End-to-end message latency",
    ["topic", "pipeline"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 5, 30, 60],
)
```

---

## Reliability Checklist

```
Delivery Guarantees:
[ ] Delivery semantics documented for each pipeline (at-most/at-least/exactly-once)
[ ] Producer acks=all + enable.idempotence=true for critical pipelines
[ ] Consumer isolation.level=read_committed for transactional pipelines
[ ] Idempotent writes at consumer (upsert/INSERT ON CONFLICT)

DLQ:
[ ] Every pipeline has a DLQ topic configured
[ ] DLQ messages include full error context (class, message, original topic/offset)
[ ] DLQ monitoring alert: DLQ topic growing (indicates recurring failures)
[ ] DLQ reprocessing runbook documented and tested

State Management (Flink/Kafka Streams):
[ ] Checkpoints enabled with min 60s interval
[ ] Checkpoint storage is durable (S3/GCS, not local disk)
[ ] Savepoint procedure documented for safe upgrades
[ ] State backend is RocksDB for state > 1 GB

Consumer Stability:
[ ] cooperative-sticky assignor for zero-downtime rebalance
[ ] Static group membership (group.instance.id) for long-running consumers
[ ] max.poll.interval.ms tuned to max processing time per batch

Observability:
[ ] End-to-end latency measured and alarmed (P99 > SLO threshold)
[ ] Consumer lag alert per consumer group
[ ] DLQ topic message count alert
[ ] Checkpoint failure alert for Flink jobs
```

---

## Anti-Patterns

1. **EOS with non-idempotent side effects** — Kafka transactions guarantee exactly-once to Kafka topics, but an HTTP API call inside the transaction is still at-least-once; model idempotency at the API level too.
2. **DLQ without monitoring** — messages silently accumulate in DLQ while the pipeline appears healthy; alert on DLQ lag growth.
3. **Auto-commit in at-least-once pipeline** — auto-commit marks offsets before processing finishes; a crash between fetch and commit loses messages.
4. **Infinite retry on poison message** — a malformed message retried indefinitely blocks the partition; route to DLQ after N retries.
5. **Checkpoint storage on broker disk** — broker disk failure loses all checkpoints; always store checkpoints in object storage (S3/GCS).

---

## References

- Kafka exactly-once: `confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/`
- Kafka Streams EOS: `docs.confluent.io/platform/current/streams/developer-guide/config-streams.html`
- Flink checkpointing: `nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints/`
- Related skills: `[[apache-kafka]]`, `[[apache-flink]]`, `[[pyspark-streaming]]`, `[[infra-kafka-platform-review]]`, `[[cdc-debezium]]`
