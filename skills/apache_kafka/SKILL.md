---
name: apache-kafka
description: Apache Kafka — topics, partitions, consumer groups, producer/consumer configuration (acks, idempotence, compression, commit strategies), exactly-once semantics, Kafka Connect (source/sink connectors, SMTs, DLQ), Schema Registry, consumer lag monitoring, CLI operations, Python confluent-kafka examples, partition strategy, replication, retention
---

# Apache Kafka

## When to Use

Load this skill when the user needs to:
- Design Kafka topics, choose partition count and replication factor
- Configure producers (acks, idempotence, batching, compression)
- Configure consumers (commit strategies, rebalancing, exactly-once)
- Set up Kafka Connect source/sink connectors
- Handle errors with Dead Letter Queues
- Monitor consumer lag and cluster health
- Write Python producers/consumers with `confluent-kafka`

---

## Core Concepts

```
Broker cluster (3+ nodes)
  └─ Topic (logical stream of records)
       └─ Partition 0  [offset 0, 1, 2, 3, ...]  ← replicated to follower brokers
       └─ Partition 1  [offset 0, 1, 2, ...]
       └─ Partition N  ...

Producer → writes to partitions (by key hash or round-robin)
Consumer Group → each partition consumed by exactly one consumer in the group
                 N partitions ÷ M consumers = M≤N for full parallelism
```

**Offset** — position of a consumer in a partition; committed to `__consumer_offsets` topic.  
**Leader replica** — handles reads and writes; followers replicate.  
**ISR (In-Sync Replicas)** — replicas that are caught up with the leader.

---

## Topic Design

### Partition Count

```
Partitions = max(target_throughput_MB/s ÷ per_partition_throughput, desired_consumer_parallelism)
```

Rules of thumb:
- Start with **6–12 partitions** for most topics; increase later (but keyed topics can't repartition without rekeying).
- Never set partitions < number of consumers in the consuming group.
- **Avoid over-partitioning** — each partition has file handles and memory overhead on every broker.

### Replication & Durability

```bash
# Create topic with production settings
kafka-topics.sh --bootstrap-server broker:9092 --create \
  --topic orders \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000 \
  --config compression.type=snappy
```

| Parameter | Recommended | Notes |
|-----------|-------------|-------|
| `replication.factor` | 3 | Survive 1 broker loss |
| `min.insync.replicas` | 2 | With `acks=all` ensures at least 2 copies |
| `unclean.leader.election.enable` | `false` | Prevent data loss from lagging replica becoming leader |
| `retention.ms` | 7 days (604800000) | Or `-1` for infinite; override per topic |
| `retention.bytes` | `-1` or explicit | Size-based retention per partition |
| `cleanup.policy` | `delete` or `compact` | `compact` for changelog/event-sourcing topics |
| `compression.type` | `snappy` or `lz4` | Set on topic; producer compression preferred |

### Topic Naming Convention

```
<domain>.<entity>.<event>       → payments.orders.placed
<env>.<domain>.<entity>        → prod.payments.orders
<team>.<service>.<event>       → checkout.cart.item_added
dlq.<original-topic>           → dlq.payments.orders.placed
```

---

## Producer Configuration

### Durability vs Throughput Trade-offs

```python
# HIGH DURABILITY — production default for critical data
durable_producer = {
    "bootstrap.servers": "broker1:9092,broker2:9092,broker3:9092",
    "client.id": "orders-producer-v1",

    # Durability
    "acks": "all",                              # wait for all ISR acks
    "enable.idempotence": True,                 # exactly-once per session
    "max.in.flight.requests.per.connection": 5, # up to 5 with idempotence=True

    # Throughput
    "batch.size": 65536,           # 64 KB batch size
    "linger.ms": 10,               # wait up to 10ms to fill batch
    "compression.type": "snappy",  # snappy: fast; gzip: best ratio; lz4: balanced

    # Resilience
    "retries": 2147483647,         # retry indefinitely (with idempotence)
    "delivery.timeout.ms": 120000, # give up after 2 min total
    "request.timeout.ms": 30000,
    "buffer.memory": 67108864,     # 64 MB send buffer
}

# HIGH THROUGHPUT — telemetry / non-critical events
throughput_producer = {
    "bootstrap.servers": "broker:9092",
    "acks": "1",               # leader ack only
    "batch.size": 131072,      # 128 KB
    "linger.ms": 50,
    "compression.type": "lz4",
    "buffer.memory": 134217728, # 128 MB
}
```

### Idempotent Producer (prevents duplicates on retry)

`enable.idempotence=True` assigns each producer a **PID** and a **sequence number** per partition. The broker deduplicates retried messages within the producer session.  
**Requires**: `acks=all`, `max.in.flight.requests.per.connection ≤ 5`, `retries > 0`.

### Transactional Producer (exactly-once across partitions)

```python
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "broker:9092",
    "transactional.id": "order-processor-1",  # unique per producer instance
    "enable.idempotence": True,
})

producer.init_transactions()

try:
    producer.begin_transaction()
    producer.produce("orders", key="1001", value='{"status":"placed"}')
    producer.produce("audit",  key="1001", value='{"action":"order_placed"}')
    producer.commit_transaction()
except Exception as e:
    producer.abort_transaction()
    raise
```

### Python Producer — Full Example

```python
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import json, logging

logger = logging.getLogger(__name__)

def delivery_callback(err, msg):
    if err:
        logger.error(f"Delivery failed | topic={msg.topic()} partition={msg.partition()} error={err}")
    else:
        logger.debug(f"Delivered | topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")

producer = Producer({
    "bootstrap.servers": "broker:9092",
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "snappy",
    "batch.size": 65536,
    "linger.ms": 10,
})

def send_order(order: dict) -> None:
    producer.produce(
        topic="orders",
        key=str(order["order_id"]).encode(),
        value=json.dumps(order).encode(),
        on_delivery=delivery_callback,
    )
    producer.poll(0)   # trigger delivery callbacks without blocking

# After batch:
producer.flush()       # block until all in-flight messages delivered
```

---

## Consumer Configuration

### Core Settings

```python
consumer_config = {
    "bootstrap.servers": "broker:9092",
    "group.id": "orders-service-v1",
    "client.id": "orders-consumer-1",

    # Offset reset: what to do when no committed offset exists
    "auto.offset.reset": "earliest",   # earliest | latest | error

    # ALWAYS disable auto-commit for production — control commits manually
    "enable.auto.commit": False,

    # Rebalance protocol
    "partition.assignment.strategy": "cooperative-sticky",  # minimises rebalance disruption

    # Session & poll timeouts
    "session.timeout.ms": 30000,       # consumer considered dead after 30s without heartbeat
    "heartbeat.interval.ms": 10000,    # heartbeat every 10s (< session.timeout.ms / 3)
    "max.poll.interval.ms": 300000,    # max 5 min between poll() calls before consumer kicked

    # Throughput
    "fetch.min.bytes": 1024,           # wait for at least 1 KB before returning
    "fetch.max.wait.ms": 500,
    "max.poll.records": 500,           # max messages per poll()
}
```

### Commit Strategies

```python
from confluent_kafka import Consumer, KafkaError, TopicPartition

consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])

# --- Strategy 1: Synchronous commit after each batch (safe, lower throughput)
try:
    while True:
        msgs = consumer.consume(num_messages=100, timeout=1.0)
        for msg in msgs:
            if msg.error():
                logger.error(msg.error())
                continue
            process(msg)
        if msgs:
            consumer.commit(asynchronous=False)   # block until broker confirms
except Exception:
    pass
finally:
    consumer.close()

# --- Strategy 2: Async commit in loop + sync commit on shutdown (balanced)
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(msg.error())
            continue
        process(msg)
        consumer.commit(asynchronous=True)
except KeyboardInterrupt:
    consumer.commit(asynchronous=False)   # final sync flush
finally:
    consumer.close()

# --- Strategy 3: Manual per-partition offset commit (exactly-once with external store)
def on_assign(consumer, partitions):
    # restore offsets from external store (e.g. DB) on rebalance
    for p in partitions:
        offset = db.get_offset(p.topic, p.partition) or OFFSET_BEGINNING
        p.offset = offset
    consumer.assign(partitions)

consumer.subscribe(["orders"], on_assign=on_assign)

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    process_and_save_to_db(msg)           # write result + offset atomically
    consumer.commit(offsets=[
        TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
    ], asynchronous=False)
```

### Consumer Group Operations (CLI)

```bash
# List all consumer groups
kafka-consumer-groups.sh --bootstrap-server broker:9092 --list

# Describe group — shows lag per partition
kafka-consumer-groups.sh --bootstrap-server broker:9092 \
  --describe --group orders-service-v1

# Reset offsets to beginning (for re-processing)
kafka-consumer-groups.sh --bootstrap-server broker:9092 \
  --group orders-service-v1 --topic orders \
  --reset-offsets --to-earliest --execute

# Shift offsets back by N messages
kafka-consumer-groups.sh --bootstrap-server broker:9092 \
  --group orders-service-v1 --topic orders \
  --reset-offsets --shift-by -1000 --execute

# Reset to specific datetime
kafka-consumer-groups.sh --bootstrap-server broker:9092 \
  --group orders-service-v1 --topic orders \
  --reset-offsets --to-datetime 2024-01-15T00:00:00.000 --execute
```

---

## Exactly-Once Semantics (EOS)

| Delivery Guarantee | Producer | Consumer | Use When |
|---|---|---|---|
| At-most-once | `acks=0/1`, no retry | auto.commit before processing | Metrics, telemetry |
| At-least-once | `acks=all` + retries | commit after processing | Most pipelines (handle idempotent consumers) |
| Exactly-once | `enable.idempotence=True` + transactions | `isolation.level=read_committed` | Financial, inventory, audit |

```python
# Exactly-once consumer: only read committed transactional messages
consumer = Consumer({
    **consumer_config,
    "isolation.level": "read_committed",   # skip messages from aborted transactions
})
```

---

## Schema Registry

Enforces schema evolution rules; decouples producers from consumers.

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

schema_registry_conf = {"url": "http://schema-registry:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

order_schema_str = """
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example",
  "fields": [
    {"name": "order_id", "type": "long"},
    {"name": "amount",   "type": "double"},
    {"name": "status",   "type": "string"}
  ]
}
"""

avro_serializer = AvroSerializer(schema_registry_client, order_schema_str)
avro_deserializer = AvroDeserializer(schema_registry_client)

# Producer with Avro serialization
producer = Producer({"bootstrap.servers": "broker:9092"})
producer.produce(
    topic="orders",
    value=avro_serializer(
        {"order_id": 1001, "amount": 299.99, "status": "placed"},
        SerializationContext("orders", MessageField.VALUE),
    ),
)
```

**Schema compatibility modes** (set per subject):

| Mode | Rule |
|------|------|
| `BACKWARD` (default) | New schema reads old data (consumers upgrade first) |
| `FORWARD` | Old schema reads new data (producers upgrade first) |
| `FULL` | Both directions |
| `NONE` | No checks — dangerous |

```bash
# Check compatibility before registering
curl -X POST http://schema-registry:8081/compatibility/subjects/orders-value/versions/latest \
  -H "Content-Type: application/json" \
  -d '{"schema": "<escaped_json_schema>"}'
```

---

## Kafka Connect

### Source Connector — PostgreSQL CDC (Debezium)

```json
{
  "name": "postgres-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "secret",
    "database.dbname": "orders_db",
    "topic.prefix": "cdc",
    "table.include.list": "public.orders,public.customers",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",

    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "errors.deadletterqueue.topic.name": "dlq.cdc.postgres-source",
    "errors.deadletterqueue.topic.replication.factor": "3",
    "errors.deadletterqueue.context.headers.enable": "true"
  }
}
```

### Sink Connector — Iceberg via Kafka Connect

```json
{
  "name": "iceberg-sink",
  "config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "4",
    "topics": "cdc.public.orders",
    "iceberg.tables": "silver.orders",
    "iceberg.catalog": "rest",
    "iceberg.catalog.uri": "http://iceberg-rest:8181",
    "iceberg.tables.upsert-mode-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq.iceberg-sink",
    "errors.deadletterqueue.context.headers.enable": "true"
  }
}
```

### Dead Letter Queue — Reading Failed Messages

```python
from confluent_kafka import Consumer
import json

dlq_consumer = Consumer({
    "bootstrap.servers": "broker:9092",
    "group.id": "dlq-inspector",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
dlq_consumer.subscribe(["dlq.cdc.postgres-source"])

while True:
    msg = dlq_consumer.poll(1.0)
    if msg is None:
        continue
    headers = {k: v.decode() for k, v in (msg.headers() or [])}
    print({
        "offset":    msg.offset(),
        "partition": msg.partition(),
        "error":     headers.get("__connect.errors.exception.message"),
        "stage":     headers.get("__connect.errors.stage"),
        "connector": headers.get("__connect.errors.connector.name"),
        "payload":   msg.value(),
    })
    dlq_consumer.commit(asynchronous=False)
```

### Connector REST API

```bash
# Deploy connector
curl -X POST http://connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector_config.json

# Check status
curl http://connect:8083/connectors/postgres-source/status

# Restart failed task
curl -X POST http://connect:8083/connectors/postgres-source/tasks/0/restart

# Update config without restart
curl -X PUT http://connect:8083/connectors/postgres-source/config \
  -H "Content-Type: application/json" \
  -d '{"table.include.list": "public.orders,public.customers,public.products", ...}'

# Delete connector
curl -X DELETE http://connect:8083/connectors/postgres-source
```

---

## Monitoring — Consumer Lag

### Via CLI

```bash
# Real-time lag across all partitions
watch -n 5 kafka-consumer-groups.sh \
  --bootstrap-server broker:9092 \
  --describe --group orders-service-v1

# Output columns: GROUP | TOPIC | PARTITION | CURRENT-OFFSET | LOG-END-OFFSET | LAG
```

### Via Python (programmatic lag check)

```python
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition

def get_consumer_lag(bootstrap_servers: str, group_id: str, topic: str) -> dict:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group_id})

    metadata = admin.list_topics(topic=topic)
    partitions = [
        TopicPartition(topic, p)
        for p in metadata.topics[topic].partitions
    ]

    committed = consumer.committed(partitions)
    high_watermarks = {
        p.partition: consumer.get_watermark_offsets(p)[1]  # (low, high)
        for p in partitions
    }

    lag = {}
    for tp in committed:
        hw = high_watermarks[tp.partition]
        committed_offset = tp.offset if tp.offset >= 0 else 0
        lag[tp.partition] = hw - committed_offset

    consumer.close()
    return lag  # {partition_id: lag}

# Alert if total lag > threshold
lag = get_consumer_lag("broker:9092", "orders-service-v1", "orders")
total_lag = sum(lag.values())
if total_lag > 10000:
    alert(f"High consumer lag: {total_lag} messages behind")
```

### Key JMX Metrics to Monitor

| Metric | Alert Threshold | Meaning |
|--------|----------------|---------|
| `kafka.consumer:records-lag-max` | > 10 000 | Max lag across partitions |
| `kafka.producer:record-error-rate` | > 0 | Producer delivery failures |
| `kafka.producer:request-latency-avg` | > 100ms | Broker write latency |
| `kafka.server:UnderReplicatedPartitions` | > 0 | Replicas falling behind |
| `kafka.server:ActiveControllerCount` | ≠ 1 | Controller election issue |
| `kafka.server:BytesInPerSec` | varies | Inbound throughput |

---

## Topic Management CLI

```bash
# List topics
kafka-topics.sh --bootstrap-server broker:9092 --list

# Describe topic (partitions, replicas, ISR)
kafka-topics.sh --bootstrap-server broker:9092 --describe --topic orders

# Alter topic (add partitions — keyed topics lose ordering on existing keys!)
kafka-topics.sh --bootstrap-server broker:9092 --alter \
  --topic orders --partitions 24

# Change retention
kafka-configs.sh --bootstrap-server broker:9092 \
  --entity-type topics --entity-name orders \
  --alter --add-config retention.ms=2592000000  # 30 days

# Delete topic
kafka-topics.sh --bootstrap-server broker:9092 --delete --topic orders

# Produce test messages
kafka-console-producer.sh --bootstrap-server broker:9092 \
  --topic orders --property "key.separator=:" --property "parse.key=true"

# Consume from beginning
kafka-console-consumer.sh --bootstrap-server broker:9092 \
  --topic orders --from-beginning \
  --property print.key=true --property print.timestamp=true
```

---

## Partition Strategy

```python
from confluent_kafka import Producer

# Default: hash(key) % num_partitions — ensures order per key
producer.produce("orders", key=b"user_123", value=b"...")

# Round-robin: key=None
producer.produce("events", key=None, value=b"...")

# Custom partitioner (confluent-kafka via on_delivery is not partitioner)
# Use key design to control routing:
#   - order_id → all events for one order go to same partition
#   - user_id  → all events for one user go to same partition
#   - region   → geographic affinity (but watch hotspots!)

# HOT PARTITION detection: one partition has 10x more messages
# Fix: use composite key = f"{high_cardinality_id}_{salt}"
# Or: use null key (round-robin) if ordering not needed
```

---

## Docker Compose — Local Kafka Stack

```yaml
version: "3.9"
services:
  broker:
    image: confluentinc/cp-kafka:7.6.0
    hostname: broker
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://broker:9092,CONTROLLER://broker:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_MIN_INSYNC_REPLICAS: 1
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qg"   # base64 UUID

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    ports: ["8081:8081"]
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: broker:9092
    depends_on: [broker]

  kafka-connect:
    image: confluentinc/cp-kafka-connect:7.6.0
    ports: ["8083:8083"]
    environment:
      CONNECT_BOOTSTRAP_SERVERS: broker:9092
      CONNECT_GROUP_ID: kafka-connect-cluster
      CONNECT_CONFIG_STORAGE_TOPIC: _connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: _connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: _connect-status
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
    depends_on: [broker, schema-registry]

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ports: ["8080:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: broker:9092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME: connect
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: http://kafka-connect:8083
    depends_on: [broker]
```

---

## Best Practices

1. **Always set `acks=all` + `enable.idempotence=True`** for business-critical producers — prevents duplicates on broker failover.
2. **Disable `enable.auto.commit`** in all production consumers — commit only after successful processing.
3. **Use `partition.assignment.strategy=cooperative-sticky`** — incremental rebalancing eliminates stop-the-world pauses.
4. **Design topics around consumers, not producers** — partition count = desired consumer parallelism.
5. **Use keys for ordering guarantees** — all messages with the same key go to the same partition and are processed in order.
6. **Never increase partitions on a keyed topic** without a migration plan — key routing changes break ordering for existing keys.
7. **Always configure DLQ** for Kafka Connect sinks — `errors.tolerance=all` without DLQ silently drops data.
8. **Monitor consumer lag** as the primary health metric — lag > threshold triggers backpressure or scale-out.
9. **Use Schema Registry with BACKWARD compatibility** — consumers can upgrade independently of producers.
10. **Set `min.insync.replicas = replication_factor - 1`** (e.g. 2 for RF=3) — ensures durability during one broker failure.
11. **Separate log directories (`log.dirs`) from OS disk** — Kafka is I/O intensive; co-location degrades both.
12. **Use `isolation.level=read_committed`** in consumers that read from transactional topics.

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| `acks=0` or `acks=1` for critical data | Data loss on broker failure | `acks=all` + `enable.idempotence=True` |
| `enable.auto.commit=True` | Commits offset before processing complete → data loss on crash | Disable; commit manually after processing |
| Too many small topics (thousands) | Controller metadata overload, slow elections | Consolidate; use partitions for parallelism |
| 1 partition per topic | Zero consumer parallelism | Design for expected throughput |
| `max.in.flight.requests.per.connection > 5` with idempotence | Idempotence requires ≤ 5 | Leave default (5) or set to 1 for strict ordering |
| Consuming in `while True` without `close()` | Partition never rebalanced to another consumer | Always `consumer.close()` in `finally` |
| `auto.offset.reset=latest` in new consumer group | Misses all historical messages | Use `earliest` for new groups unless intentional |
| Storing large payloads (> 1 MB) in Kafka | Memory pressure, slow replication | Store in object storage, publish reference/URL |
| Sharing `group.id` across different applications | One app steals partitions from another | Each application gets a unique `group.id` |
| Changing partition count on keyed topic in production | Key-to-partition mapping breaks, ordering violated | Add new topic version + consumer migration |
| No DLQ for Connect sinks | Errors silently drop data | Set `errors.tolerance=all` + `errors.deadletterqueue.topic.name` |

---

## References to Consult When Needed

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Kafka Python Client](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [Confluent Schema Registry Docs](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Kafka Connect Error Handling & DLQ](https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/)
- [Kafka Production Deployment Guide](https://docs.confluent.io/platform/current/kafka/deployment.html)
