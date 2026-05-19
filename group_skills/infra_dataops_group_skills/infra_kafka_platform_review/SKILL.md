---
name: infra-kafka-platform-review
description: Kafka production platform review — broker configuration (replication factor/min.insync.replicas/rack awareness/KRaft mode), topic design (partition count formula/compaction/retention), consumer group management (lag monitoring/rebalance tuning/cooperative sticky), producer tuning (acks/idempotence/compression/batching), security (SASL_SSL/ACLs/mTLS), JMX metrics to Prometheus (kafka-exporter/JMX exporter), alert rules (under-replicated partitions/ISR shrink/consumer lag/disk), capacity planning, Strimzi Kubernetes operator
---

# Kafka Production Platform Review

## When to Use

- Reviewing a Kafka cluster before production promotion
- Diagnosing replication lag, consumer group issues, or performance problems
- Designing topic partitioning and retention strategy
- Setting up Kafka monitoring with Prometheus and Grafana
- Running Kafka on Kubernetes with Strimzi

---

## Broker Configuration (Production)

```properties
# server.properties — production broker settings

# Cluster identity (KRaft mode — no ZooKeeper for Kafka 3.3+)
process.roles=broker,controller       # dev only; separate in prod
node.id=1
controller.quorum.voters=1@kafka-0:9093,2@kafka-1:9093,3@kafka-2:9093

# Replication safety
default.replication.factor=3
min.insync.replicas=2                 # producer acks=all requires 2 of 3 ISR
unclean.leader.election.enable=false  # never elect out-of-sync replica as leader
auto.create.topics.enable=false       # control schema drift

# Rack awareness (requires broker.rack set per broker)
replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector

# Log retention
log.retention.hours=168               # 7 days default
log.segment.bytes=1073741824          # 1 GB segment rollover
log.cleanup.policy=delete             # or compact for changelog topics
log.retention.check.interval.ms=300000

# Compression (broker-level re-compression)
compression.type=producer             # preserve producer compression

# Network / threads
num.network.threads=8                 # increase on high-throughput brokers
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600   # 100 MB max request

# Replication performance
replica.fetch.max.bytes=1048576
replica.fetch.wait.max.ms=500

# Listeners
listeners=SASL_SSL://:9092,CONTROLLER://:9093
advertised.listeners=SASL_SSL://kafka-0.kafka.svc.cluster.local:9092
```

```properties
# JVM heap (KAFKA_HEAP_OPTS)
# For brokers with 64 GB RAM, OS page cache is more important than heap
KAFKA_HEAP_OPTS="-Xms6g -Xmx6g"
KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35"
```

---

## Topic Design Best Practices

### Partition Count Formula

```
partitions = max(target_throughput_MBps / broker_write_MBps, consumer_parallelism)

# Example: 500 MB/s throughput, 100 MB/s per broker write, 20 consumer instances
partitions = max(500/100, 20) = max(5, 20) = 20 partitions
```

```bash
# Create topic with explicit config
kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic orders \
  --partitions 24 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000 \
  --config segment.bytes=536870912 \
  --config compression.type=lz4

# Describe topic config
kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic orders
```

### Topic Naming Convention

```
{env}.{domain}.{entity}.{version}

prod.orders.created.v1
prod.customers.updated.v2
staging.events.pageview.v1
```

### Compacted Topics (Changelog)

```bash
# Create compacted topic for current state (e.g., Kafka Streams KTable)
kafka-topics.sh --bootstrap-server kafka:9092 \
  --create \
  --topic customer-state \
  --partitions 12 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.1 \
  --config segment.ms=3600000 \     # compact hourly
  --config delete.retention.ms=86400000
```

---

## Consumer Group Management

### Cooperative Sticky Rebalance

```python
from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'orders-processor',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,

    # Use cooperative rebalance (Kafka 2.4+) — no stop-the-world
    'partition.assignment.strategy': 'cooperative-sticky',

    # Tune session and heartbeat for stability
    'session.timeout.ms': 60000,         # 60s (default 45s)
    'heartbeat.interval.ms': 10000,      # must be < session.timeout.ms / 3
    'max.poll.interval.ms': 300000,      # 5 min for slow processors

    # Fetch tuning
    'fetch.min.bytes': 65536,
    'fetch.max.wait.ms': 500,
    'max.partition.fetch.bytes': 1048576,
})
```

### Consumer Lag Monitoring

```bash
# Check all groups and their lag
kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --all-groups \
  | awk 'NR==1 || $6 > 0 {print}' \
  | sort -k6 -rn   # sort by lag desc

# Lag for specific group
kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group orders-processor \
  --describe

# Reset consumer offset to specific time (use with caution)
kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group orders-processor \
  --topic orders \
  --reset-offsets \
  --to-datetime 2024-01-15T03:00:00.000 \
  --dry-run   # remove --dry-run to execute
```

---

## Producer Configuration

```python
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'kafka:9092',

    # Durability: all ISR must acknowledge
    'acks': 'all',

    # Idempotence: prevents duplicates on retry
    'enable.idempotence': True,

    # Compression (lz4 is best latency/throughput tradeoff)
    'compression.type': 'lz4',

    # Batching for throughput
    'linger.ms': 5,                  # wait up to 5ms to batch
    'batch.size': 65536,             # 64 KB batch
    'buffer.memory': 67108864,       # 64 MB total buffer

    # Retry configuration
    'retries': 2147483647,           # effectively infinite with idempotence
    'delivery.timeout.ms': 120000,   # 2 min total delivery attempt

    # Ordering: keep messages in order within partition
    'max.in.flight.requests.per.connection': 5,  # safe with idempotence
})
```

---

## Security: SASL_SSL + ACLs

```properties
# Broker: SASL/SCRAM-SHA-512 over TLS
listeners=SASL_SSL://:9092
ssl.keystore.location=/etc/kafka/ssl/kafka.server.keystore.jks
ssl.keystore.password=${SSL_KEYSTORE_PASSWORD}
ssl.truststore.location=/etc/kafka/ssl/kafka.server.truststore.jks
ssl.truststore.password=${SSL_TRUSTSTORE_PASSWORD}
ssl.client.auth=required             # mTLS: require client certificates

sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512

# ACL authorization
authorizer.class.name=kafka.security.authorizer.AclAuthorizer
super.users=User:admin
```

```bash
# Create user credentials (stored in ZooKeeper/KRaft metadata)
kafka-configs.sh --bootstrap-server kafka:9092 \
  --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=secret]' \
  --entity-type users \
  --entity-name orders-producer

# Grant producer ACL
kafka-acls.sh --bootstrap-server kafka:9092 \
  --add \
  --allow-principal User:orders-producer \
  --operation Write \
  --topic orders

# Grant consumer group ACL
kafka-acls.sh --bootstrap-server kafka:9092 \
  --add \
  --allow-principal User:orders-consumer \
  --operation Read \
  --topic orders \
  --group orders-processor

# List all ACLs
kafka-acls.sh --bootstrap-server kafka:9092 --list
```

---

## Strimzi Kafka on Kubernetes

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: production
  namespace: kafka
spec:
  kafka:
    version: 3.7.0
    replicas: 3
    listeners:
      - name: tls
        port: 9093
        type: internal
        tls: true
        authentication:
          type: scram-sha-512
    config:
      default.replication.factor: 3
      min.insync.replicas: 2
      unclean.leader.election.enable: "false"
      auto.create.topics.enable: "false"
      log.retention.hours: 168
      compression.type: producer
      num.network.threads: 8
      num.io.threads: 16
    storage:
      type: jbod
      volumes:
        - id: 0
          type: persistent-claim
          size: 500Gi
          class: fast-ssd
          deleteClaim: false
    rack:
      topologyKey: topology.kubernetes.io/zone
    resources:
      requests:
        memory: 16Gi
        cpu: "4"
      limits:
        memory: 16Gi
        cpu: "8"
    jvmOptions:
      -Xms: 6144m
      -Xmx: 6144m
    metricsConfig:
      type: jmxPrometheusExporter
      valueFrom:
        configMapKeyRef:
          name: kafka-metrics
          key: kafka-metrics-config.yml

  zookeeper:
    replicas: 3
    storage:
      type: persistent-claim
      size: 10Gi
      class: fast-ssd

  entityOperator:
    topicOperator: {}
    userOperator: {}
```

---

## Prometheus Monitoring

```yaml
# JMX exporter config (kafka-metrics-config.yml)
lowercaseOutputName: true
rules:
  # Under-replicated partitions — should be 0
  - pattern: kafka.server<type=ReplicaManager, name=UnderReplicatedPartitions><>Value
    name: kafka_server_replicamanager_underreplicatedpartitions
    type: GAUGE

  # Active controller — exactly 1 in cluster
  - pattern: kafka.controller<type=KafkaController, name=ActiveControllerCount><>Value
    name: kafka_controller_active_count
    type: GAUGE

  # ISR shrinks — rate of in-sync replica shrinkage (should be ~0)
  - pattern: kafka.server<type=ReplicaManager, name=IsrShrinksPerSec><>(.+)
    name: kafka_server_replicamanager_isrshrinks_total
    type: COUNTER

  # Bytes in/out per topic
  - pattern: kafka.server<type=BrokerTopicMetrics, name=(BytesIn|BytesOut)PerSec, topic=(.+)><>OneMinuteRate
    name: kafka_server_brokertopicmetrics_$1_rate
    labels:
      topic: $2
    type: GAUGE

  # Request latency
  - pattern: kafka.network<type=RequestMetrics, name=TotalTimeMs, request=(.+)><>99thPercentile
    name: kafka_network_requestmetrics_totaltime_p99_ms
    labels:
      request: $1
    type: GAUGE
```

```yaml
# Prometheus alert rules
groups:
  - name: kafka_alerts
    rules:
      - alert: KafkaUnderReplicatedPartitions
        expr: kafka_server_replicamanager_underreplicatedpartitions > 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Under-replicated partitions detected: {{ $value }}"

      - alert: KafkaNoActiveController
        expr: sum(kafka_controller_active_count) != 1
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Kafka cluster has {{ $value }} active controllers (expected 1)"

      - alert: KafkaConsumerGroupLag
        expr: kafka_consumer_group_lag{group=~".*-processor"} > 100000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Consumer group {{ $labels.group }} lag {{ $value }} messages"

      - alert: KafkaBrokerDiskHigh
        expr: kubelet_volume_stats_available_bytes{persistentvolumeclaim=~"data-production-kafka-.*"} / kubelet_volume_stats_capacity_bytes < 0.15
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Kafka broker disk < 15% free"
```

---

## Capacity Planning

```python
# capacity_planner.py
def estimate_kafka_storage(
    topics: list[dict],        # [{name, partitions, replication_factor, msg_size_bytes, msgs_per_sec, retention_hours}]
    overhead_factor: float = 1.3,  # 30% overhead for indexes, metadata
) -> dict:
    total_bytes_per_sec = 0
    total_storage_bytes = 0

    for t in topics:
        write_bytes_sec = t["msg_size_bytes"] * t["msgs_per_sec"]
        replicated_bytes_sec = write_bytes_sec * t["replication_factor"]
        storage = replicated_bytes_sec * t["retention_hours"] * 3600

        total_bytes_per_sec += replicated_bytes_sec
        total_storage_bytes += storage

    total_storage_with_overhead = total_storage_bytes * overhead_factor

    return {
        "total_write_throughput_MBps": round(total_bytes_per_sec / 1e6, 1),
        "total_storage_TB": round(total_storage_with_overhead / 1e12, 2),
        "recommended_broker_disk_TB": round(total_storage_with_overhead / 3 / 1e12, 2),  # spread across 3 brokers
    }

# Example
result = estimate_kafka_storage([
    {"name": "orders", "partitions": 24, "replication_factor": 3,
     "msg_size_bytes": 2048, "msgs_per_sec": 10000, "retention_hours": 168},
    {"name": "events", "partitions": 48, "replication_factor": 3,
     "msg_size_bytes": 512, "msgs_per_sec": 100000, "retention_hours": 72},
])
# → {"total_write_throughput_MBps": 213.9, "total_storage_TB": 1.47, ...}
```

---

## Production Readiness Checklist

```
Cluster Configuration:
[ ] 3+ brokers, 3 controllers (KRaft) or 3 ZooKeeper nodes
[ ] min.insync.replicas=2 (all topics)
[ ] unclean.leader.election.enable=false
[ ] auto.create.topics.enable=false
[ ] Rack awareness configured (broker.rack + replica.selector.class)

Data Safety:
[ ] Replication factor ≥ 3 for all production topics
[ ] Producer acks=all + enable.idempotence=true
[ ] Consumer manual commit (not auto-commit)
[ ] DLQ (Dead Letter Queue) for poison messages

Security:
[ ] SASL_SSL on all listeners
[ ] mTLS (ssl.client.auth=required) for internal communication
[ ] ACLs on all topics (no wildcard consumer access)
[ ] Credentials rotated via external secret manager

Operations:
[ ] JMX metrics exposed to Prometheus
[ ] Alerts on UnderReplicatedPartitions, ISR shrinks, disk ≥ 85%
[ ] Consumer group lag dashboard per consumer group
[ ] Kafka upgrade runbook documented (rolling restart procedure)
[ ] Topic retention and cleanup policy reviewed quarterly
```

---

## Anti-Patterns

1. **`acks=1` for critical data** — broker ack without ISR confirmation loses data on leader failure; use `acks=all` for anything that matters.
2. **Auto-create topics enabled** — rogue producers create under-configured topics with replication factor 1; disable and manage topics via IaC.
3. **Consumer auto-commit** — marks offset committed before processing completes; use manual commit after successful processing.
4. **All topics in one consumer group** — a single group consuming many unrelated topics makes lag monitoring noisy and rebalance storms more likely.
5. **No partition count headroom** — scaling consumer instances beyond partition count gains nothing; plan partitions for 2–3x future consumer scale.

---

## References

- Confluent production deployment: `docs.confluent.io/platform/current/kafka/deployment.html`
- Strimzi operator: `strimzi.io/docs/operators/latest/overview.html`
- Kafka monitoring JMX: `docs.confluent.io/platform/current/kafka/monitoring.html`
- KRaft mode: `kafka.apache.org/documentation/#kraft`
- Related skills: `[[apache-kafka]]`, `[[infra-kafka-cost-optimizer]]`, `[[infra-streaming-reliability-review]]`, `[[dataops-disaster-recovery-review]]`
