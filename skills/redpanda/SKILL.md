---
name: redpanda
description: Redpanda Kafka-compatible streaming — cluster setup, topic config, rpk CLI, producer/consumer tuning, tiered storage, Schema Registry, Kafka Connect compatibility, monitoring, Docker/Kubernetes deployment
---

# Redpanda

## When to Use

Load this skill when the user needs to:
- Deploy or operate a Redpanda cluster (Docker, Kubernetes, bare metal)
- Create/configure topics with `rpk` or Redpanda Console
- Tune producers and consumers for Redpanda's C++ broker
- Enable and configure tiered storage (Shadow Indexing) with S3/GCS/ABS
- Use Redpanda's built-in Schema Registry or Kafka Connect compatibility
- Secure a Redpanda cluster with SASL/SCRAM, mTLS, or ACLs
- Monitor Redpanda via Prometheus/Grafana
- Migrate from Apache Kafka to Redpanda
- Write Python clients using `confluent-kafka` or `aiokafka` against Redpanda

### Redpanda vs Apache Kafka — When to Choose Each

| Dimension | Redpanda | Apache Kafka |
|---|---|---|
| Architecture | Single C++ binary, Raft consensus per partition | JVM + ZooKeeper (legacy) or KRaft (3.x+) |
| Ops complexity | No ZooKeeper, no JVM tuning, no controller separate process | KRaft simplifies but still JVM GC pauses |
| Tail latency (p99) | Sub-millisecond typical; no GC pauses | 2–10 ms typical; GC spikes to 50–200 ms |
| Throughput | Comparable; thread-per-core model | Comparable at scale with page-cache tuning |
| Tiered storage | Built-in Shadow Indexing (no third-party plugin) | Tiered storage in 3.6+ (preview); Confluent Tiered Storage |
| Schema Registry | Built-in (no separate process) | Confluent Schema Registry (separate service) |
| Kafka Connect | Compatible (run standard Connect workers) | Native |
| MirrorMaker 2 | Fully compatible | Native |
| Transactions / EOS | Fully supported | Fully supported |
| Ecosystem maturity | Younger; most Kafka tooling works | Mature; widest ecosystem |
| License | BSL 1.1 (free for most uses) / Enterprise | Apache 2.0 / Confluent Commercial |

**Choose Redpanda when**: low operational overhead matters, you want lowest latency without JVM tuning, you prefer a single binary, or you need built-in tiered storage without add-ons.

**Choose Kafka when**: you need the widest ecosystem compatibility, Confluent Platform features, or your team has deep Kafka expertise.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Redpanda Node (single binary)           │
│                                                          │
│  Kafka API  (:9092)   HTTP Proxy (:8082)                 │
│  Admin API  (:9644)   Schema Registry (:8081)            │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │              Partition Raft Groups                │   │
│  │  [topic/0: leader]  [topic/1: follower]  ...     │   │
│  └──────────────────────────────────────────────────┘   │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │         Shadow Indexing (Tiered Storage)          │   │
│  │   Local NVMe  →  S3/GCS/ABS (remote segments)   │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

**Raft-based replication** — each partition is an independent Raft group. The partition leader handles reads and writes; followers replicate via Raft log entries. There is no global controller; partition leadership is distributed across all brokers.

**Thread-per-core (Seastar framework)** — each CPU core owns a dedicated set of partitions. No cross-core locking for data path operations. This eliminates JVM GC pauses and produces consistent low latency.

**Shadow Indexing** — Redpanda's built-in tiered storage. Local segments are uploaded to object storage (S3/GCS/ABS) in the background. Remote segments are accessible via a manifest index without downloading the full segment. This decouples storage capacity from broker disk, enabling retention of months of data on cheap object storage.

**Single binary** — the Kafka API, Admin API, Schema Registry, and HTTP Proxy all run in one process. No separate ZooKeeper, no Schema Registry container.

---

## Docker Compose — Local Setup

### Single-Node (Development)

```yaml
# docker-compose.yml — Redpanda single-node + Console
version: "3.9"
services:
  redpanda:
    image: redpandadata/redpanda:v24.1.13
    container_name: redpanda
    command:
      - redpanda
      - start
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
      - --pandaproxy-addr internal://0.0.0.0:8082,external://0.0.0.0:18082
      - --advertise-pandaproxy-addr internal://redpanda:8082,external://localhost:18082
      - --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081
      - --rpc-addr redpanda:33145
      - --advertise-rpc-addr redpanda:33145
      - --mode dev-container      # disables disk/CPU checks for dev
      - --smp 2
      - --memory 2G
      - --default-log-level=warn
    volumes:
      - redpanda_data:/var/lib/redpanda/data
    ports:
      - "18081:18081"   # Schema Registry (external)
      - "18082:18082"   # HTTP Proxy (external)
      - "19092:19092"   # Kafka API (external)
      - "19644:9644"    # Admin API (external)

  console:
    image: redpandadata/console:v2.6.0
    container_name: redpanda-console
    ports:
      - "8080:8080"
    environment:
      CONFIG_FILEPATH: /tmp/config.yml
    volumes:
      - ./console-config.yml:/tmp/config.yml:ro
    depends_on:
      - redpanda

volumes:
  redpanda_data:
```

```yaml
# console-config.yml
kafka:
  brokers: ["redpanda:9092"]
  schemaRegistry:
    enabled: true
    urls: ["http://redpanda:8081"]
redpanda:
  adminApi:
    enabled: true
    urls: ["http://redpanda:9644"]
```

### 3-Broker Cluster (Staging / Testing)

```yaml
# docker-compose.cluster.yml
version: "3.9"

x-redpanda-node: &redpanda-node
  image: redpandadata/redpanda:v24.1.13
  volumes:
    - type: volume
      source: redpanda_data_${NODE_ID:-0}
      target: /var/lib/redpanda/data

services:
  redpanda-0:
    <<: *redpanda-node
    container_name: redpanda-0
    command:
      - redpanda start
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda-0:9092,external://localhost:19092
      - --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081
      - --rpc-addr redpanda-0:33145
      - --advertise-rpc-addr redpanda-0:33145
      - --smp 2
      - --memory 2G
      - --seeds redpanda-0:33145,redpanda-1:33145,redpanda-2:33145
      - --node-id 0
      - --mode dev-container
      - --default-log-level=warn
    ports:
      - "19092:19092"
      - "18081:18081"
      - "19644:9644"

  redpanda-1:
    <<: *redpanda-node
    container_name: redpanda-1
    command:
      - redpanda start
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:29092
      - --advertise-kafka-addr internal://redpanda-1:9092,external://localhost:29092
      - --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:28081
      - --rpc-addr redpanda-1:33145
      - --advertise-rpc-addr redpanda-1:33145
      - --smp 2
      - --memory 2G
      - --seeds redpanda-0:33145,redpanda-1:33145,redpanda-2:33145
      - --node-id 1
      - --mode dev-container
      - --default-log-level=warn
    ports:
      - "29092:29092"
      - "28081:28081"
      - "29644:9644"

  redpanda-2:
    <<: *redpanda-node
    container_name: redpanda-2
    command:
      - redpanda start
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:39092
      - --advertise-kafka-addr internal://redpanda-2:9092,external://localhost:39092
      - --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:38081
      - --rpc-addr redpanda-2:33145
      - --advertise-rpc-addr redpanda-2:33145
      - --smp 2
      - --memory 2G
      - --seeds redpanda-0:33145,redpanda-1:33145,redpanda-2:33145
      - --node-id 2
      - --mode dev-container
      - --default-log-level=warn
    ports:
      - "39092:39092"
      - "38081:38081"
      - "39644:9644"

  console:
    image: redpandadata/console:v2.6.0
    ports:
      - "8080:8080"
    environment:
      CONFIG_FILEPATH: /tmp/config.yml
    volumes:
      - ./console-config-cluster.yml:/tmp/config.yml:ro
    depends_on:
      - redpanda-0

volumes:
  redpanda_data_0:
  redpanda_data_1:
  redpanda_data_2:
```

```yaml
# console-config-cluster.yml
kafka:
  brokers:
    - redpanda-0:9092
    - redpanda-1:9092
    - redpanda-2:9092
  schemaRegistry:
    enabled: true
    urls: ["http://redpanda-0:8081"]
redpanda:
  adminApi:
    enabled: true
    urls:
      - "http://redpanda-0:9644"
      - "http://redpanda-1:9644"
      - "http://redpanda-2:9644"
```

---

## Kubernetes Deployment

### Helm Chart (redpanda/redpanda)

```bash
helm repo add redpanda https://charts.redpanda.com
helm repo update

# Install with custom values
helm install redpanda redpanda/redpanda \
  --namespace redpanda \
  --create-namespace \
  --values values.yaml \
  --wait
```

### Production values.yaml

```yaml
# values.yaml — Redpanda Helm chart v5.x
image:
  tag: "v24.1.13"

clusterSpec:
  nodeSelector:
    kubernetes.io/os: linux
  tolerations: []

# 3-broker cluster
statefulset:
  replicas: 3

resources:
  cpu:
    cores: 4          # Redpanda pins to CPU cores (thread-per-core)
    overprovisioned: false
  memory:
    container:
      max: 8Gi
    redpanda:
      reserveMemory: 1Gi   # reserved for OS/Seastar internals
      memory: 6Gi          # heap available to Redpanda

storage:
  persistentVolume:
    enabled: true
    size: 500Gi
    storageClass: "fast-ssd"   # NVMe-backed StorageClass recommended

# Tiered storage — S3
storage:
  tiered:
    config:
      cloud_storage_enabled: true
      cloud_storage_region: us-east-1
      cloud_storage_bucket: my-redpanda-tiered
      cloud_storage_credentials_source: aws_instance_metadata   # or iam_role
    persistentVolume:
      enabled: true
      size: 20Gi          # local cache only; data lives in S3
      storageClass: "fast-ssd"

# Listeners
listeners:
  kafka:
    port: 9092
    tls:
      enabled: true
      cert: "tls-cert"
  admin:
    port: 9644
    tls:
      enabled: true
      cert: "tls-cert"
  schemaRegistry:
    port: 8081
    tls:
      enabled: false    # internal cluster only

# SASL authentication
auth:
  sasl:
    enabled: true
    mechanism: SCRAM-SHA-256
    secretRef: redpanda-users
    users:
      - name: admin
        password: "${ADMIN_PASSWORD}"
        mechanism: SCRAM-SHA-256
      - name: app-producer
        password: "${PRODUCER_PASSWORD}"
        mechanism: SCRAM-SHA-256

# TLS certificates (cert-manager)
tls:
  enabled: true
  certs:
    tls-cert:
      issuerRef:
        name: cluster-issuer
        kind: ClusterIssuer
      caEnabled: true

# Redpanda configuration overrides
config:
  cluster:
    log_compression_type: snappy
    default_topic_replications: 3
    default_topic_partitions: 12
    kafka_batch_max_bytes: 10485760          # 10 MB
    group_max_session_timeout_ms: 600000
    auto_create_topics_enabled: false        # enforce explicit topic creation
  tunable:
    kafka_connection_rate_limit: 10000
    max_compacted_log_segment_size: 536870912  # 512 MB

# Console (Redpanda Console sidecar)
console:
  enabled: true
  ingress:
    enabled: true
    className: nginx
    hosts:
      - host: redpanda-console.example.com
        paths:
          - path: /
            pathType: Prefix
```

### Redpanda Operator (CRD-based management)

```bash
helm install redpanda-operator redpanda/operator \
  --namespace redpanda-operator \
  --create-namespace

# Apply a Redpanda custom resource
kubectl apply -f redpanda-cluster.yaml
```

```yaml
# redpanda-cluster.yaml
apiVersion: cluster.redpanda.com/v1alpha2
kind: Redpanda
metadata:
  name: redpanda-prod
  namespace: redpanda
spec:
  chartRef: {}    # uses operator-managed chart version
  clusterSpec:
    image:
      tag: v24.1.13
    statefulset:
      replicas: 3
    resources:
      cpu:
        cores: 4
      memory:
        container:
          max: 8Gi
        redpanda:
          memory: 6Gi
    storage:
      persistentVolume:
        size: 500Gi
        storageClass: fast-ssd
    auth:
      sasl:
        enabled: true
        mechanism: SCRAM-SHA-256
        users:
          - name: admin
            password: "changeme"
```

---

## rpk CLI

`rpk` is the official Redpanda CLI. It replaces Kafka's `kafka-topics.sh`, `kafka-consumer-groups.sh`, and the Admin UI for most operations.

### Cluster Operations

```bash
# Configure default profile (persisted to ~/.config/rpk/rpk.yaml)
rpk profile create local \
  --set kafka_api.brokers=localhost:19092 \
  --set admin_api.addresses=localhost:19644

rpk profile use local

# Cluster health overview
rpk cluster health

# List all brokers and their status
rpk cluster info

# View cluster configuration
rpk cluster config get

# Modify cluster config (e.g., allow topic auto-creation)
rpk cluster config set auto_create_topics_enabled true

# Diagnostics bundle (attach to support tickets)
rpk debug bundle --output /tmp/redpanda-bundle.zip
```

### Topic Operations

```bash
# Create topic with production settings
rpk topic create orders \
  --partitions 12 \
  --replicas 3 \
  --config retention.ms=604800000 \
  --config min.insync.replicas=2 \
  --config compression.type=snappy

# Describe topic (partitions, leaders, replicas, config)
rpk topic describe orders

# List all topics
rpk topic list

# Produce test messages (interactive)
rpk topic produce orders

# Produce with key (tab-separated key\tvalue)
echo -e "order-123\t{\"status\":\"placed\",\"amount\":99.99}" | \
  rpk topic produce orders --key-schema-id topic --format "%k\t%v\n"

# Consume from beginning, print key and value
rpk topic consume orders --from-beginning --format "%k %v\n" --num 100

# Consume from specific offset
rpk topic consume orders --offset 500 --partitions 0,1

# Delete topic
rpk topic delete orders

# Modify topic config
rpk topic alter-config orders \
  --set retention.ms=2592000000 \
  --set retention.bytes=10737418240

# Add partitions (irreversible for keyed topics)
rpk topic add-partitions orders --num 6
```

### Consumer Group Operations

```bash
# List consumer groups
rpk group list

# Describe group — shows offsets and lag per partition
rpk group describe orders-service-v1

# Seek consumer group offsets
rpk group seek orders-service-v1 \
  --topics orders \
  --to start             # earliest

rpk group seek orders-service-v1 \
  --topics orders \
  --to end               # latest (skip all pending)

rpk group seek orders-service-v1 \
  --topics orders \
  --to timestamp:2024-06-01T00:00:00Z

# Delete consumer group (must have no active members)
rpk group delete orders-service-v1
```

### ACL Management

```bash
# Create user (SASL/SCRAM)
rpk acl user create app-producer --password "S3cur3Pass!" --mechanism SCRAM-SHA-256
rpk acl user create app-consumer --password "S3cur3Pass!" --mechanism SCRAM-SHA-256

# Grant producer permissions on topic
rpk acl create \
  --allow-principal User:app-producer \
  --operation write,describe \
  --topic orders

# Grant consumer permissions (read topic + group)
rpk acl create \
  --allow-principal User:app-consumer \
  --operation read,describe \
  --topic orders

rpk acl create \
  --allow-principal User:app-consumer \
  --operation read \
  --group orders-service-v1

# List all ACLs
rpk acl list

# Delete ACL
rpk acl delete \
  --allow-principal User:app-producer \
  --operation write \
  --topic orders
```

### System Tuning

```bash
# Tune OS settings for production (run as root on each broker host)
rpk redpanda tune all

# Tune specific subsystem
rpk redpanda tune disk_irq
rpk redpanda tune net
rpk redpanda tune cpu

# Verify tuning status
rpk redpanda check
```

---

## Topic Configuration

### Key Configuration Properties

```bash
# Create a compacted changelog topic (for KV-style state)
rpk topic create user-profiles \
  --partitions 24 \
  --replicas 3 \
  --config cleanup.policy=compact \
  --config min.insync.replicas=2 \
  --config segment.bytes=134217728 \
  --config min.cleanable.dirty.ratio=0.1 \
  --config delete.retention.ms=86400000

# Create a time-bounded event topic with tiered storage
rpk topic create clickstream \
  --partitions 48 \
  --replicas 3 \
  --config retention.ms=-1 \
  --config retention.bytes=-1 \
  --config redpanda.remote.write=true \
  --config redpanda.remote.read=true \
  --config redpanda.remote.delete=true \
  --config segment.bytes=536870912
```

### Configuration Reference

| Property | Recommended Value | Notes |
|---|---|---|
| `replication.factor` | 3 | Survive 1 broker loss |
| `min.insync.replicas` | 2 | With `acks=all`, ensures 2 in-sync copies |
| `retention.ms` | `604800000` (7d) | `-1` = infinite (use with tiered storage) |
| `retention.bytes` | `-1` or explicit | Per-partition byte cap |
| `cleanup.policy` | `delete` or `compact` | `compact` for event-sourcing topics |
| `segment.bytes` | `536870912` (512 MB) | Larger segments = fewer S3 uploads |
| `compression.type` | `snappy` or `lz4` | Set on broker/topic or let producer decide |
| `redpanda.remote.write` | `true` | Upload segments to tiered storage |
| `redpanda.remote.read` | `true` | Serve reads from tiered storage when local absent |
| `redpanda.remote.delete` | `true` | Allow deleting remote segments on expiry |
| `unclean.leader.election.enable` | `false` | Prevent data loss from out-of-sync leader |

---

## Producer Tuning

### Durability vs Throughput

```python
# HIGH DURABILITY — production default for critical data
durable_config = {
    "bootstrap.servers": "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092",
    "client.id": "orders-producer-v1",

    # Durability
    "acks": "all",                               # wait for all ISR acks
    "enable.idempotence": True,                  # exactly-once per session
    "max.in.flight.requests.per.connection": 5,  # required for idempotence

    # Throughput
    "batch.size": 65536,            # 64 KB
    "linger.ms": 5,                 # Redpanda: lower than Kafka — sub-ms ACK latency
    "compression.type": "snappy",   # snappy: fast; lz4: balanced; zstd: best ratio

    # Retry resilience
    "retries": 2147483647,
    "delivery.timeout.ms": 30000,   # 30s total (Redpanda is faster; reduce timeout)
    "request.timeout.ms": 10000,
    "buffer.memory": 67108864,      # 64 MB
    "queue.buffering.max.messages": 1000000,
}

# HIGH THROUGHPUT — telemetry / analytics events
throughput_config = {
    "bootstrap.servers": "redpanda-0:9092",
    "acks": "1",
    "batch.size": 131072,     # 128 KB
    "linger.ms": 20,
    "compression.type": "lz4",
    "buffer.memory": 134217728,
    "queue.buffering.max.kbytes": 1048576,  # 1 GB total buffer
}
```

### Redpanda-Specific Notes

- **`linger.ms` can be lower** — Redpanda's Raft commit is faster than Kafka's ISR flush. Values of 1–5 ms are practical for low-latency paths where Kafka would need 10–20 ms.
- **`compression.type=producer`** — let the producer decide; broker will not recompress. This avoids CPU overhead on the broker side.
- **`acks=all` is safe with 1 ms linger** — because Redpanda's Raft write is NVMe-synchronous with no JVM GC pauses.
- **No JVM heap tuning** — all memory config for Redpanda is done server-side; no producer-side JVM flags needed.

---

## Consumer Tuning

### Core Settings

```python
consumer_config = {
    "bootstrap.servers": "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092",
    "group.id": "orders-service-v1",
    "client.id": "orders-consumer-1",

    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,           # always manage commits manually

    # Redpanda supports cooperative-sticky — use it
    "partition.assignment.strategy": "cooperative-sticky",

    "session.timeout.ms": 30000,
    "heartbeat.interval.ms": 3000,
    "max.poll.interval.ms": 300000,

    # Fetch tuning — Redpanda responds fast; keep fetch.max.wait.ms low
    "fetch.min.bytes": 1,
    "fetch.max.wait.ms": 100,    # 100ms max wait (vs 500ms default for Kafka)
    "max.poll.records": 500,
    "fetch.max.bytes": 52428800, # 50 MB max per fetch response
}
```

### Commit Strategies

```python
from confluent_kafka import Consumer, KafkaError, TopicPartition
import logging

logger = logging.getLogger(__name__)

consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])

# Strategy 1: Synchronous commit after each batch (safe, slightly lower throughput)
try:
    while True:
        msgs = consumer.consume(num_messages=500, timeout=1.0)
        if not msgs:
            continue
        for msg in msgs:
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue
            process(msg)
        consumer.commit(asynchronous=False)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()

# Strategy 2: Async commit in loop, sync commit on shutdown
try:
    while True:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            logger.error("Consumer error: %s", msg.error())
            continue
        process(msg)
        consumer.commit(asynchronous=True)
except KeyboardInterrupt:
    consumer.commit(asynchronous=False)
finally:
    consumer.close()
```

### Rebalance Handling

```python
def on_assign(consumer, partitions):
    logger.info("Partitions assigned: %s", [p.partition for p in partitions])
    consumer.assign(partitions)

def on_revoke(consumer, partitions):
    # Commit before revoke to avoid reprocessing
    consumer.commit(asynchronous=False)
    logger.info("Partitions revoked: %s", [p.partition for p in partitions])

consumer.subscribe(
    ["orders"],
    on_assign=on_assign,
    on_revoke=on_revoke,
)
```

---

## Tiered Storage (Shadow Indexing)

Redpanda's Shadow Indexing offloads log segments to object storage. Old segments can be deleted locally while remaining accessible via the remote segment index.

### Enable Cluster-Wide (redpanda.yaml / Helm)

```yaml
# In Helm values.yaml → config.cluster
config:
  cluster:
    cloud_storage_enabled: true
    cloud_storage_region: us-east-1
    cloud_storage_bucket: redpanda-tiered-prod
    cloud_storage_credentials_source: aws_instance_metadata
    # Optional tuning
    cloud_storage_upload_loop_initial_backoff_ms: 100
    cloud_storage_upload_loop_max_backoff_ms: 10000
    cloud_storage_max_segment_size: 536870912     # 512 MB max segment before upload
    cloud_storage_cache_size: 21474836480         # 20 GB local cache for reads
```

### GCS Configuration

```yaml
config:
  cluster:
    cloud_storage_enabled: true
    cloud_storage_region: us-central1
    cloud_storage_bucket: redpanda-tiered-gcs
    cloud_storage_credentials_source: gcp_instance_metadata
```

### Azure Blob Storage

```yaml
config:
  cluster:
    cloud_storage_enabled: true
    cloud_storage_azure_storage_account: myaccount
    cloud_storage_azure_container: redpanda-tiered
    cloud_storage_azure_shared_key_secret: AZURE_STORAGE_KEY_SECRET_NAME
```

### Enable Tiered Storage Per Topic

```bash
# Enable on existing topic
rpk topic alter-config clickstream \
  --set redpanda.remote.write=true \
  --set redpanda.remote.read=true \
  --set redpanda.remote.delete=true \
  --set retention.ms=-1          # keep remote data indefinitely
  --set local.retention.ms=86400000  # keep local copy for 1 day

# Create with tiered storage and infinite retention
rpk topic create events \
  --partitions 48 \
  --replicas 3 \
  --config redpanda.remote.write=true \
  --config redpanda.remote.read=true \
  --config retention.ms=-1 \
  --config segment.bytes=536870912
```

### Recovery from Object Storage

```bash
# Enable remote recovery for a new/replacement cluster
# Set in redpanda.yaml before starting:
#   cloud_storage_enabled: true
#   cloud_storage_recovery_enabled: true
# Then create topic with same name — Redpanda will hydrate from remote segments

# Force topic recovery manually
rpk cluster config set cloud_storage_recovery_topic_validation_mode: check_manifest_exists
```

---

## Schema Registry

Redpanda includes a Confluent-compatible Schema Registry at port 8081. No separate service needed.

### REST API

```bash
# Register an Avro schema
curl -X POST http://localhost:18081/subjects/orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"order_id\",\"type\":\"long\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"status\",\"type\":\"string\"}]}"
  }'

# Get schema by subject and version
curl http://localhost:18081/subjects/orders-value/versions/latest

# List all subjects
curl http://localhost:18081/subjects

# Check compatibility before registering
curl -X POST http://localhost:18081/compatibility/subjects/orders-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "<escaped_schema>"}'

# Set compatibility mode per subject
curl -X PUT http://localhost:18081/config/orders-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
```

### Compatibility Modes

| Mode | Rule | When to Use |
|---|---|---|
| `BACKWARD` (default) | New schema can read old messages | Consumers upgrade before producers |
| `FORWARD` | Old schema can read new messages | Producers upgrade before consumers |
| `FULL` | Both backward and forward | Safe rolling upgrades either direction |
| `NONE` | No checks | Dev/test only — dangerous in production |

### Python with Avro Serialization

```python
from confluent_kafka import Producer, Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import logging

logger = logging.getLogger(__name__)

# Schema Registry client pointing to Redpanda's built-in SR
schema_registry_client = SchemaRegistryClient({
    "url": "http://localhost:18081",
})

ORDER_SCHEMA = """
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.orders",
  "fields": [
    {"name": "order_id", "type": "long"},
    {"name": "customer_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "status", "type": {"type": "enum", "name": "Status",
      "symbols": ["PLACED", "PROCESSING", "SHIPPED", "CANCELLED"]}},
    {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"}
  ]
}
"""

avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry_client,
    schema_str=ORDER_SCHEMA,
    conf={"auto.register.schemas": True},
)
avro_deserializer = AvroDeserializer(
    schema_registry_client=schema_registry_client,
    schema_str=ORDER_SCHEMA,
)

# --- Producer ---
producer = Producer({
    "bootstrap.servers": "localhost:19092",
    "acks": "all",
    "enable.idempotence": True,
})

def send_order(order: dict) -> None:
    serialized = avro_serializer(
        order,
        SerializationContext("orders", MessageField.VALUE),
    )
    producer.produce(
        topic="orders",
        key=str(order["order_id"]).encode(),
        value=serialized,
        on_delivery=lambda err, msg: logger.error(err) if err else None,
    )
    producer.poll(0)

producer.flush()

# --- Consumer ---
consumer = Consumer({
    "bootstrap.servers": "localhost:19092",
    "group.id": "orders-processor-v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe(["orders"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error("Consumer error: %s", msg.error())
            continue
        order = avro_deserializer(
            msg.value(),
            SerializationContext("orders", MessageField.VALUE),
        )
        logger.info("Processing order %s: %s", order["order_id"], order["status"])
        consumer.commit(asynchronous=False)
finally:
    consumer.close()
```

---

## Kafka Connect Compatibility

Redpanda exposes the full Kafka API; standard Kafka Connect workers connect to it unmodified. Run Connect as a separate service pointing to Redpanda brokers.

### Connect Worker Config

```properties
# connect-distributed.properties
bootstrap.servers=redpanda-0:9092,redpanda-1:9092,redpanda-2:9092
group.id=connect-cluster

# Internal topics — Redpanda will create if auto_create_topics_enabled=true
config.storage.topic=_connect-configs
offset.storage.topic=_connect-offsets
status.storage.topic=_connect-status

config.storage.replication.factor=3
offset.storage.replication.factor=3
status.storage.replication.factor=3

key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://redpanda-0:8081

# SASL authentication to Redpanda
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="connect-user" password="connect-secret";

# Producer settings for Connect workers
producer.acks=all
producer.enable.idempotence=true
producer.compression.type=snappy
```

### Debezium PostgreSQL Source Connector

```json
{
  "name": "postgres-cdc-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "debezium_secret",
    "database.dbname": "orders_db",
    "database.server.name": "orders-pg",
    "topic.prefix": "cdc",
    "table.include.list": "public.orders,public.customers",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "dbz_publication",
    "snapshot.mode": "initial",
    "decimal.handling.mode": "string",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "dlq.cdc.postgres-source",
    "errors.deadletterqueue.topic.replication.factor": "3",
    "errors.deadletterqueue.context.headers.enable": "true"
  }
}
```

### S3 Sink Connector

```json
{
  "name": "s3-sink-orders",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "4",
    "topics": "orders",
    "s3.region": "us-east-1",
    "s3.bucket.name": "data-lake-raw",
    "s3.part.size": "67108864",
    "flush.size": "10000",
    "rotate.interval.ms": "60000",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "parquet.codec": "snappy",
    "schema.compatibility": "FULL",
    "locale": "en_US",
    "timezone": "UTC",
    "timestamp.extractor": "RecordField",
    "timestamp.field": "created_at",
    "s3.ssea.name": "aws:kms",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq.s3-sink",
    "errors.deadletterqueue.context.headers.enable": "true"
  }
}
```

---

## Security

### SASL/SCRAM-256

```bash
# Create users via rpk
rpk acl user create admin-user    --password "AdminP@ss!" --mechanism SCRAM-SHA-256
rpk acl user create app-producer  --password "ProdP@ss!"  --mechanism SCRAM-SHA-256
rpk acl user create app-consumer  --password "ConsP@ss!"  --mechanism SCRAM-SHA-256

# Producer ACLs
rpk acl create --allow-principal User:app-producer \
  --operation write,describe,create \
  --topic orders

# Consumer ACLs
rpk acl create --allow-principal User:app-consumer \
  --operation read,describe \
  --topic orders

rpk acl create --allow-principal User:app-consumer \
  --operation read,describe \
  --group orders-service-v1

# Super-user (full cluster access)
rpk acl create --allow-principal User:admin-user \
  --operation all \
  --topic "*" \
  --cluster
```

### Python Client with SASL/SCRAM

```python
sasl_config = {
    "bootstrap.servers": "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092",
    "security.protocol": "SASL_PLAINTEXT",   # use SASL_SSL in production
    "sasl.mechanism": "SCRAM-SHA-256",
    "sasl.username": "app-producer",
    "sasl.password": "ProdP@ss!",
}

producer = Producer({**sasl_config, "acks": "all", "enable.idempotence": True})
```

### mTLS Configuration

```yaml
# In Helm values.yaml
listeners:
  kafka:
    port: 9092
    tls:
      enabled: true
      cert: "tls-cert"
      requireClientAuth: true   # enforce mTLS — reject unauthenticated clients

tls:
  enabled: true
  certs:
    tls-cert:
      issuerRef:
        name: cluster-issuer
        kind: ClusterIssuer
      caEnabled: true
```

```python
# Python client with mTLS
mtls_config = {
    "bootstrap.servers": "redpanda.example.com:9092",
    "security.protocol": "SSL",
    "ssl.ca.location": "/certs/ca.crt",
    "ssl.certificate.location": "/certs/client.crt",
    "ssl.key.location": "/certs/client.key",
    "ssl.endpoint.identification.algorithm": "https",
}
```

### ACL Deny Rules

```bash
# Deny a specific user from a topic (useful for emergency lockout)
rpk acl create --deny-principal User:compromised-service \
  --operation read,write \
  --topic "*"

# List all ACLs
rpk acl list

# Delete ACL
rpk acl delete \
  --allow-principal User:app-producer \
  --operation write \
  --topic orders
```

---

## Monitoring

### Prometheus Metrics Endpoint

Redpanda exposes Prometheus metrics at `:9644/metrics` and `:9644/public_metrics` (stable, non-internal subset).

```yaml
# Prometheus scrape config
scrape_configs:
  - job_name: redpanda
    static_configs:
      - targets:
          - redpanda-0:9644
          - redpanda-1:9644
          - redpanda-2:9644
    metrics_path: /public_metrics
    scrape_interval: 15s
    scrape_timeout: 10s
```

### Key Metrics

| Metric | Alert Threshold | Meaning |
|---|---|---|
| `redpanda_kafka_request_latency_seconds` (p99) | > 50 ms | End-to-end produce/fetch latency |
| `redpanda_cluster_unavailable_partitions` | > 0 | Partitions with no leader |
| `redpanda_cluster_under_replicated_replicas` | > 0 | Replicas falling behind Raft leader |
| `redpanda_kafka_consumer_group_committed_offset` | lag > threshold | Consumer group lag |
| `redpanda_storage_disk_free_bytes` | < 20% free | Disk pressure |
| `redpanda_io_queue_depth` | sustained > 100 | Storage I/O saturation |
| `redpanda_memory_available_memory_bytes` | < 500 MB | Memory pressure |
| `redpanda_kafka_records_fetched_total` | — | Consumer throughput |
| `redpanda_kafka_records_produced_total` | — | Producer throughput |
| `redpanda_raft_leadership_changes` | spike | Partition leader elections |

### Consumer Lag via rpk

```bash
# Real-time lag per consumer group
watch -n 5 rpk group describe orders-service-v1

# Output includes: GROUP, TOPIC, PARTITION, CURRENT-OFFSET, LOG-END-OFFSET, LAG, MEMBER-ID
```

### Prometheus Alert Rules

```yaml
# alerts.yaml
groups:
  - name: redpanda
    rules:
      - alert: RedpandaUnavailablePartitions
        expr: redpanda_cluster_unavailable_partitions > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Redpanda has unavailable partitions"
          description: "{{ $value }} partitions have no leader"

      - alert: RedpandaUnderReplicatedReplicas
        expr: redpanda_cluster_under_replicated_replicas > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Under-replicated replicas detected"

      - alert: RedpandaHighProduceLatency
        expr: histogram_quantile(0.99, rate(redpanda_kafka_request_latency_seconds_bucket{request="produce"}[5m])) > 0.05
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "p99 produce latency above 50ms"

      - alert: RedpandaDiskLow
        expr: (redpanda_storage_disk_free_bytes / redpanda_storage_disk_total_bytes) < 0.2
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Redpanda disk usage above 80%"
```

### Grafana Dashboard

Import the official Redpanda Grafana dashboard (ID **18135**) or deploy via the Helm chart:

```yaml
# In Helm values.yaml
monitoring:
  enabled: true
  scrapeInterval: 15s
  labels:
    prometheus: "kube-prometheus"
```

### Cluster Health Check (rpk)

```bash
# Overall health — lists any degraded nodes or under-replicated partitions
rpk cluster health

# Expected healthy output:
# Healthy:               true
# Controller ID:         0
# All nodes:             [0 1 2]
# Nodes down:            []
# Leaderless partitions: []
# Under-replicated partitions: []
```

---

## Python Client

### confluent-kafka (Drop-in Kafka Replacement)

`confluent-kafka` works against Redpanda with **zero code changes**. Only the `bootstrap.servers` address changes.

```python
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
import json, logging, time

logger = logging.getLogger(__name__)

REDPANDA_BROKERS = "localhost:19092"

# ---- Admin: Create topic programmatically ----
admin = AdminClient({"bootstrap.servers": REDPANDA_BROKERS})
topics = [
    NewTopic(
        "events",
        num_partitions=12,
        replication_factor=1,           # 1 for local dev; 3 for production
        config={
            "retention.ms": "604800000",
            "compression.type": "snappy",
            "min.insync.replicas": "1",
        },
    )
]
fs = admin.create_topics(topics)
for topic, f in fs.items():
    try:
        f.result()
        logger.info("Topic %s created", topic)
    except Exception as e:
        logger.error("Failed to create topic %s: %s", topic, e)

# ---- Producer ----
producer = Producer({
    "bootstrap.servers": REDPANDA_BROKERS,
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "snappy",
    "linger.ms": 5,
    "batch.size": 65536,
})

for i in range(1000):
    event = {"id": i, "ts": int(time.time() * 1000), "value": i * 2.5}
    producer.produce(
        "events",
        key=str(i).encode(),
        value=json.dumps(event).encode(),
        on_delivery=lambda err, msg: logger.error(err) if err else None,
    )
    producer.poll(0)

producer.flush()

# ---- Consumer ----
consumer = Consumer({
    "bootstrap.servers": REDPANDA_BROKERS,
    "group.id": "events-processor-v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "partition.assignment.strategy": "cooperative-sticky",
    "fetch.max.wait.ms": 100,
})
consumer.subscribe(["events"])

try:
    while True:
        msgs = consumer.consume(num_messages=500, timeout=1.0)
        for msg in msgs:
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue
            event = json.loads(msg.value())
            logger.debug("event id=%s", event["id"])
        if msgs:
            consumer.commit(asynchronous=False)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
```

### aiokafka — Async Producer and Consumer

```python
import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

logger = logging.getLogger(__name__)

REDPANDA_BROKERS = "localhost:19092"

# ---- Async Producer ----
async def produce_events(events: list[dict]) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=REDPANDA_BROKERS,
        acks="all",
        enable_idempotence=True,
        compression_type="snappy",
        linger_ms=5,
        max_batch_size=65536,
    )
    await producer.start()
    try:
        tasks = [
            producer.send(
                "events",
                key=str(e["id"]).encode(),
                value=json.dumps(e).encode(),
            )
            for e in events
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                logger.error("Produce error: %s", r)
        await producer.flush()
    finally:
        await producer.stop()

# ---- Async Consumer ----
async def consume_events(topic: str, group_id: str) -> None:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=REDPANDA_BROKERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        fetch_max_wait_ms=100,
        max_poll_records=500,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            logger.info("Processing event id=%s", event["id"])
            await consumer.commit()
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()

# ---- Main ----
async def main():
    events = [{"id": i, "value": i * 1.5} for i in range(100)]
    await produce_events(events)
    await asyncio.wait_for(consume_events("events", "events-async-v1"), timeout=30)

asyncio.run(main())
```

---

## Migration from Apache Kafka

### Compatibility Checklist

| Feature | Redpanda Support | Notes |
|---|---|---|
| Kafka Protocol (produce/fetch/metadata) | Full (API v0–v13+) | Drop-in replacement for clients |
| Consumer Groups / Offsets | Full | `__consumer_offsets` topic internal |
| Transactions / EOS | Full | `transactional.id`, `isolation.level` |
| Admin API (create/delete topics, ACLs) | Full | via Kafka Admin or `rpk` |
| Kafka Streams | Full | Connects via standard bootstrap.servers |
| ksqlDB | Partial | Check version matrix; most operations work |
| MirrorMaker 2 | Full | Use MM2 to migrate data from Kafka |
| Kafka Connect | Full | Run standard Connect workers against Redpanda |
| Confluent Schema Registry clients | Full | Points to Redpanda's built-in SR |
| JMX metrics | Not available | Use Prometheus endpoint instead |
| Log4j / JVM metrics | Not applicable | Single C++ binary |
| ZooKeeper-dependent tools | Not applicable | No ZooKeeper in Redpanda |

### Unsupported / Incompatible Features

- **KIP-500 (KRaft) internal protocol** — Redpanda uses its own Raft; do not attempt to federate Raft nodes between Redpanda and KRaft Kafka.
- **Confluent Replicator** — use MirrorMaker 2 instead.
- **Some Confluent Platform enterprise features** (RBAC, Audit Logs in Confluent format) require Redpanda Enterprise.
- **`kafka-log-dirs.sh`** — use `rpk cluster storage disk` or Admin API.

### Migration Steps with MirrorMaker 2

```properties
# mm2.properties — replicate from Kafka to Redpanda
clusters = source, target

source.bootstrap.servers = kafka-broker:9092
target.bootstrap.servers = redpanda-0:9092,redpanda-1:9092,redpanda-2:9092

source->target.enabled = true
source->target.topics = orders,customers,payments

# Offset translation — enables consumer groups to resume on Redpanda
source->target.sync.group.offsets.enabled = true
source->target.group.filter.regex = .*

# Replication factor for Redpanda target
target.config.storage.replication.factor = 3
target.offset.storage.replication.factor = 3
target.status.storage.replication.factor = 3

# Topic config sync
replication.factor = 3
offset-syncs.topic.replication.factor = 3
heartbeats.topic.replication.factor = 3
checkpoints.topic.replication.factor = 3
```

```bash
# Run MirrorMaker 2 connecting Kafka → Redpanda
connect-mirror-maker.sh mm2.properties

# Verify consumer group offsets were synced
rpk group describe orders-service-v1

# Cutover: point clients to Redpanda brokers and restart
# Client change: bootstrap.servers=redpanda-0:9092,redpanda-1:9092,redpanda-2:9092
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Running `--mode dev-container` in production | Disables disk/CPU/OS tuning checks; data integrity risk | Remove flag; run `rpk redpanda tune all` before starting |
| `acks=0` or `acks=1` for durable data | Data loss on broker failure | `acks=all` + `enable.idempotence=True` |
| `enable.auto.commit=True` in consumers | Commits before processing completes → data loss on crash | Disable; commit manually after processing |
| Not setting `min.insync.replicas` | `acks=all` satisfied by 1 replica when followers are lagging | Set `min.insync.replicas=2` for RF=3 |
| Keeping `auto_create_topics_enabled=true` in production | Typos create orphan topics silently | Disable; create topics explicitly with `rpk` |
| Using JMX-only monitoring tools | Redpanda has no JVM/JMX | Use Prometheus metrics at `:9644/public_metrics` |
| Over-provisioning `--smp` (more cores than physical cores) | Seastar context-switches degrade performance | Set `--smp` = physical CPU cores assigned to container |
| Small `segment.bytes` with tiered storage | Too many small S3 objects → S3 request cost + slow uploads | Use 256 MB – 1 GB segments for tiered topics |
| `retention.ms=-1` without tiered storage enabled | Disk fills up permanently | Only use infinite retention with `redpanda.remote.write=true` |
| Sharing Redpanda broker ports between internal and external listeners with same address | Routing failures; clients resolve to wrong address | Always set separate `--advertise-kafka-addr` for internal and external |
| Using `--memory` larger than container memory limit | OOM kill | Set `--memory` to ~75% of container `resources.limits.memory` |
| No DLQ for Connect sinks | Errors silently drop records | Set `errors.tolerance=all` + `errors.deadletterqueue.topic.name` |
| Enabling `unclean.leader.election.enable=true` | Data loss when out-of-sync replica becomes leader | Keep `false` (Redpanda default) |

---

## References to Consult When Needed

- [Redpanda Documentation](https://docs.redpanda.com/)
- [rpk CLI Reference](https://docs.redpanda.com/current/reference/rpk/)
- [Redpanda Helm Chart Values Reference](https://docs.redpanda.com/current/deploy/deployment-option/self-hosted/kubernetes/kubernetes-deploy/)
- [Redpanda Operator (CRD)](https://docs.redpanda.com/current/deploy/deployment-option/self-hosted/kubernetes/k-operator/)
- [Shadow Indexing / Tiered Storage](https://docs.redpanda.com/current/manage/tiered-storage/)
- [Redpanda Schema Registry API](https://docs.redpanda.com/current/develop/http-proxy/)
- [Redpanda Monitoring Reference](https://docs.redpanda.com/current/manage/monitoring/)
- [Redpanda Security: SASL, mTLS, ACLs](https://docs.redpanda.com/current/manage/security/)
- [MirrorMaker 2 with Redpanda](https://docs.redpanda.com/current/migrate/kafka-migrate/)
- [confluent-kafka Python client](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [aiokafka documentation](https://aiokafka.readthedocs.io/)
