---
name: infra-kafka-cost-optimizer
description: Kafka cost optimization — tiered storage (S3/GCS remote log offload, local.retention.ms vs retention.ms), topic retention right-sizing (per-topic audit SQL), log compaction economics, partition count right-sizing (over-partitioned cluster detection), consumer group idle cleanup, broker instance right-sizing (disk vs compute), Redpanda vs Confluent Cloud cost comparison, MirrorMaker2 cross-region cost, compression savings calculator
---

# Kafka Cost Optimizer

## When to Use

- Reducing Kafka broker disk costs by offloading cold segments to object storage
- Auditing topic retention and eliminating over-retained data
- Right-sizing broker instances for actual throughput workloads
- Detecting and cleaning up abandoned topics and consumer groups
- Comparing self-managed vs managed Kafka (MSK/Confluent Cloud) economics

---

## Tiered Storage (Cold Data to S3/GCS)

### Apache Kafka / Strimzi Native Tiered Storage (Kafka 3.6+)

```properties
# Enable tiered storage on broker
remote.log.storage.system.enable=true
remote.log.manager.thread.pool.size=4

# S3-backed remote log (kafka-s3-tiered-storage plugin)
remote.log.storage.manager.class.name=org.apache.kafka.server.log.remote.storage.S3RemoteStorageManager
remote.log.storage.manager.impl.prefix=s3.rlsm.

s3.rlsm.bucket.name=my-kafka-tiered-storage
s3.rlsm.region=us-east-1
s3.rlsm.path.prefix=kafka/

# Keep only 1 day on disk, 30 days total (29 days in S3)
# Per-topic override: set local.retention.ms at topic level
remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager
```

```bash
# Create topic with tiered storage: 1 day local, 30 days in S3
kafka-topics.sh --bootstrap-server kafka:9092 \
  --create \
  --topic orders \
  --config remote.storage.enable=true \
  --config local.retention.ms=86400000 \    # 1 day on broker disk
  --config retention.ms=2592000000           # 30 days total (29 in S3)

# Alter existing topic to enable tiered storage
kafka-configs.sh --bootstrap-server kafka:9092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config 'remote.storage.enable=true,local.retention.ms=86400000'
```

### Strimzi with Tiered Storage

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: production
  namespace: kafka
spec:
  kafka:
    config:
      remote.log.storage.system.enable: "true"
      # local: 6h, total: 7d → 95% of data offloaded to S3
      # configure per-topic via KafkaTopic CRD
```

---

## Topic Retention Audit

```python
# topic_retention_audit.py — find over-retained, unused, and large topics
import subprocess
import json
from datetime import datetime, timedelta

def audit_topics(bootstrap_server: str) -> list[dict]:
    """Analyze all topics for cost optimization opportunities."""

    # Get topic list
    topics_raw = subprocess.run(
        ["kafka-topics.sh", "--bootstrap-server", bootstrap_server,
         "--list", "--exclude-internal"],
        capture_output=True, text=True
    ).stdout.strip().split("\n")

    results = []
    for topic in topics_raw:
        if not topic:
            continue

        # Get topic config
        config_raw = subprocess.run(
            ["kafka-configs.sh", "--bootstrap-server", bootstrap_server,
             "--entity-type", "topics", "--entity-name", topic, "--describe"],
            capture_output=True, text=True
        ).stdout

        # Parse retention
        retention_ms = extract_config(config_raw, "retention.ms", default=604800000)
        retention_bytes = extract_config(config_raw, "retention.bytes", default=-1)

        # Get partition info
        desc_raw = subprocess.run(
            ["kafka-topics.sh", "--bootstrap-server", bootstrap_server,
             "--describe", "--topic", topic],
            capture_output=True, text=True
        ).stdout
        partition_count = desc_raw.count("Partition:")

        # Get log size from JMX or estimate
        # Using kafka-log-dirs.sh for disk usage
        log_dirs_raw = subprocess.run(
            ["kafka-log-dirs.sh", "--bootstrap-server", bootstrap_server,
             "--topic-list", topic, "--describe"],
            capture_output=True, text=True
        ).stdout

        results.append({
            "topic": topic,
            "partitions": partition_count,
            "retention_days": round(retention_ms / 86400000, 1),
            "retention_bytes_GB": round(retention_bytes / 1e9, 2) if retention_bytes > 0 else -1,
        })

    return sorted(results, key=lambda x: x["retention_days"], reverse=True)


def extract_config(text: str, key: str, default):
    import re
    match = re.search(rf"{key}=(\d+)", text)
    return int(match.group(1)) if match else default
```

```bash
# Quick CLI check: topics with retention > 30 days
kafka-topics.sh --bootstrap-server kafka:9092 --list | while read topic; do
  retention=$(kafka-configs.sh --bootstrap-server kafka:9092 \
    --entity-type topics --entity-name "$topic" --describe 2>/dev/null \
    | grep retention.ms | awk -F'=' '{print $2}' | awk '{print $1}')
  days=$(( ${retention:-604800000} / 86400000 ))
  if [ "$days" -gt 30 ]; then
    echo "OVER-RETAINED: $topic retention=${days}d"
  fi
done
```

---

## Compression Savings Calculator

```python
def estimate_compression_savings(
    uncompressed_mb_per_sec: float,
    compression_type: str = "lz4",
) -> dict:
    """Estimate storage and network savings from compression."""
    ratios = {
        "none":   1.0,
        "gzip":   0.25,   # ~75% reduction for text/JSON
        "snappy": 0.45,   # ~55% reduction
        "lz4":    0.35,   # ~65% reduction, fastest
        "zstd":   0.28,   # ~72% reduction, best ratio/speed tradeoff
    }
    ratio = ratios[compression_type]
    compressed_mb_per_sec = uncompressed_mb_per_sec * ratio

    monthly_uncompressed_TB = uncompressed_mb_per_sec * 3600 * 24 * 30 / 1e6
    monthly_compressed_TB = monthly_uncompressed_TB * ratio

    # EBS gp3: ~$0.08/GB/month, S3: ~$0.023/GB/month
    ebs_savings_usd = (monthly_uncompressed_TB - monthly_compressed_TB) * 1000 * 0.08
    s3_savings_usd  = (monthly_uncompressed_TB - monthly_compressed_TB) * 1000 * 0.023

    return {
        "compression_type": compression_type,
        "original_MB_per_sec": uncompressed_mb_per_sec,
        "compressed_MB_per_sec": round(compressed_mb_per_sec, 1),
        "monthly_disk_savings_TB": round(monthly_uncompressed_TB - monthly_compressed_TB, 2),
        "ebs_savings_usd_per_month": round(ebs_savings_usd, 0),
        "s3_tiered_savings_usd_per_month": round(s3_savings_usd, 0),
    }

# Example: 200 MB/s uncompressed JSON data
result = estimate_compression_savings(200, "lz4")
# → ~65% reduction, ~130 MB/s savings, significant monthly storage cost reduction
```

---

## Over-Partitioned Cluster Detection

```bash
# Count total partitions per broker (high partition count = high memory/CPU overhead)
kafka-topics.sh --bootstrap-server kafka:9092 --describe | \
  grep "Leader:" | awk '{print $6}' | sort | uniq -c | sort -rn

# Topics with partitions >> actual consumer parallelism
kafka-consumer-groups.sh --bootstrap-server kafka:9092 --all-groups --describe \
  | awk 'NR>1 {print $1, $2}' \
  | sort -u \
  | while read group topic; do
      consumers=$(kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
        --group "$group" --describe 2>/dev/null | grep -c "STABLE")
      partitions=$(kafka-topics.sh --bootstrap-server kafka:9092 \
        --describe --topic "$topic" 2>/dev/null | grep -c "Partition:")
      if [ "$partitions" -gt "$((consumers * 3))" ]; then
        echo "OVER-PARTITIONED: topic=$topic partitions=$partitions consumers=$consumers"
      fi
    done
```

```bash
# Reduce partition count: requires recreating topic (breaking change)
# Step 1: Mirror traffic to new topic with fewer partitions via MirrorMaker2
# Step 2: Cut over consumers after lag is zero
# Step 3: Delete old topic after retention period

# Non-breaking: reduce replication factor for non-critical topics
kafka-topics.sh --bootstrap-server kafka:9092 \
  --alter \
  --topic dev-staging-events \
  --replication-factor 2   # was 3
# Then run kafka-reassign-partitions.sh to rebalance replicas
```

---

## Idle Consumer Group Cleanup

```bash
#!/bin/bash
# cleanup_idle_consumer_groups.sh
# Delete consumer groups with zero members and no recent activity

BOOTSTRAP="kafka:9092"
DRY_RUN=${1:-true}

kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --list | while read group; do
  state=$(kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP \
    --group "$group" --describe 2>/dev/null \
    | awk 'NR==2 {print $5}')    # STATE column

  if [ "$state" = "Empty" ] || [ -z "$state" ]; then
    echo "IDLE group: $group (state=${state:-UNKNOWN})"
    if [ "$DRY_RUN" = "false" ]; then
      kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP \
        --group "$group" --delete
      echo "  → deleted"
    fi
  fi
done
```

---

## Managed vs Self-Managed Cost Model

```python
COST_MODEL = {
    # Self-managed on AWS (3 broker cluster)
    "self_managed": {
        "broker_instance": "r6i.2xlarge",   # 8 vCPU, 64 GB RAM
        "broker_count": 3,
        "instance_usd_hr": 0.504,
        "ebs_gp3_1tb_usd_month": 80,
        "disk_tb_per_broker": 2,
        "ops_hours_per_month": 20,         # upgrade, monitoring, incidents
        "ops_rate_usd_hr": 150,
    },
    # MSK Serverless
    "msk_serverless": {
        "per_partition_hr_usd": 0.0015,
        "per_gb_stored_usd": 0.10,
        "per_gb_in_usd": 0.10,
        "per_gb_out_usd": 0.05,
    },
    # Confluent Cloud (Basic cluster)
    "confluent_cloud": {
        "per_cku_hr_usd": 0.44,       # 1 CKU = 250 MB/s throughput
        "per_gb_stored_usd": 0.00015,  # tiered storage
        "min_ckus": 1,
    },
}

def monthly_self_managed_cost(model: dict) -> float:
    instance = model["broker_count"] * model["instance_usd_hr"] * 730
    disk = model["broker_count"] * model["disk_tb_per_broker"] * model["ebs_gp3_1tb_usd_month"]
    ops = model["ops_hours_per_month"] * model["ops_rate_usd_hr"]
    return instance + disk + ops
```

---

## Quick Wins Checklist

```
Retention Optimization:
[ ] Audit topics with retention > 7 days — reduce if no consumer needs it
[ ] Enable tiered storage for topics with retention > 1 day (offload 80%+ to S3)
[ ] Switch time-series topics from delete to compact+delete for state topics

Compression:
[ ] Enable lz4 compression on all high-throughput producers (often 50-70% disk reduction)
[ ] Verify compression.type=producer on brokers (don't recompress)

Partitioning:
[ ] Identify topics with partitions > 10x consumer parallelism — schedule reduction
[ ] Consolidate low-volume topics into fewer partitions

Cleanup:
[ ] Delete Empty/Idle consumer groups (reduce broker metadata load)
[ ] Delete dev/test topics not used in >30 days
[ ] Remove topics with 0 bytes/sec for >2 weeks

Right-sizing:
[ ] Profile actual broker CPU/disk I/O — most clusters are disk-bound not CPU-bound
[ ] Consider io-optimized instances (i3.2xlarge) vs memory-optimized for high-throughput
```

---

## Anti-Patterns

1. **Default 7-day retention for all topics** — event log topics need 7 days, but state changelog topics only need enough to cover consumer downtime (hours); audit per-topic.
2. **Tiered storage without monitoring remote fetch latency** — consumers reading cold data from S3 incur higher latency; monitor `FetchFromFollower` vs `FetchFromRemote` rates.
3. **Reducing replication factor to save disk** — going from RF=3 to RF=1 saves 66% disk but loses all redundancy; use tiered storage instead.
4. **Partition inflation for throughput** — each partition has memory overhead (~1 MB); 10,000 partitions × 64 brokers = 640 MB just for metadata.
5. **Ignoring cross-AZ data transfer** — Kafka replication across AZs incurs ~$0.01/GB transfer; co-locate producers and consumers in the same AZ when possible.

---

## References

- Kafka tiered storage: `kafka.apache.org/documentation/#tiered_storage`
- Confluent tiered storage: `docs.confluent.io/platform/current/kafka/tiered-storage.html`
- AWS MSK pricing: `aws.amazon.com/msk/pricing/`
- Related skills: `[[infra-kafka-platform-review]]`, `[[infra-streaming-reliability-review]]`, `[[de-cost-optimization]]`, `[[terraform-data]]`
