---
name: trino-memory-and-spill-tuning
description: Trino memory management and spill-to-disk tuning — query.max-memory/query.max-total-memory/query.max-memory-per-node properties, memory.heap-headroom-per-node, JVM heap sizing (80% of RAM), spill-enabled/spiller-spill-path/spill-compression-codec configuration, exchange buffer tuning, OOM kill diagnosis, memory pool sizing, fault-tolerant execution exchange manager (S3/filesystem), session-level memory overrides, memory-intensive operator patterns (HashJoin/Sort/Window/GroupBy)
---

# Trino Memory and Spill Tuning

## When to Use

- Queries are being killed with "Query exceeded memory limit" errors
- Workers are crashing with OutOfMemoryError
- Large joins, sorts, or GROUP BY operations fail mid-execution
- Tuning a new cluster for large analytical workloads
- Deciding whether to enable spill-to-disk

---

## Memory Architecture

```
Per Worker JVM Heap (-Xmx)
├── memory.heap-headroom-per-node  (default: 30% of heap)
│   └── Reserved for JVM internals, off-heap allocations
└── Trino managed memory (70% of heap)
    ├── query.max-memory-per-node  (default: 30% of heap)
    │   └── User memory: hash tables, sort buffers, GROUP BY state
    └── System memory
        └── Exchange buffers, task bookkeeping
```

**Constraint**: `query.max-memory-per-node + memory.heap-headroom-per-node < JVM -Xmx`

---

## Memory Configuration Properties

```properties
# etc/config.properties

# Cluster-wide memory limit per query (user memory only)
query.max-memory=50GB

# Cluster-wide limit including revocable memory (set to 2x query.max-memory)
query.max-total-memory=100GB

# Per-node user memory limit per query
query.max-memory-per-node=8GB

# JVM heap reserved for non-Trino allocations
memory.heap-headroom-per-node=4GB
```

**Sizing guide** for a 64GB worker node:

```
JVM -Xmx = 54GB (85% of 64GB)

memory.heap-headroom-per-node = 8GB (15% of Xmx)
query.max-memory-per-node      = 24GB (45% of Xmx)

# Constraint check: 8 + 24 = 32GB < 54GB ✓
# Cluster with 10 workers:
# query.max-memory = 10 × 24GB × 0.5 = 120GB (50% utilization cap)
# query.max-total-memory = 240GB
```

---

## JVM Configuration for Memory

```
# etc/jvm.config — production 64GB node
-server
-Xmx54G
-XX:InitialRAMPercentage=80
-XX:MaxRAMPercentage=80
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError          # crash fast on OOM (avoids zombie state)
-XX:+HeapDumpOnOutOfMemoryError      # capture heap dump for diagnosis
-XX:HeapDumpPath=/var/trino/data/
-XX:ReservedCodeCacheSize=512M
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
```

---

## Spill-to-Disk Configuration

Spill allows memory-intensive operations to overflow to local disk instead of failing with OOM.

**Supported operations**: HashJoin, ORDER BY / Sort, aggregation, window functions.

```properties
# etc/config.properties

# Enable spill
spill-enabled=true

# Spill location(s) — use fast SSD, not system disk
spiller-spill-path=/data/trino-spill,/data2/trino-spill

# Don't spill if disk is > 90% full
spiller-max-used-space-threshold=0.85

# Compress spilled data (reduces I/O, adds CPU)
spill-compression-codec=LZ4       # or ZSTD for better ratio

# Encrypt spilled data (for compliance)
spill-encryption-enabled=false    # set true if data is sensitive

# Per-node spill budget
max-spill-per-node=100GB
query-max-spill-per-node=20GB     # per-query spill limit on a single node
```

**Session property overrides:**

```sql
-- Enable spill for this session
SET SESSION spill_enabled = true;

-- Allow larger spill budget
SET SESSION query_max_spill_per_node = '50GB';
```

---

## Exchange Buffer Tuning

Exchanges (network shuffles) buffer data between stages. Large exchanges can cause memory pressure.

```properties
# etc/config.properties

# Output buffer per task (increase for high-fan-out queries)
sink.max-buffer-size=32MB

# Exchange client buffer (increase for high-throughput queries)
exchange.client-threads=25
exchange.concurrent-request-multiplier=3

# Compress exchange data (reduces network by ~50%, adds CPU)
# Set via session: SET SESSION exchange_compression_codec = 'LZ4';
```

```sql
-- Per-query exchange compression
SET SESSION exchange_compression_codec = 'LZ4';    -- fast, good ratio
SET SESSION exchange_compression_codec = 'ZSTD';   -- better ratio, more CPU
```

---

## Fault-Tolerant Execution (TASK retry mode)

For large batch queries that fail partway through, enable task-level retries with an exchange manager.

```properties
# etc/config.properties
retry-policy=TASK
task-retry-attempts-per-task=4
```

```properties
# etc/exchange-manager.properties — S3 backend
exchange-manager.name=filesystem
exchange.base-directories=s3://my-bucket/trino-exchange/
exchange.s3.region=us-east-1
exchange.s3.aws-access-key=${ENV:AWS_ACCESS_KEY}
exchange.s3.aws-secret-key=${ENV:AWS_SECRET_KEY}
```

```properties
# etc/exchange-manager.properties — local filesystem (dev/test)
exchange-manager.name=filesystem
exchange.base-directories=/tmp/trino-exchange
```

**When to use FTE**:
- Batch ETL jobs running > 30 minutes on large data
- Worker nodes on spot/preemptible instances
- Workloads where full query retry is too expensive

---

## OOM Diagnosis Workflow

```bash
# 1. Check if queries were killed due to OOM
grep "Query exceeded memory limit" /var/trino/data/var/log/server.log | tail -20

# 2. Find which query consumed the most memory (via REST)
curl -s http://coordinator:8080/v1/query \
  | jq '.[] | {queryId, state, memoryPool, totalMemoryReservation}' \
  | sort -t'"' -k8 -rn | head -10

# 3. Get detailed memory usage for a specific query
curl -s http://coordinator:8080/v1/query/<query_id> \
  | jq '.queryStats | {
      peakTotalMemoryReservation,
      peakUserMemoryReservation,
      spilledBytes
    }'

# 4. Check heap usage via JMX
curl -s http://worker:8080/v1/jmx/mbean/java.lang:type=Memory \
  | jq '.attributes[] | select(.name == "HeapMemoryUsage")'
```

---

## Memory-Intensive Operators and Fixes

| Operator | Memory Use | Fix |
|----------|-----------|-----|
| `HashJoin` (build side) | Entire build table fits in memory | Reduce build side; enable spill; use broadcast only for small tables |
| `OrderBy` / `Sort` | All rows buffered for global sort | Add `LIMIT`; use approximate sort; enable spill |
| `Aggregation` (high cardinality) | Hash table with all distinct keys | Partition before aggregating; enable spill |
| `Window function` | Rows within a partition buffered | Partition on fewer rows; avoid `ROWS BETWEEN UNBOUNDED` |
| `DISTINCT` | Full distinct set per partition | Replace `COUNT(DISTINCT)` with approx_distinct() when exact not needed |

```sql
-- Replace exact COUNT(DISTINCT) with approximate (100x less memory)
-- BAD: holds all customer IDs in hash set
SELECT COUNT(DISTINCT customer_id) FROM iceberg.silver.orders;

-- GOOD: HyperLogLog approximation, <1% error
SELECT approx_distinct(customer_id) FROM iceberg.silver.orders;

-- Replace ORDER BY without LIMIT
-- BAD: sorts all rows globally
SELECT * FROM iceberg.silver.orders ORDER BY amount DESC;

-- GOOD: add LIMIT
SELECT * FROM iceberg.silver.orders ORDER BY amount DESC LIMIT 1000;
```

---

## Session-Level Memory Overrides

```sql
-- Increase memory limit for a specific heavy query
SET SESSION query_max_memory = '100GB';
SET SESSION query_max_total_memory = '200GB';

-- Force spill for memory-intensive join
SET SESSION spill_enabled = true;
SET SESSION join_spill_enabled = true;

-- Adjust broadcast limit (default 100MB, increase for larger dims)
SET SESSION join_max_broadcast_table_size = '300MB';
```

---

## Memory Sizing Quick Reference

| Cluster Size | Node RAM | -Xmx | query.max-memory-per-node | query.max-memory |
|-------------|---------|-------|--------------------------|-----------------|
| Dev (1 node) | 16GB | 12G | 4GB | 4GB |
| Small (5 workers) | 32GB | 26G | 8GB | 40GB |
| Medium (10 workers) | 64GB | 54G | 20GB | 200GB |
| Large (20 workers) | 128GB | 108G | 45GB | 900GB |

---

## Anti-Patterns

1. **Setting `query.max-memory-per-node` + `memory.heap-headroom-per-node` > JVM -Xmx** — causes native OOM crashes outside Trino's memory management; the constraint must hold.
2. **Using system disk for spill path** — Trino spills GB of data; using the OS root disk fills `/` and crashes the node; always use a dedicated fast disk.
3. **Enabling spill without fast local SSDs** — spilling to spinning disk or network-attached storage is slower than failing the query; spill only works when disk is faster than re-doing computation.
4. **Not setting `-XX:+ExitOnOutOfMemoryError`** — without this flag, the JVM continues running in a broken heap state after OOM; set it so the process crashes cleanly and restarts.
5. **Large `query.max-memory` without proportional `query.max-total-memory`** — revocable memory (spill, exchange) is counted toward total but not user memory; if total = user limit, revocable operations fail.

---

## References

- Memory properties: `trino.io/docs/current/admin/properties-resource-management.html`
- Spill properties: `trino.io/docs/current/admin/properties-spilling.html`
- Fault-tolerant execution: `trino.io/docs/current/admin/fault-tolerant-execution.html`
- Related skills: `[[trino-admin-cluster-health]]`, `[[trino-resource-group-governance]]`, `[[trino-query-optimization]]`
