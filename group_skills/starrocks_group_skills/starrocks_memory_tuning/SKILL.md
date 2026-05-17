---
name: starrocks-memory-tuning
description: StarRocks memory tuning — BE memory architecture (mem_limit, query pool, load pool, compaction pool, metadata cache), query memory limits (query_mem_limit session var), memory spill to disk (spill_mode), OOM analysis (be.out log), mem_tracker hierarchy, FE heap tuning (JAVA_OPTS), Primary Key table persistent index memory, jemalloc tuning
---

# StarRocks Memory Tuning

## When to Use

Load this skill when the user needs to:
- Diagnose **query OOM errors** (`MemoryLimitExceeded` in BE logs or query failure with code 1001)
- Investigate a **BE crash** caused by out-of-memory (OOM kill from the Linux kernel or internal abort)
- Reduce **memory pressure** that is causing slow queries, stalled loads, or compaction backlog
- **Right-size BE memory** parameters for a new cluster or when adding workloads
- Configure **memory spill to disk** to allow large aggregations/sorts that exceed DRAM
- Tune **FE JVM heap** when FE suffers frequent GC pauses or OutOfMemoryError
- Optimize **Primary Key table** memory footprint via persistent index
- Build Prometheus **alerts** for memory utilization on BE nodes

---

## BE Memory Architecture

### High-Level Layout

```
┌─────────────────────────────────────────────────────────────────────┐
│  BE Process Address Space                                           │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  mem_limit  (default = 90% of system RAM)                    │  │
│  │                                                              │  │
│  │  ┌──────────────────┐  ┌──────────────────────────────────┐ │  │
│  │  │  Query Pool      │  │  Load / Import Pool              │ │  │
│  │  │  (query_mem_limit│  │  (load_process_max_memory_limit) │ │  │
│  │  │   per query;     │  │                                  │ │  │
│  │  │   resource group │  │  ┌──────────────────────────┐   │ │  │
│  │  │   mem_limit frac)│  │  │  write_buffer_size        │   │ │  │
│  │  └──────────────────┘  │  │  (per-tablet buffer)      │   │ │  │
│  │                        │  └──────────────────────────┘   │ │  │
│  │  ┌──────────────────┐  └──────────────────────────────────┘ │  │
│  │  │  Compaction Pool │  ┌──────────────────────────────────┐ │  │
│  │  │  (compaction_mem │  │  Page Cache (data blocks)        │ │  │
│  │  │   _limit_percent)│  │  (storage_page_cache_limit)      │ │  │
│  │  └──────────────────┘  └──────────────────────────────────┘ │  │
│  │                                                              │  │
│  │  ┌──────────────────────────────────────────────────────┐   │  │
│  │  │  Tablet Metadata Cache + Column Pool                 │   │  │
│  │  │  + PK Index Cache (primary_key_index_cache_capacity) │   │  │
│  │  └──────────────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Memory Pools Summary

| Pool | Config Parameter | Default | Purpose |
|---|---|---|---|
| Total cap | `mem_limit` | `90%` of system RAM | Hard ceiling for the entire BE process |
| Query pool | `query_mem_limit` (session) / resource group `mem_limit` | `0` (no per-query limit) | Analytical query execution |
| Load pool | `load_process_max_memory_limit_bytes` | `107374182400` (100 GB) | All concurrent import jobs |
| Compaction pool | `compaction_mem_limit_percent` | `10` (10% of mem_limit) | Background tablet compaction |
| Page cache | `storage_page_cache_limit` | `20%` of mem_limit | Decoded data block cache (hot data) |
| PK index cache | `primary_key_index_cache_capacity` | `10%` of mem_limit | In-memory portion of PK persistent index |
| Column pool | internal, not directly tunable | — | Reusable column chunk buffers |
| Tablet metadata | internal | — | Tablet rowset metadata (grows with table count) |

### `mem_limit` — Total BE Memory Cap

Set in `be.conf`:

```properties
# Absolute bytes
mem_limit=137438953472      # 128 GB

# Or percentage of total system RAM (recommended)
mem_limit=80%               # leave headroom for OS, jemalloc overhead, kernel buffers
```

**Guidelines:**
- Default `90%` is safe for dedicated BE hosts. On shared hosts lower to `70–80%`.
- Setting `mem_limit` too high (>90%) risks OOM kills from the Linux kernel (`dmesg | grep oom`).
- The value is read at BE startup. After changing `be.conf`, restart the BE process.
- With jemalloc, actual RSS can temporarily exceed `mem_limit` by ~5–10% due to arena fragmentation; factor this in on memory-constrained hosts.

### mem_tracker Hierarchy

StarRocks uses a hierarchical `mem_tracker` tree to account every byte:

```
MemTracker root (process total)
├── query_pool          — all running queries combined
│   ├── query#q1        — one tracker per query
│   │   ├── HashJoin
│   │   ├── Aggregation
│   │   └── Sort
│   └── query#q2
├── load                — all active stream/broker/insert loads
│   ├── load_channel#txn1
│   └── load_channel#txn2
├── compaction          — background compaction tasks
├── page_cache          — storage page cache
├── pk_index_cache      — primary key index hot tier
├── tablet_metadata     — rowset metadata overhead
└── column_pool         — reusable column buffers
```

The mem_tracker tree is visible in real time via the BE HTTP API (see OOM Analysis section).

---

## Query Memory Limits

### `query_mem_limit` Session Variable

Controls maximum memory a single query may consume on each BE. When the limit is exceeded the query is cancelled with `MemoryLimitExceeded`.

```sql
-- Set for the current session (8 GB)
SET query_mem_limit = 8589934592;

-- Set at the global level (affects all new sessions)
SET GLOBAL query_mem_limit = 8589934592;

-- Disable per-query limit (rely on resource group or BE mem_limit alone)
SET query_mem_limit = 0;

-- Check current value
SHOW VARIABLES LIKE 'query_mem_limit';
```

**Legacy alias:** `exec_mem_limit` is an older name for the same variable, accepted for backward compatibility.

```sql
-- Both are equivalent
SET exec_mem_limit = 8589934592;
SET query_mem_limit = 8589934592;
```

**Recommended defaults by query type:**

| Workload | `query_mem_limit` |
|---|---|
| Interactive dashboards (aggregations, < 100M rows) | `4294967296` (4 GB) |
| Medium analytical queries | `8589934592` (8 GB) |
| Heavy ETL / full table scans | `17179869184` (16 GB) or spill enabled |
| Ad-hoc / no limit enforced | `0` + resource group cap |

### Resource Group `mem_limit`

Resource groups provide workload isolation. The `mem_limit` parameter in a resource group is a **fraction of the total BE `mem_limit`**, not an absolute byte count.

```sql
-- Create a resource group for BI dashboards capped at 40% of BE memory
CREATE RESOURCE GROUP bi_group
  TO (user = 'bi_user')
  WITH (
    cpu_core_limit    = 8,
    mem_limit         = 0.4,      -- 40% of BE mem_limit
    concurrency_limit = 20,
    type              = 'normal'
  );

-- Create a resource group for heavy ETL capped at 60% of BE memory
CREATE RESOURCE GROUP etl_group
  TO (user = 'etl_user')
  WITH (
    cpu_core_limit    = 16,
    mem_limit         = 0.6,
    concurrency_limit = 5,
    type              = 'normal'
  );

-- Show existing resource groups
SHOW RESOURCE GROUPS ALL;

-- Alter memory limit
ALTER RESOURCE GROUP bi_group WITH (mem_limit = 0.35);
```

**Note:** Resource group `mem_limit` fractions across groups can sum to more than 1.0 — they act as soft quotas unless the BE process is under pressure. Only one group can be active per query.

---

## Memory Spill to Disk

When a query exceeds available memory, StarRocks can spill intermediate operator state to the local filesystem instead of cancelling the query. Spill adds 3–10x latency but prevents OOM failures for large joins, aggregations, and sorts.

### Enabling Spill

```sql
-- Auto spill: spill when memory pressure is detected (recommended)
SET spill_mode = 'auto';

-- Force spill: always spill to disk regardless of memory pressure (for testing)
SET spill_mode = 'force';

-- Disable spill (default)
SET spill_mode = 'off';

-- Apply globally
SET GLOBAL spill_mode = 'auto';
```

### Spill-Capable Operators

| Operator | Spills when | Notes |
|---|---|---|
| **HashJoin** | Build-side hash table exceeds threshold | Only equi-joins; broadcast joins do not spill |
| **Aggregation** | Hash aggregate table exceeds threshold | Partial aggregation spills before final merge |
| **Sort / TopN** | Sort buffer exceeds threshold | External merge sort |

### Spill Tuning Parameters

```properties
# be.conf — set these on each BE node

# Size of in-memory hash/sort table before spilling (bytes, default 10 MB)
spill_mem_table_size = 10485760

# Maximum total spill disk usage per query (0 = unlimited)
# Prevents runaway spills from filling disks
spill_max_disk_bytes_per_query = 107374182400   # 100 GB

# Directory for spill files; defaults to storage_root_path subdirectory
# By default: <storage_root_path>/spill
# Override to a fast NVMe path:
# spill_local_storage_dir = /nvme0/starrocks/spill
```

**Spill directory location:**
- Default: `<storage_root_path>/spill` (same disk as tablets)
- Best practice: point `spill_local_storage_dir` to a separate fast NVMe device to avoid I/O contention with tablet reads/writes.

### Spill Performance Impact

```
Without spill (query OOM killed):  0 ms  (failure)
With spill (auto mode):            query_time × 3–10× depending on spill volume
```

- For queries spilling more than 50 GB, expect 5–10x slowdown.
- Monitor spill activity via `information_schema.query_detail` (column `spill_bytes`).

```sql
-- Find queries that spilled in the last hour
SELECT
    query_id,
    user,
    db,
    LEFT(sql, 80)        AS sql_preview,
    query_duration_ms,
    spill_bytes,
    mem_cost_bytes
FROM information_schema.query_detail
WHERE start_time > NOW() - INTERVAL 1 HOUR
  AND spill_bytes > 0
ORDER BY spill_bytes DESC
LIMIT 20;
```

---

## Page Cache Tuning

The page cache stores decoded data blocks (columnar pages) in BE memory for hot data reuse across queries.

```properties
# be.conf

# Absolute bytes (takes precedence over percentage)
storage_page_cache_limit = 17179869184    # 16 GB

# Or percentage of mem_limit (default = 20%)
storage_page_cache_limit = 20%

# Completely disable page cache for pure ETL / write-heavy workloads
disable_storage_page_cache = true         # default false

# Cache scope: DATA (data pages only), INDEX (index pages only), ALL (both)
# Default = ALL
storage_page_cache_type = ALL
```

**When to increase page cache:**
- High repeat-query rate on the same fact tables (dashboards, BI tools).
- `starrocks_be_page_cache_hit_rate` Prometheus metric is below 80%.

**When to reduce or disable page cache:**
- Pure bulk-load / ETL workload with no repeat reads.
- Query pool memory is being starved — OOM errors on queries while page cache is large.
- Cold data accessed infrequently (full scans on historical partitions).

**Page cache sizing formula (rule of thumb):**

```
page_cache = min(hot_working_set_GB, mem_limit × 0.30)
```

For a 256 GB BE with a 50 GB hot working set:
```
page_cache = min(50, 256×0.9×0.30) ≈ min(50, 69) = 50 GB
```

```properties
storage_page_cache_limit = 53687091200    # 50 GB
```

---

## Primary Key Persistent Index Memory

### Without Persistent Index (default for PK tables created before SR 3.0)

The entire Primary Key index is kept in BE memory. Memory cost scales as:

```
memory_per_tablet ≈ number_of_rows × avg_key_bytes
```

For a 1-billion-row PK table with an 8-byte integer key:
```
1,000,000,000 × 8 bytes = 8 GB per replica
```
With 3 replicas: **24 GB** just for PK indexes across the cluster.

### With Persistent Index (StarRocks 3.x, recommended)

```sql
-- Enable when creating a Primary Key table
CREATE TABLE orders (
    order_id     BIGINT NOT NULL,
    customer_id  INT,
    order_date   DATE,
    amount       DECIMAL(18,2)
)
ENGINE = OLAP
PRIMARY KEY (order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 64
PROPERTIES (
    "replication_num"      = "3",
    "enable_persistent_index" = "true"    -- store index on disk, cache hot tier in RAM
);

-- Enable on existing PK table
ALTER TABLE orders SET ("enable_persistent_index" = "true");
```

With persistent index, only the **hot tier** of the L0/L1/L2 index structure is cached in RAM:

```properties
# be.conf

# Memory budget for PK index hot-tier cache across all PK tables (bytes)
# Default: 10% of mem_limit
primary_key_index_cache_capacity = 10737418240    # 10 GB
```

**Index structure with persistent index:**

```
L0 (in-memory write buffer)      → flushed to disk periodically
L1 (immutable on-disk index)     → cached pages in primary_key_index_cache
L2 (compacted on-disk index)     → mostly cold, read from disk on lookup miss
```

**Memory savings:** For large PK tables, persistent index reduces in-memory index footprint by 80–95% at the cost of occasional disk I/O for cold key lookups during upserts.

**Tuning guidance:**

```properties
# For clusters with many large PK tables, increase cache capacity
primary_key_index_cache_capacity = 21474836480    # 20 GB

# Reduce if PK tables are small and memory is needed elsewhere
primary_key_index_cache_capacity = 5368709120     # 5 GB
```

---

## OOM Analysis

### Step 1: Check `be.out` Log

BE stderr is written to `<be_log_dir>/be.out`. Search for OOM indicators:

```bash
# On the affected BE host
grep -E "MemoryLimitExceeded|Process memory not enough|OOM|bad_alloc" \
    /opt/starrocks/be/log/be.out | tail -50

# Example output patterns:
# [W] MemoryLimitExceeded: query_id=3c2a... mem_tracker=query_pool
#     consumed=8192MB limit=8192MB peak=8200MB
#
# [E] Process memory not enough, cancel query_id=abc123
#     process_mem=119GB mem_limit=120GB
#
# terminate called after throwing an instance of 'std::bad_alloc'
```

Key fields in OOM messages:

| Field | Meaning |
|---|---|
| `consumed` | Memory actually used at time of cancellation |
| `limit` | The limit that was breached (query_mem_limit or mem_limit) |
| `peak` | Maximum watermark during query lifetime |
| `mem_tracker` | Which pool was exceeded (query_pool, load, compaction, etc.) |

### Step 2: Real-Time mem_tracker Dump via HTTP API

```bash
# Full mem_tracker tree (all trackers, hierarchical)
curl -s http://<be_host>:8040/api/mem_tracker | python3 -m json.tool

# Filter for top memory consumers
curl -s http://<be_host>:8040/api/mem_tracker \
  | python3 -c "
import json, sys
data = json.load(sys.stdin)
# Sort by current consumption descending
trackers = data.get('mem_tracker', [])
sorted_t = sorted(trackers, key=lambda x: x.get('current_consumption', 0), reverse=True)
for t in sorted_t[:20]:
    mb = t['current_consumption'] // 1048576
    print(f\"{mb:8d} MB  {t['label']}\")
"
```

Example output:
```
   45312 MB  root
   38900 MB  query_pool
   18234 MB  query#3c2a1f...
    8192 MB  query#7b9d0c...
    4096 MB  page_cache
    2048 MB  load
     512 MB  pk_index_cache
     128 MB  compaction
```

### Step 3: Query `information_schema.be_metrics`

```sql
-- Current memory metrics across all BEs
SELECT
    be_host,
    name,
    value
FROM information_schema.be_metrics
WHERE name IN (
    'mem_limit',
    'process_mem_bytes',
    'query_mem_bytes',
    'load_mem_bytes',
    'compaction_mem_bytes',
    'page_cache_mem_bytes',
    'pk_index_cache_mem_bytes',
    'column_pool_mem_bytes'
)
ORDER BY be_host, name;
```

```sql
-- Memory utilization percentage per BE
SELECT
    be_host,
    MAX(CASE WHEN name = 'process_mem_bytes'  THEN value END)                AS used_bytes,
    MAX(CASE WHEN name = 'mem_limit'           THEN value END)                AS limit_bytes,
    ROUND(
        MAX(CASE WHEN name = 'process_mem_bytes' THEN value END) * 100.0 /
        NULLIF(MAX(CASE WHEN name = 'mem_limit'  THEN value END), 0), 2
    )                                                                          AS utilization_pct
FROM information_schema.be_metrics
WHERE name IN ('process_mem_bytes', 'mem_limit')
GROUP BY be_host
ORDER BY utilization_pct DESC;
```

### Step 4: Check Linux OOM Killer

```bash
# Check if BE was killed by the Linux OOM killer
dmesg -T | grep -i "oom\|killed process" | grep -i starrocks

# Or check syslog
grep -i "out of memory\|oom-killer" /var/log/syslog | tail -20
```

If the Linux OOM killer fires before StarRocks internal limits, lower `mem_limit` to give more headroom.

### Step 5: SHOW PROC for Memory State

```sql
-- Connect to FE and inspect BE memory from FE's perspective
SHOW PROC '/backends'\G
-- Look at: MemUsedPct, MemAvailCapacity columns
```

---

## FE Memory Tuning

The FE is a Java process. Memory issues manifest as `OutOfMemoryError` in `fe.log`, or slow query planning due to GC pressure.

### JVM Heap Configuration

Edit `fe/conf/fe.conf`:

```properties
# Heap size — set Xms = Xmx to avoid resizing pauses
# Recommended: 8–32 GB depending on cluster size and metadata volume
JAVA_OPTS="-Xmx16g -Xms16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 \
           -XX:+PrintGCDetails -XX:+PrintGCDateStamps \
           -Xloggc:${STARROCKS_HOME}/log/fe.gc.log \
           -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 \
           -XX:GCLogFileSize=50m"
```

**Sizing guidelines:**

| Cluster scale | Recommended FE heap |
|---|---|
| Small (< 50 tables, < 10 BEs) | 8 GB (`-Xmx8g`) |
| Medium (< 1000 tables, 10–50 BEs) | 16 GB (`-Xmx16g`) |
| Large (> 1000 tables, 50+ BEs, many partitions) | 32 GB (`-Xmx32g`) |
| Very large (millions of tablets, micro-partitions) | 48–64 GB (`-Xmx48g`) |

### GC Tuning

```properties
# G1GC is recommended for FE in SR 3.x (replaces CMS)
JAVA_OPTS="-Xmx16g -Xms16g \
           -XX:+UseG1GC \
           -XX:MaxGCPauseMillis=200 \
           -XX:InitiatingHeapOccupancyPercent=45 \
           -XX:G1HeapRegionSize=16m \
           -XX:ConcGCThreads=4 \
           -XX:ParallelGCThreads=8"
```

### Analyzing FE GC Logs

```bash
# Check GC log location (from fe.conf JAVA_OPTS -Xloggc path)
tail -100 /opt/starrocks/fe/log/fe.gc.log

# Quick GC summary using gceasy / jstat if available
jstat -gcutil $(pgrep -f StarRocksFE) 1000 20

# Example jstat output columns:
# S0C S1C S0U S1U   EC    EU    OC     OU     MC     MU   CCSC CCSU  YGC YGCT FGC FGCT  GCT
# High FGC (Full GC count) or FGCT (Full GC time) → heap too small or leak
```

**Warning signs:**
- Full GC occurring more than once per hour → increase heap.
- GC pause > 500ms → tune G1GC or increase heap.
- `java.lang.OutOfMemoryError: Java heap space` in `fe.log` → critical, increase `-Xmx`.

### FE Memory Consumers

| Consumer | Notes |
|---|---|
| Query planning | Parse tree, logical plan, physical plan (released after planning) |
| Metadata cache | Table/partition/tablet descriptors cached in FE heap |
| Audit log buffer | Accumulated before flushing to `fe.audit.log` |
| BDB-JE edit log cache | Replication log buffering; grows with write throughput |
| Statistics cache | Column NDV, histograms cached for CBO optimizer |

---

## Load Memory Tuning

Import jobs (stream load, broker load, routine load, INSERT INTO SELECT) consume BE memory via the load pool.

```properties
# be.conf

# Total memory budget for ALL active import transactions on this BE (bytes)
# Default: 107374182400 (100 GB) — effectively uncapped on most hosts
load_process_max_memory_limit_bytes = 21474836480    # 20 GB (tighter cap)

# Memory per tablet write buffer for each active load channel (bytes)
# Each concurrent load ×  number of tablets × this value
# Default: 104857600 (100 MB)
write_buffer_size = 104857600

# Maximum single stream load request body size (MB)
# Default: 10240 (10 GB)
streaming_load_max_mb = 10240
```

**Load memory estimation:**

```
load_memory = concurrent_loads × tablets_per_table × write_buffer_size
```

Example: 5 concurrent loads, each writing to a table with 64 tablets:
```
5 × 64 × 100 MB = 32,000 MB = ~31 GB
```

If this exceeds `load_process_max_memory_limit_bytes`, loads will queue or fail. Either:
1. Increase `load_process_max_memory_limit_bytes`
2. Reduce `write_buffer_size` (trades memory for more frequent mini-compactions)
3. Reduce concurrent load jobs

**Routine load memory:**
Routine load (Kafka consumer) has an additional cap:

```properties
# Maximum memory per Routine Load task (bytes, default 1 GB)
max_routine_load_task_concurrent_num = 5
routine_load_thread_pool_size        = 10
```

---

## jemalloc Tuning

StarRocks BE uses jemalloc as its allocator. Misconfigured jemalloc can cause high RSS without actual memory pressure (fragmentation).

```properties
# be.conf

# Number of jemalloc arenas (default: 4× CPU cores, can be high on large hosts)
# Reducing arenas decreases per-arena fragmentation overhead
# Recommended: 4–8 arenas for most deployments
JEMALLOC_CONF="narenas:4,tcache:false,lg_tcache_max:20"

# Alternatively set via environment before starting BE:
# export MALLOC_CONF="narenas:4,background_thread:true,dirty_decay_ms:5000"
```

**Common jemalloc settings:**

| Setting | Value | Effect |
|---|---|---|
| `narenas` | 4–8 | Fewer arenas → less fragmentation, slightly lower concurrency |
| `background_thread` | `true` | Enables background thread for memory decay (returns memory to OS faster) |
| `dirty_decay_ms` | `5000` | Time (ms) before dirty pages are returned to OS (default: 10000) |
| `muzzy_decay_ms` | `10000` | Time (ms) before muzzy pages are returned to OS |
| `tcache` | `false` | Disable thread cache (reduces per-thread fragmentation at cost of allocation speed) |

**Checking jemalloc stats:**

```bash
# Dump jemalloc stats via BE HTTP API (requires MALLOC_CONF=stats_print:true or explicit call)
curl -s http://<be_host>:8040/api/jeprof/heap > heap.prof

# Check retained memory vs allocated
curl -s http://<be_host>:8040/api/mem_tracker \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(json.dumps(d.get('jemalloc',{}), indent=2))"
```

---

## Memory Monitoring

### Prometheus Metrics

Key BE memory metrics exposed at `http://<be_host>:8040/metrics`:

| Metric | Description |
|---|---|
| `starrocks_be_process_mem_bytes` | Total BE process RSS |
| `starrocks_be_query_mem_bytes` | Memory used by all running queries |
| `starrocks_be_load_mem_bytes` | Memory used by active load jobs |
| `starrocks_be_compaction_mem_bytes` | Memory used by compaction tasks |
| `starrocks_be_page_cache_mem_bytes` | Storage page cache size |
| `starrocks_be_pk_index_cache_mem_bytes` | Primary Key index cache size |
| `starrocks_be_column_pool_mem_bytes` | Column pool (reusable buffers) |
| `starrocks_be_mem_limit` | Configured `mem_limit` value |

### Prometheus Alert Rules

```yaml
# prometheus/alerts/starrocks_memory.yml
groups:
  - name: starrocks_memory
    rules:
      # BE memory utilization high
      - alert: StarRocksBeMemoryHigh
        expr: |
          (starrocks_be_process_mem_bytes / starrocks_be_mem_limit) > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "StarRocks BE memory high on {{ $labels.host }}"
          description: >
            BE {{ $labels.host }} memory utilization is
            {{ $value | humanizePercentage }} (threshold: 85%).

      # BE memory critical — risk of OOM
      - alert: StarRocksBeMemoryCritical
        expr: |
          (starrocks_be_process_mem_bytes / starrocks_be_mem_limit) > 0.95
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "StarRocks BE memory critical on {{ $labels.host }}"
          description: >
            BE {{ $labels.host }} memory utilization is
            {{ $value | humanizePercentage }}. Imminent OOM risk.

      # Page cache consuming disproportionate memory
      - alert: StarRocksPageCacheOversized
        expr: |
          (starrocks_be_page_cache_mem_bytes / starrocks_be_mem_limit) > 0.40
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "StarRocks page cache oversized on {{ $labels.host }}"
          description: >
            Page cache on {{ $labels.host }} is consuming
            {{ $value | humanizePercentage }} of mem_limit.
            Consider reducing storage_page_cache_limit.

      # Query pool OOM rate
      - alert: StarRocksQueryOomRate
        expr: |
          rate(starrocks_be_query_mem_limit_exceeded_total[5m]) > 0.1
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "StarRocks query OOM errors on {{ $labels.host }}"
          description: >
            More than 0.1 OOM cancellations/sec on {{ $labels.host }}.
            Check query_mem_limit settings and spill configuration.
```

### Grafana Dashboard Panels (PromQL)

```promql
# Panel: BE Memory Utilization %
(starrocks_be_process_mem_bytes / starrocks_be_mem_limit) * 100

# Panel: Memory breakdown by pool (stacked area)
starrocks_be_query_mem_bytes
starrocks_be_load_mem_bytes
starrocks_be_compaction_mem_bytes
starrocks_be_page_cache_mem_bytes
starrocks_be_pk_index_cache_mem_bytes

# Panel: Free memory (headroom)
starrocks_be_mem_limit - starrocks_be_process_mem_bytes

# Panel: Query spill bytes per second
rate(starrocks_be_spill_bytes_total[1m])
```

### SQL-Based Monitoring

```sql
-- Memory summary per BE from information_schema
SELECT
    be_host,
    ROUND(
        MAX(CASE WHEN name = 'process_mem_bytes'       THEN value / 1073741824.0 END), 2
    ) AS process_gb,
    ROUND(
        MAX(CASE WHEN name = 'mem_limit'               THEN value / 1073741824.0 END), 2
    ) AS limit_gb,
    ROUND(
        MAX(CASE WHEN name = 'query_mem_bytes'         THEN value / 1073741824.0 END), 2
    ) AS query_gb,
    ROUND(
        MAX(CASE WHEN name = 'load_mem_bytes'          THEN value / 1073741824.0 END), 2
    ) AS load_gb,
    ROUND(
        MAX(CASE WHEN name = 'page_cache_mem_bytes'    THEN value / 1073741824.0 END), 2
    ) AS page_cache_gb,
    ROUND(
        MAX(CASE WHEN name = 'compaction_mem_bytes'    THEN value / 1073741824.0 END), 2
    ) AS compaction_gb
FROM information_schema.be_metrics
WHERE name IN (
    'process_mem_bytes', 'mem_limit', 'query_mem_bytes',
    'load_mem_bytes', 'page_cache_mem_bytes', 'compaction_mem_bytes'
)
GROUP BY be_host
ORDER BY process_gb DESC;
```

---

## Memory Tuning Playbook

### Scenario 1: Query OOM — `MemoryLimitExceeded`

```
Symptom: Query fails with "MemoryLimitExceeded" or error code 1001
```

1. Check `be.out` for the breached limit (`query_mem_limit` vs process `mem_limit`).
2. If per-query limit: raise `query_mem_limit` for the session or enable spill.
3. If process limit: profile query memory — use `EXPLAIN ANALYZE` to find the largest operator.
4. Enable `spill_mode = 'auto'` for the workload.
5. If spill insufficient: add more BE nodes or upgrade BE RAM.

```sql
-- Quick fix: raise limit for heavy query session
SET query_mem_limit = 17179869184;   -- 16 GB
SET spill_mode = 'auto';

-- Then run the query
SELECT ...;
```

### Scenario 2: BE Crash (Linux OOM Kill)

```
Symptom: BE process disappears; dmesg shows "oom-killer" killed starrocks_be
```

1. `dmesg -T | grep -i oom` — confirm OOM kill.
2. Lower `mem_limit` in `be.conf` to give OS 15–20% headroom.
3. Check if page cache is consuming excess memory — reduce `storage_page_cache_limit`.
4. Enable `background_thread:true` in jemalloc to improve memory return to OS.
5. Review concurrent load jobs — high `concurrent_loads × write_buffer_size` may spike RSS.

### Scenario 3: Slow Queries Due to Memory Pressure

```
Symptom: Queries are slow; no OOM errors but high memory utilization visible in metrics
```

1. Check `starrocks_be_process_mem_bytes / starrocks_be_mem_limit` — above 85%?
2. Identify which pool dominates via `be_metrics` query (Section: SQL-Based Monitoring).
3. If page cache dominant: reduce `storage_page_cache_limit`.
4. If query pool dominant: enforce resource groups with `mem_limit` fractions.
5. If load dominant: reduce concurrent loads or lower `write_buffer_size`.

### Scenario 4: PK Table Using Excessive Memory

```
Symptom: After loading large Primary Key table, BE memory jumps by tens of GB
```

1. Check if `enable_persistent_index = false` on the table.
2. Alter the table: `ALTER TABLE t SET ("enable_persistent_index" = "true");`
3. Wait for compaction to rebuild the persistent index on disk.
4. Tune `primary_key_index_cache_capacity` to control hot-tier footprint.

---

## Anti-Patterns

### No Per-Query `query_mem_limit`

```sql
-- WRONG: relies on process mem_limit alone — one bad query can OOM the BE
SET query_mem_limit = 0;

-- RIGHT: set a reasonable per-query cap, enable spill for overflow
SET query_mem_limit = 8589934592;   -- 8 GB
SET spill_mode = 'auto';
```

### Page Cache Too Large — Starves Query Pool

```properties
# WRONG: 60% of mem_limit for page cache leaves only 40% for queries and loads
storage_page_cache_limit = 60%

# RIGHT: cap page cache to leave adequate query pool headroom
# Rule: page_cache + expected_peak_query_mem + load_mem < 90% of mem_limit
storage_page_cache_limit = 20%   # or tune based on hot working set measurement
```

### Missing Spill Configuration on Heavy ETL

```sql
-- WRONG: run large hash join/aggregation with default spill=off and insufficient query_mem_limit
-- Result: query cancelled with MemoryLimitExceeded

-- RIGHT for ETL sessions
SET query_mem_limit  = 32212254720;   -- 30 GB
SET spill_mode       = 'auto';
SET spill_mem_table_size = 52428800;  -- 50 MB tables before spill
```

### Spill Directory on Same Disk as Tablets

```properties
# WRONG: spill on the same disk as tablet storage — causes I/O contention
# (default behavior; acceptable only if no fast alternative)

# RIGHT: dedicate a fast NVMe path for spill
spill_local_storage_dir = /nvme1/starrocks_spill
```

### PK Index Fully In-Memory for Large Tables

```sql
-- WRONG: large PK table without persistent index — each replica holds full index in RAM
CREATE TABLE events (
    event_id BIGINT NOT NULL,
    ...
)
ENGINE = OLAP
PRIMARY KEY (event_id)
...
PROPERTIES ("enable_persistent_index" = "false");   -- wastes RAM

-- RIGHT
PROPERTIES ("enable_persistent_index" = "true");
```

### FE Heap Too Small — Query Planning Pauses

```properties
# WRONG: default or low FE heap on a large cluster
JAVA_OPTS="-Xmx4g -Xms4g"   # insufficient for clusters with 1000+ tables

# RIGHT: size according to cluster metadata volume
JAVA_OPTS="-Xmx16g -Xms16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
```

### Mismatched `mem_limit` and OS Overcommit

```bash
# WRONG: setting mem_limit=95% on a host with other processes (Kafka, monitoring agent, etc.)
# Risk: combined RSS exceeds physical RAM → kernel OOM kills StarRocks BE

# RIGHT: account for all non-StarRocks processes
# OS + other services overhead ≈ 10–20 GB on typical hosts
# mem_limit = total_RAM - 20_GB - jemalloc_overhead(~5%)
# On 256 GB host: mem_limit = 256 - 20 = 236 GB → ~92%; use 85% to be safe
mem_limit = 85%
```

---

## Output Expectations

When applying this skill, produce:

1. **Diagnosis** — identify which memory pool caused the issue from log/metric evidence.
2. **be.conf changes** — exact parameter names, values, and comments explaining the rationale.
3. **Session variable commands** — `SET` statements ready to run in a MySQL client.
4. **Resource group DDL** — if workload isolation is needed.
5. **Prometheus alert YAML** — if monitoring setup is requested.
6. **Runbook steps** — ordered checklist for the specific failure scenario.

Always include:
- The unit of memory values (bytes vs percentage vs GB suffix).
- Whether a parameter change requires BE/FE restart.
- The tradeoff accepted when enabling spill (latency vs availability).

---

## References to Consult When Needed

- StarRocks 3.x documentation: Memory Management — `https://docs.starrocks.io/docs/administration/management/resource_management/Memory_management/`
- StarRocks 3.x documentation: Resource Groups — `https://docs.starrocks.io/docs/administration/management/resource_management/resource_group/`
- StarRocks 3.x documentation: Query Spill — `https://docs.starrocks.io/docs/administration/management/resource_management/spill_to_disk/`
- StarRocks 3.x documentation: Primary Key table — `https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/`
- StarRocks GitHub: `be/src/runtime/mem_tracker.h` — mem_tracker hierarchy implementation
