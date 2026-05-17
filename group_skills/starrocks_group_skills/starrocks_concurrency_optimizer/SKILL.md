---
name: starrocks-concurrency-optimizer
description: StarRocks high-concurrency optimization — pipeline_dop tuning for BI workloads, resource groups for workload isolation (short_query type), query queue configuration, FE connection pool, tablet scan parallelism, point query optimization (Primary Key table direct lookup), query cache for repeated dashboard queries, BE thread pool tuning
---

# StarRocks — High-Concurrency Optimization

## When to Use

Load this skill when the user needs to:
- Handle BI tools (Superset, Tableau, Grafana, Metabase) generating 100+ simultaneous queries
- Diagnose p99 latency spikes that appear only under concurrent load but not in isolation
- Resolve CPU saturation on BE nodes caused by excessive parallelism for small queries
- Eliminate connection pool exhaustion (too many clients connecting to a single FE node)
- Isolate interactive dashboard queries from long-running ETL or ad-hoc analytical queries
- Configure query queue to prevent thundering-herd overload during peak dashboard refresh
- Optimize repeated identical dashboard queries using the query result cache
- Tune Primary Key table lookups for key-value access patterns (sub-millisecond p99)
- Size BE thread pools for a mixed workload (short queries + heavy aggregations)

---

## Concurrency Architecture

### Request Lifecycle

```
BI Tool (JDBC/MySQL protocol)
        │
        ▼
  FE Leader / Follower  ──── MySQL protocol port 9030
        │  parse SQL → logical plan → CBO optimize → physical plan
        │  fragment assignment: each fragment → target BE(s)
        ▼
  Fragment Scheduler (FE)
        │  RPC: PlanFragmentService → BE
        ▼
  BE Pipeline Engine
        │  PipelineDriver per fragment instance
        │  Operators: Scan → Exchange → HashJoin → Aggregation → ...
        ▼
  BE Thread Pools
        ├─ pipeline_exec_thread_pool   — executes PipelineDrivers
        ├─ scan_thread_pool            — tablet I/O + decompression
        └─ brpc_num_threads            — inter-BE / FE-BE RPC
```

### Parallelism Model

Each query fragment spawns `pipeline_dop` **PipelineDriver instances** per BE.  
For a query touching 3 BEs with `pipeline_dop=4`: `3 × 4 = 12` parallel execution streams compete for `pipeline_exec_thread_pool` slots.

At 200 concurrent small queries with default `pipeline_dop=8` on a 16-core BE:  
`200 × 8 = 1600` driver instances competing for 16 execution threads → context-switch explosion, p99 collapses.

**The core concurrency rule:** `concurrent_queries × pipeline_dop` must stay well below `pipeline_exec_thread_pool_thread_num`.

---

## 1. Pipeline DOP (Degree of Parallelism)

### Default Behavior

When `pipeline_dop = 0` (the default), StarRocks auto-sets DOP per query:

```
pipeline_dop_auto = BE CPU cores / 4
```

On a 32-core BE: auto DOP = 8. This is appropriate for a low-concurrency analytical cluster but is destructive for high-concurrency BI.

### System-Wide DOP Reduction

Set at the FE level — applies to all new sessions:

```sql
-- View current value
SHOW VARIABLES LIKE 'pipeline_dop';

-- Set globally (persists across FE restarts via fe.conf for older versions;
--  in 3.x it is a session/global variable)
SET GLOBAL pipeline_dop = 2;
```

**Sizing guidance by concurrency tier:**

| Peak concurrent queries | Recommended pipeline_dop | Notes |
|---|---|---|
| < 50 | 0 (auto) or 4–8 | Let auto-DOP work |
| 50–150 | 4 | Reduce CPU contention |
| 150–300 | 2 | Small queries; extra parallelism yields no benefit |
| 300+ | 1–2 | Point lookups; pipeline overhead dominates |

### Per-Query DOP Override (Hint)

For specific slow queries that need more parallelism despite a low global setting:

```sql
-- Force DOP=8 for a single heavy aggregation
SELECT /*+ SET_VAR(pipeline_dop=8) */
    region,
    SUM(revenue) AS total_revenue
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY region;
```

This is the correct way to give ETL or ad-hoc queries full parallelism without raising the global DOP that would hurt concurrent BI.

### Per-Session DOP (BI Tool Sessions)

Set at connection time in the JDBC connection string or via an `init_sql`:

```
jdbc:mysql://fe-host:9030/mydb?sessionVariables=pipeline_dop=2
```

Or inside a session:

```sql
SET pipeline_dop = 2;
-- run all dashboard queries
```

### DOP and Resource Groups

Resource groups have a `cpu_core_limit` property that caps the effective DOP for queries assigned to that group — even if `pipeline_dop` is set higher. See Section 2.

---

## 2. Resource Groups for Workload Isolation

Resource groups partition BE CPU and memory across query classes. For concurrency management the most important type is `short_query`.

### Resource Group Types

| Type | Purpose | Key Behavior |
|---|---|---|
| `normal` (default) | General queries | Shares resources with other normal groups |
| `short_query` | Many small BI queries | Runs with hard concurrency limit; CPU preempts `normal` group queries if needed |
| `mv` | Materialized view refresh | Reserved slots for MV background tasks |

### Creating a 3-Tier Resource Group Setup

**Tier 1: Dashboard Group (short_query) — BI tool connection pool**

```sql
CREATE RESOURCE GROUP rg_dashboard
    TO
        (user='bi_user'),           -- all queries from the BI service account
        (db='analytics')            -- or by database
    WITH (
        'type'              = 'short_query',
        'cpu_core_limit'    = 4,    -- max CPU cores on each BE for this group
        'mem_limit'         = '20%',-- fraction of BE process memory
        'concurrency_limit' = 100,  -- reject queries beyond this count
        'big_query_cpu_second_limit' = 10,  -- downgrade query if CPU > 10s
        'big_query_scan_rows_limit' = 10000000,  -- downgrade if scans > 10M rows
        'big_query_mem_limit'       = '4294967296'  -- 4 GB per query hard cap
    );
```

**Tier 2: Ad-hoc Analyst Group**

```sql
CREATE RESOURCE GROUP rg_adhoc
    TO
        (role='analyst')
    WITH (
        'type'              = 'normal',
        'cpu_core_limit'    = 12,
        'mem_limit'         = '40%',
        'concurrency_limit' = 20,
        'big_query_cpu_second_limit' = 300,
        'big_query_scan_rows_limit'  = 1000000000,
        'big_query_mem_limit'        = '21474836480'  -- 20 GB
    );
```

**Tier 3: ETL / Load Group**

```sql
CREATE RESOURCE GROUP rg_etl
    TO
        (user='etl_user')
    WITH (
        'type'              = 'normal',
        'cpu_core_limit'    = 16,
        'mem_limit'         = '35%',
        'concurrency_limit' = 10,
        'big_query_cpu_second_limit' = 3600,
        'big_query_scan_rows_limit'  = 5000000000,
        'big_query_mem_limit'        = '107374182400'  -- 100 GB
    );
```

### Resource Group Classifier Priority

When a query matches multiple resource groups, the **most specific classifier wins** (highest `weight`). Explicit classifiers beat database-level ones, which beat role-level ones.

```sql
-- Assign a specific user/IP combination higher priority
ALTER RESOURCE GROUP rg_dashboard
    ADD CLASSIFIER
        (user='bi_user', source_ip='10.10.0.0/16');
```

### View Resource Group Assignments

```sql
SHOW RESOURCE GROUPS ALL;

-- Which group did my last query land in?
SELECT resource_group
FROM information_schema.be_workgroup_metrics
LIMIT 10;
```

### Effective DOP Within a Resource Group

A query assigned to `rg_dashboard` with `cpu_core_limit=4` on a 32-core BE will have its effective DOP capped at `min(pipeline_dop, cpu_core_limit)`. With `pipeline_dop=2` and `cpu_core_limit=4`, effective DOP = 2. With `pipeline_dop=8` and `cpu_core_limit=4`, effective DOP = 4.

---

## 3. Query Queue

Without a query queue, concurrent query bursts cause all queries to degrade simultaneously. The queue serializes excess queries while the cluster handles the current batch, preserving latency for queries already executing.

### Enable Query Queue (FE Configuration)

Set in `fe.conf` or via `ADMIN SET FRONTEND CONFIG`:

```sql
-- Enable queue for SELECT queries
ADMIN SET FRONTEND CONFIG ("enable_query_queue_select" = "true");

-- Enable queue for load jobs (INSERT INTO SELECT, Broker Load, etc.)
ADMIN SET FRONTEND CONFIG ("enable_query_queue_load" = "true");

-- Enable queue for statistics collection
ADMIN SET FRONTEND CONFIG ("enable_query_queue_statistic" = "true");
```

### Queue Sizing Parameters

```sql
-- Max queries waiting in queue (across all resource groups)
ADMIN SET FRONTEND CONFIG ("query_queue_max_queued_queries" = "1000");

-- Reject a queued query if it waits longer than this (seconds)
ADMIN SET FRONTEND CONFIG ("query_queue_pending_timeout_second" = "300");

-- Global concurrency threshold that triggers queuing:
-- when total running queries exceed this, new ones are queued
ADMIN SET FRONTEND CONFIG ("query_queue_concurrency_limit" = "200");
-- Note: resource group concurrency_limit is checked first;
-- this is the cluster-wide backstop.
```

### Resource-Group-Level Queue Triggers

A query enters the queue when the resource group's `max_cpu_cores` is exceeded:

```sql
ALTER RESOURCE GROUP rg_dashboard
    WITH ('max_cpu_cores' = '16');
-- queries assigned to rg_dashboard queue when the group would exceed 16 CPU-cores in flight
```

### Monitoring the Queue

```sql
-- See currently queued / running queries
SHOW PROCESSLIST;
-- State = "Pending" → query is in the queue
-- State = "Running" → executing on BEs

-- Query queue statistics via FE metrics endpoint
-- GET http://fe-host:8030/metrics  (look for query_queue_*)

-- Information schema view (StarRocks 3.1+)
SELECT
    query_id,
    state,
    resource_group,
    pending_time_ms,
    start_time
FROM information_schema.current_queries
WHERE state = 'PENDING'
ORDER BY pending_time_ms DESC;
```

### Queue Behavior Under Load

```
Client sends query
      │
      ▼
FE checks resource group concurrency_limit
      │ limit reached?
     YES → check query_queue_max_queued_queries
            │ queue not full → query waits (Pending)
            │    │ slot opens → query promoted to Running
            │    └ query_queue_pending_timeout_second exceeded → ERROR 1064
            └ queue full → immediate error: "Query queue is full"
      NO  → query dispatched directly to BEs
```

---

## 4. FE Connection Handling

### FE Connection Limit

Each FE node has a hard cap on simultaneous MySQL-protocol connections:

```properties
# fe.conf
qe_max_connection = 1024   # default 1024; raise for large BI deployments
```

For a deployment with 3 FE nodes at `qe_max_connection=1024`:  
maximum total connections = 3072, but each FE independently enforces its own limit.

### Session Timeout

```properties
# fe.conf — prevent idle connections from consuming slots
wait_timeout = 300          # seconds; default 8 hours (28800) — too long for BI

# Also set at the client side in JDBC:
# jdbc:mysql://fe:9030/db?autoReconnect=true&socketTimeout=30000
```

Set `wait_timeout` to match your BI tool's connection pool idle eviction time (typically 300–600 s).

### HikariCP JDBC Connection Pool (BI Tool Backend)

Correct pool sizing prevents both under-provisioning (high latency) and over-connection (FE saturation):

```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:mysql://fe-lb:9030/analytics");
config.setUsername("bi_user");
config.setPassword("...");

// Pool size: keep well below qe_max_connection / num_app_instances
config.setMaximumPoolSize(50);          // 50 connections per app instance
config.setMinimumIdle(10);              // keep 10 warm connections
config.setConnectionTimeout(5000);     // 5 s — fail fast if cluster overloaded
config.setIdleTimeout(300000);         // 5 min — release before FE wait_timeout
config.setMaxLifetime(1800000);        // 30 min — rotate connections
config.setKeepaliveTime(60000);        // 1 min — keep idle connections alive
config.setConnectionInitSql("SET pipeline_dop = 2");  // per-connection DOP

// Validation
config.setConnectionTestQuery("SELECT 1");
```

### FE Load Balancing

Never point all BI tool connections at a single FE. Use one of:

1. **Hardware/software load balancer (recommended):**
   ```
   # HAProxy example — round-robin across 3 FE nodes
   frontend starrocks_fe
       bind *:9030
       default_backend starrocks_fe_pool

   backend starrocks_fe_pool
       balance roundrobin
       option tcp-check
       server fe1 10.0.0.1:9030 check
       server fe2 10.0.0.2:9030 check
       server fe3 10.0.0.3:9030 check
   ```

2. **DNS round-robin:** Multiple A records for `fe-cluster.internal` pointing to all FE IPs. Simpler but lacks health checking.

3. **JDBC failover:**
   ```
   jdbc:mysql:loadbalance://fe1:9030,fe2:9030,fe3:9030/analytics
   ```

**FE Follower nodes can serve queries** — they do not need to proxy through Leader. In StarRocks 3.x all FE nodes (Leader + Followers) accept query connections.

---

## 5. Tablet Scan Parallelism

Scan parallelism determines how finely an individual tablet's data is divided across scan threads. More tablets = higher inherent parallelism at the scan layer.

### Tablet Count and Concurrency Throughput

```sql
-- Check tablet distribution for a table
SHOW TABLETS FROM analytics.orders;

-- Recommended tablet count formula:
-- tablets = BE_count × cores_per_BE × 3  (for large fact tables)
-- For concurrency: err on MORE tablets — each small query gets its own scan thread

-- Create table with explicit tablet count for high-concurrency BI table:
CREATE TABLE analytics.dim_product (
    product_id   BIGINT        NOT NULL,
    product_name VARCHAR(255),
    category     VARCHAR(100),
    price        DECIMAL(12,2)
)
DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 48   -- many buckets → many tablets
PROPERTIES (
    "replication_num" = "3",
    "compression"     = "LZ4"
);
```

### Scan Key and Range Tuning

```properties
# be.conf — controls how many scan key ranges the BE sends to each tablet in one RPC
max_scan_key_num = 1024          # default 1024; reduce for very high concurrency to limit memory per scan

# Rows fetched per scan range (lower = smaller memory chunks, better for concurrent small scans)
doris_scan_range_row_count = 524288   # default 524288 (512K rows)
# For high-concurrency point-query workloads, a smaller value reduces memory per query:
doris_scan_range_row_count = 65536    # 64K rows per range
```

### Adaptive Scan (StarRocks 3.x)

StarRocks 3.x has adaptive scan scheduling: if a tablet is already being scanned by another query, the scan thread pool scheduler delays the new scan request to avoid I/O contention. This is automatic and requires no configuration.

---

## 6. Point Query Optimization (Primary Key Tables)

For BI lookup patterns that fetch single rows or narrow ranges by primary key, StarRocks Primary Key tables support a dedicated low-latency path that bypasses normal fragment execution entirely.

### Enable Point Query Cache

```sql
-- Session or global variable
SET enable_point_query_detail_cache = true;
SET GLOBAL enable_point_query_detail_cache = true;
```

### How the Direct Lookup Path Works

```
Normal query path:
  FE plan → N fragment instances → scan tablets → merge → return

Point query path (Primary Key + equi-predicate on all PK columns):
  FE detects single-row lookup →
  route directly to tablet leader BE →
  row cache lookup (persistent index in memory) →
  return row, no fragment scheduling overhead
```

Latency profile:
- **Normal scan path:** 10–50 ms for indexed table, dominated by fragment setup overhead
- **Point query path:** 0.5–3 ms, dominated by network RTT

### Query Pattern That Triggers Point Lookup

```sql
-- Triggers direct PK lookup (all PK columns specified with =)
SELECT *
FROM orders
WHERE order_id = 12345678;

-- Also triggers: all PK columns present, additional non-PK filters are post-filter
SELECT order_id, status, amount
FROM orders
WHERE order_id = 12345678
  AND status = 'SHIPPED';   -- post-filter applied after PK lookup
```

```sql
-- Does NOT trigger point lookup: range scan on PK
SELECT * FROM orders WHERE order_id BETWEEN 1000 AND 2000;

-- Does NOT trigger: PK partially specified
SELECT * FROM orders WHERE order_id = 1 AND region_id = 5;
-- (only if both order_id AND region_id are part of the composite PK)
```

### Persistent Index for Point Queries

The persistent index stores PK → row location mapping in BE memory (with overflow to SSD). Required for sub-millisecond point queries:

```sql
CREATE TABLE orders (
    order_id    BIGINT   NOT NULL,
    user_id     BIGINT,
    status      VARCHAR(32),
    amount      DECIMAL(12,2),
    created_at  DATETIME
)
PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 64
PROPERTIES (
    "enable_persistent_index" = "true",   -- keep PK index in memory/SSD
    "persistent_index_type"   = "LOCAL",  -- LOCAL = SSD-backed; CLOUD for shared storage
    "compression"             = "LZ4"
);
```

### Verify Point Query Execution

```sql
EXPLAIN SELECT * FROM orders WHERE order_id = 12345678;
-- Look for: SHORT_CIRCUIT in the plan output — indicates direct lookup path
-- Absence of "HASH JOIN", "EXCHANGE" nodes also indicates point-query path
```

---

## 7. Query Cache

The query cache stores scan results for tablet-level granular data, enabling exact cache hits when the same time partition is scanned repeatedly (common in dashboard refreshes).

### Enable Query Cache

```sql
-- Session level (for BI tool connection init SQL)
SET enable_query_cache = true;

-- Global (all sessions)
SET GLOBAL enable_query_cache = true;
```

### Cache Capacity and Entry Size

```properties
# be.conf — configure before BE startup (or via ADMIN SET CONFIG for dynamic ones)
query_cache_capacity = 536870912    # 512 MB per BE; increase for large dashboard workloads
```

```sql
-- Per-entry size limit (queries producing results larger than this skip the cache)
SET query_cache_entry_max_bytes = 4194304;   -- 4 MB per cache entry (default)
SET query_cache_entry_max_rows  = 409600;    -- 400K rows per cache entry (default)
```

### Cache Invalidation Rules

The query cache is **partition-aware**: a cache entry is keyed on `(query_digest, tablet_id, tablet_version)`. The cache is automatically invalidated when:
- New data is loaded into the tablet (tablet version advances)
- Partition is overwritten
- Table metadata changes

This means fresh streaming data (frequent small loads) will have low cache hit rates. The cache is most effective for:
- Immutable historical partitions (yesterday, last week, last month)
- Dashboard date filters that do not include the current streaming partition

### Verifying Cache Hits

```sql
-- EXPLAIN VERBOSE shows cache nodes in the plan
EXPLAIN VERBOSE
SELECT
    date_trunc('hour', event_ts) AS hour_bucket,
    COUNT(*)                     AS event_count
FROM events
WHERE event_date = '2024-03-15'
GROUP BY 1;
-- Look for: QueryCacheNode in plan — indicates cache is eligible
-- Look for: CachePopulate / CacheProbe operators
```

```sql
-- Runtime cache statistics (StarRocks 3.x)
SELECT
    be_id,
    query_cache_capacity,
    query_cache_usage,
    query_cache_hit_count,
    query_cache_miss_count,
    round(query_cache_hit_count * 100.0 /
          nullif(query_cache_hit_count + query_cache_miss_count, 0), 2) AS hit_rate_pct
FROM information_schema.be_metrics
WHERE metric_name LIKE 'query_cache%';
```

### Forcing Cache Population (Warm-Up)

For dashboards with a cold start problem, pre-warm the cache by running representative queries before users arrive:

```sql
-- Run during off-peak hours to populate cache for common dashboard patterns
SELECT /*+ SET_VAR(enable_query_cache=true) */
    region,
    SUM(revenue),
    COUNT(DISTINCT user_id)
FROM sales
WHERE sale_date >= CURRENT_DATE - INTERVAL 7 DAY
GROUP BY region;
```

---

## 8. BE Thread Pool Tuning

### Execution Thread Pool

```properties
# be.conf
# Number of threads in the pipeline execution pool.
# Default: equal to CPU core count.
# For high concurrency with low DOP, you may set slightly above core count
# to absorb bursts without queueing.
pipeline_exec_thread_pool_thread_num = 0   # 0 = auto (= CPU cores)

# For a 32-core BE handling 300 concurrent small queries at DOP=1:
# 300 queries × 1 DOP = 300 driver instances
# With 32 threads, at most 32 run simultaneously → 268 drivers wait
# This is correct and intended — increase only if scan threads are idle but exec is not
pipeline_exec_thread_pool_thread_num = 32  # keep at core count or slightly above (1.5×)
```

### Scan Thread Pool

```properties
# be.conf
# Tablet scan threads (I/O + decompression)
# Each scan range occupies one scan thread for its duration
# For NVMe SSDs with high IOPS, increase beyond default
num_threads_per_disk = 4           # threads per disk device (HDD: 1, SSD: 4, NVMe: 8)

# Total max scan threads across all disks
max_scanner_thread_num = 48        # set to: num_disks × num_threads_per_disk
```

### BRPC / RPC Thread Pool

```properties
# be.conf
# Inter-BE and FE-BE RPC threads (data exchange between fragments)
brpc_num_threads = -1   # default -1 = auto (= CPU cores); rarely needs tuning
# For very high fan-out queries (many BEs exchanging shuffle data):
brpc_num_threads = 64
```

### Connector Scan Thread Pool (External Tables)

```properties
# be.conf — for queries on external Hive/Iceberg/JDBC/S3 tables
pipeline_connector_scan_thread_num_per_cpu = 8   # default 8
# Increase if external scans dominate workload and CPU is idle:
pipeline_connector_scan_thread_num_per_cpu = 16
```

### Thread Pool Sizing Decision Tree

```
Is p99 high but CPU utilization low?
  YES → scan threads are the bottleneck
        → increase max_scanner_thread_num / num_threads_per_disk
  NO  → CPU is saturated?
    YES → too many parallel exec threads competing
          → reduce pipeline_dop; enforce resource group cpu_core_limit
    NO  → RPC/network is the bottleneck
          → check brpc_num_threads; check network saturation
```

---

## 9. Connection Limit Patterns

### Checking Current Connections

```sql
-- Connections per FE node (run on each FE)
SHOW FRONTENDS\G
-- Look at: QueryPort (9030), current_connections (not directly shown)

-- Show active sessions (all FE nodes)
SHOW PROCESSLIST;

-- Count by user
SELECT user, COUNT(*) AS conn_count
FROM information_schema.processlist
GROUP BY user
ORDER BY conn_count DESC;
```

### BI Tool-Specific Pool Sizing

| BI Tool | Recommended Pool Size per FE | Notes |
|---|---|---|
| Apache Superset | 20–40 | Async executor; set `SQLALCHEMY_POOL_SIZE=20` |
| Grafana | 10–20 per datasource | Each panel is a separate query; keep pool small |
| Tableau Server | 50–100 | Large extract jobs + interactive; separate service accounts |
| Metabase | 15–30 | Connection pool config in `MB_DB_CONNECTION_URI` |
| Redash | 10–20 | Single-threaded queue per datasource |

### Superset StarRocks SQLAlchemy Config

```python
# superset_config.py
SQLALCHEMY_DATABASE_URI = "mysql+pymysql://bi_user:password@fe-lb:9030/analytics"

# Connection pool
SQLALCHEMY_POOL_SIZE    = 20
SQLALCHEMY_MAX_OVERFLOW = 10    # additional burst connections
SQLALCHEMY_POOL_TIMEOUT = 30    # raise error if pool empty after 30s
SQLALCHEMY_POOL_RECYCLE = 300   # recycle connections every 5 min (< FE wait_timeout)

# Per-query execution options passed via connect_args
SQLALCHEMY_CUSTOM_CONNECT_ARGS = {
    "connect_timeout": 10,
    "init_command": "SET pipeline_dop=2, enable_query_cache=true"
}
```

### Wait Timeout Alignment

```
FE wait_timeout (fe.conf)         → 300 s
BI tool pool idle_timeout          → 240 s   (evict before FE closes)
BI tool pool keepalive interval    → 60 s    (ping to keep connection alive)
JDBC socketTimeout                 → 30 s    (fail fast on hung BE)
```

Always set the BI tool pool idle timeout **shorter** than the FE `wait_timeout`. If the BI tool holds a connection longer than `wait_timeout`, FE closes it silently, causing the next query on that connection to fail with "Lost connection to MySQL server".

---

## 10. Monitoring and Diagnostics

### Key Metrics to Watch

```sql
-- 1. Current query concurrency by resource group
SELECT
    resource_group,
    COUNT(*) AS running_queries,
    COUNT(*) FILTER (WHERE state = 'PENDING') AS queued_queries
FROM information_schema.current_queries
GROUP BY resource_group;

-- 2. Resource group CPU usage
SELECT *
FROM information_schema.be_workgroup_metrics
ORDER BY cpu_core_used DESC;

-- 3. Query latency histogram (BE metrics endpoint)
-- GET http://be-host:8040/metrics  → search for query_execution_time_us_*

-- 4. Slow queries log
-- fe/log/fe.audit.log  — contains query, user, duration, resource group

-- 5. Query queue depth trend
SELECT
    FLOOR(UNIX_TIMESTAMP(start_time) / 60) * 60 AS minute_bucket,
    COUNT(*) AS queued_count
FROM information_schema.query_history
WHERE state = 'PENDING'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 60;
```

### Throughput Capacity Estimation

Before tuning, establish your cluster's maximum sustainable throughput:

```sql
-- Run this to measure baseline: a simple aggregation that represents a "typical dashboard query"
-- Measure QPS at which p99 > SLA (e.g., 500 ms)

-- Query profiling: enable profile collection for slow queries
SET enable_profile = true;
SET big_query_profile_threshold = 5000;  -- collect profile for queries > 5 s

-- View profiles
SHOW QUERY PROFILE '/';  -- list recent profiles
SHOW QUERY PROFILE '/<query_id>';  -- detail for specific query
```

---

## Anti-Patterns

### 1. High pipeline_dop With Many Concurrent Queries

**Wrong:** Keep global `pipeline_dop=0` (auto = 8 on 32-core BEs) and run 200 concurrent BI queries.  
**Result:** `200 × 8 = 1600` pipeline drivers competing for 32 execution threads; massive context switching; p99 spikes to seconds.  
**Fix:** Set `pipeline_dop=2` globally and use `SET_VAR` hints for queries that genuinely need higher DOP.

### 2. No Resource Groups

**Wrong:** All queries — ETL scans, ad-hoc analysis, dashboard queries — compete in a single pool.  
**Result:** One heavy ETL query can saturate all CPU cores and cause all BI dashboards to timeout simultaneously.  
**Fix:** Create at minimum a `short_query` resource group for BI service accounts and a `normal` group for ETL, with `cpu_core_limit` enforcing isolation.

### 3. No Query Queue

**Wrong:** Disable query queue and allow unlimited concurrency.  
**Result:** At peak load, 400 queries arrive simultaneously; the BE is overwhelmed; all 400 fail instead of 200 succeeding quickly and 200 waiting briefly.  
**Fix:** Enable `enable_query_queue_select=true` with a reasonable `query_queue_max_queued_queries` so excess queries wait rather than fail.

### 4. Single FE for Hundreds of BI Users

**Wrong:** Point all BI tool connections at one FE node.  
**Result:** FE becomes the bottleneck for query planning and connection handling; `qe_max_connection` exhausted; FE CPU becomes the bottleneck even though BEs are idle.  
**Fix:** Deploy 3+ FE nodes and place a load balancer in front. FE query planning is CPU-intensive; distribute it.

### 5. Oversized JDBC Connection Pool

**Wrong:** Set `maximumPoolSize=500` on each of 10 application instances = 5000 connections to one FE.  
**Result:** FE OOM on connection state; `qe_max_connection` exceeded; FE refuses connections from legitimate users.  
**Fix:** Size each pool conservatively (`10–50` per instance); distribute connections across multiple FE nodes via load balancer.

### 6. Ignoring Query Cache for Dashboard Workloads

**Wrong:** Leave `enable_query_cache=false` on a cluster where 50 Grafana panels refresh every 30 seconds all querying the same yesterday/last-week partitions.  
**Result:** BE scan threads re-decompress the same data blocks thousands of times per hour.  
**Fix:** Enable query cache; set `query_cache_capacity=1073741824` (1 GB) on BEs; pin historical partition queries to cache-eligible patterns.

### 7. Point Queries Without Persistent Index

**Wrong:** Use a Primary Key table for key-value lookups but leave `enable_persistent_index=false`.  
**Result:** Every lookup becomes a full tablet scan; latency is 50–200 ms instead of 1–3 ms.  
**Fix:** Always set `enable_persistent_index=true` on Primary Key tables used for point lookups. For shared-storage deployments use `persistent_index_type=CLOUD`.

### 8. Mismatched wait_timeout and Pool Idle Timeout

**Wrong:** FE `wait_timeout=28800` (8 hours default), pool `idleTimeout=3600000` (1 hour).  
**Result:** After 1 hour, the pool evicts connections from its side but FE still counts them; on reconnect FE occasionally sends an error on the first query of a recycled connection.  
**Fix:** Set FE `wait_timeout=300`; pool `idleTimeout=240000`; pool `keepaliveTime=60000`. FE closes idle connections at 300 s, pool evicts them at 240 s before FE does.

---

## Quick-Reference Configuration Checklist

```
## FE (fe.conf or ADMIN SET FRONTEND CONFIG)
qe_max_connection                    = 2048      # per FE
wait_timeout                         = 300
enable_query_queue_select            = true
enable_query_queue_load              = true
query_queue_max_queued_queries       = 1000
query_queue_pending_timeout_second   = 120
query_queue_concurrency_limit        = 300

## BE (be.conf)
pipeline_exec_thread_pool_thread_num = 0         # auto = core count
num_threads_per_disk                 = 4          # SSD
max_scanner_thread_num               = 48
brpc_num_threads                     = -1         # auto
query_cache_capacity                 = 536870912  # 512 MB

## Global session variables (SET GLOBAL)
pipeline_dop               = 2                   # for 200+ concurrent BI queries
enable_query_cache         = true
enable_point_query_detail_cache = true

## Resource groups
rg_dashboard   type=short_query cpu_core_limit=4  mem_limit=20% concurrency_limit=100
rg_adhoc       type=normal      cpu_core_limit=12 mem_limit=40% concurrency_limit=20
rg_etl         type=normal      cpu_core_limit=16 mem_limit=35% concurrency_limit=10
```

---

## References to Consult When Needed

- StarRocks 3.x Documentation: [Resource Groups](https://docs.starrocks.io/docs/administration/resource_group/)
- StarRocks 3.x Documentation: [Query Queue](https://docs.starrocks.io/docs/administration/query_queues/)
- StarRocks 3.x Documentation: [Query Cache](https://docs.starrocks.io/docs/using_starrocks/query_cache/)
- StarRocks 3.x Documentation: [Pipeline Engine](https://docs.starrocks.io/docs/administration/BE_configuration/)
- StarRocks 3.x Documentation: [Primary Key Table](https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/)
- StarRocks 3.x Documentation: [FE Configuration](https://docs.starrocks.io/docs/administration/FE_configuration/)
