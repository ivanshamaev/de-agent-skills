---
name: starrocks-aggregation-optimizer
description: StarRocks aggregation optimization — one-phase vs two-phase aggregation, pre-aggregation pushdown to scan, Aggregate Key table automatic merge, GROUP BY on distribution key (local aggregation), COUNT DISTINCT optimization (multi-phase), HLL/BITMAP approximate aggregation, aggregation spill tuning, EXPLAIN aggregation nodes
---

# StarRocks Aggregation Optimizer

## When to Use

Load this skill when the user needs to:
- Diagnose slow `GROUP BY` queries or aggregation bottlenecks in StarRocks
- Understand or control one-phase vs two-phase aggregation planning
- Optimize `COUNT DISTINCT` queries on high-cardinality columns
- Design pre-aggregation into Aggregate Key tables or async materialized views
- Use HLL or BITMAP sketches to approximate distinct counts at scale
- Tune aggregation memory usage to prevent OOM on large groupings
- Read and interpret `EXPLAIN` output for `AggregationNode`, `AggregateNode`, pre-aggregation flags
- Understand when spill-to-disk is triggered and how to configure it

---

## Aggregation Architecture

StarRocks uses a two-tier aggregation pipeline across the distributed cluster:

```
┌───────────────────────────────────────────────────────────────────┐
│  BE node (per tablet)                                             │
│  ┌──────────────┐    ┌─────────────────┐                         │
│  │  OlapScanNode│ →  │ Local AggNode   │  (partial aggregation)  │
│  │  preagg=on   │    │ (per-BE reduce) │                         │
│  └──────────────┘    └────────┬────────┘                         │
└───────────────────────────────┼───────────────────────────────────┘
                                │ Exchange (network shuffle)
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│  FE / Coordinator                                                 │
│  ┌──────────────────────────────┐                                 │
│  │  Global AggNode              │  (final merge / aggregation)   │
│  │  (merge partial results)     │                                 │
│  └──────────────────────────────┘                                 │
└───────────────────────────────────────────────────────────────────┘
```

### Three levels where aggregation work happens

| Level | Who | What |
|---|---|---|
| **Scan pre-aggregation** | Each BE, at storage layer | Merges rowsets of Aggregate Key table; eliminates rows before they reach the query engine |
| **Local (partial) aggregation** | Each BE, in the query pipeline | Builds a local hash table per BE; shrinks data before network transfer |
| **Global (final) aggregation** | Coordinator or shuffle target BE | Merges all partial results into the final GROUP BY result |

The goal is to eliminate as many rows as possible before they cross the network. The optimizer selects one-phase or two-phase strategy based on statistics and distribution properties.

---

## One-Phase vs Two-Phase Aggregation

### One-Phase Aggregation

One-phase aggregation occurs when **all rows belonging to a GROUP BY key are guaranteed to reside on the same BE**. This happens when:
- The `GROUP BY` key is identical to or a prefix of the distribution (hash bucket) key
- The distribution key guarantees that `hash(group_by_cols) % buckets` maps each group to a single BE

With one-phase aggregation:
- No shuffle exchange is needed — each BE aggregates its own complete, non-overlapping slice of the data
- The coordinator collects already-final results from each BE and unions them
- `EXPLAIN` shows a **single** `AggregationNode` (or `ONE_PHASE_LOCAL_AGGREGATE` label) above the scan

```
EXPLAIN SELECT user_id, SUM(amount)
FROM orders                          -- DISTRIBUTED BY HASH(user_id)
GROUP BY user_id;

-- EXPLAIN output (simplified):
PLAN FRAGMENT 0
  OUTPUT EXPRS: user_id | sum(amount)
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:AGGREGATE (update finalize)      ← single AggregationNode, finalize on BE
    group by: user_id
    output: sum(amount)
  1:OlapScanNode
    TABLE: orders
    PREAGGREGATION: ON
```

Key signals in EXPLAIN:
- `AGGREGATE (update finalize)` — partial AND final aggregation done on same BE
- `PREAGGREGATION: ON` — storage-level merge active
- No `EXCHANGE` node between scan and aggregate

### Two-Phase Aggregation

Two-phase aggregation occurs when the GROUP BY keys do **not** match the distribution key, so rows for the same group can live on multiple BEs:

1. **Phase 1 (Partial)**: each BE builds a local partial aggregate hash table over its own tablets
2. **Exchange**: partial results are shuffled by GROUP BY key hash to the correct BE
3. **Phase 2 (Final/Merge)**: each target BE receives all partial results for its assigned keys and performs the final merge

```
EXPLAIN SELECT region, COUNT(*), SUM(revenue)
FROM orders                          -- DISTRIBUTED BY HASH(user_id)
GROUP BY region;

-- EXPLAIN output (simplified):
PLAN FRAGMENT 0
  OUTPUT EXPRS: region | count(*) | sum(revenue)
  PARTITION: UNPARTITIONED

  RESULT SINK

  4:AGGREGATE (merge finalize)       ← Phase 2: global merge on coordinator
    group by: region
    output: count(*), sum(revenue)
  3:EXCHANGE                         ← network shuffle partitioned by region

PLAN FRAGMENT 1
  PARTITION: HASH_PARTITIONED: region

  STREAM DATA SINK
    EXCHANGE ID: 03
    HASH_PARTITIONED: region

  2:AGGREGATE (update serialize)     ← Phase 1: partial agg on each BE
    group by: region
    output: count(*), sum(revenue)
  1:OlapScanNode
    TABLE: orders
    PREAGGREGATION: ON
```

Key signals:
- `AGGREGATE (update serialize)` — Phase 1 partial result, serialized for network
- `EXCHANGE` node with `HASH_PARTITIONED: <group_by_cols>` — shuffle by group key
- `AGGREGATE (merge finalize)` — Phase 2 final merge

### CBO Selection Logic

The Cost-Based Optimizer (CBO) chooses based on:

| Condition | CBO Chooses |
|---|---|
| GROUP BY ⊆ distribution key | One-phase (local) |
| GROUP BY NDV is very low (e.g., <1000 distinct values) | One-phase, broadcast the small result set |
| GROUP BY NDV is high, distribution key differs | Two-phase shuffle |
| COUNT DISTINCT on high-cardinality column | Multi-phase (see COUNT DISTINCT section) |
| Table statistics missing or stale | CBO may choose suboptimally — run `ANALYZE TABLE` |

Force a specific strategy when the CBO gets it wrong:

```sql
-- Force two-phase (useful when CBO picks one-phase but data is skewed)
SET new_planner_agg_stage = 2;

-- Force one-phase
SET new_planner_agg_stage = 1;

-- Let CBO decide (default)
SET new_planner_agg_stage = 0;
```

---

## Pre-Aggregation Pushdown

### What Pre-Aggregation Is

For **Aggregate Key** tables, StarRocks stores pre-merged rowsets: at load time and compaction time, rows with identical key columns are merged using the declared aggregate functions (`SUM`, `MAX`, `MIN`, `REPLACE`, `HLL_UNION`, `BITMAP_UNION`, etc.).

At scan time, the storage layer can return **one merged row per key** instead of all raw rowsets — this is called **pre-aggregation**. It eliminates rows before they ever reach the query engine pipeline, dramatically reducing CPU and memory usage downstream.

### Reading `PREAGGREGATION` in EXPLAIN

```
OlapScanNode
  TABLE: dws_user_metrics
  PREAGGREGATION: ON        ← storage merges rowsets at scan
```

vs.

```
OlapScanNode
  TABLE: dws_user_metrics
  PREAGGREGATION: OFF       ← full raw rowsets returned to engine
```

When `PREAGGREGATION: ON`, the scan node emits at most one row per distinct key combination — massive reduction for tables with high load frequency.

### Reasons Pre-Aggregation Turns OFF

Pre-aggregation cannot be applied when the query prevents the storage layer from safely merging:

| Reason | Example |
|---|---|
| Query aggregate function ≠ stored aggregate function | Table stores `SUM(amount)` but query does `AVG(amount)` — engine needs raw values |
| WHERE filter references a non-key (value) column | `WHERE revenue > 1000` — the merged `SUM(revenue)` cannot be filtered per-row |
| SELECT references a non-key column without aggregation | `SELECT user_id, amount FROM agg_table` — raw `amount` values needed |
| Query uses DISTINCT on a value column | `SELECT DISTINCT user_id, status` where status is a value column |
| JOIN on a value column | Joining the table on a value column forces full row read |

**Diagnosis and fix:**

```sql
-- Check if preagg is ON
EXPLAIN SELECT date, SUM(page_views), SUM(click_count)
FROM dws_daily_stats
GROUP BY date;
-- Look for: PREAGGREGATION: ON

-- This turns preagg OFF — AVG requires raw values:
EXPLAIN SELECT date, AVG(session_duration)
FROM dws_daily_stats   -- session_duration stored as SUM
GROUP BY date;
-- PREAGGREGATION: OFF

-- Fix: store both SUM and COUNT separately in Aggregate Key table
-- Then compute AVG = SUM / COUNT in the query
CREATE TABLE dws_daily_stats (
    date              DATE           NOT NULL,
    user_id           BIGINT         NOT NULL,
    page_views        BIGINT         SUM     DEFAULT '0',
    click_count       BIGINT         SUM     DEFAULT '0',
    session_dur_sum   BIGINT         SUM     DEFAULT '0',  -- SUM for AVG numerator
    session_dur_cnt   BIGINT         SUM     DEFAULT '0'   -- COUNT for AVG denominator
)
AGGREGATE KEY(date, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32;

-- Query AVG correctly:
SELECT date, session_dur_sum / NULLIF(session_dur_cnt, 0) AS avg_session_sec
FROM dws_daily_stats
GROUP BY date;
-- PREAGGREGATION: ON
```

### Aggregate Key Table Auto-Merge

The Aggregate Key table continuously merges rowsets in background compaction. Query-time behavior:

```
Load batch 1: (user=1, date='2025-01-01', views=100)
Load batch 2: (user=1, date='2025-01-01', views=50)

During compaction → merged: (user=1, date='2025-01-01', views=150)

At query time (before compaction):
  Scan returns 2 rowsets → engine applies SUM → 150
At query time (after compaction):
  Scan returns 1 merged row → engine reads 150 directly
  PREAGGREGATION: ON in both cases — but after compaction it's faster
```

Force compaction to ensure pre-aggregation efficiency:

```sql
-- Manual compaction on a specific partition
ALTER TABLE dws_daily_stats COMPACT PARTITION p20250101;

-- Check compaction status
SHOW PROC '/statuses/be_compaction_tasks';
```

---

## GROUP BY on Distribution Key

When the `GROUP BY` clause matches the distribution key, the optimizer recognizes that all rows for each group are co-located on a single BE — no shuffle is necessary.

### Identifying Local Aggregation in EXPLAIN

```sql
-- Table: DISTRIBUTED BY HASH(user_id)
EXPLAIN SELECT user_id, SUM(amount), COUNT(*) AS txn_count
FROM orders
GROUP BY user_id;
```

EXPLAIN output signals for local aggregation:
- Single `AggregationNode` with no preceding `EXCHANGE`
- Label `ONE_PHASE_LOCAL_AGGREGATE` or `update finalize` on same fragment as scan
- Fragment partition type matches distribution key

```
PLAN FRAGMENT 1
  PARTITION: HASH_PARTITIONED: user_id    ← matches distribution key

  2:AGGREGATE (update finalize)            ← local agg, no shuffle
    group by: user_id
    output: sum(amount), count(*)
  1:OlapScanNode
    TABLE: orders
    PREAGGREGATION: ON
```

### Design for Local Aggregation

Choose the distribution key to match the most frequent `GROUP BY` pattern:

```sql
-- Metric table: most queries GROUP BY user_id
CREATE TABLE dws_user_daily_metrics (
    dt              DATE           NOT NULL,
    user_id         BIGINT         NOT NULL,
    page_views      BIGINT         SUM     DEFAULT '0',
    session_count   BIGINT         SUM     DEFAULT '0',
    revenue         DECIMAL(18,2)  SUM     DEFAULT '0.00'
)
AGGREGATE KEY(dt, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 64   -- user_id = distribution key
PARTITION BY RANGE(dt) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 DAY)
)
PROPERTIES ("replication_num" = "3");

-- This query gets LOCAL aggregation (no network shuffle):
SELECT user_id, SUM(revenue) AS total_revenue
FROM dws_user_daily_metrics
WHERE dt BETWEEN '2025-01-01' AND '2025-03-31'
GROUP BY user_id;
```

When the primary aggregation pattern involves multiple columns:

```sql
-- If most queries GROUP BY (region, user_id), set distribution key to user_id
-- (region usually has low cardinality — putting it first causes skew)
DISTRIBUTED BY HASH(user_id) BUCKETS 64
-- GROUP BY region, user_id → still two-phase, but user_id hashing helps partial agg
```

---

## COUNT DISTINCT Optimization

### The Problem with Naive COUNT DISTINCT

`COUNT(DISTINCT col)` on a high-cardinality column is expensive:
1. All rows must be read and de-duplicated
2. De-duplication requires a large hash set in memory
3. In two-phase mode, partial results cannot be merged — COUNT DISTINCT is not decomposable

### Multi-Phase COUNT DISTINCT

StarRocks 3.x implements a **three-phase** strategy for high-cardinality COUNT DISTINCT:

```
Phase 1 (Partial, per-BE):
  For each (group_key, distinct_col), emit one row
  → reduces to unique (group, value) pairs per BE

Phase 2 (Shuffle):
  Shuffle by (group_key, distinct_col_bucket)
  → each target BE receives a subset of distinct values

Phase 3 (Final merge):
  Count distinct values per group_key
```

Enable bucket-based parallelism:

```sql
-- Session variables for COUNT DISTINCT bucketing
SET enable_distinct_column_bucketization = true;
SET count_distinct_column_buckets = 1024;   -- split distinct space into 1024 virtual buckets
```

With bucketing enabled, Phase 2 shuffle distributes distinct values across more BEs, reducing the per-node memory footprint.

EXPLAIN with COUNT DISTINCT and bucketing:

```sql
EXPLAIN SELECT user_id, COUNT(DISTINCT session_id)
FROM events
GROUP BY user_id;

-- Shows 3 AggregationNode operators in the plan:
-- AggregateNode (update serialize)     ← Phase 1 local partial
-- EXCHANGE HASH_PARTITIONED            ← shuffle by (user_id, session_id bucket)
-- AggregateNode (merge serialize)      ← Phase 2 bucket-level partial count
-- EXCHANGE HASH_PARTITIONED            ← shuffle by user_id
-- AggregateNode (merge finalize)       ← Phase 3 final COUNT
```

### Multiple COUNT DISTINCT Columns

Multiple `COUNT DISTINCT` on different columns in the same query triggers rewrite to avoid cross-product:

```sql
-- Problematic: StarRocks rewrites this internally
SELECT
    dt,
    COUNT(DISTINCT user_id)    AS dau,
    COUNT(DISTINCT session_id) AS sessions
FROM events
GROUP BY dt;

-- StarRocks 3.x rewrites as UNION of two separate COUNT DISTINCT queries
-- or uses multi-column bucketing — let the optimizer handle it
-- Verify with EXPLAIN

-- Alternative: use HLL for approximate counts (see HLL section)
SELECT
    dt,
    approx_count_distinct(user_id)    AS dau_approx,
    approx_count_distinct(session_id) AS sessions_approx
FROM events
GROUP BY dt;
```

### approx_count_distinct()

`approx_count_distinct(col)` uses HyperLogLog internally. Key properties:
- ~2% relative error (configurable via HLL precision)
- 10–100× faster than exact `COUNT(DISTINCT)`
- Works on any data type (strings, integers, UUIDs)
- Does not require a special table type — works on Duplicate Key tables

```sql
-- Fast approximate DAU with ~2% error
SELECT
    DATE(event_ts)          AS dt,
    approx_count_distinct(user_id)   AS dau,
    approx_count_distinct(device_id) AS unique_devices
FROM ods.page_events
WHERE event_ts >= '2025-01-01'
GROUP BY DATE(event_ts)
ORDER BY dt;
```

---

## HLL Approximate Aggregation

HLL (HyperLogLog) sketches stored natively in Aggregate Key tables enable approximate `COUNT DISTINCT` with:
- ~1–2% relative error
- ~10× storage reduction compared to storing raw distinct values
- O(1) merge cost — HLL sketches are mergeable by XOR

### Aggregate Key Table with HLL Column

```sql
CREATE TABLE dws_user_daily_hll (
    dt          DATE    NOT NULL,
    region      VARCHAR(64),
    -- HLL columns for approximate distinct counts
    user_hll    HLL     HLL_UNION   NOT NULL,   -- approximate distinct users
    device_hll  HLL     HLL_UNION   NOT NULL    -- approximate distinct devices
)
AGGREGATE KEY(dt, region)
DISTRIBUTED BY HASH(region) BUCKETS 16
PARTITION BY RANGE(dt) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 DAY)
)
PROPERTIES ("replication_num" = "3");
```

### Loading HLL Data

```sql
-- INSERT using hll_hash() to create HLL sketch from a raw value
INSERT INTO dws_user_daily_hll
SELECT
    DATE(event_ts)    AS dt,
    region,
    hll_hash(user_id)   AS user_hll,
    hll_hash(device_id) AS device_hll
FROM ods.page_events
WHERE event_ts >= CURRENT_DATE - INTERVAL 1 DAY;
```

With StreamLoad from a Routine Load or Spark connector, use the `hll_hash` function mapping in the column expression.

### Querying HLL Columns

```sql
-- HLL_UNION_AGG merges sketches across rows, HLL_CARDINALITY returns the estimate
SELECT
    dt,
    region,
    HLL_UNION_AGG(user_hll)                         AS raw_hll_sketch,
    HLL_CARDINALITY(HLL_UNION_AGG(user_hll))        AS approx_dau,
    HLL_CARDINALITY(HLL_UNION_AGG(device_hll))      AS approx_unique_devices
FROM dws_user_daily_hll
WHERE dt BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY dt, region
ORDER BY dt, region;

-- Roll up across regions — HLL merges are O(1) per sketch:
SELECT
    dt,
    HLL_CARDINALITY(HLL_UNION_AGG(user_hll)) AS total_dau
FROM dws_user_daily_hll
WHERE dt BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY dt
ORDER BY dt;
```

### HLL Functions Reference

| Function | Usage |
|---|---|
| `hll_hash(value)` | Create HLL sketch from a single value — use at ingest |
| `hll_empty()` | Create empty HLL sketch — useful for COALESCE default |
| `HLL_UNION(col)` | Aggregate function: merge HLL sketches into one sketch |
| `HLL_UNION_AGG(col)` | Alias for `HLL_UNION` (same semantics) |
| `HLL_CARDINALITY(hll_val)` | Scalar: return estimated cardinality from a single HLL sketch |
| `HLL_RAW_AGG(col)` | Return merged HLL sketch as raw bytes (for downstream processing) |

### HLL vs Exact COUNT DISTINCT

| Property | Exact COUNT DISTINCT | HLL Approximate |
|---|---|---|
| Error | 0% | ~1–2% |
| Memory (per group) | O(distinct values) | ~16 KB fixed per sketch |
| Query speed | Slow on high cardinality | 10–100× faster |
| Storage | No extra overhead | ~16 KB per HLL column per row |
| Supports merge/rollup | No (must re-aggregate from raw) | Yes — sketches merge in O(1) |
| Works without Aggregate Key table | Yes | Yes (approx_count_distinct) — but sketch storage requires Aggregate Key |

---

## BITMAP Aggregation

BITMAP provides **exact** COUNT DISTINCT for integer ID columns by storing a compressed bitset. Each bit position represents whether that integer ID appears in the group.

### When to Use BITMAP vs HLL

| Criterion | BITMAP | HLL |
|---|---|---|
| Accuracy required | Exact (0% error) | Approximate (~1–2% error) |
| Column type | Integer (TINYINT to BIGINT, non-negative) | Any type |
| Cardinality range | Best under 10M distinct values per bitmap | Any cardinality |
| Set operations needed | YES — `bitmap_and`, `bitmap_or`, `bitmap_not` | No native set ops |
| Storage per row | Variable (compressed, ~few KB–MB for dense IDs) | Fixed ~16 KB |

### Aggregate Key Table with BITMAP Column

```sql
CREATE TABLE dws_user_daily_bitmap (
    dt          DATE        NOT NULL,
    region      VARCHAR(64) NOT NULL,
    active_users BITMAP     BITMAP_UNION  NOT NULL   -- exact distinct user_id count
)
AGGREGATE KEY(dt, region)
DISTRIBUTED BY HASH(region) BUCKETS 16
PARTITION BY RANGE(dt) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 DAY)
)
PROPERTIES ("replication_num" = "3");
```

### Loading BITMAP Data

```sql
-- to_bitmap() converts integer to single-bit BITMAP
INSERT INTO dws_user_daily_bitmap
SELECT
    DATE(event_ts)    AS dt,
    region,
    to_bitmap(user_id) AS active_users
FROM ods.page_events
WHERE event_ts >= CURRENT_DATE - INTERVAL 1 DAY
  AND user_id IS NOT NULL
  AND user_id >= 0;   -- to_bitmap requires non-negative integers
```

For non-integer IDs (e.g., UUIDs, strings), map to integer IDs first using a dimension table, then use `to_bitmap()`.

### Querying BITMAP Columns

```sql
-- Exact DAU per day/region
SELECT
    dt,
    region,
    BITMAP_UNION_COUNT(active_users) AS exact_dau   -- exact COUNT DISTINCT
FROM dws_user_daily_bitmap
WHERE dt BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY dt, region;

-- Rollup across regions — BITMAP_UNION merges sets:
SELECT
    dt,
    BITMAP_UNION_COUNT(BITMAP_UNION(active_users)) AS total_dau
FROM dws_user_daily_bitmap
WHERE dt BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY dt;

-- Set operations: users active on both Jan 1 and Jan 2
SELECT BITMAP_AND_COUNT(a.active_users, b.active_users) AS retained
FROM dws_user_daily_bitmap a
JOIN dws_user_daily_bitmap b ON a.region = b.region
WHERE a.dt = '2025-01-01' AND b.dt = '2025-01-02'
  AND a.region = 'US';
```

### BITMAP Functions Reference

| Function | Usage |
|---|---|
| `to_bitmap(bigint)` | Convert integer to single-element BITMAP |
| `bitmap_from_string('1,2,3')` | Create BITMAP from comma-separated integers |
| `BITMAP_UNION(col)` | Aggregate function: merge BITMAPs — use as agg function in GROUP BY |
| `BITMAP_UNION_COUNT(col)` | Aggregate function: merge BITMAPs and return cardinality — shorthand for `BITMAP_COUNT(BITMAP_UNION(col))` |
| `BITMAP_COUNT(bitmap)` | Scalar: return cardinality of a single BITMAP value |
| `BITMAP_AND(b1, b2)` | Scalar set intersection |
| `BITMAP_OR(b1, b2)` | Scalar set union |
| `BITMAP_NOT(b1, b2)` | Scalar set difference (b1 minus b2) |
| `BITMAP_AND_COUNT(b1, b2)` | Cardinality of intersection |
| `BITMAP_OR_COUNT(b1, b2)` | Cardinality of union |
| `bitmap_contains(bitmap, val)` | Membership test |

---

## Aggregation Spill

### When Aggregation Spills to Disk

For large GROUP BY operations where the hash table cannot fit in BE memory, StarRocks 3.x supports spilling intermediate aggregation data to local disk. Without spill, large aggregations cause BE OOM and query failure.

Spill is triggered when aggregate hash table memory exceeds a threshold, writing sorted run files to the BE's spill directory (`spill_storage_volume`).

### Spill Configuration Variables

**Session-level (per query):**

```sql
-- Enable automatic spill when memory threshold is exceeded
SET spill_mode = 'auto';        -- spill only when needed (default: 'none')
-- or
SET spill_mode = 'force';       -- always spill (for testing or guaranteed-OOM queries)

-- Memory threshold per operator before spill triggers (bytes)
-- Default: 2 GB
SET spill_operator_min_memory_bytes = 2147483648;

-- Size of each in-memory spill partition before flush to disk (bytes)
-- Default: 1 MB
SET spill_mem_table_size = 1048576;
```

**BE configuration (`be.conf`):**

```properties
# Directory for spill temporary files (should be on fast SSD)
spill_storage_volume = /data/starrocks/spill

# Max total disk space for spill across all queries (bytes)
# Default: unlimited (0)
spill_storage_limit = 107374182400   # 100 GB
```

### Spill vs Memory Tuning Decision

| Situation | Recommended Action |
|---|---|
| Occasional large GROUP BY on wide date ranges | Enable `spill_mode = 'auto'` at session level |
| Recurring large aggregations in scheduled jobs | Tune BE `mem_limit` upward; add more BE nodes |
| OOM during COUNT DISTINCT | Use HLL or BITMAP sketches instead |
| Ad-hoc exploratory query on full table | `SET spill_mode = 'force'` for safety |
| Production dashboard queries OOM | Redesign using Aggregate Key table or async MV |
| BE memory < 64 GB and spill is frequent | Scale out BE nodes; spill has high I/O cost |

### Monitoring Spill in EXPLAIN ANALYZE

```sql
-- After running with spill, check query profile for spill metrics:
-- In the query profile (SHOW QUERY PROFILE '<query_id>'):
-- SpillBytes: bytes written to disk by spill
-- SpillWriteTime: time spent writing spill files
-- SpillReadTime: time spent reading spill files back
-- If SpillBytes > 0 and SpillWriteTime dominates → memory is the bottleneck
```

---

## EXPLAIN Aggregation Analysis

### Full EXPLAIN Walkthrough

```sql
EXPLAIN SELECT
    region,
    DATE(event_ts)            AS dt,
    COUNT(*)                  AS events,
    SUM(revenue)              AS total_revenue,
    COUNT(DISTINCT user_id)   AS dau
FROM ods.page_events
WHERE event_ts >= '2025-01-01'
GROUP BY region, DATE(event_ts);
```

Sample annotated output:

```
PLAN FRAGMENT 0                              ← coordinator fragment
  OUTPUT EXPRS: region | dt | events | total_revenue | dau
  PARTITION: UNPARTITIONED

  RESULT SINK

  8:AGGREGATE (merge finalize)               ← Phase 3: final COUNT DISTINCT merge
    group by: region, dt
    output: count(*), sum(revenue), count(distinct user_id)
  7:EXCHANGE                                 ← shuffle by (region, dt)

PLAN FRAGMENT 1                              ← intermediate fragment
  PARTITION: HASH_PARTITIONED: region, dt

  6:AGGREGATE (merge serialize)              ← Phase 2: partial distinct count
    group by: region, dt, user_id            ← user_id still in group for dedup
    output: count(*), sum(revenue)
  5:EXCHANGE                                 ← shuffle by (region, dt, user_id_bucket)

PLAN FRAGMENT 2                              ← BE scan fragment
  PARTITION: HASH_PARTITIONED: region, dt

  4:AGGREGATE (update serialize)             ← Phase 1: partial agg on each BE
    group by: region, dt, user_id
    output: count(*), sum(revenue)
  3:OlapScanNode
      TABLE: page_events
      PREAGGREGATION: OFF                    ← OFF because table is Duplicate Key
      partitions: 365/365 selected
      rollup: page_events
      tabletRatio: 32/32                     ← all 32 buckets scanned
      cardinality: 1200000000
      avgRowSize: 128.0
      numNodes: 8
```

### Key EXPLAIN Signals for Aggregation

| Signal | Meaning | Action |
|---|---|---|
| `PREAGGREGATION: ON` | Storage-level merge active on Aggregate Key table | Good — no action needed |
| `PREAGGREGATION: OFF` | Pre-agg disabled | Investigate why: check WHERE/SELECT clauses |
| `AGGREGATE (update finalize)` | One-phase local agg — optimal | Good — distribution key matches GROUP BY |
| `AGGREGATE (update serialize)` | Phase 1 partial agg | Check if two-phase is avoidable |
| `AGGREGATE (merge finalize)` | Final merge — coordinator | Normal for two-phase |
| `AGGREGATE (merge serialize)` | Intermediate merge — 3-phase COUNT DISTINCT | Expected for high-cardinality distinct |
| `EXCHANGE HASH_PARTITIONED` | Data shuffle — check shuffle key matches GROUP BY | OK for two-phase |
| High `cardinality` at scan with low rows after agg | Pre-agg or local agg is eliminating rows well | Good |
| Low `tabletRatio` | Partition pruning working | Good |
| `tabletRatio: 32/32` on full table with wide range | All partitions scanned — review partition filter | Add partition predicate |

### Rows-In vs Rows-Out Ratio

Check the aggregation efficiency via the ratio of input rows (scan cardinality) to output rows (final aggregate result):

```sql
-- Use EXPLAIN COSTS or query profile for row count estimates:
EXPLAIN COSTS SELECT region, SUM(revenue) FROM orders GROUP BY region;

-- If AggregationNode shows:
--   rows-in:  100 000 000
--   rows-out:     5 000
-- → 20 000:1 reduction — aggregation is highly effective

-- If rows-in ≈ rows-out → low selectivity — consider HLL/BITMAP or MV
```

---

## Design Patterns

### Decision Matrix: Where to Perform Aggregation

| Scenario | Recommended Approach | Rationale |
|---|---|---|
| Fixed set of known metrics, loaded continuously | **Aggregate Key table** | Pre-aggregation at load+compaction time; zero query-time aggregation cost for exact SUM/MAX/MIN |
| Approximate DAU / unique counts, query latency < 100 ms | **HLL column in Aggregate Key table** | Sketch merges in microseconds; 10× storage savings |
| Exact distinct counts on integer IDs, set operations needed | **BITMAP column in Aggregate Key table** | Exact, mergeable, supports AND/OR/NOT |
| Complex aggregations computed on-demand, shape unknown at design time | **Async Materialized View** | Transparent rewrite; handles multi-table agg; can be refreshed on schedule |
| Ad-hoc exploration on raw data | **Query-time aggregation on Duplicate Key** | No pre-aggregation design needed; use approx_count_distinct for speed |
| Frequently queried rollup (e.g., daily → weekly → monthly) | **Tiered Aggregate Key tables** with ETL pipelines | Each tier pre-aggregates the lower tier; compounding reduction |

### Pre-Aggregation in Aggregate Key Table Pattern

```sql
-- Tier 1: hourly pre-aggregated metrics
CREATE TABLE dws_events_hourly (
    event_hour      DATETIME       NOT NULL,
    region          VARCHAR(64)    NOT NULL,
    event_type      VARCHAR(64)    NOT NULL,
    event_count     BIGINT         SUM     DEFAULT '0',
    revenue_sum     DECIMAL(18,2)  SUM     DEFAULT '0.00',
    user_hll        HLL            HLL_UNION NOT NULL
)
AGGREGATE KEY(event_hour, region, event_type)
DISTRIBUTED BY HASH(region) BUCKETS 16
PARTITION BY RANGE(event_hour) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 DAY)
)
PROPERTIES ("replication_num" = "3");

-- Tier 2: daily pre-aggregated metrics (aggregated from Tier 1)
CREATE TABLE dws_events_daily (
    event_date      DATE           NOT NULL,
    region          VARCHAR(64)    NOT NULL,
    event_count     BIGINT         SUM     DEFAULT '0',
    revenue_sum     DECIMAL(18,2)  SUM     DEFAULT '0.00',
    user_hll        HLL            HLL_UNION NOT NULL
)
AGGREGATE KEY(event_date, region)
DISTRIBUTED BY HASH(region) BUCKETS 16
PARTITION BY RANGE(event_date) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 DAY)
)
PROPERTIES ("replication_num" = "3");

-- Load Tier 2 from Tier 1 using HLL_UNION:
INSERT INTO dws_events_daily
SELECT
    DATE(event_hour)       AS event_date,
    region,
    SUM(event_count)       AS event_count,
    SUM(revenue_sum)       AS revenue_sum,
    HLL_UNION(user_hll)    AS user_hll
FROM dws_events_hourly
WHERE event_hour >= CURRENT_DATE - INTERVAL 1 DAY
  AND event_hour <  CURRENT_DATE
GROUP BY DATE(event_hour), region;
```

### Async Materialized View for Complex Aggregations

```sql
-- Async MV for weekly cohort retention — complex aggregation not suited for Aggregate Key
CREATE MATERIALIZED VIEW mv_weekly_retention
DISTRIBUTED BY HASH(cohort_week) BUCKETS 8
REFRESH ASYNC EVERY (INTERVAL 1 DAY)
AS
SELECT
    DATE_TRUNC('week', first_event_date)  AS cohort_week,
    DATE_TRUNC('week', event_date)        AS activity_week,
    COUNT(DISTINCT user_id)               AS retained_users
FROM (
    SELECT
        user_id,
        MIN(DATE(event_ts))  OVER (PARTITION BY user_id) AS first_event_date,
        DATE(event_ts)                                    AS event_date
    FROM ods.page_events
) cohort_data
GROUP BY
    DATE_TRUNC('week', first_event_date),
    DATE_TRUNC('week', event_date);

-- StarRocks CBO will transparently rewrite matching queries to use this MV
```

---

## Anti-Patterns

### 1. COUNT DISTINCT on High-Cardinality Column Without Sketch

```sql
-- BAD: Forces full deduplication hash set in memory; OOM on billions of rows
SELECT dt, COUNT(DISTINCT user_id) AS dau
FROM ods.page_events
GROUP BY dt;

-- GOOD option A: Use approx_count_distinct (2% error, 100× faster)
SELECT dt, approx_count_distinct(user_id) AS dau_approx
FROM ods.page_events
GROUP BY dt;

-- GOOD option B: Use HLL column in Aggregate Key table (1% error, O(1) merge)
SELECT dt, HLL_CARDINALITY(HLL_UNION_AGG(user_hll)) AS dau_approx
FROM dws_events_daily
GROUP BY dt;

-- GOOD option C: Use BITMAP column for exact result with integer IDs
SELECT dt, BITMAP_UNION_COUNT(active_users) AS exact_dau
FROM dws_user_daily_bitmap
GROUP BY dt;
```

### 2. GROUP BY Without Statistics Leading to Bad Plan

```sql
-- Symptom: CBO chooses two-phase when one-phase would be faster
-- (or vice versa) because NDV estimates are wrong

-- Fix: run ANALYZE to collect column statistics
ANALYZE TABLE dws_events_hourly UPDATE HISTOGRAM ON region, event_type;
ANALYZE TABLE orders;

-- Check statistics exist:
SHOW STATS META WHERE `Table` = 'orders';

-- Check column histogram:
SHOW HISTOGRAM META WHERE `Table` = 'orders' AND `Column` = 'region';
```

### 3. Pre-Aggregation Disabled by WHERE Filter on Value Column

```sql
-- BAD: WHERE on non-key column disables preagg on Aggregate Key table
SELECT event_date, SUM(revenue)
FROM dws_daily_revenue           -- Aggregate Key table
WHERE revenue > 1000             -- revenue is a value column → preagg OFF
GROUP BY event_date;

-- FIX A: Move filter to key columns
SELECT event_date, SUM(revenue)
FROM dws_daily_revenue
WHERE event_date >= '2025-01-01' -- event_date is a key column → preagg ON
GROUP BY event_date;

-- FIX B: If filtering on value column is required, use Duplicate Key + query-time agg
--        or filter after aggregation:
SELECT event_date, total_revenue
FROM (
    SELECT event_date, SUM(revenue) AS total_revenue
    FROM dws_daily_revenue
    GROUP BY event_date
) t
WHERE total_revenue > 1000;
-- PREAGGREGATION: ON in inner query
```

### 4. Mismatched Aggregate Function Disabling Pre-Aggregation

```sql
-- BAD: Table stores SUM(revenue) but query computes AVG — preagg can't help
CREATE TABLE dws_daily_avg_wrong (
    dt       DATE           NOT NULL,
    user_id  BIGINT         NOT NULL,
    revenue  DECIMAL(18,2)  SUM DEFAULT '0.00'   -- stored as SUM
)
AGGREGATE KEY(dt, user_id);

SELECT dt, AVG(revenue) FROM dws_daily_avg_wrong GROUP BY dt;
-- PREAGGREGATION: OFF — engine must read raw rows to compute AVG

-- GOOD: Store both SUM and COUNT; compute AVG at query time
CREATE TABLE dws_daily_avg_correct (
    dt            DATE           NOT NULL,
    user_id       BIGINT         NOT NULL,
    revenue_sum   DECIMAL(18,2)  SUM DEFAULT '0.00',
    revenue_cnt   BIGINT         SUM DEFAULT '0'
)
AGGREGATE KEY(dt, user_id);

SELECT dt, SUM(revenue_sum) / NULLIF(SUM(revenue_cnt), 0) AS avg_revenue
FROM dws_daily_avg_correct
GROUP BY dt;
-- PREAGGREGATION: ON
```

### 5. Large Spill Without Root Cause Investigation

```sql
-- BAD: Enabling spill as a permanent fix without understanding root cause
SET spill_mode = 'force';   -- on by default for all queries

-- GOOD: Profile first, then choose the right solution
-- Step 1: Check if GROUP BY key has high or low cardinality
-- Step 2: If low cardinality → one-phase should work; check if stats are stale
-- Step 3: If high cardinality → consider HLL/BITMAP or MV rollup
-- Step 4: If result set is large and memory is genuinely insufficient → spill is correct

-- Only set spill at session level for specific queries:
SET spill_mode = 'auto';
SELECT region, user_segment, COUNT(*), SUM(revenue)
FROM fact_orders_large
GROUP BY region, user_segment;
SET spill_mode = 'none';   -- restore default after
```

### 6. Using BITMAP on Non-Integer or Negative IDs

```sql
-- BAD: to_bitmap() only accepts non-negative BIGINT
SELECT to_bitmap(user_id) FROM events WHERE user_id < 0;   -- returns NULL or error
SELECT to_bitmap(session_uuid) FROM events;                 -- won't work on VARCHAR

-- GOOD: Map to integer ID first
-- For UUID sessions: maintain a session_id → integer_id mapping table
-- For negative IDs: shift by offset → to_bitmap(user_id + 2147483648)
-- Or use HLL (supports any type):
SELECT hll_hash(session_uuid) FROM events;
```

---

## References

- StarRocks documentation — Query acceleration: aggregation optimization
- StarRocks documentation — Aggregate Key table: pre-aggregation and compaction behavior
- StarRocks documentation — HLL functions and approximate COUNT DISTINCT
- StarRocks documentation — BITMAP functions and exact COUNT DISTINCT
- StarRocks documentation — Query spill configuration (`spill_mode`, `spill_operator_min_memory_bytes`)
- StarRocks documentation — EXPLAIN output interpretation: fragment plans, AggregationNode labels
- StarRocks documentation — Session variables: `new_planner_agg_stage`, `enable_distinct_column_bucketization`, `count_distinct_column_buckets`
- StarRocks documentation — Async Materialized Views for transparent query rewrite
- Related skills: `starrocks-ddl-table-types` (Aggregate Key table design), `starrocks-explain-plan` (EXPLAIN reading), `starrocks-materialized-views` (async MV setup), `starrocks-bucketing` (distribution key selection for local aggregation)
