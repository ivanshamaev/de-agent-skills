---
name: starrocks-query-optimizer
description: StarRocks query optimization — vectorized execution engine, pipeline execution model, runtime filters (bloom/in/min-max), predicate pushdown, partition pruning, sort merge optimization, query hints (SET_VAR/JOIN hints), session variables for optimizer control, CBO statistics (ANALYZE TABLE/AUTO ANALYZE), query cache, pipeline parallelism tuning
---

# StarRocks — Query Optimizer

## When to Use

Load this skill when the user needs to:
- Investigate a slow StarRocks query (EXPLAIN, profile analysis)
- Optimize a new query before it reaches production
- Reduce BI dashboard latency (runtime filters, query cache, MV rewrite)
- Tune batch ETL performance (parallelism, join strategies, predicate pushdown)
- Collect or refresh CBO statistics for better query plans
- Override the optimizer's default decisions with hints or session variables
- Diagnose missing runtime filters, bad join strategies, or skewed aggregations

---

## Execution Architecture

### Vectorized Engine

StarRocks evaluates every operator on **column batches** (vectors), not row-by-row. A single batch contains up to 4096 rows per column stored as a contiguous typed array. This layout:

- Enables **SIMD (AVX2/AVX-512)** instructions — the CPU processes 8–16 values per clock cycle for arithmetic, comparisons, and hash operations.
- Improves CPU cache utilization: one column's batch fits in L2/L3 cache; accessing adjacent rows in a column is sequential.
- Eliminates per-row interpreter dispatch overhead present in traditional volcano-model engines.

Column batches flow through a chain of vectorized operators: `Scan → Filter → Project → HashJoin → Aggregate → Sort → Exchange → Output`.

### Pipeline Execution Model

StarRocks 3.x executes queries using a **pipeline engine** instead of the legacy blocking scheduler. Key concepts:

| Concept | Description |
|---|---|
| **Fragment** | A sub-plan that runs on one BE. A query plan is split into multiple fragments connected by Exchange nodes. |
| **Fragment Instance** | One parallel copy of a fragment running on one BE core lane. Multiple instances run concurrently (controlled by `pipeline_dop`). |
| **Pipeline** | An operator chain inside a fragment that can execute without blocking. Adjacent operators are fused into one pipeline where possible. |
| **Driver** | The execution unit scheduled by the BE pipeline scheduler. Each driver processes one batch at a time in a non-blocking loop. |
| **Exchange Node** | Boundary between fragments; shuffles data between BEs (hash/broadcast/pass-through). |

```
Query Plan (on FE)
 └─ Fragment 0: Coordinator (result sink)
      └─ Exchange ← Fragment 1 (Aggregation, dop=4)
                        └─ Exchange ← Fragment 2 (HashJoin + Scan, dop=4)
                                           ├─ Build: Scan(dim_table) × 4 instances
                                           └─ Probe: Scan(fact_table) × 4 instances
```

### Operator Fusion

The pipeline engine fuses adjacent operators that share the same data locality into a single compiled pipeline. Example: a scan, a predicate filter, and a projection on the same table are fused into one pipeline — the column batch never leaves the CPU register file between operators. This eliminates intermediate materializations and reduces memory allocation.

---

## Cost-Based Optimizer (CBO)

### How CBO Works

The CBO uses column statistics to estimate:
- **Row counts** after each filter and join
- **Cardinality** of GROUP BY keys (affects aggregation strategy: local pre-agg vs. two-phase)
- **Data distribution** for join reorder (smaller build-side first, broadcast threshold check)
- **NDV (Number of Distinct Values)** for runtime filter type selection

Without accurate statistics, the optimizer defaults to conservative estimates that may produce bad plans (wrong join order, broadcast of a large table, missing local pre-aggregation).

### Collecting Full Statistics

```sql
-- Collect column-level statistics for an entire table (full scan)
ANALYZE TABLE orders;

-- Collect stats for specific columns only (faster for wide tables)
ANALYZE TABLE orders (user_id, order_date, amount, region);

-- Full stats + histograms in one pass
ANALYZE TABLE orders UPDATE HISTOGRAM ON region, status;

-- Collect stats for an external Iceberg table via catalog
ANALYZE TABLE iceberg_catalog.sales_db.orders;
```

`ANALYZE TABLE` is asynchronous by default. It submits a background job. Check progress:

```sql
SHOW ANALYZE STATUS;
-- Output columns: Id, Database, Table, Columns, Type, Status, StartTime, EndTime, Properties, Reason
```

For a synchronous collection (blocks until done — use only in scripts):

```sql
ANALYZE TABLE orders WITH SYNC MODE;
```

### Histograms for Skewed Columns

A histogram captures the value distribution of a column in buckets, enabling the optimizer to estimate filtered row counts more precisely for high-skew columns:

```sql
-- Build histograms on columns that appear in WHERE predicates with equality / range filters
ANALYZE TABLE orders UPDATE HISTOGRAM ON region, status, order_source
PROPERTIES ("histogram_bucket_num" = "128");
```

Guidelines:
- Use histograms on **low-to-medium cardinality** columns that have uneven distributions (e.g., 80% of orders come from one region).
- Avoid histograms on very high-cardinality columns (UUIDs, order IDs) — the CBO uses NDV-based estimates instead, and histograms add overhead.
- Default bucket count is 64; increase to 128–256 for high-skew distributions.

### Auto-Analyze

StarRocks can automatically collect statistics in the background:

```sql
-- Enable auto stats collection on first data load (recommended for new tables)
SET GLOBAL enable_statistic_collect_on_first_load = true;

-- Trigger background re-analysis when the fraction of modified rows exceeds this ratio
-- Default: 0.8 (80% of rows changed triggers re-analysis)
SET GLOBAL statistic_auto_analyze_ratio = 0.5;   -- more aggressive: re-analyze at 50% change

-- Control auto-analyze time window (avoid business hours)
SET GLOBAL statistic_auto_analyze_start_time = '22:00:00';
SET GLOBAL statistic_auto_analyze_end_time   = '06:00:00';
```

Auto-analyze runs as background tasks on the FE. It does not interfere with query execution but does consume BE CPU/IO during the collection scan.

### Force Re-collection

```sql
-- Drop all statistics for a table, forcing the next ANALYZE (or auto-analyze) to rebuild from scratch
DROP STATS orders;

-- Drop only histogram data, keep basic column stats
DROP HISTOGRAM ON orders (region, status);
```

Use `DROP STATS` when you know data characteristics have fundamentally changed (e.g., after a data migration that altered value distributions).

### Inspecting Statistics

```sql
-- View column-level statistics (NDV, null_count, min, max, data_size, update_time)
SHOW STATS META WHERE `table` = 'orders';

-- View histogram bucket details for a specific column
SHOW HISTOGRAM META WHERE `table` = 'orders' AND `column` = 'region';
```

---

## Runtime Filters

### How Runtime Filters Work

Runtime filters are a join-time optimization: during a hash join, the **build side** (smaller table) generates a compact filter structure from its join key values. This filter is then **pushed down to the probe-side scan node** before probe-side data is read, allowing the scan to skip tablets, blocks, or rows that will never match the join.

```
HashJoin (orders.user_id = users.user_id)
  ├─ Build: Scan(users)  →  builds BloomFilter(user_id values)
  │                              ↓  sends filter to probe scan
  └─ Probe: Scan(orders) →  applies BloomFilter at scan time
                              (skips tablets with no matching user_ids)
```

The filter is built **before** the probe scan starts when possible (RF wait), or applied asynchronously as the probe scan progresses.

### Filter Types

| Type | Trigger Condition | Benefit |
|---|---|---|
| **Bloom filter** | Build-side NDV > `runtime_filter_max_in_num` (default 1024) | Probabilistic membership test; low false-positive rate; compact representation |
| **IN filter** | Build-side NDV ≤ `runtime_filter_max_in_num` (default 1024) | Exact membership list; zero false positives; very fast for small sets |
| **MIN-MAX filter** | Numeric / date join key on an ordered column | Prunes tablets outside [min, max] range; useful for time-range joins |

Multiple filter types are often combined: an IN filter for an exact small-set check plus a MIN-MAX filter for range pruning on a date column.

### Enabling and Configuring

```sql
-- Session variable: master switch (default: true in StarRocks 3.x)
SET enable_runtime_filter = true;

-- Maximum number of distinct values for the IN filter path (above this, use Bloom)
SET runtime_filter_max_in_num = 1024;

-- Whether to push runtime filters across Exchange nodes (inter-fragment)
-- Enabling this allows filters to cross BE boundaries (more pruning, slight extra latency)
SET runtime_filter_on_exchange_node = true;

-- Max memory for a single Bloom filter build (bytes); prevents OOM on huge build sides
SET global_runtime_filter_build_max_size = 67108864;  -- 64 MB default

-- How long the probe scan waits for the build-side filter before proceeding without it (ms)
SET runtime_filter_scan_wait_time = 20;   -- default 20 ms
```

### Reading Runtime Filters in EXPLAIN VERBOSE

```sql
EXPLAIN VERBOSE
SELECT o.order_id, o.amount, u.name
FROM orders o
JOIN users u ON o.user_id = u.user_id
WHERE u.country = 'RU';
```

Key nodes to look for:

```
PLAN FRAGMENT 1
  4: HASH JOIN [INNER JOIN, BROADCAST]
     |  join predicates: o.user_id = u.user_id
     |  build runtime filters:
     |    - filter_id = RF000, build_expr = u.user_id, filter_type = BLOOM_FILTER

PLAN FRAGMENT 2
  0: OlapScanNode [orders]
     |  TABLE: orders
     |  PREDICATES: o.user_id IS NOT NULL
     |  runtime filters: RF000[user_id] <- RF000
```

The `runtime filters: RF000[user_id] <- RF000` line at the OlapScanNode confirms the filter was pushed to the scan. If it is missing, the filter was not applied (check type mismatch or size thresholds).

### Common Runtime Filter Issues

| Symptom | Cause | Fix |
|---|---|---|
| RF not in EXPLAIN | Build side too large (exceeds `global_runtime_filter_build_max_size`) | Increase max size or filter the dimension table first |
| RF in plan but not effective | Column type mismatch between build and probe sides | Ensure join keys have identical types (cast if needed) |
| RF not crossing Exchange | `runtime_filter_on_exchange_node = false` | Set to `true` for inter-fragment RF pushdown |
| IN filter not used | NDV exceeds `runtime_filter_max_in_num` | Bloom filter is used instead — usually acceptable; reduce threshold to keep IN filter |

---

## Predicate Pushdown

### How Predicates Push Down

The optimizer rewrites the query plan to move filter predicates as close to the data source (OlapScanNode or ExternalScanNode) as possible. Benefits:
- Fewer rows pass through the operator pipeline, reducing downstream CPU and memory.
- Predicates applied at scan time leverage tablet-level pruning (when the predicate targets partition columns or sort key prefix).

```sql
-- Without pushdown (hypothetical naive plan):
-- Scan ALL rows → JOIN → FILTER (amount > 1000)

-- With predicate pushdown (StarRocks default):
-- Scan (amount > 1000 applied at tablet) → JOIN
```

### Verifying Pushdown with EXPLAIN

```sql
EXPLAIN
SELECT o.order_id, o.amount
FROM orders o
WHERE o.order_date >= '2026-01-01'
  AND o.amount > 1000
  AND o.region = 'EU';
```

Look for `PREDICATES` in the `OlapScanNode` section:

```
0: OlapScanNode
   TABLE: orders
   PREAGGREGATION: ON
   PREDICATES: order_date >= '2026-01-01', amount > 1000, region = 'EU'
   partitions=2/12
   rollup: orders
   tabletRatio=16/192
```

- `partitions=2/12` — partition pruning eliminated 10 of 12 partitions (predicate on `order_date`).
- `tabletRatio=16/192` — only 16 of 192 tablets are scanned.
- `PREDICATES` at scan level — all three filters are pushed down.

### Column Pruning

StarRocks scans only the columns referenced in the query (projection pushdown). Wide tables with 100+ columns benefit significantly: if the query touches 5 columns, the engine reads only those 5 column files from storage.

```sql
-- Only user_id, order_date, amount are scanned — all other columns are skipped
SELECT user_id, SUM(amount)
FROM orders
WHERE order_date = '2026-01-15'
GROUP BY user_id;
```

### Short-Circuit Evaluation

Predicates in a conjunction (`AND`) are evaluated in the order the optimizer chooses, with the most selective predicate first. The optimizer uses NDV statistics to estimate selectivity:

- A filter `region = 'EU'` with NDV=5 (5 regions, 20% selectivity) is applied before `status = 'active'` with NDV=2 (50% selectivity).
- Enable reordering: `SET enable_predicate_reorder = true;` (default: true).

---

## Sort Optimization

### Top-N Optimization

When a query has both `ORDER BY` and `LIMIT`, StarRocks recognizes the **Top-N pattern** and uses a priority queue (heap) to avoid materializing all rows:

```sql
-- Top-N: reads only enough data to maintain a heap of size 100
SELECT user_id, SUM(amount) AS total
FROM orders
WHERE order_date >= '2026-01-01'
GROUP BY user_id
ORDER BY total DESC
LIMIT 100;
```

`EXPLAIN` shows `TOP-N` in the sort node:

```
3: SORT
   order by: total DESC
   TOP-N: true
   limit: 100
```

Without `LIMIT`, the sort node materializes all rows before sorting — avoid unbounded `ORDER BY` on large result sets.

### Pre-Sorted Data Advantage

StarRocks tables are physically sorted by the sort key (the leading columns of the key in Duplicate Key / Aggregate Key / Primary Key models). If the `ORDER BY` in the query matches the **prefix** of the sort key, the scan returns pre-sorted data and the sort operator becomes a pass-through or a cheap merge:

```sql
-- Table: DUPLICATE KEY(order_date, user_id, order_id)
-- This ORDER BY matches the sort key prefix — no sort needed
SELECT order_date, user_id, amount
FROM orders
ORDER BY order_date, user_id
LIMIT 1000;
```

`EXPLAIN` for this query will show either no `SORT` node, or:

```
3: SORT
   order by: order_date ASC, user_id ASC
   one_phase_sort: true   -- data already sorted per BE, no merge needed
```

### One-Phase vs Two-Phase Sort

| Sort Phase | When | Cost |
|---|---|---|
| `one_phase_sort` | Data is pre-sorted on each BE by the sort key; result is a local top-N per BE, then a merge at coordinator | Low — one pass per BE |
| `two_phase_sort` | Data is not pre-sorted; each BE does a full sort locally, then a merge sort at coordinator | Higher — full sort on each BE |

To get one-phase sort, ensure `ORDER BY` columns match the table's sort key prefix.

---

## Query Hints

Hints are SQL comments that override the optimizer's automatic decisions for a single query execution without changing session variables or table definitions.

### SET_VAR Hint — Per-Query Session Variable Override

```sql
-- Override pipeline_dop just for this query (e.g., reduce for a small lookup)
SELECT /*+ SET_VAR(pipeline_dop=2) */
    user_id, COUNT(*) AS cnt
FROM orders
WHERE order_date = '2026-01-15'
GROUP BY user_id;

-- Assign a resource group for an ETL query
SELECT /*+ SET_VAR(resource_group='etl_rg') */
    region,
    SUM(amount) AS total_amount
FROM orders
GROUP BY region;

-- Disable runtime filter for a specific query (debugging)
SELECT /*+ SET_VAR(enable_runtime_filter=false) */
    o.order_id, u.name
FROM orders o JOIN users u ON o.user_id = u.user_id;

-- Increase optimizer timeout for a complex multi-way join query
SELECT /*+ SET_VAR(new_planner_optimize_timeout=30000) */
    ...;
```

### Join Strategy Hints

```sql
-- Force BROADCAST join: replicate the smaller right-hand table to every probe BE
-- Use when the optimizer chooses SHUFFLE but the right table is known to be tiny
SELECT /*+ JOIN(orders BROADCAST) */
    o.order_id, p.product_name
FROM orders o
JOIN products p ON o.product_id = p.product_id;

-- Force SHUFFLE (hash redistribution) join: redistribute both sides by join key
-- Use when the optimizer chooses BROADCAST but the right table is large
SELECT /*+ JOIN(orders SHUFFLE) */
    o.order_id, l.location_name
FROM orders o
JOIN large_dim_table l ON o.location_id = l.location_id;

-- Hint applies to a specific join; use table alias if needed
SELECT /*+ JOIN(o BROADCAST) */
    o.order_id, r.region_name
FROM orders o
JOIN regions r ON o.region_id = r.region_id
WHERE o.order_date >= '2026-01-01';
```

### Nested Loop Join Hint

Nested loop join (NLJ) is used for cross joins and non-equi joins. The optimizer may also choose it for tiny tables. To force NLJ:

```sql
-- Force nested loop join between t1 and t2
-- Only appropriate when at least one side is very small (< thousands of rows)
SELECT /*+ USE_NL(t1 t2) */
    t1.id, t2.value
FROM t1
JOIN t2 ON t1.key > t2.lower AND t1.key < t2.upper;
```

Avoid `USE_NL` on large tables — it produces O(m × n) row comparisons and will be extremely slow.

### Combining Multiple Hints

```sql
SELECT /*+ SET_VAR(pipeline_dop=8, resource_group='reporting_rg') JOIN(dim BROADCAST) */
    f.sale_date,
    d.category,
    SUM(f.revenue) AS total_revenue
FROM fact_sales f
JOIN dim_category d ON f.category_id = d.category_id
WHERE f.sale_date BETWEEN '2026-01-01' AND '2026-03-31'
GROUP BY f.sale_date, d.category;
```

---

## Session Variables for Optimizer Control

Set at session level with `SET` or globally with `SET GLOBAL`. Use `SET_VAR` hints for per-query overrides.

### Parallelism

```sql
-- Degree of parallelism (DOP) for pipeline execution
-- 0 = auto (FE calculates based on BE core count and query complexity)
-- N = fixed number of pipeline driver instances per fragment per BE
SET pipeline_dop = 0;        -- recommended default: auto
SET pipeline_dop = 4;        -- fixed 4 parallel drivers per fragment per BE

-- Max DOP the auto mode can choose (cap for high-concurrency scenarios)
SET max_pipeline_dop = 16;
```

**Formula for manual DOP tuning**: `pipeline_dop = floor(BE_cores / 4)` as a starting point, then double for scan-heavy workloads or halve for memory-bound joins.

### Join Reorder and Planning

```sql
-- Maximum number of join nodes considered during join reorder search
-- Higher = better plans for complex queries; lower = faster optimization
SET cbo_max_reorder_node = 8;    -- default: 8 (covers most schemas)
SET cbo_max_reorder_node = 4;    -- faster planning for 10+ table joins where reorder adds little

-- Enable/disable CBO table pruning (eliminate unused join tables)
SET enable_cbo_table_prune = true;

-- Optimizer time budget in milliseconds (query fails if exceeded)
SET new_planner_optimize_timeout = 10000;   -- 10 s default

-- Broadcast join size threshold (bytes): if build side > this, use SHUFFLE instead
SET broadcast_row_limit = 15000000;         -- 15 M rows default
```

### Predicate and Filter Control

```sql
-- Allow optimizer to reorder predicates by estimated selectivity
SET enable_predicate_reorder = true;

-- Push down predicates through outer joins (careful: may change semantics)
SET enable_outer_join_reorder = true;

-- Enable partition pruning for dynamic partition predicates
SET enable_partition_prune = true;

-- Minimum size of a runtime filter (bytes); filters smaller than this are skipped
-- (very small filters have negligible benefit and add overhead)
SET runtime_filter_min_size = 1024;

-- Maximum size of a single runtime filter (bytes)
SET runtime_filter_max_size = 16777216;   -- 16 MB
```

### Statistics and CBO Behavior

```sql
-- Disable CBO and fall back to rule-based optimizer (emergency only; not recommended)
SET enable_cbo = true;   -- keep true in production

-- Use per-column statistics for cardinality estimates (requires ANALYZE to be run)
SET enable_statistics_collect_profile = true;

-- Assume uniform distribution when column stats are missing (vs. worst-case)
-- Setting to false uses conservative estimates and may produce safer but slower plans
SET enable_missing_stats_estimate = true;
```

---

## Query Cache

### How the Query Cache Works

StarRocks 3.x provides a **tablet-level result cache** that stores the output of a scan + aggregate pipeline for a specific combination of:
- Scan range (tablet ID + version)
- Applied predicates
- Projected columns

When an identical sub-scan is requested again, the BE returns the cached result without re-reading the tablet. This is especially effective for BI dashboards that run the same aggregation over recent partitions repeatedly.

```
Without cache:  FE → BE1 (full tablet scan) → aggregate → result
With cache hit: FE → BE1 (cache lookup)     → result        (sub-ms)
```

### Enabling Query Cache

```sql
-- Session level
SET enable_query_cache = true;

-- Global default
SET GLOBAL enable_query_cache = false;   -- disabled by default in most deployments

-- Per-query via hint
SELECT /*+ SET_VAR(enable_query_cache=true) */
    order_date,
    SUM(amount) AS daily_revenue
FROM orders
WHERE order_date >= '2026-01-01'
GROUP BY order_date
ORDER BY order_date;
```

### Cache Size and Eviction

```sql
-- Maximum bytes stored per single cache entry (default: 4 MB)
-- Entries exceeding this limit are not cached
SET query_cache_entry_max_bytes = 8388608;    -- 8 MB

-- Maximum number of rows in a single cache entry (default: 409600)
SET query_cache_entry_max_rows = 409600;

-- Total cache capacity per BE is controlled in be.conf:
-- query_cache_capacity = 536870912   (512 MB, set in be.conf, requires BE restart)
```

Cache entries are evicted using LRU when capacity is exhausted.

### When Cache Hits

A cache hit requires all of the following to match:
1. Same tablet ID and same tablet version (no new data written since last cache fill)
2. Identical set of applied predicates (including runtime filters resolved at plan time)
3. Identical projected columns
4. Same aggregate functions

A cache **miss** occurs when:
- The tablet has been updated since the last cache fill (version changed)
- The query predicates differ even slightly (different date range, different filter value)
- The query is the first execution of this scan (cold start)

### Verifying Cache Usage with EXPLAIN VERBOSE

```sql
EXPLAIN VERBOSE
SELECT /*+ SET_VAR(enable_query_cache=true) */
    order_date, SUM(amount)
FROM orders
WHERE order_date = '2026-01-15'
GROUP BY order_date;
```

Look for `QueryCache` in the plan output:

```
0: OlapScanNode
   TABLE: orders
   QueryCache: { digest: 0x7f3a... }
```

At runtime, check cache hit/miss counters in the BE metrics:
```
query_cache_hit_count
query_cache_miss_count
query_cache_populate_count
```

Access via `curl http://<be_host>:8040/metrics | grep query_cache`.

### Query Cache Anti-Patterns

- Do not enable for OLTP-style queries that update rows frequently — the cache will be invalidated constantly, yielding no benefit.
- Avoid on tables with very large tablets exceeding `query_cache_entry_max_bytes` — cache entries will never be stored.
- Do not combine with `pipeline_dop` > 1 for aggregations that produce large result sets — each driver generates separate cache entries, multiplying memory consumption.

---

## Pipeline Parallelism Tuning

### Auto DOP (Recommended)

```sql
SET pipeline_dop = 0;   -- 0 = auto mode
```

In auto mode, the FE assigns DOP per query based on:
- Number of BE nodes and their core counts
- Query type (scan-heavy vs. compute-heavy)
- Table size estimates from statistics

Auto DOP is the correct default for mixed workloads. It avoids over-parallelism on small queries running at high concurrency.

### Manual DOP Tuning

Use fixed DOP only when you know the workload pattern:

```sql
-- Reduce DOP for small lookup queries or high-concurrency OLTP-like workloads
SET pipeline_dop = 2;

-- Increase DOP for a single large ETL scan (use most of the BE's cores)
SET pipeline_dop = 16;

-- Per-query override (preferred over session-level for production)
SELECT /*+ SET_VAR(pipeline_dop=8) */
    region, SUM(amount)
FROM orders
WHERE order_date BETWEEN '2026-01-01' AND '2026-03-31'
GROUP BY region;
```

### When to Reduce DOP

| Scenario | Recommended DOP | Reason |
|---|---|---|
| Small dimension table scan (< 1M rows) | 1–2 | Parallelism overhead exceeds benefit |
| High-concurrency dashboard (100+ concurrent queries) | 1–4 | Prevents CPU saturation; each query needs fewer cores |
| Memory-limited JOIN (large hash tables) | 2–4 | Fewer parallel hash tables means lower peak memory |
| Single large ETL query (exclusive time window) | 16–32 | Use full BE capacity |

### DOP and Memory Interaction

Each pipeline driver allocates its own hash table for joins and aggregations. Setting `pipeline_dop=16` means 16 independent hash tables in memory simultaneously per BE. For memory-bound workloads:

```sql
-- Estimate memory per driver: total_join_memory / pipeline_dop
-- If join build side is 2 GB and dop=16, each driver needs ~125 MB

-- Spill to disk when hash tables exceed memory limit (StarRocks 3.1+)
SET enable_spill = true;
SET spill_mem_table_size = 268435456;   -- 256 MB per spill chunk
```

---

## Optimization Workflow

Follow this systematic process to investigate and fix a slow query.

### Step 1 — Capture the Baseline Plan

```sql
-- Basic plan: shows join order, scan predicates, partition pruning
EXPLAIN
SELECT o.order_id, o.amount, u.name, p.category
FROM orders o
JOIN users u   ON o.user_id    = u.user_id
JOIN products p ON o.product_id = p.product_id
WHERE o.order_date >= '2026-01-01'
  AND u.country = 'RU'
  AND p.category IN ('Electronics', 'Books');
```

### Step 2 — Get Verbose Plan with Runtime Filter Details

```sql
EXPLAIN VERBOSE
SELECT o.order_id, o.amount, u.name, p.category
FROM orders o
JOIN users u   ON o.user_id    = u.user_id
JOIN products p ON o.product_id = p.product_id
WHERE o.order_date >= '2026-01-01'
  AND u.country = 'RU'
  AND p.category IN ('Electronics', 'Books');
```

Check for:
- `partitions=X/Y` at OlapScanNode — is partition pruning working?
- `tabletRatio=X/Y` — are tablets being skipped?
- `PREDICATES` at scan level — are all filters pushed down?
- `runtime filters: RF00X[col]` — are runtime filters applied to the large table scan?
- Join type: `BROADCAST` vs `SHUFFLE` — is the smaller table on the build side?

### Step 3 — Collect Statistics if Missing

```sql
-- Check whether stats are up to date
SHOW STATS META WHERE `table` IN ('orders', 'users', 'products');

-- Collect if missing or stale
ANALYZE TABLE orders (user_id, order_date, product_id, amount, region);
ANALYZE TABLE users  (user_id, country);
ANALYZE TABLE products (product_id, category);

-- Add histograms for low-cardinality filter columns
ANALYZE TABLE users    UPDATE HISTOGRAM ON country;
ANALYZE TABLE products UPDATE HISTOGRAM ON category;

-- Confirm collection finished
SHOW ANALYZE STATUS;
```

### Step 4 — Re-EXPLAIN and Identify Bottleneck

After fresh statistics, re-run `EXPLAIN VERBOSE` and look for improvements:
- Join order may flip (smaller table moves to build side)
- IN filter (instead of Bloom filter) may appear for `category` (low NDV)
- `partitions` and `tabletRatio` may decrease further

If the plan still looks suboptimal, identify the primary bottleneck:

| Observation | Likely Bottleneck | Action |
|---|---|---|
| `tabletRatio=192/192` | No partition pruning | Check partition column is in WHERE; ensure predicate pushdown |
| No runtime filters on large table | RF not applied | Check type mismatch; increase `global_runtime_filter_build_max_size` |
| `SHUFFLE` join on a tiny dimension | Wrong join strategy | Use `/*+ JOIN(dim BROADCAST) */` hint |
| Aggregation is two-phase with high-NDV keys | Skewed aggregation | Pre-aggregate in CTE; redistribute by a different key |
| High wall time despite good plan | Low DOP or resource contention | Increase `pipeline_dop`; check resource group limits |

### Step 5 — Apply Fix

```sql
-- Example: apply fixes discovered in Steps 3–4

-- Fix 1: correct join strategy for a known-small dimension
-- Fix 2: reduce DOP for a concurrent dashboard query
-- Fix 3: enable cross-Exchange runtime filters

SELECT
    /*+ SET_VAR(pipeline_dop=4, runtime_filter_on_exchange_node=true)
        JOIN(u BROADCAST) JOIN(p BROADCAST) */
    o.order_id,
    o.amount,
    u.name,
    p.category
FROM orders o
JOIN users u    ON o.user_id    = u.user_id
JOIN products p ON o.product_id = p.product_id
WHERE o.order_date >= '2026-01-01'
  AND u.country = 'RU'
  AND p.category IN ('Electronics', 'Books');
```

### Step 6 — Profile at Runtime (Query Profile)

For queries that are already in production and have real execution data, use query profiling:

```sql
-- Enable profiling for the current session
SET enable_profile = true;

-- Run the query
SELECT ...;

-- Retrieve the most recent profile (FE web UI also shows this)
-- In StarRocks 3.x: http://<fe_host>:8030/query_detail?query_id=<id>
```

The profile shows per-operator timing, row counts, and memory usage across all BEs — far more detail than `EXPLAIN VERBOSE`. Look for operators with disproportionately high `OperatorTotalTime` or `PullRowNum` significantly higher than expected.

---

## Anti-Patterns

### Stale or Missing Statistics

**Problem**: The CBO has no statistics or outdated statistics for a large table. The optimizer underestimates cardinality, causes a large table to be placed on the build side of a BROADCAST join, and the BE runs out of memory.

**Symptoms**:
- `EXPLAIN` shows BROADCAST join with a large table on the right (build) side
- Query is slow and profiler shows high memory allocation for the HashJoin operator
- `SHOW STATS META` shows a very old `UpdateTime` or no row at all

**Fix**:
```sql
ANALYZE TABLE large_fact_table (join_key_col, filter_col_1, filter_col_2) WITH SYNC MODE;
-- Then re-run the query — optimizer will now choose SHUFFLE join
```

### Missing Runtime Filter Due to Type Mismatch

**Problem**: Join key columns have different data types on the two sides (e.g., `INT` on the fact table and `BIGINT` on the dimension table). StarRocks does not apply a runtime filter when types do not match exactly after implicit cast resolution.

**Symptoms**:
- `EXPLAIN VERBOSE` shows a HashJoin node but **no** `runtime filters:` line at the probe OlapScanNode
- Large fact table is fully scanned even with a highly selective dimension filter

**Fix**:
```sql
-- Check types
DESCRIBE orders;    -- user_id: INT
DESCRIBE users;     -- user_id: BIGINT

-- Add explicit CAST to unify types (or better: fix the DDL)
SELECT o.order_id, u.name
FROM orders o
JOIN users u ON CAST(o.user_id AS BIGINT) = u.user_id
WHERE u.country = 'RU';

-- Long-term fix: ALTER TABLE to align column types
ALTER TABLE orders MODIFY COLUMN user_id BIGINT NOT NULL;
```

### Over-Parallelism on Small Tables

**Problem**: `pipeline_dop` is set globally to a high value (e.g., 16) for large ETL queries, but the same setting applies to small lookup queries, causing thread scheduling overhead to dominate over actual computation time.

**Symptoms**:
- Small queries (< 1M rows) have unexpectedly high latency in high-concurrency scenarios
- BE CPU metrics show high context-switch overhead
- Individual small queries are slower than expected given data size

**Fix**:
```sql
-- Reset session to auto DOP
SET pipeline_dop = 0;

-- Override per-query for large ETL queries only
SELECT /*+ SET_VAR(pipeline_dop=16) */
    region, SUM(amount)
FROM very_large_fact_table
GROUP BY region;
```

### Predicate Not Pushed Down (Non-Deterministic Functions)

**Problem**: A predicate using a non-deterministic function (e.g., `NOW()`, `RAND()`, `UUID()`) cannot be evaluated at planning time and is not pushed to the scan node.

**Symptoms**:
- Full table scan despite a date filter involving `NOW()` or `CURRENT_DATE()`
- `EXPLAIN` shows the filter at a higher operator (e.g., Project or Filter node), not at OlapScanNode

**Fix**:
```sql
-- Anti-pattern: NOW() prevents pushdown
SELECT * FROM orders WHERE order_date >= DATE_SUB(NOW(), INTERVAL 7 DAY);

-- Fix: bind the date value at the application layer or use a literal
SELECT * FROM orders WHERE order_date >= '2026-05-10';  -- computed externally

-- Or use a CTE with a pre-computed constant
WITH params AS (SELECT DATE_SUB(NOW(), INTERVAL 7 DAY) AS cutoff)
SELECT o.*
FROM orders o
CROSS JOIN params p
WHERE o.order_date >= p.cutoff;
-- Note: this still may not push through; prefer literal dates in high-performance paths
```

### Unbounded ORDER BY on Large Result Sets

**Problem**: `ORDER BY` without `LIMIT` forces a full global sort across all BEs. For millions of rows, this materializes the entire dataset in memory and sorts it, producing high latency and memory pressure.

```sql
-- Anti-pattern: full sort of 500M rows before returning to client
SELECT user_id, SUM(amount) AS total
FROM orders
GROUP BY user_id
ORDER BY total DESC;   -- no LIMIT: sorts all 50M user rows

-- Fix: always pair ORDER BY with LIMIT for analytical queries
SELECT user_id, SUM(amount) AS total
FROM orders
GROUP BY user_id
ORDER BY total DESC
LIMIT 1000;
```

### Broadcasting a Large Table

**Problem**: The optimizer lacks statistics and estimates a large table as small, choosing BROADCAST join. The BEs run out of memory building hash tables.

```sql
-- Anti-pattern: no hint, stale stats, optimizer broadcasts 100M-row table
SELECT f.*, d.name
FROM fact_sales f
JOIN dim_product d ON f.product_id = d.product_id;

-- Fix: force SHUFFLE to redistribute both sides by join key
SELECT /*+ JOIN(d SHUFFLE) */
    f.sale_date, d.name, SUM(f.revenue)
FROM fact_sales f
JOIN dim_product d ON f.product_id = d.product_id
GROUP BY f.sale_date, d.name;
```

---

## Output Expectations

When optimizing a StarRocks query, always provide:

1. **EXPLAIN analysis**: identify the problematic operator (scan, join, aggregate, sort) with the specific output line that reveals the issue.
2. **Root cause**: one-sentence diagnosis (e.g., "Runtime filter is missing because `orders.user_id` is INT and `users.user_id` is BIGINT — type mismatch prevents RF application").
3. **Fix**: the corrected SQL with hints, or the `ANALYZE TABLE` / `SET` statement required.
4. **Verification**: the expected change in the EXPLAIN output after the fix (e.g., "After CAST alignment, EXPLAIN VERBOSE will show `runtime filters: RF000[user_id]` at the OlapScanNode").

---

## References to Consult When Needed

- StarRocks 3.x documentation: Query Acceleration — Runtime Filter, CBO Statistics
- StarRocks 3.x documentation: Administration — Pipeline Engine, Resource Groups
- StarRocks 3.x documentation: SQL Reference — ANALYZE TABLE, EXPLAIN, Query Hints
- Internal spec: `docs/specs/trino_iceberg_performance_optimization.md` — cross-engine comparison for CBO and runtime filter concepts
