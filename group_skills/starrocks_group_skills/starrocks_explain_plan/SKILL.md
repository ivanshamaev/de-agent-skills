---
name: starrocks-explain-plan
description: StarRocks EXPLAIN plan reading — EXPLAIN/EXPLAIN VERBOSE/EXPLAIN COSTS output structure, operator nodes (OlapScanNode/HashJoinNode/AggregationNode/ExchangeNode/SortNode/ProjectNode), reading fragment instances, identifying bottlenecks (shuffle exchange cost, large scan rows, memory spill), runtime filter nodes, partition pruning in scan, query profile (SHOW PROFILELIST/EXPLAIN ANALYZE)
---

# StarRocks — EXPLAIN Plan Reading and Query Profiling

## When to Use

Load this skill when the user needs to:
- Diagnose why a StarRocks query is slow or consuming excessive memory
- Verify that partition pruning is eliminating partitions correctly
- Confirm runtime filters are being pushed down to OlapScanNode
- Validate that query rewrite to a materialized view is happening
- Understand whether a join is using BROADCAST, SHUFFLE, or COLOCATE strategy
- Confirm the query optimizer is using correct cardinality estimates
- Profile actual execution metrics (rows per operator, time, memory) with `EXPLAIN ANALYZE`
- Tune CBO statistics after ANALYZE TABLE or after schema changes
- Compare plan cost before and after adding a hint (`/*+ JOIN_HINT(...) */`)

---

## EXPLAIN Variants

StarRocks provides four EXPLAIN modes. Each reveals a different layer of the plan.

### `EXPLAIN SELECT ...`
Produces the **logical plan** with row cardinality estimates from CBO. Use this for a quick overview of the operator tree, join order, and filter placement. Output is the least verbose; good for a first look.

```sql
EXPLAIN
SELECT region, SUM(amount) AS revenue
FROM orders
WHERE order_date >= '2026-01-01'
GROUP BY region;
```

### `EXPLAIN VERBOSE SELECT ...`
Produces the **physical plan** with additional detail:
- Runtime filter build/probe assignments (`RF001`, `RF002`, ...)
- Per-operator output column list
- Predicate pushdown details
- `COLOCATE` join annotation
Use this when investigating runtime filter application or verifying column pruning.

```sql
EXPLAIN VERBOSE
SELECT o.region, SUM(o.amount)
FROM orders o
JOIN dim_region r ON o.region_id = r.region_id
GROUP BY o.region;
```

### `EXPLAIN COSTS SELECT ...`
Produces the **costed plan** with CBO cost estimates per operator:
- `cardinality` — estimated output rows
- `avgRowSize` — estimated bytes per row
- `cost` — cumulative cost score
Use this to spot cardinality mismatches, identify why the optimizer chose a particular join order, and verify statistics were collected.

```sql
EXPLAIN COSTS
SELECT o.order_id, c.customer_name, SUM(oi.quantity)
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, c.customer_name;
```

### `EXPLAIN ANALYZE SELECT ...`
**Executes the query** and collects actual runtime metrics per operator. Returns the plan annotated with:
- `PullRowNum` — rows produced by each operator
- `PushRowNum` — rows consumed
- `OperatorTotalTime` — cumulative wall clock time
- `PeakMemoryBytes` — peak memory per operator
This is the most powerful diagnostic tool but has overhead because it runs the query.

```sql
EXPLAIN ANALYZE
SELECT region, SUM(amount)
FROM orders
WHERE order_date BETWEEN '2026-01-01' AND '2026-03-31'
GROUP BY region;
```

---

## Plan Structure

### Fragments and Pipelines

A StarRocks query plan is divided into **fragments** (units of distributed execution). Each fragment runs on one or more BE nodes in parallel. Fragments communicate through **ExchangeNode** operators.

```
PLAN FRAGMENT 0   -- coordinator fragment, returns final result
  OUTPUT EXPRS: region, revenue
  PARTITION: UNPARTITIONED

    AGGREGATE (merge finalize)              -- second phase aggregation
    |  group by: region
    |
    EXCHANGE                                -- gather from worker fragments

PLAN FRAGMENT 1   -- worker fragments on all BEs
  PARTITION: HASH_PARTITIONED: region

    AGGREGATE (update serialize)            -- first phase pre-aggregation
    |  group by: region
    |
    OlapScanNode                            -- scan tablets on local BE
       TABLE: orders
       partitions=3/365
       rollup: orders
       tabletRatio=32/320
```

Reading rules:
- **Read bottom-up** to follow data flow (scan → filter → join → agg → exchange).
- **Read top-down** to follow fragment boundaries and understand shuffle topology.
- `PLAN FRAGMENT 0` is always the final fragment that sends rows to the client.
- Higher fragment numbers are deeper worker fragments running on BEs.

### Fragment Communication

| Exchange Type | Meaning | Cost Implication |
|---|---|---|
| `UNPARTITIONED` / `GATHER` | All rows sent to one node (coordinator) | Low volume OK; dangerous for large result sets |
| `BROADCAST` | Full copy of one side sent to every BE | Only safe when build side is small (< `broadcast_row_limit`) |
| `HASH_PARTITIONED: col` | Rows shuffled by hash of `col` across all BEs | Network-intensive; triggers full shuffle |
| `BUCKET_SHUFFLE_HASH_PARTITIONED` | Rows shuffled to match bucket placement of the probe-side table | Avoids full shuffle; requires matching distribution key |
| `COLOCATE` | No exchange at all; join done locally on co-located tablets | Zero shuffle cost; requires tables bucketed the same way |

---

## Key Operator Nodes

### OlapScanNode
The leaf node. Scans tablets from a StarRocks native table or rollup (synchronous MV).

**What to look for:**

```
OlapScanNode
   TABLE: orders
   PREAGGREGATION: ON
   PREDICATES: order_date >= '2026-01-01', amount > 0
   partitions=3/365          -- CRITICAL: 3 of 365 partitions scanned
   rollup: mv_orders_daily   -- MV rewrite happened
   tabletRatio=96/3650       -- tablets assigned to this fragment instance
   tabletList=10001,10002,...
   cardinality=150000
   avgRowSize=48.0
   RuntimeFilters: RF000[amount] <- RF000
```

- `partitions=X/Y`: X scanned out of Y total. If X equals Y on a partitioned table, pruning failed.
- `rollup`: if this shows an MV name rather than the base table name, query rewrite succeeded.
- `PREAGGREGATION: ON`: the Aggregate Key model is applying pre-aggregation during scan.
- `PREAGGREGATION: OFF`: pre-agg disabled; scan reads raw rows (common when SELECT uses non-key columns).
- `PREDICATES`: predicates pushed into the scan. If your filter is missing here, it was not pushed down.
- `RuntimeFilters`: shows which runtime filters are applied at this scan node.
- `tabletRatio=X/Y`: X tablets assigned to this instance out of Y total. Low ratio with many partitions is normal for parallel scan.

### HashJoinNode
Executes hash joins. Build phase materializes one side (inner hash table); probe phase streams the other side.

```
HashJoinNode
  join op: INNER JOIN (BROADCAST)
  colocate: false, reason: ...
  equal join conjunct: o.customer_id = c.customer_id
  build runtime filters:
    - filter_id = RF001, build_expr = c.customer_id, remote = false
```

**What to look for:**
- `BROADCAST`: build side fits in memory and was broadcast to all probe-side BEs. Verify build cardinality is small.
- `PARTITIONED` or `SHUFFLE`: both sides shuffled. This means two ExchangeNodes above the join — expensive.
- `COLOCATE`: no shuffle; both tables co-located on the same buckets. Best outcome for large joins.
- `build runtime filters`: runtime filters generated here and pushed to probe-side scan nodes.
- `colocate: false, reason: ...`: tells you exactly why colocate join was not used.

**Build vs probe side:** The build side is the smaller relation loaded into the hash table. In the plan, the build side is the right child of HashJoinNode. If the optimizer chose the wrong build side (larger table as build), use `/*+ JOIN_ORDER(...) */` or collect statistics.

### AggregationNode
Handles GROUP BY and aggregate functions. StarRocks normally uses **two-phase aggregation**:
- Phase 1 (`update serialize`): partial aggregation on each BE, reducing data before shuffle.
- Phase 2 (`merge finalize`): final aggregation after the shuffle exchange.

```
AGGREGATE (update serialize)          -- Fragment 1: pre-agg on each BE
  group by: region, order_date
  output: region, order_date, sum(amount)

EXCHANGE HASH_PARTITIONED: region     -- shuffle on GROUP BY key

AGGREGATE (merge finalize)            -- Fragment 0: final agg
  group by: region, order_date
  output: region, sum(amount)
```

**What to look for:**
- `(update serialize)` + `(merge finalize)` = normal two-phase agg with shuffle. Expected for large aggregations.
- `(update finalize)` only (one phase, no exchange before it): streaming aggregation; optimizer decided no shuffle needed (e.g., GROUP BY equals distribution key — colocate agg).
- `(merge serialize)` in middle fragments: multi-level aggregation for very complex cases.
- If `PREAGGREGATION: ON` appears in OlapScanNode, aggregation started even earlier at scan time (Aggregate Key model benefit).

### ExchangeNode
Represents data movement between fragments. The most significant cost driver for distributed queries.

```
EXCHANGE
   HASH_PARTITIONED: o.region_id
   cardinality=5000000
```

**What to look for:**
- Type: `UNPARTITIONED`, `BROADCAST`, `HASH_PARTITIONED`, `BUCKET_SHUFFLE_HASH_PARTITIONED`.
- High cardinality on a `HASH_PARTITIONED` exchange = large shuffle = likely bottleneck.
- Multiple `HASH_PARTITIONED` exchanges in one plan = multiple shuffles. Each is a round-trip across the network.
- `BROADCAST` with high cardinality build side = excessive memory and network. Check if the optimizer has stale stats making the build side appear small.

### SortNode
Handles ORDER BY and TopN.

```
SortNode
  order by: revenue DESC NULLS LAST
  offset: 0
  limit: 100
  TopN: true
```

**What to look for:**
- `TopN: true`: optimizer detected LIMIT and will maintain a bounded heap rather than sorting the full dataset. Much cheaper.
- Missing `TopN`: sort without LIMIT — materializes and sorts the entire input. Often a bottleneck.
- `PARTIAL SORT` in an intermediate fragment: partial sort before exchange, then final merge sort in the coordinator fragment. This is efficient.

### ProjectNode
Evaluates expressions and computes derived columns. Usually lightweight, but watch for:
- Complex expressions evaluated on every row (CASE WHEN, string functions, JSON extraction).
- Projection after a large join — if expressions can be moved before the join, push them earlier.

```
ProjectNode
  output: order_id, amount * 1.1 AS amount_with_tax, UPPER(region) AS region_upper
```

### TableFunctionNode
Used for LATERAL JOIN with table functions (e.g., `json_each`, `unnest`).

```
TableFunctionNode
  table function: json_each(payload)
  lateral join
```

**What to look for:** These expand rows; a JSON column with deeply nested arrays can multiply row count significantly. Check output cardinality versus input cardinality.

### UnionNode / IntersectNode / ExceptNode
Handle `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT`. Each child branch runs independently and results are combined.

- `UNION ALL` (UnionNode with passthrough): no dedup, cheapest.
- `UNION` (UnionNode with aggregate): dedup required, adds an AggregationNode on top.
- Always verify partition pruning is applied independently to each branch of a UNION.

---

## Reading Row Estimates (Cardinality)

Every node in `EXPLAIN COSTS` output shows `cardinality=X`. This is the CBO's estimate of rows output by that operator.

```
HashJoinNode
  ...
  cardinality=2500000    -- optimizer expects 2.5 M rows out of this join
```

**When estimates are wrong:**
- `cardinality=1` or `cardinality=-1`: no statistics; the optimizer is guessing. Run `ANALYZE TABLE table_name`.
- Cardinality wildly off from actual row counts (visible with `EXPLAIN ANALYZE`): statistics are stale after bulk loads. Run `ANALYZE TABLE table_name SAMPLE ROWS 5000000`.
- Cardinality underestimated on join output: NDV (number of distinct values) for join keys is wrong. Collect full statistics: `ANALYZE TABLE table_name WITH SYNC MODE`.

**Fixing stale statistics:**
```sql
-- Full synchronous analyze (blocks until done)
ANALYZE TABLE orders WITH SYNC MODE;

-- Sample-based analyze (faster for very large tables)
ANALYZE TABLE orders SAMPLE ROWS 5000000;

-- Collect histogram for skewed column
ANALYZE TABLE orders UPDATE HISTOGRAM ON region;

-- Check existing statistics
SHOW STATS META WHERE `table` = 'orders';
```

---

## Identifying Shuffle Cost

Every `HASH_PARTITIONED` ExchangeNode moves all rows matching its hash bucket across the network. In a multi-node cluster this is a serialization + network + deserialization cycle.

**Detection in EXPLAIN:**
```
EXCHANGE
   HASH_PARTITIONED: orders.customer_id
   cardinality=50000000      -- 50 M rows are being shuffled
```

**Remediation options:**

1. **Colocate join**: if both tables are distributed by `customer_id` with the same bucket count, StarRocks skips the exchange entirely.
   - Verify with `SHOW CREATE TABLE` — both tables must have `DISTRIBUTED BY HASH(customer_id) BUCKETS N` with the same N.
   - After verifying, re-check EXPLAIN for `colocate: true`.

2. **Broadcast join**: if one side is small (< tens of millions of rows), hint broadcast:
   ```sql
   SELECT /*+ JOIN_HINT(BROADCAST) */ o.*, c.name
   FROM orders o JOIN customers c ON o.customer_id = c.customer_id;
   ```

3. **Bucket shuffle join**: if the probe side matches the bucket key but bucket counts differ, bucket shuffle still avoids a full repartition of the probe side.

4. **Pre-aggregate before join**: reduce the large side with a CTE aggregation before joining, lowering the shuffle cardinality.

---

## Partition Pruning Verification

Partition pruning is visible in the `OlapScanNode` output:

```
OlapScanNode
   TABLE: orders
   partitions=3/365          -- only 3 of 365 date partitions scanned
   tabletRatio=96/11680
```

A good plan shows a small fraction of partitions. If you see `partitions=365/365` on a query that filters `order_date`, pruning failed.

**Common reasons pruning fails:**

| Cause | Example | Fix |
|---|---|---|
| Function wrapping on partition column | `WHERE DATE_FORMAT(order_date, '%Y-%m') = '2026-01'` | Rewrite to range: `order_date BETWEEN '2026-01-01' AND '2026-01-31'` |
| Implicit type cast | Partition column is `DATE`, filter is a string with time: `'2026-01-01 00:00:00'` | Use typed literal: `DATE '2026-01-01'` |
| OR on non-partition column combined with partition column | Complex OR conditions | Simplify or use IN list on partition column |
| Dynamic partition not yet created | Future partition referenced | Check `SHOW PARTITIONS FROM table` |
| Expression partition with complex expression | Pruning only works on simple expressions | Use `PARTITION BY RANGE(col)` with explicit ranges |

**Verifying after fix:**
Run `EXPLAIN COSTS` again and confirm `partitions=N/365` where N is the expected number of partitions for the date range.

---

## Runtime Filter in EXPLAIN VERBOSE

Runtime filters (RF) are bloom filters or in-list filters generated on the build side of a HashJoin and pushed down to OlapScanNode on the probe side to reduce scan rows early.

### Reading Runtime Filters in the Plan

```sql
EXPLAIN VERBOSE
SELECT o.order_id, o.amount, c.customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE c.country = 'US';
```

```
PLAN FRAGMENT 0
  HashJoinNode
    join op: INNER JOIN (BROADCAST)
    build runtime filters:
      - filter_id = RF000, build_expr = c.customer_id, remote = false

  EXCHANGE (BROADCAST)
    |
    OlapScanNode                   -- build side: customers
       TABLE: customers
       PREDICATES: country = 'US'
       cardinality=50000

PLAN FRAGMENT 1
  OlapScanNode                     -- probe side: orders
     TABLE: orders
     partitions=365/365
     RuntimeFilters: RF000[customer_id] <- RF000   -- RF applied here
     cardinality=10000000
```

**What to verify:**
- `filter_id = RF000` in `HashJoinNode` build section matches `RF000` in the probe-side `OlapScanNode`.
- `remote = false`: the filter is applied locally on each BE. `remote = true` means the filter must be sent across the network first (slightly more latency, but still beneficial).
- If `RuntimeFilters` is absent in OlapScanNode, the filter was not pushed down. Common causes:
  - Join key type mismatch (INT vs BIGINT — fix with explicit CAST in DDL).
  - `enable_runtime_filter_pipeline = false` session variable.
  - Cardinality of build side too large — StarRocks disables RF if build side estimated > `runtime_filter_on_exchange_node_threshold`.

**Forcing runtime filter:**
```sql
SET enable_runtime_filter_pipeline = true;
SET runtime_filter_wait_time_ms = 1000;
```

---

## MV Query Rewrite Verification

When a synchronous or asynchronous MV covers the query, the planner rewrites OlapScanNode to read from the MV instead of the base table.

**Before rewrite:**
```
OlapScanNode
   TABLE: orders           -- scanning full base table
   rollup: orders
   partitions=90/365
   tabletRatio=2880/11680
```

**After rewrite (synchronous MV):**
```
OlapScanNode
   TABLE: orders
   rollup: mv_orders_region_daily   -- reading the rollup MV
   partitions=90/365
   tabletRatio=2880/11680
   PREAGGREGATION: ON
```

**After rewrite (asynchronous MV):**
```
OlapScanNode
   TABLE: mv_orders_region_daily    -- TABLE points to the MV itself
   partitions=3/90
   tabletRatio=96/2880
```

**Confirming rewrite happened:**
- The `rollup` field changes from the base table name to the MV name (synchronous MV).
- The `TABLE` field points to the MV table directly (asynchronous MV).
- Row count (`cardinality`) drops dramatically compared to the base table scan.

**When rewrite does not happen:**
```sql
-- Check if MV is valid and active
SHOW MATERIALIZED VIEWS WHERE name = 'mv_orders_region_daily';

-- Force rewrite attempt and see reason
SET enable_materialized_view_rewrite = true;
SET materialized_view_rewrite_mode = 'DEFAULT';

-- Use hint to force a specific MV
SELECT /*+ USE_MV(mv_orders_region_daily) */ region, SUM(amount)
FROM orders
GROUP BY region;
```

Common reasons rewrite fails:
- MV is in `INACTIVE` state (base table DDL changed, refresh failed).
- Query uses columns not in the MV output.
- Query filter does not match MV's partition range (for partition-aware async MV).
- `enable_materialized_view_rewrite = false` at session or global level.

---

## Query Profile (SHOW PROFILELIST and EXPLAIN ANALYZE)

### SHOW PROFILELIST
Lists recent query profiles stored on the FE. Profiles are retained based on `profile_timeout_s`.

```sql
SHOW PROFILELIST;
-- Returns: QueryId, StartTime, TotalTime, State, Statement (truncated)

-- Get a specific profile by QueryId
SHOW PROFILE FOR 'a1b2c3d4-...';
```

Use `SHOW PROFILELIST` to:
- Find a recently slow query by start time and duration.
- Retrieve the QueryId for deeper inspection.

### EXPLAIN ANALYZE
Executes the query and returns the plan annotated with actual runtime metrics. The output format interleaves the plan tree with per-operator metrics.

```sql
EXPLAIN ANALYZE
SELECT region, SUM(amount)
FROM orders
WHERE order_date BETWEEN '2026-01-01' AND '2026-03-31'
GROUP BY region;
```

**Sample annotated output:**
```
PLAN FRAGMENT 1
  3: AGGREGATE (update serialize)
     - OperatorTotalTime: 1.234s
     - PullRowNum: 150000       -- rows this operator consumed
     - PushRowNum: 42           -- rows this operator produced (42 distinct regions)
     - PeakMemoryBytes: 8388608 -- 8 MB peak
  |
  2: OlapScanNode
     TABLE: orders
     partitions=90/365
     - OperatorTotalTime: 8.512s   -- scan is the bottleneck
     - PullRowNum: 0               -- leaf node, no input
     - PushRowNum: 150000          -- rows scanned and passed up
     - PeakMemoryBytes: 67108864   -- 64 MB for scan buffer
     - BytesRead: 1073741824       -- 1 GB of data read from disk
```

### Key Metrics to Read

| Metric | What It Tells You |
|---|---|
| `OperatorTotalTime` | Wall-clock time spent in this operator across all pipeline threads |
| `PullRowNum` | Rows this operator read from its child operator |
| `PushRowNum` | Rows this operator passed to its parent |
| `PeakMemoryBytes` | Maximum memory used by this operator |
| `BytesRead` | Bytes read from disk (OlapScanNode only) |
| `RuntimeFilterEffectRows` | Rows filtered by runtime filter at scan time |
| `SpillBytes` | Bytes spilled to disk (if operator spilled; indicates memory pressure) |

### Identifying the Slowest Operator
1. Compare `OperatorTotalTime` across all operators.
2. The operator with the highest time is the primary bottleneck.
3. Cross-reference `PullRowNum` vs `PushRowNum` — a large reduction means the operator is doing heavy work (filter, agg) or the selectivity is high (good). A near-equal ratio with high time means it is a processing bottleneck (sort, join).
4. If `SpillBytes > 0`, the query is hitting memory limits. Increase `query_mem_limit` or reduce parallelism.

---

## Annotated EXPLAIN Examples

### Example 1: Simple Aggregation with Partition Pruning and Pre-Aggregation

**Query:**
```sql
EXPLAIN COSTS
SELECT region, SUM(amount) AS revenue
FROM orders
WHERE order_date BETWEEN '2026-01-01' AND '2026-03-31'
GROUP BY region;
```

**Plan output (annotated):**
```
PLAN FRAGMENT 0  -- coordinator: final aggregation and return
  OUTPUT EXPRS: region, revenue
  PARTITION: UNPARTITIONED

  4: AGGREGATE (merge finalize)        -- [1] second-phase agg: merges partial results
     group by: region
     cardinality=42                    -- [2] 42 distinct regions: tiny output

  3: EXCHANGE                          -- [3] HASH_PARTITIONED on region: minimal shuffle (42 keys)
     HASH_PARTITIONED: region
     cardinality=42

PLAN FRAGMENT 1  -- workers: scan + first-phase agg
  PARTITION: HASH_PARTITIONED: region

  2: AGGREGATE (update serialize)      -- [4] first-phase pre-agg: collapses rows per region per BE
     group by: region
     cardinality=42

  1: OlapScanNode                      -- [5] THE KEY NODE: verify partition pruning
     TABLE: orders
     PREAGGREGATION: ON               -- [6] Aggregate Key model: SUM applied during scan
     PREDICATES: order_date >= '2026-01-01', order_date <= '2026-03-31'
     partitions=90/365                -- [7] GOOD: only 90 of 365 partitions scanned
     rollup: mv_orders_region_daily   -- [8] GOOD: synchronous MV used for pre-agg
     tabletRatio=2880/11680
     cardinality=150000
     avgRowSize=28.0
```

**Findings:**
- [7] Partition pruning reduced scan from 365 to 90 partitions (Jan–Mar).
- [8] Synchronous MV `mv_orders_region_daily` was selected — scan reads pre-aggregated data.
- [6] `PREAGGREGATION: ON` confirms Aggregate Key model is collapsing data at scan time.
- [3] Exchange is HASH_PARTITIONED on `region` with only 42 distinct values — negligible shuffle cost.

---

### Example 2: Two-Table Join with BROADCAST and Runtime Filter

**Query:**
```sql
EXPLAIN VERBOSE
SELECT o.order_id, o.amount, c.customer_name, c.country
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE c.country = 'US'
  AND o.order_date >= '2026-03-01';
```

**Plan output (annotated):**
```
PLAN FRAGMENT 0
  OUTPUT EXPRS: order_id, amount, customer_name, country
  PARTITION: UNPARTITIONED

  5: EXCHANGE (GATHER)

PLAN FRAGMENT 1
  PARTITION: RANDOM

  4: HashJoinNode                            -- [1] BROADCAST join: customers is small
     join op: INNER JOIN (BROADCAST)
     colocate: false, reason: tables not bucketed on join key
     equal join conjunct: o.customer_id = c.customer_id
     output columns: [order_id, amount, customer_name, country]
     build runtime filters:
       - filter_id = RF000, build_expr = c.customer_id, remote = false  -- [2] RF built from customers

  |--3: EXCHANGE (BROADCAST)               -- [3] full customers table broadcast to all BEs
  |     cardinality=125000                 -- [4] 125K US customers: small, broadcast is fine
  |
  |     2: OlapScanNode
  |          TABLE: customers
  |          PREDICATES: country = 'US'    -- [5] country filter pushed to scan
  |          partitions=1/1               -- [6] customers is unpartitioned
  |          cardinality=125000
  |          avgRowSize=42.0

  1: OlapScanNode                           -- [7] probe side: orders
     TABLE: orders
     PREDICATES: order_date >= '2026-03-01'
     partitions=31/365                     -- [8] GOOD: only March scanned
     tabletRatio=992/11680
     cardinality=4200000
     avgRowSize=24.0
     RuntimeFilters: RF000[customer_id] <- RF000  -- [9] RF applied: skip orders with no US customer
```

**Findings:**
- [1] BROADCAST join: `customers` (125K US rows) is small enough to broadcast to all BEs. No shuffle of orders.
- [2] Runtime filter RF000 is built from `customers.customer_id` and pushed to probe-side scan.
- [9] RF000 applied at OlapScanNode for `orders` — orders not matching any US customer are skipped at scan time.
- [8] Partition pruning limits orders scan to March (31 partitions).
- If `customers` were much larger (millions), you would see `PARTITIONED` instead of `BROADCAST` and an extra shuffle exchange.

---

### Example 3: Complex BI Query with MV Rewrite, Runtime Filter, and Partition Pruning

**Query:**
```sql
EXPLAIN VERBOSE
SELECT
    d.quarter,
    p.category,
    SUM(f.revenue)    AS total_revenue,
    COUNT(DISTINCT f.customer_id) AS unique_customers
FROM fact_sales f
JOIN dim_date d    ON f.sale_date = d.date_key
JOIN dim_product p ON f.product_id = p.product_id
WHERE d.year = 2026
  AND p.category IN ('Electronics', 'Appliances')
GROUP BY d.quarter, p.category
ORDER BY total_revenue DESC
LIMIT 20;
```

**Plan output (annotated):**
```
PLAN FRAGMENT 0
  OUTPUT EXPRS: quarter, category, total_revenue, unique_customers
  PARTITION: UNPARTITIONED

  10: TOP-N                                       -- [1] TopN: only top 20 kept; no full sort
      order by: total_revenue DESC
      limit: 20
      TopN: true

  9: AGGREGATE (merge finalize)
     group by: quarter, category
     cardinality=8

  8: EXCHANGE HASH_PARTITIONED: quarter, category  -- [2] shuffle on 8 groups: trivial

PLAN FRAGMENT 1
  PARTITION: HASH_PARTITIONED: quarter, category

  7: AGGREGATE (update serialize)
     group by: quarter, category
     cardinality=8

  6: HashJoinNode                                  -- [3] dim_product join
     join op: INNER JOIN (BROADCAST)
     build runtime filters:
       - filter_id = RF001, build_expr = p.product_id, remote = false
     cardinality=8500000

  |--5: OlapScanNode
  |     TABLE: dim_product
  |     PREDICATES: category IN ('Electronics', 'Appliances')  -- [4] filter pushed
  |     partitions=1/1
  |     cardinality=1200

  4: HashJoinNode                                  -- [5] dim_date join
     join op: INNER JOIN (BROADCAST)
     build runtime filters:
       - filter_id = RF000, build_expr = d.date_key, remote = false
     cardinality=85000000

  |--3: OlapScanNode
  |     TABLE: dim_date
  |     PREDICATES: year = 2026                    -- [6] year filter pushed
  |     partitions=1/1
  |     cardinality=365

  2: OlapScanNode                                  -- [7] fact table: verify MV + pruning + RFs
     TABLE: fact_sales
     rollup: mv_fact_sales_quarter_category        -- [8] GOOD: synchronous MV rewrite
     PREAGGREGATION: ON
     partitions=365/1826                           -- [9] GOOD: 2026 only (365 of 5 years)
     tabletRatio=11680/58400
     cardinality=1500000
     avgRowSize=32.0
     RuntimeFilters:
       RF000[sale_date] <- RF000                   -- [10] date filter pushed from dim_date
       RF001[product_id] <- RF001                  -- [11] product filter pushed from dim_product
```

**Findings:**
- [8] MV `mv_fact_sales_quarter_category` rewrites the fact scan — reads pre-aggregated rollup instead of raw rows.
- [9] Partition pruning restricts fact_sales to 2026 (365 of 1826 days across 5 years of data).
- [10–11] Two runtime filters applied at the fact scan: one from the date dimension (year=2026), one from products (Electronics/Appliances). These further reduce scan rows before aggregation.
- [1] TopN optimization: the SortNode is a bounded heap keeping only 20 rows; no full sort materialization.
- [2] Only 8 distinct (quarter, category) combinations: the shuffle on 8 groups is negligible.

---

## EXPLAIN Checklist

Run through this checklist on every slow query EXPLAIN:

1. **Partition pruning**: check `partitions=X/Y` in every OlapScanNode. If X equals Y and the table is large, add or fix the partition filter.
2. **MV rewrite**: check `rollup:` or `TABLE:` in OlapScanNode. If pointing to base table when an MV exists, investigate rewrite failure.
3. **PREAGGREGATION**: confirm `PREAGGREGATION: ON` for Aggregate Key tables when aggregating. OFF means the model is not helping.
4. **Runtime filters**: verify `RuntimeFilters:` section in probe-side OlapScanNode. If missing, the filter was not pushed — check key type compatibility and session variables.
5. **Join strategy**: check `(BROADCAST)`, `(PARTITIONED)`, or `(COLOCATE)` in HashJoinNode. Prefer COLOCATE > BROADCAST > PARTITIONED. If PARTITIONED is used for a large fact join, consider co-location or distribution key alignment.
6. **Shuffle cardinality**: check `cardinality` on every EXCHANGE node with HASH_PARTITIONED. Large cardinality + HASH_PARTITIONED = expensive shuffle.
7. **Cardinality estimates**: run `EXPLAIN COSTS` and scan all `cardinality=` values. Values of `1` or `-1` indicate missing statistics — run `ANALYZE TABLE`.
8. **Build side size**: in HashJoinNode, confirm the build side has lower cardinality than the probe side. If reversed, update statistics or add a JOIN_ORDER hint.
9. **TopN optimization**: if the query has `ORDER BY ... LIMIT N`, verify SortNode shows `TopN: true`. If absent, the optimizer may sort millions of rows unnecessarily.
10. **EXPLAIN ANALYZE for hotspot**: if the logical plan looks correct, run `EXPLAIN ANALYZE` to find the actual slow operator by `OperatorTotalTime`, and check `SpillBytes` for memory pressure.

---

## Anti-Patterns

**Ignoring cardinality mismatches**
Running `EXPLAIN COSTS` and seeing `cardinality=1` on large tables without updating statistics. The optimizer makes poor join order, join strategy, and aggregation decisions when cardinalities are wrong. Always run `ANALYZE TABLE` after bulk loads.

```sql
-- Wrong: assume statistics are current
EXPLAIN COSTS SELECT ...;

-- Right: check and refresh statistics first
SHOW STATS META WHERE `table` = 'orders';
ANALYZE TABLE orders WITH SYNC MODE;
EXPLAIN COSTS SELECT ...;
```

**Not checking partition count in OlapScanNode**
Looking at execution time without verifying `partitions=X/Y`. A query scanning 365 partitions instead of 3 is 100x more expensive by design — no amount of join tuning will fix an absent partition filter.

**Assuming runtime filter was applied without checking**
Seeing a HashJoinNode build filter entry and assuming it applies at the scan. Always verify the probe-side OlapScanNode contains `RuntimeFilters: RF00X[col]`. The filter may have been disabled due to type mismatch, cardinality threshold, or session variables.

**Using function-wrapped partition columns in WHERE**
```sql
-- Breaks partition pruning:
WHERE YEAR(order_date) = 2026

-- Correct:
WHERE order_date BETWEEN '2026-01-01' AND '2026-12-31'
```

**Confusing EXPLAIN output with actual execution**
`EXPLAIN` (without ANALYZE) shows estimated rows and cost, not actual values. A plan that looks cheap with bad statistics can be slow in practice. Use `EXPLAIN ANALYZE` to see actual `PullRowNum` and `OperatorTotalTime`.

**Not checking `colocate: false, reason:`**
When expecting a colocate join but not getting it, the reason field in HashJoinNode explains exactly why. Common reasons: different bucket counts, different distribution key types, tables in different databases. Fixing the reason eliminates the shuffle entirely.

**Ignoring `SpillBytes` in EXPLAIN ANALYZE**
Non-zero `SpillBytes` on HashJoinNode or AggregationNode means the operator ran out of memory and spilled to disk. This adds significant latency. Fix by increasing `query_mem_limit`, reducing parallelism (`set_var pipeline_dop`), or pre-filtering the build side.

---

## References

- StarRocks 3.x documentation: Query Profile and EXPLAIN — [docs.starrocks.io](https://docs.starrocks.io/docs/administration/query_profile_overview/)
- StarRocks 3.x EXPLAIN ANALYZE syntax — [docs.starrocks.io](https://docs.starrocks.io/docs/sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN_ANALYZE/)
- Query optimizer hints (`JOIN_HINT`, `USE_MV`) — [docs.starrocks.io](https://docs.starrocks.io/docs/sql-reference/sql-hints/)
- Runtime filter configuration — [docs.starrocks.io](https://docs.starrocks.io/docs/using_starrocks/runtime_filter/)
- `ANALYZE TABLE` statistics collection — [docs.starrocks.io](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/)
- Cross-skill reference: `starrocks-materialized-views` skill for MV creation and rewrite configuration
- Cross-skill reference: `starrocks-query-optimizer` skill for CBO join order hints and statistics tuning
- Cross-skill reference: `starrocks-partitioning` skill for partition strategy design affecting pruning
