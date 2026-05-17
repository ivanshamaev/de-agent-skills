---
name: starrocks-join-optimization
description: StarRocks join optimization — broadcast join (small dimension to all BEs), shuffle/hash join (repartition both sides), colocate join (no network transfer, same distribution key), bucket shuffle join, nested loop join (cross join/non-equi), join strategy selection rules, runtime filter effect on joins, join hints, EXPLAIN join strategy verification, skew join handling
---

# StarRocks Join Optimization

## When to Use

Load this skill when the user needs to:
- Diagnose slow join queries in StarRocks (high network traffic, OOM, skewed execution)
- Choose or force a join strategy (broadcast, shuffle, colocate, bucket shuffle)
- Set up colocate join groups to eliminate network shuffle between frequently joined tables
- Understand and tune runtime filters for join probe-side scan reduction
- Read and interpret EXPLAIN output to verify join strategy selection
- Handle data skew in join keys causing uneven BE workloads
- Control join order and join reorder via hints or session variables
- Troubleshoot wrong strategy caused by stale or missing table statistics

---

## Join Strategies Overview

| Strategy | When StarRocks Uses It | Network Cost | Memory Requirement | EXPLAIN Keyword |
|---|---|---|---|---|
| **Broadcast** | Build side row count < `broadcast_row_limit` | Low — build side sent once to all BEs | Must fit build side in each BE memory | `BROADCAST` in HashJoinNode |
| **Shuffle (Hash Partition)** | Both tables large, no colocation or bucket match | High — both sides repartitioned by hash | Distributed across all BEs | `PARTITIONED` in ExchangeNode |
| **Colocate** | Both tables in same colocate group, same distribution key and bucket count | Zero — no data movement | Minimal — local join only | `COLOCATE` in HashJoinNode |
| **Bucket Shuffle** | Join key matches distribution key of one side | Low — only one side redistributed | Moderate — one side local, other shuffled to match | `BUCKET_SHUFFLE` in ExchangeNode |
| **Nested Loop** | Non-equi predicate, CROSS JOIN, cartesian product | Very high if large | Very high — O(N*M) | `NESTLOOP` in NestLoopJoinNode |

---

## Broadcast Join

### How It Works

The build side (smaller table) is fully replicated to every BE. Each BE then probes the local copy using its local fragment of the probe side. No probe-side rows cross the network.

```
FE plan:
  probe side (large fact) → each BE scans local tablets
  build side (small dim)  → broadcast to ALL BEs
  each BE: hash_join(local_probe, in-memory_build)
```

### Threshold: `broadcast_row_limit`

The optimizer broadcasts the build side only when its estimated row count is below the session variable `broadcast_row_limit` (default **15,000,000 rows**). This is a row count limit, not a byte limit — set it based on expected row width.

```sql
-- Lower threshold to force shuffle for moderately sized dimension tables
SET broadcast_row_limit = 5000000;

-- Raise threshold when small dimension tables exceed default row count
-- but still fit in BE memory (e.g., wide dimension with few columns selected)
SET broadcast_row_limit = 30000000;
```

The optimizer also respects `max_broadcast_row_num` as an absolute safety cap.

### EXPLAIN Identification

```sql
EXPLAIN SELECT f.order_id, d.region
FROM   orders f
JOIN   regions d ON f.region_id = d.region_id;
```

Look for `BROADCAST` in the join node:

```
4:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  build runtime filters:
|  - filter_id = 0, build_expr = (d.region_id), remote = false
|  hash predicates:
|  - f.region_id = d.region_id
|  ...
3:EXCHANGE
   distribution type: BROADCAST
```

### Best Practices

- Ideal for dimension tables under **100 MB** build side (after projection/filter pushdown)
- Confirm row count estimate is accurate: run `ANALYZE TABLE regions` if stats are stale
- If BE memory is constrained, lower `broadcast_row_limit` to prevent OOM

### Hint Syntax

```sql
SELECT /*+ JOIN(regions BROADCAST) */
       f.order_id, d.region
FROM   orders f
JOIN   regions d ON f.region_id = d.region_id;
```

The hint table name refers to the build-side table. The hint overrides the optimizer's strategy; use it only when you are certain about table sizes.

---

## Shuffle (Hash Partition) Join

### How It Works

Both the build side and the probe side are repartitioned across all BEs using the same hash function on the join key. After shuffling, each BE performs a local hash join on its slice of both inputs.

```
FE plan:
  build side → HASH partition by join_key → each BE receives 1/N of rows
  probe side → HASH partition by join_key → each BE receives matching 1/N of rows
  each BE: local hash_join
```

### Network Cost

Shuffle is the most expensive strategy in terms of network I/O because **both sides** cross the network. For a 1 TB fact-to-fact join, expect significant cluster-internal traffic. Ensure your network fabric is not the bottleneck.

### EXPLAIN Identification

```sql
EXPLAIN SELECT a.user_id, b.event_type, SUM(a.revenue)
FROM   daily_sales a
JOIN   user_events b ON a.user_id = b.user_id
GROUP BY 1, 2;
```

Look for `PARTITIONED` ExchangeNode on both sides:

```
5:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  ...
4:EXCHANGE
|  distribution type: SHUFFLE
|  partition exprs: [b.user_id]
...
2:EXCHANGE
   distribution type: SHUFFLE
   partition exprs: [a.user_id]
```

### Best Practices

- Accepted fallback when neither colocate nor bucket shuffle applies
- If used repeatedly for the same table pair, consider promoting to colocate group
- Ensure join key columns have high cardinality to avoid post-shuffle skew
- For very large inputs, enable `enable_spill = true` to spill hash tables to disk and avoid OOM

### Hint Syntax

```sql
SELECT /*+ JOIN(daily_sales SHUFFLE) */
       a.user_id, b.event_type
FROM   daily_sales a
JOIN   user_events b ON a.user_id = b.user_id;
```

---

## Colocate Join

### How It Works

When two tables belong to the same colocate group and share the same distribution key, bucket count, and replica count, rows with the same join key reside in the same tablet on the same BE. The FE generates a plan where each BE joins its local tablets — **zero network transfer**.

```
BE_1: tablet_0(orders) JOIN tablet_0(order_items) → local hash join
BE_2: tablet_1(orders) JOIN tablet_1(order_items) → local hash join
...  no data crosses BEs
```

### Setup

Both tables must be created with the same `colocate_with` group name, the same distribution key column(s), and the same bucket count:

```sql
CREATE TABLE orders (
    order_id   BIGINT   NOT NULL,
    user_id    BIGINT   NOT NULL,
    amount     DECIMAL(18, 2),
    created_at DATETIME
)
ENGINE = OLAP
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "3",
    "colocate_with"   = "order_group"
);

CREATE TABLE order_items (
    order_id    BIGINT   NOT NULL,
    item_id     BIGINT   NOT NULL,
    sku_id      BIGINT,
    quantity    INT,
    unit_price  DECIMAL(18, 2)
)
ENGINE = OLAP
DUPLICATE KEY(order_id, item_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32   -- same key, same bucket count
PROPERTIES (
    "replication_num" = "3",
    "colocate_with"   = "order_group"      -- same group
);
```

Key requirements (all three must match exactly):

| Requirement | Detail |
|---|---|
| `colocate_with` value | Identical string in both tables' PROPERTIES |
| Distribution key | Same column(s) in same order |
| Bucket count | Identical `BUCKETS N` |
| Replica count | Identical `replication_num` |

### Verifying Group Status

```sql
SHOW PROC '/colocation_group';
```

Example output:

```
+------------------+--------------------+-----------+---------+-----------+
| GroupId          | TableIds           | BucketsNum| ReplicaNum| IsStable |
+------------------+--------------------+-----------+---------+-----------+
| 10001.order_group| 10002, 10003       | 32        | 3         | true     |
+------------------+--------------------+-----------+---------+-----------+
```

`IsStable = true` is required for colocate join to be used. If `IsStable = false`, StarRocks falls back to shuffle. Instability happens during BE failure, tablet migration, or bucket count mismatch after ALTER TABLE.

### Session Variable

```sql
SET enable_colocate_join = true;   -- default is true; set false only for debugging
```

### EXPLAIN Identification

```sql
EXPLAIN SELECT o.order_id, oi.sku_id, oi.quantity
FROM   orders o
JOIN   order_items oi ON o.user_id = oi.user_id;
```

```
3:HASH JOIN
|  join op: INNER JOIN (COLOCATE)
|  hash predicates:
|  - o.user_id = oi.user_id
|  ...
2:OlapScanNode  -- orders tablets (local)
1:OlapScanNode  -- order_items tablets (local)
-- NOTE: no ExchangeNode between scan and join
```

The absence of an ExchangeNode between the OlapScanNodes and the HashJoinNode is the definitive indicator of colocate execution.

### Altering Bucket Count (Breaks Colocation Temporarily)

```sql
ALTER TABLE orders
    DISTRIBUTED BY HASH(user_id) BUCKETS 64;
-- Must also alter order_items to the same bucket count
-- Group becomes IsStable=false until both tables match and tablets are rebalanced
ALTER TABLE order_items
    DISTRIBUTED BY HASH(user_id) BUCKETS 64;
```

After both ALTERs, wait for `SHOW PROC '/colocation_group'` to show `IsStable = true` before expecting colocate joins.

---

## Bucket Shuffle Join

### How It Works

Bucket shuffle is a middle-ground strategy between colocate (zero network) and full shuffle (both sides moved). When the join key matches the distribution key of one table, that table's data is already partitioned by the join key. Only the **other side** is repartitioned to match the existing bucket layout.

```
probe side (orders, HASH(user_id)) → already distributed by user_id → no movement
build side (sessions)              → redistribute to match orders' bucket layout
each BE: local hash join on its slice
```

### When It Activates

StarRocks activates bucket shuffle automatically when:
1. The equi-join predicate includes the full distribution key of at least one table
2. The tables are not in the same colocate group (otherwise colocate wins)
3. The non-distribution-key side's estimated size makes full broadcast too large

### EXPLAIN Identification

```sql
EXPLAIN SELECT o.order_id, s.session_duration
FROM   orders o                          -- DISTRIBUTED BY HASH(user_id)
JOIN   sessions s ON o.user_id = s.user_id;
```

```
4:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  ...
3:EXCHANGE
|  distribution type: BUCKET_SHUFFLE
|  partition exprs: [s.user_id]
|  -- sessions side is redistributed to match orders' bucket layout
2:OlapScanNode  -- orders scanned locally, no exchange needed
```

### Limitations

- Bucket shuffle requires the join key to be exactly the distribution key (no subset, no extra columns)
- Does not apply across different bucket counts unless StarRocks can compute a compatible mapping
- Falls back to shuffle if bucket mapping is incompatible

---

## Nested Loop Join (Cross Join / Non-Equi)

### How It Works

For queries without equi-join predicates — including `CROSS JOIN`, range predicates (`a.val BETWEEN b.lo AND b.hi`), and inequality predicates (`a.ts > b.start_ts`) — StarRocks uses a nested loop join. Every row of the outer (probe) side is compared against every row of the inner (build) side: O(N * M) complexity.

### EXPLAIN Identification

```sql
EXPLAIN SELECT a.event_id, b.campaign_id
FROM   events a
CROSS JOIN campaigns b;
```

```
3:NESTLOOP JOIN
|  join op: CROSS JOIN
|  colocate: false, reason: cross join not support colocate
|  ...
2:OlapScanNode  -- campaigns (build, inner)
1:OlapScanNode  -- events (probe, outer)
```

### Danger Signals

Any `NESTLOOP JOIN` on tables with more than a few thousand rows is a potential performance catastrophe. Alert immediately if EXPLAIN shows NESTLOOP on large tables and investigate whether:

1. The join predicate can be rewritten as an equi-join (`a.region_id = b.region_id`) with the range filter moved to a WHERE clause
2. The non-equi condition can be approximated with a bucketed range approach
3. A CROSS JOIN is accidental (missing ON clause)

### Hint Syntax

```sql
-- Force nested loop (useful for tiny tables with complex predicates)
SELECT /*+ USE_NL(a b) */
       a.event_id, b.label
FROM   events a, event_labels b
WHERE  a.event_ts BETWEEN b.start_ts AND b.end_ts;
```

Only use `USE_NL` when the build side is guaranteed tiny (hundreds of rows).

---

## Join Strategy Selection Logic

The optimizer follows this decision flow for each equi-join:

```
1. COLOCATE CHECK
   Both tables in same colocate group?
   Group IsStable = true?
   enable_colocate_join = true?
     YES → use COLOCATE JOIN  (zero network cost)
     NO  → continue

2. BROADCAST SIZE CHECK
   Estimated build-side row count < broadcast_row_limit?
   Estimated build-side bytes < max_broadcast_row_num threshold?
     YES → use BROADCAST JOIN  (build side sent to all BEs)
     NO  → continue

3. BUCKET SHUFFLE CHECK
   Join key == distribution key of one table?
   Bucket mapping is compatible?
     YES → use BUCKET SHUFFLE JOIN  (one side redistributed)
     NO  → continue

4. SHUFFLE FALLBACK
   Use SHUFFLE (PARTITIONED) JOIN  (both sides repartitioned by hash)
```

The optimizer can override this flow when join hints are present. When statistics are stale, size estimates may be wrong and the optimizer may pick the wrong strategy — always run `ANALYZE TABLE` on tables involved in slow joins.

---

## Runtime Filters on Joins

### Concept

When the optimizer builds a hash table for the build side, it simultaneously generates a compact **runtime filter** (Bloom filter, IN list, or min/max range) from the build-side join key values. This filter is pushed down to the probe-side scan, allowing the storage layer to skip data blocks that cannot possibly match.

```
Build phase:  scan build table → build hash table → generate Bloom filter(join_key)
              → push filter to all probe-side scan nodes (possibly cross-network: "remote=true")
Probe phase:  scan probe table → apply Bloom filter per data block → skip non-matching blocks
              → probe surviving rows against hash table
```

The effect is a dramatic reduction in rows read from disk on the probe side, especially when the build side is highly selective.

### EXPLAIN VERBOSE Identification

Use `EXPLAIN VERBOSE` to see filter node IDs:

```sql
EXPLAIN VERBOSE SELECT f.order_id, d.region
FROM   orders f
JOIN   regions d ON f.region_id = d.region_id
WHERE  d.country = 'US';
```

```
4:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  build runtime filters:
|  - filter_id = 0, build_expr = (d.region_id), remote = false
|    type: BLOOM_FILTER
|  hash predicates:
|  - f.region_id = d.region_id
|  ...

2:OlapScanNode (orders)
   TABLE: orders
   runtime filters: 0[bloom] <- d.region_id
   -- filter_id=0 is applied here at scan time
```

The `runtime filters: 0[bloom] <- d.region_id` line confirms the filter is pushed down to the probe-side tablet scan. Rows that the Bloom filter eliminates are never read from disk.

### Session Variables for Runtime Filters

```sql
-- Master switch (default: true)
SET enable_runtime_filter = true;

-- Filter types: BLOOM_FILTER, IN, MIN_MAX, IN_OR_BLOOM (default: IN_OR_BLOOM)
SET runtime_filter_type = 'IN_OR_BLOOM';

-- Maximum IN-list size before switching to Bloom filter (default: 1024)
SET runtime_filter_max_in_num = 1024;

-- Bloom filter size in bytes (0 = auto-size based on NDV estimate)
SET runtime_bloom_filter_size = 0;

-- Minimum row count threshold to generate a runtime filter
SET runtime_filter_scan_wait_time = 20000;  -- ms probe scan waits for filter
```

### Filter Types Guidance

| Type | Best For | Limitation |
|---|---|---|
| `IN` | Low NDV build side (< 1024 distinct values) | Does not scale to high-NDV keys |
| `BLOOM_FILTER` | High-NDV equi-join keys (user_id, order_id) | Small false positive rate |
| `MIN_MAX` | Range joins or monotonic keys (timestamps) | Only effective for clustered probe data |
| `IN_OR_BLOOM` | General purpose — auto-selects based on NDV | Default; rarely needs to be changed |

### Cross-Exchange (Remote) Filters

For shuffle joins, the runtime filter must cross the ExchangeNode to reach the probe-side scan. EXPLAIN shows `remote = true` for such filters. Remote filters add a small synchronization overhead but are generally worth it for selective build sides.

---

## Join Skew Handling

### Detecting Skew

In a uniform shuffle join, each BE processes roughly the same number of rows. Skew occurs when one or more join key values are vastly over-represented (e.g., `user_id = -1` representing anonymous users). Symptoms:

- Query profile shows some BE instances finishing 10x faster than others
- One or two fragment instances have operator input/output row counts orders of magnitude larger
- OOM or spill only on specific BE nodes

Access the query profile via the StarRocks WebUI (`http://<fe-host>:8030`) → **Query History** → select query → **Profile**.

In the profile, look for:
```
HASH_JOIN_NODE (id=4)
  - RowsReturned: 980,000,000   ← one instance
  - RowsReturned: 1,200,000     ← typical instance
```

### Salt Key Approach (Manual)

Manually salt the skewed join key by appending a random integer suffix `[0, N)` to the high-frequency values on the probe side and replicating the corresponding build-side rows N times.

```sql
-- Step 1: Identify skewed key values
SELECT user_id, COUNT(*) AS cnt
FROM   orders
GROUP BY user_id
ORDER BY cnt DESC
LIMIT 10;
-- Suppose user_id = -1 has 500M rows (skewed)

-- Step 2: Create salted version of the fact table (or CTE)
WITH salted_orders AS (
    SELECT
        order_id,
        CASE
            WHEN user_id = -1
                THEN CONCAT(CAST(user_id AS VARCHAR), '_', CAST(FLOOR(RAND() * 10) AS VARCHAR))
            ELSE CAST(user_id AS VARCHAR)
        END AS user_id_salt,
        amount,
        created_at
    FROM orders
),

-- Step 3: Explode the dimension table for the skewed key value
exploded_users AS (
    SELECT
        CONCAT(CAST(user_id AS VARCHAR), '_', CAST(salt AS VARCHAR)) AS user_id_salt,
        region,
        user_type
    FROM users
    CROSS JOIN (SELECT SEQUENCE(0, 9) AS arr) t
    LATERAL VIEW explode(arr) e AS salt
    WHERE user_id = -1

    UNION ALL

    SELECT CAST(user_id AS VARCHAR) AS user_id_salt, region, user_type
    FROM users
    WHERE user_id != -1
)

-- Step 4: Join on salted key
SELECT so.order_id, eu.region, eu.user_type, so.amount
FROM   salted_orders so
JOIN   exploded_users eu ON so.user_id_salt = eu.user_id_salt;
```

This distributes the skewed key's rows evenly across 10 hash buckets instead of piling them on one BE.

### Skew Join Hint (StarRocks 3.x)

StarRocks 3.x does not expose a declarative `SKEW_JOIN` hint as of the 3.3 release. Use the manual salting pattern above for production workloads. Monitor future releases for native skew join support.

### Preventing Skew at Design Time

- Avoid using sentinel values (NULL, -1, 0, 'UNKNOWN') as join keys; filter them out before joining
- Distribute tables by a composite key when a single column has a long-tail distribution
- For star-schema joins on skewed dimension keys, use broadcast join to eliminate hash-based skew entirely

---

## Multi-Table Join Order

### CBO Join Reorder

StarRocks Cost-Based Optimizer (CBO) automatically reorders joins to minimize intermediate result set sizes. The CBO considers:
- Table row counts and NDV from statistics (collected by `ANALYZE TABLE`)
- Filter selectivity
- Join type (inner/left outer/semi/anti)

The maximum number of tables the CBO will consider for reorder is controlled by:

```sql
SET cbo_max_reorder_node = 4;   -- default: 4, increase for complex multi-join queries
```

For queries with more than `cbo_max_reorder_node` tables, the CBO uses the original SQL join order beyond the threshold. Increase carefully — reorder search space is factorial.

### Enable/Disable Reorder

```sql
SET enable_join_reorder = true;   -- default: true
-- Disable only for debugging to isolate a regression introduced by reorder
SET enable_join_reorder = false;
```

### Leading Hint

Force a specific join order when you know the CBO will make a suboptimal choice (e.g., statistics are stale and cannot be refreshed):

```sql
SELECT /*+ LEADING(date_dim store_sales item_dim) */
       dd.d_year,
       ss.ss_quantity,
       id.i_brand
FROM   store_sales ss
JOIN   date_dim    dd ON ss.sold_date_sk = dd.d_date_sk
JOIN   item_dim    id ON ss.item_sk      = id.item_sk
WHERE  dd.d_year = 2024;
```

The LEADING hint specifies the left-to-right build order — StarRocks builds the join tree starting from the leftmost table in the hint. Combined with individual join strategy hints:

```sql
SELECT /*+ LEADING(date_dim store_sales item_dim) JOIN(date_dim BROADCAST) JOIN(item_dim BROADCAST) */
       ...
```

---

## EXPLAIN Verification Examples

### Example 1: Broadcast Join

```sql
EXPLAIN SELECT o.order_id, r.region_name
FROM   orders o
JOIN   regions r ON o.region_id = r.region_id;
```

Expected EXPLAIN (annotated):

```
PLAN FRAGMENT 0
 OUTPUT EXPRS: o.order_id | r.region_name
  PARTITION: UNPARTITIONED

  4:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)         ← broadcast strategy confirmed
  |  build runtime filters:
  |  - filter_id = 0, build_expr = (r.region_id), remote = false
  |  hash predicates:
  |  - o.region_id = r.region_id
  |  cardinality: 50000000
  |
  |----3:EXCHANGE                            ← build side (regions) broadcast
  |       distribution type: BROADCAST
  |       cardinality: 5000
  |
  2:OlapScanNode                             ← probe side (orders) scanned locally
     TABLE: orders
     PREDICATES: none
     runtime filters: 0[bloom] <- r.region_id  ← runtime filter applied at scan
     cardinality: 50000000
```

Key indicators: `BROADCAST` in HashJoinNode, single `EXCHANGE` node (build side only), `runtime filters` on probe scan.

### Example 2: Colocate Join

```sql
EXPLAIN SELECT o.order_id, oi.sku_id, oi.quantity
FROM   orders o
JOIN   order_items oi ON o.user_id = oi.user_id;
```

Expected EXPLAIN:

```
PLAN FRAGMENT 0
 OUTPUT EXPRS: o.order_id | oi.sku_id | oi.quantity
  PARTITION: UNPARTITIONED

  5:EXCHANGE

  PLAN FRAGMENT 1
   OUTPUT EXPRS:
    PARTITION: HASH_PARTITIONED: o.user_id

    4:HASH JOIN
    |  join op: INNER JOIN (COLOCATE)        ← colocate strategy confirmed
    |  colocate: true, reason: same group
    |  build runtime filters:
    |  - filter_id = 0, build_expr = (oi.user_id), remote = false
    |  hash predicates:
    |  - o.user_id = oi.user_id
    |  cardinality: 200000000
    |
    3:OlapScanNode                           ← build side (order_items) — local scan, no EXCHANGE
       TABLE: order_items
       cardinality: 100000000
    |
    2:OlapScanNode                           ← probe side (orders) — local scan, no EXCHANGE
       TABLE: orders
       runtime filters: 0[bloom] <- oi.user_id
       cardinality: 50000000
```

Key indicator: **no ExchangeNode between OlapScanNodes and HashJoinNode** and `COLOCATE` label. Both scans run on the same fragment inside each BE.

### Example 3: Runtime Filter Push-Down

```sql
EXPLAIN VERBOSE
SELECT f.order_id, d.city
FROM   orders f
JOIN   cities d ON f.city_id = d.city_id
WHERE  d.country_code = 'DE';
```

Expected EXPLAIN VERBOSE (partial):

```
  3:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)
  |  build runtime filters:
  |  - filter_id = 0, build_expr = (d.city_id), remote = false, type = IN_OR_BLOOM
  |  equal join conjunct: f.city_id = d.city_id
  |  output columns: 1, 2
  |
  |----2:EXCHANGE
  |       distribution type: BROADCAST
  |       cardinality: 412              ← 412 cities in DE after WHERE pushdown
  |
  1:OlapScanNode
     TABLE: orders
     PREDICATES: none
     runtime filters: 0[in_or_bloom] <- d.city_id   ← filter pushed to tablet scan
     cardinality: 50000000

     -- With filter applied, actual rows scanned:
     -- orders has 50M rows; ~2% match German cities → ~1M rows actually evaluated
```

The runtime filter for a highly selective build side (412 rows from cities) dramatically reduces rows read from the orders table. The `VERBOSE` output shows filter type and target expression.

---

## Anti-Patterns

### Missing Colocate Group

**Problem**: Two tables that are always joined on the same key are not in the same colocate group. Every join generates a full shuffle.

**Detection**: EXPLAIN shows `PARTITIONED` instead of `COLOCATE` for a high-frequency join between tables with matching distribution keys.

**Fix**: Re-create both tables with `"colocate_with" = "same_group_name"` and identical distribution key + bucket count. Plan for a one-time data reload.

---

### Broadcasting a Large Table

**Problem**: Statistics are stale or missing; the optimizer underestimates the build-side row count and chooses broadcast. A large table is replicated to every BE, causing OOM or extreme memory pressure.

**Detection**: Query fails with `Memory limit exceeded` or runs very slowly. `EXPLAIN` shows `BROADCAST` on a table that actually has hundreds of millions of rows.

**Fix**:
1. Run `ANALYZE TABLE large_table` to refresh statistics.
2. Lower `broadcast_row_limit` to prevent future accidental broadcasts.
3. If needed, use a hint to force shuffle: `/*+ JOIN(large_table SHUFFLE) */`.

---

### Non-Equi Join on Large Dataset

**Problem**: A join uses a non-equi predicate (e.g., range overlap, `!=`, `<`), triggering nested loop join on large tables.

**Detection**: EXPLAIN shows `NESTLOOP JOIN`. Profile shows very high RowsReturned on the NestLoopJoinNode instance.

**Fix**:
- Rewrite as equi-join + post-filter WHERE clause where possible.
- Pre-filter one side to a small subset before the nested loop.
- Use a range bucketing approach: bucket both sides by a range key, then join only within-bucket pairs using equi-join on the bucket column plus a post-filter.

---

### Stale Statistics Causing Wrong Strategy

**Problem**: The CBO selects broadcast when it should shuffle, or shuffles when colocate is available, because table row counts or NDV are outdated.

**Detection**:
```sql
SHOW TABLE STATUS FROM my_db LIKE 'orders';
-- Check Update_time vs last ANALYZE time
```

Or query the `information_schema`:
```sql
SELECT table_name, update_time, table_rows
FROM   information_schema.tables
WHERE  table_schema = 'my_db'
  AND  table_name IN ('orders', 'order_items');
```

**Fix**: Schedule regular `ANALYZE TABLE` runs after bulk loads. Use `ANALYZE TABLE t UPDATE HISTOGRAM ON col1, col2` for skewed columns. For very large tables use sample-based analysis:

```sql
ANALYZE TABLE orders WITH SAMPLE ROWS 1000000;
-- or percent-based
ANALYZE TABLE orders WITH SAMPLE PERCENT 10;
```

---

### Ignoring Colocate Group Instability

**Problem**: A colocate group becomes unstable (e.g., after a BE failure and tablet rebalancing) and the optimizer silently falls back to shuffle. Join performance degrades unnoticed.

**Detection**:
```sql
SHOW PROC '/colocation_group';
-- IsStable = false for one or more groups
```

**Fix**: Wait for tablet rebalancing to complete (monitor `SHOW PROC '/tablets_distribution'`). If a BE is permanently removed, update the colocate group's `replication_num` to match the new cluster size.

---

### Composite Distribution Key Partial Match

**Problem**: A table is distributed by `HASH(order_id, user_id)` (composite key). A join on only `user_id` cannot use colocate or bucket shuffle because the full distribution key is not present.

**Fix**: Choose distribution keys that are always used together in the most critical joins. If multiple join patterns are needed, prefer a single high-cardinality column as the distribution key rather than a composite.

---

## References

- StarRocks documentation: [Join Optimization](https://docs.starrocks.io/docs/using_starrocks/query_acceleration/join_optimization/)
- StarRocks documentation: [Colocate Join](https://docs.starrocks.io/docs/using_starrocks/colocate_join/)
- StarRocks documentation: [Runtime Filter](https://docs.starrocks.io/docs/using_starrocks/query_acceleration/runtime_filter/)
- StarRocks documentation: [CBO Optimizer](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/)
- StarRocks documentation: [Query Hints](https://docs.starrocks.io/docs/sql-reference/sql-statements/query_hints/)
- Related skill: `starrocks-bucketing` — distribution key selection, colocate group setup, tablet sizing
- Related skill: `starrocks-explain-plan` — reading full EXPLAIN output, fragment structure, exchange nodes
- Related skill: `starrocks-query-optimizer` — CBO statistics, ANALYZE TABLE, session variables
