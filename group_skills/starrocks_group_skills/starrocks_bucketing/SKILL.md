---
name: starrocks-bucketing
description: StarRocks bucketing (distribution) — DISTRIBUTED BY HASH vs RANDOM, bucket count selection formula, distribution key selection (high cardinality/join key), skew detection and prevention, colocate join setup (colocate_with property), bucket count modification (ALTER TABLE), auto bucket sizing (StarRocks 3.2+), tablet sizing target (1-10 GB)
---

# StarRocks Bucketing (Data Distribution)

## When to Use

Load this skill when the user needs to:
- Design a new StarRocks table and choose distribution method and bucket count
- Investigate tablet skew or uneven data distribution across BEs
- Set up colocate joins to eliminate shuffle for frequently joined tables
- Change the bucket count of an existing table after data growth
- Choose between `DISTRIBUTED BY HASH` and `DISTRIBUTED BY RANDOM`
- Understand the auto bucket sizing feature (StarRocks 3.2+)
- Diagnose join performance issues where EXPLAIN shows `SHUFFLE` instead of `COLOCATE`

---

## Bucketing Concepts

### What a Bucket Is

In StarRocks, **bucket = tablet**. A tablet is the fundamental unit of:
- **Parallel scan**: each tablet is processed by one BE thread independently
- **Data storage**: data files (segments) physically live inside a tablet
- **Load balancing**: the FE distributes tablets evenly across BE nodes

When a table has `N` buckets, the data is split into exactly `N` tablets per partition. For non-partitioned tables, there are exactly `N` tablets total.

### How Hash Bucketing Works

```
INSERT row → hash(distribution_key_columns) → bucket_id = hash_value % bucket_count
```

- Every row with the same distribution key value always lands in the same tablet
- Tablets are assigned to BEs by the FE tablet scheduler
- Each BE hosts `bucket_count / BE_count` tablets on average (assuming even assignment)

### Relationship Between Buckets and BE Nodes

```
BE_1: tablet_0, tablet_3, tablet_6, ...
BE_2: tablet_1, tablet_4, tablet_7, ...
BE_3: tablet_2, tablet_5, tablet_8, ...
```

- A query that touches all tablets will fan out to all BEs — full parallel scan
- If `bucket_count < BE_count`, some BEs will have no work for that table — wasted parallelism
- If `bucket_count >> BE_count`, each BE handles many tablets — more overhead per query but better parallelism for large scans

### Tablet Sizing Target

StarRocks documentation recommends keeping each tablet between **1 GB and 10 GB** of compressed data. The sweet spot is **~5 GB per tablet**.

- Tablets smaller than 1 GB: too many metadata operations, excessive tablet scheduling overhead
- Tablets larger than 10 GB: compaction pressure, slower individual tablet scans, harder to rebalance

---

## DISTRIBUTED BY HASH

### Syntax

```sql
CREATE TABLE orders (
    order_id     BIGINT        NOT NULL,
    user_id      BIGINT        NOT NULL,
    status       VARCHAR(32),
    amount       DECIMAL(18, 2),
    created_at   DATETIME
)
ENGINE = OLAP
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "3"
);
```

For a composite distribution key:

```sql
CREATE TABLE order_items (
    order_id     BIGINT    NOT NULL,
    item_id      BIGINT    NOT NULL,
    sku_id       BIGINT,
    quantity     INT,
    unit_price   DECIMAL(18, 2)
)
ENGINE = OLAP
DUPLICATE KEY(order_id, item_id)
DISTRIBUTED BY HASH(order_id, item_id) BUCKETS 64
PROPERTIES (
    "replication_num" = "3"
);
```

### Distribution Key Selection Rules

**Rule 1 — High cardinality (millions of distinct values)**

The distribution key must spread rows evenly across buckets. Low-cardinality keys create skew.

```sql
-- GOOD: user_id has millions of distinct values → even distribution
DISTRIBUTED BY HASH(user_id) BUCKETS 32

-- BAD: status has ~10 distinct values → most rows pile into a few buckets
DISTRIBUTED BY HASH(status) BUCKETS 32

-- BAD: country_code for a dataset with 80% traffic from one country
DISTRIBUTED BY HASH(country_code) BUCKETS 32
```

**Rule 2 — Frequently used in JOIN ON conditions**

Rows that join together should land on the same BE to enable local joins without shuffle. Choose the column(s) that appear in the most common `JOIN ON` clause.

```sql
-- If queries frequently do: orders JOIN order_items ON orders.order_id = order_items.order_id
-- Then distribute both tables on order_id

CREATE TABLE orders (...)        DISTRIBUTED BY HASH(order_id) BUCKETS 32;
CREATE TABLE order_items (...)   DISTRIBUTED BY HASH(order_id) BUCKETS 32;
-- Now joins on order_id are colocate-eligible
```

**Rule 3 — Avoid skewed columns**

Do not use columns where a small number of values account for a large percentage of rows.

Common skew traps:
- `status` (e.g., 95% rows have status = 'active')
- `country_code` for datasets with one dominant country
- `tenant_id` for single-tenant SaaS deployments
- `event_type` for heavily imbalanced event streams
- `is_deleted` boolean columns

**Rule 4 — Use composite keys when no single high-cardinality column exists**

```sql
-- Neither order_id nor product_id alone is skewed,
-- but together they have very high cardinality and match the JOIN pattern
DISTRIBUTED BY HASH(order_id, product_id) BUCKETS 48
```

**Rule 5 — Distribution key should be immutable**

StarRocks does not support updating the distribution key columns of an existing row without a full rewrite. Choose keys that never change (IDs, not mutable status/date fields).

### Verifying Distribution Evenness

Check tablet distribution across BEs after loading data:

```sql
-- Show all tablets for a table with their BE assignment and size
SHOW TABLET FROM orders;

-- Output columns include:
-- TabletId, ReplicaId, BackendId, SchemaHash, Version, DataSize, RowCount

-- Identify skewed tablets (large DataSize outliers)
-- If one tablet has 10x the DataSize of others, the distribution key is skewed
```

To get per-BE size aggregates, query the `information_schema` system table:

```sql
-- Tablet size distribution across BEs for a specific table
SELECT
    be_id,
    COUNT(*)                          AS tablet_count,
    SUM(data_size) / 1073741824.0     AS total_size_gb,
    AVG(data_size) / 1073741824.0     AS avg_tablet_size_gb,
    MAX(data_size) / 1073741824.0     AS max_tablet_size_gb
FROM information_schema.be_tablets
WHERE table_name = 'orders'
  AND db_name     = 'sales'
GROUP BY be_id
ORDER BY total_size_gb DESC;
```

---

## DISTRIBUTED BY RANDOM (StarRocks 2.5.7+)

### Syntax

```sql
CREATE TABLE events_log (
    event_id     BIGINT,
    event_type   VARCHAR(64),
    payload      VARCHAR(65535),
    event_time   DATETIME
)
ENGINE = OLAP
DUPLICATE KEY(event_id)
DISTRIBUTED BY RANDOM BUCKETS 16
PROPERTIES (
    "replication_num" = "3"
);
```

### When to Use RANDOM Distribution

Random distribution assigns rows to buckets using an internal counter or random assignment rather than a hash. Use it when:

1. **Append-only ingestion** with no join requirements — event logs, raw CDC streams, sensor data
2. **The table will never be joined to other tables** on a common key
3. **All columns are skewed** and no suitable hash key exists
4. **Write performance matters most** — random distribution avoids hot-bucket writes for skewed-key insert workloads

### Trade-offs

| Aspect | HASH | RANDOM |
|--------|------|--------|
| Write distribution | Even only if key is high-cardinality | Always even |
| Colocate join eligible | Yes | No |
| Local JOIN performance | Best (with colocate) | Requires shuffle |
| Skew risk | Yes (depends on key choice) | None |
| Suitable for | Dimension tables, fact tables with joins | Append-only logs, wide tables |

---

## Bucket Count Selection

### Formula

```
bucket_count = max(BE_count, ceil(estimated_compressed_size_GB / tablet_target_size_GB))
```

Where:
- `BE_count` = number of BE nodes in the cluster
- `estimated_compressed_size_GB` = expected compressed table size in GB
- `tablet_target_size_GB` = 5 (recommended; acceptable range: 1–10)

The `max()` ensures at least one tablet per BE for full parallelism.

### Quick Reference by Table Size

| Estimated Table Size | 3 BEs | 6 BEs | 10 BEs | 20 BEs |
|----------------------|-------|-------|--------|--------|
| < 1 GB (small dim)   | 4     | 6     | 10     | 20     |
| 1 – 10 GB            | 4     | 6     | 10     | 20     |
| 10 – 50 GB           | 10    | 12    | 12     | 20     |
| 50 – 100 GB          | 20    | 20    | 20     | 24     |
| 100 – 500 GB         | 32    | 48    | 48     | 64     |
| 500 GB – 1 TB        | 64    | 96    | 100    | 128    |
| 1 – 5 TB             | 128   | 192   | 200    | 256    |
| > 5 TB               | 256   | 256   | 512    | 512    |

Note: round up to a power of 2 or a multiple of BE count for even distribution.

### Worked Examples

**Example 1**: 3 BE nodes, 30 GB table

```
bucket_count = max(3, ceil(30 / 5)) = max(3, 6) = 6 → use 8 (nearest power-of-2 ≥ 6)
```

**Example 2**: 10 BE nodes, 400 GB table

```
bucket_count = max(10, ceil(400 / 5)) = max(10, 80) = 80 → use 80 (multiple of 10)
```

**Example 3**: 6 BE nodes, 500 MB lookup/dimension table

```
bucket_count = max(6, ceil(0.5 / 5)) = max(6, 1) = 6 → use 6
```

### Minimum Bucket Count Rules

- Always use at least `BE_count` buckets so every BE has work
- For small tables (< 1 GB): use exactly `BE_count` or a small multiple
- Do not use 1 bucket even for tiny tables — this serializes all queries to one BE

---

## Auto Bucket Sizing (StarRocks 3.2+)

StarRocks 3.2 introduced `BUCKETS AUTO`, which lets the system compute the optimal bucket count at table creation time and re-evaluate it at each partition load.

### Syntax

```sql
CREATE TABLE events (
    event_id    BIGINT,
    user_id     BIGINT,
    event_type  VARCHAR(64),
    event_time  DATETIME
)
ENGINE = OLAP
DUPLICATE KEY(event_id)
PARTITION BY RANGE(event_time)(
    PARTITION p20240101 VALUES LESS THAN ("2024-01-02"),
    PARTITION p20240102 VALUES LESS THAN ("2024-01-03")
)
DISTRIBUTED BY HASH(user_id) BUCKETS AUTO
PROPERTIES (
    "replication_num"   = "3",
    "autobucket_size_gb" = "5"    -- target tablet size in GB; default 10
);
```

### How Auto Bucketing Works

1. At table creation, StarRocks creates an initial bucket count based on `BE_count`
2. When a new partition is created (or data is loaded into the first partition), StarRocks estimates the data size
3. It calculates `bucket_count = ceil(partition_size_GB / autobucket_size_gb)` with a floor of `BE_count`
4. Subsequent partitions may get different bucket counts if data volume changes significantly

### Configuring the Target Tablet Size

```sql
-- Set globally (FE configuration)
-- In fe.conf:
-- autobucket_size_gb = 10

-- Per-table override via PROPERTIES
PROPERTIES (
    "autobucket_size_gb" = "5"
)

-- For RANDOM distribution with auto bucketing
DISTRIBUTED BY RANDOM BUCKETS AUTO
PROPERTIES (
    "autobucket_size_gb" = "5"
)
```

### When to Use Auto vs. Manual Bucketing

- Use **AUTO** for new tables where data volume is uncertain or grows over time
- Use **manual** when you have precise size estimates and want full control
- Use **manual** for colocate join groups — all tables in the group must have the same bucket count, and AUTO may diverge between tables

---

## Skew Detection and Prevention

### Detecting Skew

**Step 1**: Check overall tablet size distribution for a table

```sql
SELECT
    tablet_id,
    be_id,
    data_size / 1073741824.0     AS size_gb,
    row_count
FROM information_schema.be_tablets
WHERE table_name = 'orders'
  AND db_name = 'sales'
ORDER BY data_size DESC
LIMIT 20;
```

**Step 2**: Compute skew ratio

```sql
SELECT
    MAX(data_size)  AS max_tablet_bytes,
    AVG(data_size)  AS avg_tablet_bytes,
    MAX(data_size) / NULLIF(AVG(data_size), 0)  AS skew_ratio
FROM information_schema.be_tablets
WHERE table_name = 'orders'
  AND db_name = 'sales';
-- skew_ratio > 3.0 is problematic; > 10.0 is severe
```

**Step 3**: Find the skew-causing key value (for HASH distribution)

If skew ratio exceeds 3x, find which distribution key values are overrepresented:

```sql
-- Find top distribution key values by row count
SELECT
    user_id,
    COUNT(*) AS row_count
FROM orders
GROUP BY user_id
ORDER BY row_count DESC
LIMIT 20;

-- If one user_id has billions of rows, that key value is causing skew
```

### Skew Thresholds

| Skew Ratio (max / avg) | Assessment | Action |
|------------------------|------------|--------|
| 1.0 – 1.5 | Healthy | None |
| 1.5 – 3.0 | Acceptable | Monitor |
| 3.0 – 10.0 | Problematic | Investigate and fix distribution key |
| > 10.0 | Severe | Immediate redesign required |

### Preventing Skew

**Strategy 1**: Add a secondary high-cardinality column to the composite distribution key

```sql
-- Instead of just user_id (skewed for large users):
DISTRIBUTED BY HASH(user_id, order_id) BUCKETS 64
-- order_id adds entropy; large users still produce many rows but spread across buckets
```

**Strategy 2**: Switch to RANDOM if no join is needed

```sql
-- For write-heavy, query-light log tables:
DISTRIBUTED BY RANDOM BUCKETS 32
```

**Strategy 3**: Filter skewed keys before writing to a hot table and route them separately

```sql
-- Write the 99th-percentile users to a separate table with RANDOM distribution
-- Route analytical queries to union both tables
```

---

## Changing Bucket Count (ALTER TABLE)

### Syntax

StarRocks 3.2+ supports modifying the distribution of existing tables. This rewrites the table data.

```sql
-- Change bucket count for a non-partitioned table
ALTER TABLE orders
    DISTRIBUTED BY HASH(user_id) BUCKETS 64;

-- Change both the distribution key and bucket count
ALTER TABLE orders
    DISTRIBUTED BY HASH(user_id, order_id) BUCKETS 128;

-- Change to RANDOM distribution
ALTER TABLE orders
    DISTRIBUTED BY RANDOM BUCKETS 32;
```

For partitioned tables, StarRocks rewrites all partitions:

```sql
-- Increase bucket count after data growth
ALTER TABLE events_partitioned
    DISTRIBUTED BY HASH(user_id) BUCKETS 128;
```

### When to Change the Bucket Count

- **After significant data growth**: table has grown 5× since original design and tablets exceed 10 GB
- **After adding BE nodes**: the original bucket count is now a bottleneck (e.g., 8 buckets on 20 BEs)
- **After discovering severe skew**: the distribution key was poorly chosen
- **Before setting up a colocate join group**: both tables must have the same bucket count

### Operational Considerations

- **Data rewrite**: `ALTER TABLE ... DISTRIBUTED BY` rewrites all table data. This is a heavy operation.
- **Disk space**: temporarily requires 2× disk space (original + rewritten data)
- **Concurrent queries**: queries continue to work against the original data during rewrite (StarRocks uses MVCC-style visibility)
- **Duration**: proportional to table size; plan during off-peak hours for large tables
- **Monitoring progress**: use `SHOW ALTER TABLE` to track the operation status

```sql
-- Monitor ALTER TABLE progress
SHOW ALTER TABLE COLUMN FROM sales_db;
-- or for distribution changes:
SHOW ALTER TABLE DISTRIBUTED FROM sales_db;
```

---

## Colocate Join

### What Colocate Join Does

Normally, joining two tables requires a **shuffle** step: the FE inserts exchange nodes into the query plan to redistribute rows to the correct BE before the join executes. This causes network I/O and reduces parallelism.

With colocate join, two tables in the same **colocate group** are guaranteed to have:
- The same distribution key columns and types
- The same bucket count
- The same replication factor

This means rows with the same join key value are always on the same BE in both tables. The FE can emit a **local join** (no shuffle) for any query joining those tables on their distribution key.

### Creating a Colocate Group

```sql
-- Table 1: orders — distribution key = user_id, 32 buckets
CREATE TABLE orders (
    order_id    BIGINT     NOT NULL,
    user_id     BIGINT     NOT NULL,
    status      VARCHAR(32),
    amount      DECIMAL(18, 2),
    created_at  DATETIME
)
ENGINE = OLAP
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "3",
    "colocate_with"   = "user_group"    -- colocate group name
);

-- Table 2: user_profiles — must use SAME distribution key type, SAME bucket count, SAME replication_num
CREATE TABLE user_profiles (
    user_id       BIGINT     NOT NULL,
    username      VARCHAR(128),
    email         VARCHAR(256),
    region        VARCHAR(64),
    created_at    DATETIME
)
ENGINE = OLAP
DUPLICATE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "3",
    "colocate_with"   = "user_group"    -- same group name
);

-- Table 3: user_events — same group
CREATE TABLE user_events (
    event_id    BIGINT     NOT NULL,
    user_id     BIGINT     NOT NULL,
    event_type  VARCHAR(64),
    event_time  DATETIME
)
ENGINE = OLAP
DUPLICATE KEY(event_id, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "3",
    "colocate_with"   = "user_group"
);
```

### Colocate Group Requirements

All tables in the same colocate group MUST have:

| Requirement | Example Compliant | Example Non-Compliant |
|-------------|------------------|-----------------------|
| Same distribution key column count | Both use 1 column | One uses 1, other uses 2 |
| Same distribution key column types | Both use `BIGINT` | One uses `BIGINT`, other `VARCHAR` |
| Same bucket count | Both use 32 | One uses 32, other uses 64 |
| Same replication factor | Both use `"replication_num" = "3"` | One uses 3, other uses 1 |
| Same engine type | Both `OLAP` | Mixed `OLAP` / `MYSQL` |

If any requirement is violated, StarRocks rejects the table creation with an error about incompatible colocate group properties.

### Checking Colocate Group Status

```sql
-- List all colocate groups and their status
SHOW PROC '/colocation_group';

-- Output columns:
-- GroupId, GroupName, TableIds, BucketsNum, ReplicationNum,
-- DistributionCols, IsStable

-- A group is "stable" when all its tablet replicas are properly placed.
-- An "unstable" group cannot use colocate join until rebalancing completes.
```

Example output interpretation:

```
GroupId: 12345
GroupName: sales_db.user_group
TableIds: [101, 102, 103]
BucketsNum: 32
ReplicationNum: 3
DistributionCols: [user_id]
IsStable: true          ← colocate join is active
```

If `IsStable = false`, the FE falls back to shuffle join automatically.

### Verifying Colocate Join in Query Plans

```sql
-- Use EXPLAIN to verify the join strategy
EXPLAIN SELECT
    o.order_id,
    o.amount,
    u.username,
    u.region
FROM orders o
JOIN user_profiles u ON o.user_id = u.user_id
WHERE o.created_at >= '2024-01-01';
```

Look for the join node type in the output:

```
-- GOOD: colocate join active (no shuffle)
HASH JOIN (join op: INNER JOIN (COLOCATE))
  |  equal join conjunct: o.user_id = u.user_id

-- BAD: shuffle join (colocate group unstable or tables not in same group)
HASH JOIN (join op: INNER JOIN (BUCKET_SHUFFLE))
  |  equal join conjunct: o.user_id = u.user_id

-- ALSO BAD: broadcast join (small table broadcast, not colocate)
HASH JOIN (join op: INNER JOIN (BROADCAST))
  |  equal join conjunct: o.user_id = u.user_id
```

### Enabling and Disabling Colocate Join

```sql
-- Enable colocate join at session level (default is enabled)
SET enable_colocate_join = true;

-- Disable colocate join for a specific session (debugging)
SET enable_colocate_join = false;

-- Disable globally in FE config (not recommended)
-- In fe.conf: enable_colocate_join = false
```

### Colocate Group Rebalancing

When the cluster topology changes (BE added, BE removed, BE crashes), the colocate group may become **unstable** because tablet placement no longer satisfies the colocate constraint (tablets for the same bucket index must be on the same set of BEs for all tables in the group).

StarRocks automatically rebalances colocate groups via the **tablet scheduler**. During rebalancing:
- The group is marked `IsStable = false`
- All queries fall back to shuffle join
- No manual intervention is required

To check rebalancing progress:

```sql
-- Monitor tablet migration status
SHOW PROC '/tablet_scheduler';

-- Check group stability
SHOW PROC '/colocation_group';

-- View backend status to confirm all BEs are alive
SHOW BACKENDS;
```

To manually trigger re-evaluation of a colocate group (after BE recovery):

```sql
-- No direct SQL command; the scheduler runs automatically.
-- Ensure all BEs are ALIVE via SHOW BACKENDS.
-- The FE will automatically schedule migrations to restore colocate placement.
```

### Removing a Table from a Colocate Group

```sql
-- Remove a table from its colocate group by clearing the property
ALTER TABLE orders SET ("colocate_with" = "");

-- Move a table to a different colocate group
ALTER TABLE orders SET ("colocate_with" = "new_group");
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| `DISTRIBUTED BY HASH(status) BUCKETS 16` — low cardinality key | Most rows hash to a few buckets; massive skew; one BE does all the work for JOIN | Use a high-cardinality column: `HASH(user_id)` |
| `BUCKETS 2` on a 10-BE cluster | Only 2 tablets → 8 BEs idle for every query on this table; no parallelism | Use at least `BE_count` buckets |
| `BUCKETS 1024` on a 1 GB table | 1024 tablets × 3 replicas = 3072 tiny tablet files; metadata explosion; slow compaction | Use `max(BE_count, ceil(1/5)) = BE_count` |
| Different bucket counts for colocate tables | Colocate group is permanently unstable; all joins shuffle | Ensure all tables in the group have the same bucket count |
| Different distribution key types in colocate group | Group creation fails or group is unstable | Match types exactly: both `BIGINT`, not one `INT` and one `BIGINT` |
| Using `DISTRIBUTED BY RANDOM` for tables you join | Cannot use colocate join; every join shuffles | Use `HASH` on the join key for tables that participate in joins |
| Never revisiting bucket count after 10× data growth | Tablets grow to 50–100 GB; compaction stalls; slow scans | Periodically check `information_schema.be_tablets` and run `ALTER TABLE ... DISTRIBUTED BY` when tablets exceed 10 GB |
| Choosing distribution key that changes (e.g., `updated_at`) | Updates require delete + reinsert with a new bucket assignment; unpredictable behavior | Use immutable identifier columns (IDs, UUIDs) as distribution key |
| Using `BUCKETS AUTO` in a colocate group | AUTO may assign different bucket counts for different partitions across tables, breaking colocate constraint | Use explicit `BUCKETS N` for all tables in a colocate group |
| Joining colocate tables on a non-distribution column | StarRocks cannot use colocate join; falls back to shuffle | The JOIN ON column must match the distribution key column for colocate to activate |

---

## Production Workflow: Initial Table Design

```sql
-- Step 1: Identify join patterns
-- Most frequent join: orders JOIN users ON orders.user_id = users.user_id
-- Most frequent join: orders JOIN order_items ON orders.order_id = order_items.order_id

-- Step 2: Estimate data sizes (year 1)
-- orders: ~500 GB compressed
-- users: ~10 GB compressed
-- order_items: ~2 TB compressed

-- Step 3: Calculate bucket counts (6 BE cluster)
-- orders:      max(6, ceil(500/5)) = max(6, 100) = 100 → use 96 (multiple of 6)
-- users:       max(6, ceil(10/5))  = max(6, 2)   = 6   → use 6
-- order_items: max(6, ceil(2000/5)) = max(6, 400) = 400 → use 396 (multiple of 6)

-- Step 4: Create colocate groups
-- Group "order_group": orders + order_items (same distribution key: order_id, same bucket count needed)
-- Problem: orders needs 96 buckets, order_items needs 396 → can't colocate!
-- Resolution: prioritize the most impactful join; if order_id join is more frequent:
--   Use 128 buckets for both (round up to power-of-2, tablets will be ~4 GB for orders, ~16 GB for items)
--   Or: keep them separate and accept shuffle for order_items join

-- Step 5: Create tables
CREATE TABLE orders (
    order_id    BIGINT     NOT NULL,
    user_id     BIGINT     NOT NULL,
    status      VARCHAR(32),
    amount      DECIMAL(18, 2),
    created_at  DATETIME
)
ENGINE = OLAP
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 128
PROPERTIES (
    "replication_num" = "3",
    "colocate_with"   = "order_group"
);

CREATE TABLE order_items (
    item_id     BIGINT     NOT NULL,
    order_id    BIGINT     NOT NULL,
    sku_id      BIGINT,
    quantity    INT,
    unit_price  DECIMAL(18, 2)
)
ENGINE = OLAP
DUPLICATE KEY(item_id, order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 128
PROPERTIES (
    "replication_num" = "3",
    "colocate_with"   = "order_group"
);

-- Step 6: Verify colocate group is stable
SHOW PROC '/colocation_group';

-- Step 7: Verify with EXPLAIN
EXPLAIN SELECT o.order_id, SUM(i.unit_price * i.quantity) AS revenue
FROM orders o
JOIN order_items i ON o.order_id = i.order_id
WHERE o.created_at >= '2024-01-01'
GROUP BY o.order_id;
-- Expect: HASH JOIN (COLOCATE) in plan
```

---

## Monitoring Bucket Health (Ongoing)

```sql
-- Weekly health check: tablet size distribution
SELECT
    db_name,
    table_name,
    COUNT(*)                                       AS tablet_count,
    ROUND(AVG(data_size) / 1073741824.0, 2)        AS avg_size_gb,
    ROUND(MAX(data_size) / 1073741824.0, 2)        AS max_size_gb,
    ROUND(MIN(data_size) / 1073741824.0, 2)        AS min_size_gb,
    ROUND(MAX(data_size) / NULLIF(AVG(data_size), 0), 2) AS skew_ratio
FROM information_schema.be_tablets
WHERE db_name NOT IN ('_statistics_', 'information_schema')
GROUP BY db_name, table_name
HAVING avg_size_gb > 0.1    -- skip trivially small tables
ORDER BY max_size_gb DESC;

-- Alert thresholds:
-- max_size_gb > 10   → consider ALTER TABLE to increase bucket count
-- skew_ratio > 3.0   → investigate distribution key skew
-- avg_size_gb < 0.1  → too many buckets; consider consolidation
```

---

## References

- [StarRocks Data Distribution Documentation](https://docs.starrocks.io/docs/table_design/data_distribution/)
- [StarRocks Colocate Join Documentation](https://docs.starrocks.io/docs/table_design/data_distribution/colocate_join/)
- [StarRocks Table Design Overview](https://docs.starrocks.io/docs/table_design/StarRocks_table_design/)
- [StarRocks ALTER TABLE Syntax](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE/)
- [StarRocks information_schema Reference](https://docs.starrocks.io/docs/reference/information_schema/be_tablets/)
