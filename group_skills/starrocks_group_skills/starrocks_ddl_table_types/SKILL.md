---
name: starrocks-ddl-table-types
description: StarRocks table types — Duplicate Key (append-only log/event), Aggregate Key (pre-aggregation metrics), Unique Key (upsert dedup), Primary Key (full DML upsert/delete, MVCC), CREATE TABLE DDL, key column selection, aggregate functions (SUM/MAX/MIN/REPLACE/HLL_UNION/BITMAP_UNION), when to use each type, storage engine differences
---

# StarRocks — Table Types and DDL

## When to Use

Load this skill when the user needs to:
- Choose the right StarRocks table type (Duplicate Key, Aggregate Key, Unique Key, Primary Key)
- Write `CREATE TABLE` DDL with correct key definitions, partitioning, and properties
- Understand pre-aggregation semantics and which aggregate functions to use
- Design CDC target tables with Primary Key for real-time upserts and deletes
- Tune storage properties: replication, compression, persistent index, sort keys
- Perform `ALTER TABLE` operations on existing StarRocks tables
- Understand the relationship between sort key, distribution key, and Primary Key

---

## Decision Table: Which Table Type to Use

| Need | Table Type | Key Reason |
|---|---|---|
| Raw event / log data, no dedup, max ingest throughput | **Duplicate Key** | Rows with same key coexist; no merge overhead at load |
| Pre-aggregate metrics at load time, reduce storage and query cost | **Aggregate Key** | Non-key columns have aggregate functions applied on merge |
| Deduplicate rows by natural key, no need for UPDATE/DELETE | **Unique Key** | Latest row wins on key collision (REPLACE semantics) |
| Real-time upsert + DELETE, CDC ingestion, DML-heavy workloads | **Primary Key** | Full DML support, MVCC row-level updates, delete vectors |

**Quick flowchart:**
```
Need UPDATE or DELETE after insert?
  YES → Primary Key
  NO  → Need pre-aggregation (SUM, MAX, etc.) at load time?
          YES → Aggregate Key
          NO  → Need deduplication (keep latest)?
                  YES → Unique Key
                  NO  → Duplicate Key
```

---

## 1. Duplicate Key Table

### Use Case
- Append-only event streams: clickstream, access logs, IoT sensor readings
- Raw data landing zone before transformation
- Any table where multiple rows with the same logical key are valid
- Maximum write throughput — no aggregation or dedup overhead

### Storage Layout
Rows are physically sorted by the `DUPLICATE KEY(...)` columns. Duplicate keys are allowed — multiple rows with identical key column values coexist in the same tablet. No merging occurs at load time or compaction time.

The `DUPLICATE KEY` columns must be a **prefix** of the sort key (by default, the sort key is identical to the key columns unless `ORDER BY` is specified separately in StarRocks 3.0+).

### CREATE TABLE DDL

```sql
CREATE TABLE IF NOT EXISTS ods.page_events (
    event_date      DATE           NOT NULL COMMENT 'Partition date',
    event_ts        DATETIME       NOT NULL COMMENT 'Event timestamp (UTC)',
    event_id        VARCHAR(36)    NOT NULL COMMENT 'UUID, deduplicated upstream',
    user_id         BIGINT         NOT NULL COMMENT 'User identifier',
    session_id      VARCHAR(64)    NOT NULL COMMENT 'Browser session',
    event_type      VARCHAR(64)    NOT NULL COMMENT 'page_view | click | scroll',
    page_url        VARCHAR(2048)  NOT NULL COMMENT 'Full URL',
    referrer_url    VARCHAR(2048)           COMMENT 'Referrer URL, nullable',
    device_type     VARCHAR(32)             COMMENT 'desktop | mobile | tablet',
    country_code    CHAR(2)                 COMMENT 'ISO 3166-1 alpha-2',
    ip_address      VARCHAR(45)             COMMENT 'IPv4 or IPv6',
    user_agent      VARCHAR(512)            COMMENT 'Raw user agent string',
    load_ts         DATETIME       NOT NULL COMMENT 'ETL load timestamp'
)
DUPLICATE KEY(event_date, event_ts, event_id)   -- prefix of ORDER BY
COMMENT 'Raw page interaction events — Duplicate Key for append-only log'
PARTITION BY RANGE(event_date) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
ORDER BY (event_date, event_ts, event_id)       -- explicit sort key (SR 3.0+)
PROPERTIES (
    "replication_num"  = "3",
    "storage_medium"   = "SSD",
    "compression"      = "LZ4",
    "dynamic_partition.enable"        = "true",
    "dynamic_partition.time_unit"     = "DAY",
    "dynamic_partition.start"         = "-30",
    "dynamic_partition.end"           = "3",
    "dynamic_partition.prefix"        = "p",
    "dynamic_partition.buckets"       = "32"
);
```

### Key Rules
- `DUPLICATE KEY` columns must be a contiguous prefix of the `ORDER BY` (sort key) columns.
- All data types except FLOAT, DOUBLE, JSON, ARRAY, MAP, STRUCT are supported as key columns.
- No restriction on value (non-key) columns.
- This is the **default** table type if no `KEY` clause is specified.

---

## 2. Aggregate Key Table

### Use Case
- Pre-aggregated metrics tables: daily/hourly rollups, session summaries
- Wide fact tables where raw data is too large to query at full granularity
- Reducing cardinality at ingest time instead of query time
- Approximate analytics using HLL (HyperLogLog) or BITMAP sketches

### How Aggregation Works
StarRocks applies aggregate functions **twice**:
1. **At load time** — within each batch, rows with identical key columns are merged before being written to disk.
2. **At compaction time** — background compaction merges rowsets with matching keys across segments.
3. **At query time** — the query engine applies the same aggregate functions when combining rowsets that have not yet been fully compacted.

The result is equivalent to `GROUP BY <key_columns>` with the configured aggregate functions applied to each value column.

### Aggregate Functions per Column Type

| Function | Description | Typical Column Type |
|---|---|---|
| `SUM` | Accumulate numeric values | BIGINT, DOUBLE, DECIMAL |
| `MAX` | Keep the maximum value | INT, BIGINT, DATETIME, VARCHAR |
| `MIN` | Keep the minimum value | INT, BIGINT, DATETIME, VARCHAR |
| `REPLACE` | Keep the latest loaded value (by load order) | Any non-key type |
| `REPLACE_IF_NOT_NULL` | `REPLACE` but ignores NULL updates | Any nullable type |
| `HLL_UNION` | Merge HyperLogLog sketches for approx. COUNT DISTINCT | HLL |
| `BITMAP_UNION` | Merge bitmap sets for exact/approx. COUNT DISTINCT | BITMAP |
| `PERCENTILE_UNION` | Merge TDigest sketches for approx. percentiles | PERCENTILE |

### CREATE TABLE DDL — Daily Metrics Rollup

```sql
CREATE TABLE IF NOT EXISTS dws.daily_user_metrics (
    metric_date        DATE        NOT NULL COMMENT 'Aggregation date',
    user_id            BIGINT      NOT NULL COMMENT 'User identifier',
    country_code       CHAR(2)     NOT NULL COMMENT 'ISO 3166-1 country',
    product_category   VARCHAR(64) NOT NULL COMMENT 'Top-level product category',

    -- Additive metrics
    page_views         BIGINT      SUM     DEFAULT "0"   COMMENT 'Total page views',
    session_count      INT         SUM     DEFAULT "0"   COMMENT 'Total sessions started',
    purchase_count     INT         SUM     DEFAULT "0"   COMMENT 'Total purchases',
    revenue_cents      BIGINT      SUM     DEFAULT "0"   COMMENT 'Revenue in cents',
    active_seconds     BIGINT      SUM     DEFAULT "0"   COMMENT 'Seconds of active engagement',

    -- Max/min latencies
    max_load_time_ms   INT         MAX     DEFAULT "0"   COMMENT 'Max page load time',
    min_load_time_ms   INT         MIN     DEFAULT "99999" COMMENT 'Min page load time',

    -- Replace semantics — keep latest cohort assignment
    cohort_id          VARCHAR(32) REPLACE DEFAULT ""    COMMENT 'Experiment cohort',

    -- Replace only if not null — sparse attribute updates
    user_segment       VARCHAR(32) REPLACE_IF_NOT_NULL   COMMENT 'Marketing segment',

    -- HLL for approx. distinct users from external source
    unique_devices     HLL         HLL_UNION             COMMENT 'HLL sketch of distinct device_ids',

    -- Bitmap for exact active user sets by day
    active_user_bitmap BITMAP      BITMAP_UNION          COMMENT 'Bitmap of user_ids active today',

    -- Percentile for p50/p90/p99 latency
    load_time_pct      PERCENTILE  PERCENTILE_UNION      COMMENT 'TDigest for load time percentiles'
)
AGGREGATE KEY(metric_date, user_id, country_code, product_category)
COMMENT 'Daily user engagement metrics — Aggregate Key rollup table'
PARTITION BY RANGE(metric_date) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "storage_medium"  = "SSD",
    "compression"     = "ZSTD"
);
```

### Querying HLL and BITMAP Columns

```sql
-- Approximate distinct device count
SELECT
    metric_date,
    country_code,
    HLL_CARDINALITY(unique_devices) AS approx_distinct_devices
FROM dws.daily_user_metrics
WHERE metric_date BETWEEN '2025-06-01' AND '2025-06-30'
GROUP BY metric_date, country_code;

-- Exact active users per day (bitmap)
SELECT
    metric_date,
    BITMAP_COUNT(active_user_bitmap) AS active_users
FROM dws.daily_user_metrics
WHERE metric_date = '2025-06-15';

-- Approximate p99 load time
SELECT
    metric_date,
    PERCENTILE_APPROX(load_time_pct, 0.99) AS p99_load_ms
FROM dws.daily_user_metrics
WHERE metric_date = '2025-06-15';
```

### Loading into HLL / BITMAP Columns

```sql
-- Insert with HLL sketch built from raw values
INSERT INTO dws.daily_user_metrics
    (metric_date, user_id, country_code, product_category,
     page_views, session_count, purchase_count, revenue_cents,
     active_seconds, max_load_time_ms, min_load_time_ms,
     unique_devices, active_user_bitmap)
SELECT
    CAST(event_date AS DATE),
    user_id,
    country_code,
    product_category,
    COUNT(*)                         AS page_views,
    COUNT(DISTINCT session_id)       AS session_count,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END),
    SUM(revenue_cents),
    SUM(active_seconds),
    MAX(load_time_ms),
    MIN(load_time_ms),
    HLL_RAW_AGG(device_id)           AS unique_devices,   -- builds sketch
    BITMAP_AGG(CAST(user_id AS INT)) AS active_user_bitmap
FROM ods.page_events
WHERE event_date = '2025-06-15'
GROUP BY event_date, user_id, country_code, product_category;
```

### Key Rules
- Every non-key column must have an aggregate function declared in the column definition.
- `AGGREGATE KEY` columns become the grouping key; their order defines the sort order.
- Aggregation is idempotent only for `SUM`, `MAX`, `MIN` — **do not load the same batch twice** with `REPLACE`; it will overwrite with the duplicate load.
- Cannot use `UPDATE` or `DELETE` on Aggregate Key tables.

---

## 3. Unique Key Table

### Use Case
- Dimension tables loaded via full or incremental snapshot
- Deduplication by business key without needing in-place UPDATE/DELETE
- Tables where "latest wins" semantics are acceptable (last write wins per key)
- Lower overhead than Primary Key when full DML is not required

### Semantics
On key collision, the incoming row **replaces** the existing row entirely. This is equivalent to `REPLACE INTO` semantics. There is no support for partial column updates — the entire row is replaced.

Unlike Primary Key, Unique Key does **not** use MVCC. Updates are handled at compaction time by discarding older versions of rows with the same key. This means:
- No `DELETE` statement support (only full-row replacement via load)
- Slightly lower write latency than Primary Key for simple upsert workloads
- No delete vector overhead

### CREATE TABLE DDL

```sql
CREATE TABLE IF NOT EXISTS dim.dim_product (
    product_id      BIGINT       NOT NULL COMMENT 'Business key (PK)',
    sku             VARCHAR(64)  NOT NULL COMMENT 'Stock keeping unit',
    product_name    VARCHAR(256) NOT NULL COMMENT 'Product display name',
    category_l1     VARCHAR(64)  NOT NULL COMMENT 'Top-level category',
    category_l2     VARCHAR(64)           COMMENT 'Sub-category',
    brand           VARCHAR(128)          COMMENT 'Brand name',
    unit_price_usd  DECIMAL(12,2)         COMMENT 'Current list price USD',
    weight_kg       DECIMAL(8,3)          COMMENT 'Weight in kilograms',
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE COMMENT 'Active flag',
    created_at      DATETIME     NOT NULL COMMENT 'Record creation timestamp',
    updated_at      DATETIME     NOT NULL COMMENT 'Last update timestamp'
)
UNIQUE KEY(product_id)
COMMENT 'Product dimension — Unique Key, latest-write-wins deduplication'
DISTRIBUTED BY HASH(product_id) BUCKETS 8
ORDER BY (product_id)
PROPERTIES (
    "replication_num" = "3",
    "compression"     = "LZ4"
);
```

### Unique Key with Composite Business Key

```sql
CREATE TABLE IF NOT EXISTS dim.dim_store_product_price (
    store_id       INT          NOT NULL COMMENT 'Store identifier',
    product_id     BIGINT       NOT NULL COMMENT 'Product identifier',
    effective_date DATE         NOT NULL COMMENT 'Price effective date',
    price_usd      DECIMAL(12,2) NOT NULL COMMENT 'Price for store+product on date',
    currency_code  CHAR(3)      NOT NULL DEFAULT 'USD',
    updated_at     DATETIME     NOT NULL
)
UNIQUE KEY(store_id, product_id, effective_date)
COMMENT 'Store-level price overrides — Unique Key on composite business key'
DISTRIBUTED BY HASH(store_id, product_id) BUCKETS 16
ORDER BY (store_id, product_id, effective_date)
PROPERTIES (
    "replication_num" = "3",
    "compression"     = "LZ4"
);
```

### Key Rules
- `UNIQUE KEY` columns form the deduplication key; each unique combination appears at most once after compaction.
- `ORDER BY` (sort key) must start with the `UNIQUE KEY` columns.
- No `DELETE` or `UPDATE` DML is supported — use Primary Key if DELETE is needed.
- Behaves like Aggregate Key with all value columns using `REPLACE` semantics.

---

## 4. Primary Key Table

### Use Case
- CDC (Change Data Capture) target tables receiving INSERT/UPDATE/DELETE events
- Real-time user profile tables updated by streaming pipelines (Flink, Spark Structured Streaming)
- Order management tables requiring exact DELETE semantics
- Any table that needs partial column updates (`UPDATE t SET col = val WHERE pk = x`)

### MVCC Storage Architecture
Primary Key tables use a **row-level MVCC** model with delete vectors:

```
Write path:
  New row → Rowset (column files on disk)
  DELETE / UPDATE → Delete vector (bitmap marking deleted row positions in older rowsets)

Read path:
  Merge rowsets + apply delete vectors to reconstruct the current state of each row
  Persistent index (optional) accelerates PK lookup to apply delete vectors without full scan
```

The **persistent index** (`enable_persistent_index = true`) stores the PK-to-rowset-position mapping on disk (SSD-backed). Without it, the index is rebuilt in memory on BE restart. For large tables (>100 M rows) where memory is constrained, the persistent index is essential.

**Memory constraint:** The PK index must fit in BE memory. Each PK entry consumes approximately 40–100 bytes. A table with 1 billion rows with an 8-byte integer PK needs ~80 GB memory for the in-memory index. Use persistent index and size accordingly.

### CREATE TABLE DDL — CDC Target Table (Orders)

```sql
CREATE TABLE IF NOT EXISTS dwd.orders (
    order_id        BIGINT       NOT NULL COMMENT 'Primary key — CDC source PK',
    user_id         BIGINT       NOT NULL COMMENT 'User who placed the order',
    store_id        INT          NOT NULL COMMENT 'Fulfilling store',
    order_status    VARCHAR(32)  NOT NULL COMMENT 'placed|confirmed|shipped|delivered|cancelled',
    order_date      DATE         NOT NULL COMMENT 'Date order was placed',
    total_amount    DECIMAL(14,2) NOT NULL COMMENT 'Total order amount USD',
    discount_amount DECIMAL(14,2) NOT NULL DEFAULT "0.00" COMMENT 'Discount applied',
    shipping_address VARCHAR(512)          COMMENT 'Delivery address',
    payment_method  VARCHAR(32)           COMMENT 'card|wallet|cod',
    created_at      DATETIME     NOT NULL COMMENT 'Row creation time (source)',
    updated_at      DATETIME     NOT NULL COMMENT 'Last CDC update time',
    __op            TINYINT               COMMENT 'CDC op: 0=UPSERT 1=DELETE (internal, optional)'
)
PRIMARY KEY(order_id)
COMMENT 'Orders — Primary Key for CDC upsert/delete from OLTP source'
PARTITION BY RANGE(order_date) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(order_id) BUCKETS 32
ORDER BY (order_id, order_date)     -- sort key starts with PK columns
PROPERTIES (
    "replication_num"          = "3",
    "storage_medium"           = "SSD",
    "compression"              = "LZ4",
    "enable_persistent_index"  = "true",
    "primary_key_cache_size"   = "4294967296"   -- 4 GB PK index cache (bytes)
);
```

### CREATE TABLE DDL — User Profile Table

```sql
CREATE TABLE IF NOT EXISTS dim.user_profile (
    user_id          BIGINT       NOT NULL COMMENT 'Global user ID',
    email            VARCHAR(256) NOT NULL COMMENT 'Email address',
    username         VARCHAR(64)  NOT NULL COMMENT 'Display name',
    phone            VARCHAR(32)           COMMENT 'Phone number',
    country_code     CHAR(2)               COMMENT 'ISO 3166-1',
    signup_date      DATE                  COMMENT 'Account creation date',
    last_login_date  DATE                  COMMENT 'Most recent login',
    tier             VARCHAR(16)           COMMENT 'bronze|silver|gold|platinum',
    lifetime_orders  INT          NOT NULL DEFAULT "0" COMMENT 'Total orders placed',
    lifetime_revenue DECIMAL(14,2) NOT NULL DEFAULT "0.00",
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,
    updated_at       DATETIME     NOT NULL COMMENT 'Last profile update'
)
PRIMARY KEY(user_id)
COMMENT 'User profile — Primary Key for real-time streaming updates'
DISTRIBUTED BY HASH(user_id) BUCKETS 16
ORDER BY (user_id)
PROPERTIES (
    "replication_num"         = "3",
    "storage_medium"          = "SSD",
    "compression"             = "LZ4",
    "enable_persistent_index" = "true"
);
```

### DML on Primary Key Tables

```sql
-- Standard INSERT (upsert: replaces existing row if PK matches)
INSERT INTO dim.user_profile (user_id, email, username, updated_at)
VALUES (1001, 'alice@example.com', 'alice', NOW());

-- UPDATE specific columns
UPDATE dim.user_profile
SET tier = 'gold', lifetime_orders = lifetime_orders + 1, updated_at = NOW()
WHERE user_id = 1001;

-- DELETE a row
DELETE FROM dim.user_profile
WHERE user_id = 9999;

-- Bulk upsert from staging
INSERT INTO dwd.orders
SELECT * FROM stage.orders_cdc_batch
WHERE batch_id = 20250617;
```

### Partial Column Updates (Primary Key Only)

StarRocks supports column-level partial updates for Primary Key tables, avoiding the need to re-supply all columns:

```sql
-- Only update the tier and lifetime_revenue columns
-- Other columns retain their existing values
UPDATE dim.user_profile
SET tier = 'platinum', lifetime_revenue = 150000.00
WHERE user_id = 42;
```

Partial updates via `INSERT` require the table property `"partial_update" = "true"` and using the `COLUMNS` clause in `INSERT INTO ... COLUMNS(...)`:

```sql
INSERT INTO dim.user_profile (user_id, tier, updated_at)
VALUES (42, 'platinum', NOW());
-- Requires: set partial_update property or use stream load with partial_update=true
```

### Key Rules
- `ORDER BY` sort key must **start with** all `PRIMARY KEY` columns.
- PK columns must fit in memory (or use persistent index for disk-backed lookup).
- `enable_persistent_index = true` is strongly recommended for tables > 50 M rows.
- Supports full INSERT, UPDATE, DELETE, and UPSERT semantics.
- No FLOAT/DOUBLE/JSON/ARRAY/MAP/STRUCT in PK columns.

---

## 5. DDL Properties Reference

| Property | Type | Default | Description |
|---|---|---|---|
| `replication_num` | INT | `3` | Number of tablet replicas. Use `1` in dev/test. |
| `storage_medium` | STRING | `"HDD"` | `"SSD"` or `"HDD"`. SSD for hot/primary data. |
| `storage_cooldown_time` | DATETIME | — | Time after which SSD data migrates to HDD. |
| `compression` | STRING | `"LZ4"` | `"LZ4"` (fast), `"ZSTD"` (higher ratio), `"SNAPPY"`, `"NONE"` |
| `enable_persistent_index` | BOOLEAN | `"false"` | Primary Key only. Stores PK index on disk (SSD). Required for large PK tables. |
| `primary_key_cache_size` | BYTES | system default | Primary Key only. Size in bytes of the in-memory PK index cache. |
| `bloom_filter_columns` | STRING | — | Comma-separated columns to add Bloom filter index (reduces false reads on point lookups). |
| `dynamic_partition.enable` | BOOLEAN | `"false"` | Enable automatic daily/monthly partition creation and drop. |
| `dynamic_partition.time_unit` | STRING | — | `"DAY"`, `"WEEK"`, `"MONTH"` |
| `dynamic_partition.start` | INT | — | Oldest partition to keep (negative = days/months ago). |
| `dynamic_partition.end` | INT | — | How far ahead to pre-create partitions. |
| `dynamic_partition.prefix` | STRING | `"p"` | Partition name prefix. |
| `dynamic_partition.buckets` | INT | — | Bucket count for dynamically created partitions. |

### Full Properties Example

```sql
PROPERTIES (
    "replication_num"           = "3",
    "storage_medium"            = "SSD",
    "storage_cooldown_time"     = "2025-12-31 00:00:00",
    "compression"               = "ZSTD",
    "enable_persistent_index"   = "true",
    "primary_key_cache_size"    = "8589934592",   -- 8 GB
    "bloom_filter_columns"      = "user_id,order_status",
    "dynamic_partition.enable"  = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start"   = "-90",
    "dynamic_partition.end"     = "7",
    "dynamic_partition.prefix"  = "p",
    "dynamic_partition.buckets" = "32"
);
```

---

## 6. Sort Key vs Distribution Key

### Concepts

| Concept | Defined By | Purpose |
|---|---|---|
| **Distribution Key** | `DISTRIBUTED BY HASH(col1, col2)` | Determines which tablet (shard) a row goes to. Controls data locality and join colocaton. |
| **Sort Key** (pre-3.0) | First N key columns implicitly | Physical sort order within each tablet. Determines prefix index efficiency. |
| **Sort Key** (3.0+, explicit) | `ORDER BY (col1, col2, ...)` clause | Decoupled from key columns. Defines physical row order and prefix index. |

### StarRocks 3.0+ Decoupling

Before StarRocks 3.0, the sort key was always identical to the key columns (first N columns of `DUPLICATE KEY`, `AGGREGATE KEY`, etc.). Starting with 3.0, Primary Key tables support an explicit `ORDER BY` clause that can include additional columns beyond the PK, enabling better range scan performance:

```sql
-- Primary Key on order_id, but sort by (order_id, order_date) for efficient date-range scans
CREATE TABLE dwd.orders (
    order_id    BIGINT   NOT NULL,
    order_date  DATE     NOT NULL,
    user_id     BIGINT   NOT NULL,
    ...
)
PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 32
ORDER BY (order_id, order_date)   -- sort key is superset of PK
PROPERTIES ("replication_num" = "3");
```

### Rules for Sort Key and Distribution Key Interaction

- The `ORDER BY` (sort key) must **start with** the table's key columns (PK / Unique Key).
- The distribution key (`DISTRIBUTED BY HASH(...)`) is independent of the sort key.
- For co-located joins: distribution keys of joined tables must be the same columns with the same bucket count and identical `COLOCATE GROUP`.
- A good sort key has **high selectivity on the leading columns** to enable prefix filtering via the sparse index.

### Choosing Distribution Key

```sql
-- Single large dimension table: hash on PK
DISTRIBUTED BY HASH(user_id) BUCKETS 32

-- Fact table frequently joined to user dimension: co-locate on user_id
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES ("colocate_with" = "user_group")

-- Multi-column distribution for high-cardinality composite keys
DISTRIBUTED BY HASH(store_id, product_id) BUCKETS 16
```

**Bucket count guidelines:**
- Each bucket should hold 1–10 GB of uncompressed data.
- Use powers of 2 for even distribution.
- StarRocks 3.1+ supports `DISTRIBUTED BY HASH(col) BUCKETS AUTO` for automatic sizing.

---

## 7. Data Type Constraints for Key Columns

### Supported Key Column Types

| Category | Types Allowed as Key | Notes |
|---|---|---|
| Integer | `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `LARGEINT` | All integer types valid |
| String | `CHAR(N)`, `VARCHAR(N)` | N must be <= 65533 bytes |
| Date/Time | `DATE`, `DATETIME` | Stored as fixed-width integers internally |
| Decimal | `DECIMAL(P, S)` | Fully supported as key |
| Boolean | `BOOLEAN` | Stored as TINYINT(1) |

### Forbidden Key Column Types

| Type | Reason |
|---|---|
| `FLOAT` | IEEE 754 imprecision makes equality comparison unreliable for deduplication |
| `DOUBLE` | Same reason as FLOAT — cannot guarantee exact key matching |
| `JSON` | Variable-structure, not sortable by definition |
| `ARRAY<T>` | Not sortable; no canonical representation |
| `MAP<K,V>` | Not sortable; no canonical representation |
| `STRUCT<...>` | Not sortable |
| `HLL` | Sketch type, not a business key |
| `BITMAP` | Sketch type, not a business key |
| `PERCENTILE` | Sketch type |

```sql
-- WRONG: FLOAT in key column
CREATE TABLE bad_example (
    metric_value FLOAT NOT NULL,   -- ERROR: FLOAT cannot be key column
    label        VARCHAR(64)
)
DUPLICATE KEY(metric_value);       -- will fail at CREATE TABLE time

-- CORRECT: use DECIMAL for numeric keys
CREATE TABLE good_example (
    price_usd DECIMAL(12, 4) NOT NULL,
    label     VARCHAR(64)
)
DUPLICATE KEY(price_usd);
```

### NULL Handling in Key Columns
- Key columns in `AGGREGATE KEY`, `UNIQUE KEY`, and `PRIMARY KEY` must be declared `NOT NULL`.
- `DUPLICATE KEY` columns may be nullable, but NULL values will all sort to the front and be treated as equal for sort purposes (not for logical identity).

---

## 8. ALTER TABLE

### Add a Column

```sql
-- Add a column to the end of the schema (any table type)
ALTER TABLE dim.user_profile
ADD COLUMN preferred_language VARCHAR(8) DEFAULT 'en' COMMENT 'ISO 639-1 language code'
AFTER country_code;

-- Add multiple columns atomically
ALTER TABLE dwd.orders
ADD COLUMN (
    carrier_code    VARCHAR(32) COMMENT 'Shipping carrier',
    tracking_number VARCHAR(128) COMMENT 'Shipment tracking'
);
```

### Modify a Column (Type Widening Only)

StarRocks allows **widening** type changes only (e.g., INT → BIGINT, VARCHAR(32) → VARCHAR(256)):

```sql
ALTER TABLE dim.user_profile
MODIFY COLUMN phone VARCHAR(64);   -- widened from VARCHAR(32)
```

Type **narrowing** is not allowed (BIGINT → INT will fail). Changing data types that alter storage layout (e.g., INT to DECIMAL) requires a full table rebuild.

### Change Column Order

```sql
-- Move a column to a specific position
ALTER TABLE dim.user_profile
ORDER BY (user_id, email, username, phone, country_code, preferred_language,
          signup_date, last_login_date, tier, lifetime_orders, lifetime_revenue,
          is_active, updated_at);
```

### Drop a Column

```sql
-- Cannot drop key columns; can drop value columns
ALTER TABLE dim.user_profile
DROP COLUMN phone;
```

### Rename a Table

```sql
ALTER TABLE dim.user_profile RENAME dim.user_profiles_v2;
```

### Modify Table Properties

```sql
-- Change replication factor
ALTER TABLE dwd.orders
SET ("replication_num" = "2");

-- Enable persistent index after creation (Primary Key tables only)
ALTER TABLE dwd.orders
SET ("enable_persistent_index" = "true");

-- Change compression algorithm
ALTER TABLE ods.page_events
SET ("compression" = "ZSTD");
```

### Constraints on ALTER TABLE
- **Key columns cannot be dropped or reordered** in Aggregate, Unique, or Primary Key tables.
- **Key column types cannot be changed** once the table is created.
- Adding a column in Aggregate Key tables **requires specifying an aggregate function**.
- Schema changes are online (non-blocking reads) but may temporarily increase resource usage.

```sql
-- Adding a column to Aggregate Key table requires SUM/MAX/MIN/etc.
ALTER TABLE dws.daily_user_metrics
ADD COLUMN add_to_cart_count INT SUM DEFAULT "0"
    COMMENT 'Cart addition events';
```

---

## 9. Production Examples

### (a) Event Log Table — Duplicate Key

```sql
-- High-throughput append-only log table
-- Use case: Kafka → Flink/Stream Load → StarRocks ods layer
CREATE TABLE IF NOT EXISTS ods.app_events (
    log_date         DATE          NOT NULL COMMENT 'Partition date (UTC)',
    event_ts         DATETIME      NOT NULL COMMENT 'Event timestamp',
    event_id         VARCHAR(36)   NOT NULL COMMENT 'UUID from producer',
    app_id           INT           NOT NULL COMMENT 'Application identifier',
    user_id          BIGINT                 COMMENT 'User ID (null for anon)',
    event_name       VARCHAR(128)  NOT NULL COMMENT 'Event name/type',
    event_category   VARCHAR(64)            COMMENT 'Category grouping',
    platform         VARCHAR(16)            COMMENT 'ios|android|web',
    app_version      VARCHAR(32)            COMMENT 'Semver app version',
    properties       JSON                   COMMENT 'Arbitrary event properties',
    device_id        VARCHAR(64)            COMMENT 'Device fingerprint',
    session_id       VARCHAR(64)            COMMENT 'Session identifier',
    ip_address       VARCHAR(45)            COMMENT 'Client IP',
    server_ts        DATETIME      NOT NULL COMMENT 'Server receipt time',
    ingest_ts        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(log_date, event_ts, event_id)
COMMENT 'Raw application events — Duplicate Key, append-only'
PARTITION BY RANGE(log_date) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(user_id, event_id) BUCKETS 48
ORDER BY (log_date, event_ts, event_id)
PROPERTIES (
    "replication_num"             = "3",
    "storage_medium"              = "SSD",
    "compression"                 = "LZ4",
    "bloom_filter_columns"        = "user_id,device_id",
    "dynamic_partition.enable"    = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start"     = "-7",
    "dynamic_partition.end"       = "3",
    "dynamic_partition.prefix"    = "p",
    "dynamic_partition.buckets"   = "48"
);
```

### (b) Daily Metrics Table — Aggregate Key

```sql
-- Pre-aggregated daily revenue and engagement metrics
-- Loaded from ods layer by daily Airflow job or Flink window
CREATE TABLE IF NOT EXISTS dws.daily_store_metrics (
    report_date     DATE         NOT NULL COMMENT 'Reporting date',
    store_id        INT          NOT NULL COMMENT 'Store identifier',
    product_id      BIGINT       NOT NULL COMMENT 'Product identifier',
    channel         VARCHAR(32)  NOT NULL COMMENT 'web|app|partner',

    order_count     BIGINT       SUM     DEFAULT "0",
    gmv_cents       BIGINT       SUM     DEFAULT "0"   COMMENT 'Gross merchandise value (cents)',
    revenue_cents   BIGINT       SUM     DEFAULT "0"   COMMENT 'Net revenue after discounts',
    return_count    INT          SUM     DEFAULT "0",
    return_amount   BIGINT       SUM     DEFAULT "0",
    avg_score       DECIMAL(5,2) MAX     DEFAULT "0"   COMMENT 'Highest review score seen',
    min_price_cents BIGINT       MIN     DEFAULT "0"   COMMENT 'Lowest price offered',

    unique_buyers   HLL          HLL_UNION             COMMENT 'Approx. distinct buyers',
    buyer_bitmap    BITMAP       BITMAP_UNION          COMMENT 'Exact buyer set',

    last_updated    DATETIME     REPLACE               COMMENT 'Latest batch load time'
)
AGGREGATE KEY(report_date, store_id, product_id, channel)
COMMENT 'Daily store-product-channel metrics rollup — Aggregate Key'
PARTITION BY RANGE(report_date) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(store_id, product_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "storage_medium"  = "SSD",
    "compression"     = "ZSTD"
);
```

### (c) User Dimension Table — Unique Key

```sql
-- Snapshot-loaded user dimension; no DELETE needed, latest-write-wins
-- Refreshed daily from DWH or via CDC snapshot
CREATE TABLE IF NOT EXISTS dim.dim_user (
    user_id          BIGINT       NOT NULL COMMENT 'Surrogate user key',
    external_id      VARCHAR(128)          COMMENT 'Source system ID',
    email            VARCHAR(256) NOT NULL COMMENT 'Email address',
    full_name        VARCHAR(256)          COMMENT 'Display name',
    date_of_birth    DATE                  COMMENT 'User DoB',
    gender           CHAR(1)               COMMENT 'M|F|O|U',
    country_code     CHAR(2)               COMMENT 'ISO 3166-1',
    city             VARCHAR(128)          COMMENT 'City of residence',
    signup_date      DATE         NOT NULL COMMENT 'Account signup date',
    acquisition_src  VARCHAR(64)           COMMENT 'organic|paid|referral|social',
    tier             VARCHAR(16)           COMMENT 'bronze|silver|gold|platinum',
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,
    is_verified      BOOLEAN      NOT NULL DEFAULT FALSE,
    total_orders     INT          NOT NULL DEFAULT 0,
    lifetime_value   DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT 'LTV USD',
    last_order_date  DATE                  COMMENT 'Most recent order',
    record_source    VARCHAR(64)           COMMENT 'ETL source pipeline',
    loaded_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(user_id)
COMMENT 'User dimension — Unique Key for daily snapshot deduplication'
DISTRIBUTED BY HASH(user_id) BUCKETS 16
ORDER BY (user_id)
PROPERTIES (
    "replication_num"      = "3",
    "storage_medium"       = "SSD",
    "compression"          = "LZ4",
    "bloom_filter_columns" = "email,external_id"
);
```

### (d) Orders CDC Table — Primary Key

```sql
-- CDC target table receiving INSERT/UPDATE/DELETE from upstream OLTP (MySQL/Postgres)
-- Ingested via Flink CDC or Stream Load with __op column
CREATE TABLE IF NOT EXISTS dwd.fact_orders (
    order_id         BIGINT        NOT NULL COMMENT 'Source PK',
    user_id          BIGINT        NOT NULL COMMENT 'Buyer user ID',
    store_id         INT           NOT NULL COMMENT 'Fulfilling store ID',
    order_date       DATE          NOT NULL COMMENT 'Order placement date',
    order_status     VARCHAR(32)   NOT NULL COMMENT 'placed|confirmed|shipped|delivered|cancelled|refunded',
    payment_status   VARCHAR(32)   NOT NULL COMMENT 'pending|authorized|captured|refunded',
    currency_code    CHAR(3)       NOT NULL DEFAULT 'USD',
    subtotal_cents   BIGINT        NOT NULL DEFAULT 0,
    discount_cents   BIGINT        NOT NULL DEFAULT 0,
    shipping_cents   BIGINT        NOT NULL DEFAULT 0,
    tax_cents        BIGINT        NOT NULL DEFAULT 0,
    total_cents      BIGINT        NOT NULL DEFAULT 0 COMMENT 'subtotal - discount + shipping + tax',
    item_count       INT           NOT NULL DEFAULT 0,
    shipping_method  VARCHAR(32)            COMMENT 'standard|express|overnight',
    carrier_code     VARCHAR(32)            COMMENT 'Shipping carrier',
    tracking_number  VARCHAR(128)           COMMENT 'Carrier tracking number',
    shipping_address VARCHAR(512)           COMMENT 'Delivery address JSON',
    placed_at        DATETIME      NOT NULL COMMENT 'Order creation time (source)',
    confirmed_at     DATETIME               COMMENT 'Confirmation time',
    shipped_at       DATETIME               COMMENT 'Shipment time',
    delivered_at     DATETIME               COMMENT 'Delivery time',
    cancelled_at     DATETIME               COMMENT 'Cancellation time (if any)',
    updated_at       DATETIME      NOT NULL COMMENT 'CDC source update time'
)
PRIMARY KEY(order_id)
COMMENT 'Orders fact — Primary Key for real-time CDC from OLTP source'
PARTITION BY RANGE(order_date) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(order_id) BUCKETS 32
ORDER BY (order_id, order_date)
PROPERTIES (
    "replication_num"         = "3",
    "storage_medium"          = "SSD",
    "compression"             = "LZ4",
    "enable_persistent_index" = "true",
    "primary_key_cache_size"  = "4294967296",
    "bloom_filter_columns"    = "user_id,store_id"
);
```

---

## 10. Anti-Patterns

### 1. Using Duplicate Key When Deduplication Is Required
```sql
-- WRONG: Expecting Duplicate Key to deduplicate rows — it does not
-- Same order_id will appear multiple times
CREATE TABLE orders_bad (order_id BIGINT, ...)
DUPLICATE KEY(order_id);

-- CORRECT: Use Unique Key or Primary Key
CREATE TABLE orders_good (order_id BIGINT, ...)
UNIQUE KEY(order_id);
```

### 2. Using Aggregate Key for Data That Requires Exact Replay
```sql
-- WRONG: Loading the same batch twice into an Aggregate Key table with SUM
-- SUM will double-count; MIN/MAX may not re-aggregate correctly after idempotent reload
INSERT INTO dws.daily_store_metrics SELECT ... FROM stage.metrics WHERE batch = 42;
-- Reload same batch:
INSERT INTO dws.daily_store_metrics SELECT ... FROM stage.metrics WHERE batch = 42;
-- Result: order_count, revenue_cents are now double what they should be

-- CORRECT: Deduplicate upstream, or use a staging table + TRUNCATE PARTITION before reload
TRUNCATE TABLE dws.daily_store_metrics PARTITION (p202506);
INSERT INTO dws.daily_store_metrics SELECT ... FROM stage.metrics WHERE report_date = '2025-06-15';
```

### 3. Float/Double in Key Columns
```sql
-- WRONG: DOUBLE in PRIMARY KEY
CREATE TABLE sensor_data (
    sensor_id    INT     NOT NULL,
    reading_val  DOUBLE  NOT NULL,   -- ERROR at CREATE TABLE
    ...
)
PRIMARY KEY(sensor_id, reading_val);

-- CORRECT: DECIMAL for numeric precision keys
CREATE TABLE sensor_data (
    sensor_id   INT           NOT NULL,
    reading_val DECIMAL(12,4) NOT NULL,
    ...
)
PRIMARY KEY(sensor_id, reading_val);
```

### 4. Forgetting `enable_persistent_index` on Large Primary Key Tables
```sql
-- WRONG: Large PK table without persistent index
-- BE will hold 10B+ row PK index entirely in memory → OOM
CREATE TABLE huge_events (...) PRIMARY KEY(event_id)
PROPERTIES ("replication_num" = "3");  -- missing enable_persistent_index

-- CORRECT
PROPERTIES (
    "replication_num"         = "3",
    "enable_persistent_index" = "true"
);
```

### 5. Wrong Sort Key for Primary Key Tables
```sql
-- WRONG: ORDER BY does not start with PRIMARY KEY columns
CREATE TABLE dwd.orders (
    order_id   BIGINT NOT NULL,
    order_date DATE   NOT NULL,
    ...
)
PRIMARY KEY(order_id)
ORDER BY (order_date, order_id);  -- ERROR: order_date before order_id, but order_id is the PK

-- CORRECT: ORDER BY must start with PK columns
ORDER BY (order_id, order_date)
```

### 6. Using Unique Key When DELETE Is Needed
```sql
-- WRONG: Attempting DELETE on Unique Key table
DELETE FROM dim.dim_product WHERE product_id = 99;
-- ERROR: DELETE is not supported on Unique Key tables

-- CORRECT: Use Primary Key if DELETE support is required
CREATE TABLE dim.dim_product (...) PRIMARY KEY(product_id) ...;
DELETE FROM dim.dim_product WHERE product_id = 99;  -- works
```

### 7. Too Many or Too Few Buckets
```sql
-- WRONG: 1 bucket for a large event table — creates hot spot
DISTRIBUTED BY HASH(user_id) BUCKETS 1;

-- WRONG: 1024 buckets for a small 10 MB dimension table — too many small tablets
DISTRIBUTED BY HASH(user_id) BUCKETS 1024;

-- CORRECT: Target 1–10 GB of uncompressed data per bucket
-- For a 100 GB table: 16–64 buckets is a reasonable starting point
DISTRIBUTED BY HASH(user_id) BUCKETS 32;
```

### 8. Missing NOT NULL on Key Columns for Aggregate/Unique/Primary Key
```sql
-- WRONG: Nullable PK column
CREATE TABLE dim.user_profile (
    user_id BIGINT COMMENT 'PK',   -- no NOT NULL → nullable by default
    ...
)
PRIMARY KEY(user_id);
-- This will cause issues with CDC operations and delete vector correctness

-- CORRECT
CREATE TABLE dim.user_profile (
    user_id BIGINT NOT NULL,
    ...
)
PRIMARY KEY(user_id);
```

---

## 11. Decision Framework Summary

```
What is the primary write pattern?

1. Append-only (never update/delete rows)?
     → Is there a need to aggregate/pre-roll up metrics at load time?
           YES: Aggregate Key (SUM/MAX/MIN/HLL/BITMAP)
           NO:  Duplicate Key (raw logs, events)

2. Upsert (insert or replace latest version of a row)?
     → Is UPDATE or DELETE required after insert?
           YES: Primary Key (CDC, real-time upsert/delete, streaming)
           NO:  Unique Key (dimension snapshot, dedup by natural key)
```

**Additional signals to use Primary Key over Unique Key:**
- Receiving Debezium/Flink CDC events with `op = 'd'` (deletes)
- Using `UPDATE ... SET col = val WHERE pk = x` in application code
- Streaming pipeline sends partial row updates (only changed columns)

**Additional signals to use Aggregate Key over Duplicate Key + query-time GROUP BY:**
- Source data volume is 10-100x the desired query granularity
- Dashboards query the same daily/hourly aggregation repeatedly
- Downstream query latency requirements are under 100 ms on multi-billion row datasets

---

## References to Consult When Needed

- StarRocks documentation: Table types overview — `https://docs.starrocks.io/docs/table_design/table_types/`
- Duplicate Key table — `https://docs.starrocks.io/docs/table_design/table_types/duplicate_key_table/`
- Aggregate Key table — `https://docs.starrocks.io/docs/table_design/table_types/aggregate_table/`
- Unique Key table — `https://docs.starrocks.io/docs/table_design/table_types/unique_key_table/`
- Primary Key table — `https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/`
- Sort key and prefix index — `https://docs.starrocks.io/docs/table_design/indexes/Prefix_index_sort_key/`
- Data distribution — `https://docs.starrocks.io/docs/table_design/Data_distribution/`
- CREATE TABLE DDL reference — `https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/`
- ALTER TABLE reference — `https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE/`
