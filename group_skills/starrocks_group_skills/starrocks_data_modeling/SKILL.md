---
name: starrocks-data-modeling
description: StarRocks data modeling — star schema vs wide table design, fact/dimension DDL patterns, denormalization strategy for OLAP, bitmap indexes, bloom filter indexes, sort key optimization, schema review checklist, SCD type 2 with Primary Key table, aggregation table design, BI acceleration patterns, avoiding over-normalization
---

# StarRocks Data Modeling

## When to Use

Load this skill when the user needs to:
- Design new StarRocks tables for analytical or BI workloads
- Review an existing StarRocks schema for performance issues
- Decide between star schema, wide table, or aggregation table approaches
- Choose sort keys, distribution keys, partitioning strategies
- Add bitmap or bloom filter indexes to accelerate queries
- Implement SCD Type 2 history tracking with Primary Key tables
- Migrate data models from Hive, Spark, or OLTP databases to StarRocks
- Optimize BI tool query patterns (Superset, Metabase, Tableau, Power BI)

---

## StarRocks OLAP Data Modeling Principles

StarRocks is an MPP analytical database — every design decision must optimize for **scan + aggregate throughput**, not for write-time normalization.

### Key differences from OLTP modeling

| Concern | OLTP (PostgreSQL, MySQL) | StarRocks OLAP |
|---|---|---|
| Normalization | 3NF, minimize redundancy | Denormalize aggressively; redundancy is cheap |
| Joins | Fast FK lookups via indexes | Joins are expensive at billion-row scale; pre-join where possible |
| Indexes | B-Tree on every FK/PK | Sort key (implicit prefix index) + bitmap/bloom filter only |
| Updates | Row-level UPDATE, small txns | Bulk UPSERT via Primary Key table or INSERT OVERWRITE partitions |
| Primary key | Enforced uniqueness constraint | Primary Key table type for upsert; Duplicate Key for append-only |
| Row ordering | Heap-ordered, index-navigated | Always sorted on disk by sort key — scan short-circuits on prefix |

### The three modeling mandates

1. **Denormalize aggressively.** Flatten dimension attributes into the fact row when dimensions are stable and small (< 5M rows). Eliminate the join at query time.
2. **Sort key is your index.** There are no secondary clustered indexes. The sort key determines which granules can be skipped. Filter columns that appear in WHERE and GROUP BY should lead the sort key.
3. **Pre-aggregate where possible.** Use Aggregate Key tables or materialized views to pre-compute SUM/COUNT/MAX/MIN for repeated dashboard queries. Do not rely on raw fact scans for every BI request.

---

## Table Types Quick Reference

| Table Type | Key Purpose | Use When |
|---|---|---|
| **Duplicate Key** | Append-only fact storage | Event logs, raw facts, no deduplication needed |
| **Aggregate Key** | Pre-aggregated summaries | Daily/hourly rollup tables, metrics aggregation |
| **Unique Key** | Upsert with full-row replacement | Dimension tables, slow-changing data, idempotent loads |
| **Primary Key** | Upsert with partial-column update, delete | CDC targets, SCD Type 2, real-time dimension updates |

> **Unique Key vs Primary Key**: Unique Key merges on read (slower queries, faster writes). Primary Key merges on write via delete-bitmap (faster queries, higher write cost). Prefer Primary Key for tables that are queried frequently and updated less often.

---

## Star Schema in StarRocks

StarRocks executes star schema joins efficiently when:
- Dimension tables are small enough for broadcast join (replicated to all BEs)
- Fact table is partitioned on the date dimension key
- Sort key on fact table starts with the most selective filter column

### Fact Table: `fct_orders`

```sql
CREATE TABLE fct_orders (
    order_date          DATE           NOT NULL  COMMENT "Partition key — always first",
    order_id            BIGINT         NOT NULL  COMMENT "Degenerate dimension",
    customer_id         INT            NOT NULL,
    product_id          INT            NOT NULL,
    store_id            SMALLINT       NOT NULL,
    channel             VARCHAR(32)    NOT NULL  COMMENT "online / retail / partner",
    -- Additive measures
    quantity            INT            NOT NULL  DEFAULT 1,
    unit_price          DECIMAL(12,2)  NOT NULL,
    discount_amount     DECIMAL(12,2)  NOT NULL  DEFAULT 0,
    gross_revenue       DECIMAL(14,2)  NOT NULL  COMMENT "quantity * unit_price",
    net_revenue         DECIMAL(14,2)  NOT NULL  COMMENT "gross_revenue - discount_amount",
    -- Metadata
    created_at          DATETIME       NOT NULL,
    load_ts             DATETIME       NOT NULL  DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(order_date, order_id)                 -- sort key prefix; also deduplication hint
PARTITION BY RANGE(order_date) (
    PARTITION p2024_01 VALUES LESS THAN ("2024-02-01"),
    PARTITION p2024_02 VALUES LESS THAN ("2024-03-01"),
    PARTITION p2024_03 VALUES LESS THAN ("2024-04-01")
    -- Use dynamic partition creation in production:
)
DISTRIBUTED BY HASH(customer_id) BUCKETS 64        -- spread reads across BEs
ORDER BY (order_date, customer_id, product_id)      -- StarRocks 3.3+ decoupled sort key
PROPERTIES (
    "replication_num" = "3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "2",
    "dynamic_partition.prefix" = "p",
    "bloom_filter_columns" = "customer_id,product_id,order_id",
    "compression" = "LZ4"
);
```

Key decisions explained:
- `DUPLICATE KEY(order_date, order_id)` — the first two columns declare the sort key prefix. StarRocks builds a sparse prefix index on these columns automatically.
- `DISTRIBUTED BY HASH(customer_id)` — distributes rows evenly and collocates rows for the same customer on the same BE, accelerating per-customer aggregations.
- `ORDER BY` (3.3+) — decouples the sort key from the distribution key, allowing independent optimization.
- `bloom_filter_columns` — equality filter acceleration for high-cardinality FK columns.
- Dynamic partitioning — auto-creates monthly partitions without manual DDL.

### Dimension Table: `dim_customers`

```sql
CREATE TABLE dim_customers (
    customer_id         INT            NOT NULL,
    customer_key        VARCHAR(64)    NOT NULL  COMMENT "Business natural key",
    full_name           VARCHAR(256)   NOT NULL,
    email               VARCHAR(256),
    phone               VARCHAR(32),
    country_code        CHAR(2)        NOT NULL,
    region              VARCHAR(64),
    city                VARCHAR(128),
    segment             VARCHAR(32)    NOT NULL  COMMENT "SMB / Enterprise / Consumer",
    acquisition_channel VARCHAR(64),
    first_order_date    DATE,
    customer_tier       TINYINT        NOT NULL  DEFAULT 1  COMMENT "1=Bronze 2=Silver 3=Gold",
    is_active           BOOLEAN        NOT NULL  DEFAULT TRUE,
    updated_at          DATETIME       NOT NULL
)
PRIMARY KEY(customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 8         -- small table: fewer buckets
PROPERTIES (
    "replication_num" = "3",                        -- replicate to all BEs for broadcast join
    "bloom_filter_columns" = "customer_key,email"
);

-- Bitmap index for low-cardinality filter columns
CREATE INDEX ix_dim_customers_segment   ON dim_customers (segment)       USING BITMAP;
CREATE INDEX ix_dim_customers_country   ON dim_customers (country_code)  USING BITMAP;
CREATE INDEX ix_dim_customers_tier      ON dim_customers (customer_tier) USING BITMAP;
```

Key decisions:
- `PRIMARY KEY` — enables UPSERT loads from CDC or periodic full refresh.
- Small bucket count (8) — dimension fits in memory; fewer buckets reduces overhead.
- `"replication_num" = "3"` — with 3 BEs all replicas are local; StarRocks picks broadcast join automatically.

### Dimension Table: `dim_products`

```sql
CREATE TABLE dim_products (
    product_id          INT            NOT NULL,
    product_key         VARCHAR(64)    NOT NULL  COMMENT "SKU or EAN",
    product_name        VARCHAR(256)   NOT NULL,
    brand               VARCHAR(128),
    category_l1         VARCHAR(64)    NOT NULL  COMMENT "Top-level category",
    category_l2         VARCHAR(64),
    category_l3         VARCHAR(64),
    unit_cost           DECIMAL(12,2),
    list_price          DECIMAL(12,2),
    weight_kg           DECIMAL(8,3),
    is_active           BOOLEAN        NOT NULL  DEFAULT TRUE,
    introduced_date     DATE,
    updated_at          DATETIME       NOT NULL
)
PRIMARY KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "3",
    "bloom_filter_columns" = "product_key"
);

CREATE INDEX ix_dim_products_category ON dim_products (category_l1) USING BITMAP;
CREATE INDEX ix_dim_products_brand    ON dim_products (brand)        USING BITMAP;
```

### Join query leveraging the star schema

```sql
-- BI query: monthly revenue by customer segment and product category
-- StarRocks chooses BROADCAST join for dim tables automatically
SELECT
    DATE_TRUNC('month', o.order_date)  AS month,
    c.segment,
    p.category_l1,
    COUNT(DISTINCT o.order_id)         AS order_count,
    SUM(o.net_revenue)                 AS total_revenue,
    AVG(o.net_revenue)                 AS avg_order_value
FROM fct_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN dim_products  p ON o.product_id  = p.product_id
WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
  AND c.is_active = TRUE
GROUP BY 1, 2, 3
ORDER BY 1, total_revenue DESC;
```

---

## Wide Table Strategy

### When to denormalize into a wide table

Flatten dimension attributes directly into the fact table when:
- Dimension has fewer than 1 million rows and changes infrequently (weekly or less)
- Every query joins the same small set of dimensions
- BI tool generates large sequential scans (Tableau extracts, Power BI DirectQuery)
- The team wants to expose a single table to the BI layer (simplifies governance)
- Dimension attributes are needed for aggregation (GROUP BY segment, category) more than for lookup

### Wide table DDL: `wide_orders`

```sql
CREATE TABLE wide_orders (
    order_date          DATE           NOT NULL,
    order_id            BIGINT         NOT NULL,
    -- Fact measures
    quantity            INT            NOT NULL,
    unit_price          DECIMAL(12,2)  NOT NULL,
    discount_amount     DECIMAL(12,2)  NOT NULL  DEFAULT 0,
    gross_revenue       DECIMAL(14,2)  NOT NULL,
    net_revenue         DECIMAL(14,2)  NOT NULL,
    -- Customer dimension (flattened)
    customer_id         INT            NOT NULL,
    customer_segment    VARCHAR(32)    NOT NULL,
    customer_country    CHAR(2)        NOT NULL,
    customer_region     VARCHAR(64),
    customer_tier       TINYINT        NOT NULL,
    -- Product dimension (flattened)
    product_id          INT            NOT NULL,
    product_brand       VARCHAR(128),
    product_category_l1 VARCHAR(64)    NOT NULL,
    product_category_l2 VARCHAR(64),
    -- Store dimension (flattened)
    store_id            SMALLINT       NOT NULL,
    store_city          VARCHAR(128),
    store_country       CHAR(2)        NOT NULL,
    channel             VARCHAR(32)    NOT NULL,
    -- Metadata
    created_at          DATETIME       NOT NULL
)
DUPLICATE KEY(order_date, order_id)
PARTITION BY RANGE(order_date) (
    START ("2023-01-01") END ("2026-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(customer_id) BUCKETS 96
ORDER BY (order_date, customer_segment, product_category_l1)
PROPERTIES (
    "replication_num" = "3",
    "bloom_filter_columns" = "customer_id,product_id,order_id",
    "compression" = "LZ4"
);

-- Bitmap indexes for BI filter columns
CREATE INDEX ix_wo_segment    ON wide_orders (customer_segment)    USING BITMAP;
CREATE INDEX ix_wo_country    ON wide_orders (customer_country)    USING BITMAP;
CREATE INDEX ix_wo_channel    ON wide_orders (channel)             USING BITMAP;
CREATE INDEX ix_wo_category   ON wide_orders (product_category_l1) USING BITMAP;
CREATE INDEX ix_wo_tier       ON wide_orders (customer_tier)       USING BITMAP;
```

### Loading the wide table from star schema

```sql
-- Full monthly refresh — use INSERT OVERWRITE on target partition
INSERT OVERWRITE wide_orders PARTITION (order_date >= '2024-12-01' AND order_date < '2025-01-01')
SELECT
    o.order_date,
    o.order_id,
    o.quantity,
    o.unit_price,
    o.discount_amount,
    o.gross_revenue,
    o.net_revenue,
    o.customer_id,
    c.segment          AS customer_segment,
    c.country_code     AS customer_country,
    c.region           AS customer_region,
    c.customer_tier,
    o.product_id,
    p.brand            AS product_brand,
    p.category_l1      AS product_category_l1,
    p.category_l2      AS product_category_l2,
    o.store_id,
    s.city             AS store_city,
    s.country_code     AS store_country,
    o.channel,
    o.created_at
FROM fct_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN dim_products  p ON o.product_id  = p.product_id
JOIN dim_stores    s ON o.store_id    = s.store_id
WHERE o.order_date >= '2024-12-01'
  AND o.order_date <  '2025-01-01';
```

### Trade-offs

| Aspect | Wide Table | Star Schema |
|---|---|---|
| Query complexity | Single table scan, no joins | Multi-table joins required |
| Query performance (BI) | Faster — no shuffle joins | Slightly slower — join cost |
| Write complexity | Must re-flatten on dim change | Dimension update is isolated |
| Storage | Higher (repeated dim values) | Lower (dim stored once) |
| Dimension freshness | Stale until reload | Always current |
| Concurrent grain dimensions | Hard to support | Clean separate fact tables |

### When NOT to use a wide table

- Dimension attributes change frequently (e.g., customer tier recalculated daily) — every fact partition needs rebuild
- Multiple conflicting grains exist (order-level + order-line-level measures) — wide table cannot cleanly represent both without row explosion
- Dimension has > 5M rows — flattening loses storage efficiency with no join savings (StarRocks broadcast join threshold)
- Strict late-binding data contracts — dim schema changes cascade to all wide tables

---

## Sort Key Optimization

The sort key is the single most impactful design decision in StarRocks. Every granule (1024 rows by default) has a min/max entry in the sparse prefix index. A query that matches the sort key prefix can skip all non-matching granules before reading any data.

### How prefix index skipping works

```
Sort key: (order_date, customer_segment, channel)

Query: WHERE order_date = '2024-06-15' AND customer_segment = 'Enterprise'
→ StarRocks reads prefix index, skips all granules outside the matching range
→ Only reads granules where order_date = '2024-06-15' AND customer_segment = 'Enterprise'

Query: WHERE channel = 'online'           -- no sort key prefix match
→ Full table scan (no prefix skipping)
→ Bloom filter on channel may help if bloom_filter_columns includes channel
```

### Sort key selection rules

1. **Time column first** when most queries filter on a date range. This is the most common access pattern in OLAP.
2. **Most selective equality filter second** — cardinality should be low-to-medium so each distinct value spans many rows (good for range scan).
3. **GROUP BY columns third** — if aggregation is after the filter, putting GROUP BY columns in the sort key allows the engine to aggregate sorted runs cheaply.
4. **Avoid high-cardinality columns in sort key** — `order_id` (UUID-like) as the first sort key column randomizes sort order and defeats range scan. Use it in bloom filter instead.
5. **Maximum 3–5 columns in sort key** — additional columns have diminishing returns and increase memory for the index.

### StarRocks 3.3+ decoupled sort key

Before 3.3, `DUPLICATE KEY` / `AGGREGATE KEY` / `UNIQUE KEY` implied both the sort key and the uniqueness key. StarRocks 3.3 introduces the `ORDER BY` clause that decouples them:

```sql
-- Sort key (ORDER BY) is optimized for query filter patterns
-- DUPLICATE KEY only declares the leading key columns for the deduplication hint
CREATE TABLE events (
    event_date   DATE        NOT NULL,
    user_id      BIGINT      NOT NULL,
    session_id   VARCHAR(64) NOT NULL,
    event_type   VARCHAR(64) NOT NULL,
    page_url     VARCHAR(512),
    duration_ms  INT,
    created_at   DATETIME    NOT NULL
)
DUPLICATE KEY(event_date, user_id)              -- legacy key declaration (first 2 cols)
DISTRIBUTED BY HASH(user_id) BUCKETS 128
ORDER BY (event_date, event_type, user_id)      -- true sort key — decoupled in 3.3+
PROPERTIES ("replication_num" = "3");
-- event_type in position 2 of sort key even though it's not in DUPLICATE KEY prefix
```

### Sort key examples by use case

```sql
-- Time-series events (most queries filter by date + event_type)
ORDER BY (event_date, event_type, user_id)

-- Sales fact (filter by date, segment GROUP BY)
ORDER BY (order_date, customer_segment, product_category_l1)

-- Log analytics (filter by date + severity + service)
ORDER BY (log_date, severity, service_name)

-- User behavior (filter by date + country, then GROUP BY user)
ORDER BY (activity_date, country_code, user_id)
```

---

## Bitmap Index

### Purpose and mechanics

A bitmap index stores one bit per row for each distinct value of a column. For a column `status` with values `{active, inactive, pending}`, StarRocks maintains three bitmaps. A query `WHERE status = 'active'` becomes a single bitmap lookup; `WHERE status IN ('active', 'pending')` is a bitwise OR.

Bitmap indexes accelerate:
- Point filters on low-cardinality columns (`WHERE segment = 'SMB'`)
- IN-list filters (`WHERE channel IN ('online', 'mobile')`)
- Combined multi-column filters via bitwise AND/OR across multiple bitmap indexes
- `COUNT DISTINCT` on low-cardinality columns (bitmap cardinality estimation)

### When to use bitmap index

- Column has **fewer than 100,000 distinct values** (low to medium cardinality)
- Column appears frequently in WHERE clauses or GROUP BY
- Column is not the leading sort key column (sort key already handles prefix scans efficiently)

### DDL syntax

```sql
-- At table creation
CREATE TABLE fct_sessions (
    session_date    DATE         NOT NULL,
    session_id      BIGINT       NOT NULL,
    user_id         BIGINT       NOT NULL,
    platform        VARCHAR(32)  NOT NULL,   -- ios / android / web / desktop
    country_code    CHAR(2)      NOT NULL,
    browser         VARCHAR(64),
    os              VARCHAR(64),
    entry_channel   VARCHAR(64),
    session_duration_s INT,
    page_views      INT,
    INDEX ix_platform     (platform)     USING BITMAP,
    INDEX ix_country      (country_code) USING BITMAP,
    INDEX ix_os           (os)           USING BITMAP,
    INDEX ix_entry_channel(entry_channel) USING BITMAP
)
DUPLICATE KEY(session_date, session_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 64
PROPERTIES ("replication_num" = "3");

-- Post-creation (ALTER TABLE)
ALTER TABLE fct_sessions ADD INDEX ix_browser (browser) USING BITMAP;
```

### Bitmap index limitations

- Do NOT use bitmap indexes on high-cardinality columns (UUIDs, timestamps, free-text) — the index becomes larger than the data.
- Bitmap indexes consume memory during query execution (decompressed bitmaps for matching granules).
- Building a bitmap index at creation time is free; adding via ALTER rebuilds the table column (can take minutes on large tables).

---

## Bloom Filter Index

### Purpose and mechanics

A bloom filter is a probabilistic data structure stored per granule (1024-row block). When a query filters `WHERE user_id = 12345`, StarRocks checks the bloom filter for each granule: if the filter says "definitely not present", the granule is skipped entirely without reading any data. False positives are possible (granule is read but contains no match), but false negatives are not (no valid rows are ever skipped).

Bloom filters accelerate:
- Equality filters on high-cardinality columns (`WHERE user_id = ?`, `WHERE order_id = ?`)
- Multi-column equality filters (each column has its own bloom filter)
- Point lookups in large tables that cannot use sort key prefix skipping

### When to use bloom filter

- Column has **high cardinality** (user IDs, order IDs, UUIDs, email addresses)
- Column is frequently used in `WHERE col = value` equality filters
- Column is NOT in the leading sort key (if it's sort key col 1, prefix skipping already handles it)
- Acceptable memory overhead: ~10 bits per row per column at 1% false positive rate

### DDL syntax

```sql
-- Specified as a table PROPERTY (not an index DDL statement)
CREATE TABLE fct_orders (
    order_date   DATE        NOT NULL,
    order_id     BIGINT      NOT NULL,
    customer_id  INT         NOT NULL,
    product_id   INT         NOT NULL,
    session_id   BIGINT,
    gross_revenue DECIMAL(14,2) NOT NULL
)
DUPLICATE KEY(order_date, order_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 64
PROPERTIES (
    "replication_num"      = "3",
    "bloom_filter_columns" = "customer_id,product_id,session_id,order_id"
    --                        ^ comma-separated column names, no spaces around commas
);

-- Modify bloom filter columns on existing table
ALTER TABLE fct_orders SET ("bloom_filter_columns" = "customer_id,product_id,order_id");

-- Remove all bloom filters
ALTER TABLE fct_orders SET ("bloom_filter_columns" = "");
```

### Bloom filter limitations

- Only supports equality predicates (`=`, `IN`). Does NOT accelerate `>`, `<`, `BETWEEN`, `LIKE`.
- Does NOT work on FLOAT or DOUBLE columns (use INT/BIGINT, DECIMAL, VARCHAR, CHAR).
- Memory overhead: approximately `num_rows * num_bf_columns * 10 bits / 8` bytes in total.
- Too many bloom filter columns wastes memory; limit to columns with the highest query frequency.

### Bitmap vs Bloom Filter decision matrix

| Scenario | Use Bitmap | Use Bloom Filter |
|---|---|---|
| `status` column (5 values) | Yes | No |
| `country_code` (200 values) | Yes | No |
| `product_category` (500 values) | Yes | No |
| `user_id` (50M distinct values) | No | Yes |
| `order_id` (1B distinct values) | No | Yes |
| `session_id` UUID | No | Yes |
| `email` (high cardinality) | No | Yes |
| `event_type` (50 values) in WHERE | Yes | No |

---

## SCD Type 2 with Primary Key Table

### Approach: versioned rows in a Primary Key table

StarRocks Primary Key tables support efficient UPSERT (delete old row + insert new row atomically). For SCD Type 2, each version of a dimension record is a separate row identified by a composite key `(natural_key, valid_from)`.

### DDL

```sql
CREATE TABLE dim_customers_scd2 (
    customer_id     INT          NOT NULL  COMMENT "Surrogate key (auto-increment source)",
    customer_key    VARCHAR(64)  NOT NULL  COMMENT "Business natural key",
    valid_from      DATE         NOT NULL  COMMENT "Version start date (inclusive)",
    valid_to        DATE         NOT NULL  COMMENT "Version end date (exclusive); 9999-12-31 = current",
    is_current      BOOLEAN      NOT NULL  DEFAULT TRUE,
    -- Tracked attributes (Type 2 change triggers new row)
    full_name       VARCHAR(256) NOT NULL,
    email           VARCHAR(256),
    segment         VARCHAR(32)  NOT NULL,
    country_code    CHAR(2)      NOT NULL,
    customer_tier   TINYINT      NOT NULL,
    -- Non-tracked attributes (Type 1 — overwrite in place)
    phone           VARCHAR(32),
    updated_at      DATETIME     NOT NULL
)
PRIMARY KEY(customer_id)               -- surrogate key is the PK for UPSERT targeting
DISTRIBUTED BY HASH(customer_id) BUCKETS 8
PROPERTIES (
    "replication_num"      = "3",
    "bloom_filter_columns" = "customer_key"
);

-- Bitmap indexes on commonly filtered SCD attributes
CREATE INDEX ix_scd2_is_current ON dim_customers_scd2 (is_current)   USING BITMAP;
CREATE INDEX ix_scd2_segment    ON dim_customers_scd2 (segment)       USING BITMAP;
CREATE INDEX ix_scd2_country    ON dim_customers_scd2 (country_code)  USING BITMAP;
```

### Load pattern: close existing row + insert new version

```sql
-- Step 1: Stage incoming changes in a temporary table
CREATE TABLE tmp_customer_changes LIKE dim_customers_scd2;

INSERT INTO tmp_customer_changes
    (customer_id, customer_key, valid_from, valid_to, is_current,
     full_name, email, segment, country_code, customer_tier, phone, updated_at)
SELECT
    src.customer_id,
    src.customer_key,
    CURDATE()       AS valid_from,
    '9999-12-31'    AS valid_to,
    TRUE            AS is_current,
    src.full_name,
    src.email,
    src.segment,
    src.country_code,
    src.customer_tier,
    src.phone,
    NOW()           AS updated_at
FROM staging_customers src
JOIN dim_customers_scd2 cur
  ON src.customer_key = cur.customer_key
 AND cur.is_current   = TRUE
WHERE src.segment       <> cur.segment        -- tracked attribute changed
   OR src.country_code  <> cur.country_code
   OR src.customer_tier <> cur.customer_tier
   OR src.full_name     <> cur.full_name;

-- Step 2: Close the current row (UPSERT updates is_current and valid_to in place)
UPDATE dim_customers_scd2 tgt
SET    valid_to    = DATE_SUB(CURDATE(), INTERVAL 1 DAY),
       is_current  = FALSE
WHERE  tgt.customer_key IN (SELECT customer_key FROM tmp_customer_changes)
  AND  tgt.is_current   = TRUE;

-- Step 3: Insert new version rows
INSERT INTO dim_customers_scd2
SELECT * FROM tmp_customer_changes;

-- Step 4: Insert brand-new customers (not previously in dimension)
INSERT INTO dim_customers_scd2
    (customer_id, customer_key, valid_from, valid_to, is_current,
     full_name, email, segment, country_code, customer_tier, phone, updated_at)
SELECT
    src.customer_id,
    src.customer_key,
    CURDATE()       AS valid_from,
    '9999-12-31'    AS valid_to,
    TRUE            AS is_current,
    src.full_name,
    src.email,
    src.segment,
    src.country_code,
    src.customer_tier,
    src.phone,
    NOW()
FROM staging_customers src
WHERE NOT EXISTS (
    SELECT 1
    FROM   dim_customers_scd2 d
    WHERE  d.customer_key = src.customer_key
);
```

### Point-in-time join from fact table

```sql
-- Join fact to the historically correct dimension version
SELECT
    o.order_date,
    c.segment                          AS customer_segment_at_order_time,
    SUM(o.net_revenue)                 AS revenue
FROM fct_orders o
JOIN dim_customers_scd2 c
  ON o.customer_id = c.customer_id
 AND o.order_date >= c.valid_from
 AND o.order_date <  c.valid_to
GROUP BY 1, 2
ORDER BY 1;
```

---

## Aggregation Table Design

Use the **Aggregate Key** table type to pre-compute SUM/COUNT/MAX/MIN/HLL/BITMAP aggregations at load time. Queries that match the aggregation grain are answered entirely from the aggregate table — no raw fact scan needed.

### Daily sales summary: `agg_daily_sales`

```sql
CREATE TABLE agg_daily_sales (
    sale_date           DATE         NOT NULL,
    customer_segment    VARCHAR(32)  NOT NULL,
    product_category_l1 VARCHAR(64)  NOT NULL,
    product_category_l2 VARCHAR(64)  NOT NULL  DEFAULT '',
    country_code        CHAR(2)      NOT NULL,
    channel             VARCHAR(32)  NOT NULL,
    -- Pre-aggregated measures (aggregation function is MANDATORY in AGGREGATE KEY table)
    order_count         BIGINT       SUM        NOT NULL  DEFAULT "0",
    order_line_count    BIGINT       SUM        NOT NULL  DEFAULT "0",
    gross_revenue       DECIMAL(18,2) SUM       NOT NULL  DEFAULT "0",
    net_revenue         DECIMAL(18,2) SUM       NOT NULL  DEFAULT "0",
    discount_amount     DECIMAL(18,2) SUM       NOT NULL  DEFAULT "0",
    total_quantity      BIGINT       SUM        NOT NULL  DEFAULT "0",
    distinct_customers  BIGINT       HLL_UNION  NOT NULL  COMMENT "Approx distinct count",
    max_order_value     DECIMAL(14,2) MAX       NOT NULL  DEFAULT "0"
)
AGGREGATE KEY(sale_date, customer_segment, product_category_l1,
              product_category_l2, country_code, channel)
PARTITION BY RANGE(sale_date) (
    START ("2023-01-01") END ("2026-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(customer_segment, country_code) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "compression"     = "LZ4"
);

-- Load from fact table (daily batch)
INSERT INTO agg_daily_sales
SELECT
    o.order_date                            AS sale_date,
    c.segment                               AS customer_segment,
    p.category_l1                           AS product_category_l1,
    COALESCE(p.category_l2, '')             AS product_category_l2,
    c.country_code,
    o.channel,
    COUNT(DISTINCT o.order_id)              AS order_count,
    COUNT(*)                                AS order_line_count,
    SUM(o.gross_revenue)                    AS gross_revenue,
    SUM(o.net_revenue)                      AS net_revenue,
    SUM(o.discount_amount)                  AS discount_amount,
    SUM(o.quantity)                         AS total_quantity,
    HLL_HASH(o.customer_id)                 AS distinct_customers,
    MAX(o.net_revenue)                      AS max_order_value
FROM fct_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN dim_products  p ON o.product_id  = p.product_id
WHERE o.order_date = CURDATE() - INTERVAL 1 DAY   -- yesterday's data
GROUP BY 1, 2, 3, 4, 5, 6;
```

### Querying the aggregation table

```sql
-- BI query answered entirely from agg table — no fact scan
SELECT
    DATE_TRUNC('month', sale_date)   AS month,
    customer_segment,
    country_code,
    SUM(order_count)                 AS orders,
    SUM(net_revenue)                 AS revenue,
    HLL_CARDINALITY(SUM(distinct_customers)) AS approx_unique_customers
FROM agg_daily_sales
WHERE sale_date BETWEEN '2024-01-01' AND '2024-12-31'
  AND country_code = 'US'
GROUP BY 1, 2, 3
ORDER BY 1, revenue DESC;
```

### When to create an aggregation table

- Dashboard query runs > 2 seconds on the raw fact table
- Query uses only SUM/COUNT/MAX/MIN/COUNT DISTINCT aggregations
- The aggregation grain is stable and well-understood
- Data is loaded in bulk (not real-time row-by-row inserts)

### Aggregate Key limitations

- The aggregation key columns must appear in the `AGGREGATE KEY()` declaration in the same order as in CREATE TABLE.
- Only the declared aggregate functions are supported; you cannot later query a SUM column with MIN.
- `HLL_UNION` columns return approximate counts (error rate ~1–2%); use `BITMAP_UNION` for exact COUNT DISTINCT on integer IDs.

---

## BI Acceleration Patterns

### Pattern 1: Materialized view over fact table

```sql
-- Pre-join and pre-aggregate at the materialized view level
-- StarRocks rewrites qualifying queries transparently
CREATE MATERIALIZED VIEW mv_monthly_revenue_by_segment
REFRESH ASYNC EVERY (INTERVAL 1 HOUR)
AS
SELECT
    DATE_TRUNC('month', o.order_date)   AS sale_month,
    c.segment                            AS customer_segment,
    p.category_l1                        AS product_category,
    c.country_code,
    SUM(o.net_revenue)                   AS net_revenue,
    COUNT(DISTINCT o.order_id)           AS order_count
FROM fct_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN dim_products  p ON o.product_id  = p.product_id
GROUP BY 1, 2, 3, 4;
```

### Pattern 2: Synchronous materialized view for single-table aggregation

```sql
-- Synchronous MV — updated inline with every base table write
-- Best for single-table aggregations; adds write latency
CREATE MATERIALIZED VIEW mv_daily_channel_totals
AS
SELECT
    order_date,
    channel,
    SUM(gross_revenue)   AS daily_gross_revenue,
    SUM(net_revenue)     AS daily_net_revenue,
    COUNT(*)             AS order_count
FROM fct_orders
GROUP BY order_date, channel;
```

### Pattern 3: Collocated join (same distribution key)

```sql
-- fct_orders DISTRIBUTED BY HASH(customer_id)
-- collocated_customer_metrics DISTRIBUTED BY HASH(customer_id)
-- → join executed locally on each BE without data shuffle
CREATE TABLE collocated_customer_metrics (
    metric_date  DATE    NOT NULL,
    customer_id  INT     NOT NULL,
    lifetime_orders   INT     SUM  DEFAULT "0",
    lifetime_revenue  DECIMAL(18,2) SUM DEFAULT "0"
)
AGGREGATE KEY(metric_date, customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 64   -- same bucket count as fct_orders
PROPERTIES ("replication_num" = "3");
```

---

## Schema Review Checklist

Run through these 15 questions when reviewing any StarRocks table definition:

1. **Table type** — Is the table type (`DUPLICATE` / `AGGREGATE` / `UNIQUE` / `PRIMARY KEY`) the right one for the access pattern? DUPLICATE for append-only facts; PRIMARY KEY for upsert targets.

2. **Sort key columns** — Does the sort key start with the most-frequently-filtered column (usually a date)? Are sort key columns low-to-medium cardinality to benefit prefix scan?

3. **Decoupled sort key** — On StarRocks 3.3+, is `ORDER BY` used to optimize sort key independently from the DISTRIBUTE KEY and DUPLICATE KEY prefix?

4. **Distribution key** — Is the distribution key a high-cardinality column that spreads rows evenly? Does it match the JOIN key of frequent queries to enable collocated joins?

5. **Bucket count** — Is bucket count at least 3–5x the number of BEs? Is it a multiple of BE count? Typical sizing: `BE_count × 2` to `BE_count × 5` for large tables.

6. **Partitioning** — Is the table partitioned on a date column? Is dynamic partitioning enabled? Are partition granules right-sized (monthly for years of data, daily for months)?

7. **Bloom filter columns** — Are all high-cardinality equality-filter columns (user_id, order_id, session_id) listed in `bloom_filter_columns`? Are any low-cardinality columns incorrectly listed (should use bitmap instead)?

8. **Bitmap indexes** — Do low-cardinality filter columns (< 100K distinct values) that appear in WHERE have bitmap indexes? Are high-cardinality columns excluded?

9. **Replication num** — Is `replication_num` set to match the number of BEs (typically 3)? Dimension tables serving broadcast joins benefit from replication equal to BE count.

10. **Column types** — Are `DECIMAL` types used for monetary amounts (not `FLOAT`/`DOUBLE`)? Are `TINYINT`/`SMALLINT` used for low-range integers to save storage?

11. **Nullable columns** — Are NULL defaults used intentionally? Unnecessary NULLs prevent StarRocks from using certain optimizations. Prefer `DEFAULT ''` or `DEFAULT 0` for non-optional fields.

12. **Compression** — Is `"compression" = "LZ4"` (balanced) or `"ZSTD"` (higher compression, more CPU) set appropriately? LZ4 for hot tables; ZSTD for cold/archival.

13. **Aggregation measures** — In AGGREGATE KEY tables, does every measure column have the correct aggregation function (SUM vs MAX vs HLL_UNION vs BITMAP_UNION)? Are the functions consistent with query semantics?

14. **SCD handling** — For dimension tables with history, is there a versioning strategy (SCD2 with `valid_from`/`valid_to`, or full reload)? Is `is_current` indexed?

15. **Materialized views** — Are there aggregation materialized views covering the top-5 most expensive dashboard queries? Are they refreshed on a schedule that meets freshness SLAs?

---

## Anti-Patterns

### Anti-pattern 1: OLTP-style normalization

```sql
-- BAD: 4th normal form with a category bridge table
CREATE TABLE product_categories (
    product_id  INT NOT NULL,
    category_id INT NOT NULL
) DUPLICATE KEY(product_id, category_id);
-- Every query joining product to its category through this bridge
-- costs a 3-way join over billions of rows

-- GOOD: denormalize category into the product dimension
CREATE TABLE dim_products (
    product_id      INT         NOT NULL,
    category_l1     VARCHAR(64) NOT NULL,  -- flattened
    category_l2     VARCHAR(64),
    category_l3     VARCHAR(64),
    ...
) PRIMARY KEY(product_id) ...;
```

### Anti-pattern 2: High-cardinality column as sort key lead

```sql
-- BAD: UUID order_id as first sort key column — random order defeats scan
CREATE TABLE fct_orders (
    order_id    VARCHAR(36) NOT NULL,  -- UUID
    order_date  DATE        NOT NULL,
    ...
) DUPLICATE KEY(order_id, order_date) ...;
-- Query: WHERE order_date = '2024-06-15' → full table scan (date is key col 2)

-- GOOD: date first, then a low-cardinality filter column
CREATE TABLE fct_orders (
    order_date  DATE        NOT NULL,
    channel     VARCHAR(32) NOT NULL,
    order_id    VARCHAR(36) NOT NULL,
    ...
) DUPLICATE KEY(order_date, channel, order_id)
ORDER BY (order_date, channel)
PROPERTIES ("bloom_filter_columns" = "order_id,customer_id") ...;
-- order_id point lookups served by bloom filter; range scan served by sort key
```

### Anti-pattern 3: Wrong table type for the access pattern

```sql
-- BAD: AGGREGATE KEY table for raw event logs (no aggregation needed)
-- All columns must declare aggregate functions; data is pre-merged at load time
CREATE TABLE raw_events (
    event_id    BIGINT   NOT NULL,
    user_id     BIGINT   SUM,     -- nonsensical: summing a user_id
    event_type  VARCHAR(64) REPLACE  -- REPLACE overwrites; multiple events collapsed
) AGGREGATE KEY(event_id) ...;

-- GOOD: DUPLICATE KEY for append-only raw events
CREATE TABLE raw_events (
    event_date   DATE        NOT NULL,
    event_id     BIGINT      NOT NULL,
    user_id      BIGINT      NOT NULL,
    event_type   VARCHAR(64) NOT NULL
) DUPLICATE KEY(event_date, event_id) ...;
```

### Anti-pattern 4: No partitioning on large time-series tables

```sql
-- BAD: no partitioning — all queries scan the full table
CREATE TABLE fct_clicks (
    click_ts  DATETIME  NOT NULL,
    user_id   BIGINT    NOT NULL,
    ...
) DUPLICATE KEY(click_ts, user_id) ...;

-- GOOD: partition by date; queries limited to relevant partitions
CREATE TABLE fct_clicks (
    click_date  DATE      NOT NULL,
    click_ts    DATETIME  NOT NULL,
    user_id     BIGINT    NOT NULL,
    ...
) DUPLICATE KEY(click_date, click_ts, user_id)
PARTITION BY RANGE(click_date) (
    START ("2023-01-01") END ("2026-01-01") EVERY (INTERVAL 1 DAY)
) ...;
```

### Anti-pattern 5: Bloom filter on low-cardinality columns

```sql
-- BAD: bloom filter on a column with 5 distinct values is useless
-- Each granule will almost certainly match — the filter never skips anything
PROPERTIES ("bloom_filter_columns" = "status,channel");
-- status has 3 values; channel has 6 values → use bitmap indexes instead

-- GOOD: bloom filter on high-cardinality columns
PROPERTIES ("bloom_filter_columns" = "user_id,order_id,session_id");
CREATE INDEX ix_status  ON tbl (status)  USING BITMAP;
CREATE INDEX ix_channel ON tbl (channel) USING BITMAP;
```

### Anti-pattern 6: Over-bucketing small tables

```sql
-- BAD: small dimension table with 100K rows and 128 buckets
-- Each bucket has ~800 rows; query planning and metadata overhead dominates
CREATE TABLE dim_regions (
    region_id   INT         NOT NULL,
    region_name VARCHAR(64) NOT NULL
) PRIMARY KEY(region_id)
DISTRIBUTED BY HASH(region_id) BUCKETS 128;  -- 128 buckets for 100K rows

-- GOOD: match bucket count to data volume
-- Rule of thumb: each bucket should hold ~1GB of data after compression
CREATE TABLE dim_regions (
    region_id   INT         NOT NULL,
    region_name VARCHAR(64) NOT NULL
) PRIMARY KEY(region_id)
DISTRIBUTED BY HASH(region_id) BUCKETS 4;
```

### Anti-pattern 7: Missing bitmap on `is_current` in SCD2 tables

```sql
-- BAD: SCD2 table queried with WHERE is_current = TRUE but no bitmap index
-- Forces full scan to evaluate the boolean filter
SELECT * FROM dim_customers_scd2 WHERE is_current = TRUE AND segment = 'Enterprise';

-- GOOD: bitmap on is_current (2 distinct values = ideal for bitmap)
CREATE INDEX ix_is_current ON dim_customers_scd2 (is_current) USING BITMAP;
-- Combined with bitmap on segment, StarRocks uses bitmap AND to find matching rows
```

---

## References

- StarRocks Table Design overview: https://docs.starrocks.io/docs/table_design/
- StarRocks Table Types: https://docs.starrocks.io/docs/table_design/table_types/
- StarRocks Sort Key and Prefix Index: https://docs.starrocks.io/docs/table_design/sort_key/
- StarRocks Distribution: https://docs.starrocks.io/docs/table_design/data_distribution/
- StarRocks Bitmap Index: https://docs.starrocks.io/docs/table_design/indexes/Bitmap_index/
- StarRocks Bloom Filter Index: https://docs.starrocks.io/docs/table_design/indexes/Bloomfilter_index/
- StarRocks Materialized Views: https://docs.starrocks.io/docs/using_starrocks/async_mv/
- StarRocks Best Practices: https://docs.starrocks.io/docs/best_practices/
- StarRocks Primary Key table (partial update, DELETE): https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/
