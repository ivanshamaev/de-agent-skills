---
name: clickhouse-olap
description: ClickHouse OLAP — MergeTree family engines (MergeTree/ReplacingMergeTree/AggregatingMergeTree/SummingMergeTree/CollapsingMergeTree), ORDER BY/PARTITION BY/PRIMARY KEY design, data skipping indexes (minmax/set/bloom_filter), TTL tiered storage, materialized views, projections, LowCardinality/Nullable/AggregateFunction types, INSERT batching, FINAL keyword, query optimization (PREWHERE/parallel_replicas), dictionaries, Kafka engine, Python clickhouse-connect, best practices
---

# ClickHouse OLAP

## When to Use

Load this skill when the user needs to:
- Design ClickHouse tables for analytical workloads (facts, metrics, logs, events)
- Choose the right MergeTree engine variant
- Optimize queries (ORDER BY design, data skipping indexes, PREWHERE)
- Set up materialized views for real-time aggregations
- Configure TTL for data lifecycle management
- Write Python code with `clickhouse-connect` or `clickhouse-driver`

---

## Architecture Overview

```
Client → ClickHouse Server
         ├─ Query Processor (Vectorized Engine)
         ├─ MergeTree Storage
         │    ├─ Partitions (PARTITION BY)
         │    │    └─ Data Parts (sorted, immutable)
         │    │         └─ Granules (8192 rows each)
         │    ├─ Sparse Primary Index (marks file)
         │    └─ Data Skipping Indexes (secondary)
         └─ Background Merges
```

**Granule** — minimum read unit (default 8192 rows). Primary index stores one mark per granule. Data skipping indexes operate on granule blocks.

---

## Data Types

### Key Type Choices

```sql
-- Prefer fixed-width types
UInt8, UInt16, UInt32, UInt64, UInt128, UInt256
Int8, Int16, Int32, Int64, Int128, Int256
Float32, Float64
Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S)  -- for money

-- Dates & times
Date, Date32                    -- days since epoch (4 bytes / 4 bytes)
DateTime('UTC'), DateTime64(3, 'UTC')  -- seconds / milliseconds

-- Strings
String                          -- arbitrary length
FixedString(N)                  -- exactly N bytes
LowCardinality(String)          -- dictionary encoding; use when cardinality < 10 000

-- Arrays & Maps
Array(T)
Map(K, V)
Tuple(a T1, b T2)

-- Nullable (avoid in ORDER BY / primary key)
Nullable(T)                     -- wraps any type; adds null bitmap overhead
```

### LowCardinality — Use Everywhere for String Columns < 10k Distinct Values

```sql
CREATE TABLE events (
    event_type   LowCardinality(String),   -- "click", "view", "purchase"
    country_code LowCardinality(String),   -- "RU", "BY", "KZ"
    status       LowCardinality(String),   -- "placed", "shipped", "delivered"
    ...
)
```

LowCardinality uses dictionary encoding: stores integer index instead of full string per row. Reduces storage 3–5× and speeds up GROUP BY/JOIN on string columns significantly.

---

## MergeTree — Base Engine

```sql
CREATE TABLE events
(
    event_id     UInt64,
    user_id      UInt64,
    event_type   LowCardinality(String),
    page_url     String,
    session_id   String,
    amount       Nullable(Decimal64(2)),
    event_time   DateTime('UTC'),
    dt           Date MATERIALIZED toDate(event_time),   -- generated column

    -- Data skipping index: fast lookup by user_id
    INDEX idx_user   user_id   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_type   event_type TYPE set(20)            GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(dt)          -- one partition per month
ORDER BY (event_type, user_id, event_time)   -- sort key = primary key
PRIMARY KEY (event_type, user_id)   -- coarser primary key (subset of ORDER BY)
SETTINGS
    index_granularity = 8192,
    min_bytes_for_wide_part = 10485760,   -- 10 MB: use wide format above this size
    storage_policy = 'tiered';            -- optional: hot/cold storage tiers
```

**ORDER BY design rules:**
1. Put low-cardinality columns first (they prune most effectively).
2. Put time column last (it's often used for range scans after point lookups).
3. PRIMARY KEY can be a prefix of ORDER BY — use when you don't want to index every sort column.

---

## ReplacingMergeTree — Upsert / Deduplication

Keeps the latest version of a row per `ORDER BY` key. Background merges deduplicate.

```sql
CREATE TABLE orders (
    order_id    UInt64,
    user_id     UInt64,
    status      LowCardinality(String),
    amount      Decimal64(2),
    updated_at  DateTime('UTC'),
    is_deleted  UInt8 DEFAULT 0       -- soft delete flag
)
ENGINE = ReplacingMergeTree(updated_at, is_deleted)
-- ver = updated_at: keep row with max updated_at
-- is_deleted: if 1 on the winning row, treated as deleted (ClickHouse 23.2+)
PARTITION BY toYYYYMM(updated_at)
ORDER BY order_id;
```

**Query — force deduplication at query time:**

```sql
-- FINAL: merge result in memory, return one row per ORDER BY key
-- Slow on large tables; use for small lookups or with partition filter
SELECT order_id, status, amount
FROM orders FINAL
WHERE order_id = 12345;

-- Preferred: use argMax to get latest version without FINAL
SELECT
    order_id,
    argMax(status,     updated_at) AS status,
    argMax(amount,     updated_at) AS amount,
    argMax(is_deleted, updated_at) AS is_deleted
FROM orders
WHERE order_id = 12345
GROUP BY order_id
HAVING is_deleted = 0;
```

---

## AggregatingMergeTree — Pre-Aggregated Data

Stores intermediate aggregate states; merges them on background. Used with materialized views for real-time pre-aggregation.

```sql
-- Aggregating table
CREATE TABLE orders_agg_daily
(
    dt          Date,
    status      LowCardinality(String),
    order_cnt   AggregateFunction(count),               -- count state
    revenue     AggregateFunction(sum, Decimal64(2)),   -- sum state
    uniq_users  AggregateFunction(uniq, UInt64)         -- HLL state
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, status);

-- Materialized view that populates it
CREATE MATERIALIZED VIEW orders_agg_daily_mv
TO orders_agg_daily
AS
SELECT
    toDate(event_time) AS dt,
    status,
    countState()       AS order_cnt,
    sumState(amount)   AS revenue,
    uniqState(user_id) AS uniq_users
FROM orders
GROUP BY dt, status;

-- Query: use -Merge combinator
SELECT
    dt,
    status,
    countMerge(order_cnt)  AS orders,
    sumMerge(revenue)      AS revenue,
    uniqMerge(uniq_users)  AS unique_users
FROM orders_agg_daily
GROUP BY dt, status
ORDER BY dt DESC;
```

---

## SummingMergeTree — Simple Numeric Aggregation

Sums numeric columns for identical ORDER BY keys during background merge.

```sql
CREATE TABLE page_views_daily
(
    dt        Date,
    page_id   UInt64,
    views     UInt64,    -- summed automatically
    clicks    UInt64     -- summed automatically
)
ENGINE = SummingMergeTree()    -- or SummingMergeTree(views, clicks) to sum specific cols
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, page_id);

-- Insert raw events (no pre-aggregation needed)
INSERT INTO page_views_daily VALUES ('2024-01-15', 42, 1, 0);
INSERT INTO page_views_daily VALUES ('2024-01-15', 42, 0, 1);

-- Query: always GROUP BY + sum() to account for not-yet-merged parts
SELECT dt, page_id, sum(views), sum(clicks)
FROM page_views_daily
GROUP BY dt, page_id;
```

---

## CollapsingMergeTree — Mutable Rows via +1/-1 Sign

```sql
CREATE TABLE user_balance
(
    user_id  UInt64,
    balance  Decimal64(2),
    sign     Int8    -- +1 = insert, -1 = cancel previous
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY user_id;

-- Insert initial balance
INSERT INTO user_balance VALUES (1001, 1000.00, 1);

-- Update: cancel old row + insert new row
INSERT INTO user_balance VALUES
    (1001, 1000.00, -1),   -- cancel
    (1001, 1250.00,  1);   -- new value

-- Query
SELECT user_id, sum(balance * sign) AS current_balance
FROM user_balance
GROUP BY user_id
HAVING sum(sign) > 0;
```

---

## Data Skipping Indexes

```sql
CREATE TABLE orders (
    order_id  UInt64,
    user_id   UInt64,
    status    LowCardinality(String),
    tags      Array(String),
    payload   String,
    event_time DateTime

    -- minmax: best for numeric range queries
    INDEX idx_order_id order_id TYPE minmax GRANULARITY 4,

    -- set: best for low-cardinality equality (< 1000 distinct values per granule block)
    INDEX idx_status status TYPE set(20) GRANULARITY 1,

    -- bloom_filter: best for high-cardinality point lookups (user_id, session_id)
    INDEX idx_user user_id TYPE bloom_filter(0.01) GRANULARITY 1,

    -- ngrambf: substring search in string columns
    INDEX idx_payload payload TYPE ngrambf_v1(4, 1024, 2, 0) GRANULARITY 1,

    -- For Array columns: existence check
    INDEX idx_tags arrayJoin(tags) TYPE bloom_filter(0.05) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY (status, user_id, event_time);
```

**Index selection guide:**

| Query Pattern | Index Type |
|---|---|
| `WHERE col = X` (high cardinality) | `bloom_filter` |
| `WHERE col = X` (low cardinality) | `set(N)` |
| `WHERE col BETWEEN a AND b` | `minmax` |
| `WHERE col LIKE '%substr%'` | `ngrambf_v1` |
| `WHERE has(array_col, X)` | `bloom_filter` on `arrayJoin` |

---

## TTL — Data Lifecycle

```sql
CREATE TABLE logs
(
    log_id      UInt64,
    level       LowCardinality(String),
    message     String,
    created_at  DateTime('UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (level, created_at)
-- Table TTL: delete rows older than 90 days, move to cold after 30 days
TTL
    created_at + INTERVAL 30 DAY TO VOLUME 'cold',
    created_at + INTERVAL 90 DAY DELETE
SETTINGS storage_policy = 'tiered';

-- Column TTL: null out sensitive data after 30 days
ALTER TABLE logs MODIFY COLUMN message
    String TTL created_at + INTERVAL 30 DAY;
```

---

## Partitioning Best Practices

```sql
-- GOOD: month-based for time-series
PARTITION BY toYYYYMM(dt)           -- ~12 partitions per year

-- GOOD: date for high-ingest daily data
PARTITION BY toDate(event_time)     -- one partition per day

-- BAD: high-cardinality partition (creates thousands of tiny partitions)
PARTITION BY user_id                -- millions of partitions = metadata hell

-- GOOD: multi-level with tuple
PARTITION BY (toYear(dt), toMonth(dt))

-- Drop old partitions efficiently
ALTER TABLE events DROP PARTITION '202301';
ALTER TABLE events DROP PARTITION ID '202301';

-- Detach without delete (for archival)
ALTER TABLE events DETACH PARTITION '202301';
```

**Partition count rule**: keep partition count < 1000; each partition has metadata overhead. Use monthly partitions for < 3 years of data; weekly for hot tables with frequent drops.

---

## Materialized Views

```sql
-- Real-time counter: count events per minute
CREATE TABLE events_per_minute
(
    window_start DateTime('UTC'),
    event_type   LowCardinality(String),
    cnt          AggregateFunction(count),
    uniq_users   AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, event_type);

CREATE MATERIALIZED VIEW events_per_minute_mv
TO events_per_minute
AS
SELECT
    toStartOfMinute(event_time) AS window_start,
    event_type,
    countState()       AS cnt,
    uniqState(user_id) AS uniq_users
FROM events
GROUP BY window_start, event_type;

-- Query
SELECT
    window_start,
    event_type,
    countMerge(cnt)      AS events,
    uniqMerge(uniq_users) AS users
FROM events_per_minute
WHERE window_start >= now() - INTERVAL 1 HOUR
GROUP BY window_start, event_type
ORDER BY window_start;
```

**Materialized view rules:**
- Target table must exist before creating the view.
- MV only fires for **new inserts** — does not process existing data.
- Populate existing data manually after creating MV:
  ```sql
  INSERT INTO target_table SELECT ... FROM source_table;
  ```

---

## Projections (Secondary Sort Orders)

```sql
-- Table sorted by (dt, user_id) — fast for time-range + user lookups
CREATE TABLE orders
(
    order_id    UInt64,
    user_id     UInt64,
    status      LowCardinality(String),
    amount      Decimal64(2),
    dt          Date,

    -- Projection: store pre-aggregated summary sorted by status
    PROJECTION daily_status_agg
    (
        SELECT
            dt,
            status,
            count() AS cnt,
            sum(amount) AS revenue
        GROUP BY dt, status
    ),

    -- Projection: alternative sort order for status-first queries
    PROJECTION by_status
    (
        SELECT *
        ORDER BY (status, dt, user_id)
    )
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, user_id);

-- Projection is used automatically by the query optimizer
-- when the query matches its sorting/grouping
SELECT dt, status, count(), sum(amount)
FROM orders
GROUP BY dt, status;  -- uses daily_status_agg projection

-- Materialize projection on existing data
ALTER TABLE orders MATERIALIZE PROJECTION daily_status_agg;
```

---

## Query Optimization

### PREWHERE

```sql
-- ClickHouse applies PREWHERE before reading all columns (auto-optimization)
-- Force manual PREWHERE for expensive predicates
SELECT user_id, event_type, amount
FROM events
PREWHERE dt = today()            -- read only dt column first, filter rows
WHERE event_type = 'purchase';   -- then read remaining columns for survivors
```

### SELECT — Only What You Need

```sql
-- BAD: reads all columns into memory
SELECT * FROM events WHERE dt = today();

-- GOOD: read only needed columns
SELECT user_id, event_type, amount FROM events WHERE dt = today();
```

### Avoid Functions on Indexed Columns

```sql
-- BAD: function on ORDER BY column prevents index use
SELECT * FROM events WHERE toDate(event_time) = '2024-01-15';

-- GOOD: range on the indexed column directly
SELECT * FROM events WHERE event_time >= '2024-01-15' AND event_time < '2024-01-16';
```

### Aggregation with LowCardinality

```sql
-- LowCardinality columns aggregate faster (dictionary-based)
SELECT event_type, count() FROM events GROUP BY event_type;   -- fast
SELECT page_url, count()   FROM events GROUP BY page_url;      -- slow (high cardinality String)
```

### FINAL — Use Sparingly

```sql
-- FINAL is slow: triggers in-memory merge for entire result set
-- OK for point lookups with small partition filter
SELECT * FROM orders FINAL WHERE order_id = 12345;

-- Prefer argMax for large scans on ReplacingMergeTree
SELECT order_id, argMax(status, updated_at), argMax(amount, updated_at)
FROM orders
WHERE dt >= '2024-01-01'
GROUP BY order_id;
```

---

## INSERT Patterns

```sql
-- Batch inserts: always insert 10k–1M rows per INSERT
-- ClickHouse creates one data part per INSERT — too many INSERTs = too many parts

-- BAD: one row per INSERT
INSERT INTO events VALUES (1, 1001, 'click', ...);
INSERT INTO events VALUES (2, 1002, 'view', ...);

-- GOOD: batch insert
INSERT INTO events VALUES
    (1, 1001, 'click', ...),
    (2, 1002, 'view', ...),
    (3, 1003, 'purchase', ...);

-- Idempotent insert (deduplication by insert block hash)
SET insert_deduplicate = 1;   -- default on replicated tables

-- INSERT SELECT from staging
INSERT INTO orders
SELECT order_id, user_id, status, amount, updated_at
FROM orders_staging
WHERE dt = '2024-01-15';
```

---

## Python Integration

### clickhouse-connect (recommended, HTTP-based)

```python
import clickhouse_connect

client = clickhouse_connect.get_client(
    host="clickhouse.internal",
    port=8123,
    username="default",
    password="",
    database="default",
    secure=False,
    settings={"max_execution_time": 300},
)

# Query → Python list of dicts
result = client.query("SELECT order_id, status, amount FROM orders WHERE dt = '2024-01-15'")
for row in result.named_results():
    print(row)  # {"order_id": 1, "status": "placed", "amount": 299.99}

# Query → Pandas DataFrame
df = client.query_df("SELECT * FROM orders_agg WHERE dt >= today() - 7")

# Insert pandas DataFrame
import pandas as pd
df = pd.DataFrame({"order_id": [1, 2], "status": ["placed", "shipped"], "amount": [100.0, 200.0]})
client.insert_df("orders", df)

# Insert list of dicts
client.insert("events", [
    {"event_id": 1, "user_id": 1001, "event_type": "click", "event_time": "2024-01-15 10:00:00"},
    {"event_id": 2, "user_id": 1002, "event_type": "purchase", "event_time": "2024-01-15 10:01:00"},
])

# Execute DDL / DML
client.command("ALTER TABLE orders DROP PARTITION '202312'")
```

### Kafka Engine (Streaming Ingest)

```sql
-- Kafka consumer table (ephemeral, no storage)
CREATE TABLE orders_kafka_queue
(
    order_id    UInt64,
    user_id     UInt64,
    status      String,
    amount      Float64,
    event_time  DateTime
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list       = 'broker:9092',
    kafka_topic_list        = 'orders',
    kafka_group_name        = 'clickhouse-orders',
    kafka_format            = 'JSONEachRow',
    kafka_num_consumers     = 4,
    kafka_max_block_size    = 65536;

-- MergeTree landing table
CREATE TABLE orders_raw
(
    order_id    UInt64,
    user_id     UInt64,
    status      LowCardinality(String),
    amount      Decimal64(2),
    event_time  DateTime('UTC'),
    _ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toDate(event_time)
ORDER BY (user_id, event_time);

-- Materialized view bridges Kafka → MergeTree
CREATE MATERIALIZED VIEW orders_kafka_mv TO orders_raw
AS SELECT
    order_id,
    user_id,
    status,
    toDecimal64(amount, 2) AS amount,
    toDateTime(event_time) AS event_time
FROM orders_kafka_queue;
```

---

## Dictionaries (External Lookup Tables)

```sql
-- Flat dictionary from PostgreSQL (for ID → Name lookups)
CREATE DICTIONARY user_dict
(
    user_id  UInt64,
    username String,
    region   String
)
PRIMARY KEY user_id
SOURCE(POSTGRESQL(
    HOST 'postgres'
    PORT 5432
    USER 'clickhouse'
    PASSWORD 'secret'
    DB 'users'
    TABLE 'users'
))
LAYOUT(HASHED())              -- in-memory hash map
LIFETIME(MIN 300 MAX 600);    -- refresh every 5-10 minutes

-- Usage in queries (fast key-value lookup without JOIN)
SELECT
    e.user_id,
    dictGet('user_dict', 'username', e.user_id) AS username,
    dictGet('user_dict', 'region',   e.user_id) AS region,
    count() AS events
FROM events e
GROUP BY e.user_id, username, region;
```

---

## Best Practices

1. **Design ORDER BY for your most common query predicates** — this is the single most impactful decision for ClickHouse performance.
2. **Use `LowCardinality(String)`** for all string columns with fewer than 10 000 distinct values.
3. **Avoid `Nullable(T)` in ORDER BY columns** — nulls can't participate in index lookups; use sentinel values (0, empty string) instead.
4. **Batch inserts**: minimum 10 000 rows per INSERT to avoid creating too many small data parts.
5. **Never use `SELECT *`** — ClickHouse is columnar; reading unused columns wastes I/O.
6. **Partition by month for most time-series** — daily partitions create too many parts; yearly makes drops too coarse.
7. **Use `ReplacingMergeTree` with `argMax`** for mutable dimension tables instead of `FINAL` on large tables.
8. **Materialized views + AggregatingMergeTree** for real-time dashboards — pre-aggregate at ingest, query aggregated table.
9. **Add `bloom_filter` indexes** on high-cardinality ID columns queried with equality (`user_id`, `session_id`, `order_id`).
10. **Use Projections** for alternative query patterns that don't match the primary ORDER BY.
11. **TTL DELETE + TTL TO VOLUME** for automatic hot/cold tiering — keep recent data on SSD, archive to HDD/object storage.
12. **Set `max_execution_time`** on queries to prevent runaway scans from blocking resources.

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| High-cardinality `PARTITION BY` (user_id, order_id) | Millions of partitions; metadata explosion; slow queries | Use `toYYYYMM(dt)` or `toDate(dt)` |
| ORDER BY on a single timestamp column | No point-lookup optimization; every query scans all granules | Put selective columns first: `(status, user_id, event_time)` |
| One row per INSERT | Creates thousands of tiny data parts; triggers emergency merges | Batch 10k–1M rows per INSERT |
| `SELECT *` on large tables | Reads all columns; defeats columnar storage advantage | Always list only needed columns |
| `WHERE toDate(event_time) = ...` | Function on indexed column prevents index use | Use range: `event_time >= X AND event_time < Y` |
| `FINAL` on large tables | In-memory merge; slow; blocks resources | Use `argMax` for latest-value queries |
| No `LowCardinality` on string columns | 3-5× more storage; slower GROUP BY | Wrap all low-cardinality strings: `LowCardinality(String)` |
| Using `String` for amounts | Requires CAST on every computation | Use `Decimal64(2)` for money |
| Materialized view without pre-populating | Historical data missing from aggregation | Run `INSERT INTO target SELECT ... FROM source` after MV creation |
| No TTL on high-velocity tables | Unbounded data growth | Set `TTL` with DELETE or TO VOLUME policy |

---

## References to Consult When Needed

- [ClickHouse MergeTree Documentation](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
- [ClickHouse Best Practices Guide](https://clickhouse.com/docs/en/guides/best-practices/)
- [ClickHouse Query Optimization](https://clickhouse.com/docs/en/guides/best-practices/sparse-primary-indexes)
- [AggregatingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/aggregatingmergetree)
- [ClickHouse Materialized Views](https://clickhouse.com/docs/en/materialized-view)
- [clickhouse-connect Python client](https://clickhouse.com/docs/en/integrations/python)
- [Kafka Engine](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka)
