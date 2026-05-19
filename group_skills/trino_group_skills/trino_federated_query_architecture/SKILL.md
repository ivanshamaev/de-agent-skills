---
name: trino-federated-query-architecture
description: Trino federated query architecture across heterogeneous sources — cross-catalog JOIN patterns (Iceberg+PostgreSQL+MySQL+Kafka+ClickHouse), pushdown behavior per connector type, minimizing cross-catalog data movement, materializing JDBC data into Iceberg, query routing strategy, connector-specific limitations (JDBC serial fetch, Kafka read-once), performance cost model for federated joins, metadata caching, CREATE TABLE AS SELECT federation patterns, catalog isolation design
---

# Trino Federated Query Architecture

## When to Use

- Joining data from multiple systems (data lake + operational DB + Kafka) in a single query
- Deciding whether to use live federation or pre-materialization
- Designing a cross-domain reporting layer without moving data
- Understanding performance implications before writing a cross-catalog query

---

## Federation Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     Trino Federated Query Layer                  │
│                                                                  │
│  SELECT f.amount, c.region, p.name                               │
│  FROM iceberg.gold.fact_orders f                                 │
│  JOIN postgresql.public.customers c ON f.customer_id = c.id     │
│  JOIN mysql.inventory.products p ON f.product_id = p.id         │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │  Iceberg     │  │ PostgreSQL   │  │   MySQL      │           │
│  │  (S3/Parquet)│  │ (JDBC)       │  │  (JDBC)      │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
└──────────────────────────────────────────────────────────────────┘
```

---

## Connector Performance Characteristics

Understanding how each connector fetches data is essential for correct federation design:

| Connector | Fetch Method | Parallelism | Pushdown Support |
|-----------|-------------|-------------|-----------------|
| **Iceberg** | Parallel split reads (Parquet/ORC) | High (1 split per file) | Predicate, projection, aggregation, limit |
| **Hive** | Parallel split reads | High | Predicate, projection |
| **PostgreSQL** | JDBC serial cursor | Low (1 connection per table) | Predicate, aggregation, join, limit, topn |
| **MySQL** | JDBC serial cursor | Low | Predicate, aggregation, limit |
| **ClickHouse** | JDBC | Low | Predicate, aggregation |
| **Kafka** | Per-partition readers | Medium | Limited (topic + timestamp range) |
| **MongoDB** | Cursor-based | Low | Predicate |
| **JMX** | In-memory | N/A | None |

**Key insight**: Iceberg/Hive reads scale with worker count. JDBC connectors (PostgreSQL, MySQL) make a single serial connection — Trino fetches all rows then filters/joins in memory. For large JDBC tables, always push predicates down.

---

## Cross-Catalog JOIN Patterns

### Pattern 1: Small Dimension from PostgreSQL (Broadcast)

```sql
-- GOOD: PostgreSQL table is small (< 100MB), broadcasts to all workers
-- Trino fetches entire table from PG into worker memory, then broadcasts
SELECT
    f.order_id,
    f.amount,
    c.region,
    c.tier
FROM iceberg.silver.orders f
JOIN postgresql.public.customers c
  ON f.customer_id = c.id
WHERE f.order_date = DATE '2024-06-01'   -- predicate pushed into Iceberg (partition prune)
  AND c.tier = 'premium';               -- predicate pushed into PostgreSQL query

-- EXPLAIN shows: BROADCAST exchange for the PG join
```

### Pattern 2: Large JDBC Table → Pre-Materialize First

```sql
-- BAD: joining 50M-row PG table directly — Trino fetches 50M rows serially over JDBC
SELECT f.amount, p.product_name
FROM iceberg.silver.orders f
JOIN postgresql.catalog.products p ON f.product_id = p.id;   -- 50M rows from PG!

-- GOOD: materialize the large PG table into Iceberg daily
CREATE TABLE iceberg.silver.products_snapshot
WITH (format = 'PARQUET', compression_codec = 'ZSTD')
AS SELECT id, product_name, category, price, updated_at
   FROM postgresql.catalog.products;

-- Now join two Iceberg tables (parallel, pushdown, fast)
SELECT f.amount, p.product_name
FROM iceberg.silver.orders f
JOIN iceberg.silver.products_snapshot p ON f.product_id = p.id;
```

### Pattern 3: Kafka Event Preview (Read-Once)

```sql
-- Query Kafka topic as a table (reads current offset window)
-- Note: results are non-repeatable — each query reads different offsets
SELECT
    _timestamp,
    json_extract_scalar(cast(_message as VARCHAR), '$.event_type') AS event_type,
    json_extract_scalar(cast(_message as VARCHAR), '$.user_id')    AS user_id
FROM kafka.default."prod.user.events.v1"
WHERE _timestamp > NOW() - INTERVAL '1' HOUR
LIMIT 1000;
```

### Pattern 4: Cross-Schema Enrichment (Same Catalog)

```sql
-- Cross-schema within same Iceberg catalog — same connector, full pushdown
SELECT
    o.order_id,
    o.amount,
    c.name     AS customer_name,
    c.region,
    p.product_name
FROM iceberg.silver.orders o
JOIN iceberg.silver.customers c ON o.customer_id = c.customer_id
JOIN iceberg.gold.dim_product p ON o.product_id  = p.product_id
WHERE o.order_date = DATE '2024-06-01';
```

---

## Predicate Pushdown Strategy

Always write predicates to enable connector-level filtering:

```sql
-- GOOD: predicate on PG side is pushed down (becomes WHERE in PG query)
SELECT * FROM postgresql.public.orders
WHERE status = 'shipped'        -- pushed to PG: SELECT * WHERE status='shipped'
  AND created_at > NOW() - INTERVAL '7' DAY;

-- VERIFY: EXPLAIN (TYPE IO) shows constraint on columns
EXPLAIN (TYPE IO)
SELECT * FROM postgresql.public.orders WHERE status = 'shipped';
-- → "constraint": {"columnConstraints": [{"columnName": "status", "domain": "shipped"}]}
```

**PostgreSQL connector limitation**: range predicates on VARCHAR columns are NOT pushed down (only equality):

```sql
-- This pushes down (equality on VARCHAR):
WHERE country = 'US'

-- This does NOT push down (range on VARCHAR):
WHERE country BETWEEN 'A' AND 'M'   -- full table scan in PG, filter in Trino
```

---

## Materialization Decision Matrix

| JDBC Table Size | Query Frequency | Strategy |
|----------------|----------------|----------|
| < 10MB | Any | Live federation (broadcast) |
| 10MB–500MB | < 1/hour | Live federation with predicate pushdown |
| > 500MB | > 1/day | Daily CTAS snapshot into Iceberg |
| > 500MB | < 1/week | Live federation with tight predicates |
| Any | Real-time | Keep in JDBC, use Debezium→Iceberg CDC |

---

## Snapshot Pattern: JDBC → Iceberg

```sql
-- Daily incremental snapshot (append new/changed rows)
CREATE TABLE IF NOT EXISTS iceberg.silver.pg_orders_snapshot (
    order_id    BIGINT,
    customer_id BIGINT,
    status      VARCHAR,
    amount      DECIMAL(18,2),
    updated_at  TIMESTAMP(6)
) WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);

-- Incremental load (Airflow task, run daily)
INSERT INTO iceberg.silver.pg_orders_snapshot
SELECT order_id, customer_id, status, CAST(amount AS DECIMAL(18,2)), updated_at
FROM postgresql.public.orders
WHERE updated_at >= TIMESTAMP '{{ yesterday_ds }} 00:00:00'
  AND updated_at <  TIMESTAMP '{{ ds }} 00:00:00';
```

---

## Multi-Source Reporting Query

```sql
-- Report combining Iceberg lake + PostgreSQL OLTP + ClickHouse analytics
-- All predicates pushed to respective connectors
SELECT
    o.order_date,
    c.region                           AS customer_region,
    p.category                         AS product_category,
    COUNT(o.order_id)                  AS order_count,
    SUM(o.amount)                      AS revenue,
    MAX(ch.page_views)                 AS peak_page_views
FROM iceberg.silver.orders o                               -- Iceberg: parallel scan
JOIN postgresql.public.customers c                         -- PG: small table, broadcast
  ON o.customer_id = c.id
JOIN iceberg.gold.dim_product p                            -- Iceberg: broadcast
  ON o.product_id = p.product_id
LEFT JOIN clickhouse.analytics.daily_traffic ch            -- ClickHouse: aggregated
  ON o.order_date = ch.event_date AND c.region = ch.region
WHERE o.order_date BETWEEN DATE '2024-01-01' AND DATE '2024-03-31'
  AND c.tier IN ('premium', 'enterprise')
GROUP BY o.order_date, c.region, p.category
ORDER BY revenue DESC
LIMIT 100;
```

---

## Catalog Isolation Design

Separate catalogs by domain and access scope:

```
etc/catalog/
├── iceberg.properties          → lake data (Iceberg on S3)
├── iceberg_external.properties → external Iceberg (read-only partner data)
├── postgresql_orders.properties → orders OLTP DB (read-only replica)
├── postgresql_users.properties  → users OLTP DB (read-only replica)
├── clickhouse.properties        → analytics ClickHouse
├── kafka.properties             → event streaming (read-only)
└── tpch.properties              → TPC-H test data
```

Use resource groups to restrict which users can query which catalogs:

```json
{
  "selectors": [
    {
      "user": ".*",
      "queryType": "SELECT",
      "queryText": ".*postgresql_orders.*",
      "group": "global.readonly.readonly_${USER}"
    }
  ]
}
```

---

## Anti-Patterns

1. **Joining two large JDBC tables cross-connector** — both tables are fetched serially and joined in Trino memory; a 10M×5M JDBC join moves 15M rows over JDBC before any join; always materialize at least one side into Iceberg.
2. **No predicate on JDBC side in cross-catalog query** — Trino fetches the entire JDBC table when predicates can't be pushed; always include at least one equality predicate on a JDBC table.
3. **Querying Kafka topics in production reports** — Kafka connector reads are non-repeatable and non-deterministic; use for exploration only; for production, sink Kafka to Iceberg via Flink/Debezium.
4. **No metadata caching on JDBC catalogs** — each query does schema/stats lookups; set `metadata.cache-ttl=5m` in JDBC catalog config to avoid thundering herd of metadata requests.
5. **Cross-catalog JOINs in scheduled dbt models** — dbt incremental on top of JDBC sources creates large full-table scans per run; snapshot JDBC sources into Iceberg first, then run dbt transformations on Iceberg only.

---

## References

- Trino connectors: `trino.io/docs/current/connector.html`
- PostgreSQL connector: `trino.io/docs/current/connector/postgresql.html`
- Iceberg connector: `trino.io/docs/current/connector/iceberg.html`
- Kafka connector: `trino.io/docs/current/connector/kafka.html`
- Related skills: `[[trino-lakehouse-platform-architect]]`, `[[trino-query-optimization]]`, `[[trino-iceberg-best-practices]]`
