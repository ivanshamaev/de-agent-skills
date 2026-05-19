---
name: starrocks-realtime-analytics
description: StarRocks real-time analytics — Kafka → Routine Load → Primary Key table for sub-second freshness, low-latency BI query patterns, real-time dashboard design (materialized view for pre-aggregation), metric store patterns, window-based freshness for streaming dashboards, colocate join for real-time multi-table queries, resource group isolation for OLAP vs ingestion
---

# StarRocks Real-Time Analytics

## When to Use

- Sub-minute latency analytics from Kafka event streams
- Real-time operational dashboards (revenue, orders, user activity)
- Live metric stores queried by BI tools (Grafana, Superset, Tableau)
- Combining streaming ingestion with ad-hoc analytical queries
- Real-time anomaly detection or threshold alerting

---

## Architecture

```
Kafka Topic (events) ──► Routine Load ──► StarRocks Primary Key Table
                                                  │
                         BI Tool ◄── Query ────────┤
                         Grafana ◄── SQL ──────────┤
                                                  │
                    Async MV (pre-aggregation) ◄──┘
                    (refreshes every 1-5 min)
```

---

## Real-Time Event Table (Primary Key)

```sql
-- Real-time orders table with Primary Key (upsert semantics)
CREATE TABLE rt.orders (
    order_id        BIGINT          NOT NULL,
    customer_id     BIGINT          NOT NULL,
    product_id      BIGINT          NOT NULL,
    region          VARCHAR(64)     NOT NULL,
    amount          DECIMAL(10, 2),
    quantity        INT,
    status          VARCHAR(32),
    created_at      DATETIME(3),            -- millisecond precision
    updated_at      DATETIME(3)
)
PRIMARY KEY(order_id)
PARTITION BY RANGE(created_at) (
    START ("2024-01-01") END ("2025-01-01") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(order_id) BUCKETS 32   -- more buckets for high concurrency
PROPERTIES (
    "enable_persistent_index" = "true",
    "replication_num" = "3",
    "storage_medium" = "SSD"               -- SSD for low-latency reads
);
```

---

## Kafka → Routine Load for Real-Time Ingestion

```sql
-- Routine Load for real-time event stream
CREATE ROUTINE LOAD rt_ingest.orders_stream ON orders
PROPERTIES (
    "desired_concurrent_number" = "8",     -- match Kafka partition count
    "max_batch_interval" = "5",            -- commit every 5s for low latency
    "max_batch_rows" = "100000",
    "max_error_number" = "500",
    "format" = "json",
    "jsonpaths" = "[\"$.order_id\",\"$.customer_id\",\"$.product_id\",\"$.region\",\"$.amount\",\"$.quantity\",\"$.status\",\"$.created_at\",\"$.updated_at\"]",
    "columns" = "order_id,customer_id,product_id,region,amount,quantity,status,created_at,updated_at",
    "strict_mode" = "true",
    "timezone" = "UTC"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "orders_stream",
    "kafka_partitions" = "0,1,2,3,4,5,6,7",
    "kafka_offsets" = "OFFSET_END,OFFSET_END,OFFSET_END,OFFSET_END,OFFSET_END,OFFSET_END,OFFSET_END,OFFSET_END",
    "property.group.id" = "sr_rt_orders"
);
```

---

## Real-Time Metric Queries

### Current Revenue (Last N Minutes)

```sql
-- Real-time revenue in the last 15 minutes
SELECT
    DATE_TRUNC('minute', updated_at)    AS minute,
    region,
    COUNT(*)                             AS orders,
    SUM(amount)                          AS revenue
FROM rt.orders
WHERE updated_at >= DATE_SUB(NOW(), INTERVAL 15 MINUTE)
GROUP BY DATE_TRUNC('minute', updated_at), region
ORDER BY minute DESC, revenue DESC;
```

### Active Users in Last Hour

```sql
SELECT COUNT(DISTINCT customer_id) AS active_customers
FROM rt.orders
WHERE updated_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR);
```

### Real-Time Conversion Funnel

```sql
-- Event-based funnel (events table with event_type column)
SELECT
    event_type,
    COUNT(DISTINCT session_id) AS sessions,
    COUNT(*) AS events
FROM rt.user_events
WHERE event_ts >= DATE_SUB(NOW(), INTERVAL 30 MINUTE)
GROUP BY event_type
ORDER BY events DESC;
```

---

## Pre-Aggregation with Async Materialized View

For dashboard queries that run repeatedly, pre-aggregate with async MV:

```sql
-- 1-minute order metrics MV (refreshes every minute)
CREATE MATERIALIZED VIEW rt_mv.orders_per_minute
DISTRIBUTED BY HASH(minute, region) BUCKETS 8
REFRESH ASYNC EVERY (INTERVAL 1 MINUTE)
AS
SELECT
    DATE_TRUNC('minute', created_at)    AS minute,
    region,
    COUNT(*)                             AS order_count,
    SUM(amount)                          AS revenue,
    COUNT(DISTINCT customer_id)          AS unique_customers
FROM rt.orders
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
GROUP BY DATE_TRUNC('minute', created_at), region;
```

Query the MV (automatically selected by optimizer for matching queries):
```sql
-- This query will use the MV transparently if optimizer picks it
SELECT minute, region, order_count, revenue
FROM rt.orders
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
-- Or query MV directly:
-- FROM rt_mv.orders_per_minute
ORDER BY minute DESC;
```

Force MV query:
```sql
SELECT /*+ USE_MV("rt_mv.orders_per_minute") */
    minute, region, order_count
FROM rt.orders
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR);
```

---

## Colocate Join for Real-Time Multi-Table Queries

When joining orders with products at high frequency, colocate both tables:

```sql
-- orders and products colocated by product_id
CREATE TABLE rt.orders (
    order_id   BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    amount     DECIMAL(10, 2),
    ...
)
PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 16  -- distribute by join key
PROPERTIES ("colocate_with" = "rt_group");

CREATE TABLE rt.products (
    product_id   BIGINT NOT NULL,
    product_name VARCHAR(128),
    category     VARCHAR(64),
    ...
)
PRIMARY KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 16  -- same distribution
PROPERTIES ("colocate_with" = "rt_group");  -- same group

-- Join executes locally on each BE — no data shuffle
SELECT
    p.category,
    COUNT(*) AS orders,
    SUM(o.amount) AS revenue
FROM rt.orders o
JOIN rt.products p USING (product_id)
WHERE o.created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
GROUP BY p.category;
```

---

## Resource Group Isolation

Separate real-time ingestion (Routine Load) from analytical queries:

```sql
-- Create resource groups
CREATE RESOURCE GROUP rt_query_group
TO (user='bi_user')
WITH (
    "cpu_core_limit" = "8",
    "mem_limit" = "30%",
    "concurrency_limit" = "20",           -- max parallel queries
    "type" = "normal"
);

CREATE RESOURCE GROUP ingestion_group
TO (user='etl_user')
WITH (
    "cpu_core_limit" = "4",
    "mem_limit" = "20%",
    "type" = "normal"
);

-- Short-query fast lane (< 1s queries)
CREATE RESOURCE GROUP short_query_group
TO (user='dashboard_user')
WITH (
    "cpu_core_limit" = "4",
    "mem_limit" = "15%",
    "concurrency_limit" = "50",
    "short_query_time" = "1000",           -- ms; routes sub-1s queries here
    "type" = "short_query"
);
```

---

## Dashboard Query Patterns

### Grafana Time-Series Query (Prometheus-compatible format)

```sql
-- Grafana SQL data source: orders per minute for time range
SELECT
    DATE_TRUNC('minute', created_at) AS time,
    region AS metric,
    COUNT(*) AS value
FROM rt.orders
WHERE created_at BETWEEN '$__timeFrom()' AND '$__timeTo()'
GROUP BY DATE_TRUNC('minute', created_at), region
ORDER BY time;
```

### Rolling Window Metrics

```sql
-- 5-minute rolling average order value
SELECT
    DATE_TRUNC('minute', created_at) AS minute,
    AVG(amount) OVER (
        ORDER BY DATE_TRUNC('minute', created_at)
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS rolling_5m_avg
FROM (
    SELECT DATE_TRUNC('minute', created_at), AVG(amount) AS amount
    FROM rt.orders
    WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
    GROUP BY DATE_TRUNC('minute', created_at)
) t
ORDER BY minute;
```

### Top-N Real-Time (Approximate)

```sql
-- Top 10 customers by revenue in last hour (approximate for speed)
SELECT customer_id, SUM(amount) AS revenue
FROM rt.orders
WHERE updated_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
GROUP BY customer_id
ORDER BY revenue DESC
LIMIT 10;
```

---

## Freshness Monitoring

```sql
-- Check ingestion lag: how fresh is the data?
SELECT
    MAX(updated_at)                          AS latest_record,
    TIMESTAMPDIFF(SECOND, MAX(updated_at), NOW()) AS lag_seconds,
    COUNT(*)                                 AS records_last_5min
FROM rt.orders
WHERE updated_at >= DATE_SUB(NOW(), INTERVAL 5 MINUTE);
```

Alert if lag exceeds threshold:
```python
def check_rt_freshness(sr_host: str, max_lag_seconds: int = 60) -> None:
    import pymysql
    conn = pymysql.connect(host=sr_host, port=9030, user="monitor")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TIMESTAMPDIFF(SECOND, MAX(updated_at), NOW())
        FROM rt.orders
    """)
    lag = cursor.fetchone()[0]
    conn.close()
    if lag and lag > max_lag_seconds:
        raise RuntimeError(f"Real-time data is {lag}s stale (max: {max_lag_seconds}s)")
```

---

## Anti-Patterns

1. **`max_batch_interval` > 30s** for real-time dashboards — Routine Load only commits every N seconds; `max_batch_interval=5` achieves near-realtime, `60s` is batch.
2. **Using Duplicate Key for real-time updates** — duplicate key appends every version; Primary Key automatically merges to latest state, which is what dashboards need.
3. **No MV for repeated aggregation queries** — BI tools often send the same aggregation query hundreds of times per minute; pre-aggregate with async MV.
4. **Joining large tables without colocate** — cross-BE shuffle for every join adds 10-100ms latency; colocate by join key.
5. **No resource group for BI queries** — ingestion and analytical queries compete for the same CPUs; isolate with resource groups.
6. **DATE_TRUNC('second', NOW())** in correlated subqueries — called once per row, not once per query; materialize to a variable or use CTE.

---

## References

- Routine Load: `docs.starrocks.io/docs/loading/RoutineLoad/`
- Async Materialized Views: `docs.starrocks.io/docs/using_starrocks/Materialized_view/`
- Colocate Join: `docs.starrocks.io/docs/using_starrocks/Colocate_join/`
- Resource Groups: `docs.starrocks.io/docs/administration/management/resource_management/resource_group/`
- Related skills: `[[starrocks-routine-load-kafka]]`, `[[starrocks-materialized-views]]`, `[[starrocks-join-optimization]]`, `[[starrocks-concurrency-optimizer]]`
