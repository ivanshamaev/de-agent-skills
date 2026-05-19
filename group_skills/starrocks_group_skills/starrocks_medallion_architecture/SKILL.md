---
name: starrocks-medallion-architecture
description: StarRocks Medallion (Bronze/Silver/Gold) architecture — Bronze raw ingestion (Duplicate Key), Silver cleansed layer (Primary Key upsert/dedup), Gold aggregated layer (Aggregate Key or MV), incremental transform patterns (INSERT OVERWRITE partition, watermark-based), layer DDL patterns, CDC Bronze landing, dbt-compatible layer design, inter-layer INSERT SELECT pipelines
---

# StarRocks Medallion Architecture

## When to Use

- Designing a multi-layer data platform on StarRocks
- Structuring Bronze (raw) → Silver (cleansed) → Gold (aggregated) tables
- Building incremental pipelines between layers
- Choosing the right StarRocks table type per layer
- Integrating CDC streams into the medallion architecture

---

## Layer Design Principles

| Layer | Purpose | StarRocks Table Type | Load Pattern |
|-------|---------|---------------------|--------------|
| Bronze | Raw, immutable ingestion | Duplicate Key | Broker Load / Stream Load / Routine Load |
| Silver | Cleansed, deduplicated, enriched | Primary Key (upsert) | INSERT OVERWRITE or incremental merge |
| Gold | Business aggregations, metrics | Aggregate Key or MV | INSERT OVERWRITE or Materialized View refresh |

---

## Bronze Layer (Raw Ingestion)

Bronze tables store raw data as-is — no transformation, immutable history.

```sql
-- Bronze: raw CDC events, Duplicate Key preserves all versions
CREATE TABLE bronze.orders_raw (
    ingestion_id    BIGINT          NOT NULL,  -- auto-increment surrogate
    order_id        BIGINT          NOT NULL,
    customer_id     BIGINT,
    amount          DECIMAL(10, 2),
    status          VARCHAR(32),
    op              VARCHAR(4),                -- CDC op: c/u/d/r
    source_ts       DATETIME(6),              -- source timestamp
    kafka_topic     VARCHAR(128),
    kafka_partition INT,
    kafka_offset    BIGINT,
    ingested_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(ingestion_id, order_id)          -- Duplicate: keep all rows
PARTITION BY RANGE(ingested_at) (
    PARTITION p202401 VALUES LESS THAN ("2024-02-01"),
    PARTITION p202402 VALUES LESS THAN ("2024-03-01")
)
DISTRIBUTED BY HASH(order_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "storage_medium" = "HDD"                  -- cold storage for raw
);

-- Bronze ingestion via Routine Load from Kafka
CREATE ROUTINE LOAD bronze_cdc.orders_raw_load ON orders_raw
PROPERTIES (
    "desired_concurrent_number" = "4",
    "format" = "json",
    "jsonpaths" = "[\"$.order_id\",\"$.customer_id\",\"$.amount\",\"$.status\",\"$.op\",\"$.ts_ms\"]",
    "columns" = "order_id,customer_id,amount,status,op,source_ts_ms,source_ts=FROM_UNIXTIME(source_ts_ms/1000),ingestion_id=0,ingested_at=NOW()",
    "max_error_number" = "1000"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "postgres.sales.orders",
    "property.group.id" = "sr_bronze_orders"
);
```

---

## Silver Layer (Cleansed + Deduplicated)

Silver tables hold the current state — deduplicated, enriched, with PK enforcement.

```sql
-- Silver: current state, Primary Key for upsert semantics
CREATE TABLE silver.orders (
    order_id        BIGINT          NOT NULL,
    customer_id     BIGINT          NOT NULL,
    amount          DECIMAL(10, 2),
    status          VARCHAR(32),
    created_at      DATETIME,
    updated_at      DATETIME,
    -- Enrichment columns
    customer_tier   VARCHAR(16),
    region          VARCHAR(64),
    -- Metadata
    _bronze_ts      DATETIME,       -- when this row first appeared in bronze
    _silver_ts      DATETIME        -- when last upserted into silver
)
PRIMARY KEY(order_id)
PARTITION BY RANGE(created_at) (
    START ("2024-01-01") END ("2025-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(order_id) BUCKETS 16
PROPERTIES (
    "enable_persistent_index" = "true",
    "replication_num" = "3"
);

-- Silver: incremental load from Bronze (latest op per order_id)
-- Run every N minutes via Airflow
INSERT INTO silver.orders
SELECT
    b.order_id,
    b.customer_id,
    b.amount,
    b.status,
    b.source_ts                AS created_at,
    b.source_ts                AS updated_at,
    c.tier                     AS customer_tier,
    c.region,
    b.ingested_at              AS _bronze_ts,
    NOW()                      AS _silver_ts
FROM (
    -- Deduplicate: take latest op per order_id from Bronze
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY source_ts DESC, ingested_at DESC) AS rn
        FROM bronze.orders_raw
        WHERE op IN ('c', 'u', 'r')         -- exclude DELETEs for upsert
          AND ingested_at >= DATE_SUB(NOW(), INTERVAL 30 MINUTE)
    ) t
    WHERE rn = 1
) b
LEFT JOIN silver.dim_customers c ON b.customer_id = c.customer_id;
-- Primary Key table: INSERT automatically upserts on order_id
```

### Handling Deletes in Silver

```sql
-- Load delete events separately using __op column
INSERT INTO silver.orders
SELECT
    order_id,
    customer_id,
    amount,
    status,
    source_ts,
    source_ts,
    NULL, NULL, ingested_at, NOW(),
    1 AS __op    -- 1 = DELETE for StarRocks PK table
FROM bronze.orders_raw
WHERE op = 'd'
  AND ingested_at >= DATE_SUB(NOW(), INTERVAL 30 MINUTE);
```

---

## Gold Layer (Business Aggregations)

Gold layer provides business-ready metrics, often implemented as Aggregate Key tables or Materialized Views.

```sql
-- Gold: daily order metrics (Aggregate Key)
CREATE TABLE gold.orders_daily (
    dt              DATE        NOT NULL,
    customer_id     BIGINT      NOT NULL,
    region          VARCHAR(64) NOT NULL,
    -- Metrics (Aggregate Key functions)
    order_count     BIGINT      SUM DEFAULT "0",
    total_revenue   DECIMAL(18,2) SUM DEFAULT "0",
    avg_amount      DECIMAL(10,2) REPLACE DEFAULT "0",
    first_order_ts  DATETIME    MIN DEFAULT "9999-12-31",
    last_order_ts   DATETIME    MAX DEFAULT "1970-01-01"
)
AGGREGATE KEY(dt, customer_id, region)
PARTITION BY RANGE(dt) (
    START ("2024-01-01") END ("2025-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(customer_id) BUCKETS 8
PROPERTIES ("replication_num" = "3");

-- Gold: daily refresh via INSERT OVERWRITE
INSERT OVERWRITE gold.orders_daily PARTITION (dt='{{ ds }}')
SELECT
    DATE(o.created_at)              AS dt,
    o.customer_id,
    COALESCE(c.region, 'unknown')   AS region,
    COUNT(*)                        AS order_count,
    SUM(o.amount)                   AS total_revenue,
    AVG(o.amount)                   AS avg_amount,
    MIN(o.created_at)               AS first_order_ts,
    MAX(o.updated_at)               AS last_order_ts
FROM silver.orders o
LEFT JOIN silver.dim_customers c ON o.customer_id = c.customer_id
WHERE DATE(o.created_at) = '{{ ds }}'
GROUP BY DATE(o.created_at), o.customer_id, COALESCE(c.region, 'unknown');
```

---

## Materialized View as Gold Layer

For frequently queried metrics, use async MV for automatic refresh:

```sql
-- Async MV: Gold-level metric, refreshes on schedule
CREATE MATERIALIZED VIEW gold_mv.revenue_by_region
DISTRIBUTED BY HASH(region) BUCKETS 8
REFRESH ASYNC EVERY (INTERVAL 1 HOUR)
AS
SELECT
    DATE_TRUNC('day', o.created_at) AS day,
    c.region,
    COUNT(*)                        AS order_count,
    SUM(o.amount)                   AS revenue
FROM silver.orders o
JOIN silver.dim_customers c ON o.customer_id = c.customer_id
WHERE o.status != 'cancelled'
GROUP BY DATE_TRUNC('day', o.created_at), c.region;
```

---

## Inter-Layer Pipeline Orchestration

Full pipeline: Bronze → Silver → Gold, orchestrated by Airflow:

```python
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta

@dag(
    dag_id="medallion_pipeline",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["starrocks", "medallion"],
)
def medallion_pipeline():

    @task
    def bronze_to_silver(ds: str = None):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        # Watermark: process bronze rows ingested in this interval
        # Silver is PK: INSERT upserts, covering all ops
        hook.run(f"""
            INSERT INTO silver.orders
            SELECT
                order_id, customer_id, amount, status,
                source_ts AS created_at, source_ts AS updated_at,
                NULL AS customer_tier, NULL AS region,
                ingested_at AS _bronze_ts, NOW() AS _silver_ts
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY source_ts DESC) AS rn
                FROM bronze.orders_raw
                WHERE DATE(ingested_at) = '{ds}'
                  AND op != 'd'
            ) t
            WHERE rn = 1
        """)

    @task
    def silver_to_gold(ds: str = None):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        hook.run(f"""
            INSERT OVERWRITE gold.orders_daily PARTITION (dt='{ds}')
            SELECT
                DATE(created_at)  AS dt,
                customer_id,
                COALESCE(region, 'unknown') AS region,
                COUNT(*)          AS order_count,
                SUM(amount)       AS total_revenue,
                AVG(amount)       AS avg_amount,
                MIN(created_at)   AS first_order_ts,
                MAX(updated_at)   AS last_order_ts
            FROM silver.orders
            WHERE DATE(created_at) = '{ds}'
            GROUP BY DATE(created_at), customer_id, COALESCE(region, 'unknown')
        """)

    @task
    def validate_layer(layer: str, ds: str = None, min_rows: int = 100):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        table = {"bronze": "bronze.orders_raw", "silver": "silver.orders", "gold": "gold.orders_daily"}[layer]
        date_col = "ingested_at" if layer == "bronze" else ("created_at" if layer == "silver" else "dt")
        count = hook.get_records(
            f"SELECT COUNT(*) FROM {table} WHERE DATE({date_col}) = '{ds}'"
        )[0][0]
        if count < min_rows:
            raise ValueError(f"{layer} layer: only {count} rows for {ds}, expected >= {min_rows}")
        print(f"{layer} layer validation passed: {count} rows")

    b2s = bronze_to_silver()
    s2g = silver_to_gold()
    val_s = validate_layer.override(task_id="validate_silver")(layer="silver")
    val_g = validate_layer.override(task_id="validate_gold")(layer="gold")

    b2s >> val_s >> s2g >> val_g


dag = medallion_pipeline()
```

---

## Schema Conventions

| Convention | Bronze | Silver | Gold |
|-----------|--------|--------|------|
| Partition column | `ingested_at` (load time) | `created_at` (business time) | `dt` (day) |
| Primary key | `ingestion_id` (surrogate) | Business key (`order_id`) | Dimension keys |
| Null handling | Allow nulls | Coalesce nulls | COALESCE in aggregation |
| CDC op column | Keep `op` column | Handle in load logic | Excluded |
| Retention | 90 days → cold storage | 2 years | 5 years |
| Storage medium | HDD (cheaper) | SSD for hot, HDD for cold | SSD |

---

## Anti-Patterns

1. **Applying transforms in Bronze** — Bronze is a raw landing zone; any bug in a transform requires reprocessing from source. Keep Bronze immutable.
2. **Using Aggregate Key in Silver** — Silver needs upsert semantics for CDC; Aggregate Key cannot express record replacement. Use Primary Key.
3. **INSERT INTO (append) for Gold** — daily partition re-runs append rather than replace; always use `INSERT OVERWRITE` for Gold partitions.
4. **No deduplication in Bronze → Silver** — CDC streams often produce multiple ops for the same key; always deduplicate with ROW_NUMBER before merging into Silver.
5. **Refreshing Gold MVs too frequently** — MV refresh consumes BE resources; tune to match business SLA, not maximum frequency.
6. **Missing `__op` for DELETE propagation** — If source deletes should flow to Silver, explicitly load `op=d` rows with `__op=1` header; omitting this causes deleted records to persist.

---

## References

- StarRocks table types: `docs.starrocks.io/docs/table_design/table_types/`
- INSERT OVERWRITE: `docs.starrocks.io/docs/loading/InsertInto/`
- Materialized Views: `docs.starrocks.io/docs/using_starrocks/Materialized_view/`
- Related skills: `[[starrocks-ddl-table-types]]`, `[[starrocks-partitioning]]`, `[[starrocks-materialized-views]]`, `[[starrocks-cdc-pipeline]]`, `[[medallion-architecture]]`
