---
name: dbt-starrocks-models
description: dbt + StarRocks models — dbt-starrocks adapter setup (profiles.yml), all materializations (table/view/incremental/ephemeral), incremental strategies (append/insert_overwrite/unique_key merge), StarRocks-specific model config (engine/keys/partition_by/distributed_by/properties), Duplicate/Aggregate/Primary Key table DDL from dbt, partition_by with date_trunc, on_schema_change behavior
---

# dbt + StarRocks Models

## When to Use

- Building dbt transformation pipelines targeting StarRocks
- Defining StarRocks table DDL (key type, partitioning, distribution) via dbt model config
- Incremental models with StarRocks-native INSERT OVERWRITE or upsert strategies
- Mixing StarRocks internal tables with external Iceberg catalog tables in dbt

---

## Installation and Setup

```bash
pip install dbt-starrocks
# dbt-starrocks requires dbt-core >= 1.5.0
```

### profiles.yml

```yaml
starrocks_project:
  target: prod
  outputs:
    dev:
      type: starrocks
      host: sr-fe.internal
      port: 9030
      schema: dbt_dev
      username: dbt_user
      password: "{{ env_var('STARROCKS_PASSWORD') }}"
      database: sales
      connect_timeout: 10

    prod:
      type: starrocks
      host: sr-fe-prod.internal
      port: 9030
      schema: sales
      username: dbt_prod
      password: "{{ env_var('STARROCKS_PASSWORD_PROD') }}"
      database: sales
```

---

## Table Materialization

dbt-starrocks generates `CREATE TABLE ... AS SELECT` DDL with StarRocks-specific properties.

### Duplicate Key Table

```sql
-- models/staging/stg_orders_raw.sql
{{ config(
    materialized='table',
    engine='OLAP',
    keys=['order_id', 'created_at'],
    table_type='DUPLICATE',
    distributed_by='HASH(order_id)',
    buckets=16,
    partition_by={
        "field": "created_at",
        "data_type": "date",
        "granularity": "day"
    },
    properties={
        "replication_num": "3",
        "storage_medium": "HDD"
    }
) }}

SELECT
    order_id,
    customer_id,
    amount,
    status,
    created_at,
    updated_at
FROM {{ source('raw', 'orders_raw') }}
```

### Primary Key Table (Upsert)

```sql
-- models/silver/orders.sql
{{ config(
    materialized='table',
    engine='OLAP',
    keys=['order_id'],
    table_type='PRIMARY',
    distributed_by='HASH(order_id)',
    buckets=16,
    partition_by={
        "field": "created_at",
        "data_type": "date",
        "granularity": "month"
    },
    properties={
        "enable_persistent_index": "true",
        "replication_num": "3"
    }
) }}

SELECT
    order_id,
    customer_id,
    amount,
    status,
    created_at,
    updated_at
FROM {{ ref('stg_orders_raw') }}
WHERE amount > 0
  AND status IS NOT NULL
```

### Aggregate Key Table

```sql
-- models/gold/orders_daily_agg.sql
{{ config(
    materialized='table',
    engine='OLAP',
    keys=['dt', 'customer_id', 'region'],
    table_type='AGGREGATE',
    distributed_by='HASH(customer_id)',
    buckets=8,
    partition_by={
        "field": "dt",
        "data_type": "date",
        "granularity": "month"
    },
    properties={
        "replication_num": "3"
    }
) }}

SELECT
    DATE(created_at)                            AS dt,
    customer_id,
    COALESCE(c.region, 'unknown')               AS region,
    COUNT(*)                                     AS order_count,    -- SUM in Aggregate Key
    SUM(amount)                                  AS total_revenue,
    MAX(updated_at)                              AS last_updated
FROM {{ ref('orders') }} o
LEFT JOIN {{ ref('dim_customers') }} c USING (customer_id)
GROUP BY DATE(created_at), customer_id, COALESCE(c.region, 'unknown')
```

---

## Incremental Materialization

### append Strategy

```sql
-- models/incremental/events_append.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    keys=['event_id'],
    table_type='DUPLICATE',
    distributed_by='HASH(event_id)',
    buckets=32,
    partition_by={
        "field": "event_ts",
        "data_type": "datetime",
        "granularity": "day"
    }
) }}

SELECT event_id, user_id, event_type, event_ts
FROM {{ source('raw', 'events') }}

{% if is_incremental() %}
WHERE event_ts > (SELECT MAX(event_ts) FROM {{ this }})
{% endif %}
```

### insert_overwrite Strategy (Partition Replace)

```sql
-- models/incremental/orders_daily.sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    keys=['dt', 'customer_id'],
    table_type='DUPLICATE',
    distributed_by='HASH(customer_id)',
    buckets=8,
    partition_by={
        "field": "dt",
        "data_type": "date",
        "granularity": "day"
    }
) }}

SELECT
    DATE(created_at) AS dt,
    customer_id,
    SUM(amount) AS revenue,
    COUNT(*) AS orders
FROM {{ ref('stg_orders_raw') }}

{% if is_incremental() %}
-- Only recompute partitions newer than 3 days ago (late-arriving data window)
WHERE DATE(created_at) >= DATE_SUB(CURDATE(), INTERVAL 3 DAY)
{% endif %}

GROUP BY DATE(created_at), customer_id
```

### unique_key Strategy (Upsert via Primary Key)

```sql
-- models/incremental/customers.sql
{{ config(
    materialized='incremental',
    incremental_strategy='unique_key',
    unique_key='customer_id',
    keys=['customer_id'],
    table_type='PRIMARY',
    distributed_by='HASH(customer_id)',
    buckets=8,
    properties={"enable_persistent_index": "true"}
) }}

SELECT
    customer_id,
    customer_name,
    email,
    region,
    tier,
    updated_at
FROM {{ source('raw', 'customers') }}

{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

---

## View Materialization

```sql
-- models/marts/vw_revenue_summary.sql
{{ config(materialized='view') }}

SELECT
    region,
    DATE_TRUNC('month', created_at) AS month,
    SUM(amount) AS revenue,
    COUNT(*) AS orders
FROM {{ ref('orders') }}
GROUP BY region, DATE_TRUNC('month', created_at)
```

---

## Ephemeral Models (CTEs)

```sql
-- models/intermediate/int_orders_enriched.sql
{{ config(materialized='ephemeral') }}

SELECT
    o.*,
    c.region,
    c.tier AS customer_tier
FROM {{ ref('stg_orders_raw') }} o
LEFT JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id
```

Ephemeral models are inlined as CTEs in the parent model — no table is created.

---

## dbt_project.yml StarRocks Config

```yaml
# dbt_project.yml
name: starrocks_dbt
version: "1.0"
profile: starrocks_project

models:
  starrocks_dbt:
    staging:
      +materialized: table
      +table_type: DUPLICATE
      +replication_num: "1"  # dev: 1 replica
    silver:
      +materialized: incremental
      +incremental_strategy: unique_key
      +table_type: PRIMARY
    gold:
      +materialized: table
      +table_type: AGGREGATE
```

---

## Partition Configuration Options

```sql
-- Time-based partition (most common)
partition_by={
    "field": "created_at",
    "data_type": "date",    -- or "datetime"
    "granularity": "day"    -- "hour", "day", "month", "year"
}

-- For expression-based partition (RANGE with VALUES LESS THAN):
-- Not directly supported in dbt-starrocks config;
-- use pre-hook to create partitions manually or dynamic partition creation:
properties={
    "dynamic_partition.enable": "true",
    "dynamic_partition.time_unit": "DAY",
    "dynamic_partition.start": "-30",
    "dynamic_partition.end": "7",
    "dynamic_partition.prefix": "p",
    "dynamic_partition.buckets": "16"
}
```

---

## on_schema_change Behavior

```sql
{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns'  -- or 'fail', 'ignore', 'sync_all_columns'
) }}
```

| Value | StarRocks behavior |
|-------|-------------------|
| `fail` | Raise error if schema changed |
| `ignore` | Keep running, don't add new columns |
| `append_new_columns` | Add new columns, keep existing data |
| `sync_all_columns` | Full table rebuild (expensive) |

For Primary Key tables: `sync_all_columns` drops and recreates — use cautiously.

---

## Sources and Refs

```yaml
# models/sources.yml
sources:
  - name: raw
    database: raw_layer
    schema: ingest
    tables:
      - name: orders_raw
        loaded_at_field: ingested_at
        freshness:
          warn_after: {count: 2, period: hour}
          error_after: {count: 6, period: hour}
      - name: customers
```

```sql
-- Reference source
SELECT * FROM {{ source('raw', 'orders_raw') }}

-- Reference model
SELECT * FROM {{ ref('stg_orders_raw') }}

-- Reference model in another project (cross-project)
SELECT * FROM {{ ref('other_project', 'dim_products') }}
```

---

## Anti-Patterns

1. **Not specifying `table_type`** — dbt-starrocks defaults to DUPLICATE; most Silver/Gold tables need PRIMARY or AGGREGATE, which requires explicit config.
2. **Using `unique_key` strategy on Duplicate Key tables** — StarRocks can't upsert on Duplicate Key; use PRIMARY KEY table for upsert.
3. **`incremental_strategy='append'` with `on_schema_change='ignore'`** — new source columns silently dropped; set `append_new_columns` for evolving sources.
4. **No `partition_by` on large incremental tables** — full table scans on every incremental run; always partition by a date column.
5. **`buckets` too low for high-concurrency tables** — default 16 buckets may cause hotspots; set to `4 * BE count` for fact tables.
6. **Using ephemeral models for heavy transforms** — ephemeral inlines SQL as CTEs, which can produce very large query plans; materialize intermediate heavy aggregations.

---

## References

- dbt-starrocks adapter: `github.com/StarRocks/starrocks/tree/main/contrib/dbt-connector`
- dbt incremental models: `docs.getdbt.com/docs/build/incremental-models`
- Related skills: `[[dbt-core]]`, `[[starrocks-ddl-table-types]]`, `[[starrocks-partitioning]]`, `[[dbt-starrocks-performance]]`
