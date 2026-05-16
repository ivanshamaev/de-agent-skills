---
name: medallion_architecture
description: Use when designing or implementing Medallion (Bronze/Silver/Gold) data lakehouse architecture — layer definitions, DDL for each layer, DML load and update patterns, incremental pipeline design, deduplication strategies (row_number/merge/hash/CDC/watermark), schema evolution, data quality gates, partitioning per layer, late-arriving data, orchestration patterns, and best practices for Iceberg-based lakehouses.
---

# Medallion Architecture for Data Lakehouse

## When to Use

Use this skill when:
- Designing a new data lakehouse from scratch using Bronze/Silver/Gold layers
- Deciding what belongs in Bronze vs Silver vs Gold
- Writing DDL for any Medallion layer on Iceberg/Parquet (Trino, Spark SQL, dbt)
- Implementing incremental pipeline patterns: watermark, CDC, full-refresh, streaming
- Choosing and implementing a deduplication strategy
- Handling schema evolution, late-arriving data, or data quality gates between layers
- Designing partitioning and sorting strategies per layer
- Structuring Airflow DAGs or dbt projects around the Medallion pattern

---

## Architecture Overview

Medallion organises a data lakehouse into **three quality layers**. Data flows in one direction: Bronze → Silver → Gold. Each layer applies progressively more transformation and enforces progressively stricter quality rules.

```
External Sources
(RDBMS, Kafka, S3 files, APIs, CDC streams)
        │
        ▼
┌───────────────────────────────────────────────────────┐
│  BRONZE  (raw zone)                                   │
│  • Exact copy of source data                          │
│  • Append-only, no transforms                         │
│  • Schema-on-read, full fidelity                      │
│  • Partitioned by ingestion date                      │
│  • Preserves duplicates and all source errors         │
└───────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────┐
│  SILVER  (cleansed zone)                              │
│  • Typed, validated, deduplicated                     │
│  • Conformed naming and domains                       │
│  • Enriched with lookups                              │
│  • ACID upserts via MERGE (Iceberg format_version=2)  │
│  • Partitioned by business event date                 │
└───────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────┐
│  GOLD  (serving zone)                                 │
│  • Aggregates, KPIs, wide tables, dimensional models  │
│  • Business-domain scoped                             │
│  • Optimised for BI tools and analysts                │
│  • Partitioned by reporting period                    │
└───────────────────────────────────────────────────────┘
```

**Core principle**: never skip a layer. Bronze → Silver → Gold is the only direction of flow. Gold is never loaded directly from source.

---

## Layer 1: Bronze

### Purpose and Rules

Bronze is the **immutable raw archive**. It stores data exactly as it arrived from the source — format, errors, duplicates and all.

Rules:
1. **Never transform source values** — cast to string if the schema is unknown; preserve nulls as-is
2. **Always append** — Bronze rows are never updated or deleted
3. **Capture ingestion metadata** alongside source fields
4. **One Bronze table per source entity** — don't merge multiple sources into one Bronze table
5. **Schema evolution is allowed** — add columns as the source schema changes; use `schema_evolution_mode=union`

### Mandatory Bronze columns

| Column | Type | Description |
|---|---|---|
| `_ingest_ts` | `timestamp` | When this row was loaded into Bronze |
| `_source_file` | `varchar` | Source file name, Kafka topic, or API endpoint |
| `_source_system` | `varchar` | Identifies the upstream system |
| `_batch_id` | `varchar` | Pipeline run ID for traceability |
| `_row_hash` | `char(32)` | MD5 of all source columns — used for dedup in Silver |

### DDL — Bronze table

```sql
-- Bronze: raw orders from CRM system
create table bronze.crm_orders (
    -- Source columns (all as varchar or original types — no casting)
    order_id            varchar,
    customer_id         varchar,
    product_id          varchar,
    quantity            varchar,          -- keep as string if source type is uncertain
    unit_price          varchar,
    order_status        varchar,
    order_timestamp     varchar,          -- raw string timestamp from source
    shipping_address    varchar,
    raw_payload         varchar,          -- full JSON payload if available

    -- Ingestion metadata
    _ingest_ts          timestamp         not null,
    _source_file        varchar           not null,
    _source_system      varchar           not null,
    _batch_id           varchar           not null,
    _row_hash           char(32)          not null    -- md5 of source columns
)
with (
    format           = 'PARQUET',
    format_version   = 2,
    partitioning     = ARRAY['day(_ingest_ts)'],      -- partition by ingestion date
    sorted_by        = ARRAY['order_id']
);
```

### DML — Bronze ingestion (append-only)

```sql
-- Full append: every pipeline run adds rows. Never filters for "already loaded".
insert into bronze.crm_orders
select
    order_id,
    customer_id,
    product_id,
    cast(quantity   as varchar),
    cast(unit_price as varchar),
    order_status,
    cast(order_timestamp as varchar),
    shipping_address,
    null                                            as raw_payload,
    current_timestamp                               as _ingest_ts,
    '{{ source_file }}'                             as _source_file,
    'CRM_SYSTEM'                                    as _source_system,
    '{{ batch_id }}'                                as _batch_id,
    md5(concat_ws('||',
        coalesce(order_id, ''),
        coalesce(customer_id, ''),
        coalesce(cast(order_timestamp as varchar), '')
    ))                                              as _row_hash
from external.crm_orders_landed;
```

### Bronze for CDC (Debezium / Kafka)

For CDC streams, Bronze captures the operation type and before/after record states:

```sql
create table bronze.cdc_crm_customers (
    -- CDC envelope fields
    _cdc_op             char(1)     not null,    -- I=Insert, U=Update, D=Delete
    _cdc_ts             timestamp   not null,    -- source commit timestamp
    _cdc_lsn            varchar,                 -- log sequence number (PostgreSQL WAL position)
    _cdc_transaction_id varchar,

    -- After-image (new row state for I and U; null for D)
    customer_id         varchar,
    full_name           varchar,
    email               varchar,
    country_code        varchar,
    segment             varchar,
    updated_at          varchar,

    -- Before-image (previous state for U; null for I)
    before_email        varchar,
    before_segment      varchar,

    -- Ingestion metadata
    _ingest_ts          timestamp   not null,
    _source_system      varchar     not null,
    _batch_id           varchar     not null
)
with (
    format         = 'PARQUET',
    format_version = 2,
    partitioning   = ARRAY['day(_cdc_ts)']
);
```

---

## Layer 2: Silver

### Purpose and Rules

Silver is the **single source of truth** for operational data. It is clean, typed, deduplicated, and validated.

Rules:
1. **One authoritative row per business entity key** (e.g. one row per `order_id`) — or one row per key per version if tracking history
2. **Full type casting**: all dates, numbers, booleans cast to proper types
3. **Validated**: NULLs handled, referential integrity checked, domain values validated
4. **Conformed naming**: use consistent column names across all Silver tables
5. **MERGE-based upsert**: updated records from Silver sources are handled via MERGE
6. **Audit columns preserved**: `created_at`, `updated_at`, `_silver_loaded_ts`, `_source_system`
7. **Partitioned by event/business date**, not ingestion date

### Mandatory Silver columns

| Column | Type | Description |
|---|---|---|
| `_silver_loaded_ts` | `timestamp` | When this version was written to Silver |
| `_source_system` | `varchar` | Originating source |
| `_is_deleted` | `boolean` | Soft-delete flag for CDC-sourced tables |
| `created_at` | `timestamp` | Business creation timestamp from source |
| `updated_at` | `timestamp` | Business update timestamp from source |

### DDL — Silver table

```sql
-- Silver: clean, deduplicated orders
create table silver.orders (
    -- Business key
    order_id            varchar(50)     not null,

    -- Foreign keys (conformed names)
    customer_id         varchar(50),
    product_id          varchar(50),

    -- Typed and validated attributes
    quantity            int             not null,
    unit_price          decimal(18, 2)  not null,
    discount_amount     decimal(18, 2)  not null  default 0,
    order_status        varchar(30)     not null,
    shipping_address    varchar(500),

    -- Business timestamps
    order_ts            timestamp       not null,
    shipped_ts          timestamp,
    delivered_ts        timestamp,
    created_at          timestamp       not null,
    updated_at          timestamp       not null,

    -- Audit
    _silver_loaded_ts   timestamp       not null,
    _source_system      varchar(100)    not null,
    _is_deleted         boolean         not null  default false
)
with (
    format           = 'PARQUET',
    format_version   = 2,
    partitioning     = ARRAY['day(order_ts)'],        -- business event date
    sorted_by        = ARRAY['customer_id', 'order_ts']
);
```

### DML — Silver MERGE (upsert from Bronze)

The core Silver load pattern: read from Bronze, deduplicate, cast, validate, then MERGE into Silver.

```sql
-- Step 1: prepare deduped staging from Bronze
with bronze_deduped as (
    select
        -- Cast to proper types
        order_id,
        customer_id,
        product_id,
        try_cast(quantity    as int)           as quantity,
        try_cast(unit_price  as decimal(18,2)) as unit_price,
        coalesce(try_cast(unit_price as decimal(18,2)) * try_cast(quantity as int), 0) as line_total,
        coalesce(order_status, 'UNKNOWN')      as order_status,
        shipping_address,
        try_cast(order_timestamp as timestamp) as order_ts,
        _ingest_ts                             as created_at,
        _ingest_ts                             as updated_at,
        _source_system,
        -- Dedup: keep latest version per order_id within this batch
        row_number() over (
            partition by order_id
            order by try_cast(order_timestamp as timestamp) desc nulls last,
                     _ingest_ts desc
        ) as rn
    from bronze.crm_orders
    where day(_ingest_ts) >= current_date - interval '3' day   -- incremental window
      and order_id is not null
      and try_cast(quantity as int) > 0
      and try_cast(unit_price as decimal(18,2)) >= 0
),
staging as (
    select * from bronze_deduped where rn = 1
)
-- Step 2: MERGE into Silver
merge into silver.orders as target
using staging as source
on target.order_id = source.order_id
when matched and source.updated_at > target.updated_at then
    update set
        product_id          = source.product_id,
        quantity            = source.quantity,
        unit_price          = source.unit_price,
        order_status        = source.order_status,
        shipping_address    = source.shipping_address,
        order_ts            = source.order_ts,
        updated_at          = source.updated_at,
        _silver_loaded_ts   = current_timestamp
when not matched then
    insert (order_id, customer_id, product_id, quantity, unit_price,
            discount_amount, order_status, shipping_address,
            order_ts, created_at, updated_at, _silver_loaded_ts, _source_system, _is_deleted)
    values (source.order_id, source.customer_id, source.product_id,
            source.quantity, source.unit_price, 0, source.order_status,
            source.shipping_address, source.order_ts,
            source.created_at, source.updated_at,
            current_timestamp, source._source_system, false);
```

### DML — Silver from CDC (soft-deletes)

```sql
merge into silver.customers as target
using (
    -- Latest CDC event per customer_id
    select
        customer_id,
        full_name,
        email,
        country_code,
        segment,
        try_cast(updated_at as timestamp) as updated_at,
        _cdc_op,
        _cdc_ts,
        row_number() over (partition by customer_id order by _cdc_ts desc) as rn
    from bronze.cdc_crm_customers
    where day(_cdc_ts) >= current_date - interval '1' day
) as source
on target.customer_id = source.customer_id and source.rn = 1
when matched and source._cdc_op = 'D' then
    update set _is_deleted = true, updated_at = source._cdc_ts, _silver_loaded_ts = current_timestamp
when matched and source._cdc_op in ('U', 'I') and source.updated_at > target.updated_at then
    update set
        full_name          = source.full_name,
        email              = source.email,
        country_code       = source.country_code,
        segment            = source.segment,
        updated_at         = source.updated_at,
        _is_deleted        = false,
        _silver_loaded_ts  = current_timestamp
when not matched and source._cdc_op != 'D' then
    insert (customer_id, full_name, email, country_code, segment,
            created_at, updated_at, _silver_loaded_ts, _source_system, _is_deleted)
    values (source.customer_id, source.full_name, source.email,
            source.country_code, source.segment,
            source._cdc_ts, source._cdc_ts, current_timestamp, 'CRM_SYSTEM', false);
```

---

## Layer 3: Gold

### Purpose and Rules

Gold serves **business consumers**: BI tools, analysts, ML pipelines, APIs. It contains aggregated, denormalised, or dimensionally modelled tables.

Rules:
1. **Business-domain scoped** — one set of Gold tables per domain (finance, marketing, logistics)
2. **No raw or operational data** — only derived, computed, or aggregated results
3. **Stable column names and types** — Gold is a contract with consumers; breaking changes require versioning
4. **Rebuilt fully or incrementally** — Gold tables are typically truncate-and-reload or large-batch incremental; SLA is less critical than accuracy
5. **Optimised for read** — sorted_by on the most common filter column; use materialized views where connector supports them

### DDL — Gold aggregate table

```sql
-- Gold: daily revenue by region and product category
create table gold.revenue_daily (
    dt                  date            not null,
    region              varchar(50)     not null,
    product_category    varchar(100)    not null,
    order_count         bigint          not null,
    gross_revenue       decimal(18, 2)  not null,
    net_revenue         decimal(18, 2)  not null,
    avg_order_value     decimal(18, 2)  not null,
    unique_customers    bigint          not null,
    _gold_loaded_ts     timestamp       not null
)
with (
    format         = 'PARQUET',
    format_version = 2,
    partitioning   = ARRAY['month(dt)'],
    sorted_by      = ARRAY['dt', 'region']
);
```

### DDL — Gold wide flat table (for BI / self-service)

```sql
-- Gold: one wide row per order (join of Silver entities)
create table gold.orders_wide (
    order_id            varchar(50)     not null,
    order_ts            timestamp       not null,
    order_status        varchar(30),
    -- Customer attributes (denormalized)
    customer_id         varchar(50),
    customer_name       varchar(400),
    customer_country    varchar(100),
    customer_segment    varchar(50),
    -- Product attributes (denormalized)
    product_id          varchar(50),
    product_name        varchar(200),
    product_category    varchar(100),
    brand               varchar(100),
    -- Measures
    quantity            int,
    unit_price          decimal(18, 2),
    gross_amount        decimal(18, 2),
    net_amount          decimal(18, 2),
    -- Audit
    _gold_loaded_ts     timestamp       not null
)
with (
    format         = 'PARQUET',
    format_version = 2,
    partitioning   = ARRAY['month(order_ts)'],
    sorted_by      = ARRAY['customer_id', 'order_ts']
);
```

### DML — Gold rebuild (truncate + reload)

```sql
-- For Gold tables rebuilt daily from scratch
insert overwrite gold.revenue_daily
select
    cast(o.order_ts as date)            as dt,
    c.country_code                      as region,
    p.category                          as product_category,
    count(distinct o.order_id)          as order_count,
    sum(o.quantity * o.unit_price)      as gross_revenue,
    sum(o.quantity * o.unit_price
        - o.discount_amount)            as net_revenue,
    avg(o.quantity * o.unit_price)      as avg_order_value,
    count(distinct o.customer_id)       as unique_customers,
    current_timestamp                   as _gold_loaded_ts
from silver.orders o
left join silver.customers c on o.customer_id = c.customer_id
    and c._is_deleted = false
left join silver.products  p on o.product_id  = p.product_id
where o._is_deleted = false
  and o.order_status not in ('CANCELLED', 'TEST')
group by 1, 2, 3;
```

### DML — Gold incremental append

```sql
-- For Gold tables that accumulate daily aggregates (no rewrite of history)
insert into gold.revenue_daily
select
    cast(o.order_ts as date)        as dt,
    c.country_code                  as region,
    p.category                      as product_category,
    count(distinct o.order_id)      as order_count,
    sum(o.quantity * o.unit_price)  as gross_revenue,
    sum(o.quantity * o.unit_price - o.discount_amount) as net_revenue,
    avg(o.quantity * o.unit_price)  as avg_order_value,
    count(distinct o.customer_id)   as unique_customers,
    current_timestamp               as _gold_loaded_ts
from silver.orders o
left join silver.customers c on o.customer_id = c.customer_id
left join silver.products  p on o.product_id  = p.product_id
where cast(o.order_ts as date) = current_date - interval '1' day   -- yesterday only
  and o._is_deleted = false
  and o.order_status not in ('CANCELLED', 'TEST')
group by 1, 2, 3;
```

---

## Deduplication Strategies

### 1. ROW_NUMBER window function (Bronze → Silver)

The most common pattern. Use when the source has multiple rows per key with a natural "latest wins" ordering.

```sql
-- Keep the latest row per order_id, ordered by source timestamp then ingestion time
with ranked as (
    select *,
        row_number() over (
            partition by order_id
            order by
                try_cast(order_timestamp as timestamp) desc nulls last,
                _ingest_ts desc
        ) as rn
    from bronze.crm_orders
    where order_id is not null
)
select * from ranked where rn = 1;
```

**When to use**: Full-scan or recent-window dedup of a Bronze snapshot where every batch contains all records (full extract) or the latest version of changed records.

### 2. MERGE-based upsert dedup (Silver)

Handles incremental loads where only changed/new records arrive. Relies on the target table already holding the "winner" from previous loads.

```sql
merge into silver.orders as target
using (
    select *, row_number() over (partition by order_id order by updated_at desc) as rn
    from bronze.crm_orders
    where day(_ingest_ts) >= current_date - interval '1' day
) as source
on target.order_id = source.order_id and source.rn = 1
when matched and source.updated_at > target.updated_at then update set ...
when not matched then insert ...;
```

**When to use**: Incremental loads from CDC or delta extracts where you want the latest state.

### 3. Hash-based exact dedup (Bronze → Bronze or Bronze → Silver)

Removes exact duplicate rows (same values in all columns) using a pre-computed row hash.

```sql
-- Remove exact duplicates: keep one row per unique _row_hash per business key
with deduped as (
    select *,
        row_number() over (
            partition by order_id, _row_hash
            order by _ingest_ts asc    -- keep first occurrence
        ) as rn
    from bronze.crm_orders
)
select * from deduped where rn = 1;
```

**When to use**: When the source system sends the same record multiple times with no changes (idempotent retries, duplicate file deliveries).

### 4. Event-time watermark dedup (Streaming / CDC)

For streaming pipelines with out-of-order events. Only accept events within a watermark window; drop late arrivals beyond the threshold.

```sql
-- Accept events within 48h of current processing time; drop older
with windowed as (
    select *,
        row_number() over (
            partition by order_id
            order by event_ts desc
        ) as rn
    from bronze.kafka_orders
    where event_ts >= current_timestamp - interval '48' hour   -- watermark
      and event_ts <= current_timestamp                         -- no future events
)
select * from windowed where rn = 1;
```

**When to use**: Kafka/Flink-sourced tables where events may arrive out of order; streaming lakehouse loads.

### 5. Group-by aggregate dedup (factless facts or count tables)

When rows represent events that should be counted once per key per period, use GROUP BY to collapse duplicates while summing measures.

```sql
-- Collapse duplicate click events to one row per (session_id, page_url, hour)
select
    date_trunc('hour', event_ts)    as event_hour,
    session_id,
    page_url,
    count(*)                        as click_count,
    min(event_ts)                   as first_click_ts,
    max(event_ts)                   as last_click_ts
from bronze.web_clicks
where day(_ingest_ts) >= current_date - interval '1' day
group by 1, 2, 3;
```

**When to use**: When duplicates are logically equivalent and the measure is the count of events.

### 6. CDC-aware dedup (ordered by LSN or offset)

For CDC streams, use the log sequence number or Kafka offset to establish a strict ordering before deduplication.

```sql
-- Keep the final state per customer_id based on CDC LSN ordering
with cdc_ordered as (
    select *,
        row_number() over (
            partition by customer_id
            order by
                _cdc_lsn  desc,     -- LSN is strictly ordered within a transaction
                _cdc_ts   desc
        ) as rn
    from bronze.cdc_crm_customers
    where day(_cdc_ts) >= current_date - interval '1' day
)
select * from cdc_ordered where rn = 1 and _cdc_op != 'D';   -- exclude deletes
```

**When to use**: Debezium or log-based CDC where you need to replay the final state of each record.

### 7. Surrogate key collision check (pre-load validation)

Before inserting into Silver or Gold, verify that the hash-based surrogate key doesn't collide:

```sql
-- Detect potential hash collisions before load
select _row_hash, count(distinct order_id) as distinct_keys
from bronze.crm_orders
group by 1
having count(distinct order_id) > 1;
-- If any rows returned → collision; investigate the hash formula
```

---

## Pipeline Design Patterns

### Pattern 1: Full Extract → Bronze append → Silver MERGE

Best for: small/medium sources (< 50M rows), source supports full extract, simple ops.

```
[Source DB] → full SELECT → [Bronze append] → [Silver MERGE with dedup]
Pipeline frequency: daily
Latency: hours
```

```sql
-- Bronze: append today's full extract
insert into bronze.crm_orders
select *, current_timestamp as _ingest_ts, 'DAILY_FULL' as _batch_id, 'CRM' as _source_system,
    md5(concat_ws('||', coalesce(order_id,''), coalesce(order_timestamp,''))) as _row_hash
from external.crm_orders_snapshot;

-- Silver: MERGE from Bronze (last 3 days window to catch late updates)
merge into silver.orders as t
using (
    select *, row_number() over (partition by order_id order by try_cast(order_timestamp as timestamp) desc) as rn
    from bronze.crm_orders
    where day(_ingest_ts) >= current_date - interval '3' day
) as s on t.order_id = s.order_id and s.rn = 1
when matched and try_cast(s.order_timestamp as timestamp) > t.updated_at then update set ...
when not matched then insert ...;
```

### Pattern 2: Watermark incremental extract

Best for: large sources with a reliable `updated_at` column; minimises extract cost.

```sql
-- Read watermark from Silver (max updated_at already loaded)
-- [stored in pipeline metadata table]
select coalesce(max(updated_at), timestamp '2020-01-01 00:00:00') as watermark
from silver.orders;

-- Bronze: append only rows newer than watermark
insert into bronze.crm_orders
select *, current_timestamp as _ingest_ts, '{{ batch_id }}' as _batch_id, 'CRM' as _source_system,
    md5(concat_ws('||', order_id, cast(updated_at as varchar))) as _row_hash
from external.crm_orders
where updated_at > '{{ watermark }}'::timestamp;   -- parameterised from pipeline
```

**Watermark metadata table:**

```sql
create table pipeline.watermarks (
    pipeline_name       varchar(200)    primary key,
    last_watermark_ts   timestamp       not null,
    last_run_ts         timestamp       not null,
    rows_loaded         bigint          not null  default 0,
    status              varchar(20)     not null  default 'SUCCESS'
);

-- Update after successful load
update pipeline.watermarks
set last_watermark_ts = (select max(updated_at) from silver.orders),
    last_run_ts       = current_timestamp,
    status            = 'SUCCESS'
where pipeline_name = 'crm_orders_to_silver';
```

### Pattern 3: CDC streaming → Bronze → Silver micro-batch

Best for: near-realtime requirements, source is OLTP DB, Debezium + Kafka available.

```
PostgreSQL WAL → Debezium → Kafka → Spark Structured Streaming / Flink
  → Bronze (Iceberg, Kafka sink, micro-batch every 5 min)
  → Silver (MERGE every 15 min, CDC-aware dedup by LSN)
  → Gold (rebuild hourly aggregates)
```

```sql
-- Spark Structured Streaming Bronze write (concept)
-- kafka_df.selectExpr("CAST(value AS STRING) as raw_json", ...)
--   .withColumn("_ingest_ts", current_timestamp())
--   .withColumn("_cdc_op", get_json_object("raw_json", "$.op"))
--   .writeStream.format("iceberg").outputMode("append")
--   .trigger(processingTime="5 minutes").start("bronze.cdc_crm_orders")

-- Silver MERGE from CDC Bronze (scheduled every 15 min)
merge into silver.orders as target
using (
    with latest_cdc as (
        select *,
            row_number() over (partition by order_id order by _cdc_lsn desc) as rn
        from bronze.cdc_crm_orders
        where _ingest_ts >= current_timestamp - interval '30' minute
    )
    select * from latest_cdc where rn = 1
) as source
on target.order_id = source.order_id
when matched and source._cdc_op = 'D' then
    update set _is_deleted = true, updated_at = source._cdc_ts
when matched and source._cdc_op in ('U', 'I') then
    update set ...
when not matched and source._cdc_op != 'D' then
    insert ...;
```

### Pattern 4: Full-refresh Gold rebuild

For Gold tables where recalculating from Silver is faster than maintaining incremental state.

```sql
-- Truncate + reload: Gold is always correct as of last run
-- Implemented as INSERT OVERWRITE on partitioned Iceberg table

-- Rebuild only affected partitions (partition-pruned overwrite)
insert overwrite gold.revenue_daily
-- WHERE clause limits which partitions are overwritten in Iceberg
-- SELECT filters the same partitions from Silver
select ... from silver.orders
where cast(order_ts as date) >= current_date - interval '7' day;
```

### Pattern 5: Backfill / reprocessing

When Silver or Gold logic changes, reprocess a historical range from Bronze.

```sql
-- Step 1: delete the affected Gold partition range
delete from gold.revenue_daily
where dt between date '2024-01-01' and date '2024-01-31';

-- Step 2: reprocess from Bronze with updated logic
insert into gold.revenue_daily
select ...
from silver.orders
where cast(order_ts as date) between date '2024-01-01' and date '2024-01-31';
```

For Silver backfill, use Bronze as the source but with the **full history window** (not the incremental window):

```sql
-- Silver backfill: wipe the partition, reload from Bronze with new dedup logic
delete from silver.orders
where day(order_ts) between date '2024-01-01' and date '2024-01-31';

merge into silver.orders as target
using (
    select *, row_number() over (partition by order_id order by try_cast(order_timestamp as timestamp) desc) as rn
    from bronze.crm_orders
    where try_cast(order_timestamp as date) between date '2024-01-01' and date '2024-01-31'
) as source on target.order_id = source.order_id and source.rn = 1
when matched then update set ...
when not matched then insert ...;
```

---

## Schema Evolution

### Bronze: additive-only changes (safe)

Bronze accepts schema changes from the source without modifying existing rows.

```sql
-- New column appeared in the source → add to Bronze table
alter table bronze.crm_orders add column loyalty_points varchar;

-- Historical rows will have NULL in the new column — this is expected
```

Use `sorted_by` + `format_version=2` on Bronze Iceberg tables so OPTIMIZE can rewrite old files with the new column after adding it.

### Silver: controlled evolution

Silver evolves in sync with business rules. Add columns without breaking existing consumers.

```sql
-- Add new attribute to Silver
alter table silver.orders add column loyalty_points_earned int;

-- Populate it for existing rows in bulk
update silver.orders
set loyalty_points_earned = cast(b.loyalty_points as int)
from bronze.crm_orders b
where silver.orders.order_id = b.order_id
  and b.loyalty_points is not null
  and silver.orders.loyalty_points_earned is null;
```

### Gold: versioning for breaking changes

When a Gold table changes in a way that breaks BI reports, create a versioned table rather than altering in-place.

```sql
-- gold.revenue_daily_v2 — new columns, changed aggregation logic
-- Keep gold.revenue_daily running for existing consumers
-- Cutover consumers to v2 on agreed date, then drop v1
```

---

## Data Quality Gates

### Between Bronze and Silver

Add explicit DQ checks before the Silver MERGE executes. Fail the pipeline if critical checks fail; log warnings for non-critical.

```sql
-- DQ checks as assertions (fail pipeline if count > 0)

-- 1. Null business key
select count(*) as null_key_count from bronze.crm_orders
where order_id is null and day(_ingest_ts) = current_date;
-- Expected: 0. Fail if > 0.

-- 2. Invalid cast
select count(*) as bad_quantity from bronze.crm_orders
where try_cast(quantity as int) is null and quantity is not null
  and day(_ingest_ts) = current_date;
-- Expected: 0. Fail if > threshold.

-- 3. Referential integrity (orders must reference known customers)
select count(*) as unknown_customers
from (select distinct customer_id from bronze.crm_orders where day(_ingest_ts) = current_date) o
where not exists (select 1 from silver.customers c where c.customer_id = o.customer_id);
-- Log as warning; Silver may receive customers in the next batch.

-- 4. Volume anomaly (today's load < 50% of 7-day average → suspicious)
with daily_counts as (
    select day(_ingest_ts) as dt, count(*) as row_count
    from bronze.crm_orders
    where day(_ingest_ts) >= current_date - interval '8' day
    group by 1
),
avg_prior as (
    select avg(row_count) as avg_7d
    from daily_counts where dt < current_date
)
select d.row_count, a.avg_7d,
    d.row_count / a.avg_7d as ratio
from daily_counts d, avg_prior a
where d.dt = current_date and d.row_count < 0.5 * a.avg_7d;
-- Fail pipeline if ratio < 0.5.
```

### DQ metadata table

```sql
create table pipeline.dq_results (
    run_id          varchar(100)    not null,
    pipeline_name   varchar(200)    not null,
    layer           char(6)         not null,   -- BRONZE | SILVER | GOLD
    check_name      varchar(200)    not null,
    check_sql       varchar(2000),
    result_value    bigint,
    expected_value  bigint,
    status          varchar(10)     not null,   -- PASS | WARN | FAIL
    checked_at      timestamp       not null,
    primary key (run_id, check_name)
);
```

---

## Partitioning and Storage Strategy per Layer

### Bronze

- **Partition by ingestion date** (`day(_ingest_ts)`) — always, regardless of source event date
- **No `sorted_by`** needed — Bronze is write-optimised; reads are filtered by partition
- **Target file size**: 128–512 MB per file
- **Retention**: keep forever (Bronze is the audit archive) or as agreed with compliance

### Silver

- **Partition by business event date** (`day(order_ts)`, `month(created_at)`) — not by ingestion date
- **`sorted_by` on join key + event date** — enables file skipping for downstream Gold queries
- **Target file size**: 256 MB – 1 GB
- **Run OPTIMIZE on recent partitions** after every MERGE batch (MERGE creates small delete files)
- **Retention**: match source system SLA (typically 3–7 years)

### Gold

- **Partition by reporting period** (`month(dt)`, `year(dt)`)
- **`sorted_by` on the most common GROUP BY key** (e.g. `region`, `product_category`)
- **Target file size**: 128–512 MB (Gold tables are smaller)
- **Retention**: indefinite (Gold is the analytical archive)

### OPTIMIZE schedule recommendation

```sql
-- After every Silver MERGE batch: compact new files
alter table silver.orders execute optimize
where day(order_ts) >= current_date - interval '3' day;

-- Weekly: full compaction + expire snapshots
alter table silver.orders execute optimize(file_size_threshold => '100MB');
alter table silver.orders execute expire_snapshots(retention_threshold => '14d', retain_last => 10);
alter table silver.orders execute remove_orphan_files(retention_threshold => '14d');
analyze iceberg_catalog.silver.orders;
```

---

## Naming Conventions

```
Databases / schemas:
  bronze.<source_system>_<entity>     bronze.crm_orders
  silver.<entity>                     silver.orders
  gold.<domain>_<subject>             gold.finance_revenue_daily

Columns:
  Business keys:      <entity>_id           order_id, customer_id
  Timestamps:         <event>_ts / <event>_at    order_ts, created_at, updated_at
  Bronze metadata:    _ingest_ts, _source_file, _source_system, _batch_id, _row_hash
  Silver metadata:    _silver_loaded_ts, _source_system, _is_deleted
  Gold metadata:      _gold_loaded_ts
  Flags:              is_<condition>        is_deleted, is_active
  Measures:           <noun>_<unit>         revenue_usd, duration_seconds
```

---

## Medallion in dbt

Map dbt project structure directly to Medallion layers:

```yaml
# dbt_project.yml
models:
  my_project:
    bronze:
      +schema: bronze
      +materialized: incremental
      +incremental_strategy: append
      +on_schema_change: append_new_columns

    silver:
      +schema: silver
      +materialized: incremental
      +incremental_strategy: merge
      +on_schema_change: sync_all_columns
      +properties:
        format: "'PARQUET'"
        format_version: "'2'"

    gold:
      +schema: gold
      +materialized: table
      +on_table_exists: drop
      +properties:
        format: "'PARQUET'"
```

**Bronze dbt model** (`models/bronze/crm_orders.sql`):
```sql
{{ config(
    materialized          = 'incremental',
    incremental_strategy  = 'append',
    unique_key            = '_row_hash',
) }}

select
    order_id, customer_id, product_id,
    cast(quantity   as varchar) as quantity,
    cast(unit_price as varchar) as unit_price,
    order_status, order_timestamp,
    current_timestamp           as _ingest_ts,
    '{{ invocation_id }}'       as _batch_id,
    'CRM_SYSTEM'                as _source_system,
    md5(concat_ws('||', coalesce(order_id,''), coalesce(order_timestamp,''))) as _row_hash
from {{ source('crm', 'orders_landed') }}
{% if is_incremental() %}
  where loaded_at > (select max(_ingest_ts) from {{ this }})
{% endif %}
```

**Silver dbt model** (`models/silver/orders.sql`):
```sql
{{ config(
    materialized          = 'incremental',
    incremental_strategy  = 'merge',
    unique_key            = 'order_id',
    on_schema_change      = 'sync_all_columns',
    properties            = {
        "format":       "'PARQUET'",
        "format_version": "'2'",
        "partitioning": "ARRAY['day(order_ts)']",
        "sorted_by":    "ARRAY['customer_id', 'order_ts']",
    }
) }}

with bronze as (
    select *,
        row_number() over (partition by order_id order by _ingest_ts desc) as rn
    from {{ ref('crm_orders') }}
    {% if is_incremental() %}
      where _ingest_ts > (select max(_silver_loaded_ts) from {{ this }})
    {% endif %}
),
cleaned as (
    select
        order_id,
        customer_id,
        try_cast(quantity   as int)            as quantity,
        try_cast(unit_price as decimal(18, 2)) as unit_price,
        coalesce(order_status, 'UNKNOWN')      as order_status,
        try_cast(order_timestamp as timestamp) as order_ts,
        _ingest_ts                             as created_at,
        _ingest_ts                             as updated_at,
        _source_system,
        false                                  as _is_deleted,
        current_timestamp                      as _silver_loaded_ts
    from bronze where rn = 1 and order_id is not null
)
select * from cleaned
```

---

## Orchestration with Airflow

```python
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime, timedelta

with DAG(
    'medallion_orders_pipeline',
    schedule_interval='0 4 * * *',
    start_date=datetime(2024, 1, 1),
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
) as dag:

    # Group 0: Bronze ingestion (parallel per source)
    bronze_orders    = TrinoOperator(task_id='bronze_orders',
        sql='sql/bronze/load_crm_orders.sql', trino_conn_id='trino_dwh')
    bronze_customers = TrinoOperator(task_id='bronze_customers',
        sql='sql/bronze/load_crm_customers.sql', trino_conn_id='trino_dwh')

    # Group 0.5: DQ checks on Bronze (fail fast)
    dq_bronze_orders = TrinoOperator(task_id='dq_bronze_orders',
        sql='sql/dq/check_bronze_orders.sql', trino_conn_id='trino_dwh')

    # Group 1: Silver MERGE (parallel per entity)
    silver_orders    = TrinoOperator(task_id='silver_orders',
        sql='sql/silver/merge_orders.sql',    trino_conn_id='trino_dwh')
    silver_customers = TrinoOperator(task_id='silver_customers',
        sql='sql/silver/merge_customers.sql', trino_conn_id='trino_dwh')

    # Group 1.5: Silver OPTIMIZE (compaction)
    optimize_silver_orders = TrinoOperator(task_id='optimize_silver_orders',
        sql="alter table silver.orders execute optimize where day(order_ts) >= current_date - interval '3' day",
        trino_conn_id='trino_dwh')

    # Group 2: Gold rebuild (after all Silver ready)
    gold_revenue = TrinoOperator(task_id='gold_revenue',
        sql='sql/gold/rebuild_revenue_daily.sql', trino_conn_id='trino_dwh')
    gold_orders_wide = TrinoOperator(task_id='gold_orders_wide',
        sql='sql/gold/rebuild_orders_wide.sql',   trino_conn_id='trino_dwh')

    # Update watermarks
    update_watermarks = TrinoOperator(task_id='update_watermarks',
        sql='sql/pipeline/update_watermarks.sql', trino_conn_id='trino_dwh')

    # DAG dependencies
    [bronze_orders, bronze_customers] >> dq_bronze_orders
    dq_bronze_orders >> [silver_orders, silver_customers]
    silver_orders >> optimize_silver_orders
    [silver_orders, silver_customers, optimize_silver_orders] >> [gold_revenue, gold_orders_wide]
    [gold_revenue, gold_orders_wide] >> update_watermarks
```

---

## Best Practices

### Architecture
- **Bronze is sacred** — never modify or delete Bronze rows; it is the only recovery point if Silver/Gold logic is wrong
- **One Bronze table per source table** — do not join or denormalise in Bronze; this happens in Silver
- **Silver owns deduplication** — Bronze may contain duplicates; Silver is the first layer that must not
- **Gold depends only on Silver** — never read Bronze in a Gold model
- **Keep Gold as views first** — materialise as tables only when query latency requires it

### Incremental processing
- **Use `updated_at` watermarks** where the source exposes them; avoid full table scans on large sources
- **Always include a lookback window** (3–7 days) in Silver MERGE to catch late-arriving updates missed by the watermark
- **Idempotent loads** — every Bronze append and every Silver MERGE must be safe to re-run without creating duplicates
- **Store the watermark externally** (pipeline metadata table) — never compute it from `max(updated_at)` of the target table alone, because a failed load may have loaded 0 rows

### Deduplication
- **Dedup in Silver, not Bronze** — Bronze is the raw record; dedup logic is business logic
- **Hash the dedup key + the "latest wins" order column** — document both clearly
- **Window size for dedup** — for daily watermark loads, use a 3–7 day Bronze lookback; for CDC, deduplicate within the same LSN batch
- **Audit dedup rate** — track `rows_in_bronze / rows_merged_to_silver` per batch; a sudden drop indicates data issues

### Schema
- **Add columns, never remove** from Bronze; Silver and Gold can remove columns via versioning
- **Prefix all metadata columns with `_`** — clearly separates pipeline columns from business columns
- **Use `try_cast` instead of `cast`** in Bronze → Silver — casting failures produce NULL instead of pipeline errors; log and alert on high null rates

### Storage
- **OPTIMIZE Silver partitions after every MERGE batch** — Iceberg MERGE produces delete files that accumulate and slow reads
- **ANALYZE after OPTIMIZE** — stale statistics break join optimisation in Trino
- **Bloom filters on high-cardinality join columns** — add `parquet_bloom_filter_columns = ARRAY['order_id', 'customer_id']` to Silver tables with frequent point lookups

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Transforming data in Bronze | Bronze cannot be trusted as an audit record; recovery is impossible | Bronze = raw copy only; all transforms in Silver |
| Loading Gold directly from source | Bypasses dedup and validation; two consumers see different data | Gold reads only from Silver |
| No `_ingest_ts` in Bronze | Cannot determine when data was loaded; incremental logic breaks | Always add `_ingest_ts = current_timestamp` at ingest time |
| Watermark from `max(target.updated_at)` without fallback | A no-op load (0 rows) advances the watermark; next run misses the gap | Store watermark in a separate metadata table; update only on success |
| No lookback window in Silver MERGE | Late-arriving updates from source are permanently missed | Always merge a 3–7 day window from Bronze, not just today |
| MERGE without OPTIMIZE | Delete files accumulate; reads slow down over weeks | Run OPTIMIZE on Silver partitions after every MERGE |
| One giant Silver table for everything | Hard to maintain, unrelated schemas change each other | One Silver table per business entity |
| Rebuilding Gold from Gold | Circular dependency; errors compound across reprocessing | Always rebuild Gold from Silver, never from Gold |
| Skipping DQ checks between layers | Bad data silently propagates to BI | Add assertion checks after Bronze ingest; fail pipeline early |
| `SELECT *` in Silver → Gold | Schema changes in Silver silently break Gold columns | Enumerate columns explicitly in every cross-layer SELECT |
| Dedup using only `DISTINCT` | DISTINCT collapses rows with identical values but misses rows with same key and different values | Use ROW_NUMBER with an ordering column to pick the canonical version |

---

## Output Expectations

When designing or implementing a Medallion architecture:
- Classify each table as Bronze / Silver / Gold and justify the placement.
- Show DDL with correct partitioning, `sorted_by`, `format_version`, and metadata columns per layer.
- For Silver loads, show both the dedup CTE and the MERGE statement.
- State which deduplication strategy is used and why (`ROW_NUMBER` with what ordering, or CDC-aware by LSN).
- Include the incremental window size in Bronze reads and explain why it's needed.
- Show the OPTIMIZE call that should follow every MERGE batch.
- For Gold, specify whether the table is rebuilt (truncate+reload) or incrementally appended, and why.
