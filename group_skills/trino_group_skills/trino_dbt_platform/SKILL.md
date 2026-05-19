---
name: trino-dbt-platform
description: Production dbt + Trino platform — dbt-trino adapter profiles.yml (ldap/kerberos/oauth/jwt/certificate auth), all materializations (table/view/incremental/materialized_view/ephemeral), incremental strategies (append/merge/delete+insert), Iceberg table_properties in config blocks (format/partitioning/sorted_by/location), on_schema_change, snapshots with TIMESTAMP(6), slim CI with state:modified+ and --defer, dbt project structure (staging/intermediate/mart), ANALYZE post-hook, session_properties in profiles, multi-thread parallelism
---

# dbt + Trino Production Platform

## When to Use

- Building a dbt project targeting Trino as the query engine
- Designing incremental models on Iceberg tables
- Setting up dbt CI/CD with slim builds using state:modified+
- Configuring dbt materializations with Iceberg-specific properties
- Implementing SCD2 snapshots or MERGE-based upserts on Trino/Iceberg

---

## Installation

```bash
pip install dbt-trino
# For dbt 1.8+
pip install "dbt-trino>=1.8.0"
```

---

## profiles.yml

### LDAP / Password (most common)

```yaml
# ~/.dbt/profiles.yml
data_platform:
  target: dev
  outputs:
    dev:
      type: trino
      method: ldap
      host: trino-coordinator.internal
      port: 8080
      database: iceberg          # Trino catalog name
      schema: "{{ env_var('DBT_SCHEMA', 'dev_' ~ env_var('USER', 'local')) }}"
      user: "{{ env_var('TRINO_USER') }}"
      password: "{{ env_var('TRINO_PASSWORD') }}"
      threads: 8
      retries: 3
      session_properties:
        query_max_run_time: 4h
        exchange_compression_codec: LZ4
        join_reordering_strategy: AUTOMATIC
      http_scheme: http

    prod:
      type: trino
      method: ldap
      host: trino-coordinator.internal
      port: 443
      database: iceberg
      schema: gold
      user: "{{ env_var('TRINO_PROD_USER') }}"
      password: "{{ env_var('TRINO_PROD_PASSWORD') }}"
      threads: 16
      retries: 5
      http_scheme: https
      session_properties:
        query_max_run_time: 8h
        spill_enabled: "true"
```

### JWT (service accounts, CI/CD)

```yaml
    ci:
      type: trino
      method: jwt
      host: trino.internal
      port: 443
      database: iceberg
      schema: ci_dbt
      jwt_token: "{{ env_var('TRINO_JWT_TOKEN') }}"
      threads: 4
      http_scheme: https
```

### OAuth 2.0 (interactive user sessions)

```yaml
    interactive:
      type: trino
      method: oauth
      host: trino.internal
      port: 443
      database: iceberg
      schema: adhoc
      threads: 4
```

---

## dbt Project Structure

```
dbt_project/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/             # raw → cleaned 1:1 to sources
│   │   ├── _sources.yml
│   │   ├── stg_orders.sql
│   │   └── stg_customers.sql
│   ├── intermediate/        # business logic joins
│   │   ├── int_order_items.sql
│   │   └── int_customer_orders.sql
│   └── mart/                # final analytical models
│       ├── fact_orders.sql
│       ├── dim_customer.sql
│       └── agg_daily_revenue.sql
├── macros/
│   ├── trino_utils.sql
│   └── generate_schema_name.sql
├── snapshots/
│   └── scd2_customers.sql
└── tests/
```

```yaml
# dbt_project.yml
name: data_platform
version: 1.0.0
profile: data_platform

models:
  data_platform:
    staging:
      +materialized: view
      +schema: silver
    intermediate:
      +materialized: ephemeral
    mart:
      +materialized: table
      +on_table_exists: drop      # required for AWS Glue catalog
      +post-hook: "ANALYZE {{ this }}"   # update CBO statistics after every run

vars:
  start_date: '2020-01-01'
```

---

## Materializations with Iceberg Properties

### Table with Iceberg Properties

```sql
-- models/mart/fact_orders.sql
{{
  config(
    materialized      = 'table',
    on_table_exists   = 'drop',
    properties = {
      "format":            "'PARQUET'",
      "compression_codec": "'ZSTD'",
      "format_version":    "2",
      "partitioning":      "ARRAY['day(order_date)', 'region']",
      "sorted_by":         "ARRAY['customer_id']",
      "location":          "'s3://data-lake/gold/fact_orders/'"
    },
    post_hook = "ANALYZE {{ this }}"
  )
}}

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    o.amount,
    c.region,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
```

### Incremental: append (streaming/event tables)

```sql
-- models/staging/stg_events.sql
{{
  config(
    materialized = 'incremental',
    properties = {
      "format":        "'PARQUET'",
      "partitioning":  "ARRAY['day(event_time)']"
    }
  )
}}

SELECT
    event_id,
    user_id,
    event_type,
    event_time,
    properties
FROM {{ source('kafka_landing', 'events') }}

{% if is_incremental() %}
WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
```

### Incremental: merge (upsert/SCD1)

```sql
-- models/mart/dim_customer.sql
{{
  config(
    materialized        = 'incremental',
    incremental_strategy = 'merge',
    unique_key          = 'customer_id',
    properties = {
      "format":           "'PARQUET'",
      "format_version":   "2",
      "sorted_by":        "ARRAY['customer_id']"
    }
  )
}}

SELECT
    customer_id,
    name,
    email,
    region,
    tier,
    updated_at
FROM {{ ref('stg_customers') }}

{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

### Incremental: delete+insert (partition overwrite)

```sql
-- models/mart/fact_daily_revenue.sql
{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'order_date',
    properties = {
      "format":        "'PARQUET'",
      "partitioning":  "ARRAY['month(order_date)']"
    }
  )
}}

SELECT
    order_date,
    COUNT(*)               AS order_count,
    SUM(amount)            AS gross_revenue
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
-- Reprocess last 3 days to handle late arrivals
WHERE order_date >= DATE_ADD('day', -3, CURRENT_DATE)
{% endif %}

GROUP BY order_date
```

---

## Snapshots (SCD2 on Iceberg)

```sql
-- snapshots/scd2_customers.sql
{% snapshot scd2_customers %}
{{
  config(
    target_schema = 'snapshots',
    strategy      = 'timestamp',
    unique_key    = 'customer_id',
    updated_at    = 'updated_at',
    properties = {
      "format":         "'PARQUET'",
      "format_version": "2"
    }
  )
}}

SELECT customer_id, name, email, region, tier, updated_at
FROM {{ source('bronze', 'customers') }}

{% endsnapshot %}
```

Fix Iceberg TIMESTAMP precision (Iceberg requires TIMESTAMP(6) WITH TIME ZONE):

```sql
-- macros/trino_utils.sql
{% macro trino__current_timestamp() %}
  current_timestamp(6)
{% endmacro %}
```

---

## Sources Configuration

```yaml
# models/staging/_sources.yml
version: 2
sources:
  - name: bronze
    database: iceberg
    schema: bronze
    tables:
      - name: orders
        loaded_at_field: ingested_at
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 24, period: hour}
      - name: customers
      - name: events
```

---

## Slim CI: state:modified+ with --defer

```bash
# Generate state artifact in production (save to S3)
dbt run --target prod
aws s3 cp target/manifest.json s3://dbt-artifacts/production/manifest.json

# CI pipeline: only run models changed since last production run
aws s3 cp s3://dbt-artifacts/production/manifest.json prod-manifest/

dbt run \
  --target ci \
  --select state:modified+ \
  --defer \
  --state prod-manifest/ \
  --threads 4

dbt test \
  --target ci \
  --select state:modified+ \
  --defer \
  --state prod-manifest/
```

---

## ANALYZE Post-Hook for CBO

```yaml
# dbt_project.yml — apply to all mart models
models:
  data_platform:
    mart:
      +post-hook: "ANALYZE {{ this }}"

# For specific high-cardinality columns only
models:
  data_platform:
    mart:
      +post-hook: >
        ANALYZE {{ this }}
        WITH (columns = ARRAY['customer_id', 'order_date', 'region'])
```

---

## Anti-Patterns

1. **`on_table_exists = 'rename'` with AWS Glue** — Glue does not support table rename; use `drop` mode instead.
2. **`merge` strategy without `format_version = 2`** — Iceberg v1 tables don't support equality delete files needed for MERGE; always set `"format_version": "2"` in properties.
3. **Not setting `sorted_by` on large incremental models** — unsorted Parquet files disable row-group skipping; add `sorted_by` on the primary filter column.
4. **Missing `ANALYZE` post-hook on mart models** — fresh tables have no statistics; the optimizer can't choose optimal join strategies without them.
5. **Very wide incremental windows (is_incremental() filter selects all rows)** — incremental model that reads the whole table on every run is slower than a full refresh; always bound the watermark filter to a realistic window (e.g., 3 days).
6. **Single thread (`threads: 1`) in profiles.yml** — default is 1; set to 8–16 for parallel model execution to significantly reduce run time.

---

## References

- dbt-trino adapter: `docs.getdbt.com/docs/core/connect-data-platform/trino-setup`
- Trino-specific configs: `docs.getdbt.com/reference/resource-configs/trino-configs`
- Related skills: `[[trino-iceberg-best-practices]]`, `[[trino-dbt-query-performance]]`, `[[trino-query-optimization]]`
