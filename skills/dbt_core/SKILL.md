---
name: dbt-core
description: dbt Core (adapter-agnostic) — project structure, models (SQL/Python), sources and refs, materializations (table/view/incremental/ephemeral), incremental strategies (append/merge/delete+insert), is_incremental() macro, snapshots (SCD Type 2), seeds, tests (generic/singular/severity), Jinja macros, dbt-utils package, node selection syntax (graph operators), hooks, exposures, metrics (MetricFlow), CI/CD (slim CI with state:modified+), dbt_project.yml, profiles.yml, multi-adapter support (Spark/PostgreSQL/ClickHouse/BigQuery)
---

# dbt Core

## When to Use

Load this skill when the user needs to:
- Build or review dbt models, sources, seeds, snapshots, or tests
- Configure incremental models with the right strategy and `is_incremental()` filter
- Write Jinja macros for DRY transformations
- Set up SCD Type 2 snapshots
- Run dbt in CI/CD pipelines with slim CI
- Use dbt with adapters beyond Trino (Spark, PostgreSQL, ClickHouse, BigQuery)

---

## Project Structure

```
my_dbt_project/
├── dbt_project.yml          # project config
├── profiles.yml             # connection profiles (typically ~/.dbt/profiles.yml)
├── packages.yml             # package dependencies
├── models/
│   ├── staging/             # 1:1 source renaming & casting
│   │   └── _sources.yml     # source declarations
│   ├── intermediate/        # joins & light transformations
│   └── marts/               # business-level aggregations
│       ├── finance/
│       └── marketing/
├── macros/                  # reusable Jinja functions
├── tests/                   # singular SQL tests
├── seeds/                   # static CSV reference data
├── snapshots/               # SCD Type 2 tracking
├── analyses/                # ad-hoc SQL (not materialized)
└── target/                  # compiled SQL output (gitignored)
```

---

## dbt_project.yml

```yaml
name: my_project
version: '1.0.0'
profile: my_warehouse

model-paths: ["models"]
seed-paths:  ["seeds"]
test-paths:  ["tests"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  my_project:
    # Global defaults
    +persist_docs:
      relation: true
      columns: true

    staging:
      +materialized: view
      +schema: staging

    intermediate:
      +materialized: ephemeral

    marts:
      +materialized: table
      +schema: marts
      finance:
        +tags: ["finance", "daily"]
      marketing:
        +tags: ["marketing"]

snapshots:
  my_project:
    +target_schema: snapshots
    +strategy: timestamp
    +unique_key: id
```

---

## profiles.yml

```yaml
# ~/.dbt/profiles.yml

# PostgreSQL
my_project_pg:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres.internal
      port: 5432
      user: "{{ env_var('DBT_PG_USER') }}"
      password: "{{ env_var('DBT_PG_PASSWORD') }}"
      dbname: analytics
      schema: dbt_dev
      threads: 4

    prod:
      type: postgres
      host: postgres.internal
      port: 5432
      user: "{{ env_var('DBT_PG_USER_PROD') }}"
      password: "{{ env_var('DBT_PG_PASSWORD_PROD') }}"
      dbname: analytics
      schema: marts
      threads: 8

# Apache Spark / Databricks
my_project_spark:
  target: dev
  outputs:
    dev:
      type: spark
      method: thrift
      host: spark-thrift.internal
      port: 10001
      schema: dbt_dev
      threads: 4

# ClickHouse
my_project_ch:
  target: dev
  outputs:
    dev:
      type: clickhouse
      host: clickhouse.internal
      port: 8123
      user: default
      password: "{{ env_var('CH_PASSWORD') }}"
      database: default
      schema: dbt_dev
      threads: 4
      secure: false
```

---

## Sources & Refs

### sources.yml

```yaml
# models/staging/_sources.yml
version: 2

sources:
  - name: raw_orders
    database: raw_db
    schema: public
    description: "Raw operational data from orders service"
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: _loaded_at

    tables:
      - name: orders
        description: "Raw orders table"
        columns:
          - name: order_id
            description: "Primary key"
            tests: [unique, not_null]
          - name: status
            tests:
              - accepted_values:
                  values: [placed, shipped, delivered, cancelled]

      - name: customers
        identifier: tbl_customers   # actual table name if different from source name
```

### Using ref() and source()

```sql
-- models/staging/stg_orders.sql
SELECT
    order_id,
    user_id,
    CAST(amount AS DECIMAL(18,2))        AS amount,
    LOWER(TRIM(status))                  AS status,
    CAST(created_at AS TIMESTAMP)        AS created_at,
    CURRENT_TIMESTAMP                    AS _loaded_at
FROM {{ source('raw_orders', 'orders') }}

-- models/marts/fct_daily_revenue.sql
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE status = 'delivered'
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)
SELECT
    DATE_TRUNC('day', o.created_at)     AS dt,
    c.region,
    COUNT(*)                             AS order_cnt,
    SUM(o.amount)                        AS revenue
FROM orders o
JOIN customers c USING (user_id)
GROUP BY 1, 2
```

---

## Materializations

| Materialization | Behavior | Use Case |
|---|---|---|
| `view` | DDL view, no data storage | Lightweight staging models |
| `table` | Full rebuild every run | Small/medium marts, dim tables |
| `incremental` | Append/merge new rows only | Large fact tables, event logs |
| `ephemeral` | CTE inlined into downstream | Intermediate transforms with no direct consumers |
| `materialized_view` | DB-native materialized view | Where supported (Postgres, Redshift, BigQuery) |

```sql
-- Per-model config override
{{ config(
    materialized='table',
    schema='finance',
    alias='fact_revenue',          -- overrides model file name
    tags=['daily', 'finance'],
    persist_docs={"relation": true, "columns": true},
    meta={"owner": "finance-team", "sla": "6am UTC"},
) }}
```

---

## Incremental Models

### Core Pattern

```sql
-- models/marts/fct_events.sql
{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',    -- see strategy table below
    on_schema_change='append_new_columns',
) }}

SELECT
    event_id,
    user_id,
    event_type,
    event_time,
    properties,
    CURRENT_TIMESTAMP AS _dbt_loaded_at
FROM {{ ref('stg_events') }}

{% if is_incremental() %}
  -- Only new records since last run
  WHERE event_time > (
      SELECT COALESCE(MAX(event_time), '1900-01-01'::TIMESTAMP)
      FROM {{ this }}
  )
{% endif %}
```

### Incremental Strategies by Adapter

| Strategy | Adapters | Behavior |
|---|---|---|
| `append` | All | INSERT new rows; no deduplication |
| `merge` | Snowflake, BigQuery, Spark, Databricks, Trino, PG | MERGE (upsert) on `unique_key` |
| `delete+insert` | Snowflake, Spark, BigQuery | DELETE matching, then INSERT |
| `insert_overwrite` | Spark, BigQuery | Overwrite entire partitions |
| `microbatch` | dbt 1.9+, most adapters | Process one batch period at a time |

```sql
-- Spark / Iceberg: partition-level overwrite
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['dt'],
    file_format='iceberg',
) }}

SELECT *, CAST(event_time AS DATE) AS dt
FROM {{ ref('stg_events') }}
{% if is_incremental() %}
WHERE dt >= CAST(DATEADD(day, -3, CURRENT_DATE) AS DATE)
{% endif %}
```

### Incremental Predicates (Limit Target Table Scan)

```yaml
# models/marts/properties.yml
models:
  - name: fct_events
    config:
      materialized: incremental
      unique_key: event_id
      incremental_strategy: merge
      incremental_predicates:
        - "DBT_INTERNAL_DEST.event_time > DATEADD(day, -7, CURRENT_DATE)"
```

`DBT_INTERNAL_DEST` = target table alias, `DBT_INTERNAL_SOURCE` = staging table alias.

### on_schema_change Options

| Value | Behavior |
|---|---|
| `ignore` (default) | Fail on column removal; ignore new columns |
| `fail` | Error on any schema divergence |
| `append_new_columns` | Add new columns; keep old columns |
| `sync_all_columns` | Add new, remove deleted columns |

---

## Snapshots (SCD Type 2)

```sql
-- snapshots/customers_snapshot.sql
{% snapshot customers_snapshot %}

    {{ config(
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        schema='snapshots',
        dbt_valid_to_current="'9999-12-31'::DATE",
        snapshot_meta_column_names={
            'dbt_valid_from': 'valid_from',
            'dbt_valid_to':   'valid_to',
        },
        hard_deletes='new_record',   -- track deletes with dbt_is_deleted flag
    ) }}

    SELECT
        customer_id,
        name,
        email,
        city,
        tier,
        updated_at
    FROM {{ source('raw', 'customers') }}

{% endsnapshot %}

-- Query: current records
SELECT * FROM {{ ref('customers_snapshot') }} WHERE valid_to = '9999-12-31'

-- Query: history at a point in time
SELECT * FROM {{ ref('customers_snapshot') }}
WHERE valid_from <= '2024-01-15' AND valid_to > '2024-01-15'
```

```bash
dbt snapshot                          # run all snapshots
dbt snapshot --select customers_snapshot
```

---

## Seeds

```bash
# seeds/country_codes.csv
country_code,country_name,region
RU,Russia,EMEA
BY,Belarus,EMEA
KZ,Kazakhstan,APAC
```

```yaml
# dbt_project.yml
seeds:
  my_project:
    +schema: reference
    +column_types:
      country_code: varchar(2)
```

```sql
SELECT o.order_id, c.country_name, c.region
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('country_codes') }} c USING (country_code)
```

```bash
dbt seed                # load/update seeds
dbt seed --full-refresh # truncate and reload
```

---

## Tests

### Generic Tests (YAML)

```yaml
# models/staging/_models.yml
version: 2

models:
  - name: stg_orders
    description: "Staged orders"
    columns:
      - name: order_id
        tests: [unique, not_null]

      - name: status
        tests:
          - accepted_values:
              values: [placed, shipped, delivered, cancelled, returned]
              severity: warn

      - name: user_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: user_id

      - name: amount
        tests:
          - dbt_utils.not_null_proportion:   # from dbt-utils package
              at_least: 0.95
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000
```

### Singular Tests (SQL files)

```sql
-- tests/assert_positive_revenue.sql
-- Returns failing rows (non-empty result = test failure)
SELECT
    dt,
    SUM(revenue) AS total_revenue
FROM {{ ref('fct_daily_revenue') }}
GROUP BY 1
HAVING SUM(revenue) < 0
```

### Test Configuration

```yaml
# Global test severity
tests:
  +severity: warn            # warn instead of fail
  +store_failures: true      # persist failing rows to DB
  +store_failures_as: table  # table | view | ephemeral

# Per-test severity with threshold
- name: order_id
  tests:
    - unique:
        severity: warn
        warn_if: ">0"
        error_if: ">100"
```

```bash
dbt test                                # all tests
dbt test --select stg_orders            # tests on one model
dbt test --select "source:raw_orders"   # source tests
dbt test --select "test_type:generic"   # generic only
dbt test --store-failures               # persist failures
```

---

## Jinja Macros

```sql
-- macros/safe_divide.sql
{% macro safe_divide(numerator, denominator, default=0) %}
    CASE
        WHEN COALESCE({{ denominator }}, 0) = 0 THEN {{ default }}
        ELSE {{ numerator }} / NULLIF({{ denominator }}, 0)
    END
{% endmacro %}

-- Usage
SELECT
    order_id,
    {{ safe_divide('profit', 'revenue') }} AS margin,
    {{ safe_divide('clicks', 'impressions', default=0) }} AS ctr
FROM {{ ref('fct_campaigns') }}
```

```sql
-- macros/generate_surrogate_key.sql  (pattern from dbt-utils)
{% macro surrogate_key(field_list) %}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}

-- macros/get_column_values.sql — dynamic value list
{% macro get_payment_methods() %}
    {% set query %}
        SELECT DISTINCT payment_method
        FROM {{ ref('stg_payments') }}
        ORDER BY 1
    {% endset %}
    {% if execute %}
        {% set results = run_query(query) %}
        {% set methods = results.columns[0].values() %}
        {{ return(methods) }}
    {% else %}
        {{ return([]) }}
    {% endif %}
{% endmacro %}

-- Usage in model
{% set payment_methods = get_payment_methods() %}
SELECT
    order_id,
    {% for method in payment_methods %}
    SUM(CASE WHEN payment_method = '{{ method }}' THEN amount ELSE 0 END)
        AS {{ method | replace(' ', '_') }}_amount
    {%- if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ ref('stg_payments') }}
GROUP BY 1
```

### Hooks

```yaml
# dbt_project.yml
models:
  my_project:
    +pre-hook:
      - "{{ logging.log_model_start(this) }}"
    +post-hook:
      - "ANALYZE {{ this }}"
      - "GRANT SELECT ON {{ this }} TO ROLE analyst"
      - "{{ logging.log_model_end(this) }}"

# Or in model config:
{{ config(
    post_hook=[
        "GRANT SELECT ON {{ this }} TO ROLE reporter",
        "ANALYZE {{ this }}",
    ]
) }}
```

---

## Packages (packages.yml)

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
  - package: calogica/dbt_expectations
    version: 0.10.4
  - package: dbt-labs/codegen
    version: 0.12.1
```

```bash
dbt deps   # install packages
```

**Commonly used macros:**
- `dbt_utils.generate_surrogate_key(['id', 'date'])` — MD5 surrogate key
- `dbt_utils.pivot(column, values)` — dynamic pivot
- `dbt_utils.date_spine(datepart, start, end)` — date series generation
- `dbt_utils.not_null_proportion(at_least=0.95)` — partial null test
- `dbt_expectations.expect_column_values_to_be_between(min, max)` — range test

---

## Node Selection Syntax

```bash
# By name
dbt run --select stg_orders
dbt run --select marts.finance.*          # all models in marts/finance/

# Graph operators
dbt run --select +stg_orders             # model + all upstream parents
dbt run --select stg_orders+             # model + all downstream children
dbt run --select 2+stg_orders+2          # 2-degree neighborhood

# By tag
dbt run --select "tag:daily"
dbt run --select "tag:finance,tag:daily" # intersection (both tags)

# By config
dbt run --select "config.materialized:incremental"

# By source
dbt test --select "source:raw_orders"
dbt test --select "source:raw_orders.orders"

# Exclude
dbt run --select "marts.*" --exclude "marts.deprecated.*"

# State-based (slim CI)
dbt run --select "state:modified+"  --state ./prod-artifacts/
dbt test --select "state:modified+" --state ./prod-artifacts/

# All together
dbt run --select "+tag:finance,config.materialized:incremental" \
        --exclude "tag:deprecated"
```

---

## Slim CI (GitHub Actions Example)

```yaml
# .github/workflows/dbt-ci.yml
name: dbt CI

on:
  pull_request:
    branches: [main]

jobs:
  dbt-ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install dbt
        run: pip install dbt-postgres==1.8.0

      - name: Download production artifacts
        run: |
          # Download manifest.json from production run (S3, GCS, etc.)
          aws s3 cp s3://dbt-artifacts/prod/manifest.json ./prod-artifacts/manifest.json

      - name: dbt build (only modified + downstream)
        env:
          DBT_PG_USER: ${{ secrets.DBT_PG_USER }}
          DBT_PG_PASSWORD: ${{ secrets.DBT_PG_PASSWORD }}
        run: |
          dbt deps
          dbt build \
            --select "state:modified+" \
            --state ./prod-artifacts/ \
            --target ci \
            --defer
            # --defer: use prod results for unmodified upstream models
```

---

## CLI Reference

```bash
dbt init my_project              # scaffold new project
dbt debug                        # test connection
dbt deps                         # install packages from packages.yml
dbt compile                      # compile Jinja → SQL (no execution)
dbt run                          # run all models
dbt run --select stg_orders+     # run model + downstream
dbt run --full-refresh           # rebuild all incrementals from scratch
dbt test                         # run all tests
dbt build                        # run + test + seed + snapshot in DAG order
dbt snapshot                     # run all snapshots
dbt seed                         # load seed CSV files
dbt source freshness             # check source freshness
dbt docs generate                # generate documentation site
dbt docs serve                   # serve docs on localhost:8080
dbt clean                        # remove target/ and dbt_packages/
dbt ls --select "tag:daily"      # list matching models without running
```

---

## Best Practices

1. **Layer your project**: staging (raw → typed+renamed) → intermediate (joins) → marts (business metrics). Never reference raw sources from marts.
2. **Every model needs a primary key test** (`unique` + `not_null`) — this is the minimum DQ floor.
3. **Use incremental models for tables > 1M rows** — rebuilding full table on every run is expensive.
4. **Always add `is_incremental()` filter** — without it, an incremental model rebuilds fully every time.
5. **Use `on_schema_change='append_new_columns'`** by default; `sync_all_columns` for full schema sync.
6. **Seeds for static lookups only** (country codes, enums) — not for large datasets.
7. **Snapshots for source tables that get overwritten** — not for tables that already have history.
8. **Use `--defer` in CI** — avoids rebuilding unchanged upstream models in the CI environment.
9. **Persist docs** (`persist_docs: {relation: true, columns: true}`) — column descriptions appear in warehouse schema browsers.
10. **Tag models by schedule and domain** — `+tags: ["daily", "finance"]` enables targeted CI and monitoring.

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| `ref()` in staging → raw source | Staging should be 1:1 with source tables | Use `source()` in staging, `ref()` everywhere else |
| No `is_incremental()` filter | Model rebuilds fully on every run despite `materialized='incremental'` | Add `{% if is_incremental() %} WHERE ... {% endif %}` |
| `unique_key` with NULLs | MERGE ON NULL = NULL fails silently | Ensure PK column is NOT NULL; use surrogate key |
| `on_schema_change='sync_all_columns'` without testing | Silently drops columns that consumers depend on | Use `append_new_columns`; test schema changes in CI |
| Heavy computation in macros at parse time | `run_query()` inside macro runs during `dbt compile`, not just `dbt run` | Wrap in `{% if execute %}` guard |
| No source freshness tests | Stale source data goes undetected | Add `freshness` block to all sources with SLA |
| Giant monolithic models | Hard to test, debug, and cache incrementally | Split by logical function; use ephemeral for intermediates |
| Hardcoded environment paths in models | CI/dev/prod share same model but different schemas | Use `target.schema`, `env_var()`, or `var()` |

---

## References to Consult When Needed

- [dbt Core Documentation](https://docs.getdbt.com/)
- [dbt Incremental Models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt Snapshots](https://docs.getdbt.com/docs/build/snapshots)
- [dbt Tests](https://docs.getdbt.com/docs/build/data-tests)
- [dbt Node Selection](https://docs.getdbt.com/reference/node-selection/syntax)
- [dbt-utils package](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/)
- [dbt-expectations package](https://hub.getdbt.com/calogica/dbt_expectations/latest/)
