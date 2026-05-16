---
name: dbt_trino
description: Use when writing, configuring, or optimizing dbt projects targeting Trino or Starburst — covering profiles.yml setup, all authentication methods, materializations (table/view/incremental/materialized_view/ephemeral), incremental strategies (append/merge/delete+insert), table properties (format/partitioning/sorted_by), on_schema_change, seeds, snapshots, grants, session properties, data modeling patterns (Kimball/staging/intermediate/mart), dbt project structure, tests, and CI/CD.
---

# dbt + Trino / Starburst Engineer

## When to Use

Use this skill when:
- Setting up a new dbt project targeting Trino or Starburst
- Writing or debugging `profiles.yml` connection and authentication config
- Choosing and configuring materializations for Trino models
- Implementing incremental models with append, merge, or delete+insert strategies
- Setting Iceberg/Hive/Delta table properties (format, partitioning, sorted_by) via dbt
- Designing a dbt project structure: staging → intermediate → mart layers
- Writing dbt tests, seeds, snapshots against Trino
- Configuring grants, session properties, and model contracts
- Optimising dbt build performance for large Trino tables

---

## Installation

```bash
pip install dbt-trino
# or with extras for oauth token caching
pip install dbt-trino "trino[external-authentication-token-cache]"
```

Supported: Trino 478+, Starburst Enterprise 477-e.1+, Starburst Galaxy.

---

## profiles.yml

Located at `~/.dbt/profiles.yml` (global) or `<project_root>/profiles.yml` (project-scoped).

### Complete parameter reference

```yaml
my_trino_project:
  target: dev
  outputs:
    dev:
      type: trino

      # --- Connection ---
      host: trino.mycompany.com        # no http:// prefix
      port: 8080                       # 443 for TLS
      http_scheme: http                # http | https (auto-https for ldap/kerberos/jwt)
      database: iceberg_catalog        # Trino catalog name
      schema: analytics                # default schema (avoid mixed case)

      # --- Auth (see Authentication section) ---
      method: ldap                     # none | ldap | kerberos | jwt | certificate | oauth | oauth_console
      user: john.doe
      password: "{{ env_var('DBT_TRINO_PASSWORD') }}"

      # --- Performance ---
      threads: 8                       # parallel model builds (default: 1)
      retries: 5                       # retry on connection error (default: 3)

      # --- Session ---
      timezone: Europe/Moscow          # session timezone
      session_properties:
        query_max_run_time: 4h
        join_reordering_strategy: AUTOMATIC
        join_distribution_type: AUTOMATIC
        task_writer_count: "4"

      # --- Roles ---
      roles:
        iceberg_catalog: analyst       # catalog: role

      # --- Misc ---
      http_headers:
        X-Trino-Client-Info: dbt-trino
      prepared_statements_enabled: true   # for dbt seed (default: true)
```

### Authentication methods

#### None (dev / local Trino without auth)
```yaml
method: none
user: admin
```

#### LDAP (most common enterprise setup)
```yaml
method: ldap
user: john.doe
password: "{{ env_var('DBT_TRINO_PASSWORD') }}"
# Optional: run queries as another user
impersonation_user: service_account
```

#### JWT
```yaml
method: jwt
jwt_token: "{{ env_var('DBT_TRINO_JWT_TOKEN') }}"
```

#### Kerberos
```yaml
method: kerberos
user: trino-service
keytab: /etc/security/trino.keytab
krb5_config: /etc/krb5.conf
principal: trino@REALM.EXAMPLE.COM
service_name: trino              # default
hostname_override: REALM.EXAMPLE.COM  # optional
mutual_authentication: false
```

#### Certificate (mTLS)
```yaml
method: certificate
client_certificate: /path/to/client.crt
client_private_key: /path/to/client.key
cert: /path/to/ca.crt            # optional CA bundle
```

#### OAuth (browser-based, Starburst Galaxy)
```yaml
method: oauth
host: myaccount-mycluster.trino.galaxy.starburst.io
port: 443
database: dbt_target
schema: analytics
```

#### Starburst Galaxy user format
```yaml
user: john.doe@mycompany.com/analyst_role   # email/role
```

---

## dbt Project Structure

Recommended layout for a Trino-backed dbt project:

```
my_project/
├── dbt_project.yml
├── profiles.yml            # or ~/.dbt/profiles.yml
├── packages.yml
├── models/
│   ├── staging/            # Bronze → raw source cleaning, 1:1 with source tables
│   │   ├── _sources.yml    # source() definitions
│   │   ├── _staging.yml    # model docs + tests
│   │   └── stg_orders.sql
│   ├── intermediate/       # Silver → joins, enrichments, not exposed to BI
│   │   └── int_orders_enriched.sql
│   └── marts/              # Gold → business-facing, exposed to BI
│       ├── core/
│       │   └── fct_orders.sql
│       └── finance/
│           └── agg_revenue_daily.sql
├── seeds/                  # small reference data (CSV)
├── snapshots/              # SCD Type 2 tables
├── tests/                  # custom singular tests
├── macros/                 # custom Jinja macros
└── analyses/               # ad-hoc SQL, not materialised
```

### dbt_project.yml skeleton

```yaml
name: my_project
version: 1.0.0
config-version: 2

profile: my_trino_project

model-paths: ["models"]
seed-paths: ["seeds"]
snapshot-paths: ["snapshots"]
macro-paths: ["macros"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  my_project:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: view
      +schema: intermediate
    marts:
      +materialized: table
      +on_table_exists: drop      # safe for Iceberg + Glue
      +properties:
        format: "'PARQUET'"
```

---

## Materializations

### View

Default. Rebuilt on every `dbt run` via `CREATE OR REPLACE VIEW`.

```sql
-- models/staging/stg_events.sql
{{ config(materialized='view') }}

select
    event_id,
    cast(event_ts as timestamp(6) with time zone) as event_ts,
    lower(event_type) as event_type,
    user_id
from {{ source('raw', 'events') }}
where event_id is not null
```

**view_security** — controls whether the view runs with the definer's or invoker's permissions:
```sql
{{ config(materialized='view', view_security='invoker') }}
```

### Table

Rebuilt as a full table on every run. Use for mart-layer models queried by BI.

```sql
{{ config(
    materialized    = 'table',
    on_table_exists = 'drop',      -- drop | rename | replace | skip
    properties      = {
        "format":       "'PARQUET'",
        "partitioning": "ARRAY['day(event_ts)']",
        "sorted_by":    "ARRAY['user_id']",
    }
) }}

select ...
```

**on_table_exists** options:

| Value | Behaviour | When to use |
|---|---|---|
| `rename` | Create temp → rename old to backup → rename temp to target (default) | Standard Hive/Iceberg |
| `drop` | Drop then recreate | AWS Glue, Delta, Iceberg when rename fails |
| `replace` | `CREATE OR REPLACE TABLE` | Connectors that support it |
| `skip` | `CREATE TABLE IF NOT EXISTS` | Idempotent create, never overwrites |

### Incremental

On first run: full `CREATE TABLE AS SELECT`. On subsequent runs: only new/changed rows are processed.

```sql
-- models/marts/fct_events.sql
{{ config(
    materialized        = 'incremental',
    unique_key          = 'event_id',
    incremental_strategy= 'merge',
    on_schema_change    = 'sync_all_columns',
    properties          = {
        "format":       "'PARQUET'",
        "partitioning": "ARRAY['day(event_ts)']",
    }
) }}

select
    event_id,
    user_id,
    event_type,
    event_ts
from {{ ref('stg_events') }}

{% if is_incremental() %}
  where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
```

#### Incremental strategies

| Strategy | unique_key | Trino connector support | Description |
|---|---|---|---|
| `append` | not required | All | INSERT new rows only. No dedup. |
| `merge` | required | Iceberg, Delta (v2) | MERGE INTO: UPDATE matched rows, INSERT new ones |
| `delete+insert` | required | Iceberg, Hive | DELETE matching rows, then INSERT all — safe for partitioned tables |

**append** — simplest, no dedup:
```sql
{{ config(materialized='incremental') }}
select * from {{ ref('stg_events') }}
{% if is_incremental() %}
  where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
```

**merge** — upsert (requires Iceberg format_version=2):
```sql
{{ config(
    materialized='incremental',
    unique_key=['user_id', 'date_day'],
    incremental_strategy='merge',
) }}
select
    user_id,
    date_trunc('day', event_ts) as date_day,
    count(*) as event_count,
    max(event_ts) as last_seen_ts
from {{ ref('stg_events') }}
{% if is_incremental() %}
  where event_ts >= (select max(last_seen_ts) from {{ this }}) - interval '1' day
{% endif %}
group by 1, 2
```

**delete+insert** — deletes rows matching `unique_key` then inserts; works reliably on partitioned Iceberg tables:
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    properties={"partitioning": "ARRAY['day(created_at)']"},
) }}
select * from {{ ref('stg_orders') }}
{% if is_incremental() %}
  where created_at >= (select max(created_at) from {{ this }}) - interval '1' day
{% endif %}
```

#### Incremental on Hive with partition overwrite

Enable in Trino catalog config:
```
<catalog>.insert-existing-partitions-behavior=OVERWRITE
```

Or via session property in profile:
```yaml
session_properties:
  hive.insert_existing_partitions_behavior: OVERWRITE
```

Model:
```sql
{{ config(
    materialized='incremental',
    properties={
        "format":          "'PARQUET'",
        "partitioned_by":  "ARRAY['dt']",
    }
) }}
select *, cast(event_ts as date) as dt
from {{ ref('stg_events') }}
{% if is_incremental() %}
  where event_ts >= current_date - interval '3' day
{% endif %}
```

#### incremental_predicates — limit target table scan

For very large target tables, restrict how much of the target Trino scans during MERGE:

```sql
{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    incremental_predicates=[
        "DBT_INTERNAL_DEST.event_ts >= current_date - interval '7' day"
    ],
) }}
```

`DBT_INTERNAL_DEST` = alias for the target table; `DBT_INTERNAL_SOURCE` = alias for the new data CTE.

#### on_schema_change

Controls behaviour when source query columns differ from target table columns:

| Value | Behaviour |
|---|---|
| `ignore` | Default. New columns silently dropped; removing columns causes run failure |
| `fail` | Error if schema diverges — forces explicit `--full-refresh` |
| `append_new_columns` | Adds new columns to target; keeps old columns |
| `sync_all_columns` | Adds new + removes dropped columns; handles type changes |

```sql
{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
) }}
```

### Ephemeral

Not materialised in the warehouse — inlined as a CTE in dependent models. Use for lightweight reusable transformations.

```sql
-- models/staging/stg_events_cleaned.sql
{{ config(materialized='ephemeral') }}

select * from {{ source('raw', 'events') }}
where event_id is not null
  and event_type != 'test'
```

Downstream model sees it as:
```sql
with __dbt__cte__stg_events_cleaned as (
    select * from raw.events where ...
)
select * from __dbt__cte__stg_events_cleaned ...
```

**Limits**: cannot `ref()` from operations/macros; don't overuse — long CTE chains are hard to debug.

### Materialized View

Backed by a Trino materialized view. dbt refreshes it on each `dbt run`.

```sql
{{ config(
    materialized='materialized_view',
    properties={
        'format': "'PARQUET'"
    },
) }}

select
    date_trunc('day', event_ts) as dt,
    event_type,
    count(*) as event_count
from {{ ref('stg_events') }}
group by 1, 2
```

Use when the connector manages refresh automatically and you want database-native incremental logic.

---

## Table Properties

Set Iceberg/Hive/Delta table properties via the `properties` dict. Values are **Trino SQL expressions** (strings must be double-quoted to produce a SQL string literal):

```sql
{{ config(
    materialized='table',
    properties={
        -- File format
        "format":        "'PARQUET'",          -- PARQUET | ORC | AVRO

        -- Iceberg partitioning
        "partitioning":  "ARRAY['day(event_ts)', 'bucket(32, user_id)']",

        -- Iceberg sorted_by (within data files)
        "sorted_by":     "ARRAY['user_id', 'event_type']",

        -- Iceberg format version (2 = full ACID)
        "format_version": "'2'",

        -- Target file size (bytes)
        "write_target_data_file_size_bytes": "536870912",   -- 512 MB

        -- Hive-style partitioning (non-Iceberg)
        "partitioned_by": "ARRAY['dt']",

        -- ORC Bloom filters
        "orc_bloom_filter_columns": "ARRAY['user_id', 'session_id']",
        "orc_bloom_filter_fpp":     "0.01",
    }
) }}
```

**Important**: String values must be quoted with inner single quotes so they produce a SQL string literal, e.g. `"'PARQUET'"` renders as `'PARQUET'` in the DDL.

### Partition transform reference

```sql
"partitioning": "ARRAY['day(event_ts)']"               -- daily
"partitioning": "ARRAY['hour(event_ts)']"              -- hourly
"partitioning": "ARRAY['month(event_ts)']"             -- monthly
"partitioning": "ARRAY['bucket(32, user_id)']"         -- hash bucket
"partitioning": "ARRAY['region', 'day(event_ts)']"     -- combined
"partitioning": "ARRAY['day(event_ts)', 'bucket(32, user_id)']"  -- time + bucket
```

---

## Session Properties

### Global (profile-level) — applies to all queries

```yaml
# profiles.yml
session_properties:
  query_max_run_time: 4h
  join_reordering_strategy: AUTOMATIC
  join_distribution_type: AUTOMATIC
  join_max_broadcast_table_size: 200MB
  task_writer_count: "4"
```

### Per-model (pre_hook) — applies to one model only

```sql
{{ config(
    pre_hook=[
        "set session query_max_run_time = '30m'",
        "set session join_distribution_type = 'BROADCAST'",
    ]
) }}
```

Use `pre_hook` for expensive models that need specific resource limits without affecting the whole profile.

---

## Sources

Define source tables in `_sources.yml`:

```yaml
# models/staging/_sources.yml
version: 2

sources:
  - name: raw
    database: iceberg_catalog   # Trino catalog
    schema: raw_data
    tables:
      - name: events
        description: "Raw clickstream events from Kafka sink"
        freshness:
          warn_after: {count: 1, period: hour}
          error_after: {count: 4, period: hour}
        loaded_at_field: event_ts
        columns:
          - name: event_id
            tests: [not_null, unique]
          - name: event_ts
            tests: [not_null]
```

Reference in models:
```sql
select * from {{ source('raw', 'events') }}
```

---

## Tests

### Built-in generic tests

```yaml
# models/marts/_marts.yml
version: 2

models:
  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: status
        tests:
          - accepted_values:
              values: ['placed', 'shipped', 'delivered', 'cancelled']
      - name: customer_id
        tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_id
```

### Singular tests (custom SQL)

```sql
-- tests/assert_positive_amounts.sql
select order_id
from {{ ref('fct_orders') }}
where amount < 0
```

### dbt-expectations (extended test library)

```bash
# packages.yml
packages:
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]
```

```yaml
columns:
  - name: amount
    tests:
      - dbt_expectations.expect_column_values_to_be_between:
          min_value: 0
          max_value: 1000000
```

---

## Seeds

Small CSV reference tables loaded with `dbt seed`.

```yaml
# dbt_project.yml
seeds:
  my_project:
    +schema: reference
    +properties:
      format: "'PARQUET'"
    country_codes:
      +column_types:
        code: varchar(2)
        name: varchar(100)
```

Batch size for large seed files (override default 1000 rows/batch):
```sql
-- macros/trino_batch_size.sql
{% macro trino__get_batch_size() %}
  {{ return(5000) }}
{% endmacro %}
```

---

## Snapshots (SCD Type 2)

```sql
-- snapshots/snp_customers.sql
{% snapshot snp_customers %}
{{
    config(
        target_schema   = 'snapshots',
        unique_key      = 'customer_id',
        strategy        = 'timestamp',
        updated_at      = 'updated_at',
        invalidate_hard_deletes = true,
    )
}}
select * from {{ source('raw', 'customers') }}
{% endsnapshot %}
```

For Iceberg (millisecond precision override):
```sql
-- macros/trino_current_timestamp.sql
{% macro trino__current_timestamp() %}
    current_timestamp(6)
{% endmacro %}
```

---

## Grants

Control SELECT/INSERT access on materialised objects. Supported by Starburst Enterprise, Galaxy, and Hive sql-standard security.

```yaml
# models/marts/_marts.yml
models:
  - name: fct_orders
    config:
      grants:
        select: ['reporter_role', 'bi_service']
```

Or in model config:
```sql
{{ config(grants={'select': ['reporter_role', 'bi_service']}) }}
```

---

## Model Contracts

Enforce column-level constraints. Trino supports `not_null` only — ensure the underlying connector supports it before enabling.

```yaml
models:
  - name: fct_orders
    config:
      contract:
        enforced: true
    columns:
      - name: order_id
        data_type: bigint
        constraints:
          - type: not_null
      - name: amount
        data_type: decimal(18,2)
```

---

## Data Modeling Patterns

### Three-layer project layout

```
staging/     → 1:1 with source tables. Rename, cast, light cleaning. Always views.
intermediate/ → Complex joins and business logic that feeds multiple marts. Views or ephemeral.
marts/        → Business-facing fact and dimension tables. Tables, queried by BI.
```

**Naming conventions**:
- `stg_<source>__<table>` — staging (e.g. `stg_postgres__orders`)
- `int_<entity>_<verb>` — intermediate (e.g. `int_orders_enriched`)
- `fct_<event>` — fact table (e.g. `fct_orders`)
- `dim_<entity>` — dimension table (e.g. `dim_customers`)
- `agg_<metric>_<grain>` — aggregate (e.g. `agg_revenue_daily`)

### Staging model

```sql
-- models/staging/stg_postgres__orders.sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('postgres', 'orders') }}
),
renamed as (
    select
        id                                          as order_id,
        user_id                                     as customer_id,
        status,
        cast(amount as decimal(18, 2))              as amount,
        cast(created_at as timestamp(6) with time zone) as created_at,
        cast(updated_at as timestamp(6) with time zone) as updated_at
    from source
)
select * from renamed
```

### Fact table (incremental)

```sql
-- models/marts/core/fct_orders.sql
{{ config(
    materialized        = 'incremental',
    unique_key          = 'order_id',
    incremental_strategy= 'merge',
    on_schema_change    = 'sync_all_columns',
    properties          = {
        "format":       "'PARQUET'",
        "partitioning": "ARRAY['month(created_at)']",
        "sorted_by":    "ARRAY['customer_id', 'created_at']",
    }
) }}

with orders as (
    select * from {{ ref('stg_postgres__orders') }}
    {% if is_incremental() %}
      where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),
customers as (
    select * from {{ ref('dim_customers') }}
)
select
    o.order_id,
    o.customer_id,
    c.country,
    o.status,
    o.amount,
    o.created_at,
    o.updated_at
from orders o
left join customers c using (customer_id)
```

### Dimension table (full refresh on each run)

```sql
-- models/marts/core/dim_customers.sql
{{ config(
    materialized    = 'table',
    on_table_exists = 'drop',
    properties      = {"format": "'PARQUET'"},
) }}

select
    customer_id,
    name,
    email,
    country,
    segment,
    created_at
from {{ ref('stg_postgres__customers') }}
```

### Daily aggregate (incremental append)

```sql
-- models/marts/finance/agg_revenue_daily.sql
{{ config(
    materialized    = 'incremental',
    properties      = {
        "format":       "'PARQUET'",
        "partitioning": "ARRAY['month(dt)']",
    }
) }}

select
    date_trunc('day', created_at)   as dt,
    country,
    status,
    count(*)                        as order_count,
    sum(amount)                     as revenue,
    avg(amount)                     as avg_order_value
from {{ ref('fct_orders') }}

{% if is_incremental() %}
  where created_at >= (select max(dt) from {{ this }}) - interval '3' day
{% endif %}

group by 1, 2, 3
```

### Cross-catalog / federated query

Trino's key advantage: join tables from different catalogs in one dbt model:

```sql
-- models/marts/int_orders_with_crm.sql
{{ config(materialized='view') }}

select
    o.order_id,
    o.amount,
    c.salesforce_id,
    c.account_tier
from {{ ref('stg_postgres__orders') }}      o   -- PostgreSQL via Trino connector
left join {{ ref('stg_salesforce__accounts') }} c   -- Salesforce via Trino connector
    on o.customer_id = c.customer_id
```

No ETL needed between systems — Trino executes the join at query time across connectors.

---

## Useful CLI Commands

```bash
# Initial setup
dbt debug                             # test connection and config
dbt deps                              # install packages from packages.yml

# Build
dbt run                               # run all models
dbt run -s staging                    # run staging folder only
dbt run -s fct_orders+               # run fct_orders and all downstreams
dbt run -s +fct_orders               # run fct_orders and all upstreams
dbt run --full-refresh -s fct_orders  # rebuild incremental from scratch
dbt build                             # run + test + seed + snapshot

# Testing
dbt test                              # run all tests
dbt test -s fct_orders               # test one model
dbt source freshness                  # check source freshness

# Inspection
dbt compile -s fct_orders            # show compiled SQL without running
dbt show -s fct_orders --limit 10    # preview model output
dbt ls -s config.materialized:incremental  # list all incremental models

# Seeds and snapshots
dbt seed
dbt snapshot
```

---

## CI/CD Pattern

### Slim CI (run only changed models)

```bash
# In CI pipeline: compare against production manifest
dbt run \
  -s state:modified+ \
  --defer \
  --state ./prod-manifest/

dbt test \
  -s state:modified+ \
  --defer \
  --state ./prod-manifest/
```

`--defer` makes unmodified upstream refs resolve to their production counterparts, avoiding a full rebuild.

### GitHub Actions example

```yaml
# .github/workflows/dbt_ci.yml
name: dbt CI
on: [pull_request]

jobs:
  dbt-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: pip install dbt-trino

      - name: Download prod manifest
        run: aws s3 cp s3://my-bucket/dbt-artifacts/manifest.json ./prod-manifest/manifest.json

      - name: dbt run changed models
        env:
          DBT_TRINO_PASSWORD: ${{ secrets.DBT_TRINO_PASSWORD }}
        run: |
          dbt run \
            --profiles-dir . \
            --target ci \
            --select state:modified+ \
            --defer \
            --state ./prod-manifest/

      - name: dbt test changed models
        run: |
          dbt test \
            --profiles-dir . \
            --target ci \
            --select state:modified+ \
            --defer \
            --state ./prod-manifest/
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| `is_incremental()` in a CTE used later | Late filter — all rows are still read upstream | Move filter to the earliest CTE / source subquery |
| `unique_key` with NULLs | MERGE/DELETE+INSERT silently fails to match NULL rows | Wrap with `coalesce(unique_key, 'unknown')` or ensure NOT NULL |
| `incremental_strategy='merge'` on Hive connector | Hive doesn't support MERGE | Use `delete+insert` or `append` for Hive catalogs |
| `on_schema_change='ignore'` with evolving sources | New columns silently dropped | Use `sync_all_columns` or `append_new_columns` |
| String table properties without inner quotes | `"PARQUET"` renders as bare identifier — DDL error | Always `"'PARQUET'"` (outer double + inner single quotes) |
| `threads > 8` without tuning Trino resources | Too many concurrent queries → coordinator OOM | Keep threads ≤ 8 unless Trino cluster is sized for it |
| `on_table_exists='rename'` with AWS Glue | Glue doesn't support atomic rename → failures | Set `on_table_exists='drop'` for Glue catalogs |
| Staging models as `table` | Rebuilds every run, wastes compute | Staging is always `view` — only marts are tables |
| No `--full-refresh` after logic change | Incremental model contains stale rows computed by old logic | After changing filter logic, always `--full-refresh` |
| Storing passwords in profiles.yml plaintext | Credential leak in version control | Use `{{ env_var('DBT_TRINO_PASSWORD') }}` |
| Skipping `dbt test` in CI | Silent data quality regressions | Always run `dbt test -s state:modified+` in CI |

---

## Output Expectations

When working on dbt + Trino tasks:
- Show the full model config block (`{{ config(...) }}`) with all relevant options.
- For incremental models: show both the full-load path and the `is_incremental()` filter path.
- Specify `properties` dict with correct quoting (`"'PARQUET'"`) for table/format configs.
- Recommend materialization and incremental strategy based on connector (Iceberg vs Hive vs Delta).
- Flag when `--full-refresh` is needed after a logic or schema change.
- For cross-catalog patterns: show how `source()` and `ref()` map to different Trino catalogs.

---

## References

- dbt + Trino setup: https://docs.getdbt.com/docs/core/connect-data-platform/trino-setup
- Trino-specific configs: https://docs.getdbt.com/reference/resource-configs/trino-configs
- dbt-trino adapter: https://github.com/starburstdata/dbt-trino
- dbt incremental models: https://docs.getdbt.com/docs/build/incremental-models
- Local Trino + Iceberg spec: `docs/specs/trino_iceberg_performance_optimization.md`
