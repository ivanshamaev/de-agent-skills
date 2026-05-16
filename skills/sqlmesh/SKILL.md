---
name: sqlmesh
description: SQLMesh data transformation framework — model kinds (FULL/INCREMENTAL_BY_TIME_RANGE/INCREMENTAL_BY_UNIQUE_KEY/SCD_TYPE_2/VIEW/SEED), plan/apply workflow, virtual environments, state-aware deploys, audits, unit tests, CI/CD, dbt migration
---

# SQLMesh

## When to Use

Activate this skill when the task involves:
- Writing SQLMesh models (FULL, incremental, SCD, seeds, views)
- Understanding the plan/apply workflow and virtual environments
- Migrating from dbt to SQLMesh
- Setting up SQLMesh CI/CD pipelines
- Writing audits and unit tests for SQLMesh models
- Configuring adapters (DuckDB, Spark, Trino, PostgreSQL, BigQuery)
- Diagnosing breaking vs. non-breaking changes

---

## Core Concepts

```
┌────────────────────────────────────────────────────────────┐
│  SQLMesh Workflow                                          │
│                                                            │
│  1. Edit models (SQL/Python files in models/)              │
│     ↓                                                      │
│  2. sqlmesh plan [environment]                             │
│     → diff: breaking / non-breaking / new                  │
│     → shows backfill dates required                        │
│     ↓                                                      │
│  3. sqlmesh apply                                          │
│     → creates physical tables (fingerprinted versions)     │
│     → runs backfill for changed intervals                  │
│     → updates environment to point to new versions         │
│                                                            │
│  Virtual Environments                                      │
│  dev  → model_v2_dev   (only changed models are new)       │
│  prod → model_v1_prod  (unchanged → re-use prod tables)    │
│                                                            │
│  State: stored in state DB (DuckDB/PostgreSQL/Snowflake)   │
└────────────────────────────────────────────────────────────┘
```

**Key differentiator from dbt**: SQLMesh tracks which physical table version each model corresponds to. Promoting dev → prod for an unchanged model is a **zero-cost reference swap** (no re-computation). Only truly modified models backfill.

---

## Installation & Project Setup

```bash
pip install sqlmesh
# adapter extras:
pip install "sqlmesh[spark]"    # PySpark
pip install "sqlmesh[trino]"    # Trino
pip install "sqlmesh[bigquery]" # BigQuery
pip install "sqlmesh[dbt]"      # dbt project import

# Create a new project
sqlmesh init -t duckdb my_project     # DuckDB for local dev
cd my_project
```

### Project Structure

```
my_project/
├── config.yaml               # adapter + environments config
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql
│   │   └── stg_customers.sql
│   ├── silver/
│   │   └── orders.sql
│   └── gold/
│       └── fct_revenue.sql
├── audits/
│   └── custom_audits.sql
├── tests/
│   └── test_stg_orders.yaml
├── seeds/
│   └── payment_methods.csv
└── macros/
    └── helpers.py
```

### config.yaml

```yaml
gateways:
  local:
    connection:
      type: duckdb
      database: dev.db

  production:
    connection:
      type: spark
      config:
        spark.master: k8s://https://k8s-api:6443
        spark.kubernetes.container.image: registry.local/spark:3.5.1
      catalog: iceberg
    state_connection:
      type: postgres
      host: postgres-prod.internal
      port: 5432
      database: sqlmesh_state
      user: sqlmesh
      password: "{{ env_var('SQLMESH_DB_PASSWORD') }}"

default_gateway: local

model_defaults:
  dialect: spark
  cron: "@daily"
  storage_format: parquet
  partitioned_by_: [ds]

variables:
  lookback_days: 7
```

---

## Model Kinds

### FULL — Full Refresh

```sql
MODEL (
  name silver.dim_products,
  kind FULL,
  cron '@daily',
  grain product_id,
  audits (NOT_NULL(columns = [product_id, name]), UNIQUE_VALUES(columns = [product_id]))
);

SELECT
    product_id,
    name,
    category,
    price,
    is_active
FROM bronze.raw_products
WHERE is_deleted = FALSE
```

### VIEW

```sql
MODEL (
  name silver.v_active_customers,
  kind VIEW
);

SELECT customer_id, email, created_at
FROM silver.customers
WHERE is_active = TRUE
```

Materialized view (adapter-dependent):
```sql
MODEL (
  name silver.mv_daily_stats,
  kind VIEW (materialized TRUE)
);

SELECT DATE_TRUNC('day', event_at) as day, COUNT(*) as cnt
FROM bronze.events
GROUP BY 1
```

### INCREMENTAL_BY_TIME_RANGE — Time-Partitioned

```sql
MODEL (
  name silver.events,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date,
    lookback 3                   -- reprocess last 3 days to handle late arrivals
  ),
  start '2024-01-01',
  cron '@daily',
  grain event_id,
  partitioned_by event_date,
  audits (
    NOT_NULL(columns = [event_id, event_date, user_id]),
    UNIQUE_VALUES(columns = [event_id])
  )
);

SELECT
    event_id,
    user_id,
    event_type,
    event_date,
    properties
FROM bronze.raw_events
WHERE
    -- SQLMesh injects @start_ds / @end_ds (date strings) or @start_date / @end_date (timestamps)
    event_date BETWEEN @start_ds AND @end_ds
    AND is_valid = TRUE
```

Available time macros:

| Macro | Type | Example Value |
|-------|------|---------------|
| `@start_ds` | string (YYYY-MM-DD) | `'2024-03-15'` |
| `@end_ds` | string (YYYY-MM-DD) | `'2024-03-15'` |
| `@start_date` | date | `DATE '2024-03-15'` |
| `@end_date` | date | `DATE '2024-03-15'` |
| `@start_ts` | timestamp | `TIMESTAMP '2024-03-15 00:00:00'` |
| `@end_ts` | timestamp | `TIMESTAMP '2024-03-15 23:59:59'` |

### INCREMENTAL_BY_UNIQUE_KEY — Upsert

```sql
MODEL (
  name silver.customers,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key customer_id,
    when_matched (
      WHEN MATCHED THEN UPDATE SET
        target.email        = source.email,
        target.updated_at   = source.updated_at,
        target.tier         = source.tier
    )
  ),
  cron '@daily',
  grain customer_id,
  audits (NOT_NULL(columns = [customer_id, email]))
);

SELECT
    customer_id,
    email,
    tier,
    updated_at
FROM bronze.raw_customers
```

Composite key:
```sql
kind INCREMENTAL_BY_UNIQUE_KEY (
  unique_key (order_id, line_item_id)
)
```

### SCD_TYPE_2_BY_TIME — Slowly Changing Dimension

```sql
MODEL (
  name silver.scd_products,
  kind SCD_TYPE_2_BY_TIME (
    unique_key product_id
    -- SQLMesh auto-adds: valid_from TIMESTAMP, valid_to TIMESTAMP (NULL = current)
  ),
  cron '@daily',
  grain product_id
);

SELECT
    product_id,
    name,
    price,
    category,
    updated_at             -- SQLMesh uses this to detect changes
FROM bronze.raw_products
```

Query current records: `WHERE valid_to IS NULL`

Query as-of date:
```sql
SELECT * FROM silver.scd_products
WHERE product_id = 42
  AND valid_from <= '2024-06-01'
  AND (valid_to IS NULL OR valid_to > '2024-06-01')
```

### SCD_TYPE_2_BY_COLUMN — Change-Detected SCD

```sql
MODEL (
  name silver.scd_prices,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key product_id,
    columns [price, tier]         -- track changes only in these columns
  ),
  cron '@daily'
);

SELECT product_id, name, price, tier
FROM bronze.products
```

### SEED — Static CSV Data

```sql
MODEL (
  name bronze.payment_methods,
  kind SEED (
    path '../../seeds/payment_methods.csv'
  ),
  columns (
    method_id    INT,
    method_name  VARCHAR,
    is_active    BOOLEAN
  ),
  grain method_id
);
```

---

## MODEL() Properties Reference

```sql
MODEL (
  name          silver.orders,          -- required: schema.model_name
  kind          INCREMENTAL_BY_TIME_RANGE (time_column created_at),

  -- Scheduling
  cron          '@daily',               -- @hourly | @weekly | cron expression
  start         '2024-01-01',           -- earliest date for backfill
  batch_size    30,                     -- days per backfill batch
  batch_concurrency 2,                  -- parallel batches

  -- Physical layout
  storage_format parquet,              -- parquet | delta | iceberg | orc
  partitioned_by created_date,         -- partition column(s)
  clustered_by  customer_id,           -- clustering key

  -- Data quality
  grain         order_id,              -- primary key for uniqueness
  audits (
    NOT_NULL(columns = [order_id, created_at]),
    UNIQUE_VALUES(columns = [order_id])
  ),

  -- Metadata
  owner         'data-platform',
  stamp         'v2',                  -- part of version fingerprint
  description   'Incremental orders table',
  tags          [orders, silver],
  dialect       spark                  -- override global default
);
```

---

## Macros

SQLMesh macros are Python functions decorated with `@macro()`.

```python
# macros/helpers.py
from sqlmesh import macro
from sqlglot import exp

@macro()
def safe_divide(evaluator, numerator, denominator):
    """Cross-database safe division returning NULL on divide-by-zero."""
    return exp.Case(
        conditions=[exp.EQ(this=denominator, expression=exp.Literal.number(0))],
        default=exp.Null(),
        else_=exp.Div(this=exp.Cast(this=numerator, to=exp.DataType.build("FLOAT")), expression=denominator),
    )

@macro()
def current_date_str(evaluator):
    from datetime import date
    return exp.Literal.string(date.today().isoformat())
```

Use in models:
```sql
SELECT
    @safe_divide(revenue, sessions)  AS revenue_per_session,
    @current_date_str()              AS loaded_date
FROM silver.marketing
```

### Built-in Macro Variables

```sql
-- In model SQL
@model_name           -- 'silver.orders'
@schema               -- 'silver'
@this                 -- fully qualified table reference
@this_model           -- alias for @this

-- Runtime
@execution_time       -- timestamp of current run
@start_ds, @end_ds    -- time range boundaries (INCREMENTAL_BY_TIME_RANGE)
@start_date, @end_date
@start_ts, @end_ts
```

---

## Audits

Audits are SQL queries that **must return 0 rows** to pass. They run after each materialization.

### Built-in Audits

```sql
MODEL (
  name silver.orders,
  audits (
    NOT_NULL(columns = [order_id, customer_id, created_at]),
    UNIQUE_VALUES(columns = [order_id]),
    ACCEPTED_VALUES(column = status, is_in = ('pending', 'complete', 'cancelled')),
    ACCEPTED_RANGE(column = total, min_v = 0, max_v = 1000000),
    NUMBER_OF_ROWS(threshold = 1)   -- at least 1 row
  )
)
```

### Custom Audit

```sql
-- audits/no_negative_totals.sql
AUDIT (
  name no_negative_totals,
  dialect spark
);

SELECT order_id, total
FROM @this
WHERE total < 0
-- Returns rows = audit failures
```

```sql
-- Use in a model:
MODEL (
  name silver.orders,
  audits (no_negative_totals())
);
```

---

## Unit Tests

Unit tests verify model SQL against static input/output data.

```yaml
# tests/test_stg_orders.yaml
test_stg_orders_basic:
  model: silver.stg_orders
  inputs:
    bronze.raw_orders:
      rows:
        - order_id: 1
          customer_id: 100
          total: 1500
          status: "COMPLETE"
          created_at: "2024-03-15"
        - order_id: 2
          customer_id: 101
          total: -50
          status: "PENDING"
          created_at: "2024-03-15"
  outputs:
    rows:
      - order_id: 1
        customer_id: 100
        total_usd: 15.00
        status: "complete"
        created_date: "2024-03-15"
    partial: true   # allow extra rows in output
```

Generate tests automatically:
```bash
sqlmesh create_test silver.stg_orders --query bronze.raw_orders "SELECT * FROM bronze.raw_orders LIMIT 5"
```

Run tests:
```bash
sqlmesh test
sqlmesh test silver.stg_orders      # specific model
```

---

## Plan / Apply Workflow

### Development Cycle

```bash
# 1. Create dev environment plan
sqlmesh plan dev

# SQLMesh output:
# New models:
#   + silver.new_model
# Directly Modified:
#   ~ silver.events [non-breaking]  -- added a column
# Indirectly Modified:
#   ~ gold.fct_revenue             -- depends on silver.events

# Backfill: silver.events (2024-01-01 - 2024-03-15)
# Apply - will use existing tables? [y/N]: y

# 2. Preview specific date range (cheaper in dev)
sqlmesh plan dev --start 2024-03-01 --end 2024-03-15

# 3. Apply with auto-approval
sqlmesh plan dev --auto-approve
```

### Promote to Production

```bash
# Promote dev -> prod
# SQLMesh checks: what's in dev that prod doesn't have?
# Unchanged models → zero-cost reference swap
# Changed models   → backfill if needed
sqlmesh plan prod
sqlmesh plan prod --auto-approve
```

### Backfill Specific Models

```bash
# Re-run specific model for a date range
sqlmesh run --model silver.events --start 2024-01-01 --end 2024-03-15

# Restatement: reprocess existing data (fixes upstream issues)
sqlmesh plan --restate-model silver.events --start 2024-01-01
```

### Change Types

| Change | Classification | Behavior |
|--------|---------------|---------|
| Add column | Non-breaking | Backfill changed model only |
| Remove column | Breaking | Full backfill of model + downstream |
| Change filter logic | Breaking | Full backfill |
| Add comment | Metadata-only | No backfill |
| Rename model | Breaking | New physical table created |
| Fix typo in audit | Non-breaking | No backfill |

Use `--forward-only` to force non-breaking treatment (skip backfill, accept data as-is):
```bash
sqlmesh plan prod --forward-only
```

---

## CI/CD Pattern

```yaml
# .github/workflows/sqlmesh_ci.yml
name: SQLMesh CI

on:
  pull_request:
    paths: ["models/**", "audits/**", "tests/**", "macros/**"]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install SQLMesh
        run: pip install "sqlmesh[duckdb]"

      - name: Lint models
        run: sqlmesh format --check

      - name: Run unit tests
        run: sqlmesh test

      - name: Plan against CI environment (DuckDB, last 7 days)
        run: |
          sqlmesh plan ci \
            --start $(date -d '7 days ago' +%Y-%m-%d) \
            --end $(date +%Y-%m-%d) \
            --auto-approve
        env:
          SQLMESH_CI_CONNECTION: '{"type": "duckdb", "database": ":memory:"}'

  deploy_prod:
    needs: validate
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Deploy to production
        run: sqlmesh plan prod --auto-approve
        env:
          SQLMESH_DB_PASSWORD: ${{ secrets.SQLMESH_DB_PASSWORD }}
```

---

## dbt Migration

```bash
# Import existing dbt project
sqlmesh init -t dbt .

# SQLMesh reads dbt_project.yml, profiles.yml, models/, macros/
# Generates SQLMesh-compatible config.yaml
```

Key mapping:

| dbt | SQLMesh |
|-----|---------|
| `materialized: 'table'` | `kind FULL` |
| `materialized: 'incremental'` | `kind INCREMENTAL_BY_UNIQUE_KEY` or `INCREMENTAL_BY_TIME_RANGE` |
| `materialized: 'view'` | `kind VIEW` |
| `materialized: 'ephemeral'` | `kind EMBEDDED` |
| `{{ ref('model') }}` | `silver.model` (direct reference) |
| `{{ source('schema', 'table') }}` | `bronze.table` or EXTERNAL model |
| `is_incremental()` | removed — use `@start_ds`/`@end_ds` macros |
| `dbt test` | SQLMesh audits |
| `dbt run` | `sqlmesh run` |
| `dbt test` | `sqlmesh audit` |

---

## Anti-Patterns

1. **Calling `sqlmesh plan` from Airflow or a scheduler** — plans are interactive CI/CD tools; `sqlmesh run` is the correct runtime command. Plans prompt for user input.

2. **Not specifying `start` date on incremental models** — without `start`, SQLMesh will backfill from the beginning of time, causing infinite backfill on first deploy.

3. **Using `time_column` with non-UTC timestamps** — SQLMesh time macros operate in UTC. Storing timestamps in local time causes incorrect interval boundaries.

4. **Treating breaking changes as non-breaking via `--forward-only` in prod** — `--forward-only` skips backfill but leaves historical data stale. Use only for truly additive changes or when re-processing is not feasible.

5. **Storing state in DuckDB (default) in production** — DuckDB state is local-disk only; multiple workers can't share it. Use PostgreSQL, MySQL, or Snowflake as the state backend for multi-node deployments.

6. **Missing `lookback` on INCREMENTAL_BY_TIME_RANGE with late-arriving data** — without lookback, data arriving after the partition window is never re-processed. Set `lookback` to cover your SLA window.

7. **Using FULL models for large fact tables** — FULL models truncate and reload everything on every run, which is cost-prohibitive for billion-row tables. Use `INCREMENTAL_BY_TIME_RANGE` with appropriate partitioning.

8. **Mixing SQLMesh environments with dbt targets** — if migrating incrementally, don't run both tools against the same schema simultaneously; they will conflict on table ownership.

---

## References to Consult When Needed

- SQLMesh concepts: `sqlmesh.readthedocs.io/en/stable/concepts/overview/`
- Model kinds: `sqlmesh.readthedocs.io/en/stable/concepts/models/model_kinds/`
- Plans: `sqlmesh.readthedocs.io/en/stable/concepts/plans/`
- Built-in audits: `sqlmesh.readthedocs.io/en/stable/concepts/audits/`
- Tests: `sqlmesh.readthedocs.io/en/stable/concepts/tests/`
- dbt migration: `sqlmesh.readthedocs.io/en/stable/integrations/dbt/`
- CLI reference: `sqlmesh.readthedocs.io/en/stable/reference/cli/`
