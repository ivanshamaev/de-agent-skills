---
name: dbt-starrocks-performance
description: dbt + StarRocks performance tuning — model DAG optimization (fan-in/fan-out), incremental merge window sizing, partition-aware incremental filters, avoiding full table rebuilds, query plan hints in dbt SQL (LEADING/JOIN hints), materialized view as dbt model target, concurrent model execution with threads, pre/post hooks for ANALYZE TABLE, dbt run selectors to minimize rebuilt models
---

# dbt + StarRocks Performance Tuning

## When to Use

- Incremental dbt models that run slowly due to poor partition filtering
- dbt DAG with wide fan-out causing excessive table rebuilds
- Need to run ANALYZE TABLE after dbt model refreshes
- Optimizing join order and query plans for complex dbt models
- Tuning thread count and model concurrency for large dbt projects

---

## Partition-Aware Incremental Filters

The most impactful optimization: ensure incremental models only scan new partitions.

```sql
-- BAD: scans entire source table even in incremental mode
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partition_by={"field": "created_at", "data_type": "date", "granularity": "day"}
) }}

SELECT * FROM {{ source('raw', 'events') }}
{% if is_incremental() %}
WHERE event_ts > (SELECT MAX(event_ts) FROM {{ this }})
{% endif %}
```

The `MAX(event_ts)` subquery scans `this` — fine for small tables but expensive for billions of rows.

```sql
-- GOOD: use partition column for both filter and watermark
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "event_date", "data_type": "date", "granularity": "day"}
) }}

{% set lookback_days = 3 %}

SELECT
    DATE(event_ts) AS event_date,
    user_id,
    event_type,
    event_ts
FROM {{ source('raw', 'events') }}

{% if is_incremental() %}
-- Reprocess last N days to handle late-arriving data
WHERE DATE(event_ts) >= DATE_SUB(CURDATE(), INTERVAL {{ lookback_days }} DAY)
{% endif %}
```

With `insert_overwrite`, dbt replaces only the matching partitions — no full scan of `this`.

---

## Late-Arriving Data Window

Balance completeness vs performance:

```sql
{% set incremental_lookback_days = var('lookback_days', 3) %}

{% if is_incremental() %}
WHERE DATE(created_at) >= DATE_SUB(
    -- Use a fixed reference point, not NOW(), for reproducibility
    DATE('{{ var("ds", modules.datetime.date.today().isoformat()) }}'),
    INTERVAL {{ incremental_lookback_days }} DAY
)
{% endif %}
```

Run with override:
```bash
dbt run --vars '{"lookback_days": 7, "ds": "2024-01-15"}'
```

---

## ANALYZE TABLE via Post-Hook

Always refresh CBO statistics after materializing a table:

```sql
-- models/gold/orders_daily.sql
{{ config(
    materialized='table',
    post_hook=[
        "ANALYZE TABLE {{ this }} WITH ASYNC MODE",
    ]
) }}

SELECT ...
```

For incremental models with partition granularity:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "dt", "data_type": "date", "granularity": "day"},
    post_hook=[
        -- Analyze only today's partition (much faster than full table)
        "ANALYZE TABLE {{ this }} PARTITION (p{{ run_started_at.strftime('%Y%m%d') }}) WITH ASYNC MODE",
    ]
) }}
```

Bulk ANALYZE in post-hook for large projects:
```yaml
# dbt_project.yml
models:
  my_project:
    gold:
      +post-hook: "ANALYZE TABLE {{ this }} WITH ASYNC MODE"
```

---

## Query Hints in dbt Models

Embed StarRocks query hints directly in model SQL for complex join optimization:

```sql
-- models/gold/revenue_report.sql
{{ config(materialized='table') }}

-- Hint: force join order (orders first, then products)
SELECT /*+ LEADING(o p c) JOIN_METHOD(o, p, BROADCAST) */
    p.category,
    c.region,
    COUNT(*) AS order_count,
    SUM(o.amount) AS revenue
FROM {{ ref('orders') }} o
JOIN {{ ref('dim_products') }} p USING (product_id)
JOIN {{ ref('dim_customers') }} c USING (customer_id)
WHERE o.dt = CURDATE()
GROUP BY p.category, c.region
```

---

## Pre-Hook: Ensure Partition Exists

For models that INSERT OVERWRITE into manually managed partitions:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    pre_hook=[
        """
        ALTER TABLE {{ this }}
        ADD PARTITION IF NOT EXISTS p{{ run_started_at.strftime('%Y%m%d') }}
        VALUES [("{{ run_started_at.strftime('%Y-%m-%d') }}"),
                ("{{ (run_started_at + modules.datetime.timedelta(days=1)).strftime('%Y-%m-%d') }}"))
        """
    ]
) }}
```

---

## Materialized View as dbt Model

Create a StarRocks async MV using a dbt custom materialization or post-hook:

```sql
-- Approach 1: Create MV in a post-hook on the base table
{{ config(
    materialized='table',
    post_hook=[
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS {{ this }}_mv
        DISTRIBUTED BY HASH(region) BUCKETS 8
        REFRESH ASYNC EVERY (INTERVAL 1 HOUR)
        AS SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue
           FROM {{ this }}
           GROUP BY region
        """
    ]
) }}
```

---

## Thread and Concurrency Tuning

### profiles.yml Thread Config

```yaml
starrocks_project:
  outputs:
    prod:
      type: starrocks
      threads: 8           # concurrent model builds (default: 1)
      # ...
```

### Which Models Can Run in Parallel

dbt parallelizes models with no dependencies between them. Ensure your DAG is as wide as possible:

```
staging/stg_orders    ─┐
staging/stg_customers ─┼──► silver/orders (sequential join)
staging/stg_products  ─┘

silver/orders_by_region ─┐
silver/orders_by_product ─┼──► gold/revenue_report
silver/customer_segments ─┘
```

Avoid chains where each model depends on the previous — this prevents parallelism.

---

## Model Selection for Minimal Rebuilds

```bash
# Only run models that changed + their downstream dependents
dbt run --select state:modified+

# Run only changed models, no downstream
dbt run --select state:modified

# Run only a specific model and its upstream dependencies
dbt run --select +orders

# Run a specific tag group
dbt run --select tag:daily

# Exclude expensive models from CI
dbt run --exclude tag:expensive
```

---

## var() for Environment-Specific Config

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "dt",
        "data_type": "date",
        "granularity": var('partition_granularity', 'day')
    },
    properties={
        "replication_num": var('replication_num', '3')
    }
) }}
```

```yaml
# dbt_project.yml
vars:
  partition_granularity: "day"
  replication_num: "3"    # override to "1" in dev
```

```bash
dbt run --vars '{"replication_num": "1"}' --target dev
```

---

## Materialization Choice vs Performance

| Model Type | Materialization | Rebuild Frequency | Performance |
|-----------|----------------|-------------------|-------------|
| Staging (raw cleanup) | `table` | Daily | Full rebuild is cheap |
| Silver (CDC upsert) | `incremental` (unique_key) | Continuous | Only processes delta |
| Intermediate heavy joins | `table` | Daily | Avoid ephemeral for >100M rows |
| Gold partitions | `incremental` (insert_overwrite) | Daily | Replace partitions, not full table |
| Real-time metrics | MV via post-hook | Auto (async) | Near-real-time without DAG run |
| Rarely queried reports | `view` | On query | No storage cost |

---

## Profiling Slow Models

```bash
# Show timing for each model
dbt run --select orders_daily --log-level debug 2>&1 | grep "Completed"

# Use dbt artifacts for timing analysis
cat target/run_results.json | python3 -c "
import json, sys
r = json.load(sys.stdin)
for n in sorted(r['results'], key=lambda x: x.get('execution_time', 0), reverse=True)[:10]:
    print(f\"{n['execution_time']:.1f}s  {n['unique_id']}\")
"
```

---

## Anti-Patterns

1. **`incremental_strategy='append'` on a growing table without watermark** — re-runs append all historical rows repeatedly; always add an `is_incremental()` filter.
2. **`MAX(event_ts) FROM {{ this }}`** for watermark on billion-row tables — scans the entire previous model; use partition column bounds instead.
3. **`threads: 1`** — serial execution for large dbt projects can take hours; set to 4-16 based on BE count.
4. **No post-hook ANALYZE** — CBO uses stale stats after model rebuild, producing bad plans for downstream queries; always ANALYZE after table materialization.
5. **Using `view` for heavy aggregations** — view recomputes the aggregation on every BI query; materialize as table or MV.
6. **`on_schema_change='sync_all_columns'`** on large tables — triggers full table rebuild every time a column is added; use `append_new_columns` instead.

---

## References

- dbt-starrocks incremental: `github.com/StarRocks/starrocks/tree/main/contrib/dbt-connector`
- ANALYZE TABLE: `docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ANALYZE_TABLE/`
- dbt model selection: `docs.getdbt.com/reference/node-selection/syntax`
- Related skills: `[[dbt-starrocks-models]]`, `[[starrocks-cbo]]`, `[[starrocks-query-optimizer]]`, `[[dbt-core]]`
