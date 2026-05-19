---
name: trino-dbt-query-performance
description: Optimizing dbt-generated SQL performance on Trino — ephemeral vs view vs table materialization trade-offs, CTE explosion patterns, partition-aware incremental filters (bounded watermarks), MERGE vs delete+insert strategy selection, avoiding full-refresh anti-patterns, query hints in dbt SQL (BROADCAST hint), dbt thread tuning, session_properties for dbt runs, ANALYZE post-hooks, incremental model design patterns for Iceberg, avoiding small file proliferation from frequent incremental runs
---

# dbt + Trino Query Performance

## When to Use

- dbt models are running slower than expected on Trino
- Incremental models are running full scans despite `is_incremental()` filter
- Choosing between materialization strategies for a given model
- Reducing dbt run time by parallelizing or optimizing model dependencies
- Preventing small file accumulation from frequent incremental writes

---

## Materialization Trade-Off Matrix

| Materialization | When to Use | Trino Cost |
|----------------|------------|-----------|
| `view` | Cheap transformations, always-fresh seldom queried | Query-time: recomputed every run |
| `ephemeral` | Shared logic included inline, not stored | No I/O; becomes a CTE in parent |
| `table` | Stable aggregates, heavy joins queried frequently | Build-time: full rebuild each run |
| `incremental` | High-churn fact tables where full rebuild is expensive | Build-time: partial update |
| `materialized_view` | Real-time pre-aggregation updated by Trino automatically | Refresh on every `dbt run` |

---

## Ephemeral Models: Avoid CTE Explosion

`ephemeral` models are inlined as CTEs. Deep ephemeral chains create one giant SQL with many CTEs — harder for the optimizer to plan efficiently.

```sql
-- BAD: 5-level deep ephemeral chain → 1 query with 5 CTEs
-- stg_a (ephemeral) → stg_b (ephemeral) → int_c (ephemeral) → int_d (ephemeral) → mart_e

-- GOOD: break the chain with a materialized intermediate at the mid-point
-- stg_a (ephemeral) → stg_b (ephemeral)
-- int_combined (table/incremental)   ← write to Iceberg here
-- int_c (ephemeral) → mart_e
```

**Rule of thumb**: no ephemeral chain deeper than 3 levels. Materialize anything that's heavy or reused by multiple models.

---

## Incremental Models: Partition-Aware Filters

The `is_incremental()` filter must be bounded to prevent full table scans on large tables.

```sql
-- BAD: reads all rows from source on every incremental run
{{  config(materialized = 'incremental') }}
SELECT * FROM {{ source('bronze', 'events') }}
{% if is_incremental() %}
WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}

-- The MAX(event_time) subquery itself scans the whole target table;
-- if target is huge, this subquery is slow even before the filter.
```

```sql
-- GOOD: bounded watermark + partition pruning on source
{{
  config(
    materialized = 'incremental',
    properties   = {"partitioning": "ARRAY['day(event_time)']"}
  )
}}

{% set lookback_days = 3 %}

SELECT * FROM {{ source('bronze', 'events') }}
{% if is_incremental() %}
WHERE event_time >= (
  -- Bounded to last 3 days regardless of max — prevents full scan
  SELECT GREATEST(
    MAX(event_time),
    CAST(DATE_ADD('day', -{{ lookback_days }}, CURRENT_DATE) AS TIMESTAMP)
  ) FROM {{ this }}
)
AND event_time < CAST(CURRENT_DATE AS TIMESTAMP)  -- partition pruning on source too
{% endif %}
```

**Why 3-day lookback**: handles late-arriving data while keeping the incremental window small.

---

## MERGE vs delete+insert: Choosing the Right Strategy

| Scenario | Strategy | Why |
|----------|----------|-----|
| Primary key upsert, < 10% rows change | `merge` | Only touches changed rows |
| Partition-based refresh (whole day overwritten) | `delete+insert` | Faster than row-level operations |
| High delete ratio (> 30% rows change) | `delete+insert` | MERGE equality deletes accumulate; compact more often |
| No deduplication needed | `append` | Fastest — just INSERT |

```sql
-- delete+insert: more predictable for time-partitioned tables
{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'order_date',          -- delete matching keys first
    properties           = {
      "format":        "'PARQUET'",
      "partitioning":  "ARRAY['month(order_date)']"
    }
  )
}}

SELECT order_date, SUM(amount) AS daily_revenue
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
WHERE order_date >= DATE_ADD('day', -7, CURRENT_DATE)  -- reprocess last 7 days
{% endif %}
GROUP BY order_date
```

---

## Avoiding Full-Refresh Surprises

`dbt run --full-refresh` drops and recreates incremental tables. On large Iceberg tables this can take hours.

```yaml
# Protect critical tables from accidental full-refresh
# dbt_project.yml
models:
  data_platform:
    mart:
      fact_orders:
        +full_refresh: false   # raises error if --full-refresh is passed
```

```sql
-- Model-level guard
{{ config(full_refresh = false) }}
```

---

## Query Hints in dbt SQL

Use Trino query hints for specific join strategies:

```sql
-- models/mart/fact_order_items.sql
-- Force broadcast join for small dimension table
SELECT /*+ BROADCAST(d) */
    f.order_id,
    f.item_id,
    d.product_name,
    d.category,
    f.quantity,
    f.unit_price
FROM {{ ref('int_order_items') }} f
JOIN {{ ref('dim_product') }} d ON f.product_id = d.product_id
```

---

## Session Properties for dbt Runs

Set Trino session properties per target in `profiles.yml`:

```yaml
# ~/.dbt/profiles.yml
data_platform:
  outputs:
    prod:
      type: trino
      ...
      session_properties:
        # Allow longer dbt runs
        query_max_run_time: 8h
        # Compress network exchanges (reduces shuffle overhead ~50%)
        exchange_compression_codec: LZ4
        # Enable spill for heavy aggregations
        spill_enabled: "true"
        # Sort within files for better downstream performance
        sorted_writing_enabled: "true"
        # CBO join reordering
        join_reordering_strategy: AUTOMATIC
        join_distribution_type: AUTOMATIC
```

---

## Thread Tuning

dbt parallelizes independent models using `threads`. Optimal value depends on cluster size.

```yaml
# profiles.yml
dev:
  threads: 4   # 1 thread per 2 workers is a reasonable start

prod:
  threads: 16  # 10-worker cluster: 16 threads is safe
```

```bash
# Override threads at runtime
dbt run --threads 8

# Profile the model execution DAG — see where time is spent
dbt run --profiles-dir . --target prod 2>&1 | grep "Completed\|Running"
```

---

## ANALYZE Post-Hooks for CBO

Without statistics, Trino's CBO defaults to worst-case estimates, choosing suboptimal join strategies.

```yaml
# dbt_project.yml — targeted ANALYZE on mart models
models:
  data_platform:
    mart:
      +post-hook:
        - "ANALYZE {{ this }} WITH (columns = ARRAY['customer_id', 'order_date', 'region', 'status'])"
```

```sql
-- Model-specific post-hook with all columns
{{
  config(
    post_hook = "ANALYZE {{ this }}"
  )
}}
```

---

## Small File Prevention in Incremental Models

Frequent small incremental runs create many tiny Parquet files, degrading read performance.

```sql
-- Strategy 1: Run optimize as a post-hook (when run is infrequent, e.g. hourly)
{{
  config(
    materialized = 'incremental',
    post_hook    = [
      "ALTER TABLE {{ this }} EXECUTE optimize(file_size_threshold => '128MB')"
    ]
  )
}}
```

```yaml
# Strategy 2: Run optimize in a separate dbt operation (daily)
# macros/maintenance.sql
{% macro optimize_iceberg_table(relation) %}
  ALTER TABLE {{ relation }} EXECUTE optimize(file_size_threshold => '128MB')
{% endmacro %}
```

```bash
# Run maintenance operation separately in Airflow
dbt run-operation optimize_iceberg_table --args '{relation: iceberg.silver.events}'
```

**Guideline**: if a model runs > 4× per hour, add a separate daily compaction job rather than post-hook.

---

## Model Testing for Performance Regression

```yaml
# models/mart/schema.yml
models:
  - name: fact_orders
    tests:
      - dbt_utils.expression_is_true:
          expression: "order_date >= DATE '2020-01-01'"
          name: valid_date_range
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1000000
          max_value: 10000000000
```

---

## Anti-Patterns

1. **Ephemeral chain depth > 3** — creates a single monster SQL query that's hard to optimize; materialize at natural breaking points.
2. **Unbounded `is_incremental()` watermark** — `WHERE ts > (SELECT MAX(ts) FROM {{ this }})` causes a full scan of target table on every run; bound with absolute floor date.
3. **`merge` on Iceberg tables with heavy deletes** — each MERGE cycle adds equality delete files; without regular `OPTIMIZE`, read performance degrades; run compaction daily.
4. **`threads: 1` in all environments** — single-threaded dbt runs models sequentially; set threads to min(workers × 2, 16) for faster pipeline execution.
5. **No session properties in profiles.yml** — default `query_max_run_time=100d` is fine, but missing `exchange_compression_codec` and `spill_enabled` leaves performance on the table for heavy models.
6. **Not pinning `full_refresh: false` on critical large tables** — a developer accidentally running `dbt run --full-refresh` on a 1TB fact table can cause a multi-hour outage.

---

## References

- dbt-trino materializations: `docs.getdbt.com/reference/resource-configs/trino-configs`
- dbt incremental models: `docs.getdbt.com/docs/build/incremental-models`
- Related skills: `[[trino-dbt-platform]]`, `[[trino-iceberg-best-practices]]`, `[[trino-query-optimization]]`, `[[trino-airflow-lakehouse-pipelines]]`
