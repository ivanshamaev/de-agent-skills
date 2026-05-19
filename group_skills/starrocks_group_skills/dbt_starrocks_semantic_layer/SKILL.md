---
name: dbt-starrocks-semantic-layer
description: dbt + StarRocks semantic layer — dbt Semantic Layer (MetricFlow) on StarRocks, metric definitions (simple/ratio/cumulative/derived), dimension definitions, saved queries, semantic model DDL, exposures for BI tool documentation, dbt-metricflow query CLI, linking metrics to StarRocks materialized views for performance, business KPI governance patterns
---

# dbt + StarRocks Semantic Layer

## When to Use

- Define business metrics (revenue, conversion, churn) once, use everywhere
- Serve consistent KPIs to BI tools (Tableau, Superset, Looker, Grafana)
- Create a semantic layer on top of StarRocks facts and dimensions
- Generate time-series metrics with automatic period-over-period comparison
- Enforce metric definitions so different teams don't compute revenue differently

---

## Architecture

```
StarRocks Tables (Physical Layer)
  ├── silver.orders     (fact)
  ├── silver.dim_customers (dimension)
  └── gold.orders_daily (pre-aggregated for performance)
            │
dbt Semantic Layer (Logic Layer)
  ├── semantic_models/  (entity/measure/dimension definitions)
  ├── metrics/          (business metric definitions)
  └── saved_queries/    (pre-defined report queries)
            │
BI Tools (Consumption Layer)
  ├── dbt Cloud Explore
  ├── Tableau / Superset (via dbt Semantic Layer API)
  └── MetricFlow CLI     (for testing)
```

---

## Installation

```bash
pip install dbt-metricflow
# MetricFlow is included in dbt-core >= 1.6.0
```

```yaml
# packages.yml (if using dbt_semantic_interfaces)
packages:
  - package: dbt-labs/dbt_semantic_interfaces
    version: [">=0.5.0"]
```

---

## Semantic Model Definition

A semantic model maps a physical table to entities, dimensions, and measures.

```yaml
# models/semantic_models/orders_semantic.yml
semantic_models:
  - name: orders
    description: "Order-level fact table"
    model: ref('orders')  # physical StarRocks table

    entities:
      - name: order
        type: primary
        expr: order_id
      - name: customer
        type: foreign
        expr: customer_id

    dimensions:
      - name: created_at
        type: time
        expr: created_at
        type_params:
          time_granularity: day
      - name: status
        type: categorical
        expr: status
      - name: region
        type: categorical
        expr: region

    measures:
      - name: order_count
        description: "Number of orders"
        agg: count
        expr: order_id
      - name: revenue
        description: "Total order revenue"
        agg: sum
        expr: amount
      - name: avg_order_value
        description: "Average order value"
        agg: average
        expr: amount
      - name: unique_customers
        description: "Number of unique customers"
        agg: count_distinct
        expr: customer_id
```

---

## Metric Definitions

```yaml
# models/metrics/order_metrics.yml
metrics:
  # Simple metric
  - name: total_revenue
    description: "Total revenue from confirmed orders"
    type: simple
    type_params:
      measure: revenue
    filter: |
      {{ Dimension('order__status') }} != 'cancelled'

  # Ratio metric
  - name: average_order_value
    description: "Average order value (revenue / orders)"
    type: ratio
    type_params:
      numerator: revenue
      denominator: order_count
    filter: |
      {{ Dimension('order__status') }} != 'cancelled'

  # Derived metric (compound formula)
  - name: revenue_per_customer
    description: "Revenue divided by unique customers"
    type: derived
    type_params:
      expr: "revenue / unique_customers"
      metrics:
        - name: revenue
        - name: unique_customers

  # Cumulative metric (running total)
  - name: cumulative_revenue_mtd
    description: "Month-to-date cumulative revenue"
    type: cumulative
    type_params:
      measure: revenue
      window: 1 month

  # Period-over-period metric
  - name: revenue_wow
    description: "Revenue week-over-week growth"
    type: derived
    type_params:
      expr: "revenue - revenue_last_week"
      metrics:
        - name: revenue
        - name: revenue
          alias: revenue_last_week
          offset_window: 1 week
```

---

## Dimension Model with Entities

```yaml
# models/semantic_models/customers_semantic.yml
semantic_models:
  - name: customers
    model: ref('dim_customers')

    entities:
      - name: customer
        type: primary
        expr: customer_id

    dimensions:
      - name: region
        type: categorical
        expr: region
      - name: tier
        type: categorical
        expr: tier
      - name: signup_date
        type: time
        expr: signup_date
        type_params:
          time_granularity: day
```

---

## Saved Queries

Pre-define common report queries for BI tools:

```yaml
# models/saved_queries/weekly_revenue.yml
saved_queries:
  - name: weekly_revenue_by_region
    description: "Weekly revenue breakdown by region"
    query_params:
      metrics:
        - total_revenue
        - order_count
        - average_order_value
      group_by:
        - TimeDimension('order__created_at', 'week')
        - Dimension('order__region')
      where:
        - "{{ TimeDimension('order__created_at', 'week') }} >= dateadd(week, -12, current_date)"

  - name: customer_revenue_summary
    description: "Revenue and orders per customer segment"
    query_params:
      metrics:
        - total_revenue
        - unique_customers
        - revenue_per_customer
      group_by:
        - Dimension('customer__tier')
        - Dimension('order__region')
```

---

## MetricFlow CLI Queries

```bash
# List all metrics
mf list metrics

# List all dimensions available with a metric
mf list dimensions --metrics total_revenue

# Query a metric
mf query \
  --metrics total_revenue \
  --group-by order__created_at__week,order__region \
  --where "order__status != 'cancelled'" \
  --start-time 2024-01-01 \
  --end-time 2024-03-31

# Query with multiple metrics
mf query \
  --metrics total_revenue,order_count,average_order_value \
  --group-by order__created_at__day \
  --order order__created_at__day \
  --limit 30

# Explain the generated SQL (to verify StarRocks query)
mf query \
  --metrics total_revenue \
  --group-by order__created_at__day \
  --explain
```

---

## Linking Metrics to StarRocks MVs for Performance

For high-frequency metric queries, use a pre-aggregated MV as the semantic model source:

```yaml
# semantic_models/orders_daily_semantic.yml
# Point semantic model at the pre-aggregated Gold table
semantic_models:
  - name: orders_daily
    model: ref('orders_daily')       # StarRocks Aggregate Key table

    entities:
      - name: order_daily
        type: primary
        expr: "CONCAT(dt, '_', customer_id)"

    dimensions:
      - name: dt
        type: time
        expr: dt
        type_params:
          time_granularity: day
      - name: region
        type: categorical
        expr: region

    measures:
      - name: revenue
        agg: sum
        expr: total_revenue           # already aggregated
      - name: order_count
        agg: sum
        expr: order_count
```

This directs MetricFlow to query the pre-aggregated table instead of the raw orders table — orders of magnitude faster.

---

## Exposures for BI Documentation

Document which BI dashboards depend on which dbt models:

```yaml
# models/exposures.yml
exposures:
  - name: revenue_dashboard
    description: "Main revenue Grafana dashboard"
    type: dashboard
    maturity: production
    url: "https://grafana.internal/d/revenue-main"
    owner:
      name: "Data Team"
      email: "data@company.com"
    depends_on:
      - ref('orders_daily')
      - metric('total_revenue')
      - metric('average_order_value')

  - name: customer_360_report
    description: "Customer 360 view in Tableau"
    type: analysis
    maturity: production
    depends_on:
      - ref('dim_customers')
      - ref('orders')
      - metric('unique_customers')
      - metric('revenue_per_customer')
```

---

## Project Layout

```
models/
  semantic_models/
    orders_semantic.yml
    customers_semantic.yml
    products_semantic.yml
  metrics/
    order_metrics.yml
    customer_metrics.yml
  saved_queries/
    weekly_revenue.yml
    customer_cohorts.yml
  exposures.yml
```

---

## Anti-Patterns

1. **Defining the same metric in multiple places** — e.g., "revenue" in both a Grafana dashboard SQL and a dbt metric. Semantic Layer exists to avoid this; route all KPIs through dbt metrics.
2. **Pointing semantic models at Bronze tables** — MetricFlow will scan raw immutable data; always point at Silver/Gold cleansed tables.
3. **Ignoring `filter` on metrics** — `revenue` without `status != 'cancelled'` includes cancelled orders; add business filters at metric definition.
4. **Not testing metric SQL with `mf query --explain`** — MetricFlow generates SQL that may not be optimal for StarRocks (e.g., missing partition filters); always inspect the generated SQL.
5. **Complex derived metrics without pre-aggregation** — MetricFlow will join large tables at query time; pre-aggregate in the semantic model's source table.
6. **Exposures without `url`** — makes the dependency graph less useful; always link to the actual BI URL.

---

## References

- dbt MetricFlow: `docs.getdbt.com/docs/build/about-metricflow`
- MetricFlow CLI: `docs.getdbt.com/docs/build/metricflow-cli`
- Semantic models: `docs.getdbt.com/docs/build/semantic-models`
- Related skills: `[[dbt-starrocks-models]]`, `[[dbt-starrocks-performance]]`, `[[starrocks-data-modeling]]`, `[[starrocks-materialized-views]]`
