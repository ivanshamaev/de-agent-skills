---
name: dbt-starrocks-testing
description: dbt + StarRocks testing — generic tests (not_null/unique/accepted_values/relationships), singular tests (custom SQL assertions), source freshness tests (loaded_at_field), StarRocks-specific volume and freshness tests, dbt-expectations integration, test severity (warn vs error), store_failures for debugging failed tests, ANALYZE before test runs, partition-scoped test SQL
---

# dbt + StarRocks Testing

## When to Use

- Validate StarRocks table contents after dbt model runs
- Test referential integrity between StarRocks fact and dimension tables
- Verify freshness of source tables before running downstream models
- Build custom SQL-based assertions for StarRocks-specific checks
- Store test failures as a table for post-run investigation

---

## Generic Tests (schema.yml)

```yaml
# models/silver/schema.yml
version: 2

models:
  - name: orders
    description: "Cleansed orders, Primary Key on order_id"
    columns:
      - name: order_id
        description: "Primary key"
        tests:
          - not_null
          - unique
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id
              severity: warn   # warn, not error — some orphans allowed
      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']
      - name: amount
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 999999
              row_condition: "status != 'cancelled'"

  - name: orders_daily
    description: "Gold daily aggregation"
    columns:
      - name: dt
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: date
      - name: total_revenue
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
```

---

## Source Freshness Tests

```yaml
# models/sources.yml
sources:
  - name: raw
    database: raw_layer
    schema: ingest
    tables:
      - name: orders_raw
        loaded_at_field: ingested_at       # column to check for freshness
        freshness:
          warn_after: {count: 2, period: hour}
          error_after: {count: 6, period: hour}

      - name: customers
        loaded_at_field: updated_at
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}
```

Run freshness check:
```bash
dbt source freshness
```

---

## Singular Tests (Custom SQL Assertions)

Singular tests are SQL files in `tests/` that should return 0 rows when passing.

### Null Rate Test

```sql
-- tests/assert_orders_amount_not_null.sql
-- Fails if null rate for amount exceeds 0.1%
SELECT *
FROM (
    SELECT
        SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS null_rate
    FROM {{ ref('orders') }}
    WHERE dt >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
) t
WHERE null_rate > 0.001
```

### Volume Anomaly Test

```sql
-- tests/assert_orders_daily_volume.sql
-- Fails if today's count is less than 50% of yesterday's
SELECT *
FROM (
    SELECT
        today_cnt,
        yesterday_cnt,
        today_cnt * 1.0 / NULLIF(yesterday_cnt, 0) AS ratio
    FROM (
        SELECT COUNT(*) AS today_cnt     FROM {{ ref('orders_daily') }} WHERE dt = CURDATE()
    ) t1
    CROSS JOIN (
        SELECT COUNT(*) AS yesterday_cnt FROM {{ ref('orders_daily') }} WHERE dt = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
    ) t2
) t
WHERE ratio < 0.5 OR ratio IS NULL
```

### Referential Integrity Test

```sql
-- tests/assert_orders_valid_customers.sql
-- All orders must have a matching customer
SELECT o.order_id, o.customer_id
FROM {{ ref('orders') }} o
LEFT JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
  AND o.dt >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
LIMIT 100
```

### No Future Dates Test

```sql
-- tests/assert_orders_no_future_dates.sql
SELECT order_id, created_at
FROM {{ ref('orders') }}
WHERE created_at > NOW() + INTERVAL 1 HOUR
```

---

## Partition-Scoped Tests

For large tables, scope tests to a recent partition for performance:

```sql
-- tests/assert_orders_daily_not_empty.sql
-- Verify the latest partition is not empty
{{ config(severity='error') }}

SELECT *
FROM (
    SELECT COUNT(*) AS row_count
    FROM {{ ref('orders_daily') }}
    WHERE dt = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
) t
WHERE row_count = 0
```

Use a macro to make partitioned tests reusable:

```sql
-- macros/test_partition_not_empty.sql
{% macro test_partition_not_empty(model, column_name, lookback_days=1) %}
SELECT *
FROM (
    SELECT COUNT(*) AS cnt
    FROM {{ model }}
    WHERE {{ column_name }} = DATE_SUB(CURDATE(), INTERVAL {{ lookback_days }} DAY)
) t
WHERE cnt = 0
{% endmacro %}
```

```yaml
# schema.yml
models:
  - name: orders_daily
    tests:
      - test_partition_not_empty:
          column_name: dt
          lookback_days: 1
```

---

## dbt-expectations Integration

```bash
# packages.yml
packages:
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]
```

Useful expectations for StarRocks:

```yaml
columns:
  - name: amount
    tests:
      - dbt_expectations.expect_column_values_to_be_between:
          min_value: 0.01
          strictly: true
      - dbt_expectations.expect_column_mean_to_be_between:
          min_value: 50
          max_value: 500
          group_by: [region]

  - name: status
    tests:
      - dbt_expectations.expect_column_distinct_count_to_equal:
          value: 5           # exactly 5 distinct status values

models:
  - name: orders_daily
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1000
          max_value: 10000000
```

---

## Store Test Failures

Store failing rows for post-run investigation:

```yaml
# dbt_project.yml
tests:
  +store_failures: true
  +store_failures_as: table    # or "view"
  +schema: dbt_test_failures   # schema to store failure tables
```

Query failures after a run:
```sql
-- Find which orders failed the referential integrity test
SELECT *
FROM dbt_test_failures.not_null_orders_order_id
LIMIT 100;
```

---

## Test Severity Levels

```yaml
- name: customer_id
  tests:
    - not_null:
        severity: error     # fails dbt run (default)
    - relationships:
        to: ref('dim_customers')
        field: customer_id
        severity: warn      # logs warning, doesn't fail run
```

Programmatic severity override:
```bash
# Run tests; treat all failures as warnings (useful in dev)
dbt test --warn-error-options '{"include": "all"}'
```

---

## ANALYZE Before Test Runs

Run ANALYZE before tests to ensure CBO uses fresh statistics (avoids slow test queries):

```yaml
# dbt_project.yml
on-run-start:
  - "ANALYZE TABLE {{ target.database }}.orders WITH ASYNC MODE"
```

Or via pre-hook on specific test models:

```yaml
models:
  - name: orders
    config:
      pre_hook: "ANALYZE TABLE {{ this }} WITH ASYNC MODE"
```

---

## Test Execution in CI

```bash
# Run all tests
dbt test

# Run tests for specific model only
dbt test --select orders

# Run only singular tests
dbt test --select test_type:singular

# Run only generic tests
dbt test --select test_type:generic

# Run tests and downstream tests
dbt test --select +orders+

# Skip freshness in CI (run separately)
dbt test --exclude source:*
dbt source freshness
```

---

## Anti-Patterns

1. **`not_null` + `unique` tests on billion-row Primary Key tables** — StarRocks enforces PK uniqueness; running dbt uniqueness tests is redundant and expensive. Test PK once in staging, not on every run.
2. **Singular tests without LIMIT** — a buggy test that returns millions of rows instead of 0 causes OOM in the FE; always add `LIMIT 1000` to singular tests.
3. **No partition filter in tests** — full table scans in tests defeat performance optimization; scope to recent partitions.
4. **`store_failures=true` in production with high cardinality failures** — storing 10M failed rows creates enormous failure tables; add LIMIT to singular test SQL.
5. **Running `dbt test` without prior `dbt run`** — tests on stale models; always run models before testing in CI.
6. **No freshness tests on sources** — silent source delays cause downstream models to produce stale results; always configure `freshness` on critical sources.

---

## References

- dbt generic tests: `docs.getdbt.com/docs/build/data-tests`
- dbt-expectations: `github.com/calogica/dbt-expectations`
- dbt source freshness: `docs.getdbt.com/docs/build/sources#source-freshness`
- Related skills: `[[dbt-starrocks-models]]`, `[[dbt-core]]`, `[[starrocks-data-quality-guardian]]`, `[[soda-core]]`
