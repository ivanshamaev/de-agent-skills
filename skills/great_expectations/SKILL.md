---
name: great-expectations
description: Great Expectations (GX) — DataContext setup, Data Sources (Pandas/Spark/SQL/file), Expectation Suites, built-in expectations (null/uniqueness/range/set/regex/table-level/statistical), Validation Definitions, Checkpoints, Data Docs, Airflow integration, custom expectations, dbt integration, CI/CD usage, severity levels (warning/critical)
---

# Great Expectations (GX)

## When to Use

Load this skill when the user needs to:
- Validate data quality in pipelines (null checks, range checks, uniqueness, schema)
- Build Expectation Suites for tables or DataFrames
- Set up Checkpoints that run validations and route results
- Integrate GX with Airflow, dbt, Spark, or CI/CD pipelines
- Generate Data Docs for data quality reporting
- Write custom Expectations

---

## Core Concepts

```
DataContext          — entrypoint; manages config, stores, connections
  └─ DataSource      — connection to a data backend (Pandas, Spark, Postgres, etc.)
       └─ DataAsset  — logical dataset (table, DataFrame, file pattern)
            └─ BatchDefinition — how to slice the asset (whole / partition by date)
                 └─ Batch      — actual data sample to validate

ExpectationSuite     — named collection of Expectation objects
ValidationDefinition — links (BatchDefinition → ExpectationSuite)
Checkpoint           — runs one or more ValidationDefinitions; routes results to actions
  └─ Actions: store results, update Data Docs, send Slack/email alerts

ValidationResult     — pass/fail per expectation with metrics (unexpected %, samples)
```

---

## Installation

```bash
pip install great_expectations                        # base
pip install great_expectations[spark]                # + PySpark support
pip install great_expectations[sqlalchemy]           # + SQL (PostgreSQL, MySQL, etc.)
pip install great_expectations[trino]                # + Trino/Starburst
```

---

## DataContext Setup

```python
import great_expectations as gx

# File-based context (persists config + results to disk)
context = gx.get_context(mode="file", project_root_dir="/opt/gx")

# Ephemeral context (in-memory, no persistence — useful in notebooks/CI)
context = gx.get_context(mode="ephemeral")

# Cloud context (GX Cloud SaaS) — requires GREAT_EXPECTATIONS_CLOUD_* env vars
context = gx.get_context(mode="cloud")
```

---

## Data Sources

### Pandas (DataFrame)

```python
import pandas as pd
import great_expectations as gx

context = gx.get_context(mode="ephemeral")
df = pd.read_parquet("/data/silver/orders/dt=2024-01-15/part-0.parquet")

data_source = context.data_sources.add_pandas("orders-pandas")
data_asset  = data_source.add_dataframe_asset(name="orders")
batch_def   = data_asset.add_batch_definition_whole_dataframe("full")

batch = batch_def.get_batch(batch_parameters={"dataframe": df})
```

### PySpark (DataFrame)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark_df = spark.table("silver.orders").filter("dt = '2024-01-15'")

data_source = context.data_sources.add_spark("orders-spark")
data_asset  = data_source.add_dataframe_asset(name="orders")
batch_def   = data_asset.add_batch_definition_whole_dataframe("full")

batch = batch_def.get_batch(batch_parameters={"dataframe": spark_df})
```

### SQL (PostgreSQL / Trino / MySQL / SQLite)

```python
# PostgreSQL
ds = context.data_sources.add_postgres(
    name="warehouse",
    connection_string="postgresql+psycopg2://user:pass@host:5432/db",
)

# Trino
ds = context.data_sources.add_sql(
    name="trino",
    connection_string="trino://user@host:8080/catalog/schema",
)

# Add table asset
table_asset = ds.add_table_asset(name="orders", table_name="silver.orders")
batch_def   = table_asset.add_batch_definition_whole_table("full")

# Partitioned by date (runs incrementally)
from great_expectations.datasource.fluent.sql_datasource import ColumnSplitterDaily

daily_batch_def = table_asset.add_batch_definition_daily(
    name="daily",
    column="event_time",
)
batch = daily_batch_def.get_batch(
    batch_parameters={"year": 2024, "month": 1, "day": 15}
)
```

---

## Expectation Suites

```python
suite = context.suites.add(
    gx.core.ExpectationSuite(name="orders-silver-quality")
)

# Add expectations one by one
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="user_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="amount", min_value=0, max_value=1_000_000
))

# Save (for file-based context)
context.suites.update(suite)
```

---

## Built-in Expectations

### Completeness (Null Checks)

```python
gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id")

# Tolerate up to 5% nulls
gx.expectations.ExpectColumnValuesToNotBeNull(
    column="discount_code",
    mostly=0.95,   # 0.0–1.0: fraction that MUST pass (1.0 = 100%)
)
```

### Uniqueness

```python
gx.expectations.ExpectColumnValuesToBeUnique(column="order_id")

# Compound uniqueness (multi-column)
gx.expectations.ExpectMultiColumnSumToEqual(column_list=["order_id", "line_item_id"])

# Distinct count range
gx.expectations.ExpectColumnUniqueValueCountToBeBetween(
    column="status", min_value=1, max_value=10
)
```

### Value Range

```python
gx.expectations.ExpectColumnValuesToBeBetween(
    column="amount", min_value=0, max_value=1_000_000
)

gx.expectations.ExpectColumnMinToBeBetween(column="amount", min_value=0)
gx.expectations.ExpectColumnMaxToBeBetween(column="amount", max_value=1_000_000)
gx.expectations.ExpectColumnMeanToBeBetween(column="amount", min_value=50, max_value=500)
gx.expectations.ExpectColumnMedianToBeBetween(column="amount", min_value=10, max_value=1000)
gx.expectations.ExpectColumnStdevToBeBetween(column="amount", min_value=0, max_value=10_000)
```

### Set Membership

```python
gx.expectations.ExpectColumnValuesToBeInSet(
    column="status",
    value_set=["placed", "shipped", "delivered", "cancelled", "returned"],
)

gx.expectations.ExpectColumnValuesToNotBeInSet(
    column="status",
    value_set=["DELETED", "ERROR"],  # legacy values that shouldn't appear
)

# Categorical distribution check
gx.expectations.ExpectColumnDistinctValuesToBeInSet(
    column="country_code",
    value_set=["RU", "BY", "KZ", "UA"],
)
```

### Pattern / Regex

```python
gx.expectations.ExpectColumnValuesToMatchRegex(
    column="email",
    regex=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
)

gx.expectations.ExpectColumnValuesToNotMatchRegex(
    column="phone",
    regex=r"^\s+$",   # not whitespace-only
)

gx.expectations.ExpectColumnValuesToMatchLikePattern(
    column="order_id",
    like_pattern="ORD-%",   # SQL LIKE pattern
)
```

### Type Checking

```python
gx.expectations.ExpectColumnValuesToBeOfType(column="order_id", type_="int64")
gx.expectations.ExpectColumnValuesToBeInTypeList(
    column="amount", type_list=["float64", "float32", "Decimal"]
)
```

### Table-Level

```python
# Row count
gx.expectations.ExpectTableRowCountToBeBetween(min_value=1000, max_value=10_000_000)
gx.expectations.ExpectTableRowCountToEqual(value=86_400)  # exactly N rows

# Column presence
gx.expectations.ExpectTableColumnsToMatchOrderedList(
    column_list=["order_id", "user_id", "amount", "status", "event_time"]
)
gx.expectations.ExpectTableColumnsToMatchSet(
    column_set={"order_id", "user_id", "amount"},
    exact_match=False,   # allow extra columns
)
gx.expectations.ExpectTableColumnCountToBeBetween(min_value=5, max_value=50)

# Column existence
gx.expectations.ExpectColumnToExist(column="order_id")
```

### Freshness / Recency

```python
import datetime

gx.expectations.ExpectColumnMaxToBeBetween(
    column="event_time",
    min_value=datetime.datetime.utcnow() - datetime.timedelta(hours=2),
    max_value=datetime.datetime.utcnow(),
)
```

### Referential Integrity (Custom / SQL pattern)

```python
# SQL-based: count orphan FK references
gx.expectations.UnexpectedRowsExpectation(
    unexpected_rows_query="""
        SELECT o.order_id
        FROM orders o
        LEFT JOIN customers c ON o.user_id = c.customer_id
        WHERE c.customer_id IS NULL
    """,
)
```

---

## Severity Levels

```python
gx.expectations.ExpectColumnValuesToNotBeNull(
    column="order_id",
    result_format="COMPLETE",
    meta={"severity": "critical"},   # metadata (GX doesn't enforce programmatically)
)

# GX v1.x formal severity
gx.expectations.ExpectColumnValuesToBeBetween(
    column="discount_pct",
    min_value=0,
    max_value=100,
    severity="warning",  # warning | critical (v1.x+)
)
```

---

## Validation Definitions & Checkpoints

```python
import great_expectations as gx

context = gx.get_context(mode="file", project_root_dir="/opt/gx")

# 1. Data source + asset + batch definition
ds = context.data_sources.add_postgres("warehouse", connection_string="postgresql+psycopg2://...")
asset = ds.add_table_asset(name="orders", table_name="silver.orders")
batch_def = asset.add_batch_definition_whole_table("full")

# 2. Expectation suite
suite = context.suites.add(gx.core.ExpectationSuite(name="orders-quality"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"))
suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0))

# 3. Validation definition
val_def = context.validation_definitions.add(
    gx.core.ValidationDefinition(
        name="orders-daily-validation",
        data=batch_def,
        suite=suite,
    )
)

# 4. Checkpoint with actions
checkpoint = context.checkpoints.add(
    gx.checkpoint.Checkpoint(
        name="orders-daily-checkpoint",
        validation_definitions=[val_def],
        actions=[
            gx.checkpoint.UpdateDataDocsAction(name="update_data_docs"),
            gx.checkpoint.SlackNotificationAction(
                name="slack_on_fail",
                slack_webhook="https://hooks.slack.com/services/...",
                notify_on="failure",
                notify_with=["update_data_docs"],
            ),
        ],
        result_format={"result_format": "COMPLETE"},
    )
)

# 5. Run
result = checkpoint.run()
print(result.success)           # True / False
print(result.describe())        # human-readable summary
```

### Failing Fast on Critical Failures

```python
result = checkpoint.run()
if not result.success:
    failed_expectations = [
        v for v in result.run_results.values()
        for r in v["validation_result"].results
        if not r.success
    ]
    raise ValueError(f"Data quality FAILED: {len(failed_expectations)} expectations failed")
```

---

## Airflow Integration

```python
from airflow.sdk import dag, task
import pendulum

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dq"],
)
def orders_dq_pipeline():

    @task
    def run_dq_checkpoint(ds: str) -> bool:
        import great_expectations as gx

        context = gx.get_context(mode="file", project_root_dir="/opt/airflow/gx")
        checkpoint = context.checkpoints.get("orders-daily-checkpoint")

        result = checkpoint.run(
            batch_parameters={"year": int(ds[:4]), "month": int(ds[5:7]), "day": int(ds[8:10])}
        )

        if not result.success:
            # Count critical failures
            failures = sum(
                1 for v in result.run_results.values()
                for r in v["validation_result"].results
                if not r.success
            )
            raise ValueError(f"DQ FAILED: {failures} expectations failed for {ds}")

        return result.success

    @task
    def load_to_gold(dq_passed: bool, ds: str):
        if not dq_passed:
            raise ValueError("Cannot load to Gold: DQ check failed")
        # ... actual load logic ...

    dq_ok = run_dq_checkpoint()
    load_to_gold(dq_ok)

orders_dq_pipeline()
```

---

## dbt Integration

```python
# Run GX after dbt run using dbt artifacts
# great_expectations/checkpoints/post_dbt_checkpoint.py

import great_expectations as gx

context = gx.get_context(mode="file")

# Point to dbt-generated table
ds = context.data_sources.add_postgres("warehouse", connection_string="...")
asset = ds.add_table_asset("orders_mart", table_name="marts.orders")
batch_def = asset.add_batch_definition_whole_table("full")

suite = context.suites.get("orders-mart-quality")
val_def = context.validation_definitions.get("orders-mart-validation")
checkpoint = context.checkpoints.get("orders-mart-checkpoint")

result = checkpoint.run()
assert result.success, f"Post-dbt DQ failed: {result.describe()}"
```

---

## Custom Expectations

```python
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.core import ExpectationConfiguration

class ExpectColumnValuesToBeValidINN(ColumnMapExpectation):
    """Validate Russian INN (ИНН) format: 10 or 12 digits."""

    map_metric = "column_values.match_regex"
    success_keys = ("regex",)

    default_kwarg_values = {
        "regex": r"^\d{10}$|^\d{12}$",
        "mostly": 1.0,
    }

    examples = [
        {
            "data": {"inn": ["7743013908", "123456789012", "invalid"]},
            "tests": [
                {"title": "valid INN", "exact_match_out": False,
                 "include_in_gallery": True,
                 "in": {"column": "inn"},
                 "out": {"success": False, "unexpected_list": ["invalid"]}},
            ],
        }
    ]

    library_metadata = {
        "maturity": "experimental",
        "tags": ["russia", "inn", "tax"],
        "contributors": ["@my-team"],
    }

# Use:
suite.add_expectation(
    ExpectColumnValuesToBeValidINN(column="inn_number")
)
```

---

## Data Docs

```python
# Build HTML Data Docs locally
context.build_data_docs()

# Open in browser (local only)
context.open_data_docs()

# Programmatic path
data_docs_dir = context.root_directory + "/uncommitted/data_docs/local_site/index.html"
```

Data Docs are generated automatically by `UpdateDataDocsAction` in Checkpoint.  
For CI/CD, publish to S3 or internal web server:

```python
# In great_expectations.yml data_docs_sites config:
# s3_site:
#   class_name: SiteBuilder
#   store_backend:
#     class_name: TupleS3StoreBackend
#     bucket: my-gx-docs
#     prefix: data_docs/
```

---

## Common Validation Patterns

### Schema Validation (column set + types)

```python
suite.add_expectation(gx.expectations.ExpectTableColumnsToMatchSet(
    column_set={"order_id", "user_id", "amount", "status", "event_time"},
    exact_match=True,
))
for col, expected_type in [
    ("order_id", "int64"), ("amount", "float64"), ("status", "object")
]:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column=col, type_=expected_type)
    )
```

### Completeness Tier (critical columns vs optional)

```python
CRITICAL_COLS = ["order_id", "user_id", "amount", "event_time"]
OPTIONAL_COLS = ["discount_code", "promo_id", "referrer"]

for col in CRITICAL_COLS:
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column=col, meta={"severity": "critical"}
    ))
for col in OPTIONAL_COLS:
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column=col, mostly=0.5, meta={"severity": "warning"}  # 50% fill rate OK
    ))
```

### Volume Anomaly Detection

```python
# Expect daily row count within historical range
suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(
    min_value=50_000,    # P5 of historical daily volume
    max_value=500_000,   # P95 of historical daily volume
))
```

---

## Best Practices

1. **Validate at every layer boundary**: bronze→silver (schema + nulls), silver→gold (business rules + volume).
2. **Use `mostly` for non-critical columns** — enforcing 100% on optional fields generates noise; set `mostly=0.95`.
3. **Separate suites by severity**: `critical-checks` suite gates the pipeline; `warning-checks` suite logs but doesn't fail.
4. **Store suites in version control** — export as JSON: `context.suites.get("name").save()`.
5. **Use `ExpectTableRowCountToBeBetween`** on every table — row count anomalies are the cheapest signal of upstream issues.
6. **Run GX inside `foreachBatch`** for streaming pipelines — validate each micro-batch before writing to sink.
7. **Use `result_format="COMPLETE"`** in development to see unexpected values; use `"SUMMARY"` in production for performance.
8. **Publish Data Docs to shared storage** (S3/MinIO) so data consumers and stakeholders can inspect quality reports.
9. **Add freshness check** (`ExpectColumnMaxToBeBetween` on timestamp column) — detects stalled pipelines.
10. **Integrate with dbt `dbt-expectations` package** for in-model tests instead of separate validation layer when already using dbt.

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| No row count check | Pipeline silently produces 0 rows | Always add `ExpectTableRowCountToBeBetween(min_value=1)` |
| `mostly=1.0` on all nullable columns | Too strict; fails on legitimate sparse data | Use `mostly=0.95` for known-sparse columns |
| Running GX on full history every time | Slow and expensive on large tables | Use `BatchDefinition` with date partitioning |
| No action list in Checkpoint | Failures are silent — no alerts, no Data Docs | Add `UpdateDataDocsAction` + alert action |
| Storing expectations only in notebook | No reproducibility, no CI/CD | Persist to file-based context, commit to git |
| Validating after writing to prod | Data already committed when failure detected | Validate staging data BEFORE writing to prod |
| One giant suite for everything | Hard to understand which check failed and why | Split by domain: `schema`, `completeness`, `business_rules` |
| Ignoring `unexpected_count` in results | "Success" with 0.1% unexpected values silently accepted | Log and alert on unexpected_count even when `mostly` threshold passes |

---

## References to Consult When Needed

- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [GX Core Quickstart](https://docs.greatexpectations.io/docs/core/introduction/try_gx)
- [Expectations Gallery](https://greatexpectations.io/expectations/)
- [GX Airflow Integration](https://docs.greatexpectations.io/docs/oss/integrations/integration_airflow)
- [dbt-expectations package](https://github.com/calogica/dbt-expectations)
