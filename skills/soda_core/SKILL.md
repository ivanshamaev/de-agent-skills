---
name: soda-core
description: Soda Core data quality — SodaCL checks (row_count, missing, invalid, duplicate, freshness, schema, reference, custom SQL), configuration.yml for PostgreSQL/Spark/ClickHouse/BigQuery, soda scan CLI, Airflow integration, dbt integration, alerting
---

# Soda Core Data Quality

## When to Use

Activate this skill when the task involves:
- Writing SodaCL checks for data quality validation (nulls, ranges, freshness, schema, uniqueness)
- Setting up Soda Core configuration for PostgreSQL, Spark, ClickHouse, Trino, or BigQuery
- Running `soda scan` from CLI or within Airflow DAGs
- Designing a data quality gate pattern: scan → fail pipeline on violation
- Integrating Soda with dbt (supplementing or replacing dbt tests)
- Writing custom SQL metric checks
- Configuring warn vs. fail thresholds

---

## Core Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│   checks.yml          configuration.yml                     │
│   (SodaCL checks)     (data source connection)              │
│        ↓                      ↓                             │
│   ┌─────────────────────────────────┐                       │
│   │        soda scan                │                       │
│   │  translates SodaCL → SQL        │                       │
│   │  runs against data source       │                       │
│   │  returns: pass / warn / fail / error │                  │
│   └───────────────┬─────────────────┘                       │
│                   ↓                                          │
│   ┌───────────────────────────────┐                         │
│   │  Results                      │                         │
│   │  • stdout + logs              │                         │
│   │  • Soda Cloud (optional SaaS) │                         │
│   │  • Airflow task state         │                         │
│   └───────────────────────────────┘                         │
└──────────────────────────────────────────────────────────────┘
```

---

## Installation

```bash
# Core + specific adapter
pip install soda-core-postgres        # PostgreSQL
pip install soda-core-spark-df        # Spark DataFrames
pip install soda-core-trino           # Trino
pip install soda-core-bigquery        # BigQuery
pip install soda-core-clickhouse      # ClickHouse
pip install soda-core-duckdb          # DuckDB (dev/testing)
pip install soda-core-sqlserver       # SQL Server
```

---

## Configuration File (configuration.yml)

### PostgreSQL

```yaml
# soda/configuration.yml
data_source postgres_prod:
  type: postgres
  host: ${POSTGRES_HOST}
  port: "5432"
  username: ${POSTGRES_USER}
  password: ${POSTGRES_PASSWORD}
  database: analytics
  schema: silver
```

### Spark (via DataFrame connector)

```yaml
data_source spark_local:
  type: spark_df
  # SparkSession is passed programmatically — no host config here
```

### Trino

```yaml
data_source trino_prod:
  type: trino
  host: trino.internal
  port: 8080
  username: ${TRINO_USER}
  auth:
    type: kerberos
  catalog: iceberg
  schema: silver
  http_scheme: https
```

### ClickHouse

```yaml
data_source clickhouse_prod:
  type: clickhouse
  host: clickhouse.internal
  port: 8123
  username: ${CH_USER}
  password: ${CH_PASSWORD}
  database: silver
```

### BigQuery

```yaml
data_source bigquery_prod:
  type: bigquery
  account_info_json: ${BIGQUERY_CREDENTIALS}
  auth_scopes:
    - https://www.googleapis.com/auth/bigquery
  project_id: my-gcp-project
  dataset: silver
```

Test connection:
```bash
soda test-connection -d postgres_prod -c soda/configuration.yml
```

---

## SodaCL Check Syntax

### File Structure

```yaml
# soda/checks/silver_orders.yml
checks for orders:             # "for <table_name>"
  - row_count > 0
  - missing_count(order_id) = 0
  - duplicate_count(order_id) = 0
  - missing_percent(customer_id) < 1%
```

Multiple tables in one file:
```yaml
checks for orders:
  - row_count > 0

checks for customers:
  - missing_count(email) = 0
```

---

### Row Count

```yaml
checks for orders:
  - row_count > 0                           # at least 1 row
  - row_count > 1000                        # volume check
  - row_count between 10000 and 10000000    # range
```

### Missing Values

```yaml
checks for orders:
  - missing_count(order_id) = 0            # no nulls
  - missing_count(customer_id) = 0
  - missing_percent(discount) < 5%         # allow up to 5% null discounts
  - missing_count(notes):                  # custom missing definition
      missing values: ["N/A", "n/a", ""]
      fail: when > 0
```

### Duplicate / Uniqueness

```yaml
checks for orders:
  - duplicate_count(order_id) = 0          # strict uniqueness
  - duplicate_count(order_id, order_date) = 0   # composite key
```

### Numeric Range / Statistics

```yaml
checks for order_items:
  - min(quantity) >= 1                     # no zero or negative quantities
  - max(price) <= 99999.99
  - avg(discount_percent) between 0 and 50
  - sum(total_amount) > 0
  - stddev(price) < 1000                  # sanity check on variance
  - invalid_percent(price):               # custom invalid definition
      valid min: 0
      valid max: 999999
      fail: when > 1%
```

### Invalid Values

```yaml
checks for orders:
  - invalid_count(status):
      valid values: [pending, processing, shipped, delivered, cancelled]
      fail: when > 0

  - invalid_count(country_code):
      valid format: ISO 3166 alpha-2    # built-in format validator
      fail: when > 0

  - invalid_count(email):
      valid regex: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$
      fail: when > 0

  - invalid_percent(order_date):
      valid min: 2020-01-01
      valid max: today
      fail: when > 0
```

### Freshness

```yaml
checks for orders:
  - freshness(created_at) < 24h         # most recent row is < 24h old
  - freshness(created_at) < 1d          # synonym
  - freshness(updated_at) < 2h:
      warn: when > 1h
      fail: when > 2h
```

### Schema

```yaml
checks for orders:
  - schema:
      fail:
        when required column missing:
          - order_id
          - customer_id
          - total
          - created_at
        when wrong column type:
          order_id: bigint
          total:    numeric
          created_at: timestamp
        when wrong index:         # column order in result set
          order_id: 0
      warn:
        when forbidden column present:
          - password
          - ssn
```

### Referential Integrity

```yaml
# Ensure every order has a valid customer
checks for orders:
  - values in (customer_id) must exist in customers (customer_id)
```

### Custom SQL Metric

```yaml
checks for orders:
  - order_date_after_ship_date = 0:
      order_date_after_ship_date query: |
        SELECT COUNT(*)
        FROM orders
        WHERE order_date > ship_date

  - negative_total_count = 0:
      negative_total_count query: |
        SELECT COUNT(*)
        FROM orders
        WHERE total < 0

  - distinct_statuses > 0:
      distinct_statuses query: |
        SELECT COUNT(DISTINCT status) FROM orders
```

### Warn vs. Fail Thresholds

```yaml
checks for orders:
  - row_count:
      warn: when < 5000      # warn only
      fail: when < 1000      # fail the scan

  - missing_percent(email):
      warn: when between 1% and 5%
      fail: when > 5%

  - freshness(created_at):
      warn: when > 1h
      fail: when > 24h
```

### Variables and Filters

```yaml
filter orders [daily]:
  where: created_at >= CURRENT_DATE - INTERVAL '1 day'
    AND created_at < CURRENT_DATE

checks for orders [daily]:
  - row_count > 500
  - missing_count(order_id) = 0
```

Filters allow running the same checks on subsets (e.g., yesterday's partition).

---

## Running Scans

```bash
# Basic scan
soda scan -d postgres_prod -c soda/configuration.yml soda/checks/

# Scan single checks file
soda scan -d postgres_prod -c soda/configuration.yml soda/checks/silver_orders.yml

# Scan with variable
soda scan -d postgres_prod -c soda/configuration.yml \
  soda/checks/daily_orders.yml \
  -V batch_date=2024-03-15

# Verbose output
soda scan -d postgres_prod -c soda/configuration.yml soda/checks/ -v

# Send results to Soda Cloud
soda scan -d postgres_prod -c soda/configuration.yml soda/checks/ \
  --cloud-api-key-id $SODA_API_KEY_ID \
  --cloud-api-key-secret $SODA_API_KEY_SECRET
```

Exit codes:
| Code | Meaning |
|------|---------|
| `0` | All checks passed |
| `2` | One or more checks warned |
| `3` | One or more checks failed |
| `4` | Scan error (connection failure, SQL error) |

---

## Python Programmatic API

```python
import logging
from soda.scan import Scan

def run_soda_scan(
    data_source_name: str,
    table_name: str,
    checks_yaml: str,
    variables: dict | None = None,
) -> bool:
    """Run a Soda scan programmatically. Returns True if all checks passed."""
    scan = Scan()
    scan.set_verbose(True)
    scan.set_scan_definition_name(f"soda_{table_name}")
    scan.set_data_source_name(data_source_name)

    scan.add_configuration_yaml_file("soda/configuration.yml")
    scan.add_sodacl_yaml_str(checks_yaml)

    if variables:
        for key, value in variables.items():
            scan.add_variables({key: value})

    exit_code = scan.execute()

    # Log all check results
    for check_result in scan.get_scan_results()["checks"]:
        status = check_result["outcome"]
        name   = check_result["name"]
        logging.info(f"[{status.upper()}] {name}")

    if exit_code == 0:
        return True
    elif exit_code == 2:
        logging.warning("Soda scan completed with warnings")
        return True     # treat warn as pass (adjust to your policy)
    else:
        return False    # fail = 3, error = 4

# --- Spark DataFrame scanning ---
from pyspark.sql import SparkSession
from soda.core.scan import Scan as SparkScan

def scan_spark_df(spark: SparkSession, df, table_alias: str, checks_yaml: str) -> bool:
    scan = SparkScan()
    scan.set_data_source_name("spark_local")
    scan.add_spark_session(spark)

    # Register the DataFrame as a temp view
    df.createOrReplaceTempView(table_alias)
    scan.add_sodacl_yaml_str(checks_yaml)
    exit_code = scan.execute()
    return exit_code in (0, 2)
```

---

## Airflow Integration

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os

def run_soda_quality_check(table: str, checks_file: str, data_source: str) -> None:
    """Runs soda scan and raises ValueError on failure to fail the Airflow task."""
    from soda.scan import Scan

    conn = BaseHook.get_connection("postgres_warehouse")

    os.environ["POSTGRES_HOST"]     = conn.host
    os.environ["POSTGRES_USER"]     = conn.login
    os.environ["POSTGRES_PASSWORD"] = conn.password

    scan = Scan()
    scan.set_scan_definition_name(f"airflow_{table}_{datetime.utcnow().date()}")
    scan.set_data_source_name(data_source)
    scan.add_configuration_yaml_file("/opt/airflow/soda/configuration.yml")
    scan.add_sodacl_yaml_file(f"/opt/airflow/soda/checks/{checks_file}")

    exit_code = scan.execute()

    if exit_code not in (0, 2):    # 2=warn is acceptable
        raise ValueError(
            f"Soda data quality check FAILED for {table} "
            f"(exit code {exit_code}). Pipeline halted."
        )

with DAG(
    dag_id="etl_with_quality_gates",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
) as dag:

    transform_orders = PythonOperator(
        task_id="transform_orders",
        python_callable=run_transform,
    )

    check_orders = PythonOperator(
        task_id="check_orders_quality",
        python_callable=run_soda_quality_check,
        op_kwargs={
            "table": "orders",
            "checks_file": "silver_orders.yml",
            "data_source": "postgres_prod",
        },
    )

    load_to_mart = PythonOperator(
        task_id="load_to_mart",
        python_callable=run_load,
    )

    # Gate: load only runs if quality check passes
    transform_orders >> check_orders >> load_to_mart
```

---

## dbt Integration

Soda can ingest dbt test results and push them to Soda Cloud, or run alongside dbt as a complementary quality layer.

### Install

```bash
pip install soda-core-postgres soda-core-dbt
```

### Run dbt Tests Then Ingest Results

```bash
dbt test --target prod
soda ingest dbt --target prod -d postgres_prod -c soda/configuration.yml
```

### Supplement dbt Tests with SodaCL

Pattern: use dbt tests for schema/relationship integrity, use SodaCL for volume anomalies, freshness SLAs, and cross-table referential integrity that dbt can't easily express.

```yaml
# soda/checks/post_dbt_silver_orders.yml
checks for silver.orders:
  - freshness(created_at) < 2h:           # SLA check dbt can't do
      fail: when > 2h
  - row_count > 100000                    # volume anomaly
  - values in (customer_id) must exist in silver.customers (customer_id)
```

---

## Recommended File Layout

```
soda/
├── configuration.yml             # data source connections
└── checks/
    ├── bronze/
    │   ├── raw_orders.yml
    │   └── raw_customers.yml
    ├── silver/
    │   ├── orders.yml
    │   └── customers.yml
    └── gold/
        └── fct_revenue.yml
```

---

## Anti-Patterns

1. **No `warn` threshold — only `fail`** — a single rogue null fails the entire pipeline. Use `warn` for non-critical checks and reserve `fail` for blocking violations (null primary keys, referential integrity).

2. **Scanning massive tables without filters** — running full-table scans on multi-billion-row tables is expensive. Use `filter` blocks to restrict scans to recent partitions.

3. **Hardcoding credentials in configuration.yml** — use environment variables `${VAR}` syntax everywhere; never commit passwords to git.

4. **One giant checks file for all tables** — hard to maintain and slow (scans all tables serially). Organize by layer and table; run per-table in parallel from Airflow.

5. **Treating exit code 2 (warn) as failure in CI** — warns indicate degraded quality, not critical failure. Only exit code 3 and 4 should fail the pipeline.

6. **Not setting `row_count > 0` as a baseline** — empty tables are a common failure mode. Always add a minimum row count check before other checks (they run on an empty table and give misleading results).

7. **Using custom SQL checks for everything** — SodaCL built-in checks (`missing_count`, `duplicate_count`, etc.) are optimized and adapter-agnostic. Only use custom SQL for logic that can't be expressed with built-ins.

8. **No alerting on scan failures** — a failed scan that nobody sees is useless. Route Soda Cloud alerts to Slack/PagerDuty or configure Airflow callbacks on the quality gate task.

---

## References to Consult When Needed

- SodaCL reference: `docs.soda.io/soda-cl/soda-cl-overview.html`
- Built-in metrics: `docs.soda.io/soda-cl/numeric-metrics.html`
- Configuration: `docs.soda.io/soda-core/configuration.html`
- Airflow guide: `docs.astronomer.io/learn/soda-data-quality`
- dbt integration: `docs.soda.io/soda/integrate-dbt.html`
