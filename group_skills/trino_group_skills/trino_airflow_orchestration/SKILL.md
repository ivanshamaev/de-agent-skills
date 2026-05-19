---
name: trino-airflow-orchestration
description: Airflow orchestration for Trino pipelines — TrinoOperator and TrinoHook usage, trino_conn_id connection setup, idempotent DAG patterns (INSERT INTO / MERGE with deterministic run_ids), partition-aware scheduling with Airflow logical_date, sensor patterns (TrinoCheckOperator), SLA monitoring, retry configuration for Trino queries, incremental load orchestration, dbt + Trino Airflow integration (BashOperator/DbtRunOperator), metadata-driven DAG generation
---

# Airflow Orchestration for Trino Pipelines

## When to Use

- Scheduling Trino SQL pipelines with dependencies and retry logic
- Building partition-aware incremental loads triggered by Airflow
- Orchestrating dbt runs that target Trino
- Implementing SLA monitoring for Trino-powered data pipelines
- Creating metadata-driven DAG patterns for large numbers of similar Trino jobs

---

## Connection Setup

```python
# Airflow Connection (via UI or environment variable)
# Conn ID: trino_default
# Conn Type: Trino
# Host: trino-coordinator.internal
# Port: 8080
# Login: airflow_svc
# Password: (if LDAP)
# Extra: {"auth": "basic", "http_scheme": "http"}
```

```bash
# Or set via environment variable
export AIRFLOW_CONN_TRINO_DEFAULT='trino://airflow_svc:password@trino-coordinator.internal:8080/iceberg'
```

```python
# Or programmatically
from airflow.models import Connection
from airflow import settings

conn = Connection(
    conn_id   = 'trino_default',
    conn_type = 'trino',
    host      = 'trino-coordinator.internal',
    port      = 8080,
    login     = 'airflow_svc',
    schema    = 'iceberg',
    extra     = '{"auth": "basic", "http_scheme": "http"}'
)
session = settings.Session()
session.add(conn)
session.commit()
```

---

## TrinoOperator: Basic Usage

```python
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime, timedelta

with DAG(
    dag_id            = 'trino_daily_agg',
    start_date        = datetime(2024, 1, 1),
    schedule_interval = '@daily',
    catchup           = False,
    default_args      = {
        'retries':         3,
        'retry_delay':     timedelta(minutes=5),
        'retry_exponential_backoff': True,
    },
    tags = ['trino', 'gold'],
) as dag:

    refresh_daily_revenue = TrinoOperator(
        task_id     = 'refresh_daily_revenue',
        trino_conn_id = 'trino_default',
        sql = """
            INSERT INTO iceberg.gold.daily_revenue
            SELECT
                order_date,
                region,
                COUNT(*)        AS order_count,
                SUM(amount)     AS gross_revenue
            FROM iceberg.silver.orders
            WHERE order_date = DATE '{{ ds }}'
              AND order_date NOT IN (
                  SELECT order_date FROM iceberg.gold.daily_revenue
                  WHERE order_date = DATE '{{ ds }}'
              )
            GROUP BY order_date, region
        """,
        handler = list,   # fetch results (small result sets only)
    )
```

---

## Idempotent Trino DAGs

Use MERGE or conditional INSERT to make DAG runs safe to retry:

```python
# Pattern 1: DELETE + INSERT (idempotent partition refresh)
delete_partition = TrinoOperator(
    task_id    = 'delete_partition',
    trino_conn_id = 'trino_default',
    sql = """
        DELETE FROM iceberg.gold.daily_revenue
        WHERE order_date = DATE '{{ ds }}'
    """,
)

insert_partition = TrinoOperator(
    task_id    = 'insert_partition',
    trino_conn_id = 'trino_default',
    sql = """
        INSERT INTO iceberg.gold.daily_revenue
        SELECT order_date, region, COUNT(*), SUM(amount)
        FROM iceberg.silver.orders
        WHERE order_date = DATE '{{ ds }}'
        GROUP BY order_date, region
    """,
)

delete_partition >> insert_partition
```

```python
# Pattern 2: MERGE upsert (idempotent by primary key)
upsert_customers = TrinoOperator(
    task_id    = 'upsert_customers',
    trino_conn_id = 'trino_default',
    sql = """
        MERGE INTO iceberg.silver.customers t
        USING (
            SELECT customer_id, name, email, region, tier, updated_at
            FROM iceberg.bronze.customers_raw
            WHERE ingested_date = DATE '{{ ds }}'
        ) s ON t.customer_id = s.customer_id
        WHEN MATCHED AND s.updated_at > t.updated_at THEN
            UPDATE SET name = s.name, email = s.email, region = s.region,
                       tier = s.tier, updated_at = s.updated_at
        WHEN NOT MATCHED THEN
            INSERT (customer_id, name, email, region, tier, updated_at)
            VALUES (s.customer_id, s.name, s.email, s.region, s.tier, s.updated_at)
    """,
)
```

---

## TrinoHook: Dynamic SQL with Python

```python
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task

@task
def analyze_table(table_name: str) -> dict:
    hook = TrinoHook(trino_conn_id='trino_default')
    
    # Run ANALYZE
    hook.run(f"ANALYZE iceberg.silver.{table_name}")
    
    # Query metadata
    rows = hook.get_records(f"""
        SELECT partition, record_count, file_count,
               ROUND(total_size / 1024.0 / 1024 / 1024, 2) AS total_gb
        FROM iceberg.silver."{table_name}$partitions"
        ORDER BY file_count DESC
        LIMIT 5
    """)
    
    return {"table": table_name, "top_partitions": rows}


@task
def compact_small_partitions(table_name: str, threshold_files: int = 100) -> int:
    hook = TrinoHook(trino_conn_id='trino_default')
    
    # Find partitions with too many files
    small_file_partitions = hook.get_records(f"""
        SELECT partition
        FROM iceberg.silver."{table_name}$partitions"
        WHERE file_count > {threshold_files}
        ORDER BY file_count DESC
        LIMIT 10
    """)
    
    compacted = 0
    for (partition,) in small_file_partitions:
        hook.run(f"""
            ALTER TABLE iceberg.silver.{table_name}
            EXECUTE optimize(file_size_threshold => '128MB')
            WHERE {partition}
        """)
        compacted += 1
    
    return compacted
```

---

## Partition-Aware Incremental Load DAG

```python
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task, dag
from datetime import datetime, timedelta

@dag(
    dag_id            = 'trino_incremental_orders',
    start_date        = datetime(2024, 1, 1),
    schedule_interval = '0 2 * * *',   # 2 AM daily
    catchup           = True,           # backfill missed runs
    max_active_runs   = 3,
    default_args      = {'retries': 2, 'retry_delay': timedelta(minutes=10)},
    tags              = ['trino', 'incremental', 'silver'],
)
def trino_incremental_pipeline():

    @task
    def check_source_freshness(ds: str, **context) -> bool:
        hook = TrinoHook(trino_conn_id='trino_default')
        count = hook.get_first(f"""
            SELECT COUNT(*) FROM iceberg.bronze.orders_raw
            WHERE ingested_date = DATE '{ds}'
        """)[0]
        if count == 0:
            raise ValueError(f"No source data for {ds}, count={count}")
        return True

    load_silver_orders = TrinoOperator(
        task_id       = 'load_silver_orders',
        trino_conn_id = 'trino_default',
        sql = """
            MERGE INTO iceberg.silver.orders t
            USING (
                SELECT order_id, customer_id, order_date, status, amount, region, updated_at
                FROM iceberg.bronze.orders_raw
                WHERE ingested_date = DATE '{{ ds }}'
            ) s ON t.order_id = s.order_id
            WHEN MATCHED AND s.updated_at > t.updated_at THEN
                UPDATE SET status = s.status, amount = s.amount, updated_at = s.updated_at
            WHEN NOT MATCHED THEN
                INSERT VALUES (s.order_id, s.customer_id, s.order_date, s.status, s.amount, s.region, s.updated_at)
        """,
    )

    run_analyze = TrinoOperator(
        task_id       = 'run_analyze',
        trino_conn_id = 'trino_default',
        sql           = "ANALYZE iceberg.silver.orders WITH (columns = ARRAY['customer_id', 'order_date', 'status'])",
    )

    check_source_freshness() >> load_silver_orders >> run_analyze

trino_incremental_pipeline()
```

---

## dbt + Trino Integration

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_PROJECT_DIR = '/opt/dbt/data_platform'
DBT_PROFILES_DIR = '/opt/dbt/profiles'

with DAG(
    dag_id            = 'dbt_trino_daily',
    start_date        = datetime(2024, 1, 1),
    schedule_interval = '0 4 * * *',
    catchup           = False,
    tags              = ['dbt', 'trino'],
) as dag:

    dbt_run_staging = BashOperator(
        task_id = 'dbt_run_staging',
        bash_command = f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run \
              --profiles-dir {DBT_PROFILES_DIR} \
              --target prod \
              --select staging \
              --threads 8 \
              --vars '{{{{ execution_date: "{{{{ ds }}}}" }}}}'
        """,
    )

    dbt_run_mart = BashOperator(
        task_id = 'dbt_run_mart',
        bash_command = f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run \
              --profiles-dir {DBT_PROFILES_DIR} \
              --target prod \
              --select mart \
              --threads 16
        """,
    )

    dbt_test = BashOperator(
        task_id = 'dbt_test',
        bash_command = f"""
            cd {DBT_PROJECT_DIR} && \
            dbt test \
              --profiles-dir {DBT_PROFILES_DIR} \
              --target prod \
              --select mart \
              --threads 8
        """,
    )

    dbt_run_staging >> dbt_run_mart >> dbt_test
```

---

## SLA Monitoring

```python
from airflow.models import DAG
import logging

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logging.error(
        f"SLA missed! DAG: {dag.dag_id}, "
        f"Tasks: {[t.task_id for t in task_list]}, "
        f"SLAs: {slas}"
    )
    # Send alert to PagerDuty / Slack here

with DAG(
    dag_id        = 'trino_critical_pipeline',
    sla_miss_callback = sla_miss_callback,
    ...
) as dag:

    load_task = TrinoOperator(
        task_id = 'load_gold_metrics',
        trino_conn_id = 'trino_default',
        sla = timedelta(hours=2),   # task must complete within 2h of DAG start
        sql = "INSERT INTO iceberg.gold.metrics SELECT ...",
    )
```

---

## Anti-Patterns

1. **Non-idempotent INSERT without DELETE guard** — `INSERT INTO ... SELECT ...` run twice creates duplicate rows in Iceberg; always use MERGE or DELETE+INSERT pattern.
2. **`catchup=True` with expensive full-table Trino queries** — when backfilling 6 months of a slow query, Airflow will run many parallel executions overwhelming Trino; set `max_active_runs=1` and bound queries to `{{ ds }}`.
3. **TrinoOperator with `handler=list` on large result sets** — fetching millions of rows into Airflow worker memory OOMs the worker; use `handler=None` for DDL/DML and avoid SELECT results in TrinoOperator.
4. **Hard-coded schema names** — use `{{ var.value.trino_schema }}` or environment variables; hard-coded names break multi-environment pipelines.
5. **No retries on Trino operators** — transient Trino failures (worker restart, OOM) are recoverable; always set `retries=2` with exponential backoff.

---

## References

- Trino Airflow provider: `airflow.apache.org/docs/apache-airflow-providers-trino/`
- TrinoOperator: `airflow.apache.org/docs/apache-airflow-providers-trino/stable/operators/trino.html`
- Related skills: `[[trino-airflow-lakehouse-pipelines]]`, `[[trino-dbt-platform]]`, `[[trino-iceberg-best-practices]]`
