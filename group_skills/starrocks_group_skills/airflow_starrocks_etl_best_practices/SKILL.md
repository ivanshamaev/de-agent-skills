---
name: airflow-starrocks-etl-best-practices
description: Airflow + StarRocks ETL best practices — idempotent DAG design (label strategy/partition overwrite), retry with backoff, SLA callbacks, dynamic partition creation, duplicate prevention with MERGE/INSERT OVERWRITE, DAG-level concurrency controls, dependency ordering, catchup safety, data lineage tagging
---

# Airflow + StarRocks ETL Best Practices

## When to Use

- Designing production-grade Airflow DAGs that write to StarRocks
- Ensuring idempotency so reruns and backfills don't duplicate data
- Handling transient StarRocks FE/BE failures gracefully
- Managing SLA monitoring and alerting for ingestion pipelines
- Enforcing dependency ordering across multi-table loads

---

## Idempotency Patterns

### Pattern 1: INSERT OVERWRITE (Partition Replacement)

Safest for partitioned tables — always replaces the partition atomically:

```python
@task
def overwrite_partition(ds: str = None, db: str = "sales"):
    hook = MySqlHook(mysql_conn_id="starrocks_prod")
    partition_date = ds  # e.g. "2024-01-15"

    # INSERT OVERWRITE atomically replaces partition contents
    hook.run(f"""
        INSERT OVERWRITE orders PARTITION (dt='{partition_date}')
        SELECT order_id, customer_id, amount, status, created_at
        FROM staging.orders_raw
        WHERE DATE(created_at) = '{partition_date}'
          AND amount > 0
    """)
    print(f"Overwrote partition dt={partition_date}")
```

INSERT OVERWRITE is safe to retry any number of times — it replaces, not appends.

### Pattern 2: Broker Load Label Strategy

Labels must be deterministic to ensure idempotent retries:

```python
def make_load_label(table: str, ds: str, attempt: int = 0) -> str:
    """Deterministic label: same inputs always yield same label."""
    base = f"{table}_{ds.replace('-', '')}"
    return base if attempt == 0 else f"{base}_a{attempt}"

@task
def trigger_idempotent_load(ds: str = None, db: str = "sales"):
    hook = MySqlHook(mysql_conn_id="starrocks_prod")
    label = make_load_label("orders", ds)

    # Check existing label state
    rows = hook.get_records(
        f"SHOW LOAD FROM {db} WHERE LABEL = '{label}'"
    )
    if rows:
        state = rows[0][2]
        if state == "FINISHED":
            print(f"Label {label} already FINISHED — idempotent skip")
            return label
        elif state == "CANCELLED":
            # Use attempt=1 label for retry
            label = make_load_label("orders", ds, attempt=1)
            print(f"Previous load CANCELLED, retrying with label {label}")

    hook.run(f"""
        LOAD LABEL {db}.{label} (
            DATA INFILE("s3a://datalake/orders/dt={ds}/*.parquet")
            INTO TABLE orders FORMAT AS "parquet"
        )
        WITH BROKER ("aws.s3.use_instance_profile" = "true", "aws.s3.region" = "us-east-1")
        PROPERTIES ("timeout" = "3600", "max_filter_ratio" = "0.01")
    """)
    return label
```

### Pattern 3: Deduplication with MERGE

For upsert-style loads where source may have duplicates:

```python
@task
def merge_into_target(ds: str = None, db: str = "sales"):
    hook = MySqlHook(mysql_conn_id="starrocks_prod")

    # Stage → dedup → merge
    hook.run(f"""
        INSERT INTO {db}.orders
        SELECT order_id, customer_id, amount, status, updated_at
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
            FROM {db}.orders_staging
            WHERE load_date = '{ds}'
        ) t
        WHERE rn = 1
    """)
```

For Primary Key tables, INSERT automatically upserts — no explicit MERGE needed.

---

## Retry and Backoff

### Task-Level Retry Config

```python
from datetime import timedelta
from airflow.decorators import task

@task(
    retries=3,
    retry_delay=timedelta(minutes=2),
    retry_exponential_backoff=True,  # 2m, 4m, 8m
    max_retry_delay=timedelta(minutes=30),
)
def trigger_broker_load(ds: str = None):
    ...
```

### Retry-Safe Load Trigger

```python
import time

def trigger_with_retry(hook, sql: str, max_retries: int = 3):
    last_exc = None
    for attempt in range(max_retries):
        try:
            hook.run(sql)
            return
        except Exception as e:
            last_exc = e
            if "Label already exists" in str(e):
                print(f"Label exists (idempotent), continuing")
                return
            wait = (2 ** attempt) * 10  # 10s, 20s, 40s
            print(f"Attempt {attempt+1} failed: {e}. Retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"All retries failed: {last_exc}")
```

---

## SLA Monitoring

### SLA Miss Callback

```python
from airflow import DAG
from airflow.utils.email import send_email
from datetime import datetime, timedelta


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    subject = f"[SLA MISS] DAG {dag.dag_id} — {len(slas)} task(s) overdue"
    body = f"""
    DAG: {dag.dag_id}
    Missed tasks: {[s.task_id for s in slas]}
    Blocking tasks: {[t.task_id for t in blocking_task_list]}
    Check Airflow UI for details.
    """
    send_email(
        to=["data-alerts@company.com"],
        subject=subject,
        html_content=f"<pre>{body}</pre>",
    )


with DAG(
    dag_id="starrocks_daily_load",
    schedule="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    sla_miss_callback=sla_miss_callback,
    default_args={
        "sla": timedelta(hours=3),  # All tasks must finish within 3h of schedule
    },
) as dag:
    ...
```

### Freshness Check Post-Load

```python
@task
def check_freshness(table: str, ds: str = None, db: str = "sales", max_delay_hours: int = 4):
    hook = MySqlHook(mysql_conn_id="starrocks_prod")
    rows = hook.get_records(f"""
        SELECT
            MAX(updated_at) AS latest,
            COUNT(*) AS row_count
        FROM {db}.{table}
        WHERE DATE(created_at) = '{ds}'
    """)
    latest, count = rows[0]
    if latest is None:
        raise ValueError(f"No data found in {table} for {ds}")
    print(f"Latest record: {latest}, count: {count}")
```

---

## Dynamic Partition Management

### Automatic Partition Creation

```python
from datetime import datetime, timedelta

@task
def ensure_partitions(
    table: str,
    start_ds: str,
    num_days: int = 7,
    db: str = "sales",
):
    """Pre-create partitions for the next N days."""
    hook = MySqlHook(mysql_conn_id="starrocks_prod")
    start = datetime.strptime(start_ds, "%Y-%m-%d")

    for i in range(num_days):
        day = start + timedelta(days=i)
        p_start = day.strftime("%Y-%m-%d")
        p_end = (day + timedelta(days=1)).strftime("%Y-%m-%d")
        p_name = f"p{day.strftime('%Y%m%d')}"

        try:
            hook.run(f"""
                ALTER TABLE {db}.{table}
                ADD PARTITION {p_name}
                VALUES [("{p_start}"), ("{p_end}"))
            """)
            print(f"Created partition {p_name}")
        except Exception as e:
            if "Duplicate partition name" in str(e) or "already exists" in str(e):
                print(f"Partition {p_name} already exists — skipping")
            else:
                raise
```

### Check Partition Health

```python
@task
def check_partition_exists(table: str, ds: str = None, db: str = "sales") -> bool:
    hook = MySqlHook(mysql_conn_id="starrocks_prod")
    partition_name = f"p{ds.replace('-', '')}"
    rows = hook.get_records(f"SHOW PARTITIONS FROM {db}.{table}")

    partition_names = [r[1] for r in rows]  # PartitionName at index 1
    if partition_name not in partition_names:
        raise ValueError(f"Partition {partition_name} not found in {db}.{table}")
    return True
```

---

## Dependency Ordering

### Multi-Table Load with Dependencies

```python
from airflow.utils.task_group import TaskGroup

@dag(
    dag_id="starrocks_multi_table_load",
    schedule="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
)
def multi_table_load():

    with TaskGroup("dimensions") as dims:
        load_customers = trigger_broker_load.override(task_id="load_customers")(table="customers")
        load_products  = trigger_broker_load.override(task_id="load_products")(table="products")
        wait_customers = wait_for_load.override(task_id="wait_customers")
        wait_products  = wait_for_load.override(task_id="wait_products")

        load_customers >> wait_customers
        load_products  >> wait_products

    with TaskGroup("facts") as facts:
        load_orders = trigger_broker_load.override(task_id="load_orders")(table="orders")
        wait_orders = wait_for_load.override(task_id="wait_orders")
        load_orders >> wait_orders

    analyze = analyze_tables.override(task_id="analyze_all")()
    validate = validate_counts.override(task_id="validate_all")()

    dims >> facts >> analyze >> validate
```

---

## Concurrency Controls

```python
from airflow.models import Pool

# In Airflow UI or CLI: create pool
# airflow pools set starrocks_load_pool 4 "Max concurrent StarRocks loads"

@task(pool="starrocks_load_pool", pool_slots=1)
def trigger_broker_load(**kwargs):
    ...

# Also limit total active DAG runs for load DAGs
dag = DAG(
    max_active_runs=2,          # Max concurrent DAG runs
    max_active_tasks=4,         # Max concurrent tasks across all runs
    concurrency=4,
    ...
)
```

---

## Data Lineage Tagging

Tag Airflow runs with StarRocks load metadata for audit:

```python
from airflow.models import TaskInstance

@task
def tag_lineage(load_result: dict, ds: str = None, **context):
    ti: TaskInstance = context["ti"]
    ti.xcom_push(key="load_bytes",    value=load_result.get("LoadBytes", 0))
    ti.xcom_push(key="loaded_rows",   value=load_result.get("EtlInfo", ""))
    ti.xcom_push(key="load_label",    value=load_result.get("Label", ""))
    ti.xcom_push(key="finish_time",   value=load_result.get("FinishTime", ""))

    # Optional: push to observability store
    hook = MySqlHook(mysql_conn_id="meta_db")
    hook.run("""
        INSERT INTO pipeline_runs
            (dag_id, run_date, table_name, load_label, load_bytes, finish_time)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, parameters=(
        context["dag"].dag_id, ds, "orders",
        load_result.get("Label"), load_result.get("LoadBytes"), load_result.get("FinishTime")
    ))
```

---

## Catchup Safety Checklist

For DAGs with `catchup=True`:

- [ ] `max_active_runs=1` — prevents concurrent backfill runs from racing on same partition
- [ ] Labels are deterministic (`{table}_{ds}`) — safe to re-trigger for any date
- [ ] INSERT OVERWRITE or Primary Key upsert — re-running for same ds replaces, doesn't append
- [ ] Partition creation is idempotent — `ADD PARTITION IF NOT EXISTS` or catch "already exists"
- [ ] ANALYZE runs per-partition, not full table — avoids blocking other queries during backfill
- [ ] No `datetime.now()` in SQL — always use `ds` template variable for reproducibility

---

## Anti-Patterns

1. **`INSERT INTO` without partition overwrite for batch loads** — reruns append duplicate rows; use `INSERT OVERWRITE` or Primary Key table.
2. **Non-deterministic labels** (`{table}_{datetime.now()}`) — each retry creates a new load, leading to duplicate data.
3. **`max_active_runs` not set** — concurrent DAG runs for the same date race on partition writes.
4. **Catching all exceptions in `trigger_broker_load`** — swallows auth failures and network errors silently; only catch known idempotency errors.
5. **Running ANALYZE for full table after every micro-batch** — extremely expensive; run ANALYZE per partition after daily loads only.
6. **No SLA on load completion** — pipeline silently falls behind; always set `sla` on critical tasks.

---

## References

- Airflow SLA docs: `airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#slas`
- StarRocks INSERT OVERWRITE: `docs.starrocks.io/docs/loading/InsertInto/`
- Related skills: `[[airflow-starrocks-pipeline]]`, `[[starrocks-broker-load]]`, `[[starrocks-partitioning]]`, `[[airflow-starrocks-backfill]]`
