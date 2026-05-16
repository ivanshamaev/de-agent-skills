---
name: airflow-dags
description: Apache Airflow DAG authoring — DAG definition, TaskFlow API (@task/@dag decorators), operators (Bash/Python/SQL/HTTP), sensors (poke/reschedule modes), TaskGroups, dynamic task mapping (expand/partial), branching, trigger rules, XComs, Pools, callbacks, cross-DAG pipelines, best practices
---

# Apache Airflow — DAG Authoring

## When to Use

Load this skill when the user needs to:
- Write or review Airflow DAGs
- Use TaskFlow API, operators, or sensors
- Build task pipelines, TaskGroups, or dynamic task mapping
- Configure callbacks, pools, XComs, or cross-DAG dependencies
- Apply best practices or debug anti-patterns in DAG code

---

## DAG Definition — Three Styles

### 1. Context Manager (classic)

```python
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="my_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "owner": "data-team",
    },
    tags=["etl", "finance"],
    doc_md="""## My Pipeline\nLoads daily finance data.""",
) as dag:
    EmptyOperator(task_id="start")
```

### 2. @dag Decorator (recommended for TaskFlow)

```python
from airflow.sdk import dag
import pendulum

@dag(
    dag_id="my_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["etl"],
)
def my_pipeline():
    ...

my_pipeline()  # registers the DAG
```

### 3. Constructor (legacy, avoid in new code)

```python
dag = DAG("my_dag", start_date=pendulum.datetime(2024, 1, 1), schedule="@daily")
task = BashOperator(task_id="t1", bash_command="echo hi", dag=dag)
```

---

## Key DAG Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `dag_id` | str | Unique identifier |
| `start_date` | datetime | First logical date to schedule (always use `pendulum.datetime`) |
| `schedule` | str / None | Cron, `@daily`, `@hourly`, `timedelta`, Dataset, `None`, `@once`, `@continuous` |
| `catchup` | bool | Run missed intervals since `start_date`. Default `True` — always set `False` unless intentional. |
| `max_active_runs` | int | Max concurrent DAG runs. Use `1` for sequential pipelines. |
| `max_active_tasks` | int | Max concurrently running tasks across all active runs |
| `default_args` | dict | Default params applied to every task (owner, retries, retry_delay, etc.) |
| `tags` | list[str] | UI filtering labels |
| `params` | dict | Runtime-configurable parameters exposed in UI |
| `on_failure_callback` | callable | Called on DAG run failure |
| `sla_miss_callback` | callable | Called when SLA is missed |
| `render_template_as_native_obj` | bool | Jinja renders to native Python types instead of strings |
| `doc_md` | str | Markdown docs shown in UI |

---

## TaskFlow API

The preferred modern style. `@task` wraps a Python function into an operator; return values pass automatically as XComs.

### Basic Example

```python
import json
import pendulum
from airflow.sdk import dag, task

@dag(
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def order_etl():

    @task
    def extract() -> dict:
        return {"order_id": 1001, "amount": 299.99}

    @task
    def transform(order: dict) -> dict:
        order["amount_with_tax"] = round(order["amount"] * 1.2, 2)
        return order

    @task
    def load(order: dict) -> None:
        print(f"Loading order {order['order_id']}: {order['amount_with_tax']}")

    raw = extract()
    enriched = transform(raw)
    load(enriched)

order_etl()
```

### multiple_outputs=True — unpack dict as separate XCom keys

```python
@task(multiple_outputs=True)
def get_config() -> dict:
    return {"host": "db.internal", "port": 5432, "db": "warehouse"}

cfg = get_config()
# downstream can reference cfg["host"], cfg["port"] individually
```

### .override() — reuse task with different metadata

```python
@task
def process(record: dict) -> dict:
    return {**record, "processed": True}

step_a = process.override(task_id="process_customers")(customers_data)
step_b = process.override(task_id="process_orders")(orders_data)
```

### Accessing template context inside @task

```python
from airflow.sdk import get_current_context

@task
def my_task():
    ctx = get_current_context()
    ds = ctx["ds"]                     # logical date string: "2024-01-15"
    ti = ctx["task_instance"]
    logical_dt = ctx["logical_date"]   # pendulum.DateTime
    print(f"Running for {ds}")
```

Or as explicit kwargs:

```python
@task
def my_task(ds=None, logical_date=None):
    print(ds, logical_date)
```

---

## Common Operators

### BashOperator

```python
from airflow.providers.standard.operators.bash import BashOperator

run_script = BashOperator(
    task_id="run_script",
    bash_command="dbt run --select +my_model --target prod",
    env={"DBT_PROFILES_DIR": "/opt/airflow/dbt"},
    cwd="/opt/airflow/dbt",
    retries=1,
)
```

### PythonOperator

```python
from airflow.providers.standard.operators.python import PythonOperator

def transform_data(ds: str, **kwargs) -> None:
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="extract_task")
    print(f"Processing {ds}: {data}")

transform = PythonOperator(
    task_id="transform",
    python_callable=transform_data,
    op_kwargs={"extra_param": "value"},  # merged with context on call
)
```

### BranchPythonOperator

```python
from airflow.providers.standard.operators.python import BranchPythonOperator

def choose_branch(ds: str) -> str:
    return "load_full" if ds.endswith("-01") else "load_incremental"

branch = BranchPythonOperator(
    task_id="branch",
    python_callable=choose_branch,
)

load_full = BashOperator(task_id="load_full", bash_command="echo full")
load_inc  = BashOperator(task_id="load_incremental", bash_command="echo inc")
join      = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

branch >> [load_full, load_inc] >> join
```

### SQLExecuteQueryOperator

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

run_sql = SQLExecuteQueryOperator(
    task_id="merge_staging",
    conn_id="trino_default",
    sql="""
        MERGE INTO gold.orders AS t
        USING silver.orders_stage AS s ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET amount = s.amount
        WHEN NOT MATCHED THEN INSERT VALUES (s.order_id, s.amount)
    """,
    autocommit=True,
)
```

### HttpOperator

```python
from airflow.providers.http.operators.http import HttpOperator

trigger_api = HttpOperator(
    task_id="trigger_api",
    http_conn_id="api_default",
    endpoint="/v1/jobs/start",
    method="POST",
    data='{"job": "etl_run"}',
    headers={"Content-Type": "application/json"},
    response_check=lambda resp: resp.status_code == 202,
    log_response=True,
)
```

### TriggerDagRunOperator — cross-DAG trigger

```python
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

trigger_downstream = TriggerDagRunOperator(
    task_id="trigger_reporting_dag",
    trigger_dag_id="reporting_pipeline",
    conf={"source_dag_run_id": "{{ run_id }}", "ds": "{{ ds }}"},
    wait_for_completion=True,    # block until triggered DAG finishes
    poke_interval=60,
    failed_states=["failed"],
)
```

---

## Sensors

Sensors wait for a condition to be met before allowing downstream tasks to proceed.

### Sensor Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `poke` (default) | Holds a worker slot continuously | Short waits (< 5 min) |
| `reschedule` | Releases slot between checks | Long waits (hours); saves worker slots |

**Always use `mode="reschedule"` for waits longer than a few minutes.**

### Core Parameters

```python
sensor = MySensor(
    task_id="wait_for_data",
    poke_interval=60,          # seconds between checks
    timeout=3600,              # fail after this many seconds
    mode="reschedule",         # release worker slot between pokes
    soft_fail=False,           # True = SKIPPED instead of FAILED on timeout
    exponential_backoff=True,  # progressively increase poke_interval
)
```

### FileSensor

```python
from airflow.providers.standard.sensors.file import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_landing_file",
    filepath="/data/landing/orders_{{ ds_nodash }}.csv",
    poke_interval=300,
    timeout=7200,
    mode="reschedule",
)
```

### HttpSensor

```python
from airflow.providers.http.sensors.http import HttpSensor

wait_for_api = HttpSensor(
    task_id="wait_for_api",
    http_conn_id="api_default",
    endpoint="/v1/status",
    request_params={"date": "{{ ds }}"},
    response_check=lambda resp: resp.json().get("status") == "ready",
    poke_interval=60,
    timeout=1800,
    mode="reschedule",
)
```

### ExternalTaskSensor — wait for another DAG

```python
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

wait_upstream = ExternalTaskSensor(
    task_id="wait_for_ingestion",
    external_dag_id="ingestion_pipeline",
    external_task_id="load_complete",     # None = wait for whole DAG run
    execution_date_fn=lambda dt: dt,      # map this DAG's logical date to upstream's
    timeout=3600,
    mode="reschedule",
    failed_states=["failed", "upstream_failed"],
)
```

### @task.sensor Decorator

```python
from airflow.sdk import task
from airflow.sdk.types import PokeReturnValue

@task.sensor(poke_interval=60, timeout=3600, mode="reschedule")
def wait_for_partition(ds: str) -> PokeReturnValue:
    from my_hooks import HiveHook
    hook = HiveHook(hive_conn_id="hive_default")
    ready = hook.check_for_named_partition("db", "orders", f"dt={ds}")
    return PokeReturnValue(is_done=ready, xcom_value=ds if ready else None)
```

---

## Task Dependencies

```python
# Basic chaining
extract >> transform >> load

# Fan-out / Fan-in
extract >> [transform_a, transform_b] >> join

# Reverse arrow
join << [transform_a, transform_b]

# Sequential list
from airflow.sdk import chain
chain(t1, t2, t3, t4)

# Pairwise (lists must be equal length)
chain(extract, [clean_a, clean_b], [load_a, load_b], report)

# Cross-product dependencies
from airflow.sdk import cross_downstream
cross_downstream([source_a, source_b], [target_x, target_y])
# Equivalent to: source_a >> [target_x, target_y]; source_b >> [target_x, target_y]
```

### Trigger Rules

| Rule | Meaning |
|------|---------|
| `all_success` (default) | All direct upstream tasks succeeded |
| `all_failed` | All upstream failed |
| `all_done` | All upstream finished (any state) |
| `all_skipped` | All upstream skipped |
| `one_success` | At least one upstream succeeded |
| `one_failed` | At least one upstream failed |
| `one_done` | At least one upstream finished |
| `none_failed` | No upstream failed (success or skip OK) |
| `none_skipped` | No upstream skipped |
| `none_failed_min_one_success` | No failures, at least one success (use after branching) |
| `always` | Run regardless of upstream state |

```python
# Correct join after branching
join = EmptyOperator(
    task_id="join",
    trigger_rule="none_failed_min_one_success",
)

# Watcher pattern: alert on any upstream failure
from airflow.exceptions import AirflowException

@task(trigger_rule="one_failed", retries=0)
def watcher():
    raise AirflowException("A pipeline task failed — see upstream logs.")

# Wire watcher to all other tasks
list(dag.tasks) >> watcher()
```

---

## TaskGroups

Organize related tasks into collapsible UI groups.

### Decorator Style (recommended)

```python
from airflow.sdk import task, task_group

@dag(schedule="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def my_pipeline():

    @task_group(group_id="ingest", default_args={"retries": 3})
    def ingest_group():
        @task
        def fetch_orders(): ...

        @task
        def fetch_customers(): ...

        fetch_orders()
        fetch_customers()

    @task_group(group_id="transform")
    def transform_group():
        @task
        def clean(): ...

        @task
        def enrich(): ...

        clean() >> enrich()

    ingest_group() >> transform_group()

my_pipeline()
```

### Context Manager Style

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup(group_id="validation") as validation:
    check_nulls   = SQLExecuteQueryOperator(task_id="check_nulls",   ...)
    check_counts  = SQLExecuteQueryOperator(task_id="check_counts",  ...)
    check_nulls >> check_counts
```

### Nested TaskGroups

```python
@task_group(group_id="processing")
def processing():

    @task_group(group_id="stage_1")
    def stage_1():
        t1 = EmptyOperator(task_id="extract")
        t2 = EmptyOperator(task_id="validate")
        t1 >> t2

    @task_group(group_id="stage_2")
    def stage_2():
        EmptyOperator(task_id="load")

    stage_1() >> stage_2()
```

Task IDs are prefixed: `processing.stage_1.extract`, `processing.stage_2.load`.

---

## Dynamic Task Mapping

Creates task instances at runtime based on actual data.

### expand() + partial()

```python
from airflow.sdk import task

@task
def process_table(table: str, schema: str) -> int:
    print(f"Processing {schema}.{table}")
    return 1

# schema is constant, table varies
process_table.partial(schema="silver").expand(table=["orders", "customers", "products"])
```

### expand() from upstream task output

```python
@task
def get_tables() -> list[str]:
    # query metadata catalog at runtime
    return ["orders", "customers", "returns", "products"]

@task
def validate_table(table: str) -> bool:
    print(f"Validating {table}")
    return True

tables = get_tables()
validate_table.expand(table=tables)
```

### expand_kwargs() — sets of parameters (no cross-product)

```python
from airflow.providers.standard.operators.bash import BashOperator

BashOperator.partial(task_id="run_job").expand_kwargs([
    {"bash_command": "dbt run --select orders",    "env": {"TARGET": "prod"}},
    {"bash_command": "dbt run --select customers", "env": {"TARGET": "prod"}},
    {"bash_command": "dbt run --select products",  "env": {"TARGET": "dev"}},
])
```

### .map() — transform upstream list before expanding

```python
from airflow.sdk.exceptions import AirflowSkipException

@task
def list_partitions() -> list[str]:
    return ["2024-01-01", "2024-01-02", "SKIP_ME", "2024-01-04"]

def skip_invalid(partition: str) -> str:
    if partition.startswith("SKIP"):
        raise AirflowSkipException(f"Skipping {partition}")
    return partition

@task
def process_partition(partition: str): ...

valid = list_partitions().map(skip_invalid)
process_partition.expand(partition=valid)
```

### Dynamic TaskGroup mapping

```python
@task_group(group_id="process_region")
def process_region(region: str):
    @task
    def extract(r: str): return f"data_{r}"

    @task
    def load(data: str): print(f"Loading: {data}")

    load(extract(region))

process_region.expand(region=["EU", "US", "APAC"])
```

### Limiting concurrency of mapped tasks

```python
@task(max_active_tis_per_dag=4)   # max 4 running at once across all DAG runs
def heavy_task(item: str): ...
```

---

## XComs

### Implicit XCom (TaskFlow, preferred)

```python
@task
def produce() -> dict:
    return {"key": "value"}   # auto-pushed to XCom

@task
def consume(data: dict):
    print(data["key"])         # auto-pulled from XCom

consume(produce())
```

### Explicit XCom (traditional operators)

```python
def push_fn(ti, **_):
    ti.xcom_push(key="result", value={"rows": 42})

def pull_fn(ti, **_):
    result = ti.xcom_pull(task_ids="push_task", key="result")
    print(result["rows"])
```

### XCom guidelines

- XComs use the Airflow metadata DB — keep payloads small (< 48 KB).
- For large data (DataFrames, files): write to object storage and push the path as XCom.
- Disable XCom push when not needed: `do_xcom_push=False` on operator.

---

## Callbacks

Defined at DAG or task level.

```python
def on_failure(context):
    ti  = context["task_instance"]
    dag = context["dag"]
    log_url = ti.log_url
    send_alert(f"FAILED: {dag.dag_id}.{ti.task_id}\n{log_url}")

def on_success(context):
    ti = context["task_instance"]
    print(f"Task {ti.task_id} succeeded at {ti.end_date}")

def on_retry(context):
    ti = context["task_instance"]
    print(f"Retry {ti.try_number} for {ti.task_id}")

with DAG(
    dag_id="my_dag",
    on_failure_callback=on_failure,   # fires on DAG run failure
    ...
) as dag:
    task = BashOperator(
        task_id="step",
        bash_command="my_script.sh",
        on_failure_callback=on_failure,
        on_success_callback=on_success,
        on_retry_callback=on_retry,
        sla=pendulum.duration(hours=2),  # SLA per task
    )
```

### SLA Callback (DAG-level)

```python
def sla_miss_handler(dag, task_list, blocking_task_list, slas, blocking_tis):
    send_alert(f"SLA missed in {dag.dag_id}: {[s.task_id for s in slas]}")

with DAG("my_dag", sla_miss_callback=sla_miss_handler, ...):
    ...
```

---

## Pools

Control how many tasks run concurrently against a resource.

```python
# Create via CLI:
# airflow pools set db_pool 5 "Max 5 concurrent DB tasks"

@task(pool="db_pool", pool_slots=1, priority_weight=10)
def query_database(table: str): ...

# High-priority task uses 2 slots (counts as double):
@task(pool="api_pool", pool_slots=2, priority_weight=20)
def heavy_api_call(): ...
```

**priority_weight**: higher value → scheduled first when pool is full. Default 1.

---

## Variables and Connections

```python
from airflow.sdk import Variable, Connection

# WRONG — executes at parse time, hits DB on every scheduler cycle:
# MY_ENV = Variable.get("my_env")

# RIGHT — read inside task at execution time:
@task
def my_task():
    env = Variable.get("my_env", default_var="prod")
    secret = Variable.get("db_password", deserialize_json=False)
    ...

# ALSO RIGHT — Jinja template (lazy evaluation):
BashOperator(
    task_id="run",
    bash_command="echo {{ var.value.my_env }}",
)

# Connections:
@task
def fetch():
    conn = Connection.get("trino_default")
    print(conn.host, conn.port)
```

---

## Execution Environments

### @task.virtualenv — ephemeral venv per run

```python
@task.virtualenv(
    task_id="run_pandas",
    requirements=["pandas==2.1.0", "pyarrow>=14.0"],
    python_version="3.11",
    system_site_packages=False,
)
def process():
    import pandas as pd
    df = pd.DataFrame({"a": [1, 2, 3]})
    return df.to_json()
```

### @task.external_python — pre-existing venv

```python
@task.external_python(python="/opt/venvs/pandas_env/bin/python")
def process():
    import pandas as pd
    ...
```

### @task.docker

```python
@task.docker(image="my-registry/etl-image:1.2", network_mode="host")
def run_in_container():
    import special_lib
    ...
```

### @task.kubernetes

```python
@task.kubernetes(
    image="my-registry/spark-submit:3.5",
    name="spark-submit-task",
    namespace="airflow",
    in_cluster=True,
    get_logs=True,
)
def submit_spark_job(): ...
```

---

## Cross-DAG Pipelines

### Pattern 1: TriggerDagRunOperator + ExternalTaskSensor

```python
# upstream_dag.py
with DAG("ingestion_pipeline", schedule="@daily", ...):
    ...
    notify_done = EmptyOperator(task_id="done")  # ExternalTaskSensor target

# downstream_dag.py
with DAG("transformation_pipeline", schedule=None, ...):

    wait = ExternalTaskSensor(
        task_id="wait_ingestion",
        external_dag_id="ingestion_pipeline",
        external_task_id="done",
        execution_date_fn=lambda dt: dt,   # same logical date
        mode="reschedule",
        timeout=7200,
    )

    transform = BashOperator(task_id="transform", bash_command="dbt run")
    wait >> transform
```

### Pattern 2: Dataset-driven scheduling (Airflow 2.4+)

```python
from airflow.sdk import Asset

orders_asset = Asset("s3://datalake/silver/orders/")

# producer DAG
with DAG("ingestion", schedule="@daily", ...):
    BashOperator(
        task_id="load_orders",
        bash_command="spark-submit load_orders.py",
        outlets=[orders_asset],
    )

# consumer DAG — runs automatically when asset is updated
with DAG("transformation", schedule=[orders_asset], ...):
    BashOperator(task_id="transform", bash_command="dbt run --select orders+")
```

---

## Complete Pipeline Example

```python
import pendulum
from airflow.sdk import dag, task, task_group
from airflow.providers.standard.sensors.file import FileSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id="daily_orders_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["orders", "daily"],
)
def daily_orders_pipeline():

    wait_for_file = FileSensor(
        task_id="wait_for_landing_file",
        filepath="/data/landing/orders_{{ ds_nodash }}.csv",
        poke_interval=300,
        timeout=7200,
        mode="reschedule",
    )

    @task_group(group_id="ingest")
    def ingest():

        @task
        def read_file(ds: str) -> list[dict]:
            import csv
            rows = []
            with open(f"/data/landing/orders_{ds.replace('-','')}.csv") as f:
                rows = list(csv.DictReader(f))
            return rows

        @task
        def validate(rows: list[dict]) -> list[dict]:
            valid = [r for r in rows if r.get("order_id")]
            if not valid:
                raise ValueError("No valid rows found")
            return valid

        raw = read_file()
        return validate(raw)

    @task_group(group_id="transform")
    def transform(rows: list[dict]):

        @task
        def enrich(rows: list[dict]) -> list[dict]:
            for r in rows:
                r["total_with_tax"] = float(r["amount"]) * 1.2
            return rows

        @task
        def deduplicate(rows: list[dict]) -> list[dict]:
            seen = set()
            result = []
            for r in rows:
                if r["order_id"] not in seen:
                    seen.add(r["order_id"])
                    result.append(r)
            return result

        return deduplicate(enrich(rows))

    load_silver = SQLExecuteQueryOperator(
        task_id="load_silver",
        conn_id="trino_default",
        sql="CALL silver.orders_merge_proc('{{ ds }}')",
    )

    dq_check = SQLExecuteQueryOperator(
        task_id="dq_check",
        conn_id="trino_default",
        sql="""
            SELECT CASE WHEN COUNT(*) = 0 THEN TRUE
            ELSE RAISE('DQ FAILED: nulls in order_id') END
            FROM silver.orders WHERE dt = DATE '{{ ds }}' AND order_id IS NULL
        """,
    )

    validated = ingest()
    transformed = transform(validated)
    wait_for_file >> validated
    transformed >> load_silver >> dq_check

daily_orders_pipeline()
```

---

## Jinja Templating

```python
# Common template variables
"{{ ds }}"                # logical date string: "2024-01-15"
"{{ ds_nodash }}"         # "20240115"
"{{ ts }}"                # ISO timestamp: "2024-01-15T00:00:00+00:00"
"{{ run_id }}"            # unique run identifier
"{{ dag.dag_id }}"
"{{ task.task_id }}"
"{{ var.value.my_var }}"              # Airflow Variable
"{{ conn.my_conn.host }}"             # Connection attribute
"{{ macros.ds_add(ds, 7) }}"         # date arithmetic
"{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m/%d') }}"
"{{ next_ds }}"           # next scheduled logical date
"{{ prev_ds }}"           # previous scheduled logical date

# In BashOperator, pass via env for safety (avoids shell injection):
BashOperator(
    task_id="run",
    bash_command="process.sh",
    env={"LOGICAL_DATE": "{{ ds }}", "RUN_ID": "{{ run_id }}"},
)
```

---

## Best Practices

### DAG File Structure

1. **No top-level execution** — never call APIs, DB queries, or `Variable.get()` at module level. The scheduler parses every DAG file every `min_file_process_interval` seconds.
2. **One DAG per file** — simplifies testing, discovery, and debugging.
3. **Use `pendulum`** for `start_date`, never `datetime.now()` or relative dates.
4. **Always set `catchup=False`** unless you explicitly need historical backfill.
5. **Always set `max_active_runs=1`** for stateful pipelines (prevents overlapping runs).

### Task Design

6. **Idempotency** — tasks must produce the same result when re-run for the same logical date. Use MERGE/UPSERT instead of INSERT; write to partitions, not full tables.
7. **Atomicity** — each task does one logical unit of work; avoid giant tasks that mix extract + transform + load.
8. **Never read "latest" data** inside a task — always read a specific partition/date derived from `{{ ds }}` or context.
9. **Store large intermediate data in object storage**, not XCom. Push the S3/HDFS path as XCom.
10. **Use Connections**, never hardcode credentials in DAG code.

### Performance

11. **Avoid heavy imports at top level** — move `import pandas`, `import pyspark` inside task functions.
12. **Use `mode="reschedule"` on sensors** for waits > 5 minutes.
13. **Limit dynamic task count** — default max is 1024 (`max_map_length`). Large fanouts degrade the scheduler.
14. **Use Pools** to cap concurrency against shared resources (databases, APIs).
15. **Clean metadata DB regularly**: `airflow db clean --clean-before-timestamp "$(date -d '90 days ago' '+%Y-%m-%d')"`.

### Testing

```bash
# Parse check — runs in < 2s if DAG is healthy
python my_dag.py

# DAG load test
python -c "from airflow.dag_processing.dagbag import DagBag; d=DagBag(); assert not d.import_errors"

# Full dry-run with executor
airflow dags test my_dag_id 2024-01-15

# Single task dry-run
airflow tasks test my_dag_id my_task_id 2024-01-15
```

Unit test example:

```python
from airflow.dag_processing.dagbag import DagBag

def test_dag_loads():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.get_dag("daily_orders_pipeline")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) > 0

def test_no_import_errors():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert dagbag.import_errors == {}, dagbag.import_errors
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| `Variable.get()` at module level | Runs on every parse cycle, slows scheduler | Move inside task or use Jinja `{{ var.value.key }}` |
| `datetime.now()` as `start_date` | DAG shifts its start each time file is reparsed | Use fixed `pendulum.datetime(2024, 1, 1)` |
| Skipping `catchup=False` | Hundreds of backfill runs spawn on first deploy | Always set explicitly |
| Giant monolithic task | One failure reruns everything; no observability | Split into extract → validate → transform → load |
| `INSERT` without deduplication | Retries produce duplicate rows | Use MERGE/UPSERT or partition-overwrite |
| `xcom_push` of large objects | Bloats metadata DB, slow XCom reads | Write to object storage, push path |
| Importing heavy libraries at top level | Each DAG parse imports pandas/spark | Import inside task functions |
| `mode="poke"` on long-running sensors | Holds worker slot for hours | Use `mode="reschedule"` |
| Hardcoded credentials | Security risk, breaks on rotation | Use Airflow Connections |
| `trigger_rule="all_success"` after branch | Downstream never runs (branch path skipped) | Use `none_failed_min_one_success` |
| Missing `max_active_runs=1` on stateful DAG | Concurrent runs corrupt shared state | Set `max_active_runs=1` |
| `depends_on_past=True` without monitoring | Stuck run blocks all future runs silently | Use `wait_for_past_depends_before_skipping` + alerting |

---

## References to Consult When Needed

- [Apache Airflow Docs — DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
- [Apache Airflow Docs — TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Apache Airflow Docs — Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dynamic-task-mapping.html)
- [Apache Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Astronomer — DAG Writing Best Practices](https://www.astronomer.io/docs/learn/airflow-dag-writing-best-practices)
- [Astronomer — Dynamic Tasks Guide](https://www.astronomer.io/docs/learn/dynamic-tasks)
- [Astronomer — TaskGroups Guide](https://www.astronomer.io/docs/learn/task-groups)
- [Astronomer — Airflow Pools Guide](https://www.astronomer.io/docs/learn/airflow-pools)
