---
name: airflow-starrocks-cdc-orchestrator
description: Airflow CDC orchestration for StarRocks — watermark-based incremental sync, Routine Load health check DAG, Flink job submission via REST API, offset lag alerting, dead letter queue reprocessing, multi-table incremental sync with dependency graph, schema change detection and pipeline pause/resume
---

# Airflow CDC Orchestration for StarRocks

## When to Use

- Orchestrate CDC pipelines feeding StarRocks (Flink CDC jobs, Routine Load jobs)
- Watermark-based incremental sync (poll source, detect new data, load delta)
- Monitor Kafka Routine Load lag and alert or auto-pause on excessive lag
- Handle DLQ (Dead Letter Queue) reprocessing after fixing parse errors
- Coordinate schema changes across Debezium → Kafka → StarRocks without data loss

---

## Architecture

```
Airflow DAG
  ├── HealthCheckSensor ──► SHOW ROUTINE LOAD (is RUNNING?)
  ├── LagCheckOperator  ──► Kafka consumer lag < threshold?
  ├── WatermarkTask     ──► Read MAX(updated_at) from StarRocks
  ├── IncrementalLoad   ──► Flink job or Broker Load for delta
  ├── DLQReprocess      ──► Replay failed events from DLQ topic
  └── SchemaChangeGuard ──► Pause → ALTER → Resume pipeline
```

---

## Watermark-Based Incremental Sync DAG

```python
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import time

STARROCKS_CONN = "starrocks_prod"
DATABASE = "sales"


@dag(
    dag_id="starrocks_cdc_incremental_sync",
    schedule="*/15 * * * *",      # every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,             # prevent overlapping incremental windows
    tags=["starrocks", "cdc"],
)
def cdc_incremental_sync():

    @task
    def get_watermark(table: str = "orders") -> str:
        """Get the latest updated_at already in StarRocks."""
        hook = MySqlHook(mysql_conn_id=STARROCKS_CONN)
        rows = hook.get_records(f"""
            SELECT COALESCE(
                MAX(updated_at),
                '2024-01-01 00:00:00'
            ) AS watermark
            FROM {DATABASE}.{table}
        """)
        watermark = str(rows[0][0])
        print(f"Current watermark: {watermark}")
        return watermark

    @task
    def check_source_rows(watermark: str, table: str = "orders") -> int:
        """Count rows in source that are newer than watermark."""
        # Replace with your source DB hook (PostgreSQL, MySQL, etc.)
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id="source_pg")
        rows = pg_hook.get_records(f"""
            SELECT COUNT(*) FROM {table}
            WHERE updated_at > '{watermark}'::timestamp
        """)
        count = rows[0][0]
        print(f"New rows since {watermark}: {count}")
        return count

    @task.branch
    def decide_load(new_rows: int) -> str:
        if new_rows == 0:
            print("No new rows — skipping load")
            return "skip_load"
        return "load_incremental"

    @task(task_id="load_incremental")
    def load_incremental(watermark: str, table: str = "orders"):
        """Load rows newer than watermark into StarRocks staging table."""
        import requests, json

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id="source_pg")

        # Extract from source
        rows = pg_hook.get_records(f"""
            SELECT order_id, customer_id, amount, status, updated_at
            FROM {table}
            WHERE updated_at > '{watermark}'::timestamp
            ORDER BY updated_at
            LIMIT 50000
        """)

        if not rows:
            return

        # Convert to NDJSON
        import hashlib, time as t
        records = [
            {"order_id": r[0], "customer_id": r[1], "amount": float(r[2]),
             "status": r[3], "updated_at": str(r[4])}
            for r in rows
        ]
        ndjson = "\n".join(json.dumps(r) for r in records)
        label = f"cdc_{table}_{int(t.time())}_{hashlib.md5(ndjson[:100].encode()).hexdigest()[:6]}"

        resp = requests.put(
            f"http://sr-fe.internal:8030/api/{DATABASE}/{table}/_stream_load",
            headers={
                "label": label,
                "Expect": "100-continue",
                "format": "json",
                "jsonpaths": '["$.order_id","$.customer_id","$.amount","$.status","$.updated_at"]',
                "columns": "order_id,customer_id,amount,status,updated_at",
            },
            data=ndjson.encode(),
            auth=("root", "password"),
            timeout=120,
        )
        result = resp.json()
        if result["Status"] not in ("Success", "Publish Timeout"):
            raise RuntimeError(f"Stream Load failed: {result}")
        print(f"Loaded {result['NumberLoadedRows']} rows, label={label}")

    @task(task_id="skip_load")
    def skip_load():
        print("No new data — skipping")

    watermark = get_watermark()
    count = check_source_rows(watermark)
    branch = decide_load(count)
    load = load_incremental(watermark)
    skip = skip_load()

    branch >> [load, skip]


dag = cdc_incremental_sync()
```

---

## Routine Load Health Monitor DAG

Runs every 5 minutes; auto-restarts NEED_SCHEDULE jobs:

```python
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

ROUTINE_LOAD_JOBS = [
    {"db": "sales",    "job": "cdc_orders"},
    {"db": "sales",    "job": "cdc_customers"},
    {"db": "finance",  "job": "cdc_transactions"},
]


@dag(
    dag_id="routine_load_health_monitor",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["starrocks", "monitoring"],
)
def routine_load_health_monitor():

    @task
    def check_and_heal_jobs() -> dict:
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        report = {}

        for job_config in ROUTINE_LOAD_JOBS:
            db = job_config["db"]
            job = job_config["job"]

            rows = hook.get_records(f"SHOW ROUTINE LOAD FOR {db}.{job}")
            if not rows:
                report[f"{db}.{job}"] = "NOT_FOUND"
                continue

            # Column indices (0-based) from SHOW ROUTINE LOAD
            row = rows[0]
            state = row[7] if len(row) > 7 else "UNKNOWN"
            reason = row[16] if len(row) > 16 else ""
            error_log = row[17] if len(row) > 17 else ""

            report[f"{db}.{job}"] = state

            if state == "PAUSED":
                print(f"WARNING: {db}.{job} is PAUSED. Reason: {reason}")
                # Send alert (replace with PagerDuty/Slack hook)
                print(f"Error logs: {error_log}")

            elif state == "NEED_SCHEDULE":
                print(f"Resuming {db}.{job} (was NEED_SCHEDULE)")
                hook.run(f"RESUME ROUTINE LOAD FOR {db}.{job}")

            elif state == "CANCELLED":
                print(f"CRITICAL: {db}.{job} CANCELLED. Manual intervention required.")

        return report

    check_and_heal_jobs()


dag = routine_load_health_monitor()
```

---

## Flink Job Orchestration via REST API

Submit and monitor Flink CDC jobs from Airflow:

```python
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
import json, time

FLINK_REST_CONN = "flink_jobmanager"  # HTTP connection to Flink REST API


@task
def submit_flink_cdc_job(jar_id: str, entry_class: str, parallelism: int = 4) -> str:
    """Submit a Flink CDC JAR job and return job ID."""
    hook = HttpHook(method="POST", http_conn_id=FLINK_REST_CONN)
    response = hook.run(
        endpoint=f"/jars/{jar_id}/run",
        data=json.dumps({
            "entryClass": entry_class,
            "parallelism": parallelism,
            "programArgs": "--source postgres --target starrocks",
        }),
        headers={"Content-Type": "application/json"},
    )
    job_id = response.json()["jobid"]
    print(f"Submitted Flink job: {job_id}")
    return job_id


@task
def wait_for_flink_job(job_id: str, timeout_minutes: int = 30):
    """Poll Flink job status until RUNNING or FINISHED."""
    hook = HttpHook(method="GET", http_conn_id=FLINK_REST_CONN)
    deadline = time.time() + timeout_minutes * 60

    while time.time() < deadline:
        response = hook.run(endpoint=f"/jobs/{job_id}")
        status = response.json().get("state", "UNKNOWN")
        print(f"Flink job {job_id} state: {status}")

        if status == "RUNNING":
            print("Flink CDC job is RUNNING — pipeline active")
            return status
        elif status in ("FINISHED", "CANCELED", "FAILED"):
            if status != "FINISHED":
                raise RuntimeError(f"Flink job {job_id} ended with state: {status}")
            return status

        time.sleep(15)

    raise TimeoutError(f"Flink job {job_id} did not reach RUNNING within {timeout_minutes}m")


@task
def cancel_flink_job(job_id: str):
    """Cancel a running Flink CDC job (e.g., before schema change)."""
    hook = HttpHook(method="PATCH", http_conn_id=FLINK_REST_CONN)
    hook.run(
        endpoint=f"/jobs/{job_id}",
        data=json.dumps({"status": "canceled"}),
        headers={"Content-Type": "application/json"},
    )
    print(f"Flink job {job_id} cancel requested")
```

---

## DLQ Reprocessing DAG

Replay events from Dead Letter Queue after fixing parse errors:

```python
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

@dag(
    dag_id="starrocks_dlq_reprocess",
    schedule=None,  # triggered manually after DLQ investigation
    start_date=datetime(2024, 1, 1),
    params={
        "dlq_topic": "dlq.starrocks.orders",
        "target_table": "orders",
        "max_messages": 10000,
    },
    tags=["starrocks", "dlq"],
)
def dlq_reprocess():

    @task
    def read_dlq_messages(params: dict = None) -> list:
        """Read messages from DLQ Kafka topic."""
        from confluent_kafka import Consumer, KafkaException
        import json

        consumer = Consumer({
            "bootstrap.servers": "kafka:9092",
            "group.id": "dlq_reprocess_airflow",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([params["dlq_topic"]])

        messages = []
        max_msgs = params.get("max_messages", 10000)
        timeout = 10.0

        while len(messages) < max_msgs:
            msg = consumer.poll(timeout)
            if msg is None:
                break
            if msg.error():
                continue
            try:
                record = json.loads(msg.value().decode("utf-8"))
                messages.append(record)
            except json.JSONDecodeError as e:
                print(f"Skipping non-JSON DLQ message: {e}")

        consumer.close()
        print(f"Read {len(messages)} messages from DLQ")
        return messages

    @task
    def transform_and_load(messages: list, params: dict = None) -> dict:
        """Apply fix transformations and load to StarRocks."""
        import requests, json, hashlib, time

        if not messages:
            return {"loaded": 0}

        # Apply any fix transformations here
        fixed = []
        for msg in messages:
            try:
                # Example fix: parse nested payload
                payload = msg.get("payload", msg)
                after = payload.get("after", payload)
                if after and after.get("order_id"):
                    fixed.append(after)
            except Exception as e:
                print(f"Skipping unfixable message: {e}")

        if not fixed:
            return {"loaded": 0}

        ndjson = "\n".join(json.dumps(r, default=str) for r in fixed)
        label = f"dlq_reprocess_{params['target_table']}_{int(time.time())}"

        resp = requests.put(
            f"http://sr-fe.internal:8030/api/sales/{params['target_table']}/_stream_load",
            headers={
                "label": label,
                "Expect": "100-continue",
                "format": "json",
                "max_filter_ratio": "0.1",  # allow some errors during DLQ replay
            },
            data=ndjson.encode(),
            auth=("root", "password"),
            timeout=300,
        )
        result = resp.json()
        print(f"DLQ reload: {result['NumberLoadedRows']} rows, {result.get('NumberFilteredRows', 0)} filtered")
        return result

    messages = read_dlq_messages()
    transform_and_load(messages)


dag = dlq_reprocess()
```

---

## Schema Change Coordination DAG

Safely propagate a schema change through the CDC pipeline:

```python
@dag(
    dag_id="cdc_schema_change_coordinator",
    schedule=None,
    params={
        "routine_load_job": "cdc_orders",
        "db": "sales",
        "alter_sql": "ALTER TABLE sales.orders ADD COLUMN discount DECIMAL(10,2) DEFAULT 0",
    },
    tags=["starrocks", "schema-change"],
)
def cdc_schema_change_coordinator():

    @task
    def pause_routine_load(params: dict = None):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        db = params["db"]
        job = params["routine_load_job"]

        hook.run(f"PAUSE ROUTINE LOAD FOR {db}.{job}")

        # Wait for PAUSED state
        for _ in range(60):
            rows = hook.get_records(f"SHOW ROUTINE LOAD FOR {db}.{job}")
            if rows and rows[0][7] == "PAUSED":
                print(f"Job {job} PAUSED")
                return
            time.sleep(3)
        raise TimeoutError(f"Job {job} did not pause")

    @task
    def apply_alter(params: dict = None):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        hook.run(params["alter_sql"])
        print(f"Applied: {params['alter_sql']}")

    @task
    def update_debezium_column_list(params: dict = None):
        """Signal Debezium to refresh schema via Kafka Connect REST API."""
        import requests
        resp = requests.post(
            "http://kafka-connect:8083/connectors/postgres-cdc-source/tasks/0/restart"
        )
        print(f"Debezium task restart: {resp.status_code}")

    @task
    def resume_routine_load(params: dict = None):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        db = params["db"]
        job = params["routine_load_job"]
        hook.run(f"RESUME ROUTINE LOAD FOR {db}.{job}")
        print(f"Job {job} resumed")

    pause = pause_routine_load()
    alter = apply_alter()
    debezium = update_debezium_column_list()
    resume = resume_routine_load()

    pause >> alter >> debezium >> resume


dag = cdc_schema_change_coordinator()
```

---

## Anti-Patterns

1. **Running incremental sync DAG with `catchup=True`** — each backfill run picks the same MAX(updated_at), causing all historical data to be re-loaded; disable catchup for watermark DAGs.
2. **Not setting `max_active_runs=1`** — two concurrent incremental runs read the same watermark and load the same data twice.
3. **Detecting schema changes by catching SQL exceptions** — fragile; explicitly check `SHOW CREATE TABLE` before and after DDL operations.
4. **Reprocessing DLQ without applying the fix** — reloading original broken messages reproduces the same error; always transform before reloading.
5. **No drain wait after PAUSE ROUTINE LOAD** — tasks in flight may still be writing; poll for `State=PAUSED` before applying schema change.
6. **Using HTTP Flink REST API without authentication** — Flink REST API is unauthenticated by default; use a proxy or network policy in production.

---

## References

- StarRocks Routine Load: `docs.starrocks.io/docs/loading/RoutineLoad/`
- Flink REST API: `nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/`
- Related skills: `[[airflow-starrocks-pipeline]]`, `[[starrocks-cdc-pipeline]]`, `[[starrocks-routine-load-kafka]]`, `[[airflow-starrocks-data-quality]]`
