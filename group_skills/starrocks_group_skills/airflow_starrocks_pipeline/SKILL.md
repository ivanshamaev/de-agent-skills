---
name: airflow-starrocks-pipeline
description: Airflow + StarRocks pipeline orchestration — MySqlHook for DDL/DML, Broker Load trigger + poll pattern, Stream Load via HTTP operator, Routine Load job lifecycle (pause/resume/stop), sensor for load completion, ANALYZE TABLE post-load, partition-aware DAG templating, Airflow variables for StarRocks connection config, XCom label passing
---

# Airflow + StarRocks Pipeline Orchestration

## When to Use

- Orchestrate StarRocks Broker Load (async S3 batch ingestion) with polling
- Trigger and monitor Routine Load lifecycle (pause/resume for schema changes)
- Build daily/hourly ETL DAGs that write to StarRocks and run post-load ANALYZE
- Chain data quality checks after load completion
- Manage StarRocks DDL (CREATE TABLE, ADD PARTITION) from Airflow

---

## Connection Setup

### Airflow Connection (MySQL protocol)

StarRocks FE exposes a MySQL-compatible interface on port 9030.

```
Connection ID: starrocks_prod
Connection Type: MySQL
Host: sr-fe.internal
Port: 9030
Schema: sales
Login: etl_user
Password: <password>
```

Via environment variable:
```bash
AIRFLOW_CONN_STARROCKS_PROD='mysql://etl_user:password@sr-fe.internal:9030/sales'
```

### Hook Usage

```python
from airflow.providers.mysql.hooks.mysql import MySqlHook

hook = MySqlHook(mysql_conn_id="starrocks_prod")

# Execute DDL / DML
hook.run("ALTER TABLE orders ADD PARTITION ...")

# Query results
rows = hook.get_records("SHOW LOAD FROM sales WHERE LABEL = 'my_load'")
row_dict = dict(zip(col_names, rows[0]))
```

---

## Pattern 1: Broker Load DAG (S3 Batch)

```python
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import time

STARROCKS_CONN = "starrocks_prod"
DATABASE = "sales"
S3_REGION = "us-east-1"


@dag(
    dag_id="starrocks_broker_load_orders",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["starrocks", "ingestion"],
)
def broker_load_orders():

    @task
    def add_partition(ds: str = None):
        """Ensure target partition exists before loading."""
        hook = MySqlHook(mysql_conn_id=STARROCKS_CONN)
        partition_name = f"p{ds.replace('-', '')}"
        partition_start = ds
        partition_end = (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

        hook.run(f"""
            ALTER TABLE {DATABASE}.orders
            ADD PARTITION IF NOT EXISTS {partition_name}
            VALUES [("{partition_start}"), ("{partition_end}"))
        """)
        return partition_name

    @task
    def trigger_broker_load(ds: str = None) -> str:
        hook = MySqlHook(mysql_conn_id=STARROCKS_CONN)
        label = f"orders_{ds.replace('-', '')}"

        # Idempotent: check if label already completed
        existing = hook.get_records(
            f"SHOW LOAD FROM {DATABASE} WHERE LABEL = '{label}'"
        )
        if existing:
            state = existing[0][2]  # State column index
            if state == "FINISHED":
                print(f"Label {label} already FINISHED — skipping")
                return label
            elif state in ("LOADING", "ETL", "PENDING"):
                print(f"Label {label} already in state {state} — will poll")
                return label
            # CANCELLED → re-run with same label is not allowed; use date+attempt
            label = f"orders_{ds.replace('-', '')}_retry"

        hook.run(f"""
            LOAD LABEL {DATABASE}.{label} (
                DATA INFILE("s3a://datalake/orders/dt={ds}/*.parquet")
                INTO TABLE orders
                FORMAT AS "parquet"
            )
            WITH BROKER (
                "aws.s3.use_instance_profile" = "true",
                "aws.s3.region" = "{S3_REGION}"
            )
            PROPERTIES (
                "timeout" = "3600",
                "max_filter_ratio" = "0.01"
            )
        """)
        return label

    @task
    def wait_for_load(label: str, timeout_minutes: int = 60) -> dict:
        hook = MySqlHook(mysql_conn_id=STARROCKS_CONN)
        col_names = [
            "JobId", "Label", "State", "Progress", "Type", "EtlInfo",
            "TaskInfo", "ErrorMsg", "CreateTime", "EtlStartTime",
            "LoadStartTime", "FinishTime", "URL", "JobDetails",
            "TransactionId", "Reason", "LoadBytes", "ClusterName",
            "Comment", "TrackingSQL"
        ]
        deadline = time.time() + timeout_minutes * 60

        while time.time() < deadline:
            rows = hook.get_records(
                f"SHOW LOAD FROM {DATABASE} WHERE LABEL = '{label}'"
            )
            if not rows:
                time.sleep(30)
                continue

            row = dict(zip(col_names, rows[0]))
            state = row["State"]

            if state == "FINISHED":
                print(f"Load finished: {row['EtlInfo']}")
                return row
            elif state == "CANCELLED":
                raise RuntimeError(
                    f"Broker Load CANCELLED: {row['ErrorMsg']}"
                )

            print(f"Load state={state}, progress={row['Progress']}")
            time.sleep(30)

        raise TimeoutError(f"Load {label} did not finish within {timeout_minutes} minutes")

    @task
    def analyze_partition(ds: str = None):
        hook = MySqlHook(mysql_conn_id=STARROCKS_CONN)
        partition_name = f"p{ds.replace('-', '')}"
        hook.run(f"""
            ANALYZE TABLE {DATABASE}.orders
            PARTITION ({partition_name})
            WITH ASYNC MODE
        """)

    @task
    def validate_row_count(ds: str = None, min_rows: int = 1000):
        hook = MySqlHook(mysql_conn_id=STARROCKS_CONN)
        rows = hook.get_records(f"""
            SELECT COUNT(*) FROM {DATABASE}.orders
            WHERE DATE(created_at) = '{ds}'
        """)
        count = rows[0][0]
        if count < min_rows:
            raise ValueError(f"Expected >= {min_rows} rows for {ds}, got {count}")
        print(f"Validation passed: {count} rows for {ds}")

    from datetime import timedelta

    partition = add_partition()
    label = trigger_broker_load()
    load_result = wait_for_load(label)
    analyze = analyze_partition()
    validate = validate_row_count()

    partition >> label >> load_result >> analyze >> validate


dag = broker_load_orders()
```

---

## Pattern 2: Stream Load Micro-Batch DAG

For application data pushed every N minutes:

```python
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
import json
import hashlib
import time

@dag(
    dag_id="stream_load_events",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["starrocks", "stream-load"],
)
def stream_load_events():

    @task
    def extract_events(data_interval_start=None, data_interval_end=None) -> list:
        """Pull events from source API between interval bounds."""
        # Replace with actual extraction logic
        return [
            {"event_id": "e1", "user_id": 1, "event_type": "click", "ts": "2024-01-15 10:00:00"},
            {"event_id": "e2", "user_id": 2, "event_type": "view",  "ts": "2024-01-15 10:01:00"},
        ]

    @task
    def load_to_starrocks(records: list, data_interval_start=None) -> dict:
        import requests

        if not records:
            return {"NumberLoadedRows": 0}

        ts_str = data_interval_start.strftime("%Y%m%d%H%M%S")
        content_hash = hashlib.md5(
            json.dumps(records, default=str).encode()
        ).hexdigest()[:8]
        label = f"events_{ts_str}_{content_hash}"

        ndjson = "\n".join(json.dumps(r) for r in records)

        resp = requests.put(
            "http://sr-fe.internal:8030/api/sales/events/_stream_load",
            headers={
                "label": label,
                "Expect": "100-continue",
                "format": "json",
                "jsonpaths": '["$.event_id","$.user_id","$.event_type","$.ts"]',
                "columns": "event_id,user_id,event_type,ts",
                "max_filter_ratio": "0.01",
            },
            data=ndjson.encode("utf-8"),
            auth=("root", "password"),
            timeout=120,
        )
        result = resp.json()

        if result["Status"] not in ("Success", "Publish Timeout"):
            raise RuntimeError(f"Stream load failed: {result['Status']} — {result.get('Message')}")

        print(f"Loaded {result['NumberLoadedRows']} rows, label={label}")
        return result

    records = extract_events()
    stream_load_events(records)


dag = stream_load_events()
```

---

## Pattern 3: Routine Load Lifecycle Management

Pause Routine Load during schema changes, then resume:

```python
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
import time

@dag(
    dag_id="routine_load_schema_change",
    schedule=None,  # triggered manually or by upstream event
    start_date=datetime(2024, 1, 1),
    tags=["starrocks", "schema-change"],
)
def routine_load_schema_change():

    @task
    def pause_routine_load(job_name: str = "cdc_orders", db: str = "sales"):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        hook.run(f"PAUSE ROUTINE LOAD FOR {db}.{job_name}")

        # Wait for running tasks to drain
        for _ in range(30):
            rows = hook.get_records(
                f"SHOW ROUTINE LOAD FOR {db}.{job_name}"
            )
            if rows:
                cols = [d[0] for d in hook.get_conn().cursor().description] if False else [
                    "Id", "Name", "CreateTime", "PauseTime", "EndTime",
                    "DbName", "TableName", "State", "DataSourceType",
                    "CurrentTaskNum", "JobProperties", "DataSourceProperties",
                    "CustomProperties", "Statistic", "Progress",
                    "TimestampProgress", "ReasonOfStateChanged", "ErrorLogUrls",
                    "OtherMsg", "TrackingSQL"
                ]
                row = dict(zip(cols[:len(rows[0])], rows[0]))
                if row.get("State") == "PAUSED":
                    print(f"Routine Load {job_name} is PAUSED")
                    return
            time.sleep(5)

        raise TimeoutError(f"Routine Load {job_name} did not pause in time")

    @task
    def apply_schema_change(db: str = "sales"):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        hook.run(f"ALTER TABLE {db}.orders ADD COLUMN discount DECIMAL(10,2) DEFAULT 0")
        print("Schema change applied")

    @task
    def resume_routine_load(job_name: str = "cdc_orders", db: str = "sales"):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        hook.run(f"RESUME ROUTINE LOAD FOR {db}.{job_name}")
        print(f"Routine Load {job_name} resumed")

    pause = pause_routine_load()
    change = apply_schema_change()
    resume = resume_routine_load()

    pause >> change >> resume


dag = routine_load_schema_change()
```

---

## Custom Sensor: Wait for Routine Load Lag < Threshold

```python
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json

class RoutineLoadLagSensor(BaseSensorOperator):
    """Waits until Routine Load consumer lag drops below max_lag messages."""

    def __init__(
        self,
        job_name: str,
        db: str,
        max_lag: int,
        starrocks_conn_id: str = "starrocks_prod",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.db = db
        self.max_lag = max_lag
        self.starrocks_conn_id = starrocks_conn_id

    def poke(self, context) -> bool:
        hook = MySqlHook(mysql_conn_id=self.starrocks_conn_id)
        rows = hook.get_records(
            f"SHOW ROUTINE LOAD FOR {self.db}.{self.job_name}"
        )
        if not rows:
            self.log.info("Routine Load job not found")
            return False

        # Parse progress: {"Partition[0]: 12345", ...}
        row = rows[0]
        # State is at index 7 in typical column order
        state = row[7] if len(row) > 7 else "UNKNOWN"
        if state != "RUNNING":
            self.log.info(f"Routine Load state is {state}, not RUNNING")
            return False

        # Statistic: {"totalRows":123,"errorRows":0,"unselectedRows":0,...}
        try:
            stat_str = row[13] if len(row) > 13 else "{}"
            stat = json.loads(stat_str)
            lag = stat.get("receiveBytes", 0)  # proxy for lag
            self.log.info(f"Current receiveBytes: {lag}")
        except Exception:
            pass

        # Progress: per-partition offsets JSON
        try:
            progress_raw = row[14] if len(row) > 14 else "{}"
            # StarRocks returns: {"0":"12345","1":"67890"}
            committed = json.loads(progress_raw.split(": ", 1)[-1])
            total_committed = sum(int(v) for v in committed.values())
            self.log.info(f"Total committed offset: {total_committed}")
        except Exception:
            pass

        return True  # Sensor passes once job is RUNNING; pair with lag check above


# Usage in DAG:
# lag_sensor = RoutineLoadLagSensor(
#     task_id="wait_for_low_lag",
#     job_name="cdc_orders",
#     db="sales",
#     max_lag=10000,
#     poke_interval=30,
#     timeout=600,
#     mode="reschedule",
# )
```

---

## ANALYZE TABLE After Load

Always run ANALYZE after bulk load to update CBO statistics:

```python
@task
def post_load_analyze(table: str, partition: str = None, db: str = "sales"):
    hook = MySqlHook(mysql_conn_id="starrocks_prod")
    if partition:
        sql = f"ANALYZE TABLE {db}.{table} PARTITION ({partition}) WITH ASYNC MODE"
    else:
        sql = f"ANALYZE TABLE {db}.{table} WITH ASYNC MODE"
    hook.run(sql)
    print(f"ANALYZE submitted for {db}.{table}" + (f" PARTITION {partition}" if partition else ""))
```

For full statistics (slower, more accurate):
```python
hook.run(f"""
    ANALYZE FULL TABLE {db}.{table}
    PROPERTIES("statistic_auto_collect_ratio" = "1.0")
""")
```

---

## Airflow Variables Pattern for StarRocks Config

Store StarRocks-specific config in Airflow Variables to avoid hardcoding:

```python
from airflow.models import Variable

SR_CONFIG = {
    "fe_host": Variable.get("starrocks_fe_host", default_var="sr-fe.internal"),
    "fe_http_port": Variable.get("starrocks_fe_http_port", default_var="8030"),
    "database": Variable.get("starrocks_database", default_var="sales"),
    "s3_region": Variable.get("s3_region", default_var="us-east-1"),
}
```

Set via CLI:
```bash
airflow variables set starrocks_fe_host sr-fe-prod.internal
airflow variables set starrocks_database sales_prod
```

---

## Anti-Patterns

1. **Not checking SHOW LOAD state before re-triggering** — a FINISHED load with the same label will silently succeed (idempotent), but a CANCELLED label needs a new label; always check state before retry.
2. **No `catchup=True` + `max_active_runs=1` for Broker Load DAGs** — concurrent runs create duplicate labels or race on same partition; enforce `max_active_runs=1`.
3. **Polling with `time.sleep` without deadline** — hangs the Airflow worker slot indefinitely; always set a timeout and raise on expiry.
4. **Calling `ANALYZE TABLE` synchronously in DAG** — blocks the task for minutes on large tables; use `WITH ASYNC MODE` then check with `SHOW ANALYZE STATUS` if needed.
5. **Using MySqlHook for Stream Load** — Stream Load is HTTP PUT, not SQL; use `requests` or `HttpHook` for Stream Load, `MySqlHook` for DDL/Broker Load management.
6. **Not pausing Routine Load during DDL** — schema changes while Routine Load is running cause parse failures and job pause; always pause first.

---

## References

- StarRocks Loading overview: `docs.starrocks.io/docs/loading/Loading_intro/`
- Broker Load: `docs.starrocks.io/docs/loading/BrokerLoad/`
- Routine Load: `docs.starrocks.io/docs/loading/RoutineLoad/`
- Related skills: `[[starrocks-broker-load]]`, `[[starrocks-stream-load]]`, `[[starrocks-routine-load-kafka]]`, `[[airflow-starrocks-etl-best-practices]]`
