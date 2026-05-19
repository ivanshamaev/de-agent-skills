---
name: dataops-airflow-production-readiness
description: Airflow production readiness — idempotent tasks (UPSERT/partition overwrite), no top-level code at DAG parse time (Variable.get inside tasks), KubernetesExecutor configuration, connection/variable management (no hardcoded secrets), SLA callbacks, retry with exponential backoff, pool management, max_active_runs, dag_concurrency, celery vs kubernetes executor trade-offs, metadata DB maintenance (airflow db clean), health check endpoints, structured logging
---

# Airflow Production Readiness

## When to Use

- Preparing an Airflow deployment for production
- Auditing an existing Airflow installation before a major version upgrade
- Diagnosing scheduler performance issues or slow DAG parsing
- Reviewing new DAG code for production anti-patterns
- Setting up Airflow on Kubernetes with proper resource isolation

---

## Idempotent Task Design

```python
# ✅ Idempotent: INSERT OVERWRITE / UPSERT — safe to re-run
@task
def load_orders(ds: str):
    conn = get_trino_connection()
    conn.execute(f"""
        INSERT OVERWRITE INTO silver.orders
        PARTITION (order_date = '{ds}')
        SELECT * FROM bronze.orders_raw
        WHERE order_date = '{ds}'
    """)

# ❌ Not idempotent: appends rows on every run
@task
def load_orders_bad(ds: str):
    conn.execute(f"""
        INSERT INTO silver.orders
        SELECT * FROM bronze.orders_raw WHERE order_date = '{ds}'
    """)
```

---

## No Top-Level Code (DAG Parse Safety)

```python
# ❌ BAD: Variable.get() called at parse time — hammers metadata DB
MY_BUCKET = Variable.get("s3_data_lake_bucket")

with DAG("etl_orders", ...) as dag:
    ...

# ✅ GOOD: Variable accessed inside task (lazy evaluation)
with DAG("etl_orders", ...) as dag:
    @task
    def run_etl():
        bucket = Variable.get("s3_data_lake_bucket")   # called at execution time
        ...

# ✅ GOOD: Use Jinja template (resolves at task runtime, not parse time)
with DAG("etl_orders", ...) as dag:
    BashOperator(
        task_id="sync_s3",
        bash_command="aws s3 sync /tmp/output {{ var.value.s3_data_lake_bucket }}/output/",
    )

# ❌ BAD: expensive import at module level
import pandas as pd     # always imported even for unrelated DAGs
import torch            # slow, slows every dag parse

# ✅ GOOD: import inside task function
@task
def transform():
    import pandas as pd  # only imported when task runs
    ...
```

---

## Connection and Secret Management

```python
# ✅ Use Airflow Connections for credentials
from airflow.hooks.base import BaseHook

@task
def query_trino():
    conn = BaseHook.get_connection("trino_production")
    # conn.host, conn.login, conn.password, conn.port
    ...

# ✅ Use Secret Backends (Vault, AWS SSM)
# airflow.cfg:
# [secrets]
# backend = airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend
# backend_kwargs = {"connections_prefix": "/airflow/connections", "variables_prefix": "/airflow/variables"}

# ❌ Never hardcode in DAG files:
# DB_PASSWORD = "s3cr3t"
```

---

## Retry and SLA Configuration

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

with DAG(
    dag_id="etl_orders",
    start_date=days_ago(1),
    schedule="0 2 * * *",
    max_active_runs=1,          # prevent concurrent runs of same DAG
    catchup=False,              # don't backfill on deploy
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(hours=1),
        "on_failure_callback": notify_slack_on_failure,
        "sla": timedelta(hours=2),          # SLA per task
    },
    sla_miss_callback=sla_miss_handler,     # DAG-level SLA
    tags=["orders", "etl"],
) as dag:
    ...


def sla_miss_handler(dag, task_list, blocking_task_list, slas, blocking_tis):
    message = f"SLA missed for DAG {dag.dag_id}\n" \
              f"Tasks: {[t.task_id for t in task_list]}"
    send_slack_alert(channel="#data-sla-alerts", message=message)
```

---

## KubernetesExecutor Configuration

```yaml
# values.yaml (Airflow Helm chart)
executor: KubernetesExecutor

workers:
  resources:
    requests:
      cpu: 500m
      memory: 1Gi
    limits:
      cpu: 2
      memory: 4Gi

  podAnnotations:
    cluster-autoscaler.kubernetes.io/safe-to-evict: "false"

# Pod template for workers
workerPodTemplate: |
  apiVersion: v1
  kind: Pod
  spec:
    containers:
    - name: base
      resources:
        requests:
          cpu: 500m
          memory: 1Gi
    serviceAccountName: airflow-worker
    tolerations:
    - key: spot
      operator: Exists
      effect: NoSchedule
```

```python
# Per-task resource override
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

KubernetesPodOperator(
    task_id="spark_submit",
    image="apache/spark:3.5.0",
    cmds=["spark-submit", "--master", "k8s://https://kubernetes.default.svc"],
    resources=k8s.V1ResourceRequirements(
        requests={"cpu": "2", "memory": "8Gi"},
        limits={"cpu": "4", "memory": "16Gi"},
    ),
    is_delete_operator_pod=True,
)
```

---

## Pool Management

```python
# Limit concurrent database-heavy tasks
# Create pool in Airflow UI or CLI:
# airflow pools set db_intensive_pool 10 "Pool for DB-heavy tasks"

from airflow.operators.python import PythonOperator

load_task = PythonOperator(
    task_id="load_to_warehouse",
    python_callable=load_fn,
    pool="db_intensive_pool",   # max 10 concurrent across all DAGs
    pool_slots=2,               # this task consumes 2 slots
)
```

---

## Scheduler Configuration (airflow.cfg)

```ini
[scheduler]
min_file_process_interval = 30   # re-parse DAG files every 30s (default: 30)
dag_dir_list_interval = 60       # scan dag folder every 60s
max_dagruns_to_create_per_loop = 10
max_tis_per_query = 512

[core]
parallelism = 256                # max concurrent task instances across cluster
dag_concurrency = 32             # max tasks per DAG running concurrently
max_active_tasks_per_dag = 32
max_active_runs_per_dag = 5

[kubernetes_executor]
worker_pods_creation_batch_size = 16   # create up to 16 pods per scheduler loop
delete_worker_pods = True
```

---

## Metadata DB Maintenance

```bash
# Clean old task instances, logs, dag runs (run monthly)
airflow db clean \
  --clean-before-timestamp "$(date -d '90 days ago' --utc +%Y-%m-%dT%H:%M:%S)" \
  --tables dag_run,task_instance,log,job,xcom \
  --yes

# Upgrade DB schema after Airflow version upgrade
airflow db upgrade

# Check metadata DB size (PostgreSQL)
psql $AIRFLOW_DB_CONN -c "
  SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS size
  FROM pg_catalog.pg_statio_user_tables
  ORDER BY pg_total_relation_size(relid) DESC
  LIMIT 10;
"
```

---

## Health Check Endpoints

```bash
# Airflow health endpoint (liveness probe)
curl http://airflow-webserver:8080/health
# Returns: {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy", "latest_scheduler_heartbeat": "..."}}

# Kubernetes liveness probe
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 60
  failureThreshold: 5
```

---

## Production Readiness Checklist

```
[ ] All tasks idempotent (no plain INSERT — use UPSERT/INSERT OVERWRITE)
[ ] No Variable.get() / Connection access at DAG parse time
[ ] No heavy imports at module level
[ ] Retries configured with exponential backoff
[ ] SLA callbacks configured for critical DAGs
[ ] max_active_runs=1 for non-idempotent or resource-heavy DAGs
[ ] catchup=False unless backfill is explicitly needed
[ ] Connections stored in Airflow (not env vars or DAG code)
[ ] Pools configured for DB-intensive and API-rate-limited tasks
[ ] KubernetesExecutor pod resource limits set (no unbounded pods)
[ ] Metadata DB cleaned monthly
[ ] Liveness/readiness probes configured on all Airflow components
[ ] Alert rules: scheduler heartbeat, task failure rate, SLA miss
```

---

## Anti-Patterns

1. **`Variable.get()` at DAG parse level** — each scheduler parse hits the metadata DB; called hundreds of times per minute at scale; use Jinja templates or access inside tasks.
2. **`catchup=True` with no partition filter** — DAG backfills years of data on first deploy; always set `catchup=False` unless backfill is required.
3. **No `max_active_runs` limit** — parallel DAG runs compete for DB connections and warehouse resources; set `max_active_runs=1` for heavy ETL.
4. **No pools for rate-limited external APIs** — all tasks hit the API simultaneously and trigger rate limits; use pools to cap concurrency.
5. **Storing secrets in DAG default_args** — appears in rendered templates and logs; always use Airflow Connections.
6. **Not cleaning the metadata DB** — the `log` and `task_instance` tables grow unboundedly; schedule monthly `airflow db clean`.

---

## References

- Airflow best practices: `airflow.apache.org/docs/apache-airflow/stable/best-practices.html`
- Airflow Helm chart: `airflow.apache.org/docs/helm-chart/stable/`
- KubernetesExecutor: `airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/kubernetes.html`
- Related skills: `[[airflow-dags]]`, `[[dataops-airflow-ha-review]]`, `[[dataops-airflow-observability]]`
