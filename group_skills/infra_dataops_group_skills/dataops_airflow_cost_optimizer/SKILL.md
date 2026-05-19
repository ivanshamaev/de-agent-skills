---
name: dataops-airflow-cost-optimizer
description: Airflow cost optimization — KubernetesPodOperator right-sizing (request vs actual CPU/memory), spot node tolerations for batch tasks, task consolidation (reduce pod-per-task overhead), pool-based concurrency control, idle worker cleanup, log retention policy (S3 lifecycle), metadata DB right-sizing, CeleryExecutor worker autoscaling (KEDA Kafka/queue depth), DAG run frequency analysis (oversceduled DAGs), cloud cost attribution per DAG
---

# Airflow Cost Optimizer

## When to Use

- Airflow cloud bill growing disproportionately to pipeline count
- KubernetesExecutor creating excessive pods for small tasks
- Workers over-provisioned with unused CPU/memory
- Log storage accumulating without retention policy
- Teams running redundant pipelines without visibility

---

## KubernetesPodOperator Right-Sizing

```python
# Step 1: Identify tasks with large resource requests vs actual usage
# Check actual usage in Grafana: container_cpu_usage_seconds_total{pod=~"airflow-.*"}

# ❌ Over-provisioned — requesting 4 CPU for a task that uses 200m
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

over_provisioned = KubernetesPodOperator(
    task_id="transform",
    image="my-etl:latest",
    container_resources=k8s.V1ResourceRequirements(
        requests={"cpu": "4", "memory": "16Gi"},    # wastes 3.8 CPU
        limits={"cpu": "4", "memory": "16Gi"},
    ),
)

# ✅ Right-sized: P95 actual + 20% headroom
right_sized = KubernetesPodOperator(
    task_id="transform",
    image="my-etl:latest",
    container_resources=k8s.V1ResourceRequirements(
        requests={"cpu": "250m", "memory": "512Mi"},  # P95 actual
        limits={"cpu": "1", "memory": "2Gi"},          # burst headroom
    ),
)
```

```bash
# Query P95 CPU and memory for each airflow pod in last 7 days
kubectl top pods -n airflow --sort-by=cpu

# Prometheus: P95 CPU per airflow worker pod
quantile_over_time(0.95,
  rate(container_cpu_usage_seconds_total{
    namespace="airflow",
    pod=~"airflow-run-.*"
  }[5m])[7d:5m]
) * 1000  # millicores
```

---

## Spot Nodes for Batch Tasks

```python
# Tolerate spot interruption for non-critical batch tasks
from kubernetes.client import models as k8s

spot_tolerations = [
    k8s.V1Toleration(
        key="cloud.google.com/gke-spot",
        operator="Equal",
        value="true",
        effect="NoSchedule",
    )
]

spot_affinity = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(
        preferred_during_scheduling_ignored_during_execution=[
            k8s.V1PreferredSchedulingTerm(
                weight=100,
                preference=k8s.V1NodeSelectorTerm(
                    match_expressions=[
                        k8s.V1NodeSelectorRequirement(
                            key="cloud.google.com/gke-spot",
                            operator="In",
                            values=["true"],
                        )
                    ]
                ),
            )
        ]
    )
)

batch_task = KubernetesPodOperator(
    task_id="heavy_batch_transform",
    image="spark-etl:latest",
    tolerations=spot_tolerations,
    affinity=spot_affinity,
    is_delete_operator_pod=True,
    # Handle SIGTERM from spot preemption
    termination_grace_period=120,
)
```

---

## Task Consolidation

```python
# ❌ Expensive: 10 separate pods for small tasks (10x pod startup overhead)
for table in TABLES:
    KubernetesPodOperator(
        task_id=f"load_{table}",
        image="loader:latest",
        arguments=["--table", table],
    )

# ✅ Cheaper: 1 pod that processes all tables sequentially
KubernetesPodOperator(
    task_id="load_all_tables",
    image="loader:latest",
    arguments=["--tables", ",".join(TABLES)],
)

# Or: TaskFlow with PythonOperator (no pod overhead)
@task
def load_tables(tables: list[str], ds: str):
    for table in tables:
        load_table(table, ds)
```

---

## CeleryExecutor Autoscaling with KEDA

```yaml
# KEDA ScaledObject for Celery workers
# Scale based on Celery queue depth (Redis)
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: airflow-worker
  namespace: airflow
spec:
  scaleTargetRef:
    name: airflow-worker
  pollingInterval: 15
  cooldownPeriod: 60
  minReplicaCount: 1        # always keep 1 worker (fast startup)
  maxReplicaCount: 20
  triggers:
  - type: redis
    metadata:
      address: redis-master.airflow.svc.cluster.local:6379
      listName: celery      # Celery uses a Redis list as queue
      listLength: "5"       # scale up when queue > 5 tasks per worker
```

---

## Log Retention Policy

```hcl
# Terraform: S3 lifecycle for Airflow logs
resource "aws_s3_bucket_lifecycle_configuration" "airflow_logs" {
  bucket = aws_s3_bucket.airflow_logs.id

  rule {
    id     = "task-logs-retention"
    status = "Enabled"

    filter { prefix = "task-logs/" }

    # Move to infrequent access after 30 days
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    # Delete after 90 days
    expiration {
      days = 90
    }
  }
}
```

```bash
# Check current log storage size
aws s3 ls s3://my-airflow-logs/task-logs/ --recursive --human-readable --summarize \
  | grep "Total Size"

# Find oldest logs
aws s3 ls s3://my-airflow-logs/task-logs/ | sort | head -20
```

---

## Metadata DB Right-Sizing

```sql
-- Identify table sizes driving DB storage cost
SELECT
    relname AS table_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    pg_size_pretty(pg_relation_size(relid)) AS data_size,
    n_live_tup AS live_rows
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC
LIMIT 10;

-- Overly frequent cleanup: remove task instances older than 90 days
DELETE FROM task_instance
WHERE start_date < NOW() - INTERVAL '90 days'
  AND state IN ('success', 'failed', 'skipped');

-- Remove XCom values older than 7 days (these can be large)
DELETE FROM xcom
WHERE timestamp < NOW() - INTERVAL '7 days';
```

---

## Over-Scheduled DAG Detection

```sql
-- DAGs that run more frequently than they need to
SELECT
    dag_id,
    schedule_interval,
    COUNT(*) AS run_count,
    AVG(EXTRACT(EPOCH FROM (end_date - start_date)) / 60) AS avg_duration_min,
    COUNT(*) * AVG(EXTRACT(EPOCH FROM (end_date - start_date)) / 60) AS total_compute_min
FROM dag_run
WHERE state = 'success'
  AND start_date > NOW() - INTERVAL '7 days'
GROUP BY dag_id, schedule_interval
HAVING schedule_interval IN ('* * * * *', '*/5 * * * *', '*/15 * * * *')  -- frequent schedules
ORDER BY total_compute_min DESC
LIMIT 20;
```

---

## Cost Attribution by DAG

```python
# Estimate Kubernetes compute cost per DAG
# (requires pod labels with dag_id)
import subprocess, json

def get_pod_costs_by_dag(namespace: str = "airflow") -> dict:
    """Estimate monthly cost per DAG based on pod CPU*seconds."""
    result = subprocess.run(
        ["kubectl", "get", "pods", "-n", namespace,
         "-l", "dag_id", "-o", "json"],
        capture_output=True, text=True
    )
    pods = json.loads(result.stdout)

    costs = {}
    CPU_COST_PER_CORE_HOUR = 0.048  # $/core-hour (e.g., GKE n2-standard-4)

    for pod in pods["items"]:
        dag_id = pod["metadata"]["labels"].get("dag_id", "unknown")
        cpu_req = pod["spec"]["containers"][0]["resources"].get("requests", {}).get("cpu", "100m")
        cpu_cores = float(cpu_req.rstrip("m")) / 1000 if cpu_req.endswith("m") else float(cpu_req)

        if dag_id not in costs:
            costs[dag_id] = 0
        costs[dag_id] += cpu_cores * CPU_COST_PER_CORE_HOUR

    return dict(sorted(costs.items(), key=lambda x: -x[1]))
```

---

## Cost Optimization Checklist

```
[ ] KubernetesPodOperator resource requests match P95 actual usage
[ ] Batch tasks run on spot nodes with preemption handling
[ ] Small tasks consolidated (< 30s tasks use PythonOperator, not KPO)
[ ] KEDA autoscaling on Celery workers (no idle workers overnight)
[ ] S3 log lifecycle: IA after 30d, delete after 90d
[ ] Metadata DB cleaned monthly (task_instance, log, xcom)
[ ] XCom large values stored externally (S3), not in DB
[ ] Over-scheduled DAGs identified and frequency reduced
[ ] Dev/staging Airflow scaled to 0 overnight (CronJob or KEDA cron)
[ ] Cost attribution labels (dag_id, team) on all KPO pods
```

---

## Anti-Patterns

1. **One pod per small task** — KubernetesExecutor starts a pod for every task, including 5-second SQL checks; consolidate small tasks into PythonOperator.
2. **Workers running 24/7 in dev** — dev workers sit idle 14+ hours/day; use KEDA with minReplicas=0 for non-prod.
3. **No XCom size limits** — XCom stores data in metadata DB; large XComs (DataFrames, lists) bloat the DB and slow scheduler; use S3 for data > 1MB.
4. **Log retention at never-delete** — Airflow logs grow 1-2 GB/day for active clusters; without lifecycle rules, S3 cost accumulates indefinitely.
5. **Over-scheduling health-check DAGs** — a DAG running every minute to "check freshness" costs 1440 pod starts/day; use sensors or reduce frequency to every 15 min.

---

## References

- Airflow KubernetesExecutor: `airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/kubernetes.html`
- KEDA Redis scaler: `keda.sh/docs/2.13/scalers/redis-lists/`
- S3 lifecycle: `docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html`
- Related skills: `[[dataops-airflow-production-readiness]]`, `[[infra-kubernetes-cost-optimizer]]`, `[[de-cost-optimization]]`
