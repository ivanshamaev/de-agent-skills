---
name: dataops-self-healing-platform
description: Self-healing data platform — auto-restart failed Airflow DAGs (idempotency required), Kafka consumer group auto-resume after partition rebalance, circuit breaker for flapping pipelines, automatic partition backfill on gap detection, auto-scale Kubernetes resources on OOM detection, stale statistics auto-ANALYZE, DQ gate auto-quarantine pattern, dead letter queue reprocessing, watchdog agents for heartbeat monitoring, self-healing Airflow sensor (RoutineLoadLagSensor pattern)
---

# Self-Healing Data Platform

## When to Use

- Reducing on-call burden by automating known recovery actions
- Handling transient infrastructure failures without human intervention
- Auto-recovering from common failure patterns (OOM, rebalance, stale stats)
- Implementing circuit breakers for flapping pipelines
- Setting up automated DQ quarantine for bad partitions

---

## Auto-Restart Failed DAG Runs

```python
# Airflow DAG: watch for failed runs and auto-restart idempotent pipelines
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from airflow.api.client.local_client import Client
from datetime import datetime, timedelta
import pendulum

ELIGIBLE_DAGS = [
    "etl_orders",
    "etl_customers",
    "etl_products",
]
MAX_AUTO_RETRIES = 3

def auto_restart_failed_dags():
    client = Client(None, None)

    for dag_id in ELIGIBLE_DAGS:
        failed_runs = DagRun.find(
            dag_id=dag_id,
            state=DagRunState.FAILED,
            execution_start_date=datetime.utcnow() - timedelta(hours=6),
        )

        for run in failed_runs:
            retry_count = int(run.conf.get("auto_retry_count", 0))
            if retry_count >= MAX_AUTO_RETRIES:
                # Circuit breaker: don't retry indefinitely, alert instead
                send_alert(f"Auto-retry limit reached for {dag_id}: {run.run_id}")
                continue

            # Clear failed tasks and re-trigger
            client.trigger_dag(
                dag_id=dag_id,
                run_id=f"auto_retry_{run.run_id}",
                conf={"auto_retry_count": retry_count + 1, "original_run_id": run.run_id},
            )

with DAG(
    dag_id="self_healing_watchdog",
    schedule="*/15 * * * *",   # every 15 minutes
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["platform", "self-healing"],
) as dag:
    PythonOperator(
        task_id="auto_restart_failed_dags",
        python_callable=auto_restart_failed_dags,
    )
```

---

## Kafka Consumer Auto-Resume

```python
# Auto-resume stuck or lagging Kafka consumers
import subprocess
import json
from datetime import datetime

CONSUMER_GROUPS = {
    "orders-processor": {
        "max_lag": 50000,
        "topics": ["orders-raw"],
        "action": "scale_up",     # scale_up | reset_offset | alert
    },
    "orders-dlq-processor": {
        "max_lag": 1000,
        "topics": ["orders-dlq"],
        "action": "alert",
    },
}

def monitor_and_heal_consumers():
    """Check consumer lag and take automated healing action."""
    for group_name, config in CONSUMER_GROUPS.items():
        lag = get_consumer_lag(group_name)

        if lag > config["max_lag"]:
            action = config["action"]

            if action == "scale_up":
                # Scale up consumer deployment
                current_replicas = get_replicas(f"deployment/{group_name}")
                new_replicas = min(current_replicas * 2, 20)
                subprocess.run([
                    "kubectl", "scale", f"deployment/{group_name}",
                    f"--replicas={new_replicas}", "-n", "kafka"
                ])
                log.info(f"Scaled {group_name} from {current_replicas} to {new_replicas} replicas")

            elif action == "reset_offset":
                # Reset to latest if lag is irrecoverable (data too old)
                subprocess.run([
                    "kafka-consumer-groups.sh",
                    "--bootstrap-server", KAFKA_BOOTSTRAP,
                    "--group", group_name,
                    "--reset-offsets", "--to-latest",
                    "--execute"
                ])
                log.warning(f"Reset offsets for {group_name} due to unrecoverable lag")

            elif action == "alert":
                send_alert(f"Consumer {group_name} lag={lag} > threshold={config['max_lag']}")
```

---

## Circuit Breaker for Flapping Pipelines

```python
import time
from enum import Enum
from dataclasses import dataclass, field

class CircuitState(Enum):
    CLOSED = "closed"       # normal operation
    OPEN = "open"           # failing, reject calls
    HALF_OPEN = "half_open" # testing recovery

@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: int = 5      # failures before opening
    recovery_timeout: int = 300     # seconds before trying again
    _failures: int = 0
    _state: CircuitState = CircuitState.CLOSED
    _opened_at: float = 0

    def call(self, func, *args, **kwargs):
        if self._state == CircuitState.OPEN:
            if time.time() - self._opened_at > self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
            else:
                raise Exception(f"Circuit {self.name} is OPEN — skipping call")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        self._failures = 0
        self._state = CircuitState.CLOSED

    def _on_failure(self):
        self._failures += 1
        if self._failures >= self.failure_threshold:
            self._state = CircuitState.OPEN
            self._opened_at = time.time()
            send_alert(f"Circuit breaker OPEN: {self.name} after {self._failures} failures")

# Usage in Airflow task
trino_circuit = CircuitBreaker("trino-etl", failure_threshold=3, recovery_timeout=300)

@task
def load_orders(ds: str):
    def _load():
        return execute_trino_insert(ds)
    return trino_circuit.call(_load)
```

---

## Gap Detection and Auto-Backfill

```python
# Detect missing partitions and trigger backfill
def detect_and_fill_gaps(dag_id: str, table: str, days_lookback: int = 7) -> list[str]:
    """Find missing partitions and trigger backfill DAG runs."""
    from airflow.api.client.local_client import Client

    # Expected partitions: every day for lookback period
    expected = set(
        str(datetime.today() - timedelta(days=i)) for i in range(1, days_lookback + 1)
    )

    # Actual partitions present in table
    present = set(
        row[0] for row in execute_query(
            f"SELECT DISTINCT CAST(order_date AS VARCHAR) FROM {table} "
            f"WHERE order_date >= CURRENT_DATE - INTERVAL '{days_lookback}' DAY"
        )
    )

    missing = expected - present

    if missing:
        client = Client(None, None)
        for partition_date in sorted(missing):
            client.trigger_dag(
                dag_id=dag_id,
                run_id=f"gap_fill_{partition_date}_{int(time.time())}",
                conf={"partition_date": partition_date, "triggered_by": "gap_detector"},
                execution_date=partition_date,
            )
        send_alert(f"Auto-triggered backfill for {len(missing)} missing partitions: {sorted(missing)}")

    return list(missing)
```

---

## Kubernetes OOM Auto-Scale

```yaml
# VPA with "Auto" mode: automatically resize OOMKilled pods
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: spark-etl-vpa
  namespace: spark
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: spark-driver
  updatePolicy:
    updateMode: "Auto"          # restart pods with new resource requests
  resourcePolicy:
    containerPolicies:
      - containerName: spark
        minAllowed:
          memory: 2Gi
        maxAllowed:
          memory: 32Gi         # cap to prevent runaway scaling
        controlledResources: ["memory"]
```

```python
# Python: detect OOMKilled pods and immediately upsize
def heal_oomkilled_pods(namespace: str = "airflow"):
    pods = get_pods(namespace)
    for pod in pods:
        for container in pod.get("status", {}).get("containerStatuses", []):
            last_state = container.get("lastState", {}).get("terminated", {})
            if last_state.get("reason") == "OOMKilled":
                current_memory = get_memory_limit(pod, container["name"])
                new_memory = increase_by_50_pct(current_memory)
                patch_deployment_memory(
                    pod["metadata"]["labels"].get("app"),
                    container["name"],
                    new_memory,
                    namespace,
                )
                log.warning(f"Auto-scaled memory for {pod['metadata']['name']}: {current_memory} → {new_memory}")
```

---

## DQ Auto-Quarantine

```python
# Automatically quarantine bad partitions that fail DQ checks
@task
def dq_gate_with_quarantine(ds: str, table: str, checks: dict) -> bool:
    """Run DQ checks; on failure, quarantine partition instead of failing pipeline."""
    results = run_dq_checks(table, ds, checks)

    if results["status"] == "FAIL":
        severity = results.get("severity", "soft")

        if severity == "hard":
            # Hard failure: alert and stop pipeline
            raise ValueError(f"DQ hard failure for {table} partition {ds}: {results['failures']}")

        else:
            # Soft failure: quarantine and continue pipeline
            quarantine_partition(
                source_table=table,
                partition_date=ds,
                quarantine_table=f"quarantine.{table.split('.')[-1]}",
                reason=json.dumps(results["failures"]),
            )
            log.warning(f"Quarantined partition {ds} for {table}: {results['failures']}")
            # Return False to skip downstream tasks but not fail pipeline
            return False

    return True

def quarantine_partition(source_table, partition_date, quarantine_table, reason):
    execute_query(f"""
        INSERT INTO {quarantine_table}
        SELECT *, '{reason}' AS quarantine_reason, CURRENT_TIMESTAMP AS quarantined_at
        FROM {source_table}
        WHERE order_date = '{partition_date}'
    """)
    execute_query(f"""
        DELETE FROM {source_table}
        WHERE order_date = '{partition_date}'
    """)
```

---

## Watchdog Heartbeat Monitor

```python
# Watchdog: if a pipeline hasn't run in expected window, trigger it
PIPELINE_HEARTBEATS = {
    "etl_orders": {"max_gap_minutes": 60, "cron": "0 2 * * *"},
    "kafka_consumer_metrics": {"max_gap_minutes": 10, "cron": "*/10 * * * *"},
}

def watchdog_check():
    for dag_id, config in PIPELINE_HEARTBEATS.items():
        last_success = get_last_successful_run(dag_id)
        gap_minutes = (datetime.utcnow() - last_success).total_seconds() / 60

        if gap_minutes > config["max_gap_minutes"]:
            log.error(f"Watchdog: {dag_id} hasn't run in {gap_minutes:.0f} min (threshold: {config['max_gap_minutes']})")
            send_alert(f"⚠️ Pipeline watchdog: {dag_id} overdue by {gap_minutes - config['max_gap_minutes']:.0f} min")
            trigger_dag(dag_id, run_id=f"watchdog_{int(time.time())}")
```

---

## Self-Healing Checklist

```
[ ] Critical DAGs have auto-restart with MAX_AUTO_RETRIES circuit breaker
[ ] Kafka consumers have lag monitoring + auto-scale action
[ ] Kubernetes VPA configured for OOM-prone pods
[ ] Gap detector runs daily to find and backfill missing partitions
[ ] DQ soft failures quarantine partition instead of failing pipeline
[ ] Watchdog heartbeat monitor for all critical pipelines
[ ] All self-healing actions are logged with reason and action taken
[ ] Circuit breaker prevents runaway retry storms
[ ] Self-healing actions send to #data-eng-healings Slack channel
[ ] Monthly review: which auto-heals recurred? (indicates underlying bug)
```

---

## Anti-Patterns

1. **Auto-retry without idempotency** — retrying a non-idempotent task doubles the data; ensure INSERT OVERWRITE before enabling auto-restart.
2. **No circuit breaker on auto-restart** — a DAG with a code bug restarts 50 times before someone notices; set MAX_AUTO_RETRIES=3.
3. **Self-healing without logging** — auto-actions without logs make incidents harder to debug; log every healing action with reason, timestamp, and action taken.
4. **Offset reset without alerting** — silently resetting consumer offsets to latest discards unprocessed messages; always alert when resetting.
5. **VPA in Auto mode on stateful pods** — VPA restarts pods to resize; databases and Kafka brokers can't be restarted arbitrarily; use `updateMode: "Off"` for stateful workloads.

---

## References

- Airflow DAG trigger API: `airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html`
- VPA: `github.com/kubernetes/autoscaler/tree/master/vertical-pod-autoscaler`
- Circuit breaker pattern: `martinfowler.com/bliki/CircuitBreaker.html`
- Related skills: `[[starrocks-self-healing]]`, `[[dataops-root-cause-analysis]]`, `[[dataops-airflow-production-readiness]]`
