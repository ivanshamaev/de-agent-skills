---
name: dataops-airflow-observability
description: Airflow observability — StatsD/OpenTelemetry metrics (scheduler_heartbeat/task_duration/pool_open_slots/dag_processing_total_parse_time), Prometheus scraping, Grafana dashboards (DAG success rate/task duration/slot utilization), structured task logging (JSON formatter), OpenTelemetry traces for task spans, DAG SLA miss alerts, anomaly detection on task duration, Airflow audit log, DagBag parse error monitoring, dead letter queue for failed tasks
---

# Airflow Observability

## When to Use

- Setting up visibility into Airflow scheduler and task performance
- Building Grafana dashboards for a data platform overview
- Diagnosing why tasks are slow or queuing
- Alerting on SLA misses and error spikes
- Tracing a task failure through its log chain

---

## Metrics Collection Setup

### StatsD + Prometheus

```ini
# airflow.cfg
[metrics]
statsd_on = True
statsd_host = statsd-exporter.monitoring.svc.cluster.local
statsd_port = 8125
statsd_prefix = airflow
statsd_allow_list = scheduler,executor,pool,dag,task  # allowlist prefixes
```

```yaml
# statsd-exporter mapping (converts StatsD to Prometheus labels)
# statsd_exporter_config.yaml
mappings:
  # dag.etl_orders.task.load_orders.duration
  - match: "airflow.dag.*.task.*.duration"
    name: "airflow_task_duration_seconds"
    labels:
      dag_id: "$1"
      task_id: "$2"

  # dag.etl_orders.task.load_orders.failures
  - match: "airflow.dag.*.task.*.failures"
    name: "airflow_task_failures_total"
    labels:
      dag_id: "$1"
      task_id: "$2"

  # pool.open_slots.default_pool
  - match: "airflow.pool.open_slots.*"
    name: "airflow_pool_open_slots"
    labels:
      pool_name: "$1"

  # scheduler.tasks.starving
  - match: "airflow.scheduler.tasks.starving"
    name: "airflow_scheduler_tasks_starving"
```

### OpenTelemetry (Airflow 2.7+)

```ini
# airflow.cfg
[metrics]
otel_on = True
otel_host = otel-collector.monitoring.svc.cluster.local
otel_port = 4318
otel_prefix = airflow
otel_interval_milliseconds = 30000
```

```yaml
# OpenTelemetry Collector config
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

exporters:
  prometheus:
    endpoint: "0.0.0.0:9090"
  jaeger:
    endpoint: jaeger-collector:14250

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
    traces:
      receivers: [otlp]
      exporters: [jaeger]
```

---

## Key Metrics to Monitor

### Prometheus Queries

```promql
# Task failure rate (last 1 hour, by DAG)
sum(increase(airflow_task_failures_total[1h])) by (dag_id)

# Task success rate percentage
100 * sum(increase(airflow_task_success_total[1h])) by (dag_id) /
(sum(increase(airflow_task_success_total[1h])) by (dag_id) +
 sum(increase(airflow_task_failures_total[1h])) by (dag_id))

# P95 task duration by task_id
histogram_quantile(0.95,
  sum(rate(airflow_task_duration_seconds_bucket[1h])) by (le, dag_id, task_id)
)

# Pool slot utilization
1 - (airflow_pool_open_slots / airflow_pool_total_slots)

# Scheduler heartbeat age (seconds since last heartbeat)
time() - airflow_scheduler_last_scheduler_run

# DAG parse time
airflow_dag_processing_total_parse_time

# Queued tasks (tasks waiting for execution)
airflow_executor_queued_tasks

# Running tasks
airflow_executor_running_tasks
```

---

## Grafana Dashboard Panels

```json
// Grafana dashboard JSON snippet for key panels

// Panel 1: Task Success/Failure Rate
{
  "title": "Task outcomes (last 24h)",
  "type": "timeseries",
  "targets": [
    {
      "expr": "sum(increase(airflow_task_success_total[5m])) by (dag_id)",
      "legendFormat": "success - {{dag_id}}"
    },
    {
      "expr": "sum(increase(airflow_task_failures_total[5m])) by (dag_id)",
      "legendFormat": "failure - {{dag_id}}"
    }
  ]
}
```

```yaml
# Grafana dashboard provisioning (deploy via ConfigMap)
apiVersion: v1
kind: ConfigMap
metadata:
  name: airflow-dashboard
  namespace: monitoring
  labels:
    grafana_dashboard: "1"
data:
  airflow-overview.json: |
    {
      "title": "Airflow Overview",
      "panels": [...]
    }
```

---

## Structured Task Logging

```python
# airflow.cfg: structured JSON logs
[logging]
log_format = {"time": "%(asctime)s", "level": "%(levelname)s", "dag": "%(dag_id)s", "task": "%(task_id)s", "run": "%(run_id)s", "message": "%(message)s"}

# Or configure programmatically
import logging
import json

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "dag_id": getattr(record, "dag_id", None),
            "task_id": getattr(record, "task_id", None),
            "run_id": getattr(record, "run_id", None),
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)
```

```python
# In a task: use structured log fields
@task
def load_orders(ds: str):
    import logging
    log = logging.getLogger(__name__)
    log.info("Loading orders", extra={
        "partition_date": ds,
        "source_table": "bronze.orders_raw",
        "target_table": "silver.orders",
    })
    rows = execute_query(...)
    log.info("Load complete", extra={"rows_loaded": rows, "partition_date": ds})
```

---

## Alert Rules

```yaml
# Prometheus alerting rules
groups:
  - name: airflow
    rules:

    - alert: AirflowSchedulerHeartbeatStale
      expr: (time() - airflow_scheduler_last_scheduler_run) > 120
      for: 2m
      labels:
        severity: critical
        team: data-engineering
      annotations:
        summary: "Airflow scheduler not heartbeating"
        runbook: "https://wiki.my-org.com/runbooks/airflow-scheduler-down"

    - alert: AirflowTaskFailureSpike
      expr: |
        sum(increase(airflow_task_failures_total[15m])) by (dag_id) > 5
      for: 5m
      labels:
        severity: warning
      annotations:
        summary: "High task failure rate for DAG {{ $labels.dag_id }}"

    - alert: AirflowPoolStarved
      expr: airflow_pool_open_slots{pool_name="default_pool"} == 0
      for: 10m
      labels:
        severity: warning
      annotations:
        summary: "Airflow default pool fully occupied for 10 min"

    - alert: AirflowSLAMiss
      expr: increase(airflow_sla_missed_total[1h]) > 0
      labels:
        severity: warning
      annotations:
        summary: "SLA missed for DAG {{ $labels.dag_id }}"

    - alert: AirflowDagParseError
      expr: airflow_dag_processing_import_errors > 0
      labels:
        severity: warning
      annotations:
        summary: "DAG import errors detected"
```

---

## DAG Run Audit Queries

```sql
-- Failed DAG runs in last 24 hours
SELECT dag_id, run_id, state, start_date, end_date,
       EXTRACT(EPOCH FROM (end_date - start_date)) / 60 AS duration_min
FROM dag_run
WHERE state = 'failed'
  AND start_date > NOW() - INTERVAL '24 hours'
ORDER BY start_date DESC;

-- Tasks taking > 2x their historical average
WITH task_stats AS (
    SELECT dag_id, task_id,
           AVG(EXTRACT(EPOCH FROM (end_date - start_date))) AS avg_duration_sec,
           STDDEV(EXTRACT(EPOCH FROM (end_date - start_date))) AS std_duration_sec
    FROM task_instance
    WHERE state = 'success'
      AND start_date > NOW() - INTERVAL '30 days'
    GROUP BY dag_id, task_id
),
recent_tasks AS (
    SELECT dag_id, task_id, run_id,
           EXTRACT(EPOCH FROM (end_date - start_date)) AS duration_sec
    FROM task_instance
    WHERE state = 'success'
      AND start_date > NOW() - INTERVAL '24 hours'
)
SELECT r.dag_id, r.task_id, r.run_id,
       ROUND(r.duration_sec / 60, 1) AS duration_min,
       ROUND(s.avg_duration_sec / 60, 1) AS avg_duration_min,
       ROUND(r.duration_sec / NULLIF(s.avg_duration_sec, 0), 2) AS slowdown_ratio
FROM recent_tasks r
JOIN task_stats s USING (dag_id, task_id)
WHERE r.duration_sec > 2 * s.avg_duration_sec
ORDER BY slowdown_ratio DESC
LIMIT 20;

-- DagBag import errors
SELECT dag_id, last_parsed_time, last_pickled, fileloc
FROM dag
WHERE is_paused = FALSE AND is_active = FALSE
ORDER BY last_parsed_time DESC;
```

---

## Observability Checklist

```
[ ] StatsD or OTEL metrics enabled in airflow.cfg
[ ] StatsD exporter → Prometheus scrape configured
[ ] Grafana dashboard with: task success/failure, duration, pool slots, scheduler heartbeat
[ ] Alert: scheduler heartbeat stale (> 2 min)
[ ] Alert: task failure spike (> 5 failures in 15 min)
[ ] Alert: pool starved (0 open slots for > 10 min)
[ ] Alert: SLA miss callback + Prometheus counter
[ ] Structured JSON log format enabled
[ ] Remote log storage (S3/GCS) — not local pods
[ ] DAG parse error alert (airflow_dag_processing_import_errors > 0)
[ ] Weekly SLA report generated from metadata DB
```

---

## Anti-Patterns

1. **No metrics backend configured** — default Airflow has no metrics; scheduler hangs go undetected for hours; always configure StatsD or OTEL.
2. **Alerting only on task failures** — pool starvation and scheduler heartbeat issues prevent tasks from running without any failures visible; monitor queue depth and heartbeat too.
3. **Logs only on local pods** — pod replacement during upgrade or OOM wipes all logs; configure remote logging from day one.
4. **No SLA configured** — pipeline latency issues detected only by downstream consumers; set SLA on every critical DAG.
5. **Using print() instead of logging** — print output isn't captured by Airflow log viewer with structured context; always use `logging.getLogger(__name__)`.

---

## References

- Airflow metrics: `airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html`
- StatsD exporter: `github.com/prometheus/statsd_exporter`
- OpenTelemetry: `opentelemetry.io`
- Related skills: `[[dataops-airflow-production-readiness]]`, `[[dataops-airflow-ha-review]]`, `[[infra-prometheus-optimization]]`
