---
name: infra-prometheus-optimization
description: Prometheus optimization — recording rules (level:metric:operations naming), cardinality explosion diagnosis (tsdb status API), scrape interval tuning, remote write configuration (batch size/queue capacity), Thanos/Mimir for long-term retention, federation vs remote write, AlertManager routing trees (inhibition/silencing), absent() alerts for missing series, relabeling to drop expensive labels, TSDB compaction, ServiceMonitor vs PodMonitor patterns
---

# Prometheus Optimization

## When to Use

- Prometheus OOM-ing or consuming excessive memory
- Query performance is poor (dashboards timing out)
- Remote write queue is backing up
- Setting up long-term metric retention (> 15 days)
- Reducing alert noise while keeping critical alerts sharp

---

## Cardinality Diagnosis

```bash
# Check top metrics by series count (TSDB Status API)
curl -s http://prometheus:9090/api/v1/status/tsdb | jq '
  .data.seriesCountByMetricName[:20]'

# Top label names by series count
curl -s http://prometheus:9090/api/v1/status/tsdb | jq '
  .data.seriesCountByLabelValuePair[:20]'

# Find a specific high-cardinality metric
curl -s "http://prometheus:9090/api/v1/query?query=count({__name__='http_requests_total'})%20by%20(user_id)" \
  | jq '.data.result | length'

# Total active series
curl -s "http://prometheus:9090/api/v1/query?query=prometheus_tsdb_head_series" \
  | jq '.data.result[0].value[1]'
```

### Drop High-Cardinality Labels via Relabeling

```yaml
# scrape_config relabeling: drop user_id before storing
scrape_configs:
  - job_name: api-server
    static_configs:
      - targets: [api:9090]
    metric_relabel_configs:
      # Drop high-cardinality label
      - source_labels: [user_id]
        action: labeldrop

      # Drop metrics you don't need
      - source_labels: [__name__]
        regex: "go_gc_.*|go_memstats_.*"
        action: drop

      # Keep only critical HTTP paths
      - source_labels: [__name__, path]
        regex: 'http_requests_total;/internal/.*'
        action: drop
```

---

## Recording Rules (level:metric:operations)

```yaml
groups:
  - name: data_platform_recordings
    interval: 1m     # evaluate every minute
    rules:

    # Naming: level:metric:operations
    # job:airflow_task_failures:rate5m
    - record: job:airflow_task_failures:rate5m
      expr: |
        sum without (instance, pod) (
          rate(airflow_task_failures_total[5m])
        )

    # job_dag:airflow_task_success:rate1h — per-dag aggregation
    - record: job_dag:airflow_task_success:rate1h
      expr: |
        sum without (instance, pod) (
          rate(airflow_task_success_total[1h])
        )

    # Precompute expensive pipeline health ratio
    - record: job_dag:pipeline_error_rate:ratio1h
      expr: |
        job_dag:airflow_task_failures:rate1h
        /
        (job_dag:airflow_task_success:rate1h + job_dag:airflow_task_failures:rate1h)

    # Kafka lag by consumer group
    - record: job_consumergroup:kafka_consumer_lag:sum
      expr: |
        sum without (partition, topic_partition) (
          kafka_consumergroup_lag
        )
```

---

## AlertManager Routing and Inhibition

```yaml
# alertmanager.yml
global:
  slack_api_url: ${SLACK_WEBHOOK_URL}
  pagerduty_url: https://events.pagerduty.com/v2/enqueue

route:
  receiver: slack-warning
  group_by: [alertname, dag_id, namespace]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h

  routes:
    # CRITICAL → PagerDuty (immediately)
    - match:
        severity: critical
      receiver: pagerduty-critical
      group_wait: 10s
      repeat_interval: 1h

    # Data platform alerts → dedicated channel
    - match_re:
        alertname: "Airflow.*|Pipeline.*|Kafka.*"
      receiver: slack-data-eng
      continue: true

inhibit_rules:
  # If node is down, don't fire pod/container alerts on same node
  - source_match:
      alertname: NodeDown
    target_match_re:
      alertname: "KubePod.*|Container.*"
    equal: [node]

  # If scheduler is down, suppress all task-level alerts
  - source_match:
      alertname: AirflowSchedulerHeartbeatStale
    target_match_re:
      alertname: "AirflowTask.*|AirflowSLA.*"

receivers:
  - name: pagerduty-critical
    pagerduty_configs:
      - routing_key: ${PAGERDUTY_KEY}
        description: '{{ .CommonAnnotations.summary }}'
        severity: '{{ .CommonLabels.severity }}'

  - name: slack-data-eng
    slack_configs:
      - channel: '#data-eng-alerts'
        title: '{{ .CommonAnnotations.summary }}'
        text: |
          *Alert:* {{ .CommonLabels.alertname }}
          *DAG:* {{ .CommonLabels.dag_id }}
          *Runbook:* {{ .CommonAnnotations.runbook }}

  - name: slack-warning
    slack_configs:
      - channel: '#infra-alerts'
```

---

## Absent Alerts (Missing Series)

```yaml
# Alert when a scrape target disappears entirely
- alert: AirflowSchedulerMetricsMissing
  expr: absent(airflow_scheduler_heartbeat)
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "Airflow scheduler metrics not being scraped"
    description: "The airflow_scheduler_heartbeat metric has disappeared — scheduler may be down or metrics endpoint broken"

# Alert when Kafka metrics disappear
- alert: KafkaMetricsMissing
  expr: absent(kafka_server_brokertopicmetrics_messagesinpersec)
  for: 3m
  labels:
    severity: warning
```

---

## Remote Write Tuning (Prometheus → Mimir)

```yaml
# prometheus.yml
remote_write:
  - url: http://mimir.monitoring.svc:9090/api/v1/push

    queue_config:
      capacity: 10000           # in-memory queue size
      max_shards: 50            # parallel write shards
      min_shards: 1
      max_samples_per_send: 5000
      batch_send_deadline: 5s
      min_backoff: 30ms
      max_backoff: 5s

    write_relabel_configs:
      # Only remote-write important metrics (save bandwidth)
      - source_labels: [__name__]
        regex: "airflow_.*|kafka_.*|spark_.*|pipeline_.*"
        action: keep
```

```promql
# Monitor remote write health
prometheus_remote_storage_queue_highest_sent_timestamp_seconds
prometheus_remote_storage_samples_pending
prometheus_remote_storage_failed_samples_total
```

---

## Thanos/Mimir — Long-Term Retention

```yaml
# Thanos Sidecar (sits alongside Prometheus)
# Uploads TSDB blocks to S3 every 2h
apiVersion: monitoring.coreos.com/v1
kind: Prometheus
metadata:
  name: main
spec:
  retention: 7d           # local: 7 days
  thanos:
    objectStorageConfig:
      secret:
        name: thanos-objstore
        key: objstore.yml

# objstore.yml
type: S3
config:
  bucket: my-company-prometheus-data
  endpoint: s3.us-east-1.amazonaws.com
  region: us-east-1
```

---

## Scrape Interval Tuning

```yaml
# global defaults
global:
  scrape_interval: 60s         # default: 15s — increase to reduce storage
  evaluation_interval: 60s

scrape_configs:
  # High-frequency: critical latency metrics
  - job_name: api-server
    scrape_interval: 15s

  # Medium: operational metrics
  - job_name: airflow
    scrape_interval: 30s

  # Low frequency: slow-changing infra metrics
  - job_name: node-exporter
    scrape_interval: 60s

  # Very low: billing/cost metrics (changes hourly)
  - job_name: cost-exporter
    scrape_interval: 300s
```

---

## ServiceMonitor vs PodMonitor

```yaml
# ServiceMonitor — preferred (scrapes via Service endpoint)
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: airflow-metrics
  namespace: monitoring
spec:
  selector:
    matchLabels:
      app: airflow
      component: scheduler
  namespaceSelector:
    matchNames: [airflow]
  endpoints:
    - port: metrics
      path: /metrics
      interval: 30s

# PodMonitor — use when no Service exists
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: spark-executor-metrics
spec:
  selector:
    matchLabels:
      spark-role: executor
  namespaceSelector:
    any: true
  podMetricsEndpoints:
    - port: metrics
      path: /metrics
```

---

## Optimization Checklist

```
[ ] Total active series < 10M (check prometheus_tsdb_head_series)
[ ] High-cardinality labels (user_id, trace_id) dropped via relabeling
[ ] Recording rules precompute all Grafana panel queries
[ ] Recording rules follow level:metric:operations naming
[ ] Remote write queue depth < 1000 (prometheus_remote_storage_samples_pending)
[ ] Scrape interval 30-60s for non-critical metrics
[ ] Long-term storage: Thanos/Mimir with S3 backend (> 15 day retention)
[ ] AlertManager inhibition rules prevent storm on node failure
[ ] absent() alerts for all critical scrape targets
[ ] Weekly cardinality review (drop metrics with 0 dashboard usage)
```

---

## Anti-Patterns

1. **No recording rules for Grafana dashboards** — complex PromQL queries re-evaluated every 15s per user; precompute with recording rules.
2. **High-cardinality labels in metric names** — `user_id` in labels × 1M users = 1M series per metric; drop at collection.
3. **15s scrape interval for all targets** — 4x more storage/CPU than 60s; use 15s only for latency-sensitive metrics.
4. **Alerting on raw counter thresholds** — `errors_total > 100` fires permanently after 100 errors; always use `rate()` or `increase()`.
5. **No remote write for important metrics** — local Prometheus retains 15 days by default; critical metrics need long-term storage for capacity planning.
6. **Alert flood on node failure** — one failing node triggers 50+ alerts; add inhibition rules to suppress child alerts when parent (NodeDown) fires.

---

## References

- Prometheus best practices: `prometheus.io/docs/practices/`
- Recording rules: `prometheus.io/docs/practices/rules/`
- Thanos: `thanos.io/docs/`
- Mimir: `grafana.com/docs/mimir/latest/`
- Related skills: `[[infra-observability-stack-review]]`, `[[infra-grafana-dashboard-review]]`, `[[infra-alert-fatigue-reduction]]`
