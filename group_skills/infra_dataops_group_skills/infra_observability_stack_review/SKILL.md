---
name: infra-observability-stack-review
description: Observability stack review — three pillars (metrics/logs/traces), Prometheus+Grafana+Loki+Tempo (LGTM stack), OpenTelemetry Collector as universal agent, cardinality management, log aggregation patterns (Fluent Bit→Loki), distributed tracing (Jaeger/Tempo), alerting pipeline (Alertmanager routing), SLO definition and error budget tracking, data platform observability (pipeline freshness/throughput/error rate), observability as code (Grafana provisioning)
---

# Observability Stack Review

## When to Use

- Designing the observability layer for a new data platform
- Auditing an existing monitoring setup (gaps, cardinality issues)
- Setting up the LGTM stack (Loki + Grafana + Tempo + Mimir/Prometheus)
- Defining SLOs for data pipelines
- Debugging a production incident without sufficient visibility

---

## Three Pillars Architecture

```
                    ┌─────────────────────────────────────┐
                    │         OpenTelemetry Collector       │
                    │  (deployed as DaemonSet on each node) │
                    └──────┬──────────┬──────────┬─────────┘
                           │          │          │
                      Metrics       Logs       Traces
                           │          │          │
                     ┌─────▼─┐  ┌────▼──┐  ┌───▼────┐
                     │Prometheus│  │ Loki  │  │ Tempo  │
                     └─────┬──┘  └────┬──┘  └───┬────┘
                           └──────────┴──────────┘
                                      │
                               ┌──────▼──────┐
                               │   Grafana   │
                               │  (unified   │
                               │    UI)      │
                               └─────────────┘
```

---

## OpenTelemetry Collector (Universal Agent)

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc: { endpoint: 0.0.0.0:4317 }
      http: { endpoint: 0.0.0.0:4318 }

  prometheus:
    config:
      scrape_configs:
        - job_name: airflow
          static_configs:
            - targets: [airflow-webserver:9090]
        - job_name: kafka
          static_configs:
            - targets: [kafka-jmx-exporter:9101]

  filelog:
    include: [/var/log/pods/**/*.log]
    start_at: beginning
    include_file_path: true
    operators:
      - type: json_parser
        timestamp:
          parse_from: attributes.time
          layout: '%Y-%m-%dT%H:%M:%S.%fZ'

processors:
  batch:
    send_batch_size: 1000
    timeout: 10s

  resourcedetection:
    detectors: [k8snode, k8s_cluster]

  k8sattributes:
    passthrough: false
    extract:
      metadata: [k8s.pod.name, k8s.namespace.name, k8s.deployment.name]
      labels:
        - tag_name: app
          key: app
          from: pod

  memory_limiter:
    check_interval: 1s
    limit_mib: 512

exporters:
  prometheusremotewrite:
    endpoint: http://mimir:9090/api/v1/push

  loki:
    endpoint: http://loki:3100/loki/api/v1/push
    labels:
      resource:
        k8s.namespace.name: namespace
        k8s.pod.name: pod
        app: app

  otlp/tempo:
    endpoint: http://tempo:4317

service:
  pipelines:
    metrics:
      receivers: [otlp, prometheus]
      processors: [memory_limiter, batch, resourcedetection]
      exporters: [prometheusremotewrite]
    logs:
      receivers: [otlp, filelog]
      processors: [memory_limiter, batch, k8sattributes]
      exporters: [loki]
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlp/tempo]
```

---

## Prometheus + Mimir (Long-Term Metrics)

```yaml
# Helm: kube-prometheus-stack
helm upgrade --install prometheus-stack \
  prometheus-community/kube-prometheus-stack \
  -n monitoring \
  --set prometheus.prometheusSpec.remoteWrite[0].url=http://mimir:9090/api/v1/push \
  --set prometheus.prometheusSpec.retention=7d \
  --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage=100Gi
```

### Cardinality Management

```bash
# Find high-cardinality metrics (> 10k series)
curl -s http://prometheus:9090/api/v1/label/__name__/values \
  | jq -r '.data[]' \
  | while read metric; do
      count=$(curl -sg "http://prometheus:9090/api/v1/query?query=count($metric)" \
        | jq '.data.result[0].value[1]' 2>/dev/null)
      echo "$count $metric"
    done \
  | sort -rn \
  | head -20

# Drop high-cardinality labels before storing
# In otel-collector or prometheus relabeling:
metric_relabel_configs:
  - source_labels: [user_id]
    action: labeldrop   # user_id has 1M+ unique values — drop it
```

---

## Loki (Log Aggregation)

```yaml
# Fluent Bit → Loki (DaemonSet on all nodes)
[OUTPUT]
    Name            loki
    Match           *
    Host            loki.monitoring.svc.cluster.local
    Port            3100
    Labels          job=fluentbit,namespace=$kubernetes['namespace_name'],pod=$kubernetes['pod_name'],app=$kubernetes['labels']['app']
    Label_Keys      $level,$dag_id,$task_id
    Remove_Keys     kubernetes
    Line_Format     json
```

```logql
# LogQL queries for data platform logs

# All errors from Airflow scheduler in last 1h
{namespace="airflow", app="scheduler"} |= "ERROR" | json

# Task failures with structured log fields
{namespace="airflow"} | json | level="ERROR" | dag_id != "" 
  | line_format "DAG={{.dag_id}} task={{.task_id}}: {{.message}}"

# Kafka consumer lag spikes
{namespace="kafka"} | json | __error__="" 
  | consumer_lag > 10000
```

---

## Tempo (Distributed Tracing)

```yaml
# Grafana datasource linking: traces → logs correlation
# In Grafana Tempo datasource config:
tracesToLogs:
  datasourceUid: loki
  tags: [service.name, namespace]
  mappedTags:
    - key: service.name
      value: app
  filterByTraceID: true
  filterBySpanID: false

# LogQL for correlated logs:
# {namespace="airflow"} | traceID = "${__trace.traceId}"
```

---

## SLO Definition

```yaml
# Pyrra (SLO as Code) — generates Prometheus recording rules + alerts
apiVersion: pyrra.dev/v1alpha1
kind: ServiceLevelObjective
metadata:
  name: airflow-task-success-rate
  namespace: monitoring
spec:
  target: "99"                   # 99% success rate SLO
  window: 28d
  description: "99% of Airflow tasks complete successfully over 28 days"
  indicator:
    ratio:
      errors:
        metric: airflow_task_failures_total{}
      total:
        metric: |
          airflow_task_success_total{} + airflow_task_failures_total{}
```

```promql
# Error budget remaining (burn rate alert at 5x)
(
  sum(rate(airflow_task_failures_total[1h])) /
  sum(rate((airflow_task_success_total + airflow_task_failures_total)[1h]))
) > (5 * (1 - 0.99))   # 5x burn rate = exhausts 1h budget in 12min
```

---

## Data Platform Observability Metrics

```yaml
# Custom metrics for pipeline observability

# Freshness: time since last successful pipeline run
- record: pipeline_freshness_seconds
  expr: |
    time() - max(
      airflow_dagrun_end_date{dag_id=~".*", state="success"}
    ) by (dag_id)

# Throughput: rows processed per second
- record: pipeline_throughput_rows_per_second
  expr: |
    rate(pipeline_rows_processed_total[5m])

# Error rate: fraction of runs that failed
- record: pipeline_error_rate
  expr: |
    sum(rate(airflow_dagrun_total{state="failed"}[1h])) by (dag_id) /
    sum(rate(airflow_dagrun_total[1h])) by (dag_id)
```

---

## Observability as Code (Grafana Provisioning)

```yaml
# Grafana dashboard ConfigMap (auto-loaded)
apiVersion: v1
kind: ConfigMap
metadata:
  name: data-platform-dashboard
  namespace: monitoring
  labels:
    grafana_dashboard: "1"      # picked up by Grafana sidecar
data:
  data-platform.json: |
    {
      "title": "Data Platform Overview",
      "uid": "data-platform",
      "panels": [...]
    }
```

```yaml
# Grafana alert provisioning
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-alerts
  namespace: monitoring
  labels:
    grafana_alert: "1"
data:
  pipeline-alerts.yaml: |
    groups:
      - name: data-platform
        rules:
          - alert: PipelineFreshnessViolation
            expr: pipeline_freshness_seconds > 7200
            annotations:
              summary: "Pipeline {{ $labels.dag_id }} data is stale (> 2h)"
```

---

## Observability Maturity Checklist

```
Level 1 — Basic (must have):
[ ] Prometheus scrapes all services (via ServiceMonitor)
[ ] Grafana with Kubernetes + app dashboards
[ ] Alertmanager routes CRITICAL to PagerDuty, WARNING to Slack
[ ] Centralized log aggregation (Loki/ELK)
[ ] Uptime/healthcheck monitoring

Level 2 — Intermediate:
[ ] OpenTelemetry Collector as universal agent
[ ] Distributed tracing (Tempo/Jaeger) on data pipeline calls
[ ] Log-to-trace correlation in Grafana
[ ] SLO definitions with error budget burn alerts
[ ] Custom data platform metrics (freshness/throughput/error rate)

Level 3 — Advanced:
[ ] Long-term metrics storage (Mimir/Thanos) — 1+ year retention
[ ] Cardinality control (< 10M active series)
[ ] SLO-driven alerting (burn rate, not threshold)
[ ] Automatic anomaly detection on metrics
[ ] Observability as code (everything in Git)
```

---

## Anti-Patterns

1. **Three separate dashboards for metrics/logs/traces** — no cross-signal correlation; use Grafana Explore for unified view and link traces to logs.
2. **Scraping every metric label including `user_id`** — explodes cardinality to millions of series; drop high-cardinality labels at collection time.
3. **No log structure** — `grep`-based debugging across millions of unstructured lines is slow; enforce JSON logging with `dag_id`, `task_id`, `level` fields.
4. **Alerting on raw counters** — `airflow_task_failures_total > 100` fires permanently; always alert on `rate()` or `increase()`.
5. **No SLO definition** — teams manage alerts ad-hoc without knowing what "healthy" means; define SLOs before writing alert rules.

---

## References

- OpenTelemetry Collector: `opentelemetry.io/docs/collector/`
- Loki: `grafana.com/docs/loki/latest/`
- Tempo: `grafana.com/docs/tempo/latest/`
- Pyrra SLO: `github.com/pyrra-dev/pyrra`
- Related skills: `[[infra-prometheus-optimization]]`, `[[infra-grafana-dashboard-review]]`, `[[infra-opentelemetry-instrumentation]]`, `[[infra-alert-fatigue-reduction]]`
