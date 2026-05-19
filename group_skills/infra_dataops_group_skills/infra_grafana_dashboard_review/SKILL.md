---
name: infra-grafana-dashboard-review
description: Grafana dashboard review — panel types (timeseries/stat/gauge/table/heatmap), variable templating (datasource/label_values/query), dashboard linking and drilldown, data platform dashboards (pipeline overview/DAG health/Kafka throughput/Spark performance), alerting from panels, annotation markers for deployments, dashboard-as-code (Grafonnet/Terraform grafana provider), performance optimization (query caching/recording rules), dark/light theme standards
---

# Grafana Dashboard Review

## When to Use

- Building dashboards for a data platform (Airflow, Kafka, Spark, dbt)
- Reviewing existing dashboards for usability and performance issues
- Setting up dashboard-as-code (version-controlled JSON or Grafonnet)
- Adding deployment annotations to correlate incidents with releases
- Creating standardized dashboard templates for multiple environments

---

## Data Platform Overview Dashboard

### Panel Structure

```
Row 1: Pipeline Health (last 24h)
  ├── Stat: DAG success rate %
  ├── Stat: Active task instances
  ├── Stat: Failed DAG runs (last 1h)
  └── Stat: SLA misses today

Row 2: Pipeline Throughput
  ├── Timeseries: Task completions per minute by DAG
  ├── Timeseries: Task duration P50/P95 by task_id
  └── Heatmap: Task duration distribution

Row 3: Infrastructure
  ├── Timeseries: Pool slot utilization (default_pool)
  ├── Timeseries: Scheduler task queue depth
  └── Gauge: Worker CPU/memory utilization
```

---

## Variable Templating

```json
{
  "templating": {
    "list": [
      {
        "name": "datasource",
        "type": "datasource",
        "query": "prometheus",
        "label": "Datasource"
      },
      {
        "name": "namespace",
        "type": "query",
        "datasource": "${datasource}",
        "query": "label_values(kube_namespace_labels, namespace)",
        "label": "Namespace",
        "multi": true,
        "includeAll": true,
        "allValue": ".*"
      },
      {
        "name": "dag_id",
        "type": "query",
        "datasource": "${datasource}",
        "query": "label_values(airflow_task_success_total{namespace=~\"$namespace\"}, dag_id)",
        "label": "DAG",
        "multi": true,
        "includeAll": true
      },
      {
        "name": "environment",
        "type": "custom",
        "options": [
          {"text": "Production", "value": "prod"},
          {"text": "Staging", "value": "staging"}
        ],
        "label": "Environment"
      }
    ]
  }
}
```

---

## Key Panel Configurations

### Pipeline Success Rate (Stat Panel)

```json
{
  "type": "stat",
  "title": "Pipeline Success Rate (24h)",
  "targets": [{
    "expr": "100 * sum(increase(airflow_task_success_total{dag_id=~\"$dag_id\"}[24h])) / (sum(increase(airflow_task_success_total{dag_id=~\"$dag_id\"}[24h])) + sum(increase(airflow_task_failures_total{dag_id=~\"$dag_id\"}[24h])))",
    "legendFormat": "Success Rate"
  }],
  "options": {
    "reduceOptions": {"calcs": ["lastNotNull"]},
    "colorMode": "background",
    "thresholds": {
      "steps": [
        {"color": "red", "value": 0},
        {"color": "yellow", "value": 95},
        {"color": "green", "value": 99}
      ]
    }
  }
}
```

### Task Duration Heatmap

```json
{
  "type": "heatmap",
  "title": "Task Duration Distribution",
  "targets": [{
    "expr": "sum(rate(airflow_task_duration_seconds_bucket{dag_id=~\"$dag_id\"}[5m])) by (le)",
    "format": "heatmap",
    "legendFormat": "{{le}}"
  }],
  "options": {
    "calculate": false,
    "yAxis": {
      "unit": "s",
      "decimals": 0
    }
  }
}
```

### Kafka Lag Table

```json
{
  "type": "table",
  "title": "Kafka Consumer Lag",
  "targets": [{
    "expr": "sum(kafka_consumergroup_lag) by (consumergroup, topic)",
    "legendFormat": "",
    "instant": true,
    "format": "table"
  }],
  "transformations": [
    {"id": "organize", "options": {
      "renameByName": {
        "consumergroup": "Consumer Group",
        "topic": "Topic",
        "Value": "Lag"
      }
    }},
    {"id": "sortBy", "options": {
      "fields": [{"displayName": "Lag", "desc": true}]
    }}
  ],
  "fieldConfig": {
    "overrides": [{
      "matcher": {"id": "byName", "options": "Lag"},
      "properties": [{
        "id": "thresholds",
        "value": {
          "steps": [
            {"color": "green", "value": 0},
            {"color": "yellow", "value": 1000},
            {"color": "red", "value": 10000}
          ]
        }
      }, {
        "id": "custom.displayMode",
        "value": "color-background"
      }]
    }]
  }
}
```

---

## Deployment Annotations

```bash
# Annotate Grafana with deployment events (via API)
GRAFANA_URL="http://grafana.monitoring.svc:3000"
GRAFANA_TOKEN="${GRAFANA_SA_TOKEN}"

annotate_deployment() {
  local dag_id=$1
  local version=$2
  curl -s -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${GRAFANA_TOKEN}" \
    -d "{
      \"dashboardUID\": \"data-platform\",
      \"time\": $(date +%s000),
      \"text\": \"Deploy: ${dag_id} v${version}\",
      \"tags\": [\"deploy\", \"${dag_id}\"]
    }" \
    "${GRAFANA_URL}/api/annotations"
}

# Call from CI/CD after successful deploy
annotate_deployment "etl_orders" "$IMAGE_TAG"
```

```yaml
# GitHub Actions step: annotate on deploy
- name: Annotate Grafana
  run: |
    curl -s -X POST \
      -H "Authorization: Bearer ${{ secrets.GRAFANA_TOKEN }}" \
      -H "Content-Type: application/json" \
      -d '{"text": "Deploy: ${{ github.repository }}@${{ github.sha }}", "tags": ["deploy"]}' \
      ${{ secrets.GRAFANA_URL }}/api/annotations
```

---

## Dashboard-as-Code (Terraform)

```hcl
# Manage Grafana dashboards via Terraform
resource "grafana_dashboard" "data_platform" {
  config_json = file("${path.module}/dashboards/data-platform.json")
  folder      = grafana_folder.data_engineering.id
  overwrite   = true
}

resource "grafana_folder" "data_engineering" {
  title = "Data Engineering"
  uid   = "data-engineering"
}

# Alert rule as code
resource "grafana_rule_group" "pipeline_alerts" {
  name             = "Pipeline Alerts"
  folder_uid       = grafana_folder.data_engineering.uid
  interval_seconds = 60

  rule {
    name      = "Pipeline Freshness"
    condition = "C"
    for       = "10m"
    labels    = { severity = "warning", team = "data-engineering" }
    annotations = {
      summary = "Pipeline {{ $labels.dag_id }} data is stale"
      runbook = "https://wiki.my-org.com/runbooks/pipeline-freshness"
    }

    data {
      ref_id         = "A"
      query_type     = "range"
      datasource_uid = grafana_data_source.prometheus.uid
      model = jsonencode({
        expr = "time() - max(airflow_dagrun_end_date{state='success'}) by (dag_id)"
      })
    }

    data {
      ref_id = "C"
      model  = jsonencode({ type = "threshold", conditions = [{ evaluator = { type = "gt", params = [7200] } }] })
    }
  }
}
```

---

## Dashboard Performance Optimization

```json
// Use recording rules instead of complex PromQL in panels
// ❌ Slow: complex query re-evaluated per panel load
{
  "expr": "sum(rate(airflow_task_failures_total[5m])) by (dag_id) / (sum(rate(airflow_task_success_total[5m])) by (dag_id) + sum(rate(airflow_task_failures_total[5m])) by (dag_id))"
}

// ✅ Fast: precomputed recording rule
{
  "expr": "job_dag:pipeline_error_rate:ratio5m"
}
```

```json
// Limit default time range to avoid full-scan queries
{
  "time": { "from": "now-6h", "to": "now" },
  "timepicker": {
    "refresh_intervals": ["30s", "1m", "5m"],
    "time_options": ["1h", "3h", "6h", "12h", "24h", "2d", "7d"]
  }
}
```

---

## Dashboard Review Checklist

```
[ ] All variable dropdowns use label_values() — not hardcoded
[ ] Time range defaults to 6h or 24h (not 90d)
[ ] Panels use recording rules for complex queries
[ ] Stat panels have color thresholds (green/yellow/red)
[ ] Table panels have cell coloring on key columns
[ ] Deployment annotations configured in CI/CD
[ ] Dashboard UID set (not auto-generated) for stable links
[ ] Folder structure: Data Engineering / Airflow / Kafka / Spark
[ ] Dashboard JSON committed to git (Terraform or ConfigMap)
[ ] Links between dashboards: overview → detail drilldown
[ ] Description field populated on dashboard and key panels
```

---

## Anti-Patterns

1. **No variable templating** — hardcoded namespace/dag_id means dashboard only works for one environment; use `label_values()` variables.
2. **Complex PromQL in every panel** — 20 panels × complex query = slow dashboard and high Prometheus load; precompute with recording rules.
3. **Default time range of 30 days** — loads millions of data points on open; set default to 6h or 24h.
4. **Dashboard managed only in Grafana UI** — no version history, no peer review; manage dashboards as JSON in git.
5. **No color thresholds on key metrics** — a success rate of 60% looks the same as 99.9% without colors; always add threshold steps.

---

## References

- Grafana panel types: `grafana.com/docs/grafana/latest/panels-visualizations/`
- Terraform Grafana provider: `registry.terraform.io/providers/grafana/grafana/latest/docs`
- Grafana annotations API: `grafana.com/docs/grafana/latest/http_api/annotations/`
- Related skills: `[[infra-observability-stack-review]]`, `[[infra-prometheus-optimization]]`, `[[dataops-airflow-observability]]`
