---
name: aiops-observability-copilot
description: AIOps observability copilot — natural language to PromQL/LogQL translation, alert explanation in plain English (what fired/why/impact), Grafana dashboard auto-generation from service topology, anomaly narrative generation from metric patterns, on-call context enrichment (related alerts/recent deploys/runbook links), log pattern clustering for noise reduction, SLO status explanation, automated weekly observability health digest
---

# AIOps Observability Copilot

## When to Use

- Translating natural language questions about infrastructure into PromQL/LogQL queries
- Explaining what a firing alert means in plain English for non-experts
- Auto-generating Grafana dashboard JSON from a service description
- Building context-rich incident summaries for on-call engineers
- Clustering noisy log errors to surface unique patterns

---

## Natural Language to PromQL Translation

```python
import anthropic

client = anthropic.Anthropic()

PROMQL_SYSTEM = """You are a Prometheus and Grafana expert. Translate natural language questions about 
infrastructure metrics into PromQL queries.

Context about the data platform:
- Namespaces: airflow, kafka, spark, monitoring, postgres
- Key metrics: container_cpu_usage_seconds_total, container_memory_working_set_bytes,
  kafka_consumer_group_lag, airflow_dag_run_duration_success, up, kube_pod_status_phase

Rules:
- Always use rate() for counters over [5m] window
- Use avg() for deployment-level aggregation
- Add namespace or pod label selectors to scope queries
- Return ONLY the PromQL query, no explanation

Examples:
User: "Show me CPU usage for Airflow scheduler"
PromQL: rate(container_cpu_usage_seconds_total{namespace="airflow", pod=~"airflow-scheduler.*"}[5m])

User: "Kafka consumer lag for orders group"
PromQL: sum(kafka_consumer_group_lag{group="orders-processor"}) by (topic, partition)"""


def nl_to_promql(question: str) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        system=PROMQL_SYSTEM,
        messages=[{"role": "user", "content": question}],
    )
    return response.content[0].text.strip()


LOGQL_SYSTEM = """You are a Grafana Loki expert. Translate natural language questions into LogQL queries.
Return ONLY the LogQL query.

Available label values: namespace (airflow|kafka|spark), app, pod, container
Common patterns: level="error", level="warn", duration > 30s"""


def nl_to_logql(question: str) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        system=LOGQL_SYSTEM,
        messages=[{"role": "user", "content": question}],
    )
    return response.content[0].text.strip()


# Example usage
queries = {
    "Show Airflow scheduler errors in the last hour": nl_to_logql,
    "How much memory is Spark using?": nl_to_promql,
    "Kafka consumer lag for orders processor": nl_to_promql,
}
```

---

## Alert Explanation Engine

```python
def explain_alert(
    alert_name: str,
    alert_labels: dict,
    alert_annotations: dict,
    current_metric_value: float | None = None,
    related_alerts: list[str] | None = None,
) -> str:
    """Generate a plain-English explanation of a firing alert."""

    context = f"""
Alert: {alert_name}
Labels: {alert_labels}
Description: {alert_annotations.get('description', 'N/A')}
Current value: {current_metric_value}
Related firing alerts: {related_alerts or []}
Runbook: {alert_annotations.get('runbook_url', 'N/A')}
"""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system="""You are an expert SRE explaining alerts to on-call engineers.
For each alert, explain in clear plain English:
1. WHAT is happening (one sentence)
2. WHY this is a problem (impact on users/data)
3. MOST LIKELY cause (top 2-3 candidates)
4. IMMEDIATE action to take (one clear step)
Keep the total response under 150 words.""",
        messages=[{"role": "user", "content": f"Explain this alert:\n{context}"}],
    )
    return response.content[0].text


# Example output:
# "The Airflow scheduler pod is using 95% of its CPU limit, causing it to slow down
# and potentially miss DAG scheduling deadlines. This is likely caused by: 1) too many
# DAGs parsed simultaneously, 2) a runaway Python import in a DAG file.
# Immediate action: check `airflow dags list-import-errors` and reduce parsing_parallelism."
```

---

## Incident Context Enrichment

```python
from datetime import datetime, timedelta

def enrich_incident_context(
    incident_time: datetime,
    affected_services: list[str],
    alert_name: str,
) -> dict:
    """
    Build rich context for an on-call engineer:
    - Recent deployments near incident time
    - Related alerts
    - Relevant runbooks
    """
    from prometheus_api_client import PrometheusConnect
    import subprocess

    prom = PrometheusConnect(url=PROMETHEUS_URL)

    # 1. Recent Kubernetes deployments (10 min window before incident)
    events_raw = subprocess.run(
        ["kubectl", "get", "events", "-A",
         "--field-selector", "reason=ScalingReplicaSet",
         "-o", "json"],
        capture_output=True, text=True
    )
    import json
    events = json.loads(events_raw.stdout).get("items", [])
    recent_deploys = [
        f"{e['involvedObject']['namespace']}/{e['involvedObject']['name']} — {e['message']}"
        for e in events
        if (incident_time - timedelta(minutes=30)).isoformat() < e.get("lastTimestamp", "") < incident_time.isoformat()
    ]

    # 2. Related alerts firing at same time
    related = prom.custom_query(
        'ALERTS{alertstate="firing"}',
        params={"time": incident_time.timestamp()}
    )
    related_alerts = [r["metric"].get("alertname") for r in related if r["metric"].get("alertname") != alert_name]

    # 3. Generate narrative using LLM
    narrative = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        messages=[{
            "role": "user",
            "content": f"""Build a 3-sentence incident context summary for on-call:
Alert: {alert_name}
Affected services: {affected_services}
Recent deployments: {recent_deploys[:5]}
Also firing: {related_alerts[:5]}
Time: {incident_time.isoformat()}

Focus on: what changed recently, what else is broken, what to check first."""
        }]
    ).content[0].text

    return {
        "incident_time": incident_time.isoformat(),
        "recent_deployments": recent_deploys[:5],
        "related_alerts": related_alerts[:10],
        "narrative": narrative,
        "runbook_url": RUNBOOK_BASE_URL + alert_name.lower().replace("_", "-"),
    }
```

---

## Log Pattern Clustering

```python
from collections import Counter
import re

def cluster_log_errors(
    log_lines: list[str],
    max_clusters: int = 10,
) -> list[dict]:
    """
    Cluster similar error log lines to surface unique error patterns.
    Reduces 10,000 noisy log lines to 5-10 unique patterns.
    """
    # Normalize: remove timestamps, IDs, numbers
    def normalize(line: str) -> str:
        line = re.sub(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?Z?', '<TS>', line)
        line = re.sub(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<UUID>', line)
        line = re.sub(r'\b[0-9]+\b', '<N>', line)
        line = re.sub(r'partition=\w+', 'partition=<P>', line)
        line = re.sub(r'offset=\w+', 'offset=<O>', line)
        return line.strip()

    normalized = [normalize(l) for l in log_lines if l.strip()]
    pattern_counts = Counter(normalized)

    top_patterns = pattern_counts.most_common(max_clusters * 3)

    # Use LLM to group and explain remaining similar patterns
    if len(top_patterns) > max_clusters:
        patterns_text = "\n".join(f"{count}x: {pattern}" for pattern, count in top_patterns[:30])

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            messages=[{
                "role": "user",
                "content": f"""Group these error log patterns into {max_clusters} distinct error categories.
For each category, write a 1-line description of what's failing.
Return JSON: [{{"category": "str", "count": int, "example": "str", "likely_cause": "str"}}]

Patterns:
{patterns_text}"""
            }]
        )
        import json
        text = response.content[0].text
        json_match = re.search(r'\[.*\]', text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())

    return [
        {"category": pattern, "count": count, "example": pattern, "likely_cause": ""}
        for pattern, count in top_patterns[:max_clusters]
    ]
```

---

## Auto-Generated Dashboard JSON

```python
def generate_dashboard(
    service_name: str,
    metrics: list[str],
    namespace: str,
) -> dict:
    """Generate a Grafana dashboard JSON for a service."""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{
            "role": "user",
            "content": f"""Generate a Grafana dashboard JSON for {service_name} in namespace {namespace}.
Include panels for: {', '.join(metrics)}

Use these panel types:
- timeseries for rate/gauge metrics
- stat for single current value
- table for lag/consumer group info

Return valid Grafana JSON with 3-4 panels. Use templating variable $namespace = {namespace}."""
        }]
    )

    import json, re
    json_match = re.search(r'\{.*\}', response.content[0].text, re.DOTALL)
    if json_match:
        return json.loads(json_match.group())
    return {}
```

---

## Weekly Observability Health Digest

```python
def generate_weekly_digest(
    alert_history: list[dict],   # [{name, fired_at, resolved_at, duration_min}]
    slo_states: list[dict],      # [{slo_name, budget_remaining_pct, status}]
) -> str:
    """Generate a weekly observability health digest."""

    total_alerts = len(alert_history)
    mttr = sum(a.get("duration_min", 0) for a in alert_history) / max(total_alerts, 1)
    critical = [a for a in alert_history if a.get("severity") == "critical"]

    slo_at_risk = [s for s in slo_states if s["budget_remaining_pct"] < 50]

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": f"""Write a concise weekly observability health digest (under 300 words) for the data platform team.

Stats this week:
- Total alerts fired: {total_alerts}
- Critical alerts: {len(critical)}
- Mean time to resolve: {mttr:.0f} minutes
- SLOs at risk (< 50% budget): {[s['slo_name'] for s in slo_at_risk]}
- Top alerts: {[a['name'] for a in sorted(alert_history, key=lambda x: x.get('duration_min', 0), reverse=True)[:5]]}

Include: summary, top concern, one improvement recommendation."""
        }]
    ).content[0].text

    return response
```

---

## Anti-Patterns

1. **LLM-generated PromQL without validation** — always execute the generated query against a real Prometheus instance before showing it to users; hallucinated metric names silently return no data.
2. **Alert explanation without metric value** — "CPU is high" means nothing without "CPU is at 98% (limit: 4 cores)"; always inject the current metric value into the LLM context.
3. **Log clustering on raw lines** — clustering without normalization groups "error at offset 1234" and "error at offset 5678" as different patterns; normalize timestamps and IDs first.
4. **Replacing on-call with fully automated copilot** — the copilot enriches context, it doesn't replace judgment; always keep a human in the loop for SEV1 decisions.
5. **Stale runbook links** — alert annotations that point to 404 runbooks erode trust; validate runbook URLs as part of alert rule CI.

---

## References

- PromQL: `prometheus.io/docs/prometheus/latest/querying/basics/`
- LogQL: `grafana.com/docs/loki/latest/query/log_queries/`
- Grafana dashboard JSON: `grafana.com/docs/grafana/latest/dashboards/build-dashboards/`
- Related skills: `[[infra-prometheus-optimization]]`, `[[infra-grafana-dashboard-review]]`, `[[aiops-autonomous-incident-response]]`, `[[infra-observability-stack-review]]`
