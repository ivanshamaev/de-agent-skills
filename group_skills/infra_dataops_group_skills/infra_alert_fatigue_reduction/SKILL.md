---
name: infra-alert-fatigue-reduction
description: Alert fatigue reduction — SLO-based alerting (burn rate vs threshold), multi-window burn rate alerts (short/long window), AlertManager inhibition and silencing, alert deduplication (group_by), routing by severity (PagerDuty for critical / Slack for warning), alert ownership labels, runbook links in annotations, alert review process (weekly noise audit), flapping detection (for/pending period tuning), absent() vs rate() alerting patterns, ticket-based escalation for warning-level noise
---

# Alert Fatigue Reduction

## When to Use

- On-call team is overwhelmed with noisy or irrelevant alerts
- Alerts fire frequently but require no action (false positives)
- Critical incidents are missed because alerts are ignored
- Setting up alerting for a new data platform from scratch
- Auditing existing alert rules for quality and actionability

---

## The Problem: Threshold-Based Alerting

```yaml
# ❌ Threshold alerts cause fatigue:
- alert: TaskFailures
  expr: airflow_task_failures_total > 5    # fires forever once 5 failures accumulate
  # Fires on every evaluation, no duration, no context

# ❌ Too sensitive — fires on any spike
- alert: HighCPU
  expr: cpu_usage > 0.8
  # No "for" duration — fires on 15s spike, pages engineer at 3am
```

---

## SLO-Based Burn Rate Alerting

```yaml
# ✅ Multi-window burn rate: catches both fast burns (short window) and slow burns (long window)
# SLO: 99% pipeline success rate over 28 days (error budget: 1% = 403 minutes/28d)

groups:
  - name: pipeline-slo-alerts
    rules:

    # Page immediately: burning through 1-hour budget in 5 minutes (12x burn rate)
    - alert: PipelineErrorBudgetCritical
      expr: |
        (
          job_dag:pipeline_error_rate:ratio5m > (14.4 * 0.01)   # 5m window: 14.4x burn
        ) and (
          job_dag:pipeline_error_rate:ratio1h > (14.4 * 0.01)   # 1h window confirms trend
        )
      for: 2m
      labels:
        severity: critical
        page: "true"
      annotations:
        summary: "Pipeline error budget burning fast ({{ $labels.dag_id }})"
        description: "At current burn rate, 1-hour error budget exhausted in 5 minutes"
        runbook: "https://wiki.my-org.com/runbooks/pipeline-slo"

    # Warn: burning through 6-hour budget over 1 hour
    - alert: PipelineErrorBudgetWarning
      expr: |
        (
          job_dag:pipeline_error_rate:ratio30m > (6 * 0.01)
        ) and (
          job_dag:pipeline_error_rate:ratio6h > (6 * 0.01)
        )
      for: 15m
      labels:
        severity: warning
        page: "false"
      annotations:
        summary: "Pipeline error budget depleting ({{ $labels.dag_id }})"
        description: "At current burn rate, error budget exhausted in ~{{ $value | humanizeDuration }}"
```

---

## AlertManager Configuration for Noise Reduction

```yaml
# alertmanager.yml
global:
  resolve_timeout: 5m

route:
  receiver: null-receiver          # default: drop (alert must explicitly match a route)
  group_by: [alertname, namespace, dag_id]
  group_wait: 30s                  # wait 30s to batch related alerts
  group_interval: 5m               # resend grouped alert every 5m
  repeat_interval: 4h              # don't repeat the same alert > every 4h

  routes:
    # Critical: page PagerDuty immediately
    - match:
        severity: critical
        page: "true"
      receiver: pagerduty-oncall
      group_wait: 10s
      repeat_interval: 1h

    # Warning: Slack only (no page)
    - match:
        severity: warning
      receiver: slack-data-eng
      group_wait: 2m               # batch warnings for 2 min
      repeat_interval: 8h          # don't repeat warning > every 8h

    # Info: create ticket (no interrupt)
    - match:
        severity: info
      receiver: jira-ticket
      repeat_interval: 24h

    # Infrastructure storms: suppress derived alerts
    - match:
        alertname: NodeDown
      receiver: pagerduty-oncall
      routes:
        []   # inhibition handles derived alerts

inhibit_rules:
  # Node down → suppress all pod/container alerts on that node
  - source_match:
      alertname: NodeDown
    target_match_re:
      alertname: "KubePod.*|Container.*"
    equal: [node]

  # Airflow scheduler down → suppress task-level alerts
  - source_match:
      alertname: AirflowSchedulerHeartbeatStale
    target_match_re:
      alertname: "Airflow(Task|SLA|Pool).*"

  # Critical fires → suppress corresponding warning
  - source_match:
      severity: critical
    target_match:
      severity: warning
    equal: [alertname, dag_id]    # suppress warning when critical fires for same alert+dag
```

---

## Alert Quality Standards

### Required Fields on Every Alert

```yaml
- alert: PipelineFreshnessViolation
  expr: pipeline_freshness_seconds{dag_id=~".+"} > 7200
  for: 10m                         # REQUIRED: avoid flapping
  labels:
    severity: warning              # REQUIRED: critical/warning/info
    team: data-engineering         # REQUIRED: who owns this alert
    page: "false"                  # REQUIRED: does this page on-call?
  annotations:
    summary: "Pipeline {{ $labels.dag_id }} data stale > 2h"   # REQUIRED: one-line what
    description: "Last successful run: {{ $value | humanizeDuration }} ago. Check if DAG is paused or if upstream source is delayed."  # REQUIRED: context
    runbook: "https://wiki.my-org.com/runbooks/pipeline-freshness"   # REQUIRED: what to do
    dashboard: "https://grafana.my-org.com/d/data-platform?var-dag_id={{ $labels.dag_id }}"
```

---

## "For" Duration Tuning

```yaml
# Tune "for" to avoid flapping on transient spikes

# ❌ No "for": fires on 15-second spike
- alert: HighCPU
  expr: cpu_usage > 0.8

# ✅ With "for": requires sustained condition
- alert: HighCPU
  expr: cpu_usage > 0.8
  for: 5m     # must be > 80% for 5 continuous minutes

# Guidelines by alert type:
# CPU/memory spikes:        for: 5m
# Pipeline failure rate:    for: 10m
# Freshness violations:     for: 15m (tolerate brief re-runs)
# Scheduler heartbeat:      for: 2m  (critical, catch fast)
# Cost threshold exceeded:  for: 1h  (trends, not spikes)
```

---

## Weekly Alert Noise Audit

```bash
#!/bin/bash
# alert_noise_audit.sh — run weekly, report top firing alerts

PROM_URL="http://prometheus:9090"

# Alerts that fired most in last 7 days
curl -s "${PROM_URL}/api/v1/query?query=sum(increase(ALERTS_FOR_STATE[7d])) by (alertname) > 0" \
  | jq -r '.data.result | sort_by(-.value[1] | tonumber) | .[:20] | .[] |
    "\(.value[1] | tonumber | round) fires: \(.metric.alertname)"'

# Alerts firing right now (potential noise)
curl -s "${PROM_URL}/api/v1/alerts" \
  | jq -r '.data.alerts[] | select(.state == "firing") |
    "\(.labels.severity) | \(.labels.alertname) | \(.annotations.summary)"' \
  | sort | uniq -c | sort -rn
```

### Noise Reduction Decision Tree

```
Alert fires > 5 times/week with no action taken?
  ├── Is it a false positive?
  │   ├── Yes → Fix the underlying condition or raise threshold
  │   └── No → Add "for" duration or use burn rate instead of threshold
  ├── Is it actionable?
  │   ├── No → Demote severity (warning → info) or convert to ticket
  │   └── Yes → Write the runbook, ensure on-call is trained
  └── Is it redundant (covered by another alert)?
      └── Yes → Delete it
```

---

## Alert Ownership Model

```yaml
# Every alert has a team label for routing and accountability
# teams: data-engineering, platform, ml-ops, analytics

- alert: KafkaConsumerLagHigh
  labels:
    severity: warning
    team: data-engineering          # routed to data-eng Slack channel
    service: kafka-orders-consumer
    runbook_version: "2024-01-15"
```

```yaml
# AlertManager route by team
routes:
  - match:
      team: data-engineering
    receiver: slack-data-engineering
  - match:
      team: platform
    receiver: slack-platform
  - match:
      team: ml-ops
    receiver: slack-mlops
```

---

## Actionability Checklist (Per Alert)

```
[ ] Is the alert actionable? (can engineer take a specific action?)
[ ] Does the runbook exist and is it linked in annotations?
[ ] Is the "for" duration appropriate (no flapping)?
[ ] Is severity correct? (critical = page, warning = Slack, info = ticket)
[ ] Is there a corresponding inhibition if a parent alert fires?
[ ] Has this alert been tested (by temporarily breaking the condition)?
[ ] Does this alert duplicate another? (if yes, delete one)
[ ] Is the team/ownership label set?
[ ] How often does this fire per week? (> 5 with no action = noise)
```

---

## Anti-Patterns

1. **Everything is CRITICAL** — engineers ignore pages; reserve CRITICAL for "data is unavailable to users right now" scenarios only.
2. **No "for" duration** — 15-second CPU spike at 2am pages the entire on-call rotation; add `for: 5m` minimum.
3. **Alert without runbook** — on-call engineer spends 20 minutes figuring out what to do; every alert must link a runbook.
4. **Threshold-based alerting on growing counters** — `errors_total > 100` fires forever; use `rate()` or `increase()` over a time window.
5. **No inhibition rules** — one flaky node triggers 50 pod alerts, flooding Slack; define inhibition for parent/child alert relationships.
6. **Never auditing alert noise** — alerts accumulate over months; run a weekly report of top-firing alerts and eliminate those with no actions taken.

---

## References

- Google SRE workbook — alerting on SLOs: `sre.google/workbook/alerting-on-slos/`
- Multi-window burn rate: `prometheus.io/docs/practices/alerting/`
- AlertManager: `prometheus.io/docs/alerting/latest/alertmanager/`
- Related skills: `[[infra-prometheus-optimization]]`, `[[infra-observability-stack-review]]`, `[[dataops-sla-monitoring]]`
