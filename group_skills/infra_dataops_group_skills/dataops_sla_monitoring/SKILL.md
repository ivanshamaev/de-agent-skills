---
name: dataops-sla-monitoring
description: Data platform SLA monitoring — freshness SLAs (data available by X:XX), completeness SLAs (row count variance < N%), latency SLAs (pipeline completes within N minutes), SLA breach detection (Prometheus/Soda/SQL), SLA burn rate alerting, SLA reporting dashboards, SLA miss root cause linking, cascading SLA dependencies (upstream delay propagation), SLA definition process, error budget tracking, consumer notification on SLA breach
---

# DataOps SLA Monitoring

## When to Use

- Defining SLAs for data products consumed by BI/analytics
- Setting up freshness and completeness monitoring
- Alerting consumers when a data SLA is at risk
- Tracking error budget consumption across pipelines
- Investigating an SLA miss root cause

---

## SLA Definition Framework

```yaml
# sla_definitions.yml — version-controlled SLA catalog
version: "1.0"
slas:
  - name: orders_daily_freshness
    description: "Daily orders data must be available by 06:00 UTC"
    dataset: gold.fact_orders
    type: freshness
    target:
      available_by: "06:00 UTC"
      timezone: UTC
    breach_threshold_minutes: 60    # SLA at risk if > 1h late
    owner_team: data-engineering
    consumer_teams: [analytics, bi-team, finance]
    severity: high
    error_budget_minutes_per_month: 120   # 2h downtime/month budget

  - name: orders_completeness
    description: "Daily orders row count within 1% of previous week's same-day"
    dataset: gold.fact_orders
    type: completeness
    target:
      max_row_count_variance_pct: 1.0
      lookback_days: 7
    owner_team: data-engineering
    severity: medium

  - name: kafka_consumer_latency
    description: "Orders topic consumer lag < 10,000 messages at all times"
    type: streaming_latency
    target:
      max_lag: 10000
      consumer_group: orders-processor
    severity: high
```

---

## Freshness SLA Monitoring

### Prometheus-Based

```yaml
# Recording rule: seconds since last successful pipeline run
groups:
  - name: sla_recordings
    rules:
      - record: pipeline_last_success_timestamp
        expr: |
          max by (dag_id) (
            airflow_dagrun_end_date{state="success"}
          )

      - record: pipeline_freshness_seconds
        expr: |
          time() - pipeline_last_success_timestamp

# Alert rules
  - name: sla_alerts
    rules:
      - alert: DataFreshnessAtRisk
        expr: |
          pipeline_freshness_seconds{dag_id="etl_orders"} > 10800   # 3h = at risk
        for: 5m
        labels:
          severity: warning
          sla: orders_daily_freshness
        annotations:
          summary: "Orders data freshness at risk"
          description: "Last successful run was {{ $value | humanizeDuration }} ago (SLA: 6h)"

      - alert: DataFreshnessSLABreached
        expr: |
          pipeline_freshness_seconds{dag_id="etl_orders"} > 21600   # 6h = breached
        labels:
          severity: critical
          sla: orders_daily_freshness
```

### SQL-Based Freshness Check

```sql
-- SLA freshness check (run every 15 min via monitoring DAG)
WITH sla_definitions AS (
    SELECT 'gold.fact_orders' AS table_name,
           360 AS max_age_minutes,     -- 6h SLA
           'orders_daily_freshness' AS sla_name
),
table_freshness AS (
    SELECT
        'gold.fact_orders' AS table_name,
        MAX(updated_at) AS last_update,
        TIMESTAMPDIFF(MINUTE, MAX(updated_at), NOW()) AS age_minutes
    FROM gold.fact_orders
    WHERE order_date = CURRENT_DATE
)
SELECT
    d.sla_name,
    d.table_name,
    f.last_update,
    f.age_minutes,
    d.max_age_minutes,
    CASE
        WHEN f.age_minutes IS NULL THEN 'NO_DATA'
        WHEN f.age_minutes > d.max_age_minutes THEN 'BREACHED'
        WHEN f.age_minutes > d.max_age_minutes * 0.8 THEN 'AT_RISK'
        ELSE 'OK'
    END AS sla_status
FROM sla_definitions d
LEFT JOIN table_freshness f USING (table_name);
```

---

## Completeness SLA

```python
# Soda Core check for completeness SLA
# checks/gold/fact_orders.yml
checks for gold.fact_orders:
  - row_count:
      name: "Daily orders completeness SLA"
      fail:
        when not between:
          min_percentage: -1.0   # allow up to 1% fewer rows than last week
          max_percentage: 200.0  # alert on sudden 2x spike
        previous_period: 7d
        group_by: order_date

  - anomaly detection for row_count:
      name: "Anomaly detection: orders volume"
      warn:
        when > 3 stdevs
```

```python
# Python: row count SLA check against last week same-day
import pendulum
from sqlalchemy import create_engine

def check_completeness_sla(partition_date: str) -> dict:
    engine = create_engine(TRINO_URI)

    last_week = pendulum.parse(partition_date).subtract(days=7).date()

    current_count = engine.execute(
        f"SELECT COUNT(*) FROM gold.fact_orders WHERE order_date = '{partition_date}'"
    ).scalar()

    reference_count = engine.execute(
        f"SELECT COUNT(*) FROM gold.fact_orders WHERE order_date = '{last_week}'"
    ).scalar()

    if reference_count == 0:
        return {"status": "NO_REFERENCE", "current": current_count}

    variance_pct = abs(current_count - reference_count) / reference_count * 100

    return {
        "status": "BREACHED" if variance_pct > 1.0 else "OK",
        "current_count": current_count,
        "reference_count": reference_count,
        "variance_pct": round(variance_pct, 2),
        "partition_date": partition_date,
    }
```

---

## Error Budget Tracking

```python
# error_budget_tracker.py — track monthly SLA budget consumption
from datetime import datetime, timedelta
import psycopg2

def calculate_error_budget(sla_name: str, month: datetime) -> dict:
    """Calculate error budget for a given SLA in a calendar month."""

    # SLA definition: 99.5% uptime = 3.6h budget per 30-day month
    target_pct = 99.5
    month_minutes = 30 * 24 * 60  # 43,200 minutes
    budget_minutes = month_minutes * (1 - target_pct / 100)  # 216 minutes

    # Actual downtime (minutes pipeline data was stale beyond SLA)
    conn = psycopg2.connect(METADATA_DB)
    actual_downtime = conn.execute("""
        SELECT COALESCE(SUM(duration_minutes), 0) AS total_breach_minutes
        FROM sla_breach_log
        WHERE sla_name = %s
          AND breach_start >= %s
          AND breach_start < %s
    """, (sla_name, month.replace(day=1), (month + timedelta(days=31)).replace(day=1))).fetchone()[0]

    remaining_budget = budget_minutes - actual_downtime
    budget_consumed_pct = actual_downtime / budget_minutes * 100

    return {
        "sla_name": sla_name,
        "month": month.strftime("%Y-%m"),
        "budget_minutes": budget_minutes,
        "consumed_minutes": actual_downtime,
        "remaining_minutes": remaining_budget,
        "budget_consumed_pct": round(budget_consumed_pct, 1),
        "status": "EXHAUSTED" if remaining_budget <= 0 else
                  "AT_RISK" if budget_consumed_pct > 50 else "OK",
    }
```

---

## Consumer Notification on SLA Breach

```python
# Slack notification on SLA breach (triggered by Airflow SLA callback)
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    import requests

    consumers = get_sla_consumers(dag.dag_id)   # from SLA catalog
    message = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"⚠️ Data SLA Breach: {dag.dag_id}"}
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text":
                    f"*DAG:* `{dag.dag_id}`\n"
                    f"*Blocked tasks:* {[t.task_id for t in blocking_task_list]}\n"
                    f"*SLA:* Data should have been available by 06:00 UTC\n"
                    f"*Impact:* Affects dashboards consumed by {', '.join(consumers)}"
                }
            },
            {
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View in Airflow"},
                    "url": f"http://airflow.internal/dags/{dag.dag_id}/grid",
                }]
            }
        ]
    }

    # Notify data-eng + all consumer channels
    for channel in ["#data-eng-sla", "#analytics-alerts", "#bi-team"]:
        requests.post(SLACK_WEBHOOK, json={"channel": channel, **message})
```

---

## SLA Reporting Dashboard (SQL)

```sql
-- Weekly SLA performance report
SELECT
    sla_name,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN status = 'OK' THEN 1 ELSE 0 END) AS ok_count,
    SUM(CASE WHEN status = 'BREACHED' THEN 1 ELSE 0 END) AS breach_count,
    ROUND(100.0 * SUM(CASE WHEN status = 'OK' THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate_pct,
    ROUND(AVG(CASE WHEN status = 'BREACHED' THEN breach_duration_minutes END), 1) AS avg_breach_min,
    MAX(CASE WHEN status = 'BREACHED' THEN breach_duration_minutes END) AS max_breach_min
FROM sla_check_log
WHERE check_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY sla_name
ORDER BY success_rate_pct;
```

---

## Anti-Patterns

1. **No SLA defined = no breach possible** — teams learn about stale data from angry Slack messages; define SLAs before deploying pipelines.
2. **SLA threshold = pipeline max runtime** — SLA should be defined from the consumer's perspective (data available by X time), not pipeline runtime.
3. **Alerting only the data engineering team** — consumers don't know data is late and keep using stale numbers; notify consumer teams too.
4. **Error budget not tracked** — teams keep burning budget without realizing it; track and report monthly budget consumption.
5. **Freshness measured at pipeline completion** — the pipeline might complete but load 0 rows; always check actual data recency, not just pipeline status.

---

## References

- Google SRE: SLOs: `sre.google/sre-book/service-level-objectives/`
- Soda SodaCL freshness: `docs.soda.io/soda-cl/freshness.html`
- Related skills: `[[dataops-airflow-production-readiness]]`, `[[infra-alert-fatigue-reduction]]`, `[[de-production-readiness]]`, `[[soda-core]]`
