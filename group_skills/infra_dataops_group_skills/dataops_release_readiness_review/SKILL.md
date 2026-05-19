---
name: dataops-release-readiness-review
description: Release readiness review for data platforms — pre-release checklist (schema migrations tested, backward compatibility, rollback plan), runbook completeness, monitoring and alerting configured, on-call handoff, load/soak testing results, data reconciliation between versions, feature flag strategy, go/no-go criteria, post-release validation (15-min/1-hour/24-hour checkpoints), communication templates
---

# Release Readiness Review

## When to Use

- Before deploying a major data platform change to production
- Onboarding a new pipeline component (new Kafka topic, new dbt model set)
- Quarterly release readiness audit of existing data products
- After an incident — ensuring the fix is truly production-ready
- Coordinating a release involving multiple teams or systems

---

## Pre-Release Checklist

### Code Quality

```
[ ] All PR reviews completed and approved
[ ] CI pipeline passes (lint + test + security scan)
[ ] No CRITICAL/HIGH security findings from tfsec/checkov/Trivy
[ ] dbt tests pass for modified models (not_null, unique, accepted_values)
[ ] Airflow DAG imports cleanly (DagBag no errors)
[ ] Code coverage meets threshold (≥ 80% for new logic)
```

### Schema and Data Compatibility

```
[ ] Database migrations are backward-compatible (no DROP/RENAME without shim)
[ ] Migration tested on staging copy of production data
[ ] Existing consumers validated against new schema
[ ] dbt --state:modified+ tests pass in staging
[ ] Data reconciliation: row counts match between old and new pipeline version
[ ] No implicit data type changes that silently truncate values
```

### Rollback Plan

```
[ ] Rollback procedure documented in runbook
[ ] Rollback tested in staging (not just documented)
[ ] Max rollback time measured and within SLA recovery window
[ ] Data backfill plan ready if rollback needed post-cutover
[ ] Feature flag to disable new pipeline without redeployment (if applicable)
```

### Infrastructure

```
[ ] New resource limits set (CPU/memory requests tuned to staging observations)
[ ] PVCs sized correctly (verified with staging storage metrics)
[ ] HPA configured if workload is variable
[ ] Kubernetes pod disruption budget protects availability during deploy
[ ] New network policies allow required inter-service communication
```

### Monitoring and Alerting

```
[ ] Prometheus metrics emitted by new component
[ ] Grafana dashboard covers: throughput, latency, error rate, freshness
[ ] Alert rules configured for: SLA breach, error spike, freshness violation
[ ] Alert routing to correct on-call team
[ ] Runbook linked from alert (click → what to do)
```

### Operational Readiness

```
[ ] Runbook written with: deployment steps, validation commands, rollback steps
[ ] On-call engineer briefed on change
[ ] Communication sent to downstream consumers (schema changes, downtime window)
[ ] Change window confirmed with stakeholders
[ ] Support team aware of expected behavior changes
```

---

## Go/No-Go Criteria

```python
# go_no_go_check.py — automated pre-release validation
import subprocess
import sys

checks = {
    "dbt_tests_pass": lambda: run("dbt test --target staging").returncode == 0,
    "no_critical_cves": lambda: run("trivy image --exit-code 1 --severity CRITICAL myimage:latest").returncode == 0,
    "staging_pipeline_success": lambda: check_last_n_dag_runs("etl_orders", n=5, env="staging"),
    "row_count_match": lambda: reconcile_row_counts("orders_staging", "orders_prod") < 0.01,  # < 1% diff
    "runbook_exists": lambda: Path("runbooks/etl-orders-v2.md").exists(),
    "alerts_configured": lambda: check_alert_rules_exist(["etl_orders_sla", "etl_orders_freshness"]),
}

failures = [name for name, check in checks.items() if not check()]
if failures:
    print(f"GO/NO-GO: NO-GO — failed checks: {failures}")
    sys.exit(1)
else:
    print("GO/NO-GO: GO — all checks passed")
```

---

## Data Reconciliation Between Versions

```sql
-- Compare row counts: old pipeline vs new pipeline
WITH old_pipeline AS (
    SELECT COUNT(*) AS cnt, MAX(updated_at) AS max_ts
    FROM analytics_v1.fact_orders
    WHERE order_date = CURRENT_DATE - INTERVAL '1' DAY
),
new_pipeline AS (
    SELECT COUNT(*) AS cnt, MAX(updated_at) AS max_ts
    FROM analytics_v2.fact_orders
    WHERE order_date = CURRENT_DATE - INTERVAL '1' DAY
)
SELECT
    old_pipeline.cnt AS old_count,
    new_pipeline.cnt AS new_count,
    ABS(old_pipeline.cnt - new_pipeline.cnt) AS diff,
    ROUND(100.0 * ABS(old_pipeline.cnt - new_pipeline.cnt) / NULLIF(old_pipeline.cnt, 0), 2) AS diff_pct,
    CASE WHEN diff_pct < 1.0 THEN 'PASS' ELSE 'FAIL' END AS reconciliation_status
FROM old_pipeline, new_pipeline;

-- Check key metric consistency (revenue)
SELECT
    ABS(old_rev - new_rev) / NULLIF(old_rev, 0) * 100 AS revenue_diff_pct
FROM (
    SELECT SUM(order_amount) AS old_rev FROM analytics_v1.fact_orders WHERE order_date = CURRENT_DATE - 1
) old,
(
    SELECT SUM(order_amount) AS new_rev FROM analytics_v2.fact_orders WHERE order_date = CURRENT_DATE - 1
) new;
```

---

## Post-Release Validation Checkpoints

### 15-Minute Checkpoint

```bash
#!/bin/bash
# post_release_15min.sh
echo "=== 15-min post-release check ==="

# 1. Pod health
kubectl get pods -n airflow | grep -v Running | grep -v Completed

# 2. Recent errors
kubectl logs -n airflow -l component=scheduler \
  --since=15m | grep -i "error\|exception\|critical" | tail -20

# 3. Kafka consumer lag (if applicable)
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group orders-consumer --describe \
  | awk 'NR>1 {if ($6 > 1000) print "HIGH LAG:", $0}'

# 4. Pipeline success rate
psql $AIRFLOW_CONN -c "
  SELECT dag_id, state, COUNT(*) 
  FROM dag_run 
  WHERE start_date > NOW() - INTERVAL '15 minutes'
  GROUP BY dag_id, state
"
```

### 1-Hour Checkpoint

```bash
# 1. Verify first full pipeline run completed successfully
# 2. Check data freshness metrics
# 3. Confirm no SLA alerts fired
# 4. Validate downstream consumers (BI dashboards loading correctly)
```

### 24-Hour Checkpoint

```bash
# 1. Full day of pipeline runs completed without error
# 2. Data volume within ±5% of previous day
# 3. No customer-reported issues
# 4. Cleanup: scale down blue deployment, drop shadow schema
# 5. Archive rollback artifacts (60-day retention)
```

---

## Runbook Template

```markdown
# Runbook: ETL Orders v2 Deployment

## Change Summary
- Upgrading orders ETL to use incremental PRIMARY KEY strategy
- New column: customer_segment (VARCHAR 50, nullable initially)
- Estimated duration: 20 minutes

## Pre-Conditions
- [ ] Staging validation complete (see Jira DATA-1234)
- [ ] On-call: @alice (primary), @bob (backup)
- [ ] Change window: 2024-01-15 22:00-23:00 UTC

## Deployment Steps
1. `kubectl apply -f k8s/production/ -n airflow`
2. `kubectl rollout status deployment/airflow-scheduler -n airflow`
3. Run post-release checks (post_release_15min.sh)
4. Confirm pipeline success at 22:30

## Rollback Steps
1. `kubectl rollout undo deployment/airflow-scheduler -n airflow`
2. `kubectl rollout status deployment/airflow-scheduler -n airflow`
3. Notify: post in #data-eng-incidents

## Validation Commands
- Check scheduler logs: `kubectl logs -n airflow -l component=scheduler --since=15m`
- Check dag run: `airflow dags trigger etl_orders_v2 --run-id smoke_$(date +%s)`

## Contacts
- Data Engineering: #data-engineering Slack
- On-call PD: [PagerDuty link]
- Escalation: @engineering-manager
```

---

## Communication Templates

### Pre-Release (T-24h)

```
Subject: [DATA] ETL Orders v2 — Production deployment 2024-01-15 22:00 UTC

Hi team,

We're deploying ETL Orders v2 tomorrow at 22:00 UTC (maintenance window).

Changes:
- New incremental load strategy (faster, less warehouse cost)
- New customer_segment column (nullable, no breaking change)

Expected downtime: None (zero-downtime deployment)
Rollback window: 30 minutes post-deployment

If you have concerns, reply before 18:00 UTC tomorrow.

—Data Engineering
```

### Post-Release (on success)

```
✅ ETL Orders v2 deployed successfully at 22:17 UTC
- All pipelines running normally
- No alerts fired
- Rollback artifacts retained for 7 days

—Data Engineering
```

---

## Anti-Patterns

1. **No staging validation with production-scale data** — pipelines that pass on 1% sample fail on full load; always test with a production copy.
2. **Runbook written after deployment** — under-pressure rollback without a runbook causes mistakes; runbook must be reviewed and approved pre-deploy.
3. **Skipping 15-minute checkpoint** — most deployment issues surface in the first pipeline run; don't mark deployment complete without a validation window.
4. **No downstream consumer notification** — BI teams discover broken dashboards before data engineers know there's a problem; communicate schema changes 48h in advance.
5. **Deleting rollback artifacts immediately** — if issues appear 12 hours later, no way to roll back; keep blue deployment/old schema for at least 24 hours.

---

## References

- Google SRE: production readiness review: `sre.google/sre-book/evolving-sre-engagement-model/`
- dbt state-based CI: `docs.getdbt.com/reference/node-selection/methods#the-state-method`
- Change management: `itil4.org/`
- Related skills: `[[dataops-cicd-pipeline-review]]`, `[[dataops-blue-green-deployment]]`, `[[de-production-readiness]]`, `[[de-postmortem]]`
