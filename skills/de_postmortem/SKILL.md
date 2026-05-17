---
name: de-postmortem-writer
description: DE blameless postmortem — incident timeline reconstruction, impact quantification, root cause chain (5 Whys), corrective actions (CAPA), postmortem document template, severity classification, SLA breach reporting, communication templates, lessons-learned facilitation, postmortem review checklist
---

# DE Blameless Postmortem Writer

## When to Use

Load this skill when the user needs to:
- Write a postmortem after an SLA breach — a pipeline completed too late or not at all
- Document a data quality incident that affected business reports, dashboards, or downstream consumers
- Analyze and communicate a pipeline outage lasting more than 1 hour
- Handle a security or compliance incident (data exposure, unauthorized access, PII leak)
- Process a near-miss with high blast radius — something that almost caused a major incident
- Facilitate a postmortem review meeting and drive blameless root cause analysis
- Build a postmortem repository and tracking system for a data engineering team

---

## 1. Blameless Culture Foundation

### Why Blameless Postmortems Produce Better Outcomes

Blame-driven postmortems optimize for punishment. Blameless postmortems optimize for learning. When engineers fear blame, they:
- Under-report near-misses and partial failures
- Avoid experimenting with improvements (too risky)
- Optimize for personal safety over system improvement

Blameless postmortems produce:
- Accurate, complete timelines (no self-protective omissions)
- Systemic fixes rather than personnel changes
- Psychological safety to raise future concerns early

The foundational principle: **"Every person who contributed to the incident was acting rationally given the information, tools, and incentives available to them at that moment."**

### Psychological Safety in Incident Reviews

Concrete practices:
- Open the meeting by explicitly stating: "This is a blameless review. We are here to learn what happened, not to assign fault."
- Prohibit "why did you do X?" questions directed at individuals. Redirect to "what was the system state that led to X?"
- Praise people who made the incident visible, even if they caused it.
- Rotate the facilitator role — do not let the person with the most authority run the meeting.
- Document "what went well" as sincerely as "what went poorly."

### Human Error Is a Symptom, Not a Cause

When "human error" appears in a root cause statement, it is a signal that the analysis stopped too early. Human errors are predictable outputs of systems that make errors easy to make and hard to detect.

| Blame Statement | System Observation Reframe |
|---|---|
| "Engineer deployed bad code on Friday afternoon" | "The change management process allowed unreviewed deployments during high-risk windows" |
| "The analyst used the wrong date filter" | "The data model had no validation layer to catch date filter errors before data was published" |
| "On-call didn't respond for 30 minutes" | "The alerting system sent only a single page with no escalation to backup on-call" |
| "Developer forgot to add a test" | "The CI pipeline did not require row-count tests for incremental models" |
| "Team lead approved a risky migration without staging" | "The deployment process had no mandatory staging environment gate for schema changes" |

---

## 2. Incident Severity Classification

### Severity Matrix: Impact × Urgency

```
                    HIGH URGENCY          LOW URGENCY
                  (response < 1h)       (response < 4h)
HIGH IMPACT   |      SEV-1            |     SEV-2        |
LOW IMPACT    |      SEV-2            |     SEV-3/4      |
```

| Severity | Definition | Postmortem Required | Response Time |
|---|---|---|---|
| SEV-1 | Business-critical data unavailable; regulatory or financial reporting at risk; executive-visible dashboards down; SLA breach with contractual penalty | Always (within 48h) | 15 minutes |
| SEV-2 | Major pipeline failure affecting ≥1 downstream team; SLA breach without contractual penalty; data unavailable >2h; incorrect data in production dashboards | Always (within 72h) | 1 hour |
| SEV-3 | Degraded data quality in non-critical tables; partial pipeline failure with workaround available; single non-customer-facing report delayed | Team discretion | 4 hours |
| SEV-4 | Minor issue; single non-critical pipeline delayed <1h; no user impact; self-healing | Not required | Next business day |

### SEV-1 Indicators (Escalate Immediately)
- Financial close, regulatory filing, or executive report blocked by data unavailability
- PII or confidential data exposed to unauthorized parties
- Production database in inconsistent state with no clear recovery path
- Revenue recognition pipeline producing materially incorrect numbers ($10k+)
- Data loss — rows deleted or overwritten with no recovery path

### SEV-2 Indicators
- One or more team's analytical workflows blocked for >2h
- SLA to internal customers breached by >1h
- Data quality issue discovered in published reports, requiring correction and re-communication
- Kafka consumer lag >4h on business-critical topic

### Postmortem Trigger Threshold Summary
- SEV-1: always required, draft within 24h, final within 5 business days
- SEV-2: always required, draft within 48h, final within 7 business days
- SEV-3: required if recurring (same system, second time in 30 days), optional otherwise
- SEV-4: not required; log in incident tracking system only

---

## 3. Postmortem Document Template

Copy and fill this template for every SEV-1 and SEV-2 incident.

```markdown
# Postmortem: <Short Descriptive Title>

| Field           | Value |
|-----------------|-------|
| **Incident ID** | INC-YYYY-NNN |
| **Severity**    | SEV-1 / SEV-2 |
| **Status**      | Draft / In Review / Final |
| **Date**        | YYYY-MM-DD |
| **Duration**    | HH:MM (from first impact to full resolution) |
| **Authors**     | @engineer1, @engineer2 |
| **Stakeholders**| @manager, @data-consumer-team |
| **Reviewers**   | @peer-engineer (not involved in incident) |

---

## Executive Summary

<!-- 2–3 sentences, non-technical. Explain what broke, who was affected,
     and what was done to fix it. Assume the reader is a business stakeholder. -->

On [DATE], the [SYSTEM] pipeline experienced [BRIEF DESCRIPTION], resulting in
[BUSINESS IMPACT] for approximately [DURATION]. The issue was caused by [1-SENTENCE
ROOT CAUSE]. The team resolved the incident by [1-SENTENCE FIX] and has taken
[N] preventive actions to ensure this class of failure cannot recur.

---

## Impact

| Metric | Value |
|--------|-------|
| Total duration | N hours N minutes |
| Rows affected | N (table: schema.table_name, date range: YYYY-MM-DD to YYYY-MM-DD) |
| Tables affected | N tables (list: schema.table1, schema.table2) |
| Downstream systems | N dashboards, N pipelines, N API consumers |
| Business impact | [e.g., Revenue reporting delayed 4h; manual reconciliation effort: 8 person-hours] |
| Financial impact | [$NNN estimated, or "not quantified"] |
| SLA breach | [e.g., SLA target: data available by 08:00 UTC; actual: 11:32 UTC; breach duration: 3h 32m] |

---

## Timeline (UTC)

| Timestamp (UTC) | Event | Who |
|-----------------|-------|-----|
| YYYY-MM-DD HH:MM | [Root cause event — deploy, config change, upstream failure] | [system / person] |
| YYYY-MM-DD HH:MM | [First data impact — pipeline failed, data became stale] | [system] |
| YYYY-MM-DD HH:MM | [Detection — alert fired / stakeholder reported / manual check] | [monitoring / person] |
| YYYY-MM-DD HH:MM | [Acknowledgment — on-call paged and responded] | [@engineer] |
| YYYY-MM-DD HH:MM | [Escalation — if applicable] | [@manager] |
| YYYY-MM-DD HH:MM | [Root cause identified] | [@engineer] |
| YYYY-MM-DD HH:MM | [Fix deployed / data backfill started] | [@engineer] |
| YYYY-MM-DD HH:MM | [Data validated — correct data confirmed in target tables] | [@engineer] |
| YYYY-MM-DD HH:MM | [Stakeholders notified of resolution] | [@engineer / @manager] |

**Key SLA metrics:**
- Time to detect (TTD): HH:MM from first impact to detection
- Time to acknowledge (TTA): HH:MM from alert to on-call response
- Time to resolve (TTR): HH:MM from acknowledgment to resolution

---

## Detection

How was the incident discovered?

- [ ] Automated monitoring alert (alert name: ___)
- [ ] User/stakeholder report (reported by: ___, via: Slack/email/Jira)
- [ ] Manual check during routine review
- [ ] External party notification

**Gap (if applicable):** _If detection was not automated, explain why and what would have detected it faster._

---

## Response

- **On-call engineer paged:** @engineer at HH:MM UTC
- **Escalation chain:** @engineer → @team-lead (HH:MM) → @manager (if applicable, HH:MM)
- **Communication channel:** #incident-YYYY-NNN Slack channel created at HH:MM
- **Stakeholder notification sent:** HH:MM UTC (template in Appendix)

---

## Root Cause

> **The root cause was [SYSTEM/PROCESS X], which was triggered by [CHANGE/CONDITION Y],
> resulting in [FAILURE MECHANISM Z].**

Example:
> The root cause was the absence of a consumer group configuration freeze policy,
> which was triggered by an upstream team increasing the Kafka topic partition count
> without coordination, resulting in a consumer group rebalance storm that halted
> message processing for 4 hours.

---

## Contributing Factors

1. [Factor 1 — condition that made root cause possible]
2. [Factor 2 — condition that made detection slower]
3. [Factor 3 — condition that made recovery harder]
4. [Factor 4 — process gap or missing safeguard]

---

## 5-Whys Root Cause Chain

| Why # | Question | Answer |
|-------|----------|--------|
| 1 | Why did [SYMPTOM observed by users] occur? | Because [DIRECT CAUSE] |
| 2 | Why did [DIRECT CAUSE] occur? | Because [INTERMEDIATE CAUSE] |
| 3 | Why did [INTERMEDIATE CAUSE] occur? | Because [DEEPER CAUSE] |
| 4 | Why did [DEEPER CAUSE] occur? | Because [SYSTEMIC CAUSE] |
| 5 | Why did [SYSTEMIC CAUSE] occur? | Because [ROOT SYSTEM/PROCESS GAP] |

**Root cause (from Why 5):** [SYSTEM OR PROCESS GAP — this is what the CAPA actions must address]

---

## What Went Well

<!-- This section must be genuine. Find at least 3 real positives. -->

1. [e.g., The on-call runbook for Kafka consumer lag was accurate and reduced TTR by 30 minutes]
2. [e.g., The monitoring alert fired within 8 minutes of the first failure]
3. [e.g., The team communicated proactively to stakeholders throughout the incident]

---

## What Went Poorly

1. [e.g., No automated check existed for consumer group rebalance events]
2. [e.g., The escalation path was unclear — two engineers were paged simultaneously for different symptoms]
3. [e.g., Data backfill took 2h longer than expected due to undocumented dependency on a manual step]

---

## Corrective Actions (CAPA)

| # | Action | Owner | Due Date | Status | Prevention Type |
|---|--------|-------|----------|--------|-----------------|
| 1 | [Specific action — what exactly will be done] | @owner | YYYY-MM-DD | Open | Detection |
| 2 | [Specific action] | @owner | YYYY-MM-DD | Open | Prevention |
| 3 | [Specific action] | @owner | YYYY-MM-DD | Open | Response |
| 4 | [Specific action] | @owner | YYYY-MM-DD | Open | Process |
| 5 | [Specific action] | @owner | YYYY-MM-DD | Open | Prevention |

Prevention Types: **Detection** (alerting/monitoring) | **Prevention** (code gates/contracts) | **Response** (runbooks/escalation) | **Process** (change management/testing)

---

## Lessons Learned

1. [Transferable insight — applicable beyond this incident]
2. [Architecture or design lesson]
3. [Process lesson]

---

## Appendix

- Relevant logs: [link or inline excerpt]
- SQL queries used in investigation: [inline or link]
- Grafana/dashboard screenshots: [link]
- Slack incident thread: [link]
- Related tickets/PRs: [link]
```

---

## 4. Complete Postmortem Examples

---

### Example 1: Late-Arriving Data SLA Breach (Kafka Partition Rebalance Storm)

```markdown
# Postmortem: Silver Layer SLA Breach — Kafka Consumer Rebalance Storm

| Field           | Value |
|-----------------|-------|
| **Incident ID** | INC-2025-047 |
| **Severity**    | SEV-2 |
| **Status**      | Final |
| **Date**        | 2025-03-14 |
| **Duration**    | 4h 12m (07:18 UTC – 11:30 UTC) |
| **Authors**     | @ali.okonkwo, @priya.nair |
| **Stakeholders**| @data-consumers, @bi-team, @ops-manager |
| **Reviewers**   | @dmitri.volkov (not involved) |

---

## Executive Summary

On March 14, 2025, the Silver layer of the order events pipeline experienced a
4-hour processing halt after an upstream team increased the Kafka topic partition
count from 12 to 48 without coordinating with the data engineering team. This
triggered a consumer group rebalance storm that suspended all message consumption,
causing 12 downstream dashboards to show stale data for 4 hours and breaching the
08:00 UTC SLA by 3h 30m. The team resolved the incident by scaling the consumer
group and reprocessing from committed offsets. Five preventive actions have been
opened to eliminate this failure mode.

---

## Impact

| Metric | Value |
|--------|-------|
| Total duration | 4h 12m |
| Rows affected | ~18.4M events (order_events topic, 2025-03-14 03:00–07:18 UTC backlog) |
| Tables affected | 3 tables: silver.order_events, silver.order_events_hourly_agg, gold.daily_revenue |
| Downstream systems | 12 Grafana dashboards, 2 Airflow DAGs (daily_revenue_load, churn_model_feature_gen) |
| Business impact | Morning revenue standup delayed 3.5h; Finance team ran manual estimates for board pack |
| Financial impact | 6 person-hours manual reconciliation work (~$900 at fully-loaded rate) |
| SLA breach | Target: Silver layer available by 08:00 UTC; actual: 11:30 UTC; breach: 3h 30m |

---

## Timeline (UTC)

| Timestamp (UTC)      | Event                                                                                     | Who                  |
|----------------------|-------------------------------------------------------------------------------------------|----------------------|
| 2025-03-14 03:00     | Upstream platform team increases `order_events` topic from 12 → 48 partitions            | @upstream-platform   |
| 2025-03-14 03:02     | PySpark Structured Streaming job detects partition count change, triggers rebalance        | Kafka broker / Spark |
| 2025-03-14 03:02     | Consumer group enters continuous rebalance loop; message processing halts                  | system               |
| 2025-03-14 03:02     | Kafka consumer lag begins growing (~3M msg/h ingestion rate, 0 msg consumed)              | system               |
| 2025-03-14 07:18     | Automated consumer lag alert fires: lag > 10M messages on `orders-silver-consumer`        | PagerDuty            |
| 2025-03-14 07:23     | On-call @ali.okonkwo acknowledges page                                                    | @ali.okonkwo         |
| 2025-03-14 07:25     | @ali.okonkwo checks Grafana consumer lag dashboard; confirms processing halted since 03:02| @ali.okonkwo         |
| 2025-03-14 07:40     | Identifies partition count change in Kafka topic metadata                                  | @ali.okonkwo         |
| 2025-03-14 07:45     | Escalates to @priya.nair; creates #incident-2025-047 Slack channel                       | @ali.okonkwo         |
| 2025-03-14 07:47     | Stakeholder notification sent to #bi-team and #data-consumers                             | @priya.nair          |
| 2025-03-14 08:00     | SLA breach confirmed; @ops-manager notified                                                | @priya.nair          |
| 2025-03-14 08:15     | Streaming job restarted with updated consumer group config (minPartitions=48)             | @ali.okonkwo         |
| 2025-03-14 08:20     | Rebalance resolves; lag begins draining at ~9M msg/h                                      | system               |
| 2025-03-14 11:10     | Consumer lag reaches zero; Silver layer data current                                       | system               |
| 2025-03-14 11:30     | gold.daily_revenue recomputed; dashboards validated by @priya.nair                        | @priya.nair          |
| 2025-03-14 11:35     | Resolution notification sent to stakeholders                                               | @priya.nair          |

**Key SLA metrics:**
- Time to detect (TTD): 4h 16m (03:02 → 07:18) — alert threshold too high
- Time to acknowledge (TTA): 5 minutes (07:18 → 07:23)
- Time to resolve (TTR): 4h 7m (07:23 → 11:30)

---

## Detection

- [x] Automated monitoring alert — PagerDuty: "Consumer lag > 10M on orders-silver-consumer"

**Gap:** The alert threshold of 10M messages took 4+ hours to breach. A lower threshold
(e.g., 500K messages = ~10 minutes of lag) would have detected the rebalance storm within
15 minutes of start. The rebalance event itself had no dedicated alert.

---

## Response

- **On-call engineer paged:** @ali.okonkwo at 07:18 UTC
- **Escalation:** @priya.nair engaged at 07:45 UTC
- **Communication channel:** #incident-2025-047 created at 07:45 UTC
- **Stakeholder notification:** First update at 07:47 UTC; resolution at 11:35 UTC

---

## Root Cause

> The root cause was the absence of a cross-team change coordination process for Kafka
> topic configuration changes, which was triggered by the upstream platform team
> increasing partition count from 12 to 48 without notifying data engineering, resulting
> in a consumer group rebalance storm that halted all message processing for 4 hours 12 minutes.

---

## Contributing Factors

1. The consumer lag alert threshold (10M messages) was set during initial deployment and never tuned; it was too high to detect rapid halts.
2. No dedicated alert existed for Kafka consumer group rebalance events.
3. The PySpark Structured Streaming job was not configured with `minPartitions` to handle topic partition increases gracefully.
4. The upstream platform team's change management process did not include downstream consumer notification as a required step.
5. The runbook for consumer lag incidents did not include Kafka partition count changes as a diagnostic step.

---

## 5-Whys Root Cause Chain

| Why # | Question | Answer |
|-------|----------|--------|
| 1 | Why were 12 downstream dashboards showing stale data? | Because silver.order_events stopped receiving new rows at 03:02 UTC |
| 2 | Why did silver.order_events stop receiving new rows? | Because the PySpark Structured Streaming consumer halted all processing |
| 3 | Why did the consumer halt processing? | Because the Kafka topic partition count changed from 12 to 48, triggering a continuous consumer group rebalance loop |
| 4 | Why did the rebalance loop not resolve on its own? | Because the streaming job's `minPartitions` config was set to 12, causing it to repeatedly rebalance as partition assignment changed |
| 5 | Why was the rebalance not anticipated or mitigated? | Because there was no cross-team notification process for Kafka topic configuration changes, and the consumer job had no partition-change resilience configuration |

**Root cause:** No inter-team change coordination process for shared Kafka topics existed.

---

## What Went Well

1. Once identified, the on-call engineer diagnosed the root cause (partition count change) in under 15 minutes using Kafka topic metadata inspection.
2. The team communicated proactively to stakeholders at 07:47 UTC, preventing the BI team from using stale data in the 09:00 board call.
3. Committed offsets were intact; no data was lost — only delayed. The backfill completed cleanly without manual intervention.
4. The existing consumer lag Grafana dashboard made the growth pattern immediately obvious once the engineer logged in.

---

## What Went Poorly

1. Alert threshold was far too high — 4h 16m TTD is unacceptable for a SEV-2.
2. No alert existed for the rebalance event itself (zero processed messages for >5 minutes).
3. The streaming job's partition configuration was brittle — a valid upstream operational change immediately caused a production outage.
4. The incident runbook did not list "check Kafka topic partition count" as a diagnostic step for consumer lag.
5. There was no cross-team change process — the upstream team acted within their own system's policies but inadvertently broke a downstream consumer.

---

## Corrective Actions (CAPA)

| # | Action | Owner | Due Date | Status | Prevention Type |
|---|--------|-------|----------|--------|-----------------|
| 1 | Reduce consumer lag alert threshold from 10M to 500K messages; add a second alert for "0 messages processed in 5 minutes" | @ali.okonkwo | 2025-03-21 | Open | Detection |
| 2 | Add PagerDuty alert for Kafka consumer group rebalance events lasting >2 minutes (via JMX `rebalance-latency-avg` metric) | @platform-team | 2025-03-28 | Open | Detection |
| 3 | Update all PySpark Structured Streaming Kafka consumer jobs to set `minPartitions` dynamically from topic partition count at startup | @priya.nair | 2025-03-28 | Open | Prevention |
| 4 | Establish a "Kafka Topic Change RFC" process: any partition count, retention, or compaction change to a shared topic requires 48h advance notice to all registered consumers | @data-arch | 2025-04-11 | Open | Process |
| 5 | Update consumer lag runbook to include "check Kafka topic partition count change" as Step 2 in the diagnostic flow | @ali.okonkwo | 2025-03-21 | Open | Response |
| 6 | Implement a Kafka topic config change notification bot: when any topic property changes, post to #kafka-changes Slack channel with consumer group list | @platform-team | 2025-04-18 | Open | Detection |

---

## Lessons Learned

1. Shared infrastructure changes (Kafka topics, schema registry, Hive metastore) require downstream consumer impact analysis before execution — not just operational correctness in isolation.
2. Alert thresholds set at system launch are almost always too conservative; they must be reviewed quarterly against actual incident data.
3. Resilience to upstream operational changes (partition count increases) must be built into streaming consumers at the code level, not relied upon via process controls alone.
```

---

### Example 2: Incremental dbt Model Data Loss (Silent Row Drop)

```markdown
# Postmortem: 2M Row Data Loss in dbt Incremental Model — 3 Days of Incorrect Revenue Reports

| Field           | Value |
|-----------------|-------|
| **Incident ID** | INC-2025-063 |
| **Severity**    | SEV-1 |
| **Status**      | Final |
| **Date**        | 2025-04-09 |
| **Duration**    | 73h (2025-04-06 06:00 UTC – 2025-04-09 07:00 UTC) |
| **Authors**     | @chen.wei, @fatima.al-rashid |
| **Stakeholders**| @cfo-office, @revenue-team, @data-consumers, @compliance |
| **Reviewers**   | @independent-reviewer (not involved) |

---

## Executive Summary

Between April 6 and April 9, 2025, the `mart.fact_revenue` dbt model silently dropped
approximately 2 million rows, causing three days of materially understated revenue figures
in the Finance team's daily revenue report and the executive dashboard. The root cause was
an incremental model filter that referenced the column `event_created_at`, which had been
renamed to `event_occurred_at` in a source schema migration on April 6. The column rename
caused the `is_incremental()` filter to evaluate to always-false (returning zero rows from
source), so each daily dbt run appended zero rows rather than the expected ~700K rows per day.
The issue was discovered on April 9 when a Finance analyst noticed a 3-day trend divergence
against a separate transactional report. A full-refresh backfill was completed successfully.
All downstream reports have been corrected and Finance has been notified. Six CAPA actions are
open to prevent silent data loss of this class.

---

## Impact

| Metric | Value |
|--------|-------|
| Total duration | 73 hours (3 calendar days of incorrect data) |
| Rows affected | ~2.1M rows missing from mart.fact_revenue (2025-04-06 through 2025-04-08) |
| Tables affected | 4 tables: mart.fact_revenue, mart.revenue_daily_summary, gold.executive_kpis, rpt.finance_daily |
| Downstream systems | 2 executive dashboards, 1 Finance daily report, 1 CFO board pack, 1 compliance filing draft |
| Business impact | Revenue understated by ~$4.2M over 3 days in Finance reports; CFO board pack required manual correction |
| Financial impact | Compliance review required ($15K estimated external audit cost); 12 person-hours manual correction |
| SLA breach | Revenue report available by 07:00 UTC daily; data was available but materially incorrect |

---

## Timeline (UTC)

| Timestamp (UTC)      | Event                                                                                                  | Who                     |
|----------------------|--------------------------------------------------------------------------------------------------------|-------------------------|
| 2025-04-05 22:00     | Source team deploys schema migration: renames `event_created_at` → `event_occurred_at` in raw.events  | @source-platform-team   |
| 2025-04-06 06:00     | dbt daily run executes; `is_incremental()` filter references `event_created_at` (now nonexistent)     | Airflow / dbt           |
| 2025-04-06 06:00     | dbt run completes with status SUCCESS (zero rows appended; no row-count test exists)                   | dbt                     |
| 2025-04-06 06:02     | mart.fact_revenue missing ~680K rows for 2025-04-05; downstream tables propagate incorrect totals     | system                  |
| 2025-04-06–08        | Finance team reviews reports; slight revenue dip noted but attributed to "weekend effect"              | Finance analyst          |
| 2025-04-09 09:15     | @finance-analyst-sara notices 3-day divergence vs transactional ERP system; raises in #data-questions | @finance-analyst-sara   |
| 2025-04-09 09:22     | @chen.wei joins Slack thread; begins investigation                                                     | @chen.wei               |
| 2025-04-09 09:45     | @chen.wei confirms row count drop: 0 rows appended April 6–8; finds column rename in source changelog | @chen.wei               |
| 2025-04-09 09:55     | Severity upgraded to SEV-1; @fatima.al-rashid and @cfo-office notified                                | @chen.wei               |
| 2025-04-09 10:00     | #incident-2025-063 created; stakeholder communication sent                                             | @fatima.al-rashid       |
| 2025-04-09 10:15     | dbt model fix: rename `event_created_at` → `event_occurred_at` in incremental filter                  | @chen.wei               |
| 2025-04-09 10:30     | Full-refresh backfill started for 2025-04-05 through 2025-04-08                                       | @chen.wei               |
| 2025-04-09 12:45     | Backfill complete; row counts validated against source raw table                                       | @chen.wei               |
| 2025-04-09 13:00     | Executive dashboard and Finance report updated; Finance team notified                                  | @fatima.al-rashid       |
| 2025-04-09 14:00     | CFO board pack correction issued                                                                       | Finance team            |
| 2025-04-09 15:00     | Compliance team notified; regulatory filing placed on hold pending review                              | @fatima.al-rashid       |

**Key SLA metrics:**
- Time to detect (TTD): 73 hours (issue silent; no automated detection)
- Time to acknowledge (TTA): 7 minutes after user report (09:15 → 09:22)
- Time to resolve (TTR): 3h 38m after acknowledgment (09:22 → 13:00)

---

## Detection

- [x] User/stakeholder report — @finance-analyst-sara via #data-questions Slack at 09:15 UTC

**Gap:** No automated check existed for:
- Row count delta between consecutive daily dbt runs (an append of 0 rows should alert)
- Row count reconciliation between source (raw.events) and target (mart.fact_revenue)
- Schema drift between source tables and dbt model SQL at compile time
- dbt column reference validation against live source schema

---

## Response

- **Issue raised by:** @finance-analyst-sara (Finance) at 09:15 UTC
- **On-call engineer engaged:** @chen.wei at 09:22 UTC
- **Escalation:** @fatima.al-rashid (team lead) at 09:55 UTC; @cfo-office at 09:55 UTC
- **Communication channel:** #incident-2025-063 created at 10:00 UTC
- **Stakeholder notifications:** Initial at 10:00 UTC; resolution at 13:00 UTC; compliance at 15:00 UTC

---

## Root Cause

> The root cause was the absence of a schema drift detection gate between source systems
> and dbt model SQL, which was triggered by the source platform team renaming the column
> `event_created_at` to `event_occurred_at` without notifying data engineering, resulting
> in the `is_incremental()` filter returning zero rows and dbt silently appending no data
> for 3 days while reporting run status as SUCCESS.

---

## Contributing Factors

1. The dbt model's `is_incremental()` filter used a column name that was not validated against the live source schema at compile or run time.
2. No row-count reconciliation check existed — appending zero rows to `mart.fact_revenue` was indistinguishable from a legitimate low-volume period.
3. The source team's schema migration process did not include downstream consumer impact analysis or notification.
4. The weekend "natural dip" heuristic caused Finance analysts to dismiss the first 2 days of anomalous data.
5. `mart.fact_revenue` had no dbt snapshot, making point-in-time comparison impossible without raw source requery.
6. The CI pipeline for dbt only ran `dbt compile` and `dbt test --select state:modified` — the column rename was not a modified node, so no test ran for it.

---

## 5-Whys Root Cause Chain

| Why # | Question | Answer |
|-------|----------|--------|
| 1 | Why did the Finance daily revenue report understate revenue by $4.2M over 3 days? | Because mart.fact_revenue was missing ~2.1M rows for April 6–8 |
| 2 | Why were 2.1M rows missing from mart.fact_revenue? | Because the daily dbt incremental run appended 0 rows each day while reporting SUCCESS |
| 3 | Why did the incremental run append 0 rows? | Because the `is_incremental()` filter `WHERE event_created_at > (SELECT MAX(event_created_at) FROM {{ this }})` referenced a column that no longer existed in source, causing the subquery to return NULL and the WHERE clause to match nothing |
| 4 | Why did the column reference not cause a failure? | Because the column was renamed in source (raw.events) but the old column name (`event_created_at`) still existed in the `mart.fact_revenue` target table, so dbt did not raise a compile error — it only evaluated the subquery against the target |
| 5 | Why was there no mechanism to catch this silent failure? | Because no row-count reconciliation test, no schema drift detection, and no minimum-row-delta alert existed for incremental models handling revenue-critical data |

**Root cause:** Incremental dbt models lacked row-count validation gates, and no schema drift detection existed between source columns and model SQL references.

---

## What Went Well

1. Once the issue was surfaced, root cause was identified in under 25 minutes due to clear source system changelog and dbt model history.
2. The backfill ran without data loss — committed offsets and raw source data were intact.
3. The Finance analyst's cross-system comparison (ERP vs data warehouse) was the correct verification pattern and ultimately detected the issue.
4. The team escalated to SEV-1 quickly and proactively notified compliance before being asked.

---

## What Went Poorly

1. 73 hours of silent data loss with zero automated detection — the worst failure mode in data engineering.
2. The dbt `SUCCESS` status on a zero-row run was semantically misleading and should have been a WARNING at minimum.
3. The source team schema migration process had no downstream impact assessment step.
4. Revenue-critical models had no dbt snapshot, making forensic investigation harder than necessary.
5. The CI pipeline did not catch column renames in source tables referenced by unchanged model SQL.

---

## Corrective Actions (CAPA)

| # | Action | Owner | Due Date | Status | Prevention Type |
|---|--------|-------|----------|--------|-----------------|
| 1 | Add dbt row-count reconciliation test for all revenue-critical incremental models: fail if daily delta < 10% of 7-day rolling average (unless explicitly whitelisted) | @chen.wei | 2025-04-18 | Open | Detection |
| 2 | Add Great Expectations checkpoint on `mart.fact_revenue` daily: `expect_table_row_count_to_be_between(min_value=500000, max_value=2000000)` with Slack alert action | @chen.wei | 2025-04-18 | Open | Detection |
| 3 | Create dbt snapshot (`snp_fact_revenue`) to enable point-in-time comparison for all future incidents involving this table | @fatima.al-rashid | 2025-04-25 | Open | Response |
| 4 | Implement schema drift detection in the ingestion layer: compare source column fingerprint (name + type hash) to stored baseline at each run; alert on mismatch before dbt runs | @data-platform | 2025-05-09 | Open | Prevention |
| 5 | Update the source platform team's schema migration runbook to require a "downstream consumer registry" check step and 48h advance notification for any column rename or removal | @data-arch | 2025-05-02 | Open | Process |
| 6 | Add dbt CI step: `dbt source freshness` + `dbt build --select state:modified+` with column-level lineage impact check to detect when unchanged models reference changed source columns | @chen.wei | 2025-05-09 | Open | Prevention |

---

## Lessons Learned

1. A dbt run that returns SUCCESS with zero rows appended is often more dangerous than a run that fails — silent data loss can persist undetected for days. Every incremental model on a revenue-critical table must have a minimum-row-delta guard.
2. Schema rename migrations in source systems are silent killers for downstream incremental models. Column renames must trigger a downstream impact scan before being deployed to production.
3. dbt snapshots are not optional for tables that feed financial reporting — they are the forensic audit trail that enables rapid incident investigation and stakeholder communication.
```

---

### Example 3: ClickHouse Schema Evolution Breaking Query (NULL Values During Mutation)

```markdown
# Postmortem: ClickHouse Column Add — NULL Reads on Grafana Revenue Dashboard (45 min)

| Field           | Value |
|-----------------|-------|
| **Incident ID** | INC-2025-081 |
| **Severity**    | SEV-2 |
| **Status**      | Final |
| **Date**        | 2025-05-07 |
| **Duration**    | 45m (14:02 UTC – 14:47 UTC) |
| **Authors**     | @maya.berg, @igor.petrov |
| **Stakeholders**| @ops-team, @product-analytics, @bi-team |
| **Reviewers**   | @independent-reviewer (not involved) |

---

## Executive Summary

On May 7, 2025, a zero-downtime column addition to a ClickHouse ReplicatedMergeTree table
(`analytics.session_events`) triggered an async background mutation across 3 replicas. During
the 45 minutes the mutation was in progress, queries reading the new column (`session_quality_score`)
returned NULL for rows on parts not yet mutated. This caused the "Session Quality" Grafana panel
to show NULL/0 values for all live traffic metrics, triggering a false alarm in the product team
that the feature was broken. No data was lost; the mutation completed and the dashboard recovered
automatically. Four CAPA actions are opened to prevent dashboard disruption during future ClickHouse
schema changes.

---

## Impact

| Metric | Value |
|--------|-------|
| Total duration | 45 minutes |
| Rows affected | ~22M rows in analytics.session_events showing NULL for `session_quality_score` during mutation |
| Tables affected | 1 table: analytics.session_events |
| Downstream systems | 3 Grafana panels (Session Quality dashboard), 1 real-time alerting rule on session quality score |
| Business impact | False positive product incident alert; product team paused A/B test rollout for 30 minutes |
| Financial impact | 30-min A/B test pause (negligible, estimated $0 revenue impact) |
| SLA breach | No SLA breach; incident classified SEV-2 due to dashboard disruption and false incident trigger |

---

## Timeline (UTC)

| Timestamp (UTC)      | Event                                                                                                         | Who               |
|----------------------|---------------------------------------------------------------------------------------------------------------|-------------------|
| 2025-05-07 14:00     | @igor.petrov executes `ALTER TABLE analytics.session_events ADD COLUMN session_quality_score Float32 DEFAULT 0` | @igor.petrov      |
| 2025-05-07 14:00     | DDL returns immediately (ADD COLUMN is non-blocking in ClickHouse); background mutation begins on 3 replicas  | ClickHouse        |
| 2025-05-07 14:02     | Grafana query `SELECT avg(session_quality_score) FROM session_events WHERE ...` starts returning NULL         | system            |
| 2025-05-07 14:04     | Grafana alerting rule fires: "Session Quality Score avg = NULL (expected > 0.5)"                              | Grafana / PD      |
| 2025-05-07 14:06     | @product-pm-kai pages @maya.berg: "session quality feature is broken in prod"                                 | @product-pm-kai   |
| 2025-05-07 14:08     | @maya.berg checks feature flag service — no change deployed                                                   | @maya.berg        |
| 2025-05-07 14:12     | @maya.berg queries ClickHouse replica 1 directly: column exists but returns NULL on ~80% of rows              | @maya.berg        |
| 2025-05-07 14:15     | @maya.berg checks `system.mutations` table; finds in-progress mutation on all 3 replicas                     | @maya.berg        |
| 2025-05-07 14:17     | Root cause identified: ADD COLUMN mutation in progress; communicates to @product-pm-kai                      | @maya.berg        |
| 2025-05-07 14:17     | A/B test rollout paused by product team as precaution                                                         | Product team      |
| 2025-05-07 14:20     | @igor.petrov confirms this is expected ClickHouse behavior; no corrective action needed — wait for mutation   | @igor.petrov      |
| 2025-05-07 14:30     | Grafana alert silenced; product team informed of ETA                                                          | @maya.berg        |
| 2025-05-07 14:45     | `system.mutations` shows `is_done = 1` on all replicas                                                        | system            |
| 2025-05-07 14:47     | Grafana panels showing correct values; false alarm incident closed                                            | @maya.berg        |
| 2025-05-07 14:50     | Product team resumes A/B test rollout                                                                         | Product team      |

**Key SLA metrics:**
- Time to detect (TTD): 2 minutes (automated Grafana alert)
- Time to acknowledge (TTA): 2 minutes (14:04 → 14:06)
- Time to resolve (TTR): 41 minutes (14:06 → 14:47) — resolution was waiting for mutation to complete

---

## Detection

- [x] Automated monitoring alert — Grafana alerting: "Session Quality Score avg = NULL"

**Gap:** The alert was technically correct (NULL is a real anomaly) but fired for a known,
planned maintenance event. The engineer who executed the schema change did not suppress the
alert or communicate to the monitoring system that a mutation was in progress.

---

## Response

- **Alert fired:** Grafana PagerDuty at 14:04 UTC
- **Product team reported:** @product-pm-kai at 14:06 UTC
- **On-call engineer:** @maya.berg engaged at 14:06 UTC
- **Root cause identified:** 14:17 UTC (11 minutes after engagement)
- **Communication channel:** Direct Slack thread between @maya.berg and @product-pm-kai

---

## Root Cause

> The root cause was the absence of a schema change communication and monitoring
> suppression protocol for ClickHouse DDL operations, which was triggered by executing
> an `ALTER TABLE ADD COLUMN` on a production ReplicatedMergeTree table without notifying
> dashboard consumers or suppressing alerts for the mutation window, resulting in Grafana
> returning NULL values for the new column during the 45-minute async mutation and triggering
> a false production incident.

---

## Contributing Factors

1. ClickHouse `ADD COLUMN` operations on ReplicatedMergeTree tables trigger async background mutations; queries on un-mutated parts return NULL for the new column — this behavior is documented but was not widely known on the team.
2. The Grafana alert for `session_quality_score` was configured with no `NULL` handling — `AVG(NULL)` returns NULL rather than a meaningful fallback.
3. No schema change communication or alert suppression process existed for ClickHouse DDL operations.
4. The engineer executing the schema change did not estimate mutation completion time before executing in production.
5. The product team's A/B test pause caused unnecessary 30-minute delay, though it was the correct cautious response given their information.

---

## 5-Whys Root Cause Chain

| Why # | Question | Answer |
|-------|----------|--------|
| 1 | Why did the Session Quality Grafana panel show NULL for 45 minutes? | Because queries on `session_quality_score` returned NULL for rows whose ClickHouse parts had not yet been mutated |
| 2 | Why were parts not yet mutated? | Because `ALTER TABLE ADD COLUMN` on ReplicatedMergeTree triggers a background mutation that processes parts asynchronously; the DDL returns immediately but data is not updated instantly |
| 3 | Why did the Grafana query not handle this NULL period gracefully? | Because the query used `avg(session_quality_score)` with no NULL coalesce, and the alert threshold was `< 0.5` with no NULL exclusion |
| 4 | Why was no NULL handling added to the query? | Because the query was written assuming the column would always be populated; the async mutation behavior was not communicated to dashboard owners |
| 5 | Why was the mutation behavior not communicated? | Because there was no schema change process that required impact assessment, mutation time estimation, or consumer notification before ClickHouse DDL execution |

**Root cause:** No schema change process existed for ClickHouse DDL that would require mutation impact assessment, consumer notification, and alert suppression before execution.

---

## What Went Well

1. The automated Grafana alert detected the anomaly within 2 minutes — detection infrastructure worked correctly.
2. Root cause was identified in 11 minutes from engagement — the `system.mutations` table provided clear, immediate diagnostic evidence.
3. The engineer did not panic and attempt a rollback (which would have been incorrect); instead confirmed the mutation would complete and communicated the ETA.
4. No data was lost — ClickHouse background mutations are non-destructive and eventually consistent.

---

## What Went Poorly

1. A planned, routine schema change caused a 45-minute false incident and disrupted an A/B test rollout.
2. The engineer executing the DDL did not estimate mutation completion time or notify dashboard consumers.
3. No Grafana alert suppression was applied for the maintenance window.
4. The ClickHouse async mutation behavior was not documented in the team's schema change runbook.
5. The Grafana query's NULL handling was fragile — `avg(session_quality_score)` should have used `avg(coalesce(session_quality_score, 0))` or excluded NULL rows.

---

## Corrective Actions (CAPA)

| # | Action | Owner | Due Date | Status | Prevention Type |
|---|--------|-------|----------|--------|-----------------|
| 1 | Update all Grafana queries on ClickHouse columns that may be NULL-during-mutation to use `avg(coalesce(col, 0))` or `avgIf(col, col IS NOT NULL)` | @maya.berg | 2025-05-14 | Open | Prevention |
| 2 | Add a ClickHouse Schema Change Runbook step: estimate mutation duration using `SELECT count() FROM system.parts WHERE table='X' AND database='Y' AND active=1` before executing in production; communicate ETA to channel #clickhouse-changes | @igor.petrov | 2025-05-14 | Open | Process |
| 3 | Create a pre-flight script for ClickHouse `ALTER TABLE` operations: automatically posts mutation start/estimated end to #clickhouse-changes and creates a Grafana maintenance window (silences alerts for the estimated duration) | @data-platform | 2025-05-28 | Open | Process |
| 4 | Add `system.mutations` monitoring: alert if any mutation has `is_done = 0` for >30 minutes (may indicate stuck mutation); separate from business-logic alerts | @igor.petrov | 2025-05-21 | Open | Detection |
```

---

## 5. CAPA Framework

Every postmortem must produce Corrective and Preventive Actions (CAPA). A CAPA item that is vague, unowned, or undated is not a CAPA — it is a wish.

### CAPA Classification

| Type | What It Addresses | Example |
|------|-------------------|---------|
| **Detection** | Monitoring, alerting, dashboards — make future failures visible faster | Add consumer lag alert with 500K threshold; add zero-row alert for revenue models |
| **Prevention** | Code gates, CI checks, data contracts — stop the failure from being introduced | Add dbt schema contract test; require row-count test in CI for incremental models |
| **Response** | Runbooks, on-call rotation, escalation paths — reduce TTR when failure occurs | Add Kafka partition change to consumer lag runbook; define clear escalation path |
| **Process** | Change management, testing requirements, cross-team communication | Require 48h advance notice for shared Kafka topic changes; mandatory staging gate |

### CAPA Quality Checklist

Each CAPA action must satisfy all five criteria:

- **Specific** — describes exactly what will be built/changed, not "improve monitoring"
- **Measurable** — has a success condition ("alert fires within 10 minutes of zero-row run")
- **Assigned** — has a single named owner (@person, not @team)
- **Time-bound** — has a specific due date (not "next sprint" or "soon")
- **Linked to root cause** — addresses the Why 5 system gap, not the Why 1 symptom

### CAPA Prioritization

Implement in this order:
1. Detection improvements — reduce TTD; highest leverage for future incidents
2. Prevention improvements — stop the failure class entirely
3. Response improvements — reduce TTR if the failure recurs
4. Process improvements — often highest effort, highest long-term value

### Tracking CAPA Completion

```markdown
| CAPA ID | Action | Owner | Due Date | Status | Verification |
|---------|--------|-------|----------|--------|--------------|
| INC-047-C1 | Reduce consumer lag alert to 500K | @ali.okonkwo | 2025-03-21 | Done | Alert tested 2025-03-20; fired in test at 8 min |
| INC-047-C2 | Add rebalance event alert | @platform | 2025-03-28 | In Progress | — |
| INC-063-C1 | Add row-count delta test for fact_revenue | @chen.wei | 2025-04-18 | Done | dbt test added; CI passing |
```

Review open CAPAs weekly in team standup until all are closed. An unclosed CAPA 30 days past due is an escalation trigger.

---

## 6. Impact Quantification Patterns

### Row Count Impact

```sql
-- Count rows missing from target vs what source produced for the affected window
-- Run against the source (raw/staging) and target tables

-- Source row count for affected window
SELECT COUNT(*) AS source_rows
FROM raw.order_events
WHERE event_occurred_at >= '2025-04-06 00:00:00 UTC'
  AND event_occurred_at <  '2025-04-09 00:00:00 UTC';

-- Target row count for same window
SELECT COUNT(*) AS target_rows
FROM mart.fact_revenue
WHERE event_date >= '2025-04-06'
  AND event_date <  '2025-04-09';

-- Delta: missing rows
-- If source_rows = 2,100,000 and target_rows = 0, delta = 2,100,000 missing rows
```

```sql
-- Daily row count trend to identify when the drop started
SELECT
    event_date,
    COUNT(*)                                AS row_count,
    LAG(COUNT(*)) OVER (ORDER BY event_date) AS prev_day_count,
    ROUND(
        100.0 * (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY event_date))
        / NULLIF(LAG(COUNT(*)) OVER (ORDER BY event_date), 0),
        1
    ) AS pct_change_vs_prev
FROM mart.fact_revenue
WHERE event_date >= CURRENT_DATE - INTERVAL '14 days'
GROUP BY event_date
ORDER BY event_date DESC;
```

### Downstream Impact — Lineage Graph Traversal

```bash
# dbt: list all models downstream of the broken table
dbt ls --select "fact_revenue+" --output name

# Output:
# mart.revenue_daily_summary
# gold.executive_kpis
# rpt.finance_daily
# rpt.churn_model_features
# ...
```

```python
# Count downstream tables and dashboards from OpenLineage / Marquez
import requests

MARQUEZ_URL = "http://marquez:5000"

def count_downstream_impact(namespace: str, broken_dataset: str, depth: int = 5) -> dict:
    resp = requests.get(
        f"{MARQUEZ_URL}/api/v1/lineage",
        params={"nodeId": f"dataset:{namespace}:{broken_dataset}", "depth": depth},
        timeout=15,
    )
    resp.raise_for_status()
    graph = resp.json().get("graph", [])

    downstream_datasets = [
        n for n in graph
        if n["type"] == "DATASET" and n["id"] != f"dataset:{namespace}:{broken_dataset}"
    ]
    downstream_jobs = [n for n in graph if n["type"] == "JOB"]

    return {
        "broken_dataset": broken_dataset,
        "downstream_tables": len(downstream_datasets),
        "downstream_jobs": len(downstream_jobs),
        "table_names": [n["data"].get("name") for n in downstream_datasets],
    }
```

### Business Impact Translation

```python
# Template: translate technical impact to business impact
def quantify_business_impact(
    rows_affected: int,
    avg_revenue_per_row: float,        # e.g., average order value
    report_delay_hours: float,
    analyst_hours_manual_work: float,
    analyst_hourly_rate: float = 150,  # fully loaded
) -> dict:
    revenue_at_risk      = rows_affected * avg_revenue_per_row
    manual_work_cost     = analyst_hours_manual_work * analyst_hourly_rate
    delay_business_hours = report_delay_hours

    return {
        "revenue_at_risk_usd":       revenue_at_risk,
        "manual_remediation_usd":    manual_work_cost,
        "report_delay_hours":        delay_business_hours,
        "total_estimated_impact_usd": revenue_at_risk + manual_work_cost,
    }

# Example:
impact = quantify_business_impact(
    rows_affected=2_100_000,
    avg_revenue_per_row=2.0,     # $2 average order contribution
    report_delay_hours=73,
    analyst_hours_manual_work=12,
)
# Output: {"revenue_at_risk_usd": 4200000, "manual_remediation_usd": 1800, ...}
```

### SLA Breach Duration and Penalty

```python
from datetime import datetime, timezone

def calculate_sla_breach(
    sla_deadline_utc: str,    # "2025-04-06 08:00:00"
    resolution_utc: str,      # "2025-04-09 13:00:00"
    penalty_per_hour: float = 0.0,  # set if contractual SLA
) -> dict:
    fmt = "%Y-%m-%d %H:%M:%S"
    deadline    = datetime.strptime(sla_deadline_utc, fmt).replace(tzinfo=timezone.utc)
    resolution  = datetime.strptime(resolution_utc, fmt).replace(tzinfo=timezone.utc)

    breach_duration = resolution - deadline
    breach_hours    = breach_duration.total_seconds() / 3600
    penalty         = breach_hours * penalty_per_hour

    return {
        "sla_deadline_utc":     sla_deadline_utc,
        "resolution_utc":       resolution_utc,
        "breach_duration_hours": round(breach_hours, 2),
        "breach_duration_human": str(breach_duration),
        "estimated_penalty_usd": round(penalty, 2),
    }

# Example:
result = calculate_sla_breach(
    sla_deadline_utc="2025-04-06 08:00:00",
    resolution_utc="2025-04-09 13:00:00",
    penalty_per_hour=500.0,
)
# {"breach_duration_hours": 77.0, "estimated_penalty_usd": 38500.0}
```

### Stakeholder Communication Templates

**SEV-1 Initial Notification (send within 15 minutes of confirmation):**

```
Subject: [SEV-1 ACTIVE] Data Incident — [SYSTEM NAME] — [SHORT DESCRIPTION]

Team,

We are currently investigating a SEV-1 data incident affecting [SYSTEM/REPORT NAME].

Impact: [1 sentence — what is broken and who is affected]
Duration: Began approximately [TIME UTC]. Currently ongoing.
Status: Root cause under investigation. Estimated resolution: [TIME or "unknown"].

Do NOT use [AFFECTED REPORTS/DASHBOARDS] for decision-making until this message is updated.

Next update: [TIME UTC] or upon resolution, whichever is first.

Incident channel: #incident-[ID]
Incident lead: @[name]
```

**SEV-2 Initial Notification (send within 1 hour):**

```
Subject: [SEV-2] Data Delay — [SYSTEM NAME] — [SHORT DESCRIPTION]

Team,

We are investigating a data delay affecting [SYSTEM/REPORT].

Impact: [1 sentence]
Duration: Since approximately [TIME UTC].
SLA status: [On track / At risk / Breached by X hours]
Status: [Investigating / Root cause identified / Fix in progress]

Affected: [List affected dashboards/reports]
Workaround: [None available / Manual query available at: ...]

Next update in 1 hour or upon resolution.
Incident lead: @[name]
```

**Resolution Notification (both severities):**

```
Subject: [RESOLVED] [SEV-X] Data Incident — [SYSTEM NAME] — [SHORT DESCRIPTION]

Team,

The [SEV-X] data incident affecting [SYSTEM] has been resolved as of [TIME UTC].

Resolution summary: [1-2 sentences: what was fixed and how]
Data status: [All data current as of X / Backfill complete / Data accurate from DATE]
Action required: [None / Please refresh [DASHBOARD] / Please re-run [REPORT]]

Total duration: [HH:MM]
SLA breach: [None / X hours Y minutes]

A full postmortem will be shared by [DATE].
Thank you for your patience.
```

---

## 7. Postmortem Meeting Facilitation Guide

### Meeting Format (60 minutes maximum)

| Segment | Duration | Description |
|---------|----------|-------------|
| Opening + blameless framing | 5 min | Facilitator states purpose; reiterates blameless principle |
| Timeline walk-through | 20 min | Chronological review; everyone contributes corrections |
| Root cause discussion | 15 min | 5-Whys chain; stop when systemic root cause is reached |
| What went well / poorly | 10 min | Equal time on both; prevents blame spiral |
| CAPA action capture | 8 min | Assign owner, due date, and type for each action |
| Wrap-up + next steps | 2 min | Confirm postmortem owner; state publish deadline |

**Attendance:** Keep to those directly involved + 1-2 observers. >8 people makes action item ownership diffuse.

### Facilitator vs Note-Taker Roles

**Facilitator responsibilities:**
- Drive agenda timing; cut off rabbit holes
- Redirect blame statements to system questions (see below)
- Ensure all timeline gaps are filled before moving to root cause
- Capture every CAPA as: action, owner, due date, type

**Note-taker responsibilities:**
- Record the timeline in real time (UTC timestamps required)
- Document 5-Whys chain answers verbatim
- Capture "what went well" and "what went poorly" items
- Track action items with owner and due date
- Post draft document within 24h

### Redirecting Blame Statements

| Blame statement heard in the room | Facilitator redirect |
|---|---|
| "Why did @engineer deploy on a Friday?" | "What does our change freeze process say about high-risk windows, and was it clear?" |
| "Who approved this without testing?" | "What testing was required by the process, and what would have caught this?" |
| "The on-call didn't respond fast enough" | "What was the escalation path, and was it clear and functional?" |
| "Someone forgot to update the config" | "What would have prevented this config from being stale in the first place?" |
| "This was obviously the wrong approach" | "What information did the engineer have at the time, and what would have led to a different decision?" |

### Timeline Walk-Through Technique

1. Start from the first observable symptom (not the alert), work backward to find the root cause event.
2. Read each timeline entry aloud; ask "does anyone remember something that happened between this event and the next?"
3. Fill gaps with explicit "unknown" entries rather than leaving them blank.
4. Mark the "detection gap" explicitly: time between first impact and detection.
5. Do not allow discussion of causes during timeline reconstruction — just facts and times.

### Action Item Capture Protocol

At the end of every action item discussion, read it back aloud:
> "Action: [EXACT DESCRIPTION]. Owner: @[name]. Due: [DATE]. Type: [Detection/Prevention/Response/Process]. Does @[name] accept this?"

Do not leave the room with any action item that does not have a named owner and date.

---

## 8. Postmortem Review Checklist

Use this checklist before changing status from "In Review" to "Final".

### Content Quality
- [ ] Timeline covers detection-to-resolution with UTC timestamps for every key event
- [ ] Root cause statement is a single sentence describing a system or process issue, not "human error"
- [ ] 5-Whys chain reaches a systemic root cause (not stopped at first symptom)
- [ ] All contributing factors listed (minimum 3)
- [ ] Impact is quantified: rows affected, tables affected, duration, and business impact (at minimum, a $ estimate or person-hours)
- [ ] "What went well" section contains at least 2 genuine positive observations (not empty or perfunctory)
- [ ] "What went poorly" section contains specific, factual observations (not vague)

### CAPA Quality
- [ ] All CAPA actions have a single named owner (@person, not @team)
- [ ] All CAPA actions have a specific due date
- [ ] All CAPA actions are classified by type (Detection/Prevention/Response/Process)
- [ ] At least one CAPA action addresses the systemic root cause (Why 5), not just the immediate symptom
- [ ] CAPA actions are entered in the team's incident tracking system (Jira/Linear/GitHub Issues)

### Process
- [ ] Reviewed by at least one engineer who was NOT involved in the incident
- [ ] Reviewed by the team lead or on-call manager
- [ ] Stakeholders (affected teams, manager) have been notified of the postmortem's availability
- [ ] Document is stored in the shared postmortem repository with correct naming convention (`YYYYMMDD-<slug>.md`)
- [ ] Index file updated with this incident's entry
- [ ] All SEV-1 postmortems reviewed by engineering leadership before "Final" status

---

## 9. Postmortem Repository and Tracking

### Recommended Storage: Git + Markdown

Store postmortems in a version-controlled repository for searchability, diff history, and link stability.

```
postmortems/
├── README.md                         # Index of all postmortems
├── 20250314-kafka-rebalance-silver.md
├── 20250409-dbt-incremental-row-loss.md
├── 20250507-clickhouse-column-add-nulls.md
└── template.md                       # Blank template
```

### Naming Convention

```
YYYYMMDD-<2-5-word-kebab-slug>.md

Examples:
  20250314-kafka-rebalance-silver.md
  20250409-dbt-fact-revenue-row-loss.md
  20250507-clickhouse-column-mutation-nulls.md
```

### Index File Schema

Maintain a `README.md` index with these columns:

```markdown
| Date | ID | Severity | System | Duration | Root Cause Summary | CAPA Status |
|------|----|----------|--------|----------|--------------------|-------------|
| 2025-05-07 | INC-2025-081 | SEV-2 | ClickHouse | 45m | No DDL change communication protocol | 3/4 open |
| 2025-04-09 | INC-2025-063 | SEV-1 | dbt / mart.fact_revenue | 73h | No incremental row-count validation | 5/6 open |
| 2025-03-14 | INC-2025-047 | SEV-2 | Kafka / Silver layer | 4h 12m | No cross-team Kafka topic change process | 4/6 open |
```

### Quarterly Postmortem Review

Hold a 45-minute quarterly review with the full data engineering team. Agenda:

1. **System frequency analysis** (10 min): Which systems appear in the most postmortems? Rank by incident count and total downtime.

```python
import os, re
from collections import Counter
from pathlib import Path

# Parse index README for system column, count frequency
postmortem_dir = Path("postmortems")
systems = []
for md_file in postmortem_dir.glob("*.md"):
    content = md_file.read_text()
    # Extract system from frontmatter or first table row
    match = re.search(r"\| (SEV-\d) \| ([^|]+) \|", content)
    if match:
        systems.append(match.group(2).strip())

print("Incidents by system:")
for system, count in Counter(systems).most_common():
    print(f"  {system}: {count}")
```

2. **CAPA completion rate** (10 min): What percentage of CAPA actions from the last quarter are closed? A rate below 70% indicates systemic follow-through failure.

3. **Recurring root causes** (15 min): Are any root cause categories appearing multiple times? (e.g., "no row-count validation" appearing in 3 postmortems = structural investment needed)

4. **Lessons learned synthesis** (10 min): Which lessons from individual postmortems should become team-wide practices or architecture standards?

---

## 10. Anti-Patterns

| Anti-Pattern | Problem | Correct Approach |
|---|---|---|
| Root cause = "human error" | Stops analysis at the symptom; no systemic fix; same error recurs | Always ask "what system condition made this error easy to make and hard to detect?" |
| CAPA = "be more careful" | Unmeasurable, unenforceable, and not a system change | Every CAPA must be a specific artifact: an alert, a test, a runbook step, or a process document |
| Postmortem written 2 weeks later | Key details forgotten; timeline inaccurate; ownership unclear | Draft within 24-48h of resolution while memory is fresh |
| "What went well" section left blank or filled with sarcasm | Signals a blame culture; inhibits honest participation | Require 2 genuine positives; facilitator must model authentic praise |
| Timeline with gaps and "approximately" | Ambiguous evidence prevents accurate root cause analysis | Use UTC timestamps; mark unknowns as "UNKNOWN — to investigate" |
| CAPA owner = @team (not a person) | No individual accountability; items never close | Every CAPA has one named person (@individual); teams don't own tasks, people do |
| Postmortem not reviewed by someone outside the incident | Author bias; may miss contributing factors; validates blamelessness | Require at least one external reviewer before "Final" status |
| Impact section: "data was unavailable" (no quantification) | Business impact not communicated; perceived as low-severity | Always quantify: rows, tables, duration, $, person-hours |
| Re-opening old postmortems to add new CAPA items | Dilutes tracking; obscures original root cause | Create a new postmortem for the recurrence; reference the original |
| SEV-3 incidents never reviewed | Near-misses accumulate into SEV-1s; contributing factors go unfixed | Review SEV-3 in team weekly if recurring; track in incident log even without full postmortem |
| Postmortem used in performance reviews | Engineers self-censor and under-report; destroys blameless culture | Postmortems are learning artifacts, not performance evidence; enforce this policy explicitly |

---

## References to Consult When Needed

- `skills/de_rca/SKILL.md` — Pipeline root cause analysis: Airflow/Spark/dbt failure diagnosis, lineage tracing, log analysis patterns, 5-Whys templates
- `skills/airflow_dags/SKILL.md` — Airflow operator and sensor reference for understanding pipeline failure modes
- `skills/dbt_core/SKILL.md` — dbt materializations, incremental strategies, `is_incremental()` patterns
- `skills/great_expectations/SKILL.md` — Expectation suites, Checkpoint actions, row-count and freshness checks
- `skills/soda_core/SKILL.md` — SodaCL data quality checks, Airflow gate integration
- `skills/openlineage/SKILL.md` — Lineage graph traversal, downstream impact analysis via Marquez API
- `skills/datahub_catalog/SKILL.md` — DataHub GraphQL lineage API for blast radius mapping
- `skills/apache_kafka/SKILL.md` — Consumer group management, lag monitoring, partition configuration
- `skills/clickhouse_olap/SKILL.md` — ReplicatedMergeTree mutations, system.mutations table, schema evolution
- `skills/delta_lake/SKILL.md` — Time Travel, RESTORE, Change Data Feed for forensic data recovery
- [Google SRE Book — Postmortem Culture](https://sre.google/sre-book/postmortem-culture/)
- [Google SRE Book — Example Postmortem](https://sre.google/sre-book/example-postmortem/)
- [PagerDuty Incident Response Guide](https://response.pagerduty.com/)
- [Atlassian Incident Management Handbook](https://www.atlassian.com/incident-management/handbook)
