---
name: dataops-postmortem-generator
description: DataOps blameless postmortem generation — severity matrix (SEV1-4), postmortem Markdown template with timeline/impact/root cause/action items, 3 filled-in examples (Kafka rebalance/dbt incremental data loss/ClickHouse schema mutation), CAPA framework (Corrective/Action/Preventive/Action), impact quantification SQL, stakeholder communication templates, 60-min facilitation guide, postmortem review checklist, Git-based postmortem repository management
---

# DataOps Postmortem Generator

## When to Use

- After any SEV1 or SEV2 data platform incident
- Conducting a blameless postmortem review meeting
- Documenting lessons learned to prevent recurrence
- Creating a searchable postmortem library for the team
- Communicating incident impact to stakeholders

---

## Severity Matrix

| Severity | Criteria | Response | Postmortem |
|----------|----------|----------|------------|
| **SEV1** | Production data unavailable > 1h, revenue-impacting | Page immediately, all-hands | Required < 48h |
| **SEV2** | SLA breach > 2h or data correctness issue affecting users | Page on-call, engage team | Required < 1 week |
| **SEV3** | Pipeline delayed < 2h, no user impact | Slack alert, fix in business hours | Recommended |
| **SEV4** | Minor degradation, auto-recovered | Ticket, fix in next sprint | Optional |

---

## Postmortem Markdown Template

```markdown
# Postmortem: [Title — what failed]

**Date**: YYYY-MM-DD
**Severity**: SEV[1-4]
**Duration**: X hours Y minutes
**Author**: [Name]
**Reviewers**: [Names]
**Status**: Draft / In Review / Approved

---

## Summary

One paragraph: what happened, customer impact, and resolution.

## Impact

| Metric | Value |
|--------|-------|
| Duration | X hours |
| Affected datasets | `gold.fact_orders`, `gold.dim_customers` |
| Affected consumers | Analytics team, BI dashboards, Finance reports |
| SLA breach | Yes — data was 4h late (SLA: 2h) |
| Revenue impact | ~$X (if quantifiable) |
| Data rows affected | X rows incorrect / missing |

## Timeline (UTC)

| Time | Event |
|------|-------|
| 02:00 | Scheduled pipeline starts |
| 02:47 | First task failure observed in Airflow |
| 03:15 | On-call engineer paged (SLA at risk) |
| 03:22 | RCA started: identified Kafka offset reset |
| 04:10 | Pipeline re-run initiated |
| 06:30 | Data available (4h SLA breach) |
| 07:00 | All consumers notified of resolution |

## Root Cause

[Describe the root cause in plain language. Use 5-Why if helpful.]

**Why the pipeline failed**: ...
**Why that happened**: ...
**Why that wasn't prevented**: ...

## Contributing Factors

- [Factor 1: e.g., No alert on Kafka consumer group reset]
- [Factor 2: e.g., Default retry delay of 5 min was too slow]

## What Went Well

- [e.g., On-call responded within 7 minutes of page]
- [e.g., Rollback took < 5 minutes due to INSERT OVERWRITE design]

## What Went Poorly

- [e.g., No monitoring on consumer group offset resets]
- [e.g., Postmortem process not clear to new team members]

## Action Items (CAPA)

| ID | Type | Action | Owner | Due Date | Status |
|----|------|--------|-------|----------|--------|
| P-001 | Prevent | Add Prometheus alert: consumer group offset reset | @alice | 2024-01-22 | Open |
| P-002 | Detect | Add freshness SLA check for gold.fact_orders | @bob | 2024-01-19 | Done |
| P-003 | Respond | Add runbook for Kafka rebalance incident | @charlie | 2024-01-26 | Open |
| P-004 | Process | Run quarterly incident response drill | @team | 2024-04-01 | Open |

## Lessons Learned

- [Key insight that generalizes beyond this specific incident]
- [Process improvement recommendation]
```

---

## Example 1: Kafka Rebalance SLA Breach

```markdown
# Postmortem: Kafka Rebalance Caused 4h Orders SLA Breach

**Date**: 2024-01-15 | **Severity**: SEV2 | **Duration**: 4h 30min

## Summary
A Kafka consumer group rebalance caused the orders processing pipeline to lag by 90,000 messages. Combined with slow rebalance recovery (due to a session.timeout.ms misconfiguration), data was unavailable to BI consumers for 4.5 hours, breaching the 2h SLA.

## Root Cause
A new consumer pod was deployed with `session.timeout.ms=6000` (6s) instead of the standard `60000` (60s). Under high message load, the pod repeatedly timed out and triggered rebalances, oscillating between joining and leaving the consumer group every 10-15 seconds.

## Action Items
- P-001: Add max.poll.interval.ms and session.timeout.ms to consumer config linting
- P-002: Alert on consumer group rebalances > 3 in 5 minutes
- P-003: Document consumer group configuration standards in engineering wiki
```

---

## Impact Quantification SQL

```sql
-- Quantify data rows affected during incident window
WITH incident_window AS (
    SELECT
        TIMESTAMP '2024-01-15 02:47:00' AS incident_start,
        TIMESTAMP '2024-01-15 06:30:00' AS incident_end
)
SELECT
    COUNT(*) AS rows_affected,
    COUNT(DISTINCT customer_id) AS customers_affected,
    SUM(order_amount) AS revenue_affected,
    MIN(order_date) AS earliest_affected_date,
    MAX(order_date) AS latest_affected_date
FROM gold.fact_orders
WHERE created_at BETWEEN (SELECT incident_start FROM incident_window)
                     AND (SELECT incident_end FROM incident_window);

-- SLA breach duration
SELECT
    sla_name,
    breach_start,
    breach_end,
    TIMESTAMPDIFF(MINUTE, breach_start, breach_end) AS breach_duration_min,
    CASE
        WHEN TIMESTAMPDIFF(MINUTE, breach_start, breach_end) > 240 THEN 'SEV1'
        WHEN TIMESTAMPDIFF(MINUTE, breach_start, breach_end) > 120 THEN 'SEV2'
        ELSE 'SEV3'
    END AS severity
FROM sla_breach_log
WHERE breach_start::DATE = '2024-01-15';
```

---

## Stakeholder Communication Templates

### SEV1 Initial Notification (T+15min)

```
Subject: [INCIDENT SEV1] Data Platform: Orders data unavailable

We are investigating an incident affecting the orders data pipeline.

Status: INVESTIGATING
Started: 2024-01-15 03:15 UTC
Impact: Orders data in gold.fact_orders not refreshing
Affected teams: Analytics, BI, Finance

Our on-call engineers are actively working on this.
Next update: 2024-01-15 04:00 UTC

—Data Engineering On-Call
```

### SEV1 Resolution

```
Subject: [RESOLVED SEV1] Data Platform: Orders data now available

The orders data incident has been resolved.

Resolution time: 2024-01-15 06:30 UTC
Total duration: 3h 15min
SLA breach: 1h 15min (SLA: 2h)

Orders data is fully refreshed as of 06:30 UTC.
All reports and dashboards should now show current data.

A postmortem will be shared by 2024-01-17.

—Data Engineering
```

---

## 60-Minute Facilitation Guide

```
00:00 - 05:00  Set the tone: this is blameless. The system failed, not people.
05:00 - 15:00  Timeline walkthrough: each person confirms their portion
15:00 - 25:00  5-Why root cause analysis (facilitator drives)
25:00 - 35:00  What went well + what went poorly (everyone contributes)
35:00 - 50:00  Action items: SMART, assigned, with due dates (not "TBD")
50:00 - 55:00  Lessons learned: what generalizes to other systems?
55:00 - 60:00  Close: who writes the postmortem? Deadline?

Blame-redirect phrases:
  "What was the team's mental model at that time?" (not "why did you do that")
  "What would have helped you catch this earlier?" (not "why didn't you catch it")
  "What information was missing from the system?" (not "why didn't you check")
```

---

## Postmortem Repository Layout

```
postmortems/
  README.md              # index of all postmortems
  template.md            # blank template
  2024/
    01/
      PM-001-orders-kafka-rebalance.md
      PM-002-dbt-incremental-data-loss.md
    02/
      PM-003-clickhouse-schema-mutation.md
  action-items.md        # aggregated open action items across all postmortems
```

```bash
# Generate action item summary from all postmortems
grep -r "| Open |" postmortems/2024/ | \
  awk -F'|' '{printf "%-10s %-50s %-15s\n", $3, $4, $7}' | \
  sort -k3
```

---

## Postmortem Review Checklist

```
[ ] Timeline is complete (every event has exact time in UTC)
[ ] Root cause identified (not just symptoms)
[ ] 5-Why analysis completed
[ ] All action items have: owner, due date, SMART definition
[ ] Impact quantified (rows affected, SLA breach minutes, consumers impacted)
[ ] Stakeholder communication templates attached
[ ] Lessons learned generalize beyond this specific incident
[ ] Postmortem reviewed by at least one person not involved in incident
[ ] Filed in postmortems/ repository within 1 week
[ ] Action items added to sprint board and tracked
[ ] Follow-up review scheduled in 30 days to check action item progress
```

---

## Anti-Patterns

1. **Blame in the postmortem** — "the engineer made a mistake" is not a root cause; the system allowed the mistake to happen — fix the system.
2. **Action items without owners** — "the team should add monitoring" never gets done; every action item needs one named owner.
3. **Postmortem filed but never reviewed** — writing is only half the value; schedule a review meeting and publish to the team.
4. **Action items not tracked to completion** — 60% of action items are never completed without tracking; add to sprint board with due dates.
5. **Skipping postmortem for SEV3** — SEV3 incidents contain valuable learning; recommend postmortems for any recurring SEV3 pattern.

---

## References

- Google SRE postmortem culture: `sre.google/sre-book/postmortem-culture/`
- Blameless postmortem: `etsy.com/codeascraft/blameless-postmortems/`
- Related skills: `[[de-postmortem]]`, `[[dataops-root-cause-analysis]]`, `[[dataops-sla-monitoring]]`
