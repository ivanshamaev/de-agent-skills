---
name: de-architecture-decision
description: DE architecture decision records (ADR) — ADR template, trade-off analysis framework, technology evaluation criteria, common DE architecture decisions (storage format, orchestrator, streaming vs batch, lakehouse vs warehouse), decision reversibility, stakeholder communication
---

# DE Architecture Decision Records

## When to Use

Load this skill when the user needs to:
- Choose between two or more data engineering technologies or approaches ("which tool should we use for X?")
- Design the high-level architecture for a new data platform component ("how should we architect Y?")
- Evaluate and score competing options ("we need to decide between A and B")
- Write up or document a past decision for the team record ("document why we chose Z")
- Understand the trade-offs of a specific DE architectural pattern
- Communicate a technical decision to non-technical stakeholders or leadership
- Manage the lifecycle of existing ADRs (supersede, deprecate, link to code)

---

## 1. ADR Format

Every Architecture Decision Record follows this template. Fill every section — leaving a section blank signals the analysis is incomplete.

```markdown
# ADR-NNN: <Short Imperative Title>

**Date:** YYYY-MM-DD
**Status:** Proposed | Accepted | Deprecated | Superseded by ADR-NNN
**Deciders:** <names / roles>
**Tags:** storage | orchestration | streaming | transformation | quality | catalog | secrets | infrastructure

---

## Context

Describe the situation that forces a decision. Include:
- Business or technical requirement driving the change
- Non-negotiable constraints (budget ceiling, compliance, existing contracts)
- Current state and its pain points
- Timeline pressures
- Team size and skill profile

## Decision

State the decision clearly in one or two sentences.
> We will use <technology/approach> for <purpose>.

## Rationale

Explain *why* this option was selected over the alternatives.
- How it satisfies the constraints from Context
- Which evaluation criteria tipped the balance
- Any proof-of-concept or benchmark results that informed the choice

## Alternatives Considered

### Option A: <Name>
**Pros:**
- …

**Cons:**
- …

**Why not chosen:** …

### Option B: <Name>
**Pros:**
- …

**Cons:**
- …

**Why not chosen:** …

### Option C: <Name> (if applicable)
…

## Consequences

**Positive:**
- …

**Negative:**
- …

**Neutral / Watch:**
- …

## Follow-up Actions

- [ ] Owner, due date: action item
- [ ] Owner, due date: action item

---
*Links: [ticket] [design doc] [benchmark results] [related ADR]*
```

---

## 2. Trade-off Evaluation Framework

### Weighted Scoring Matrix

Assign weights (must sum to 100) based on project context, then score each option 1–5 per criterion. Multiply score × weight and sum the row.

**Context-based weight guidance:**

| Context | Emphasize | De-emphasize |
|---|---|---|
| Early-stage startup | Cost, team fit, ecosystem | Vendor lock-in, operational complexity |
| Enterprise / regulated | Vendor risk, operational complexity, ecosystem | Cost |
| Real-time / sub-second SLA | Performance (latency), operational complexity | Cost |
| Batch analytics | Performance (throughput), cost | Latency |
| Small team (< 5 DE) | Team fit, operational complexity | Ecosystem maturity |
| Large team (> 20 DE) | Ecosystem maturity, vendor risk | Team fit learning curve |

**Example matrix — Orchestrator selection (mid-size batch-focused team):**

| Criterion | Weight | Airflow 2.x | Prefect 3 | Dagster |
|---|---|---|---|---|
| Performance (throughput, scalability ceiling) | 15 | 4 → 60 | 4 → 60 | 3 → 45 |
| Operational complexity (deploy, upgrade, observe) | 20 | 3 → 60 | 4 → 80 | 4 → 80 |
| Cost (licensing, compute, engineering effort) | 15 | 5 → 75 | 3 → 45 | 3 → 45 |
| Ecosystem (connectors, community, docs, cadence) | 20 | 5 → 100 | 3 → 60 | 3 → 60 |
| Team fit (skills, learning curve, hiring market) | 20 | 5 → 100 | 3 → 60 | 3 → 60 |
| Vendor risk (OSS health, commercial dependency, exit path) | 10 | 5 → 50 | 3 → 30 | 3 → 30 |
| **Total** | **100** | **445** | **335** | **320** |

Scores 1–5: 1 = very poor, 3 = adequate, 5 = excellent.

### Scoring Rubric for Each Criterion

**Performance**
- 5: Proven at 10× your expected scale; documented benchmarks exist
- 4: Handles your load with headroom; team has validated this
- 3: Handles your load at launch; may need tuning or sharding later
- 2: Requires significant workaround or custom code to meet the requirement
- 1: Cannot meet the requirement without architectural changes

**Operational Complexity**
- 5: Managed service or trivial Helm chart; self-healing; mature runbooks
- 4: Well-documented deployment; < 1 day to upgrade; good observability built in
- 3: Multi-service deployment; upgrade requires planned downtime; some observability
- 2: Complex multi-service with known operational pain; limited built-in monitoring
- 1: No managed path; upgrades routinely cause incidents; black-box internals

**Cost**
- 5: Open-source with minimal infra overhead; negligible licensing
- 4: Open-source with moderate infra cost; or affordable SaaS tier
- 3: Moderate licensing or infra cost within budget
- 2: Significant cost; requires budget approval or trade-off conversation
- 1: Cost-prohibitive at your scale or violates budget constraints

**Ecosystem**
- 5: De-facto standard; hundreds of connectors; active community; weekly releases
- 4: Mature ecosystem; major connectors available; active community; monthly releases
- 3: Growing ecosystem; key connectors available; responsive maintainers
- 2: Limited connectors; small community; slow release cadence
- 1: Niche tool; single maintainer; connector gaps require custom code

**Team Fit**
- 5: Team already uses this daily; same language/paradigm; easy hiring
- 4: Team familiar; < 1 sprint to become productive; candidates widely available
- 3: Some team members know it; 1–2 months to full productivity
- 2: New to the team; 3–6 months to productive; limited hiring pool
- 1: Entirely foreign technology; steep learning curve; very thin hiring market

**Vendor Risk**
- 5: Pure Apache/Linux Foundation OSS; permissive license; multiple large sponsors
- 4: OSS with strong commercial backing; clear license; easy fork path
- 3: OSS but controlled by a single company; BSL or similar license
- 2: Commercial SaaS with data portability but switching cost is high
- 1: Proprietary closed-source; data lock-in; no exit path

### Decision Reversibility Spectrum

Before committing, assess how hard it would be to undo this decision in 12–18 months.

```
Easy to reverse                                    Hard to reverse
      |---------------------------------------------------|
  Config    Schema   Orchestrator   Storage format   Data model
  change    change     swap          migration        refactor

      ^                                                    ^
   Low risk                                          High risk
   Decide quickly                              Invest more in evaluation
```

| Decision type | Reversibility | Recommended process |
|---|---|---|
| Adding a new tool alongside existing | High | Spike + team demo |
| Replacing a config or connector | High | PR review |
| Switching orchestrator | Medium | 3-month parallel run |
| Changing storage format | Low | Full migration plan + rollback ADR |
| Changing data model (fact/dim) | Very low | Stakeholder sign-off + versioning |
| Moving cloud providers | Very low | Architecture committee + exec approval |

---

## 3. Pre-filled ADR Examples

---

### ADR-001: Adopt Lambda Architecture with Micro-Batch for Near-Real-Time Reporting

**Date:** 2025-03-10
**Status:** Accepted
**Deciders:** Lead DE, Analytics Engineering Manager, VP of Data
**Tags:** streaming, batch, architecture

---

**Context**

The business requires order status dashboards to refresh within 10 minutes of an event occurring. The existing data platform is purely batch: nightly Airflow DAGs load the Hive/Parquet warehouse by 06:00 UTC. The analytics team uses Trino for ad-hoc queries. The DE team has no prior Flink or Kafka Streams experience. A 2-month delivery window is imposed. Infrastructure budget is capped at existing compute + $3k/month.

**Decision**

We will implement a Lambda architecture: the existing nightly batch pipeline is kept as the historical accuracy layer, and a new PySpark Structured Streaming job running 5-minute micro-batch windows writes an Iceberg "hot" table that the dashboard queries. Both layers write to the same Iceberg table namespace, with the batch layer overwriting partitions for dates older than today.

**Rationale**

Pure streaming (Flink) was rejected due to the team's skill gap and the 2-month deadline. A 5-minute micro-batch interval satisfies the 10-minute SLA with margin. PySpark Structured Streaming uses the same Spark codebase as existing ETL jobs, minimising ramp-up time. Lambda complexity (dual code paths) is accepted as a transitional state; a future ADR will evaluate moving to Kappa when the team is Flink-proficient.

**Alternatives Considered**

**Option A: Pure Streaming with Apache Flink**
Pros: true event-time processing; lowest latency; single code path (Kappa).
Cons: no team Flink experience; 4–6 month ramp-up; misses the delivery window; additional operational surface.
Why not chosen: timeline and skill gap make this untenable in the current window.

**Option B: Pure Batch — Reduce Airflow Schedule to Every 10 Minutes**
Pros: zero new infrastructure; trivial change.
Cons: 10-minute Airflow schedule creates scheduler contention; full table scans every 10 minutes violate cost constraints; no time-travel or incremental semantics.
Why not chosen: cost and scheduler-contention risk.

**Option C: Kappa Architecture with PySpark Streaming Only**
Pros: single code path; no dual-write complexity.
Cons: reprocessing historical partitions in streaming mode is error-prone; backfill latency unpredictable under load.
Why not chosen: historical accuracy risk; batch overwrite is simpler and already tested.

**Consequences**

Positive: 10-minute dashboard SLA met; reuses existing Spark infrastructure; team upskills on Structured Streaming.
Negative: two code paths to maintain for the same data; reconciliation between batch and streaming layers required nightly.
Neutral / Watch: monitor for drift between batch and streaming row counts; build automated reconciliation task.

**Follow-up Actions**

- [ ] DE Lead, 2025-04-01: Deploy streaming job to staging and validate 5-min window latency
- [ ] DE Lead, 2025-05-01: Build nightly reconciliation Airflow task (batch vs streaming row counts)
- [ ] DE Manager, 2025-09-01: Review Flink readiness; open ADR-011 for Kappa migration if team is ready

---

### ADR-002: Use Apache Iceberg as the Lakehouse Table Format

**Date:** 2025-01-15
**Status:** Accepted
**Deciders:** Principal DE, Platform Architect
**Tags:** storage, lakehouse

---

**Context**

The data lake on S3 uses plain Parquet files managed by the Hive Metastore. Pain points: no ACID updates/deletes (GDPR erasure requests require full partition rewrites), no time travel, schema evolution breaks Trino queries, and small-file accumulation degrades performance. The platform uses both PySpark for ETL and Trino for ad-hoc queries. A unified open format is required that both engines read natively without a vendor-managed service.

**Decision**

We will migrate all production tables to Apache Iceberg (v1.5+) stored on S3, with the REST catalog (backed by a PostgreSQL store). Spark and Trino will both use the Iceberg catalog connector.

**Rationale**

Iceberg's REST catalog is engine-agnostic; both Spark and Trino have mature, maintained Iceberg connectors. Iceberg v2 supports row-level deletes and upserts (MERGE) enabling GDPR compliance without full partition rewrites. Hidden partitioning and partition evolution prevent query breakage on partition changes. The open specification and Apache governance mitigate vendor lock-in risk.

**Alternatives Considered**

**Option A: Delta Lake (Databricks open-source)**
Pros: strong ACID support; excellent Spark integration; large community; PySpark Delta API is ergonomic.
Cons: Trino's Delta connector lags behind the Spark connector in feature parity; Linux Foundation governance is newer; the log-based format makes non-Spark readers harder to implement correctly.
Why not chosen: Trino read path is a first-class requirement; Delta's Trino connector maturity is lower than Iceberg's.

**Option B: Stay on Plain Parquet + Hive Metastore**
Pros: zero migration effort; fully understood by the team.
Cons: GDPR erasure requires full partition rewrites (hours per table); no time travel; schema evolution requires manual coordination; small-file problem worsens over time.
Why not chosen: GDPR and time-travel requirements cannot be met.

**Option C: Apache Hudi**
Pros: mature CDC and upsert semantics; good Spark support; used at large scale at Uber.
Cons: more complex operational model (MOR vs COW trade-offs); Trino connector less mature than Iceberg; smaller community.
Why not chosen: Trino query path is weaker; operational complexity higher than Iceberg for our team.

**Consequences**

Positive: GDPR row-level deletes in minutes rather than hours; time travel for debugging; schema evolution without breaking Trino; hidden partitioning simplifies DDL.
Negative: migration of 500 existing tables is a 3–4 month project; team must learn Iceberg-specific maintenance (OPTIMIZE, expire_snapshots).
Neutral / Watch: monitor REST catalog PostgreSQL for performance under high metadata load; snapshot expiry must be scheduled or S3 costs grow.

**Follow-up Actions**

- [ ] DE Lead, 2025-02-15: Migrate top-10 highest-value tables as pilot; validate Trino + Spark read/write
- [ ] Platform Eng, 2025-03-01: Set up REST catalog HA (two instances + PG connection pooling)
- [ ] DE Team, 2025-06-01: Complete full migration; open ADR-010 for Hive → Iceberg migration runbook

---

### ADR-003: Use Apache Airflow 2.x with KubernetesExecutor as the Orchestrator

**Date:** 2024-11-20
**Status:** Accepted
**Deciders:** DE Manager, Platform Architect, SRE Lead
**Tags:** orchestration

---

**Context**

The team of 8 data engineers needs a production orchestrator for 300+ DAGs. Requirements: Python-native task authoring, complex inter-DAG dependencies, Kubernetes-based infrastructure (EKS), per-task resource isolation, strong community support, and a self-hosted deployment (data residency requirement rules out managed SaaS). The team has prior Airflow 1.x experience.

**Decision**

We will deploy Apache Airflow 2.8 using the official Helm chart on EKS with the KubernetesExecutor. Each task runs in an isolated pod with its own resource requests/limits. The Helm chart manages the scheduler, webserver, and triggerer components.

**Rationale**

Airflow's operator and provider ecosystem (300+ providers) is unmatched. Prior team experience reduces ramp-up. KubernetesExecutor provides per-task isolation without a shared worker pool, which eliminates resource contention between heavy Spark-submitting tasks and lightweight sensor tasks. Self-hosted Helm satisfies data residency. The TaskFlow API (Airflow 2.x) makes Python-native DAGs ergonomic.

**Alternatives Considered**

**Option A: Prefect 3**
Pros: modern Python-first API; elegant deployment model; Prefect Cloud managed option; good observability UI.
Cons: provider ecosystem is smaller than Airflow's; team has zero Prefect experience; self-hosted Prefect Server is less battle-tested at 300+ flow scale; no KubernetesExecutor equivalent (uses separate worker deployments).
Why not chosen: ecosystem gap and experience gap.

**Option B: Dagster**
Pros: Software-Defined Assets is a strong abstraction; built-in data lineage; excellent testing story.
Cons: steeper learning curve (asset-first mental model is different from task-first); fewer provider integrations; self-hosted at 300+ asset scale requires careful Dagster Daemon tuning; no equivalent to Airflow pools for external system throttling.
Why not chosen: learning curve and asset model adoption cost for an existing task-oriented team.

**Option C: Argo Workflows**
Pros: native Kubernetes CRDs; excellent K8s integration; strong GitOps story.
Cons: not Python-native; YAML-heavy DAG definition; no data engineering operators; data teams find it harder to use than Airflow.
Why not chosen: poor data engineering operator ecosystem.

**Consequences**

Positive: operator ecosystem covers all required integrations (Spark, Trino, dbt, HTTP, S3); KubernetesExecutor eliminates shared worker bottlenecks; team productive from day one.
Negative: Airflow scheduler is stateful; scheduler upgrades require downtime windows; KubernetesExecutor creates many short-lived pods (K8s API server load).
Neutral / Watch: monitor K8s API server request rate when pod churn is high; plan upgrade windows quarterly.

**Follow-up Actions**

- [ ] Platform Eng, 2024-12-15: Deploy Airflow 2.8 Helm chart to EKS staging; validate DAG import time < 30s
- [ ] SRE, 2025-01-15: Configure Prometheus + Grafana dashboards for scheduler heartbeat and pod-creation latency
- [ ] DE Lead, 2025-02-01: Migrate top-50 DAGs from legacy scheduler; validate all sensors and callbacks

---

### ADR-004: Use dbt Core with Trino Adapter as the Transformation Layer

**Date:** 2025-02-05
**Status:** Accepted
**Deciders:** Analytics Engineering Lead, DE Lead
**Tags:** transformation

---

**Context**

The analytics engineering team of 6 (mostly SQL-fluent, minimal Spark experience) needs to manage 200+ transformation models targeting Trino+Iceberg and occasionally ClickHouse. Requirements: SQL-first authoring, version-controlled models, automated testing, CI/CD integration, and cross-engine support without duplicating logic. The team already uses dbt for a small Postgres project.

**Decision**

We will use dbt Core (1.8+) with the `dbt-trino` adapter as the primary transformation layer. ClickHouse models will use `dbt-clickhouse` in the same project with separate targets. All models, tests, and macros live in a single Git repository with a mono-project structure.

**Rationale**

The team's SQL expertise and prior dbt exposure minimise ramp-up. dbt's incremental materialisation and `is_incremental()` macro map naturally to Iceberg MERGE. The `dbt-trino` adapter is actively maintained and supports Iceberg table properties. dbt's `--select state:modified+` enables slim CI (only test changed models). Cross-adapter support is achieved through `adapter.dispatch` macros without duplicating SQL.

**Alternatives Considered**

**Option A: SQLMesh**
Pros: first-class virtual environments; built-in plan/apply workflow prevents accidental prod mutations; integrated audit and unit test framework; Trino support.
Cons: steeper learning curve; smaller community than dbt; fewer pre-built packages (dbt-utils equivalent is smaller); less hiring market familiarity.
Why not chosen: community and package ecosystem gap; existing team invested in dbt patterns.

**Option B: Direct Spark SQL Scripts Orchestrated by Airflow**
Pros: full Spark power (complex ML features, custom UDFs, large joins); no new framework to learn for the Spark team.
Cons: no lineage graph; no automated testing framework; difficult to enforce standards; analysts cannot author models; CI/CD requires custom tooling for change detection.
Why not chosen: no lineage, testing, or governance story out of the box.

**Option C: Apache Hop / Pentaho**
Pros: GUI-based; non-developer friendly.
Cons: no version control story; SQL-first is not the paradigm; poor Trino support; no modern CI/CD integration.
Why not chosen: incompatible with Git-first, code-review-driven workflow.

**Consequences**

Positive: analysts can author models with PR review; dbt docs auto-generate lineage DAG; `dbt test` catches data quality issues before production merge; slim CI cuts test time by 70%.
Negative: dbt is not a compute engine — heavy transforms (window functions over billions of rows) must still use Spark; managing two runtimes (dbt+Trino and PySpark) adds operational surface.
Neutral / Watch: watch `dbt-trino` adapter release cadence; pin adapter version in requirements.txt and test upgrades in staging.

**Follow-up Actions**

- [ ] Analytics Eng Lead, 2025-03-01: Migrate 50 highest-priority models from SQL scripts to dbt
- [ ] DE Lead, 2025-03-15: Set up slim CI GitHub Actions workflow with `state:modified+` and `--defer`
- [ ] Analytics Eng, 2025-04-01: Establish dbt model style guide (staging/intermediate/mart layers)

---

### ADR-005: Use Trino + Iceberg on S3 as the Primary OLAP Store

**Date:** 2024-10-10
**Status:** Accepted
**Deciders:** VP Data, Principal DE, Head of Analytics
**Tags:** storage, lakehouse, warehouse

---

**Context**

The analytics platform must support ad-hoc queries over a 100-billion-row fact table (events), 50 analysts, and mixed workloads (interactive dashboards < 5s, large exports < 30 min). Current Redshift cluster costs $28k/month and is capacity-constrained. The organisation is cloud-neutral (uses AWS and GCP). Data residency requires the storage layer to be in EU-West-1. The DE platform is already on Kubernetes with S3 object storage.

**Decision**

We will replace Redshift with Trino (version 440+) deployed on EKS, querying Iceberg tables stored on S3. Compute and storage are separated; analysts query via the Trino coordinator. Heavy scheduled reports run on dedicated Trino worker groups via resource groups.

**Rationale**

Trino on K8s scales compute independently of storage; we can add workers for peak load and scale down overnight. S3 storage cost for 100B rows (Parquet/Iceberg with Snappy) is ~$400/month vs Redshift's managed storage premium. Iceberg provides ACID MERGE for GDPR erasure and schema evolution. Trino's ANSI SQL and JDBC driver are drop-in for most Redshift queries. Estimated total cost: $6k/month for compute + $1k storage, saving ~$21k/month.

**Alternatives Considered**

**Option A: ClickHouse (self-hosted or Cloud)**
Pros: fastest columnar engine for aggregation queries; MergeTree family purpose-built for OLAP; excellent compression.
Cons: non-standard SQL dialect (requires query rewrite); ReplacingMergeTree deduplication is eventually consistent (FINAL keyword); less analyst-friendly; no native Iceberg write path; schema changes require ALTERs rather than Iceberg evolution.
Why not chosen: SQL dialect migration cost and lack of Iceberg write path.

**Option B: Stay on Redshift**
Pros: mature, well-known, RA3 managed storage, deep AWS integration.
Cons: $28k/month is unsustainable at 2× growth; storage and compute coupled; cross-cloud read is impossible; Spectrum for S3 queries adds latency.
Why not chosen: cost and cloud neutrality requirements.

**Option C: Google BigQuery**
Pros: serverless; near-zero operational burden; excellent BI tool integration.
Cons: GCP-only; data residency in EU requires BigQuery EU region (limited feature parity with US); per-query cost model unpredictable at ad-hoc analyst scale; not cloud-neutral.
Why not chosen: GCP lock-in violates cloud-neutrality policy.

**Consequences**

Positive: ~75% infrastructure cost reduction; elastic compute; cloud-neutral storage on S3; Iceberg features (time travel, MERGE, schema evolution).
Negative: Trino operational complexity (JVM tuning, OOM debugging, query memory limits); analysts need to learn Trino SQL dialect differences (no lateral column aliases in older versions).
Neutral / Watch: monitor Trino coordinator memory under high concurrency; set resource groups to cap per-user query memory.

**Follow-up Actions**

- [ ] Platform Eng, 2024-11-01: Deploy Trino 440 on EKS staging; run TPC-DS benchmark against Redshift baseline
- [ ] DE Lead, 2024-12-01: Migrate top-10 dashboard queries; validate < 5s p95 on interactive workload
- [ ] Finance, 2025-01-01: Decommission Redshift after 30-day parallel run

---

### ADR-006: Use Debezium with Kafka Connect for CDC from PostgreSQL

**Date:** 2025-04-01
**Status:** Accepted
**Deciders:** DE Lead, Backend Platform Lead
**Tags:** cdc, streaming

---

**Context**

The order management PostgreSQL database (v14, 200GB, ~5k transactions/minute peak) must feed the data warehouse with sub-minute latency for operational reporting. The current approach (timestamp-based polling every 5 minutes) misses deletes and introduces 5-minute lag. Kafka is already deployed as the event bus. The DE team has Kafka Connect operational experience.

**Decision**

We will deploy Debezium PostgreSQL connector (v2.6) via Kafka Connect in distributed mode. The connector reads the PostgreSQL WAL via the `pgoutput` plugin, publishes change events to Kafka topics (`orders.public.orders`, etc.), and a Kafka Connect JDBC Sink or Flink job consumes the topics to write to the Iceberg DWH.

**Rationale**

WAL-based CDC captures inserts, updates, and deletes with sub-second latency, satisfying the sub-minute SLA. Debezium is the de-facto open-source CDC standard with production use at thousands of organisations. The `pgoutput` plugin is built into PostgreSQL 10+ — no third-party plugin installation required. Reusing the existing Kafka Connect cluster minimises new operational surface.

**Alternatives Considered**

**Option A: Timestamp-Based Polling (Enhanced)**
Pros: no new infrastructure; simple SQL queries; easy to debug.
Cons: cannot capture deletes; polling interval creates irreducible lag; high-frequency polling increases DB load; missed updates during down windows require manual reconciliation.
Why not chosen: deletes are a hard requirement; latency SLA cannot be met.

**Option B: pglogical Custom Replication**
Pros: native PostgreSQL logical replication; lower overhead than Debezium for simple cases.
Cons: requires manual consumer code; no out-of-the-box Kafka integration; schema evolution handling is custom; operational burden is higher.
Why not chosen: Kafka integration requires custom connector code that Debezium already provides.

**Option C: AWS DMS (Database Migration Service)**
Pros: managed service; no operational burden for replication itself.
Cons: AWS lock-in violates cloud-neutrality policy; DMS CDC to Kafka requires additional Kinesis hop; limited SMT support; data format less flexible than Debezium's.
Why not chosen: cloud lock-in and limited SMT flexibility.

**Consequences**

Positive: deletes captured; sub-minute latency; replication slot provides durable WAL offset; Debezium schema registry integration provides forward/backward compatible schema evolution.
Negative: replication slot must be monitored — unconsumed WAL causes PostgreSQL disk growth; adding Kafka Connect workers increases infra footprint; snapshot mode must be tested carefully for large tables.
Neutral / Watch: alert on replication slot lag > 500MB; test connector failover behaviour; document snapshot procedure for new tables.

**Follow-up Actions**

- [ ] DE Lead, 2025-05-01: Deploy Debezium connector to staging; validate delete events captured
- [ ] Platform Eng, 2025-05-15: Add Prometheus alert on `pg_replication_slots.confirmed_flush_lsn` lag
- [ ] DE Lead, 2025-06-01: Decommission timestamp-polling job after 2-week parallel validation

---

### ADR-007: Use Soda Core (SQL checks) + dbt Tests (Model-Level) for Data Quality

**Date:** 2025-03-20
**Status:** Accepted
**Deciders:** Analytics Engineering Lead, Data Governance Lead
**Tags:** quality

---

**Context**

The platform has 200 dbt models and 50 Python/PySpark pipelines. Data quality failures are discovered by downstream analysts, not by the pipelines themselves. Requirements: (1) row-level SQL checks on source landing tables before transformation; (2) model-level schema and business-rule tests integrated into dbt; (3) centralised quality dashboard and Slack alerting; (4) no Python SDK required for SQL-only analysts to add checks.

**Decision**

We will use a two-layer DQ architecture: Soda Core (SodaCL YAML checks) runs as Airflow tasks on source/landing tables immediately after ingestion; dbt generic and singular tests run during the dbt build step for model-level validation. Soda's scan results publish to Soda Cloud for the centralised dashboard. Critical Soda failures block the downstream dbt DAG via Airflow sensor.

**Rationale**

SodaCL is a declarative YAML DSL that SQL-proficient analysts can write without Python. Soda Core integrates with Trino, ClickHouse, and PostgreSQL via a single `configuration.yml`. dbt tests are the natural choice for model-level validation since they run in the same execution graph. The two-layer approach gives early failure detection (before expensive transformation) and model-level contract enforcement.

**Alternatives Considered**

**Option A: Great Expectations Only**
Pros: rich Python API; supports Pandas and Spark in addition to SQL; strong Checkpoint+Action framework.
Cons: YAML is complex and verbose; the new GX Core 1.x API has significant learning curve; no native SodaCL-equivalent YAML for SQL analysts; Airflow integration requires more boilerplate; Data Docs hosting is additional operational surface.
Why not chosen: analyst authoring experience is worse than SodaCL.

**Option B: dbt Tests Only**
Pros: single tool; integrated into existing dbt workflow; no new infrastructure.
Cons: dbt tests run post-transformation — source data issues are caught late; limited built-in check types (null, unique, accepted_values, relationships); custom business-rule SQL tests require dbt singular test files.
Why not chosen: source landing table checks require a pre-transformation validation gate.

**Option C: Custom SQL Assertions in Airflow**
Pros: maximum flexibility; no new dependencies.
Cons: no standard check format; no centralised dashboard; each check requires custom Python/SQL; no community library of check templates; maintenance burden scales poorly.
Why not chosen: no governance story; not sustainable at 200+ table scale.

**Consequences**

Positive: SQL analysts add SodaCL checks without Python; source failures blocked before transformation saves compute; Soda Cloud dashboard provides organisation-wide quality visibility.
Negative: two DQ systems to understand; Soda Core requires maintenance of `configuration.yml` per environment; Soda Cloud SaaS cost (~$500/month at current scan volume).
Neutral / Watch: monitor Soda scan duration — large table scans may slow Airflow task completion; consider `sample_percent` for very large tables.

**Follow-up Actions**

- [ ] Data Governance Lead, 2025-04-15: Define Soda check severity standards (warn vs fail thresholds)
- [ ] Analytics Eng, 2025-05-01: Add Soda checks to top-20 source tables; wire Slack notifications
- [ ] DE Lead, 2025-05-15: Build Airflow ExternalTaskSensor to gate dbt DAG on Soda scan pass

---

### ADR-008: Adopt DataHub as the Data Catalog and Lineage Platform

**Date:** 2025-01-08
**Status:** Accepted
**Deciders:** Head of Data, Data Governance Lead, DE Lead
**Tags:** catalog, governance

---

**Context**

A team of 15 data engineers and 30 analysts has no centralised metadata catalog. Data discovery relies on Slack questions and tribal knowledge. GDPR impact analysis (which tables contain PII?) requires manual spreadsheet tracking. Column-level lineage for debugging broken dashboards is unavailable. Requirements: automated ingestion from Airflow, dbt, Spark, Trino, and PostgreSQL; column-level lineage; tagging for PII classification; self-hosted (data residency); active development and community.

**Decision**

We will deploy DataHub (v0.13+) self-hosted on Kubernetes using the official Helm chart. Ingestion recipes will be configured for PostgreSQL, Trino/Iceberg, dbt, and Airflow. Spark lineage will use the DataHub Spark listener agent. Column-level lineage will use the `FineGrainedLineage` facet emitted by dbt and Spark.

**Rationale**

DataHub has the most active development velocity in the open-source catalog space (daily releases, strong LinkedIn engineering support). Its ingestion framework covers all required sources via maintained recipes. Column-level lineage from dbt is supported via the dbt-datahub integration without custom code. The GraphQL API enables programmatic impact analysis (find all downstream datasets of a PII column). Kubernetes Helm deployment fits the existing platform.

**Alternatives Considered**

**Option A: OpenMetadata**
Pros: clean UI; strong governance features; active community; good dbt integration.
Cons: smaller deployment base than DataHub; Airflow integration is less mature; column-level lineage from Spark requires more custom instrumentation; Kubernetes operator is newer and less proven at scale.
Why not chosen: DataHub's Spark lineage and Airflow integration are more mature.

**Option B: Amundsen (Lyft)**
Pros: mature project; good search UX; open source.
Cons: development has slowed (fewer releases); no column-level lineage built in; dbt integration requires community plugins; Neo4j as the graph backend is an additional operational dependency.
Why not chosen: development velocity has declined; no native column-level lineage.

**Option C: Atlan (SaaS)**
Pros: excellent UX; managed service; strong integrations.
Cons: SaaS — data residency requirement not met; per-user cost is significant at 45 users; vendor lock-in.
Why not chosen: data residency violation.

**Consequences**

Positive: analysts discover tables without Slack questions; GDPR impact analysis automated via GraphQL API; column-level lineage enables root-cause analysis in minutes not hours.
Negative: DataHub is operationally complex (Kafka, Elasticsearch, MySQL, and GMS — 4 services minimum); initial ingestion recipe setup requires 2–3 weeks of engineering.
Neutral / Watch: Elasticsearch index size grows with lineage events; plan quarterly index maintenance; monitor GMS heap under high ingestion load.

**Follow-up Actions**

- [ ] Platform Eng, 2025-02-01: Deploy DataHub Helm chart to K8s; validate all 4 core services healthy
- [ ] DE Lead, 2025-03-01: Configure ingestion recipes for PostgreSQL, Trino, dbt, Airflow
- [ ] Data Governance Lead, 2025-03-15: Tag all known PII columns via DataHub API; test GDPR search query

---

### ADR-009: Use HashiCorp Vault with Kubernetes Auth for Secrets Management

**Date:** 2024-12-01
**Status:** Accepted
**Deciders:** Platform Architect, Security Lead, SRE Lead
**Tags:** secrets, infrastructure, security

---

**Context**

The Kubernetes-based data platform (EKS) runs 20+ data workloads (Airflow, Spark, Trino, dbt runners) that each need credentials for databases, APIs, and object storage. Current state: secrets in Kubernetes Secrets (base64-encoded, not encrypted at rest by default), committed to GitOps repo in some cases. Security audit found credentials in plaintext in 3 DAG files. Multi-cloud future (AWS + GCP within 18 months) requires a secrets solution that works across both clouds without re-engineering.

**Decision**

We will deploy HashiCorp Vault (v1.16+) on Kubernetes using the official Vault Helm chart with integrated storage (Raft). Workloads authenticate via the Vault Kubernetes auth method (pod's ServiceAccount JWT). The Vault Agent Injector sidecar renders secrets as files into pod volumes. Vault is the single source of truth for all production credentials.

**Rationale**

Vault's Kubernetes auth method is natively multi-cluster and cloud-agnostic — the same Vault instance can serve both AWS EKS and GCP GKE workloads without per-cloud configuration changes. Dynamic secrets (e.g., Vault PostgreSQL engine issuing short-lived DB credentials) eliminate long-lived credential rotation risk. Raft integrated storage removes the external Consul dependency. The Vault Agent Injector requires no application code changes — secrets appear as files in the pod filesystem.

**Alternatives Considered**

**Option A: AWS SSM Parameter Store + Secrets Manager**
Pros: managed service; no operational burden; deep AWS IAM integration; Airflow has a built-in SSM secrets backend.
Cons: AWS-only — multi-cloud future requires re-engineering; cross-account access is complex; no dynamic secrets for databases; vendor lock-in.
Why not chosen: multi-cloud requirement cannot be met.

**Option B: Native Kubernetes Secrets (Enhanced with External Secrets Operator)**
Pros: native K8s; External Secrets Operator syncs from AWS/GCP secret stores; no new cluster components.
Cons: still cloud-specific backend (ESO just moves the problem); no dynamic secrets; no audit log of secret access per pod; credential rotation requires ESO sync + pod restart.
Why not chosen: no dynamic secrets; audit log per workload is a compliance requirement.

**Option C: Sealed Secrets (Bitnami)**
Pros: GitOps-friendly; simple; Kubernetes-native.
Cons: secrets are static (no dynamic rotation); SealedSecret decryption key loss = all secrets lost; no audit trail; no cross-cluster sharing without key distribution.
Why not chosen: no dynamic secrets; audit requirement not met.

**Consequences**

Positive: short-lived dynamic database credentials eliminate credential theft risk; full audit log of which pod accessed which secret and when; cloud-agnostic for multi-cloud; application code requires no changes (file-based injection).
Negative: Vault is operationally complex — unsealing, HA Raft configuration, and backup procedures require SRE investment; Vault Agent Injector sidecar adds memory to every pod (50–100MB); on-call team must learn Vault emergency procedures.
Neutral / Watch: test Vault seal/unseal automation (auto-unseal via AWS KMS or GCP KMS) before go-live; practice restore from Raft snapshot in staging.

**Follow-up Actions**

- [ ] SRE Lead, 2025-01-15: Deploy Vault Helm chart; configure Raft HA (3 replicas); validate auto-unseal
- [ ] Security Lead, 2025-02-01: Migrate all production credentials from K8s Secrets to Vault; rotate all keys
- [ ] Platform Eng, 2025-02-15: Enable Vault audit log shipping to SIEM; verify per-pod access logs

---

### ADR-010: Migrate Hive Tables to Apache Iceberg Incrementally via Spark CTAS

**Date:** 2025-06-01
**Status:** Accepted
**Deciders:** DE Lead, Platform Architect
**Tags:** migration, storage, lakehouse

---

**Context**

The data platform has 500 Hive-managed tables on HDFS/S3, totalling 80TB. The organisation has decided to adopt Apache Iceberg (see ADR-002). Pain points with Hive: no time travel, no ACID deletes, schema evolution breaks downstream queries, and the Hive Metastore is a scaling bottleneck. Migration must not disrupt the 30 production DAGs that read these tables. Target state: all tables on Iceberg REST catalog, Hive Metastore decommissioned within 9 months.

**Decision**

We will migrate tables incrementally over 9 months using a Spark CTAS (CREATE TABLE AS SELECT) pattern: for each table, create a new Iceberg table in a parallel namespace, backfill data from the Hive source, run a parallel validation period (both tables populated), then cut over consumers by updating DAG connection strings. Tables are migrated in priority tiers (highest downstream dependency count first).

**Rationale**

Spark CTAS is the safest migration path — the Hive source is read-only during backfill, the Iceberg table is built from scratch, and a validation gate (row count + checksum) must pass before any consumer is pointed at it. In-place migration using `ALTER TABLE ... SET TBLPROPERTIES` (Iceberg in-place migration) was evaluated but rejected because it leaves the underlying Hive metadata as a dependency during a transitional period, which complicates the Hive Metastore decommission goal. The incremental tier approach limits blast radius — 10 tables migrated per sprint, not 500 at once.

**Alternatives Considered**

**Option A: In-Place Iceberg Migration (ALTER TABLE)**
Pros: no data copy; instant switch; Iceberg-compatible reader can query immediately.
Cons: leaves Hive Metastore as a dependency until all tables are migrated; mixed Hive + Iceberg state in the same metastore is operationally confusing; rollback requires reverting ALTER TABLE which is not always clean.
Why not chosen: Hive Metastore dependency persists; clean decommission is harder.

**Option B: BigLake Managed Tables (GCP)**
Pros: Google-managed; Iceberg-compatible; no self-hosted catalog.
Cons: GCP-only; data residency in EU requires BigLake EU region (limited availability); introduces GCP dependency in an AWS-primary platform; cost unpredictable.
Why not chosen: cloud lock-in; not part of the cloud-neutral strategy.

**Option C: Stay on Hive Metastore with Iceberg for New Tables Only**
Pros: zero migration effort for existing tables; new tables benefit from Iceberg immediately.
Cons: dual catalog state persists indefinitely; Hive Metastore scaling issues remain; GDPR erasure on old Hive tables still requires full partition rewrites; time travel unavailable for historical data.
Why not chosen: does not resolve the Hive Metastore scaling bottleneck; GDPR compliance gap remains for 500 tables.

**Consequences**

Positive: phased migration limits blast radius; parallel validation gate prevents silent data loss; Hive Metastore decommission achievable within 9 months; GDPR compliance for all tables after migration.
Negative: 9 months of dual-catalog operational overhead; Spark CTAS jobs consume significant compute for 80TB backfill; each table requires a code change in the consuming DAG to update the catalog reference.
Neutral / Watch: track migration progress in a shared spreadsheet (table name, tier, status, validation date); automate the CTAS Airflow DAG as a parameterised template.

**Follow-up Actions**

- [ ] DE Lead, 2025-07-01: Build parameterised Airflow DAG for CTAS migration + validation gate
- [ ] DE Team, 2025-07-15: Complete Tier-1 migration (50 highest-priority tables); validate all consumers
- [ ] Platform Eng, 2026-03-01: Decommission Hive Metastore after final table migrated and validated

---

## 4. Technology Evaluation Criteria Reference

### Detailed Scoring Rubric

Use this rubric when populating the weighted scoring matrix for any technology evaluation.

#### Performance

| Score | Throughput | Latency | Scalability Ceiling |
|---|---|---|---|
| 5 | Documented benchmark at 10× your scale | < 100ms p99 for interactive; < 1min for batch | Linear horizontal scaling; no known ceiling |
| 4 | Benchmarked at 3× your scale | < 1s p99 interactive; < 10min batch | Horizontal scaling with minor config |
| 3 | Handles your current load with headroom | < 5s p99 interactive; < 1h batch | Scales with tuning; known limits documented |
| 2 | Meets load now; expect bottleneck at 2× | 5–30s interactive; 1–4h batch | Vertical scaling only; or costly workarounds |
| 1 | Struggles at current load | > 30s interactive; > 4h batch | Cannot scale to meet requirements |

#### Operational Complexity

| Score | Deployment | Upgrades | Observability | Backup/Restore |
|---|---|---|---|---|
| 5 | Single Helm chart; managed option available | Zero-downtime rolling; one command | Built-in dashboards + metrics | Automated; tested; < 1h RTO |
| 4 | Multi-component but documented Helm | Planned downtime < 30min; documented | Prometheus metrics built in | Documented procedure; < 4h RTO |
| 3 | Multi-service; 1–2 days to set up | Downtime < 2h; some manual steps | Logs available; metrics need work | Manual procedure; < 8h RTO |
| 2 | Complex; requires custom scripts | Upgrade frequently causes issues | Limited visibility; custom instrumentation needed | Untested or undocumented |
| 1 | Black box; no Helm/operator | Breaking changes per release | No observability | No backup/restore path |

#### Cost

Estimate total cost of ownership (TCO) over 24 months: licensing + compute + storage + engineering days.

| Score | Licensing | Compute/Storage Ratio | Engineering Overhead |
|---|---|---|---|
| 5 | Free (Apache OSS, MIT, Apache-2) | < 10% of existing infra spend | < 2 engineering weeks/year |
| 4 | Free or affordable SaaS tier | 10–25% of existing infra spend | 2–8 engineering weeks/year |
| 3 | Moderate commercial license | 25–50% of existing infra spend | 1–2 engineering months/year |
| 2 | Significant license cost | 50–100% of existing infra spend | 2–4 engineering months/year |
| 1 | Cost-prohibitive | > 100% of existing infra spend | > 4 engineering months/year |

#### Ecosystem Maturity

| Score | Connectors | Community | Documentation | Release Cadence |
|---|---|---|---|---|
| 5 | 100+ official connectors; SDKs in 5+ languages | > 10k GitHub stars; active Slack/Discord | Comprehensive; versioned; tutorials | Weekly patch; monthly minor |
| 4 | 30–100 connectors; major systems covered | 2–10k stars; responsive maintainers | Good reference docs; some gaps | Bi-weekly patch; quarterly minor |
| 3 | 10–30 connectors; key systems covered | 500–2k stars; community forums | Adequate reference; limited tutorials | Monthly patch; semi-annual minor |
| 2 | < 10 connectors; gaps in key systems | < 500 stars; slow responses | Sparse documentation | Infrequent releases; breaking changes |
| 1 | Minimal connectors; most require custom code | Niche; single maintainer | Minimal or outdated docs | No predictable cadence |

#### Team Fit

| Score | Existing Skills | Learning Curve | Hiring Market |
|---|---|---|---|
| 5 | Team uses it daily | Day-1 productive | Widely available; standard in job descriptions |
| 4 | Team familiar with it | < 1 sprint to productive | Easy to hire; common skill |
| 3 | 1–2 team members know it | 1–2 months to productive | Moderate hiring pool |
| 2 | No direct experience; related skills | 3–6 months to productive | Small hiring pool; premium compensation |
| 1 | Entirely foreign paradigm | > 6 months to productive | Very thin market; specialist only |

#### Vendor Risk

| Score | Governance | License | Exit Path |
|---|---|---|---|
| 5 | Apache/Linux Foundation; multi-sponsor | Apache-2 or MIT | Fork freely; data in open format |
| 4 | Strong OSS; commercial backer; BSL-style with exemptions | OSS with permissive commercial | Fork with effort; data portable |
| 3 | Single-company OSS; SSPL or CC | Usage restrictions in license | Migration possible but costly |
| 2 | Commercial with community edition | Proprietary; data export available | High switching cost; 6–12 month migration |
| 1 | Closed-source proprietary | No source access; data lock-in | No practical exit; full re-platform required |

---

## 5. Stakeholder Communication Patterns

### Non-Technical Stakeholder One-Pager

Use this format when presenting a decision to product, finance, or executive stakeholders.

```markdown
## Decision Summary: <Title>

**Problem**
<One paragraph: what is broken or what opportunity are we addressing? Avoid jargon.>

**Decision**
<One sentence: what we have decided to do.>

**Why this option**
- <Bullet 1: business benefit>
- <Bullet 2: cost or risk reduction>
- <Bullet 3: team / timeline fit>

**What we are not doing (and why)**
<One sentence per rejected alternative, in plain language.>

**Risks and mitigations**
| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Migration causes dashboard downtime | Low | High | Parallel run; rollback plan ready |
| Higher operational cost if compute not scaled down | Medium | Medium | Auto-scaling policy in week 1 |

**Timeline**
| Milestone | Date |
|---|---|
| Pilot (10% of traffic) | YYYY-MM-DD |
| Full rollout | YYYY-MM-DD |
| Old system decommissioned | YYYY-MM-DD |

**Cost impact**
Current: $X/month → Target: $Y/month (save $Z/month, $W over 24 months)

**Decision owner:** <Name>
**Review date:** <Date — revisit if assumptions change>
```

### Decision Review Meeting Agenda Template

```
Decision Review — <ADR Title>
Duration: 45 minutes

1. Context recap (5 min) — Presenter
   - Problem statement
   - Non-negotiable constraints

2. Options walkthrough (15 min) — Presenter
   - Scoring matrix review
   - Top 3 options pros/cons

3. Recommendation (5 min) — Presenter
   - Proposed decision and rationale

4. Open questions and concerns (15 min) — All
   - Structured round: each attendee states one concern or question
   - Presenter responds with mitigation or adjusts recommendation

5. Decision and next steps (5 min) — Decision owner
   - Explicit decision stated aloud (not implicit)
   - Follow-up owners and dates assigned
   - ADR status set to "Accepted" or "Proposed pending POC"
```

### Handling Disagreement and Escalation

**Step 1 — Acknowledge the concern explicitly.** Restate the objection in your own words: "You're saying that if we choose Flink now, we risk the Q3 deadline — is that right?" This demonstrates you've heard the concern and prevents the discussion from becoming circular.

**Step 2 — Separate "I disagree with the decision" from "I disagree with the process."** If the concern is about missing information, run a time-boxed spike (1–2 days) and reconvene. If the concern is a values disagreement ("we should never use SaaS"), escalate to the architecture committee.

**Step 3 — Use a "disagree and commit" resolution path.** If consensus cannot be reached but the decision is time-sensitive: document the dissent in the ADR Consequences section ("Team member X raised concern Y; this will be monitored via metric Z"), set a formal review date, and proceed. Dissent is recorded, not suppressed.

**Step 4 — Escalation path:**
- Technical disagreement → Architecture Review Board (if it exists) or engineering leadership
- Budget disagreement → Finance + VP sign-off
- Cross-team impact → Raise as RFC (Request for Comments) with a 5-business-day comment window

---

## 6. ADR Lifecycle Management

### Numbering and Status

- Number sequentially: ADR-001, ADR-002, ... ADR-NNN. Never reuse a number.
- Use a three-digit zero-padded number to keep filesystem sort order stable.
- Status transitions:

```
Proposed → Accepted      (decision made, implemented)
Accepted  → Deprecated   (decision is still in place but superseded by circumstances; not replaced by a new ADR)
Accepted  → Superseded by ADR-NNN  (a new ADR replaces this one; link bidirectionally)
```

- When superseding: add `Superseded by ADR-NNN` to the old ADR's status field AND add a "Supersedes ADR-NNN" note in the new ADR's context.
- Never delete an ADR — historical decisions are valuable context.

### File and Directory Layout

```
docs/decisions/
  ADR-001-lambda-architecture.md
  ADR-002-iceberg-table-format.md
  ADR-003-airflow-kubernetes-executor.md
  ...
  README.md          ← index of all ADRs with one-line summaries
```

### Linking ADRs to Code

Place a `DECISIONS.md` in the repository root (or in `docs/`) that lists all ADRs with status and a link to the full document. Reference relevant ADRs from code comments and README sections:

```python
# This pipeline uses Iceberg MERGE for upserts.
# Architecture rationale: docs/decisions/ADR-002-iceberg-table-format.md
def upsert_orders(df: DataFrame, target_table: str) -> None:
    ...
```

```yaml
# airflow/dags/orders_etl.py
# Orchestrator choice rationale: docs/decisions/ADR-003-airflow-kubernetes-executor.md
```

In `CLAUDE.md` or `README.md`:
```markdown
## Architecture Decisions
All significant architectural decisions are recorded in `docs/decisions/`.
See [ADR Index](docs/decisions/README.md) for the full list.
Key decisions:
- [ADR-002](docs/decisions/ADR-002-iceberg-table-format.md): Why we use Apache Iceberg
- [ADR-003](docs/decisions/ADR-003-airflow-kubernetes-executor.md): Why we use Airflow with KubernetesExecutor
```

### Tooling

| Tool | Use case | Notes |
|---|---|---|
| `adr-tools` (CLI) | Create/supersede ADRs from the terminal; enforces numbering | `brew install adr-tools`; `adr new "Use Iceberg"` |
| `log4brains` | Web UI for browsing ADRs; generates static site | npm-based; integrates with GitHub Actions for auto-publish |
| Architecture Decision Hub (ADHub) | Enterprise SaaS for ADR management, search, and governance | Useful at 100+ ADRs with multiple teams |
| GitHub Discussions | Light-weight RFC process before writing an ADR | Label `type: architecture-decision`; link to resulting ADR |

**adr-tools quick start:**
```bash
# Initialise ADR directory in the repo
adr init docs/decisions

# Create a new ADR (auto-increments number)
adr new "Use Apache Iceberg as Table Format"
# → creates docs/decisions/0002-use-apache-iceberg-as-table-format.md

# Supersede an existing ADR
adr new -s 2 "Migrate from Iceberg v1 to Iceberg v2"
# → creates ADR-0003 and updates ADR-0002 status to Superseded
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Decision by consensus (HiPPO) | The highest-paid person's opinion wins; alternatives never documented | Use a weighted scoring matrix; document dissent in the ADR |
| No ADR for "obvious" decisions | Obvious today, mysterious in 18 months when the author has left | Write a short ADR even for obvious choices; future readers are grateful |
| ADR written after implementation | Decision is rationalised post-hoc; alternatives never genuinely evaluated | Write the ADR in "Proposed" status before building; accept after review |
| Scoring matrix without weights | All criteria treated as equal regardless of context | Set weights before scoring; weights make the trade-off explicit |
| Single alternative considered | Not a real decision record — just a justification | Always evaluate at least 2 alternatives; 3 is better |
| ADR that is a technology vendor pitch | Reads as marketing copy for the chosen tool | Use the scoring rubric; include genuine cons for the chosen option |
| Accepting an ADR but never doing the follow-up actions | Decisions rot; follow-up is the accountability mechanism | Assign owners and dates to every action item; track in a ticket system |
| Status left as "Proposed" for months | Team ignores the document; no clear decision was made | Set a decision deadline in the meeting agenda; escalate if missed |
| No link from code to ADR | Developers encounter a pattern and don't know why; re-debate the decision | Add ADR links in code comments and README |
| Superseding an ADR without updating the old one | Old ADR found via search; reader follows stale guidance | Update old ADR status to "Superseded by ADR-NNN" bidirectionally |
| Massive ADR covering 5 decisions | Too long to read; impossible to supersede cleanly | One decision per ADR; split if scope is growing |
| Skipping the Consequences section | Team only sees upsides; negative consequences are a surprise | Fill in negative consequences honestly — this is where operational surprises live |

---

## References to Consult When Needed

- `skills/trino_iceberg/SKILL.md` — Trino + Iceberg DDL, DML, partition design, maintenance (relevant to ADR-002, ADR-005, ADR-010)
- `skills/airflow_dags/SKILL.md` — Airflow operator reference, TaskFlow API, KubernetesExecutor config (relevant to ADR-003)
- `skills/dbt_trino/SKILL.md` — dbt + Trino adapter, incremental strategies, CI/CD (relevant to ADR-004)
- `skills/cdc_debezium/SKILL.md` — Debezium connector config, WAL, SMT, Kafka Connect deployment (relevant to ADR-006)
- `skills/soda_core/SKILL.md` — SodaCL check syntax, Airflow integration, scan API (relevant to ADR-007)
- `skills/great_expectations/SKILL.md` — GX DataContext, Checkpoints, Airflow gate (relevant to ADR-007 alternative)
- `skills/datahub_catalog/SKILL.md` — DataHub ingestion recipes, lineage, GraphQL API (relevant to ADR-008)
- `skills/delta_lake/SKILL.md` — Delta Lake DDL/DML, MERGE patterns, OPTIMIZE (relevant to ADR-002 alternative)
- `skills/pyspark_streaming/SKILL.md` — Structured Streaming, micro-batch triggers, watermarks (relevant to ADR-001)
- `skills/apache_flink/SKILL.md` — Flink Table API, event-time, checkpointing (relevant to ADR-001 alternative)
- `skills/kubernetes_data/SKILL.md` — Spark-on-K8s, KubernetesExecutor, Secrets management, RBAC (relevant to ADR-003, ADR-009)
- `skills/medallion_architecture/SKILL.md` — Bronze/Silver/Gold layers, CDC micro-batch, schema evolution (relevant to ADR-001, ADR-002)
- [ADR GitHub repository — Joel Parker Henderson](https://github.com/joelparkerhenderson/architecture-decision-record) — extensive ADR template collection
- [adr-tools CLI](https://github.com/npryce/adr-tools) — command-line tool for creating and managing ADRs
- [log4brains](https://github.com/thomvaill/log4brains) — ADR web UI and static site generator
- [Documenting Architecture Decisions — Michael Nygard (original 2011 post)](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
- [Architecture Decision Records in Practice — ThoughtWorks Technology Radar](https://www.thoughtworks.com/en-us/radar/techniques/architecture-decision-records)
