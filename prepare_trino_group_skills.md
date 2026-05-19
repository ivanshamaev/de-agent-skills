# Trino Modern Data Stack Skills Pack

Ниже — набор из **20 production-grade Trino skills** для Modern Data Stack.
Каждый skill рассчитан на полноценный `SKILL.md` файл в стиле:

* AI Agent Skills
* Claude Code Skills
* Data Platform Skills

Skills покрывают:

* Trino administration
* Iceberg lakehouse
* dbt + Trino
* Airflow orchestration
* Docker Compose
* Query optimization
* Platform engineering
* Governance
* Production readiness

Документация:

* [Trino Docs](https://trino.io/docs/current/?utm_source=chatgpt.com)
* [Iceberg Docs](https://iceberg.apache.org/docs/latest/?utm_source=chatgpt.com)
* [dbt Docs](https://docs.getdbt.com/?utm_source=chatgpt.com)
* [Airflow Docs](https://airflow.apache.org/docs/?utm_source=chatgpt.com)

---

# 1. `trino-lakehouse-platform-architect`

## Purpose

Проектирование Trino-based Modern Data Platform.

## Covers

* Iceberg
* Hive Metastore
* MinIO/S3
* Kafka
* dbt
* Airflow
* Superset

## Best Practices

* decoupled storage/compute
* object storage first
* metadata-driven architecture
* Iceberg as table format
* federated query layer

---

# 2. `trino-query-optimization`

## Focus

Оптимизация distributed SQL queries.

## Covers

* predicate pushdown
* join reordering
* dynamic filtering
* broadcast joins
* aggregation pushdown

## Best Practices

* avoid SELECT *
* reduce shuffle
* minimize cross-catalog joins
* filter early

### Docs

* [Trino Optimizer Docs](https://trino.io/docs/current/optimizer.html?utm_source=chatgpt.com)

---

# 3. `trino-explain-plan-review`

## Focus

Анализ `EXPLAIN` и distributed execution plans.

## Reviews

* stage distribution
* exchange bottlenecks
* skew detection
* remote scans
* repartitioning

---

# 4. `trino-iceberg-best-practices`

## Focus

Production-ready Iceberg usage with Trino.

## Covers

* hidden partitioning
* snapshot management
* metadata cleanup
* partition evolution
* file compaction

## Best Practices

* avoid tiny files
* optimize snapshots
* use partition evolution carefully
* enable metadata pruning

### Docs

* [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html?utm_source=chatgpt.com)

---

# 5. `trino-admin-cluster-health`

## Focus

Cluster observability and administration.

## Covers

* coordinator health
* worker stability
* query queue
* memory pressure
* spill analysis

## Best Practices

* dedicated coordinator
* autoscaling workers
* monitor heap pressure

---

# 6. `trino-memory-and-spill-tuning`

## Focus

Memory optimization.

## Covers

* spill-to-disk
* memory pools
* exchange buffers
* OOM prevention

## Best Practices

* spill large joins
* optimize partition count
* avoid oversized exchanges

---

# 7. `trino-resource-group-governance`

## Focus

Workload governance.

## Covers

* resource groups
* query prioritization
* concurrency control
* tenant isolation

### Docs

* [Resource Groups Docs](https://trino.io/docs/current/admin/resource-groups.html?utm_source=chatgpt.com)

---

# 8. `trino-dbt-platform`

## Purpose

Production dbt + Trino architecture.

## Covers

* dbt-trino adapter
* incremental models
* semantic layer
* slim CI
* testing

## Best Practices

* incremental-first models
* partition-aware transformations
* avoid massive rebuilds

---

# 9. `trino-dbt-query-performance`

## Focus

Optimizing dbt-generated SQL.

## Covers

* ephemeral models
* CTE explosion
* materialization strategy
* merge optimization

---

# 10. `trino-airflow-orchestration`

## Purpose

Airflow orchestration for Trino pipelines.

## Covers

* DAG generation
* retries
* SLAs
* sensors
* incremental loads

## Best Practices

* idempotent DAGs
* partition-aware scheduling
* metadata-driven orchestration

---

# 11. `trino-airflow-lakehouse-pipelines`

## Focus

Lakehouse ETL orchestration.

## Covers

* Iceberg maintenance
* snapshot expiration
* compaction jobs
* metadata cleanup

---

# 12. `trino-docker-compose-stack`

## Purpose

Generate full local Trino Lakehouse environment.

## Generates

* Trino
* Iceberg
* Hive Metastore
* MinIO
* Airflow
* dbt

## Best Practices

* healthchecks
* startup ordering
* persistent volumes
* isolated networks

---

# 13. `trino-federated-query-architecture`

## Focus

Cross-system analytics.

## Covers

* PostgreSQL
* Kafka
* Iceberg
* Hive
* ClickHouse

## Best Practices

* minimize federated joins
* push compute to source
* cache expensive datasets

---

# 14. `trino-file-layout-optimization`

## Focus

Data layout optimization.

## Covers

* Parquet sizing
* ORC tuning
* row groups
* partition structure

## Best Practices

* 128MB–1GB file sizing
* avoid tiny files
* optimize scan parallelism

---

# 15. `trino-observability-platform`

## Purpose

Monitoring and observability.

## Covers

* Prometheus
* Grafana
* OpenTelemetry
* query metrics
* JVM monitoring

### Docs

* [Prometheus Docs](https://prometheus.io/docs/?utm_source=chatgpt.com)

---

# 16. `trino-production-readiness-review`

## Focus

Production deployment validation.

## Covers

* HA architecture
* backups
* security
* scaling
* observability

## Best Practices

* coordinator HA
* autoscaling workers
* encrypted object storage

---

# 17. `trino-security-and-governance`

## Focus

Security hardening.

## Covers

* TLS
* OAuth
* RBAC
* Ranger integration
* catalog isolation

## Best Practices

* least privilege
* separate catalogs by domain
* secure object storage

---

# 18. `trino-cost-optimization`

## Focus

Warehouse and infrastructure cost reduction.

## Covers

* query efficiency
* scan minimization
* autoscaling
* storage optimization

## Best Practices

* optimize metadata scans
* reduce full table scans
* compact Iceberg tables

---

# 19. `trino-self-healing-platform`

## Purpose

Autonomous operations for Trino platform.

## Autonomous Actions

* restart failed workers
* detect query anomalies
* rebalance workloads
* optimize memory configs

## AI Features

* RCA generation
* workload prediction
* incident remediation

---

# 20. `trino-modern-data-stack-reference-architecture`

## Purpose

Generate end-to-end Modern Data Stack architecture.

## Stack

* Airflow
* Kafka
* Iceberg
* Trino
* dbt
* Superset
* MinIO
* Prometheus
* Grafana

## Outputs

* docker-compose
* architecture diagrams
* infra configs
* operational recommendations

---

# Recommended Repository Structure

```txt
skills/
├── trino-query-optimization/
│   ├── SKILL.md
│   ├── examples/
│   ├── templates/
│   └── checks/
├── trino-iceberg-best-practices/
├── trino-dbt-platform/
├── trino-airflow-orchestration/
└── ...
```

---

# Recommended SKILL.md Template

```md
---
name: trino-query-optimization
description: Optimize distributed SQL workloads in Trino
---

# Triggers
- optimize trino query
- analyze explain plan
- reduce query latency

# Workflow
1. Analyze query
2. Review explain plan
3. Detect bottlenecks
4. Optimize joins
5. Reduce shuffle
6. Validate pushdown

# Best Practices
- filter early
- avoid SELECT *
- minimize repartitioning

# Outputs
- optimized SQL
- tuning recommendations
- explain analysis
```

---

# Самые важные skills для начала

Если делать MVP репозитория:

1. trino-query-optimization
2. trino-iceberg-best-practices
3. trino-dbt-platform
4. trino-airflow-orchestration
5. trino-admin-cluster-health
6. trino-memory-and-spill-tuning
7. trino-production-readiness-review
8. trino-observability-platform
9. trino-federated-query-architecture
10. trino-modern-data-stack-reference-architecture
