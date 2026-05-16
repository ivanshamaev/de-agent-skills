
## 📂 1. Orchestration & Workflow Management (Оркестрация)

* apache-airflow-pipelines: Разработка DAG, динамическая генерация задач и идемпотентность.
* Документация: Apache Airflow Docs
   * Best Practices: Astronomer Airflow Best Practices
* dagster-assets: Управление конвейерами на основе декларативных Software-Defined Assets (SDA).
* Документация: Dagster Docs
   * Best Practices: Dagster Best Practices Guide
* prefect-workflows: Построение событийно-ориентированных (event-driven) потоков данных с динамическим кэшированием.
* Документация: Prefect Docs
   * Best Practices: Prefect Idiomatic Patterns
* mage-ai-pipelines: Создание гибридных пайплайнов (SQL + Python) в модульной архитектуре Mage.
* Документация: Mage.ai Docs
   * Best Practices: Mage Design Principles

## 📊 2. Data Transformation & Modeling (Трансформация и моделирование)

* dbt-modeling: Модульная трансформация данных, макросы, генерация тестов и построение графов зависимостей (lineage).
* Документация: dbt Core Docs
   * Best Practices: dbt Labs Analytics Engineering Guide
* sqlmesh-incremental: Продвинутое инкрементальное обновление таблиц с виртуальной средой изолированного тестирования (Virtual Environments).
* Документация: SQLMesh Docs
   * Best Practices: SQLMesh Architecture Guides
* data-vault-modeling: Проектирование корпоративных хранилищ по методологии Data Vault 2.0 (Hubs, Satellites, Links).
* Документация: Data Vault Alliance
   * Best Practices: Scalable dbt Vault Patterns
* dimensional-modeling: Проектирование классических схем «Звезда» и «Снежинка» по методологии Кимбалла.
* Документация: Kimball Group Core Concepts
   * Best Practices: Dimensional Modeling Techniques

## 🚀 3. Big Data & Distributed Computing (Распределенные вычисления)

* apache-spark-optimization: Настройка Spark Catalyst Optimizer, тюнинг памяти (Executor/Driver), борьба со skewing (перекосом данных).
* Документация: Apache Spark Documentation
   * Best Practices: Databricks Spark Performance Tuning
* pyspark-structured-streaming: Разработка отказоустойчивых конвейеров обработки потоковых данных в реальном времени.
* Документация: PySpark Streaming Guide
   * Best Practices: Structured Streaming Production Checklist
* apache-flink-processing: Разработка stateful-приложений обработки потоков с управлением event-time и windowing.
* Документация: Apache Flink Docs
   * Best Practices: Flink Operations & Tuning Best Practices
* ray-data-processing: Распределенные вычисления для Data и AI нагрузок на базе фреймворка Ray.
* Документация: Ray Data Docs
   * Best Practices: Ray Design Patterns

## 🗄️ 4. Modern Storage & Data Lakes (Хранение данных)

* delta-lake-management: Оптимизация ACID-таблиц (Z-Order, Optimize, Vacuum) и тайм-тревелинг (Time Travel).
* Документация: Delta Lake Docs
   * Best Practices: Delta Lake Production Optimization
* apache-iceberg-optimization: Управление скрытым партиционированием (Hidden Partitioning) и эволюцией схем без перезаписи данных.
* Документация: Apache Iceberg Docs
   * Best Practices: Tabular Iceberg Definitive Guide
* clickhouse-olap: Проектирование сверхбыстрых аналитических таблиц с движками семейства MergeTree.
* Документация: ClickHouse Docs
   * Best Practices: ClickHouse Query Optimization Guide
* snowflake-warehouse-tuning: Настройка автоскейлинга виртуальных складов Snowflake и оптимизация затрат на запросы (FinOps).
* Документация: Snowflake Documentation
   * Best Practices: Snowflake Performance & Cost Best Practices
* duckdb-local-analytics: Эффективный in-process анализ Parquet/CSV файлов на локальных ресурсах без развертывания кластера.
* Документация: DuckDB Docs
   * Best Practices: DuckDB Performance Optimization
* postgresql-indexing: Настройка индексов (B-Tree, BRIN, GIN) и стратегий вакуумирования (Autovacuum) для Data-нагрузок.
* Документация: PostgreSQL Manual
   * Best Practices: Supabase Postgres Performance Guidelines [2] 

## 📥 5. Data Ingestion & Streaming (Интеграция и стриминг)

* apache-kafka-pipelines: Настройка топиков, партиционирования, консьюмер-групп и обеспечение семантики Exactly-Once.
* Документация: Apache Kafka Docs
   * Best Practices: Confluent Kafka Best Practices
* airbyte-connections: Настройка кастомных и стандартных коннекторов по протоколу Airbyte (ELT).
* Документация: Airbyte Docs
   * Best Practices: Airbyte Connector Development Guide
* meltano-singapore: Декларативное управление ELT-пайплайнами на основе стандартов Singer (Taps и Targets).
* Документация: Meltano Docs
   * Best Practices: Meltano Production Deployment Guide
* vectorized-redpanda: Настройка высокопроизводительного Kafka-совместимого стриминга на базе Redpanda без JVM.
* Документация: Redpanda Docs
   * Best Practices: Redpanda Cluster Deployment & Tuning

## 🛠️ 6. Data Quality, Governance & Observability (Качество и наблюдаемость)

* great-expectations-validation: Создание кастомных чек-листов (Expectation Suites) и автоматическая валидация батчей данных.
* Документация: Great Expectations Docs
   * Best Practices: Great Expectations Deployment Patterns
* soda-core-scans: Написание SodaCL файлов для декларативного мониторинга качества данных в CI/CD.
* Документация: Soda Core Docs
   * Best Practices: SodaCL Best Practices
* openlineage-tracking: Сбор метаданных о происхождении данных (data lineage) в сквозном формате.
* Документация: OpenLineage Docs
   * Best Practices: OpenLineage Integration with Airflow/Spark
* amundsen-metadata: Интеграция каталогов данных Amundsen для прозрачного поиска (Data Discovery).
* Документация: Amundsen Docs
   * Best Practices: Amundsen Architecture & Ingestion
* data-contracts-definition: Разработка контрактов данных (Data Contracts) на базе YAML/JSON-schema для защиты от ломающих изменений (breaking changes).
* Документация: Data Contract CLI
   * Best Practices: PayPal Data Contract Architecture Patterns

## ☁️ 7. Infrastructure as Code & DataOps (Инфраструктура и CI/CD)

* terraform-data-infrastructure: Декларативное развертывание облачных хранилищ, бакетов S3/GCS и IAM-ролей.
* Документация: HashiCorp Terraform Docs
   * Best Practices: Terraform Standard Module Structure
* kubernetes-data-jobs: Развертывание и масштабирование стейтлесс- и стейтфул-нагрузок (Spark-on-k8s, Airflow Helm charts).
* Документация: Kubernetes Docs
   * Best Practices: Cloud Native Computing Foundation (CNCF) Guides
* docker-data-environments: Создание воспроизводимых, многоэтапных (multi-stage) Docker-образов для дата-сервисов с минимизацией веса.
* Документация: Docker Docs
   * Best Practices: Docker Engine Best Practices for Production
* github-actions-dataops: Настройка CI/CD пайплайнов для dbt-тестов, линтинга SQL (SQLFluff) и автоматического деплоя DAG-файлов.
* Документация: GitHub Actions Docs
   * Best Practices: Automating dbt with GitHub Actions
* sqlfluff-linting: Конфигурирование автоматического линтинга и авто-исправления синтаксиса сложных SQL-диалектов.
* Документация: SQLFluff Docs
   * Best Practices: SQLFluff Real-world Configuration Guide





===============================================

Ниже — набор из **35+ Data Engineering AI Agent Skills** для твоего репозитория [de-agent-skills](https://github.com/ivanshamaev/de-agent-skills?utm_source=chatgpt.com).
Я подобрал skills в стиле экосистемы Agent Skills / Claude Code Skills: каждый skill — это отдельный workflow с best practices, документацией и четкими trigger-фразами. Основывался на лучших практиках из репозиториев [Agent Skills Specification](https://github.com/agentskills/agentskills?utm_source=chatgpt.com), [addyosmani/agent-skills](https://github.com/addyosmani/agent-skills?utm_source=chatgpt.com) и [Olshansk agent-skills](https://github.com/Olshansk/agent-skills?utm_source=chatgpt.com). ([GitHub][1])

---

# Core Data Engineering Skills

## 1. `de-airflow-dag-review`

Проверка DAG'ов на reliability, retries, idempotency, SLAs.

### Документация

* [Apache Airflow Docs](https://airflow.apache.org/docs/?utm_source=chatgpt.com)
* [Astronomer Airflow Best Practices](https://www.astronomer.io/docs/learn/airflow-best-practices/?utm_source=chatgpt.com)

### Best Practices

* Avoid dynamic DAG explosions
* Use TaskFlow API
* Idempotent tasks
* Centralized alerting
* Retry policies + exponential backoff

---

## 2. `de-spark-job-optimizer`

Оптимизация Spark jobs.

### Документация

* [Apache Spark Docs](https://spark.apache.org/docs/latest/?utm_source=chatgpt.com)
* [Databricks Spark Tuning Guide](https://docs.databricks.com/aws/en/optimizations/spark-ui-guide?utm_source=chatgpt.com)

### Best Practices

* Avoid wide shuffles
* Partition pruning
* Broadcast joins
* Adaptive Query Execution
* Cache only reused datasets

---

## 3. `de-dbt-model-review`

Проверка dbt моделей и lineage.

### Документация

* [dbt Docs](https://docs.getdbt.com/?utm_source=chatgpt.com)
* [dbt Style Guide](https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md?utm_source=chatgpt.com)

### Best Practices

* Layered marts
* Incremental models
* Source freshness
* Naming conventions
* Test coverage

---

## 4. `de-kafka-stream-debug`

Диагностика Kafka lag, throughput, delivery guarantees.

### Документация

* [Apache Kafka Docs](https://kafka.apache.org/documentation/?utm_source=chatgpt.com)
* [Confluent Best Practices](https://docs.confluent.io/platform/current/streams/developer-guide/best-practices.html?utm_source=chatgpt.com)

### Best Practices

* Idempotent producers
* Proper partition strategy
* Schema Registry
* DLQ patterns
* Consumer lag monitoring

---

## 5. `de-data-quality-guardian`

Автоматический аудит качества данных.

### Документация

* [Great Expectations Docs](https://docs.greatexpectations.io/?utm_source=chatgpt.com)
* [Monte Carlo Data Quality Guide](https://www.montecarlodata.com/blog-data-quality-checks-in-etl/?utm_source=chatgpt.com)

### Best Practices

* Freshness checks
* Null thresholds
* Schema drift detection
* Volume anomaly checks
* Referential integrity tests

---

## 6. `de-snowflake-cost-optimizer`

### Документация

* [Snowflake Docs](https://docs.snowflake.com/?utm_source=chatgpt.com)
* [Snowflake Cost Optimization](https://docs.snowflake.com/en/user-guide/cost-understanding-overall?utm_source=chatgpt.com)

### Best Practices

* Auto suspend warehouses
* Clustering strategy
* Query profile analysis
* Materialized views
* Storage lifecycle policies

---

## 7. `de-bigquery-optimizer`

### Документация

* [BigQuery Docs](https://cloud.google.com/bigquery/docs?utm_source=chatgpt.com)
* [BigQuery Performance Best Practices](https://cloud.google.com/bigquery/docs/best-practices-performance-overview?utm_source=chatgpt.com)

### Best Practices

* Partitioned tables
* Clustered tables
* Avoid SELECT *
* Predicate pushdown
* Bytes scanned minimization

---

## 8. `de-redshift-performance-tuner`

### Документация

* [Amazon Redshift Docs](https://docs.aws.amazon.com/redshift/?utm_source=chatgpt.com)
* [AWS Redshift Best Practices](https://docs.aws.amazon.com/redshift/latest/dg/c_designing-queries-best-practices.html?utm_source=chatgpt.com)

---

## 9. `de-clickhouse-query-optimizer`

### Документация

* [ClickHouse Docs](https://clickhouse.com/docs?utm_source=chatgpt.com)
* [ClickHouse Best Practices](https://clickhouse.com/docs/en/guides/best-practices?utm_source=chatgpt.com)

---

## 10. `de-iceberg-table-maintenance`

### Документация

* [Apache Iceberg Docs](https://iceberg.apache.org/docs/latest/?utm_source=chatgpt.com)
* [Iceberg Best Practices](https://www.dremio.com/blog/best-practices-for-managing-apache-iceberg-tables/?utm_source=chatgpt.com)

---

# Lakehouse & Storage Skills

## 11. `de-delta-lake-optimizer`

* [Delta Lake Docs](https://docs.delta.io/latest/index.html?utm_source=chatgpt.com)
* Z-ordering
* Vacuum retention
* Optimize compaction

---

## 12. `de-hudi-maintenance`

* [Apache Hudi Docs](https://hudi.apache.org/docs/overview/?utm_source=chatgpt.com)

---

## 13. `de-parquet-optimizer`

* [Parquet Docs](https://parquet.apache.org/docs/?utm_source=chatgpt.com)

---

## 14. `de-partitioning-strategy-designer`

* Time-based partitioning
* Hash partitioning
* Small files prevention

### Документация

* [Databricks Partitioning Guide](https://docs.databricks.com/aws/en/tables/partitions?utm_source=chatgpt.com)

---

## 15. `de-schema-evolution-manager`

* Forward/backward compatibility
* Avro evolution

### Документация

* [Confluent Schema Evolution](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html?utm_source=chatgpt.com)

---

# Streaming Skills

## 16. `de-flink-stream-review`

* [Apache Flink Docs](https://nightlies.apache.org/flink/flink-docs-master/?utm_source=chatgpt.com)

---

## 17. `de-kinesis-pipeline-audit`

* [Amazon Kinesis Docs](https://docs.aws.amazon.com/kinesis/?utm_source=chatgpt.com)

---

## 18. `de-streaming-latency-debugger`

* Watermarks
* Event-time processing
* Backpressure analysis

---

## 19. `de-cdc-pipeline-review`

* Debezium validation
* Ordering guarantees

### Документация

* [Debezium Docs](https://debezium.io/documentation/?utm_source=chatgpt.com)

---

# SQL & Modeling Skills

## 20. `de-sql-performance-review`

* Query plans
* Join order analysis
* Index recommendations

### Документация

* [Use The Index Luke](https://use-the-index-luke.com/?utm_source=chatgpt.com)

---

## 21. `de-dimensional-model-review`

* Star schema validation
* Slowly Changing Dimensions

### Документация

* [Kimball Group](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/?utm_source=chatgpt.com)

---

## 22. `de-data-vault-review`

* Hub/link/satellite validation

### Документация

* [Data Vault Alliance](https://datavaultalliance.com/?utm_source=chatgpt.com)

---

## 23. `de-metric-layer-review`

* Semantic consistency
* KPI governance

### Документация

* [Transform MetricFlow Docs](https://docs.getdbt.com/docs/build/metricflow-commands?utm_source=chatgpt.com)

---

# Reliability & Observability

## 24. `de-pipeline-observability-setup`

* Lineage
* SLA tracking
* Incident routing

### Документация

* [OpenLineage Docs](https://openlineage.io/docs/?utm_source=chatgpt.com)

---

## 25. `de-data-lineage-builder`

* Column lineage
* End-to-end lineage

### Документация

* [DataHub Docs](https://datahubproject.io/docs/?utm_source=chatgpt.com)

---

## 26. `de-root-cause-analysis`

* Failed DAG RCA
* Dependency tracing
* Upstream/downstream analysis

---

## 27. `de-sla-monitoring`

* SLA miss prediction
* Critical path analysis

### Документация

* [Airflow SLAs Docs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html?utm_source=chatgpt.com#slas)

---

## 28. `de-incidents-postmortem-writer`

* Blameless postmortems
* CAPA generation

### Документация

* [Google SRE Book](https://sre.google/sre-book/table-of-contents/?utm_source=chatgpt.com)

---

# Infrastructure & Platform

## 29. `de-terraform-review`

* IaC validation
* State management

### Документация

* [Terraform Docs](https://developer.hashicorp.com/terraform/docs?utm_source=chatgpt.com)

---

## 30. `de-kubernetes-data-platform-review`

* Spark on K8s
* Autoscaling
* Resource quotas

### Документация

* [Kubernetes Docs](https://kubernetes.io/docs/home/?utm_source=chatgpt.com)

---

## 31. `de-docker-pipeline-review`

* Reproducible builds
* Multi-stage Dockerfiles

### Документация

* [Docker Docs](https://docs.docker.com/?utm_source=chatgpt.com)

---

## 32. `de-ci-cd-data-pipelines`

* GitOps
* DAG validation
* dbt CI

### Документация

* [GitHub Actions Docs](https://docs.github.com/actions?utm_source=chatgpt.com)

---

# Governance & Security

## 33. `de-pii-detection-review`

* Sensitive columns scanning
* Masking strategy

### Документация

* [Google DLP Docs](https://cloud.google.com/sensitive-data-protection/docs?utm_source=chatgpt.com)

---

## 34. `de-rbac-audit`

* Least privilege review
* Warehouse access audit

---

## 35. `de-gdpr-compliance-check`

* Retention policies
* Right-to-delete workflows

### Документация

* [GDPR Official Portal](https://gdpr.eu/?utm_source=chatgpt.com)

---

# ML/DataOps Skills

## 36. `de-feature-store-review`

* Online/offline consistency
* Point-in-time correctness

### Документация

* [Feast Docs](https://docs.feast.dev/?utm_source=chatgpt.com)

---

## 37. `de-ml-data-pipeline-review`

* Training-serving skew
* Dataset versioning

### Документация

* [TFX Docs](https://www.tensorflow.org/tfx?utm_source=chatgpt.com)

---

## 38. `de-rag-data-pipeline`

* Chunking
* Embedding refresh
* Hybrid retrieval

### Документация

* [LangChain Docs](https://python.langchain.com/docs/introduction/?utm_source=chatgpt.com)
* [LlamaIndex Docs](https://docs.llamaindex.ai/?utm_source=chatgpt.com)

---

# AI-Agent Specific Skills

## 39. `de-agentic-debugger`

AI-agent workflow для RCA data pipeline failures.

### Источники

* [Agent Skills Docs](https://github.com/agentskills/agentskills?utm_source=chatgpt.com)
* [AI Agent Skills Catalog](https://developersglobal.github.io/ai-agent-skills/?utm_source=chatgpt.com)

---

## 40. `de-data-platform-architect`

Генерация архитектуры data platform.

### Best Practices

* Medallion architecture
* Separation of compute/storage
* Event-driven ingestion
* Governance-first design

---

# Что ещё очень рекомендую добавить

## Meta-skills

* `de-session-commit`
* `de-architecture-decision-record`
* `de-tech-debt-review`
* `de-production-readiness-review`
* `de-cost-estimation`
* `de-oncall-assistant`
* `de-runbook-generator`

---

# Как лучше организовать репозиторий

Структура:

```txt
skills/
  de-airflow-dag-review/
    SKILL.md
    examples/
    templates/
    checks/
```

Внутри `SKILL.md`:

```md
---
name: de-airflow-dag-review
description: Reviews Airflow DAGs for reliability and production readiness
---

# Triggers
- review airflow dag
- optimize dag
- airflow best practices

# Workflow
1. Validate retries
2. Validate SLAs
3. Detect anti-patterns
4. Review dependencies
5. Generate remediation plan

# Output
- Issues
- Risk level
- Suggested fixes
```

---

# Самые ценные skills для старта

Если делать MVP репозитория — я бы начал с:

1. Airflow DAG Review
2. Spark Optimizer
3. dbt Review
4. Data Quality Guardian
5. Kafka Stream Debug
6. SQL Performance Review
7. Pipeline Observability
8. Root Cause Analysis
9. Snowflake Cost Optimizer
10. Production Readiness Review

---

# Дополнительные каталоги skills

Полезно изучить:

* [agentskills.me](https://agentskills.me/?utm_source=chatgpt.com)
* [DevelopersGlobal AI Skills](https://developersglobal.github.io/ai-agent-skills/?utm_source=chatgpt.com)
* [addyosmani/agent-skills](https://github.com/addyosmani/agent-skills?utm_source=chatgpt.com)
* [Anthropic Skills Repo](https://github.com/anthropics/skills?utm_source=chatgpt.com)

([agentskills.me][2])

[1]: https://github.com/agentskills/agentskills?utm_source=chatgpt.com "GitHub - agentskills/agentskills: Specification and documentation for Agent Skills · GitHub"
[2]: https://agentskills.me/?utm_source=chatgpt.com "Discover Agent Skills - Agent Skills"



---------------------------------------------------


Чтобы построить действительно **AI Agentic Data Platform**, нужно думать не как про “очередной data stack”, а как про **операционную систему для автономных data agents**.

Главная идея:
не люди руками управляют пайплайнами, а AI-агенты:

* понимают intent,
* строят ingestion,
* оптимизируют compute,
* чинят data quality,
* делают RCA,
* управляют cost/performance/governance,
* генерируют SQL/dbt/Spark,
* следят за reliability.

Это уже следующий слой после:

* Data Warehouse
* Lakehouse
* Modern Data Stack
* DataOps

Ниже — архитектура платформы, которая сегодня выглядела бы действительно инновационно.

---

# 1. Vision платформы

Платформа должна быть:

| Capability       | Описание                          |
| ---------------- | --------------------------------- |
| Agent-native     | Всё управляется AI agents         |
| Metadata-driven  | Metadata — главный источник truth |
| Event-driven     | Реакция на события realtime       |
| Self-healing     | Автофикс pipeline failures        |
| Multi-engine     | Spark/Flink/dbt/Snowflake/etc     |
| Intent-based     | Пользователь пишет intent         |
| Autonomous       | Agents принимают решения          |
| Observable       | Полная observability              |
| Governance-first | Security + lineage встроены       |

---

# 2. Главная концепция — Data Agents

Вместо “tooling platform”:

```txt id="6d92gb"
Human → Supervisor Agent → Specialized Agents → Data Platform
```

---

# 3. Архитектура AI Agentic Data Platform

## Layer 1 — User Intent Layer

Пользователь пишет:

```txt id="dzaxd8"
"Create CDC pipeline from PostgreSQL to Iceberg"
```

или:

```txt id="5fkhxp"
"Почему revenue dashboard сломался?"
```

---

## Layer 2 — Orchestrator Agent

Главный Supervisor Agent:

* decomposes tasks
* delegates work
* coordinates agents
* validates outputs
* manages memory/context

Технологии:

* LangGraph
* Temporal
* AutoGen
* CrewAI

Полезные ссылки:

* [LangGraph](https://www.langchain.com/langgraph?utm_source=chatgpt.com)
* [Temporal](https://temporal.io?utm_source=chatgpt.com)
* [CrewAI](https://www.crewai.com?utm_source=chatgpt.com)

---

# 4. Specialized Agents

## Ingestion Agent

Создает ingestion pipelines.

Поддержка:

* Kafka
* CDC
* REST
* S3
* Databases

---

## Data Quality Agent

Автоматически:

* detects anomalies
* creates expectations
* fixes broken contracts

Технологии:

* Great Expectations
* Soda

---

## Optimization Agent

Оптимизирует:

* Spark
* Snowflake
* BigQuery
* ClickHouse

Автоматически:

* partitioning
* clustering
* query rewrite
* compute scaling

---

## Root Cause Analysis Agent

Когда pipeline падает:

* анализ lineage
* logs
* schema drift
* infra events
* upstream dependencies

И предлагает auto-remediation.

---

## Governance Agent

Автоматически:

* PII detection
* RBAC
* GDPR
* policy enforcement

---

## Cost Agent

Realtime cost intelligence:

* runaway queries
* warehouse inefficiency
* small file explosion
* idle clusters

---

# 5. Metadata Layer — САМОЕ ВАЖНОЕ

Большинство data platforms проваливаются потому что metadata — вторична.

В AI-native platform metadata = brain.

Нужны:

* lineage
* schemas
* ownership
* SLAs
* query history
* quality history
* incidents
* contracts
* embeddings

Технологии:

* DataHub
* OpenMetadata
* OpenLineage

Сайты:

* [DataHub](https://datahubproject.io?utm_source=chatgpt.com)
* [OpenMetadata](https://open-metadata.org?utm_source=chatgpt.com)
* [OpenLineage](https://openlineage.io?utm_source=chatgpt.com)

---

# 6. Knowledge Graph + Semantic Layer

Следующий большой шаг.

Платформа должна понимать:

* что такое revenue
* что такое customer
* какие datasets связаны

То есть:
не просто tables,
а semantic understanding.

Технологии:

* graph DB
* embeddings
* vector search
* ontology engine

---

# 7. AI Memory Architecture

Без memory agent platform не работает.

Нужны:

| Memory Type | Purpose               |
| ----------- | --------------------- |
| Short-term  | current task          |
| Episodic    | прошлые incidents     |
| Semantic    | архитектурные знания  |
| Vector      | similarity retrieval  |
| Operational | query/runtime history |

Технологии:

* Qdrant
* Weaviate
* Neo4j

---

# 8. Execution Layer

Нужен abstraction над compute engines.

Под капотом:

* Spark
* Flink
* dbt
* Snowflake
* BigQuery
* DuckDB
* ClickHouse

Но для agents это:

```txt id="7b74t5"
run_transformation()
optimize_query()
backfill_data()
```

---

# 9. Event-Driven Platform

Agents должны реагировать на:

* failed DAG
* schema drift
* SLA miss
* cost spike
* freshness anomaly

Через:

* Kafka
* Pulsar
* Temporal events

---

# 10. Self-Healing Pipelines

Самая инновационная часть.

Pipeline упал →
agent:

1. анализирует проблему
2. ищет root cause
3. генерирует fix
4. запускает tests
5. deploys patch
6. делает postmortem

Это уже AI SRE для data engineering.

---

# 11. Data Contracts Layer

Будущее DE — contracts-first.

Нужны:

* schema contracts
* freshness contracts
* SLA contracts
* semantic contracts

Технологии:

* OpenAPI
* Apache Avro
* Protocol Buffers

---

# 12. AI-Native Developer Experience

Вместо UI:

* conversational platform
* natural language workflows
* auto-generated pipelines

Пример:

```txt id="vmg46w"
"Create bronze/silver/gold medallion pipeline for Shopify orders"
```

И платформа сама:

* создаёт ingestion
* dbt models
* tests
* observability
* alerts

---

# 13. Multi-Agent Collaboration

Будущее — agent swarms.

Например:

* Planner Agent
* Spark Agent
* Governance Agent
* Cost Agent
* Reliability Agent

Работают параллельно.

---

# 14. Ключевые технологии для MVP

Я бы строил MVP так:

## Core Stack

### Orchestration

* Temporal
* LangGraph

### Metadata

* DataHub

### Storage

* Iceberg + S3

### Compute

* Spark + dbt

### Streaming

* Kafka

### AI Layer

* OpenAI / Claude
* MCP
* Tool calling

### Memory

* Qdrant

### Observability

* OpenLineage

---

# 15. Самая большая ошибка

Большинство делают:

```txt id="3qj5pj"
LLM + chat + SQL generation
```

Но это НЕ agentic platform.

Настоящая AI data platform:

* autonomous
* stateful
* event-driven
* self-improving
* metadata-native
* operationally aware

---

# 16. Что будет killer feature

Если делать стартап —
вот killer features:

## Autonomous RCA

AI сам расследует incidents.

## Self-healing data pipelines

Автофикс production.

## AI Query Optimizer

Автоматическая оптимизация warehouse cost.

## Semantic Data Brain

AI понимает business semantics.

## Natural Language Platform Engineering

“Создай CDC pipeline”.

---

# 17. Что сейчас реально отсутствует на рынке

Никто пока нормально не решил:

* AI-native metadata graph
* autonomous optimization
* self-healing pipelines
* agent memory for DE
* semantic governance
* AI platform operations

Именно здесь огромная возможность.

---

# 18. Если делать platform today — roadmap

## Phase 1

AI copilots

## Phase 2

Agent workflows

## Phase 3

Autonomous operations

## Phase 4

Self-healing platform

## Phase 5

Fully autonomous data engineering organization

---

# 19. Что обязательно изучить

## Agent Systems

* [LangGraph Docs](https://docs.langchain.com/langgraph-platform/index?utm_source=chatgpt.com)
* [Anthropic MCP](https://modelcontextprotocol.io?utm_source=chatgpt.com)

## Metadata

* [DataHub Docs](https://docs.datahub.com?utm_source=chatgpt.com)

## Lakehouse

* [Apache Iceberg](https://iceberg.apache.org?utm_source=chatgpt.com)

## Workflow Engines

* [Temporal Docs](https://docs.temporal.io?utm_source=chatgpt.com)

## Streaming

* [Kafka Docs](https://kafka.apache.org/documentation/?utm_source=chatgpt.com)

---

# 20. Как бы я позиционировал это

Не:

```txt id="2v0vab"
Modern Data Platform
```

А:

```txt id="wj4hcl"
Autonomous Data Engineering Platform
```

или:

```txt id="j8fcsh"
AI Operating System for Data Engineering
```
