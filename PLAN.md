# DE Agent Skills — Plan

Дорожная карта разработки скиллов для AI-агента по Data Engineering.  
Основан на `prepare_plan.md`. Статусы обновляются по мере реализации.

## Статусы

| Статус | Значение |
|--------|----------|
| ✅ Done | Скилл реализован в `skills/` |
| 🔲 Todo | В очереди |
| ⭐ Priority | Высокий приоритет, взять следующим |
| 💤 Low | Низкий приоритет (облачные сервисы, недоступные из РФ, или узкая аудитория) |

---

## 1. Orchestration & Workflow Management

| Статус | Скилл | Файл | Описание |
|--------|-------|------|----------|
| ✅ Done | `airflow_dags` | [skills/airflow_dags/SKILL.md](skills/airflow_dags/SKILL.md) | DAG authoring, TaskFlow API, операторы, сенсоры, TaskGroups, dynamic mapping, XComs, callbacks, пайплайны |
| ✅ Done | `airflow_dag_factory` | [skills/airflow_dag_factory/SKILL.md](skills/airflow_dag_factory/SKILL.md) | YAML-based DAG Factory v1.0+, декларативная генерация DAGов |
| ✅ Done | `dagster-assets` | [skills/dagster_assets/SKILL.md](skills/dagster_assets/SKILL.md) | Software-Defined Assets, declarative pipelines, partitions, sensors, IO managers |
| ✅ Done | `prefect-workflows` | [skills/prefect_workflows/SKILL.md](skills/prefect_workflows/SKILL.md) | Event-driven flows, dynamic caching, deployments, workers |
| ✅ Done | `mage-ai-pipelines` | [skills/mage_ai/SKILL.md](skills/mage_ai/SKILL.md) | Hybrid SQL+Python pipelines, модульная архитектура |

---

## 2. Data Transformation & Modeling

| Статус | Скилл | Файл | Описание |
|--------|-------|------|----------|
| ✅ Done | `dbt_trino` | [skills/dbt_trino/SKILL.md](skills/dbt_trino/SKILL.md) | dbt + Trino/Starburst: профили, материализации, инкрементальные стратегии, CI/CD |
| ✅ Done | `kimball_data_modeling` | [skills/kimball_data_modeling/SKILL.md](skills/kimball_data_modeling/SKILL.md) | Dimensional modeling, fact/dim DDL, SCD 0-6, DML-паттерны |
| ✅ Done | `data_vault_2` | [skills/data_vault_2/SKILL.md](skills/data_vault_2/SKILL.md) | DV 2.0: Hub/Link/Satellite, PIT, Bridge, Business Vault, пайплайны |
| ✅ Done | `medallion_architecture` | [skills/medallion_architecture/SKILL.md](skills/medallion_architecture/SKILL.md) | Bronze/Silver/Gold: DDL, DML, 7 стратегий дедупликации, watermark, CDC |
| ✅ Done | `dbt-core` | [skills/dbt_core/SKILL.md](skills/dbt_core/SKILL.md) | Общий dbt Core: seeds, snapshots, macros, hooks, packages, MetricFlow, мультиадаптерные проекты |
| ✅ Done | `dbt-macros` | [skills/dbt_macros/SKILL.md](skills/dbt_macros/SKILL.md) | Jinja fundamentals, macro authoring, adapter.dispatch, cross-database macros, run_query, built-in overrides |
| ✅ Done | `sqlmesh` | [skills/sqlmesh/SKILL.md](skills/sqlmesh/SKILL.md) | Incremental models, Virtual Environments, plan/apply workflow, state-aware deploys |

---

## 3. Big Data & Distributed Computing

| Статус | Скилл | Файл | Описание |
|--------|-------|------|----------|
| ✅ Done | `spark_sql` | [skills/spark_sql/SKILL.md](skills/spark_sql/SKILL.md) | Spark SQL для Hive/HDFS/Lakehouse: запросы, DDL, запись, оптимизация |
| ✅ Done | `pyspark_etl` | [skills/pyspark_etl/SKILL.md](skills/pyspark_etl/SKILL.md) | PySpark DataFrame пайплайны, трансформации, joins, производительность |
| ✅ Done | `pyspark_streaming` | [skills/pyspark_streaming/SKILL.md](skills/pyspark_streaming/SKILL.md) | Structured Streaming: Kafka/file sources, output modes, watermarks, windows, foreachBatch, checkpointing |
| ✅ Done | `apache_flink` | [skills/apache_flink/SKILL.md](skills/apache_flink/SKILL.md) | Stateful stream processing, event-time, windowing, Table API, Flink SQL, checkpoints, RocksDB |
| ✅ Done | `ray-data` | [skills/ray_data/SKILL.md](skills/ray_data/SKILL.md) | Distributed ML/Data workloads, Ray Datasets, remote functions, actors |

---

## 4. Modern Storage & Data Lakes

| Статус | Скилл | Файл | Описание |
|--------|-------|------|----------|
| ✅ Done | `trino_iceberg` | [skills/trino_iceberg/SKILL.md](skills/trino_iceberg/SKILL.md) | Trino + Iceberg: DDL, партиционирование, бакетирование, ALTER TABLE, DML, MERGE, оптимизация |
| ✅ Done | `delta_lake` | [skills/delta_lake/SKILL.md](skills/delta_lake/SKILL.md) | Delta: Z-Order, OPTIMIZE, VACUUM, Time Travel, DML, Change Data Feed, schema evolution, RESTORE, clone |
| ✅ Done | `clickhouse_olap` | [skills/clickhouse_olap/SKILL.md](skills/clickhouse_olap/SKILL.md) | ClickHouse: MergeTree-семейство, партиции, TTL, materialized views, проекции, оптимизация запросов |
| ✅ Done | `duckdb` | [skills/duckdb/SKILL.md](skills/duckdb/SKILL.md) | In-process analytics, Parquet/CSV/JSON/Iceberg/Delta, extensions, DuckDB SQL, Python API |
| ✅ Done | `postgresql-data-engineering` | [skills/postgresql_de/SKILL.md](skills/postgresql_de/SKILL.md) | Партиционирование, индексы (B-Tree/BRIN/GIN/GIST/partial/covering), COPY, EXPLAIN ANALYZE, autovacuum, JSONB, CTE, bulk load |
| 💤 Low | `snowflake` | — | Виртуальные склады, кластеризация, Snowpark, FinOps (недоступен из РФ напрямую) |

---

## 5. Data Ingestion & Streaming

| Статус | Скилл | Файл | Описание |
|--------|-------|------|----------|
| ✅ Done | `apache_kafka` | [skills/apache_kafka/SKILL.md](skills/apache_kafka/SKILL.md) | Топики, партиции, consumer groups, exactly-once, Schema Registry, Kafka Connect, DLQ, мониторинг lag |
| ✅ Done | `cdc-pipelines` | [skills/cdc_debezium/SKILL.md](skills/cdc_debezium/SKILL.md) | Debezium CDC: коннекторы PostgreSQL/MySQL, change event структура, outbox pattern, Iceberg sink |
| ✅ Done | `airbyte` | [skills/airbyte/SKILL.md](skills/airbyte/SKILL.md) | Sync modes, Python CDK (HttpStream/IncrementalMixin), Connector Builder, normalization, Airflow интеграция, Terraform |
| ✅ Done | `redpanda` | [skills/redpanda/SKILL.md](skills/redpanda/SKILL.md) | Kafka-совместимый стриминг без JVM, настройка кластера, тюнинг |
| 💤 Low | `meltano` | — | Declarative ELT с Singer taps/targets |

---

## 6. Data Quality, Governance & Observability

| Статус | Скилл | Файл | Описание |
|--------|-------|------|----------|
| ✅ Done | `great-expectations` | [skills/great_expectations/SKILL.md](skills/great_expectations/SKILL.md) | Expectation Suites, Checkpoints, Data Docs, интеграция с Airflow/dbt |
| ✅ Done | `openlineage` | [skills/openlineage/SKILL.md](skills/openlineage/SKILL.md) | Column-level lineage, интеграция с Airflow/Spark/dbt, OpenLineage Events, Marquez |
| ✅ Done | `soda-core` | [skills/soda_core/SKILL.md](skills/soda_core/SKILL.md) | SodaCL checks, CLI scans, интеграция с dbt/Airflow, алерты |
| ✅ Done | `data-contracts` | [skills/data_contracts/SKILL.md](skills/data_contracts/SKILL.md) | YAML spec, Data Contract CLI, SodaCL quality checks, breaking change detection, CI/CD |
| ✅ Done | `datahub-catalog` | [skills/datahub_catalog/SKILL.md](skills/datahub_catalog/SKILL.md) | GMS архитектура, ingestion recipes, Python SDK lineage, column-level lineage, GraphQL search |
| 💤 Low | `amundsen` | — | Data discovery catalog (уступает DataHub по активности сообщества) |

---

## 7. Infrastructure as Code & DataOps

| Статус | Скилл | Файл | Описание |
|--------|-------|------|----------|
| ✅ Done | `kubernetes-data-platform` | [skills/kubernetes_data/SKILL.md](skills/kubernetes_data/SKILL.md) | Spark-on-K8s, Airflow Helm chart, KubernetesExecutor, KubernetesPodOperator, resource quotas |
| ✅ Done | `github-actions-dataops` | [skills/github_actions_dataops/SKILL.md](skills/github_actions_dataops/SKILL.md) | dbt slim CI, SQLFluff PR annotations, DAG integrity tests, Docker builds, OIDC, reusable workflows |
| ✅ Done | `docker-data-environments` | [skills/docker_data_envs/SKILL.md](skills/docker_data_envs/SKILL.md) | Multi-stage Dockerfiles для data сервисов, layer caching, private registry |
| ✅ Done | `terraform-data-infrastructure` | [skills/terraform_data/SKILL.md](skills/terraform_data/SKILL.md) | S3/MinIO, IAM, Kafka, K8s cluster через IaC |
| ✅ Done | `sqlfluff` | [skills/sqlfluff/SKILL.md](skills/sqlfluff/SKILL.md) | SQL linting, диалекты, конфигурация `.sqlfluff`, авто-фикс, CI |

---

## 8. ML/DataOps (AI-Adjacent)

| Статус | Скилл | Файл | Описание |
|--------|-------|------|----------|
| ✅ Done | `feature-store` | [skills/feature_store/SKILL.md](skills/feature_store/SKILL.md) | Feast: online/offline store, point-in-time correctness, feature views |
| ✅ Done | `mlflow-data-pipelines` | [skills/mlflow_pipelines/SKILL.md](skills/mlflow_pipelines/SKILL.md) | Experiment tracking, model registry, MLflow Projects для DE |
| ✅ Done | `rag-data-pipeline` | [skills/rag_pipeline/SKILL.md](skills/rag_pipeline/SKILL.md) | Chunking, embedding refresh, vector store loading, hybrid retrieval |
| 💤 Low | `ml-data-pipeline-review` | — | Training-serving skew, dataset versioning, TFX |

---

## 9. AI-Agentic & Meta Skills

| Статус | Скилл | Файл | Описание |
|--------|-------|------|----------|
| ✅ Done | `de-root-cause-analysis` | [skills/de_rca/SKILL.md](skills/de_rca/SKILL.md) | RCA для pipeline failures: lineage tracing, upstream/downstream анализ, logs |
| ✅ Done | `de-production-readiness` | [skills/de_production_readiness/SKILL.md](skills/de_production_readiness/SKILL.md) | Production readiness review: retries, idempotency, alerting, SLAs, observability |
| ✅ Done | `de-architecture-decision` | [skills/de_adr/SKILL.md](skills/de_adr/SKILL.md) | ADR generation: template, trade-offs, context, alternatives |
| ✅ Done | `de-cost-optimization` | [skills/de_cost_optimization/SKILL.md](skills/de_cost_optimization/SKILL.md) | Query cost analysis, warehouse sizing, storage tier recommendations |
| ✅ Done | `de-postmortem-writer` | [skills/de_postmortem/SKILL.md](skills/de_postmortem/SKILL.md) | Blameless postmortem генератор, CAPA |

---

## Имеющиеся Specs

| Spec | Тема |
|------|------|
| `vertica_query_optimization_v11.md` | Vertica 11.x query optimization |
| `vertica_admin_guide_v24.md` | Vertica 24.3.x administration |
| `trino_iceberg_performance_optimization.md` | Trino + Iceberg performance |
| `hdfs_hive_parquet_datalake.md` | HDFS/Hive/Parquet lakehouse |
| `pyspark_enterprise.md` | PySpark enterprise patterns |
| `spark_sql_enterprise.md` | Spark SQL enterprise |
| `spark_sql_hdfs_hive_operations.md` | Spark SQL + HDFS/Hive ops |

---

## Рекомендуемая очерёдность

### ✅ Выполнено (батч 1, скиллы 1–6)

| # | Скилл | Статус |
|---|-------|--------|
| 1 | `apache_kafka` | ✅ Done |
| 2 | `pyspark_streaming` | ✅ Done |
| 3 | `delta_lake` | ✅ Done |
| 4 | `apache_flink` | ✅ Done |
| 5 | `great_expectations` | ✅ Done |
| 6 | `clickhouse_olap` | ✅ Done |

### ✅ Выполнено (батч 2, скиллы 7–10)

| # | Скилл | Статус |
|---|-------|--------|
| 7 | `dbt-core` | ✅ Done |
| 8 | `cdc-pipelines` | ✅ Done |
| 9 | `openlineage` | ✅ Done |
| 10 | `kubernetes-data-platform` | ✅ Done |

### ✅ Выполнено (батч 3, скиллы 11–15)

| # | Скилл | Статус |
|---|-------|--------|
| 11 | `dbt-macros` | ✅ Done |
| 12 | `dagster-assets` | ✅ Done |
| 13 | `sqlmesh` | ✅ Done |
| 14 | `soda-core` | ✅ Done |
| 15 | `duckdb` | ✅ Done |

### ✅ Выполнено (батч 4, скиллы 16–20)

| # | Скилл | Статус |
|---|-------|--------|
| 16 | `postgresql-data-engineering` | ✅ Done |
| 17 | `github-actions-dataops` | ✅ Done |
| 18 | `data-contracts` | ✅ Done |
| 19 | `datahub-catalog` | ✅ Done |
| 20 | `airbyte` | ✅ Done |

### ✅ Выполнено (батч 5, скиллы 21–25)

| # | Скилл | Статус |
|---|-------|--------|
| 21 | `docker-data-environments` | ✅ Done |
| 22 | `terraform-data-infrastructure` | ✅ Done |
| 23 | `mlflow-data-pipelines` | ✅ Done |
| 24 | `rag-data-pipeline` | ✅ Done |
| 25 | `de-production-readiness` | ✅ Done |

### ✅ Выполнено (батч 6, скиллы 26–30)

| # | Скилл | Статус |
|---|-------|--------|
| 26 | `prefect-workflows` | ✅ Done |
| 27 | `redpanda` | ✅ Done |
| 28 | `ray-data` | ✅ Done |
| 29 | `de-root-cause-analysis` | ✅ Done |
| 30 | `de-architecture-decision` | ✅ Done |

### ✅ Выполнено (батч 7, скиллы 31–35)

| # | Скилл | Статус |
|---|-------|--------|
| 31 | `sqlfluff` | ✅ Done |
| 32 | `feature-store` | ✅ Done |
| 33 | `de-cost-optimization` | ✅ Done |
| 34 | `de-postmortem-writer` | ✅ Done |
| 35 | `mage-ai-pipelines` | ✅ Done |

---

## StarRocks Group Skills (`group_skills/starrocks_group_skills/`)

42 skills total, organized into 8 groups. All ✅ Done.

### Group 1 — Admin & Operations (5)

| Скилл | Файл |
|-------|------|
| `starrocks-admin-cluster-health` | starrocks_admin_cluster_health/SKILL.md |
| `starrocks-admin-compaction` | starrocks_admin_compaction/SKILL.md |
| `starrocks-admin-query-monitor` | starrocks_admin_query_monitor/SKILL.md |
| `starrocks-admin-security` | starrocks_admin_security/SKILL.md |
| `starrocks-admin-backup-restore` | starrocks_admin_backup_restore/SKILL.md |

### Group 2 — DDL & Table Design (5)

| Скилл | Файл |
|-------|------|
| `starrocks-ddl-table-types` | starrocks_ddl_table_types/SKILL.md |
| `starrocks-partitioning` | starrocks_partitioning/SKILL.md |
| `starrocks-bucketing` | starrocks_bucketing/SKILL.md |
| `starrocks-materialized-views` | starrocks_materialized_views/SKILL.md |
| `starrocks-data-modeling` | starrocks_data_modeling/SKILL.md |

### Group 3 — Query Optimization (9)

| Скилл | Файл |
|-------|------|
| `starrocks-admin-storage-balancer` | starrocks_admin_storage_balancer/SKILL.md |
| `starrocks-schema-evolution` | starrocks_schema_evolution/SKILL.md |
| `starrocks-realtime-modeling` | starrocks_realtime_modeling/SKILL.md |
| `starrocks-query-optimizer` | starrocks_query_optimizer/SKILL.md |
| `starrocks-explain-plan` | starrocks_explain_plan/SKILL.md |
| `starrocks-join-optimization` | starrocks_join_optimization/SKILL.md |
| `starrocks-aggregation-optimizer` | starrocks_aggregation_optimizer/SKILL.md |
| `starrocks-memory-tuning` | starrocks_memory_tuning/SKILL.md |
| `starrocks-concurrency-optimizer` | starrocks_concurrency_optimizer/SKILL.md |

### Group 4 — Ingestion (6)

| Скилл | Файл |
|-------|------|
| `starrocks-cbo` | starrocks_cbo/SKILL.md |
| `starrocks-stream-load` | starrocks_stream_load/SKILL.md |
| `starrocks-routine-load-kafka` | starrocks_routine_load_kafka/SKILL.md |
| `starrocks-broker-load` | starrocks_broker_load/SKILL.md |
| `starrocks-files-ingestion` | starrocks_files_ingestion/SKILL.md |
| `starrocks-cdc-pipeline` | starrocks_cdc_pipeline/SKILL.md |

### Group 5 — Airflow + StarRocks (5)

| Скилл | Файл |
|-------|------|
| `airflow-starrocks-pipeline` | airflow_starrocks_pipeline/SKILL.md |
| `airflow-starrocks-etl-best-practices` | airflow_starrocks_etl_best_practices/SKILL.md |
| `airflow-starrocks-cdc-orchestrator` | airflow_starrocks_cdc_orchestrator/SKILL.md |
| `airflow-starrocks-data-quality` | airflow_starrocks_data_quality/SKILL.md |
| `airflow-starrocks-backfill` | airflow_starrocks_backfill/SKILL.md |

### Group 6 — StarRocks Pipeline (4)

| Скилл | Файл |
|-------|------|
| `starrocks-medallion-architecture` | starrocks_medallion_architecture/SKILL.md |
| `starrocks-realtime-analytics` | starrocks_realtime_analytics/SKILL.md |
| `starrocks-lakehouse-integration` | starrocks_lakehouse_integration/SKILL.md |
| `starrocks-data-quality-guardian` | starrocks_data_quality_guardian/SKILL.md |

### Group 7 — dbt + StarRocks (5)

| Скилл | Файл |
|-------|------|
| `dbt-starrocks-models` | dbt_starrocks_models/SKILL.md |
| `dbt-starrocks-performance` | dbt_starrocks_performance/SKILL.md |
| `dbt-starrocks-testing` | dbt_starrocks_testing/SKILL.md |
| `dbt-starrocks-semantic-layer` | dbt_starrocks_semantic_layer/SKILL.md |
| `dbt-starrocks-production-readiness` | dbt_starrocks_production_readiness/SKILL.md |

### Group 8 — AI-Agent Skills (3)

| Скилл | Файл |
|-------|------|
| `starrocks-ai-query-autotuner` | starrocks_ai_query_autotuner/SKILL.md |
| `starrocks-ai-incident-rca` | starrocks_ai_incident_rca/SKILL.md |
| `starrocks-self-healing` | starrocks_self_healing/SKILL.md |
