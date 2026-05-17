# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Roadmap

See [PLAN.md](PLAN.md) for the full skills roadmap: which skills are done (✅), which are next (⭐ Priority), and the recommended implementation order.

## What This Repository Is

A collection of **Claude Code agent skills** for data engineering topics. Skills are loaded by the Claude Agent SDK harness at runtime to give Claude specialized domain knowledge and behavioral rules for a specific technology or task. There is no build system, test runner, or application code — the repository is purely documentation-as-skills.

## Repository Layout

```
skills/<name>/SKILL.md      — skill definitions consumed by the agent harness
docs/specs/<name>.md        — detailed enterprise specs referenced by skills
guides/<name>.md            — tutorial-style narrative guides (Russian, for humans)
```

Skills, specs, and guides are related but serve different audiences:
- `skills/` — concise, prescriptive instruction files loaded directly into the agent context.
- `docs/specs/` — verbose enterprise specifications with deep rationale; skills cross-reference these.
- `guides/` — human-readable tutorials written in Russian that synthesize the specs.

## Skill File Format

Every skill is a single `SKILL.md` with YAML frontmatter:

```markdown
---
name: <kebab-case-slug>
description: <one-sentence trigger description used by the harness to decide when to load this skill>
---

# <Title>

## When to Use
...
```

The `description` field is the most critical part — the harness uses it to match user intent to the right skill. Keep it specific and keyword-rich so it activates on the right requests and not on unrelated ones.

A skill must be self-contained: it cannot assume other skills are loaded simultaneously. Cross-references to `docs/specs/` are allowed in a "References to Consult When Needed" section at the bottom.

## Adding a New Skill

1. Create `skills/<name>/SKILL.md` — follow the structure of an existing skill such as `skills/spark_sql/SKILL.md`.
2. Include these sections in order: **When to Use**, **Core Workflow** (or equivalent), technology-specific content sections, **Anti-Patterns**, **Output Expectations**, optionally **References to Consult When Needed**.
3. Write in English.
4. Make examples production-quality — not toy snippets.

## Existing Skills

| Skill | Topic |
|-------|-------|
| `spark_sql` | Spark SQL for Hive/HDFS/lakehouse — queries, DDL, writes, performance |
| `pyspark_etl` | PySpark DataFrame pipelines — transforms, joins, writes, Spark performance |
| `vertica` | Vertica SQL — DDL, DML, CRUD, projections, segmentation, update strategies |
| `vertica_query_optimization` | Vertica 11.x query performance — EXPLAIN, projection design, join/GROUP BY/ORDER BY tuning, encoding, RLE, Data Collector diagnostics |
| `airflow_dag_factory` | Airflow DAG Factory (dag-factory v1.0+) — YAML DAG authoring, defaults hierarchy, dynamic mapping, datasets, callbacks, TaskFlow, env vars, DRY anchors, large-scale generation, CI/CD linting |
| `trino_iceberg` | Trino + Apache Iceberg — table DDL, partition transforms, sorted tables, DML/MERGE, EXPLAIN plan reading, join optimization, ANALYZE, table maintenance (optimize/expire_snapshots/remove_orphan_files), schema evolution, time travel, metadata table diagnostics |
| `dbt_trino` | dbt + Trino/Starburst — profiles.yml, all auth methods, materializations (table/view/incremental/materialized_view/ephemeral), incremental strategies (append/merge/delete+insert), Iceberg table properties, on_schema_change, seeds, snapshots, grants, data modeling (staging/intermediate/mart), CI/CD |
| `kimball_data_modeling` | Kimball dimensional modeling — fact table types (transaction/snapshot/accumulating), dimension design, SCD types 0/1/2/3/4/6, surrogate keys, conformed/role-playing/junk/degenerate dimensions, bridge tables, date dimension DDL, fact/dim DDL, DML load patterns, late-arriving data, best practices |
| `data_vault_2` | Data Vault 2.0 — Hubs, Links, Satellites, hash keys, hash diff, staging layer, Multi-Active/Effectivity/Computed satellites, Reference tables, Same-As Links, PIT tables, Bridge tables, Business Vault, Information Mart construction, insert-only DML patterns, pipeline sequencing (Airflow + dbt + automate-dv) |
| `medallion_architecture` | Medallion (Bronze/Silver/Gold) — layer design, DDL per layer, DML load/upsert patterns, 7 deduplication strategies (ROW_NUMBER/MERGE/hash/watermark/GROUP BY/CDC/surrogate check), schema evolution, DQ gates, partitioning per layer, watermark pipelines, CDC micro-batch, Airflow DAG, dbt project structure |
| `airflow_dags` | Apache Airflow DAG authoring — DAG definition (3 styles), TaskFlow API (@task/@dag), operators (Bash/Python/SQL/HTTP), sensors (poke/reschedule), TaskGroups (nested/dynamic), dynamic task mapping (expand/partial/map/zip), branching, trigger rules, XComs, Pools, callbacks, cross-DAG pipelines (TriggerDagRunOperator/ExternalTaskSensor/Dataset), Jinja templates, best practices |
| `apache_kafka` | Apache Kafka — topics/partitions/consumer groups, producer config (acks/idempotence/compression), consumer commit strategies, exactly-once semantics, Schema Registry (Avro), Kafka Connect (source/sink, SMTs, DLQ), consumer lag monitoring, CLI operations, Python confluent-kafka, Docker Compose |
| `pyspark_streaming` | PySpark Structured Streaming — Kafka/file/rate sources, output modes (append/complete/update), triggers (ProcessingTime/AvailableNow), watermarks, event-time windows (tumbling/sliding/session), stateful deduplication, foreachBatch, Delta/Iceberg sinks, stream-stream joins, RocksDB state store |
| `apache_flink` | Apache Flink — Table API + SQL (Kafka/filesystem/Iceberg DDL, windowing TUMBLE/HOP/SESSION/CUMULATE), DataStream API (KeyedStream, stateful ProcessFunction, ValueState/MapState), event-time + watermarks, checkpointing (EXACTLY_ONCE, RocksDB), Kafka source/sink (exactly-once), savepoints, deployment (standalone/K8s) |
| `delta_lake` | Delta Lake — DDL (CREATE/PARTITIONED BY/generated columns), DML (INSERT/UPDATE/DELETE/MERGE), upsert/SCD2/CDC MERGE patterns, schema evolution (mergeSchema/ALTER TABLE/columnMapping), OPTIMIZE/Z-ORDER BY, VACUUM, Time Travel (VERSION/TIMESTAMP AS OF), RESTORE, shallow/deep clone, Change Data Feed, streaming read/write |
| `clickhouse_olap` | ClickHouse OLAP — MergeTree family (MergeTree/ReplacingMergeTree/AggregatingMergeTree/SummingMergeTree/CollapsingMergeTree), ORDER BY/PARTITION BY design, data skipping indexes (minmax/set/bloom_filter), TTL tiered storage, materialized views (-State/-Merge), projections, LowCardinality, INSERT batching, FINAL vs argMax, Kafka engine, Python clickhouse-connect |
| `great_expectations` | Great Expectations — DataContext (file/ephemeral), Data Sources (Pandas/Spark/SQL), Expectation Suites, built-in expectations (null/uniqueness/range/set/regex/table-level), Validation Definitions, Checkpoints with actions (Data Docs/Slack), Airflow integration, custom expectations, severity levels |
| `dbt_core` | dbt Core (multi-adapter) — project structure, profiles.yml (PostgreSQL/Spark/ClickHouse), sources + refs, all 5 materializations, incremental strategies, is_incremental(), on_schema_change, SCD2 snapshots, seeds, generic + singular tests, Jinja macros, dbt-utils/dbt-expectations packages, node selection (graph operators), slim CI with state:modified+ + --defer |
| `cdc_debezium` | Debezium CDC — PostgreSQL connector (pgoutput, replication slot, publication), MySQL connector (binlog), change event structure (before/after/op/source), snapshot modes, SMT ExtractNewRecordState, Outbox pattern with EventRouter, Kafka Connect REST API, Flink CDC integration, Iceberg sink, replication lag monitoring |
| `openlineage` | OpenLineage — RunEvent spec (Job/Run/Dataset entities, facets), Marquez backend setup, Airflow provider config, Spark listener, dbt emission, column-level lineage (ColumnLineageDatasetFacet), custom Python emitter, namespace conventions, impact analysis via lineage graph API |
| `kubernetes_data` | Kubernetes data platform — Spark-on-K8s (spark-submit cluster mode, pod templates, dynamic allocation, RBAC), Airflow Helm chart (KubernetesExecutor, git-sync, values.yaml), KubernetesPodOperator, resource quotas, LimitRange, Secrets management, Spark History Server, monitoring |
| `dbt_macros` | dbt Jinja macros — macro syntax, context variables (this/target/adapter/execute/model), run_query with execute guard, adapter.dispatch cross-database macros, dbt.* built-ins (date_trunc/dateadd/type_*), generate_schema_name override, hooks, macro documentation |
| `dagster_assets` | Dagster Software-Defined Assets — @asset/@multi_asset decorators, asset dependencies, DailyPartitionsDefinition/StaticPartitionsDefinition/DynamicPartitionsDefinition, custom IO managers (S3 Parquet), sensors (@asset_sensor, cursor idempotency), declarative automation (AutomationCondition.eager/on_cron/on_missing), jobs, testing with materialize() |
| `sqlmesh` | SQLMesh — model kinds (FULL/VIEW/INCREMENTAL_BY_TIME_RANGE/INCREMENTAL_BY_UNIQUE_KEY/SCD_TYPE_2/SEED), MODEL() DDL properties, @start_ds/@end_ds macros, plan/apply workflow, virtual environments, breaking vs non-breaking changes, audits (NOT_NULL/UNIQUE_VALUES/custom), unit tests, CI/CD, dbt migration |
| `soda_core` | Soda Core data quality — SodaCL checks (row_count, missing_count/percent, duplicate_count, invalid, freshness, schema, reference, custom SQL metric), configuration.yml for PostgreSQL/Trino/ClickHouse/BigQuery, soda scan CLI, Python programmatic scan, Airflow gate integration, dbt integration, warn vs fail thresholds |
| `duckdb` | DuckDB OLAP — read_parquet/read_csv/read_json/iceberg_scan/delta_scan (local + S3), SQL (window functions, QUALIFY, PIVOT/UNPIVOT, ASOF join, SAMPLE), Python API (duckdb.connect, fetchdf, register, Arrow UDFs), extensions (httpfs/iceberg/delta/postgres), COPY TO (partitioned Parquet), performance tuning (memory_limit/threads/EXPLAIN ANALYZE) |
| `postgresql_de` | PostgreSQL for data engineering — declarative partitioning (RANGE/LIST/HASH/pg_partman), indexes (B-Tree/BRIN/GIN/GIST/partial/covering/INCLUDE), COPY bulk load, EXPLAIN ANALYZE plan reading, autovacuum tuning, window functions, JSONB, CTEs, LATERAL joins, bulk-load patterns (UNLOGGED/pg_bulkload) |
| `airbyte` | Airbyte ELT — source/destination connectors, sync modes (Full Refresh Overwrite/Append, Incremental Append/Deduped), cursor fields, primary keys, deployment (abctl, Kubernetes Helm, Airbyte Cloud), Connector Builder, Python CDK (HttpStream, IncrementalMixin), normalization (dbt-based, _airbyte_raw_ tables), Airbyte API, Terraform provider, schema evolution, Airflow AirbyteTriggerSyncOperator |
| `github_actions_dataops` | GitHub Actions DataOps CI/CD — dbt slim CI (state:modified+, --defer, manifest.json S3 artifact), SQLFluff lint with PR annotations, Airflow DAG integrity tests (DagBag, cycle detection), Docker multi-stage builds (ghcr.io, cache-from), OIDC for AWS/GCP (no static keys), reusable workflows, composite actions |
| `data_contracts` | Data Contracts — datacontract.yaml spec (schema, quality, SLA, servers, changelog), Data Contract CLI (init/lint/test/diff/breaking/export/publish), SodaCL embedded quality checks, breaking change detection, CI/CD GitHub Actions workflow, versioning (semver), DataHub/OpenMetadata integration |
| `datahub_catalog` | DataHub data catalog — GMS architecture, Docker Compose + Kubernetes Helm deployment, ingestion recipes (PostgreSQL/Hive/Spark/dbt/Airflow/Kafka/S3), Python SDK (DatahubRestEmitter, dataset lineage), column-level lineage (FineGrainedLineage), GraphQL search, CLI (ingest/delete/timeline), transformers |
| `docker_data_envs` | Docker for data engineering environments — multi-stage Dockerfiles (dbt/Spark/Airflow), BuildKit layer caching (--mount=type=cache), private registries (ghcr.io/Harbor), docker buildx multi-platform builds (amd64/arm64), Docker Compose local data stacks (Spark+Airflow+Kafka+MinIO+Postgres+Schema Registry), BuildKit secrets for private PyPI, security hardening (non-root user, slim base images, read-only FS, .dockerignore), GitHub Actions CI/CD matrix builds |
| `mlflow_pipelines` | MLflow for data engineering — tracking server (PostgreSQL backend + S3 artifacts, Docker Compose), experiment tracking (params/metrics/tags/artifacts/autolog), ETL job metadata logging (row counts, DQ metrics, lineage tags), nested runs for hyperparameter sweeps, Model Registry (register/alias/promote, stages deprecated → aliases), model serving (pyfunc REST API/batch scoring/Spark UDF), MLproject files (conda/docker envs, entry points), Airflow integration (@task XCom run_id, model promotion gate), Spark integration (mlflow.spark.autolog/log_model/Delta metadata) |
| `terraform_data` | Terraform for data infrastructure — project layout (modules/envs/Terragrunt), S3 buckets (SSE-KMS/lifecycle/policy), MinIO provider, IAM instance profiles + IRSA for EKS, aws_msk_cluster + configuration, Kubernetes Helm releases (Airflow/Spark History Server), namespace + resource quotas, typed variables with validation, GitHub Actions OIDC workflow + drift detection, Terragrunt DRY config |
| `rag_pipeline` | RAG data pipeline — chunking strategies (fixed/semantic/recursive/sentence-window), embedding models (OpenAI/Cohere/BGE), vector stores (pgvector HNSW/Chroma/Qdrant/Weaviate), SHA-256 hash incremental refresh with Airflow expand(), hybrid retrieval (BM25+dense+RRF fusion), CrossEncoder reranking, metadata filtering, retrieval quality metrics, A/B testing by user_id hash |
| `de_production_readiness` | DE production readiness — idempotency patterns (INSERT ON CONFLICT/MERGE/dynamic partition overwrite/SHA-256 job IDs), retry_with_backoff with full jitter, Airflow retry config, SLA monitoring (sla_miss_callback/Prometheus alerts/PagerDuty), freshness SodaCL checks, structured logging (JsonFormatter), OpenTelemetry tracing, data reconciliation (row count+checksum+duplicate detection), CircuitBreaker, blue/green schema swap |
| `prefect_workflows` | Prefect 3.x workflows — @flow/@task decorators (retries/caching/timeout), task result caching (task_input_hash), submit()/map() parallelism, state hooks (on_failure/on_completion), pause_flow_run human-in-the-loop, deployments (prefect.yaml build/push/pull), work pool types (Process/Docker/Kubernetes/ECS), schedules (cron/interval/RRule), event-driven triggers, DockerContainer/KubernetesJob runners, ConcurrencyLimit, DaskTaskRunner/RayTaskRunner, artifacts, S3 result storage, Airflow migration table |
| `redpanda` | Redpanda Kafka-compatible streaming — Raft-per-partition architecture, Docker Compose (single-node + 3-broker), Kubernetes Helm chart (values.yaml with SASL/TLS/tiered storage), rpk CLI (topics/consumer groups/ACLs/tune), topic config (retention/compaction/tiered storage), producer/consumer tuning, Shadow Indexing (S3/GCS/ABS), Schema Registry (Avro/Protobuf), Kafka Connect compatibility, SASL/SCRAM + mTLS security, Prometheus monitoring, Python confluent-kafka/aiokafka, Kafka migration checklist |
| `ray_data` | Ray Data distributed processing — Dataset/Block/ObjectStore architecture, read_parquet/csv/json (S3/GCS), custom PostgresChunkDatasource, map/map_batches (pandas/pyarrow), filter/flat_map, groupby aggregations (custom AggregateFn), stateful Actor transforms (GPU batch inference with HuggingFace), @ray.remote + ray.get/put, DuckDB interop, iter_batches streaming, write_parquet with partition_cols, IcebergDatasink, KubeRay RayCluster/RayJob CRDs, Airflow @task + SubmitRayJob, performance tuning (override_num_blocks/prefetch_batches/fusion) |
| `de_rca` | DE root cause analysis — failure taxonomy (infrastructure/data/logic/dependency/configuration/concurrency), 5-step RCA framework, Airflow diagnosis (task state machine/metadata DB SQL/DagBag errors/sensor vs timeout), Spark diagnosis (executor vs driver OOM/skew detection+salting/FetchFailed/serialization errors), dbt diagnosis (run_results.json/incremental bugs/snapshot failures), DQ anomaly queries (volume/freshness/distribution shift/null explosion/duplicates), lineage impact via OpenLineage+DataHub, log analysis with jq+pandas, RCA document template |
| `de_adr` | DE architecture decision records — ADR Markdown template (context/decision/rationale/alternatives/consequences), weighted scoring matrix with 6 criteria (performance/ops complexity/cost/ecosystem/team fit/vendor risk), decision reversibility spectrum, 10 pre-filled ADRs (batch vs streaming/Iceberg vs Delta/Airflow vs Prefect vs Dagster/dbt vs SQLMesh/Trino vs ClickHouse/Debezium vs polling/Soda vs GE/DataHub vs OpenMetadata/Vault vs SSM/Iceberg migration), stakeholder one-pager template, ADR lifecycle management, adr-tools/log4brains tooling |

## Existing Specs

| Spec | Topic |
|------|-------|
| `vertica_query_optimization_v11.md` | Vertica 11.x query optimization — EXPLAIN tokens, projection design, join/GROUP BY/ORDER BY, encoding, dc_* tables |
| `vertica_admin_guide_v24.md` | Vertica 24.3.x administration — architecture, users/roles, projections, partitioning, COPY, resource pools, backup, monitoring |
| `trino_iceberg_performance_optimization.md` | Trino + Iceberg — optimizer, CBO, join strategies, pushdown, partitioning, sorted tables, maintenance, time travel, schema evolution |

## Docs/Specs Relationship

Skills reference specs when detailed rationale would bloat the skill file. For example, `skills/spark_sql/SKILL.md` references `docs/specs/spark_sql_hdfs_hive_operations.md` for HDFS/Hive DDL detail. When editing a skill, check whether the underlying spec needs updating too.
