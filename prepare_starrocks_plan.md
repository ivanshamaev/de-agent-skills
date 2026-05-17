Ниже — структурированный набор **AI Agent Skills для StarRocks Data Platform**.
Я разбил их по направлениям так, чтобы это можно было напрямую класть в твой `de-agent-skills` репозиторий.

Документация:

* [StarRocks Docs](https://docs.starrocks.io?utm_source=chatgpt.com)
* [StarRocks GitHub](https://github.com/StarRocks/starrocks?utm_source=chatgpt.com)
* [StarRocks Best Practices](https://docs.starrocks.io/docs/best_practices/?utm_source=chatgpt.com)

---

# StarRocks Administration Skills

## 1. `starrocks-admin-cluster-health`

Проверка состояния FE/BE/CN nodes.

### Проверяет

* FE quorum
* BE availability
* Tablet health
* Replica status
* Compaction backlog
* Storage imbalance

### Docs

* [Cluster Management](https://docs.starrocks.io/docs/administration/management/cluster_management/?utm_source=chatgpt.com)

---

## 2. `starrocks-admin-compaction-optimizer`

### Focus

* Base compaction
* Cumulative compaction
* Compaction score analysis
* Write amplification reduction

### Docs

* [Compaction Docs](https://docs.starrocks.io/docs/administration/management/compaction/?utm_source=chatgpt.com)

---

## 3. `starrocks-admin-storage-balancer`

### Focus

* Tablet distribution
* Disk skew
* Backend balancing
* Capacity forecasting

---

## 4. `starrocks-admin-query-monitor`

### Focus

* Slow queries
* Memory spills
* Queue analysis
* Resource groups

### Docs

* [Query Management](https://docs.starrocks.io/docs/administration/management/resource_management/?utm_source=chatgpt.com)

---

## 5. `starrocks-admin-backup-restore`

### Focus

* Snapshot strategy
* Cross-cluster restore
* Incremental backups
* Disaster recovery

---

## 6. `starrocks-admin-security-audit`

### Focus

* RBAC
* User privileges
* External auth
* Data masking

### Docs

* [Security Docs](https://docs.starrocks.io/docs/administration/user_privs/?utm_source=chatgpt.com)

---

# StarRocks DDL & Data Modeling Skills

## 7. `starrocks-ddl-table-designer`

### Generates

* Primary Key tables
* Duplicate Key tables
* Aggregate Key tables
* Unique Key tables

### Docs

* [Table Types](https://docs.starrocks.io/docs/table_design/table_types/?utm_source=chatgpt.com)

---

## 8. `starrocks-partitioning-strategy`

### Focus

* Range partitioning
* Dynamic partitions
* Partition pruning
* Hot/cold strategy

### Best Practices

* Time-based partitioning
* Avoid tiny partitions
* Partition lifecycle automation

---

## 9. `starrocks-bucketing-optimizer`

### Focus

* Bucket count selection
* Distribution keys
* Skew prevention

---

## 10. `starrocks-materialized-view-advisor`

### Focus

* Async MVs
* Query rewrite
* Aggregation acceleration

### Docs

* [Materialized Views](https://docs.starrocks.io/docs/using_starrocks/async_mv/Materialized_view/?utm_source=chatgpt.com)

---

## 11. `starrocks-schema-evolution-manager`

### Focus

* ALTER safety
* Column evolution
* Backward compatibility

---

## 12. `starrocks-data-model-review`

### Reviews

* Fact tables
* Dimensions
* Star schema
* Denormalization strategy

### Best Practices

* Avoid over-normalization
* Optimize for OLAP scans
* Use wide-table strategy carefully

---

## 13. `starrocks-wide-table-advisor`

### Focus

* Join elimination
* Flattening strategy
* BI acceleration

---

## 14. `starrocks-real-time-modeling`

### Focus

* Realtime aggregates
* Streaming upserts
* Mutable dimensions

---

# StarRocks Query Optimization Skills

## 15. `starrocks-query-optimizer`

### Focus

* Join reorder
* Predicate pushdown
* Vectorized execution
* Runtime filters

### Docs

* [Query Tuning](https://docs.starrocks.io/docs/best_practices/query_tuning/?utm_source=chatgpt.com)

---

## 16. `starrocks-explain-plan-analyzer`

### Focus

* EXPLAIN analysis
* Pipeline engine
* Exchange nodes
* Scan optimization

---

## 17. `starrocks-join-optimization`

### Focus

* Broadcast join
* Shuffle join
* Colocate join

### Best Practices

* Small dimension broadcasting
* Reduce network shuffle

---

## 18. `starrocks-aggregation-optimizer`

### Focus

* Pre-aggregation
* Rollups
* Aggregation pushdown

---

## 19. `starrocks-memory-tuning`

### Focus

* Spill prevention
* Query memory limits
* BE memory tuning

---

## 20. `starrocks-concurrency-optimizer`

### Focus

* High concurrency tuning
* BI workloads
* Queue management

---

## 21. `starrocks-cost-based-optimizer-review`

### Focus

* Statistics freshness
* Cardinality estimation
* CBO hints

---

# StarRocks Data Ingestion Skills

## 22. `starrocks-stream-load-review`

### Focus

* Stream Load tuning
* Batch sizing
* Error handling

### Docs

* [Stream Load Docs](https://docs.starrocks.io/docs/loading/StreamLoad/?utm_source=chatgpt.com)

---

## 23. `starrocks-broker-load-optimizer`

### Focus

* S3/HDFS ingestion
* Parallelism
* Retry strategy

---

## 24. `starrocks-routine-load-kafka`

### Focus

* Kafka ingestion
* Exactly-once semantics
* Lag analysis

### Docs

* [Routine Load Docs](https://docs.starrocks.io/docs/loading/RoutineLoad/?utm_source=chatgpt.com)

---

## 25. `starrocks-cdc-pipeline-builder`

### Focus

* Debezium
* Flink CDC
* Upsert streams

---

## 26. `starrocks-files-ingestion-optimizer`

### Focus

* Parquet
* ORC
* Iceberg external tables

---

# Airflow + StarRocks Skills

## 27. `airflow-starrocks-pipeline-generator`

### Generates

* DAGs
* Sensors
* Load jobs
* Validation tasks

### Docs

* [Airflow Docs](https://airflow.apache.org/docs/?utm_source=chatgpt.com)

---

## 28. `airflow-starrocks-etl-best-practices`

### Focus

* Idempotent DAGs
* Backfills
* Retry strategy
* SLAs

---

## 29. `airflow-starrocks-cdc-orchestrator`

### Focus

* CDC DAG orchestration
* Watermarks
* Incremental sync

---

## 30. `airflow-starrocks-data-quality-checks`

### Focus

* Post-load validation
* Freshness checks
* Volume anomaly detection

---

## 31. `airflow-starrocks-backfill-manager`

### Focus

* Historical reprocessing
* Partition backfills
* Replay safety

---

# StarRocks Pipeline Skills

## 32. `starrocks-medallion-architecture-builder`

### Generates

* Bronze/Silver/Gold layers
* Incremental transformations
* Aggregation strategy

---

## 33. `starrocks-realtime-analytics-pipeline`

### Focus

* Kafka → StarRocks
* Low latency BI
* Streaming dashboards

---

## 34. `starrocks-lakehouse-integration`

### Focus

* Iceberg catalog
* Hive Metastore
* External tables

### Docs

* [Lakehouse Integration](https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog/?utm_source=chatgpt.com)

---

## 35. `starrocks-data-quality-guardian`

### Focus

* Freshness
* Duplicate detection
* Null anomaly
* Metric drift

---

# StarRocks + dbt Skills

## 36. `dbt-starrocks-model-review`

### Focus

* Incremental models
* Materializations
* Ephemeral usage

### Docs

* [dbt Docs](https://docs.getdbt.com?utm_source=chatgpt.com)

---

## 37. `dbt-starrocks-performance-optimizer`

### Focus

* Model DAG optimization
* Incremental merge
* Partition-aware models

---

## 38. `dbt-starrocks-testing-framework`

### Focus

* Generic tests
* Custom tests
* Freshness validation

---

## 39. `dbt-starrocks-semantic-layer`

### Focus

* Metrics layer
* Business KPIs
* Semantic governance

---

## 40. `dbt-starrocks-production-readiness`

### Focus

* CI/CD
* Slim CI
* State comparison
* Artifact validation

---

# Advanced AI-Agent Skills for StarRocks

## 41. `starrocks-ai-query-autotuner`

AI анализирует query plans и:

* переписывает SQL
* рекомендует MV
* предлагает repartitioning

---

## 42. `starrocks-ai-incident-rca`

AI agent:

* анализирует failed queries
* FE/BE logs
* compaction
* memory spikes
* Kafka lag

И делает RCA.

---

## 43. `starrocks-self-healing-platform`

### Autonomous Actions

* restart failed loads
* rebalance tablets
* trigger compaction
* reroute queries
* adjust memory limits

---

# Самые важные skills для MVP

Если делать первую мощную версию репозитория:

1. starrocks-query-optimizer
2. starrocks-materialized-view-advisor
3. starrocks-partitioning-strategy
4. starrocks-routine-load-kafka
5. airflow-starrocks-pipeline-generator
6. dbt-starrocks-model-review
7. starrocks-admin-cluster-health
8. starrocks-data-model-review
9. starrocks-ai-incident-rca
10. starrocks-self-healing-platform

---

# Что особенно инновационно для AI Agents

Вот где реально можно сделать next-gen platform:

| Skill                  | Innovation                      |
| ---------------------- | ------------------------------- |
| AI Query Autotuner     | autonomous SQL optimization     |
| AI RCA                 | self-debugging warehouse        |
| Self Healing           | autonomous operations           |
| Semantic Modeling      | AI-generated marts              |
| Cost AI                | realtime warehouse optimization |
| AI Compaction Tuning   | adaptive storage optimization   |
| Autonomous MV Creation | self-optimizing OLAP            |

---

# Очень полезные дополнительные ресурсы

## StarRocks Architecture

* [StarRocks Architecture](https://docs.starrocks.io/docs/introduction/Architecture/?utm_source=chatgpt.com)

## Best Practices

* [StarRocks Best Practices](https://docs.starrocks.io/docs/best_practices/?utm_source=chatgpt.com)

## Performance

* [Performance Tuning](https://docs.starrocks.io/docs/best_practices/query_tuning/?utm_source=chatgpt.com)

## Loading

* [Data Loading Docs](https://docs.starrocks.io/docs/loading/?utm_source=chatgpt.com)

## dbt

* [dbt Official Docs](https://docs.getdbt.com?utm_source=chatgpt.com)
