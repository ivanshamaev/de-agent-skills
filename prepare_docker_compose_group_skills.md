# Docker Compose AI Agent Skills for Modern Data Stack

Ниже — набор из **50 AI Skills** для генерации production-grade `docker-compose.yml` для Modern Data Stack, Lakehouse, Streaming, MLOps, Observability и Agentic Data Platforms.

Это очень сильное направление для AI agents, потому что:

* compose generation хорошо формализуется,
* AI может автоматически собирать dependency graph,
* валидировать networking,
* healthchecks,
* volumes,
* startup ordering,
* secrets,
* observability,
* scaling profiles.

---

# Core Docker Compose Skills

## 1. `docker-compose-modern-data-stack-generator`

### Generates

Полноценный MDS stack:

* Airflow
* dbt
* Postgres
* MinIO
* Spark
* Kafka
* Trino
* Superset

---

## 2. `docker-compose-production-hardening`

### Focus

* restart policies
* healthchecks
* resource limits
* non-root users
* network isolation

### Docs

* [Docker Compose Docs](https://docs.docker.com/compose/?utm_source=chatgpt.com)

---

## 3. `docker-compose-network-topology-designer`

### Focus

* internal networks
* external ingress
* service isolation
* DNS naming

---

## 4. `docker-compose-healthcheck-generator`

### Generates

* HTTP checks
* TCP checks
* readiness probes
* dependency validation

---

## 5. `docker-compose-volume-strategy`

### Focus

* persistent volumes
* bind mounts
* named volumes
* backup persistence

---

## 6. `docker-compose-secrets-management`

### Focus

* .env generation
* Docker secrets
* Vault integration
* secret injection

---

## 7. `docker-compose-resource-optimizer`

### Focus

* memory limits
* CPU allocation
* JVM tuning
* storage constraints

---

## 8. `docker-compose-multi-environment-generator`

### Generates

* dev
* staging
* prod
  profiles

---

## 9. `docker-compose-startup-order-manager`

### Focus

* depends_on
* wait-for-it
* health-based startup

---

## 10. `docker-compose-debugging-assistant`

### Focus

* failed services
* port conflicts
* DNS issues
* startup loops

---

# Airflow Compose Skills

## 11. `docker-compose-airflow-cluster`

### Generates

* scheduler
* webserver
* workers
* triggerer
* postgres
* redis

### Docs

* [Airflow Docker Docs](https://airflow.apache.org/docs/docker-stack/?utm_source=chatgpt.com)

---

## 12. `docker-compose-airflow-celeryexecutor`

### Focus

* CeleryExecutor
* Redis
* worker autoscaling

---

## 13. `docker-compose-airflow-kafka-stack`

### Generates

* Airflow
* Kafka
* Schema Registry
* Kafka UI

---

## 14. `docker-compose-airflow-spark-stack`

### Generates

* Airflow
* Spark master
* Spark workers
* history server

---

# Kafka & Streaming Compose Skills

## 15. `docker-compose-kafka-cluster`

### Generates

* Kafka brokers
* Zookeeper/KRaft
* Schema Registry
* Kafka Connect

### Docs

* [Kafka Docs](https://kafka.apache.org/documentation/?utm_source=chatgpt.com)

---

## 16. `docker-compose-kafka-kraft-mode`

### Focus

* KRaft setup
* controller quorum
* broker configs

---

## 17. `docker-compose-kafka-connect-stack`

### Generates

* Debezium
* Kafka Connect
* connectors
* monitoring

---

## 18. `docker-compose-streaming-platform`

### Generates

* Kafka
* Flink
* Schema Registry
* MinIO
* Iceberg

---

## 19. `docker-compose-flink-cluster`

### Generates

* JobManager
* TaskManagers
* HA setup

---

# Lakehouse Compose Skills

## 20. `docker-compose-lakehouse-stack`

### Generates

* Iceberg
* Nessie
* MinIO
* Spark
* Trino

---

## 21. `docker-compose-iceberg-platform`

### Focus

* REST catalog
* Hive metastore
* object storage

---

## 22. `docker-compose-trino-lakehouse`

### Generates

* Trino
* Iceberg
* Hive
* MinIO

---

## 23. `docker-compose-starrocks-platform`

### Generates

* FE nodes
* BE nodes
* Kafka
* MinIO

---

## 24. `docker-compose-clickhouse-cluster`

### Generates

* ClickHouse shards
* Keeper/Zookeeper
* dashboards

---

# dbt & Transformation Skills

## 25. `docker-compose-dbt-platform`

### Generates

* dbt
* warehouse
* scheduler
* docs hosting

### Docs

* [dbt Docs](https://docs.getdbt.com/?utm_source=chatgpt.com)

---

## 26. `docker-compose-dbt-airflow-stack`

### Focus

* orchestrated dbt
* testing
* lineage

---

## 27. `docker-compose-dbt-trino-stack`

### Generates

* dbt
* Trino
* Iceberg
* MinIO

---

# Observability Compose Skills

## 28. `docker-compose-observability-stack`

### Generates

* Prometheus
* Grafana
* Loki
* Tempo

### Docs

* [Grafana Docs](https://grafana.com/docs/?utm_source=chatgpt.com)

---

## 29. `docker-compose-opentelemetry-platform`

### Focus

* tracing
* metrics
* logs
* collectors

---

## 30. `docker-compose-monitoring-for-data-platform`

### Focus

* Airflow monitoring
* Kafka metrics
* Spark metrics
* Trino metrics

---

# MLOps Compose Skills

## 31. `docker-compose-mlflow-platform`

### Generates

* MLflow
* Postgres
* MinIO

---

## 32. `docker-compose-feature-store-stack`

### Generates

* Feast
* Redis
* Postgres
* Spark

---

## 33. `docker-compose-vector-db-platform`

### Generates

* Qdrant
* Weaviate
* embeddings API

---

# AI Agent Platform Skills

## 34. `docker-compose-agentic-data-platform`

### Generates

* LangGraph
* Temporal
* Qdrant
* Kafka
* Airflow
* Metadata services

---

## 35. `docker-compose-ai-observability-stack`

### Generates

* Langfuse
* OpenTelemetry
* Grafana

---

## 36. `docker-compose-rag-platform`

### Generates

* vector DB
* embedding service
* LLM gateway
* API service

---

# Security & Governance Skills

## 37. `docker-compose-security-hardening`

### Focus

* rootless containers
* network segmentation
* TLS configs

---

## 38. `docker-compose-vault-integration`

### Generates

* Vault
* secret injection
* token management

---

## 39. `docker-compose-authentication-stack`

### Generates

* Keycloak
* OAuth proxy
* RBAC integration

---

# Infrastructure Skills

## 40. `docker-compose-reverse-proxy-generator`

### Generates

* Nginx
* Traefik
* TLS routing

---

## 41. `docker-compose-load-balancer-stack`

### Focus

* HAProxy
* ingress routing
* sticky sessions

---

## 42. `docker-compose-local-cloud-emulator`

### Generates

* LocalStack
* MinIO
* fake cloud infra

---

# Dev Environment Skills

## 43. `docker-compose-local-dev-environment`

### Focus

* hot reload
* mounted volumes
* debug ports

---

## 44. `docker-compose-data-engineering-sandbox`

### Generates

* notebooks
* Spark
* Kafka
* dbt
* Trino

---

## 45. `docker-compose-jupyter-platform`

### Generates

* Jupyter
* Spark kernels
* MLflow integration

---

# Reliability Skills

## 46. `docker-compose-disaster-recovery-review`

### Focus

* backup containers
* restore workflows
* snapshot automation

---

## 47. `docker-compose-self-healing-platform`

### Focus

* restart policies
* watchdogs
* auto-recovery

---

## 48. `docker-compose-production-readiness`

### Focus

* logging
* monitoring
* persistence
* scaling readiness

---

# AI-Native Compose Skills

## 49. `docker-compose-ai-stack-architect`

### AI Generates

* complete architecture
* dependency graph
* networking
* startup order

---

## 50. `docker-compose-autonomous-platform-builder`

### Fully Autonomous

AI agent:

* выбирает сервисы
* строит compose
* валидирует зависимости
* генерирует `.env`
* создает healthchecks
* делает observability
* пишет README

---

# Самые важные skills для MVP

Если делать killer repository:

1. docker-compose-modern-data-stack-generator
2. docker-compose-lakehouse-stack
3. docker-compose-airflow-cluster
4. docker-compose-kafka-cluster
5. docker-compose-observability-stack
6. docker-compose-production-hardening
7. docker-compose-healthcheck-generator
8. docker-compose-trino-lakehouse
9. docker-compose-agentic-data-platform
10. docker-compose-autonomous-platform-builder

---

# Самые инновационные AI-agent skills

| Skill                       | Innovation             |
| --------------------------- | ---------------------- |
| Autonomous Platform Builder | AI infra generation    |
| AI Stack Architect          | architecture synthesis |
| Self-Healing Compose        | autonomous recovery    |
| Dependency Graph AI         | smart orchestration    |
| Observability Generator     | auto-monitoring        |
| Security Hardening AI       | autonomous security    |

---

# Полезные технологии для compose generation

## Storage

* MinIO
* PostgreSQL
* Redis

## Streaming

* Kafka
* Flink
* Redpanda

## Lakehouse

* Iceberg
* Nessie
* Hive Metastore

## Query Engines

* Trino
* StarRocks
* ClickHouse

## Orchestration

* Airflow
* Dagster
* Temporal

## AI Layer

* LangGraph
* Qdrant
* Langfuse

---

# Очень полезные ресурсы

## Docker Compose

* [Docker Compose Docs](https://docs.docker.com/compose/?utm_source=chatgpt.com)

## Awesome Compose

* [Awesome Docker Compose](https://github.com/docker/awesome-compose?utm_source=chatgpt.com)

## Airflow Docker

* [Airflow Docker Stack](https://airflow.apache.org/docs/docker-stack/?utm_source=chatgpt.com)

## Trino

* [Trino Docs](https://trino.io/docs/current/?utm_source=chatgpt.com)

## Kafka

* [Kafka Docs](https://kafka.apache.org/documentation/?utm_source=chatgpt.com)
