---
name: trino-lakehouse-platform-architect
description: Trino-based Modern Data Platform architecture design — decoupled storage/compute, Iceberg as open table format, Hive Metastore/Glue catalog, MinIO/S3 object storage, Kafka ingestion layer, dbt transformation, Airflow orchestration, Superset BI — coordinator/worker topology, catalog design, metadata-driven architecture, multi-layer lakehouse (Bronze/Silver/Gold), federated query across heterogeneous sources
---

# Trino Lakehouse Platform Architecture

## When to Use

- Designing a new Trino-based data platform from scratch
- Migrating from legacy Hadoop/Hive stack to a Modern Data Stack
- Architecting decoupled storage/compute to eliminate vendor lock-in
- Building a multi-tenant analytical platform with federated query across databases, object storage, and streaming sources
- Choosing between Trino, Spark, ClickHouse for the query layer

---

## Core Architecture: Compute vs Storage Separation

```
┌──────────────────────────────────────────────────────────────────┐
│                        BI Layer                                   │
│         Superset / Metabase / Grafana / Tableau                  │
└────────────────────────────┬─────────────────────────────────────┘
                             │ SQL
┌────────────────────────────▼─────────────────────────────────────┐
│                     Query Layer: Trino                            │
│   Coordinator (1 per cluster) + Workers (auto-scale)            │
│   Catalogs: iceberg, postgresql, kafka, mysql, ...              │
└──────┬────────────┬──────────────┬──────────────────────────────┘
       │            │              │
┌──────▼──┐  ┌──────▼──────┐  ┌───▼────────────────────────┐
│ Iceberg  │  │  PostgreSQL  │  │  Kafka (read via connector) │
│ (S3/GCS) │  │  (OLTP)     │  │  (streaming data preview)  │
└──────┬──┘  └─────────────┘  └────────────────────────────┘
       │
┌──────▼──────────────────────────────────────────────────────────┐
│           Object Storage: MinIO / AWS S3 / GCS                   │
│   bronze/  silver/  gold/  (Parquet + Iceberg metadata)         │
└──────────────────────────────────────────────────────────────────┘
       │
┌──────▼──────────────────────────────────────────────────────────┐
│           Catalog / Metastore Layer                              │
│   Hive Metastore (HMS) / AWS Glue / REST catalog / Nessie       │
└──────────────────────────────────────────────────────────────────┘
```

**Design principle**: Trino is the query engine only — it stores nothing. All data lives in object storage (S3/GCS/MinIO) as open-format Iceberg/Parquet files.

---

## Cluster Topology

### Coordinator

```properties
# etc/config.properties — coordinator
coordinator=true
node-scheduler.include-coordinator=false   # never schedule work on coordinator
http-server.http.port=8080
discovery.uri=http://trino-coordinator:8080
query.max-memory=50GB
query.max-memory-per-node=10GB
query.max-total-memory=60GB
query.max-history=1000
query.min-expire-age=30m
```

### Workers

```properties
# etc/config.properties — worker
coordinator=false
http-server.http.port=8080
discovery.uri=http://trino-coordinator:8080
```

### JVM (per node, scale Xmx to ~80% of RAM)

```
# etc/jvm.config
-server
-Xmx54G
-XX:InitialRAMPercentage=80
-XX:MaxRAMPercentage=80
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:ReservedCodeCacheSize=512M
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
```

### OS Prerequisites

```bash
# /etc/security/limits.conf
* soft nofile 131072
* hard nofile 131072
* soft nproc  128000
* hard nproc  128000
```

---

## Catalog Design

Each catalog maps Trino to one data source via a properties file in `etc/catalog/`.

### Iceberg on S3 + HMS

```properties
# etc/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.path-style-access=true
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=minio_access
hive.s3.aws-secret-key=minio_secret
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
iceberg.max-partitions-per-writer=100
iceberg.table-statistics-enabled=true
iceberg.target-max-file-size=512MB
```

### Iceberg on S3 + Glue (AWS)

```properties
connector.name=iceberg
iceberg.catalog.type=glue
hive.metastore.glue.region=us-east-1
hive.s3.region=us-east-1
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
```

### PostgreSQL (operational DB)

```properties
# etc/catalog/postgresql.properties
connector.name=postgresql
connection-url=jdbc:postgresql://postgres:5432/appdb
connection-user=trino_reader
connection-password=${ENV:POSTGRES_PASSWORD}
metadata.cache-ttl=5m
dynamic-filtering.enabled=true
```

### Kafka (event preview)

```properties
# etc/catalog/kafka.properties
connector.name=kafka
kafka.nodes=kafka:9092
kafka.hide-internal-columns=false
```

---

## Lakehouse Layer Design

| Layer | Iceberg Schema | Key Properties | Use Case |
|-------|---------------|----------------|----------|
| **Bronze** | `iceberg.bronze` | `format=PARQUET`, no partition | Raw ingestion, append-only |
| **Silver** | `iceberg.silver` | `format=PARQUET`, `partitioning=ARRAY['day(event_time)']` | Cleaned, deduplicated |
| **Gold** | `iceberg.gold` | `format=PARQUET`, partitioned + sorted | Analytical aggregates |

```sql
-- Bronze: raw events, append only
CREATE SCHEMA iceberg.bronze WITH (location = 's3://data-lake/bronze/');

CREATE TABLE iceberg.bronze.orders_raw (
    raw_payload VARCHAR,
    kafka_topic  VARCHAR,
    kafka_offset BIGINT,
    ingested_at  TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD'
);

-- Silver: curated orders
CREATE SCHEMA iceberg.silver WITH (location = 's3://data-lake/silver/');

CREATE TABLE iceberg.silver.orders (
    order_id     BIGINT        NOT NULL,
    customer_id  BIGINT,
    order_date   DATE,
    amount       DECIMAL(18,2),
    status       VARCHAR,
    updated_at   TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(order_date)'],
    sorted_by    = ARRAY['customer_id'],
    compression_codec = 'ZSTD',
    format_version = 2
);

-- Gold: aggregated fact
CREATE SCHEMA iceberg.gold WITH (location = 's3://data-lake/gold/');

CREATE TABLE iceberg.gold.daily_revenue (
    order_date      DATE,
    region          VARCHAR,
    total_orders    BIGINT,
    gross_revenue   DECIMAL(18,2),
    net_revenue     DECIMAL(18,2)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['month(order_date)', 'region'],
    sorted_by    = ARRAY['order_date DESC'],
    compression_codec = 'ZSTD'
);
```

---

## Technology Selection Matrix

| Need | Recommended | Why |
|------|-------------|-----|
| Large analytical queries across data lake | **Trino** | Distributed SQL, federated, no data lock-in |
| Sub-second dashboard queries on pre-aggregated data | **ClickHouse** | Columnar, co-located compute+storage |
| ML feature engineering at scale | **Spark** | UDFs, MLlib, Python ecosystem |
| Operational queries on fresh OLTP data | **PostgreSQL/MySQL direct** | Low latency, indexes |
| Mixed: federate OLTP + data lake | **Trino** | Cross-catalog JOIN |

---

## Component Stack Reference

| Component | Technology | Version |
|-----------|-----------|---------|
| Query engine | Trino | 481+ |
| Table format | Apache Iceberg | 1.7+ |
| Metastore | Hive Metastore 3.x / AWS Glue | 3.1.3 |
| Object storage | MinIO (on-prem) / AWS S3 / GCS | - |
| Transformation | dbt-trino | 1.8+ |
| Orchestration | Apache Airflow | 2.9+ |
| Streaming ingest | Apache Kafka + Flink/Debezium | - |
| BI | Apache Superset | 4.x |
| Monitoring | Prometheus + Grafana | - |

---

## Anti-Patterns

1. **Running Trino coordinator on the same node that runs heavy workers** — dedicate coordinator, set `node-scheduler.include-coordinator=false` in production.
2. **Using Hive-style static partitions (date=2024-01-01) instead of Iceberg hidden partitioning** — Iceberg transforms (`day(ts)`) enable partition evolution and don't require client-side partition pruning.
3. **One giant catalog for all data** — separate catalogs by domain or environment (bronze/silver/gold as schemas within `iceberg`, not as separate catalogs) for RBAC and resource isolation.
4. **Cross-catalog JOINs between connectors** — Trino fetches data from each connector separately then joins in-memory; expensive for large tables. Pre-materialize into Iceberg when JOIN is frequent.
5. **JVM heap under 16GB per worker** — Trino is memory-intensive; workers with less than 16GB heap will spill constantly; 32–64GB is typical production sizing.
6. **Omitting `sorted_by` on Gold tables** — sorted files dramatically improve filter performance and compaction efficiency on large analytical tables.

---

## References

- Trino deployment: `trino.io/docs/current/installation/deployment.html`
- Iceberg connector: `trino.io/docs/current/connector/iceberg.html`
- Trino concepts: `trino.io/docs/current/overview/concepts.html`
- Related skills: `[[trino-iceberg-best-practices]]`, `[[trino-query-optimization]]`, `[[trino-docker-compose-stack]]`, `[[trino-production-readiness-review]]`
