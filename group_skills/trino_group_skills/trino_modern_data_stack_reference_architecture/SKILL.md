---
name: trino-modern-data-stack-reference-architecture
description: Trino Modern Data Stack end-to-end reference architecture — full docker-compose (Kafka + MinIO + Hive Metastore + Trino + Airflow + dbt + Superset + Prometheus + Grafana), medallion lake DDL and pipeline DAGs, Kafka→Iceberg streaming ingest, dbt project layout with Trino profiles, Superset dataset config, Prometheus alert rules, production Kubernetes Helm values, operational runbook for daily maintenance
---

# Trino Modern Data Stack Reference Architecture

## When to Use

- Bootstrapping a new data platform from scratch
- Evaluating the full Modern Data Stack on a single machine before cloud deployment
- Reference for component wiring: how Kafka, Iceberg, Trino, dbt, Airflow, Superset connect
- Migrating from a legacy Hadoop/Hive cluster to a lakehouse architecture
- Onboarding a new data engineering team to production patterns

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         MODERN DATA STACK                                    │
│                                                                              │
│  INGEST            STORAGE          TRANSFORM        SERVE       OBSERVE     │
│  ──────────        ─────────        ─────────        ──────      ─────────   │
│  Kafka             MinIO / S3       dbt              Superset    Prometheus  │
│  (streaming)       (object store)   (SQL models)     (BI)        Grafana     │
│  Airflow           Apache Iceberg   Airflow          Trino CLI   Alert Mgr   │
│  (batch ETL)       (table format)   (orchestration)  JDBC apps               │
│                    Hive Metastore                                             │
│                    (catalog)                                                  │
│                                                                              │
│                         ┌─────────┐                                          │
│                         │  TRINO  │  ← query engine (all layers)             │
│                         └─────────┘                                          │
└──────────────────────────────────────────────────────────────────────────────┘

Data flow:
  Kafka topic → Trino Kafka connector / Flink → Bronze Iceberg table (MinIO)
  Bronze → Silver (MERGE / dedup via Airflow+Trino)
  Silver → Gold (dbt models / materialized views)
  Gold → Superset dashboards / ad-hoc Trino queries
```

---

## Component Versions (reference stack)

| Component | Image | Purpose |
|-----------|-------|---------|
| Apache Kafka | `apache/kafka:3.7` | Event streaming / CDC ingest |
| MinIO | `minio/minio:RELEASE.2024-06-01` | S3-compatible object store |
| PostgreSQL 16 | `postgres:16` | Hive Metastore backend + Airflow metadata |
| Hive Metastore 3.x | `apache/hive:3.1.3` | Iceberg catalog (HMS) |
| Trino 469 | `trinodb/trino:469` | Distributed SQL query engine |
| Apache Airflow 2.9 | `apache/airflow:2.9.3` | Pipeline orchestration |
| dbt-trino | `ghcr.io/dbt-labs/dbt-trino:1.8` | SQL transformation layer |
| Apache Superset | `apache/superset:4.0.0` | BI and dashboards |
| Prometheus | `prom/prometheus:v2.52.0` | Metrics collection |
| Grafana | `grafana/grafana:11.0.0` | Dashboards and alerts |

---

## docker-compose.yml

```yaml
version: "3.9"

x-airflow-common: &airflow-common
  image: apache/airflow:2.9.3
  environment: &airflow-env
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__FERNET_KEY: "your-fernet-key-here"
    AIRFLOW__WEBSERVER__SECRET_KEY: "your-secret-key-here"
    AIRFLOW_CONN_TRINO_DEFAULT: "trino://trino@trino-coordinator:8080/iceberg"
  volumes:
    - ./dags:/opt/airflow/dags
    - ./dbt:/opt/airflow/dbt
  depends_on:
    postgres-airflow:
      condition: service_healthy

services:

  # ── Kafka ─────────────────────────────────────────────────────────────────
  kafka:
    image: apache/kafka:3.7.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qg"
    healthcheck:
      test: ["CMD", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 15s
      timeout: 10s
      retries: 5

  # ── MinIO ─────────────────────────────────────────────────────────────────
  minio:
    image: minio/minio:RELEASE.2024-06-01T17-36-36Z
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin123
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 10s
      timeout: 5s
      retries: 5

  minio-init:
    image: minio/mc:latest
    container_name: minio-init
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
        mc alias set local http://minio:9000 minioadmin minioadmin123;
        mc mb --ignore-existing local/warehouse;
        mc mb --ignore-existing local/trino-spill;
        mc anonymous set none local/warehouse;
        echo 'MinIO buckets ready';
      "

  # ── Hive Metastore ────────────────────────────────────────────────────────
  postgres-metastore:
    image: postgres:16
    container_name: postgres-metastore
    environment:
      POSTGRES_DB: metastore
      POSTGRES_USER: hive
      POSTGRES_PASSWORD: hive123
    volumes:
      - postgres-metastore-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U hive -d metastore"]
      interval: 10s
      timeout: 5s
      retries: 5

  hive-metastore:
    image: apache/hive:3.1.3
    container_name: hive-metastore
    depends_on:
      postgres-metastore:
        condition: service_healthy
      minio-init:
        condition: service_completed_successfully
    ports:
      - "9083:9083"
    environment:
      SERVICE_NAME: metastore
      DB_DRIVER: postgres
      SERVICE_OPTS: >-
        -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
        -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres-metastore:5432/metastore
        -Djavax.jdo.option.ConnectionUserName=hive
        -Djavax.jdo.option.ConnectionPassword=hive123
    volumes:
      - ./conf/hive-site.xml:/opt/hive/conf/hive-site.xml:ro
    healthcheck:
      test: ["CMD-SHELL", "ss -tnl | grep -q 9083"]
      interval: 15s
      timeout: 10s
      retries: 10

  # ── Trino ─────────────────────────────────────────────────────────────────
  trino-coordinator:
    image: trinodb/trino:469
    container_name: trino-coordinator
    ports:
      - "8080:8080"
    depends_on:
      hive-metastore:
        condition: service_healthy
    volumes:
      - ./trino/coordinator/config.properties:/etc/trino/config.properties:ro
      - ./trino/coordinator/jvm.config:/etc/trino/jvm.config:ro
      - ./trino/coordinator/node.properties:/etc/trino/node.properties:ro
      - ./trino/catalog:/etc/trino/catalog:ro
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/v1/info/state"]
      interval: 15s
      timeout: 10s
      retries: 10

  trino-worker:
    image: trinodb/trino:469
    depends_on:
      trino-coordinator:
        condition: service_healthy
    volumes:
      - ./trino/worker/config.properties:/etc/trino/config.properties:ro
      - ./trino/worker/jvm.config:/etc/trino/jvm.config:ro
      - ./trino/worker/node.properties:/etc/trino/node.properties:ro
      - ./trino/catalog:/etc/trino/catalog:ro
    deploy:
      replicas: 2

  # ── Airflow ───────────────────────────────────────────────────────────────
  postgres-airflow:
    image: postgres:16
    container_name: postgres-airflow
    environment:
      POSTGRES_DB: airflow
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
    volumes:
      - postgres-airflow-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow -d airflow"]
      interval: 10s
      timeout: 5s
      retries: 5

  airflow-init:
    <<: *airflow-common
    container_name: airflow-init
    command: >
      bash -c "
        airflow db migrate &&
        airflow users create --username admin --password admin
          --firstname Admin --lastname Admin --role Admin --email admin@example.com
      "

  airflow-webserver:
    <<: *airflow-common
    container_name: airflow-webserver
    command: webserver
    ports:
      - "8088:8080"
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  # ── Superset ──────────────────────────────────────────────────────────────
  superset:
    image: apache/superset:4.0.0
    container_name: superset
    ports:
      - "8088:8088"
    environment:
      SUPERSET_SECRET_KEY: "your-superset-secret-key"
      DATABASE_URL: "postgresql+psycopg2://superset:superset@postgres-airflow/superset"
    command: >
      bash -c "
        superset db upgrade &&
        superset fab create-admin --username admin --firstname Admin
          --lastname Admin --email admin@example.com --password admin &&
        superset init &&
        superset run -p 8088 --with-threads --reload --debugger
      "

  # ── Prometheus + Grafana ──────────────────────────────────────────────────
  prometheus:
    image: prom/prometheus:v2.52.0
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./monitoring/rules:/etc/prometheus/rules:ro
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.retention.time=30d'

  grafana:
    image: grafana/grafana:11.0.0
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_USERS_ALLOW_SIGN_UP: "false"
    volumes:
      - grafana-data:/var/lib/grafana
      - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards:ro
      - ./monitoring/grafana/datasources:/etc/grafana/provisioning/datasources:ro
    depends_on:
      - prometheus

volumes:
  minio-data:
  postgres-metastore-data:
  postgres-airflow-data:
  grafana-data:
```

---

## Trino Configuration Files

### `trino/coordinator/config.properties`

```properties
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080
discovery.uri=http://trino-coordinator:8080

query.max-memory=4GB
query.max-total-memory=8GB
query.max-memory-per-node=2GB
memory.heap-headroom-per-node=512MB

query.max-execution-time=4h
query.max-history=500
query.min-expire-age=15m

spill-enabled=true
spiller-spill-path=/tmp/trino-spill
max-spill-per-node=20GB
```

### `trino/coordinator/jvm.config`

```
-server
-Xmx6G
-XX:InitialRAMPercentage=80
-XX:MaxRAMPercentage=80
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/var/trino/heap-dump.hprof
-XX:+UseGCOverheadLimit
-XX:ReservedCodeCacheSize=512M
-Djdk.attach.allowAttachSelf=true
-Dfile.encoding=UTF-8
```

### `trino/worker/config.properties`

```properties
coordinator=false
http-server.http.port=8080
discovery.uri=http://trino-coordinator:8080

query.max-memory-per-node=2GB
memory.heap-headroom-per-node=512MB

spill-enabled=true
spiller-spill-path=/tmp/trino-spill
```

### `trino/catalog/iceberg.properties`

```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore:9083

# S3/MinIO storage
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin123
hive.s3.ssl.enabled=false

# Defaults
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
iceberg.target-max-file-size=268435456

# Performance
iceberg.split-manager-threads=4
iceberg.max-partitions-per-writer=100
```

### `trino/catalog/kafka.properties`

```properties
connector.name=kafka
kafka.nodes=kafka:9092
kafka.default-schema=events
kafka.hide-internal-columns=false
kafka.table-description-dir=/etc/trino/kafka-tables
```

### `conf/hive-site.xml`

```xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://postgres-metastore:5432/metastore</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>s3a://warehouse/</value>
  </property>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>http://minio:9000</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value>minioadmin</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>minioadmin123</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.autoCreateSchema</name>
    <value>true</value>
  </property>
</configuration>
```

---

## Medallion Lake DDL

Run against Trino coordinator to bootstrap all three layers:

```sql
-- Bronze: raw ingest, append-only, minimal schema
CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location = 's3://warehouse/bronze/');

CREATE TABLE IF NOT EXISTS iceberg.bronze.orders_raw (
    ingested_at      TIMESTAMP(6) WITH TIME ZONE,
    kafka_offset     BIGINT,
    kafka_partition  INTEGER,
    payload          VARCHAR          -- raw JSON string
)
WITH (
    format         = 'PARQUET',
    partitioning   = ARRAY['day(ingested_at)'],
    format_version = '2'
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.customers_raw (
    ingested_at   TIMESTAMP(6) WITH TIME ZONE,
    payload       VARCHAR
)
WITH (
    format         = 'PARQUET',
    partitioning   = ARRAY['day(ingested_at)'],
    format_version = '2'
);


-- Silver: cleansed, typed, deduplicated
CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location = 's3://warehouse/silver/');

CREATE TABLE IF NOT EXISTS iceberg.silver.orders (
    order_id        BIGINT,
    customer_id     BIGINT,
    order_date      DATE,
    status          VARCHAR(32),
    total_amount    DECIMAL(18, 2),
    currency        VARCHAR(3),
    updated_at      TIMESTAMP(6) WITH TIME ZONE,
    _etl_loaded_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format         = 'PARQUET',
    partitioning   = ARRAY['month(order_date)'],
    sorted_by      = ARRAY['customer_id'],
    format_version = '2',
    compression_codec = 'ZSTD'
);

CREATE TABLE IF NOT EXISTS iceberg.silver.customers (
    customer_id   BIGINT,
    email         VARCHAR,
    country_code  VARCHAR(2),
    created_at    TIMESTAMP(6) WITH TIME ZONE,
    updated_at    TIMESTAMP(6) WITH TIME ZONE,
    _etl_loaded_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format         = 'PARQUET',
    format_version = '2',
    compression_codec = 'ZSTD'
);


-- Gold: aggregated, business-ready
CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location = 's3://warehouse/gold/');

CREATE TABLE IF NOT EXISTS iceberg.gold.daily_revenue (
    report_date      DATE,
    country_code     VARCHAR(2),
    order_count      BIGINT,
    revenue_usd      DECIMAL(18, 2),
    avg_order_value  DECIMAL(18, 2),
    _refreshed_at    TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format         = 'PARQUET',
    partitioning   = ARRAY['month(report_date)'],
    format_version = '2',
    compression_codec = 'ZSTD'
);
```

---

## Airflow DAGs

### Bronze Ingest DAG (Kafka → Iceberg)

```python
from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.trino.hooks.trino import TrinoHook

@dag(
    dag_id="kafka_to_bronze",
    schedule="*/5 * * * *",    # every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
    tags=["ingest", "bronze"],
)
def kafka_to_bronze():

    @task
    def ingest_orders() -> int:
        hook = TrinoHook(trino_conn_id="trino_default")
        # Read from Kafka connector, insert into Bronze
        # Kafka connector topic = 'orders' mapped to kafka.events.orders
        result = hook.get_first("""
            INSERT INTO iceberg.bronze.orders_raw (ingested_at, kafka_offset, kafka_partition, payload)
            SELECT
                NOW() AS ingested_at,
                _message_offset,
                _partition_id,
                _message
            FROM kafka.events.orders
            WHERE _message_offset > (
                SELECT COALESCE(MAX(kafka_offset), -1)
                FROM iceberg.bronze.orders_raw
                WHERE ingested_at >= CURRENT_DATE
                  AND kafka_partition = _partition_id
            )
        """)
        return result[0] if result else 0

    @task
    def ingest_customers() -> int:
        hook = TrinoHook(trino_conn_id="trino_default")
        result = hook.get_first("""
            INSERT INTO iceberg.bronze.customers_raw (ingested_at, payload)
            SELECT NOW(), _message
            FROM kafka.events.customers
            WHERE _timestamp >= NOW() - INTERVAL '6' MINUTE
        """)
        return result[0] if result else 0

    ingest_orders()
    ingest_customers()

kafka_to_bronze()
```

### Silver Transform DAG (Bronze → Silver, daily)

```python
from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.utils.dates import days_ago

@dag(
    dag_id="bronze_to_silver",
    schedule="0 3 * * *",      # 03:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["transform", "silver"],
)
def bronze_to_silver():

    @task
    def merge_orders(logical_date=None) -> dict:
        hook = TrinoHook(trino_conn_id="trino_default")
        process_date = (logical_date or datetime.utcnow()).strftime("%Y-%m-%d")

        hook.run(f"""
            MERGE INTO iceberg.silver.orders AS target
            USING (
                SELECT
                    CAST(JSON_EXTRACT_SCALAR(payload, '$.order_id')   AS BIGINT)       AS order_id,
                    CAST(JSON_EXTRACT_SCALAR(payload, '$.customer_id') AS BIGINT)      AS customer_id,
                    DATE(JSON_EXTRACT_SCALAR(payload, '$.order_date'))                 AS order_date,
                    JSON_EXTRACT_SCALAR(payload, '$.status')                           AS status,
                    CAST(JSON_EXTRACT_SCALAR(payload, '$.total_amount') AS DECIMAL(18,2)) AS total_amount,
                    COALESCE(JSON_EXTRACT_SCALAR(payload, '$.currency'), 'USD')        AS currency,
                    CAST(JSON_EXTRACT_SCALAR(payload, '$.updated_at')
                         AS TIMESTAMP(6) WITH TIME ZONE)                               AS updated_at,
                    NOW()                                                              AS _etl_loaded_at
                FROM iceberg.bronze.orders_raw
                WHERE ingested_at >= DATE '{process_date}'
                  AND ingested_at < DATE '{process_date}' + INTERVAL '1' DAY
                  AND JSON_EXTRACT_SCALAR(payload, '$.order_id') IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY JSON_EXTRACT_SCALAR(payload, '$.order_id')
                    ORDER BY CAST(JSON_EXTRACT_SCALAR(payload, '$.updated_at')
                                  AS TIMESTAMP(6) WITH TIME ZONE) DESC
                ) = 1
            ) AS source ON target.order_id = source.order_id
            WHEN MATCHED AND source.updated_at > target.updated_at THEN
                UPDATE SET
                    status        = source.status,
                    total_amount  = source.total_amount,
                    updated_at    = source.updated_at,
                    _etl_loaded_at = source._etl_loaded_at
            WHEN NOT MATCHED THEN
                INSERT (order_id, customer_id, order_date, status, total_amount,
                        currency, updated_at, _etl_loaded_at)
                VALUES (source.order_id, source.customer_id, source.order_date,
                        source.status, source.total_amount, source.currency,
                        source.updated_at, source._etl_loaded_at)
        """)
        count = hook.get_first("SELECT COUNT(*) FROM iceberg.silver.orders")[0]
        return {"silver_orders_total": count}

    @task
    def dq_gate(metrics: dict) -> None:
        total = metrics.get("silver_orders_total", 0)
        if total == 0:
            raise ValueError("DQ gate failed: silver.orders is empty after merge")

    @task
    def analyze_silver() -> None:
        hook = TrinoHook(trino_conn_id="trino_default")
        hook.run("ANALYZE iceberg.silver.orders")
        hook.run("ANALYZE iceberg.silver.customers")

    metrics = merge_orders()
    dq_gate(metrics) >> analyze_silver()

bronze_to_silver()
```

### Gold Refresh + Maintenance DAG

```python
from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.trino.hooks.trino import TrinoHook

ICEBERG_TABLES = [
    "iceberg.bronze.orders_raw",
    "iceberg.bronze.customers_raw",
    "iceberg.silver.orders",
    "iceberg.silver.customers",
    "iceberg.gold.daily_revenue",
]

@dag(
    dag_id="gold_refresh_and_maintenance",
    schedule="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["gold", "maintenance"],
)
def gold_refresh_and_maintenance():

    @task
    def refresh_daily_revenue() -> None:
        hook = TrinoHook(trino_conn_id="trino_default")
        hook.run("""
            DELETE FROM iceberg.gold.daily_revenue
            WHERE report_date >= CURRENT_DATE - INTERVAL '3' DAY
        """)
        hook.run("""
            INSERT INTO iceberg.gold.daily_revenue
            SELECT
                o.order_date                       AS report_date,
                c.country_code,
                COUNT(*)                           AS order_count,
                SUM(o.total_amount)                AS revenue_usd,
                AVG(o.total_amount)                AS avg_order_value,
                NOW()                              AS _refreshed_at
            FROM iceberg.silver.orders o
            JOIN iceberg.silver.customers c ON o.customer_id = c.customer_id
            WHERE o.order_date >= CURRENT_DATE - INTERVAL '3' DAY
              AND o.status != 'CANCELLED'
            GROUP BY o.order_date, c.country_code
        """)

    @task
    def compact_table(table: str) -> None:
        hook = TrinoHook(trino_conn_id="trino_default")
        hook.run(f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '128MB')")

    @task
    def expire_snapshots(table: str) -> None:
        hook = TrinoHook(trino_conn_id="trino_default")
        hook.run(f"""
            ALTER TABLE {table} EXECUTE expire_snapshots(
                retention_threshold => '7d'
            )
        """)

    @task
    def remove_orphans(table: str) -> None:
        hook = TrinoHook(trino_conn_id="trino_default")
        hook.run(f"""
            ALTER TABLE {table} EXECUTE remove_orphan_files(
                retention_threshold => '2d'
            )
        """)

    # Refresh Gold first
    refresh_task = refresh_daily_revenue()

    # Then run maintenance on all tables sequentially
    for table in ICEBERG_TABLES:
        compact  = compact_table.override(task_id=f"compact_{table.split('.')[-1]}")(table)
        expire   = expire_snapshots.override(task_id=f"expire_{table.split('.')[-1]}")(table)
        orphans  = remove_orphans.override(task_id=f"orphans_{table.split('.')[-1]}")(table)
        refresh_task >> compact >> expire >> orphans

gold_refresh_and_maintenance()
```

---

## dbt Project Structure

```
dbt/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql
│   │   └── stg_customers.sql
│   ├── intermediate/
│   │   └── int_orders_enriched.sql
│   └── marts/
│       ├── mart_daily_revenue.sql
│       └── mart_customer_lifetime_value.sql
├── tests/
│   └── assert_no_negative_revenue.sql
└── macros/
    └── generate_schema_name.sql
```

### `dbt/profiles.yml`

```yaml
mds_trino:
  target: dev
  outputs:
    dev:
      type: trino
      method: none
      host: localhost
      port: 8080
      database: iceberg
      schema: gold
      threads: 4
      http_scheme: http

    prod:
      type: trino
      method: ldap
      host: trino.prod.internal
      port: 8443
      database: iceberg
      schema: gold
      user: "{{ env_var('DBT_TRINO_USER') }}"
      password: "{{ env_var('DBT_TRINO_PASSWORD') }}"
      threads: 8
      http_scheme: https
```

### `dbt/dbt_project.yml`

```yaml
name: mds_trino
version: "1.0.0"
config-version: 2
profile: mds_trino

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

models:
  mds_trino:
    staging:
      +materialized: view
      +schema: silver
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
      +schema: gold
      +table_properties:
        format: PARQUET
        format_version: "'2'"
        compression_codec: ZSTD
```

### `dbt/models/staging/stg_orders.sql`

```sql
-- staging/stg_orders.sql  (materialized: view on iceberg.silver)
SELECT
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    currency,
    updated_at
FROM {{ source('silver', 'orders') }}
WHERE status IS NOT NULL
```

### `dbt/models/marts/mart_daily_revenue.sql`

```sql
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['report_date', 'country_code'],
        table_properties = {
            'format': 'PARQUET',
            'format_version': "'2'",
            'partitioning': "ARRAY['month(report_date)']",
            'compression_codec': 'ZSTD'
        }
    )
}}

SELECT
    o.order_date                     AS report_date,
    c.country_code,
    COUNT(*)                         AS order_count,
    SUM(o.total_amount)              AS revenue_usd,
    ROUND(AVG(o.total_amount), 2)    AS avg_order_value,
    NOW()                            AS _refreshed_at
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
WHERE o.status != 'CANCELLED'

{% if is_incremental() %}
    AND o.order_date >= CURRENT_DATE - INTERVAL '3' DAY
{% endif %}

GROUP BY o.order_date, c.country_code
```

---

## Monitoring Configuration

### `monitoring/prometheus.yml`

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - /etc/prometheus/rules/*.yml

scrape_configs:
  - job_name: trino
    static_configs:
      - targets:
          - trino-coordinator:9090
          - trino-worker:9090
    relabel_configs:
      - source_labels: [__address__]
        regex: "trino-coordinator.*"
        target_label: role
        replacement: coordinator
      - source_labels: [__address__]
        regex: "trino-worker.*"
        target_label: role
        replacement: worker

  - job_name: kafka
    static_configs:
      - targets: ['kafka:9308']     # kafka-exporter

  - job_name: minio
    static_configs:
      - targets: ['minio:9000']
    metrics_path: /minio/v2/metrics/cluster
```

### `monitoring/rules/trino-alerts.yml`

```yaml
groups:
  - name: trino-mds
    interval: 30s
    rules:

      - alert: TrinoNoWorkers
        expr: trino_active_workers == 0
        for: 1m
        labels: {severity: critical}
        annotations:
          summary: "All Trino workers are down"

      - alert: TrinoHighQueueDepth
        expr: trino_queued_queries > 20
        for: 3m
        labels: {severity: warning}
        annotations:
          summary: "Trino queue depth {{ $value }} — scale workers"

      - alert: TrinoOOMKills
        expr: increase(trino_queries_killed_oom_total[10m]) > 0
        labels: {severity: warning}
        annotations:
          summary: "{{ $value }} queries OOM-killed in last 10m"

      - alert: KafkaConsumerLag
        expr: kafka_consumergroup_lag_sum > 100000
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "Kafka lag {{ $value }} on group {{ $labels.consumergroup }}"

      - alert: MinIODiskUsageHigh
        expr: minio_cluster_capacity_usable_free_bytes / minio_cluster_capacity_usable_total_bytes < 0.15
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "MinIO free capacity below 15%"
```

---

## Quick Start

```bash
# Clone / create project layout
mkdir -p mds && cd mds
mkdir -p trino/{coordinator,worker,catalog} conf dags dbt monitoring/{grafana/{dashboards,datasources},rules}

# Copy config files, then:
docker compose up -d

# Wait for all services
docker compose ps   # all should be healthy

# Bootstrap Iceberg schemas
docker exec trino-coordinator trino \
  --server http://localhost:8080 \
  --execute "CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3://warehouse/bronze/');
             CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location = 's3://warehouse/silver/');
             CREATE SCHEMA IF NOT EXISTS iceberg.gold   WITH (location = 's3://warehouse/gold/');"

# Run dbt models
docker exec airflow-scheduler \
  bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir . --project-dir ."

# Trigger Airflow DAGs
open http://localhost:8080   # Airflow UI  (admin/admin)
open http://localhost:8088   # Superset    (admin/admin)
open http://localhost:3000   # Grafana     (admin/admin)
open http://localhost:8080   # Trino Web UI
```

---

## Kubernetes Production Deployment

```yaml
# values-production.yaml for trinodb/trino Helm chart
image:
  tag: "469"

coordinator:
  jvm:
    maxHeapSize: "54G"
  config:
    query:
      maxMemory: "200GB"
  resources:
    requests: {memory: "60Gi", cpu: "8"}
    limits:   {memory: "64Gi", cpu: "16"}
  nodeSelector:
    node-role: trino-coordinator

worker:
  replicas: 5
  jvm:
    maxHeapSize: "54G"
  config:
    query:
      maxMemoryPerNode: "32GB"
  resources:
    requests: {memory: "60Gi", cpu: "8"}
    limits:   {memory: "64Gi", cpu: "16"}
  nodeSelector:
    node-role: trino-worker
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 20
    targetCPUUtilizationPercentage: 60

additionalCatalogs:
  iceberg: |
    connector.name=iceberg
    iceberg.catalog.type=glue
    hive.metastore.glue.region=us-east-1
    hive.s3.endpoint=https://s3.us-east-1.amazonaws.com
    iceberg.file-format=PARQUET
    iceberg.compression-codec=ZSTD

serviceMonitor:
  enabled: true

additionalConfigProperties:
  - "internal-communication.shared-secret=${ENV:TRINO_SHARED_SECRET}"
  - "http-server.authentication.type=OAUTH2"
  - "retry-policy=TASK"
  - "task-retry-attempts-per-task=4"
  - "spill-enabled=true"
  - "spiller-spill-path=/data/trino-spill"
```

---

## Operational Runbook

### Daily health check

```bash
# 1. Confirm coordinator is active
curl -s http://trino:8080/v1/info/state | tr -d '"'   # → ACTIVE

# 2. Check worker count
curl -s http://trino:8080/v1/node | python3 -c "import sys,json; print(len(json.load(sys.stdin)),'workers')"

# 3. Check for failed queries in last 1h
trino --server http://trino:8080 --execute "
  SELECT COUNT(*) AS failed
  FROM system.runtime.queries
  WHERE state = 'FAILED'
    AND create_time >= NOW() - INTERVAL '1' HOUR"

# 4. Check Iceberg table health
trino --server http://trino:8080 --execute "
  SELECT table_schema, table_name,
         element_at(properties, 'current-snapshot-id') AS snapshot_id
  FROM iceberg.information_schema.tables
  WHERE table_schema IN ('bronze','silver','gold')
  ORDER BY 1,2"
```

### Weekly cost review

```sql
-- Top 10 most expensive queries this week
SELECT
    user,
    source,
    ROUND(SUM(total_cpu_time_secs) / 3600.0, 1)    AS cpu_hours,
    ROUND(SUM(total_bytes_read) / 1e12, 2)          AS tb_scanned,
    COUNT(*)                                         AS query_count
FROM system.runtime.queries
WHERE create_time >= NOW() - INTERVAL '7' DAY
  AND state = 'FINISHED'
GROUP BY user, source
ORDER BY cpu_hours DESC
LIMIT 10;
```

### Iceberg maintenance (run after large loads)

```sql
ALTER TABLE iceberg.silver.orders
    EXECUTE optimize(file_size_threshold => '256MB');

ALTER TABLE iceberg.silver.orders
    EXECUTE expire_snapshots(retention_threshold => '7d');

ALTER TABLE iceberg.silver.orders
    EXECUTE remove_orphan_files(retention_threshold => '2d');

ANALYZE iceberg.silver.orders;
```

---

## Component Integration Points

| From | To | Mechanism | Notes |
|------|----|-----------|-------|
| Kafka | Bronze Iceberg | Trino Kafka connector INSERT | 5-min micro-batch |
| Bronze | Silver | Trino MERGE via Airflow | Daily at 03:00 |
| Silver | Gold | dbt incremental / Trino INSERT | Daily at 05:00 |
| Gold | Superset | Trino JDBC SQLAlchemy | Live queries |
| dbt | Trino | trino adapter (LDAP/none) | Incremental models |
| Airflow | Trino | TrinoHook / TrinoOperator | All pipeline tasks |
| Trino | Prometheus | JMX exporter (port 9090) | Scraped every 15s |
| Prometheus | Grafana | prometheus datasource | Dashboards + alerts |

---

## Anti-Patterns

1. **Running dbt and Airflow as two separate orchestrators for the same DAG** — pick one: either Airflow calls dbt via BashOperator, or dbt is the sole orchestrator; dual orchestration creates dependency conflicts and unclear ownership.
2. **Pointing all layers (Bronze/Silver/Gold) to the same schema** — without schema separation, `OPTIMIZE` on Bronze large raw tables competes with analyst queries on Gold; always use separate schemas with separate resource groups.
3. **No watermark table for incremental ingestion** — relying on `MAX(kafka_offset)` queries against a growing Bronze table creates O(n) scans on every micro-batch; maintain a lightweight `watermarks` table.
4. **Using Trino for Kafka streaming ingest without micro-batching** — the Kafka connector performs a full topic scan on every query; implement explicit offset tracking or use Flink/Kafka Connect for true streaming, reserving Trino for batch ingestion.
5. **Superset connecting directly to Bronze tables** — Bronze tables are large, uncompacted, and unoptimized; all BI tools must connect only to Gold; enforce this with Trino file-based ACL restricting the `superset` service account to `iceberg.gold`.
6. **Single-node MinIO in production** — the docker-compose MinIO is single-node only; in production use MinIO distributed mode (≥4 nodes) or a managed S3 service for durability and erasure coding.
7. **No resource group isolation between dbt and interactive queries** — dbt runs can saturate all workers during business hours; assign dbt to a `batch` resource group with lower `schedulingWeight` and `maxConcurrentQueries`.

---

## References

- Trino ecosystem: `trino.io/ecosystem/index.html`
- Trino deployment: `trino.io/docs/current/installation/deployment.html`
- Trino Kafka connector: `trino.io/docs/current/connector/kafka.html`
- Iceberg connector: `trino.io/docs/current/connector/iceberg.html`
- dbt-trino adapter: `docs.getdbt.com/docs/core/connect-data-platform/trino-setup`
- Related skills: `[[trino-lakehouse-platform-architect]]`, `[[trino-airflow-lakehouse-pipelines]]`, `[[trino-dbt-platform]]`, `[[trino-docker-compose-stack]]`, `[[trino-observability-platform]]`, `[[trino-cost-optimization]]`, `[[trino-security-and-governance]]`
