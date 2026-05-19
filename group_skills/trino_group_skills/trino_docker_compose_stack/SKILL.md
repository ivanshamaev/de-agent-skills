---
name: trino-docker-compose-stack
description: Full local Trino Lakehouse Docker Compose stack — trinodb/trino coordinator+worker, Hive Metastore 3.x with PostgreSQL backend, MinIO object storage (S3-compatible), Apache Airflow, dbt-trino, Apache Superset BI, Prometheus+Grafana monitoring, healthchecks, startup ordering, persistent volumes, isolated networks, catalog property files, Iceberg catalog config, environment variable injection, Trino CLI access
---

# Trino Lakehouse Docker Compose Stack

## When to Use

- Setting up a complete local development environment for Trino-based lakehouse
- Reproducing production issues in an isolated environment
- Onboarding new engineers to the data platform stack
- Running integration tests for Trino SQL, dbt models, or Airflow pipelines

---

## Directory Structure

```
trino-lakehouse/
├── docker-compose.yml
├── .env
├── trino/
│   ├── coordinator/
│   │   ├── config.properties
│   │   ├── jvm.config
│   │   ├── node.properties
│   │   └── log.properties
│   ├── worker/
│   │   ├── config.properties
│   │   ├── jvm.config
│   │   └── node.properties
│   └── catalog/
│       ├── iceberg.properties
│       ├── postgresql.properties
│       └── tpch.properties
├── hive/
│   └── hive-site.xml
├── airflow/
│   └── dags/
├── dbt/
│   ├── profiles.yml
│   └── dbt_project.yml
└── prometheus/
    └── prometheus.yml
```

---

## docker-compose.yml

```yaml
version: "3.9"

networks:
  lakehouse:
    driver: bridge

volumes:
  minio-data:
  postgres-metastore-data:
  postgres-airflow-data:
  trino-data:

services:

  # ── MinIO (S3-compatible object storage) ─────────────────────────
  minio:
    image: quay.io/minio/minio:latest
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"    # Console UI
    environment:
      MINIO_ROOT_USER:     ${MINIO_ROOT_USER:-minioadmin}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-minioadmin}
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
    networks:
      - lakehouse
    healthcheck:
      test:     ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout:  5s
      retries:  5

  minio-init:
    image: quay.io/minio/mc:latest
    container_name: minio-init
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
        mc alias set local http://minio:9000 ${MINIO_ROOT_USER:-minioadmin} ${MINIO_ROOT_PASSWORD:-minioadmin};
        mc mb --ignore-existing local/data-lake;
        mc mb --ignore-existing local/data-lake/bronze;
        mc mb --ignore-existing local/data-lake/silver;
        mc mb --ignore-existing local/data-lake/gold;
        mc mb --ignore-existing local/hive-warehouse;
        echo 'Buckets created';
      "
    networks:
      - lakehouse

  # ── PostgreSQL for Hive Metastore ─────────────────────────────────
  postgres-metastore:
    image: postgres:15-alpine
    container_name: postgres-metastore
    environment:
      POSTGRES_DB:       metastore
      POSTGRES_USER:     hive
      POSTGRES_PASSWORD: hive
    volumes:
      - postgres-metastore-data:/var/lib/postgresql/data
    networks:
      - lakehouse
    healthcheck:
      test:     ["CMD-SHELL", "pg_isready -U hive -d metastore"]
      interval: 10s
      timeout:  5s
      retries:  5

  # ── Hive Metastore ─────────────────────────────────────────────────
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
      SERVICE_NAME:      metastore
      DB_DRIVER:         postgres
      SERVICE_OPTS: >-
        -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
        -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres-metastore:5432/metastore
        -Djavax.jdo.option.ConnectionUserName=hive
        -Djavax.jdo.option.ConnectionPassword=hive
    volumes:
      - ./hive/hive-site.xml:/opt/hive/conf/hive-site.xml:ro
    networks:
      - lakehouse
    healthcheck:
      test:     ["CMD", "nc", "-z", "localhost", "9083"]
      interval: 15s
      timeout:  10s
      retries:  10

  # ── Trino Coordinator ─────────────────────────────────────────────
  trino-coordinator:
    image: trinodb/trino:481
    container_name: trino-coordinator
    depends_on:
      hive-metastore:
        condition: service_healthy
    ports:
      - "8080:8080"
    volumes:
      - ./trino/coordinator/config.properties:/etc/trino/config.properties:ro
      - ./trino/coordinator/jvm.config:/etc/trino/jvm.config:ro
      - ./trino/coordinator/node.properties:/etc/trino/node.properties:ro
      - ./trino/coordinator/log.properties:/etc/trino/log.properties:ro
      - ./trino/catalog:/etc/trino/catalog:ro
      - trino-data:/var/trino/data
    networks:
      - lakehouse
    healthcheck:
      test:     ["CMD", "curl", "-f", "http://localhost:8080/v1/info/state"]
      interval: 15s
      timeout:  10s
      retries:  10

  # ── Trino Worker ──────────────────────────────────────────────────
  trino-worker:
    image: trinodb/trino:481
    container_name: trino-worker
    depends_on:
      trino-coordinator:
        condition: service_healthy
    volumes:
      - ./trino/worker/config.properties:/etc/trino/config.properties:ro
      - ./trino/worker/jvm.config:/etc/trino/jvm.config:ro
      - ./trino/worker/node.properties:/etc/trino/node.properties:ro
      - ./trino/catalog:/etc/trino/catalog:ro
    networks:
      - lakehouse

  # ── Airflow ───────────────────────────────────────────────────────
  postgres-airflow:
    image: postgres:15-alpine
    container_name: postgres-airflow
    environment:
      POSTGRES_DB:       airflow
      POSTGRES_USER:     airflow
      POSTGRES_PASSWORD: airflow
    volumes:
      - postgres-airflow-data:/var/lib/postgresql/data
    networks:
      - lakehouse
    healthcheck:
      test:     ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 10s
      timeout:  5s
      retries:  5

  airflow:
    image: apache/airflow:2.9.2
    container_name: airflow
    depends_on:
      postgres-airflow:
        condition: service_healthy
      trino-coordinator:
        condition: service_healthy
    ports:
      - "8081:8080"
    environment:
      AIRFLOW__CORE__EXECUTOR:              LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:  postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow
      AIRFLOW__CORE__FERNET_KEY:            ${AIRFLOW_FERNET_KEY:-46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLt0nIo3fI=}
      AIRFLOW__CORE__LOAD_EXAMPLES:         "false"
      AIRFLOW_CONN_TRINO_DEFAULT:           "trino://airflow@trino-coordinator:8080/iceberg"
    volumes:
      - ./airflow/dags:/opt/airflow/dags
    command: >
      bash -c "airflow db upgrade &&
               airflow users create --username admin --password admin
                 --firstname Admin --lastname User --role Admin
                 --email admin@example.com &&
               airflow webserver & airflow scheduler"
    networks:
      - lakehouse

  # ── Apache Superset ───────────────────────────────────────────────
  superset:
    image: apache/superset:latest
    container_name: superset
    depends_on:
      trino-coordinator:
        condition: service_healthy
    ports:
      - "8088:8088"
    environment:
      SUPERSET_SECRET_KEY: ${SUPERSET_SECRET_KEY:-superset_secret}
    command: >
      bash -c "pip install trino sqlalchemy-trino &&
               superset db upgrade &&
               superset fab create-admin --username admin --firstname Admin
                 --lastname User --email admin@superset.local --password admin &&
               superset init &&
               superset run -p 8088 --host 0.0.0.0"
    networks:
      - lakehouse

  # ── Prometheus + Grafana ──────────────────────────────────────────
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    networks:
      - lakehouse

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    depends_on:
      - prometheus
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_SECURITY_ADMIN_USER:     admin
    networks:
      - lakehouse
```

---

## Configuration Files

### trino/coordinator/config.properties

```properties
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080
discovery.uri=http://trino-coordinator:8080
query.max-memory=2GB
query.max-memory-per-node=1GB
query.max-total-memory=4GB
```

### trino/coordinator/jvm.config

```
-server
-Xmx4G
-XX:InitialRAMPercentage=80
-XX:MaxRAMPercentage=80
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:ReservedCodeCacheSize=512M
-Djdk.attach.allowAttachSelf=true
```

### trino/coordinator/node.properties

```properties
node.environment=local
node.id=coordinator-1
node.data-dir=/var/trino/data
```

### trino/worker/config.properties

```properties
coordinator=false
http-server.http.port=8080
discovery.uri=http://trino-coordinator:8080
query.max-memory-per-node=1GB
```

### trino/catalog/iceberg.properties

```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.path-style-access=true
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
hive.s3.ssl.enabled=false
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
```

### trino/catalog/postgresql.properties

```properties
connector.name=postgresql
connection-url=jdbc:postgresql://postgres-metastore:5432/appdb
connection-user=hive
connection-password=hive
```

### hive/hive-site.xml

```xml
<configuration>
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
    <value>minioadmin</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>s3a://data-lake/hive-warehouse</value>
  </property>
</configuration>
```

### prometheus/prometheus.yml

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: trino-coordinator
    static_configs:
      - targets: ['trino-coordinator:9090']
    metrics_path: /metrics
  - job_name: trino-worker
    static_configs:
      - targets: ['trino-worker:9090']
```

---

## .env File

```bash
# .env
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
AIRFLOW_FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLt0nIo3fI=
SUPERSET_SECRET_KEY=superset_secret_change_me
```

---

## Quick Start

```bash
# Start everything (first run takes 3–5 min)
docker compose up -d

# Check health
docker compose ps

# Connect Trino CLI
docker exec -it trino-coordinator trino --catalog iceberg --schema bronze

# Create a test schema
docker exec -it trino-coordinator trino --execute \
  "CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3://data-lake/bronze/')"

# Run dbt against local stack
cd dbt && dbt run --target dev

# Access UIs:
# Trino:      http://localhost:8080
# Airflow:    http://localhost:8081  (admin/admin)
# MinIO:      http://localhost:9001  (minioadmin/minioadmin)
# Superset:   http://localhost:8088  (admin/admin)
# Grafana:    http://localhost:3000  (admin/admin)
# Prometheus: http://localhost:9090

# Stop stack
docker compose down

# Full cleanup including volumes
docker compose down -v
```

---

## dbt profiles.yml for Local Stack

```yaml
# dbt/profiles.yml
data_platform:
  target: dev
  outputs:
    dev:
      type: trino
      method: none
      host: localhost
      port: 8080
      database: iceberg
      schema: dev_local
      threads: 4
      http_scheme: http
```

---

## Anti-Patterns

1. **No `depends_on` with `condition: service_healthy`** — without healthcheck conditions, Trino starts before Hive Metastore is ready, causing catalog registration failures; always use health-conditioned depends_on.
2. **Mounting `/etc/trino` as a single directory** — if the directory is missing or wrong, Trino silently uses defaults; mount individual files with `:ro` to make missing config obvious.
3. **Using `latest` image tags** — `trinodb/trino:latest` pulls different versions across team members; pin to a specific version (e.g., `481`) for reproducibility.
4. **Sharing the network with host** — `network_mode: host` exposes all container ports on the host; use a named bridge network for proper service discovery and isolation.
5. **Skipping minio-init** — without creating the buckets first, Iceberg table creation fails; minio-init as a `service_completed_successfully` dependency is the clean pattern.

---

## References

- Trino containers: `trino.io/docs/current/installation/containers.html`
- trinodb/trino Docker Hub: `hub.docker.com/r/trinodb/trino`
- Related skills: `[[trino-lakehouse-platform-architect]]`, `[[trino-iceberg-best-practices]]`, `[[trino-dbt-platform]]`
