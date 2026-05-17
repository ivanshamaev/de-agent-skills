---
name: docker-data-environments
description: Docker for data engineering environments — multi-stage Dockerfiles for dbt/Spark/Airflow images, BuildKit layer caching (--mount=type=cache), private registries (ghcr.io/Harbor), docker buildx multi-platform builds, Docker Compose local data stacks (Spark+Airflow+Kafka+MinIO+Postgres), BuildKit secrets for private PyPI, security hardening (non-root user, slim base images, read-only FS), CI/CD with GitHub Actions build-push-action
---

# Docker for Data Environments

## When to Use

Load this skill when the user needs to:
- Write or review Dockerfiles for data tools (dbt, Spark, Airflow, Flink, Trino, etc.)
- Optimize Docker layer caching for faster CI/CD builds
- Push/pull images to private registries (ghcr.io, Harbor) with proper tagging
- Build multi-platform images with `docker buildx` for amd64/arm64
- Stand up a local data stack with Docker Compose (Spark, Airflow, Kafka, MinIO, Postgres)
- Securely pass private PyPI tokens or other secrets at build time with BuildKit
- Harden container images: non-root user, minimal base, read-only filesystem, `.dockerignore`
- Wire up GitHub Actions CI/CD pipelines that build and push data tool images

---

## Multi-Stage Dockerfiles for Data Tools

### dbt Image

Build: Python base, install adapters from `requirements.txt`, copy project last.

```dockerfile
# syntax=docker/dockerfile:1.7
ARG PYTHON_VERSION=3.11
ARG DBT_CORE_VERSION=1.8.*
ARG DBT_TRINO_VERSION=1.8.*

# ── build stage ──────────────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim AS builder

WORKDIR /build

# Copy dependency manifest first — this layer is cached until requirements change
COPY requirements.txt .

# BuildKit cache mount: pip cache survives across builds on the same host
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip \
    && pip install \
        dbt-core==${DBT_CORE_VERSION} \
        dbt-trino==${DBT_TRINO_VERSION} \
        dbt-postgres \
        -r requirements.txt \
    --no-cache-dir \
    --prefix=/install

# ── runtime stage ─────────────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/install/bin:$PATH" \
    PYTHONPATH="/install/lib/python3.11/site-packages"

# Non-root user
RUN groupadd --gid 1001 dbt \
    && useradd --uid 1001 --gid dbt --shell /bin/bash --create-home dbt

WORKDIR /dbt

# Copy installed packages from builder
COPY --from=builder /install /install

# Copy project files last (most frequently changed)
COPY --chown=dbt:dbt . .

USER dbt

ENTRYPOINT ["dbt"]
CMD ["--version"]
```

`requirements.txt` example:
```
dbt-core==1.8.*
dbt-trino==1.8.*
dbt-utils==1.3.*
```

### Spark Image

Build: OpenJDK slim, add JARs, install PySpark + Python dependencies.

```dockerfile
# syntax=docker/dockerfile:1.7
ARG JAVA_VERSION=17
ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3
ARG SCALA_VERSION=2.12

# ── spark download stage ──────────────────────────────────────────────────────
FROM eclipse-temurin:${JAVA_VERSION}-jre-jammy AS spark-download

ARG SPARK_VERSION
ARG HADOOP_VERSION

WORKDIR /opt

RUN apt-get update && apt-get install -y --no-install-recommends curl tar \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
    "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xzf - \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

# ── extra JARs stage ──────────────────────────────────────────────────────────
FROM spark-download AS jars

ARG SCALA_VERSION

# Download connector JARs into the Spark jars directory
RUN curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_${SCALA_VERSION}-1.6.1.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/1.6.1/iceberg-spark-runtime-3.5_${SCALA_VERSION}-1.6.1.jar" \
 && curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

# ── python deps stage ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS pydeps

COPY requirements.txt .

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install pyspark==3.5.3 delta-spark==3.2.0 -r requirements.txt \
    --no-cache-dir \
    --prefix=/pyinstall

# ── runtime stage ─────────────────────────────────────────────────────────────
FROM eclipse-temurin:${JAVA_VERSION}-jre-jammy AS runtime

ENV SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:/opt/spark/sbin:$PATH" \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH="/pyinstall/lib/python3.11/site-packages" \
    PYTHONUNBUFFERED=1

# Install Python runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-distutils \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1001 spark \
    && useradd --uid 1001 --gid spark --shell /bin/bash --create-home spark

COPY --from=jars /opt/spark /opt/spark
COPY --from=pydeps /pyinstall /pyinstall

WORKDIR /opt/spark

USER spark

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
```

### Airflow Custom Image

Extend the official image with custom providers and packages.

```dockerfile
# syntax=docker/dockerfile:1.7
ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION} AS base

# Switch to root only for system-level packages
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy constraints/requirements before project files
COPY --chown=airflow:root requirements-airflow.txt .

# Use pip cache mount + official constraints
ARG CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN --mount=type=cache,target=/home/airflow/.cache/pip,uid=50000,gid=0 \
    pip install --no-cache-dir \
        -r requirements-airflow.txt \
        --constraint "${CONSTRAINTS_URL}"

# Copy DAGs and plugins last
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/
```

`requirements-airflow.txt` example:
```
apache-airflow-providers-trino==5.7.3
apache-airflow-providers-amazon==8.26.0
apache-airflow-providers-databricks==6.8.0
dbt-core==1.8.*
dbt-trino==1.8.*
```

---

## Layer Caching Strategy

**Rule: order instructions from least-changed to most-changed.**

```
FROM                         ← changes only when base image bumps (rare)
ARG / ENV (build-time only)  ← rarely changes
RUN apt-get install          ← changes when system deps change
COPY requirements*.txt .     ← changes when adding/removing packages
RUN pip install              ← invalidated only when requirements change
COPY . .                     ← changes on every source code edit
RUN <compile/test step>
```

Use `--mount=type=cache` so pip and apt caches survive cache misses:

```dockerfile
# apt cache (persisted at /var/cache/apt)
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends gcc

# pip cache
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt
```

Separate stable and volatile requirements files:
```
requirements-base.txt    # numpy, pandas, pyarrow — change rarely
requirements-app.txt     # project-specific packages — change often
```

```dockerfile
COPY requirements-base.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements-base.txt

COPY requirements-app.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements-app.txt

COPY . .
```

---

## Private Registry: ghcr.io and Harbor

### Image Tagging Strategy

Use a combination of semantic version and git SHA for full traceability:

```
ghcr.io/org/spark-etl:3.5.3-abc1234   # semver + short SHA (immutable)
ghcr.io/org/spark-etl:3.5.3           # semver alias (updated on release)
ghcr.io/org/spark-etl:latest          # mutable pointer (dev use only)

harbor.internal.corp/data/dbt:1.8.2-abc1234
```

### Push/Pull — ghcr.io

```bash
# Authenticate
echo "${GITHUB_TOKEN}" | docker login ghcr.io -u "${GITHUB_ACTOR}" --password-stdin

# Build with multi-platform
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --tag ghcr.io/org/dbt-trino:1.8.2-$(git rev-parse --short HEAD) \
  --tag ghcr.io/org/dbt-trino:1.8.2 \
  --push \
  .

# Pull
docker pull ghcr.io/org/dbt-trino:1.8.2-abc1234
```

### Push/Pull — Harbor (self-hosted)

```bash
# Authenticate
docker login harbor.internal.corp \
  --username "${HARBOR_USER}" \
  --password "${HARBOR_PASSWORD}"

# Tag and push
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --tag harbor.internal.corp/data/spark-etl:3.5.3-$(git rev-parse --short HEAD) \
  --cache-from type=registry,ref=harbor.internal.corp/data/spark-etl:buildcache \
  --cache-to   type=registry,ref=harbor.internal.corp/data/spark-etl:buildcache,mode=max \
  --push \
  .
```

### docker buildx Multi-Platform Setup

```bash
# Create and use a multi-platform builder (run once per CI runner or machine)
docker buildx create \
  --name multiarch-builder \
  --driver docker-container \
  --use

docker buildx inspect --bootstrap

# Build for both architectures, push directly to registry
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --file Dockerfile.spark \
  --tag ghcr.io/org/spark-etl:${VERSION}-${GIT_SHA} \
  --tag ghcr.io/org/spark-etl:${VERSION} \
  --push \
  .
```

---

## Docker Compose for Local Data Stacks

Complete `compose.yaml` for a local Spark + Airflow + Postgres + MinIO + Kafka + Schema Registry stack:

```yaml
# compose.yaml  — local data engineering stack
# Usage: docker compose up -d
# Requires: Docker Compose v2.20+

x-airflow-common: &airflow-common
  image: ${AIRFLOW_IMAGE:-apache/airflow:2.10.4-python3.11}
  environment: &airflow-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    AIRFLOW__CORE__FERNET_KEY: "${FERNET_KEY:?FERNET_KEY env var required}"
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__WEBSERVER__SECRET_KEY: "${WEBSERVER_SECRET_KEY:?required}"
    # S3-compatible storage (MinIO)
    AWS_ACCESS_KEY_ID: minioadmin
    AWS_SECRET_ACCESS_KEY: minioadmin
    AWS_DEFAULT_REGION: us-east-1
    AIRFLOW_CONN_MINIO_DEFAULT: >-
      aws://minioadmin:minioadmin@?endpoint_url=http%3A%2F%2Fminio%3A9000&region_name=us-east-1
  volumes:
    - ./dags:/opt/airflow/dags
    - ./plugins:/opt/airflow/plugins
    - airflow-logs:/opt/airflow/logs
  depends_on:
    postgres:
      condition: service_healthy
  networks:
    - data-net

services:
  # ── Postgres (Airflow metadata + general use) ──────────────────────────────
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./init-db:/docker-entrypoint-initdb.d  # extra DBs for dbt etc.
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 10s
    networks:
      - data-net

  # ── Airflow ────────────────────────────────────────────────────────────────
  airflow-init:
    <<: *airflow-common
    command: >
      bash -c "
        airflow db migrate &&
        airflow users create
          --username admin --password admin
          --firstname Admin --lastname Admin
          --role Admin --email admin@example.com
      "
    restart: "no"

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: unless-stopped

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", "airflow jobs check --job-type SchedulerJob --local"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s
    restart: unless-stopped

  # ── Spark ─────────────────────────────────────────────────────────────────
  spark-master:
    image: ${SPARK_IMAGE:-bitnami/spark:3.5.3}
    environment:
      SPARK_MODE: master
      SPARK_MASTER_HOST: spark-master
      SPARK_RPC_AUTHENTICATION_ENABLED: "no"
      SPARK_RPC_ENCRYPTION_ENABLED: "no"
      SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED: "no"
      SPARK_SSL_ENABLED: "no"
    ports:
      - "7077:7077"
      - "8081:8080"
    volumes:
      - spark-work:/opt/bitnami/spark/work
      - ./spark/conf:/opt/bitnami/spark/conf:ro
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080"]
      interval: 15s
      timeout: 5s
      retries: 5
    networks:
      - data-net

  spark-worker:
    image: ${SPARK_IMAGE:-bitnami/spark:3.5.3}
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 2G
      SPARK_WORKER_CORES: 2
    volumes:
      - spark-work:/opt/bitnami/spark/work
    depends_on:
      spark-master:
        condition: service_healthy
    deploy:
      replicas: 2
    networks:
      - data-net

  # ── MinIO (S3-compatible object storage) ──────────────────────────────────
  minio:
    image: minio/minio:RELEASE.2024-11-07T00-52-20Z
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 15s
      timeout: 5s
      retries: 5
      start_period: 10s
    networks:
      - data-net

  # Create default buckets on startup
  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
        mc alias set local http://minio:9000 minioadmin minioadmin;
        mc mb --ignore-existing local/raw-data;
        mc mb --ignore-existing local/processed;
        mc mb --ignore-existing local/iceberg-warehouse;
      "
    networks:
      - data-net
    restart: "no"

  # ── Kafka ─────────────────────────────────────────────────────────────────
  zookeeper:
    image: confluentinc/cp-zookeeper:7.7.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zk-data:/var/lib/zookeeper/data
      - zk-log:/var/lib/zookeeper/log
    healthcheck:
      test: ["CMD", "echo", "ruok", "|", "nc", "localhost", "2181"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - data-net

  kafka:
    image: confluentinc/cp-kafka:7.7.1
    depends_on:
      zookeeper:
        condition: service_healthy
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_RETENTION_HOURS: 24
    ports:
      - "9092:9092"
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 15s
      timeout: 10s
      retries: 5
      start_period: 20s
    networks:
      - data-net

  # ── Schema Registry ───────────────────────────────────────────────────────
  schema-registry:
    image: confluentinc/cp-schema-registry:7.7.1
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8082
    ports:
      - "8082:8082"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8082/subjects"]
      interval: 15s
      timeout: 5s
      retries: 5
      start_period: 15s
    networks:
      - data-net

# ── Volumes ────────────────────────────────────────────────────────────────
volumes:
  postgres-data:
  airflow-logs:
  spark-work:
  minio-data:
  kafka-data:
  zk-data:
  zk-log:

# ── Networks ───────────────────────────────────────────────────────────────
networks:
  data-net:
    driver: bridge
    ipam:
      config:
        - subnet: 172.28.0.0/16
```

Usage:
```bash
# Generate Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
export FERNET_KEY=<output>
export WEBSERVER_SECRET_KEY=$(openssl rand -hex 32)

docker compose up -d
docker compose logs -f airflow-scheduler
docker compose ps   # verify all services healthy
```

---

## BuildKit Secrets for Private PyPI

Never bake credentials into image layers. Use `--secret` to mount them as ephemeral files available only during a `RUN` step.

```dockerfile
# syntax=docker/dockerfile:1.7

FROM python:3.11-slim AS builder

WORKDIR /build

COPY requirements.txt .

# The secret is mounted at /run/secrets/pip_token only during this step.
# It does NOT appear in any image layer or in `docker history`.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=secret,id=pip_token \
    PIP_INDEX_URL="https://$(cat /run/secrets/pip_token)@private.pypi.corp/simple/" \
    pip install -r requirements.txt \
    --no-cache-dir \
    --prefix=/install
```

Build invocation:
```bash
# Pass the token value from an environment variable
docker buildx build \
  --secret id=pip_token,env=PIP_TOKEN \
  --tag myorg/dbt-custom:1.0.0 \
  .

# Or pass from a file
docker buildx build \
  --secret id=pip_token,src=./secrets/pip_token.txt \
  --tag myorg/dbt-custom:1.0.0 \
  .
```

For `.netrc` style credentials (PEP 440 index with auth):
```dockerfile
RUN --mount=type=secret,id=netrc,target=/root/.netrc,mode=0600 \
    pip install --no-cache-dir -r requirements.txt --prefix=/install
```

```bash
# netrc content: machine private.pypi.corp login __token__ password $TOKEN
docker buildx build \
  --secret id=netrc,src="${HOME}/.netrc" \
  .
```

---

## Security Hardening

### Non-Root User

```dockerfile
# Create a locked system account with no shell — principle of least privilege
RUN groupadd --system --gid 1001 appgroup \
    && useradd --system --uid 1001 --gid appgroup \
       --no-create-home --shell /sbin/nologin appuser

# Assign ownership before switching user
COPY --chown=appuser:appgroup . /app

USER appuser
```

Always use numeric UID/GID for Kubernetes `runAsUser` / `runAsGroup` compatibility:
```yaml
# Kubernetes securityContext equivalents
securityContext:
  runAsUser: 1001
  runAsGroup: 1001
  runAsNonRoot: true
  readOnlyRootFilesystem: true
```

### Minimal Base Images

| Base | Compressed size | Use case |
|------|----------------|----------|
| `python:3.11` | ~350 MB | Avoid — full Debian, large attack surface |
| `python:3.11-slim` | ~45 MB | Standard choice for most data tools |
| `python:3.11-slim-bookworm` | ~45 MB | Pinned Debian release — preferred for reproducibility |
| `gcr.io/distroless/python3` | ~20 MB | No shell — use when hardest security posture needed |
| `eclipse-temurin:17-jre-jammy` | ~220 MB | JVM-based tools (Spark, Flink) |

Always pin the full image digest in production:
```dockerfile
FROM python:3.11-slim-bookworm@sha256:<digest> AS runtime
```

### pip Flags

```dockerfile
# --no-cache-dir: do not write pip's HTTP cache into the image layer
# --no-compile:   skip .pyc creation (saves space, Python recompiles on import)
RUN pip install --no-cache-dir --no-compile -r requirements.txt
```

When using `--mount=type=cache`, `--no-cache-dir` prevents pip from writing INTO the image layer while the bind-mounted cache still accelerates downloads.

### .dockerignore

```
# Version control
.git
.gitignore

# Python
__pycache__
*.py[cod]
*.egg-info
.eggs
dist
build
.venv
venv

# Secrets / credentials
.env
*.env
secrets/
**/.aws
**/.netrc

# CI / local tooling
.github
.tox
.mypy_cache
.ruff_cache
.pytest_cache
htmlcov
.coverage
docs/_build

# Large artifacts that should not be in context
*.parquet
*.csv
*.json.gz
data/
```

### Read-Only Runtime Filesystem

```bash
# Identify which paths need to be writable at runtime, then allow only those
docker run \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=256m \
  --tmpfs /var/run:rw,noexec,nosuid \
  myorg/dbt-custom:1.0.0 \
  run --profiles-dir /dbt --target prod
```

In Docker Compose:
```yaml
services:
  dbt:
    image: myorg/dbt-custom:1.0.0
    read_only: true
    tmpfs:
      - /tmp:mode=1777,size=268435456
```

---

## CI/CD Integration — GitHub Actions

### Single-Image Build and Push

```yaml
# .github/workflows/build-dbt.yml
name: Build dbt Image

on:
  push:
    branches: [main]
    tags: ["v*.*.*"]
  pull_request:
    branches: [main]

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository_owner }}/dbt-trino

jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
      id-token: write   # for OIDC signing (optional)

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Log in to GHCR
        if: github.event_name != 'pull_request'
        uses: docker/login-action@v3
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Extract metadata (tags, labels)
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
          tags: |
            # semver from git tag: v1.8.2 → 1.8.2
            type=semver,pattern={{version}}
            type=semver,pattern={{major}}.{{minor}}
            # short git SHA on every push
            type=sha,prefix=sha-,format=short
            # branch name on PRs
            type=ref,event=branch
            # latest only on main
            type=raw,value=latest,enable={{is_default_branch}}

      - name: Build and push
        uses: docker/build-push-action@v6
        with:
          context: .
          file: docker/Dockerfile.dbt
          platforms: linux/amd64,linux/arm64
          push: ${{ github.event_name != 'pull_request' }}
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          # Persist layer cache in the registry between runs
          cache-from: type=registry,ref=${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:buildcache
          cache-to:   type=registry,ref=${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:buildcache,mode=max
          # Pass private PyPI token at build time (not baked into layers)
          secrets: |
            pip_token=${{ secrets.PRIVATE_PYPI_TOKEN }}
```

### Matrix Build — Multiple Data Tool Images

```yaml
# .github/workflows/build-matrix.yml
name: Build Data Tool Images

on:
  push:
    branches: [main]
    tags: ["v*.*.*"]

jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    strategy:
      fail-fast: false
      matrix:
        include:
          - image: dbt-trino
            dockerfile: docker/Dockerfile.dbt
            context: .
          - image: spark-etl
            dockerfile: docker/Dockerfile.spark
            context: .
          - image: airflow-custom
            dockerfile: docker/Dockerfile.airflow
            context: .

    steps:
      - uses: actions/checkout@v4

      - uses: docker/setup-buildx-action@v3

      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - id: meta
        uses: docker/metadata-action@v5
        with:
          images: ghcr.io/${{ github.repository_owner }}/${{ matrix.image }}
          tags: |
            type=semver,pattern={{version}}
            type=sha,prefix=sha-,format=short
            type=raw,value=latest,enable={{is_default_branch}}

      - uses: docker/build-push-action@v6
        with:
          context: ${{ matrix.context }}
          file: ${{ matrix.dockerfile }}
          platforms: linux/amd64,linux/arm64
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          cache-from: type=registry,ref=ghcr.io/${{ github.repository_owner }}/${{ matrix.image }}:buildcache
          cache-to:   type=registry,ref=ghcr.io/${{ github.repository_owner }}/${{ matrix.image }}:buildcache,mode=max
          secrets: |
            pip_token=${{ secrets.PRIVATE_PYPI_TOKEN }}
```

### Push to Harbor (self-hosted)

```yaml
      - name: Log in to Harbor
        uses: docker/login-action@v3
        with:
          registry: harbor.internal.corp
          username: ${{ secrets.HARBOR_USER }}
          password: ${{ secrets.HARBOR_PASSWORD }}

      - name: Build and push to Harbor
        uses: docker/build-push-action@v6
        with:
          context: .
          platforms: linux/amd64,linux/arm64
          push: true
          tags: harbor.internal.corp/data/${{ matrix.image }}:${{ steps.meta.outputs.version }}
          cache-from: type=registry,ref=harbor.internal.corp/data/${{ matrix.image }}:buildcache
          cache-to:   type=registry,ref=harbor.internal.corp/data/${{ matrix.image }}:buildcache,mode=max
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| `COPY . .` before `pip install` | Every source change invalidates the pip layer | Copy `requirements.txt` first, then `COPY . .` |
| `RUN pip install` without `--no-cache-dir` (when not using cache mounts) | pip HTTP cache baked into the layer, bloating the image | Add `--no-cache-dir`, or use `--mount=type=cache` |
| `ENV SECRET_KEY=abc123` in Dockerfile | Secret baked into every image layer, visible in `docker history` | Use `--secret` with `--mount=type=secret` |
| Single-stage Dockerfile with compiler toolchain in runtime image | Compilers, headers, and build tools inflate the image | Use multi-stage; copy only `/install` into runtime |
| `USER root` in production images | Full root inside container — container escape = host compromise | Create a non-root system user, switch with `USER` |
| `latest` tag in production | Non-reproducible; rollback is guesswork | Pin to `semver+SHA` tag; treat `latest` as dev-only |
| Baking data files or model artifacts into the image | Image bloat; secrets or PII in registry | Mount data at runtime; use volumes or object storage |
| No `.dockerignore` | Entire repo (`.git`, `venv`, `data/`) sent as build context | Always provide a `.dockerignore` |
| `apt-get update` and `apt-get install` in separate `RUN` layers | Stale apt cache; install may fail on cache reuse | Combine into one `RUN` or use `--mount=type=cache` |
| Ignoring platform mismatch | Apple Silicon (arm64) images fail silently on amd64 CI | Always build with `--platform linux/amd64,linux/arm64` |
| Storing large JARs in Git and `COPY`-ing them | Large build context; JARs must be versioned separately | Download JARs in a dedicated stage using `curl` from Maven Central |

---

## References to Consult When Needed

- [Docker Docs — Multi-stage builds](https://docs.docker.com/build/building/multi-stage/)
- [Docker Docs — BuildKit secrets](https://docs.docker.com/build/building/secrets/)
- [Docker Docs — Multi-platform builds with GitHub Actions](https://docs.docker.com/build/ci/github-actions/multi-platform/)
- [Docker Docs — Base image hardening](https://docs.docker.com/dhi/core-concepts/hardening/)
- [docker/build-push-action — GitHub Marketplace](https://github.com/marketplace/actions/build-and-push-docker-images)
- [docker/metadata-action — GitHub Marketplace](https://github.com/marketplace/actions/docker-metadata-action)
- [Sysdig — Top 21 Dockerfile best practices](https://www.sysdig.com/learn-cloud-native/dockerfile-best-practices)
- [Harbor Documentation](https://goharbor.io/docs/)
