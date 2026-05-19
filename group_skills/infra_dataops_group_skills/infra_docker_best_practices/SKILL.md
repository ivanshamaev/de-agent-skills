---
name: infra-docker-best-practices
description: Docker best practices — multi-stage builds (builder/runtime separation), layer cache optimization (dependency install before source copy), minimal base images (distroless/alpine/slim), security hardening (non-root USER, read-only FS, no SUID binaries), .dockerignore patterns, BuildKit secrets for private registries, image vulnerability scanning (Trivy), COPY vs ADD, CMD vs ENTRYPOINT patterns, data engineering Dockerfiles (dbt/Spark/Python ETL)
---

# Docker Best Practices

## When to Use

- Building production Docker images for data pipelines
- Reducing image size to speed up CI/CD and reduce registry costs
- Hardening images against CVE exposure
- Debugging slow Docker builds (cache invalidation)
- Reviewing Dockerfiles in code review

---

## Multi-Stage Builds

### Pattern: Builder + Runtime Stage

```dockerfile
# Stage 1: Build dependencies (heavy — compile, install)
FROM python:3.11-slim AS builder

WORKDIR /app
COPY requirements.txt .

RUN pip install --user --no-cache-dir -r requirements.txt

# Stage 2: Runtime (lean — only what's needed to run)
FROM python:3.11-slim AS runtime

WORKDIR /app

# Copy only installed packages from builder
COPY --from=builder /root/.local /root/.local

# Copy application code
COPY src/ .

# Security: non-root user
RUN useradd -m -u 1001 appuser
USER appuser

ENV PATH=/root/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
```

**Result**: Runtime image excludes pip cache, build tools, and intermediate files.

### Data Engineering: dbt Image

```dockerfile
FROM python:3.11-slim AS builder

ARG DBT_VERSION=1.7.0
ARG DBT_ADAPTER=dbt-trino

RUN pip install --user --no-cache-dir \
    dbt-core==${DBT_VERSION} \
    ${DBT_ADAPTER}==1.7.0

# ─────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Install git (needed for dbt deps)
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /root/.local /root/.local

# Non-root user
RUN useradd -m -u 1001 dbt && mkdir -p /dbt && chown dbt:dbt /dbt
USER dbt
WORKDIR /dbt

ENV PATH=/root/.local/bin:$PATH
ENV DBT_PROFILES_DIR=/dbt/profiles

ENTRYPOINT ["dbt"]
CMD ["--help"]
```

---

## Layer Cache Optimization

### Rule: Copy Dependencies Before Source Code

```dockerfile
# BAD: any code change invalidates pip install cache
COPY . /app
RUN pip install -r /app/requirements.txt

# GOOD: cache pip install separately from source changes
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt
COPY src/ /app/src/        # only invalidates from here
```

### Combine RUN Commands for apt-get

```dockerfile
# BAD: update and install in separate layers
RUN apt-get update
RUN apt-get install -y curl wget git

# GOOD: single layer, clean up cache in same RUN
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*   # removes cached package lists
```

### BuildKit Cache Mounts (Fastest for Dependencies)

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Cache pip packages across builds (host cache, not in image)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements.txt

# Cache apt packages
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y git
```

Enable with: `DOCKER_BUILDKIT=1 docker build .`

---

## Minimal Base Images

| Base Image | Size | Use Case |
|-----------|------|---------|
| `python:3.11-slim` | ~50MB | Most Python apps |
| `python:3.11-alpine` | ~25MB | When musl libc is OK |
| `gcr.io/distroless/python3` | ~15MB | Production, no shell |
| `ubuntu:22.04` | ~70MB | When system packages needed |
| `scratch` | 0MB | Static Go/Rust binaries |

```dockerfile
# Distroless: no shell, no package manager — minimal attack surface
FROM python:3.11-slim AS builder
COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt

FROM gcr.io/distroless/python3-debian11
COPY --from=builder /install /usr/local
COPY src/ /app
WORKDIR /app
CMD ["/app/main.py"]
```

---

## Security Hardening

### Non-Root User

```dockerfile
# Create and use a non-root user
RUN groupadd -r appgroup && useradd -r -g appgroup -u 1001 appuser

# Set ownership on necessary directories
RUN mkdir -p /data && chown appuser:appgroup /data

USER appuser    # all subsequent RUN/CMD/ENTRYPOINT run as this user
```

### Read-Only Root Filesystem

```yaml
# In Kubernetes: enforce in pod spec
securityContext:
  readOnlyRootFilesystem: true
# Add writable emptyDir for /tmp if needed
volumeMounts:
- name: tmp
  mountPath: /tmp
volumes:
- name: tmp
  emptyDir: {}
```

### BuildKit Secrets (Private PyPI / npm)

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Secret never stored in image layer
RUN --mount=type=secret,id=pip_token \
    pip install --extra-index-url \
    https://$(cat /run/secrets/pip_token)@pkgs.company.com/simple/ \
    my-private-package
```

```bash
# Build with secret
docker build \
  --secret id=pip_token,env=PIP_TOKEN \
  -t myimage .
```

---

## .dockerignore

```
# .dockerignore — exclude everything not needed for the build
.git
.gitignore
.env
*.env
__pycache__/
*.pyc
*.pyo
.pytest_cache/
.mypy_cache/
.tox/
dist/
build/
*.egg-info/
docs/
tests/
*.md
*.log
.DS_Store
node_modules/
.venv/
venv/
```

---

## CMD vs ENTRYPOINT

```dockerfile
# ENTRYPOINT: fixed executable, CMD: default args
ENTRYPOINT ["python", "-m", "gunicorn"]
CMD ["--workers=4", "--bind=0.0.0.0:8080", "app:app"]

# Override CMD at runtime:
# docker run myimage --workers=8 --bind=0.0.0.0:8080 app:app

# Use exec form (not shell form) — proper signal handling
CMD ["python", "main.py"]        # exec form: PID 1 receives SIGTERM
# NOT: CMD python main.py        # shell form: /bin/sh -c wraps it, signal lost
```

---

## Spark ETL Image Example

```dockerfile
# syntax=docker/dockerfile:1
FROM openjdk:11-jre-slim AS base

ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3

# Install Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl procps tini \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
       | tar -xz -C /opt/ \
    && ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Python layer
FROM base AS python

RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY --mount=type=cache,target=/root/.cache/pip requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Runtime
FROM python AS runtime

RUN useradd -m -u 1001 spark
USER spark
WORKDIR /app

COPY --chown=spark:spark src/ .

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/opt/spark/bin/spark-submit", "--help"]
```

---

## Image Scanning in CI

```yaml
# GitHub Actions: scan with Trivy
- name: Build image
  run: docker build -t $IMAGE_NAME .

- name: Scan with Trivy
  uses: aquasecurity/trivy-action@master
  with:
    image-ref: ${{ env.IMAGE_NAME }}
    format: 'sarif'
    output: 'trivy-results.sarif'
    severity: 'CRITICAL,HIGH'
    exit-code: '1'             # fail CI on CRITICAL/HIGH

- name: Upload Trivy results to GitHub Security tab
  uses: github/codeql-action/upload-sarif@v3
  with:
    sarif_file: 'trivy-results.sarif'
```

---

## Image Size Audit

```bash
# Analyze image layers
docker history myimage:latest

# Using dive (interactive layer explorer)
dive myimage:latest

# Check final image size
docker images myimage:latest --format "{{.Size}}"
```

---

## Anti-Patterns

1. **`COPY . .` before dependency install** — every code change busts the pip/npm cache; copy lockfiles first.
2. **`RUN apt-get update` in separate layer** — stale cache causes `apt-get install` to fail; always combine in one RUN.
3. **Running as root** — if the container is compromised, attacker has root; always set `USER nonroot`.
4. **`:latest` tag in production** — non-deterministic; use specific digests or version tags.
5. **Including `.git` and secrets in build context** — add to `.dockerignore`; `.env` files often contain credentials.
6. **Shell form for CMD/ENTRYPOINT** — `CMD python app.py` makes shell PID 1, which doesn't forward signals; use exec form.

---

## References

- Docker Dockerfile best practices: `docs.docker.com/develop/develop-images/dockerfile_best-practices/`
- BuildKit: `docs.docker.com/build/buildkit/`
- Distroless: `github.com/GoogleContainerTools/distroless`
- Trivy: `trivy.dev/docs/`
- Related skills: `[[infra-kubernetes-security-audit]]`, `[[github-actions-dataops]]`, `[[docker-data-environments]]`
