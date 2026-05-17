---
name: github-actions-dataops
description: Use when building or reviewing GitHub Actions CI/CD pipelines for data engineering — covering dbt slim CI (state:modified+, --defer, manifest.json), SQLFluff SQL linting with PR annotations, Airflow DAG integrity tests (pytest/DagBag), Great Expectations and Soda data quality gates, multi-stage Docker image builds for Spark/dbt/Airflow pushed to ghcr.io, secrets management with OIDC for AWS/GCP (no static keys), environment-scoped secrets, reusable workflows (workflow_call), matrix builds, composite actions, and full end-to-end dbt project CI/CD workflows.
---

# GitHub Actions for DataOps / CI/CD

## When to Use

Load this skill when the user needs to:
- Build or review GitHub Actions workflows for dbt, Airflow, Spark, or other data tools
- Set up slim CI for dbt (`state:modified+`, `--defer`, `manifest.json` artifact passing)
- Lint SQL with SQLFluff and surface inline PR annotations
- Validate Airflow DAGs in CI using pytest / DagBag checks
- Run Great Expectations or Soda as a data quality gate in CI
- Build and push Docker images for data tools to ghcr.io with layer caching
- Manage secrets securely — OIDC for AWS/GCP, no static credentials
- Create reusable workflows, matrix builds, and composite actions
- Deploy dbt docs to GitHub Pages

---

## Core Workflow Anatomy

```yaml
# Minimal structure — every workflow follows this skeleton
name: <workflow-name>

on:
  pull_request:
    branches: [main]
    paths:                        # only trigger when relevant files change
      - 'dbt/**'
      - '.github/workflows/**'

permissions:
  contents: read                  # least-privilege default
  id-token: write                 # required for OIDC token request
  pull-requests: write            # required for PR annotations / comments

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true        # cancel stale runs on new push

jobs:
  my-job:
    runs-on: ubuntu-24.04         # pin major version, not "latest"
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0          # needed for state:modified comparisons
```

---

## dbt CI/CD

### Slim CI — `state:modified+` with `--defer`

Slim CI runs only the models that changed (and their downstream dependents) by comparing the PR's `manifest.json` against the production one. `--defer` means unmodified upstream models are resolved from the production environment, so the CI database stays lean.

```yaml
# .github/workflows/dbt-ci.yml
name: dbt CI

on:
  pull_request:
    branches: [main]
    paths:
      - 'dbt/**'
      - '.github/workflows/dbt-ci.yml'

permissions:
  contents: read
  id-token: write          # OIDC → AWS/GCP
  pull-requests: write

concurrency:
  group: dbt-ci-${{ github.ref }}
  cancel-in-progress: true

env:
  DBT_VERSION: "1.8.7"
  DBT_ADAPTER: "dbt-trino==1.8.5"

jobs:
  dbt-slim-ci:
    runs-on: ubuntu-24.04
    timeout-minutes: 45

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      # --- Auth (OIDC, no static keys) ---
      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::${{ vars.AWS_ACCOUNT_ID }}:role/github-actions-dbt
          aws-region: eu-central-1

      # --- Python + dbt ---
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install dbt
        run: |
          pip install --upgrade pip
          pip install ${{ env.DBT_ADAPTER }} "dbt-core==${{ env.DBT_VERSION }}"

      - name: Install dbt packages
        working-directory: dbt
        run: dbt deps

      # --- Download production manifest ---
      - name: Download prod manifest
        run: |
          aws s3 cp s3://${{ vars.DBT_ARTIFACTS_BUCKET }}/prod/manifest.json \
            ./dbt/prod-artifacts/manifest.json || true
          # "|| true" → first run when no prod artifact exists yet

      # --- Compile (catches syntax errors fast) ---
      - name: dbt compile
        working-directory: dbt
        env:
          DBT_TARGET_HOST: ${{ secrets.TRINO_HOST }}
          DBT_TARGET_USER: ${{ secrets.TRINO_USER }}
        run: |
          dbt compile \
            --select "state:modified+" \
            --state ./prod-artifacts/ \
            --target ci

      # --- Build (run + test) only changed + downstream ---
      - name: dbt build — slim CI
        working-directory: dbt
        env:
          DBT_TARGET_HOST: ${{ secrets.TRINO_HOST }}
          DBT_TARGET_USER: ${{ secrets.TRINO_USER }}
        run: |
          dbt build \
            --select "state:modified+" \
            --state ./prod-artifacts/ \
            --target ci \
            --defer \
            --favor-state              # prefer prod results for unmodified nodes

      # --- Upload CI manifest as artifact for PR comparison ---
      - name: Upload CI manifest
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: dbt-manifest-ci-${{ github.run_id }}
          path: dbt/target/manifest.json
          retention-days: 7
```

### dbt Docs Deploy to GitHub Pages

```yaml
# .github/workflows/dbt-docs.yml
name: dbt Docs

on:
  push:
    branches: [main]
    paths: ['dbt/**']

permissions:
  contents: write
  pages: write
  id-token: write

jobs:
  docs:
    runs-on: ubuntu-24.04
    environment:
      name: github-pages
      url: ${{ steps.deploy.outputs.page_url }}

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install dbt
        run: pip install dbt-trino==1.8.5

      - name: dbt deps
        working-directory: dbt
        run: dbt deps

      - name: Generate dbt docs
        working-directory: dbt
        env:
          DBT_TARGET_HOST: ${{ secrets.TRINO_HOST_PROD }}
          DBT_TARGET_USER: ${{ secrets.TRINO_USER_PROD }}
        run: |
          dbt docs generate --target prod
          mkdir -p ../docs-site
          cp -r target/catalog.json target/manifest.json target/index.html ../docs-site/

      - name: Setup Pages
        uses: actions/configure-pages@v5

      - name: Upload Pages artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: docs-site

      - name: Deploy to GitHub Pages
        id: deploy
        uses: actions/deploy-pages@v4
```

### Promote Prod Manifest After Successful Deploy

```yaml
# job added to your production deploy workflow
  publish-manifest:
    needs: dbt-prod-run
    runs-on: ubuntu-24.04
    steps:
      - name: Download run artifacts
        uses: actions/download-artifact@v4
        with:
          name: dbt-manifest-prod-${{ github.run_id }}
          path: ./artifacts

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::${{ vars.AWS_ACCOUNT_ID }}:role/github-actions-dbt
          aws-region: eu-central-1

      - name: Push to S3
        run: |
          aws s3 cp ./artifacts/manifest.json \
            s3://${{ vars.DBT_ARTIFACTS_BUCKET }}/prod/manifest.json
```

---

## SQLFluff — SQL Linting with PR Annotations

### `.sqlfluff` Config

```ini
# .sqlfluff
[sqlfluff]
dialect = trino
templater = dbt
max_line_length = 120
exclude_rules = RF04           # RF04 = keyword capitalisation (optional)

[sqlfluff:templater:dbt]
project_dir = ./dbt

[sqlfluff:rules:capitalisation.keywords]
capitalisation_policy = upper

[sqlfluff:rules:capitalisation.identifiers]
capitalisation_policy = lower

[sqlfluff:rules:layout.indent]
indent_unit = space
tab_space_size = 4
```

### Lint Workflow with Native GitHub Annotations

```yaml
# .github/workflows/sqlfluff.yml
name: SQLFluff

on:
  pull_request:
    paths:
      - 'dbt/models/**/*.sql'
      - 'dbt/macros/**/*.sql'
      - 'dbt/tests/**/*.sql'
      - '.sqlfluff'

permissions:
  contents: read
  pull-requests: write          # needed for PR comments

jobs:
  lint:
    runs-on: ubuntu-24.04
    timeout-minutes: 15

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install sqlfluff + dbt templater
        run: pip install sqlfluff sqlfluff-templater-dbt dbt-trino==1.8.5

      - name: Install dbt packages
        working-directory: dbt
        run: dbt deps

      # --format github-annotation-native → inline PR annotations, no extra action needed
      - name: sqlfluff lint
        id: lint
        working-directory: dbt
        continue-on-error: true   # collect all errors before failing
        run: |
          sqlfluff lint models/ macros/ tests/ \
            --format github-annotation-native \
            --annotation-level failure \
            --dialect trino \
            2>&1 | tee ../sqlfluff-output.txt
          echo "exit_code=${PIPESTATUS[0]}" >> "$GITHUB_OUTPUT"

      # Post a summary comment on the PR
      - name: Comment lint results on PR
        if: steps.lint.outputs.exit_code != '0'
        uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            const output = fs.readFileSync('sqlfluff-output.txt', 'utf8');
            const body = `## SQLFluff Lint Results\n\`\`\`\n${output.slice(0, 60000)}\n\`\`\``;
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body
            });

      - name: Fail if lint errors found
        if: steps.lint.outputs.exit_code != '0'
        run: exit 1

  autofix:
    runs-on: ubuntu-24.04
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ github.head_ref }}
          token: ${{ secrets.GITHUB_TOKEN }}

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install sqlfluff
        run: pip install sqlfluff sqlfluff-templater-dbt dbt-trino==1.8.5

      - name: Install dbt packages
        working-directory: dbt
        run: dbt deps

      - name: sqlfluff fix
        working-directory: dbt
        run: |
          sqlfluff fix models/ macros/ \
            --dialect trino \
            --force

      - name: Commit fixes
        uses: stefanzweifel/git-auto-commit-action@v5
        with:
          commit_message: "style: sqlfluff autofix"
          file_pattern: "dbt/models/**/*.sql dbt/macros/**/*.sql"
```

---

## Airflow DAG Validation

### pytest DAG Integrity Tests

```python
# tests/test_dag_integrity.py
"""Structural DAG validation — runs without an Airflow database."""
import importlib
import os
from pathlib import Path

import pytest
from airflow.models import DagBag

DAG_FOLDER = Path(__file__).parent.parent / "dags"
LOAD_EXAMPLES = False


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=LOAD_EXAMPLES)


def test_no_import_errors(dagbag: DagBag) -> None:
    """All DAG files must import cleanly."""
    assert dagbag.import_errors == {}, (
        f"DAG import errors:\n"
        + "\n".join(f"  {f}: {e}" for f, e in dagbag.import_errors.items())
    )


def test_all_dags_have_tags(dagbag: DagBag) -> None:
    """Every DAG must declare at least one tag for discoverability."""
    missing = [dag_id for dag_id, dag in dagbag.dags.items() if not dag.tags]
    assert not missing, f"DAGs missing tags: {missing}"


def test_all_dags_have_owner(dagbag: DagBag) -> None:
    """default_args must specify owner (not the Airflow default 'airflow')."""
    bad = [
        dag_id
        for dag_id, dag in dagbag.dags.items()
        if dag.default_args.get("owner", "airflow") == "airflow"
    ]
    assert not bad, f"DAGs with default owner: {bad}"


def test_no_cycles(dagbag: DagBag) -> None:
    """Airflow already detects cycles during DagBag load; assert none exist."""
    # Cycle detection happens at load time; if dagbag loaded, we're good.
    # But explicitly trigger the check for safety:
    for dag_id, dag in dagbag.dags.items():
        # This raises if a cycle is found
        dag.test_cycle()


def test_task_count_reasonable(dagbag: DagBag) -> None:
    """No DAG should have an absurd number of static tasks (signals design issue)."""
    max_tasks = 200
    oversized = {
        dag_id: len(dag.tasks)
        for dag_id, dag in dagbag.dags.items()
        if len(dag.tasks) > max_tasks
    }
    assert not oversized, f"DAGs with >{max_tasks} tasks: {oversized}"


def test_retries_configured(dagbag: DagBag) -> None:
    """Every DAG should configure at least 1 retry."""
    missing_retries = [
        dag_id
        for dag_id, dag in dagbag.dags.items()
        if dag.default_args.get("retries", 0) < 1
    ]
    assert not missing_retries, f"DAGs without retries: {missing_retries}"
```

### Airflow DAG CI Workflow

```yaml
# .github/workflows/airflow-dag-validation.yml
name: Airflow DAG Validation

on:
  pull_request:
    paths:
      - 'dags/**'
      - 'tests/test_dag_integrity.py'
      - 'requirements-airflow.txt'

permissions:
  contents: read

jobs:
  dag-integrity:
    runs-on: ubuntu-24.04
    timeout-minutes: 15

    strategy:
      matrix:
        python-version: ["3.11"]
        airflow-version: ["2.10.4"]

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
          cache: pip

      - name: Install Airflow + test deps
        run: |
          pip install --upgrade pip
          AIRFLOW_VERSION=${{ matrix.airflow-version }}
          PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
          CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
          pip install "apache-airflow==${AIRFLOW_VERSION}" \
            --constraint "${CONSTRAINT_URL}"
          # install project-specific providers
          pip install apache-airflow-providers-amazon \
                      apache-airflow-providers-google \
                      apache-airflow-providers-trino \
                      pytest pytest-asyncio

      - name: Initialize Airflow DB (sqlite, no real connections)
        env:
          AIRFLOW__CORE__UNIT_TEST_MODE: "True"
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: "sqlite:///./airflow-test.db"
        run: airflow db migrate

      - name: Run DAG integrity tests
        env:
          AIRFLOW__CORE__UNIT_TEST_MODE: "True"
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: "sqlite:///./airflow-test.db"
          AIRFLOW__CORE__DAGS_FOLDER: "${{ github.workspace }}/dags"
          # Stub secrets so DAGs that read env vars don't crash
          AIRFLOW_CONN_TRINO_DEFAULT: "trino://user@localhost:8080/catalog"
        run: |
          pytest tests/test_dag_integrity.py -v \
            --tb=short \
            --junit-xml=reports/dag-integrity.xml

      - name: Publish test results
        if: always()
        uses: EnricoMi/publish-unit-test-result-action@v2
        with:
          files: reports/dag-integrity.xml
```

---

## Great Expectations / Soda as CI Quality Gates

### Great Expectations in CI

```yaml
# .github/workflows/data-quality-ge.yml
name: Data Quality — Great Expectations

on:
  pull_request:
    paths: ['dbt/**', 'great_expectations/**']

permissions:
  contents: read
  id-token: write

jobs:
  ge-checkpoint:
    runs-on: ubuntu-24.04
    timeout-minutes: 30

    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::${{ vars.AWS_ACCOUNT_ID }}:role/github-actions-ge
          aws-region: eu-central-1

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install dependencies
        run: |
          pip install great_expectations sqlalchemy \
            trino[sqlalchemy] dbt-trino

      - name: Run GE Checkpoint
        env:
          GE_TRINO_HOST: ${{ secrets.TRINO_HOST }}
          GE_TRINO_USER: ${{ secrets.TRINO_USER }}
        run: |
          great_expectations checkpoint run nightly_staging_checkpoint \
            --result-format SUMMARY

      - name: Upload Data Docs
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: ge-data-docs-${{ github.run_id }}
          path: great_expectations/uncommitted/data_docs/
          retention-days: 14
```

### Soda in CI (Official GitHub Action)

```yaml
# .github/workflows/data-quality-soda.yml
name: Data Quality — Soda

on:
  pull_request:
    paths: ['dbt/**', 'soda/**']

permissions:
  contents: read
  id-token: write

jobs:
  soda-scan:
    runs-on: ubuntu-24.04
    timeout-minutes: 20

    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::${{ vars.AWS_ACCOUNT_ID }}:role/github-actions-soda
          aws-region: eu-central-1

      # Official Soda action runs checks defined in SodaCL YAML files
      - name: Soda Library Scan
        uses: sodadata/soda-github-action@v1
        env:
          SODA_CLOUD_API_KEY: ${{ secrets.SODA_CLOUD_API_KEY }}
          SODA_CLOUD_API_SECRET: ${{ secrets.SODA_CLOUD_API_SECRET }}
          TRINO_HOST: ${{ secrets.TRINO_HOST }}
          TRINO_USER: ${{ secrets.TRINO_USER }}
        with:
          soda_library_version: latest
          data_source: trino_staging
          configuration: soda/configuration.yml
          checks: soda/checks/staging_checks.yml
```

```yaml
# soda/checks/staging_checks.yml  — referenced above
checks for stg_orders:
  - row_count > 0
  - missing_count(order_id) = 0
  - duplicate_count(order_id) = 0
  - freshness(created_at) < 2h
  - schema:
      fail:
        when required column missing: [order_id, user_id, status, amount, created_at]
  - invalid_count(status) = 0:
      valid values: [placed, shipped, delivered, cancelled, returned]
```

---

## Docker Image Builds for Data Tools

### Multi-Stage Dockerfile for dbt + Trino

```dockerfile
# Dockerfile.dbt
# Stage 1 — build deps layer (expensive, rarely changes)
FROM python:3.11-slim AS builder
WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install \
    dbt-core==1.8.7 \
    dbt-trino==1.8.5 \
    sqlfluff==3.3.0 \
    sqlfluff-templater-dbt

# Stage 2 — lean runtime image
FROM python:3.11-slim AS runtime
WORKDIR /dbt

COPY --from=builder /install /usr/local

# dbt project files (copied separately for better layer caching)
COPY dbt/profiles.yml /root/.dbt/profiles.yml
COPY dbt/ .

ENTRYPOINT ["dbt"]
CMD ["--help"]
```

```dockerfile
# Dockerfile.spark  — multi-stage Spark + Python image
FROM eclipse-temurin:17-jre-jammy AS jre-base

FROM python:3.11-slim AS spark-builder
WORKDIR /build

ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3

# Download Spark (cached as long as ARG doesn't change)
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL \
      "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
      -o spark.tgz && \
    tar -xzf spark.tgz

FROM python:3.11-slim AS spark-runtime
WORKDIR /opt/spark

COPY --from=jre-base /opt/java /opt/java
COPY --from=spark-builder /build/spark-*/  .

ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN pip install --no-cache-dir pyspark==3.5.3 delta-spark==3.2.1

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
```

### Build & Push Workflow (ghcr.io + Layer Cache)

```yaml
# .github/workflows/docker-build.yml
name: Docker Build

on:
  push:
    branches: [main]
    paths:
      - 'Dockerfile.*'
      - 'requirements*.txt'
  pull_request:
    paths:
      - 'Dockerfile.*'
      - 'requirements*.txt'

permissions:
  contents: read
  packages: write              # push to ghcr.io

jobs:
  build:
    runs-on: ubuntu-24.04
    timeout-minutes: 45

    strategy:
      matrix:
        image:
          - { name: dbt,   dockerfile: Dockerfile.dbt,   context: . }
          - { name: spark, dockerfile: Dockerfile.spark, context: . }

    steps:
      - uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Log in to ghcr.io
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Docker metadata (tags + labels)
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: ghcr.io/${{ github.repository }}/${{ matrix.image.name }}
          tags: |
            type=sha,prefix=sha-,format=short
            type=ref,event=branch
            type=raw,value=latest,enable=${{ github.ref == 'refs/heads/main' }}

      - name: Build and push
        uses: docker/build-push-action@v6
        with:
          context: ${{ matrix.image.context }}
          file:    ${{ matrix.image.dockerfile }}
          push: ${{ github.event_name == 'push' }}
          tags:    ${{ steps.meta.outputs.tags }}
          labels:  ${{ steps.meta.outputs.labels }}
          # Registry cache — reuses layers from previous pushes
          cache-from: type=registry,ref=ghcr.io/${{ github.repository }}/${{ matrix.image.name }}:cache
          cache-to:   type=registry,ref=ghcr.io/${{ github.repository }}/${{ matrix.image.name }}:cache,mode=max
          # Build args for reproducible builds
          build-args: |
            BUILDKIT_INLINE_CACHE=1
```

---

## Secrets Management

### GitHub Secrets + Environment-Scoped Secrets

Secrets are set in **Settings → Secrets and variables → Actions**.

```
Scope hierarchy:
  Repository secrets       → available in all workflows
  Environment secrets      → available only when job targets that environment
  Organization secrets     → shared across repos (with repo allow-list)
```

```yaml
jobs:
  deploy-prod:
    runs-on: ubuntu-24.04
    environment: production        # activates environment-scoped secrets
                                   # and requires manual approval if configured
    steps:
      - name: Use prod secret
        env:
          DB_PASSWORD: ${{ secrets.DB_PASSWORD }}   # from 'production' env
        run: echo "deploying with scoped secret"
```

### OIDC — AWS (No Static Keys)

```yaml
# Required permission — add to workflow-level permissions block
permissions:
  id-token: write
  contents: read

jobs:
  deploy:
    steps:
      - name: Configure AWS credentials via OIDC
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_ROLE_ARN }}
          # Optional: scope to this repo + branch only (enforced in IAM trust policy)
          role-session-name: github-${{ github.run_id }}
          aws-region: eu-central-1
          # No aws-access-key-id / aws-secret-access-key — uses OIDC token exchange
```

**AWS IAM trust policy** (restrict to specific repo and ref):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::<account>:oidc-provider/token.actions.githubusercontent.com"
    },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
      },
      "StringLike": {
        "token.actions.githubusercontent.com:sub": "repo:my-org/my-repo:ref:refs/heads/main"
      }
    }
  }]
}
```

### OIDC — GCP (No Static Keys)

```yaml
      - name: Authenticate to GCP via OIDC
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: >-
            projects/${{ vars.GCP_PROJECT_NUMBER }}/locations/global/
            workloadIdentityPools/github-pool/providers/github-provider
          service_account: github-actions@${{ vars.GCP_PROJECT_ID }}.iam.gserviceaccount.com
          # No credentials_json — keyless via OIDC token exchange
```

---

## Reusable Workflows, Matrix Builds, Composite Actions

### Reusable Workflow (`workflow_call`)

```yaml
# .github/workflows/_dbt-run.yml  — reusable, called by other workflows
name: dbt Run (reusable)

on:
  workflow_call:
    inputs:
      target:
        description: "dbt target profile (ci | staging | prod)"
        required: true
        type: string
      select:
        description: "dbt --select expression"
        required: false
        type: string
        default: "state:modified+"
      dbt_adapter:
        required: false
        type: string
        default: "dbt-trino==1.8.5"
    secrets:
      TRINO_HOST:
        required: true
      TRINO_USER:
        required: true
    outputs:
      run_status:
        description: "dbt exit code"
        value: ${{ jobs.run.outputs.status }}

jobs:
  run:
    runs-on: ubuntu-24.04
    timeout-minutes: 60
    outputs:
      status: ${{ steps.dbt.outcome }}

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install dbt
        run: pip install ${{ inputs.dbt_adapter }} dbt-core

      - name: dbt deps
        working-directory: dbt
        run: dbt deps

      - name: dbt build
        id: dbt
        working-directory: dbt
        env:
          DBT_TARGET_HOST: ${{ secrets.TRINO_HOST }}
          DBT_TARGET_USER: ${{ secrets.TRINO_USER }}
        run: |
          dbt build \
            --select "${{ inputs.select }}" \
            --target "${{ inputs.target }}"
```

**Caller workflow:**

```yaml
# .github/workflows/deploy-staging.yml
name: Deploy Staging

on:
  push:
    branches: [staging]

jobs:
  dbt-staging:
    uses: ./.github/workflows/_dbt-run.yml
    with:
      target: staging
      select: "tag:daily"
    secrets:
      TRINO_HOST: ${{ secrets.TRINO_HOST_STAGING }}
      TRINO_USER: ${{ secrets.TRINO_USER_STAGING }}
```

### Matrix Build

```yaml
jobs:
  test-adapters:
    strategy:
      fail-fast: false
      matrix:
        adapter:
          - { name: trino,    package: "dbt-trino==1.8.5",    target: trino-ci    }
          - { name: spark,    package: "dbt-spark==1.8.0",    target: spark-ci    }
          - { name: postgres, package: "dbt-postgres==1.8.2", target: postgres-ci }
        python: ["3.10", "3.11"]

    runs-on: ubuntu-24.04
    name: "${{ matrix.adapter.name }} / py${{ matrix.python }}"

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python }}
          cache: pip

      - name: Install ${{ matrix.adapter.name }} adapter
        run: pip install ${{ matrix.adapter.package }}

      - name: dbt build
        env:
          DBT_TARGET: ${{ matrix.adapter.target }}
        run: |
          cd dbt
          dbt deps
          dbt build --target ${{ matrix.adapter.target }}
```

### Composite Action

```yaml
# .github/actions/setup-dbt/action.yml
name: Setup dbt
description: "Install dbt with caching and run dbt deps"

inputs:
  adapter:
    description: "pip package for the dbt adapter"
    required: true
  project_dir:
    description: "Path to the dbt project"
    required: false
    default: "dbt"

runs:
  using: composite
  steps:
    - uses: actions/setup-python@v5
      with:
        python-version: "3.11"
        cache: pip

    - name: Install dbt
      shell: bash
      run: pip install --upgrade pip ${{ inputs.adapter }}

    - name: dbt deps
      shell: bash
      working-directory: ${{ inputs.project_dir }}
      run: dbt deps
```

**Using the composite action:**

```yaml
      - uses: ./.github/actions/setup-dbt
        with:
          adapter: dbt-trino==1.8.5
          project_dir: dbt
```

---

## Full Worked Example — dbt Project: Lint → Compile → Test → Slim Deploy

```yaml
# .github/workflows/dbt-full-ci-cd.yml
# Full pipeline: SQLFluff lint → dbt compile → slim CI test → promote manifest
name: dbt Full CI/CD

on:
  pull_request:
    branches: [main]
    paths: ['dbt/**', '.sqlfluff', '.github/workflows/dbt-full-ci-cd.yml']
  push:
    branches: [main]
    paths: ['dbt/**']

permissions:
  contents: read
  id-token: write
  pull-requests: write
  packages: write

concurrency:
  group: dbt-cicd-${{ github.ref }}
  cancel-in-progress: true

env:
  DBT_PROJECT_DIR: dbt
  DBT_ADAPTER: "dbt-trino==1.8.5"

jobs:
  # ── Job 1: SQL lint ─────────────────────────────────────────────────────────
  lint:
    name: SQLFluff Lint
    runs-on: ubuntu-24.04
    timeout-minutes: 10

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install sqlfluff
        run: pip install sqlfluff sqlfluff-templater-dbt ${{ env.DBT_ADAPTER }}

      - name: dbt deps (needed by templater)
        working-directory: ${{ env.DBT_PROJECT_DIR }}
        run: dbt deps

      - name: SQLFluff lint
        working-directory: ${{ env.DBT_PROJECT_DIR }}
        run: |
          sqlfluff lint models/ \
            --format github-annotation-native \
            --annotation-level failure \
            --dialect trino

  # ── Job 2: dbt compile ──────────────────────────────────────────────────────
  compile:
    name: dbt Compile
    needs: lint
    runs-on: ubuntu-24.04
    timeout-minutes: 15

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_ROLE_ARN }}
          aws-region: eu-central-1

      - uses: ./.github/actions/setup-dbt
        with:
          adapter: ${{ env.DBT_ADAPTER }}

      - name: Download prod manifest
        run: |
          aws s3 cp s3://${{ vars.DBT_ARTIFACTS_BUCKET }}/prod/manifest.json \
            ./${{ env.DBT_PROJECT_DIR }}/prod-artifacts/manifest.json || true

      - name: dbt compile
        working-directory: ${{ env.DBT_PROJECT_DIR }}
        env:
          DBT_HOST: ${{ secrets.TRINO_HOST_CI }}
          DBT_USER: ${{ secrets.TRINO_USER_CI }}
        run: |
          dbt compile \
            --select "state:modified+" \
            --state ./prod-artifacts/ \
            --target ci

      - name: Upload compiled manifest
        uses: actions/upload-artifact@v4
        with:
          name: dbt-manifest-compiled-${{ github.run_id }}
          path: ${{ env.DBT_PROJECT_DIR }}/target/manifest.json

  # ── Job 3: dbt slim CI test ─────────────────────────────────────────────────
  test:
    name: dbt Slim CI Test
    needs: compile
    runs-on: ubuntu-24.04
    timeout-minutes: 45
    # Only run tests on PRs; push to main goes straight to deploy
    if: github.event_name == 'pull_request'

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_ROLE_ARN }}
          aws-region: eu-central-1

      - uses: ./.github/actions/setup-dbt
        with:
          adapter: ${{ env.DBT_ADAPTER }}

      - name: Download prod manifest
        run: |
          aws s3 cp s3://${{ vars.DBT_ARTIFACTS_BUCKET }}/prod/manifest.json \
            ./${{ env.DBT_PROJECT_DIR }}/prod-artifacts/manifest.json || true

      - name: dbt build — slim CI
        working-directory: ${{ env.DBT_PROJECT_DIR }}
        env:
          DBT_HOST: ${{ secrets.TRINO_HOST_CI }}
          DBT_USER: ${{ secrets.TRINO_USER_CI }}
        run: |
          dbt build \
            --select "state:modified+" \
            --state ./prod-artifacts/ \
            --target ci \
            --defer \
            --favor-state

      - name: Upload CI run artifacts
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: dbt-artifacts-ci-${{ github.run_id }}
          path: |
            ${{ env.DBT_PROJECT_DIR }}/target/manifest.json
            ${{ env.DBT_PROJECT_DIR }}/target/run_results.json
          retention-days: 7

  # ── Job 4: Deploy to prod (main branch only) ────────────────────────────────
  deploy-prod:
    name: Deploy to Production
    needs: compile
    runs-on: ubuntu-24.04
    timeout-minutes: 90
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    environment: production       # requires manual approval gate if configured

    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_ROLE_ARN_PROD }}
          aws-region: eu-central-1

      - uses: ./.github/actions/setup-dbt
        with:
          adapter: ${{ env.DBT_ADAPTER }}

      - name: dbt run — full prod
        working-directory: ${{ env.DBT_PROJECT_DIR }}
        env:
          DBT_HOST: ${{ secrets.TRINO_HOST_PROD }}
          DBT_USER: ${{ secrets.TRINO_USER_PROD }}
        run: |
          dbt build \
            --target prod \
            --select "tag:daily"

      - name: Upload prod manifest
        uses: actions/upload-artifact@v4
        with:
          name: dbt-manifest-prod-${{ github.run_id }}
          path: ${{ env.DBT_PROJECT_DIR }}/target/manifest.json

      - name: Promote manifest to S3 (for next slim CI run)
        run: |
          aws s3 cp \
            ./${{ env.DBT_PROJECT_DIR }}/target/manifest.json \
            s3://${{ vars.DBT_ARTIFACTS_BUCKET }}/prod/manifest.json
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Hardcoded AWS/GCP credentials in secrets | Static keys must be rotated; leak = full access | Use OIDC (`id-token: write`) + `aws-actions/configure-aws-credentials@v4` with `role-to-assume` |
| `runs-on: ubuntu-latest` | Image can change between runs, breaking reproducibility | Pin to `ubuntu-24.04` or a specific runner image |
| No `fetch-depth: 0` on checkout | dbt `state:modified+` cannot diff git history; always finds all models modified | Add `fetch-depth: 0` to `actions/checkout@v4` |
| `dbt build` without `--defer` in CI | CI re-runs all upstream models even if they are unchanged; wastes compute and time | Add `--defer --favor-state` with a downloaded prod manifest |
| `|| true` on prod manifest download without a fallback | First-run silently skips slim selection, running everything | Add a comment and handle first-run explicitly with a condition |
| Installing dbt without pinning adapter version | Minor version bumps can silently change SQL generation | Pin `dbt-trino==1.8.5` (exact version), not `>=1.8` |
| Storing `profiles.yml` with credentials in git | Credential leak via repository history | Use `env_var()` in profiles.yml and inject via GitHub Secrets |
| SQLFluff without `--dialect` | Falls back to ANSI, missing dialect-specific rules | Always pass `--dialect trino` (or configured in `.sqlfluff`) |
| Using `GITHUB_TOKEN` to push images without `packages: write` | Workflow fails with 403 on ghcr.io push | Add `permissions: packages: write` at workflow or job level |
| Reusable workflow without `secrets: inherit` or explicit secrets | Called workflow cannot access secrets from caller | Pass secrets explicitly via `secrets:` block or use `secrets: inherit` |
| Running full `dbt build` in every PR regardless of changes | Slow CI, wasted cloud compute, noisy failures on unrelated models | Use `paths:` trigger filters + `state:modified+` selector |
| No `concurrency` group with `cancel-in-progress` | Stale workflow runs pile up when multiple commits pushed quickly | Add `concurrency: { group: ..., cancel-in-progress: true }` |
| Airflow DagBag tests without env stubs | Real connections attempted; tests fail in ephemeral CI runners | Set `AIRFLOW__CORE__UNIT_TEST_MODE=True` and stub connection strings |

---

## References to Consult When Needed

- [GitHub Actions: Workflow syntax](https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions)
- [GitHub Actions: Reusing workflows](https://docs.github.com/en/actions/using-workflows/reusing-workflows)
- [GitHub Actions: Security hardening](https://docs.github.com/en/actions/security-guides/security-hardening-for-github-actions)
- [GitHub Actions: OIDC token](https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/about-security-hardening-with-openid-connect)
- [aws-actions/configure-aws-credentials](https://github.com/aws-actions/configure-aws-credentials)
- [google-github-actions/auth](https://github.com/google-github-actions/auth)
- [docker/build-push-action](https://github.com/docker/build-push-action)
- [Docker: Cache management with GitHub Actions](https://docs.docker.com/build/ci/github-actions/cache/)
- [dbt: Slim CI](https://docs.getdbt.com/docs/deploy/slim-ci-cd)
- [dbt: Node selection — state](https://docs.getdbt.com/reference/node-selection/methods#the-state-method)
- [SQLFluff: GitHub Actions annotations](https://docs.sqlfluff.com/en/stable/production/github_actions.html)
- [sqlfluff/sqlfluff-github-actions](https://github.com/sqlfluff/sqlfluff-github-actions)
- [Soda: GitHub Action](https://github.com/sodadata/soda-github-action)
- [Great Expectations: GitHub Action](https://github.com/great-expectations/great_expectations_action)
- [Airflow: DAG testing guide (Astronomer)](https://github.com/astronomer/airflow-testing-guide)
