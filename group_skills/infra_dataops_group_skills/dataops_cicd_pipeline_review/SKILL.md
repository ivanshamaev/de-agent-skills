---
name: dataops-cicd-pipeline-review
description: DataOps CI/CD pipeline review — pipeline stages for data projects (lint/test/build/scan/deploy), trunk-based vs GitFlow branching, environment promotion (dev→staging→prod), artifact versioning (Docker image tags/dbt manifest), deployment gating (DQ checks/smoke tests/manual approval), rollback strategy, pipeline observability (DORA metrics), reusable workflow patterns, monorepo vs polyrepo CI strategy
---

# DataOps CI/CD Pipeline Review

## When to Use

- Reviewing a CI/CD pipeline for a data platform component
- Designing pipeline stages for dbt, Airflow DAGs, or Spark jobs
- Implementing environment promotion with quality gates
- Auditing deployment strategy for production data pipelines
- Measuring and improving pipeline velocity (DORA metrics)

---

## Pipeline Stage Design for Data Projects

### dbt CI Pipeline

```yaml
# .github/workflows/dbt-ci.yml
name: dbt CI

on:
  pull_request:
    paths:
      - "dbt/**"
      - ".github/workflows/dbt-ci.yml"

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install sqlfluff sqlfluff-templater-dbt
      - run: sqlfluff lint dbt/models --dialect trino

  test:
    needs: lint
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - uses: actions/checkout@v4

      # Download main branch manifest for --defer
      - name: Download base manifest
        run: |
          aws s3 cp s3://my-company-artifacts/dbt/manifest.json \
            /tmp/manifest-base.json

      - name: dbt slim CI (modified models only)
        env:
          DBT_TRINO_HOST: ${{ secrets.TRINO_HOST }}
          DBT_TRINO_USER: ${{ secrets.TRINO_USER }}
        run: |
          cd dbt
          dbt deps
          dbt run \
            --select state:modified+ \
            --defer \
            --state /tmp/manifest-base.json \
            --target ci
          dbt test \
            --select state:modified+ \
            --defer \
            --state /tmp/manifest-base.json \
            --target ci

  upload-manifest:
    needs: test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - name: Upload manifest for future CI runs
        run: |
          aws s3 cp dbt/target/manifest.json \
            s3://my-company-artifacts/dbt/manifest.json
```

### Airflow DAG CI Pipeline

```yaml
name: Airflow DAG CI

on:
  pull_request:
    paths:
      - "dags/**"

jobs:
  dag-integrity:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_DB: airflow
          POSTGRES_USER: airflow
          POSTGRES_PASSWORD: airflow
        options: >-
          --health-cmd pg_isready
          --health-interval 10s

    steps:
      - uses: actions/checkout@v4
      - run: pip install apache-airflow==2.8.0 pytest

      - name: Validate DAG imports
        run: |
          python -c "
          from airflow.models import DagBag
          bag = DagBag(dag_folder='dags/', include_examples=False)
          assert not bag.import_errors, f'DAG import errors: {bag.import_errors}'
          print(f'Loaded {len(bag.dags)} DAGs successfully')
          "

      - name: Check for cycles
        run: |
          python -m pytest tests/dags/ -v --tb=short

      - name: Lint DAG files
        run: |
          ruff check dags/
          black --check dags/
```

---

## Environment Promotion Strategy

```
main branch → staging auto-deploy → [DQ gate] → prod manual approval → prod deploy
                                        ↓ fail
                                    Slack alert + block merge
```

### Promotion Gates

```yaml
# Staging deploy job
deploy-staging:
  runs-on: ubuntu-latest
  environment: staging
  steps:
    - name: Deploy to staging
      run: kubectl apply -f k8s/staging/

    - name: Run smoke tests
      run: pytest tests/smoke/ --env=staging

    - name: Data quality gate
      run: |
        soda scan -d trino_staging -c soda-config.yml checks/staging/

# Production deploy — requires manual approval
deploy-prod:
  needs: deploy-staging
  runs-on: ubuntu-latest
  environment:
    name: production      # GitHub environment with required reviewers
    url: https://airflow.prod.my-company.com
  steps:
    - name: Deploy to production
      run: kubectl apply -f k8s/production/
```

---

## Artifact Versioning

```bash
# Docker image tagging strategy
# dev branch:   sha-abc1234
# main branch:  main-20240115-abc1234  (date + sha)
# release tag:  v1.2.3

IMAGE_TAG="${GITHUB_REF_NAME}-${GITHUB_RUN_NUMBER}-${GITHUB_SHA:0:7}"
docker build -t ghcr.io/my-org/airflow:${IMAGE_TAG} .
docker push ghcr.io/my-org/airflow:${IMAGE_TAG}

# For production releases, also tag as latest
if [[ "$GITHUB_REF_TYPE" == "tag" ]]; then
  docker tag ghcr.io/my-org/airflow:${IMAGE_TAG} \
    ghcr.io/my-org/airflow:latest
fi
```

---

## Rollback Strategy

```yaml
# Rollback job triggered on failed deployment
rollback:
  needs: deploy-prod
  if: failure()
  runs-on: ubuntu-latest
  environment: production
  steps:
    - name: Rollback Kubernetes deployment
      run: |
        kubectl rollout undo deployment/airflow-scheduler -n airflow
        kubectl rollout undo deployment/airflow-webserver -n airflow
        kubectl rollout status deployment/airflow-scheduler -n airflow

    - name: Notify on rollback
      uses: slackapi/slack-github-action@v1.27.0
      with:
        payload: |
          {
            "text": "🔄 Production rollback triggered for ${{ github.repository }} by ${{ github.actor }}"
          }
      env:
        SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK }}
```

---

## Reusable Workflows

```yaml
# .github/workflows/_docker-build.yml (reusable)
on:
  workflow_call:
    inputs:
      image-name:
        required: true
        type: string
      dockerfile:
        required: false
        type: string
        default: Dockerfile
    outputs:
      image-tag:
        description: "Built image tag"
        value: ${{ jobs.build.outputs.image-tag }}
    secrets:
      GHCR_TOKEN:
        required: true

jobs:
  build:
    runs-on: ubuntu-latest
    outputs:
      image-tag: ${{ steps.meta.outputs.tags }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/metadata-action@v5
        id: meta
        with:
          images: ghcr.io/my-org/${{ inputs.image-name }}
      - uses: docker/build-push-action@v5
        with:
          file: ${{ inputs.dockerfile }}
          push: true
          tags: ${{ steps.meta.outputs.tags }}

# .github/workflows/airflow-release.yml (caller)
jobs:
  build-airflow:
    uses: ./.github/workflows/_docker-build.yml
    with:
      image-name: airflow
      dockerfile: docker/airflow/Dockerfile
    secrets:
      GHCR_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

---

## DORA Metrics Tracking

```bash
# Deployment Frequency — count deployments to production per week
gh run list \
  --workflow=deploy-production.yml \
  --branch=main \
  --status=success \
  --created ">=2024-01-08" \
  --json createdAt \
  | jq 'length'

# Lead Time — time from first commit to production deploy
# (use GitHub Deployments API)
gh api \
  /repos/my-org/my-repo/deployments \
  --paginate \
  --jq '.[] | select(.environment == "production") | {sha, created_at}'

# Change Failure Rate — failed deployments / total deployments
gh run list \
  --workflow=deploy-production.yml \
  --json conclusion \
  | jq '[.[] | select(.conclusion == "failure")] | length as $fail |
       length as $total |
       ($fail / $total * 100 | round | tostring) + "%"'
```

---

## Anti-Patterns

1. **Deploying directly to production without staging** — no validation environment means data bugs reach users first; always promote through at least one non-prod environment.
2. **No rollback automation** — manual rollback under pressure is error-prone; automate rollback on failed deploy with `if: failure()`.
3. **Running full dbt test suite on every PR** — 1000-model test runs take 30+ minutes; use `state:modified+` with `--defer` for slim CI.
4. **No artifact versioning (always tagging `:latest`)** — no way to roll back to a specific version; pin images to immutable tags (SHA + timestamp).
5. **Manual production deploys with no approval gate** — single engineer can push directly to prod; require environment protection rules with reviewers.
6. **Ignoring DORA metrics** — teams can't improve what they don't measure; track deployment frequency and lead time from day one.

---

## References

- GitHub Actions reusable workflows: `docs.github.com/en/actions/sharing-automations/reusing-workflows`
- dbt slim CI: `docs.getdbt.com/docs/deploy/slim-ci-jobs`
- DORA metrics: `dora.dev`
- Airflow CI best practices: `airflow.apache.org/docs/apache-airflow/stable/best-practices.html`
- Related skills: `[[github-actions-dataops]]`, `[[infra-gitops-deployment-review]]`, `[[dataops-airflow-production-readiness]]`
