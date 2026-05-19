---
name: dataops-github-actions-optimizer
description: GitHub Actions optimization for DataOps — path filters to skip unchanged components, concurrency groups (cancel-in-progress), job parallelization, caching strategies (pip/npm/Maven/Docker layers), self-hosted runners for heavy workloads, matrix strategy for multi-environment tests, reusable workflows to reduce duplication, OIDC for cloud credentials (no static keys), artifact retention, workflow timing analysis
---

# GitHub Actions Optimizer (DataOps)

## When to Use

- CI pipeline is slow (> 15 min for routine PRs)
- Workflows run unnecessarily on unrelated file changes
- Static credentials (AWS_SECRET_ACCESS_KEY) stored in repository secrets
- Teams duplicate the same workflow steps across multiple repos
- Self-hosted runner capacity is under- or over-provisioned

---

## Path Filters — Skip Unrelated Runs

```yaml
on:
  push:
    branches: [main]
  pull_request:
    paths:
      - "dbt/**"
      - "requirements*.txt"
      - ".github/workflows/dbt-ci.yml"
    paths-ignore:
      - "docs/**"
      - "*.md"
      - ".github/workflows/airflow-*.yml"  # other workflow files
```

---

## Concurrency Control — Cancel Stale Runs

```yaml
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true   # cancel queued/in-progress runs for same branch

# For main branch: never cancel (always run to completion)
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: ${{ github.ref != 'refs/heads/main' }}
```

---

## Caching Strategies

### Python / pip

```yaml
- uses: actions/setup-python@v5
  with:
    python-version: "3.11"
    cache: pip                  # built-in pip caching

# Or manually for more control:
- uses: actions/cache@v4
  with:
    path: ~/.cache/pip
    key: ${{ runner.os }}-pip-${{ hashFiles('**/requirements*.txt') }}
    restore-keys: |
      ${{ runner.os }}-pip-
```

### Docker Layer Cache (BuildKit)

```yaml
- uses: docker/setup-buildx-action@v3

- uses: docker/build-push-action@v5
  with:
    push: true
    tags: ghcr.io/my-org/airflow:${{ github.sha }}
    cache-from: type=gha          # GitHub Actions cache backend
    cache-to: type=gha,mode=max   # cache all layers, not just final stage
```

### Maven / Java (Spark)

```yaml
- uses: actions/setup-java@v4
  with:
    java-version: "17"
    cache: maven
```

### Node.js (for Airflow plugins with npm)

```yaml
- uses: actions/setup-node@v4
  with:
    node-version: "20"
    cache: npm
    cache-dependency-path: airflow-ui/package-lock.json
```

---

## Job Parallelization

```yaml
jobs:
  # Run independent checks in parallel
  lint-sql:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: sqlfluff lint dbt/models --dialect trino

  lint-python:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: ruff check . && black --check .

  security-scan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: checkov -d . --framework terraform

  # Integration test waits for all lints to pass
  integration-test:
    needs: [lint-sql, lint-python, security-scan]
    runs-on: ubuntu-latest
    steps:
      - run: pytest tests/integration/ -v
```

---

## Matrix Strategy — Multi-Environment Tests

```yaml
jobs:
  test-dbt:
    strategy:
      fail-fast: false          # don't cancel other jobs if one fails
      matrix:
        environment: [dev, staging]
        python-version: ["3.10", "3.11"]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}

      - name: Run dbt tests
        env:
          DBT_TARGET: ${{ matrix.environment }}
        run: |
          dbt test --target ${{ matrix.environment }}
```

---

## OIDC — No Static Cloud Credentials

### AWS OIDC

```yaml
permissions:
  id-token: write    # required for OIDC
  contents: read

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::123456789012:role/GitHubActionsDataPlatform
          aws-region: us-east-1
          # No AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY needed

      - run: aws s3 ls s3://my-data-lake/
```

### GCP OIDC

```yaml
- uses: google-github-actions/auth@v2
  with:
    workload_identity_provider: projects/123/locations/global/workloadIdentityPools/github/providers/github
    service_account: data-platform@my-project.iam.gserviceaccount.com
```

### Terraform AWS Provider (OIDC)

```hcl
# IAM role trust policy for GitHub Actions OIDC
data "aws_iam_policy_document" "github_actions_trust" {
  statement {
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    actions = ["sts:AssumeRoleWithWebIdentity"]
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:my-org/data-platform:ref:refs/heads/main"]
    }
  }
}
```

---

## Self-Hosted Runners for Heavy Workloads

```yaml
jobs:
  spark-test:
    # Use self-hosted runner with more memory for Spark integration tests
    runs-on: [self-hosted, linux, xlarge]
    steps:
      - run: |
          spark-submit \
            --master local[4] \
            --driver-memory 8g \
            tests/spark/test_etl_job.py

  dbt-full-run:
    # Light jobs stay on GitHub-hosted runners
    runs-on: ubuntu-latest
    steps:
      - run: dbt run --target ci
```

### Runner Group Config (for large team)

```yaml
# Register self-hosted runner labels
# --labels: linux,xlarge,spark
# --labels: linux,gpu,ml

# Route jobs by label
runs-on: [self-hosted, spark]     # Spark jobs → high-memory nodes
runs-on: [self-hosted, gpu]       # ML training → GPU nodes
runs-on: ubuntu-latest             # standard jobs → GitHub-hosted
```

---

## Artifact Management

```yaml
- name: Upload dbt artifacts (manifest, run_results)
  uses: actions/upload-artifact@v4
  with:
    name: dbt-artifacts-${{ github.sha }}
    path: |
      dbt/target/manifest.json
      dbt/target/run_results.json
    retention-days: 30   # keep for 30 days (default 90 days, reduces storage cost)

- name: Download base manifest for slim CI
  uses: actions/download-artifact@v4
  with:
    name: dbt-artifacts-main
    path: /tmp/dbt-base
```

---

## Workflow Timing Analysis

```bash
# Identify slowest jobs via GitHub CLI
gh run list \
  --workflow=dbt-ci.yml \
  --limit 20 \
  --json conclusion,startedAt,completedAt,name \
  | jq 'map(
      . + {duration_seconds:
        ((.completedAt | strptime("%Y-%m-%dT%H:%M:%SZ") | mktime) -
         (.startedAt  | strptime("%Y-%m-%dT%H:%M:%SZ") | mktime))
      }
    ) | sort_by(.duration_seconds) | reverse | .[:10] | .[] |
    "\(.name): \(.duration_seconds)s (\(.conclusion))"'
```

---

## Optimization Checklist

```
[ ] Path filters: workflows skip runs when unrelated files change
[ ] Concurrency: cancel-in-progress=true for PR workflows
[ ] pip/Maven/npm cache configured with dependency hash keys
[ ] Docker builds use cache-from=type=gha
[ ] Independent jobs run in parallel, not sequentially
[ ] No static AWS/GCP credentials — OIDC configured
[ ] Heavy jobs (Spark, GPU) routed to self-hosted runners
[ ] Artifact retention set to 30 days (not default 90)
[ ] Reusable workflows extract common steps (_docker-build.yml, _dbt-run.yml)
[ ] matrix.fail-fast: false for multi-environment test jobs
```

---

## Anti-Patterns

1. **No path filters on monorepo** — every PR runs all pipelines regardless of what changed; add `paths:` filter per workflow.
2. **Static AWS/GCP keys in repository secrets** — 90-day rotation burden + credential leak risk; migrate to OIDC.
3. **Sequential jobs where parallel is fine** — lint-sql, lint-python, security-scan have no dependency; run concurrently.
4. **No caching for pip install** — 2-min pip install on every run × 50 PRs/day = 100 wasted minutes/day; cache pip.
5. **Using GitHub-hosted runners for Spark integration tests** — 7GB RAM on `ubuntu-latest` causes OOM; use self-hosted runners with sufficient memory.
6. **Artifact retention at 90 days (default)** — large artifacts (test reports, built images) inflate storage costs; set retention-days: 7-30 based on actual use.

---

## References

- GitHub Actions caching: `docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/caching-dependencies-to-speed-up-workflows`
- OIDC for AWS: `docs.github.com/en/actions/security-for-github-actions/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services`
- Self-hosted runners: `docs.github.com/en/actions/hosting-your-own-runners`
- Related skills: `[[github-actions-dataops]]`, `[[dataops-cicd-pipeline-review]]`, `[[infra-docker-best-practices]]`
