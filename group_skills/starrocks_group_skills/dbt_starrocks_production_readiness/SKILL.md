---
name: dbt-starrocks-production-readiness
description: dbt + StarRocks production readiness — slim CI with state:modified+, manifest.json artifact storage and --defer for dev, dbt Cloud vs dbt Core deployment, RBAC for dbt users in StarRocks, secrets management for profiles.yml, dbt run/test exit codes in CI, breaking change detection, scheduled dbt runs via Airflow/dbt Cloud, dbt docs generation and hosting, model versioning
---

# dbt + StarRocks Production Readiness

## When to Use

- Setting up CI/CD for a dbt project targeting StarRocks
- Configuring RBAC so dbt has minimum required permissions
- Deploying dbt via Airflow or dbt Cloud
- Detecting breaking changes before merging to main
- Maintaining dbt documentation for the team

---

## Minimum StarRocks RBAC for dbt

```sql
-- Create dbt user
CREATE USER dbt_prod IDENTIFIED BY 'strong_password_here';

-- Permissions needed for dbt to run
GRANT SELECT, INSERT, UPDATE, DELETE, DROP, CREATE TABLE, ALTER
    ON sales.* TO dbt_prod;

-- For ANALYZE TABLE (post-hooks)
GRANT SELECT ON _statistics_.* TO dbt_prod;

-- If dbt accesses external catalogs
GRANT USAGE ON CATALOG iceberg_hms TO dbt_prod;
GRANT SELECT ON iceberg_hms.raw.* TO dbt_prod;

-- Dev schema: dbt user creates its own tables
GRANT ALL ON dbt_dev.* TO dbt_user;

-- Read-only user for BI tools (queries dbt output)
CREATE USER bi_readonly IDENTIFIED BY 'password';
GRANT SELECT ON sales.* TO bi_readonly;
```

---

## Secrets Management for profiles.yml

### Environment Variables (CI/CD)

```yaml
# profiles.yml — no hardcoded secrets
starrocks_project:
  outputs:
    prod:
      type: starrocks
      host: "{{ env_var('SR_HOST') }}"
      port: 9030
      username: "{{ env_var('SR_USER') }}"
      password: "{{ env_var('SR_PASSWORD') }}"
      database: sales
      schema: "{{ env_var('SR_SCHEMA', 'sales') }}"
```

GitHub Actions secrets:
```yaml
# .github/workflows/dbt_ci.yml
env:
  SR_HOST: ${{ secrets.STARROCKS_HOST }}
  SR_USER: ${{ secrets.STARROCKS_USER }}
  SR_PASSWORD: ${{ secrets.STARROCKS_PASSWORD }}
  SR_SCHEMA: sales
```

### Vault Integration

```python
# generate_profiles.py — called before dbt run
import hvac, yaml, os

client = hvac.Client(url=os.environ['VAULT_ADDR'], token=os.environ['VAULT_TOKEN'])
secret = client.secrets.kv.read_secret_version(path="data/starrocks/prod")["data"]["data"]

profile = {
    "starrocks_project": {
        "outputs": {
            "prod": {
                "type": "starrocks",
                "host": secret["host"],
                "port": 9030,
                "username": secret["username"],
                "password": secret["password"],
                "database": "sales",
                "schema": "sales",
            }
        },
        "target": "prod",
    }
}

with open(os.path.expanduser("~/.dbt/profiles.yml"), "w") as f:
    yaml.dump(profile, f)
```

---

## Slim CI with state:modified+

Only rebuild models that changed + their downstream dependents:

```yaml
# .github/workflows/dbt_ci.yml
name: dbt CI

on:
  pull_request:
    paths:
      - 'models/**'
      - 'tests/**'
      - 'macros/**'
      - 'dbt_project.yml'

jobs:
  dbt-ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install dbt
        run: pip install dbt-starrocks

      - name: Download production manifest
        run: |
          aws s3 cp s3://dbt-artifacts/prod/manifest.json ./prod_manifest/manifest.json
        env:
          AWS_DEFAULT_REGION: us-east-1

      - name: dbt deps
        run: dbt deps

      - name: dbt compile (for diff)
        run: dbt compile --profiles-dir .

      - name: dbt run (changed models only)
        run: |
          dbt run \
            --select state:modified+ \
            --defer \
            --state ./prod_manifest \
            --target ci \
            --profiles-dir .
        env:
          SR_HOST: ${{ secrets.STARROCKS_CI_HOST }}
          SR_USER: ${{ secrets.STARROCKS_CI_USER }}
          SR_PASSWORD: ${{ secrets.STARROCKS_CI_PASSWORD }}
          SR_SCHEMA: "dbt_ci_${{ github.event.pull_request.number }}"

      - name: dbt test (changed models + downstream)
        run: |
          dbt test \
            --select state:modified+ \
            --defer \
            --state ./prod_manifest
```

---

## Upload Manifest After Prod Run

```yaml
# Post-production run step
- name: Upload manifest
  run: |
    aws s3 cp target/manifest.json s3://dbt-artifacts/prod/manifest.json
```

---

## Breaking Change Detection

```bash
# In CI: compare current branch schema vs production
dbt ls --select state:modified --state ./prod_manifest

# Run dbt compile and check for breaking changes
# Breaking = column removed, type changed, model deleted
dbt compile --state ./prod_manifest

# Check for breaking changes via dbt-audit-helper
dbt run-operation compare_relations --args '{
  "a_relation": "prod.sales.orders",
  "b_relation": "dev.dbt_dev.orders"
}'
```

Custom macro to detect column drops:

```sql
-- macros/check_breaking_changes.sql
{% macro check_breaking_changes(model_name) %}
    {% set prod_columns = adapter.get_columns_in_relation(
        source('prod', model_name)
    ) | map(attribute='name') | list %}

    {% set new_columns = adapter.get_columns_in_relation(
        ref(model_name)
    ) | map(attribute='name') | list %}

    {% set dropped = prod_columns | reject('in', new_columns) | list %}
    {% if dropped | length > 0 %}
        {{ exceptions.raise_compiler_error(
            "Breaking change: columns dropped from " ~ model_name ~ ": " ~ dropped
        ) }}
    {% endif %}
{% endmacro %}
```

---

## Airflow dbt Operator

```python
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
    dag_id="dbt_starrocks_daily",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "starrocks"],
)
def dbt_daily():
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /dbt && dbt deps --profiles-dir /dbt/profiles",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /dbt && dbt run --target prod --profiles-dir /dbt/profiles",
        env={
            "SR_HOST": "{{ var('starrocks_host') }}",
            "SR_PASSWORD": "{{ conn.get('starrocks_prod').password }}",
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /dbt && dbt test --target prod --profiles-dir /dbt/profiles",
    )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command="cd /dbt && dbt source freshness --profiles-dir /dbt/profiles",
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /dbt && dbt docs generate --target prod && aws s3 sync target/ s3://dbt-docs/",
    )

    dbt_deps >> dbt_run >> dbt_test >> dbt_freshness >> dbt_docs


dag = dbt_daily()
```

---

## Selective Production Runs

```bash
# Run only Gold layer (fastest daily refresh)
dbt run --select tag:gold

# Run staging + silver + gold in order
dbt run --select staging+ --exclude tag:expensive

# Run a specific model and all its dependencies
dbt run --select +orders_daily

# Run tests for a specific layer only
dbt test --select silver

# Skip freshness tests in nightly runs (too slow)
dbt test --exclude source:*
```

---

## Model Versioning

dbt supports versioned models (dbt >= 1.5):

```yaml
# models/schema.yml
models:
  - name: orders
    latest_version: 2
    versions:
      - v: 1
        defined_in: orders_v1.sql
        deprecation_date: "2024-06-01"
      - v: 2
        defined_in: orders_v2.sql
```

Downstream models reference a version:
```sql
SELECT * FROM {{ ref('orders', version=2) }}
```

---

## dbt Documentation

Generate and host docs:

```bash
# Generate docs
dbt docs generate

# Serve locally for preview
dbt docs serve --port 8080

# Deploy to S3 + CloudFront
aws s3 sync target/ s3://dbt-docs-bucket/ --delete
aws cloudfront create-invalidation --distribution-id XXXXX --paths "/*"
```

Enrich with descriptions:

```yaml
# models/schema.yml
models:
  - name: orders
    description: |
      ## Orders (Silver Layer)
      Deduplicated and enriched order records. Source: CDC from PostgreSQL orders table.
      **Refreshed**: every 15 minutes via Routine Load + dbt incremental.
      **SLA**: < 30 minutes freshness.
    columns:
      - name: order_id
        description: "Unique order identifier. Primary Key."
      - name: amount
        description: "Order amount in USD. Null for cancelled orders."
```

---

## Exit Code Handling in CI

```bash
# dbt exits 0=success, 1=runtime error, 2=compile error
dbt run --select state:modified+
STATUS=$?

if [ $STATUS -eq 1 ]; then
  echo "dbt run failed — check run artifacts"
  # Upload artifacts even on failure
  aws s3 cp target/run_results.json s3://dbt-artifacts/ci/run_results.json
  exit 1
elif [ $STATUS -eq 2 ]; then
  echo "dbt compilation error — check model SQL"
  exit 2
fi

# Continue to tests only if run succeeded
dbt test --select state:modified+
```

---

## Anti-Patterns

1. **Hardcoding passwords in profiles.yml** — committed to git, rotated manually; use `env_var()` exclusively.
2. **No `--defer` in CI** — without defer, CI must build all upstream models from scratch; costs hours; use `--defer --state prod_manifest`.
3. **`dbt run` without `dbt test` in production** — silent data quality regressions; always follow run with tests.
4. **Not uploading manifest after prod run** — slim CI state comparison breaks; upload `target/manifest.json` to S3 as last step of prod run.
5. **Running all models in CI on every PR** — slow and expensive; use `state:modified+` to only rebuild what changed.
6. **Granting `ALL` to dbt user in production** — dbt should not drop production tables it doesn't own; scope permissions to specific schemas.

---

## References

- dbt slim CI: `docs.getdbt.com/best-practices/best-practice-workflows#run-only-modified-models-to-test-changes-slim-ci`
- dbt state: `docs.getdbt.com/reference/node-selection/methods#the-state-method`
- dbt model versioning: `docs.getdbt.com/docs/collaborate/govern/model-versions`
- Related skills: `[[dbt-starrocks-models]]`, `[[dbt-starrocks-testing]]`, `[[github-actions-dataops]]`, `[[starrocks-admin-security]]`
