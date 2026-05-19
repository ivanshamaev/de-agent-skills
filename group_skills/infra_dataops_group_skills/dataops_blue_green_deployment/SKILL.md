---
name: dataops-blue-green-deployment
description: Blue/green deployment for data platforms — Kubernetes blue/green with service selector swap, Argo Rollouts blue-green strategy, database schema migration compatibility (backward-compatible DDL), dbt blue/green schema swap (swap_schema macro), Airflow DAG versioning (dag_id with version suffix), traffic cutover and rollback procedure, smoke tests before cutover, Canary vs blue/green decision guide
---

# Blue/Green Deployment

## When to Use

- Deploying breaking schema changes to production tables
- Upgrading Airflow, Spark History Server, or other stateful data platform components
- Zero-downtime dbt model migrations (rename table, change column type)
- Rolling back a bad deployment without data loss
- Testing a new pipeline version before switching production traffic

---

## Kubernetes Blue/Green with Service Selector Swap

```yaml
# Two identical deployments: blue (current) and green (new)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver-blue
  labels:
    app: airflow-webserver
    version: blue
spec:
  replicas: 2
  selector:
    matchLabels:
      app: airflow-webserver
      version: blue
  template:
    metadata:
      labels:
        app: airflow-webserver
        version: blue
    spec:
      containers:
      - name: webserver
        image: apache/airflow:2.7.0     # current version

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver-green
  labels:
    app: airflow-webserver
    version: green
spec:
  replicas: 2
  selector:
    matchLabels:
      app: airflow-webserver
      version: green
  template:
    metadata:
      labels:
        app: airflow-webserver
        version: green
    spec:
      containers:
      - name: webserver
        image: apache/airflow:2.8.0     # new version

---
# Service points to blue initially
apiVersion: v1
kind: Service
metadata:
  name: airflow-webserver
spec:
  selector:
    app: airflow-webserver
    version: blue          # ← change to green to cut over
  ports:
  - port: 8080
    targetPort: 8080
```

### Cutover Script

```bash
#!/bin/bash
# blue_green_cutover.sh

NAMESPACE="airflow"
SERVICE="airflow-webserver"
NEW_VERSION="green"
OLD_VERSION="blue"

echo "Running smoke tests on green deployment..."
GREEN_POD=$(kubectl get pod -n $NAMESPACE -l version=$NEW_VERSION \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n $NAMESPACE $GREEN_POD -- \
  curl -sf http://localhost:8080/health || { echo "Smoke test FAILED"; exit 1; }

echo "Cutting over to $NEW_VERSION..."
kubectl patch service $SERVICE -n $NAMESPACE \
  -p "{\"spec\":{\"selector\":{\"version\":\"$NEW_VERSION\"}}}"

echo "Verifying cutover..."
sleep 5
kubectl describe service $SERVICE -n $NAMESPACE | grep "version:"

echo "Scaling down $OLD_VERSION (keep for rollback)..."
kubectl scale deployment airflow-webserver-$OLD_VERSION \
  -n $NAMESPACE --replicas=0

echo "Cutover complete. Rollback: kubectl scale deployment airflow-webserver-$OLD_VERSION --replicas=2"
```

---

## Argo Rollouts Blue/Green

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Rollout
metadata:
  name: spark-history-server
  namespace: spark
spec:
  replicas: 2
  selector:
    matchLabels:
      app: spark-history-server
  template:
    spec:
      containers:
      - name: history-server
        image: apache/spark:3.5.0
  strategy:
    blueGreen:
      activeService: spark-history-server-active    # serves production traffic
      previewService: spark-history-server-preview  # new version for testing
      autoPromotionEnabled: false   # require manual promotion
      scaleDownDelaySeconds: 300    # keep blue running 5 min after cutover
      prePromotionAnalysis:
        templates:
        - templateName: smoke-test
        args:
        - name: service-name
          value: spark-history-server-preview
```

```bash
# Check rollout status
kubectl argo rollouts get rollout spark-history-server -n spark --watch

# Promote green to active (after preview validation)
kubectl argo rollouts promote spark-history-server -n spark

# Abort and rollback to blue
kubectl argo rollouts abort spark-history-server -n spark
```

---

## dbt Blue/Green Schema Swap

```sql
-- Strategy: build new version in shadow schema, swap atomically

-- 1. Build in shadow schema
dbt run --target prod --vars '{"target_schema": "analytics_v2"}'

-- 2. Run tests on shadow schema
dbt test --target prod --vars '{"target_schema": "analytics_v2"}'

-- 3. Atomic swap (Trino/Postgres)
-- swap_schema macro:
```

```sql
-- macros/swap_schema.sql
{% macro swap_schema(production_schema, shadow_schema) %}
  {% if execute %}
    {% set old_schema = production_schema + '_old' %}

    -- Rename production → old
    {% set rename_prod %}
      ALTER SCHEMA {{ production_schema }} RENAME TO {{ old_schema }};
    {% endset %}
    {% do run_query(rename_prod) %}

    -- Rename shadow → production
    {% set rename_shadow %}
      ALTER SCHEMA {{ shadow_schema }} RENAME TO {{ production_schema }};
    {% endset %}
    {% do run_query(rename_shadow) %}

    -- Drop old schema (after validation)
    -- {{ log("Old schema available at: " + old_schema, info=True) }}
  {% endif %}
{% endmacro %}
```

```yaml
# dbt profile for blue/green
prod:
  target: prod
  outputs:
    prod:
      schema: analytics
    shadow:
      schema: analytics_v2    # shadow target
```

---

## Database Schema Migration Compatibility

### Backward-Compatible Migration Pattern

```sql
-- Phase 1: Add new column (nullable, no breaking change)
ALTER TABLE orders ADD COLUMN customer_segment VARCHAR(50);

-- Phase 2: Backfill (run in background, application writes to both)
UPDATE orders
SET customer_segment = classify_customer(customer_id)
WHERE customer_segment IS NULL;

-- Phase 3: Add NOT NULL constraint after backfill complete
ALTER TABLE orders ALTER COLUMN customer_segment SET NOT NULL;

-- Phase 4: Remove old column in next release (after all consumers updated)
-- ALTER TABLE orders DROP COLUMN old_customer_type;
```

### Incompatible Changes — Use View Shim

```sql
-- Rename column safely:
-- Step 1: Add new column, keep old
ALTER TABLE orders ADD COLUMN order_reference VARCHAR(50);
UPDATE orders SET order_reference = order_id_legacy;

-- Step 2: Create view with old name for backward compatibility
CREATE OR REPLACE VIEW orders_v1 AS
SELECT *, order_reference AS order_id_legacy FROM orders;

-- Step 3: After all consumers switched, drop old column
```

---

## Airflow DAG Versioning

```python
# dag_id includes version — old and new DAGs coexist during transition
DAG_ID = "etl_orders_v2"   # previous: etl_orders_v1

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    tags=["orders", "v2"],
) as dag:
    ...
```

```python
# Transition pattern:
# Week 1: run both v1 and v2 in parallel, compare results
# Week 2: unpause v2, pause v1 (don't delete — preserve history)
# Week 3: delete v1 DAG file after v2 proven stable

# Pause old DAG (keep for rollback)
airflow dags pause etl_orders_v1

# Verify new DAG runs successfully
airflow dags trigger etl_orders_v2 --run-id smoke_test_$(date +%s)
```

---

## Deployment Checklist

```
Pre-deploy (green build):
[ ] Green deployment passes all unit and integration tests
[ ] Smoke tests pass against green preview service
[ ] Database migrations are backward-compatible
[ ] Feature flags enabled for gradual rollout (if applicable)
[ ] Rollback procedure documented and tested in staging

Cutover:
[ ] Monitor error rate for 5 min after traffic switch
[ ] Validate key metrics (pipeline success rate, query latency)
[ ] Keep blue deployment scaled at 0 (not deleted) for 30 min

Post-deploy:
[ ] Blue deployment confirmed healthy and ready for rollback
[ ] After 24h: delete blue deployment and old DB schema
[ ] Update runbook with new version numbers
```

---

## Canary vs Blue/Green Decision

| Criteria | Blue/Green | Canary |
|----------|-----------|--------|
| Risk tolerance | Low — instant rollback | Medium — gradual exposure |
| Traffic control | All-or-nothing switch | Percentage-based (10%→50%→100%) |
| Database migrations | Required to be backward-compatible | Same requirement |
| Infra cost | 2x during transition | 2x during rollout |
| Best for | Schema changes, major upgrades | API changes, new features |
| Rollback speed | Instant (service selector) | Fast (reduce canary weight to 0%) |

---

## Anti-Patterns

1. **Breaking database changes without backward compatibility** — old app version writes to renamed column and breaks; always keep old column until all consumers migrated.
2. **Deleting blue deployment immediately after cutover** — no rollback possible if green has issues; keep blue at 0 replicas for at least 30 minutes.
3. **Blue/green for stateful workloads with shared storage** — both versions write to the same database simultaneously, causing corruption; coordinate write cutover before switching reads.
4. **No smoke tests before promotion** — promoting an unhealthy green version silently breaks production; always run health checks against preview before cutover.
5. **Changing dag_id during upgrade** — Airflow treats it as a new DAG, losing all run history; use versioned dag_id from the start.

---

## References

- Argo Rollouts blue/green: `argo-rollouts.readthedocs.io/en/stable/features/bluegreen/`
- Kubernetes service selector: `kubernetes.io/docs/concepts/services-networking/service/`
- dbt swap_schema: `docs.getdbt.com/docs/build/hooks-and-operations`
- Expand/contract DB migrations: `martinfowler.com/bliki/ParallelChange.html`
- Related skills: `[[infra-gitops-deployment-review]]`, `[[dataops-cicd-pipeline-review]]`, `[[dataops-release-readiness-review]]`
