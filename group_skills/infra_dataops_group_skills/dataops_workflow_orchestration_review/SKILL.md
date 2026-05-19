---
name: dataops-workflow-orchestration-review
description: Workflow orchestration comparison and review — Airflow vs Prefect vs Dagster vs Temporal (architecture, execution model, dependency management, observability), when to choose each, migration patterns from Airflow to Prefect/Dagster, orchestrator evaluation scorecard, event-driven vs schedule-based triggering, Software-Defined Assets (Dagster) vs task-based (Airflow), hybrid orchestration patterns
---

# Workflow Orchestration Review

## When to Use

- Selecting an orchestration platform for a new data platform
- Evaluating migration from Airflow to a modern alternative
- Comparing orchestration capabilities for a specific use case
- Reviewing architecture of existing orchestration setup
- Designing event-driven pipeline triggers

---

## Orchestrator Comparison

| Feature | Airflow | Prefect | Dagster | Temporal |
|---------|---------|---------|---------|---------|
| **Model** | DAG task graph | Flow/task functions | Asset-centric | Durable workflow functions |
| **Scheduling** | Cron + sensors | Deployments + schedules | Partitions + schedules | Timer signals |
| **State store** | PostgreSQL | PostgreSQL/SQLite | PostgreSQL/SQLite | Cassandra/PostgreSQL |
| **Execution** | Distributed (K8s/Celery) | Work pools (K8s/Docker/Process) | Op launchers | Worker process |
| **Observability** | Limited (logs + metadata DB) | Built-in UI + events | Asset catalog + lineage | Workflow history |
| **Data lineage** | External (OpenLineage) | None native | Built-in (Assets) | None native |
| **Learning curve** | High | Medium | Medium | High |
| **Best for** | ETL/ELT, well-established pipelines | Python data pipelines, ML | dbt + asset lineage, BI | Long-running workflows, sagas |

---

## Airflow: When to Choose

```python
# Choose Airflow when:
# - Team already knows Airflow, large operator ecosystem
# - Complex DAG dependencies with 100+ tasks
# - Integration with Hadoop/Spark ecosystem
# - Enterprise: Astronomer/Google Composer/MWAA support needed

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG("etl_orders", start_date=datetime(2024,1,1), schedule="@daily") as dag:
    extract = PythonOperator(task_id="extract", python_callable=extract_fn)
    transform = PythonOperator(task_id="transform", python_callable=transform_fn)
    load = PythonOperator(task_id="load", python_callable=load_fn)

    extract >> transform >> load
```

---

## Prefect: When to Choose

```python
# Choose Prefect when:
# - Python-first workflows without YAML/DAG overhead
# - Dynamic workflows that change structure at runtime
# - Rapid iteration needed (no DAG file structure requirements)

from prefect import flow, task
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

@task(retries=3, retry_delay_seconds=60)
def extract_orders(ds: str) -> list[dict]:
    ...

@task
def transform_orders(orders: list[dict]) -> list[dict]:
    ...

@task
def load_orders(orders: list[dict], ds: str) -> int:
    ...

@flow(name="etl-orders", log_prints=True)
def etl_orders_flow(ds: str):
    orders = extract_orders(ds)
    transformed = transform_orders(orders)
    count = load_orders(transformed, ds)
    return count

# Deploy with schedule
deployment = Deployment.build_from_flow(
    flow=etl_orders_flow,
    name="daily-orders",
    schedule=CronSchedule(cron="0 2 * * *", timezone="UTC"),
    work_pool_name="kubernetes-pool",
)
deployment.apply()
```

---

## Dagster: When to Choose

```python
# Choose Dagster when:
# - Asset-centric thinking (what data exists, not what tasks run)
# - dbt integration is central to the platform
# - Partition-aware incremental processing
# - Data lineage and catalog are first-class requirements

from dagster import asset, AssetIn, DailyPartitionsDefinition

partitions = DailyPartitionsDefinition(start_date="2024-01-01")

@asset(
    partitions_def=partitions,
    group_name="bronze",
    description="Raw orders from source system",
)
def raw_orders(context) -> None:
    ds = context.partition_key
    extract_and_store(ds, "s3://data-lake/bronze/orders/")

@asset(
    ins={"raw_orders": AssetIn()},
    partitions_def=partitions,
    group_name="silver",
    description="Cleaned and deduplicated orders",
)
def orders(context, raw_orders) -> None:
    ds = context.partition_key
    transform_and_store(ds)

# Dagster UI shows: asset graph, partition status, lineage
# dbt integration: load dbt project as Dagster assets
from dagster_dbt import dbt_assets, DbtProject

dbt_project = DbtProject(project_dir="dbt/")

@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_analytics(context, dbt):
    yield from dbt.cli(["build"], context=context).stream()
```

---

## Temporal: When to Choose

```python
# Choose Temporal when:
# - Long-running workflows (hours to days)
# - Saga pattern: multi-step transactions with compensation
# - Exactly-once execution guarantees required
# - Microservice coordination (not just data pipelines)

from temporalio import activity, workflow
from datetime import timedelta

@activity.defn
async def validate_order(order_id: str) -> bool:
    ...

@activity.defn
async def charge_payment(order_id: str, amount: float) -> str:
    ...

@activity.defn
async def ship_order(order_id: str, payment_id: str) -> None:
    ...

@activity.defn
async def refund_payment(payment_id: str) -> None:
    ...

@workflow.defn
class OrderFulfillmentWorkflow:
    @workflow.run
    async def run(self, order_id: str, amount: float) -> str:
        is_valid = await workflow.execute_activity(
            validate_order,
            order_id,
            start_to_close_timeout=timedelta(minutes=5),
        )
        if not is_valid:
            raise ValueError("Invalid order")

        payment_id = await workflow.execute_activity(
            charge_payment,
            args=[order_id, amount],
            start_to_close_timeout=timedelta(minutes=10),
        )

        try:
            await workflow.execute_activity(
                ship_order,
                args=[order_id, payment_id],
                start_to_close_timeout=timedelta(hours=1),
            )
        except Exception:
            # Compensation: refund on ship failure
            await workflow.execute_activity(refund_payment, payment_id)
            raise

        return payment_id
```

---

## Event-Driven Triggering Patterns

```python
# Airflow: Dataset-based triggering
from airflow import Dataset

bronze_orders = Dataset("s3://data-lake/bronze/orders/")
silver_orders = Dataset("s3://data-lake/silver/orders/")

# DAG A: produces bronze_orders
with DAG("extract_orders", schedule="0 1 * * *") as dag:
    PythonOperator(
        task_id="extract",
        python_callable=extract_fn,
        outlets=[bronze_orders],   # marks dataset as updated
    )

# DAG B: triggered when bronze_orders is updated
with DAG("transform_orders", schedule=[bronze_orders]) as dag:
    PythonOperator(
        task_id="transform",
        python_callable=transform_fn,
        outlets=[silver_orders],
    )
```

```python
# Prefect: event-driven deployments
from prefect.events import DeploymentEventTrigger

# Trigger when S3 event received
Deployment.build_from_flow(
    flow=transform_flow,
    name="transform-on-upload",
    triggers=[
        DeploymentEventTrigger(
            expect={"prefect.flow-run.Completed"},
            match_related={"prefect.resource.name": "extract-orders"},
        )
    ],
)
```

---

## Migration from Airflow to Dagster

```python
# Migration strategy:
# 1. Run Dagster alongside Airflow (parallel operation)
# 2. Migrate lowest-risk DAGs first
# 3. Use Dagster's Airflow migration utilities

# Dagster migration table
MIGRATION_MAP = {
    "airflow_concept": "dagster_equivalent",
    "DAG": "Job / Asset graph",
    "Task": "Op / Asset",
    "TaskInstance": "Op execution",
    "XCom": "IO Manager / Output",
    "Variable": "Resource / Config",
    "Connection": "Resource",
    "Pool": "Concurrency limit",
    "Sensor": "Sensor / Auto-materialize",
    "schedule_interval": "ScheduleDefinition / cron_schedule",
}

# Dagster equivalent of Airflow DAG:
from dagster import job, op, ScheduleDefinition

@op
def extract(): ...

@op
def transform(orders): ...

@op
def load(transformed): ...

@job
def etl_orders():
    load(transform(extract()))

daily_schedule = ScheduleDefinition(
    job=etl_orders,
    cron_schedule="0 2 * * *",
)
```

---

## Evaluation Scorecard

```
Score each 1-5 for your use case:

                            Airflow  Prefect  Dagster  Temporal
Operator ecosystem            5        2        3        1
Python ergonomics             2        5        4        5
Asset lineage                 2        2        5        1
Learning curve (lower=easier) 2        4        4        2
Cloud managed option          5        5        4        2
dbt integration               3        3        5        1
Long-running workflows        2        3        3        5
Community/adoption            5        4        4        3

Decision: highest total score for your weighted criteria
```

---

## Anti-Patterns

1. **Choosing orchestrator based on hype** — evaluate against your actual use cases (number of DAGs, team Python skills, data lineage needs); wrong tool is expensive to migrate from.
2. **Running all orchestrators simultaneously** — Airflow for ETL + Prefect for ML + Dagster for analytics creates operational complexity; standardize on 1-2 max.
3. **Migrating all DAGs at once** — migrate incrementally; start with the 5 lowest-risk DAGs to learn the new system before committing.
4. **Using Airflow for dynamic/branching logic** — complex Airflow DAGs with PythonBranchOperator become unreadable; Prefect's native Python flow is much cleaner for conditional logic.
5. **Not evaluating managed options** — self-hosted Dagster/Airflow requires dedicated DevOps effort; Astronomer/Prefect Cloud/MWAA can be more cost-effective for small teams.

---

## References

- Airflow: `airflow.apache.org`
- Prefect: `docs.prefect.io`
- Dagster: `docs.dagster.io`
- Temporal: `docs.temporal.io`
- Dagster dbt integration: `docs.dagster.io/integrations/dbt`
- Related skills: `[[airflow-dags]]`, `[[dagster-assets]]`, `[[prefect-workflows]]`
