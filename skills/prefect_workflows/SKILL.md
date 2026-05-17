---
name: prefect-workflows
description: Prefect 3.x workflows — flows, tasks, deployments, work pools, event-driven triggers, caching, retries, state hooks, Prefect Cloud/server, Python SDK, Docker/K8s infrastructure
---

# Prefect Workflows

## When to Use

Activate this skill when the task involves:
- Authoring Prefect 3.x flows and tasks with `@flow` / `@task` decorators
- Configuring retries, caching, timeouts, and concurrency limits on tasks
- Building deployments via `prefect.yaml` or `flow.serve()`
- Setting up Work Pools (Process, Docker, Kubernetes, ECS)
- Implementing state change hooks (`on_completion`, `on_failure`)
- Using event-driven triggers or automations on Prefect Cloud
- Migrating Airflow DAGs to Prefect
- Running parallel workloads with `.submit()`, `.map()`, or `DaskTaskRunner`

### Prefect vs Airflow vs Dagster

| Dimension | Prefect 3.x | Airflow 2.x | Dagster |
|-----------|-------------|-------------|---------|
| Execution model | Python-native flows; any Python is valid | DAG of operators; strict graph structure | Asset-centric; tracks data objects |
| Scheduling | Cron / interval / RRule + event triggers | Cron / timetable + sensors | Schedules + automation conditions |
| Deployment | `prefect deploy` or `.serve()` | DAG file drop | `dagster deploy` / Cloud |
| Dynamic work | `.map()` / `.submit()` inline | Dynamic task mapping (2.3+) | Dynamic partitions |
- Choose **Prefect** when you want minimal boilerplate, event-driven triggers, and flexibility to run arbitrary Python without wrapping operators.
- Choose **Airflow** when your organization already runs it at scale or needs rich ecosystem connectors (Astronomer, MWAA).
- Choose **Dagster** when you need asset lineage, staleness tracking, and a data-catalog-style UI.

---

## Installation

```bash
pip install prefect                        # core
pip install prefect-aws                    # S3, ECS, Secrets Manager
pip install prefect-gcp                    # GCS, BigQuery, Cloud Run
pip install prefect-docker                 # DockerContainer runner
pip install prefect-kubernetes             # KubernetesJob runner
pip install prefect-dask                   # DaskTaskRunner
pip install prefect-ray                    # RayTaskRunner
```

Start a local Prefect server (SQLite backend):

```bash
prefect server start          # UI at http://127.0.0.1:4200
```

Or connect to Prefect Cloud:

```bash
prefect cloud login --key pnu_XXXX        # paste API key
prefect config set PREFECT_API_URL="https://api.prefect.cloud/api/accounts/.../workspaces/..."
```

---

## Core Concepts

```
Flow  ─── contains ──►  Tasks (units of work)
  │                        │
  │  runs as a            └── may submit futures → PrefectFuture
  │  Deployment               may call nested Flows (subflows)
  │
  └─ scheduled by  ──►  Schedule (cron/interval/rrule)
                          OR
                         Trigger (event-driven, Prefect Cloud)

Deployment  ──builds──►  Flow code + infrastructure config
  │
  └─ assigns ──►  Work Pool  ──polls──►  Worker process
                    │
                    └─ types: Process | Docker | Kubernetes | ECS
```

| Concept | Role |
|---------|------|
| **Flow** | Python function decorated with `@flow`; the top-level schedulable unit |
| **Task** | Python function decorated with `@task`; atomic unit with retry/cache/state |
| **State** | Result wrapper: Completed, Failed, Crashed, Cancelled, Pending, Running |
| **Artifact** | Human-readable output attached to a flow/task run (markdown, table, link) |
| **Block** | Named, versioned credential/config object stored in Prefect (S3Bucket, Secret, etc.) |
| **Work Pool** | Named queue backed by an infrastructure type (Process/Docker/K8s/ECS) |
| **Deployment** | Registered version of a flow with schedule, parameters, and work pool |
| **Worker** | Long-running process that polls a work pool and executes flow runs |

---

## Flow and Task Authoring

### `@flow` Decorator

```python
from prefect import flow
from prefect.logging import get_run_logger

@flow(
    name="ingest-daily-events",
    description="Load raw events from S3 and write to DWH",
    retries=1,
    retry_delay_seconds=60,
    timeout_seconds=3600,
    log_prints=True,               # redirect print() to Prefect logger
)
def ingest_daily_events(
    event_date: str,
    source_bucket: str = "data-lake-raw",
    target_schema: str = "bronze",
) -> dict:
    logger = get_run_logger()
    logger.info(f"Starting ingestion for {event_date}")

    files = list_s3_files(bucket=source_bucket, prefix=f"events/{event_date}/")
    logger.info(f"Found {len(files)} files")

    results = []
    for f in files:
        row_count = load_file_to_dwh(s3_key=f, schema=target_schema)
        results.append(row_count)

    total = sum(results)
    logger.info(f"Loaded {total} rows total")
    return {"rows_loaded": total, "files_processed": len(files)}
```

### `@task` Decorator

```python
from prefect import task
from prefect.cache_policies import INPUTS
import pandas as pd
import boto3

@task(
    name="read-parquet-from-s3",
    retries=3,
    retry_delay_seconds=[10, 30, 60],   # exponential-style backoff list
    timeout_seconds=300,
    tags=["s3", "io"],
    cache_policy=INPUTS,                # cache based on all task inputs
    cache_expiration=timedelta(hours=1),
)
def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(response["Body"])


@task(
    name="validate-schema",
    retries=0,
    tags=["validation"],
)
def validate_schema(df: pd.DataFrame, required_columns: list[str]) -> pd.DataFrame:
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    return df


@task(name="write-to-warehouse", retries=2, retry_delay_seconds=30)
def write_to_warehouse(df: pd.DataFrame, table: str, schema: str = "bronze") -> int:
    from sqlalchemy import create_engine
    import os
    engine = create_engine(os.environ["WAREHOUSE_CONN"])
    df.to_sql(table, engine, schema=schema, if_exists="append", index=False)
    return len(df)
```

### Task Result Caching

```python
from datetime import timedelta
from prefect import task
from prefect.cache_policies import INPUTS, TASK_SOURCE, NO_CACHE
from prefect.cache_policies import CachePolicy

# Cache by all task input arguments (most common DE pattern)
@task(cache_policy=INPUTS, cache_expiration=timedelta(hours=6))
def fetch_exchange_rates(base_currency: str, target_date: str) -> dict:
    """Expensive API call; cache result for 6 hours per input combo."""
    import httpx
    resp = httpx.get(
        f"https://api.exchangerate-api.com/v4/latest/{base_currency}",
        params={"date": target_date},
    )
    resp.raise_for_status()
    return resp.json()["rates"]


# Custom cache key — cache per date only, ignoring other params
from prefect.cache_policies import CacheKeyFnPolicy

def cache_by_date(context, parameters):
    return parameters["event_date"]

@task(cache_policy=CacheKeyFnPolicy(cache_key_fn=cache_by_date),
      cache_expiration=timedelta(days=1))
def load_dimension_table(event_date: str, conn_string: str) -> pd.DataFrame:
    """Re-use cached result for same date regardless of conn_string."""
    ...


# Disable caching explicitly
@task(cache_policy=NO_CACHE)
def send_alert(message: str) -> None:
    """Side effects must never be cached."""
    ...
```

### Passing Results Between Tasks

```python
from prefect import flow, task

@flow(name="orders-etl")
def orders_etl(run_date: str) -> dict:
    # Direct (synchronous) call — blocks until complete, returns value
    raw_df = read_parquet_from_s3(
        bucket="data-lake-raw",
        key=f"orders/{run_date}/part-00000.parquet",
    )
    validated_df = validate_schema(raw_df, required_columns=["order_id", "amount"])
    rows = write_to_warehouse(validated_df, table="orders_raw")
    return {"rows": rows}
```

### `submit()` vs Direct Call — Parallelism

```python
from prefect import flow, task
from prefect.futures import PrefectFuture

@task
def process_partition(partition_key: str) -> int:
    """Process one date partition — runs in parallel via submit()."""
    df = read_partition(partition_key)
    return write_partition(df, partition_key)


@flow(name="backfill-partitions")
def backfill_partitions(start_date: str, end_date: str) -> int:
    import pandas as pd
    date_range = pd.date_range(start_date, end_date, freq="D")
    partition_keys = [d.strftime("%Y-%m-%d") for d in date_range]

    # submit() returns PrefectFuture immediately — all partitions run concurrently
    futures: list[PrefectFuture] = [
        process_partition.submit(pk) for pk in partition_keys
    ]

    # .result() blocks until each future completes; raises on failure
    results = [f.result() for f in futures]
    return sum(results)
```

`.map()` is syntactic sugar for submitting a list:

```python
@flow(name="parallel-map-example")
def parallel_map(event_date: str) -> list[int]:
    keys = list_s3_files(bucket="data-lake", prefix=f"events/{event_date}/")
    # Each key processed in a separate concurrent task run
    futures = process_partition.map(keys)
    return [f.result() for f in futures]
```

### Nested Flows (Subflows)

```python
from prefect import flow

@flow(name="load-one-source")
def load_one_source(source: str, run_date: str) -> dict:
    files = list_files(source, run_date)
    futures = process_file.map(files)
    return {"source": source, "files": len(files), "rows": sum(f.result() for f in futures)}


@flow(name="daily-multi-source-load", log_prints=True)
def daily_multi_source_load(run_date: str) -> dict:
    """Orchestrator flow calling subflows per source."""
    sources = ["orders", "customers", "products", "sessions"]
    totals = {}
    for source in sources:
        # Subflow call — appears as nested run in UI; failures bubble up
        result = load_one_source(source=source, run_date=run_date)
        totals[source] = result["rows"]
        print(f"  {source}: {result['rows']} rows")
    return totals
```

---

## State and Error Handling

### State Types

| State | Meaning |
|-------|---------|
| `Pending` | Run is queued, not yet started |
| `Running` | Actively executing |
| `Completed` | Finished successfully |
| `Failed` | Raised an exception |
| `Crashed` | Infrastructure-level failure (OOM, node eviction) |
| `Cancelled` | Cancelled by user or automation |
| `Paused` | Waiting for human approval (`pause_flow_run`) |

### State Change Hooks

```python
from prefect import flow, task
from prefect.context import get_run_context
import httpx


def on_flow_failure(flow, flow_run, state):
    """Called when the flow transitions to Failed state."""
    error = state.result(raise_on_failure=False)
    httpx.post(
        "https://hooks.slack.com/services/XXX/YYY/ZZZ",
        json={
            "text": (
                f":red_circle: *{flow.name}* failed\n"
                f"Run: {flow_run.name}\n"
                f"Error: {error}"
            )
        },
    )


def on_flow_completion(flow, flow_run, state):
    result = state.result()
    httpx.post(
        "https://hooks.slack.com/services/XXX/YYY/ZZZ",
        json={"text": f":white_check_mark: *{flow.name}* completed — {result}"},
    )


@flow(
    name="monitored-etl",
    on_failure=[on_flow_failure],
    on_completion=[on_flow_completion],
)
def monitored_etl(run_date: str) -> str:
    rows = run_pipeline(run_date)
    return f"Loaded {rows} rows for {run_date}"


# Task-level hooks
def on_task_retry(task, task_run, state):
    print(f"Retrying {task.name}, attempt {task_run.run_count}")


@task(on_failure=[on_task_retry])
def fragile_api_call(endpoint: str) -> dict:
    ...
```

### `allow_failure()` — Optional Task Results

```python
from prefect import flow, task, allow_failure

@task
def enrich_with_geo(df: pd.DataFrame) -> pd.DataFrame:
    """Geo enrichment — non-critical; pipeline continues if this fails."""
    return call_geo_api(df)


@flow(name="orders-with-optional-enrichment")
def orders_enriched(run_date: str):
    orders = load_orders(run_date)

    # allow_failure wraps the result — downstream gets None on failure
    geo_result = allow_failure(enrich_with_geo)(orders)

    if isinstance(geo_result, pd.DataFrame):
        orders = geo_result       # use enriched data
    else:
        print("Geo enrichment failed — continuing without it")

    write_to_warehouse(orders, table="orders_enriched")
```

### `Pause` for Human-in-the-Loop Approval

```python
from prefect import flow, task, pause_flow_run
from prefect.input import RunInput

class ApprovalInput(RunInput):
    approved: bool
    approver: str = "unknown"


@flow(name="schema-migration-with-approval")
def schema_migration(target_table: str, new_schema: str):
    preview_sql = generate_migration_sql(target_table, new_schema)
    print(f"Proposed SQL:\n{preview_sql}")

    # Pause and wait for a human to submit ApprovalInput via API/UI
    approval: ApprovalInput = pause_flow_run(
        wait_for_input=ApprovalInput,
        timeout=86400,   # 24 h max wait
    )

    if not approval.approved:
        raise ValueError(f"Migration rejected by {approval.approver}")

    execute_sql(preview_sql)
    print(f"Migration applied by {approval.approver}")
```

### `Abort` — Stop Without Retry

```python
from prefect import task
from prefect.exceptions import Abort

@task
def validate_data_contract(df: pd.DataFrame, contract_version: str) -> pd.DataFrame:
    """Hard stop if contract version is unrecognized — retrying won't help."""
    known_versions = {"v1", "v2", "v3"}
    if contract_version not in known_versions:
        raise Abort(f"Unknown contract version: {contract_version!r} — aborting, not retrying")
    validate(df, contract_version)
    return df
```

---

## Deployments and Work Pools

### `prefect.yaml` Manifest

```yaml
# prefect.yaml — lives at the root of your project
name: data-platform-flows
prefect-version: "3.0.0"

# ---------- Build step (build Docker image, push to registry) ----------
build:
  - prefect_docker.deployments.steps.build_docker_image:
      id: build-image
      requires: prefect-docker>=0.4.0
      image_name: ghcr.io/my-org/data-platform
      tag: "{{ $GITHUB_SHA | default('latest') }}"
      dockerfile: Dockerfile
      platform: linux/amd64

# ---------- Push step ----------
push:
  - prefect_docker.deployments.steps.push_docker_image:
      requires: prefect-docker>=0.4.0
      image_name: "{{ build-image.image_name }}"
      tag: "{{ build-image.tag }}"

# ---------- Pull step (runs inside the worker) ----------
pull:
  - prefect.deployments.steps.set_working_directory:
      directory: /app

# ---------- Deployments ----------
deployments:

  - name: orders-etl-daily
    description: "Daily orders ingestion — runs at 05:00 UTC"
    flow_name: orders-etl               # must match @flow(name=...)
    entrypoint: flows/orders_etl.py:orders_etl
    work_pool:
      name: kubernetes-pool
      work_queue_name: default
      job_variables:
        image: "{{ build-image.image_name }}:{{ build-image.tag }}"
        namespace: data-platform
        cpu_request: "500m"
        memory_request: "1Gi"
        cpu_limit: "2000m"
        memory_limit: "4Gi"
    parameters:
      source_bucket: data-lake-raw
      target_schema: bronze
    schedules:
      - cron: "0 5 * * *"
        timezone: UTC
        active: true

  - name: backfill-on-demand
    description: "On-demand backfill — triggered programmatically"
    flow_name: backfill-partitions
    entrypoint: flows/backfill.py:backfill_partitions
    work_pool:
      name: docker-pool
      job_variables:
        image: "{{ build-image.image_name }}:{{ build-image.tag }}"
        env:
          WAREHOUSE_CONN: "{{ prefect.blocks.secret/warehouse-conn }}"
    schedules: []         # no schedule — manual or event-triggered
```

Deploy all entries:

```bash
prefect deploy --all
# or deploy a single deployment by name:
prefect deploy --name orders-etl-daily
```

### Work Pool Types

| Work Pool Type | When to Use |
|----------------|-------------|
| `process` | Local dev, single-machine CI/CD |
| `docker` | Docker-capable host, isolated dependencies |
| `kubernetes` | Production K8s cluster |
| `ecs` | AWS ECS Fargate — serverless containers |
| `cloud-run` | GCP Cloud Run — serverless containers |

Create a work pool via CLI:

```bash
# Local process pool
prefect work-pool create dev-pool --type process

# Docker pool
prefect work-pool create docker-pool --type docker

# Kubernetes pool (uses in-cluster service account by default)
prefect work-pool create kubernetes-pool --type kubernetes

# Start a worker that polls this pool
prefect worker start --pool kubernetes-pool
```

### Serving a Flow Locally

```python
# flows/orders_etl.py — run this script directly for local serving
from prefect import flow
from prefect.schedules import CronSchedule

@flow(name="orders-etl")
def orders_etl(event_date: str, source_bucket: str = "data-lake-raw") -> dict:
    ...

if __name__ == "__main__":
    # Registers a deployment AND starts polling in-process
    orders_etl.serve(
        name="orders-etl-local",
        cron="0 5 * * *",
        parameters={"source_bucket": "data-lake-raw"},
        tags=["local", "dev"],
    )
```

```bash
python flows/orders_etl.py      # starts serving; Ctrl+C to stop
```

---

## Schedules and Triggers

### Cron Schedule

```yaml
# In prefect.yaml
schedules:
  - cron: "0 5 * * *"        # every day at 05:00 UTC
    timezone: "America/New_York"
    active: true
```

```python
# Programmatic via Python SDK
from prefect.client.orchestration import get_client
from prefect.schedules import CronSchedule
import asyncio

async def add_schedule():
    async with get_client() as client:
        deployment = await client.read_deployment_by_name("orders-etl/orders-etl-daily")
        await client.update_deployment_schedule(
            deployment.id,
            schedule_id=deployment.schedules[0].id,
            active=True,
        )
asyncio.run(add_schedule())
```

### Interval and RRule Schedules

```yaml
schedules:
  # Every 6 hours
  - interval: 21600           # seconds
    anchor_date: "2024-01-01T00:00:00Z"
    timezone: UTC

  # RRule — every weekday at 06:30
  - rrule: "FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR;BYHOUR=6;BYMINUTE=30"
    timezone: "Europe/Berlin"
```

### Event-Driven Triggers (Prefect Cloud)

Automations let you trigger a deployment when a specific event fires — e.g., when a flow completes, when a custom event is emitted, or when a metric threshold is crossed.

```python
# Emit a custom event from any flow/task
from prefect.events import emit_event

@task
def load_file_to_s3(local_path: str, s3_key: str) -> str:
    upload(local_path, s3_key)
    emit_event(
        event="data.file.uploaded",
        resource={"prefect.resource.id": f"s3://{s3_key}"},
        payload={"bytes": get_file_size(local_path), "key": s3_key},
    )
    return s3_key
```

Configure the trigger in Prefect Cloud UI or via Terraform/API:

```json
{
  "name": "Trigger transform after upload",
  "trigger": {
    "type": "EventTrigger",
    "match": { "prefect.resource.id": "s3://data-lake-raw/*" },
    "expect": ["data.file.uploaded"],
    "threshold": 1,
    "within": 60
  },
  "actions": [{
    "type": "run-deployment",
    "deployment_id": "<uuid>",
    "parameters": { "s3_key": "{{ event.payload.key }}" }
  }]
}
```

### Programmatic Trigger — `run_deployment()`

```python
from prefect.deployments import run_deployment

async def trigger_backfill(start_date: str, end_date: str):
    """Trigger a deployment run and wait for it to complete."""
    flow_run = await run_deployment(
        name="backfill-partitions/backfill-on-demand",
        parameters={"start_date": start_date, "end_date": end_date},
        timeout=0,           # 0 = fire-and-forget; positive int = wait N seconds
        tags=["programmatic"],
    )
    return flow_run.id
```

Synchronous alternative:

```python
import asyncio
flow_run_id = asyncio.run(trigger_backfill("2024-01-01", "2024-01-31"))
print(f"Flow run ID: {flow_run_id}")
```

---

## Infrastructure Blocks

### DockerContainer Runner

```python
# Save block once:
from prefect_docker import DockerContainer

block = DockerContainer(
    image="ghcr.io/my-org/data-platform:latest",
    image_pull_policy="ALWAYS",
    env={"WAREHOUSE_CONN": "postgresql://..."},
    volumes=["data-volume:/data"],
    auto_remove=True,
    labels={"team": "data-platform"},
)
block.save("data-platform-docker", overwrite=True)
```

Reference in `prefect.yaml`:

```yaml
work_pool:
  name: docker-pool
  job_variables:
    image: ghcr.io/my-org/data-platform:latest
    image_pull_policy: ALWAYS
    env:
      WAREHOUSE_CONN: "{{ prefect.blocks.secret/warehouse-conn }}"
    volumes:
      - data-volume:/data
    auto_remove: true
```

### KubernetesJob Runner

```yaml
# In prefect.yaml job_variables for kubernetes work pool
work_pool:
  name: kubernetes-pool
  job_variables:
    image: ghcr.io/my-org/data-platform:latest
    namespace: data-platform
    service_account_name: prefect-worker-sa
    image_pull_secrets:
      - name: ghcr-credentials
    finished_job_ttl: 300       # clean up completed jobs after 5 min
    env:
      - name: WAREHOUSE_CONN
        valueFrom:
          secretKeyRef:
            name: warehouse-secret
            key: connection-string
    resources:
      requests:
        cpu: "500m"
        memory: "2Gi"
      limits:
        cpu: "2000m"
        memory: "8Gi"
    tolerations:
      - key: "dedicated"
        operator: "Equal"
        value: "data-jobs"
        effect: "NoSchedule"
```

### GitHub Storage Pull Step

```yaml
# Pull flow code from a private GitHub repo at runtime
pull:
  - prefect.deployments.steps.git_clone:
      id: clone-repo
      requires: prefect>=3.0.0
      repository: https://github.com/my-org/data-platform.git
      branch: main
      access_token: "{{ prefect.blocks.secret/github-token }}"

  - prefect.deployments.steps.pip_install_requirements:
      requirements_file: requirements.txt
      stream_output: true
```

---

## Concurrency and Parallelism

### `.submit()` → PrefectFuture

```python
from prefect import flow, task
from prefect.futures import wait, PrefectFuture
from typing import Sequence

@task(retries=2, retry_delay_seconds=10)
def transform_partition(partition_id: str, run_date: str) -> dict:
    df = read_partition(partition_id, run_date)
    df = apply_business_rules(df)
    rows = write_partition(df, partition_id)
    return {"partition": partition_id, "rows": rows}


@flow(name="parallel-partitioned-etl")
def parallel_partitioned_etl(run_date: str, max_workers: int = 8) -> dict:
    partitions = get_partition_list(run_date)

    # Submit all — returns futures immediately
    futures: list[PrefectFuture] = [
        transform_partition.submit(pid, run_date)
        for pid in partitions
    ]

    # Wait for all to complete (returns completed futures)
    done, failed = wait(futures, timeout=3600)

    results = [f.result() for f in done]
    total_rows = sum(r["rows"] for r in results)

    if failed:
        raise RuntimeError(f"{len(failed)} partitions failed: {[f.name for f in failed]}")

    return {"partitions": len(done), "rows": total_rows}
```

### `.map()` for Bulk Parallelism

```python
@task
def validate_file(s3_key: str, schema_version: str) -> bool:
    df = read_parquet_from_s3("data-lake", s3_key)
    return validate_schema_version(df, schema_version)


@flow(name="validate-all-files")
def validate_all_files(run_date: str, schema_version: str = "v3") -> list[bool]:
    keys = list_s3_files("data-lake", f"events/{run_date}/")

    # map() expands the first positional arg; use partial for fixed args
    futures = validate_file.map(keys, schema_version=schema_version)
    return [f.result() for f in futures]
```

### ConcurrencyLimits

Global concurrency slots prevent overloading downstream systems.

```bash
# Create a limit named "warehouse-writes" with 5 slots
prefect concurrency-limit create warehouse-writes 5
```

```python
from prefect.concurrency.sync import concurrency

@task
def write_to_warehouse_limited(df: pd.DataFrame, table: str) -> int:
    # Acquires a slot from "warehouse-writes" before executing; releases on exit
    with concurrency("warehouse-writes", occupy=1):
        engine = get_engine()
        df.to_sql(table, engine, if_exists="append", index=False)
        return len(df)
```

Async version:

```python
from prefect.concurrency.asyncio import concurrency as async_concurrency

async def async_write(df, table):
    async with async_concurrency("warehouse-writes", occupy=1):
        await async_engine_write(df, table)
```

### Task Runners

Task runners control how submitted tasks are executed.

```python
from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner

# ThreadPoolTaskRunner (default) — thread-based, good for I/O-bound work
@flow(task_runner=ThreadPoolTaskRunner(max_workers=16))
def io_bound_pipeline(run_date: str):
    keys = list_s3_files("data-lake", f"raw/{run_date}/")
    futures = read_parquet_from_s3.map("data-lake", keys)
    return [f.result() for f in futures]
```

```python
from prefect_dask import DaskTaskRunner

# DaskTaskRunner — distributed compute for CPU-bound or large-scale work
@flow(
    task_runner=DaskTaskRunner(
        cluster_kwargs={
            "n_workers": 4,
            "threads_per_worker": 2,
            "memory_limit": "4GB",
        }
    )
)
def cpu_bound_pipeline(run_date: str):
    partitions = get_partition_list(run_date)
    futures = transform_partition.map(partitions, run_date=run_date)
    return [f.result() for f in futures]
```

```python
from prefect_ray import RayTaskRunner

# RayTaskRunner — Ray cluster for GPU workloads or very large fan-outs
@flow(task_runner=RayTaskRunner(address="ray://ray-head:10001"))
def ml_feature_pipeline(run_date: str):
    segments = get_customer_segments()
    futures = compute_features.map(segments, run_date=run_date)
    return [f.result() for f in futures]
```

---

## Artifacts and Results

### Creating Artifacts

```python
from prefect import flow, task
from prefect.artifacts import (
    create_markdown_artifact,
    create_table_artifact,
    create_link_artifact,
    create_progress_artifact,
)
import pandas as pd


@task
def profile_dataframe(df: pd.DataFrame, table_name: str) -> dict:
    stats = {
        "rows": len(df),
        "columns": len(df.columns),
        "null_rate": df.isnull().mean().to_dict(),
    }

    # Markdown artifact — appears in flow run UI
    create_markdown_artifact(
        key=f"profile-{table_name}",
        markdown=f"""## Profile: `{table_name}`

| Metric | Value |
|--------|-------|
| Rows | {stats['rows']:,} |
| Columns | {stats['columns']} |

### Null Rates
""" + "\n".join(f"- `{col}`: {rate:.2%}" for col, rate in stats["null_rate"].items()),
        description=f"Data profile for {table_name}",
    )

    # Table artifact — renders as interactive table in UI
    create_table_artifact(
        key=f"sample-{table_name}",
        table=df.head(20).to_dict(orient="records"),
        description=f"Sample rows from {table_name}",
    )

    return stats


@task
def write_report_to_s3(stats: dict, s3_key: str) -> str:
    upload_json(stats, s3_key)
    create_link_artifact(
        key="report-link",
        link=f"https://s3.console.aws.amazon.com/s3/object/data-lake/{s3_key}",
        description="Full profiling report on S3",
    )
    return s3_key
```

### Result Storage

By default, results are stored in Prefect's database (suitable for small objects). For DataFrames and large objects, configure a result storage block.

```python
# Save an S3 result storage block once
from prefect_aws import S3Bucket

s3_results = S3Bucket(
    bucket_name="prefect-results",
    credentials=AwsCredentials.load("prod-aws-creds"),
    bucket_folder="flow-results",
)
s3_results.save("prefect-results-s3", overwrite=True)
```

```python
from prefect import flow, task
from prefect.results import S3ResultStore
from prefect_aws.s3 import S3Bucket

result_storage = S3Bucket.load("prefect-results-s3")

@task(
    result_storage=result_storage,
    result_serializer="pickle",         # or "json" for serializable objects
    persist_result=True,                # always persist (default: auto)
)
def compute_large_aggregate(run_date: str) -> pd.DataFrame:
    """Result persisted to S3; can be loaded by downstream deployments."""
    return run_heavy_aggregation(run_date)


@flow(
    name="aggregate-pipeline",
    result_storage=result_storage,
    persist_result=True,
)
def aggregate_pipeline(run_date: str) -> pd.DataFrame:
    return compute_large_aggregate(run_date)
```

Retrieve a persisted result from a prior flow run:

```python
from prefect.client.orchestration import get_client
import asyncio

async def get_flow_run_result(flow_run_id: str):
    async with get_client() as client:
        flow_run = await client.read_flow_run(flow_run_id)
        # Retrieve the serialized result via the state
        result = await flow_run.state.result(raise_on_failure=False, fetch=True)
        return result

df = asyncio.run(get_flow_run_result("<flow-run-uuid>"))
```

---

## Airflow Migration Patterns

### Concept Mapping

| Airflow 2.x | Prefect 3.x | Notes |
|-------------|-------------|-------|
| `DAG` | `@flow` | Flows are plain Python; no graph definition required |
| `@task` (TaskFlow) | `@task` | Nearly identical signature |
| `Operator` | `@task` wrapper | Wrap any operator logic in a task function |
| `DAG(schedule_interval=...)` | `cron=` in `prefect.yaml` or `.serve()` | |
| `XCom push/pull` | Return values / futures | Direct Python return values; no serialization to metadata DB |
| `Variable` | `prefect.blocks.secret/...` | Blocks are versioned and audited |
| `Connection` | `Block` (e.g., `AwsCredentials`) | Typed, validated credential objects |
| `BranchPythonOperator` | `if/else` in flow | No special operator — just Python |
| `TriggerDagRunOperator` | `run_deployment()` | |
| `ExternalTaskSensor` | Automation trigger / `run_deployment()` wait | Event-driven preferred |
| `Pool` | `ConcurrencyLimit` | Slot-based concurrency |
| `task_concurrency` | `ConcurrencyLimit` per task | |
| `PythonVirtualenvOperator` | Work pool `job_variables` image | Full env isolation via Docker/K8s |
| `@dag` + `@task` | `@flow` + `@task` | Very similar in Prefect 3.x |

### Airflow DAG → Prefect Flow Migration Example

**Before (Airflow)**:

```python
# airflow_dag.py
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="0 5 * * *", start_date=datetime(2024, 1, 1), catchup=False)
def orders_etl():
    @task
    def extract() -> list[dict]:
        return fetch_orders_api()

    @task
    def transform(raw: list[dict]) -> list[dict]:
        return [clean_order(r) for r in raw]

    @task
    def load(cleaned: list[dict]) -> int:
        return bulk_insert("orders", cleaned)

    raw = extract()
    cleaned = transform(raw)
    load(cleaned)

orders_etl()
```

**After (Prefect 3.x)**:

```python
# flows/orders_etl.py
from prefect import flow, task

@task(retries=3, retry_delay_seconds=30)
def extract() -> list[dict]:
    return fetch_orders_api()

@task
def transform(raw: list[dict]) -> list[dict]:
    return [clean_order(r) for r in raw]

@task(retries=2, retry_delay_seconds=10)
def load(cleaned: list[dict]) -> int:
    return bulk_insert("orders", cleaned)

@flow(name="orders-etl")
def orders_etl() -> int:
    raw = extract()
    cleaned = transform(raw)
    return load(cleaned)

if __name__ == "__main__":
    orders_etl.serve(name="orders-etl-daily", cron="0 5 * * *")
```

Key differences:
- No `start_date`, no `catchup` — Prefect does not backfill by default
- Retries configured directly on `@task`, not at DAG level
- XComs replaced by Python return values
- Schedule lives in `prefect.yaml` or `.serve()`, not in the flow code

---

## Complete Production Example — Incremental DWH Load

```python
# flows/incremental_load.py
"""
Incremental load: reads new Parquet files from S3 landing zone,
validates schema, deduplicates, and upserts into a Postgres warehouse table.
"""
from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

import boto3
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact, create_markdown_artifact
from prefect.cache_policies import INPUTS
from prefect.concurrency.sync import concurrency
from sqlalchemy import create_engine, text


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(
    name="list-landing-files",
    retries=2,
    retry_delay_seconds=10,
    cache_policy=INPUTS,
    cache_expiration=timedelta(minutes=5),
)
def list_landing_files(bucket: str, prefix: str) -> list[str]:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys += [obj["Key"] for obj in page.get("Contents", [])]
    return sorted(keys)


@task(
    name="read-parquet",
    retries=3,
    retry_delay_seconds=[5, 15, 45],
    tags=["s3", "io"],
)
def read_parquet(bucket: str, key: str) -> pd.DataFrame:
    logger = get_run_logger()
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(obj["Body"])
    logger.info(f"Read {len(df)} rows from s3://{bucket}/{key}")
    return df


@task(name="validate-and-cast")
def validate_and_cast(
    df: pd.DataFrame,
    required_cols: list[str],
    run_date: str,
) -> pd.DataFrame:
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Schema violation — missing columns: {missing}")

    df = df[required_cols].copy()
    df["_run_date"] = run_date
    df["_loaded_at"] = pd.Timestamp.utcnow()
    return df


@task(name="deduplicate")
def deduplicate(df: pd.DataFrame, pk_columns: list[str]) -> pd.DataFrame:
    before = len(df)
    df = df.sort_values("_loaded_at").drop_duplicates(subset=pk_columns, keep="last")
    after = len(df)
    get_run_logger().info(f"Deduplication: {before} → {after} rows ({before - after} removed)")
    return df


@task(name="upsert-to-postgres", retries=2, retry_delay_seconds=30)
def upsert_to_postgres(
    df: pd.DataFrame,
    table: str,
    schema: str,
    pk_columns: list[str],
) -> int:
    with concurrency("warehouse-writes", occupy=1):
        conn_str = os.environ["WAREHOUSE_CONN"]
        engine = create_engine(conn_str)
        staging = f"_staging_{table}"

        with engine.begin() as conn:
            # Write to staging
            df.to_sql(staging, conn, schema=schema, if_exists="replace", index=False)

            # Upsert from staging
            cols = ", ".join(df.columns)
            updates = ", ".join(
                f"{c} = EXCLUDED.{c}" for c in df.columns if c not in pk_columns
            )
            pk_constraint = ", ".join(pk_columns)
            conn.execute(text(f"""
                INSERT INTO {schema}.{table} ({cols})
                SELECT {cols} FROM {schema}.{staging}
                ON CONFLICT ({pk_constraint})
                DO UPDATE SET {updates}
            """))
            conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{staging}"))

        get_run_logger().info(f"Upserted {len(df)} rows into {schema}.{table}")
        return len(df)


def on_pipeline_failure(flow, flow_run, state):
    error = state.result(raise_on_failure=False)
    get_run_logger().error(f"Pipeline failed: {error}")
    # In production: send to PagerDuty / Slack webhook here


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(
    name="incremental-dwh-load",
    description="Incremental S3 → Postgres upsert pipeline",
    retries=0,
    timeout_seconds=7200,
    on_failure=[on_pipeline_failure],
    log_prints=True,
)
def incremental_dwh_load(
    run_date: str,
    source_bucket: str = "data-lake-raw",
    source_prefix: str = "orders",
    target_schema: str = "silver",
    target_table: str = "orders",
    pk_columns: list[str] | None = None,
    required_columns: list[str] | None = None,
) -> dict[str, Any]:
    logger = get_run_logger()
    pk_columns = pk_columns or ["order_id"]
    required_columns = required_columns or [
        "order_id", "customer_id", "order_date", "amount", "status"
    ]

    prefix = f"{source_prefix}/{run_date}/"
    keys = list_landing_files(bucket=source_bucket, prefix=prefix)

    if not keys:
        logger.warning(f"No files found for {run_date} — skipping")
        return {"files": 0, "rows": 0}

    logger.info(f"Processing {len(keys)} files for {run_date}")

    # Read all files in parallel
    read_futures = [read_parquet.submit(source_bucket, key) for key in keys]
    dfs = [f.result() for f in read_futures]
    combined = pd.concat(dfs, ignore_index=True)

    validated = validate_and_cast(combined, required_columns, run_date)
    deduped = deduplicate(validated, pk_columns)

    rows_loaded = upsert_to_postgres(deduped, target_table, target_schema, pk_columns)

    # Artifacts for observability
    create_markdown_artifact(
        key="load-summary",
        markdown=f"""## Load Summary: `{target_schema}.{target_table}`

| Metric | Value |
|--------|-------|
| Run date | `{run_date}` |
| Files processed | {len(keys)} |
| Raw rows | {len(combined):,} |
| After dedup | {len(deduped):,} |
| Rows upserted | {rows_loaded:,} |
""",
    )
    create_table_artifact(
        key="sample-rows",
        table=deduped.head(10).to_dict(orient="records"),
        description="Sample of loaded rows",
    )

    return {"files": len(keys), "rows": rows_loaded}


if __name__ == "__main__":
    incremental_dwh_load(run_date="2024-03-15")
```

---

## Anti-Patterns

1. **Putting heavy logic in flow body instead of tasks** — code outside `@task` has no retries, no caching, and no state tracking. Wrap every significant operation in a task.

2. **Using `@flow` retries as a substitute for `@task` retries** — flow-level retries re-run the entire flow from scratch, including already-completed tasks (unless results are persisted). Prefer task-level retries for individual operations.

3. **Mutable default parameters** — Python's mutable default argument trap applies to flows:

   ```python
   # Wrong — list is shared across calls
   @flow
   def process(items: list = []):  # noqa
       ...

   # Correct
   @flow
   def process(items: list | None = None):
       items = items or []
   ```

4. **Caching side-effectful tasks** — never apply `cache_policy=INPUTS` to tasks that send emails, write to external systems, or produce non-deterministic output. Use `cache_policy=NO_CACHE`.

5. **Blocking I/O in async flows without `await`** — mixing synchronous blocking calls inside `async def` flows stalls the event loop. Wrap sync calls with `asyncio.to_thread()` or use sync flows.

6. **Not setting `persist_result=True` for cross-deployment data sharing** — results are ephemeral by default. If a downstream deployment needs upstream data, enable result persistence with explicit storage.

7. **Storing secrets in flow parameters** — parameters appear in the UI and run history. Store secrets in Prefect Secret blocks and load them inside the flow:

   ```python
   from prefect.blocks.system import Secret
   conn_str = Secret.load("warehouse-conn").get()
   ```

8. **Running workers as root in Docker/K8s** — use a non-root user in the Dockerfile. Prefect workers do not require root.

9. **Large payloads through XCom-style return values** — returning DataFrames from tasks passes them through Prefect's result infrastructure. For DataFrames > 100 MB, write to S3/GCS inside the task and return only the path.

10. **One deployment per environment instead of work pool variables** — use a single `prefect.yaml` with `job_variables` overridden per work pool rather than duplicating deployments for dev/staging/prod.

11. **Not using `tags` for cost attribution** — always tag deployments and runs with `team`, `project`, and `env` so Prefect Cloud usage can be audited by team.

---

## References to Consult When Needed

- Prefect 3.x concepts: `docs.prefect.io/3.0/concepts`
- Flows & Tasks: `docs.prefect.io/3.0/develop/write-flows`
- Deployments: `docs.prefect.io/3.0/deploy/infrastructure-concepts/prefect-yaml`
- Work Pools: `docs.prefect.io/3.0/deploy/infrastructure-concepts/work-pools`
- Workers: `docs.prefect.io/3.0/deploy/infrastructure-concepts/workers`
- Caching: `docs.prefect.io/3.0/develop/task-caching`
- State hooks: `docs.prefect.io/3.0/develop/state-hooks`
- Concurrency limits: `docs.prefect.io/3.0/develop/global-concurrency-limits`
- Artifacts: `docs.prefect.io/3.0/develop/artifacts`
- Results: `docs.prefect.io/3.0/develop/results`
- Automations (Cloud): `docs.prefect.io/3.0/automate/events/automations-triggers`
- Prefect Docker integration: `prefecthq.github.io/prefect-docker`
- Prefect Kubernetes integration: `prefecthq.github.io/prefect-kubernetes`
