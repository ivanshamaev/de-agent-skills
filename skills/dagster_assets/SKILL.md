---
name: dagster-assets
description: Dagster Software-Defined Assets (SDA) — @asset/@multi_asset decorators, asset dependencies, partitions (daily/static/dynamic/multi), sensors, IO managers, declarative automation (AutomationCondition), jobs, schedules, resource definitions, deployment
---

# Dagster Software-Defined Assets

## When to Use

Activate this skill when the task involves:
- Defining data pipelines as Software-Defined Assets in Dagster
- Setting up partitioned assets (time-based, static, dynamic)
- Writing sensors to trigger asset materializations on events
- Implementing IO managers for custom storage backends
- Configuring declarative automation (AutomationCondition / eager / on_cron)
- Structuring Dagster projects, resources, and definitions
- Deploying Dagster with Docker, Kubernetes, or Dagster Cloud

---

## Core Model

```
Asset (data object in storage)
  ↑
  │  materialized by
  │
Asset Function  ──reads──►  Upstream Assets (via deps / function params)
  │
  └─ uses IO Manager to write output
       uses Resources for external connections
       may be Partitioned (date/region/etc.)
       may be triggered by Sensor or Schedule or AutomationCondition
```

Every asset represents a **persistent data object** (table, file, ML model). The job of the asset function is to compute and save it. Dagster tracks materialization history, staleness, and lineage automatically.

---

## Installation

```bash
pip install dagster dagster-webserver
# Optional integrations
pip install dagster-aws dagster-dbt dagster-spark dagster-k8s
```

---

## Basic Assets

### `@asset` — Single Asset

```python
import dagster as dg
import pandas as pd

@dg.asset
def raw_orders() -> pd.DataFrame:
    """Load raw orders from source system."""
    return pd.read_csv("s3://data-lake/raw/orders.csv")

@dg.asset(deps=[raw_orders])
def stg_orders(raw_orders: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate orders."""
    df = raw_orders.dropna(subset=["order_id", "customer_id"])
    df["order_date"] = pd.to_datetime(df["order_date"])
    df = df[df["total"] > 0]
    return df

@dg.asset
def fct_orders(stg_orders: pd.DataFrame) -> pd.DataFrame:
    """Build orders fact table."""
    return stg_orders.assign(
        revenue_usd=stg_orders["total"] / 100,
        order_month=stg_orders["order_date"].dt.to_period("M").astype(str),
    )
```

### Asset with Context and Metadata

```python
@dg.asset(
    group_name="silver",
    key_prefix=["warehouse", "silver"],
    compute_kind="pandas",
    tags={"team": "data-platform"},
)
def customers(context: dg.AssetExecutionContext, raw_customers: pd.DataFrame) -> pd.DataFrame:
    context.log.info(f"Processing {len(raw_customers)} raw customer rows")

    df = raw_customers.copy()
    df["email"] = df["email"].str.lower().str.strip()
    df = df.drop_duplicates(subset=["customer_id"])

    context.add_output_metadata({
        "row_count": len(df),
        "preview": dg.MetadataValue.md(df.head().to_markdown()),
        "schema": dg.MetadataValue.json(dict(df.dtypes.astype(str))),
    })
    return df
```

### `@multi_asset` — Multiple Outputs from One Operation

```python
@dg.multi_asset(
    outs={
        "users":  dg.AssetOut(key_prefix="silver", description="Cleaned users"),
        "events": dg.AssetOut(key_prefix="silver", description="Cleaned events"),
    },
    can_subset=True,
)
def parse_api_response(context: dg.AssetExecutionContext):
    """Single API call producing two tables."""
    data = fetch_from_api()

    users_df = pd.DataFrame(data["users"])
    events_df = pd.DataFrame(data["events"])

    yield dg.MaterializeResult(
        asset_key="users",
        metadata={"row_count": len(users_df)},
        value=users_df,
    )
    yield dg.MaterializeResult(
        asset_key="events",
        metadata={"row_count": len(events_df)},
        value=events_df,
    )
```

### External Asset (Not Managed by Dagster)

```python
raw_salesforce = dg.AssetSpec(
    key="raw_salesforce_accounts",
    group_name="external",
    description="Salesforce accounts loaded by Fivetran — not materialized by Dagster",
)
```

---

## Partitioned Assets

### Daily Partitions

```python
daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2024-01-01",
    timezone="UTC",
)

@dg.asset(partitions_def=daily_partitions)
def daily_events(context: dg.AssetExecutionContext) -> pd.DataFrame:
    partition_date = context.partition_key           # "2024-03-15"
    start, end = context.partition_time_window       # (datetime, datetime)

    df = query_warehouse(
        f"SELECT * FROM events WHERE event_date = '{partition_date}'"
    )
    context.log.info(f"Loaded {len(df)} events for {partition_date}")
    return df
```

### Other Time Granularities

```python
hourly    = dg.HourlyPartitionsDefinition(start_date="2024-01-01-00:00")
weekly    = dg.WeeklyPartitionsDefinition(start_date="2024-01-01")
monthly   = dg.MonthlyPartitionsDefinition(start_date="2024-01-01")
```

### Static Partitions

```python
region_partitions = dg.StaticPartitionsDefinition(["us", "eu", "apac"])

@dg.asset(partitions_def=region_partitions)
def regional_summary(context: dg.AssetExecutionContext) -> pd.DataFrame:
    region = context.partition_key      # "us" | "eu" | "apac"
    return compute_summary(region=region)
```

### Multi-Dimensional Partitions

```python
multi_partitions = dg.MultiPartitionsDefinition({
    "date":   dg.DailyPartitionsDefinition(start_date="2024-01-01"),
    "region": dg.StaticPartitionsDefinition(["us", "eu", "apac"]),
})

@dg.asset(partitions_def=multi_partitions)
def regional_daily(context: dg.AssetExecutionContext) -> pd.DataFrame:
    keys = context.partition_key.keys_by_dimension   # {"date": "2024-03-15", "region": "us"}
    return compute(date=keys["date"], region=keys["region"])
```

### Dynamic Partitions (Runtime)

```python
customer_partitions = dg.DynamicPartitionsDefinition(name="customers")

@dg.asset(partitions_def=customer_partitions)
def customer_report(context: dg.AssetExecutionContext) -> pd.DataFrame:
    customer_id = context.partition_key
    return build_report(customer_id=customer_id)
```

Add partitions programmatically (usually from a sensor):

```python
context.instance.add_dynamic_partitions("customers", ["cust_001", "cust_002"])
```

---

## Resources

Resources encapsulate external connections. Inject them into asset functions.

```python
from dagster_aws.s3 import S3Resource
import dagster as dg

class WarehouseResource(dg.ConfigurableResource):
    host:     str
    database: str
    user:     str
    password: str

    def query(self, sql: str) -> pd.DataFrame:
        import psycopg2
        conn = psycopg2.connect(
            host=self.host, dbname=self.database,
            user=self.user, password=self.password,
        )
        return pd.read_sql(sql, conn)

@dg.asset
def orders(warehouse: WarehouseResource) -> pd.DataFrame:
    return warehouse.query("SELECT * FROM raw.orders")

# Wire up in Definitions
defs = dg.Definitions(
    assets=[orders],
    resources={
        "warehouse": WarehouseResource(
            host="db.prod.internal",
            database="analytics",
            user=dg.EnvVar("DB_USER"),
            password=dg.EnvVar("DB_PASSWORD"),
        ),
        "s3": S3Resource(region_name="us-east-1"),
    },
)
```

---

## IO Managers

IO managers control how asset outputs are stored and loaded. Default manager: in-memory (no persistence) or filesystem pickle.

### Custom IO Manager — Parquet on S3

```python
from dagster import IOManager, InputContext, OutputContext, io_manager
import boto3
import pandas as pd
import io

class S3ParquetIOManager(IOManager):
    def __init__(self, bucket: str, prefix: str = "data"):
        self.bucket = bucket
        self.prefix = prefix
        self.s3 = boto3.client("s3")

    def _key(self, context) -> str:
        parts = list(context.asset_key.path)
        if context.has_partition_key:
            parts.append(context.partition_key)
        return f"{self.prefix}/{'/'.join(parts)}.parquet"

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        key = self._key(context)
        buf = io.BytesIO()
        obj.to_parquet(buf, index=False)
        buf.seek(0)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buf.read())
        context.add_output_metadata({"s3_key": key, "rows": len(obj)})

    def load_input(self, context: InputContext) -> pd.DataFrame:
        key = self._key(context.upstream_output)
        response = self.s3.get_object(Bucket=self.bucket, Key=key)
        return pd.read_parquet(io.BytesIO(response["Body"].read()))

@io_manager(config_schema={"bucket": str, "prefix": str})
def s3_parquet_io_manager(init_context):
    return S3ParquetIOManager(
        bucket=init_context.resource_config["bucket"],
        prefix=init_context.resource_config.get("prefix", "data"),
    )
```

```python
# Assign IO manager per asset
@dg.asset(io_manager_key="s3_parquet")
def silver_orders() -> pd.DataFrame:
    ...

defs = dg.Definitions(
    assets=[silver_orders],
    resources={
        "s3_parquet": s3_parquet_io_manager.configured(
            {"bucket": "data-lake", "prefix": "silver"}
        ),
    },
)
```

### Built-in IO Managers

| Manager | Package | Format |
|---------|---------|--------|
| `FilesystemIOManager` | `dagster` | Pickle (local, dev) |
| `InMemoryIOManager` | `dagster` | In-memory (testing) |
| `S3PickleIOManager` | `dagster-aws` | Pickle on S3 |
| `BigQueryPandasIOManager` | `dagster-gcp-pandas` | DataFrame ↔ BQ table |
| `SnowflakePandasIOManager` | `dagster-snowflake-pandas` | DataFrame ↔ Snowflake |
| `DuckDBPandasIOManager` | `dagster-duckdb-pandas` | DataFrame ↔ DuckDB |

---

## Sensors

Sensors poll external state and trigger runs.

### Basic Sensor

```python
@dg.sensor(
    target=dg.AssetSelection.assets(daily_events),
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def new_files_sensor(context: dg.SensorEvaluationContext):
    import boto3
    s3 = boto3.client("s3")

    last_seen = context.cursor or ""
    response = s3.list_objects_v2(Bucket="data-lake", Prefix="raw/events/")
    new_files = [
        obj["Key"] for obj in response.get("Contents", [])
        if obj["Key"] > last_seen
    ]

    if not new_files:
        return dg.SkipReason("No new files found")

    run_requests = [
        dg.RunRequest(
            run_key=f"file-{file_key.replace('/', '-')}",   # idempotency key
            run_config={"ops": {"daily_events": {"config": {"file": file_key}}}},
        )
        for file_key in new_files
    ]

    context.update_cursor(sorted(new_files)[-1])
    return run_requests
```

### Asset Sensor — React to Upstream Materialization

```python
@dg.asset_sensor(
    asset_key=dg.AssetKey("raw_orders"),
    target=dg.AssetSelection.assets(stg_orders),
)
def raw_orders_sensor(context: dg.SensorEvaluationContext, asset_event):
    return dg.RunRequest(
        run_key=f"orders-{asset_event.run_id}",
        tags={"triggered_by": "raw_orders_sensor"},
    )
```

### Multi-Asset Sensor

```python
@dg.multi_asset_sensor(
    monitored_assets=[dg.AssetKey("raw_orders"), dg.AssetKey("raw_customers")],
    target=dg.AssetSelection.assets(fct_orders),
)
def both_sources_sensor(context: dg.MultiAssetSensorEvaluationContext):
    latest_orders   = context.latest_materialization_records_by_key().get(dg.AssetKey("raw_orders"))
    latest_customers = context.latest_materialization_records_by_key().get(dg.AssetKey("raw_customers"))

    if latest_orders and latest_customers:
        context.advance_all_cursors()
        return dg.RunRequest(run_key=f"{latest_orders.run_id}-{latest_customers.run_id}")
    return dg.SkipReason("Waiting for both raw_orders and raw_customers")
```

---

## Declarative Automation

Declarative automation replaces imperative sensors/schedules for common patterns.

```python
@dg.asset(
    partitions_def=dg.DailyPartitionsDefinition("2024-01-01"),
    # Materialize every day at 3am UTC after upstream is ready
    automation_condition=dg.AutomationCondition.on_cron("0 3 * * *"),
)
def daily_report(...): ...

@dg.asset(
    # Eagerly re-materialize whenever any upstream changes
    automation_condition=dg.AutomationCondition.eager(),
)
def derived_table(stg_orders: pd.DataFrame): ...

@dg.asset(
    # Fill in any missing partitions when upstream is available
    automation_condition=dg.AutomationCondition.on_missing(),
)
def backfillable_asset(...): ...
```

Enable in `Definitions`:

```python
defs = dg.Definitions(
    assets=all_assets,
    sensors=[dg.AutomationConditionSensorDefinition(
        name="automation_condition_sensor",
        target="*",
    )],
)
```

---

## Jobs and Schedules

```python
# Define a job from a subset of assets
etl_job = dg.define_asset_job(
    name="daily_etl_job",
    selection=dg.AssetSelection.groups("silver", "gold"),
    tags={"team": "data-platform"},
    config=dg.RunConfig(
        ops={"daily_events": dg.OpConfig(config={"timeout_seconds": 3600})}
    ),
)

# Daily schedule
daily_schedule = dg.ScheduleDefinition(
    job=etl_job,
    cron_schedule="0 4 * * *",       # 4am UTC daily
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
```

---

## Project Structure

```
my_dagster_project/
├── my_dagster_project/
│   ├── __init__.py             # exports Definitions
│   ├── assets/
│   │   ├── __init__.py         # collect all assets
│   │   ├── bronze.py
│   │   ├── silver.py
│   │   └── gold.py
│   ├── resources/
│   │   ├── __init__.py
│   │   ├── warehouse.py        # WarehouseResource
│   │   └── io_managers.py      # S3ParquetIOManager
│   ├── sensors/
│   │   └── file_sensors.py
│   ├── jobs.py
│   └── definitions.py          # top-level Definitions object
├── tests/
│   └── test_assets.py
├── pyproject.toml              # [tool.dagster] section
└── dagster.yaml                # deployment config
```

```python
# my_dagster_project/definitions.py
import dagster as dg
from my_dagster_project.assets import bronze, silver, gold
from my_dagster_project.resources import warehouse_resource, s3_parquet_io_manager
from my_dagster_project.sensors import new_files_sensor
from my_dagster_project.jobs import daily_schedule

defs = dg.Definitions(
    assets=dg.load_assets_from_modules([bronze, silver, gold]),
    resources={
        "warehouse": warehouse_resource,
        "io_manager": s3_parquet_io_manager,
    },
    sensors=[new_files_sensor],
    schedules=[daily_schedule],
)
```

---

## Testing

```python
# tests/test_assets.py
from dagster import materialize, build_sensor_context
from my_dagster_project.assets.silver import customers
from my_dagster_project.resources import WarehouseResource

def test_customers_asset():
    result = materialize(
        assets=[customers],
        resources={
            "warehouse": WarehouseResource(
                host="localhost",
                database="test_db",
                user="test",
                password="test",
            )
        },
    )
    assert result.success
    output = result.output_for_node("customers")
    assert len(output) > 0
    assert "customer_id" in output.columns
    assert output["email"].str.islower().all()

def test_sensor_yields_run_requests():
    context = build_sensor_context()
    result = new_files_sensor(context)
    # verify SkipReason on empty bucket
    assert isinstance(result, dg.SkipReason)
```

### Materializing in Unit Tests

```python
from dagster import materialize_to_memory

def test_pipeline():
    result = materialize_to_memory([raw_orders, stg_orders, fct_orders])
    assert result.success
    orders_df = result.output_for_node("fct_orders")
    assert "revenue_usd" in orders_df.columns
```

---

## Deployment — Docker Compose

```yaml
version: "3.8"
services:
  postgresql:
    image: postgres:14
    environment:
      POSTGRES_DB: dagster
      POSTGRES_USER: dagster
      POSTGRES_PASSWORD: dagster
    volumes:
      - dagster-db:/var/lib/postgresql/data

  dagster-webserver:
    image: my-dagster-project:latest
    command: dagster-webserver -h 0.0.0.0 -p 3000 -w workspace.yaml
    ports:
      - "3000:3000"
    environment:
      DAGSTER_POSTGRES_HOST: postgresql
      DAGSTER_POSTGRES_USER: dagster
      DAGSTER_POSTGRES_PASSWORD: dagster
      DAGSTER_POSTGRES_DB: dagster
    depends_on: [postgresql, dagster-daemon]

  dagster-daemon:
    image: my-dagster-project:latest
    command: dagster-daemon run
    environment:
      DAGSTER_POSTGRES_HOST: postgresql
      DAGSTER_POSTGRES_USER: dagster
      DAGSTER_POSTGRES_PASSWORD: dagster
      DAGSTER_POSTGRES_DB: dagster
    depends_on: [postgresql]

volumes:
  dagster-db:
```

```yaml
# dagster.yaml
storage:
  postgres:
    postgres_db:
      username: dagster
      password: dagster
      hostname: postgresql
      db_name: dagster
      port: 5432

run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 10
```

---

## Anti-Patterns

1. **Putting business logic in sensors** — sensors should only detect events and emit `RunRequest`. Move transformation logic into assets.

2. **Missing `run_key` in sensors** — without `run_key`, the same event triggers duplicate runs after restarts. Always set `run_key` to a unique, deterministic identifier.

3. **Giant monolithic assets** — one asset that loads, transforms, and writes is hard to retry and debug. Break into at least 3 layers: ingestion → staging → marts.

4. **Not using resources for external connections** — hardcoding connection strings in asset functions makes testing impossible. Always inject connections via `ConfigurableResource`.

5. **Ignoring IO managers for large DataFrames** — default pickle IO manager stores in memory and local disk. For production data volumes, use S3/GCS Parquet or warehouse IO managers.

6. **Missing partition window checks** — time-partitioned assets that don't filter by `context.partition_time_window` reprocess all historical data on every run.

7. **Creating >100k partitions** — Dagster UI degrades with very large partition sets. For high-cardinality partitions, use dynamic partitions and only add active ones.

8. **Loading entire upstream assets as function parameters** — for large tables, this loads the full DataFrame into memory. Use resources to query with partition predicates instead.

9. **Not setting `default_status=RUNNING` on sensors** — sensors start as STOPPED by default and won't run until manually enabled. Set `DefaultSensorStatus.RUNNING` for sensors that should always be active.

10. **Using `@op` and `@job` instead of `@asset`** — new projects should default to assets. The `@op`/`@job` API is lower-level and loses the lineage, metadata, and declarative automation benefits.

---

## References to Consult When Needed

- Software-Defined Assets: `docs.dagster.io/concepts/assets/software-defined-assets`
- Partitioning: `docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets`
- IO Managers: `docs.dagster.io/guides/build/io-managers`
- Sensors: `docs.dagster.io/guides/automate/sensors`
- Declarative Automation: `docs.dagster.io/guides/automate/declarative-automation`
- Resources: `docs.dagster.io/guides/build/resources`
