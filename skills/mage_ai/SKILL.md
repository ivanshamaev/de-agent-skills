---
name: mage-ai-pipelines
description: Mage AI data pipelines — block types (data loader/transformer/exporter/sensor/custom), pipeline YAML, hybrid SQL+Python blocks, triggers (schedule/event/API), streaming pipelines, dbt integration, Spark integration, Docker/Kubernetes deployment, io_config.yaml, pipeline variables, callbacks, backfills
---

# Mage AI Pipelines

## When to Use

Activate this skill when the task involves:
- Authoring data pipelines in Mage AI using block-based composition
- Configuring `io_config.yaml` connection profiles for PostgreSQL, S3, BigQuery
- Mixing SQL transformer blocks with Python loaders/exporters in the same pipeline
- Setting up schedule, event, or API triggers and backfills
- Building streaming pipelines (Kafka → transform → sink)
- Integrating dbt projects as blocks inside a Mage pipeline
- Running Spark workloads from Mage with `executor_type: spark`
- Deploying Mage with Docker Compose or Kubernetes
- Writing block-level callbacks (Slack, audit table) and unit-testing blocks in CI

**Mage vs Airflow vs Prefect — when to choose Mage:**

| Factor | Mage | Airflow | Prefect |
|--------|------|---------|---------|
| Authoring UX | Browser notebook IDE with live block preview | Python files + external IDE | Python files + Prefect Cloud UI |
| SQL-first pipelines | Native hybrid SQL+Python blocks | SQLOperator, no live feedback | SQL tasks via connectors |
| Team onboarding | Low — analysts can write SQL blocks | Medium — DAG + operator concepts | Medium — flow/task concepts |
| Streaming | Built-in streaming pipeline type | Needs Flink/Spark Streaming | Third-party integrations |
| dbt integration | First-class dbt block type | BashOperator or community provider | `prefect-dbt` |
| Iteration speed | Hot-reload, block-level re-run | Full DAG restart to test | Similar to Airflow |
| Production maturity | Growing (v0.9+) | Very mature | Mature |

Choose Mage when the team values quick iteration in a notebook-style UI, needs hybrid SQL+Python without boilerplate, or is building moderately complex batch/streaming pipelines without requiring Airflow's full ecosystem.

---

## Core Concepts

```
                      ┌──────────────────────────────────────────┐
                      │              Mage Pipeline               │
  Trigger ──────────► │  [Sensor] ──► [Data Loader] ──► [Transformer] ──► [Data Exporter]
  (schedule/event/API)│                    │                 │
                      │              [SQL Block] ◄───────────┘
                      │              [Custom Block] (branching)
                      │  Backfill sweeps execution_date range    │
                      └──────────────────────────────────────────┘
                                         │
                             io_config.yaml (connection profiles)
                             pipelines/<name>/metadata.yaml (DAG wiring)
                             pipelines/<name>/triggers.yaml (schedules/events)
```

**Block** — single executable unit: Python file (loader, transformer, exporter, sensor, custom) or SQL file. Each block has one decorated function. Blocks receive upstream outputs as positional arguments.

**Pipeline** — directed acyclic graph of blocks defined in `pipelines/<name>/metadata.yaml`. The YAML wires `upstream_blocks` references.

**Trigger** — rule that starts a pipeline run: cron schedule, S3/API event, or direct REST call.

**Run** — one execution for a specific `execution_date`; Jinja vars like `{{ ds }}` resolve at run time.

**Backfill** — a range of runs sweeping `start_time`..`end_time` at a given `interval_unit`.

**io_config.yaml** — project-wide connection profiles; blocks select a profile by name via `ConfigFileLoader`.

---

## Project Structure

```
mage_project/
├── io_config.yaml              # connection profiles (default / dev / prod)
├── metadata.yaml               # project-level metadata and default variables
├── pipelines/
│   ├── orders_daily_etl/
│   │   ├── metadata.yaml       # block list + upstream_blocks wiring
│   │   └── triggers.yaml       # schedule / event triggers
│   └── kafka_orders_stream/
│       ├── metadata.yaml       # pipeline_type: streaming
│       └── triggers.yaml
├── data_loaders/
├── transformers/
├── data_exporters/
├── sensors/
├── custom/
├── callbacks/
└── dbt/                        # optional dbt project (dbt_project.yml + models/)
```

---

## io_config.yaml

```yaml
# mage_project/io_config.yaml
version: 0.1.1

default:
  # PostgreSQL
  POSTGRES_CONNECT_TIMEOUT: 10
  POSTGRES_DBNAME:   "{{ env_var('POSTGRES_DB', 'analytics') }}"
  POSTGRES_HOST:     "{{ env_var('POSTGRES_HOST') }}"
  POSTGRES_PASSWORD: "{{ env_var('POSTGRES_PASSWORD') }}"
  POSTGRES_PORT:     5432
  POSTGRES_USER:     "{{ env_var('POSTGRES_USER') }}"
  POSTGRES_SCHEMA:   public

  # AWS / S3
  AWS_ACCESS_KEY_ID:     "{{ env_var('AWS_ACCESS_KEY_ID') }}"
  AWS_SECRET_ACCESS_KEY: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
  AWS_REGION:            us-east-1

  # Google BigQuery (inline service account)
  GOOGLE_SERVICE_ACC_KEY:
    type:         service_account
    project_id:   "{{ env_var('GCP_PROJECT_ID') }}"
    private_key:  "{{ env_var('GCP_PRIVATE_KEY') }}"
    client_email: "{{ env_var('GCP_CLIENT_EMAIL') }}"
  GOOGLE_SERVICE_ACC_KEY_FILEPATH: ""   # use inline key above

dev:
  POSTGRES_DBNAME:   analytics_dev
  POSTGRES_HOST:     localhost
  POSTGRES_PASSWORD: dev_password
  POSTGRES_PORT:     5432
  POSTGRES_USER:     dev_user
  AWS_ACCESS_KEY_ID:     ""            # use ~/.aws credentials
  AWS_SECRET_ACCESS_KEY: ""
  AWS_REGION:            us-east-1

prod:
  POSTGRES_DBNAME:   "{{ env_var('PROD_POSTGRES_DB') }}"
  POSTGRES_HOST:     "{{ env_var('PROD_POSTGRES_HOST') }}"
  POSTGRES_PASSWORD: "{{ env_var('PROD_POSTGRES_PASSWORD') }}"
  POSTGRES_PORT:     5432
  POSTGRES_USER:     "{{ env_var('PROD_POSTGRES_USER') }}"
  AWS_ACCESS_KEY_ID:     "{{ env_var('AWS_ACCESS_KEY_ID') }}"
  AWS_SECRET_ACCESS_KEY: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
  AWS_REGION:            us-east-1
  GOOGLE_SERVICE_ACC_KEY:
    type:         service_account
    project_id:   "{{ env_var('GCP_PROJECT_ID') }}"
    private_key:  "{{ env_var('GCP_PRIVATE_KEY') }}"
    client_email: "{{ env_var('GCP_CLIENT_EMAIL') }}"
```

Select a profile per block via the `configuration.config_profile` key in `metadata.yaml`, or explicitly in Python:

```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
loader = ConfigFileLoader(get_repo_path(), config_profile='prod')
```

---

## Block Types

### Data Loader — PostgreSQL

```python
# data_loaders/load_orders_pg.py
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_orders(*args, **kwargs) -> pd.DataFrame:
    ds              = kwargs.get('ds', '2024-01-01')
    config_profile  = kwargs.get('config_profile', 'default')
    config_path     = path.join(get_repo_path(), 'io_config.yaml')

    query = f"""
        SELECT order_id, customer_id, order_date,
               status, total_amount_cents, currency
        FROM raw.orders
        WHERE order_date = '{ds}'
          AND deleted_at IS NULL
    """
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as pg:
        return pg.load(query)


@test
def test_output(df: pd.DataFrame, *args) -> None:
    assert df is not None, "Loader returned None"
    assert 'order_id' in df.columns
```

### Data Loader — S3 Parquet

```python
# data_loaders/load_events_s3.py
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_events(*args, **kwargs):
    ds_nodash = kwargs.get('ds', '2024-01-01').replace('-', '')
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with S3.with_config(ConfigFileLoader(config_path, 'default')) as s3:
        return s3.load('data-lake-prod', f'events/dt={ds_nodash}/events.parquet')

@test
def test_output(df, *args) -> None:
    assert df is not None and len(df) > 0
```

### Transformer — Python

```python
# transformers/clean_orders.py
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def clean_orders(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """Multiple upstream block outputs arrive as positional *args."""
    df = df.copy()
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['total_usd']  = df['total_amount_cents'] / 100.0
    df.drop(columns=['total_amount_cents', 'currency'], inplace=True)
    df = df[df['total_usd'] > 0].dropna(subset=['order_id', 'customer_id'])
    df = df.drop_duplicates(subset=['order_id'])
    df['status'] = df['status'].str.upper().str.strip()
    valid = {'NEW', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED'}
    return df[df['status'].isin(valid)]


@test
def test_output(df: pd.DataFrame, *args) -> None:
    assert df['total_usd'].gt(0).all()
    assert df['order_id'].is_unique
```

### Transformer — SQL Block

SQL transformer blocks reference upstream Python block outputs as Jinja variables and execute directly on the configured data warehouse.

```sql
-- transformers/enrich_orders.sql
-- configuration: data_provider=postgresql, data_provider_profile=prod

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    o.total_usd,
    c.email          AS customer_email,
    c.country_code,
    r.rate           AS usd_to_eur_rate,
    o.total_usd * r.rate AS total_eur
FROM {{ df_1 }} AS o                        -- output of upstream block 1 (clean_orders)
LEFT JOIN {{ df_2 }} AS r                   -- output of upstream block 2 (load_exchange_rates)
       ON r.currency = 'EUR'
      AND r.date     = o.order_date::date
LEFT JOIN public.dim_customers c
       ON c.customer_id = o.customer_id
WHERE o.order_date >= '{{ variables.get("start_date", "2024-01-01") }}'
```

### Data Exporter — PostgreSQL

```python
# data_exporters/export_orders_pg.py
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_orders(df: pd.DataFrame, *args, **kwargs) -> None:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, kwargs.get('config_profile', 'default'))) as pg:
        pg.export(
            df,
            kwargs.get('schema_name', 'analytics'),
            kwargs.get('table_name', 'fct_orders'),
            index=False,
            if_exists='append',    # 'replace' | 'append' | 'fail'
        )
```

### Data Exporter — S3 Parquet

```python
# data_exporters/export_orders_s3.py
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from os import path
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_to_s3(df: pd.DataFrame, *args, **kwargs) -> None:
    ds_nodash  = kwargs.get('ds', '2024-01-01').replace('-', '')
    bucket     = 'data-lake-prod'
    object_key = f'analytics/fct_orders/dt={ds_nodash}/fct_orders.parquet'
    config_path = path.join(get_repo_path(), 'io_config.yaml')

    with S3.with_config(ConfigFileLoader(config_path, 'default')) as s3:
        s3.export(df, bucket, object_key)
```

### Sensor — File Existence Check

```python
# sensors/wait_for_s3_file.py
import boto3

if 'sensor' not in globals():
    from mage_ai.data_preparation.decorators import sensor


@sensor
def check_s3_file(*args, **kwargs) -> bool:
    """Return True to proceed, False to keep polling."""
    ds_nodash = kwargs.get('ds', '2024-01-01').replace('-', '')
    bucket    = 'data-lake-prod'
    key       = f'events/dt={ds_nodash}/_SUCCESS'

    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False
```

### Custom Block — Branching Logic

```python
# custom/branch_on_volume.py
import pandas as pd

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def branch_on_volume(df: pd.DataFrame, *args, **kwargs) -> dict:
    row_count   = len(df)
    threshold   = int(kwargs.get('row_count_threshold', 10_000))
    branch_name = 'heavy_load' if row_count >= threshold else 'light_load'
    print(f"[branch_on_volume] rows={row_count}, branch={branch_name}")
    return {'branch': branch_name, 'row_count': row_count, 'data': df}


@test
def test_output(output: dict, *args) -> None:
    assert output['branch'] in ('heavy_load', 'light_load')
```

---

## Hybrid SQL+Python Blocks

### Jinja variable reference patterns

```sql
-- Upstream block outputs (positional)
SELECT * FROM {{ df_1 }}              -- first upstream block's DataFrame
SELECT * FROM {{ df_2 }}              -- second upstream block's DataFrame

-- Pipeline variables
WHERE order_date = '{{ variables.get("execution_date") }}'

-- Raw block output (when upstream returns dict, not DataFrame)
SELECT * FROM {{ block_output(parse=False) }}

-- Built-in date shortcuts
WHERE dt = '{{ ds }}'                 -- "2024-03-15"
WHERE dt_key = '{{ ds_nodash }}'      -- "20240315"
```

### Full hybrid: Python loader → SQL enrichment → Python exporter

```sql
-- transformers/daily_order_summary.sql
-- Upstream: clean_orders (Python transformer), load_exchange_rates (Python loader)
SELECT
    order_date::date                                        AS report_date,
    status,
    COUNT(*)                                                AS order_count,
    SUM(total_usd)                                          AS revenue_usd,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_usd) AS median_order_value
FROM {{ df_1 }} AS o
WHERE order_date >= '{{ variables.get("start_date", "2024-01-01") }}'
  AND order_date <  '{{ variables.get("end_date",   "2024-01-02") }}'
GROUP BY 1, 2
ORDER BY 1, 2
```

Target connection profile in `metadata.yaml`:
```yaml
blocks:
  - uuid: daily_order_summary
    type: transformer
    language: sql
    configuration:
      data_provider:         postgresql
      data_provider_profile: prod      # maps to io_config.yaml → prod profile
      use_raw_sql:           false
      limit:                 10000     # UI preview limit
```

---

## Pipeline YAML (metadata.yaml)

```yaml
# pipelines/orders_daily_etl/metadata.yaml
name: orders_daily_etl
description: >
  Load, clean, enrich, and export daily order data from PostgreSQL and S3.
  Supports backfill via execution_date variable.

pipeline_type: python        # python | streaming | integration | dbt
executor_type: local_python  # local_python | ecs | gcp_cloud_run | spark | k8s

# Spark config (used when executor_type: spark)
spark_config:
  app_name: orders_daily_etl
  spark_master: "k8s://https://k8s.prod.internal:443"
  executor_instances: 4
  executor_cores: 2
  executor_memory: 4g
  driver_memory:   2g

# Pipeline-level default variables (overridable per trigger or API call)
variables:
  start_date:           "2024-01-01"
  end_date:             "2024-01-02"
  schema_name:          analytics
  row_count_threshold:  10000
  config_profile:       default

blocks:
  - uuid: wait_for_s3_file
    type: sensor
    language: python
    upstream_blocks: []
    configuration:
      timeout:          3600   # seconds before sensor times out
      polling_interval: 60

  - uuid: load_orders_pg
    type: data_loader
    language: python
    upstream_blocks: []
    configuration:
      config_profile: prod

  - uuid: load_events_s3
    type: data_loader
    language: python
    upstream_blocks:
      - wait_for_s3_file

  - uuid: clean_orders
    type: transformer
    language: python
    upstream_blocks:
      - load_orders_pg

  - uuid: enrich_orders
    type: transformer
    language: sql
    upstream_blocks:
      - clean_orders
      - load_events_s3
    configuration:
      data_provider:         postgresql
      data_provider_profile: prod

  - uuid: branch_on_volume
    type: custom
    language: python
    upstream_blocks:
      - enrich_orders
    configuration:
      condition: "{{ df_1 | length > 0 }}"    # skip block when result is empty

  - uuid: export_orders_pg
    type: data_exporter
    language: python
    upstream_blocks:
      - branch_on_volume
    callbacks:
      - uuid: notify_slack_on_failure
        type: callback
      - uuid: audit_run_success
        type: callback

  - uuid: export_orders_s3
    type: data_exporter
    language: python
    upstream_blocks:
      - enrich_orders
```

---

## Triggers

### Schedule trigger (cron) — triggers.yaml

```yaml
# pipelines/orders_daily_etl/triggers.yaml
triggers:
  - name: daily_06_utc
    pipeline_uuid: orders_daily_etl
    schedule_type: time
    schedule_interval: "0 6 * * *"     # also accepts @daily, @hourly, @weekly
    start_time: "2024-01-01T06:00:00"
    status: active
    settings:
      skip_if_previous_running: true
      allow_blocks_to_fail: false
    variables:
      config_profile: prod
```

### Event trigger — S3 file arrival

```yaml
triggers:
  - name: on_s3_upload
    pipeline_uuid: orders_daily_etl
    schedule_type: event
    event_matchers:
      - event_type: aws_event
        pattern:
          source: ["aws.s3"]
          detail-type: ["Object Created"]
          detail:
            bucket:
              name: ["data-lake-prod"]
            object:
              key:
                - prefix: "raw/orders/"
    status: active
```

### API trigger (REST)

```bash
curl -X POST http://localhost:6789/api/pipeline_runs \
  -H 'Content-Type: application/json' \
  -d '{
    "pipeline_run": {
      "pipeline_uuid": "orders_daily_etl",
      "variables": {
        "ds":           "2024-03-15",
        "start_date":   "2024-03-15",
        "end_date":     "2024-03-16",
        "config_profile": "prod"
      }
    }
  }'
```

### Backfill (API)

```bash
curl -X POST http://localhost:6789/api/pipelines/orders_daily_etl/backfills \
  -H 'Content-Type: application/json' \
  -d '{
    "backfill": {
      "name":           "backfill_march_2024",
      "start_time":     "2024-03-01T06:00:00",
      "end_time":       "2024-03-31T06:00:00",
      "interval_type":  "day",
      "interval_units": 1,
      "variables": {"config_profile": "prod"}
    }
  }'
```

---

## Pipeline Variables and Templating

### Built-in Jinja variables (injected per run)

| Variable | Example | Description |
|----------|---------|-------------|
| `{{ execution_date }}` | `2024-03-15 06:00:00` | Run's scheduled datetime |
| `{{ ds }}` | `2024-03-15` | Date string YYYY-MM-DD |
| `{{ ds_nodash }}` | `20240315` | Date string without dashes |
| `{{ yesterday_ds }}` | `2024-03-14` | Previous calendar day |
| `{{ next_ds }}` | `2024-03-16` | Next calendar day |
| `{{ ts }}` | `2024-03-15T06:00:00` | ISO timestamp |

### Accessing variables in Python blocks

```python
@data_loader
def load(*args, **kwargs):
    ds          = kwargs.get('ds')                         # "2024-03-15"
    ds_nodash   = kwargs.get('ds_nodash')                  # "20240315"
    start_date  = kwargs.get('start_date', '2024-01-01')   # custom variable
    batch_size  = int(kwargs.get('batch_size', 5000))      # cast manually

    query = f"""
        SELECT * FROM raw.orders
        WHERE order_date = '{ds}'
        LIMIT {batch_size}
    """
```

### Block-level variable override in metadata.yaml

```yaml
blocks:
  - uuid: load_orders_pg
    type: data_loader
    configuration:
      variables:
        batch_size:     50000   # overrides pipeline default for this block only
        config_profile: prod
```

---

## dbt Integration

### Folder layout

```
mage_project/dbt/
├── dbt_project.yml
├── profiles.yml            # uses {{ env_var() }} for credentials
└── models/
    ├── staging/
    │   └── stg_orders.sql
    └── marts/
        └── fct_orders.sql
```

### dbt blocks in metadata.yaml

```yaml
blocks:
  - uuid: dbt_run_fct_orders
    type: dbt
    language: yaml
    upstream_blocks:
      - export_orders_pg           # ensures raw data is loaded before dbt runs
    configuration:
      dbt_project_name:   analytics   # matches name in dbt_project.yml
      dbt_profile_target: prod
      command:            run
      flags: "--select fct_orders --vars '{execution_date: {{ ds }}}'"

  - uuid: dbt_test_fct_orders
    type: dbt
    language: yaml
    upstream_blocks:
      - dbt_run_fct_orders
    configuration:
      dbt_project_name:   analytics
      dbt_profile_target: prod
      command:            test
      flags:              "--select fct_orders"
```

### Staging upstream DataFrame as a dbt source

```python
# data_exporters/stage_for_dbt.py — write Python block output to a staging table
@data_exporter
def stage_for_dbt(df: pd.DataFrame, *args, **kwargs) -> None:
    with Postgres.with_config(ConfigFileLoader(config_path, 'prod')) as pg:
        pg.export(df, 'staging', 'raw_orders_staged', index=False, if_exists='replace')
```

```sql
-- dbt/models/staging/stg_orders.sql
-- depends_on: {{ source('staging', 'raw_orders_staged') }}
SELECT order_id, customer_id, order_date, status, total_usd
FROM {{ source('staging', 'raw_orders_staged') }}
```

---

## Spark Integration

### executor_type: spark in metadata.yaml

```yaml
name: orders_spark_etl
executor_type: spark

spark_config:
  app_name:           orders_spark_etl
  spark_master:       "k8s://https://k8s.prod.internal:443"
  executor_instances: 8
  executor_cores:     4
  executor_memory:    8g
  driver_memory:      4g
  spark_jars:
    - "s3a://jars/delta-core_2.12-2.4.0.jar"
    - "s3a://jars/hadoop-aws-3.3.4.jar"
  spark_conf:
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    spark.hadoop.fs.s3a.aws.credentials.provider: "com.amazonaws.auth.InstanceProfileCredentialsProvider"
```

### SparkSession access inside a block

```python
# transformers/spark_clean_orders.py
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def spark_transform(df, *args, **kwargs):
    spark = kwargs['spark']     # injected by Mage when executor_type=spark

    # Convert Pandas to Spark if upstream returned a Pandas DF
    spark_df = df if hasattr(df, 'rdd') else spark.createDataFrame(df)

    return (
        spark_df
        .filter(F.col('total_amount_cents') > 0)
        .withColumn('total_usd', (F.col('total_amount_cents') / 100).cast(DoubleType()))
        .dropna(subset=['order_id', 'customer_id'])
        .dropDuplicates(['order_id'])
    )
```

### Reading and writing Delta tables

```python
# data_loaders/load_delta_orders.py — spark = kwargs['spark']
@data_loader
def load_delta(*args, **kwargs):
    spark = kwargs['spark']
    return spark.read.format('delta').load('s3a://data-lake-prod/bronze/orders/') \
                .filter(F.col('order_date') == kwargs.get('ds', '2024-01-01'))

# data_exporters/export_delta_orders.py
@data_exporter
def export_delta(df, *args, **kwargs):
    df.write.format('delta').mode('overwrite') \
      .option('replaceWhere', f"order_date = '{kwargs.get('ds', '2024-01-01')}'") \
      .partitionBy('order_date').save('s3a://data-lake-prod/silver/orders/')
```

---

## Streaming Pipelines

### metadata.yaml for a streaming pipeline

```yaml
name: kafka_orders_stream
pipeline_type: streaming      # disables batch scheduling; runs continuously

blocks:
  - uuid: kafka_orders_source
    type: data_loader
    upstream_blocks: []
  - uuid: transform_order_event
    type: transformer
    upstream_blocks: [kafka_orders_source]
  - uuid: sink_orders_pg
    type: data_exporter
    upstream_blocks: [transform_order_event]
```

### Kafka source block

```python
# data_loaders/kafka_orders_source.py
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_from_kafka(*args, **kwargs):
    """Return a config dict — Mage manages the consumer loop."""
    return {
        'connector_type':      'kafka',
        'bootstrap_server':    'kafka:9092',
        'topic':               'orders.raw',
        'consumer_group':      'mage-orders-consumer',
        'auto_offset_reset':   'latest',
        'batch_size':          100,
        'timeout_ms':          1000,
        'security_protocol':   'SASL_SSL',
        'sasl_mechanism':      'PLAIN',
        'sasl_username':       '{{ env_var("KAFKA_API_KEY") }}',
        'sasl_password':       '{{ env_var("KAFKA_API_SECRET") }}',
        'schema_registry_url': 'https://schema-registry.prod.internal',
    }
```

### Streaming transformer — stateless record processing

```python
# transformers/transform_order_event.py
import json

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform_events(messages: list, *args, **kwargs) -> list:
    """messages is a list of raw Kafka message dicts per micro-batch."""
    output = []
    for msg in messages:
        try:
            p = msg if isinstance(msg, dict) else json.loads(msg)
            output.append({
                'order_id':    p['order_id'],
                'customer_id': p['customer_id'],
                'status':      p.get('status', 'UNKNOWN').upper(),
                'total_usd':   float(p.get('total_amount_cents', 0)) / 100.0,
                'event_ts':    p['event_timestamp'],
            })
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            print(f"[WARN] Skipping malformed message: {exc}")
    return output
```

### Sink block — PostgreSQL

```python
# data_exporters/sink_orders_pg.py
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def sink_to_pg(events: list, *args, **kwargs) -> None:
    if not events:
        return
    df = pd.DataFrame(events)
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'prod')) as pg:
        pg.export(df, 'streaming', 'orders_realtime', index=False, if_exists='append')
```

### Windowed aggregation with in-block state

```python
# transformers/windowed_order_agg.py
from datetime import datetime, timedelta

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

_WINDOW_SEC = 300
_state: dict = {}   # {window_key: {customer_id: total_usd}}
                    # NOTE: single-process only — use Redis for multi-replica deployments


@transformer
def aggregate_window(events: list, *args, **kwargs) -> list:
    now = datetime.utcnow()
    w   = now.replace(second=0, microsecond=0)
    w   = w - timedelta(seconds=w.second % _WINDOW_SEC)
    wk  = str(w)

    bucket = _state.setdefault(wk, {})
    for ev in events:
        cid = ev.get('customer_id', 'unknown')
        bucket[cid] = bucket.get(cid, 0.0) + ev.get('total_usd', 0.0)

    cutoff  = str(now - timedelta(seconds=_WINDOW_SEC * 2))
    results = []
    for k in [k for k in _state if k < cutoff]:
        for cid, total in _state.pop(k).items():
            results.append({'window_start': k, 'customer_id': cid, 'total_usd': total})
    return results
```

---

## Callbacks

Define callback blocks in `callbacks/` and attach them to blocks in `metadata.yaml`.

### On-failure Slack notification

```python
# callbacks/notify_slack_on_failure.py
import os, json, urllib.request

if 'callback' not in globals():
    from mage_ai.data_preparation.decorators import callback


@callback('on_failure')
def notify_slack(parent_block_data: dict, **kwargs) -> None:
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if not webhook_url:
        return

    message = {
        'text': (
            f':red_circle: *Mage block failed*\n'
            f'Pipeline: `{kwargs.get("pipeline_uuid")}`\n'
            f'Block:    `{kwargs.get("block_uuid")}`\n'
            f'Run ID:   `{kwargs.get("pipeline_run_id")}`\n'
            f'Error:    ```{str(parent_block_data.get("error", ""))[:500]}```'
        )
    }
    data = json.dumps(message).encode()
    urllib.request.urlopen(
        urllib.request.Request(webhook_url, data=data,
                               headers={'Content-Type': 'application/json'}),
        timeout=10,
    )
```

### On-success audit table write

```python
# callbacks/audit_run_success.py
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from datetime import datetime, timezone
from os import path
import pandas as pd

if 'callback' not in globals():
    from mage_ai.data_preparation.decorators import callback


@callback('on_success')
def write_audit(parent_block_data: dict, **kwargs) -> None:
    record = pd.DataFrame([{
        'pipeline_uuid':   kwargs.get('pipeline_uuid'),
        'block_uuid':      kwargs.get('block_uuid'),
        'pipeline_run_id': kwargs.get('pipeline_run_id'),
        'row_count':       parent_block_data.get('output_row_count'),
        'completed_at':    datetime.now(timezone.utc),
    }])
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'prod')) as pg:
        pg.export(record, 'audit', 'pipeline_run_log', index=False, if_exists='append')
```

### Attaching callbacks in metadata.yaml

```yaml
blocks:
  - uuid: export_orders_pg
    type: data_exporter
    upstream_blocks: [enrich_orders]
    callbacks:
      - uuid: notify_slack_on_failure
        type: callback
      - uuid: audit_run_success
        type: callback
```

---

## Docker and Kubernetes Deployment

### Docker Compose — local development

```yaml
# docker-compose.yml
version: "3.9"
services:
  mage:
    image: mageai/mageai:0.9.72
    command: mage start mage_project
    ports: ["6789:6789"]
    volumes:
      - ./mage_project:/home/src/mage_project
      - mage-data:/home/src/.mage_data
    environment:
      MAGE_PROJECT_PATH:             /home/src/mage_project
      MAGE_DATABASE_CONNECTION_URL:  postgresql://mage:mage@postgres:5432/magedb
      POSTGRES_HOST:                 postgres
      POSTGRES_DB:                   analytics
      POSTGRES_USER:                 ${POSTGRES_USER}
      POSTGRES_PASSWORD:             ${POSTGRES_PASSWORD}
      AWS_ACCESS_KEY_ID:             ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY:         ${AWS_SECRET_ACCESS_KEY}
      AWS_REGION:                    us-east-1
      SLACK_WEBHOOK_URL:             ${SLACK_WEBHOOK_URL}
    depends_on:
      postgres:
        condition: service_healthy

  postgres:
    image: postgres:15
    environment: {POSTGRES_DB: magedb, POSTGRES_USER: mage, POSTGRES_PASSWORD: mage}
    volumes: [pg-data:/var/lib/postgresql/data]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U mage"]
      interval: 10s
      retries: 5

volumes:
  mage-data:
  pg-data:
```

### Kubernetes Deployment

```yaml
# k8s/mage-configmap.yaml — inject environment-specific io_config via ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: mage-io-config
  namespace: data-platform
data:
  io_config.yaml: |
    version: 0.1.1
    default:
      POSTGRES_HOST:     postgres-svc.data-platform.svc.cluster.local
      POSTGRES_DBNAME:   analytics
      POSTGRES_USER:     "{{ env_var('POSTGRES_USER') }}"
      POSTGRES_PASSWORD: "{{ env_var('POSTGRES_PASSWORD') }}"
      AWS_REGION:        us-east-1
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mage-server
  namespace: data-platform
spec:
  replicas: 1
  selector:
    matchLabels: {app: mage-server}
  template:
    metadata:
      labels: {app: mage-server}
    spec:
      serviceAccountName: mage-sa
      containers:
        - name: mage
          image: mageai/mageai:0.9.72
          command: ["mage", "start", "mage_project"]
          ports: [{containerPort: 6789}]
          resources:
            requests: {cpu: 500m, memory: 1Gi}
            limits:   {cpu: "2",  memory: 4Gi}
          envFrom:
            - secretRef: {name: mage-secrets}     # POSTGRES_USER, POSTGRES_PASSWORD, AWS_*
          env:
            - name:  MAGE_PROJECT_PATH
              value: /home/src/mage_project
            - name:  MAGE_DATABASE_CONNECTION_URL
              valueFrom:
                secretKeyRef: {name: mage-secrets, key: database_url}
          volumeMounts:
            - name: pipeline-storage
              mountPath: /home/src/mage_project
            - name: io-config
              mountPath: /home/src/mage_project/io_config.yaml
              subPath:   io_config.yaml
      volumes:
        - name: pipeline-storage
          persistentVolumeClaim: {claimName: mage-pipeline-pvc}
        - name: io-config
          configMap: {name: mage-io-config}
---
apiVersion: v1
kind: Service
metadata: {name: mage-svc, namespace: data-platform}
spec:
  selector: {app: mage-server}
  ports: [{port: 6789, targetPort: 6789}]
  type: ClusterIP
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: mage-pipeline-pvc, namespace: data-platform}
spec:
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 20Gi}}
```

---

## Testing Blocks

```python
# tests/test_clean_orders.py
import pandas as pd
import pytest
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from transformers.clean_orders import clean_orders


@pytest.fixture
def raw_df():
    return pd.DataFrame({
        'order_id':           ['ORD-001', 'ORD-002', 'ORD-003', 'ORD-001', None],
        'customer_id':        ['CUST-A',  'CUST-B',  'CUST-C',  'CUST-A',  'CUST-D'],
        'order_date':         ['2024-03-15'] * 5,
        'status':             ['new', ' SHIPPED ', 'bad_status', 'new', 'NEW'],
        'total_amount_cents': [10000, 25000, 5000, 10000, 0],
        'currency':           ['USD'] * 5,
    })


def test_deduplication(raw_df):
    assert clean_orders(raw_df)['order_id'].is_unique


def test_zero_totals_removed(raw_df):
    assert (clean_orders(raw_df)['total_usd'] > 0).all()


def test_invalid_status_removed(raw_df):
    assert 'bad_status' not in clean_orders(raw_df)['status'].values


def test_null_order_id_removed(raw_df):
    assert clean_orders(raw_df)['order_id'].notna().all()


def test_total_usd_calculated(raw_df):
    result = clean_orders(raw_df)
    assert result.loc[result['order_id'] == 'ORD-002', 'total_usd'].iloc[0] == 250.0
```

For programmatic block execution in integration tests (skipped in CI by default):

```python
# tests/test_pipeline_integration.py
@pytest.mark.skipif(os.getenv('CI_SKIP_INTEGRATION', 'true') == 'true', reason="integration")
def test_clean_orders_block():
    from mage_ai.data_preparation.models.pipeline import Pipeline
    block  = Pipeline.get('orders_daily_etl').get_block('clean_orders')
    result = block.execute_sync(
        input_args=[pd.DataFrame({
            'order_id': ['ORD-100'], 'customer_id': ['CUST-1'],
            'order_date': ['2024-03-15'], 'status': ['new'],
            'total_amount_cents': [5000], 'currency': ['USD'],
        })],
        global_vars={'ds': '2024-03-15'},
    )
    assert result is not None and len(result) == 1
```

---

## Anti-Patterns

1. **Hardcoding credentials in block code** — always use `io_config.yaml` with `{{ env_var() }}`. Never embed passwords or API keys in `.py` or `.sql` files.

2. **Monolithic blocks that load, transform, and export** — break pipelines into discrete blocks. Monolithic blocks cannot be individually retried, previewed, or unit-tested.

3. **Using `if_exists='replace'` in production exporters** — this silently truncates the target table. Use `'append'` with a pre-step that deletes the partition, or implement upsert logic via SQL.

4. **Streaming transformers with large in-process state** — module-level state is lost on restart and does not scale across replicas. Use Redis, a compacted Kafka topic, or PostgreSQL for stateful streaming.

5. **Missing `skip_if_previous_running: true` on schedule triggers** — without this guard, slow pipelines accumulate overlapping runs, creating data duplicates and resource contention.

6. **Not implementing `@test` functions** — every `@data_loader` and `@transformer` must have a `@test` that checks output schema and non-empty result. Tests execute after each block in the UI.

7. **Placing SQL blocks before sensor blocks** — sensors are gates; they must precede the data loaders they guard. Order: sensor → loader → transformer → exporter.

8. **Not setting `config_profile: prod` in production triggers** — omitting this causes production scheduled runs to fall through to the `default` profile, which may target a dev database.

9. **Running Spark blocks with `executor_type: local_python`** — Spark blocks call `kwargs['spark']`, which raises `KeyError` unless the pipeline sets `executor_type: spark`. Always configure at pipeline level.

10. **Renaming block files without updating metadata.yaml** — `metadata.yaml` references blocks by `uuid` (file stem). Renaming a block file without updating `uuid` entries silently breaks the pipeline.

---

## References to Consult When Needed

- Mage AI docs home: `docs.mage.ai`
- Block types reference: `docs.mage.ai/design/blocks`
- io_config.yaml format: `docs.mage.ai/development/io_config`
- Pipeline YAML spec: `docs.mage.ai/design/core-abstractions`
- Streaming pipelines: `docs.mage.ai/streaming/overview`
- dbt integration: `docs.mage.ai/integrations/dbt`
- Spark executor: `docs.mage.ai/integrations/spark-pyspark`
- Kubernetes deployment: `docs.mage.ai/production/deploying-to-cloud/kubernetes`
- Backfill API: `docs.mage.ai/orchestration/backfills/overview`
- Triggers YAML spec: `docs.mage.ai/orchestration/triggers/trigger-pipeline`
- Callbacks: `docs.mage.ai/design/blocks/callbacks`
