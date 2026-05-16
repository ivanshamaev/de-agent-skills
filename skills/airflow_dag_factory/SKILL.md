---
name: airflow_dag_factory
description: Use when building, reviewing, debugging, or scaling Apache Airflow DAGs declaratively with dag-factory YAML configs — including project setup, loader configuration, defaults hierarchy, custom/provider operators, dynamic task mapping, datasets/assets, callbacks, TaskFlow decorators, Jinja2 templating, environment variables, YAML DRY patterns, large-scale multi-DAG generation, CI/CD linting, and migration from pre-1.0.
---

# Airflow DAG Factory

Build Apache Airflow DAGs declaratively with **dag-factory** — a library that turns YAML configuration files into Airflow DAGs with no boilerplate Python.

> **Package**: `dag-factory` on PyPI  
> **Repo**: https://github.com/astronomer/dag-factory  
> **Docs**: https://astronomer.github.io/dag-factory/latest/  
> **Targets**: dag-factory **v1.0+**, Python **3.10+**, Airflow **2.9+** (Airflow 3 supported)  
> For pre-1.0 projects, read [reference/migration.md](reference/migration.md) before applying any guidance here.

---

## When to Use dag-factory

| Scenario | Recommendation |
|---|---|
| Low-code, repetitive DAG patterns (same operators, varying params) | **dag-factory** — ideal |
| Many similar DAGs that differ only in config values | **dag-factory** — generate many DAGs from one YAML |
| Complex branching, dynamic Python logic, Pydantic validation | Python DAGs or **blueprint** skill instead |
| Full TaskFlow pipeline in Python | **authoring-dags** skill instead |
| Existing dag-factory <1.0 project | Follow [reference/migration.md](reference/migration.md) first |

---

## Quick Decision Table

| User Request | Go to |
|---|---|
| "Create a YAML DAG" / "Convert Python DAG to YAML" | [Defining a DAG in YAML](#defining-a-dag-in-yaml) |
| "Set up dag-factory in my project" | [Project Setup](#project-setup) |
| "Share defaults across DAGs / set start_date once" | [Defaults](#defaults) |
| "Custom operator / KPO / Slack / Snowflake" | [Custom & Provider Operators](#custom--provider-operators) |
| "Dynamic / mapped tasks / expand / partial" | [Dynamic Task Mapping](#dynamic-task-mapping) |
| "Schedule on dataset / outlets / inlets" | [Datasets and Assets](#datasets-and-assets) |
| "Add callback / Slack on failure" | [Callbacks](#callbacks) |
| "Use @task decorator / TaskFlow in YAML" | [TaskFlow API](#taskflow-api) |
| "Use env vars in YAML" | [Environment Variables](#environment-variables) |
| "Jinja2 templates in YAML" | [Jinja2 Templating](#jinja2-templating) |
| "Generate 50 DAGs from one template" | [Large-Scale Patterns](#large-scale-patterns) |
| "DRY config / reuse blocks across DAGs" | [DRY with YAML Anchors](#dry-with-yaml-anchors) |
| "Lint / validate YAML" | [Validation Commands](#validation-commands) |
| "Migrate from Airflow 2 → 3" | [Validation Commands](#validation-commands) (`dagfactory convert`) |
| "Migrate from dag-factory <1.0" | [reference/migration.md](reference/migration.md) |
| Errors / broken DAG | [Troubleshooting](#troubleshooting) |

---

## Project Setup

### 1. Install

Add to `requirements.txt`:

```
dag-factory>=1.0.0
```

dag-factory does **not** install Airflow providers. Install providers your YAML references explicitly:

```
apache-airflow-providers-slack
apache-airflow-providers-cncf-kubernetes
apache-airflow-providers-http
```

### 2. Create the Loader File

Create `dags/load_dags.py`. Airflow discovers DAGs from this file:

```python
import os
from pathlib import Path
from dagfactory import load_yaml_dags

CONFIG_ROOT_DIR = Path(os.getenv("CONFIG_ROOT_DIR", "/usr/local/airflow/dags/"))

# Option A: load every *.yml / *.yaml under a folder (recommended)
load_yaml_dags(globals_dict=globals(), dags_folder=str(CONFIG_ROOT_DIR))

# Option B: load a single file
# load_yaml_dags(globals_dict=globals(), config_filepath=str(CONFIG_ROOT_DIR / "my_dag.yml"))

# Option C: load from a Python dict (useful for testing)
# load_yaml_dags(globals_dict=globals(), config_dict={...})
```

`globals_dict=globals()` is required — generated DAG objects must be registered into the module namespace for Airflow to discover them.

### 3. Recommended Project Structure

```
project/
├── dags/
│   ├── load_dags.py          # single loader — only Python file in dags/
│   ├── defaults.yml           # global defaults (schedule, owner, retries, start_date)
│   ├── ingestion/
│   │   ├── defaults.yml       # team-level overrides
│   │   ├── orders.yml
│   │   └── customers.yml
│   └── transformation/
│       ├── defaults.yml
│       └── mart_revenue.yml
├── include/
│   ├── callbacks/
│   │   └── slack.py           # callback functions
│   └── tasks/
│       └── transforms.py      # Python callables for PythonOperator / @task
└── requirements.txt
```

**Key rules:**
- Keep `dags/` clean — only the loader `.py` and YAML/YML config files.
- Put all Python callables, SQL, and helpers in `include/` — the DAG processor ignores this directory by default.
- Never put `defaults.yml` logic directly in `dags_folder` root if you point `dags_folder` at that directory — dag-factory may try to parse it as a DAG config. Use `defaults_config_path` to separate it, or use a `default:` block inside each YAML file.

### 4. Verify

```bash
dagfactory --version
dagfactory lint dags/
```

---

## Defining a DAG in YAML

Each top-level YAML key (other than `default`) defines one DAG. The key becomes the `dag_id`.

```yaml
# dags/ingestion/orders.yml
default:
  default_args:
    start_date: 2025-01-01
    owner: data-team
    retries: 2
  catchup: false
  schedule: "0 3 * * *"

ingest_orders:
  description: "Load orders from source to staging"
  tags: [ingestion, orders]
  task_groups:
    - group_name: validate
      tooltip: "Data quality checks"
      dependencies: [extract]
  tasks:
    - task_id: extract
      operator: airflow.operators.bash.BashOperator
      bash_command: "echo extracting orders"
    - task_id: load
      operator: airflow.operators.bash.BashOperator
      bash_command: "echo loading orders"
      dependencies: [extract]
    - task_id: check_row_count
      operator: airflow.operators.bash.BashOperator
      bash_command: "echo checking row count"
      dependencies: [load]
      task_group_name: validate
    - task_id: check_nulls
      operator: airflow.operators.bash.BashOperator
      bash_command: "echo checking nulls"
      dependencies: [load]
      task_group_name: validate
```

### Key Fields Reference

| Field | Level | Description |
|---|---|---|
| `default` | file | Shared args applied to every DAG in this file |
| `default_args` | DAG or `default` | Standard Airflow `default_args` (owner, retries, start_date, …) |
| `schedule` | DAG | Cron string, preset (`@daily`), Dataset URI list, or `__type__` timetable |
| `catchup` | DAG | Boolean; default Airflow behavior is `true` — set `false` explicitly |
| `description` | DAG | Human-readable description |
| `tags` | DAG | List of tag strings |
| `max_active_runs` | DAG | Max concurrent DAG runs |
| `dagrun_timeout` | DAG | `__type__: datetime.timedelta` block |
| `tasks` | DAG | **List** of task dicts; each needs `task_id` + `operator` |
| `operator` | task | Full Python import path to operator class |
| `dependencies` | task/group | List of upstream `task_id`s or `group_name`s |
| `task_groups` | DAG | List of group dicts; each needs `group_name` |
| `task_group_name` | task | Assigns task to a task group |

> Tasks do not need to be listed in dependency order — dag-factory resolves topology from `dependencies`.

### List vs Dict Format

Use the **list format** (shown above) for all new code — it is the v1.0 standard and required for task groups.

The legacy dict format (where `tasks` is a dict keyed by `task_id`) still works for backward compatibility but is not recommended.

---

## Defaults

Four layers of defaults, in **precedence order** (highest wins):

| Priority | Source | How |
|---|---|---|
| 1 (highest) | Individual DAG block | `default_args:` or DAG-level keys inside the DAG |
| 2 | `default:` block in the same YAML file | Top-level `default:` key |
| 3 | `defaults_config_dict=` | Passed as Python dict to `load_yaml_dags` |
| 4 (lowest) | `defaults.yml` file | Auto-detected walking up the dir tree, or explicit `defaults_config_path=` |

### `default:` Block (same file)

Best for templates with multiple DAGs in one file:

```yaml
default:
  default_args:
    owner: analytics
    start_date: 2025-01-01
    retries: 1
    retry_delay:
      __type__: datetime.timedelta
      minutes: 5
  catchup: false
  schedule: "@daily"
  tags: [analytics]

revenue_daily:
  description: "Daily revenue mart"
  tasks:
    - task_id: build
      operator: airflow.operators.bash.BashOperator
      bash_command: "dbt run --select mart_revenue"

users_daily:
  description: "Daily user mart"
  tasks:
    - task_id: build
      operator: airflow.operators.bash.BashOperator
      bash_command: "dbt run --select mart_users"
```

### `defaults.yml` File (hierarchical, team-wide)

Place `defaults.yml` files in the directory tree. dag-factory **merges** them walking from the DAG file up to the root, with files closer to the DAG winning:

```yaml
# dags/defaults.yml — global
schedule: "0 6 * * *"
catchup: false
default_args:
  start_date: '2025-01-01'
  owner: platform
  retries: 2
```

```yaml
# dags/ingestion/defaults.yml — team-level overrides
default_args:
  owner: ingestion-team
  retries: 3
tags: [ingestion]
```

A DAG inside `dags/ingestion/` inherits both files, with `ingestion/defaults.yml` values winning.

**Important**: set `defaults_config_path` in the loader to the highest ancestor folder you want merged:

```python
load_yaml_dags(
    globals_dict=globals(),
    dags_folder="/usr/local/airflow/dags/",
    defaults_config_path="/usr/local/airflow/dags/",  # walk from here
)
```

---

## Custom & Provider Operators

Reference any operator by its **full Python import path**. All other task keys become `__init__` kwargs.

```yaml
tasks:
  - task_id: wait_for_api
    operator: airflow.providers.http.sensors.http.HttpSensor
    http_conn_id: api_conn
    endpoint: /health
    request_params: {}
    response_check:
      __type__: builtins.str
      __args__: ["lambda response: response.json()['status'] == 'ok'"]
    poke_interval: 30
    timeout: 600
    mode: reschedule

  - task_id: run_snowflake
    operator: airflow.providers.snowflake.operators.snowflake.SnowflakeOperator
    snowflake_conn_id: snowflake_default
    sql: "CALL my_procedure()"
    warehouse: TRANSFORM_WH
    database: ANALYTICS
    schema: PUBLIC

  - task_id: custom_op
    operator: include.operators.MyCustomOperator
    my_param: "value"
```

For **Airflow 3**, prefer `airflow.providers.standard.operators.*` over the legacy `airflow.operators.*` — use `dagfactory convert` to auto-rewrite:

```bash
dagfactory convert dags/ --override
```

### KubernetesPodOperator

Use `__type__` for nested k8s objects (legacy auto-casting was removed in v1.0):

```yaml
tasks:
  - task_id: run_pod
    operator: airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator
    image: python:3.12-slim
    cmds: ["python", "-c"]
    arguments: ["print('hello from pod')"]
    name: my-pod
    namespace: default
    get_logs: true
    is_delete_operator_pod: true
    container_resources:
      __type__: kubernetes.client.models.V1ResourceRequirements
      limits:
        cpu: "1"
        memory: "1Gi"
      requests:
        cpu: "500m"
        memory: "512Mi"
    env_vars:
      - __type__: kubernetes.client.models.V1EnvVar
        name: MY_ENV
        value: "my_value"
```

---

## Dynamic Task Mapping

Use `expand` and `partial` keys. Two reference syntaxes — **do not mix them**:

| Syntax | Use case |
|---|---|
| `task_id.output` | Inside `expand.op_args` / `expand.op_kwargs` (XCom-style) |
| `+task_id` | As a bare `expand` value; for TaskFlow decorator tasks |

### PythonOperator — XCom-style expand

```yaml
tasks:
  - task_id: get_ids
    operator: airflow.providers.standard.operators.python.PythonOperator
    python_callable_name: fetch_ids
    python_callable_file: /usr/local/airflow/include/tasks/extract.py

  - task_id: process_id
    operator: airflow.providers.standard.operators.python.PythonOperator
    python_callable_name: process_one
    python_callable_file: /usr/local/airflow/include/tasks/extract.py
    partial:
      op_kwargs:
        env: production
    expand:
      op_args: get_ids.output          # XCom-style: inside op_args
    dependencies: [get_ids]
```

### TaskFlow — bare `+task_id` expand

```yaml
tasks:
  - task_id: build_numbers
    decorator: airflow.sdk.definitions.decorators.task
    python_callable: include.tasks.sample.build_numbers_list

  - task_id: double
    decorator: airflow.sdk.definitions.decorators.task
    python_callable: include.tasks.sample.double_number
    expand:
      number: +build_numbers           # bare +: expand over XComArg
    dependencies: [build_numbers]
```

### map_index_template (Airflow 2.9+)

```yaml
  - task_id: process_item
    operator: airflow.providers.standard.operators.python.PythonOperator
    python_callable_name: process
    python_callable_file: /usr/local/airflow/include/tasks/process.py
    expand:
      op_args: get_items.output
    map_index_template: "{{ task.item_id }}"   # callable sets context["item_id"]
```

**Supported patterns**: simple mapping, task-generated mapping, `partial`, multiple-parameter mapping, `map_index_template`.  
**Unsupported**: mapping over task groups, zipping expanding collections, `transform()`.

---

## Datasets and Assets

### Producer / Consumer pattern

```yaml
ingest_orders:
  default_args:
    start_date: '2025-01-01'
  schedule: "0 3 * * *"
  catchup: false
  tasks:
    - task_id: load
      operator: airflow.operators.bash.BashOperator
      bash_command: "echo loading"
      outlets:
        - s3://datalake/orders/latest.parquet

transform_orders:
  default_args:
    start_date: '2025-01-01'
  schedule:
    - s3://datalake/orders/latest.parquet   # triggered when producer updates this
  catchup: false
  tasks:
    - task_id: transform
      operator: airflow.operators.bash.BashOperator
      bash_command: "echo transforming"
```

### Conditional Dataset Scheduling (Airflow 2.9+ / dag-factory 0.22+)

```yaml
schedule:
  datasets:
    __or__:
      - __and__:
          - s3://datalake/orders/latest.parquet
          - s3://datalake/customers/latest.parquet
      - s3://datalake/orders_fallback/latest.parquet
```

> **Airflow 3 gotcha**: task-level `outlets`/`inlets` may be silently ignored for asset-aware scheduling in some Airflow 3.x versions. If asset-triggered DAGs don't fire, verify with a Python DAG first.

---

## Callbacks

Three styles — all valid at DAG, TaskGroup, or Task level (or under `default_args`):

### 1. String callable path (simplest)

```yaml
tasks:
  - task_id: extract
    operator: airflow.operators.bash.BashOperator
    bash_command: "extract.sh"
    on_failure_callback: include.callbacks.slack.notify_failure
    on_success_callback: include.callbacks.metrics.record_success
```

### 2. Callable with kwargs

```yaml
tasks:
  - task_id: load
    operator: airflow.operators.bash.BashOperator
    bash_command: "load.sh"
    on_failure_callback:
      callback: include.callbacks.slack.send_message
      channel: "#data-alerts"
      mention: "@oncall"
```

### 3. File path + function name

```yaml
tasks:
  - task_id: validate
    operator: airflow.operators.bash.BashOperator
    bash_command: "validate.sh"
    on_retry_callback_name: log_retry
    on_retry_callback_file: /usr/local/airflow/include/callbacks/logging.py
```

### 4. Provider callback (e.g. Slack)

```yaml
tasks:
  - task_id: critical_load
    operator: airflow.operators.bash.BashOperator
    bash_command: "load.sh"
    on_failure_callback:
      callback: airflow.providers.slack.notifications.slack.send_slack_notification
      slack_conn_id: slack_default
      text: ":red_circle: *{{ dag.dag_id }}* / *{{ task.task_id }}* failed."
      channel: "#data-oncall"
```

---

## TaskFlow API

Use the `decorator` key with the full import path to the Airflow task decorator. Reference the callable via `python_callable: module.function`.

```yaml
pipeline_dag:
  default_args:
    start_date: 2025-01-01
  schedule: "@daily"
  tasks:
    - task_id: extract
      decorator: airflow.sdk.definitions.decorators.task
      python_callable: include.tasks.pipeline.extract_data

    - task_id: transform
      decorator: airflow.sdk.definitions.decorators.task
      python_callable: include.tasks.pipeline.transform_data
      dependencies: [extract]

    - task_id: load
      decorator: airflow.sdk.definitions.decorators.task
      python_callable: include.tasks.pipeline.load_data
      dependencies: [transform]
```

Corresponding callable file (`include/tasks/pipeline.py`):

```python
def extract_data() -> list[dict]:
    return [{"id": 1}, {"id": 2}]

def transform_data(records: list[dict]) -> list[dict]:
    return [{"id": r["id"], "processed": True} for r in records]

def load_data(records: list[dict]) -> None:
    print(f"Loading {len(records)} records")
```

Mix TaskFlow tasks with traditional operator tasks freely — they resolve via `dependencies` like any other task.

---

## Environment Variables

dag-factory supports `os.environ` expansion in several places. Use env vars for environment-specific config (paths, connection IDs, bucket names):

### In the loader

```python
import os
from dagfactory import load_yaml_dags

load_yaml_dags(
    globals_dict=globals(),
    dags_folder=os.getenv("DAGS_FOLDER", "/usr/local/airflow/dags/"),
    defaults_config_path=os.getenv("DAGS_FOLDER", "/usr/local/airflow/dags/"),
)
```

### In YAML via `$VAR` / `${VAR}` substitution

dag-factory expands environment variables in YAML string values:

```yaml
tasks:
  - task_id: run_script
    operator: airflow.providers.standard.operators.python.PythonOperator
    python_callable_name: run
    python_callable_file: $CONFIG_ROOT_DIR/include/tasks/run.py  # expanded at load time

  - task_id: load_s3
    operator: airflow.operators.bash.BashOperator
    bash_command: "aws s3 cp data.csv s3://$S3_BUCKET/raw/"
```

Set env vars in `Dockerfile`, `.env`, Airflow connection settings, or Astro CLI `airflow_settings.yaml`.

---

## Jinja2 Templating

Standard Airflow Jinja2 macros work in any templated operator field:

```yaml
tasks:
  - task_id: extract_partition
    operator: airflow.operators.bash.BashOperator
    bash_command: >
      python extract.py
        --date {{ ds }}
        --start {{ data_interval_start }}
        --end {{ data_interval_end }}

  - task_id: run_sql
    operator: airflow.providers.snowflake.operators.snowflake.SnowflakeOperator
    snowflake_conn_id: snowflake_default
    sql: |
      SELECT * FROM orders
      WHERE order_date = '{{ ds }}'
        AND run_id = '{{ run_id }}'
```

Useful macros: `{{ ds }}`, `{{ ds_nodash }}`, `{{ ts }}`, `{{ run_id }}`, `{{ dag.dag_id }}`, `{{ task.task_id }}`, `{{ data_interval_start }}`, `{{ data_interval_end }}`, `{{ var.value.my_var }}`, `{{ conn.my_conn_id.host }}`.

---

## Custom Python Objects (`__type__`)

For any non-scalar value (datetime, timedelta, timetable, k8s objects), use `__type__`:

```yaml
# Timeout with timedelta
execution_timeout:
  __type__: datetime.timedelta
  hours: 2
  minutes: 30

# start_date as datetime
start_date:
  __type__: datetime.datetime
  year: 2025
  month: 1
  day: 1

# Custom timetable
schedule:
  __type__: airflow.timetables.trigger.CronTriggerTimetable
  cron: "0 1 * * 1-5"
  timezone: Europe/Moscow

# List of typed objects
env_vars:
  __type__: builtins.list
  items:
    - __type__: kubernetes.client.models.V1EnvVar
      name: ENVIRONMENT
      value: production
    - __type__: kubernetes.client.models.V1EnvVar
      name: LOG_LEVEL
      value: INFO
```

**`__type__`** is the full import path to the class.  
**`__args__`** is a list of positional constructor arguments.  
**Other keys** become keyword arguments.

**Reserved keys** — do not use for your own data: `__type__`, `__args__`, `__join__`, `__and__`, `__or__`, and `items` inside a `__type__: builtins.list` block.

---

## DRY with YAML Anchors

YAML native anchors and merge keys (`<<`) reduce repetition across tasks in one file without any dag-factory-specific features:

```yaml
# Define reusable blocks with & — place at top of file
x-python-defaults: &python-defaults
  operator: airflow.providers.standard.operators.python.PythonOperator
  python_callable_file: /usr/local/airflow/include/tasks/etl.py

x-retry-policy: &retry-policy
  retries: 3
  retry_delay:
    __type__: datetime.timedelta
    minutes: 10
  execution_timeout:
    __type__: datetime.timedelta
    hours: 1

default:
  default_args:
    <<: *retry-policy
    owner: data-team
    start_date: 2025-01-01
  catchup: false

ingest_orders:
  schedule: "0 2 * * *"
  tasks:
    - task_id: extract
      <<: *python-defaults
      python_callable_name: extract_orders
    - task_id: validate
      <<: *python-defaults
      python_callable_name: validate_orders
      dependencies: [extract]

ingest_customers:
  schedule: "0 3 * * *"
  tasks:
    - task_id: extract
      <<: *python-defaults
      python_callable_name: extract_customers
    - task_id: validate
      <<: *python-defaults
      python_callable_name: validate_customers
      dependencies: [extract]
```

**Rules:**
- Define anchors at the top of the file.
- Name anchors descriptively (`&retry-policy`, not `&a1`).
- Use `x-` prefix for anchor-only blocks (the `x-` prefix is ignored by YAML parsers as an extension field — it will not become a DAG).
- Avoid deep nesting; merge only what actually varies.

---

## Large-Scale Patterns

### Many DAGs from a single file

One YAML file can define dozens of DAGs. Use `default:` for shared config, anchors for shared task structure:

```yaml
default:
  default_args:
    start_date: 2025-01-01
    owner: ingestion
    retries: 2
  catchup: false
  schedule: "0 4 * * *"

x-ingest-tasks: &ingest-tasks
  tasks:
    - task_id: extract
      operator: airflow.operators.bash.BashOperator
      bash_command: "python include/ingest.py --source {{ dag.dag_id }}"
    - task_id: load
      operator: airflow.operators.bash.BashOperator
      bash_command: "python include/load.py --source {{ dag.dag_id }}"
      dependencies: [extract]

ingest_orders:
  <<: *ingest-tasks
  tags: [orders]

ingest_customers:
  <<: *ingest-tasks
  tags: [customers]

ingest_products:
  <<: *ingest-tasks
  tags: [products]
  schedule: "0 5 * * *"   # override default schedule for this DAG
```

### Programmatic YAML generation

For truly large fleets (50+ DAGs), generate YAML files from a template rather than hand-writing them. Run the generator in CI or as a pre-deploy step:

```python
# scripts/generate_dags.py
import yaml
from pathlib import Path

SOURCES = [
    {"name": "orders",    "schedule": "0 2 * * *", "table": "raw.orders"},
    {"name": "customers", "schedule": "0 3 * * *", "table": "raw.customers"},
    # ... more sources
]

template = {
    "default": {
        "default_args": {"start_date": "2025-01-01", "retries": 2},
        "catchup": False,
    }
}

for source in SOURCES:
    dag_config = {
        f"ingest_{source['name']}": {
            "schedule": source["schedule"],
            "tasks": [
                {
                    "task_id": "extract",
                    "operator": "airflow.operators.bash.BashOperator",
                    "bash_command": f"python include/ingest.py --table {source['table']}",
                }
            ],
        }
    }
    template.update(dag_config)

output = Path("dags/generated/ingestion.yml")
output.write_text(yaml.dump(template, sort_keys=False))
```

### Symlink-based partitioning (for very large fleets)

When a single YAML generates hundreds of DAGs and Airflow DAG processor overhead becomes visible, split into symlinked loader files:

```python
# dags/load_orders.py  (symlink to dags/load_factory.py)
# dags/load_customers.py  (symlink to dags/load_factory.py)

# dags/load_factory.py
import os
from dagfactory import load_yaml_dags

# Each symlink has a different __file__ name
source = os.path.basename(__file__).replace("load_", "").replace(".py", "")
load_yaml_dags(
    globals_dict=globals(),
    config_filepath=f"/usr/local/airflow/dags/generated/{source}.yml",
)
```

Each symlink processes only its own YAML, keeping the Airflow scheduler's DAG parse scope smaller.

---

## Validation Commands

```bash
# Verify install
dagfactory --version

# Lint YAML syntax for a file or directory
dagfactory lint dags/

# Verbose: per-file results table
dagfactory lint dags/ --verbose

# Show diffs to migrate Airflow 2 → 3 import paths
dagfactory convert dags/

# Apply conversions in place
dagfactory convert dags/ --override
```

`dagfactory lint` validates YAML syntax only. Operator import errors and missing kwargs surface at Airflow parse time.

### Full validation workflow

```bash
# 1. YAML syntax
dagfactory lint dags/

# 2. Airflow 2 → 3 migration (if needed)
dagfactory convert dags/ --override

# 3. Airflow parse (Astro CLI)
astro dev parse

# 4. Alternative: bare Airflow
airflow dags list-import-errors
```

---

## CI/CD Integration

### GitHub Actions snippet

```yaml
# .github/workflows/validate_dags.yml
name: Validate DAGs
on: [push, pull_request]

jobs:
  lint-and-parse:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: pip install dag-factory>=1.0.0 apache-airflow>=2.9

      - name: Lint YAML
        run: dagfactory lint dags/ --verbose

      - name: Parse DAGs
        run: airflow dags list-import-errors --output json | python -c "
import sys, json
errors = json.load(sys.stdin)
if errors: sys.exit(1)
"
```

### Pre-commit hook

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: dagfactory-lint
        name: dag-factory lint
        entry: dagfactory lint
        language: python
        files: \.ya?ml$
        pass_filenames: true
```

---

## Common Pitfalls

| Pitfall | Root cause | Fix |
|---|---|---|
| DAG doesn't appear in UI | `globals_dict=globals()` not passed to `load_yaml_dags` | Add `globals_dict=globals()` |
| `defaults.yml` shows as broken DAG | File in `dags_folder` without a valid DAG key | Use `defaults_config_path=` to point at its parent; name it outside `dags_folder` |
| `ModuleNotFoundError` on operator | Provider not installed | Add `apache-airflow-providers-<name>` to `requirements.txt` |
| `ModuleNotFoundError` after Airflow 3 upgrade | Legacy `airflow.operators.*` paths removed | Run `dagfactory convert dags/ --override` |
| Wrong type on scalar (e.g. start_date) | String passed where datetime expected | Use `__type__: datetime.datetime` block |
| XCom reference not working in `expand` | Mixed `+task_id` and `task_id.output` syntax | Use `task_id.output` inside `op_args`/`op_kwargs`; use `+task_id` for bare expand |
| Asset outlets silently ignored on Airflow 3.x | Known issue #718 | Verify with a Python DAG; track issue on GitHub |
| `dagfactory` tag added to all DAGs | v1.0 auto-adds it | Filter by tag in UI, or override with your own `tags:` list |
| Conditional dataset schedule ignored | Airflow <2.9 or dag-factory <0.22 | Upgrade; rename `!and`/`!or` → `__and__`/`__or__` |
| Multiple `defaults.yml` not merging | `defaults_config_path` not pointing at root ancestor | Set it to the highest dir you want included |
| KPO nested objects error | Legacy auto-casting removed in v1.0 | Use `__type__: kubernetes.client.models.V1...` |

---

## Troubleshooting

### DAG missing from Airflow UI

1. Check `dags/load_dags.py` exists and calls `load_yaml_dags(globals_dict=globals(), ...)`.
2. Run `dagfactory lint dags/` — fix any YAML syntax errors.
3. Run `astro dev parse` or `airflow dags list-import-errors`.
4. Check the Airflow webserver and scheduler logs for parse errors.

### Import errors

```
ModuleNotFoundError: No module named 'airflow.operators.email'
```

For Airflow 3: run `dagfactory convert dags/ --override`. Then reinstall: `pip install apache-airflow-providers-standard`.

### YAML parses but tasks don't run / wrong topology

- Verify `dependencies:` lists correct `task_id` values (case-sensitive).
- For task groups, `task_group_name:` on a task must match `group_name:` on the group definition.
- Check `dagfactory lint` — it catches structural YAML errors.

### XCom mapping not working

- Inside `expand.op_args` → use `task_id.output` syntax.
- As a bare `expand` value → use `+task_id` syntax.
- Never mix the two in the same task.

### Timedelta / datetime type errors

Replace:

```yaml
# BAD — string will fail
execution_timeout: "3600"
retry_delay: "00:05:00"
```

With:

```yaml
# GOOD — __type__ syntax
execution_timeout:
  __type__: datetime.timedelta
  hours: 1
retry_delay:
  __type__: datetime.timedelta
  minutes: 5
```

---

## Verification Checklist

Before calling a DAG complete:

- [ ] `dagfactory lint dags/` passes with no errors
- [ ] `astro dev parse` (or `airflow dags list-import-errors`) shows no import errors
- [ ] DAG appears in Airflow UI with correct `dag_id`, schedule, and tags
- [ ] All task dependencies render correctly in Grid/Graph view
- [ ] Required provider packages listed in `requirements.txt`
- [ ] Python callables are in `include/`, not in `dags/`
- [ ] `defaults.yml` not in `dags_folder` root (or `defaults_config_path` set correctly)

---

## Related Skills

- **authoring-dags** — Pure Python Airflow DAGs with `af` CLI. Use when YAML cannot express the logic.
- **testing-dags** — Testing DAGs, debugging failures, test → fix → retest loop.
- **debugging-dags** — Troubleshooting failed DAG runs.

---

## References

- GitHub: https://github.com/astronomer/dag-factory
- Docs: https://astronomer.github.io/dag-factory/latest/
- PyPI: https://pypi.org/project/dag-factory/
- Migration guide: https://astronomer.github.io/dag-factory/latest/migration_guide/
- Migration notes (pre-1.0 → 1.0): [reference/migration.md](reference/migration.md)
