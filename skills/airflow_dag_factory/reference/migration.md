# dag-factory Migration Notes (pre-1.0 → 1.0)

When working with an existing project on dag-factory <1.0, apply these changes before using guidance from SKILL.md.

---

## Breaking Changes

| Change | Old (pre-1.0) | New (v1.0+) |
|---|---|---|
| Class access | `from dagfactory import DagFactory` | `from dagfactory import load_yaml_dags` |
| Loader kwarg (dict) | `default_args_config_dict=` | `defaults_config_dict=` |
| Loader kwarg (path) | `default_args_config_path=` | `defaults_config_path=` |
| Schedule key | `schedule_interval: "0 3 * * *"` | `schedule: "0 3 * * *"` |
| DAGrun timeout | `dagrun_timeout_sec: 300` | `dagrun_timeout: {__type__: datetime.timedelta, seconds: 300}` |
| Retry delay | `retry_delay_sec: 60` | `retry_delay: {__type__: datetime.timedelta, seconds: 60}` |
| Execution timeout | `execution_timeout_secs: 3600` | `execution_timeout: {__type__: datetime.timedelta, hours: 1}` |
| SLA | `sla_secs: 7200` | `sla: {__type__: datetime.timedelta, hours: 2}` |
| Logical dataset keys | `!and`, `!or`, `!join`, `and`, `or` | `__and__`, `__or__`, `__join__` |
| Dataset schedule nesting | (no `datasets:` wrapper) | `schedule: {datasets: {__or__: [...]}}` |
| Timetable | `timetable: {callable: ..., params: {...}}` | `timetable: {__type__: ..., __args__: [...]}` |
| Provider dependencies | Auto-installed | Install `apache-airflow-providers-*` manually |
| `clean_dags()` | Available on factory class | Removed — use `AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL` |
| Tasks / task_groups format | Dict keyed by id | List with `task_id` / `group_name` (dict still works) |
| KPO nested object casting | Automatic for k8s types | Removed — use `__type__: kubernetes.client.models.V1...` |

---

## Migration Workflow

Follow in order:

### 1. Update the loader file

```python
# OLD
from dagfactory import DagFactory
factory = DagFactory(config_filepath="dags/example.yml")
factory.generate_dags(globals())

# NEW
from dagfactory import load_yaml_dags
load_yaml_dags(globals_dict=globals(), dags_folder="/usr/local/airflow/dags/")
```

### 2. Rename loader kwargs

```python
# OLD
load_yaml_dags(..., default_args_config_dict=my_dict)
load_yaml_dags(..., default_args_config_path="/path/to/defaults")

# NEW
load_yaml_dags(..., defaults_config_dict=my_dict)
load_yaml_dags(..., defaults_config_path="/path/to/defaults")
```

### 3. Install providers explicitly

Add to `requirements.txt` every provider your YAML references:

```
apache-airflow-providers-http
apache-airflow-providers-slack
apache-airflow-providers-cncf-kubernetes
apache-airflow-providers-snowflake
```

### 4. Rename `schedule_interval` → `schedule`

```yaml
# OLD
schedule_interval: "0 3 * * *"

# NEW
schedule: "0 3 * * *"
```

### 5. Replace `*_sec` / `*_secs` timeout shortcuts

```yaml
# OLD
dagrun_timeout_sec: 3600
retry_delay_sec: 300
execution_timeout_secs: 7200

# NEW
dagrun_timeout:
  __type__: datetime.timedelta
  hours: 1
retry_delay:
  __type__: datetime.timedelta
  minutes: 5
execution_timeout:
  __type__: datetime.timedelta
  hours: 2
```

### 6. Update logical dataset keys

```yaml
# OLD
schedule:
  !and:
    - s3://bucket/dataset1
    - s3://bucket/dataset2

# NEW
schedule:
  datasets:
    __and__:
      - s3://bucket/dataset1
      - s3://bucket/dataset2
```

### 7. Update timetable definitions

```yaml
# OLD
timetable:
  callable: my_module.MyTimetable
  params:
    timezone: UTC

# NEW
schedule:
  __type__: my_module.MyTimetable
  timezone: UTC
```

### 8. Convert `tasks` / `task_groups` dicts to lists (recommended)

```yaml
# OLD (dict format — still works but not recommended)
tasks:
  task_1:
    operator: airflow.operators.bash.BashOperator
    bash_command: "echo 1"
  task_2:
    operator: airflow.operators.bash.BashOperator
    bash_command: "echo 2"
    dependencies: [task_1]

# NEW (list format — preferred)
tasks:
  - task_id: task_1
    operator: airflow.operators.bash.BashOperator
    bash_command: "echo 1"
  - task_id: task_2
    operator: airflow.operators.bash.BashOperator
    bash_command: "echo 2"
    dependencies: [task_1]
```

### 9. Remove `clean_dags()` calls

```python
# OLD
factory.clean_dags(globals())

# NEW — control stale DAG removal via Airflow config:
# AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=300
# Or in airflow.cfg:
# [dag_processor]
# refresh_interval = 300
```

### 10. Convert nested k8s objects

```yaml
# OLD — legacy auto-casting (removed)
container_resources:
  limits: {cpu: "1", memory: "1Gi"}
  requests: {cpu: "500m", memory: "512Mi"}

# NEW — explicit __type__
container_resources:
  __type__: kubernetes.client.models.V1ResourceRequirements
  limits:
    cpu: "1"
    memory: "1Gi"
  requests:
    cpu: "500m"
    memory: "512Mi"
```

---

## Validation After Migration

```bash
# 1. YAML syntax
dagfactory lint dags/

# 2. Airflow 2 → 3 import paths (if also upgrading Airflow)
dagfactory convert dags/ --override

# 3. Have Airflow parse the DAGs
astro dev parse
# or: airflow dags list-import-errors
```

---

## Reference

- Official migration guide: https://astronomer.github.io/dag-factory/latest/migration_guide/
- dag-factory v1.0 release notes: https://github.com/astronomer/dag-factory/releases/tag/v1.0.0
