---
name: de-production-readiness
description: Data engineering production readiness — idempotency patterns (INSERT ON CONFLICT, MERGE, partition overwrite), retry strategies (exponential backoff, Airflow retry config), SLA monitoring (Airflow sla_miss_callback, Prometheus PromQL), data freshness checks (Soda freshness, max event_time), structured logging (JSON logs, OpenTelemetry tracing), data reconciliation (row count, checksum, duplicate detection), graceful degradation (circuit breaker, fallback, skip failed partitions), change management (blue/green table swap, schema migration rollback)
---

# DE Production Readiness

## When to Use

Load this skill when the user needs to:
- Make a data pipeline idempotent and safe to retry or re-run
- Implement exponential backoff, retry logic, or error classification in Python or Airflow
- Set up SLA monitoring, alerting (Slack, PagerDuty), or Prometheus metrics for a pipeline
- Define or enforce data freshness SLAs and automate freshness checks
- Add structured logging, OpenTelemetry tracing, or observability metrics to a pipeline
- Build or audit data reconciliation: row counts, checksums, duplicate detection
- Implement graceful degradation — circuit breakers, stale-data fallback, skipping bad partitions
- Perform a blue/green table swap, schema migration, or define a rollback procedure

---

## 1. Idempotency Patterns

An idempotent pipeline produces the same final state whether it runs once or ten times for the same logical batch. This is the single most important property of a reliable pipeline because it makes retries, backfills, and incident recovery safe.

### Idempotent INSERT — ON CONFLICT DO NOTHING (PostgreSQL / Trino)

```sql
-- PostgreSQL: dedup on natural key
INSERT INTO orders (order_id, customer_id, amount, created_at)
SELECT order_id, customer_id, amount, created_at
FROM orders_staging
ON CONFLICT (order_id) DO NOTHING;

-- Trino / Iceberg: use MERGE as the equivalent
MERGE INTO orders AS t
USING orders_staging AS s
ON t.order_id = s.order_id
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, amount, created_at)
  VALUES (s.order_id, s.customer_id, s.amount, s.created_at);
```

### Idempotent MERGE — Upsert Pattern

```sql
-- Works for Spark SQL, Trino+Iceberg, Delta Lake, ClickHouse ReplacingMergeTree
MERGE INTO dim_customers AS t
USING (
    SELECT * FROM dim_customers_staging
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND t.updated_at < s.updated_at THEN
  UPDATE SET
    t.name        = s.name,
    t.email       = s.email,
    t.updated_at  = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, updated_at)
  VALUES (s.customer_id, s.name, s.email, s.updated_at);
```

### Partition Overwrite Instead of Append

Overwriting an entire partition on each run is idempotent; appending blindly creates duplicates on retry.

```python
# PySpark — dynamic partition overwrite (safe re-run)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .save("s3://datalake/events/")
```

```sql
-- Spark SQL equivalent
INSERT OVERWRITE TABLE events PARTITION (event_date = '2024-06-01')
SELECT * FROM events_staging WHERE event_date = '2024-06-01';
```

### Deterministic Job IDs

Generate job run IDs deterministically from business keys so re-runs produce the same ID and idempotency guards in downstream systems work correctly.

```python
import hashlib

def make_run_id(dag_id: str, logical_date: str) -> str:
    """Stable run ID — same inputs always produce the same ID."""
    key = f"{dag_id}::{logical_date}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]

run_id = make_run_id("orders_etl", "2024-06-01")
# Insert guard: skip if run_id already recorded in audit table
```

### Checkpointing for Streaming Jobs

```python
# PySpark Structured Streaming — checkpoint enables exactly-once restart
query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://checkpoints/orders_stream/")
    .trigger(processingTime="1 minute")
    .start("s3://datalake/orders_stream/")
)
```

---

## 2. Retry Strategies

### Exponential Backoff with Jitter (Python)

Jitter prevents thundering-herd when many workers retry simultaneously after a shared outage.

```python
import time
import random
import logging
from functools import wraps

logger = logging.getLogger(__name__)


class TransientError(Exception):
    """Retry this — network blip, lock timeout, rate limit."""

class PermanentError(Exception):
    """Do NOT retry — bad data, auth failure, schema mismatch."""


def retry_with_backoff(
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    retryable: type = TransientError,
):
    """Decorator: exponential backoff with full jitter."""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except PermanentError:
                    raise  # never retry permanent errors
                except retryable as exc:
                    if attempt == max_attempts:
                        logger.error(
                            "max_retries_exceeded",
                            extra={"function": fn.__name__, "attempt": attempt, "error": str(exc)},
                        )
                        raise
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    if jitter:
                        delay = random.uniform(0, delay)  # full jitter
                    logger.warning(
                        "retrying",
                        extra={"function": fn.__name__, "attempt": attempt, "delay_s": round(delay, 2)},
                    )
                    time.sleep(delay)
        return wrapper
    return decorator


# Usage
@retry_with_backoff(max_attempts=5, base_delay=2.0, retryable=TransientError)
def fetch_api_data(url: str) -> dict:
    import requests
    resp = requests.get(url, timeout=10)
    if resp.status_code == 429:
        raise TransientError(f"Rate limited: {resp.status_code}")
    if resp.status_code == 401:
        raise PermanentError(f"Auth failure: {resp.status_code}")
    resp.raise_for_status()
    return resp.json()
```

### Classify Transient vs Permanent Errors

| Error class | Examples | Action |
|---|---|---|
| Transient | Network timeout, DB lock timeout, API rate limit (429), resource busy | Retry with backoff |
| Permanent | Auth failure (401/403), schema mismatch, bad data (400), missing table | Fail immediately, alert |
| Indeterminate | Unknown DB error, unexpected exception | Retry once, then alert |

### Airflow Retry Configuration

```python
from datetime import timedelta
import pendulum
from airflow.sdk import dag, task

@dag(
    dag_id="orders_etl",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,   # 5m, 10m, 20m
        "max_retry_delay": timedelta(minutes=60),
        "execution_timeout": timedelta(hours=2),
        "sla": timedelta(hours=4),           # triggers sla_miss_callback
    },
)
def orders_etl():
    ...
```

### Spark Stage Retry

```python
# spark-defaults.conf or SparkSession config
spark = (
    SparkSession.builder
    .config("spark.task.maxFailures", "4")          # default 4 — retries per task
    .config("spark.stage.maxConsecutiveAttempts", "4")
    .config("spark.network.timeout", "300s")
    .config("spark.sql.broadcastTimeout", "300")
    .getOrCreate()
)
```

---

## 3. Alerting and SLA Monitoring

### Airflow SLA Miss Callback

The `sla` parameter is measured from DAG start time. Callbacks fire after the scheduler's next heartbeat past the deadline.

```python
import pendulum
from airflow.sdk import dag, task
from airflow.utils.email import send_email


def handle_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called by Airflow when any task in dag misses its SLA."""
    missed = ", ".join(t.task_id for t in task_list)
    message = (
        f"SLA missed in DAG `{dag.dag_id}`\n"
        f"Tasks: {missed}\n"
        f"Run at: {pendulum.now('UTC').isoformat()}"
    )
    # Slack
    _post_slack(message)
    # Email
    send_email(
        to=["data-oncall@company.com"],
        subject=f"[AIRFLOW SLA] {dag.dag_id}",
        html_content=f"<pre>{message}</pre>",
    )


def _post_slack(text: str):
    import requests, os
    requests.post(os.environ["SLACK_WEBHOOK_URL"], json={"text": text}, timeout=5)


@dag(
    dag_id="orders_etl",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    sla_miss_callback=handle_sla_miss,
    default_args={"sla": pendulum.duration(hours=4)},
)
def orders_etl():
    ...
```

### Prometheus Alerts — Pipeline Latency (PromQL)

Expose a `pipeline_last_success_timestamp_seconds` gauge from each job (or via Pushgateway), then alert on staleness.

```yaml
# prometheus/alerts/pipelines.yml
groups:
  - name: data_pipelines
    rules:
      # Alert when pipeline has not succeeded for more than 4 hours
      - alert: PipelineSLAMiss
        expr: |
          (time() - pipeline_last_success_timestamp_seconds{job="orders_etl"}) > 14400
        for: 5m
        labels:
          severity: critical
          team: data-eng
        annotations:
          summary: "Pipeline {{ $labels.job }} has not completed in 4h"
          runbook: "https://wiki.company.com/runbooks/orders_etl"

      # Alert on high error rate
      - alert: PipelineHighErrorRate
        expr: |
          rate(pipeline_records_failed_total[5m])
          / (rate(pipeline_records_processed_total[5m]) + 1) > 0.05
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Error rate > 5% in {{ $labels.job }}"

      # Alert on consumer lag (Kafka)
      - alert: KafkaConsumerLagHigh
        expr: kafka_consumer_lag_sum{consumer_group="orders-cg"} > 100000
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Consumer lag {{ $value }} for {{ $labels.consumer_group }}"
```

```python
# Push metrics from Python job to Prometheus Pushgateway
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import time

def push_pipeline_metrics(job_name: str, records: int, errors: int):
    registry = CollectorRegistry()

    Gauge("pipeline_last_success_timestamp_seconds",
          "Epoch of last successful run", registry=registry).set(time.time())
    Gauge("pipeline_records_processed_total",
          "Records processed", registry=registry).set(records)
    Gauge("pipeline_records_failed_total",
          "Records failed", registry=registry).set(errors)

    push_to_gateway("pushgateway:9091", job=job_name, registry=registry)
```

### PagerDuty Webhook Integration

```python
import requests, os

def page_oncall(summary: str, severity: str = "critical", dag_id: str = ""):
    """Fire PagerDuty Events API v2 alert."""
    payload = {
        "routing_key": os.environ["PD_ROUTING_KEY"],
        "event_action": "trigger",
        "payload": {
            "summary": summary,
            "severity": severity,  # critical | error | warning | info
            "source": f"airflow:{dag_id}",
            "custom_details": {"dag_id": dag_id},
        },
    }
    resp = requests.post(
        "https://events.pagerduty.com/v2/enqueue",
        json=payload,
        timeout=5,
    )
    resp.raise_for_status()
```

### Dead-Letter Queue Alerting

```python
# Kafka DLQ consumer — alert if DLQ depth crosses threshold
from confluent_kafka import Consumer
from prometheus_client import Gauge

dlq_depth_gauge = Gauge("dlq_depth", "Messages in dead-letter queue", ["topic"])

def monitor_dlq(dlq_topic: str, bootstrap_servers: str):
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "dlq-monitor",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([dlq_topic])
    depth = 0
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            break
        if not msg.error():
            depth += 1
    dlq_depth_gauge.labels(topic=dlq_topic).set(depth)
    if depth > 100:
        page_oncall(f"DLQ {dlq_topic} has {depth} unprocessed messages", severity="error")
    consumer.close()
```

---

## 4. Data Freshness SLAs

### Query-Based Freshness Check (SQL)

```sql
-- Check that the orders table has been loaded within the last 2 hours
SELECT
    MAX(event_time)                                        AS latest_event,
    NOW() - MAX(event_time)                                AS lag,
    CASE
        WHEN NOW() - MAX(event_time) > INTERVAL '2 hours'
        THEN 'STALE'
        ELSE 'FRESH'
    END                                                    AS freshness_status
FROM orders;
```

### Soda Core Freshness Check

```yaml
# checks/orders_freshness.yml
checks for orders:
  - freshness(event_time) < 2h:
      name: orders_loaded_within_2h
      fail:
        when freshness > 2h
      warn:
        when freshness > 1h
  - freshness(event_time) < 24h:
      name: orders_not_older_than_1_day
```

```python
# Run freshness scan from Airflow
from airflow.sdk import task
from soda.scan import Scan

@task
def check_orders_freshness():
    scan = Scan()
    scan.set_data_source_name("trino_prod")
    scan.add_configuration_yaml_file("/opt/soda/configuration.yml")
    scan.add_sodacl_yaml_file("/opt/soda/checks/orders_freshness.yml")
    scan.set_scan_definition_name("orders_freshness")
    exit_code = scan.execute()
    if exit_code not in (0, 1):   # 0=pass, 1=warn; 2+=fail
        raise ValueError(f"Freshness check failed with exit code {exit_code}")
    return scan.get_scan_results()
```

### Automated Freshness Monitoring Dashboard

```python
# Expose freshness lag as a Prometheus metric for Grafana dashboard
import time
from prometheus_client import Gauge
import trino

freshness_lag_gauge = Gauge(
    "table_freshness_lag_seconds",
    "Seconds since last row was written",
    ["schema", "table"],
)

def update_freshness_metrics(tables: list[dict]):
    conn = trino.dbapi.connect(host="trino", port=8080, user="monitor")
    cursor = conn.cursor()
    for t in tables:
        cursor.execute(
            f"SELECT EXTRACT(EPOCH FROM (NOW() - MAX({t['ts_col']})))"
            f" FROM {t['schema']}.{t['table']}"
        )
        lag_s = cursor.fetchone()[0] or 0
        freshness_lag_gauge.labels(
            schema=t["schema"], table=t["table"]
        ).set(lag_s)
```

---

## 5. Observability

### Structured Logging (JSON)

Every log line must carry correlation fields so logs from distributed workers can be joined.

```python
import logging
import json
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Merge extra fields passed by the caller
        for key in ("job_id", "run_id", "table", "partition", "row_count", "error"):
            if hasattr(record, key):
                log[key] = getattr(record, key)
        if record.exc_info:
            log["exception"] = self.formatException(record.exc_info)
        return json.dumps(log, ensure_ascii=False)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


# Usage
logger = get_logger("orders_etl")

logger.info(
    "partition_loaded",
    extra={
        "job_id": "orders_etl",
        "run_id": "2024-06-01",
        "table": "orders",
        "partition": "2024-06-01",
        "row_count": 142_503,
    },
)
```

Sample output:
```json
{
  "ts": "2024-06-01T06:05:12.003Z",
  "level": "INFO",
  "logger": "orders_etl",
  "message": "partition_loaded",
  "job_id": "orders_etl",
  "run_id": "2024-06-01",
  "table": "orders",
  "partition": "2024-06-01",
  "row_count": 142503
}
```

### OpenTelemetry for Airflow and Spark

```python
# requirements: opentelemetry-sdk opentelemetry-exporter-otlp
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Bootstrap once per process (call from DAG or job entrypoint)
def setup_tracing(service_name: str, otlp_endpoint: str = "http://otel-collector:4317"):
    provider = TracerProvider()
    exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

tracer = trace.get_tracer("orders_etl")

# Wrap Airflow tasks as spans
def load_partition(run_id: str, partition: str):
    with tracer.start_as_current_span("load_partition") as span:
        span.set_attribute("run_id", run_id)
        span.set_attribute("partition", partition)
        span.set_attribute("table", "orders")
        try:
            rows = _do_load(partition)
            span.set_attribute("row_count", rows)
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(trace.StatusCode.ERROR, str(exc))
            raise
```

```python
# Airflow 3.x — enable built-in OTEL export via airflow.cfg
# [metrics]
# otel_on = True
# otel_host = otel-collector
# otel_port = 4318
# otel_prefix = airflow
```

### Key Metrics to Expose

| Metric | Type | Description |
|---|---|---|
| `records_processed_total` | Counter | Total rows successfully written |
| `records_failed_total` | Counter | Rows sent to DLQ or discarded |
| `error_rate` | Gauge | `failed / (processed + failed)` per window |
| `lag_seconds` | Gauge | Event time lag (now - max event_time) |
| `partition_load_duration_seconds` | Histogram | Wall-clock time per partition |
| `pipeline_last_success_timestamp_seconds` | Gauge | Epoch of last successful run |
| `dlq_depth` | Gauge | Unprocessed messages in dead-letter queue |
| `schema_drift_detected` | Counter | Schema changes caught at ingest |

---

## 6. Data Reconciliation

### Row Count Check — Upstream vs Downstream

```python
import logging

logger = logging.getLogger("reconciliation")

def reconcile_row_counts(
    upstream_conn,
    downstream_conn,
    upstream_query: str,
    downstream_query: str,
    tolerance_pct: float = 0.01,  # 1%
) -> dict:
    with upstream_conn.cursor() as cur:
        cur.execute(upstream_query)
        upstream_count = cur.fetchone()[0]

    with downstream_conn.cursor() as cur:
        cur.execute(downstream_query)
        downstream_count = cur.fetchone()[0]

    diff = abs(upstream_count - downstream_count)
    diff_pct = diff / max(upstream_count, 1)
    passed = diff_pct <= tolerance_pct

    result = {
        "upstream_count": upstream_count,
        "downstream_count": downstream_count,
        "diff": diff,
        "diff_pct": round(diff_pct, 6),
        "passed": passed,
    }
    level = logging.INFO if passed else logging.ERROR
    logger.log(level, "row_count_reconciliation", extra=result)
    if not passed:
        raise AssertionError(
            f"Row count mismatch: upstream={upstream_count}, "
            f"downstream={downstream_count}, diff_pct={diff_pct:.2%}"
        )
    return result
```

### Checksum Comparison

```sql
-- Upstream (source DB)
SELECT
    COUNT(*)                                        AS row_count,
    SUM(amount)                                     AS amount_sum,
    COUNT(DISTINCT order_id)                        AS distinct_orders,
    MD5(STRING_AGG(order_id::text ORDER BY order_id)) AS order_id_hash
FROM orders
WHERE order_date = '2024-06-01';

-- Downstream (data warehouse) — run same query, compare results
SELECT
    COUNT(*)                                        AS row_count,
    SUM(amount)                                     AS amount_sum,
    COUNT(DISTINCT order_id)                        AS distinct_orders,
    MD5(STRING_AGG(order_id::text ORDER BY order_id)) AS order_id_hash
FROM dw.fact_orders
WHERE order_date = '2024-06-01';
```

### Duplicate Detection

```sql
-- Detect duplicates on natural key
SELECT order_id, COUNT(*) AS cnt
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;
```

```python
# PySpark duplicate detection
from pyspark.sql import functions as F

def detect_duplicates(df, key_cols: list[str], label: str = "") -> int:
    dup_count = (
        df.groupBy(key_cols)
          .agg(F.count("*").alias("cnt"))
          .filter(F.col("cnt") > 1)
          .agg(F.sum("cnt").alias("total_dupes"))
          .collect()[0]["total_dupes"] or 0
    )
    if dup_count > 0:
        logger.error(
            "duplicates_detected",
            extra={"label": label, "key_cols": key_cols, "duplicate_rows": dup_count},
        )
    return dup_count
```

### Automated Reconciliation Report (Airflow Task)

```python
from airflow.sdk import task
import pandas as pd

@task
def reconciliation_report(run_date: str) -> dict:
    checks = [
        {
            "name": "orders_row_count",
            "upstream_sql": f"SELECT COUNT(*) FROM source.orders WHERE order_date='{run_date}'",
            "downstream_sql": f"SELECT COUNT(*) FROM dw.fact_orders WHERE order_date='{run_date}'",
        },
        {
            "name": "orders_amount_sum",
            "upstream_sql": f"SELECT ROUND(SUM(amount),2) FROM source.orders WHERE order_date='{run_date}'",
            "downstream_sql": f"SELECT ROUND(SUM(amount),2) FROM dw.fact_orders WHERE order_date='{run_date}'",
        },
    ]
    rows = []
    for check in checks:
        upstream_val = _query_scalar(SOURCE_CONN, check["upstream_sql"])
        downstream_val = _query_scalar(DW_CONN, check["downstream_sql"])
        passed = abs(upstream_val - downstream_val) / max(abs(upstream_val), 1) <= 0.001
        rows.append({"check": check["name"], "upstream": upstream_val,
                      "downstream": downstream_val, "passed": passed})

    report = pd.DataFrame(rows)
    failures = report[~report["passed"]]
    if len(failures):
        _post_slack(f"Reconciliation FAILED for {run_date}:\n{failures.to_string()}")
        raise ValueError(f"{len(failures)} reconciliation checks failed")
    return report.to_dict()
```

---

## 7. Graceful Degradation

### Circuit Breaker Pattern

```python
import time
import threading
from enum import Enum

class State(Enum):
    CLOSED = "closed"         # normal — requests pass through
    OPEN = "open"             # tripped — requests fail-fast with fallback
    HALF_OPEN = "half_open"   # probing — one test request allowed


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 2,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self._state = State.CLOSED
        self._failures = 0
        self._successes = 0
        self._opened_at: float = 0.0
        self._lock = threading.Lock()

    @property
    def state(self) -> State:
        with self._lock:
            if self._state == State.OPEN:
                if time.monotonic() - self._opened_at >= self.recovery_timeout:
                    self._state = State.HALF_OPEN
            return self._state

    def call(self, fn, *args, fallback=None, **kwargs):
        st = self.state
        if st == State.OPEN:
            return fallback() if callable(fallback) else fallback

        try:
            result = fn(*args, **kwargs)
            with self._lock:
                self._failures = 0
                if st == State.HALF_OPEN:
                    self._successes += 1
                    if self._successes >= self.success_threshold:
                        self._state = State.CLOSED
                        self._successes = 0
            return result
        except Exception as exc:
            with self._lock:
                self._failures += 1
                self._successes = 0
                if self._failures >= self.failure_threshold:
                    self._state = State.OPEN
                    self._opened_at = time.monotonic()
                    logger.error("circuit_breaker_opened", extra={"error": str(exc)})
            raise


# Usage — wrap external API call
enrichment_cb = CircuitBreaker(failure_threshold=5, recovery_timeout=120.0)

def enrich_record(record: dict) -> dict:
    def fetch():
        return requests.get(f"https://api.enrichment.io/v1/{record['id']}", timeout=3).json()

    def stale_fallback():
        # Return last known-good data from cache
        return cache.get(record["id"]) or {}

    return enrichment_cb.call(fetch, fallback=stale_fallback)
```

### Fallback to Cached / Stale Data

```python
import diskcache

cache = diskcache.Cache("/tmp/pipeline_cache")

def get_reference_data(key: str, max_age_s: int = 3600) -> dict:
    """Return fresh data if API is up; stale cached data if it is down."""
    try:
        data = fetch_api_data(key)
        cache.set(key, data, expire=max_age_s * 24)  # cache for 24x SLA
        return data
    except TransientError as exc:
        cached = cache.get(key)
        if cached:
            logger.warning("using_stale_cache", extra={"key": key, "reason": str(exc)})
            return cached
        raise  # no cache — propagate error
```

### Partial Pipeline Execution — Skip Failed Partitions

```python
from airflow.sdk import task
import pendulum

@task
def load_partitions(partitions: list[str]) -> dict:
    results = {"success": [], "failed": []}
    for partition in partitions:
        try:
            _load_partition(partition)
            results["success"].append(partition)
        except PermanentError as exc:
            logger.error(
                "partition_skipped",
                extra={"partition": partition, "reason": str(exc)},
            )
            results["failed"].append(partition)
            # Continue with remaining partitions; alert at the end
    if results["failed"]:
        _post_slack(
            f"WARNING: {len(results['failed'])} partitions skipped: "
            f"{results['failed']}"
        )
    return results
```

---

## 8. Change Management

### Blue/Green Table Swap

Write to a staging table (`_new` suffix), validate, then atomically rename. Production reads are never interrupted.

```sql
-- Step 1: create new version of the table (outside a transaction)
CREATE TABLE dim_products_new AS SELECT * FROM dim_products WHERE 1=0;

-- Step 2: load data into the new table
INSERT INTO dim_products_new SELECT ... FROM source;

-- Step 3: validate
SELECT COUNT(*) FROM dim_products_new;   -- must match expected row count

-- Step 4: atomic swap (one DDL statement — no window where table is missing)
ALTER TABLE dim_products RENAME TO dim_products_old;
ALTER TABLE dim_products_new RENAME TO dim_products;

-- Step 5: keep _old for 24h rollback window, then drop
-- DROP TABLE dim_products_old;
```

```python
# Python helper for blue/green swap with validation gate
def blue_green_swap(
    conn,
    table: str,
    new_table: str,
    min_row_count: int,
):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {new_table}")
        count = cur.fetchone()[0]
        if count < min_row_count:
            raise ValueError(
                f"Validation failed: {new_table} has {count} rows, "
                f"expected >= {min_row_count}"
            )
        old_table = f"{table}_old"
        # Atomic swap — both renames in a single transaction
        cur.execute(f"ALTER TABLE {table} RENAME TO {old_table}")
        cur.execute(f"ALTER TABLE {new_table} RENAME TO {table}")
    logger.info("blue_green_swap_complete",
                extra={"table": table, "row_count": count})
```

### Schema Migration Checklist

Run migrations in the **expand-contract** pattern — never drop columns in the same deploy that removes code that reads them.

**Phase 1 — Expand (backward-compatible, run before code deploy)**
- [ ] Add new nullable columns with `ALTER TABLE ... ADD COLUMN ... DEFAULT NULL`
- [ ] Add new tables, indexes, or views
- [ ] Deploy code that writes to BOTH old and new columns/tables

**Phase 2 — Migrate (backfill data if needed)**
- [ ] Backfill new column from old: `UPDATE table SET new_col = old_col WHERE new_col IS NULL`
- [ ] Verify row counts and checksums post-backfill

**Phase 3 — Contract (cleanup, run only after Phase 2 is verified in production)**
- [ ] Remove old code paths
- [ ] Drop deprecated columns: `ALTER TABLE ... DROP COLUMN ...`
- [ ] Drop deprecated tables

### Backward-Compatible Schema Changes

| Change | Safe? | Notes |
|---|---|---|
| Add nullable column | Yes | Existing readers unaffected |
| Add column with DEFAULT | Yes | Check default doesn't break constraints |
| Widen column type (INT→BIGINT) | Usually yes | Verify all consumers accept wider type |
| Rename column | No — use alias first | Add alias view; migrate consumers; drop old |
| Change column type (FLOAT→TEXT) | No | Use expand-contract |
| Drop column | No — must contract | Ensure no readers before dropping |
| Add NOT NULL constraint | No | Backfill NULLs first; add constraint last |

### Rollback Procedure

```sql
-- Rollback a blue/green swap within the 24h window
ALTER TABLE dim_products RENAME TO dim_products_broken;
ALTER TABLE dim_products_old RENAME TO dim_products;

-- Rollback a schema migration (contract phase not yet applied)
-- Simply redeploy previous application version — old column still exists

-- Rollback a partition overwrite (if Delta Lake / Iceberg)
-- Delta Lake: RESTORE TABLE events TO VERSION AS OF 42;
-- Iceberg: CALL catalog.system.rollback_to_snapshot('schema.table', <snapshot_id>);
```

```python
# Airflow rollback task — run manually or via TriggerDagRunOperator
from airflow.sdk import dag, task
import pendulum

@dag(dag_id="dim_products_rollback", schedule=None,
     start_date=pendulum.datetime(2024, 1, 1, tz="UTC"))
def dim_products_rollback():

    @task
    def swap_back():
        # Assumes _old table exists within rollback window
        _execute_sql("""
            ALTER TABLE dim_products RENAME TO dim_products_broken;
            ALTER TABLE dim_products_old  RENAME TO dim_products;
        """)
        logger.info("rollback_complete", extra={"table": "dim_products"})

    swap_back()

dim_products_rollback()
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Blind `INSERT INTO ... SELECT` | Duplicates on retry | Use `INSERT ... ON CONFLICT` or MERGE |
| Append-mode partition writes | Doubles rows on backfill | Use dynamic partition overwrite |
| Retry without jitter | Thundering herd after shared outage | Add full jitter |
| Retrying on permanent errors | Wastes retries, delays alerting | Classify errors; raise immediately on permanent |
| SLA alert only on task failure | Misses slow-but-succeeding jobs | Add `sla` parameter to time-bound tasks |
| Alerting on every transient error | Alert fatigue | Alert on SLA breach, not individual retries |
| Non-deterministic job IDs | Idempotency guards fail silently | Derive IDs from logical date + DAG ID |
| DROP TABLE then CREATE (swap) | Momentary table-not-found for readers | Always rename, never drop-then-create |
| Run schema DROP in same deploy as code change | Instant breakage if rollback needed | Expand-contract: separate deploys |
| Log row counts only at end of job | No visibility into partial failures | Log per-partition with structured fields |
| No DLQ for streaming pipeline | Bad records silently dropped | Route parse/schema errors to dead-letter topic |
| No circuit breaker on external API | One slow API stalls entire pipeline | Wrap with circuit breaker + stale-cache fallback |

---

## References to Consult When Needed

- `docs/specs/` — Vertica, Trino+Iceberg, Spark SQL specs for engine-specific DDL/DML syntax
- `skills/airflow_dags/SKILL.md` — Full Airflow operator/sensor/callback reference
- `skills/pyspark_streaming/SKILL.md` — Checkpointing, watermarks, exactly-once in Structured Streaming
- `skills/soda_core/SKILL.md` — Full SodaCL check syntax, scan API, Airflow gate integration
- `skills/great_expectations/SKILL.md` — Expectation suites, Checkpoint actions, Slack integration
- `skills/openlineage/SKILL.md` — Run/Job/Dataset lineage events, Marquez, column-level lineage
- `skills/delta_lake/SKILL.md` — RESTORE, Change Data Feed, OPTIMIZE, Time Travel syntax
- `skills/apache_kafka/SKILL.md` — DLQ config, consumer lag monitoring, exactly-once semantics
- [Data Engineering Design Patterns — Idempotency (O'Reilly)](https://www.oreilly.com/library/view/data-engineering-design/9781098165826/ch04.html)
- [Airflow SLA and Retries — Orchestra Guide](https://www.getorchestra.io/guides/airflow-concepts-airflow-sla-and-retries)
- [OpenTelemetry for Data Pipelines — bix-tech.com](https://bix-tech.com/distributed-observability-for-data-pipelines-with-opentelemetry-a-practical-endtoend-playbook-for-2026/)
- [Circuit Breaker Pattern — Azure Architecture Center](https://learn.microsoft.com/en-us/azure/architecture/patterns/circuit-breaker)
- [Soda Data Reconciliation — docs.soda.io](https://docs.soda.io/data-testing/data-reconciliation)
