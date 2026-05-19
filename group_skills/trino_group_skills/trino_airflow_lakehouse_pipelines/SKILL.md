---
name: trino-airflow-lakehouse-pipelines
description: Airflow-orchestrated Iceberg lakehouse ETL pipelines — Bronze/Silver/Gold layer DAGs, Iceberg maintenance jobs (optimize/expire_snapshots/remove_orphan_files) as Airflow tasks, snapshot expiration scheduling, compaction DAG patterns, partition-by-partition backfill with dynamic task mapping, late-arriving data handling, watermark tracking table, post-load data quality gates (row count/freshness/null rate checks via TrinoHook), full medallion pipeline DAG example
---

# Airflow Lakehouse Pipelines (Iceberg + Trino)

## When to Use

- Building multi-layer Bronze→Silver→Gold ETL orchestrated by Airflow
- Scheduling Iceberg maintenance (compaction, snapshot expiry, orphan cleanup)
- Handling late-arriving data with reprocessing logic
- Implementing data quality gates between pipeline stages
- Backfilling historical partitions with Airflow dynamic task mapping

---

## Full Medallion Pipeline DAG

```python
from airflow.decorators import dag, task
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from datetime import datetime, timedelta

CONN = 'trino_default'

@dag(
    dag_id            = 'lakehouse_medallion_pipeline',
    start_date        = datetime(2024, 1, 1),
    schedule_interval = '0 3 * * *',
    catchup           = True,
    max_active_runs   = 2,
    default_args      = {'retries': 2, 'retry_delay': timedelta(minutes=5)},
    tags              = ['lakehouse', 'medallion'],
)
def medallion_pipeline():

    # ── BRONZE: raw ingest quality gate ──────────────────────────────
    @task
    def check_bronze_freshness(ds: str, **ctx) -> int:
        hook = TrinoHook(trino_conn_id=CONN)
        count = hook.get_first(f"""
            SELECT COUNT(*) FROM iceberg.bronze.orders_raw
            WHERE ingested_date = DATE '{ds}'
        """)[0]
        if count == 0:
            raise ValueError(f"No bronze data for {ds}")
        return count

    # ── SILVER: cleaned/deduplicated ─────────────────────────────────
    load_silver = TrinoOperator(
        task_id       = 'load_silver_orders',
        trino_conn_id = CONN,
        sql = """
            MERGE INTO iceberg.silver.orders t
            USING (
                SELECT
                    order_id,
                    customer_id,
                    CAST(order_date AS DATE)          AS order_date,
                    TRIM(UPPER(status))               AS status,
                    CAST(amount AS DECIMAL(18,2))     AS amount,
                    COALESCE(region, 'UNKNOWN')       AS region,
                    updated_at
                FROM iceberg.bronze.orders_raw
                WHERE ingested_date = DATE '{{ ds }}'
                  AND order_id IS NOT NULL
            ) s ON t.order_id = s.order_id
            WHEN MATCHED AND s.updated_at > t.updated_at THEN
                UPDATE SET status     = s.status,
                           amount     = s.amount,
                           updated_at = s.updated_at
            WHEN NOT MATCHED THEN
                INSERT (order_id, customer_id, order_date, status, amount, region, updated_at)
                VALUES (s.order_id, s.customer_id, s.order_date, s.status, s.amount, s.region, s.updated_at)
        """,
    )

    # ── SILVER DQ GATE ────────────────────────────────────────────────
    @task
    def dq_silver_gate(ds: str, **ctx) -> None:
        hook = TrinoHook(trino_conn_id=CONN)
        # Row count check
        count = hook.get_first(f"""
            SELECT COUNT(*) FROM iceberg.silver.orders
            WHERE order_date = DATE '{ds}'
        """)[0]
        if count < 100:
            raise ValueError(f"Silver row count {count} too low for {ds}")
        # Null rate check
        null_rate = hook.get_first(f"""
            SELECT CAST(COUNT(*) FILTER (WHERE customer_id IS NULL) AS DOUBLE) / COUNT(*)
            FROM iceberg.silver.orders
            WHERE order_date = DATE '{ds}'
        """)[0]
        if null_rate > 0.01:
            raise ValueError(f"Null rate {null_rate:.2%} exceeds 1% threshold")

    # ── GOLD: aggregate metrics ───────────────────────────────────────
    load_gold = TrinoOperator(
        task_id       = 'load_gold_revenue',
        trino_conn_id = CONN,
        sql = """
            DELETE FROM iceberg.gold.daily_revenue
            WHERE order_date = DATE '{{ ds }}';

            INSERT INTO iceberg.gold.daily_revenue
            SELECT
                order_date,
                region,
                COUNT(*)                                          AS order_count,
                SUM(amount)                                       AS gross_revenue,
                COUNT(*) FILTER (WHERE status = 'completed')     AS completed_orders
            FROM iceberg.silver.orders
            WHERE order_date = DATE '{{ ds }}'
            GROUP BY order_date, region
        """,
    )

    # ── ANALYZE for CBO ──────────────────────────────────────────────
    analyze = TrinoOperator(
        task_id       = 'analyze_silver',
        trino_conn_id = CONN,
        sql = "ANALYZE iceberg.silver.orders WITH (columns = ARRAY['customer_id','order_date','region','status'])",
    )

    bronze_count = check_bronze_freshness()
    bronze_count >> load_silver >> dq_silver_gate() >> load_gold >> analyze

medallion_pipeline()
```

---

## Iceberg Maintenance DAG

Schedule daily after the main pipeline completes.

```python
from airflow.decorators import dag, task
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from datetime import datetime, timedelta

CONN    = 'trino_default'
TABLES  = [
    ('iceberg', 'bronze', 'orders_raw'),
    ('iceberg', 'silver', 'orders'),
    ('iceberg', 'silver', 'customers'),
    ('iceberg', 'gold',   'daily_revenue'),
]

@dag(
    dag_id            = 'iceberg_maintenance',
    start_date        = datetime(2024, 1, 1),
    schedule_interval = '0 6 * * *',   # 6 AM — after main pipeline
    catchup           = False,
    default_args      = {'retries': 1},
    tags              = ['iceberg', 'maintenance'],
)
def iceberg_maintenance():

    for catalog, schema, table in TABLES:
        fqn = f"{catalog}.{schema}.{table}"

        # 1. Compact small files
        optimize = TrinoOperator(
            task_id       = f'optimize_{schema}_{table}',
            trino_conn_id = CONN,
            sql           = f"ALTER TABLE {fqn} EXECUTE optimize(file_size_threshold => '128MB')",
        )

        # 2. Expire old snapshots (keep 7 days, minimum 2 snapshots)
        expire = TrinoOperator(
            task_id       = f'expire_snapshots_{schema}_{table}',
            trino_conn_id = CONN,
            sql = f"""
                ALTER TABLE {fqn}
                EXECUTE expire_snapshots(retention_threshold => '7d', retain_last => 2)
            """,
        )

        # 3. Remove orphan files (after snapshot expiry)
        remove_orphans = TrinoOperator(
            task_id       = f'remove_orphans_{schema}_{table}',
            trino_conn_id = CONN,
            sql = f"""
                ALTER TABLE {fqn}
                EXECUTE remove_orphan_files(retention_threshold => '7d')
            """,
        )

        optimize >> expire >> remove_orphans

iceberg_maintenance()
```

---

## Partition Backfill with Dynamic Task Mapping

```python
from airflow.decorators import dag, task
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime

CONN = 'trino_default'

@dag(
    dag_id     = 'backfill_silver_orders',
    start_date = datetime(2024, 1, 1),
    schedule   = None,   # manual trigger only
    params     = {'start_date': '2024-01-01', 'end_date': '2024-03-31'},
    catchup    = False,
    tags       = ['backfill', 'silver'],
)
def backfill_dag():

    @task
    def get_date_range(start_date: str, end_date: str) -> list[str]:
        hook = TrinoHook(trino_conn_id=CONN)
        rows = hook.get_records(f"""
            SELECT CAST(d AS VARCHAR) FROM (
                SELECT sequence(
                    DATE '{start_date}',
                    DATE '{end_date}',
                    INTERVAL '1' DAY
                )
            ) t(dates)
            CROSS JOIN UNNEST(dates) AS t(d)
        """)
        return [r[0] for r in rows]

    @task
    def process_partition(partition_date: str) -> dict:
        hook = TrinoHook(trino_conn_id=CONN)

        # Delete existing data for idempotency
        hook.run(f"""
            DELETE FROM iceberg.silver.orders
            WHERE order_date = DATE '{partition_date}'
        """)

        # Re-load from bronze
        hook.run(f"""
            INSERT INTO iceberg.silver.orders
            SELECT order_id, customer_id, order_date, status, amount, region, updated_at
            FROM iceberg.bronze.orders_raw
            WHERE ingested_date = DATE '{partition_date}'
              AND order_id IS NOT NULL
        """)

        count = hook.get_first(f"""
            SELECT COUNT(*) FROM iceberg.silver.orders
            WHERE order_date = DATE '{partition_date}'
        """)[0]
        return {'date': partition_date, 'rows': count}

    dates = get_date_range(
        start_date="{{ params.start_date }}",
        end_date="{{ params.end_date }}"
    )
    process_partition.expand(partition_date=dates)

backfill_dag()
```

---

## Watermark Table for Late-Arriving Data

Track the last successfully processed watermark per table to handle late arrivals:

```sql
-- Create watermark tracking table
CREATE TABLE IF NOT EXISTS iceberg.platform.pipeline_watermarks (
    pipeline_name  VARCHAR  NOT NULL,
    table_name     VARCHAR  NOT NULL,
    last_processed DATE     NOT NULL,
    processed_at   TIMESTAMP(6),
    row_count      BIGINT
)
WITH (format = 'PARQUET', format_version = 2);
```

```python
@task
def get_watermark(pipeline: str, table: str) -> str:
    hook = TrinoHook(trino_conn_id=CONN)
    result = hook.get_first(f"""
        SELECT CAST(last_processed AS VARCHAR)
        FROM iceberg.platform.pipeline_watermarks
        WHERE pipeline_name = '{pipeline}' AND table_name = '{table}'
        ORDER BY processed_at DESC LIMIT 1
    """)
    return result[0] if result else '2020-01-01'

@task
def update_watermark(pipeline: str, table: str, ds: str, row_count: int) -> None:
    hook = TrinoHook(trino_conn_id=CONN)
    hook.run(f"""
        INSERT INTO iceberg.platform.pipeline_watermarks
        VALUES ('{pipeline}', '{table}', DATE '{ds}', CURRENT_TIMESTAMP, {row_count})
    """)
```

---

## Data Quality Gate Pattern

```python
@task
def dq_gate(table_fqn: str, ds: str, checks: dict) -> None:
    """
    checks = {
        'min_rows': 1000,
        'max_null_pct': 0.01,
        'freshness_hours': 25
    }
    """
    hook = TrinoHook(trino_conn_id=CONN)

    # Row count
    count = hook.get_first(
        f"SELECT COUNT(*) FROM {table_fqn} WHERE order_date = DATE '{ds}'"
    )[0]
    if count < checks.get('min_rows', 0):
        raise ValueError(f"Row count {count} < min {checks['min_rows']}")

    # Null rate on primary key
    null_pct = hook.get_first(f"""
        SELECT CAST(COALESCE(SUM(CASE WHEN order_id IS NULL THEN 1 END), 0) AS DOUBLE) / COUNT(*)
        FROM {table_fqn}
        WHERE order_date = DATE '{ds}'
    """)[0]
    max_null = checks.get('max_null_pct', 0.0)
    if null_pct > max_null:
        raise ValueError(f"Null rate {null_pct:.3%} > {max_null:.3%}")

    # Freshness
    age_hours = hook.get_first(f"""
        SELECT EXTRACT(HOUR FROM (CURRENT_TIMESTAMP - MAX(updated_at)))
        FROM {table_fqn}
        WHERE order_date = DATE '{ds}'
    """)[0]
    max_age = checks.get('freshness_hours', 48)
    if age_hours and age_hours > max_age:
        raise ValueError(f"Data is {age_hours}h old, threshold {max_age}h")
```

---

## Anti-Patterns

1. **Running OPTIMIZE inside the main ingest DAG on every run** — compaction on every micro-batch multiplies I/O overhead; schedule OPTIMIZE in a separate maintenance DAG (e.g. daily at 6 AM).
2. **EXPIRE_SNAPSHOTS before OPTIMIZE completes** — if optimize creates new snapshots and expiry runs concurrently, it may expire the fresh snapshot; always chain `optimize >> expire >> remove_orphans`.
3. **Dynamic task mapping with hundreds of date partitions** — `expand()` with 365 dates creates 365 Airflow task instances, overwhelming the scheduler DB; chunk large backfills into weekly batches.
4. **No DQ gate between Bronze and Silver** — silently loading malformed bronze data into silver causes downstream model failures that are hard to trace; always check row count and null rate before promoting data.
5. **No `max_active_runs` limit on catchup DAGs** — a pipeline with `catchup=True` and no `max_active_runs` can spawn hundreds of concurrent DAG runs all querying Trino simultaneously; set `max_active_runs=3`.

---

## References

- Iceberg maintenance: `trino.io/docs/current/connector/iceberg.html`
- Airflow Trino provider: `airflow.apache.org/docs/apache-airflow-providers-trino/`
- Related skills: `[[trino-airflow-orchestration]]`, `[[trino-iceberg-best-practices]]`, `[[trino-dbt-platform]]`
