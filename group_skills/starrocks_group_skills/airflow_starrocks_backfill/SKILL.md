---
name: airflow-starrocks-backfill
description: Airflow + StarRocks backfill patterns — historical partition reprocessing, date range loops with INSERT OVERWRITE, Broker Load replay for S3 partitions, safe backfill with max_active_runs=1, partial backfill resume from checkpoint, backfill idempotency via label strategy, clearing downstream tasks before rerun, parallel partition backfill with concurrency limits
---

# Airflow + StarRocks Backfill Patterns

## When to Use

- Reprocess historical partitions after fixing a bug in transform logic
- Re-load data after upstream source correction (wrong values corrected in source)
- Load data for new tables from existing S3 history
- Recover from a failed incremental pipeline that missed N days
- Migrate data from one partition scheme to another

---

## Core Backfill Concepts for StarRocks

StarRocks backfill safety requirements:
1. **INSERT OVERWRITE** — replaces partition atomically; safe to re-run for same date
2. **Broker Load labels** — deterministic label per `(table, date)` ensures idempotency
3. **Partition pre-creation** — target partitions must exist before INSERT OVERWRITE
4. **Airflow `max_active_runs=1`** — prevents concurrent runs from racing on same partition

---

## Pattern 1: Catchup-Based Backfill

Use Airflow's native catchup mechanism for date-by-date reprocessing:

```python
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta

@dag(
    dag_id="starrocks_orders_backfill",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),    # backfill starts from here
    end_date=datetime(2024, 3, 31),     # optional: stop backfill here
    catchup=True,                        # process all missed intervals
    max_active_runs=1,                   # one partition at a time
    tags=["starrocks", "backfill"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
)
def orders_backfill():

    @task
    def ensure_partition(ds: str = None, db: str = "sales"):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        p_name = f"p{ds.replace('-', '')}"
        p_start = ds
        p_end = (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            hook.run(f"""
                ALTER TABLE {db}.orders
                ADD PARTITION {p_name}
                VALUES [('{p_start}'), ('{p_end}'))
            """)
        except Exception as e:
            if "already exists" in str(e).lower() or "Duplicate" in str(e):
                pass  # partition exists — fine
            else:
                raise

    @task
    def load_partition(ds: str = None, db: str = "sales"):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        label = f"backfill_orders_{ds.replace('-', '')}"

        # Check if already completed
        rows = hook.get_records(
            f"SHOW LOAD FROM {db} WHERE LABEL = '{label}'"
        )
        if rows and rows[0][2] == "FINISHED":
            print(f"Partition {ds} already loaded — skipping")
            return

        hook.run(f"""
            LOAD LABEL {db}.{label} (
                DATA INFILE("s3a://datalake/orders/dt={ds}/*.parquet")
                INTO TABLE orders
                FORMAT AS "parquet"
            )
            WITH BROKER (
                "aws.s3.use_instance_profile" = "true",
                "aws.s3.region" = "us-east-1"
            )
            PROPERTIES (
                "timeout" = "3600",
                "max_filter_ratio" = "0.01",
                "load_parallelism" = "4"
            )
        """)

        # Poll for completion
        import time
        for _ in range(120):  # wait up to 60 min
            rows = hook.get_records(
                f"SHOW LOAD FROM {db} WHERE LABEL = '{label}'"
            )
            if rows:
                state = rows[0][2]
                if state == "FINISHED":
                    return
                elif state == "CANCELLED":
                    raise RuntimeError(f"Load CANCELLED for {ds}: {rows[0][7]}")
            time.sleep(30)
        raise TimeoutError(f"Load for {ds} timed out")

    @task
    def analyze_partition(ds: str = None, db: str = "sales"):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        p_name = f"p{ds.replace('-', '')}"
        hook.run(f"""
            ANALYZE TABLE {db}.orders PARTITION ({p_name}) WITH ASYNC MODE
        """)

    partition = ensure_partition()
    load = load_partition()
    analyze = analyze_partition()

    partition >> load >> analyze


dag = orders_backfill()
```

---

## Pattern 2: Programmatic Date Range Backfill DAG

Manually triggered backfill for arbitrary date ranges:

```python
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta

@dag(
    dag_id="starrocks_range_backfill",
    schedule=None,  # triggered manually
    start_date=datetime(2024, 1, 1),
    params={
        "start_date": "2024-01-01",
        "end_date": "2024-03-31",
        "table": "orders",
        "db": "sales",
        "dry_run": False,
    },
    tags=["starrocks", "backfill"],
    max_active_runs=1,
)
def range_backfill():

    @task
    def generate_date_range(params: dict = None) -> list[str]:
        start = datetime.strptime(params["start_date"], "%Y-%m-%d")
        end   = datetime.strptime(params["end_date"],   "%Y-%m-%d")
        dates = []
        cur = start
        while cur <= end:
            dates.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        print(f"Generated {len(dates)} dates: {dates[0]} → {dates[-1]}")
        return dates

    @task
    def check_completed_partitions(dates: list[str], params: dict = None) -> list[str]:
        """Return only dates that need processing (not yet loaded)."""
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        db = params["db"]
        table = params["table"]

        pending = []
        for ds in dates:
            label = f"backfill_{table}_{ds.replace('-', '')}"
            rows = hook.get_records(
                f"SHOW LOAD FROM {db} WHERE LABEL = '{label}'"
            )
            if rows and rows[0][2] == "FINISHED":
                print(f"  SKIP {ds} — already loaded")
            else:
                pending.append(ds)
                print(f"  PENDING {ds}")

        print(f"Pending dates: {len(pending)}/{len(dates)}")
        return pending

    @task
    def backfill_date_batch(pending_dates: list[str], params: dict = None):
        """Process pending dates sequentially."""
        import time
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        db = params["db"]
        table = params["table"]
        dry_run = params.get("dry_run", False)

        results = []
        for ds in pending_dates:
            print(f"\n--- Processing {ds} ---")

            if dry_run:
                print(f"DRY RUN: would load {table} for {ds}")
                results.append({"ds": ds, "status": "dry_run"})
                continue

            # Ensure partition exists
            p_name = f"p{ds.replace('-', '')}"
            p_end = (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            try:
                hook.run(f"""
                    ALTER TABLE {db}.{table}
                    ADD PARTITION {p_name} VALUES [('{ds}'), ('{p_end}'))
                """)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise

            # Trigger load
            label = f"backfill_{table}_{ds.replace('-', '')}"
            hook.run(f"""
                LOAD LABEL {db}.{label} (
                    DATA INFILE("s3a://datalake/{table}/dt={ds}/*.parquet")
                    INTO TABLE {table} FORMAT AS "parquet"
                )
                WITH BROKER (
                    "aws.s3.use_instance_profile" = "true",
                    "aws.s3.region" = "us-east-1"
                )
                PROPERTIES ("timeout" = "3600", "max_filter_ratio" = "0.01")
            """)

            # Poll
            for _ in range(120):
                rows = hook.get_records(
                    f"SHOW LOAD FROM {db} WHERE LABEL = '{label}'"
                )
                if rows:
                    state = rows[0][2]
                    if state == "FINISHED":
                        print(f"  ✓ {ds} loaded")
                        results.append({"ds": ds, "status": "ok"})
                        break
                    elif state == "CANCELLED":
                        raise RuntimeError(f"Load CANCELLED for {ds}: {rows[0][7]}")
                time.sleep(30)
            else:
                raise TimeoutError(f"Load timed out for {ds}")

        return results

    dates = generate_date_range()
    pending = check_completed_partitions(dates)
    backfill_date_batch(pending)


dag = range_backfill()
```

---

## Pattern 3: INSERT OVERWRITE Backfill (Transform Re-run)

When the source data is in StarRocks (not S3), re-apply the transform:

```python
@task
def recompute_partition(ds: str = None, db: str = "sales"):
    """Recompute a partition using INSERT OVERWRITE — idempotent."""
    hook = MySqlHook(mysql_conn_id="starrocks_prod")

    # INSERT OVERWRITE atomically replaces the partition
    hook.run(f"""
        INSERT OVERWRITE {db}.orders_daily PARTITION (dt='{ds}')
        SELECT
            DATE(created_at)          AS dt,
            customer_id,
            COUNT(*)                  AS order_count,
            SUM(amount)               AS total_revenue,
            AVG(amount)               AS avg_order_value,
            COUNT(DISTINCT status)    AS status_variety
        FROM {db}.orders
        WHERE DATE(created_at) = '{ds}'
          AND status != 'cancelled'
        GROUP BY DATE(created_at), customer_id
    """)
    print(f"Recomputed partition dt={ds}")
```

---

## Backfill Progress Tracking

Store backfill state in a tracking table for observability:

```python
# Create tracking table in StarRocks (one-time setup)
CREATE_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS sales.backfill_tracking (
    job_name    VARCHAR(128) NOT NULL,
    table_name  VARCHAR(128) NOT NULL,
    ds          DATE NOT NULL,
    status      VARCHAR(32)  NOT NULL,   -- pending/running/done/failed
    loaded_rows BIGINT,
    started_at  DATETIME,
    finished_at DATETIME
) PRIMARY KEY(job_name, table_name, ds)
DISTRIBUTED BY HASH(ds) BUCKETS 4
PROPERTIES("enable_persistent_index" = "true");
"""

@task
def update_backfill_tracking(
    ds: str,
    table: str,
    status: str,
    loaded_rows: int = None,
):
    hook = MySqlHook(mysql_conn_id="starrocks_prod")
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    hook.run(f"""
        INSERT INTO sales.backfill_tracking
            (job_name, table_name, ds, status, loaded_rows,
             started_at, finished_at)
        VALUES (
            'range_backfill', '{table}', '{ds}', '{status}',
            {loaded_rows or 'NULL'},
            {'NULL' if status != 'running' else f"'{now}'"},
            {'NULL' if status in ('running', 'pending') else f"'{now}'"}
        )
    """)
```

Query progress:
```sql
-- Check backfill progress
SELECT
    ds,
    status,
    loaded_rows,
    TIMESTAMPDIFF(SECOND, started_at, finished_at) AS duration_sec
FROM sales.backfill_tracking
WHERE job_name = 'range_backfill'
  AND table_name = 'orders'
ORDER BY ds;

-- Summary
SELECT status, COUNT(*) AS days, SUM(loaded_rows) AS total_rows
FROM sales.backfill_tracking
WHERE job_name = 'range_backfill'
GROUP BY status;
```

---

## Parallel Backfill with Concurrency Control

For faster backfills, parallelize across dates with a pool limit:

```python
from airflow.providers.mysql.hooks.mysql import MySqlHook
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

@task
def parallel_backfill(
    pending_dates: list[str],
    table: str,
    db: str,
    max_workers: int = 4,  # max concurrent loads
):
    """Process multiple dates in parallel, up to max_workers."""
    hook = MySqlHook(mysql_conn_id="starrocks_prod")

    def load_one(ds: str) -> dict:
        label = f"backfill_{table}_{ds.replace('-', '')}"
        try:
            hook.run(f"""
                LOAD LABEL {db}.{label} (
                    DATA INFILE("s3a://datalake/{table}/dt={ds}/*.parquet")
                    INTO TABLE {table} FORMAT AS "parquet"
                )
                WITH BROKER ("aws.s3.use_instance_profile" = "true", "aws.s3.region" = "us-east-1")
                PROPERTIES ("timeout" = "3600")
            """)
            # Poll for completion
            for _ in range(120):
                rows = hook.get_records(f"SHOW LOAD FROM {db} WHERE LABEL = '{label}'")
                if rows and rows[0][2] == "FINISHED":
                    return {"ds": ds, "status": "ok"}
                elif rows and rows[0][2] == "CANCELLED":
                    return {"ds": ds, "status": "failed", "error": rows[0][7]}
                time.sleep(30)
            return {"ds": ds, "status": "timeout"}
        except Exception as e:
            return {"ds": ds, "status": "error", "error": str(e)}

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(load_one, ds): ds for ds in pending_dates}
        for fut in as_completed(futures):
            result = fut.result()
            results.append(result)
            print(f"  {result['ds']}: {result['status']}")

    failed = [r for r in results if r["status"] != "ok"]
    if failed:
        raise RuntimeError(f"Backfill failed for {len(failed)} dates: {failed}")

    return results
```

---

## Anti-Patterns

1. **`catchup=True` on production incremental DAGs** — backfill and live ingestion run simultaneously, causing watermark conflicts; use a separate dedicated backfill DAG.
2. **Using `INSERT INTO` (append) for backfill** — re-running appends duplicates; always use `INSERT OVERWRITE` for partition replacement.
3. **No `max_active_runs=1` on backfill DAG** — concurrent backfill runs race on the same partition; one will overwrite the other's data.
4. **Backfilling without pre-checking which dates are done** — re-triggering FINISHED labels is safe (idempotent), but wastes resources; skip already-FINISHED labels.
5. **Parallel backfill without BE resource check** — many concurrent Broker Loads saturate BE disk I/O; limit to `max_workers` <= BE count.
6. **Deleting partitions before re-loading** — if the new load fails, the data is gone; use INSERT OVERWRITE which only replaces on success.

---

## References

- Airflow catchup: `airflow.apache.org/docs/apache-airflow/stable/dag-run.html#catchup`
- StarRocks INSERT OVERWRITE: `docs.starrocks.io/docs/loading/InsertInto/`
- StarRocks partition management: `docs.starrocks.io/docs/table_design/Data_distribution/`
- Related skills: `[[airflow-starrocks-pipeline]]`, `[[airflow-starrocks-etl-best-practices]]`, `[[starrocks-broker-load]]`, `[[starrocks-partitioning]]`
