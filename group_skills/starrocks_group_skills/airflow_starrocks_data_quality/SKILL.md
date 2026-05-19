---
name: airflow-starrocks-data-quality
description: Airflow + StarRocks data quality gates — post-load row count validation, freshness checks (MAX(updated_at) vs expected), null explosion detection, duplicate key audit on Primary Key tables, volume anomaly detection (z-score vs 7-day avg), SODA-style SQL quality checks embedded in DAG, quarantine pattern for bad partitions
---

# Airflow + StarRocks Data Quality Gates

## When to Use

- Validate data after Broker Load or Stream Load completes
- Detect silent data quality regressions (nulls, duplicates, volume anomalies)
- Gate downstream transformations behind passing quality checks
- Quarantine bad partitions before they reach analytical models
- Build lightweight DQ monitoring without a full SODA/GE install

---

## Core Quality Check Task Pattern

```python
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from typing import List, Dict, Any


def sr_check(sql: str, conn_id: str = "starrocks_prod") -> Any:
    """Run a SQL check, return scalar result."""
    hook = MySqlHook(mysql_conn_id=conn_id)
    rows = hook.get_records(sql)
    return rows[0][0] if rows else None


def assert_check(label: str, actual: Any, expected_fn, message: str = ""):
    """Assert a condition; raise on failure."""
    if not expected_fn(actual):
        raise ValueError(f"DQ FAIL [{label}]: actual={actual}. {message}")
    print(f"DQ PASS [{label}]: actual={actual}")
```

---

## Row Count Validation

```python
@task
def check_row_count(
    table: str,
    ds: str,
    db: str = "sales",
    min_rows: int = 1000,
    max_rows: int = None,
):
    count = sr_check(f"""
        SELECT COUNT(*) FROM {db}.{table}
        WHERE DATE(created_at) = '{ds}'
    """)

    assert_check(
        f"{table}.row_count({ds})",
        count,
        lambda v: v >= min_rows,
        f"Expected >= {min_rows} rows",
    )

    if max_rows:
        assert_check(
            f"{table}.row_count_max({ds})",
            count,
            lambda v: v <= max_rows,
            f"Expected <= {max_rows} rows (possible duplication)",
        )
    return count
```

---

## Freshness Check

```python
@task
def check_freshness(
    table: str,
    db: str = "sales",
    max_delay_minutes: int = 60,
):
    """Verify that the latest record is recent enough."""
    latest = sr_check(f"SELECT MAX(updated_at) FROM {db}.{table}")
    if latest is None:
        raise ValueError(f"Table {db}.{table} has no records")

    # Parse datetime
    from datetime import datetime, timezone
    if isinstance(latest, str):
        latest = datetime.strptime(latest, "%Y-%m-%d %H:%M:%S")

    delay_minutes = (datetime.utcnow() - latest).total_seconds() / 60
    assert_check(
        f"{table}.freshness",
        delay_minutes,
        lambda v: v <= max_delay_minutes,
        f"Latest record is {delay_minutes:.1f}m ago, max allowed: {max_delay_minutes}m",
    )
    return latest
```

---

## Null Explosion Detection

```python
@task
def check_null_rates(
    table: str,
    ds: str,
    db: str = "sales",
    columns: List[str] = None,
    max_null_rate: float = 0.01,
):
    """Fail if any column's null rate exceeds threshold."""
    if not columns:
        return

    hook = MySqlHook(mysql_conn_id="starrocks_prod")
    select_nulls = ",\n        ".join(
        f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_{col}"
        for col in columns
    )
    rows = hook.get_records(f"""
        SELECT
            COUNT(*) AS total,
            {select_nulls}
        FROM {db}.{table}
        WHERE DATE(created_at) = '{ds}'
    """)

    if not rows:
        raise ValueError(f"No data for {table} on {ds}")

    row = rows[0]
    total = row[0]
    if total == 0:
        raise ValueError(f"Zero rows in {table} for {ds}")

    failures = []
    for i, col in enumerate(columns):
        null_count = row[i + 1]
        null_rate = null_count / total
        if null_rate > max_null_rate:
            failures.append(f"{col}: {null_rate:.2%} nulls (max {max_null_rate:.2%})")

    if failures:
        raise ValueError(f"Null rate violations in {table}:\n" + "\n".join(failures))

    print(f"Null rate check passed for {table} on {ds} ({total} rows)")
```

---

## Duplicate Detection (Primary Key Tables)

```python
@task
def check_no_duplicates(
    table: str,
    pk_column: str,
    ds: str,
    db: str = "sales",
):
    """Check that Primary Key has no duplicates after load."""
    # For PK tables StarRocks enforces uniqueness, but staging tables may not
    dup_count = sr_check(f"""
        SELECT COUNT(*) FROM (
            SELECT {pk_column}, COUNT(*) AS cnt
            FROM {db}.{table}
            WHERE DATE(created_at) = '{ds}'
            GROUP BY {pk_column}
            HAVING cnt > 1
        ) dups
    """)

    assert_check(
        f"{table}.duplicates({ds})",
        dup_count,
        lambda v: v == 0,
        f"Found {dup_count} duplicate {pk_column} values",
    )
```

---

## Volume Anomaly Detection (Z-Score)

```python
@task
def check_volume_anomaly(
    table: str,
    ds: str,
    db: str = "sales",
    lookback_days: int = 7,
    max_z_score: float = 3.0,
):
    """Fail if today's row count is anomalous vs past N days (z-score)."""
    hook = MySqlHook(mysql_conn_id="starrocks_prod")

    # Get daily counts for past N days
    from datetime import datetime, timedelta
    end_date = datetime.strptime(ds, "%Y-%m-%d")
    start_date = end_date - timedelta(days=lookback_days)

    rows = hook.get_records(f"""
        SELECT DATE(created_at) AS dt, COUNT(*) AS cnt
        FROM {db}.{table}
        WHERE DATE(created_at) BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND '{ds}'
        GROUP BY DATE(created_at)
        ORDER BY dt
    """)

    if len(rows) < 3:
        print(f"Insufficient history for z-score check on {table} ({len(rows)} days)")
        return

    counts = [r[1] for r in rows]
    today_count = counts[-1]
    historical = counts[:-1]

    mean = sum(historical) / len(historical)
    variance = sum((x - mean) ** 2 for x in historical) / len(historical)
    std = variance ** 0.5

    if std == 0:
        print(f"Zero std dev in history for {table} — skipping z-score check")
        return

    z_score = abs(today_count - mean) / std
    print(f"Volume check: today={today_count}, mean={mean:.0f}, std={std:.0f}, z={z_score:.2f}")

    assert_check(
        f"{table}.volume_anomaly({ds})",
        z_score,
        lambda v: v <= max_z_score,
        f"z-score={z_score:.2f} exceeds max={max_z_score}. "
        f"Today={today_count}, historical_mean={mean:.0f}",
    )
```

---

## Referential Integrity Check

```python
@task
def check_referential_integrity(
    fact_table: str,
    fact_fk_column: str,
    dim_table: str,
    dim_pk_column: str,
    ds: str,
    db: str = "sales",
    max_orphan_rate: float = 0.001,
):
    """Check that FK references in fact table exist in dimension."""
    hook = MySqlHook(mysql_conn_id="starrocks_prod")
    rows = hook.get_records(f"""
        SELECT
            COUNT(*) AS total_fact,
            SUM(CASE WHEN d.{dim_pk_column} IS NULL THEN 1 ELSE 0 END) AS orphans
        FROM {db}.{fact_table} f
        LEFT JOIN {db}.{dim_table} d ON f.{fact_fk_column} = d.{dim_pk_column}
        WHERE DATE(f.created_at) = '{ds}'
    """)

    total, orphans = rows[0]
    if total == 0:
        raise ValueError(f"No fact rows for {ds}")

    orphan_rate = orphans / total
    assert_check(
        f"{fact_table}.referential_integrity({ds})",
        orphan_rate,
        lambda v: v <= max_orphan_rate,
        f"Orphan rate {orphan_rate:.4%} exceeds max {max_orphan_rate:.4%} "
        f"({orphans}/{total} rows missing {dim_table})",
    )
```

---

## Quarantine Bad Partitions

If DQ fails, prevent downstream queries from reading the bad partition:

```python
@task
def quarantine_partition(
    table: str,
    ds: str,
    db: str = "sales",
    reason: str = "DQ failure",
):
    """Move bad partition data to quarantine table."""
    hook = MySqlHook(mysql_conn_id="starrocks_prod")
    partition_name = f"p{ds.replace('-', '')}"

    # Copy to quarantine
    hook.run(f"""
        INSERT INTO {db}.{table}_quarantine
        SELECT *, '{reason}' AS quarantine_reason, NOW() AS quarantine_ts
        FROM {db}.{table}
        WHERE DATE(created_at) = '{ds}'
    """)

    # Remove from main table (partition drop)
    hook.run(f"""
        ALTER TABLE {db}.{table}
        DROP PARTITION {partition_name}
    """)

    print(f"Partition {partition_name} quarantined: {reason}")


# Re-create the empty partition so future loads work
@task
def recreate_partition(table: str, ds: str, db: str = "sales"):
    hook = MySqlHook(mysql_conn_id="starrocks_prod")
    from datetime import datetime, timedelta
    p_start = ds
    p_end = (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    p_name = f"p{ds.replace('-', '')}"

    hook.run(f"""
        ALTER TABLE {db}.{table}
        ADD PARTITION {p_name}
        VALUES [('{p_start}'), ('{p_end}'))
    """)
```

---

## Full DQ Gate DAG

```python
@dag(
    dag_id="starrocks_dq_gate",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["starrocks", "dq"],
)
def starrocks_dq_gate():

    # Row count
    row_check = check_row_count(
        table="orders", min_rows=5000, max_rows=5_000_000
    )

    # Freshness
    fresh_check = check_freshness(
        table="orders", max_delay_minutes=240
    )

    # Null rates
    null_check = check_null_rates(
        table="orders",
        columns=["order_id", "customer_id", "amount"],
        max_null_rate=0.0,
    )

    # Volume anomaly
    volume_check = check_volume_anomaly(
        table="orders",
        max_z_score=3.0,
    )

    # Referential integrity
    ref_check = check_referential_integrity(
        fact_table="orders",
        fact_fk_column="customer_id",
        dim_table="dim_customers",
        dim_pk_column="customer_id",
    )

    # All checks must pass before downstream runs
    [row_check, fresh_check, null_check, volume_check, ref_check]


dag = starrocks_dq_gate()
```

---

## Anti-Patterns

1. **Checking counts with `SELECT COUNT(*)` on PK table after partial update** — partial updates don't change row counts; check the specific updated columns instead.
2. **Running DQ queries without partition filter** — full table scans on large tables take minutes; always filter by partition column.
3. **Raising on DQ failure without quarantine** — downstream tasks may still read bad data if they start before the exception propagates; quarantine first.
4. **`max_null_rate=1.0` for required columns** — silently accepts completely null columns; set 0.0 for NOT NULL columns.
5. **Z-score with < 7 days of history** — unreliable; add `if len(rows) < 3: return` guard and log a warning instead of failing.
6. **No DQ checks on dimension tables** — stale or missing dimension rows cause incorrect joins in downstream aggregations; always check FK coverage.

---

## References

- StarRocks SHOW PARTITIONS: `docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/SHOW_PARTITIONS/`
- Related skills: `[[airflow-starrocks-pipeline]]`, `[[airflow-starrocks-etl-best-practices]]`, `[[starrocks-data-quality-guardian]]`, `[[soda-core]]`
