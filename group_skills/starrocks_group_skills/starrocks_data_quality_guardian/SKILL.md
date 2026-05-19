---
name: starrocks-data-quality-guardian
description: StarRocks data quality — freshness queries (MAX(updated_at) vs expected SLA), duplicate detection on Primary Key tables, null anomaly SQL (column-level null rate vs baseline), volume drift detection (day-over-day ratio), cross-table referential integrity checks, data completeness for partitions, StarRocks-specific quality checks (tablet health, compaction lag, replication factor), Python DQ scan class
---

# StarRocks Data Quality Guardian

## When to Use

- Validate data freshness, completeness, and integrity in StarRocks tables
- Detect silent data quality regressions after loads
- Monitor structural health (tablet replication, compaction state)
- Build lightweight DQ without SODA/GE — pure SQL + Python
- Create quality metrics visible in dashboards (store results in a DQ table)

---

## Freshness Checks

### Table-Level Freshness

```sql
-- How stale is the latest data?
SELECT
    MAX(updated_at)                                         AS latest_record,
    NOW()                                                   AS checked_at,
    TIMESTAMPDIFF(MINUTE, MAX(updated_at), NOW())           AS lag_minutes,
    CASE
        WHEN TIMESTAMPDIFF(MINUTE, MAX(updated_at), NOW()) <= 5   THEN 'OK'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(updated_at), NOW()) <= 30  THEN 'WARNING'
        ELSE 'CRITICAL'
    END AS freshness_status
FROM sales.orders;
```

### Partition-Level Freshness

```sql
-- Check freshness per partition
SELECT
    dt,
    MAX(updated_at)                                           AS latest_in_partition,
    COUNT(*)                                                   AS row_count,
    TIMESTAMPDIFF(HOUR, MAX(updated_at), NOW())               AS hours_since_update
FROM sales.orders
WHERE dt >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
GROUP BY dt
ORDER BY dt DESC;
```

---

## Duplicate Detection

### Primary Key Violations (Staging Tables)

```sql
-- Find duplicates in a staging/Duplicate Key table
SELECT
    order_id,
    COUNT(*) AS occurrences
FROM sales.orders_staging
WHERE DATE(created_at) = CURDATE()
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 100;
```

### Summary: Duplicate Rate

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS unique_keys,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_rows,
    ROUND(
        (COUNT(*) - COUNT(DISTINCT order_id)) / COUNT(*) * 100, 4
    ) AS dup_rate_pct
FROM sales.orders_staging
WHERE DATE(created_at) = CURDATE();
```

---

## Null Rate Analysis

### Per-Column Null Rates

```sql
SELECT
    'order_id'    AS column_name, SUM(CASE WHEN order_id    IS NULL THEN 1 ELSE 0 END) AS null_count, COUNT(*) AS total FROM sales.orders WHERE dt = CURDATE()
UNION ALL
SELECT 'customer_id', SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM sales.orders WHERE dt = CURDATE()
UNION ALL
SELECT 'amount',      SUM(CASE WHEN amount      IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM sales.orders WHERE dt = CURDATE()
UNION ALL
SELECT 'status',      SUM(CASE WHEN status      IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM sales.orders WHERE dt = CURDATE();
```

### Null Rate vs Historical Baseline

```sql
-- Compare today's null rate with 7-day average
WITH daily_nulls AS (
    SELECT
        dt,
        SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amount,
        COUNT(*) AS total
    FROM sales.orders
    WHERE dt >= DATE_SUB(CURDATE(), INTERVAL 8 DAY)
    GROUP BY dt
),
baseline AS (
    SELECT AVG(null_amount / total) AS avg_null_rate
    FROM daily_nulls
    WHERE dt < CURDATE()
)
SELECT
    d.dt,
    d.null_amount / d.total AS today_null_rate,
    b.avg_null_rate AS baseline_null_rate,
    (d.null_amount / d.total) / NULLIF(b.avg_null_rate, 0) AS ratio
FROM daily_nulls d
CROSS JOIN baseline b
WHERE d.dt = CURDATE();
```

---

## Volume Anomaly Detection

### Day-Over-Day Volume Ratio

```sql
SELECT
    today.order_count                         AS today_count,
    yesterday.order_count                     AS yesterday_count,
    ROUND(today.order_count / NULLIF(yesterday.order_count, 0), 3) AS dod_ratio,
    CASE
        WHEN today.order_count / NULLIF(yesterday.order_count, 0) < 0.5  THEN 'CRITICAL_DROP'
        WHEN today.order_count / NULLIF(yesterday.order_count, 0) < 0.8  THEN 'WARNING_DROP'
        WHEN today.order_count / NULLIF(yesterday.order_count, 0) > 2.0  THEN 'WARNING_SPIKE'
        ELSE 'OK'
    END AS status
FROM (
    SELECT COUNT(*) AS order_count FROM sales.orders WHERE dt = CURDATE()
) today
CROSS JOIN (
    SELECT COUNT(*) AS order_count FROM sales.orders WHERE dt = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
) yesterday;
```

### 7-Day Rolling Average vs Today

```sql
WITH weekly_avg AS (
    SELECT AVG(cnt) AS avg_cnt, STDDEV(cnt) AS std_cnt
    FROM (
        SELECT dt, COUNT(*) AS cnt
        FROM sales.orders
        WHERE dt BETWEEN DATE_SUB(CURDATE(), INTERVAL 8 DAY) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        GROUP BY dt
    ) t
),
today AS (
    SELECT COUNT(*) AS cnt FROM sales.orders WHERE dt = CURDATE()
)
SELECT
    t.cnt AS today_count,
    w.avg_cnt,
    w.std_cnt,
    (t.cnt - w.avg_cnt) / NULLIF(w.std_cnt, 0) AS z_score
FROM today t, weekly_avg w;
-- |z_score| > 3 → anomaly
```

---

## Referential Integrity Checks

### Fact → Dimension Coverage

```sql
-- What fraction of fact rows have no matching dimension?
SELECT
    COUNT(*) AS total_orders,
    SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS orphan_orders,
    ROUND(
        SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 4
    ) AS orphan_rate_pct
FROM sales.orders o
LEFT JOIN sales.dim_customers c ON o.customer_id = c.customer_id
WHERE o.dt = CURDATE();
```

### Cross-System Row Count Reconciliation

```sql
-- Compare Bronze vs Silver row counts for the same day
SELECT
    'bronze' AS layer, COUNT(*) AS row_count
FROM bronze.orders_raw
WHERE DATE(ingested_at) = CURDATE()
UNION ALL
SELECT
    'silver', COUNT(*)
FROM silver.orders
WHERE DATE(created_at) = CURDATE();
```

---

## Data Completeness

### Missing Partitions Check

```sql
-- Find dates with zero rows (missing data)
WITH date_series AS (
    -- Generate last 30 days (StarRocks doesn't have generate_series, use a reference table)
    SELECT DATE_SUB(CURDATE(), INTERVAL n DAY) AS dt
    FROM (
        SELECT 0 AS n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9
        UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14
    ) nums
),
daily_counts AS (
    SELECT dt, COUNT(*) AS row_count
    FROM sales.orders
    WHERE dt >= DATE_SUB(CURDATE(), INTERVAL 15 DAY)
    GROUP BY dt
)
SELECT
    d.dt,
    COALESCE(c.row_count, 0) AS row_count,
    CASE WHEN c.row_count IS NULL THEN 'MISSING' ELSE 'OK' END AS status
FROM date_series d
LEFT JOIN daily_counts c ON d.dt = c.dt
ORDER BY d.dt DESC;
```

---

## StarRocks-Specific Health Checks

### Tablet Replication Health

```sql
-- Tablets with insufficient replicas
SELECT
    TABLE_NAME,
    PARTITION_NAME,
    TABLET_ID,
    REPLICA_COUNT
FROM information_schema.be_tablets
WHERE REPLICA_COUNT < 3
  AND TABLE_SCHEMA = 'sales';
```

### Unhealthy Tablets

```sql
-- Check for unhealthy tablets (requires admin access)
SHOW TABLET WHERE State != 'NORMAL';
```

### Compaction Status

```sql
-- Backends with high compaction score (needs compaction)
SHOW BACKENDS;
-- Check CompactionScore column; > 100 indicates backlog
```

### Routine Load Errors

```sql
-- Routine Load jobs with high error rates
SELECT
    Name,
    State,
    Statistic
FROM (
    SHOW ROUTINE LOAD FROM sales
) rl
-- Parse Statistic JSON for errorRows
WHERE State = 'PAUSED';
```

---

## Python DQ Scan Class

```python
import pymysql
from dataclasses import dataclass, field
from typing import Callable, List
import json


@dataclass
class DQCheck:
    name: str
    sql: str
    assertion: Callable[[float], bool]
    severity: str = "error"   # "warn" or "error"
    description: str = ""


@dataclass
class DQResult:
    check_name: str
    value: float
    passed: bool
    severity: str
    description: str


class StarRocksDQScanner:
    def __init__(self, host: str, port: int = 9030, user: str = "root", password: str = "", db: str = "sales"):
        self.conn_params = dict(host=host, port=port, user=user, password=password, db=db)

    def run_checks(self, checks: List[DQCheck]) -> List[DQResult]:
        results = []
        conn = pymysql.connect(**self.conn_params)
        try:
            for check in checks:
                cursor = conn.cursor()
                cursor.execute(check.sql)
                row = cursor.fetchone()
                value = float(row[0]) if row and row[0] is not None else 0.0
                passed = check.assertion(value)
                results.append(DQResult(
                    check_name=check.name,
                    value=value,
                    passed=passed,
                    severity=check.severity,
                    description=check.description,
                ))
                status = "PASS" if passed else check.severity.upper()
                print(f"  [{status}] {check.name}: {value}")
        finally:
            conn.close()

        failed_errors = [r for r in results if not r.passed and r.severity == "error"]
        if failed_errors:
            raise RuntimeError(
                f"DQ scan failed: {len(failed_errors)} error-level checks failed:\n"
                + "\n".join(f"  - {r.check_name}: {r.value}" for r in failed_errors)
            )
        return results


# Usage
def run_daily_dq(ds: str):
    scanner = StarRocksDQScanner("sr-fe.internal", db="sales")

    checks = [
        DQCheck(
            name=f"orders.row_count.{ds}",
            sql=f"SELECT COUNT(*) FROM orders WHERE dt = '{ds}'",
            assertion=lambda v: v >= 1000,
            description="Minimum 1000 orders expected per day",
        ),
        DQCheck(
            name=f"orders.freshness.{ds}",
            sql=f"SELECT TIMESTAMPDIFF(HOUR, MAX(updated_at), NOW()) FROM orders WHERE dt = '{ds}'",
            assertion=lambda v: v <= 4,
            description="Data must be updated within last 4 hours",
        ),
        DQCheck(
            name=f"orders.null_amount.{ds}",
            sql=f"SELECT SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM orders WHERE dt = '{ds}'",
            assertion=lambda v: v <= 0.001,
            description="Null rate for amount must be <= 0.1%",
        ),
        DQCheck(
            name=f"orders.volume_dod.{ds}",
            sql=f"""
                SELECT today.cnt / NULLIF(yesterday.cnt, 0)
                FROM (SELECT COUNT(*) AS cnt FROM orders WHERE dt = '{ds}') today
                CROSS JOIN (SELECT COUNT(*) AS cnt FROM orders WHERE dt = DATE_SUB('{ds}', INTERVAL 1 DAY)) yesterday
            """,
            assertion=lambda v: 0.5 <= v <= 2.0,
            severity="warn",
            description="Day-over-day volume ratio between 0.5x and 2.0x",
        ),
    ]

    results = scanner.run_checks(checks)
    return results
```

---

## Store DQ Results for Trending

```sql
-- Create DQ results table (one-time setup)
CREATE TABLE IF NOT EXISTS monitoring.dq_results (
    check_name  VARCHAR(256)    NOT NULL,
    check_date  DATE            NOT NULL,
    value       DOUBLE,
    passed      BOOLEAN,
    severity    VARCHAR(16),
    description VARCHAR(512),
    checked_at  DATETIME        DEFAULT CURRENT_TIMESTAMP
) PRIMARY KEY(check_name, check_date)
DISTRIBUTED BY HASH(check_name) BUCKETS 4
PROPERTIES ("enable_persistent_index" = "true");
```

---

## Anti-Patterns

1. **Running DQ checks without partition filter** — full table scan on large tables; always filter to the specific partition being validated.
2. **`COUNT(*)` equality checks** — exact counts change daily; use range checks (>= min, <= max) or ratio checks instead.
3. **No DQ on dimension tables** — bad dimension data silently produces wrong metrics; validate dimensions at load time too.
4. **Running DQ after downstream has already consumed the data** — gate downstream tasks behind DQ; use Airflow `>>` dependency.
5. **Ignoring compaction health** — high compaction score causes slow reads; check BE compaction status in monitoring.
6. **Treating `NULL` and empty string as equivalent** — StarRocks distinguishes them; check both: `amount IS NULL OR amount = ''` for VARCHAR columns.

---

## References

- StarRocks system tables: `docs.starrocks.io/docs/reference/information_schema/`
- SHOW TABLET: `docs.starrocks.io/docs/sql-reference/sql-statements/cluster-management/tablet_replica/SHOW_TABLET/`
- Related skills: `[[airflow-starrocks-data-quality]]`, `[[starrocks-admin-cluster-health]]`, `[[starrocks-admin-query-monitor]]`, `[[soda-core]]`, `[[great-expectations]]`
