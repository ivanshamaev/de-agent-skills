---
name: starrocks-admin-cluster-health
description: StarRocks cluster health monitoring — FE/BE/CN node status, tablet health, replica status, compaction backlog, storage imbalance, SHOW commands, system tables (information_schema/sys), alert thresholds, FE quorum checks, BE disk usage, dead replica detection, Prometheus metrics
---

# StarRocks Cluster Health Monitoring

## When to Use

Load this skill when the user needs to:
- Perform **daily health checks** on a StarRocks cluster (FE/BE/CN liveness, disk usage, tablet health)
- **Triage incidents**: dead nodes, query failures, high latency, replication lag, compaction pressure
- Plan **capacity expansion**: disk usage trends, tablet distribution imbalance, BE load scores
- Verify **FE quorum** after a rolling restart or network partition
- Investigate **under-replicated or missing tablets** following a BE crash or network event
- Monitor **compaction backlog** that may degrade query performance or trigger write stalls
- Build **Prometheus/Grafana alerting** on StarRocks cluster health metrics

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    FE Layer (Metadata)                       │
│  Leader FE ──► Follower FE ──► Follower FE                  │
│  (1 leader, 0–N followers, 0–N observers)                   │
│  BDB-JE edit log replicated via Raft-like consensus          │
└──────────────────────────┬──────────────────────────────────┘
                           │  Heartbeat + Tablet Report
┌──────────────────────────▼──────────────────────────────────┐
│              BE/CN Layer (Storage & Compute)                 │
│                                                              │
│  Shared-Nothing mode: each BE owns local disk storage        │
│    BE-1  BE-2  BE-3  ...  BE-N                               │
│    tablets replicated across BEs (default replication = 3)  │
│                                                              │
│  Shared-Data mode (Lake): CN nodes are stateless compute,    │
│    storage is remote object store (S3/HDFS/OSS/COS/GCS)     │
│    CN-1  CN-2  CN-3  ...  CN-N                               │
└─────────────────────────────────────────────────────────────┘
```

**Key concepts:**
- **FE Leader**: accepts DDL writes, coordinates tablet scheduling, runs global metadata.
- **FE Follower**: participates in quorum; can serve reads and DML.
- **FE Observer**: read-only replica; does NOT count toward quorum.
- **Tablet**: the unit of storage and replication. A table is split into tablets; each tablet has N replicas (usually 3) spread across BEs.
- **Quorum rule**: majority of Follower FEs must be alive. With 3 Followers → need 2 alive; with 5 → need 3.
- **CN (Compute Node)**: shared-data only; no persistent local tablet storage.

---

## FE Health Checks

### SHOW FRONTENDS

```sql
-- Run from any MySQL-compatible client connected to FE QueryPort (default 9030)
SHOW FRONTENDS\G
```

Key columns returned:

| Column | Meaning |
|---|---|
| `Name` | FE node identifier (`<host>_<edit_log_port>`) |
| `Host` | Hostname or IP |
| `EditLogPort` | BDB-JE replication port (default 9010) |
| `HttpPort` | Web UI / REST API port (default 8030) |
| `QueryPort` | MySQL protocol port (default 9030) |
| `RpcPort` | Internal RPC port (default 9020) |
| `Role` | `LEADER`, `FOLLOWER`, or `OBSERVER` |
| `IsMaster` | `true` for the current Leader (deprecated alias for Role=LEADER) |
| `ClusterId` | Numeric cluster ID; all nodes must match |
| `Join` | Whether this node has joined the cluster |
| `Alive` | `true` = heartbeat received within timeout |
| `ReplayedJournalId` | Last BDB-JE journal entry replicated to this FE |
| `LastStartTime` | Wall-clock time of last FE process start |
| `LastHeartbeat` | Timestamp of last heartbeat from this FE |
| `IsHelper` | Whether this node was a helper node at bootstrap |
| `ErrMsg` | Non-empty string if the node is reporting an error |

```sql
-- Identify non-alive FEs
SELECT Name, Host, Role, Alive, ErrMsg
FROM information_schema.fe_metrics   -- available in SR 3.1+
WHERE Alive = 'false';

-- Alternatively via SHOW PROC
SHOW PROC '/frontends';
```

#### FE Quorum Check

```sql
-- Count alive Followers (Observers excluded from quorum)
SELECT
    Role,
    COUNT(*) AS total,
    SUM(CASE WHEN Alive = 'true' THEN 1 ELSE 0 END) AS alive_count
FROM (
    SELECT
        TRIM(Role)  AS Role,
        TRIM(Alive) AS Alive
    FROM information_schema.frontends   -- SR 3.2+; use SHOW FRONTENDS in older versions
) t
WHERE Role IN ('LEADER', 'FOLLOWER')
GROUP BY Role;
```

Expected: `alive_count` for (LEADER + FOLLOWER) combined >= ceil((total_followers + 1) / 2).

```bash
# Shell one-liner — exits non-zero if fewer than 2 out of 3 FEs are alive
mysql -h fe_host -P 9030 -u root --batch --skip-column-names \
  -e "SHOW FRONTENDS" \
  | awk -F'\t' '$10=="true" && $7!="OBSERVER"' \
  | wc -l \
  | xargs -I{} test {} -ge 2
```

#### Edit Log Lag Detection

A Follower whose `ReplayedJournalId` is far behind the Leader is applying metadata slowly — potential sign of GC pressure or I/O bottleneck on that FE host.

```sql
-- Fetch ReplayedJournalId for all FEs (column index 12 in SHOW FRONTENDS)
SHOW FRONTENDS;
-- Compare the Leader's ReplayedJournalId with each Follower's value.
-- A lag > 10 000 journals is worth investigating.
```

---

## BE Health Checks

### SHOW BACKENDS

```sql
SHOW BACKENDS\G
```

Key columns:

| Column | Meaning |
|---|---|
| `BackendId` | Unique numeric BE ID |
| `Host` | Hostname or IP |
| `HeartbeatServicePort` | Port for FE→BE heartbeat (default 9050) |
| `BePort` | Thrift RPC port (default 9060) |
| `HttpPort` | Web UI port (default 8040) |
| `BrpcPort` | Binary RPC port (default 8060) |
| `LastStartTime` | Last process start |
| `LastHeartbeat` | Last heartbeat received |
| `Alive` | `true` = currently healthy |
| `SystemDecommissioned` | `true` if decommission is in progress |
| `TabletNum` | Number of tablet replicas on this BE |
| `DataUsedCapacity` | Bytes of tablet data stored |
| `AvailCapacity` | Free disk space available |
| `TotalCapacity` | Total disk capacity |
| `UsedPct` | `DataUsedCapacity / TotalCapacity * 100` |
| `MaxDiskUsedPct` | Highest `UsedPct` across all disks on this BE |
| `ErrMsg` | Error message if unhealthy |
| `Version` | StarRocks BE version string |
| `Status` | JSON blob with extra status fields |

```sql
-- Dead BE detection
SHOW BACKENDS\G
-- Look for Alive = false rows.

-- Query-based dead BE check
SELECT BackendId, Host, Alive, TabletNum, ErrMsg
FROM information_schema.be_configs   -- use SHOW BACKENDS in older versions
WHERE Alive = 'false';
```

#### Disk Usage Alerts

```sql
-- Via SHOW BACKENDS output (parse in application layer or script)
-- Thresholds:
--   UsedPct > 75%  => WARNING  (consider rebalancing or adding BEs)
--   UsedPct > 85%  => CRITICAL (compaction stalls likely; add capacity immediately)
--   MaxDiskUsedPct > 90% => EMERGENCY (StarRocks may stop writing to that disk)

-- SQL approach using information_schema (SR 3.1+)
SELECT
    BackendId,
    Host,
    ROUND(DataUsedCapacity / 1073741824, 2)  AS data_used_gb,
    ROUND(TotalCapacity    / 1073741824, 2)  AS total_gb,
    ROUND(UsedPct, 2)                        AS used_pct,
    ROUND(MaxDiskUsedPct, 2)                 AS max_disk_pct,
    CASE
        WHEN MaxDiskUsedPct > 85 THEN 'CRITICAL'
        WHEN MaxDiskUsedPct > 75 THEN 'WARNING'
        ELSE 'OK'
    END AS disk_status
FROM information_schema.be_configs
ORDER BY MaxDiskUsedPct DESC;
```

```bash
# SHOW PROC for detailed per-disk breakdown
# Connect via mysql CLI, then:
mysql -h fe_host -P 9030 -u root -e "SHOW PROC '/backends'"
```

---

## Tablet Health

Tablets are the fundamental data unit. Each tablet has N replicas. The FE tablet scheduler continuously monitors replica health.

### Global Tablet Statistics

```sql
-- Overall cluster tablet health summary
SHOW PROC '/statistic';
```

Columns returned by `SHOW PROC '/statistic'`:

| Column | Meaning |
|---|---|
| `DbId` | Database ID (`Total` row = cluster-wide aggregate) |
| `DbName` | Database name |
| `TableNum` | Number of tables |
| `PartitionNum` | Number of partitions |
| `IndexNum` | Number of materialized indexes (rollups) |
| `TabletNum` | Total tablet count |
| `ReplicaNum` | Total replica count |
| `UnhealthyTabletNum` | Replicas missing, under-replicated, or in error state |
| `InconsistentTabletNum` | Replicas whose checksum does not agree |
| `CloningTabletNum` | Tablets currently being cloned/repaired |
| `BadTabletNum` | Tablets with at least one bad replica |
| `ErrorMsgNum` | Count of tablets with error messages |

```sql
-- Focus on cluster-wide totals
SHOW PROC '/statistic';
-- The last row with DbName='Total' gives aggregates.
-- Healthy cluster: UnhealthyTabletNum = 0, InconsistentTabletNum = 0.
```

### Finding Unhealthy Tablets

```sql
-- information_schema.tablets — available SR 2.5+
SELECT
    TABLE_NAME,
    PARTITION_NAME,
    TABLET_ID,
    BACKEND_ID,
    TABLET_STATUS,
    REPLICA_STATUS
FROM information_schema.tablets
WHERE TABLET_STATUS != 'NORMAL'
   OR REPLICA_STATUS != 'NORMAL'
ORDER BY TABLE_NAME, TABLET_ID
LIMIT 200;

-- Drill into a specific tablet
SHOW TABLET 123456789;
-- Returns: DbName, TableName, PartitionName, IndexName, DbId, TableId,
--          PartitionId, IndexId, IsSync, Order, DetailCmd

-- Execute the DetailCmd to see per-replica detail
SHOW PROC '/dbs/<DbId>/<TableId>/partitions/<PartitionId>/<IndexId>/<TabletId>';
```

### Manual Repair Trigger

```sql
-- Force the tablet scheduler to prioritize repair of a table/partition
ADMIN REPAIR TABLE sales_fact PARTITION (p2024_01, p2024_02);

-- Check repair progress
SHOW PROC '/statistic';   -- CloningTabletNum decreases as repair completes

-- Cancel repair priority (returns to normal scheduler priority)
ADMIN CANCEL REPAIR TABLE sales_fact PARTITION (p2024_01);
```

---

## Replica Status

### Problem Replicas

```sql
-- List tablets with replication problems
SHOW PROC '/replicas/problems';
-- Returns tablets that are: UNDER_REPLICATED, VERSION_INCOMPLETE, COLOCATE_MISMATCH,
-- REPLICA_MISSING, REPLICA_RELOCATING, REDUNDANT, etc.

-- Under-replicated: fewer alive replicas than the replication_num configured for the table
-- Missing: all replicas are dead/unreachable
```

#### Manual Replica Status Override

Use `ADMIN SET REPLICA STATUS` only as a last resort when the scheduler is stuck:

```sql
-- Mark a specific replica as BAD so the scheduler will drop and re-clone it
ADMIN SET REPLICA STATUS
    PROPERTIES(
        "tablet_id" = "123456789",
        "backend_id" = "10003",
        "status" = "bad"
    );

-- Mark as OK if a replica was incorrectly flagged
ADMIN SET REPLICA STATUS
    PROPERTIES(
        "tablet_id" = "123456789",
        "backend_id" = "10003",
        "status" = "ok"
    );
```

```sql
-- Count replicas by status across the cluster (SR 3.1+)
SELECT
    REPLICA_STATUS,
    COUNT(*) AS cnt
FROM information_schema.tablets
GROUP BY REPLICA_STATUS
ORDER BY cnt DESC;
```

---

## Compaction Backlog

StarRocks uses LSM-style compaction to merge small rowsets into larger ones. High compaction backlog indicates write pressure and degrades scan performance.

### Compaction Overview

```sql
-- Cluster-wide compaction status
SHOW PROC '/compactions';
-- Returns per-BE compaction stats including:
--   BeId, CumulativeCompactionTaskNum, BaseCompactionTaskNum,
--   CumulativeCompactionScore, BaseCompactionScore
```

| Metric | Warning threshold | Critical threshold |
|---|---|---|
| `CumulativeCompactionScore` | > 100 | > 1000 |
| `BaseCompactionScore` | > 10 | > 100 |
| `CumulativeCompactionTaskNum` | > 50 running | > 200 queued |

**High cumulative score** means small rowsets are accumulating faster than compaction can merge them — usually caused by high-frequency small writes (many small INSERT batches). **High base score** usually means large rowsets haven't been merged into the base rowset recently.

### Compaction Tuning (BE config — set in `be.conf` or via HTTP API)

```bash
# View current compaction config on a BE
curl http://be_host:8040/api/show_config | grep -i compaction

# Increase parallelism for cumulative compaction (BE config, requires restart or dynamic set)
curl -X POST http://be_host:8040/api/update_config \
  -d "cumulative_compaction_num_threads_per_disk=4"

curl -X POST http://be_host:8040/api/update_config \
  -d "base_compaction_num_threads_per_disk=2"

# Lower the score threshold that triggers base compaction
curl -X POST http://be_host:8040/api/update_config \
  -d "base_compaction_check_interval_seconds=60"
```

```sql
-- Per-tablet compaction info (SR 3.1+)
SELECT
    BACKEND_ID,
    COUNT(*) AS tablet_count,
    MAX(ROW_COUNT) AS max_rows_per_tablet
FROM information_schema.tablets
WHERE TABLET_STATUS = 'NORMAL'
GROUP BY BACKEND_ID
ORDER BY tablet_count DESC;
```

---

## Storage Balance

StarRocks automatically balances tablets across BEs based on disk usage and tablet count. Imbalance increases after adding new BEs or recovering from a BE failure.

### Balance Status

```sql
-- View ongoing tablet migration and balance operations
SHOW PROC '/cluster_balance';
-- Sub-paths:
--   /cluster_balance/overview      — summary of pending/running migrations
--   /cluster_balance/pending_tablets   — tablets waiting to be moved
--   /cluster_balance/running_tablets   — tablets currently being cloned for balance
--   /cluster_balance/history_tablets   — recently completed migrations (ring buffer)

SHOW PROC '/cluster_balance/overview';
-- Key fields: TotalScheduledTabletNum, TotalRunningTabletNum, BalanceSlotNumPerPath
```

```sql
-- Detect imbalanced BEs — large spread in TabletNum or DataUsedCapacity
SELECT
    BackendId,
    Host,
    TabletNum,
    ROUND(DataUsedCapacity / 1073741824, 2) AS data_gb,
    ROUND(UsedPct, 2) AS used_pct
FROM information_schema.be_configs
ORDER BY TabletNum DESC;

-- A healthy cluster has tablet counts within ~20% of the mean.
-- Compute coefficient of variation in application code or:
SELECT
    MIN(TabletNum) AS min_tablets,
    MAX(TabletNum) AS max_tablets,
    ROUND(AVG(TabletNum), 0) AS avg_tablets,
    ROUND((MAX(TabletNum) - MIN(TabletNum)) * 100.0 / AVG(TabletNum), 1) AS spread_pct
FROM information_schema.be_configs
WHERE Alive = 'true';
-- spread_pct > 50% after 30 minutes of balance activity warrants investigation.
```

### Balance Configuration (FE dynamic parameters)

```sql
-- View current FE configuration (filter for balance-related params)
ADMIN SHOW FRONTEND CONFIG LIKE '%balance%';

-- Adjust balance aggressiveness (default 0.1; higher = more aggressive rebalancing)
ADMIN SET FRONTEND CONFIG ("balance_load_score_threshold" = "0.1");

-- Maximum concurrent tablet clone tasks per BE
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets" = "100");

-- Disable balance temporarily (e.g., during maintenance window)
ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
-- Re-enable:
ADMIN SET FRONTEND CONFIG ("disable_balance" = "false");
```

---

## Cluster Health Summary Query

A single query that aggregates key health signals into one result set for dashboards or automated checks:

```sql
-- Run against FE QueryPort; requires information_schema access
-- Returns one row per health dimension with status and detail

WITH fe_health AS (
    SELECT
        'FE_ALIVE' AS metric,
        CAST(SUM(CASE WHEN Alive = 'true' THEN 1 ELSE 0 END) AS VARCHAR) AS value,
        CAST(COUNT(*) AS VARCHAR) AS total,
        CASE
            WHEN SUM(CASE WHEN Alive = 'true' AND Role != 'OBSERVER' THEN 1 ELSE 0 END)
                 < CEIL((SUM(CASE WHEN Role != 'OBSERVER' THEN 1 ELSE 0 END) + 1.0) / 2)
            THEN 'CRITICAL'
            WHEN SUM(CASE WHEN Alive = 'false' THEN 1 ELSE 0 END) > 0 THEN 'WARNING'
            ELSE 'OK'
        END AS status
    FROM information_schema.frontends
),
be_alive AS (
    SELECT
        'BE_ALIVE' AS metric,
        CAST(SUM(CASE WHEN Alive = 'true' THEN 1 ELSE 0 END) AS VARCHAR) AS value,
        CAST(COUNT(*) AS VARCHAR) AS total,
        CASE
            WHEN SUM(CASE WHEN Alive = 'false' THEN 1 ELSE 0 END) > 0 THEN 'CRITICAL'
            ELSE 'OK'
        END AS status
    FROM information_schema.be_configs
),
disk_health AS (
    SELECT
        'DISK_MAX_USED_PCT' AS metric,
        CAST(ROUND(MAX(MaxDiskUsedPct), 1) AS VARCHAR) AS value,
        '100' AS total,
        CASE
            WHEN MAX(MaxDiskUsedPct) > 85 THEN 'CRITICAL'
            WHEN MAX(MaxDiskUsedPct) > 75 THEN 'WARNING'
            ELSE 'OK'
        END AS status
    FROM information_schema.be_configs
    WHERE Alive = 'true'
),
tablet_health AS (
    SELECT
        'UNHEALTHY_TABLETS' AS metric,
        CAST(SUM(CASE WHEN TABLET_STATUS != 'NORMAL' THEN 1 ELSE 0 END) AS VARCHAR) AS value,
        CAST(COUNT(*) AS VARCHAR) AS total,
        CASE
            WHEN SUM(CASE WHEN TABLET_STATUS != 'NORMAL' THEN 1 ELSE 0 END) > 0 THEN 'WARNING'
            ELSE 'OK'
        END AS status
    FROM information_schema.tablets
),
balance_check AS (
    SELECT
        'BE_TABLET_SPREAD_PCT' AS metric,
        CAST(ROUND((MAX(TabletNum) - MIN(TabletNum)) * 100.0
                    / NULLIF(AVG(TabletNum), 0), 1) AS VARCHAR) AS value,
        '50' AS total,
        CASE
            WHEN (MAX(TabletNum) - MIN(TabletNum)) * 100.0
                    / NULLIF(AVG(TabletNum), 0) > 50 THEN 'WARNING'
            ELSE 'OK'
        END AS status
    FROM information_schema.be_configs
    WHERE Alive = 'true'
)
SELECT metric, value, total, status
FROM fe_health
UNION ALL SELECT metric, value, total, status FROM be_alive
UNION ALL SELECT metric, value, total, status FROM disk_health
UNION ALL SELECT metric, value, total, status FROM tablet_health
UNION ALL SELECT metric, value, total, status FROM balance_check
ORDER BY FIELD(status, 'CRITICAL', 'WARNING', 'OK'), metric;
```

---

## Prometheus Metrics

StarRocks FE and BE expose metrics at `/metrics` (HTTP) for Prometheus scraping.

### FE Metrics (port 8030 by default)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `starrocks_fe_query_total` | counter | `type` (total/err) | Cumulative queries received by this FE |
| `starrocks_fe_query_err` | counter | — | Cumulative failed queries |
| `starrocks_fe_query_latency_ms` | histogram | `quantile` | Query end-to-end latency percentiles |
| `starrocks_fe_connection_total` | gauge | — | Current active client connections |
| `starrocks_fe_scheduled_tablet_num` | gauge | — | Tablets currently being scheduled (clone/repair) |
| `starrocks_fe_tablet_max_compaction_score` | gauge | — | Highest compaction score across all BEs |
| `starrocks_fe_job` | gauge | `job` (schema_change/rollup/load/export) | Running background jobs |
| `starrocks_fe_meta_log_count` | gauge | — | Number of BDB-JE log entries; large value = slow checkpoint |
| `starrocks_fe_image_write` | counter | — | Number of FE metadata image writes |

### BE Metrics (port 8040 by default)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `starrocks_be_disk_used_capacity` | gauge | `path` | Bytes used on each disk path |
| `starrocks_be_disk_total_capacity` | gauge | `path` | Total capacity per disk path |
| `starrocks_be_disk_data_used_capacity` | gauge | `path` | Bytes of tablet data per disk path |
| `starrocks_be_tablet_num` | gauge | — | Number of tablet replicas on this BE |
| `starrocks_be_compaction_score` | gauge | `type` (cumulative/base) | Compaction score per type |
| `starrocks_be_compaction_bytes_total` | counter | `type` | Bytes compacted |
| `starrocks_be_engine_requests_total` | counter | `status`, `type` | Storage engine requests (read/write/compaction) |
| `starrocks_be_query_scan_rows` | counter | — | Rows scanned by queries on this BE |
| `starrocks_be_query_scan_bytes` | counter | — | Bytes scanned |
| `starrocks_be_mem_pool_bytes_total` | gauge | — | Total memory allocated to memory pool |
| `starrocks_be_process_mem_bytes` | gauge | — | RSS memory of the BE process |
| `starrocks_be_load_bytes` | counter | — | Bytes loaded via stream load / broker load |
| `starrocks_be_rowset_count` | gauge | — | Total number of rowsets; high value = compaction lag |

### Prometheus Scrape Configuration

```yaml
# prometheus.yml scrape config
scrape_configs:
  - job_name: starrocks_fe
    metrics_path: /metrics
    static_configs:
      - targets:
          - fe1:8030
          - fe2:8030
          - fe3:8030

  - job_name: starrocks_be
    metrics_path: /metrics
    static_configs:
      - targets:
          - be1:8040
          - be2:8040
          - be3:8040
```

### Key Alertmanager Rules

```yaml
# alerts/starrocks.yml
groups:
  - name: starrocks_cluster
    rules:
      - alert: StarRocksFEDown
        expr: up{job="starrocks_fe"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "FE node {{ $labels.instance }} is down"

      - alert: StarRocksBEDown
        expr: up{job="starrocks_be"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "BE node {{ $labels.instance }} is down"

      - alert: StarRocksDiskUsageWarning
        expr: >
          starrocks_be_disk_used_capacity / starrocks_be_disk_total_capacity > 0.75
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "BE {{ $labels.instance }} disk {{ $labels.path }} usage > 75%"

      - alert: StarRocksDiskUsageCritical
        expr: >
          starrocks_be_disk_used_capacity / starrocks_be_disk_total_capacity > 0.85
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "BE {{ $labels.instance }} disk {{ $labels.path }} usage > 85%"

      - alert: StarRocksHighCompactionScore
        expr: starrocks_be_compaction_score{type="cumulative"} > 1000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "BE {{ $labels.instance }} cumulative compaction score > 1000"

      - alert: StarRocksQueryErrorRate
        expr: >
          rate(starrocks_fe_query_err[5m])
          / rate(starrocks_fe_query_total[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "FE {{ $labels.instance }} query error rate > 5%"
```

---

## Health Check Script

Production-ready Python script that runs SHOW commands, evaluates thresholds, and exits non-zero on critical findings. Suitable for cron, Airflow PythonOperator, or a liveness probe.

```python
#!/usr/bin/env python3
"""
StarRocks cluster health check script.

Exit codes:
  0  = OK (all thresholds within bounds)
  1  = WARNING (soft threshold breached, not yet critical)
  2  = CRITICAL (hard threshold breached, immediate action required)

Usage:
  python starrocks_health_check.py \
      --host fe1.internal \
      --port 9030 \
      --user root \
      --password secret \
      [--warn-disk 75] \
      [--crit-disk 85] \
      [--crit-unhealthy-tablets 0]
"""

import argparse
import sys
from dataclasses import dataclass, field
from typing import List, Tuple

try:
    import mysql.connector
except ImportError:
    print("CRITICAL: mysql-connector-python not installed. Run: pip install mysql-connector-python")
    sys.exit(2)


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    name: str
    status: str          # 'OK', 'WARNING', 'CRITICAL'
    message: str


@dataclass
class HealthReport:
    results: List[CheckResult] = field(default_factory=list)

    def add(self, result: CheckResult) -> None:
        self.results.append(result)

    @property
    def worst_status(self) -> str:
        priority = {"CRITICAL": 2, "WARNING": 1, "OK": 0}
        return max(self.results, key=lambda r: priority.get(r.status, 0)).status

    @property
    def exit_code(self) -> int:
        return {"OK": 0, "WARNING": 1, "CRITICAL": 2}.get(self.worst_status, 2)

    def print_summary(self) -> None:
        priority = {"CRITICAL": 2, "WARNING": 1, "OK": 0}
        for r in sorted(self.results, key=lambda x: -priority.get(x.status, 0)):
            print(f"[{r.status:<8}] {r.name}: {r.message}")


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_fe_quorum(cursor) -> CheckResult:
    cursor.execute(
        "SELECT Role, Alive FROM information_schema.frontends"
    )
    rows = cursor.fetchall()
    quorum_nodes = [(r["Role"], r["Alive"]) for r in rows if r["Role"] != "OBSERVER"]
    total = len(quorum_nodes)
    alive = sum(1 for _, a in quorum_nodes if str(a).lower() in ("true", "1", "yes"))
    needed = (total // 2) + 1
    dead = total - alive
    if alive < needed:
        return CheckResult(
            "FE_QUORUM",
            "CRITICAL",
            f"Only {alive}/{total} quorum FEs alive (need {needed}). LEADER election at risk.",
        )
    if dead > 0:
        return CheckResult(
            "FE_QUORUM",
            "WARNING",
            f"{dead} FE(s) are down but quorum maintained ({alive}/{total}).",
        )
    return CheckResult("FE_QUORUM", "OK", f"All {total} quorum FEs alive.")


def check_be_alive(cursor) -> CheckResult:
    cursor.execute(
        "SELECT BackendId, Host, Alive, ErrMsg FROM information_schema.be_configs"
    )
    rows = cursor.fetchall()
    dead = [r for r in rows if str(r["Alive"]).lower() not in ("true", "1", "yes")]
    if dead:
        detail = ", ".join(f"{r['Host']} (id={r['BackendId']})" for r in dead[:5])
        return CheckResult(
            "BE_ALIVE",
            "CRITICAL",
            f"{len(dead)} BE(s) dead: {detail}",
        )
    return CheckResult("BE_ALIVE", "OK", f"All {len(rows)} BEs alive.")


def check_disk_usage(
    cursor,
    warn_pct: float = 75.0,
    crit_pct: float = 85.0,
) -> CheckResult:
    cursor.execute(
        "SELECT BackendId, Host, MaxDiskUsedPct FROM information_schema.be_configs WHERE Alive='true'"
    )
    rows = cursor.fetchall()
    critical_bes = [r for r in rows if float(r["MaxDiskUsedPct"]) > crit_pct]
    warning_bes  = [r for r in rows if crit_pct >= float(r["MaxDiskUsedPct"]) > warn_pct]

    if critical_bes:
        detail = ", ".join(
            f"{r['Host']}={float(r['MaxDiskUsedPct']):.1f}%" for r in critical_bes[:3]
        )
        return CheckResult("DISK_USAGE", "CRITICAL", f"BEs above {crit_pct}%: {detail}")
    if warning_bes:
        detail = ", ".join(
            f"{r['Host']}={float(r['MaxDiskUsedPct']):.1f}%" for r in warning_bes[:3]
        )
        return CheckResult("DISK_USAGE", "WARNING", f"BEs above {warn_pct}%: {detail}")
    max_pct = max(float(r["MaxDiskUsedPct"]) for r in rows) if rows else 0.0
    return CheckResult("DISK_USAGE", "OK", f"Max disk usage {max_pct:.1f}%.")


def check_tablet_health(cursor, crit_threshold: int = 0) -> CheckResult:
    cursor.execute(
        "SELECT COUNT(*) AS cnt FROM information_schema.tablets WHERE TABLET_STATUS != 'NORMAL'"
    )
    row = cursor.fetchone()
    count = int(row["cnt"])
    if count > crit_threshold:
        return CheckResult(
            "TABLET_HEALTH",
            "WARNING" if count <= 10 else "CRITICAL",
            f"{count} unhealthy tablet(s). Run: SELECT * FROM information_schema.tablets "
            f"WHERE TABLET_STATUS != 'NORMAL' LIMIT 20;",
        )
    return CheckResult("TABLET_HEALTH", "OK", "All tablets healthy.")


def check_replica_status(cursor) -> CheckResult:
    cursor.execute(
        "SELECT REPLICA_STATUS, COUNT(*) AS cnt FROM information_schema.tablets "
        "GROUP BY REPLICA_STATUS"
    )
    rows = cursor.fetchall()
    status_map = {r["REPLICA_STATUS"]: int(r["cnt"]) for r in rows}
    bad_statuses = {k: v for k, v in status_map.items() if k not in ("NORMAL", "OK")}
    if bad_statuses:
        detail = ", ".join(f"{k}={v}" for k, v in bad_statuses.items())
        total_bad = sum(bad_statuses.values())
        severity = "CRITICAL" if total_bad > 50 else "WARNING"
        return CheckResult("REPLICA_STATUS", severity, f"Non-normal replicas: {detail}")
    return CheckResult("REPLICA_STATUS", "OK", "All replicas have normal status.")


def check_storage_balance(cursor, spread_warn_pct: float = 50.0) -> CheckResult:
    cursor.execute(
        "SELECT MIN(TabletNum) AS mn, MAX(TabletNum) AS mx, AVG(TabletNum) AS avg_t "
        "FROM information_schema.be_configs WHERE Alive='true'"
    )
    row = cursor.fetchone()
    mn, mx, avg_t = float(row["mn"]), float(row["mx"]), float(row["avg_t"])
    if avg_t == 0:
        return CheckResult("STORAGE_BALANCE", "OK", "No tablets to balance.")
    spread = (mx - mn) * 100.0 / avg_t
    if spread > spread_warn_pct:
        return CheckResult(
            "STORAGE_BALANCE",
            "WARNING",
            f"Tablet spread {spread:.1f}% (min={int(mn)}, max={int(mx)}, avg={avg_t:.0f}). "
            "Consider rebalancing.",
        )
    return CheckResult("STORAGE_BALANCE", "OK", f"Tablet spread {spread:.1f}% (OK).")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="StarRocks cluster health check")
    p.add_argument("--host",  default="localhost", help="FE host")
    p.add_argument("--port",  type=int, default=9030, help="FE MySQL port")
    p.add_argument("--user",  default="root")
    p.add_argument("--password", default="")
    p.add_argument("--warn-disk", type=float, default=75.0, help="Disk usage warning %")
    p.add_argument("--crit-disk", type=float, default=85.0, help="Disk usage critical %")
    p.add_argument("--crit-unhealthy-tablets", type=int, default=0)
    p.add_argument("--balance-warn-spread", type=float, default=50.0)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    report = HealthReport()

    try:
        conn = mysql.connector.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database="information_schema",
            connection_timeout=10,
        )
        cursor = conn.cursor(dictionary=True)
    except Exception as exc:
        print(f"[CRITICAL ] CONNECTION: Cannot connect to {args.host}:{args.port} — {exc}")
        sys.exit(2)

    try:
        checks: List[Tuple] = [
            (check_fe_quorum,       (cursor,)),
            (check_be_alive,        (cursor,)),
            (check_disk_usage,      (cursor, args.warn_disk, args.crit_disk)),
            (check_tablet_health,   (cursor, args.crit_unhealthy_tablets)),
            (check_replica_status,  (cursor,)),
            (check_storage_balance, (cursor, args.balance_warn_spread)),
        ]
        for fn, fn_args in checks:
            try:
                report.add(fn(*fn_args))
            except Exception as exc:
                report.add(CheckResult(fn.__name__, "WARNING", f"Check failed: {exc}"))
    finally:
        cursor.close()
        conn.close()

    report.print_summary()
    sys.exit(report.exit_code)


if __name__ == "__main__":
    main()
```

Run the script:

```bash
# Install dependency
pip install mysql-connector-python

# Basic run
python starrocks_health_check.py \
    --host fe1.internal \
    --port 9030 \
    --user root \
    --password "${SR_PASSWORD}" \
    --warn-disk 75 \
    --crit-disk 85

# Integrate with Airflow
# In a DAG: BashOperator(bash_command="python /opt/scripts/starrocks_health_check.py --host ...")
# Task fails automatically on exit code != 0.

# Example output:
# [CRITICAL ] BE_ALIVE: 1 BE(s) dead: be3.internal (id=10005)
# [WARNING  ] DISK_USAGE: BEs above 75%: be1.internal=78.3%, be2.internal=76.1%
# [OK       ] FE_QUORUM: All 3 quorum FEs alive.
# [OK       ] TABLET_HEALTH: All tablets healthy.
# [OK       ] REPLICA_STATUS: All replicas have normal status.
# [OK       ] STORAGE_BALANCE: Tablet spread 12.4% (OK).
```

---

## Quick Reference — SHOW PROC Paths

| Path | What it shows |
|---|---|
| `SHOW PROC '/frontends'` | Detailed FE node list |
| `SHOW PROC '/backends'` | Detailed BE node list with per-disk info |
| `SHOW PROC '/statistic'` | Tablet health totals per database |
| `SHOW PROC '/replicas/problems'` | Tablets with replication issues |
| `SHOW PROC '/compactions'` | Per-BE compaction scores and task counts |
| `SHOW PROC '/cluster_balance'` | Storage balance status and migration queue |
| `SHOW PROC '/cluster_balance/overview'` | Balance job summary |
| `SHOW PROC '/cluster_balance/pending_tablets'` | Tablets waiting migration |
| `SHOW PROC '/cluster_balance/running_tablets'` | In-flight tablet moves |
| `SHOW PROC '/dbs'` | Database list with IDs |
| `SHOW PROC '/tasks'` | Background task list |

---

## Anti-Patterns

**Do not run ADMIN REPAIR TABLE cluster-wide without narrowing to a specific partition.**
`ADMIN REPAIR TABLE t` without a `PARTITION` clause escalates priority for all partitions simultaneously, flooding the tablet scheduler with clone tasks and degrading query performance.

**Do not use `ADMIN SET REPLICA STATUS ... status=bad` on large numbers of replicas at once.**
Marking dozens of replicas bad in rapid succession causes a clone storm. Mark one or two, confirm re-clone completes, then continue.

**Do not disable balance (`disable_balance=true`) and forget to re-enable it.**
After maintenance windows, operators frequently forget to re-enable balancing. Tablets pile up on restarted BEs with zero load, causing hotspots. Add a scheduled re-enable step to your runbook.

**Do not alert only on `Alive=false` for BEs.**
A BE can be alive but have `MaxDiskUsedPct > 90%`, effectively refusing new tablet writes. Always include disk checks alongside liveness checks.

**Do not ignore `InconsistentTabletNum > 0`.**
Inconsistent tablets have diverging checksums across replicas — silent data corruption. Treat this as CRITICAL and escalate immediately; data verification with `ADMIN CHECK TABLE` is required.

**Do not set `max_balancing_tablets` to a very large value under production load.**
Tablet migration competes with user queries for disk I/O and network. Keep `max_balancing_tablets <= 100` during peak hours; increase to 500+ only during maintenance windows.

**Do not rely solely on `SHOW PROC` in automation scripts.**
`SHOW PROC` output format can shift between minor SR versions. Prefer `information_schema` tables (`be_configs`, `frontends`, `tablets`) which have stable schemas. Fall back to `SHOW PROC` only for data not yet exposed in `information_schema`.

---

## References to Consult When Needed

- StarRocks docs — Cluster Management: `https://docs.starrocks.io/docs/administration/management/cluster_management/`
- StarRocks docs — Monitoring Metrics: `https://docs.starrocks.io/docs/administration/management/monitoring/metrics/`
- StarRocks docs — System Variables: `https://docs.starrocks.io/docs/reference/System_variable/`
- StarRocks docs — `information_schema` tables: `https://docs.starrocks.io/docs/reference/information_schema/`
- StarRocks docs — ADMIN commands reference: `https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/`
- StarRocks docs — Tablet Scheduler internals: `https://docs.starrocks.io/docs/administration/management/tablet_management/`
