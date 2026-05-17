---
name: starrocks-admin-storage-balancer
description: StarRocks storage balancing — tablet distribution across BEs, disk skew detection (SHOW BACKENDS disk usage), cluster_balance analysis (SHOW PROC '/cluster_balance'), balance_load_score_threshold tuning, tablet migration monitoring, BE decommission, adding new BE nodes, storage capacity forecasting, imbalance alerts
---

# StarRocks Storage Balancer Administration

## When to Use

Load this skill when the user needs to:
- **Add new BE nodes** to an existing cluster and wait for automatic rebalancing to complete
- **Diagnose disk skew alerts** where one or more BEs have significantly higher `UsedPct` than others
- **Decommission a BE** node gracefully, ensuring all tablets migrate away before the node is removed
- **Recover from a BE failure** by understanding how missing replicas are repaired and load redistributed
- **Tune balance aggressiveness** via `balance_load_score_threshold`, `max_balancing_tablets`, and related FE parameters
- **Monitor tablet migration progress** via `SHOW PROC '/cluster_balance'` and Prometheus metrics
- **Forecast storage capacity** and estimate days-to-full based on current disk usage trends
- **Move tablets between disks** on the same BE when one disk path is hot

---

## Tablet Distribution Concepts

### How StarRocks Distributes Tablets

StarRocks splits each table partition into `bucket_num` tablets. Each tablet has `replication_num` replicas (default 3). The FE **tablet scheduler** is responsible for placing and balancing replicas.

```
Table Partition
  └─ Tablet 1  → replica on BE-1, BE-2, BE-3
  └─ Tablet 2  → replica on BE-1, BE-3, BE-4
  └─ Tablet 3  → replica on BE-2, BE-3, BE-4
  ...
```

Placement decisions are made by the **tablet scheduler** (runs on Leader FE) using two objectives:

1. **Repair**: fix under-replicated, missing, or inconsistent replicas — always has highest priority.
2. **Balance**: redistribute tablets to reduce load score skew across BEs — lower priority than repair.

### Load Score Calculation

The FE assigns each BE a **load score** used to compare relative heaviness:

```
load_score = w_tablet * (tablet_num / avg_tablet_num)
           + w_disk   * (data_used_capacity / total_capacity)
```

Default weights are equal (`w_tablet = w_disk = 0.5`). The scheduler migrates tablets **from the highest-score BE to the lowest-score BE** when the score difference exceeds `balance_load_score_threshold` (default `0.1` = 10%).

### What Triggers Rebalancing

- A new BE is registered: its load score is 0, so it is far below the cluster average — migrations begin immediately.
- A BE is decommissioned: its tablets must move to other BEs.
- A BE recovers after downtime and has missing replicas cloned in, temporarily changing the distribution.
- Ongoing ingestion naturally shifts tablet counts over time; the scheduler periodically re-evaluates balance.
- An operator manually changes `balance_load_score_threshold` to a smaller value.

---

## Detecting Imbalance

### SHOW BACKENDS — Per-BE Disk Usage

```sql
-- Run from any MySQL client (port 9030)
SHOW BACKENDS\G
```

Key columns for storage balance:

| Column | Meaning |
|---|---|
| `BackendId` | Unique numeric BE identifier |
| `Host` | Hostname or IP |
| `Alive` | `true` if heartbeat received within timeout |
| `SystemDecommissioned` | `true` if a decommission is in progress |
| `TabletNum` | Number of tablet replicas on this BE |
| `DataUsedCapacity` | Bytes of actual tablet data stored |
| `AvailCapacity` | Bytes of free disk space |
| `TotalCapacity` | Total disk capacity of all data paths on this BE |
| `UsedPct` | `(TotalCapacity - AvailCapacity) / TotalCapacity * 100` |
| `MaxDiskUsedPct` | Highest `UsedPct` across individual disk paths on this BE |

### Detecting Imbalanced BEs by Disk Usage

A cluster is considered **disk-imbalanced** when `max(UsedPct) - min(UsedPct) > 20%` across alive BEs:

```sql
-- Requires information_schema.be_configs (StarRocks 3.1+)
-- For older versions parse SHOW BACKENDS output in the application layer.
SELECT
    BackendId,
    Host,
    TabletNum,
    ROUND(DataUsedCapacity / 1073741824.0, 2) AS data_used_gb,
    ROUND(TotalCapacity    / 1073741824.0, 2) AS total_gb,
    ROUND(UsedPct, 2)                         AS used_pct,
    ROUND(MaxDiskUsedPct, 2)                  AS max_disk_pct
FROM information_schema.be_configs
WHERE Alive = 'true'
ORDER BY UsedPct DESC;
```

```sql
-- Imbalance detection: flag when spread > 20 percentage points
SELECT
    MAX(UsedPct)                              AS max_used_pct,
    MIN(UsedPct)                              AS min_used_pct,
    ROUND(MAX(UsedPct) - MIN(UsedPct), 2)     AS spread_pct,
    CASE
        WHEN MAX(UsedPct) - MIN(UsedPct) > 20 THEN 'IMBALANCED'
        ELSE 'OK'
    END                                       AS disk_balance_status,
    ROUND(AVG(UsedPct), 2)                    AS avg_used_pct,
    MAX(TabletNum)                            AS max_tablets,
    MIN(TabletNum)                            AS min_tablets,
    ROUND(AVG(TabletNum), 0)                  AS avg_tablets,
    ROUND(
        (MAX(TabletNum) - MIN(TabletNum)) * 100.0 / NULLIF(AVG(TabletNum), 0),
        1
    )                                         AS tablet_spread_pct
FROM information_schema.be_configs
WHERE Alive = 'true';
```

**Interpretation thresholds:**

| Metric | OK | Warning | Critical |
|---|---|---|---|
| Disk `UsedPct` spread | < 20 pp | 20–40 pp | > 40 pp |
| Tablet count spread | < 30% | 30–60% | > 60% |
| Any single `MaxDiskUsedPct` | < 75% | 75–85% | > 85% |

### SHOW PROC '/cluster_balance' — Sub-paths

```sql
-- Top-level overview of the balance subsystem
SHOW PROC '/cluster_balance';
```

This path lists sub-nodes. Navigate each with `SHOW PROC`:

| Sub-path | What it shows |
|---|---|
| `/cluster_balance/overview` | Summary counters: pending tablet count, running migrations, slots per path |
| `/cluster_balance/working_slots` | Available clone slots per BE disk path (how much concurrency is left) |
| `/cluster_balance/balance` | In-flight balance migrations currently being executed |
| `/cluster_balance/pending_tablets` | Tablets queued for migration (waiting for a free slot) |
| `/cluster_balance/history_tablets` | Ring buffer of recently completed migrations |

```sql
-- Summary: how many migrations are queued and running right now
SHOW PROC '/cluster_balance/overview';
-- Key fields: TotalScheduledTabletNum, TotalRunningTabletNum, BalanceSlotNumPerPath
-- A healthy idle cluster: TotalRunningTabletNum = 0 and TotalScheduledTabletNum = 0.
-- After adding a new BE: expect hundreds to thousands of TotalScheduledTabletNum initially.

-- In-flight migrations: source BE → destination BE
SHOW PROC '/cluster_balance/balance';
-- Columns: TabletId, Type (BALANCE), State (PENDING/RUNNING/FINISHED),
--          OrigTabletId, SchemaHash, Src, Dest, CreateTime, ScheduledTime, FinishedTime

-- Pending migrations (waiting for slot)
SHOW PROC '/cluster_balance/pending_tablets';
-- Same columns as /balance but for tablets not yet started.
```

---

## Balance Configuration

All balance parameters are dynamic FE configuration values. Changes take effect immediately without a restart.

### balance_load_score_threshold

Controls how large the load score difference between two BEs must be before a migration is initiated.

```sql
-- View current value
ADMIN SHOW FRONTEND CONFIG LIKE '%balance_load_score_threshold%';

-- Default is 0.1 (10%). Lower = more aggressive, more migrations.
-- Raise to 0.2 if the cluster is frequently migrating tablets in a noisy cluster.
-- Lower to 0.05 after adding a large batch of new BEs to speed up initial fill.
ADMIN SET FRONTEND CONFIG ("balance_load_score_threshold" = "0.1");
```

**When to tune:**
- Newly added BEs are filling too slowly: lower to `0.05`.
- Cluster has continuous low-volume migrations that never fully settle: raise to `0.15`.
- Small cluster (3 BEs) with natural data skew from partitioning: raise to `0.2` to avoid futile churn.

### tablet_sched_balancer_strategy

Selects what the load score is based on.

```sql
ADMIN SHOW FRONTEND CONFIG LIKE '%tablet_sched_balancer_strategy%';

-- Values:
--   'disk_and_tablet'  (default) — score combines disk usage AND tablet count
--   'disk'             — score based on disk usage only; tablet count is ignored
ADMIN SET FRONTEND CONFIG ("tablet_sched_balancer_strategy" = "disk_and_tablet");
```

Use `disk` strategy when tables have very uneven tablet sizes (e.g., range-partitioned tables where recent partitions are much larger) to prioritize equalizing disk usage over tablet count.

### max_balancing_tablets

Maximum number of concurrent tablet clone tasks generated by the balance scheduler at any moment.

```sql
ADMIN SHOW FRONTEND CONFIG LIKE '%max_balancing_tablets%';

-- Default: 100. Safe range: 50–500.
-- Increase during maintenance windows to fill new BEs faster.
-- Decrease during peak query hours to reduce I/O contention.
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets" = "100");
```

### tablet_sched_max_migration_task_sent_per_minute

Throttle on how many migration tasks the scheduler dispatches per minute to any single BE.

```sql
ADMIN SHOW FRONTEND CONFIG LIKE '%tablet_sched_max_migration_task_sent_per_minute%';

-- Default: 800. Lower to 200–400 on spinning-disk deployments.
-- High values can saturate disk I/O on the source BE during peak hours.
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_migration_task_sent_per_minute" = "800");
```

### Disabling Balance Temporarily

```sql
-- Pause all balance migrations (repair tasks still run — they are not affected)
ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");

-- Re-enable (CRITICAL: always re-enable after the maintenance window)
ADMIN SET FRONTEND CONFIG ("disable_balance" = "false");
```

### Viewing All Balance-Related FE Config

```sql
ADMIN SHOW FRONTEND CONFIG LIKE '%balance%';
ADMIN SHOW FRONTEND CONFIG LIKE '%tablet_sched%';
```

---

## Adding New BE Nodes

Adding a BE node is a zero-downtime operation. Tablets migrate automatically after the node registers.

### Step-by-Step

**Step 1 — Deploy the BE process on the new host.**

```bash
# On the new BE host: edit be.conf
# Required settings:
#   be_host = <new_be_ip>           # must be reachable by all FEs
#   be_port = 9060                  # Thrift RPC
#   webserver_port = 8040           # HTTP Web UI
#   brpc_port = 8060                # Binary RPC
#   heartbeat_service_port = 9050   # FE→BE heartbeat
#   storage_root_path = /data1;/data2   # semi-colon-separated disk paths

# Start the BE process
bin/start_be.sh --daemon
```

**Step 2 — Register the new BE from the Leader FE.**

```sql
-- Connect to FE Leader (port 9030) as a user with ALTER SYSTEM privilege
ALTER SYSTEM ADD BACKEND 'new_be_host:9050';
-- Use the heartbeat_service_port (9050), NOT the be_port (9060).
```

**Step 3 — Verify the BE has joined.**

```sql
SHOW BACKENDS\G
-- New BE should appear with:
--   Alive = true
--   TabletNum = 0      (initially — tablets have not migrated yet)
--   SystemDecommissioned = false
--   DataUsedCapacity = 0
```

**Step 4 — Optionally accelerate initial fill.**

```sql
-- Lower balance threshold and raise concurrency temporarily
ADMIN SET FRONTEND CONFIG ("balance_load_score_threshold" = "0.05");
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets" = "300");
```

**Step 5 — Monitor rebalancing progress.**

```sql
-- Watch tablet count grow on the new BE over time
SELECT BackendId, Host, TabletNum, ROUND(UsedPct, 2) AS used_pct
FROM information_schema.be_configs
ORDER BY TabletNum DESC;

-- Check pending migration queue shrinking
SHOW PROC '/cluster_balance/overview';

-- Watch in-flight migrations
SHOW PROC '/cluster_balance/balance';
```

**Step 6 — Restore normal balance settings once equilibrium is reached.**

```sql
ADMIN SET FRONTEND CONFIG ("balance_load_score_threshold" = "0.1");
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets" = "100");
```

**Step 7 — Confirm cluster is balanced.**

```sql
SELECT
    ROUND(MAX(UsedPct) - MIN(UsedPct), 2)                         AS disk_spread_pp,
    ROUND(
        (MAX(TabletNum) - MIN(TabletNum)) * 100.0 / AVG(TabletNum),
        1
    )                                                              AS tablet_spread_pct
FROM information_schema.be_configs
WHERE Alive = 'true';
-- Target: disk_spread_pp < 10 and tablet_spread_pct < 20 after equilibrium.
```

---

## Decommissioning a BE

Decommissioning gracefully migrates all tablets off a BE before it is removed from the cluster. Never kill a BE process before decommissioning; that causes replica loss.

### Initiate Decommission

```sql
-- Use the heartbeat_service_port (same port as ADD BACKEND)
ALTER SYSTEM DECOMMISSION BACKEND 'be_host:9050';
```

This sets `SystemDecommissioned = true` on the BE and triggers the tablet scheduler to migrate all replicas to other BEs. The BE remains alive and serves queries during migration.

### Monitor Decommission Progress

```sql
-- Check SystemDecommissioned flag and remaining tablet count
SELECT BackendId, Host, Alive, SystemDecommissioned, TabletNum
FROM information_schema.be_configs
ORDER BY SystemDecommissioned DESC, TabletNum DESC;

-- The target BE's TabletNum should decrease to 0.
-- Decommission is complete when TabletNum = 0 AND the BE is removed from the list.
-- This can take minutes to hours depending on data volume and cluster throughput.
```

```sql
-- Watch migration activity
SHOW PROC '/cluster_balance/balance';
-- Source or Dest matching the decommissioning BE's BackendId confirms migrations are running.

-- SHOW PROC '/backends' for per-disk detail (column SystemDecommissioned)
SHOW PROC '/backends';
```

### Cancelling Decommission

If you change your mind before `TabletNum` reaches 0:

```sql
-- Cancel decommission — all remaining tablets stay on this BE
CANCEL DECOMMISSION BACKEND 'be_host:9050';

-- Verify SystemDecommissioned returns to false
SELECT Host, SystemDecommissioned, TabletNum
FROM information_schema.be_configs
WHERE Host = 'be_host';
```

### Requirements for Successful Decommission

- The remaining BEs must have enough free capacity to absorb all tablets from the decommissioned BE. If not, decommission will stall with tablets unable to find a home.
- The cluster must have at least `replication_num` alive BEs after removing the target. For a 3-replica table: minimum 3 BEs must remain.
- If the cluster has only 3 BEs and `replication_num = 3`, decommission will never complete — there is nowhere to place the third replica copy. Add a replacement BE first.

---

## Disk-Level Balancing

Each BE can have multiple disk paths (`storage_root_path`). StarRocks also balances tablets across disks on the same BE.

### Disk-Level Thresholds

```sql
-- View disk balance thresholds (FE dynamic config)
ADMIN SHOW FRONTEND CONFIG LIKE '%tablet_sched_balance_disk%';
```

| Parameter | Default | Meaning |
|---|---|---|
| `tablet_sched_balance_disk_threshold_low` | `0.5` (50%) | Below this, the disk is a migration target |
| `tablet_sched_balance_disk_threshold_high` | `0.7` (70%) | Above this, the disk is a migration source |

When a disk on a BE exceeds the high threshold, the scheduler migrates tablets from it to disks below the low threshold — either on the same BE or on other BEs.

```sql
-- Adjust thresholds for dense storage deployments (e.g., NVMe arrays)
ADMIN SET FRONTEND CONFIG ("tablet_sched_balance_disk_threshold_low" = "0.55");
ADMIN SET FRONTEND CONFIG ("tablet_sched_balance_disk_threshold_high" = "0.75");
```

### Viewing Per-Disk Breakdown on a BE

```sql
-- SHOW PROC '/backends' returns one row per disk path per BE
SHOW PROC '/backends';
-- Key columns per disk: StorageMedium, StoragePath, UsedCapacity, TotalCapacity, UsedPct,
--                       TabletNum, State (ONLINE/OFFLINE/DECOMMISSIONED)
```

```bash
# From shell: list disk paths on a specific BE
mysql -h fe_host -P 9030 -u root --batch \
  -e "SHOW PROC '/backends'" \
  | awk -F'\t' 'NR==1 || $3=="be_host"'
```

---

## Tablet Migration Monitoring

### SHOW PROC '/cluster_balance/pending_tablets'

```sql
SHOW PROC '/cluster_balance/pending_tablets';
```

Key columns:

| Column | Meaning |
|---|---|
| `TabletId` | ID of the tablet being scheduled |
| `Type` | `BALANCE` (rebalancing) or `REPAIR` (fixing missing/unhealthy replicas) |
| `State` | `PENDING` / `RUNNING` / `FINISHED` / `CANCELLED` |
| `OrigTabletId` | Original tablet (same as TabletId for non-colocate) |
| `SchemaHash` | Schema version hash |
| `Src` | Source BE ID (where tablet is moving from) |
| `Dest` | Destination BE ID (where tablet is moving to) |
| `CreateTime` | When the task was created |
| `ScheduledTime` | When the task was last evaluated |
| `ErrMsg` | Error message if task failed |

### Estimating Completion Time

```sql
-- Count remaining pending + running balance tasks
SELECT
    State,
    COUNT(*) AS task_count
FROM (
    -- Parse from SHOW PROC output via application layer or use:
    SELECT 'PENDING' AS State  -- placeholder; actual query requires SHOW PROC
) t
GROUP BY State;
```

A practical approach using migration throughput:

```sql
-- Step 1: capture TabletNum on the new BE at time T1
-- Step 2: capture again 10 minutes later at T2
-- Step 3: estimate: remaining_tablets / ((T2_tablets - T1_tablets) / 10 min)

-- Example calculation (run in application or Python):
-- T1_tablets = 5000, T2_tablets = 8500 (10 min gap)
-- rate = (8500 - 5000) / 10 = 350 tablets/min
-- target_tablets = 50000 (avg * num_bes / new_be_count)
-- remaining = 50000 - 8500 = 41500
-- ETA = 41500 / 350 = ~118 minutes

-- Monitor rate in real time:
SELECT BackendId, Host, TabletNum, NOW() AS sample_time
FROM information_schema.be_configs
WHERE Alive = 'true'
ORDER BY BackendId;
-- Run this query at T1 and T2; compute the delta.
```

### Prometheus Metrics for Migration

Scrape BE metrics from `http://be_host:8040/metrics`:

| Metric | Type | Description |
|---|---|---|
| `starrocks_be_tablet_migration_count` | gauge | Number of tablet migrations currently in progress on this BE |
| `starrocks_be_clone_task_total` | counter | Cumulative clone tasks completed (includes both repair and balance) |
| `starrocks_be_clone_bytes_total` | counter | Cumulative bytes transferred by clone operations |
| `starrocks_be_disk_data_used_capacity` | gauge | `path` label — bytes per disk path; track per-path fill rate |
| `starrocks_be_tablet_num` | gauge | Tablet count on this BE; converging to cluster mean = balance in progress |

```yaml
# Grafana alert rule — migration throughput stalled
- alert: StarRocksBalanceStalled
  expr: >
    increase(starrocks_be_clone_task_total[30m]) == 0
    and
    starrocks_be_tablet_num < (
      avg(starrocks_be_tablet_num) * 0.7
    )
  for: 30m
  labels:
    severity: warning
  annotations:
    summary: "BE {{ $labels.instance }} has low tablet count and no recent clone activity"
```

---

## Capacity Forecasting

Project days until a BE (or the cluster) reaches a disk usage threshold using historical growth from `information_schema`.

### Days-to-Full Calculation

This query compares current data size with a reference point. For a single-sample forecast, use the ingestion rate from your load monitoring; for a two-point estimate, run the query twice and compute the delta in your application layer.

```sql
-- Current storage snapshot for capacity planning
SELECT
    BackendId,
    Host,
    ROUND(DataUsedCapacity / 1073741824.0,  2)  AS data_used_gb,
    ROUND(TotalCapacity    / 1073741824.0,  2)  AS total_gb,
    ROUND(AvailCapacity    / 1073741824.0,  2)  AS avail_gb,
    ROUND(UsedPct,         2)                   AS used_pct,
    ROUND(MaxDiskUsedPct,  2)                   AS max_disk_pct,
    -- At 80% threshold: how many GB of headroom remains?
    ROUND(
        (TotalCapacity * 0.80 - DataUsedCapacity) / 1073741824.0,
        2
    )                                           AS headroom_to_80pct_gb
FROM information_schema.be_configs
WHERE Alive = 'true'
ORDER BY MaxDiskUsedPct DESC;
```

```sql
-- Two-point forecast (requires storing a snapshot in a staging table)
-- Schema: CREATE TABLE sr_be_snapshots (
--   snapshot_time DATETIME,
--   backend_id BIGINT,
--   data_used_bytes BIGINT
-- );

-- After inserting two snapshots spaced N hours apart:
WITH snapshots AS (
    SELECT
        backend_id,
        MAX(CASE WHEN snapshot_time = (SELECT MAX(snapshot_time) FROM sr_be_snapshots)
                 THEN data_used_bytes END) AS latest_bytes,
        MAX(CASE WHEN snapshot_time = (SELECT MIN(snapshot_time) FROM sr_be_snapshots)
                 THEN data_used_bytes END) AS earliest_bytes,
        TIMESTAMPDIFF(
            HOUR,
            MIN(snapshot_time),
            MAX(snapshot_time)
        )                                  AS hours_elapsed
    FROM sr_be_snapshots
    GROUP BY backend_id
),
growth AS (
    SELECT
        s.backend_id,
        b.Host,
        s.latest_bytes,
        b.TotalCapacity,
        ROUND((s.latest_bytes - s.earliest_bytes) / NULLIF(s.hours_elapsed, 0), 0)
                                           AS bytes_per_hour
    FROM snapshots s
    JOIN information_schema.be_configs b ON b.BackendId = s.backend_id
    WHERE b.Alive = 'true'
)
SELECT
    backend_id,
    Host,
    ROUND(latest_bytes    / 1073741824.0, 2)    AS current_data_gb,
    ROUND(TotalCapacity   / 1073741824.0, 2)    AS total_gb,
    ROUND(bytes_per_hour  / 1073741824.0, 4)    AS growth_gb_per_hour,
    -- Days until 85% full
    ROUND(
        (TotalCapacity * 0.85 - latest_bytes)
        / NULLIF(bytes_per_hour, 0) / 24.0,
        1
    )                                           AS days_to_85pct
FROM growth
ORDER BY days_to_85pct ASC NULLS LAST;
```

**Rule of thumb:** trigger a capacity expansion when `days_to_85pct < 30` on any BE.

---

## Rebalance After BE Failure

When a BE goes dead (process crash, host failure, network partition), the tablet scheduler immediately begins repair — prioritized above normal balance tasks.

### Failure Recovery Sequence

```
BE fails
  │
  ├─ FE heartbeat timeout (default: 30 s) → BE marked Alive = false
  │
  ├─ Tablet scheduler identifies all tablets with a replica on the dead BE
  │    → these become UNDER_REPLICATED (2 replicas remain from 3-replica tables)
  │
  ├─ Repair tasks are generated and dispatched to healthy BEs
  │    → each repair task clones a replica from a live replica to a healthy BE
  │
  └─ After repair: all tablets restored to replication_num replicas
       → balance tasks may re-run afterward to normalize distribution
```

### Monitoring Repair After Failure

```sql
-- Check how many tablets are under-replicated
SELECT
    TABLET_STATUS,
    COUNT(*) AS cnt
FROM information_schema.tablets
GROUP BY TABLET_STATUS
ORDER BY cnt DESC;
-- NORMAL: all replicas healthy
-- UNDER_REPLICATED: one or more replicas missing
-- REPLICA_MISSING: all replicas unavailable (data loss risk)

-- Identify which tables have under-replicated tablets
SELECT
    TABLE_NAME,
    COUNT(*) AS unhealthy_count
FROM information_schema.tablets
WHERE TABLET_STATUS != 'NORMAL'
GROUP BY TABLE_NAME
ORDER BY unhealthy_count DESC
LIMIT 20;
```

```sql
-- Monitor repair progress: CloningTabletNum should decrease over time
SHOW PROC '/statistic';
-- Scan the row with DbName = 'Total':
--   CloningTabletNum  = tablets currently being cloned (repair in progress)
--   UnhealthyTabletNum = tablets still needing attention
```

### Accelerating Repair After Failure

```sql
-- Prioritize repair for a critical table or partition
ADMIN REPAIR TABLE critical_table PARTITION (p2026_01, p2026_02);

-- Check repair priority queue
SHOW PROC '/cluster_balance/pending_tablets';
-- Tasks with Type = 'REPAIR' are scheduled before Type = 'BALANCE' tasks.
```

### Estimating Recovery Time

```
recovery_time ≈ (under_replicated_tablet_count × avg_tablet_size_gb)
                / (clone_bandwidth_gb_per_sec × max_repair_task_slots)

Example:
  5 000 under-replicated tablets × 0.5 GB average = 2 500 GB to clone
  Network bandwidth per BE: ~200 MB/s
  max_tablet_sched_task_slots_per_drive = 4 (default)
  6 healthy BEs × 4 slots × 200 MB/s = 4 800 MB/s = 4.7 GB/s
  Recovery ETA ≈ 2 500 / 4.7 ≈ 532 seconds ≈ ~9 minutes (ideal)

Real world: expect 2–5× longer due to I/O contention with live queries.
```

```sql
-- Tune clone slot concurrency if recovery is too slow (BE config via HTTP API)
-- Increasing this speeds repair but competes with query I/O.
-- (This is a BE-level config; set on each BE individually)
```

```bash
curl -X POST http://be_host:8040/api/update_config \
  -d "max_tablet_sched_task_slots_per_drive=8"
# Default is 4. Raise to 8 on NVMe; keep at 4 on HDD to avoid I/O saturation.
```

### If a BE is Permanently Lost

```sql
-- If the BE host is unrecoverable, drop it from the cluster
-- Only do this after confirming the host will never return.
-- StarRocks will re-replicate all affected tablets once there are enough healthy BEs.
ALTER SYSTEM DROP BACKEND 'dead_be_host:9050';

-- Verify the BE is removed
SELECT BackendId, Host, Alive
FROM information_schema.be_configs;

-- Then monitor repair progress
SHOW PROC '/statistic';
```

**Warning:** `DROP BACKEND` is irreversible. If you drop a BE and then `replication_num = 1` tablets had their only replica there, that data is permanently lost. Always ensure `replication_num >= 3` in production.

---

## Anti-Patterns

**Do not kill the BE process without decommissioning first.**
Abruptly stopping a BE makes all its tablets `UNDER_REPLICATED` simultaneously. The repair storm can saturate cluster I/O for minutes to hours. Always run `ALTER SYSTEM DECOMMISSION BACKEND` and wait for `TabletNum = 0` before stopping the process.

**Do not decommission a BE when the cluster has no capacity headroom.**
If the remaining BEs do not have enough free space to absorb the decommissioned BE's tablets, decommission will stall indefinitely with `TabletNum > 0`. Add replacement BEs or free capacity first.

**Do not set `balance_load_score_threshold = 0` or near-zero.**
This causes the scheduler to migrate tablets continuously for any small imbalance, generating constant background I/O and making the cluster difficult to observe. The minimum practical value is `0.05`.

**Do not raise `max_balancing_tablets` to very high values during peak query hours.**
Tablet migration competes for disk I/O and network bandwidth with scan queries. Keep `max_balancing_tablets <= 100` during business hours; increase to 300–500 only in off-peak maintenance windows.

**Do not forget to re-enable balance after a maintenance pause.**
`ADMIN SET FRONTEND CONFIG ("disable_balance" = "true")` is easy to forget. Always add a scheduled step in your runbook to re-enable it. Tablets accumulate on restarted BEs, creating hotspots that degrade query performance.

**Do not use `ALTER SYSTEM DROP BACKEND` to remove a healthy BE.**
`DROP BACKEND` does NOT migrate tablets first. Use `DECOMMISSION BACKEND` for graceful removal. Use `DROP BACKEND` only for permanently dead hosts that will never rejoin.

**Do not add all new BEs simultaneously to a small cluster during business hours.**
Adding N new BEs simultaneously generates N × avg_tablets migrations at once. This can briefly saturate disk I/O. In production, stagger BE additions by 15–30 minutes to spread the initial migration wave.

**Do not rely on tablet count alone to judge balance.**
A cluster with uniform tablet counts but highly skewed tablet sizes will show disk imbalance despite equal `TabletNum`. Always check both `TabletNum` spread and `UsedPct` spread together.

---

## References to Consult When Needed

- StarRocks docs — Tablet scheduler internals: `https://docs.starrocks.io/docs/administration/management/tablet_management/`
- StarRocks docs — Scale up/down (add/remove BE): `https://docs.starrocks.io/docs/administration/management/Scale_up_down/`
- StarRocks docs — FE configuration reference: `https://docs.starrocks.io/docs/administration/management/FE_configuration/`
- StarRocks docs — BE configuration reference: `https://docs.starrocks.io/docs/administration/management/BE_configuration/`
- StarRocks docs — Monitoring metrics (Prometheus): `https://docs.starrocks.io/docs/administration/management/monitoring/metrics/`
- StarRocks docs — ADMIN commands reference: `https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/`
- StarRocks docs — `information_schema` tables: `https://docs.starrocks.io/docs/reference/information_schema/`
- StarRocks docs — Cluster management overview: `https://docs.starrocks.io/docs/administration/management/cluster_management/`
