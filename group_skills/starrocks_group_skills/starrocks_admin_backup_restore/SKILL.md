---
name: starrocks-admin-backup-restore
description: StarRocks backup and restore — CREATE REPOSITORY (S3/HDFS/NFS), BACKUP DATABASE/TABLE, SHOW BACKUP/RESTORE status, RESTORE with partition granularity, cross-cluster restore, incremental backup strategy, snapshot lifecycle, disaster recovery RTO/RPO targets
---

# StarRocks — Backup and Restore

## When to Use

Load this skill when the user needs to:
- Set up disaster recovery for a StarRocks cluster (DR environment, RTO/RPO planning)
- Back up specific tables or partitions to S3, HDFS, or NFS
- Restore a database or table after accidental deletion, data corruption, or schema mistake
- Copy data between clusters (production → staging, region migration)
- Implement a scheduled backup strategy with Airflow
- Manage snapshot retention and lifecycle policies
- Comply with data retention or auditability requirements

---

## Architecture Overview

```
StarRocks FE (coordinator)
  │
  ├─ BACKUP → creates a snapshot (consistent read view of tablets)
  │            uploads tablet files to remote repository
  │
  └─ RESTORE ← downloads tablet files from repository
               creates new local tablets, swaps them into the catalog

Repository: S3 / HDFS / NFS (broker-mediated or S3-native)
  └─ <repo_root>/
        └─ __starrocks_repository_v2__/
              └─ <snapshot_name>_<timestamp>/
                    ├─ meta/          (FE metadata: schema, partition info)
                    └─ tablet_<id>/   (BE segment files per tablet)
```

**Key constraints:**
- BACKUP and RESTORE are async jobs — poll with `SHOW BACKUP` / `SHOW RESTORE`.
- Only one BACKUP or RESTORE job can run per database at a time.
- Tables must be OLAP (internal) tables; external tables (Hive, Iceberg catalogs) cannot be backed up.
- Views, materialized views, and UDFs are **not** included in BACKUP.
- StarRocks does not support native incremental backup; partition-level rotation is the workaround.

---

## 1. Repository Setup

### CREATE REPOSITORY — AWS S3

```sql
-- Prerequisite: Broker process must be running on all BE nodes
-- (or use S3-native access for StarRocks 3.0+ without broker)

CREATE REPOSITORY s3_backup_repo
WITH BROKER "broker_name"
ON LOCATION "s3://my-backup-bucket/starrocks-backups"
PROPERTIES (
    "aws.s3.access_key"  = "AKIAIOSFODNN7EXAMPLE",
    "aws.s3.secret_key"  = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "aws.s3.endpoint"    = "s3.us-east-1.amazonaws.com",
    "aws.s3.region"      = "us-east-1"
);
```

**Using IAM Instance Profile (recommended for EC2/EKS deployments — no static keys):**

```sql
CREATE REPOSITORY s3_backup_repo_iam
WITH BROKER "broker_name"
ON LOCATION "s3://my-backup-bucket/starrocks-backups"
PROPERTIES (
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region"               = "us-east-1"
);
```

### CREATE REPOSITORY — S3-Compatible (MinIO)

```sql
CREATE REPOSITORY minio_backup_repo
WITH BROKER "broker_name"
ON LOCATION "s3://backups/starrocks"
PROPERTIES (
    "aws.s3.access_key"        = "minioadmin",
    "aws.s3.secret_key"        = "minioadmin",
    "aws.s3.endpoint"          = "http://minio.internal:9000",
    "aws.s3.region"            = "us-east-1",
    "aws.s3.path_style_access" = "true"    -- required for MinIO / non-AWS
);
```

### CREATE REPOSITORY — HDFS (Kerberos-secured cluster)

```sql
CREATE REPOSITORY hdfs_backup_repo
WITH BROKER "broker_name"
ON LOCATION "hdfs://namenode:9000/user/starrocks/backups"
PROPERTIES (
    "username"                    = "starrocks",
    "password"                    = "",
    "dfs.nameservices"            = "mycluster",
    "dfs.ha.namenodes.mycluster"  = "nn1,nn2",
    "dfs.namenode.rpc-address.mycluster.nn1" = "namenode1:9000",
    "dfs.namenode.rpc-address.mycluster.nn2" = "namenode2:9000",
    "dfs.client.failover.proxy.provider.mycluster"
        = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
);
```

### CREATE REPOSITORY — HDFS with Kerberos

```sql
CREATE REPOSITORY hdfs_kerb_repo
WITH BROKER "broker_name"
ON LOCATION "hdfs://secure-cluster/starrocks/backups"
PROPERTIES (
    "hadoop.security.authentication"  = "kerberos",
    "kerberos_principal"              = "starrocks/starrocks-host@REALM.COM",
    "kerberos_keytab"                 = "/etc/security/keytabs/starrocks.keytab",
    "kerberos_keytab_content"         = ""   -- leave empty when using keytab file path
);
```

### Verify and List Repositories

```sql
-- List all repositories in the cluster
SHOW REPOSITORIES;

-- Expected columns:
-- RepoId | RepoName         | CreateTime          | IsReadOnly | Location                          | Broker         | ErrMsg
-- 10001  | s3_backup_repo   | 2024-01-15 10:00:00 | false      | s3://my-backup-bucket/...         | broker_name    |
```

### Drop Repository

```sql
-- Only removes the repository registration from FE; does NOT delete files on S3/HDFS
DROP REPOSITORY s3_backup_repo;
```

---

## 2. BACKUP Operations

### Full Database Backup

```sql
-- Back up entire database (all OLAP tables)
BACKUP SNAPSHOT analytics.snap_analytics_20240115
TO s3_backup_repo
PROPERTIES (
    "type"    = "full",
    "timeout" = "86400"   -- seconds; default 86400 (24 h); increase for large databases
);
```

### Backup Specific Tables

```sql
-- Back up two tables from the analytics database
BACKUP SNAPSHOT analytics.snap_orders_users_20240115
TO s3_backup_repo
ON (orders, users)
PROPERTIES (
    "type"    = "full",
    "timeout" = "7200"
);
```

### Backup at Partition Granularity

```sql
-- Only back up specific partitions — key pattern for pseudo-incremental backup
-- Partition names must match exactly as shown in SHOW PARTITIONS
BACKUP SNAPSHOT analytics.snap_orders_p202401_20240201
TO s3_backup_repo
ON (
    orders     PARTITION (p202401, p202312),   -- two month partitions from orders
    events     PARTITION (p20240115)           -- one day partition from events
)
PROPERTIES (
    "type"    = "full",
    "timeout" = "3600"
);
```

**BACKUP syntax reference:**

```
BACKUP SNAPSHOT <db_name>.<snapshot_name>
TO <repo_name>
[ ON ( <table_name> [ PARTITION (<part1> [, <part2> ...]) ]
       [, <table_name> [ PARTITION (...) ] ...] ) ]
[ PROPERTIES ("key" = "value" [, ...]) ]
```

| PROPERTIES key | Values | Default | Notes |
|---|---|---|---|
| `type` | `full` | `full` | Only `full` is supported; incremental is not native |
| `timeout` | seconds (string) | `86400` | Job-level timeout before CANCELLED |

### Monitor Backup Progress

```sql
-- Check backup status for the current database
SHOW BACKUP FROM analytics;

-- Check backup status across all databases (requires ADMIN privilege)
SHOW BACKUP;
```

**Status values:**

| Status | Meaning |
|---|---|
| `PENDING` | Job submitted; waiting for FE to schedule |
| `SNAPSHOTING` | FE is creating a consistent tablet snapshot |
| `UPLOAD_SNAPSHOT` | BE nodes are uploading segment files to the repository |
| `UPLOADING` | Upload in progress |
| `SAVE_META` | FE is writing metadata (schema, mapping) to the repository |
| `UPLOAD_INFO` | Finalizing upload info |
| `FINISHED` | Backup complete; snapshot is available in the repository |
| `CANCELLED` | Job failed or timed out; check `Status` column for error details |

```sql
-- Example output columns from SHOW BACKUP:
-- JobId | SnapshotName                    | DbName    | State     | BackupObjs          | CreateTime          | SnapshotFinishedTime | UploadFinishedTime  | FinishedTime        | UnfinishedTasks | Progress | TaskErrMsg | Status | Timeout
-- 10012 | snap_orders_p202401_20240201   | analytics | FINISHED  | [analytics.orders]  | 2024-02-01 02:00:00 | 2024-02-01 02:01:30  | 2024-02-01 02:04:10 | 2024-02-01 02:04:12 |               0 | 100%     |            | OK     | 3600
```

### Cancel a Running Backup

```sql
-- Cancels the currently running backup job for this database
CANCEL BACKUP FROM analytics;
```

---

## 3. RESTORE Operations

### Full Database Restore (same cluster, same DB name)

```sql
RESTORE SNAPSHOT analytics.snap_analytics_20240115
FROM s3_backup_repo
PROPERTIES (
    "backup_timestamp"  = "2024-01-15-10-00-00",   -- timestamp from SHOW SNAPSHOT
    "replication_num"   = "3",
    "timeout"           = "86400"
);
```

### Restore Specific Tables

```sql
RESTORE SNAPSHOT analytics.snap_orders_users_20240115
FROM s3_backup_repo
ON (orders, users)
PROPERTIES (
    "backup_timestamp" = "2024-01-15-10-00-00",
    "replication_num"  = "3",
    "timeout"          = "7200"
);
```

### Restore to a Different Table Name

```sql
-- Rename table during restore — useful for side-by-side comparison or non-destructive recovery
RESTORE SNAPSHOT analytics.snap_orders_users_20240115
FROM s3_backup_repo
ON (
    orders AS orders_restored_20240115,    -- restore "orders" as "orders_restored_20240115"
    users  AS users_restored_20240115
)
PROPERTIES (
    "backup_timestamp" = "2024-01-15-10-00-00",
    "replication_num"  = "1",              -- staging: use fewer replicas
    "timeout"          = "7200"
);
```

### Restore Specific Partitions

```sql
-- Restore only selected partitions into an existing table
-- The target table must already exist with a compatible schema
RESTORE SNAPSHOT analytics.snap_orders_p202401_20240201
FROM s3_backup_repo
ON (
    orders PARTITION (p202401, p202312)
)
PROPERTIES (
    "backup_timestamp" = "2024-02-01-02-00-00",
    "replication_num"  = "3",
    "timeout"          = "3600"
);
```

**RESTORE syntax reference:**

```
RESTORE SNAPSHOT <db_name>.<snapshot_name>
FROM <repo_name>
[ ON ( <tbl_name> [ AS <new_tbl_name> ]
       [ PARTITION (<part1> [, <part2>...]) [ AS <new_part_name> ] ]
       [, ...] ) ]
[ PROPERTIES ("key" = "value" [, ...]) ]
```

| PROPERTIES key | Values | Default | Notes |
|---|---|---|---|
| `backup_timestamp` | `YYYY-MM-DD-HH-mm-ss` | (required) | Obtain from `SHOW SNAPSHOT ON <repo>` |
| `replication_num` | `"1"` / `"3"` | table default | Override replica count for the restored table |
| `timeout` | seconds (string) | `86400` | Job-level timeout |
| `meta_version` | `"x"` | auto | Rarely needed; set only when restoring from an older StarRocks version |

### Inspect Available Snapshots in a Repository

```sql
-- List all snapshots stored in a repository
SHOW SNAPSHOT ON s3_backup_repo;

-- Filter by snapshot name
SHOW SNAPSHOT ON s3_backup_repo WHERE SNAPSHOT = "snap_orders_p202401_20240201";

-- Output columns:
-- Snapshot                          | Timestamp           | Status  | Database  | Details
-- snap_orders_p202401_20240201     | 2024-02-01-02-00-00 | OK      | analytics | ...
```

### Monitor Restore Progress

```sql
SHOW RESTORE FROM analytics;

-- Status values: PENDING → DOWNLOADING → DOWNLOAD_SNAPSHOT → COMMITTING → FINISHED / CANCELLED
```

**Status values:**

| Status | Meaning |
|---|---|
| `PENDING` | Job queued |
| `DOWNLOADING` | BE nodes are downloading segment files from the repository |
| `DOWNLOAD_SNAPSHOT` | Snapshot metadata is being applied |
| `DIR_MOVE` | Tablet directories are being moved into place |
| `COMMITTING` | FE is committing the new tablet mapping into the catalog |
| `FINISHED` | Restore complete; table is queryable |
| `CANCELLED` | Job failed or timed out |

### Cancel a Running Restore

```sql
CANCEL RESTORE FROM analytics;
```

---

## 4. Incremental Backup Strategy

StarRocks does **not** support native incremental backup at the storage level. The recommended approach is **partition-level rotation**: back up only the newest (or recently modified) partitions instead of the entire table each time.

### Partition Rotation Pattern

```
Day 1:  BACKUP ON (orders PARTITION (p20240101))
Day 2:  BACKUP ON (orders PARTITION (p20240102))
...
Day 31: BACKUP ON (orders PARTITION (p20240131))
Week 1 end: BACKUP ON (orders) — full monthly snapshot
```

### Naming Convention for Rotation

Use a consistent snapshot name schema that embeds date and scope:

```
<db>.<table>_<scope>_<YYYYMMDD>
analytics.snap_orders_daily_p20240201
analytics.snap_orders_monthly_20240201
analytics.snap_full_db_20240101
```

### Snapshot Lifecycle on S3

Apply S3 lifecycle rules to auto-expire old snapshots:

```json
{
  "Rules": [
    {
      "ID": "expire-daily-backups",
      "Filter": { "Prefix": "starrocks-backups/__starrocks_repository_v2__/snap_orders_daily_" },
      "Status": "Enabled",
      "Expiration": { "Days": 14 }
    },
    {
      "ID": "expire-monthly-backups",
      "Filter": { "Prefix": "starrocks-backups/__starrocks_repository_v2__/snap_orders_monthly_" },
      "Status": "Enabled",
      "Expiration": { "Days": 90 }
    },
    {
      "ID": "retain-full-db-backups",
      "Filter": { "Prefix": "starrocks-backups/__starrocks_repository_v2__/snap_full_db_" },
      "Status": "Enabled",
      "Expiration": { "Days": 365 }
    }
  ]
}
```

> **Note:** `DROP REPOSITORY` removes only the FE registry entry; files on S3 must be deleted separately via S3 lifecycle policies or `aws s3 rm`.

---

## 5. Cross-Cluster Restore

Restoring a snapshot from a production cluster into a staging or DR cluster is supported as long as both clusters can reach the same repository.

### Prerequisites

1. **Same repository registration on the target cluster** — create the same `CREATE REPOSITORY` pointing to the same S3/HDFS path.
2. **Broker installed on all BE nodes of the target cluster** with network access to the repository.
3. **Compatible StarRocks version** — target version must be >= source version (downgrade restores are not supported).
4. **Database must exist on the target cluster** — `CREATE DATABASE` before running `RESTORE`.
5. **Sufficient storage** on target BE nodes: ensure free disk space ≥ snapshot size × `replication_num`.

### Step-by-Step: Production → Staging Copy

```sql
-- ===== On PRODUCTION cluster =====

-- 1. Create repository pointing to shared S3 bucket
CREATE REPOSITORY s3_shared_repo
WITH BROKER "broker_name"
ON LOCATION "s3://shared-backup-bucket/cross-cluster"
PROPERTIES (
    "aws.s3.access_key" = "AKIAIOSFODNN7EXAMPLE",
    "aws.s3.secret_key" = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "aws.s3.endpoint"   = "s3.us-east-1.amazonaws.com",
    "aws.s3.region"     = "us-east-1"
);

-- 2. Take a full snapshot of the target database
BACKUP SNAPSHOT analytics.snap_prod_analytics_20240115
TO s3_shared_repo
PROPERTIES ("type" = "full", "timeout" = "86400");

-- 3. Wait for FINISHED
SHOW BACKUP FROM analytics;

-- ===== On STAGING cluster =====

-- 4. Create the same repository registration
CREATE REPOSITORY s3_shared_repo
WITH BROKER "broker_name"
ON LOCATION "s3://shared-backup-bucket/cross-cluster"
PROPERTIES (
    "aws.s3.access_key" = "AKIAIOSFODNN7EXAMPLE",
    "aws.s3.secret_key" = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "aws.s3.endpoint"   = "s3.us-east-1.amazonaws.com",
    "aws.s3.region"     = "us-east-1"
);

-- 5. Verify the snapshot is visible
SHOW SNAPSHOT ON s3_shared_repo
WHERE SNAPSHOT = "snap_prod_analytics_20240115";

-- 6. Create target database if it does not exist
CREATE DATABASE IF NOT EXISTS analytics;

-- 7. Restore with reduced replication (staging usually has fewer BEs)
RESTORE SNAPSHOT analytics.snap_prod_analytics_20240115
FROM s3_shared_repo
PROPERTIES (
    "backup_timestamp" = "2024-01-15-10-00-00",
    "replication_num"  = "1",
    "timeout"          = "86400"
);

-- 8. Monitor restore
SHOW RESTORE FROM analytics;
```

### Network Requirements

| Source | Target | Required access |
|---|---|---|
| S3 bucket | Target BE nodes | Outbound HTTPS 443 to `s3.<region>.amazonaws.com` |
| MinIO | Target BE nodes | Outbound HTTP/HTTPS to MinIO endpoint |
| HDFS NameNode | Target BE nodes | TCP 9000 (RPC) + DataNode ports |

---

## 6. Backup Airflow DAG

A production-grade DAG that runs nightly partition backups and alerts on failure.

```python
"""
DAG: starrocks_nightly_backup
Schedule: 02:00 UTC daily
Purpose: Back up yesterday's partition of critical tables to S3.
         Poll until FINISHED; alert on failure or timeout.
"""
import pendulum
import time
import pymysql
from contextlib import contextmanager
from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

# ── connection helpers ────────────────────────────────────────────────────────

STARROCKS_CONN = {
    "host":   "starrocks-fe.internal",
    "port":   9030,          # MySQL protocol port
    "user":   "backup_user",
    "password": "{{ var.value.starrocks_backup_password }}",
    "database": "analytics",
    "connect_timeout": 30,
}

REPO_NAME     = "s3_backup_repo"
POLL_INTERVAL = 30    # seconds between SHOW BACKUP polls
MAX_WAIT_SEC  = 7200  # 2 h hard timeout


@contextmanager
def sr_conn():
    """Yield a pymysql connection to StarRocks FE."""
    conn = pymysql.connect(**STARROCKS_CONN)
    try:
        yield conn
    finally:
        conn.close()


def _execute(sql: str) -> list[dict]:
    with sr_conn() as conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall() or []


# ── DAG definition ────────────────────────────────────────────────────────────

@dag(
    dag_id="starrocks_nightly_backup",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 2 * * *",      # 02:00 UTC
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "owner": "data-platform",
    },
    tags=["starrocks", "backup", "dr"],
)
def starrocks_nightly_backup():

    @task()
    def compute_partition_name(ds: str = None) -> str:
        """Return the partition name for yesterday's date."""
        yesterday = pendulum.parse(ds).subtract(days=1)
        return f"p{yesterday.format('YYYYMMDD')}"   # e.g. p20240114

    @task()
    def run_backup(partition_name: str, ds: str = None) -> str:
        """Submit BACKUP job and return snapshot name."""
        date_nodash = ds.replace("-", "")            # 20240115
        snapshot = f"snap_orders_daily_{date_nodash}"
        db = STARROCKS_CONN["database"]

        sql = f"""
        BACKUP SNAPSHOT {db}.{snapshot}
        TO {REPO_NAME}
        ON (
            orders PARTITION ({partition_name}),
            events PARTITION ({partition_name})
        )
        PROPERTIES (
            "type"    = "full",
            "timeout" = "7200"
        )
        """
        _execute(sql)
        return snapshot

    @task()
    def wait_for_backup(snapshot_name: str) -> str:
        """Poll SHOW BACKUP until FINISHED or CANCELLED."""
        db = STARROCKS_CONN["database"]
        elapsed = 0

        while elapsed < MAX_WAIT_SEC:
            rows = _execute(f"SHOW BACKUP FROM `{db}`")
            if not rows:
                time.sleep(POLL_INTERVAL)
                elapsed += POLL_INTERVAL
                continue

            # Find our snapshot (may have multiple historical rows)
            row = next(
                (r for r in rows if r.get("SnapshotName") == snapshot_name),
                None
            )
            if row is None:
                time.sleep(POLL_INTERVAL)
                elapsed += POLL_INTERVAL
                continue

            state = row.get("State", "")
            if state == "FINISHED":
                return snapshot_name
            if state == "CANCELLED":
                err = row.get("Status", "unknown error")
                raise RuntimeError(f"BACKUP {snapshot_name} CANCELLED: {err}")

            # Still running — PENDING / SNAPSHOTING / UPLOADING / etc.
            time.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL

        raise TimeoutError(
            f"BACKUP {snapshot_name} did not finish within {MAX_WAIT_SEC}s"
        )

    @task()
    def verify_snapshot(snapshot_name: str) -> None:
        """Confirm snapshot appears in SHOW SNAPSHOT ON repo."""
        rows = _execute(
            f"SHOW SNAPSHOT ON {REPO_NAME} WHERE SNAPSHOT = '{snapshot_name}'"
        )
        if not rows:
            raise ValueError(
                f"Snapshot {snapshot_name} not found in {REPO_NAME} after backup"
            )

    # ── task wiring ───────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    partition   = compute_partition_name()
    snapshot    = run_backup(partition)
    finished    = wait_for_backup(snapshot)
    start >> partition >> snapshot >> finished >> verify_snapshot(finished) >> end


starrocks_nightly_backup()
```

### Monthly Full-Database Backup DAG (separate schedule)

```python
@dag(
    dag_id="starrocks_monthly_full_backup",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 3 1 * *",    # 03:00 UTC on the 1st of each month
    catchup=False,
    max_active_runs=1,
    tags=["starrocks", "backup", "dr"],
)
def starrocks_monthly_full_backup():

    @task()
    def run_full_backup(ds: str = None) -> str:
        date_nodash = ds.replace("-", "")
        snapshot = f"snap_full_analytics_{date_nodash}"
        db = "analytics"
        sql = f"""
        BACKUP SNAPSHOT {db}.{snapshot}
        TO {REPO_NAME}
        PROPERTIES ("type" = "full", "timeout" = "86400")
        """
        _execute(sql)
        return snapshot

    @task()
    def wait_for_backup(snapshot_name: str) -> str:
        # Same polling logic as nightly DAG (factor into a shared module)
        db = "analytics"
        elapsed = 0
        while elapsed < 86400:
            rows = _execute(f"SHOW BACKUP FROM `{db}`")
            row  = next((r for r in rows if r.get("SnapshotName") == snapshot_name), None)
            if row:
                if row["State"] == "FINISHED":
                    return snapshot_name
                if row["State"] == "CANCELLED":
                    raise RuntimeError(f"BACKUP CANCELLED: {row.get('Status')}")
            time.sleep(60)
            elapsed += 60
        raise TimeoutError("Monthly backup timed out after 24h")

    snap = run_full_backup()
    wait_for_backup(snap)


starrocks_monthly_full_backup()
```

---

## 7. RTO/RPO Design

### Table Criticality Tiers

| Tier | Description | Examples | RPO Target | RTO Target |
|---|---|---|---|---|
| **T1 — Critical** | Revenue-impacting; real-time dashboards; SLA-bound | `orders`, `payments`, `sessions` | ≤ 1 hour | ≤ 2 hours |
| **T2 — Important** | Operational reporting; pipeline outputs | `user_profiles`, `product_catalog` | ≤ 24 hours | ≤ 4 hours |
| **T3 — Standard** | Analytical aggregates; derived tables | `daily_kpis`, `cohort_analysis` | ≤ 7 days | ≤ 8 hours |
| **T4 — Archival** | Historical, rarely queried | `raw_events_2020` | ≤ 30 days | ≤ 24 hours |

### Backup Frequency Matrix

| Tier | Partition Backup | Full Table Backup | Retention (S3) |
|---|---|---|---|
| T1 | Every 1 h (last partition) | Daily | Daily: 14 days; Monthly: 12 months |
| T2 | Daily (yesterday's partition) | Weekly | Daily: 7 days; Monthly: 6 months |
| T3 | Weekly | Monthly | Monthly: 3 months |
| T4 | Monthly | Quarterly | 12 months |

### Restore Testing Schedule

Untested backups are not backups. Run restore drills on a schedule:

| Frequency | Action |
|---|---|
| Weekly | Restore a T1 partition to a staging table; run row-count check |
| Monthly | Restore a full T1 table to staging; run diff against production |
| Quarterly | Full DR rehearsal: restore entire database to isolated cluster; validate queries |

```sql
-- Weekly restore drill (run on staging cluster)
RESTORE SNAPSHOT analytics.snap_orders_daily_20240201
FROM s3_backup_repo
ON (orders AS orders_dr_test)
PROPERTIES (
    "backup_timestamp" = "2024-02-01-02-00-00",
    "replication_num"  = "1",
    "timeout"          = "3600"
);

-- Validation query after restore
SELECT COUNT(*), MIN(created_at), MAX(created_at)
FROM orders_dr_test;

-- Compare against production (run on prod)
SELECT COUNT(*), MIN(created_at), MAX(created_at)
FROM orders
WHERE dt = '2024-02-01';

-- Cleanup after drill
DROP TABLE IF EXISTS analytics.orders_dr_test;
```

---

## 8. Snapshot Lifecycle Management

### Naming Convention

```
<db>.<scope>_<table|"full">_<date>[_<extra>]
```

Examples:
```
analytics.snap_orders_daily_p20240201          -- daily partition backup
analytics.snap_orders_weekly_20240204          -- weekly table backup
analytics.snap_full_db_20240101               -- monthly full-database backup
analytics.snap_orders_pre_migration_20240301  -- one-off pre-migration snapshot
```

### Listing and Auditing Snapshots

```sql
-- All snapshots in a repository
SHOW SNAPSHOT ON s3_backup_repo;

-- Filter by prefix pattern (use LIKE in newer SR versions)
SHOW SNAPSHOT ON s3_backup_repo WHERE SNAPSHOT LIKE "snap_orders_%";
```

### S3 Object Layout

StarRocks stores snapshots under a fixed prefix inside the repository root:

```
s3://my-backup-bucket/starrocks-backups/
  __starrocks_repository_v2__/
    snap_orders_daily_p20240201_20240201020015/     ← snapshot_name + timestamp
      meta/
        image.xxxxxx                                ← FE metadata (schema, partition map)
      tablet_10001/
        10001_10002_xxxxxxxx.dat                    ← segment data files
        10001_10002_xxxxxxxx.idx
      tablet_10003/
        ...
```

> All files within a snapshot are immutable after BACKUP finishes. Deleting individual files inside a snapshot directory will corrupt the snapshot and make it unrestorable.

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Backing up the entire cluster database daily without partition scoping | Unnecessary I/O and S3 cost; long backup windows block new backup jobs | Use partition-level backup for T1 tables; schedule full backups weekly/monthly |
| Using the same snapshot name repeatedly | New backup silently overwrites the previous snapshot in the repository | Always embed a datestamp in the snapshot name |
| Not polling `SHOW BACKUP` before running `RESTORE` | RESTORE submitted on a snapshot still uploading will fail | Always wait for `State = FINISHED` before attempting a restore |
| Restoring directly into a production table (same name, same cluster) | Active queries on the table are disrupted during COMMITTING phase; no rollback path | Restore into `<table>_restored_<date>`, validate, then swap with `ALTER TABLE RENAME` |
| No S3 lifecycle rules | Old snapshots accumulate indefinitely; unbounded storage cost | Apply Expiration lifecycle rules per prefix/tier immediately after creating the repository |
| Backing up views or materialized views | BACKUP silently skips them with no error | Separately version-control all DDL in git; redeploy views after restoring base tables |
| Cross-version downgrade restore (newer → older StarRocks) | Meta format incompatibility; restore fails with corrupted snapshot error | Always restore to a cluster version >= the backup source version |
| Running BACKUP during peak ingestion hours | BACKUP takes a snapshot of tablets; heavy concurrent writes increase snapshot size and duration | Schedule backups during off-peak hours (typically 01:00–05:00 local) |
| No restore drills | Backup completeness is unverified; recovery SLA is fiction | Run weekly automated restore checks with row-count validation |
| Dropping repository before deleting S3 objects | S3 objects are orphaned (no StarRocks reference, not cleaned up automatically) | Delete S3 objects first (or set lifecycle rules), then `DROP REPOSITORY` |

---

## References to Consult When Needed

- [StarRocks Backup and Restore — Official Guide](https://docs.starrocks.io/docs/administration/management/backup_recovery/backup_and_restore/)
- [BACKUP SQL Reference](https://docs.starrocks.io/docs/sql-reference/sql-statements/backup_restore/BACKUP/)
- [RESTORE SQL Reference](https://docs.starrocks.io/docs/sql-reference/sql-statements/backup_restore/RESTORE/)
- [CREATE REPOSITORY SQL Reference](https://docs.starrocks.io/docs/sql-reference/sql-statements/backup_restore/CREATE_REPOSITORY/)
- [SHOW BACKUP SQL Reference](https://docs.starrocks.io/docs/sql-reference/sql-statements/backup_restore/SHOW_BACKUP/)
- [SHOW RESTORE SQL Reference](https://docs.starrocks.io/docs/sql-reference/sql-statements/backup_restore/SHOW_RESTORE/)
- [SHOW SNAPSHOT SQL Reference](https://docs.starrocks.io/docs/sql-reference/sql-statements/backup_restore/SHOW_SNAPSHOT/)
- [StarRocks Broker Configuration](https://docs.starrocks.io/docs/deployment/deploy_broker/)
- [S3 Lifecycle Rules — AWS Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
