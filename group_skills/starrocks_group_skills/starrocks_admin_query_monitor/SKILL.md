---
name: starrocks-admin-query-monitor
description: StarRocks query monitoring and resource management — slow query log, SHOW PROCESSLIST, information_schema.query_history, resource groups (CREATE RESOURCE GROUP), classifiers, CPU/memory/concurrency limits, query queuing, KILL QUERY, workload isolation, Audit Loader plugin
---

# StarRocks — Query Monitoring and Resource Management

## When to Use

Load this skill when the user needs to:
- Diagnose high query latency or unexpectedly slow queries in StarRocks
- Investigate memory OOM errors or BE crashes caused by runaway queries
- Isolate resource contention between teams (BI analysts vs. ETL pipelines)
- Set up workload isolation so dashboard queries are never starved by bulk loads
- Kill a stuck or runaway query
- Configure slow query logging and parse slow query log entries
- Build long-term query audit infrastructure using the Audit Loader plugin
- Write SQL dashboards for query performance statistics (p95/p99, top-N slowest)

---

## Real-Time Query Monitoring

### SHOW PROCESSLIST

Returns one row per active query on the FE the client is connected to.

```sql
SHOW PROCESSLIST;
```

| Column | Description |
|---|---|
| `Id` | Connection (session) ID — used in `KILL` |
| `User` | Database user running the query |
| `Host` | Client IP and port |
| `Db` | Current default database |
| `Command` | Command type: `Query`, `Sleep`, `Killed` |
| `Time` | Seconds since the command started |
| `State` | Execution phase, e.g. `PLANNING`, `RUNNING`, `PENDING` |
| `Info` | SQL text (truncated to 100 chars in short form) |

### SHOW FULL PROCESSLIST

Returns the full, untruncated SQL text in the `Info` column. Use this when `SHOW PROCESSLIST` shows a truncated statement.

```sql
SHOW FULL PROCESSLIST;
```

Useful for identifying the exact query behind a suspicious long-running connection.

### Filter Long-Running Queries Inline

```sql
-- Sessions running longer than 60 seconds
SELECT Id, User, Host, Db, Time, State, Info
FROM information_schema.processlist
WHERE Command = 'Query'
  AND Time > 60
ORDER BY Time DESC;
```

`information_schema.processlist` is the table-form equivalent of `SHOW PROCESSLIST` and supports arbitrary predicates.

### Kill a Runaway Query

```sql
-- Kill by connection ID (obtained from SHOW PROCESSLIST or information_schema.processlist)
KILL QUERY <connection_id>;

-- Example
KILL QUERY 1042;
```

`KILL QUERY` signals the FE to abort the query and release BE resources. The connection itself stays alive. To drop the connection entirely use `KILL CONNECTION <connection_id>`.

Only the query owner or a user with `SYSTEM`-level admin privilege can kill a query.

---

## Query History

### information_schema.query_history

`information_schema.query_history` stores a rolling in-memory record of recently completed queries (default retention: last 100 queries per FE; configurable via `query_detail_num`). For long-term retention use the Audit Loader plugin (see section below).

```sql
DESC information_schema.query_history;
```

Key columns:

| Column | Type | Description |
|---|---|---|
| `QueryId` | VARCHAR | Globally unique query identifier |
| `StartTime` | DATETIME | Wall-clock time when the query was submitted |
| `EndTime` | DATETIME | Wall-clock time when the query finished |
| `TotalTime` | BIGINT | End-to-end elapsed time in **milliseconds** |
| `QueryCpuCost` | BIGINT | Cumulative CPU time across all BE threads (ms) |
| `QueryMemCost` | BIGINT | Peak memory used by the query (bytes) |
| `State` | VARCHAR | `EOF` (success), `ERR` (error), `RUNNING` |
| `Stmt` | VARCHAR | Full SQL text |
| `User` | VARCHAR | Database user |
| `DefaultDb` | VARCHAR | Default database at query time |
| `WareHouse` | VARCHAR | Warehouse / resource group name (if applicable) |
| `ReturnRows` | BIGINT | Number of result rows returned to the client |
| `ScanRows` | BIGINT | Total rows scanned across all BE tablets |
| `ScanBytes` | BIGINT | Total bytes scanned |
| `CpuCostSeconds` | DOUBLE | CPU cost in seconds (alias for `QueryCpuCost / 1000`) |

> Column availability varies slightly by StarRocks version (3.x adds warehouse columns). Always run `DESC information_schema.query_history` on the target cluster to confirm.

### Find Slow Queries (threshold-based)

```sql
-- Queries slower than 10 seconds in the last 24 hours
SELECT
    QueryId,
    StartTime,
    TotalTime / 1000.0          AS total_sec,
    QueryCpuCost / 1000.0       AS cpu_sec,
    QueryMemCost / 1073741824.0 AS mem_gb,
    ReturnRows,
    ScanRows,
    User,
    LEFT(Stmt, 300)             AS stmt_preview
FROM information_schema.query_history
WHERE State = 'EOF'
  AND StartTime >= NOW() - INTERVAL 1 DAY
  AND TotalTime > 10000            -- 10 000 ms = 10 s
ORDER BY TotalTime DESC
LIMIT 50;
```

### Top CPU Consumers

```sql
SELECT
    User,
    COUNT(*)                        AS query_count,
    SUM(QueryCpuCost) / 1000.0      AS total_cpu_sec,
    AVG(QueryCpuCost) / 1000.0      AS avg_cpu_sec,
    MAX(QueryCpuCost) / 1000.0      AS max_cpu_sec,
    SUM(ScanRows)                   AS total_scan_rows
FROM information_schema.query_history
WHERE State = 'EOF'
  AND StartTime >= NOW() - INTERVAL 1 HOUR
GROUP BY User
ORDER BY total_cpu_sec DESC
LIMIT 20;
```

### Top Memory Consumers

```sql
SELECT
    QueryId,
    User,
    StartTime,
    QueryMemCost / 1073741824.0 AS peak_mem_gb,
    TotalTime / 1000.0          AS total_sec,
    LEFT(Stmt, 200)             AS stmt_preview
FROM information_schema.query_history
WHERE State = 'EOF'
  AND StartTime >= NOW() - INTERVAL 1 HOUR
ORDER BY QueryMemCost DESC
LIMIT 20;
```

### Failed Query Analysis

```sql
-- All errors in the last 6 hours
SELECT
    QueryId,
    StartTime,
    TotalTime / 1000.0 AS total_sec,
    User,
    DefaultDb,
    LEFT(Stmt, 400)    AS stmt_preview
FROM information_schema.query_history
WHERE State = 'ERR'
  AND StartTime >= NOW() - INTERVAL 6 HOUR
ORDER BY StartTime DESC;
```

---

## Slow Query Log

### FE Configuration

The slow query threshold is controlled by the FE parameter `qe_slow_log_ms` (milliseconds). Queries exceeding this threshold are written to the FE audit log.

```sql
-- Check current threshold (dynamic, no restart required)
ADMIN SHOW FRONTEND CONFIG LIKE 'qe_slow_log_ms';

-- Raise threshold to 5 seconds at runtime
ADMIN SET FRONTEND CONFIG ("qe_slow_log_ms" = "5000");

-- Persist in fe.conf (requires FE restart to take effect on fresh startup)
-- qe_slow_log_ms = 5000
```

Default: `5000` ms (5 seconds).

### Log File Location

```
$STARROCKS_HOME/log/fe.audit.log
```

The audit log is a structured text file. Each slow-query entry contains a pipe-delimited record.

### Log Entry Format

```
2024-08-15 14:32:01,104 [slow_query] |Client=192.168.1.55:48312|User=analyst|Db=dwh|State=EOF|Time=12543|ScanBytes=2147483648|ScanRows=85000000|ReturnRows=1|CpuTime=34201|SqlHash=a3f9c2d1|ExecTime=12543|Stmt=SELECT ...
```

Key fields:
- `Time` — total elapsed ms
- `CpuTime` — BE CPU ms
- `ScanBytes` / `ScanRows` — data volume
- `SqlHash` — repeatable hash of the normalized SQL (useful for grouping)
- `Stmt` — full SQL text

### Parsing with grep/awk

```bash
# Show all slow queries from today sorted by Time descending
grep 'slow_query' $STARROCKS_HOME/log/fe.audit.log \
  | grep "$(date +%Y-%m-%d)" \
  | awk -F'|' '{
      for(i=1;i<=NF;i++){
          if($i ~ /^Time=/){ split($i,a,"="); time=a[2] }
          if($i ~ /^User=/){ split($i,a,"="); user=a[2] }
          if($i ~ /^Stmt=/){ stmt=substr($i,6,120) }
      }
      print time"\t"user"\t"stmt
  }' \
  | sort -rn \
  | head -20
```

---

## Resource Groups

Resource groups are the primary mechanism for workload isolation in StarRocks. Each resource group caps CPU, memory, and concurrency for the queries assigned to it. Assignment is done via **classifiers** that match query attributes at execution time.

### CREATE RESOURCE GROUP

```sql
CREATE RESOURCE GROUP <group_name>
TO
    <classifier1>,
    <classifier2>, ...   -- at least one classifier required
WITH (
    "cpu_core_limit"             = "<n>",       -- soft CPU cores (integer)
    "mem_limit"                  = "<pct>%",    -- % of BE total memory
    "concurrency_limit"          = "<n>",       -- max concurrent queries
    "big_query_cpu_second_limit" = "<n>",       -- CPU seconds before query is killed (0 = disabled)
    "big_query_scan_rows_limit"  = "<n>",       -- scan rows before query is killed (0 = disabled)
    "big_query_mem_limit"        = "<bytes>",   -- memory bytes before query is killed (0 = disabled)
    "type"                       = "normal"     -- "normal" (default) or "short_query"
);
```

**Parameter reference:**

| Parameter | Unit | Effect |
|---|---|---|
| `cpu_core_limit` | integer (logical cores) | Soft cap on CPU threads; proportional scheduling under contention |
| `mem_limit` | percentage string, e.g. `"30%"` | Maximum fraction of BE process memory this group may use |
| `concurrency_limit` | integer | Maximum simultaneously executing queries in this group; excess queries queue |
| `big_query_cpu_second_limit` | integer (CPU-seconds) | Accumulated CPU time limit per query; query is killed when exceeded |
| `big_query_scan_rows_limit` | integer (rows) | Row scan limit per query |
| `big_query_mem_limit` | integer (bytes) | Peak memory limit per query |
| `type` | `normal` / `short_query` | `short_query` groups get priority access to CPU when the cluster is lightly loaded |

### Classifier Syntax

Classifiers are inline sub-expressions inside the `TO` clause. Each field is optional; multiple fields are ANDed together. The most specific matching classifier wins (highest specificity score).

```sql
-- Classifier fields (all optional, ANDed):
(
    user    = "<db_user>",               -- exact match on database username
    role    = "<db_role>",               -- exact match on active role
    query_type IN ("SELECT", "INSERT"),  -- filter by statement type
    db      = "<database_name>",         -- current default database
    source_ip = "<cidr>",                -- client IP range, e.g. "192.168.10.0/24"
    plan_cpu_cost_range = "[<lo>, <hi>)",-- optimizer-estimated CPU cost range
    plan_mem_cost_range = "[<lo>, <hi>)" -- optimizer-estimated memory cost range
)
```

`query_type` accepted values: `SELECT`, `INSERT`, `CTAS`, `UPDATE`, `DELETE`, `LOAD`, `EXPORT`.

### Specificity Scoring

When a query matches multiple classifiers, StarRocks picks the one with the highest specificity score. Each matched field contributes a weight:

- `user` → 8
- `role` → 4
- `query_type` → 2
- `db` → 1
- `source_ip` → 0 (tie-break only, uses prefix length)
- `plan_cpu_cost_range`, `plan_mem_cost_range` → 0

### ALTER RESOURCE GROUP

```sql
-- Add a new classifier to an existing group
ALTER RESOURCE GROUP etl_rg
ADD
    (user = 'spark_loader', query_type IN ('INSERT', 'LOAD'));

-- Remove a classifier by its system-assigned ID
-- First find classifier IDs:
SHOW RESOURCE GROUP etl_rg;
-- Then drop by ID:
ALTER RESOURCE GROUP etl_rg
DROP (id1, id2);

-- Modify resource parameters (no classifier change)
ALTER RESOURCE GROUP etl_rg
WITH ("mem_limit" = "50%", "concurrency_limit" = "4");
```

### DROP RESOURCE GROUP

```sql
DROP RESOURCE GROUP etl_rg;
```

A group cannot be dropped if queries are currently using it; kill active queries first.

### SHOW RESOURCE GROUPS

```sql
-- All groups visible to the current user
SHOW RESOURCE GROUPS;

-- Specific group with its classifiers
SHOW RESOURCE GROUP etl_rg;

-- All groups (requires admin privilege)
SHOW ALL RESOURCE GROUPS;
```

Output columns: `Name`, `Id`, `CPUCoreLimit`, `MemLimit`, `ConcurrencyLimit`, `BigQueryCpuSecondLimit`, `BigQueryScanRowsLimit`, `BigQueryMemLimit`, `Type`, `Classifiers`.

---

## Workload Isolation Patterns

### Design Principles

| Workload | Characteristics | Resource Group Settings |
|---|---|---|
| BI dashboards | Short queries (<5 s), high concurrency (50–200 QPS), low memory | High `concurrency_limit`, moderate `mem_limit`, `type = short_query` |
| Ad-hoc analytics | Medium duration (5–120 s), medium concurrency, large scans | Medium `concurrency_limit`, higher `mem_limit` |
| ETL / bulk insert | Long-running (minutes to hours), low concurrency, heavy CPU | Low `concurrency_limit`, high `mem_limit`, `big_query_*` limits as safety net |

### 3-Tier Setup: dashboard_rg / adhoc_rg / etl_rg

```sql
-- =====================================================================
-- Tier 1 — BI dashboards: fast, high-concurrency, priority scheduling
-- =====================================================================
CREATE RESOURCE GROUP dashboard_rg
TO
    (user = 'bi_user',    query_type IN ('SELECT')),
    (role = 'bi_role',    query_type IN ('SELECT')),
    (source_ip = '10.10.5.0/24')   -- Superset / Metabase servers
WITH (
    "cpu_core_limit"             = "8",
    "mem_limit"                  = "20%",
    "concurrency_limit"          = "40",
    "big_query_cpu_second_limit" = "60",    -- kill if > 60 CPU-seconds
    "big_query_scan_rows_limit"  = "500000000",
    "big_query_mem_limit"        = "2147483648",  -- 2 GB
    "type"                       = "short_query"
);

-- =====================================================================
-- Tier 2 — Ad-hoc / interactive analytics
-- =====================================================================
CREATE RESOURCE GROUP adhoc_rg
TO
    (user = 'analyst',  query_type IN ('SELECT')),
    (role = 'analyst_role')
WITH (
    "cpu_core_limit"             = "12",
    "mem_limit"                  = "30%",
    "concurrency_limit"          = "10",
    "big_query_cpu_second_limit" = "300",
    "big_query_scan_rows_limit"  = "2000000000",
    "big_query_mem_limit"        = "8589934592",  -- 8 GB
    "type"                       = "normal"
);

-- =====================================================================
-- Tier 3 — ETL / bulk loads: low concurrency, large memory
-- =====================================================================
CREATE RESOURCE GROUP etl_rg
TO
    (user = 'etl_user',  query_type IN ('INSERT', 'LOAD', 'CTAS')),
    (role = 'etl_role')
WITH (
    "cpu_core_limit"             = "16",
    "mem_limit"                  = "50%",
    "concurrency_limit"          = "4",
    "big_query_cpu_second_limit" = "0",           -- no CPU kill for ETL
    "big_query_scan_rows_limit"  = "0",
    "big_query_mem_limit"        = "0",
    "type"                       = "normal"
);
```

### Assigning Users to Resource Groups via Classifiers

Classifiers are embedded in the `CREATE / ALTER RESOURCE GROUP` statement — there is no separate `ASSIGN` command. Assignment happens at query runtime: StarRocks evaluates all classifiers and picks the best match.

```sql
-- Add a new team to an existing resource group
ALTER RESOURCE GROUP adhoc_rg
ADD
    (user = 'data_scientist', query_type IN ('SELECT'));

-- Route queries from a specific database to ETL group
ALTER RESOURCE GROUP etl_rg
ADD
    (db = 'staging', query_type IN ('INSERT', 'CTAS'));
```

To verify which resource group a query was assigned to, check the `WareHouse` / `ResourceGroup` column in `information_schema.query_history` or in the FE audit log.

---

## Query Queuing

When a resource group's `concurrency_limit` is reached, new queries enter a queue rather than failing immediately. Queue behaviour is controlled by FE session/global variables.

### Global Query Queue Parameters

Set via `SET GLOBAL` or in `fe.conf`:

```sql
-- Enable query queuing globally (default: false in older versions, true in 3.x)
SET GLOBAL enable_query_queue_select = true;
SET GLOBAL enable_query_queue_load   = true;    -- for load operations

-- Maximum number of queries waiting in queue per resource group
SET GLOBAL query_queue_max_queued_queries = 100;

-- How long (seconds) a queued query waits before returning an error to the client
SET GLOBAL query_queue_pending_timeout_second = 300;

-- Concurrency threshold below which queries bypass the queue
-- (protects the global queue from being enabled unnecessarily)
SET GLOBAL query_queue_concurrency_limit = 0;   -- 0 means use resource group limit
```

`ADMIN SHOW FRONTEND CONFIG LIKE 'query_queue%';` shows all related parameters.

### Monitoring Queue Depth

```sql
-- Active + queued queries per resource group (3.1+)
SELECT
    resource_group,
    COUNT(*) FILTER (WHERE state = 'RUNNING') AS running,
    COUNT(*) FILTER (WHERE state = 'PENDING') AS queued
FROM information_schema.processlist
GROUP BY resource_group;
```

### Session-Level Override

```sql
-- Disable queuing for a specific session (e.g., urgent operational query)
SET enable_query_queue_select = false;
```

---

## Memory Spill

When a query's intermediate result set (hash join, aggregation, sort) exceeds available memory, StarRocks can spill to local disk rather than OOM-killing the query.

### Key Parameters

```sql
-- Enable spill for the current session
SET spill_mode = 'auto';       -- 'auto' | 'force' | 'off' (default 'off' pre-3.1, 'auto' in 3.2+)

-- Memory threshold per query that triggers spill (bytes; 0 = use mem_limit)
SET query_mem_limit = 4294967296;   -- 4 GB

-- Hash table size threshold for spill (bytes)
-- If the hash table of an aggregation/join exceeds this, it spills.
-- Configured at BE level in be.conf:
-- spill_mem_table_size = 134217728   (128 MB default)

-- Maximum disk space a single query may use for spill (bytes, BE-level)
-- spill_storage_volume = <path>   (configure in be.conf)
```

### Checking Spill Activity

```sql
-- Queries that spilled in the last hour
SELECT
    QueryId,
    User,
    StartTime,
    TotalTime / 1000.0          AS total_sec,
    QueryMemCost / 1073741824.0 AS peak_mem_gb,
    LEFT(Stmt, 200)             AS stmt_preview
FROM information_schema.query_history
WHERE StartTime >= NOW() - INTERVAL 1 HOUR
  AND QueryMemCost > 4294967296   -- > 4 GB peak memory suggests potential spill
ORDER BY QueryMemCost DESC
LIMIT 20;
```

Spill metrics are also available in BE logs (`be.INFO`) under the keyword `spill`.

---

## Audit Loader Plugin

`information_schema.query_history` is in-memory and holds only the most recent N queries. For long-term retention, compliance, and historical trending, use the **AuditLoader** plugin, which streams FE audit log entries into a StarRocks table.

### Install the Plugin

```sql
-- Plugin is shipped with StarRocks under fe/plugins/auditloader/
INSTALL PLUGIN FROM "/opt/starrocks/fe/plugins/AuditLoader.zip";

-- Verify installation
SHOW PLUGINS;
-- Look for: AuditLoader  |  AUDIT  |  INSTALLED
```

### Create the Audit Target Table

```sql
CREATE DATABASE IF NOT EXISTS audit_db;

CREATE TABLE IF NOT EXISTS audit_db.starrocks_audit_log (
    query_id        VARCHAR(64)     NOT NULL,
    `timestamp`     DATETIME        NOT NULL,
    client_ip       VARCHAR(32),
    user            VARCHAR(64),
    db              VARCHAR(96),
    state           VARCHAR(8),
    error_code      INT,
    error_message   VARCHAR(512),
    query_time      BIGINT          COMMENT 'ms',
    scan_bytes      BIGINT,
    scan_rows       BIGINT,
    return_rows     BIGINT,
    cpu_cost_ns     BIGINT,
    mem_cost_bytes  BIGINT,
    stmt_id         INT,
    is_query        TINYINT,
    frontend_ip     VARCHAR(32),
    cpu_total_time  BIGINT,
    sql_hash        VARCHAR(64),
    sql_digest      VARCHAR(64),
    peak_memory     BIGINT,
    stmt            VARCHAR(65533)  COMMENT 'full SQL'
)
ENGINE = OLAP
DUPLICATE KEY(query_id, `timestamp`)
PARTITION BY RANGE(`timestamp`) (
    START ("2024-01-01") END ("2026-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(query_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "dynamic_partition.enable"       = "true",
    "dynamic_partition.time_unit"    = "MONTH",
    "dynamic_partition.start"        = "-3",
    "dynamic_partition.end"          = "1",
    "dynamic_partition.prefix"       = "p",
    "dynamic_partition.buckets"      = "8"
);
```

### Configure the Plugin

Edit or create `$STARROCKS_HOME/conf/audit_loader.conf` (exact path depends on plugin version):

```ini
# Target FE address for stream load
frontend_host  = 127.0.0.1
frontend_port  = 8030

# Target table
database = audit_db
table    = starrocks_audit_log

# Stream load batch settings
max_batch_size        = 50MB
max_batch_interval_s  = 60

# Credentials (use a dedicated low-privilege user)
user     = audit_writer
password = <password>
```

### Reload / Uninstall Plugin

```sql
-- Reload after config change (no restart needed)
UNINSTALL PLUGIN AuditLoader;
INSTALL PLUGIN FROM "/opt/starrocks/fe/plugins/AuditLoader.zip";
```

---

## Dashboard Queries

### Daily Query Statistics

```sql
SELECT
    DATE(StartTime)                             AS query_date,
    COUNT(*)                                    AS total_queries,
    COUNT(*) FILTER (WHERE State = 'ERR')       AS failed_queries,
    ROUND(100.0 * COUNT(*) FILTER (WHERE State = 'ERR') / COUNT(*), 2) AS error_rate_pct,
    ROUND(AVG(TotalTime) / 1000.0, 2)           AS avg_sec,
    ROUND(MAX(TotalTime) / 1000.0, 2)           AS max_sec,
    ROUND(SUM(ScanRows) / 1e9, 2)               AS total_scan_rows_bn,
    ROUND(SUM(ScanBytes) / 1e12, 3)             AS total_scan_tb
FROM audit_db.starrocks_audit_log
WHERE `timestamp` >= CURDATE() - INTERVAL 30 DAY
  AND is_query = 1
GROUP BY DATE(StartTime)
ORDER BY query_date DESC;
```

### P95 / P99 Latency by User

```sql
SELECT
    user,
    COUNT(*)                                              AS queries,
    ROUND(PERCENTILE_APPROX(query_time, 0.50) / 1000, 2) AS p50_sec,
    ROUND(PERCENTILE_APPROX(query_time, 0.95) / 1000, 2) AS p95_sec,
    ROUND(PERCENTILE_APPROX(query_time, 0.99) / 1000, 2) AS p99_sec,
    ROUND(MAX(query_time) / 1000, 2)                      AS max_sec
FROM audit_db.starrocks_audit_log
WHERE `timestamp` >= NOW() - INTERVAL 1 DAY
  AND state = 'EOF'
  AND is_query = 1
GROUP BY user
ORDER BY p99_sec DESC;
```

### Top-N Slowest Queries (last 7 days)

```sql
SELECT
    query_id,
    `timestamp`,
    user,
    db,
    ROUND(query_time / 1000.0, 2)         AS total_sec,
    ROUND(mem_cost_bytes / 1073741824.0, 2) AS peak_mem_gb,
    scan_rows,
    ROUND(scan_bytes / 1073741824.0, 2)   AS scan_gb,
    LEFT(stmt, 300)                       AS stmt_preview
FROM audit_db.starrocks_audit_log
WHERE `timestamp` >= NOW() - INTERVAL 7 DAY
  AND state = 'EOF'
  AND is_query = 1
ORDER BY query_time DESC
LIMIT 20;
```

### Hourly Query Throughput (last 24 hours)

```sql
SELECT
    DATE_FORMAT(`timestamp`, '%Y-%m-%d %H:00') AS hour_bucket,
    COUNT(*)                                    AS query_count,
    COUNT(*) FILTER (WHERE state = 'ERR')       AS errors,
    ROUND(AVG(query_time) / 1000, 2)            AS avg_sec,
    ROUND(PERCENTILE_APPROX(query_time, 0.95) / 1000, 2) AS p95_sec
FROM audit_db.starrocks_audit_log
WHERE `timestamp` >= NOW() - INTERVAL 24 HOUR
  AND is_query = 1
GROUP BY hour_bucket
ORDER BY hour_bucket;
```

### Identify Repeated Slow SQL (by hash)

```sql
-- Group identical query shapes by sql_hash to find recurring hotspots
SELECT
    sql_hash,
    COUNT(*)                                              AS executions,
    ROUND(AVG(query_time) / 1000, 2)                     AS avg_sec,
    ROUND(MAX(query_time) / 1000, 2)                     AS max_sec,
    ROUND(PERCENTILE_APPROX(query_time, 0.95) / 1000, 2) AS p95_sec,
    SUM(scan_rows)                                        AS total_scan_rows,
    ANY_VALUE(LEFT(stmt, 300))                            AS sample_stmt
FROM audit_db.starrocks_audit_log
WHERE `timestamp` >= NOW() - INTERVAL 7 DAY
  AND state = 'EOF'
  AND query_time > 5000     -- only queries > 5 s
GROUP BY sql_hash
ORDER BY executions * avg_sec DESC   -- sort by total impact
LIMIT 20;
```

---

## Anti-Patterns

**Setting `mem_limit = "100%"` on a resource group** — this leaves no memory for BE internal operations (compaction, schema change) and will cause OOM. Keep the sum of all resource group `mem_limit` values at or below 80–85% of BE memory.

**Using only a single `default_wg` group for all workloads** — without isolation, a single large ETL query can saturate CPU and stall BI dashboards for minutes. Always create separate groups for interactive and batch workloads.

**Relying on `information_schema.query_history` for audit** — it is in-memory, limited to the last N queries per FE, and lost on FE restart. Deploy the Audit Loader plugin for any retention requirement beyond a few minutes.

**Setting `big_query_cpu_second_limit` on ETL groups** — ETL jobs legitimately consume large amounts of CPU. Setting a CPU kill limit on ETL resource groups will cause random job failures. Reserve `big_query_*` limits for interactive groups where runaway queries are a real risk.

**Not setting `concurrency_limit` on ETL groups** — without a concurrency cap, a large number of parallel loads can overwhelm BE disks and tablet compaction. Even ETL groups should have a sensible cap (4–8 concurrent queries for most clusters).

**Queuing timeouts too short** — if `query_queue_pending_timeout_second` is set to 10–30 s, moderate traffic spikes will cause end-user errors instead of brief waits. A value of 120–300 s is more appropriate for BI workloads.

**Forgetting to add `PARTITION` to the audit table** — `audit_db.starrocks_audit_log` will grow unboundedly without range partitioning and dynamic partition management. Always partition by month and set `dynamic_partition.start = -3` to auto-drop old data.

**Killing queries with `KILL CONNECTION` instead of `KILL QUERY`** — `KILL CONNECTION` drops the TCP connection, which can confuse connection poolers and cause pool exhaustion. Prefer `KILL QUERY <id>` unless the connection itself is stuck.

---

## References to Consult When Needed

- StarRocks docs — Resource Groups: `https://docs.starrocks.io/docs/administration/management/resource_management/resource_group/`
- StarRocks docs — Query Management: `https://docs.starrocks.io/docs/administration/Query_management/Query_management/`
- StarRocks docs — information_schema reference: `https://docs.starrocks.io/docs/reference/information_schema/`
- StarRocks docs — Audit Loader plugin: `https://docs.starrocks.io/docs/administration/audit_loader/`
- StarRocks docs — Memory spill: `https://docs.starrocks.io/docs/administration/management/resource_management/spill_to_disk/`
