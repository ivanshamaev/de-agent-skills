---
name: starrocks-admin-compaction
description: StarRocks compaction tuning — base compaction vs cumulative compaction, compaction score analysis, write amplification reduction, BE config parameters (compaction_threads/min_cumulative_compaction_num_singleton_deltas/max_compaction_candidate_num), SHOW PROC '/compactions', manual compaction trigger, Primary Key table compaction, rowset management
---

# StarRocks Compaction Administration

## When to Use

Load this skill when the user needs to:
- Diagnose high compaction score alerts (cumulative score > 100, base score > 10)
- Investigate slow queries after heavy ingestion or bulk loads
- Reduce disk space pressure caused by rowset accumulation
- Tune BE compaction parameters for write-heavy workloads
- Trigger manual compaction on specific tablets or tables
- Understand Primary Key table compaction and delete vector merging
- Monitor compaction progress and backlog via Prometheus / Grafana
- Troubleshoot compaction stuck, disk I/O saturation, or compaction loop failures

---

## Compaction Architecture

### Storage Hierarchy

```
Table
 └─ Partition
     └─ Tablet  (unit of replication and scheduling)
          └─ Rowset (immutable, created per ingestion batch or compaction)
               └─ Segment files (.dat)  — data stored column by column
                    └─ Index files (.idx)
```

Each Stream Load / INSERT commit writes one or more **rowsets** to a tablet. Without compaction, rowsets accumulate indefinitely, causing:
- More file descriptors opened per query
- Slower point lookups (must scan multiple bloom filters)
- Higher memory pressure during merge reads
- Wasted disk space from redundant delete markers

### Two Compaction Types

| Type | Trigger | Input | Goal |
|---|---|---|---|
| **Cumulative Compaction** | New singleton rowsets accumulate above threshold | Recent small rowsets (level 0) | Merge recent incremental writes into one larger rowset |
| **Base Compaction** | A base rowset + cumulative rowsets exceed size/count threshold | All rowsets for a tablet | Collapse entire tablet into one base rowset; reclaim delete space |

```
Before Cumulative Compaction:
  [base rowset 1]  +  [rs2] [rs3] [rs4] [rs5] [rs6]
                        ↑ singleton rowsets from ingestion

After Cumulative Compaction:
  [base rowset 1]  +  [merged cumulative rowset 2-6]

After Base Compaction:
  [new base rowset 1-6]  (all merged, deletes resolved)
```

### Compaction Score Formula

StarRocks assigns each tablet a **compaction score** that drives scheduling priority. Higher score = higher urgency.

**Cumulative compaction score** is dominated by the number of singleton rowsets above the cumulative point:

```
cumulative_score ≈ num_singleton_rowsets_above_cumulative_point
                   + size_penalty_factor (rowsets > max_cumulative_compaction_size_bytes)
```

**Base compaction score** is driven by total rowset count and total bytes relative to the base rowset:

```
base_score ≈ total_rowset_count / base_compaction_num_rows_per_rowset
             + total_bytes / max_base_compaction_bytes_threshold
```

**Operational thresholds:**
- Cumulative score > 100 → compaction is behind ingestion rate; investigate BE load
- Cumulative score > 1000 → critical backlog; read performance severely degraded
- Base score > 10 → base compaction is not keeping pace; disk space will grow
- Base score > 100 → emergency; merge immediately or risk query timeouts

---

## Reading Compaction State

### SHOW PROC '/compactions'

```sql
SHOW PROC '/compactions';
```

Returns a row per tablet replica undergoing or queued for compaction.

| Column | Type | Meaning |
|---|---|---|
| `TabletId` | BIGINT | Tablet identifier |
| `ReplicaId` | BIGINT | Replica identifier on a specific BE |
| `BackendId` | BIGINT | BE node hosting this replica |
| `SchemaHash` | BIGINT | Schema version hash |
| `Versions` | VARCHAR | Version range of the compaction (e.g. `[2-45]`) |
| `RowSetsNum` | INT | Number of rowsets being merged |
| `SegmentsNum` | INT | Total segment files involved |
| `InputRowsCount` | BIGINT | Rows read from input rowsets |
| `InputRowsDataSize` | BIGINT | Bytes read |
| `OutputRowsCount` | BIGINT | Rows after merge (lower = deletes resolved) |
| `OutputRowsDataSize` | BIGINT | Bytes written |
| `TotalReadIOBytes` | BIGINT | I/O read bytes including index |
| `TotalWriteIOBytes` | BIGINT | I/O write bytes |
| `TotalSegmentCount` | INT | Output segment count |
| `StartTime` | DATETIME | When this compaction task started |
| `State` | VARCHAR | `RUNNING`, `SUCCESS`, `FAILED`, `CANCELLED` |
| `Progress` | INT | Percentage complete (0-100) |
| `Type` | VARCHAR | `BASE` or `CUMULATIVE` |

**Interpretation patterns:**

```sql
-- Check for long-running compaction tasks (> 10 minutes)
SHOW PROC '/compactions';
-- Filter State = RUNNING with old StartTime

-- Stuck compaction: same TabletId in RUNNING for > 30 minutes
-- → suspect disk I/O saturation or a corrupt segment

-- Many FAILED entries → check BE log: grep 'compact' be.WARNING
```

### SHOW TABLET — Per-Tablet Compaction Status

```sql
-- Get tablet list for a table
SHOW TABLETS FROM db_name.table_name;

-- Inspect one tablet's compaction state
SHOW TABLET 1234567;
-- Returns: TabletId, State, LstSuccessVersion, LstFailedVersion,
--          LstConsistencyCheckTime, CompactionStatus, ...

-- Full compaction detail for a tablet
SHOW TABLET 1234567 STATUS;
```

Key fields from `SHOW TABLET <id>`:

| Field | Meaning |
|---|---|
| `CumulativeCompactionStatus` | `SUCCESS` / `RUNNING` / `FAILED` — last cumulative attempt |
| `BaseCompactionStatus` | `SUCCESS` / `RUNNING` / `FAILED` — last base attempt |
| `CumulativePoint` | Version boundary: rowsets at or above this are in cumulative zone |
| `NumRowsets` | Total rowset count on this tablet |
| `NumSegments` | Total segment file count |

### information_schema.be_compactions

Available in StarRocks 3.0+. Queryable from any MySQL client.

```sql
-- Current compaction workload across all BEs
SELECT
    be_id,
    tablet_id,
    compaction_type,
    input_rowsets_count,
    input_rowsets_size / 1073741824  AS input_gb,
    output_rowsets_size / 1073741824 AS output_gb,
    state,
    start_time,
    TIMESTAMPDIFF(SECOND, start_time, NOW()) AS elapsed_sec
FROM information_schema.be_compactions
WHERE state = 'RUNNING'
ORDER BY elapsed_sec DESC;

-- Compaction throughput by BE (last hour)
SELECT
    be_id,
    compaction_type,
    COUNT(*)                                AS tasks_completed,
    SUM(input_rowsets_size) / 1073741824    AS total_input_gb,
    SUM(output_rowsets_size) / 1073741824   AS total_output_gb,
    AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) AS avg_duration_sec
FROM information_schema.be_compactions
WHERE state = 'SUCCESS'
  AND end_time >= NOW() - INTERVAL 1 HOUR
GROUP BY be_id, compaction_type
ORDER BY be_id, compaction_type;

-- Tablets with high rowset count (compaction candidates)
SELECT
    tablet_id,
    be_id,
    input_rowsets_count,
    compaction_type
FROM information_schema.be_compactions
WHERE input_rowsets_count > 50
ORDER BY input_rowsets_count DESC
LIMIT 20;
```

### Compaction Score via Metrics Endpoint

Query a BE directly for per-tablet compaction scores:

```bash
# Get compaction score for all tablets on BE (port 8040 is BE HTTP port)
curl -s http://<be_host>:8040/api/compaction/show?tablet_id=<tablet_id>

# Example response (JSON):
# {
#   "cumulative_score": 35,
#   "base_score": 2,
#   "cumulative_rowsets": 35,
#   "base_rowsets": 1,
#   "cumulative_point": 48
# }
```

**High score thresholds summary:**

| Score | Type | Severity | Action |
|---|---|---|---|
| > 20 | Cumulative | Warning | Monitor; may self-resolve |
| > 100 | Cumulative | High | Increase `compaction_threads` |
| > 1000 | Cumulative | Critical | Manual trigger + investigate ingestion rate |
| > 5 | Base | Warning | Normal; monitor |
| > 10 | Base | High | Check `max_base_compaction_bytes`; free disk space |
| > 50 | Base | Critical | Manual trigger; possible disk saturation |

---

## BE Configuration Parameters

Configure in `be.conf` on each BE node. Changes to most parameters require BE restart unless marked as dynamic.

### Core Compaction Thread Counts

```properties
# Number of threads dedicated to ALL compaction work (base + cumulative combined)
# Default: 4
# Recommended for write-heavy workloads: 8–12
# Note: increasing past 16 rarely helps; I/O becomes the bottleneck
compaction_threads=8

# Separate thread count for cumulative compaction only (overrides compaction_threads
# for cumulative tasks when set > 0; StarRocks 3.1+)
# Default: 0 (uses compaction_threads pool)
cumulative_compaction_threads=4

# Separate thread count for base compaction only (StarRocks 3.1+)
# Default: 0 (uses compaction_threads pool)
base_compaction_threads=2
```

### Cumulative Compaction Thresholds

```properties
# Minimum number of singleton rowsets before triggering cumulative compaction
# Default: 5
# Lower = more frequent small merges (lower read latency, higher write amp)
# Higher = larger merges (less CPU, but more rowsets accumulate before merge)
# Recommended for high-ingest: 3
# Recommended for batch-heavy: 10
min_cumulative_compaction_num_singleton_deltas=5

# Maximum singleton rowsets that one cumulative compaction task can consume
# Default: 1000
# Reduce if individual compaction tasks are taking too long and blocking threads
max_cumulative_compaction_num_singleton_deltas=1000

# Maximum total bytes a single cumulative compaction task will process
# Default: 1073741824 (1 GB)
# Increase for large-tablet workloads to reduce task count
# Decrease to keep individual tasks short and predictable
max_cumulative_compaction_size_bytes=1073741824

# How often (seconds) the compaction scheduler checks for cumulative candidates
# Default: 1
# Increase to 5–10 if scheduler is consuming measurable CPU on lightly-loaded BEs
cumulative_compaction_check_interval_seconds=1
```

### Base Compaction Thresholds

```properties
# How often (seconds) the scheduler looks for base compaction candidates
# Default: 60 (StarRocks 2.x); 600 (StarRocks 3.x)
# Reduce to 30–60 on write-heavy clusters to run base compaction more aggressively
base_compaction_check_interval_seconds=60

# Maximum input bytes processed in one base compaction task
# Default: 21474836480 (20 GB)
# Reduce to 5–10 GB on BEs with limited RAM or slower disks
# Increase if you want base compaction to finish in fewer tasks
max_base_compaction_bytes=21474836480

# Minimum number of cumulative rowsets required to trigger base compaction
# Default: 5
# Increase to delay base compaction until more data is accumulated
base_compaction_min_rowset_num=5

# Minimum ratio of data overlapping between cumulative and base rowsets
# that triggers base compaction regardless of rowset count
# Default: 0.3  (30%)
base_compaction_min_data_ratio=0.3
```

### Candidate Queue

```properties
# Maximum number of tablets kept in the compaction candidate priority queue
# Default: 40960
# Increase if you have > 40960 tablets per BE and want all of them eligible
# Note: large values increase scheduler memory usage
max_compaction_candidate_num=40960
```

### Memory Limits for Compaction

```properties
# Maximum memory a single compaction task may use (bytes)
# Default: 0 (auto: 1/4 of total BE memory)
# Set explicitly on BEs with many parallel compaction threads
# to avoid OOM during merge of large tablets
compaction_memory_limit_per_worker=536870912   # 512 MB per thread

# Total memory budget for all concurrent compaction tasks
# Default: 0 (auto: total_be_memory * 0.25)
total_compaction_memory_limit=4294967296       # 4 GB total
```

### Primary Key Table — Compaction Parameters

```properties
# Minimum interval (seconds) between two compaction runs on the same
# Primary Key tablet. Prevents thrashing on continuously updated PKT tables.
# Default: 0 (no minimum gap)
# Recommended: 60–300 for tables with high update frequency
update_compaction_per_tablet_min_interval_seconds=60

# Maximum number of delete vectors (per-rowset) to merge in one PKT compaction
# Default: 1000
# Increase if delete vectors accumulate faster than they are merged
update_compaction_delvec_file_io_amp_ratio=2

# Number of threads dedicated to Primary Key table compaction
# Default: 0 (uses compaction_threads pool)
# Set to 2–4 on clusters with heavy PKT update workloads to isolate PKT compaction
update_compaction_threads=2

# Size threshold (bytes) for a single PKT compaction output segment
# Default: 268435456 (256 MB)
update_compaction_size_threshold=268435456
```

---

## Primary Key Table Compaction

Primary Key (PKT) tables use a fundamentally different compaction model from Duplicate/Aggregate tables.

### How PKT Compaction Differs

```
Duplicate Key table rowset:
  [row1] [row2] [row3] ...  — all rows stored, merge by overwrite

Primary Key table rowset:
  [row1] [row2] [row3] ...  — base row data
  +
  [delete vector bitmap]    — per-rowset; marks rows deleted by UPSERT/DELETE
```

When you run `UPDATE` or `DELETE` on a PKT table, StarRocks writes:
1. A new rowset with the updated rows (or empty for pure deletes)
2. A **delete vector** (a roaring bitmap) against the old rowset, marking which rows are superseded

Over time, delete vectors pile up. Compaction merges them:

```
Before PKT compaction:
  [base rowset A]  del_vec_A: {row2, row5}
  [rowset B]       del_vec_B: {row1}
  [rowset C]       (no deletes)

After PKT compaction:
  [merged rowset]  — rows 1, 2, 5 from A removed; final values from B/C written
                     del_vec cleared
```

### PKT-Specific Compaction Behavior

- **No delete vector = no compaction urgency**: A PKT tablet with only appends (no updates/deletes) compacts identically to a Duplicate Key table.
- **High update rate = delete vector accumulation**: Tables receiving continuous `UPDATE` or `DELETE` need `update_compaction_per_tablet_min_interval_seconds` tuned down to allow faster merge.
- **Persistent Index impact**: PKT tables with `enable_persistent_index = true` store the primary key index on disk. Compaction rewrites the index too — factor in extra I/O budget.

### PKT Compaction Configuration Example

```properties
# For a cluster running heavy UPSERT workloads on Primary Key tables:
update_compaction_threads=4
update_compaction_per_tablet_min_interval_seconds=30
update_compaction_size_threshold=134217728   # 128 MB — smaller segments for faster index rebuild
max_base_compaction_bytes=10737418240        # 10 GB — keep base tasks bounded
```

### Check PKT Delete Vector Accumulation

```sql
-- Tablets with large delete vector counts (high update pressure)
SELECT
    tablet_id,
    be_id,
    input_rowsets_count,
    compaction_type
FROM information_schema.be_compactions
WHERE compaction_type = 'UPDATE'
ORDER BY input_rowsets_count DESC
LIMIT 20;
```

```bash
# BE HTTP API: inspect a specific PKT tablet's delete vector stats
curl -s "http://<be_host>:8040/api/update/get_del_vec?tablet_id=<tablet_id>"
```

---

## Manual Compaction Trigger

Available in StarRocks 3.1+. Forces immediate compaction without waiting for the background scheduler.

### Table-Level Manual Compaction

```sql
-- Trigger compaction for ALL tablets of a table (both base and cumulative)
ALTER TABLE db_name.table_name COMPACT;

-- Trigger for a specific partition only
ALTER TABLE db_name.table_name COMPACT PARTITION (p20240101);

-- Trigger cumulative compaction only
ALTER TABLE db_name.table_name COMPACT CUMULATIVE;

-- Trigger base compaction only
ALTER TABLE db_name.table_name COMPACT BASE;
```

`ALTER TABLE ... COMPACT` is **asynchronous**. The statement returns immediately; compaction runs in background. Monitor progress with:

```sql
-- Check if compaction has started
SHOW PROC '/compactions';

-- Or query information_schema
SELECT tablet_id, state, compaction_type, start_time
FROM information_schema.be_compactions
WHERE state = 'RUNNING'
ORDER BY start_time;
```

### Tablet-Level Manual Compaction via BE API

For surgical control (single tablet, useful when one tablet has an extreme backlog):

```bash
# Trigger cumulative compaction on a specific tablet
curl -X POST "http://<be_host>:8040/api/compaction/run?tablet_id=<tablet_id>&compact_type=cumulative"

# Trigger base compaction on a specific tablet
curl -X POST "http://<be_host>:8040/api/compaction/run?tablet_id=<tablet_id>&compact_type=base"

# Response: {"status": "Success", "msg": "..."}

# Check tablet compaction status
curl -s "http://<be_host>:8040/api/compaction/show?tablet_id=<tablet_id>"
```

### When to Manually Trigger

| Scenario | Action |
|---|---|
| Post-bulk-load batch (large historical backfill) | `ALTER TABLE ... COMPACT` immediately after load completes |
| Pre-maintenance window (reduce I/O during business hours) | Schedule manual compact during off-peak |
| Single tablet with extreme rowset count (> 500) | BE API on that specific tablet |
| Query degradation after heavy deletes/updates | `ALTER TABLE ... COMPACT BASE` to resolve delete markers |

---

## Write Amplification Reduction

Write amplification (WA) = bytes written to disk / bytes of user data. Compaction is the primary driver of WA in StarRocks.

### Root Cause: Too Many Small Commits

Every Stream Load / INSERT OVERWRITE / Broker Load transaction creates at least one rowset per tablet. Frequent small commits produce many small rowsets, forcing aggressive cumulative compaction.

```
Scenario A — 10 000 tiny loads of 1 MB each:
  → 10 000 rowsets × N tablets
  → Cumulative compaction merges repeatedly: WA ≈ 10–20×

Scenario B — 100 loads of 100 MB each:
  → 100 rowsets × N tablets
  → Far fewer compaction cycles: WA ≈ 2–4×
```

### Strategy 1: Increase Batch Size for Stream Load

```bash
# BAD: many small loads
for file in /data/day/*.csv; do
    curl -X PUT "http://<fe_host>:8030/api/mydb/mytable/_stream_load" \
        -H "label: load_$(date +%s%N)" \
        --data-binary "@$file"
done

# GOOD: concatenate or use larger batches
cat /data/day/*.csv | curl -X PUT "http://<fe_host>:8030/api/mydb/mytable/_stream_load" \
    -H "label: load_$(date +%Y%m%d_%H)" \
    -H "max_filter_ratio: 0.01" \
    --data-binary @-
```

Target: **at least 100 MB per load transaction**, ideally 256 MB–1 GB.

### Strategy 2: Reduce Commit Frequency in Flink / Spark Connectors

```java
// StarRocks Flink connector sink options
StarRocksSinkOptions.builder()
    .withProperty("sink.buffer-flush.max-bytes", "268435456")   // 256 MB before flush
    .withProperty("sink.buffer-flush.interval-ms", "30000")     // or every 30 seconds
    .withProperty("sink.max-retries", "3")
    .build();
```

```python
# PySpark StarRocks connector
df.write \
    .format("starrocks") \
    .option("starrocks.fe.http.url", "http://fe_host:8030") \
    .option("starrocks.write.properties.buffer_size", "268435456") \
    .option("starrocks.write.properties.flush_interval_ms", "30000") \
    .mode("append") \
    .save()
```

### Strategy 3: Partition Alignment

Writes that span many partitions create rowsets in ALL those partitions simultaneously. Align data to partition boundaries before loading:

```sql
-- Pre-partition data before loading (Spark example)
df.repartition(col("dt"))
  .sortWithinPartitions("dt", "user_id")
  .write
  .partitionBy("dt")
  .format("parquet")
  .save("/staging/")

-- Then load each partition separately
```

### Strategy 4: Reduce Tablet Count for Small Tables

Excessive tablets amplify the rowset count problem. For small tables, use fewer tablets:

```sql
-- Instead of default (auto-computed, often too many tablets for small tables)
CREATE TABLE small_dim_table (
    id      INT,
    name    VARCHAR(128),
    created DATE
)
ENGINE = OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4    -- explicit small bucket count
PROPERTIES ("replication_num" = "3");
```

Rule of thumb: each tablet should hold 1–10 GB of data at steady state. Calculate:
```
buckets = CEILING(expected_table_size_gb / 1 GB)
```

---

## Monitoring Compaction

### Prometheus Metrics

StarRocks BEs expose metrics on port `8040/metrics` (Prometheus format).

Key compaction metrics:

| Metric | Type | Description |
|---|---|---|
| `starrocks_be_compaction_bytes_total` | Counter | Total bytes read during compaction (label: `type=base\|cumulative`) |
| `starrocks_be_compaction_rowsets_total` | Counter | Total rowsets consumed by compaction |
| `starrocks_be_compaction_deltas_total` | Counter | Total rowset deltas (versions) merged |
| `starrocks_be_running_task_num{type="base_compaction"}` | Gauge | Currently running base compaction tasks |
| `starrocks_be_running_task_num{type="cumulative_compaction"}` | Gauge | Currently running cumulative compaction tasks |
| `starrocks_be_compaction_score` | Gauge | Maximum compaction score across all tablets on this BE |

```yaml
# prometheus.yml — scrape config for StarRocks BE
scrape_configs:
  - job_name: starrocks_be
    static_configs:
      - targets:
          - be-01:8040
          - be-02:8040
          - be-03:8040
    metrics_path: /metrics
    scrape_interval: 15s
```

### Grafana Panel Setup

**Panel 1 — Compaction Score (alert threshold)**

```
# PromQL
max by (instance) (starrocks_be_compaction_score)

# Alert rule: fire when score > 100 for 5 minutes
ALERT HighCompactionScore
  IF max by (instance)(starrocks_be_compaction_score) > 100
  FOR 5m
  LABELS { severity = "warning" }
  ANNOTATIONS { summary = "BE {{ $labels.instance }} compaction score {{ $value }}" }
```

**Panel 2 — Compaction Throughput (bytes/sec)**

```
# PromQL: rate of bytes compacted per second (rolling 5m)
rate(starrocks_be_compaction_bytes_total{type="cumulative"}[5m])
rate(starrocks_be_compaction_bytes_total{type="base"}[5m])
```

**Panel 3 — Active Compaction Tasks**

```
# PromQL
starrocks_be_running_task_num{type="cumulative_compaction"}
starrocks_be_running_task_num{type="base_compaction"}
```

**Panel 4 — Compaction Write Amplification (ratio)**

```
# PromQL: ratio of compaction output bytes to ingestion bytes
# Proxy metric: compaction bytes / ingest bytes
rate(starrocks_be_compaction_bytes_total[10m])
  /
rate(starrocks_be_load_bytes_total[10m])
```

### Alerting Thresholds

```yaml
# Suggested alert rules
- name: starrocks_compaction
  rules:
    - alert: StarRocksCompactionScoreCritical
      expr: max by (instance) (starrocks_be_compaction_score) > 500
      for: 2m
      labels:
        severity: critical
      annotations:
        summary: "StarRocks BE {{ $labels.instance }} compaction score {{ $value }}"

    - alert: StarRocksCompactionScoreHigh
      expr: max by (instance) (starrocks_be_compaction_score) > 100
      for: 10m
      labels:
        severity: warning

    - alert: StarRocksNoCompactionProgress
      expr: increase(starrocks_be_compaction_bytes_total[15m]) == 0
             and starrocks_be_compaction_score > 50
      for: 5m
      labels:
        severity: critical
      annotations:
        summary: "BE {{ $labels.instance }} has high score but zero compaction throughput — possibly stuck"
```

---

## Troubleshooting

### Compaction Stuck (No Progress)

**Symptom**: `starrocks_be_compaction_score` remains high; `starrocks_be_compaction_bytes_total` rate is zero.

**Diagnosis:**

```bash
# 1. Check BE logs for compaction errors
grep -i 'compaction\|compact' /path/to/be/log/be.WARNING | tail -100

# 2. Verify compaction threads are not all blocked
curl -s "http://<be_host>:8040/api/compaction/show" | python3 -m json.tool

# 3. Check disk space — compaction needs ~2× rowset size free
df -h /path/to/be/storage

# 4. Check open file descriptor limit
cat /proc/$(pgrep starrocks_be)/limits | grep 'open files'
```

**Common causes and fixes:**

| Cause | Symptom in logs | Fix |
|---|---|---|
| Disk full | `No space left on device` in be.WARNING | Free disk space; delete old partitions |
| FD limit exhausted | `Too many open files` | Increase `ulimit -n` to 655360 in BE startup script |
| OOM during large compaction | `Memory limit exceeded` in be.WARNING | Set `compaction_memory_limit_per_worker`; reduce `max_base_compaction_bytes` |
| Corrupt segment file | `checksum mismatch` in be.WARNING | Run `ADMIN CHECK TABLET (tablet_id)` then repair |
| BE node CPU saturated | Compaction tasks queued but not starting | Reduce `compaction_threads`; check for competing workloads |

### Disk I/O Saturation During Compaction

**Symptom**: `iostat` shows BE disks at 100% utilization; query latency spikes during compaction windows.

**Mitigation options:**

```properties
# be.conf — limit compaction I/O rate (bytes/sec per thread)
# Default: 0 (unlimited)
# Set to limit compaction to ~200 MB/s total (8 threads × 25 MB/s)
compaction_io_limit_mb_per_second=25

# Reduce parallel compaction threads during business hours
# (requires BE restart or dynamic SET via admin API)
compaction_threads=4

# Increase the check interval to reduce scheduling frequency
cumulative_compaction_check_interval_seconds=5
base_compaction_check_interval_seconds=120
```

**I/O isolation with Linux cgroups (advanced):**

```bash
# Assign StarRocks BE process to a blkio cgroup with limited weight
cgcreate -g blkio:/starrocks
cgset -r blkio.weight=200 /starrocks    # default is 500; lower = less I/O priority
cgexec -g blkio:/starrocks ./start_be.sh
```

### Emergency Compaction Disable

Use only as a last resort (e.g., disk failure imminent, emergency read-only mode needed).

```bash
# Disable cumulative compaction on a specific BE via HTTP API
curl -X POST "http://<be_host>:8040/api/update_config?cumulative_compaction_check_interval_seconds=3600"

# Re-enable
curl -X POST "http://<be_host>:8040/api/update_config?cumulative_compaction_check_interval_seconds=1"
```

Note: `api/update_config` accepts dynamic BE parameters and takes effect immediately without restart.

### Compaction Generating Excessive Small Segments

**Symptom**: Compaction completes but segment count stays high.

**Cause**: `max_segment_file_size` is too small, or the merge produces many tiny segments from sparse data.

```properties
# be.conf — minimum rows before a new segment is written
# Default: 1048576 (1M rows)
# Increase if compaction output has too many small segments
max_segment_file_size=268435456   # 256 MB per segment file
```

### Validating Compaction Health — Runbook

```bash
# Step 1: Check overall compaction scores from FE
mysql -h fe_host -P 9030 -e "
SELECT
    be_id,
    MAX(input_rowsets_count) AS max_rowsets,
    COUNT(*) AS running_tasks
FROM information_schema.be_compactions
WHERE state = 'RUNNING'
GROUP BY be_id;"

# Step 2: Find worst tablets
mysql -h fe_host -P 9030 -e "
SELECT tablet_id, be_id, input_rowsets_count, compaction_type
FROM information_schema.be_compactions
WHERE state IN ('RUNNING', 'FAILED')
ORDER BY input_rowsets_count DESC
LIMIT 10;"

# Step 3: For top offender tablet, check score
curl -s "http://<be_host>:8040/api/compaction/show?tablet_id=<tablet_id>"

# Step 4: Manually trigger if needed
curl -X POST "http://<be_host>:8040/api/compaction/run?tablet_id=<tablet_id>&compact_type=cumulative"

# Step 5: Verify progress
curl -s "http://<be_host>:8040/api/compaction/show?tablet_id=<tablet_id>"
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Thousands of tiny Stream Load transactions per hour | One rowset per load → extreme cumulative score → constant compaction | Batch to ≥ 100 MB per load |
| `compaction_threads=1` on write-heavy cluster | Single thread cannot keep pace with ingest; score grows unbounded | Set 8–12 threads for write-heavy BEs |
| Ignoring compaction score until queries fail | By the time queries are slow, backlogs are in the thousands | Alert at score > 100; act at score > 500 |
| Manual compact during peak query hours | Compaction I/O competes with query I/O; both degrade | Schedule `ALTER TABLE ... COMPACT` during off-peak windows |
| Setting `max_base_compaction_bytes` to unlimited | One base compaction task can OOM the BE on large tablets | Keep at 10–20 GB; let it run in multiple passes |
| Over-partitioning + high ingest rate | Each partition gets its own rowsets → 10× more rowsets total | Right-size bucket count; use monthly not daily partitions for low-ingest dims |
| Not monitoring delete vector count on PKT tables | Delete vectors silently accumulate; read performance degrades because every scan must apply multiple bitmaps | Monitor `update_compaction_*` metrics; set `update_compaction_per_tablet_min_interval_seconds` |
| Running `ALTER TABLE COMPACT` on every table on a schedule without score monitoring | Wasted I/O and CPU on tables that don't need it | Compact only tablets/tables where score > threshold |
| Disabling compaction permanently via cron/config | Rowsets accumulate forever; eventually queries OOM or timeout | Use dynamic `api/update_config` for temporary pause only; always re-enable |

---

## References to Consult When Needed

- [StarRocks Compaction Overview](https://docs.starrocks.io/docs/administration/management/compaction/)
- [StarRocks BE Configuration Reference](https://docs.starrocks.io/docs/administration/Configuration/)
- [StarRocks Primary Key Table](https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/)
- [StarRocks SHOW PROC](https://docs.starrocks.io/docs/administration/management/monitoring/Monitor_and_Alert/)
- [StarRocks information_schema](https://docs.starrocks.io/docs/reference/information_schema/be_compactions/)
- [StarRocks ALTER TABLE COMPACT](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_TABLE/)
- [StarRocks Tablet Management](https://docs.starrocks.io/docs/administration/management/tablet_index/)
