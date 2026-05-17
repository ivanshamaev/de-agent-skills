---
name: starrocks-partitioning
description: StarRocks partitioning strategy — RANGE partitioning (manual/dynamic/expression), LIST partitioning, dynamic partition properties (enable/time_unit/start/end/prefix/replication_num), partition pruning verification via EXPLAIN, hot/cold tiering with storage_medium, partition lifecycle automation, SHOW PARTITIONS, ADD/DROP/TRUNCATE PARTITION
---

# StarRocks — Partitioning Strategy

## When to Use

Load this skill when the user needs to:
- Design a partition scheme for a new StarRocks table (event logs, metrics, orders, IoT)
- Automate partition creation and expiry for time-series data
- Diagnose whether queries are pruning partitions correctly via EXPLAIN
- Implement hot/cold tiering to move cold partitions from SSD to HDD
- Manage partition lifecycle (ADD, DROP, TRUNCATE) with or without data loss risk
- Choose between manual RANGE, dynamic RANGE, expression RANGE, or LIST partitioning
- Combine partitioning with bucketing for optimal data layout

---

## 1. Partitioning Concepts

### Partition as Unit of Data Management

A partition in StarRocks is the coarsest unit of physical data management:
- **Query scope**: the query planner eliminates irrelevant partitions before scanning begins
- **Data lifecycle**: you can ADD, DROP, or TRUNCATE individual partitions independently
- **Storage tiering**: each partition carries its own `storage_medium` and `storage_cooldown_time`
- **Compaction**: compaction runs per tablet within a partition; smaller partitions compact faster
- **Replication**: `replication_num` can be overridden per partition for cost control

A partition is further subdivided into **buckets** (tablets). The bucket count controls the degree of parallelism within a partition scan and the file count stored on each BE. Partitioning determines *which data to touch*; bucketing determines *how that data is split across BEs*.

### Partition Pruning = Scan Elimination

When a query carries a filter on the partition column, StarRocks FE resolves which partitions satisfy the predicate and sends tablet scan RPCs only to BEs that hold those partitions. All other partitions are skipped entirely — no I/O, no decompression, no CPU.

Effective pruning requires:
1. The filter column must be the partition column (or an expression the planner can evaluate at plan time).
2. The filter must not wrap the partition column in a non-deterministic or runtime function.
3. There must be no implicit type cast that prevents constant-folding.

### Relationship with Bucketing

```
Table
 └── Partition p_20240101   (RANGE row group)
 │    ├── Bucket 0  (tablet → stored on BE-1 + BE-2 + BE-3)
 │    ├── Bucket 1
 │    └── Bucket N
 └── Partition p_20240102
      ├── Bucket 0
      └── Bucket N
```

Define partition granularity first (typically days or months). Then choose bucket count as roughly `total_partition_data_bytes / 1 GB`, adjusted to a power of two. A common mistake is choosing too-fine partition granularity (hourly for low-volume tables) and ending up with thousands of small tablets.

---

## 2. Manual RANGE Partitioning

### Single-Column RANGE

```sql
CREATE TABLE events (
    event_id    BIGINT       NOT NULL,
    dt          DATE         NOT NULL,
    user_id     BIGINT,
    event_type  VARCHAR(64),
    payload     JSON
)
ENGINE = OLAP
DUPLICATE KEY(event_id, dt)
PARTITION BY RANGE(dt) (
    PARTITION p20240101 VALUES LESS THAN ('2024-01-02'),
    PARTITION p20240102 VALUES LESS THAN ('2024-01-03'),
    PARTITION p20240103 VALUES LESS THAN ('2024-01-04'),
    PARTITION p20240104 VALUES LESS THAN ('2024-01-05'),
    PARTITION p20240105 VALUES LESS THAN ('2024-01-06'),
    PARTITION p20240106 VALUES LESS THAN ('2024-01-07'),
    PARTITION p_future  VALUES LESS THAN (MAXVALUE)
)
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "storage_medium"  = "SSD"
);
```

Rules for `VALUES LESS THAN`:
- Partitions are **left-closed, right-open**: `p20240101` holds rows where `dt >= '2024-01-01'` AND `dt < '2024-01-02'`.
- The last partition should use `MAXVALUE` as a catch-all unless you want data outside the declared range to be rejected.
- Partition names must be unique within the table. Use a consistent naming convention (`p` + `YYYYMMDD` for days, `p` + `YYYYMM` for months).

### Multi-Column RANGE Partitioning

Use multi-column RANGE when data is logically partitioned by more than one ordered dimension (e.g., year + month as integers, or region_id + date).

```sql
CREATE TABLE orders_mc (
    order_id    BIGINT    NOT NULL,
    region_id   INT       NOT NULL,
    order_date  DATE      NOT NULL,
    amount      DECIMAL(18,2)
)
ENGINE = OLAP
DUPLICATE KEY(order_id)
PARTITION BY RANGE(region_id, order_date) (
    PARTITION p_r1_2024q1 VALUES LESS THAN (2, '2024-04-01'),
    PARTITION p_r1_2024q2 VALUES LESS THAN (2, '2024-07-01'),
    PARTITION p_r2_2024q1 VALUES LESS THAN (3, '2024-04-01'),
    PARTITION p_r2_2024q2 VALUES LESS THAN (3, '2024-07-01'),
    PARTITION p_future    VALUES LESS THAN (MAXVALUE, MAXVALUE)
)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES ("replication_num" = "3");
```

Multi-column RANGE pruning is lexicographic: a filter like `region_id = 1 AND order_date >= '2024-01-01'` prunes correctly only if the planner can resolve both dimensions. Prefer single-column partitioning unless you have a strong operational reason for the multi-column layout.

### Batch Creation with START ... END ... EVERY

For tables that need many pre-created partitions, use the batch syntax to avoid writing hundreds of `VALUES LESS THAN` clauses:

```sql
CREATE TABLE metrics_daily (
    ts          DATETIME     NOT NULL,
    host        VARCHAR(128),
    metric_name VARCHAR(128),
    value       DOUBLE
)
ENGINE = OLAP
DUPLICATE KEY(ts, host)
PARTITION BY RANGE(ts) (
    START ("2024-01-01") END ("2025-01-01") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(host) BUCKETS 8
PROPERTIES ("replication_num" = "3");
```

- `START` is inclusive; `END` is exclusive.
- `EVERY` accepts `INTERVAL N DAY`, `INTERVAL N WEEK`, `INTERVAL N MONTH`, `INTERVAL N YEAR`, or a plain integer for numeric columns.
- StarRocks auto-names generated partitions as `p20240101`, `p20240102`, etc. for DATE/DATETIME columns.

To pre-create monthly partitions for a full year:

```sql
PARTITION BY RANGE(order_date) (
    START ("2024-01-01") END ("2025-01-01") EVERY (INTERVAL 1 MONTH)
)
```

---

## 3. Dynamic Partitioning

Dynamic partitioning delegates partition lifecycle management to the StarRocks FE scheduler. The scheduler runs periodically (default every 600 seconds) and:
1. Creates new partitions in the **future window** (`end` days/hours/months ahead of today).
2. Drops partitions that have aged past the **retention window** (`start` days/hours/months before today, expressed as a negative integer).

### Full Property Reference

| Property | Type | Required | Default | Description |
|---|---|---|---|---|
| `dynamic_partition.enable` | BOOLEAN | yes | — | `true` to activate the scheduler |
| `dynamic_partition.time_unit` | STRING | yes | — | `HOUR`, `DAY`, `WEEK`, `MONTH`, `YEAR` |
| `dynamic_partition.start` | INT | no | `INT_MIN` | Negative offset: partitions older than this are dropped. `-30` = keep 30 days of history |
| `dynamic_partition.end` | INT | yes | — | Positive offset: pre-create up to this many future units. `3` = 3 days ahead |
| `dynamic_partition.prefix` | STRING | yes | — | Partition name prefix, e.g. `"p"` → `p20240101` |
| `dynamic_partition.buckets` | INT | no | table default | Bucket count for each new partition |
| `dynamic_partition.replication_num` | INT | no | table default | Replication factor for new partitions |
| `dynamic_partition.history_partition_num` | INT | no | `0` | How many historical partitions to create on table creation (backfill) |
| `dynamic_partition.time_zone` | STRING | no | server TZ | Partition boundary time zone, e.g. `"UTC"`, `"America/New_York"` |
| `dynamic_partition.start_day_of_week` | INT | no | `1` (Monday) | For WEEK granularity: which day starts the week (1=Mon … 7=Sun) |
| `dynamic_partition.start_day_of_month` | INT | no | `1` | For MONTH granularity: which day of month starts the partition |

### Production Example — Daily Dynamic Partitioning

```sql
CREATE TABLE user_events (
    event_id    BIGINT       NOT NULL,
    event_date  DATE         NOT NULL,
    user_id     BIGINT       NOT NULL,
    event_type  VARCHAR(64),
    properties  JSON
)
ENGINE = OLAP
DUPLICATE KEY(event_id, event_date)
PARTITION BY RANGE(event_date) ()          -- empty: scheduler manages all partitions
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES (
    "replication_num"                       = "3",
    "dynamic_partition.enable"              = "true",
    "dynamic_partition.time_unit"           = "DAY",
    "dynamic_partition.start"               = "-90",   -- drop partitions older than 90 days
    "dynamic_partition.end"                 = "3",     -- pre-create 3 days ahead
    "dynamic_partition.prefix"              = "p",
    "dynamic_partition.buckets"             = "16",
    "dynamic_partition.replication_num"     = "3",
    "dynamic_partition.history_partition_num" = "7",   -- create last 7 days on DDL run
    "dynamic_partition.time_zone"           = "UTC"
);
```

### Hourly Dynamic Partitioning (High-Frequency Ingestion)

```sql
CREATE TABLE clickstream_hourly (
    ts          DATETIME     NOT NULL,
    session_id  VARCHAR(64)  NOT NULL,
    page_url    VARCHAR(512),
    user_agent  VARCHAR(256)
)
ENGINE = OLAP
DUPLICATE KEY(ts, session_id)
PARTITION BY RANGE(ts) ()
DISTRIBUTED BY HASH(session_id) BUCKETS 8
PROPERTIES (
    "replication_num"                   = "3",
    "dynamic_partition.enable"          = "true",
    "dynamic_partition.time_unit"       = "HOUR",
    "dynamic_partition.start"           = "-72",   -- keep 72 hours
    "dynamic_partition.end"             = "6",     -- pre-create 6 hours ahead
    "dynamic_partition.prefix"          = "p",
    "dynamic_partition.buckets"         = "8",
    "dynamic_partition.time_zone"       = "UTC"
);
```

### Inspecting Dynamic Partition State

```sql
-- Show all tables with dynamic partitioning enabled
SHOW DYNAMIC PARTITION TABLES;
```

The output includes columns: `TableName`, `Enable`, `TimeUnit`, `Start`, `End`, `Prefix`, `Buckets`, `ReplicationNum`, `StartOfMonth`, `StartOfWeek`, `LastUpdateTime`, `LastSchedulerTime`, `State`, `LastCreatePartitionMsg`, `LastRemovePartitionMsg`. Check `LastCreatePartitionMsg` and `LastRemovePartitionMsg` for scheduling errors.

### Modifying Dynamic Partition Properties Online

```sql
-- Extend retention from 30 to 90 days
ALTER TABLE user_events SET (
    "dynamic_partition.start" = "-90"
);

-- Increase look-ahead window from 3 to 7 days
ALTER TABLE user_events SET (
    "dynamic_partition.end" = "7"
);

-- Disable dynamic partitioning (switch to manual management)
ALTER TABLE user_events SET (
    "dynamic_partition.enable" = "false"
);
```

`ALTER TABLE ... SET` is metadata-only and takes effect on the next scheduler run. No data movement occurs.

---

## 4. Expression Partitioning (StarRocks 3.0+)

Expression partitioning lets StarRocks auto-create partitions as data arrives, using a `date_trunc` or column expression on the partition column. There is no need to pre-declare partition boundaries or configure a background scheduler.

### Single-Column Expression Partitioning

```sql
CREATE TABLE sensor_readings (
    reading_id  BIGINT       NOT NULL,
    ts          DATETIME     NOT NULL,
    sensor_id   VARCHAR(64)  NOT NULL,
    temperature DOUBLE,
    humidity    DOUBLE
)
ENGINE = OLAP
DUPLICATE KEY(reading_id, ts)
PARTITION BY date_trunc('day', ts)         -- auto-creates one partition per calendar day
DISTRIBUTED BY HASH(sensor_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3"
);
```

When a row with `ts = '2024-03-15 09:12:34'` is inserted, StarRocks automatically creates partition `p20240315` covering `[2024-03-15 00:00:00, 2024-03-16 00:00:00)` if it does not yet exist. Supported truncation levels: `'hour'`, `'day'`, `'month'`, `'year'`.

### Multi-Column Expression Partitioning

```sql
CREATE TABLE sales_by_region (
    sale_id    BIGINT       NOT NULL,
    city       VARCHAR(64)  NOT NULL,
    sale_ts    DATETIME     NOT NULL,
    amount     DECIMAL(18,2)
)
ENGINE = OLAP
DUPLICATE KEY(sale_id)
PARTITION BY (city, date_trunc('month', sale_ts))  -- partition per city × month
DISTRIBUTED BY HASH(sale_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "3"
);
```

Partition names are auto-generated as a compound of the column values, e.g., `p_beijing_202403`. Query filters on both dimensions prune precisely.

### Expression Partitioning vs. Dynamic Partitioning

| Dimension | Dynamic Partitioning | Expression Partitioning |
|---|---|---|
| Partition creation | Background scheduler (async) | On first INSERT into that range (sync) |
| Partition deletion | Automatic via `start` offset | Manual (`DROP PARTITION`) or via TTL property |
| Partition pre-creation | Yes (`end` offset) | No (on-demand only) |
| Multi-column | No | Yes |
| Late-arriving data | Creates new partition or fails if outside range | Always creates as needed |
| Minimum version | All versions | 3.0+ |
| Preferred use case | Strict retention/lifecycle automation | Flexible ingestion, multi-column partitioning |

For strict TTL enforcement (auto-drop old partitions), dynamic partitioning or a manual `DROP PARTITION` job is required even when using expression partitioning. Expression partitioning alone does not drop old partitions.

---

## 5. LIST Partitioning

LIST partitioning assigns rows to partitions based on membership in a discrete value set rather than an ordered range.

### Single-Column LIST

```sql
CREATE TABLE regional_sales (
    sale_id    BIGINT       NOT NULL,
    region     VARCHAR(32)  NOT NULL,
    sale_date  DATE         NOT NULL,
    amount     DECIMAL(18,2)
)
ENGINE = OLAP
DUPLICATE KEY(sale_id, region)
PARTITION BY LIST(region) (
    PARTITION p_americas VALUES IN ('us', 'ca', 'br', 'mx'),
    PARTITION p_emea     VALUES IN ('uk', 'de', 'fr', 'nl', 'za'),
    PARTITION p_apac     VALUES IN ('jp', 'cn', 'sg', 'au', 'in')
)
DISTRIBUTED BY HASH(sale_id) BUCKETS 8
PROPERTIES ("replication_num" = "3");
```

A row with `region = 'us'` goes to `p_americas`. A row with `region = 'xx'` (not listed) is rejected with an error unless a catch-all partition is added (`DEFAULT` keyword is not supported in StarRocks 3.x; add regions explicitly or use an `OTHER` partition with the remaining values).

### Multi-Column LIST Partitioning

```sql
CREATE TABLE product_inventory (
    product_id  BIGINT      NOT NULL,
    warehouse   VARCHAR(32) NOT NULL,
    category    VARCHAR(32) NOT NULL,
    stock_qty   INT
)
ENGINE = OLAP
DUPLICATE KEY(product_id)
PARTITION BY LIST(warehouse, category) (
    PARTITION p_nyc_elec   VALUES IN (('nyc', 'electronics'), ('nyc', 'appliances')),
    PARTITION p_nyc_other  VALUES IN (('nyc', 'clothing'),    ('nyc', 'furniture')),
    PARTITION p_la_elec    VALUES IN (('la',  'electronics'), ('la',  'appliances')),
    PARTITION p_la_other   VALUES IN (('la',  'clothing'),    ('la',  'furniture'))
)
DISTRIBUTED BY HASH(product_id) BUCKETS 4
PROPERTIES ("replication_num" = "3");
```

Multi-column LIST partitioning is supported in StarRocks 3.1+. Each tuple in `VALUES IN (...)` is evaluated as a compound key.

---

## 6. Partition Management

### SHOW PARTITIONS

```sql
SHOW PARTITIONS FROM user_events;
```

Key columns returned:

| Column | Description |
|---|---|
| `PartitionId` | Internal partition numeric ID |
| `PartitionName` | Name declared in DDL or auto-generated |
| `VisibleVersion` | Latest committed version (data version) |
| `State` | `NORMAL`, `SCHEMA_CHANGE`, `ROLLUP` |
| `PartitionKey` | Partition column name(s) |
| `Range` | Partition boundary, e.g., `[2024-01-01, 2024-01-02)` |
| `DistributionKey` | Bucketing column(s) |
| `Buckets` | Number of buckets in this partition |
| `ReplicationNum` | Current replication factor |
| `StorageMedium` | `SSD` or `HDD` |
| `CooldownTime` | Cooldown timestamp or `9999-12-31 23:59:59` if none |
| `LastConsistencyCheckTime` | Tablet consistency check time |
| `DataSize` | Approximate compressed data size |
| `RowCount` | Approximate row count |

Filter for a specific partition range:

```sql
SHOW PARTITIONS FROM user_events
WHERE PartitionName LIKE 'p2024%'
ORDER BY PartitionName;
```

### ADD PARTITION — RANGE

```sql
-- Add a single future partition manually
ALTER TABLE events
ADD PARTITION p20240201
VALUES LESS THAN ('2024-02-02')
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES (
    "replication_num"  = "3",
    "storage_medium"   = "SSD"
);
```

Batch-add via START ... END ... EVERY:

```sql
ALTER TABLE events
ADD PARTITIONS
START ("2024-02-01") END ("2024-03-01") EVERY (INTERVAL 1 DAY)
DISTRIBUTED BY HASH(user_id) BUCKETS 16;
```

### ADD PARTITION — LIST

```sql
ALTER TABLE regional_sales
ADD PARTITION p_mena
VALUES IN ('sa', 'ae', 'eg', 'il')
DISTRIBUTED BY HASH(sale_id) BUCKETS 4
PROPERTIES ("replication_num" = "3");
```

### DROP PARTITION

```sql
-- Drops the partition AND its data (default)
ALTER TABLE events DROP PARTITION p20240101;

-- Keep the partition data as a detached backup (not loaded back automatically)
ALTER TABLE events DROP PARTITION p20240101 FORCE;
```

`DROP PARTITION` without `FORCE` moves data to a recycle bin with a retention window (controlled by FE config `catalog_trash_expire_second`, default 86400 seconds). Within that window you can recover with `RECOVER PARTITION`. After expiry the data is permanently deleted.

### TRUNCATE PARTITION

Remove all data from a partition without dropping the partition definition:

```sql
-- Truncate a single partition
ALTER TABLE events TRUNCATE PARTITION p20240101;

-- Truncate multiple partitions at once
ALTER TABLE events TRUNCATE PARTITION p20240101, p20240102, p20240103;
```

Use TRUNCATE when you want to reload a partition (e.g., reprocess a day's data) but keep the partition structure intact.

### RENAME PARTITION

```sql
ALTER TABLE events RENAME PARTITION p_future p_overflow;
```

---

## 7. Partition Pruning Verification

### Reading EXPLAIN Output

```sql
EXPLAIN SELECT count(*), event_type
FROM user_events
WHERE event_date = '2024-01-15'
GROUP BY event_type;
```

Relevant fragment in the plan output:

```
SCAN [user_events]
  partitions=1/92
  tabletRatio=16/1472
  ...
```

- `partitions=1/92` means 1 out of 92 total partitions will be scanned. Pruning is working.
- `tabletRatio=16/1472` means 16 tablets (1 partition × 16 buckets) out of 1472 total tablets.

For a date range query:

```sql
EXPLAIN SELECT *
FROM user_events
WHERE event_date BETWEEN '2024-01-01' AND '2024-01-07';
```

Expected: `partitions=7/92`.

### When Pruning Does NOT Occur

**Case 1 — Function wrapping the partition column**

```sql
-- BAD: wraps partition column in a function → full scan, partitions=92/92
WHERE DATE_FORMAT(event_date, '%Y-%m') = '2024-01'

-- GOOD: filter directly on the partition column
WHERE event_date >= '2024-01-01' AND event_date < '2024-02-01'
```

**Case 2 — Implicit type cast**

```sql
-- BAD: event_date is DATE, but comparison with integer causes implicit cast
WHERE event_date = 20240101

-- GOOD: use the correct literal type
WHERE event_date = '2024-01-01'
```

**Case 3 — Subquery or JOIN on the partition column**

```sql
-- BAD: the planner cannot resolve the predicate at compile time
WHERE event_date IN (SELECT dt FROM date_ranges WHERE flag = 1)
```

Rewrite using a literal list or a constant expression the optimizer can fold at plan time.

**Case 4 — OR across different partitions with non-partition column**

```sql
-- BAD: the OR condition involves a non-partition column, disabling pruning
WHERE event_date = '2024-01-01' OR user_id = 12345
```

The planner must conservatively scan all partitions because `user_id = 12345` could exist in any partition.

### Using EXPLAIN VERBOSE for Partition Detail

```sql
EXPLAIN VERBOSE SELECT count(*)
FROM user_events
WHERE event_date = '2024-01-15';
```

`EXPLAIN VERBOSE` includes the resolved partition list and predicate evaluation details.

---

## 8. Hot/Cold Storage Tiering

### Setting Storage Medium at Table Level

```sql
CREATE TABLE hot_events (
    ts      DATETIME NOT NULL,
    user_id BIGINT,
    action  VARCHAR(64)
)
ENGINE = OLAP
DUPLICATE KEY(ts, user_id)
PARTITION BY RANGE(ts) (
    START ("2024-01-01") END ("2025-01-01") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "storage_medium"  = "SSD"    -- default medium for all new partitions
);
```

### Per-Partition Storage Medium at CREATE Time

```sql
CREATE TABLE tiered_metrics (
    ts      DATE     NOT NULL,
    host    VARCHAR(128),
    value   DOUBLE
)
ENGINE = OLAP
DUPLICATE KEY(ts, host)
PARTITION BY RANGE(ts) (
    PARTITION p_hot   VALUES LESS THAN ('2024-02-01')
        ("storage_medium" = "SSD"),
    PARTITION p_warm  VALUES LESS THAN ('2023-08-01')
        ("storage_medium" = "SSD"),
    PARTITION p_cold1 VALUES LESS THAN ('2023-02-01')
        ("storage_medium" = "HDD"),
    PARTITION p_cold2 VALUES LESS THAN ('2022-08-01')
        ("storage_medium" = "HDD")
)
DISTRIBUTED BY HASH(host) BUCKETS 8
PROPERTIES ("replication_num" = "3");
```

### Moving a Partition to Cold Storage

```sql
ALTER TABLE tiered_metrics
MODIFY PARTITION p_warm
SET (
    "storage_medium" = "HDD"
);
```

This triggers a background tablet migration from SSD to HDD. Migration is online — queries continue to run. Monitor progress via `SHOW TABLET` or BE metrics.

### Automatic Cooling via storage_cooldown_time

Set a future timestamp at which the partition will be automatically migrated from SSD to HDD:

```sql
-- At table creation
CREATE TABLE events_tiered (
    event_date DATE NOT NULL,
    ...
)
...
PARTITION BY RANGE(event_date) (
    PARTITION p20240101 VALUES LESS THAN ('2024-01-02')
        ("storage_medium" = "SSD", "storage_cooldown_time" = "2024-04-01 00:00:00"),
    PARTITION p20240102 VALUES LESS THAN ('2024-01-03')
        ("storage_medium" = "SSD", "storage_cooldown_time" = "2024-04-02 00:00:00")
)
...
```

Or update an existing partition's cooldown time:

```sql
ALTER TABLE events_tiered
MODIFY PARTITION p20240101
SET (
    "storage_cooldown_time" = "2024-04-01 00:00:00"
);
```

After the cooldown time passes, the StarRocks FE scheduler automatically moves the partition's tablets to HDD. Set `storage_cooldown_time` 90 days after the partition's start date for a typical hot/warm SLA.

### Combining Dynamic Partitioning with Tiering

New partitions created by the dynamic partition scheduler inherit the table-level `storage_medium`. To apply per-partition cooldown, set the table default and then update individual partitions, or use a periodic Airflow/cron job:

```sql
-- Periodic job: cool down partitions older than 30 days
ALTER TABLE user_events
MODIFY PARTITION p20240101
SET (
    "storage_medium"        = "HDD",
    "storage_cooldown_time" = "9999-12-31 23:59:59"   -- already cool, disable future re-check
);
```

---

## 9. Partition Strategy Decision Matrix

| Data Volume per Day | Query Scan Range | Retention | Recommended Granularity | Approach |
|---|---|---|---|---|
| < 100 MB | Weeks to months | 1–3 years | Monthly (`INTERVAL 1 MONTH`) | Manual RANGE or dynamic MONTH |
| 100 MB – 10 GB | Days to weeks | 90–365 days | Daily (`INTERVAL 1 DAY`) | Dynamic DAY or expression `date_trunc('day', …)` |
| 10 GB – 1 TB | Hours to days | 30–90 days | Daily with hourly buckets | Dynamic DAY + high bucket count |
| > 1 TB | Hours | 7–30 days | Hourly (`HOUR`) | Dynamic HOUR with careful bucket tuning |
| Discrete categories, not time | Per-category | Indefinite | LIST by category | LIST partitioning |
| Multiple ordered dims | Both dims in filters | Varies | Multi-column LIST or expression | Expression (3.0+) or multi-column LIST |

**Time column type recommendations:**
- Use `DATE` for day-granularity partitions (smaller storage, clean range boundaries).
- Use `DATETIME` when queries also filter by time-of-day or you need hourly granularity.
- Do not use `VARCHAR` or `INT` for time columns intended for partition pruning — the planner will not prune correctly for RANGE partitions with string types (except quoted date literals).

---

## 10. Complete Production Examples

### Example A — Daily Events Table with Dynamic Partitioning

```sql
CREATE TABLE app_events (
    event_id     BIGINT        NOT NULL COMMENT 'Unique event identifier',
    event_date   DATE          NOT NULL COMMENT 'Partition column — event calendar date',
    event_ts     DATETIME      NOT NULL COMMENT 'Full event timestamp',
    user_id      BIGINT        NOT NULL COMMENT 'User who triggered the event',
    session_id   VARCHAR(64)   COMMENT 'Browser/app session',
    event_type   VARCHAR(64)   NOT NULL COMMENT 'click, page_view, purchase, …',
    properties   JSON          COMMENT 'Arbitrary event properties',
    ingested_at  DATETIME      DEFAULT CURRENT_TIMESTAMP COMMENT 'Load timestamp'
)
ENGINE = OLAP
DUPLICATE KEY(event_id, event_date)
COMMENT 'Raw application event stream, retained 90 days'
PARTITION BY RANGE(event_date) ()          -- dynamic partition scheduler owns all partitions
DISTRIBUTED BY HASH(user_id) BUCKETS 16
ORDER BY (event_type, user_id)             -- sort key for Colocate/prefix pruning
PROPERTIES (
    "replication_num"                         = "3",
    "storage_medium"                          = "SSD",
    -- Dynamic partitioning
    "dynamic_partition.enable"                = "true",
    "dynamic_partition.time_unit"             = "DAY",
    "dynamic_partition.start"                 = "-90",
    "dynamic_partition.end"                   = "3",
    "dynamic_partition.prefix"                = "p",
    "dynamic_partition.buckets"               = "16",
    "dynamic_partition.replication_num"       = "3",
    "dynamic_partition.history_partition_num" = "30",
    "dynamic_partition.time_zone"             = "UTC"
);
```

Verify scheduler health after creation:

```sql
SHOW DYNAMIC PARTITION TABLES\G
```

Check partition list:

```sql
SHOW PARTITIONS FROM app_events
ORDER BY PartitionName DESC
LIMIT 10;
```

Confirm pruning on a common query pattern:

```sql
EXPLAIN SELECT event_type, count(*) AS cnt
FROM app_events
WHERE event_date = CURDATE()
GROUP BY event_type
ORDER BY cnt DESC
LIMIT 20;
-- Expect: partitions=1/N
```

### Example B — Same Table with Expression Partitioning (StarRocks 3.0+)

```sql
CREATE TABLE app_events_expr (
    event_id     BIGINT       NOT NULL,
    event_ts     DATETIME     NOT NULL COMMENT 'Source of partitioning via date_trunc',
    user_id      BIGINT       NOT NULL,
    session_id   VARCHAR(64),
    event_type   VARCHAR(64)  NOT NULL,
    properties   JSON
)
ENGINE = OLAP
DUPLICATE KEY(event_id, event_ts)
PARTITION BY date_trunc('day', event_ts)   -- auto-creates partition on INSERT
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "storage_medium"  = "SSD"
);
```

Expression partitioning does not require a separate DATE column alongside DATETIME — partition boundaries are derived from the expression at insert time. This simplifies the schema but requires a manual TTL strategy:

```sql
-- Weekly cleanup job: drop partitions older than 90 days
ALTER TABLE app_events_expr
DROP PARTITION p20240101;   -- replace with dynamically computed name in automation
```

### Example C — Monthly Partitioned Fact Table with Hot/Cold Tiering

```sql
CREATE TABLE orders_fact (
    order_id      BIGINT         NOT NULL,
    order_month   DATE           NOT NULL COMMENT 'First day of order month, partition col',
    customer_id   BIGINT         NOT NULL,
    product_id    BIGINT         NOT NULL,
    quantity      INT,
    unit_price    DECIMAL(18,4),
    gross_revenue DECIMAL(18,4),
    discount      DECIMAL(18,4),
    net_revenue   DECIMAL(18,4)
)
ENGINE = OLAP
AGGREGATE KEY(order_id, order_month, customer_id, product_id)
PARTITION BY RANGE(order_month) (
    START ("2021-01-01") END ("2024-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(customer_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "storage_medium"  = "SSD"
);

-- After creation: cool down historical partitions
-- (run as a one-time migration or periodic job)
ALTER TABLE orders_fact MODIFY PARTITION p202101 SET ("storage_medium" = "HDD");
ALTER TABLE orders_fact MODIFY PARTITION p202102 SET ("storage_medium" = "HDD");
-- ... continue for all partitions older than 6 months
```

---

## 11. Anti-Patterns

### Too Many Small Partitions

```sql
-- BAD: hourly partitions on a table ingesting only 10 MB/hour
-- Results in thousands of tiny tablets; compaction and metadata overhead dominate
"dynamic_partition.time_unit" = "HOUR"

-- GOOD: use daily partitions; rely on sort key for hour-level pruning within partition
"dynamic_partition.time_unit" = "DAY"
-- Plus ORDER BY (hour_column, user_id) in the table DDL
```

A healthy tablet size target is 100 MB – 1 GB of compressed data. If your largest partition has < 10 MB after a week, move to a coarser granularity.

### Not Using Partition Pruning

```sql
-- BAD: no partition filter → full scan every time
SELECT * FROM events WHERE user_id = 12345;

-- GOOD: always include the partition column in WHERE when querying large ranges
SELECT * FROM events WHERE event_date = '2024-01-15' AND user_id = 12345;
```

Ensure application code and BI tools always pass the partition column predicate. Without it, every query becomes a full table scan.

### Wrapping the Partition Column in a Function

```sql
-- BAD: YEAR(event_date) = 2024 cannot be used for RANGE pruning
WHERE YEAR(event_date) = 2024

-- GOOD: rewrite as a range filter on the partition column directly
WHERE event_date >= '2024-01-01' AND event_date < '2025-01-01'
```

### Wrong Column Type for the Partition Column

```sql
-- BAD: VARCHAR partition column — string-sorted RANGE is error-prone and prunes poorly
PARTITION BY RANGE(event_date_str) (
    PARTITION p_jan VALUES LESS THAN ('2024-02-01')   -- '2024-01-31' < '2024-02-01' OK,
                                                       -- but '2024-1-5' > '2024-02-01' is wrong
)

-- GOOD: use DATE or DATETIME for time-series partition columns
event_date DATE NOT NULL
PARTITION BY RANGE(event_date) (...)
```

Always declare time partition columns as `DATE` or `DATETIME`. Never use `VARCHAR` or `BIGINT` (YYYYMMDD integers) for partition columns — RANGE boundary evaluation is string/numeric, not calendar-aware.

### Using a Non-Partition Column in the Partition DDL

```sql
-- BAD: user_id is not the partition column
PARTITION BY RANGE(user_id) (
    PARTITION p1 VALUES LESS THAN (1000000),
    PARTITION p2 VALUES LESS THAN (2000000)
)
-- Queries with WHERE event_date = '...' will scan ALL partitions
```

Partition columns should be the most common filter dimension in production queries. Partitioning by a low-cardinality high-selectivity column (like user_id range) instead of time means time-based queries lose all partition elimination.

### Forgetting MAXVALUE on Manual RANGE Tables

```sql
-- BAD: no catch-all partition → inserts with dt >= '2024-01-06' fail with "No partition found"
PARTITION BY RANGE(dt) (
    PARTITION p20240101 VALUES LESS THAN ('2024-01-02'),
    PARTITION p20240105 VALUES LESS THAN ('2024-01-06')
    -- no MAXVALUE partition
)

-- GOOD: always add a MAXVALUE partition on manual tables
PARTITION p_future VALUES LESS THAN (MAXVALUE)
```

Dynamic and expression partitioning do not need MAXVALUE because they create partitions on demand. Manual RANGE tables without MAXVALUE will reject out-of-range inserts.

---

## 12. References

- StarRocks documentation: Table Design — Partitioning
- StarRocks documentation: Dynamic Partitioning
- StarRocks documentation: Expression Partitioning (3.0+)
- StarRocks documentation: ALTER TABLE — Partition Operations
- StarRocks documentation: EXPLAIN Statement
- StarRocks documentation: Storage Medium and Cooldown
- Companion skills: `starrocks-bucketing` (bucket count tuning), `starrocks-data-modeling` (table type and key selection)
