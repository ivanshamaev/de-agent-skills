---
name: starrocks-realtime-modeling
description: StarRocks real-time data modeling — Primary Key table streaming upserts (Flink/Kafka Routine Load), mutable dimension tables (partial column UPDATE), real-time pre-aggregation with Aggregate Key + streaming inserts, Routine Load for Kafka CDC, real-time materialized view refresh, low-latency BI patterns, latency vs consistency trade-offs
---

# StarRocks — Real-Time Data Modeling

## When to Use

Load this skill when the user needs to:
- Build sub-minute latency BI pipelines with StarRocks as the serving layer
- Ingest Kafka topics directly into StarRocks via Routine Load
- Stream data from Flink into StarRocks using the Flink connector
- Propagate CDC changes (Debezium, Flink CDC) into mutable StarRocks tables
- Implement partial column updates on dimension or fact tables
- Design Aggregate Key tables for streaming counters and real-time pre-aggregation
- Configure asynchronous materialized views with short refresh intervals for near-real-time dashboards
- Tune commit intervals, checkpoint intervals, and MV refresh cadence for a freshness SLA

---

## Architecture Patterns

Three canonical patterns cover the majority of real-time StarRocks use cases. Choose based on your data source, latency target, and operational complexity budget.

### Pattern A: Kafka → Routine Load → Primary Key Table

```
Kafka Topic
    │
    │  CREATE ROUTINE LOAD
    ▼
StarRocks BE (stream reader threads)
    │
    ▼
Primary Key Table
    │
    ▼
BI / Dashboard Query
```

- **Latency**: 5–30 seconds (driven by `max_batch_interval_s`).
- **Complexity**: Low. Entirely managed inside StarRocks; no external compute cluster needed.
- **Best for**: Simple JSON or CSV Kafka topics where the message structure maps cleanly to a StarRocks table.
- **Limitations**: No stateful transformations; limited filtering (WHERE clause only); schema must match Kafka payload closely.

### Pattern B: Kafka → Flink → StarRocks Flink Connector

```
Kafka Topic
    │
    │  Flink Source (FlinkKafkaConsumer / KafkaSource)
    ▼
Flink Streaming Job
  (enrichment, deduplication, joins, transformations)
    │
    │  StarRocks Flink Connector Sink
    ▼
StarRocks Primary Key / Aggregate Key Table
    │
    ▼
BI / Dashboard Query
```

- **Latency**: 10–60 seconds (driven by Flink checkpoint interval + StarRocks stream load buffer flush).
- **Complexity**: Medium-High. Requires a Flink cluster and job management.
- **Best for**: Pipelines that need stateful processing (windowed aggregations, joins, deduplication) before writing to StarRocks.
- **Exactly-once** requires `sink.semantic=exactly-once` + Flink checkpointing enabled.

### Pattern C: CDC (Debezium) → Flink CDC → Primary Key Table

```
OLTP Database (MySQL / PostgreSQL)
    │
    │  Debezium connector → Kafka
    │    OR flink-cdc-connector (direct, no Kafka)
    ▼
Flink CDC Source
    │
    │  Upsert / Delete handling in Flink
    │  StarRocks Flink Connector Sink (upsert mode)
    ▼
StarRocks Primary Key Table
    │
    ▼
BI / Dashboard Query
```

- **Latency**: 5–30 seconds from OLTP commit to StarRocks visibility.
- **Complexity**: Medium. Flink CDC connectors handle snapshot + binlog/WAL automatically.
- **Best for**: Keeping an OLAP copy of an OLTP table in sync, enabling analytics on live transactional data.
- **Delete handling**: Flink CDC emits DELETE events; the StarRocks Flink connector in upsert mode converts them to `DELETE` operations on the Primary Key table.

---

## Primary Key Table for Real-Time Upserts

The Primary Key table model is the correct choice for any mutable data in StarRocks. It maintains a delete-bitmap on write so queries read merged data without extra merge work at scan time.

### DDL

```sql
CREATE TABLE orders (
    order_id        BIGINT          NOT NULL  COMMENT "Primary key",
    customer_id     INT             NOT NULL,
    product_id      INT             NOT NULL,
    order_status    VARCHAR(32)     NOT NULL  DEFAULT 'PENDING'
                                              COMMENT "PENDING/CONFIRMED/SHIPPED/DELIVERED/CANCELLED",
    order_ts        DATETIME        NOT NULL,
    update_ts       DATETIME        NOT NULL  COMMENT "Last update timestamp from source",
    amount          DECIMAL(18, 2)  NOT NULL,
    quantity        INT             NOT NULL,
    region          VARCHAR(64)     NOT NULL
)
PRIMARY KEY (order_id)
PARTITION BY RANGE(order_ts) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(order_id) BUCKETS 32
ORDER BY (order_id, region)
PROPERTIES (
    "replication_num"           = "3",
    "enable_persistent_index"   = "true",    -- required for large PK tables; keeps index on disk
    "persistent_index_type"     = "LOCAL",   -- LOCAL = per-BE SSD; CLOUD for shared storage
    "storage_medium"            = "SSD",
    "compression"               = "LZ4"
);
```

**Bucket count guidance for concurrent writes:**
- Each Routine Load task or Flink task writes to tablets in parallel.
- Rule: `BUCKETS = max(32, ceil(total_daily_rows / 10_000_000))`.
- For very high-throughput topics (> 500k rows/s), use 64–128 buckets and ensure `desired_concurrent_number` in Routine Load matches bucket layout.

**`enable_persistent_index=true`**: Stores the primary key index on disk (SSD preferred) rather than purely in memory. Mandatory for tables with > ~100M rows. Without it, StarRocks holds the entire PK index in BE memory — on large tables this causes OOM and write stalls.

### Partial Column UPDATE Syntax

StarRocks 3.x supports partial-column UPDATE for Primary Key tables, enabling targeted field mutations without re-writing the full row.

```sql
-- Update a single column
UPDATE orders
SET    order_status = 'SHIPPED'
WHERE  order_id = 123456789;

-- Update multiple columns atomically
UPDATE orders
SET    order_status = 'DELIVERED',
       update_ts    = '2026-05-17 14:30:00'
WHERE  order_id = 123456789;

-- Batch update from a subquery (e.g., applying CDC changes)
UPDATE orders tgt
SET    tgt.order_status = src.new_status,
       tgt.update_ts    = src.changed_at
FROM   order_status_changes src
WHERE  tgt.order_id = src.order_id;
```

For stream ingestion, partial-column update is configured at the load level using `partial_update` property (Stream Load / Flink connector). This avoids fetching existing rows during write — the engine applies column-level merge internally using the stored delete bitmap.

### INSERT ON DUPLICATE KEY UPDATE

```sql
-- Upsert a single row
INSERT INTO orders (order_id, order_status, update_ts)
VALUES (123456789, 'SHIPPED', '2026-05-17 14:30:00')
ON DUPLICATE KEY UPDATE
    order_status = VALUES(order_status),
    update_ts    = VALUES(update_ts);
```

Use this pattern for small-scale CDC replay or backfill scripts. For streaming ingestion at scale, prefer Routine Load or Flink connector — `INSERT ... ON DUPLICATE KEY UPDATE` incurs per-statement FE overhead and is not suitable for high-throughput writes.

---

## Routine Load for Kafka

Routine Load is StarRocks' built-in Kafka consumer. It runs persistent background reader tasks on BE nodes and performs micro-batch Stream Load commits at a configurable interval.

### Full CREATE ROUTINE LOAD Syntax

```sql
-- JSON Kafka topic: orders events
CREATE ROUTINE LOAD sales_db.rl_orders ON orders
COLUMNS TERMINATED BY ","            -- ignored for JSON FORMAT
COLUMNS (
    order_id,
    customer_id,
    product_id,
    order_status,
    order_ts,
    update_ts,
    amount,
    quantity,
    region
)
WHERE order_id IS NOT NULL           -- discard malformed rows without an order_id
PARTITION (
    kafka_partition_1,
    kafka_partition_2,
    kafka_partition_3
)                                    -- optional: pin specific partitions
PROPERTIES (
    "desired_concurrent_number"  = "8",       -- parallel reader tasks per BE; should not exceed partition count
    "max_batch_interval_s"       = "10",      -- commit every 10 seconds (latency floor)
    "max_batch_rows"             = "300000",  -- or commit when this many rows accumulated
    "max_batch_size"             = "209715200",-- 200 MB max per micro-batch
    "max_error_number"           = "1000",    -- tolerate up to 1000 parse errors before pausing
    "max_filter_ratio"           = "0.01",    -- pause if > 1% of rows fail to parse
    "strict_mode"                = "false",   -- false = coerce types; true = reject type mismatches
    "timezone"                   = "UTC",
    "format"                     = "json",
    "jsonpaths"                  = "[\"$.order_id\",\"$.customer_id\",\"$.product_id\",\"$.order_status\",\"$.order_ts\",\"$.update_ts\",\"$.amount\",\"$.quantity\",\"$.region\"]",
    "json_root"                  = "$.payload",  -- set if message has an envelope (e.g., Debezium)
    "strip_outer_array"          = "false"
)
FROM KAFKA (
    "kafka_broker_list"          = "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
    "kafka_topic"                = "orders.events",
    "kafka_partitions"           = "0,1,2,3,4,5,6,7",
    "kafka_offsets"              = "OFFSET_BEGINNING",   -- or specific offsets, or OFFSET_END
    "property.group.id"          = "starrocks_rl_orders",
    "property.security.protocol" = "SASL_SSL",           -- omit if no auth
    "property.sasl.mechanism"    = "PLAIN",
    "property.sasl.username"     = "kafka_user",
    "property.sasl.password"     = "kafka_password"
);
```

#### Loading CSV Topics

```sql
CREATE ROUTINE LOAD sales_db.rl_orders_csv ON orders
COLUMNS TERMINATED BY ","
COLUMNS (
    order_id,
    customer_id,
    product_id,
    order_status,
    order_ts,
    update_ts,
    amount,
    quantity,
    region
)
PROPERTIES (
    "desired_concurrent_number" = "4",
    "max_batch_interval_s"      = "15",
    "max_error_number"          = "500",
    "max_filter_ratio"          = "0.005",
    "format"                    = "csv"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka-broker-1:9092",
    "kafka_topic"       = "orders.csv",
    "kafka_offsets"     = "OFFSET_BEGINNING"
);
```

### Monitoring Routine Load

```sql
-- Overview: State, current lag, throughput, error counts
SHOW ROUTINE LOAD FOR rl_orders\G

-- Key fields to monitor:
--   State             : RUNNING | PAUSED | NEED_SCHEDULE | STOPPED
--   Statistics        : {"receivedBytes":..., "errorRows":..., "loadedRows":...}
--   Progress          : {"0":"12345","1":"12346", ...}  — per-partition offset
--   Lag               : offset lag per partition
--   ReasonOfStateChanged: populated on PAUSED state

-- List all Routine Load jobs in database
SHOW ALL ROUTINE LOAD;

-- Check individual task execution details
SHOW ROUTINE LOAD TASK WHERE JobName = 'rl_orders';
```

### Lifecycle Management

```sql
-- Pause a running job (e.g., during schema change)
PAUSE ROUTINE LOAD FOR rl_orders;

-- Resume after fixing the underlying issue
RESUME ROUTINE LOAD FOR rl_orders;

-- Permanently stop (cannot be restarted; creates a new job to restart)
STOP ROUTINE LOAD FOR rl_orders;

-- Alter properties without stopping (supported subset of properties)
ALTER ROUTINE LOAD FOR rl_orders
PROPERTIES (
    "desired_concurrent_number" = "16",
    "max_batch_interval_s"      = "5"
);
```

### Error Investigation

When a Routine Load job transitions to `PAUSED` state:

```sql
-- Step 1: Read the pause reason
SHOW ROUTINE LOAD FOR rl_orders\G
-- Look at: ReasonOfStateChanged, ErrorLogUrls

-- Step 2: Inspect per-task error details
SHOW ROUTINE LOAD TASK WHERE JobName = 'rl_orders';

-- Step 3: Access FE error log URLs returned in ErrorLogUrls
-- These are HTTP endpoints served by FE; download with curl:
--   curl http://<fe_host>:8030/api/_load_error_log?file=<error_log_path>
```

Common error causes:
- `max_error_number` exceeded: Kafka messages are malformed or schema changed upstream.
- `max_filter_ratio` exceeded: High proportion of null primary keys or type mismatches.
- `OFFSET_OUT_OF_RANGE`: Kafka log retention deleted offsets the job had not yet consumed — reset offsets.

---

## Flink Connector for StarRocks

The StarRocks Flink connector uses the StarRocks Stream Load HTTP API internally. It buffers rows in memory, flushes to StarRocks BE on checkpoint (exactly-once) or on a time/size threshold (at-least-once).

### Maven Dependency

```xml
<dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>flink-connector-starrocks</artifactId>
    <!-- Use the version matching your Flink major version -->
    <version>1.2.9_flink-1.18</version>
</dependency>
```

### Gradle

```groovy
implementation 'com.starrocks:flink-connector-starrocks:1.2.9_flink-1.18'
```

### Flink Table API — DDL

```sql
-- Flink SQL: define StarRocks table as a Flink sink
CREATE TABLE orders_sink (
    order_id     BIGINT       NOT NULL,
    customer_id  INT          NOT NULL,
    product_id   INT          NOT NULL,
    order_status STRING,
    order_ts     TIMESTAMP(3),
    update_ts    TIMESTAMP(3),
    amount       DECIMAL(18, 2),
    quantity     INT,
    region       STRING,
    PRIMARY KEY (order_id) NOT ENFORCED   -- required for upsert mode
)
WITH (
    'connector'                            = 'starrocks',
    'jdbc-url'                             = 'jdbc:mysql://fe-host:9030',
    'load-url'                             = 'fe-host:8030;fe-host2:8030',
    'database-name'                        = 'sales_db',
    'table-name'                           = 'orders',
    'username'                             = 'flink_user',
    'password'                             = 'flink_password',
    'sink.semantic'                        = 'exactly-once',  -- requires checkpointing
    'sink.buffer-flush.max-bytes'          = '157286400',     -- 150 MB
    'sink.buffer-flush.interval-ms'        = '15000',         -- flush every 15 s if not full
    'sink.max-retries'                     = '3',
    'sink.properties.format'               = 'json',
    'sink.properties.strip_outer_array'    = 'false',
    'sink.properties.partial_update'       = 'false'          -- set true for partial-column writes
);

-- Flink SQL: upsert insert from upstream source
INSERT INTO orders_sink
SELECT
    order_id,
    customer_id,
    product_id,
    order_status,
    order_ts,
    update_ts,
    CAST(amount AS DECIMAL(18,2)),
    quantity,
    region
FROM orders_kafka_source
WHERE order_id IS NOT NULL;
```

### Flink DataStream API — Builder Pattern

```java
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;

StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
    .withProperty("jdbc-url",                        "jdbc:mysql://fe-host:9030")
    .withProperty("load-url",                        "fe-host:8030;fe-host2:8030")
    .withProperty("database-name",                   "sales_db")
    .withProperty("table-name",                      "orders")
    .withProperty("username",                        "flink_user")
    .withProperty("password",                        "flink_password")
    .withProperty("sink.semantic",                   "exactly-once")
    .withProperty("sink.buffer-flush.max-bytes",     "157286400")
    .withProperty("sink.buffer-flush.interval-ms",   "15000")
    .withProperty("sink.properties.format",          "json")
    .build();

// Schema must match the StarRocks table
TableSchema tableSchema = TableSchema.builder()
    .field("order_id",     DataTypes.BIGINT().notNull())
    .field("customer_id",  DataTypes.INT().notNull())
    .field("order_status", DataTypes.STRING())
    .field("amount",       DataTypes.DECIMAL(18, 2))
    .field("update_ts",    DataTypes.TIMESTAMP(3))
    .primaryKey("order_id")
    .build();

SinkFunction<Row> starRocksSink = StarRocksSink.sink(tableSchema, sinkOptions);

DataStream<Row> stream = ...; // your upstream DataStream
stream.addSink(starRocksSink).name("starrocks-orders-sink");
```

### Upsert Mode Configuration

When the Flink table definition has a `PRIMARY KEY NOT ENFORCED` clause and `sink.semantic=exactly-once`, the connector automatically switches to upsert mode:

- **INSERT** → Stream Load upsert (behaves as `INSERT INTO ... ON DUPLICATE KEY UPDATE` for all columns).
- **UPDATE_AFTER** (ChangelogStream) → upsert on the primary key.
- **DELETE** → physical delete from the Primary Key table (requires `sink.properties.op_ts_field` or the StarRocks side handles delete via primary key match).

For CDC streams (Debezium / Flink CDC), set `sink.semantic=exactly-once` and enable Flink checkpointing:

```java
env.enableCheckpointing(30_000L);                          // 30 s checkpoint interval
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);
env.getCheckpointConfig().setCheckpointTimeout(120_000L);
```

---

## Real-Time Aggregates

### Aggregate Key Table for Streaming Counters

Aggregate Key tables accumulate metrics automatically: each new row with the same key columns is merged with existing rows using the declared aggregate function. This enables idempotent streaming inserts — writing the same event twice produces the correct final aggregate.

```sql
CREATE TABLE realtime_order_metrics (
    metric_date      DATE          NOT NULL,
    region           VARCHAR(64)   NOT NULL,
    order_status     VARCHAR(32)   NOT NULL,
    -- Measures: SUM accumulates on each insert
    order_count      BIGINT        SUM         DEFAULT "0",
    total_amount     DECIMAL(18,2) SUM         DEFAULT "0",
    total_quantity   BIGINT        SUM         DEFAULT "0",
    -- MAX retains the latest update timestamp
    last_update_ts   DATETIME      MAX         DEFAULT "1970-01-01 00:00:00"
)
AGGREGATE KEY (metric_date, region, order_status)
PARTITION BY RANGE(metric_date) (
    START ("2025-01-01") END ("2026-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(region) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "compression"     = "LZ4"
);

-- Streaming insert: Routine Load or Flink inserts pre-computed deltas
-- Example: insert a single-order delta row
INSERT INTO realtime_order_metrics
    (metric_date, region, order_status, order_count, total_amount, total_quantity, last_update_ts)
VALUES
    ('2026-05-17', 'WEST', 'CONFIRMED', 1, 149.99, 2, '2026-05-17 10:15:00');
-- StarRocks merges this with the existing row for ('2026-05-17','WEST','CONFIRMED')
-- using SUM for order_count/total_amount/total_quantity and MAX for last_update_ts
```

**Important constraint**: Aggregate Key only supports additive mutations. You cannot correct a previously inserted delta (e.g., to reverse a cancelled order) without an equal and opposite compensating row.

### Async MV for Pre-Aggregation with Short Refresh Interval

For more flexible aggregation that supports corrections and complex logic, use an async MV over a Primary Key base table:

```sql
-- Base table: raw orders (Primary Key, mutable)
-- See the DDL in the "Primary Key Table" section above

-- Async MV: daily region-level summary, refreshed every minute
CREATE MATERIALIZED VIEW mv_orders_daily_region
REFRESH ASYNC EVERY(INTERVAL 1 MINUTE)
PARTITION BY (metric_date)
DISTRIBUTED BY HASH(region) BUCKETS 16
AS
SELECT
    DATE(order_ts)          AS metric_date,
    region,
    order_status,
    COUNT(*)                AS order_count,
    SUM(amount)             AS total_amount,
    SUM(quantity)           AS total_quantity,
    MAX(update_ts)          AS last_update_ts
FROM orders
GROUP BY
    DATE(order_ts),
    region,
    order_status;
```

Monitor MV refresh status:

```sql
SHOW MATERIALIZED VIEWS LIKE 'mv_orders_daily_region'\G
-- Key fields:
--   last_refresh_time       : when the last refresh completed
--   last_refresh_state      : SUCCESS | FAILED | PENDING
--   inactive_reason         : populated if MV is in INACTIVE state
--   rows                    : approximate row count in MV
```

Force a manual refresh:

```sql
REFRESH MATERIALIZED VIEW mv_orders_daily_region;
```

Inspect whether a query rewrites to the MV:

```sql
EXPLAIN SELECT metric_date, region, SUM(total_amount)
FROM mv_orders_daily_region
GROUP BY metric_date, region;
-- Or for transparent rewrite from the base table:
EXPLAIN SELECT DATE(order_ts), region, SUM(amount)
FROM orders
GROUP BY DATE(order_ts), region;
```

### Trade-off: Aggregate Key vs Async MV

| Dimension | Aggregate Key Table | Async MV over Primary Key |
|---|---|---|
| Latency to visibility | Immediate (merge on write) | Up to `INTERVAL` delay (1 min minimum) |
| Schema flexibility | Fixed at table creation; hard to change aggregation logic | MV can be dropped and recreated; base table unchanged |
| Corrections/reversals | Requires compensating delta rows; complex | Base table UPDATE propagates on next MV refresh |
| Multi-table joins in agg | Not supported | Fully supported |
| DELETE support | Not supported (no subtraction) | Supported via base table DELETE + refresh |
| Query rewrite | Automatic via synchronous rollup | Automatic (requires `enable_materialized_view_rewrite=true`) |
| Best for | High-throughput append-only counters | Mutable data, complex aggregations, dashboard pre-computation |

---

## Mutable Dimension Pattern

Dimensions in a real-time pipeline change frequently (user profile updates, product price changes, category reassignments). Use a Primary Key table to keep dimensions current.

### Dimension Table DDL

```sql
CREATE TABLE dim_customers (
    customer_id      INT           NOT NULL  COMMENT "Natural key from CRM",
    email            VARCHAR(255)  NOT NULL,
    full_name        VARCHAR(255)  NOT NULL,
    segment          VARCHAR(64)   NOT NULL  DEFAULT 'STANDARD'
                                             COMMENT "STANDARD / PREMIUM / VIP",
    country_code     CHAR(2)       NOT NULL,
    city             VARCHAR(128)  NOT NULL  DEFAULT '',
    is_active        BOOLEAN       NOT NULL  DEFAULT TRUE,
    created_at       DATETIME      NOT NULL,
    updated_at       DATETIME      NOT NULL
)
PRIMARY KEY (customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 16
ORDER BY (customer_id, country_code)
PROPERTIES (
    "replication_num"         = "3",
    "enable_persistent_index" = "true",
    "compression"             = "LZ4"
);
```

### Full Row Replacement vs Partial UPDATE

**Full row replacement** (default upsert): The CDC source emits the complete after-image of the row. This is the simplest approach and requires no special Stream Load options.

```sql
-- Full row upsert from Routine Load or Flink: no special config; just insert the after-image row
INSERT INTO dim_customers
VALUES (42, 'alice@example.com', 'Alice Smith', 'VIP', 'US', 'San Francisco', TRUE,
        '2024-01-15 09:00:00', '2026-05-17 10:00:00');
```

**Partial UPDATE**: Use when only specific fields changed and the Kafka message contains only the changed fields (common with Debezium or MongoDB CDC). Requires Stream Load `partial_update=true` header or Flink connector `sink.properties.partial_update=true`.

```sql
-- SQL-level partial update: change only segment and updated_at
UPDATE dim_customers
SET    segment    = 'PREMIUM',
       updated_at = '2026-05-17 11:00:00'
WHERE  customer_id = 42;
```

For Flink connector partial updates, specify the subset of columns in the Flink table DDL and enable `partial_update`:

```sql
-- Flink SQL: partial-update sink for customer segment changes only
CREATE TABLE dim_customers_segment_sink (
    customer_id  INT     NOT NULL,
    segment      STRING,
    updated_at   TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
)
WITH (
    'connector'                          = 'starrocks',
    'jdbc-url'                           = 'jdbc:mysql://fe-host:9030',
    'load-url'                           = 'fe-host:8030',
    'database-name'                      = 'sales_db',
    'table-name'                         = 'dim_customers',
    'username'                           = 'flink_user',
    'password'                           = 'flink_password',
    'sink.properties.partial_update'     = 'true',
    'sink.properties.format'             = 'json'
);
```

### Handling Deletes in CDC Stream

CDC delete events must be propagated to the StarRocks dimension table to avoid stale rows appearing in joins.

**Soft delete** (recommended for BI): Add an `is_active` flag and update it to FALSE instead of physically deleting. Queries filter on `WHERE is_active = TRUE`.

```sql
-- Soft delete via partial update
UPDATE dim_customers
SET    is_active  = FALSE,
       updated_at = NOW()
WHERE  customer_id = 42;
```

**Hard delete**: Flink CDC with the StarRocks connector in changelog mode handles DELETE events natively:

```java
// In Flink DataStream, CDC source emits RowKind.DELETE rows
// The StarRocks connector translates DELETE rows to physical deletes on the Primary Key table
// No special code needed — just ensure the schema includes all PK columns
```

For Routine Load, hard deletes cannot be expressed directly. Use a staging table + scheduled merge pattern, or switch to the Flink connector for CDC workloads that include deletes.

---

## Latency vs Consistency

Every component in the real-time pipeline introduces a latency contribution. Tune each independently to meet the end-to-end freshness SLA.

### Component Latency Budget

| Component | Controlling Parameter | Typical Range | Minimum |
|---|---|---|---|
| Routine Load commit | `max_batch_interval_s` | 5–60 s | 5 s |
| Flink checkpoint (exactly-once) | `env.enableCheckpointing(ms)` | 10–60 s | 10 s |
| Flink buffer flush (at-least-once) | `sink.buffer-flush.interval-ms` | 1–30 s | 1 s |
| Async MV refresh | `EVERY(INTERVAL N MINUTE)` | 1–60 min | 1 min |
| StarRocks compaction | Background (auto) | 0–30 s additional | n/a |

### Tuning for p99 Freshness SLA

**SLA: < 30 seconds end-to-end**

```sql
-- Routine Load: commit every 5 seconds
ALTER ROUTINE LOAD FOR rl_orders
PROPERTIES ("max_batch_interval_s" = "5", "desired_concurrent_number" = "16");
```

```java
// Flink: use at-least-once with aggressive flush
.withProperty("sink.semantic",                 "at-least-once")
.withProperty("sink.buffer-flush.interval-ms", "5000")  // 5 s
.withProperty("sink.buffer-flush.max-bytes",   "52428800") // 50 MB
```

**SLA: < 5 minutes for dashboard pre-aggregation**

```sql
-- Async MV with 1-minute refresh
CREATE MATERIALIZED VIEW mv_orders_daily_region
REFRESH ASYNC EVERY(INTERVAL 1 MINUTE)
...
```

**SLA: < 60 seconds, exactly-once, CDC**

```java
env.enableCheckpointing(30_000L);  // 30 s checkpoint interval
// StarRocks Flink connector flushes on checkpoint boundary
// Total latency ≈ checkpoint interval + StarRocks Stream Load commit time (1–3 s)
```

**Consistency vs Freshness Trade-off:**

- `exactly-once` adds checkpoint barrier latency. If your downstream can tolerate occasional duplicates (idempotent consumers on the StarRocks side via Primary Key dedup), `at-least-once` with a shorter flush interval gives lower p99 latency.
- Async MV refresh is always eventually consistent. Do not use async MVs for dashboards that require transactional consistency — query the base Primary Key table directly instead and accept higher query cost.
- Aggregate Key tables are always immediately consistent with the stream, but cannot express corrections. If your data source can emit late corrections (e.g., order amount adjustments), use a Primary Key table with an async MV on top.

---

## Production Example: Complete Real-Time Orders Pipeline

This example builds a full pipeline: Kafka → Flink CDC → StarRocks Primary Key orders table + async MV for dashboard.

### 1. StarRocks Tables

```sql
-- Primary Key orders table (Pattern C: CDC target)
CREATE TABLE orders (
    order_id        BIGINT          NOT NULL,
    customer_id     INT             NOT NULL,
    product_id      INT             NOT NULL,
    order_status    VARCHAR(32)     NOT NULL  DEFAULT 'PENDING',
    order_ts        DATETIME        NOT NULL,
    update_ts       DATETIME        NOT NULL,
    amount          DECIMAL(18, 2)  NOT NULL,
    quantity        INT             NOT NULL,
    region          VARCHAR(64)     NOT NULL,
    source_db_ts    DATETIME        NOT NULL  COMMENT "Original OLTP commit timestamp"
)
PRIMARY KEY (order_id)
PARTITION BY RANGE(order_ts) (
    START ("2025-01-01") END ("2027-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(order_id) BUCKETS 32
ORDER BY (order_id, region)
PROPERTIES (
    "replication_num"         = "3",
    "enable_persistent_index" = "true",
    "compression"             = "LZ4"
);

-- Async MV: hourly order summary by region and status (for live dashboards)
CREATE MATERIALIZED VIEW mv_orders_hourly
REFRESH ASYNC EVERY(INTERVAL 1 MINUTE)
PARTITION BY (hour_ts)
DISTRIBUTED BY HASH(region) BUCKETS 16
AS
SELECT
    DATE_TRUNC('hour', order_ts)  AS hour_ts,
    region,
    order_status,
    COUNT(*)                       AS order_count,
    SUM(amount)                    AS total_amount,
    SUM(quantity)                  AS total_quantity,
    MAX(source_db_ts)              AS max_source_db_ts
FROM orders
GROUP BY
    DATE_TRUNC('hour', order_ts),
    region,
    order_status;
```

### 2. Flink Job (Java)

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

public class OrdersCdcToStarRocks {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Exactly-once checkpointing — required for StarRocks exactly-once sink
        env.enableCheckpointing(30_000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);
        env.getCheckpointConfig().setCheckpointTimeout(120_000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setParallelism(8);

        // Flink CDC MySQL source — captures snapshot + ongoing binlog
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
            .hostname("mysql-host")
            .port(3306)
            .databaseList("commerce")
            .tableList("commerce.orders")
            .username("cdc_user")
            .password("cdc_password")
            .deserializer(new JsonDebeziumDeserializationSchema())
            .startupOptions(StartupOptions.initial())   // snapshot then streaming
            .serverId("5401-5408")                      // unique server IDs per parallel task
            .build();

        DataStream<String> cdcStream = env
            .fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql-cdc-orders")
            .setParallelism(8);

        // Transform: extract after-image fields from Debezium JSON envelope
        DataStream<Row> ordersStream = cdcStream
            .flatMap(new DebeziumOrderRowMapper())  // custom FlatMapFunction
            .returns(TypeInformation.of(Row.class));

        // StarRocks sink
        TableSchema schema = TableSchema.builder()
            .field("order_id",       DataTypes.BIGINT().notNull())
            .field("customer_id",    DataTypes.INT().notNull())
            .field("product_id",     DataTypes.INT().notNull())
            .field("order_status",   DataTypes.STRING())
            .field("order_ts",       DataTypes.TIMESTAMP(3))
            .field("update_ts",      DataTypes.TIMESTAMP(3))
            .field("amount",         DataTypes.DECIMAL(18, 2))
            .field("quantity",       DataTypes.INT())
            .field("region",         DataTypes.STRING())
            .field("source_db_ts",   DataTypes.TIMESTAMP(3))
            .primaryKey("order_id")
            .build();

        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url",                      "jdbc:mysql://sr-fe-host:9030")
            .withProperty("load-url",                      "sr-fe-host:8030;sr-fe-host2:8030")
            .withProperty("database-name",                 "sales_db")
            .withProperty("table-name",                    "orders")
            .withProperty("username",                      "flink_writer")
            .withProperty("password",                      "writer_password")
            .withProperty("sink.semantic",                 "exactly-once")
            .withProperty("sink.buffer-flush.max-bytes",   "157286400")
            .withProperty("sink.buffer-flush.interval-ms", "15000")
            .withProperty("sink.max-retries",              "3")
            .withProperty("sink.properties.format",        "json")
            .build();

        ordersStream
            .addSink(StarRocksSink.sink(schema, sinkOptions))
            .name("starrocks-orders-sink")
            .setParallelism(8);

        env.execute("orders-cdc-to-starrocks");
    }
}
```

### 3. Dashboard Query (via Async MV)

```sql
-- Transparent rewrite: StarRocks planner rewrites this to scan mv_orders_hourly
SELECT
    hour_ts,
    region,
    SUM(total_amount)  AS revenue,
    SUM(order_count)   AS orders
FROM mv_orders_hourly
WHERE hour_ts >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
  AND order_status = 'CONFIRMED'
GROUP BY hour_ts, region
ORDER BY hour_ts DESC, revenue DESC;
```

### 4. Monitoring Queries

```sql
-- Check pipeline lag: how far behind is the oldest MV refresh?
SELECT
    TABLE_NAME,
    LAST_REFRESH_TIME,
    TIMESTAMPDIFF(SECOND, LAST_REFRESH_TIME, NOW()) AS lag_seconds,
    LAST_REFRESH_STATE
FROM information_schema.materialized_views
WHERE TABLE_SCHEMA = 'sales_db';

-- Check Routine Load lag (if using Pattern A instead of Flink)
SHOW ROUTINE LOAD FOR rl_orders\G

-- Check Primary Key table compaction status
SHOW PROC '/statistic';
```

---

## Anti-Patterns

### 1. Small Batch Sizes in Routine Load or Flink Sink

**Problem**: Setting `max_batch_interval_s=1` or `sink.buffer-flush.interval-ms=500` causes hundreds of tiny Stream Load requests per second. Each Stream Load transaction has fixed overhead on the FE and BE. High transaction rates overwhelm the FE edit log and cause write amplification on BE tablets.

**Fix**: Minimum commit interval of 5 seconds for Routine Load; minimum flush interval of 5 seconds for Flink sink. Accept slightly higher latency in exchange for stable throughput.

### 2. No Persistent Index on Large Primary Key Tables

**Problem**: With `enable_persistent_index=false` (default in older StarRocks versions), the entire primary key index is held in BE memory. A table with 500M rows and 8-byte keys consumes ~4 GB of BE memory per replica. This causes OOM, BE crashes, and query timeouts under write pressure.

**Fix**: Always set `enable_persistent_index=true` for Primary Key tables expected to exceed ~50M rows. Place the StarRocks data path on SSD for acceptable index lookup performance.

### 3. Using Duplicate Key for Mutable Data

**Problem**: Duplicate Key tables do not support upsert or delete. Writing CDC updates to a Duplicate Key table produces unbounded duplicates that must be deduplicated at query time with `ROW_NUMBER()` or `QUALIFY`. Query cost grows linearly with update frequency.

**Fix**: Use Primary Key table for any data that has updates or deletes in the source. Use Duplicate Key only for append-only immutable facts (event logs, raw sensor data, etc.).

### 4. Relying on Async MV for Transactional Consistency

**Problem**: An async MV with `EVERY(INTERVAL 1 MINUTE)` can lag by up to one refresh interval. If a BI user queries the MV immediately after a critical order update, they may see stale data. Using MVs for financial reconciliation or audit queries is incorrect.

**Fix**: Query the base Primary Key table directly for consistency-critical queries. Use async MVs only for dashboard aggregations where a minute of lag is acceptable.

### 5. Setting `desired_concurrent_number` Higher Than Kafka Partition Count

**Problem**: Routine Load creates one reader task per concurrent slot. Tasks assigned to partitions that do not exist idle and consume FE scheduling resources without contributing throughput.

**Fix**: `desired_concurrent_number` must be less than or equal to the number of Kafka partitions. A value equal to the partition count is optimal for maximum parallelism.

### 6. Not Monitoring Routine Load Consumer Lag

**Problem**: If the Kafka producer throughput spikes (e.g., a batch backfill), Routine Load may fall behind without triggering any alerts. The job continues running in `RUNNING` state but with growing lag.

**Fix**: Export `SHOW ROUTINE LOAD` output to a monitoring system. Alert when per-partition lag exceeds a threshold (e.g., > 100k messages or > 5 minutes of wall-clock lag).

---

## References to Consult When Needed

- StarRocks documentation: Primary Key table — https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/
- StarRocks documentation: Routine Load — https://docs.starrocks.io/docs/loading/RoutineLoad/
- StarRocks documentation: Flink connector — https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/
- StarRocks documentation: Asynchronous materialized views — https://docs.starrocks.io/docs/using_starrocks/Materialized_view/
- StarRocks documentation: Partial update — https://docs.starrocks.io/docs/loading/partial_update/
- Flink CDC connectors — https://nightlies.apache.org/flink/flink-cdc-docs-stable/
- `skills/apache_flink/SKILL.md` — Flink DataStream API, checkpointing, stateful operators
- `skills/pyspark_streaming/SKILL.md` — for Spark Structured Streaming alternative to Flink
- `skills/apache_kafka/SKILL.md` — Kafka topic design, consumer group management, Schema Registry
- `skills/cdc_debezium/SKILL.md` — Debezium connector setup, change event structure, SMTs
- `group_skills/starrocks_group_skills/starrocks_materialized_views/SKILL.md` — full async MV DDL reference, partition-aware refresh, query rewrite debugging
- `group_skills/starrocks_group_skills/starrocks_data_modeling/SKILL.md` — table type selection, sort key design, star schema patterns
