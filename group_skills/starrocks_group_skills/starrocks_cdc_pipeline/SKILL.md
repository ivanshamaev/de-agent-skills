---
name: starrocks-cdc-pipeline
description: StarRocks CDC pipeline — Flink CDC (flink-cdc-connectors) to StarRocks Primary Key table, Debezium + Kafka → Routine Load upsert, StarRocks Flink connector (exactly-once, DELETE semantics), schema change propagation, Debezium envelope parsing, multi-table CDC fan-out, dead letter queue for failed CDC events, lag monitoring
---

# StarRocks CDC Pipeline

## When to Use

- Real-time replication from PostgreSQL/MySQL/Oracle → StarRocks
- Streaming CDC events from Kafka into mutable StarRocks tables
- Building real-time operational analytics on transactional data
- Deduplicating and merging change events before landing in StarRocks

---

## Architecture Patterns

```
Pattern A: Debezium → Kafka → Routine Load (simple, JSON CDC)
PostgreSQL ──Debezium──► Kafka topic ──Routine Load──► StarRocks PK Table

Pattern B: Flink CDC → StarRocks Flink Connector (full DML + schema change)
PostgreSQL ──Flink CDC──► Flink job ──StarRocks Sink──► StarRocks PK Table

Pattern C: Debezium → Kafka → Flink → StarRocks (transform + enrich)
PostgreSQL ──Debezium──► Kafka ──Flink job──► StarRocks PK Table
                                    ↑ join/enrich/filter
```

---

## Pattern A: Routine Load from Debezium/Kafka

Simplest approach — no Flink required. Only works for INSERT/UPDATE (no DELETE).

### Debezium Message Format (Postgres)

```json
{
  "schema": {...},
  "payload": {
    "before": null,
    "after": {
      "order_id": 12345,
      "customer_id": 101,
      "amount": 99.99,
      "status": "pending",
      "updated_at": 1705315200000
    },
    "source": {"db": "sales", "table": "orders", "lsn": 12345678},
    "op": "c",
    "ts_ms": 1705315200500
  }
}
```

### Routine Load for Debezium JSON

```sql
-- Target table (Primary Key for upsert semantics)
CREATE TABLE orders (
    order_id    BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    amount      DECIMAL(10, 2),
    status      VARCHAR(32),
    updated_at  DATETIME
) PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 16
PROPERTIES("enable_persistent_index" = "true");

-- Routine Load: extract .after.* fields
CREATE ROUTINE LOAD sales.cdc_orders ON orders
PROPERTIES (
    "desired_concurrent_number" = "4",
    "format" = "json",
    "jsonpaths" = "[\"$.payload.after.order_id\",\"$.payload.after.customer_id\",\"$.payload.after.amount\",\"$.payload.after.status\",\"$.payload.after.updated_at\"]",
    "columns" = "order_id,customer_id,amount,status,updated_at_ms,updated_at=FROM_UNIXTIME(updated_at_ms/1000)",
    -- Skip delete events (op='d') — Routine Load cannot handle deletes
    "where" = "$.payload.op != 'd'"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "postgres.sales.orders",
    "property.group.id" = "sr_cdc_orders"
);
```

**Limitation**: Routine Load cannot process `op=d` (DELETE) events. For full DML support, use Pattern B.

---

## Pattern B: Flink CDC → StarRocks Flink Connector

Supports INSERT, UPDATE, DELETE with exactly-once semantics.

### Maven Dependencies

```xml
<!-- pom.xml -->
<dependency>
    <groupId>com.ververica</groupId>
    <artifactId>flink-connector-postgres-cdc</artifactId>
    <version>3.1.0</version>
</dependency>
<dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>flink-connector-starrocks</artifactId>
    <version>1.2.9_flink-1.18</version>
</dependency>
```

### Flink CDC Job (Flink Table API)

```java
// Full DML CDC: INSERT + UPDATE + DELETE → StarRocks PK table
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4);

// Enable checkpointing for exactly-once
env.enableCheckpointing(30000);
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// PostgreSQL CDC source
tableEnv.executeSql("""
    CREATE TABLE pg_orders (
        order_id   BIGINT PRIMARY KEY NOT ENFORCED,
        customer_id BIGINT,
        amount     DECIMAL(10, 2),
        status     STRING,
        updated_at TIMESTAMP(3)
    ) WITH (
        'connector' = 'postgres-cdc',
        'hostname'  = 'pg-host',
        'port'      = '5432',
        'username'  = 'replicator',
        'password'  = 'secret',
        'database-name' = 'sales',
        'schema-name'   = 'public',
        'table-name'    = 'orders',
        'decoding.plugin.name' = 'pgoutput',
        'slot.name'     = 'flink_cdc_slot'
    )
""");

// StarRocks sink
tableEnv.executeSql("""
    CREATE TABLE sr_orders (
        order_id    BIGINT PRIMARY KEY NOT ENFORCED,
        customer_id BIGINT,
        amount      DECIMAL(10, 2),
        status      STRING,
        updated_at  TIMESTAMP(3)
    ) WITH (
        'connector'              = 'starrocks',
        'jdbc-url'               = 'jdbc:mysql://sr-fe:9030',
        'load-url'               = 'sr-fe:8030',
        'username'               = 'root',
        'password'               = 'password',
        'database-name'          = 'sales',
        'table-name'             = 'orders',
        'sink.semantic'          = 'exactly-once',
        'sink.buffer-flush.max-rows' = '1000',
        'sink.buffer-flush.interval-ms' = '5000',
        'sink.properties.partial_update' = 'false'
    )
""");

tableEnv.executeSql("INSERT INTO sr_orders SELECT * FROM pg_orders");
env.execute("PG Orders CDC → StarRocks");
```

### MySQL CDC Source

```java
tableEnv.executeSql("""
    CREATE TABLE mysql_orders (...) WITH (
        'connector'  = 'mysql-cdc',
        'hostname'   = 'mysql-host',
        'port'       = '3306',
        'username'   = 'replicator',
        'password'   = 'secret',
        'database-name' = 'sales',
        'table-name'    = 'orders',
        'server-id'     = '5401-5404',
        'server-time-zone' = 'UTC'
    )
""");
```

---

## Pattern C: Debezium → Kafka → Flink Transform → StarRocks

For enrichment, filtering, or fan-out to multiple tables:

```java
// Read from Kafka (Debezium JSON format)
tableEnv.executeSql("""
    CREATE TABLE kafka_orders_cdc (
        payload ROW<
            before ROW<order_id BIGINT, status STRING>,
            after  ROW<order_id BIGINT, customer_id BIGINT,
                       amount DECIMAL(10,2), status STRING>,
            op     STRING
        >
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'postgres.sales.orders',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'flink_cdc_processor',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'
    )
""");

// Extract and filter: only write non-deleted rows to StarRocks
tableEnv.executeSql("""
    INSERT INTO sr_orders
    SELECT
        payload.after.order_id,
        payload.after.customer_id,
        payload.after.amount,
        payload.after.status
    FROM kafka_orders_cdc
    WHERE payload.op IN ('c', 'u', 'r')
      AND payload.after IS NOT NULL
""");
```

---

## Multi-Table CDC Fan-Out

Route CDC events from a single Kafka topic to multiple StarRocks tables based on source table name:

```python
# Flink Python API: route by source table
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
tableEnv = StreamTableEnvironment.create(env)

# Read all CDC events from Debezium
tableEnv.execute_sql("""
    CREATE TABLE all_cdc (
        source_table STRING,
        op           STRING,
        after_json   STRING  -- raw JSON of after payload
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'postgres.sales.*',  -- Debezium topic pattern
        ...
    )
""")

# Route: orders
tableEnv.execute_sql("""
    INSERT INTO sr_orders
    SELECT
        CAST(JSON_VALUE(after_json, '$.order_id') AS BIGINT),
        CAST(JSON_VALUE(after_json, '$.customer_id') AS BIGINT),
        CAST(JSON_VALUE(after_json, '$.amount') AS DECIMAL(10, 2))
    FROM all_cdc WHERE source_table = 'orders' AND op != 'd'
""")

# Route: customers
tableEnv.execute_sql("""
    INSERT INTO sr_customers
    SELECT ...
    FROM all_cdc WHERE source_table = 'customers' AND op != 'd'
""")
```

---

## Schema Change Propagation

Debezium detects source schema changes and emits a schema change event. Handle in Flink:

```java
// Use Flink CDC's built-in schema evolution (requires Flink CDC 3.0+)
// Enable: 'scan.incremental.snapshot.enabled' = 'true'
// StarRocks sink handles added columns via partial_update or ALTER TABLE

// Manual: detect schema change signal topics from Debezium
// "postgres.sales.orders.Schema" topic → parse → execute ALTER TABLE in StarRocks
```

For production: use the **StarRocks Flink Connector** with schema evolution enabled:
```java
// In StarRocks sink properties:
'sink.properties.enable_schema_evolution' = 'true'
```

---

## Dead Letter Queue for Failed CDC Events

```python
# Flink: route parse errors to DLQ Kafka topic
tableEnv.execute_sql("""
    CREATE TABLE dlq_orders WITH (
        'connector' = 'kafka',
        'topic' = 'dlq.starrocks.orders',
        'properties.bootstrap.servers' = 'kafka:9092',
        'format' = 'json'
    ) LIKE kafka_orders_cdc (EXCLUDING OPTIONS)
""")

# Route unparseable events to DLQ
tableEnv.execute_sql("""
    INSERT INTO dlq_orders
    SELECT * FROM kafka_orders_cdc
    WHERE payload.after IS NULL AND payload.op NOT IN ('d')
""")
```

---

## CDC Lag Monitoring

```python
import pymysql
from confluent_kafka.admin import AdminClient

def get_cdc_lag(kafka_brokers: str, topic: str, sr_host: str, job_name: str) -> dict:
    """Compare Kafka high watermark vs Routine Load committed offset."""
    # Get Kafka high watermarks
    admin = AdminClient({"bootstrap.servers": kafka_brokers})
    meta = admin.list_topics(topic)
    partitions = list(meta.topics[topic].partitions.keys())

    # Get committed offsets from StarRocks
    conn = pymysql.connect(host=sr_host, port=9030, user="root")
    cursor = conn.cursor()
    cursor.execute(f"SHOW ROUTINE LOAD FOR sales.{job_name}")
    columns = [d[0] for d in cursor.description]
    row = dict(zip(columns, cursor.fetchone()))
    conn.close()

    import json
    progress_str = row.get("Progress", "{}")
    # Parse: {"Partition[0]: 12345", ...}
    committed = {}
    for item in progress_str.split(";"):
        if ":" in item:
            p, offset = item.split(":", 1)
            committed[int(p.strip().replace("Partition[","").replace("]",""))] = int(offset.strip())

    return {"committed_offsets": committed, "state": row.get("State")}
```

---

## Production Checklist

- [ ] StarRocks target table uses `PRIMARY KEY` with `enable_persistent_index=true`
- [ ] Flink checkpointing enabled (exactly-once)
- [ ] Replication slot created in PostgreSQL for Debezium (`CREATE REPLICATION SLOT`)
- [ ] WAL level set to `logical` in PostgreSQL (`wal_level = logical`)
- [ ] DLQ topic configured for parse failures
- [ ] Lag alerting on Kafka consumer group
- [ ] ANALYZE TABLE scheduled after initial snapshot load
- [ ] Routine Load job monitoring via `SHOW ROUTINE LOAD`

---

## Anti-Patterns

1. **Using Routine Load for DELETE events** — Routine Load doesn't support deletes; use Flink StarRocks connector for full DML.
2. **No exactly-once on Flink checkpoint** — data duplication on job restart; always enable `EXACTLY_ONCE` checkpointing.
3. **Large Flink buffer intervals** — `sink.buffer-flush.interval-ms=60000` causes 60s lag; set to 5000ms for real-time.
4. **No replication slot monitoring** — PostgreSQL replication slots accumulate WAL if consumer falls behind; alert on slot lag > 10GB.
5. **Missing `enable_persistent_index=true`** — large PK tables without persistent index cause memory pressure on BE.
6. **Ignoring schema changes** — source schema change breaks CDC pipeline silently; implement schema evolution or alerting.

---

## References

- Flink StarRocks connector: `docs.starrocks.io/docs/loading/Flink-connector-starrocks/`
- Routine Load: `docs.starrocks.io/docs/loading/RoutineLoad/`
- Flink CDC connectors: `github.com/apache/flink-cdc`
- Related skills: `[[starrocks-routine-load-kafka]]`, `[[starrocks-stream-load]]`, `[[starrocks-realtime-modeling]]`, `[[cdc-debezium]]`
