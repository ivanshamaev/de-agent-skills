---
name: cdc-debezium
description: Change Data Capture with Debezium — PostgreSQL/MySQL/Oracle CDC connectors, change event structure (before/after/op/source), snapshot modes, Kafka Connect deployment, SMT transformations, outbox pattern, Iceberg/Delta sink integration, idempotency guarantees
---

# CDC Pipelines with Debezium

## When to Use

Activate this skill when the task involves:
- Setting up Debezium connectors for PostgreSQL, MySQL, or Oracle
- Interpreting or transforming CDC change events (insert/update/delete/truncate)
- Designing CDC pipelines: database → Kafka → data lake / warehouse
- Implementing the Outbox pattern for transactional CDC
- Troubleshooting replication lag, snapshot failures, or schema evolution
- Integrating CDC streams with Apache Flink, Spark Structured Streaming, or dbt

---

## Core Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Source Database                                             │
│  ┌─────────────┐  WAL / binlog / redo log                   │
│  │ PostgreSQL  │──────────────────────┐                     │
│  │ MySQL       │                      ▼                     │
│  │ Oracle      │          ┌───────────────────────┐         │
│  └─────────────┘          │  Kafka Connect Worker │         │
│                           │  ┌─────────────────┐  │         │
│                           │  │ Debezium Source │  │         │
│                           │  │    Connector    │  │         │
│                           │  └────────┬────────┘  │         │
│                           └───────────│───────────┘         │
│                                       ▼                     │
│                           ┌───────────────────────┐         │
│                           │  Kafka Topic          │         │
│                           │  <prefix>.<db>.<tbl>  │         │
│                           └───────────┬───────────┘         │
│                                       ▼                     │
│                    ┌──────────────────────────────┐         │
│                    │  Consumers                   │         │
│                    │  • Kafka Connect Sink         │         │
│                    │  • Apache Flink / Spark       │         │
│                    │  • ksqlDB / Kafka Streams     │         │
│                    └──────────────────────────────┘         │
└──────────────────────────────────────────────────────────────┘
```

Debezium reads database **transaction logs** (WAL for PostgreSQL, binlog for MySQL), not the tables themselves — zero query load on the source.

---

## Change Event Structure

Every Debezium event is a Kafka message with a structured envelope:

```json
{
  "schema": { ... },
  "payload": {
    "before": {
      "id": 1001,
      "name": "Alice",
      "email": "alice@example.com",
      "updated_at": 1700000000000
    },
    "after": {
      "id": 1001,
      "name": "Alice Smith",
      "email": "alice@example.com",
      "updated_at": 1700001000000
    },
    "source": {
      "version": "2.5.0.Final",
      "connector": "postgresql",
      "name": "pgserver1",
      "ts_ms": 1700001000123,
      "snapshot": "false",
      "db": "mydb",
      "sequence": "[\"24023119\",\"24023255\"]",
      "schema": "public",
      "table": "customers",
      "txId": 756,
      "lsn": 24023255,
      "xmin": null
    },
    "op": "u",
    "ts_ms": 1700001000456,
    "transaction": {
      "id": "756:24023255",
      "total_order": 1,
      "data_collection_order": 1
    }
  }
}
```

### Operation Types

| `op` | Meaning | `before` | `after` |
|------|---------|----------|---------|
| `c`  | INSERT (create) | `null` | row state |
| `u`  | UPDATE | previous state | new state |
| `d`  | DELETE | previous state | `null` |
| `r`  | READ (snapshot) | `null` | row state |
| `t`  | TRUNCATE | `null` | `null` |

### Tombstone Events

After a `d` (delete) event, Debezium emits a **tombstone** — a message with `null` value and the same key — enabling Kafka log compaction to eventually remove the record.

---

## PostgreSQL Connector

### Prerequisites

```sql
-- postgresql.conf: must be set before Debezium can connect
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;

-- Create dedicated Debezium user
CREATE ROLE debezium WITH LOGIN PASSWORD 'secret' REPLICATION;
GRANT CONNECT ON DATABASE mydb TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- Create publication (pgoutput plugin — no extra extension needed)
CREATE PUBLICATION dbz_pub FOR TABLE customers, orders, products;
-- Or capture all tables:
CREATE PUBLICATION dbz_pub FOR ALL TABLES;
```

Restart PostgreSQL after changing `wal_level`.

### Connector Configuration

```json
{
  "name": "pg-customers-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "postgres-host",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "secret",
    "database.dbname": "mydb",

    "topic.prefix": "pgserver1",
    "plugin.name": "pgoutput",
    "publication.name": "dbz_pub",
    "slot.name": "debezium_slot",

    "table.include.list": "public.customers,public.orders",

    "snapshot.mode": "initial",
    "snapshot.locking.mode": "minimal",

    "heartbeat.interval.ms": "30000",
    "heartbeat.topics.prefix": "__debezium-heartbeat",

    "key.converter": "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,table,lsn,source.ts_ms",
    "transforms.unwrap.add.headers": "db",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq.pgserver1.customers",
    "errors.deadletterqueue.topic.replication.factor": "3",
    "errors.deadletterqueue.context.headers.enable": "true"
  }
}
```

### Snapshot Modes (PostgreSQL)

| Mode | Description | When to Use |
|------|-------------|-------------|
| `initial` | Snapshot on first start only; then stream | Default — new deployments |
| `initial_only` | Snapshot then stop — no streaming | One-time historical load |
| `no_data` | Skip snapshot, start streaming from now | Append-only use cases |
| `always` | Snapshot on every connector restart | Dev/testing only |
| `exported` | Consistent snapshot without table locks (PG 15+) | Large tables in production |
| `incremental` | Re-snapshot selected tables without stopping | Add new tables mid-stream |

### Replication Slot Monitoring

```sql
-- Check lag: drop-behind LSN distance
SELECT
    slot_name,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag_size,
    active,
    database
FROM pg_replication_slots
WHERE slot_type = 'logical';

-- Alert if lag_size > 5GB — Debezium consumer is too slow
```

---

## MySQL Connector

### Prerequisites

```ini
# my.cnf — server-id must be unique across all replicas
server-id         = 12345
log_bin           = mysql-bin
binlog_format     = ROW
binlog_row_image  = FULL
expire_logs_days  = 7
gtid_mode         = ON    # recommended
enforce_gtid_consistency = ON
```

```sql
CREATE USER 'debezium'@'%' IDENTIFIED BY 'secret';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
    ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
```

### Connector Configuration

```json
{
  "name": "mysql-orders-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",

    "database.hostname": "mysql-host",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "secret",
    "database.server.id": "12345",

    "topic.prefix": "mysqlserver1",
    "database.include.list": "shop",
    "table.include.list": "shop.orders,shop.order_items",

    "snapshot.mode": "initial",
    "snapshot.locking.mode": "minimal",

    "include.schema.changes": "true",
    "schema.history.internal.kafka.topic": "schema-changes.shop",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",

    "key.converter": "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,table,source.ts_ms"
  }
}
```

MySQL requires `schema.history.internal.kafka.topic` — a durable log of all DDL changes. Never delete this topic.

---

## Single Message Transforms (SMTs)

### ExtractNewRecordState — Flattening Events

Without SMT, downstream consumers receive the full `before`/`after`/`source` envelope. `ExtractNewRecordState` flattens it:

**Before SMT:**
```json
{"payload": {"before": null, "after": {"id": 1, "name": "Alice"}, "op": "c"}}
```

**After SMT:**
```json
{"id": 1, "name": "Alice", "__op": "c", "__table": "customers", "__source_ts_ms": 1700001000123}
```

### Field Masking for PII

```json
"transforms": "mask_email",
"transforms.mask_email.type": "org.apache.kafka.connect.transforms.MaskField$Value",
"transforms.mask_email.fields": "email,phone",
"transforms.mask_email.replacement": "***"
```

### Routing to Per-Table Topics

```json
"transforms": "route",
"transforms.route.type": "io.debezium.transforms.ByLogicalTableRouter",
"transforms.route.topic.regex": "pgserver1\\.public\\.(.*)",
"transforms.route.topic.replacement": "cdc.raw.$1"
```

---

## Kafka Connect Deployment

### Docker Compose (Standalone)

```yaml
version: "3.8"
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
    ports:
      - "8081:8081"

  kafka-connect:
    image: debezium/connect:2.5
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: debezium-connect-cluster
      CONFIG_STORAGE_TOPIC: connect-configs
      OFFSET_STORAGE_TOPIC: connect-offsets
      STATUS_STORAGE_TOPIC: connect-statuses
      CONFIG_STORAGE_REPLICATION_FACTOR: 1
      OFFSET_STORAGE_REPLICATION_FACTOR: 1
      STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: io.confluent.kafka.serializers.KafkaAvroSerializer
      CONNECT_VALUE_CONVERTER: io.confluent.kafka.serializers.KafkaAvroSerializer
      CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
    ports:
      - "8083:8083"
    depends_on:
      - kafka
      - schema-registry
```

### Connector Lifecycle via REST API

```bash
# Deploy connector
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @pg-customers-connector.json

# Check status
curl -s http://localhost:8083/connectors/pg-customers-connector/status | jq

# Pause / Resume (drains without losing offset)
curl -X PUT http://localhost:8083/connectors/pg-customers-connector/pause
curl -X PUT http://localhost:8083/connectors/pg-customers-connector/resume

# Restart failed task
curl -X POST "http://localhost:8083/connectors/pg-customers-connector/tasks/0/restart"

# List all connectors
curl -s http://localhost:8083/connectors | jq

# Delete connector (releases replication slot!)
curl -X DELETE http://localhost:8083/connectors/pg-customers-connector
```

---

## Outbox Pattern

The Outbox pattern guarantees **exactly-once delivery** of domain events without dual writes or distributed transactions.

### PostgreSQL Setup

```sql
-- Outbox table lives in the same database as the domain table
CREATE TABLE outbox (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(255) NOT NULL,   -- e.g. 'Order'
    aggregate_id   VARCHAR(255) NOT NULL,   -- e.g. order_id
    event_type     VARCHAR(255) NOT NULL,   -- e.g. 'OrderPlaced'
    payload        JSONB         NOT NULL,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Application writes domain change + outbox row in ONE transaction
BEGIN;
  INSERT INTO orders (id, customer_id, total) VALUES (42, 7, 199.99);
  INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
  VALUES ('Order', '42', 'OrderPlaced',
    '{"order_id": 42, "customer_id": 7, "total": 199.99}');
COMMIT;
```

### Outbox Connector Configuration

```json
{
  "name": "outbox-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "topic.prefix": "outbox",
    "table.include.list": "public.outbox",
    "plugin.name": "pgoutput",
    "publication.name": "dbz_outbox_pub",
    "slot.name": "debezium_outbox_slot",

    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.field.event.id": "id",
    "transforms.outbox.table.field.event.key": "aggregate_id",
    "transforms.outbox.table.field.event.type": "event_type",
    "transforms.outbox.table.field.event.payload": "payload",
    "transforms.outbox.route.by.field": "aggregate_type",
    "transforms.outbox.route.topic.replacement": "events.${routedByValue}.Avro",
    "transforms.outbox.table.expand.json.payload": "true",

    "tombstones.on.delete": "false"
  }
}
```

The `EventRouter` SMT routes each row to `events.Order.Avro`, `events.Payment.Avro`, etc. and tombstones deleted outbox rows (after cleanup jobs purge them).

---

## Consuming CDC Events — Python

```python
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import json

consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "cdc-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "max.poll.interval.ms": 300000,
})
consumer.subscribe(["pgserver1.public.customers"])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        # With ExtractNewRecordState SMT, value is a flat dict
        value = json.loads(msg.value())
        op = value.get("__op", "r")

        if op == "c":
            upsert_to_sink(value)
        elif op == "u":
            upsert_to_sink(value)
        elif op == "d":
            delete_from_sink(value["id"])

        consumer.commit(asynchronous=False)
finally:
    consumer.close()
```

---

## Apache Flink CDC Integration

Flink SQL can consume Debezium-formatted Kafka topics natively via the `debezium-json` format.

```sql
-- Flink SQL: source table reading Debezium JSON from Kafka
CREATE TABLE customers_cdc (
    id          BIGINT,
    name        STRING,
    email       STRING,
    updated_at  TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'  = 'kafka',
    'topic'      = 'pgserver1.public.customers',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'flink-cdc-group',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'debezium-json',
    'debezium-json.schema-include' = 'false'
);

-- Flink SQL: Iceberg sink (upsert semantics)
CREATE TABLE customers_iceberg (
    id        BIGINT,
    name      STRING,
    email     STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'          = 'iceberg',
    'catalog-name'       = 'rest',
    'catalog-type'       = 'rest',
    'uri'                = 'http://iceberg-rest:8181',
    'warehouse'          = 's3://lakehouse/warehouse',
    'database-name'      = 'silver',
    'table-name'         = 'customers'
);

INSERT INTO customers_iceberg
SELECT id, name, email FROM customers_cdc;
```

For **Avro with Schema Registry**, use `'format' = 'debezium-avro-confluent'` and set `'debezium-avro-confluent.url'`.

---

## Iceberg / Delta Sink via Kafka Connect

### Iceberg Sink Connector (Tabular)

```json
{
  "name": "iceberg-sink-customers",
  "config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "pgserver1.public.customers",

    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://iceberg-rest:8181",
    "iceberg.catalog.warehouse": "s3://lakehouse/warehouse",

    "iceberg.tables": "silver.customers",
    "iceberg.tables.upsert-mode-enabled": "true",
    "iceberg.tables.cdc-field": "__op",

    "iceberg.control.topic": "iceberg-control",
    "iceberg.control.group-id": "iceberg-sink-group",
    "iceberg.commit.interval-ms": "60000",
    "iceberg.commit.timeout-ms": "30000"
  }
}
```

---

## Schema Evolution Handling

Debezium propagates DDL changes automatically. Key behaviors:

| Change | PostgreSQL (pgoutput) | MySQL |
|--------|-----------------------|-------|
| `ADD COLUMN` | New field appears in `after` | Schema history updated; new events include column |
| `DROP COLUMN` | Field disappears from `after` | Schema history updated |
| `RENAME COLUMN` | Treat as drop + add (no rename event) | Same |
| `ALTER COLUMN TYPE` | Supported for compatible types | Supported |
| `TRUNCATE TABLE` | `op: "t"` event emitted | Emits `op: "t"` |

For downstream Iceberg/Delta sinks, enable schema evolution on the sink connector to auto-add new columns rather than failing.

---

## Monitoring

### Key JMX Metrics

| Metric | Connector Type | Alert Threshold |
|--------|---------------|-----------------|
| `MilliSecondsBehindSource` | All | > 60,000 ms |
| `NumberOfCommittedTransactions` | MySQL | Rate drop to 0 |
| `SnapshotRunning` | All | Stuck > expected time |
| `TotalNumberOfErrorsSeen` | All | > 0 |
| `QueueRemainingCapacity` | All | < 10% |

### PostgreSQL Replication Lag

```sql
SELECT
    application_name,
    pg_size_pretty(pg_wal_lsn_diff(
        sent_lsn, replay_lsn
    )) AS replay_lag,
    state
FROM pg_stat_replication;
```

### Heartbeat Configuration

Without heartbeats, a quiet source database (no writes) will not advance the replication slot LSN, causing WAL accumulation.

```json
"heartbeat.interval.ms": "30000",
"heartbeat.action.query": "INSERT INTO debezium_heartbeat VALUES (DEFAULT) ON CONFLICT DO NOTHING"
```

Create the heartbeat table:
```sql
CREATE TABLE IF NOT EXISTS debezium_heartbeat (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ DEFAULT now()
);
```

---

## Anti-Patterns

1. **Not monitoring replication slot lag** — WAL accumulates unboundedly; PostgreSQL disk fills up. Set alert at 5 GB lag.

2. **Deleting the replication slot externally** — kills the connector offset; forces full re-snapshot. Never drop slots manually.

3. **Using `snapshot.mode: always` in production** — re-snapshots entire tables on every restart, causing massive traffic and duplicates downstream.

4. **One connector for all tables** — a single failed task blocks all tables. Split high-throughput or high-risk tables into separate connectors.

5. **Missing heartbeat on write-quiet databases** — WAL accumulates when source tables have no writes. Always configure `heartbeat.interval.ms`.

6. **Not enabling the DLQ** — deserialization or routing errors silently stop the connector task. Always configure `errors.tolerance: all` + `errors.deadletterqueue.topic.name`.

7. **Dual writes (app writes DB + Kafka)** — violates atomicity. Use the Outbox pattern instead.

8. **Using `schema_only` snapshot mode** (deprecated) — replaced by `no_data`. Using it produces a warning and may not be supported in future versions.

9. **No `tombstones.on.delete` handling in sink** — sinks that don't process tombstones accumulate ghost records in compacted topics.

10. **Transforming events inside Kafka Connect for complex logic** — SMTs are for simple field mapping only. Route raw events to Kafka and apply transformation in Flink or Spark.

---

## References to Consult When Needed

- Debezium PostgreSQL connector reference: `debezium.io/documentation/reference/stable/connectors/postgresql.html`
- Debezium MySQL connector reference: `debezium.io/documentation/reference/stable/connectors/mysql.html`
- ExtractNewRecordState SMT: `debezium.io/documentation/reference/stable/transformations/event-flattening.html`
- Outbox EventRouter SMT: `debezium.io/documentation/reference/stable/transformations/outbox-event-router.html`
- Flink Debezium format: `nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/debezium/`
