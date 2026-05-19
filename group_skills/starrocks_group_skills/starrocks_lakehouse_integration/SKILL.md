---
name: starrocks-lakehouse-integration
description: StarRocks lakehouse integration — Iceberg/Hive/Delta external catalogs (HMS/Glue/REST), cross-catalog INSERT INTO SELECT, partition filter pushdown verification, external table statistics (ANALYZE on Iceberg), writing back to Iceberg from StarRocks (3.1+), Delta Lake catalog setup, Unity Catalog (3.2+), query federation across StarRocks + Iceberg + Hive in single SQL, cache invalidation (REFRESH EXTERNAL TABLE)
---

# StarRocks Lakehouse Integration

## When to Use

- Query Iceberg/Hive/Delta Lake tables directly from StarRocks without copying data
- ETL: read from lakehouse → transform → write to StarRocks internal tables
- Federation queries joining StarRocks internal tables with external Iceberg tables
- Writing StarRocks query results back to Iceberg (3.1+)
- Schema discovery and sampling on lakehouse files before committing to DDL

---

## Supported External Catalogs

| Catalog Type | StarRocks Connector | Formats Supported |
|-------------|--------------------|--------------------|
| Apache Iceberg | `iceberg` | Parquet, ORC, Avro |
| Apache Hive | `hive` | Parquet, ORC, CSV, Text |
| Delta Lake | `delta` | Parquet |
| Apache Hudi | `hudi` | Parquet |
| JDBC (PostgreSQL, MySQL) | `jdbc` | — |
| Elasticsearch | `elasticsearch` | — |

---

## Iceberg Catalog Setup

### Hive Metastore (HMS)

```sql
CREATE EXTERNAL CATALOG iceberg_hms
COMMENT "Iceberg catalog backed by Hive Metastore"
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://hive-metastore.internal:9083",
    -- S3 credentials
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
);
```

### AWS Glue

```sql
CREATE EXTERNAL CATALOG iceberg_glue
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "glue",
    "aws.glue.use_instance_profile" = "true",
    "aws.glue.region" = "us-east-1",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
);
```

### REST Catalog (Polaris / Tabular / Nessie)

```sql
CREATE EXTERNAL CATALOG iceberg_rest
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "https://polaris.internal/api/catalog",
    "iceberg.catalog.warehouse" = "production_warehouse",
    "iceberg.catalog.security" = "oauth2",
    "iceberg.catalog.oauth2.credential" = "client_id:client_secret",
    "iceberg.catalog.oauth2.scope" = "PRINCIPAL_ROLE:ALL",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
);
```

### MinIO + HMS (Local/On-prem)

```sql
CREATE EXTERNAL CATALOG iceberg_minio
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://hive-metastore:9083",
    "aws.s3.access_key" = "minio_user",
    "aws.s3.secret_key" = "minio_password",
    "aws.s3.endpoint" = "http://minio.internal:9000",
    "aws.s3.enable_path_style_access" = "true"
);
```

---

## Hive Catalog

```sql
CREATE EXTERNAL CATALOG hive_prod
PROPERTIES (
    "type" = "hive",
    "hive.metastore.uris" = "thrift://hive-metastore:9083",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
);

-- Query partitioned Hive table
SELECT * FROM hive_prod.warehouse.orders
WHERE dt = '2024-01-15'
LIMIT 1000;
```

---

## Delta Lake Catalog

```sql
CREATE EXTERNAL CATALOG delta_prod
PROPERTIES (
    "type" = "delta",
    "hive.metastore.uris" = "thrift://hive-metastore:9083",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
);

SELECT * FROM delta_prod.analytics.orders_delta
WHERE event_date = '2024-01-15';
```

---

## Catalog Navigation

```sql
-- List all catalogs
SHOW CATALOGS;

-- Set default catalog for session
SET CATALOG iceberg_hms;

-- List databases
SHOW DATABASES FROM iceberg_hms;

-- List tables
SHOW TABLES FROM iceberg_hms.analytics;

-- Describe external table schema
DESCRIBE iceberg_hms.analytics.orders;

-- View catalog definition
SHOW CREATE CATALOG iceberg_hms;

-- Drop catalog
DROP CATALOG iceberg_hms;
```

---

## Cross-Catalog ETL

Read from Iceberg, transform, write to StarRocks internal table:

```sql
-- Daily partition load from Iceberg → StarRocks
INSERT OVERWRITE sales.orders PARTITION (dt='2024-01-15')
SELECT
    order_id,
    customer_id,
    amount,
    status,
    DATE(created_at) AS dt
FROM iceberg_hms.raw_layer.orders
WHERE DATE(created_at) = '2024-01-15'
  AND amount > 0
  AND status IS NOT NULL;
```

### Incremental Load from Iceberg

```sql
-- Load only rows newer than StarRocks watermark
INSERT INTO sales.orders
SELECT o.order_id, o.customer_id, o.amount, o.status, o.created_at
FROM iceberg_hms.raw_layer.orders o
WHERE o.updated_at > (
    SELECT COALESCE(MAX(updated_at), '2024-01-01') FROM sales.orders
);
```

---

## Writing Back to Iceberg (StarRocks 3.1+)

StarRocks can write to catalogs it manages:

```sql
-- Create a new database in external Iceberg catalog
CREATE DATABASE iceberg_hms.gold_layer;

-- Create Iceberg table through StarRocks
CREATE TABLE iceberg_hms.gold_layer.orders_daily (
    dt          DATE,
    customer_id BIGINT,
    region      VARCHAR(64),
    order_count BIGINT,
    revenue     DECIMAL(18, 2)
)
PARTITION BY (dt);

-- Write StarRocks aggregation to Iceberg
INSERT INTO iceberg_hms.gold_layer.orders_daily
SELECT
    DATE(created_at)     AS dt,
    customer_id,
    region,
    COUNT(*)             AS order_count,
    SUM(amount)          AS revenue
FROM sales.orders
WHERE DATE(created_at) = '2024-01-15'
GROUP BY DATE(created_at), customer_id, region;
```

---

## Partition Filter Pushdown

StarRocks pushes WHERE clauses on partition columns down to Iceberg/Hive metadata, skipping entire partitions:

```sql
-- Good: uses partition pruning
SELECT COUNT(*) FROM iceberg_hms.analytics.events
WHERE dt = '2024-01-15';
-- EXPLAIN shows: partitions=1/365

-- Bad: function wrapping prevents pruning
SELECT COUNT(*) FROM iceberg_hms.analytics.events
WHERE YEAR(dt) = 2024 AND MONTH(dt) = 1;
-- Use instead:
WHERE dt >= '2024-01-01' AND dt < '2024-02-01'
```

Verify partition pushdown:
```sql
EXPLAIN SELECT COUNT(*)
FROM iceberg_hms.analytics.events
WHERE dt = '2024-01-15';
-- Look for: partitions=1/365 in IcebergScanNode
```

---

## Statistics on External Tables

CBO has no stats on external tables by default; collect manually:

```sql
-- Collect statistics on Iceberg table
ANALYZE TABLE iceberg_hms.analytics.orders
WITH ASYNC MODE;

-- Check analyze status
SHOW ANALYZE STATUS;

-- Collect on specific columns only (faster)
ANALYZE TABLE iceberg_hms.analytics.orders
(order_id, customer_id, amount);
```

---

## Cache Invalidation

StarRocks caches external table metadata. Refresh when source changes:

```sql
-- Refresh metadata for specific external table
REFRESH EXTERNAL TABLE iceberg_hms.analytics.orders;

-- Refresh specific partitions only
REFRESH EXTERNAL TABLE iceberg_hms.analytics.events
PARTITION ('dt=2024-01-15', 'dt=2024-01-16');
```

---

## Federation Query: StarRocks + Iceberg + Hive

```sql
-- Join internal StarRocks table with Iceberg and Hive tables
SELECT
    sr.order_id,
    sr.customer_id,
    ice.product_name,
    hive.customer_name,
    sr.amount
FROM sales.orders sr                                    -- internal StarRocks
JOIN iceberg_hms.catalog.products ice                   -- Iceberg catalog
    ON sr.product_id = ice.product_id
JOIN hive_prod.customer_db.customers hive               -- Hive catalog
    ON sr.customer_id = hive.customer_id
WHERE sr.created_at >= '2024-01-15'
  AND ice.category = 'electronics'
LIMIT 1000;
```

---

## Iceberg Time Travel Queries

```sql
-- Query Iceberg table at a specific snapshot
SELECT * FROM iceberg_hms.analytics.orders
FOR VERSION AS OF 3821550127947089987;

-- Query Iceberg table at a specific timestamp
SELECT * FROM iceberg_hms.analytics.orders
FOR TIMESTAMP AS OF '2024-01-15 10:00:00';

-- Show Iceberg table history
SELECT * FROM iceberg_hms.analytics.orders$snapshots
ORDER BY committed_at DESC
LIMIT 10;
```

---

## Iceberg Schema Evolution Handling

| Change Type | StarRocks Behavior |
|-------------|-------------------|
| Add column | Returns NULL for pre-add rows |
| Drop column | Column hidden, not in SELECT * |
| Rename column | New name appears automatically |
| Type widening (INT→BIGINT) | Read with new type automatically |
| Type narrowing | May cause errors — avoid |

---

## Anti-Patterns

1. **`s3://` instead of `s3a://` in catalog config** — StarRocks requires `s3a://` for AWS S3 paths in INFILE/FILES(); catalog config uses `aws.s3.*` properties instead.
2. **No REFRESH EXTERNAL TABLE after Iceberg compaction** — compaction creates new manifest files; StarRocks caches the old metadata and queries fail or miss data.
3. **No statistics on external tables** — CBO assumes uniform distribution, leading to bad join orders and plans; always run ANALYZE after schema discovery.
4. **Storing Iceberg catalog credentials in DDL** — `CREATE EXTERNAL CATALOG` with plaintext keys gets logged in FE audit log; use environment profiles or Vault injection.
5. **Writing to Hive-managed tables via StarRocks** — StarRocks Iceberg write (3.1+) supports only Iceberg; Hive-managed tables are read-only from StarRocks.
6. **Querying large Iceberg tables without partition filter** — StarRocks will scan all files; always add partition predicate and verify with EXPLAIN.

---

## References

- Iceberg catalog: `docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog/`
- Hive catalog: `docs.starrocks.io/docs/data_source/catalog/hive_catalog/`
- Delta catalog: `docs.starrocks.io/docs/data_source/catalog/delta_lake_catalog/`
- Write to external catalogs: `docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog/#write-to-iceberg-tables`
- Related skills: `[[starrocks-files-ingestion]]`, `[[starrocks-data-modeling]]`, `[[trino-iceberg]]`, `[[delta-lake]]`
