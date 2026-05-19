---
name: starrocks-files-ingestion
description: StarRocks file ingestion — FILES() table function (SELECT/INSERT from S3/HDFS Parquet/ORC/CSV without CREATE TABLE), Iceberg external catalog (HMS/Glue/REST), CREATE EXTERNAL CATALOG, cross-catalog INSERT INTO SELECT, schema auto-detection, partition filter pushdown on external tables, SHOW CREATE CATALOG, external table DDL patterns
---

# StarRocks Files Ingestion & External Catalogs

## When to Use

- Query Parquet/ORC files on S3/HDFS directly without creating a table
- One-off data exploration before deciding on import strategy
- Iceberg lakehouse integration (read Iceberg tables in StarRocks)
- ETL: read from Iceberg/Hive external catalog → transform → write to StarRocks internal table
- Schema auto-detection for rapid prototyping

---

## FILES() Table Function

The `FILES()` function lets you query files directly without creating a table first.

### Read Parquet from S3

```sql
-- Auto-detect schema and query
SELECT * FROM FILES(
    "path" = "s3a://datalake/orders/dt=2024-01-15/*.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "AKIAIOSFODNN7EXAMPLE",
    "aws.s3.secret_key" = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "aws.s3.region" = "us-east-1"
)
LIMIT 100;
```

### With IAM Instance Profile

```sql
SELECT COUNT(*), SUM(amount)
FROM FILES(
    "path" = "s3a://datalake/orders/dt=2024-01-15/*.parquet",
    "format" = "parquet",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
);
```

### INSERT INTO StarRocks Table from Files

```sql
-- Load Parquet files into StarRocks table (one-time or scheduled)
INSERT INTO orders
SELECT order_id, customer_id, amount, created_at
FROM FILES(
    "path" = "s3a://datalake/orders/dt=2024-01-15/*.parquet",
    "format" = "parquet",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
);
```

### CTAS — Create Table from Files

```sql
-- Create table with auto-detected schema from Parquet files
CREATE TABLE orders_auto
AS SELECT * FROM FILES(
    "path" = "s3a://datalake/orders/dt=2024-01-15/part-*.parquet",
    "format" = "parquet",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
);

-- Then check the inferred schema
SHOW CREATE TABLE orders_auto;
```

### CSV from HDFS

```sql
SELECT *
FROM FILES(
    "path" = "hdfs://namenode:9000/data/events/*.csv",
    "format" = "csv",
    "csv.column_separator" = ",",
    "csv.row_delimiter" = "\n",
    "csv.skip_header" = "1"  -- skip header row
);
```

---

## Iceberg External Catalog

### Create Catalog — Hive Metastore

```sql
CREATE EXTERNAL CATALOG iceberg_hms
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://metastore.internal:9083",
    -- S3 credentials for data files
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
);
```

### Create Catalog — AWS Glue

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

### Create Catalog — REST Catalog (Polaris/Tabular/Nessie)

```sql
CREATE EXTERNAL CATALOG iceberg_rest
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "https://polaris.internal/api/catalog",
    "iceberg.catalog.warehouse" = "my_warehouse",
    -- OAuth2 authentication
    "iceberg.catalog.security" = "oauth2",
    "iceberg.catalog.oauth2.credential" = "client_id:client_secret",
    "iceberg.catalog.oauth2.scope" = "PRINCIPAL_ROLE:ALL",
    -- S3 credentials for data
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
);
```

### Create Catalog — MinIO + HMS

```sql
CREATE EXTERNAL CATALOG iceberg_minio
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://metastore:9083",
    "aws.s3.access_key" = "minio_user",
    "aws.s3.secret_key" = "minio_password",
    "aws.s3.endpoint" = "http://minio.internal:9000",
    "aws.s3.enable_path_style_access" = "true"
);
```

---

## Querying External Catalogs

```sql
-- Set current catalog
SET CATALOG iceberg_hms;

-- Or use fully-qualified names
SELECT * FROM iceberg_hms.analytics.orders LIMIT 100;

-- Show catalogs
SHOW CATALOGS;

-- Inspect catalog
SHOW CREATE CATALOG iceberg_hms;

-- Show databases in catalog
SHOW DATABASES FROM iceberg_hms;

-- Show tables in database
SHOW TABLES FROM iceberg_hms.analytics;

-- Describe table
DESCRIBE iceberg_hms.analytics.orders;
```

---

## ETL Pattern: External Catalog → Internal Table

```sql
-- Transform and land data from Iceberg into StarRocks daily partition
INSERT OVERWRITE orders PARTITION (dt='2024-01-15')
SELECT
    order_id,
    customer_id,
    amount,
    status,
    created_at
FROM iceberg_hms.raw.orders
WHERE DATE(created_at) = '2024-01-15'
  AND amount > 0;
```

### Incremental Load Pattern

```sql
-- Load only new partitions from Iceberg
INSERT INTO orders
SELECT order_id, customer_id, amount, created_at
FROM iceberg_hms.raw.orders
WHERE created_at >= (
    SELECT MAX(created_at) FROM orders
);
```

---

## Partition Filter Pushdown

StarRocks pushes partition filters down to the Iceberg metadata layer, skipping files not matching the predicate:

```sql
-- Partition filter on dt column — only reads matching Iceberg partitions
SELECT COUNT(*) FROM iceberg_hms.analytics.events
WHERE dt = '2024-01-15';
-- EXPLAIN shows: partitions=1/365
```

Verify with EXPLAIN:
```sql
EXPLAIN SELECT * FROM iceberg_hms.analytics.events WHERE dt = '2024-01-15';
-- Look for: "partitions=1/365" in IcebergScanNode
```

Filters that DON'T push down:
- Function wrapping: `WHERE YEAR(dt) = 2024` — use `WHERE dt >= '2024-01-01'` instead
- Non-partition columns: only partition key columns benefit from pruning

---

## Hive Metastore External Catalog

```sql
CREATE EXTERNAL CATALOG hive_catalog
PROPERTIES (
    "type" = "hive",
    "hive.metastore.uris" = "thrift://metastore:9083",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
);

-- Query Hive partitioned table
SELECT * FROM hive_catalog.warehouse.orders
WHERE dt = '2024-01-15'
LIMIT 1000;
```

---

## Schema Evolution Handling

StarRocks handles Iceberg schema evolution transparently:
- Added columns: appear as NULL in StarRocks query if not backfilled
- Dropped columns: hidden, not returned in SELECT *
- Renamed columns: new name appears in StarRocks

For type widening (INT → BIGINT): StarRocks reads with new type automatically.

---

## Writing Back to Iceberg (StarRocks 3.1+)

StarRocks can write to Iceberg catalogs it manages:

```sql
-- Create Iceberg database
CREATE DATABASE iceberg_hms.new_db;

-- Create Iceberg table via StarRocks
CREATE TABLE iceberg_hms.analytics.orders_gold (
    order_id BIGINT,
    customer_id BIGINT,
    revenue DECIMAL(18, 2),
    dt DATE
)
PARTITION BY (dt);

-- Write to Iceberg table
INSERT INTO iceberg_hms.analytics.orders_gold
SELECT order_id, customer_id, SUM(amount), DATE(created_at)
FROM iceberg_hms.raw.orders
GROUP BY 1, 2, 4;
```

---

## Drop External Catalog

```sql
DROP CATALOG iceberg_hms;
```

---

## Anti-Patterns

1. **Using `FILES()` for production daily loads** — FILES() has no parallelism tuning; use Broker Load for large production batch loads.
2. **Not verifying partition pushdown** — ETL that scans all Iceberg partitions is 100× slower than needed; always EXPLAIN and verify.
3. **Mixing catalog credentials in DDL** — store sensitive credentials in environment variables or HashiCorp Vault, not in CREATE CATALOG DDL committed to git.
4. **CTAS without explicit DDL** — auto-detected types may be wrong (e.g., BIGINT instead of INT); always review `SHOW CREATE TABLE` after CTAS.
5. **No statistics on external tables** — CBO has no stats on Iceberg tables by default; run `ANALYZE TABLE iceberg_hms.db.t` after schema discovery.
6. **`s3://` instead of `s3a://`** — StarRocks requires `s3a://` for AWS S3 URIs.

---

## References

- FILES() function: `docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/`
- Iceberg catalog: `docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog/`
- Hive catalog: `docs.starrocks.io/docs/data_source/catalog/hive_catalog/`
- Related skills: `[[starrocks-broker-load]]`, `[[starrocks-lakehouse-integration]]`, `[[starrocks-partitioning]]`
