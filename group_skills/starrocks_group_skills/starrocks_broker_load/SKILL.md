---
name: starrocks-broker-load
description: StarRocks Broker Load — LOAD DATA from S3/HDFS/GCS/MinIO/Azure, CSV/Parquet/ORC format support, column mapping expressions, multi-table atomic load, SHOW LOAD status polling, CANCEL LOAD, S3 credential patterns (access key/IAM role/instance profile), parallelism tuning, Airflow integration, wildcard file paths
---

# StarRocks Broker Load

## When to Use

- Bulk load from S3, HDFS, GCS, Azure, MinIO (files already in object storage)
- Daily/hourly batch ETL landing zone → StarRocks
- Large files (GB to TB range) — async, doesn't block caller
- Multi-table atomic load (load multiple tables in one transaction)
- Loading Parquet/ORC files from data lake

Not for: continuous Kafka ingestion (Routine Load), real-time push from application (Stream Load).

---

## Basic Syntax

```sql
LOAD LABEL database.label_name (
    DATA INFILE("s3a://bucket/path/orders/2024-01-15/*.parquet")
    INTO TABLE orders
    FORMAT AS "parquet"
    (order_id, customer_id, amount, created_at, status)
)
WITH BROKER (
    "aws.s3.access_key" = "AKIAIOSFODNN7EXAMPLE",
    "aws.s3.secret_key" = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "aws.s3.region" = "us-east-1"
)
PROPERTIES (
    "timeout" = "3600",
    "max_filter_ratio" = "0.01"
);
```

---

## Storage Authentication Patterns

### AWS S3 — Static Keys

```sql
WITH BROKER (
    "aws.s3.access_key" = "${AWS_ACCESS_KEY}",
    "aws.s3.secret_key" = "${AWS_SECRET_KEY}",
    "aws.s3.region" = "us-east-1"
)
```

### AWS S3 — IAM Role (EC2 Instance Profile)

```sql
WITH BROKER (
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
)
```

### AWS S3 — AssumeRole

```sql
WITH BROKER (
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.iam_role_arn" = "arn:aws:iam::123456789012:role/StarRocksDataLoader",
    "aws.s3.region" = "us-east-1"
)
```

### MinIO (S3-Compatible)

```sql
WITH BROKER (
    "aws.s3.access_key" = "minio_user",
    "aws.s3.secret_key" = "minio_password",
    "aws.s3.endpoint" = "http://minio.internal:9000",
    "aws.s3.enable_path_style_access" = "true"  -- required for MinIO
)
```

### HDFS — Simple Auth

```sql
WITH BROKER (
    "username" = "hdfs_user",
    "password" = ""
)
-- INFILE path: "hdfs://namenode:9000/data/orders/*.parquet"
```

### HDFS — HA + Kerberos

```sql
WITH BROKER (
    "username" = "hdfs_user",
    "dfs.nameservices" = "my_cluster",
    "dfs.ha.namenodes.my_cluster" = "nn1,nn2",
    "dfs.namenode.rpc-address.my_cluster.nn1" = "nn1:9000",
    "dfs.namenode.rpc-address.my_cluster.nn2" = "nn2:9000",
    "dfs.client.failover.proxy.provider.my_cluster"
        = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
    "hadoop.security.authentication" = "kerberos",
    "kerberos_principal" = "hdfs/namenode@REALM",
    "kerberos_keytab" = "/etc/security/keytabs/hdfs.keytab"
)
```

### Google GCS

```sql
WITH BROKER (
    "gcp.gcs.service_account_email" = "sr-loader@project.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "key_id",
    "gcp.gcs.service_account_private_key" = "-----BEGIN RSA PRIVATE KEY-----\n..."
)
-- INFILE path: "gs://bucket/path/*.parquet"
```

---

## File Format Configuration

### CSV

```sql
LOAD LABEL sales.csv_load (
    DATA INFILE("s3a://bucket/csv/orders/*.csv")
    INTO TABLE orders
    COLUMNS TERMINATED BY ","
    (order_id, customer_id, amount_str, created_at_str)
    SET (
        amount = CAST(amount_str AS DECIMAL(10, 2)),
        created_at = STR_TO_DATE(created_at_str, '%Y-%m-%d %H:%i:%s')
    )
    WHERE status != 'test'
)
WITH BROKER (...)
PROPERTIES ("timeout" = "7200");
```

### Parquet

```sql
LOAD LABEL sales.parquet_load (
    DATA INFILE("s3a://datalake/orders/dt=2024-01-15/*.parquet")
    INTO TABLE orders
    FORMAT AS "parquet"
    -- Columns auto-mapped by name if schema matches
    -- Or explicit: (order_id, customer_id, amount, created_at)
)
WITH BROKER (...)
PROPERTIES (
    "timeout" = "3600",
    "load_parallelism" = "8"
);
```

### ORC

```sql
LOAD LABEL sales.orc_load (
    DATA INFILE("hdfs://cluster/warehouse/orders/dt=2024-01-15/000000_0")
    INTO TABLE orders
    FORMAT AS "orc"
)
WITH BROKER (...);
```

---

## Multi-Table Atomic Load

Load multiple tables in a single transaction — either all succeed or all rollback:

```sql
LOAD LABEL sales.daily_etl_20240115 (
    DATA INFILE("s3a://bucket/orders/dt=2024-01-15/*.parquet")
    INTO TABLE orders
    FORMAT AS "parquet",

    DATA INFILE("s3a://bucket/order_items/dt=2024-01-15/*.parquet")
    INTO TABLE order_items
    FORMAT AS "parquet",

    DATA INFILE("s3a://bucket/customers/full/*.parquet")
    INTO TABLE dim_customers
    FORMAT AS "parquet"
        (customer_id, name, email, region)
)
WITH BROKER (
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
)
PROPERTIES (
    "timeout" = "7200",
    "max_filter_ratio" = "0.01"
);
```

---

## Monitoring SHOW LOAD

```sql
-- Check specific label
SHOW LOAD FROM sales WHERE LABEL = "daily_etl_20240115"\G

-- All recent loads sorted by start time
SHOW LOAD FROM sales ORDER BY CreateTime DESC LIMIT 20;

-- Failed loads
SHOW LOAD FROM sales WHERE State = "CANCELLED" ORDER BY CreateTime DESC LIMIT 10;
```

Key columns:
| Column | Description |
|--------|-------------|
| `Label` | Job label |
| `State` | PENDING / LOADING / FINISHED / CANCELLED |
| `Progress` | ETL progress percentage |
| `EtlInfo` | Bytes scanned, filtered rows |
| `TaskInfo` | Cluster ID, timeout, max_filter_ratio |
| `ErrorMsg` | Failure message |
| `TrackingSQL` | Query to find load details |
| `CreateTime` / `EtlStartTime` / `LoadStartTime` / `FinishTime` | Timestamps |

State transitions:
```
PENDING → ETL → LOADING → FINISHED
                        ↓
                    CANCELLED (on error or CANCEL LOAD)
```

---

## Cancel a Load

```sql
-- Cancel by label
CANCEL LOAD FROM sales WHERE LABEL = "daily_etl_20240115";

-- Cancel all pending loads for a table
CANCEL LOAD FROM sales WHERE LABEL LIKE "orders_%";
```

---

## Parallelism Tuning

Broker Load automatically splits files across BEs. Tune with:

```sql
PROPERTIES (
    "load_parallelism" = "8",    -- tasks per BE (default: 1)
    "timeout" = "7200"           -- seconds before job cancelled
)
```

Throughput estimate:
```
total_throughput = BE_count × load_parallelism × per_task_throughput
per_task_throughput ≈ 100-300 MB/s depending on file format and disk speed
```

For a 100GB load with 5 BEs and `load_parallelism=4`:
```
estimated_time ≈ 100GB / (5 × 4 × 200 MB/s) ≈ 25 seconds
```

---

## Airflow Integration

```python
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import time

@dag(schedule="0 2 * * *", start_date=datetime(2024, 1, 1), catchup=False)
def daily_starrocks_load():

    @task
    def trigger_broker_load(ds=None):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        date_nodash = ds.replace("-", "")
        label = f"daily_orders_{date_nodash}"

        hook.run(f"""
            LOAD LABEL sales.{label} (
                DATA INFILE("s3a://datalake/orders/dt={ds}/*.parquet")
                INTO TABLE orders
                FORMAT AS "parquet"
            )
            WITH BROKER (
                "aws.s3.use_instance_profile" = "true",
                "aws.s3.region" = "us-east-1"
            )
            PROPERTIES ("timeout" = "3600", "max_filter_ratio" = "0.01")
        """)
        return label

    @task
    def wait_for_load(label: str):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        for attempt in range(60):  # wait up to 30 min
            rows = hook.get_records(
                f"SHOW LOAD FROM sales WHERE LABEL = '{label}'"
            )
            if not rows:
                time.sleep(30)
                continue

            row = dict(zip(
                ["JobId","Label","State","Progress","Type","EtlInfo",
                 "TaskInfo","ErrorMsg","CreateTime","EtlStartTime",
                 "LoadStartTime","FinishTime","URL","JobDetails",
                 "TransactionId","Reason","LoadBytes","ClusterName",
                 "Comment","TrackingSQL"],
                rows[0]
            ))

            if row["State"] == "FINISHED":
                print(f"Load finished: {row['EtlInfo']}")
                return
            elif row["State"] == "CANCELLED":
                raise RuntimeError(
                    f"Broker load failed: {row['ErrorMsg']}"
                )
            time.sleep(30)
        raise TimeoutError(f"Load {label} did not finish within 30 minutes")

    @task
    def run_post_load_analyze(ds=None):
        hook = MySqlHook(mysql_conn_id="starrocks_prod")
        date_nodash = ds.replace("-", "")
        hook.run(
            f"ANALYZE TABLE sales.orders PARTITION (p{date_nodash}) WITH ASYNC MODE"
        )

    label = trigger_broker_load()
    wait_for_load(label) >> run_post_load_analyze()

dag = daily_starrocks_load()
```

---

## Wildcard and Multiple Paths

```sql
-- All files matching pattern
DATA INFILE("s3a://bucket/orders/dt=2024-01-15/*.parquet")

-- Multiple explicit files
DATA INFILE(
    "s3a://bucket/orders/part-00000.parquet",
    "s3a://bucket/orders/part-00001.parquet",
    "s3a://bucket/orders/part-00002.parquet"
)

-- Multiple date partitions
DATA INFILE("s3a://bucket/orders/dt=2024-01-1[0-5]/*.parquet")
```

---

## Anti-Patterns

1. **Not checking SHOW LOAD after trigger** — load is async; application thinks it succeeded but data is still loading.
2. **No `timeout` property** — default timeout may kill large loads; calculate and set explicitly.
3. **`max_filter_ratio=1`** — silently drops all bad rows; set ≤ 0.01 for production loads.
4. **Duplicate labels without checking** — if label exists and FINISHED, load is idempotent; if FAILED, you need to fix + use new label.
5. **Loading many tiny files** — creates excessive tablets; pre-merge small files with `spark.sql("OPTIMIZE")` or AWS S3 `CopyObject` before loading.
6. **No partition-level ANALYZE after load** — CBO uses stale stats; add ANALYZE step in pipeline.
7. **Using `s3://` prefix instead of `s3a://`** — StarRocks requires `s3a://` for AWS S3.

---

## References

- Broker Load docs: `docs.starrocks.io/docs/loading/BrokerLoad/`
- S3 credentials: `docs.starrocks.io/docs/integrations/loading_tools/AWS_S3/`
- Related skills: `[[starrocks-stream-load]]`, `[[starrocks-files-ingestion]]`, `[[starrocks-partitioning]]`
