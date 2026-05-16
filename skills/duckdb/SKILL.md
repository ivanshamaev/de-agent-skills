---
name: duckdb
description: DuckDB in-process OLAP analytics — reading Parquet/CSV/JSON/Iceberg/Delta from S3/local, SQL (window functions, PIVOT, ASOF join, QUALIFY), Python API (duckdb.connect, fetchdf, register, UDFs), extensions (httpfs/iceberg/delta/postgres), performance tuning, COPY TO, persistent DB
---

# DuckDB

## When to Use

Activate this skill when the task involves:
- Querying Parquet, CSV, JSON, Iceberg, or Delta files directly with SQL
- Building local analytics pipelines without a database server
- Using DuckDB as an ETL engine for file-to-file transforms (Parquet → Parquet)
- Running DuckDB from Python with DataFrame integration (pandas/Arrow/Polars)
- Reading data from S3, GCS, Azure, or HTTP endpoints
- Using DuckDB as a lightweight alternative to Spark for single-machine workloads
- Replacing pandas for large-file analytical queries

---

## Core Concepts

```
DuckDB is an embedded analytical database — runs in-process, no server.

┌─────────────────────────────────────────────────────────────┐
│  DuckDB Architecture                                        │
│                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ Parquet  │  │ CSV/JSON │  │ Iceberg  │  │ Delta    │  │
│  │ (S3/GCS) │  │ (local)  │  │ (REST)   │  │          │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  │
│       └─────────────┼─────────────┴──────────────┘        │
│                     ▼                                      │
│          ┌──────────────────┐                              │
│          │  DuckDB Engine   │                              │
│          │  Vectorized MPP  │                              │
│          │  Columnar exec   │                              │
│          └────────┬─────────┘                              │
│                   ▼                                        │
│  ┌───────────┐  ┌──────────┐  ┌──────────────────────┐   │
│  │ Parquet   │  │ CSV      │  │ Pandas / Arrow /      │   │
│  │ (output)  │  │ (output) │  │ Polars DataFrame      │   │
│  └───────────┘  └──────────┘  └──────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**Key property**: DuckDB reads files (Parquet, CSV, JSON, Iceberg) directly — no loading step required. It pushes predicates and projections down into the file scan.

---

## Installation

```bash
pip install duckdb                       # Python API
pip install duckdb "duckdb[adbc]"       # ADBC driver (Arrow Database Connectivity)

# CLI (no Python needed)
curl -fsSL https://install.duckdb.org | sh
duckdb                                  # in-memory shell
duckdb mydb.duckdb                      # persistent DB shell
```

---

## Python API

### Connection Modes

```python
import duckdb

# In-memory (session-scoped, no persistence)
con = duckdb.connect(":memory:")

# Persistent (file-backed)
con = duckdb.connect("analytics.duckdb")

# With configuration
con = duckdb.connect(
    "analytics.duckdb",
    config={
        "threads": 8,
        "memory_limit": "8GB",
        "default_order": "ASC",
        "temp_directory": "/tmp/duckdb_spill",
    },
)

# Module-level (default in-memory connection — fine for scripts)
duckdb.sql("SELECT 42")
```

### Querying and Fetching Results

```python
# Fetch as pandas DataFrame
df = con.sql("SELECT * FROM read_parquet('data/*.parquet')").df()

# Fetch as Arrow Table
arrow_tbl = con.sql("SELECT * FROM orders").arrow()

# Fetch as list of tuples (DB-API)
rows = con.execute("SELECT order_id, total FROM orders WHERE total > ?", [100]).fetchall()

# Single row
row = con.execute("SELECT COUNT(*) FROM orders").fetchone()
count = row[0]

# Print results (interactive exploration)
con.sql("SELECT category, SUM(total) FROM orders GROUP BY 1").show()

# Describe result schema
con.sql("DESCRIBE SELECT * FROM orders").show()
```

### Registering Python Objects

```python
import pandas as pd

orders_df = pd.read_csv("orders.csv")

# Register DataFrame as a virtual table
con.register("orders", orders_df)
result = con.sql("SELECT status, COUNT(*) FROM orders GROUP BY status").df()

# Register directly in SQL (DuckDB auto-detects Python variables in scope)
result = duckdb.sql("SELECT * FROM orders_df LIMIT 5").df()  # detects local variable

# Create persistent table from DataFrame
con.sql("CREATE OR REPLACE TABLE orders AS SELECT * FROM orders_df")
```

---

## Reading Files

### Parquet

```python
# Single file
con.sql("SELECT * FROM 'data/orders.parquet'").show()

# Glob pattern
con.sql("SELECT * FROM 'data/orders/*.parquet'").show()

# Explicit function with options
con.sql("""
    SELECT * FROM read_parquet(
        'data/orders/*.parquet',
        hive_partitioning = true,    -- recognize date= / region= dirs
        union_by_name    = true      -- handle mismatched schemas across files
    )
    WHERE region = 'us' AND date >= '2024-01-01'
""").show()

# S3 (requires httpfs extension — auto-loaded)
con.sql("""
    SELECT category, SUM(total) AS revenue
    FROM read_parquet('s3://data-lake/silver/orders/**/*.parquet', hive_partitioning=true)
    WHERE event_date = '2024-03-15'
    GROUP BY 1
    ORDER BY 2 DESC
""").df()
```

### CSV

```python
# Auto-detect schema
con.sql("SELECT * FROM 'data/customers.csv'").show()

# With options
con.sql("""
    SELECT * FROM read_csv(
        'data/customers.csv',
        header    = true,
        delimiter = ',',
        columns   = {'id': 'INTEGER', 'email': 'VARCHAR', 'created_at': 'TIMESTAMP'},
        ignore_errors = true         -- skip malformed rows
    )
""").df()

# Multiple files
con.sql("SELECT * FROM read_csv('data/part_*.csv', union_by_name=true)").show()
```

### JSON

```python
con.sql("SELECT * FROM 'data/events.json'").show()
con.sql("SELECT * FROM read_json('data/events.ndjson', format='newline_delimited')").show()
con.sql("SELECT * FROM read_json('data/events/*.json', union_by_name=true)").show()
```

### Iceberg (extension)

```python
con.install_extension("iceberg")
con.load_extension("iceberg")

# Query Iceberg table via REST catalog
con.sql("""
    SELECT * FROM iceberg_scan(
        'rest+http://iceberg-rest:8181/v1/namespaces/silver/tables/orders'
    )
""").show()

# Or via S3 metadata path
con.sql("SELECT * FROM iceberg_scan('s3://lake/silver/orders/')").show()
```

### Delta Lake (extension)

```python
con.install_extension("delta")
con.load_extension("delta")

con.sql("SELECT * FROM delta_scan('s3://lake/silver/orders/')").show()
```

---

## SQL Features

### DDL

```sql
-- Create table from file
CREATE TABLE orders AS SELECT * FROM read_parquet('data/orders.parquet');

-- Create schema
CREATE SCHEMA IF NOT EXISTS silver;

-- Create persistent table with schema
CREATE OR REPLACE TABLE silver.orders (
    order_id    BIGINT  PRIMARY KEY,
    customer_id BIGINT  NOT NULL,
    status      VARCHAR,
    total       DECIMAL(10, 2),
    created_at  TIMESTAMP
);

-- Create view over Parquet files
CREATE OR REPLACE VIEW silver.events AS
SELECT * FROM read_parquet('s3://lake/silver/events/**/*.parquet', hive_partitioning=true);
```

### Window Functions

```sql
SELECT
    order_date,
    region,
    SUM(total)  OVER (PARTITION BY region ORDER BY order_date) AS cumulative_revenue,
    AVG(total)  OVER (PARTITION BY region ORDER BY order_date
                      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_avg,
    LAG(total)  OVER (PARTITION BY region ORDER BY order_date)  AS prev_day_total,
    RANK()      OVER (PARTITION BY order_date ORDER BY total DESC) AS daily_rank
FROM daily_orders;
```

### QUALIFY — Post-Window Filter

```sql
-- Top 3 customers by revenue per region (QUALIFY replaces outer subquery)
SELECT
    region,
    customer_id,
    SUM(total) AS revenue,
    RANK() OVER (PARTITION BY region ORDER BY SUM(total) DESC) AS rnk
FROM orders
GROUP BY region, customer_id
QUALIFY rnk <= 3;
```

### PIVOT / UNPIVOT

```sql
-- Auto-pivot: regions become columns
PIVOT orders
ON region
USING SUM(total)
GROUP BY order_date;

-- Explicit pivot with ordering
PIVOT orders
ON status IN ('pending', 'shipped', 'delivered', 'cancelled')
USING COUNT(*) AS cnt
GROUP BY DATE_TRUNC('month', created_at) AS month;

-- UNPIVOT: columns back to rows
UNPIVOT (SELECT * FROM pivoted_orders)
ON us_revenue, eu_revenue, apac_revenue
INTO NAME region VALUE revenue;
```

### ASOF JOIN — Nearest-Key Join

```sql
-- Join each trade to the last price before it
SELECT
    t.trade_id,
    t.executed_at,
    t.quantity,
    p.price
FROM trades AS t
ASOF JOIN prices AS p
    ON t.symbol = p.symbol
    AND t.executed_at >= p.price_time;
```

### SAMPLE — Fast Approximate Queries

```sql
-- 10% random sample
SELECT * FROM orders USING SAMPLE 10%;
SELECT * FROM orders USING SAMPLE 100000 ROWS;
SELECT * FROM orders USING SAMPLE 10% (bernoulli, 42);  -- seed for reproducibility
```

### COPY TO — Export

```sql
-- Parquet (with compression and partitioning)
COPY (SELECT * FROM silver.orders)
TO 's3://lake/export/orders/'
(FORMAT parquet, PARTITION_BY (order_date), COMPRESSION zstd);

-- CSV
COPY (SELECT order_id, customer_id, total FROM orders WHERE status = 'delivered')
TO '/tmp/delivered_orders.csv'
(FORMAT csv, HEADER true, DELIMITER ',');

-- JSON Lines
COPY (SELECT * FROM events LIMIT 10000)
TO 'events.ndjson'
(FORMAT json, ARRAY false);
```

### Aggregates and Structs

```sql
-- Struct aggregation
SELECT
    customer_id,
    LIST(STRUCT_PACK(order_id, total, created_at) ORDER BY created_at) AS order_history
FROM orders
GROUP BY customer_id;

-- Unnest array/struct
SELECT
    customer_id,
    UNNEST(order_history).order_id,
    UNNEST(order_history).total
FROM customer_orders;

-- Approximate count distinct (HyperLogLog)
SELECT APPROX_COUNT_DISTINCT(user_id) FROM events;
```

---

## Extensions

Install and load extensions (most auto-load on first use since DuckDB 0.10):

```python
con.install_extension("httpfs")     # S3, GCS, Azure, HTTP
con.install_extension("iceberg")    # Apache Iceberg tables
con.install_extension("delta")      # Delta Lake tables
con.install_extension("postgres")   # PostgreSQL scanner
con.install_extension("mysql")      # MySQL scanner
con.install_extension("spatial")    # Spatial / GeoJSON
con.install_extension("json")       # JSON functions (usually built-in)
con.install_extension("fts")        # Full-text search
```

Or in SQL:
```sql
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;
```

### S3 Configuration

```python
con.sql("""
    CREATE SECRET s3_prod (
        TYPE s3,
        KEY_ID     'AKIAIOSFODNN7EXAMPLE',
        SECRET     'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        REGION     'us-east-1'
    )
""")

# MinIO / on-prem S3
con.sql("""
    CREATE SECRET minio (
        TYPE s3,
        KEY_ID    'minioadmin',
        SECRET    'minioadmin',
        ENDPOINT  'minio.local:9000',
        USE_SSL   false,
        URL_STYLE 'path'
    )
""")
```

### PostgreSQL Scanner

```python
con.install_extension("postgres")
con.load_extension("postgres")

# Attach a PostgreSQL database
con.sql("""
    ATTACH 'host=prod-db port=5432 dbname=analytics user=analyst password=secret'
    AS pg_prod (TYPE postgres, READ_ONLY)
""")

# Query directly
con.sql("SELECT * FROM pg_prod.public.orders LIMIT 100").df()

# Join DuckDB table with Postgres table
con.sql("""
    SELECT o.order_id, c.email
    FROM pg_prod.public.orders o
    JOIN local_customers c ON o.customer_id = c.customer_id
""").df()
```

---

## User-Defined Functions (UDFs)

### Scalar UDF (Python)

```python
def normalize_email(email: str) -> str:
    if email is None:
        return None
    return email.lower().strip()

con.create_function(
    "normalize_email",
    normalize_email,
    ["VARCHAR"],
    "VARCHAR",
)

con.sql("SELECT normalize_email(email) FROM customers").df()
```

### Vectorized UDF (Apache Arrow — much faster)

```python
import pyarrow.compute as pc
import pyarrow as pa

def fast_discount(prices: pa.Array) -> pa.Array:
    return pc.multiply(prices, 0.85)

con.create_function(
    "apply_discount",
    fast_discount,
    [duckdb.typing.DOUBLE],
    duckdb.typing.DOUBLE,
    type="arrow",
)
```

---

## Performance Tuning

### Configuration Settings

```python
con = duckdb.connect(config={
    "threads":              8,           # default: all logical CPUs
    "memory_limit":         "16GB",      # RAM limit before spill to disk
    "temp_directory":       "/ssd/tmp",  # spill location (fast SSD preferred)
    "max_memory":           "16GB",      # alias for memory_limit
    "enable_object_cache":  True,        # cache Parquet metadata
    "disabled_filesystems": "LocalFileSystem",  # restrict to S3 only
})
```

SQL:
```sql
SET threads = 8;
SET memory_limit = '16GB';
SET temp_directory = '/ssd/duckdb_tmp';
SET enable_object_cache = true;
```

### EXPLAIN and Profiling

```sql
-- Logical plan
EXPLAIN SELECT category, SUM(total) FROM orders GROUP BY 1;

-- Physical plan with cardinality estimates
EXPLAIN ANALYZE SELECT category, SUM(total) FROM orders GROUP BY 1;
```

```python
# JSON profiling
con.sql("PRAGMA enable_profiling='json'")
con.sql("PRAGMA profiling_output='/tmp/profile.json'")
con.sql("SELECT category, SUM(total) FROM orders GROUP BY 1").fetchall()
con.sql("PRAGMA disable_profiling")
```

### File Layout Tips

| Pattern | Recommendation |
|---------|---------------|
| Parquet file size | 128 MB – 1 GB per file |
| Row group size | 100k–1M rows |
| Compression | ZSTD (best ratio) or SNAPPY (fastest) |
| Hive partitioning | Partition by high-selectivity columns (date, region) |
| Column ordering | Put frequently-filtered columns first in ORDER BY |
| Statistics | DuckDB reads Parquet column statistics for predicate pushdown |

---

## Common Patterns

### ETL: Parquet → Transform → Parquet

```python
import duckdb

con = duckdb.connect(config={"threads": 8, "memory_limit": "8GB"})

# Install extensions once
con.sql("INSTALL httpfs; LOAD httpfs;")

# Set S3 credentials
con.sql("""
    CREATE SECRET s3 (TYPE s3, KEY_ID $key, SECRET $secret, REGION 'us-east-1')
""", {"key": "AKID...", "secret": "abc..."})

# Transform
con.sql("""
    COPY (
        SELECT
            order_id,
            customer_id,
            total / 100.0                                AS total_usd,
            LOWER(status)                                AS status,
            DATE_TRUNC('day', created_at)::DATE          AS created_date,
            SUM(total / 100.0) OVER (
                PARTITION BY customer_id
                ORDER BY created_at
                ROWS UNBOUNDED PRECEDING
            )                                            AS customer_ltv
        FROM read_parquet(
            's3://data-lake/bronze/orders/**/*.parquet',
            hive_partitioning = true
        )
        WHERE created_at >= '2024-01-01'
    )
    TO 's3://data-lake/silver/orders/'
    (FORMAT parquet, PARTITION_BY (created_date), COMPRESSION zstd)
""")
```

### Replace Pandas for Large Files

```python
# pandas (slow for large files): 
df = pd.read_parquet("big_file.parquet")          # reads everything into RAM
result = df.groupby("category")["revenue"].sum()

# DuckDB (vectorized, columnar, only reads needed columns):
result = duckdb.sql("""
    SELECT category, SUM(revenue) AS total
    FROM 'big_file.parquet'
    GROUP BY category
    ORDER BY total DESC
""").df()
```

### Incremental Processing with DuckDB + Persistent DB

```python
con = duckdb.connect("pipeline.duckdb")

con.sql("""
    CREATE TABLE IF NOT EXISTS watermark (
        table_name VARCHAR PRIMARY KEY,
        max_ts     TIMESTAMP
    )
""")

# Load only new records
def load_incremental(table: str, source_path: str):
    row = con.execute(
        "SELECT max_ts FROM watermark WHERE table_name = ?", [table]
    ).fetchone()
    last_ts = row[0] if row else "1970-01-01"

    con.sql(f"""
        INSERT INTO {table}
        SELECT * FROM read_parquet('{source_path}/**/*.parquet', hive_partitioning=true)
        WHERE updated_at > '{last_ts}'
    """)

    con.sql(f"""
        INSERT OR REPLACE INTO watermark
        SELECT '{table}', MAX(updated_at) FROM {table}
    """)
```

---

## Anti-Patterns

1. **Using DuckDB for OLTP workloads** — DuckDB is column-oriented and optimized for analytics. Concurrent writes and point-lookups are slow. Use PostgreSQL for OLTP.

2. **Opening the same persistent database from multiple processes simultaneously** — DuckDB supports only one writer. Use MotherDuck or a dedicated service for multi-process access.

3. **Not setting `memory_limit`** — without a limit, DuckDB will use all available RAM. Set to ~70% of system memory and configure `temp_directory` for overflow.

4. **Reading many small Parquet files without coalescing** — thousands of 1MB files are slower than one 1GB file due to metadata overhead. Compact files to 128MB–1GB before querying.

5. **Using `USING SAMPLE` without a seed for reproducible analytics** — random sampling gives different results each run. Always specify a seed: `USING SAMPLE 10% (bernoulli, 42)`.

6. **Not using hive partitioning on partitioned S3 data** — without `hive_partitioning=true`, DuckDB reads all files regardless of partition filters. This can be 100x slower.

7. **Loading entire DataFrames into DuckDB tables instead of querying directly** — for one-time reads, `SELECT * FROM read_parquet('file.parquet')` is faster than loading first. Only create tables for frequently-reused data.

8. **Using Python scalar UDFs for CPU-intensive transforms** — scalar UDFs invoke Python row-by-row and are slow. Use vectorized Arrow UDFs or push logic into SQL for 10–100x better performance.

9. **Ignoring `EXPLAIN ANALYZE`** — DuckDB's planner makes different choices based on statistics. Always profile unexpected slowness with `EXPLAIN ANALYZE` before optimizing blindly.

---

## References to Consult When Needed

- DuckDB documentation: `duckdb.org/docs/`
- Python API reference: `duckdb.org/docs/api/python/`
- Extensions list: `duckdb.org/docs/extensions/overview.html`
- Iceberg integration: `duckdb.org/docs/extensions/iceberg.html`
- S3 / httpfs: `duckdb.org/docs/extensions/httpfs/s3api.html`
- Performance guide: `duckdb.org/docs/guides/performance/overview.html`
