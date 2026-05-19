---
name: starrocks-stream-load
description: StarRocks Stream Load — HTTP PUT API (curl/Python requests), CSV/JSON format parameters (column_separator/jsonpaths/strip_outer_array), columns mapping, partial_update for Primary Key tables, label idempotency, max_filter_ratio, timeout tuning, merge commit mode (3.4+), response JSON parsing, Python bulk loader pattern, error diagnosis
---

# StarRocks Stream Load

## When to Use

- Micro-batch loading from application code (Python/Java/Go)
- Loading files from local disk or streaming buffers
- Real-time CDC sinks with upsert/partial update semantics
- ETL step that produces data in memory and needs to push to StarRocks
- Alternative to COPY INTO for simple push-based ingestion

Not for: large S3/HDFS file ingestion (use Broker Load) or Kafka continuous ingestion (use Routine Load).

---

## Core Concepts

Stream Load uses an HTTP PUT request directly to the FE (or BE) node. The request body is the data payload (CSV or JSON).

```
Client → PUT /api/{db}/{table}/_stream_load → FE → BE tablets
```

- Transactional: commit or rollback atomically
- Synchronous: response arrives after data is committed
- Label: unique string per load for idempotency (retry-safe)

---

## Basic CSV Load

```bash
# Load CSV from file
curl --location-trusted \
  -u root:password \
  -H "label:order_load_20240115_001" \
  -H "Expect:100-continue" \
  -H "column_separator:," \
  -H "columns:order_id,customer_id,amount,created_at" \
  -T /data/orders_20240115.csv \
  -XPUT \
  "http://fe-host:8030/api/sales/orders/_stream_load"
```

### CSV with Custom Separator

```bash
# Tab-separated
curl ... -H "column_separator:\t" ...

# Multi-char separator (up to 50 bytes)
curl ... -H "column_separator:|" ...

# Null value representation
curl ... -H "null_values_indicator:\N" ...
```

---

## JSON Load

### Single JSON Object Per Line (NDJSON)

```bash
curl --location-trusted \
  -u root:password \
  -H "label:json_load_001" \
  -H "Expect:100-continue" \
  -H "format:json" \
  -H "jsonpaths:[\"$.order_id\",\"$.customer_id\",\"$.amount\"]" \
  -H "columns:order_id,customer_id,amount" \
  -T /data/orders.ndjson \
  -XPUT \
  "http://fe-host:8030/api/sales/orders/_stream_load"
```

### JSON Array (Strip Outer Array)

```bash
# Input: [{"id":1,"val":"a"},{"id":2,"val":"b"}]
curl ... \
  -H "format:json" \
  -H "strip_outer_array:true" \
  -H "jsonpaths:[\"$.id\",\"$.val\"]" \
  ...
```

### JSON with Computed Columns

```bash
# Map JSON fields to table columns with expressions
curl ... \
  -H "format:json" \
  -H "jsonpaths:[\"$.ts\",\"$.user_id\",\"$.amount\"]" \
  -H "columns:ts_str,user_id,amount,ts=str_to_date(ts_str,'%Y-%m-%dT%H:%i:%s')" \
  ...
```

---

## All Key Headers Reference

| Header | Default | Description |
|--------|---------|-------------|
| `label` | — | **Required.** Unique load label for idempotency |
| `column_separator` | `\t` | Field delimiter for CSV |
| `row_delimiter` | `\n` | Row separator for CSV |
| `columns` | all columns | Column mapping / computed expressions |
| `where` | — | Filter rows to load |
| `format` | csv | `csv` / `json` / `avro` |
| `jsonpaths` | — | JSON field paths array |
| `strip_outer_array` | false | Strip outer `[...]` in JSON |
| `max_filter_ratio` | 0 | Max fraction of bad rows allowed (0=reject any error) |
| `strict_mode` | false | Reject rows with type mismatch |
| `timeout` | 600 | Job timeout in seconds |
| `timezone` | `UTC` | Timezone for datetime parsing |
| `partial_update` | false | Only update specified columns (PK table) |
| `merge_condition` | — | Conditional MERGE for PK table |
| `enable_merge_commit` | false | Batch concurrent loads (3.4+) |

---

## Partial Update (Primary Key Table)

Update only specific columns without touching others:

```bash
# Only update `status` and `updated_at` columns
curl --location-trusted \
  -u root:password \
  -H "label:partial_update_001" \
  -H "Expect:100-continue" \
  -H "format:json" \
  -H "partial_update:true" \
  -H "columns:order_id,status,updated_at" \
  -H "jsonpaths:[\"$.order_id\",\"$.status\",\"$.updated_at\"]" \
  -d '{"order_id":12345,"status":"shipped","updated_at":"2024-01-15 10:30:00"}' \
  -XPUT \
  "http://fe-host:8030/api/sales/orders/_stream_load"
```

Partial update requires: table must be Primary Key type; `columns` header must list only the columns being updated (must include PK columns).

---

## Error Handling and Response

### Success Response

```json
{
    "TxnId": 1234567,
    "Label": "order_load_20240115_001",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 10000,
    "NumberLoadedRows": 10000,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 1048576,
    "LoadTimeMs": 1250,
    "BeginTxnTimeMs": 5,
    "StreamLoadPutTimeMs": 10,
    "ReadDataTimeMs": 800,
    "WriteDataTimeMs": 435
}
```

### Partial Failure Response

```json
{
    "Status": "Success",
    "NumberFilteredRows": 50,
    "ErrorURL": "http://be-host:8040/api/_load_error_log?file=error_log_xxx"
}
```

Fetch error details:
```bash
curl "http://be-host:8040/api/_load_error_log?file=error_log_xxx" | head -50
```

### Common Status Values

| Status | Meaning |
|--------|---------|
| `Success` | All rows loaded |
| `Publish Timeout` | Load committed but tablets still publishing; data will be visible soon |
| `Label Already Exists` | Duplicate label — check if previous load succeeded before retrying |
| `Fail` | Load failed — check `Message` field |

---

## Python Client Pattern

```python
import requests
import json
import hashlib
import time
from typing import List, Dict, Any


class StarRocksStreamLoader:
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.base_url = f"http://{host}:{port}/api/{database}"
        self.auth = (user, password)

    def load_json(
        self,
        table: str,
        records: List[Dict[str, Any]],
        label: str | None = None,
        columns: List[str] | None = None,
        partial_update: bool = False,
    ) -> dict:
        if not label:
            content_hash = hashlib.md5(
                json.dumps(records, default=str).encode()
            ).hexdigest()[:8]
            label = f"{table}_{int(time.time())}_{content_hash}"

        ndjson = "\n".join(json.dumps(r, default=str) for r in records)

        headers = {
            "label": label,
            "Expect": "100-continue",
            "format": "json",
            "strip_outer_array": "false",
        }
        if columns:
            col_str = ",".join(columns)
            headers["columns"] = col_str
            headers["jsonpaths"] = json.dumps([f"$.{c}" for c in columns])
        if partial_update:
            headers["partial_update"] = "true"

        url = f"{self.base_url}/{table}/_stream_load"
        resp = requests.put(
            url,
            headers=headers,
            data=ndjson.encode("utf-8"),
            auth=self.auth,
            timeout=120,
        )
        resp.raise_for_status()
        result = resp.json()

        if result["Status"] not in ("Success", "Publish Timeout"):
            raise RuntimeError(
                f"Stream load failed: {result['Status']} — {result.get('Message', '')}\n"
                f"Error URL: {result.get('ErrorURL', 'none')}"
            )

        return result

    def load_csv(
        self,
        table: str,
        csv_data: str,
        label: str,
        columns: str | None = None,
        separator: str = ",",
        max_filter_ratio: float = 0.0,
    ) -> dict:
        headers = {
            "label": label,
            "Expect": "100-continue",
            "format": "csv",
            "column_separator": separator,
            "max_filter_ratio": str(max_filter_ratio),
        }
        if columns:
            headers["columns"] = columns

        url = f"{self.base_url}/{table}/_stream_load"
        resp = requests.put(
            url,
            headers=headers,
            data=csv_data.encode("utf-8"),
            auth=self.auth,
            timeout=300,
        )
        resp.raise_for_status()
        result = resp.json()

        if result["Status"] not in ("Success", "Publish Timeout"):
            raise RuntimeError(f"Stream load failed: {result}")
        return result


# Usage
loader = StarRocksStreamLoader("fe-host", 8030, "root", "pass", "sales")

orders = [
    {"order_id": 1, "customer_id": 101, "amount": 99.99, "created_at": "2024-01-15"},
    {"order_id": 2, "customer_id": 102, "amount": 149.99, "created_at": "2024-01-15"},
]

result = loader.load_json("orders", orders)
print(f"Loaded {result['NumberLoadedRows']} rows in {result['LoadTimeMs']}ms")
```

---

## Idempotency with Labels

Labels provide at-least-once with idempotency:

```python
def load_with_retry(loader, table, records, label, max_retries=3):
    for attempt in range(max_retries):
        try:
            return loader.load_json(table, records, label=label)
        except Exception as e:
            error_msg = str(e)
            if "Label Already Exists" in error_msg:
                # Previous load succeeded — check result
                print(f"Label {label} already exists — load previously succeeded")
                return {"Status": "Success", "NumberLoadedRows": len(records)}
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
```

Label format recommendation: `{table}_{date}_{batch_id}` — deterministic and debuggable.

Labels expire after `label_keep_max_second` seconds (default 3 days).

---

## Merge Commit Mode (StarRocks 3.4+)

For high-concurrency micro-batch loads, merge commit batches many small requests into one transaction:

```bash
curl ... \
  -H "enable_merge_commit:true" \
  -H "merge_commit_interval_ms:1000" \
  -H "merge_commit_async:true" \  # return immediately, don't wait for commit
  ...
```

Merge commit reduces write amplification from many small loads. Use when:
- > 100 concurrent stream load requests per second
- Each request < 1MB
- Latency tolerance > 1 second

---

## Performance Tuning

### Optimal Batch Size

| Data Size | Recommended Approach |
|-----------|---------------------|
| < 1 MB | Enable merge_commit_mode |
| 1 MB – 100 MB | Standard stream load |
| 100 MB – 1 GB | Split into 100 MB chunks |
| > 1 GB | Use Broker Load instead |

### Parallelism

```python
# Load multiple files in parallel using ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed

def load_file(path: str) -> dict:
    label = f"batch_{path.stem}_{int(time.time())}"
    with open(path) as f:
        return loader.load_csv("orders", f.read(), label)

with ThreadPoolExecutor(max_workers=8) as pool:
    futures = {pool.submit(load_file, p): p for p in csv_files}
    for fut in as_completed(futures):
        result = fut.result()
        print(f"{futures[fut]}: {result['NumberLoadedRows']} rows")
```

### Timeout Calculation

```
timeout_seconds = data_size_bytes / (cluster_load_speed_bytes_per_sec * 0.7)
```

Typical cluster load speed: 100-300 MB/s per BE.

---

## Anti-Patterns

1. **No label** — cannot safely retry; duplicates on transient failures.
2. **max_filter_ratio=1** in production — silently ignores all bad data; keep ≤ 0.01 for production.
3. **Loading 10GB files** — stream load holds entire payload in memory; split into ≤ 1GB chunks.
4. **strict_mode=false for schema changes** — type mismatches silently truncate values; enable strict_mode in staging.
5. **Not checking ErrorURL** — `NumberFilteredRows > 0` means data loss; always fetch error log.
6. **Sending requests to BE directly** — send to FE for proper load routing and monitoring; BE direct is only for internal optimization.
7. **No Expect:100-continue** — without it, curl sends all data before getting auth failure; wastes bandwidth.

---

## References

- Stream Load docs: `docs.starrocks.io/docs/loading/StreamLoad/`
- Partial update: `docs.starrocks.io/docs/loading/Load_to_Primary_Key_tables/`
- Related skills: `[[starrocks-routine-load-kafka]]`, `[[starrocks-cdc-pipeline]]`, `[[starrocks-ddl-table-types]]`
