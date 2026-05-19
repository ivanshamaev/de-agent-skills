---
name: trino-explain-plan-review
description: Trino EXPLAIN and EXPLAIN ANALYZE plan reading — DISTRIBUTED/LOGICAL/IO/VALIDATE formats, fragment types (SINGLE/HASH/ROUND_ROBIN/BROADCAST/SOURCE), exchange node analysis, stage bottleneck detection, data skew identification via task row-count variance, spill detection, operator-level metrics (ScanFilterProject/HashJoin/Aggregation/LocalExchange), cost estimate vs actual row disparity, slow plan patterns and fixes
---

# Trino EXPLAIN Plan Review

## When to Use

- A query is unexpectedly slow and you need to find the bottleneck
- Verifying that predicate/partition pushdown is actually happening
- Detecting data skew across tasks
- Comparing plan cost estimates against actual runtime metrics
- Reviewing query plan before deploying to production

---

## EXPLAIN Syntax Reference

```sql
-- Default: DISTRIBUTED plan (most useful for production diagnosis)
EXPLAIN SELECT ...;

-- Distributed plan (explicit)
EXPLAIN (TYPE DISTRIBUTED) SELECT ...;

-- Logical plan (high-level operators)
EXPLAIN (TYPE LOGICAL) SELECT ...;

-- IO plan (what tables/columns are read, with constraints)
EXPLAIN (TYPE IO) SELECT ...;

-- Validate syntax only — returns TRUE/FALSE
EXPLAIN (TYPE VALIDATE) SELECT ...;

-- Output as JSON (useful for programmatic analysis)
EXPLAIN (FORMAT JSON) SELECT ...;

-- Runtime metrics — execute the query and report actual operators
EXPLAIN ANALYZE SELECT ...;

-- Full verbose metrics: percentile distributions, driver counts
EXPLAIN ANALYZE VERBOSE SELECT ...;
```

---

## DISTRIBUTED Plan: Fragment Types

A distributed plan splits into numbered **Fragments** separated by exchange operators. Each fragment runs on specific nodes.

| Fragment Type | Description | Runs On |
|---------------|-------------|---------|
| `SINGLE` | Final result aggregation | Coordinator |
| `SOURCE` | Reads data from connector splits | Where splits are located |
| `HASH` | Partitioned by hash key (joins, GROUP BY) | All workers, fixed partition |
| `ROUND_ROBIN` | Round-robin distribution | All workers |
| `BROADCAST` | Build side replicated to all workers | All workers |

```
Fragment 0 [SINGLE]         ← coordinator collects final output
  Output[order_id, total]
    └── RemoteStreamingExchange[GATHER]
          └── Fragment 1 [HASH]   ← join stage, repartitioned by join key
                HashJoin[order_id]
                  ├── RemoteExchange[REPARTITION] ← probe side from Fragment 2
                  └── LocalExchange[BROADCAST]    ← build side from Fragment 3
Fragment 1 ...
Fragment 2 [SOURCE]         ← scans fact_orders splits
  ScanFilterProject[table=fact_orders, ...]
Fragment 3 [SOURCE]         ← scans dim_customer splits
  ScanFilterProject[table=dim_customer, ...]
```

**Key insight**: Every `RemoteExchange` = network shuffle. Minimize them.

---

## Reading an EXPLAIN DISTRIBUTED Plan

```sql
EXPLAIN
SELECT o.customer_id, SUM(o.amount) AS total
FROM iceberg.silver.orders o
JOIN iceberg.gold.dim_customer c ON o.customer_id = c.customer_id
WHERE o.order_date >= DATE '2024-01-01'
GROUP BY o.customer_id;
```

Look for:

| What to check | Where to look | Good sign | Bad sign |
|---------------|--------------|-----------|----------|
| Partition pruning | `ScanFilterProject` constraints | `partitions=42` (pruned) | `partitions=3650` (full scan) |
| Predicate pushdown | Presence of `Filter` above scan | No `Filter` node | `Filter[...]` above `TableScan` |
| Join strategy | `HashJoin` type | `BROADCAST` (small dim) | `PARTITIONED` for tiny table |
| Exchange count | Number of `RemoteExchange` nodes | ≤ 3 for simple join | > 5 suggests over-shuffling |
| Broadcast size | `LocalExchange[BROADCAST]` | Small row count | Millions of rows → OOM risk |

---

## EXPLAIN ANALYZE: Runtime Metrics

`EXPLAIN ANALYZE` runs the query and annotates the plan with actual metrics.

```sql
EXPLAIN ANALYZE VERBOSE
SELECT customer_id, SUM(amount)
FROM iceberg.silver.orders
WHERE order_date = DATE '2024-06-01'
GROUP BY customer_id;
```

**Sample output interpretation:**

```
Fragment 1 [HASH]
  CPU: 45.23s, Scheduled: 62.10s, Blocked: 3.21s (I/O: 1.5s, Overall: 1.7s)
  Output: 1234567 rows (98.5MB), Filtered: 78.3%
  Input avg.: 15.2M rows, Input std.dev.: 234.5% ← HIGH SKEW
  
  - ScanFilterProject[table=silver.orders] {
      Input: 15.2M avg (range: 1K–45M)          ← SKEWED splits
      CPU fraction: 35%
    }
```

### Metrics to Flag

| Metric | Threshold | Meaning | Fix |
|--------|-----------|---------|-----|
| `Input std.dev.` | > 50% | Data skew across tasks | Add bucket/salt column, rewrite partition |
| `Blocked: I/O` | > 10% of Scheduled | Slow object storage reads | Check S3/MinIO throughput, increase workers |
| `Blocked: Overall` | Persistent | Memory pressure / slow consumers | Increase `query.max-memory-per-node` |
| CPU fraction | < 20% on scan | Most time in network | Reduce exchanges, increase partition pushdown |
| Estimated rows >> Actual | 100x difference | Stale statistics | Run `ANALYZE table` |

---

## Common Plan Patterns and Fixes

### Pattern 1: Missing Partition Pruning

```
-- BAD PLAN: full scan despite date filter
ScanFilterProject[table=silver.orders]
  Estimates: {rows: 3650000000, cpu: 3.65T, ...}
  Filter: (YEAR(order_date) = 2024)   ← function wrapping prevents pushdown

-- FIX: use range predicate
WHERE order_date >= DATE '2024-01-01' AND order_date < DATE '2025-01-01'

-- GOOD PLAN: only matching partitions read
ScanFilterProject[table=silver.orders]
  Estimates: {rows: 365000000, cpu: 365B, ...}
  Constraint: order_date in [2024-01-01, 2024-12-31]
```

### Pattern 2: Accidental Cross Join

```
-- BAD: missing join condition creates cross join
Fragment 1 [BROADCAST]
  CrossJoin
    ├── Source: fact (1B rows)
    └── Broadcast: dim (100 rows) → 100B rows output

-- FIX: add ON clause with correct column
```

### Pattern 3: Data Skew in Hash Join

```
-- SYMPTOM: one task processes 90% of rows
Fragment 2 [HASH]
  Input avg.: 5M rows, Input std.dev.: 890%   ← extreme skew
  HashJoin[customer_id]

-- CAUSE: customer_id distribution is highly skewed (many NULLs or one hot key)
-- FIX 1: filter NULLs before join
-- FIX 2: salt the hot key
SELECT CONCAT(CAST(customer_id AS VARCHAR), '_', CAST(MOD(ABS(order_id), 8) AS VARCHAR))
```

### Pattern 4: Unnecessary Remote Exchange

```
-- BAD: intermediate aggregation forces repartition
Fragment 1 [HASH]
  RemoteExchange[REPARTITION]   ← extra shuffle
    Aggregate[partial]
      ScanFilterProject

-- GOOD when: pre-aggregation on SOURCE fragment reduces data 100x before shuffle
-- BAD when: cardinality is unchanged (COUNT DISTINCT of unique key)
```

### Pattern 5: Small Files → Too Many Splits

```
-- SYMPTOM: thousands of splits, each tiny
ScanFilterProject[table=bronze.events]
  Splits: 45000 × 1MB    ← 45K tiny files

-- FIX: run optimize to compact
ALTER TABLE iceberg.bronze.events EXECUTE optimize(file_size_threshold => '128MB');
```

---

## IO Plan: Verify Table Access

```sql
EXPLAIN (TYPE IO, FORMAT JSON)
SELECT order_id FROM iceberg.silver.orders
WHERE order_date = DATE '2024-06-01';
```

```json
{
  "inputTableColumnInfos": [
    {
      "table": {"catalog": "iceberg", "schemaTable": {"schema": "silver", "table": "orders"}},
      "columns": ["order_id", "order_date"],   -- columns actually read
      "constraint": {
        "columnConstraints": [
          {
            "columnName": "order_date",
            "nullability": "NOT_NULL",
            "domain": {"ranges": [{"low": "2024-06-01", "high": "2024-06-01"}]}
          }
        ]
      },
      "estimate": {"outputRowCount": 1234567.0, "outputSizeInBytes": 9876543.0}
    }
  ]
}
```

Use IO plan to verify:
- Only necessary columns are read (`columns` list)
- Partition constraint is applied (`domain.ranges`)
- Estimated row count is reasonable (not full table)

---

## EXPLAIN ANALYZE Verbose: Skew Detection

```sql
EXPLAIN ANALYZE VERBOSE
SELECT customer_id, COUNT(*)
FROM iceberg.silver.orders
GROUP BY customer_id;
```

In verbose output, look for:

```
Input rows distribution:
  p25: 1.2M, p50: 3.5M, p75: 8.1M, p99: 45.2M   ← p99 >> p50 = SKEW
  
Active drivers: 48/64 (25% idle) ← some tasks starved
```

High p99/p50 ratio (> 5x) indicates hot partitions requiring data redistribution.

---

## Quick EXPLAIN Checklist

```sql
-- Run EXPLAIN first:
EXPLAIN (TYPE DISTRIBUTED, FORMAT TEXT) <your query>;

-- Check:
-- [ ] Number of fragments ≤ 5 for simple queries
-- [ ] ScanFilterProject has partition constraints (not full scan)
-- [ ] No Filter node above TableScan (predicate pushed down)
-- [ ] JOIN uses BROADCAST for small build side (< 100MB)
-- [ ] No unexpected CrossJoin nodes
-- [ ] Estimated rows are reasonable (not billions for small result)

-- Then run EXPLAIN ANALYZE to get actuals:
EXPLAIN ANALYZE VERBOSE <your query>;

-- Check actuals:
-- [ ] Input std.dev. < 50% (no skew)
-- [ ] Blocked time < 5% of scheduled time
-- [ ] Estimated rows ≈ actual rows (CBO working)
-- [ ] CPU fraction > 30% of scheduled time
```

---

## Anti-Patterns

1. **Only running `EXPLAIN` without `ANALYZE`** — estimated costs are based on statistics that may be stale; always run `EXPLAIN ANALYZE` to see actual row counts.
2. **Ignoring `Input std.dev.`** — high deviation (> 200%) is the primary skew signal; it means a few tasks do most of the work while others sit idle.
3. **Missing the difference between `SINGLE` and `HASH` fragments** — `SINGLE` means one node does all the work; a `SINGLE` fragment handling billions of rows is a bottleneck.
4. **Not checking `Filter` node placement** — a `Filter` above `TableScan` means the predicate wasn't pushed into the connector; rewrite the filter to enable pushdown.
5. **Reading `TEXT` format for automated analysis** — use `FORMAT JSON` for programmatic processing; `TEXT` is for human reading only.

---

## References

- EXPLAIN syntax: `trino.io/docs/current/sql/explain.html`
- EXPLAIN ANALYZE: `trino.io/docs/current/sql/explain-analyze.html`
- Pushdown verification: `trino.io/docs/current/optimizer/pushdown.html`
- Related skills: `[[trino-query-optimization]]`, `[[trino-memory-and-spill-tuning]]`, `[[trino-iceberg-best-practices]]`
