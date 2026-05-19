---
name: trino-query-optimization
description: Trino distributed SQL query optimization — predicate/projection/aggregation pushdown, join reordering (AUTOMATIC/ELIMINATE_CROSS_JOINS), broadcast vs partitioned joins, dynamic filtering, CBO with ANALYZE, filter-early patterns, partition pruning, avoiding SELECT *, reducing shuffle, cross-catalog query cost, session property tuning, query hints, anti-patterns for slow Trino queries
---

# Trino Query Optimization

## When to Use

- A Trino query is slower than expected and you need to diagnose and fix it
- Reviewing query patterns before deploying to production
- Choosing the right partitioning, join order, or materialization strategy
- Tuning session properties for a specific workload class

---

## Optimization Hierarchy (apply in order)

```
1. Data layout    — partition pruning, sorted files, file sizing
2. Pushdown       — predicate / projection / aggregation / join
3. Join strategy  — broadcast vs partitioned, reorder
4. Dynamic filter — wait timeout, build-side filtering
5. Memory         — spill, exchange buffer, broadcast limit
6. Parallelism    — task.writer-count, task.concurrency
```

---

## 1. Predicate Pushdown — Filter Early

Always push filters to the earliest possible stage. Trino automatically pushes WHERE predicates into connector scans.

```sql
-- SLOW: full table scan, filter in Trino
SELECT customer_id, SUM(amount)
FROM iceberg.gold.orders
GROUP BY customer_id;

-- FAST: partition pruning via predicate pushdown
SELECT customer_id, SUM(amount)
FROM iceberg.gold.orders
WHERE order_date >= DATE '2024-01-01'     -- partition column → file pruning
  AND status = 'completed'                -- pushed to Parquet row-group filter
GROUP BY customer_id;
```

**Identify pushdown success in EXPLAIN:**
- Predicate pushed down: no `ScanFilterProject`, constraint appears in `TableScan`
- Predicate NOT pushed down: `Filter[...]` operator appears above `TableScan`

---

## 2. Avoid SELECT * — Projection Pushdown

```sql
-- SLOW: reads all columns from Parquet/ORC
SELECT * FROM iceberg.silver.orders WHERE order_date = DATE '2024-06-01';

-- FAST: reads only 3 columns — pushes projection into connector
SELECT order_id, customer_id, amount
FROM iceberg.silver.orders
WHERE order_date = DATE '2024-06-01';
```

---

## 3. Aggregation Pushdown

Trino can push `COUNT`, `SUM`, `MIN`, `MAX`, `AVG` into JDBC connectors (PostgreSQL, MySQL):

```sql
-- FAST for PostgreSQL connector: aggregation computed on PG side
SELECT region, COUNT(*) AS orders_count
FROM postgresql.public.orders
GROUP BY region;
```

Aggregation pushdown is **NOT** applied when:
- Expression inside function: `SUM(a * b)` — compute in Trino instead
- `ROLLUP`, `CUBE`, `GROUPING SETS` present
- `WHERE` filter present (limitation in some connectors)

---

## 4. Join Optimization

### Broadcast vs Partitioned

| Strategy | When | Config |
|----------|------|--------|
| **Broadcast** | Build side < `join-max-broadcast-table-size` (default 100MB) | Automatic |
| **Partitioned** | Both tables large | Automatic |

```sql
-- Force broadcast join (small dimension table)
SELECT /*+ BROADCAST(d) */ f.order_id, d.region_name
FROM iceberg.gold.fact_orders f
JOIN iceberg.gold.dim_region d ON f.region_id = d.region_id;

-- Force partitioned join (avoid OOM from large dimension)
SELECT /*+ REPARTITION(f, d) */ f.order_id, d.category
FROM iceberg.gold.fact_orders f
JOIN iceberg.gold.dim_product d ON f.product_id = d.product_id;
```

### Join Reordering (CBO)

The optimizer reorders joins automatically when statistics exist. Ensure stats are current:

```sql
ANALYZE iceberg.silver.orders;
ANALYZE iceberg.gold.dim_customer;
```

Control join reordering:

```sql
-- Session: full automatic CBO reordering (default)
SET SESSION join_reordering_strategy = 'AUTOMATIC';

-- Session: eliminate cross joins only (use when CBO stats are stale)
SET SESSION join_reordering_strategy = 'ELIMINATE_CROSS_JOINS';

-- Config property (etc/config.properties)
-- optimizer.join-reordering-strategy=AUTOMATIC
-- join-distribution-type=AUTOMATIC
-- join-max-broadcast-table-size=300MB
```

### Syntactic Join Order (when CBO disabled)

When `join_reordering_strategy=NONE`, Trino loads the **rightmost** table into memory as the build side. Write joins largest→smallest right to left:

```sql
-- Largest table on left, smallest (broadcast candidate) on right
SELECT f.order_id, d.region_name
FROM iceberg.gold.fact_orders f         -- large: 1B rows
JOIN iceberg.gold.dim_region d          -- small: 200 rows
  ON f.region_id = d.region_id;
```

---

## 5. Dynamic Filtering

Dynamic filters propagate build-side values to the probe side at runtime, eliminating rows before they cross the network.

```sql
-- Dynamic filter automatically pushes the result of dim_product.category = 'Electronics'
-- into the fact table scan, reducing splits read
SELECT f.amount, d.category
FROM iceberg.gold.fact_sales f
JOIN iceberg.gold.dim_product d ON f.product_id = d.product_id
WHERE d.category = 'Electronics';
```

Tune dynamic filter wait:

```properties
# etc/config.properties
dynamic-filtering.small-broadcast-max-distinct-values-per-driver=1000
dynamic-filtering.small-broadcast-max-size-per-driver=512kB
dynamic-filtering.large-broadcast-max-distinct-values-per-driver=50000
dynamic-filtering.large-broadcast-max-size-per-driver=20MB
```

Session property:

```sql
SET SESSION dynamic_filter_wait_timeout = '2s';  -- wait longer for build-side completion
```

---

## 6. Partition Pruning (Iceberg)

Iceberg hidden partitions are automatically pruned when filter matches partition transform:

```sql
-- Partition: partitioning = ARRAY['day(order_date)']
-- This filter prunes all irrelevant day-partitions at the manifest level
SELECT COUNT(*) FROM iceberg.silver.orders
WHERE order_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-31';

-- BAD: function on partition column disables pruning
SELECT COUNT(*) FROM iceberg.silver.orders
WHERE DATE_TRUNC('month', order_date) = DATE '2024-01-01';  -- CAN'T prune

-- GOOD: rewrite to range filter
SELECT COUNT(*) FROM iceberg.silver.orders
WHERE order_date >= DATE '2024-01-01' AND order_date < DATE '2024-02-01';
```

---

## 7. Minimize Repartitioning (Shuffle)

Every `PARTITION BY`, `GROUP BY`, `JOIN`, and `ORDER BY` causes a shuffle (exchange). Minimize exchanges:

```sql
-- BAD: two separate shuffles
SELECT region, SUM(amount)
FROM (
    SELECT region, amount
    FROM iceberg.silver.orders
    GROUP BY region, amount   -- 1st shuffle
) sub
GROUP BY region;              -- 2nd shuffle

-- GOOD: single GROUP BY
SELECT region, SUM(amount)
FROM iceberg.silver.orders
GROUP BY region;
```

Control task parallelism:

```sql
-- Reduce task count for small queries to avoid overhead
SET SESSION task_writer_count = 4;     -- writers per task
SET SESSION task_concurrency = 8;      -- parallel operators per task
```

---

## 8. Statistics-Based Optimization (CBO)

Run `ANALYZE` regularly on high-churn tables so the optimizer can:
- Choose broadcast vs partitioned join
- Reorder joins by estimated row count
- Estimate cost of stage output

```sql
-- Analyze all columns
ANALYZE iceberg.silver.orders;

-- Analyze specific columns (cheaper, still effective for join ordering)
ANALYZE iceberg.silver.orders WITH (columns = ARRAY['customer_id', 'order_date', 'status']);

-- Check statistics availability
SELECT column_name, row_count, distinct_values_count, null_fraction
FROM iceberg.silver."orders$partitions"
LIMIT 10;
```

---

## 9. Useful Session Properties

```sql
-- Memory
SET SESSION query_max_memory = '10GB';
SET SESSION query_max_total_memory = '20GB';

-- Joins
SET SESSION join_distribution_type = 'AUTOMATIC';        -- or BROADCAST, PARTITIONED
SET SESSION join_reordering_strategy = 'AUTOMATIC';
SET SESSION join_max_broadcast_table_size = '200MB';

-- Spill (enable for large sorts/joins)
SET SESSION spill_enabled = true;

-- Dynamic filtering
SET SESSION dynamic_filter_wait_timeout = '2s';

-- Exchange compression (reduces network I/O ~50%)
SET SESSION exchange_compression_codec = 'LZ4';

-- Aggregation
SET SESSION prefer_partial_aggregation = true;
```

---

## 10. Cross-Catalog Query Best Practices

Trino fetches data from each connector independently and joins results in-memory on workers. For large cross-catalog joins:

```sql
-- EXPENSIVE: Trino pulls all data from both systems, joins locally
SELECT p.product_name, o.amount
FROM postgresql.public.products p           -- JDBC: serial row-by-row fetch
JOIN iceberg.silver.orders o ON o.product_id = p.id
WHERE o.order_date = DATE '2024-06-01';

-- BETTER: use CREATE TABLE AS SELECT to materialize the small side into Iceberg first
CREATE TABLE iceberg.silver.products_snapshot AS
SELECT id, product_name FROM postgresql.public.products;

-- Then join two Iceberg tables (parallel, with pushdown)
SELECT p.product_name, o.amount
FROM iceberg.silver.products_snapshot p
JOIN iceberg.silver.orders o ON o.product_id = p.id
WHERE o.order_date = DATE '2024-06-01';
```

---

## Anti-Patterns

1. **`SELECT *` on wide tables** — Parquet/ORC reads only requested columns; `SELECT *` reads all and defeats projection pushdown.
2. **Functions on partition columns in WHERE** — `YEAR(order_date) = 2024` disables Iceberg partition pruning; use range predicates instead.
3. **Joining large tables across catalogs (JDBC + Iceberg)** — JDBC connectors fetch rows serially; always materialize JDBC data into Iceberg before large joins.
4. **Stale or missing statistics** — CBO falls back to heuristics (ELIMINATE_CROSS_JOINS) when no stats exist; run `ANALYZE` after bulk loads.
5. **Too many small stages from nested CTEs** — each CTE creates a separate stage; flatten deeply nested CTEs when possible.
6. **Forcing broadcast join for large tables** — if build side exceeds `join-max-broadcast-table-size`, Trino OOMs on workers; use AUTOMATIC or PARTITIONED.
7. **`ORDER BY` without `LIMIT`** — full global sort across all workers is extremely expensive in distributed SQL; always add `LIMIT` or use window functions.

---

## References

- Pushdown: `trino.io/docs/current/optimizer/pushdown.html`
- Cost-based optimizations: `trino.io/docs/current/optimizer/cost-based-optimizations.html`
- Session properties: `trino.io/docs/current/sql/set-session.html`
- Related skills: `[[trino-explain-plan-review]]`, `[[trino-iceberg-best-practices]]`, `[[trino-file-layout-optimization]]`, `[[trino-memory-and-spill-tuning]]`
