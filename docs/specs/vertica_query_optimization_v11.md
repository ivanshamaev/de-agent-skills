# Vertica Query Optimization v11.1 — Specification

> Source: [Vertica 11.1.x Documentation — Query optimization](https://docs.vertica.com/11.1.x/en/data-analysis/query-optimization/)

---

## Table of Contents

1. [How the Vertica Query Optimizer Works](#1-how-the-vertica-query-optimizer-works)
2. [Initial Process for Improving Query Performance](#2-initial-process-for-improving-query-performance)
3. [Column Encoding](#3-column-encoding)
4. [Projections for Queries with Predicates](#4-projections-for-queries-with-predicates)
5. [GROUP BY Queries](#5-group-by-queries)
6. [DISTINCT in a SELECT Query List](#6-distinct-in-a-select-query-list)
7. [JOIN Queries](#7-join-queries)
8. [ORDER BY Queries](#8-order-by-queries)
9. [Analytic Functions](#9-analytic-functions)
10. [LIMIT Queries](#10-limit-queries)
11. [INSERT-SELECT Operations](#11-insert-select-operations)
12. [DELETE and UPDATE Queries](#12-delete-and-update-queries)
13. [Data Collector Table Queries](#13-data-collector-table-queries)

---

## 1. How the Vertica Query Optimizer Works

When a query is submitted, the Vertica query optimizer **automatically chooses a set of operations** to compute the requested result. The optimizer selects projections, join strategies, aggregation algorithms, and sort strategies based on:

- Available projections and their sort/segmentation properties.
- Column statistics collected by `ANALYZE_STATISTICS`.
- Query structure (predicates, join keys, GROUP BY columns, ORDER BY columns).

The optimizer cannot compensate for missing projections or stale statistics. Every optimization described in this document works by ensuring that projections match the shape of the queries that run against them.

---

## 2. Initial Process for Improving Query Performance

The documentation defines a three-step foundational workflow:

### Step 1: Run Database Designer

Database Designer creates a **physical schema** for your database with optimal projections and encodings. When running it:
- Include representative sample queries and data.
- Always enable **Update Statistics** — the optimizer depends on current statistics to select projections, determine join order, and choose distribution algorithms.
- You can rerun Database Designer incrementally with new frequent queries.

```sql
-- Re-evaluate encodings on an existing projection without rebuilding the full design
SELECT DESIGNER_DESIGN_PROJECTION_ENCODINGS(
    'my_schema', 'my_design', 'my_schema.my_table', TRUE
);
```

### Step 2: Monitor Query Events

Query the `QUERY_EVENTS` system table to detect planning, optimization, and execution issues proactively:

```sql
SELECT event_timestamp, event_type, event_description, suggested_action
FROM v_monitor.query_events
WHERE event_timestamp > NOW() - INTERVAL '1 hour'
ORDER BY event_timestamp DESC;
```

Event severity types:
- **Informational** — alerts about query behavior, no immediate action required.
- **Issues requiring corrective action** — suboptimal plans due to missing statistics or projections.
- **Critical** — execution failures or resource exhaustion.

### Step 3: Review the Query Plan

Two methods:

**1. `EXPLAIN` — static plan (before execution):**

```sql
EXPLAIN
SELECT u.country, SUM(o.amount)
FROM dwh.fact_orders o
JOIN dwh.dim_users u ON o.user_id = u.user_id
WHERE o.order_date >= DATE '2026-01-01'
GROUP BY u.country;
```

**2. `QUERY_PLAN_PROFILES` — runtime plan (after execution):**

```sql
SELECT node_name, path_id, path_line, counter_name, counter_value
FROM v_monitor.query_plan_profiles
WHERE transaction_id = <txn_id>
  AND statement_id   = <stmt_id>
ORDER BY path_id, path_line;
```

`QUERY_PLAN_PROFILES` shows actual data flow and resource consumption per operation, making it essential for diagnosing slow-running queries.

### Statistics Management

If statistics become stale between Database Designer runs, refresh them manually:

```sql
-- Full table
SELECT ANALYZE_STATISTICS('dwh.fact_orders');

-- Single column (faster for targeted refresh)
SELECT ANALYZE_STATISTICS('dwh.fact_orders', 'order_date');

-- Full schema
SELECT ANALYZE_STATISTICS('dwh');
```

---

## 3. Column Encoding

Encoding reduces on-disk data size, decreasing I/O and improving query speed. Encoding is defined per-column in a projection.

### 3.1 AUTO Encoding (recommended starting point)

Vertica selects the best encoding for each column automatically:

```sql
CREATE PROJECTION dwh.fact_orders_p1
AS SELECT order_id, user_id, order_date, status, amount
FROM dwh.fact_orders
ORDER BY order_date, user_id
SEGMENTED BY HASH(order_id) ALL NODES
ENCODING AUTO;
```

### 3.2 Encoding Types Reference

| Encoding | Best for | Notes |
|---|---|---|
| `AUTO` | Any column | Vertica chooses optimal encoding per column |
| `RLE` | Low-cardinality columns that are sorted | Stores `(count, value)` pairs; requires contiguous runs |
| `DELTAVAL` | Date, time, monotonically increasing integers | Stores differences between consecutive values |
| `DELTA` | Integer columns with small differences | Compact for slowly changing numeric sequences |
| `BLOCKDICT` | Low-to-medium cardinality strings | Dictionary per storage block |
| `GCDDELTA` | Integer columns divisible by a common factor | Divides by GCD before storing |
| `COMMONDELTA_COMP` | Integer sequences with variable deltas | Combines delta encoding with compression |
| `ZSTD` | General-purpose compression | LZ-style, good ratio on varied data |
| `BZIP` | High compression ratio, less read-critical | Slower decompression than ZSTD |
| `GZIP` | Legacy compatibility | Slower than ZSTD |
| `NONE` | Columns already incompressible | Avoids CPU overhead on random data |

Define explicit per-column encodings when AUTO is insufficient:

```sql
CREATE PROJECTION dwh.fact_orders_encoded
AS SELECT status, order_date, order_id, user_id, amount
FROM dwh.fact_orders
ORDER BY status, order_date
SEGMENTED BY HASH(order_id) ALL NODES
ENCODING status     RLE,
          order_date DELTAVAL,
          order_id   DELTA,
          user_id    DELTA,
          amount     AUTO;
```

### 3.3 Improving Column Compression: FLOAT → NUMERIC

`FLOAT` stores approximate values and uses more space. Converting to `NUMERIC` with 18 or fewer digits of precision **reduces on-disk size, improves compression, and improves query performance**.

```sql
-- Bad: approximate, larger storage
CREATE TABLE t (amount FLOAT);

-- Good: exact representation, better compression
CREATE TABLE t (amount NUMERIC(18,2));
```

Conditions for safe conversion:
- Total precision must be **≤ 18 digits** (Vertica's NUMERIC is fine-tuned for this case).
- All actual values must fit within the declared precision and scale.
  - Values exceeding precision **error** on insert/load.
  - Values with excess decimal places are **silently rounded** to the declared scale.

Typical use case: monetary amounts (`NUMERIC(11,2)` handles values from 0 to ~999,999,999.99 and stores cents accurately).

### 3.4 Run Length Encoding (RLE) — Detailed Rules

RLE is the highest-impact encoding for analytical workloads on low-cardinality columns. It replaces a run of identical values with a single `(count, value)` pair.

**Example:** A `gender` column sorted first in the projection might contain 8,147 consecutive `'F'` values followed by 7,956 `'M'` values — RLE stores this as two pairs instead of 16,103 values.

**Requirements for effective RLE:**

1. **Low cardinality** — the column must have few distinct values (e.g., status codes, boolean flags, category codes, years).
2. **Contiguous identical values** — values must appear in runs. This requires the column to be the **leading sort column** (or among the first) in the projection's `ORDER BY`.
3. **Sort order alignment** — the projection must be sorted on the RLE column so runs are physically co-located on disk.

```sql
-- status column: low-cardinality, sorted first → long runs → RLE highly effective
CREATE PROJECTION dwh.fact_orders_by_status
AS SELECT status, order_date, order_id, amount, user_id
FROM dwh.fact_orders
ORDER BY status, order_date, order_id
SEGMENTED BY HASH(order_id) ALL NODES
ENCODING status RLE, order_date DELTAVAL, order_id DELTA, amount AUTO;
```

**When NOT to use RLE:**
- High-cardinality columns (IDs, emails, UUIDs) — RLE inflates size because every value is a new run.
- Columns not in the leading position of the sort order — runs won't be contiguous.

---

## 4. Projections for Queries with Predicates

A **covering projection** contains every column referenced by a query (SELECT list, WHERE, JOIN ON, GROUP BY, ORDER BY). Without a covering projection, Vertica falls back to the super projection, which reads all columns and incurs full-width I/O.

### 4.1 General Design Principle

Place the **most selective predicate columns first** in the projection's `ORDER BY`. This allows Vertica to skip non-matching data blocks:

```sql
-- Query pattern: WHERE order_date = ? AND status = ?
-- Design: put selective filter columns first in ORDER BY
CREATE PROJECTION dwh.fact_orders_by_date_status
AS SELECT order_date, status, order_id, user_id, amount
FROM dwh.fact_orders
ORDER BY order_date, status, order_id   -- filter cols first
SEGMENTED BY HASH(order_id) ALL NODES;
```

### 4.2 Date Range Queries

For queries filtering by date ranges:

- Apply **RLE** to low-cardinality prefix columns (e.g., `trade_year` or `status`) that narrow the search before the date column.
- Sort from **lowest to highest cardinality** in ORDER BY:
  1. Year or month (lowest cardinality, RLE)
  2. Date (medium cardinality, DELTAVAL)
  3. Transaction/order ID (high cardinality, DELTA)

```sql
-- If trade_date alone is too unique for effective range pruning,
-- add a derived lower-cardinality column as the leading sort key
CREATE PROJECTION trading.trades_by_year_date
AS SELECT
    trade_year,        -- derived: EXTRACT(YEAR FROM trade_date)
    trade_date,
    trade_id,
    symbol,
    quantity,
    price
FROM trading.trades
ORDER BY trade_year, trade_date, trade_id
SEGMENTED BY HASH(trade_id) ALL NODES
ENCODING trade_year RLE, trade_date DELTAVAL, trade_id DELTA, price AUTO;
```

Query using both columns for tight predicate pruning:

```sql
SELECT trade_id, symbol, quantity, price
FROM trading.trades
WHERE trade_year = 2026
  AND trade_date BETWEEN DATE '2026-01-01' AND DATE '2026-03-31';
```

### 4.3 High-Cardinality Primary Key Queries

When queries filter by a high-cardinality PK (e.g., customer ID in the billions), create a **bucket column** with lower cardinality to narrow the search:

```sql
-- customer_id ranges in billions: create a bucket column
ALTER TABLE dwh.dim_customers ADD COLUMN cust_bucket INT;
UPDATE dwh.dim_customers SET cust_bucket = customer_id / 1000000;

CREATE PROJECTION dwh.dim_customers_by_bucket
AS SELECT cust_bucket, customer_id, name, email, country
FROM dwh.dim_customers
ORDER BY cust_bucket, customer_id
SEGMENTED BY HASH(customer_id) ALL NODES
ENCODING cust_bucket RLE, customer_id DELTA;
```

Query using both bucket and exact ID:

```sql
SELECT customer_id, name, email
FROM dwh.dim_customers
WHERE cust_bucket = 42          -- narrows to ~1M rows
  AND customer_id = 42000137;   -- exact match within bucket
```

### 4.4 Checking Which Projection a Query Uses

```sql
-- After executing the query, find its transaction and statement ID:
SELECT transaction_id, statement_id, request
FROM v_monitor.query_requests
ORDER BY start_timestamp DESC
LIMIT 10;

-- Then check the plan profile:
SELECT path_line
FROM v_monitor.query_plan_profiles
WHERE transaction_id = <txn_id>
  AND statement_id   = <stmt_id>
ORDER BY path_id, path_line;
```

---

## 5. GROUP BY Queries

### 5.1 Two Aggregation Algorithms

Vertica implements GROUP BY using one of two algorithms, visible in `EXPLAIN`:

#### GROUPBY PIPELINED (optimal)

- Retains only **current group data** in memory.
- Does not build a full hash table.
- Requires less memory and executes faster, especially with high-cardinality GROUP BY keys.
- Shows as `GROUPBY PIPELINED` in EXPLAIN.

#### GROUPBY HASH (fallback)

- Builds a **full in-memory hash table** on GROUP BY columns before grouping.
- Used when PIPELINED requirements are not met.
- Shows as `GROUPBY HASH` in EXPLAIN.
- Memory-intensive; may spill to disk on large datasets.

### 5.2 Conditions for GROUPBY PIPELINED

All three conditions must be satisfied simultaneously:

**Condition 1:** All GROUP BY columns must appear in the projection's `ORDER BY` clause.

**Condition 2:** When the GROUP BY has fewer columns than the projection ORDER BY, the GROUP BY columns must form a **contiguous prefix** of the ORDER BY starting from the first column.

```
Projection ORDER BY: (a, b, c)

GROUP BY a          → PIPELINED  (prefix: a)
GROUP BY a, b       → PIPELINED  (prefix: a, b)
GROUP BY a, b, c    → PIPELINED  (full match)
GROUP BY b          → HASH       (not a prefix — a is missing)
GROUP BY a, c       → HASH       (non-contiguous — b is skipped)
GROUP BY d          → HASH       (d not in ORDER BY at all)
```

**Condition 3:** If GROUP BY columns don't start the ORDER BY, all preceding ORDER BY columns must appear as **single-value equality predicates** in `WHERE`:

```sql
-- Projection ORDER BY: (order_date, status, user_id)
-- GROUP BY status → normally HASH (not a prefix)
-- BUT with a constant equality predicate on order_date:
SELECT status, COUNT(*) 
FROM dwh.fact_orders
WHERE order_date = DATE '2026-01-15'   -- constant equality on leading column
GROUP BY status;                        -- → PIPELINED
```

### 5.3 Controlling Algorithm with GBYTYPE Hint

```sql
-- Force PIPELINED (warns and falls back to HASH if conditions not met):
SELECT status, SUM(amount)
FROM dwh.fact_orders
GROUP BY /*+ GBYTYPE(PIPE) */ status;

-- Force HASH:
SELECT status, SUM(amount)
FROM dwh.fact_orders
GROUP BY /*+ GBYTYPE(HASH) */ status;
```

Use hints only for investigation. Let the optimizer decide in production.

### 5.4 Avoiding RESEGMENT GROUPS

Rule: the GROUP BY clause must include **all segmentation columns** of the chosen projection. It may include additional columns.

```sql
-- Projection segmented by HASH(order_id, user_id)

GROUP BY order_id, user_id          -- OK: all segmentation columns present
GROUP BY order_id, user_id, status  -- OK: superset allowed
GROUP BY order_id                   -- RESEGMENT: missing user_id
GROUP BY order_id + 1, user_id      -- RESEGMENT: expression on segmentation column breaks alignment
```

EXPLAIN indicators:
- `GROUPBY PIPELINED` — no resegmentation. Optimal.
- `GROUPBY PIPELINED (RESEGMENT GROUPS)` — resegmentation required. Redesign.
- `GROUPBY HASH` — no sort-based optimization. Consider projection redesign.

### 5.5 Pre-Aggregation Before JOIN

When a JOIN precedes GROUP BY, the intermediate result may not be segmented on the GROUP BY keys, forcing resegmentation. Pre-aggregating before the JOIN avoids this:

```sql
-- Pattern: aggregate BEFORE joining to avoid post-join RESEGMENT
WITH user_revenue AS (
    SELECT user_id, SUM(amount) AS revenue
    FROM dwh.fact_orders
    WHERE order_date >= DATE '2026-01-01'
    GROUP BY user_id                   -- segmentation key: user_id → no RESEGMENT
)
SELECT u.country, ur.revenue
FROM user_revenue ur
JOIN dwh.dim_users u ON ur.user_id = u.user_id;
```

vs. the slower alternative:

```sql
-- Anti-pattern: GROUP BY after JOIN — intermediate result may not be aligned
SELECT u.country, SUM(o.amount)
FROM dwh.fact_orders o
JOIN dwh.dim_users u ON o.user_id = u.user_id
WHERE o.order_date >= DATE '2026-01-01'
GROUP BY u.country;  -- may trigger RESEGMENT GROUPS
```

---

## 6. DISTINCT in a SELECT Query List

### 6.1 DISTINCT Without Aggregates — Rewritten as GROUP BY

Vertica internally rewrites `SELECT DISTINCT` (with no aggregates) as `GROUP BY`:

```sql
-- These two queries are treated identically by the optimizer:
SELECT DISTINCT a, b, c FROM t;
-- ↓ rewritten as:
SELECT a, b, c FROM t GROUP BY a, b, c;
```

**Optimization strategy:** apply all GROUP BY optimization techniques (projection ORDER BY alignment, PIPELINED conditions, segmentation).

### 6.2 Single DISTINCT Aggregate — Rewritten as Two-Level GROUP BY

A single `COUNT(DISTINCT col)` is computed by first deduplicating, then aggregating:

```sql
-- Original:
SELECT a, b, COUNT(DISTINCT c) AS dcnt
FROM t
GROUP BY a, b;

-- Internal rewrite (two-level GROUP BY):
SELECT a, b, COUNT(dcnt)
FROM (
    SELECT a, b, c AS dcnt
    FROM t
    GROUP BY a, b, c   -- inner: deduplicate c within (a,b) groups
)
GROUP BY a, b;          -- outer: count unique c per (a,b)
```

For fastest execution, design the projection so the inner GROUP BY can use `GROUPBY PIPELINED`:
- Projection ORDER BY should start with `(a, b, c)` in that order.
- Projection segmentation should include `a` or `b` (or both).

### 6.3 Multiple DISTINCT Aggregates — No Efficient Rewrite

```sql
-- This query has NO efficient SQL rewrite:
SELECT a, COUNT(DISTINCT b), COUNT(DISTINCT c)
FROM t
GROUP BY a;
```

Vertica **must** use `GROUPBY HASH` and resegment the data. There is no projection design that eliminates this cost. The only lever is **memory allocation**: ensure the session/resource pool has sufficient memory to avoid spilling the hash table to disk.

```sql
-- Give the query more memory via a resource pool hint
SET SESSION RESOURCE_POOL = large_memory_pool;
```

Alternatively, decompose into separate queries if latency is critical:

```sql
SELECT a, COUNT(DISTINCT b) AS cnt_b FROM t GROUP BY a;
-- join with:
SELECT a, COUNT(DISTINCT c) AS cnt_c FROM t GROUP BY a;
```

### 6.4 Approximate COUNT DISTINCT — Preferred for Large Tables

Vertica provides approximate distinct counting based on HyperLogLog-style synopses with a default error of **±1.25%** standard deviation:

```sql
-- Exact (slow on large tables)
SELECT COUNT(DISTINCT user_id) FROM dwh.fact_orders;

-- Approximate — fast, ~1.25% error
SELECT APPROXIMATE_COUNT_DISTINCT(user_id) FROM dwh.fact_orders;
```

**Synopsis pattern for incremental workloads** — compute synopses incrementally and merge them:

```sql
-- 1. Create a table to hold daily synopses
CREATE TABLE dwh.daily_user_synopses (
    page_id      INT,
    visit_date   DATE,
    users_synopsis LONG VARBINARY(65000)
);

-- 2. Each day, insert a synopsis (not the full data)
INSERT INTO dwh.daily_user_synopses
SELECT
    page_id,
    visit_date::DATE,
    APPROXIMATE_COUNT_DISTINCT_SYNOPSIS(user_id) AS users_synopsis
FROM dwh.page_views
WHERE visit_date = CURRENT_DATE - 1
GROUP BY page_id, visit_date::DATE;

-- 3. Query approximate distinct users over a date range by merging synopses
SELECT
    page_id,
    APPROXIMATE_COUNT_DISTINCT_OF_SYNOPSIS(
        APPROXIMATE_COUNT_DISTINCT_SYNOPSIS_MERGE(users_synopsis)
    ) AS distinct_users
FROM dwh.daily_user_synopses
WHERE visit_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
GROUP BY page_id;
```

Function reference:

| Function | Purpose |
|---|---|
| `APPROXIMATE_COUNT_DISTINCT(col)` | Direct approximate distinct count from raw data |
| `APPROXIMATE_COUNT_DISTINCT_SYNOPSIS(col)` | Creates binary synopsis object from raw data |
| `APPROXIMATE_COUNT_DISTINCT_OF_SYNOPSIS(synopsis)` | Reads approximate count from a synopsis |
| `APPROXIMATE_COUNT_DISTINCT_SYNOPSIS_MERGE(synopsis)` | Merges multiple synopses into one |

Use approximate functions in dashboards and exploratory analytics where 1-2% error is acceptable. Use exact `COUNT(DISTINCT)` only for auditing and reconciliation.

---

## 7. JOIN Queries

### 7.1 Hash Join vs Merge Join

Vertica selects the join algorithm based on whether the input projections are sorted on the join key.

| Algorithm | Condition | EXPLAIN output |
|---|---|---|
| **Merge join** | Both inputs sorted on join key | `JOIN MERGEJOIN(inputs presorted)` |
| **Hash join** | Inputs not sorted on join key | `JOIN HASH` |

Merge join is **faster and uses less memory** than hash join.

**Example EXPLAIN output:**

```
-- Hash join (baseline):
JOIN HASH [Cost: 752, Rows: 300K]

-- After adding sorted projections (merge join):
JOIN MERGEJOIN(inputs presorted) [Cost: 731, Rows: 300K]
```

### 7.2 Triggering Merge Joins via Projection Design

The join key column must be **first (or early) in the projection's ORDER BY**:

```sql
-- Both projections sorted on the join key (user_id)
CREATE PROJECTION dwh.fact_orders_join_user
AS SELECT order_id, user_id, order_date, amount, status
FROM dwh.fact_orders
ORDER BY user_id, order_date
SEGMENTED BY HASH(user_id) ALL NODES;

CREATE PROJECTION dwh.dim_users_join
AS SELECT user_id, country, email, region
FROM dwh.dim_users
ORDER BY user_id
SEGMENTED BY HASH(user_id) ALL NODES;

-- Now this join uses MERGEJOIN:
SELECT o.order_id, o.amount, u.country
FROM dwh.fact_orders o
JOIN dwh.dim_users u ON o.user_id = u.user_id;
```

**Ad-hoc alternative** — pre-sort in a subquery (less efficient than a covering projection, but useful for one-off queries):

```sql
SELECT o.order_id, o.amount, u.country
FROM (SELECT * FROM dwh.fact_orders ORDER BY user_id) o
JOIN (SELECT * FROM dwh.dim_users   ORDER BY user_id) u
  ON o.user_id = u.user_id;
```

### 7.3 Identical Segmentation — Local Joins (No Network I/O)

When both joined tables are segmented by the **same expression on the same join key columns**, the join executes locally on each node without redistributing data. This eliminates cross-node network traffic.

**Requirements:**
1. Join condition must be pure equality: `t1.j1 = t2.j1 AND t1.j2 = t2.j2 ...`
2. **All** segmentation columns must appear in the join condition (superset is OK on the join side).
3. Same data type on both sides — `BIGINT = BIGINT`, not `BIGINT = INT` or `BIGINT = FLOAT`.
4. Same hash function expression (including column order in `HASH()`).
5. Same segment count and same node assignments (`ALL NODES` on both).

```sql
-- Identically segmented: join is local, no RESEGMENT
CREATE TABLE dwh.fact_orders  (...) SEGMENTED BY HASH(user_id) ALL NODES;
CREATE TABLE dwh.dim_users    (...) SEGMENTED BY HASH(user_id) ALL NODES;

-- Join on user_id: data already co-located → local join, no RESEGMENT in EXPLAIN
SELECT o.order_id, u.country
FROM dwh.fact_orders o
JOIN dwh.dim_users u ON o.user_id = u.user_id;
```

**Mismatched segmentation — forces RESEGMENT:**

```sql
CREATE TABLE t1 (id INT, x INT) SEGMENTED BY HASH(id, x) ALL NODES;
CREATE TABLE t2 (id INT, x INT) SEGMENTED BY HASH(id, x) ALL NODES;

-- RESEGMENT: join only on id, but segmentation uses HASH(id, x) — partial key
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;

-- No RESEGMENT: join includes all segmentation columns
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id AND t1.x = t2.x;
```

EXPLAIN signal: if the plan contains `RESEGMENT` on the large table, segmentation does not match for that join.

### 7.4 Join Predicate Structure — Single-Table Sides

Each side of a join equality predicate must reference **only one table**. When a predicate mixes columns from both tables on one side, Vertica cannot use the standard join optimization path:

```sql
-- Good: each side references a single table
ON t1.a + t1.b = t2.x - t2.y

-- Bad: right side mixes both tables → extra overhead
ON t1.a = t2.x + t1.b
```

### 7.5 Variable-Length String Join Format

By default, Vertica buffers JOIN key columns using their **declared maximum length**. A `VARCHAR(1000)` join column allocates 1000 bytes per row in the join buffer, even if actual strings are 3-10 bytes. This wastes memory and can cause **join spills to disk**.

Enable variable-length formatting to buffer only actual string lengths:

```sql
-- Database level (persistent):
ALTER DATABASE DEFAULT SET PARAMETER JoinDefaultTupleFormat = 'variable';

-- Session level:
ALTER SESSION SET PARAMETER JoinDefaultTupleFormat = 'variable';

-- Query level (JFMT hint):
SELECT /*+ JFMT(variable) */ o.order_id, u.email
FROM dwh.fact_orders o
JOIN dwh.dim_users u ON o.user_id = u.user_id;
```

Verify with `EXPLAIN VERBOSE`:
- `JF_EE_VARIABLE_FORMAT` — variable-length active (good).
- `JF_EE_FIXED_FORMAT` — fixed-length active; consider enabling variable format.

**Limitation:** variable-length formatting is **not available for merge joins** or event series joins — only hash joins benefit.

---

## 8. ORDER BY Queries

### 8.1 Sort Elimination via Projection Design

Vertica stores projection data in **ascending sort order physically**. If the query's ORDER BY matches the projection's ORDER BY prefix, the sort phase is **eliminated entirely** — the optimizer reads data already in the correct order.

```sql
-- Projection created with ORDER BY (order_date, user_id, order_id)

-- Sort ELIMINATED for these queries (matching ASC prefix):
ORDER BY order_date
ORDER BY order_date, user_id
ORDER BY order_date, user_id, order_id

-- Sort NOT ELIMINATED:
ORDER BY order_date DESC           -- DESC always forces re-sort (storage is always ASC)
ORDER BY user_id                   -- not a prefix (order_date must come first)
ORDER BY order_date, order_id      -- non-contiguous (skips user_id)
ORDER BY order_date, user_id, order_id, amount  -- extends beyond projection ORDER BY
```

### 8.2 Design Principle

Design projection ORDER BY based on the most common ORDER BY patterns in production queries. For queries that always sort DESC on a column, there is no way to avoid the re-sort through projection design — physical storage is always ASC in Vertica 11.1.

### 8.3 Interaction with LIMIT

When `ORDER BY` is combined with `LIMIT`, Vertica applies **Top-K optimization** (see Section 10). If the projection's ORDER BY already matches, sort elimination + Top-K compound: the query reads data in sorted order and stops after K rows without ever sorting the full dataset.

---

## 9. Analytic Functions

### 9.1 Empty OVER() — Single-Node Bottleneck

An empty `OVER()` clause (no PARTITION BY, no ORDER BY) treats the **entire input as a single partition**, forcing execution on a single node:

```sql
-- Bad: entire fact_orders funneled to one node
SELECT order_id, SUM(amount) OVER() AS grand_total
FROM dwh.fact_orders;
```

This is a serial bottleneck on large tables. Solutions:

**Option A:** Add PARTITION BY to distribute across nodes:

```sql
SELECT order_id, SUM(amount) OVER (PARTITION BY order_date) AS daily_total
FROM dwh.fact_orders;
```

**Option B:** Compute the global aggregate in a CTE and join back:

```sql
WITH totals AS (
    SELECT SUM(amount) AS grand_total
    FROM dwh.fact_orders
    WHERE order_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
)
SELECT o.order_id, o.amount, t.grand_total
FROM dwh.fact_orders o
CROSS JOIN totals t
WHERE o.order_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31';
```

Option B avoids the window function overhead entirely and is fully parallelized.

### 9.2 NULL Sort Order — Eliminating Runtime Sort

The optimizer can eliminate the sort step for an analytic `ORDER BY` when the analytic clause's sort order matches the **projection's physical sort order and NULL placement exactly**.

#### Default NULL placement by data type (physical storage):

| Data type | Physical storage default |
|---|---|
| NUMERIC, INTEGER, DATE, TIME, TIMESTAMP, INTERVAL | ASC, **NULLS FIRST** |
| FLOAT, STRING, BOOLEAN | ASC, **NULLS LAST** |

#### Default NULL placement in OVER(ORDER BY):

| Specification | NULL placement |
|---|---|
| `ORDER BY col ASC` | NULLS LAST |
| `ORDER BY col DESC` | NULLS FIRST |

This mismatch is the key issue: an INTEGER column is physically stored `ASC NULLS FIRST`, but an analytic `ORDER BY col ASC` defaults to `NULLS LAST`. The optimizer sees a mismatch and inserts a runtime sort.

#### To eliminate runtime sort, align explicitly:

```sql
-- INTEGER column (physical: ASC NULLS FIRST)
-- Analytic clause must specify NULLS FIRST to match → sort eliminated:
SELECT user_id,
    ROW_NUMBER() OVER (
        PARTITION BY country
        ORDER BY user_id ASC NULLS FIRST   -- matches physical storage
    ) AS rn
FROM dwh.dim_users;

-- FLOAT column (physical: ASC NULLS LAST)
-- Analytic clause must specify NULLS LAST to match:
SELECT order_id,
    RANK() OVER (
        PARTITION BY order_date
        ORDER BY score ASC NULLS LAST      -- matches physical storage
    ) AS score_rank
FROM dwh.fact_scores;
```

#### NULLS AUTO — let Vertica choose the fastest placement:

```sql
-- Vertica picks NULLS FIRST or NULLS LAST to avoid a runtime sort
SELECT user_id,
    ROW_NUMBER() OVER (
        PARTITION BY country
        ORDER BY user_id ASC NULLS AUTO
    ) AS rn
FROM dwh.dim_users;
```

Use `NULLS AUTO` when the specific NULL position doesn't matter for query correctness.

#### Check projection's actual NULL order:

```sql
SELECT column_name, sort_position, sort_order, sort_null_order
FROM v_catalog.projection_columns
WHERE projection_name = 'dim_users_p1'
ORDER BY sort_position;
-- sort_null_order: 'NULLS FIRST' or 'NULLS LAST'
```

### 9.3 Runtime Sorting of NULL Values — Projection Design

Runtime sorting of NULL values in analytic functions is triggered when the query optimizer cannot satisfy the OVER clause from the projection's physical sort. Projection design eliminates it:

**1. Declare NOT NULL constraints** on sort columns where possible — the optimizer can skip NULL handling entirely:

```sql
CREATE TABLE dwh.employees (
    emp_id   BIGINT NOT NULL,
    dept_id  INT    NOT NULL,    -- NOT NULL: no runtime NULL sort needed
    salary   INT    NOT NULL     -- NOT NULL: no runtime NULL sort needed
);

CREATE PROJECTION dwh.employees_p1
AS SELECT emp_id, dept_id, salary
FROM dwh.employees
ORDER BY dept_id, salary
SEGMENTED BY HASH(emp_id) ALL NODES;

-- This query eliminates runtime sort because dept_id and salary are NOT NULL
-- and the projection is sorted on (dept_id, salary):
SELECT dept_id, salary, emp_id,
    RANK() OVER (PARTITION BY dept_id ORDER BY salary) AS salary_rank
FROM dwh.employees;
```

**2. Use NULLS AUTO** for columns with NULLs when exact NULL placement is not required.

**3. Align analytic ORDER BY with projection sort** (see 9.2).

---

## 10. LIMIT Queries

### 10.1 Top-K Optimization

Vertica applies **Top-K optimization** automatically when a query has both `ORDER BY` and `LIMIT`. Instead of sorting the full result set and truncating, the optimizer maintains only K rows in memory at each step.

Benefits:
- Avoids sorting the entire dataset.
- Avoids writing to disk for large intermediate results.
- Returns only the requested K rows.

```sql
-- Top-K active: Vertica tracks only 100 best rows, never sorts full dataset
SELECT user_id, SUM(amount) AS revenue
FROM dwh.fact_orders
WHERE order_date >= DATE '2026-01-01'
GROUP BY user_id
ORDER BY revenue DESC
LIMIT 100;
```

### 10.2 LIMIT Without ORDER BY — Nondeterministic Results

`LIMIT` without `ORDER BY` returns an arbitrary subset of rows. Results are nondeterministic across executions. Always include `ORDER BY` in production queries using LIMIT:

```sql
-- Bad: nondeterministic
SELECT order_id FROM dwh.fact_orders LIMIT 100;

-- Good: deterministic
SELECT order_id FROM dwh.fact_orders ORDER BY order_id LIMIT 100;
```

### 10.3 Window LIMIT — Per-Partition Row Restriction

Limit rows within each partition using a window clause:

```sql
SELECT order_id, user_id, order_date, amount
FROM (
    SELECT
        order_id, user_id, order_date, amount,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY order_date DESC NULLS LAST
        ) AS rn
    FROM dwh.fact_orders
) t
WHERE rn <= 5;    -- keep 5 most recent orders per user
```

Or using `QUALIFY` (Vertica-specific, more concise):

```sql
SELECT order_id, user_id, order_date, amount
FROM dwh.fact_orders
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id
    ORDER BY order_date DESC NULLS LAST
) <= 5;
```

---

## 11. INSERT-SELECT Operations

### 11.1 Matching Sort Orders — Eliminating the Sort Phase

`INSERT /*+direct*/ INTO target SELECT ... FROM source` skips the sort phase when the SELECT output order matches the target projection's ORDER BY.

**How it works:**
- Vertica must sort incoming data to match the target projection's ORDER BY before writing.
- If the source SELECT already produces data in that order, the sort is skipped.

```sql
-- Target projection ORDER BY: (col1, col2, col3)

-- No sort needed: column order in SELECT matches projection ORDER BY
INSERT /*+direct*/ INTO destination
SELECT col1, col2, col3 FROM source;

-- Sort needed: columns resequenced
INSERT /*+direct*/ INTO destination
SELECT col1, col3, col2 FROM source;   -- col2 and col3 swapped

-- No sort needed: explicit ORDER BY overrides column order
INSERT /*+direct*/ INTO destination
SELECT col1, col3, col2 FROM source
ORDER BY col1, col2, col3;             -- matches target projection ORDER BY
```

### 11.2 Matching Segmentation — Eliminating RESEGMENT

Create source and target projections with the **same segmentation expression and segment count**. Data is already distributed correctly on each node — no cross-node data movement during INSERT.

```sql
-- Source projection
CREATE PROJECTION source_p (col1, col2, col3)
AS SELECT col1, col2, col3 FROM source
ORDER BY col1, col2, col3
SEGMENTED BY HASH(col3) ALL NODES;   -- segmented by col3

-- Target projection — identical segmentation
CREATE PROJECTION destination_p (col1, col2, col3)
AS SELECT col1, col2, col3 FROM destination
ORDER BY col1, col2, col3
SEGMENTED BY HASH(col3) ALL NODES;   -- same expression

-- INSERT: no RESEGMENT in EXPLAIN
INSERT /*+direct*/ INTO destination SELECT * FROM source;
```

Check the INSERT plan with `EXPLAIN`:
- `RESEGMENT` in the plan → segmentation mismatch; revisit projection definitions.
- No `RESEGMENT` → data moves locally per node; optimal throughput.

### 11.3 The `/*+direct*/` Hint

Direct-mode INSERT bypasses the Write Optimized Store (WOS) and writes directly to Read Optimized Store (ROS) on disk. Use it for:
- Large bulk loads (avoid WOS overflow).
- INSERT-SELECT operations where sort/segmentation matching is designed in.

```sql
INSERT /*+direct*/ INTO dwh.fact_orders_archive
SELECT * FROM dwh.fact_orders
WHERE order_date < DATE '2024-01-01';
```

---

## 12. DELETE and UPDATE Queries

### 12.1 Internal Mechanics

Vertica is optimized for **read-heavy analytic workloads**. DELETE and UPDATE are structurally expensive:

1. **Logical deletion**: deleted/updated rows are **marked as logically deleted**, not removed immediately.
2. **All projections must be updated**: the operation is bounded by the speed of the **slowest projection**. Minimizing the number of projections on write-heavy tables directly improves DELETE/UPDATE throughput.
3. **Exclusive lock**: only one DELETE or UPDATE can execute per table at a time (cross-table operations are concurrent).
4. **Physical purge is deferred**: logically deleted rows persist on disk, consuming storage and degrading query performance, until the Tuple Mover purges them.

### 12.2 Projection Requirements for Optimized DELETE/UPDATE

A projection is **optimized for DELETE/UPDATE** if it contains all columns referenced in the WHERE clause predicate. A projection missing predicate columns cannot be optimized and becomes a bottleneck.

```sql
-- DELETE on column 'order_date'
-- Every projection on fact_orders must include order_date
-- to avoid falling back to a slow full-scan path

-- Check: does every projection include the predicate column?
SELECT projection_name, column_name
FROM v_catalog.projection_columns
WHERE table_schema = 'dwh'
  AND table_name   = 'fact_orders'
  AND column_name  = 'order_date'
ORDER BY projection_name;
```

### 12.3 Sort Order for DELETE/UPDATE Performance

Design projections so **frequently-used DELETE/UPDATE predicate columns appear early** in the ORDER BY clause, allowing Vertica to find matching rows quickly:

```sql
-- If deletes are most often by order_date, put it first in ORDER BY
CREATE PROJECTION dwh.fact_orders_for_delete
AS SELECT order_date, order_id, user_id, status, amount
FROM dwh.fact_orders
ORDER BY order_date, order_id   -- order_date first → fast predicate scan
SEGMENTED BY HASH(order_id) ALL NODES;
```

Use `EVALUATE_DELETE_PERFORMANCE` to analyze sort order issues:

```sql
SELECT EVALUATE_DELETE_PERFORMANCE('dwh.fact_orders');
```

### 12.4 Partition-Aligned DELETE (Fast Path)

When the DELETE WHERE clause aligns with the **partition expression**, Vertica can drop entire ROS containers instead of marking individual rows. This is orders of magnitude faster for bulk historical deletes:

```sql
-- Partition expression: PARTITION BY order_date::DATE
-- This DELETE aligns with the partition boundary → drops ROS containers directly
DELETE FROM dwh.fact_orders
WHERE order_date BETWEEN DATE '2023-01-01' AND DATE '2023-12-31';
```

For even larger bulk deletes, **drop the partition explicitly**:

```sql
ALTER TABLE dwh.fact_orders
    DROP PARTITION BETWEEN '2023-01-01' AND '2023-12-31';
```

### 12.5 Post-Delete Performance Recovery

After deleting **10% or more** of a table's rows, query performance can degrade because Vertica must skip logically deleted rows during scans. Force physical purge to restore performance:

```sql
-- Purge entire table
SELECT PURGE_TABLE('dwh.fact_orders');

-- Purge a specific partition range (faster, targeted)
SELECT PURGE_PARTITION('dwh.fact_orders', '2023-01-01', '2023-12-31');
```

Purging also improves **cluster recovery performance** after crashes — a large number of unprocessed delete markers slows recovery.

### 12.6 Subquery Restrictions

Optimized DELETE with subqueries requires:
- All target table columns in WHERE must exist in all projection definitions.
- Subqueries must return **multiple rows** (scalar single-row subqueries are not supported in optimized DELETE paths).
- **Replicated projections** cannot support optimized DELETE when the subquery references the target table.

### 12.7 Preferred Alternatives for Bulk UPDATE

For bulk updates affecting most rows in a partition, row-level UPDATE is slow (rewrites all projections). Prefer:

| Situation | Recommended approach |
|---|---|
| Update all rows in a partition | Partition swap: CTAS staging + `ALTER TABLE SWAP PARTITION` |
| Full table rebuild | `CREATE TABLE ... AS SELECT` + rename swap |
| Narrow targeted update (few rows) | Standard `UPDATE` with a predicate aligned to sort order |
| Insert new + update existing | `MERGE INTO` from staging table |

---

## 13. Data Collector Table Queries

The Data Collector records Vertica internal events and request metrics in `dc_*` tables. These are projection-backed tables optimized for specific join patterns.

### 13.1 Key DC Tables

| Table | Content |
|---|---|
| `dc_requests_issued` | One row per query submitted: SQL text, user, session, timestamp |
| `dc_requests_completed` | One row per completed query: row counts, resource usage |
| `dc_session_starts` | Session open events |
| `dc_session_ends` | Session close events with duration |

Primary join key: `(session_id, node_name)`.

### 13.2 Core Optimization Rules

**Rule 1: Always join on `node_name`**

DC request tables are written by the initiator node. `session_id` and `node_name` are correlated — joining only on `session_id` triggers `RESEGMENT`. Include `node_name` in every join between DC tables:

```sql
-- Bad: RESEGMENT in plan (missing node_name)
SELECT ri.request, rc.processed_row_count
FROM dc_requests_issued  ri
JOIN dc_requests_completed rc ON ri.session_id = rc.session_id;

-- Good: no RESEGMENT (node_name eliminates redistribution)
SELECT ri.request, rc.processed_row_count
FROM dc_requests_issued  ri
JOIN dc_requests_completed rc
  ON  ri.session_id = rc.session_id
  AND ri.node_name  = rc.node_name;
```

**Rule 2: Use static TIMESTAMPTZ literals on the `time` column**

The `time` column enables partition pruning on DC tables. **Volatile functions** (`NOW()`, `CURRENT_TIMESTAMP`) disable pushdown — Vertica cannot evaluate them at plan time:

```sql
-- Bad: volatile function disables partition pruning
WHERE time > NOW() - INTERVAL '1 day'

-- Bad: TRUNC() wrapping prevents pushdown
WHERE TRUNC(time) = CURRENT_DATE

-- Good: static literal cast to TIMESTAMPTZ
WHERE time > '2026-05-15 00:00:00'::TIMESTAMPTZ
  AND time < '2026-05-16 00:00:00'::TIMESTAMPTZ
```

**Rule 3: Select only needed columns**

DC tables are wide. Projecting only necessary columns reduces I/O significantly.

### 13.3 Diagnostic Query Templates

**Find the slowest queries in a time window:**

```sql
SELECT
    ri.session_id,
    ri.user_name,
    ri.node_name,
    DATEDIFF('millisecond', ri.time, rc.time) AS duration_ms,
    rc.processed_row_count,
    ri.request
FROM dc_requests_issued  ri
JOIN dc_requests_completed rc
  ON  ri.session_id = rc.session_id
  AND ri.node_name  = rc.node_name
WHERE ri.time > '2026-05-15 00:00:00'::TIMESTAMPTZ
  AND ri.time < '2026-05-16 00:00:00'::TIMESTAMPTZ
ORDER BY duration_ms DESC
LIMIT 50;
```

**Session-level statistics:**

```sql
SELECT
    ss.session_id,
    ss.user_name,
    ss.client_hostname,
    DATEDIFF('second', ss.time, se.time) AS session_duration_s
FROM dc_session_starts ss
JOIN dc_session_ends   se
  ON  ss.session_id = se.session_id
  AND ss.node_name  = se.node_name
WHERE ss.time > '2026-05-15 00:00:00'::TIMESTAMPTZ
ORDER BY session_duration_s DESC
LIMIT 20;
```

**Queries with high row counts (potential full-scan suspects):**

```sql
SELECT
    ri.user_name,
    rc.processed_row_count,
    DATEDIFF('millisecond', ri.time, rc.time) AS duration_ms,
    ri.request
FROM dc_requests_issued  ri
JOIN dc_requests_completed rc
  ON  ri.session_id = rc.session_id
  AND ri.node_name  = rc.node_name
WHERE ri.time > '2026-05-15 00:00:00'::TIMESTAMPTZ
  AND rc.processed_row_count > 100000000
ORDER BY rc.processed_row_count DESC
LIMIT 20;
```

---

## Appendix: Quick Reference

### EXPLAIN Signal Lookup

| Signal | Problem | Fix |
|---|---|---|
| `JOIN HASH` | No sorted projection on join key | Create projection sorted on join key |
| `RESEGMENT` | Segmentation mismatch on join/GROUP BY | Align segmentation keys |
| `BROADCAST` | Table sent to all nodes | Expected for small dims; verify size |
| `GROUPBY HASH` | No PIPELINED-eligible projection | Align projection ORDER BY with GROUP BY |
| `GROUPBY PIPELINED (RESEGMENT GROUPS)` | GROUP BY missing segmentation columns | Include all segmentation cols in GROUP BY |
| Sort operation before write | INSERT source not sorted on target ORDER BY | Add `ORDER BY` to SELECT or align projections |

### Projection Design Checklist

- [ ] Projection covers all columns referenced by the target query pattern.
- [ ] ORDER BY starts with the most selective predicate and sort columns.
- [ ] GROUP BY columns form a contiguous prefix of ORDER BY.
- [ ] All segmentation columns appear in the GROUP BY (for GROUP BY queries) or join condition (for joins).
- [ ] JOIN key column is first or early in ORDER BY.
- [ ] Low-cardinality, sorted columns use RLE encoding.
- [ ] Date/sequence columns use DELTAVAL or DELTA encoding.
- [ ] Other columns use AUTO or explicitly chosen encoding.
- [ ] `ANALYZE_STATISTICS` is run after creating or refreshing projections.

### Key System Tables for Optimization

| Table | Use |
|---|---|
| `v_catalog.projections` | List projections, check `is_up_to_date`, `has_statistics` |
| `v_catalog.projection_columns` | Column sort order, NULL order, encoding per projection |
| `v_monitor.query_events` | Optimizer warnings and suggestions |
| `v_monitor.query_plan_profiles` | Runtime execution plan with actual metrics |
| `v_monitor.query_requests` | Recent queries with transaction/statement IDs |
| `v_monitor.table_storage` | Row counts and storage per table |
| `dc_requests_issued` / `dc_requests_completed` | Query history, duration, row counts |
| `dc_session_starts` / `dc_session_ends` | Session history |
