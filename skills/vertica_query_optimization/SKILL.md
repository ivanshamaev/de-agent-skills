---
name: vertica_query_optimization
description: Use when optimizing, diagnosing, or reviewing Vertica 11.x SQL query performance — covering EXPLAIN plan reading, projection design for predicates/joins/GROUP BY/ORDER BY/analytic functions, segmentation strategies, column encoding, RLE, sort elimination, Top-K, INSERT-SELECT tuning, DELETE/UPDATE internals, and Data Collector diagnostics.
---

# Vertica Query Optimization Engineer (v11.1)

## When to Use

Use this skill when:
- A Vertica query is slow and needs diagnosis or redesign
- The user asks how to read or interpret a Vertica `EXPLAIN` plan
- The task involves projection design for specific query patterns (joins, GROUP BY, ORDER BY, analytic functions)
- The user asks about segmentation, encoding, RLE, sort elimination, or resegmentation
- The user needs to tune INSERT-SELECT, DELETE, or UPDATE operations
- The user wants to query Data Collector tables (`dc_*`) for performance diagnostics

Use alongside the `vertica` skill when DDL changes are also required.

---

## Core Optimization Workflow

1. Run `EXPLAIN <query>` and read the plan for cost, join type, GROUP BY algorithm, RESEGMENT, BROADCAST, and sort operations.
2. Check which projection the optimizer chose: `v_catalog.projections`, `v_monitor.query_plan_profiles`.
3. Update statistics: `SELECT ANALYZE_STATISTICS('schema.table');`
4. Design or adjust projections to match the query's `ORDER BY`, `GROUP BY`, join keys, and segmentation.
5. Re-run `EXPLAIN` to confirm plan improvements before executing on production data.
6. For encoding changes, use `DESIGNER_DESIGN_PROJECTION_ENCODINGS` or re-create the projection with explicit `ENCODING` clauses.

---

## Reading EXPLAIN Plans

Always run `EXPLAIN` before tuning. Key signals:

| EXPLAIN token | Meaning | Action |
|---|---|---|
| `JOIN HASH` | Hash join — projection not sorted on join key | Create sorted projection on join key |
| `JOIN MERGEJOIN(inputs presorted)` | Merge join — optimal | Good, keep |
| `RESEGMENT` | Data is being redistributed across nodes | Fix segmentation keys to match join/GROUP BY |
| `BROADCAST` | Small table is sent to all nodes | Expected for small dims; verify size |
| `GROUPBY HASH` | Hash-based aggregation — more memory | Redesign projection ORDER BY |
| `GROUPBY PIPELINED` | Pipeline aggregation — optimal | Good |
| `GROUPBY PIPELINED (RESEGMENT GROUPS)` | Pipelined but with resegmentation overhead | Fix segmentation to match GROUP BY |
| `Top-K` | LIMIT optimization active — good | Good |

```sql
EXPLAIN
SELECT u.country, SUM(o.amount)
FROM dwh.fact_orders o
JOIN dwh.dim_users u ON o.user_id = u.user_id
WHERE o.order_date >= DATE '2026-01-01'
GROUP BY u.country;
```

Use `EXPLAIN VERBOSE` when diagnosing join format issues (see variable-length string joins below).

---

## Column Encoding

### Choosing encodings

The recommended starting point is **AUTO** encoding — Vertica selects the best encoding per column automatically:

```sql
CREATE PROJECTION dwh.fact_orders_p1
AS SELECT order_id, user_id, order_date, status, amount
FROM dwh.fact_orders
ORDER BY order_date, user_id
SEGMENTED BY HASH(order_id) ALL NODES
ENCODING AUTO;
```

Use `DESIGNER_DESIGN_PROJECTION_ENCODINGS` to re-evaluate encodings on an existing projection without rebuilding the full design:

```sql
SELECT DESIGNER_DESIGN_PROJECTION_ENCODINGS(
    'dwh', 'my_design', 'dwh.fact_orders', TRUE
);
```

After any encoding or projection change, refresh statistics:

```sql
SELECT ANALYZE_STATISTICS('dwh.fact_orders');
```

### FLOAT → NUMERIC: improve compression and precision

`FLOAT` uses more space and stores approximate values. Converting to `NUMERIC` with 18 or fewer digits of precision reduces on-disk size and improves query speed.

```sql
-- Bad: approximate, larger storage
amount FLOAT

-- Good: exact, smaller storage, better compression
amount NUMERIC(18,2)
```

Conditions for safe conversion:
- Precision ≤ 18 digits total.
- All actual values fit within the declared precision and scale (values exceeding precision error; excess decimals are rounded).

### Run Length Encoding (RLE)

RLE is optimal for **low-cardinality columns that are sorted first** in the projection. It stores runs of identical values as `(count, value)` pairs instead of repeating the value.

```sql
CREATE PROJECTION dwh.fact_orders_by_status
AS SELECT status, order_date, order_id, amount
FROM dwh.fact_orders
ORDER BY status, order_date   -- status sorted first → long runs → RLE effective
SEGMENTED BY HASH(order_id) ALL NODES
ENCODING status RLE, order_date DELTAVAL, order_id DELTA, amount AUTO;
```

Requirements:
- Column must be the **leading sort key** (or an early one) in the projection so identical values are physically contiguous.
- Works best when `SELECT` predicates filter on the RLE column — the engine can skip entire runs.
- Do not apply RLE to high-cardinality or randomly ordered columns — it will inflate storage.

---

## Projections for Queries with Predicates

A covering projection must contain all columns referenced by the query (SELECT, WHERE, JOIN ON, GROUP BY, ORDER BY). If no covering projection exists, Vertica uses the super projection, which reads every column.

Design rule: put the most selective predicate columns **first** in `ORDER BY` so the optimizer can skip non-matching data blocks:

```sql
-- Query pattern: WHERE order_date = ? AND status = ?
CREATE PROJECTION dwh.fact_orders_by_date_status
AS SELECT order_date, status, order_id, user_id, amount
FROM dwh.fact_orders
ORDER BY order_date, status, order_id
SEGMENTED BY HASH(order_id) ALL NODES;
```

Check which projection a query uses:

```sql
SELECT projection_name
FROM v_monitor.query_plan_profiles
WHERE transaction_id = <txn_id> AND statement_id = <stmt_id>;
```

---

## GROUP BY Optimization

### GROUPBY PIPELINED vs GROUPBY HASH

Vertica uses two GROUP BY algorithms. **PIPELINED is faster** — it streams through sorted data without building an in-memory hash table. **HASH** builds a hash table first and uses more memory.

PIPELINED requires all three conditions:
1. All GROUP BY columns appear in the projection's `ORDER BY`.
2. GROUP BY columns form a **contiguous prefix** of the ORDER BY (or a contiguous subset starting from the first column).
3. If GROUP BY columns don't start the ORDER BY, all preceding ORDER BY columns must appear as single-value equality predicates in `WHERE`.

```sql
-- Projection ORDER BY: (order_date, status, user_id)
-- These GROUP BY clauses trigger PIPELINED:
GROUP BY order_date                           -- prefix
GROUP BY order_date, status                   -- prefix
GROUP BY order_date, status, user_id          -- full match
-- WHERE order_date = '2026-01-01' GROUP BY status  -- condition 3

-- These trigger HASH:
GROUP BY status                               -- not a prefix
GROUP BY order_date, user_id                  -- non-contiguous (skips status)
```

Force algorithm with the `GBYTYPE` hint (use only to investigate; let the optimizer decide in production):

```sql
SELECT status, SUM(amount)
FROM dwh.fact_orders
GROUP BY /*+ GBYTYPE(PIPE) */ status;
-- If PIPE conditions aren't met, Vertica warns and falls back to HASH.
```

### Avoiding RESEGMENT GROUPS

Rule: the `GROUP BY` clause must include **all** segmentation columns of the projection. It may include additional columns.

```sql
-- Projection segmented by HASH(order_id, user_id)
GROUP BY order_id, user_id          -- OK: contains all segmentation columns
GROUP BY order_id, user_id, status  -- OK: superset allowed
GROUP BY order_id                   -- RESEGMENT: missing user_id
GROUP BY order_id+1, user_id        -- RESEGMENT: expression on segmentation column
```

EXPLAIN indicators:
- `GROUPBY PIPELINED` — no resegmentation.
- `GROUPBY PIPELINED (RESEGMENT GROUPS)` — resegmentation required; redesign segmentation or GROUP BY.

When a JOIN precedes GROUP BY, the intermediate result may not be segmented on the GROUP BY keys, forcing resegmentation. Performing GROUP BY **before** the JOIN (pre-aggregating) can avoid this:

```sql
WITH agg AS (
    SELECT user_id, SUM(amount) AS revenue
    FROM dwh.fact_orders
    WHERE order_date >= DATE '2026-01-01'
    GROUP BY user_id          -- aggregate before join
)
SELECT u.country, agg.revenue
FROM agg
JOIN dwh.dim_users u ON agg.user_id = u.user_id;
```

---

## JOIN Optimization

### Merge join vs hash join

Merge join is **faster and uses less memory** than hash join. It requires both inputs to be sorted on the join key.

Trigger merge join by creating projections sorted on join columns (join key must be **first** in ORDER BY):

```sql
CREATE PROJECTION dwh.fact_orders_join_user
AS SELECT order_id, user_id, order_date, amount
FROM dwh.fact_orders
ORDER BY user_id, order_date
SEGMENTED BY HASH(user_id) ALL NODES;

CREATE PROJECTION dwh.dim_users_join
AS SELECT user_id, country, email
FROM dwh.dim_users
ORDER BY user_id
SEGMENTED BY HASH(user_id) ALL NODES;
```

EXPLAIN before and after:
```
-- Before: JOIN HASH [Cost: 752, Rows: 300K]
-- After:  JOIN MERGEJOIN(inputs presorted) [Cost: 731, Rows: 300K]
```

Alternatively, force pre-sort in-query (less efficient than a covering projection, but useful for ad-hoc queries):

```sql
SELECT o.order_id, o.amount, u.country
FROM (SELECT * FROM dwh.fact_orders ORDER BY user_id) o
JOIN (SELECT * FROM dwh.dim_users ORDER BY user_id) u
  ON o.user_id = u.user_id;
```

### Identical segmentation (local joins)

When both tables are segmented by the same expression on the same join key, the join executes **locally on each node** with no network data movement.

Requirements:
- Join condition must be pure equality: `t1.j1 = t2.j1 AND t1.j2 = t2.j2 ...`
- All segmentation columns must appear in the join condition.
- The same data type on both sides (e.g., `BIGINT` = `BIGINT`, not `BIGINT` = `FLOAT`).
- Same hash function and segment count on both tables.

```sql
-- Identically segmented: join is local, no RESEGMENT in EXPLAIN
CREATE TABLE dwh.fact_orders (...) SEGMENTED BY HASH(user_id) ALL NODES;
CREATE TABLE dwh.dim_users   (...) SEGMENTED BY HASH(user_id) ALL NODES;

SELECT o.order_id, u.country
FROM dwh.fact_orders o
JOIN dwh.dim_users u ON o.user_id = u.user_id;  -- local join
```

EXPLAIN check: if plan contains `RESEGMENT` or `BROADCAST` on the large table side, segmentation does not match.

### Predicate structure for joins

Each side of an equality predicate must reference **only one table**. Mixed-table predicates prevent efficient join planning:

```sql
-- Good: each side is from one table
ON t1.a + t1.b = t2.x - t2.y

-- Bad: right side mixes both tables — extra processing overhead
ON t1.a = t2.x + t1.b
```

### Joining variable-length string data

By default, Vertica buffers JOIN columns using their declared maximum length (e.g., `VARCHAR(1000)` always allocates 1000 bytes per row in the join buffer), wasting memory and causing join spills.

Enable variable-length join formatting to buffer only actual string lengths:

```sql
-- Session level
ALTER SESSION SET PARAMETER JoinDefaultTupleFormat = 'variable';

-- Query level (hint)
SELECT /*+ JFMT(variable) */ o.order_id, u.email
FROM dwh.fact_orders o
JOIN dwh.dim_users u ON o.user_id = u.user_id;
```

Verify with `EXPLAIN VERBOSE` — look for `JF_EE_VARIABLE_FORMAT` (good) vs `JF_EE_FIXED_FORMAT`.

**Limitation**: variable-length formatting is not available for **merge joins** and **event series joins** — only hash joins benefit.

---

## ORDER BY Optimization (Sort Elimination)

Vertica stores projection data in ascending sort order. If the query's `ORDER BY` matches the projection's `ORDER BY` prefix, the sort phase is eliminated entirely.

```sql
-- Projection ORDER BY: (order_date, user_id, order_id)
-- Sort eliminated for:
ORDER BY order_date
ORDER BY order_date, user_id
ORDER BY order_date, user_id, order_id

-- Sort NOT eliminated for:
ORDER BY order_date DESC       -- DESC forces re-sort (storage is always ASC)
ORDER BY user_id               -- not a prefix (order_date must come first)
ORDER BY order_date, order_id  -- non-contiguous (user_id is skipped)
```

Design projection ORDER BY by the most common ORDER BY patterns in queries. For queries that always sort DESC, accept the re-sort or invert the data at write time.

---

## LIMIT and Top-K Optimization

Vertica applies **Top-K optimization** automatically when a query has both `ORDER BY` and `LIMIT`. Instead of sorting the entire result set and then truncating, the optimizer tracks only the K best rows at each step — avoiding full-dataset sort and disk spill.

```sql
-- Top-K active: fast, avoids full sort
SELECT user_id, SUM(amount) AS revenue
FROM dwh.fact_orders
GROUP BY user_id
ORDER BY revenue DESC
LIMIT 100;
```

LIMIT without ORDER BY produces nondeterministic results — always include ORDER BY when LIMIT is used in production.

Window LIMIT restricts rows per partition:

```sql
SELECT user_id, order_date, amount,
       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC) AS rn
FROM dwh.fact_orders
QUALIFY rn <= 5;   -- keep latest 5 orders per user (more efficient than LIMIT in subquery)
```

---

## DISTINCT Optimization

### COUNT(DISTINCT) and exact distinct aggregates

Single `COUNT(DISTINCT col)` on a low-cardinality column is handled efficiently if the projection is sorted on that column. Multiple `COUNT(DISTINCT ...)` on different columns in one SELECT forces multiple passes — restructure as separate subqueries if performance matters.

### Approximate COUNT DISTINCT (preferred for large tables)

Vertica provides approximate distinct count functions that are orders of magnitude faster for high-cardinality columns:

```sql
-- Exact (slow on large tables)
SELECT COUNT(DISTINCT user_id) FROM dwh.fact_orders;

-- Approximate (fast, ~2% error by default)
SELECT APPROXIMATE_COUNT_DISTINCT(user_id) FROM dwh.fact_orders;

-- With explicit hyperloglog precision (1–14, default varies by version)
SELECT APPROXIMATE_COUNT_DISTINCT(user_id USING PARAMETERS hll_bits=12)
FROM dwh.fact_orders;
```

Use `APPROXIMATE_COUNT_DISTINCT` in dashboards and exploratory queries where 1-2% error is acceptable. Reserve exact `COUNT(DISTINCT)` for auditing and reconciliation.

---

## Analytic Function Optimization

### Avoid empty OVER() clauses

`OVER()` with no PARTITION BY runs on a **single node** — the entire input is treated as one partition. For large tables this is a serial bottleneck:

```sql
-- Bad: executes on a single node
SELECT order_id, SUM(amount) OVER() AS grand_total
FROM dwh.fact_orders;

-- Better: distribute by a business key
SELECT order_id, SUM(amount) OVER (PARTITION BY order_date) AS daily_total
FROM dwh.fact_orders;
```

If a true global aggregate is needed, compute it in a CTE and join back rather than using `OVER()`:

```sql
WITH totals AS (
    SELECT SUM(amount) AS grand_total FROM dwh.fact_orders
)
SELECT o.order_id, o.amount, t.grand_total
FROM dwh.fact_orders o
CROSS JOIN totals t;
```

### NULL sort order — eliminate runtime sort

The optimizer can eliminate the sort step for an analytic `ORDER BY` when it matches the **projection's physical sort order and NULL placement**. Default NULL placement by data type:

| Data type | Physical storage default | OVER(ORDER BY) default |
|---|---|---|
| NUMERIC, INTEGER, DATE, TIME, TIMESTAMP, INTERVAL | ASC, NULLS FIRST | ASC → NULLS LAST |
| FLOAT, STRING, BOOLEAN | ASC, NULLS LAST | ASC → NULLS LAST |

To eliminate runtime sort, align the analytic clause with physical storage:

```sql
-- INTEGER column stored ASC NULLS FIRST (physical default)
-- Specify NULLS FIRST explicitly to match and skip runtime sort:
SELECT user_id,
    ROW_NUMBER() OVER (
        PARTITION BY country
        ORDER BY user_id ASC NULLS FIRST  -- matches physical storage → sort eliminated
    ) AS rn
FROM dwh.dim_users;
```

Use `NULLS AUTO` to let Vertica choose the placement that avoids a sort:

```sql
ORDER BY user_id ASC NULLS AUTO
```

When in doubt, check the projection's actual sort and null order in `v_catalog.projection_columns` (`sort_position`, `sort_order`, `sort_null_order`) and align the analytic clause accordingly.

---

## INSERT-SELECT Optimization

### Matching sort orders (avoid sort phase)

`INSERT /*+direct*/ INTO target SELECT ... FROM source` skips the sort step if the SELECT output order matches the target projection's `ORDER BY`.

```sql
-- Avoids sort: column order matches target projection ORDER BY (col1, col2, col3)
INSERT /*+direct*/ INTO dwh.fact_orders_archive
SELECT col1, col2, col3 FROM dwh.fact_orders
WHERE order_date < DATE '2024-01-01';

-- Requires sort: column order resequenced
INSERT /*+direct*/ INTO dwh.fact_orders_archive
SELECT col1, col3, col2 FROM dwh.fact_orders   -- col2 and col3 swapped

-- Avoids sort: explicit ORDER BY overrides column order
INSERT /*+direct*/ INTO dwh.fact_orders_archive
SELECT col1, col3, col2 FROM dwh.fact_orders
ORDER BY col1, col2, col3;   -- explicit ORDER BY matches target projection
```

### Matching segmentation (avoid resegmentation)

Create source and target projections with the same segmentation expression and segment count. The `RESEGMENT` step in the INSERT plan disappears, reducing network traffic:

```sql
CREATE PROJECTION dwh.fact_orders_archive_p
AS SELECT col1, col2, col3 FROM dwh.fact_orders_archive
ORDER BY col1, col2, col3
SEGMENTED BY HASH(col3) ALL NODES;  -- same as source projection
```

Check INSERT plan with `EXPLAIN`:
- `RESEGMENT` in the plan → segmentation mismatch.
- No `RESEGMENT` → INSERT is fully local per node.

---

## DELETE and UPDATE Internals

Vertica is **optimized for read-heavy analytic workloads**. DELETE and UPDATE are internally expensive because:

1. Deleted/updated rows are **logically marked**, not removed immediately.
2. **All projections** must be updated — the operation speed is capped by the slowest projection.
3. Physically deleted rows are purged later by the Tuple Mover; until then, they occupy storage.

Minimize the cost:
- Align `WHERE` predicates with the partition expression to enable **partition-scoped deletion** (drops entire ROS containers instead of marking rows):
  ```sql
  DELETE FROM dwh.fact_orders
  WHERE order_date BETWEEN DATE '2025-01-01' AND DATE '2025-12-31';
  ```
- Prefer `TRUNCATE TABLE` when removing all rows — it is immediate and drops ROS containers.
- For bulk updates affecting most of a partition, use **partition swap** (CTAS staging table + `ALTER TABLE SWAP PARTITION`) instead of `UPDATE`.
- Limit the number of projections on tables with heavy DELETE/UPDATE churn — fewer projections = faster writes.
- After heavy DELETE/UPDATE, force purge to reclaim space:
  ```sql
  SELECT PURGE_TABLE('dwh.fact_orders');
  SELECT PURGE_PARTITION('dwh.fact_orders', '2025-01-01', '2025-12-31');
  ```

---

## Data Collector (dc_*) Diagnostic Queries

Use `dc_requests_issued` and `dc_requests_completed` to investigate slow queries. Join on `session_id` AND `node_name` to avoid resegmentation:

```sql
-- Find slowest queries in a time window
SELECT
    ri.session_id,
    ri.user_name,
    rc.processed_row_count,
    DATEDIFF('millisecond', ri.time, rc.time) AS duration_ms,
    ri.request
FROM dc_requests_issued  ri
JOIN dc_requests_completed rc
  ON  ri.session_id = rc.session_id   -- always include session_id
  AND ri.node_name  = rc.node_name    -- include node_name to avoid RESEGMENT
WHERE ri.time > '2026-05-01'::TIMESTAMPTZ
  AND ri.time < '2026-05-02'::TIMESTAMPTZ
ORDER BY duration_ms DESC
LIMIT 50;
```

Rules for querying DC tables:
- Always filter on the `time` column using **static TIMESTAMPTZ literals** — volatile functions (`NOW()`, `CURRENT_TIMESTAMP`) prevent predicate pushdown.
- Include `node_name` in join conditions between DC tables — it prevents resegmentation because the initiator node writes all request rows.
- Select only columns you need — DC tables are wide; unnecessary columns increase I/O.

```sql
-- Bad: volatile function disables partition pruning
WHERE time > NOW() - INTERVAL '1 day'

-- Good: static cast
WHERE time > (CURRENT_TIMESTAMP - INTERVAL '1 day')::TIMESTAMPTZ
-- or better: use a literal computed outside the query
WHERE time > '2026-05-15 00:00:00'::TIMESTAMPTZ
  AND time < '2026-05-16 00:00:00'::TIMESTAMPTZ
```

Check session-level statistics:

```sql
SELECT
    ss.session_id,
    ss.user_name,
    se.session_duration_ms,
    ss.client_hostname
FROM dc_session_starts ss
JOIN dc_session_ends se
  ON  ss.session_id = se.session_id
  AND ss.node_name  = se.node_name
WHERE ss.time > '2026-05-15'::TIMESTAMPTZ
ORDER BY se.session_duration_ms DESC
LIMIT 20;
```

---

## Projection Management

```sql
-- Check which projections cover a table and their status
SELECT projection_name, is_super_projection, is_up_to_date, has_statistics
FROM v_catalog.projections
WHERE projection_schema = 'dwh' AND anchor_table_name = 'fact_orders';

-- Check projection column sort and encoding details
SELECT column_name, sort_position, sort_order, sort_null_order, encoding_type
FROM v_catalog.projection_columns
WHERE projection_name = 'fact_orders_p1'
ORDER BY sort_position;

-- Check which projection was used by a recent query
SELECT node_name, path_id, path_line
FROM v_monitor.query_plan_profiles
WHERE transaction_id = <txn_id> AND statement_id = <stmt_id>
ORDER BY path_id, path_line;

-- Refresh projections after bulk loads or design changes
SELECT MAKE_AHM_NOW();
SELECT START_REFRESH();
-- Or refresh a single table:
SELECT REFRESH('dwh.fact_orders');

-- Re-run statistics after projection changes
SELECT ANALYZE_STATISTICS('dwh.fact_orders');
SELECT ANALYZE_STATISTICS('dwh.fact_orders', 'order_date');
```

---

## Anti-Patterns

Do not:
- Query without `EXPLAIN` first when diagnosing a slow query — always read the plan.
- Use `OVER()` with an empty OVER clause on large tables — it runs on a single node.
- Rely on `COUNT(DISTINCT)` for high-cardinality columns in production dashboards — use `APPROXIMATE_COUNT_DISTINCT`.
- Use `FLOAT` for monetary or fixed-precision values — it wastes storage and compresses poorly; use `NUMERIC(p,s)`.
- Apply RLE encoding to high-cardinality or unsorted columns — it inflates size instead of compressing.
- Ignore `RESEGMENT` in EXPLAIN — it means data is moving across nodes for every query execution.
- Use `DESC` ORDER BY expecting sort elimination — Vertica's physical storage is always ASC; DESC always triggers a re-sort.
- Leave purge on heavy DELETE/UPDATE tables to chance — call `PURGE_TABLE` explicitly after large deletes.
- Use volatile functions (`NOW()`, `TRUNC()`) in `WHERE` predicates on DC tables — they disable predicate pushdown.
- Create many projections on write-heavy tables — each projection is updated on every INSERT/UPDATE/DELETE.
- Skip `ANALYZE_STATISTICS` after creating or refreshing projections — stale stats cause poor plan choices.

---

## Output Expectations

When optimizing Vertica queries:
- Show the `EXPLAIN` output excerpt that reveals the problem (hash join, RESEGMENT, GROUPBY HASH).
- Propose the specific projection DDL that fixes the issue, with explicit `ORDER BY`, `SEGMENTED BY`, and `ENCODING`.
- Explain which EXPLAIN token disappears or changes as a result of the fix.
- Mention when `ANALYZE_STATISTICS` and `REFRESH` are needed after the change.
- Call out version-specific limitations (e.g., variable-length join format is unavailable for merge joins in v11.1).

## References

- Local deep-dive spec: `/docs/specs/vertica_query_optimization_v11.md`
- Vertica 11.1.x Query Optimization: https://docs.vertica.com/11.1.x/en/data-analysis/query-optimization/
- Vertica 11.1.x EXPLAIN: https://docs.vertica.com/11.1.x/en/sql-reference/statements/explain/
- Vertica 11.1.x Projections: https://docs.vertica.com/11.1.x/en/concepts/projections/
- Vertica 11.1.x Analytic Functions: https://docs.vertica.com/11.1.x/en/sql-reference/functions/analytic-functions/
