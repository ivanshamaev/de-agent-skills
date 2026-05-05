# SKILL: Spark SQL Generator

## When to use

Use this skill when:
- User asks for SQL queries
- Data is in Spark / Hive / Lakehouse
- Query can be expressed in SQL instead of PySpark

---

## Core Principles

- Prefer ANSI SQL compatible syntax
- Use CTEs for readability
- Avoid SELECT *
- Push filters early
- Use partition columns in WHERE

---

## Query Structure

Always structure queries like:

```sql
WITH base AS (
    SELECT ...
    FROM table
    WHERE ...
),
aggregated AS (
    SELECT ...
    FROM base
)
SELECT * FROM aggregated;
````

---

## Common Patterns

### Filtering

```sql
SELECT user_id
FROM events
WHERE event_date >= '2025-01-01'
```

---

### Aggregation

```sql
SELECT country, COUNT(*) AS cnt
FROM events
GROUP BY country
```

---

### Joins

```sql
SELECT e.user_id, u.country
FROM events e
JOIN users u
ON e.user_id = u.user_id
```

---

### Window Functions

```sql
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time DESC) AS rn
FROM events
```

---

### Deduplication

```sql
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
    FROM table
)
SELECT *
FROM ranked
WHERE rn = 1
```

---

## Performance Rules

* Filter on partition columns:

```sql
WHERE event_date = '2025-01-01'
```

* Avoid cross joins
* Avoid unnecessary DISTINCT
* Limit data early if possible

---

## Anti-Patterns (DO NOT DO)

❌ SELECT * on large tables
❌ Missing WHERE on partitioned tables
❌ Nested subqueries instead of CTE
❌ Cartesian joins
❌ Using DISTINCT instead of proper grouping

---

## Spark-Specific Features

### Insert overwrite

```sql
INSERT OVERWRITE TABLE dwh.revenue
SELECT ...
```

---

### Create table

```sql
CREATE TABLE dwh.revenue
USING PARQUET
AS
SELECT ...
```

---

### Delta (if available)

```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

---

## Example Query

```sql
WITH purchases AS (
    SELECT user_id, amount
    FROM events
    WHERE event_type = 'purchase'
),
joined AS (
    SELECT p.user_id, u.country, p.amount
    FROM purchases p
    JOIN users u ON p.user_id = u.user_id
)
SELECT country, SUM(amount) AS revenue
FROM joined
GROUP BY country;
```

---

## Output Expectations

* Always return valid Spark SQL
* Use CTEs for complex logic
* Optimize for large datasets
* Avoid unnecessary columns

