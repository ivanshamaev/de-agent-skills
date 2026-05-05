---
name: pyspark-style-guide
description: Write idiomatic, performant PySpark code following the Palantir PySpark Style Guide.
---

When writing or reviewing PySpark code, follow these rules derived from the [Palantir PySpark Style Guide](https://github.com/palantir/pyspark-style-guide).

## Column Selection

Prefer referencing columns by string name (Spark 3.0+) over `df.colName` or `F.col()` unless disambiguation is needed:

```python
# bad
df = df.select(F.lower(df.colA))

# good
df = df.select(F.lower('colA'))

# acceptable when two dataframes share a column name (e.g. in joins)
df = df.join(df2, on=(df.key == df2.key), how='left')
```

## Logical Operations

Keep filter/when expressions to **three sub-expressions at most**. Extract complex conditions into named variables:

```python
# bad
F.when((F.col('status') == 'Delivered') | ((F.datediff('deliveryDate', 'current_date') < 0) & (F.col('reg') != '')), 'In Service')

# good
is_delivered = (F.col('status') == 'Delivered')
date_passed  = (F.datediff('deliveryDate', 'current_date') < 0)
has_reg      = (F.col('reg').rlike('.+'))

F.when(is_delivered | (date_passed & has_reg), 'In Service')
```

## `select` as a Schema Contract

Use `select` at the start or end of a transform to declare the expected schema. Keep each selected column to **one** `spark.sql.functions` call plus an optional `.alias()`. If you need more than three transformed columns in a single select, extract it into a `clean_<name>()` function.

Prefer `select` + `.alias()` over `withColumnRenamed`, and `select` + `.cast()` over `withColumn` for type changes:

```python
# bad
df.select('key', 'comments').withColumnRenamed('comments', 'num_comments')

# good
df.select('key', F.col('comments').alias('num_comments'))

# bad
df.select('comments').withColumn('comments', F.col('comments').cast('double'))

# good
df.select(F.col('comments').cast('double'))
```

Use `.withColumn()` when adding a single computed column that would make a `select` too complex:

```python
# good
df = df.withColumn(
    'days_open',
    (F.coalesce(F.unix_timestamp('closed_at'), F.unix_timestamp()) - F.unix_timestamp('created_at')) / 86400,
)
```

Avoid `.drop()` for trimming schemas; prefer an explicit `select` instead. Drop columns after joins when the join introduces redundant columns.

## Empty Columns

Always use `F.lit(None)` for placeholder columns — never empty strings or sentinel values like `'NA'`:

```python
# bad
df = df.withColumn('foo', F.lit(''))

# good
df = df.withColumn('foo', F.lit(None))
```

## Comments

Write comments that explain **why**, not **what**. The code itself shows what is happening; comments should provide data context or rationale that cannot be inferred from the code alone.

## UDFs

Avoid UDFs. They are dramatically less performant than native PySpark. Almost every UDF can be replaced with a combination of native `pyspark.sql.functions`.

## Joins

- Always specify `how=` as a keyword argument.
- Prefer `left` joins over `right` joins; swap the dataframe order instead.
- Use dataframe aliases to disambiguate columns after joins instead of bulk-renaming columns:

```python
# bad
flights = flights.join(aircraft, 'aircraft_id', 'inner')

# good
flights = flights.join(aircraft, 'aircraft_id', how='inner')

# bad – avoid right joins
flights = aircraft.join(flights, 'aircraft_id', how='right')

# good
flights = flights.join(aircraft, 'aircraft_id', how='left')

# bad – bulk rename to avoid collisions
for col in columns:
    flights = flights.withColumnRenamed(col, 'flights_' + col)

# good – use aliases
flights = flights.alias('flights')
parking  = parking.alias('parking')
flights  = flights.join(parking, on='flight_code', how='left')
flights  = flights.select(
    F.col('flights.start_time').alias('flight_start_time'),
    F.col('parking.total_time').alias('client_parking_total_time'),
)
```

Never use `.dropDuplicates()` or `.distinct()` to hide join explosions. Investigate and fix the root cause.

## Window Functions

Always specify an explicit frame (`rowsBetween` or `rangeBetween`). Without one, Spark infers a frame that changes based on whether the window is ordered, leading to subtle bugs:

```python
# bad – frame is implicit and changes with ordering
w = W.partitionBy('key').orderBy('num')

# good – frame is always explicit
w = W.partitionBy('key').orderBy('num').rowsBetween(W.unboundedPreceding, W.unboundedFollowing)
```

For analytic functions like `F.first()` / `F.last()`, pass `ignorenulls=True` when nulls should be skipped. Use `F.asc_nulls_last()` / `F.asc_nulls_first()` to control null ordering explicitly.

## Empty `partitionBy()`

Never use `W.partitionBy()` with no arguments — it forces all data into a single partition. Use an aggregation instead:

```python
# bad
w  = W.partitionBy()
df = df.select(F.sum('num').over(w).alias('sum'))

# good
df = df.agg(F.sum('num').alias('sum'))
```

## Chaining Expressions

- Keep chains to **5 statements or fewer** in a single block.
- Do **not** mix different operation types (select/filter, join, withColumn) in one chain.
- Group operations by type into separate assignment steps.
- Wrap multi-line chains in parentheses; never use `\` line continuations:

```python
# bad – mixed operations, long chain
df = (
    df
    .select('a', 'b', 'key')
    .filter(F.col('a') == 'x')
    .withColumn('ratio', F.col('b') / F.col('c'))
    .join(df2, 'key', how='inner')
    .join(df3, 'key', how='left')
    .drop('c')
)

# good – split by operation type
df = (
    df
    .select('a', 'b', 'c', 'key')
    .filter(F.col('a') == 'x')
)
df = df.withColumn('ratio', F.col('b') / F.col('c'))
df = (
    df
    .join(df2, 'key', how='inner')
    .join(df3, 'key', how='left')
    .drop('c')
)
```

For long or repeated logic, extract it into a well-named function rather than creating an enormous chain.

## General Best Practices

- Keep files under **250 lines** and functions under **70 lines**.
- Use `from pyspark.sql import functions as F, types as T, Window as W` — avoid introducing new aliases.
- Extract magic strings and integer literals into named constants or variables.
- Never leave commented-out code in the repository; use git history instead.
- Use `F.asc_nulls_last()` / `F.desc_nulls_last()` when null ordering matters.
- Avoid `.otherwise()` as a catch-all fallback — it silently swallows unexpected values.