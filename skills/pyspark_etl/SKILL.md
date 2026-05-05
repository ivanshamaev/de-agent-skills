# SKILL: PySpark ETL Engineer

## When to use
Use this skill when:
- You need to write ETL pipelines in PySpark
- You are transforming large datasets
- You are working with DataFrames, not pandas
- Data volume is large (GB–TB+)

---

## Core Principles

- Always use DataFrame API (avoid RDD unless explicitly required)
- Avoid `collect()` on large datasets
- Prefer lazy transformations
- Push filters early (predicate pushdown)
- Minimize shuffles
- Use built-in Spark functions instead of UDFs

---

## Coding Standards

### Imports
```python
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
````

---

### Read Data

Prefer columnar formats:

```python
df = spark.read.parquet("/data/input/")
```

With options:

```python
df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load("/data/file.csv")
)
```

---

### Transformations

#### Select columns

```python
df = df.select("user_id", "event_time")
```

#### Filter early

```python
df = df.filter(F.col("event_date") >= "2025-01-01")
```

#### Add columns

```python
df = df.withColumn("event_day", F.to_date("event_time"))
```

#### Aggregations

```python
df = (
    df.groupBy("country")
      .agg(F.count("*").alias("cnt"))
)
```

---

### Joins

```python
df = events.join(users, on="user_id", how="inner")
```

Best practices:

* Broadcast small tables:

```python
from pyspark.sql.functions import broadcast

df = events.join(broadcast(users), "user_id")
```

---

### Write Data

```python
df.write.mode("overwrite").parquet("/data/output/")
```

Partitioned write:

```python
df.write.partitionBy("event_date").parquet("/data/output/")
```

---

## Performance Rules

* Repartition only when necessary:

```python
df = df.repartition(200, "user_id")
```

* Use cache only if reused:

```python
df.cache()
```

* Avoid wide transformations unless needed

---

## Anti-Patterns (DO NOT DO)

❌ `collect()` on large datasets
❌ Using Python loops instead of Spark transformations
❌ UDFs when built-in functions exist
❌ Writing unpartitioned large tables
❌ Multiple `.withColumn()` chains when `.select()` can be used

---

## Example Pipeline

```python
events = spark.read.parquet("/raw/events/")
users = spark.read.parquet("/raw/users/")

result = (
    events
    .filter(F.col("event_type") == "purchase")
    .join(users, "user_id")
    .groupBy("country")
    .agg(F.sum("amount").alias("revenue"))
)

result.write.mode("overwrite").parquet("/dwh/revenue/")
```

---

## Output Expectations

* Always return clean, readable PySpark code
* Use chaining style
* Include imports if missing
* Optimize for large-scale data
