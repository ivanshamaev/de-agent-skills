# Spark SQL на HDFS/Hive: Tutorial 101 для разработчика

Это подробный практический гайд по работе со Spark SQL поверх HDFS и Hive Metastore, когда данные хранятся в Parquet и разложены по партициям. Документ написан для разработчика, который должен не просто написать запрос, а построить и сопровождать production data lake или production ETL/ELT pipeline на больших данных.

Гайд основан на локальных спецификациях и skills репозитория:

- `docs/specs/spark_sql_enterprise.md`
- `docs/specs/spark_sql_hdfs_hive_operations.md`
- `docs/specs/hdfs_hive_parquet_datalake.md`
- `skills/spark_sql/SKILL.md`

Цель: дать единую картину от базовых понятий до advanced production-практик: архитектура lake, Hive DDL, HDFS layout, Parquet, партиции, Spark SQL transformations, joins, windows, overwrite, backfill, compaction, statistics, `EXPLAIN`, debugging, metadata repair и data quality.

## 1. Что мы строим

Типичная целевая архитектура:

```text
HDFS
  хранит физические файлы и директории

Hive Metastore
  хранит базы, таблицы, схемы, партиции, locations и table properties

Spark SQL
  читает Hive metadata, строит physical plan, читает Parquet-файлы с HDFS,
  выполняет transformations, joins, aggregations, windows и writes

Parquet
  columnar формат хранения для аналитики

Partitioned tables
  таблицы физически разложены по директориям вида event_date=2026-05-05
```

Ключевая мысль: Hive table и HDFS files не одно и то же. Hive знает метаданные. HDFS хранит файлы. Spark использует и то, и другое. Если метаданные и файлы расходятся, запрос может вернуть ноль строк, прочитать не те данные или упасть с `FileNotFoundException`.

## 2. Минимальный словарь

`HDFS`: распределенная файловая система. В ней лежат директории и файлы.

`Hive Metastore`: каталог метаданных. В нем есть database, table, schema, partition list, table location, partition locations.

`External table`: Hive управляет метаданными, но не обязан владеть физическими файлами. Drop external table обычно удаляет metadata, но не HDFS data.

`Managed table`: Hive/Spark владеет table lifecycle. Drop может удалить данные. Семантика зависит от версии и платформы, поэтому всегда проверяй.

`Partition`: логическая часть таблицы, обычно соответствующая HDFS-директории. Например `event_date=2026-05-05/country=US`.

`Partition pruning`: Spark читает только нужные партиции, если фильтр написан по partition column напрямую.

`Predicate pushdown`: Spark/Parquet может применить фильтры ближе к чтению файлов.

`Column pruning`: Spark читает только нужные колонки из Parquet.

`Shuffle`: перераспределение данных между executors. Возникает на joins, group by, distinct, repartition, windows.

`Skew`: часть ключей или партиций намного больше остальных, из-за чего несколько tasks работают долго.

`Small files`: много маленьких файлов, из-за которых страдают namenode, planning time и scan performance.

## 3. Mental model: как Spark SQL читает Hive table

Когда ты пишешь:

```sql
SELECT
    user_id,
    SUM(amount) AS revenue
FROM raw.events
WHERE event_date = DATE '2026-05-05'
GROUP BY user_id;
```

Spark делает примерно следующее:

1. Идет в Hive Metastore и узнает схему таблицы `raw.events`.
2. Узнает table location и partition metadata.
3. Анализирует `WHERE event_date = DATE '2026-05-05'`.
4. Отбирает только нужные partition locations.
5. Строит physical plan.
6. Читает Parquet-файлы только нужных партиций.
7. Читает только колонки `user_id`, `amount`, `event_date`.
8. Делает partial aggregation на executors.
9. Делает shuffle по `user_id`.
10. Делает final aggregation.

Если ты напишешь:

```sql
WHERE TO_DATE(event_time) = DATE '2026-05-05'
```

а таблица partitioned by `event_date`, Spark может не использовать partition pruning и прочитать намного больше данных.

## 4. Архитектура data lake на HDFS

Рекомендуемая структура:

```text
/data/lake/
  landing/
  raw/
  refined/
  mart/
  quarantine/
  tmp/
  sandbox/
```

Смысл слоев:

`landing`: данные как пришли из источника. Часто CSV/JSON/Avro/raw files. Используются для replay и аудита.

`raw`: типизированные Parquet-таблицы, близкие к источнику. Минимум трансформаций. Есть lineage fields.

`refined`: очищенные, дедуплицированные, нормализованные и согласованные данные.

`mart`: готовые витрины, факты, агрегаты, dimensions, snapshots.

`quarantine`: плохие записи, которые не прошли parsing/validation.

`tmp`: временные staging данные с TTL.

`sandbox`: исследовательская зона, не production dependency.

Пример Hive databases:

```sql
CREATE DATABASE IF NOT EXISTS landing
LOCATION 'hdfs:///data/lake/landing';

CREATE DATABASE IF NOT EXISTS raw
LOCATION 'hdfs:///data/lake/raw';

CREATE DATABASE IF NOT EXISTS refined
LOCATION 'hdfs:///data/lake/refined';

CREATE DATABASE IF NOT EXISTS mart
LOCATION 'hdfs:///data/lake/mart';

CREATE DATABASE IF NOT EXISTS quarantine
LOCATION 'hdfs:///data/lake/quarantine';
```

## 5. Почему Parquet

Parquet хорош для аналитики, потому что:

- хранит данные по колонкам;
- позволяет читать только нужные колонки;
- поддерживает compression;
- хранит statistics на уровне row groups;
- хорошо работает со Spark SQL, Hive, Trino/Presto, Impala;
- подходит для больших фактов и витрин.

Правила:

- Для curated tables используй Parquet или ORC, не CSV/JSON.
- Для money используй `DECIMAL`, не `DOUBLE`.
- Для id чаще используй `STRING`, если не нужна арифметика.
- Не смешивай Parquet и ORC в одной table location.
- Не меняй физические типы колонок между партициями без миграционного плана.

## 6. External tables как default

Для data lake на HDFS чаще всего используют external tables:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS raw.events (
    event_id STRING,
    user_id STRING,
    event_time TIMESTAMP,
    event_type STRING,
    amount DECIMAL(18, 2),
    ingestion_ts TIMESTAMP,
    source_file STRING,
    batch_id STRING
)
PARTITIONED BY (event_date DATE, source_system STRING)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/raw/events'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'lake.layer' = 'raw',
    'lake.owner' = 'data-platform'
);
```

Почему external:

- drop metadata не должен случайно удалить raw data;
- данные могут читать разные engines;
- recovery часто делается через re-register partitions;
- HDFS retention управляется отдельно.

Managed table можно использовать, если платформа явно владеет lifecycle и все понимают последствия `DROP`/`TRUNCATE`.

## 7. HDFS layout для Hive partitions

Хороший layout:

```text
hdfs:///data/lake/raw/events/
  event_date=2026-05-05/
    source_system=web/
      part-00000-....snappy.parquet
      part-00001-....snappy.parquet
```

Правила:

- Одна таблица - один table root.
- Не смешивай разные schemas под одним root.
- Не складывай temporary output внутрь production table root.
- Partition directory names должны совпадать с partition columns.
- Не используй high-cardinality partitions.
- Не пиши production data в `/user/ivan/tmp`.
- Temporary/staging данные держи в `/data/lake/tmp/...`.

## 8. Выбор partition columns

Хорошие partition columns:

- `event_date`
- `order_date`
- `ingestion_date`
- `snapshot_date`
- `business_date`
- `country`, `region`, `source_system`, если cardinality небольшая и фильтры частые

Плохие partition columns:

- `user_id`
- `event_id`
- `request_id`
- `uuid`
- transaction id
- любое поле с миллионами уникальных значений

Главное правило: partition column должен реально использоваться в `WHERE`.

Хорошо:

```sql
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
```

Плохо:

```sql
WHERE TO_DATE(event_time) BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
```

Если нужна точная timestamp-логика:

```sql
WHERE event_date = DATE '2026-05-05'
  AND event_time >= TIMESTAMP '2026-05-05 00:00:00'
  AND event_time <  TIMESTAMP '2026-05-06 00:00:00'
```

## 9. Naming conventions

Практичный стандарт:

- databases: `landing`, `raw`, `refined`, `mart`, `quarantine`;
- table names: lowercase snake_case;
- partition date: `event_date`, `order_date`, `ingestion_date`;
- timestamps: `event_time`, `ingestion_ts`, `processed_at`, `updated_at`;
- source lineage: `source_system`, `source_file`, `batch_id`;
- mart tables должны отражать grain: `country_daily_revenue`, `user_daily_metrics`.

Не называй production tables `test1`, `new_events`, `events_tmp_final2`.

## 10. Spark SQL session baseline

Перед тяжелыми jobs обычно включают AQE:

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.sources.partitionOverwriteMode = dynamic;
SET spark.sql.storeAssignmentPolicy = ANSI;
```

Shuffle partitions:

```sql
SET spark.sql.shuffle.partitions = 4000;
```

Но это не магическое число. Ориентир:

- 128 MB per shuffle partition: больше parallelism, меньше memory pressure;
- 256-512 MB: меньше overhead, но нужно больше стабильной памяти;
- слишком мало partitions: spills и stragglers;
- слишком много partitions: scheduler overhead и small files.

Если statistics плохие и Spark ошибочно broadcast'ит большую таблицу:

```sql
SET spark.sql.autoBroadcastJoinThreshold = -1;
```

## 11. Первый production table: raw events

DDL:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS raw.events (
    event_id STRING,
    user_id STRING,
    event_time TIMESTAMP,
    event_type STRING,
    amount DECIMAL(18, 2),
    ingestion_ts TIMESTAMP,
    source_file STRING,
    batch_id STRING
)
PARTITIONED BY (event_date DATE, source_system STRING)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/raw/events'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'lake.layer' = 'raw',
    'lake.owner' = 'data-platform'
);
```

Проверка:

```sql
SHOW CREATE TABLE raw.events;
DESCRIBE FORMATTED raw.events;
SHOW TBLPROPERTIES raw.events;
SHOW PARTITIONS raw.events;
```

Проверка HDFS:

```bash
hdfs dfs -ls /data/lake/raw/events
hdfs dfs -du -s -h /data/lake/raw/events
hdfs dfs -count -h /data/lake/raw/events
```

## 12. Landing to raw: типизация и quarantine

Landing table может быть CSV:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS landing.events_csv (
    raw_event_id STRING,
    raw_user_id STRING,
    raw_event_time STRING,
    raw_event_type STRING,
    raw_amount STRING
)
PARTITIONED BY (ingestion_date DATE, source_system STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///data/lake/landing/events_csv';
```

Raw insert:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE raw.events
PARTITION (event_date, source_system)
SELECT
    raw_event_id AS event_id,
    raw_user_id AS user_id,
    TRY_CAST(raw_event_time AS TIMESTAMP) AS event_time,
    raw_event_type AS event_type,
    TRY_CAST(raw_amount AS DECIMAL(18, 2)) AS amount,
    CURRENT_TIMESTAMP() AS ingestion_ts,
    INPUT_FILE_NAME() AS source_file,
    '${batch_id}' AS batch_id,
    TO_DATE(TRY_CAST(raw_event_time AS TIMESTAMP)) AS event_date,
    source_system
FROM landing.events_csv
WHERE ingestion_date = DATE '2026-05-05'
  AND source_system = 'web'
  AND raw_event_id IS NOT NULL
  AND TRY_CAST(raw_event_time AS TIMESTAMP) IS NOT NULL;
```

Quarantine table:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS quarantine.events_invalid (
    raw_event_id STRING,
    raw_user_id STRING,
    raw_event_time STRING,
    raw_event_type STRING,
    raw_amount STRING,
    invalid_reason STRING,
    quarantined_at TIMESTAMP
)
PARTITIONED BY (ingestion_date DATE, source_system STRING)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/quarantine/events_invalid';
```

Quarantine insert:

```sql
INSERT INTO TABLE quarantine.events_invalid
PARTITION (ingestion_date, source_system)
SELECT
    raw_event_id,
    raw_user_id,
    raw_event_time,
    raw_event_type,
    raw_amount,
    CASE
        WHEN raw_event_id IS NULL THEN 'missing_event_id'
        WHEN TRY_CAST(raw_event_time AS TIMESTAMP) IS NULL THEN 'invalid_event_time'
        WHEN TRY_CAST(raw_amount AS DECIMAL(18, 2)) IS NULL THEN 'invalid_amount'
        ELSE 'unknown'
    END AS invalid_reason,
    CURRENT_TIMESTAMP() AS quarantined_at,
    ingestion_date,
    source_system
FROM landing.events_csv
WHERE ingestion_date = DATE '2026-05-05'
  AND source_system = 'web'
  AND (
      raw_event_id IS NULL
      OR TRY_CAST(raw_event_time AS TIMESTAMP) IS NULL
      OR TRY_CAST(raw_amount AS DECIMAL(18, 2)) IS NULL
  );
```

## 13. Почему нельзя сразу строить mart из landing

Landing часто содержит:

- строки вместо типов;
- неверные timestamps;
- пустые ids;
- дубли;
- разные source formats;
- непредсказуемые file names;
- late arriving files.

Если строить marts напрямую из landing, каждую витрину придется учить заново парсить грязь. Правильный путь:

```text
landing -> raw typed parquet -> refined clean parquet -> mart
```

## 14. Refined слой: очистка и дедупликация

DDL:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS refined.events_clean (
    event_id STRING,
    user_id STRING,
    event_time TIMESTAMP,
    event_type STRING,
    amount DECIMAL(18, 2),
    ingestion_ts TIMESTAMP,
    source_system STRING
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/refined/events_clean';
```

Дедупликация должна быть детерминированной:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE refined.events_clean
PARTITION (event_date)
WITH source_events AS (
    SELECT
        event_id,
        user_id,
        event_time,
        event_type,
        amount,
        ingestion_ts,
        source_system,
        event_date
    FROM raw.events
    WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
),
ranked AS (
    SELECT
        source_events.*,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ingestion_ts DESC NULLS LAST, source_system ASC
        ) AS rn
    FROM source_events
)
SELECT
    event_id,
    user_id,
    event_time,
    event_type,
    amount,
    ingestion_ts,
    source_system,
    event_date
FROM ranked
WHERE rn = 1;
```

Не делай так:

```sql
SELECT DISTINCT *
FROM raw.events;
```

`DISTINCT` не объясняет, почему появились дубли и какую запись нужно считать правильной.

## 15. Mart слой: готовая витрина

DDL:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS mart.country_daily_revenue (
    country STRING,
    purchases BIGINT,
    gross_revenue DECIMAL(18, 2),
    refund_amount DECIMAL(18, 2),
    net_revenue DECIMAL(18, 2),
    processed_at TIMESTAMP
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/mart/country_daily_revenue';
```

Предположим, есть dimension:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS refined.users_current (
    user_id STRING,
    country STRING,
    segment STRING,
    updated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/refined/users_current';
```

Build mart:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.country_daily_revenue
PARTITION (event_date)
WITH events AS (
    SELECT
        event_date,
        user_id,
        event_type,
        amount
    FROM refined.events_clean
    WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
),
users AS (
    SELECT
        user_id,
        country
    FROM refined.users_current
),
enriched AS (
    SELECT
        e.event_date,
        COALESCE(u.country, 'UNKNOWN') AS country,
        e.event_type,
        e.amount
    FROM events e
    LEFT JOIN users u
        ON e.user_id = u.user_id
)
SELECT
    country,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    SUM(amount) FILTER (WHERE event_type = 'purchase') AS gross_revenue,
    SUM(amount) FILTER (WHERE event_type = 'refund') AS refund_amount,
    SUM(amount) AS net_revenue,
    CURRENT_TIMESTAMP() AS processed_at,
    event_date
FROM enriched
GROUP BY event_date, country;
```

Важно: partition column `event_date` стоит последней в `SELECT`, потому что используется dynamic partition insert.

## 16. Стиль production Spark SQL

Правила:

- Пиши CTE по стадиям: `source_filtered`, `deduped`, `enriched`, `aggregated`, `final`.
- Не используй `SELECT *` в production write.
- Указывай явные aliases.
- Квалифицируй колонки при joins.
- Используй typed literals: `DATE '2026-05-05'`.
- Не смешивай parsing, join, aggregation и formatting в одном огромном `SELECT`.
- Делай grain каждой CTE понятным.

Хорошо:

```sql
WITH source_filtered AS (
    SELECT
        event_date,
        user_id,
        event_id,
        event_type,
        amount
    FROM refined.events_clean
    WHERE event_date = DATE '2026-05-05'
),
aggregated AS (
    SELECT
        event_date,
        user_id,
        COUNT(*) AS event_count,
        SUM(amount) AS total_amount
    FROM source_filtered
    GROUP BY event_date, user_id
)
SELECT
    event_date,
    user_id,
    event_count,
    total_amount
FROM aggregated;
```

## 17. Joins: правила, которые спасают production

Базовые правила:

- Всегда указывай join type.
- Не используй `RIGHT JOIN`, поменяй стороны и сделай `LEFT JOIN`.
- Перед join проверь grain обеих таблиц.
- Перед join проверь uniqueness на dimension side.
- Фильтруй и проектируй большие таблицы до join.
- Не добавляй `DISTINCT` после join explosion.
- Используй `LEFT SEMI JOIN` для existence.
- Используй `LEFT ANTI JOIN` для exclusion.

Проверка dimension uniqueness:

```sql
SELECT
    user_id,
    COUNT(*) AS rows
FROM refined.users_current
GROUP BY user_id
HAVING COUNT(*) > 1
LIMIT 100;
```

Если dimension не уникальна:

```sql
WITH users_one_row AS (
    SELECT
        user_id,
        country,
        segment
    FROM (
        SELECT
            u.*,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY updated_at DESC NULLS LAST
            ) AS rn
        FROM refined.users_current u
    ) x
    WHERE rn = 1
)
SELECT
    e.event_id,
    e.user_id,
    u.country
FROM refined.events_clean e
LEFT JOIN users_one_row u
    ON e.user_id = u.user_id
WHERE e.event_date = DATE '2026-05-05';
```

Existence:

```sql
SELECT
    e.event_id,
    e.user_id
FROM refined.events_clean e
LEFT SEMI JOIN mart.active_users u
    ON e.user_id = u.user_id
WHERE e.event_date = DATE '2026-05-05';
```

Exclusion:

```sql
SELECT
    e.event_id,
    e.user_id
FROM refined.events_clean e
LEFT ANTI JOIN mart.blocked_users b
    ON e.user_id = b.user_id
WHERE e.event_date = DATE '2026-05-05';
```

## 18. Broadcast join

Broadcast join полезен, когда маленькая таблица помещается в память executors.

```sql
SELECT /*+ BROADCAST(u) */
    e.event_date,
    e.event_id,
    e.user_id,
    u.country
FROM refined.events_clean e
JOIN refined.users_current u
    ON e.user_id = u.user_id
WHERE e.event_date = DATE '2026-05-05';
```

Используй broadcast только если:

- dimension реально маленькая;
- statistics не врут;
- таблица не вырастет внезапно;
- executors имеют memory headroom.

Не broadcast'и таблицу неизвестного размера.

## 19. Sort-merge join

Если обе таблицы большие, Spark часто выберет `SortMergeJoin`.

Для large-large join:

- фильтруй обе стороны по partition columns;
- оставляй только нужные колонки;
- проверь типы join keys;
- проверь skew;
- подумай, можно ли агрегировать до join;
- проверь physical plan.

Пример:

```sql
WITH events AS (
    SELECT
        event_date,
        user_id,
        event_id
    FROM refined.events_clean
    WHERE event_date = DATE '2026-05-05'
),
orders AS (
    SELECT
        order_date,
        user_id,
        order_id,
        amount
    FROM refined.orders_clean
    WHERE order_date = DATE '2026-05-05'
)
SELECT
    e.user_id,
    COUNT(DISTINCT e.event_id) AS events,
    COUNT(DISTINCT o.order_id) AS orders,
    SUM(o.amount) AS order_amount
FROM events e
JOIN orders o
    ON e.user_id = o.user_id
GROUP BY e.user_id;
```

Осторожно: это может быть many-to-many join. Перед таким join нужно понимать cardinality.

## 20. Skew: как понять и что делать

Признаки skew:

- несколько tasks работают намного дольше;
- несколько tasks читают гигабайты shuffle, остальные мегабайты;
- один ключ занимает огромную долю данных.

Диагностика:

```sql
SELECT
    user_id,
    COUNT(*) AS rows
FROM refined.events_clean
WHERE event_date = DATE '2026-05-05'
GROUP BY user_id
ORDER BY rows DESC
LIMIT 50;
```

Что делать по порядку:

1. Отфильтровать лишние строки до join.
2. Спроецировать только нужные колонки.
3. Агрегировать до join, если семантика позволяет.
4. Включить AQE skew join.
5. Разделить hot keys и normal keys.
6. Использовать salting только если можешь доказать корректность.

Split hot keys:

```sql
WITH hot_keys AS (
    SELECT user_id
    FROM refined.events_clean
    WHERE event_date = DATE '2026-05-05'
    GROUP BY user_id
    HAVING COUNT(*) > 100000000
),
normal_events AS (
    SELECT e.*
    FROM refined.events_clean e
    LEFT ANTI JOIN hot_keys h
        ON e.user_id = h.user_id
    WHERE e.event_date = DATE '2026-05-05'
),
hot_events AS (
    SELECT e.*
    FROM refined.events_clean e
    INNER JOIN hot_keys h
        ON e.user_id = h.user_id
    WHERE e.event_date = DATE '2026-05-05'
)
SELECT COUNT(*) FROM normal_events
UNION ALL
SELECT COUNT(*) FROM hot_events;
```

## 21. Aggregations

Правила:

- `COUNT(*)` считает строки.
- `COUNT(col)` не считает `NULL`.
- `GROUP BY` должен соответствовать output grain.
- Используй conditional aggregation вместо нескольких scans.
- Не делай global `ORDER BY` перед aggregation.
- `COUNT(DISTINCT high_cardinality_key)` дорогой.

Пример:

```sql
SELECT
    event_date,
    country,
    COUNT(*) AS events,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    SUM(amount) FILTER (WHERE event_type = 'purchase') AS gross_revenue,
    SUM(amount) FILTER (WHERE event_type = 'refund') AS refunds,
    SUM(amount) AS net_revenue
FROM mart.events_enriched
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date, country;
```

Несколько уровней агрегации за один проход:

```sql
SELECT
    event_date,
    country,
    source_system,
    SUM(amount) AS revenue,
    GROUPING_ID(event_date, country, source_system) AS grouping_id
FROM refined.events_clean
WHERE event_date = DATE '2026-05-05'
GROUP BY GROUPING SETS (
    (event_date, country, source_system),
    (event_date, country),
    (event_date)
);
```

## 22. Window functions

Window functions делают shuffle/sort, поэтому их нужно применять осознанно.

Правила:

- Указывай `PARTITION BY`.
- Для `ROW_NUMBER`, `RANK`, `FIRST_VALUE`, `LAST_VALUE` нужен deterministic `ORDER BY`.
- Добавляй tie-breaker.
- Для running totals задавай frame.
- Не делай window по raw PB-scale данным, если можно агрегировать раньше.

Latest row:

```sql
SELECT
    user_id,
    status,
    effective_from
FROM (
    SELECT
        user_id,
        status,
        effective_from,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY effective_from DESC NULLS LAST, ingestion_ts DESC NULLS LAST
        ) AS rn
    FROM refined.user_status_history
    WHERE snapshot_date <= DATE '2026-05-05'
) x
WHERE rn = 1;
```

Running total:

```sql
SUM(amount) OVER (
    PARTITION BY user_id
    ORDER BY event_time ASC NULLS LAST, event_id ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS running_amount
```

## 23. Static vs dynamic partition overwrite

Static overwrite:

```sql
INSERT OVERWRITE TABLE mart.country_daily_revenue
PARTITION (event_date = DATE '2026-05-05')
SELECT
    country,
    purchases,
    gross_revenue,
    refund_amount,
    net_revenue,
    CURRENT_TIMESTAMP() AS processed_at
FROM tmp.country_daily_revenue_2026_05_05;
```

Dynamic overwrite:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.country_daily_revenue
PARTITION (event_date)
SELECT
    country,
    purchases,
    gross_revenue,
    refund_amount,
    net_revenue,
    CURRENT_TIMESTAMP() AS processed_at,
    event_date
FROM tmp.country_daily_revenue_window
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05';
```

Dynamic overwrite безопасен только если result содержит ровно те партиции, которые нужно заменить.

Перед write:

- проверь source range;
- проверь target table;
- проверь column order;
- проверь partition columns;
- проверь write mode;
- подготовь validation queries.

## 24. Post-write validation

После write нельзя ограничиваться “Spark job succeeded”.

Проверяй row counts:

```sql
SELECT
    event_date,
    COUNT(*) AS rows
FROM mart.country_daily_revenue
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date
ORDER BY event_date;
```

Проверяй duplicate grain:

```sql
SELECT
    event_date,
    country,
    COUNT(*) AS rows
FROM mart.country_daily_revenue
WHERE event_date = DATE '2026-05-05'
GROUP BY event_date, country
HAVING COUNT(*) > 1;
```

Проверяй reconciliation:

```sql
WITH source AS (
    SELECT
        event_date,
        SUM(amount) AS source_net_revenue
    FROM refined.events_clean
    WHERE event_date = DATE '2026-05-05'
    GROUP BY event_date
),
target AS (
    SELECT
        event_date,
        SUM(net_revenue) AS target_net_revenue
    FROM mart.country_daily_revenue
    WHERE event_date = DATE '2026-05-05'
    GROUP BY event_date
)
SELECT
    s.event_date,
    s.source_net_revenue,
    t.target_net_revenue,
    s.source_net_revenue - t.target_net_revenue AS diff
FROM source s
JOIN target t
    ON s.event_date = t.event_date;
```

## 25. Backfill

Backfill - это production change, а не “просто прогнать за год”.

Правила:

- используй тот же SQL path, что и daily job;
- обрабатывай явные ranges;
- дели большие ranges на batches;
- не пиши одновременно в одни и те же partitions;
- валидируй каждый batch;
- сохраняй старые данные до конца проверки;
- логируй processed range.

Пример:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.country_daily_revenue
PARTITION (event_date)
SELECT
    country,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    SUM(amount) FILTER (WHERE event_type = 'purchase') AS gross_revenue,
    SUM(amount) FILTER (WHERE event_type = 'refund') AS refund_amount,
    SUM(amount) AS net_revenue,
    CURRENT_TIMESTAMP() AS processed_at,
    event_date
FROM refined.events_clean
WHERE event_date BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
GROUP BY event_date, country;
```

## 26. Hive partition metadata

Если Spark пишет через Hive table API, partition metadata часто обновляется автоматически.

Если файлы появились на HDFS вне Hive/Spark table write, нужно добавить partitions:

```sql
ALTER TABLE raw.events ADD IF NOT EXISTS
    PARTITION (event_date = '2026-05-05', source_system = 'web')
    LOCATION 'hdfs:///data/lake/raw/events/event_date=2026-05-05/source_system=web';
```

Bulk repair:

```sql
MSCK REPAIR TABLE raw.events;
REFRESH TABLE raw.events;
```

Но на больших таблицах `MSCK REPAIR TABLE` может быть тяжелым. Для известных ranges лучше targeted `ALTER TABLE ADD PARTITION`.

Drop partition metadata:

```sql
ALTER TABLE raw.events DROP IF EXISTS
    PARTITION (event_date = '2026-05-05', source_system = 'web');
```

Это не обязательно удаляет HDFS files. Metadata removal и physical delete - разные операции.

## 27. REFRESH TABLE и stale metadata

Spark session может держать устаревшие file listings.

Используй:

```sql
REFRESH TABLE raw.events;
```

Если таблица cached:

```sql
UNCACHE TABLE raw.events;
REFRESH TABLE raw.events;
```

Когда нужно:

- другой job перезаписал partitions;
- появились новые files;
- partitions добавили вне текущей Spark session;
- видишь `FileNotFoundException`;
- direct HDFS reality и Spark results расходятся.

Важно: `REFRESH TABLE` не добавляет partitions в metastore. Для этого нужен `ALTER TABLE ADD PARTITION` или `MSCK REPAIR TABLE`.

## 28. HDFS inspection команды

Проверить root:

```bash
hdfs dfs -ls /data/lake/raw/events
hdfs dfs -du -s -h /data/lake/raw/events
hdfs dfs -count -h /data/lake/raw/events
```

Проверить partition:

```bash
hdfs dfs -ls /data/lake/raw/events/event_date=2026-05-05/source_system=web
hdfs dfs -du -s -h /data/lake/raw/events/event_date=2026-05-05/source_system=web
hdfs dfs -count -h /data/lake/raw/events/event_date=2026-05-05/source_system=web
```

Проверить existence:

```bash
hdfs dfs -test -d /data/lake/raw/events/event_date=2026-05-05/source_system=web
```

Проверить ACL:

```bash
hdfs dfs -getfacl /data/lake/raw/events
hdfs dfs -getfacl /data/lake/raw/events/event_date=2026-05-05/source_system=web
```

Используй HDFS commands для диагностики, а не как основной pipeline API.

## 29. Direct path reads

Нормально:

```sql
SELECT *
FROM raw.events
WHERE event_date = DATE '2026-05-05';
```

Для диагностики:

```sql
SELECT *
FROM parquet.`hdfs:///data/lake/raw/events/event_date=2026-05-05/source_system=web`
LIMIT 10;
```

Direct path read:

- обходит Hive partition metadata;
- помогает понять, есть ли файлы физически;
- не должен становиться production business logic без причины.

## 30. Small files и compaction

Small files вредят:

- Spark planning;
- HDFS namenode;
- scan performance;
- scheduler overhead.

Ориентир file size:

- 128-512 MB для большинства Parquet tables;
- 512 MB-1 GB для очень больших sequential scans;
- меньше допустимо для низкообъемных partitions.

Диагностика:

```bash
hdfs dfs -count -h /data/lake/mart/country_daily_revenue/event_date=2026-05-05
hdfs dfs -du -h /data/lake/mart/country_daily_revenue/event_date=2026-05-05 | head
```

Compaction partition:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;
SET spark.sql.shuffle.partitions = 200;

INSERT OVERWRITE TABLE mart.country_daily_revenue
PARTITION (event_date)
SELECT
    country,
    purchases,
    gross_revenue,
    refund_amount,
    net_revenue,
    processed_at,
    event_date
FROM mart.country_daily_revenue
WHERE event_date = DATE '2026-05-05';
```

Не делай `coalesce(1)`-аналог ради одного файла на большой партиции.

## 31. Statistics

Optimizer может принимать плохие решения без statistics.

Собрать table stats:

```sql
ANALYZE TABLE raw.events COMPUTE STATISTICS;
```

Column stats:

```sql
ANALYZE TABLE raw.events COMPUTE STATISTICS FOR COLUMNS event_date, user_id, source_system;
```

Когда обновлять:

- после больших backfills;
- после massive overwrite;
- после compaction;
- после изменения распределения keys;
- если Spark выбирает странный join strategy.

Проверить:

```sql
DESCRIBE EXTENDED raw.events;
DESCRIBE EXTENDED raw.events user_id;
```

## 32. EXPLAIN FORMATTED

Перед дорогим запросом:

```sql
EXPLAIN FORMATTED
SELECT
    user_id,
    SUM(amount) AS revenue
FROM refined.events_clean
WHERE event_date = DATE '2026-05-05'
GROUP BY user_id;
```

Ищи:

- `FileScan parquet`;
- `PartitionFilters`;
- `PushedFilters`;
- `ReadSchema`;
- `Exchange`;
- `BroadcastHashJoin`;
- `SortMergeJoin`;
- `HashAggregate`;
- `Window`;
- `CartesianProduct`.

Хорошо:

```text
PartitionFilters: [isnotnull(event_date), (event_date = 2026-05-05)]
ReadSchema: struct<user_id:string,amount:decimal(18,2)>
```

Плохо:

```text
PartitionFilters: []
ReadSchema: struct<all columns...>
```

Если нет partition filters на большой таблице, сначала исправь SQL, потом думай о ресурсах.

## 33. Null semantics

В SQL `NULL = NULL` не true.

Правила:

- `COUNT(col)` не считает null.
- `COUNT(*)` считает строки.
- `SUM` по всем null может вернуть null.
- `NOT IN` с null в subquery может вести себя неожиданно.
- После `LEFT JOIN` фильтр по right-side column в `WHERE` может превратить left join в inner join.

Осторожно:

```sql
SELECT e.*
FROM refined.events_clean e
LEFT JOIN refined.users_current u
    ON e.user_id = u.user_id
WHERE u.country = 'US';
```

Если нужно сохранить unmatched:

```sql
WHERE u.country = 'US'
   OR u.user_id IS NULL
```

Если нужна null-safe equality:

```sql
ON a.key <=> b.key
```

Используй `<=>` только если бизнес-семантика действительно требует `NULL = NULL`.

## 34. Time и timezone

Разделяй:

- `event_time`: когда событие произошло;
- `ingestion_ts`: когда попало в lake;
- `processed_at`: когда обработано pipeline;
- `event_date`: partition/business date.

Используй half-open intervals:

```sql
WHERE event_time >= TIMESTAMP '2026-05-05 00:00:00'
  AND event_time <  TIMESTAMP '2026-05-06 00:00:00'
```

Не делай:

```sql
WHERE event_time BETWEEN TIMESTAMP '2026-05-05 00:00:00'
                     AND TIMESTAMP '2026-05-05 23:59:59'
```

Проблемы:

- microseconds/nanoseconds;
- timezone;
- daylight saving;
- inconsistent parsing.

## 35. Schema evolution

Безопаснее:

- добавить nullable column;
- добавить derived metric с downstream approval;
- расширить тип, если readers поддерживают.

Опасно:

- rename column;
- drop column;
- сменить partition columns;
- изменить physical type в Parquet;
- `STRING` -> `TIMESTAMP` без validation;
- `DOUBLE` -> `DECIMAL` без reconciliation.

Add column:

```sql
ALTER TABLE mart.country_daily_revenue ADD COLUMNS (
    average_order_value DECIMAL(18, 2)
);
```

После:

```sql
REFRESH TABLE mart.country_daily_revenue;
DESCRIBE FORMATTED mart.country_daily_revenue;
SELECT *
FROM mart.country_daily_revenue
WHERE event_date = DATE '2026-05-05'
LIMIT 10;
```

Не предполагай, что все engines одинаково хорошо читают mixed Parquet schemas.

## 36. Security и governance

Проверяй:

- кто owner таблицы;
- какой service user пишет;
- HDFS ACLs;
- Ranger/Sentry grants;
- PII classification;
- retention;
- audit requirements.

HDFS ACL:

```bash
hdfs dfs -getfacl /data/lake/raw/events
hdfs dfs -getfacl /data/lake/mart/country_daily_revenue
```

Правила:

- не копируй raw PII в mart без нужды;
- quarantine может содержать чувствительные значения;
- tmp и sandbox должны иметь TTL;
- secrets не должны попадать в SQL, logs, table properties.

## 37. Observability

Production pipeline должен логировать:

- job name;
- batch id;
- processing range;
- source table;
- target table;
- input rows;
- output rows;
- invalid rows;
- touched partitions;
- Spark application id;
- status;
- started_at/finished_at.

Audit table:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS mart.pipeline_run_audit (
    job_name STRING,
    batch_id STRING,
    source_table STRING,
    target_table STRING,
    process_start_date DATE,
    process_end_date DATE,
    input_rows BIGINT,
    output_rows BIGINT,
    invalid_rows BIGINT,
    spark_application_id STRING,
    status STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
)
PARTITIONED BY (run_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/mart/pipeline_run_audit';
```

## 38. Типовые failure modes

### Hive видит partition, HDFS файлов нет

Симптомы:

- `SHOW PARTITIONS` показывает partition;
- query возвращает ноль строк или падает;
- HDFS path пустой или отсутствует.

Проверка:

```sql
DESCRIBE EXTENDED raw.events PARTITION (event_date = '2026-05-05', source_system = 'web');
```

```bash
hdfs dfs -ls /data/lake/raw/events/event_date=2026-05-05/source_system=web
```

Решения:

- восстановить файлы;
- изменить partition location;
- удалить metadata partition;
- переписать partition.

### Файлы есть, Hive partition не видит

Проверка:

```bash
hdfs dfs -ls /data/lake/raw/events/event_date=2026-05-05/source_system=web
```

Fix:

```sql
ALTER TABLE raw.events ADD IF NOT EXISTS
    PARTITION (event_date = '2026-05-05', source_system = 'web')
    LOCATION 'hdfs:///data/lake/raw/events/event_date=2026-05-05/source_system=web';

REFRESH TABLE raw.events;
```

### Stale Spark file listing

Fix:

```sql
UNCACHE TABLE raw.events;
REFRESH TABLE raw.events;
```

### Small-file incident

Fix:

- compact affected partitions;
- tune shuffle partitions;
- включить AQE coalesce;
- изменить upstream write layout;
- не писать micro-batches напрямую в analytical table без compaction.

### Schema mismatch

Fix:

- найти affected partitions;
- переписать partitions с consistent schema;
- временно сделать compatibility view с casts, если безопасно;
- не включать schema merge вслепую на critical pipelines.

## 39. End-to-end мини-проект

Задача: построить lake pipeline для событий сайта.

Слои:

```text
landing.web_events_csv
raw.web_events
refined.web_events_clean
mart.country_daily_revenue
quarantine.web_events_invalid
```

Шаг 1. Создать landing:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS landing.web_events_csv (
    raw_event_id STRING,
    raw_user_id STRING,
    raw_event_time STRING,
    raw_event_type STRING,
    raw_amount STRING,
    raw_country STRING
)
PARTITIONED BY (ingestion_date DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///data/lake/landing/web_events_csv';
```

Шаг 2. Создать raw:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS raw.web_events (
    event_id STRING,
    user_id STRING,
    event_time TIMESTAMP,
    event_type STRING,
    amount DECIMAL(18, 2),
    country STRING,
    ingestion_ts TIMESTAMP,
    source_file STRING
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/raw/web_events';
```

Шаг 3. Создать quarantine:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS quarantine.web_events_invalid (
    raw_event_id STRING,
    raw_user_id STRING,
    raw_event_time STRING,
    raw_event_type STRING,
    raw_amount STRING,
    raw_country STRING,
    invalid_reason STRING,
    quarantined_at TIMESTAMP
)
PARTITIONED BY (ingestion_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/quarantine/web_events_invalid';
```

Шаг 4. Загрузить raw:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE raw.web_events
PARTITION (event_date)
SELECT
    raw_event_id AS event_id,
    raw_user_id AS user_id,
    TRY_CAST(raw_event_time AS TIMESTAMP) AS event_time,
    raw_event_type AS event_type,
    TRY_CAST(raw_amount AS DECIMAL(18, 2)) AS amount,
    raw_country AS country,
    CURRENT_TIMESTAMP() AS ingestion_ts,
    INPUT_FILE_NAME() AS source_file,
    TO_DATE(TRY_CAST(raw_event_time AS TIMESTAMP)) AS event_date
FROM landing.web_events_csv
WHERE ingestion_date = DATE '2026-05-05'
  AND raw_event_id IS NOT NULL
  AND TRY_CAST(raw_event_time AS TIMESTAMP) IS NOT NULL;
```

Шаг 5. Создать refined:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS refined.web_events_clean (
    event_id STRING,
    user_id STRING,
    event_time TIMESTAMP,
    event_type STRING,
    amount DECIMAL(18, 2),
    country STRING,
    ingestion_ts TIMESTAMP
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/refined/web_events_clean';
```

Шаг 6. Дедуплицировать:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE refined.web_events_clean
PARTITION (event_date)
WITH ranked AS (
    SELECT
        event_id,
        user_id,
        event_time,
        event_type,
        amount,
        country,
        ingestion_ts,
        event_date,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ingestion_ts DESC NULLS LAST
        ) AS rn
    FROM raw.web_events
    WHERE event_date = DATE '2026-05-05'
)
SELECT
    event_id,
    user_id,
    event_time,
    event_type,
    amount,
    country,
    ingestion_ts,
    event_date
FROM ranked
WHERE rn = 1;
```

Шаг 7. Создать mart:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS mart.country_daily_revenue (
    country STRING,
    purchases BIGINT,
    revenue DECIMAL(18, 2),
    processed_at TIMESTAMP
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/lake/mart/country_daily_revenue';
```

Шаг 8. Построить mart:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.country_daily_revenue
PARTITION (event_date)
SELECT
    country,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    SUM(amount) FILTER (WHERE event_type = 'purchase') AS revenue,
    CURRENT_TIMESTAMP() AS processed_at,
    event_date
FROM refined.web_events_clean
WHERE event_date = DATE '2026-05-05'
GROUP BY event_date, country;
```

Шаг 9. Validate:

```sql
SELECT event_date, COUNT(*) AS rows
FROM raw.web_events
WHERE event_date = DATE '2026-05-05'
GROUP BY event_date;

SELECT event_date, COUNT(*) AS rows
FROM refined.web_events_clean
WHERE event_date = DATE '2026-05-05'
GROUP BY event_date;

SELECT event_date, COUNT(*) AS rows, SUM(revenue) AS revenue
FROM mart.country_daily_revenue
WHERE event_date = DATE '2026-05-05'
GROUP BY event_date;
```

Шаг 10. Inspect HDFS:

```bash
hdfs dfs -count -h /data/lake/raw/web_events/event_date=2026-05-05
hdfs dfs -count -h /data/lake/refined/web_events_clean/event_date=2026-05-05
hdfs dfs -count -h /data/lake/mart/country_daily_revenue/event_date=2026-05-05
```

## 40. Production checklist перед merge/deploy

Проверь:

- Таблицы external/managed выбраны намеренно.
- HDFS locations явные и корректные.
- Partition columns соответствуют query patterns.
- Нет high-cardinality partitions.
- Все production writes без `SELECT *`.
- Все large reads имеют partition filters.
- Dynamic overwrite ограничен правильным range.
- Partition columns есть в output.
- Joins имеют явный type.
- Dimensions уникальны или дедуплицированы.
- Нет `DISTINCT` для скрытия join explosion.
- Windows deterministic.
- Null behavior продуман.
- Timezone/date derivation продумана.
- Есть post-write validation.
- Есть HDFS/file count inspection для новых таблиц.
- Есть statistics refresh policy.
- Есть recovery plan.
- Есть owner, ACLs и retention.

## 41. Anti-patterns

Не делай:

- full scan большой Hive table без partition filter;
- full overwrite для daily pipeline;
- `SELECT *` в production insert;
- `MSCK REPAIR TABLE` ежедневно на огромной таблице без необходимости;
- `DISTINCT` после join без анализа;
- global `ORDER BY` на PB-scale данных;
- partition by `user_id`;
- миллионы small files;
- direct path reads как основной production contract;
- ручной HDFS delete как часть нормального pipeline;
- schema change без downstream review;
- broadcast hint на таблицу неизвестного размера;
- хранить money в `DOUBLE`;
- строить mart напрямую из landing CSV;
- доверять только факту успешного Spark job.

## 42. Как думать при оптимизации

Порядок:

1. Исправь семантику: grain, keys, nulls, duplicates.
2. Ограничь scan: partition pruning, predicate pushdown, column pruning.
3. Уменьши данные до shuffle: filters, projection, pre-aggregation.
4. Исправь join strategy: uniqueness, broadcast, semi/anti join.
5. Обработай skew: AQE, split hot keys, salting.
6. Настрой shuffle partitions.
7. Исправь file layout и small files.
8. Обнови statistics.
9. Только потом увеличивай cluster resources.

## 43. Быстрая шпаргалка команд

Hive metadata:

```sql
SHOW CREATE TABLE raw.events;
DESCRIBE FORMATTED raw.events;
SHOW TBLPROPERTIES raw.events;
SHOW COLUMNS IN raw.events;
SHOW PARTITIONS raw.events;
DESCRIBE EXTENDED raw.events PARTITION (event_date = '2026-05-05');
```

Partition add:

```sql
ALTER TABLE raw.events ADD IF NOT EXISTS
    PARTITION (event_date = '2026-05-05')
    LOCATION 'hdfs:///data/lake/raw/events/event_date=2026-05-05';
```

Repair:

```sql
MSCK REPAIR TABLE raw.events;
REFRESH TABLE raw.events;
```

Stats:

```sql
ANALYZE TABLE raw.events COMPUTE STATISTICS;
ANALYZE TABLE raw.events COMPUTE STATISTICS FOR COLUMNS event_date, user_id;
```

Explain:

```sql
EXPLAIN FORMATTED
SELECT ...
```

HDFS:

```bash
hdfs dfs -ls /data/lake/raw/events
hdfs dfs -du -s -h /data/lake/raw/events
hdfs dfs -count -h /data/lake/raw/events/event_date=2026-05-05
hdfs dfs -getfacl /data/lake/raw/events
```

## 44. Главная идея

Spark SQL на HDFS/Hive становится надежным, когда разработчик одновременно держит в голове пять вещей:

1. Что означает строка данных с точки зрения бизнеса.
2. Как таблица описана в Hive Metastore.
3. Как файлы физически лежат на HDFS.
4. Как Spark построит physical plan.
5. Как write можно безопасно повторить, проверить и откатить.

Если одна из этих частей игнорируется, появляются типовые production-инциденты: полный scan вместо partition pruning, join explosion, потеря партиций, stale metadata, small files, неверные витрины, дорогие backfills и поломанные downstream consumers.

Хороший Spark SQL developer на HDFS/Hive пишет не просто SQL. Он проектирует data contract, file layout, metadata lifecycle, safe write strategy и validation path.

