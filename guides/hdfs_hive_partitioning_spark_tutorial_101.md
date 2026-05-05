# HDFS, Hive и Partitioning для Spark: Tutorial 101

Это подробный tutorial для разработчика, который работает со Spark поверх HDFS и Hive. Документ объясняет, как устроены HDFS, Hive Metastore, partitioned tables, Parquet-файлы, file layout, чтение и запись через Spark SQL, а также почему размер файлов, HDFS blocks, Parquet row groups и количество partitions напрямую влияют на производительность.

Фокус документа:

- HDFS как storage layer для Spark.
- Hive Metastore как metadata/catalog layer.
- Hive partitioning и физический layout на HDFS.
- Parquet как основной columnar формат.
- Spark SQL как compute/query engine.
- Best practices для enterprise-проектов с TB-PB объемами.

Документ написан как Tutorial 101: он объясняет базовые идеи, но не упрощает production-реальность.

## 1. Главная ментальная модель

Когда Spark читает таблицу Hive, он работает сразу с несколькими слоями:

```text
SQL query
  -> Spark SQL analyzer / optimizer
  -> Hive Metastore metadata
  -> HDFS directory and file listing
  -> Parquet file metadata
  -> HDFS blocks on DataNodes
  -> Spark tasks on executors
```

Важнейшая идея:

```text
Hive table != HDFS files
Hive partition != просто директория
Spark partition != Hive partition
Parquet row group != HDFS block
HDFS block != Spark task, хотя они связаны через file splits
```

Если разработчик путает эти уровни, появляются типичные production-проблемы:

- Spark читает всю таблицу вместо одной даты.
- Hive показывает partition, но файлов на HDFS нет.
- Файлы на HDFS есть, но Hive их не видит.
- Один partition содержит миллион tiny files.
- Parquet-файлы слишком маленькие или слишком большие.
- Broadcast join падает по memory.
- Backfill случайно перезаписывает не те partitions.
- `MSCK REPAIR TABLE` нагружает metastore и namenode.

## 2. Что такое HDFS

HDFS, Hadoop Distributed File System, это распределенная файловая система для больших файлов и batch/streaming reads.

HDFS оптимизирован под:

- большие файлы;
- последовательное чтение;
- write once, read many;
- отказоустойчивость через replication;
- перенос вычислений к данным;
- работу на кластере commodity machines.

HDFS не оптимизирован под:

- миллионы маленьких файлов;
- частые random updates;
- низколатентные point lookups;
- большое количество metadata operations в tight loop;
- частые append/update сценарии как в OLTP database.

## 3. Архитектура HDFS

Классическая HDFS-архитектура:

```text
NameNode
  хранит namespace metadata:
    directories
    files
    permissions
    block mapping
    replication info

DataNodes
  хранят физические blocks
  обслуживают read/write requests
  отправляют heartbeats и block reports

Client
  обращается к NameNode за metadata
  читает/пишет данные напрямую с DataNodes
```

Важно: user data не течет через NameNode. NameNode отвечает за metadata и координацию. DataNodes хранят и отдают bytes.

## 4. NameNode

NameNode хранит namespace HDFS:

- список директорий;
- список файлов;
- владельцев и permissions;
- mapping file -> blocks;
- mapping blocks -> DataNodes;
- replication factor;
- snapshots metadata;
- quotas;
- ACL metadata.

NameNode держит metadata в памяти. Поэтому большое количество файлов и директорий - это нагрузка на память NameNode.

Почему small files опасны:

```text
1 файл 512 MB
  -> 1 inode
  -> несколько HDFS blocks
  -> мало metadata overhead

1 000 000 файлов по 50 KB
  -> 1 000 000 inodes
  -> 1 000 000+ block records
  -> огромный metadata overhead
  -> медленные listings
  -> тяжелый Spark planning
```

Практический вывод: HDFS любит большие файлы. Data lake на HDFS должен контролировать количество файлов.

## 5. DataNode

DataNode:

- хранит HDFS blocks на локальных дисках;
- обслуживает read requests;
- обслуживает write pipeline;
- отправляет NameNode heartbeat;
- отправляет block reports;
- реплицирует blocks по команде NameNode;
- удаляет blocks по команде NameNode.

Для Spark это важно, потому что Spark executors часто запускаются на тех же worker nodes. Если executor читает block с локального DataNode, это data locality. Если нет, чтение идет по сети.

На современных кластерах locality все еще полезна, но не всегда решающая: network bandwidth, scheduler behavior, YARN/Kubernetes placement и cache effects могут менять картину.

## 6. HDFS blocks

Файл в HDFS разбивается на blocks.

Типичный block size:

```text
128 MB
256 MB
512 MB
1 GB
```

Исторически часто встречался 128 MB. Для больших аналитических Parquet workloads иногда используют 256 MB, 512 MB или 1 GB, особенно если Parquet row groups тоже крупные.

Пример:

```text
file: events_0001.parquet, size 900 MB
HDFS block size: 256 MB

blocks:
  block 1: 256 MB
  block 2: 256 MB
  block 3: 256 MB
  block 4: 132 MB
```

Каждый block имеет replicas, обычно 3.

## 7. Replication factor

Replication factor определяет, сколько копий block хранится на DataNodes.

Пример:

```text
file size: 1 TB
replication factor: 3
physical disk consumption: примерно 3 TB
```

Почему replication важен:

- отказоустойчивость;
- чтение с ближайшей replica;
- восстановление после DataNode failures;
- балансировка нагрузки.

Но replication увеличивает storage cost. Для cold data на некоторых платформах могут использовать erasure coding, но это отдельная тема и зависит от кластера.

Проверить replication:

```bash
hdfs dfs -stat '%r %n' /data/lake/raw/events/event_date=2026-05-05/part-00000.parquet
```

Изменить replication:

```bash
hdfs dfs -setrep -w 3 /data/lake/raw/events/event_date=2026-05-05
```

Не меняй replication production paths без платформенной политики.

## 8. Как HDFS читает файл

Упрощенно:

1. Client спрашивает NameNode: где blocks файла?
2. NameNode возвращает block locations.
3. Client выбирает ближайшие DataNodes.
4. Client читает bytes напрямую с DataNodes.
5. Если block replica недоступна, client пробует другую replica.

Spark использует это через Hadoop InputFormat / FileSystem API. Spark строит file scan, получает files, splits, metadata, а затем создает tasks.

## 9. Как HDFS пишет файл

Упрощенно:

1. Client просит NameNode создать файл.
2. NameNode выбирает DataNodes для replicas.
3. Client пишет в первый DataNode.
4. Первый DataNode передает bytes второму.
5. Второй передает третьему.
6. После подтверждений block считается записанным.

Обычно HDFS-файлы имеют модель write-once. Это хорошо сочетается с data lake: мы пишем новые files/partitions, а не обновляем строки внутри файла.

## 10. Почему HDFS плохо переносит small files

Small files создают проблемы на трех уровнях:

1. NameNode metadata:
   много files/directories -> много inodes и block records.

2. Spark planning:
   нужно listing огромного количества files;
   нужно создать file scan metadata;
   scheduler видит много маленьких input splits.

3. Execution:
   open/close cost становится сравнимым с чтением данных;
   tasks становятся слишком мелкими;
   появляется scheduler overhead.

Пример плохого layout:

```text
event_date=2026-05-05/
  1 200 000 файлов по 20 KB
```

Это может быть всего 24 GB данных, но job будет тяжелым из-за metadata и file open overhead.

Хороший layout:

```text
event_date=2026-05-05/
  100 файлов по 256 MB
```

## 11. Базовые HDFS команды для Spark-разработчика

Проверить директорию:

```bash
hdfs dfs -ls /data/lake/raw/events
```

Проверить partition:

```bash
hdfs dfs -ls /data/lake/raw/events/event_date=2026-05-05
```

Посчитать размер:

```bash
hdfs dfs -du -s -h /data/lake/raw/events/event_date=2026-05-05
```

Посчитать files/directories:

```bash
hdfs dfs -count -h /data/lake/raw/events/event_date=2026-05-05
```

Проверить existence:

```bash
hdfs dfs -test -d /data/lake/raw/events/event_date=2026-05-05
```

Посмотреть ACL:

```bash
hdfs dfs -getfacl /data/lake/raw/events
```

Проверить место:

```bash
hdfs dfs -df -h /data/lake
```

Важное правило: HDFS commands используй для диагностики и эксплуатации, но production pipeline должен писать через Spark/Hive table APIs, если данные являются Hive table.

## 12. Hive Metastore

Hive Metastore хранит metadata:

- databases;
- tables;
- schemas;
- partition definitions;
- table locations;
- partition locations;
- table properties;
- statistics;
- owner/grants, если интегрировано с security layer.

Spark SQL использует Hive Metastore как catalog:

```sql
SELECT *
FROM raw.events
WHERE event_date = DATE '2026-05-05';
```

Spark не угадывает сам, где лежит таблица. Он берет `LOCATION` из metastore.

## 13. Managed vs External Hive tables

Hive знает два базовых типа таблиц:

- managed/internal;
- external.

Managed table:

- Hive считает, что владеет данными;
- drop/truncate может удалить files;
- lifecycle управляется каталогом;
- нужно быть особенно осторожным с destructive operations.

External table:

- Hive управляет metadata;
- data лежит в явном `LOCATION`;
- drop external table обычно удаляет metadata, но не files;
- хорошо подходит для data lake на HDFS.

Проверить тип:

```sql
DESCRIBE FORMATTED raw.events;
DESCRIBE EXTENDED raw.events;
```

Production правило: перед `DROP`, `TRUNCATE`, `INSERT OVERWRITE`, `ALTER TABLE DROP PARTITION` всегда понимай, managed это table или external.

## 14. Hive database layout

Для lake удобно разделить зоны:

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

Слои:

- `landing`: как пришло из источника;
- `raw`: typed Parquet, минимальные изменения;
- `refined`: cleaned/conformed/deduplicated;
- `mart`: готовые витрины;
- `quarantine`: плохие записи;
- `tmp`: временные outputs.

## 15. Hive external Parquet table

Пример:

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

Обрати внимание:

- `event_date` и `source_system` не перечислены в main columns, они идут в `PARTITIONED BY`.
- `LOCATION` явный.
- Формат Parquet.
- Есть lineage fields: `source_file`, `batch_id`, `ingestion_ts`.

## 16. Partitioning в Hive

Hive partition - это metadata entry, обычно соответствующая HDFS-директории:

```text
hdfs:///data/lake/raw/events/
  event_date=2026-05-05/
    source_system=web/
      part-00000.snappy.parquet
```

В Hive:

```sql
SHOW PARTITIONS raw.events;
```

Может показать:

```text
event_date=2026-05-05/source_system=web
event_date=2026-05-05/source_system=mobile
event_date=2026-05-06/source_system=web
```

Важно: физическая директория может существовать, но Hive может не знать partition. Или наоборот: Hive знает partition, но HDFS path удален.

## 17. Как выбрать partition columns

Выбирай partition columns по query patterns.

Хорошо:

- `event_date`, если почти все запросы фильтруют по датам;
- `ingestion_date`, если данные обрабатываются по дате загрузки;
- `snapshot_date`, если таблица snapshot-based;
- `country`, `source_system`, `tenant`, если cardinality контролируемая и фильтры частые.

Плохо:

- `user_id`;
- `event_id`;
- `uuid`;
- `request_id`;
- `order_id`;
- любое поле с миллионами значений.

Почему high-cardinality partition плохо:

```text
partition by user_id
  -> миллионы directories
  -> миллионы partition entries в metastore
  -> tiny files
  -> дорогой listing
  -> медленный planning
  -> тяжелый metastore
```

## 18. Partition pruning

Partition pruning - это когда Spark отбрасывает ненужные partitions еще до чтения файлов.

Хорошо:

```sql
SELECT
    event_date,
    COUNT(*) AS rows
FROM raw.events
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date;
```

Плохо:

```sql
SELECT COUNT(*)
FROM raw.events
WHERE TO_DATE(event_time) = DATE '2026-05-05';
```

Если table partitioned by `event_date`, фильтр должен быть по `event_date`.

Если нужна точная timestamp-логика:

```sql
WHERE event_date = DATE '2026-05-05'
  AND event_time >= TIMESTAMP '2026-05-05 00:00:00'
  AND event_time <  TIMESTAMP '2026-05-06 00:00:00'
```

Так ты получаешь и partition pruning, и корректный timestamp interval.

## 19. Почему количество partitions тоже важно

Слишком мало partitions:

- запросы часто читают лишние данные;
- daily jobs могут перезаписывать слишком большой объем;
- backfills становятся грубыми.

Слишком много partitions:

- metastore раздувается;
- `SHOW PARTITIONS`, `MSCK REPAIR`, planning становятся медленными;
- появляются tiny files;
- HDFS namespace нагружается.

Практический баланс:

- daily partition для больших fact/event tables почти всегда нормальный default;
- hourly partition только если объемы и query patterns это оправдывают;
- second-level partition по low-cardinality полю допустим, если реально используется;
- high-cardinality partition почти всегда ошибка.

## 20. Spark partition vs Hive partition

Hive partition:

```text
event_date=2026-05-05
```

Это logical/metadata partition таблицы.

Spark partition:

```text
task input partition
shuffle partition
RDD/DataFrame partition
```

Это execution unit.

Они связаны, но не равны.

Пример:

```text
1 Hive partition event_date=2026-05-05
  contains 100 Parquet files
  each 256 MB

Spark may create:
  ~200 input partitions
  then 4000 shuffle partitions after groupBy
```

Нельзя думать: “у меня 1 Hive partition, значит будет 1 Spark task”.

## 21. Parquet: что внутри файла

Parquet-файл содержит:

```text
File
  Row group 1
    Column chunk: column A
      pages
    Column chunk: column B
      pages
    Column chunk: column C
      pages
  Row group 2
    Column chunks...
  File metadata/footer
```

Ключевые понятия:

`row group`: горизонтальный блок строк внутри Parquet-файла.

`column chunk`: данные одной колонки внутри row group.

`page`: меньшая единица encoding/compression внутри column chunk.

`footer`: metadata в конце файла: schema, row groups, column chunk locations, statistics.

Почему это важно для Spark:

- Spark может читать только нужные колонки.
- Spark/Parquet может использовать statistics для skipping.
- Слишком маленькие row groups ухудшают sequential IO.
- Слишком большие row groups требуют больше memory/buffering.

## 22. Column pruning

Parquet хранит колонки отдельно. Если запрос читает 3 колонки из 100, Spark может читать только эти 3.

Хорошо:

```sql
SELECT
    user_id,
    amount
FROM raw.events
WHERE event_date = DATE '2026-05-05';
```

Плохо:

```sql
SELECT *
FROM raw.events
WHERE event_date = DATE '2026-05-05';
```

`SELECT *`:

- читает лишние колонки;
- увеличивает IO;
- увеличивает network;
- увеличивает memory pressure;
- делает pipelines хрупкими к schema changes.

Production правило: explicit column list.

## 23. Predicate pushdown

Predicate pushdown - это когда фильтр применяется ближе к file scan.

Пример:

```sql
SELECT
    user_id,
    amount
FROM raw.events
WHERE event_date = DATE '2026-05-05'
  AND event_type = 'purchase';
```

`event_date` может дать partition pruning.

`event_type = 'purchase'` может стать pushed filter на Parquet scan, если тип/выражение поддерживаются.

Pushdown могут ломать:

- UDF в `WHERE`;
- function wrapper вокруг колонки;
- сложные `OR`;
- casts в неправильную сторону;
- чтение JSON string вместо typed columns.

## 24. Размер Parquet-файла

На практике для аналитических Parquet tables часто целятся в:

```text
128 MB - 512 MB per file
```

Для очень больших sequential fact scans иногда:

```text
512 MB - 1 GB per file
```

Но размер файла не выбирается в вакууме. Он связан с:

- HDFS block size;
- Parquet row group size;
- Spark input split size;
- file open cost;
- parallelism;
- memory;
- query patterns;
- compression;
- downstream engines.

## 25. Почему слишком маленький Parquet-файл плохо

Например:

```text
1 partition = 50 GB
files = 500 000
average file = 100 KB
```

Проблемы:

- NameNode metadata overhead;
- медленный HDFS listing;
- Spark planning overhead;
- много file open operations;
- tasks слишком мелкие;
- scheduler overhead;
- плохая compression efficiency;
- слишком маленькие row groups;
- больше Parquet footers.

Spark имеет настройку `spark.sql.files.openCostInBytes`, которая учитывает стоимость открытия файла при упаковке files в input partitions. Но это не лечит фундаментально плохой layout.

## 26. Почему слишком большой Parquet-файл тоже может быть плохо

Например:

```text
1 file = 20 GB
```

Проблемы:

- меньше parallelism;
- отдельный task может работать очень долго;
- риск stragglers;
- тяжелее retry;
- row groups могут быть крупными и memory-heavy;
- неравномерная загрузка executors.

Для больших sequential scans крупные файлы полезны, но нужно сохранять достаточно parallelism.

## 27. HDFS block size и Parquet row group size

Apache Parquet docs рекомендуют большие row groups для больших sequential IO. В официальной конфигурационной документации Parquet как optimized setup приводится идея крупных row groups, например 512 MB - 1 GB, и HDFS block size достаточно большой, чтобы row group помещался в block.

Практическая интерпретация:

```text
HDFS block size: 256 MB или 512 MB
Parquet row group: 128 MB - 512 MB
Parquet file: 256 MB - 1 GB
```

Но конкретные значения зависят от:

- Spark version;
- executor memory;
- cluster IO;
- compression codec;
- schema width;
- average row size;
- downstream engines;
- SLA.

Для многих enterprise Spark lake tables хороший starting point:

```text
target file size: 256 MB
maxPartitionBytes: 128 MB или 256 MB
shuffle output tuned to produce 128-512 MB files
```

## 28. Spark file scan settings

В Spark SQL важны:

```sql
SET spark.sql.files.maxPartitionBytes = 134217728;
SET spark.sql.files.openCostInBytes = 4194304;
```

`spark.sql.files.maxPartitionBytes`:

- максимум bytes, которые Spark упаковывает в одну input partition при чтении files;
- default часто 128 MB;
- влияет на число input tasks.

`spark.sql.files.openCostInBytes`:

- оценочная стоимость открытия файла;
- помогает Spark учитывать overhead small files;
- особенно заметно при большом количестве маленьких files.

Также:

```sql
SET spark.sql.sources.parallelPartitionDiscovery.threshold = 32;
SET spark.sql.sources.parallelPartitionDiscovery.parallelism = 10000;
```

Эти настройки влияют на parallel listing большого числа input paths.

Не используй configs как замену нормальному file layout.

## 29. Как Spark превращает files в tasks

Упрощенно:

1. Spark получает список files.
2. Spark смотрит размеры files и `maxPartitionBytes`.
3. Маленькие files могут быть упакованы вместе.
4. Большие files могут быть split, если формат splittable и условия позволяют.
5. Получаются input partitions.
6. На каждую input partition запускается task.

Пример:

```text
10 files по 256 MB
maxPartitionBytes = 128 MB

Spark может создать примерно 20 input partitions
```

Другой пример:

```text
10 000 files по 1 MB
openCostInBytes = 4 MB
maxPartitionBytes = 128 MB

Spark будет группировать files,
но planning и metadata overhead все равно будут большими
```

## 30. Target file count

При записи partition нужно думать:

```text
partition size / target file size = target number of files
```

Пример:

```text
event_date=2026-05-05 size: 1 TB
target file size: 256 MB

1 TB / 256 MB ~= 4096 files
```

Это много, но для 1 TB partition может быть нормально, если cluster большой и queries читают эту дату часто.

Другой пример:

```text
partition size: 10 GB
target file size: 256 MB

10 GB / 256 MB ~= 40 files
```

Если ты видишь:

```text
10 GB partition -> 50 000 files
```

это small-file incident.

## 31. Управление количеством output files

В Spark SQL можно использовать hints:

```sql
SELECT /*+ REPARTITION(400, event_date) */
    ...
FROM ...
```

Или:

```sql
SELECT /*+ REBALANCE(event_date) */
    ...
FROM ...
```

`REBALANCE` полезен при AQE, когда нужно лучше распределить output и уменьшить skew/small files.

Но hints нужно использовать осознанно.

Плохой паттерн:

```sql
SELECT /*+ COALESCE(1) */ *
FROM huge_table;
```

Один output file для большой таблицы почти всегда плохая идея.

## 32. Shuffle partitions и output files

Многие Spark SQL writes формируют output files после shuffle.

Если:

```sql
SET spark.sql.shuffle.partitions = 200;
```

то после `GROUP BY` у тебя может быть около 200 shuffle partitions, но итоговый file count зависит еще от:

- number of output Hive partitions;
- task output distribution;
- dynamic partition write behavior;
- AQE coalescing;
- skew;
- writer implementation.

Слишком большой `spark.sql.shuffle.partitions` может дать много маленьких files.

Слишком маленький - spills, long tasks, low parallelism.

Starting point:

```text
post-shuffle data size / desired partition size
```

Например:

```text
post-shuffle data = 500 GB
target shuffle partition = 256 MB
500 GB / 256 MB ~= 2000 shuffle partitions
```

## 33. AQE

Adaptive Query Execution помогает Spark принимать runtime-решения:

- coalesce post-shuffle partitions;
- optimize skewed joins;
- convert sort-merge join to broadcast join;
- change join strategies using runtime stats.

Baseline:

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
```

AQE полезен, но он не исправит:

- неправильный partitioning;
- отсутствие partition filters;
- миллионы tiny files;
- join explosion;
- плохую семантику keys.

## 34. Hive partition registration

Если Spark пишет через Hive table:

```sql
INSERT INTO TABLE raw.events
PARTITION (event_date, source_system)
SELECT ...
```

partition metadata обычно обновляется.

Если files попали на HDFS напрямую:

```text
hdfs:///data/lake/raw/events/event_date=2026-05-05/source_system=web
```

Hive может не знать partition.

Добавить partition:

```sql
ALTER TABLE raw.events ADD IF NOT EXISTS
    PARTITION (event_date = '2026-05-05', source_system = 'web')
    LOCATION 'hdfs:///data/lake/raw/events/event_date=2026-05-05/source_system=web';
```

Bulk discovery:

```sql
MSCK REPAIR TABLE raw.events;
REFRESH TABLE raw.events;
```

Best practice: для известных ranges используй targeted `ALTER TABLE ADD PARTITION`, а не blind `MSCK REPAIR TABLE` по огромной таблице.

## 35. REFRESH TABLE

Spark session может иметь stale metadata/file listing.

Используй:

```sql
REFRESH TABLE raw.events;
```

Если cached:

```sql
UNCACHE TABLE raw.events;
REFRESH TABLE raw.events;
```

`REFRESH TABLE` не добавляет partitions в Hive Metastore. Он обновляет Spark-side metadata/file cache.

## 36. Static vs dynamic partition insert

Static partition:

```sql
INSERT OVERWRITE TABLE mart.daily_revenue
PARTITION (event_date = DATE '2026-05-05')
SELECT
    country,
    SUM(amount) AS revenue
FROM refined.events_clean
WHERE event_date = DATE '2026-05-05'
GROUP BY country;
```

Dynamic partition:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.daily_revenue
PARTITION (event_date)
SELECT
    country,
    SUM(amount) AS revenue,
    event_date
FROM refined.events_clean
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date, country;
```

Dynamic overwrite опасен, если result содержит лишние partitions.

Перед dynamic overwrite:

- проверь `WHERE`;
- проверь output partitions;
- проверь column order;
- проверь row counts before/after;
- имей recovery path.

## 37. Safe write checklist

Перед write:

- Таблица существует?
- Это external или managed?
- Location правильный?
- Source range bounded?
- Partition filter есть?
- Output schema совпадает?
- Partition columns есть в result?
- Write mode intentional?
- Dynamic overwrite безопасен?
- Target file count ожидаемый?
- Validation queries готовы?
- Rollback возможен?

После write:

- row count by partition;
- duplicate grain check;
- metric reconciliation;
- HDFS file count;
- HDFS size;
- `SHOW PARTITIONS`;
- downstream smoke test.

## 38. Post-write validation

Row counts:

```sql
SELECT
    event_date,
    COUNT(*) AS rows
FROM mart.daily_revenue
WHERE event_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-05'
GROUP BY event_date
ORDER BY event_date;
```

Duplicate grain:

```sql
SELECT
    event_date,
    country,
    COUNT(*) AS rows
FROM mart.daily_revenue
WHERE event_date = DATE '2026-05-05'
GROUP BY event_date, country
HAVING COUNT(*) > 1;
```

HDFS file count:

```bash
hdfs dfs -count -h /data/lake/mart/daily_revenue/event_date=2026-05-05
```

Size:

```bash
hdfs dfs -du -s -h /data/lake/mart/daily_revenue/event_date=2026-05-05
```

## 39. EXPLAIN FORMATTED

Перед тяжелым запросом:

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

## 40. Statistics

Spark optimizer использует statistics для:

- join planning;
- broadcast decisions;
- cost estimates;
- cardinality estimates.

Table stats:

```sql
ANALYZE TABLE raw.events COMPUTE STATISTICS;
```

Column stats:

```sql
ANALYZE TABLE raw.events COMPUTE STATISTICS FOR COLUMNS event_date, user_id, source_system;
```

Обновляй stats после:

- больших backfills;
- больших overwrites;
- compaction;
- изменения распределения keys;
- schema/data layout changes.

Не доверяй `EXPLAIN COST`, если stats отсутствуют или устарели.

## 41. Joins поверх HDFS/Hive tables

Для больших joins:

- фильтруй обе стороны по partitions;
- выбирай только нужные колонки;
- проверяй типы keys;
- проверяй uniqueness;
- избегай many-to-many, если это не требование;
- используй `LEFT SEMI JOIN` для existence;
- используй `LEFT ANTI JOIN` для exclusion.

Fact + small dimension:

```sql
SELECT /*+ BROADCAST(u) */
    e.event_date,
    e.user_id,
    u.country,
    e.amount
FROM refined.events_clean e
JOIN refined.users_current u
    ON e.user_id = u.user_id
WHERE e.event_date = DATE '2026-05-05';
```

Broadcast только если dimension реально маленькая.

Large + large:

```sql
WITH events AS (
    SELECT event_date, user_id, event_id
    FROM refined.events_clean
    WHERE event_date = DATE '2026-05-05'
),
orders AS (
    SELECT order_date, user_id, order_id, amount
    FROM refined.orders_clean
    WHERE order_date = DATE '2026-05-05'
)
SELECT
    e.user_id,
    COUNT(DISTINCT e.event_id) AS events,
    COUNT(DISTINCT o.order_id) AS orders,
    SUM(o.amount) AS amount
FROM events e
JOIN orders o
    ON e.user_id = o.user_id
GROUP BY e.user_id;
```

Перед таким join проверь cardinality и skew.

## 42. Skew

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

Признаки:

- несколько tasks сильно дольше;
- spill на отдельных tasks;
- shuffle read сильно отличается между tasks;
- hot keys занимают большую долю данных.

Решения:

1. Filter/projection до join.
2. Pre-aggregation.
3. AQE skew join.
4. Split hot keys.
5. Salting, только если корректность доказана.

## 43. Backfills

Backfill - это controlled production operation.

Правила:

- explicit date range;
- batch ranges;
- same code path as daily job;
- no concurrent writes to same partitions;
- validation per batch;
- rollback plan;
- stats refresh after large backfill.

Пример:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE mart.daily_revenue
PARTITION (event_date)
SELECT
    country,
    SUM(amount) AS revenue,
    event_date
FROM refined.events_clean
WHERE event_date BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
GROUP BY event_date, country;
```

## 44. Compaction

Compaction нужна, когда partition содержит слишком много маленьких файлов.

Перед compaction:

```bash
hdfs dfs -count -h /data/lake/refined/events_clean/event_date=2026-05-05
hdfs dfs -du -s -h /data/lake/refined/events_clean/event_date=2026-05-05
```

Compaction через overwrite partition:

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;
SET spark.sql.shuffle.partitions = 400;

INSERT OVERWRITE TABLE refined.events_clean
PARTITION (event_date)
SELECT
    event_id,
    user_id,
    event_time,
    event_type,
    amount,
    ingestion_ts,
    source_system,
    event_date
FROM refined.events_clean
WHERE event_date = DATE '2026-05-05';
```

После:

```bash
hdfs dfs -count -h /data/lake/refined/events_clean/event_date=2026-05-05
```

Не compact'и всю таблицу, если проблема только в нескольких partitions.

## 45. Permissions и ACL

Проблемы permissions часто выглядят как странные read/write failures.

Проверить:

```bash
hdfs dfs -ls -d /data/lake/raw/events
hdfs dfs -getfacl /data/lake/raw/events
hdfs dfs -getfacl /data/lake/raw/events/event_date=2026-05-05
```

Типовые проблемы:

- interactive user имеет доступ, service user нет;
- partition directories созданы с другой group;
- default ACL не применился;
- staging path writable, final path not writable;
- Hive table location и actual partition location имеют разные ACL.

Best practice: production jobs должны работать под service users с явно настроенными ACL/grants.

## 46. Security и governance

Для HDFS/Hive lake нужны:

- Kerberos или platform authentication;
- HDFS ACLs;
- Ranger/Sentry или аналог;
- owner per database/table;
- PII classification;
- audit logs;
- retention policy;
- separated dev/stage/prod paths.

Не делай:

- raw PII в mart без необходимости;
- secrets в SQL/table properties;
- production output в personal path;
- sandbox как источник production pipeline.

## 47. Типовые incidents и диагностика

### Query вернул ноль строк

Проверить:

```sql
SHOW PARTITIONS raw.events;
DESCRIBE EXTENDED raw.events PARTITION (event_date = '2026-05-05');
```

```bash
hdfs dfs -ls /data/lake/raw/events/event_date=2026-05-05
```

Возможные причины:

- partition не зарегистрирован;
- partition location неправильный;
- files отсутствуют;
- partition type mismatch;
- query filter не совпадает с partition values.

### FileNotFoundException

Часто означает stale metadata/file listing.

Действия:

```sql
UNCACHE TABLE raw.events;
REFRESH TABLE raw.events;
```

Проверить HDFS path из error.

### Slow query

Порядок:

1. `EXPLAIN FORMATTED`.
2. Проверить `PartitionFilters`.
3. Проверить `ReadSchema`.
4. Проверить file count.
5. Проверить small files.
6. Проверить skew.
7. Проверить join strategy.
8. Проверить stats.
9. Смотреть Spark UI stages/shuffle/spill.

## 48. Best practices: HDFS layout

- Один dataset - один table root.
- Не смешивать formats в одном root.
- Не смешивать schemas в одном root.
- Не писать tmp внутри production root.
- Использовать Hive-style partition directories.
- Контролировать file count per partition.
- Использовать HDFS snapshots для critical zones, если доступны.
- Настраивать retention по слоям.
- Не удалять HDFS paths вручную без плана metadata cleanup.

## 49. Best practices: Hive

- Для lake чаще использовать external tables.
- Всегда задавать explicit `LOCATION`.
- Проверять managed/external перед destructive operations.
- Использовать `SHOW CREATE TABLE`, `DESCRIBE FORMATTED`, `SHOW PARTITIONS`.
- Для известных partitions использовать `ALTER TABLE ADD PARTITION`.
- Не запускать `MSCK REPAIR TABLE` на огромных таблицах без необходимости.
- Обновлять stats для важных tables/columns.
- Не менять schema без downstream review.

## 50. Best practices: partitioning

- Partition по частым filters.
- Date partition как default для event/fact tables.
- Не partition by high-cardinality keys.
- Держать partition count управляемым.
- Использовать direct predicates по partition columns.
- Не оборачивать partition columns в functions.
- Для timestamp windows использовать partition date + half-open timestamp interval.
- Dynamic overwrite только с bounded source range.

## 51. Best practices: Parquet

- Curated layers хранить в Parquet.
- Target file size обычно 128-512 MB.
- Для больших sequential scans допустимы 512 MB-1 GB files.
- Избегать tiny files.
- Использовать explicit schema и правильные types.
- Money хранить как `DECIMAL`.
- Не хранить весь payload как JSON string в mart/refined.
- Следить за row group / HDFS block / file size совместимостью.
- Не смешивать incompatible schemas across partitions.

## 52. Best practices: Spark SQL

- Явные CTE.
- Явные колонки.
- Partition filters в начале.
- Projection pushdown.
- `EXPLAIN FORMATTED` для дорогих queries.
- Явные join types.
- `LEFT SEMI` / `LEFT ANTI` для existence/exclusion.
- Не скрывать join explosion через `DISTINCT`.
- AQE включен, но не используется как замена нормальному дизайну.
- Shuffle partitions рассчитаны от data size.
- Post-write validation обязательна.

## 53. Anti-patterns

Не делай:

- миллионы tiny files;
- `SELECT *` из wide Parquet table;
- full scan без partition filter;
- full overwrite для daily job;
- partition by `user_id`;
- `MSCK REPAIR TABLE` как ежедневную привычку;
- direct HDFS path как production contract без причины;
- `coalesce(1)` для больших outputs;
- broad `INSERT OVERWRITE TABLE`;
- blind broadcast hints;
- schema evolution без проверки старых partitions;
- ручной `hdfs dfs -rm -r` вместо controlled table operation;
- `DISTINCT` как средство от дубликатов после join.

## 54. Практический sizing cheat sheet

Ориентиры, не догмы:

```text
HDFS block size:
  128 MB legacy/common
  256-512 MB для крупных analytical workloads
  1 GB в отдельных optimized setups

Parquet file size:
  128-512 MB default enterprise target
  512 MB-1 GB для очень больших sequential fact scans

Parquet row group:
  128-512 MB часто практично
  512 MB-1 GB возможно для scan-heavy workloads

Spark input partition:
  spark.sql.files.maxPartitionBytes default часто 128 MB

Shuffle partition:
  128-512 MB post-shuffle data per partition
```

Всегда проверяй на реальном workload:

- scan time;
- scheduler time;
- spill;
- memory;
- file count;
- Spark UI stage metrics;
- downstream query patterns.

## 55. Мини-лаборатория: проверить health partition

Допустим, есть:

```text
mart.daily_revenue
event_date=2026-05-05
```

Шаг 1. Hive metadata:

```sql
SHOW CREATE TABLE mart.daily_revenue;
DESCRIBE FORMATTED mart.daily_revenue;
SHOW PARTITIONS mart.daily_revenue;
```

Шаг 2. SQL count:

```sql
SELECT
    event_date,
    COUNT(*) AS rows
FROM mart.daily_revenue
WHERE event_date = DATE '2026-05-05'
GROUP BY event_date;
```

Шаг 3. HDFS files:

```bash
hdfs dfs -count -h /data/lake/mart/daily_revenue/event_date=2026-05-05
hdfs dfs -du -s -h /data/lake/mart/daily_revenue/event_date=2026-05-05
```

Шаг 4. Plan:

```sql
EXPLAIN FORMATTED
SELECT
    country,
    SUM(revenue)
FROM mart.daily_revenue
WHERE event_date = DATE '2026-05-05'
GROUP BY country;
```

Шаг 5. Оценить:

- есть ли `PartitionFilters`;
- сколько файлов;
- средний размер файла;
- нет ли duplicate grain;
- не читает ли запрос лишние колонки.

## 56. Production review checklist

Перед тем как принять дизайн или PR:

- HDFS layout понятен?
- Table external/managed выбрана осознанно?
- `LOCATION` явный?
- Partition columns соответствуют filters?
- Нет high-cardinality partitions?
- File size target задан?
- Small-file mitigation есть?
- Spark SQL использует partition pruning?
- `SELECT *` отсутствует в production path?
- Dynamic overwrite bounded?
- Post-write validation есть?
- Metadata repair/refresh strategy есть?
- Stats refresh strategy есть?
- ACL/owner/retention указаны?
- Recovery path есть?

## 57. Sources

Официальные источники и документация, на которых основаны best practices:

- Apache Hadoop HDFS Architecture: https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
- Apache Hadoop FileSystem Shell: https://hadoop.apache.org/docs/r3.3.0/hadoop-project-dist/hadoop-common/FileSystemShell.html
- Apache Hive LanguageManual DDL: https://hive.apache.org/docs/latest/language/languagemanual-ddl/
- Apache Hive Managed vs External Tables: https://hive.apache.org/docs/latest/language/managed-vs--external-tables/
- Apache Spark SQL Performance Tuning: https://spark.apache.org/docs/latest/sql-performance-tuning.html
- Apache Parquet Concepts: https://parquet.apache.org/docs/concepts/
- Apache Parquet File Format: https://parquet.apache.org/docs/file-format/
- Apache Parquet Configurations: https://parquet.apache.org/docs/file-format/configurations/

