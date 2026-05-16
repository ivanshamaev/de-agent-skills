# Data Engineering Agent Skills

Репозиторий содержит специализированные знания в виде **agent skills** — предметных инструкций, которые загружаются в контекст агента при решении задач data engineering. Каждый скилл охватывает конкретный инструмент или технологию: от написания SQL до оркестрации пайплайнов и оптимизации аналитических запросов.

---

## Содержание

1. [Концепции и парадигмы](#поставщики-skills)
2. [Концепции и парадигмы](#концепции-и-парадигмы)
3. [Архитектурные паттерны](#архитектурные-паттерны)
4. [Моделирование данных](#моделирование-данных)
5. [Стек инструментов](#стек-инструментов)
   - [Хранилище объектов](#хранилище-объектов)
   - [Форматы таблиц (Table Formats)](#форматы-таблиц)
   - [Метасторы и каталоги данных](#метасторы-и-каталоги-данных)
   - [Пакетная обработка (Batch)](#пакетная-обработка-batch)
   - [Потоковая обработка (Streaming)](#потоковая-обработка-streaming)
   - [Оркестрация](#оркестрация)
   - [Трансформация данных](#трансформация-данных)
   - [Аналитические базы данных (OLAP / Serving)](#аналитические-базы-данных)
   - [Качество данных и наблюдаемость](#качество-данных-и-наблюдаемость)
6. [Скиллы в этом репозитории](#скиллы-в-этом-репозитории)
7. [Референсные архитектуры](#референсные-архитектуры)
8. [Доступность из РФ](#доступность-из-рф)
9. [Ресурсы и ссылки](#ресурсы-и-ссылки)

---

## Поставщики Skills

| Название                | Ссылка                                                     | Описание                                  |
| ----------------------- | ---------------------------------------------------------- | ----------------------------------------- |
| ClickHouse Agent Skills | [agent-skills](https://github.com/ClickHouse/agent-skills) | Набор skills для AI-агентов от ClickHouse |
| Astronomer Agents       | [agents](https://github.com/astronomer/agents)             | AI agent tooling for data engineering workflows |

## Концепции и парадигмы

### Data Warehouse (DWH)

Централизованное реляционное хранилище, оптимизированное для аналитических запросов. Данные приходят через ETL-процесс уже очищенными и смоделированными по схемам Kimball (звезда/снежинка) или 3NF. Главное: **схема определяется при записи** (schema-on-write).

Примеры: Vertica, Greenplum, ClickHouse, Redshift, BigQuery, Snowflake.

### Data Lake

Централизованное хранилище сырых данных в нативном формате (JSON, CSV, Parquet, ORC) на объектном хранилище или HDFS. **Схема определяется при чтении** (schema-on-read). Дёшево хранить любые данные, но сложно обеспечить качество и управляемость.

Примеры: HDFS + Hive, S3 + Glue, MinIO + Hive Metastore.

### Data Lakehouse

Архитектура, объединяющая гибкость Data Lake (открытое объектное хранилище) с надёжностью DWH (ACID-транзакции, версионирование, схемы, индексы). Реализуется через **открытые форматы таблиц** поверх объектного хранилища.

Ключевые форматы: Apache Iceberg, Apache Hudi, Delta Lake.

### Сравнение парадигм

| Критерий | DWH | Data Lake | Data Lakehouse |
|---|---|---|---|
| Схема | Write-time | Read-time | Write-time (гибко) |
| ACID | Да | Нет | Да |
| Форматы данных | Структурированные | Любые | Структурированные + полуструктурированные |
| SQL-поддержка | Полная | Ограниченная | Полная |
| Стоимость хранения | Высокая | Низкая | Низкая |
| Версионирование | Нет | Нет | Да (snapshots) |
| Типичный движок | Proprietary | Spark/Hive | Spark, Trino, Flink |

---

## Архитектурные паттерны

### Lambda-архитектура

Два независимых слоя обработки: **batch layer** (полная переработка исторических данных) и **speed layer** (потоковая обработка для свежих данных). **Serving layer** объединяет результаты. Проблема — двойная логика в двух кодовых базах.

```
источники → [Batch Layer: Spark]   → batch views  ↘
          → [Speed Layer: Flink]   → real-time views → [Serving: Trino/ClickHouse] → BI
```

### Kappa-архитектура

Единственный слой — потоковый. Весь бэтч моделируется как реплей стрима с начала. Проще в поддержке, но требует зрелого стриминга (Kafka + Flink).

```
источники → Kafka → Flink → Iceberg/Hudi → [Serving: Trino/ClickHouse] → BI
```

### Medallion-архитектура (Bronze / Silver / Gold)

Трёхуровневая организация данных внутри Data Lakehouse:

| Слой | Назначение | Формат | Трансформации |
|---|---|---|---|
| **Bronze** | Сырые данные как есть (append-only) | Parquet/Avro/JSON | Нет — только приём и хранение |
| **Silver** | Очищенные, дедуплицированные, конформные | Parquet + Iceberg | Парсинг, типизация, дедупликация, SCD |
| **Gold** | Агрегаты, витрины, бизнес-метрики | Parquet + Iceberg | Joins, GROUP BY, бизнес-логика |

Каждый слой — отдельная Iceberg/Hudi/Delta-таблица. Переход между слоями оркестрирует Airflow/Dagster.

### ELT vs ETL

**ETL** (Extract → Transform → Load): трансформация до загрузки в хранилище. Подходит для DWH с ограниченными вычислительными ресурсами.

**ELT** (Extract → Load → Transform): загрузка сырых данных, трансформация внутри хранилища мощностями Spark/Trino/dbt. Современный стандарт для Lakehouse.

---

## Моделирование данных

### Dimensional Modeling (Kimball)

Схема «звезда» и «снежинка» для аналитических DWH. Факты + измерения. Быстро для агрегаций, понятно аналитикам.

```sql
-- Факт
CREATE TABLE fact_sales (sale_id, date_id, customer_id, product_id, amount);
-- Измерение
CREATE TABLE dim_customer (customer_id, name, country, segment);
```

Медленно меняющиеся измерения (SCD):
- **SCD Type 1** — перезапись (история теряется)
- **SCD Type 2** — новая строка с `effective_date` / `expiry_date` / `is_current`
- **SCD Type 3** — дополнительная колонка `prev_value`

### Data Vault 2.0

Подходит для корпоративных DWH с множеством источников. Три типа сущностей:
- **Hub** — ключевые бизнес-объекты (только бизнес-ключ + хэш)
- **Link** — связи между хабами
- **Satellite** — атрибуты и история изменений

Загрузка всегда insert-only, полная аудируемость. Инструменты: dbt + dbtvault.

### Inmon (3NF / Enterprise DWH)

Нормализованная корпоративная модель (3NF), из которой строятся витрины данных (data marts) по Kimball. Больше joins, меньше дублирования.

---

## Стек инструментов

### Хранилище объектов

Основа любого Data Lake / Lakehouse — S3-совместимое объектное хранилище.

| Инструмент | Тип | Описание | РФ |
|---|---|---|---|
| **MinIO** | Open source | Высокопроизводительное S3-совместимое хранилище, self-hosted. Стандарт для on-premise Lakehouse | ✅ |
| **Apache HDFS** | Open source | Распределённая ФС Hadoop. Зрелая технология, но сложная в эксплуатации | ✅ |
| **Ceph** | Open source | Объектное + блочное + файловое хранилище. Сложнее MinIO, гибче | ✅ |
| **Yandex Object Storage** | Cloud (RU) | S3-совместимое, доступно из РФ, полная совместимость с boto3 | ✅ |
| **VK Cloud Storage** | Cloud (RU) | S3-совместимое облако, доступно из РФ | ✅ |

**Рекомендация for on-premise**: MinIO — проще всего поднять, полностью совместим с S3 API, поддерживает erasure coding и multi-site репликацию.

### Форматы таблиц

Открытые форматы хранения таблиц поверх объектного хранилища — основа Lakehouse.

| Инструмент | Описание | Ключевые особенности | РФ |
|---|---|---|---|
| **Apache Iceberg** | Open source (Apache) | ACID, hidden partitioning, partition/schema evolution, time travel, concurrent writes. Наиболее зрелый и широко поддержанный | ✅ |
| **Apache Hudi** | Open source (Apache) | Copy-on-Write и Merge-on-Read, встроенный CDC, record-level index. Сильна в upsert-сценариях | ✅ |
| **Delta Lake** | Open source (Linux Foundation) | ACID поверх Parquet, Change Data Feed, Z-Order clustering. Тесно интегрирована со Spark | ✅ |
| **Apache Paimon** | Open source (Apache) | Потоковый lakehouse, нативная интеграция с Flink, LSM-структура для high-frequency upserts | ✅ |

**Сравнение Iceberg / Hudi / Delta**

| Критерий | Iceberg | Hudi | Delta Lake |
|---|---|---|---|
| ACID | Да | Да | Да |
| Time Travel | Да | Да | Да |
| Schema Evolution | Полная | Частичная | Частичная |
| Partition Evolution | Да | Нет | Нет |
| CDC / Upserts | COW | COW + MOR | COW |
| Основной движок | Spark, Trino, Flink | Spark, Flink | Spark |
| Поддержка Trino | Нативная | Ограниченная | Ограниченная |

**Рекомендация**: Apache Iceberg — лучший выбор для новых проектов: наиболее полная поддержка Trino и Flink, зрелый стандарт для Lakehouse.

### Метасторы и каталоги данных

Хранят метаданные таблиц: схемы, расположения файлов, partition spec.

| Инструмент | Описание | РФ |
|---|---|---|
| **Apache Hive Metastore (HMS)** | Классический метастор, используется Spark/Trino/Flink. Хранит схемы в MySQL/PostgreSQL | ✅ |
| **Project Nessie** | Git-подобный каталог для Iceberg/Delta/Hudi. Ветки, теги, транзакции на уровне каталога | ✅ |
| **Apache Atlas** | Каталог с lineage, классификациями, политиками. Часть HDP-экосистемы | ✅ |
| **DataHub** | Современный data catalog с lineage, поиском, governance. Разработан LinkedIn | ✅ |
| **OpenMetadata** | Open source data catalog, metadata management, data quality интеграция | ✅ |
| **Apache Polaris** | Открытый REST-каталог для Iceberg (Snowflake open source) | ✅ |
| **Apache Gravitino** | Unified metadata layer для нескольких каталогов (Iceberg, Hive, JDBC) | ✅ |

### Пакетная обработка (Batch)

| Инструмент | Описание | РФ |
|---|---|---|
| **Apache Spark** | Распределённый движок для ETL/ELT. PySpark + Spark SQL — индустриальный стандарт batch-обработки. Нативная поддержка Iceberg/Hudi/Delta | ✅ |
| **Trino** | Распределённый SQL-движок для федеративных запросов. Работает поверх Iceberg, Hive, PostgreSQL, Kafka и др. без ETL | ✅ |
| **Apache Hive** | SQL поверх HDFS/S3, зрелая технология. Медленнее Spark/Trino, но хорошо поддерживается в Hadoop-экосистеме | ✅ |
| **DuckDB** | Встраиваемая OLAP-база для локальной и mid-scale обработки. Идеален для dbt-разработки, тестов, ad-hoc анализа | ✅ |
| **Apache Beam** | Унифицированная модель для batch и streaming. Работает поверх Spark, Flink, Dataflow | ✅ |

### Потоковая обработка (Streaming)

| Инструмент | Назначение | РФ |
|---|---|---|
| **Apache Kafka** | Распределённый брокер сообщений. Основа стриминговых архитектур: event bus, CDC sink, буфер между источниками и обработчиками | ✅ |
| **Apache Flink** | Потоковый + batch движок. Stateful обработка, exactly-once, нативная запись в Iceberg/Hudi/Paimon | ✅ |
| **Kafka Streams** | Библиотека потоковой обработки поверх Kafka. Лёгкий вариант без отдельного кластера | ✅ |
| **Apache Pulsar** | Альтернатива Kafka: мультитенантность, geo-репликация, раздельное хранение и брокеринг | ✅ |
| **Debezium** | CDC (Change Data Capture) из RDBMS (PostgreSQL, MySQL, Oracle) в Kafka. Основа для репликации OLTP → Lakehouse | ✅ |
| **Kafka Connect** | Фреймворк для коннекторов source/sink. Сотни готовых коннекторов (S3, JDBC, Elasticsearch, ...) | ✅ |
| **Apache Spark Structured Streaming** | Микробатч-стриминг поверх Spark. Хорош при уже имеющемся Spark-кластере | ✅ |
| **RisingWave** | Потоковая база данных с SQL-интерфейсом. Материализованные представления над стримами | ✅ |

**CDC-паттерн (типичная интеграция):**
```
PostgreSQL/MySQL  →  Debezium  →  Kafka  →  Flink/Spark Streaming  →  Iceberg (Silver)
```

### Оркестрация

| Инструмент | Описание | РФ |
|---|---|---|
| **Apache Airflow** | Де-факто стандарт оркестрации. DAG как Python-код, богатая экосистема провайдеров, UI мониторинга | ✅ |
| **dag-factory** | Конфигурационное управление Airflow DAG через YAML. Устраняет boilerplate Python | ✅ |
| **Prefect** | Современная альтернатива Airflow. Декораторы Python, динамические workflows, self-hosted Prefect Server | ✅ |
| **Dagster** | Asset-ориентированная оркестрация. Явная зависимость по data assets, встроенный lineage и observability | ✅ |
| **Mage** | Notebook-стиль разработки пайплайнов. Быстрый старт, встроенный dbt-интегратор | ✅ |
| **Apache DolphinScheduler** | Enterprise-оркестратор из экосистемы Apache. Поддерживает сотни задач, визуальный редактор DAG | ✅ |

**Выбор оркестратора:**
- Большая команда, много DAG → **Airflow** (наибольшая экосистема и знания)
- Data assets / lineage первичны → **Dagster**
- Python-first, простой старт → **Prefect**
- YAML-конфигурация Airflow → **dag-factory** поверх Airflow

### Трансформация данных

| Инструмент | Описание | РФ |
|---|---|---|
| **dbt (data build tool)** | SQL-first трансформации: модели, тесты, документация, lineage. Компилирует SQL под нужный движок (Spark, Trino, ClickHouse, DuckDB, ...) | ✅ |
| **dbt-spark** | dbt-адаптер для Apache Spark | ✅ |
| **dbt-trino** | dbt-адаптер для Trino | ✅ |
| **dbt-clickhouse** | dbt-адаптер для ClickHouse | ✅ |
| **Apache Spark SQL** | SQL-движок Spark. Поддержка оконных функций, Iceberg DDL/DML, сложных трансформаций | ✅ |
| **SQLMesh** | Следующее поколение dbt: виртуальные среды, column-level lineage, автоматический backfill | ✅ |
| **PySpark** | Python API для Spark. DataFrame + SQL. Стандарт для сложных Python-ETL пайплайнов | ✅ |

**Типичная связка**: Airflow оркестрирует → dbt трансформирует → Trino обслуживает запросы.

### Аналитические базы данных

OLAP-движки для serving layer — конечная точка для BI и аналитики.

| Инструмент | Описание | РФ |
|---|---|---|
| **ClickHouse** | Колоночная OLAP-база, разработана Яндексом. Рекордная скорость агрегаций, Kafka/S3 интеграция. Лучший выбор для realtime-аналитики и event-данных | ✅ |
| **Apache Doris** | MPP OLAP с MySQL-совместимым протоколом. Хорош для смешанных OLAP/OLTP нагрузок, встроенный Kafka-импорт | ✅ |
| **StarRocks** | Fork Apache Doris, активно развивается. Быстрее Doris, лучше поддержка Iceberg/Hudi | ✅ |
| **Greenplum** | MPP на базе PostgreSQL. Хорош для сложного SQL, функциональности DWH | ✅ |
| **Vertica** | Колоночная MPP база. Enterprise-уровень, используется в крупных корпоративных DWH | ⚠️ Enterprise |
| **Apache Pinot** | Realtime OLAP для user-facing аналитики. Крайне низкая латентность, Kafka-import | ✅ |
| **Druid** | Realtime OLAP для временных рядов и событий. Встроенный Kafka-импорт, sub-second latency | ✅ |

**Сценарии выбора:**
- Realtime-аналитика, события, метрики → **ClickHouse**
- Интерактивный SQL + Iceberg/Hudi lake → **Trino + ClickHouse**
- User-facing аналитика < 100ms → **Apache Pinot**
- Классический DWH с PostgreSQL-совместимостью → **Greenplum**

### Качество данных и наблюдаемость

| Инструмент | Описание | РФ |
|---|---|---|
| **Great Expectations** | Python-библиотека для data quality. Expectation суиты, data docs, интеграция с Airflow | ✅ |
| **dbt tests** | Встроенные тесты dbt: not_null, unique, accepted_values, relationships. Плюс dbt-expectations | ✅ |
| **Soda** | Data quality checks через YAML. Интеграция с Airflow, Spark, Trino, BigQuery | ✅ |
| **Apache Griffin** | Data quality на Spark. Batch и streaming проверки | ✅ |
| **OpenLineage** | Открытый стандарт для data lineage. Интеграция с Airflow, Spark, dbt, Flink | ✅ |
| **Prometheus + Grafana** | Метрики кластеров Kafka, Spark, Airflow. Alerting на SLA пайплайнов | ✅ |

---

## Скиллы в этом репозитории

### Загруженные скиллы

| Скилл | Инструмент | Покрытие |
|---|---|---|
| [`spark_sql`](skills/spark_sql/SKILL.md) | Apache Spark SQL | Запросы, DDL, запись в форматы, работа с Hive/HDFS, performance |
| [`pyspark_etl`](skills/pyspark_etl/SKILL.md) | PySpark | DataFrame API, трансформации, joins, запись, Spark-тюнинг |
| [`vertica`](skills/vertica/SKILL.md) | Vertica | DDL, DML, CRUD, проекции, сегментация, схемы обновления данных |
| [`vertica_query_optimization`](skills/vertica_query_optimization/SKILL.md) | Vertica 11.x | EXPLAIN-планы, проекции, RLE, GROUP BY, JOIN, ORDER BY, Data Collector |
| [`airflow_dag_factory`](skills/airflow_dag_factory/SKILL.md) | Airflow + dag-factory v1.0+ | YAML DAG, defaults-иерархия, dynamic mapping, datasets, CI/CD |
| [`trino_iceberg`](skills/trino_iceberg/SKILL.md) | Trino + Apache Iceberg | DDL, бакетирование, партицирование, ALTER TABLE, DML, обслуживание, time travel |

### Спецификации (docs/specs)

Детальные технические справочники, на которые ссылаются скиллы:

| Документ | Содержание |
|---|---|
| [`vertica_query_optimization_v11.md`](docs/specs/vertica_query_optimization_v11.md) | Vertica 11.x — EXPLAIN-токены, дизайн проекций, оптимизация JOIN/GROUP BY, кодирование |
| [`vertica_admin_guide_v24.md`](docs/specs/vertica_admin_guide_v24.md) | Vertica 24.3.x — архитектура, пользователи/роли, WOS/ROS/TM, резервное копирование, мониторинг |
| [`trino_iceberg_performance_optimization.md`](docs/specs/trino_iceberg_performance_optimization.md) | Trino + Iceberg — оптимизатор, CBO, join-стратегии, pushdown, partitioning, sorted tables, maintenance |
| [`spark_sql_hdfs_hive_operations.md`](docs/specs/spark_sql_hdfs_hive_operations.md) | Spark SQL + HDFS/Hive — DDL, операции с данными |
| [`spark_sql_enterprise.md`](docs/specs/spark_sql_enterprise.md) | Enterprise-паттерны Spark SQL |
| [`pyspark_enterprise.md`](docs/specs/pyspark_enterprise.md) | Enterprise-паттерны PySpark |
| [`hdfs_hive_parquet_datalake.md`](docs/specs/hdfs_hive_parquet_datalake.md) | HDFS + Hive + Parquet Data Lake |

### Учебные гайды (guides)

Нарративные руководства на русском языке:

| Гайд | Тема |
|---|---|
| [`spark_sql_hdfs_hive_tutorial_101.md`](guides/spark_sql_hdfs_hive_tutorial_101.md) | Spark SQL + HDFS + Hive — tutorial с нуля |
| [`hdfs_hive_partitioning_spark_tutorial_101.md`](guides/hdfs_hive_partitioning_spark_tutorial_101.md) | Партицирование HDFS/Hive в Spark |

---

## Референсные архитектуры

### 1. Self-hosted Open Source Lakehouse (on-premise / российское облако)

Минимальный жизнеспособный стек для продуктовой системы:

```
Источники (OLTP БД, API, файлы)
       │
       ▼
  Kafka + Debezium           ← CDC из RDBMS, event streaming
       │
       ▼
   Apache Spark              ← batch ETL (Bronze → Silver → Gold)
   + Spark Structured        ← streaming micro-batch
     Streaming
       │
       ▼
 MinIO (S3-compatible)       ← объектное хранилище
 + Apache Iceberg            ← table format (ACID, versioning)
 + Hive Metastore            ← метаданные таблиц
       │
       ▼
     Trino                   ← SQL query engine (ad-hoc + BI)
   ClickHouse                ← fast OLAP serving layer
       │
       ▼
BI (Superset / Metabase / Grafana)

Оркестрация: Apache Airflow + dag-factory
Трансформации: dbt-spark / dbt-trino
Качество данных: Great Expectations + dbt tests
Data Catalog: DataHub или OpenMetadata
```

### 2. Streaming Lakehouse (Kappa)

```
Источники → Kafka → Flink → Iceberg (Bronze)
                          → Iceberg (Silver, Flink SQL трансформации)
                          → ClickHouse (Gold / Realtime serving)

Оркестрация: Airflow (batch-части) + Flink JobManager (streaming)
```

### 3. Batch ELT с dbt

```
Источники → Airbyte/Kafka → MinIO/S3 (raw) → Spark (load to Iceberg Bronze)
                                            ↓
                                  dbt-trino/dbt-spark
                          (Silver модели: clean, dedup, type)
                          (Gold модели: mart, aggregates)
                                            ↓
                                  Trino (serving)
```

---

## Доступность из РФ

### Статус инструментов

| Инструмент | Доступность | Способ получения |
|---|---|---|
| Apache Spark | ✅ Полная | [spark.apache.org](https://spark.apache.org/downloads.html), PyPI (`pyspark`) |
| Apache Kafka | ✅ Полная | [kafka.apache.org](https://kafka.apache.org/downloads), Docker |
| Apache Flink | ✅ Полная | [flink.apache.org](https://flink.apache.org/downloads/), Docker |
| Apache Airflow | ✅ Полная | PyPI (`apache-airflow`), Docker |
| Trino | ✅ Полная | GitHub Releases, Docker |
| ClickHouse | ✅ Полная | [clickhouse.com](https://clickhouse.com/docs/en/install), APT/RPM репозиторий |
| MinIO | ✅ Полная | GitHub Releases, Docker, Homebrew |
| Apache Iceberg | ✅ Полная | Maven Central (JAR), PyPI (`pyiceberg`) |
| dbt | ✅ Полная | PyPI (`dbt-core`, адаптеры) |
| DuckDB | ✅ Полная | PyPI (`duckdb`), GitHub |
| Debezium | ✅ Полная | Docker Hub, Maven Central |
| Apache Hudi | ✅ Полная | Maven Central, PyPI (`hudi`) |
| Delta Lake | ✅ Полная | Maven Central, PyPI (`delta-spark`) |
| DataHub | ✅ Полная | PyPI (`acryl-datahub`), Docker |
| Docker Hub | ⚠️ Rate limit | 100 pull/6ч на IP. Использовать зеркала или собственный registry |
| AWS S3 | ❌ Недоступен | Использовать MinIO, Yandex Object Storage, VK Cloud S3 |
| Google Cloud Storage | ❌ Недоступен | Использовать MinIO |
| Azure Blob Storage | ❌ Недоступен | Использовать MinIO |
| Databricks Cloud | ❌ Недоступен | Self-hosted Spark + Iceberg |
| Snowflake | ❌ Недоступен | Self-hosted ClickHouse / Trino + Iceberg |

### Зеркала пакетных менеджеров

```bash
# Python / pip — зеркало Яндекса
pip install apache-airflow \
  --index-url https://mirror.yandex.ru/mirrors/pypi/simple/ \
  --trusted-host mirror.yandex.ru

# Или настроить глобально
pip config set global.index-url https://mirror.yandex.ru/mirrors/pypi/simple/

# Maven (Gradle / Maven) — зеркало Aliyun (быстрее из РФ чем Maven Central)
# В ~/.m2/settings.xml:
# <mirror>
#   <id>aliyun</id>
#   <url>https://maven.aliyun.com/repository/public</url>
#   <mirrorOf>central</mirrorOf>
# </mirror>
```

### Docker-образы без Docker Hub

```bash
# Собственный registry: Harbor (open source)
# docker pull harbor.internal.company.ru/apache/spark:3.5.0

# Yandex Container Registry
# docker pull cr.yandex/<registry_id>/spark:3.5.0

# Предзагрузить образы через VPN и загрузить в приватный registry:
docker pull apache/spark:3.5.0
docker tag apache/spark:3.5.0 registry.internal/spark:3.5.0
docker push registry.internal/spark:3.5.0
```

### Российские облачные S3-совместимые хранилища

Для managed-деплоев без on-premise MinIO:

| Провайдер | Endpoint | Совместимость |
|---|---|---|
| **Yandex Object Storage** | `storage.yandexcloud.net` | S3 API, boto3, Spark S3A |
| **VK Cloud Storage** | `hb.vkcs.cloud` | S3 API, boto3 |
| **SberCloud OBS** | `obs.ru-moscow-1.hc.sbercloud.ru` | S3 API |

Конфигурация Spark для Yandex Object Storage:
```python
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net")
spark.conf.set("spark.hadoop.fs.s3a.access.key", "<KEY>")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "<SECRET>")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
```

---

## Ресурсы и ссылки

### Официальная документация

| Инструмент | Документация |
|---|---|
| Apache Spark | https://spark.apache.org/docs/latest/ |
| Apache Kafka | https://kafka.apache.org/documentation/ |
| Apache Flink | https://nightlies.apache.org/flink/flink-docs-stable/ |
| Apache Airflow | https://airflow.apache.org/docs/ |
| Apache Iceberg | https://iceberg.apache.org/docs/latest/ |
| Trino | https://trino.io/docs/current/ |
| ClickHouse | https://clickhouse.com/docs/ |
| dbt | https://docs.getdbt.com/ |
| MinIO | https://min.io/docs/minio/linux/index.html |
| Debezium | https://debezium.io/documentation/reference/ |
| DataHub | https://datahubproject.io/docs/ |
| dag-factory | https://astronomer.github.io/dag-factory/ |
| Vertica | https://docs.vertica.com/24.3.x/en/ |

### Обучение и практика

- **Fundamentals of Data Engineering** (Joe Reis, Matt Housley) — базовая книга по DE
- **Designing Data-Intensive Applications** (Martin Kleppmann) — распределённые системы
- **The Data Warehouse Toolkit** (Ralph Kimball) — dimensional modeling
- **Data Vault 2.0** (Dan Linstedt) — Data Vault моделирование
- [dbt Learn](https://courses.getdbt.com/) — бесплатные курсы dbt
- [Databricks Academy](https://www.databricks.com/learn/training) — Spark, Delta, Lakehouse
- [Confluent Training](https://training.confluent.io/) — Kafka

### Добавление нового скилла

1. Создать `skills/<name>/SKILL.md` по образцу существующего скилла (например, [`skills/trino_iceberg/SKILL.md`](skills/trino_iceberg/SKILL.md)).
2. Обязательные секции: **When to Use**, **Core Workflow**, предметные секции, **Anti-Patterns**, **Output Expectations**.
3. При необходимости добавить детальный справочник в `docs/specs/<name>.md`.
4. Обновить таблицы скиллов и спецификаций в этом README и в [CLAUDE.md](CLAUDE.md).
