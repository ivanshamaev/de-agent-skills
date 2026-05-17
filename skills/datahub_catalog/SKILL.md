---
name: datahub-catalog
description: DataHub data catalog — metadata ingestion recipes (PostgreSQL/Hive/Spark/dbt/Airflow/Kafka/S3), Python SDK (DatahubRestEmitter, MetadataChangeProposalWrapper), column-level lineage (FineGrainedLineage), GMS/MCE/MCP architecture, Kubernetes Helm deployment, CLI operations, search/discovery REST API and GraphQL
---

# DataHub Data Catalog

## When to Use

Activate this skill when the task involves:
- Setting up or configuring DataHub (Docker Compose quickstart or Kubernetes Helm deployment)
- Writing ingestion recipes in YAML for PostgreSQL, Hive, Spark, dbt, Airflow, Kafka, or S3
- Using the DataHub Python SDK to emit metadata, lineage, or schema programmatically
- Implementing table-level or column-level lineage via `DatahubRestEmitter` and `FineGrainedLineage`
- Managing metadata entities — Dataset, DataFlow, DataJob, Dashboard, Chart — with ownership, tags, and glossary terms
- Searching and discovering assets via the UI, REST API, or GraphQL lineage traversal
- Integrating dbt artifact ingestion or the `datahub-airflow-plugin` for pipeline lineage
- Running CLI operations: `datahub ingest`, `datahub check`, `datahub delete`, `datahub timeline`

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  DataHub Architecture                                                         │
│                                                                              │
│  Producers                  Ingestion Layer          Storage & Serving       │
│  ┌──────────────┐           ┌─────────────────┐      ┌──────────────────┐    │
│  │ Ingestion    │──MCP/MCE─▶│ GMS             │─────▶│ MySQL / Postgres │    │
│  │ Framework    │           │ (Metadata Store)│      │ (aspects store)  │    │
│  └──────────────┘           └────────┬────────┘      └──────────────────┘    │
│  ┌──────────────┐                    │ MCL                                    │
│  │ REST Emitter │──MCP REST─▶        │ (via Kafka CDC)                       │
│  └──────────────┘           ┌────────▼────────┐      ┌──────────────────┐    │
│  ┌──────────────┐           │ MAE Consumer    │─────▶│ Elasticsearch    │    │
│  │ Kafka topic  │──MCP──────│ MCE Consumer    │      │ (search index)   │    │
│  │ (async)      │           └─────────────────┘      └──────────────────┘    │
│  └──────────────┘                                     ┌──────────────────┐    │
│                                                       │ Neo4j (optional) │    │
│                              DataHub Frontend ◀───────│ (graph store)    │    │
│                              :9002                    └──────────────────┘    │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Core Services

| Component | Role | Default Port |
|-----------|------|-------------|
| **GMS** (Generalized Metadata Service) | Central metadata store; accepts MCPs via REST or Kafka | 8080 |
| **MCE Consumer** | Async Kafka consumer for `MetadataChangeEvent_v4` topic | — |
| **MAE Consumer** | Processes `MetadataAuditEvent_v4`; updates Elasticsearch + graph | — |
| **Frontend** | React SPA serving search, lineage graph, and entity pages | 9002 |
| **Elasticsearch** | Full-text search index; updated asynchronously from GMS | 9200 |
| **Kafka** | Message bus for MCP/MCL event streams | 9092 |
| **MySQL / PostgreSQL** | Persistent aspect store | 3306 / 5432 |

### Event Types

| Event | Direction | Description |
|-------|-----------|-------------|
| **MCP** (MetadataChangeProposal) | Producer → GMS | Proposes a change to a single aspect of one entity |
| **MCE** (MetadataChangeEvent) | Producer → Kafka topic | Older batched event; prefer MCP for new integrations |
| **MCL** (MetadataChangeLog) | GMS → downstream consumers | Confirmed change log after GMS writes the aspect |
| **MAE** (MetadataAuditEvent) | GMS → Kafka | Legacy audit event; replaced by MCL in v0.9+ |

---

## Deployment

### Docker Compose Quickstart

```bash
pip install acryl-datahub
datahub docker quickstart          # pulls docker-compose and starts all services
# Access UI at http://localhost:9002  (default: datahub / datahub)
# GMS REST at http://localhost:8080
```

Full compose override for resource-constrained environments:

```yaml
# docker-compose.override.yml
version: "3.8"
services:
  datahub-gms:
    environment:
      DATAHUB_SERVER_TYPE: quickstart
      ELASTICSEARCH_USE_SSL: "false"
    deploy:
      resources:
        limits:
          memory: 2g

  elasticsearch:
    deploy:
      resources:
        limits:
          memory: 1g
    environment:
      ES_JAVA_OPTS: "-Xms512m -Xmx512m"
```

### Kubernetes Helm Deployment

```bash
helm repo add datahub https://helm.datahubproject.io/
helm repo update

# Install prerequisites (Kafka, Elasticsearch, MySQL)
helm install prerequisites datahub/datahub-prerequisites \
  --namespace datahub --create-namespace \
  -f prerequisites-values.yaml

# Install DataHub
helm install datahub datahub/datahub \
  --namespace datahub \
  -f datahub-values.yaml \
  --version 0.4.0
```

Minimal `datahub-values.yaml` for production:

```yaml
global:
  graph_service_impl: elasticsearch   # use neo4j for advanced graph queries
  datahub_analytics_enabled: true

datahub-gms:
  replicaCount: 2
  resources:
    requests:
      cpu: "1"
      memory: 2Gi
    limits:
      cpu: "2"
      memory: 4Gi
  env:
    DATAHUB_SERVER_TYPE: prod
    KAFKA_BOOTSTRAP_SERVER: "kafka-headless:9092"
    EBEAN_DATASOURCE_HOST: "mysql:3306"
    EBEAN_DATASOURCE_USERNAME: datahub
    EBEAN_DATASOURCE_PASSWORD: "${MYSQL_PASSWORD}"
    ELASTICSEARCH_HOST: "elasticsearch-master"
    ELASTICSEARCH_PORT: "9200"

datahub-frontend:
  replicaCount: 2
  resources:
    requests:
      cpu: 500m
      memory: 1Gi
    limits:
      cpu: "1"
      memory: 2Gi

datahub-mae-consumer:
  replicaCount: 1
  resources:
    requests:
      memory: 512Mi
    limits:
      memory: 1Gi

datahub-mce-consumer:
  replicaCount: 1
```

**Minimum cluster requirements**: 3 nodes, 7 GB RAM total; 16 GB RAM recommended for production.

---

## Ingestion Framework

### Installation

```bash
# Base CLI
pip install 'acryl-datahub[datahub-rest]'

# Source-specific extras
pip install 'acryl-datahub[postgres]'
pip install 'acryl-datahub[hive]'
pip install 'acryl-datahub[spark]'
pip install 'acryl-datahub[dbt]'
pip install 'acryl-datahub[airflow]'
pip install 'acryl-datahub[kafka]'
pip install 'acryl-datahub[s3]'
```

### Recipe YAML Format

Every ingestion job is a recipe file:

```yaml
# recipe.yaml — top-level keys: source, sink, transformers, schedule

source:
  type: <source-type>            # e.g. postgres, hive, dbt, kafka, s3
  config:
    <source-specific-config>

transformers:                    # optional, applied in order
  - type: simple_add_dataset_tags
    config:
      tag_urns:
        - "urn:li:tag:pii"

sink:
  type: datahub-rest             # or datahub-kafka for async
  config:
    server: "http://datahub-gms:8080"
    token: "${DATAHUB_TOKEN}"    # personal access token

pipeline_name: my_ingestion_job  # used for run tracking / rollback

# For scheduled runs via datahub-airflow-plugin (cron expression):
# schedule:
#   interval: "0 6 * * *"
#   timezone: "UTC"
```

---

## Ingestion Recipes by Source

### PostgreSQL

```yaml
source:
  type: postgres
  config:
    host_port: "prod-postgres:5432"
    database: analytics
    username: datahub_reader
    password: "${POSTGRES_PASSWORD}"
    include_tables: true
    include_views: true
    profiling:
      enabled: true
      profile_table_level_only: false
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  type: datahub-rest
  config:
    server: "http://datahub-gms:8080"
```

### Hive

```yaml
source:
  type: hive
  config:
    host_port: "hive-metastore:10000"
    scheme: hive
    username: hive
    password: "${HIVE_PASSWORD}"
    database: "prod_db"          # omit to ingest all databases
    include_column_lineage: true
    stateful_ingestion:
      enabled: true

sink:
  type: datahub-rest
  config:
    server: "http://datahub-gms:8080"
```

### Spark

```yaml
# Spark lineage is typically captured at runtime via the Spark listener JAR,
# not a standalone recipe. For post-hoc Spark metadata ingestion:
source:
  type: spark
  config:
    # Spark listener emits MCPs to DataHub during spark-submit
    # Add to spark-submit:
    #   --packages io.acryl:acryl-spark-lineage:0.2.16
    #   --conf spark.extraListeners=datahub.spark.DatahubSparkListener
    #   --conf spark.datahub.rest.server=http://datahub-gms:8080
```

Runtime Spark configuration:

```python
spark = SparkSession.builder \
    .appName("etl_job") \
    .config("spark.extraListeners", "datahub.spark.DatahubSparkListener") \
    .config("spark.datahub.rest.server", "http://datahub-gms:8080") \
    .config("spark.datahub.rest.token", os.environ["DATAHUB_TOKEN"]) \
    .config("spark.datahub.env", "PROD") \
    .config("spark.datahub.flow_name", "silver_etl_pipeline") \
    .getOrCreate()
```

### dbt

```yaml
source:
  type: dbt
  config:
    manifest_path: "s3://my-bucket/dbt-artifacts/manifest.json"
    catalog_path:  "s3://my-bucket/dbt-artifacts/catalog.json"
    run_results_paths:
      - "s3://my-bucket/dbt-artifacts/run_results.json"
    target_platform: trino        # underlying SQL platform
    target_platform_instance: prod-trino
    environment: PROD
    include_column_lineage: true
    meta_mapping:
      owner:
        match: ".*"
        operation: "add_owner"
        config:
          owner_type: corpuser
    tag_prefix: "dbt:"            # tags prefixed with "dbt:" in DataHub
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true
    aws_connection:
      aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
      aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
      aws_region: "us-east-1"

sink:
  type: datahub-rest
  config:
    server: "http://datahub-gms:8080"
```

### Airflow

```yaml
# Captures DAG/task metadata and lineage via the datahub-airflow-plugin.
# Install: pip install acryl-datahub[airflow]
# Add to airflow.cfg:
#   [datahub]
#   enabled = true
#   conn_id = datahub_rest_default

# Airflow Connection (datahub_rest_default):
#   conn_type: datahub_rest
#   host: http://datahub-gms:8080
#   extra: {"token": "<PAT>"}

# For recipe-based DAG metadata ingestion:
source:
  type: airflow
  config:
    base_url: "http://airflow-webserver:8080"
    airflow_connection_id: datahub_rest_default
    # Captures: DAGs as DataFlows, Tasks as DataJobs
    # with inlet/outlet annotations as dataset lineage
```

Annotate Airflow tasks for automatic dataset lineage:

```python
from airflow.lineage.entities import Table
from datahub_airflow_plugin.entities import Dataset, Urn

with DAG("daily_etl", ...) as dag:

    @task(
        inlets=[Dataset("postgres", "public.orders")],
        outlets=[Dataset("hive", "gold.daily_revenue")],
    )
    def transform_orders(**kwargs):
        ...
```

### Kafka

```yaml
source:
  type: kafka
  config:
    connection:
      bootstrap: "kafka-broker:9092"
      schema_registry_url: "http://schema-registry:8081"
    topic_patterns:
      allow:
        - "^prod\\..*"
      deny:
        - ".*\\.dlq$"
    stateful_ingestion:
      enabled: true

sink:
  type: datahub-rest
  config:
    server: "http://datahub-gms:8080"
```

### S3

```yaml
source:
  type: s3
  config:
    path_specs:
      - include: "s3://data-lake/bronze/{table}/**/*.parquet"
        table_name: "{table}"
    aws_config:
      aws_region: "us-east-1"
      aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
      aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
    profiling:
      enabled: false             # enable for row/column stats
    max_rows: 100                # rows sampled for schema inference

sink:
  type: datahub-rest
  config:
    server: "http://datahub-gms:8080"
```

---

## Metadata Entities and URN Format

### Entity Types

| Entity | URN Pattern | Description |
|--------|-------------|-------------|
| **Dataset** | `urn:li:dataset:(urn:li:dataPlatform:<platform>,<name>,<env>)` | Table, view, stream, file |
| **DataFlow** | `urn:li:dataFlow:(airflow,<dag_id>,<env>)` | Airflow DAG / pipeline |
| **DataJob** | `urn:li:dataJob:(urn:li:dataFlow:(...),<task_id>)` | Airflow task / Spark job |
| **Dashboard** | `urn:li:dashboard:(<tool>,<id>)` | Superset / Mode dashboard |
| **Chart** | `urn:li:chart:(<tool>,<id>)` | Individual visualization |
| **CorpUser** | `urn:li:corpuser:<username>` | Human user |
| **CorpGroup** | `urn:li:corpGroup:<group_name>` | Team or group |
| **Tag** | `urn:li:tag:<tag_name>` | Free-form tag |
| **GlossaryTerm** | `urn:li:glossaryTerm:<node_path>` | Business glossary term |
| **Container** | `urn:li:container:<hash>` | Database / schema / bucket |

### URN Examples

```
urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)
urn:li:dataset:(urn:li:dataPlatform:hive,gold.daily_revenue,PROD)
urn:li:dataset:(urn:li:dataPlatform:kafka,prod.events.page_view,PROD)
urn:li:dataFlow:(airflow,daily_etl,PROD)
urn:li:dataJob:(urn:li:dataFlow:(airflow,daily_etl,PROD),transform_orders)
```

### Common Aspects per Entity

| Aspect | Applies To | Purpose |
|--------|-----------|---------|
| `datasetProperties` | Dataset | Description, custom properties, tags |
| `schemaMetadata` | Dataset | Column names, types, field descriptions |
| `ownership` | All | Owners with role (DATAOWNER, PRODUCER, CONSUMER) |
| `globalTags` | All | Free-form tag associations |
| `glossaryTerms` | All | Business glossary term associations |
| `upstreamLineage` | Dataset | Table-level + column-level upstream datasets |
| `dataJobInputOutput` | DataJob | Input/output datasets for a job |
| `institutionalMemory` | All | Links to wikis/runbooks |
| `domains` | All | Business domain classification |

---

## Programmatic Ingestion — Python SDK

### Installation and Emitter Setup

```python
pip install 'acryl-datahub[datahub-rest]'
```

```python
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
import datahub.metadata.schema_classes as models

emitter = DatahubRestEmitter(
    gms_server="http://datahub-gms:8080",
    token="<personal-access-token>",   # omit for unauthenticated local dev
    extra_headers={},
    connect_timeout_sec=10,
    read_timeout_sec=30,
)
emitter.test_connection()              # raises on failure
```

### Emit a Dataset with Schema

```python
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaMetadata, SchemaField, SchemaFieldDataType,
    StringTypeClass, LongTypeClass, DateTypeClass,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)"

# Properties aspect
props_mcp = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=models.DatasetPropertiesClass(
        description="Daily order transactions from the OLTP system.",
        customProperties={
            "team": "data-platform",
            "sla": "daily-6am-utc",
        },
    ),
)

# Schema aspect
schema_mcp = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=models.SchemaMetadataClass(
        schemaName="public.orders",
        platform="urn:li:dataPlatform:postgres",
        version=0,
        hash="",
        platformSchema=models.OtherSchemaClass(rawSchema=""),
        fields=[
            models.SchemaFieldClass(
                fieldPath="order_id",
                type=models.SchemaFieldDataTypeClass(type=models.LongTypeClass()),
                nativeDataType="bigint",
                description="Primary key.",
            ),
            models.SchemaFieldClass(
                fieldPath="customer_id",
                type=models.SchemaFieldDataTypeClass(type=models.LongTypeClass()),
                nativeDataType="bigint",
                description="FK to customers table.",
            ),
            models.SchemaFieldClass(
                fieldPath="order_date",
                type=models.SchemaFieldDataTypeClass(type=models.DateTypeClass()),
                nativeDataType="date",
            ),
            models.SchemaFieldClass(
                fieldPath="total_amount",
                type=models.SchemaFieldDataTypeClass(type=models.NumberTypeClass()),
                nativeDataType="numeric(12,2)",
            ),
            models.SchemaFieldClass(
                fieldPath="status",
                type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                nativeDataType="varchar(32)",
                tags=models.GlobalTagsClass(
                    tags=[models.TagAssociationClass("urn:li:tag:pii")]
                ),
            ),
        ],
    ),
)

# Ownership aspect
ownership_mcp = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=models.OwnershipClass(
        owners=[
            models.OwnerClass(
                owner="urn:li:corpuser:jane.doe",
                type=models.OwnershipTypeClass.DATAOWNER,
            ),
            models.OwnerClass(
                owner="urn:li:corpGroup:data-platform",
                type=models.OwnershipTypeClass.PRODUCER,
            ),
        ]
    ),
)

# Tags aspect
tags_mcp = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=models.GlobalTagsClass(
        tags=[
            models.TagAssociationClass("urn:li:tag:finance"),
            models.TagAssociationClass("urn:li:tag:daily-ingestion"),
        ]
    ),
)

# Glossary terms aspect
terms_mcp = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=models.GlossaryTermsClass(
        terms=[
            models.GlossaryTermAssociationClass("urn:li:glossaryTerm:Revenue.OrderRevenue"),
        ],
        auditStamp=models.AuditStampClass(
            time=int(time.time() * 1000),
            actor="urn:li:corpuser:datahub",
        ),
    ),
)

# Emit all aspects
with emitter:
    for mcp in [props_mcp, schema_mcp, ownership_mcp, tags_mcp, terms_mcp]:
        emitter.emit(mcp)
```

### Emit Table-Level Dataset Lineage

```python
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    UpstreamLineage, Upstream,
)

downstream_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,gold.daily_revenue,PROD)"
upstream_urn   = "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)"

lineage_mcp = MetadataChangeProposalWrapper(
    entityUrn=downstream_urn,
    aspect=models.UpstreamLineageClass(
        upstreams=[
            models.UpstreamClass(
                dataset=upstream_urn,
                type=models.DatasetLineageTypeClass.TRANSFORMED,
            )
        ]
    ),
)

with emitter:
    emitter.emit(lineage_mcp)
```

---

## Column-Level Lineage

Use `FineGrainedLineage` to attach field-to-field mappings to the `upstreamLineage` aspect of the downstream dataset.

```python
from datahub.metadata.schema_classes import (
    UpstreamLineageClass,
    UpstreamClass,
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageUpstreamTypeClass,
    FineGrainedLineageDownstreamTypeClass,
)

downstream_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,gold.daily_revenue,PROD)"
upstream_urn   = "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)"

def field_urn(dataset_urn: str, field: str) -> str:
    return f"urn:li:schemaField:({dataset_urn},{field})"

lineage_mcp = MetadataChangeProposalWrapper(
    entityUrn=downstream_urn,
    aspect=UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=upstream_urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
        ],
        fineGrainedLineages=[
            # order_date → order_date (pass-through)
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[field_urn(upstream_urn, "order_date")],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[field_urn(downstream_urn, "order_date")],
                confidenceScore=1.0,
            ),
            # total_amount → revenue (aggregated)
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[field_urn(upstream_urn, "total_amount")],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[field_urn(downstream_urn, "revenue")],
                confidenceScore=0.9,
                transformOperation="SUM",
            ),
        ],
    ),
)

with emitter:
    emitter.emit(lineage_mcp)
```

### Column Lineage via DataJob (Recommended for ETL Jobs)

Attaching column lineage to a `DataJob` provides richer context (which job produced the lineage) and avoids overwriting dataset-level aspects on re-emission:

```python
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    FineGrainedLineageUpstreamTypeClass,
    FineGrainedLineageDownstreamTypeClass,
)

job_urn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,daily_etl,PROD),transform_orders)"

job_io_mcp = MetadataChangeProposalWrapper(
    entityUrn=job_urn,
    aspect=DataJobInputOutputClass(
        inputDatasets=[upstream_urn],
        outputDatasets=[downstream_urn],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[field_urn(upstream_urn, "order_date")],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[field_urn(downstream_urn, "order_date")],
                confidenceScore=1.0,
            ),
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[field_urn(upstream_urn, "total_amount")],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[field_urn(downstream_urn, "revenue")],
                confidenceScore=0.9,
            ),
        ],
    ),
)

with emitter:
    emitter.emit(job_io_mcp)
```

---

## Search and Discovery

### UI Search

- Full-text search across entity names, descriptions, field names, and custom properties
- Filter by platform, environment (PROD/DEV/STAGING), entity type, owner, tag, domain
- Lineage graph view: upstream/downstream traversal with hop-count control

### REST API (OpenAPI)

```bash
# Search datasets by keyword
curl -s \
  -H "Authorization: Bearer ${DATAHUB_TOKEN}" \
  "http://datahub-gms:8080/entities?action=search" \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "input": "orders",
    "entity": "dataset",
    "start": 0,
    "count": 10
  }' | jq '.value.entities[].entity.urn'

# Get entity aspects
curl -s \
  -H "Authorization: Bearer ${DATAHUB_TOKEN}" \
  "http://datahub-gms:8080/entities/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Apostgres%2Canalytics.public.orders%2CPROD%29" \
  | jq '.aspects'
```

### GraphQL API

DataHub exposes a full GraphQL API at `http://datahub-frontend:9002/api/graphql`.

```graphql
# Search datasets
query SearchDatasets {
  search(input: {
    type: DATASET
    query: "orders"
    start: 0
    count: 10
    filters: [
      { field: "platform", value: "postgres" }
      { field: "env", value: "PROD" }
    ]
  }) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
        ... on Dataset {
          name
          description
          platform { name }
          ownership {
            owners {
              owner { urn }
              type
            }
          }
          tags {
            tags { tag { name } }
          }
        }
      }
    }
  }
}
```

```graphql
# Traverse lineage graph downstream from a dataset
query GetLineage {
  searchAcrossLineage(input: {
    urn: "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)"
    direction: DOWNSTREAM
    start: 0
    count: 100
    orFilters: [
      {
        and: [
          { field: "degree", values: ["1", "2", "3"], condition: EQUAL }
        ]
      }
    ]
  }) {
    searchResults {
      degree
      entity {
        urn
        type
        ... on Dataset { name platform { name } }
        ... on DataJob  { jobId dataFlow { flowId } }
      }
    }
  }
}
```

```python
# Python: GraphQL lineage traversal
import requests

def get_downstream_lineage(dataset_urn: str, max_hops: int = 3) -> list[dict]:
    query = """
    query GetLineage($urn: String!, $count: Int!) {
      searchAcrossLineage(input: {
        urn: $urn
        direction: DOWNSTREAM
        start: 0
        count: $count
        orFilters: [{and: [{field: "degree", values: ["1","2","3"], condition: EQUAL}]}]
      }) {
        searchResults {
          degree
          entity { urn type }
        }
      }
    }
    """
    resp = requests.post(
        "http://datahub-frontend:9002/api/graphql",
        headers={
            "Authorization": f"Bearer {os.environ['DATAHUB_TOKEN']}",
            "Content-Type": "application/json",
        },
        json={"query": query, "variables": {"urn": dataset_urn, "count": 500}},
    )
    resp.raise_for_status()
    return resp.json()["data"]["searchAcrossLineage"]["searchResults"]
```

---

## dbt Integration

### Ingestion Recipe (dbt artifacts → DataHub)

Push dbt manifest + catalog after each `dbt run`:

```yaml
# dbt_ingestion.yaml
source:
  type: dbt
  config:
    manifest_path: "/dbt-project/target/manifest.json"
    catalog_path:  "/dbt-project/target/catalog.json"
    run_results_paths:
      - "/dbt-project/target/run_results.json"
    target_platform: trino
    environment: PROD
    include_column_lineage: true
    # Map dbt model meta to DataHub ownership
    meta_mapping:
      owner:
        match: ".*"
        operation: "add_owner"
        config:
          owner_type: corpuser

sink:
  type: datahub-rest
  config:
    server: "http://datahub-gms:8080"
    token: "${DATAHUB_TOKEN}"
```

### Airflow DAG Triggering dbt + DataHub Push

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datahub_airflow_plugin.operators.datahub import DatahubEmitterOperator
from datetime import datetime

with DAG("dbt_with_datahub", start_date=datetime(2024, 1, 1), schedule="@daily") as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /dbt-project && dbt run --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /dbt-project && dbt test --target prod",
    )

    push_to_datahub = BashOperator(
        task_id="push_to_datahub",
        bash_command=(
            "datahub ingest -c /recipes/dbt_ingestion.yaml "
            "--pipeline-name dbt_daily"
        ),
        env={"DATAHUB_TOKEN": "{{ var.value.datahub_token }}"},
    )

    dbt_run >> dbt_test >> push_to_datahub
```

### datahub-airflow-plugin (Automatic Pipeline Lineage)

```bash
pip install 'acryl-datahub[airflow]'
```

Add to `airflow.cfg`:

```ini
[datahub]
enabled = true
conn_id = datahub_rest_default
# Captures: every DAG as DataFlow, every task as DataJob
# inlet/outlet annotations on tasks become dataset lineage edges
```

Create Airflow connection (`datahub_rest_default`):
- Connection Type: `datahub_rest`
- Host: `http://datahub-gms:8080`
- Extra: `{"token": "<PAT>", "timeout_sec": 10}`

---

## CLI Operations

### Setup

```bash
pip install 'acryl-datahub[datahub-rest]'
datahub init    # writes ~/.datahubenv with server URL and token

# Or via environment variable
export DATAHUB_GMS_URL=http://datahub-gms:8080
export DATAHUB_GMS_TOKEN=<personal-access-token>
```

### datahub ingest

```bash
# Run a recipe file
datahub ingest -c recipe.yaml

# Dry run (validate recipe without emitting)
datahub ingest -c recipe.yaml --dry-run

# List all past ingestion runs
datahub ingest list-runs --page-size 20

# Roll back a specific run (soft-deletes ingested metadata)
datahub ingest rollback --run-id <run-id>

# Report ingestion run status
datahub ingest show --run-id <run-id>
```

### datahub check

```bash
# Verify connectivity to GMS
datahub check server-config

# Validate a recipe YAML without running it
datahub check datahub-connection -c recipe.yaml

# Check docker services are healthy (local quickstart)
datahub docker check
```

### datahub delete

```bash
# Soft-delete a single entity (hidden in UI, physically retained)
datahub delete \
  --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"

# Hard-delete (physically removes all aspects — irreversible)
datahub delete \
  --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)" \
  --hard

# Delete all datasets from a platform (dry-run first)
datahub delete --platform hive --entity-type dataset --dry-run
datahub delete --platform hive --entity-type dataset --force

# Delete a container and all its children recursively
datahub delete \
  --urn "urn:li:container:77644901c4f574845578ebd18b7c14fa" \
  --recursive

# Delete a specific aspect (not the whole entity)
datahub delete \
  --urn "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)" \
  --aspect upstreamLineage
```

### datahub timeline

```bash
# View the change history for an entity
datahub timeline \
  --urn "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)" \
  --category TECHNICAL_SCHEMA \
  --start 7daysago

# All change categories for the last 24 hours
datahub timeline \
  --urn "urn:li:dataset:(urn:li:dataPlatform:hive,gold.daily_revenue,PROD)" \
  --start 24hoursago

# Available categories:
# TECHNICAL_SCHEMA, DOCUMENTATION, OWNERSHIP, TAG, GLOSSARY_TERM, LIFECYCLE
```

### datahub dataset (YAML-based Entity Management)

```bash
# Upsert a dataset and its aspects from a YAML descriptor
datahub dataset upsert -f dataset_descriptor.yaml
```

```yaml
# dataset_descriptor.yaml
- id: "analytics.public.orders"
  platform: postgres
  env: PROD
  description: "Transactional order data."
  schema:
    file: schema.json          # SchemaMetadata JSON
  owners:
    - id: jane.doe
      type: DATAOWNER
  tags:
    - finance
    - pii
  glossaryTerms:
    - Revenue.OrderRevenue
```

### Useful Utilities

```bash
# Get a URN for a dataset by name
datahub get \
  --urn "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)"

# Emit an MCP JSON file directly
datahub put \
  --urn "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)" \
  --aspect datasetProperties \
  -d '{"description": "Updated description."}'

# Retrieve all aspects of an entity as JSON
datahub get \
  --urn "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)" \
  --aspect schemaMetadata
```

---

## Transformers

Transformers mutate MCPs in-flight during ingestion — use them to add ownership, tags, or glossary terms without modifying the source recipe:

```yaml
source:
  type: postgres
  config:
    host_port: "prod-postgres:5432"
    database: analytics
    username: datahub_reader
    password: "${POSTGRES_PASSWORD}"

transformers:
  # Assign owner to all datasets ingested
  - type: simple_add_dataset_ownership
    config:
      owner_urns:
        - "urn:li:corpuser:data-platform-team"

  # Tag all datasets containing "pii" in their name
  - type: pattern_add_dataset_tags
    config:
      tag_pattern:
        rules:
          ".*pii.*": ["urn:li:tag:PII", "urn:li:tag:Sensitive"]

  # Attach glossary terms based on field name patterns
  - type: pattern_add_dataset_schema_terms
    config:
      term_pattern:
        rules:
          ".*email.*": ["urn:li:glossaryTerm:PersonalData.Email"]
          ".*revenue.*": ["urn:li:glossaryTerm:Finance.Revenue"]

  # Add domain based on database name
  - type: simple_add_dataset_domain
    config:
      semantics: OVERWRITE
      domain_urn: "urn:li:domain:Finance"

sink:
  type: datahub-rest
  config:
    server: "http://datahub-gms:8080"
```

---

## Anti-Patterns

1. **Hard-coding GMS URLs and tokens in recipe files** — use environment variable substitution (`${DATAHUB_TOKEN}`) and inject secrets at runtime. Never commit tokens to source control.

2. **Running `datahub delete --hard` without `--dry-run` first** — hard deletion is permanent and affects all aspects. Always preview with `--dry-run` and target specific URNs rather than broad platform filters.

3. **Emitting schema and lineage without ensuring the entity exists first** — aspects are attached to an URN; if the Dataset entity does not exist, some aspects (e.g., `upstreamLineage`) are silently dropped. Emit `datasetProperties` (or trigger a source ingestion) before relationship aspects.

4. **Using MCE (MetadataChangeEvent) instead of MCP for new integrations** — MCE is the legacy format and is deprecated in v0.9+. Use `MetadataChangeProposalWrapper` with `DatahubRestEmitter` or the Kafka sink for all new programmatic ingestion.

5. **Attaching column-level lineage only to the Dataset `upstreamLineage` aspect when an ETL job exists** — prefer the `dataJobInputOutput` aspect on the `DataJob` entity; this preserves the association between the transformation logic and the lineage, and prevents overwrites when the dataset is re-ingested from a source connector.

6. **Ignoring `stateful_ingestion`** — without `stateful_ingestion.enabled: true` and `remove_stale_metadata: true`, deleted tables and dropped columns accumulate as ghost entities in DataHub indefinitely.

7. **Skipping `target_platform` in dbt recipes** — DataHub uses `target_platform` to link dbt models to their physical datasets on the underlying warehouse. Omitting it breaks lineage stitching between dbt models and source/downstream datasets.

8. **Using `datahub-kafka` sink in environments where Kafka is unavailable** — the Kafka sink requires the MCE Consumer service to be running. For direct writes, always use `datahub-rest`. Use Kafka sink only when throughput justifies the operational overhead.

9. **Not scoping ingestion with `table_pattern` / `schema_pattern` allows** — ingesting an entire warehouse on every run is slow and inflates Elasticsearch. Scope recipes to the databases and schemas your team owns.

10. **Conflating DataFlow and DataJob URNs** — a DataFlow is the DAG/pipeline; a DataJob is an individual task within it. DataJob URNs nest inside DataFlow URNs. Mixing them up breaks the pipeline view in the UI.

---

## References to Consult When Needed

- DataHub architecture: `docs.datahub.com/docs/architecture/architecture`
- MCP/MCL event spec: `docs.datahub.com/docs/advanced/mcp-mcl`
- Python SDK / REST emitter: `docs.datahub.com/docs/metadata-ingestion/as-a-library`
- Ingestion sources index: `docs.datahub.com/docs/metadata-ingestion`
- Lineage tutorial: `docs.datahub.com/docs/api/tutorials/lineage`
- dbt source: `docs.datahub.com/docs/generated/ingestion/sources/dbt`
- Airflow integration: `docs.datahub.com/docs/lineage/airflow`
- Airflow plugin: `docs.datahub.com/docs/metadata-ingestion-modules/airflow-plugin`
- Kubernetes Helm: `docs.datahub.com/docs/deploy/kubernetes`
- Helm chart repo: `github.com/acryldata/datahub-helm`
- Delete metadata: `docs.datahub.com/docs/how/delete-metadata`
- CLI reference: `docs.datahub.com/docs/cli`
- GraphQL API: `docs.datahub.com/docs/api/graphql/overview`
- Metadata model: `docs.datahub.com/docs/metadata-modeling/metadata-model`
- Fine-grained lineage example: `github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_emitter_dataset_finegrained.py`
