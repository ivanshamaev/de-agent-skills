---
name: data-contracts
description: Data Contracts — datacontract.com YAML specification (schema, quality, SLA, ownership, servers, changelog), Data Contract CLI (init/test/lint/diff/breaking/publish/export), SodaCL-style quality checks embedded in contract, breaking change detection (column removal/type change/constraint tightening), CI/CD GitHub Actions PR enforcement, Kafka/S3/database server definitions, contract-driven development (producer-first), semver versioning and deprecation, DataHub and OpenMetadata catalog publishing
---

# Data Contracts

## When to Use

Load this skill when the user needs to:
- Author or review a `datacontract.yaml` file following the datacontract.com specification
- Run `datacontract` CLI commands (init, test, lint, diff, breaking, publish, export, catalog)
- Embed SodaCL or custom SQL quality checks inside a contract
- Detect breaking changes between contract versions in PRs
- Set up GitHub Actions CI/CD to enforce contracts automatically
- Define server blocks for Kafka topics, S3 buckets, or database tables
- Follow the contract-driven development pattern (producer writes contract first)
- Apply semver versioning and deprecation notices to contracts
- Publish contracts to DataHub or OpenMetadata for catalog governance

---

## Core Concepts

```
datacontract.yaml
  ├── info          — id, title, version, owner, contact, description, status
  ├── servers       — where the data lives (Kafka, S3, Postgres, Snowflake, …)
  ├── terms         — usage/access conditions, billing, retention
  ├── models        — logical schema: tables → fields (type, constraints, PII, examples)
  ├── quality       — SodaCL checks or custom SQL assertions
  ├── servicelevels — SLA commitments (freshness, availability, latency, retention)
  └── info.links    — external docs, data catalog URLs, Slack channels
```

---

## Installation

```bash
pip install datacontract-cli              # latest stable

# or with extras for specific data sources
pip install "datacontract-cli[s3]"        # AWS S3 / Parquet
pip install "datacontract-cli[kafka]"     # Apache Kafka
pip install "datacontract-cli[postgres]"  # PostgreSQL / Trino / Vertica
pip install "datacontract-cli[all]"       # all extras
```

---

## Complete Contract YAML Example

```yaml
# datacontract.yaml  — datacontract.com specification v0.9.3
dataContractSpecification: 0.9.3

# ─── Identity & Ownership ───────────────────────────────────────────────────
id: urn:datacontract:orders:v2
info:
  title: Orders Domain — Enriched Orders
  version: 2.1.0                         # semver for THIS contract document
  status: active                         # draft | active | deprecated | retired
  description: |
    Enriched order events produced by the Orders microservice after payment
    gateway confirmation. One record per order state transition.
  owner: team-orders                     # team slug or email group
  contact:
    name: Orders Platform Team
    url: https://wiki.example.com/orders
    email: orders-platform@example.com
  links:
    data_catalog: https://datahub.example.com/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,orders.enriched,PROD)
    runbook: https://wiki.example.com/orders/runbook
    slack: https://example.slack.com/archives/C0ORDERS

# ─── Terms of Use ────────────────────────────────────────────────────────────
terms:
  usage: |
    Data may be used for operational reporting and analytics. Re-publication
    to external systems requires written approval from the Orders team.
  limitations: |
    PII fields must not be exposed in dashboards without masking. See
    https://wiki.example.com/data-classification for the classification guide.
  billing: free                          # free | subscription | metered
  noticePeriod: P3M                      # ISO 8601 duration — 3-month deprecation notice

# ─── Servers (Where the Data Lives) ─────────────────────────────────────────
servers:
  production_kafka:
    type: kafka
    host: kafka.example.com:9092
    topic: orders.enriched.v2
    format: avro                         # avro | json | protobuf
    delimiter: none                      # newline | comma | none
    description: Primary Kafka topic; compacted, Avro schema in Confluent Registry
    environment: production
    roles:
      - name: read
        description: Consumer group access
      - name: write
        description: Orders service producer access

  production_s3:
    type: s3
    environment: production
    location: s3://data-lake-prod/orders/enriched/v2/
    format: parquet
    delimiter: none
    description: Daily Parquet snapshot synced from Kafka at 02:00 UTC

  analytics_postgres:
    type: postgres
    environment: production
    host: analytics-db.internal
    port: 5432
    database: analytics
    schema: orders
    description: Materialized serving layer for BI tools

# ─── Models (Schema) ─────────────────────────────────────────────────────────
models:
  enriched_orders:
    description: One row per order state transition with enriched customer data
    type: table
    fields:
      order_id:
        type: varchar
        description: Globally unique order identifier (UUID v4)
        required: true
        unique: true
        pattern: "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        example: "a3b2c1d0-e5f6-4a7b-8c9d-0e1f2a3b4c5d"
        pii: false

      customer_id:
        type: varchar
        description: Reference to the customer entity
        required: true
        references: customers.customer_id   # cross-model FK hint
        pii: false

      customer_email:
        type: varchar
        description: Customer email address — PII, must be masked in marts
        required: true
        pii: true
        dataClass: sensitive               # sensitive | restricted | internal | public
        classification: PII_EMAIL

      order_status:
        type: varchar
        description: Current state in the order lifecycle
        required: true
        enum:
          - CREATED
          - PAYMENT_PENDING
          - CONFIRMED
          - SHIPPED
          - DELIVERED
          - CANCELLED

      order_total_usd:
        type: decimal
        precision: 12
        scale: 2
        description: Total order value in USD including tax and shipping
        required: true
        minimum: 0
        example: 149.99
        pii: false

      created_at:
        type: timestamp
        description: UTC timestamp when the order was first created
        required: true
        pii: false

      updated_at:
        type: timestamp
        description: UTC timestamp of the most recent state transition
        required: true
        pii: false

      metadata:
        type: object
        description: Opaque key/value bag for source-system flags
        required: false
        pii: false
        fields:
          source_system:
            type: varchar
            description: Originating microservice name
            required: false
          feature_flags:
            type: array
            description: Active feature flags at order creation time
            required: false

# ─── Quality Checks (SodaCL) ─────────────────────────────────────────────────
quality:
  type: SodaCL                           # SodaCL | montecarlo | custom
  specification:
    checks for enriched_orders:
      # Completeness
      - row_count > 1000:
          name: Minimum daily row count
          severity: error

      - missing_count(order_id) = 0:
          name: order_id must never be null
          severity: error

      - missing_count(customer_id) = 0:
          name: customer_id must never be null
          severity: error

      - missing_percent(customer_email) < 1:
          name: Email null rate under 1 %
          severity: warning

      # Uniqueness
      - duplicate_count(order_id) = 0:
          name: order_id must be unique
          severity: error

      # Value validity
      - invalid_count(order_status) = 0:
          name: order_status must be in allowed enum values
          valid values: [CREATED, PAYMENT_PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED]
          severity: error

      - min(order_total_usd) >= 0:
          name: No negative order totals
          severity: error

      # Freshness
      - freshness(updated_at) < 2h:
          name: Data must not be more than 2 hours stale
          severity: error

      # Custom SQL check
      - failed rows:
          name: No orders with DELIVERED status and zero total
          fail query: |
            SELECT order_id
            FROM enriched_orders
            WHERE order_status = 'DELIVERED'
              AND order_total_usd = 0
          severity: warning

# ─── Service Level Agreements ────────────────────────────────────────────────
servicelevels:
  availability:
    description: Kafka topic must be available 99.9 % of the time
    percentage: 99.9%

  retention:
    description: Kafka topic retention; S3 data kept for 3 years
    period: P7D                          # Kafka: 7 days ISO 8601 duration
    unlimited: false

  latency:
    description: End-to-end latency from source event to topic
    threshold: 30s
    sourceTimestampField: enriched_orders.created_at
    processedTimestampField: enriched_orders.updated_at

  freshness:
    description: New data must arrive every hour during business hours
    threshold: 1h
    timestampField: enriched_orders.updated_at

  frequency:
    description: Batch S3 snapshot produced once per day
    type: batch
    cron: "0 2 * * *"
    tzinfo: UTC

  support:
    description: Business-hours support; critical incidents 24/7
    time: 9am to 5pm CET, Mon-Fri
    responseTime: 4h

# ─── Changelog ───────────────────────────────────────────────────────────────
# Follow semver: MAJOR.MINOR.PATCH
# MAJOR — breaking change (column removed, type narrowed, constraint added)
# MINOR — backward-compatible addition (new optional column, relaxed constraint)
# PATCH — non-schema changes (description update, SLA adjustment, contact info)
changelog:
  - version: 2.1.0
    date: 2025-11-01
    author: orders-platform@example.com
    description: |
      Added `metadata` object field (optional, non-breaking).
      Tightened freshness SLA from 4h to 2h (SLA-only change, MINOR).
  - version: 2.0.0
    date: 2025-09-15
    author: orders-platform@example.com
    description: |
      BREAKING: Removed `legacy_order_ref` (VARCHAR) — deprecated in v1.5.0.
      BREAKING: Changed `order_total` type from FLOAT to DECIMAL(12,2).
      Consumers must migrate before 2025-09-15; use v1.x for backward compat.
  - version: 1.5.0
    date: 2025-06-01
    author: orders-platform@example.com
    description: |
      Deprecated `legacy_order_ref`; will be removed in 2.0.0 (3-month notice).
      Added `customer_email` field (optional in this version).
```

---

## Data Contract CLI Commands

### Install & Bootstrap

```bash
pip install "datacontract-cli[all]"

# Scaffold a new contract from built-in template
datacontract init datacontract.yaml

# Validate the YAML structure against the JSON Schema
datacontract lint datacontract.yaml
```

### Testing Against a Live Data Source

```bash
# Test schema + quality checks (uses servers[*].type to auto-detect driver)
datacontract test datacontract.yaml

# Test only schema (skip quality checks)
datacontract test --only-schema datacontract.yaml

# Test only quality checks
datacontract test --only-quality datacontract.yaml

# Use a non-default server block
datacontract test --server production_s3 datacontract.yaml

# Test examples embedded in the contract (no live source needed)
datacontract test --examples datacontract.yaml

# Output as JSON for downstream tooling
datacontract test --format json datacontract.yaml
```

**Environment variables for credentials (never hard-code in YAML):**

```bash
# PostgreSQL
export DATACONTRACT_POSTGRES_USERNAME=svc_datacontract
export DATACONTRACT_POSTGRES_PASSWORD=secret

# AWS S3 (standard AWS env vars)
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=eu-west-1

# Kafka
export DATACONTRACT_KAFKA_SASL_USERNAME=...
export DATACONTRACT_KAFKA_SASL_PASSWORD=...
```

### Diff and Breaking Change Detection

```bash
# Show all differences between two contract files
datacontract diff datacontract-v1.yaml datacontract-v2.yaml

# Exit non-zero if any breaking changes exist (use in CI)
datacontract breaking datacontract-v1.yaml datacontract-v2.yaml
```

**What `datacontract breaking` flags as breaking changes:**

| Change | Breaking? | Reason |
|--------|-----------|--------|
| Remove a field | YES | Consumers reading that field will fail |
| Rename a field | YES | Treated as remove + add |
| Narrow a type (varchar→int, decimal precision reduction) | YES | Existing data may not cast |
| Make a nullable field `required: true` | YES | Existing NULLs become invalid |
| Add a new `enum` value | NO | Additive; consumers ignore unknown values |
| Remove an `enum` value | YES | Existing data containing that value becomes invalid |
| Add a new optional field | NO | Backward-compatible |
| Tighten a `minimum` or `maximum` constraint | YES | Existing data may violate it |
| Loosen a `minimum` or `maximum` constraint | NO | Backward-compatible |
| Change SLA threshold (freshness, latency) | NO | Not a schema change |
| Remove a server block | YES | Consumers depending on that server break |

### Export

```bash
# Export to multiple targets for downstream consumption
datacontract export --format html       datacontract.yaml > contract.html
datacontract export --format avro       datacontract.yaml > schema.avsc
datacontract export --format jsonschema datacontract.yaml > schema.json
datacontract export --format sql        datacontract.yaml            # CREATE TABLE DDL
datacontract export --format dbt        datacontract.yaml            # dbt schema YAML
datacontract export --format sodacl     datacontract.yaml            # standalone SodaCL file
datacontract export --format terraform  datacontract.yaml            # Terraform resource blocks
datacontract export --format bigquery   datacontract.yaml            # BigQuery schema JSON
```

### Publish & Catalog

```bash
# Publish test results to a remote URL (webhook / internal platform)
datacontract publish datacontract.yaml \
  --publish-url https://data-platform.example.com/api/contracts

# Build a static HTML catalog from all contracts in a directory
datacontract catalog --files "contracts/**/*.yaml" --output ./catalog-site
```

### CI Command (GitHub Actions / Azure DevOps optimized)

```bash
# Wraps `test` with inline annotations and Markdown step-summary output
datacontract ci datacontract.yaml

# Run against all contracts in repo
datacontract ci contracts/*.yaml --fail-on warning --format json
```

---

## Server Block Reference

### Kafka

```yaml
servers:
  prod_kafka:
    type: kafka
    host: kafka.example.com:9092           # broker(s), comma-separated for multiple
    topic: orders.enriched.v2
    format: avro                           # avro | json | protobuf | csv
    schemaRegistryUrl: https://schema-registry.example.com
    partitions: 12
    replicationFactor: 3
    environment: production
```

### AWS S3 / Parquet

```yaml
servers:
  prod_s3:
    type: s3
    environment: production
    location: s3://data-lake-prod/orders/enriched/v2/dt={date}/
    format: parquet
    delimiter: none                        # row delimiter for line-delimited formats
    endpointUrl: https://s3.amazonaws.com  # override for MinIO or S3-compatible
    roles:
      - name: read
        description: arn:aws:iam::123456789:role/DataContractReadRole
```

### PostgreSQL / Trino / Vertica

```yaml
servers:
  analytics_postgres:
    type: postgres
    environment: production
    host: analytics-db.internal
    port: 5432
    database: analytics
    schema: orders

  trino_iceberg:
    type: trino
    environment: production
    host: trino.example.com
    port: 443
    catalog: iceberg
    schema: orders_enriched
```

### Snowflake / BigQuery / Databricks

```yaml
servers:
  snowflake_prod:
    type: snowflake
    environment: production
    account: xy12345.eu-west-1
    database: ANALYTICS
    schema: ORDERS

  bigquery_prod:
    type: bigquery
    environment: production
    project: my-gcp-project
    dataset: orders_enriched
```

---

## Quality Checks — SodaCL Embedded in Contract

The `quality.specification` block mirrors standard SodaCL syntax. The CLI
translates it to a Soda scan at runtime.

```yaml
quality:
  type: SodaCL
  specification:
    checks for orders_daily_snapshot:
      # Row count with warn threshold
      - row_count:
          warn: when < 500
          fail: when < 100

      # Cross-table referential integrity via custom SQL
      - failed rows:
          name: All customer_ids must exist in the customers table
          fail query: |
            SELECT o.order_id
            FROM   orders_daily_snapshot o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE  c.customer_id IS NULL
          severity: error

      # Schema check (column existence)
      - schema:
          fail:
            when required column missing: [order_id, customer_id, order_status]
            when wrong column type:
              order_total_usd: decimal

      # Statistical distribution guard
      - avg(order_total_usd) between 20 and 500:
          name: Average order total sanity check
          severity: warning
```

---

## Breaking Change Detection Workflow

### Local Development

```bash
# Before committing: compare working version against main branch version
git show origin/main:contracts/orders.yaml > /tmp/orders_main.yaml
datacontract diff /tmp/orders_main.yaml contracts/orders.yaml
datacontract breaking /tmp/orders_main.yaml contracts/orders.yaml
```

### Deprecation Pattern Before a Breaking Change

```yaml
# Step 1 — v1.5.0: add deprecation notice, ship new field in parallel
models:
  enriched_orders:
    fields:
      legacy_order_ref:
        type: varchar
        description: |
          DEPRECATED as of v1.5.0. Will be removed in v2.0.0 (2025-09-15).
          Use `order_id` instead.
        required: false        # make optional immediately for a safe transition
        deprecated: true

      order_id:
        type: varchar
        description: Replacement for legacy_order_ref. Use this field.
        required: true
```

```yaml
# Step 2 — after noticePeriod (3 months) — v2.0.0: remove legacy_order_ref
# Bump MAJOR version; record in changelog
info:
  version: 2.0.0

changelog:
  - version: 2.0.0
    date: 2025-09-15
    description: "BREAKING: Removed legacy_order_ref. Deprecated since v1.5.0."
```

---

## CI/CD: GitHub Actions Workflow

```yaml
# .github/workflows/data-contract.yml
name: Data Contract CI

on:
  pull_request:
    paths:
      - 'contracts/**/*.yaml'
      - 'contracts/**/*.yml'

jobs:
  lint:
    name: Lint contracts
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install datacontract-cli
        run: pip install "datacontract-cli[all]"

      - name: Lint all changed contracts
        run: |
          git diff --name-only origin/${{ github.base_ref }}...HEAD \
            | grep -E 'contracts/.*\.ya?ml$' \
            | xargs -I{} datacontract lint {}

  breaking-changes:
    name: Detect breaking changes
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0          # full history needed to access base branch

      - name: Install datacontract-cli
        run: pip install "datacontract-cli[all]"

      - name: Check for breaking changes in modified contracts
        run: |
          CHANGED=$(git diff --name-only origin/${{ github.base_ref }}...HEAD \
                    | grep -E 'contracts/.*\.ya?ml$')
          for contract in $CHANGED; do
            BASE_REF="origin/${{ github.base_ref }}:${contract}"
            if git cat-file -e "${BASE_REF}" 2>/dev/null; then
              git show "${BASE_REF}" > /tmp/contract_base.yaml
              echo "=== Breaking change check: $contract ==="
              datacontract breaking /tmp/contract_base.yaml "$contract"
            else
              echo "$contract is new — skipping breaking change check"
            fi
          done

  test:
    name: Test contract against data source
    runs-on: ubuntu-latest
    # Only run on non-draft PRs targeting main to avoid hitting prod on every commit
    if: github.event.pull_request.draft == false
    env:
      DATACONTRACT_POSTGRES_USERNAME: ${{ secrets.DC_POSTGRES_USER }}
      DATACONTRACT_POSTGRES_PASSWORD: ${{ secrets.DC_POSTGRES_PASSWORD }}
    steps:
      - uses: actions/checkout@v4

      - name: Install datacontract-cli
        run: pip install "datacontract-cli[postgres]"

      - name: Run contract tests (schema + quality)
        run: |
          datacontract ci contracts/orders/datacontract.yaml \
            --server analytics_postgres \
            --fail-on error \
            --format json | tee test-results.json

      - name: Upload test results
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: contract-test-results
          path: test-results.json
```

---

## Contract-Driven Development Pattern

The pattern places the contract as the **source of truth** that drives both
producer and consumer development.

```
                 ┌──────────────────────────────────┐
 PRODUCER TEAM   │  1. Write datacontract.yaml       │
                 │     (schema, quality, SLA, servers)│
                 │  2. Review with consumer teams    │
                 │  3. Merge to main (CI lint passes) │
                 └─────────────┬────────────────────┘
                               │  contract is the spec
           ┌───────────────────▼───────────────────┐
           │           datacontract.yaml            │
           │      (versioned in git, single source) │
           └──────┬──────────────────────┬──────────┘
                  │                      │
   ┌──────────────▼──────┐   ┌──────────▼──────────────┐
   │  PRODUCER            │   │  CONSUMER TEAMS          │
   │  - Implement pipeline│   │  - Read contract schema  │
   │  - Run test against  │   │  - Validate their code   │
   │    live output       │   │    against contract      │
   │  - Block deploy on   │   │  - Subscribe to changelog│
   │    test failure      │   │  - Request changes via PR│
   └─────────────────────┘   └─────────────────────────┘
```

**Workflow rules:**
1. Producer team owns the contract file; changes go through PR review.
2. Consumer teams are **required reviewers** for any PR that increments MAJOR version.
3. New fields may be added (MINOR) without consumer approval; removals require it.
4. Contract is tested against the actual data source in CI before merge.
5. Deployment of the pipeline is blocked if `datacontract test` exits non-zero.

---

## Versioning Strategy (SemVer)

```
MAJOR.MINOR.PATCH
  │      │     └── Non-schema: description edits, SLA tweaks, contact updates
  │      └──────── Backward-compatible: new optional field, loosened constraint,
  │                new server block, new quality check added
  └─────────────── Breaking: field removal/rename, type narrowing, new required
                   field, enum value removal, server block removal
```

**Deprecation timeline:**
- Announce removal in MINOR release: set `deprecated: true`, update `description`, add changelog entry.
- Honor `noticePeriod` (ISO 8601 duration, e.g., `P3M` = 3 months) before breaking change.
- Cut MAJOR release after notice period; remove deprecated fields.
- Maintain a `v1.x` branch of the contract file for consumers who cannot migrate immediately.

---

## DataHub Integration

```bash
# Export contract to DataHub-compatible YAML assertion format
datacontract export --format datahub datacontract.yaml > datahub-assertions.yaml

# Publish assertions to DataHub via datahub CLI
pip install acryl-datahub
datahub datacontract upsert -f datahub-assertions.yaml

# Or use DataHub REST API directly
curl -X POST https://datahub.example.com/openapi/v3/entity/datacontract \
     -H "Authorization: Bearer ${DATAHUB_TOKEN}" \
     -H "Content-Type: application/json" \
     -d @datahub-contract-payload.json
```

DataHub maps contract fields to:
- `schemaAssertions` — driven by `models` section
- `freshnessAssertions` — driven by `servicelevels.freshness`
- `dataQualityAssertions` — driven by `quality` section

---

## OpenMetadata Integration

```bash
# Export contract as ODCS (Open Data Contract Standard) for OpenMetadata import
datacontract export --format odcs datacontract.yaml > odcs-export.yaml

# Upload via OpenMetadata REST API
curl -X POST https://openmetadata.example.com/api/v1/dataContracts \
     -H "Authorization: Bearer ${OM_TOKEN}" \
     -H "Content-Type: application/json" \
     -d @odcs-export.json

# Python SDK — export ODCS from OpenMetadata
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from openmetadata.contracts import DataContracts

client = OpenMetadata(server_config)
DataContracts.export_odcs(contract_id="<uuid>").execute(client)
```

OpenMetadata renders contracts in the **Data Contract** tab of each table asset,
showing schema assertions, quality rules, and SLA breaches inline with lineage.

---

## Anti-Patterns

| Anti-pattern | Problem | Fix |
|---|---|---|
| Hard-coding credentials in `servers` block | Secrets leak into git history | Always use environment variables (`DATACONTRACT_*` or AWS/GCP standard env vars) |
| Skipping `datacontract breaking` in CI | Breaking changes merged silently, consumers break in prod | Make `datacontract breaking` a required status check on the main branch |
| Using `version: 1.0.0` forever | No consumer can track changes; changelog useless | Increment semver on every meaningful change |
| Omitting `deprecated: true` before removal | Consumers have no advance warning | Deprecate in a MINOR release; remove only in MAJOR after notice period |
| Writing quality checks only in SodaCL files, not in contract | Contract and tests diverge | Embed checks in `quality.specification`; export SodaCL file from contract |
| One monolithic contract for an entire platform | Too broad to own, too large to review | One contract per logical data product / domain entity |
| Putting PII fields without `pii: true` and `dataClass` | Governance tools cannot detect sensitive data | Always mark PII fields explicitly |
| Testing only in dev against sample data | Production schema drift goes undetected | Run `datacontract test` against prod (or a production-mirror) in CI |
| No `terms.noticePeriod` defined | Producer can break consumers overnight | Set `noticePeriod: P3M` (or appropriate ISO 8601 duration) in all active contracts |

---

## References to Consult When Needed

- **Data Contract Specification** (datacontract.com): https://datacontract-specification.com/
- **Data Contract CLI docs**: https://cli.datacontract.com/
- **CLI GitHub repository**: https://github.com/datacontract/datacontract-cli
- **Specification GitHub repository**: https://github.com/datacontract/datacontract-specification
- **Open Data Contract Standard (ODCS v3)**: https://bitol-io.github.io/open-data-contract-standard/
- **DataHub Data Contracts**: https://docs.datahub.com/docs/managed-datahub/observe/data-contract
- **OpenMetadata Data Contracts**: https://docs.open-metadata.org/v1.11.x/how-to-guides/data-contracts/spec
- **SodaCL reference** (for `quality.specification` authoring): https://docs.soda.io/soda-cl/soda-cl-overview.html
- **datacontract-action** (official GitHub Action): https://github.com/datacontract/datacontract-action
