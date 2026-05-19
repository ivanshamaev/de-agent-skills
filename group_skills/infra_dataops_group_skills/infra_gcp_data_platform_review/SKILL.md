---
name: infra-gcp-data-platform-review
description: GCP data platform review — BigQuery (dataset IAM/column-level security/partitioned tables/clustering/reservations), GCS data lake (uniform bucket-level access/lifecycle/CMEK), Dataproc vs Dataflow vs Spark on GKE, Pub/Sub streaming, Cloud Composer (managed Airflow), Workload Identity for GKE, VPC Service Controls, Data Catalog, Dataplex for data governance, BigQuery cost optimization (slot reservations vs on-demand), Cloud Logging/Monitoring
---

# GCP Data Platform Review

## When to Use

- Designing or reviewing a GCP-native data platform
- Auditing BigQuery access controls and costs
- Choosing between GCP managed services vs self-managed
- Setting up Workload Identity for GKE workloads
- Implementing VPC Service Controls for data exfiltration prevention

---

## GCS Data Lake Architecture

```hcl
# Terraform: GCS 3-zone data lake
resource "google_storage_bucket" "zones" {
  for_each = toset(["bronze", "silver", "gold"])
  name     = "${local.prefix}-data-lake-${each.key}"
  location = var.region
  project  = var.project_id

  uniform_bucket_level_access = true    # no per-object ACLs — IAM only
  public_access_prevention    = "enforced"

  encryption {
    default_kms_key_name = google_kms_crypto_key.bucket[each.key].id
  }

  lifecycle_rule {
    condition { age = 30 }
    action { type = "SetStorageClass"; storage_class = "NEARLINE" }
  }

  lifecycle_rule {
    condition { age = 90 }
    action { type = "SetStorageClass"; storage_class = "COLDLINE" }
  }

  lifecycle_rule {
    condition { age = 365; matches_prefix = ["bronze/"] }
    action { type = "Delete" }
  }

  versioning { enabled = true }

  labels = merge(local.common_labels, { zone = each.key })
}

# IAM: workload-specific access
resource "google_storage_bucket_iam_binding" "airflow_bronze" {
  bucket = google_storage_bucket.zones["bronze"].name
  role   = "roles/storage.objectCreator"
  members = [
    "serviceAccount:${google_service_account.airflow_worker.email}"
  ]
}
```

---

## BigQuery Setup and Security

```hcl
# Dataset with region and default encryption
resource "google_bigquery_dataset" "zones" {
  for_each    = toset(["bronze", "silver", "gold"])
  dataset_id  = each.key
  project     = var.project_id
  location    = var.region

  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.bq.id
  }

  # Dataset-level IAM
  access {
    role          = "roles/bigquery.dataViewer"
    user_by_email = google_service_account.dbt_prod.email
  }
  access {
    role          = "roles/bigquery.dataEditor"
    user_by_email = google_service_account.airflow_worker.email
  }
}

# Column-level security using policy tags
resource "google_data_catalog_policy_tag" "pii" {
  taxonomy     = google_data_catalog_taxonomy.sensitive.id
  display_name = "PII"
  description  = "Personally Identifiable Information"
}

resource "google_bigquery_datapolicy_data_policy" "pii_masking" {
  project          = var.project_id
  location         = var.region
  data_policy_id   = "pii-masking"
  policy_tag       = google_data_catalog_policy_tag.pii.name
  data_policy_type = "DATA_MASKING_POLICY"
  data_masking_policy {
    predefined_expression = "SHA256"
  }
}
```

### BigQuery Table Partitioning and Clustering

```sql
-- Partitioned + clustered table (reduces bytes scanned = lower cost)
CREATE TABLE IF NOT EXISTS `my-project.gold.fact_orders`
(
  order_id      STRING NOT NULL,
  customer_id   STRING,
  order_date    DATE,
  order_amount  NUMERIC,
  region        STRING
)
PARTITION BY order_date
CLUSTER BY region, customer_id
OPTIONS (
  partition_expiration_days = 1095,    -- 3 years
  require_partition_filter = TRUE       -- prevent full scans
);

-- IAM policy on specific columns (via column policy tags)
ALTER TABLE `my-project.gold.customers`
ALTER COLUMN email SET OPTIONS (
  policy_tags = '{"names": ["projects/my-project/locations/us/taxonomies/123/policyTags/456"]}'
);
```

---

## Workload Identity for GKE

```hcl
# Link Kubernetes ServiceAccount to GCP Service Account
resource "google_service_account" "airflow_worker" {
  account_id   = "${local.prefix}-airflow-worker"
  display_name = "Airflow worker service account"
  project      = var.project_id
}

resource "google_service_account_iam_binding" "airflow_workload_identity" {
  service_account_id = google_service_account.airflow_worker.name
  role               = "roles/iam.workloadIdentityUser"
  members = [
    "serviceAccount:${var.project_id}.svc.id.goog[airflow/airflow-worker]"
  ]
}

# Annotate K8s ServiceAccount
resource "kubernetes_service_account" "airflow_worker" {
  metadata {
    name      = "airflow-worker"
    namespace = "airflow"
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.airflow_worker.email
    }
  }
}
```

---

## Dataproc vs Dataflow vs Spark on GKE

| Aspect | Dataproc | Dataflow | Spark on GKE |
|--------|----------|----------|--------------|
| Engine | Spark/Hadoop | Apache Beam | Spark |
| Management | Semi-managed | Fully managed | Self-managed |
| Startup time | 2-3 min (ephemeral) | Auto | 30-60s |
| Spot support | Full (preemptible) | Auto | Full |
| Python | PySpark | Beam Python SDK | PySpark |
| Streaming | Structured Streaming | Dataflow Streaming | Structured Streaming |
| Cost model | Per VM-hour | Per DCU-hour | Per GKE node-hour |
| Best for | Large batch Spark | Beam pipelines, auto-scale | K8s platform users |

---

## Cloud Pub/Sub Streaming

```python
# Producer: publish with ordering key
from google.cloud import pubsub_v1
import json

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("my-project", "orders-raw")

def publish_order(order: dict):
    data = json.dumps(order).encode("utf-8")
    future = publisher.publish(
        topic_path,
        data=data,
        ordering_key=order["region"],           # partition by region for ordering
        partition_date=order["order_date"],      # custom attribute
    )
    return future.result()

# Consumer: push subscription to Cloud Run
# Or pull subscription with parallelism:
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path("my-project", "orders-processor")

def callback(message):
    order = json.loads(message.data.decode("utf-8"))
    process_order(order)
    message.ack()

flow_control = pubsub_v1.types.FlowControl(max_messages=100)
streaming_pull_future = subscriber.subscribe(
    subscription_path,
    callback=callback,
    flow_control=flow_control,
)
```

---

## BigQuery Cost Optimization

```sql
-- Monitor query costs
SELECT
  user_email,
  SUM(total_bytes_billed) / POW(10, 12) AS tb_billed,
  SUM(total_bytes_billed) / POW(10, 12) * 5 AS estimated_cost_usd,
  COUNT(*) AS query_count
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND statement_type = 'SELECT'
GROUP BY user_email
ORDER BY tb_billed DESC
LIMIT 20;

-- Find expensive queries
SELECT
  query,
  total_bytes_billed / POW(10, 9) AS gb_billed,
  total_bytes_billed / POW(10, 12) * 5 AS cost_usd,
  creation_time
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY total_bytes_billed DESC
LIMIT 10;
```

```hcl
# BigQuery reservations (flat-rate vs on-demand)
resource "google_bigquery_capacity_commitment" "batch_slots" {
  project      = var.project_id
  location     = "US"
  slot_count   = 500
  plan         = "MONTHLY"              # 500 slots × $20/slot/mo = $10k/mo
}

resource "google_bigquery_reservation" "batch" {
  project    = var.project_id
  location   = "US"
  name       = "batch-jobs"
  slot_capacity = 400                   # 400 of 500 slots for batch
  ignore_idle_slots = true              # use idle slots from other reservations
}
```

---

## VPC Service Controls

```hcl
# Prevent data exfiltration: only GCS/BQ access from within perimeter
resource "google_access_context_manager_service_perimeter" "data_platform" {
  parent = "accessPolicies/${var.access_policy_id}"
  name   = "accessPolicies/${var.access_policy_id}/servicePerimeters/data-platform"
  title  = "Data Platform Perimeter"

  status {
    restricted_services = [
      "storage.googleapis.com",
      "bigquery.googleapis.com",
    ]
    resources = [
      "projects/${var.project_number}"
    ]
    access_levels = [
      google_access_context_manager_access_level.corp_vpn.name
    ]
  }
}
```

---

## Review Checklist

```
[ ] GCS: uniform bucket-level access, CMEK, lifecycle rules, no public access
[ ] BigQuery: dataset IAM (no allUsers), CMEK, partition filter required
[ ] Column-level security via policy tags for PII fields
[ ] Workload Identity configured (no JSON service account keys)
[ ] GKE: private cluster, Workload Identity enabled cluster-wide
[ ] VPC Service Controls for GCS + BigQuery (prevent exfiltration)
[ ] BigQuery: partitioned + clustered tables on all large tables
[ ] Cost: require_partition_filter=TRUE on tables > 1TB
[ ] Cloud Logging sink: exported to GCS for long-term retention
[ ] Monitoring: BigQuery slot utilization, GCS storage growth
```

---

## Anti-Patterns

1. **JSON service account key files** — rotate manually, easy to leak; use Workload Identity for GKE and environment-based auth everywhere else.
2. **No `require_partition_filter`** — full table scans on petabyte tables destroy BigQuery budgets; enable for all tables > 100GB.
3. **On-demand BigQuery for heavy batch workloads** — flat-rate reservations are cheaper above ~100TB/month; analyze with `INFORMATION_SCHEMA.JOBS`.
4. **Dataproc cluster running 24/7** — Dataproc should be ephemeral (start for job, terminate after); persistent clusters waste 80%+ uptime.
5. **No VPC Service Controls** — compromised VM can exfiltrate data to external GCS; enforce VPC-SC perimeter around all data services.

---

## References

- BigQuery security: `cloud.google.com/bigquery/docs/column-level-security`
- Workload Identity: `cloud.google.com/kubernetes-engine/docs/how-to/workload-identity`
- VPC Service Controls: `cloud.google.com/vpc-service-controls/docs/`
- Dataproc: `cloud.google.com/dataproc/docs/`
- Related skills: `[[terraform-data]]`, `[[infra-kubernetes-security-audit]]`, `[[de-cost-optimization]]`
