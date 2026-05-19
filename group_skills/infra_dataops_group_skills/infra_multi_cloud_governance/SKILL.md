---
name: infra-multi-cloud-governance
description: Multi-cloud governance — cloud-agnostic data platform patterns, federated identity (OIDC/SAML between AWS/GCP/Azure), Terraform multi-cloud modules, cross-cloud data replication (S3↔GCS/Azure), unified cost management (FinOps Foundation framework), cloud-agnostic observability (OpenTelemetry), policy enforcement (OPA Gatekeeper across clouds), disaster recovery cross-cloud, vendor lock-in avoidance (open formats Iceberg/Parquet), centralized secrets management (HashiCorp Vault)
---

# Multi-Cloud Governance

## When to Use

- Operating data infrastructure across 2+ cloud providers
- Preventing vendor lock-in for a critical data platform
- Setting up unified cost visibility across AWS, GCP, and Azure
- Implementing centralized secrets management across clouds
- Designing cross-cloud disaster recovery for data pipelines

---

## Multi-Cloud Architecture Principles

```
Core principle: Use open formats and standards at every layer

Storage:     Parquet / Iceberg  (not Snowflake/BigQuery native)
Compute:     Spark / Trino      (not cloud-specific SQL engines)
Streaming:   Kafka protocol     (MSK/Event Hubs/Confluent)
Secrets:     HashiCorp Vault    (not AWS SSM / GCP Secret Manager)
IaC:         Terraform          (not CloudFormation/ARM)
Containers:  OCI/Kubernetes     (not Lambda/Cloud Functions)
Observability: OpenTelemetry   (not CloudWatch/Cloud Logging)
```

---

## HashiCorp Vault — Centralized Secrets

```hcl
# Vault deployment on Kubernetes (multi-cloud compatible)
helm upgrade --install vault hashicorp/vault \
  -n vault \
  --set server.ha.enabled=true \
  --set server.ha.replicas=3 \
  --set server.ha.raft.enabled=true \
  --set server.ha.raft.setNodeId=true \
  --set server.dataStorage.storageClass=gp3

# Configure Vault auth backends per cloud
resource "vault_auth_backend" "aws" {
  type = "aws"
}

resource "vault_aws_auth_backend_config" "main" {
  backend    = vault_auth_backend.aws.path
  access_key = var.vault_aws_access_key
  secret_key = var.vault_aws_secret_key
}

resource "vault_auth_backend" "gcp" {
  type = "gcp"
}

resource "vault_auth_backend" "azure" {
  type = "azure"
}

resource "vault_auth_backend" "kubernetes" {
  type = "kubernetes"    # for all K8s clusters across clouds
}
```

```python
# Retrieve secrets from Vault (cloud-agnostic)
import hvac
import os

def get_vault_secret(path: str, key: str) -> str:
    client = hvac.Client(
        url=os.environ["VAULT_ADDR"],
        token=os.environ.get("VAULT_TOKEN") or _get_k8s_token()
    )
    secret = client.secrets.kv.v2.read_secret_version(path=path)
    return secret["data"]["data"][key]

db_password = get_vault_secret("data-platform/production/trino", "password")
```

---

## Cross-Cloud Data Replication

### S3 → GCS (Batch Sync)

```bash
# Use Storage Transfer Service (managed) or rclone
# Terraform: GCS Transfer Job from S3
resource "google_storage_transfer_job" "s3_to_gcs" {
  description = "Daily sync S3 bronze → GCS bronze"
  project     = var.gcp_project

  transfer_spec {
    aws_s3_data_source {
      bucket_name = "my-company-bronze-prod"
      aws_access_key {
        access_key_id     = var.aws_transfer_key
        secret_access_key = var.aws_transfer_secret
      }
    }
    gcs_data_sink {
      bucket_name = "my-company-bronze-gcp"
      path        = "replicated/"
    }
    transfer_options {
      overwrite_objects_already_existing_in_sink = false
      delete_objects_from_source_after_transfer  = false
    }
  }

  schedule {
    schedule_start_date { year = 2024; month = 1; day = 1 }
    start_time_of_day { hours = 3; minutes = 0 }
  }
}
```

```bash
# rclone cross-cloud sync (works with any S3-compatible + GCS + Azure)
rclone sync \
  s3:my-company-bronze-prod/orders/ \
  gcs:my-company-bronze-gcp/orders/ \
  --transfers 32 \
  --checkers 16 \
  --s3-region us-east-1 \
  --gcs-project-number ${GCP_PROJECT_NUMBER} \
  --progress
```

### Open Table Format (Iceberg) — Cloud-Agnostic Tables

```sql
-- Iceberg table works with Trino on AWS, GCP, or Azure
-- Just change the catalog's warehouse location

-- AWS: s3://my-bucket/warehouse/
-- GCP: gs://my-bucket/warehouse/
-- Azure: abfss://container@account.dfs.core.windows.net/warehouse/

CREATE TABLE orders.fact_orders (
  order_id    VARCHAR,
  customer_id VARCHAR,
  order_date  DATE,
  amount      DECIMAL(18,2)
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['month(order_date)'],
  sorted_by = ARRAY['customer_id']
);
```

---

## Unified Cost Management (FinOps)

```python
# Aggregate costs from multiple cloud providers
import boto3                    # AWS Cost Explorer
from google.cloud import billing_v1
from azure.mgmt.costmanagement import CostManagementClient

def get_aws_costs(start: str, end: str) -> dict:
    ce = boto3.client("ce", region_name="us-east-1")
    response = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="DAILY",
        Metrics=["UnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
    )
    return {r["Keys"][0]: float(r["Total"]["UnblendedCost"]["Amount"])
            for r in response["ResultsByTime"][0]["Groups"]}

# Tag-based cost attribution (consistent tags across clouds)
REQUIRED_TAGS = {
    "AWS":   {"team": "tag:team", "project": "tag:project"},
    "GCP":   {"team": "labels.team", "project": "labels.project"},
    "Azure": {"team": "tags/team", "project": "tags/project"},
}
```

---

## Federated Identity (OIDC Cross-Cloud)

```hcl
# Allow GCP service account to assume AWS role (cross-cloud OIDC)
resource "aws_iam_role" "gcp_to_aws" {
  name = "gcp-data-pipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = "accounts.google.com"
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "accounts.google.com:sub" = var.gcp_service_account_id
        }
      }
    }]
  })
}

# GCP → AWS credential chain (no static keys)
# GCP service account obtains a token via GCP metadata server
# exchanges it for AWS STS credentials via AssumeRoleWithWebIdentity
```

---

## OPA Gatekeeper — Unified Policy Across Clusters

```yaml
# Policy: all containers must have resource limits (applies to any K8s cluster)
apiVersion: templates.gatekeeper.sh/v1
kind: ConstraintTemplate
metadata:
  name: k8srequiredlimits
spec:
  crd:
    spec:
      names:
        kind: K8sRequiredLimits
  targets:
    - target: admission.k8s.gatekeeper.sh
      rego: |
        package k8srequiredlimits
        violation[{"msg": msg}] {
          container := input.review.object.spec.containers[_]
          not container.resources.limits.cpu
          msg := sprintf("Container '%s' must have CPU limits", [container.name])
        }

---
# Deploy same policy to AWS EKS, GCP GKE, and Azure AKS clusters
# via ArgoCD app-of-apps pattern with a shared policy repo
```

---

## Disaster Recovery Cross-Cloud

```
RTO (Recovery Time Objective): < 4 hours
RPO (Recovery Point Objective): < 1 hour

DR Strategy:
├── Primary: AWS (US-East-1)
├── Secondary: GCP (US-Central-1) — warm standby
└── Failover trigger: Route 53 health check → DNS failover

Data sync:
  - Iceberg tables: replicated hourly to GCS via Storage Transfer
  - Kafka: Kafka MirrorMaker 2 (topic replication lag < 5 min)
  - Metadata DB: PostgreSQL logical replication to CloudSQL

Compute:
  - GKE cluster pre-deployed with same Helm charts
  - ArgoCD synced to same GitOps repo
  - "Scale to 0" — minimal running cost until failover
```

---

## Vendor Lock-In Risk Matrix

| Technology | Lock-In Risk | Mitigation |
|-----------|--------------|------------|
| Snowflake native tables | HIGH | Use Iceberg external tables + Parquet |
| BigQuery native storage | HIGH | Export to GCS Parquet + Iceberg |
| AWS Glue crawler | MEDIUM | Use Apache Atlas or OpenLineage instead |
| MSK (Kafka) | LOW | Standard Kafka protocol, migratable |
| Lambda/Cloud Functions | HIGH | Use Kubernetes + containered functions |
| CloudFormation | HIGH | Terraform for all IaC |
| Databricks (Delta) | MEDIUM | Open Delta format, portable |

---

## Anti-Patterns

1. **Cloud-native ETL tools for core pipelines** — AWS Glue/ADF tightly couples pipeline logic to a cloud; use Spark on K8s for portability.
2. **No tagging standard across clouds** — AWS uses `tag:team`, GCP uses `labels.team`, Azure uses `tags/team`; define a universal tagging spec and enforce via OPA.
3. **Different secrets backends per cloud** — managing AWS SSM + GCP Secret Manager + Azure Key Vault separately triples ops burden; centralize on Vault.
4. **Assuming multi-cloud = double the cost** — idle standby cluster costs < 5% of active; warm standby is affordable; DR costs more than running 24/7.
5. **No cross-cloud cost visibility** — each cloud has its own billing console; without a unified view, FinOps optimization is impossible.

---

## References

- HashiCorp Vault: `vaultproject.io/docs`
- FinOps Foundation: `finops.org/framework/`
- rclone cross-cloud: `rclone.org/docs/`
- OPA Gatekeeper: `open-policy-agent.github.io/gatekeeper/`
- Related skills: `[[infra-aws-data-platform-review]]`, `[[infra-gcp-data-platform-review]]`, `[[infra-azure-data-platform-review]]`, `[[infra-terraform-review]]`
