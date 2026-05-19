---
name: infra-aws-data-platform-review
description: AWS data platform review — S3 data lake (lifecycle/replication/encryption/access points), EMR vs Glue ETL trade-offs, MSK Kafka configuration, RDS/Aurora for metadata, Redshift vs Athena for analytics, MWAA (managed Airflow), EKS for containerized pipelines, IAM roles (IRSA for EKS/EMREC2 instance profiles), Lake Formation row/column security, AWS Glue catalog, VPC data platform networking, cost optimization (S3 Intelligent-Tiering/Spot instances)
---

# AWS Data Platform Review

## When to Use

- Designing or reviewing an AWS-native data platform
- Auditing IAM permissions for data pipelines
- Choosing between AWS managed services vs self-managed
- Optimizing S3 data lake costs
- Setting up Lake Formation for fine-grained data access control

---

## S3 Data Lake Architecture

```hcl
# Terraform: 3-zone data lake (Bronze/Silver/Gold)
resource "aws_s3_bucket" "zones" {
  for_each = toset(["bronze", "silver", "gold"])
  bucket   = "${local.prefix}-data-lake-${each.key}"
  tags     = merge(local.common_tags, { Zone = each.key })
}

# Encryption with separate KMS keys per zone
resource "aws_s3_bucket_server_side_encryption_configuration" "zones" {
  for_each = aws_s3_bucket.zones
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.zones[each.key].arn
    }
    bucket_key_enabled = true   # reduces KMS API calls by 99%
  }
}

# Lifecycle: tiered storage by zone age
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.zones["bronze"].id
  rule {
    id     = "raw-data-tiering"
    status = "Enabled"
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
    expiration { days = 365 }
  }
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "all" {
  for_each                = aws_s3_bucket.zones
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
```

---

## IAM — IRSA (IAM Roles for Service Accounts)

```hcl
# Airflow on EKS: worker pods assume an IAM role without static keys
module "irsa_airflow" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name = "${local.prefix}-airflow-worker"

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["airflow:airflow-worker"]
    }
  }

  role_policy_arns = {
    s3_access    = aws_iam_policy.airflow_s3.arn
    glue_access  = aws_iam_policy.airflow_glue.arn
  }
}

# Scoped S3 policy
resource "aws_iam_policy" "airflow_s3" {
  name   = "${local.prefix}-airflow-s3"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = ["${aws_s3_bucket.zones["bronze"].arn}/*",
                    "${aws_s3_bucket.zones["silver"].arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.zones["bronze"].arn,
                    aws_s3_bucket.zones["silver"].arn]
      }
    ]
  })
}
```

---

## MSK Kafka Configuration

```hcl
resource "aws_msk_cluster" "main" {
  cluster_name           = "${local.prefix}-kafka"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.m5.xlarge"
    client_subnets  = module.vpc.private_subnets
    storage_info {
      ebs_storage_info {
        volume_size                = 1000   # GB per broker
        provisioned_throughput {
          enabled           = true
          volume_throughput = 250           # MiB/s
        }
      }
    }
    security_groups = [aws_security_group.msk.id]
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
    at_rest_encryption_info {
      encryption_at_rest_kms_key_arn = aws_kms_key.msk.arn
    }
  }

  client_authentication {
    sasl {
      iam = true    # SASL/IAM auth — no credentials to manage
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }
}

resource "aws_msk_configuration" "main" {
  kafka_versions = ["3.5.1"]
  name           = "${local.prefix}-kafka-config"
  server_properties = <<-EOF
    auto.create.topics.enable=false
    log.retention.hours=168
    num.partitions=3
    default.replication.factor=3
    min.insync.replicas=2
    compression.type=lz4
  EOF
}
```

---

## Lake Formation — Fine-Grained Access Control

```hcl
# Register S3 location with Lake Formation
resource "aws_lakeformation_resource" "data_lake" {
  arn      = aws_s3_bucket.zones["gold"].arn
  role_arn = aws_iam_role.lakeformation.arn
}

# Grant table-level access to analytics team
resource "aws_lakeformation_permissions" "analytics_read" {
  principal   = aws_iam_role.analytics_team.arn
  permissions = ["SELECT"]
  permissions_with_grant_option = []

  table {
    database_name = "gold"
    name          = "fact_orders"
  }
}

# Column-level security: hide PII columns from non-privileged role
resource "aws_lakeformation_permissions" "pii_columns" {
  principal   = aws_iam_role.data_scientist.arn
  permissions = ["SELECT"]

  table_with_columns {
    database_name = "gold"
    name          = "customers"
    column_names  = ["customer_id", "order_count", "total_spend"]
    # excluded: email, phone, address
  }
}
```

---

## MWAA vs Self-Managed Airflow on EKS

| Aspect | MWAA | EKS Helm Chart |
|--------|------|----------------|
| Setup effort | Low (minutes) | High (days) |
| Cost (2 schedulers) | ~$300/mo + workers | ~$150/mo + workers |
| Airflow version | Limited (2-3 supported) | Any version |
| Plugin support | Limited | Unlimited |
| KubernetesExecutor | No | Yes |
| Custom Python deps | S3 requirements.txt | Dockerfile |
| HA | Managed | Manual (2+ schedulers) |
| Scaling | Limited | Full autoscaling |
| Best for | Small teams, quick start | Large teams, custom needs |

---

## Glue vs EMR vs Spark-on-EKS

| Aspect | AWS Glue | Amazon EMR | Spark on EKS |
|--------|----------|------------|--------------|
| Management | Serverless | Semi-managed | Self-managed |
| Startup time | 1-2 min | 5-10 min | 30-60s (reuse) |
| Cost model | Per DPU-hour | Per EC2-hour | Per pod resource |
| Spot support | Limited | Full | Full |
| Custom jars | Limited | Full | Full |
| Iceberg support | Via Glue catalog | Full | Full |
| Best for | Simple ETL, serverless | Large Spark jobs | Kubernetes platform |

---

## Cost Optimization

```bash
# S3 Intelligent-Tiering: auto-tier objects not accessed in 30 days
resource "aws_s3_bucket_intelligent_tiering_configuration" "bronze" {
  bucket = aws_s3_bucket.zones["bronze"].id
  name   = "auto-tiering"
  status = "Enabled"

  tiering {
    access_tier = "DEEP_ARCHIVE_ACCESS"
    days        = 180
  }
}

# EMR spot configuration
resource "aws_emr_instance_fleet" "task" {
  cluster_id = aws_emr_cluster.main.id
  name       = "task-fleet"
  target_spot_capacity = 20   # 20 units of spot (80% cheaper)
  target_on_demand_capacity = 2

  instance_type_configs {
    instance_type     = "m5.xlarge"
    bid_price_as_percentage_of_on_demand_price = 100
    ebs_config {
      size = 100
      type = "gp3"
    }
  }
}
```

---

## VPC Networking for Data Platform

```hcl
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${local.prefix}-data-platform"
  cidr = "10.0.0.0/16"

  azs              = ["us-east-1a", "us-east-1b", "us-east-1c"]
  private_subnets  = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets   = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  enable_nat_gateway     = true
  single_nat_gateway     = false  # one per AZ for HA
  enable_vpn_gateway     = false

  # S3 VPC endpoint (free data transfer within VPC)
  enable_s3_endpoint     = true
  # Glue VPC endpoint
  enable_glue_endpoint   = true
}
```

---

## Review Checklist

```
[ ] S3 buckets: encryption (KMS), versioning, public access blocked, lifecycle
[ ] IAM: IRSA for EKS workloads (no static keys), least-privilege policies
[ ] MSK: TLS + IAM auth, min.insync.replicas=2, encryption at rest
[ ] Lake Formation: column-level security for PII tables
[ ] VPC: private subnets for all data services, S3/Glue VPC endpoints
[ ] EKS: node groups in private subnets, IRSA enabled
[ ] CloudTrail: enabled for all regions, S3 bucket with lifecycle
[ ] Cost: S3 Intelligent-Tiering, Spot for EMR/EKS batch, Savings Plans
[ ] Monitoring: CloudWatch → Prometheus metrics via CW exporter
[ ] Backups: automated RDS snapshots (7+ day retention), S3 versioning
```

---

## Anti-Patterns

1. **Static AWS credentials in Airflow connections** — rotate every 90 days or face locked pipelines; use IRSA on EKS or instance profiles on EC2.
2. **S3 public access not blocked** — one misconfigured bucket policy exposes entire data lake; always set `restrict_public_buckets = true`.
3. **No S3 lifecycle rules on bronze zone** — raw data accumulates indefinitely; raw zone grows 10x faster than gold zone.
4. **Lake Formation disabled with broad S3 bucket policies** — no row/column-level control; enable Lake Formation for all gold/silver tables.
5. **No VPC endpoints for S3/Glue** — data transfers via internet gateway incur NAT Gateway data processing costs; S3 VPC endpoint is free.

---

## References

- AWS Lake Formation: `docs.aws.amazon.com/lake-formation/latest/dg/`
- MSK best practices: `docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html`
- IRSA: `docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html`
- EMR on EKS: `docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/`
- Related skills: `[[terraform-data]]`, `[[infra-kubernetes-security-audit]]`, `[[de-cost-optimization]]`
