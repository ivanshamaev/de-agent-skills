---
name: terraform-data-infrastructure
description: Terraform for data infrastructure — S3/MinIO data lake buckets (versioning, lifecycle, SSE-KMS), IAM roles for Spark/Airflow (least-privilege, IRSA on EKS), MSK/Kafka clusters (aws_msk_cluster, encryption, custom broker config), Kubernetes data platform (helm_release Airflow + Spark, namespace resource quotas), module layout (modules/ + envs/), typed variables with validation, S3 remote state + DynamoDB locking, Terragrunt DRY configs, GitHub Actions CI/CD pipeline with plan/apply
---

# Terraform for Data Infrastructure

## When to Use

Load this skill when the user needs to:
- Provision S3 or MinIO buckets for a data lake (versioning, lifecycle transitions, SSE-KMS encryption, bucket policies)
- Create IAM roles and policies for Spark, Airflow, or other data workloads with least-privilege access; configure IRSA on EKS
- Create or manage Amazon MSK (Managed Streaming for Kafka) clusters or Confluent Cloud Kafka clusters
- Deploy Airflow, Spark History Server, or other data tools on Kubernetes using `helm_release`
- Structure a Terraform monorepo with reusable modules and per-environment `tfvars`
- Manage Terraform state with S3 + DynamoDB locking or Terraform Cloud
- Set up GitHub Actions CI/CD for `terraform plan` / `apply`, or adopt Terragrunt for DRY multi-environment configs
- Write typed variables with validation, sensitive outputs, and `locals` for computed values

---

## Project Structure

A scalable layout separates reusable modules from environment-specific entry points:

```
infra/
├── modules/
│   ├── s3-lake/            # S3 bucket + lifecycle + encryption + policy
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── kafka/              # MSK cluster + configuration + security groups
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   └── k8s-data/           # Helm releases (Airflow, Spark History), namespaces, quotas
│       ├── main.tf
│       ├── variables.tf
│       └── outputs.tf
│
├── envs/
│   ├── dev/
│   │   ├── main.tf         # calls modules with dev values
│   │   ├── backend.tf      # remote state — dev prefix
│   │   ├── terraform.tfvars
│   │   └── variables.tf
│   └── prod/
│       ├── main.tf
│       ├── backend.tf      # remote state — prod prefix
│       ├── terraform.tfvars
│       └── variables.tf
│
└── terragrunt/             # optional DRY wrapper (see Terragrunt section)
    ├── terragrunt.hcl
    ├── dev/
    │   └── s3-lake/terragrunt.hcl
    └── prod/
        └── s3-lake/terragrunt.hcl
```

### `backend.tf` — S3 Remote State with DynamoDB Locking

```hcl
# envs/prod/backend.tf
terraform {
  backend "s3" {
    bucket         = "my-company-tf-state"
    key            = "prod/data-platform/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    kms_key_id     = "arn:aws:kms:us-east-1:123456789012:key/mrk-abc123"
    dynamodb_table = "terraform-state-lock"      # PAY_PER_REQUEST billing mode
  }
}
```

Bootstrap the state bucket once (outside Terraform, to avoid the chicken-and-egg problem):

```bash
aws s3api create-bucket --bucket my-company-tf-state --region us-east-1
aws s3api put-bucket-versioning \
  --bucket my-company-tf-state \
  --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption \
  --bucket my-company-tf-state \
  --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"aws:kms"}}]}'

aws dynamodb create-table \
  --table-name terraform-state-lock \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

---

## S3 / MinIO Buckets for the Data Lake

### `modules/s3-lake/variables.tf`

```hcl
variable "bucket_name" {
  type        = string
  description = "Globally unique S3 bucket name."
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{2,61}[a-z0-9]$", var.bucket_name))
    error_message = "Bucket name must be 4-63 lowercase characters, digits, hyphens, or dots."
  }
}

variable "environment" {
  type    = string
  default = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "kms_key_arn" {
  type        = string
  description = "ARN of the KMS CMK for SSE-KMS. If empty, SSE-S3 (AES-256) is used."
  default     = ""
  sensitive   = false
}

variable "glacier_transition_days" {
  type        = number
  default     = 90
  description = "Days before transitioning non-current versions to Glacier Instant Retrieval."
}

variable "expiration_days" {
  type        = number
  default     = 365
  description = "Days before expiring non-current object versions entirely."
}

variable "allowed_role_arns" {
  type        = list(string)
  description = "IAM role ARNs allowed to read/write this bucket."
  default     = []
}
```

### `modules/s3-lake/main.tf`

```hcl
locals {
  use_kms = var.kms_key_arn != ""
  common_tags = {
    Environment = var.environment
    ManagedBy   = "terraform"
    Module      = "s3-lake"
  }
}

resource "aws_s3_bucket" "lake" {
  bucket        = var.bucket_name
  force_destroy = var.environment != "prod"   # protect prod from accidental deletion
  tags          = local.common_tags
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = local.use_kms ? "aws:kms" : "AES256"
      kms_master_key_id = local.use_kms ? var.kms_key_arn : null
    }
    bucket_key_enabled = local.use_kms   # reduce KMS API calls / cost
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "transition-noncurrent-to-glacier"
    status = "Enabled"

    filter { prefix = "" }   # applies to all objects

    noncurrent_version_transition {
      noncurrent_days = var.glacier_transition_days
      storage_class   = "GLACIER_IR"   # Glacier Instant Retrieval — ms restore latency
    }

    noncurrent_version_expiration {
      noncurrent_days = var.expiration_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "lake_policy" {
  # Deny unencrypted uploads
  statement {
    sid     = "DenyNonEncryptedUploads"
    effect  = "Deny"
    actions = ["s3:PutObject"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = ["${aws_s3_bucket.lake.arn}/*"]
    condition {
      test     = "StringNotEquals"
      variable = "s3:x-amz-server-side-encryption"
      values   = local.use_kms ? ["aws:kms"] : ["AES256"]
    }
  }

  # Allow specified roles
  dynamic "statement" {
    for_each = length(var.allowed_role_arns) > 0 ? [1] : []
    content {
      sid     = "AllowDataWorkloads"
      effect  = "Allow"
      actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      principals {
        type        = "AWS"
        identifiers = var.allowed_role_arns
      }
      resources = [
        aws_s3_bucket.lake.arn,
        "${aws_s3_bucket.lake.arn}/*",
      ]
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  bucket = aws_s3_bucket.lake.id
  policy = data.aws_iam_policy_document.lake_policy.json
}
```

### `modules/s3-lake/outputs.tf`

```hcl
output "bucket_id"  { value = aws_s3_bucket.lake.id }
output "bucket_arn" { value = aws_s3_bucket.lake.arn }
output "bucket_regional_domain_name" {
  value = aws_s3_bucket.lake.bucket_regional_domain_name
}
```

### MinIO Provider (On-Prem)

For on-premises deployments use the `minio` community provider — API-compatible with S3:

```hcl
terraform {
  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = "~> 2.5"
    }
  }
}

provider "minio" {
  minio_server   = var.minio_endpoint          # e.g. "minio.internal:9000"
  minio_user     = var.minio_access_key
  minio_password = var.minio_secret_key
  minio_ssl      = false
}

resource "minio_s3_bucket" "bronze" {
  bucket = "bronze"
  acl    = "private"
}

resource "minio_s3_bucket_versioning" "bronze" {
  bucket = minio_s3_bucket.bronze.bucket
  versioning_configuration {
    status = "Enabled"
  }
}

# MinIO bucket policy (JSON, same as AWS)
resource "minio_iam_policy" "spark_rw" {
  name   = "spark-rw-bronze"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = [
        "arn:aws:s3:::bronze",
        "arn:aws:s3:::bronze/*",
      ]
    }]
  })
}
```

---

## IAM for Data Workloads

### Least-Privilege Role for Spark on EC2 / EMR

```hcl
# modules/iam-spark/main.tf

data "aws_iam_policy_document" "spark_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "spark" {
  name               = "${var.environment}-spark-role"
  assume_role_policy = data.aws_iam_policy_document.spark_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "spark_s3" {
  statement {
    sid     = "ReadWriteLake"
    effect  = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      var.lake_bucket_arn,
      "${var.lake_bucket_arn}/*",
    ]
  }

  statement {
    sid     = "AllowKMSForLake"
    effect  = "Allow"
    actions = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [var.kms_key_arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["s3.${var.region}.amazonaws.com"]
    }
  }

  # Glue / Hive metastore — read-only
  statement {
    sid    = "GlueReadOnly"
    effect = "Allow"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables",
      "glue:GetPartition", "glue:GetPartitions",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "spark_s3" {
  name   = "spark-s3-access"
  role   = aws_iam_role.spark.id
  policy = data.aws_iam_policy_document.spark_s3.json
}

resource "aws_iam_instance_profile" "spark" {
  name = "${var.environment}-spark-instance-profile"
  role = aws_iam_role.spark.name
}
```

### IRSA — IAM Roles for Service Accounts (EKS)

IRSA lets a Kubernetes ServiceAccount assume an IAM role without long-lived credentials. The pod's JWT token is exchanged for AWS credentials via the cluster OIDC provider.

```hcl
# Get the EKS cluster OIDC issuer URL
data "aws_eks_cluster" "this" {
  name = var.eks_cluster_name
}

data "aws_iam_openid_connect_provider" "eks" {
  url = data.aws_eks_cluster.this.identity[0].oidc[0].issuer
}

locals {
  oidc_provider_arn = data.aws_iam_openid_connect_provider.eks.arn
  # Strip "https://" prefix for condition matching
  oidc_provider_id  = replace(data.aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")
}

# ─── Airflow IRSA ────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "airflow_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_id}:sub"
      values   = ["system:serviceaccount:airflow:airflow"]   # namespace:serviceaccount
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_id}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "airflow" {
  name               = "${var.environment}-airflow-irsa"
  assume_role_policy = data.aws_iam_policy_document.airflow_assume.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "airflow_s3" {
  statement {
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
    resources = [
      var.airflow_logs_bucket_arn,
      "${var.airflow_logs_bucket_arn}/*",
    ]
  }
  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [var.dags_bucket_arn, "${var.dags_bucket_arn}/*"]
  }
}

resource "aws_iam_role_policy" "airflow_s3" {
  name   = "airflow-s3"
  role   = aws_iam_role.airflow.id
  policy = data.aws_iam_policy_document.airflow_s3.json
}

# ─── Spark IRSA ──────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "spark_assume_irsa" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_id}:sub"
      values   = ["system:serviceaccount:spark-jobs:spark"]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_id}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "spark_irsa" {
  name               = "${var.environment}-spark-irsa"
  assume_role_policy = data.aws_iam_policy_document.spark_assume_irsa.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy" "spark_lake" {
  name   = "spark-lake-access"
  role   = aws_iam_role.spark_irsa.id
  policy = data.aws_iam_policy_document.spark_s3.json   # reuse policy from above
}

# Annotate the K8s ServiceAccount so the pod inherits the role
resource "kubernetes_service_account" "spark" {
  metadata {
    name      = "spark"
    namespace = kubernetes_namespace.spark_jobs.metadata[0].name
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.spark_irsa.arn
    }
  }
}
```

---

## MSK / Kafka

### `modules/kafka/main.tf` — Amazon MSK

```hcl
variable "kafka_version"      { default = "3.7.x.kraft" }
variable "broker_count"       { default = 3 }
variable "broker_instance_type" { default = "kafka.m5.2xlarge" }
variable "broker_volume_size"  { default = 1000 }   # GiB per broker
variable "kms_key_arn"         { default = "" }

locals {
  use_kms = var.kms_key_arn != ""
}

resource "aws_msk_configuration" "this" {
  name              = "${var.environment}-broker-config"
  kafka_versions    = [var.kafka_version]

  server_properties = <<-EOT
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=6
    log.retention.hours=168
    log.retention.bytes=107374182400
    log.segment.bytes=1073741824
    compression.type=producer
    message.max.bytes=10485760
    replica.lag.time.max.ms=30000
    unclean.leader.election.enable=false
    offsets.topic.replication.factor=3
    transaction.state.log.replication.factor=3
    transaction.state.log.min.isr=2
  EOT
}

resource "aws_security_group" "msk" {
  name        = "${var.environment}-msk-sg"
  description = "MSK broker security group"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Kafka TLS from data workload SG"
    from_port       = 9094
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [var.data_workload_sg_id]
  }

  ingress {
    description     = "Kafka IAM from data workload SG"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [var.data_workload_sg_id]
  }

  ingress {
    description     = "Zookeeper (legacy) — restricted"
    from_port       = 2181
    to_port         = 2181
    protocol        = "tcp"
    security_groups = [var.data_workload_sg_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.environment}-kafka"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type  = var.broker_instance_type
    client_subnets = var.private_subnet_ids   # one per AZ; length must equal broker_count
    storage_info {
      ebs_storage_info {
        volume_size = var.broker_volume_size
        provisioned_throughput {
          enabled           = true
          volume_throughput = 250   # MiB/s per broker
        }
      }
    }
    security_groups = [aws_security_group.msk.id]
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = local.use_kms ? var.kms_key_arn : null
    encryption_in_transit {
      client_broker = "TLS"          # TLS | TLS_PLAINTEXT | PLAINTEXT
      in_cluster    = true
    }
  }

  client_authentication {
    sasl {
      iam   = true    # IAM authentication for clients — no creds to rotate
      scram = false
    }
    tls {
      certificate_authority_arns = []
    }
    unauthenticated = false
  }

  open_monitoring {
    prometheus {
      jmx_exporter  { enabled_in_broker = true }
      node_exporter { enabled_in_broker = true }
    }
  }

  logging_config {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
      s3 {
        enabled = true
        bucket  = var.logs_bucket_id
        prefix  = "msk/${var.environment}/broker-logs/"
      }
    }
  }

  tags = local.common_tags
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.environment}"
  retention_in_days = 30
}
```

### Outputs

```hcl
output "bootstrap_brokers_tls" {
  value = aws_msk_cluster.this.bootstrap_brokers_tls
}
output "bootstrap_brokers_sasl_iam" {
  value = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
}
output "zookeeper_connect_string" {
  value     = aws_msk_cluster.this.zookeeper_connect_string
  sensitive = true
}
```

### Confluent Cloud Alternative

```hcl
terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.11"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_api_key
  cloud_api_secret = var.confluent_api_secret
}

resource "confluent_environment" "data" {
  display_name = "${var.environment}-data"
  stream_governance { package = "ESSENTIALS" }
}

resource "confluent_kafka_cluster" "dedicated" {
  display_name = "${var.environment}-kafka"
  availability = "MULTI_ZONE"
  cloud        = "AWS"
  region       = var.region

  dedicated { cku = 2 }   # Confluent Kafka Units — scale as needed

  environment { id = confluent_environment.data.id }
}

resource "confluent_kafka_topic" "events" {
  kafka_cluster { id = confluent_kafka_cluster.dedicated.id }
  topic_name         = "raw.events"
  partitions_count   = 12
  rest_endpoint      = confluent_kafka_cluster.dedicated.rest_endpoint
  config = {
    "cleanup.policy"      = "delete"
    "retention.ms"        = "604800000"   # 7 days
    "min.insync.replicas" = "2"
  }
  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}
```

---

## Kubernetes Data Platform

### `modules/k8s-data/main.tf`

```hcl
terraform {
  required_providers {
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.31" }
    helm       = { source = "hashicorp/helm",       version = "~> 2.14" }
  }
}

# ─── Namespaces ──────────────────────────────────────────────────────────────

resource "kubernetes_namespace" "airflow" {
  metadata {
    name   = "airflow"
    labels = { "app.kubernetes.io/managed-by" = "terraform" }
  }
}

resource "kubernetes_namespace" "spark_jobs" {
  metadata {
    name   = "spark-jobs"
    labels = { "app.kubernetes.io/managed-by" = "terraform" }
  }
}

# ─── Resource Quotas ─────────────────────────────────────────────────────────

resource "kubernetes_resource_quota" "spark" {
  metadata {
    name      = "spark-quota"
    namespace = kubernetes_namespace.spark_jobs.metadata[0].name
  }
  spec {
    hard = {
      "requests.cpu"    = "80"
      "requests.memory" = "320Gi"
      "limits.cpu"      = "120"
      "limits.memory"   = "480Gi"
      "pods"            = "100"
    }
  }
}

resource "kubernetes_resource_quota" "airflow" {
  metadata {
    name      = "airflow-quota"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }
  spec {
    hard = {
      "requests.cpu"    = "20"
      "requests.memory" = "40Gi"
      "limits.cpu"      = "40"
      "limits.memory"   = "80Gi"
      "pods"            = "50"
    }
  }
}

# ─── Airflow Helm Release ─────────────────────────────────────────────────────

resource "helm_release" "airflow" {
  name             = "airflow"
  repository       = "https://airflow.apache.org"
  chart            = "airflow"
  version          = var.airflow_chart_version    # pin for reproducibility
  namespace        = kubernetes_namespace.airflow.metadata[0].name
  create_namespace = false
  atomic           = true       # roll back if install fails
  cleanup_on_fail  = true
  timeout          = 600

  values = [
    templatefile("${path.module}/templates/airflow-values.yaml.tpl", {
      environment           = var.environment
      airflow_image_tag     = var.airflow_image_tag
      airflow_irsa_role_arn = var.airflow_irsa_role_arn
      db_host               = var.airflow_db_host
      dags_repo             = var.dags_git_repo
      log_bucket            = var.airflow_logs_bucket_id
    })
  ]

  set_sensitive {
    name  = "data.metadataConnection.pass"
    value = var.airflow_db_password
  }

  set_sensitive {
    name  = "fernetKey"
    value = var.airflow_fernet_key
  }

  depends_on = [kubernetes_resource_quota.airflow]
}

# ─── Spark History Server Helm Release ────────────────────────────────────────

resource "helm_release" "spark_history_server" {
  name             = "spark-history-server"
  repository       = "https://charts.helm.sh/stable"
  chart            = "spark-history-server"
  version          = var.spark_history_chart_version
  namespace        = kubernetes_namespace.spark_jobs.metadata[0].name
  create_namespace = false

  set {
    name  = "hdfs.logDirectory"
    value = "s3a://${var.lake_bucket_id}/spark-history"
  }
  set {
    name  = "serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn"
    value = var.spark_irsa_role_arn
  }
  set {
    name  = "resources.requests.cpu"
    value = "500m"
  }
  set {
    name  = "resources.requests.memory"
    value = "1Gi"
  }

  depends_on = [kubernetes_resource_quota.spark]
}

# ─── Node Taints for Data Workloads (EKS Managed Node Group) ─────────────────

# Declared separately per-environment via aws_eks_node_group, shown here as reference:
# taint { key = "dedicated", value = "spark",   effect = "NO_SCHEDULE" }
# taint { key = "dedicated", value = "airflow",  effect = "NO_SCHEDULE" }
```

### `modules/k8s-data/templates/airflow-values.yaml.tpl`

```yaml
executor: KubernetesExecutor

images:
  airflow:
    repository: "registry.example.com/airflow"
    tag: "${airflow_image_tag}"
    pullPolicy: IfNotPresent

config:
  core:
    dags_are_paused_at_creation: "True"
    max_active_runs_per_dag: "5"
  logging:
    remote_logging: "True"
    remote_base_log_folder: "s3://${log_bucket}/airflow-logs"
    remote_log_conn_id: "aws_default"

scheduler:
  replicas: 2

dags:
  gitSync:
    enabled: true
    repo: "${dags_repo}"
    branch: main
    depth: 1

serviceAccount:
  create: true
  name: airflow
  annotations:
    eks.amazonaws.com/role-arn: "${airflow_irsa_role_arn}"

data:
  metadataConnection:
    user: airflow
    host: "${db_host}"
    port: 5432
    db: airflow
    protocol: postgresql

redis:
  enabled: false

triggerer:
  enabled: true
```

---

## Variables, Outputs, and Locals

### Typed Variables with Validation

```hcl
# envs/prod/variables.tf

variable "region" {
  type        = string
  description = "AWS region for all resources."
  default     = "us-east-1"
  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9]$", var.region))
    error_message = "Must be a valid AWS region slug (e.g. us-east-1)."
  }
}

variable "environment" {
  type    = string
  default = "prod"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "airflow_db_password" {
  type        = string
  description = "Password for the Airflow metadata database."
  sensitive   = true    # redacted from plan output and state logs
}

variable "airflow_fernet_key" {
  type      = string
  sensitive = true
}

variable "kafka_broker_count" {
  type = number
  validation {
    condition     = var.kafka_broker_count >= 3 && var.kafka_broker_count % 3 == 0
    error_message = "Broker count must be a multiple of 3 (one per AZ) and >= 3."
  }
}

variable "eks_node_groups" {
  type = map(object({
    instance_types = list(string)
    min_size       = number
    max_size       = number
    desired_size   = number
    taint_value    = optional(string, "")
  }))
  description = "Map of EKS managed node group configurations."
}
```

### `terraform.tfvars` (prod)

```hcl
# envs/prod/terraform.tfvars
region      = "us-east-1"
environment = "prod"

kafka_broker_count   = 3

eks_node_groups = {
  spark-workers = {
    instance_types = ["r6i.4xlarge"]
    min_size       = 0
    max_size       = 20
    desired_size   = 3
    taint_value    = "spark"
  }
  airflow-workers = {
    instance_types = ["m6i.2xlarge"]
    min_size       = 2
    max_size       = 10
    desired_size   = 3
    taint_value    = "airflow"
  }
  system = {
    instance_types = ["m6i.xlarge"]
    min_size       = 2
    max_size       = 5
    desired_size   = 2
  }
}
```

### Locals for Computed Values

```hcl
locals {
  name_prefix = "${var.environment}-${var.project}"

  # bucket names derived from environment — avoid duplication across modules
  bronze_bucket = "${local.name_prefix}-bronze"
  silver_bucket = "${local.name_prefix}-silver"
  gold_bucket   = "${local.name_prefix}-gold"

  # all data workload roles — used in bucket policy
  data_role_arns = [
    module.iam_spark.role_arn,
    module.iam_airflow.role_arn,
  ]

  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
    CostCenter  = var.cost_center
  }
}
```

### Cross-Module Outputs

```hcl
# envs/prod/main.tf — wiring modules together via outputs
module "s3_bronze" {
  source          = "../../modules/s3-lake"
  bucket_name     = local.bronze_bucket
  environment     = var.environment
  kms_key_arn     = module.kms.key_arn
  allowed_role_arns = local.data_role_arns
}

module "kafka" {
  source               = "../../modules/kafka"
  environment          = var.environment
  vpc_id               = module.vpc.vpc_id
  private_subnet_ids   = module.vpc.private_subnets
  data_workload_sg_id  = module.eks.worker_security_group_id
  logs_bucket_id       = module.s3_bronze.bucket_id
  kms_key_arn          = module.kms.key_arn
  broker_count         = var.kafka_broker_count
}

module "k8s_data" {
  source                    = "../../modules/k8s-data"
  environment               = var.environment
  airflow_irsa_role_arn     = module.iam_airflow.irsa_role_arn
  spark_irsa_role_arn       = module.iam_spark.irsa_role_arn
  lake_bucket_id            = module.s3_bronze.bucket_id
  airflow_logs_bucket_id    = module.s3_bronze.bucket_id
  airflow_db_host           = module.rds_airflow.endpoint
  airflow_db_password       = var.airflow_db_password
  airflow_fernet_key        = var.airflow_fernet_key
  dags_git_repo             = "https://github.com/org/airflow-dags.git"
}
```

---

## State Management and CI/CD

### Terraform Cloud Backend (Alternative to S3)

```hcl
# envs/prod/backend.tf  — Terraform Cloud
terraform {
  cloud {
    organization = "my-company"
    workspaces {
      name = "data-platform-prod"
    }
  }
}
```

### GitHub Actions CI/CD Pipeline

```yaml
# .github/workflows/terraform.yml
name: Terraform

on:
  pull_request:
    paths: ["infra/**"]
  push:
    branches: [main]
    paths: ["infra/**"]

permissions:
  id-token: write    # OIDC token for AWS AssumeRoleWithWebIdentity
  contents: read
  pull-requests: write

env:
  TF_VERSION: "1.9.8"
  AWS_REGION: "us-east-1"
  WORKING_DIR: "infra/envs/prod"

jobs:
  plan:
    name: Terraform Plan
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request'
    defaults:
      run:
        working-directory: ${{ env.WORKING_DIR }}

    steps:
      - uses: actions/checkout@v4

      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::123456789012:role/github-actions-tf-plan
          aws-region: ${{ env.AWS_REGION }}

      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: ${{ env.TF_VERSION }}

      - name: Terraform Init
        run: terraform init -input=false

      - name: Terraform Validate
        run: terraform validate

      - name: Terraform Format Check
        run: terraform fmt -check -recursive

      - name: Terraform Plan
        id: plan
        run: |
          terraform plan \
            -var-file="terraform.tfvars" \
            -var="airflow_db_password=${{ secrets.AIRFLOW_DB_PASSWORD }}" \
            -var="airflow_fernet_key=${{ secrets.AIRFLOW_FERNET_KEY }}" \
            -input=false \
            -out=tfplan \
            -no-color 2>&1 | tee plan.txt
          echo "exit_code=${PIPESTATUS[0]}" >> "$GITHUB_OUTPUT"

      - name: Comment Plan on PR
        uses: actions/github-script@v7
        if: always()
        with:
          script: |
            const fs = require('fs');
            const plan = fs.readFileSync('${{ env.WORKING_DIR }}/plan.txt', 'utf8');
            const truncated = plan.length > 60000 ? plan.slice(-60000) + '\n[truncated]' : plan;
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: `## Terraform Plan\n\`\`\`hcl\n${truncated}\n\`\`\``
            });

  apply:
    name: Terraform Apply
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'
    environment: production    # requires manual approval gate in GitHub
    defaults:
      run:
        working-directory: ${{ env.WORKING_DIR }}

    steps:
      - uses: actions/checkout@v4

      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::123456789012:role/github-actions-tf-apply
          aws-region: ${{ env.AWS_REGION }}

      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: ${{ env.TF_VERSION }}

      - name: Terraform Init
        run: terraform init -input=false

      - name: Terraform Apply
        run: |
          terraform apply \
            -var-file="terraform.tfvars" \
            -var="airflow_db_password=${{ secrets.AIRFLOW_DB_PASSWORD }}" \
            -var="airflow_fernet_key=${{ secrets.AIRFLOW_FERNET_KEY }}" \
            -auto-approve \
            -input=false

  drift:
    name: Drift Detection
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    # schedule: cron: '0 6 * * 1-5'   # run in calling workflow
    defaults:
      run:
        working-directory: ${{ env.WORKING_DIR }}

    steps:
      - uses: actions/checkout@v4
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::123456789012:role/github-actions-tf-plan
          aws-region: ${{ env.AWS_REGION }}
      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: ${{ env.TF_VERSION }}
      - run: terraform init -input=false
      - name: Detect Drift
        run: |
          terraform plan \
            -var-file="terraform.tfvars" \
            -var="airflow_db_password=${{ secrets.AIRFLOW_DB_PASSWORD }}" \
            -var="airflow_fernet_key=${{ secrets.AIRFLOW_FERNET_KEY }}" \
            -detailed-exitcode \
            -input=false || \
          (echo "DRIFT DETECTED — manual review required" && exit 1)
```

### Terragrunt — DRY Multi-Environment Configs

Terragrunt eliminates per-environment `backend.tf` and `provider.tf` duplication. One root `terragrunt.hcl` defines the S3 backend pattern; child configs just declare their module source and inputs.

```hcl
# terragrunt/terragrunt.hcl  (root)
locals {
  account_id  = get_aws_account_id()
  region      = "us-east-1"
  environment = basename(dirname(get_terragrunt_dir()))   # dev / prod
}

remote_state {
  backend = "s3"
  generate = {
    path      = "backend.tf"
    if_exists = "overwrite_terragrunt"
  }
  config = {
    bucket         = "my-company-tf-state-${local.account_id}"
    key            = "${local.environment}/${path_relative_to_include()}/terraform.tfstate"
    region         = local.region
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
  }
}

generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<-EOF
    provider "aws" {
      region = "${local.region}"
      default_tags {
        tags = {
          Environment = "${local.environment}"
          ManagedBy   = "terraform"
        }
      }
    }
  EOF
}
```

```hcl
# terragrunt/prod/s3-lake/terragrunt.hcl
include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../../../modules/s3-lake"
}

inputs = {
  environment             = "prod"
  bucket_name             = "my-company-prod-bronze"
  kms_key_arn             = dependency.kms.outputs.key_arn
  glacier_transition_days = 90
  expiration_days         = 730
  allowed_role_arns       = dependency.iam.outputs.data_role_arns
}

dependency "kms" {
  config_path = "../kms"
  mock_outputs = { key_arn = "arn:aws:kms:us-east-1:123456789012:key/mock" }
  mock_outputs_allowed_terraform_commands = ["validate", "plan"]
}

dependency "iam" {
  config_path = "../iam"
  mock_outputs = { data_role_arns = [] }
  mock_outputs_allowed_terraform_commands = ["validate", "plan"]
}
```

Deploy all prod modules in dependency order:

```bash
cd terragrunt/prod
terragrunt run-all plan   # shows plan for all modules
terragrunt run-all apply  # applies in DAG order
```

---

## Anti-Patterns

1. **Storing `terraform.tfstate` in Git** — state contains plaintext secrets (DB passwords, private keys). Always use a remote backend (S3 + DynamoDB or Terraform Cloud). Add `*.tfstate` and `*.tfstate.backup` to `.gitignore`.

2. **Hardcoding secrets in `terraform.tfvars`** — `terraform.tfvars` is committed to Git. Pass sensitive variables via environment variables (`TF_VAR_airflow_db_password`) or a secrets manager integration, never as plaintext in tracked files.

3. **No `sensitive = true` on secret variables** — without it, Terraform prints secret values in plan output and logs. Mark every password, token, and key variable with `sensitive = true`.

4. **Using the default AWS provider `region` without explicit pinning** — a developer with a different CLI default region can silently deploy to the wrong region. Always set `region` explicitly in the provider block or via a required variable.

5. **Sharing one state file across environments** — a bad `prod` apply destroys dev resources and vice versa. Keep per-environment state files with separate keys (and separate AWS accounts for critical workloads).

6. **No DynamoDB lock table** — two concurrent `terraform apply` runs will corrupt state. The lock table is cheap (PAY_PER_REQUEST) and must always accompany an S3 backend.

7. **Pinning `~> aws` provider without a minor-version floor** — `~> 5.0` allows 5.x patches but a major jump from 4.x→5.x is breaking. Use `~> 5.60` (current major + recent minor) to allow patches while preventing accidental major upgrades.

8. **`force_destroy = true` on production S3 buckets** — one `terraform destroy` permanently deletes all data. Set `force_destroy = false` for prod; use separate out-of-band deletion procedures.

9. **Creating MSK clusters without `min.insync.replicas=2`** — with the default of 1, a single broker failure silently allows producers to commit messages that are never replicated, causing data loss on broker restart.

10. **Running Terraform Apply directly in CI without a plan approval gate** — auto-approve on push to main can silently destroy production resources. Use a GitHub environment protection rule requiring manual approval before `apply`, or use Terraform Cloud's Run Queue with policy checks.

11. **Not tagging resources with `environment` and `ManagedBy`** — untagged resources become orphans after state drift. Use a `default_tags` block in the AWS provider (or `local.common_tags`) applied to every resource.

12. **One giant `main.tf` per environment instead of modules** — copy-pasting infrastructure blocks between `envs/dev` and `envs/prod` creates drift. Extract every reusable component into a module immediately.

---

## References to Consult When Needed

- Terraform AWS provider docs: `registry.terraform.io/providers/hashicorp/aws/latest/docs`
- `aws_msk_cluster` resource: `registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/msk_cluster`
- terraform-aws-modules/iam IRSA submodule: `registry.terraform.io/modules/terraform-aws-modules/iam/aws/latest/submodules/iam-role-for-service-accounts`
- Terragrunt docs: `terragrunt.gruntwork.io/docs/`
- Terraform S3 backend: `developer.hashicorp.com/terraform/language/settings/backends/s3`
- MinIO Terraform provider: `registry.terraform.io/providers/aminueza/minio/latest`
- Confluent provider: `registry.terraform.io/providers/confluentinc/confluent/latest`
- AWS prescriptive guidance — Terraform best practices: `docs.aws.amazon.com/prescriptive-guidance/latest/terraform-aws-provider-best-practices/`
