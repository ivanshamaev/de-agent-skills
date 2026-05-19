---
name: infra-terraform-security-scan
description: Terraform security scanning — tfsec static analysis (AWS/GCP/Azure misconfigurations), Checkov IaC policy checks (750+ rules, CIS benchmarks), tflint security rules, KICS scanner, S3 encryption/public access/versioning checks, IAM least-privilege patterns, security group rule review (0.0.0.0/0), KMS key rotation, VPC flow logs, CloudTrail enabled, pre-commit hooks, SARIF output for GitHub Security tab, policy-as-code with OPA/Sentinel
---

# Terraform Security Scan

## When to Use

- Security review of a Terraform PR before merge
- Compliance assessment against CIS Benchmarks (AWS/GCP/Azure)
- Auditing existing infrastructure for misconfigurations
- Setting up security gates in CI/CD pipelines
- Establishing policy-as-code for IaC governance

---

## tfsec — Static Analysis

### Installation and Basic Usage

```bash
# Install
brew install tfsec              # macOS
curl -s https://raw.githubusercontent.com/aquasecurity/tfsec/master/scripts/install_linux.sh | bash

# Scan current directory
tfsec .

# Scan with specific output format
tfsec . --format json | jq '.results[] | {rule: .rule_id, severity: .severity, description: .description}'

# SARIF output for GitHub Security tab
tfsec . --format sarif --out tfsec-results.sarif

# Include tfvars values in analysis
tfsec . --tfvars-file terraform.tfvars

# Ignore low-severity findings
tfsec . --minimum-severity HIGH
```

### Inline Suppressions

```hcl
resource "aws_s3_bucket" "public_assets" {
  bucket = "my-company-public-assets"

  #tfsec:ignore:aws-s3-no-public-buckets
  #tfsec:ignore:aws-s3-block-public-acls
  tags = local.common_tags
}
```

### Config File

```yaml
# .tfsec/config.yaml
minimum_severity: MEDIUM
exclude:
  - aws-s3-no-public-buckets   # intentionally public CDN bucket
```

---

## Checkov — 750+ Policy Checks

### Installation and Usage

```bash
# Install
pip install checkov

# Scan Terraform directory
checkov -d . --framework terraform

# Scan with specific checks only (CIS AWS benchmark)
checkov -d . --check CKV_AWS_1,CKV_AWS_2  # specific checks
checkov -d . --bc-api-key $BRIDGECREW_API_KEY  # with Bridgecrew cloud

# Output formats
checkov -d . -o json > checkov-results.json
checkov -d . -o sarif > checkov-results.sarif

# Soft-fail: report findings but don't fail the pipeline
checkov -d . --soft-fail

# Skip specific checks
checkov -d . --skip-check CKV_AWS_79,CKV_AWS_91
```

### Key Terraform Checks (AWS)

| Check ID | Description |
|----------|-------------|
| `CKV_AWS_18` | S3 bucket access logging enabled |
| `CKV_AWS_19` | S3 bucket encryption enabled |
| `CKV_AWS_20` | S3 bucket not publicly accessible |
| `CKV_AWS_21` | S3 versioning enabled |
| `CKV_AWS_52` | S3 MFA delete enabled |
| `CKV_AWS_2` | ALB/ELB HTTPS listeners only |
| `CKV_AWS_79` | EC2 IMDSv2 enforced |
| `CKV_AWS_135` | EC2 no public IP at launch |
| `CKV_AWS_17` | RDS not publicly accessible |
| `CKV_AWS_16` | RDS encryption at rest |
| `CKV_AWS_23` | RDS backup retention >= 7 days |
| `CKV_AWS_25` | Security group no unrestricted SSH |
| `CKV_AWS_24` | Security group no unrestricted RDP |
| `CKV_AWS_36` | CloudTrail logging enabled |
| `CKV_AWS_86` | CloudFront logging enabled |

---

## Common Security Patterns to Enforce

### S3 Hardening

```hcl
# ✅ Correct: fully hardened S3 bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = "${local.name_prefix}-data-lake"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data_lake.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_logging" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = "data-lake/"
}
```

### KMS Key with Rotation

```hcl
resource "aws_kms_key" "data_lake" {
  description             = "KMS key for data lake S3 encryption"
  enable_key_rotation     = true       # CKV_AWS_7: rotation required
  deletion_window_in_days = 30
  tags                    = local.common_tags
}

resource "aws_kms_alias" "data_lake" {
  name          = "alias/${local.name_prefix}-data-lake"
  target_key_id = aws_kms_key.data_lake.key_id
}
```

### Security Groups — No 0.0.0.0/0

```hcl
# ❌ Fails CKV_AWS_25 — unrestricted SSH
resource "aws_security_group_rule" "bad_ssh" {
  type        = "ingress"
  from_port   = 22
  to_port     = 22
  protocol    = "tcp"
  cidr_blocks = ["0.0.0.0/0"]   # never do this
}

# ✅ Correct — restrict to VPC CIDR or specific source SG
resource "aws_security_group_rule" "bastion_ssh" {
  type                     = "ingress"
  from_port                = 22
  to_port                  = 22
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.bastion.id
  security_group_id        = aws_security_group.workers.id
}
```

### IAM Least Privilege

```hcl
# ❌ Overly broad
data "aws_iam_policy_document" "bad" {
  statement {
    actions   = ["s3:*"]
    resources = ["*"]
  }
}

# ✅ Scoped to specific bucket and actions
data "aws_iam_policy_document" "airflow_s3" {
  statement {
    sid     = "ReadDags"
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.dags.arn,
      "${aws_s3_bucket.dags.arn}/*",
    ]
  }
  statement {
    sid       = "WriteLogs"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.logs.arn}/airflow/*"]
  }
}
```

### RDS Security

```hcl
resource "aws_db_instance" "main" {
  engine                  = "postgres"
  engine_version          = "15.4"
  instance_class          = "db.t3.medium"

  # Security requirements
  publicly_accessible     = false          # CKV_AWS_17
  storage_encrypted       = true           # CKV_AWS_16
  kms_key_id              = aws_kms_key.rds.arn
  backup_retention_period = 7              # CKV_AWS_23 — >= 7 days
  deletion_protection     = true
  skip_final_snapshot     = false

  # No default port in production
  port = 5433

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
}
```

---

## GitHub Actions CI Integration

```yaml
name: Terraform Security Scan

on:
  pull_request:
    paths:
      - "**.tf"

jobs:
  tfsec:
    runs-on: ubuntu-latest
    permissions:
      security-events: write  # for SARIF upload

    steps:
      - uses: actions/checkout@v4

      - name: tfsec scan
        uses: aquasecurity/tfsec-action@v1.0.0
        with:
          soft_fail: false
          format: sarif
          sarif_file: tfsec.sarif

      - name: Upload tfsec SARIF
        uses: github/codeql-action/upload-sarif@v3
        with:
          sarif_file: tfsec.sarif

  checkov:
    runs-on: ubuntu-latest
    permissions:
      security-events: write

    steps:
      - uses: actions/checkout@v4

      - name: Checkov scan
        uses: bridgecrewio/checkov-action@master
        with:
          directory: .
          framework: terraform
          output_format: sarif
          output_file_path: checkov.sarif
          skip_check: CKV_AWS_79  # IMDSv2 — covered by launch template

      - name: Upload Checkov SARIF
        uses: github/codeql-action/upload-sarif@v3
        with:
          sarif_file: checkov.sarif
```

---

## Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/antonbabenko/pre-commit-terraform
    rev: v1.92.0
    hooks:
      - id: terraform_fmt
      - id: terraform_validate
      - id: terraform_tflint
      - id: terraform_tfsec
        args:
          - --args=--minimum-severity=HIGH
      - id: terraform_checkov
        args:
          - --args=--skip-check CKV_AWS_79

  - repo: https://github.com/gitleaks/gitleaks
    rev: v8.18.0
    hooks:
      - id: gitleaks   # catch secrets before commit
```

---

## OPA/Conftest Policy-as-Code

```rego
# policies/s3_encryption.rego
package terraform.s3

deny[msg] {
  resource := input.resource.aws_s3_bucket[name]
  not input.resource.aws_s3_bucket_server_side_encryption_configuration[name]
  msg := sprintf("S3 bucket '%s' does not have server-side encryption configured", [name])
}

deny[msg] {
  resource := input.resource.aws_security_group_rule[name]
  resource.cidr_blocks[_] == "0.0.0.0/0"
  resource.from_port <= 22
  resource.to_port >= 22
  msg := sprintf("Security group rule '%s' allows SSH from 0.0.0.0/0", [name])
}
```

```bash
# Run conftest against Terraform plan JSON
terraform plan -out=tfplan.binary
terraform show -json tfplan.binary > tfplan.json
conftest test tfplan.json -p policies/
```

---

## Anti-Patterns

1. **Skipping security scans in CI for "small changes"** — misconfigurations are often in small changes; always scan on every PR.
2. **Mass-suppressing tfsec findings** — `#tfsec:ignore:*` hides real issues; document each suppression with a reason.
3. **Not scanning modules separately** — root module scan misses issues inside nested modules; use `--recursive` flag.
4. **Failing CI without human review for warnings** — MEDIUM findings block good PRs; set minimum severity to HIGH in blocking gate, report MEDIUM as warnings.
5. **Checking in tfvars with secrets** — `.gitignore` terraform.tfvars files containing secrets; inject via env var or Vault.

---

## References

- tfsec: `github.com/aquasecurity/tfsec`
- Checkov: `checkov.io`
- pre-commit-terraform: `github.com/antonbabenko/pre-commit-terraform`
- KICS: `checkmarx.com/blog/kics-open-source-iac-scanning/`
- OPA Conftest: `conftest.dev`
- CIS AWS Terraform Benchmark: `cisecurity.org`
- Related skills: `[[infra-terraform-review]]`, `[[infra-terraform-cost-estimator]]`, `[[infra-kubernetes-security-audit]]`
