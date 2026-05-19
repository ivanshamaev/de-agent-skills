---
name: infra-terraform-review
description: Terraform code review — module structure (main/variables/outputs/modules layout), variable validation blocks, type constraints, sensitive variables, locals vs variables, resource naming conventions, provider version pinning, data sources vs resources, module composition patterns, state management (remote backend S3/GCS), workspace strategy, dependency management (depends_on vs implicit), terraform fmt/validate/tflint checks, DRY with Terragrunt
---

# Terraform Code Review

## When to Use

- Reviewing a Terraform PR for a data platform component
- Auditing a new module before it goes to production
- Establishing team coding standards for IaC
- Diagnosing state drift or dependency issues
- Evaluating module reusability and composability

---

## Module Structure

### Recommended File Layout

```
modules/
  s3-data-lake/
    main.tf          # primary resource definitions
    variables.tf     # all input variables with types + validation
    outputs.tf       # all outputs with descriptions
    versions.tf      # required_providers with version constraints
    README.md        # auto-generated with terraform-docs

envs/
  dev/
    main.tf          # module calls + env-specific overrides
    terraform.tfvars # variable values (not secrets)
    backend.tf       # remote state config
  prod/
    main.tf
    terraform.tfvars
    backend.tf
```

### versions.tf — Pin Providers

```hcl
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"   # allows 5.x but not 6.x
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.20.0, < 3.0.0"
    }
  }
}
```

---

## variables.tf Patterns

### Variable Validation

```hcl
variable "environment" {
  type        = string
  description = "Deployment environment. Controls resource sizing and retention."

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "bucket_name" {
  type        = string
  description = "S3 bucket name for the data lake landing zone."

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "Bucket name must be 3–63 lowercase letters, digits, or hyphens."
  }
}

variable "retention_days" {
  type        = number
  description = "S3 lifecycle expiration for raw data in days."
  default     = 90

  validation {
    condition     = var.retention_days >= 1 && var.retention_days <= 3650
    error_message = "retention_days must be between 1 and 3650."
  }
}

variable "db_password" {
  type        = string
  description = "RDS master password. Injected from secrets manager at plan time."
  sensitive   = true   # never shown in plan output or logs
}

variable "tags" {
  type = map(string)
  description = "Resource tags applied to all resources."
  default = {}
}
```

### Locals for Derived Values

```hcl
# Use locals, not variables, for computed/derived values
locals {
  name_prefix = "${var.project}-${var.environment}"

  # Merge standard tags with caller-supplied tags
  common_tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
    },
    var.tags
  )

  # Conditional sizing
  node_count = var.environment == "prod" ? 3 : 1
}
```

---

## outputs.tf Patterns

```hcl
output "bucket_arn" {
  description = "ARN of the data lake S3 bucket."
  value       = aws_s3_bucket.data_lake.arn
}

output "bucket_name" {
  description = "Name of the data lake S3 bucket."
  value       = aws_s3_bucket.data_lake.id
}

output "kms_key_id" {
  description = "KMS key ID used for bucket encryption."
  value       = aws_kms_key.data_lake.key_id
  sensitive   = true
}
```

---

## Resource Naming Conventions

```hcl
# Pattern: <project>-<environment>-<component>[-<index>]

resource "aws_s3_bucket" "data_lake" {
  bucket = "${local.name_prefix}-data-lake"
  tags   = local.common_tags
}

resource "aws_iam_role" "airflow_task" {
  name = "${local.name_prefix}-airflow-task-role"
  tags = local.common_tags
}

# For multi-instance: use for_each over count (preserves keys on deletion)
resource "aws_s3_bucket" "zones" {
  for_each = toset(["raw", "processed", "curated"])

  bucket = "${local.name_prefix}-${each.key}"
  tags   = merge(local.common_tags, { Zone = each.key })
}
```

---

## Remote State Backend

```hcl
# backend.tf
terraform {
  backend "s3" {
    bucket         = "my-company-terraform-state"
    key            = "data-platform/prod/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    kms_key_id     = "alias/terraform-state"
    dynamodb_table = "terraform-state-lock"   # prevents concurrent applies
  }
}
```

### State Output Cross-Module Reference

```hcl
# Read output from another module's state
data "terraform_remote_state" "networking" {
  backend = "s3"
  config = {
    bucket = "my-company-terraform-state"
    key    = "networking/prod/terraform.tfstate"
    region = "us-east-1"
  }
}

# Use it
resource "aws_db_subnet_group" "main" {
  subnet_ids = data.terraform_remote_state.networking.outputs.private_subnet_ids
}
```

---

## Data Sources vs Resources

```hcl
# data source: read existing infra (don't manage it)
data "aws_vpc" "main" {
  filter {
    name   = "tag:Environment"
    values = [var.environment]
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]  # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

# resource: manage lifecycle (create/update/destroy)
resource "aws_subnet" "private" {
  vpc_id     = data.aws_vpc.main.id
  cidr_block = "10.0.1.0/24"
}
```

---

## depends_on — Use Sparingly

```hcl
# Implicit dependency (preferred): reference the resource attribute
resource "aws_iam_role_policy" "airflow" {
  role   = aws_iam_role.airflow.name   # implicit dependency on aws_iam_role.airflow
  policy = data.aws_iam_policy_document.airflow.json
}

# Explicit depends_on: only when Terraform can't infer the dependency
resource "aws_s3_bucket_policy" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  policy = data.aws_iam_policy_document.data_lake.json

  depends_on = [aws_s3_bucket_public_access_block.data_lake]
}
```

---

## Linting and Validation

```bash
# Format check (run in CI)
terraform fmt -check -recursive

# Syntax + validity
terraform validate

# tflint — catches provider-specific errors and deprecations
tflint --init
tflint --recursive

# tflint config
cat .tflint.hcl
```

```hcl
# .tflint.hcl
plugin "aws" {
  enabled = true
  version = "0.27.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}

rule "terraform_required_version" { enabled = true }
rule "terraform_required_providers" { enabled = true }
rule "terraform_naming_convention" {
  enabled = true
  module {
    format = "snake_case"
  }
}
rule "aws_instance_invalid_type" { enabled = true }
```

```bash
# Generate docs from variables.tf + outputs.tf
terraform-docs markdown table . > README.md
```

---

## Terragrunt for DRY Configs

```hcl
# terragrunt.hcl (root)
locals {
  account_vars = read_terragrunt_config(find_in_parent_folders("account.hcl"))
  env_vars     = read_terragrunt_config(find_in_parent_folders("env.hcl"))

  account_id  = local.account_vars.locals.account_id
  environment = local.env_vars.locals.environment
  aws_region  = local.env_vars.locals.aws_region
}

remote_state {
  backend = "s3"
  generate = {
    path      = "backend.tf"
    if_exists = "overwrite"
  }
  config = {
    bucket         = "tfstate-${local.account_id}"
    key            = "${path_relative_to_include()}/terraform.tfstate"
    region         = local.aws_region
    encrypt        = true
    dynamodb_table = "terraform-locks"
  }
}

generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite"
  contents  = <<EOF
provider "aws" {
  region = "${local.aws_region}"
}
EOF
}
```

```hcl
# envs/prod/s3-data-lake/terragrunt.hcl
include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../../../modules/s3-data-lake"
}

inputs = {
  environment    = "prod"
  retention_days = 365
  bucket_name    = "my-company-prod-data-lake"
}
```

---

## Review Checklist

```
[ ] Provider versions pinned (~> minor, not =)
[ ] required_version set for Terraform itself
[ ] All variables have type + description + validation
[ ] Sensitive variables marked sensitive = true
[ ] Derived values in locals{}, not repeated inline
[ ] Resources use for_each instead of count where key stability matters
[ ] Remote state backend configured with encryption + lock
[ ] No hardcoded account IDs, regions, or passwords
[ ] common_tags applied to all resources
[ ] terraform fmt passes cleanly
[ ] terraform validate passes
[ ] tflint passes with AWS plugin
[ ] No overly broad IAM policies (*:* actions or resources)
[ ] outputs.tf covers all values a caller might need
[ ] README generated by terraform-docs
```

---

## Anti-Patterns

1. **`count` for named resources** — deleting mid-list shifts all indices; use `for_each = toset(...)` instead.
2. **Hardcoded ARNs and account IDs** — breaks across accounts; use `data "aws_caller_identity"` and `data "aws_region"`.
3. **No provider version constraints** — a major upgrade can silently break all resources; always pin with `~>`.
4. **Storing secrets in tfvars** — committed to git and logged in state; inject via environment variable or secrets manager.
5. **Giant `main.tf` file** — hard to review; split into `s3.tf`, `iam.tf`, `rds.tf` per resource type.
6. **`terraform apply` without plan review** — always run `plan` first, review changes, then `apply`; automate this in CI.

---

## References

- Terraform module structure: `developer.hashicorp.com/terraform/language/modules/develop/structure`
- Variable validation: `developer.hashicorp.com/terraform/language/values/variables`
- tflint: `github.com/terraform-linters/tflint`
- terraform-docs: `terraform-docs.io`
- Terragrunt: `terragrunt.gruntwork.io/docs/`
- Related skills: `[[infra-terraform-security-scan]]`, `[[infra-terraform-cost-estimator]]`, `[[github-actions-dataops]]`
