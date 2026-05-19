---
name: infra-terraform-cost-estimator
description: Terraform cost estimation — Infracost breakdown and diff commands (monthly cost per resource), PR cost comments (before/after change delta), budget thresholds (block PRs over cost limit), usage files for variable quantities (S3 GB/requests), AWS pricing data sources, cost tagging strategy, OpenTofu/Terraform cost attribution, FinOps tagging policy enforcement, multi-environment cost comparison (dev vs prod)
---

# Terraform Cost Estimator

## When to Use

- Reviewing a PR that adds new infrastructure — get cost delta before merge
- Estimating monthly cloud cost for a proposed data platform architecture
- Setting budget guardrails that fail CI when cost increase exceeds threshold
- Attributing costs by team/project/environment through tagging
- Comparing costs across cloud providers for a workload

---

## Infracost — Terraform Cost Estimation

### Installation

```bash
# macOS
brew install infracost

# Linux
curl -fsSL https://raw.githubusercontent.com/infracost/infracost/master/scripts/install.sh | sh

# Authenticate (free API key)
infracost auth login
```

### Basic Commands

```bash
# Full cost breakdown of current Terraform code
infracost breakdown --path .

# Breakdown with usage estimates (S3 storage, Lambda invocations, etc.)
infracost breakdown --path . --usage-file infracost-usage.yml

# Show cost DIFF between current and changed code
# (compare against main branch plan)
infracost diff \
  --path . \
  --compare-to /tmp/infracost-base.json

# Output formats
infracost breakdown --path . --format json   > infracost.json
infracost breakdown --path . --format table  # human-readable
infracost breakdown --path . --format html   > cost-report.html
```

### Sample Output

```
Project: data-platform

 Name                                         Monthly Qty  Unit           Monthly Cost
 ─────────────────────────────────────────────────────────────────────────────────────
 aws_s3_bucket.data_lake
 ├─ Standard storage                               500 GB  GB-months            $11.50
 ├─ PUT/COPY/POST/LIST requests                 10,000 1k operations             $0.05
 └─ GET/SELECT/other requests                  100,000 1k operations             $0.04

 aws_rds_cluster.main
 ├─ Aurora MySQL (db.r6g.2xlarge) x 2            2 Months  instance           $876.00
 └─ Storage (I/O included)                       100 GB  GB-months            $10.00

 aws_msk_cluster.kafka
 └─ Kafka (kafka.m5.xlarge) x 3                  3 Months  instance           $655.20

 MONTHLY TOTAL                                                               $1,552.79
```

### Usage File for Variable Quantities

```yaml
# infracost-usage.yml — provide estimates for usage-based pricing
version: 0.1
resource_usage:
  aws_s3_bucket.data_lake:
    storage_gb: 500
    monthly_tier1_requests: 10000     # PUT/COPY/POST/LIST
    monthly_tier2_requests: 100000    # GET/SELECT

  aws_lambda_function.etl_processor:
    monthly_requests: 1000000
    request_duration_ms: 500

  aws_cloudwatch_log_group.airflow:
    storage_gb: 10
    monthly_data_ingested_gb: 50

  aws_athena_database.data_lake:
    monthly_data_scanned_tb: 1.5
```

---

## GitHub Actions CI Integration

### PR Cost Comment

```yaml
name: Infracost Cost Estimate

on:
  pull_request:
    paths:
      - "**.tf"
      - "**.tfvars"

jobs:
  infracost:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pull-requests: write

    env:
      INFRACOST_API_KEY: ${{ secrets.INFRACOST_API_KEY }}

    steps:
      - name: Checkout PR branch
        uses: actions/checkout@v4

      - name: Setup Infracost
        uses: infracost/actions/setup@v3

      - name: Checkout base branch (for diff)
        uses: actions/checkout@v4
        with:
          ref: ${{ github.event.pull_request.base.ref }}
          path: base

      - name: Generate base cost estimate
        run: |
          infracost breakdown \
            --path base \
            --format json \
            --out-file /tmp/infracost-base.json

      - name: Generate PR cost estimate
        run: |
          infracost breakdown \
            --path . \
            --format json \
            --out-file /tmp/infracost-pr.json

      - name: Post cost diff as PR comment
        run: |
          infracost diff \
            --path /tmp/infracost-pr.json \
            --compare-to /tmp/infracost-base.json \
            --format diff \
            --out-file /tmp/infracost-diff.txt
          gh pr comment ${{ github.event.number }} --body "$(cat /tmp/infracost-diff.txt)"
        env:
          GH_TOKEN: ${{ github.token }}
```

### Budget Gate — Fail PR on Cost Increase

```yaml
      - name: Check cost increase threshold
        run: |
          DIFF=$(infracost diff \
            --path /tmp/infracost-pr.json \
            --compare-to /tmp/infracost-base.json \
            --format json | jq '.diffTotalMonthlyCost | tonumber')

          echo "Monthly cost increase: $${DIFF}"

          # Fail if increase > $500/month
          if (( $(echo "$DIFF > 500" | bc -l) )); then
            echo "::error::Monthly cost increase ($${DIFF}) exceeds threshold ($500)"
            exit 1
          fi
```

---

## FinOps Tagging Strategy

### Mandatory Tags Policy in Terraform

```hcl
# variables.tf — enforce tags at module level
variable "tags" {
  type        = map(string)
  description = "Resource tags. team, project, and cost_center are required."

  validation {
    condition = (
      contains(keys(var.tags), "team") &&
      contains(keys(var.tags), "project") &&
      contains(keys(var.tags), "cost_center")
    )
    error_message = "tags must include: team, project, cost_center."
  }
}

locals {
  required_tags = {
    team        = var.tags["team"]
    project     = var.tags["project"]
    cost_center = var.tags["cost_center"]
    environment = var.environment
    managed_by  = "terraform"
    repo        = "data-platform"
  }
  all_tags = merge(local.required_tags, var.tags)
}
```

### Tag Enforcement via Checkov

```bash
# Fail on resources missing required tags
checkov -d . --check CKV_AWS_338  # EC2 tags
checkov -d . --check CKV2_AWS_20  # S3 tagging
```

### OPA Policy for Tagging

```rego
# policies/required_tags.rego
package terraform.tagging

required_tags := {"team", "project", "cost_center", "environment"}

deny[msg] {
  resource := input.resource[_][name]
  resource_tags := {tag | resource.tags[tag]}
  missing := required_tags - resource_tags
  count(missing) > 0
  msg := sprintf("Resource '%s' is missing required tags: %v", [name, missing])
}
```

---

## Multi-Environment Cost Comparison

```bash
# Compare dev vs prod cost estimates
infracost breakdown \
  --path envs/dev \
  --format json \
  > infracost-dev.json

infracost breakdown \
  --path envs/prod \
  --format json \
  > infracost-prod.json

# Generate comparison report
python3 << 'EOF'
import json

with open("infracost-dev.json") as f:
    dev = json.load(f)
with open("infracost-prod.json") as f:
    prod = json.load(f)

dev_cost = float(dev["totalMonthlyCost"])
prod_cost = float(prod["totalMonthlyCost"])

print(f"Dev monthly:  ${dev_cost:,.2f}")
print(f"Prod monthly: ${prod_cost:,.2f}")
print(f"Multiplier:   {prod_cost / dev_cost:.1f}x")
EOF
```

---

## Cost Estimation for Data Platform Components

```bash
# S3 data lake cost model
cat infracost-usage.yml << 'EOF'
version: 0.1
resource_usage:
  # Data lake zones — estimate by GB stored and requests/month
  aws_s3_bucket.bronze:
    storage_gb: 5000
    monthly_tier1_requests: 50000
  aws_s3_bucket.silver:
    storage_gb: 2000
    monthly_tier1_requests: 100000
  aws_s3_bucket.gold:
    storage_gb: 500
    monthly_tier1_requests: 200000

  # Athena queries — estimate by TB scanned
  aws_athena_workgroup.main:
    monthly_data_scanned_tb: 10

  # EMR cluster — on-demand hours per month
  aws_emr_cluster.spark:
    monthly_hrs: 200
EOF

infracost breakdown --path . --usage-file infracost-usage.yml
```

---

## Cost Reporting Dashboard (Python)

```python
import json
import subprocess
from pathlib import Path

def estimate_costs(terraform_dirs: list[str]) -> dict:
    results = {}
    for tf_dir in terraform_dirs:
        result = subprocess.run(
            ["infracost", "breakdown", "--path", tf_dir, "--format", "json"],
            capture_output=True, text=True
        )
        data = json.loads(result.stdout)
        results[tf_dir] = {
            "monthly_cost": float(data.get("totalMonthlyCost", 0)),
            "resources": [
                {
                    "name": r["name"],
                    "monthly_cost": float(r.get("monthlyCost", 0))
                }
                for r in data.get("projects", [{}])[0].get("breakdown", {}).get("resources", [])
                if float(r.get("monthlyCost", 0)) > 0
            ]
        }
    return results

dirs = ["modules/kafka", "modules/airflow", "modules/spark-history"]
costs = estimate_costs(dirs)

for module, info in sorted(costs.items(), key=lambda x: -x[1]["monthly_cost"]):
    print(f"\n{module}: ${info['monthly_cost']:,.2f}/mo")
    for r in sorted(info["resources"], key=lambda x: -x["monthly_cost"])[:5]:
        print(f"  {r['name']}: ${r['monthly_cost']:,.2f}")
```

---

## Anti-Patterns

1. **Ignoring cost estimation until after deploy** — compute and data resources can be 10x more expensive than estimated; shift cost review left to PR stage.
2. **No usage file for variable-cost resources** — S3, Lambda, Athena costs are zero without usage estimates; always provide `infracost-usage.yml`.
3. **No tagging on resources** — AWS Cost Explorer can't attribute costs to teams; enforce tags in Terraform variables.
4. **Comparing costs without plan diff** — `infracost breakdown` alone doesn't show what changed; always use `infracost diff` in CI to show deltas.
5. **Setting cost threshold too high** — a threshold of $10k/month won't catch a developer adding 3 high-memory RDS instances; tune per environment.

---

## References

- Infracost CLI: `infracost.io/docs/`
- Infracost GitHub Actions: `github.com/infracost/actions`
- AWS Pricing API: `aws.amazon.com/pricing/`
- OpenCost (runtime cost): `opencost.io`
- Related skills: `[[infra-terraform-review]]`, `[[infra-terraform-security-scan]]`, `[[infra-kubernetes-cost-optimizer]]`, `[[de-cost-optimization]]`
