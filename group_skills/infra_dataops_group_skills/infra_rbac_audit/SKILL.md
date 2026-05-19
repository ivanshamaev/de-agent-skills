---
name: infra-rbac-audit
description: RBAC audit for data platforms — Kubernetes RBAC (ClusterRole/Role/Binding audit), AWS IAM permission boundary and policy review (AWS IAM Access Analyzer), GCP IAM audit (principle of least privilege/owner role detection), database RBAC (PostgreSQL/Trino/ClickHouse role hierarchies), Airflow RBAC (role-based menu/DAG access), dbt permissions (warehouse roles), service account proliferation detection, wildcard permission detection, privilege escalation paths
---

# RBAC Audit

## When to Use

- Pre-compliance audit (SOC2, GDPR, PCI-DSS access control requirements)
- Investigating a potential insider threat or credential misuse
- Onboarding a new team — reviewing what access they need vs have
- Quarterly permission review across all data platform components
- Identifying service accounts with excessive permissions

---

## Kubernetes RBAC Audit

```bash
# Find all ClusterRoleBindings with cluster-admin
kubectl get clusterrolebindings -o json | jq -r '
  .items[] |
  select(.roleRef.name == "cluster-admin") |
  "ClusterRoleBinding: \(.metadata.name)
   Subjects: \([.subjects[]? | "\(.kind)/\(.name)"] | join(", "))"
'

# Find wildcard permissions in ClusterRoles
kubectl get clusterroles -o json | jq -r '
  .items[] |
  .metadata.name as $name |
  .rules[]? |
  select(.verbs[] == "*" or .resources[] == "*") |
  "Role: \($name) | resources: \(.resources) | verbs: \(.verbs)"
'

# What can a service account do?
kubectl auth can-i --list \
  -n production \
  --as system:serviceaccount:production:airflow-worker

# Find service accounts that automount tokens (default: true)
kubectl get serviceaccounts -A -o json | jq -r '
  .items[] |
  select(.automountServiceAccountToken != false) |
  "\(.metadata.namespace)/\(.metadata.name)"
' | grep -v "^kube-"
```

### RBAC Audit Script

```python
#!/usr/bin/env python3
"""Kubernetes RBAC audit — find over-privileged service accounts."""
import subprocess
import json

def audit_rbac():
    issues = []

    # 1. cluster-admin bindings
    result = subprocess.run(
        ["kubectl", "get", "clusterrolebindings", "-o", "json"],
        capture_output=True, text=True
    )
    bindings = json.loads(result.stdout)
    for b in bindings["items"]:
        if b["roleRef"]["name"] == "cluster-admin":
            subjects = [f"{s['kind']}/{s['name']}" for s in b.get("subjects", [])]
            issues.append({
                "severity": "CRITICAL",
                "type": "cluster-admin-binding",
                "resource": b["metadata"]["name"],
                "subjects": subjects,
                "recommendation": "Replace with namespace-scoped Role with minimal permissions",
            })

    # 2. wildcard permissions
    result = subprocess.run(
        ["kubectl", "get", "clusterroles", "-o", "json"],
        capture_output=True, text=True
    )
    roles = json.loads(result.stdout)
    for role in roles["items"]:
        for rule in role.get("rules", []):
            if "*" in rule.get("verbs", []) and role["metadata"]["name"] not in ["cluster-admin", "admin", "edit", "view"]:
                issues.append({
                    "severity": "HIGH",
                    "type": "wildcard-verbs",
                    "resource": role["metadata"]["name"],
                    "recommendation": f"Replace 'verbs: [\"*\"]' with specific verbs: {rule.get('resources')}",
                })

    return issues

for issue in sorted(audit_rbac(), key=lambda x: x["severity"]):
    print(f"[{issue['severity']}] {issue['type']}: {issue['resource']}")
    print(f"  → {issue['recommendation']}\n")
```

---

## AWS IAM Audit

```bash
# AWS IAM Access Analyzer — find external access
aws accessanalyzer create-analyzer \
  --analyzer-name data-platform-analyzer \
  --type ACCOUNT

# List findings (resources accessible from outside the account)
aws accessanalyzer list-findings \
  --analyzer-arn arn:aws:access-analyzer:us-east-1:123456789012:analyzer/data-platform-analyzer \
  --query 'findings[?status==`ACTIVE`]' \
  | jq '.[] | {resource: .resource, resourceType: .resourceType, action: .action}'

# Find policies with AdministratorAccess
aws iam list-policies --scope Local --query "Policies[?PolicyName!='']" | \
  jq -r '.[].Arn' | while read arn; do
    doc=$(aws iam get-policy-version \
      --policy-arn $arn \
      --version-id $(aws iam get-policy --policy-arn $arn --query "Policy.DefaultVersionId" --output text) \
      --query "PolicyVersion.Document")
    echo $doc | jq -e '
      .Statement[] |
      select(.Effect == "Allow" and
             (.Action | if type == "array" then contains(["*"]) else . == "*" end) and
             (.Resource | if type == "array" then contains(["*"]) else . == "*" end))
    ' > /dev/null 2>&1 && echo "⚠️  Admin-equivalent policy: $arn"
  done

# Check unused IAM roles (90+ days)
aws iam get-account-authorization-details | \
  jq -r '.RoleDetailList[] |
  select(.RoleLastUsed.LastUsedDate != null) |
  "\(.RoleName): last_used=\(.RoleLastUsed.LastUsedDate)"'
```

---

## GCP IAM Audit

```bash
# Find project-level owner/editor bindings (overly broad)
gcloud projects get-iam-policy my-project \
  --format=json | jq '
  .bindings[] |
  select(.role | test("roles/(owner|editor)")) |
  "Role: \(.role) | Members: \(.members | join(", "))"
'

# Find service accounts with owner role
gcloud projects get-iam-policy my-project \
  --format=json | jq -r '
  .bindings[] |
  select(.role == "roles/owner") |
  .members[] | select(startswith("serviceAccount:"))
'

# Check all service account key usage
gcloud iam service-accounts list --format="value(email)" | \
  while read sa; do
    keys=$(gcloud iam service-accounts keys list \
      --iam-account=$sa --format="value(name,validAfterTime)" 2>/dev/null)
    if [ -n "$keys" ]; then
      echo "SA: $sa → Keys: $keys"
    fi
  done

# Find bindings older than 90 days without recent activity
gcloud asset search-all-iam-policies \
  --scope=projects/my-project \
  --query="policy.bindings.role:roles/bigquery.dataEditor" \
  --format="json" | jq '.[].policy.bindings'
```

---

## Database RBAC Audit

### PostgreSQL (Airflow Metadata DB)

```sql
-- Users with superuser or CREATEDB
SELECT usename, usesuper, usecreatedb, usecreaterole
FROM pg_user
WHERE usesuper OR usecreatedb
ORDER BY usename;

-- Table-level grants
SELECT grantee, table_schema, table_name, privilege_type
FROM information_schema.table_privileges
WHERE table_schema = 'public'
  AND grantee NOT IN ('postgres', 'PUBLIC')
ORDER BY grantee, table_name;

-- Roles and their members
SELECT r.rolname AS role, m.rolname AS member
FROM pg_roles r
JOIN pg_auth_members am ON am.roleid = r.oid
JOIN pg_roles m ON m.oid = am.member
ORDER BY r.rolname;
```

### Trino

```sql
-- List all grants
SHOW GRANTS ON SCHEMA gold TO PUBLIC;

-- Find users with ALL PRIVILEGES
SELECT * FROM system.metadata.table_privileges
WHERE privilege_type = 'SELECT'
  AND grantee = 'PUBLIC';   -- PUBLIC means everyone — audit this

-- Revoke overly broad grants
REVOKE ALL PRIVILEGES ON SCHEMA gold FROM PUBLIC;
GRANT SELECT ON TABLE gold.fact_orders TO ROLE analyst;
```

---

## Airflow RBAC Audit

```python
# Programmatic Airflow RBAC review
from airflow.www.security import AirflowSecurityManager
from airflow import settings
from sqlalchemy.orm import Session

def audit_airflow_roles():
    with Session(settings.engine) as session:
        from flask_appbuilder.security.sqla.models import User, Role
        
        users = session.query(User).all()
        for user in users:
            roles = [r.name for r in user.roles]
            if "Admin" in roles:
                print(f"⚠️  Admin user: {user.username} ({user.email})")
            elif "Op" in roles:
                print(f"ℹ️  Operator user: {user.username} - roles: {roles}")

# DAG-level access (Airflow 2.4+)
# Define in airflow.cfg:
# [webserver]
# rbac = True
# [core]
# auth_backends = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
```

---

## Service Account Proliferation

```bash
# Kubernetes: service accounts per namespace
kubectl get serviceaccounts -A --no-headers | wc -l

# Find service accounts with no associated workloads (orphaned)
kubectl get serviceaccounts -A -o json | jq -r '
  .items[] |
  .metadata.namespace as $ns |
  .metadata.name as $name |
  select($name != "default") |
  "\($ns)/\($name)"
' | while IFS='/' read ns sa; do
    bindings=$(kubectl get rolebindings,clusterrolebindings -A -o json 2>/dev/null | \
      jq -r --arg ns "$ns" --arg sa "$sa" '
        .items[] | select(.subjects[]? | .namespace == $ns and .name == $sa) |
        .metadata.name')
    if [ -z "$bindings" ]; then
      echo "Orphaned SA (no role bindings): $ns/$sa"
    fi
  done
```

---

## RBAC Audit Checklist

```
Kubernetes:
[ ] No cluster-admin bindings for application service accounts
[ ] No wildcard verbs (*) in custom ClusterRoles
[ ] automountServiceAccountToken=false on all SAs that don't need API access
[ ] All workloads use dedicated SA (not default SA)

AWS:
[ ] No policies with Action:* + Resource:*
[ ] IAM Access Analyzer active with no active external-access findings
[ ] No unused roles (last used > 90 days)
[ ] MFA required for all human IAM users

GCP:
[ ] No service accounts with roles/owner or roles/editor
[ ] No service account JSON keys (use Workload Identity)
[ ] project-level bindings reviewed quarterly

Database:
[ ] No application users with superuser/CREATEDB
[ ] Analysts have SELECT only (not INSERT/UPDATE/DELETE)
[ ] Public schema access restricted

Platform:
[ ] Airflow: no Admin users outside platform team
[ ] Vault: policies scoped to specific secret paths
[ ] All RBAC changes go through PR review (GitOps)
```

---

## Anti-Patterns

1. **Shared service account for all pipelines** — one compromise exposes everything; each pipeline gets its own SA with minimal permissions.
2. **`Action: "*"` in IAM policies for data pipelines** — pipelines need S3 read/write and Glue catalog — not EC2 or IAM management; scope to exact actions.
3. **GCP Owner role on service accounts** — owners can grant themselves any permission, breaking least privilege; use specific data roles instead.
4. **No quarterly access review** — stale permissions accumulate silently; schedule quarterly audit of all human accounts.
5. **default Kubernetes ServiceAccount used by pods** — default SA may have accumulated bindings from other resources; always create dedicated SAs.

---

## References

- Kubernetes RBAC: `kubernetes.io/docs/reference/access-authn-authz/rbac/`
- AWS IAM Access Analyzer: `docs.aws.amazon.com/IAM/latest/UserGuide/what-is-access-analyzer.html`
- GCP IAM best practices: `cloud.google.com/iam/docs/using-iam-securely`
- Related skills: `[[infra-kubernetes-security-audit]]`, `[[infra-secrets-management-review]]`, `[[infra-compliance-readiness]]`
