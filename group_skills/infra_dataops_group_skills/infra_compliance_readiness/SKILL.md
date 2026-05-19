---
name: infra-compliance-readiness
description: Compliance readiness for data platforms — SOC2 Type II controls mapping (CC6/CC7/CC8), GDPR data processing requirements (PII inventory/right-to-erasure/data residency), PCI-DSS data platform scope reduction, CIS Benchmarks (Kubernetes/AWS/GCP), audit log completeness (Vault/Kubernetes/cloud trail), data retention and deletion policies, encryption at rest/transit evidence, access review processes, compliance-as-code (OPA policies), vulnerability management (CVE SLA)
---

# Compliance Readiness

## When to Use

- Preparing for SOC2 Type II audit of a data platform
- GDPR compliance review (PII handling, right-to-erasure implementation)
- CIS Benchmark assessment for Kubernetes clusters
- Establishing audit evidence collection processes
- Mapping data platform controls to compliance frameworks

---

## SOC2 Type II Control Mapping

```yaml
# SOC2 CC6 — Logical and Physical Access Controls
CC6.1: # Limit logical access to protect information assets
  controls:
    - Kubernetes RBAC: namespace-scoped roles, no wildcard permissions
    - Vault RBAC: policies scoped to specific secret paths
    - Database: separate user per service, no shared credentials
    - Cloud IAM: IRSA/Workload Identity, no static keys
  evidence:
    - kubectl get clusterrolebindings -o json  # quarterly screenshot
    - vault policy list && vault policy read <policy>
    - aws iam generate-credential-report

CC6.6: # Protect against security threats from outside the system
  controls:
    - No public IPs on data services
    - Kubernetes NetworkPolicy default-deny
    - Security groups with no 0.0.0.0/0 ingress
    - Container vulnerability scanning (Trivy) in CI
  evidence:
    - Trivy scan results (SARIF) from last 90 days
    - AWS Config rule: restricted-ssh, no-unrestricted-ports
    - Network topology diagram

CC7.2: # Monitor system components for anomalies
  controls:
    - Prometheus alerting on error rates, SLA misses
    - CloudTrail + VPC Flow Logs
    - Vault audit log
    - Kubernetes API server audit log
  evidence:
    - Grafana dashboard screenshots
    - Alert history (AlertManager resolved incidents)
    - Vault audit log sample

CC8.1: # Change management process
  controls:
    - All infra changes via Terraform PR + review
    - ArgoCD GitOps for Kubernetes deployments
    - dbt slim CI for SQL changes
    - No manual changes to production (enforced by IAM)
  evidence:
    - GitHub PR history with approval evidence
    - ArgoCD sync history
```

---

## GDPR PII Inventory

```sql
-- Create PII data inventory table
CREATE TABLE compliance.pii_inventory (
    table_schema    VARCHAR,
    table_name      VARCHAR,
    column_name     VARCHAR,
    pii_category    VARCHAR,  -- 'name', 'email', 'phone', 'address', 'ssn', 'dob'
    processing_basis VARCHAR,  -- 'legitimate_interest', 'contract', 'consent'
    retention_days  INTEGER,
    encryption      BOOLEAN,
    masking_applied BOOLEAN,
    data_subject_id_column VARCHAR,
    owner_team      VARCHAR,
    last_reviewed   DATE
);

-- Populate from Glue/Iceberg metadata + manual annotation
INSERT INTO compliance.pii_inventory VALUES
('gold', 'customers', 'email',          'email',   'contract',             365, true, false, 'customer_id', 'data-engineering', CURRENT_DATE),
('gold', 'customers', 'phone',          'phone',   'contract',             365, true, false, 'customer_id', 'data-engineering', CURRENT_DATE),
('gold', 'customers', 'full_name',      'name',    'contract',             365, true, false, 'customer_id', 'data-engineering', CURRENT_DATE),
('silver', 'orders_enriched', 'ip_address', 'network_id', 'legitimate_interest', 90, true, true,  'customer_id', 'data-engineering', CURRENT_DATE);
```

### Right-to-Erasure Implementation

```python
def handle_erasure_request(customer_id: str, request_id: str):
    """GDPR Article 17: Right to Erasure implementation."""
    log.info("Processing erasure request", extra={
        "customer_id": customer_id,
        "request_id": request_id,
    })

    erasure_tables = [
        ("gold", "customers"),
        ("silver", "orders_enriched"),
        ("bronze", "clickstream_raw"),
    ]

    for schema, table in erasure_tables:
        # Check PII inventory for this table
        pii_cols = get_pii_columns(schema, table)
        if not pii_cols:
            continue

        # Replace PII with anonymized values (not hard delete for audit trail)
        conn.execute(f"""
            UPDATE {schema}.{table}
            SET
                {', '.join(f"{col} = 'REDACTED_GDPR_{request_id[:8]}'" for col in pii_cols)},
                gdpr_erased_at = CURRENT_TIMESTAMP,
                gdpr_request_id = '{request_id}'
            WHERE customer_id = '{customer_id}'
        """)

    # Log completion for compliance evidence
    conn.execute(f"""
        INSERT INTO compliance.erasure_log
        VALUES ('{request_id}', '{customer_id}', CURRENT_TIMESTAMP, 'COMPLETED')
    """)
```

---

## CIS Kubernetes Benchmark (Automated)

```bash
# Run kube-bench (CIS Kubernetes Benchmark)
kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: kube-bench
spec:
  template:
    spec:
      hostPID: true
      containers:
      - name: kube-bench
        image: aquasec/kube-bench:latest
        command: ["kube-bench"]
        volumeMounts:
        - name: var-lib-etcd
          mountPath: /var/lib/etcd
          readOnly: true
        - name: etc-kubernetes
          mountPath: /etc/kubernetes
          readOnly: true
      volumes:
      - name: var-lib-etcd
        hostPath:
          path: /var/lib/etcd
      - name: etc-kubernetes
        hostPath:
          path: /etc/kubernetes
      restartPolicy: Never
EOF

kubectl logs -l job-name=kube-bench --tail=200 | \
  grep -E "^\[FAIL\]|^\[WARN\]" | head -50
```

---

## Audit Log Completeness

```bash
# Kubernetes API server audit logs
# Should capture: read of secrets, pod exec, privileged container creation

# Verify audit logging is enabled
kubectl get apiserver -o yaml | grep -A 10 "audit"

# Check recent privileged operations
kubectl get events -A | grep -i "privileged\|exec\|secret"

# CloudTrail: verify all regions have logging
aws cloudtrail describe-trails | \
  jq '.trailList[] | select(.IsMultiRegionTrail == true) | {Name, S3BucketName, IsLogging}'

# Vault audit log (ship to SIEM)
vault audit enable file file_path=/var/log/vault/audit.log

# Query Vault audit log for secret access
jq 'select(.type == "response" and .auth.metadata.username != null) |
    {time: .time, user: .auth.metadata.username, path: .request.path, op: .request.operation}' \
  /var/log/vault/audit.log | head -50
```

---

## Encryption Evidence

```bash
# AWS: verify all S3 buckets have encryption
aws s3api list-buckets --query "Buckets[].Name" --output text | \
  tr '\t' '\n' | while read bucket; do
    enc=$(aws s3api get-bucket-encryption --bucket $bucket 2>/dev/null | \
      jq -r '.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm')
    if [ -z "$enc" ] || [ "$enc" = "null" ]; then
      echo "⚠️  No encryption: $bucket"
    else
      echo "✅ $bucket: $enc"
    fi
  done

# Kubernetes: verify etcd encryption at rest
kubectl get apiserver -o yaml | grep -A 20 "encryption"

# Database: verify TLS in transit
psql "$AIRFLOW_DB_CONN" -c "SHOW ssl;"
psql "$AIRFLOW_DB_CONN" -c "SELECT count(*) FROM pg_stat_ssl WHERE ssl = true;"
```

---

## Compliance-as-Code (OPA)

```rego
# policies/data-compliance.rego
package data_platform.compliance

# GDPR: all tables with PII must have encryption annotation
deny[msg] {
  table := input.resource.aws_glue_catalog_table[name]
  not table.parameters["pii_encrypted"]
  table.parameters["contains_pii"] == "true"
  msg := sprintf("Glue table '%s' contains PII but encryption is not confirmed", [name])
}

# Data retention: no table without retention policy
deny[msg] {
  table := input.resource.aws_glue_catalog_table[name]
  not table.parameters["retention_days"]
  msg := sprintf("Glue table '%s' has no retention_days parameter", [name])
}

# SOC2 CC6: no public S3 buckets
deny[msg] {
  bucket := input.resource.aws_s3_bucket_public_access_block[name]
  not bucket.restrict_public_buckets
  msg := sprintf("S3 bucket '%s' does not have restrict_public_buckets=true", [name])
}
```

---

## Vulnerability Management Policy

```yaml
# CVE response SLA by severity
vulnerability_sla:
  critical:
    sla_days: 3
    action: "Patch or mitigate immediately, notify security team"
  high:
    sla_days: 14
    action: "Patch in next sprint, track in security board"
  medium:
    sla_days: 30
    action: "Include in quarterly hardening"
  low:
    sla_days: 90
    action: "Track and patch during regular dependency updates"

# Trivy scan in CI: block CRITICAL, warn HIGH
- name: Trivy container scan
  uses: aquasecurity/trivy-action@master
  with:
    image-ref: myimage:latest
    severity: CRITICAL,HIGH
    exit-code: '1'        # block on CRITICAL
    ignore-unfixed: true  # skip if no fix available
```

---

## Compliance Readiness Checklist

```
SOC2:
[ ] Access review documented and performed quarterly
[ ] All infrastructure changes via PR + approval (no manual prod access)
[ ] Incident response plan documented and tested
[ ] Vulnerability scan results retained 12 months
[ ] Audit logs (CloudTrail, Vault, K8s) retained 12 months

GDPR:
[ ] PII inventory complete and reviewed semi-annually
[ ] Right-to-erasure process implemented and tested
[ ] Data processing agreements with all third parties
[ ] Data residency requirements enforced (EU data stays in EU)
[ ] Privacy by design: PII anonymized in non-prod environments

Technical Controls:
[ ] CIS Benchmark scan: no FAIL items in control plane
[ ] All data encrypted at rest (KMS/CMEK) and in transit (TLS)
[ ] MFA required for all human access to production
[ ] No shared credentials across services
[ ] Penetration test conducted annually
```

---

## Anti-Patterns

1. **Compliance evidence collected manually at audit time** — scrambling to collect screenshots right before audit; automate evidence collection into a compliance dashboard.
2. **PII in non-production environments** — staging should use anonymized data; PII in dev violates GDPR even if access is restricted.
3. **No right-to-erasure implementation** — GDPR Article 17 requires it; hard to implement after the fact when data is spread across 50 tables.
4. **Audit logs with 30-day retention** — SOC2 requires 12 months; set S3 lifecycle and CloudWatch log group retention before audit.
5. **Manual access reviews** — "we reviewed it 18 months ago" fails SOC2; schedule quarterly access review as a recurring calendar event with evidence collection.

---

## References

- SOC2 Trust Services Criteria: `aicpa.org/resources/article/2017-trust-services-criteria`
- GDPR: `gdpr.eu`
- CIS Benchmarks: `cisecurity.org/cis-benchmarks/`
- kube-bench: `github.com/aquasecurity/kube-bench`
- Related skills: `[[infra-rbac-audit]]`, `[[infra-secrets-management-review]]`, `[[infra-network-security-review]]`
