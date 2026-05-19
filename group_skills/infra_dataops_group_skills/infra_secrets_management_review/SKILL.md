---
name: infra-secrets-management-review
description: Secrets management review — HashiCorp Vault (KV v2/dynamic credentials/lease renewal/audit log), External Secrets Operator (Vault/AWS SSM/GCP Secret Manager → K8s secrets), secret rotation strategies (database dynamic credentials/TLS cert-manager), detecting secrets in code (gitleaks/truffleHog/Semgrep), Airflow connections from Vault backend, dbt profiles without hardcoded credentials, CI/CD secrets (GitHub OIDC/GitLab CI variables), no plaintext secrets in logs/configs
---

# Secrets Management Review

## When to Use

- Auditing how secrets are stored and accessed in a data platform
- Migrating from hardcoded credentials to a secrets manager
- Setting up dynamic database credentials via Vault
- Detecting secret leaks in git history or application logs
- Implementing automatic secret rotation

---

## Vault KV v2 — Static Secrets

```bash
# Enable KV v2 secrets engine
vault secrets enable -path=data-platform kv-v2

# Store secrets
vault kv put data-platform/production/trino \
  host="trino.prod.internal" \
  user="etl_user" \
  password="$(openssl rand -base64 32)"

vault kv put data-platform/production/kafka \
  bootstrap_servers="kafka-1:9092,kafka-2:9092,kafka-3:9092" \
  sasl_username="pipeline_user" \
  sasl_password="$(openssl rand -base64 32)"

# Retrieve
vault kv get -format=json data-platform/production/trino | jq '.data.data'

# Version history
vault kv metadata get data-platform/production/trino
```

```hcl
# Vault policy for Airflow workers
resource "vault_policy" "airflow_worker" {
  name = "airflow-worker"
  policy = <<EOT
# Read-only access to data platform secrets
path "data-platform/data/production/*" {
  capabilities = ["read"]
}
path "data-platform/metadata/production/*" {
  capabilities = ["read", "list"]
}
# Dynamic database credentials
path "database/creds/airflow-trino" {
  capabilities = ["read"]
}
EOT
}
```

---

## Vault Dynamic Database Credentials

```hcl
# Vault generates short-lived DB credentials — no static password to rotate
resource "vault_database_secrets_mount" "main" {
  path = "database"
}

resource "vault_database_secret_backend_connection" "trino" {
  backend       = vault_database_secrets_mount.main.path
  name          = "trino"
  allowed_roles = ["airflow-trino", "dbt-trino"]

  # Trino via PostgreSQL protocol
  postgresql {
    connection_url = "postgresql://{{username}}:{{password}}@trino-coordinator:5432/default"
    username       = "vault_admin"
    password       = var.vault_db_admin_password
  }
}

resource "vault_database_secret_backend_role" "airflow" {
  backend = vault_database_secrets_mount.main.path
  name    = "airflow-trino"
  db_name = vault_database_secret_backend_connection.trino.name

  creation_statements = [
    "CREATE USER \"{{name}}\" WITH PASSWORD '{{password}}' VALID UNTIL '{{expiration}}';",
    "GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA silver TO \"{{name}}\";",
  ]
  revocation_statements = ["DROP USER IF EXISTS \"{{name}}\";"]

  default_ttl = "1h"
  max_ttl     = "24h"
}
```

```python
# Airflow: dynamic Trino credentials via Vault
import hvac
from airflow.hooks.base import BaseHook

def get_trino_connection():
    vault = hvac.Client(url=os.environ["VAULT_ADDR"])
    vault.auth.kubernetes.login(
        role="airflow-worker",
        jwt=open("/var/run/secrets/kubernetes.io/serviceaccount/token").read()
    )
    creds = vault.secrets.database.generate_credentials(name="airflow-trino")
    return {
        "host": "trino.prod.internal",
        "username": creds["data"]["username"],
        "password": creds["data"]["password"],
        "lease_id": creds["lease_id"],    # renew before expiry
    }
```

---

## External Secrets Operator

```yaml
# ClusterSecretStore: connect ESO to Vault
apiVersion: external-secrets.io/v1beta1
kind: ClusterSecretStore
metadata:
  name: vault-backend
spec:
  provider:
    vault:
      server: http://vault.vault.svc.cluster.local:8200
      path: data-platform
      version: v2
      auth:
        kubernetes:
          mountPath: kubernetes
          role: external-secrets-operator
          serviceAccountRef:
            name: external-secrets
            namespace: external-secrets

---
# ExternalSecret: pull from Vault → K8s Secret
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: trino-credentials
  namespace: airflow
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: ClusterSecretStore
  target:
    name: trino-credentials        # name of K8s Secret to create
    creationPolicy: Owner
    template:
      type: Opaque
  data:
    - secretKey: password
      remoteRef:
        key: production/trino
        property: password
    - secretKey: username
      remoteRef:
        key: production/trino
        property: user
```

---

## Airflow Vault Secret Backend

```ini
# airflow.cfg
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {
  "connections_path": "data-platform/data/airflow/connections",
  "variables_path": "data-platform/data/airflow/variables",
  "url": "http://vault.vault.svc.cluster.local:8200",
  "auth_type": "kubernetes",
  "role_id": "airflow"
}
```

```bash
# Store Airflow connection in Vault (not Airflow DB)
vault kv put data-platform/airflow/connections/trino_production \
  conn_type="trino" \
  host="trino.prod.internal" \
  port="443" \
  schema="gold" \
  login="airflow_user" \
  password="..." \
  extra='{"auth": "ldap", "http_scheme": "https"}'
```

---

## Secret Scanning (Prevent Leaks)

```yaml
# .pre-commit-config.yaml — detect secrets before commit
repos:
  - repo: https://github.com/gitleaks/gitleaks
    rev: v8.18.0
    hooks:
      - id: gitleaks

  - repo: https://github.com/trufflesecurity/trufflehog
    rev: v3.67.0
    hooks:
      - id: trufflehog
        args: [--only-verified]
```

```bash
# Scan git history for leaked secrets
gitleaks detect --source . --report-format json --report-path leaks.json

# Scan with TruffleHog (detects verified credentials)
trufflehog git file://. --only-verified

# Semgrep: find hardcoded credentials patterns
semgrep --config "p/secrets" --output results.sarif .

# GitHub Actions secret scanner on every PR
jobs:
  secret-scan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }  # full history for scanning

      - name: Gitleaks scan
        uses: gitleaks/gitleaks-action@v2
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

---

## dbt Profiles Without Hardcoded Credentials

```yaml
# profiles.yml — use env_var() everywhere
production:
  target: prod
  outputs:
    prod:
      type: trino
      host: "{{ env_var('DBT_TRINO_HOST') }}"
      port: 443
      user: "{{ env_var('DBT_TRINO_USER') }}"
      password: "{{ env_var('DBT_TRINO_PASSWORD') }}"
      database: production
      schema: gold
      http_scheme: https
```

```bash
# Inject from Vault at runtime
eval $(vault kv get -format=json data-platform/production/trino \
  | jq -r '.data.data | to_entries[] | "export DBT_TRINO_\(.key | ascii_upcase)=\(.value)"')
dbt run --target prod
```

---

## Secret Rotation Checklist

```
[ ] No secrets in code (git pre-commit gitleaks hook active)
[ ] No secrets in Airflow Variables or Connections (use Vault backend)
[ ] No secrets in Kubernetes Secrets directly (use External Secrets Operator)
[ ] No secrets in terraform.tfvars committed to git
[ ] Database credentials: dynamic via Vault (TTL 1h)
[ ] TLS certificates: auto-renewed via cert-manager
[ ] API keys: rotated quarterly, stored in Vault
[ ] CI/CD: OIDC instead of static cloud credentials
[ ] Vault audit log enabled and shipped to SIEM
[ ] All secrets have owner label (team/system using them)
```

---

## Anti-Patterns

1. **Secrets in environment variables in Deployment YAML** — visible to anyone with `kubectl describe pod`; use ExternalSecrets or secretKeyRef.
2. **Long-lived static DB passwords** — a leaked password is valid indefinitely; use Vault dynamic credentials with 1h TTL.
3. **Secrets in Airflow Variables (metadata DB)** — Variables are not encrypted at rest by default; use Vault Secret Backend for sensitive values.
4. **GitHub repository secrets with never-expiring API keys** — use OIDC for AWS/GCP/Azure; for services that don't support OIDC, set expiry and rotate.
5. **No pre-commit secret scanning** — developers accidentally commit API keys; gitleaks catches them before push.
6. **Vault without audit log** — who accessed what secret is unauditable; always enable Vault audit log and ship to a SIEM.

---

## References

- HashiCorp Vault: `vaultproject.io/docs`
- External Secrets Operator: `external-secrets.io/docs/`
- Gitleaks: `github.com/gitleaks/gitleaks`
- cert-manager: `cert-manager.io/docs/`
- Related skills: `[[infra-kubernetes-security-audit]]`, `[[infra-rbac-audit]]`, `[[dataops-airflow-production-readiness]]`
