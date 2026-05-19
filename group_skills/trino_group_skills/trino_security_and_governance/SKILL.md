---
name: trino-security-and-governance
description: Trino security hardening and data governance — TLS/HTTPS setup, internal-communication shared secret, authentication (LDAP/OAuth2/JWT), file-based access control (catalog/schema/table/column rules, column masking, row-level filtering, query kill permissions), OPA integration, Ranger for dynamic row-filter and column-mask, catalog isolation by domain, user group mapping, impersonation rules, audit logging via event listener, password file authentication for development
---

# Trino Security and Governance

## When to Use

- Hardening a Trino cluster before opening it to multiple teams
- Implementing column-level masking for PII data (SSN, email, phone)
- Adding row-level security (e.g., users see only their team's data)
- Setting up OAuth2 / LDAP authentication for enterprise SSO
- Auditing all query executions for compliance (SOC2/GDPR)

---

## Security Layers

```
1. Transport Security    → TLS between clients and coordinator
2. Internal Security     → Shared secret between coordinator and workers
3. Authentication        → Who are you? (LDAP/OAuth2/JWT)
4. Authorization         → What can you do? (File ACL / OPA / Ranger)
5. Audit Logging         → What did you do? (Event listener)
```

---

## 1. TLS Setup

```properties
# etc/config.properties — coordinator

# Enable HTTPS
http-server.https.enabled=true
http-server.https.port=8443
http-server.https.keystore.path=/etc/trino/certs/trino-keystore.jks
http-server.https.keystore.key=${ENV:KEYSTORE_PASSWORD}

# Redirect HTTP → HTTPS
http-server.http.enabled=false

# Internal node communication over HTTPS
internal-communication.https.required=true
internal-communication.https.keystore.path=/etc/trino/certs/internal-keystore.jks
internal-communication.https.keystore.key=${ENV:INTERNAL_KEYSTORE_PASSWORD}
internal-communication.shared-secret=${ENV:TRINO_SHARED_SECRET}
```

Generate a shared secret:

```bash
openssl rand -base64 32
# → use output as TRINO_SHARED_SECRET
```

---

## 2. Authentication

### LDAP

```properties
# etc/config.properties
http-server.authentication.type=PASSWORD

# etc/password-authenticator.properties
password-authenticator.name=ldap
ldap.url=ldaps://ldap.company.com:636
ldap.user-bind-pattern=uid=${USER},cn=users,dc=company,dc=com
ldap.group-auth-pattern=(&(objectClass=groupOfNames)(member=uid=${USER},cn=users,dc=company,dc=com))
ldap.cache-ttl=1m
```

### OAuth2 (Enterprise SSO)

```properties
# etc/config.properties
http-server.authentication.type=OAUTH2

# etc/oauth2-http-server-authentication.properties
oauth2-http-server-authentication.auth-url=https://auth.company.com/oauth2/authorize
oauth2-http-server-authentication.token-url=https://auth.company.com/oauth2/token
oauth2-http-server-authentication.client-id=${ENV:OAUTH2_CLIENT_ID}
oauth2-http-server-authentication.client-secret=${ENV:OAUTH2_CLIENT_SECRET}
oauth2-http-server-authentication.jwks-url=https://auth.company.com/oauth2/jwks
oauth2-http-server-authentication.scopes=openid,email,profile
oauth2-http-server-authentication.principal-field=email
```

### JWT (Service Accounts / CI)

```properties
# etc/config.properties
http-server.authentication.type=JWT

# etc/jwt-authenticator.properties
jwt-authenticator.name=jwt
jwt.key-file=/etc/trino/certs/jwt-public-key.pem
jwt.principal-field=sub
```

### Password File (Development)

```properties
# etc/config.properties
http-server.authentication.type=PASSWORD

# etc/password-authenticator.properties
password-authenticator.name=file
file.password-file=/etc/trino/password.db
```

```bash
# Create password file with bcrypt hashing
htpasswd -B -c /etc/trino/password.db admin
htpasswd -B     /etc/trino/password.db analyst1
```

---

## 3. File-Based Access Control

```properties
# etc/access-control.properties
access-control.name=file
security.config-file=etc/rules.json
security.refresh-period=30s    # hot-reload without restart
```

### Complete rules.json

```json
{
  "catalogs": [
    {
      "group":   "platform_admins",
      "allow":   "all"
    },
    {
      "group":   "data_engineers",
      "catalog": "iceberg",
      "allow":   "all"
    },
    {
      "group":   "data_engineers",
      "catalog": "postgresql",
      "allow":   "read-only"
    },
    {
      "group":   "analysts",
      "catalog": "iceberg",
      "allow":   "read-only"
    },
    {
      "group":   "analysts",
      "catalog": "tpch",
      "allow":   "read-only"
    },
    {
      "user":  ".*",
      "allow": "none"
    }
  ],

  "schemas": [
    {"group": "platform_admins",    "owner": true},
    {"group": "data_engineers",     "catalog": "iceberg", "schema": "bronze|silver", "owner": true},
    {"group": "data_engineers",     "catalog": "iceberg", "schema": "gold",           "owner": false}
  ],

  "tables": [
    {
      "group":      "platform_admins",
      "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP", "GRANT_SELECT"]
    },
    {
      "group":      "data_engineers",
      "catalog":    "iceberg",
      "schema":     "bronze|silver",
      "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE"]
    },
    {
      "group":      "data_engineers",
      "catalog":    "iceberg",
      "schema":     "gold",
      "privileges": ["SELECT"]
    },
    {
      "group":      "analysts",
      "catalog":    "iceberg",
      "schema":     "gold",
      "privileges": ["SELECT"],
      "columns": [
        {
          "name":  "email",
          "mask":  "regexp_replace(email, '(.)(.*)(@.*)', '$1***$3')"
        },
        {
          "name":  "ssn",
          "mask":  "'XXX-XX-' || substr(ssn, 8, 4)"
        },
        {
          "name":  "phone",
          "allow": false
        }
      ]
    },
    {
      "group":      "hr_team",
      "catalog":    "iceberg",
      "schema":     "gold",
      "table":      "employees",
      "privileges": ["SELECT"],
      "filter":     "department = 'HR' OR current_user = manager_id"
    }
  ],

  "queries": [
    {
      "group": "platform_admins",
      "allow": ["execute", "kill", "view"]
    },
    {
      "allow": ["execute"]
    }
  ],

  "system_information": [
    {
      "group": "platform_admins",
      "allow": ["read", "write"]
    },
    {
      "allow": ["read"]
    }
  ],

  "impersonation": [
    {
      "original_role": "platform_admins",
      "new_user":      ".*",
      "allow":         true
    }
  ]
}
```

---

## 4. User Group Mapping

Map external identities (LDAP groups, OAuth claims) to Trino groups for ACL matching:

```properties
# etc/group-provider.properties
group-provider.name=file
file.group-file=/etc/trino/groups.properties
file.refresh-period=60s
```

```properties
# /etc/trino/groups.properties  (group=user1,user2,user3)
platform_admins=alice,bob
data_engineers=charlie,diana,eve
analysts=frank,grace,henry
hr_team=irene,john
```

For LDAP groups:

```properties
group-provider.name=ldap
ldap.url=ldaps://ldap.company.com:636
ldap.user-bind-pattern=uid=${USER},cn=users,dc=company,dc=com
ldap.group-base-dn=cn=groups,dc=company,dc=com
ldap.group-search-filter=(member=uid=${USER},cn=users,dc=company,dc=com)
ldap.group-name-attribute=cn
```

---

## 5. Catalog-Level Security

Each catalog can also have per-catalog ACL files:

```properties
# etc/catalog/iceberg.properties
iceberg.security=FILE
security.config-file=etc/catalog/iceberg-rules.json
```

```json
// etc/catalog/iceberg-rules.json
{
  "schemas": [
    {"group": "data_engineers", "schema": "bronze|silver", "owner": true}
  ],
  "tables": [
    {
      "group":      "analysts",
      "schema":     "gold",
      "privileges": ["SELECT"],
      "columns": [
        {"name": "email", "mask": "regexp_replace(email, '(.)(.*)(@.*)', '$1***$3')"}
      ]
    }
  ]
}
```

---

## 6. Audit Logging (Event Listener)

```properties
# etc/event-listener.properties
event-listener.name=audit-log
audit.log.path=/var/trino/data/var/log/audit.log
audit.log.max-size=100MB
audit.log.max-history=30
```

Audit log entry format (implement via custom EventListener plugin):

```json
{
  "timestamp":    "2024-06-01T10:23:45Z",
  "query_id":     "20240601_102345_00001_xyz",
  "user":         "analyst@company.com",
  "source":       "superset",
  "catalog":      "iceberg",
  "schema":       "gold",
  "state":        "FINISHED",
  "query_type":   "SELECT",
  "wall_time_ms": 2340,
  "rows_read":    125000,
  "bytes_read":   5242880,
  "query_text":   "SELECT customer_id, SUM(amount) FROM iceberg.gold.fact_orders..."
}
```

---

## 7. OPA Integration (Advanced)

For dynamic, policy-as-code authorization:

```properties
# etc/access-control.properties
access-control.name=opa
opa.policy.uri=http://opa-server:8181/v1/data/trino/allow
opa.policy.batched-uri=http://opa-server:8181/v1/data/trino/batch
```

OPA Rego policy example:

```rego
package trino

default allow = false

# Platform admins can do anything
allow {
    input.context.identity.user == data.platform_admins[_]
}

# Analysts can only SELECT from gold schema
allow {
    input.action.operation == "SelectFromColumns"
    input.action.resource.table.schemaTable.schema == "gold"
    input.context.groups[_] == "analysts"
}

# Block access to PII columns
allow = false {
    input.action.operation == "SelectFromColumns"
    input.action.resource.column.name == "ssn"
    not input.context.groups[_] == "platform_admins"
}
```

---

## Anti-Patterns

1. **`http-server.authentication.type=PASSWORD` with `password-authenticator.name=file` in production** — file-based passwords are not integrated with your IdP; use LDAP or OAuth2 for production teams.
2. **Internal communication without shared secret** — without `internal-communication.shared-secret`, any process that can reach worker ports can submit arbitrary tasks; always set the shared secret.
3. **Storing credentials in catalog `.properties` files in plain text** — catalog files are often checked into Git; use `${ENV:VAR_NAME}` substitution or HashiCorp Vault.
4. **`allow: all` catch-all at the end of catalogs array** — any rule after the first match is irrelevant; a `.*` catch-all should use `allow: none` to deny unmatched access, not `all`.
5. **No column masking on PII in analyst-facing schemas** — analysts querying `gold` tables containing email/SSN/phone without masking is a GDPR/compliance violation; implement masks before opening access.

---

## References

- Security overview: `trino.io/docs/current/security/overview.html`
- File-based ACL: `trino.io/docs/current/security/file-system-access-control.html`
- LDAP authentication: `trino.io/docs/current/security/ldap.html`
- OAuth2: `trino.io/docs/current/security/oauth2.html`
- Related skills: `[[trino-resource-group-governance]]`, `[[trino-production-readiness-review]]`, `[[trino-admin-cluster-health]]`
