---
name: starrocks-admin-security
description: StarRocks security and RBAC — CREATE USER/ROLE, GRANT/REVOKE privileges (catalog/database/table/view/MV/function/resource group level), built-in roles (cluster_admin/db_admin/user_admin/public), LDAP/LDAP group auth, row-level security policies, column masking, SSL/TLS, audit log, privilege inheritance, security best practices
---

# StarRocks Security and RBAC

## When to Use

Use this skill when:
- Setting up a multi-tenant StarRocks cluster with isolated access per team or business unit
- Implementing compliance controls for PII, GDPR, HIPAA, or SOC 2 — including column masking and row-level filtering
- Granting BI tool users (Superset, Tableau, Grafana) read-only access with the principle of least privilege
- Integrating with an enterprise LDAP/Active Directory for centralized identity management
- Auditing who has access to what, reviewing privilege inheritance, or rotating credentials
- Hardening a new cluster before production go-live

---

## Privilege System Overview

StarRocks 3.x uses a unified RBAC model. All access decisions go through:

```
Identity (User / Role)
  └─ Role membership (zero or more roles, including inherited roles)
       └─ Privilege grants on objects
            └─ Object hierarchy: SYSTEM → CATALOG → DATABASE → TABLE / VIEW / MV / FUNCTION
```

Key rules:
- A user gets the union of all privileges from all directly and transitively granted roles.
- The special `public` role is granted to every user automatically at login.
- Privileges cannot be partially inherited — a role either has a privilege or it does not.
- `GRANT OPTION` allows the grantee to re-grant the same privilege to others.
- Privilege checks happen at query parse time; changing a role takes effect on the next query.

---

## User Management

### Create a user with a password

```sql
-- Plain password (hashed internally with mysql_native_password)
CREATE USER 'alice'@'%' IDENTIFIED BY 'StrongPass123!';

-- Explicit plugin
CREATE USER 'alice'@'10.0.0.%' IDENTIFIED WITH mysql_native_password BY 'StrongPass123!';

-- LDAP simple-bind user (password validated by the LDAP server at login)
CREATE USER 'bob'@'%' IDENTIFIED WITH authentication_ldap_simple AS 'uid=bob,ou=people,dc=example,dc=com';

-- No password (service account that connects only from localhost via trust)
CREATE USER 'etl_svc'@'127.0.0.1';
```

Host pattern rules:
- `'%'` — any host
- `'10.0.0.%'` — CIDR-style prefix match
- `'127.0.0.1'` — exact IP
- IPv6 literals are supported

### Alter user

```sql
-- Change password
ALTER USER 'alice'@'%' IDENTIFIED BY 'NewPass456!';

-- Switch authentication plugin
ALTER USER 'alice'@'%' IDENTIFIED WITH authentication_ldap_simple
    AS 'uid=alice,ou=people,dc=corp,dc=example,dc=com';

-- Set default roles that activate automatically on login
ALTER USER 'alice'@'%' DEFAULT ROLE 'analyst', 'report_viewer';

-- Disable / re-enable a user (StarRocks 3.1+)
ALTER USER 'alice'@'%' ACCOUNT_LOCK;
ALTER USER 'alice'@'%' ACCOUNT_UNLOCK;
```

### Drop user

```sql
DROP USER 'alice'@'%';
```

### Password self-service

```sql
-- Current session user changes own password
SET PASSWORD = PASSWORD('NewPass789!');

-- Admin changes another user's password
SET PASSWORD FOR 'alice'@'%' = PASSWORD('AdminReset!');
```

### Inspect users

```sql
SHOW USERS;

-- Show the privileges granted directly to a user
SHOW GRANTS FOR 'alice'@'%';

-- Show effective privileges (including all roles)
SHOW GRANTS FOR 'alice'@'%' WITH ROLES;
```

---

## Role Management

### Create and drop roles

```sql
CREATE ROLE analyst;
CREATE ROLE report_viewer;
CREATE ROLE etl_writer;
DROP ROLE analyst;
```

### Grant and revoke a role to/from a user

```sql
GRANT ROLE analyst TO USER 'alice'@'%';
GRANT ROLE report_viewer, analyst TO USER 'alice'@'%';
REVOKE ROLE analyst FROM USER 'alice'@'%';
```

### Role inheritance — grant a role to another role

```sql
-- report_viewer inherits everything analyst has
GRANT ROLE analyst TO ROLE report_viewer;

-- senior_analyst inherits analyst, which in turn inherits junior_analyst
CREATE ROLE junior_analyst;
GRANT ROLE junior_analyst TO ROLE analyst;
GRANT ROLE analyst TO ROLE senior_analyst;
```

### Show roles

```sql
SHOW ROLES;

-- Grants on a role
SHOW GRANTS FOR ROLE analyst;
```

### Built-in roles

| Role | Scope | Key capabilities |
|------|-------|-----------------|
| `root` | System | Superuser; owns the cluster. Cannot be dropped or revoked from the root user. |
| `cluster_admin` | System | NODE management, resource group administration, storage volume admin. Not a DBA role. |
| `db_admin` | System | All DDL/DML across all databases; cannot manage users/roles/privileges. |
| `user_admin` | System | CREATE/ALTER/DROP USER, CREATE/DROP ROLE, GRANT/REVOKE (only up to own privilege level). |
| `public` | System | Implicitly granted to every user. By default holds no privileges; grant safe read-only objects here with care. |

```sql
-- Give a DBA full database control without cluster-level risk
GRANT ROLE db_admin TO USER 'dba_user'@'%';

-- Give an IAM admin user/role management only
GRANT ROLE user_admin TO USER 'iam_admin'@'%';

-- Grant a global read permission to all users via public
GRANT SELECT ON TABLE default_catalog.shared_db.dim_date TO ROLE public;
```

---

## Privilege Reference

### GRANT syntax

```sql
GRANT <privilege_list>
    ON <object_type> <object_name>
    TO { USER 'user'@'host' | ROLE role_name }
    [WITH GRANT OPTION];

REVOKE <privilege_list>
    ON <object_type> <object_name>
    FROM { USER 'user'@'host' | ROLE role_name };
```

### System-level privileges

```sql
-- Allows adding/removing FE/BE/CN nodes
GRANT NODE ON SYSTEM TO ROLE cluster_admin;

-- Allows granting any privilege the grantee holds (with GRANT OPTION)
GRANT GRANT ON SYSTEM TO USER 'iam_admin'@'%';

-- Allows creating resource groups
GRANT CREATE RESOURCE GROUP ON SYSTEM TO ROLE cluster_admin;

-- Allows creating external catalogs
GRANT CREATE EXTERNAL CATALOG ON SYSTEM TO ROLE catalog_admin;

-- Allows creating storage volumes (shared-nothing / S3 / HDFS)
GRANT CREATE STORAGE VOLUME ON SYSTEM TO ROLE cluster_admin;
```

### Catalog-level privileges

```sql
-- USAGE on a catalog lets users see databases inside it
GRANT USAGE ON CATALOG hive_prod TO ROLE analyst;

-- Allow creating databases inside the default catalog
GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE db_admin;

-- Drop a catalog (dangerous — use sparingly)
GRANT DROP ON CATALOG hive_prod TO ROLE catalog_admin;
```

### Database-level privileges

```sql
-- Allow creating tables in a specific database
GRANT CREATE TABLE ON DATABASE sales_db TO ROLE etl_writer;

-- Allow creating views
GRANT CREATE VIEW ON DATABASE sales_db TO ROLE etl_writer;

-- Allow creating materialized views
GRANT CREATE MATERIALIZED VIEW ON DATABASE sales_db TO ROLE etl_writer;

-- Allow creating functions
GRANT CREATE FUNCTION ON DATABASE sales_db TO ROLE developer;

-- Bulk-grant to all databases in a catalog
GRANT CREATE TABLE ON ALL DATABASES IN CATALOG default_catalog TO ROLE db_admin;
```

### Table-level privileges

```sql
-- Read-only BI user
GRANT SELECT ON TABLE sales_db.orders TO ROLE analyst;

-- ETL writer
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE sales_db.orders TO ROLE etl_writer;

-- Full table control (DDL)
GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, DROP, EXPORT
    ON TABLE sales_db.orders
    TO ROLE table_owner;

-- Bulk-grant SELECT on all current tables in a database
GRANT SELECT ON ALL TABLES IN DATABASE sales_db TO ROLE analyst;

-- Bulk-grant across all databases in the catalog
GRANT SELECT ON ALL TABLES IN ALL DATABASES IN CATALOG default_catalog TO ROLE reporting;
```

Note: `GRANT ... ON ALL TABLES` is evaluated at grant time. Tables created later require a fresh grant or a default privilege policy (see below).

### View and Materialized View privileges

```sql
-- View is a first-class object; SELECT on the view does not require SELECT on base tables
GRANT SELECT ON VIEW sales_db.v_orders_summary TO ROLE analyst;

-- Materialized View
GRANT SELECT ON MATERIALIZED VIEW sales_db.mv_daily_revenue TO ROLE analyst;
GRANT ALTER, REFRESH ON MATERIALIZED VIEW sales_db.mv_daily_revenue TO ROLE etl_writer;
```

### Function privileges

```sql
GRANT USAGE ON FUNCTION sales_db.udf_mask_email TO ROLE analyst;
```

### Resource group privileges

```sql
-- Allow a user's queries to be submitted to a specific resource group
GRANT USAGE ON RESOURCE GROUP etl_rg TO ROLE etl_writer;
```

### Default privileges — future objects

```sql
-- Any table created in sales_db by any user will automatically grant SELECT to analyst
ALTER DEFAULT PRIVILEGES IN DATABASE sales_db
    GRANT SELECT ON TABLES TO ROLE analyst;

-- Revoke future default
ALTER DEFAULT PRIVILEGES IN DATABASE sales_db
    REVOKE SELECT ON TABLES FROM ROLE analyst;
```

---

## LDAP Integration

### FE configuration (`fe.conf`)

```properties
# Enable LDAP simple-bind authentication
authentication_ldap_simple_server_host = ldap.corp.example.com
authentication_ldap_simple_server_port = 389
authentication_ldap_simple_bind_base_dn = ou=people,dc=corp,dc=example,dc=com
authentication_ldap_simple_bind_root_dn = cn=starrocks_svc,ou=svc,dc=corp,dc=example,dc=com
authentication_ldap_simple_bind_root_pwd = LdapServicePassword!
authentication_ldap_simple_user_search_attr = uid

# For LDAPS (port 636)
authentication_ldap_simple_server_port = 636
authentication_ldap_simple_ssl_conn = true
```

Reload without restart:

```sql
ADMIN SET FRONTEND CONFIG ('authentication_ldap_simple_server_host' = 'ldap.corp.example.com');
```

### Create LDAP-authenticated users

```sql
-- The AS clause is the user's full DN in the LDAP directory
CREATE USER 'alice'@'%'
    IDENTIFIED WITH authentication_ldap_simple
    AS 'uid=alice,ou=people,dc=corp,dc=example,dc=com';
```

### LDAP group mapping pattern

StarRocks does not natively auto-map LDAP groups to roles. Use a provisioning script or Airflow DAG that:

1. Queries LDAP group membership via `ldapsearch`.
2. Issues `GRANT ROLE <role> TO USER '...'@'%'` for new members.
3. Issues `REVOKE ROLE <role> FROM USER '...'@'%'` for removed members.

```bash
# Example: sync members of ldap group "bi_analysts" to StarRocks role "analyst"
ldapsearch -x -H ldap://ldap.corp.example.com \
  -D "cn=starrocks_svc,ou=svc,dc=corp,dc=example,dc=com" \
  -w "$LDAP_SVC_PASS" \
  -b "ou=groups,dc=corp,dc=example,dc=com" \
  "(cn=bi_analysts)" member \
  | grep "^member:" \
  | awk '{print $2}' \
  | while read dn; do
      uid=$(echo "$dn" | grep -oP 'uid=\K[^,]+')
      mysql -h sr-fe -u iam_admin -p"$IAM_PASS" \
        -e "GRANT ROLE analyst TO USER '${uid}'@'%';"
    done
```

---

## Row-Level Security

StarRocks 3.2+ supports **row access policies** that filter which rows a user/role can see.

### Create a row access policy

```sql
-- Only return rows where tenant_id matches the session variable set at login
CREATE ROW ACCESS POLICY rls_orders_by_tenant
    ON sales_db.orders
    USING (tenant_id = CURRENT_USER_ATTRIBUTE('tenant_id'));
```

`CURRENT_USER_ATTRIBUTE` reads a user attribute set via `ALTER USER ... SET PROPERTIES`.

### Set a user attribute for row-level filtering

```sql
ALTER USER 'alice'@'%' SET PROPERTIES ('tenant_id' = 'acme_corp');
ALTER USER 'bob'@'%'   SET PROPERTIES ('tenant_id' = 'globex');
```

### Attach / detach a row access policy

```sql
-- Attach
ALTER TABLE sales_db.orders
    SET ROW ACCESS POLICY rls_orders_by_tenant;

-- Detach
ALTER TABLE sales_db.orders
    UNSET ROW ACCESS POLICY rls_orders_by_tenant;
```

### Show row access policies

```sql
SHOW ROW ACCESS POLICIES;
SHOW ROW ACCESS POLICIES ON TABLE sales_db.orders;
```

### Drop a policy

```sql
DROP ROW ACCESS POLICY rls_orders_by_tenant ON sales_db.orders;
```

---

## Data Masking

Column masking replaces sensitive column values with masked output for users who do not hold the `UNMASK` privilege.

### Create a masking policy

```sql
-- Full redaction for non-privileged users
CREATE MASKING POLICY mask_email
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('pii_admin', 'root') THEN val
        ELSE REGEXP_REPLACE(val, '(^[^@]{2})[^@]+@', '\\1***@')
    END;

-- Mask all but last 4 digits of a credit card
CREATE MASKING POLICY mask_cc_number
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('payments_admin') THEN val
        ELSE CONCAT('****-****-****-', RIGHT(val, 4))
    END;

-- Null out a column entirely for unauthorized users
CREATE MASKING POLICY mask_null
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('pii_admin') THEN val
        ELSE NULL
    END;
```

### Apply a masking policy to a column

```sql
ALTER TABLE sales_db.customers
    MODIFY COLUMN email
    SET MASKING POLICY mask_email;

ALTER TABLE sales_db.payments
    MODIFY COLUMN card_number
    SET MASKING POLICY mask_cc_number;
```

### Remove a masking policy from a column

```sql
ALTER TABLE sales_db.customers
    MODIFY COLUMN email
    UNSET MASKING POLICY;
```

### Inspect masking policies

```sql
SHOW MASKING POLICIES;
SHOW MASKING POLICIES ON TABLE sales_db.customers;
```

### Drop a masking policy

```sql
DROP MASKING POLICY mask_email;
```

---

## Audit Log

The FE audit log records every SQL statement, user, host, execution time, and result.

### Location and format

```
$STARROCKS_FE_HOME/log/fe.audit.log
```

Log rotation: daily, kept 30 days by default.

Sample log entry (pipe-delimited):

```
2026-05-17 09:12:34,211|127.0.0.1|alice|sales_db|SELECT * FROM orders WHERE ...|1|200ms|OK
```

Fields: `timestamp | client_host | user | database | sql | result_rows | query_time | state`

### Configuration in `fe.conf`

```properties
# Comma-separated modules to audit: slow_query, query, load, stream_load
audit_log_modules = slow_query,query

# Log queries slower than this threshold (ms); 0 = log all
qe_slow_log_ms = 5000

# Audit log directory (default: $STARROCKS_FE_HOME/log)
audit_log_dir = /data/starrocks/log

# Max audit log file size before rotation (bytes)
audit_log_roll_num = 90
```

### Parse audit log with shell

```bash
# Top 10 slowest queries in the last hour
grep "$(date +'%Y-%m-%d %H')" /data/starrocks/log/fe.audit.log \
  | awk -F'|' '{print $7, $4, $5}' \
  | sort -rn \
  | head 10

# Failed queries
grep "ERR\|FAILED" /data/starrocks/log/fe.audit.log | tail -50

# All queries by a specific user
grep "|alice|" /data/starrocks/log/fe.audit.log | tail -100
```

### Audit log plugin (StarRocks Enterprise)

Enterprise Edition ships an `AuditLoader` plugin that writes audit events to an internal StarRocks table for SQL querying:

```sql
-- After AuditLoader plugin is installed and configured
SELECT
    query_time,
    user,
    db,
    LEFT(stmt, 200)  AS sql_snippet,
    scan_rows,
    return_rows,
    state
FROM starrocks_audit_db__.starrocks_slow_log_tbl__
WHERE query_time > NOW() - INTERVAL 1 HOUR
ORDER BY query_time DESC
LIMIT 50;
```

---

## SSL / TLS

### FE TLS configuration (`fe.conf`)

```properties
# Enable TLS for MySQL protocol port (default 9030)
enable_ssl = true
ssl_certificate_file = /etc/starrocks/certs/fe.crt
ssl_private_key_file  = /etc/starrocks/certs/fe.key

# Optionally require client-side certificates
ssl_require_client_auth = false
ssl_ca_certificate_file = /etc/starrocks/certs/ca.crt
```

### BE TLS configuration (`be.conf`)

```properties
# Internal BE HTTP service
be_http_enable_ssl = true
be_https_port = 8443
ssl_certificate_file = /etc/starrocks/certs/be.crt
ssl_private_key_file  = /etc/starrocks/certs/be.key
```

### Connect with SSL from MySQL client

```bash
mysql -h sr-fe-host -P 9030 -u alice \
    --ssl-mode=REQUIRED \
    --ssl-ca=/etc/starrocks/certs/ca.crt \
    -p
```

### Verify SSL status from SQL

```sql
SHOW STATUS LIKE 'Ssl_cipher';
```

An empty result means the connection is not encrypted. A cipher name (e.g. `TLS_AES_256_GCM_SHA384`) confirms TLS is active.

---

## Multi-tenant Pattern — Database-per-Team Isolation

```sql
-- 1. Create isolated databases per team
CREATE DATABASE team_marketing;
CREATE DATABASE team_finance;

-- 2. Create team roles
CREATE ROLE mkt_writer;
CREATE ROLE mkt_reader;
CREATE ROLE fin_writer;
CREATE ROLE fin_reader;

-- 3. Grant DDL/DML to writer roles
GRANT CREATE TABLE, CREATE VIEW, CREATE MATERIALIZED VIEW
    ON DATABASE team_marketing TO ROLE mkt_writer;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN DATABASE team_marketing TO ROLE mkt_writer;

GRANT CREATE TABLE, CREATE VIEW, CREATE MATERIALIZED VIEW
    ON DATABASE team_finance TO ROLE fin_writer;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN DATABASE team_finance TO ROLE fin_writer;

-- 4. Grant read-only access to reader roles
GRANT SELECT ON ALL TABLES IN DATABASE team_marketing TO ROLE mkt_reader;
GRANT SELECT ON ALL TABLES IN DATABASE team_finance   TO ROLE fin_reader;

-- 5. Ensure reader roles cannot see the other team's data
--    (no cross-database grants)

-- 6. Assign resource groups per team (cap query resource usage)
CREATE RESOURCE GROUP mkt_rg
    TO (role='mkt_writer'), (role='mkt_reader')
    WITH (cpu_core_limit = 4, mem_limit = '16g', concurrency_limit = 20);

CREATE RESOURCE GROUP fin_rg
    TO (role='fin_writer'), (role='fin_reader')
    WITH (cpu_core_limit = 8, mem_limit = '32g', concurrency_limit = 10);

-- 7. Assign users to team roles
GRANT ROLE mkt_writer TO USER 'mkt_etl'@'%';
GRANT ROLE mkt_reader TO USER 'mkt_bi'@'%';
GRANT ROLE fin_writer TO USER 'fin_etl'@'%';
GRANT ROLE fin_reader TO USER 'fin_bi'@'%';

-- 8. Default privileges so future tables are auto-readable
ALTER DEFAULT PRIVILEGES IN DATABASE team_marketing
    GRANT SELECT ON TABLES TO ROLE mkt_reader;
ALTER DEFAULT PRIVILEGES IN DATABASE team_finance
    GRANT SELECT ON TABLES TO ROLE fin_reader;
```

---

## Security Hardening Checklist

1. **Change the root password immediately** after cluster initialization:
   ```sql
   SET PASSWORD FOR 'root'@'%' = PASSWORD('VeryStrongRootPass!');
   ```

2. **Disable or lock unused built-in accounts** (e.g., anonymous user):
   ```sql
   ALTER USER ''@'%' ACCOUNT_LOCK;
   DROP USER ''@'%';  -- if anonymous user exists
   ```

3. **Restrict root to localhost only** — create a named admin instead of using root remotely:
   ```sql
   -- Create a named superuser
   CREATE USER 'sr_admin'@'10.0.0.%' IDENTIFIED BY 'AdminPass!';
   GRANT ROLE cluster_admin, db_admin, user_admin TO USER 'sr_admin'@'10.0.0.%';
   -- Then remove root's open host binding
   -- (root@'%' cannot be dropped, but can be locked and password-rotated regularly)
   ```

4. **Apply the principle of least privilege** — never use `db_admin` for BI read-only connections.

5. **Rotate service account passwords** on a schedule (90 days); use secrets management (Vault, AWS Secrets Manager):
   ```bash
   # Rotate via script
   NEW_PASS=$(vault kv get -field=password secret/starrocks/etl_svc)
   mysql -h sr-fe -u sr_admin -p"$ADMIN_PASS" \
     -e "SET PASSWORD FOR 'etl_svc'@'127.0.0.1' = PASSWORD('${NEW_PASS}');"
   ```

6. **Enable and retain audit logs** for at least 90 days (compliance minimum). Ship to a SIEM (Splunk, Elasticsearch):
   ```bash
   # Tail audit log into Filebeat / Fluentd
   tail -F /data/starrocks/log/fe.audit.log | filebeat -e -c /etc/filebeat/starrocks.yml
   ```

7. **Enable SSL/TLS** for all client connections and internal FE-BE communication in production.

8. **Use resource groups** to prevent one tenant from monopolizing cluster resources.

9. **Audit privilege grants regularly**:
   ```sql
   -- List all non-public role members
   SELECT grantee, role_name, is_grantable
   FROM information_schema.applicable_roles
   WHERE role_name NOT IN ('public')
   ORDER BY role_name, grantee;

   -- Find users with system-level privileges
   SHOW GRANTS FOR 'alice'@'%';
   ```

10. **Never store passwords in DAG code or ETL scripts** — use environment variables or a secrets manager; inject at runtime.

---

## Anti-Patterns

**Granting db_admin to application users**
`db_admin` allows DROP TABLE, TRUNCATE, and ALTER across all databases. Application/BI users need only `SELECT`; ETL users need `SELECT, INSERT, UPDATE, DELETE` on their own tables. Never use `db_admin` for non-DBA accounts.

**Using root for ETL pipelines**
Root has unrestricted access and its activity is harder to audit by team. Create dedicated service accounts per pipeline with scoped privileges.

**`GRANT SELECT ON ALL TABLES` without default privileges**
This grants access to tables existing at grant time only. New tables created later are invisible to the role. Always pair with `ALTER DEFAULT PRIVILEGES` to cover future objects.

**Hardcoding credentials in DAG/ETL code**
Credentials checked into git or passed as plaintext arguments are a critical security breach. Use environment variables, Vault, or Kubernetes secrets; reference only the reference name in code.

**Skipping LDAP TLS (LDAPS)**
Plain `ldap://` port 389 transmits bind passwords in clear text. Always use `ldaps://` port 636 or STARTTLS in production LDAP configuration.

**Over-broad host wildcards for privileged users**
`CREATE USER 'dba'@'%'` allows connection from any IP. Restrict to the subnet of your ETL hosts: `'dba'@'10.20.30.%'`.

**Sharing a single read account across BI tools and ad-hoc users**
Shared accounts make audit logs unattributable and complicate privilege revocation. Issue per-user or per-tool accounts even when they share the same role.

**Ignoring masking policy on views**
Row-level and column masking policies apply to the base table, not automatically to views on top. Verify that views do not expose unmasked data to unauthorized roles by testing with a non-privileged user session.

---

## References to Consult When Needed

- [StarRocks Privilege Overview](https://docs.starrocks.io/docs/administration/user_privs/privilege_overview/)
- [StarRocks RBAC Guide](https://docs.starrocks.io/docs/administration/user_privs/role_based_access_control/)
- [User Privileges Overview](https://docs.starrocks.io/docs/administration/user_privs/)
- [StarRocks CREATE USER syntax](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/CREATE_USER/)
- [StarRocks GRANT syntax](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/GRANT/)
- [StarRocks Row Access Policy](https://docs.starrocks.io/docs/administration/user_privs/row_policy/)
- [StarRocks Data Masking](https://docs.starrocks.io/docs/administration/user_privs/column_masking/)
- [StarRocks Audit Log](https://docs.starrocks.io/docs/administration/audit_loader/)
- [StarRocks SSL/TLS Config](https://docs.starrocks.io/docs/administration/ssl/)
