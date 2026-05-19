---
name: trino-production-readiness-review
description: Trino production readiness checklist and review — coordinator HA (active/passive with load balancer), worker autoscaling, TLS/HTTPS configuration, authentication setup, resource groups for multi-tenancy, JVM sizing, spill configuration, fault-tolerant execution for batch workloads, monitoring (Prometheus/alerting), graceful shutdown, backup strategy for Hive Metastore, catalog security, query history retention, log rotation, Kubernetes deployment checklist
---

# Trino Production Readiness Review

## When to Use

- Preparing a Trino cluster for its first production deployment
- Reviewing an existing cluster before a major traffic increase
- Conducting a pre-launch readiness checklist for a new data platform
- Hardening a development cluster for production use

---

## Production Readiness Checklist

### 1. Infrastructure & HA

```
[ ] Coordinator is on dedicated node (node-scheduler.include-coordinator=false)
[ ] Workers on separate nodes from coordinator
[ ] Load balancer in front of coordinator (HAProxy/ALB/nginx)
[ ] Coordinator health check: GET /v1/info/state → "ACTIVE"
[ ] Worker autoscaling configured (K8s HPA or cloud autoscaler)
[ ] OS ulimits: nofile=131072, nproc=128000 on all nodes
[ ] Swap disabled: swapoff -a (in /etc/fstab)
[ ] Fast local SSDs mounted at /data/trino-spill for spill-to-disk
```

### 2. JVM & Memory

```
[ ] -Xmx set to 80-85% of node RAM (not default)
[ ] -XX:+ExitOnOutOfMemoryError set (prevents zombie JVM)
[ ] -XX:+HeapDumpOnOutOfMemoryError with accessible heap dump path
[ ] query.max-memory set (not default 20GB)
[ ] query.max-total-memory = 2× query.max-memory
[ ] memory.heap-headroom-per-node set (not default)
[ ] Constraint: query.max-memory-per-node + heap-headroom < Xmx
```

### 3. Security

```
[ ] TLS enabled (http-server.https.enabled=true or load balancer terminates TLS)
[ ] Authentication configured (LDAP/OAuth2/JWT — not "none")
[ ] Shared secret for internal node communication (internal-communication.shared-secret)
[ ] Access control configured (file-based or OPA or Ranger)
[ ] Catalogs use read-only DB credentials for JDBC connectors
[ ] No plaintext credentials in catalog property files (use environment vars)
```

### 4. Query Governance

```
[ ] Resource groups configured (at minimum: interactive vs batch isolation)
[ ] query.max-execution-time set (e.g. 8h for batch, 30m for interactive)
[ ] query.max-stage-count set (default 150 is usually fine)
[ ] query.max-memory set per resource group softMemoryLimit
```

### 5. Observability

```
[ ] JMX Prometheus exporter running (port 9090)
[ ] Prometheus scraping coordinator and workers
[ ] Alerts configured for: worker loss, OOM kills, queue depth, P99 latency
[ ] Log rotation configured (var/log/ doesn't fill disk)
[ ] Query history retained: query.max-history=1000, query.min-expire-age=30m
[ ] Slow query event listener deployed
```

### 6. Iceberg Maintenance

```
[ ] Daily OPTIMIZE job scheduled (file compaction)
[ ] Daily EXPIRE_SNAPSHOTS job scheduled (snapshot cleanup)
[ ] Daily REMOVE_ORPHAN_FILES job scheduled (orphan cleanup)
[ ] ANALYZE scheduled after bulk loads (CBO statistics)
```

---

## Coordinator HA with Load Balancer

Trino coordinator is a single point of failure. For HA, run an active/standby pair behind a load balancer (only one should be active at a time in most deployments).

```nginx
# nginx upstream for Trino coordinator
upstream trino_cluster {
    least_conn;
    server trino-coordinator-1:8080 max_fails=2 fail_timeout=30s;
    # server trino-coordinator-2:8080 backup;  # standby for HA
    keepalive 32;
}

server {
    listen 80;
    listen 443 ssl;

    ssl_certificate     /etc/ssl/trino.crt;
    ssl_certificate_key /etc/ssl/trino.key;

    location / {
        proxy_pass         http://trino_cluster;
        proxy_http_version 1.1;
        proxy_set_header   Connection "";
        proxy_set_header   Host $host;
        proxy_read_timeout 3600s;   # allow long-running queries
        proxy_send_timeout 3600s;
    }
}
```

---

## Complete Production config.properties

```properties
# etc/config.properties — coordinator

# Basic
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080

# TLS
http-server.https.enabled=true
http-server.https.port=8443
http-server.https.keystore.path=/etc/trino/certs/keystore.jks
http-server.https.keystore.key=${ENV:KEYSTORE_PASSWORD}

# Discovery
discovery.uri=http://trino-coordinator:8080

# Internal communication secret
internal-communication.https.required=true
internal-communication.shared-secret=${ENV:TRINO_SHARED_SECRET}

# Memory
query.max-memory=200GB
query.max-total-memory=400GB
query.max-memory-per-node=32GB
memory.heap-headroom-per-node=8GB

# Query management
query.max-execution-time=12h
query.max-run-time=24h
query.max-history=2000
query.min-expire-age=1h
query.max-stage-count=200

# Spill
spill-enabled=true
spiller-spill-path=/data/trino-spill
spill-compression-codec=LZ4
max-spill-per-node=200GB
query-max-spill-per-node=50GB

# Resource groups
resource-groups.config-file=etc/resource-groups.json

# Logging
log.annotation-file=etc/log.properties
```

---

## Kubernetes Deployment (Helm)

```yaml
# values.yaml for trinodb/trino Helm chart

image:
  tag: "481"

coordinator:
  jvm:
    maxHeapSize: "54G"
  config:
    query:
      maxMemory: "200GB"
  resources:
    requests:
      memory: "60Gi"
      cpu: "8"
    limits:
      memory: "64Gi"
      cpu: "16"
  nodeSelector:
    node-role: trino-coordinator
  tolerations:
    - key: trino-coordinator
      operator: Exists

worker:
  replicas: 5
  jvm:
    maxHeapSize: "54G"
  config:
    query:
      maxMemoryPerNode: "32GB"
  resources:
    requests:
      memory: "60Gi"
      cpu: "8"
    limits:
      memory: "64Gi"
      cpu: "16"
  nodeSelector:
    node-role: trino-worker
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 20
    targetCPUUtilizationPercentage: 60

additionalCatalogs:
  iceberg: |
    connector.name=iceberg
    iceberg.catalog.type=glue
    hive.metastore.glue.region=us-east-1
    iceberg.file-format=PARQUET
    iceberg.compression-codec=ZSTD

serviceMonitor:
  enabled: true    # Prometheus ServiceMonitor for scraping
```

---

## Graceful Upgrade Procedure

```bash
# Rolling worker upgrade:
for WORKER in worker-1 worker-2 worker-3; do
  echo "Gracefully shutting down $WORKER..."

  # 1. Signal graceful shutdown
  curl -X PUT -H "Content-Type: application/json" \
    "http://$WORKER:8080/v1/info/state" -d '"SHUTTING_DOWN"'

  # 2. Wait until all tasks drain (check every 10s, max 10 min)
  timeout 600 bash -c "
    while [ \"\$(curl -s http://$WORKER:8080/v1/info/state)\" != '\"INACTIVE\"' ]; do
      sleep 10
    done
  "

  # 3. Stop and upgrade
  ssh $WORKER "sudo systemctl stop trino && sudo apt install trino-server=481 && sudo systemctl start trino"

  # 4. Wait for worker to re-register
  sleep 30
  echo "$WORKER upgraded."
done
```

---

## Hive Metastore Backup

```bash
# Backup HMS PostgreSQL database (run daily via cron)
pg_dump \
  -h hive-metastore-db \
  -U hive \
  -d metastore \
  -F custom \
  -f /backup/hms-$(date +%Y%m%d).dump

# Restore
pg_restore \
  -h hive-metastore-db \
  -U hive \
  -d metastore_restore \
  /backup/hms-20240601.dump
```

---

## Pre-Launch Verification Script

```bash
#!/bin/bash
set -e

COORDINATOR="http://trino-coordinator:8080"
ERRORS=0

check() {
  local desc="$1"
  local result="$2"
  local expected="$3"
  if [ "$result" = "$expected" ]; then
    echo "✓ $desc"
  else
    echo "✗ $desc (got: $result, expected: $expected)"
    ERRORS=$((ERRORS+1))
  fi
}

# Check coordinator state
STATE=$(curl -s "$COORDINATOR/v1/info/state" | tr -d '"')
check "Coordinator state ACTIVE" "$STATE" "ACTIVE"

# Check worker count
WORKERS=$(curl -s "$COORDINATOR/v1/node" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
check "Worker count >= 3" "$([ $WORKERS -ge 3 ] && echo 'ok')" "ok"

# Check failed nodes
FAILED=$(curl -s "$COORDINATOR/v1/node/failed" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
check "No failed nodes" "$FAILED" "0"

# Check query execution
RESULT=$(curl -s -H "X-Trino-User: admin" \
  -H "X-Trino-Catalog: tpch" \
  -H "X-Trino-Schema: sf1" \
  "$COORDINATOR/v1/statement" \
  -d "SELECT 1 AS health_check" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('state','UNKNOWN'))")
check "Query submission works" "$([ '$RESULT' != 'FAILED' ] && echo 'ok')" "ok"

echo ""
if [ $ERRORS -eq 0 ]; then
  echo "✓ All checks passed — cluster is production-ready"
else
  echo "✗ $ERRORS check(s) failed — resolve before going to production"
  exit 1
fi
```

---

## Anti-Patterns

1. **Single coordinator without load balancer** — coordinator restart causes all in-flight queries to fail; a load balancer enables health-based failover even without true HA.
2. **Default `query.max-memory=20GB`** — this is too low for any real cluster; calculate from actual worker count and heap size.
3. **No graceful shutdown in rolling upgrades** — hard-killing workers mid-query causes `FAILED` queries for users; always use the graceful shutdown API.
4. **HMS database without backups** — HMS PostgreSQL is the source of truth for all Iceberg table metadata; without daily backups, a corruption event is catastrophic.
5. **Spill on NFS or network storage** — Trino spill writes GB/s; network-attached storage creates I/O bottlenecks; always use local fast SSDs for spill paths.

---

## References

- Trino deployment: `trino.io/docs/current/installation/deployment.html`
- Trino security: `trino.io/docs/current/security/overview.html`
- Fault-tolerant execution: `trino.io/docs/current/admin/fault-tolerant-execution.html`
- Related skills: `[[trino-admin-cluster-health]]`, `[[trino-memory-and-spill-tuning]]`, `[[trino-security-and-governance]]`, `[[trino-observability-platform]]`
