---
name: trino-admin-cluster-health
description: Trino cluster health monitoring and administration — coordinator/worker health checks, REST API status endpoints (/v1/info /v1/node /v1/query /v1/cluster), JMX MBean metrics (running queries/failed queries/OOM kills/memory pool), Prometheus JMX exporter config, Grafana dashboards, query queue depth monitoring, worker node stability, memory pressure detection, BLOCKED query diagnosis, Web UI interpretation, log analysis, graceful shutdown
---

# Trino Admin: Cluster Health

## When to Use

- Cluster appears slow or queries are queuing unexpectedly
- Workers are reporting failures or dropping out of the cluster
- Memory pressure causing OOM query kills
- Setting up monitoring for a new Trino cluster
- Performing rolling upgrades or worker restarts

---

## REST API Health Endpoints

Trino exposes a comprehensive REST API for monitoring without requiring a client connection.

```bash
COORDINATOR="http://trino-coordinator:8080"

# Cluster info: version, environment, uptime
curl -s $COORDINATOR/v1/info | jq .

# All registered nodes (coordinator + workers)
curl -s $COORDINATOR/v1/node | jq '.[].uri'

# Failed nodes (should be empty in healthy cluster)
curl -s $COORDINATOR/v1/node/failed | jq .

# Cluster-level stats: running queries, blocked queries, active workers
curl -s $COORDINATOR/v1/cluster | jq .

# All active queries with state
curl -s -H "X-Trino-User: admin" $COORDINATOR/v1/query | jq '.[] | {queryId, state, query}'

# Specific query details
curl -s -H "X-Trino-User: admin" $COORDINATOR/v1/query/<query_id> | jq .

# Kill a query
curl -X DELETE -H "X-Trino-User: admin" $COORDINATOR/v1/query/<query_id>
```

---

## Key Health Metrics via REST

```bash
# Worker count and health
curl -s $COORDINATOR/v1/cluster | jq '{
  activeWorkers: .activeWorkers,
  runningQueries: .runningQueries,
  queuedQueries:  .queuedQueries,
  blockedQueries: .blockedQueries
}'

# Check if coordinator is ready to serve requests
curl -s $COORDINATOR/v1/info/state
# → "ACTIVE" = ready, "SHUTTING_DOWN" = graceful shutdown in progress
```

---

## JMX MBeans for Prometheus

Configure Prometheus JMX exporter in `jmx_exporter.yaml`:

```yaml
# prometheus-jmx-exporter.yaml
lowercaseOutputName: true
lowercaseOutputLabelNames: true
rules:
  # Running and queued queries
  - pattern: 'trino.execution<name=QueryManager><>RunningQueries'
    name: trino_running_queries
    type: GAUGE

  - pattern: 'trino.execution<name=QueryManager><>(StartedQueries|FailedQueries)\.FiveMinute\.Count'
    name: trino_queries_$1_5m
    type: GAUGE

  - pattern: 'trino.execution<name=QueryManager><>ExecutionTime\.FiveMinutes\.(P50|P90|P99)'
    name: trino_query_execution_time_$1_ms
    type: GAUGE

  # Memory
  - pattern: 'trino.memory<type=ClusterMemoryPool, name=general><>FreeDistributedBytes'
    name: trino_free_memory_bytes
    type: GAUGE

  - pattern: 'trino.memory<name=ClusterMemoryManager><>QueriesKilledDueToOutOfMemory'
    name: trino_queries_killed_oom_total
    type: COUNTER

  # Workers
  - pattern: 'trino.failuredetector<name=HeartbeatFailureDetector><>ActiveCount'
    name: trino_active_workers
    type: GAUGE

  # Task throughput
  - pattern: 'trino.execution<name=SqlTaskManager><>InputDataSize\.FiveMinute\.Count'
    name: trino_input_data_rate_5m_bytes
    type: GAUGE
```

JVM args to enable JMX exporter:

```
# etc/jvm.config (add to existing config)
-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=9090:/opt/jmx_exporter/prometheus-jmx-exporter.yaml
```

---

## Prometheus Alert Rules

```yaml
# trino-alerts.yaml
groups:
  - name: trino
    rules:
      - alert: TrinoWorkersLow
        expr: trino_active_workers < 3
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Trino has {{ $value }} active workers (threshold: 3)"

      - alert: TrinoQueryQueueHigh
        expr: trino_running_queries > 80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Trino query queue depth {{ $value }} > 80"

      - alert: TrinoOOMKills
        expr: increase(trino_queries_killed_oom_total[5m]) > 0
        labels:
          severity: warning
        annotations:
          summary: "Trino killed {{ $value }} queries due to OOM in last 5m"

      - alert: TrinoHighQueryFailureRate
        expr: |
          rate(trino_queries_FailedQueries_5m[5m]) /
          rate(trino_queries_StartedQueries_5m[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Trino failure rate {{ $value | humanizePercentage }}"

      - alert: TrinoFreeMemoryLow
        expr: trino_free_memory_bytes < 5 * 1024 * 1024 * 1024  # < 5GB free
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Trino cluster free memory {{ $value | humanize1024 }}B"
```

---

## Query State Diagnosis

```bash
# Find all BLOCKED queries (may indicate memory/resource pressure)
curl -s -H "X-Trino-User: admin" $COORDINATOR/v1/query \
  | jq '.[] | select(.state == "BLOCKED") | {queryId, state, queryTextPreview}'

# Find long-running queries (running > 10 minutes)
curl -s -H "X-Trino-User: admin" $COORDINATOR/v1/query \
  | jq --argjson threshold 600000 \
    '.[] | select(.state == "RUNNING") | select(.elapsedTime.toMillis // 0 > $threshold)'

# Count queries by state
curl -s -H "X-Trino-User: admin" $COORDINATOR/v1/query \
  | jq 'group_by(.state) | map({state: .[0].state, count: length})'
```

**Query states reference:**

| State | Normal? | Action if Stuck |
|-------|---------|----------------|
| `QUEUED` | Briefly | Check resource group limits, increase `maxConcurrentQueries` |
| `PLANNING` | Briefly | May indicate stale stats or very complex query |
| `RUNNING` | Expected | Monitor progress via Web UI |
| `BLOCKED` | Briefly | Persistent = memory pressure, investigate spill or kill |
| `FINISHING` | Briefly | Normal finalisation |
| `FAILED` | No | Check query error in `/v1/query/<id>` |

---

## Log Monitoring

```bash
# Trino server log location
tail -f /var/trino/data/var/log/server.log

# Key patterns to alert on:
grep -E "WORKER_LOST|NodeManager|OutOfMemoryError|exceeded memory limit" server.log

# Worker registration events
grep "NodeManager" server.log | tail -50

# OOM query kills
grep "exceeded memory limit" server.log | tail -20
```

Key log patterns:

| Pattern | Meaning | Action |
|---------|---------|--------|
| `Query exceeded memory limit` | OOM kill | Increase `query.max-memory` or add spill |
| `WORKER_LOST` | Worker crashed/disconnected | Check worker logs, heap dumps |
| `Too many tasks active` | Worker overloaded | Scale out workers |
| `Connection refused` | Discovery server unreachable | Check coordinator health |

---

## Graceful Worker Shutdown (Rolling Upgrade)

```bash
# 1. Put worker in graceful shutdown mode (new queries won't be scheduled)
curl -X PUT -H "Content-Type: application/json" \
  http://worker-host:8080/v1/info/state \
  -d '"SHUTTING_DOWN"'

# 2. Wait for all tasks to finish (poll until running tasks = 0)
watch -n 5 'curl -s http://worker-host:8080/v1/info/state'

# 3. Once state = INACTIVE, safely stop the process
bin/launcher stop
```

Configure shutdown grace period:

```properties
# etc/config.properties
shutdown.grace-period=2m
```

---

## Worker Node Checklist

For each worker node, verify:

```bash
# JVM heap usage (should be < 80% of -Xmx)
curl -s http://worker:8080/v1/jmx/mbean/java.lang:type=Memory \
  | jq '.attributes[] | select(.name == "HeapMemoryUsage")'

# Number of active tasks
curl -s http://worker:8080/v1/task | jq 'length'

# Worker is registered with coordinator
curl -s http://coordinator:8080/v1/node | jq '.[].uri' | grep worker-host
```

---

## Web UI Key Views

Navigate to `http://trino-coordinator:8080/ui/`:

| View | Path | What to Check |
|------|------|---------------|
| Query list | `/ui/` | QUEUED count, BLOCKED count, recent FAILED |
| Query details | `/ui/query/<id>` | Stage breakdown, task timings, CPU vs wall time |
| Worker status | `/ui/worker/<id>` | Heap used, tasks running, thread count |
| Resource groups | `/ui/` (filter) | Concurrency limits, queue depth per group |

**Performance signals in Web UI:**
- High "Blocked" percentage = memory or I/O bottleneck
- Many `QUEUED` queries = resource group concurrency limit hit
- `wall_time >> cpu_time` = waiting on network/I/O, not compute

---

## Anti-Patterns

1. **Running coordinator with `node-scheduler.include-coordinator=true` in production** — coordinator becomes a bottleneck for both planning and execution; always set `false` for clusters with > 3 workers.
2. **Not monitoring OOM kills** — `QueriesKilledDueToOutOfMemory` counter silently increments; queries fail with cryptic errors; alert on this metric.
3. **Ignoring BLOCKED query state** — persistent BLOCKED queries hold memory slots and prevent other queries from running; set alert if any query is BLOCKED > 10 minutes.
4. **Hard shutting down workers during query execution** — kills in-flight tasks; always use graceful shutdown via `/v1/info/state` PUT.
5. **Exposing JMX without authentication in production** — JMX can expose sensitive cluster information; bind to localhost only or use TLS.

---

## References

- Admin REST API: `trino.io/docs/current/develop/client-protocol.html`
- Web UI: `trino.io/docs/current/admin/web-interface.html`
- JMX connector: `trino.io/docs/current/connector/jmx.html`
- Related skills: `[[trino-memory-and-spill-tuning]]`, `[[trino-resource-group-governance]]`, `[[trino-observability-platform]]`, `[[trino-production-readiness-review]]`
