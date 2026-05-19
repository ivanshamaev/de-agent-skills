---
name: trino-observability-platform
description: Trino observability and monitoring platform — JMX Prometheus exporter configuration (running queries/failed queries/OOM kills/execution latency P50/P90/P99/memory pool metrics), Grafana dashboard panels, OpenTelemetry trace propagation, query-level event listener for structured logging, Prometheus alert rules (worker loss/queue depth/OOM/failure rate/p99 latency), log aggregation patterns, query history analysis via REST API, slow query detection SQL
---

# Trino Observability Platform

## When to Use

- Setting up Prometheus + Grafana monitoring for a new Trino cluster
- Investigating cluster performance degradation
- Building a slow-query log and alerting system
- Adding OpenTelemetry trace context to Trino queries
- Creating SLA dashboards for data platform consumers

---

## JMX Prometheus Exporter Setup

```yaml
# jmx_exporter_config.yaml
lowercaseOutputName: true
lowercaseOutputLabelNames: true

rules:
  # ── Query execution ───────────────────────────────────────────────
  - pattern: 'trino.execution<name=QueryManager><>RunningQueries'
    name: trino_running_queries
    type: GAUGE
    help: Number of currently running queries

  - pattern: 'trino.execution<name=QueryManager><>QueuedQueries'
    name: trino_queued_queries
    type: GAUGE
    help: Number of queued queries

  - pattern: 'trino.execution<name=QueryManager><>(StartedQueries)\.FiveMinute\.Count'
    name: trino_started_queries_5m
    type: GAUGE

  - pattern: 'trino.execution<name=QueryManager><>(FailedQueries)\.FiveMinute\.Count'
    name: trino_failed_queries_5m
    type: GAUGE

  - pattern: 'trino.execution<name=QueryManager><>ExecutionTime\.FiveMinutes\.(P50|P90|P99)'
    name: trino_query_execution_time_$1_ms
    type: GAUGE
    help: Query execution time percentile in milliseconds

  - pattern: 'trino.execution<name=QueryManager><>WallInputBytesRate\.FiveMinutes\.(P90)'
    name: trino_input_bytes_rate_$1
    type: GAUGE

  # ── Memory ────────────────────────────────────────────────────────
  - pattern: 'trino.memory<type=ClusterMemoryPool, name=general><>FreeDistributedBytes'
    name: trino_free_memory_bytes
    type: GAUGE

  - pattern: 'trino.memory<type=ClusterMemoryPool, name=general><>TotalDistributedBytes'
    name: trino_total_memory_bytes
    type: GAUGE

  - pattern: 'trino.memory<name=ClusterMemoryManager><>QueriesKilledDueToOutOfMemory'
    name: trino_queries_killed_oom_total
    type: COUNTER

  # ── Workers ───────────────────────────────────────────────────────
  - pattern: 'trino.failuredetector<name=HeartbeatFailureDetector><>ActiveCount'
    name: trino_active_workers
    type: GAUGE

  # ── Tasks ─────────────────────────────────────────────────────────
  - pattern: 'trino.execution<name=SqlTaskManager><>InputDataSize\.FiveMinute\.Count'
    name: trino_input_data_size_5m_bytes
    type: GAUGE

  - pattern: 'trino.execution<name=SqlTaskManager><>InputPositions\.FiveMinute\.Count'
    name: trino_input_rows_5m
    type: GAUGE

  # ── JVM ───────────────────────────────────────────────────────────
  - pattern: 'java.lang<type=Memory><HeapMemoryUsage>used'
    name: jvm_heap_used_bytes
    type: GAUGE

  - pattern: 'java.lang<type=Memory><HeapMemoryUsage>max'
    name: jvm_heap_max_bytes
    type: GAUGE

  - pattern: 'java.lang<type=GarbageCollector, name=(.+)><>CollectionTime'
    name: jvm_gc_collection_time_ms
    labels:
      gc: $1
    type: COUNTER
```

Add to `etc/jvm.config`:

```
-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent-0.20.0.jar=9090:/opt/jmx_exporter/jmx_exporter_config.yaml
```

---

## Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval:     15s
  evaluation_interval: 15s

rule_files:
  - /etc/prometheus/rules/trino-rules.yml

scrape_configs:
  - job_name: trino-coordinator
    static_configs:
      - targets: ['trino-coordinator:9090']
    relabel_configs:
      - target_label: role
        replacement: coordinator

  - job_name: trino-workers
    static_configs:
      - targets:
          - 'trino-worker-1:9090'
          - 'trino-worker-2:9090'
          - 'trino-worker-3:9090'
    relabel_configs:
      - target_label: role
        replacement: worker
```

---

## Prometheus Alert Rules

```yaml
# /etc/prometheus/rules/trino-rules.yml
groups:
  - name: trino
    interval: 30s
    rules:

      - alert: TrinoWorkerLost
        expr: trino_active_workers < 3
        for: 2m
        labels:
          severity: critical
          team: platform
        annotations:
          summary: "Trino active workers = {{ $value }} (minimum: 3)"
          runbook: "https://wiki.internal/runbooks/trino-worker-lost"

      - alert: TrinoQueryQueueHigh
        expr: trino_queued_queries > 50
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Trino query queue depth {{ $value }}"

      - alert: TrinoHighP99Latency
        expr: trino_query_execution_time_P99_ms > 300000   # > 5 minutes
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Trino P99 query latency {{ $value | humanizeDuration }}"

      - alert: TrinoOOMKills
        expr: increase(trino_queries_killed_oom_total[10m]) > 0
        labels:
          severity: warning
        annotations:
          summary: "{{ $value }} queries killed due to OOM in last 10m"

      - alert: TrinoHighFailureRate
        expr: |
          rate(trino_failed_queries_5m[5m]) /
          (rate(trino_started_queries_5m[5m]) + 0.001) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Trino failure rate {{ $value | humanizePercentage }}"

      - alert: TrinoLowFreeMemory
        expr: trino_free_memory_bytes / trino_total_memory_bytes < 0.1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Trino cluster free memory {{ $value | humanizePercentage }}"

      - alert: TrinoJVMHeapPressure
        expr: jvm_heap_used_bytes / jvm_heap_max_bytes > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "JVM heap {{ $value | humanizePercentage }} on {{ $labels.instance }}"
```

---

## Grafana Dashboard Panels

```json
// Key panels to include in a Trino dashboard:

// Panel 1: Running + Queued Queries (time series)
{
  "title": "Query Activity",
  "targets": [
    {"expr": "trino_running_queries", "legendFormat": "Running"},
    {"expr": "trino_queued_queries",  "legendFormat": "Queued"}
  ]
}

// Panel 2: Query Latency Percentiles (time series)
{
  "title": "Query Execution Latency",
  "targets": [
    {"expr": "trino_query_execution_time_P50_ms / 1000", "legendFormat": "P50 (s)"},
    {"expr": "trino_query_execution_time_P90_ms / 1000", "legendFormat": "P90 (s)"},
    {"expr": "trino_query_execution_time_P99_ms / 1000", "legendFormat": "P99 (s)"}
  ]
}

// Panel 3: Cluster Memory (gauge)
{
  "title": "Cluster Free Memory",
  "targets": [{"expr": "trino_free_memory_bytes / trino_total_memory_bytes * 100"}],
  "thresholds": [{"color": "red", "value": 10}, {"color": "yellow", "value": 30}]
}

// Panel 4: Worker Count (stat)
{
  "title": "Active Workers",
  "targets": [{"expr": "trino_active_workers"}],
  "thresholds": [{"color": "red", "value": 0}, {"color": "yellow", "value": 3}]
}

// Panel 5: OOM Kills Rate (time series)
{
  "title": "OOM Query Kills",
  "targets": [{"expr": "increase(trino_queries_killed_oom_total[5m])", "legendFormat": "OOM kills/5m"}]
}

// Panel 6: Input Data Throughput
{
  "title": "Input Data Rate",
  "targets": [{"expr": "trino_input_data_size_5m_bytes / 5 / 60", "legendFormat": "bytes/sec"}]
}
```

---

## Event Listener: Structured Query Logging

Trino supports event listeners that receive callbacks on query creation/completion/split completion.

```java
// Custom EventListener plugin — logs slow queries to JSON
public class SlowQueryEventListener implements EventListener {

    private static final Logger log = Logger.getLogger(SlowQueryEventListener.class.getName());
    private final long slowQueryThresholdMs;

    public SlowQueryEventListener(Map<String, String> config) {
        this.slowQueryThresholdMs = Long.parseLong(config.getOrDefault("slow-query-threshold-ms", "30000"));
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event) {
        long wallTimeMs = event.getStatistics().getWallTime().toMillis();
        if (wallTimeMs >= slowQueryThresholdMs || event.getFailureInfo().isPresent()) {
            Map<String, Object> record = Map.of(
                "query_id",       event.getMetadata().getQueryId(),
                "query_text",     event.getMetadata().getQuery().substring(0, Math.min(500, event.getMetadata().getQuery().length())),
                "state",          event.getMetadata().getQueryState(),
                "wall_time_ms",   wallTimeMs,
                "cpu_time_ms",    event.getStatistics().getCpuTime().toMillis(),
                "peak_memory_mb", event.getStatistics().getPeakUserMemoryBytes() / 1024 / 1024,
                "rows_input",     event.getStatistics().getTotalRows(),
                "bytes_input",    event.getStatistics().getTotalBytes(),
                "user",           event.getContext().getUser(),
                "source",         event.getContext().getSource().orElse("unknown"),
                "error",          event.getFailureInfo().map(f -> f.getErrorCode().getName()).orElse(null),
                "timestamp",      event.getCreateTime().toString()
            );
            log.warning("SLOW_QUERY: " + toJson(record));
        }
    }
}
```

Configure in `etc/event-listener.properties`:

```properties
event-listener.name=slow-query-logger
slow-query-threshold-ms=30000
```

---

## Slow Query Detection via REST API

```python
import requests
import json
from datetime import datetime, timedelta

def find_slow_queries(coordinator_url: str, threshold_sec: int = 60) -> list[dict]:
    """Find currently running queries slower than threshold."""
    resp = requests.get(
        f"{coordinator_url}/v1/query",
        headers={"X-Trino-User": "monitoring"}
    )
    queries = resp.json()

    slow = []
    for q in queries:
        if q.get('state') not in ('RUNNING', 'BLOCKED'):
            continue
        elapsed_ms = q.get('elapsedTime', {}).get('toMillis', 0) if isinstance(q.get('elapsedTime'), dict) else 0
        if elapsed_ms > threshold_sec * 1000:
            slow.append({
                'query_id':    q['queryId'],
                'state':       q['state'],
                'elapsed_sec': elapsed_ms / 1000,
                'user':        q.get('session', {}).get('user', 'unknown'),
                'query_text':  q.get('query', '')[:200],
            })

    return sorted(slow, key=lambda x: x['elapsed_sec'], reverse=True)


def get_query_resource_usage(coordinator_url: str, query_id: str) -> dict:
    """Get peak memory, CPU time, data scanned for a query."""
    resp = requests.get(
        f"{coordinator_url}/v1/query/{query_id}",
        headers={"X-Trino-User": "monitoring"}
    )
    data = resp.json()
    stats = data.get('queryStats', {})
    return {
        'query_id':              query_id,
        'state':                 data.get('state'),
        'peak_memory_gb':        stats.get('peakUserMemoryReservation', 0) / 1024**3,
        'cpu_time_sec':          stats.get('totalCpuTime', '0s').rstrip('s'),
        'wall_time_sec':         stats.get('elapsedTime', '0s').rstrip('s'),
        'total_bytes_read_gb':   stats.get('processedInputDataSize', 0) / 1024**3,
        'total_rows_read':       stats.get('processedInputPositions', 0),
        'spilled_bytes_gb':      stats.get('spilledDataSize', 0) / 1024**3,
    }
```

---

## OpenTelemetry Trace Propagation

Pass trace context from orchestration systems into Trino queries via session properties:

```python
from opentelemetry import trace
from trino.dbapi import connect

tracer = trace.get_tracer(__name__)

with tracer.start_as_current_span("trino_query") as span:
    trace_id = format(span.get_span_context().trace_id, '032x')
    span_id  = format(span.get_span_context().span_id, '016x')

    conn = connect(
        host        = 'trino-coordinator',
        port        = 8080,
        user        = 'pipeline_svc',
        http_headers = {
            'X-Trino-Client-Info': f'trace_id={trace_id},span_id={span_id}',
        }
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM iceberg.silver.orders")
    span.set_attribute("db.rows_returned", cur.fetchone()[0])
```

---

## Anti-Patterns

1. **Only alerting on worker count** — worker loss is a lagging indicator; alert on `trino_queued_queries > 20` and P99 latency first as early warning signals.
2. **Scraping JMX every second** — at 1s intervals, JMX collection itself consumes significant CPU on Trino coordinator; use 15s–30s intervals.
3. **Not retaining slow query logs** — without structured query logs, debugging production incidents hours later is impossible; always write completed query metadata to an external store.
4. **Alerting on `trino_running_queries` absolute value** — a cluster with 50 workers may normally run 150 queries; threshold should be relative to `hardConcurrencyLimit`, not absolute.
5. **No JVM heap alert** — GC pressure above 85% heap utilization causes stop-the-world pauses that manifest as query latency spikes, not OOM kills; always monitor heap %.

---

## References

- Trino JMX connector: `trino.io/docs/current/connector/jmx.html`
- Event listener SPI: `trino.io/docs/current/develop/event-listener.html`
- Admin properties: `trino.io/docs/current/admin/properties.html`
- Related skills: `[[trino-admin-cluster-health]]`, `[[trino-memory-and-spill-tuning]]`, `[[trino-production-readiness-review]]`
