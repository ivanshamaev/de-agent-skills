---
name: trino-self-healing-platform
description: Autonomous self-healing Trino platform — Python watchdog agents for automatic worker restart detection, hung query killer, OOM-killed query resubmission, Iceberg compaction auto-trigger on small-file detection, stale statistics auto-ANALYZE, cluster memory pressure relief (kill low-priority queries), anomaly detection on query latency (Z-score), Claude-based RCA generation for incidents, Prometheus AlertManager webhook integration, Airflow self-healing sensor
---

# Trino Self-Healing Platform

## When to Use

- Building autonomous operations for a Trino cluster (reducing on-call load)
- Automatically detecting and recovering from common failure patterns
- Triggering Iceberg maintenance proactively based on file health metrics
- Generating automated RCA reports when Trino alerts fire
- Implementing a watchdog that keeps the cluster in a known-good state

---

## Watchdog Agent Architecture

```
AlertManager Webhook
        │
        ▼
┌──────────────────────────────────────────────┐
│          Trino Self-Healing Agent            │
│                                              │
│  ┌──────────────┐  ┌─────────────────────┐  │
│  │ Worker Watch │  │  Query Health Watch  │  │
│  │ - detect loss│  │  - kill hung queries │  │
│  │ - restart    │  │  - resubmit OOM jobs │  │
│  └──────────────┘  └─────────────────────┘  │
│                                              │
│  ┌──────────────┐  ┌─────────────────────┐  │
│  │ Iceberg Maint│  │  RCA Generator       │  │
│  │ - auto-compact│ │  - Claude API        │  │
│  │ - auto-analyze│ │  - structured report │  │
│  └──────────────┘  └─────────────────────┘  │
└──────────────────────────────────────────────┘
```

---

## Hung Query Killer

```python
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class HungQueryKiller:
    """Automatically kill queries that exceed time thresholds by type."""

    THRESHOLDS = {
        'interactive': 30 * 60,    # 30 minutes
        'batch':      8 * 60 * 60, # 8 hours
        'default':    2 * 60 * 60, # 2 hours
    }

    def __init__(self, coordinator_url: str, admin_user: str = 'platform_watchdog'):
        self.coordinator = coordinator_url
        self.headers = {'X-Trino-User': admin_user}

    def get_running_queries(self) -> list[dict]:
        resp = requests.get(f"{self.coordinator}/v1/query", headers=self.headers, timeout=10)
        resp.raise_for_status()
        return [q for q in resp.json() if q.get('state') in ('RUNNING', 'BLOCKED')]

    def get_query_elapsed_sec(self, query: dict) -> float:
        elapsed = query.get('elapsedTime', '')
        if not elapsed:
            return 0.0
        # Parse ISO 8601 duration like "PT1H23M45.678S"
        import re
        m = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?', str(elapsed))
        if not m:
            return 0.0
        h = float(m.group(1) or 0)
        mi = float(m.group(2) or 0)
        s = float(m.group(3) or 0)
        return h * 3600 + mi * 60 + s

    def classify_query(self, query: dict) -> str:
        source = query.get('session', {}).get('source', '') or ''
        if any(k in source.lower() for k in ('superset', 'tableau', 'metabase', 'adhoc')):
            return 'interactive'
        if any(k in source.lower() for k in ('airflow', 'dbt', 'batch')):
            return 'batch'
        return 'default'

    def kill_query(self, query_id: str, reason: str) -> bool:
        try:
            resp = requests.delete(
                f"{self.coordinator}/v1/query/{query_id}",
                headers=self.headers,
                timeout=10
            )
            logger.warning(f"KILLED query {query_id}: {reason}")
            return resp.status_code in (200, 204)
        except Exception as e:
            logger.error(f"Failed to kill query {query_id}: {e}")
            return False

    def run_cycle(self) -> list[str]:
        killed = []
        for query in self.get_running_queries():
            elapsed = self.get_query_elapsed_sec(query)
            q_type  = self.classify_query(query)
            threshold = self.THRESHOLDS[q_type]

            if elapsed > threshold:
                reason = f"Exceeded {q_type} threshold {threshold/3600:.1f}h (elapsed: {elapsed/3600:.2f}h)"
                if self.kill_query(query['queryId'], reason):
                    killed.append(query['queryId'])

        return killed
```

---

## Memory Pressure Relief

```python
class MemoryPressureReliever:
    """Kill lowest-priority queries when cluster memory is critically low."""

    def __init__(self, coordinator_url: str, free_memory_threshold_pct: float = 0.05):
        self.coordinator = coordinator_url
        self.threshold = free_memory_threshold_pct
        self.headers = {'X-Trino-User': 'platform_watchdog'}

    def get_free_memory_pct(self) -> float:
        resp = requests.get(f"{self.coordinator}/v1/cluster", headers=self.headers, timeout=5)
        data = resp.json()
        total = data.get('totalMemoryBytes', 1)
        free  = data.get('freeMemoryBytes', 0)
        return free / total

    def get_queries_by_memory(self) -> list[dict]:
        resp = requests.get(f"{self.coordinator}/v1/query", headers=self.headers, timeout=10)
        queries = [q for q in resp.json() if q.get('state') == 'RUNNING']
        # Sort by memory usage ascending (kill least-memory-using first — less impact)
        return sorted(queries, key=lambda q: q.get('totalMemoryReservation', 0))

    def relieve_pressure(self) -> list[str]:
        killed = []
        free_pct = self.get_free_memory_pct()

        if free_pct > self.threshold:
            return killed  # healthy

        logger.warning(f"Memory pressure: {free_pct:.1%} free. Killing low-priority queries.")

        # Kill adhoc queries first (lowest business impact)
        for query in self.get_queries_by_memory():
            source = query.get('session', {}).get('source', '')
            if any(k in (source or '').lower() for k in ('adhoc', 'jupyter', 'superset')):
                self.kill_query(query['queryId'], "memory pressure relief")
                killed.append(query['queryId'])

                # Re-check memory after each kill
                if self.get_free_memory_pct() > self.threshold * 2:
                    break

        return killed

    def kill_query(self, query_id: str, reason: str) -> bool:
        resp = requests.delete(
            f"{self.coordinator}/v1/query/{query_id}",
            headers=self.headers, timeout=10
        )
        logger.warning(f"Killed query {query_id} for: {reason}")
        return resp.status_code in (200, 204)
```

---

## Iceberg Auto-Compaction Trigger

```python
class IcebergCompactionAgent:
    """Monitor Iceberg table health and trigger compaction when needed."""

    SMALL_FILE_THRESHOLD_MB = 10      # files < 10MB are "small"
    SMALL_FILE_RATIO_TRIGGER = 0.3    # compact when 30%+ of files are small
    FILE_COUNT_TRIGGER = 500          # compact when partition has > 500 files

    def __init__(self, trino_conn_id: str = 'trino_default'):
        from airflow.providers.trino.hooks.trino import TrinoHook
        self.hook = TrinoHook(trino_conn_id=trino_conn_id)

    def get_unhealthy_partitions(self, catalog: str, schema: str, table: str) -> list[dict]:
        rows = self.hook.get_records(f"""
            SELECT
                CAST(partition AS VARCHAR)              AS partition_str,
                file_count,
                total_size,
                record_count,
                -- ratio of small files (< 10MB) to total files
                CAST(
                    (SELECT COUNT(*) FROM iceberg.{schema}."{table}$files" f
                     WHERE f.file_size_in_bytes < {self.SMALL_FILE_THRESHOLD_MB} * 1024 * 1024)
                    AS DOUBLE
                ) / NULLIF(file_count, 0) AS small_file_ratio
            FROM iceberg.{schema}."{table}$partitions"
            WHERE file_count > {self.FILE_COUNT_TRIGGER}
               OR (
                   (SELECT COUNT(*) FROM iceberg.{schema}."{table}$files" f
                    WHERE f.file_size_in_bytes < {self.SMALL_FILE_THRESHOLD_MB} * 1024 * 1024)
                   * 1.0 / NULLIF(file_count, 0) > {self.SMALL_FILE_RATIO_TRIGGER}
               )
            ORDER BY file_count DESC
            LIMIT 10
        """)
        return [{'partition': r[0], 'file_count': r[1], 'total_size': r[2]} for r in rows]

    def compact_table(self, catalog: str, schema: str, table: str, where_clause: str = '') -> None:
        sql = f"""
            ALTER TABLE {catalog}.{schema}.{table}
            EXECUTE optimize(file_size_threshold => '256MB')
            {('WHERE ' + where_clause) if where_clause else ''}
        """
        self.hook.run(sql)
        logger.info(f"Compacted {catalog}.{schema}.{table} [{where_clause or 'full table'}]")

    def auto_maintain(self, tables: list[tuple]) -> list[dict]:
        """tables = [(catalog, schema, table), ...]"""
        actions = []
        for catalog, schema, table in tables:
            partitions = self.get_unhealthy_partitions(catalog, schema, table)
            for part in partitions:
                self.compact_table(catalog, schema, table)
                actions.append({'table': f"{catalog}.{schema}.{table}", 'partition': part['partition']})
        return actions
```

---

## Stale Statistics Auto-ANALYZE

```python
class StaleStatsAnalyzer:
    """Automatically run ANALYZE on tables with stale or missing statistics."""

    def __init__(self, trino_conn_id: str = 'trino_default'):
        from airflow.providers.trino.hooks.trino import TrinoHook
        self.hook = TrinoHook(trino_conn_id=trino_conn_id)

    def find_stale_tables(self, schema: str, max_age_hours: int = 24) -> list[str]:
        """Find tables where stats haven't been refreshed in max_age_hours."""
        rows = self.hook.get_records(f"""
            SELECT table_name
            FROM iceberg.information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_type = 'BASE TABLE'
        """)
        tables_needing_analyze = []

        for (table_name,) in rows:
            result = self.hook.get_first(f"""
                SELECT MAX(last_updated_ms)
                FROM iceberg.{schema}."{table_name}$properties"
                WHERE key = 'statistics_last_updated'
            """)
            last_updated_ms = result[0] if result and result[0] else 0
            age_hours = (datetime.utcnow().timestamp() * 1000 - last_updated_ms) / 3_600_000

            if age_hours > max_age_hours:
                tables_needing_analyze.append(table_name)

        return tables_needing_analyze

    def analyze_tables(self, schema: str, tables: list[str]) -> None:
        for table in tables:
            self.hook.run(f"""
                ANALYZE iceberg.{schema}.{table}
                WITH (columns = ARRAY['customer_id', 'order_date', 'region', 'status'])
            """)
            logger.info(f"ANALYZE complete: iceberg.{schema}.{table}")
```

---

## Claude-Based RCA Generator

```python
import anthropic
import requests

def generate_trino_rca(
    alert_name: str,
    alert_description: str,
    coordinator_url: str,
) -> str:
    """Generate structured RCA using Claude when a Trino alert fires."""

    client = anthropic.Anthropic()

    # Collect cluster context
    cluster_info = requests.get(f"{coordinator_url}/v1/cluster",
                                headers={'X-Trino-User': 'watchdog'}).json()
    failed_queries = requests.get(f"{coordinator_url}/v1/query",
                                  headers={'X-Trino-User': 'watchdog'}).json()
    failed_queries = [q for q in failed_queries if q.get('state') == 'FAILED'][-5:]

    context = f"""
Alert: {alert_name}
Description: {alert_description}

Cluster state:
- Active workers: {cluster_info.get('activeWorkers', 'unknown')}
- Running queries: {cluster_info.get('runningQueries', 0)}
- Queued queries: {cluster_info.get('queuedQueries', 0)}
- Free memory: {cluster_info.get('freeMemoryBytes', 0) / 1e9:.1f} GB

Recent failures:
{chr(10).join([f"- {q['queryId']}: {q.get('failureInfo', {}).get('type', 'unknown')}" for q in failed_queries])}
"""

    response = client.messages.create(
        model      = 'claude-haiku-4-5-20251001',
        max_tokens = 1024,
        system     = """You are a Trino platform reliability engineer.
Analyze the cluster state and generate a concise RCA report with:
1. Root cause (1-2 sentences)
2. Impact
3. Immediate remediation steps
4. Prevention recommendations
Keep the report under 300 words.""",
        messages = [{'role': 'user', 'content': context}]
    )

    return response.content[0].text
```

---

## Airflow Self-Healing Sensor DAG

```python
from airflow.decorators import dag, task
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta

@dag(
    dag_id            = 'trino_self_healing_watchdog',
    start_date        = datetime(2024, 1, 1),
    schedule_interval = '*/15 * * * *',   # every 15 minutes
    catchup           = False,
    default_args      = {'retries': 0},
    tags              = ['self-healing', 'trino'],
)
def self_healing_dag():

    @task
    def check_and_heal() -> dict:
        import requests

        coordinator = 'http://trino-coordinator:8080'
        actions = {}

        # 1. Kill hung queries
        killer = HungQueryKiller(coordinator)
        killed = killer.run_cycle()
        if killed:
            actions['killed_hung_queries'] = killed

        # 2. Relieve memory pressure
        reliever = MemoryPressureReliever(coordinator)
        freed = reliever.relieve_pressure()
        if freed:
            actions['killed_for_memory'] = freed

        # 3. Auto-compact unhealthy Iceberg tables
        compactor = IcebergCompactionAgent()
        compacted = compactor.auto_maintain([
            ('iceberg', 'bronze', 'orders_raw'),
            ('iceberg', 'silver', 'orders'),
            ('iceberg', 'silver', 'events'),
        ])
        if compacted:
            actions['compacted_tables'] = compacted

        return actions

    check_and_heal()

self_healing_dag()
```

---

## Latency Anomaly Detection (Z-Score)

```python
import statistics

def detect_latency_anomaly(coordinator_url: str, window_queries: int = 100) -> dict:
    """Z-score detection on recent query wall times."""
    from airflow.providers.trino.hooks.trino import TrinoHook
    hook = TrinoHook(trino_conn_id='trino_default')

    rows = hook.get_records(f"""
        SELECT total_wall_time_secs
        FROM system.runtime.queries
        WHERE state = 'FINISHED'
          AND create_time >= NOW() - INTERVAL '1' HOUR
        ORDER BY create_time DESC
        LIMIT {window_queries}
    """)

    times = [r[0] for r in rows if r[0] is not None]
    if len(times) < 10:
        return {'status': 'insufficient_data'}

    mean   = statistics.mean(times)
    stdev  = statistics.stdev(times)
    latest = times[0]

    z_score = (latest - mean) / stdev if stdev > 0 else 0.0

    return {
        'latest_wall_sec': latest,
        'mean_wall_sec':   round(mean, 1),
        'stdev_wall_sec':  round(stdev, 1),
        'z_score':         round(z_score, 2),
        'anomaly':         abs(z_score) > 3.0,
    }
```

---

## Anti-Patterns

1. **Killing queries without logging the reason** — autonomous kills look like mysterious failures to users; always log `query_id + reason + timestamp` to a persistent store.
2. **Self-healing agent with too-aggressive thresholds** — killing queries after 10 minutes is too aggressive for batch jobs; classify by source and apply appropriate thresholds per query type.
3. **Running auto-compaction during peak hours** — compaction is CPU and I/O intensive; trigger it only during off-peak windows (e.g., 2-6 AM).
4. **Infinite retry loops for killed queries** — if a query keeps being killed, don't re-queue it indefinitely; add a max-retry counter and alert after 3 failures.
5. **No human approval for cluster-wide actions** — autonomous memory relief killing 20+ queries simultaneously is risky; add a Slack approval gate for batch kills above 5 queries.

---

## References

- Trino REST API: `trino.io/docs/current/develop/client-protocol.html`
- System runtime tables: `trino.io/docs/current/connector/system.html`
- Related skills: `[[trino-admin-cluster-health]]`, `[[trino-memory-and-spill-tuning]]`, `[[trino-observability-platform]]`, `[[trino-airflow-lakehouse-pipelines]]`
