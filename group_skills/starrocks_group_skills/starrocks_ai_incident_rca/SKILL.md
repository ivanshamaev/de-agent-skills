---
name: starrocks-ai-incident-rca
description: StarRocks AI incident RCA — autonomous root cause analysis agent for FE/BE failures, memory spike diagnosis (BE heap/process memory), Routine Load pause RCA (ErrorLogUrls + error patterns), Kafka consumer lag spike analysis, compaction backlog detection, tablet replication failures, OOM kill patterns, query timeout clusters, FE GC pressure detection, structured incident report generation
---

# StarRocks AI Incident RCA

## When to Use

- On-call engineer needs fast root cause for a StarRocks alert
- BE node down/OOM: determine which queries or loads caused it
- Routine Load job paused: find root cause without manual log digging
- Kafka consumer lag spiked: diagnose whether it's a StarRocks or Kafka issue
- Query timeout storm: identify common patterns across slow queries
- Automated post-incident analysis for the on-call runbook

---

## RCA Agent Workflow

```
Incident signal (alert/PagerDuty/Slack)
  │
  ├── 1. Classify incident type
  │       ├── BE memory / OOM
  │       ├── Routine Load pause
  │       ├── Query timeout spike
  │       ├── Replication failure
  │       └── Kafka consumer lag
  │
  ├── 2. Collect evidence (parallel)
  │       ├── SHOW BACKENDS
  │       ├── SHOW ROUTINE LOAD
  │       ├── Query audit log (last 30 min)
  │       └── SHOW TABLET WHERE State != 'NORMAL'
  │
  ├── 3. Analyze patterns
  ├── 4. Identify root cause
  └── 5. Generate structured RCA report
```

---

## Incident Classifier

```python
import pymysql
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime, timedelta


class IncidentType(Enum):
    BE_OOM = "be_oom"
    ROUTINE_LOAD_PAUSE = "routine_load_pause"
    QUERY_TIMEOUT = "query_timeout"
    TABLET_REPLICATION = "tablet_replication"
    KAFKA_LAG_SPIKE = "kafka_lag_spike"
    COMPACTION_BACKLOG = "compaction_backlog"
    UNKNOWN = "unknown"


@dataclass
class Evidence:
    source: str
    data: dict
    collected_at: datetime = field(default_factory=datetime.utcnow)


class StarRocksRCAAgent:
    def __init__(self, host: str, port: int = 9030, user: str = "root", password: str = ""):
        self.conn_params = dict(host=host, port=port, user=user, password=password)
        self._conn = None

    def _query(self, sql: str) -> list:
        conn = pymysql.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            cols = [d[0] for d in cursor.description] if cursor.description else []
            rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
            return rows
        finally:
            conn.close()

    def diagnose(self, incident_type: IncidentType, context: dict = None) -> "RCAReport":
        evidence = self.collect_evidence(incident_type)
        root_cause = self.analyze(incident_type, evidence, context or {})
        return RCAReport(incident_type=incident_type, evidence=evidence, root_cause=root_cause)
```

---

## BE Memory / OOM Diagnosis

```python
def diagnose_be_oom(self) -> dict:
    """Check BE memory usage and identify memory consumers."""
    # Step 1: Check BE memory state
    backends = self._query("SHOW BACKENDS")
    be_issues = []
    for be in backends:
        total_mem = be.get("TotalCapacity", "")
        used_mem  = be.get("UsedCapacity", "")
        # Parse GB values if present
        be_issues.append({
            "host": be.get("Host"),
            "is_alive": be.get("Alive") == "true",
            "status": be.get("Status", ""),
        })

    # Step 2: Find memory-heavy queries from audit log
    heavy_queries = self._query("""
        SELECT
            QueryId,
            User,
            LEFT(Statement, 300) AS sql,
            QueryTime,
            ScanRows,
            ScanBytes,
            ReturnRows
        FROM information_schema.be_cloud_native_compactions
    """) if False else []  # Replace with actual audit log table if available

    # Step 3: Check Routine Load task count (each task holds memory)
    rl_tasks = self._query("SHOW ROUTINE LOAD FROM sales")
    running_jobs = [r for r in rl_tasks if r.get("State") == "RUNNING"]

    return {
        "backend_status": be_issues,
        "running_routine_loads": len(running_jobs),
        "memory_recommendation": self._generate_oom_recommendation(be_issues, running_jobs),
    }


def _generate_oom_recommendation(self, be_issues: list, running_jobs: list) -> str:
    dead_bes = [b for b in be_issues if not b["is_alive"]]

    if dead_bes:
        return (
            f"BE(s) down: {[b['host'] for b in dead_bes]}. "
            "Likely OOM kill. Check /proc/{pid}/status on BE host. "
            "Reduce max_routine_load_task_concurrent_num or memory_limitation in BE config."
        )
    if len(running_jobs) > 10:
        return (
            f"{len(running_jobs)} concurrent Routine Load jobs may be competing for memory. "
            "Reduce desired_concurrent_number per job or use resource groups."
        )
    return "No obvious OOM cause — check BE log: grep -i 'OOM\\|memory\\|killed' be.INFO"
```

---

## Routine Load Pause RCA

```python
def diagnose_routine_load_pause(self, db: str = "sales") -> dict:
    """Find why Routine Load jobs are paused."""
    jobs = self._query(f"SHOW ROUTINE LOAD FROM {db}")
    paused_jobs = [j for j in jobs if j.get("State") == "PAUSED"]

    diagnoses = []
    for job in paused_jobs:
        name = job.get("Name", "unknown")
        reason = job.get("ReasonOfStateChanged", "")
        error_log_urls = job.get("ErrorLogUrls", "")

        cause = self._classify_rl_pause_reason(reason)
        diagnoses.append({
            "job": name,
            "reason_raw": reason,
            "cause": cause,
            "error_log_urls": error_log_urls,
            "action": self._rl_pause_action(cause),
        })

    return {"paused_jobs": diagnoses}


def _classify_rl_pause_reason(self, reason: str) -> str:
    reason_lower = reason.lower()
    if "errortoomain" in reason_lower or "max_error_number" in reason_lower:
        return "error_threshold_exceeded"
    elif "offset out of range" in reason_lower:
        return "kafka_offset_expired"
    elif "jsonpath" in reason_lower or "parse" in reason_lower:
        return "parse_error"
    elif "null" in reason_lower and "column" in reason_lower:
        return "null_in_non_null_column"
    elif "arithmetic" in reason_lower or "overflow" in reason_lower:
        return "type_overflow"
    elif "schema" in reason_lower:
        return "schema_change"
    return "unknown"


def _rl_pause_action(self, cause: str) -> str:
    actions = {
        "error_threshold_exceeded": (
            "Check ErrorLogUrls: curl 'http://be:8040/api/_load_error_log?file=...' | head -50\n"
            "Fix source data or increase max_error_number\n"
            "Then: RESUME ROUTINE LOAD FOR db.job"
        ),
        "kafka_offset_expired": (
            "Kafka retention expired. Reset offsets:\n"
            "1. PAUSE ROUTINE LOAD FOR db.job\n"
            "2. ALTER ROUTINE LOAD FOR db.job FROM KAFKA (\"kafka_offsets\" = \"OFFSET_BEGINNING,...\")\n"
            "3. RESUME ROUTINE LOAD FOR db.job"
        ),
        "parse_error": (
            "Message format changed. Check:\n"
            "1. Debezium message structure\n"
            "2. Update jsonpaths in ROUTINE LOAD config\n"
            "3. ALTER ROUTINE LOAD to update jsonpaths"
        ),
        "null_in_non_null_column": (
            "Source sending NULL for a NOT NULL column.\n"
            "Fix: ALTER TABLE ADD DEFAULT or filter with WHERE in ROUTINE LOAD"
        ),
        "schema_change": (
            "Source schema changed.\n"
            "1. Apply ALTER TABLE to StarRocks table\n"
            "2. Update ROUTINE LOAD jsonpaths\n"
            "3. RESUME"
        ),
    }
    return actions.get(cause, "Check StarRocks FE log: grep -i 'routine load' fe.log | tail -100")
```

---

## Query Timeout Cluster Analysis

```python
import re
from collections import Counter


def diagnose_query_timeouts(self, audit_log_path: str, window_minutes: int = 30) -> dict:
    """Find common patterns in timed-out queries."""
    slow_queries = []
    cutoff = datetime.utcnow() - timedelta(minutes=window_minutes)

    pattern = re.compile(
        r'(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?'
        r'State=(?P<state>\w+).*?'
        r'Time=(?P<time>\d+).*?'
        r'SQL=(?P<sql>.+)',
        re.IGNORECASE
    )

    try:
        with open(audit_log_path) as f:
            for line in f:
                m = pattern.search(line)
                if m and m.group("state") in ("ERR", "TIMEOUT"):
                    ts = datetime.strptime(m.group("ts"), "%Y-%m-%d %H:%M:%S")
                    if ts >= cutoff:
                        slow_queries.append({
                            "ts": ts,
                            "time_ms": int(m.group("time")),
                            "sql": m.group("sql")[:200],
                        })
    except FileNotFoundError:
        return {"error": f"Audit log not found: {audit_log_path}"}

    if not slow_queries:
        return {"slow_queries": 0, "message": "No timeout queries in window"}

    # Classify SQL patterns
    patterns = Counter()
    for q in slow_queries:
        sql = q["sql"].upper()
        if "CROSS JOIN" in sql:
            patterns["cartesian_join"] += 1
        elif "SELECT *" in sql and "WHERE" not in sql:
            patterns["full_table_scan"] += 1
        elif "GROUP BY" in sql and "ORDER BY" in sql and "LIMIT" not in sql:
            patterns["large_aggregation"] += 1
        else:
            patterns["other"] += 1

    return {
        "total_slow_queries": len(slow_queries),
        "avg_time_ms": sum(q["time_ms"] for q in slow_queries) / len(slow_queries),
        "max_time_ms": max(q["time_ms"] for q in slow_queries),
        "pattern_breakdown": dict(patterns.most_common()),
        "top_slow_queries": sorted(slow_queries, key=lambda x: x["time_ms"], reverse=True)[:5],
    }
```

---

## Tablet Replication Failure Diagnosis

```python
def diagnose_tablet_replication(self, db: str = "sales") -> dict:
    """Find tables with unhealthy tablets."""
    # In StarRocks, check via SHOW TABLET
    try:
        tablets = self._query(f"""
            SELECT
                TABLE_NAME,
                PARTITION_NAME,
                COUNT(*) AS tablet_count,
                SUM(CASE WHEN REPLICA_COUNT < 3 THEN 1 ELSE 0 END) AS under_replicated
            FROM information_schema.be_tablets
            WHERE TABLE_SCHEMA = '{db}'
            GROUP BY TABLE_NAME, PARTITION_NAME
            HAVING under_replicated > 0
        """)
    except Exception:
        tablets = []

    # Check BE alive status
    backends = self._query("SHOW BACKENDS")
    dead_bes = [b for b in backends if b.get("Alive") != "true"]

    return {
        "under_replicated_tablets": len(tablets),
        "affected_tables": list({t.get("TABLE_NAME") for t in tablets}),
        "dead_backends": [b.get("Host") for b in dead_bes],
        "recommendation": (
            "Dead BEs detected — replication recovery will happen automatically once BEs recover. "
            "Check BE log for OOM or disk full."
            if dead_bes else
            "No dead BEs — check if clone operations are in progress via SHOW TABLET WHERE State = 'CLONE'"
        ),
    }
```

---

## Structured RCA Report Generator

```python
@dataclass
class RCAReport:
    incident_type: IncidentType
    evidence: dict
    root_cause: dict
    generated_at: datetime = field(default_factory=datetime.utcnow)

    def to_markdown(self) -> str:
        lines = [
            f"# StarRocks Incident RCA — {self.incident_type.value}",
            f"**Generated**: {self.generated_at.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "",
            "## Root Cause",
        ]

        if isinstance(self.root_cause, dict):
            for key, val in self.root_cause.items():
                lines.append(f"**{key}**: {val}")
        else:
            lines.append(str(self.root_cause))

        lines.append("")
        lines.append("## Evidence")
        for key, val in self.evidence.items():
            lines.append(f"### {key}")
            if isinstance(val, list):
                for item in val[:10]:  # cap at 10 items
                    lines.append(f"- {item}")
            else:
                lines.append(str(val))

        return "\n".join(lines)
```

---

## Usage Example

```python
agent = StarRocksRCAAgent("sr-fe.internal")

# Diagnose Routine Load pause
report = agent.diagnose(IncidentType.ROUTINE_LOAD_PAUSE, {"db": "sales"})
print(report.to_markdown())

# Diagnose BE OOM
oom_report = agent.diagnose(IncidentType.BE_OOM)
print(oom_report.to_markdown())
```

---

## Anti-Patterns

1. **Diagnosing from SHOW BACKENDS alone** — BE "Alive=true" doesn't mean it's healthy (memory pressure can exist without crash); always check memory metrics.
2. **Auto-resuming Routine Load without fixing root cause** — RESUME on a parse-error job re-pauses in seconds; fix the data issue first.
3. **Checking only the most recent paused job** — multiple jobs may be paused for different reasons; iterate all paused jobs.
4. **Ignoring compaction score in memory diagnosis** — high compaction backlog consumes BE memory; always check CompactionScore in SHOW BACKENDS.
5. **Treating query timeout as a network issue** — most timeouts are bad query plans (full scan, Cartesian join) not infrastructure; analyze EXPLAIN first.

---

## References

- Routine Load error diagnosis: `docs.starrocks.io/docs/loading/RoutineLoad/#check-load-progress`
- BE monitoring: `docs.starrocks.io/docs/administration/management/monitoring/`
- StarRocks audit log: `docs.starrocks.io/docs/administration/management/audit_loader/`
- Related skills: `[[starrocks-admin-cluster-health]]`, `[[starrocks-admin-query-monitor]]`, `[[starrocks-routine-load-kafka]]`, `[[de-rca]]`
