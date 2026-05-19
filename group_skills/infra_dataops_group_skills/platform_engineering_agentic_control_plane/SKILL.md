---
name: platform-engineering-agentic-control-plane
description: Agentic control plane for data platform — MCP (Model Context Protocol) server exposing platform tools to AI agents (FastMCP/Python SDK), natural language platform operations (trigger DAG/create topic/run dbt/query Trino via LLM), Claude-based platform assistant with tool use, multi-agent platform governance (audit agent/cost agent/reliability agent), MCP server deployment (Docker/Kubernetes), tool authorization and audit logging, agentic workflow patterns (plan-execute-verify), platform chat interface
---

# Agentic Control Plane for Data Platform

## When to Use

- Enabling AI agents (Claude Code, custom agents) to operate the data platform through a standardized MCP interface
- Building a natural language chat interface for data platform operations ("trigger the orders ETL for yesterday")
- Creating a multi-agent governance system where specialized agents monitor cost, reliability, and compliance
- Exposing platform tools to any MCP-compatible AI client without custom integrations per client
- Implementing agentic workflows that observe platform state and take autonomous corrective actions

---

## MCP Server for Data Platform

### Server Definition (FastMCP)

```python
# platform_mcp_server.py
from mcp.server.fastmcp import FastMCP
from datetime import datetime, timedelta
import subprocess, json, logging

logger = logging.getLogger(__name__)

# Initialize MCP server — name shown in Claude/agent UIs
mcp = FastMCP(
    "data-platform",
    instructions="""You are the data platform control plane.
Use these tools to manage Airflow DAGs, Kafka topics, Trino queries, and dbt jobs.
Always confirm destructive operations before executing.
Log a reason for every action you take."""
)
```

### Platform Tools

```python
@mcp.tool()
async def list_airflow_dags(
    active_only: bool = True,
    tag: str | None = None,
) -> str:
    """List all Airflow DAGs with their current status.

    Args:
        active_only: If True, only return non-paused DAGs
        tag: Filter by tag (e.g., 'gold-layer', 'kafka', 'orders')
    """
    from sqlalchemy import create_engine, text
    engine = create_engine(AIRFLOW_DB_URI)
    query = """
        SELECT dag_id, is_paused, is_active,
               tags,
               last_parsed_time,
               next_dagrun
        FROM dag
        WHERE (:active_only = FALSE OR is_active = TRUE)
          AND (:tag IS NULL OR :tag = ANY(ARRAY(SELECT t.name FROM dag_tag t WHERE t.dag_id = dag.dag_id)))
        ORDER BY dag_id LIMIT 50
    """
    rows = engine.execute(text(query), {"active_only": active_only, "tag": tag}).fetchall()
    return json.dumps([dict(r) for r in rows], default=str)


@mcp.tool()
async def trigger_airflow_dag(
    dag_id: str,
    conf: dict | None = None,
    reason: str = "triggered by AI agent",
) -> str:
    """Trigger an Airflow DAG run.

    Args:
        dag_id: The DAG identifier to trigger
        conf: Optional configuration dictionary passed to the DAG
        reason: Human-readable reason for triggering (required for audit)
    """
    from airflow.api.client.local_client import Client
    import uuid

    client = Client(None, None)
    run_id = f"agent_{uuid.uuid4().hex[:8]}"

    logger.info(f"Agent triggering DAG {dag_id}: {reason}")
    client.trigger_dag(
        dag_id=dag_id,
        run_id=run_id,
        conf={**(conf or {}), "_agent_reason": reason, "_triggered_at": datetime.utcnow().isoformat()},
    )
    return json.dumps({"run_id": run_id, "dag_id": dag_id, "status": "triggered", "reason": reason})


@mcp.tool()
async def get_dag_run_status(dag_id: str, run_id: str) -> str:
    """Get the status of a specific DAG run.

    Args:
        dag_id: The DAG identifier
        run_id: The run identifier returned by trigger_airflow_dag
    """
    from sqlalchemy import create_engine, text
    engine = create_engine(AIRFLOW_DB_URI)
    row = engine.execute(text("""
        SELECT dr.state, dr.start_date, dr.end_date,
               COUNT(CASE WHEN ti.state = 'failed' THEN 1 END) AS failed_tasks,
               COUNT(CASE WHEN ti.state = 'success' THEN 1 END) AS success_tasks,
               COUNT(*) AS total_tasks
        FROM dag_run dr
        LEFT JOIN task_instance ti ON dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id
        WHERE dr.dag_id = :dag_id AND dr.run_id = :run_id
        GROUP BY dr.state, dr.start_date, dr.end_date
    """), {"dag_id": dag_id, "run_id": run_id}).fetchone()

    if not row:
        return json.dumps({"error": f"Run {run_id} not found for DAG {dag_id}"})

    return json.dumps({
        "dag_id": dag_id, "run_id": run_id,
        "state": row.state,
        "started_at": str(row.start_date),
        "finished_at": str(row.end_date),
        "tasks": {"total": row.total_tasks, "success": row.success_tasks, "failed": row.failed_tasks},
    }, default=str)


@mcp.tool()
async def query_trino(
    sql: str,
    catalog: str = "iceberg",
    schema: str = "gold",
    max_rows: int = 100,
) -> str:
    """Execute a read-only SQL query against Trino.

    Args:
        sql: SELECT query to execute (INSERT/UPDATE/DROP not permitted)
        catalog: Trino catalog (default: iceberg)
        schema: Default schema for the query
        max_rows: Maximum rows to return (default: 100, max: 1000)

    Returns:
        JSON with columns and rows
    """
    # Safety: only allow read operations
    normalized = sql.strip().upper()
    if not (normalized.startswith("SELECT") or normalized.startswith("WITH") or normalized.startswith("SHOW") or normalized.startswith("DESCRIBE")):
        return json.dumps({"error": "Only SELECT, WITH, SHOW, DESCRIBE queries are permitted"})

    max_rows = min(max_rows, 1000)

    from trino.dbapi import connect
    conn = connect(host=TRINO_HOST, port=443, user="agent", catalog=catalog, schema=schema)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchmany(max_rows)
    cols = [d[0] for d in cur.description]
    return json.dumps({"columns": cols, "rows": rows, "row_count": len(rows)})


@mcp.tool()
async def get_kafka_consumer_lag(
    group: str | None = None,
    topic: str | None = None,
) -> str:
    """Get Kafka consumer group lag.

    Args:
        group: Consumer group name (omit for all groups)
        topic: Filter by topic name
    """
    cmd = ["kafka-consumer-groups.sh", "--bootstrap-server", KAFKA_BOOTSTRAP, "--describe"]
    if group:
        cmd.extend(["--group", group])
    else:
        cmd.append("--all-groups")

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return json.dumps({"error": result.stderr})

    # Parse output: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG
    lines = [l for l in result.stdout.split("\n") if l.strip() and not l.startswith("GROUP")]
    parsed = []
    for line in lines:
        parts = line.split()
        if len(parts) >= 6:
            try:
                lag = int(parts[5]) if parts[5] != "-" else 0
                if topic is None or parts[1] == topic:
                    parsed.append({
                        "group": parts[0], "topic": parts[1], "partition": int(parts[2]),
                        "current_offset": parts[3], "log_end_offset": parts[4], "lag": lag,
                    })
            except (ValueError, IndexError):
                continue

    # Aggregate by group+topic
    from collections import defaultdict
    agg = defaultdict(int)
    for p in parsed:
        agg[f"{p['group']}::{p['topic']}"] += p["lag"]

    return json.dumps([
        {"group": k.split("::")[0], "topic": k.split("::")[1], "total_lag": v}
        for k, v in sorted(agg.items(), key=lambda x: x[1], reverse=True)
    ])


@mcp.tool()
async def create_kafka_topic(
    name: str,
    partitions: int,
    retention_hours: int = 168,
    reason: str = "created by AI agent",
) -> str:
    """Create a new Kafka topic. Requires naming convention: {env}.{domain}.{entity}.v{N}.

    Args:
        name: Topic name following convention: prod.orders.created.v1
        partitions: Number of partitions (1-96)
        retention_hours: Data retention in hours (default: 168 = 7 days)
        reason: Reason for creating the topic (required for audit)
    """
    import re
    if not re.match(r'^(prod|staging|dev)\.[a-z]+\.[a-z_]+\.v\d+$', name):
        return json.dumps({"error": "Topic name must match: {env}.{domain}.{entity}.v{N}"})
    if partitions < 1 or partitions > 96:
        return json.dumps({"error": "Partitions must be between 1 and 96"})

    logger.info(f"Agent creating Kafka topic {name}: {reason}")
    result = subprocess.run([
        "kafka-topics.sh", "--bootstrap-server", KAFKA_BOOTSTRAP,
        "--create", "--topic", name,
        "--partitions", str(partitions),
        "--replication-factor", "3",
        "--config", "min.insync.replicas=2",
        "--config", f"retention.ms={retention_hours * 3600000}",
    ], capture_output=True, text=True, timeout=30)

    if result.returncode != 0:
        return json.dumps({"error": result.stderr})

    return json.dumps({"topic": name, "partitions": partitions, "status": "created", "reason": reason})


@mcp.tool()
async def get_platform_health() -> str:
    """Get overall data platform health: Airflow scheduler heartbeat, Kafka cluster status, recent pipeline failures."""
    from sqlalchemy import create_engine, text
    engine = create_engine(AIRFLOW_DB_URI)

    # Scheduler heartbeat
    heartbeat = engine.execute(text("""
        SELECT TIMESTAMPDIFF(SECOND, latest_heartbeat, NOW()) AS age_sec
        FROM job WHERE job_type = 'SchedulerJob' ORDER BY latest_heartbeat DESC LIMIT 1
    """)).scalar()

    # Failed DAGs in last 1h
    failed_dags = engine.execute(text("""
        SELECT COUNT(*) FROM dag_run
        WHERE state = 'failed' AND start_date > NOW() - INTERVAL '1' HOUR
    """)).scalar()

    # Kafka broker status
    brokers_raw = subprocess.run(
        ["kafka-broker-api-versions.sh", "--bootstrap-server", KAFKA_BOOTSTRAP],
        capture_output=True, text=True, timeout=10
    )

    return json.dumps({
        "scheduler_heartbeat_age_sec": heartbeat,
        "scheduler_healthy": heartbeat < 30 if heartbeat else False,
        "failed_dag_runs_1h": failed_dags,
        "kafka_brokers_reachable": brokers_raw.returncode == 0,
        "checked_at": datetime.utcnow().isoformat(),
    })
```

---

## Running the MCP Server

```python
# Entry point
if __name__ == "__main__":
    import sys
    # STDIO transport for Claude Desktop / Claude Code
    mcp.run(transport="stdio")
```

```bash
# Start with SSE transport for web clients
uvicorn platform_mcp_server:mcp.get_app() --host 0.0.0.0 --port 8080
```

### Claude Desktop Configuration

```json
{
  "mcpServers": {
    "data-platform": {
      "command": "python",
      "args": ["/opt/platform/platform_mcp_server.py"],
      "env": {
        "AIRFLOW_DB_URI": "postgresql://airflow:secret@airflow-db:5432/airflow",
        "KAFKA_BOOTSTRAP": "kafka:9092",
        "TRINO_HOST": "trino.internal"
      }
    }
  }
}
```

---

## Multi-Agent Governance

```python
# governance_agents.py — specialized agents that continuously monitor the platform
import anthropic
import schedule, time

client = anthropic.Anthropic()

def run_cost_governance_agent():
    """Cost governance agent: detects expensive queries, idle resources, over-retained topics."""
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2048,
        system="""You are a cost governance agent for a data platform.
Use tools to identify cost waste and generate a prioritized action list.
Check: 1) queries with > 100 GB scanned today, 2) topics with retention > 30 days,
3) idle deployments with 0 CPU for > 7 days.
Output a JSON list of findings with priority and estimated savings.""",
        tools=TOOLS,   # reuse platform MCP tools
        messages=[{"role": "user", "content": "Run cost governance audit for today."}],
    )
    _process_agent_response(response, "cost_governance")


def run_reliability_agent():
    """Reliability agent: checks SLA compliance, consumer lag, failed DAGs."""
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2048,
        system="""You are a reliability agent. Check platform health every 15 minutes.
If any consumer lag > 50000 messages OR any critical DAG failed in the last hour,
immediately trigger healing actions and notify the team.
Otherwise, confirm 'All systems healthy'.""",
        tools=TOOLS,
        messages=[{"role": "user", "content": "Run reliability check."}],
    )
    _process_agent_response(response, "reliability")


# Schedule agents
schedule.every(15).minutes.do(run_reliability_agent)
schedule.every(1).hours.do(run_cost_governance_agent)

if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(60)
```

---

## Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: platform-mcp-server
  namespace: platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: platform-mcp-server
  template:
    spec:
      containers:
        - name: mcp-server
          image: ghcr.io/myorg/platform-mcp-server:latest
          command: ["uvicorn", "platform_mcp_server:app", "--host", "0.0.0.0", "--port", "8080"]
          env:
            - name: AIRFLOW_DB_URI
              valueFrom:
                secretKeyRef:
                  name: platform-secrets
                  key: airflow-db-uri
            - name: KAFKA_BOOTSTRAP
              value: "production-kafka-bootstrap.kafka.svc.cluster.local:9093"
          resources:
            requests:
              cpu: "0.25"
              memory: 256Mi
            limits:
              cpu: "1"
              memory: 512Mi
```

---

## Anti-Patterns

1. **No authorization on MCP tools** — every tool callable by any agent with access; implement tool-level authorization (read-only tools vs write tools vs admin tools).
2. **Stateful agents without checkpointing** — long-running agentic workflows lose state on failure; checkpoint agent progress to a store and design for resumption.
3. **Agents that can call each other recursively** — two agents triggering each other creates infinite loops; enforce a maximum call depth.
4. **MCP server with production DB credentials in env** — MCP servers are accessible to any connected agent; use read-only replicas for query tools, and separate credentials per tool scope.
5. **Agentic platform ops without rollback** — an agent that applies 50 VPA changes in one cycle is hard to roll back; batch changes in small sets with validation between batches.

---

## References

- MCP Python SDK: `modelcontextprotocol.io/docs/develop/build-server`
- FastMCP: `github.com/jlowin/fastmcp`
- Claude tool use: `platform.claude.com/docs/en/docs/build-with-claude/tool-use`
- Anthropic agentic patterns: `anthropic.com/research/building-effective-agents`
- Related skills: `[[platform-engineering-data-platform-api]]`, `[[platform-engineering-internal-developer-platform]]`, `[[aiops-autonomous-incident-response]]`, `[[dataops-self-healing-platform]]`
