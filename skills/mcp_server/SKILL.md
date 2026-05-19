---
name: mcp-server
description: MCP (Model Context Protocol) server development — FastMCP/Python SDK, tools/resources/prompts primitives, STDIO and Streamable HTTP transports, Claude Desktop/Claude Code client config, MCP Inspector testing, security best practices (input validation/OAuth2/confused deputy prevention), production Docker/Kubernetes deployment, agentic data platform integration
---

# MCP Server Development

## When to Use

- Building a server that exposes tools, data, or prompt templates to any MCP-compatible AI client (Claude Desktop, Claude Code, Cursor, VS Code Copilot)
- Integrating AI agents with internal systems: databases, Airflow, Kafka, REST APIs, filesystems
- Creating a reusable integration layer — write once, works in all MCP-compatible clients
- Exposing platform operations to Claude Code agents without custom integrations per client

---

## Architecture

```
┌─────────────────────────────────────┐
│  MCP Host (Claude Desktop / Claude Code)
│  ┌──────────────┐  ┌──────────────┐  │
│  │  MCP Client 1│  │  MCP Client 2│  │
│  └──────┬───────┘  └──────┬───────┘  │
└─────────│─────────────────│──────────┘
          │ STDIO           │ HTTP/SSE
          ▼                 ▼
   Local MCP Server     Remote MCP Server
```

| Role | Description |
|------|-------------|
| **Host** | AI application managing connections (Claude Desktop, Claude Code) |
| **Client** | One connection per server, lives inside the host |
| **Server** | Program exposing tools/resources/prompts via JSON-RPC 2.0 |

**Transport options:**
- **STDIO** — local subprocess; parent communicates via stdin/stdout; never `print()` in handlers
- **Streamable HTTP** — remote server; SSE for server→client streams; requires session management

---

## Server Primitives

| Primitive | Controlled By | Purpose |
|-----------|--------------|---------|
| **Tools** | Model (LLM decides when to call) | Actions: query DB, trigger DAG, call API |
| **Resources** | Application (client decides when to read) | Read-only data: config, logs, metrics |
| **Prompts** | User (human selects from UI) | Reusable message templates with parameters |

---

## Setup

```bash
pip install "mcp[cli]"
# or for FastMCP high-level API:
pip install fastmcp
```

```python
# server.py — minimal FastMCP server
from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    "my-server",
    instructions="You are a data platform assistant. Use these tools to query data and manage pipelines."
)

if __name__ == "__main__":
    mcp.run(transport="stdio")
```

---

## Tools

Tools are the primary way the model takes actions. Annotate with `@mcp.tool()`.

```python
import json
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("data-platform")

@mcp.tool()
async def query_database(
    sql: str,
    database: str = "production",
    max_rows: int = 100,
) -> str:
    """Execute a read-only SQL query.

    Args:
        sql: SELECT query to run (INSERT/UPDATE/DROP not permitted)
        database: Target database name
        max_rows: Maximum rows returned (max: 1000)
    """
    # Validate: only allow read operations
    normalized = sql.strip().upper()
    if not any(normalized.startswith(kw) for kw in ("SELECT", "WITH", "SHOW", "DESCRIBE")):
        return json.dumps({"error": "Only SELECT, WITH, SHOW, DESCRIBE are permitted"})

    max_rows = min(max_rows, 1000)
    # ... execute query
    return json.dumps({"columns": cols, "rows": rows, "row_count": len(rows)})


@mcp.tool()
async def trigger_pipeline(
    dag_id: str,
    conf: dict | None = None,
    reason: str = "triggered by AI agent",
) -> str:
    """Trigger an Airflow DAG run.

    Args:
        dag_id: DAG identifier
        conf: Optional run configuration
        reason: Reason for trigger (required for audit log)
    """
    import uuid, logging
    logging.getLogger(__name__).info(f"Agent triggering {dag_id}: {reason}")
    # ... trigger DAG
    run_id = f"agent_{uuid.uuid4().hex[:8]}"
    return json.dumps({"run_id": run_id, "dag_id": dag_id, "status": "triggered"})
```

**Tool annotations** — declare read-only vs destructive intent:

```python
from mcp.server.models import Tool
from mcp.types import Annotations

@mcp.tool(annotations=Annotations(readOnlyHint=True))
async def list_topics() -> str:
    """List all Kafka topics."""
    ...

@mcp.tool(annotations=Annotations(destructiveHint=True, requiresConfirmationHint=True))
async def delete_topic(name: str) -> str:
    """Delete a Kafka topic. This action is irreversible."""
    ...
```

---

## Resources

Resources provide read-only context the application can attach to conversations. Use URI patterns.

```python
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("data-platform")

# Static resource
@mcp.resource("platform://config")
async def get_platform_config() -> str:
    """Current platform configuration."""
    return json.dumps({
        "kafka_brokers": "kafka:9092",
        "trino_host": "trino.internal",
        "environment": "production",
    })

# Dynamic resource with URI template
@mcp.resource("dag://{dag_id}/last-run")
async def get_last_dag_run(dag_id: str) -> str:
    """Get the last run status for a specific DAG."""
    # ... query Airflow metadata DB
    return json.dumps({"dag_id": dag_id, "state": state, "duration_sec": duration})

# Resource returning binary content (e.g., a plot image)
@mcp.resource("report://{name}/chart", mime_type="image/png")
async def get_chart(name: str) -> bytes:
    """Generate and return a chart as PNG."""
    # ... generate matplotlib figure
    return png_bytes
```

---

## Prompts

Prompts are reusable message templates exposed to the user through client UIs.

```python
from mcp.types import PromptMessage, TextContent

@mcp.prompt()
async def investigate_failure(dag_id: str, run_id: str) -> list[PromptMessage]:
    """Template for investigating a failed DAG run."""
    return [
        PromptMessage(
            role="user",
            content=TextContent(
                type="text",
                text=f"""Investigate the failure of DAG '{dag_id}' run '{run_id}'.

Steps to follow:
1. Call get_dag_run_status(dag_id='{dag_id}', run_id='{run_id}') to get current state
2. Call get_task_logs(dag_id='{dag_id}', run_id='{run_id}') to fetch error logs
3. Check upstream dependencies and data quality
4. Propose a fix and trigger a rerun if appropriate

Document your findings in a structured root cause analysis."""
            )
        )
    ]
```

---

## Transport Configuration

### STDIO (local, subprocess)

```python
# server.py
if __name__ == "__main__":
    mcp.run(transport="stdio")
```

```json
// claude_desktop_config.json  (~/.config/claude/claude_desktop_config.json on Linux)
{
  "mcpServers": {
    "data-platform": {
      "command": "python",
      "args": ["/opt/platform/server.py"],
      "env": {
        "AIRFLOW_DB_URI": "postgresql://airflow:secret@airflow-db/airflow",
        "TRINO_HOST": "trino.internal"
      }
    }
  }
}
```

```json
// Claude Code: .claude/settings.json
{
  "mcpServers": {
    "data-platform": {
      "command": "python",
      "args": ["/opt/platform/server.py"]
    }
  }
}
```

### Streamable HTTP (remote)

```python
# server.py — HTTP transport with SSE
import uvicorn

app = mcp.get_asgi_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
```

```json
// Claude Desktop remote server
{
  "mcpServers": {
    "data-platform-remote": {
      "type": "http",
      "url": "https://platform-mcp.internal/mcp"
    }
  }
}
```

---

## Testing with MCP Inspector

```bash
# Interactive browser-based testing UI — no client needed
npx @modelcontextprotocol/inspector python /path/to/server.py

# With env vars
npx @modelcontextprotocol/inspector \
  -e AIRFLOW_DB_URI=postgresql://... \
  python server.py

# Test HTTP server
npx @modelcontextprotocol/inspector --transport http --url http://localhost:8080/mcp
```

The Inspector lets you:
- Browse all tools/resources/prompts the server exposes
- Call tools interactively with custom arguments
- Inspect raw JSON-RPC messages
- Validate schemas before connecting a real client

---

## Logging and Debugging

```python
import logging
import sys

# CRITICAL: always log to stderr, never stdout (STDIO transport uses stdout for JSON-RPC)
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

@mcp.tool()
async def my_tool(param: str) -> str:
    logger.info(f"my_tool called with param={param!r}")
    try:
        result = do_work(param)
        logger.info(f"my_tool succeeded: {result}")
        return result
    except Exception as e:
        logger.error(f"my_tool failed: {e}", exc_info=True)
        return json.dumps({"error": str(e)})
```

**Critical rule:** Never use `print()` in STDIO servers — it corrupts the JSON-RPC stream.

---

## Security

### Input Validation

```python
import re, json

@mcp.tool()
async def create_topic(name: str, partitions: int) -> str:
    """Create a Kafka topic with naming convention enforcement."""
    # Whitelist validation — reject anything not matching expected pattern
    if not re.match(r'^(prod|staging|dev)\.[a-z]+\.[a-z_]+\.v\d+$', name):
        return json.dumps({"error": "Topic name must match: {env}.{domain}.{entity}.v{N}"})
    if not (1 <= partitions <= 96):
        return json.dumps({"error": "partitions must be 1–96"})
    # ... proceed
```

### OAuth2 / JWT for HTTP Servers

```python
from fastapi import Security, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt

security = HTTPBearer()

async def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
```

### Confused Deputy Prevention

Validate that the authenticated identity has permission for the specific resource being accessed. Do not trust `resource_id` parameters without checking ownership.

```python
@mcp.tool()
async def get_pipeline_logs(dag_id: str, user_token: str) -> str:
    user = decode_token(user_token)
    # Check user's team owns this DAG — don't assume dag_id is safe
    if not user_owns_dag(user["team"], dag_id):
        return json.dumps({"error": "Access denied"})
    # ... fetch logs
```

### Secrets: Never Hardcode

```python
import os

# Load from environment — set via client config or Kubernetes secrets
AIRFLOW_DB_URI = os.environ["AIRFLOW_DB_URI"]
JWT_SECRET = os.environ["JWT_SECRET"]
```

---

## Production Deployment

### Docker

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN useradd -r -u 1001 mcpuser && chown -R mcpuser /app
USER mcpuser

EXPOSE 8080
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]
```

### Kubernetes

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
          command: ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]
          env:
            - name: AIRFLOW_DB_URI
              valueFrom:
                secretKeyRef:
                  name: platform-secrets
                  key: airflow-db-uri
          resources:
            requests:
              cpu: "0.25"
              memory: 256Mi
            limits:
              cpu: "1"
              memory: 512Mi
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
```

---

## Complete Example: Data Platform MCP Server

```python
# platform_mcp_server.py
from mcp.server.fastmcp import FastMCP
from datetime import datetime
import json, logging, os, sys

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)

AIRFLOW_DB_URI = os.environ["AIRFLOW_DB_URI"]
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TRINO_HOST = os.environ["TRINO_HOST"]

mcp = FastMCP(
    "data-platform",
    instructions="""You are the data platform control plane.
Use these tools to manage Airflow DAGs, Kafka topics, Trino queries, and dbt jobs.
Always confirm destructive operations before executing.
Log a reason for every action you take."""
)

@mcp.tool()
async def list_airflow_dags(active_only: bool = True, tag: str | None = None) -> str:
    """List all Airflow DAGs with current status."""
    from sqlalchemy import create_engine, text
    engine = create_engine(AIRFLOW_DB_URI)
    rows = engine.execute(text("""
        SELECT dag_id, is_paused, is_active, next_dagrun
        FROM dag
        WHERE (:active_only = FALSE OR is_active = TRUE)
        ORDER BY dag_id LIMIT 50
    """), {"active_only": active_only, "tag": tag}).fetchall()
    return json.dumps([dict(r) for r in rows], default=str)


@mcp.tool()
async def query_trino(sql: str, catalog: str = "iceberg", schema: str = "gold", max_rows: int = 100) -> str:
    """Execute a read-only SQL query against Trino."""
    normalized = sql.strip().upper()
    if not any(normalized.startswith(kw) for kw in ("SELECT", "WITH", "SHOW", "DESCRIBE")):
        return json.dumps({"error": "Only SELECT/WITH/SHOW/DESCRIBE permitted"})
    from trino.dbapi import connect
    conn = connect(host=TRINO_HOST, port=443, user="agent", catalog=catalog, schema=schema)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchmany(min(max_rows, 1000))
    cols = [d[0] for d in cur.description]
    return json.dumps({"columns": cols, "rows": rows, "row_count": len(rows)})


@mcp.resource("platform://health")
async def get_platform_health() -> str:
    """Overall platform health status."""
    from sqlalchemy import create_engine, text
    engine = create_engine(AIRFLOW_DB_URI)
    heartbeat_age = engine.execute(text("""
        SELECT EXTRACT(EPOCH FROM (NOW() - latest_heartbeat))
        FROM job WHERE job_type = 'SchedulerJob' ORDER BY latest_heartbeat DESC LIMIT 1
    """)).scalar()
    return json.dumps({
        "scheduler_healthy": heartbeat_age < 30 if heartbeat_age else False,
        "checked_at": datetime.utcnow().isoformat(),
    })


@mcp.prompt()
async def investigate_dag_failure(dag_id: str, run_id: str) -> list:
    from mcp.types import PromptMessage, TextContent
    return [PromptMessage(role="user", content=TextContent(type="text", text=f"""
Investigate DAG failure: dag_id='{dag_id}', run_id='{run_id}'.

1. Call list_airflow_dags() to confirm the DAG exists and is active
2. Check the run status and failed task details
3. Review recent upstream pipeline completions
4. Propose a remediation plan
"""))]


if __name__ == "__main__":
    mcp.run(transport="stdio")
```

---

## Anti-Patterns

1. **`print()` in STDIO server** — corrupts the JSON-RPC message stream; all output goes to stderr via `logging`.
2. **No input validation on tool parameters** — allows prompt injection and command injection; whitelist all inputs.
3. **Hardcoded secrets in server code** — MCP servers connect to real systems; load credentials from environment variables or Vault.
4. **Missing tool annotations** — don't declare `destructiveHint=True` on delete/overwrite tools; clients can't warn users before destructive actions.
5. **Stateless session handling for HTTP** — Streamable HTTP requires session IDs (`Mcp-Session-Id` header); without them concurrent clients collide.
6. **Mixing STDIO and HTTP transports in one binary** — transport is selected at startup; don't try to auto-detect or switch at runtime.
7. **Tools that call each other recursively** — agents can loop; each tool must be a leaf action, not an orchestrator.
8. **Production credentials in Claude Desktop config JSON** — the `env` block in client config is plaintext; use a wrapper script that reads from Vault or the OS keychain.

---

## Quick Reference

```bash
# Install
pip install "mcp[cli]" fastmcp

# Test locally with Inspector
npx @modelcontextprotocol/inspector python server.py

# Run STDIO server (invoked by client, not standalone)
python server.py

# Run HTTP server
uvicorn server:app --host 0.0.0.0 --port 8080
```

```python
# Skeleton
from mcp.server.fastmcp import FastMCP
import sys, logging

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
mcp = FastMCP("my-server")

@mcp.tool()
async def my_tool(param: str) -> str:
    """Tool description shown to LLM."""
    return "result"

@mcp.resource("data://{item_id}")
async def my_resource(item_id: str) -> str:
    return "resource content"

if __name__ == "__main__":
    mcp.run(transport="stdio")
```

---

## References

- Full spec and guide: `docs/specs/mcp_server_guide.md`
- MCP Python SDK: `modelcontextprotocol.io/docs/develop/build-server`
- FastMCP: `github.com/jlowin/fastmcp`
- MCP Inspector: `modelcontextprotocol.io/docs/tools/inspector`
- Security best practices: `modelcontextprotocol.io/docs/tutorials/security/security_best_practices`
- Related skills: `[[platform-engineering-agentic-control-plane]]`, `[[platform-engineering-data-platform-api]]`, `[[aiops-autonomous-incident-response]]`
