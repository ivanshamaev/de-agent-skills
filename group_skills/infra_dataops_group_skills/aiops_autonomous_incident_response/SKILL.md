---
name: aiops-autonomous-incident-response
description: AIOps autonomous incident response agent — LLM-driven diagnosis loop (Claude tool-use agent with kubectl/SQL/Prometheus tools), alert-to-action pipeline (PagerDuty webhook → agent trigger), automated RCA generation (failure taxonomy + log correlation), self-healing action executor with approval gate, incident severity classification, runbook automation (structured YAML runbooks executed by agent), escalation logic, incident timeline auto-generation, Slack integration for human-in-the-loop approvals
---

# AIOps Autonomous Incident Response

## When to Use

- Automating first-responder diagnosis for data platform alerts
- Reducing MTTR by running diagnostic steps autonomously before paging a human
- Generating RCA drafts from logs, metrics, and Kubernetes events
- Executing known runbook steps automatically for recurring incident patterns
- Building an AI copilot that assists on-call engineers with context-rich summaries

---

## Architecture Overview

```
Alert (PagerDuty/Alertmanager)
    ↓
Incident Router (severity classification)
    ↓
Autonomous Agent (Claude claude-sonnet-4-6 with tools)
    ├── Tool: kubectl_get_pods
    ├── Tool: prometheus_query
    ├── Tool: fetch_airflow_task_logs
    ├── Tool: query_metadata_db
    └── Tool: execute_healing_action (requires approval for SEV1/2)
    ↓
Action Decision
    ├── Auto-heal (low-risk, pre-approved actions)
    ├── Human approval gate (Slack button → approve/deny)
    └── Escalate to on-call (complex / novel incident)
    ↓
Incident Report (posted to Slack + stored as postmortem draft)
```

---

## Tool Definitions for Incident Agent

```python
import anthropic
import subprocess
import json
from datetime import datetime, timezone

client = anthropic.Anthropic()

TOOLS = [
    {
        "name": "kubectl_describe",
        "description": "Get Kubernetes pod/deployment/node status and events. Use to diagnose OOMKilled, CrashLoopBackOff, pending pods.",
        "input_schema": {
            "type": "object",
            "properties": {
                "resource_type": {"type": "string", "enum": ["pod", "deployment", "node", "pvc", "job"]},
                "name": {"type": "string", "description": "Resource name or label selector (e.g., app=airflow-scheduler)"},
                "namespace": {"type": "string", "default": "airflow"},
            },
            "required": ["resource_type"],
        },
    },
    {
        "name": "prometheus_query",
        "description": "Query Prometheus metrics for the last N minutes. Use to check CPU, memory, Kafka lag, error rates.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "PromQL query"},
                "lookback_minutes": {"type": "integer", "default": 30},
            },
            "required": ["query"],
        },
    },
    {
        "name": "fetch_pod_logs",
        "description": "Fetch last N lines of logs from a Kubernetes pod. Use to find exception stack traces and error messages.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pod_name": {"type": "string"},
                "namespace": {"type": "string", "default": "airflow"},
                "tail_lines": {"type": "integer", "default": 200},
                "previous": {"type": "boolean", "default": False, "description": "Fetch logs from previous container (after crash)"},
            },
            "required": ["pod_name"],
        },
    },
    {
        "name": "airflow_task_diagnosis",
        "description": "Query Airflow metadata DB for failed tasks, recent DAG runs, scheduler heartbeat. Use when an Airflow DAG is failing.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "lookback_hours": {"type": "integer", "default": 6},
            },
            "required": ["dag_id"],
        },
    },
    {
        "name": "execute_healing_action",
        "description": "Execute a pre-approved healing action. ONLY use for low-risk actions: restart pod, clear failed tasks, trigger backfill. HIGH-RISK actions require human approval.",
        "input_schema": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["restart_pod", "clear_failed_tasks", "trigger_dag_backfill", "scale_deployment"],
                },
                "target": {"type": "string", "description": "Resource to act on"},
                "namespace": {"type": "string", "default": "airflow"},
                "risk_level": {"type": "string", "enum": ["low", "medium", "high"]},
            },
            "required": ["action", "target", "risk_level"],
        },
    },
]
```

---

## Tool Executor

```python
def execute_tool(tool_name: str, tool_input: dict) -> str:
    """Execute a tool call from the agent and return the result."""

    if tool_name == "kubectl_describe":
        cmd = ["kubectl", "describe", tool_input["resource_type"]]
        if "name" in tool_input:
            cmd.extend([tool_input["name"], "-n", tool_input.get("namespace", "airflow")])
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return result.stdout[:4000] + ("...[truncated]" if len(result.stdout) > 4000 else "")

    elif tool_name == "prometheus_query":
        import requests
        minutes = tool_input.get("lookback_minutes", 30)
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": tool_input["query"], "time": "now"},
            timeout=10,
        )
        data = resp.json()
        return json.dumps(data.get("data", {}).get("result", [])[:10])  # limit results

    elif tool_name == "fetch_pod_logs":
        cmd = ["kubectl", "logs", tool_input["pod_name"],
               "-n", tool_input.get("namespace", "airflow"),
               f"--tail={tool_input.get('tail_lines', 200)}"]
        if tool_input.get("previous"):
            cmd.append("--previous")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        logs = result.stdout
        # Filter to error/warning lines for brevity
        error_lines = [l for l in logs.split("\n") if any(x in l.upper() for x in ["ERROR", "WARN", "EXCEPTION", "OOM", "KILLED"])]
        return "\n".join(error_lines[-100:]) or logs[-2000:]

    elif tool_name == "airflow_task_diagnosis":
        from sqlalchemy import create_engine, text
        engine = create_engine(AIRFLOW_DB_URI)
        dag_id = tool_input["dag_id"]
        hours = tool_input.get("lookback_hours", 6)
        result = engine.execute(text("""
            SELECT task_id, state, start_date, end_date, try_number
            FROM task_instance
            WHERE dag_id = :dag_id
              AND start_date > NOW() - INTERVAL ':hours hours'
              AND state IN ('failed', 'upstream_failed')
            ORDER BY start_date DESC LIMIT 20
        """), {"dag_id": dag_id, "hours": hours})
        rows = [dict(r) for r in result]
        return json.dumps(rows, default=str)

    elif tool_name == "execute_healing_action":
        risk = tool_input.get("risk_level", "medium")
        if risk in ("medium", "high"):
            # Request human approval via Slack
            return request_approval(tool_input)

        # Low-risk: execute immediately
        action = tool_input["action"]
        target = tool_input["target"]
        ns = tool_input.get("namespace", "airflow")

        if action == "restart_pod":
            subprocess.run(["kubectl", "delete", "pod", target, "-n", ns])
            return f"Pod {target} deleted (will be recreated by ReplicaSet)"
        elif action == "clear_failed_tasks":
            subprocess.run(["airflow", "tasks", "clear", target, "--yes"])
            return f"Cleared failed tasks for DAG: {target}"

        return f"Action {action} not implemented"

    return f"Unknown tool: {tool_name}"
```

---

## Incident Response Agent Loop

```python
def run_incident_agent(
    alert_name: str,
    alert_labels: dict,
    alert_annotations: dict,
    max_turns: int = 10,
) -> dict:
    """
    Run the autonomous incident response agent.
    Returns structured incident report.
    """
    incident_id = f"INC-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    system_prompt = """You are an autonomous incident response agent for a data platform.
Your goal is to diagnose the incident, identify the root cause, and take safe corrective actions.

PROTOCOL:
1. Gather diagnostic information using tools before drawing conclusions
2. Follow the failure taxonomy: Infrastructure → Data → Logic → Dependency → Configuration
3. For healing actions: only execute LOW-RISK actions autonomously; request approval for medium/high
4. Always quantify the impact (services affected, data lag, users impacted)
5. End with a structured incident report including: root_cause, impact, actions_taken, recommended_followup

CONSTRAINTS:
- Never delete persistent data without human approval
- Never scale down production deployments without approval
- Maximum 3 healing action attempts before escalating"""

    user_message = f"""
INCIDENT: {alert_name}
ID: {incident_id}
Time: {datetime.now(timezone.utc).isoformat()}
Labels: {json.dumps(alert_labels)}
Description: {alert_annotations.get('description', 'No description')}

Investigate this incident and take appropriate action.
"""

    messages = [{"role": "user", "content": user_message}]
    actions_taken = []
    tool_results = []

    for turn in range(max_turns):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=system_prompt,
            tools=TOOLS,
            messages=messages,
        )

        # Add assistant response to conversation
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            # Agent finished — extract structured report
            final_text = next(
                (b.text for b in response.content if hasattr(b, "text")),
                "No report generated"
            )
            return {
                "incident_id": incident_id,
                "alert_name": alert_name,
                "report": final_text,
                "actions_taken": actions_taken,
                "tool_calls": len(tool_results),
                "resolved": "root_cause" in final_text.lower(),
            }

        if response.stop_reason == "tool_use":
            # Execute all tool calls
            tool_result_blocks = []
            for block in response.content:
                if block.type == "tool_use":
                    result = execute_tool(block.name, block.input)
                    tool_results.append({"tool": block.name, "input": block.input})
                    if block.name == "execute_healing_action":
                        actions_taken.append(f"{block.input['action']} → {block.input['target']}")

                    tool_result_blocks.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })

            messages.append({"role": "user", "content": tool_result_blocks})

    return {"incident_id": incident_id, "error": "Max turns reached — escalating to on-call"}
```

---

## Alert Webhook Trigger

```python
from flask import Flask, request, jsonify
import threading

app = Flask(__name__)

@app.route("/webhook/alertmanager", methods=["POST"])
def handle_alertmanager():
    """Receive Alertmanager webhook and trigger autonomous response."""
    payload = request.json

    for alert in payload.get("alerts", []):
        if alert["status"] != "firing":
            continue

        alert_name = alert["labels"].get("alertname", "unknown")
        severity = alert["labels"].get("severity", "warning")

        # Only auto-respond to critical and warning alerts
        if severity not in ("critical", "warning"):
            continue

        # Run agent in background thread
        thread = threading.Thread(
            target=_run_and_post,
            args=(alert_name, alert["labels"], alert["annotations"]),
            daemon=True,
        )
        thread.start()

    return jsonify({"status": "accepted"})

def _run_and_post(alert_name, labels, annotations):
    report = run_incident_agent(alert_name, labels, annotations)
    post_to_slack(report)   # post summary + link to full report
```

---

## Slack Approval Gate

```python
def request_approval(action: dict) -> str:
    """Send Slack message with approve/deny buttons for risky actions."""
    import requests

    message = {
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text":
                    f"*Incident Agent requests approval*\n"
                    f"Action: `{action['action']}`\n"
                    f"Target: `{action['target']}`\n"
                    f"Risk: *{action['risk_level'].upper()}*"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {"type": "button", "text": {"type": "plain_text", "text": "Approve"},
                     "style": "primary", "value": f"approve:{json.dumps(action)}"},
                    {"type": "button", "text": {"type": "plain_text", "text": "Deny"},
                     "style": "danger", "value": f"deny:{json.dumps(action)}"},
                ]
            }
        ]
    }

    requests.post(SLACK_WEBHOOK, json=message)
    return "PENDING_APPROVAL: Action sent to Slack for approval. Waiting..."
```

---

## Anti-Patterns

1. **Fully autonomous healing without guardrails** — an agent that can delete PVCs, drop tables, or scale-to-zero without approval can turn a minor incident into a major outage.
2. **No action audit trail** — every action taken by the agent must be logged with timestamp, justification, and outcome for postmortem review.
3. **Agent hallucinating resource names** — validate all kubectl/API inputs against a whitelist of known resource names before execution.
4. **Infinite retry on failing tool** — if kubectl fails 3 times, escalate to human rather than looping; set max_turns and circuit break.
5. **Single-LLM diagnosis without validation** — agent diagnosis should be validated by at least one confirming data source before healing actions are taken.

---

## References

- Claude tool use: `platform.claude.com/docs/en/docs/build-with-claude/tool-use`
- Anthropic agentic patterns: `anthropic.com/research/building-effective-agents`
- Alertmanager webhooks: `prometheus.io/docs/alerting/latest/configuration/#webhook_config`
- Related skills: `[[aiops-infrastructure-anomaly-detection]]`, `[[dataops-root-cause-analysis]]`, `[[dataops-postmortem-generator]]`, `[[dataops-self-healing-platform]]`
