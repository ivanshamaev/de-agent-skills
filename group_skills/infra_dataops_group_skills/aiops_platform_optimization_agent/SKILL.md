---
name: aiops-platform-optimization-agent
description: AIOps continuous platform optimization agent — autonomous optimization loop (observe→analyze→recommend→apply), Kubernetes resource rightsizing (VPA recommendation reader + apply), Kafka consumer lag auto-scale, Spark job configuration tuning from history, idle resource detection (unused deployments/PVCs/topics), compaction and VACUUM scheduling, auto-ANALYZE stale statistics, cost savings attribution, optimization audit trail, human approval gate for high-risk changes
---

# AIOps Platform Optimization Agent

## When to Use

- Running continuous platform optimization without manual intervention
- Detecting and reclaiming idle or over-provisioned Kubernetes resources
- Automatically applying VPA recommendations to right-size workloads
- Scheduling compaction/VACUUM/ANALYZE jobs based on table statistics
- Building a weekly optimization report with savings attribution

---

## Optimization Agent Architecture

```python
import anthropic
from dataclasses import dataclass
from enum import Enum
from typing import Callable

class OptimizationRisk(Enum):
    LOW = "low"       # apply automatically
    MEDIUM = "medium" # apply with notification
    HIGH = "high"     # require human approval

@dataclass
class OptimizationAction:
    id: str
    category: str          # compute / storage / kafka / database
    resource: str          # what to optimize
    action: str            # what to do
    expected_saving: str   # human-readable benefit
    risk: OptimizationRisk
    apply_fn: Callable     # function to execute the optimization

class PlatformOptimizationAgent:
    """Continuously discover and apply platform optimizations."""

    def __init__(self, approval_required_for_high_risk: bool = True):
        self.client = anthropic.Anthropic()
        self.approval_required = approval_required_for_high_risk
        self.applied_actions: list[dict] = []

    def discover_optimizations(self) -> list[OptimizationAction]:
        """Discover optimization opportunities across the platform."""
        actions = []
        actions.extend(self._check_kubernetes_rightsizing())
        actions.extend(self._check_idle_resources())
        actions.extend(self._check_kafka_optimizations())
        actions.extend(self._check_database_maintenance())
        return actions

    def run_optimization_cycle(self) -> dict:
        """One optimization cycle: discover → prioritize → apply → report."""
        actions = self.discover_optimizations()

        # Use LLM to prioritize by expected impact
        prioritized = self._prioritize_with_llm(actions)

        applied = []
        skipped = []

        for action in prioritized:
            if action.risk == OptimizationRisk.LOW:
                result = self._apply_action(action)
                applied.append(result)
            elif action.risk == OptimizationRisk.MEDIUM:
                result = self._apply_action(action)
                applied.append(result)
                self._notify_slack(action, result)
            elif action.risk == OptimizationRisk.HIGH:
                if self.approval_required:
                    self._request_approval(action)
                    skipped.append(action)
                else:
                    result = self._apply_action(action)
                    applied.append(result)

        return {
            "cycle_timestamp": datetime.now(timezone.utc).isoformat(),
            "total_discovered": len(actions),
            "applied": len(applied),
            "pending_approval": len(skipped),
            "actions": applied,
        }
```

---

## Kubernetes Resource Rightsizing

```python
import subprocess, json

def _check_kubernetes_rightsizing(self) -> list[OptimizationAction]:
    """Read VPA recommendations and generate rightsizing actions."""
    actions = []

    # Get all VPA objects in data platform namespaces
    result = subprocess.run(
        ["kubectl", "get", "vpa", "-A", "-o", "json"],
        capture_output=True, text=True
    )
    vpas = json.loads(result.stdout).get("items", [])

    for vpa in vpas:
        name = vpa["metadata"]["name"]
        ns = vpa["metadata"]["namespace"]
        recs = vpa.get("status", {}).get("recommendation", {}).get("containerRecommendations", [])

        for rec in recs:
            container = rec["containerName"]
            target_cpu = rec.get("target", {}).get("cpu")
            target_mem = rec.get("target", {}).get("memory")

            if not target_cpu and not target_mem:
                continue

            # Get current requests
            current = _get_current_resource_requests(name, ns, container)
            current_mem_gi = parse_memory_gi(current.get("memory", "0"))
            target_mem_gi = parse_memory_gi(target_mem or "0")

            # Only act if recommendation differs by > 20%
            if current_mem_gi > 0 and abs(target_mem_gi - current_mem_gi) / current_mem_gi > 0.2:
                savings_pct = round((current_mem_gi - target_mem_gi) / current_mem_gi * 100, 0)
                actions.append(OptimizationAction(
                    id=f"vpa-{ns}-{name}-{container}",
                    category="compute",
                    resource=f"{ns}/{name}/{container}",
                    action=f"Update memory request: {current_mem_gi:.1f}Gi → {target_mem_gi:.1f}Gi",
                    expected_saving=f"{savings_pct:+.0f}% memory {'savings' if savings_pct > 0 else 'increase'}",
                    risk=OptimizationRisk.MEDIUM,
                    apply_fn=lambda: _apply_vpa_recommendation(ns, name, container, target_cpu, target_mem),
                ))

    return actions


def _apply_vpa_recommendation(namespace, deployment, container, cpu, memory):
    """Patch deployment with VPA-recommended resource requests."""
    patch = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": container,
                        "resources": {
                            "requests": {
                                k: v for k, v in
                                {"cpu": cpu, "memory": memory}.items()
                                if v is not None
                            }
                        }
                    }]
                }
            }
        }
    }
    subprocess.run([
        "kubectl", "patch", "deployment", deployment,
        "-n", namespace,
        "--patch", json.dumps(patch),
    ])
    return {"status": "patched", "deployment": deployment, "cpu": cpu, "memory": memory}
```

---

## Idle Resource Detection

```python
def _check_idle_resources(self) -> list[OptimizationAction]:
    """Detect idle Kubernetes resources (zero traffic/activity for > 7 days)."""
    actions = []

    # Deployments with 0 CPU usage for > 7 days
    idle_deployments = _prometheus_query("""
        sum by (namespace, pod) (
            avg_over_time(rate(container_cpu_usage_seconds_total[5m])[7d:5m])
        ) == 0
    """)

    for metric in idle_deployments:
        ns = metric["metric"].get("namespace")
        pod_prefix = metric["metric"].get("pod", "").rsplit("-", 2)[0]
        if ns and pod_prefix and ns not in ("kube-system", "monitoring"):
            actions.append(OptimizationAction(
                id=f"idle-{ns}-{pod_prefix}",
                category="compute",
                resource=f"{ns}/{pod_prefix}",
                action="Scale to 0 replicas (zero CPU usage for 7 days)",
                expected_saving="100% compute cost for this deployment",
                risk=OptimizationRisk.HIGH,   # human approval required
                apply_fn=lambda: subprocess.run(
                    ["kubectl", "scale", "deployment", pod_prefix, "-n", ns, "--replicas=0"]
                ),
            ))

    # PVCs not mounted to any pod
    result = subprocess.run(
        ["kubectl", "get", "pvc", "-A", "-o", "json"],
        capture_output=True, text=True
    )
    pvcs = json.loads(result.stdout).get("items", [])

    for pvc in pvcs:
        if pvc.get("status", {}).get("phase") == "Bound":
            # Check if any pod mounts this PVC
            pvc_name = pvc["metadata"]["name"]
            ns = pvc["metadata"]["namespace"]
            capacity = pvc.get("spec", {}).get("resources", {}).get("requests", {}).get("storage", "?")

            if not _pvc_is_mounted(pvc_name, ns):
                actions.append(OptimizationAction(
                    id=f"unmounted-pvc-{ns}-{pvc_name}",
                    category="storage",
                    resource=f"{ns}/{pvc_name}",
                    action=f"Delete unmounted PVC ({capacity})",
                    expected_saving=f"Disk cost for {capacity} storage",
                    risk=OptimizationRisk.HIGH,   # never auto-delete storage
                    apply_fn=lambda: subprocess.run(
                        ["kubectl", "delete", "pvc", pvc_name, "-n", ns]
                    ),
                ))

    return actions
```

---

## Database Maintenance Scheduler

```python
def _check_database_maintenance(self) -> list[OptimizationAction]:
    """Detect tables needing ANALYZE, VACUUM, or compaction."""
    from sqlalchemy import create_engine, text
    actions = []

    # Trino/Iceberg: tables with stale statistics (> 7 days)
    engine = create_engine(TRINO_URI)
    stale_stats = engine.execute(text("""
        SELECT table_schema, table_name,
               last_analyzed,
               TIMESTAMPDIFF(DAY, last_analyzed, NOW()) AS days_since_analyze,
               row_count
        FROM information_schema.tables t
        LEFT JOIN system.metadata.table_statistics s
               ON t.table_name = s.table_name
        WHERE t.table_schema IN ('silver', 'gold')
          AND (s.last_analyzed IS NULL OR s.last_analyzed < NOW() - INTERVAL '7' DAY)
          AND t.row_count > 1000000
        ORDER BY t.row_count DESC
        LIMIT 20
    """)).fetchall()

    for row in stale_stats:
        table = f"{row.table_schema}.{row.table_name}"
        actions.append(OptimizationAction(
            id=f"analyze-{table}",
            category="database",
            resource=table,
            action=f"ANALYZE TABLE (stale stats: {row.days_since_analyze}d ago)",
            expected_saving="Better query planning / reduced full scans",
            risk=OptimizationRisk.LOW,
            apply_fn=lambda t=table: engine.execute(text(f"ANALYZE {t}")),
        ))

    # Iceberg: tables needing compaction (many small files)
    small_file_tables = engine.execute(text("""
        SELECT table_schema || '.' || table_name AS full_table,
               file_count,
               ROUND(total_size / 1e9, 1) AS size_gb,
               ROUND(total_size / file_count / 1e6, 1) AS avg_file_mb
        FROM system.metadata.table_files
        WHERE file_count > 500
          AND total_size / file_count < 32 * 1e6    -- avg file < 32 MB
        ORDER BY file_count DESC LIMIT 10
    """)).fetchall()

    for row in small_file_tables:
        actions.append(OptimizationAction(
            id=f"compact-{row.full_table}",
            category="storage",
            resource=row.full_table,
            action=f"OPTIMIZE (small files: {row.file_count} files, avg {row.avg_file_mb}MB)",
            expected_saving="Faster scans, reduced metadata overhead",
            risk=OptimizationRisk.LOW,
            apply_fn=lambda t=row.full_table: engine.execute(text(f"ALTER TABLE {t} EXECUTE optimize")),
        ))

    return actions
```

---

## LLM Prioritization

```python
def _prioritize_with_llm(self, actions: list[OptimizationAction]) -> list[OptimizationAction]:
    """Use LLM to prioritize optimization actions by impact."""
    if len(actions) <= 5:
        return sorted(actions, key=lambda a: a.risk.value)

    summary = "\n".join(
        f"{i+1}. [{a.risk.value.upper()}] {a.category} — {a.resource}: {a.action} | Benefit: {a.expected_saving}"
        for i, a in enumerate(actions)
    )

    response = self.client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": f"""Rank these platform optimization actions by business impact (highest first).
Return a JSON array of the action numbers in priority order.

Actions:
{summary}

Prioritize: 1) Actions preventing outages or SLA breaches, 2) Cost savings, 3) Performance improvements.
Return ONLY a JSON array like: [3, 1, 5, 2, 4]"""
        }]
    )

    import re, json
    match = re.search(r'\[[\d,\s]+\]', response.content[0].text)
    if match:
        order = json.loads(match.group())
        action_map = {i+1: a for i, a in enumerate(actions)}
        return [action_map[i] for i in order if i in action_map]

    return actions  # fallback: original order
```

---

## Weekly Optimization Report

```python
def generate_weekly_report(applied_actions: list[dict]) -> str:
    by_category = {}
    for a in applied_actions:
        cat = a.get("category", "other")
        by_category.setdefault(cat, []).append(a)

    lines = [
        f"# Platform Optimization Report — Week of {datetime.now().strftime('%Y-%m-%d')}",
        "",
        f"**Total actions applied**: {len(applied_actions)}",
        "",
    ]
    for cat, items in by_category.items():
        lines.append(f"## {cat.title()} ({len(items)} actions)")
        for item in items:
            lines.append(f"- {item.get('resource')}: {item.get('action')}")
        lines.append("")

    return "\n".join(lines)
```

---

## Anti-Patterns

1. **Fully automated high-risk actions** — auto-deleting PVCs or scaling production deployments to 0 without approval can cause data loss; always gate destructive actions on human approval.
2. **Optimizing in peak hours** — running compaction/ANALYZE during business hours competes with production queries; schedule maintenance during off-peak windows.
3. **No rollback plan for resource changes** — VPA-applied resource reductions that cause OOM need a rollback path; record original values before applying.
4. **Missing audit trail** — automated optimizations that are invisible to operators create debugging nightmares; log every action with timestamp, resource, before/after state.
5. **Optimizing symptoms not causes** — repeatedly compacting the same table after each load is a symptom of a poorly configured writer; fix the root cause (writer creates small files).

---

## References

- Kubernetes VPA: `github.com/kubernetes/autoscaler/tree/master/vertical-pod-autoscaler`
- Iceberg OPTIMIZE: `iceberg.apache.org/docs/latest/maintenance/`
- Claude tool use for agents: `platform.claude.com/docs/en/docs/build-with-claude/tool-use`
- Related skills: `[[aiops-capacity-planning-agent]]`, `[[aiops-query-cost-analyzer]]`, `[[de-cost-optimization]]`, `[[dataops-self-healing-platform]]`
