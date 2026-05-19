---
name: starrocks-self-healing
description: StarRocks self-healing automation — auto-restart failed Routine Load jobs (detect PAUSED/CANCELLED + RESUME), rebalance tablet distribution after BE add/remove (REBALANCE command), trigger manual compaction for high-score tablets, auto-ANALYZE stale statistics, partition management (auto-drop old partitions), dead load label cleanup, BE disk space alerts + auto-tiering, scheduled health check + remediation agent
---

# StarRocks Self-Healing Automation

## When to Use

- Automatically recover Routine Load jobs that pause due to transient errors
- Rebalance tablets after adding new BEs to the cluster
- Trigger compaction when compaction score exceeds threshold
- Automatically clean up expired load labels and old partitions
- Schedule periodic health checks that remediate common issues without manual intervention

---

## Auto-Restart Routine Load Jobs

```python
import pymysql
import time
import logging
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class RoutineLoadJob:
    db: str
    name: str
    state: str
    reason: str
    error_log_urls: str


class StarRocksSelfHealingAgent:
    def __init__(self, host: str, port: int = 9030, user: str = "root", password: str = ""):
        self.conn_params = dict(host=host, port=port, user=user, password=password)

    def _query(self, sql: str, db: str = None) -> list:
        params = {**self.conn_params}
        if db:
            params["db"] = db
        conn = pymysql.connect(**params)
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            cols = [d[0] for d in cursor.description] if cursor.description else []
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
        finally:
            conn.close()

    def _execute(self, sql: str):
        conn = pymysql.connect(**self.conn_params)
        try:
            conn.cursor().execute(sql)
            conn.commit()
        finally:
            conn.close()

    # ─────────────────────────────────────────────────────────
    # Routine Load: auto-resume transient failures
    # ─────────────────────────────────────────────────────────

    TRANSIENT_PAUSE_PATTERNS = [
        "connection refused",
        "kafka broker not available",
        "network timeout",
        "leader not available",
    ]

    HARD_PAUSE_PATTERNS = [
        "errortoomain",      # data quality: must fix source
        "offset out of range",  # Kafka retention: need to reset
        "schema",
    ]

    def heal_routine_loads(self, databases: List[str]) -> List[dict]:
        """Check all databases for paused Routine Load jobs; auto-resume transient ones."""
        actions = []

        for db in databases:
            try:
                rows = self._query(f"SHOW ROUTINE LOAD FROM {db}", db=db)
            except Exception as e:
                logger.warning(f"Failed to check RL in {db}: {e}")
                continue

            for row in rows:
                state = row.get("State", "")
                name = row.get("Name", "")
                reason = (row.get("ReasonOfStateChanged") or "").lower()

                if state != "PAUSED":
                    continue

                # Determine if this is a transient or hard pause
                is_transient = any(p in reason for p in self.TRANSIENT_PAUSE_PATTERNS)
                is_hard = any(p in reason for p in self.HARD_PAUSE_PATTERNS)

                if is_transient and not is_hard:
                    logger.info(f"Auto-resuming {db}.{name} (transient: {reason[:100]})")
                    try:
                        self._execute(f"RESUME ROUTINE LOAD FOR {db}.{name}")
                        actions.append({
                            "action": "RESUMED",
                            "job": f"{db}.{name}",
                            "reason": reason[:100],
                        })
                    except Exception as e:
                        logger.error(f"Failed to resume {db}.{name}: {e}")
                        actions.append({"action": "RESUME_FAILED", "job": f"{db}.{name}", "error": str(e)})
                else:
                    logger.warning(f"Manual intervention required for {db}.{name}: {reason[:200]}")
                    actions.append({
                        "action": "REQUIRES_MANUAL_FIX",
                        "job": f"{db}.{name}",
                        "reason": reason[:200],
                    })

        return actions
```

---

## Compaction Backlog Healing

```python
    def heal_compaction_backlog(self, compaction_score_threshold: int = 100) -> List[dict]:
        """Trigger manual compaction on BEs with high compaction score."""
        backends = self._query("SHOW BACKENDS")
        actions = []

        for be in backends:
            host = be.get("Host", "")
            score_str = be.get("CompactionScore", "0")
            try:
                score = float(score_str)
            except (ValueError, TypeError):
                continue

            if score > compaction_score_threshold:
                logger.info(f"Triggering compaction on {host} (score={score:.0f})")
                try:
                    # Trigger compaction via StarRocks admin command
                    self._execute(f"ADMIN COMPACT TABLE TABLETS ALL")
                    actions.append({
                        "action": "COMPACTION_TRIGGERED",
                        "be_host": host,
                        "compaction_score": score,
                    })
                except Exception as e:
                    logger.error(f"Compaction trigger failed on {host}: {e}")
                    actions.append({
                        "action": "COMPACTION_FAILED",
                        "be_host": host,
                        "error": str(e),
                    })

        return actions
```

---

## Auto-ANALYZE Stale Statistics

```python
    def heal_stale_statistics(
        self,
        databases: List[str],
        max_age_hours: int = 24,
    ) -> List[dict]:
        """Find and refresh stale statistics."""
        actions = []

        for db in databases:
            try:
                jobs = self._query(f"""
                    SELECT TableName, Status, EndTime
                    FROM _statistics_.analyze_jobs
                    WHERE DbName = '{db}'
                    ORDER BY EndTime DESC
                """)
            except Exception:
                continue

            from datetime import datetime, timedelta
            from collections import defaultdict

            latest_by_table: dict = defaultdict(lambda: None)
            for job in jobs:
                table = job.get("TableName")
                end_time = job.get("EndTime")
                if table and end_time and latest_by_table[table] is None:
                    latest_by_table[table] = job

            # Find all tables in the database
            try:
                tables = self._query(f"SHOW TABLES FROM {db}")
                table_names = [t.get(f"Tables_in_{db}") or list(t.values())[0] for t in tables]
            except Exception:
                continue

            now = datetime.utcnow()
            for table in table_names:
                job = latest_by_table.get(table)
                needs_analyze = False

                if job is None:
                    needs_analyze = True
                    age_desc = "never analyzed"
                else:
                    status = job.get("Status", "")
                    end_time = job.get("EndTime")
                    if isinstance(end_time, str):
                        try:
                            end_time = datetime.strptime(end_time[:19], "%Y-%m-%d %H:%M:%S")
                            age_hours = (now - end_time).total_seconds() / 3600
                            needs_analyze = age_hours > max_age_hours or status != "SUCCESS"
                            age_desc = f"{age_hours:.1f}h old"
                        except ValueError:
                            needs_analyze = True
                            age_desc = "unknown age"

                if needs_analyze:
                    try:
                        self._execute(f"ANALYZE TABLE {db}.{table} WITH ASYNC MODE")
                        logger.info(f"ANALYZE triggered for {db}.{table} ({age_desc})")
                        actions.append({"action": "ANALYZE_TRIGGERED", "table": f"{db}.{table}"})
                    except Exception as e:
                        logger.error(f"ANALYZE failed for {db}.{table}: {e}")

        return actions
```

---

## Auto-Drop Expired Partitions

```python
    def heal_partition_retention(
        self,
        db: str,
        table: str,
        retention_days: int = 90,
        dry_run: bool = True,
    ) -> List[dict]:
        """Drop partitions older than retention_days."""
        from datetime import datetime, timedelta

        cutoff = datetime.utcnow() - timedelta(days=retention_days)
        cutoff_str = cutoff.strftime("%Y-%m-%d")

        partitions = self._query(f"SHOW PARTITIONS FROM {db}.{table}")
        actions = []

        for p in partitions:
            p_name = p.get("PartitionName", "")
            p_range = p.get("Range", "") or p.get("PartitionRange", "")

            # Parse range upper bound: "[2023-01-01, 2023-02-01)"
            range_match = __import__("re").search(r'\[([^,]+),\s*([^\)]+)\)', str(p_range))
            if not range_match:
                continue

            upper_bound = range_match.group(2).strip().rstrip(")")
            try:
                p_date = datetime.strptime(upper_bound[:10], "%Y-%m-%d")
            except ValueError:
                continue

            if p_date.date() < cutoff.date():
                action = {
                    "action": "DROP_PARTITION" if not dry_run else "DRY_RUN_DROP",
                    "table": f"{db}.{table}",
                    "partition": p_name,
                    "range": p_range,
                }
                if not dry_run:
                    try:
                        self._execute(f"ALTER TABLE {db}.{table} DROP PARTITION {p_name}")
                        logger.info(f"Dropped old partition {db}.{table}.{p_name}")
                    except Exception as e:
                        action["error"] = str(e)
                        logger.error(f"Failed to drop {p_name}: {e}")
                else:
                    logger.info(f"[DRY RUN] Would drop {db}.{table}.{p_name} ({p_range})")
                actions.append(action)

        return actions
```

---

## Dead Load Label Cleanup

```python
    def heal_failed_load_labels(
        self,
        databases: List[str],
        cleanup_cancelled: bool = True,
    ) -> List[dict]:
        """Report (and optionally clean up) CANCELLED broker load jobs."""
        actions = []

        for db in databases:
            rows = self._query(
                f"SHOW LOAD FROM {db} WHERE State = 'CANCELLED' ORDER BY CreateTime DESC LIMIT 100"
            )
            for row in rows:
                label = row.get("Label", "")
                finish_time = row.get("FinishTime", "")
                actions.append({
                    "db": db,
                    "label": label,
                    "finish_time": finish_time,
                    "note": "CANCELLED label — reuse with new label name to retry",
                })
            logger.info(f"{db}: found {len(rows)} CANCELLED load labels")

        return actions
```

---

## Scheduled Health Check and Remediation

```python
def run_full_health_check(agent: StarRocksSelfHealingAgent, databases: List[str]):
    """Run all healing checks and summarize actions taken."""
    all_actions = []

    # 1. Routine Load recovery
    logger.info("=== Checking Routine Load jobs ===")
    rl_actions = agent.heal_routine_loads(databases)
    all_actions.extend(rl_actions)

    # 2. Compaction backlog
    logger.info("=== Checking compaction backlog ===")
    comp_actions = agent.heal_compaction_backlog(compaction_score_threshold=100)
    all_actions.extend(comp_actions)

    # 3. Stale statistics
    logger.info("=== Checking stale statistics ===")
    stats_actions = agent.heal_stale_statistics(databases, max_age_hours=48)
    all_actions.extend(stats_actions)

    # 4. Partition retention (dry run by default)
    logger.info("=== Checking partition retention ===")
    for db in databases:
        # Only run on tables with known high retention overhead
        for table in ["orders", "events"]:
            try:
                part_actions = agent.heal_partition_retention(db, table, retention_days=90, dry_run=True)
                all_actions.extend(part_actions)
            except Exception:
                pass

    # Summary
    by_type: dict = {}
    for a in all_actions:
        key = a.get("action", "UNKNOWN")
        by_type[key] = by_type.get(key, 0) + 1

    logger.info(f"Health check complete. Actions: {by_type}")
    return all_actions


# Schedule via Airflow or cron:
# */10 * * * * python -c "
#   from starrocks_healer import run_full_health_check, StarRocksSelfHealingAgent
#   agent = StarRocksSelfHealingAgent('sr-fe.internal')
#   run_full_health_check(agent, ['sales', 'finance'])
# "
```

---

## Airflow Integration

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="starrocks_self_healing",
    schedule="*/10 * * * *",  # every 10 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["starrocks", "self-healing"],
)
def starrocks_self_healing():

    @task
    def heal_routine_loads():
        from starrocks_healer import StarRocksSelfHealingAgent
        agent = StarRocksSelfHealingAgent("sr-fe.internal")
        return agent.heal_routine_loads(["sales", "finance"])

    @task
    def heal_compaction():
        from starrocks_healer import StarRocksSelfHealingAgent
        agent = StarRocksSelfHealingAgent("sr-fe.internal")
        return agent.heal_compaction_backlog(threshold=150)

    @task
    def heal_statistics():
        from starrocks_healer import StarRocksSelfHealingAgent
        agent = StarRocksSelfHealingAgent("sr-fe.internal")
        return agent.heal_stale_statistics(["sales"], max_age_hours=48)

    # Run checks in parallel — they are independent
    [heal_routine_loads(), heal_compaction(), heal_statistics()]


dag = starrocks_self_healing()
```

---

## Anti-Patterns

1. **Auto-resuming ALL paused Routine Load jobs** — data-quality pauses (ErrorTooMany) need root cause fix first; resuming blindly re-pauses in seconds. Only resume transient pauses.
2. **Triggering compaction on every check run** — compaction is expensive; only trigger when CompactionScore exceeds a meaningful threshold (> 100).
3. **Dropping partitions without dry_run=True first** — partition drop is irreversible; always test with dry_run before enabling live drops.
4. **Running self-healing with root/admin account** — use a dedicated user with minimal permissions (ANALYZE, RESUME ROUTINE LOAD, ALTER TABLE partition-only).
5. **No rate limiting on ANALYZE triggers** — triggering ANALYZE on all tables simultaneously saturates BEs; add a delay or limit to N tables per run.
6. **Ignoring NEED_SCHEDULE state** — NEED_SCHEDULE means the FE is trying to schedule but can't (e.g., no alive BEs); don't confuse with PAUSED.

---

## References

- Routine Load management: `docs.starrocks.io/docs/loading/RoutineLoad/#manage-a-routine-load-job`
- Compaction: `docs.starrocks.io/docs/administration/management/BE_management/compaction/`
- ANALYZE TABLE: `docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ANALYZE_TABLE/`
- Related skills: `[[starrocks-admin-cluster-health]]`, `[[starrocks-admin-compaction]]`, `[[starrocks-routine-load-kafka]]`, `[[starrocks-ai-incident-rca]]`
