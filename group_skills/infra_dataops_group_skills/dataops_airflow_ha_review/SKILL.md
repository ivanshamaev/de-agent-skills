---
name: dataops-airflow-ha-review
description: Airflow high availability review — multi-scheduler setup (HA schedulers with PostgreSQL row-level locking), CeleryExecutor worker pool sizing, KubernetesExecutor pod limits, metadata DB HA (PostgreSQL HA with PgBouncer connection pooling), DAG sync strategy (git-sync sidecar/S3/GitDagBundle), log storage (S3/GCS), webserver multi-replica, triggerer HA, rolling upgrades without downtime, Celery Flower monitoring, heartbeat alerts
---

# Airflow High Availability Review

## When to Use

- Setting up Airflow for a production data platform (multi-team, 100+ DAGs)
- Diagnosing scheduler hangs or single-point-of-failure issues
- Planning a zero-downtime Airflow version upgrade
- Reviewing Airflow HA before a critical business period
- Evaluating CeleryExecutor vs KubernetesExecutor for HA requirements

---

## Multi-Scheduler High Availability

Airflow 2.0+ supports multiple concurrent schedulers using PostgreSQL row-level locking.

```yaml
# Airflow Helm chart values.yaml
scheduler:
  replicas: 2            # 2 schedulers for HA; one is active, one is hot standby
  resources:
    requests:
      cpu: 1
      memory: 2Gi
    limits:
      cpu: 4
      memory: 8Gi

  livenessProbe:
    initialDelaySeconds: 60
    timeoutSeconds: 10
    failureThreshold: 5
    periodSeconds: 30
    command:
      - python
      - -Wignore
      - -c
      - |
        import os, pendulum
        from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
        ...

  # Scheduler heartbeat timeout (default 5 min — increase for high-load clusters)
  env:
    - name: AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_THRESHOLD
      value: "30"     # seconds between heartbeats
```

```ini
# airflow.cfg
[scheduler]
num_runs = -1                          # run indefinitely
scheduler_heartbeat_sec = 5            # write heartbeat to DB every 5s
scheduler_health_check_threshold = 30  # alert if heartbeat > 30s ago
```

---

## Metadata Database HA

```yaml
# PostgreSQL HA with PgBouncer (connection pooling)
# Recommended: AWS RDS Multi-AZ or CloudSQL HA

# PgBouncer values (Bitnami chart)
pgbouncer:
  enabled: true
  maxClientConn: 1000
  defaultPoolSize: 50
  poolMode: transaction

# Airflow DB connection (via PgBouncer)
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:${DB_PASSWORD}@pgbouncer:5432/airflow
AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE: 5
AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW: 10
AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_PRE_PING: "true"
```

```bash
# Check DB connection pool usage
psql $AIRFLOW_DB -c "SELECT count(*), state FROM pg_stat_activity WHERE datname='airflow' GROUP BY state"

# Monitor DB size growth
psql $AIRFLOW_DB -c "SELECT pg_size_pretty(pg_database_size('airflow'))"
```

---

## DAG Sync Strategy (git-sync)

```yaml
# Airflow Helm chart: git-sync sidecar
dags:
  gitSync:
    enabled: true
    repo: https://github.com/my-org/airflow-dags
    branch: main
    rev: HEAD
    depth: 1
    maxFailures: 3
    subPath: dags/
    wait: 60              # sync interval seconds
    containerName: git-sync
    uid: 65533
    securityContext:
      runAsUser: 65533
      runAsGroup: 65533

  # For private repos: use SSH key
  gitSync:
    sshKeySecret: airflow-git-ssh-key
    knownHosts: |
      github.com ssh-rsa AAAA...

# Alternative: S3 sync (for large DAG repos)
dags:
  persistence:
    enabled: true
  initContainers:
    - name: s3-sync
      image: amazon/aws-cli:latest
      command: ["aws", "s3", "sync", "s3://my-airflow-dags/", "/opt/airflow/dags/"]
      env:
        - name: AWS_ROLE_ARN
          value: arn:aws:iam::123456789:role/AirflowDagReader
```

---

## Remote Log Storage

```yaml
# Store task logs in S3 (not local pods)
env:
  - name: AIRFLOW__LOGGING__REMOTE_LOGGING
    value: "True"
  - name: AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER
    value: "s3://my-airflow-logs/task-logs"
  - name: AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID
    value: "aws_default"
  - name: AIRFLOW__LOGGING__ENCRYPT_S3_LOGS
    value: "True"

# GCS alternative
env:
  - name: AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER
    value: "gs://my-airflow-logs/task-logs"
  - name: AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID
    value: "google_cloud_default"
```

---

## CeleryExecutor Worker Pool

```yaml
# Helm chart: Celery workers
workers:
  replicas: 3                   # minimum HA workers
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 20
    targetCPUUtilizationPercentage: 70

  resources:
    requests:
      cpu: 500m
      memory: 1Gi
    limits:
      cpu: 4
      memory: 8Gi

  podDisruptionBudget:
    enabled: true
    config:
      maxUnavailable: 1          # never take down all workers simultaneously

# Celery broker: Redis with Sentinel for HA
redis:
  enabled: true
  sentinel:
    enabled: true
    masterSet: mymaster
    replicas: 3
```

```ini
# airflow.cfg — Celery
[celery]
worker_concurrency = 16          # tasks per worker process
worker_autoscale = 16,4          # max_concurrency,min_concurrency
worker_prefetch_multiplier = 1   # don't prefetch tasks (prevents long queue delays)
task_acks_late = True            # ack after execution (prevents lost tasks on worker crash)
```

---

## Triggerer HA (for deferrable operators)

```yaml
triggerer:
  replicas: 2                    # HA triggerers (Airflow 2.7+)
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
  env:
    - name: AIRFLOW__TRIGGERER__DEFAULT_CAPACITY
      value: "1000"              # max deferred tasks per triggerer
```

---

## Webserver HA

```yaml
webserver:
  replicas: 2                    # HA webserver
  resources:
    requests:
      cpu: 500m
      memory: 1Gi

  # Session backend: Redis (required for multi-replica webserver)
  env:
    - name: AIRFLOW__WEBSERVER__SECRET_KEY
      valueFrom:
        secretKeyRef:
          name: airflow-webserver-secret
          key: webserver-secret-key

# Session storage (shared across replicas)
AIRFLOW__SESSION_BACKEND__SESSION_BACKEND: redis
AIRFLOW__SESSION_BACKEND__SESSION_REDIS_URL: redis://redis:6379/3
```

---

## Zero-Downtime Upgrade Procedure

```bash
# KubernetesExecutor upgrade procedure
# 1. Upgrade Airflow image in Helm values
# 2. Apply Helm upgrade (rolling restart)
helm upgrade airflow apache-airflow/airflow \
  --version 1.13.0 \
  --values values.yaml \
  --set images.airflow.tag=2.8.0 \
  --atomic \
  --timeout 10m

# 3. Run DB migration (if needed)
kubectl exec -it airflow-scheduler-<pod> -- airflow db upgrade

# CeleryExecutor upgrade procedure
# 1. Put workers in offline mode (stop accepting new tasks)
kubectl exec -it airflow-worker-<pod> -- airflow celery stop

# 2. Wait for running tasks to complete
kubectl exec -it airflow-worker-<pod> -- celery -A airflow.executors.celery_executor inspect active

# 3. Upgrade worker pods
kubectl rollout restart deployment/airflow-worker
```

---

## Scheduler Heartbeat Monitoring

```bash
# Check scheduler heartbeat (last alive time)
psql $AIRFLOW_DB -c "
  SELECT job_type, hostname, latest_heartbeat, state
  FROM job
  WHERE job_type = 'SchedulerJob'
  ORDER BY latest_heartbeat DESC
  LIMIT 5;
"

# Alert if scheduler hasn't heartbeated in 2 minutes
psql $AIRFLOW_DB -c "
  SELECT CASE
    WHEN MAX(latest_heartbeat) < NOW() - INTERVAL '2 minutes'
    THEN 'ALERT: Scheduler heartbeat stale!'
    ELSE 'OK'
  END AS status
  FROM job
  WHERE job_type = 'SchedulerJob'
"
```

```yaml
# Prometheus alert rule
- alert: AirflowSchedulerHeartbeatStale
  expr: time() - airflow_scheduler_heartbeat > 120
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "Airflow scheduler heartbeat stale"
    description: "No scheduler heartbeat for > 2 minutes"
```

---

## HA Checklist

```
[ ] 2+ scheduler replicas (PostgreSQL required for HA mode)
[ ] Metadata DB is PostgreSQL (not SQLite) with Multi-AZ HA
[ ] PgBouncer connection pooling in front of metadata DB
[ ] Remote log storage (S3/GCS) — not local pod filesystem
[ ] git-sync for DAG distribution (not NFS/local volume)
[ ] 2+ webserver replicas with shared session backend (Redis)
[ ] 2+ triggerer replicas for deferrable operators
[ ] Worker PodDisruptionBudget set (maxUnavailable: 1)
[ ] Worker autoscaling configured
[ ] Scheduler heartbeat alert configured (< 2 min threshold)
[ ] Zero-downtime upgrade procedure documented and tested
```

---

## Anti-Patterns

1. **Single scheduler replica** — scheduler crash = all DAGs stop running; run at least 2 schedulers.
2. **Local filesystem for logs** — pod replacement loses all task logs; use S3/GCS remote logging.
3. **SQLite metadata database** — no concurrent access, data loss on restart; use PostgreSQL with Multi-AZ.
4. **NFS for DAG distribution** — NFS latency causes DAG parse timeouts at scale; use git-sync sidecar.
5. **No PodDisruptionBudget for workers** — Kubernetes node drain can kill all workers simultaneously; set maxUnavailable: 1.
6. **`task_acks_late = False` (default)** — if a Celery worker crashes mid-task, the task is lost; set `task_acks_late = True`.

---

## References

- Airflow HA scheduling: `airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/production-deployment.html`
- Airflow Helm chart: `airflow.apache.org/docs/helm-chart/stable/`
- git-sync: `github.com/kubernetes/git-sync`
- Related skills: `[[dataops-airflow-production-readiness]]`, `[[dataops-airflow-observability]]`, `[[infra-kubernetes-cluster-health]]`
