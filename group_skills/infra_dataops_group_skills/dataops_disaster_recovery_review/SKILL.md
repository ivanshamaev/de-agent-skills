---
name: dataops-disaster-recovery-review
description: Disaster recovery review for data platforms — RTO/RPO definitions, DR strategy selection (cold/warm/hot standby), Airflow metadata DB backup and restore (pg_dump/WAL), Kafka topic replication (MirrorMaker2), data lake replication (S3 CRR/GCS Transfer), Kubernetes cluster backup (Velero), runbook for DR failover, DR testing (game day exercises), backup validation, cross-region Terraform, recovery time measurement
---

# Disaster Recovery Review

## When to Use

- Defining RTO/RPO objectives for a data platform
- Reviewing existing backup/restore coverage
- Planning a DR failover procedure
- Running a game day DR test exercise
- Auditing backup completeness before a compliance review

---

## RTO/RPO Objectives

```yaml
# DR objectives per data platform component
dr_objectives:
  airflow_metadata_db:
    rto: 30min     # time to restore Airflow functionality
    rpo: 1h        # max acceptable data loss
    strategy: warm_standby   # PostgreSQL hot standby + daily pg_dump to S3

  data_lake:
    rto: 4h        # time to restore data access
    rpo: 24h       # daily S3 CRR to secondary region
    strategy: warm_standby

  kafka:
    rto: 1h        # time to restore streaming
    rpo: 5min      # MirrorMaker2 replication lag < 5 min
    strategy: active_passive

  trino_catalog:
    rto: 2h
    rpo: 1h
    strategy: cold_standby   # rebuild from Iceberg metadata

  kubernetes_cluster:
    rto: 2h        # restore from Velero backup
    rpo: 4h        # Velero backup every 4h
    strategy: cold_standby
```

---

## Airflow Metadata DB Backup and Restore

### Backup (pg_dump)

```bash
#!/bin/bash
# airflow_db_backup.sh — run daily via cron or Airflow DAG

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="airflow_backup_${TIMESTAMP}.sql.gz"
S3_BUCKET="my-company-dr-backups"
S3_PREFIX="airflow/postgres"

# Dump metadata DB
pg_dump \
  -h $AIRFLOW_DB_HOST \
  -U $AIRFLOW_DB_USER \
  -d airflow \
  --no-owner \
  --no-acl \
  -Fc \
  | gzip \
  | aws s3 cp - "s3://${S3_BUCKET}/${S3_PREFIX}/${BACKUP_FILE}"

# Verify backup
if [ $? -eq 0 ]; then
  echo "Backup succeeded: s3://${S3_BUCKET}/${S3_PREFIX}/${BACKUP_FILE}"
  # Update latest pointer
  echo "${BACKUP_FILE}" | aws s3 cp - "s3://${S3_BUCKET}/${S3_PREFIX}/LATEST"
else
  echo "BACKUP FAILED" >&2
  send_alert "Airflow DB backup failed"
  exit 1
fi

# Lifecycle: keep 30 days of backups
```

### Restore Procedure

```bash
#!/bin/bash
# airflow_db_restore.sh

BACKUP_FILE=$(aws s3 cp s3://${S3_BUCKET}/${S3_PREFIX}/LATEST - 2>/dev/null)
echo "Restoring from: ${BACKUP_FILE}"

# Download and restore
aws s3 cp "s3://${S3_BUCKET}/${S3_PREFIX}/${BACKUP_FILE}" - \
  | gunzip \
  | pg_restore \
    -h $DR_AIRFLOW_DB_HOST \
    -U $AIRFLOW_DB_USER \
    -d airflow \
    --no-owner \
    --no-acl \
    -v

# Validate restore
psql -h $DR_AIRFLOW_DB_HOST -U $AIRFLOW_DB_USER -d airflow -c \
  "SELECT COUNT(*) AS dag_count FROM dag WHERE is_active = TRUE;"

echo "Restore completed. Validate Airflow connectivity before switching traffic."
```

---

## Kafka MirrorMaker2 (Cross-Region Replication)

```yaml
# MirrorMaker2 configuration
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaMirrorMaker2
metadata:
  name: kafka-mm2
  namespace: kafka
spec:
  version: 3.5.0
  replicas: 3
  connectCluster: us-east-1

  clusters:
    - alias: us-east-1            # primary
      bootstrapServers: kafka-primary:9092
    - alias: us-west-2            # DR
      bootstrapServers: kafka-dr:9092
      config:
        ssl.endpoint.identification.algorithm: https
        security.protocol: SSL

  mirrors:
    - sourceCluster: us-east-1
      targetCluster: us-west-2
      sourceConnector:
        config:
          replication.factor: 3
          sync.topic.acls.enabled: "true"
          replication.policy.separator: "."
      heartbeatConnector:
        config:
          heartbeats.topic.replication.factor: 3
      checkpointConnector:
        config:
          checkpoints.topic.replication.factor: 3
          sync.group.offsets.enabled: "true"   # sync consumer offsets
          sync.group.offsets.interval.seconds: "60"

      topicsPattern: "orders.*|customers.*|events.*"
      groupsPattern: "orders-processor|events-consumer"
```

```bash
# Monitor MirrorMaker2 lag
kafka-consumer-groups.sh \
  --bootstrap-server kafka-dr:9092 \
  --group mm2-us-east-1-to-us-west-2 \
  --describe | awk 'NR>1 {print $NF}'  # show lag column
```

---

## S3 Cross-Region Replication (Data Lake DR)

```hcl
resource "aws_s3_bucket_replication_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake_primary.id
  role   = aws_iam_role.s3_replication.arn

  rule {
    id     = "replicate-all-zones"
    status = "Enabled"

    filter {}   # replicate all objects

    destination {
      bucket        = aws_s3_bucket.data_lake_dr.arn
      storage_class = "STANDARD_IA"   # cost-optimize DR copy
    }

    delete_marker_replication {
      status = "Enabled"   # replicate deletes too
    }
  }
}

# Verify replication lag
resource "aws_cloudwatch_metric_alarm" "replication_lag" {
  alarm_name          = "s3-replication-lag-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ReplicationLatency"
  namespace           = "AWS/S3"
  period              = 300
  statistic           = "Maximum"
  threshold           = 300   # 5 min lag
  dimensions = {
    SourceBucket      = aws_s3_bucket.data_lake_primary.id
    DestinationBucket = aws_s3_bucket.data_lake_dr.id
    RuleId            = "replicate-all-zones"
  }
}
```

---

## Kubernetes Cluster Backup (Velero)

```bash
# Install Velero
velero install \
  --provider aws \
  --plugins velero/velero-plugin-for-aws:v1.8.0 \
  --bucket my-company-velero-backups \
  --backup-location-config region=us-east-1 \
  --snapshot-location-config region=us-east-1 \
  --use-node-agent

# Create backup schedule (every 4h)
velero schedule create data-platform \
  --schedule="0 */4 * * *" \
  --include-namespaces airflow,kafka,spark,monitoring \
  --ttl 720h    # keep 30 days

# Check backup status
velero backup get

# Restore from latest backup
velero restore create \
  --from-backup $(velero backup get | tail -1 | awk '{print $1}') \
  --include-namespaces airflow \
  --wait
```

---

## DR Failover Runbook

```markdown
## DR Failover Procedure: Primary Region Unavailable

**RTO target**: 2 hours from decision to declare failover

### Pre-conditions
- [ ] Confirm primary region is unavailable (not a partial outage)
- [ ] Get approval from engineering manager
- [ ] Notify all consumer teams (email + Slack)

### Failover Steps

1. **DNS failover** (T+0 to T+5 min)
   - Update Route53/Cloud DNS to DR endpoints
   - `aws route53 change-resource-record-sets` (automated via runbook script)

2. **Airflow metadata DB** (T+5 to T+20 min)
   - Promote PostgreSQL standby: `pg_ctl promote -D /var/lib/postgresql/data`
   - Or restore from latest S3 backup to DR RDS
   - Validate: `psql $DR_DB -c "SELECT COUNT(*) FROM dag;"`

3. **Kafka failover** (T+5 to T+15 min)
   - MirrorMaker2 already replicating; redirect producers/consumers to DR brokers
   - Check offset sync: `kafka-consumer-groups.sh --bootstrap-server $DR_KAFKA ...`

4. **Data lake access** (T+5 to T+10 min)
   - S3 CRR already active; update Trino catalog to point to DR bucket
   - `ALTER TABLE gold.fact_orders SET LOCATION 's3://data-lake-dr/gold/fact_orders'`

5. **Kubernetes workloads** (T+15 to T+90 min)
   - Restore Velero backup to DR cluster
   - Update ArgoCD target cluster
   - Validate Airflow scheduler is running and healthy

6. **Validation** (T+90 to T+120 min)
   - Run smoke test DAG
   - Verify data freshness: last partition date
   - Confirm Grafana dashboards show metrics from DR cluster
```

---

## DR Test Game Day

```bash
#!/bin/bash
# dr_game_day.sh — simulate DR scenario in non-prod

echo "=== DR Game Day: $(date) ==="
echo "Scenario: Primary database failure"

# 1. Take snapshot before test
pg_dump -h staging-db -U airflow -d airflow -Fc > /tmp/pre-dr-backup.dump

# 2. Simulate failure: stop primary DB
kubectl scale deployment airflow-postgres --replicas=0 -n airflow-staging

# 3. Start timer
START=$(date +%s)

# 4. Restore from S3 backup
BACKUP=$(aws s3 ls s3://my-company-dr-backups/airflow/ | tail -1 | awk '{print $4}')
aws s3 cp "s3://my-company-dr-backups/airflow/$BACKUP" - \
  | gunzip | pg_restore -h dr-db-staging ...

# 5. Measure RTO
END=$(date +%s)
RTO=$(( (END - START) / 60 ))
echo "RTO achieved: ${RTO} minutes (target: 30 minutes)"

# 6. Validate
psql -h dr-db-staging -c "SELECT COUNT(*) FROM task_instance WHERE state='success';"
```

---

## DR Checklist

```
Backup Coverage:
[ ] Airflow metadata DB: daily pg_dump to S3, retention 30d
[ ] Data lake: S3 CRR to secondary region (< 5 min lag)
[ ] Kafka topics: MirrorMaker2 replication (< 5 min lag)
[ ] Kubernetes cluster: Velero backup every 4h, retention 30d
[ ] dbt manifest.json: versioned in S3

Recovery Readiness:
[ ] Restore procedures documented and tested quarterly
[ ] DR infrastructure pre-provisioned (warm standby)
[ ] DNS failover automated (< 5 min)
[ ] Consumer team notification template ready
[ ] DR game day conducted within last 6 months

RTO/RPO Validation:
[ ] Actual recovery time measured in last game day
[ ] Backup integrity verified weekly (restore to test environment)
[ ] Replication lag monitored and alerted (> 10 min)
```

---

## Anti-Patterns

1. **Backups never tested** — "we have backups" without "we have restored from backups" is false security; test restore quarterly.
2. **RTO/RPO not defined** — "we'll recover as fast as possible" is not an SLA; define numbers and design to meet them.
3. **S3 CRR without delete marker replication** — deleting a file in primary doesn't delete from DR; accidentally losing DR data on next restore.
4. **Warm standby not kept in sync** — a 6-month-old standby cluster requires 6 months of catch-up; keep DR infrastructure current with automated sync.
5. **No consumer notification plan** — consumers make business decisions based on stale data during recovery; have a notification template ready and send immediately.

---

## References

- Velero: `velero.io/docs/`
- Kafka MirrorMaker2: `kafka.apache.org/documentation/#georeplication`
- S3 CRR: `docs.aws.amazon.com/AmazonS3/latest/userguide/replication.html`
- PostgreSQL PITR: `postgresql.org/docs/current/continuous-archiving.html`
- Related skills: `[[dataops-postmortem-generator]]`, `[[dataops-sla-monitoring]]`, `[[infra-kubernetes-storage-review]]`
