---
name: kubernetes-data-platform
description: Data platform on Kubernetes — Spark-on-K8s (spark-submit, pod templates, dynamic allocation, RBAC), Airflow on K8s (Helm chart, KubernetesExecutor, KubernetesPodOperator, git-sync, DAG storage), resource management, namespaces, autoscaling
---

# Kubernetes Data Platform

## When to Use

Activate this skill when the task involves:
- Running Spark jobs on Kubernetes (`--master k8s://`)
- Deploying Apache Airflow with the official Helm chart
- Configuring KubernetesExecutor or KubernetesPodOperator
- Designing RBAC, namespaces, and resource quotas for data workloads
- Setting up git-sync for DAG deployment
- Troubleshooting pod scheduling, OOM kills, or image pull failures
- Dynamic allocation for Spark executors on K8s

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Kubernetes Cluster                                             │
│                                                                 │
│  ┌──────────────────┐   ┌──────────────────────────────────┐  │
│  │  airflow ns       │   │  spark-jobs ns                   │  │
│  │  ┌────────────┐  │   │  ┌─────────┐  ┌──────────────┐  │  │
│  │  │ Scheduler  │──┼───┼─►│ Driver  │  │ Executor (×N)│  │  │
│  │  │ Webserver  │  │   │  │  Pod    │  │    Pods      │  │  │
│  │  │ Workers    │  │   │  └─────────┘  └──────────────┘  │  │
│  │  │ (K8s pods) │  │   │                                  │  │
│  │  └────────────┘  │   │  ServiceAccount: spark           │  │
│  │  PVC: logs, dags │   │  Role: create/delete pods        │  │
│  └──────────────────┘   └──────────────────────────────────┘  │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Shared Infrastructure                                   │  │
│  │  • MinIO / S3 (checkpoints, event logs, jars)            │  │
│  │  • Kafka                                                 │  │
│  │  • PostgreSQL (Airflow metastore)                        │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Spark on Kubernetes

### RBAC Setup

```bash
# Create namespace and service account
kubectl create namespace spark-jobs

kubectl create serviceaccount spark \
  --namespace spark-jobs

# Bind edit role — allows Spark driver to create/delete executor pods
kubectl create clusterrolebinding spark-role \
  --clusterrole=edit \
  --serviceaccount=spark-jobs:spark \
  --namespace=spark-jobs
```

Minimum required permissions for the driver:
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-driver-role
  namespace: spark-jobs
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "configmaps"]
    verbs: ["create", "delete", "get", "list", "watch", "patch"]
```

### Building the Spark Image

```dockerfile
# Dockerfile — extend official image, add Python dependencies and JARs
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

# Add JARs (iceberg, delta, hadoop-aws, openlineage)
COPY jars/ /opt/spark/jars/

# Add Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER spark
```

```bash
# Build and push
docker build -t registry.local/spark:3.5.1-custom .
docker push registry.local/spark:3.5.1-custom
```

Or use the official build tool:
```bash
./bin/docker-image-tool.sh \
  -r registry.local \
  -t 3.5.1-custom \
  -p kubernetes/dockerfiles/spark/bindings/python/Dockerfile \
  build push
```

### spark-submit — Cluster Mode

```bash
spark-submit \
  --master k8s://https://$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' | sed 's|https://||') \
  --deploy-mode cluster \
  --name silver-transform \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=registry.local/spark:3.5.1-custom \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  \
  --conf spark.driver.cores=2 \
  --conf spark.driver.memory=4g \
  --conf spark.driver.memoryOverhead=1g \
  --conf spark.executor.instances=4 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=8g \
  --conf spark.executor.memoryOverhead=2g \
  \
  --conf spark.kubernetes.driver.podTemplateFile=s3a://data-lake/k8s/driver-template.yaml \
  --conf spark.kubernetes.executor.podTemplateFile=s3a://data-lake/k8s/executor-template.yaml \
  \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=s3a://data-lake/spark-history \
  \
  local:///opt/spark/jobs/silver_transform.py
```

`local:///` means the file is baked into the container image. Use `s3a://` or `hdfs://` for dynamically-uploaded scripts.

### Pod Templates

```yaml
# driver-template.yaml
apiVersion: v1
kind: Pod
spec:
  nodeSelector:
    node.kubernetes.io/role: data-driver
  tolerations:
    - key: dedicated
      operator: Equal
      value: spark
      effect: NoSchedule
  volumes:
    - name: spark-local-dir
      emptyDir: {}
  containers:
    - name: spark
      volumeMounts:
        - mountPath: /tmp/spark-local
          name: spark-local-dir
      resources:
        requests:
          cpu: "2"
          memory: "5Gi"
        limits:
          cpu: "4"
          memory: "6Gi"
      env:
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: s3-credentials
              key: access-key
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: s3-credentials
              key: secret-key
```

```yaml
# executor-template.yaml
apiVersion: v1
kind: Pod
spec:
  nodeSelector:
    node.kubernetes.io/role: data-worker
  tolerations:
    - key: dedicated
      operator: Equal
      value: spark
      effect: NoSchedule
  volumes:
    - name: spark-local-dir
      emptyDir:
        sizeLimit: 200Gi
  containers:
    - name: spark
      volumeMounts:
        - mountPath: /tmp/spark-local
          name: spark-local-dir
      env:
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: s3-credentials
              key: access-key
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: s3-credentials
              key: secret-key
```

### Dynamic Allocation

```bash
spark-submit \
  --master k8s://https://k8s-api:6443 \
  --deploy-mode cluster \
  --name streaming-job \
  \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=20 \
  --conf spark.dynamicAllocation.initialExecutors=2 \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.dynamicAllocation.shuffleTracking.timeout=600s \
  --conf spark.dynamicAllocation.executorIdleTimeout=120s \
  --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=600s \
  \
  --conf spark.kubernetes.allocation.batch.size=5 \
  --conf spark.kubernetes.allocation.batch.delay=1s \
  \
  local:///opt/spark/jobs/streaming.py
```

Dynamic allocation requires either an external shuffle service or `shuffleTracking.enabled=true`.

### Spark History Server

```yaml
# spark-history-server.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-history-server
  namespace: spark-jobs
spec:
  replicas: 1
  selector:
    matchLabels:
      app: spark-history-server
  template:
    metadata:
      labels:
        app: spark-history-server
    spec:
      containers:
        - name: spark-history-server
          image: apache/spark:3.5.1-scala2.12-java17-ubuntu
          command:
            - /opt/spark/sbin/start-history-server.sh
          env:
            - name: SPARK_HISTORY_OPTS
              value: >-
                -Dspark.history.fs.logDirectory=s3a://data-lake/spark-history
                -Dspark.history.ui.port=18080
                -Dspark.hadoop.fs.s3a.endpoint=http://minio:9000
                -Dspark.hadoop.fs.s3a.access.key=minioadmin
                -Dspark.hadoop.fs.s3a.secret.key=minioadmin
                -Dspark.hadoop.fs.s3a.path.style.access=true
          ports:
            - containerPort: 18080
---
apiVersion: v1
kind: Service
metadata:
  name: spark-history-server
  namespace: spark-jobs
spec:
  selector:
    app: spark-history-server
  ports:
    - port: 18080
      targetPort: 18080
```

---

## Airflow on Kubernetes

### Helm Chart Deployment

```bash
# Add Airflow Helm repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Create namespace and secrets
kubectl create namespace airflow

kubectl create secret generic airflow-db-credentials \
  --from-literal=connection=postgresql://airflow:airflow@postgres:5432/airflow \
  --namespace airflow

kubectl create secret generic airflow-fernet-key \
  --from-literal=fernet-key=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())") \
  --namespace airflow

# Install
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --values values.yaml \
  --version 1.15.0
```

### values.yaml — Production KubernetesExecutor

```yaml
# values.yaml
executor: KubernetesExecutor

# Images
images:
  airflow:
    repository: registry.local/airflow
    tag: 2.9.0-custom
    pullPolicy: IfNotPresent

# Airflow configuration overrides
config:
  core:
    dags_are_paused_at_creation: "True"
    max_active_runs_per_dag: "5"
    max_active_tasks_per_dag: "16"
  kubernetes_executor:
    namespace: airflow
    worker_pods_creation_batch_size: "8"
    delete_worker_pods: "True"
    delete_worker_pods_on_failure: "False"
  logging:
    remote_logging: "True"
    remote_base_log_folder: "s3://data-lake/airflow-logs"
    remote_log_conn_id: "aws_default"
    encrypt_s3_logs: "False"

# Scheduler
scheduler:
  replicas: 2
  resources:
    requests:
      cpu: "500m"
      memory: "1Gi"
    limits:
      cpu: "2"
      memory: "4Gi"

# Webserver
webserver:
  replicas: 2
  resources:
    requests:
      cpu: "500m"
      memory: "512Mi"
    limits:
      cpu: "1"
      memory: "2Gi"
  service:
    type: ClusterIP

# Workers (KubernetesExecutor — pods created per task)
workers:
  resources:
    requests:
      cpu: "500m"
      memory: "1Gi"
    limits:
      cpu: "2"
      memory: "4Gi"

# DAGs — git-sync sidecar
dags:
  gitSync:
    enabled: true
    repo: https://github.com/org/airflow-dags.git
    branch: main
    depth: 1
    subPath: dags
    wait: 60
    credentialsSecret: git-credentials   # Secret with .git-credentials

# Logs — remote S3 (no PVC needed with KubernetesExecutor)
logs:
  persistence:
    enabled: false

# Metadata database — external PostgreSQL
data:
  metadataConnection:
    user: airflow
    pass: ~
    host: postgres.airflow.svc.cluster.local
    port: 5432
    db: airflow
    protocol: postgresql
  metadataSecretName: airflow-db-credentials

# Fernet key from secret
fernetKeySecretName: airflow-fernet-key

# Redis — not needed for KubernetesExecutor
redis:
  enabled: false

# Triggerer (for deferred operators)
triggerer:
  enabled: true
  replicas: 1
  resources:
    requests:
      cpu: "250m"
      memory: "512Mi"

# Pod template for worker pods
podTemplate:
  nodeSelector:
    node.kubernetes.io/role: airflow-worker
  tolerations:
    - key: dedicated
      operator: Equal
      value: airflow
      effect: NoSchedule
  env:
    - name: AIRFLOW__OPENLINEAGE__TRANSPORT
      value: '{"type": "http", "url": "http://marquez.lineage:5000", "endpoint": "api/v1/lineage"}'
    - name: AIRFLOW__OPENLINEAGE__NAMESPACE
      value: airflow

# RBAC
rbac:
  create: true

# Service account
serviceAccount:
  create: true
  name: airflow
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789:role/airflow-s3-role
```

### KubernetesPodOperator

The `KubernetesPodOperator` runs an arbitrary container as a task — ideal for isolated environments, non-Python code, or heavy dependencies.

```python
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

spark_transform = KubernetesPodOperator(
    task_id="spark_transform_orders",
    name="spark-transform-orders",
    namespace="spark-jobs",
    image="registry.local/spark:3.5.1-custom",
    cmds=["python3"],
    arguments=["/opt/spark/jobs/transform_orders.py"],
    env_vars={
        "SPARK_MASTER": "k8s://https://k8s-api:6443",
        "WAREHOUSE_URI": "s3a://data-lake/warehouse",
    },
    env_from=[
        k8s.V1EnvFromSource(
            secret_ref=k8s.V1SecretEnvSource(name="s3-credentials")
        )
    ],
    container_resources=k8s.V1ResourceRequirements(
        requests={"cpu": "1", "memory": "2Gi"},
        limits={"cpu": "2", "memory": "4Gi"},
    ),
    node_selector={"node.kubernetes.io/role": "data-worker"},
    tolerations=[
        k8s.V1Toleration(
            key="dedicated",
            operator="Equal",
            value="spark",
            effect="NoSchedule",
        )
    ],
    image_pull_policy="IfNotPresent",
    get_logs=True,
    log_events_on_failure=True,
    is_delete_operator_pod=True,       # clean up after completion
    startup_timeout_seconds=120,
    on_finish_action="delete_pod",
    do_xcom_push=False,
)
```

### Airflow Triggering Spark Jobs

For Spark jobs submitted by Airflow (not directly, but via `SparkSubmitOperator` or `KubernetesPodOperator`):

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

run_etl = SparkSubmitOperator(
    task_id="run_silver_etl",
    application="s3a://data-lake/jobs/silver_etl.py",
    name="silver-etl-{{ ds_nodash }}",
    conn_id="spark_k8s",               # Connection: master = k8s://https://...
    deploy_mode="cluster",
    driver_memory="4g",
    executor_memory="8g",
    executor_cores=4,
    num_executors=4,
    conf={
        "spark.kubernetes.namespace": "spark-jobs",
        "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
        "spark.kubernetes.container.image": "registry.local/spark:3.5.1-custom",
        "spark.openlineage.transport.type": "http",
        "spark.openlineage.transport.url": "http://marquez.lineage:5000",
        "spark.openlineage.namespace": "spark://k8s",
        "spark.openlineage.parentJobNamespace": "airflow",
        "spark.openlineage.parentJobName": "etl_dag.run_silver_etl",
    },
    jars="s3a://data-lake/jars/openlineage-spark_2.12-1.15.0.jar",
)
```

---

## Resource Quotas per Namespace

```yaml
# resource-quota.yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: spark-jobs-quota
  namespace: spark-jobs
spec:
  hard:
    requests.cpu: "80"
    requests.memory: "320Gi"
    limits.cpu: "120"
    limits.memory: "480Gi"
    pods: "100"
    persistentvolumeclaims: "20"
---
apiVersion: v1
kind: LimitRange
metadata:
  name: spark-default-limits
  namespace: spark-jobs
spec:
  limits:
    - type: Container
      default:
        cpu: "2"
        memory: "4Gi"
      defaultRequest:
        cpu: "500m"
        memory: "1Gi"
      max:
        cpu: "16"
        memory: "64Gi"
```

---

## Secrets Management

```bash
# Create S3 credentials secret
kubectl create secret generic s3-credentials \
  --from-literal=access-key=AKIAIOSFODNN7EXAMPLE \
  --from-literal=secret-key=wJalrXUtnFEMI/K7MDENG \
  --namespace spark-jobs

# Create git credentials for Airflow git-sync
kubectl create secret generic git-credentials \
  --from-literal=.git-credentials="https://token:ghp_xxx@github.com" \
  --namespace airflow
```

For production, use **External Secrets Operator** to sync secrets from AWS Secrets Manager or HashiCorp Vault:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: s3-credentials
  namespace: spark-jobs
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secrets-store
    kind: ClusterSecretStore
  target:
    name: s3-credentials
  data:
    - secretKey: access-key
      remoteRef:
        key: prod/spark/s3
        property: access_key_id
    - secretKey: secret-key
      remoteRef:
        key: prod/spark/s3
        property: secret_access_key
```

---

## Monitoring

### Prometheus + Grafana

```yaml
# Airflow Helm values — enable metrics
statsd:
  enabled: false   # use built-in Prometheus metrics instead

# Install Prometheus operator separately, then:
# airflow.cfg
[metrics]
statsd_on = False
[metrics_allow_list]
# Airflow 2.6+ exposes /metrics endpoint on port 8085 (scheduler)
```

### Spark Metrics

```bash
# Enable Spark Prometheus sink in spark-defaults.conf
spark.metrics.conf.*.sink.prometheussink.class=org.apache.spark.metrics.sink.PrometheusServlet
spark.metrics.conf.*.sink.prometheussink.path=/metrics/prometheus
spark.metrics.conf.driver.sink.prometheussink.class=org.apache.spark.metrics.sink.PrometheusServlet
spark.ui.prometheus.enabled=true
```

### Key Alerts

| Alert | Condition | Severity |
|-------|-----------|---------|
| Spark executor OOM | Container killed by OOMKiller | Critical |
| Airflow task queued > 30min | Task stuck in scheduled state | Warning |
| Worker pod pending > 5min | Insufficient cluster capacity | Warning |
| Airflow scheduler heartbeat missed | Scheduler dead | Critical |
| Spark driver pod crash loop | `restartCount > 3` | Critical |

---

## Common Debugging Commands

```bash
# Spark — find driver pod
kubectl get pods -n spark-jobs -l spark-role=driver

# Spark — tail driver logs
kubectl logs -n spark-jobs -l spark-role=driver --tail=100 -f

# Spark — check why executor pods are pending
kubectl describe pod <executor-pod-name> -n spark-jobs

# Airflow — check task pod logs
kubectl logs -n airflow -l dag_id=etl_dag,task_id=run_silver_etl --tail=200

# Airflow — scheduler logs
kubectl logs -n airflow -l component=scheduler --tail=100 -f

# Check resource quota usage
kubectl describe resourcequota spark-jobs-quota -n spark-jobs

# See pending pods and their events
kubectl get events -n spark-jobs --sort-by='.lastTimestamp' | tail -20

# Airflow — trigger a DAG manually
kubectl exec -n airflow deploy/airflow-scheduler -- \
  airflow dags trigger etl_dag --exec-date 2024-03-15T00:00:00
```

---

## Anti-Patterns

1. **Not setting memory overhead** — JVM processes (Spark driver/executor) need overhead for off-heap memory. Always set `memoryOverhead` ≥ 10% of heap, or `memoryOverheadFactor=0.1` (min 384 MB).

2. **Running Spark in client mode on K8s** — client mode runs the driver outside K8s, creating asymmetric connectivity. Always use `--deploy-mode cluster` for Kubernetes.

3. **Storing DAGs in a PVC shared between scheduler and workers** — race conditions and mount failures under load. Use git-sync or remote object storage with the DAG processor.

4. **No `is_delete_operator_pod=True` on KubernetesPodOperator** — completed pods accumulate and exhaust the pod limit. Always clean up after task completion.

5. **Using `clusterrole=cluster-admin` for Spark service account** — grants excessive privilege. Grant only `edit` in the spark namespace, or create a minimal Role with only pod/service/configmap permissions.

6. **Not configuring pod disruption budgets** — rolling node upgrades can kill all Airflow scheduler replicas simultaneously. Create a PDB ensuring at least 1 scheduler replica is available.

7. **No liveness/readiness probes on Airflow webserver** — traffic is sent to a unresponsive pod during restart. The Helm chart adds these by default; don't disable them.

8. **Baking secrets into Docker images** — any `docker history` or registry access reveals them. Use Kubernetes Secrets or External Secrets Operator instead.

9. **Dynamic allocation without shuffle tracking** — requires an external shuffle service (not available on K8s by default). Enable `shuffleTracking.enabled=true` when no shuffle service is deployed.

10. **Ignoring resource quotas until cluster exhaustion** — a single runaway Spark job can consume all cluster memory. Set namespace ResourceQuotas and LimitRanges before the first production job.

---

## References to Consult When Needed

- Spark on Kubernetes: `spark.apache.org/docs/latest/running-on-kubernetes.html`
- Airflow Helm chart: `airflow.apache.org/docs/helm-chart/stable/`
- KubernetesPodOperator: `airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/`
- Spark History Server on K8s: `spark.apache.org/docs/latest/monitoring.html`
- External Secrets Operator: `external-secrets.io/`
