---
name: infra-kubernetes-cost-optimizer
description: Kubernetes cost optimization — idle workload detection (zero-replica deployments, always-pending jobs), overprovisioning analysis (request vs actual CPU/memory), namespace-level cost attribution, node bin-packing via requests tuning, spot instance strategy, LimitRange defaults, right-sizing with VPA recommendations, Goldilocks tool, resource efficiency metrics, namespace resource quotas for chargeback
---

# Kubernetes Cost Optimizer

## When to Use

- Kubernetes cloud bill is growing without proportional workload growth
- Clusters are underutilized but node count is high
- Teams over-request resources to "be safe"
- Identifying idle namespaces or dormant workloads
- Implementing FinOps practices on Kubernetes

---

## Identify Overprovisioning

### CPU and Memory Request vs Actual

```bash
# Compare requests vs actual usage per pod
kubectl top pods -A --sort-by=cpu

# Get all pods with their resource requests
kubectl get pods -A -o json | jq -r '
  .items[] |
  .metadata.namespace as $ns |
  .metadata.name as $name |
  .spec.containers[].resources.requests |
  "\($ns)/\($name): cpu=\(.cpu // "none") mem=\(.memory // "none")"
'

# Find pods with no resource requests (invisible to scheduler)
kubectl get pods -A -o json | jq -r '
  .items[] |
  select(.spec.containers[].resources.requests == null) |
  "\(.metadata.namespace)/\(.metadata.name)"
'
```

### Node Utilization

```bash
# Node CPU/memory utilization
kubectl top nodes

# Get allocatable vs requested per node
kubectl describe nodes | grep -A 8 "Allocated resources:"

# Nodes below 30% utilization (candidates for removal)
kubectl top nodes | awk 'NR>1 {if ($3 < 30 && $5 < 30) print $1, "CPU:"$3, "Mem:"$5}'
```

---

## Goldilocks — VPA-Based Right-Sizing Recommendations

Goldilocks runs VPA in recommendation mode and generates dashboards:

```bash
# Install Goldilocks
helm repo add fairwinds-stable https://charts.fairwinds.com/stable
helm install goldilocks fairwinds-stable/goldilocks -n goldilocks --create-namespace

# Enable namespace for recommendations
kubectl label namespace production goldilocks.fairwinds.com/enabled=true

# Port-forward dashboard
kubectl -n goldilocks port-forward svc/goldilocks-dashboard 8080:80
```

The dashboard shows right-sized CPU/memory requests per container based on actual usage.

---

## Detect Idle Workloads

### Zero-Traffic Deployments

```bash
# Find deployments with 0 replicas
kubectl get deployments -A -o json | jq -r '
  .items[] |
  select(.spec.replicas == 0) |
  "\(.metadata.namespace)/\(.metadata.name)"
'

# Find deployments that haven't been updated in 90 days
kubectl get deployments -A -o json | jq -r '
  .items[] |
  select(
    (.metadata.creationTimestamp | fromdateiso8601) < (now - 7776000)
  ) |
  "\(.metadata.namespace)/\(.metadata.name): created=\(.metadata.creationTimestamp)"
'
```

### Unused ConfigMaps and Secrets

```bash
# Find ConfigMaps not referenced by any pod/deployment
# (manual audit — list all then cross-reference)
kubectl get configmaps -A --no-headers | grep -v "kube-" | wc -l

# Find PVCs not bound to any pod
kubectl get pvc -A | grep -v Bound
```

---

## Resource Request Tuning

### LimitRange — Namespace Defaults

```yaml
# Ensure all containers get sane defaults
apiVersion: v1
kind: LimitRange
metadata:
  name: default-limits
  namespace: production
spec:
  limits:
  - type: Container
    default:           # applied if no limit set
      cpu: 500m
      memory: 256Mi
    defaultRequest:    # applied if no request set
      cpu: 100m
      memory: 128Mi
    max:               # hard cap per container
      cpu: 4
      memory: 4Gi
    min:
      cpu: 10m
      memory: 16Mi
```

### ResourceQuota — Namespace Budget

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: production-quota
  namespace: production
spec:
  hard:
    requests.cpu: "50"          # total CPU requests across all pods
    requests.memory: 100Gi
    limits.cpu: "100"
    limits.memory: 200Gi
    persistentvolumeclaims: "20"
    count/pods: "200"
```

---

## Right-Sizing Formula

```
# Recommended CPU request = P95 actual usage × 1.2 (20% headroom)
# Recommended memory request = P99 actual usage × 1.3 (30% headroom)

# Prometheus query: P95 CPU over last 7 days
quantile_over_time(0.95,
  rate(container_cpu_usage_seconds_total{container!="",namespace="production"}[5m])[7d:5m]
) * 1000  # convert to millicores

# Prometheus query: P99 memory over last 7 days
quantile_over_time(0.99,
  container_memory_working_set_bytes{container!="",namespace="production"}[7d:5m]
)
```

---

## Cost Attribution by Namespace

```bash
# OpenCost / Kubecost: provides per-namespace cost
# Without tooling: estimate based on node cost × resource share

# CPU share per namespace
kubectl top pods -A | awk '{print $1}' | sort | uniq -c | sort -rn

# Resource requests by namespace
kubectl get pods -A -o json | jq -r '
  .items[] |
  .metadata.namespace as $ns |
  .spec.containers[].resources.requests |
  "\($ns) cpu=\(.cpu // "0") memory=\(.memory // "0")"
' | sort | uniq -c
```

### Recommended: OpenCost / Kubecost

```bash
# Install OpenCost (open-source, no license required)
helm install opencost \
  opencost/opencost \
  -n opencost --create-namespace \
  --set opencost.exporter.defaultClusterId=prod-cluster

# Port-forward UI
kubectl port-forward -n opencost svc/opencost 9090:9090
```

---

## Spot Instance Cost Savings

```yaml
# GKE — create spot node pool (up to 80% cheaper)
gcloud container node-pools create spot-pool \
  --cluster=prod \
  --machine-type=n2-standard-8 \
  --spot \
  --num-nodes=0 \
  --enable-autoscaling \
  --min-nodes=0 \
  --max-nodes=50

# EKS — Karpenter node pool with spot preference
apiVersion: karpenter.sh/v1alpha5
kind: Provisioner
metadata:
  name: spot-provisioner
spec:
  requirements:
  - key: karpenter.sh/capacity-type
    operator: In
    values: ["spot", "on-demand"]  # spot preferred
  - key: node.kubernetes.io/instance-type
    operator: In
    values: ["m5.xlarge", "m5.2xlarge", "m4.xlarge"]
  limits:
    resources:
      cpu: 1000
  ttlSecondsAfterEmpty: 30
```

---

## Cost Optimization Checklist

```
[ ] All containers have resource requests set
[ ] LimitRange installed in each namespace
[ ] ResourceQuota set per team/namespace
[ ] VPA running in recommendation mode (Goldilocks)
[ ] Idle deployments (0 replicas) reviewed and removed
[ ] Spot/preemptible nodes used for batch/streaming workloads
[ ] Cluster Autoscaler scale-down enabled
[ ] PVCs without pods reviewed and cleaned up
[ ] Image sizes minimized (multi-stage builds)
[ ] Scheduled scale-down for dev/staging clusters at night
[ ] Cost attribution dashboard (OpenCost/Kubecost) installed
```

---

## Scheduled Scale-Down for Non-Prod

```yaml
# Scale down staging at night using CronJob
apiVersion: batch/v1
kind: CronJob
metadata:
  name: staging-scaledown
spec:
  schedule: "0 22 * * 1-5"      # 22:00 Mon-Fri
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: scaler
          containers:
          - name: kubectl
            image: bitnami/kubectl
            command:
            - /bin/sh
            - -c
            - |
              kubectl scale deployment --all --replicas=0 -n staging
          restartPolicy: Never
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: staging-scaleup
spec:
  schedule: "0 8 * * 1-5"       # 08:00 Mon-Fri
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: scaler
          containers:
          - name: kubectl
            image: bitnami/kubectl
            command:
            - /bin/sh
            - -c
            - |
              kubectl scale deployment --all --replicas=2 -n staging
          restartPolicy: Never
```

---

## Anti-Patterns

1. **Setting `requests == limits`** for all containers — prevents Burstable class and over-reserves; set requests to P50 and limits to P99+.
2. **No LimitRange in namespaces** — containers without requests can starve other pods on the same node.
3. **Over-provisioning "just in case"** — requests 4x actual usage is common; right-size to P95 + 20%.
4. **Keeping all clusters running 24/7** — dev/staging clusters should scale to zero overnight.
5. **Not using spot for stateless workloads** — spot instances cost 60-80% less; batch jobs, stream processing, and APIs tolerate interruption.

---

## References

- Goldilocks: `github.com/FairwindsOps/goldilocks`
- OpenCost: `opencost.io`
- Karpenter: `karpenter.sh/docs/`
- Related skills: `[[infra-kubernetes-autoscaling-review]]`, `[[infra-kubernetes-cluster-health]]`, `[[de-cost-optimization]]`
