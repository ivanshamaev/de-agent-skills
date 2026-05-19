---
name: infra-kubernetes-autoscaling-review
description: Kubernetes autoscaling review — HPA (CPU/memory/custom/external metrics), stabilizationWindowSeconds anti-flapping, VPA vs HPA decision, Cluster Autoscaler tuning (scale-down delay/utilization threshold), KEDA event-driven autoscaling (Kafka lag/queue depth), spot node optimization, pod disruption budgets, scaling behavior policies (Percent/Pods/Max/Min selectPolicy)
---

# Kubernetes Autoscaling Review

## When to Use

- Reviewing HPA configuration for a production workload
- Diagnosing autoscaling that isn't triggering or is flapping
- Choosing between HPA, VPA, and KEDA for a specific workload type
- Tuning Cluster Autoscaler for cost-efficient node provisioning
- Setting up event-driven scaling for Kafka consumer or queue-based workloads

---

## HPA (Horizontal Pod Autoscaler)

### Basic CPU + Memory HPA

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: orders-api
  namespace: production
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: orders-api
  minReplicas: 3
  maxReplicas: 50
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70      # scale up when avg > 70% of request
  - type: Resource
    resource:
      name: memory
      target:
        type: AverageValue
        averageValue: 400Mi
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 0   # scale up immediately
      policies:
      - type: Percent
        value: 100                    # max double replicas per 15s
        periodSeconds: 15
    scaleDown:
      stabilizationWindowSeconds: 300 # wait 5 min before scale-down
      policies:
      - type: Percent
        value: 10                     # reduce max 10% per minute
        periodSeconds: 60
      selectPolicy: Min               # conservative: use smallest reduction
```

### Prerequisites: Resource Requests

HPA only works when containers have requests set:

```yaml
containers:
- name: app
  resources:
    requests:
      cpu: 200m        # HPA compares actual usage vs this
      memory: 256Mi
    limits:
      cpu: 1000m
      memory: 512Mi
  readinessProbe:      # Critical: unready pods skew HPA metrics
    httpGet:
      path: /ready
      port: 8080
    initialDelaySeconds: 10
    periodSeconds: 5
```

### HPA Diagnosis

```bash
# Check HPA status and current metrics
kubectl get hpa -n <namespace>
kubectl describe hpa <name> -n <namespace>

# Check events for scaling decisions
kubectl get events -n <namespace> | grep Horizontal

# Check metrics availability
kubectl get --raw /apis/metrics.k8s.io/v1beta1/pods | jq .

# Is metrics-server running?
kubectl get deployment metrics-server -n kube-system
```

Common issues:
| Symptom | Root Cause | Fix |
|---------|-----------|-----|
| `unknown` metrics | metrics-server not installed | Deploy metrics-server |
| No scaling at 100% CPU | No resource requests set | Add `resources.requests.cpu` |
| Rapid flapping | Low stabilizationWindowSeconds | Set `scaleDown.stabilizationWindowSeconds: 300` |
| Never scales down | Pods not fully ready | Fix readinessProbe |

---

## KEDA — Event-Driven Autoscaling

KEDA extends HPA with external trigger sources (Kafka, queues, cron, etc.).

### Kafka Consumer Lag Scaler

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: kafka-consumer-scaler
spec:
  scaleTargetRef:
    name: orders-consumer
  pollingInterval: 15        # check lag every 15s
  cooldownPeriod: 30
  minReplicaCount: 1
  maxReplicaCount: 20
  triggers:
  - type: kafka
    metadata:
      bootstrapServers: kafka:9092
      consumerGroup: orders-processor
      topic: orders
      lagThreshold: "100"          # scale when lag > 100 messages per partition
      activationLagThreshold: "10" # scale from 0 when lag > 10
```

### Cron-Based Scaler (Pre-warm for business hours)

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: api-business-hours
spec:
  scaleTargetRef:
    name: orders-api
  triggers:
  - type: cron
    metadata:
      timezone: Europe/Moscow
      start: "0 9 * * 1-5"    # 09:00 Mon–Fri
      end: "0 21 * * 1-5"     # 21:00 Mon–Fri
      desiredReplicas: "10"
```

---

## VPA (Vertical Pod Autoscaler)

Use VPA when workload has fixed concurrency but unknown resource needs.

```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: spark-worker
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: spark-worker
  updatePolicy:
    updateMode: "Off"    # Off=recommendations only, Auto=restart pods to apply
  resourcePolicy:
    containerPolicies:
    - containerName: spark
      minAllowed:
        cpu: 100m
        memory: 256Mi
      maxAllowed:
        cpu: 4
        memory: 8Gi
      controlledResources: ["cpu", "memory"]
```

```bash
# Check VPA recommendations
kubectl describe vpa spark-worker
# Look for: Recommendation section with target/lowerBound/upperBound
```

### HPA vs VPA Decision Guide

| Workload Characteristic | Recommended |
|------------------------|------------|
| Variable request rate (API, web) | HPA on CPU/RPS |
| Fixed concurrency, unknown resources | VPA (Off mode for recommendations) |
| Queue-based / event-driven | KEDA |
| Stateful (databases, caches) | VPA with Auto mode carefully |
| Don't use HPA + VPA together on same container | Use only one |

---

## Cluster Autoscaler

Scales the number of nodes based on pending pods and underutilization.

### Key Configuration Flags

```yaml
# cluster-autoscaler deployment args
- --scale-down-enabled=true
- --scale-down-delay-after-add=10m       # wait 10m after scale-up before scale-down
- --scale-down-unneeded-time=10m         # node must be unneeded for 10m before removal
- --scale-down-utilization-threshold=0.5 # remove if < 50% CPU/memory utilized
- --max-node-provision-time=15m
- --skip-nodes-with-local-storage=false
- --skip-nodes-with-system-pods=true
```

### Node Annotations for Cluster Autoscaler

```bash
# Prevent a node from being removed
kubectl annotate node <node> cluster-autoscaler.kubernetes.io/scale-down-disabled=true

# Check CA activity
kubectl logs -n kube-system -l app=cluster-autoscaler --tail=100 | grep -E "scale|unneeded"
```

---

## Pod Disruption Budgets (PDB)

PDBs protect availability during scaling-down and rolling updates:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: orders-api-pdb
spec:
  minAvailable: 2          # always keep at least 2 pods running
  # OR: maxUnavailable: 1  # allow at most 1 pod disrupted at a time
  selector:
    matchLabels:
      app: orders-api
```

```bash
# View PDB status
kubectl get pdb -n <namespace>
kubectl describe pdb orders-api-pdb -n <namespace>
```

---

## Spot Node Optimization

```yaml
# Node pool with spot instances
# Mark spot nodes with a taint
taints:
- key: cloud.google.com/gke-spot
  value: "true"
  effect: NoSchedule

# Workloads that can run on spot: add toleration
tolerations:
- key: cloud.google.com/gke-spot
  operator: Equal
  value: "true"
  effect: NoSchedule

# Prefer spot, fall back to on-demand
affinity:
  nodeAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      preference:
        matchExpressions:
        - key: cloud.google.com/gke-spot
          operator: In
          values: ["true"]
```

---

## Anti-Patterns

1. **HPA without resource requests** — HPA can't calculate utilization percentage; always set `resources.requests`.
2. **HPA + VPA on same container in auto mode** — they fight each other; use only one, or VPA in Off mode.
3. **Low `stabilizationWindowSeconds` for scale-down** — causes rapid pod churn; set to 300s minimum.
4. **No PDB on scaled deployments** — Cluster Autoscaler can remove all pods simultaneously; set `minAvailable: 2`.
5. **Scaling on memory alone for JVM apps** — JVM heap grows to fill available memory regardless of load; scale on CPU or custom RPS metrics instead.
6. **`maxReplicas` too low** — HPA hits ceiling and can't handle load spikes; set maxReplicas to at least 3x normal.

---

## References

- HPA: `kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/`
- KEDA: `keda.sh/docs/`
- VPA: `github.com/kubernetes/autoscaler/tree/master/vertical-pod-autoscaler`
- Cluster Autoscaler: `github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler`
- Related skills: `[[infra-kubernetes-cluster-health]]`, `[[infra-kubernetes-cost-optimizer]]`
