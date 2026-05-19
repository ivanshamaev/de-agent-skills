---
name: infra-kubernetes-cluster-health
description: Kubernetes cluster health assessment — node status and pressure conditions (disk/memory/PID), pod failure diagnosis (CrashLoopBackOff/OOMKilled/Pending/Evicted), control plane health (API server/etcd/scheduler/controller-manager), resource quota utilization, scheduling failures (taints/affinity/insufficient resources), kubectl diagnostic commands, node eviction policies, kubelet troubleshooting
---

# Kubernetes Cluster Health

## When to Use

- Diagnosing a degraded or unresponsive Kubernetes cluster
- Investigating pods stuck in Pending, CrashLoopBackOff, or Evicted states
- Assessing control plane health before a production deployment
- Responding to node pressure alerts (disk/memory/PID)
- Capacity planning and resource quota review

---

## Node Health

### Quick Status Overview

```bash
# Overall node status — spot NotReady nodes
kubectl get nodes -o wide

# Detailed node conditions + events
kubectl describe node <node-name>

# Resource consumption per node
kubectl top nodes

# Node conditions (MemoryPressure, DiskPressure, PIDPressure, NetworkUnavailable)
kubectl get nodes -o json | jq '.items[] | {name: .metadata.name, conditions: .status.conditions}'
```

### Node Conditions Reference

| Condition | Meaning | Action |
|-----------|---------|--------|
| `MemoryPressure=True` | Node running low on memory | Evict low-priority pods; scale up |
| `DiskPressure=True` | Node disk > eviction threshold | Clean image cache; expand disk |
| `PIDPressure=True` | Too many processes on node | Find PID-leaking pods |
| `NetworkUnavailable=True` | CNI not configured correctly | Check CNI DaemonSet pods |
| `NotReady` | kubelet stopped heartbeating | Check kubelet service on node |

### Node Maintenance

```bash
# Drain node safely (reschedule pods)
kubectl drain <node-name> --ignore-daemonsets --delete-emptydir-data

# Mark schedulable again after maintenance
kubectl uncordon <node-name>

# Cordon without draining (no new pods scheduled)
kubectl cordon <node-name>
```

---

## Pod Failure Diagnosis

### Status Quick Reference

```bash
# All pods across all namespaces, filter non-Running
kubectl get pods -A | grep -v Running | grep -v Completed

# Pod details with events (most useful command)
kubectl describe pod <pod-name> -n <namespace>

# Last logs from crashed container
kubectl logs <pod-name> -n <namespace> --previous

# Recent events sorted by time
kubectl get events -n <namespace> --sort-by='.lastTimestamp' | tail -40
```

### CrashLoopBackOff

```bash
# 1. Check logs from crashed container
kubectl logs <pod> -n <ns> --previous

# 2. Check exit code
kubectl get pod <pod> -n <ns> -o json | jq '.status.containerStatuses[].lastState.terminated'

# 3. Check liveness probe config
kubectl describe pod <pod> -n <ns> | grep -A 10 "Liveness"
```

Common causes:
- Exit code `1`: application error — check app logs
- Exit code `137`: OOMKilled — increase memory limit
- Exit code `139`: segfault — application bug
- Liveness probe failing before app is ready — add `initialDelaySeconds`

### OOMKilled

```bash
# Confirm OOMKill
kubectl describe pod <pod> -n <ns> | grep -i "OOMKilled\|memory"

# Check current limits
kubectl get pod <pod> -n <ns> -o json | jq '.spec.containers[].resources'

# Check node memory under pressure
kubectl top nodes
```

Fix: increase `resources.limits.memory` or find the memory leak.

### Pending Pods

```bash
# Find all Pending pods
kubectl get pods -A --field-selector=status.phase=Pending

# Why is it Pending?
kubectl describe pod <pod> -n <ns> | grep -A 5 "Events:"
```

Common reasons:
| Reason | Diagnosis | Fix |
|--------|-----------|-----|
| `Insufficient cpu/memory` | `kubectl describe node` — check Allocatable | Scale out cluster or reduce requests |
| `Unschedulable — node selector` | Node label mismatch | Fix nodeSelector or add label to node |
| `Unschedulable — taint` | Node has NoSchedule taint | Add toleration to pod spec |
| `PVC unbound` | PVC pending | Check StorageClass and PVC events |
| `Image pull error` | Wrong image or missing secret | Check imagePullSecrets |

### Evicted Pods

```bash
# List evicted pods
kubectl get pods -A | grep Evicted

# Clean up evicted pods (bulk delete)
kubectl get pods -A | grep Evicted | awk '{print $1, $2}' | xargs -n2 kubectl delete pod -n

# Why was it evicted?
kubectl describe pod <evicted-pod> -n <ns> | grep "Reason:"
```

---

## Control Plane Health

### API Server

```bash
# Check API server responsiveness
kubectl get --raw /healthz
kubectl get --raw /readyz
kubectl get --raw /livez

# API server latency (via metrics)
kubectl get --raw /metrics | grep apiserver_request_duration_seconds
```

### etcd

```bash
# etcd health check (run inside etcd pod or host)
ETCDCTL_API=3 etcdctl \
  --endpoints=https://127.0.0.1:2379 \
  --cacert=/etc/kubernetes/pki/etcd/ca.crt \
  --cert=/etc/kubernetes/pki/etcd/server.crt \
  --key=/etc/kubernetes/pki/etcd/server.key \
  endpoint health

# Check etcd member list
ETCDCTL_API=3 etcdctl member list \
  --endpoints=https://127.0.0.1:2379 \
  --cacert=/etc/kubernetes/pki/etcd/ca.crt \
  --cert=/etc/kubernetes/pki/etcd/server.crt \
  --key=/etc/kubernetes/pki/etcd/server.key
```

### Scheduler and Controller Manager

```bash
# Check system-level component health
kubectl get componentstatuses

# Verify kube-system pods are Running
kubectl get pods -n kube-system

# Scheduler logs
kubectl logs -n kube-system -l component=kube-scheduler --tail=50

# Controller manager logs
kubectl logs -n kube-system -l component=kube-controller-manager --tail=50
```

---

## Resource Quota and Limits

```bash
# View all resource quotas
kubectl get resourcequota -A

# Detailed usage per namespace
kubectl describe resourcequota -n <namespace>

# View LimitRange (default requests/limits for containers)
kubectl describe limitrange -n <namespace>

# Compute total requested vs allocatable per node
kubectl describe nodes | grep -A 8 "Allocated resources"
```

---

## Scheduling Issues

### Taint and Toleration Check

```bash
# View all node taints
kubectl get nodes -o custom-columns=NODE:.metadata.name,TAINT:.spec.taints

# Check pod tolerations
kubectl get pod <pod> -n <ns> -o json | jq '.spec.tolerations'
```

### Affinity and Node Selector

```bash
# Check pod's nodeSelector
kubectl get pod <pod> -n <ns> -o json | jq '.spec.nodeSelector'

# Check node labels
kubectl get nodes --show-labels

# Find nodes that match a label
kubectl get nodes -l disktype=ssd
```

---

## Cluster Health Script

```bash
#!/bin/bash
# Quick cluster health summary

echo "=== Nodes ==="
kubectl get nodes

echo ""
echo "=== NotReady Nodes ==="
kubectl get nodes | grep -v " Ready"

echo ""
echo "=== Pending/Failed Pods (all namespaces) ==="
kubectl get pods -A | grep -v -E "Running|Completed"

echo ""
echo "=== Recent Events (Warnings) ==="
kubectl get events -A --field-selector type=Warning \
  --sort-by='.lastTimestamp' | tail -20

echo ""
echo "=== Node Resource Usage ==="
kubectl top nodes 2>/dev/null || echo "metrics-server not available"

echo ""
echo "=== Control Plane ==="
kubectl get pods -n kube-system | grep -E "apiserver|etcd|scheduler|controller"
```

---

## Prometheus Alerts to Configure

```yaml
# Node memory pressure
- alert: NodeMemoryPressure
  expr: kube_node_status_condition{condition="MemoryPressure",status="true"} == 1
  for: 5m
  labels:
    severity: warning

# Pod CrashLooping
- alert: PodCrashLooping
  expr: rate(kube_pod_container_status_restarts_total[15m]) * 60 * 5 > 5
  for: 5m
  labels:
    severity: critical

# Pending pods stuck
- alert: PodStuckPending
  expr: kube_pod_status_phase{phase="Pending"} > 0
  for: 15m
  labels:
    severity: warning

# API server latency
- alert: APIServerHighLatency
  expr: histogram_quantile(0.99, sum(rate(apiserver_request_duration_seconds_bucket{verb!="WATCH"}[5m])) by (le)) > 2
  for: 5m
  labels:
    severity: warning
```

---

## Anti-Patterns

1. **Ignoring `kubectl get events`** — events contain the most actionable failure reasons; always check before diving into logs.
2. **Killing Pending pods instead of diagnosing** — Pending pods need resource/scheduling fixes, not deletion; deleting recreates the same problem.
3. **No ResourceQuota in multi-tenant clusters** — a single noisy namespace can starve others; always set namespace-level quotas.
4. **No `requests` set on containers** — scheduler can't make placement decisions without requests; always set both requests and limits.
5. **Draining without `--ignore-daemonsets`** — drain will fail on DaemonSet pods; always include this flag.
6. **Not monitoring etcd disk usage** — etcd writes all cluster state; full disk = cluster API freeze.

---

## References

- Kubernetes troubleshooting: `kubernetes.io/docs/tasks/debug/`
- Node pressure eviction: `kubernetes.io/docs/concepts/scheduling-eviction/node-pressure-eviction/`
- Component health: `kubernetes.io/docs/concepts/cluster-administration/system-metrics/`
- Related skills: `[[infra-kubernetes-autoscaling-review]]`, `[[infra-kubernetes-cost-optimizer]]`, `[[infra-observability-stack-review]]`
