---
name: infra-kubernetes-storage-review
description: Kubernetes storage review — PVC lifecycle (provisioning/binding/reclaim), StorageClass selection (SSD/HDD/NVMe), dynamic vs static provisioning, StatefulSet volume management, volume performance tuning (ReadWriteMany vs ReadWriteOnce), CSI drivers (AWS EBS/GCP PD/Ceph/Longhorn), PVC resize, snapshot and backup, storage class access modes, orphaned PVs detection, shared storage patterns (NFS/CephFS)
---

# Kubernetes Storage Review

## When to Use

- Choosing the right StorageClass for a data workload
- Diagnosing PVC stuck in Pending state
- Planning storage for stateful workloads (Kafka, databases, Spark history)
- Setting up volume snapshots for backup
- Auditing orphaned PVs costing money

---

## PVC Lifecycle and Status

```bash
# List all PVCs with status
kubectl get pvc -A

# PVC stuck in Pending — why?
kubectl describe pvc <pvc-name> -n <namespace>
# Look for: Events section — "no nodes available matching...", "StorageClass not found"

# List PersistentVolumes
kubectl get pv

# Find unbound PVs (orphaned, potentially costing money)
kubectl get pv | grep -v Bound

# Find PVCs not mounted by any pod
kubectl get pvc -A -o json | jq -r '
  .items[] |
  select(.status.phase == "Bound") |
  "\(.metadata.namespace)/\(.metadata.name)"
'
```

### PVC Status Reference

| Status | Meaning |
|--------|---------|
| `Pending` | Waiting for volume to be created/bound |
| `Bound` | Successfully bound to a PV |
| `Lost` | Underlying PV deleted or unavailable |
| `Released` | PV released but not yet reclaimed |

---

## StorageClass Design

### AWS EBS StorageClasses

```yaml
# gp3 — general purpose SSD (default, best cost/performance)
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: ebs.csi.aws.com
parameters:
  type: gp3
  iops: "3000"
  throughput: "125"
  encrypted: "true"
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer  # bind to AZ where pod is scheduled

---
# io2 — high IOPS for databases
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: io2-high-iops
provisioner: ebs.csi.aws.com
parameters:
  type: io2
  iops: "10000"
  encrypted: "true"
reclaimPolicy: Retain            # Retain for databases — manual cleanup
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
```

### GKE StorageClasses

```yaml
# Standard SSD
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ssd
provisioner: pd.csi.storage.gke.io
parameters:
  type: pd-ssd
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer

# Extreme for high-throughput databases
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: extreme-ssd
provisioner: pd.csi.storage.gke.io
parameters:
  type: hyperdisk-extreme
  provisioned-iops-on-create: "100000"
allowVolumeExpansion: true
```

---

## StatefulSet Volume Management

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
spec:
  serviceName: kafka
  replicas: 3
  selector:
    matchLabels:
      app: kafka
  template:
    spec:
      containers:
      - name: kafka
        image: confluentinc/cp-kafka:7.5.0
        volumeMounts:
        - name: data
          mountPath: /var/lib/kafka/data
  volumeClaimTemplates:             # one PVC created per replica
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: gp3
      resources:
        requests:
          storage: 500Gi
```

```bash
# List PVCs for a StatefulSet
kubectl get pvc -l app=kafka -n kafka

# StatefulSet PVC naming: <pvc-name>-<statefulset-name>-<ordinal>
# e.g., data-kafka-0, data-kafka-1, data-kafka-2
```

---

## Access Modes

| Mode | Description | Use Case |
|------|-------------|---------|
| `ReadWriteOnce` (RWO) | Single node read-write | Databases, Kafka, most stateful |
| `ReadOnlyMany` (ROX) | Multiple nodes read-only | Shared configuration, static assets |
| `ReadWriteMany` (RWX) | Multiple nodes read-write | NFS, CephFS, shared ML model storage |
| `ReadWriteOncePod` | Single pod read-write | Strict single-writer enforcement |

```yaml
# RWX for shared ML model storage
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: model-store
spec:
  accessModes: ["ReadWriteMany"]
  storageClassName: efs-sc          # AWS EFS or CephFS for RWX
  resources:
    requests:
      storage: 100Gi
```

---

## Volume Expansion

```bash
# Expand a PVC (StorageClass must have allowVolumeExpansion: true)
kubectl patch pvc <pvc-name> -n <namespace> \
  -p '{"spec": {"resources": {"requests": {"storage": "200Gi"}}}}'

# Check expansion progress
kubectl describe pvc <pvc-name> -n <namespace> | grep -A 5 "Conditions"

# For EBS: expansion is online (no pod restart needed in Kubernetes 1.16+)
# For filesystem resize inside pod:
kubectl exec -it <pod> -- resize2fs /dev/xvda  # ext4
kubectl exec -it <pod> -- xfs_growfs /data     # xfs
```

---

## Volume Snapshots and Backup

```yaml
# Create a volume snapshot
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: kafka-data-snapshot-20240115
spec:
  volumeSnapshotClassName: csi-aws-vsc
  source:
    persistentVolumeClaimName: data-kafka-0

---
# Restore from snapshot
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: kafka-data-restored
spec:
  storageClassName: gp3
  dataSource:
    name: kafka-data-snapshot-20240115
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 500Gi
```

---

## Longhorn — Cloud-Native Distributed Storage

For on-premise or bare-metal clusters without cloud storage:

```bash
# Install Longhorn
helm repo add longhorn https://charts.longhorn.io
helm install longhorn longhorn/longhorn -n longhorn-system --create-namespace

# Create StorageClass
# Longhorn provides this automatically: storageclass.kubernetes.io/is-default-class: "true"

# Access Longhorn UI
kubectl -n longhorn-system port-forward svc/longhorn-frontend 8080:80
```

---

## Audit Orphaned Storage

```bash
# Find Released PVs (PVC deleted but PV still exists)
kubectl get pv | grep Released

# Delete orphaned PVs (check reclaimPolicy first!)
# If Retain: manually delete and clean up cloud volume
kubectl delete pv <pv-name>

# Find large PVCs consuming expensive SSD
kubectl get pvc -A -o json | jq -r '
  .items[] |
  "\(.spec.storageClassName) \(.spec.resources.requests.storage) \(.metadata.namespace)/\(.metadata.name)"
' | sort -k2 -rh | head -20
```

---

## Storage Performance Guidelines

| Workload | Recommended StorageClass | Min IOPS | Notes |
|----------|-------------------------|----------|-------|
| Kafka | gp3 / pd-ssd | 3000 | Sequential write; throughput matters more |
| PostgreSQL | io2 / pd-ssd | 5000+ | Random IOPS critical |
| Spark shuffle | Local SSD / NVMe | 10000+ | Use node local storage when possible |
| Elasticsearch | gp3 / pd-ssd | 3000 | Mixed random/sequential |
| Logs / archival | gp2 / pd-standard | 100 | Cost-optimize with cheap storage |
| ML models (shared) | EFS / CephFS (RWX) | N/A | Throughput for batch inference |

---

## Anti-Patterns

1. **`reclaimPolicy: Delete` for databases** — if a PVC is accidentally deleted, data is gone; use `Retain` for production databases.
2. **`volumeBindingMode: Immediate`** — PVC binds before pod schedules, can bind to wrong AZ causing pod to stay Pending; use `WaitForFirstConsumer`.
3. **No volume expansion allowed** — `allowVolumeExpansion: false` means manual PV recreation to resize; always enable it.
4. **Using NFS for write-heavy workloads** — NFS introduces network latency for every write; use block storage (EBS/PD) for databases and Kafka.
5. **Not snapshotting before StatefulSet updates** — no rollback without snapshot; snapshot before every major update.
6. **Shared PVs for multiple pods** — EBS is RWO; multiple pods mounting the same PVC causes corruption; use RWX storage or separate PVCs.

---

## References

- Kubernetes storage: `kubernetes.io/docs/concepts/storage/`
- CSI drivers: `kubernetes-csi.github.io/docs/`
- Longhorn: `longhorn.io/docs/`
- Volume snapshots: `kubernetes.io/docs/concepts/storage/volume-snapshots/`
- Related skills: `[[infra-kubernetes-cluster-health]]`, `[[infra-kubernetes-cost-optimizer]]`
