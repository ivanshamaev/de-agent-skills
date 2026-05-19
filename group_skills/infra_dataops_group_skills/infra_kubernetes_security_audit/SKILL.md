---
name: infra-kubernetes-security-audit
description: Kubernetes security audit — RBAC review (ClusterRoleBindings/RoleBindings/ServiceAccount permissions), Pod Security Standards (privileged/hostPID/hostNetwork/runAsRoot), NetworkPolicy enforcement, Secret management (encryption at rest, external-secrets), image vulnerability scanning (Trivy/Grype), admission controllers (OPA Gatekeeper/Kyverno), audit logging, CIS Kubernetes Benchmark, supply chain security (SBOM/image signing)
---

# Kubernetes Security Audit

## When to Use

- Pre-production security review of a Kubernetes cluster
- Compliance assessment (CIS Benchmark, SOC2, PCI-DSS)
- Investigating a potential security incident
- Reviewing RBAC before onboarding a new team
- Validating secrets handling practices

---

## RBAC Audit

### Find Overly Permissive Roles

```bash
# ClusterRoleBindings with cluster-admin
kubectl get clusterrolebindings -o json | jq -r '
  .items[] |
  select(.roleRef.name == "cluster-admin") |
  "\(.metadata.name): \(.subjects[]?.name)"
'

# All ClusterRoleBindings (review for over-privilege)
kubectl get clusterrolebindings -o wide

# Who can do what in a namespace
kubectl auth can-i --list -n production --as system:serviceaccount:production:default

# Can a specific SA create pods?
kubectl auth can-i create pods -n production \
  --as system:serviceaccount:production:my-service

# All RoleBindings across namespaces
kubectl get rolebindings -A -o wide
```

### Service Account Principle of Least Privilege

```yaml
# Bad: default SA with cluster-admin
# Good: dedicated SA with minimal permissions

apiVersion: v1
kind: ServiceAccount
metadata:
  name: orders-api
  namespace: production
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: orders-api-role
  namespace: production
rules:
- apiGroups: [""]
  resources: ["configmaps"]
  resourceNames: ["orders-config"]   # limit to specific resource
  verbs: ["get"]
- apiGroups: [""]
  resources: ["secrets"]
  resourceNames: ["orders-db-creds"]
  verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: orders-api-binding
  namespace: production
subjects:
- kind: ServiceAccount
  name: orders-api
roleRef:
  kind: Role
  name: orders-api-role
  apiGroup: rbac.authorization.k8s.io
```

### Auto-Mount ServiceAccount Token

```yaml
# Disable auto-mounting if pod doesn't need API access
apiVersion: v1
kind: ServiceAccount
metadata:
  name: batch-job
automountServiceAccountToken: false   # default true — disable when not needed
```

---

## Pod Security Standards

### Pod Security Admission (Kubernetes 1.25+)

```yaml
# Label namespace to enforce security standard
apiVersion: v1
kind: Namespace
metadata:
  name: production
  labels:
    pod-security.kubernetes.io/enforce: restricted    # blocks privileged pods
    pod-security.kubernetes.io/warn: restricted
    pod-security.kubernetes.io/audit: restricted
```

Security levels:
- `privileged`: no restrictions (avoid in production)
- `baseline`: prevents known privilege escalations
- `restricted`: hardened, requires non-root, no host namespaces

### Secure Pod Spec Template

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 3000
        fsGroup: 2000
        seccompProfile:
          type: RuntimeDefault       # enable seccomp
      containers:
      - name: app
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true   # container can't write to FS
          capabilities:
            drop: ["ALL"]              # drop all Linux capabilities
            add: ["NET_BIND_SERVICE"]  # only add what's needed
        volumeMounts:
        - name: tmp
          mountPath: /tmp              # writable temp dir
      volumes:
      - name: tmp
        emptyDir: {}
      hostPID: false                   # never share host PID namespace
      hostNetwork: false               # never share host network
      hostIPC: false
```

### Audit for Security Misconfigurations

```bash
# Find pods running as root
kubectl get pods -A -o json | jq -r '
  .items[] |
  select(
    .spec.securityContext.runAsUser == 0 or
    .spec.containers[].securityContext.runAsUser == 0
  ) |
  "\(.metadata.namespace)/\(.metadata.name)"
'

# Find privileged containers
kubectl get pods -A -o json | jq -r '
  .items[] |
  select(.spec.containers[].securityContext.privileged == true) |
  "\(.metadata.namespace)/\(.metadata.name)"
'

# Pods with hostNetwork=true
kubectl get pods -A -o json | jq -r '
  .items[] |
  select(.spec.hostNetwork == true) |
  "\(.metadata.namespace)/\(.metadata.name)"
'
```

---

## Network Policies

### Default-Deny All Ingress

```yaml
# Block all ingress to a namespace by default
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-ingress
  namespace: production
spec:
  podSelector: {}          # applies to all pods
  policyTypes: ["Ingress"]
```

### Allow Only Specific Traffic

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: orders-api-policy
  namespace: production
spec:
  podSelector:
    matchLabels:
      app: orders-api
  policyTypes: ["Ingress", "Egress"]
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: ingress-nginx
    ports:
    - port: 8080
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: database
    ports:
    - port: 5432
  - to:
    - namespaceSelector: {}    # DNS
    ports:
    - port: 53
      protocol: UDP
```

```bash
# Check if NetworkPolicies are enforced in namespace
kubectl get networkpolicies -n production

# Check CNI supports NetworkPolicy (Calico/Cilium/Weave do; Flannel doesn't)
kubectl get pods -n kube-system | grep -E "calico|cilium|weave"
```

---

## Secret Management

### External Secrets Operator (Vault / AWS SSM)

```yaml
# ExternalSecret pulling from HashiCorp Vault
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: db-credentials
  namespace: production
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: ClusterSecretStore
  target:
    name: db-credentials-k8s   # Kubernetes secret name
    creationPolicy: Owner
  data:
  - secretKey: password
    remoteRef:
      key: secret/production/database
      property: password
```

```bash
# Kubernetes secrets are base64, not encrypted by default
# Verify etcd encryption at rest is configured:
kubectl get apiserver -o yaml | grep -A 5 "EncryptionConfiguration"

# Audit which secrets are accessible
kubectl get secrets -A --no-headers | wc -l

# Check for secrets in plain-text env vars (bad practice)
kubectl get pods -A -o json | jq -r '
  .items[] |
  .spec.containers[].env[]? |
  select(.value != null) |
  select(.name | test("password|secret|token|key"; "i"))
'
```

---

## Image Security

### Scan Images with Trivy

```bash
# Scan a Docker image
trivy image nginx:latest

# Scan all images in a namespace
kubectl get pods -n production -o json | \
  jq -r '.items[].spec.containers[].image' | \
  sort -u | \
  while read img; do
    echo "=== $img ==="; trivy image --severity HIGH,CRITICAL "$img"
  done
```

### Admission: Deny Latest Tag

```yaml
# Kyverno policy: require image tags (no :latest)
apiVersion: kyverno.io/v1
kind: ClusterPolicy
metadata:
  name: disallow-latest-tag
spec:
  validationFailureAction: enforce
  rules:
  - name: require-image-tag
    match:
      any:
      - resources:
          kinds: ["Pod"]
    validate:
      message: "Image tag 'latest' is not allowed."
      pattern:
        spec:
          containers:
          - image: "!*:latest"
```

---

## Audit Logging

```yaml
# API server audit policy (kube-apiserver flag: --audit-policy-file)
apiVersion: audit.k8s.io/v1
kind: Policy
rules:
# Log secret access at RequestResponse level
- level: RequestResponse
  resources:
  - group: ""
    resources: ["secrets"]

# Log pod exec/attach at Metadata level
- level: Metadata
  verbs: ["exec", "attach", "portforward"]

# Minimal logging for read-only operations
- level: None
  verbs: ["get", "list", "watch"]
  users: ["system:serviceaccount:monitoring:prometheus"]
```

---

## CIS Benchmark Quick Checks

```bash
# Run kube-bench (CIS Benchmark scan)
kubectl apply -f https://raw.githubusercontent.com/aquasecurity/kube-bench/main/job.yaml

# View results
kubectl logs -l app=kube-bench -n default

# Or run locally on a node
kube-bench run --targets=master
kube-bench run --targets=node
```

---

## Anti-Patterns

1. **`cluster-admin` ClusterRoleBinding for application service accounts** — gives full cluster control; use namespace-scoped Role with specific permissions.
2. **Storing secrets in environment variables in plaintext** — visible in pod specs and logs; use secretKeyRef or external-secrets.
3. **No NetworkPolicy (default allow-all)** — any compromised pod can reach any other; deploy default-deny + explicit allow rules.
4. **`privileged: true` containers** — full host access; only needed for DaemonSets with kernel modules; avoid in application pods.
5. **`automountServiceAccountToken: true` on all SAs** — default true; tokens can be stolen and used for API access.
6. **No image vulnerability scanning in CI** — CVEs discovered in production after incident; scan in CI pipeline and block CRITICAL findings.

---

## References

- Pod Security Standards: `kubernetes.io/docs/concepts/security/pod-security-standards/`
- RBAC: `kubernetes.io/docs/reference/access-authn-authz/rbac/`
- Network Policies: `kubernetes.io/docs/concepts/services-networking/network-policies/`
- External Secrets Operator: `external-secrets.io/docs/`
- kube-bench: `github.com/aquasecurity/kube-bench`
- Related skills: `[[infra-rbac-audit]]`, `[[infra-secrets-management-review]]`, `[[infra-network-security-review]]`
