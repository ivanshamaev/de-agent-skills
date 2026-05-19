---
name: infra-gitops-deployment-review
description: GitOps deployment review — ArgoCD Application/AppProject CRDs, app-of-apps pattern, sync policies (automated+self-heal+prune), drift detection and remediation, FluxCD HelmRelease/Kustomization, git repository structure for GitOps (app code vs config repo separation), image update automation, progressive delivery (Argo Rollouts canary/blue-green), GitOps for data platform components (Airflow/Kafka/Spark)
---

# GitOps Deployment Review

## When to Use

- Setting up ArgoCD or FluxCD for a data platform
- Reviewing GitOps repo structure and separation of concerns
- Diagnosing drift between Git desired state and cluster actual state
- Implementing progressive delivery (canary/blue-green) for pipeline components
- Onboarding a new team to GitOps workflow

---

## Repository Structure

### Config Repo Separation

```
# app-code-repo/    — application source, Dockerfiles, unit tests
# gitops-config-repo/ — K8s manifests only, no app code

gitops-config-repo/
  apps/                         # ArgoCD App-of-Apps root
    production/
      airflow.yaml
      kafka.yaml
      spark-history.yaml
    staging/
      airflow.yaml
  base/                         # Kustomize base manifests
    airflow/
      deployment.yaml
      service.yaml
      kustomization.yaml
  overlays/                     # environment-specific patches
    production/
      airflow/
        kustomization.yaml      # patches: replicas, image tag, resources
        patch-replicas.yaml
    staging/
      airflow/
        kustomization.yaml
  helm/                         # Helm values per environment
    airflow/
      values-prod.yaml
      values-staging.yaml
```

---

## ArgoCD — Application CRD

### Basic Application

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: airflow-production
  namespace: argocd
  finalizers:
    - resources-finalizer.argocd.argoproj.io  # cascade delete on app removal
spec:
  project: data-platform

  source:
    repoURL: https://github.com/my-org/gitops-config
    targetRevision: main       # or specific tag: v1.2.3
    path: overlays/production/airflow

  destination:
    server: https://kubernetes.default.svc
    namespace: airflow

  syncPolicy:
    automated:
      prune: true              # delete resources removed from Git
      selfHeal: true           # revert manual cluster changes
      allowEmpty: false        # never sync if source results in empty manifests
    syncOptions:
      - CreateNamespace=true
      - PrunePropagationPolicy=foreground
      - RespectIgnoreDifferences=true
    retry:
      limit: 5
      backoff:
        duration: 5s
        factor: 2
        maxDuration: 3m

  ignoreDifferences:
    - group: apps
      kind: Deployment
      jsonPointers:
        - /spec/replicas        # HPA manages replicas — ignore diff
    - group: ""
      kind: ConfigMap
      name: airflow-config
      jsonPointers:
        - /data/airflow.cfg     # airflow generates this at runtime
```

### Helm Application

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: kafka-production
  namespace: argocd
spec:
  project: data-platform
  source:
    repoURL: https://charts.bitnami.com/bitnami
    chart: kafka
    targetRevision: "26.6.2"    # pin exact chart version
    helm:
      releaseName: kafka
      valuesObject:             # inline values
        replicaCount: 3
        persistence:
          size: 500Gi
          storageClass: gp3
      valueFiles:
        - values-prod.yaml      # from source repo at path
  destination:
    server: https://kubernetes.default.svc
    namespace: kafka
```

---

## App-of-Apps Pattern

```yaml
# apps/root-app.yaml — bootstraps all other apps
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: root
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/my-org/gitops-config
    targetRevision: main
    path: apps/production       # directory containing all Application YAMLs
  destination:
    server: https://kubernetes.default.svc
    namespace: argocd
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

```yaml
# apps/production/airflow.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: airflow
  namespace: argocd
spec:
  project: data-platform
  source:
    repoURL: https://github.com/my-org/gitops-config
    targetRevision: main
    path: helm/airflow
    helm:
      valueFiles: [values-prod.yaml]
  destination:
    server: https://kubernetes.default.svc
    namespace: airflow
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

---

## AppProject — RBAC and Scope Control

```yaml
apiVersion: argoproj.io/v1alpha1
kind: AppProject
metadata:
  name: data-platform
  namespace: argocd
spec:
  description: "Data platform workloads — Airflow, Kafka, Spark"

  sourceRepos:
    - https://github.com/my-org/gitops-config
    - https://charts.bitnami.com/bitnami
    - https://helm.releases.hashicorp.com

  destinations:
    - server: https://kubernetes.default.svc
      namespace: airflow
    - server: https://kubernetes.default.svc
      namespace: kafka
    - server: https://kubernetes.default.svc
      namespace: spark

  clusterResourceWhitelist:
    - group: ""
      kind: Namespace
    - group: rbac.authorization.k8s.io
      kind: ClusterRole

  namespaceResourceBlacklist:
    - group: ""
      kind: ResourceQuota     # only platform team can set quotas

  roles:
    - name: developer
      description: "Can sync apps but not delete"
      policies:
        - p, proj:data-platform:developer, applications, get, data-platform/*, allow
        - p, proj:data-platform:developer, applications, sync, data-platform/*, allow
      groups:
        - data-engineering-team
```

---

## Drift Detection and Remediation

```bash
# Check all app sync status
argocd app list

# Detailed sync status for an app
argocd app get airflow-production

# Show diff between Git desired state and cluster actual state
argocd app diff airflow-production

# Force sync (overwrite manual changes)
argocd app sync airflow-production --force

# Hard refresh (bypass cache, re-evaluate manifests)
argocd app get airflow-production --hard-refresh

# Sync specific resources only
argocd app sync airflow-production --resource apps:Deployment:airflow-scheduler
```

### Drift Detection Script

```bash
#!/bin/bash
# Report all apps with drift
argocd app list -o json | \
  jq -r '.[] | select(.status.sync.status != "Synced") |
  "\(.metadata.name): sync=\(.status.sync.status) health=\(.status.health.status)"'
```

---

## FluxCD Alternative

### HelmRelease

```yaml
apiVersion: helm.toolkit.fluxcd.io/v2beta2
kind: HelmRelease
metadata:
  name: airflow
  namespace: airflow
spec:
  interval: 10m
  chart:
    spec:
      chart: airflow
      version: "1.13.*"          # semver range — auto-upgrade patch
      sourceRef:
        kind: HelmRepository
        name: apache-airflow
        namespace: flux-system
  values:
    executor: KubernetesExecutor
    workers:
      replicas: 0
  upgrade:
    remediation:
      retries: 3
      strategy: rollback
  rollback:
    timeout: 5m
    cleanupOnFail: true
```

### Image Update Automation

```yaml
# Flux: automatically update image tag when new image is pushed
apiVersion: image.toolkit.fluxcd.io/v1beta2
kind: ImageUpdateAutomation
metadata:
  name: airflow-image-update
  namespace: flux-system
spec:
  interval: 5m
  sourceRef:
    kind: GitRepository
    name: gitops-config
  git:
    checkout:
      ref:
        branch: main
    commit:
      author:
        email: fluxcdbot@my-org.com
        name: FluxCD Bot
      messageTemplate: "ci: auto-update airflow image to {{range .Updated.Images}}{{.}}{{end}}"
    push:
      branch: main
  update:
    path: ./overlays/production/airflow
    strategy: Setters
```

---

## Argo Rollouts — Progressive Delivery

```yaml
# Canary deployment for Airflow API server
apiVersion: argoproj.io/v1alpha1
kind: Rollout
metadata:
  name: airflow-webserver
  namespace: airflow
spec:
  replicas: 5
  selector:
    matchLabels:
      app: airflow-webserver
  template:
    spec:
      containers:
      - name: webserver
        image: apache/airflow:2.8.0
  strategy:
    canary:
      steps:
      - setWeight: 20          # 20% traffic to new version
      - pause: {duration: 5m}  # wait 5 min, check metrics
      - setWeight: 50
      - pause: {duration: 5m}
      - setWeight: 100         # full rollout
      canaryService: airflow-webserver-canary
      stableService: airflow-webserver-stable
      trafficRouting:
        nginx:
          stableIngress: airflow-ingress
```

---

## Anti-Patterns

1. **Storing secrets in Git** — even encrypted secrets rotate manually; use External Secrets Operator + Vault/SSM + reference in Git, not the secret value.
2. **Pointing ArgoCD at `main` branch HEAD** — config changes deploy instantly without review; use a `release` branch or tag promotion workflow.
3. **No `ignoreDifferences` for HPA-managed replicas** — ArgoCD resets replica count every sync cycle; always ignore `/spec/replicas` for HPA-scaled deployments.
4. **App-of-apps without AppProject scoping** — any app can deploy to any namespace; always define AppProject with explicit destination namespaces.
5. **`prune: true` without testing in staging** — accidental resource deletion on first sync; enable pruning only after the config repo is trusted.
6. **Applying ArgoCD YAML with kubectl instead of ArgoCD CLI** — bypasses ArgoCD reconciliation; always manage Application resources through ArgoCD itself.

---

## References

- ArgoCD: `argo-cd.readthedocs.io/en/stable/`
- Argo Rollouts: `argo-rollouts.readthedocs.io/en/stable/`
- FluxCD: `fluxcd.io/flux/`
- GitOps principles: `opengitops.dev`
- Related skills: `[[github-actions-dataops]]`, `[[infra-kubernetes-security-audit]]`, `[[infra-terraform-review]]`
