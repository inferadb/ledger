# InferaDB Ledger Helm Chart

Helm chart for deploying InferaDB Ledger on Kubernetes.

## Prerequisites

- Kubernetes 1.25+
- Helm 3.0+
- PersistentVolume provisioner (for persistence)

## Installation

```bash
# Add the repository (when published)
# helm repo add inferadb https://charts.inferadb.com
# helm repo update

# Install from local chart
helm install ledger ./deploy/helm/inferadb-ledger \
  --namespace inferadb \
  --create-namespace
```

## Configuration

See `values.yaml` for all available options.

### Common Configurations

#### Production 5-node cluster

```yaml
replicaCount: 5

resources:
  requests:
    cpu: "1"
    memory: "2Gi"
  limits:
    cpu: "4"
    memory: "8Gi"

persistence:
  size: 100Gi
  storageClass: fast-ssd
```

#### Development single-node

```yaml
replicaCount: 1

resources:
  requests:
    cpu: "100m"
    memory: "256Mi"
  limits:
    cpu: "500m"
    memory: "1Gi"

persistence:
  size: 1Gi

podDisruptionBudget:
  enabled: false
```

#### Enable Prometheus ServiceMonitor

```yaml
serviceMonitor:
  enabled: true
  interval: 15s
  labels:
    release: prometheus  # Match your Prometheus Operator selector
```

## Upgrade

```bash
helm upgrade ledger ./deploy/helm/inferadb-ledger \
  --namespace inferadb
```

## Uninstall

```bash
helm uninstall ledger --namespace inferadb

# PVCs are not deleted by default (data safety)
# To remove data:
kubectl delete pvc -l app.kubernetes.io/name=inferadb-ledger -n inferadb
```

## Values Reference

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas (must be odd) | `3` |
| `image.repository` | Image repository | `inferadb/ledger` |
| `image.tag` | Image tag | Chart appVersion |
| `persistence.enabled` | Enable persistent storage | `true` |
| `persistence.size` | PVC size | `10Gi` |
| `persistence.storageClass` | Storage class | `""` (default) |
| `resources.requests.cpu` | CPU request | `250m` |
| `resources.requests.memory` | Memory request | `512Mi` |
| `resources.limits.cpu` | CPU limit | `2` |
| `resources.limits.memory` | Memory limit | `4Gi` |
| `podDisruptionBudget.enabled` | Enable PDB | `true` |
| `podDisruptionBudget.maxUnavailable` | Max unavailable pods | `1` |
| `serviceMonitor.enabled` | Enable Prometheus ServiceMonitor | `false` |
