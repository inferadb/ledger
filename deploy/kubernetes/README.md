# Kubernetes Deployment

Basic Kubernetes manifests for deploying InferaDB Ledger.

## Quick Start

```bash
# Deploy to Kubernetes
kubectl apply -k deploy/kubernetes/

# Check status
kubectl -n inferadb get pods
kubectl -n inferadb get pvc

# View logs
kubectl -n inferadb logs -f ledger-0
```

## Components

| File              | Description                                       |
| ----------------- | ------------------------------------------------- |
| namespace.yaml    | Dedicated namespace for isolation                 |
| service.yaml      | Headless service (peer discovery) + client access |
| statefulset.yaml  | StatefulSet with PVC for persistent storage       |
| pdb.yaml          | PodDisruptionBudget to maintain Raft quorum       |
| kustomization.yaml| Kustomize configuration                           |

## How Discovery Works

1. StatefulSet creates pods: `ledger-0`, `ledger-1`, `ledger-2`
2. Headless service `ledger` creates DNS A records for each pod IP
3. Pods query `ledger.inferadb.svc.cluster.local` to discover all peer IPs
4. Bootstrap coordination uses Snowflake IDs to elect bootstrap leader

## Customization

### Change Replica Count

Edit `statefulset.yaml`:

```yaml
spec:
  replicas: 5  # Must be odd for Raft quorum
```

Update `INFERADB__LEDGER__BOOTSTRAP_EXPECT` to match.

### Change Storage

Edit the `volumeClaimTemplates` in `statefulset.yaml`:

```yaml
volumeClaimTemplates:
  - spec:
      storageClassName: fast-ssd  # Your storage class
      resources:
        requests:
          storage: 100Gi
```

### Use Custom Image

With kustomize:

```bash
kubectl apply -k deploy/kubernetes/ --set-image inferadb/ledger=myregistry/ledger:v1.0.0
```

Or edit `kustomization.yaml`:

```yaml
images:
  - name: inferadb/ledger
    newName: myregistry/ledger
    newTag: v1.0.0
```

## Production Considerations

For production deployments, use the Helm chart in `deploy/helm/` which provides:

- Configurable values
- Resource tuning
- TLS configuration
- ServiceMonitor for Prometheus Operator
- Ingress/Gateway support
