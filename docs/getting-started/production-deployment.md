# Production Deployment Tutorial

Step-by-step guide to deploy Ledger on Kubernetes with monitoring and alerting.

## Prerequisites

- Kubernetes cluster (1.24+)
- kubectl configured
- Helm 3.x installed
- 3 worker nodes (recommended for node anti-affinity)
- Persistent storage provisioner (e.g., EBS, GCE PD, local-path)

## 1. Create Namespace

```bash
kubectl create namespace inferadb
kubectl config set-context --current --namespace=inferadb
```

## 2. Deploy Ledger

```bash
helm install ledger ./deploy/helm/inferadb-ledger \
  --set replicaCount=3 \
  --set persistence.size=50Gi \
  --set resources.requests.memory=8Gi \
  --set resources.requests.cpu=4
```

`replicaCount` must be odd (1, 3, 5, 7) for Raft quorum. See `values.yaml` for all available options.

If you can't use Helm (air-gapped registry, stricter manifests policy, Kustomize-based pipeline), see [`how-to/deployment.md § Raw Manifests`](../how-to/deployment.md#raw-manifests-alternative-to-helm) and [`§ Kustomize`](../how-to/deployment.md#kustomize) for the alternative packaging paths.

## 3. Verify Deployment

Wait for pods to be ready:

```bash
kubectl rollout status statefulset/ledger
```

Check cluster health:

```bash
# Port-forward to a pod
kubectl port-forward svc/ledger 50051:50051 &

# Check health
grpcurl -plaintext localhost:50051 ledger.v1.HealthService/Check

# Check cluster info
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/GetClusterInfo
```

Expected output shows 3 members and one leader.

## 4. Security Considerations

**Important**: The examples above omit TLS to keep the tutorial's happy path short. Real production deployments must add TLS — typically at the service-mesh or ingress layer rather than in Ledger itself (Ledger trusts its network perimeter by design; see [architecture/security.md](../architecture/security.md)).

### Network Security

At minimum, ensure:

1. **Network isolation**: Deploy Ledger in a private subnet/VPC not accessible from the internet
2. **NetworkPolicy**: Restrict pod-to-pod communication

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: ledger-network-policy
  namespace: inferadb
spec:
  podSelector:
    matchLabels:
      app: ledger
  policyTypes:
    - Ingress
    - Egress
  ingress:
    # Allow from other Ledger pods (peer communication)
    - from:
        - podSelector:
            matchLabels:
              app: ledger
      ports:
        - port: 50051
    # Allow from authorized clients
    - from:
        - namespaceSelector:
            matchLabels:
              name: inferadb-clients
      ports:
        - port: 50051
    # Allow Prometheus scraping
    - from:
        - namespaceSelector:
            matchLabels:
              name: monitoring
      ports:
        - port: 9090
  egress:
    # Allow DNS
    - to:
        - namespaceSelector: {}
      ports:
        - port: 53
          protocol: UDP
    # Allow peer communication
    - to:
        - podSelector:
            matchLabels:
              app: ledger
      ports:
        - port: 50051
```

### Service Mesh (Recommended)

For mTLS between pods, use a service mesh like Istio or Linkerd:

```yaml
# Istio PeerAuthentication - require mTLS for Ledger
apiVersion: security.istio.io/v1beta1
kind: PeerAuthentication
metadata:
  name: ledger-mtls
  namespace: inferadb
spec:
  selector:
    matchLabels:
      app: ledger
  mtls:
    mode: STRICT
```

### Additional Hardening

See [Security Guide](../architecture/security.md) for:

- WireGuard VPN setup for cross-region communication
- Kubernetes RBAC configuration
- Audit logging
- Secret management

## 5. Configure Monitoring

### Deploy Prometheus ServiceMonitor

Requires Prometheus Operator installed.

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: ledger
  namespace: inferadb
  labels:
    release: prometheus # Match your Prometheus selector
spec:
  selector:
    matchLabels:
      app: ledger
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
  namespaceSelector:
    matchNames:
      - inferadb
```

### Deploy PrometheusRule for Alerts

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: ledger-alerts
  namespace: inferadb
  labels:
    release: prometheus
spec:
  groups:
    - name: ledger.critical
      rules:
        - alert: LedgerDeterminismBug
          expr: ledger_determinism_bug_total > 0
          labels:
            severity: critical
          annotations:
            summary: "Determinism bug detected"
            description: "Vault {{ $labels.vault_id }} has state divergence"

        - alert: LedgerNoLeader
          expr: sum(inferadb_ledger_raft_is_leader{namespace="inferadb"}) == 0
          for: 30s
          labels:
            severity: critical
          annotations:
            summary: "No Raft leader elected"

        - alert: LedgerNodeDown
          expr: up{job="ledger"} == 0
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "Ledger node {{ $labels.instance }} down"

    - name: ledger.warning
      rules:
        - alert: LedgerHighProposalBacklog
          expr: inferadb_ledger_raft_proposals_pending > 50
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "High Raft proposal backlog"

        - alert: LedgerHighWriteLatency
          expr: histogram_quantile(0.99, rate(ledger_grpc_request_latency_seconds_bucket{service="WriteService"}[5m])) > 0.1
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Write latency p99 > 100ms"
```

Apply monitoring resources:

```bash
kubectl apply -f ledger-servicemonitor.yaml
kubectl apply -f ledger-prometheusrule.yaml
```

## 6. Create Initial Resources

Create an organization and vault:

```bash
# Create organization
grpcurl -plaintext \
  -d '{"name": "production", "region": "REGION_US_EAST_VA"}' \
  localhost:50051 ledger.v1.AdminService/CreateOrganization

# Create vault (use the organization slug from the CreateOrganization response)
grpcurl -plaintext \
  -d '{"organization": {"slug": 1234567890}}' \
  localhost:50051 ledger.v1.AdminService/CreateVault
```

## 7. Configure Client Access

### Internal Access (within cluster)

Services within the cluster connect to:

```text
ledger.inferadb.svc.cluster.local:50051
```

### External Access (Ingress)

For external gRPC access, use an NGINX Ingress with gRPC support:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ledger
  namespace: inferadb
  annotations:
    nginx.ingress.kubernetes.io/backend-protocol: "GRPC"
spec:
  rules:
    - host: ledger.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: ledger
                port:
                  number: 50051
  tls:
    - hosts:
        - ledger.example.com
      secretName: ledger-tls
```

For AWS ALB, GKE (GCE Ingress), Azure (AGIC), or other cloud-specific ingress controllers, see [`how-to/deployment.md § Ingress alternatives`](../how-to/deployment.md#ingress-alternatives).

## 8. Setup Backups

### Manual Snapshot

```bash
kubectl exec ledger-0 -- grpcurl -plaintext localhost:50051 \
  -d '{"organization": {"slug": 1234567890}, "vault": {"slug": 7180591718400}}' \
  ledger.v1.AdminService/CreateSnapshot
```

### Automated Backups with CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: ledger-backup
  namespace: inferadb
spec:
  schedule: "0 */6 * * *" # Every 6 hours
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: backup
              image: amazon/aws-cli
              command:
                - /bin/sh
                - -c
                - |
                  # Copy snapshot to S3
                  LATEST=$(ls -t /data/snapshots/*.snap 2>/dev/null | head -1)
                  if [ -n "$LATEST" ]; then
                    aws s3 cp "$LATEST" s3://my-backup-bucket/ledger/$(date +%Y%m%d-%H%M%S).snap
                  fi
              volumeMounts:
                - name: data
                  mountPath: /data
                  readOnly: true
          volumes:
            - name: data
              persistentVolumeClaim:
                claimName: data-ledger-0
          restartPolicy: OnFailure
```

## 9. Verification Checklist

After deployment, verify:

- [ ] All 3 pods are Running and Ready
- [ ] Cluster has exactly 1 leader
- [ ] Health checks pass on all nodes
- [ ] Metrics are being scraped by Prometheus
- [ ] Alerts are configured and routing correctly
- [ ] Write operations succeed
- [ ] Read operations succeed
- [ ] Backups are being created

```bash
# Quick verification script
echo "=== Pod Status ==="
kubectl get pods -l app=ledger

echo "=== Cluster Info ==="
kubectl exec ledger-0 -- grpcurl -plaintext localhost:50051 ledger.v1.AdminService/GetClusterInfo

echo "=== Health Check ==="
kubectl exec ledger-0 -- grpcurl -plaintext localhost:50051 ledger.v1.HealthService/Check

echo "=== Write Test ==="
kubectl exec ledger-0 -- grpcurl -plaintext \
  -d '{"organization": {"slug": 1234567890}, "client_id": {"id": "test"}, "idempotency_key": "dGVzdC1rZXk=", "operations": [{"set_entity": {"key": "test:deploy", "value": "dGVzdA=="}}]}' \
  localhost:50051 ledger.v1.WriteService/Write

echo "=== Read Test ==="
kubectl exec ledger-0 -- grpcurl -plaintext \
  -d '{"organization": {"slug": 1234567890}, "key": "test:deploy"}' \
  localhost:50051 ledger.v1.ReadService/Read
```

## Next Steps

- [Alerting Guide](../reference/alerting.md) - Configure PagerDuty/Slack notifications
- [Rolling Upgrade Playbook](../playbooks/rolling-upgrade.md) - Version upgrade procedures
- [Disaster Recovery](../runbooks/disaster-recovery.md) - Recovery procedures
- [Capacity Planning](../how-to/capacity-planning.md) - Sizing guidelines
