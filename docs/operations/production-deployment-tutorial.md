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

### Using Helm (Recommended)

```bash
# Add the repository (if published) or use local chart
helm install ledger ./deploy/helm/inferadb-ledger \
  --set replicaCount=3 \
  --set persistence.size=50Gi \
  --set resources.requests.memory=8Gi \
  --set resources.requests.cpu=4
```

**Note**: `replicaCount` must be odd (1, 3, 5, 7) for Raft quorum. See `values.yaml` for all available options.

### Using Raw Manifests

Create the following resources:

**ConfigMap:**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ledger-config
  namespace: inferadb
data:
  INFERADB__LEDGER__LISTEN: "0.0.0.0:50051"
  INFERADB__LEDGER__METRICS: "0.0.0.0:9090"
  INFERADB__LEDGER__DATA: "/data"
  INFERADB__LEDGER__CLUSTER: "3"
  INFERADB__LEDGER__PEERS: "ledger-headless.inferadb.svc.cluster.local"
  INFERADB__LEDGER__BATCH_SIZE: "100"
  INFERADB__LEDGER__BATCH_DELAY: "0.005"
```

**Headless Service:**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ledger-headless
  namespace: inferadb
spec:
  clusterIP: None
  selector:
    app: ledger
  ports:
    - name: grpc
      port: 50051
      targetPort: grpc
    - name: metrics
      port: 9090
      targetPort: metrics
```

**Client Service:**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ledger
  namespace: inferadb
spec:
  selector:
    app: ledger
  ports:
    - name: grpc
      port: 50051
      targetPort: grpc
```

**StatefulSet:**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ledger
  namespace: inferadb
spec:
  serviceName: ledger-headless
  replicas: 3
  selector:
    matchLabels:
      app: ledger
  template:
    metadata:
      labels:
        app: ledger
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: ledger
              topologyKey: kubernetes.io/hostname
      containers:
        - name: ledger
          image: inferadb/ledger:latest
          ports:
            - name: grpc
              containerPort: 50051
            - name: metrics
              containerPort: 9090
          envFrom:
            - configMapRef:
                name: ledger-config
          volumeMounts:
            - name: data
              mountPath: /data
          resources:
            requests:
              memory: "8Gi"
              cpu: "4"
            limits:
              memory: "16Gi"
              cpu: "8"
          livenessProbe:
            grpc:
              port: 50051
            initialDelaySeconds: 10
            periodSeconds: 10
          readinessProbe:
            grpc:
              port: 50051
            initialDelaySeconds: 5
            periodSeconds: 5
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 50Gi
```

**PodDisruptionBudget:**

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: ledger-pdb
  namespace: inferadb
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: ledger
```

Apply the manifests:

```bash
kubectl apply -f ledger-config.yaml
kubectl apply -f ledger-headless-service.yaml
kubectl apply -f ledger-client-service.yaml
kubectl apply -f ledger-statefulset.yaml
kubectl apply -f ledger-pdb.yaml
```

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

**Important**: The examples above deploy Ledger without TLS for simplicity. For production deployments, you must secure your cluster.

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

See [Security Guide](security.md) for:

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
          expr: histogram_quantile(0.99, rate(ledger_write_latency_seconds_bucket[5m])) > 0.1
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
  -d '{"name": "production"}' \
  localhost:50051 ledger.v1.AdminService/CreateOrganization

# Create vault (note the organization_slug from previous response)
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/CreateVault
```

## 7. Configure Client Access

### Internal Access (within cluster)

Services within the cluster connect to:

```
ledger.inferadb.svc.cluster.local:50051
```

### External Access (Ingress)

For external gRPC access, use an Ingress with gRPC support:

**NGINX Ingress:**

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

**AWS ALB (via ALB Ingress Controller):**

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ledger
  namespace: inferadb
  annotations:
    kubernetes.io/ingress.class: alb
    alb.ingress.kubernetes.io/scheme: internet-facing
    alb.ingress.kubernetes.io/target-type: ip
    alb.ingress.kubernetes.io/backend-protocol-version: GRPC
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
```

## 8. Setup Backups

### Manual Snapshot

```bash
kubectl exec ledger-0 -- grpcurl -plaintext localhost:50051 \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}}' \
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
  -d '{"organization_slug": {"id": "1"}, "client_id": {"id": "test"}, "sequence": "1", "operations": [{"set_entity": {"key": "test:deploy", "value": "dGVzdA=="}}]}' \
  localhost:50051 ledger.v1.WriteService/Write

echo "=== Read Test ==="
kubectl exec ledger-0 -- grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "key": "test:deploy"}' \
  localhost:50051 ledger.v1.ReadService/Read
```

## Next Steps

- [Alerting Guide](alerting.md) - Configure PagerDuty/Slack notifications
- [Rolling Upgrade](runbooks/rolling-upgrade.md) - Upgrade procedure
- [Disaster Recovery](runbooks/disaster-recovery.md) - Recovery procedures
- [Capacity Planning](capacity-planning.md) - Sizing guidelines
