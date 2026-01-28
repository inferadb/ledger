# HealthService API

Liveness and readiness checks for Ledger nodes.

## Service Definition

```protobuf
service HealthService {
  rpc Check(HealthCheckRequest) returns (HealthCheckResponse);
}
```

## Check

Returns the health status of the node and optionally specific vaults.

```bash
# Basic health check
grpcurl -plaintext localhost:50051 ledger.v1.HealthService/Check

# Check specific vault health
grpcurl -plaintext \
  -d '{"vault_checks": [{"namespace_id": {"id": "1"}, "vault_id": {"id": "1"}}]}' \
  localhost:50051 ledger.v1.HealthService/Check
```

**Request:**

| Field          | Type         | Description                         |
| -------------- | ------------ | ----------------------------------- |
| `vault_checks` | VaultCheck[] | (Optional) Specific vaults to check |

**Response:**

| Field          | Type          | Description                     |
| -------------- | ------------- | ------------------------------- |
| `status`       | HealthStatus  | Overall node health             |
| `vault_health` | VaultHealth[] | Per-vault health (if requested) |

**HealthStatus values:**

| Value       | Description                                   |
| ----------- | --------------------------------------------- |
| `HEALTHY`   | Node is operational and accepting requests    |
| `DEGRADED`  | Node is operational but some vaults unhealthy |
| `UNHEALTHY` | Node cannot serve requests                    |
| `STARTING`  | Node is starting up, not yet ready            |

**VaultHealth fields:**

| Field          | Type        | Description                       |
| -------------- | ----------- | --------------------------------- |
| `namespace_id` | NamespaceId | Vault's namespace                 |
| `vault_id`     | VaultId     | Vault identifier                  |
| `status`       | VaultStatus | Vault health status               |
| `is_leader`    | bool        | Whether this node is vault leader |
| `height`       | uint64      | Current block height              |

## Kubernetes Integration

### Liveness Probe

```yaml
livenessProbe:
  grpc:
    port: 50051
    service: ledger.v1.HealthService
  initialDelaySeconds: 10
  periodSeconds: 10
```

### Readiness Probe

```yaml
readinessProbe:
  grpc:
    port: 50051
    service: ledger.v1.HealthService
  initialDelaySeconds: 5
  periodSeconds: 5
```

For older Kubernetes versions without native gRPC probes:

```yaml
livenessProbe:
  exec:
    command:
      - grpcurl
      - -plaintext
      - localhost:50051
      - ledger.v1.HealthService/Check
  initialDelaySeconds: 10
  periodSeconds: 10
```

## HTTP Health Endpoint

Ledger also exposes an HTTP health endpoint for simpler integrations:

```bash
curl http://localhost:9090/health
```

Response:

```json
{ "status": "healthy" }
```

HTTP status codes:

| Code | Meaning               |
| ---- | --------------------- |
| 200  | Healthy               |
| 503  | Unhealthy or starting |

## Load Balancer Configuration

### AWS ALB/NLB

```yaml
targetGroupAttributes:
  - key: deregistration_delay.timeout_seconds
    value: "30"
healthCheck:
  protocol: HTTP
  port: "9090"
  path: /health
  interval: 10
  timeout: 5
  healthyThreshold: 2
  unhealthyThreshold: 3
```

### HAProxy

```
backend ledger
  option httpchk GET /health
  http-check expect status 200
  server ledger-0 ledger-0:50051 check port 9090
  server ledger-1 ledger-1:50051 check port 9090
  server ledger-2 ledger-2:50051 check port 9090
```
