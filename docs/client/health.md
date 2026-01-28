[Documentation](../README.md) > Client API > HealthService

# HealthService API

Liveness and readiness checks for Ledger nodes.

## Service Definition

```protobuf
service HealthService {
  rpc Check(HealthCheckRequest) returns (HealthCheckResponse);
}
```

## Check

Returns the health status of the node, or a specific vault if specified.

```bash
# Basic node health check
grpcurl -plaintext localhost:50051 ledger.v1.HealthService/Check

# Check specific vault health
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}, "vault_id": {"id": "1"}}' \
  localhost:50051 ledger.v1.HealthService/Check
```

**Request:**

| Field          | Type        | Description                          |
| -------------- | ----------- | ------------------------------------ |
| `namespace_id` | NamespaceId | (Optional) Namespace for vault check |
| `vault_id`     | VaultId     | (Optional) Specific vault to check   |

If both `namespace_id` and `vault_id` are omitted, returns overall node health.

**Response:**

| Field     | Type                | Description                   |
| --------- | ------------------- | ----------------------------- |
| `status`  | HealthStatus        | Health status                 |
| `message` | string              | Human-readable status message |
| `details` | map<string, string> | Additional diagnostic details |

**HealthStatus values:**

| Value                       | Description                 |
| --------------------------- | --------------------------- |
| `HEALTH_STATUS_UNSPECIFIED` | Status unknown              |
| `HEALTH_STATUS_HEALTHY`     | Node/vault is operational   |
| `HEALTH_STATUS_DEGRADED`    | Operational but with issues |
| `HEALTH_STATUS_UNAVAILABLE` | Not accepting requests      |

**Example response:**

```json
{
  "status": "HEALTH_STATUS_HEALTHY",
  "message": "Node is healthy",
  "details": {
    "leader": "true",
    "term": "42",
    "commit_index": "12345"
  }
}
```

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

## Vault Health Status

When checking vault health via `AdminService/GetVault` or `AdminService/RecoverVault`, the `VaultHealthProto` enum indicates vault state:

| Value                           | Description                               |
| ------------------------------- | ----------------------------------------- |
| `VAULT_HEALTH_PROTO_HEALTHY`    | Vault is operational                      |
| `VAULT_HEALTH_PROTO_DIVERGED`   | State divergence detected, needs recovery |
| `VAULT_HEALTH_PROTO_RECOVERING` | Recovery in progress                      |

See [Vault Repair](../operations/vault-repair.md) for recovery procedures.
