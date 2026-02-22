# Security

Ledger's security model and deployment considerations.

## Design Philosophy

Ledger is designed to run **within a secure network perimeter**. It does not include built-in authentication or authorization because it operates as an internal service accessed only by trusted components (Engine and Control).

### Trust Model

```
┌─────────────────────────────────────────────────────────────┐
│                    Secure Network Boundary                  │
│                  (WireGuard, VPC, or similar)               │
│                                                             │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐   │
│   │   Engine    │────▶│   Ledger    │◀────│   Control   │   │
│   │  (trusted)  │     │  (no auth)  │     │  (trusted)  │   │
│   └─────────────┘     └─────────────┘     └─────────────┘   │
│                              │                              │
│                              ▼                              │
│                      ┌─────────────┐                        │
│                      │  Ledger     │                        │
│                      │  Replicas   │                        │
│                      └─────────────┘                        │
└─────────────────────────────────────────────────────────────┘
```

| Component        | Trust Level      | Access                            |
| ---------------- | ---------------- | --------------------------------- |
| Engine           | Fully trusted    | Read/Write to any vault           |
| Control          | Fully trusted    | Admin operations, user management |
| Ledger nodes     | Mutually trusted | Raft consensus                    |
| External clients | Not supported    | Must go through Engine/Control    |

## Network Security

### Recommended: WireGuard Tunnel

Deploy Ledger nodes within a WireGuard mesh network:

```bash
# Example WireGuard config for Ledger node
[Interface]
Address = 10.0.0.1/24
PrivateKey = <node_private_key>
ListenPort = 51820

[Peer]
PublicKey = <peer1_public_key>
AllowedIPs = 10.0.0.2/32
Endpoint = peer1.example.com:51820

[Peer]
PublicKey = <peer2_public_key>
AllowedIPs = 10.0.0.3/32
Endpoint = peer2.example.com:51820
```

Ledger then binds to the WireGuard interface:

```bash
INFERADB__LEDGER__LISTEN=10.0.0.1:50051 inferadb-ledger --cluster 3
```

### Alternative: VPC/Private Network

In cloud environments, use VPC peering or private subnets:

| Cloud | Mechanism                                 |
| ----- | ----------------------------------------- |
| AWS   | VPC with private subnets, Security Groups |
| GCP   | VPC with firewall rules                   |
| Azure | VNet with NSGs                            |

### Kubernetes Network Policies

When running in Kubernetes, restrict traffic to Engine and Control pods:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: ledger-ingress
spec:
  podSelector:
    matchLabels:
      app: ledger
  policyTypes:
    - Ingress
  ingress:
    # Allow from Engine
    - from:
        - podSelector:
            matchLabels:
              app: engine
      ports:
        - protocol: TCP
          port: 50051
    # Allow from Control
    - from:
        - podSelector:
            matchLabels:
              app: control
      ports:
        - protocol: TCP
          port: 50051
    # Allow inter-node Raft traffic
    - from:
        - podSelector:
            matchLabels:
              app: ledger
      ports:
        - protocol: TCP
          port: 50051
```

## TLS Configuration

While Ledger doesn't require TLS within a trusted network, you can enable it for defense in depth.

### Server-Side TLS

Ledger uses gRPC's native TLS support. Configure via environment:

```bash
# Server TLS (if implemented in your deployment wrapper)
INFERADB__LEDGER__TLS_CERT=/path/to/server.crt
INFERADB__LEDGER__TLS_KEY=/path/to/server.key
```

For Kubernetes, use a TLS-terminating sidecar or service mesh:

```yaml
# Istio example - automatic mTLS
apiVersion: security.istio.io/v1beta1
kind: PeerAuthentication
metadata:
  name: ledger-mtls
spec:
  selector:
    matchLabels:
      app: ledger
  mtls:
    mode: STRICT
```

### Client-Side TLS (SDK)

The Rust SDK supports TLS connections:

```rust
use inferadb_ledger_sdk::{Client, ClientConfig, TlsConfig};

let tls = TlsConfig::builder()
    .ca_cert("/path/to/ca.crt")
    .build()?;

let client = Client::connect(
    ClientConfig::builder()
        .endpoints(vec!["https://ledger.internal:50051".into()])
        .tls(tls)
        .build()
).await?;
```

## Threat Model

### In-Scope Threats

| Threat                | Mitigation                                              |
| --------------------- | ------------------------------------------------------- |
| Network eavesdropping | Deploy within encrypted tunnel (WireGuard)              |
| Unauthorized access   | Network-level isolation; no public exposure             |
| Data corruption       | Cryptographic verification (state roots, Merkle proofs) |
| Node compromise       | Raft quorum prevents single-node attacks                |
| Replay attacks        | Transaction sequence numbers                            |

### Out-of-Scope Threats

| Threat                         | Rationale                                                                                                  |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| Full Byzantine fault tolerance | Trusted network assumption; see [Raft Message Validation](#raft-message-validation) for defensive measures |
| DDoS protection                | Internal service; external traffic blocked at perimeter                                                    |
| Client authentication          | Engine/Control handle user authentication                                                                  |
| Audit logging for compliance   | Cryptographic chain is the audit log                                                                       |

### Raft Message Validation

Ledger uses Raft (a crash-fault-tolerant protocol) for consensus. While Raft assumes honest participants, the implementation defensively validates all incoming Raft messages to prevent state corruption from misconfigured nodes, software bugs, or network corruption.

**Validated message classes:**

| Category                       | Validation                                              | Behavior on Invalid Input                 |
| ------------------------------ | ------------------------------------------------------- | ----------------------------------------- |
| Malformed vote requests        | Missing or empty vote fields                            | Returns error or rejects vote             |
| Malformed append entries       | Missing leader vote, empty entries                      | Rejects append; no log mutation           |
| Stale term messages            | Term lower than current                                 | Ignores message (Raft term check)         |
| Conflicting prev_log_id        | References non-existent log position                    | Rejects append; triggers resync           |
| Corrupted snapshot data        | Invalid or truncated snapshot bytes                     | Rejects snapshot installation             |
| Forged snapshot membership     | Membership config referencing unknown nodes             | Rejects snapshot                          |
| Snapshot with future log index | Log index beyond any committed entry                    | Rejects snapshot                          |
| Replay attacks                 | Re-sent entries with old terms or stale committed index | Ignored by term/index monotonicity checks |
| Invalid shard routing          | Requests targeting non-existent shard IDs               | Returns NOT_FOUND status                  |
| Oversized chunks               | Snapshot chunks exceeding expected bounds               | Processed without buffer overflow         |

**Key properties:**

- No invalid Raft message corrupts the log, state machine, or committed data
- The node remains a functioning cluster member after receiving any malformed message
- Stale-term messages are silently dropped per the Raft protocol
- Multi-shard routing validates shard existence before forwarding

These properties are verified by 22 Byzantine fault tests in `crates/raft/src/services/raft.rs`.

### Security Boundaries

```
┌─────────────────────────────────────────────────────────────────┐
│                         _system organization                       │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  User accounts, organization routing, global metadata      │   │
│   │  Accessible only via Control (trusted)                  │   │
│   └─────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│                         Organizations                             │
│     ┌──────────────┐   ┌──────────────┐   ┌──────────────┐      │
│     │  Org A       │   │  Org B       │   │  Org C       │      │
│     │  (isolated)  │   │  (isolated)  │   │  (isolated)  │      │
│     └──────────────┘   └──────────────┘   └──────────────┘      │
└─────────────────────────────────────────────────────────────────┘
```

**Isolation guarantees:**

- Organizations cannot access each other's data
- Vaults within an organization are cryptographically independent
- Shard assignment is transparent to clients

## Operational Security

### Secrets Management

Store sensitive configuration in a secrets manager:

```yaml
# Kubernetes Secret
apiVersion: v1
kind: Secret
metadata:
  name: ledger-config
type: Opaque
stringData:
  # No secrets required for Ledger itself
  # TLS certs if using service mesh
```

### Audit Trail

Every write operation is recorded in the cryptographic chain:

- **Transaction ID**: Unique identifier
- **Actor**: Server-assigned from auth context (`system:control`, `system:engine`)
- **Timestamp**: Block timestamp
- **Operations**: All changes in the transaction

Query audit history via `WatchBlocks` subscription or historical reads.

### Incident Response

If a node is suspected compromised:

1. **Isolate**: Remove from network (firewall/WireGuard)
2. **Verify**: Check state roots against healthy nodes
3. **Replace**: Start fresh node, let it sync from quorum
4. **Investigate**: Analyze logs and metrics

```bash
# Check if node has diverged
grpcurl -plaintext node:50051 ledger.v1.HealthService/Check

# Compare state roots across nodes
for node in node1 node2 node3; do
  grpcurl -plaintext $node:50051 ledger.v1.ReadService/GetTip
done
```

## Hardening Checklist

- [ ] Deploy within WireGuard tunnel or VPC
- [ ] Block all external traffic to Ledger ports
- [ ] Enable Kubernetes NetworkPolicy
- [ ] Use non-root container user (distroless default)
- [ ] Mount data directory as dedicated volume
- [ ] Enable metrics for monitoring
- [ ] Configure log aggregation
- [ ] Test backup/restore procedures
- [ ] Document incident response plan
