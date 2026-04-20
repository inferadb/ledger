# Security

Ledger's security model and deployment considerations.

## Design Philosophy

Ledger is designed to run **within a secure network perimeter**. It does not authenticate incoming gRPC connections — access control is enforced at the network layer by restricting connectivity to trusted components (Engine and Control). However, Ledger serves as the **JWT signing authority** for the InferaDB platform, issuing and validating tokens consumed by Engine and Control for end-user authentication.

### Trust Model

```text
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

| Component        | Trust Level      | Access                                                     |
| ---------------- | ---------------- | ---------------------------------------------------------- |
| Engine           | Fully trusted    | Read/Write to any vault                                    |
| Control          | Fully trusted    | Admin operations, user management, token lifecycle         |
| Ledger nodes     | Mutually trusted | Raft consensus                                             |
| External clients | Not supported    | Authenticate via Ledger-issued JWTs through Engine/Control |

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
INFERADB__LEDGER__LISTEN=10.0.0.1:50051 \
INFERADB__LEDGER__JOIN=10.0.0.2:50051,10.0.0.3:50051 \
inferadb-ledger
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
use inferadb_ledger_sdk::{LedgerClient, ClientConfig, TlsConfig, ServerSource};

let tls = TlsConfig::builder()
    .ca_cert("/path/to/ca.crt")
    .build()?;

let config = ClientConfig::builder()
    .servers(ServerSource::from_static(["https://ledger.internal:50051"]))
    .client_id("my-service")
    .tls(tls)
    .build()?;

let client = LedgerClient::new(config).await?;
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
| Client authentication          | Engine/Control authenticate end users via Ledger-issued JWTs                                               |
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
| Invalid region routing         | Requests targeting non-existent regions                 | Returns NOT_FOUND status                  |
| Oversized chunks               | Snapshot chunks exceeding expected bounds               | Processed without buffer overflow         |

**Key properties:**

- No invalid Raft message corrupts the log, state machine, or committed data
- The node remains a functioning cluster member after receiving any malformed message
- Stale-term messages are silently dropped per the Raft protocol
- Multi-region routing validates region existence before forwarding

These properties are verified by 22 Byzantine fault tests in `crates/services/src/services/raft.rs`.

### Security Boundaries

```text
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
- Region assignment is transparent to clients

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
  # RMK (Region Master Key) protects signing key material at rest
  # Provisioned via key manager backend (Infisical, Vault, AWS KMS, etc.)
  # TLS certs if using service mesh
```

### JWT Signing Key Protection

Ledger's TokenService manages Ed25519 signing keys for JWT issuance. Private key material is protected via envelope encryption:

1. **Region Master Key (RMK)** — provisioned externally (e.g., AWS KMS, Infisical)
2. **Data Encryption Key (DEK)** — generated per signing key, wrapped by RMK via AES-KWP
3. **Private key** — encrypted with DEK using AES-256-GCM, with `kid` as AAD

The `SigningKeyEnvelope` binary format: `wrapped_dek(40) + nonce(12) + ciphertext(32) + auth_tag(16) = 100 bytes`.

Private key material is zeroized on drop (`Zeroizing<Vec<u8>>`). `EncodingKey` is built on-demand per signing operation, never cached. Only public `DecodingKey` is cached for the validation hot path.

Key rotation follows the same envelope encryption lifecycle: generate a new DEK, re-wrap with the current RMK, and re-encrypt the private key material. The trust boundary design is documented in the [Security](#design-philosophy) section above.

### User Credential Protection

User credentials (passkeys, TOTP secrets, recovery code hashes) are stored REGIONAL and encrypted in the Raft log via `EncryptedUserSystemRequest`:

1. The service layer fetches the user's `UserShredKey` from REGIONAL state
2. The `SystemRequest` payload is encrypted with AES-256-GCM using the shred key
3. The sealed payload is proposed to the REGIONAL Raft group as `LedgerRequest::EncryptedUserSystem`
4. On apply, all nodes decrypt with the stored `UserShredKey`

**TOTP secrets** receive additional protection:

- The `TotpCredential.secret` field derives `Zeroize` — cleared from memory after use
- TOTP secrets are write-once: never returned via any API path after initial creation
- The gRPC handler strips the secret from all `ListUserCredentials` responses
- TOTP code verification happens in the service layer (not the state machine) to avoid `SystemTime::now()` non-determinism across Raft nodes

**Crypto-shredding on user erasure**: When `erase_user()` is called, the `UserShredKey` is destroyed, making all credential data in the Raft log permanently unrecoverable. The `erase_user()` saga also explicitly deletes credential records and TOTP challenges from the state layer.

**Challenge/consumption variants** (`CreateTotpChallenge`, `ConsumeTotpAndCreateSession`, `IncrementTotpAttempt`) carry only IDs and nonces (no PII) and are proposed as plain `LedgerRequest::System` — no encryption overhead.

### Email Blinding Key

Ledger uses HMAC-SHA256 with a 32-byte `EmailBlindingKey` to create privacy-preserving email uniqueness indices. The HMAC output (hex-encoded) is stored in the GLOBAL Raft log; the plaintext email stays in REGIONAL storage only.

**Threat model:**

- HMAC is a one-way function — the email cannot be recovered from the hash alone
- Rainbow table attack requires both key compromise AND domain enumeration (same approach as Okta, Auth0, Cognito)
- Domain-separated with `"email:"` prefix to prevent cross-context hash collisions
- Key material uses `ZeroizeOnDrop` for memory safety

**Deployment requirements:**

- All nodes in a cluster must use the same blinding key (same bytes, same version)
- Provision via secrets manager (Infisical, Vault, AWS Secrets Manager)
- Set via environment variable: `INFERADB__LEDGER__EMAIL_BLINDING_KEY` (hex-encoded 32 bytes)
- The key is never persisted in the Raft log — it is a runtime secret only

**Key rotation procedure:**

1. Generate a new 32-byte key and assign it the next version number
2. Deploy the new key to all nodes via your secrets manager
3. Call `RotateBlindingKey` RPC on any node — this proposes `SetBlindingKeyVersion` to Raft, updating the active version cluster-wide
4. New email registrations use the new key version; existing HMACs remain valid under their original version
5. Optionally run a background rehash to migrate existing entries to the new key (tracked via `UpdateRehashProgress` / `ClearRehashProgress` system requests)

**Monitoring:**

- Verify all nodes report the same key version via the health check endpoint
- Monitor `SetBlindingKeyVersion` events in the audit log after rotation
- Alert if any node starts with a different key version (email uniqueness enforcement will produce inconsistent results)

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
