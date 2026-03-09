# ADR: JWT Token Architecture

## Context

InferaDB Ledger needs to issue JWT tokens for upstream services (control planes, applications) to authenticate users and authorize vault access. The token system must:

1. **Be deterministic** — all time-dependent logic must use Raft-replicated timestamps, never wall-clock time
2. **Survive leader failover** — token state is part of replicated state, not ephemeral
3. **Detect theft** — refresh token reuse must trigger immediate family revocation
4. **Support key rotation** — signing keys must rotate without invalidating in-flight tokens
5. **Protect key material** — private keys must be envelope-encrypted at rest

### Options Evaluated

| Option                               | Determinism | Failover      | Key Rotation | Complexity |
| ------------------------------------ | ----------- | ------------- | ------------ | ---------- |
| Stateless JWTs only                  | N/A         | N/A           | Hard         | Low        |
| JWTs + opaque refresh                | Needs care  | If replicated | Medium       | Medium     |
| JWTs + family-based refresh (chosen) | Yes         | Yes           | Built-in     | Medium     |
| OAuth2 server delegation             | External    | External      | External     | High       |

## Decision

**Implement family-based refresh tokens with Ed25519-signed JWTs, envelope-encrypted signing keys, and Raft-deterministic timestamps.**

### Token Types

| Token        | Algorithm       | Lifetime | Storage          | Purpose                |
| ------------ | --------------- | -------- | ---------------- | ---------------------- |
| Access (JWT) | EdDSA (Ed25519) | 15 min   | Stateless        | Request authentication |
| Refresh      | SHA-256 hashed  | 7 days   | Replicated state | Session continuity     |

### Why Ed25519

- 32-byte keys (vs 256+ for RSA) — fits in a single AES-GCM block
- Deterministic signatures — same input always produces same output
- No padding oracle attacks (unlike RSA-PKCS1)
- `kid` header enables key rotation without token invalidation

### Signing Key Lifecycle

```
Active ──→ Rotated ──→ Revoked
           (grace)     (immediate)
```

- **Active**: Signs new tokens. At most one active key per scope (Global or Organization).
- **Rotated**: No longer signs, but validates existing tokens during grace period (`valid_until`).
- **Revoked**: Immediately rejects all tokens signed with this key.

Transitions are one-way and enforced as an invariant (see invariants.md #50).

### Envelope Encryption

Private key material is never stored in plaintext:

```
RMK (Region Master Key)
 └─ wraps → DEK (per-key, AES-KWP)
              └─ encrypts → Ed25519 private key (AES-256-GCM)
                              └─ kid used as AAD
```

Fixed 100-byte `SigningKeyEnvelope`: wrapped_dek (40) + nonce (12) + ciphertext (32) + auth_tag (16).

### Refresh Token Families

Each refresh token belongs to a family (identified by `family_id`). Token creation starts a new family; each refresh rotates within the family:

```
create_session → RT₁ (family=F)
refresh(RT₁)   → RT₂ (family=F), RT₁ consumed
refresh(RT₂)   → RT₃ (family=F), RT₂ consumed
refresh(RT₁)   → REUSE DETECTED → family F poisoned → all tokens revoked
```

Reuse detection is the primary theft defense: if an attacker and legitimate user both hold the same refresh token, whichever refreshes second triggers family poisoning.

### Deterministic Timestamps

All Raft apply handlers use `proposed_at` from `RaftPayload`, never `Utc::now()`. This ensures:

- Token expiry calculations are identical across all replicas
- Snapshot install + replay produces identical state
- No clock skew issues between leader and followers

### Cascade Revocation

| Trigger                     | Scope                                      |
| --------------------------- | ------------------------------------------ |
| App disabled                | All tokens issued to that app              |
| Vault disconnected from app | App + vault tokens for that connection     |
| Organization deleted        | All tokens in that organization            |
| User `revoke_all_sessions`  | All user sessions (via token version bump) |

## Consequences

### Positive

- Token validation is stateless for access tokens (check signature + expiry only)
- Signing key rotation is zero-downtime (grace period allows old tokens to validate)
- Theft detection is automatic via family poisoning
- All token state survives leader failover

### Negative

- Refresh tokens require state lookup (hash-based, O(1) in replicated B-tree)
- Signing key bootstrap requires saga orchestration (handled by `CreateSigningKeySaga`)
- `TokenMaintenanceJob` required for garbage collection of expired tokens and key transitions

### Risks Mitigated

- **Clock skew**: Eliminated by Raft-deterministic timestamps
- **Key compromise**: Envelope encryption + key rotation + revocation
- **Token theft**: Family-based reuse detection + cascade revocation
- **Stale keys**: Background maintenance transitions rotated keys past grace period

## Implementation

- `crates/types/src/token.rs` — `TokenError`, `SigningKeyEnvelope`, `SigningKeyStatus`
- `crates/services/src/jwt.rs` — `JwtEngine` with ArcSwap cache for lock-free reads
- `crates/services/src/services/token.rs` — `TokenService` gRPC implementation
- `crates/state/src/system/token.rs` — Token state management in replicated state
- `crates/raft/src/token_maintenance.rs` — `TokenMaintenanceJob` background job
- `crates/types/src/config/jwt.rs` — `JwtConfig` with all tuning parameters
