# Data Residency Architecture

How Ledger enforces data residency requirements across regions.

> **Related**: For multi-region deployment patterns, see [Multi-Region Deployment](multi-region.md). For the operational mechanics of regions (Region enum, storage layout, organization assignment, write forwarding), see [Region Management](region-management.md).

## Overview

Ledger separates data into two categories with different storage guarantees:

- **GLOBAL data** — pseudonymous control-plane entries (IDs, slugs, HMACs, status enums). Stored in the per-region GLOBAL Raft cluster. Contains zero plaintext PII.
- **REGIONAL data** — plaintext PII (names, emails, addresses). Stored exclusively in the user's assigned region via REGIONAL Raft proposals.

Each region runs its own independent GLOBAL Raft cluster. No cross-region Raft replication occurs.

## Pseudonymization Design

GLOBAL Raft entries achieve pseudonymization through three mechanisms:

1. **Email HMAC blinding** — emails are HMAC-SHA256 hashed with a 32-byte blinding key before entering the GLOBAL log. The blinding key is a runtime secret (env var), never persisted in Raft. HMAC is one-way; recovery requires key compromise AND domain enumeration. See [security.md](security.md#email-blinding-key).

2. **Dual-ID architecture** — internal sequential IDs (`OrganizationId(i64)`, `UserId(i64)`) are used in storage. External Snowflake slugs (`OrganizationSlug(u64)`, `UserSlug(u64)`) are used in APIs. Neither is PII.

3. **Name stripping** — organization, team, and app names are stripped from GLOBAL `LedgerRequest` variants. Names are written to REGIONAL state via `WriteOrganizationProfile`, `WriteTeam`, and `WriteAppProfile` system requests.

Under GDPR Art. 4(1) / Recital 26, data that cannot be attributed to a specific person without additional information (the blinding key) is pseudonymous. The blinding key is held separately from the Raft log, meeting the "kept separately" requirement.

## GLOBAL Data Inventory

Exhaustive list of data stored in the GLOBAL Raft log:

| Category        | Fields                                                             | PII Status                           |
| --------------- | ------------------------------------------------------------------ | ------------------------------------ |
| Identifiers     | `OrganizationId`, `UserId`, `TeamId`, `AppId`, `VaultId`           | Not PII (sequential integers)        |
| Slugs           | `OrganizationSlug`, `UserSlug`, `TeamSlug`, `AppSlug`, `VaultSlug` | Not PII (Snowflake IDs)              |
| Email HMACs     | hex-encoded HMAC-SHA256 output                                     | Pseudonymous (requires blinding key) |
| Status enums    | `OrganizationStatus`, `UserDirectoryStatus`, `UserRole`            | Not PII                              |
| Regions         | `Region` enum (e.g., `EU_WEST_DUBLIN`)                             | Not PII                              |
| Tiers           | `OrganizationTier`                                                 | Not PII                              |
| Timestamps      | `created_at`, `updated_at`                                         | Not PII                              |
| Crypto material | `UserShredKey` (per-user encryption key), signing key envelopes    | Not PII (key material)               |
| Token metadata  | `RefreshTokenId`, `TokenVersion`, token hashes                     | Not PII                              |

Zero plaintext PII appears in any GLOBAL Raft entry.

## Regional Isolation Guarantees

### Protected Regions

A region is protected when its `RegionDirectoryEntry.requires_residency` flag is `true` — set at provisioning time via `AdminService.ProvisionRegion` (or rewritten later via `AdminService.SetRegionResidency`). Protected regions restrict:

- **Raft group membership** — only nodes tagged with the same region can join the regional Raft group
- **Data storage** — PII proposed via `propose_regional()` is stored only in the region's state layer
- **RMK isolation** — each region has its own Region Master Key; nodes only hold RMKs for their own protected region plus non-protected regions

### Non-Protected Regions

`GLOBAL` is hardcoded as non-protected (the cluster control plane is replicated everywhere). Other regions are non-protected when their `RegionDirectoryEntry.requires_residency` is `false`. The historical `US_EAST_VA` / `US_WEST_OR` constants are non-protected by convention — operators provisioning them must pass `requires_residency=false` on the `ProvisionRegion` RPC. All nodes hold RMKs for non-protected regions.

#### US Region Replication Behavior

When provisioned with `requires_residency=false`:

- User PII (names, emails) for organizations assigned to US regions is replicated to **all** nodes in the cluster, regardless of geographic location.
- A cluster with both US and non-US nodes (e.g., a Frankfurt node for EU) will receive US user data on all nodes.
- This is a deliberate design choice — there is no US federal data residency mandate comparable to GDPR.

**Operators with CCPA or contractual data residency requirements** should either:

1. Deploy US-only clusters (no non-US nodes) to ensure US data stays within US infrastructure.
2. Use separate cluster deployments per jurisdiction rather than a single multi-region cluster.
3. Provision the region with `requires_residency=true` so the regional Raft group only accepts nodes tagged with that exact region — the same isolation contract that GDPR / PIPL / etc. regions use.

### Crypto-Shredding

User-scoped REGIONAL Raft entries are encrypted with the user's `UserShredKey` (256-bit AES key). When `erase_user()` is called:

1. All user credentials and TOTP challenges are deleted from state
2. The `UserShredKey` is destroyed from the state layer
3. All encrypted Raft log entries for that user become cryptographically unrecoverable
4. No log rewriting is required — the ciphertext remains but is permanently unreadable

**Encrypted entity types** (via `EncryptedUserSystemRequest`):

| Entity          | Key Pattern                   | Contains                                                |
| --------------- | ----------------------------- | ------------------------------------------------------- |
| User profile    | `user:{id}`                   | Name, email references                                  |
| User credential | `user_credential:{uid}:{cid}` | Passkey public keys, TOTP secrets, recovery code hashes |

**Non-encrypted REGIONAL entities** (IDs and nonces only):

| Entity         | Key Pattern                         | Contains                           |
| -------------- | ----------------------------------- | ---------------------------------- |
| TOTP challenge | `_tmp:totp_challenge:{uid}:{nonce}` | Nonce, timestamps, attempt counter |

The credential sequence counter (`_meta:seq:user_credential`) is the first REGIONAL `_meta:seq:` key. All other sequence counters are GLOBAL. This design avoids a cross-tier saga for credential creation.

See `crates/raft/src/entry_crypto.rs` for the encryption implementation.

## Multi-Raft Infrastructure

The multi-Raft routing infrastructure is fully implemented:

| Component                   | Location                                          | Purpose                                               |
| --------------------------- | ------------------------------------------------- | ----------------------------------------------------- |
| `RaftManager`               | `crates/raft/src/raft_manager.rs`                 | Manages per-region Raft group lifecycle               |
| `RegionGroup`               | `crates/raft/src/raft_manager.rs`                 | Holds Raft instance + state layer for a region        |
| `RegionResolverService`     | `crates/services/src/services/region_resolver.rs` | Maps organizations to their assigned region           |
| `propose_regional()`        | `crates/services/src/services/service_infra.rs`   | Service-layer helper for regional proposals           |
| `classify_system_request()` | `crates/raft/src/types.rs`                        | Compile-time enforcement of GLOBAL vs REGIONAL        |
| `classify_ledger_request()` | `crates/raft/src/types.rs`                        | Classification for top-level `LedgerRequest` variants |

### Request Flow

```text
Service handler
    │
    ├─ GLOBAL request ──▶ propose_request() ──▶ Organization's GLOBAL Raft
    │
    └─ REGIONAL request ─▶ propose_regional() ─▶ RaftManager
                               │                      │
                               │                      ▼
                               │               get_region_group(region)
                               │                      │
                               │                      ▼
                               └──────────────▶ Region Raft group
```

## Control Plane Boundary

Ledger is responsible only for its own data residency guarantees. The Control plane and Engine services have separate architectures and data residency concerns outside Ledger's scope.

## Compliance Summary

| Requirement                       | Implementation                                                                  |
| --------------------------------- | ------------------------------------------------------------------------------- |
| No cross-border PII replication   | Independent GLOBAL Raft per region; REGIONAL data stays in-region               |
| Pseudonymization (GDPR Art. 4(1)) | HMAC-blinded emails, stripped names, numeric IDs only in GLOBAL                 |
| Right to erasure (GDPR Art. 17)   | `erase_user()` + crypto-shredding via UserShredKey destruction                  |
| Data minimization                 | GLOBAL log contains only the minimum needed for control-plane coordination      |
| Key separation                    | Blinding key held separately from Raft; RMKs per-region; UserShredKeys per-user |
