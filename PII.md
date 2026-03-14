# PII Handling in InferaDB Ledger

Authoritative reference for how personally identifiable information flows through the InferaDB Ledger system: creation, encryption, storage, retrieval, and destruction.

## Table of Contents

- [PII Inventory](#pii-inventory)
- [Encryption Architecture](#encryption-architecture)
- [GLOBAL vs REGIONAL Raft Routing](#global-vs-regional-raft-routing)
- [Creation Flows](#creation-flows)
- [Storage Layout](#storage-layout)
- [Retrieval Flows](#retrieval-flows)
- [Destruction Flows](#destruction-flows)
- [Residual Data After Destruction](#residual-data-after-destruction)
- [PII Leak Prevention](#pii-leak-prevention)

---

## PII Inventory

Every PII field in the system, its owning struct, storage location, and encryption scope.

### User PII

| Field           | Struct      | Key Pattern             | Raft Scope | Encryption Key |
| --------------- | ----------- | ----------------------- | ---------- | -------------- |
| `name: String`  | `User`      | `user:{user_id}`        | REGIONAL   | UserShredKey     |
| `email: String` | `UserEmail` | `user_email:{email_id}` | REGIONAL   | UserShredKey     |

**Non-PII user data** (stored GLOBAL):

| Field                         | Struct               | Key Pattern           |
| ----------------------------- | -------------------- | --------------------- |
| `user: UserId`                | `UserDirectoryEntry` | `_dir:user:{user_id}` |
| `slug: Option<UserSlug>`      | `UserDirectoryEntry` | `_dir:user:{user_id}` |
| `region: Option<Region>`      | `UserDirectoryEntry` | `_dir:user:{user_id}` |
| `status: UserDirectoryStatus` | `UserDirectoryEntry` | `_dir:user:{user_id}` |

**Email uniqueness** (stored GLOBAL, no plaintext):

| Index      | Key Pattern                  | Value                 | Contains PII?       |
| ---------- | ---------------------------- | --------------------- | ------------------- |
| Email HMAC | `_idx:email_hash:{hmac_hex}` | `EmailHashEntry` enum | No (HMAC is opaque) |

**Email indices** (stored REGIONAL):

| Index            | Key Pattern                  | Value                       |
| ---------------- | ---------------------------- | --------------------------- |
| Email uniqueness | `_idx:email:{email_lower}`   | `UserEmailId` as string     |
| User-to-emails   | `_idx:user_emails:{user_id}` | Postcard `Vec<UserEmailId>` |

Source: `crates/state/src/system/types.rs:18-76`, `crates/state/src/system/keys.rs`

### Organization PII

| Field          | Struct                | Key Pattern                 | Raft Scope | Encryption Key |
| -------------- | --------------------- | --------------------------- | ---------- | -------------- |
| `name: String` | `OrganizationProfile` | `org_profile:{org_id}` | REGIONAL   | OrgShredKey         |

**Non-PII organization data** (stored GLOBAL):

| Field                                 | Struct                       | Key Pattern             |
| ------------------------------------- | ---------------------------- | ----------------------- |
| `organization: OrganizationId`        | `OrganizationDirectoryEntry` | `_dir:org:{org_id}` |
| `slug: Option<OrganizationSlug>`      | `OrganizationDirectoryEntry` | `_dir:org:{org_id}` |
| `region: Option<Region>`              | `OrganizationDirectoryEntry` | `_dir:org:{org_id}` |
| `tier: OrganizationTier`              | `OrganizationDirectoryEntry` | `_dir:org:{org_id}` |
| `status: OrganizationDirectoryStatus` | `OrganizationDirectoryEntry` | `_dir:org:{org_id}` |

Source: `crates/state/src/system/types.rs:308-363`

### Team PII

| Field          | Struct        | Key Pattern                            | Raft Scope | Encryption Key |
| -------------- | ------------- | -------------------------------------- | ---------- | -------------- |
| `name: String` | `Team` | `team:{org_id}:{team_id}` | REGIONAL   | OrgShredKey         |

Teams inherit the owning organization's OrgShredKey. No per-team encryption key exists.

Source: `crates/state/src/system/types.rs:376-395`

### App PII

| Field                         | Struct       | Key Pattern                          | Raft Scope | Encryption Key |
| ----------------------------- | ------------ | ------------------------------------ | ---------- | -------------- |
| `name: String`                | `AppProfile` | `app_profile:{org_id}:{app_id}` | REGIONAL   | OrgShredKey         |
| `description: Option<String>` | `AppProfile` | `app_profile:{org_id}:{app_id}` | REGIONAL   | OrgShredKey         |

The `App` struct (`app:{org_id}:{app_id}`) stores structural fields only (`enabled`, `credentials`, `version`) in GLOBAL state. PII is separated into `AppProfile` in REGIONAL state.

Source: `crates/state/src/system/types.rs:456-513`

### Client Assertion PII

| Field          | Struct       | Key Pattern                                            | Raft Scope | Encryption Key |
| -------------- | ------------ | ------------------------------------------------------ | ---------- | -------------- |
| assertion name | (raw string) | `assertion_name:{org_id}:{app_id}:{assertion_id}` | REGIONAL   | OrgShredKey         |

The `ClientAssertionEntry` struct (`app_assertion:{org_id}:{app_id}:{assertion_id}`) stores only the public key, enabled flag, and expiry in GLOBAL state. The user-provided name is stored separately in REGIONAL state.

Source: `crates/state/src/system/types.rs:575-598`, `crates/state/src/system/keys.rs`

### Onboarding PII (Transient)

| Field                       | Struct          | Context                                         |
| --------------------------- | --------------- | ----------------------------------------------- |
| `email: String`             | `OnboardingPii` | Sealed before Raft entry, unsealed during apply |
| `name: String`              | `OnboardingPii` | Sealed before Raft entry, unsealed during apply |
| `organization_name: String` | `OnboardingPii` | Sealed before Raft entry, unsealed during apply |

`OnboardingPii` is a postcard-serialized, AES-256-GCM encrypted blob that travels in the `WriteOnboardingUserProfile` Raft entry. It exists in plaintext only during the apply handler.

Source: `crates/raft/src/entry_crypto.rs:140-155`

---

## Encryption Architecture

### Key Hierarchy

```
External KMS (Infisical / Vault / AWS KMS)
    │
    ▼
RegionMasterKey (RMK) — 256-bit, per-region, versioned
    │
    ├──▶ wraps DEKs (AES-KWP, RFC 5649) for page-level at-rest encryption
    │        │
    │        ▼
    │    DataEncryptionKey (DEK) — 256-bit, per-page, unique, Zeroize on drop
    │        │
    │        ▼
    │    B+ tree page bodies — AES-256-GCM, page header as AAD
    │
    ├──▶ encrypts UserShredKey storage (UserShredKey records are B+ tree entities)
    │
    └──▶ encrypts OrgShredKey storage (OrgShredKey records are B+ tree entities)
```

### Per-Entity Encryption Keys

**UserShredKey** — per-user, for crypto-shredding user PII:

```rust
pub struct UserShredKey {
    pub user_id: UserId,
    pub key: [u8; 32],        // 256-bit AES key material
    pub created_at: DateTime<Utc>,
}
```

- Storage: `_shred:user:{user_id}` in REGIONAL store
- Encrypted at rest by EncryptedBackend (RMK-wrapped DEK)
- Destruction renders all `EncryptedUserSystemRequest` entries unrecoverable

Source: `crates/state/src/system/types.rs:99-120`

**OrgShredKey** — per-organization, for crypto-shredding org/team/app PII:

```rust
pub struct OrgShredKey {
    pub organization: OrganizationId,
    pub key: [u8; 32],        // 256-bit AES key material
    pub created_at: DateTime<Utc>,
}
```

- Storage: `_shred:org:{org_id}` in REGIONAL store
- Encrypted at rest by EncryptedBackend (RMK-wrapped DEK)
- Destruction renders all `EncryptedOrgSystemRequest` entries unrecoverable

Source: `crates/state/src/system/types.rs:126-144`

### Per-Entry Raft Log Encryption

PII-bearing `SystemRequest` payloads are encrypted before entering the Raft log. Two encryption scopes exist:

**EncryptedUserSystemRequest** — user-scoped:

```rust
pub struct EncryptedUserSystemRequest {
    pub sealed: Vec<u8>,       // ciphertext || 16-byte auth tag
    pub nonce: [u8; 12],       // random per encryption
    pub user_id: UserId,       // plaintext, for key lookup
}
```

- Encrypts: `UpdateUserProfile`, `CreateUserEmail`, other user-scoped PII writes
- Key: user's UserShredKey
- AAD: `user_id.value().to_le_bytes()` (prevents cross-user key substitution)

**EncryptedOrgSystemRequest** — organization-scoped:

```rust
pub struct EncryptedOrgSystemRequest {
    pub sealed: Vec<u8>,       // ciphertext || 16-byte auth tag
    pub nonce: [u8; 12],       // random per encryption
    pub organization: OrganizationId,  // plaintext, for key lookup
}
```

- Encrypts: org profile writes, team profile writes, app profile writes, assertion name writes
- Key: organization's OrgShredKey
- AAD: `organization.value().to_le_bytes()` (prevents cross-org key substitution)

**Encryption flow:**

1. Construct plaintext `SystemRequest`
2. Postcard-serialize to bytes
3. AES-256-GCM encrypt with 12-byte random nonce and entity-ID AAD
4. Wrap in `LedgerRequest::EncryptedUserSystem` or `LedgerRequest::EncryptedOrgSystem`
5. Propose to REGIONAL Raft group

**Decryption flow** (apply handler):

1. Look up UserShredKey/OrgShredKey from REGIONAL state
2. If key is `None` (destroyed): skip entry silently (crypto-shredding)
3. AES-256-GCM decrypt with stored nonce and entity-ID AAD
4. Postcard-deserialize to `SystemRequest`
5. Apply through normal state machine path

Source: `crates/raft/src/entry_crypto.rs`, `crates/raft/src/log_storage/operations.rs:5088-5268`

### Seal/Unseal Primitives

Low-level AES-256-GCM functions used by both per-entry encryption and onboarding PII sealing:

```rust
pub fn seal(plaintext: &[u8], key: &[u8; 32], aad: &[u8]) -> Result<(Vec<u8>, [u8; 12])>
pub fn unseal(sealed: &[u8], nonce: &[u8; 12], key: &[u8; 32], aad: &[u8]) -> Result<Vec<u8>>
```

- Nonce: 12 random bytes per call (never reused)
- Auth tag: 16 bytes, appended to ciphertext by aes-gcm crate
- AAD binding prevents cross-entity ciphertext substitution

Source: `crates/raft/src/entry_crypto.rs:157-193`

---

## GLOBAL vs REGIONAL Raft Routing

The multi-Raft architecture separates PII from structural data at the consensus level.

**GLOBAL Raft group** — replicated to all nodes, all regions. Contains:

- User/organization directory entries (IDs, slugs, status only)
- Email HMAC indices (cryptographic hashes, no plaintext)
- App structural records (credentials, enabled flags)
- Vault metadata, shard routing, cluster membership
- Token hashes, signing key metadata

**REGIONAL Raft groups** — replicated only within a single region. Contains:

- User records with names (plaintext after decryption)
- Email records with addresses (plaintext after decryption)
- Organization profiles with names
- Team profiles with names
- App profiles with names and descriptions
- Client assertion names
- UserShredKeys and OrgShredKeys (encrypted at rest under RMK)

**Routing rule**: any `SystemRequest` carrying plaintext PII is proposed to the REGIONAL Raft group. `EncryptedUserSystem` and `EncryptedOrgSystem` entries go to REGIONAL with PII encrypted before entering the log.

**Classification functions** (`crates/raft/src/types.rs`):

- `classify_system_request()` → `RaftScope::Global` or `RaftScope::Regional`
- `classify_ledger_request()` → `RaftScope::Global` or `RaftScope::Regional`

---

## Creation Flows

### User Onboarding (Saga)

The onboarding saga creates a user, organization, and initial session in three steps:

**Step 0 — GLOBAL** (`ActivateOnboardingUser`):

1. Allocate `UserId` (sequential from `_meta:seq:user`)
2. Allocate `UserSlug` (Snowflake ID)
3. Allocate `OrganizationId` and `OrganizationSlug`
4. Reserve email HMAC in `_idx:email_hash:{hmac}` as `EmailHashEntry::Provisioning`
5. Create `UserDirectoryEntry` with `status: Provisioning`

**Step 1 — REGIONAL** (`WriteOnboardingUserProfile`):

1. Seal `OnboardingPii { email, name, organization_name }` with UserShredKey via AES-256-GCM
2. Generate 32-byte UserShredKey and 32-byte OrgShredKey
3. Raft entry carries: sealed PII, PII nonce, UserShredKey bytes, OrgShredKey bytes, refresh token hash, refresh family ID, KID
4. Apply handler unseals PII using UserShredKey and user_id AAD
5. Creates `User` record (name from sealed PII)
6. Creates `UserEmail` record (email from sealed PII)
7. Stores `UserShredKey` at `_shred:user:{user_id}`
8. Creates `OrganizationProfile` (org name from sealed PII)
9. Stores `OrgShredKey` at `_shred:org:{org_id}`
10. Creates `RefreshToken` record

**Step 2 — GLOBAL** (`ActivateOnboardingUser`):

1. Activate `UserDirectoryEntry` status → `Active`
2. Activate `OrganizationDirectoryEntry` status → `Active`
3. Update email HMAC index from `Provisioning` → `Active(UserId)`

Source: `crates/raft/src/saga_orchestrator.rs`, `crates/raft/src/log_storage/operations.rs:3400+`

### Standalone User Creation

`SystemRequest::CreateUser` (GLOBAL):

- Allocates user ID and slug
- Creates `UserDirectoryEntry` — no PII
- User profile with PII created via subsequent REGIONAL `EncryptedUserSystem` entry

### Organization Creation

`SystemRequest::CreateOrganizationDirectory` (GLOBAL):

- Creates `OrganizationDirectoryEntry` — no PII (slug, region, tier, status)
- Organization profile with name created via subsequent REGIONAL `EncryptedOrgSystem` entry

### Team Creation

`SystemRequest::CreateOrganizationTeam` (GLOBAL):

- Creates team record with slug index — no PII
- Team profile with name created via REGIONAL `EncryptedOrgSystem` entry wrapping `WriteTeam`

### App Creation

`LedgerRequest::CreateApp` (GLOBAL):

- Creates `App` struct with credentials — no PII
- App profile with name/description created via REGIONAL `EncryptedOrgSystem` entry wrapping `WriteAppProfile`

### Client Assertion Creation

`LedgerRequest::CreateAppClientAssertion` (GLOBAL):

- Creates `ClientAssertionEntry` with public key — no PII
- Assertion name stored via REGIONAL `EncryptedOrgSystem` entry wrapping `WriteClientAssertionName`

---

## Storage Layout

### REGIONAL State (System Vault)

All REGIONAL records live in the `_system` vault (`SYSTEM_VAULT_ID`) within the organization's home region. B+ tree pages are encrypted at rest by `EncryptedBackend` (RMK → DEK → AES-256-GCM).

```
_system vault (REGIONAL)
├── user:{user_id}                                → User (name PII)
├── user_email:{email_id}                         → UserEmail (email PII)
├── _idx:email:{email_lower}                      → UserEmailId
├── _idx:user_emails:{user_id}                    → Vec<UserEmailId>
├── _shred:user:{user_id}                         → UserShredKey (key material)
├── org_profile:{org_id}                          → OrganizationProfile (name PII)
├── _shred:org:{org_id}                           → OrgShredKey (key material)
├── team:{org_id}:{team_id}                       → Team (name PII)
├── app_profile:{org_id}:{app_id}                 → AppProfile (name, description PII)
├── assertion_name:{org_id}:{app_id}:{assert_id}  → assertion name (string PII)
├── _audit:erasure:{user_id}                      → ErasureAuditRecord (no PII)
└── ...
```

### GLOBAL State (System Vault)

```
_system vault (GLOBAL)
├── _dir:user:{user_id}              → UserDirectoryEntry (no PII)
├── _idx:user:slug:{slug}            → UserId
├── _idx:email_hash:{hmac_hex}       → EmailHashEntry (no PII — HMAC)
├── _dir:org:{org_id}                → OrganizationDirectoryEntry (no PII)
├── org:{org_id}                     → Organization (skeleton, no PII)
├── _idx:org:slug:{slug}             → OrganizationId
├── app:{org_id}:{app_id}           → App (no PII — credentials only)
├── _idx:app:slug:{slug}             → AppId
├── app_assertion:{org_id}:{app_id}:{assertion_id} → ClientAssertionEntry (no PII — public key)
└── ...
```

---

## Retrieval Flows

### User Retrieval

1. Client provides `UserSlug` (external Snowflake ID)
2. GLOBAL lookup: `_idx:user:slug:{slug}` → `UserId`
3. GLOBAL lookup: `_dir:user:{user_id}` → `UserDirectoryEntry` (get region)
4. REGIONAL lookup: `user:{user_id}` → `User` (contains plaintext name)
5. Storage layer decryption: page cache → DEK cache → RMK unwrap → AES-256-GCM decrypt page
6. Postcard deserialize → `User` struct with plaintext `name` field

Email retrieval follows the same pattern: `user_email:{email_id}` → `UserEmail` with plaintext `email`.

### Organization Retrieval

1. Client provides `OrganizationSlug`
2. GLOBAL: `_idx:org:slug:{slug}` → `OrganizationId`
3. GLOBAL: `org:{org_id}` → `Organization` skeleton (structural fields, `name: ""`)
4. REGIONAL: `org_profile:{org_id}` → `OrganizationProfile` (PII overlay: `name`)
5. Service layer merges via `overlay_org_profile()` — degrades gracefully with `name: ""` when REGIONAL unavailable

### Team/App Retrieval

Same pattern: resolve slug to ID via GLOBAL index, read profile from REGIONAL store.

---

## Destruction Flows

### User Erasure

Triggered by `SystemRequest::EraseUser { user_id, region }`. The `UserRetentionReaper` background job (`crates/raft/src/user_retention.rs`) scans for users in `Deleting` status whose `deleted_at + region.retention_days()` has elapsed, then proposes `EraseUser` to the GLOBAL Raft. Manual erasure is also possible via the `EraseUser` gRPC RPC.

Eight idempotent steps executed in `erase_user()`:

**Step 1: Read directory entry**

- Read `UserDirectoryEntry` (may already be `Deleted` on re-execution)

**Step 2: Mark directory entry as Deleted** (GLOBAL)

- Set `UserDirectoryEntry.status` → `Deleted`
- Tombstone minimization: clear `slug`, `region`, `updated_at`
- Remove `_idx:user:slug:{slug}` index entry

**Step 3: Revoke all sessions**

- Revoke all refresh token families for the user
- Increment `User.version: TokenVersion` (invalidates outstanding JWTs)
- Failure logged as warning (idempotent on re-execution when slug already tombstoned)

**Step 4: Delete email hash index entries** (GLOBAL)

- Scan `_idx:email_hash:*` entries
- Delete those with `EmailHashEntry::Active(user_id)` or `Provisioning { user_id, .. }`
- Email uniqueness is released — another user can register the same address

**Step 5: Delete all UserEmail records and plaintext indices** (REGIONAL)

- Read each `UserEmail` record to extract the plaintext email address
- Delete `_idx:email:{email_lower}` uniqueness index for each email
- Delete all `user_email:{email_id}` entities
- Clear `_idx:user_emails:{user_id}` index
- Batched into a single `apply_operations` call

**Step 6: Delete UserShredKey** (REGIONAL, crypto-shredding)

- Delete `_shred:user:{user_id}` from REGIONAL store
- All `EncryptedUserSystemRequest` entries in Raft log become permanently unrecoverable
- On subsequent log replay, apply handler detects missing key and silently skips the entry

**Step 7: Delete User record** (REGIONAL)

- Delete `user:{user_id}` entity (contains plaintext `name` — PII)
- Must occur after session revocation (Step 3) because `revoke_all_user_sessions` reads `TokenVersion`
- Must occur before audit record (Step 8) so audit trail is the final write

**Step 8: Write erasure audit record** (GLOBAL)

- `ErasureAuditRecord { user_id, erased_at, region }` at `_audit:erasure:{user_id}`
- Non-PII: only opaque identifiers and timestamps
- Insert-or-overwrite for idempotent crash-resume

All steps are idempotent: `DeleteEntity` on a missing key is a no-op in the B+ tree engine. Re-executing `erase_user()` after completion does not error.

Source: `crates/state/src/system/service.rs:1178-1286`

### Organization Purge

Two-phase purge triggered by `OrganizationPurgeJob` after `deleted_at + region.retention_days()` elapses.

**Phase 1 — REGIONAL** (`PurgeOrganizationRegional`):

1. Delete all `Team` records (`team:{org_id}:*`)
2. Delete all `AppProfile` records (`app_profile:{org_id}:*`)
3. Delete all assertion name records (`assertion_name:{org_id}:*`)
4. Delete `OrganizationProfile` (`org_profile:{org_id}`) — contains plaintext name
5. Destroy `OrgShredKey` (`_shred:org:{org_id}`) — crypto-shredding
6. Clean up in-memory name indices (`team_name_index`, `app_name_index`)

**Phase 2 — GLOBAL** (`PurgeOrganization`):

1. Delete all vault metadata and slug indices for the organization
2. Delete team and app slug indices
3. Remove organization structural records

**Retry logic**: exponential backoff (100ms, 500ms, 2500ms), max 3 retries per phase. Failed organizations tracked in priority set and retried on every subsequent cycle.

Source: `crates/raft/src/log_storage/operations.rs:4224-4293`, `crates/raft/src/organization_purge.rs`

### Raft Log Truncation (PostErasureCompactionJob)

After key destruction, encrypted entries remain in the Raft log until a snapshot truncates them. The `PostErasureCompactionJob` bounds this retention period.

**Problem**: low-traffic REGIONAL groups accumulate log entries slowly. The default `snapshot_threshold` (10,000 entries) means crypto-shredded entries can remain on disk for days or weeks.

**Solution**: the job runs every `check_interval_secs` and triggers `raft.trigger().snapshot()` on any Raft group whose time since last snapshot exceeds `max_log_retention_secs`.

**After snapshot**:

- Raft log is truncated up to the snapshot index
- Crypto-shredded entries are discarded (no longer replayed)
- The snapshot itself reflects post-erasure state (UserShredKey/OrgShredKey already destroyed)
- On log replay from snapshot, apply handler finds no key → skips encrypted entries

Source: `crates/raft/src/post_erasure_compaction.rs`

---

## Residual Data After Destruction

### After User Erasure

| What Remains                                           | Location     | Contains PII?                                       |
| ------------------------------------------------------ | ------------ | --------------------------------------------------- |
| `UserDirectoryEntry { user: UserId, status: Deleted }` | GLOBAL       | No — slug, region, updated_at cleared               |
| `ErasureAuditRecord { user_id, erased_at, region }`    | GLOBAL       | No — opaque IDs and timestamps                      |
| Encrypted Raft log entries (pre-truncation)            | REGIONAL log | No — UserShredKey destroyed, ciphertext unrecoverable |

**Destroyed**:

- `User` record at `user:{user_id}` (plaintext name)
- All `UserEmail` records (email addresses)
- Email uniqueness indices `_idx:email:{email_lower}` (plaintext email in key pattern)
- `UserShredKey` (key material)
- Email hash indices `_idx:email_hash:*` (GLOBAL, uniqueness released)
- User-to-emails index `_idx:user_emails:{user_id}`
- Slug indices (slug freed)
- Refresh tokens and sessions (revoked + token version bumped)

### After Organization Purge

| What Remains                                                | Location     | Contains PII?                                   |
| ----------------------------------------------------------- | ------------ | ----------------------------------------------- |
| `OrganizationRegistry { organization_id, status: Deleted }` | GLOBAL       | No — structural only                            |
| Encrypted Raft log entries (pre-truncation)                 | REGIONAL log | No — OrgShredKey destroyed, ciphertext unrecoverable |

**Destroyed**:

- `OrganizationProfile` (name)
- All `Team` records (names)
- All `AppProfile` records (names, descriptions)
- All assertion name records
- `OrgShredKey` (key material)
- Organization/team/app slug indices
- In-memory name indices

---

## PII Leak Prevention

### Debug Redaction

Structs carrying PII implement custom `Debug` that redacts sensitive fields:

| Struct             | Redacted Fields                      | Shown Fields                  |
| ------------------ | ------------------------------------ | ----------------------------- |
| `OnboardingPii`    | `email`, `name`, `organization_name` | (none)                        |
| `OrgPii`           | `name`                               | (none)                        |
| `OnboardingCrypto` | `refresh_token`, `refresh_family_id` | `kid`, timestamps, slugs      |
| `SagaOutput`       | `refresh_token`, `refresh_family_id` | IDs, slugs, timestamps, `kid` |

Source: `crates/raft/src/saga_orchestrator.rs`

Storage-level keys also redact:

- `DataEncryptionKey::fmt()` → `[REDACTED]`
- `RegionMasterKey::fmt()` → shows version, hides key bytes

Source: `crates/store/src/crypto/types.rs`

### Canonical Log Lines

`RequestContext` (alias `CanonicalLogLine`) emits one structured JSON event per request. PII prevention:

- **No plaintext fields**: no email, name, password, or token strings
- **Numeric IDs only**: `UserId`, `OrganizationId`, `VaultId` (sequential integers)
- **Slugs are safe**: Snowflake IDs (random uint64)
- **Keys hashed**: entity keys SHA-256 hashed before logging
- **Static operation types**: generic metadata strings, no user content

Source: `crates/raft/src/logging.rs`

### Raft Log Safety

The GLOBAL/REGIONAL split ensures no plaintext PII enters the GLOBAL Raft log. REGIONAL entries carrying PII are encrypted before entering the log (`EncryptedUserSystem`, `EncryptedOrgSystem`). The only exception is the `WriteOnboardingUserProfile` entry, which carries sealed (AES-256-GCM encrypted) PII — never plaintext.

### Token Security

- Email verification tokens stored as `SHA-256(token)` — database breach doesn't leak tokens
- Refresh tokens stored as `SHA-256(token)` — only hashes persisted
- Client secrets stored as bcrypt hashes — never plaintext
- Private keys (client assertion) returned once at creation and never stored

### Sensitive Data in Memory

- `UserShredKey.key` and `OrgShredKey.key` exist as `[u8; 32]` in memory during active use
- `DataEncryptionKey` implements `Zeroize` — key bytes are zeroed on drop
- `RegionMasterKey` implements `Zeroize` — key bytes are zeroed on drop
- UserShredKey/OrgShredKey do not implement `Zeroize` (they are short-lived deserialized values)
