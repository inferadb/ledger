# InferaDB Ledger — Codebase Manifest

## Overview

InferaDB Ledger is a blockchain database for cryptographically verifiable authorization, built in Rust with a layered architecture:

```
gRPC Services (14 proto-defined, 13 implemented): Read, Write, Organization, Vault, Schema (proto only), Admin, User, Invitation, App, Token, Events, Health, SystemDiscovery, Raft
 ↓
 services — gRPC service implementations, JWT engine, server assembly
 ↓
 raft — openraft integration, apply-phase parallelism, saga orchestrator, background jobs, rate limiting, multi-region
 ↓
 consensus — purpose-built multi-shard Raft engine, event-driven Reactor, segmented WAL, pipelined replication
 ↓
 state — domain model, vaults, entities, relationships, state roots
 ↓
 store — custom B+ tree engine, ACID transactions, crash recovery
 ↓
 types — primitives, hashing, config, errors, validation
```

Supporting crates:

- **proto**: gRPC/protobuf definitions and conversions
- **consensus**: Purpose-built multi-shard Raft engine (Reactor, Shard, WAL backends, state-machine abstraction)
- **sdk**: Enterprise client library with retry, circuit breaker, metrics
- **server**: Binary with bootstrap, config, discovery, integration tests
- **test-utils**: Shared testing infrastructure (strategies, assertions, crash injection)

The codebase demonstrates production-grade engineering: zero `unsafe` code, comprehensive error handling with snafu, property-based testing with proptest, crash recovery tests, OpenTelemetry tracing, Prometheus metrics, and organization-scoped event logging with queryable audit trails.

---

## Crate: `inferadb-ledger-types`

- **Purpose**: Foundation crate providing primitives, configuration, error taxonomy, hashing, Merkle trees, and input validation.
- **Dependencies**: No workspace dependencies (foundational)
- **Quality Rating**: ★★★★★

### Files

#### `lib.rs`

- **Purpose**: Re-exports core types, errors, hashing utilities, and codec
- **Key Types/Functions**:
- Public modules: `codec`, `config`, `email_hash`, `error`, `events`, `hash`, `merkle`, `onboarding`, `snowflake`, `token`, `types`, `validation`
- Re-exports: `OrganizationId`, `OrganizationSlug`, `OrganizationUsage`, `VaultId`, `VaultSlug`, `UserId`, `UserSlug`, `UserCredentialId`, `CredentialType`, `TotpAlgorithm`, `PrimaryAuthMethod`, `PasskeyCredential`, `TotpCredential`, `RecoveryCodeCredential`, `CredentialData`, `UserCredential`, `PendingTotpChallenge`, `InviteId`, `InviteSlug`, `InvitationStatus`, `InviteEmailEntry`, `InviteIndexEntry`, `OrganizationInvitation`, `BlockHeader`, `Transaction`, `Entity`, `Relationship`, `Operation`, `EmailBlindingKey`, `compute_email_hmac`, `normalize_email`, `TokenVersion`, `TokenType`, `TokenSubject`, `UserSessionClaims`, `VaultTokenClaims`, `ValidatedToken`, `TokenError`, `SigningKeyEnvelope`, etc.
- Note: `TokenPair` exists in the SDK and proto crates but NOT in the types crate. `compute_code_hash` exists in `email_hash.rs` but is NOT re-exported from lib.rs.
- **Insights**: Clean public API surface, excellent organization

#### `codec.rs`

- **Purpose**: postcard-based serialization/deserialization with structured error handling
- **Key Types/Functions**:
- `encode<T: Serialize>(value: &T) -> Result<Vec<u8>>`: Serialize to bytes
- `decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T>`: Deserialize from bytes
- `CodecError`: snafu error with `Encode`/`Decode` variants
- Unit tests covering all domain types
- **Insights**: Excellent test coverage, postcard chosen for determinism and compactness

#### `config/`

- **Purpose**: Configuration types for all subsystems with fallible builders, serde, and JSON Schema
- **Structure**:
- `config/mod.rs` — `ConfigError` enum, re-exports from submodules
- `config/node.rs` — `NodeConfig`, `PeerConfig`
- `config/storage.rs` — `StorageConfig`, `BTreeCompactionConfig`, `IntegrityConfig`, `BackupConfig`, `TieredStorageConfig`, `RegionTieredStorageOverride`
- `config/raft.rs` — `RaftConfig`, `BatchConfig`, `ClientSequenceEvictionConfig`, `PostErasureCompactionConfig` (max_log_retention_secs default 3600, check_interval_secs default 300; triggers proactive snapshots after crypto-shredding to evict plaintext from Raft logs)
- `config/resilience.rs` — `RateLimitConfig`, `ShutdownConfig` (with `leader_transfer_timeout_secs`, default 10, 0 disables), `HealthCheckConfig`, `ValidationConfig`, `SagaConfig`, `CleanupConfig`, `MigrationConfig`
- `config/observability.rs` — `HotKeyConfig`, `MetricsCardinalityConfig` (note: `AuditConfig` previously here was removed — replaced by `EventConfig` in `events.rs`)
- `config/jwt.rs` — `JwtConfig` (issuer, session/vault access/refresh TTLs, clock skew leeway, key rotation grace period). bon fallible builder with validation (TTLs > 0, refresh > access for each pair, clock skew < min access TTL). Wired into `RuntimeConfig` (optional) and server `Config`.
- `config/runtime.rs` — `RuntimeConfig`, `RuntimeEventsConfig`, `ConfigChange`, `NonReconfigurableField`. Optional `jwt: Option<JwtConfig>` for runtime reconfiguration.
- `config/encryption.rs` — `EncryptionConfig` (enabled, key_source, algorithm, dek_cache_capacity, strict_memory_protection), `KeySource` enum (Env, File), `EncryptionAlgorithm` enum (Aes256Gcm), `RewrapConfig` (enabled, batch_size, interval_secs, target_rmk_version). Envelope encryption at-rest configuration for all persisted data (pages, snapshots, Raft logs) via DEK/RMK wrapping. Unit tests.
- `config/key_management.rs` — `RmkStatus` enum (Active, Deprecated, Decommissioned), `RmkVersionInfo` (region, version, status, created_at), `KeyManagerBackend` enum (SecretsManager, Env, File), `SecretsManagerConfig`, `SecretsProvider` enum (Infisical, Vault, AwsKms, GcpKms, AzureKeyVault). Region Master Key lifecycle and key manager backend selection.
- **Key Types/Functions**:
- Config structs across submodules (was monolithic, now organized by subsystem)
- All use `#[bon::bon]` fallible builders with validation
- All derive `serde::Serialize/Deserialize` for JSON interchange (runtime config RPCs) and postcard storage
- All derive `schemars::JsonSchema` for schema export
- **Insights**: Well-organized module split groups configs by subsystem. Excellent validation with humantime-serde for durations, byte-unit for sizes. Each struct's tests and default functions live alongside the struct definition. Encryption config supports multi-provider key management (Infisical, Vault, AWS KMS, GCP KMS, Azure Key Vault) with per-region RMK lifecycle tracking.

#### `error.rs`

- **Purpose**: Structured error taxonomy with numeric codes, retryability, and guidance
- **Key Types/Functions**:
- `DiagnosticCode`: 33 numeric codes spanning 1000-1101 (storage), 2000-2102 (consensus), 3000-3207 (application) for classification. Includes region-aware codes: `AppInsufficientRegionNodes` (3206), `AppInvalidRegionAssignment` (3207), `AppOrganizationMigrating` (3106), `AppUserMigrating` (3107). Note: `Unauthenticated` is in `ErrorCode` (error_code.rs), NOT in `DiagnosticCode`. `ErrorCode` is a separate enum for structured error responses.
- `LedgerError`: Domain-level errors with `#[snafu]` for context propagation
- `StorageError`: Storage layer errors (IO, corruption, capacity)
- `ConsensusError`: Raft consensus errors (leadership, quorum, log gaps)
- `code()`, `is_retryable()`, `suggested_action()`: Traits on all error types
- **Insights**: State-of-the-art error handling. Every error has numeric code, retryability classification, and actionable guidance. Implicit location tracking via snafu.

#### `hash.rs`

- **Purpose**: Cryptographic and non-cryptographic hashing utilities
- **Key Types/Functions**:
- `Hash = [u8; 32]`: Type alias for SHA-256 digests (not a wrapper struct)
- `hash_eq()`: Constant-time hash comparison using `subtle::ConstantTimeEq` (prevents timing side-channel attacks)
- `block_hash()`, `tx_hash()`: Domain-specific hash functions (note: `compute_state_root()` lives in `state/state.rs`, not here)
- `BucketHasher`: State bucketing (256 buckets) for incremental hashing
- **Insights**: Excellent security practices. Constant-time comparison prevents timing attacks. seahash is used internally for non-cryptographic hashing (Prometheus labels, event key encoding) but not exposed as a named type.

#### `merkle.rs`

- **Purpose**: Merkle tree and proof generation/verification using rs_merkle
- **Key Types/Functions**:
- `MerkleTree::from_leaves(leaves: &[Hash])`: Build tree from transaction hashes
- `MerkleTree::root()`: Get root hash
- `MerkleTree::proof(index: usize)`: Generate inclusion proof for transaction
- `MerkleProof::verify()`: Verify proof against root
- **Insights**: Power-of-2 leaf count limitation documented (rs_merkle constraint). Adequate for block-level Merkle trees.

#### `types/` module

- **Purpose**: Core domain types organized into 7 submodules by concern (identifiers, blocks, credentials, enums, region, usage, retention)
- **Submodules**:
  - `types/ids.rs` — ID/slug newtypes via `define_id!` and `define_string_id!` macros. All `define_id!`-generated types: `OrganizationId`, `VaultId`, `UserId`, `SigningKeyId`, `RefreshTokenId`, `UserCredentialId`, `InviteId`, `TeamId`, `AppId`, `ClientAssertionId`, `EmailVerifyTokenId`, `UserEmailId`. Hand-implemented `u64` Snowflake slugs (no prefix): `OrganizationSlug`, `VaultSlug`, `UserSlug`, `InviteSlug`, `TeamSlug`, `AppSlug`. `TokenVersion(u64)` manual (needs `Default` + `increment()`). `NodeId`, `ClientId` via `define_string_id!`.
  - `types/block.rs` — `BlockHeader` (height, organization, vault, previous_hash, tx_merkle_root, state_root, timestamp, term, committed_index), `VaultBlock`, `RegionBlock`, `Transaction` (fallible builder, max 1000 ops / 100 MB payload), `Entity` (key, value, expires_at, version), `Relationship` (resource, relation, subject), `Operation` (5 variants: CreateRelationship, DeleteRelationship, SetEntity, DeleteEntity, ExpireEntity).
  - `types/credentials.rs` — `CredentialType` enum (Passkey/Totp/RecoveryCode, Display impl for key encoding), `TotpAlgorithm` (Sha1/Sha256/Sha512), `PasskeyCredential`, `TotpCredential` (secret in `Zeroizing<Vec<u8>>`, manual Debug redacts as `[REDACTED]`, custom serde), `RecoveryCodeCredential`, `CredentialData` enum (externally-tagged for postcard), `UserCredential`, `PendingTotpChallenge`.
  - `types/enums.rs` — `UserStatus`, `UserRole`, `OrganizationMemberRole`, `PrimaryAuthMethod` (EmailCode/Passkey — not a `CredentialType`, used for TOTP challenge audit trail).
  - `types/region.rs` — `Region` enum (data-residency tier).
  - `types/usage.rs` — `OrganizationUsage` (per-org resource accounting).
  - `types/retention.rs` — `BlockRetentionPolicy`, `BlockRetentionMode`.
- **Key Types/Functions**:
  - `define_id!` macro: Generates `i64`-based newtypes with derives, Display (prefixed like `"org:42"`), FromStr, serde.
  - `define_string_id!` macro: Generates `String`-based newtypes with derives, Display, FromStr, serde.
- **Insights**: Well-factored into 7 semantic groups rather than a monolithic `types.rs`. Dual-ID architecture: internal sequential `i64` IDs for B+ tree key density, external Snowflake `u64` slugs for API-facing use. Externally-tagged serde on `CredentialData` is required for postcard compatibility (internally-tagged enums fail postcard deserialization). `TotpCredential` derives `Zeroize` on secret field for secure memory cleanup. `UserCredentialId` follows the single-ID pattern (like `SigningKeyId`, `RefreshTokenId`) — no slug since credentials are always accessed under a user context.

#### `invitation.rs`

- **Purpose**: Organization invitation types — state machine, records, and index entries for the invitation lifecycle
- **Key Types/Functions**:
- `InvitationStatus` enum: `Pending`, `Accepted`, `Declined`, `Expired`, `Revoked` (snake_case serde). `is_terminal()` method.
- `OrganizationInvitation` struct: Full REGIONAL record (Pattern 1) — `id`, `slug`, `organization`, `token_hash`, `inviter`, `invitee_email_hmac`, `invitee_email` (PII), `role`, `team`, `status`, `created_at`, `expires_at`, `resolved_at`
- `InviteIndexEntry` struct: Stored in `_idx:invite:slug:` and `_idx:invite:token_hash:` — `organization: OrganizationId`, `invite: InviteId` (no cached region)
- `InviteEmailEntry` struct: Stored in `_idx:invite:email_hash:{hmac}:{invite_id}` — `organization: OrganizationId`, `status: InvitationStatus` (deliberately minimal, no timestamps)
- Unit tests: serde roundtrips, terminal state validation, Display
- **Insights**: Invitation records follow Pattern 1 (REGIONAL-only) since they contain PII. `InviteIndexEntry` does not cache the org's region — looked up live from `OrganizationRegistry` to avoid stale routing after org migration. `InviteEmailEntry` is minimal (org+status only) to eliminate dual-write of timestamps on state transitions.

#### `snowflake.rs`

- **Purpose**: Snowflake-style globally unique ID generation for node IDs, organization slugs, vault slugs, and user slugs
- **Key Types/Functions**:
- `generate() -> Result<u64, SnowflakeError>`: Core ID generation (42-bit timestamp + 22-bit sequence)
- `generate_organization_slug() -> Result<OrganizationSlug, SnowflakeError>`: Convenience wrapper
- `generate_vault_slug() -> Result<VaultSlug, SnowflakeError>`: Convenience wrapper for vault slugs
- `generate_user_slug() -> Result<UserSlug, SnowflakeError>`: Convenience wrapper for user slugs
- `generate_team_slug() -> Result<TeamSlug, SnowflakeError>`: Convenience wrapper for team slugs
- `generate_app_slug() -> Result<AppSlug, SnowflakeError>`: Convenience wrapper for app slugs
- `extract_timestamp(id: u64) -> u64`: Extract millisecond timestamp from ID
- `extract_worker(id: u64) -> u64`: Extract worker bits from ID
- `extract_sequence(id: u64) -> u64`: Extract sequence number from ID
- `SnowflakeError`: snafu error with `SystemClock` variant
- Custom epoch: 2024-01-01 00:00:00 UTC (~139 years range)
- Thread-safe via `parking_lot::Mutex` global state
- **Insights**: Extracted from `server/src/node_id.rs` (Task 3) so all crates can generate IDs without depending on `server`. No worker/datacenter bits — slug generation (organization, vault, and user) happens on Raft leader only (single writer). 4.2M IDs/ms capacity. Shared generator between all slug types.

#### `validation.rs`

- **Purpose**: Input validation with character whitelists and configurable size limits
- **Key Types/Functions**:
- `ValidationConfig` (defined in `config/resilience.rs`): max_name_length, max_value_length, max_operations_per_transaction, etc.
- `validate_key()`, `validate_value()`, `validate_organization_name()`: Character whitelist enforcement
- `validate_relationship_string()`: Relationship tuple component validation
- `validate_operations_count()`, `validate_batch_payload_bytes()`: Size limit enforcement
- Character sets: alphanumeric + hyphen/underscore for names, UTF-8 for values
- **Insights**: Defense-in-depth security. Prevents injection attacks, DoS via large requests. Configurable limits support different deployment environments.

#### `email_hash.rs`

- **Purpose**: HMAC-SHA256 based email and verification code hashing with a blinding key for privacy-preserving global email uniqueness and secure code storage
- **Key Types/Functions**:
- `EmailBlindingKey`: 32-byte secret key for HMAC-SHA256 hashing, loaded externally at node startup, never persisted. `new(bytes, version)`, `version()`, `as_bytes()`, `FromStr` for 64-char hex strings. Derives `Zeroize` + `ZeroizeOnDrop` for secure memory cleanup (key material zeroed on drop). `#[zeroize(skip)]` on `version` field (not key material).
- `EmailBlindingKeyParseError`: snafu error with `InvalidLength` and `InvalidHex` variants
- `normalize_email(email: &str) -> String`: Trims, lowercases, strips plus-addressing (`user+tag@` → `user@`), and removes dots from Gmail/Googlemail local parts (`u.ser@gmail.com` → `user@gmail.com`) for consistent HMAC computation. Normalization is only for HMAC — plaintext email is stored as-entered in REGIONAL.
- `compute_email_hmac(key: &EmailBlindingKey, email: &str) -> String`: HMAC-SHA256 with domain prefix `"email:"` — computes `HMAC-SHA256(key, "email:" || lowercase(email))` as lowercase hex (64 chars)
- `compute_code_hash(key: &EmailBlindingKey, code: &str) -> [u8; 32]`: HMAC-SHA256 with domain prefix `"code:"` — computes `HMAC-SHA256(key, "code:" || uppercase(code))` as raw 32-byte array. Case-insensitive (uppercased + trimmed). Returns bytes (not hex) since code hashes are stored as bytes in `PendingEmailVerification`.
- `generate_verification_code(key: &EmailBlindingKey) -> (String, [u8; 32])`: Generates random 6-character code from A-Z0-9 charset (36^6 ≈ 2.18B combinations) via CSPRNG, returns `(code_string, code_hash)` where hash is computed via `compute_code_hash`.
- Unit tests (20+ traditional + 2 property tests)
- **Insights**: `Debug` intentionally redacts key material. Property tests verify HMAC collision resistance and output format consistency. Domain separation prefixes (`"email:"` / `"code:"`) prevent cross-function collision even if normalization were swapped — separate functions with distinct test coverage. `compute_code_hash` returns `[u8; 32]` (bytes) while `compute_email_hmac` returns `String` (hex) — different return types for different storage patterns.

#### `onboarding.rs`

- **Purpose**: Compile-time constants, token generation/decoding, and error types for self-service user onboarding via email verification
- **Key Types/Functions**:
- Constants: `CODE_LENGTH` (6), `CODE_TTL` (10 min), `MAX_CODE_ATTEMPTS` (5), `MAX_INITIATIONS_PER_HOUR` (3), `RATE_LIMIT_WINDOW` (1 hour), `ONBOARDING_TTL` (12 hr), `MAX_ONBOARDING_SCAN` (5,000), `ONBOARDING_TOKEN_PREFIX` (`"ilobt_"`), `SAGA_COMPLETION_TIMEOUT` (30s)
- `generate_onboarding_token() -> (String, [u8; 32])`: Generates `ilobt_{base64url_no_pad(32 random bytes)}` token + `SHA-256(raw_bytes)` hash. 256-bit entropy makes bare SHA-256 sufficient (no HMAC needed).
- `decode_onboarding_token(token: &str) -> Result<[u8; 32], OnboardingTokenError>`: Strips `ilobt_` prefix, base64url decodes, returns raw 32 bytes for hashing.
- `OnboardingTokenError` enum: `InvalidPrefix`, `InvalidEncoding`, `InvalidLength` with `Display`/`Error` impls
- Unit tests (18 tests: value assertions, relationship invariants, encode/decode roundtrip, error paths)
- **Insights**: All constants are compile-time only — no runtime reconfiguration via `RuntimeConfigHandle`. The `ilobt_` prefix ("InferaDB Ledger Onboarding Bearer Token") follows the `ilrt_` pattern used for refresh tokens, enabling secret scanning tool detection (truffleHog, GitHub scanning). SHA-256 hash of raw bytes (not the base64 string) ensures encoding-agnostic storage.

#### `error_code.rs` (updates)

- **New variants**: `RateLimited` (maps to `RESOURCE_EXHAUSTED`), `Expired` (maps to `FAILED_PRECONDITION`), `TooManyAttempts` (maps to `FAILED_PRECONDITION`) — used by onboarding apply handlers for rate limiting, code expiry, and max attempts enforcement. `InvitationRateLimited` (maps to `RESOURCE_EXHAUSTED`), `InvitationAlreadyResolved` (maps to `FAILED_PRECONDITION`), `InvitationEmailMismatch` (maps to `NOT_FOUND`), `InvitationAlreadyMember` (maps to `ALREADY_EXISTS`), `InvitationDuplicatePending` (maps to `ALREADY_EXISTS`) — used by invitation service handlers.

#### `token.rs`

- **Purpose**: JWT token types, claims, errors, and signing key envelope for the token authentication system
- **Key Types/Functions**:
- `TokenType` enum: `UserSession`, `VaultAccess` (snake_case serde)
- `TokenSubject` enum: `User(UserSlug)`, `App(AppSlug)` — refresh token subject disambiguation
- `UserSessionClaims`: JWT claims for user sessions (iss, sub, aud, exp, iat, nbf, jti, token_type, user, role, version)
- `VaultTokenClaims`: JWT claims for vault access tokens (iss, sub, aud, exp, iat, nbf, jti, token_type, org, app, vault, scopes)
- `TokenPair`: Access token + refresh token with expiry times
- `ValidatedToken` enum: `UserSession(UserSessionClaims)` | `VaultAccess(VaultTokenClaims)`
- `TokenError`: 7-variant snafu error enum (Expired, InvalidSignature, InvalidAudience, MissingClaim, InvalidTokenType, SigningKeyNotFound, SigningKeyExpired)
- `SigningKeyEnvelope`: 100-byte fixed binary layout (`wrapped_dek(40) + nonce(12) + ciphertext(32) + auth_tag(16)`) for envelope-encrypted Ed25519 private keys. `to_bytes()`/`from_bytes()` serialization.
- `SESSION_AUDIENCE` (`"inferadb-control"`), `VAULT_AUDIENCE` (`"inferadb-engine"`): Compile-time audience constants
- **Insights**: Claims use `#[serde(rename = "type")]` on `token_type` field (reserved keyword). `SigningKeyEnvelope` is a byte container only — actual crypto operations (`generate_dek`, `wrap_dek`, `unwrap_dek`) are called from `services/src/jwt.rs`. `TokenError` is domain-level only; JWT library errors live in `JwtError` (services crate) to prevent `types` from depending on `jsonwebtoken`.

#### `events.rs`

- **Purpose**: Core domain types for the organization-scoped event logging system
- **Key Types/Functions**:
- `EventEntry`: Canonical audit record with fields (`emission` first for snapshot thin deserialization, `expires_at` second for GC efficiency, `event_id`, `source_service`, `event_type`, `timestamp`, `scope`, `action`, `principal`, `organization_id: OrganizationId`, `organization: Option<OrganizationSlug>`, `vault: Option<VaultSlug>`, `outcome`, `details`, `block_height`, `trace_id`, `correlation_id`, `operations_count`)
- `EmissionMeta`: Thin deserialization target for `scan_apply_phase()` — reads only the `emission` discriminant (~2 bytes) to filter handler-phase events without full deserialization
- `EventMeta`: Thin GC wrapper — postcard deserializes `emission` (field 1) and `expires_at` (field 2) to check expiry without full deserialization
- `EventScope`: `System` (org_id=0) or `Organization` — compile-time mapped from `EventAction`
- `EventAction`: 62 variants (51 system, 11 org) — includes 6 token-related variants: `SigningKeyCreated`, `SigningKeyRotated`, `SigningKeyRevoked`, `TokenCreated`, `TokenRevoked`, `TokenRefreshed`; 5 invitation-related variants: `OrganizationMemberAdded`, `InvitationCreated`, `InvitationResolved`, `InvitationPurged`, `InvitationEmailRehashed`; 11 app-related variants: `AppCreated`, `AppUpdated`, `AppDeleted`, `AppStatusChanged`, `AppSecretRotated`, `AppAssertionCreated`, `AppAssertionDeleted`, `AppCredentialStatusChanged`, `AppVaultConnected`, `AppVaultDisconnected`, `AppVaultUpdated`; 3 team variants: `TeamCreated`, `TeamUpdated`, `TeamDeleted`; 7 user-related variants: `UserCreated`, `UserDeleted`, `UserUpdated`, `UserSoftDeleted`, `UserEmailCreated`, `UserEmailDeleted`, `UserEmailVerified`; with exhaustive `scope()`, `event_type()`, `as_str()` — adding a variant without updating all three is a compile error. System variants include `UserErased` and `UsersMigrated` for crypto-shredding and flat-to-regional migration.
- `EventEmission`: `ApplyPhase` (deterministic, all replicas) or `HandlerPhase { node_id }` (node-local)
- `EventOutcome`: `Success`, `Failed { code, detail }`, `Denied { reason }`
- `EventConfig`: Master config with `enabled`, `default_ttl_days` (90), `max_details_size_bytes` (4096), `system_log_enabled`, `organization_log_enabled`, `max_snapshot_events` (100,000), `ingestion: IngestionConfig`
- `IngestionConfig`: External ingestion config — `ingest_enabled`, `allowed_sources` (default: `["engine", "control"]`), `max_ingest_batch_size` (500), `ingest_rate_limit_per_source` (10,000/sec)
- Unit tests covering all enum variants, scope mapping, serialization round-trips, config validation
- **Insights**: Replaces the deleted `audit.rs`. Events use the canonical log line pattern (wide events) with rich structured context. `EventAction::scope()` enforces no-dual-write at the type level. `emission` as first field enables thin postcard deserialization for snapshot collection (`EmissionMeta`); `expires_at` as second field enables thin deserialization for GC (`EventMeta`). Serialization byte-pinning test prevents accidental field reorders. `source_service` is `String` (not `&'static str`) for postcard round-trip compatibility with external services (Engine, Control). `UserErased` and `UsersMigrated` system events track crypto-shredding and bulk migration operations.

---

## Crate: `inferadb-ledger-store`

- **Purpose**: Custom B+ tree storage engine with ACID transactions, page management, crash recovery, and pluggable backends.
- **Dependencies**: `types` (for errors, hashing, config)
- **Quality Rating**: ★★★★★

### Files

#### `lib.rs`

- **Purpose**: Re-exports public API (Database, Transaction, StorageBackend, Table)
- **Key Types/Functions**:
- Modules: `backend`, `bloom` (pub(crate)), `btree`, `crypto`, `db`, `dirty_bitmap`, `error`, `integrity`, `page`, `tables`, `transaction`, `types`
- **Insights**: Clean layering: backend abstraction → page management → B+ tree → database → transactions. `bloom` is crate-private. `crypto` module adds envelope encryption layer.

#### `backend/mod.rs`

- **Purpose**: Storage backend trait definition and module re-exports
- **Key Types/Functions**:
- `StorageBackend` trait: `read_page`, `write_page`, `read_header`, `write_header`, `sync`, `file_size`, `extend`, `page_size`, `page_offset`, `rewrap_pages`, `sidecar_page_count`
- Re-exports: `FileBackend` (from `file.rs`), `InMemoryBackend` (from `memory.rs`)
- **Insights**: Abstraction enables testing without filesystem overhead. Dual-slot commit protocol documented in module-level comments.

#### `backend/memory.rs`

- **Purpose**: In-memory storage backend for testing (no filesystem dependency)
- **Key Types/Functions**:
- `InMemoryBackend`: HashMap-based page storage
- Implements full `StorageBackend` trait for test isolation
- **Insights**: Enables deterministic testing without filesystem side effects.

#### `backend/file.rs`

- **Purpose**: Production file-based storage backend with lock-free reads via pread/pwrite
- **Key Types/Functions**:
- `FileBackend`: Production backend using position-based I/O
- Reads (`read_page`, `read_header`): Lock-free via `read_exact_at()` (Unix `pread(2)`) — no lock, no seek, no cursor mutation
- Writes (`write_page`, `write_header`, `extend`): Serialized via `parking_lot::Mutex<()>`
- Platform abstraction: `#[cfg(unix)]` uses `std::os::unix::fs::FileExt` for true lock-free pread; `#[cfg(windows)]` falls back to lock-based seek_read (cursor mutation)
- Concurrency tests (read+write, extend+read, read-beyond-file, multi-reader)
- **Insights**: Lock-free reads enable concurrent read scaling without contention. `Mutex<()>` protects write ordering at OS level, not a Rust value. `sync_data()` for durability (not `sync_all()`). Lock-free reads are Unix-only.

#### `bloom.rs`

- **Purpose**: Bloom filter for page existence checks (optimization)
- **Key Types/Functions**:
- `BloomFilter::new()`: Create filter (fixed size 256 bytes, 5 hashes)
- `insert(&key)`, `may_contain(&key) -> bool`: Add/check membership
- **Insights**: Space-efficient probabilistic data structure. Used to avoid disk reads for non-existent keys.

#### `btree/mod.rs`

- **Purpose**: B+ tree core logic (insert, get, remove, split, merge, compact) with extensive tests
- **Key Types/Functions**:
- `BTree::new(root_page_id, page_manager, node_manager)`: Open existing tree
- `insert(&key, &value)`, `get(&key) -> Option<Vec<u8>>`, `remove(&key) -> bool`
- `cursor() -> BTreeCursor`: Iterator support
- `compact(fill_threshold) -> Result<CompactionStats>`: O(N) forward-only compaction via `next_leaf` linked list
- `CompactionStats`: merge count and freed page count
- `find_parent_of_leaf()`: O(depth) parent discovery for merge candidates
- `BTreeIterator::advance()`: O(1) via `next_leaf` pointer (replaces O(depth) tree re-descent)
- **Insights**: Custom implementation (not using existing crate). Supports variable-length keys/values. Compaction uses forward-only iteration via leaf linked list (O(N)) instead of repeated DFS traversals (old O(N²)). Same-parent merge restriction prevents cross-branch merges. Greedy merge re-checks after each merge for multi-leaf collapse. Bulk of line count is comprehensive inline tests (26+ compaction tests).

#### `btree/node.rs`

- **Purpose**: B+ tree node representation (internal and leaf nodes) with singly-linked leaf list
- **Key Types/Functions**:
- `InternalNode`: keys + child page IDs
- `LeafNode`: cells (key-value pairs) + `next_leaf: PageId` sibling pointer
- `NODE_HEADER_SIZE`: 16 bytes (cell_count + free_start + free_end + reserved + next_leaf)
- `next_leaf()`, `set_next_leaf()`: Linked list accessors
- `split_leaf()`, `split_internal()`: Node splitting during insert (maintains linked list)
- `merge_leaves()`: Merge adjacent leaves during compaction (saves/restores next_leaf across init)
- **Insights**: Leaf nodes form singly-linked list (forward only) for O(1) sequential scan advancement. `NODE_HEADER_SIZE` increased from 8→16 bytes to accommodate `next_leaf`. `LeafNode::init()` zeroes the `next_leaf` field. Split operations maintain the linked list chain.

#### `btree/split.rs`

- **Purpose**: Node splitting logic separated from main btree module
- **Key Types/Functions**:
- `split_leaf_for_key()`: Split leaf node to accommodate a new key
- `split_branch()`: Split internal (branch) node when full
- `merge_leaves()`: Merge adjacent leaf nodes during compaction
- **Insights**: Clean separation of concerns. Split and merge logic is complex enough to warrant dedicated file.

#### `btree/cursor.rs`

- **Purpose**: Iterator over B+ tree (forward scan, range queries)
- **Key Types/Functions**:
- `BTreeCursor::new(btree, start_key)`: Position cursor
- `next() -> Option<(Vec<u8>, Vec<u8>)>`: Iterate key-value pairs
- `advance()`: Move to next leaf via linked list or parent backtracking
- **Insights**: Supports range queries with start_key bound. Fixed critical bug in `advance()` for multi-leaf traversal (resume-key pattern).

#### `page/mod.rs`

- **Purpose**: Page abstraction (configurable-size blocks), page header types
- **Key Types/Functions**:
- `Page`: Fixed-size buffer with header (page_id, page_type, checksum)
- `PageManager`: Owns backend, cache, allocator
- `read_page()`, `write_page()`, `allocate_page()`, `free_page()`
- **Insights**: Fixed-size pages simplify memory management. XXH3-64 checksums detect corruption. Actual cache and allocator live in sub-files.

#### `page/cache.rs`

- **Purpose**: LRU page cache (reduces disk I/O)
- **Key Types/Functions**:
- `PageCache::new(capacity)`: Create cache
- `get(&page_id) -> Option<Arc<Page>>`: Check cache
- `insert(page_id, page)`: Add to cache, evict LRU if full
- **Insights**: `Arc<Page>` enables safe sharing across threads. LRU eviction policy is simple and effective.

#### `page/allocator.rs`

- **Purpose**: Free page tracking (bitmap-based)
- **Key Types/Functions**:
- `PageAllocator::new()`: Initialize allocator
- `allocate() -> PageId`: Find free page, mark allocated
- `free(page_id)`: Mark page as free
- **Insights**: Bitmap stored in dedicated pages. Fast allocation via bitwise operations.

#### `dirty_bitmap.rs`

- **Purpose**: Track modified pages for transaction commit
- **Key Types/Functions**:
- `DirtyBitmap::new()`: Create bitmap
- `mark(page_id)`: Record modification
- `is_dirty(page_id) -> bool`: Check if modified
- `clear()`: Reset after commit
- **Insights**: Critical for ACID transactions. Only dirty pages are flushed to disk.

#### `integrity.rs`

- **Purpose**: Data integrity verification using XXH3-64 checksums and B+ tree structural validation
- **Key Types/Functions**:
- `IntegrityScrubber<'a, B>`: Stateful scrubber holding a database reference
- `verify_page_checksums(page_ids: &[PageId]) -> ScrubResult`: Verify XXH3-64 checksums for specified pages
- `verify_btree_invariants() -> ScrubResult`: Validate B+ tree structural integrity (ordering, parent-child consistency)
- `ScrubResult`: Result with `structural_errors: u64` count
- `ScrubError`: Error for integrity check failures
- **Insights**: Uses XXH3-64 (not CRC32) for page checksum verification. Background `IntegrityScrubberJob` in raft crate wraps this for periodic scrubbing.

#### `db/` module

- **Purpose**: Database layer with ACID transactions, multiple B+ trees (tables), and comprehensive tests — split into 4 submodules for clarity
- **Submodules**:
  - `db/mod.rs` — `Database<B: Backend>` struct and open/read/write entry points.
  - `db/transactions.rs` — `ReadTransaction` / `WriteTransaction` implementations (snapshot isolation, dirty tracking, commit/rollback), `TableTransaction<T: Table>` for type-safe table access.
  - `db/iterator.rs` — `TableIterator` with streaming range queries and resume-key support. `DEFAULT_BUFFER_SIZE` constant; buffers decoded KV pairs with background refill to amortize page cache access.
  - `db/page_providers.rs` — Transaction-layer page providers (`CachingReadPageProvider`, `BufferedWritePageProvider`, `BufferedReadPageProvider`) that sit between transactions and the page manager.
  - `db/tests.rs` — Comprehensive inline tests covering the full Database API.
- **Key Types/Functions**:
  - `Database::<FileBackend>::open<P: AsRef<Path>>(path: P) -> Result<Self>`: Open file-based database
  - `Database::<InMemoryBackend>::open_in_memory() -> Result<Self>`: Create in-memory database
  - `read() -> Result<ReadTransaction<'_, B>>`: Start read transaction
  - `write() -> Result<WriteTransaction<'_, B>>`: Start write transaction
  - Tables initialize implicitly on first write (no explicit open_table method needed)
- **Insights**: Multiple tables share the same PageManager. Dual-slot commit protocol (commit bit + sync) ensures atomicity. Page-provider indirection lets write transactions buffer dirty pages in memory without copying them back to the cache until commit.

#### `error.rs`

- **Purpose**: Storage engine error types with snafu
- **Key Types/Functions**:
- `Error`: Store error enum with IO, corruption, capacity, and internal variants
- `Result<T>`: Type alias for `std::result::Result<T, Error>`
- `PageId`, `PageType`: Type aliases re-exported from here
- **Insights**: Structured error types with snafu context selectors. Provides the foundational error type for all store operations.

#### `transaction.rs`

- **Purpose**: Snapshot state and MVCC bookkeeping (the transaction _types_ — `ReadTransaction` / `WriteTransaction` structs live in `db/transactions.rs`)
- **Key Types/Functions**:
- `CommittedState`: Per-commit snapshot metadata
- `TransactionTracker`: Active-reader tracking for MVCC garbage collection
- `SnapshotId`: Monotonic snapshot identifier
- `PendingFrees`: Page-free list pending safe reclamation once no snapshot references remain
- `TrackerState`: Internal tracker state machine
- **Insights**: This file holds the state-tracking primitives. The executable transaction types (`ReadTransaction`, `WriteTransaction`) are in `db/transactions.rs` — the separation keeps MVCC accounting isolated from transaction operations.

#### `tables.rs`

- **Purpose**: Type-safe table definitions with marker traits
- **Key Types/Functions**:
- `Table` trait: defines KeyType, ValueType, table_id
- `Entities` (0), `Relationships` (1), `ObjIndex` (2), `SubjIndex` (3): Core data tables
- `Blocks` (4), `VaultBlockIndex` (5): Block storage tables
- `RaftLog` (6), `RaftState` (7): Raft consensus tables
- `VaultMeta` (8), `OrganizationMeta` (9), `Sequences` (10): Metadata tables
- `ClientSequences` (11), `CompactionMeta` (12): Operational tables
- `OrganizationSlugIndex` (13): Org slug→internal ID mapping (`u64` → `i64`)
- `VaultSlugIndex` (14): Vault slug→internal ID mapping (`u64` → `i64`)
- `VaultHeights` (15): Composite-key vault block heights (`[org_id ++ vault_id]` → `u64`)
- `VaultHashes` (16): Composite-key vault hashes (`[org_id ++ vault_id]` → `Hash`)
- `VaultHealth` (17): Composite-key vault health status (`[org_id ++ vault_id]` → `VaultHealthStatus`)
- `UserSlugIndex` (18): User slug→internal ID mapping (`u64` → `i64`)
- `TeamSlugIndex` (19): Team slug→internal ID mapping (`u64` → `i64`)
- `AppSlugIndex` (20): App slug→internal ID mapping (`u64` → `i64`)
- `StringDictionary` (21): Interned string `id → bytes` (component string deduplication for relationship tuples)
- `StringDictionaryReverse` (22): Reverse `bytes → id` lookup for the string dictionary
- `TableEntry`: Encoded (table_id, key, value) triple for snapshot streaming
- **Insights**: Phantom types prevent mixing keys/values from different tables. Compile-time table ID assignment. 23 tables total. Tables 15-17 use composite byte keys for externalized `AppliedState` persistence. Tables 21-22 implement string interning for relationship components, reducing duplicate string storage across the relationship table.

#### `types.rs`

- **Purpose**: Key/value type encoding for the store engine
- **Key Types/Functions**:
- `KeyType` enum: Discriminant for compile-time table key type selection
- `Key` trait: Types usable as B+ tree keys (encoding/decoding)
- `Value` trait: Types usable as B+ tree values (encoding/decoding)
- `encode_length_prefixed()`, `decode_length_prefixed()`: Variable-length encoding helpers
- `encode_varint()`, `decode_varint()`: Varint encoding for compact integer storage
- **Insights**: Trait-based key/value abstraction enables type-safe table definitions. Length-prefixed encoding supports variable-length keys while maintaining sort order.

#### `crypto/`

- **Purpose**: AES-256-GCM envelope encryption for data at rest with transparent backend wrapping, DEK caching, and RMK lifecycle management
- **Structure**:
- `crypto/mod.rs` — Module re-exports (`EncryptedBackend`, caches, key managers, operations, sidecar, types) — 7 files total
- `crypto/backend.rs` — `EncryptedBackend<B: StorageBackend>`: Transparent encryption wrapper. Reads decrypt via DEK cache + AES-256-GCM. Writes generate per-page DEK, encrypt body, wrap DEK with active RMK via AES-KWP, store sidecar metadata. Lock-free reads via underlying backend's pread.
- `crypto/key_manager.rs` — `KeyManager` trait + implementations: `FileKeyManager` (versioned files at `{key_dir}/{region}/v{version}.key`), `EnvKeyManager` (env vars `LEDGER_RMK_{REGION}_V{VERSION}`), `SecretsManagerKeyManager` (external provider). RMK provisioning validation and version lifecycle tracking.
- `crypto/cache.rs` — `DekCache`: moka bounded LRU keyed by wrapped DEK form. `RmkCache`: version→key HashMap. Both implement `ZeroizeOnDrop` for secure memory cleanup.
- `crypto/operations.rs` — Core cryptographic primitives: `generate_dek()`, `wrap_dek()`/`unwrap_dek()` (AES-KWP), `encrypt_page_body()`/`decrypt_page_body()` (AES-256-GCM with 12-byte nonce + 16-byte auth tag). Proptests for round-trip verification.
- `crypto/sidecar.rs` — Per-page metadata sidecar storage (File or Memory implementation). 72 bytes per page: `rmk_version` (4) + `wrapped_dek` (40) + `nonce` (12) + `auth_tag` (16). Stored in separate `{db_path}.sidecar` file.
- `crypto/types.rs` — `DataEncryptionKey`, `RegionMasterKey`, `WrappedDek`, `CryptoMetadata`. All sensitive types implement `ZeroizeOnDrop` for secure memory cleanup.
- **Insights**: Envelope encryption pattern — each page has its own DEK (wrapped with the active RMK). RMK rotation only re-wraps DEK metadata (72 bytes/page), never re-encrypts page bodies. DEK cache provides O(1) amortized decryption. Sidecar file is separate from the main database file to avoid changing the page format. Supports transparent encryption — all existing code paths work unchanged via the `EncryptedBackend` wrapper implementing `StorageBackend`.

#### `tests/crash_recovery.rs` (tests)

- **Purpose**: Crash injection tests using CrashInjector from test-utils
- **Key Types/Functions**:
- Tests cover crashes during write, commit, sync, allocation, freeing
- **Insights**: Excellent confidence in durability guarantees. Crash injection is deterministic (not flaky).

---

## Crate: `inferadb-ledger-state`

- **Purpose**: Domain state layer managing vaults, entities, relationships, state roots, indexes, snapshots, and tiered storage.
- **Dependencies**: `types`, `store` (via StorageEngine wrapper)
- **Quality Rating**: ★★★★★

### Files

#### `lib.rs`

- **Purpose**: Re-exports public API (StateLayer, Entity/Relationship stores, ShardManager, TieredSnapshotManager)
- **Key Types/Functions**:
- Modules: `block_archive`, `bucket`, `engine`, `entity`, `events`, `events_keys`, `indexes`, `keys`, `relationship`, `shard`, `snapshot`, `state`, `tiered_storage`, `system`
- Re-exports from `events`: `EventIndex`, `EventStore`, `Events`, `EventsDatabase`, `EventsDatabaseError`, `EventStoreError`
- Re-exports from `events_keys`: `encode_event_key` (only this function is re-exported from lib.rs; other functions like `encode_event_index_key`, `encode_event_index_value`, `primary_key_from_index_value`, `org_prefix`, `org_time_prefix` exist in `events_keys.rs` but are NOT re-exported — accessed internally via `crate::events_keys::*`)
- Re-exports from `tiered_storage`: `LocalBackend`, `ObjectStorageBackend`, `StorageBackend`, `StorageTier`, `TieredSnapshotManager`, `TieredStorageConfig`, `TieredStorageError`
- **Insights**: Rich feature set: snapshots, tiered storage, multi-region, saga-based cross-region transactions, and dedicated events storage.

#### `engine.rs`

- **Purpose**: StorageEngine wrapper around store::Database with transaction helpers
- **Key Types/Functions**:
- `StorageEngine` (file backend) and `InMemoryStorageEngine` (in-memory backend) — thin wrappers around `Database`
- `open(path) -> Result<Self>`: Constructor (file-backed) or `open() -> Result<Self>` (in-memory)
- `db() -> Arc<Database<B>>`: Returns raw database handle for transaction access
- **Insights**: Thin wrapper provides backend-specific construction. Transactions are on the `Database` itself (not on the engine wrapper).

#### `state.rs`

- **Purpose**: StateLayer applies blocks, computes state roots using bucket-based incremental hashing
- **Key Types/Functions**:
- `StateLayer::new(db) -> Self`: Initialize with 256-bucket vault commitment system
- `apply_operations(vault, operations, block_height) -> Result<Hash>`: Apply operations and compute new state root
- `apply_operations_in_txn(txn, vault, operations, block_height) -> Result<Hash>`: Transaction-scoped variant
- `compute_state_root(vault) -> Result<Hash>`: Current state root (SHA-256 of 256 bucket roots)
- `get_entity()`, `relationship_exists()`, `list_entities()`, `list_relationships()`: Read queries
- `list_subjects()`, `list_resources_for_subject()`: Relationship traversal queries
- `clear_vault()`, `compact_tables()`: Management operations
- `restore_entity(txn, storage_key, encoded_value) -> Result<()>`: Re-insert entity during snapshot installation
- Infrastructure: `database_stats()`, `database()`, `table_depths()`, `begin_write()`, `persist_last_applied()`, `read_last_applied()`, `mark_dirty_keys()`, `mark_all_dirty()`, `load_vault_commitment()`, `get_bucket_roots()`, `rewrap_pages()`, `sidecar_page_count()`
- **Insights**: Bucket hashing enables incremental updates (only recompute dirty buckets). State root is deterministic and verifiable. `restore_entity()` encapsulates entity writes during snapshot install (bypasses normal apply path). 1 proptest validates determinism across independent instances.

#### `events.rs`

- **Purpose**: Events B+ tree tables (primary + secondary index), EventStore operations, and EventsDatabase managed wrapper for `events.db`
- **Key Types/Functions**:
- `Events` table: Uses `TableId::Entities` (value 0) — safe because `events.db` is a separate `Database` instance from `state.db`
- `EventIndex` table: Uses `TableId::Relationships` (value 1) — secondary index mapping `(org_id, event_id)` → `(timestamp_ns, event_hash)` for O(log n) point lookups by event ID
- `EventStore`: Stateless struct with methods taking transaction references:
- `write(txn, entry)`: Serialize via postcard, insert into both primary `Events` table and `EventIndex` in the same transaction
- `get(txn, org_id, timestamp_ns, event_id)`: Point lookup by primary key
- `get_by_id(txn, org_id, event_id)`: O(log n) lookup via `EventIndex` — two B+ tree lookups (index → reconstruct primary key → primary fetch). Returns `None` for orphaned index entries.
- `list(txn, org_id, start_ns, end_ns, limit, after_key)` → `(Vec<EventEntry>, Option<Vec<u8>>)`: Cursor-based pagination with time-range scan
- `delete_expired(read_txn, write_txn, now_unix, max_batch)`: Thin `EventMeta` deserialization to check expiry, full deserialization for expired entries to get `event_id`/`organization` for index cleanup. Removes from both `Events` and `EventIndex`.
- `count(txn, org_id)`: Prefix scan count
- `scan_apply_phase(txn, max_entries)`: For Raft snapshots — uses `EmissionMeta` thin deserialization to filter handler-phase events (~55%) without full decode, then fully deserializes apply-phase entries. Returns most-recent N apply-phase events, newest-first.
- `scan_apply_phase_ranged(txn, org_ids, cutoff_timestamp_ns, max_entries)`: Per-org range scan using B+ tree bounds instead of sort-then-truncate. Returns raw postcard bytes to avoid re-serialization overhead.
- `EventsDatabase<B>`: Wraps `Arc<Database<B>>`. Opens/creates `{data_dir}/events.db`. Manual `Clone` impl shares the `Arc`.
- `EventsDatabaseError`: snafu error with `Open { path, source }` variant
- Unit tests covering primary/index writes, get_by_id, GC index cleanup, snapshot restore index rebuild, multi-org isolation
- **Insights**: Separate `events.db` file avoids write lock contention with `state.db` (independent `Mutex`). `delete_expired` uses collect-then-delete pattern with separate read/write transactions. `scan_apply_phase` uses `EmissionMeta` thin deserialization to skip ~55% of full deserializations (handler-phase events). `EventIndex` is derivable from the primary table — snapshot restore via `EventStore::write()` automatically rebuilds the index.

#### `events_keys.rs`

- **Purpose**: 24-byte composite B+ tree key encoding for efficient per-organization time-range scans, plus secondary index key encoding for O(log n) point lookups
- **Key Types/Functions**:
- **Primary key** layout: `[org_id (8 BE) | timestamp_ns (8 BE) | seahash(event_id) (8 BE)]`
- `encode_event_key(org_id, timestamp_ns, event_id) -> Vec<u8>`: Encode primary composite key
- `decode_event_key(key) -> Option<DecodedEventKey>`: Decode back to components
- `org_prefix(org_id) -> [u8; 8]`: Prefix for scanning all events in an organization
- `org_time_prefix(org_id, timestamp_ns) -> [u8; 16]`: Prefix for time-bounded scans within an org
- **Secondary index** key/value encoding:
- `encode_event_index_key(org_id, event_id) -> Vec<u8>`: 24 bytes: `org_id(8 BE) || event_id(16)`
- `encode_event_index_value(timestamp_ns, event_id) -> Vec<u8>`: 16 bytes: `timestamp_ns(8 BE) || seahash(event_id)(8 BE)`
- `primary_key_from_index_value(org_id, index_value) -> Vec<u8>`: Reconstructs 24-byte primary key from org_id + 16-byte index value
- Constants: `EVENT_KEY_LEN` (24), `EVENT_INDEX_KEY_LEN` (24), `EVENT_INDEX_VALUE_LEN` (16)
- Unit tests (10 primary key + 6 secondary index)
- **Insights**: Big-endian encoding ensures lexicographic ordering matches chronological ordering. 8-byte seahash of event_id provides uniqueness without storing the full 16-byte UUID in the key. Index value stores `(timestamp_ns, event_hash)` — combined with `org_id`, this reconstructs the exact primary key for a single B+ tree lookup.

#### `entity.rs`

- **Purpose**: Entity CRUD operations (zero-sized type with static methods)
- **Key Types/Functions**:
- `EntityStore` — zero-sized struct with static methods (generic over `StorageBackend`)
- `get(tx, vault, key) -> Option<Entity>`: Retrieve entity
- `set(tx, vault, key, value, ...)`: Insert/update entity
- `delete(tx, vault, key) -> bool`: Remove entity
- `exists()`, `list_in_vault()`, `list_in_bucket()`, `count_in_vault()`, `scan_prefix()`: Query methods (all accept `vault: VaultId`)
- **Insights**: Zero-sized type avoids unnecessary allocations. Static methods generic over backend enable reuse across file and in-memory stores.

#### `relationship.rs`

- **Purpose**: Relationship CRUD operations with dual indexing (object and subject)
- **Key Types/Functions**:
- `RelationshipStore` — zero-sized struct with static methods (generic over `StorageBackend`)
- `create(tx, vault, resource, relation, subject)`: Insert relationship
- `exists(tx, vault, resource, relation, subject) -> bool`: Permission check
- `delete(tx, vault, resource, relation, subject) -> bool`: Remove relationship
- `list_for_resource(tx, vault, resource)`: List relationships for a resource
- `list_in_vault()`, `count_in_vault()`, `get()`: Additional query methods
- **Insights**: Dual indexing (object + subject) via `IndexManager` enables efficient queries in both directions (who can access X? what can Y access?). Critical for authorization.

#### `keys.rs`

- **Purpose**: Key encoding for storage layer (vault prefix, bucket prefix, storage keys)
- **Key Types/Functions**:
- `encode_storage_key(vault: VaultId, local_key) -> Vec<u8>`: Encode vault-scoped storage key (8-byte vault + 1-byte bucket + local key)
- `vault_prefix(vault: VaultId) -> [u8; 8]`: Vault-scoped key prefix for range scans
- `bucket_prefix(vault: VaultId, bucket_id) -> [u8; 9]`: Vault + bucket prefix for bucket-scoped scans
- `decode_storage_key(&[u8]) -> Option<StorageKey>`: Decode key into components
- `encode_obj_index_key()`, `encode_subj_index_key()`: Relationship index key constructors
- **Insights**: Fixed-size prefix (8-byte vault + 1-byte bucket_id) enables range scans. Big-endian for lexicographic ordering.

#### `indexes.rs`

- **Purpose**: Dual object/subject indexes for relationships
- **Key Types/Functions**:
- `add_to_obj_index()`, `add_to_subj_index()`: Insert into indexes
- `remove_from_obj_index()`, `remove_from_subj_index()`: Remove from indexes
- `get_subjects()`, `get_resources()`: Query indexes
- **Insights**: Index keys include all tuple components to support efficient lookups. Consistent with relationship key encoding.

#### `binary_keys.rs`

- **Purpose**: Binary key encoding for relationship tuples using interned component strings
- **Key Types/Functions**:
- `InternId` newtype: Compact integer identifier for an interned component string
- `InternCategory` enum: Distinguishes which dictionary space a string belongs to (resource type, relation, etc.)
- `encode_relationship_local_key()`, `decode_relationship_local_key()`: Binary encoding of `(resource, relation, subject)` tuples using `InternId`s
- **Insights**: Binary tuple keys compress common component strings into `InternId`s looked up through the per-vault dictionary (`dictionary.rs`). Reduces relationship-table storage by a large constant factor when applications reuse a small set of resource types and relations.

#### `dictionary.rs`

- **Purpose**: Per-vault string-interning dictionary with deduplication (`VaultDictionary`)
- **Key Types/Functions**:
- `VaultDictionary` struct: In-memory cache backed by `StringDictionary` / `StringDictionaryReverse` tables (store tables 21-22)
- Intern/lookup methods keyed by `InternCategory` + string bytes
- **Insights**: Dictionary sits between relationship encoding and the store. The reverse table gives O(log N) `bytes → id` lookup so the service layer can intern new strings without full-scan probes. This is the source of the "string interning" feature referenced in the store crate.

#### `relationship_index.rs`

- **Purpose**: In-memory hash index for O(1) relationship existence checks
- **Key Types/Functions**:
- `RelationshipIndex` struct: In-memory map keyed by relationship tuple, rebuilt from the persisted relationship table on startup
- Insert/remove/contains methods
- **Insights**: Optimization for read-heavy authorization workloads — check-permission paths avoid a B+ tree lookup. Index lives in-memory and is rebuilt deterministically from the underlying table, so it never affects state-root computation.

#### `bucket.rs`

- **Purpose**: VaultCommitment with 256 buckets, dirty tracking, incremental state root computation
- **Key Types/Functions**:
- `VaultCommitment::new()`: Initialize with 256 buckets
- `mark_dirty_by_key(local_key)`: Mark bucket dirty by key hash
- `mark_dirty(bucket)`: Mark specific bucket dirty
- `set_bucket_root(bucket, root)`: Set bucket root hash
- `compute_state_root() -> Hash`: Recompute only dirty buckets, SHA-256(bucket_roots)
- `clear_dirty()`: Reset dirty tracking after state root computation
- **Insights**: Incremental hashing is critical for performance. Dirty tracking prevents redundant computation.

#### `block_archive.rs`

- **Purpose**: Persistent block storage with optional compaction
- **Key Types/Functions**:
- `BlockArchive::new(engine) -> Self`
- `append_block(block: &RegionBlock) -> Result<()>`: Persist block
- `read_block(region_height: u64) -> Result<RegionBlock>`: Retrieve by height
- `compact_before(before_height: u64) -> Result<u64>`: Remove blocks before height
- **Insights**: Blocks stored by height. Compaction policy configurable (retain last N blocks or time-based). Supports audit requirements.

#### `shard.rs`

- **Purpose**: ShardManager coordinates multiple vaults within a shard
- **Key Types/Functions**:
- `ShardManager::new(region: Region, db: Arc<Database<B>>, snapshot_dir: PathBuf, max_snapshots: usize) -> Self`
- `register_vault(organization, vault)`: Register vault in shard
- `get_vault_meta(vault) -> Option<VaultMeta>`: Get vault metadata
- `vault_health(vault) -> Option<VaultHealth>`: Get vault health status
- `list_vaults() -> Vec<VaultId>`: List all vaults in shard
- **Insights**: Shard = collection of organizations sharing a Raft group. ShardManager is coordination layer above StateLayer.

#### `snapshot.rs`

- **Purpose**: Point-in-time snapshots with zstd compression, chain verification
- **Key Types/Functions**:
- `VaultSnapshotMeta`: Per-vault metadata within a snapshot (`vault: VaultId`, `vault_height: u64`, `state_root: Hash`, `bucket_roots: Vec<Hash>`, `key_count: u64`)
- `Snapshot`: Point-in-time snapshot with shard_height, vault metadata, and optional bucket roots
- `write_to_file(path) -> Result<()>`, `read_from_file(path) -> Result<Self>`: Serialization with zstd compression
- `SnapshotManager`: Manages snapshot directory with rotation (`save()`, `load()`, `load_latest()`, `list_snapshots()`, `find_snapshot_at_or_before()`)
- **Insights**: Snapshots include state root for verification. `SnapshotManager` handles rotation (configurable max_snapshots). Critical for backup/restore and Raft snapshot transfer.

#### `tiered_storage.rs`

- **Purpose**: Hot/warm/cold storage tiers with S3/GCS/Azure backend support, multipart upload for large snapshots
- **Key Types/Functions**:
- `StorageTier` enum: Hot, Warm, Cold
- `StorageBackend` trait: `store()`, `load()`, `exists()`, `list()`, `delete()` — unified interface for all tiers
- `LocalBackend`: File-system snapshot storage with rotation
- `ObjectStorageBackend`: Generic object storage (S3/GCS/Azure via URL-based configuration) with multipart upload for snapshots exceeding configurable threshold (default 50 MB)
- `TieredSnapshotManager`: Orchestrates snapshot storage across tiers (`store()`, `load()`, `load_latest_hot()`, `promote()`, `demote()`)
- `TieredStorageConfig` (from `types/src/config/storage.rs`): `hot_count`, `warm_url`, `warm_days`, `cold_enabled`, `demote_interval_secs`, `multipart_threshold_bytes`
- **Insights**: `ObjectStorageBackend` is a single generic implementation (not separate S3/GCS/Azure backends) — URL scheme determines provider. `load_latest_hot()` reads only from local (hot) tier, bypassing tier fallback for Raft follower catch-up to avoid S3 latency. Multipart upload uses 8 MB chunks for snapshots exceeding threshold. `TieredStorageConfig` moved to types crate for cross-crate access. Cost optimization for large deployments.

#### `system/mod.rs`

- **Purpose**: `_system` organization for cluster metadata, sagas, service discovery, and token management
- **Key Types/Functions**:
- Submodules: `cluster`, `keys`, `saga`, `service`, `token`, `types`
- Re-exports: `SigningKey`, `SigningKeyScope`, `SigningKeyStatus`, `RefreshToken`, `CreateSigningKeySaga`, `CreateSigningKeySagaState`, `CreateSigningKeyInput`, `CreateOnboardingUserSaga`, `CreateOnboardingUserSagaState`, `CreateOnboardingUserInput`, `EmailHashEntry`, `ProvisioningReservation`, `PendingEmailVerification`, `OnboardingAccount`, `OrgShredKey`, `UserShredKey`, `Organization`, `Team`, `KeyTier`, `FamilyRevocationResult`, `SubjectRevocationResult`, `AllUserSessionsRevocationResult`, `AllAppSessionsRevocationResult`, `ExpiredTokenCleanupResult`
- **Insights**: System organization stores metadata (cluster config, node registry, distributed transactions). Token submodule provides signing key and refresh token state operations.

#### `system/cluster.rs`

- **Purpose**: Multi-region cluster membership and per-Raft-group role tracking
- **Key Types/Functions**:
- `GroupMembership`: Per-Raft-group role assignments (Voter/Learner per node per region)
- `ClusterMembership`: Node registry with heartbeat tracking, wraps `GroupMembership`
- `SystemRole` enum: `Voter`, `Learner` — per-region role for each node
- **Insights**: `GroupMembership` maps `(node_id, region)` → `SystemRole`, enabling per-region voter/learner assignments. `ClusterMembership` composes `GroupMembership` with `NodeInfo` registry.

#### `system/saga/`

- **Purpose**: Distributed transaction orchestration (saga pattern). Multi-file module with per-saga submodules.
- **Submodules**:
  - `saga/mod.rs` — `Saga` enum, `SagaStep`, `SagaExecutor`, `SagaLockKey` variants, shared saga plumbing.
  - `saga/create_organization.rs` — `CreateOrganizationSaga`: organization creation across GLOBAL registry + REGIONAL profile.
  - `saga/create_signing_key.rs` — `CreateSigningKeySaga`: 5-state saga auto-bootstrapping signing keys.
  - `saga/create_user.rs` — `CreateUserSaga`: full user creation (invitation-driven).
  - `saga/create_onboarding_user.rs` — `CreateOnboardingUserSaga`: 3-step self-service user registration (PII-free saga state).
  - `saga/delete_user.rs` — `DeleteUserSaga`: cascading user deletion + crypto-shredding.
  - `saga/migrate_org.rs` — `MigrateOrgSaga`: cross-region organization migration.
  - `saga/migrate_user.rs` — `MigrateUserSaga`: user region transfer.
- **Key Types/Functions**:
- `Saga`: Multi-step distributed transaction — includes `CreateSigningKey`, `CreateOrganization`, `CreateUser`, `CreateOnboardingUser` variants
- `SagaStep`: Individual step with compensating action
- `SagaExecutor`: Orchestrates execution, handles failures
- `CreateSigningKeySaga`: 5-state saga (`Pending → KeyGenerated → Completed | Failed | TimedOut`) for auto-bootstrapping signing keys
- `CreateSigningKeyInput`: Saga input with `scope: SigningKeyScope`
- `CreateOnboardingUserSaga`: 3-step saga (`Pending → IdsAllocated → RegionalDataWritten → Completed | Failed | Compensated | TimedOut`) for self-service user registration. GLOBAL→regional→GLOBAL routing keeps PII out of the GLOBAL Raft log.
- `CreateOnboardingUserSagaState` enum: `Pending`, `IdsAllocated`, `RegionalDataWritten`, `Completed`, `Failed`, `Compensated`, `TimedOut`
- `CreateOnboardingUserInput`: `email_hmac`, `region`, `user_slug`, `organization_slug` — NO PII fields
- `SagaLockKey::SigningKeyScope(SigningKeyScope)`, `SagaLockKey::Email(String)`: Lock key variants for concurrent saga prevention
- **Insights**: Saga pattern for cross-region transactions. Compensating actions ensure eventual consistency. `CreateOnboardingUserSaga` uses `target_region()` for GLOBAL→regional→GLOBAL multi-group routing — PII (email, name, org_name) only appears in the regional step 1 Raft log. PII is passed in-memory via `SagaSubmission.pii`, not persisted in saga state.

#### `system/service/`

- **Purpose**: `SystemOrganizationService` — organization CRUD with slug-based lookup, vault slug storage, user directory management, email hash indexing, onboarding CRUD, credential CRUD, TOTP challenge management, invitation CRUD. Multi-file module split by concern.
- **Submodules**:
  - `service/mod.rs` — `SystemOrganizationService` struct, core types, re-exports.
  - `service/organizations.rs` — Organization registration, lookup, status, region assignment.
  - `service/users.rs` — User directory management, email hash indexing.
  - `service/credentials.rs` — Credential CRUD (one-TOTP, one-recovery-code, passkey uniqueness), TOTP challenge lifecycle, recovery-code consumption (constant-time via `hash_eq()`).
  - `service/email.rs` — Email HMAC indexing, blinding-key version tracking.
  - `service/invitations.rs` — Invitation CRUD with CAS-enforced state transitions (Pending-only updates).
  - `service/onboarding.rs` — `PendingEmailVerification` / `OnboardingAccount` CRUD under `_tmp:` prefix.
  - `service/erasure.rs` — User and organization erasure (crypto-shredding backstop).
  - `service/migration.rs` — Cross-region migration helpers.
  - `service/nodes.rs` — Node registry membership / heartbeat.
  - `service/audit.rs` — Compliance erasure records (`_audit:` prefix).
  - `service/user_directory.rs` — User directory scans and migrations.
  - `service/vault_slugs.rs` — Vault slug index (entity-storage pattern: `_idx:vault:slug:{slug}` → `VaultId`).
- **Key Types/Functions**:
- `register_organization()`, `get_organization()`, `get_organization_by_slug()`, `list_organizations()`
- `update_organization_status()`, `assign_organization_to_region()`
- `register_vault_slug()`, `remove_vault_slug()`, `get_vault_id_by_slug()`: Vault slug index operations
- `register_user_directory()`, `get_user_directory()`, `get_user_id_by_slug()`, `get_user_directory_by_slug()`, `update_user_directory_status()`, `update_user_directory_region()`, `list_user_directory()`: User directory management
- `register_email_hash()`, `get_email_hash()`, `remove_email_hash()`: Email hash indexing — `register_email_hash` writes `EmailHashEntry::Active(user_id)` via postcard. `get_email_hash` returns `Option<EmailHashEntry>`. `erase_user` matches both `Active(uid)` and `Provisioning(reservation)` variants.
- `get_blinding_key_version()`, `set_blinding_key_version()`: Blinding key lifecycle
- `store_user_shred_key()`, `get_user_shred_key()`: Per-user encryption key CRUD for crypto-shredding
- `store_org_shred_key()`, `get_org_shred_key()`: Per-organization encryption key CRUD for crypto-shredding
- Onboarding CRUD: `store_email_verification()`, `get_email_verification()`, `delete_email_verification()`, `store_onboarding_account()`, `get_onboarding_account_by_hmac()`, `delete_onboarding_account()` — all use postcard serialization, `SYSTEM_VAULT_ID`, and `_tmp:` key prefix.
- Credential CRUD: `create_user_credential()` (enforces one-TOTP, one-recovery-code, passkey uniqueness via type index), `list_user_credentials()` (prefix scan with optional type filter), `get_user_credential()`, `update_user_credential()` (passkey-only field updates), `delete_user_credential()` (last-credential guard), `delete_all_user_credentials()` (bypass guards — user erasure only)
- TOTP challenge management: `create_totp_challenge()` (rate limited: max 3 active per user via private `count_active_totp_challenges()`), `get_totp_challenge()`, `delete_totp_challenge()`, `increment_totp_attempts()` (max 3 attempts), `delete_all_totp_challenges()` (user erasure only)
- Recovery code: `consume_recovery_code()` (accepts pre-hashed `[u8; 32]`, constant-time comparison via `hash_eq()`)
- Invitation CRUD: `create_invitation()` (upsert), `read_invitation()`, `update_invitation_status()` (CAS: Pending-only, set resolved_at), `delete_invitation()`, `list_invitations_by_org()` (prefix scan), `update_invitation_email_hmac()` (CAS: Pending-only, for blinding key rehash). 16 unit tests covering CRUD, CAS enforcement, org isolation, limit, overwrite.
- Token operations (defined in `system/token.rs`, implemented as methods on `SystemOrganizationService`): signing key CRUD, refresh token lifecycle, family revocation, cascade operations
- **Insights**: Slug index always created alongside organization registration. Vault slug index uses entity-storage pattern (`_idx:vault:slug:{slug}` → VaultId). Onboarding store methods use `condition: None` (overwrite semantics) — re-initiation replaces old code, re-verification replaces old onboarding account. Token operations extend the service via `impl` blocks in the sibling `token.rs` file (using `pub(super)` field visibility). Credential type index keys for multi-instance types (passkeys) include credential_id as suffix to prevent overwriting. Last-credential guard counts ALL credentials including disabled (email-code auth is always available as baseline). `erase_user()` extended with credential + TOTP challenge cleanup steps (warn + continue pattern, shred key backstop).

#### `system/keys.rs`

- **Purpose**: Key encoding for system organization entries, signing keys, refresh tokens, onboarding records, and user credentials
- **Key Types/Functions**:
- `ORG_PREFIX` (`"org:"`), `ORG_SEQ_KEY` (`"_meta:seq:organization"`): Key constants
- `organization_key(OrganizationId)`, `organization_slug_key(OrganizationSlug)`: Key constructors
- `vault_slug_key(VaultSlug)`: Vault slug index key constructor (format: `"_idx:vault:slug:{slug}"`)
- `parse_organization_key()`: Key parser
- Signing key keys: `signing_key(id)`, `signing_key_kid_index(kid)`, `signing_key_scope_index(scope)`, `SIGNING_KEY_SEQ_KEY`
- Refresh token keys: `refresh_token(id)`, `refresh_token_hash_index(hash)`, `refresh_token_family_prefix(family)`, `refresh_token_family_entry(family, id)`, `refresh_token_subject_prefix(subject)`, `refresh_token_subject_entry(subject, id)`, `refresh_token_app_vault_prefix(app, vault)`, `refresh_token_app_vault_entry(app, vault, id)`, `refresh_token_family_poisoned(family)`, `REFRESH_TOKEN_SEQ_KEY`
- Onboarding keys: `onboard_verify_key(email_hmac)`, `parse_onboard_verify_key()`, `onboard_account_key(email_hmac)`, `parse_onboard_account_key()`, `ONBOARD_VERIFY_PREFIX` (`"_tmp:onboard_verify:"`), `ONBOARD_ACCOUNT_PREFIX` (`"_tmp:onboard_account:"`)
- Shred key keys: `user_shred_key(UserId)` (format: `"_shred:user:{user_id}"`), `parse_user_shred_key()`, `USER_SHRED_KEY_PREFIX` (`"_shred:user:"`), `org_shred_key(OrganizationId)` (format: `"_shred:org:{org_id}"`), `parse_org_shred_key()`, `ORG_SHRED_KEY_PREFIX` (`"_shred:org:"`)
- Assertion name keys: `assertion_name_key(organization, app, assertion)` (format: `"assertion_name:{org_id}:{app_id}:{assertion_id}"`), `assertion_name_prefix(organization, app)`, `assertion_name_org_prefix(organization)`, `ASSERTION_NAME_PREFIX` (`"assertion_name:"`)
- Organization registry keys: `organization_registry_key(OrganizationId)` (format: `"_dir:org_registry:{id}"`), `parse_organization_registry_key()`, `ORG_REGISTRY_PREFIX` (`"_dir:org_registry:"`)
- Directory keys: `DIR_PREFIX` (`"_dir:"`), `USER_DIRECTORY_PREFIX` (`"_dir:user:"`)
- Saga/node keys: `saga_key(id)` (format: `"_meta:saga:{id}"`), `SAGA_PREFIX` (`"_meta:saga:"`), `node_key(id)` (format: `"_meta:node:{id}"`), `NODE_PREFIX` (`"_meta:node:"`)
- Signing key org index: `signing_key_org_prefix(org)` (format: `"_idx:signing_key:org:{org}:"`), `signing_key_org_entry(org, id)`, `SIGNING_KEY_ORG_PREFIX` (`"_idx:signing_key:org:"`)
- Credential keys: `USER_CREDENTIAL_PREFIX` (`"user_credential:"`), `USER_CREDENTIAL_TYPE_INDEX_PREFIX` (`"_idx:user_credential:type:"`), `USER_CREDENTIAL_SEQ_KEY` (`"_meta:seq:user_credential"`), `TOTP_CHALLENGE_PREFIX` (`"_tmp:totp_challenge:"`)
- Invitation keys: `INVITE_PREFIX` (`"invite:"`) — REGIONAL full record, `INVITE_SLUG_INDEX_PREFIX` (`"_idx:invite:slug:"`) — GLOBAL slug→InviteIndexEntry, `INVITE_TOKEN_HASH_INDEX_PREFIX` (`"_idx:invite:token_hash:"`) — GLOBAL token hash→InviteIndexEntry, `INVITE_EMAIL_HASH_INDEX_PREFIX` (`"_idx:invite:email_hash:"`) — GLOBAL per-email invitation index→InviteEmailEntry, `INVITE_SEQ_KEY` (`"_meta:seq:invite"`) — GLOBAL sequence counter
- Credential key functions: `user_credential_key(user_id, credential_id)`, `user_credential_prefix(user_id)`, `user_credential_type_index_key(user_id, credential_type)` (trailing colon — prefix generator, not point-key), `totp_challenge_key(user_id, nonce)`, `totp_challenge_prefix(user_id)`
- Tier validation: `KeyTier` enum (`Global`, `Regional`), `classify_key_tier(key)` → `Option<KeyTier>`, `validate_key_tier(key, expected)` → `Result<(), String>`
- **Insights**: Dedicated key scheme for system metadata. Underscore-prefixed keys (`_dir:`, `_idx:`, `_meta:`, `_shred:`, `_tmp:`, `_audit:`) are infrastructure; bare keys are domain entities. The `_shred:` prefix isolates crypto-shredding key material. `_dir:` holds GLOBAL directory routing entries. `_meta:` holds saga state and node membership. The `:` vs `_` ASCII ordering invariant (0x3A < 0x5F) ensures `app:` prefix scans never match `app_profile:` or `app_vault:` keys. `test_prefix_taxonomy` enforces the full convention. Signing key scope index points to the current active key's kid (replaced atomically on rotation). Refresh token indexes use hex-encoded family/hash bytes. Poisoned family key is an O(1) existence check for reuse detection. `USER_CREDENTIAL_SEQ_KEY` is the first REGIONAL `_meta:seq:` key — `classify_key_tier` checks it by exact equality BEFORE the blanket `_meta:` → Global rule (ordering is load-bearing). `user_credential_type_index_key()` generates a prefix with trailing colon; the service layer appends credential_id for multi-instance types (passkeys).

#### `system/types.rs`

- **Purpose**: Type definitions for system organization (cluster membership, saga state, service records, signing keys, refresh tokens, onboarding)
- **Key Types/Functions**:
- `SigningKeyScope` enum: `Global` | `Organization(OrganizationId)` — sum type eliminating invalid states
- `SigningKeyStatus` enum: `Active` | `Rotated` | `Revoked`
- `SigningKey` struct: 13 fields (kid, public_key_bytes, encrypted_private_key, rmk_version, scope, status, valid_from, valid_until, etc.)
- `RefreshToken` struct: 13 fields (token_hash, family, token_type, subject, organization, vault, kid, expires_at, used, etc.)
- `User` struct: includes `version: TokenVersion` field for forced session invalidation
- `UserDirectoryStatus` enum: `Active`, `Migrating`, `Deleted`, `Provisioning` — `Provisioning` appended at end (index 3) for postcard serialization compatibility.
- `EmailHashEntry` enum: `Active(UserId)` | `Provisioning(ProvisioningReservation)` — tagged enum for HMAC email index values, replacing bare `UserId` bytes. Enables O(1) idempotency in saga step 0.
- `ProvisioningReservation` struct: `user_id: UserId`, `organization_id: OrganizationId` — metadata stored in HMAC index during onboarding saga execution.
- `UserShredKey` struct: `user_id: UserId`, `key: [u8; 32]`, `created_at: DateTime<Utc>` — per-user AES-256-GCM encryption key for crypto-shredding (destroying key makes user PII in Raft logs unreadable)
- `OrgShredKey` struct: `organization: OrganizationId`, `key: [u8; 32]`, `created_at: DateTime<Utc>` — per-organization AES-256-GCM encryption key for crypto-shredding (destroying key makes org data unreadable in Raft logs)
- `Organization` struct: GLOBAL skeleton with structural fields (slug, region, tier, status, members, created_at, updated_at, deleted_at, `name: ""`) — merged with REGIONAL `OrganizationProfile` PII via `overlay_org_profile()`
- `OrganizationProfile` struct: REGIONAL PII-only (`name: String`, `updated_at: DateTime<Utc>`) — overlay for `Organization` skeleton
- `Team` struct: REGIONAL-only full record (formerly `TeamProfile` — renamed because teams have no GLOBAL skeleton, so `_profile` suffix was incorrect)
- `App` struct: includes `version: TokenVersion` field (with `#[serde(default)]`) for per-app forced session invalidation
- `ClientAssertionEntry` — `name` field removed (moved to REGIONAL state for data residency)
- `PendingEmailVerification` struct: `code_hash: [u8; 32]`, `region`, `expires_at`, `attempts: u32`, `rate_limit_count: u32`, `rate_limit_window_start` — ephemeral verification record stored regionally at `_tmp:onboard_verify:{email_hmac}`. `email` field removed (PII eliminated from this record).
- `OnboardingAccount` struct: `token_hash: [u8; 32]`, `region`, `expires_at`, `created_at` — ephemeral onboarding record with NO PII fields. Stored regionally at `_tmp:onboard_account:{email_hmac}`.
- **Insights**: Decoupled from user-facing types to maintain clean separation. `SigningKeyScope` carries `OrganizationId` data to eliminate the invalid state of Global+org_id. No `SigningAlgorithm` enum — EdDSA is the only algorithm (add enum when/if needed). `OnboardingAccount` intentionally excludes PII — email, name, and organization_name are passed in-memory from the service handler to the saga orchestrator. Postcard variant index tests guard against accidental enum reordering.

#### `system/token.rs`

- **Purpose**: State operations for signing keys and refresh tokens, implemented as methods on `SystemOrganizationService`
- **Key Types/Functions**:
- Signing key ops: `store_signing_key()`, `get_signing_key_by_kid()`, `get_active_signing_key()`, `list_signing_keys()`, `update_signing_key_status()`, `delete_org_signing_keys()`, `list_rotated_keys_past_grace()`
- Refresh token ops: `store_refresh_token()`, `get_refresh_token_by_hash()`, `mark_refresh_token_used()`, `poison_token_family()`, `is_family_poisoned()`, `revoke_token_family()`, `revoke_all_subject_tokens()`, `revoke_app_vault_tokens()`, `revoke_all_org_refresh_tokens()`, `delete_expired_refresh_tokens()`, `gc_poisoned_families()`, `increment_user_token_version()`, `revoke_all_user_sessions()`, `revoke_all_app_sessions()`, `increment_app_token_version()`
- Result types: `FamilyRevocationResult`, `SubjectRevocationResult`, `AllUserSessionsRevocationResult`, `AllAppSessionsRevocationResult`, `ExpiredTokenCleanupResult`
- **Insights**: Uses `pub(super)` on `SystemOrganizationService` fields to extend the service from a sibling file. Family revocation uses `HashSet` to deduplicate families when iterating subject/app_vault indexes. `gc_poisoned_families` requires hex parsing since family bytes are hex-encoded in keys. 32 unit tests.

---

## Crate: `inferadb-ledger-proto`

- **Purpose**: Protobuf definitions for gRPC API and domain↔proto conversions.
- **Dependencies**: `types`, `prost`, `tonic`, `zeroize`
- **Quality Rating**: ★★★★☆

### Files

#### `build.rs`

- **Purpose**: Dual-mode proto compilation (dev codegen vs pre-generated for crates.io)
- **Key Types/Functions**:
- Checks if `proto/ledger/v1/ledger.proto` file exists on disk
- Dev mode (proto exists): runs `tonic_prost_build::configure()` to generate code
- Published mode (proto missing): sets `cfg(use_pregenerated_proto)` to include `src/generated/`
- **Insights**: Enables publishing to crates.io without requiring protoc. Risk: pre-generated code can drift from .proto files if not updated.

#### `lib.rs`

- **Purpose**: Conditional include of generated code
- **Key Types/Functions**:
- `#[cfg(use_pregenerated_proto)]`: Include pre-generated code when proto files unavailable
- Re-exports: `ledger.v1` module with all message types
- **Insights**: Custom cfg flag (not a feature flag) set by build.rs controls compilation mode.

#### `convert/` (directory module)

- **Purpose**: From/TryFrom trait implementations for domain↔proto conversions
- **Structure**: `convert/mod.rs` (re-exports), `convert/domain.rs` (events), `convert/identifiers.rs` (ID newtypes), `convert/operations.rs` (entities/relationships), `convert/credentials.rs` (credential types), `convert/tokens.rs` (JWT types), `convert/statuses.rs` (invitation/event statuses), `convert/tests.rs` (unit + proptests)
  Note: Raft proto types have no dedicated conversion module — the raft-layer transport handles Raft proto types directly without going through `From`/`TryFrom`.
- **Key Types/Functions**:
- `impl From<types::Entity> for proto::Entity`: Infallible domain→proto
- `impl TryFrom<proto::Entity> for types::Entity`: Fallible proto→domain (validation)
- `vault_entry_to_proto_block()`: Accepts explicit `VaultSlug` parameter (not derived from internal `VaultId`)
- Events conversions: `From<EventScope>`, `From<&EventOutcome>`, `From<&EventEmission>`, `From<&EventEntry>` (both directions), `TryFrom<proto::EventEntry>` for domain `EventEntry`, datetime↔Timestamp helpers
- `impl std::str::FromStr for EventAction`: Iterates `EventAction::ALL` array, matches via `as_str()`
- Token conversions: `DomainTokenPair → proto::TokenPair`, `ValidatedToken ↔ ValidateTokenResponse`, `signing_key_scope_from_i32()` (`UNSPECIFIED=0` → `InvalidArgument`), `validate_signing_key_status()`, `UserSessionClaims`/`VaultAccessClaims` proto ↔ domain
- Credential conversions: `From<CredentialType>` / `TryFrom<proto::CredentialType>`, `From<TotpAlgorithm>` / `TryFrom<proto::TotpAlgorithm>`, `From<&PasskeyCredential>` / `TryFrom<proto::PasskeyCredentialData>`, `From<&TotpCredential>` / `TryFrom<proto::TotpCredentialData>`, `From<&RecoveryCodeCredential>` / `TryFrom<proto::RecoveryCodeCredentialData>`, `user_credential_to_proto()` (function, not `From` — needs explicit `UserSlug` parameter), `strip_totp_secret()`, `strip_recovery_code_hashes()`, `validate_credential_info_type()`, `credential_type_from_i32()`, `totp_algorithm_from_i32()`
- Invitation conversions: `From<InviteSlug>` / `From<proto::InviteSlug>` (owned + ref, both directions), `From<InvitationStatus>` / `TryFrom<proto::InvitationStatus>` (Unspecified→Pending default), `invitation_status_from_i32()` validator
- Covers all domain types: Block, Transaction, Operation, Entity, Relationship, VaultSlug, InviteSlug, InvitationStatus, EventEntry, TokenPair, ValidatedToken, UserCredential, etc.
- Unit tests + Proptests validating round-trip conversions (including events, tokens, and credentials)
- **Insights**: Deduplication effort (Phase 2 Task 15) removed duplicate helper functions. Events proto conversion flattens Rust's data-carrying enums. Token conversions for `SigningKeyScope` domain enum (carries `OrganizationId`) and `PublicKeyInfo ↔ SigningKey` deferred to services layer where both proto and state crate types are available. `user_credential_to_proto()` is a function (not `From` trait) because domain `UserCredential.user` is `UserId(i64)` but proto uses `UserSlug(u64)` — `From` trait doesn't support extra parameters. `strip_totp_secret()` is a defense-in-depth helper for gRPC handlers (caller obligation, not conversion-layer guarantee). Proto `TotpAlgorithm` uses `SHA1 = 0` (not `UNSPECIFIED = 0`) because SHA-1 is the RFC 6238 default.

#### `generated/ledger.v1.rs`

- **Purpose**: prost-generated Rust code from proto definitions
- **Key Types/Functions**:
- All gRPC message types: ReadRequest, WriteRequest, CreateVaultRequest, VaultSlug, etc.
- Service traits (14 proto-defined): ReadService, WriteService, OrganizationService, VaultService, SchemaService, AdminService, UserService, InvitationService, AppService, TokenService, EventsService, HealthService, SystemDiscoveryService, RaftService
- Implementations present in `inferadb-ledger-services` (13): ReadService, WriteService, OrganizationService, VaultService, AdminService, UserService, InvitationService, AppService, TokenService (via `TokenServiceImpl`), EventsService, HealthService, DiscoveryService (satisfies `SystemDiscoveryService` trait), RaftService. **SchemaService is proto-only** (no impl) — placeholder for future schema registry.
- TokenService RPCs: `CreateUserSession`, `ValidateToken`, `CreateVaultToken`, `RefreshToken`, `RevokeToken`, `RevokeAllUserSessions`, `RevokeAllAppSessions`, `CreateSigningKey`, `RotateSigningKey`, `RevokeSigningKey`, `GetPublicKeys`
- OrganizationService RPCs: `GetOrganizationTeam`, `AddTeamMember`, `RemoveTeamMember`
- Organization team messages: `GetOrganizationTeamRequest/Response`, `AddTeamMemberRequest/Response`, `RemoveTeamMemberRequest/Response`
- VaultService RPCs: `UpdateVault(UpdateVaultRequest) -> UpdateVaultResponse`
- Token messages: `TokenPair`, `CreateUserSessionRequest/Response`, `ValidateTokenRequest/Response` (with `oneof claims` for `UserSessionClaims`/`VaultAccessClaims`), `CreateVaultTokenRequest/Response`, `RefreshTokenRequest/Response`, `RevokeTokenRequest/Response`, `RevokeAllUserSessionsRequest/Response`, `CreateSigningKeyRequest/Response`, `RotateSigningKeyRequest/Response`, `RevokeSigningKeyRequest/Response`, `GetPublicKeysRequest/Response`, `PublicKeyInfo`
- `SigningKeyScope` proto enum: `UNSPECIFIED=0`, `GLOBAL=1`, `ORGANIZATION=2`
- AdminService leader transfer RPCs: `TransferLeadership(TransferLeadershipRequest) -> TransferLeadershipResponse`
- RaftService internal RPCs: `TriggerElection(TriggerElectionRequest) -> TriggerElectionResponse`
- Leader transfer messages: `TransferLeadershipRequest` (target_node_id, timeout_ms), `TransferLeadershipResponse` (success, new_leader_id, message), `TriggerElectionRequest` (leader_term, leader_id), `TriggerElectionResponse` (accepted, message)
- EventsService RPCs: `ListEvents`, `GetEvent`, `CountEvents`, `IngestEvents`
- Events messages: `EventEntry`, `EventFilter`, `ListEventsRequest/Response`, `GetEventRequest/Response`, `CountEventsRequest/Response`, `IngestEventEntry`, `IngestEventsRequest/Response`, `RejectedEvent`
- Events enums: `EventScope`, `EventOutcome`, `EventEmissionPath`
- UserService onboarding RPCs: `InitiateEmailVerification`, `VerifyEmailCode`, `CompleteRegistration`
- UserService credential RPCs: `CreateUserCredential`, `ListUserCredentials`, `UpdateUserCredential`, `DeleteUserCredential`, `CreateTotpChallenge`, `VerifyTotp`, `ConsumeRecoveryCode`
- Onboarding messages: `InitiateEmailVerificationRequest/Response`, `VerifyEmailCodeRequest/Response` (with `oneof result` for `ExistingUserSession`/`OnboardingSession`/`TotpRequired`), `CompleteRegistrationRequest/Response` (includes `email` + `region` fields, response includes `User` + `TokenPair` + `OrganizationSlug`), `ExistingUserSession`, `OnboardingSession`, `TotpRequired`
- Credential messages: `CredentialType` enum, `TotpAlgorithm` enum, `UserCredential` (with `oneof data` for passkey/totp/recovery_code), `PasskeyCredentialData`, `TotpCredentialData`, `RecoveryCodeCredentialData`, `CredentialInfo` (audit trail), `CreateUserCredentialRequest/Response`, `ListUserCredentialsRequest/Response`, `UpdateUserCredentialRequest/Response`, `DeleteUserCredentialRequest/Response`, `CreateTotpChallengeRequest/Response`, `VerifyTotpRequest/Response`, `ConsumeRecoveryCodeRequest/Response`
- Proto `VaultSlug { uint64 slug }` replaces former `VaultId { int64 id }` in all external-facing RPCs
- Token messages extended: `CreateUserSessionRequest` gains `CredentialInfo` field (audit trail for primary auth method)
- **Insights**: Large generated file. Regular updates needed when .proto changes. Leader transfer added `TransferLeadership` to AdminService and `TriggerElection` to RaftService (internal). `VerifyEmailCodeResponse` uses `oneof result` pattern — generates nested enum `verify_email_code_response::Result` in Rust. `TotpRequired` is the third variant (field 3) on `VerifyEmailCodeResponse.result` oneof.

---

## Crate: `inferadb-ledger-raft`

- **Purpose**: Operationalization layer on top of the consensus engine. Provides openraft integration (`RaftLogStore` implementing `RaftStorage`), apply-phase parallelism, saga orchestration, background jobs, rate limiting, hot-key detection, batching, graceful shutdown, leader transfer, and 30+ production features. The actual Raft engine (Reactor, Shard, WAL) lives in `inferadb-ledger-consensus`; this crate is the glue between that engine and the rest of the app. gRPC services live in `inferadb-ledger-services`.
- **Dependencies**: `types`, `store`, `state`, `proto`, `consensus`, `openraft`, `tonic`
- **Quality Rating**: ★★★★☆

**Two-layer consensus architecture**: `inferadb-ledger-consensus` owns Raft correctness (election, replication, WAL, snapshotting) as a pure event-driven engine. `inferadb-ledger-raft` (this crate) owns operationalization: apply-phase workers, job scheduling, sagas, rate limiting, leader transfer orchestration, backup/restore, runtime reconfiguration. The split keeps the engine simulation-testable while letting operational concerns evolve independently.

### Core Files

#### `lib.rs`

- **Purpose**: Public API surface (2 stable modules: `metrics`, `trace_context`; remaining modules including `snapshot` are `#[doc(hidden)]`)
- **Key Types/Functions**:
- Re-exports: `LedgerTypeConfig`, `LedgerNodeId`, `RaftLogStore`, `RateLimiter`, `HotKeyDetector`, `GracefulShutdown`, `HealthState`, `BackgroundJobWatchdog`, `EventsGarbageCollector`, `SagaOrchestrator`, `SagaOrchestratorHandle`, `SagaSubmission`, `SagaOutput`, `OnboardingPii`, `OrgPii`, `OrphanCleanupJob`, `IntegrityScrubberJob`, `TokenMaintenanceJob`, `InviteMaintenanceJob`, `OrganizationPurgeJob`, `PostErasureCompactionJob`, `UserRetentionReaper`, `AutoRecoveryJob`, `BackupJob`, `BackupManager`, `BlockCompactor`, `LearnerRefreshJob`, `ResourceMetricsCollector`, `RuntimeConfigHandle`, `TtlGarbageCollector`, `RaftManager`, `RaftManagerConfig`, `RegionConfig`, `RegionGroup`, `GrpcRaftNetworkFactory`, `RegionStorage`, `RegionStorageManager`. (`LedgerServer` moved to `inferadb-ledger-services`)
- Note: `LedgerRequest` is NOT re-exported (access via `types::LedgerRequest`). `RaftPayload` is NOT re-exported either (access via `log_storage` internals). `RaftPayload` wraps `LedgerRequest` with `proposed_at` for deterministic timestamps.
- 30+ `#[doc(hidden)] pub mod` declarations for server-internal infrastructure (includes `event_writer`, `events_gc`, `snapshot`, `leader_transfer`)
- **Insights**: Phase 2 Task 2 cleaned up public API. 2 stable modules + many doc-hidden modules. `leader_transfer` module added for graceful leadership handoff. Excellent encapsulation.

#### `log_storage/`

- **Purpose**: openraft LogStore and StateMachine implementation, log storage, externalized state persistence, streaming snapshot building
- **Structure**:
- `log_storage/mod.rs` — Metadata constants, `ShardChainState`, re-exports, test suite (test fixtures use `wrap_payload` helper to construct `EntryPayload::Normal(RaftPayload { ... })`, includes deterministic timestamp tests, eviction tests, pending writes tests, state persistence tests)
- `log_storage/types.rs` — `AppliedState`, `AppliedStateCore` (5-field persistence struct for new snapshot format), `PendingExternalWrites` (20-field accumulator for externalized table writes including insert/delete pairs for organizations, vaults, vault_heights/hashes/health, sequences, client_sequences, and all 5 slug indexes), `ClientSequenceEntry` (sequence + last_seen + idempotency_key + request_hash), `OrganizationMeta` (fields: `organization: OrganizationId`, `slug: OrganizationSlug`, `region: Region`, `status`, `tier`, `pending_region: Option<Region>`, `storage_bytes: u64`), `VaultMeta` (fields: `organization: OrganizationId`, `vault: VaultId`, `slug: VaultSlug`, `name: Option<String>`, `deleted: bool`, `last_write_timestamp: u64`, `retention_policy: BlockRetentionPolicy`), `SequenceCounters` (includes `signing_key`, `refresh_token`, and `invite` fields with `next_signing_key()`/`next_refresh_token()`/`next_invite()` methods), `VaultHealthStatus`. `AppliedState` maintains bidirectional slug ↔ internal ID maps for both organizations and vaults. Deleted: `CombinedSnapshot` (replaced by file-based streaming snapshots).
- `log_storage/accessor.rs` — `AppliedStateAccessor` (29 pub query methods including org/vault/user/team/app slug resolution, vault heights/health, organization storage, client sequences), `IdempotencyCheckResult` enum (`AlreadyCommitted`/`KeyReused`/`Miss`), `client_idempotency_check()` for cross-failover deduplication via replicated `ClientSequenceEntry`. Idempotency unit tests.
- `log_storage/store.rs` — `RaftLogStore` struct definition with `client_sequence_eviction: ClientSequenceEvictionConfig` field, creation/config/accessor methods, optional `event_writer: Option<EventWriter<B>>`. New externalized state methods: `flush_external_writes()` (writes 9 tables atomically), `save_state_core()` (version-sentinel + AppliedStateCore + flush in single WriteTransaction), `load_state_from_tables()` (three-way format detection: new/old/fresh). 15+ persistence tests + benchmark test.
- `log_storage/operations/` — Directory module (was a single file, now split into submodules): `mod.rs` (dispatch logic: `apply_request()` and `apply_request_with_events()`), `token_helpers.rs` (token apply handlers), `app_helpers.rs` (app apply handlers), `org_helpers.rs` (organization apply handlers), `team_helpers.rs` (team apply handlers), `helpers.rs` (shared apply helpers). Handles all `LedgerRequest` variants including 10 token variants and 6 onboarding `SystemRequest` variants. Onboarding handlers: `CreateEmailVerification` (rate limiting via count+window, overwrites existing record), `VerifyEmailCode` (constant-time `hash_eq()`, branches on `existing_user_hmac_hit`), `CreateOnboardingUser` (step 0: idempotency guard via `EmailHashEntry::Provisioning`, CAS HMAC reservation, ID allocation), `WriteOnboardingUserProfile` (step 1: write all PII regionally, create session, delete onboarding account), `ActivateOnboardingUser` (step 2: activate user directory, update HMAC index to Active, persist org meta to B+ tree, sync org registry status), `CleanupExpiredOnboarding` (GC scan of `_tmp:` prefixes with `MAX_ONBOARDING_SCAN` limit). Also `UpdateUserProfile` handler for PII-split user updates. Token apply handlers: `CreateSigningKey` (idempotent for saga retry), `RotateSigningKey` (grace period or immediate revocation), `RevokeSigningKey`, `TransitionSigningKeyRevoked` (background job), `CreateRefreshToken`, `UseRefreshToken` (atomic check-and-consume with poisoned-family detection, version binding, scope re-validation), `RevokeTokenFamily`, `RevokeAllUserSessions` (atomic: revoke + version increment), `RevokeAllAppSessions` (atomic: revoke + app version increment), `DeleteExpiredRefreshTokens` (+ poisoned family GC). Cascade hooks: `SetAppEnabled` revokes subject tokens on disable, `RemoveAppVault` revokes app-vault tokens, `PurgeOrganization`/`DeleteOrganization` cascade signing key deletion and refresh token revocation. All time-dependent logic uses `proposed_at` from `RaftPayload`.
- `log_storage/raft_impl.rs` — `RaftLogReader`, `LedgerSnapshotBuilder` (reads from DB, not in-memory `Arc<RwLock<AppliedState>>`), `RaftStorage` trait impls. `apply_to_state_machine` creates `PendingExternalWrites`, passes through apply loop, saves via `save_state_core()`. Client sequence TTL eviction triggers on `log_id.index % eviction_interval`. `write_snapshot_to_file()` uses `SnapshotWriter` for zstd-compressed, SHA-256 checksummed file-based snapshots. `install_snapshot()` uses `SyncSnapshotReader` with `block_in_place()` for sync zstd decoding — streams directly into WriteTransactions with zero staging. `collect_snapshot_events()` uses org_ids + timestamp cutoff via `scan_apply_phase_ranged()`.
- **Key Types/Functions**:
- `RaftLogStore`: Implements openraft's `RaftStorage` trait (combined log + state machine)
- `AppliedState`: State machine with vault heights, organizations, sequences
- `apply_request()`: Dispatches to operation handlers for entities, relationships, vaults, organizations
- `AppliedStateAccessor`: Shared read accessor (passed to services without direct Raft storage access)
- **Insights**: Successfully split from monolithic file into directory module. Fields use `pub(super)` for cross-submodule access within the same effective boundary.

#### `snapshot.rs` — NEW

- **Purpose**: Streaming file-based snapshot infrastructure with zstd compression and SHA-256 integrity verification
- **Key Types/Functions**:
- `SNAPSHOT_MAGIC` (`b"LSNP"`), `SNAPSHOT_VERSION` (1), `ZSTD_LEVEL` (3), `CHECKSUM_SIZE` (32): Format constants
- `SnapshotError` enum: IO, compression, checksum, magic/version validation errors
- `HashingWriter<W>`: Wrapper that computes SHA-256 digest while writing (streaming checksum, no buffering)
- `SnapshotWriter<W: AsyncWrite + Unpin>`: Async streaming writer — magic header → version → zstd-compressed table data → SHA-256 checksum trailer. Methods: `new()`, `write_table_entries()`, `finish() -> [u8; 32]`
- `SnapshotReader`: Async streaming reader — validates magic/version, decompresses zstd, yields `(table_id, key, value)` triples. Methods: `new()`, `read_entries() -> Vec<(u8, Vec<u8>, Vec<u8>)>`, `verify_checksum()`
- `SyncSnapshotReader`: Synchronous variant for `install_snapshot()` (openraft requires sync context via `block_in_place()`). Streams directly into WriteTransactions with zero staging.
- `SNAPSHOT_TABLE_IDS`: Constant array of valid table IDs for snapshot inclusion
- `validate_table_id()`: Guards against unknown table IDs during restore
- **Insights**: Replaces the old `CombinedSnapshot` (in-memory postcard blob) with streaming file-based snapshots. Enables O(1) memory snapshots regardless of state size. The async/sync split (`SnapshotWriter`/`SyncSnapshotReader`) is necessary because openraft's `install_snapshot` callback runs in a sync context. SHA-256 checksum covers the entire compressed payload for tamper detection.

#### `leader_transfer.rs` — NEW

- **Purpose**: Graceful leader transfer coordination — orchestrates leadership handoff before shutdown or maintenance
- **Key Types/Functions**:
- `LeaderTransferConfig`: bon `#[derive(Builder)]` with `timeout` (10s), `poll_interval` (50ms), `replication_timeout` (5s)
- `LeaderTransferError`: snafu error with 8 variants (NotLeader, NoTarget, TransferInProgress, ReplicationTimeout, TargetRejected, Timeout, Connection, Rpc)
- `TransferGuard<'a>`: RAII guard over `&AtomicBool` — `compare_exchange(false, true)` on entry, `store(false)` on `Drop` for cleanup even on early `?` returns
- `transfer_leadership(raft, target, transfer_lock, config) -> Result<u64, LeaderTransferError>`: Core coordination function
- Algorithm: verify leader → select best target (most caught-up follower via `metrics.replication`) → wait for replication catch-up → send `TriggerElection` RPC to target → poll until leader changes or timeout
- Unit tests: config defaults, custom config, guard reset, concurrent lock prevention, error display
- **Insights**: Composes transfer from openraft primitives (no built-in `transfer_leader()` API). Accepts _any_ new leader (not just requested target) — pragmatic for "stop being leader before shutdown." Concurrency guard shared between AdminService RPC and GracefulShutdown.

#### `server.rs` (in `inferadb-ledger-services`)

- **Purpose**: LedgerServer builder with all gRPC services and Raft integration
- **Key Types/Functions**:
- `LedgerServer::builder()`: bon-based builder with 20+ config options including `jwt_engine`, `jwt_config`, `key_manager` for TokenService, `email_blinding_key` and `saga_orchestrator_handle` for onboarding
- `serve(self) -> Result<()>`: Start gRPC server with all services (shutdown signal wired externally)
- `health_state: HealthState` field — threaded to `WriteService` and `AdminService` for drain-phase proposal rejection
- Optional `email_blinding_key: Option<Arc<EmailBlindingKey>>` for onboarding email hashing
- Optional `saga_orchestrator_handle: Option<SagaOrchestratorHandle>` for synchronous saga submission from service handlers
- Optional `events_db: Option<EventsDatabase<FileBackend>>` for EventsService registration
- Optional `event_handle: Option<EventHandle<FileBackend>>` for handler-phase event recording in services
- Integrates: Raft node, all services, metrics, tracing, event logging, health checks
- **Insights**: Central wiring point. Excellent builder pattern. Supports graceful shutdown with connection draining. Threads `HealthState` to write and admin services for drain guard enforcement. When `events_db` is present, `EventsServiceServer` is registered on the router with `api_version_interceptor`. `email_blinding_key` and `saga_orchestrator_handle` are threaded into `ServiceContext` and `UserService` for onboarding handler access.

#### `types.rs`

- **Purpose**: Raft type configuration, payload wrapper, and request/response types
- **Key Types/Functions**:
- `RaftPayload`: Wrapper around `LedgerRequest` with leader-assigned `proposed_at: DateTime<Utc>` timestamp. Ensures all replicas apply with identical timestamps (deterministic apply-phase).
- `LedgerTypeConfig`: openraft type config with `D = RaftPayload` (was `D = LedgerRequest`)
- `LedgerNodeId`: Newtype for node ID (Snowflake ID)
- `LedgerRequest`: 45 variants — 31 structural + 10 token + 4 invitation:
  - Structural: `Write`, `CreateVault`, `DeleteOrganization`, `DeleteVault`, `UpdateVault`, `SuspendOrganization`, `ResumeOrganization`, `RemoveOrganizationMember`, `UpdateOrganizationMemberRole`, `AddOrganizationMember`, `PurgeOrganization`, `StartMigration`, `CompleteMigration`, `UpdateVaultHealth`, `System(SystemRequest)`, `BatchWrite`, `CreateOrganizationTeam`, `DeleteOrganizationTeam`, `CreateApp`, `DeleteApp`, `SetAppEnabled`, `SetAppCredentialEnabled`, `RotateAppClientSecret`, `CreateAppClientAssertion`, `DeleteAppClientAssertion`, `SetAppClientAssertionEnabled`, `AddAppVault`, `UpdateAppVault`, `RemoveAppVault`, `EncryptedUserSystem(EncryptedUserSystemRequest)`, `EncryptedOrgSystem(EncryptedOrgSystemRequest)`. Note: `CreateOrganization` is a `SystemRequest` variant, not a top-level `LedgerRequest`.
  - Signing keys: `CreateSigningKey`, `RotateSigningKey`, `RevokeSigningKey`, `TransitionSigningKeyRevoked`
  - Refresh tokens: `CreateRefreshToken`, `UseRefreshToken`, `RevokeTokenFamily`, `RevokeAllUserSessions`, `RevokeAllAppSessions`, `DeleteExpiredRefreshTokens`
  - Invitation: `CreateOrganizationInvite` (allocate InviteId, write 3 GLOBAL indexes, compute expires_at from proposed_at), `ResolveOrganizationInvite` (CAS Pending-only, update email entry status, remove token hash index), `PurgeOrganizationInviteIndexes` (delete slug + email_hash GLOBAL indexes), `RehashInviteEmailIndex` (delete old + insert new email_hash entry)
- `LedgerResponse`: Operation results (OrganizationCreated, VaultCreated, VaultUpdated, AllAppSessionsRevoked, success/error) + 11 token response variants + 6 onboarding response variants + 7 credential response variants + 5 invitation response variants (`OrganizationMemberAdded { already_member }`, `OrganizationInviteCreated { invite_id, invite_slug, expires_at }`, `OrganizationInviteResolved { invite_id }`, `OrganizationInviteIndexesPurged`, `InviteEmailIndexRehashed`):
  - Onboarding: `EmailVerificationCreated`, `EmailCodeVerified { result: EmailCodeVerifiedResult }`, `OnboardingUserCreated { user_id, organization_id }`, `OnboardingUserProfileWritten { refresh_token_id }`, `OnboardingUserActivated`, `OnboardingCleanedUp { verification_codes_deleted, onboarding_accounts_deleted, totp_challenges_deleted }`
  - Credential: `UserCredentialCreated { credential_id }`, `UserCredentialUpdated { credential_id }`, `UserCredentialDeleted { credential_id }`, `TotpChallengeCreated { nonce }`, `TotpVerified { refresh_token_id }`, `RecoveryCodeConsumed { refresh_token_id, remaining_codes }`, `TotpAttemptIncremented { attempts }`
- `EmailCodeVerifiedResult` enum: `ExistingUser` | `TotpRequired { nonce }` | `NewUser` — `ExistingUser` and `NewUser` have no fields. `TotpRequired` carries the challenge nonce for TOTP-enabled users. Service handler creates sessions (existing user without TOTP), returns onboarding token (new user), or returns challenge nonce (TOTP required).
- `TotpPreResolve` struct: `nonce: [u8; 32]`, `expires_at: DateTime<Utc>`, `user_id: UserId`, `user_slug: UserSlug` — embedded in `SystemRequest::VerifyEmailCode` as `Option<TotpPreResolve>` (replaces PRD's 4 `Option` fields — "parse, don't validate" pattern eliminates invalid states)
- `SystemRequest` variants (onboarding): `CreateEmailVerification` (REGIONAL, PII removed — email no longer in Raft log), `VerifyEmailCode` (REGIONAL, with `existing_user_hmac_hit: bool` pre-resolved at service layer), `CleanupExpiredOnboarding` (REGIONAL, no fields), `CreateOnboardingUser` (GLOBAL, saga step 0), `WriteOnboardingUserProfile` (REGIONAL, saga step 1, PII sealed via `sealed_pii`/`pii_nonce`), `ActivateOnboardingUser` (GLOBAL, saga step 2). Also `UpdateUserProfile` (REGIONAL, PII split from former `UpdateUser`).
- `SystemRequest` variants (team/assertion): `AddTeamMember` (REGIONAL), `RemoveTeamMember` (REGIONAL), `WriteClientAssertionName` (REGIONAL), `DeleteClientAssertionName` (REGIONAL)
- `SystemRequest` variants (invitation — encrypted via `EncryptedOrgSystem`): `WriteOrganizationInvite` (REGIONAL, full invitation record with PII email), `UpdateOrganizationInviteStatus` (REGIONAL, CAS Pending-only, uses proposed_at as resolved_at), `DeleteOrganizationInvite` (REGIONAL, used by retention reaper), `RehashInvitationEmailHmac` (REGIONAL, updates invitee_email_hmac for blinding key rotation)
- `SystemRequest` variants (credential CRUD — encrypted via `EncryptedUserSystemRequest`): `CreateUserCredential`, `UpdateUserCredential`, `DeleteUserCredential` (all REGIONAL)
- `SystemRequest` variants (TOTP/recovery — plain `LedgerRequest::System`, no PII): `CreateTotpChallenge`, `ConsumeTotpAndCreateSession`, `ConsumeRecoveryAndCreateSession`, `IncrementTotpAttempt` (all REGIONAL)
- `SystemRequest` variants (user management — GLOBAL): `CreateUser`, `UpdateUser`, `DeleteUser`, `EraseUser`, `UpdateUserDirectoryStatus`, `MigrateExistingUsers`
- `SystemRequest` variants (user email — REGIONAL, encrypted): `CreateUserEmail`, `DeleteUserEmail`, `VerifyUserEmail`
- `SystemRequest` variants (organization — GLOBAL unless noted): `CreateOrganization`, `UpdateOrganizationRouting`, `UpdateOrganizationStatus`, `UpdateOrganizationProfile` (REGIONAL, PII sealed), `WriteOrganizationProfile` (REGIONAL, PII sealed), `PurgeOrganizationRegional` (REGIONAL)
- `SystemRequest` variants (node/cluster — GLOBAL): `AddNode`, `RemoveNode`
- `SystemRequest` variants (email hash — GLOBAL): `RegisterEmailHash`, `RemoveEmailHash`, `SetBlindingKeyVersion`, `UpdateRehashProgress`, `ClearRehashProgress`
- `SystemRequest` variants (app/team profiles — REGIONAL, encrypted via `EncryptedOrgSystem`): `WriteTeam`, `DeleteTeam`, `UpdateTeamMemberRole`, `WriteAppProfile`, `DeleteAppProfile`
- `SystemRequest` PII sealing: `WriteOrganizationProfile` uses `sealed_name`/`name_nonce`/`shred_key_bytes` instead of plaintext `name`. `CreateEmailVerification` removed `email` field. `CreateAppClientAssertion` removed `name` field (moved to `WriteClientAssertionName`).
- **Insights**: Type-safe Raft integration. `RaftPayload` wraps `LedgerRequest` at the proposal boundary so leaders embed wall-clock timestamps — all replicas apply with the same timestamp, producing byte-identical event storage, B+ tree keys, and pagination cursors across nodes. Both `CreateOrganization` and `CreateVault` include pre-generated slugs (`OrganizationSlug`/`VaultSlug`) for atomic slug index insertion during state machine apply. Data residency invariant: no plaintext PII in ANY Raft entry (GLOBAL or REGIONAL). PII-carrying variants use sealed encryption — `WriteOrganizationProfile` seals org name with per-org shred key, `WriteOnboardingUserProfile` seals PII with user shred key. `EncryptedOrgSystem` wraps org-scoped SystemRequests encrypted with OrgShredKey for crypto-shredding. `classify_system_request()` exhaustive test forces compile-time review of new variants.

#### `error.rs`

- **Purpose**: ServiceError, RecoveryError, SagaError with gRPC status code mapping, ErrorDetails enrichment
- **Key Types/Functions**:
- `ServiceError`: snafu error with 10 variants (Storage, Raft, RateLimited, Timeout, Snapshot, etc.)
- `RecoveryError`: 8 variants for auto-recovery failures
- `SagaError`: 11 variants for distributed transaction failures (Serialization, Deserialization, SagaRaftWrite, StateRead, EntityNotFound, SequenceAllocation, UnexpectedSagaResponse, KeyGeneration, KeyEncryption, OrchestratorShutdown, PiiLost)
- `OrphanCleanupError`: 2 variants for resource leak cleanup
- `classify_raft_error(msg: &str) -> Code`: Maps Raft error messages to gRPC codes
- `is_leadership_error(msg: &str) -> bool`: Detects leadership errors for UNAVAILABLE
- **Insights**: Comprehensive error classification across multiple domains. Clients can retry UNAVAILABLE (leadership change), not FAILED_PRECONDITION.

### Service Layer (in `inferadb-ledger-services`)

#### `services/admin.rs`

- **Purpose**: AdminService gRPC implementation (cluster management, runtime config, backup/restore, leader transfer, blinding key rotation, region management)
- **Key Types/Functions**:
- `create_snapshot()`, `check_integrity()`: Snapshot and integrity operations
- `join_cluster()`, `leave_cluster()`, `get_cluster_info()`, `get_node_info()`: Cluster membership
- `recover_vault()`, `simulate_divergence()`, `force_gc()`: Recovery and maintenance
- `transfer_leadership()`: Leader transfer RPC — validates timeout (cap 60s, default 10s), calls `leader_transfer::transfer_leadership()`, records metrics, emits canonical log line. Error mapping: NotLeader/NoTarget/TargetRejected→FAILED_PRECONDITION, TransferInProgress→ABORTED, ReplicationTimeout/Timeout→DEADLINE_EXCEEDED, Connection/Rpc→INTERNAL
- `update_config()`, `get_config()`: Runtime reconfiguration RPCs
- `create_backup()`, `list_backups()`, `restore_backup()`: Backup/restore RPCs
- `rotate_blinding_key()`, `get_blinding_key_rehash_status()`: Email blinding key rotation
- `rotate_region_key()`, `get_rewrap_status()`: Region master key rotation
- `migrate_existing_users()`, `provision_region()`: Region management
- `health_state: Option<HealthState>` field — drain guard on mutating RPCs. `transfer_leadership` exempt (must work during drain)
- `transfer_lock: Arc<AtomicBool>` — shared concurrency guard with `GracefulShutdown`
- `event_handle: Option<EventHandle<B>>` for handler-phase events (ConfigurationChanged, SnapshotCreated, BackupCreated, BackupRestored, IntegrityChecked, VaultRecovered, quota denial)
- **Insights**: Largest service file — includes cluster management, runtime config, backup management, leader transfer, key rotation, and comprehensive tests. Handler-phase events replace the former `AuditLogger`.

#### `services/write.rs`

- **Purpose**: WriteService gRPC implementation (entity/relationship mutations)
- **Key Types/Functions**:
- `write()`, `batch_write()`: Entity/relationship mutations
- `create_relationship()`, `delete_relationship()`: Authorization tuple mutations
- `health_state: Option<HealthState>` field — drain guard via `check_not_draining()` before proposal submission in `write()` and `batch_write()`
- Rate limiting, hot key detection, validation, quota enforcement
- `event_handle: Option<EventHandle<B>>` for handler-phase denial events (rate limit, validation, quota)
- Error classification via `classify_batch_error()`
- **Insights**: Core data path. Rate limiting + hot key detection protect cluster. Drain guard returns `UNAVAILABLE` during Draining phase (clients retry on another node). Batch writes go through BatchWriter. Includes extensive inline tests.

#### `services/read.rs`

- **Purpose**: ReadService gRPC implementation (entity/relationship queries)
- **Key Types/Functions**:
- `read()`, `batch_read()`: Entity reads
- `list_relationships()`: Relationship queries with direction filtering
- `list_resources()`: List resources for a subject
- `list_entities()`: List entities by prefix
- Pagination support via PageToken
- **Insights**: Read path with pagination. Relationship queries use dual indexes for efficient bidirectional lookups. Includes comprehensive inline tests.

#### `services/health.rs`

- **Purpose**: HealthService with readiness/liveness/startup probes
- **Key Types/Functions**:
- `check(type: ProbeType) -> HealthCheckResponse`
- Probes: readiness (Raft ready, dependencies healthy — returns `false` for both Draining and ShuttingDown), liveness (process alive), startup (data_dir writable)
- Phase reporting via `NodePhase::as_str()` in details map: `"starting"`, `"ready"`, `"draining"`, `"shutting_down"`
- DependencyHealthChecker: disk writability, Raft lag, peer reachability
- **Insights**: Three-probe pattern for Kubernetes. Readiness gates traffic (fails during Draining — removes pod from Service endpoints without restarting), liveness triggers restart, startup delays initial traffic.

#### `services/helpers.rs`

- **Purpose**: Shared service utilities (rate limiting, validation, metadata extraction, drain guard, email validation)
- **Key Types/Functions**:
- `check_not_draining(health_state: Option<&HealthState>) -> Result<(), Status>`: Returns `UNAVAILABLE` if node is in Draining or ShuttingDown phase. Used by write and admin services before proposal submission.
- `check_rate_limit()`: Rate limit check with rich ErrorDetails
- `validate_operations()`: Validates write operations
- `record_hot_keys()`, `hash_operations()`: Hot key detection helpers
- `extract_caller()`: Common caller extraction
- `load_app()`, `read_vault_connection()`: App/vault lookup helpers
- `error_code_to_status()`, `storage_err()`, `create_replay_context()`: Error conversion helpers
- Note: `validate_email()`, `validate_user_name()`, `validate_organization_name()` are in the types crate's `validation` module, NOT in helpers.rs. Service handlers call them via `validation::validate_email()`.
- **Insights**: Phase 2 Task 1 extracted shared code from write/admin services. `check_not_draining()` added for leader transfer sprint — centralizes the drain guard pattern. `ServiceContext` (in `service_infra.rs`) gained `propose_regional(region, request)` method + `regional_state(region)` + `email_blinding_key: Option<Arc<EmailBlindingKey>>` for cross-region Raft proposals and onboarding.

#### `services/metadata.rs`

- **Purpose**: Request/response metadata helpers (correlation IDs, tracing)
- **Key Types/Functions**:
- `status_with_correlation()`: Injects x-request-id, x-trace-id, ErrorDetails
- `extract_trace_context()`: W3C Trace Context extraction
- `extract_transport_metadata()`: SDK version, forwarded-for headers
- **Insights**: Central point for metadata injection/extraction. Supports tracing and debugging.

#### `services/token.rs`

- **Purpose**: TokenService gRPC implementation — JWT token creation, validation, refresh, revocation, and signing key management
- **Key Types/Functions**:
- `TokenServiceImpl`: Struct with `ServiceContext`, `Arc<JwtEngine>`, `JwtConfig`, `Arc<dyn RegionKeyManager>`, optional `RateLimiter`
- `create_user_session()`: Forwarded to leader — resolves user, verifies active, signs access token, generates refresh token, proposes `CreateRefreshToken` through Raft
- `validate_token()`: Local read — defense-in-depth algorithm enforcement (raw header pre-check + library-level), `kid` UUID format validation, grace period check for Rotated keys, `TokenVersion` check for user sessions
- `create_vault_token()`: Forwarded to leader — resolves org/app/vault, verifies app enabled + connection exists + scopes subset, signs vault access token, generates refresh token
- `refresh_token()`: Forwarded to leader — unified for both token types. Minimal service work (hash + propose `UseRefreshToken`). State machine is authority for validation, version checking, scope re-validation. Signs new access token using authoritative data from Raft response.
- `revoke_token()`, `revoke_all_user_sessions()`: Forwarded to leader — family revocation and atomic token+version revocation
- `authenticate_client_assertion()`: Client assertion (Ed25519 JWT) verification for app authentication
- `create_signing_key()`, `rotate_signing_key()`, `revoke_signing_key()`: Signing key lifecycle with envelope encryption
- `get_public_keys()`: Local read — returns active + rotated (exclude revoked)
- Rate limiting on `create_vault_token` via per-app `app:{id}` client key
- **Insights**: Write RPCs forwarded to leader for consistent reads. `ValidateToken` and `GetPublicKeys` read local state. `RefreshTokenRotated` response carries authoritative `token_version` and `allowed_scopes` — prevents TOCTOU races.

#### `jwt.rs` (in `inferadb-ledger-services`)

- **Purpose**: JWT signing/validation engine with ArcSwap cache, envelope encryption, and refresh token utilities
- **Key Types/Functions**:
- `JwtEngine`: Lock-free validation via `ArcSwap<HashMap<String, Arc<CachedSigningKey>>>`. `CachedSigningKey` holds `DecodingKey` (public, cached) + `Zeroizing<Vec<u8>>` private key bytes (zeroized on drop). `EncodingKey` built on-demand per signing op (not cached — opaque, can't be zeroized).
- `sign_user_session()`, `sign_vault_token()`: Build `EncodingKey` from cached private bytes, sign, drop. Sets `nbf = iat`.
- `validate()`: Defense-in-depth algorithm enforcement (raw header string check + `Validation::algorithms(&[Algorithm::EdDSA])`). `kid` UUID format validation before state lookup. Error normalization (identical UNAUTHENTICATED for "kid not found" and "signature invalid"). Grace period check for Rotated keys.
- `load_key()`, `evict_key()`, `has_cached_key()`: ArcSwap copy-on-write cache management (note: `get_cached_key()` is private)
- `JwtError`: 6-variant snafu error (Signing, Decoding, KeyDecryption, KeyEncryption, Token, StateLookup)
- `scope_to_region()`: `SigningKeyScope::Global → Region::GLOBAL`, `Organization(id) → org's region`
- `encrypt_private_key()` / `decrypt_private_key()`: Envelope encryption using existing `store::crypto::operations` (generate_dek, wrap_dek, unwrap_dek). `kid` as AAD.
- `generate_refresh_token() -> (String, [u8; 32])`: `ilrt_{base64url(32 bytes)}` format (48 chars). SHA-256 hash of full prefixed string.
- `generate_family_id() -> [u8; 16]`: 16 random bytes
- **Insights**: `jsonwebtoken` v10 with `rust_crypto` feature for auto CryptoProvider registration. DER encoding asymmetry: `from_ed_der(raw_32_bytes)` for decoding, `from_ed_der(pkcs8_48_bytes)` for encoding. Refresh token `ilrt_` prefix enables secret scanning tool detection (truffleHog, GitHub scanning). 35+ unit tests.

#### `services/user.rs`

- **Purpose**: UserService gRPC implementation — user management, self-service onboarding, credential CRUD, TOTP verification, recovery code consumption
- **Key Types/Functions**:
- `initiate_email_verification()`: Validates email + region, computes HMAC, generates verification code via `compute_code_hash`, proposes `CreateEmailVerification` to regional Raft group via `propose_regional`
- `verify_email_code()`: Pre-resolves existing user from GLOBAL HMAC index (`existing_user_hmac_hit`) AND TOTP status. Existing-user path: reads user from actual region, checks suspended/deleted, updates `verified_at`, creates session via `CreateRefreshToken` in user's actual region. If user has TOTP enabled, pre-generates challenge nonce and embeds `TotpPreResolve` in the Raft proposal — state machine atomically consumes email code + creates `PendingTotpChallenge`, returns `TotpRequired { nonce }`. New-user path: returns onboarding token.
- `complete_registration()`: Idempotency check via GLOBAL HMAC index (`EmailHashEntry::Active` → fresh session, `Provisioning` → ALREADY_EXISTS). Validates onboarding token via `hash_eq`. Submits `CreateOnboardingUserSaga` via `saga_handle.submit_saga(SagaSubmission { record, pii, notify })`. Awaits `tokio::time::timeout(min(SAGA_COMPLETION_TIMEOUT, remaining_deadline - 2s), receiver)`. Signs JWT with `TokenVersion(1)` on completion. Returns `DEADLINE_EXCEEDED` on timeout (saga continues in background, client retries).
- Credential CRUD: `create_user_credential()`, `list_user_credentials()`, `update_user_credential()`, `delete_user_credential()` — route through `propose_regional_encrypted()` (encrypts with `UserShredKey` for crypto-shredding). TOTP secret stripping in gRPC response serialization via `strip_totp_secret()`.
- `create_totp_challenge()`: Creates `PendingTotpChallenge` for passkey-initiated TOTP flow. Trusted service-to-service call from Control after passkey verification.
- `verify_totp()`: Service-layer TOTP verification → Raft session creation. Reads challenge, decrypts TOTP secret via `UserShredKey`, verifies code with `SystemTime::now()` (±1 time-step window). Proposes `ConsumeTotpAndCreateSession`. Signs JWT on success, returns `TokenPair`.
- `consume_recovery_code()`: SHA-256 hashes input, reads challenge, proposes `ConsumeRecoveryAndCreateSession`. Returns `TokenPair` + remaining codes.
- Helper: `verify_totp_code()` — HMAC-SHA1/SHA256/SHA512 verification with ±1 time-step window, constant-time comparison via `subtle::ConstantTimeEq`
- Helper: `dynamic_truncate()` — RFC 4226 code extraction from HMAC digest
- Helper: `sign_session_after_challenge()` — shared JWT signing + user lookup between `verify_totp` and `consume_recovery_code` (~50 lines deduplicated)
- Helper: `resolve_user_region()` — region lookup for regional Raft proposal
- Onboarding RPCs reject with `FAILED_PRECONDITION` if `email_blinding_key` is not configured.
- **Insights**: Cross-region session creation for existing users: verification code validated in client-specified region, `RefreshToken` created in user's actual region (may differ). The `complete_registration` handler is the only synchronous saga consumer — uses `oneshot` channel for saga completion notification, with timeout fallback to idempotent retry. TOTP verification is split across service layer (non-deterministic HMAC with `SystemTime::now()`) and state machine (deterministic challenge consumption with `proposed_at`). `verify_totp_code` checks code length before constant-time comparison to avoid leaking configured `digits` via timing.

#### `services/invitation.rs`

- **Purpose**: InvitationService gRPC implementation — 10 RPCs for organization invitation lifecycle (admin + user operations)
- **Key Types/Functions**:
- `InvitationService` struct with `ServiceContext`, 8 gRPC handlers + shared helpers
- **Admin operations**: `create_organization_invite()` (email validation, rate limiting, member check with timing equalization, token generation, GLOBAL+REGIONAL proposals), `list_organization_invites()` (REGIONAL prefix scan with lazy expiration), `get_organization_invite()`, `revoke_organization_invite()` (CAS-guarded GLOBAL+REGIONAL)
- **User operations**: `list_received_invitations()` (multi-email HMAC scan, org name via overlay_org_profile), `get_invitation_details()`, `accept_invitation()` (multi-email HMAC match, partial failure recovery, GLOBAL-first proposal ordering, AddOrganizationMember + optional AddTeamMember), `decline_invitation()` (timing equalization)
- Helpers: `resolve_invite_slug()`, `scan_email_entries()` (prefix scan with 500-entry ceiling), `org_region()` (live OrganizationRegistry lookup), `validate_org_admin()`, `get_user_email_hmacs()` (multi-email), `email_matches()` (constant-time via `subtle::ConstantTimeEq`), `timing_equalization_reads()`, `build_admin_invitation()`/`build_user_invitation()` (role-based field population), `resolve_invitation()` (GLOBAL+REGIONAL dual proposal), `generate_token()` (32-byte CSPRNG + SHA-256)
- **Insights**: InviteSlug resolution goes through GLOBAL `_idx:invite:slug:{slug}` index directly (not through SlugResolver/AppliedStateAccessor). Multi-email HMAC matching ensures invitations sent to any verified email are actionable. Timing equalization via dummy reads prevents invitation existence leaks. Per-user and per-org sliding-window rate counters are separate from the existing token bucket RateLimiter.

#### Additional Services

- `services/raft.rs`: RaftService (inter-node Raft RPCs) including `TriggerElection` RPC handler — validates leader term (rejects stale), calls `raft.trigger().elect()`, records metrics via `record_trigger_election()`. Routes to the correct region via `RaftManager`.
- `services/discovery.rs`: DiscoveryService (cluster membership)
- `services/forward_client.rs`: Leader forwarding
- `services/events.rs`: `EventsService` — EventsService gRPC implementation with 4 RPCs (`ListEvents`, `GetEvent`, `CountEvents`, `IngestEvents`). `GetEvent` uses O(log n) `EventStore::get_by_id()` via secondary index (replaces former O(n) full-org scan with `COUNT_SCAN_LIMIT` cap, eliminating false-not-found for orgs with >100k events). `ListEvents` supports HMAC-signed `EventPageToken` pagination with in-memory filtering (actions, event_type_prefix, principal, outcome, emission_path, correlation_id). `IngestEvents` implements 10-step pipeline: master switch → source allow-list → batch size → rate limit → org resolution → validation → write → metrics → log. Unit tests.
- `services/slug_resolver.rs`: Organization, vault, user, and app slug ↔ internal ID resolution at gRPC boundary. `SlugResolver` wraps `AppliedStateAccessor`. Organization methods: `extract_slug`, `resolve`, `resolve_slug`, `extract_and_resolve`, `extract_and_resolve_optional`. Vault methods: `extract_vault_slug`, `resolve_vault`, `resolve_vault_slug`, `extract_and_resolve_vault`, `extract_and_resolve_vault_optional`. App methods: `extract_app_slug`, `resolve_app`, `resolve_app_slug`, `extract_and_resolve_app`, `extract_and_resolve_app_optional`. Events method: `extract_and_resolve_for_events()` (slug=0 → system org bypass). Unit tests (14 org + 19 vault + 16 app + 4 events).
- `services/organization.rs`: OrganizationService gRPC implementation — `get_organization_team()` (reads team, checks membership), `add_team_member()` (proposes `AddTeamMember` to REGIONAL Raft), `remove_team_member()` (proposes `RemoveTeamMember` to REGIONAL Raft), `overlay_org_profile()` (merges GLOBAL `Organization` skeleton with REGIONAL `OrganizationProfile` PII), `read_organization()` (reads `Organization` from GLOBAL). Uses `propose_regional_org_encrypted()` for org-scoped PII encryption. `DeleteOrganization` handler includes best-effort revocation of pending invitations (scan REGIONAL `invite:{org_id}:`, propose GLOBAL `ResolveOrganizationInvite` + REGIONAL `UpdateOrganizationInviteStatus` for each Pending).
- `services/vault.rs`: VaultService gRPC implementation — includes `update_vault()` (proposes `UpdateVault` to update retention policy)
- `services/app.rs`: AppService gRPC implementation — assertion names now stored regionally via `WriteClientAssertionName`/`DeleteClientAssertionName`. `assertion_to_proto()` loads name from REGIONAL state via `load_assertion_name()`. `CreateAppClientAssertion` writes name separately to REGIONAL Raft.
- `services/token.rs`: TokenService — includes `revoke_all_app_sessions()` (proposes `RevokeAllAppSessions`)
- `services/service_infra.rs`: `ServiceContext` — `propose_regional_org_encrypted()` reads OrgShredKey from state, encrypts SystemRequest with per-org shred key, wraps in `EncryptedOrgSystem`. Existing `propose_regional_encrypted()` renamed to use `EncryptedUserSystem` wrapper.
- `services/region_resolver.rs`: Organization→region routing
- `services/error_details.rs`: ErrorDetails proto builder

### Features (51 files)

#### Consensus Interaction

These files sit between this crate's openraft integration and the multi-shard consensus engine. They handle the "how do we actually commit and apply entries efficiently" question.

- `consensus_handle.rs`: `ConsensusHandle` — the typed handle higher layers use to propose, read-index, and query membership. Wraps the `ConsensusEngine` from the consensus crate behind a Raft-crate-owned API so the rest of the app doesn't depend on consensus-crate types directly.
- `consensus_transport.rs`: Network transport shim bridging the consensus crate's `NetworkTransport` trait to the app's gRPC-based `RaftService`. Routes `Message`s to the correct peer and surfaces delivery errors back to the reactor.
- `apply_pool.rs`: `ApplyPool` — pool of apply workers that parallelize state-machine application across shards. Each shard has its own applier to keep state deterministic per shard while letting different shards apply concurrently.
- `apply_worker.rs`: `ApplyWorker` — per-shard apply loop. Pulls committed batches from the consensus engine, calls the state machine's `apply()`, and updates `AppliedState` / `AppliedStateCore` / client idempotency tracking.
- `read_index.rs`: Read-index protocol implementation on top of `ConsensusHandle`. Used by follower reads that need linearizability without a full round-trip to the leader.
- `leader_lease.rs`: Leader-lease wrapper used by the raft crate to gate reads without a round-trip when the lease is valid. Composes the consensus crate's `LeaderLease` type with raft-crate policy (safety check on clock skew, lease refresh cadence).
- `peer_address_map.rs`: Address mapping from `NodeId` to current peer address. Updated by discovery/membership changes so the transport always sends to the current endpoint even after a node rebinds.
- `peer_tracker.rs`: Per-peer connection state, heartbeat tracking, and "last-seen" metadata for health/telemetry.
- `file_lock.rs`: Advisory lock on the data directory to prevent two ledger processes from opening the same WAL + state.

#### Core Features

- `batching.rs`: BatchWriter with request coalescing
- `event_writer.rs`: Event write infrastructure. `EventWriter<B>` (scope-filtered batch writes to events.db), `ApplyPhaseEmitter` (deterministic UUID v5 builder for apply-phase events), `HandlerPhaseEmitter` (UUID v4 builder for node-local events), `EventHandle<B>` (Arc-shared, cheaply cloneable, best-effort `record_handler_event()`), `IngestionRateLimiter` (per-source token bucket with `AtomicU64` for runtime-updatable rate). Unit tests including 10k stress test.
- `idempotency.rs`: TTL-based deduplication cache
- `pagination.rs`: HMAC-signed page tokens. Includes `EventPageToken` (version, organization, last_key, query_hash) with encode/decode/validate methods on `PageTokenCodec`.
- `rate_limit.rs`: 3-tier token bucket rate limiter
- `hot_key_detector.rs`: Count-Min Sketch with rotating windows
- `metrics.rs`: Prometheus metrics with SLI histograms. Leader transfer metrics: `ledger_leader_transfers_total` (status label), `ledger_leader_transfer_latency_seconds` (histogram), `ledger_trigger_elections_total` (result label). Events metrics: `ledger_event_writes_total` (emission/scope/action labels), `ledger_events_gc_*` (entries*deleted, cycle_duration, cycles), `ledger_events_ingest*\*`(total, batch_size, rate_limited, duration). Onboarding metrics:`ledger_onboarding_initiation_total{status}`, `ledger_onboarding_verification_total{status}`, `ledger_onboarding_registration_total{status}`, `ledger_onboarding_verification_codes_gc_total`, `ledger_onboarding_accounts_gc_total`, `ledger_totp_challenges_gc_total`. Post-erasure compaction metrics: `ledger_post_erasure_compaction_triggered_total{region}`. Organization purge metrics: `ledger_org_purge_regional_failures_total{region}`, `ledger_org_purge_global_failures_total`, `ledger_org_purge_retry_exhausted_total`.
- `otel.rs`: OpenTelemetry tracing setup and OTLP exporter configuration. `SpanAttributes` uses `vault` key.

#### Enterprise Features

- `graceful_shutdown.rs`: Shutdown coordinator with `NodePhase` (4 states: Starting → Ready → Draining → ShuttingDown), `HealthState` (AtomicU8 with `mark_draining()`, `is_accepting_proposals()`, `as_str()`), pre-phases for drain + leader transfer before the 6-phase shutdown flow, `GracefulShutdown` with `raft`/`transfer_lock` fields and `with_raft()` builder. Unit tests.
- `leader_transfer.rs`: Graceful leader transfer coordination — `LeaderTransferConfig` (bon builder), `LeaderTransferError` (8 snafu variants), `TransferGuard` (RAII concurrency lock), `transfer_leadership()` (5-step algorithm: verify leader → select target → wait for replication → send TriggerElection → poll for leader change). Unit tests.
- `runtime_config.rs`: Hot-reload via UpdateConfig RPC + ArcSwap
- `backup.rs`: Snapshot-based backups with S3/GCS/Azure
- `auto_recovery.rs`: Automatic divergence recovery
- `api_version.rs`: API version negotiation (in `inferadb-ledger-services`)
- `deadline.rs`: Request deadline propagation
- `dependency_health.rs`: Disk/Raft/peer health checks
- `entry_crypto.rs`: AES-256-GCM encryption for Raft log entries. `EncryptedUserSystemRequest` (per-user shred key encryption — used for credential CRUD and onboarding PII), `EncryptedOrgSystemRequest` (per-org shred key encryption for crypto-shredding). Generic `seal()`/`unseal()` helpers. `OnboardingPii` sealed during saga step 1. Credential data (passkey public keys, TOTP secrets, recovery code hashes) encrypted via `EncryptedUserSystemRequest` through `propose_regional_encrypted()`. Organization ID used as AAD to prevent cross-org entry substitution.

#### Background Jobs

- `block_compaction.rs`: Block archive compaction
- `btree_compaction.rs`: B+ tree compaction
- `events_gc.rs`: `EventsGarbageCollector<B>` — TTL-based event expiry background task. Runs on all nodes (not leader-only) since `events.db` is node-local. Default interval 300s, max batch 5,000. Uses thin `EventMeta` deserialization for GC efficiency. Watchdog heartbeat integration. Unit tests.
- `resource_metrics.rs`: Resource saturation metrics
- `ttl_gc.rs`: Time-to-live garbage collection
- `integrity_scrubber.rs`: Page CRC verification — runs on all nodes (not leader-only), wraps sync I/O in `spawn_blocking`, watchdog integration, configurable via `IntegrityConfig`
- `learner_refresh.rs`: Read replica refresh
- `orphan_cleanup.rs`: Orphaned membership cleanup — leader-only, correct organization-scoped vault listing, paginated user iteration, `tokio::task::yield_now()` between orgs for I/O throttling, watchdog/metrics integration, configurable via `CleanupConfig`
- `peer_maintenance.rs`: Peer health checks (in `inferadb-ledger-services`)
- `organization_purge.rs`: `OrganizationPurgeJob` — background purge of soft-deleted organizations (leader-only, watchdog integration). Retry logic with priority set: failed orgs retried next cycle with exponential backoff (100ms/500ms/2500ms, 3 max retries). `propose_regional_with_retry()` and `propose_global_with_retry()` for resilient cross-region purge orchestration.
- `post_erasure_compaction.rs`: `PostErasureCompactionJob` — triggers proactive Raft snapshots after crypto-shredding to evict plaintext from Raft logs. Runs on GLOBAL + all regional groups. Configurable via `PostErasureCompactionConfig` (retention 3600s, check interval 300s). Tracks per-group last-snapshot times.
- `invite_maintenance.rs`: `InviteMaintenanceJob` — background invitation lifecycle management (leader-only, watchdog integration). Phase 1: expire Pending invitations (read REGIONAL for expires_at, up to 200/cycle). Phase 2: reap terminal invitations past 90 days (read REGIONAL for created_at, up to 100/cycle). Scans all `_idx:invite:email_hash:` entries (up to 5000) and bounds processing per cycle. 4 unit tests.
- `user_retention.rs`: `UserRetentionReaper` — background cleanup of expired/deleted user data
- `state_root_verifier.rs`: Background state root integrity verification

#### Advanced Features

- `raft_manager.rs`: `RaftManager` orchestration — manages multiple `RegionGroup` instances, each with its own Raft, state, and block archive. `RaftManagerConfig`, `RaftManagerError`, `RaftManagerStats` types. Supports `start_system_region()`, `start_data_region()`, `register_external_region()`, `get_region_group()`. Every `LedgerServer` is inherently multi-region capable (single-region is simply one GLOBAL region)
- `raft_network.rs`: gRPC-based Raft transport
- `proto_compat.rs`: Orphan rule workarounds (`organization_status_to_proto`, `proto_to_team_member_role`) (in `inferadb-ledger-services`)
- `trace_context.rs`: W3C Trace Context
- `logging/` (directory module: `mod.rs`, `request_context.rs`, `sampling.rs`, `tests.rs`): Canonical log lines (vault field, `set_target(organization, vault)`), request context propagation, sampling configuration. Actor must be opaque identifier (slug, numeric ID), never email or display name.
- `proof.rs`: Merkle proof generation (accepts `vault_slug: Option<VaultSlug>` parameter)
- `region_router.rs`: Dynamic region routing
- `saga_orchestrator.rs`: Distributed transaction orchestration — CAS-based sequence ID allocation (prevents duplicates on leader failover), watchdog/metrics integration, configurable via `SagaConfig`, with optional `event_handle` for UserDeleted handler-phase events. JWT additions: `key_manager: Option<Arc<dyn RegionKeyManager>>` field, `execute_create_signing_key_step()`, `write_signing_key_saga()`, `check_global_signing_key_bootstrap()`, org creation triggers (`CreateOrganizationSaga` completion). **Onboarding additions**: `SagaOrchestratorHandle` (cloneable, wraps `mpsc::Sender<SagaSubmission>`) returned by `start()` for service handler submission. `SagaSubmission` (record + optional `OnboardingPii` + optional `oneshot::Sender`). `OnboardingPii` (email, name, organization_name — in-memory only, never persisted; custom `Debug` redacts all fields). `SagaOutput` (user_id, user_slug, organization_id, organization_slug, refresh_token_id, kid, refresh_family_id, refresh_expires_at; custom `Debug` redacts token fields). `pii_cache: Mutex<HashMap>` + `notify_cache: Mutex<HashMap>` for in-memory PII and notification tracking. `drain_submissions()` called each tick before `run_cycle()`. PII-loss compensation: if `pii` is `None` when constructing step 1, saga immediately compensates. `execute_create_onboarding_user_step()` implements 3-step state machine (Pending→IdsAllocated→RegionalDataWritten→Completed), fires `CreateSigningKeySaga` after step 2. **PII sealing**: org creation step 1 generates OrgShredKey and seals org name via `entry_crypto::seal()`. Onboarding step 1 seals PII with user shred key before proposing `WriteOnboardingUserProfile`.
- `token_maintenance.rs`: Background job for token lifecycle maintenance. Three-phase cycle: (1) proposes `DeleteExpiredRefreshTokens` through Raft (handles expired token cleanup + poisoned family GC), (2) scans for rotated signing keys past grace period via `list_rotated_keys_past_grace()`, proposes `TransitionSigningKeyRevoked` for each, (3) proposes `CleanupExpiredOnboarding` to each regional Raft group via `RaftManager` (scans `_tmp:onboard_verify:*`, `_tmp:onboard_account:*`, and `_tmp:totp_challenge:*`). `manager: Option<Arc<RaftManager>>` field for regional proposal access. `MaintenanceResult` accumulates `onboarding_codes_deleted`, `onboarding_accounts_deleted`, and `totp_challenges_deleted`. Configurable interval (default 300s).
- `dek_rewrap.rs`: Background job for re-wrapping Data Encryption Key sidecar metadata after Region Master Key rotation. `RewrapProgress` (AtomicU64 fields for lock-free admin queries), `DekRewrapJob<B>` (bon builder, periodic cycles, leader-only). Idempotent — pages already at target RMK version are skipped. Unit tests.
- `region_storage.rs`: Per-region database file layout and lifecycle management. `RegionStorage` (holds raw database handles for state, blocks, events per region), `RegionStorageManager` (HashMap of open regions, directory layout enforcement: `global/` for GLOBAL Raft group, `regions/{name}/` for data regions). TOCTOU-safe region opening, legacy layout detection, 16KB page size. Unit tests.

---

## Crate: `inferadb-ledger-consensus`

- **Purpose**: Purpose-built multi-shard Raft consensus engine. Event-driven Reactor with pluggable WAL backends, pipelined replication, leader leases, and a trait-based state-machine abstraction. Designed around determinism: the Reactor is a single-task event loop that returns `Action`s rather than performing I/O directly, which makes the engine simulation-testable.
- **Dependencies**: `aes-gcm`, `crc32fast`, `parking_lot`, `rand`, `rkyv`, `seahash`, `serde`, `snafu`, `tokio`, `tracing`
- **Quality Rating**: ★★★★★

### Core types (re-exported from `lib.rs`)

- `ConsensusEngine` — top-level multi-shard engine
- `Reactor` — single-task event loop driving all shards
- `Shard` — single Raft instance (state machine + log)
- `Action` — output of the reactor (e.g., `SendMessage`, `ApplyEntry`, `Fsync`)
- `Clock` / `SystemClock` — abstract time source for deterministic testing
- `RngSource` / `SystemRng` — abstract randomness source (seedable in simulation, `thread_rng`-backed in production)
- `ClosedTimestampTracker` — tracks the highest timestamp below which reads are safe
- `CommittedBatch`, `CommittedEntry` — outputs of commit
- `LeaderLease` — leader-lease based read optimization
- `Message` — Raft wire-format message envelope
- `ShardConfig`, `ShardState`
- `StateMachine` trait, `ApplyResult`, `NoopStateMachine`, `SnapshotError`
- `WalBackend` trait, `FsyncPhase` — WAL backend abstraction
- `InMemoryTransport`, `NetworkTransport` — transport abstraction
- Zero-copy helpers: `ZeroCopyError`, `access_archived`, `from_archived_bytes`, `to_archived_bytes`

### Files

#### `lib.rs`

- **Purpose**: Public API surface and re-exports
- **Insights**: Clean, narrow public surface. Consumers use `ConsensusEngine`, `Shard`, `StateMachine`, `WalBackend`, and the message types; everything else is accessible under `pub mod` submodules.

#### `reactor.rs` (1637 lines)

- **Purpose**: Single-task event loop that multiplexes across all shards
- **Key Responsibilities**:
- Receives `ReactorEvent`s (Propose, ProposeBatch, HandleMessage, Timer, etc.)
- Drives each `Shard` forward without performing I/O directly
- Batches WAL writes and network sends across shards for amortized fsync/syscall cost
- Returns `Action`s for the driver to execute
- **Insights**: The reactor is the determinism boundary. All non-determinism (time, I/O, randomness) is injected via `Clock`, `WalBackend`, and `RngSource`. This makes the reactor simulation-testable via the `simulation/` harness.

#### `shard.rs` (2795 lines)

- **Purpose**: Single Raft instance — leader election, log replication, commit index tracking, snapshot lifecycle
- **Insights**: Largest file in the crate. Event-driven (returns `Action`s, never performs I/O). Implements leader-lease optimization, closed-timestamp tracking, pipelined replication.

#### `engine.rs` (568 lines)

- **Purpose**: `ConsensusEngine` — top-level multi-shard engine, coordinates `Reactor` + shard lifecycle + router
- **Insights**: The public entry point for higher layers (raft crate wraps this). Holds the shard registry and exposes propose/read-index/membership operations.

#### `action.rs`

- **Purpose**: `Action` enum — the set of side-effects the reactor can request (send message, fsync WAL, apply entry, schedule timer, etc.)
- **Insights**: Central to the determinism model — the reactor _describes_ what needs to happen; the driver _does_ it.

#### `state_machine.rs` (194 lines)

- **Purpose**: `StateMachine` trait, `ApplyResult`, `NoopStateMachine`, snapshot interface
- **Key Types**: `StateMachine` (apply + snapshot contract), `SnapshotError`
- **Insights**: The consensus engine is state-machine-agnostic. InferaDB's `StateLayer` (in `inferadb-ledger-state`) implements this trait.

#### `wal/` module

- **Purpose**: Write-ahead log backends. Pluggable so different deployments can trade durability, throughput, and encryption guarantees.
- **Submodules**:
  - `wal/mod.rs` — Module wiring + shared types
  - `wal/segmented.rs` (1136 lines) — Segmented WAL implementation (the production backend). Per-vault AES-256-GCM, single fsync per batch
  - `wal/encrypted.rs` — Envelope-encrypted WAL wrapper (wraps another backend)
  - `wal/memory.rs` — In-memory WAL (tests, simulation)
  - `wal/io_uring_backend.rs` — io_uring-based backend (Linux, behind `io-uring` feature)
- **Insights**: The segmented backend is what production uses. The `io-uring` feature is off by default; builds on non-Linux platforms fall back to the synchronous segmented writer without feature drift.

#### `wal_backend.rs`

- **Purpose**: `WalBackend` trait, `FsyncPhase` enum — the abstraction every WAL impl satisfies
- **Insights**: Phase enum lets the reactor batch writes across shards and fsync once per batch rather than once per append.

#### `leadership.rs`

- **Purpose**: Leader election state machine, `ShardState` (follower/candidate/leader)

#### `lease.rs`

- **Purpose**: `LeaderLease` — leader-lease based read optimization (local reads without round-trip when lease is valid)

#### `closed_ts.rs`

- **Purpose**: `ClosedTimestampTracker` — tracks the highest timestamp below which reads are safe without coordinating with the leader
- **Insights**: Enables bounded-staleness reads on followers.

#### `recovery.rs`

- **Purpose**: WAL replay and snapshot-install recovery paths

#### `snapshot_crypto.rs`, `snapshot_utils.rs`

- **Purpose**: Snapshot envelope encryption and streaming helpers

#### `crypto.rs`

- **Purpose**: AES-GCM wrapper used by WAL + snapshot encryption

#### `transport.rs`

- **Purpose**: `NetworkTransport` trait plus `InMemoryTransport` for tests

#### `network_outbox.rs`

- **Purpose**: Per-peer outbox for batched message sends
- **Insights**: Pipelines AppendEntries / Heartbeat messages to keep the network hot between rounds.

#### `router.rs`

- **Purpose**: Shard routing — maps `(ShardId, key)` to the appropriate shard instance inside the engine

#### `split.rs`

- **Purpose**: Shard split logic (splitting a hot shard into two)

#### `bootstrap.rs`

- **Purpose**: First-boot shard initialization (node 1 single-voter seed, plus subsequent-node join helpers)

#### `config.rs`

- **Purpose**: `ShardConfig` — per-shard tunables (heartbeat intervals, election timeouts, batch sizes, lease durations)

#### `clock.rs`, `rng.rs`

- **Purpose**: Abstract `Clock` / `RngSource` traits with production (`SystemClock`, `SystemRng`) and deterministic test implementations
- **Insights**: Injected into the reactor — the simulation harness plugs in virtual time / seeded RNG for replayable tests.

#### `timer.rs`

- **Purpose**: Logical timer wheel for election/heartbeat/lease timeouts. Driven by the reactor, not by real `tokio::time`.

#### `message.rs`, `types.rs`

- **Purpose**: Raft wire types — `Message`, `AppendEntries`, `RequestVote`, shard IDs, node IDs

#### `committed.rs`

- **Purpose**: `CommittedEntry` / `CommittedBatch` — outputs handed to the state machine

#### `idempotency.rs`

- **Purpose**: Per-shard idempotency bookkeeping for client-retry safety

#### `circuit_breaker.rs`

- **Purpose**: Per-peer circuit breaker to avoid hammering unreachable nodes

#### `buggify.rs`

- **Purpose**: FoundationDB-style fault injection hook — deterministic "buggify" points inject rare conditions into simulation runs
- **Insights**: Used by `simulation/harness.rs` to exercise edge cases (partial fsyncs, reordered messages, GC timing) without manual test construction.

#### `zero_copy.rs`

- **Purpose**: rkyv-backed zero-copy decode helpers — `access_archived`, `from_archived_bytes`, `to_archived_bytes`
- **Insights**: WAL entries and snapshots are rkyv-archived so replay avoids deserialization allocation.

#### `error.rs`

- **Purpose**: `ConsensusError` — snafu-based error taxonomy for the crate

#### `simulation/` module

- **Submodules**: `mod.rs`, `harness.rs` (driver), `multi_raft.rs` (multi-shard scenarios), `network.rs` (simulated network with drops/delays/reorder)
- **Purpose**: Deterministic simulation testbed for Raft invariants. Pluggable into the reactor via the abstract `Clock`, `RngSource`, `NetworkTransport`, and `WalBackend`.
- **Insights**: This is how we catch Byzantine-esque edge cases (leader lease violations, commit-gap races) without flaky integration tests. Tests are replayable given the seed.

### Insights

- **Determinism is the load-bearing architectural choice.** The reactor is a pure function of `(state, event) → (new_state, Action[])`. All non-determinism (time, randomness, I/O) flows through injected traits. This is what makes `simulation/` worth the complexity: failing runs are reproducible.
- **Two-layer consensus architecture.** `inferadb-ledger-consensus` is the multi-shard engine; `inferadb-ledger-raft` wraps it with openraft compatibility, background jobs, saga orchestration, and the glue that talks to the rest of the app. The raft crate owes this layering: consensus cares only about Raft correctness and durability; raft cares about operationalization.
- **WAL backend pluggability is real.** Segmented is the production backend; in-memory supports tests; encrypted is a composable wrapper; io_uring is a Linux-only performance backend. Each satisfies `WalBackend + FsyncPhase`; the reactor doesn't know which it's calling.
- **Snapshot + WAL share the same crypto path.** `snapshot_crypto` and `wal/encrypted` both sit on `crypto.rs` (AES-GCM). Key management lives in the store crate; this crate just wraps.

---

## Crate: `inferadb-ledger-sdk`

- **Purpose**: Enterprise Rust client library with retry, circuit breaker, cancellation, metrics, and tracing.
- **Dependencies**: `types`, `proto`, `tonic`, `tokio`
- **Quality Rating**: ★★★★★

### Files

#### `lib.rs`

- **Purpose**: Re-exports public API (LedgerClient, ClientConfig, builders, error types, events types, token types)
- **Key Types/Functions**:
- Modules: `client`, `config`, `connection`, `error`, `retry`, `circuit_breaker`, `discovery`, `metrics`, `streaming`, `token`, `tracing`, `builders`, `server`, `mock`, `ops` (impl files: `app`, `credential`, `data`, `events`, `health`, `invitation`, `list`, `onboarding`, `organization`, `schema`, `token`, `user`, `vault`, `verified_read`), `types` (submodules: `admin`, `app`, `credential`, `events`, `invitation`, `query`, `read`, `schema`, `streaming`, `verified_read`)
- Events re-exports: `EventEmissionPath`, `EventFilter`, `EventOutcome`, `EventPage`, `EventScope`, `IngestRejection`, `IngestResult`, `SdkEventEntry`, `SdkIngestEventEntry`
- Token re-exports: `TokenPair`, `ValidatedToken`, `PublicKeyInfo`
- Onboarding re-exports: `EmailVerificationCode`, `EmailVerificationResult`, `RegistrationResult`
- Invitation re-exports: `InviteSlug`, `InvitationCreated`, `InvitationInfo`, `InvitationPage`, `InvitationStatus`, `ReceivedInvitationInfo`, `ReceivedInvitationPage`
- Credential re-exports: `CredentialType`, `TotpAlgorithm`, `PasskeyCredentialInfo`, `TotpCredentialInfo`, `RecoveryCodeCredentialInfo`, `CredentialData`, `UserCredentialInfo`, `RecoveryCodeResult`, `UserCredentialId`
- **Insights**: Comprehensive SDK. All features needed for production use, including events query/ingestion, JWT token management, self-service onboarding, and multi-credential authentication (passkey, TOTP, recovery codes).

#### `client.rs`

- **Purpose**: LedgerClient with 40+ public methods, retry, cancellation, metrics, and comprehensive tests
- **Key Types/Functions**:
- `LedgerClient::new(config) -> Result<Self>`: Create client (55+ public methods)
- Data ops: `read()`, `write()`, `batch_read()`, `batch_write()` — all accept `organization: OrganizationSlug, vault: Option<VaultSlug>`
- Convenience ops: `set_entity(org, vault, key, value, expires_at, condition)`, `delete_entity()` — optional TTL and condition on set operations (note: no `get_entity` convenience method; use `read()` for entity retrieval)
- `SetCondition::from_expected(expected: Option<impl Into<Vec<u8>>>) -> Self`: Convenience constructor — `None` maps to `MustNotExist`, `Some(value)` maps to `MustEqual(value)`
- Relationship ops: `check_permission()`, `create_relationship()`, `delete_relationship()`
- Admin ops: `create_organization()`, `create_vault()` (returns `VaultInfo` with `vault: VaultSlug`), `list_vaults()`
- Events ops: `list_events(org_slug, filter, limit)` → `EventPage`, `list_events_next(org_slug, page_token)`, `get_event(org_slug, event_id)`, `count_events(org_slug, filter)`, `ingest_events(org_slug, source_service, events)` → `IngestResult`
- Events types: `EventScope`, `EventOutcome`, `EventEmissionPath` enums, `SdkEventEntry`, `EventPage` (with `has_next_page()`), `EventFilter` (builder with chainable filters: `start_time`, `end_time`, `actions`, `event_type_prefix`, `principal`, `outcome_*`, `*_phase_only`, `correlation_id`), `SdkIngestEventEntry` (builder with `detail`, `details`, `trace_id`, `correlation_id`, `vault_slug`, `timestamp`), `IngestResult`, `IngestRejection`
- All methods use `with_retry_cancellable()` for retry + cancellation
- `with_metrics()` wrapper for user-perceived latency
- Token ops: `create_user_session(user)`, `validate_token(token, audience)`, `revoke_all_user_sessions(user)`, `revoke_all_app_sessions(app)`, `refresh_token(refresh_token)`, `revoke_token(refresh_token)`, `create_vault_token(org, app, vault, scopes)`, `create_signing_key(scope, org)`, `rotate_signing_key(kid, grace_period)`, `revoke_signing_key(kid)`, `get_public_keys(org)`
- Team ops: `get_organization_team(team, caller)`, `add_team_member(team, user, role, initiator)`, `remove_team_member(team, user, initiator)`
- Vault ops: `update_vault(organization, vault, retention_policy)`
- Onboarding ops: `initiate_email_verification(email, region) -> EmailVerificationCode`, `verify_email_code(email, code, region) -> EmailVerificationResult`, `complete_registration(onboarding_token, email, region, name, organization_name) -> RegistrationResult`
- Onboarding types: `EmailVerificationCode` (code field), `EmailVerificationResult` enum (`ExistingUser { user, session }` | `TotpRequired { challenge_nonce }` | `NewUser { onboarding_token }`), `RegistrationResult` (user, session, organization)
- Invitation ops (in `ops/invitation.rs`): 8 methods — Admin: `create_organization_invite(org, inviter, email, role, ttl_hours, team) -> InvitationCreated`, `list_organization_invites(org, caller, status_filter, page_token, page_size) -> InvitationPage`, `get_organization_invite(slug, caller) -> InvitationInfo`, `revoke_organization_invite(slug, caller) -> InvitationInfo`. User: `list_received_invitations(user, status_filter, page_token, page_size) -> ReceivedInvitationPage`, `get_invitation_details(slug, user) -> ReceivedInvitationInfo`, `accept_invitation(slug, acceptor) -> InvitationInfo`, `decline_invitation(slug, user) -> ReceivedInvitationInfo`
- Invitation types (in `types/invitation.rs`): `InvitationStatus` enum (Pending/Accepted/Declined/Expired/Revoked with from_proto/to_proto), `InvitationCreated` (custom `Debug` redacting token), `InvitationInfo` (admin view), `ReceivedInvitationInfo` (user view, with organization_name), `InvitationPage`, `ReceivedInvitationPage`. 8 unit tests.
- Credential ops (in `ops/credential.rs`): `create_user_credential(user, name, data) -> UserCredentialInfo`, `list_user_credentials(user, credential_type) -> Vec<UserCredentialInfo>`, `update_user_credential(user, credential_id, name, enabled, passkey_data) -> UserCredentialInfo`, `delete_user_credential(user, credential_id)`, `create_totp_challenge(user, primary_method) -> Vec<u8>`, `verify_totp(user, totp_code, challenge_nonce) -> TokenPair`, `consume_recovery_code(user, code, challenge_nonce) -> RecoveryCodeResult`
- Credential types (in `types/credential.rs`): `CredentialType` enum (Passkey/Totp/RecoveryCode with `from_data()` helper), `TotpAlgorithm` enum, `PasskeyCredentialInfo`, `TotpCredentialInfo` (manual `Debug` redacts secret), `RecoveryCodeCredentialInfo`, `CredentialData` enum, `UserCredentialInfo`, `RecoveryCodeResult` (tokens + remaining_codes)
- Events unit tests
- **Insights**: Clean API. Cancellation support via CancellationToken. Circuit breaker integrated. Metrics track end-to-end latency. Events SDK uses `from_proto`/`into_proto` associated functions (not From trait impls). `SetCondition::from_expected()` provides ergonomic CAS condition construction for compare-and-set patterns. All methods (onboarding, credential, TOTP) follow the same `with_retry_cancellable` + `with_metrics` pattern. `CredentialType` parameter on `create_user_credential` is derived from `CredentialData` discriminant via `from_data()` to prevent mismatched type+data.

#### `token.rs`

- **Purpose**: SDK-side token types with proto conversion
- **Key Types/Functions**:
- `TokenPair`: Access token + refresh token + expiry times (uses `SystemTime`)
- `ValidatedToken` enum: `UserSession` (user_slug, role) | `VaultAccess` (org_slug, app_slug, vault_slug, scopes)
- `PublicKeyInfo`: kid, public_key, status, valid_from, valid_until, created_at
- All types use `from_proto()` associated functions and `proto_timestamp_to_system_time()` for timestamp conversion
- **Insights**: SDK types use `SystemTime` (not `DateTime<Utc>`) for timestamps, consistent with other SDK types. `proto_timestamp_to_system_time()` promoted to `pub(crate)` for cross-module use.

#### `config.rs`

- **Purpose**: ClientConfig with fallible builder, TLS support
- **Key Types/Functions**:
- `ClientConfig::builder() -> ClientConfigBuilder`: bon builder
- Fields: endpoints, retry_config, circuit_breaker_config, tls_config, timeout, metrics
- `TlsConfig`: ca_cert, use_native_roots, client_cert/key (mTLS)
- Validation: at least one endpoint, valid TLS config
- **Insights**: Fallible builder validates constraints. TLS support for secure communication. Precedent for other configs.

#### `connection.rs`

- **Purpose**: ConnectionPool with circuit breaker integration
- **Key Types/Functions**:
- `ConnectionPool::new(config) -> Result<Self>`: Create pool
- `get_channel() -> Result<tonic::Channel>`: Get connection with circuit breaker check
- Circuit breaker: per-endpoint state machine (Closed→Open→HalfOpen)
- ServerSelector syncs with circuit breaker (open→mark_unhealthy, close→mark_healthy)
- **Insights**: Circuit breaker prevents cascade failures. Pool handles connection lifecycle. Sync with ServerSelector for consistent routing.

#### `error.rs`

- **Purpose**: SdkError with rich context, ServerErrorDetails decoding, and comprehensive tests
- **Key Types/Functions**:
- `SdkError`: 19 variants (Connection, Transport, Rpc, RateLimited, RetryExhausted, Config, Idempotency, AlreadyCommitted, StreamDisconnected, Timeout, Shutdown, Cancelled, InvalidUrl, Unavailable, ProofVerification, Validation, CircuitOpen, OrganizationMigrating, UserMigrating)
- `SdkError::is_cas_conflict() -> bool`: Convenience method detecting CAS conflict (gRPC FAILED_PRECONDITION with "condition" in message) — simplifies compare-and-set retry loops
- `ServerErrorDetails`: Decoded from proto ErrorDetails (error_code, retryable, retry_after, context, action)
- `is_retryable()`, `error_type()`: Classification helpers
- `attempt_history: Vec<(u32, String)>`: Retry tracking on `RetryExhausted` variant (PRD Task 2)
- **Insights**: Rich error context for debugging. ServerErrorDetails decode via prost. RateLimited variant with retry_after guidance. `is_cas_conflict()` pairs with `SetCondition::from_expected()` for ergonomic CAS patterns.

#### `retry.rs`

- **Purpose**: with_retry_cancellable (manual retry loop with tokio::select!)
- **Key Types/Functions**:
- `with_retry_cancellable<F>(pool, method, operation, token) -> Result<T>`: Retry with cancellation
- Manual retry loop: `tokio::select! { biased; ... }` checks cancellation on each iteration
- Exponential backoff with jitter
- Circuit breaker check before each attempt
- Metrics: retries, circuit state
- **Insights**: Replaced backon for cancellation support. Manual loop is more flexible. Circuit breaker integration prevents hammering open circuits.

#### `circuit_breaker.rs`

- **Purpose**: Per-endpoint circuit breaker state machine (PRD Task 5)
- **Key Types/Functions**:
- `CircuitBreaker::new(config) -> Self`
- `check() -> Result<()>`: Check state, transition Open→HalfOpen if timeout elapsed
- `record_success()`, `record_failure()`: State transitions
- States: Closed (healthy), Open (failing), HalfOpen (testing)
- `CircuitBreakerConfig`: failure_threshold, success_threshold, timeout
- **Insights**: State machine prevents cascade failures. Open state rejects fast. HalfOpen tests recovery. Syncs with ServerSelector.

#### `discovery.rs`

- **Purpose**: Background endpoint refresh (dynamic service discovery)
- **Key Types/Functions**:
- `DiscoveryService`: Background job calling GetClusterInfo RPC
- `refresh_endpoints()`: Fetch cluster info, update ConnectionPool
- Configurable interval (default 30s)
- **Insights**: Enables dynamic cluster membership. Clients discover new nodes without restart.

#### `metrics.rs`

- **Purpose**: SdkMetrics trait with noop and metrics-crate implementations (PRD Task 6)
- **Key Types/Functions**:
- `SdkMetrics` trait: record_request, record_retry, record_circuit_state, record_connection
- `NoopSdkMetrics`: Zero-overhead default (no-op methods)
- `MetricsSdkMetrics`: metrics crate facade (counters, histograms, gauges)
- Prefix: `ledger_sdk_` for all metrics
- **Insights**: Dynamic dispatch (Arc<dyn SdkMetrics>) avoids type param infection. Noop default ensures zero overhead when disabled.

#### `proto_util.rs`

- **Purpose**: Proto↔SDK conversion helpers (timestamps, enums, user info)
- **Key Types/Functions**:
- `proto_timestamp_to_system_time()`, `system_time_to_proto_timestamp()` — protobuf Timestamp ↔ `std::time::SystemTime`
- `region_from_proto_i32()`, `region_to_proto_i32()` — `Region` enum ↔ proto i32
- `user_status_from_proto_i32()`, `user_role_from_proto_i32()` — proto enum (i32) → domain enum
- `user_info_from_proto()`, `user_email_info_from_proto()` — proto `User` / `UserEmail` → SDK domain types
- `non_empty()`, `missing_response_field()` — small guards used across handlers
- **Insights**: Internal utility module reducing boilerplate across the 40+ SDK client methods. All conversions return SDK error types on invalid input rather than panicking.

#### `region_resolver.rs`

- **Purpose**: Region leader caching with TTL expiry
- **Key Types/Functions**:
- `RegionLeaderCache` — thread-safe cache keyed by `Region`, stores resolved leader with TTL
- `CachedLeader` — cached entry metadata
- `DEFAULT_TTL_SECS` (300s)
- **Insights**: Wraps `ResolveRegionLeader` RPC results so region-aware operations don't hit discovery on every call. TTL eviction keeps the cache from pinning stale leaders indefinitely after leadership transfer.

#### `streaming.rs`

- **Purpose**: WatchBlocksStream with auto-reconnection
- **Key Types/Functions**:
- `ReconnectingStream<T, P, F, Fut>`: Generic reconnecting stream wrapper
- Handles auto-reconnection on disconnect with configurable retry policy
- Backoff on errors
- **Insights**: Generic streaming abstraction for real-time block updates. Auto-reconnection for resilience. Used for event sourcing.

#### `tracing.rs`

- **Purpose**: W3C Trace Context propagation, API version header injection
- **Key Types/Functions**:
- `TraceContextInterceptor`: Injects traceparent, tracestate, x-ledger-api-version, x-sdk-version headers
- `with_timeout(duration)`: Injects grpc-timeout header (PRD Task 7)
- Always injects x-sdk-version (not gated by trace config)
- **Insights**: Tonic interceptor for header injection. W3C Trace Context standard. SDK version enables server-side telemetry.

#### `builders/` (3 files)

- **Purpose**: Type-safe request builders for read, write, relationship operations
- **Key Types/Functions**:
- `BatchReadBuilder`: Batch read with typestate pattern (`NoKeys` → keys added)
- `WriteBuilder`: Write operations with typestate pattern (`NoOps` → ops added)
- `RelationshipQueryBuilder`: Relationship queries
- Fluent APIs with validation
- **Insights**: Type-safe builders prevent invalid requests. Typestate pattern enforces required fields at compile time.

#### `server/` (3 files)

- **Purpose**: Server source, selector, resolver (endpoint management)
- **Key Types/Functions**:
- `ServerSource`: Where endpoints come from (static config, discovery, DNS)
- `ServerSelector`: Selects healthy endpoint for request (round-robin with health tracking)
- `ServerResolver`: Resolves DNS names to IPs
- **Insights**: Abstraction enables multiple endpoint sources. ServerSelector uses health tracking + circuit breaker.

#### Idempotency (no separate module)

- **Purpose**: Idempotency is handled entirely server-side via `ClientSequenceEntry` in the Raft state machine. Clients generate UUIDs for transaction IDs. There is no SDK-side idempotency module.
- **Insights**: The deduplication logic lives in `raft/src/log_storage/accessor.rs` (`client_idempotency_check()`). SDK documentation in `lib.rs` explains the server-side approach.

#### `mock.rs`

- **Purpose**: MockLedgerServer for testing (spawns a real gRPC server with in-memory state)
- **Key Types/Functions**:
- `MockLedgerServer`: Spawns a `tonic::transport::Server` with mock service implementations backed by HashMap storage, keyed by `(OrganizationSlug, VaultSlug, ...)`
- All LedgerClient methods work against it (real gRPC transport, in-memory state)
- Mock onboarding: `initiate_email_verification` returns "ABC123", `verify_email_code` returns NewUser path, `complete_registration` returns user+session+org
- Mock team: `get_organization_team`, `add_team_member`, `remove_team_member`
- Mock vault: `update_vault`
- Mock credential: Fully functional stubs for credential RPCs
- Mock admin: 7 stubs returning `Status::unimplemented()` (`create_backup`, `restore_backup`, `transfer_leadership`, `rotate_blinding_key`, `get_blinding_key_rehash_status`, `rotate_region_key`, `get_rewrap_status`)

- **Insights**: Enables integration testing with real gRPC transport without a Raft cluster. In-memory state for fast tests. Tuple keys use `OrganizationSlug`/`VaultSlug` newtypes, not raw integers or internal IDs. Large due to comprehensive mock service implementations and inline tests.

---

## Crate: `inferadb-ledger-server`

- **Purpose**: Binary with CLI, config loading, bootstrap, discovery, signal handling, and 30+ integration/benchmark tests.
- **Dependencies**: All workspace crates (`types`, `store`, `state`, `proto`, `raft`, `sdk`)
- **Quality Rating**: ★★★★★

### Files

#### `main.rs`

- **Purpose**: CLI with clap, config loading, server startup
- **Key Types/Functions**:
- `Cli`: clap command-line args (config path, node-id, bootstrap, etc.)
- `main()`: Parse args, load config, call bootstrap. Wires `node.raft` into `GracefulShutdown` via `with_raft()` for leader transfer during shutdown.
- Subcommands: `config schema` (JSON Schema export for RuntimeConfig)
- **Insights**: Clean CLI. Supports CLI args + env var overrides (`INFERADB__LEDGER__<FIELD>`). `config schema` subcommand for JSON Schema export.

#### `bootstrap.rs`

- **Purpose**: Node bootstrap, lifecycle management, background job spawning, events system wiring
- **Key Types/Functions**:
- `bootstrap_node(config) -> Result<BootstrappedNode>`: Initialize node
- `BootstrappedNode`: Handle to running node (server, Raft, 15 tracked background job handles, `saga_orchestrator_handle: SagaOrchestratorHandle`)
- Background job handles: `gc_handle`, `compactor_handle`, `recovery_handle`, `learner_refresh_handle`, `resource_metrics_handle`, `backup_handle` (optional), `events_gc_handle` (optional), `saga_handle`, `orphan_cleanup_handle`, `integrity_scrub_handle`, `org_purge_handle`, `token_maintenance_handle`, `invite_maintenance_handle`, `post_erasure_compaction_handle`, `snapshot_demotion_handle` (optional)
- Email blinding key wiring: Parses hex-encoded `email_blinding_key` from config via `parse_hex_key()` helper into `Arc<EmailBlindingKey>` with version 1, passes to `ServiceContext` and `LedgerServer`
- Events wiring: Opens `EventsDatabase` (`{data_dir}/events.db`), creates `EventWriter` and injects into `RaftLogStore` via `.with_event_writer()`, creates `EventHandle` (Arc-shared) for gRPC services, starts `EventsGarbageCollector` when `config.events.enabled`, passes `events_db` and `event_handle` to `LedgerServer`
- JWT wiring: Creates `JwtEngine`, injects `jwt_config`, `jwt_engine`, and `key_manager` into `LedgerServer` and `SagaOrchestrator`. `InMemoryKeyManager` available for test environments.
- Token maintenance: Spawns `TokenMaintenanceJob` with configurable interval (`token_maintenance_interval_secs`)
- Tiered storage wiring: Conditional `TieredSnapshotManager` construction based on `warm_url`, background demotion task
- **Insights**: Central orchestration point. Spawns 14 background jobs and holds `JoinHandle`s to keep them alive. Events GC and integrity scrubber run on all nodes (not leader-only). Saga orchestrator and orphan cleanup are leader-only. `PostErasureCompactionJob` registered with watchdog (timeout 600s). `InMemoryKeyManager` (in `store/src/crypto/key_manager.rs`) is public and test-friendly.

#### `config.rs`

- **Purpose**: ServerConfig with all subsystem configs, CLI/env-based configuration, and comprehensive tests
- **Key Types/Functions**:
- `Config`: Root config struct (raft, storage, batch, rate_limit, validation, tls, otel, events, jwt, token_maintenance_interval_secs, email_blinding_key, etc.) via clap `#[derive(Parser)]`
- `email_blinding_key: Option<String>`: Hex-encoded 32-byte key for HMAC-based email hashing. CLI arg `--email-blinding-key`, env var `INFERADB__LEDGER__EMAIL_BLINDING_KEY`. Onboarding RPCs return `FAILED_PRECONDITION` when not configured.
- `generate_runtime_config_schema()`: JSON Schema export for RuntimeConfig (used by `config schema` subcommand)
- Env var overrides: `INFERADB__LEDGER__<FIELD>` convention
- **Insights**: CLI args + env vars (no config file). Runtime reconfiguration via `UpdateConfig` RPC (JSON). JSON Schema export for validation.

#### `cluster_id.rs`

- **Purpose**: Cluster ID generation and persistence
- **Key Types/Functions**:
- Generates a unique cluster identifier during `init` and persists it to the data directory so all nodes in the cluster agree on the same ID
- Loaded on subsequent startups to detect cluster-identity mismatches (e.g., a node being pointed at the wrong data dir)
- **Insights**: Cluster ID is set once, via `init` against a seed node, and replicated to joining nodes. Used to reject nodes that belong to a different cluster at the membership level.

#### `dr_scheduler.rs`

- **Purpose**: TiKV-style data region membership scheduler (checker/scheduler separation)
- **Key Types/Functions**:
- Background task that observes data region membership state and proposes membership changes to keep regions within their replication targets
- Separates _checkers_ (detect drift from desired state) from _schedulers_ (emit concrete membership-change proposals) so policies can evolve independently
- **Insights**: This is the component that drives post-join / post-restart data-region rehydration. The 60-second delay for a newly-added node to serve reads (surfaced by the cluster-lifecycle and crash-recovery scripts) is governed by this scheduler's cadence + how quickly proposed membership changes commit through the global Raft log.

#### `coordinator.rs`

- **Purpose**: Multi-node bootstrap coordination via Snowflake IDs
- **Key Types/Functions**:
- `coordinate_bootstrap(my_node_id, my_address, config) -> Result<BootstrapDecision, CoordinatorError>`: Free function for multi-node bootstrap coordination, returns `BootstrapDecision` enum
- Snowflake ID: 64-bit (timestamp + node_id + sequence)
- **Insights**: Snowflake IDs enable decentralized ID generation (no coordination needed). Bootstrap requires initial seed nodes.

#### `discovery.rs`

- **Purpose**: Peer discovery via DNS resolution and node info exchange
- **Key Types/Functions**:
- `resolve_bootstrap_peers(config) -> Result<Vec<PeerInfo>>`: One-time peer resolution during bootstrap
- `discover_node_info(addr) -> Result<NodeInfo>`: Query a peer for its node metadata
- `DiscoveryError`: snafu error with resolution and connection variants
- **Insights**: Functional design (no trait-based providers). One-time resolution during bootstrap rather than background refresh. DNS-based peer discovery for cloud deployments.

#### `node_id.rs`

- **Purpose**: Node ID persistence (generation logic delegated to `types::snowflake`)
- **Key Types/Functions**:
- `load_or_generate_node_id(data_dir) -> Result<u64>`: Load from disk or generate via `types::snowflake::generate()`
- `write_node_id(data_dir, id) -> Result<()>`: Persist to `{data_dir}/node_id.json`
- `NodeIdError`: snafu error with IO and `Generate` (wrapping `SnowflakeError`) variants
- **Insights**: Core Snowflake generation extracted to `types::snowflake` (Task 3). This file retains only filesystem persistence logic. Persistence ensures stable node ID across restarts.

#### `shutdown.rs`

- **Purpose**: Signal handling (Ctrl-C, SIGTERM) and shutdown coordination
- **Key Types/Functions**:
- `shutdown_signal()`: Async signal handler for Ctrl-C/SIGTERM
- `ShutdownCoordinator`: Broadcast-based shutdown notification (subscribe/notify pattern)
- Used by `LedgerServer::serve()` (shutdown signal wired externally via `GracefulShutdown`)
- **Insights**: `shutdown_signal()` is the primary entry point used by `main.rs`. `ShutdownCoordinator` provides broadcast-based notification for components that need shutdown awareness.

#### Integration Tests

- **Purpose**: End-to-end tests covering replication, failover, multi-region, chaos, and more
- **Test Helper Modules**:
- `tests/common/mod.rs`: Shared cluster setup, assertions, test harness
- `tests/turmoil_common/mod.rs`: Turmoil-based network simulation helpers
- **Test Files**:
- `tests/stress_test.rs`: Concurrent write stress testing
- `tests/design_compliance.rs`: Validates implementation against DESIGN.md spec
- `tests/network_simulation.rs`: Turmoil-based network failure simulation
- `tests/chaos_consistency.rs`: Network partitions, node crashes, Byzantine scenarios
- `tests/externalized_state.rs`: Externalized state persistence and streaming snapshots
- `tests/isolation.rs`: Organization and vault isolation guarantees
- `tests/multi_region.rs`: Cross-region queries and transactions
- `tests/stress_integration.rs`: Integration-level stress tests
- `tests/leader_failover.rs`: Leader failure and re-election
- `tests/watch_blocks_realtime.rs`: Block streaming via gRPC
- `tests/ttl_gc.rs`: Time-to-live garbage collection
- `tests/saga_orchestrator.rs`: Distributed transaction orchestration
- `tests/orphan_cleanup.rs`: Resource leak cleanup
- `tests/write_read.rs`: Basic read/write/permission checks
- `tests/bootstrap_coordination.rs`: Multi-node cluster bootstrap
- `tests/stress_scale.rs`: Scale stress tests
- `tests/background_jobs.rs`: Background job lifecycle
- `tests/backup_restore.rs`: Backup and restore flows
- `tests/replication.rs`: Multi-node consensus tests
- `tests/stress_watch.rs`: Watch/streaming stress tests
- `tests/get_node_info.rs`: Node info RPC
- `tests/election.rs`: Raft election scenarios
- `tests/token_lifecycle.rs`: JWT token lifecycle integration tests (21 tests covering user session lifecycle, vault token lifecycle, refresh token theft detection, concurrent refresh+revoke races, scope update on refresh, signing key rotation/revocation/auto-bootstrap, state machine determinism, token maintenance job, rate limiting)
- `tests/onboarding.rs`: Self-service user onboarding integration tests (17 tests covering initiate verification, verify new/existing user, wrong/empty code rejection, region validation, email validation, malformed token handling, complete without verify, wrong token hash, re-verification token invalidation, domain separation, missing blinding key FAILED_PRECONDITION)
- `tests/invitation.rs`: Organization invitation integration tests
- `tests/integration.rs`: Main integration test binary entry point
- `tests/stress.rs`: Stress test module entry point
- `tests/external.rs`: External test module entry point
- **Insights**: Comprehensive end-to-end coverage. Tests require a running cluster (expected failures in local dev). Turmoil enables deterministic network simulation without real network I/O.

---

## Crate: `inferadb-ledger-test-utils`

- **Purpose**: Shared testing infrastructure (strategies, assertions, crash injection, test directories).
- **Dependencies**: `types` (for domain types)
- **Quality Rating**: ★★★★★

### Files

#### `lib.rs`

- **Purpose**: Re-exports test utilities; inline integration tests (5 `#[test]` functions)
- **Key Types/Functions**:
- Modules: `test_dir`, `config`, `crash_injector`, `strategies` (strategies is `pub mod`, rest are private with `pub use` re-exports)
- Re-exports: `TestDir`, `test_batch_config`, `CrashInjector`, `CrashPoint`
- **Insights**: Tests for all utilities live in lib.rs's `#[cfg(test)] mod tests` rather than in each submodule's own tests (crash_injector and strategies are exceptions with their own test modules). This is unusual but works well for a small utility crate.

#### `config.rs`

- **Purpose**: Test config factories for `BatchConfig`
- **Key Types/Functions**:
- `test_batch_config() -> BatchConfig`: Returns config for tests (max_batch_size=10, batch_timeout=10ms, coalesce_enabled=false)
- **Insights**: Provides sensible batch config defaults for test suites. Avoids hardcoding batch parameters in each test file.

#### `crash_injector.rs`

- **Purpose**: Deterministic crash injection for testing dual-slot commit protocol recovery
- **Key Types/Functions**:
- `CrashInjector`: Thread-safe injector using `AtomicU32` for counters (`sync_count`, `header_write_count`, `page_write_count`) and `AtomicBool` for flags (`crashed`, `armed`). Wrapped in `Arc<Self>` via `new()`.
- `CrashPoint`: 5 variants: `BeforeFirstSync`, `AfterFirstSync`, `DuringGodByteFlip`, `AfterSecondSync`, `DuringPageWrite` — each models a specific point in the dual-slot commit sequence.
- `arm()` / `disarm()`: Enable/disable injection (starts disarmed for setup operations)
- `on_sync() -> bool`, `on_header_write() -> bool`, `on_page_write(page_threshold) -> bool`: Hook methods that return `true` when the crash should trigger. Called by storage backend during commit operations.
- Unit tests validating each crash point and arm/disarm lifecycle
- **Insights**: Deterministic crash injection (not random) — each `CrashPoint` triggers at a precise operation count. The dual-slot commit protocol diagram in the module docs maps crash points to on-disk states. Crash tests in store crate exercise all 5 crash points.

#### `strategies.rs`

- **Purpose**: Proptest strategy generators for all domain types
- **Key Types/Functions** (all `pub fn arb_*() -> impl Strategy<Value = T>`):
- Primitives: `arb_key()`, `arb_value()`, `arb_small_value()`, `arb_hash()`, `arb_tx_id()`, `arb_timestamp()`
- IDs: `arb_organization_id()`, `arb_organization_slug()`, `arb_vault_id()`, `arb_vault_slug()`, `arb_region()`, `arb_shard_id()`
- Relationship components: `arb_resource()`, `arb_relation()`, `arb_subject()`
- Domain types: `arb_entity()`, `arb_relationship()`, `arb_set_condition()`, `arb_operation()`, `arb_operation_sequence()`
- Blocks: `arb_transaction()`, `arb_block_header()`, `arb_vault_block()`, `arb_vault_entry()`, `arb_region_block()`, `arb_chain_commitment()`
- Events: `arb_event_entry()` (with supporting strategies for scope, action, outcome, emission)
- Proptest functions validate strategy output well-formedness
- **Insights**: Composable strategy functions (not `Arbitrary` derives) — complex types compose from simpler ones (e.g., `arb_transaction` uses `arb_tx_id`, `arb_operation_sequence`, `arb_timestamp`). Strategy generators; 30+ proptests across multiple crates use these strategies.

#### `test_dir.rs`

- **Purpose**: `TestDir` wrapper around `tempfile::TempDir` for managed temporary directories
- **Key Types/Functions**:
- `TestDir::new() -> Self`: Create temp directory (panics on failure — acceptable for test utilities)
- `path() -> &Path`: Get underlying path
- `join<P: AsRef<Path>>(path: P) -> PathBuf`: Convenience for `self.path().join(path)`
- `Default` impl delegates to `new()`
- Cleanup via `tempfile::TempDir`'s `Drop` (not a custom Drop impl)
- **Insights**: RAII cleanup of test directories. Prevents test pollution across parallel test runs.

---

## Cross-Cutting Observations

### 1. Error Handling Excellence

The codebase demonstrates state-of-the-art error handling:

- **snafu for server crates**: Server crates (`types`, `store`, `state`, `raft`, `services`, `server`) use snafu exclusively with implicit location tracking via `#[snafu(implicit)] location: snafu::Location`. The SDK crate uses `thiserror` for consumer-facing error types. No `anyhow` anywhere.
- **Structured error taxonomy**: `ErrorCode` enum with `code()`, `is_retryable()`, and `suggested_action()` methods on all error types.
- **Context selectors**: Propagation via `.context(XxxSnafu)?` captures location automatically, never manual error construction.
- **Rich error details**: Proto `ErrorDetails` message enriches gRPC errors with structured context (error_code, retryability, retry_after, context map, suggested_action). SDK decodes via `ServerErrorDetails`.
- **Attempt history**: SDK `SdkError` tracks retry attempts with `attempt_history: Vec<(u32, String)>` for debugging.

### 2. Builder Pattern with bon

The codebase uses the `bon` crate extensively for type-safe builders:

- **Simple structs**: `#[derive(bon::Builder)]` for basic configs
- **Fallible constructors**: `#[bon::bon] impl Foo { #[builder] pub fn new(...) -> Result<Self> }` for validation
- **Conventions**: `#[builder(into)]` for String fields (accepts &str), `#[builder(default)]` matched with `#[serde(default)]` for configs
- **Performance**: bon is a proc-macro with zero runtime overhead. Estimated compile-time impact: ~2 seconds per 10 structs.
- **Examples**: `ClientConfig`, `StorageConfig`, `RaftConfig`, `Transaction`, `TlsConfig` all use fallible builders

### 3. Testing Coverage

The codebase has exceptional test coverage:

- **Property-based testing**: Proptest functions across multiple `proptest!` blocks (types, proto, state, sdk, raft, store, test-utils) using proptest. Reusable strategy generators in test-utils. Nightly CI runs with 10k iterations via `PROPTEST_CASES` env var.
- **Crash recovery testing**: Crash injection tests in store crate using `CrashInjector`. Validates durability guarantees.
- **Chaos testing**: Integration tests cover network partitions, node crashes, Byzantine faults. In-process unit tests for Byzantine scenarios.
- **Benchmarks**: Criterion benchmarks for B+ tree, read/write operations, whitepaper validation. CI tracks regressions via benchmark.yml workflow.
- **Integration tests**: Integration test files in server crate covering replication, failover, multi-region, backup/restore, rate limiting, cancellation, circuit breaker, API version, quotas, resource metrics, dependency health, canonical log lines, config reload, externalized state persistence, streaming snapshots, JWT token lifecycle (21 tests), self-service onboarding (17 tests), and stress testing.
- **Unit tests**: `#[test]` and `#[tokio::test]` functions across all crates. Coverage target: 90%+.

### 4. Security Practices

The codebase follows excellent security practices:

- **No `unsafe` code**: Zero unsafe blocks in entire codebase (enforced by CI).
- **Constant-time comparison**: `hash_eq()` provides timing-attack-resistant hash comparison via `subtle::ConstantTimeEq`, actively used in Merkle proof verification, chain integrity checks, recovery chain continuity, and onboarding code/token hash validation.
- **Input validation**: `validation.rs` enforces character whitelists and size limits to prevent injection attacks and DoS.
- **No `.unwrap()`**: All error handling via snafu `.context()`. No panics in production code.
- **Deterministic replication**: Apply-phase events are byte-identical across all Raft replicas — leader-assigned timestamps via `RaftPayload` ensure identical B+ tree keys, pagination cursors, and snapshot contents across nodes.
- **Event logging**: Organization-scoped event logging system with queryable audit trails via gRPC `EventsService`. Tracks all mutations, denials, and admin operations. Two emission paths: apply-phase (deterministic, replicated) and handler-phase (node-local, best-effort). TTL-based retention with automatic garbage collection. O(log n) `GetEvent` via secondary index (no false-not-found at scale).
- **JWT token security**: EdDSA (Ed25519) only — defense-in-depth algorithm enforcement (raw header string pre-check + library-level `Validation::algorithms`). `kid` UUID format validation before state lookup (prevents cache pollution). Error normalization (identical UNAUTHENTICATED for "kid not found" and "signature invalid"). Signing key private material: `Zeroizing<Vec<u8>>` for cached bytes, `EncodingKey` built on-demand per signing op (not cached). Refresh token `ilrt_` prefix for secret scanning tool detection. Refresh token theft detection via poisoned-family pattern (O(1) poison on reuse, background GC). Token version binding prevents race between refresh and concurrent revocation.
- **Data residency & crypto-shredding**: No plaintext PII in ANY Raft log entry (GLOBAL or REGIONAL). PII is sealed before Raft proposal: `WriteOrganizationProfile` seals org name with per-org `OrgShredKey` via AES-256-GCM, `WriteOnboardingUserProfile` seals PII with `UserShredKey`. `EncryptedOrgSystem` wraps org-scoped SystemRequests encrypted with `OrgShredKey` — destroying the shred key makes all org data in Raft logs unreadable (crypto-shredding). `PostErasureCompactionJob` triggers proactive Raft snapshots after erasure to evict plaintext from log retention. `CreateEmailVerification` and `CreateAppClientAssertion` had PII fields removed entirely. Compile-time `classify_system_request()` test forces scope classification of every variant. `EraseUser` no longer carries `erased_by` in the Raft log (actor identity in canonical log lines only). `KeyTier`/`validate_key_tier()` safeguards catch cross-tier write bugs at runtime.
- **Onboarding security**: Verification codes hashed with domain-separated HMAC (`"code:"` prefix). Onboarding tokens use 256-bit entropy with bare SHA-256 (no blinding key dependency). Rate limiting Raft-enforced (3 initiations/hr/email/region, 5 attempts/code). `EmailBlindingKey` derives `Zeroize`+`ZeroizeOnDrop`. Token consumed on use (single-use with 12hr TTL). Idempotent `complete_registration` tradeoff documented (consumed token can't be re-validated on retry — see PRD Security #15).
- **Credential security**: TOTP secrets encrypted in Raft log via `EncryptedUserSystemRequest`, crypto-shredded on user erasure. TOTP secrets are write-once (never returned after creation, stripped from gRPC responses via `strip_totp_secret()`). `TotpCredential.secret` uses `Zeroizing<Vec<u8>>` for secure memory cleanup, with manual `Debug` impl that redacts secret. TOTP verification in service layer (non-deterministic `SystemTime::now()`) — state machine uses deterministic `proposed_at` for expiry. TOTP attempts Raft-persisted (survive leader failover), state machine enforces 3-attempt limit as defense-in-depth. Challenge rate limiting (max 3 active per user) prevents cross-challenge brute force. Recovery code hashes compared via `hash_eq()` (constant-time). `VerifyTotp` and `ConsumeRecoveryCode` directly create sessions — no intermediate MFA token.
- **Envelope encryption for signing keys**: Per-key DEK wrapped by RMK via AES-KWP, private key encrypted with AES-256-GCM using `kid` as AAD (binds ciphertext to key identity). Same pattern as page-level crypto.
- **TLS support**: Client and server support TLS with optional mTLS (client certificates).
- **Rate limiting**: 3-tier token bucket rate limiter (client/organization/backpressure) prevents abuse.
- **Quota enforcement**: Per-organization resource quotas (vault count, storage size, request rate) prevent resource exhaustion.

### 5. Observability

The codebase has comprehensive observability:

- **OpenTelemetry tracing**: W3C Trace Context propagation across services. OTLP exporter for traces/metrics. Configurable sampling ratio.
- **Prometheus metrics**: Extensive metrics covering SLI/SLO, resource saturation, batch queues, rate limiting, hot keys, circuit breakers, onboarding (initiation/verification/registration counters, GC counters), etc. Custom histogram buckets for SLI.
- **Canonical log lines**: Single log line per request with all context (request_id, trace_id, client_id, organization, vault, method, status, latency, raft_round_trips, error_class, sdk_version, client_ip).
- **Structured logging**: Request-level structured logging with tracing crate. Context propagation via spans.
- **SDK metrics**: Client-side metrics (request latency, retries, circuit state, connection pool) via `SdkMetrics` trait. Noop default for zero overhead.
- **Event logging**: Persistent event system with `EventsService` gRPC API (ListEvents, GetEvent, CountEvents, IngestEvents). Dual-path emission (apply-phase deterministic via leader-assigned timestamps + handler-phase node-local). O(log n) `GetEvent` via `EventIndex` secondary index. Optimized snapshot collection via `EmissionMeta` thin deserialization (handler-phase events skipped without full decode). Prometheus metrics for event writes, GC cycles, and ingestion. Events are organization-scoped with configurable TTL and automatic GC. Supports external service ingestion (Engine, Control) via `IngestEvents` RPC with per-source rate limiting.

### 6. Enterprise Features

The codebase includes numerous production-ready features:

- **Graceful shutdown with leader transfer**: Pre-phases (mark Draining → attempt leader transfer) before 6-phase shutdown (health drain, Raft snapshot, job stop, Raft shutdown, connection drain, service stop). NodePhase: Starting → Ready → Draining → ShuttingDown. Draining phase rejects new proposals via `check_not_draining()` while allowing in-flight requests to complete. Leader transfer is best-effort (failure falls back to election timeout). ConnectionTracker and BackgroundJobWatchdog. Configurable via `leader_transfer_timeout_secs` (default 10, 0 disables).
- **Runtime reconfiguration**: UpdateConfig/GetConfig RPCs, lock-free reads via ArcSwap.
- **Backup & restore**: Snapshot-based backups with zstd compression, chain verification, S3/GCS/Azure backends.
- **Circuit breaker**: Per-endpoint state machine (Closed→Open→HalfOpen) in SDK. Prevents cascade failures.
- **Request cancellation**: CancellationToken support in SDK. Manual retry loop with `tokio::select!` for cancellation.
- **Deadline propagation**: grpc-timeout header parsing, effective_timeout (min of config and client), near-deadline rejection (100ms threshold).
- **Dependency health checks**: Disk writability, Raft lag, peer reachability. TTL cache prevents check storms.
- **API version negotiation**: x-ledger-api-version header, interceptor + tower layer, backward compatibility.
- **Hot key detection**: Count-Min Sketch with rotating windows, top-k via min-heap, rate-limited warnings.
- **Auto divergence recovery**: Background job comparing Raft log vs. state, automatic recovery from divergence.
- **Tiered storage**: Hot/warm/cold tiers with S3/GCS/Azure backends. Multipart upload for large snapshots (>50 MB). `load_latest_hot()` for Raft follower catch-up (avoids S3 latency). Background demotion task with graceful S3 degradation. Age-based or access-based promotion/demotion.
- **Multi-region**: Horizontal scaling via multiple Raft groups. Cross-region queries and transactions via saga pattern.
- **Resource quotas**: Per-organization limits (vault count, storage size, request rate). 3-tier resolution (organization → tier → global).
- **B+ tree compaction**: Merge underfull leaves, reclaim dead space. Background job with configurable interval.
- **Event logging**: Organization-scoped audit trails in dedicated `events.db`. Apply-phase (deterministic via `RaftPayload` timestamps, byte-identical across replicas) and handler-phase (node-local) emission. O(log n) `GetEvent` via `EventIndex` secondary index (eliminates false-not-found from former scan cap). Optimized `scan_apply_phase` via `EmissionMeta` thin deserialization. GC with TTL. EventsService for queries. IngestEvents for cross-service audit aggregation.
- **Externalized state & streaming snapshots**: AppliedState fields persisted to dedicated B+ tree tables (VaultHeights, VaultHashes, VaultHealth) instead of monolithic postcard blob. File-based streaming snapshots with zstd compression + SHA-256 checksums via `SnapshotWriter`/`SnapshotReader`. Three-way format detection for migration. Client sequence persistence with TTL eviction for cross-failover deduplication. Automatic write forwarding via `RaftManager`.
- **Envelope encryption at rest**: AES-256-GCM per-page DEKs wrapped with Region Master Keys (RMK) via AES-KWP. Transparent `EncryptedBackend` wrapper. DEK cache for O(1) amortized decryption. Background `DekRewrapJob` for RMK rotation (re-wraps metadata only, never re-encrypts page bodies). Multi-provider key management (Infisical, Vault, AWS KMS, GCP KMS, Azure Key Vault).
- **Per-region database layout**: `RegionStorageManager` enforces per-region directory tree (`global/` for control plane, `regions/{name}/` for data regions). Eliminates cross-region write-lock contention. Enables per-region encryption keys and independent scaling.
- **JWT token system**: Full JWT lifecycle — user session tokens (global signing key, `TokenVersion` forced invalidation), vault access tokens (per-org signing key, scope-based authorization). `JwtEngine` with ArcSwap lock-free cache. Refresh token family-based theft detection (poisoned-family pattern). Signing key auto-bootstrap via saga orchestrator (global key on cluster init, org key on org creation). `TokenMaintenanceJob` background job (expired token cleanup, poisoned family GC, signing key grace period transition). Cascade revocation on app disable, vault disconnect, org deletion. Rate limiting on vault token creation via per-app client key.
- **Privacy-preserving email hashing**: HMAC-SHA256 with `EmailBlindingKey` for global email uniqueness across regions without storing plaintext PII on the control plane.
- **Per-organization crypto-shredding**: `OrgShredKey` (AES-256-GCM per-org key, stored at `_shred:org:{id}`) seals org-scoped PII in Raft entries. `EncryptedOrgSystem` wraps SystemRequests encrypted with `OrgShredKey`. Organization purge destroys the shred key, making all org data in Raft logs unreadable. `PostErasureCompactionJob` triggers proactive snapshots to evict plaintext from log retention window. Retry-resilient purge with exponential backoff. Per-user crypto-shredding via `UserShredKey` (stored at `_shred:user:{id}`).
- **Team management**: `AddTeamMember`/`RemoveTeamMember` via REGIONAL Raft. SDK methods for team CRUD. Prevents removal of last manager.
- **Organization invitations**: Full invitation lifecycle — admin creates invitations by email with designated role and optional team, invitees accept/decline via authenticated user RPCs. 5-state machine (Pending→Accepted/Declined/Expired/Revoked). Privacy-preserving: uniform responses regardless of account existence, timing equalization via dummy reads. Four-check rate limiting (per-user 50/day, per-org 100/day, per-email pending cap 10, per-email daily total 20). Multi-email HMAC matching for all verified user emails. CAS enforcement in Raft apply handlers prevents TOCTOU races. Partial failure recovery on acceptance (Ledger-internal, not delegated to Control). InviteMaintenanceJob background job with bounded per-cycle cost. Org deletion revokes all pending invitations. Blinding key rotation rehashes both GLOBAL indexes and REGIONAL records.
- **App session revocation**: `RevokeAllAppSessions` bulk revokes all refresh tokens for an app with atomic `TokenVersion` increment.
- **Multi-credential authentication**: `UserCredential` entity with three types (passkey, TOTP, recovery code). Ledger-authoritative TOTP verification (service layer verifies, state machine creates sessions). `PendingTotpChallenge` with Raft-persisted attempt budget. Direct session creation from `VerifyTotp`/`ConsumeRecoveryCode` (no intermediate MFA token). Credential CRUD with safety invariants: one-TOTP/one-recovery-code per user, last-credential guard, passkey uniqueness.
- **And more**...

### 7. Documentation Quality

The codebase has excellent documentation:

- **ADRs (Architecture Decision Records)**: ADRs in `docs/adr/` covering key decisions (bucket-based-state-commitment, dual-slot-commit-protocol, embedded-btree-engine, count-min-sketch-hot-key-detection, three-tier-rate-limiting, atomic-health-state-machine, moka-tinylfu-idempotency-cache, network-trust-model, openraft-09-version-choice, server-assigned-sequences).
- **Invariant docs**: `docs/specs/invariants.md` documents critical system invariants.
- **Module docs**: All crates have module-level documentation with examples.
- **rustdoc examples**: Many public functions have ` ```no_run ` examples (cargo test skips execution, cargo doc validates syntax).
- **Architecture docs**: `docs/architecture/audit-protocol.md` documents the centralized audit architecture and shared event protocol for all InferaDB services. `DESIGN.md` includes JWT token architecture section (envelope encryption, ArcSwap cache, refresh token theft detection, cascade revocation, scope policy, security boundaries). `INTEGRATION.md` covers Control/Engine JWT integration guide with flow diagrams, SDK reference, security considerations, and key rotation runbook.
- **Development docs**: `docs/development/events.md` — SDK usage guide for events client methods, EventFilter builder, query patterns, ingestion guide, adding new event types
- **Operations docs**: `docs/operations/` covers alerting, API versioning, background jobs, capacity planning, configuration, dashboards, data residency architecture (including US region replication behavior and CCPA guidance), deployment, events, logging, metrics reference, multi-region, organization metrics, production deployment tutorial, region management, security, SLO, troubleshooting, vault repair, and a README. Includes `events.md` with full EventConfig reference, event catalog (system + org events), Prometheus metrics, and troubleshooting.
- **Grafana dashboards**: `docs/operations/dashboards/grafana-events-v1.json` — Grafana dashboard for event monitoring (write rates, emission paths, denial tracking, GC health)
- **Client docs**: `docs/client/` has guides covering admin, API, discovery, errors, health, idempotency, and SDK.
- **Error codes**: `docs/errors.md` documents all error codes with descriptions, causes, and suggested actions.

### 8. Dual-ID Architecture

Both organizations and vaults use two identifiers:

- **`OrganizationId(i64)`** / **`VaultId(i64)`** — Internal sequential IDs for B+ tree key density and storage performance. Generated via `SequenceCounters`. Never exposed in APIs.
- **`OrganizationSlug(u64)`** / **`VaultSlug(u64)`** — External Snowflake IDs (42-bit timestamp + 22-bit sequence). The sole identifiers in gRPC APIs and SDK. Generated via `types::snowflake::generate_organization_slug()` / `generate_vault_slug()`.

Translation happens at the gRPC service boundary via `SlugResolver`, backed by `AppliedState`'s bidirectional maps (`slug_index`/`id_to_slug` for orgs, `vault_slug_index`/`vault_id_to_slug` for vaults). All internal subsystems (rate limiter, quota checker, storage, state machine) operate on internal IDs. Responses use reverse lookup to embed slugs. `VaultMeta` stores its slug for denormalized access in list/get responses.

### 9. Code Quality Achievements

Two previously identified large-file concerns have been resolved:

- **`types/src/config.rs`** → Split into `config/` directory module with 9 submodules (mod.rs, node.rs, storage.rs, raft.rs, resilience.rs, observability.rs, runtime.rs, encryption.rs, key_management.rs). All public APIs preserved via `pub use` re-exports. Encryption and key management configs added for envelope encryption at rest.
- **`raft/src/log_storage.rs`** → Split into `log_storage/` directory module with 5 submodules + 1 subdirectory (mod.rs, types.rs, accessor.rs, store.rs, raft_impl.rs, operations/ with mod.rs + 5 helper files). Growth from externalized state persistence, streaming snapshots, client sequence eviction, and PendingExternalWrites accumulator. The `operations/` subdirectory further splits apply logic into `token_helpers.rs`, `app_helpers.rs`, `org_helpers.rs`, `team_helpers.rs`, and `helpers.rs`. Fields use `pub(super)` for cross-submodule access. All openraft trait implementations preserved.

---

## Summary

InferaDB Ledger is a **production-grade blockchain database** with exceptional engineering quality:

- **10 crates** of Rust (excluding generated code), 90%+ test coverage target
- **Zero `unsafe` code**, comprehensive error handling (snafu on server crates, thiserror on SDK), structured error taxonomy
- **Custom B+ tree engine** with ACID transactions, crash recovery, compaction
- **Two-layer consensus**: purpose-built multi-shard Raft engine in `inferadb-ledger-consensus` (event-driven `Reactor`, segmented WAL, pluggable backends) + operationalization in `inferadb-ledger-raft` (openraft integration, apply-phase parallelism, saga orchestrator, background jobs). Simulation-testable via the engine's abstract `Clock`/`RngSource`/`NetworkTransport`/`WalBackend` traits.
- **Enterprise features**: JWT token authentication (EdDSA, refresh token theft detection, signing key auto-bootstrap, cascade revocation), multi-credential authentication (passkeys, TOTP, recovery codes with Ledger-authoritative verification), organization invitations (privacy-preserving lifecycle with four-check rate limiting, multi-email HMAC matching, CAS state machine, partial failure recovery), graceful shutdown with leader transfer (Draining phase, best-effort handoff before election timeout), circuit breaker, rate limiting, hot key detection, quota enforcement, backup/restore, tiered storage with multipart upload, API versioning, deadline propagation, dependency health checks, runtime reconfiguration, organization-scoped event logging, externalized state persistence, streaming snapshots, automatic write forwarding, and many more
- **Excellent observability**: OpenTelemetry tracing, Prometheus metrics, canonical log lines, structured request logging, SDK-side metrics, queryable event audit trails via gRPC EventsService
- **Comprehensive testing**: Unit tests, property-based tests (proptest), crash recovery tests, chaos tests, integration tests
- **Security practices**: No unsafe, constant-time comparison, input validation, deterministic event replication via leader-assigned timestamps, event logging with audit trails, TLS/mTLS, rate limiting, quotas, envelope encryption at rest (AES-256-GCM with per-page DEKs wrapped by Region Master Keys), privacy-preserving email hashing (HMAC-SHA256 blinding), JWT token security (defense-in-depth algorithm enforcement, kid validation, signing key zeroization, refresh token theft detection), credential encryption in Raft log (TOTP secrets encrypted via UserShredKey, write-once with Zeroize)
- **Documentation**: ADRs, invariant specs, module docs, rustdoc examples, operations guides, client guides, error code reference, events architecture doc, Grafana dashboard templates

Overall assessment: **★★★★★ Exemplary codebase** ready for production deployment.
