//! Wire-based schema management ops.
//!
//! Mirrors [`super::super::ops::schema`]: same returned domain types
//! ([`SchemaVersion`], [`SchemaVersionSummary`], [`SchemaDeployResult`],
//! [`SchemaDiffChange`]), same `Result<_, SdkError>` shape on the consumer
//! side. Schema management has no dedicated server-side handler — both the
//! tonic and wire surfaces implement it as a pure client-side convention
//! layered over the Read / Write services, using well-known key prefixes:
//!
//! - `schema:v{version}` — schema definition + description for a specific version
//! - `schema:current` — version pointer for the currently active version
//! - `schema:latest` — version pointer for the highest deployed version
//! - `schema:history` — JSON array of previously activated versions (for rollback)
//!
//! Internal dispatch goes through [`super::data::read`] /
//! [`super::data::write`] (which themselves wrap the wire-services-generated
//! [`ReadServiceClient`](inferadb_ledger_wire_services::ReadServiceClient) /
//! [`WriteServiceClient`](inferadb_ledger_wire_services::WriteServiceClient))
//! rather than constructing a `SchemaServiceClient` — calling the schema
//! opcodes would surface `Internal "not implemented"` since the macro-emitted
//! service has no handler. The opcode block is reserved either way.
//!
//! Each public op may issue multiple sub-RPCs (e.g. `deploy_schema` reads the
//! latest pointer then writes both the version body and the latest pointer
//! in a single transaction). The supplied `request_id_seed` is consumed
//! deterministically: the first sub-call uses it directly, subsequent calls
//! derive their frame correlation ID via `wrapping_add`, so a retry cycle
//! that rotates the seed gets a fresh contiguous range without coordinating
//! per-call IDs at the call site. Idempotency keys for the underlying writes
//! are generated freshly per sub-write — the schema op itself is the unit of
//! logical idempotency from the consumer's perspective; mid-op retries that
//! preserve commit-once semantics happen one layer down inside
//! [`super::data::write`] callers' own retry policy in F.1.
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests in this file cover the
//! JSON serialization helpers (`StoredSchema`, `VersionPointer`, history
//! vec), the key-prefix discipline, and the field-level `diff_json_objects`
//! function in isolation. The seven per-op entry points themselves are not
//! exercised yet — [`LedgerClient`](crate::LedgerClient) keeps dispatching
//! through the tonic `ops::schema` path until F.1 flips the connection pool.

use std::sync::Arc;

use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};
use inferadb_ledger_wire_transport::WireClient;
use serde::{Deserialize, Serialize};

use crate::{
    error::{Result, SdkError},
    ops_wire::data,
    types::{
        query::Operation,
        read::ReadConsistency,
        schema::{SchemaDeployResult, SchemaDiffChange, SchemaVersion, SchemaVersionSummary},
    },
};

// ---------------------------------------------------------------------------
// Internal storage shapes (mirror `ops::schema`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSchema {
    definition: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VersionPointer {
    version: u32,
}

// ---------------------------------------------------------------------------
// Key helpers (mirror `ops::schema`)
// ---------------------------------------------------------------------------

fn schema_version_key(version: u32) -> String {
    format!("schema:v{version}")
}

const SCHEMA_CURRENT_KEY: &str = "schema:current";
const SCHEMA_LATEST_KEY: &str = "schema:latest";
const SCHEMA_HISTORY_KEY: &str = "schema:history";

// ---------------------------------------------------------------------------
// Deploy
// ---------------------------------------------------------------------------

/// Deploys a new schema version via the wire transport.
///
/// Mirrors [`LedgerClient::deploy_schema`](crate::LedgerClient::deploy_schema)
/// at the dispatch layer. Writes the schema body to `schema:v{version}` and
/// updates the `schema:latest` pointer in a single
/// [`super::data::write`] transaction. When `version` is `None`, reads the
/// current `schema:latest` pointer first and auto-increments from it (so
/// the call issues two sub-RPCs: a `Read` then a `Write`).
///
/// # Errors
///
/// - [`SdkError::Validation`] when an explicit `version` of zero is passed or JSON serialization
///   fails.
/// - All errors raised by the underlying [`super::data::read`] / [`super::data::write`] paths
///   (transport, server-returned [`WireError`]s, idempotency, CAS).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn deploy_schema(
    wire_client: Arc<WireClient>,
    request_id_seed: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    vault: VaultSlug,
    client_id: String,
    definition: serde_json::Value,
    version: Option<u32>,
    description: Option<String>,
) -> Result<SchemaDeployResult> {
    let mut next_id = request_id_seed;

    let version = match version {
        Some(v) => {
            if v == 0 {
                return Err(SdkError::Validation {
                    message: "schema version must be greater than 0".to_owned(),
                });
            }
            v
        },
        None => {
            let latest = read_version_pointer(
                Arc::clone(&wire_client),
                consume(&mut next_id),
                caller,
                organization,
                vault,
                SCHEMA_LATEST_KEY,
            )
            .await?;
            latest.map_or(1, |p| p.version + 1)
        },
    };

    let stored = StoredSchema { definition, description };
    let bytes = serde_json::to_vec(&stored).map_err(|e| SdkError::Validation {
        message: format!("failed to serialize schema: {e}"),
    })?;

    let latest_pointer = VersionPointer { version };
    let latest_bytes = serde_json::to_vec(&latest_pointer).map_err(|e| SdkError::Validation {
        message: format!("failed to serialize version pointer: {e}"),
    })?;

    let key = schema_version_key(version);
    let ops = vec![
        Operation::set_entity(&key, bytes, None, None),
        Operation::set_entity(SCHEMA_LATEST_KEY, latest_bytes, None, None),
    ];

    data::write(
        wire_client,
        consume(&mut next_id),
        caller,
        organization,
        Some(vault),
        client_id,
        fresh_idempotency_key(),
        ops,
    )
    .await?;

    Ok(SchemaDeployResult { version })
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

/// Lists every deployed schema version for a vault via the wire transport.
///
/// Mirrors [`LedgerClient::list_schema_versions`](crate::LedgerClient::list_schema_versions).
/// Reads `schema:latest` to learn the maximum version, `schema:current` to
/// learn which version is active, then issues a sequential `Read` per
/// version slot (1..=max) to populate the `has_definition` flag — gaps in
/// the version sequence (rolled-back deletes etc.) surface as
/// `has_definition: false` rather than being elided.
///
/// # Errors
///
/// All errors raised by the underlying [`super::data::read`] path.
pub(crate) async fn list_schema_versions(
    wire_client: Arc<WireClient>,
    request_id_seed: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    vault: VaultSlug,
) -> Result<Vec<SchemaVersionSummary>> {
    let mut next_id = request_id_seed;

    let latest = read_version_pointer(
        Arc::clone(&wire_client),
        consume(&mut next_id),
        caller,
        organization,
        vault,
        SCHEMA_LATEST_KEY,
    )
    .await?;
    let max_version = match latest {
        Some(p) => p.version,
        None => return Ok(Vec::new()),
    };

    let current = read_version_pointer(
        Arc::clone(&wire_client),
        consume(&mut next_id),
        caller,
        organization,
        vault,
        SCHEMA_CURRENT_KEY,
    )
    .await?;
    let active_version = current.map(|p| p.version);

    let mut schemas = Vec::with_capacity(max_version as usize);
    for v in 1..=max_version {
        let key = schema_version_key(v);
        let exists = data::read(
            Arc::clone(&wire_client),
            consume(&mut next_id),
            caller,
            organization,
            Some(vault),
            key,
            ReadConsistency::default(),
        )
        .await?
        .is_some();
        schemas.push(SchemaVersionSummary {
            version: v,
            has_definition: exists,
            is_active: active_version == Some(v),
        });
    }

    Ok(schemas)
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------

/// Retrieves a specific schema version via the wire transport.
///
/// Mirrors [`LedgerClient::get_schema`](crate::LedgerClient::get_schema).
/// Issues a single `Read` against `schema:v{version}` and decodes the
/// stored JSON envelope.
///
/// # Errors
///
/// - [`SdkError::Rpc`] with [`ErrorCode::NotFound`](inferadb_ledger_wire::ErrorCode::NotFound) when
///   the version slot is empty.
/// - [`SdkError::Validation`] when the stored bytes fail to deserialize.
/// - All errors raised by the underlying [`super::data::read`] path.
pub(crate) async fn get_schema(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    vault: VaultSlug,
    version: u32,
) -> Result<SchemaVersion> {
    let stored = read_stored_schema(wire_client, request_id, caller, organization, vault, version)
        .await?
        .ok_or_else(|| not_found(format!("schema version {version} not found")))?;

    Ok(SchemaVersion { version, definition: stored.definition, description: stored.description })
}

// ---------------------------------------------------------------------------
// Activate
// ---------------------------------------------------------------------------

/// Activates a specific schema version via the wire transport.
///
/// Mirrors [`LedgerClient::activate_schema`](crate::LedgerClient::activate_schema).
/// Verifies the version body exists, pushes the previously-active version
/// onto the history stack, and writes both the new `schema:current` pointer
/// and the updated history in a single transaction. Issues up to four
/// sub-RPCs (Read body / Read current / Read history / Write).
///
/// # Errors
///
/// - [`SdkError::Rpc`] with [`ErrorCode::NotFound`](inferadb_ledger_wire::ErrorCode::NotFound) when
///   the requested version body is missing.
/// - [`SdkError::Validation`] when JSON serialization fails or stored bytes are corrupt.
/// - All errors raised by the underlying [`super::data::read`] / [`super::data::write`] paths.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn activate_schema(
    wire_client: Arc<WireClient>,
    request_id_seed: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    vault: VaultSlug,
    client_id: String,
    version: u32,
) -> Result<u32> {
    let mut next_id = request_id_seed;

    let key = schema_version_key(version);
    let exists = data::read(
        Arc::clone(&wire_client),
        consume(&mut next_id),
        caller,
        organization,
        Some(vault),
        key,
        ReadConsistency::default(),
    )
    .await?
    .is_some();
    if !exists {
        return Err(not_found(format!("schema version {version} not found")));
    }

    let current = read_version_pointer(
        Arc::clone(&wire_client),
        consume(&mut next_id),
        caller,
        organization,
        vault,
        SCHEMA_CURRENT_KEY,
    )
    .await?;
    let mut history =
        read_history(Arc::clone(&wire_client), consume(&mut next_id), caller, organization, vault)
            .await?;

    if let Some(current_pointer) = current {
        history.push(current_pointer.version);
    }

    let pointer = VersionPointer { version };
    let pointer_bytes = serde_json::to_vec(&pointer).map_err(|e| SdkError::Validation {
        message: format!("failed to serialize version pointer: {e}"),
    })?;
    let history_bytes = serde_json::to_vec(&history).map_err(|e| SdkError::Validation {
        message: format!("failed to serialize history: {e}"),
    })?;

    let ops = vec![
        Operation::set_entity(SCHEMA_CURRENT_KEY, pointer_bytes, None, None),
        Operation::set_entity(SCHEMA_HISTORY_KEY, history_bytes, None, None),
    ];

    data::write(
        wire_client,
        consume(&mut next_id),
        caller,
        organization,
        Some(vault),
        client_id,
        fresh_idempotency_key(),
        ops,
    )
    .await?;

    Ok(version)
}

// ---------------------------------------------------------------------------
// Rollback
// ---------------------------------------------------------------------------

/// Rolls back to the previous schema version via the wire transport.
///
/// Mirrors [`LedgerClient::rollback_schema`](crate::LedgerClient::rollback_schema).
/// Pops the most recent entry off `schema:history`, sets `schema:current`
/// to that version, and writes back the truncated history in a single
/// transaction.
///
/// # Errors
///
/// - [`SdkError::Validation`] when the history is empty (no rollback target) or JSON serialization
///   fails.
/// - All errors raised by the underlying [`super::data::read`] / [`super::data::write`] paths.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn rollback_schema(
    wire_client: Arc<WireClient>,
    request_id_seed: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    vault: VaultSlug,
    client_id: String,
) -> Result<u32> {
    let mut next_id = request_id_seed;

    let mut history =
        read_history(Arc::clone(&wire_client), consume(&mut next_id), caller, organization, vault)
            .await?;

    let previous = history.pop().ok_or_else(|| SdkError::Validation {
        message: "no activation history to rollback to".to_owned(),
    })?;

    let pointer = VersionPointer { version: previous };
    let pointer_bytes = serde_json::to_vec(&pointer).map_err(|e| SdkError::Validation {
        message: format!("failed to serialize version pointer: {e}"),
    })?;
    let history_bytes = serde_json::to_vec(&history).map_err(|e| SdkError::Validation {
        message: format!("failed to serialize history: {e}"),
    })?;

    let ops = vec![
        Operation::set_entity(SCHEMA_CURRENT_KEY, pointer_bytes, None, None),
        Operation::set_entity(SCHEMA_HISTORY_KEY, history_bytes, None, None),
    ];

    data::write(
        wire_client,
        consume(&mut next_id),
        caller,
        organization,
        Some(vault),
        client_id,
        fresh_idempotency_key(),
        ops,
    )
    .await?;

    Ok(previous)
}

// ---------------------------------------------------------------------------
// Get active
// ---------------------------------------------------------------------------

/// Retrieves the currently-active schema via the wire transport.
///
/// Mirrors [`LedgerClient::get_active_schema`](crate::LedgerClient::get_active_schema).
/// Reads the `schema:current` pointer, then reads the body of that version.
///
/// # Errors
///
/// - [`SdkError::Rpc`] with [`ErrorCode::NotFound`](inferadb_ledger_wire::ErrorCode::NotFound) when
///   no active version is set, or when the active version's body is missing (split-brain pointer).
/// - [`SdkError::Validation`] when stored bytes are corrupt.
/// - All errors raised by the underlying [`super::data::read`] path.
pub(crate) async fn get_active_schema(
    wire_client: Arc<WireClient>,
    request_id_seed: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    vault: VaultSlug,
) -> Result<SchemaVersion> {
    let mut next_id = request_id_seed;

    let current = read_version_pointer(
        Arc::clone(&wire_client),
        consume(&mut next_id),
        caller,
        organization,
        vault,
        SCHEMA_CURRENT_KEY,
    )
    .await?
    .ok_or_else(|| not_found("no active schema version".to_owned()))?;

    let stored = read_stored_schema(
        wire_client,
        consume(&mut next_id),
        caller,
        organization,
        vault,
        current.version,
    )
    .await?
    .ok_or_else(|| {
        not_found(format!("active schema version {} has no definition", current.version))
    })?;

    Ok(SchemaVersion {
        version: current.version,
        definition: stored.definition,
        description: stored.description,
    })
}

// ---------------------------------------------------------------------------
// Diff
// ---------------------------------------------------------------------------

/// Computes a flat key-level diff between two schema versions via the wire
/// transport.
///
/// Mirrors [`LedgerClient::diff_schemas`](crate::LedgerClient::diff_schemas).
/// Reads both version bodies, then runs a top-level field comparison
/// classifying each key as `added`, `removed`, or `changed`. The diff is
/// shallow on purpose — full structural diff is left to higher-level
/// tooling.
///
/// # Errors
///
/// - [`SdkError::Rpc`] with [`ErrorCode::NotFound`](inferadb_ledger_wire::ErrorCode::NotFound) when
///   either version body is missing.
/// - [`SdkError::Validation`] when stored bytes are corrupt.
/// - All errors raised by the underlying [`super::data::read`] path.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn diff_schemas(
    wire_client: Arc<WireClient>,
    request_id_seed: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    vault: VaultSlug,
    from: u32,
    to: u32,
) -> Result<Vec<SchemaDiffChange>> {
    let mut next_id = request_id_seed;

    let from_schema = read_stored_schema(
        Arc::clone(&wire_client),
        consume(&mut next_id),
        caller,
        organization,
        vault,
        from,
    )
    .await?
    .ok_or_else(|| not_found(format!("schema version {from} not found")))?;

    let to_schema =
        read_stored_schema(wire_client, consume(&mut next_id), caller, organization, vault, to)
            .await?
            .ok_or_else(|| not_found(format!("schema version {to} not found")))?;

    Ok(diff_json_objects(&from_schema.definition, &to_schema.definition))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Consume the current request_id seed and advance to the next.
///
/// The caller's retry loop rotates `request_id_seed` across attempts; each
/// sub-RPC within a single attempt walks forward by `wrapping_add(1)` so the
/// frame correlation IDs stay distinct without coordinating per-call IDs at
/// the entry point. `wrapping_add` is fine — even at 1Bn ops/s a u128 takes
/// 1e22 years to wrap.
fn consume(seed: &mut u128) -> u128 {
    let id = *seed;
    *seed = seed.wrapping_add(1);
    id
}

/// Generate a fresh 16-byte idempotency key for a sub-write.
///
/// The schema op is the unit of logical idempotency from the consumer's
/// perspective — each sub-write within a schema op gets its own key so
/// retries at the inner-write layer (F.1) preserve commit-once semantics
/// without risking a write under one schema op being mistaken for a
/// duplicate of an earlier op's write.
fn fresh_idempotency_key() -> [u8; 16] {
    *uuid::Uuid::new_v4().as_bytes()
}

/// Build a `NotFound` `SdkError::Rpc`.
fn not_found(message: String) -> SdkError {
    SdkError::Rpc {
        code: inferadb_ledger_wire::ErrorCode::NotFound,
        message,
        request_id: None,
        trace_id: None,
        error_details: None,
    }
}

async fn read_version_pointer(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
) -> Result<Option<VersionPointer>> {
    let bytes = data::read(
        wire_client,
        request_id,
        caller,
        organization,
        Some(vault),
        key.to_owned(),
        ReadConsistency::default(),
    )
    .await?;
    match bytes {
        Some(b) => {
            let pointer: VersionPointer = serde_json::from_slice(&b).map_err(|e| {
                SdkError::Validation { message: format!("corrupt version pointer at {key}: {e}") }
            })?;
            Ok(Some(pointer))
        },
        None => Ok(None),
    }
}

async fn read_stored_schema(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    vault: VaultSlug,
    version: u32,
) -> Result<Option<StoredSchema>> {
    let key = schema_version_key(version);
    let bytes = data::read(
        wire_client,
        request_id,
        caller,
        organization,
        Some(vault),
        key.clone(),
        ReadConsistency::default(),
    )
    .await?;
    match bytes {
        Some(b) => {
            let schema: StoredSchema = serde_json::from_slice(&b).map_err(|e| {
                SdkError::Validation { message: format!("corrupt schema at {key}: {e}") }
            })?;
            Ok(Some(schema))
        },
        None => Ok(None),
    }
}

async fn read_history(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    vault: VaultSlug,
) -> Result<Vec<u32>> {
    let bytes = data::read(
        wire_client,
        request_id,
        caller,
        organization,
        Some(vault),
        SCHEMA_HISTORY_KEY.to_owned(),
        ReadConsistency::default(),
    )
    .await?;
    match bytes {
        Some(b) => {
            let history: Vec<u32> = serde_json::from_slice(&b).map_err(|e| {
                SdkError::Validation { message: format!("corrupt schema history: {e}") }
            })?;
            Ok(history)
        },
        None => Ok(Vec::new()),
    }
}

/// Computes flat key-level differences between two JSON objects.
///
/// Pure function — mirrors `ops::schema::diff_json_objects` so both surfaces
/// produce identical output. A non-object on either side collapses to an
/// empty map (keys are added or removed against the empty side).
fn diff_json_objects(from: &serde_json::Value, to: &serde_json::Value) -> Vec<SchemaDiffChange> {
    let empty = serde_json::Map::new();
    let from_obj = from.as_object().unwrap_or(&empty);
    let to_obj = to.as_object().unwrap_or(&empty);

    let mut changes = Vec::new();

    for key in from_obj.keys() {
        if !to_obj.contains_key(key) {
            changes
                .push(SchemaDiffChange { field: key.clone(), change_type: "removed".to_owned() });
        } else if from_obj.get(key) != to_obj.get(key) {
            changes
                .push(SchemaDiffChange { field: key.clone(), change_type: "changed".to_owned() });
        }
    }

    for key in to_obj.keys() {
        if !from_obj.contains_key(key) {
            changes.push(SchemaDiffChange { field: key.clone(), change_type: "added".to_owned() });
        }
    }

    changes.sort_by(|a, b| a.field.cmp(&b.field));
    changes
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    //! Unit coverage for the schema-op JSON envelope discipline, key-prefix
    //! constants, request_id seed walker, and the flat diff helper. The
    //! seven entry-point fns themselves are not exercised yet — F.1 wires
    //! them up against a real `WireClient` / `WireServer` and E.7
    //! (TestCluster migration) provides end-to-end coverage at that point.

    use serde_json::json;

    use super::*;

    // -------------------------------------------------------------------
    // Key-prefix discipline.
    // -------------------------------------------------------------------

    #[test]
    fn schema_version_key_renders_decimal_version() {
        // The prefix must stay `schema:v{n}` literally — the listing path
        // (`list_schema_versions`) and the activate-version-exists check
        // both reconstruct this key from the version u32. A drift here
        // would silently make every version slot appear empty.
        assert_eq!(schema_version_key(1), "schema:v1");
        assert_eq!(schema_version_key(42), "schema:v42");
        assert_eq!(schema_version_key(u32::MAX), format!("schema:v{}", u32::MAX));
    }

    #[test]
    fn well_known_keys_match_tonic_surface() {
        // Pin the literal strings — both the tonic and wire surfaces
        // write to the same key prefixes so a vault populated through
        // one transport reads back through the other. A typo here is a
        // silent cross-transport corruption.
        assert_eq!(SCHEMA_CURRENT_KEY, "schema:current");
        assert_eq!(SCHEMA_LATEST_KEY, "schema:latest");
        assert_eq!(SCHEMA_HISTORY_KEY, "schema:history");
    }

    // -------------------------------------------------------------------
    // request_id seed walker.
    // -------------------------------------------------------------------

    #[test]
    fn consume_returns_seed_then_advances_by_one() {
        let mut seed = 100u128;
        assert_eq!(consume(&mut seed), 100);
        assert_eq!(consume(&mut seed), 101);
        assert_eq!(consume(&mut seed), 102);
        assert_eq!(seed, 103);
    }

    #[test]
    fn consume_wraps_at_u128_max() {
        // u128 wrapping at MAX is part of the contract — the schema op
        // never depends on monotonicity, just distinctness within a
        // bounded window. The seed walker must not panic on overflow.
        let mut seed = u128::MAX;
        assert_eq!(consume(&mut seed), u128::MAX);
        assert_eq!(consume(&mut seed), 0);
        assert_eq!(consume(&mut seed), 1);
    }

    // -------------------------------------------------------------------
    // Idempotency key generation.
    // -------------------------------------------------------------------

    #[test]
    fn fresh_idempotency_key_is_sixteen_bytes_and_unique() {
        // UUID v4 gives 122 bits of entropy — collision across two
        // adjacent calls is astronomically unlikely. If this test ever
        // flakes, the RNG is broken.
        let a = fresh_idempotency_key();
        let b = fresh_idempotency_key();
        assert_eq!(a.len(), 16);
        assert_eq!(b.len(), 16);
        assert_ne!(a, b);
    }

    // -------------------------------------------------------------------
    // StoredSchema JSON envelope round-trip.
    // -------------------------------------------------------------------

    #[test]
    fn stored_schema_round_trips_with_description() {
        let stored = StoredSchema {
            definition: json!({"type": "object", "properties": {"name": {"type": "string"}}}),
            description: Some("v1 release".to_owned()),
        };
        let bytes = serde_json::to_vec(&stored).unwrap();
        let decoded: StoredSchema = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.definition, stored.definition);
        assert_eq!(decoded.description.as_deref(), Some("v1 release"));
    }

    #[test]
    fn stored_schema_round_trips_without_description() {
        // The `skip_serializing_if = "Option::is_none"` attribute drops
        // the field on the wire when None; a vault populated by the
        // tonic path produces bytes without the field, and the wire
        // path must still decode them.
        let stored = StoredSchema { definition: json!({"type": "string"}), description: None };
        let bytes = serde_json::to_vec(&stored).unwrap();
        let decoded: StoredSchema = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.definition, stored.definition);
        assert!(decoded.description.is_none());
    }

    #[test]
    fn stored_schema_decodes_legacy_payload_without_description_field() {
        // A blob written by the tonic surface predating the optional
        // description field must still decode — the field is `Option`
        // with skip-on-none, so a missing key is equivalent to None.
        let bytes = br#"{"definition":{"type":"number"}}"#;
        let decoded: StoredSchema = serde_json::from_slice(bytes).unwrap();
        assert_eq!(decoded.definition, json!({"type": "number"}));
        assert!(decoded.description.is_none());
    }

    // -------------------------------------------------------------------
    // VersionPointer JSON envelope round-trip.
    // -------------------------------------------------------------------

    #[test]
    fn version_pointer_round_trips() {
        let pointer = VersionPointer { version: 7 };
        let bytes = serde_json::to_vec(&pointer).unwrap();
        let decoded: VersionPointer = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.version, 7);
    }

    #[test]
    fn version_pointer_decode_rejects_corrupt_payload() {
        // A non-JSON payload at `schema:current` / `schema:latest` is a
        // vault-corruption signal — the surrounding helper surfaces it
        // as `SdkError::Validation`, which depends on serde returning
        // an error here rather than a default-zero pointer.
        let result: serde_json::Result<VersionPointer> = serde_json::from_slice(b"not json");
        assert!(result.is_err());
    }

    // -------------------------------------------------------------------
    // History JSON envelope round-trip.
    // -------------------------------------------------------------------

    #[test]
    fn history_round_trips_as_json_array() {
        let history: Vec<u32> = vec![1, 2, 3, 5];
        let bytes = serde_json::to_vec(&history).unwrap();
        let decoded: Vec<u32> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, history);
    }

    #[test]
    fn empty_history_round_trips_as_empty_array() {
        // A vault that has never activated a schema returns an empty
        // history; rollback against it must surface the
        // `SdkError::Validation` "no activation history" path, which
        // depends on this round-trip producing an empty Vec rather
        // than failing to deserialize.
        let history: Vec<u32> = Vec::new();
        let bytes = serde_json::to_vec(&history).unwrap();
        let decoded: Vec<u32> = serde_json::from_slice(&bytes).unwrap();
        assert!(decoded.is_empty());
    }

    // -------------------------------------------------------------------
    // diff_json_objects: shallow field-level diff helper.
    // -------------------------------------------------------------------

    #[test]
    fn diff_json_empty_when_objects_are_equal() {
        let a = json!({"a": 1, "b": "two"});
        let b = json!({"a": 1, "b": "two"});
        assert!(diff_json_objects(&a, &b).is_empty());
    }

    #[test]
    fn diff_json_classifies_added_removed_changed() {
        // Every change-type bucket must surface; the helper sorts by
        // field name so the assertion order is deterministic. The
        // domain `SchemaDiffChange` is not `PartialEq`, so this test
        // walks fields manually rather than `assert_eq!`-ing on a Vec.
        let from = json!({"keep": 1, "drop": 2, "change": "old"});
        let to = json!({"keep": 1, "change": "new", "added": 42});
        let changes = diff_json_objects(&from, &to);
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].field, "added");
        assert_eq!(changes[0].change_type, "added");
        assert_eq!(changes[1].field, "change");
        assert_eq!(changes[1].change_type, "changed");
        assert_eq!(changes[2].field, "drop");
        assert_eq!(changes[2].change_type, "removed");
    }

    #[test]
    fn diff_json_treats_non_object_input_as_empty_map() {
        // A non-object on either side collapses to an empty map, which
        // means every key on the OTHER side surfaces as added/removed.
        // This matches the tonic shim — both surfaces must produce the
        // same diff output for the same inputs.
        let from = json!("string-not-object");
        let to = json!({"a": 1, "b": 2});
        let mut changes = diff_json_objects(&from, &to);
        changes.sort_by(|a, b| a.field.cmp(&b.field));
        assert_eq!(changes.len(), 2);
        for change in &changes {
            assert_eq!(change.change_type, "added");
        }
    }

    #[test]
    fn diff_json_distinct_change_types_for_value_only_change() {
        // Same field, same type, different value → "changed" rather
        // than added+removed. This is the path that tooling uses to
        // detect compatible schema evolutions vs. structural rewrites.
        let from = json!({"a": 1});
        let to = json!({"a": 2});
        let changes = diff_json_objects(&from, &to);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].field, "a");
        assert_eq!(changes[0].change_type, "changed");
    }

    #[test]
    fn diff_json_output_is_sorted_by_field_name() {
        // The helper sorts by field for deterministic test output and
        // stable diff snapshots in tooling consumers — regression here
        // would manifest as flaky test runs in downstream consumers.
        let from = json!({"zzz": 1, "aaa": 1, "mmm": 1});
        let to = json!({});
        let changes = diff_json_objects(&from, &to);
        let fields: Vec<&str> = changes.iter().map(|c| c.field.as_str()).collect();
        assert_eq!(fields, vec!["aaa", "mmm", "zzz"]);
    }

    // -------------------------------------------------------------------
    // not_found constructor matches the tonic shim's shape.
    // -------------------------------------------------------------------

    #[test]
    fn not_found_yields_sdk_error_rpc_with_not_found_code() {
        // Pin the variant + code shape so consumers can downcast on
        // `SdkError::Rpc { code: NotFound, .. }` consistently across
        // both the tonic and wire surfaces.
        let err = not_found("schema version 99 not found".to_owned());
        match err {
            SdkError::Rpc { code, message, request_id, trace_id, error_details } => {
                assert_eq!(code, inferadb_ledger_wire::ErrorCode::NotFound);
                assert_eq!(message, "schema version 99 not found");
                assert!(request_id.is_none());
                assert!(trace_id.is_none());
                assert!(error_details.is_none());
            },
            other => panic!("expected Rpc, got {other:?}"),
        }
    }
}
