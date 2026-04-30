//! Wire-based verified read op.
//!
//! Mirrors [`super::super::ops::verified_read`]: same returned domain type
//! ([`VerifiedValue`](crate::VerifiedValue)), same `Result<_, SdkError>`
//! shape on the consumer side. Internal dispatch goes through the
//! wire-services-generated
//! [`ReadServiceClient`](inferadb_ledger_wire_services::ReadServiceClient)
//! instead of the proto-generated tonic client.
//!
//! Verification semantics live entirely client-side: the server returns the
//! `BlockHeader` carrying the state root plus the `MerkleProof` linking the
//! key/value pair to that root, and the SDK consumer calls
//! [`VerifiedValue::verify`](crate::VerifiedValue::verify) on the result.
//! The wire path performs no verification of its own — it only translates
//! the wire response into the domain type that exposes `verify()`.
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests in this file cover the
//! wire-response → domain-type mapping (`BlockHeader`, `MerkleProof`,
//! `ChainProof`, `Direction`) and the not-found short-circuit. The entry
//! point itself is not exercised yet — [`LedgerClient`](crate::LedgerClient)
//! keeps dispatching through the tonic `ops::verified_read` path until F.1
//! flips the connection pool.

use std::sync::Arc;

use inferadb_ledger_wire::services::{read as wr, shared as ws};
use inferadb_ledger_wire_services::ReadServiceClient;
use inferadb_ledger_wire_transport::WireClient;

use crate::{
    error::Result,
    ops_wire::rpc_error_to_sdk_error,
    types::{
        query::VerifiedValue,
        verified_read::{
            BlockHeader, ChainProof, Direction, MerkleProof, MerkleSibling, VerifyOpts,
        },
    },
};

/// Issue a `VerifiedRead` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::verified_read`](crate::LedgerClient::verified_read)
/// at the dispatch layer. Returns `Ok(Some(`[`VerifiedValue`]`))` when the
/// key exists (carrying the value, block header, and Merkle proof — plus an
/// optional chain proof when [`VerifyOpts::with_chain_proof`] was used) or
/// `Ok(None)` when the key was not found at the requested height.
///
/// `request_id` is the 128-bit frame-header correlation ID; the SDK's retry
/// loop is responsible for rotating it across attempts.
///
/// The SDK does **not** verify the proof here — the caller invokes
/// [`VerifiedValue::verify`](crate::VerifiedValue::verify) on the returned
/// value to check that the Merkle path matches the block header's state
/// root. Returning the unverified [`VerifiedValue`] mirrors the proto-side
/// `ops::verified_read` contract exactly.
///
/// # Errors
///
/// Returns [`SdkError::Connection`](crate::SdkError::Connection) for
/// transport / protocol failures and the appropriate
/// [`SdkError`](crate::SdkError) variant for server-returned
/// [`WireError`](inferadb_ledger_wire::error::WireError)s.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn verified_read(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: ws::UserSlug,
    organization: ws::OrganizationSlug,
    vault: Option<ws::VaultSlug>,
    key: String,
    opts: VerifyOpts,
) -> Result<Option<VerifiedValue>> {
    let client = ReadServiceClient::new(wire_client);
    let request = wr::VerifiedReadRequest {
        organization: Some(organization),
        vault,
        key,
        at_height: opts.at_height,
        include_chain_proof: opts.include_chain_proof,
        trusted_height: opts.trusted_height,
        caller: Some(caller),
    };
    let response =
        client.verified_read(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(domain_verified_value_from_wire(response))
}

// ---------------------------------------------------------------------------
// Helpers — wire → domain conversion.
//
// These mirror the proto-side `BlockHeader::from_proto`,
// `MerkleProof::from_proto`, `MerkleSibling::from_proto`, `Direction::from_proto`,
// `ChainProof::from_proto`, and `VerifiedValue::from_proto` in
// `crates/sdk/src/types/verified_read.rs` and `crates/sdk/src/types/query.rs`.
// They live in this file rather than on the domain types to keep the wire-side
// import surface contained — the domain types remain proto-only until F.1
// removes the proto path entirely.
// ---------------------------------------------------------------------------

/// Map a wire [`wr::VerifiedReadResponse`] into the SDK's
/// `Option<VerifiedValue>`.
///
/// Mirrors the proto-side `VerifiedValue::from_proto`. A response with both
/// `value` and `block_header` absent encodes "key not found at the requested
/// height" — the proto path also collapses this case to `None`. Otherwise
/// the block header and Merkle proof are required (their absence is a
/// server-side bug; the proto path uses `?` propagation against the same
/// `Option`s, which lands on `None` here too — same fail-soft shape).
fn domain_verified_value_from_wire(resp: wr::VerifiedReadResponse) -> Option<VerifiedValue> {
    // Not-found short-circuit: proto path checks
    // `response.value.is_none() && response.block_header.is_none()`. Mirror
    // it exactly.
    if resp.value.is_none() && resp.block_header.is_none() {
        return None;
    }

    let block_header = resp.block_header.map(domain_block_header_from_wire)?;
    let merkle_proof = resp.merkle_proof.map(domain_merkle_proof_from_wire)?;

    Some(VerifiedValue {
        value: resp.value.map(|b| b.to_vec()),
        block_height: resp.block_height,
        block_header,
        merkle_proof,
        chain_proof: resp.chain_proof.map(domain_chain_proof_from_wire),
    })
}

/// Map a wire [`ws::BlockHeader`] into the SDK's domain [`BlockHeader`].
///
/// Mirrors the proto-side `BlockHeader::from_proto`. The wire `timestamp`
/// is `u64` UNIX nanoseconds — `0` collapses to `None` (matches the proto
/// path's "absent `Timestamp` → `None`"). All `Option<Hash>` fields default
/// to an empty `Vec<u8>` when absent, mirroring the proto path's
/// `unwrap_or_default()` shape.
fn domain_block_header_from_wire(h: ws::BlockHeader) -> BlockHeader {
    BlockHeader {
        height: h.height,
        organization: ws::OrganizationSlug::new(h.organization.map_or(0, |o| o.value())),
        vault: ws::VaultSlug::new(h.vault.map_or(0, |v| v.value())),
        previous_hash: h.previous_hash.map(|h| h.value.to_vec()).unwrap_or_default(),
        tx_merkle_root: h.tx_merkle_root.map(|h| h.value.to_vec()).unwrap_or_default(),
        state_root: h.state_root.map(|h| h.value.to_vec()).unwrap_or_default(),
        timestamp: unix_nanos_to_system_time(h.timestamp),
        // `NodeId` is a string newtype; the proto path uses
        // `leader_id.map(|n| n.id).unwrap_or_default()`. Mirror by pulling
        // the inner string out and defaulting to "".
        leader_id: h.leader_id.map(|n| n.to_string()).unwrap_or_default(),
        term: h.term,
        committed_index: h.committed_index,
        block_hash: h.block_hash.map(|h| h.value.to_vec()).unwrap_or_default(),
    }
}

/// Map a wire [`ws::MerkleProof`] into the SDK's domain [`MerkleProof`].
///
/// Mirrors `MerkleProof::from_proto`. Absent `leaf_hash` defaults to an
/// empty `Vec<u8>` — same fail-soft shape as the proto path.
fn domain_merkle_proof_from_wire(p: ws::MerkleProof) -> MerkleProof {
    MerkleProof {
        leaf_hash: p.leaf_hash.map(|h| h.value.to_vec()).unwrap_or_default(),
        siblings: p.siblings.into_iter().map(domain_merkle_sibling_from_wire).collect(),
    }
}

/// Map a wire [`ws::MerkleSibling`] into the SDK's domain [`MerkleSibling`].
fn domain_merkle_sibling_from_wire(s: ws::MerkleSibling) -> MerkleSibling {
    MerkleSibling {
        hash: s.hash.map(|h| h.value.to_vec()).unwrap_or_default(),
        direction: domain_direction_from_wire(s.direction),
    }
}

/// Map the wire [`ws::Direction`] enum into the SDK's domain [`Direction`].
///
/// Wire `Unspecified` / `Right` / any-future-variant collapses to `Right` —
/// matches the proto path's "default to right for unspecified" fallback.
fn domain_direction_from_wire(d: ws::Direction) -> Direction {
    match d {
        ws::Direction::Left => Direction::Left,
        // Both `Right` and `Unspecified` map to `Right`; the proto path
        // does the same via the `_ => Direction::Right` arm.
        _ => Direction::Right,
    }
}

/// Map a wire [`ws::ChainProof`] into the SDK's domain [`ChainProof`].
fn domain_chain_proof_from_wire(c: ws::ChainProof) -> ChainProof {
    ChainProof { headers: c.headers.into_iter().map(domain_block_header_from_wire).collect() }
}

/// Translate a UNIX-nanoseconds timestamp into `Option<SystemTime>`.
///
/// `0` collapses to `None` — matches the proto path's "absent `Timestamp`
/// produces `None`" shape (`proto_timestamp_to_system_time` returns `None`
/// when the proto field is `Option::None`). Overflow is also `None`.
///
/// Duplicates the helper in `ops_wire::app` rather than re-exporting it —
/// each ops_wire file keeps its own helper to avoid coupling the modules
/// during the migration. F.1 may consolidate.
fn unix_nanos_to_system_time(nanos: u64) -> Option<std::time::SystemTime> {
    if nanos == 0 {
        return None;
    }
    let secs = nanos / 1_000_000_000;
    let sub_nanos = u32::try_from(nanos % 1_000_000_000).ok()?;
    std::time::UNIX_EPOCH.checked_add(std::time::Duration::new(secs, sub_nanos))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    //! Unit coverage for the wire-response → domain-type mapping. End-to-end
    //! tests against a real `WireClient` / `WireServer` are deferred to E.7
    //! (TestCluster migration) — at that point the entry point gets
    //! exercised in full.

    use std::time::{Duration, UNIX_EPOCH};

    use bytes::Bytes;

    use super::*;

    // -------------------------------------------------------------------
    // Test helpers.
    // -------------------------------------------------------------------

    fn hash(byte: u8) -> ws::Hash {
        ws::Hash { value: Bytes::copy_from_slice(&[byte; 32]) }
    }

    fn sample_block_header(byte_seed: u8) -> ws::BlockHeader {
        ws::BlockHeader {
            height: 42,
            organization: Some(ws::OrganizationSlug::new(7)),
            vault: Some(ws::VaultSlug::new(11)),
            previous_hash: Some(hash(byte_seed.wrapping_sub(1))),
            tx_merkle_root: Some(hash(byte_seed.wrapping_add(1))),
            state_root: Some(hash(byte_seed)),
            timestamp: 1_700_000_000_000_000_000,
            leader_id: Some(ws::NodeId::new("node-1")),
            term: 5,
            committed_index: 100,
            block_hash: Some(hash(byte_seed.wrapping_add(2))),
        }
    }

    fn sample_merkle_proof() -> ws::MerkleProof {
        ws::MerkleProof {
            leaf_hash: Some(hash(0xAA)),
            siblings: vec![
                ws::MerkleSibling { hash: Some(hash(0xBB)), direction: ws::Direction::Right },
                ws::MerkleSibling { hash: Some(hash(0xCC)), direction: ws::Direction::Left },
            ],
        }
    }

    // -------------------------------------------------------------------
    // Round-trip: VerifiedReadRequest / VerifiedReadResponse via postcard.
    // -------------------------------------------------------------------

    #[test]
    fn verified_read_request_postcard_round_trips() {
        // Constructed with the same field shape the production fn emits;
        // confirms the wire encoding is self-inverse.
        let req = wr::VerifiedReadRequest {
            organization: Some(ws::OrganizationSlug::new(7)),
            vault: Some(ws::VaultSlug::new(11)),
            key: "user:42".to_owned(),
            at_height: Some(100),
            include_chain_proof: true,
            trusted_height: Some(50),
            caller: Some(ws::UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: wr::VerifiedReadRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn verified_read_request_with_defaults_round_trips() {
        // `VerifyOpts::default()` produces all-None / false; confirm the
        // request projects through cleanly without any oneof tag drift.
        let req = wr::VerifiedReadRequest {
            organization: Some(ws::OrganizationSlug::new(1)),
            vault: None,
            key: "k".to_owned(),
            at_height: None,
            include_chain_proof: false,
            trusted_height: None,
            caller: Some(ws::UserSlug::new(2)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: wr::VerifiedReadRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn verified_read_response_full_round_trips() {
        let resp = wr::VerifiedReadResponse {
            value: Some(Bytes::copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF])),
            block_height: 42,
            block_header: Some(sample_block_header(0x01)),
            merkle_proof: Some(sample_merkle_proof()),
            chain_proof: Some(ws::ChainProof {
                headers: vec![sample_block_header(0x10), sample_block_header(0x20)],
            }),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: wr::VerifiedReadResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    // -------------------------------------------------------------------
    // Direction enum mapping.
    // -------------------------------------------------------------------

    #[test]
    fn domain_direction_each_variant_maps_correctly() {
        // Pin the per-variant mapping. Wire `Left` → domain `Left`;
        // wire `Right` → domain `Right`; wire `Unspecified` collapses
        // to `Right` to match the proto path's fallback ("default to
        // right for unspecified").
        assert_eq!(domain_direction_from_wire(ws::Direction::Left), Direction::Left);
        assert_eq!(domain_direction_from_wire(ws::Direction::Right), Direction::Right);
        assert_eq!(domain_direction_from_wire(ws::Direction::Unspecified), Direction::Right);
    }

    #[test]
    fn direction_repr_values_match_proto() {
        // Lock in the integer tags so a cross-protocol audit (proto vs
        // wire) can confirm the repr layout is identical.
        assert_eq!(ws::Direction::Unspecified as i32, 0);
        assert_eq!(ws::Direction::Left as i32, 1);
        assert_eq!(ws::Direction::Right as i32, 2);
    }

    // -------------------------------------------------------------------
    // unix_nanos_to_system_time edge cases.
    // -------------------------------------------------------------------

    #[test]
    fn unix_nanos_to_system_time_zero_is_none() {
        // `0` is the wire encoding for "no timestamp" — must collapse to
        // `None` to mirror the proto path's absent-`Timestamp` behavior.
        assert!(unix_nanos_to_system_time(0).is_none());
    }

    #[test]
    fn unix_nanos_to_system_time_handles_subsecond_precision() {
        let t = unix_nanos_to_system_time(1_500_000_000).expect("translates");
        assert_eq!(t, UNIX_EPOCH + Duration::new(1, 500_000_000));
    }

    #[test]
    fn unix_nanos_to_system_time_handles_recent_realistic_value() {
        let nanos: u64 = 1_700_000_000_000_000_000;
        let t = unix_nanos_to_system_time(nanos).expect("translates");
        assert_eq!(t, UNIX_EPOCH + Duration::from_nanos(nanos));
    }

    // -------------------------------------------------------------------
    // BlockHeader mapping.
    // -------------------------------------------------------------------

    #[test]
    fn domain_block_header_from_wire_carries_all_fields() {
        let wire = sample_block_header(0x05);
        let domain = domain_block_header_from_wire(wire);

        assert_eq!(domain.height, 42);
        assert_eq!(domain.organization.value(), 7);
        assert_eq!(domain.vault.value(), 11);
        assert_eq!(domain.previous_hash, vec![0x04u8; 32]);
        assert_eq!(domain.tx_merkle_root, vec![0x06u8; 32]);
        assert_eq!(domain.state_root, vec![0x05u8; 32]);
        assert_eq!(
            domain.timestamp,
            Some(UNIX_EPOCH + Duration::from_nanos(1_700_000_000_000_000_000))
        );
        assert_eq!(domain.leader_id, "node-1");
        assert_eq!(domain.term, 5);
        assert_eq!(domain.committed_index, 100);
        assert_eq!(domain.block_hash, vec![0x07u8; 32]);
    }

    #[test]
    fn domain_block_header_from_wire_handles_absent_fields_with_defaults() {
        // Proto path uses `unwrap_or_default()` for every Option field;
        // mirror exactly. Slug fields default to 0 (system-level header),
        // hashes to empty Vec, leader_id to "", timestamp (== 0) to None.
        let wire = ws::BlockHeader {
            height: 0,
            organization: None,
            vault: None,
            previous_hash: None,
            tx_merkle_root: None,
            state_root: None,
            timestamp: 0,
            leader_id: None,
            term: 0,
            committed_index: 0,
            block_hash: None,
        };
        let domain = domain_block_header_from_wire(wire);
        assert_eq!(domain.height, 0);
        assert_eq!(domain.organization.value(), 0);
        assert_eq!(domain.vault.value(), 0);
        assert!(domain.previous_hash.is_empty());
        assert!(domain.tx_merkle_root.is_empty());
        assert!(domain.state_root.is_empty());
        assert_eq!(domain.timestamp, None);
        assert_eq!(domain.leader_id, "");
        assert!(domain.block_hash.is_empty());
    }

    // -------------------------------------------------------------------
    // MerkleProof / MerkleSibling mapping.
    // -------------------------------------------------------------------

    #[test]
    fn domain_merkle_proof_from_wire_preserves_sibling_order_and_direction() {
        let wire = sample_merkle_proof();
        let domain = domain_merkle_proof_from_wire(wire);
        assert_eq!(domain.leaf_hash, vec![0xAAu8; 32]);
        assert_eq!(domain.siblings.len(), 2);
        assert_eq!(domain.siblings[0].hash, vec![0xBBu8; 32]);
        assert_eq!(domain.siblings[0].direction, Direction::Right);
        assert_eq!(domain.siblings[1].hash, vec![0xCCu8; 32]);
        assert_eq!(domain.siblings[1].direction, Direction::Left);
    }

    #[test]
    fn domain_merkle_proof_from_wire_handles_empty_siblings() {
        // Single-element tree: leaf hash equals root, no siblings.
        // `verify()` short-circuits to a leaf-hash equality check; the
        // mapping itself must propagate the empty vec cleanly.
        let wire = ws::MerkleProof { leaf_hash: Some(hash(0xFF)), siblings: vec![] };
        let domain = domain_merkle_proof_from_wire(wire);
        assert_eq!(domain.leaf_hash, vec![0xFFu8; 32]);
        assert!(domain.siblings.is_empty());
    }

    #[test]
    fn domain_merkle_proof_from_wire_handles_absent_leaf_hash() {
        // Proto path uses `unwrap_or_default()` for the leaf hash; mirror.
        let wire = ws::MerkleProof { leaf_hash: None, siblings: vec![] };
        let domain = domain_merkle_proof_from_wire(wire);
        assert!(domain.leaf_hash.is_empty());
    }

    // -------------------------------------------------------------------
    // ChainProof mapping.
    // -------------------------------------------------------------------

    #[test]
    fn domain_chain_proof_from_wire_preserves_header_order() {
        let wire =
            ws::ChainProof { headers: vec![sample_block_header(0x10), sample_block_header(0x20)] };
        let domain = domain_chain_proof_from_wire(wire);
        assert_eq!(domain.headers.len(), 2);
        assert_eq!(domain.headers[0].state_root, vec![0x10u8; 32]);
        assert_eq!(domain.headers[1].state_root, vec![0x20u8; 32]);
    }

    #[test]
    fn domain_chain_proof_from_wire_handles_empty_headers() {
        let wire = ws::ChainProof { headers: vec![] };
        let domain = domain_chain_proof_from_wire(wire);
        assert!(domain.headers.is_empty());
    }

    // -------------------------------------------------------------------
    // VerifiedReadResponse → Option<VerifiedValue> mapping.
    // -------------------------------------------------------------------

    #[test]
    fn verified_read_response_full_maps_to_some_verified_value() {
        let resp = wr::VerifiedReadResponse {
            value: Some(Bytes::copy_from_slice(&[0x01, 0x02, 0x03])),
            block_height: 42,
            block_header: Some(sample_block_header(0x05)),
            merkle_proof: Some(sample_merkle_proof()),
            chain_proof: None,
        };
        let domain = domain_verified_value_from_wire(resp).expect("found");
        assert_eq!(domain.value.as_deref(), Some(&[0x01u8, 0x02, 0x03][..]));
        assert_eq!(domain.block_height, 42);
        assert_eq!(domain.block_header.height, 42);
        assert_eq!(domain.merkle_proof.leaf_hash, vec![0xAAu8; 32]);
        assert!(domain.chain_proof.is_none());
    }

    #[test]
    fn verified_read_response_with_chain_proof_maps_through() {
        let resp = wr::VerifiedReadResponse {
            value: Some(Bytes::from_static(b"v")),
            block_height: 42,
            block_header: Some(sample_block_header(0x05)),
            merkle_proof: Some(sample_merkle_proof()),
            chain_proof: Some(ws::ChainProof { headers: vec![sample_block_header(0x10)] }),
        };
        let domain = domain_verified_value_from_wire(resp).expect("found");
        let chain = domain.chain_proof.expect("chain proof present");
        assert_eq!(chain.headers.len(), 1);
        assert_eq!(chain.headers[0].state_root, vec![0x10u8; 32]);
    }

    #[test]
    fn verified_read_response_not_found_collapses_to_none() {
        // Proto path: `if response.value.is_none() && response.block_header.is_none() { return
        // None; }`. Mirror exactly — both fields absent encodes "key not found at the
        // requested height".
        let resp = wr::VerifiedReadResponse {
            value: None,
            block_height: 0,
            block_header: None,
            merkle_proof: None,
            chain_proof: None,
        };
        assert!(domain_verified_value_from_wire(resp).is_none());
    }

    #[test]
    fn verified_read_response_missing_merkle_proof_collapses_to_none() {
        // Proto path uses `?` propagation against `Option<MerkleProof>` —
        // a server response with the header set but the proof absent
        // lands on `None`. Mirror exactly so a server-side bug doesn't
        // surface as a panicking unwrap.
        let resp = wr::VerifiedReadResponse {
            value: Some(Bytes::from_static(b"v")),
            block_height: 42,
            block_header: Some(sample_block_header(0x05)),
            merkle_proof: None,
            chain_proof: None,
        };
        assert!(domain_verified_value_from_wire(resp).is_none());
    }

    #[test]
    fn verified_read_response_missing_block_header_with_value_collapses_to_none() {
        // Header is required for verification (it carries the state root);
        // proto path's `?` against `Option<BlockHeader>` lands on `None`
        // when the header is absent even if a value is present.
        let resp = wr::VerifiedReadResponse {
            value: Some(Bytes::from_static(b"v")),
            block_height: 42,
            block_header: None,
            merkle_proof: Some(sample_merkle_proof()),
            chain_proof: None,
        };
        assert!(domain_verified_value_from_wire(resp).is_none());
    }

    #[test]
    fn verified_read_response_value_none_with_header_present_returns_some() {
        // Edge case: server returns value=None but header+proof present —
        // this is the "tombstone" / "key existed but is now deleted" path.
        // Proto path: only collapses to `None` when BOTH value and header
        // are absent; if header is present the result is `Some` with
        // `value: None`.
        let resp = wr::VerifiedReadResponse {
            value: None,
            block_height: 42,
            block_header: Some(sample_block_header(0x05)),
            merkle_proof: Some(sample_merkle_proof()),
            chain_proof: None,
        };
        let domain = domain_verified_value_from_wire(resp).expect("present");
        assert!(domain.value.is_none());
        assert_eq!(domain.block_height, 42);
    }
}
