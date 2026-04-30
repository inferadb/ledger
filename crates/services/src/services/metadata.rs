//! Correlation metadata helpers for gRPC responses.
//!
//! Injects `x-request-id` and `x-trace-id` into response metadata, ensuring
//! that both successful responses and error statuses carry correlation IDs for
//! debugging and SDK error enrichment. Also attaches a JSON-encoded
//! [`WireError`](inferadb_ledger_wire::WireError) payload to error statuses
//! (via [`wire_helpers::wire_error_to_tonic_status`](super::wire_helpers::wire_error_to_tonic_status))
//! for machine-readable error handling on the bridged tonic surface.

use tonic::Status;

/// Builds a `NotLeader` `Status` with leader hints attached as `ErrorDetails`.
///
/// Prefer this over `Status::unavailable(message)` for any not-leader rejection
/// so the client can update its region leader cache directly from the error
/// path, without issuing a separate `ResolveRegionLeader` RPC.
///
/// `leader_vault` is `Some(_)` only for vault-scoped rejections (per-vault
/// Raft groups). Region- and org-scoped rejections pass `None`; the SDK
/// uses the absence of the `leader_vault` key as the signal to fall back
/// to the org-level cache entry.
///
/// `leader_organization_slug` and `leader_vault_slug` carry the external
/// Snowflake slugs (u64) when the rejecting handler had them in scope —
/// vault-scoped gRPC handlers do; raft-internal call sites typically don't.
/// The SDK keys its `VaultLeaderCache` on `(OrganizationSlug, VaultSlug)`,
/// so absent slugs cause the SDK to fall through to the region path
/// (legacy server compat).
#[allow(clippy::too_many_arguments)]
pub(crate) fn status_with_not_leader_hint(
    message: impl Into<String>,
    leader_id: Option<u64>,
    leader_endpoint: Option<&str>,
    leader_term: Option<u64>,
    leader_shard: Option<u64>,
    leader_vault: Option<u64>,
    leader_organization_slug: Option<u64>,
    leader_vault_slug: Option<u64>,
) -> Status {
    use std::collections::BTreeMap;

    use inferadb_ledger_types::DiagnosticCode;
    use inferadb_ledger_wire::ErrorCode;

    let mut context: BTreeMap<String, String> = BTreeMap::new();
    if let Some(id) = leader_id {
        context.insert("leader_id".to_owned(), id.to_string());
    }
    if let Some(ep) = leader_endpoint.filter(|s| !s.is_empty()) {
        context.insert("leader_endpoint".to_owned(), ep.to_owned());
    }
    if let Some(term) = leader_term {
        context.insert("leader_term".to_owned(), term.to_string());
    }
    if let Some(shard) = leader_shard {
        context.insert("leader_shard".to_owned(), shard.to_string());
    }
    if let Some(v) = leader_vault {
        context.insert("leader_vault".to_owned(), v.to_string());
    }
    if let Some(s) = leader_organization_slug {
        context.insert("leader_organization_slug".to_owned(), s.to_string());
    }
    if let Some(s) = leader_vault_slug {
        context.insert("leader_vault_slug".to_owned(), s.to_string());
    }

    let wire_err = super::wire_helpers::build_wire_error(
        ErrorCode::StaleRouting,
        message,
        DiagnosticCode::ConsensusNotLeader.as_u16().to_string(),
        true,
        0,
        context,
        "Retry against the indicated leader; update your region leader cache",
    );
    super::wire_helpers::wire_error_to_tonic_status(wire_err)
}

/// Builds a `NotLeader` `Status` by extracting `(leader_id, leader_endpoint, term)`
/// from a consensus handle and a peer-address map.
///
/// Prefer this over calling [`status_with_not_leader_hint`] directly when the
/// call site has a [`inferadb_ledger_raft::ConsensusHandle`] and peer map in
/// scope — consolidates the leader-state extraction boilerplate so all
/// not-leader rejections populate the same hint shape.
///
/// Region- and org-scoped call sites pass `None` for `leader_vault`. Vault-
/// scoped call sites (per-vault Raft groups) pass `Some(vault_id_as_u64)`
/// so the SDK can key its `VaultLeaderCache` on `(region, organization_id,
/// vault_id)` rather than just `(region, organization_id)`.
///
/// `leader_organization_slug` and `leader_vault_slug` are the external
/// Snowflake slugs (u64). The SDK's `VaultLeaderCache` keys on
/// `(OrganizationSlug, VaultSlug)` — the identifier space the SDK actually
/// has at hand. Vault-scoped gRPC handlers pass them through from the
/// inbound request; raft-internal call sites that lack slug context pass
/// `None`/`None` and the SDK falls through to the region path.
pub(crate) fn not_leader_status_from_handle(
    handle: &inferadb_ledger_raft::ConsensusHandle,
    peer_addresses: Option<&inferadb_ledger_raft::PeerAddressMap>,
    message: impl Into<String>,
    leader_vault: Option<u64>,
    leader_organization_slug: Option<u64>,
    leader_vault_slug: Option<u64>,
) -> Status {
    let shard_state = handle.shard_state();
    let term = handle.current_term();
    let leader_id = shard_state.leader.map(|n| n.0);
    let leader_endpoint =
        leader_id.and_then(|id| peer_addresses.and_then(|m| m.get(id))).map(ensure_endpoint_url);
    // OrganizationId is i64 (Snowflake); the leader-hint wire field is u64.
    // Snowflake IDs are positive so this cast preserves the value.
    let leader_organization = handle.organization_id().value() as u64;
    status_with_not_leader_hint(
        message,
        leader_id,
        leader_endpoint.as_deref(),
        Some(term),
        Some(leader_organization),
        leader_vault,
        leader_organization_slug,
        leader_vault_slug,
    )
}

/// Wire-shaped sibling of [`not_leader_status_from_handle`] returning a
/// [`WireError`](inferadb_ledger_wire::WireError) directly instead of going
/// through `tonic::Status` + `ErrorDetails`.
///
/// The leader-hint context that lived in `ErrorDetails.context` is encoded
/// into [`WireError::context`] using the same key shape (`leader_id`,
/// `leader_endpoint`, `leader_term`, `leader_shard`, `leader_vault`,
/// `leader_organization_slug`, `leader_vault_slug`) — the SDK reads them
/// the same way regardless of which transport produced them.
pub(crate) fn not_leader_wire_error_from_handle(
    handle: &inferadb_ledger_raft::ConsensusHandle,
    peer_addresses: Option<&inferadb_ledger_raft::PeerAddressMap>,
    message: impl Into<String>,
    leader_vault: Option<u64>,
    leader_organization_slug: Option<u64>,
    leader_vault_slug: Option<u64>,
) -> inferadb_ledger_wire::WireError {
    use std::collections::BTreeMap;

    use inferadb_ledger_types::DiagnosticCode;
    use inferadb_ledger_wire::ErrorCode;

    let shard_state = handle.shard_state();
    let term = handle.current_term();
    let leader_id = shard_state.leader.map(|n| n.0);
    let leader_endpoint =
        leader_id.and_then(|id| peer_addresses.and_then(|m| m.get(id))).map(ensure_endpoint_url);
    // OrganizationId is i64 (Snowflake); the leader-hint wire field is u64.
    let leader_organization = handle.organization_id().value() as u64;

    let mut context: BTreeMap<String, String> = BTreeMap::new();
    if let Some(id) = leader_id {
        context.insert("leader_id".to_owned(), id.to_string());
    }
    if let Some(ep) = leader_endpoint.filter(|s| !s.is_empty()) {
        context.insert("leader_endpoint".to_owned(), ep);
    }
    context.insert("leader_term".to_owned(), term.to_string());
    context.insert("leader_shard".to_owned(), leader_organization.to_string());
    if let Some(v) = leader_vault {
        context.insert("leader_vault".to_owned(), v.to_string());
    }
    if let Some(s) = leader_organization_slug {
        context.insert("leader_organization_slug".to_owned(), s.to_string());
    }
    if let Some(s) = leader_vault_slug {
        context.insert("leader_vault_slug".to_owned(), s.to_string());
    }

    super::wire_helpers::build_wire_error(
        ErrorCode::StaleRouting,
        message,
        DiagnosticCode::ConsensusNotLeader.as_u16().to_string(),
        true,
        0,
        context,
        "Retry against the indicated leader; update your region leader cache",
    )
}

/// Wire-shaped sibling of [`not_leader_remote_region`] returning a
/// [`WireError`](inferadb_ledger_wire::WireError) directly.
#[allow(dead_code)]
pub(crate) fn not_leader_wire_error_remote_region(
    redirect: &super::region_resolver::RedirectInfo,
    message: impl Into<String>,
) -> inferadb_ledger_wire::WireError {
    use std::collections::BTreeMap;

    use inferadb_ledger_types::DiagnosticCode;
    use inferadb_ledger_wire::ErrorCode;

    let mut context: BTreeMap<String, String> = BTreeMap::new();
    if let Some(ref hint) = redirect.routing.leader_hint {
        let endpoint = ensure_endpoint_url(hint.clone());
        if !endpoint.is_empty() {
            context.insert("leader_endpoint".to_owned(), endpoint);
        }
    }

    super::wire_helpers::build_wire_error(
        ErrorCode::StaleRouting,
        message,
        DiagnosticCode::ConsensusNotLeader.as_u16().to_string(),
        true,
        0,
        context,
        "Retry against the indicated leader; update your region leader cache",
    )
}

/// Builds a `NotLeader` `Status` for a cross-region redirect, carrying the
/// remote region's leader hint (if known) so the SDK can reconnect directly
/// to the target region's leader.
///
/// Note: `RoutingInfo.leader_hint` is currently always `None` for cross-region
/// redirects — see the doc on [`super::region_resolver::RoutingInfo::leader_hint`]
/// for why. The helper remains correct: passing `None` makes the SDK fall back
/// to `ResolveRegionLeader` / `WatchLeader` on an in-region node.
pub(crate) fn not_leader_remote_region(
    redirect: &super::region_resolver::RedirectInfo,
    message: impl Into<String>,
) -> Status {
    // Cross-region redirects don't include a per-shard hint: the local
    // node has no view into the remote region's per-shard leadership map
    // (each `(region, shard)` is its own Raft group). The SDK's
    // `RegionLeaderCache` falls back to `ResolveRegionLeader` /
    // `WatchLeader` against an in-region node to learn shard leaders.
    // The vault hint is also absent for the same reason.
    status_with_not_leader_hint(
        message,
        None,
        redirect.routing.leader_hint.as_deref(),
        None,
        None,
        None,
        None,
        None,
    )
}

/// Prepends `http://` if the address has no URI scheme.
///
/// Normalizes a peer address into a valid endpoint URL for client connections.
///
/// Peer addresses are stored as bare `host:port` strings or Unix socket paths.
/// Client-facing leader hints must be valid URIs so the SDK can pass them to
/// `tonic::transport::Endpoint`. UDS paths (starting with `/`) and addresses
/// with an explicit scheme are returned as-is; bare `host:port` gets `http://`
/// prepended.
///
/// `pub(super)` so sibling modules (`discovery`, etc.) can share this helper.
pub(super) fn ensure_endpoint_url(addr: String) -> String {
    if addr.starts_with('/') || addr.contains("://") { addr } else { format!("http://{addr}") }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    /// Round-trip a `Status` produced by the metadata helpers back through
    /// `tonic_status_to_wire_error` to recover the JSON-encoded context.
    /// The bridge layer is byte-symmetric on the same hop.
    fn decode_context(status: &Status) -> std::collections::BTreeMap<String, String> {
        super::super::wire_helpers::tonic_status_to_wire_error(status.clone()).context
    }

    #[test]
    fn status_with_not_leader_hint_populates_details() {
        let status = status_with_not_leader_hint(
            "not leader for region us-east-va shard 5 vault 99",
            Some(42),
            Some("http://10.0.2.5:5000"),
            Some(7),
            Some(5),
            Some(99),
            Some(123),
            Some(456),
        );
        assert_eq!(status.code(), tonic::Code::Unavailable);

        let context = decode_context(&status);
        assert_eq!(context.get("leader_id").unwrap(), "42");
        assert_eq!(context.get("leader_endpoint").unwrap(), "http://10.0.2.5:5000");
        assert_eq!(context.get("leader_term").unwrap(), "7");
        assert_eq!(context.get("leader_shard").unwrap(), "5");
        assert_eq!(context.get("leader_vault").unwrap(), "99");
        assert_eq!(context.get("leader_organization_slug").unwrap(), "123");
        assert_eq!(context.get("leader_vault_slug").unwrap(), "456");
    }

    #[test]
    fn status_with_not_leader_hint_omits_vault_when_none() {
        // Region- and org-scoped rejections pass leader_vault = None;
        // the resulting context MUST omit the leader_vault key so the SDK
        // falls back to its org-level cache.
        let status = status_with_not_leader_hint(
            "not leader",
            Some(1),
            None,
            Some(2),
            Some(3),
            None,
            None,
            None,
        );
        let context = decode_context(&status);
        assert!(!context.contains_key("leader_vault"));
        assert!(!context.contains_key("leader_vault_slug"));
        assert_eq!(context.get("leader_shard").unwrap(), "3");
    }

    #[test]
    fn not_leader_remote_region_with_endpoint_hint_populates_details() {
        use inferadb_ledger_types::{OrganizationId, Region};

        use crate::services::region_resolver::{RedirectInfo, RoutingInfo};

        let routing = RoutingInfo {
            region: Region::US_EAST_VA,
            leader_hint: Some("node-1:50051".to_string()),
        };
        let remote = RedirectInfo {
            region: Region::US_EAST_VA,
            organization: OrganizationId::new(42),
            routing,
        };

        let status = not_leader_remote_region(&remote, "remote region");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        let context = decode_context(&status);
        assert_eq!(context.get("leader_endpoint").map(String::as_str), Some("node-1:50051"));
    }

    #[test]
    fn not_leader_remote_region_without_hint_omits_endpoint() {
        use inferadb_ledger_types::{OrganizationId, Region};

        use crate::services::region_resolver::{RedirectInfo, RoutingInfo};

        let routing = RoutingInfo { region: Region::US_EAST_VA, leader_hint: None };
        let remote = RedirectInfo {
            region: Region::US_EAST_VA,
            organization: OrganizationId::new(42),
            routing,
        };

        let status = not_leader_remote_region(&remote, "remote region");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        let context = decode_context(&status);
        assert!(!context.contains_key("leader_endpoint"));
    }
}
