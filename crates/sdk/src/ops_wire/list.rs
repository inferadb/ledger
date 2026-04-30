//! Wire-based list ops (list_entities / list_relationships / list_resources).
//!
//! Mirrors [`super::super::ops::list`]: same returned domain types, same
//! `Result<_, SdkError>` shape on the consumer side. Internal dispatch goes
//! through the wire-services-generated
//! [`ReadServiceClient`](inferadb_ledger_wire_services::ReadServiceClient)
//! instead of the proto-generated tonic client.
//!
//! Scope: only the three list-style read RPCs migrate here. The other
//! entries on `ReadService` (read / batch_read / verified_read /
//! historical_read / get_block / get_block_range / get_tip / watch_blocks)
//! ship in their own E.5.x tasks — splitting keeps each file's surface
//! area small.
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests in this file cover the
//! `ReadConsistency` mapping plus the wire-response → domain-type
//! projection (`Entity` / `Relationship` from wire, `next_page_token`
//! empty-string → `None`) in isolation. The three entry points themselves
//! are not exercised yet — [`LedgerClient`](crate::LedgerClient) keeps
//! dispatching through the tonic `ops::list` path until F.1 flips the
//! connection pool.

use std::sync::Arc;

use inferadb_ledger_wire::services::{read as wr, shared as ws};
use inferadb_ledger_wire_services::ReadServiceClient;
use inferadb_ledger_wire_transport::WireClient;

use crate::{
    error::Result,
    ops_wire::rpc_error_to_sdk_error,
    types::{
        query::{
            Entity, ListEntitiesOpts, ListRelationshipsOpts, ListResourcesOpts, PagedResult,
            Relationship,
        },
        read::ReadConsistency,
    },
};

// ---------------------------------------------------------------------------
// ListEntities
// ---------------------------------------------------------------------------

/// Issue a `ListEntities` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::list_entities`](crate::LedgerClient::list_entities)
/// at the dispatch layer. Returns a [`PagedResult`] of [`Entity`] values
/// matching the `key_prefix` filter in `opts`. The `next_page_token` is
/// surfaced as `Some` only when more pages remain — the wire's
/// empty-string sentinel collapses to `None` here.
///
/// `vault` is `None` for organization-level entities. `request_id` is the
/// 128-bit frame-header correlation ID; the SDK's retry loop is
/// responsible for rotating it across attempts.
///
/// # Errors
///
/// Returns [`SdkError::Connection`](crate::SdkError::Connection) for
/// transport / protocol failures and the appropriate
/// [`SdkError`](crate::SdkError) variant for server-returned
/// [`WireError`](inferadb_ledger_wire::WireError)s (rate-limit family,
/// migration codes, all others land in `Rpc`).
pub(crate) async fn list_entities(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: ws::UserSlug,
    organization: ws::OrganizationSlug,
    opts: ListEntitiesOpts,
) -> Result<PagedResult<Entity>> {
    let client = ReadServiceClient::new(wire_client);
    let request = wr::ListEntitiesRequest {
        organization: Some(organization),
        key_prefix: opts.key_prefix,
        at_height: opts.at_height,
        include_expired: opts.include_expired,
        limit: opts.limit,
        page_token: opts.page_token.unwrap_or_default(),
        consistency: domain_consistency_to_wire(opts.consistency),
        vault: opts.vault,
        caller: Some(caller),
    };
    let response =
        client.list_entities(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(domain_entity_page_from_wire(response))
}

// ---------------------------------------------------------------------------
// ListRelationships
// ---------------------------------------------------------------------------

/// Issue a `ListRelationships` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::list_relationships`](crate::LedgerClient::list_relationships)
/// at the dispatch layer. Returns a [`PagedResult`] of [`Relationship`]
/// tuples matching the optional `resource` / `relation` / `subject`
/// filters in `opts` — omitting a filter matches all values for that
/// field. Unlike `list_entities`, `vault` is required: relationships are
/// always vault-scoped.
///
/// # Errors
///
/// Same shape as [`list_entities`].
pub(crate) async fn list_relationships(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: ws::UserSlug,
    organization: ws::OrganizationSlug,
    vault: ws::VaultSlug,
    opts: ListRelationshipsOpts,
) -> Result<PagedResult<Relationship>> {
    let client = ReadServiceClient::new(wire_client);
    let request = wr::ListRelationshipsRequest {
        organization: Some(organization),
        vault: Some(vault),
        resource: opts.resource,
        relation: opts.relation,
        subject: opts.subject,
        at_height: opts.at_height,
        limit: opts.limit,
        page_token: opts.page_token.unwrap_or_default(),
        consistency: domain_consistency_to_wire(opts.consistency),
        caller: Some(caller),
    };
    let response =
        client.list_relationships(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(domain_relationship_page_from_wire(response))
}

// ---------------------------------------------------------------------------
// ListResources
// ---------------------------------------------------------------------------

/// Issue a `ListResources` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::list_resources`](crate::LedgerClient::list_resources)
/// at the dispatch layer. Returns a [`PagedResult`] of distinct resource
/// identifiers (e.g. `"document:1"`, `"document:2"`) whose type prefix
/// matches `opts.resource_type`. The wire response carries plain
/// `Vec<String>`; no per-item conversion is required.
///
/// # Errors
///
/// Same shape as [`list_entities`].
pub(crate) async fn list_resources(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: ws::UserSlug,
    organization: ws::OrganizationSlug,
    vault: ws::VaultSlug,
    opts: ListResourcesOpts,
) -> Result<PagedResult<String>> {
    let client = ReadServiceClient::new(wire_client);
    let request = wr::ListResourcesRequest {
        organization: Some(organization),
        vault: Some(vault),
        resource_type: opts.resource_type,
        at_height: opts.at_height,
        limit: opts.limit,
        page_token: opts.page_token.unwrap_or_default(),
        consistency: domain_consistency_to_wire(opts.consistency),
        caller: Some(caller),
    };
    let response =
        client.list_resources(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(PagedResult {
        items: response.resources,
        next_page_token: non_empty_token(response.next_page_token),
        block_height: response.block_height,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Map the SDK domain [`ReadConsistency`] to the wire enum.
///
/// The wire enum is `#[repr(i32)]` with the same values as the proto enum;
/// the SDK domain enum has no `Unspecified` variant (Rust `Default` is
/// `Eventual`), so this is a total mapping with no fallback arm. Mirrors
/// the same helper in [`super::data`] — kept private here so each
/// ops_wire file owns its own conversion surface.
fn domain_consistency_to_wire(c: ReadConsistency) -> ws::ReadConsistency {
    match c {
        ReadConsistency::Eventual => ws::ReadConsistency::Eventual,
        ReadConsistency::Linearizable => ws::ReadConsistency::Linearizable,
    }
}

/// Convert a wire [`ws::Entity`] into the SDK domain [`Entity`].
///
/// Mirrors [`Entity::from_proto`] — the wire `value` field is `Bytes` (vs
/// proto's `Vec<u8>`), so we materialize it via `to_vec()`. `expires_at`
/// preserves the proto-side filter that treats `Some(0)` as
/// "no expiration" (collapses to `None`).
fn entity_from_wire(entity: ws::Entity) -> Entity {
    Entity {
        key: entity.key,
        value: entity.value.to_vec(),
        expires_at: entity.expires_at.filter(|&ts| ts > 0),
        version: entity.version,
    }
}

/// Convert a wire [`ws::Relationship`] into the SDK domain [`Relationship`].
///
/// Both shapes carry the same `(resource, relation, subject)` triple as
/// `String`s; the conversion is field-for-field.
fn relationship_from_wire(rel: ws::Relationship) -> Relationship {
    Relationship { resource: rel.resource, relation: rel.relation, subject: rel.subject }
}

/// Map the wire `next_page_token: String` field through the SDK's
/// `Option<String>` convention.
///
/// The wire type uses an empty string to mean "no more pages" (mirrors
/// the proto field). The SDK's [`PagedResult::next_page_token`] uses
/// `Option<String>::None` for the same signal so callers can use
/// [`PagedResult::has_next_page`] cleanly.
fn non_empty_token(token: String) -> Option<String> {
    if token.is_empty() { None } else { Some(token) }
}

/// Project a wire [`wr::ListEntitiesResponse`] onto the SDK's
/// [`PagedResult<Entity>`].
fn domain_entity_page_from_wire(resp: wr::ListEntitiesResponse) -> PagedResult<Entity> {
    PagedResult {
        items: resp.entities.into_iter().map(entity_from_wire).collect(),
        next_page_token: non_empty_token(resp.next_page_token),
        block_height: resp.block_height,
    }
}

/// Project a wire [`wr::ListRelationshipsResponse`] onto the SDK's
/// [`PagedResult<Relationship>`].
fn domain_relationship_page_from_wire(
    resp: wr::ListRelationshipsResponse,
) -> PagedResult<Relationship> {
    PagedResult {
        items: resp.relationships.into_iter().map(relationship_from_wire).collect(),
        next_page_token: non_empty_token(resp.next_page_token),
        block_height: resp.block_height,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    //! Unit coverage for the wire-response → domain-type projection plus
    //! the postcard round-trip of each list request shape. End-to-end
    //! tests against a real `WireClient` / `WireServer` are deferred to
    //! E.7 (TestCluster migration) — at that point the three entry
    //! points get exercised in full.

    use bytes::Bytes;

    use super::*;

    // -------------------------------------------------------------------
    // ReadConsistency mapping (mirrors the assertion in `ops_wire::data`).
    // -------------------------------------------------------------------

    #[test]
    fn domain_consistency_each_variant_maps_correctly() {
        assert_eq!(
            domain_consistency_to_wire(ReadConsistency::Eventual),
            ws::ReadConsistency::Eventual,
        );
        assert_eq!(
            domain_consistency_to_wire(ReadConsistency::Linearizable),
            ws::ReadConsistency::Linearizable,
        );
    }

    // -------------------------------------------------------------------
    // non_empty_token: empty string → None, populated → Some.
    // -------------------------------------------------------------------

    #[test]
    fn non_empty_token_empty_string_collapses_to_none() {
        assert!(non_empty_token(String::new()).is_none());
    }

    #[test]
    fn non_empty_token_populated_string_round_trips() {
        assert_eq!(non_empty_token("cursor-abc".to_owned()).as_deref(), Some("cursor-abc"));
    }

    // -------------------------------------------------------------------
    // Entity / Relationship from-wire conversion.
    // -------------------------------------------------------------------

    #[test]
    fn entity_from_wire_carries_all_fields() {
        let wire = ws::Entity {
            key: "user:42".to_owned(),
            value: Bytes::from_static(b"payload"),
            expires_at: Some(1_700_000_000),
            version: 7,
        };
        let domain = entity_from_wire(wire);
        assert_eq!(domain.key, "user:42");
        assert_eq!(domain.value, b"payload".to_vec());
        assert_eq!(domain.expires_at, Some(1_700_000_000));
        assert_eq!(domain.version, 7);
    }

    #[test]
    fn entity_from_wire_zero_expires_at_collapses_to_none() {
        // Mirrors `Entity::from_proto` — a server-emitted `expires_at: 0`
        // means "no expiration", collapsed to `None` so `is_expired_at`
        // returns false instead of treating epoch-zero as a past expiry.
        let wire = ws::Entity {
            key: "k".to_owned(),
            value: Bytes::new(),
            expires_at: Some(0),
            version: 0,
        };
        let domain = entity_from_wire(wire);
        assert!(domain.expires_at.is_none());
    }

    #[test]
    fn entity_from_wire_none_expires_at_stays_none() {
        let wire =
            ws::Entity { key: "k".to_owned(), value: Bytes::new(), expires_at: None, version: 0 };
        let domain = entity_from_wire(wire);
        assert!(domain.expires_at.is_none());
    }

    #[test]
    fn relationship_from_wire_carries_all_fields() {
        let wire = ws::Relationship {
            resource: "doc:1".to_owned(),
            relation: "viewer".to_owned(),
            subject: "user:alice".to_owned(),
        };
        let domain = relationship_from_wire(wire);
        assert_eq!(domain.resource, "doc:1");
        assert_eq!(domain.relation, "viewer");
        assert_eq!(domain.subject, "user:alice");
    }

    // -------------------------------------------------------------------
    // Page projection: ListEntitiesResponse / ListRelationshipsResponse.
    // -------------------------------------------------------------------

    #[test]
    fn domain_entity_page_from_wire_maps_pagination() {
        let resp = wr::ListEntitiesResponse {
            entities: vec![
                ws::Entity {
                    key: "user:1".to_owned(),
                    value: Bytes::from_static(b"alpha"),
                    expires_at: None,
                    version: 1,
                },
                ws::Entity {
                    key: "user:2".to_owned(),
                    value: Bytes::from_static(b"beta"),
                    expires_at: Some(1_700_000_000),
                    version: 2,
                },
            ],
            block_height: 100,
            next_page_token: "cursor-abc".to_owned(),
        };
        let page = domain_entity_page_from_wire(resp);
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].key, "user:1");
        assert_eq!(page.items[0].value, b"alpha".to_vec());
        assert_eq!(page.items[1].expires_at, Some(1_700_000_000));
        assert_eq!(page.next_page_token.as_deref(), Some("cursor-abc"));
        assert_eq!(page.block_height, 100);
        assert!(page.has_next_page());
    }

    #[test]
    fn domain_entity_page_from_wire_empty_token_collapses_to_none() {
        let resp = wr::ListEntitiesResponse {
            entities: vec![],
            block_height: 5,
            next_page_token: String::new(),
        };
        let page = domain_entity_page_from_wire(resp);
        assert!(page.items.is_empty());
        assert!(page.next_page_token.is_none());
        assert!(!page.has_next_page());
    }

    #[test]
    fn domain_relationship_page_from_wire_maps_pagination() {
        let resp = wr::ListRelationshipsResponse {
            relationships: vec![
                ws::Relationship {
                    resource: "doc:1".to_owned(),
                    relation: "viewer".to_owned(),
                    subject: "user:alice".to_owned(),
                },
                ws::Relationship {
                    resource: "doc:1".to_owned(),
                    relation: "editor".to_owned(),
                    subject: "user:bob".to_owned(),
                },
            ],
            block_height: 200,
            next_page_token: "cursor-rel".to_owned(),
        };
        let page = domain_relationship_page_from_wire(resp);
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].relation, "viewer");
        assert_eq!(page.items[1].subject, "user:bob");
        assert_eq!(page.next_page_token.as_deref(), Some("cursor-rel"));
        assert_eq!(page.block_height, 200);
    }

    #[test]
    fn domain_relationship_page_from_wire_empty_token_collapses_to_none() {
        let resp = wr::ListRelationshipsResponse {
            relationships: vec![],
            block_height: 0,
            next_page_token: String::new(),
        };
        let page = domain_relationship_page_from_wire(resp);
        assert!(page.next_page_token.is_none());
    }

    // -------------------------------------------------------------------
    // Postcard round-trip: each list request shape (pin the wire encoding).
    // -------------------------------------------------------------------

    #[test]
    fn list_entities_request_postcard_round_trips() {
        let req = wr::ListEntitiesRequest {
            organization: Some(ws::OrganizationSlug::new(7)),
            key_prefix: "user:".to_owned(),
            at_height: Some(1000),
            include_expired: true,
            limit: 50,
            page_token: "cursor-abc".to_owned(),
            consistency: ws::ReadConsistency::Linearizable,
            vault: Some(ws::VaultSlug::new(11)),
            caller: Some(ws::UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: wr::ListEntitiesRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn list_entities_request_no_vault_round_trips() {
        // Organization-level entities arrive with `vault: None`; this is
        // distinct from vault-level on the wire and the encoding must
        // preserve it.
        let req = wr::ListEntitiesRequest {
            organization: Some(ws::OrganizationSlug::new(7)),
            key_prefix: String::new(),
            at_height: None,
            include_expired: false,
            limit: 0,
            page_token: String::new(),
            consistency: ws::ReadConsistency::Eventual,
            vault: None,
            caller: Some(ws::UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: wr::ListEntitiesRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
        assert!(decoded.vault.is_none());
    }

    #[test]
    fn list_relationships_request_postcard_round_trips_with_filters() {
        let req = wr::ListRelationshipsRequest {
            organization: Some(ws::OrganizationSlug::new(1)),
            vault: Some(ws::VaultSlug::new(2)),
            resource: Some("doc:42".to_owned()),
            relation: Some("viewer".to_owned()),
            subject: None,
            at_height: Some(500),
            limit: 25,
            page_token: "rel-cursor".to_owned(),
            consistency: ws::ReadConsistency::Linearizable,
            caller: Some(ws::UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: wr::ListRelationshipsRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn list_resources_request_postcard_round_trips() {
        let req = wr::ListResourcesRequest {
            organization: Some(ws::OrganizationSlug::new(1)),
            vault: Some(ws::VaultSlug::new(2)),
            resource_type: "document".to_owned(),
            at_height: Some(1234),
            limit: 100,
            page_token: "res-cursor".to_owned(),
            consistency: ws::ReadConsistency::Eventual,
            caller: Some(ws::UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: wr::ListResourcesRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn list_resources_response_round_trips_and_projects_resources() {
        let resp = wr::ListResourcesResponse {
            resources: vec![
                "document:1".to_owned(),
                "document:2".to_owned(),
                "document:3".to_owned(),
            ],
            block_height: 42,
            next_page_token: String::new(),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: wr::ListResourcesResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);

        // Verify the SDK projection matches what `list_resources` would
        // emit on the consumer side.
        let projected = PagedResult {
            items: decoded.resources,
            next_page_token: non_empty_token(decoded.next_page_token),
            block_height: decoded.block_height,
        };
        assert_eq!(projected.items.len(), 3);
        assert_eq!(projected.items[0], "document:1");
        assert!(projected.next_page_token.is_none());
        assert_eq!(projected.block_height, 42);
    }
}
