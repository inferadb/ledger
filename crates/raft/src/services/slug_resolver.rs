//! Organization slug resolution for gRPC service boundaries.
//!
//! Translates external [`OrganizationSlug`] values to internal [`OrganizationId`]
//! at the service boundary. Every inbound request carrying a slug must resolve
//! it before any internal operation.
//!
//! Resolution reads from the Raft-replicated [`AppliedState`] slug index,
//! ensuring consistency across all nodes.
//!
//! ## Usage
//!
//! ```no_run
//! # use inferadb_ledger_raft::log_storage::AppliedStateAccessor;
//! # fn example(applied_state: &AppliedStateAccessor) -> Result<(), tonic::Status> {
//! use inferadb_ledger_raft::services::slug_resolver::SlugResolver;
//!
//! let resolver = SlugResolver::new(applied_state.clone());
//! // resolver.extract_and_resolve(&request.organization)?;
//! # Ok(())
//! # }
//! ```

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationId, OrganizationSlug};
use tonic::Status;

use crate::log_storage::AppliedStateAccessor;

/// Resolves external organization slugs to internal organization IDs.
///
/// Wraps an [`AppliedStateAccessor`] and provides validated, error-handling
/// resolution methods for use at gRPC service boundaries.
#[derive(Clone)]
pub struct SlugResolver {
    state: AppliedStateAccessor,
}

impl SlugResolver {
    /// Creates a resolver backed by the given applied state.
    pub fn new(state: AppliedStateAccessor) -> Self {
        Self { state }
    }

    /// Extracts and validates an organization slug from a proto message.
    ///
    /// Returns `INVALID_ARGUMENT` if the slug field is missing or zero.
    pub fn extract_slug(
        proto_slug: &Option<proto::OrganizationSlug>,
    ) -> Result<OrganizationSlug, Status> {
        let slug =
            proto_slug.as_ref().ok_or_else(|| Status::invalid_argument("Missing organization"))?;
        if slug.slug == 0 {
            return Err(Status::invalid_argument("organization must be non-zero"));
        }
        Ok(OrganizationSlug::new(slug.slug))
    }

    /// Resolves an organization slug to its internal ID.
    ///
    /// Returns `NOT_FOUND` if the slug is not registered.
    pub fn resolve(&self, slug: OrganizationSlug) -> Result<OrganizationId, Status> {
        self.state.resolve_slug_to_id(slug).ok_or_else(|| {
            Status::not_found(format!("Organization with slug {} not found", slug.value()))
        })
    }

    /// Reverse lookup: internal ID to external slug.
    ///
    /// Returns `NOT_FOUND` if the ID has no associated slug.
    pub fn resolve_slug(&self, id: OrganizationId) -> Result<OrganizationSlug, Status> {
        self.state
            .resolve_id_to_slug(id)
            .ok_or_else(|| Status::not_found(format!("Organization {} not found", id)))
    }

    /// Extracts a slug from a proto message and resolves it to an internal ID.
    ///
    /// Combines [`extract_slug`](Self::extract_slug) and [`resolve`](Self::resolve)
    /// for the common case where a request carries a required organization slug.
    pub fn extract_and_resolve(
        &self,
        proto_slug: &Option<proto::OrganizationSlug>,
    ) -> Result<OrganizationId, Status> {
        let slug = Self::extract_slug(proto_slug)?;
        self.resolve(slug)
    }

    /// Extracts an optional slug and resolves it if present.
    ///
    /// Returns `Ok(None)` when the proto field is absent. Returns an error
    /// only if the slug is present but zero or not found in the index.
    pub fn extract_and_resolve_optional(
        &self,
        proto_slug: &Option<proto::OrganizationSlug>,
    ) -> Result<Option<OrganizationId>, Status> {
        match proto_slug {
            None => Ok(None),
            Some(slug) if slug.slug == 0 => {
                Err(Status::invalid_argument("organization must be non-zero"))
            },
            Some(slug) => {
                let domain_slug = OrganizationSlug::new(slug.slug);
                self.resolve(domain_slug).map(Some)
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;

    use super::*;
    use crate::log_storage::AppliedState;

    fn make_resolver(slug_entries: &[(u64, i64)]) -> SlugResolver {
        let mut state = AppliedState::default();
        for &(slug_val, org_id_val) in slug_entries {
            let slug = OrganizationSlug::new(slug_val);
            let org_id = OrganizationId::new(org_id_val);
            state.slug_index.insert(slug, org_id);
            state.id_to_slug.insert(org_id, slug);
        }
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        SlugResolver::new(accessor)
    }

    #[test]
    fn extract_slug_valid() {
        let proto = Some(proto::OrganizationSlug { slug: 42 });
        let result = SlugResolver::extract_slug(&proto).unwrap();
        assert_eq!(result.value(), 42);
    }

    #[test]
    fn extract_slug_missing_returns_invalid_argument() {
        let result = SlugResolver::extract_slug(&None);
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Missing"));
    }

    #[test]
    fn extract_slug_zero_returns_invalid_argument() {
        let proto = Some(proto::OrganizationSlug { slug: 0 });
        let result = SlugResolver::extract_slug(&proto);
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("non-zero"));
    }

    #[test]
    fn resolve_valid_slug() {
        let resolver = make_resolver(&[(100, 1)]);
        let slug = OrganizationSlug::new(100);
        let org_id = resolver.resolve(slug).unwrap();
        assert_eq!(org_id, OrganizationId::new(1));
    }

    #[test]
    fn resolve_unknown_slug_returns_not_found() {
        let resolver = make_resolver(&[(100, 1)]);
        let slug = OrganizationSlug::new(999);
        let result = resolver.resolve(slug);
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert!(status.message().contains("999"));
    }

    #[test]
    fn resolve_slug_reverse_lookup() {
        let resolver = make_resolver(&[(100, 1)]);
        let slug = resolver.resolve_slug(OrganizationId::new(1)).unwrap();
        assert_eq!(slug.value(), 100);
    }

    #[test]
    fn resolve_slug_reverse_unknown_returns_not_found() {
        let resolver = make_resolver(&[(100, 1)]);
        let result = resolver.resolve_slug(OrganizationId::new(42));
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn extract_and_resolve_valid() {
        let resolver = make_resolver(&[(42, 7)]);
        let proto = Some(proto::OrganizationSlug { slug: 42 });
        let org_id = resolver.extract_and_resolve(&proto).unwrap();
        assert_eq!(org_id, OrganizationId::new(7));
    }

    #[test]
    fn extract_and_resolve_missing_slug() {
        let resolver = make_resolver(&[]);
        let result = resolver.extract_and_resolve(&None);
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_zero_slug() {
        let resolver = make_resolver(&[]);
        let proto = Some(proto::OrganizationSlug { slug: 0 });
        let result = resolver.extract_and_resolve(&proto);
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_unknown_slug() {
        let resolver = make_resolver(&[(100, 1)]);
        let proto = Some(proto::OrganizationSlug { slug: 999 });
        let result = resolver.extract_and_resolve(&proto);
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[test]
    fn extract_and_resolve_optional_none() {
        let resolver = make_resolver(&[]);
        let result = resolver.extract_and_resolve_optional(&None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn extract_and_resolve_optional_valid() {
        let resolver = make_resolver(&[(42, 7)]);
        let proto = Some(proto::OrganizationSlug { slug: 42 });
        let result = resolver.extract_and_resolve_optional(&proto).unwrap();
        assert_eq!(result, Some(OrganizationId::new(7)));
    }

    #[test]
    fn extract_and_resolve_optional_zero() {
        let resolver = make_resolver(&[]);
        let proto = Some(proto::OrganizationSlug { slug: 0 });
        let result = resolver.extract_and_resolve_optional(&proto);
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_optional_unknown() {
        let resolver = make_resolver(&[(100, 1)]);
        let proto = Some(proto::OrganizationSlug { slug: 999 });
        let result = resolver.extract_and_resolve_optional(&proto);
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[test]
    fn multiple_slugs_resolved_independently() {
        let resolver = make_resolver(&[(100, 1), (200, 2), (300, 3)]);
        assert_eq!(resolver.resolve(OrganizationSlug::new(100)).unwrap(), OrganizationId::new(1));
        assert_eq!(resolver.resolve(OrganizationSlug::new(200)).unwrap(), OrganizationId::new(2));
        assert_eq!(resolver.resolve(OrganizationSlug::new(300)).unwrap(), OrganizationId::new(3));
    }

    #[test]
    fn bidirectional_resolution() {
        let resolver = make_resolver(&[(42, 7)]);
        let org_id = resolver.resolve(OrganizationSlug::new(42)).unwrap();
        let slug = resolver.resolve_slug(org_id).unwrap();
        assert_eq!(slug.value(), 42);
    }

    #[test]
    fn resolver_is_clone() {
        let resolver = make_resolver(&[(1, 1)]);
        let cloned = resolver.clone();
        assert_eq!(
            resolver.resolve(OrganizationSlug::new(1)).unwrap(),
            cloned.resolve(OrganizationSlug::new(1)).unwrap()
        );
    }
}
