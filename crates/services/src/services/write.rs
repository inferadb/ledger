//! Write service implementation.
//!
//! Handles transaction submission through Raft consensus.
//!
//! Uses application-level batching to coalesce multiple write requests into
//! single Raft proposals, improving throughput by reducing consensus
//! round-trips.

use std::{fmt::Write, sync::Arc, time::Duration};

use inferadb_ledger_raft::{
    idempotency::IdempotencyCache,
    logging::Sampler,
    metrics,
    proof::{self, ProofError},
    raft_manager::RaftManager,
    rate_limit::RateLimiter,
    types::OrganizationRequest,
};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{OrganizationId, SetCondition, VaultId, config::ValidationConfig};
use inferadb_ledger_wire::services::write::WriteErrorCode;
use tonic::Status;
use tracing::{debug, warn};

use super::region_resolver::{RegionContext, RegionResolver};

/// gRPC handler for transaction submission.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct WriteService {
    /// Region resolver for routing requests to the correct region.
    pub(super) resolver: Arc<dyn RegionResolver>,
    /// Raft manager for creating forward clients when needed.
    #[builder(default)]
    pub(super) manager: Option<Arc<RaftManager>>,
    /// Typed proposal service used to route per-vault writes through the
    /// shared `ProposalService` abstraction.
    pub(super) proposal_service: Arc<dyn crate::proposal::ProposalService>,
    /// Idempotency cache for duplicate detection.
    pub(super) idempotency: Arc<IdempotencyCache>,
    /// Per-organization rate limiter.
    #[builder(default)]
    pub(super) rate_limiter: Option<Arc<RateLimiter>>,
    /// Sampler for log tail sampling.
    #[builder(default)]
    pub(super) sampler: Option<Sampler>,
    /// Node ID for logging system context.
    #[builder(default)]
    pub(super) node_id: Option<u64>,
    /// Hot key detector for identifying frequently accessed keys.
    #[builder(default)]
    pub(super) hot_key_detector:
        Option<Arc<inferadb_ledger_raft::hot_key_detector::HotKeyDetector>>,
    /// Input validation configuration for request field limits.
    #[builder(default = Arc::new(ValidationConfig::default()))]
    pub(super) validation_config: Arc<ValidationConfig>,
    /// Maximum time to wait for a Raft proposal to commit.
    ///
    /// If a gRPC deadline is shorter, the deadline takes precedence.
    #[builder(default = Duration::from_secs(30))]
    pub(super) proposal_timeout: Duration,
    /// Handler-phase event handle for recording denial events.
    #[builder(default)]
    pub(super) event_handle: Option<inferadb_ledger_raft::event_writer::EventHandle<FileBackend>>,
    /// Health state for drain-phase write rejection.
    #[builder(default)]
    pub(super) health_state: Option<inferadb_ledger_raft::graceful_shutdown::HealthState>,
    /// Shared peer address map for resolving peer endpoints in `NotLeader`
    /// hint responses returned to clients on follower nodes.
    #[builder(default)]
    pub(super) peer_addresses: Option<inferadb_ledger_raft::PeerAddressMap>,
}

#[allow(clippy::result_large_err)]
impl WriteService {
    /// Attaches per-organization rate limiting to an existing service.
    #[must_use]
    pub fn with_rate_limiter(mut self, rate_limiter: Arc<RateLimiter>) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    /// Attaches hot key detector for identifying frequently accessed keys.
    #[must_use]
    pub fn with_hot_key_detector(
        mut self,
        detector: Arc<inferadb_ledger_raft::hot_key_detector::HotKeyDetector>,
    ) -> Self {
        self.hot_key_detector = Some(detector);
        self
    }

    /// Attaches input validation configuration for request field limits.
    #[must_use]
    pub fn with_validation_config(mut self, config: Arc<ValidationConfig>) -> Self {
        self.validation_config = config;
        self
    }

    /// Sets the maximum time to wait for a Raft proposal to commit.
    #[must_use]
    pub fn with_proposal_timeout(mut self, timeout: Duration) -> Self {
        self.proposal_timeout = timeout;
        self
    }

    /// Attaches the handler-phase event handle for recording denial events.
    #[must_use]
    pub fn with_event_handle(
        mut self,
        handle: inferadb_ledger_raft::event_writer::EventHandle<FileBackend>,
    ) -> Self {
        self.event_handle = Some(handle);
        self
    }

    /// Attaches health state for drain-phase write rejection.
    #[must_use]
    pub fn with_health_state(
        mut self,
        health_state: inferadb_ledger_raft::graceful_shutdown::HealthState,
    ) -> Self {
        self.health_state = Some(health_state);
        self
    }

    /// Rejects writes to organizations undergoing migration.
    ///
    /// Returns `Ok(())` if the organization is not migrating (or doesn't exist yet).
    /// Returns `Err(Status::FailedPrecondition)` with structured error details
    /// if the organization is actively migrating to another region.
    pub(super) fn check_not_migrating(
        &self,
        system: &RegionContext,
        organization_id: OrganizationId,
    ) -> Result<(), Status> {
        if let Some(org_meta) = system.applied_state.get_organization(organization_id)
            && org_meta.status == inferadb_ledger_state::system::OrganizationStatus::Migrating
        {
            let mut context = std::collections::HashMap::new();
            context.insert("organization".to_string(), organization_id.value().to_string());
            if let Some(pending) = org_meta.pending_region {
                context.insert("target_region".to_string(), pending.as_str().to_string());
            }
            let mut wire_err = super::error_details::build_error_details(
                inferadb_ledger_types::DiagnosticCode::AppOrganizationMigrating.as_u16(),
                true,
                Some(30_000),
                context,
                Some(
                    inferadb_ledger_types::DiagnosticCode::AppOrganizationMigrating
                        .suggested_action(),
                ),
            );
            wire_err.code = inferadb_ledger_wire::ErrorCode::FailedPrecondition;
            wire_err.message =
                "Organization is being migrated to another region; writes are temporarily blocked"
                    .to_string();
            return Err(super::wire_helpers::wire_error_to_tonic_status(wire_err));
        }
        Ok(())
    }

    /// Validates all operations in a wire operation list.
    /// Validates operations against the configured limits, returning the
    /// wire-native [`inferadb_ledger_wire::WireError`] on failure.
    pub(super) fn validate_operations_wire(
        &self,
        operations: &[inferadb_ledger_wire::services::shared::Operation],
    ) -> Result<(), inferadb_ledger_wire::WireError> {
        super::helpers::validate_operations(operations, &self.validation_config)
    }

    /// Checks all rate limit tiers (backpressure, organization, client),
    /// returning the wire-native [`inferadb_ledger_wire::WireError`] on
    /// failure.
    pub(super) fn check_rate_limit_wire(
        &self,
        client_id: &str,
        organization: OrganizationId,
    ) -> Result<(), inferadb_ledger_wire::WireError> {
        super::helpers::check_rate_limit(self.rate_limiter.as_ref(), client_id, organization)
    }

    /// Records key accesses from operations for hot key detection.
    pub(super) fn record_hot_keys(
        &self,
        vault: VaultId,
        operations: &[inferadb_ledger_wire::services::shared::Operation],
    ) {
        super::helpers::record_hot_keys(self.hot_key_detector.as_ref(), vault, operations);
    }

    /// Maps a failed SetCondition to the appropriate WriteErrorCode.
    ///
    /// Maps per proto WriteErrorCode:
    /// - MustNotExist failed → KEY_EXISTS (key already exists)
    /// - MustExist failed → KEY_NOT_FOUND (key doesn't exist)
    /// - VersionEquals failed with existing key → VERSION_MISMATCH
    /// - VersionEquals failed with missing key → KEY_NOT_FOUND
    /// - ValueEquals failed with existing key → VALUE_MISMATCH
    /// - ValueEquals failed with missing key → KEY_NOT_FOUND
    pub(super) fn map_condition_to_error_code(
        condition: Option<&SetCondition>,
        key_exists: bool,
    ) -> WriteErrorCode {
        match condition {
            Some(SetCondition::MustNotExist) => {
                // MustNotExist failed means the key exists
                WriteErrorCode::KeyExists
            },
            Some(SetCondition::MustExist) => {
                // MustExist failed means the key doesn't exist
                WriteErrorCode::KeyNotFound
            },
            Some(SetCondition::VersionEquals(_)) => {
                if key_exists {
                    WriteErrorCode::VersionMismatch
                } else {
                    WriteErrorCode::KeyNotFound
                }
            },
            Some(SetCondition::ValueEquals(_)) => {
                if key_exists {
                    WriteErrorCode::ValueMismatch
                } else {
                    WriteErrorCode::KeyNotFound
                }
            },
            None => {
                // No condition - shouldn't reach here for PreconditionFailed
                WriteErrorCode::Unspecified
            },
        }
    }

    /// Converts bytes to hex string for request logging.
    pub(super) fn bytes_to_hex(bytes: &[u8]) -> String {
        bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
            let _ = write!(acc, "{b:02x}");
            acc
        })
    }

    /// Extracts operation type names from wire operations for request logging.
    pub(super) fn extract_operation_types(
        operations: &[inferadb_ledger_wire::services::shared::Operation],
    ) -> Vec<&'static str> {
        use inferadb_ledger_wire::services::shared::OperationKind;

        operations
            .iter()
            .filter_map(|op| op.op.as_ref())
            .map(|op| match op {
                OperationKind::CreateRelationship(_) => "create_relationship",
                OperationKind::DeleteRelationship(_) => "delete_relationship",
                OperationKind::SetEntity(_) => "set_entity",
                OperationKind::DeleteEntity(_) => "delete_entity",
                OperationKind::ExpireEntity(_) => "expire_entity",
            })
            .collect()
    }

    /// Estimates the total payload bytes across all wire operations.
    ///
    /// Sums key and value sizes for entity operations, and resource/relation/subject
    /// lengths for relationship operations.
    pub(super) fn estimate_operations_bytes(
        operations: &[inferadb_ledger_wire::services::shared::Operation],
    ) -> usize {
        use inferadb_ledger_wire::services::shared::OperationKind;

        operations
            .iter()
            .filter_map(|op| op.op.as_ref())
            .map(|op| match op {
                OperationKind::CreateRelationship(cr) => {
                    cr.resource.len() + cr.relation.len() + cr.subject.len()
                },
                OperationKind::DeleteRelationship(dr) => {
                    dr.resource.len() + dr.relation.len() + dr.subject.len()
                },
                OperationKind::SetEntity(se) => se.key.len() + se.value.len(),
                OperationKind::DeleteEntity(de) => de.key.len(),
                OperationKind::ExpireEntity(ee) => ee.key.len(),
            })
            .sum()
    }

    /// Generates block header and transaction proof for a committed write.
    ///
    /// Uses the region resolver to obtain the block archive and applied state
    /// for the organization's region.
    ///
    /// Returns `(block_header, tx_proof, proof_error_reason)`.
    /// On success, the third element is `None`.
    /// On failure, `block_header` and `tx_proof` are `None` and `proof_error_reason`
    /// carries a stable label describing why proof generation failed. The write
    /// itself committed; the proof is a post-commit enrichment step.
    pub(super) fn generate_write_proof(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        vault_height: u64,
    ) -> (
        Option<inferadb_ledger_wire::services::shared::BlockHeader>,
        Option<inferadb_ledger_wire::services::shared::MerkleProof>,
        Option<&'static str>,
    ) {
        let ctx = match self.resolver.resolve(organization) {
            Ok(ctx) => ctx,
            Err(_) => {
                let reason = "region_unavailable";
                metrics::record_proof_generation_failure(reason);
                warn!(
                    organization_id = organization.value(),
                    vault_id = vault.value(),
                    vault_height,
                    reason,
                    "Proof generation failed: region resolver returned error"
                );
                return (None, None, Some(reason));
            },
        };

        // Resolve internal IDs to slugs for response construction
        let org_slug = ctx.applied_state.resolve_id_to_slug(organization);
        let vault_slug = ctx.applied_state.resolve_vault_id_to_slug(organization, vault);

        // Use the proof module's SNAFU-based implementation
        match proof::generate_write_proof(
            &ctx.block_archive,
            organization,
            org_slug,
            vault,
            vault_slug,
            vault_height,
            0,
        ) {
            Ok(write_proof) => (Some(write_proof.block_header), Some(write_proof.tx_proof), None),
            Err(e) => {
                // Classify the failure reason for metrics and response metadata
                let reason: &'static str = match &e {
                    ProofError::BlockNotFound { .. } => "block_not_found",
                    ProofError::NoTransactions => "no_transactions",
                    _ => "internal_error",
                };

                // Log with appropriate severity based on error type
                match &e {
                    ProofError::BlockNotFound { .. } | ProofError::NoTransactions => {
                        // Timing issue: block not yet written to archive
                        debug!(
                            error = %e,
                            organization_id = organization.value(),
                            vault_id = vault.value(),
                            vault_height,
                            reason,
                            "Proof generation skipped"
                        );
                    },
                    _ => {
                        warn!(
                            error = %e,
                            organization_id = organization.value(),
                            vault_id = vault.value(),
                            vault_height,
                            reason,
                            "Proof generation failed"
                        );
                    },
                }

                metrics::record_proof_generation_failure(reason);

                (None, None, Some(reason))
            },
        }
    }

    /// Converts wire operations to an [`OrganizationRequest`] for Raft.
    ///
    /// Server-assigned sequences: The transaction's sequence is set to 0
    /// here; the actual sequence will be assigned by the Raft state machine
    /// at apply time.
    ///
    /// `organization_slug` / `vault_slug` are the external Snowflake slugs
    /// carried from the incoming wire request. They're stamped onto the
    /// emitted `VaultEntry` at apply time (γ Phase 3a) so the
    /// block-announcement formatter can read them directly without
    /// consulting per-region `AppliedState` slug maps.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn operations_to_request_wire(
        &self,
        organization: OrganizationId,
        vault: Option<VaultId>,
        operations: &[inferadb_ledger_wire::services::shared::Operation],
        client_id: &str,
        idempotency_key: [u8; 16],
        request_hash: u64,
        organization_slug: inferadb_ledger_types::OrganizationSlug,
        vault_slug: inferadb_ledger_types::VaultSlug,
    ) -> Result<OrganizationRequest, inferadb_ledger_wire::WireError> {
        let internal_ops: Vec<inferadb_ledger_types::Operation> =
            operations.iter().map(wire_operation_to_internal).collect::<Result<Vec<_>, _>>()?;

        let transaction = inferadb_ledger_types::Transaction {
            id: *uuid::Uuid::new_v4().as_bytes(),
            client_id: inferadb_ledger_types::ClientId::new(client_id),
            sequence: 0,
            operations: internal_ops,
            timestamp: chrono::Utc::now(),
        };
        let _ = organization;

        Ok(OrganizationRequest::Write {
            vault: vault.unwrap_or(VaultId::new(0)),
            transactions: vec![transaction],
            idempotency_key,
            request_hash,
            organization_slug,
            vault_slug,
        })
    }
}

/// Converts a wire [`Operation`](inferadb_ledger_wire::services::shared::Operation)
/// to the domain [`Operation`](inferadb_ledger_types::Operation), validating that
/// the `op` oneof is present.
fn wire_operation_to_internal(
    wire_op: &inferadb_ledger_wire::services::shared::Operation,
) -> Result<inferadb_ledger_types::Operation, inferadb_ledger_wire::WireError> {
    use inferadb_ledger_wire::services::shared::{OperationKind, SetConditionKind};
    let op = wire_op.op.as_ref().ok_or_else(|| {
        super::wire_helpers::build_wire_error(
            inferadb_ledger_wire::ErrorCode::InvalidArgument,
            "Operation missing op field",
            "",
            false,
            0,
            std::collections::BTreeMap::new(),
            "",
        )
    })?;
    Ok(match op {
        OperationKind::CreateRelationship(cr) => {
            inferadb_ledger_types::Operation::CreateRelationship {
                resource: cr.resource.clone(),
                relation: cr.relation.clone(),
                subject: cr.subject.clone(),
            }
        },
        OperationKind::DeleteRelationship(dr) => {
            inferadb_ledger_types::Operation::DeleteRelationship {
                resource: dr.resource.clone(),
                relation: dr.relation.clone(),
                subject: dr.subject.clone(),
            }
        },
        OperationKind::SetEntity(se) => {
            let condition = se.condition.as_ref().and_then(|c| {
                c.condition.as_ref().map(|kind| match kind {
                    SetConditionKind::NotExists(_) => SetCondition::MustNotExist,
                    SetConditionKind::MustExists(_) => SetCondition::MustExist,
                    SetConditionKind::Version(v) => SetCondition::VersionEquals(*v),
                    SetConditionKind::ValueEquals(v) => SetCondition::ValueEquals(v.to_vec()),
                })
            });
            inferadb_ledger_types::Operation::SetEntity {
                key: se.key.clone(),
                value: se.value.to_vec(),
                condition,
                expires_at: se.expires_at,
            }
        },
        OperationKind::DeleteEntity(de) => {
            inferadb_ledger_types::Operation::DeleteEntity { key: de.key.clone() }
        },
        OperationKind::ExpireEntity(ee) => inferadb_ledger_types::Operation::ExpireEntity {
            key: ee.key.clone(),
            expired_at: ee.expired_at,
        },
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use bytes::Bytes;
    use inferadb_ledger_types::config::ValidationConfig;
    use inferadb_ledger_wire::services::shared as ws;

    use super::{WriteErrorCode, WriteService};

    fn wire_set_entity(key: &str, value: &[u8]) -> ws::Operation {
        ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: key.to_string(),
                value: Bytes::copy_from_slice(value),
                expires_at: None,
                condition: None,
            })),
        }
    }

    fn wire_delete_entity(key: &str) -> ws::Operation {
        ws::Operation {
            op: Some(ws::OperationKind::DeleteEntity(ws::DeleteEntity { key: key.to_string() })),
        }
    }

    fn wire_expire_entity(key: &str, expired_at: u64) -> ws::Operation {
        ws::Operation {
            op: Some(ws::OperationKind::ExpireEntity(ws::ExpireEntity {
                key: key.to_string(),
                expired_at,
            })),
        }
    }

    fn wire_create_relationship(resource: &str, relation: &str, subject: &str) -> ws::Operation {
        ws::Operation {
            op: Some(ws::OperationKind::CreateRelationship(ws::CreateRelationship {
                resource: resource.to_string(),
                relation: relation.to_string(),
                subject: subject.to_string(),
            })),
        }
    }

    fn wire_delete_relationship(resource: &str, relation: &str, subject: &str) -> ws::Operation {
        ws::Operation {
            op: Some(ws::OperationKind::DeleteRelationship(ws::DeleteRelationship {
                resource: resource.to_string(),
                relation: relation.to_string(),
                subject: subject.to_string(),
            })),
        }
    }

    /// Helper: drives the wire-native validation entry point used by the
    /// service. Mirrors the original `validate_wire_operations` helper but
    /// works directly with [`inferadb_ledger_wire::services::shared::Operation`].
    fn validate_wire_operations(
        operations: &[ws::Operation],
        config: &ValidationConfig,
    ) -> Result<(), inferadb_ledger_wire::WireError> {
        super::super::helpers::validate_operations(operations, config)
    }

    fn make_set_entity(key: &str, value: &[u8]) -> ws::Operation {
        wire_set_entity(key, value)
    }

    fn make_delete_entity(key: &str) -> ws::Operation {
        wire_delete_entity(key)
    }

    fn make_create_relationship(resource: &str, relation: &str, subject: &str) -> ws::Operation {
        wire_create_relationship(resource, relation, subject)
    }

    // =========================================================================
    // Validation → gRPC Status mapping tests
    // =========================================================================

    #[test]
    fn valid_set_entity_passes_validation() {
        let config = ValidationConfig::default();
        let ops = vec![make_set_entity("user:123", b"data")];
        assert!(validate_wire_operations(&ops, &config).is_ok());
    }

    #[test]
    fn valid_delete_entity_passes_validation() {
        let config = ValidationConfig::default();
        let ops = vec![make_delete_entity("user:123")];
        assert!(validate_wire_operations(&ops, &config).is_ok());
    }

    #[test]
    fn valid_relationship_passes_validation() {
        let config = ValidationConfig::default();
        let ops = vec![make_create_relationship("doc:456", "viewer", "user:123")];
        assert!(validate_wire_operations(&ops, &config).is_ok());
    }

    #[test]
    fn empty_key_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops = vec![make_set_entity("", b"data")];
        let err = validate_wire_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
        assert!(
            err.message.as_str().contains("key"),
            "Error should mention key: {}",
            err.message.as_str()
        );
    }

    #[test]
    fn key_with_invalid_chars_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops = vec![make_set_entity("user 123", b"data")];
        let err = validate_wire_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
    }

    #[test]
    fn key_exceeding_max_size_returns_invalid_argument() {
        let config = ValidationConfig::builder().max_key_bytes(10).build().unwrap();
        let ops = vec![make_set_entity(&"a".repeat(11), b"data")];
        let err = validate_wire_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
        assert!(
            err.message.as_str().contains("key"),
            "Error should mention key: {}",
            err.message.as_str()
        );
    }

    #[test]
    fn value_exceeding_max_size_returns_invalid_argument() {
        let config = ValidationConfig::builder().max_value_bytes(4).build().unwrap();
        let ops = vec![make_set_entity("key", &[0u8; 5])];
        let err = validate_wire_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
        assert!(
            err.message.as_str().contains("value"),
            "Error should mention value: {}",
            err.message.as_str()
        );
    }

    #[test]
    fn test_too_many_operations_returns_invalid_argument() {
        let config = ValidationConfig::builder().max_operations_per_write(2).build().unwrap();
        let ops = vec![
            make_set_entity("a", b"1"),
            make_set_entity("b", b"2"),
            make_set_entity("c", b"3"),
        ];
        let err = validate_wire_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
        assert!(
            err.message.as_str().contains("operations"),
            "Error should mention operations: {}",
            err.message.as_str()
        );
    }

    #[test]
    fn test_zero_operations_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops: Vec<ws::Operation> = vec![];
        let err = validate_wire_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_payload_exceeding_max_bytes_returns_invalid_argument() {
        let config = ValidationConfig::builder().max_batch_payload_bytes(10).build().unwrap();
        let ops = vec![make_set_entity("key", &[0u8; 11])];
        let err = validate_wire_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
        assert!(
            err.message.as_str().contains("payload"),
            "Error should mention payload: {}",
            err.message.as_str()
        );
    }

    #[test]
    fn test_missing_op_field_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops = vec![ws::Operation { op: None }];
        let err = validate_wire_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
        assert!(
            err.message.as_str().contains("missing"),
            "Error should mention missing: {}",
            err.message.as_str()
        );
    }

    #[test]
    fn test_relationship_invalid_chars_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops = vec![make_create_relationship("doc 456", "viewer", "user:123")];
        let err = validate_wire_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_relationship_empty_field_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops = vec![make_create_relationship("doc:456", "", "user:123")];
        let err = validate_wire_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_key_at_exact_limit_passes() {
        let config = ValidationConfig::builder().max_key_bytes(5).build().unwrap();
        let ops = vec![make_set_entity("abcde", b"v")];
        assert!(validate_wire_operations(&ops, &config).is_ok());
    }

    #[test]
    fn test_value_at_exact_limit_passes() {
        let config = ValidationConfig::builder().max_value_bytes(5).build().unwrap();
        let ops = vec![make_set_entity("k", &[0u8; 5])];
        assert!(validate_wire_operations(&ops, &config).is_ok());
    }

    #[test]
    fn test_operations_at_exact_limit_passes() {
        let config = ValidationConfig::builder().max_operations_per_write(2).build().unwrap();
        let ops = vec![make_set_entity("a", b"1"), make_set_entity("b", b"2")];
        assert!(validate_wire_operations(&ops, &config).is_ok());
    }

    // =========================================================================
    // map_condition_to_error_code
    // =========================================================================

    #[test]
    fn map_condition_must_not_exist_failed_returns_key_exists() {
        let code = WriteService::map_condition_to_error_code(
            Some(&inferadb_ledger_types::SetCondition::MustNotExist),
            true,
        );
        assert_eq!(code, WriteErrorCode::KeyExists);
    }

    #[test]
    fn map_condition_must_exist_failed_returns_key_not_found() {
        let code = WriteService::map_condition_to_error_code(
            Some(&inferadb_ledger_types::SetCondition::MustExist),
            false,
        );
        assert_eq!(code, WriteErrorCode::KeyNotFound);
    }

    #[test]
    fn map_condition_version_equals_key_exists_returns_version_mismatch() {
        let code = WriteService::map_condition_to_error_code(
            Some(&inferadb_ledger_types::SetCondition::VersionEquals(42)),
            true,
        );
        assert_eq!(code, WriteErrorCode::VersionMismatch);
    }

    #[test]
    fn map_condition_version_equals_key_missing_returns_key_not_found() {
        let code = WriteService::map_condition_to_error_code(
            Some(&inferadb_ledger_types::SetCondition::VersionEquals(42)),
            false,
        );
        assert_eq!(code, WriteErrorCode::KeyNotFound);
    }

    #[test]
    fn map_condition_value_equals_key_exists_returns_value_mismatch() {
        let code = WriteService::map_condition_to_error_code(
            Some(&inferadb_ledger_types::SetCondition::ValueEquals(b"expected".to_vec())),
            true,
        );
        assert_eq!(code, WriteErrorCode::ValueMismatch);
    }

    #[test]
    fn map_condition_value_equals_key_missing_returns_key_not_found() {
        let code = WriteService::map_condition_to_error_code(
            Some(&inferadb_ledger_types::SetCondition::ValueEquals(b"expected".to_vec())),
            false,
        );
        assert_eq!(code, WriteErrorCode::KeyNotFound);
    }

    #[test]
    fn map_condition_none_returns_unspecified() {
        let code = WriteService::map_condition_to_error_code(None, true);
        assert_eq!(code, WriteErrorCode::Unspecified);
    }

    // =========================================================================
    // bytes_to_hex
    // =========================================================================

    #[test]
    fn bytes_to_hex_empty() {
        assert_eq!(WriteService::bytes_to_hex(&[]), "");
    }

    #[test]
    fn bytes_to_hex_single_byte() {
        assert_eq!(WriteService::bytes_to_hex(&[0xff]), "ff");
    }

    #[test]
    fn bytes_to_hex_zero_padded() {
        assert_eq!(WriteService::bytes_to_hex(&[0x0a]), "0a");
    }

    #[test]
    fn bytes_to_hex_multiple_bytes() {
        assert_eq!(WriteService::bytes_to_hex(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    #[test]
    fn bytes_to_hex_all_zeros() {
        assert_eq!(WriteService::bytes_to_hex(&[0, 0, 0]), "000000");
    }

    // =========================================================================
    // extract_operation_types
    // =========================================================================

    #[test]
    fn extract_operation_types_empty() {
        let ops: Vec<ws::Operation> = vec![];
        let types = WriteService::extract_operation_types(&ops);
        assert!(types.is_empty());
    }

    #[test]
    fn extract_operation_types_all_variants() {
        let ops = vec![
            wire_set_entity("k", b"v"),
            wire_delete_entity("k"),
            wire_expire_entity("k", 100),
            wire_create_relationship("r", "rel", "s"),
            wire_delete_relationship("r", "rel", "s"),
        ];
        let types = WriteService::extract_operation_types(&ops);
        assert_eq!(
            types,
            vec![
                "set_entity",
                "delete_entity",
                "expire_entity",
                "create_relationship",
                "delete_relationship",
            ]
        );
    }

    #[test]
    fn extract_operation_types_skips_none_op() {
        let ops = vec![ws::Operation { op: None }, wire_set_entity("k", b"v")];
        let types = WriteService::extract_operation_types(&ops);
        assert_eq!(types, vec!["set_entity"]);
    }

    // =========================================================================
    // estimate_operations_bytes
    // =========================================================================

    #[test]
    fn estimate_operations_bytes_empty() {
        let ops: Vec<ws::Operation> = vec![];
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 0);
    }

    #[test]
    fn estimate_operations_bytes_set_entity() {
        let ops = vec![wire_set_entity("key", b"value")];
        // "key" = 3 bytes, "value" = 5 bytes
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 8);
    }

    #[test]
    fn estimate_operations_bytes_delete_entity() {
        let ops = vec![wire_delete_entity("entity:1")];
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 8);
    }

    #[test]
    fn estimate_operations_bytes_expire_entity() {
        let ops = vec![wire_expire_entity("abc", 100)];
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 3);
    }

    #[test]
    fn estimate_operations_bytes_create_relationship() {
        let ops = vec![wire_create_relationship("doc:1", "viewer", "user:1")];
        // "doc:1" = 5, "viewer" = 6, "user:1" = 6
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 17);
    }

    #[test]
    fn estimate_operations_bytes_delete_relationship() {
        let ops = vec![wire_delete_relationship("doc:1", "viewer", "user:1")];
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 17);
    }

    #[test]
    fn estimate_operations_bytes_mixed_operations() {
        let ops = vec![
            wire_set_entity("k", b"v"),              // 1 + 1 = 2
            wire_delete_entity("dk"),                // 2
            wire_create_relationship("r", "x", "s"), // 1 + 1 + 1 = 3
        ];
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 7);
    }

    #[test]
    fn estimate_operations_bytes_skips_none_op() {
        let ops = vec![ws::Operation { op: None }, wire_set_entity("k", b"v")];
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 2);
    }
}
