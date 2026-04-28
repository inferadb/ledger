//! Status enum conversions: `InvitationStatus`, `EventScope`, `EventOutcome`,
//! `EventEmission`, `EventEntry`, `Region`, `UserStatus`, `UserRole`, and
//! their `xxx_from_i32` helper functions plus `SigningKeyScope`/`SigningKeyStatus` helpers.

use chrono::DateTime;
use inferadb_ledger_types::{
    InvitationStatus, OrganizationId, OrganizationSlug, VaultSlug,
    events::{EventAction, EventEmission, EventEntry, EventOutcome, EventScope},
};
use tonic::Status;

use super::domain::{datetime_to_proto_timestamp, proto_timestamp_to_datetime};
use crate::proto;

// =============================================================================
// InvitationStatus conversions
// =============================================================================

/// Converts a domain [`InvitationStatus`] to its protobuf representation.
impl From<InvitationStatus> for proto::InvitationStatus {
    fn from(status: InvitationStatus) -> Self {
        match status {
            InvitationStatus::Pending => proto::InvitationStatus::Pending,
            InvitationStatus::Accepted => proto::InvitationStatus::Accepted,
            InvitationStatus::Declined => proto::InvitationStatus::Declined,
            InvitationStatus::Expired => proto::InvitationStatus::Expired,
            InvitationStatus::Revoked => proto::InvitationStatus::Revoked,
        }
    }
}

/// Converts a protobuf [`InvitationStatus`](proto::InvitationStatus) to the domain type.
///
/// Returns [`Status::invalid_argument`] for `Unspecified` — callers must provide
/// a concrete status value.
impl TryFrom<proto::InvitationStatus> for InvitationStatus {
    type Error = Status;

    fn try_from(proto: proto::InvitationStatus) -> Result<Self, Status> {
        match proto {
            proto::InvitationStatus::Unspecified => {
                Err(Status::invalid_argument("invitation status must be specified"))
            },
            proto::InvitationStatus::Pending => Ok(InvitationStatus::Pending),
            proto::InvitationStatus::Accepted => Ok(InvitationStatus::Accepted),
            proto::InvitationStatus::Declined => Ok(InvitationStatus::Declined),
            proto::InvitationStatus::Expired => Ok(InvitationStatus::Expired),
            proto::InvitationStatus::Revoked => Ok(InvitationStatus::Revoked),
        }
    }
}

/// Converts a raw `i32` proto enum value to a validated [`InvitationStatus`].
///
/// Returns [`Status::invalid_argument`] for `Unspecified` or unknown values.
pub fn invitation_status_from_i32(value: i32) -> Result<InvitationStatus, Status> {
    let proto_status = proto::InvitationStatus::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("unknown invitation status: {value}")))?;
    InvitationStatus::try_from(proto_status)
}

// =============================================================================
// EventScope conversions (domain <-> proto)
// =============================================================================

/// Converts a domain [`EventScope`] to its protobuf representation.
impl From<EventScope> for proto::EventScope {
    fn from(scope: EventScope) -> Self {
        match scope {
            EventScope::System => proto::EventScope::System,
            EventScope::Organization => proto::EventScope::Organization,
        }
    }
}

/// Converts a protobuf [`EventScope`](proto::EventScope) to the domain type.
///
/// Unspecified defaults to `Organization`.
impl From<proto::EventScope> for EventScope {
    fn from(proto: proto::EventScope) -> Self {
        match proto {
            proto::EventScope::System => EventScope::System,
            proto::EventScope::Organization | proto::EventScope::Unspecified => {
                EventScope::Organization
            },
        }
    }
}

// =============================================================================
// EventOutcome conversions (domain <-> proto)
// =============================================================================

/// Converts a domain [`EventOutcome`] to its protobuf enum value.
impl From<&EventOutcome> for proto::EventOutcome {
    fn from(outcome: &EventOutcome) -> Self {
        match outcome {
            EventOutcome::Success => proto::EventOutcome::Success,
            EventOutcome::Failed { .. } => proto::EventOutcome::Failed,
            EventOutcome::Denied { .. } => proto::EventOutcome::Denied,
        }
    }
}

// =============================================================================
// EventEmissionPath conversions (domain <-> proto)
// =============================================================================

/// Converts a domain [`EventEmission`] to its protobuf enum value.
impl From<&EventEmission> for proto::EventEmissionPath {
    fn from(emission: &EventEmission) -> Self {
        match emission {
            EventEmission::ApplyPhase => proto::EventEmissionPath::EmissionPathApplyPhase,
            EventEmission::HandlerPhase { .. } => {
                proto::EventEmissionPath::EmissionPathHandlerPhase
            },
        }
    }
}

// =============================================================================
// EventEntry conversions (domain -> proto)
// =============================================================================

/// Converts a domain [`EventEntry`] reference to its protobuf representation.
///
/// The domain type's rich enums (`EventOutcome`, `EventEmission`) are flattened
/// into separate fields in the proto message:
/// - `EventOutcome::Failed { code, detail }` → `outcome=FAILED` + `error_code` + `error_detail`
/// - `EventOutcome::Denied { reason }` → `outcome=DENIED` + `denial_reason`
/// - `EventEmission::HandlerPhase { node_id }` → `emission_path=HANDLER_PHASE` + `node_id`
impl From<&EventEntry> for proto::EventEntry {
    fn from(entry: &EventEntry) -> Self {
        let (error_code, error_detail, denial_reason) = match &entry.outcome {
            EventOutcome::Success => (None, None, None),
            EventOutcome::Failed { code, detail } => {
                (Some(code.clone()), Some(detail.clone()), None)
            },
            EventOutcome::Denied { reason } => (None, None, Some(reason.clone())),
        };

        let node_id = match &entry.emission {
            EventEmission::ApplyPhase => None,
            EventEmission::HandlerPhase { node_id } => Some(*node_id),
        };

        proto::EventEntry {
            event_id: entry.event_id.to_vec(),
            source_service: entry.source_service.clone(),
            event_type: entry.event_type.clone(),
            timestamp: Some(datetime_to_proto_timestamp(&entry.timestamp)),
            scope: proto::EventScope::from(entry.scope).into(),
            action: entry.action.as_str().to_string(),
            emission_path: proto::EventEmissionPath::from(&entry.emission).into(),
            principal: entry.principal.clone(),
            organization: Some(proto::OrganizationSlug {
                slug: entry
                    .organization
                    .map(|s| s.value())
                    .unwrap_or(entry.organization_id.value() as u64),
            }),
            vault: entry.vault.map(|s| proto::VaultSlug { slug: s.value() }),
            outcome: proto::EventOutcome::from(&entry.outcome).into(),
            error_code,
            error_detail,
            denial_reason,
            details: entry.details.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            block_height: entry.block_height,
            node_id,
            trace_id: entry.trace_id.clone(),
            correlation_id: entry.correlation_id.clone(),
            operations_count: entry.operations_count,
            expires_at: entry.expires_at,
        }
    }
}

/// Converts an owned domain [`EventEntry`] to its protobuf representation,
/// moving fields instead of cloning.
impl From<EventEntry> for proto::EventEntry {
    fn from(entry: EventEntry) -> Self {
        let proto_outcome = proto::EventOutcome::from(&entry.outcome);

        let (error_code, error_detail, denial_reason) = match entry.outcome {
            EventOutcome::Success => (None, None, None),
            EventOutcome::Failed { code, detail } => (Some(code), Some(detail), None),
            EventOutcome::Denied { reason } => (None, None, Some(reason)),
        };

        let (emission_path, node_id) = match entry.emission {
            EventEmission::ApplyPhase => (proto::EventEmissionPath::EmissionPathApplyPhase, None),
            EventEmission::HandlerPhase { node_id } => {
                (proto::EventEmissionPath::EmissionPathHandlerPhase, Some(node_id))
            },
        };

        proto::EventEntry {
            event_id: entry.event_id.to_vec(),
            source_service: entry.source_service,
            event_type: entry.event_type,
            timestamp: Some(datetime_to_proto_timestamp(&entry.timestamp)),
            scope: proto::EventScope::from(entry.scope).into(),
            action: entry.action.as_str().to_string(),
            emission_path: emission_path.into(),
            principal: entry.principal,
            organization: Some(proto::OrganizationSlug {
                slug: entry
                    .organization
                    .map(|s| s.value())
                    .unwrap_or(entry.organization_id.value() as u64),
            }),
            vault: entry.vault.map(|s| proto::VaultSlug { slug: s.value() }),
            outcome: proto_outcome.into(),
            error_code,
            error_detail,
            denial_reason,
            details: entry.details.into_iter().collect(),
            block_height: entry.block_height,
            node_id,
            trace_id: entry.trace_id,
            correlation_id: entry.correlation_id,
            operations_count: entry.operations_count,
            expires_at: entry.expires_at,
        }
    }
}

// =============================================================================
// EventEntry conversions (proto -> domain)
// =============================================================================

/// Converts a protobuf [`EventEntry`](proto::EventEntry) reference to the domain type.
///
/// Reconstructs rich enums from flattened proto fields:
/// - `outcome=FAILED` + `error_code` + `error_detail` → `EventOutcome::Failed { code, detail }`
/// - `outcome=DENIED` + `denial_reason` → `EventOutcome::Denied { reason }`
/// - `emission_path=HANDLER_PHASE` + `node_id` → `EventEmission::HandlerPhase { node_id }`
impl TryFrom<&proto::EventEntry> for EventEntry {
    type Error = Status;

    fn try_from(proto_entry: &proto::EventEntry) -> Result<Self, Self::Error> {
        let event_id: [u8; 16] = proto_entry
            .event_id
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("event_id must be exactly 16 bytes"))?;

        let timestamp = proto_entry
            .timestamp
            .as_ref()
            .map(proto_timestamp_to_datetime)
            .unwrap_or(DateTime::UNIX_EPOCH);

        let scope = proto::EventScope::try_from(proto_entry.scope)
            .unwrap_or(proto::EventScope::Unspecified)
            .into();

        let action: EventAction = proto_entry.action.parse().map_err(|_: String| {
            Status::invalid_argument(format!("unknown action: {}", proto_entry.action))
        })?;

        let outcome = match proto::EventOutcome::try_from(proto_entry.outcome)
            .unwrap_or(proto::EventOutcome::Unspecified)
        {
            proto::EventOutcome::Success | proto::EventOutcome::Unspecified => {
                EventOutcome::Success
            },
            proto::EventOutcome::Failed => EventOutcome::Failed {
                code: proto_entry.error_code.clone().unwrap_or_default(),
                detail: proto_entry.error_detail.clone().unwrap_or_default(),
            },
            proto::EventOutcome::Denied => EventOutcome::Denied {
                reason: proto_entry.denial_reason.clone().unwrap_or_default(),
            },
        };

        let emission = match proto::EventEmissionPath::try_from(proto_entry.emission_path)
            .unwrap_or(proto::EventEmissionPath::EmissionPathUnspecified)
        {
            proto::EventEmissionPath::EmissionPathApplyPhase
            | proto::EventEmissionPath::EmissionPathUnspecified => EventEmission::ApplyPhase,
            proto::EventEmissionPath::EmissionPathHandlerPhase => {
                EventEmission::HandlerPhase { node_id: proto_entry.node_id.unwrap_or(0) }
            },
        };

        let organization = proto_entry.organization.as_ref().map(|o| OrganizationSlug::new(o.slug));
        let organization_id =
            OrganizationId::new(organization.map(|s| s.value()).unwrap_or(0) as i64);

        Ok(EventEntry {
            expires_at: proto_entry.expires_at,
            event_id,
            source_service: proto_entry.source_service.clone(),
            event_type: proto_entry.event_type.clone(),
            timestamp,
            scope,
            action,
            emission,
            principal: proto_entry.principal.clone(),
            organization_id,
            organization,
            vault: proto_entry.vault.as_ref().map(|v| VaultSlug::new(v.slug)),
            outcome,
            details: proto_entry.details.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            block_height: proto_entry.block_height,
            trace_id: proto_entry.trace_id.clone(),
            correlation_id: proto_entry.correlation_id.clone(),
            operations_count: proto_entry.operations_count,
        })
    }
}

/// Converts an owned protobuf [`EventEntry`](proto::EventEntry) to the domain type,
/// moving fields instead of cloning.
impl TryFrom<proto::EventEntry> for EventEntry {
    type Error = Status;

    fn try_from(proto_entry: proto::EventEntry) -> Result<Self, Self::Error> {
        let event_id: [u8; 16] = proto_entry
            .event_id
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("event_id must be exactly 16 bytes"))?;

        let timestamp = proto_entry
            .timestamp
            .as_ref()
            .map(proto_timestamp_to_datetime)
            .unwrap_or(DateTime::UNIX_EPOCH);

        let scope = proto::EventScope::try_from(proto_entry.scope)
            .unwrap_or(proto::EventScope::Unspecified)
            .into();

        let action: EventAction = proto_entry.action.parse().map_err(|_: String| {
            Status::invalid_argument(format!("unknown action: {}", proto_entry.action))
        })?;

        let outcome = match proto::EventOutcome::try_from(proto_entry.outcome)
            .unwrap_or(proto::EventOutcome::Unspecified)
        {
            proto::EventOutcome::Success | proto::EventOutcome::Unspecified => {
                EventOutcome::Success
            },
            proto::EventOutcome::Failed => EventOutcome::Failed {
                code: proto_entry.error_code.unwrap_or_default(),
                detail: proto_entry.error_detail.unwrap_or_default(),
            },
            proto::EventOutcome::Denied => {
                EventOutcome::Denied { reason: proto_entry.denial_reason.unwrap_or_default() }
            },
        };

        let emission = match proto::EventEmissionPath::try_from(proto_entry.emission_path)
            .unwrap_or(proto::EventEmissionPath::EmissionPathUnspecified)
        {
            proto::EventEmissionPath::EmissionPathApplyPhase
            | proto::EventEmissionPath::EmissionPathUnspecified => EventEmission::ApplyPhase,
            proto::EventEmissionPath::EmissionPathHandlerPhase => {
                EventEmission::HandlerPhase { node_id: proto_entry.node_id.unwrap_or(0) }
            },
        };

        let organization = proto_entry.organization.as_ref().map(|o| OrganizationSlug::new(o.slug));
        let organization_id =
            OrganizationId::new(organization.map(|s| s.value()).unwrap_or(0) as i64);

        Ok(EventEntry {
            expires_at: proto_entry.expires_at,
            event_id,
            source_service: proto_entry.source_service,
            event_type: proto_entry.event_type,
            timestamp,
            scope,
            action,
            emission,
            principal: proto_entry.principal,
            organization_id,
            organization,
            vault: proto_entry.vault.as_ref().map(|v| VaultSlug::new(v.slug)),
            outcome,
            details: proto_entry.details.into_iter().collect(),
            block_height: proto_entry.block_height,
            trace_id: proto_entry.trace_id,
            correlation_id: proto_entry.correlation_id,
            operations_count: proto_entry.operations_count,
        })
    }
}

// =============================================================================
// Region conversions (types::Region <-> wire string)
// =============================================================================

/// Converts a region slug string (e.g. `"us-east-va"`) into the domain
/// [`Region`](inferadb_ledger_types::Region) type. Returns
/// [`Status::invalid_argument`] for empty or malformed slugs.
///
/// This is the single conversion point for proto wire messages whose `region`
/// field carries a slug string — every gRPC handler that accepts a region
/// from the wire should use this helper.
pub fn region_from_str(value: &str) -> Result<inferadb_ledger_types::Region, Status> {
    use std::str::FromStr;
    inferadb_ledger_types::Region::from_str(value)
        .map_err(|err| Status::invalid_argument(format!("invalid region: {err}")))
}

// =============================================================================
// UserStatus conversions
// =============================================================================

/// Converts a domain [`UserStatus`](inferadb_ledger_types::UserStatus) to its protobuf
/// representation.
impl From<inferadb_ledger_types::UserStatus> for proto::UserStatus {
    fn from(status: inferadb_ledger_types::UserStatus) -> Self {
        use inferadb_ledger_types::UserStatus as D;
        match status {
            D::Active => proto::UserStatus::Active,
            D::PendingOrg => proto::UserStatus::PendingOrg,
            D::Suspended => proto::UserStatus::Suspended,
            D::Deleting => proto::UserStatus::Deleting,
            D::Deleted => proto::UserStatus::Deleted,
        }
    }
}

/// Converts a protobuf [`UserStatus`](proto::UserStatus) to the domain type.
///
/// Returns [`Status::invalid_argument`] for `Unspecified`.
impl TryFrom<proto::UserStatus> for inferadb_ledger_types::UserStatus {
    type Error = Status;

    fn try_from(proto: proto::UserStatus) -> Result<Self, Status> {
        use inferadb_ledger_types::UserStatus as D;
        match proto {
            proto::UserStatus::Unspecified => {
                Err(Status::invalid_argument("user status must be specified"))
            },
            proto::UserStatus::Active => Ok(D::Active),
            proto::UserStatus::PendingOrg => Ok(D::PendingOrg),
            proto::UserStatus::Suspended => Ok(D::Suspended),
            proto::UserStatus::Deleting => Ok(D::Deleting),
            proto::UserStatus::Deleted => Ok(D::Deleted),
        }
    }
}

/// Converts a raw `i32` proto enum value to a domain
/// [`UserStatus`](inferadb_ledger_types::UserStatus).
pub fn user_status_from_i32(value: i32) -> Result<inferadb_ledger_types::UserStatus, Status> {
    let proto_status = proto::UserStatus::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("unknown user status value: {value}")))?;
    inferadb_ledger_types::UserStatus::try_from(proto_status)
}

// =============================================================================
// UserRole conversions
// =============================================================================

/// Converts a domain [`UserRole`](inferadb_ledger_types::UserRole) to its protobuf representation.
impl From<inferadb_ledger_types::UserRole> for proto::UserRole {
    fn from(role: inferadb_ledger_types::UserRole) -> Self {
        use inferadb_ledger_types::UserRole as D;
        match role {
            D::User => proto::UserRole::User,
            D::Admin => proto::UserRole::Admin,
        }
    }
}

/// Converts a protobuf [`UserRole`](proto::UserRole) to the domain type.
///
/// `Unspecified` defaults to `User`.
impl TryFrom<proto::UserRole> for inferadb_ledger_types::UserRole {
    type Error = Status;

    fn try_from(proto: proto::UserRole) -> Result<Self, Status> {
        use inferadb_ledger_types::UserRole as D;
        match proto {
            proto::UserRole::Unspecified | proto::UserRole::User => Ok(D::User),
            proto::UserRole::Admin => Ok(D::Admin),
        }
    }
}

/// Converts a raw `i32` proto enum value to a domain [`UserRole`](inferadb_ledger_types::UserRole).
pub fn user_role_from_i32(value: i32) -> Result<inferadb_ledger_types::UserRole, Status> {
    let proto_role = proto::UserRole::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("unknown user role value: {value}")))?;
    inferadb_ledger_types::UserRole::try_from(proto_role)
}

// =============================================================================
// SigningKeyScope conversions (proto enum <-> i32 helper)
// =============================================================================

/// Converts a raw `i32` proto enum value to a validated `SigningKeyScope` proto enum.
///
/// Returns [`Status::invalid_argument`] for `UNSPECIFIED` — callers must explicitly
/// choose `GLOBAL` or `ORGANIZATION`.
///
/// Note: The domain `SigningKeyScope` type (in the state crate) carries an
/// `OrganizationId` payload for the `Organization` variant. Full domain conversion
/// requires the state crate and lives in the services layer.
pub fn signing_key_scope_from_i32(value: i32) -> Result<proto::SigningKeyScope, Status> {
    let scope = proto::SigningKeyScope::try_from(value).map_err(|_| {
        Status::invalid_argument(format!("unknown signing key scope value: {value}"))
    })?;
    match scope {
        proto::SigningKeyScope::Unspecified => {
            Err(Status::invalid_argument("signing key scope must be specified"))
        },
        _ => Ok(scope),
    }
}

/// Converts a signing key status string (`"active"`, `"rotated"`, `"revoked"`) to
/// the proto `PublicKeyInfo.status` field value.
///
/// Note: The domain `SigningKeyStatus` enum is in the state crate. Full domain ↔ proto
/// conversion lives in the services layer. This helper validates the string format
/// at the proto boundary.
pub fn validate_signing_key_status(status: &str) -> Result<&str, Status> {
    match status {
        "active" | "rotated" | "revoked" => Ok(status),
        _ => Err(Status::invalid_argument(format!("unknown signing key status: {status}"))),
    }
}
