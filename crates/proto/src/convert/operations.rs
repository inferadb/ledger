//! `Operation` and `SetCondition` conversions to/from proto.

use tonic::Status;

use crate::proto;

// =============================================================================
// Operation conversions (inferadb_ledger_types::Operation <-> proto::Operation)
// =============================================================================

/// Converts a domain [`Operation`](inferadb_ledger_types::Operation) reference to its protobuf
/// representation.
impl From<&inferadb_ledger_types::Operation> for proto::Operation {
    fn from(op: &inferadb_ledger_types::Operation) -> Self {
        use inferadb_ledger_types::Operation as LedgerOp;
        use proto::operation::Op;

        match op {
            LedgerOp::CreateRelationship { resource, relation, subject } => proto::Operation {
                op: Some(Op::CreateRelationship(proto::CreateRelationship {
                    resource: resource.clone(),
                    relation: relation.clone(),
                    subject: subject.clone(),
                })),
            },
            LedgerOp::DeleteRelationship { resource, relation, subject } => proto::Operation {
                op: Some(Op::DeleteRelationship(proto::DeleteRelationship {
                    resource: resource.clone(),
                    relation: relation.clone(),
                    subject: subject.clone(),
                })),
            },
            LedgerOp::SetEntity { key, value, condition, expires_at } => {
                let condition_proto = condition.as_ref().map(|c| c.into());

                proto::Operation {
                    op: Some(Op::SetEntity(proto::SetEntity {
                        key: key.clone(),
                        value: value.clone(),
                        condition: condition_proto,
                        expires_at: *expires_at,
                    })),
                }
            },
            LedgerOp::DeleteEntity { key } => proto::Operation {
                op: Some(Op::DeleteEntity(proto::DeleteEntity { key: key.clone() })),
            },
            LedgerOp::ExpireEntity { key, expired_at } => proto::Operation {
                op: Some(Op::ExpireEntity(proto::ExpireEntity {
                    key: key.clone(),
                    expired_at: *expired_at,
                })),
            },
        }
    }
}

/// Converts an owned domain [`Operation`](inferadb_ledger_types::Operation) to its protobuf
/// representation.
impl From<inferadb_ledger_types::Operation> for proto::Operation {
    fn from(op: inferadb_ledger_types::Operation) -> Self {
        (&op).into()
    }
}

// =============================================================================
// SetCondition conversions (inferadb_ledger_types::SetCondition <-> proto::SetCondition)
// =============================================================================

/// Converts a domain [`SetCondition`](inferadb_ledger_types::SetCondition) reference to its
/// protobuf representation.
impl From<&inferadb_ledger_types::SetCondition> for proto::SetCondition {
    fn from(c: &inferadb_ledger_types::SetCondition) -> Self {
        use proto::set_condition::Condition;

        let condition = match c {
            inferadb_ledger_types::SetCondition::MustNotExist => Condition::NotExists(true),
            inferadb_ledger_types::SetCondition::MustExist => {
                // Proto uses must_exists field for this
                Condition::MustExists(true)
            },
            inferadb_ledger_types::SetCondition::VersionEquals(v) => Condition::Version(*v),
            inferadb_ledger_types::SetCondition::ValueEquals(bytes) => {
                Condition::ValueEquals(bytes.clone())
            },
        };

        proto::SetCondition { condition: Some(condition) }
    }
}

/// Converts an owned domain [`SetCondition`](inferadb_ledger_types::SetCondition) to its protobuf
/// representation.
impl From<inferadb_ledger_types::SetCondition> for proto::SetCondition {
    fn from(c: inferadb_ledger_types::SetCondition) -> Self {
        (&c).into()
    }
}

// =============================================================================
// SetCondition conversions (proto::SetCondition -> inferadb_ledger_types::SetCondition)
// =============================================================================

/// Converts a protobuf [`SetCondition`](proto::SetCondition) reference to an optional domain
/// [`SetCondition`](inferadb_ledger_types::SetCondition).
///
/// Returns `None` if the proto condition field is not set.
/// The `NotExists(false)` and `MustExists(false)` cases map to their logical
/// inverses for backward compatibility with proto3 default values.
impl From<&proto::SetCondition> for Option<inferadb_ledger_types::SetCondition> {
    fn from(proto: &proto::SetCondition) -> Self {
        use proto::set_condition::Condition;

        proto.condition.as_ref().map(|c| match c {
            Condition::NotExists(true) => inferadb_ledger_types::SetCondition::MustNotExist,
            Condition::NotExists(false) => inferadb_ledger_types::SetCondition::MustExist,
            Condition::MustExists(true) => inferadb_ledger_types::SetCondition::MustExist,
            Condition::MustExists(false) => inferadb_ledger_types::SetCondition::MustNotExist,
            Condition::Version(v) => inferadb_ledger_types::SetCondition::VersionEquals(*v),
            Condition::ValueEquals(v) => {
                inferadb_ledger_types::SetCondition::ValueEquals(v.clone())
            },
        })
    }
}

// =============================================================================
// Operation conversions (proto::Operation -> inferadb_ledger_types::Operation)
// =============================================================================

/// Converts a protobuf [`Operation`](proto::Operation) reference to the domain
/// [`Operation`](inferadb_ledger_types::Operation).
///
/// Returns `Err(Status::InvalidArgument)` if the `op` field is not set.
impl TryFrom<&proto::Operation> for inferadb_ledger_types::Operation {
    type Error = Status;

    fn try_from(proto_op: &proto::Operation) -> Result<Self, Self::Error> {
        use proto::operation::Op;

        let op = proto_op
            .op
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Operation missing op field"))?;

        match op {
            Op::CreateRelationship(cr) => {
                Ok(inferadb_ledger_types::Operation::CreateRelationship {
                    resource: cr.resource.clone(),
                    relation: cr.relation.clone(),
                    subject: cr.subject.clone(),
                })
            },
            Op::DeleteRelationship(dr) => {
                Ok(inferadb_ledger_types::Operation::DeleteRelationship {
                    resource: dr.resource.clone(),
                    relation: dr.relation.clone(),
                    subject: dr.subject.clone(),
                })
            },
            Op::SetEntity(se) => {
                let condition: Option<inferadb_ledger_types::SetCondition> =
                    se.condition.as_ref().and_then(|c| c.into());

                Ok(inferadb_ledger_types::Operation::SetEntity {
                    key: se.key.clone(),
                    value: se.value.clone(),
                    condition,
                    expires_at: se.expires_at,
                })
            },
            Op::DeleteEntity(de) => {
                Ok(inferadb_ledger_types::Operation::DeleteEntity { key: de.key.clone() })
            },
            Op::ExpireEntity(ee) => Ok(inferadb_ledger_types::Operation::ExpireEntity {
                key: ee.key.clone(),
                expired_at: ee.expired_at,
            }),
        }
    }
}

/// Converts an owned protobuf [`Operation`](proto::Operation) to the domain
/// [`Operation`](inferadb_ledger_types::Operation).
///
/// Delegates to the reference-based conversion.
impl TryFrom<proto::Operation> for inferadb_ledger_types::Operation {
    type Error = Status;

    fn try_from(proto_op: proto::Operation) -> Result<Self, Self::Error> {
        Self::try_from(&proto_op)
    }
}
