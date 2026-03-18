//! ID/Slug type conversions: `VaultSlug`, `UserSlug`, `UserEmailId`,
//! `EmailVerifyTokenId`, `InviteSlug`.

use inferadb_ledger_types::{EmailVerifyTokenId, InviteSlug, UserEmailId, UserSlug, VaultSlug};

use crate::proto;

// =============================================================================
// VaultSlug conversions (inferadb_ledger_types::VaultSlug <-> proto::VaultSlug)
// =============================================================================

/// Converts a domain [`VaultSlug`] to its protobuf representation.
impl From<VaultSlug> for proto::VaultSlug {
    fn from(slug: VaultSlug) -> Self {
        proto::VaultSlug { slug: slug.value() }
    }
}

/// Converts a protobuf [`VaultSlug`](proto::VaultSlug) to the domain type.
impl From<proto::VaultSlug> for VaultSlug {
    fn from(proto: proto::VaultSlug) -> Self {
        VaultSlug::new(proto.slug)
    }
}

/// Converts a protobuf [`VaultSlug`](proto::VaultSlug) reference to the domain type.
impl From<&proto::VaultSlug> for VaultSlug {
    fn from(proto: &proto::VaultSlug) -> Self {
        VaultSlug::new(proto.slug)
    }
}

// =============================================================================
// UserSlug conversions
// =============================================================================

/// Converts a domain [`UserSlug`] to its protobuf representation.
impl From<UserSlug> for proto::UserSlug {
    fn from(slug: UserSlug) -> Self {
        proto::UserSlug { slug: slug.value() }
    }
}

/// Converts a protobuf [`UserSlug`](proto::UserSlug) to the domain type.
impl From<proto::UserSlug> for UserSlug {
    fn from(proto: proto::UserSlug) -> Self {
        UserSlug::new(proto.slug)
    }
}

/// Converts a protobuf [`UserSlug`](proto::UserSlug) reference to the domain type.
impl From<&proto::UserSlug> for UserSlug {
    fn from(proto: &proto::UserSlug) -> Self {
        UserSlug::new(proto.slug)
    }
}

// UserEmailId conversions
// =============================================================================

/// Converts a domain [`UserEmailId`] to its protobuf representation.
impl From<UserEmailId> for proto::UserEmailId {
    fn from(id: UserEmailId) -> Self {
        proto::UserEmailId { id: id.value() }
    }
}

/// Converts a protobuf [`UserEmailId`](proto::UserEmailId) to the domain type.
impl From<proto::UserEmailId> for UserEmailId {
    fn from(proto: proto::UserEmailId) -> Self {
        UserEmailId::new(proto.id)
    }
}

/// Converts a protobuf [`UserEmailId`](proto::UserEmailId) reference to the domain type.
impl From<&proto::UserEmailId> for UserEmailId {
    fn from(proto: &proto::UserEmailId) -> Self {
        UserEmailId::new(proto.id)
    }
}

// EmailVerifyTokenId conversions
// =============================================================================

/// Converts a domain [`EmailVerifyTokenId`] to its protobuf representation.
impl From<EmailVerifyTokenId> for proto::EmailVerifyTokenId {
    fn from(id: EmailVerifyTokenId) -> Self {
        proto::EmailVerifyTokenId { id: id.value() }
    }
}

/// Converts a protobuf [`EmailVerifyTokenId`](proto::EmailVerifyTokenId) to the domain type.
impl From<proto::EmailVerifyTokenId> for EmailVerifyTokenId {
    fn from(proto: proto::EmailVerifyTokenId) -> Self {
        EmailVerifyTokenId::new(proto.id)
    }
}

/// Converts a protobuf [`EmailVerifyTokenId`](proto::EmailVerifyTokenId) reference to the domain
/// type.
impl From<&proto::EmailVerifyTokenId> for EmailVerifyTokenId {
    fn from(proto: &proto::EmailVerifyTokenId) -> Self {
        EmailVerifyTokenId::new(proto.id)
    }
}

// =============================================================================
// InviteSlug conversions
// =============================================================================

/// Converts a domain [`InviteSlug`] to its protobuf representation.
impl From<InviteSlug> for proto::InviteSlug {
    fn from(slug: InviteSlug) -> Self {
        proto::InviteSlug { slug: slug.value() }
    }
}

/// Converts a protobuf [`InviteSlug`](proto::InviteSlug) to the domain type.
impl From<proto::InviteSlug> for InviteSlug {
    fn from(proto: proto::InviteSlug) -> Self {
        InviteSlug::new(proto.slug)
    }
}

/// Converts a protobuf [`InviteSlug`](proto::InviteSlug) reference to the domain type.
impl From<&proto::InviteSlug> for InviteSlug {
    fn from(proto: &proto::InviteSlug) -> Self {
        InviteSlug::new(proto.slug)
    }
}
