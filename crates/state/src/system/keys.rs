//! Key patterns for the `_system` namespace.
//!
//! Per DESIGN.md lines 1933-1949.

use inferadb_ledger_types::{NamespaceId, NodeId, UserId};

/// Key pattern generators for `_system` namespace entities.
///
/// All keys follow the convention `{entity_type}:{id}` for primary keys
/// and `_idx:{index_name}:{value}` for secondary indexes.
pub struct SystemKeys;

impl SystemKeys {
    // ========================================================================
    // User Keys
    // ========================================================================

    /// Primary key for a user record.
    ///
    /// Pattern: `user:{id}`
    ///
    /// # Example
    /// ```
    /// use inferadb_ledger_state::system::SystemKeys;
    /// assert_eq!(SystemKeys::user_key(123), "user:123");
    /// ```
    pub fn user_key(user_id: UserId) -> String {
        format!("user:{user_id}")
    }

    /// Parse a user ID from a user key.
    ///
    /// Returns `None` if the key doesn't match the expected pattern.
    pub fn parse_user_key(key: &str) -> Option<UserId> {
        key.strip_prefix("user:").and_then(|id| id.parse().ok())
    }

    // ========================================================================
    // Email Keys
    // ========================================================================

    /// Primary key for a user email record.
    ///
    /// Pattern: `user_email:{id}`
    pub fn user_email_key(email_id: i64) -> String {
        format!("user_email:{email_id}")
    }

    /// Index key for email uniqueness lookup.
    ///
    /// Pattern: `_idx:email:{email}` → email_id
    ///
    /// Emails are normalized to lowercase for consistent lookups.
    pub fn email_index_key(email: &str) -> String {
        format!("_idx:email:{}", email.to_lowercase())
    }

    /// Index key for looking up all emails belonging to a user.
    ///
    /// Pattern: `_idx:user_emails:{user_id}` → [email_id, ...]
    pub fn user_emails_index_key(user_id: UserId) -> String {
        format!("_idx:user_emails:{user_id}")
    }

    /// Primary key for an email verification token.
    ///
    /// Pattern: `email_verify:{id}`
    pub fn email_verify_key(token_id: i64) -> String {
        format!("email_verify:{token_id}")
    }

    // ========================================================================
    // Namespace Keys
    // ========================================================================

    /// Primary key for a namespace registry entry.
    ///
    /// Pattern: `ns:{namespace_id}`
    pub fn namespace_key(namespace_id: NamespaceId) -> String {
        format!("ns:{namespace_id}")
    }

    /// Parse a namespace ID from a namespace key.
    pub fn parse_namespace_key(key: &str) -> Option<NamespaceId> {
        key.strip_prefix("ns:").and_then(|id| id.parse().ok())
    }

    /// Index key for namespace name lookup.
    ///
    /// Pattern: `_idx:ns:name:{name}` → namespace_id
    ///
    /// Names are normalized to lowercase for consistent lookups.
    pub fn namespace_name_index_key(name: &str) -> String {
        format!("_idx:ns:name:{}", name.to_lowercase())
    }

    // ========================================================================
    // Node Keys
    // ========================================================================

    /// Primary key for a cluster node info record.
    ///
    /// Pattern: `node:{id}`
    pub fn node_key(node_id: &NodeId) -> String {
        format!("node:{node_id}")
    }

    /// Parse a node ID from a node key.
    pub fn parse_node_key(key: &str) -> Option<NodeId> {
        key.strip_prefix("node:").map(|id| id.to_string())
    }

    // ========================================================================
    // Sequence Counter Keys
    // ========================================================================

    /// Key for the namespace ID sequence counter.
    ///
    /// Pattern: `_meta:seq:namespace` → next NamespaceId (starts at 1, 0 = _system)
    pub const NAMESPACE_SEQ_KEY: &'static str = "_meta:seq:namespace";

    /// Key for the vault ID sequence counter.
    ///
    /// Pattern: `_meta:seq:vault` → next VaultId
    pub const VAULT_SEQ_KEY: &'static str = "_meta:seq:vault";

    /// Key for the user ID sequence counter.
    ///
    /// Pattern: `_meta:seq:user` → next UserId
    pub const USER_SEQ_KEY: &'static str = "_meta:seq:user";

    /// Key for the user email ID sequence counter.
    ///
    /// Pattern: `_meta:seq:user_email` → next UserEmailId
    pub const USER_EMAIL_SEQ_KEY: &'static str = "_meta:seq:user_email";

    /// Key for the email verification token ID sequence counter.
    ///
    /// Pattern: `_meta:seq:email_verify` → next TokenId
    pub const EMAIL_VERIFY_SEQ_KEY: &'static str = "_meta:seq:email_verify";

    // ========================================================================
    // Saga Keys
    // ========================================================================

    /// Key for a saga state record.
    ///
    /// Pattern: `saga:{saga_id}`
    pub fn saga_key(saga_id: &str) -> String {
        format!("saga:{saga_id}")
    }

    // ========================================================================
    // Key Prefixes (for scanning)
    // ========================================================================

    /// Prefix for all user keys.
    pub const USER_PREFIX: &'static str = "user:";

    /// Prefix for all user email keys.
    pub const USER_EMAIL_PREFIX: &'static str = "user_email:";

    /// Prefix for all namespace keys.
    pub const NAMESPACE_PREFIX: &'static str = "ns:";

    /// Prefix for all node keys.
    pub const NODE_PREFIX: &'static str = "node:";

    /// Prefix for all saga keys.
    pub const SAGA_PREFIX: &'static str = "saga:";

    /// Prefix for all index keys.
    pub const INDEX_PREFIX: &'static str = "_idx:";

    /// Prefix for all metadata keys.
    pub const META_PREFIX: &'static str = "_meta:";
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_user_key() {
        assert_eq!(SystemKeys::user_key(123), "user:123");
        assert_eq!(SystemKeys::parse_user_key("user:123"), Some(123));
        assert_eq!(SystemKeys::parse_user_key("invalid:123"), None);
    }

    #[test]
    fn test_email_index_key() {
        // Should normalize to lowercase
        assert_eq!(
            SystemKeys::email_index_key("Alice@Example.COM"),
            "_idx:email:alice@example.com"
        );
    }

    #[test]
    fn test_namespace_key() {
        assert_eq!(SystemKeys::namespace_key(42), "ns:42");
        assert_eq!(SystemKeys::parse_namespace_key("ns:42"), Some(42));
    }

    #[test]
    fn test_node_key() {
        assert_eq!(SystemKeys::node_key(&"node-1".to_string()), "node:node-1");
        assert_eq!(
            SystemKeys::parse_node_key("node:node-1"),
            Some("node-1".to_string())
        );
    }

    #[test]
    fn test_sequence_keys() {
        assert_eq!(SystemKeys::NAMESPACE_SEQ_KEY, "_meta:seq:namespace");
        assert_eq!(SystemKeys::USER_SEQ_KEY, "_meta:seq:user");
    }

    #[test]
    fn test_saga_key() {
        assert_eq!(
            SystemKeys::saga_key("create-org-abc123"),
            "saga:create-org-abc123"
        );
    }

    #[test]
    fn test_prefixes() {
        assert!(SystemKeys::user_key(1).starts_with(SystemKeys::USER_PREFIX));
        assert!(SystemKeys::namespace_key(1).starts_with(SystemKeys::NAMESPACE_PREFIX));
        assert!(SystemKeys::node_key(&"n".to_string()).starts_with(SystemKeys::NODE_PREFIX));
    }
}
