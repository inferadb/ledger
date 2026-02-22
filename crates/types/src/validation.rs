//! Input validation for gRPC request fields.
//!
//! Provides configurable validation for entity keys, values, organization names,
//! relationship strings, and batch sizes. Used at both the gRPC service boundary
//! (server-side) and in SDK operation constructors (client-side).
//!
//! ## Character Whitelists
//!
//! - Entity keys: `[a-zA-Z0-9:/_.-]` — safe for storage paths and log output.
//! - Organization names: `[a-z0-9-]{1,63}` — DNS-safe labels for routing.
//! - Relationship strings: `[a-zA-Z0-9:/_.-#]` — same as keys plus `#` for subject fragments (e.g.,
//!   `user:456#member`).

use std::fmt;

use crate::config::ValidationConfig;

/// Validation error with structured context.
///
/// Contains the specific constraint that was violated and the field name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    /// The field that failed validation.
    pub field: String,
    /// Description of the violated constraint.
    pub constraint: String,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.field, self.constraint)
    }
}

impl std::error::Error for ValidationError {}

/// Validates an entity key against configured limits and character whitelist.
///
/// Entity keys must:
/// - Be non-empty
/// - Not exceed `config.max_key_bytes` in UTF-8 byte length
/// - Contain only `[a-zA-Z0-9:/_.-]`
///
/// # Errors
///
/// Returns [`ValidationError`] if the key is empty, exceeds `max_key_bytes`,
/// or contains characters outside the `[a-zA-Z0-9:/_.-]` whitelist.
pub fn validate_key(key: &str, config: &ValidationConfig) -> Result<(), ValidationError> {
    if key.is_empty() {
        return Err(ValidationError {
            field: "key".to_string(),
            constraint: "must not be empty".to_string(),
        });
    }
    if key.len() > config.max_key_bytes {
        return Err(ValidationError {
            field: "key".to_string(),
            constraint: format!(
                "length {} bytes exceeds maximum {} bytes",
                key.len(),
                config.max_key_bytes
            ),
        });
    }
    if let Some(pos) = key.find(|c: char| !is_key_char(c)) {
        return Err(ValidationError {
            field: "key".to_string(),
            constraint: format!(
                "contains invalid character {:?} at byte offset {}; allowed: [a-zA-Z0-9:/_.-]",
                key[pos..].chars().next().unwrap_or('\0'),
                pos
            ),
        });
    }
    Ok(())
}

/// Validates an entity value against configured size limits.
///
/// Values must not exceed `config.max_value_bytes`.
///
/// # Errors
///
/// Returns [`ValidationError`] if the value exceeds `max_value_bytes`.
pub fn validate_value(value: &[u8], config: &ValidationConfig) -> Result<(), ValidationError> {
    if value.len() > config.max_value_bytes {
        return Err(ValidationError {
            field: "value".to_string(),
            constraint: format!(
                "length {} bytes exceeds maximum {} bytes",
                value.len(),
                config.max_value_bytes
            ),
        });
    }
    Ok(())
}

/// Validates an organization name against configured limits and DNS-safe whitelist.
///
/// Organization names must:
/// - Be non-empty
/// - Not exceed `config.max_organization_name_bytes` in UTF-8 byte length
/// - Contain only `[a-z0-9-]`
/// - Not start or end with a hyphen
///
/// # Errors
///
/// Returns [`ValidationError`] if the name is empty, exceeds `max_organization_name_bytes`,
/// starts or ends with a hyphen, or contains characters outside `[a-z0-9-]`.
pub fn validate_organization_name(
    name: &str,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    if name.is_empty() {
        return Err(ValidationError {
            field: "organization_name".to_string(),
            constraint: "must not be empty".to_string(),
        });
    }
    if name.len() > config.max_organization_name_bytes {
        return Err(ValidationError {
            field: "organization_name".to_string(),
            constraint: format!(
                "length {} bytes exceeds maximum {} bytes",
                name.len(),
                config.max_organization_name_bytes
            ),
        });
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err(ValidationError {
            field: "organization_name".to_string(),
            constraint: "must not start or end with a hyphen".to_string(),
        });
    }
    if let Some(pos) = name.find(|c: char| !is_organization_char(c)) {
        return Err(ValidationError {
            field: "organization_name".to_string(),
            constraint: format!(
                "contains invalid character {:?} at byte offset {}; allowed: [a-z0-9-]",
                name[pos..].chars().next().unwrap_or('\0'),
                pos
            ),
        });
    }
    Ok(())
}

/// Validates a relationship string (resource, relation, or subject).
///
/// Relationship strings must:
/// - Be non-empty
/// - Not exceed `config.max_relationship_string_bytes`
/// - Contain only `[a-zA-Z0-9:/_.-#]`
///
/// # Errors
///
/// Returns [`ValidationError`] if the string is empty, exceeds
/// `max_relationship_string_bytes`, or contains characters outside
/// `[a-zA-Z0-9:/_.-#]`.
pub fn validate_relationship_string(
    value: &str,
    field_name: &str,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    if value.is_empty() {
        return Err(ValidationError {
            field: field_name.to_string(),
            constraint: "must not be empty".to_string(),
        });
    }
    if value.len() > config.max_relationship_string_bytes {
        return Err(ValidationError {
            field: field_name.to_string(),
            constraint: format!(
                "length {} bytes exceeds maximum {} bytes",
                value.len(),
                config.max_relationship_string_bytes
            ),
        });
    }
    if let Some(pos) = value.find(|c: char| !is_relationship_char(c)) {
        return Err(ValidationError {
            field: field_name.to_string(),
            constraint: format!(
                "contains invalid character {:?} at byte offset {}; allowed: [a-zA-Z0-9:/_.-#]",
                value[pos..].chars().next().unwrap_or('\0'),
                pos
            ),
        });
    }
    Ok(())
}

/// Validates the number of operations in a write request.
///
/// # Errors
///
/// Returns [`ValidationError`] if the count is zero or exceeds
/// `max_operations_per_write`.
pub fn validate_operations_count(
    count: usize,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    if count == 0 {
        return Err(ValidationError {
            field: "operations".to_string(),
            constraint: "must contain at least one operation".to_string(),
        });
    }
    if count > config.max_operations_per_write {
        return Err(ValidationError {
            field: "operations".to_string(),
            constraint: format!(
                "count {} exceeds maximum {} operations per write",
                count, config.max_operations_per_write
            ),
        });
    }
    Ok(())
}

/// Validates the total payload size of a batch of operations.
///
/// # Errors
///
/// Returns [`ValidationError`] if the total payload exceeds
/// `max_batch_payload_bytes`.
pub fn validate_batch_payload_bytes(
    total_bytes: usize,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    if total_bytes > config.max_batch_payload_bytes {
        return Err(ValidationError {
            field: "operations".to_string(),
            constraint: format!(
                "total payload {} bytes exceeds maximum {} bytes",
                total_bytes, config.max_batch_payload_bytes
            ),
        });
    }
    Ok(())
}

/// Checks if a character is allowed in entity keys.
fn is_key_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, ':' | '/' | '_' | '.' | '-')
}

/// Checks if a character is allowed in organization names.
fn is_organization_char(c: char) -> bool {
    c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'
}

/// Checks if a character is allowed in relationship strings.
fn is_relationship_char(c: char) -> bool {
    is_key_char(c) || c == '#'
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn default_config() -> ValidationConfig {
        ValidationConfig::default()
    }

    // =========================================================================
    // validate_key tests
    // =========================================================================

    #[test]
    fn test_validate_key_valid_simple() {
        let config = default_config();
        assert!(validate_key("user:123", &config).is_ok());
    }

    #[test]
    fn test_validate_key_valid_all_allowed_chars() {
        let config = default_config();
        assert!(validate_key("a-z_A-Z.0-9:/path", &config).is_ok());
    }

    #[test]
    fn test_validate_key_empty() {
        let config = default_config();
        let err = validate_key("", &config).unwrap_err();
        assert_eq!(err.field, "key");
        assert!(err.constraint.contains("empty"));
    }

    #[test]
    fn test_validate_key_exactly_at_limit() {
        let config = ValidationConfig { max_key_bytes: 10, ..ValidationConfig::default() };
        assert!(validate_key("a234567890", &config).is_ok());
    }

    #[test]
    fn test_validate_key_one_byte_over_limit() {
        let config = ValidationConfig { max_key_bytes: 10, ..ValidationConfig::default() };
        let err = validate_key("a2345678901", &config).unwrap_err();
        assert_eq!(err.field, "key");
        assert!(err.constraint.contains("exceeds maximum"));
    }

    #[test]
    fn test_validate_key_control_character() {
        let config = default_config();
        let err = validate_key("key\x00value", &config).unwrap_err();
        assert_eq!(err.field, "key");
        assert!(err.constraint.contains("invalid character"));
    }

    #[test]
    fn test_validate_key_null_byte() {
        let config = default_config();
        let err = validate_key("key\0", &config).unwrap_err();
        assert!(err.constraint.contains("invalid character"));
    }

    #[test]
    fn test_validate_key_unicode() {
        let config = default_config();
        let err = validate_key("key_\u{00e9}", &config).unwrap_err();
        assert!(err.constraint.contains("invalid character"));
    }

    #[test]
    fn test_validate_key_space() {
        let config = default_config();
        let err = validate_key("key with space", &config).unwrap_err();
        assert!(err.constraint.contains("invalid character"));
    }

    #[test]
    fn test_validate_key_newline() {
        let config = default_config();
        let err = validate_key("key\nvalue", &config).unwrap_err();
        assert!(err.constraint.contains("invalid character"));
    }

    #[test]
    fn test_validate_key_single_char() {
        let config = default_config();
        assert!(validate_key("a", &config).is_ok());
    }

    // =========================================================================
    // validate_value tests
    // =========================================================================

    #[test]
    fn test_validate_value_empty() {
        let config = default_config();
        assert!(validate_value(b"", &config).is_ok());
    }

    #[test]
    fn test_validate_value_exactly_at_limit() {
        let config = ValidationConfig { max_value_bytes: 10, ..ValidationConfig::default() };
        assert!(validate_value(&[0u8; 10], &config).is_ok());
    }

    #[test]
    fn test_validate_value_one_byte_over_limit() {
        let config = ValidationConfig { max_value_bytes: 10, ..ValidationConfig::default() };
        let err = validate_value(&[0u8; 11], &config).unwrap_err();
        assert_eq!(err.field, "value");
        assert!(err.constraint.contains("exceeds maximum"));
    }

    #[test]
    fn test_validate_value_binary_data() {
        let config = default_config();
        let data: Vec<u8> = (0..=255).collect();
        assert!(validate_value(&data, &config).is_ok());
    }

    // =========================================================================
    // validate_organization_name tests
    // =========================================================================

    #[test]
    fn test_validate_organization_name_valid_simple() {
        let config = default_config();
        assert!(validate_organization_name("my-organization", &config).is_ok());
    }

    #[test]
    fn test_validate_organization_name_valid_digits() {
        let config = default_config();
        assert!(validate_organization_name("ns-123", &config).is_ok());
    }

    #[test]
    fn test_validate_organization_name_empty() {
        let config = default_config();
        let err = validate_organization_name("", &config).unwrap_err();
        assert_eq!(err.field, "organization_name");
        assert!(err.constraint.contains("empty"));
    }

    #[test]
    fn test_validate_organization_name_uppercase() {
        let config = default_config();
        let err = validate_organization_name("MyOrganization", &config).unwrap_err();
        assert!(err.constraint.contains("invalid character"));
    }

    #[test]
    fn test_validate_organization_name_starts_with_hyphen() {
        let config = default_config();
        let err = validate_organization_name("-ns", &config).unwrap_err();
        assert!(err.constraint.contains("hyphen"));
    }

    #[test]
    fn test_validate_organization_name_ends_with_hyphen() {
        let config = default_config();
        let err = validate_organization_name("ns-", &config).unwrap_err();
        assert!(err.constraint.contains("hyphen"));
    }

    #[test]
    fn test_validate_organization_name_exactly_at_limit() {
        let config =
            ValidationConfig { max_organization_name_bytes: 10, ..ValidationConfig::default() };
        assert!(validate_organization_name("a234567890", &config).is_ok());
    }

    #[test]
    fn test_validate_organization_name_one_byte_over_limit() {
        let config =
            ValidationConfig { max_organization_name_bytes: 10, ..ValidationConfig::default() };
        let err = validate_organization_name("a2345678901", &config).unwrap_err();
        assert!(err.constraint.contains("exceeds maximum"));
    }

    #[test]
    fn test_validate_organization_name_underscore_rejected() {
        let config = default_config();
        let err = validate_organization_name("my_organization", &config).unwrap_err();
        assert!(err.constraint.contains("invalid character"));
    }

    #[test]
    fn test_validate_organization_name_dot_rejected() {
        let config = default_config();
        let err = validate_organization_name("my.organization", &config).unwrap_err();
        assert!(err.constraint.contains("invalid character"));
    }

    #[test]
    fn test_validate_organization_name_unicode_rejected() {
        let config = default_config();
        let err = validate_organization_name("n\u{00e9}space", &config).unwrap_err();
        assert!(err.constraint.contains("invalid character"));
    }

    #[test]
    fn test_validate_organization_name_control_char_rejected() {
        let config = default_config();
        let err = validate_organization_name("ns\x00name", &config).unwrap_err();
        assert!(err.constraint.contains("invalid character"));
    }

    #[test]
    fn test_validate_organization_name_single_char() {
        let config = default_config();
        assert!(validate_organization_name("a", &config).is_ok());
    }

    // =========================================================================
    // validate_relationship_string tests
    // =========================================================================

    #[test]
    fn test_validate_relationship_valid_resource() {
        let config = default_config();
        assert!(validate_relationship_string("document:123", "resource", &config).is_ok());
    }

    #[test]
    fn test_validate_relationship_valid_subject_with_fragment() {
        let config = default_config();
        assert!(validate_relationship_string("user:456#member", "subject", &config).is_ok());
    }

    #[test]
    fn test_validate_relationship_valid_relation() {
        let config = default_config();
        assert!(validate_relationship_string("viewer", "relation", &config).is_ok());
    }

    #[test]
    fn test_validate_relationship_empty() {
        let config = default_config();
        let err = validate_relationship_string("", "resource", &config).unwrap_err();
        assert_eq!(err.field, "resource");
        assert!(err.constraint.contains("empty"));
    }

    #[test]
    fn test_validate_relationship_over_limit() {
        let config =
            ValidationConfig { max_relationship_string_bytes: 5, ..ValidationConfig::default() };
        let err = validate_relationship_string("toolong", "resource", &config).unwrap_err();
        assert!(err.constraint.contains("exceeds maximum"));
    }

    #[test]
    fn test_validate_relationship_space_rejected() {
        let config = default_config();
        let err = validate_relationship_string("doc 123", "resource", &config).unwrap_err();
        assert!(err.constraint.contains("invalid character"));
    }

    #[test]
    fn test_validate_relationship_unicode_rejected() {
        let config = default_config();
        let err = validate_relationship_string("doc\u{00e9}:123", "resource", &config).unwrap_err();
        assert!(err.constraint.contains("invalid character"));
    }

    // =========================================================================
    // validate_operations_count tests
    // =========================================================================

    #[test]
    fn test_validate_operations_count_valid() {
        let config = default_config();
        assert!(validate_operations_count(1, &config).is_ok());
        assert!(validate_operations_count(500, &config).is_ok());
    }

    #[test]
    fn test_validate_operations_count_zero() {
        let config = default_config();
        let err = validate_operations_count(0, &config).unwrap_err();
        assert!(err.constraint.contains("at least one"));
    }

    #[test]
    fn test_validate_operations_count_exactly_at_limit() {
        let config = default_config();
        assert!(validate_operations_count(config.max_operations_per_write, &config).is_ok());
    }

    #[test]
    fn test_validate_operations_count_over_limit() {
        let config = default_config();
        let err =
            validate_operations_count(config.max_operations_per_write + 1, &config).unwrap_err();
        assert!(err.constraint.contains("exceeds maximum"));
    }

    // =========================================================================
    // validate_batch_payload_bytes tests
    // =========================================================================

    #[test]
    fn test_validate_batch_payload_exactly_at_limit() {
        let config = default_config();
        assert!(validate_batch_payload_bytes(config.max_batch_payload_bytes, &config).is_ok());
    }

    #[test]
    fn test_validate_batch_payload_over_limit() {
        let config = default_config();
        let err =
            validate_batch_payload_bytes(config.max_batch_payload_bytes + 1, &config).unwrap_err();
        assert!(err.constraint.contains("exceeds maximum"));
    }

    #[test]
    fn test_validate_batch_payload_zero() {
        let config = default_config();
        assert!(validate_batch_payload_bytes(0, &config).is_ok());
    }

    // =========================================================================
    // ValidationError display tests
    // =========================================================================

    #[test]
    fn test_validation_error_display() {
        let err = ValidationError { field: "key".to_string(), constraint: "too long".to_string() };
        assert_eq!(err.to_string(), "key: too long");
    }
}
