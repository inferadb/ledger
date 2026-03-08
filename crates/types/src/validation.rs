//! Input validation for gRPC request fields.
//!
//! Provides configurable validation for entity keys, values, organization names,
//! relationship strings, and batch sizes. Used at both the gRPC service boundary
//! (server-side) and in SDK operation constructors (client-side).
//!
//! ## Character Whitelists
//!
//! - Entity keys: `[a-zA-Z0-9:/_.-]` — safe for storage paths and log output.
//! - Organization names: Unicode display names (configurable character limit). No control
//!   characters, no leading/trailing whitespace, no consecutive whitespace.
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

/// Validates an organization name against configured limits.
///
/// Organization names must:
/// - Be non-empty
/// - Not exceed `config.max_organization_name_chars` in Unicode character count
/// - Contain no ASCII control characters (0x00-0x1F, 0x7F)
/// - Have no leading or trailing whitespace
/// - Contain no consecutive whitespace
///
/// Unicode letters, digits, punctuation, and single spaces are all permitted.
///
/// # Errors
///
/// Returns [`ValidationError`] if the name violates any constraint.
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

    let char_count = name.chars().count();
    if char_count > config.max_organization_name_chars {
        return Err(ValidationError {
            field: "organization_name".to_string(),
            constraint: format!(
                "length {} characters exceeds maximum {} characters",
                char_count, config.max_organization_name_chars
            ),
        });
    }

    validate_display_name_whitespace(name, "organization_name", |c| c.is_ascii_control())
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

/// Maximum email length per RFC 5321.
const MAX_EMAIL_LENGTH: usize = 320;

/// Maximum user display name length.
const MAX_USER_NAME_LENGTH: usize = 200;

/// Validates an email address for basic structural correctness.
///
/// Checks that the email:
/// - Is non-empty
/// - Does not exceed 320 characters (RFC 5321 limit)
/// - Contains exactly one `@` separator
/// - Has non-empty local and domain parts
/// - Domain contains at least one `.`
///
/// This is intentionally NOT a full RFC 5322 parser — it catches obvious
/// structural problems while deferring to verification emails for correctness.
///
/// # Errors
///
/// Returns [`ValidationError`] if the email fails any structural check.
pub fn validate_email(email: &str) -> Result<(), ValidationError> {
    if email.is_empty() {
        return Err(ValidationError {
            field: "email".to_string(),
            constraint: "must not be empty".to_string(),
        });
    }

    if email.len() > MAX_EMAIL_LENGTH {
        return Err(ValidationError {
            field: "email".to_string(),
            constraint: format!(
                "length {} bytes exceeds maximum {} bytes",
                email.len(),
                MAX_EMAIL_LENGTH
            ),
        });
    }

    let Some((local, domain)) = email.split_once('@') else {
        return Err(ValidationError {
            field: "email".to_string(),
            constraint: "must contain exactly one '@'".to_string(),
        });
    };

    // If domain contains another '@', the email has multiple '@' signs.
    if domain.contains('@') {
        return Err(ValidationError {
            field: "email".to_string(),
            constraint: "must contain exactly one '@'".to_string(),
        });
    }

    if local.is_empty() {
        return Err(ValidationError {
            field: "email".to_string(),
            constraint: "local part must not be empty".to_string(),
        });
    }

    if domain.is_empty() || !domain.contains('.') {
        return Err(ValidationError {
            field: "email".to_string(),
            constraint: "domain must contain at least one '.'".to_string(),
        });
    }

    Ok(())
}

/// Validates a user display name.
///
/// Checks that the name:
/// - Is non-empty
/// - Does not exceed 200 characters (counted as Unicode chars, not bytes)
/// - Does not contain control characters
/// - Does not have leading, trailing, or consecutive whitespace
///
/// # Errors
///
/// Returns [`ValidationError`] if the name fails any constraint.
pub fn validate_user_name(name: &str) -> Result<(), ValidationError> {
    if name.is_empty() {
        return Err(ValidationError {
            field: "user_name".to_string(),
            constraint: "must not be empty".to_string(),
        });
    }

    let char_count = name.chars().count();
    if char_count > MAX_USER_NAME_LENGTH {
        return Err(ValidationError {
            field: "user_name".to_string(),
            constraint: format!(
                "length {} characters exceeds maximum {} characters",
                char_count, MAX_USER_NAME_LENGTH
            ),
        });
    }

    validate_display_name_whitespace(name, "user_name", char::is_control)
}

/// Validates a display name for whitespace and control character rules.
///
/// Rejects leading/trailing whitespace, consecutive whitespace, and control characters.
/// The `is_forbidden_control` predicate controls which control characters are rejected:
/// `char::is_ascii_control` for organization names, `char::is_control` for user names.
fn validate_display_name_whitespace(
    name: &str,
    field: &str,
    is_forbidden_control: fn(char) -> bool,
) -> Result<(), ValidationError> {
    if name.starts_with(char::is_whitespace) {
        return Err(ValidationError {
            field: field.to_string(),
            constraint: "must not have leading whitespace".to_string(),
        });
    }

    if name.ends_with(char::is_whitespace) {
        return Err(ValidationError {
            field: field.to_string(),
            constraint: "must not have trailing whitespace".to_string(),
        });
    }

    let mut prev_was_whitespace = false;
    for (i, c) in name.chars().enumerate() {
        if is_forbidden_control(c) {
            return Err(ValidationError {
                field: field.to_string(),
                constraint: format!("contains control character at character offset {i}"),
            });
        }
        if c.is_whitespace() {
            if prev_was_whitespace {
                return Err(ValidationError {
                    field: field.to_string(),
                    constraint: format!("contains consecutive whitespace at character offset {i}"),
                });
            }
            prev_was_whitespace = true;
        } else {
            prev_was_whitespace = false;
        }
    }

    Ok(())
}

/// Checks if a character is allowed in entity keys.
fn is_key_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, ':' | '/' | '_' | '.' | '-')
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

    /// Table-driven: validate_key accepts valid keys, rejects invalid ones.
    #[test]
    fn test_validate_key_all_cases() {
        let config = default_config();

        // (input, expected_ok, constraint_substring)
        let cases: Vec<(&str, bool, &str)> = vec![
            // Valid keys
            ("user:123", true, ""),
            ("a-z_A-Z.0-9:/path", true, ""),
            ("a", true, ""),
            // Invalid keys
            ("", false, "empty"),
            ("key\x00value", false, "invalid character"),
            ("key\0", false, "invalid character"),
            ("key_\u{00e9}", false, "invalid character"),
            ("key with space", false, "invalid character"),
            ("key\nvalue", false, "invalid character"),
        ];

        for (input, expected_ok, constraint_sub) in &cases {
            let result = validate_key(input, &config);
            assert_eq!(result.is_ok(), *expected_ok, "validate_key({input:?})");
            if !expected_ok {
                let err = result.unwrap_err();
                assert_eq!(err.field, "key", "field for validate_key({input:?})");
                assert!(
                    err.constraint.contains(constraint_sub),
                    "validate_key({input:?}): {}",
                    err.constraint
                );
            }
        }
    }

    /// Boundary test: key at and over byte limit.
    #[test]
    fn test_validate_key_byte_boundary() {
        let config = ValidationConfig { max_key_bytes: 10, ..ValidationConfig::default() };
        assert!(validate_key("a234567890", &config).is_ok(), "exactly at limit");
        let err = validate_key("a2345678901", &config).unwrap_err();
        assert!(err.constraint.contains("exceeds maximum"));
    }

    /// Table-driven: validate_value accepts any binary content within size limits.
    #[test]
    fn test_validate_value_all_cases() {
        let config = default_config();
        assert!(validate_value(b"", &config).is_ok(), "empty value is valid");

        // All byte values 0..=255 should be accepted
        let data: Vec<u8> = (0..=255).collect();
        assert!(validate_value(&data, &config).is_ok(), "all byte values accepted");

        // Boundary test with custom limit
        let config = ValidationConfig { max_value_bytes: 10, ..ValidationConfig::default() };
        assert!(validate_value(&[0u8; 10], &config).is_ok(), "exactly at limit");
        let err = validate_value(&[0u8; 11], &config).unwrap_err();
        assert_eq!(err.field, "value");
        assert!(err.constraint.contains("exceeds maximum"));
    }

    /// Table-driven: validate_organization_name accepts valid display names.
    #[test]
    fn test_validate_organization_name_all_cases() {
        let config = default_config();

        // (input, expected_ok, constraint_substring)
        let cases: Vec<(&str, bool, &str)> = vec![
            // Valid names
            ("My Organization", true, ""),
            ("my-organization", true, ""),
            ("Org 123", true, ""),
            ("Microsoft Corporation", true, ""),
            ("OpenAI, LLC.", true, ""),
            ("Soci\u{00e9}t\u{00e9} G\u{00e9}n\u{00e9}rale", true, ""),
            ("\u{30C8}\u{30E8}\u{30BF}", true, ""), // Katakana
            ("\u{041C}\u{043E}\u{0441}\u{043A}\u{0432}\u{0430}", true, ""), // Cyrillic
            ("a", true, ""),
            // Invalid names
            ("", false, "empty"),
            (" org", false, "leading whitespace"),
            ("org ", false, "trailing whitespace"),
            ("my  org", false, "consecutive whitespace"),
            ("org\tname", false, "control character"),
            ("org\nname", false, "control character"),
            ("org\x00name", false, "control character"),
        ];

        for (input, expected_ok, constraint_sub) in &cases {
            let result = validate_organization_name(input, &config);
            assert_eq!(result.is_ok(), *expected_ok, "validate_organization_name({input:?})");
            if !expected_ok {
                let err = result.unwrap_err();
                assert!(
                    err.constraint.contains(constraint_sub),
                    "validate_organization_name({input:?}): {}",
                    err.constraint
                );
            }
        }
    }

    /// Boundary test: organization name char limit (ASCII and multibyte).
    #[test]
    fn test_validate_organization_name_char_boundary() {
        let config =
            ValidationConfig { max_organization_name_chars: 10, ..ValidationConfig::default() };
        assert!(validate_organization_name("a234567890", &config).is_ok());
        let err = validate_organization_name("a2345678901", &config).unwrap_err();
        assert!(err.constraint.contains("exceeds maximum"));
        assert!(err.constraint.contains("characters"));

        // CJK: 3 chars fit, 4 chars don't
        let config =
            ValidationConfig { max_organization_name_chars: 3, ..ValidationConfig::default() };
        assert!(validate_organization_name("\u{6771}\u{4EAC}\u{90FD}", &config).is_ok());
        let err =
            validate_organization_name("\u{6771}\u{4EAC}\u{90FD}\u{5E02}", &config).unwrap_err();
        assert!(err.constraint.contains("4 characters exceeds maximum 3"));
    }

    /// Table-driven: validate_relationship_string with key+# charset.
    #[test]
    fn test_validate_relationship_string_all_cases() {
        let config = default_config();

        let cases: Vec<(&str, &str, bool, &str)> = vec![
            // (input, field_name, expected_ok, constraint_substring)
            ("document:123", "resource", true, ""),
            ("user:456#member", "subject", true, ""),
            ("viewer", "relation", true, ""),
            ("", "resource", false, "empty"),
            ("doc 123", "resource", false, "invalid character"),
            ("doc\u{00e9}:123", "resource", false, "invalid character"),
        ];

        for (input, field, expected_ok, constraint_sub) in &cases {
            let result = validate_relationship_string(input, field, &config);
            assert_eq!(
                result.is_ok(),
                *expected_ok,
                "validate_relationship_string({input:?}, {field})"
            );
            if !expected_ok {
                let err = result.unwrap_err();
                assert!(
                    err.constraint.contains(constraint_sub),
                    "validate_relationship_string({input:?}): {}",
                    err.constraint
                );
            }
        }

        // Boundary test with custom limit
        let config =
            ValidationConfig { max_relationship_string_bytes: 5, ..ValidationConfig::default() };
        let err = validate_relationship_string("toolong", "resource", &config).unwrap_err();
        assert!(err.constraint.contains("exceeds maximum"));
    }

    /// Table-driven: validate_operations_count boundary cases.
    #[test]
    fn test_validate_operations_count_all_cases() {
        let config = default_config();

        let cases: Vec<(usize, bool, &str)> = vec![
            (0, false, "at least one"),
            (1, true, ""),
            (500, true, ""),
            (config.max_operations_per_write, true, ""),
            (config.max_operations_per_write + 1, false, "exceeds maximum"),
        ];

        for (count, expected_ok, constraint_sub) in &cases {
            let result = validate_operations_count(*count, &config);
            assert_eq!(result.is_ok(), *expected_ok, "validate_operations_count({count})");
            if !expected_ok {
                assert!(result.unwrap_err().constraint.contains(constraint_sub));
            }
        }
    }

    /// Table-driven: validate_batch_payload_bytes boundary cases.
    #[test]
    fn test_validate_batch_payload_all_cases() {
        let config = default_config();

        let cases: Vec<(usize, bool)> = vec![
            (0, true),
            (config.max_batch_payload_bytes, true),
            (config.max_batch_payload_bytes + 1, false),
        ];

        for (size, expected_ok) in &cases {
            let result = validate_batch_payload_bytes(*size, &config);
            assert_eq!(result.is_ok(), *expected_ok, "validate_batch_payload_bytes({size})");
            if !expected_ok {
                assert!(result.unwrap_err().constraint.contains("exceeds maximum"));
            }
        }
    }

    /// ValidationError Display format.
    #[test]
    fn test_validation_error_display() {
        let err = ValidationError { field: "key".to_string(), constraint: "too long".to_string() };
        assert_eq!(err.to_string(), "key: too long");
    }

    /// Table-driven: validate_email structural checks.
    #[test]
    fn test_validate_email_all_cases() {
        // (input, expected_ok, constraint_substring)
        let long_local = "a".repeat(310);
        let too_long_email = format!("{long_local}@example.com");

        let cases: Vec<(&str, bool, &str)> = vec![
            // Valid emails
            ("user@example.com", true, ""),
            ("a@b.c", true, ""),
            ("first.last@example.com", true, ""),
            ("user+tag@example.com", true, ""),
            // Invalid emails
            ("", false, "empty"),
            ("userexample.com", false, "@"),
            ("user@@example.com", false, "@"),
            ("@example.com", false, "local"),
            ("user@", false, "domain"),
            ("user@localhost", false, "domain"),
        ];

        for (input, expected_ok, constraint_sub) in &cases {
            let result = validate_email(input);
            assert_eq!(result.is_ok(), *expected_ok, "validate_email({input:?})");
            if !expected_ok {
                assert!(
                    result.unwrap_err().constraint.contains(constraint_sub),
                    "validate_email({input:?})"
                );
            }
        }

        // Separate: too-long email (can't put owned String in static table)
        let err = validate_email(&too_long_email).unwrap_err();
        assert!(err.constraint.contains("exceeds"));
    }

    /// Table-driven: validate_user_name display name checks.
    #[test]
    fn test_validate_user_name_all_cases() {
        // (input, expected_ok, constraint_substring)
        let cases: Vec<(&str, bool, &str)> = vec![
            // Valid names
            ("Alice Smith", true, ""),
            ("A", true, ""),
            ("Société Générale", true, ""),
            // Invalid names
            ("", false, "empty"),
            (" Alice", false, "leading"),
            ("Alice ", false, "trailing"),
            ("Alice  Smith", false, "consecutive"),
            ("Alice\x00Smith", false, "control"),
            ("Alice\tSmith", false, "control"),
            ("Alice\u{00A0} Smith", false, "consecutive whitespace"),
            ("Alice\u{2003}\u{2003}Smith", false, "consecutive whitespace"),
        ];

        for (input, expected_ok, constraint_sub) in &cases {
            let result = validate_user_name(input);
            assert_eq!(result.is_ok(), *expected_ok, "validate_user_name({input:?})");
            if !expected_ok {
                assert!(
                    result.unwrap_err().constraint.contains(constraint_sub),
                    "validate_user_name({input:?})"
                );
            }
        }

        // Boundary: exactly at limit vs over limit
        let at_limit = "a".repeat(200);
        assert!(validate_user_name(&at_limit).is_ok());
        let over_limit = "a".repeat(201);
        assert!(validate_user_name(&over_limit).unwrap_err().constraint.contains("exceeds"));
    }
}
