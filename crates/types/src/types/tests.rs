//! Tests for core domain types.

use chrono::Utc;

use super::*;
use crate::hash::ZERO_HASH;

#[test]
fn set_condition_type_bytes() {
    assert_eq!(SetCondition::MustNotExist.type_byte(), 0x01);
    assert_eq!(SetCondition::MustExist.type_byte(), 0x02);
    assert_eq!(SetCondition::VersionEquals(1).type_byte(), 0x03);
    assert_eq!(SetCondition::ValueEquals(vec![]).type_byte(), 0x04);
}

// ========================================================================
// BlockHeader Builder Tests (TDD)
// ========================================================================

#[test]
fn block_header_builder_all_fields() {
    let timestamp = Utc::now();
    let header = BlockHeader::builder()
        .height(100)
        .organization(1)
        .vault(2)
        .previous_hash(ZERO_HASH)
        .tx_merkle_root(ZERO_HASH)
        .state_root(ZERO_HASH)
        .timestamp(timestamp)
        .term(5)
        .committed_index(50)
        .build();

    assert_eq!(header.height, 100);
    assert_eq!(header.organization, OrganizationId::new(1));
    assert_eq!(header.vault, VaultId::new(2));
    assert_eq!(header.previous_hash, ZERO_HASH);
    assert_eq!(header.tx_merkle_root, ZERO_HASH);
    assert_eq!(header.state_root, ZERO_HASH);
    assert_eq!(header.timestamp, timestamp);
    assert_eq!(header.term, 5);
    assert_eq!(header.committed_index, 50);
}

#[test]
fn block_header_builder_genesis_block() {
    let header = BlockHeader::builder()
        .height(0) // Genesis
        .organization(1)
        .vault(1)
        .previous_hash(ZERO_HASH) // ZERO_HASH for genesis
        .tx_merkle_root(ZERO_HASH)
        .state_root(ZERO_HASH)
        .timestamp(Utc::now())
        .term(1)
        .committed_index(0)
        .build();

    assert_eq!(header.height, 0);
    assert_eq!(header.previous_hash, ZERO_HASH);
}

// ========================================================================
// Transaction Builder Tests (TDD)
// ========================================================================

#[test]
fn transaction_builder_valid() {
    let timestamp = Utc::now();
    let tx = Transaction::builder()
        .id([1u8; 16])
        .client_id("client-123")
        .sequence(1)
        .operations(vec![Operation::CreateRelationship {
            resource: "doc:1".into(),
            relation: "owner".into(),
            subject: "user:alice".into(),
        }])
        .timestamp(timestamp)
        .build()
        .expect("valid transaction should build");

    assert_eq!(tx.id, [1u8; 16]);
    assert_eq!(tx.client_id, ClientId::new("client-123"));
    assert_eq!(tx.sequence, 1);
    assert_eq!(tx.operations.len(), 1);
    assert_eq!(tx.timestamp, timestamp);
}

#[test]
fn transaction_builder_rejects_empty_operations() {
    let result = Transaction::builder()
        .id([1u8; 16])
        .client_id("client-123")
        .sequence(1)
        .operations(vec![])
        .timestamp(Utc::now())
        .build();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("operation"));
}

#[test]
fn transaction_builder_rejects_zero_sequence() {
    let result = Transaction::builder()
        .id([1u8; 16])
        .client_id("client-123")
        .sequence(0)
        .operations(vec![Operation::CreateRelationship {
            resource: "doc:1".into(),
            relation: "owner".into(),
            subject: "user:alice".into(),
        }])
        .timestamp(Utc::now())
        .build();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("sequence"));
}

#[test]
fn transaction_builder_accepts_str_via_into() {
    // Test that #[builder(into)] allows &str for String fields
    let tx = Transaction::builder()
        .id([2u8; 16])
        .client_id("client-456") // &str should work
        .sequence(1)
        .operations(vec![Operation::SetEntity {
            key: "entity:1".into(),
            value: vec![1, 2, 3],
            condition: None,
            expires_at: None,
        }])
        .timestamp(Utc::now())
        .build()
        .expect("valid transaction with &str");

    assert_eq!(tx.client_id, ClientId::new("client-456"));
}

#[test]
fn transaction_builder_multiple_operations() {
    let tx = Transaction::builder()
        .id([3u8; 16])
        .client_id("client-789")
        .sequence(5)
        .operations(vec![
            Operation::SetEntity {
                key: "entity:1".into(),
                value: vec![1],
                condition: None,
                expires_at: None,
            },
            Operation::CreateRelationship {
                resource: "doc:1".into(),
                relation: "viewer".into(),
                subject: "user:charlie".into(),
            },
            Operation::DeleteEntity { key: "entity:old".into() },
        ])
        .timestamp(Utc::now())
        .build()
        .expect("valid transaction with multiple operations");

    assert_eq!(tx.operations.len(), 3);
}

// ========================================================================
// Newtype Identifier Tests
// ========================================================================

/// Table-driven: all i64 newtype IDs support new/value/display/from/into/serde.
#[test]
fn id_newtypes_core_behavior() {
    // (constructor, value, expected_display, expected_json)
    let org = OrganizationId::new(42);
    assert_eq!(org.value(), 42);
    assert_eq!(format!("{org}"), "org:42");

    let vault = VaultId::new(7);
    assert_eq!(vault.value(), 7);
    assert_eq!(format!("{vault}"), "vault:7");

    let user = UserId::new(99);
    assert_eq!(user.value(), 99);
    assert_eq!(format!("{user}"), "user:99");

    let email = UserEmailId::new(3);
    assert_eq!(email.value(), 3);
    assert_eq!(format!("{email}"), "email:3");

    // From/Into i64
    let org2: OrganizationId = 42_i64.into();
    assert_eq!(org2.value(), 42);
    let raw: i64 = VaultId::new(7).into();
    assert_eq!(raw, 7);
    let email2: UserEmailId = 5_i64.into();
    assert_eq!(email2.value(), 5);
    let raw2: i64 = email.into();
    assert_eq!(raw2, 3);

    // Edge cases: negative and zero
    assert_eq!(OrganizationId::new(-1).value(), -1);
    assert_eq!(format!("{}", OrganizationId::new(-1)), "org:-1");
    assert_eq!(OrganizationId::new(0).value(), 0);
    assert_eq!(format!("{}", OrganizationId::new(0)), "org:0");
}

/// Derived traits: Eq, Ord, Hash, Copy, Serialize/Deserialize for ID newtypes.
#[test]
fn id_newtypes_derived_traits() {
    use std::collections::HashMap;

    // Equality
    assert_eq!(OrganizationId::new(1), OrganizationId::new(1));
    assert_ne!(OrganizationId::new(1), OrganizationId::new(2));

    // Ordering
    assert!(OrganizationId::new(1) < OrganizationId::new(2));
    assert!(VaultId::new(10) > VaultId::new(5));

    // HashMap key
    let mut map = HashMap::new();
    map.insert(OrganizationId::new(1), "org-a");
    map.insert(OrganizationId::new(2), "org-b");
    assert_eq!(map.get(&OrganizationId::new(1)), Some(&"org-a"));
    assert_eq!(map.get(&OrganizationId::new(3)), None);

    // Copy
    let id = OrganizationId::new(42);
    let id2 = id;
    assert_eq!(id, id2);

    // Serde round-trip (transparent serialization)
    for (json, expected_org) in [("42", 42_i64), ("7", 7)] {
        let id = OrganizationId::new(expected_org);
        let serialized = serde_json::to_string(&id).unwrap();
        assert_eq!(serialized, json);
        let deserialized: OrganizationId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, id);
    }

    let vault = VaultId::new(7);
    let json = serde_json::to_string(&vault).unwrap();
    assert_eq!(json, "7");
    assert_eq!(serde_json::from_str::<VaultId>(&json).unwrap(), vault);

    let email = UserEmailId::new(3);
    let json = serde_json::to_string(&email).unwrap();
    assert_eq!(json, "3");
    assert_eq!(serde_json::from_str::<UserEmailId>(&json).unwrap(), email);
}

#[test]
fn block_header_builder_with_newtype_ids() {
    let header = BlockHeader::builder()
        .height(1)
        .organization(OrganizationId::new(10))
        .vault(VaultId::new(20))
        .previous_hash(ZERO_HASH)
        .tx_merkle_root(ZERO_HASH)
        .state_root(ZERO_HASH)
        .timestamp(Utc::now())
        .term(1)
        .committed_index(0)
        .build();

    assert_eq!(header.organization, OrganizationId::new(10));
    assert_eq!(header.vault, VaultId::new(20));
}

// ========================================================================
// VaultSlug Tests
// ========================================================================

/// VaultSlug (u64 newtype): core operations and derived traits.
#[test]
fn vault_slug_core_and_traits() {
    use std::collections::HashMap;

    // new/value/display
    let slug = VaultSlug::new(123_456_789);
    assert_eq!(slug.value(), 123_456_789);
    assert_eq!(format!("{}", VaultSlug::new(987_654_321)), "987654321");

    // From/Into u64
    let slug2: VaultSlug = 42_u64.into();
    assert_eq!(slug2.value(), 42);
    let raw: u64 = VaultSlug::new(100).into();
    assert_eq!(raw, 100);

    // FromStr
    let parsed: VaultSlug = "12345".parse().expect("valid u64");
    assert_eq!(parsed.value(), 12345);
    assert!("not_a_number".parse::<VaultSlug>().is_err());

    // Serde round-trip
    let slug3 = VaultSlug::new(42_000_000);
    let json = serde_json::to_string(&slug3).unwrap();
    assert_eq!(json, "42000000");
    assert_eq!(serde_json::from_str::<VaultSlug>(&json).unwrap(), slug3);

    // Equality, Ordering
    assert_eq!(VaultSlug::new(1), VaultSlug::new(1));
    assert_ne!(VaultSlug::new(1), VaultSlug::new(2));
    assert!(VaultSlug::new(1) < VaultSlug::new(2));

    // HashMap key
    let mut map = HashMap::new();
    map.insert(VaultSlug::new(1), "a");
    assert_eq!(map.get(&VaultSlug::new(1)), Some(&"a"));
    assert_eq!(map.get(&VaultSlug::new(3)), None);

    // Copy + zero
    let s = VaultSlug::new(42);
    let s2 = s;
    assert_eq!(s, s2);
    assert_eq!(VaultSlug::new(0).value(), 0);
    assert_eq!(format!("{}", VaultSlug::new(0)), "0");
}

// ========================================================================
// RegionBlock Tests
// ========================================================================

// ========================================================================
// TokenVersion Tests
// ========================================================================

#[test]
fn token_version_increment_produces_sequential_values() {
    let v0 = TokenVersion::default();
    let v1 = v0.increment();
    let v2 = v1.increment();
    assert_eq!(v0.value(), 0);
    assert_eq!(v1.value(), 1);
    assert_eq!(v2.value(), 2);
}

/// TokenVersion: default, display, ordering, From/Into, serde.
#[test]
fn token_version_core_traits() {
    // Default is zero
    assert_eq!(TokenVersion::default().value(), 0);

    // Display uses "v" prefix
    let cases = [(0, "v0"), (5, "v5"), (999, "v999")];
    for (val, expected) in cases {
        assert_eq!(format!("{}", TokenVersion::new(val)), expected);
    }

    // Ordering
    assert!(TokenVersion::new(0) < TokenVersion::new(1));
    assert!(TokenVersion::new(5) > TokenVersion::new(3));

    // From/Into u64
    let v: TokenVersion = 10_u64.into();
    assert_eq!(v.value(), 10);
    let raw: u64 = TokenVersion::new(7).into();
    assert_eq!(raw, 7);

    // Serde round-trip
    let v = TokenVersion::new(42);
    let json = serde_json::to_string(&v).unwrap();
    assert_eq!(json, "42");
    let deserialized: TokenVersion = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, v);
}

#[test]
fn extract_vault_block_selects_correct_height() {
    let org = OrganizationId::new(1);
    let vault = VaultId::new(10);
    let timestamp = Utc::now();

    let entry_h5 = VaultEntry {
        organization: org,
        vault,
        vault_height: 5,
        previous_vault_hash: [0xAA; 32],
        transactions: vec![],
        tx_merkle_root: ZERO_HASH,
        state_root: [0x55; 32],
        organization_slug: OrganizationSlug::new(0),
        vault_slug: VaultSlug::new(0),
    };
    let entry_h6 = VaultEntry {
        organization: org,
        vault,
        vault_height: 6,
        previous_vault_hash: [0xBB; 32],
        transactions: vec![],
        tx_merkle_root: ZERO_HASH,
        state_root: [0x66; 32],
        organization_slug: OrganizationSlug::new(0),
        vault_slug: VaultSlug::new(0),
    };

    let block = RegionBlock {
        region: Region::US_EAST_VA,
        region_height: 100,
        previous_region_hash: ZERO_HASH,
        vault_entries: vec![entry_h5, entry_h6],
        timestamp,
        leader_id: "node-1".into(),
        term: 1,
        committed_index: 100,
    };

    let vb5 = block.extract_vault_block(org, vault, 5).unwrap();
    assert_eq!(vb5.header.height, 5);
    assert_eq!(vb5.header.previous_hash, [0xAA; 32]);
    assert_eq!(vb5.header.state_root, [0x55; 32]);

    let vb6 = block.extract_vault_block(org, vault, 6).unwrap();
    assert_eq!(vb6.header.height, 6);
    assert_eq!(vb6.header.previous_hash, [0xBB; 32]);
    assert_eq!(vb6.header.state_root, [0x66; 32]);

    // Non-existent height returns None
    assert!(block.extract_vault_block(org, vault, 7).is_none());

    // Different vault returns None
    assert!(block.extract_vault_block(org, VaultId::new(99), 5).is_none());
}

// ========================================================================
// Region tests
// ========================================================================

#[test]
fn region_display_from_str_roundtrip() {
    for region in &ALL_REGIONS {
        let display = format!("{region}");
        let parsed: Region = display.parse().unwrap();
        assert_eq!(parsed, *region);
    }
}

#[test]
fn region_from_str_invalid() {
    // After the dynamic-region migration, any well-formed kebab-case slug is
    // accepted. Empty strings, malformed slugs (uppercase, leading hyphen,
    // invalid characters) are rejected.
    let err = "".parse::<Region>().unwrap_err();
    assert_eq!(err.input, "");
    assert_eq!(format!("{err}"), "invalid region: ");
    assert!("-leading".parse::<Region>().is_err());
    assert!("trailing-".parse::<Region>().is_err());
    assert!("Bad_Underscore".parse::<Region>().is_err());
    assert!("space here".parse::<Region>().is_err());
}

#[test]
fn region_from_str_case_sensitive() {
    assert!("GLOBAL".parse::<Region>().is_err());
    assert!("US_EAST_VA".parse::<Region>().is_err());
    assert!("Global".parse::<Region>().is_err());
}

/// Region residency, serde, serialization, and derived traits.
#[test]
fn region_properties() {
    use std::collections::HashMap;

    // Residency: exactly GLOBAL, US_EAST_VA, US_WEST_OR are non-protected
    assert!(!Region::GLOBAL.requires_residency());
    assert!(!Region::US_EAST_VA.requires_residency());
    assert!(!Region::US_WEST_OR.requires_residency());
    let non_protected_count = ALL_REGIONS.iter().filter(|r| !r.requires_residency()).count();
    assert_eq!(non_protected_count, 3, "exactly GLOBAL, US_EAST_VA, US_WEST_OR");

    // Serde JSON: round-trip and matches Display
    for region in &ALL_REGIONS {
        let json = serde_json::to_string(region).unwrap();
        assert_eq!(json, format!("\"{}\"", region));
        let deserialized: Region = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, *region);
    }

    // Postcard round-trip
    for region in &ALL_REGIONS {
        let bytes = crate::encode(region).unwrap();
        let decoded: Region = crate::decode(&bytes).unwrap();
        assert_eq!(decoded, *region);
    }

    // Copy, HashMap key, Ordering
    let a = Region::IE_EAST_DUBLIN;
    let b = a;
    assert_eq!(a, b);

    let mut map = HashMap::new();
    map.insert(Region::US_EAST_VA, "virginia");
    map.insert(Region::IE_EAST_DUBLIN, "dublin");
    assert_eq!(map[&Region::US_EAST_VA], "virginia");

    assert!(Region::GLOBAL < Region::US_EAST_VA);
    assert!(Region::US_EAST_VA < Region::US_WEST_OR);
}

proptest::proptest! {
    /// For any valid Region index, `requires_residency() == true` implies the
    /// region is not GLOBAL, US_EAST_VA, or US_WEST_OR (which have no federal
    /// data residency requirement).
    #[test]
    fn prop_requires_residency_implies_non_global(idx in 0..ALL_REGIONS.len()) {
        let region = ALL_REGIONS[idx];
        if region.requires_residency() {
            proptest::prop_assert!(
                !matches!(region, Region::GLOBAL | Region::US_EAST_VA | Region::US_WEST_OR),
                "Region {:?} claims residency but is GLOBAL/US",
                region
            );
        } else {
            proptest::prop_assert!(
                matches!(region, Region::GLOBAL | Region::US_EAST_VA | Region::US_WEST_OR),
                "Region {:?} claims no residency but is not GLOBAL/US",
                region
            );
        }
    }

    /// Postcard serialization round-trip for any valid Region variant.
    #[test]
    fn prop_region_postcard_roundtrip(idx in 0..ALL_REGIONS.len()) {
        let region = ALL_REGIONS[idx];
        let bytes = crate::encode(&region).unwrap();
        let decoded: Region = crate::decode(&bytes).unwrap();
        proptest::prop_assert_eq!(decoded, region);
    }
}

// ====================================================================
// UserCredentialId tests
// ====================================================================

#[test]
fn user_credential_id_display_prefix() {
    assert_eq!(UserCredentialId::new(42).to_string(), "ucred:42");
}

#[test]
fn user_credential_id_from_i64_roundtrip() {
    let id = UserCredentialId::new(99);
    let raw: i64 = id.into();
    assert_eq!(raw, 99);
    assert_eq!(UserCredentialId::from(raw), id);
}

#[test]
fn user_credential_id_default_is_zero() {
    assert_eq!(UserCredentialId::default().value(), 0);
}

#[test]
fn user_credential_id_parse_from_str() {
    let id: UserCredentialId = "7".parse().unwrap();
    assert_eq!(id.value(), 7);
}

// ====================================================================
// CredentialType tests
// ====================================================================

#[test]
fn credential_type_display() {
    assert_eq!(CredentialType::Passkey.to_string(), "passkey");
    assert_eq!(CredentialType::Totp.to_string(), "totp");
    assert_eq!(CredentialType::RecoveryCode.to_string(), "recovery_code");
}

#[test]
fn credential_type_serde_roundtrip() {
    for ct in [CredentialType::Passkey, CredentialType::Totp, CredentialType::RecoveryCode] {
        let json = serde_json::to_string(&ct).unwrap();
        let decoded: CredentialType = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, ct);
    }
}

#[test]
fn credential_type_serde_snake_case() {
    assert_eq!(serde_json::to_string(&CredentialType::RecoveryCode).unwrap(), "\"recovery_code\"");
}

// ====================================================================
// TotpAlgorithm tests
// ====================================================================

#[test]
fn totp_algorithm_default_is_sha1() {
    assert_eq!(TotpAlgorithm::default(), TotpAlgorithm::Sha1);
}

#[test]
fn totp_algorithm_display() {
    assert_eq!(TotpAlgorithm::Sha1.to_string(), "sha1");
    assert_eq!(TotpAlgorithm::Sha256.to_string(), "sha256");
    assert_eq!(TotpAlgorithm::Sha512.to_string(), "sha512");
}

#[test]
fn totp_algorithm_serde_roundtrip() {
    for alg in [TotpAlgorithm::Sha1, TotpAlgorithm::Sha256, TotpAlgorithm::Sha512] {
        let json = serde_json::to_string(&alg).unwrap();
        let decoded: TotpAlgorithm = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, alg);
    }
}

// ====================================================================
// CredentialData tests
// ====================================================================

#[test]
fn credential_data_type_discriminator() {
    let passkey = CredentialData::Passkey(PasskeyCredential {
        credential_id: vec![1, 2, 3],
        public_key: vec![4, 5, 6],
        sign_count: 0,
        transports: vec!["internal".to_string()],
        backup_eligible: true,
        backup_state: false,
        attestation_format: None,
        aaguid: None,
    });
    assert_eq!(passkey.credential_type(), CredentialType::Passkey);

    let totp = CredentialData::Totp(TotpCredential {
        secret: zeroize::Zeroizing::new(vec![0u8; 20]),
        algorithm: TotpAlgorithm::Sha1,
        digits: 6,
        period: 30,
    });
    assert_eq!(totp.credential_type(), CredentialType::Totp);

    let recovery = CredentialData::RecoveryCode(RecoveryCodeCredential {
        code_hashes: vec![[0u8; 32]],
        total_generated: 10,
    });
    assert_eq!(recovery.credential_type(), CredentialType::RecoveryCode);
}

#[test]
fn credential_data_serde_externally_tagged() {
    let data = CredentialData::Passkey(PasskeyCredential {
        credential_id: vec![1],
        public_key: vec![2],
        sign_count: 5,
        transports: vec![],
        backup_eligible: false,
        backup_state: false,
        attestation_format: Some("packed".to_string()),
        aaguid: None,
    });
    let json = serde_json::to_string(&data).unwrap();
    // Externally-tagged: {"passkey": {...}}
    assert!(json.contains("\"passkey\""));
    let decoded: CredentialData = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, data);
}

#[test]
fn credential_data_postcard_roundtrip() {
    let data = CredentialData::Passkey(PasskeyCredential {
        credential_id: vec![1, 2],
        public_key: vec![3, 4],
        sign_count: 10,
        transports: vec!["internal".to_string()],
        backup_eligible: true,
        backup_state: false,
        attestation_format: None,
        aaguid: None,
    });
    let bytes = crate::encode(&data).unwrap();
    let decoded: CredentialData = crate::decode(&bytes).unwrap();
    assert_eq!(decoded, data);
}

// ====================================================================
// TotpCredential zeroize tests
// ====================================================================

#[test]
fn totp_credential_serde_roundtrip() {
    let cred = TotpCredential {
        secret: zeroize::Zeroizing::new(vec![0xAB; 20]),
        algorithm: TotpAlgorithm::Sha256,
        digits: 8,
        period: 60,
    };
    let json = serde_json::to_string(&cred).unwrap();
    let decoded: TotpCredential = serde_json::from_str(&json).unwrap();
    assert_eq!(&*decoded.secret, &*cred.secret);
    assert_eq!(decoded.algorithm, TotpAlgorithm::Sha256);
    assert_eq!(decoded.digits, 8);
    assert_eq!(decoded.period, 60);
}

#[test]
fn totp_credential_debug_redacts_secret() {
    let cred = TotpCredential {
        secret: zeroize::Zeroizing::new(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        algorithm: TotpAlgorithm::Sha1,
        digits: 6,
        period: 30,
    };
    let debug_output = format!("{cred:?}");
    assert!(debug_output.contains("[REDACTED]"));
    assert!(!debug_output.contains("222")); // 0xDE = 222
    assert!(!debug_output.contains("173")); // 0xAD = 173
    assert!(!debug_output.contains("0xde"));
    assert!(debug_output.contains("algorithm: Sha1"));
}

#[test]
fn totp_credential_defaults() {
    let json = r#"{"secret":[1,2,3]}"#;
    let cred: TotpCredential = serde_json::from_str(json).unwrap();
    assert_eq!(cred.algorithm, TotpAlgorithm::Sha1);
    assert_eq!(cred.digits, 6);
    assert_eq!(cred.period, 30);
}

// ====================================================================
// RecoveryCodeCredential tests
// ====================================================================

#[test]
fn recovery_code_credential_serde_roundtrip() {
    let hash = [0xFFu8; 32];
    let cred = RecoveryCodeCredential { code_hashes: vec![hash], total_generated: 10 };
    let json = serde_json::to_string(&cred).unwrap();
    let decoded: RecoveryCodeCredential = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.code_hashes, vec![hash]);
    assert_eq!(decoded.total_generated, 10);
}

// ====================================================================
// UserCredential tests
// ====================================================================

#[test]
fn user_credential_serde_roundtrip() {
    let cred = UserCredential {
        id: UserCredentialId::new(1),
        user: UserId::new(42),
        credential_type: CredentialType::Passkey,
        credential_data: CredentialData::Passkey(PasskeyCredential {
            credential_id: vec![10, 20],
            public_key: vec![30, 40],
            sign_count: 0,
            transports: vec!["usb".to_string()],
            backup_eligible: false,
            backup_state: false,
            attestation_format: None,
            aaguid: None,
        }),
        name: "YubiKey".to_string(),
        enabled: true,
        created_at: Utc::now(),
        last_used_at: None,
    };
    let json = serde_json::to_string(&cred).unwrap();
    let decoded: UserCredential = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.id, cred.id);
    assert_eq!(decoded.user, cred.user);
    assert_eq!(decoded.credential_type, CredentialType::Passkey);
    assert_eq!(decoded.name, "YubiKey");
    assert!(decoded.enabled);
    assert!(decoded.last_used_at.is_none());
}

// ====================================================================
// PendingTotpChallenge tests
// ====================================================================

#[test]
fn pending_totp_challenge_serde_roundtrip() {
    let challenge = PendingTotpChallenge {
        nonce: [0xAA; 32],
        user: UserId::new(1),
        user_slug: UserSlug::new(12345),
        expires_at: Utc::now(),
        attempts: 0,
        primary_method: PrimaryAuthMethod::EmailCode,
    };
    let json = serde_json::to_string(&challenge).unwrap();
    let decoded: PendingTotpChallenge = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.nonce, [0xAA; 32]);
    assert_eq!(decoded.user, UserId::new(1));
    assert_eq!(decoded.user_slug, UserSlug::new(12345));
    assert_eq!(decoded.attempts, 0);
    assert_eq!(decoded.primary_method, PrimaryAuthMethod::EmailCode);
}

#[test]
fn pending_totp_challenge_postcard_roundtrip() {
    let challenge = PendingTotpChallenge {
        nonce: [0xBB; 32],
        user: UserId::new(5),
        user_slug: UserSlug::new(99999),
        expires_at: Utc::now(),
        attempts: 2,
        primary_method: PrimaryAuthMethod::Passkey,
    };
    let bytes = crate::encode(&challenge).unwrap();
    let decoded: PendingTotpChallenge = crate::decode(&bytes).unwrap();
    assert_eq!(decoded.nonce, challenge.nonce);
    assert_eq!(decoded.user, challenge.user);
    assert_eq!(decoded.attempts, 2);
}

// ====================================================================
// PasskeyCredential tests
// ====================================================================

#[test]
fn passkey_credential_serde_roundtrip() {
    let cred = PasskeyCredential {
        credential_id: vec![1, 2, 3, 4],
        public_key: vec![5, 6, 7, 8],
        sign_count: 42,
        transports: vec!["internal".to_string(), "ble".to_string()],
        backup_eligible: true,
        backup_state: true,
        attestation_format: Some("packed".to_string()),
        aaguid: Some([0x01; 16]),
    };
    let json = serde_json::to_string(&cred).unwrap();
    let decoded: PasskeyCredential = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, cred);
}
