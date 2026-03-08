//! Core type definitions for InferaDB Ledger.
//!
//! Defines identifier types (OrganizationId, OrganizationSlug, VaultId, VaultSlug, etc.),
//! block and transaction structures, operations, and conditions.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::hash::Hash;

// ============================================================================
// Identifier Types
// ============================================================================

/// Generates a newtype wrapper around a numeric type for type-safe identifiers.
///
/// Each generated type provides:
/// - Standard derives: Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord
/// - Serde with `#[serde(transparent)]` for wire format compatibility
/// - `From<inner>` and `Into<inner>` conversions
/// - `Display` with a semantic prefix (e.g., `ns:123`)
/// - `new()` constructor and `value()` accessor
macro_rules! define_id {
    (
        $(#[$meta:meta])*
        $name:ident, $inner:ty, $prefix:expr
    ) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
            Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name($inner);

        impl $name {
            /// Creates a new identifier from a raw value.
            #[inline]
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            /// Returns the raw numeric value.
            #[inline]
            pub const fn value(self) -> $inner {
                self.0
            }
        }

        impl From<$inner> for $name {
            #[inline]
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }

        impl From<$name> for $inner {
            #[inline]
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}:{}", $prefix, self.0)
            }
        }

        impl std::str::FromStr for $name {
            type Err = <$inner as std::str::FromStr>::Err;

            fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
                s.parse::<$inner>().map(Self)
            }
        }
    };
}

define_id!(
    /// Internal sequential identifier for an organization (storage-layer only).
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing
    /// with [`VaultId`], [`UserId`], or [`UserEmailId`]. Never exposed in APIs —
    /// use [`OrganizationSlug`] for external identification.
    ///
    /// # Display
    ///
    /// Formats with `org:` prefix: `org:42`.
    OrganizationId, i64, "org"
);

/// Generates a newtype wrapper around `u64` for Snowflake-based external identifiers.
///
/// Each generated type provides:
/// - Standard derives: Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord
/// - Serde with `#[serde(transparent)]` for wire format compatibility
/// - `From<u64>` and `Into<u64>` conversions
/// - `Display` as raw number (no prefix) for API clarity
/// - `new()` constructor and `value()` accessor
macro_rules! define_slug {
    (
        $(#[$meta:meta])*
        $name:ident
    ) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
            Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            /// Creates a new slug from a raw Snowflake ID value.
            #[inline]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// Returns the raw Snowflake ID value.
            #[inline]
            pub const fn value(self) -> u64 {
                self.0
            }
        }

        impl From<u64> for $name {
            #[inline]
            fn from(value: u64) -> Self {
                Self(value)
            }
        }

        impl From<$name> for u64 {
            #[inline]
            fn from(slug: $name) -> Self {
                slug.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl std::str::FromStr for $name {
            type Err = <u64 as std::str::FromStr>::Err;

            fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
                s.parse::<u64>().map(Self)
            }
        }
    };
}

define_slug!(
    /// Snowflake-generated external identifier for an organization.
    ///
    /// Wraps a `u64` Snowflake ID that is the sole external identifier for
    /// organizations in gRPC APIs and the SDK.
    ///
    /// # Display
    ///
    /// Displays as the raw number: `1234567890`.
    OrganizationSlug
);

define_slug!(
    /// Snowflake-generated external identifier for a vault.
    ///
    /// Wraps a `u64` Snowflake ID that is the sole external identifier for
    /// vaults in gRPC APIs and the SDK.
    ///
    /// # Display
    ///
    /// Displays as the raw number: `1234567890`.
    VaultSlug
);

define_slug!(
    /// Snowflake-generated external identifier for a user.
    ///
    /// Wraps a `u64` Snowflake ID that is the sole external identifier for
    /// users in gRPC APIs and the SDK.
    ///
    /// # Display
    ///
    /// Displays as the raw number: `1234567890`.
    UserSlug
);

define_id!(
    /// Unique identifier for a vault within an organization.
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing
    /// with [`OrganizationId`], [`UserId`], or [`UserEmailId`].
    ///
    /// # Display
    ///
    /// Formats with `vault:` prefix: `vault:7`.
    VaultId, i64, "vault"
);

define_id!(
    /// Unique identifier for a user in the `_system` organization.
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing
    /// with [`OrganizationId`], [`VaultId`], or [`UserEmailId`].
    ///
    /// # Display
    ///
    /// Formats with `user:` prefix: `user:1`.
    UserId, i64, "user"
);

define_id!(
    /// Unique identifier for a user email record.
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing
    /// with [`UserId`], [`OrganizationId`], or [`VaultId`].
    ///
    /// # Display
    ///
    /// Formats with `email:` prefix: `email:42`.
    UserEmailId, i64, "email"
);

define_id!(
    /// Unique identifier for an email verification token.
    ///
    /// Sequential `i64` assigned by the Raft leader from the
    /// `_meta:seq:email_verify` sequence counter.
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing
    /// with [`UserEmailId`], [`UserId`], or other identifier types.
    ///
    /// # Display
    ///
    /// Formats with `verify:` prefix: `verify:42`.
    EmailVerifyTokenId, i64, "verify"
);

define_id!(
    /// Internal sequential identifier for a team within an organization.
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing
    /// with [`OrganizationId`], [`VaultId`], [`UserId`], or other identifiers.
    /// Never exposed in APIs — use [`TeamSlug`] for external identification.
    ///
    /// # Display
    ///
    /// Formats with `team:` prefix: `team:3`.
    TeamId, i64, "team"
);

impl Default for TeamId {
    fn default() -> Self {
        Self::new(0)
    }
}

define_slug!(
    /// Snowflake-generated external identifier for a team.
    ///
    /// Wraps a `u64` Snowflake ID that is the sole external identifier for
    /// teams in gRPC APIs and the SDK.
    ///
    /// # Display
    ///
    /// Displays as the raw number: `1234567890`.
    TeamSlug
);

define_id!(
    /// Internal sequential identifier for an application within an organization.
    ///
    /// Wraps an `i64` with compile-time type safety. Never exposed in APIs —
    /// use [`AppSlug`] for external identification.
    ///
    /// # Display
    ///
    /// Formats with `app:` prefix: `app:5`.
    AppId, i64, "app"
);

impl Default for AppId {
    fn default() -> Self {
        Self::new(0)
    }
}

define_slug!(
    /// Snowflake-generated external identifier for an application.
    ///
    /// Wraps a `u64` Snowflake ID that is the sole external identifier for
    /// applications in gRPC APIs and the SDK.
    ///
    /// # Display
    ///
    /// Displays as the raw number: `1234567890`.
    AppSlug
);

define_id!(
    /// Internal sequential identifier for a client assertion entry.
    ///
    /// Wraps an `i64` with compile-time type safety. Used within the
    /// application's client assertion credential type to identify
    /// individual certificate entries.
    ///
    /// # Display
    ///
    /// Formats with `assertion:` prefix: `assertion:2`.
    ClientAssertionId, i64, "assertion"
);

impl Default for ClientAssertionId {
    fn default() -> Self {
        Self::new(0)
    }
}

// ============================================================================
// Region Type
// ============================================================================

/// Geographic/jurisdictional region for data residency.
///
/// Each variant maps to a data residency jurisdiction. The enum is exhaustive —
/// adding a region is a code change, not a runtime operation — providing
/// compile-time guarantees that every match arm handles every region.
///
/// `Region` is the horizontal scaling unit: each region maps 1:1 to a Raft
/// consensus group.
///
/// # Display
///
/// Formats as lowercase with hyphens: `us-east-va`, `ie-east-dublin`, `global`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    schemars::JsonSchema
)]
#[allow(non_camel_case_types)]
pub enum Region {
    /// Global control plane. Replicated to all nodes in all regions.
    /// Stores non-PII metadata only (org registry, node discovery, sequences).
    #[serde(rename = "global")]
    GLOBAL,

    // ── North America ──────────────────────────────────────────────
    /// US East Coast, Virginia. No federal data residency requirement.
    #[serde(rename = "us-east-va")]
    US_EAST_VA,
    /// US West Coast, Oregon. No federal data residency requirement.
    #[serde(rename = "us-west-or")]
    US_WEST_OR,
    /// Canada Central, Quebec (Montreal). PIPEDA + provincial privacy laws.
    #[serde(rename = "ca-central-qc")]
    CA_CENTRAL_QC,

    // ── South America ──────────────────────────────────────────────
    /// Brazil Southeast, São Paulo. LGPD.
    #[serde(rename = "br-southeast-sp")]
    BR_SOUTHEAST_SP,

    // ── Europe (EU/EEA — GDPR) ────────────────────────────────────
    /// Ireland East, Dublin. GDPR.
    #[serde(rename = "ie-east-dublin")]
    IE_EAST_DUBLIN,
    /// France North, Paris. GDPR.
    #[serde(rename = "fr-north-paris")]
    FR_NORTH_PARIS,
    /// Germany Central, Frankfurt. GDPR.
    #[serde(rename = "de-central-frankfurt")]
    DE_CENTRAL_FRANKFURT,
    /// Sweden East, Stockholm. GDPR.
    #[serde(rename = "se-east-stockholm")]
    SE_EAST_STOCKHOLM,
    /// Italy North, Milan. GDPR.
    #[serde(rename = "it-north-milan")]
    IT_NORTH_MILAN,

    // ── United Kingdom ─────────────────────────────────────────────
    /// United Kingdom South, London. UK GDPR (post-Brexit, separate jurisdiction).
    #[serde(rename = "uk-south-london")]
    UK_SOUTH_LONDON,

    // ── Middle East & Africa ───────────────────────────────────────
    /// Saudi Arabia Central, Riyadh. PDPL (mandatory in-country processing).
    #[serde(rename = "sa-central-riyadh")]
    SA_CENTRAL_RIYADH,
    /// Bahrain Central, Manama. Bahrain PDPA.
    #[serde(rename = "bh-central-manama")]
    BH_CENTRAL_MANAMA,
    /// UAE Central, Dubai. UAE PDPL.
    #[serde(rename = "ae-central-dubai")]
    AE_CENTRAL_DUBAI,
    /// Israel Central, Tel Aviv. Privacy Protection Law.
    #[serde(rename = "il-central-tel-aviv")]
    IL_CENTRAL_TEL_AVIV,
    /// South Africa South, Cape Town. POPIA.
    #[serde(rename = "za-south-cape-town")]
    ZA_SOUTH_CAPE_TOWN,
    /// Nigeria West, Lagos. NDPA 2023 + NITDA.
    #[serde(rename = "ng-west-lagos")]
    NG_WEST_LAGOS,

    // ── Asia Pacific ───────────────────────────────────────────────
    /// Singapore Central. PDPA.
    #[serde(rename = "sg-central-singapore")]
    SG_CENTRAL_SINGAPORE,
    /// Australia East, Sydney. Australian Privacy Act.
    #[serde(rename = "au-east-sydney")]
    AU_EAST_SYDNEY,
    /// Indonesia West, Jakarta. PDP Law (mandatory local storage).
    #[serde(rename = "id-west-jakarta")]
    ID_WEST_JAKARTA,
    /// Japan East, Tokyo. APPI.
    #[serde(rename = "jp-east-tokyo")]
    JP_EAST_TOKYO,
    /// South Korea Central, Seoul. PIPA.
    #[serde(rename = "kr-central-seoul")]
    KR_CENTRAL_SEOUL,
    /// India West, Mumbai. DPDPA 2023.
    #[serde(rename = "in-west-mumbai")]
    IN_WEST_MUMBAI,
    /// Vietnam South, Ho Chi Minh City. Cybersecurity Law + Decree 53/2022.
    #[serde(rename = "vn-south-hcmc")]
    VN_SOUTH_HCMC,

    // ── China ──────────────────────────────────────────────────────
    /// China North, Beijing. PIPL (mandatory in-country storage).
    #[serde(rename = "cn-north-beijing")]
    CN_NORTH_BEIJING,
}

/// All `Region` variants in definition order.
pub const ALL_REGIONS: [Region; 25] = [
    Region::GLOBAL,
    Region::US_EAST_VA,
    Region::US_WEST_OR,
    Region::CA_CENTRAL_QC,
    Region::BR_SOUTHEAST_SP,
    Region::IE_EAST_DUBLIN,
    Region::FR_NORTH_PARIS,
    Region::DE_CENTRAL_FRANKFURT,
    Region::SE_EAST_STOCKHOLM,
    Region::IT_NORTH_MILAN,
    Region::UK_SOUTH_LONDON,
    Region::SA_CENTRAL_RIYADH,
    Region::BH_CENTRAL_MANAMA,
    Region::AE_CENTRAL_DUBAI,
    Region::IL_CENTRAL_TEL_AVIV,
    Region::ZA_SOUTH_CAPE_TOWN,
    Region::NG_WEST_LAGOS,
    Region::SG_CENTRAL_SINGAPORE,
    Region::AU_EAST_SYDNEY,
    Region::ID_WEST_JAKARTA,
    Region::JP_EAST_TOKYO,
    Region::KR_CENTRAL_SEOUL,
    Region::IN_WEST_MUMBAI,
    Region::VN_SOUTH_HCMC,
    Region::CN_NORTH_BEIJING,
];

impl Region {
    /// Lowercase hyphenated string for this region.
    ///
    /// Single source of truth for the string representation. Used by
    /// `Display`, `FromStr`, and serde rename attributes.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::GLOBAL => "global",
            Self::US_EAST_VA => "us-east-va",
            Self::US_WEST_OR => "us-west-or",
            Self::CA_CENTRAL_QC => "ca-central-qc",
            Self::BR_SOUTHEAST_SP => "br-southeast-sp",
            Self::IE_EAST_DUBLIN => "ie-east-dublin",
            Self::FR_NORTH_PARIS => "fr-north-paris",
            Self::DE_CENTRAL_FRANKFURT => "de-central-frankfurt",
            Self::SE_EAST_STOCKHOLM => "se-east-stockholm",
            Self::IT_NORTH_MILAN => "it-north-milan",
            Self::UK_SOUTH_LONDON => "uk-south-london",
            Self::SA_CENTRAL_RIYADH => "sa-central-riyadh",
            Self::BH_CENTRAL_MANAMA => "bh-central-manama",
            Self::AE_CENTRAL_DUBAI => "ae-central-dubai",
            Self::IL_CENTRAL_TEL_AVIV => "il-central-tel-aviv",
            Self::ZA_SOUTH_CAPE_TOWN => "za-south-cape-town",
            Self::NG_WEST_LAGOS => "ng-west-lagos",
            Self::SG_CENTRAL_SINGAPORE => "sg-central-singapore",
            Self::AU_EAST_SYDNEY => "au-east-sydney",
            Self::ID_WEST_JAKARTA => "id-west-jakarta",
            Self::JP_EAST_TOKYO => "jp-east-tokyo",
            Self::KR_CENTRAL_SEOUL => "kr-central-seoul",
            Self::IN_WEST_MUMBAI => "in-west-mumbai",
            Self::VN_SOUTH_HCMC => "vn-south-hcmc",
            Self::CN_NORTH_BEIJING => "cn-north-beijing",
        }
    }

    /// Whether this region requires data residency enforcement.
    ///
    /// Returns `true` for all regions except `GLOBAL`, `US_EAST_VA`, and
    /// `US_WEST_OR`. Protected regions restrict Raft group membership to nodes
    /// tagged with the same region.
    #[inline]
    pub const fn requires_residency(&self) -> bool {
        !matches!(self, Self::GLOBAL | Self::US_EAST_VA | Self::US_WEST_OR)
    }

    /// Soft-delete retention period in days for this region.
    ///
    /// After a user is soft-deleted, their data is retained for this many days
    /// before permanent erasure. EU/GDPR regions use shorter retention periods
    /// to comply with data protection regulations.
    pub const fn retention_days(&self) -> u32 {
        match self {
            // EU/EEA regions: 30-day retention (GDPR compliance)
            Self::IE_EAST_DUBLIN
            | Self::FR_NORTH_PARIS
            | Self::DE_CENTRAL_FRANKFURT
            | Self::SE_EAST_STOCKHOLM
            | Self::IT_NORTH_MILAN => 30,
            // UK: 30-day retention (UK GDPR)
            Self::UK_SOUTH_LONDON => 30,
            // All other regions: 90-day default
            _ => 90,
        }
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error returned when parsing an invalid region string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionParseError {
    /// The invalid input string.
    pub input: String,
}

impl fmt::Display for RegionParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown region: {}", self.input)
    }
}

impl std::error::Error for RegionParseError {}

impl std::str::FromStr for Region {
    type Err = RegionParseError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        ALL_REGIONS
            .iter()
            .find(|r| r.as_str() == s)
            .copied()
            .ok_or_else(|| RegionParseError { input: s.to_owned() })
    }
}

// ============================================================================
// User Types
// ============================================================================

/// User account lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserStatus {
    /// User can authenticate.
    #[default]
    Active,
    /// Pending organization creation (saga in progress).
    PendingOrg,
    /// User cannot authenticate.
    Suspended,
    /// Deletion cascade in progress.
    Deleting,
    /// Tombstone for audit.
    Deleted,
}

/// User authorization role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserRole {
    /// Regular user (default).
    #[default]
    User,
    /// Global service administrator.
    Admin,
}

/// Transaction identifier (16 bytes, typically UUIDv4).
pub type TxId = [u8; 16];

/// Node identifier in the Raft cluster.
pub type NodeId = String;

/// Client identifier for idempotency tracking.
pub type ClientId = String;

// ============================================================================
// Block Structures
// ============================================================================

/// Block header containing cryptographic chain metadata.
///
/// Block headers are hashed with a fixed 148-byte encoding:
/// height (8) + organization (8) + vault (8) + previous_hash (32) + tx_merkle_root (32)
/// + state_root (32) + timestamp_secs (8) + timestamp_nanos (4) + term (8) + committed_index (8)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bon::Builder)]
pub struct BlockHeader {
    /// Block height (0 for genesis).
    pub height: u64,
    /// Organization owning this vault.
    #[builder(into)]
    pub organization: OrganizationId,
    /// Vault identifier within the organization.
    #[builder(into)]
    pub vault: VaultId,
    /// Hash of the previous block (ZERO_HASH for genesis).
    pub previous_hash: Hash,
    /// Merkle root of transactions in this block.
    pub tx_merkle_root: Hash,
    /// State root after applying all transactions.
    pub state_root: Hash,
    /// Block creation timestamp.
    pub timestamp: DateTime<Utc>,
    /// Raft term when this block was committed.
    pub term: u64,
    /// Raft committed index for this block.
    pub committed_index: u64,
}

/// Client-facing block containing a header and transactions for a single vault.
///
/// Clients receive and verify these blocks. Each vault maintains its own
/// independent chain for cryptographic isolation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultBlock {
    /// Block header with chain metadata (includes organization, vault).
    pub header: BlockHeader,
    /// Transactions in this block.
    pub transactions: Vec<Transaction>,
}

impl VaultBlock {
    /// Returns the organization that owns this vault block.
    #[inline]
    pub fn organization(&self) -> OrganizationId {
        self.header.organization
    }

    /// Returns the vault identifier for this block.
    #[inline]
    pub fn vault(&self) -> VaultId {
        self.header.vault
    }

    /// Returns the block height in the vault chain.
    #[inline]
    pub fn height(&self) -> u64 {
        self.header.height
    }
}

/// Internal region block stored on disk, containing entries for multiple vaults.
///
/// Multiple vaults share a single Raft group. Region blocks are the physical
/// unit of Raft replication; clients never see them directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionBlock {
    /// Region this block belongs to.
    pub region: Region,
    /// Monotonic region-level height.
    pub region_height: u64,
    /// Hash linking to previous region block.
    pub previous_region_hash: Hash,
    /// Entries for each vault modified in this block.
    pub vault_entries: Vec<VaultEntry>,
    /// Block creation timestamp.
    pub timestamp: DateTime<Utc>,
    /// Raft leader that committed this block.
    pub leader_id: NodeId,
    /// Raft term when committed.
    pub term: u64,
    /// Raft committed log index.
    pub committed_index: u64,
}

/// Per-vault entry within a RegionBlock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultEntry {
    /// Organization owning this vault.
    pub organization: OrganizationId,
    /// Vault identifier.
    pub vault: VaultId,
    /// Per-vault height (independent of shard height).
    pub vault_height: u64,
    /// Hash of previous vault block.
    pub previous_vault_hash: Hash,
    /// Transactions for this vault.
    pub transactions: Vec<Transaction>,
    /// Merkle root of transactions.
    pub tx_merkle_root: Hash,
    /// State root after applying transactions.
    pub state_root: Hash,
}

/// Accumulated cryptographic commitment for a range of blocks.
///
/// Proves snapshot lineage without requiring full block replay.
/// Enables verification continuity even after transaction body compaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ChainCommitment {
    /// Sequential hash chain of all block headers in range.
    /// Ensures header ordering is preserved and any tampering invalidates chain.
    pub accumulated_header_hash: Hash,

    /// Merkle root of state_roots in range.
    /// Enables O(log n) proofs that a specific state_root was in the range.
    pub state_root_accumulator: Hash,

    /// Start height of this commitment (inclusive).
    /// 0 for genesis, or previous_snapshot_height + 1.
    pub from_height: u64,

    /// End height of this commitment (inclusive).
    /// This is the snapshot's block height.
    pub to_height: u64,
}

impl RegionBlock {
    /// Converts this [`RegionBlock`] to a region-level [`BlockHeader`] for chain commitment
    /// computation.
    ///
    /// Aggregates vault entry Merkle roots into a single header, enabling
    /// [`ChainCommitment`] computation over the region chain for snapshot verification.
    pub fn to_region_header(&self) -> BlockHeader {
        use crate::merkle::merkle_root;

        let (tx_merkle_root, state_root) = if self.vault_entries.is_empty() {
            (crate::EMPTY_HASH, crate::EMPTY_HASH)
        } else {
            let tx_roots: Vec<_> = self.vault_entries.iter().map(|e| e.tx_merkle_root).collect();
            let state_roots: Vec<_> = self.vault_entries.iter().map(|e| e.state_root).collect();
            (merkle_root(&tx_roots), merkle_root(&state_roots))
        };

        BlockHeader {
            height: self.region_height,
            organization: OrganizationId::new(0), // Region-level aggregate, not vault-specific
            vault: VaultId::new(0),
            previous_hash: self.previous_region_hash,
            tx_merkle_root,
            state_root,
            timestamp: self.timestamp,
            term: self.term,
            committed_index: self.committed_index,
        }
    }

    /// Extracts a standalone VaultBlock for client verification.
    ///
    /// Clients verify per-vault chains and never see [`RegionBlock`] directly.
    /// Requires both organization and vault since multiple organizations
    /// can share a region.
    pub fn extract_vault_block(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        vault_height: u64,
    ) -> Option<VaultBlock> {
        self.vault_entries
            .iter()
            .find(|e| {
                e.organization == organization && e.vault == vault && e.vault_height == vault_height
            })
            .map(|e| VaultBlock {
                header: BlockHeader {
                    height: e.vault_height,
                    organization: e.organization,
                    vault: e.vault,
                    previous_hash: e.previous_vault_hash,
                    tx_merkle_root: e.tx_merkle_root,
                    state_root: e.state_root,
                    timestamp: self.timestamp,
                    term: self.term,
                    committed_index: self.committed_index,
                },
                transactions: e.transactions.clone(),
            })
    }
}

// ============================================================================
// Transaction Structures
// ============================================================================

/// Error during transaction validation.
#[derive(Debug, snafu::Snafu)]
#[snafu(visibility(pub))]
pub enum TransactionValidationError {
    /// Actor identifier is empty.
    #[snafu(display("Transaction actor cannot be empty"))]
    EmptyActor,

    /// Operations list is empty.
    #[snafu(display("Transaction must contain at least one operation"))]
    EmptyOperations,

    /// Sequence number must be positive.
    #[snafu(display("Transaction sequence must be positive (got 0)"))]
    ZeroSequence,
}

/// Transaction containing one or more operations.
///
/// Use the builder pattern to construct transactions with validation:
/// ```no_run
/// # use inferadb_ledger_types::types::{Transaction, Operation};
/// # use chrono::Utc;
/// let tx = Transaction::builder()
///     .id([1u8; 16])
///     .client_id("client-123")
///     .sequence(1)
///     .actor("user:alice")
///     .operations(vec![Operation::CreateRelationship {
///         resource: "doc:1".into(),
///         relation: "owner".into(),
///         subject: "user:alice".into(),
///     }])
///     .timestamp(Utc::now())
///     .build()
///     .expect("valid transaction");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    /// Unique transaction identifier.
    pub id: TxId,
    /// Client identifier for idempotency.
    pub client_id: ClientId,
    /// Monotonic sequence number per client.
    pub sequence: u64,
    /// Actor identifier for audit logging (typically a user or service principal).
    pub actor: String,
    /// Operations to apply atomically.
    pub operations: Vec<Operation>,
    /// Transaction submission timestamp.
    pub timestamp: DateTime<Utc>,
}

#[bon::bon]
impl Transaction {
    /// Creates a new transaction with validation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `actor` is empty
    /// - `operations` is empty
    /// - `sequence` is zero
    #[builder]
    pub fn new(
        id: TxId,
        #[builder(into)] client_id: ClientId,
        sequence: u64,
        #[builder(into)] actor: String,
        operations: Vec<Operation>,
        timestamp: DateTime<Utc>,
    ) -> Result<Self, TransactionValidationError> {
        snafu::ensure!(!actor.is_empty(), EmptyActorSnafu);
        snafu::ensure!(!operations.is_empty(), EmptyOperationsSnafu);
        snafu::ensure!(sequence > 0, ZeroSequenceSnafu);

        Ok(Self { id, client_id, sequence, actor, operations, timestamp })
    }
}

/// Mutation operations that can be applied to vault state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    /// Creates a relationship tuple.
    CreateRelationship {
        /// Resource identifier (e.g., "document:123").
        resource: String,
        /// Relation name (e.g., "viewer", "editor").
        relation: String,
        /// Subject identifier (e.g., "user:456").
        subject: String,
    },
    /// Deletes a relationship tuple.
    DeleteRelationship {
        /// Resource identifier.
        resource: String,
        /// Relation name.
        relation: String,
        /// Subject identifier.
        subject: String,
    },
    /// Sets an entity value with optional condition and expiration.
    SetEntity {
        /// Entity key.
        key: String,
        /// Entity value (opaque bytes).
        value: Vec<u8>,
        /// Optional write condition.
        condition: Option<SetCondition>,
        /// Unix timestamp for expiration. A value of 0 means the entry never expires.
        expires_at: Option<u64>,
    },
    /// Deletes an entity.
    DeleteEntity {
        /// Entity key to delete.
        key: String,
    },
    /// Expires an entity (GC-initiated, distinct from DeleteEntity for audit).
    ExpireEntity {
        /// Entity key that expired.
        key: String,
        /// Unix timestamp when expiration occurred.
        expired_at: u64,
    },
}

/// Conditional write predicates for compare-and-swap operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetCondition {
    /// Key must not exist (0x01).
    MustNotExist,
    /// Key must exist (0x02).
    MustExist,
    /// Key version must equal specified value (0x03).
    VersionEquals(u64),
    /// Key value must equal specified bytes (0x04).
    ValueEquals(Vec<u8>),
}

impl SetCondition {
    /// Returns the condition type byte for encoding.
    pub fn type_byte(&self) -> u8 {
        match self {
            SetCondition::MustNotExist => 0x01,
            SetCondition::MustExist => 0x02,
            SetCondition::VersionEquals(_) => 0x03,
            SetCondition::ValueEquals(_) => 0x04,
        }
    }
}

// ============================================================================
// Entity Structures
// ============================================================================

/// Key-value record stored per-vault in the B-tree.
///
/// Each entity has a unique key within its vault, an opaque value,
/// optional TTL expiration, and a monotonic version for optimistic concurrency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entity {
    /// Unique key within the vault, conforming to the validation character whitelist.
    pub key: Vec<u8>,
    /// Opaque value bytes. Interpretation is application-defined.
    pub value: Vec<u8>,
    /// Unix timestamp for expiration. A value of 0 means the entry never expires.
    pub expires_at: u64,
    /// Block height when this entity was last modified.
    pub version: u64,
}

/// Relationship tuple (resource, relation, subject).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relationship {
    /// Resource identifier (e.g., "doc:123").
    pub resource: String,
    /// Relation name (e.g., "viewer").
    pub relation: String,
    /// Subject identifier (e.g., "user:alice").
    pub subject: String,
}

impl Relationship {
    /// Creates a new authorization tuple linking a resource, relation, and subject.
    pub fn new(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Self { resource: resource.into(), relation: relation.into(), subject: subject.into() }
    }

    /// Encodes relationship as a canonical string key.
    pub fn to_key(&self) -> String {
        format!("rel:{}#{}@{}", self.resource, self.relation, self.subject)
    }
}

// ============================================================================
// Vault Health
// ============================================================================

/// Health status of a vault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VaultHealth {
    /// Vault is operating normally.
    Healthy,
    /// Vault has diverged from expected state.
    Diverged {
        /// Expected state root hash.
        expected: Hash,
        /// Computed state root hash.
        computed: Hash,
        /// Height at which divergence was detected.
        at_height: u64,
    },
}

// ============================================================================
// Write Result
// ============================================================================

/// Outcome status of a single operation within a write request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteStatus {
    /// Entity/relationship was created.
    Created,
    /// Entity/relationship already existed (idempotent).
    AlreadyExists,
    /// Entity/relationship was updated.
    Updated,
    /// Entity/relationship was deleted.
    Deleted,
    /// Entity/relationship was not found.
    NotFound,
    /// Precondition failed (for conditional writes).
    /// Contains details about the current state for client-side conflict resolution.
    PreconditionFailed {
        /// The key that failed the condition check.
        key: String,
        /// Current version of the entity (block height when last modified), if it exists.
        current_version: Option<u64>,
        /// Current value of the entity, if it exists.
        current_value: Option<Vec<u8>>,
    },
}

/// Aggregate result of a committed write request, including per-operation statuses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteResult {
    /// Block height where the write was committed.
    pub block_height: u64,
    /// Block hash.
    pub block_hash: Hash,
    /// Status of each operation.
    pub statuses: Vec<WriteStatus>,
}

// ============================================================================
// Raft Node Identifier
// ============================================================================

/// Node identifier in the Raft cluster.
///
/// We use u64 for efficient storage and comparison. The mapping from
/// human-readable node names (e.g., "node-1") to numeric IDs is maintained
/// in the `_system` organization.
pub type LedgerNodeId = u64;

// ============================================================================
// Block Retention Policy
// ============================================================================

/// Block retention mode for storage/compliance trade-off.
///
/// Configurable retention policy determines whether transaction bodies
/// are preserved after snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum BlockRetentionMode {
    /// All transaction bodies preserved indefinitely.
    ///
    /// Suitable for audit and compliance requirements where full history
    /// must remain accessible.
    #[default]
    Full,
    /// Transaction bodies removed for blocks older than `retention_blocks` from tip.
    ///
    /// Headers (`state_root`, `tx_merkle_root`) are preserved for verification.
    /// Suitable for high-volume workloads prioritizing storage efficiency.
    Compacted,
}

/// Block retention policy for a vault.
///
/// Controls how long transaction bodies are preserved vs. compacted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRetentionPolicy {
    /// Retention mode (Full or Compacted).
    pub mode: BlockRetentionMode,
    /// For COMPACTED mode: blocks newer than tip - retention_blocks keep full transactions.
    /// Ignored for FULL mode. Default: 10000 blocks.
    pub retention_blocks: u64,
}

impl Default for BlockRetentionPolicy {
    fn default() -> Self {
        Self { mode: BlockRetentionMode::Full, retention_blocks: 10_000 }
    }
}

// ============================================================================
// Resource Accounting
// ============================================================================

/// Snapshot of per-organization resource consumption.
///
/// Used by the quota checker for enforcement and by operators for
/// capacity planning. All values are point-in-time snapshots from
/// Raft-replicated state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrganizationUsage {
    /// Cumulative estimated storage bytes for this organization.
    ///
    /// Updated on every committed write. Approximate — does not track
    /// exact on-disk overhead.
    pub storage_bytes: u64,
    /// Number of active (non-deleted) vaults in this organization.
    pub vault_count: u32,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::hash::ZERO_HASH;

    #[test]
    fn test_relationship_to_key() {
        let rel = Relationship::new("doc:123", "viewer", "user:alice");
        assert_eq!(rel.to_key(), "rel:doc:123#viewer@user:alice");
    }

    #[test]
    fn test_set_condition_type_bytes() {
        assert_eq!(SetCondition::MustNotExist.type_byte(), 0x01);
        assert_eq!(SetCondition::MustExist.type_byte(), 0x02);
        assert_eq!(SetCondition::VersionEquals(1).type_byte(), 0x03);
        assert_eq!(SetCondition::ValueEquals(vec![]).type_byte(), 0x04);
    }

    // ========================================================================
    // BlockHeader Builder Tests (TDD)
    // ========================================================================

    #[test]
    fn test_block_header_builder_all_fields() {
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
    fn test_block_header_builder_genesis_block() {
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
    fn test_transaction_builder_valid() {
        let timestamp = Utc::now();
        let tx = Transaction::builder()
            .id([1u8; 16])
            .client_id("client-123")
            .sequence(1)
            .actor("user:alice")
            .operations(vec![Operation::CreateRelationship {
                resource: "doc:1".into(),
                relation: "owner".into(),
                subject: "user:alice".into(),
            }])
            .timestamp(timestamp)
            .build()
            .expect("valid transaction should build");

        assert_eq!(tx.id, [1u8; 16]);
        assert_eq!(tx.client_id, "client-123");
        assert_eq!(tx.sequence, 1);
        assert_eq!(tx.actor, "user:alice");
        assert_eq!(tx.operations.len(), 1);
        assert_eq!(tx.timestamp, timestamp);
    }

    #[test]
    fn test_transaction_builder_empty_actor_fails() {
        let result = Transaction::builder()
            .id([1u8; 16])
            .client_id("client-123")
            .sequence(1)
            .actor("")
            .operations(vec![Operation::CreateRelationship {
                resource: "doc:1".into(),
                relation: "owner".into(),
                subject: "user:alice".into(),
            }])
            .timestamp(Utc::now())
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("actor"));
    }

    #[test]
    fn test_transaction_builder_empty_operations_fails() {
        let result = Transaction::builder()
            .id([1u8; 16])
            .client_id("client-123")
            .sequence(1)
            .actor("user:alice")
            .operations(vec![])
            .timestamp(Utc::now())
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("operation"));
    }

    #[test]
    fn test_transaction_builder_zero_sequence_fails() {
        let result = Transaction::builder()
            .id([1u8; 16])
            .client_id("client-123")
            .sequence(0)
            .actor("user:alice")
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
    fn test_transaction_builder_with_into_for_strings() {
        // Test that #[builder(into)] allows &str for String fields
        let tx = Transaction::builder()
            .id([2u8; 16])
            .client_id("client-456") // &str should work
            .sequence(1)
            .actor("user:bob") // &str should work
            .operations(vec![Operation::SetEntity {
                key: "entity:1".into(),
                value: vec![1, 2, 3],
                condition: None,
                expires_at: None,
            }])
            .timestamp(Utc::now())
            .build()
            .expect("valid transaction with &str");

        assert_eq!(tx.client_id, "client-456");
        assert_eq!(tx.actor, "user:bob");
    }

    #[test]
    fn test_transaction_builder_multiple_operations() {
        let tx = Transaction::builder()
            .id([3u8; 16])
            .client_id("client-789")
            .sequence(5)
            .actor("user:charlie")
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
    fn test_id_newtypes_core_behavior() {
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
    fn test_id_newtypes_derived_traits() {
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
    fn test_block_header_builder_with_newtype_ids() {
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

    /// OrganizationUsage is a simple Copy struct.
    #[test]
    fn test_organization_usage() {
        let zero = OrganizationUsage { storage_bytes: 0, vault_count: 0 };
        assert_eq!(zero.storage_bytes, 0);

        let usage = OrganizationUsage { storage_bytes: 1_048_576, vault_count: 5 };
        let copy = usage; // Copy
        assert_eq!(usage, copy);
    }

    // ========================================================================
    // VaultSlug Tests
    // ========================================================================

    /// VaultSlug (u64 newtype): core operations and derived traits.
    #[test]
    fn test_vault_slug_core_and_traits() {
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

    #[test]
    fn test_extract_vault_block_selects_correct_height() {
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
        };
        let entry_h6 = VaultEntry {
            organization: org,
            vault,
            vault_height: 6,
            previous_vault_hash: [0xBB; 32],
            transactions: vec![],
            tx_merkle_root: ZERO_HASH,
            state_root: [0x66; 32],
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
    fn test_region_display_from_str_round_trip() {
        for region in &ALL_REGIONS {
            let display = format!("{region}");
            let parsed: Region = display.parse().unwrap();
            assert_eq!(parsed, *region);
        }
    }

    #[test]
    fn test_region_from_str_invalid() {
        let err = "not-a-region".parse::<Region>().unwrap_err();
        assert_eq!(err.input, "not-a-region");
        assert_eq!(format!("{err}"), "unknown region: not-a-region");
    }

    #[test]
    fn test_region_from_str_case_sensitive() {
        assert!("GLOBAL".parse::<Region>().is_err());
        assert!("US_EAST_VA".parse::<Region>().is_err());
        assert!("Global".parse::<Region>().is_err());
    }

    /// Region residency, serde, serialization, and derived traits.
    #[test]
    fn test_region_properties() {
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
}
