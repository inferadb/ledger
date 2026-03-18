//! Geographic region type for data residency.

use std::fmt;

use serde::{Deserialize, Serialize};

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
    ///
    /// US regions are non-protected by design — no US federal data residency law
    /// comparable to GDPR exists. PII for US-region users replicates to all nodes.
    /// See `docs/operations/data-residency-architecture.md` for operational guidance.
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
