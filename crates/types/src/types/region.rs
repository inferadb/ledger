//! Region identifier for data residency.
//!
//! Regions are arbitrary string identifiers registered dynamically in GLOBAL
//! Raft state. The hardcoded enum has been replaced with a newtype struct so
//! that operators can provision custom regions without code changes.
//!
//! `Region::GLOBAL` is hardcoded as a special case — it always exists, is
//! never protected, and represents the cluster control plane.
//!
//! Compile-time helper constants (`Region::US_EAST_VA`, `Region::IE_EAST_DUBLIN`,
//! ...) preserve the well-known region names from the previous enum encoding.
//! These constants share the exact slug strings that were emitted by the prior
//! `serde(rename = ...)` attributes, so persisted state and wire formats remain
//! compatible.
//!
//! `Region` carries no semantics beyond its slug — the residency contract
//! (`requires_residency`, `retention_days`) lives in the GLOBAL region
//! directory (`_dir:region:{name}` → `RegionDirectoryEntry`) and is consulted
//! via `inferadb_ledger_state::system::lookup_region_residency`. The previously
//! hardcoded `Region::requires_residency()` / `Region::retention_days()`
//! methods were removed (they silently mis-classified custom regions and
//! violated GDPR for any non-built-in EU slug).
//!
//! Internally a `Region` is a `&'static str`. Dynamically registered region
//! names are interned (leaked into the static heap) so the type stays `Copy`.
//! The intern set is bounded in practice by the number of distinct region
//! names ever observed by a node — typically a small handful.

use std::{
    collections::HashSet,
    fmt,
    str::FromStr,
    sync::{Mutex, OnceLock},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Geographic / jurisdictional region for data residency.
///
/// Regions are dynamic string identifiers. `Region::GLOBAL` is always present
/// and represents the cluster control plane (non-PII metadata replicated
/// everywhere). Other regions are registered through the region directory
/// (`_dir:region:{name}` in GLOBAL state).
///
/// # Display
///
/// Formats as the underlying lowercase-hyphenated slug
/// (e.g. `us-east-va`, `ie-east-dublin`, `global`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Region(&'static str);

/// Process-wide intern set for dynamically constructed region names.
fn intern_set() -> &'static Mutex<HashSet<&'static str>> {
    static SET: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    SET.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Returns a `'static` slice for `name`, interning by leaking on first observation.
///
/// SAFETY: a poisoned mutex here means another thread leaked while interning;
/// recovering the inner data is safe because the intern set is monotonic
/// (insert-only, no observable invariants beyond presence).
fn intern(name: &str) -> &'static str {
    let mut set = match intern_set().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(existing) = set.get(name) {
        return existing;
    }
    let leaked: &'static str = Box::leak(name.to_owned().into_boxed_str());
    set.insert(leaked);
    leaked
}

impl Region {
    /// The cluster control-plane region. Always present, never protected.
    pub const GLOBAL: Region = Region("global");

    // ── North America ──────────────────────────────────────────────
    /// US East Coast, Virginia. No federal data residency requirement.
    pub const US_EAST_VA: Region = Region("us-east-va");
    /// US West Coast, Oregon. No federal data residency requirement.
    pub const US_WEST_OR: Region = Region("us-west-or");
    /// Canada Central, Quebec (Montreal). PIPEDA + provincial privacy laws.
    pub const CA_CENTRAL_QC: Region = Region("ca-central-qc");

    // ── South America ──────────────────────────────────────────────
    /// Brazil Southeast, São Paulo. LGPD.
    pub const BR_SOUTHEAST_SP: Region = Region("br-southeast-sp");

    // ── Europe (EU/EEA — GDPR) ────────────────────────────────────
    /// Ireland East, Dublin. GDPR.
    pub const IE_EAST_DUBLIN: Region = Region("ie-east-dublin");
    /// France North, Paris. GDPR.
    pub const FR_NORTH_PARIS: Region = Region("fr-north-paris");
    /// Germany Central, Frankfurt. GDPR.
    pub const DE_CENTRAL_FRANKFURT: Region = Region("de-central-frankfurt");
    /// Sweden East, Stockholm. GDPR.
    pub const SE_EAST_STOCKHOLM: Region = Region("se-east-stockholm");
    /// Italy North, Milan. GDPR.
    pub const IT_NORTH_MILAN: Region = Region("it-north-milan");

    // ── United Kingdom ─────────────────────────────────────────────
    /// United Kingdom South, London. UK GDPR (post-Brexit, separate jurisdiction).
    pub const UK_SOUTH_LONDON: Region = Region("uk-south-london");

    // ── Middle East & Africa ───────────────────────────────────────
    /// Saudi Arabia Central, Riyadh. PDPL (mandatory in-country processing).
    pub const SA_CENTRAL_RIYADH: Region = Region("sa-central-riyadh");
    /// Bahrain Central, Manama. Bahrain PDPA.
    pub const BH_CENTRAL_MANAMA: Region = Region("bh-central-manama");
    /// UAE Central, Dubai. UAE PDPL.
    pub const AE_CENTRAL_DUBAI: Region = Region("ae-central-dubai");
    /// Israel Central, Tel Aviv. Privacy Protection Law.
    pub const IL_CENTRAL_TEL_AVIV: Region = Region("il-central-tel-aviv");
    /// South Africa South, Cape Town. POPIA.
    pub const ZA_SOUTH_CAPE_TOWN: Region = Region("za-south-cape-town");
    /// Nigeria West, Lagos. NDPA 2023 + NITDA.
    pub const NG_WEST_LAGOS: Region = Region("ng-west-lagos");

    // ── Asia Pacific ───────────────────────────────────────────────
    /// Singapore Central. PDPA.
    pub const SG_CENTRAL_SINGAPORE: Region = Region("sg-central-singapore");
    /// Australia East, Sydney. Australian Privacy Act.
    pub const AU_EAST_SYDNEY: Region = Region("au-east-sydney");
    /// Indonesia West, Jakarta. PDP Law (mandatory local storage).
    pub const ID_WEST_JAKARTA: Region = Region("id-west-jakarta");
    /// Japan East, Tokyo. APPI.
    pub const JP_EAST_TOKYO: Region = Region("jp-east-tokyo");
    /// South Korea Central, Seoul. PIPA.
    pub const KR_CENTRAL_SEOUL: Region = Region("kr-central-seoul");
    /// India West, Mumbai. DPDPA 2023.
    pub const IN_WEST_MUMBAI: Region = Region("in-west-mumbai");
    /// Vietnam South, Ho Chi Minh City. Cybersecurity Law + Decree 53/2022.
    pub const VN_SOUTH_HCMC: Region = Region("vn-south-hcmc");

    // ── China ──────────────────────────────────────────────────────
    /// China North, Beijing. PIPL (mandatory in-country storage).
    pub const CN_NORTH_BEIJING: Region = Region("cn-north-beijing");

    /// Constructs a region from a `'static` slug. Intended for compile-time
    /// constants. Use [`Region::new_owned`] for runtime-allocated names.
    pub const fn new(name: &'static str) -> Self {
        Region(name)
    }

    /// Constructs a region by interning a runtime-allocated name into the
    /// process-wide static heap. The intern set is bounded in practice by the
    /// number of distinct regions registered on a node.
    pub fn new_owned(name: impl AsRef<str>) -> Self {
        let n = name.as_ref();
        // Fast path: a bounded set of well-known constants short-circuits the
        // intern lock.
        if let Some(slug) = WELL_KNOWN_SLUGS.iter().copied().find(|s| *s == n) {
            return Region(slug);
        }
        Region(intern(n))
    }

    /// Slug for this region (e.g. `"us-east-va"`).
    ///
    /// Single source of truth for the wire / on-disk representation.
    pub const fn as_str(&self) -> &'static str {
        self.0
    }

    /// Whether this is the cluster control-plane region.
    pub fn is_global(&self) -> bool {
        self.0 == "global"
    }
}

/// Compile-time list of well-known region slugs. Used by [`Region::new_owned`]
/// to short-circuit the intern lock for the common case.
const WELL_KNOWN_SLUGS: &[&str] = &[
    "global",
    "us-east-va",
    "us-west-or",
    "ca-central-qc",
    "br-southeast-sp",
    "ie-east-dublin",
    "fr-north-paris",
    "de-central-frankfurt",
    "se-east-stockholm",
    "it-north-milan",
    "uk-south-london",
    "sa-central-riyadh",
    "bh-central-manama",
    "ae-central-dubai",
    "il-central-tel-aviv",
    "za-south-cape-town",
    "ng-west-lagos",
    "sg-central-singapore",
    "au-east-sydney",
    "id-west-jakarta",
    "jp-east-tokyo",
    "kr-central-seoul",
    "in-west-mumbai",
    "vn-south-hcmc",
    "cn-north-beijing",
];

/// All well-known compile-time region constants in definition order.
///
/// This is a transitional convenience for callers that previously iterated the
/// `Region` enum. New code should consult the region directory (GLOBAL state)
/// instead — that is the authoritative list once dynamic registration lands.
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

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
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
        write!(f, "invalid region: {}", self.input)
    }
}

impl std::error::Error for RegionParseError {}

impl FromStr for Region {
    type Err = RegionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(RegionParseError { input: s.to_owned() });
        }
        // Validate slug shape: lowercase ASCII letters, digits, hyphen.
        if !s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-') {
            return Err(RegionParseError { input: s.to_owned() });
        }
        if s.starts_with('-') || s.ends_with('-') {
            return Err(RegionParseError { input: s.to_owned() });
        }
        Ok(Region::new_owned(s))
    }
}

impl Serialize for Region {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0)
    }
}

impl<'de> Deserialize<'de> for Region {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Region::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl schemars::JsonSchema for Region {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Region")
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        String::json_schema(generator)
    }
}
