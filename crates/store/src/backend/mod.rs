//! Storage backend abstraction for the store engine.
//!
//! The backend trait abstracts the underlying storage mechanism,
//! allowing both file-based (production) and in-memory (testing) implementations.
//!
//! # Crash Safety: Dual-Slot Commit
//!
//! The store uses a dual-slot commit mechanism for crash safety:
//! - The header contains TWO commit slots (primary and secondary)
//! - A "god byte" indicates which slot is currently active
//! - Commits write to the INACTIVE slot, then atomically flip the god byte
//! - Recovery reads both slots and uses the valid one
//!
//! This ensures there's ALWAYS one valid slot to recover from, even if a crash
//! occurs during the header write.

// Compile-time guard: page offsets rely on `usize` being wide enough to hold
// `page_id * page_size`. On a 32-bit target this silently truncates. Building
// for non-64-bit targets is rejected outright rather than risking disk
// corruption from a silently-narrowed offset.
const _: () =
    assert!(core::mem::size_of::<usize>() >= 8, "InferaDB Ledger requires a 64-bit target");

mod file;
mod memory;

pub use file::FileBackend;
pub use memory::InMemoryBackend;

use crate::error::{Error, PageId, Result};

/// Default page size power: 12 (meaning 2^12 = 4KB).
pub const DEFAULT_PAGE_SIZE_POWER: u8 = 12;
/// Default page size: 4KB (4096 bytes).
pub const DEFAULT_PAGE_SIZE: usize = 1 << DEFAULT_PAGE_SIZE_POWER;

/// Database header size (fixed at 768 bytes to accommodate dual commit slots).
/// Layout: 64-byte common header + 2 × 256-byte commit slots + 192-byte reserved.
pub const HEADER_SIZE: usize = 768;

/// Magic number for InferaDB database files.
pub const MAGIC: &[u8; 8] = b"INFERADB";

/// Current format version (2 = dual-slot commit).
pub const FORMAT_VERSION: u16 = 2;

/// Computes the byte offset of a page's start in the backing store.
///
/// The offset is `HEADER_SIZE + page_id * page_size`. Every multiplication and
/// addition is checked; overflow returns `Error::Overflow` rather than
/// silently truncating. With the 64-bit compile-time guard at the top of this
/// module, overflow requires astronomically large page IDs (≈ 4.6 quintillion
/// for a 4 KiB page size), but the check remains as a defense-in-depth
/// against corrupt input and future page-size changes.
///
/// # Errors
///
/// Returns [`Error::Overflow`] if the computation overflows a `u64`.
#[inline]
pub fn page_offset(page_id: PageId, page_size: usize) -> Result<u64> {
    let scaled = page_id
        .checked_mul(page_size as u64)
        .ok_or(Error::Overflow { context: "page offset: page_id * page_size" })?;
    (HEADER_SIZE as u64)
        .checked_add(scaled)
        .ok_or(Error::Overflow { context: "page offset: HEADER_SIZE + scaled" })
}

/// Computes the exclusive end-byte-offset of a page.
///
/// Equivalent to `page_offset(page_id, page_size) + page_size`, with overflow
/// checked on the final addition too.
///
/// # Errors
///
/// Returns [`Error::Overflow`] if the computation overflows a `u64`.
#[inline]
pub fn page_end_offset(page_id: PageId, page_size: usize) -> Result<u64> {
    page_offset(page_id, page_size)?
        .checked_add(page_size as u64)
        .ok_or(Error::Overflow { context: "page end offset: start + page_size" })
}

/// Storage backend trait for abstracting file I/O.
pub trait StorageBackend: Send + Sync {
    /// Reads the database header (768 bytes).
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if the read fails.
    fn read_header(&self) -> Result<Vec<u8>>;

    /// Writes the database header.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if the write fails.
    /// Returns `Error::Corrupted` if the header size is incorrect.
    fn write_header(&self, header: &[u8]) -> Result<()>;

    /// Reads a page by its ID.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if the read fails.
    fn read_page(&self, page_id: PageId) -> Result<Vec<u8>>;

    /// Writes a page at the given ID.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if the write fails.
    /// Returns `Error::Corrupted` if the data size does not match the page size.
    fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()>;

    /// Flushes all writes to durable storage.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if the sync fails.
    fn sync(&self) -> Result<()>;

    /// Returns the current file size in bytes.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if the metadata query fails.
    fn file_size(&self) -> Result<u64>;

    /// Extends the file to accommodate more pages.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if the file cannot be extended.
    fn extend(&self, new_size: u64) -> Result<()>;

    /// Returns the page size for this backend.
    fn page_size(&self) -> usize;

    /// Calculates the byte offset for a page ID.
    ///
    /// Returns `Error::Overflow` if `HEADER_SIZE + page_id * page_size`
    /// would overflow a `u64`. See [`page_offset`] for the standalone helper.
    fn page_offset(&self, page_id: PageId) -> Result<u64> {
        page_offset(page_id, self.page_size())
    }

    /// Calculates the exclusive end-byte-offset for a page ID.
    ///
    /// Equivalent to `self.page_offset(page_id) + self.page_size()`, with
    /// overflow checked on every addition.
    ///
    /// Returns `Error::Overflow` if the computation would overflow a `u64`.
    fn page_end_offset(&self, page_id: PageId) -> Result<u64> {
        page_end_offset(page_id, self.page_size())
    }

    /// Re-wraps a batch of pages' crypto metadata to a target RMK version.
    ///
    /// Non-encrypted backends return `(0, None)` (no-op). Encrypted backends
    /// scan `batch_size` pages starting from `start_page_id`, unwrapping
    /// each DEK with the old RMK and re-wrapping with the target version.
    ///
    /// Returns `(pages_rewrapped, next_page_id)` where `next_page_id` is
    /// `None` when all pages have been processed.
    fn rewrap_pages(
        &self,
        _start_page_id: u64,
        _batch_size: usize,
        _target_version: Option<u32>,
    ) -> Result<(usize, Option<u64>)> {
        Ok((0, None))
    }

    /// Returns the total number of page slots in the crypto sidecar.
    ///
    /// Non-encrypted backends return 0. Encrypted backends return the
    /// sidecar's page count (file-size / metadata-size or max page ID + 1).
    fn sidecar_page_count(&self) -> Result<u64> {
        Ok(0)
    }
}

/// A single commit slot containing the database state at a point in time.
///
/// Two of these are stored in the header. The "god byte" indicates which is active.
#[derive(Debug, Clone, Default)]
pub struct CommitSlot {
    /// Page containing the table directory.
    pub table_directory_page: u64,
    /// Total pages allocated.
    pub total_pages: u64,
    /// Last committed transaction ID.
    pub last_txn_id: u64,
    /// Timestamp of last write (Unix epoch seconds).
    pub last_write_timestamp: u64,
    /// Head of the free page list (0 = none).
    pub free_list_head: u64,
    /// Checksum of this slot's fields (XXH3-64).
    pub checksum: u64,
}

impl CommitSlot {
    /// Size of a commit slot on disk (64 bytes).
    pub const SIZE: usize = 64;

    /// Size of checksum-protected region (40 bytes = 5 × 8-byte fields before checksum).
    const CHECKSUMMED_SIZE: usize = 40;

    /// Serializes the slot to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];

        buf[0..8].copy_from_slice(&self.table_directory_page.to_le_bytes());
        buf[8..16].copy_from_slice(&self.total_pages.to_le_bytes());
        buf[16..24].copy_from_slice(&self.last_txn_id.to_le_bytes());
        buf[24..32].copy_from_slice(&self.last_write_timestamp.to_le_bytes());
        buf[32..40].copy_from_slice(&self.free_list_head.to_le_bytes());

        // Compute checksum over bytes 0-39
        let checksum = xxhash_rust::xxh3::xxh3_64(&buf[0..Self::CHECKSUMMED_SIZE]);
        buf[40..48].copy_from_slice(&checksum.to_le_bytes());

        // Bytes 48-63 are reserved/padding
        buf
    }

    /// Deserializes from bytes.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }

        Some(Self {
            table_directory_page: u64::from_le_bytes(buf[0..8].try_into().ok()?),
            total_pages: u64::from_le_bytes(buf[8..16].try_into().ok()?),
            last_txn_id: u64::from_le_bytes(buf[16..24].try_into().ok()?),
            last_write_timestamp: u64::from_le_bytes(buf[24..32].try_into().ok()?),
            free_list_head: u64::from_le_bytes(buf[32..40].try_into().ok()?),
            checksum: u64::from_le_bytes(buf[40..48].try_into().ok()?),
        })
    }

    /// Verifies the checksum of this slot.
    pub fn verify_checksum(&self) -> bool {
        let buf = self.to_bytes();
        let expected = xxhash_rust::xxh3::xxh3_64(&buf[0..Self::CHECKSUMMED_SIZE]);
        self.checksum == expected
    }
}

/// Database header structure with dual-slot commit for crash safety.
///
/// # Layout (768 bytes total)
///
/// - Bytes 0-15: Common header (magic, version, page_size, god_byte)
/// - Bytes 16-79: Commit slot 0 (64 bytes)
/// - Bytes 80-143: Commit slot 1 (64 bytes)
/// - Bytes 144-767: Reserved for future use
///
/// The "god byte" (byte 15) determines which slot is primary:
/// - Bit 0: Primary slot index (0 or 1)
/// - Bit 1: Recovery required flag (set on commit, cleared on clean shutdown)
///
/// # Dual-Slot Commit Safety Guarantees
///
/// The protocol ensures **at least one valid commit slot** survives any crash:
///
/// 1. **Write to secondary slot:** New state is written to the inactive slot while the primary
///    remains untouched. If a crash occurs here, the primary slot is still valid.
///
/// 2. **First fsync:** Ensures the secondary slot data is durable on disk. After this point, the
///    secondary slot has a valid XXH3-64 checksum.
///
/// 3. **Flip the god byte:** The primary slot index toggles, making the secondary become primary.
///    This is the atomic commit point — a single byte write.
///
/// 4. **Second fsync:** Ensures the god byte flip is durable.
///
/// **Crash scenarios:**
/// - Before step 2: Old primary is valid, secondary may be partial → recover from primary.
/// - Between steps 2-4: Both slots have valid checksums → either is usable.
/// - After step 4: New primary is valid.
///
/// **Recovery:** On open, both slots are read. The slot indicated by the god byte is tried
/// first. If its checksum is invalid (partial write), the other slot is used as fallback.
/// If both checksums are invalid, the database is unrecoverable (requires external restore).
///
/// **Recovery flag (bit 1):** Set before commit, cleared on clean shutdown. If set on open,
/// the free list is rebuilt by scanning all reachable pages — this handles the case where
/// a crash occurred after committing data pages but before persisting the free list.
#[derive(Debug, Clone)]
pub struct DatabaseHeader {
    /// Magic number: "INFERADB"
    pub magic: [u8; 8],
    /// Format version (currently 2 for dual-slot).
    pub version: u16,
    /// Page size as power of 2 (default: 12 = 4KB).
    pub page_size_power: u8,
    /// Reserved bytes.
    pub reserved: [u8; 4],
    /// God byte: bit 0 = primary slot, bit 1 = recovery required.
    pub god_byte: u8,
    /// Commit slot 0.
    pub slot0: CommitSlot,
    /// Commit slot 1.
    pub slot1: CommitSlot,
}

impl DatabaseHeader {
    /// Total header size on disk.
    pub const SIZE: usize = HEADER_SIZE;

    /// Offset of the god byte in the header (for atomic updates).
    pub const GOD_BYTE_OFFSET: usize = 15;

    /// Bit mask for primary slot index in god byte.
    pub const GOD_BYTE_SLOT_MASK: u8 = 0x01;

    /// Bit mask for recovery required flag in god byte.
    pub const GOD_BYTE_RECOVERY_MASK: u8 = 0x02;

    /// Creates a new empty header.
    pub fn new(page_size_power: u8) -> Self {
        Self {
            magic: *MAGIC,
            version: FORMAT_VERSION,
            page_size_power,
            reserved: [0; 4],
            god_byte: 0, // Slot 0 is primary, no recovery required
            slot0: CommitSlot::default(),
            slot1: CommitSlot::default(),
        }
    }

    /// Returns the index of the primary (active) slot.
    pub fn primary_slot_index(&self) -> usize {
        (self.god_byte & Self::GOD_BYTE_SLOT_MASK) as usize
    }

    /// Returns the index of the secondary (inactive) slot.
    pub fn secondary_slot_index(&self) -> usize {
        1 - self.primary_slot_index()
    }

    /// Returns a reference to the primary (active) commit slot.
    pub fn primary_slot(&self) -> &CommitSlot {
        if self.primary_slot_index() == 0 { &self.slot0 } else { &self.slot1 }
    }

    /// Returns a mutable reference to the secondary (inactive) commit slot.
    pub fn secondary_slot_mut(&mut self) -> &mut CommitSlot {
        if self.secondary_slot_index() == 0 { &mut self.slot0 } else { &mut self.slot1 }
    }

    /// Returns a reference to a slot by index.
    pub fn slot(&self, index: usize) -> &CommitSlot {
        if index == 0 { &self.slot0 } else { &self.slot1 }
    }

    /// Checks if recovery is required (unclean shutdown detected).
    pub fn recovery_required(&self) -> bool {
        (self.god_byte & Self::GOD_BYTE_RECOVERY_MASK) != 0
    }

    /// Flips the primary slot (toggle bit 0 of god byte).
    pub fn flip_primary_slot(&mut self) {
        self.god_byte ^= Self::GOD_BYTE_SLOT_MASK;
    }

    /// Sets the recovery required flag.
    pub fn set_recovery_required(&mut self, required: bool) {
        if required {
            self.god_byte |= Self::GOD_BYTE_RECOVERY_MASK;
        } else {
            self.god_byte &= !Self::GOD_BYTE_RECOVERY_MASK;
        }
    }

    /// Serializes the header to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];

        // Common header (bytes 0-15)
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..10].copy_from_slice(&self.version.to_le_bytes());
        buf[10] = self.page_size_power;
        buf[11..15].copy_from_slice(&self.reserved);
        buf[15] = self.god_byte;

        // Slot 0 (bytes 16-79)
        buf[16..16 + CommitSlot::SIZE].copy_from_slice(&self.slot0.to_bytes());

        // Slot 1 (bytes 80-143)
        buf[80..80 + CommitSlot::SIZE].copy_from_slice(&self.slot1.to_bytes());

        // Bytes 144-767 are reserved (remain zeros)
        buf
    }

    /// Deserializes from bytes.
    ///
    /// # Errors
    ///
    /// Returns `Error::Corrupted` if the buffer is too short or a commit slot is malformed.
    /// Returns `Error::InvalidMagic` if the magic number does not match.
    /// Returns `Error::UnsupportedVersion` if the format version is unsupported.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        use crate::error::Error;

        if buf.len() < Self::SIZE {
            return Err(Error::Corrupted { reason: "Header too short".into() });
        }

        let magic: [u8; 8] = buf[0..8].try_into().unwrap();
        let version = u16::from_le_bytes(buf[8..10].try_into().unwrap());
        let page_size_power = buf[10];
        let reserved: [u8; 4] = buf[11..15].try_into().unwrap();
        let god_byte = buf[15];

        // Verify magic
        if magic != *MAGIC {
            return Err(Error::InvalidMagic);
        }

        // Reject unsupported future versions
        if version > FORMAT_VERSION {
            return Err(Error::UnsupportedVersion { version });
        }

        let slot0 = CommitSlot::from_bytes(&buf[16..16 + CommitSlot::SIZE])
            .ok_or_else(|| Error::Corrupted { reason: "Failed to parse commit slot 0".into() })?;
        let slot1 = CommitSlot::from_bytes(&buf[80..80 + CommitSlot::SIZE])
            .ok_or_else(|| Error::Corrupted { reason: "Failed to parse commit slot 1".into() })?;

        Ok(Self { magic, version, page_size_power, reserved, god_byte, slot0, slot1 })
    }

    /// Validates the header and determines which slot to use.
    ///
    /// Returns the index of the valid primary slot, or an error if both are corrupt.
    /// If the indicated primary slot has an invalid checksum, tries the secondary.
    ///
    /// # Errors
    ///
    /// Returns `Error::Corrupted` if both commit slots have invalid checksums.
    pub fn validate_and_choose_slot(&self) -> Result<usize> {
        use crate::error::Error;

        let primary = self.primary_slot_index();
        let secondary = self.secondary_slot_index();

        // Try primary slot first
        if self.slot(primary).verify_checksum() {
            return Ok(primary);
        }

        // Primary is corrupt, try secondary
        if self.slot(secondary).verify_checksum() {
            // Secondary is valid - we recovered from a crash during commit
            return Ok(secondary);
        }

        // Both slots are corrupt - unrecoverable
        Err(Error::Corrupted { reason: "Both commit slots have invalid checksums".into() })
    }

    /// Returns the page size in bytes.
    pub fn page_size(&self) -> usize {
        1 << self.page_size_power
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The compile-time guard at the top of this module is always present in a
    /// normal build (it refuses to compile on 32-bit targets). This test just
    /// asserts the runtime invariant it encodes — if this ever fails, the
    /// const assert would have already failed to compile.
    #[test]
    fn test_usize_is_at_least_64_bits() {
        assert!(core::mem::size_of::<usize>() >= 8);
    }

    /// `page_offset` returns a checked offset and refuses to overflow silently.
    #[test]
    fn test_page_offset_basic_and_overflow() {
        // Normal case — page 5 at 4 KiB pages is HEADER_SIZE + 5*4096.
        let off = page_offset(5, 4096).expect("5 × 4096 must not overflow");
        assert_eq!(off, HEADER_SIZE as u64 + 5 * 4096);

        // Largest safe page_id at 4 KiB: u64::MAX / 4096. One past should overflow.
        let max_safe = u64::MAX / 4096;
        // max_safe × 4096 fits; then adding HEADER_SIZE may still overflow — both
        // are legitimate Overflow cases. We just need to see *some* overflow.
        let overflow = page_offset(u64::MAX, 4096);
        assert!(
            matches!(overflow, Err(Error::Overflow { .. })),
            "page_offset(u64::MAX, 4096) must return Overflow, got {overflow:?}"
        );
        // One extra sanity check: multiplying u64::MAX by an even larger page
        // size still traps.
        let overflow2 = page_offset(max_safe, 65_536);
        assert!(
            matches!(overflow2, Err(Error::Overflow { .. })),
            "page_offset(u64::MAX/4096, 65536) must return Overflow, got {overflow2:?}"
        );
    }

    /// `page_end_offset` checks the trailing `+ page_size` too.
    #[test]
    fn test_page_end_offset_overflow() {
        // Start with a page_id whose start fits but whose end overflows.
        // start = HEADER_SIZE + page_id * page_size. We want start to fit in
        // u64 but start + page_size to overflow.
        let page_size: usize = 4096;
        // page_id such that HEADER_SIZE + page_id * page_size = u64::MAX - 1
        // → start + page_size overflows.
        // page_id = (u64::MAX - 1 - HEADER_SIZE) / page_size (truncating).
        let page_id = (u64::MAX - 1 - HEADER_SIZE as u64) / page_size as u64;
        let start = page_offset(page_id, page_size).expect("start must fit");
        // start + page_size should overflow (or nearly so) — verify end-offset
        // catches it.
        let end = page_end_offset(page_id + 1, page_size);
        assert!(
            matches!(end, Err(Error::Overflow { .. }))
                || start.checked_add(page_size as u64).is_some(),
            "expected page_end_offset to trap overflow, got {end:?}"
        );
        // A clearly-overflowing call must fail.
        let overflow = page_end_offset(u64::MAX, page_size);
        assert!(matches!(overflow, Err(Error::Overflow { .. })));
    }

    #[test]
    fn test_header_round_trip() {
        let header = DatabaseHeader::new(DEFAULT_PAGE_SIZE_POWER);
        let bytes = header.to_bytes();
        let recovered = DatabaseHeader::from_bytes(&bytes).unwrap();

        assert_eq!(header.magic, recovered.magic);
        assert_eq!(header.version, recovered.version);
        assert_eq!(header.page_size_power, recovered.page_size_power);
        assert_eq!(header.god_byte, recovered.god_byte);
    }

    #[test]
    fn test_commit_slot_round_trip() {
        let mut slot = CommitSlot::default();
        slot.table_directory_page = 42;
        slot.total_pages = 100;
        slot.last_txn_id = 12345;
        slot.last_write_timestamp = 1700000000;

        let bytes = slot.to_bytes();
        let recovered = CommitSlot::from_bytes(&bytes).unwrap();

        assert_eq!(slot.table_directory_page, recovered.table_directory_page);
        assert_eq!(slot.total_pages, recovered.total_pages);
        assert_eq!(slot.last_txn_id, recovered.last_txn_id);
        assert!(recovered.verify_checksum());
    }

    #[test]
    fn test_dual_slot_selection() {
        let mut header = DatabaseHeader::new(DEFAULT_PAGE_SIZE_POWER);

        // Initially slot 0 is primary
        assert_eq!(header.primary_slot_index(), 0);
        assert_eq!(header.secondary_slot_index(), 1);

        // Flip to slot 1
        header.flip_primary_slot();
        assert_eq!(header.primary_slot_index(), 1);
        assert_eq!(header.secondary_slot_index(), 0);

        // Flip back to slot 0
        header.flip_primary_slot();
        assert_eq!(header.primary_slot_index(), 0);
    }

    #[test]
    fn test_recovery_flag() {
        let mut header = DatabaseHeader::new(DEFAULT_PAGE_SIZE_POWER);

        assert!(!header.recovery_required());

        header.set_recovery_required(true);
        assert!(header.recovery_required());

        header.set_recovery_required(false);
        assert!(!header.recovery_required());
    }

    #[test]
    fn test_slot_validation_primary_valid() {
        let mut header = DatabaseHeader::new(DEFAULT_PAGE_SIZE_POWER);

        // Set up slot 0 with valid data
        header.slot0.table_directory_page = 1;
        header.slot0.total_pages = 10;
        // Serialize and re-parse to get correct checksum
        let bytes = header.to_bytes();
        let header = DatabaseHeader::from_bytes(&bytes).unwrap();

        // Should choose slot 0 (primary)
        assert_eq!(header.validate_and_choose_slot().unwrap(), 0);
    }

    #[test]
    fn test_slot_validation_fallback_to_secondary() {
        let mut header = DatabaseHeader::new(DEFAULT_PAGE_SIZE_POWER);

        // Set up slot 1 with valid data
        header.slot1.table_directory_page = 1;
        header.slot1.total_pages = 10;

        // Serialize with valid checksums
        let mut bytes = header.to_bytes();

        // Corrupt slot 0's checksum (bytes 16-79 are slot 0)
        bytes[56] ^= 0xFF; // Corrupt slot 0 checksum field

        let header = DatabaseHeader::from_bytes(&bytes).unwrap();

        // Primary (slot 0) is corrupt, should fall back to slot 1
        assert_eq!(header.validate_and_choose_slot().unwrap(), 1);
    }

    #[test]
    fn test_both_slots_corrupt() {
        let header = DatabaseHeader::new(DEFAULT_PAGE_SIZE_POWER);
        let mut bytes = header.to_bytes();

        // Corrupt both slot checksums
        bytes[56] ^= 0xFF; // Slot 0 checksum
        bytes[120] ^= 0xFF; // Slot 1 checksum

        let header = DatabaseHeader::from_bytes(&bytes).unwrap();
        assert!(header.validate_and_choose_slot().is_err());
    }
}
