//! Crash injection utilities for testing crash recovery.
//!
//! Provides a [`CrashInjector`] that tracks crash points during operations and
//! triggers simulated crashes at configured points. Used to verify crash safety
//! of the dual-slot commit protocol and other durability guarantees.
//!
//! # Crash Points
//!
//! The [`CrashPoint`] enum models specific points in the commit protocol
//! where a crash could occur:
//!
//! ```text
//! Write dirty pages → Write secondary slot → Sync → Flip god byte → Sync
//!                  ↑                      ↑      ↑              ↑
//!          BeforeFirstSync     AfterFirstSync  DuringGodByteFlip  AfterSecondSync
//! ```
//!
//! Each crash point represents a different partial state that recovery must handle.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
};

/// Points in the commit protocol where a crash can be injected.
///
/// These correspond to specific windows in the dual-slot commit sequence
/// documented in `persist_state_to_disk`. Each point produces a different
/// on-disk state that recovery must handle correctly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CrashPoint {
    /// Crash after writing dirty pages but before the first sync.
    ///
    /// On-disk state: dirty pages written, secondary slot updated in header,
    /// but nothing synced. On a real filesystem, data may be partially written.
    /// Recovery should use the old primary slot.
    BeforeFirstSync,

    /// Crash after the first sync but before flipping the god byte.
    ///
    /// On-disk state: secondary slot has valid checksum and data, but
    /// the god byte still points to the old primary slot.
    /// Recovery uses old primary slot (secondary is valid but not yet active).
    AfterFirstSync,

    /// Crash during or after the god byte flip, before the second sync.
    ///
    /// On-disk state: god byte may or may not have been flipped. Both slots
    /// have valid checksums. Recovery should work regardless of which slot
    /// the god byte points to.
    DuringGodByteFlip,

    /// Crash after the second sync completes.
    ///
    /// On-disk state: fully committed. New primary slot is active and synced.
    /// This is effectively a clean commit followed by a crash — recovery
    /// should see the latest data.
    AfterSecondSync,

    /// Crash during page writes, before any header update.
    ///
    /// On-disk state: some data pages may be partially written, but the
    /// header is untouched. Recovery uses whatever the last valid commit was.
    DuringPageWrite,
}

/// Tracks crash injection state for deterministic crash simulation.
///
/// The injector counts sync operations and triggers a simulated crash
/// when the configured crash point is reached. This allows tests to
/// precisely control where in the commit sequence a "crash" occurs.
///
/// # Thread Safety
///
/// All state is atomic, making `CrashInjector` safe to share across threads.
#[derive(Debug)]
pub struct CrashInjector {
    /// The crash point to trigger.
    crash_point: CrashPoint,
    /// Number of sync calls observed.
    sync_count: AtomicU32,
    /// Number of header writes observed.
    header_write_count: AtomicU32,
    /// Number of page writes observed.
    page_write_count: AtomicU32,
    /// Whether the crash has been triggered.
    crashed: AtomicBool,
    /// Whether injection is armed (enabled).
    armed: AtomicBool,
}

impl CrashInjector {
    /// Creates a new crash injector targeting the specified crash point.
    pub fn new(crash_point: CrashPoint) -> Arc<Self> {
        Arc::new(Self {
            crash_point,
            sync_count: AtomicU32::new(0),
            header_write_count: AtomicU32::new(0),
            page_write_count: AtomicU32::new(0),
            crashed: AtomicBool::new(false),
            armed: AtomicBool::new(false),
        })
    }

    /// Arms the injector so it will trigger on the next matching operation.
    ///
    /// The injector starts disarmed to allow initial setup operations
    /// (creating the database, writing initial data) without interference.
    pub fn arm(&self) {
        self.sync_count.store(0, Ordering::SeqCst);
        self.header_write_count.store(0, Ordering::SeqCst);
        self.page_write_count.store(0, Ordering::SeqCst);
        self.crashed.store(false, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Disarms the injector.
    pub fn disarm(&self) {
        self.armed.store(false, Ordering::SeqCst);
    }

    /// Checks if the crash has been triggered.
    pub fn has_crashed(&self) -> bool {
        self.crashed.load(Ordering::SeqCst)
    }

    /// Records a sync operation and check if a crash should occur.
    ///
    /// Returns `true` if the crash should be triggered at this point.
    pub fn on_sync(&self) -> bool {
        if !self.armed.load(Ordering::SeqCst) || self.crashed.load(Ordering::SeqCst) {
            return false;
        }

        let count = self.sync_count.fetch_add(1, Ordering::SeqCst);

        let should_crash = match self.crash_point {
            // Crash before first sync: trigger on sync call 0 (the first sync)
            CrashPoint::BeforeFirstSync => count == 0,
            // Crash after first sync but before god byte flip:
            // Let sync 0 succeed, crash on header write after it (handled in on_header_write)
            CrashPoint::AfterFirstSync => false,
            // Crash during god byte flip: let first sync succeed, crash on second sync
            CrashPoint::DuringGodByteFlip => count == 1,
            // Crash after second sync: let both syncs succeed, crash after
            CrashPoint::AfterSecondSync => count == 2,
            CrashPoint::DuringPageWrite => false,
        };

        if should_crash {
            self.crashed.store(true, Ordering::SeqCst);
        }

        should_crash
    }

    /// Records a header write operation and check if a crash should occur.
    ///
    /// Returns `true` if the crash should be triggered at this point.
    pub fn on_header_write(&self) -> bool {
        if !self.armed.load(Ordering::SeqCst) || self.crashed.load(Ordering::SeqCst) {
            return false;
        }

        let count = self.header_write_count.fetch_add(1, Ordering::SeqCst);

        let should_crash = match self.crash_point {
            // After first sync: first header write is the secondary slot update (let it through),
            // second header write is the god byte flip — crash before it completes
            CrashPoint::AfterFirstSync => count == 1,
            _ => false,
        };

        if should_crash {
            self.crashed.store(true, Ordering::SeqCst);
        }

        should_crash
    }

    /// Records a page write operation and check if a crash should occur.
    ///
    /// Returns `true` if the crash should be triggered at this point.
    /// The `page_threshold` parameter controls after how many page writes to crash.
    pub fn on_page_write(&self, page_threshold: u32) -> bool {
        if !self.armed.load(Ordering::SeqCst) || self.crashed.load(Ordering::SeqCst) {
            return false;
        }

        let count = self.page_write_count.fetch_add(1, Ordering::SeqCst);

        let should_crash = match self.crash_point {
            CrashPoint::DuringPageWrite => count >= page_threshold,
            _ => false,
        };

        if should_crash {
            self.crashed.store(true, Ordering::SeqCst);
        }

        should_crash
    }

    /// Returns the configured crash point.
    pub fn crash_point(&self) -> CrashPoint {
        self.crash_point
    }

    /// Returns the number of sync operations observed.
    pub fn sync_count(&self) -> u32 {
        self.sync_count.load(Ordering::SeqCst)
    }

    /// Returns the number of header write operations observed.
    pub fn header_write_count(&self) -> u32 {
        self.header_write_count.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_crash_injector_starts_disarmed() {
        let injector = CrashInjector::new(CrashPoint::BeforeFirstSync);
        assert!(!injector.has_crashed());
        // Should not crash when disarmed
        assert!(!injector.on_sync());
        assert!(!injector.has_crashed());
    }

    #[test]
    fn test_crash_before_first_sync() {
        let injector = CrashInjector::new(CrashPoint::BeforeFirstSync);
        injector.arm();

        // First sync should trigger crash
        assert!(injector.on_sync());
        assert!(injector.has_crashed());

        // Subsequent operations should not trigger again
        assert!(!injector.on_sync());
    }

    #[test]
    fn test_crash_after_first_sync() {
        let injector = CrashInjector::new(CrashPoint::AfterFirstSync);
        injector.arm();

        // First sync should succeed
        assert!(!injector.on_sync());
        // First header write (secondary slot) should succeed
        assert!(!injector.on_header_write());
        // Second header write (god byte flip) should crash
        assert!(injector.on_header_write());
        assert!(injector.has_crashed());
    }

    #[test]
    fn test_crash_during_god_byte_flip() {
        let injector = CrashInjector::new(CrashPoint::DuringGodByteFlip);
        injector.arm();

        // First sync succeeds
        assert!(!injector.on_sync());
        // Second sync (after god byte flip) crashes
        assert!(injector.on_sync());
        assert!(injector.has_crashed());
    }

    #[test]
    fn test_crash_during_page_write() {
        let injector = CrashInjector::new(CrashPoint::DuringPageWrite);
        injector.arm();

        // Page writes before threshold succeed
        assert!(!injector.on_page_write(3));
        assert!(!injector.on_page_write(3));
        assert!(!injector.on_page_write(3));
        // Page write at threshold crashes
        assert!(injector.on_page_write(3));
        assert!(injector.has_crashed());
    }

    #[test]
    fn test_arm_resets_state() {
        let injector = CrashInjector::new(CrashPoint::BeforeFirstSync);
        injector.arm();
        assert!(injector.on_sync());
        assert!(injector.has_crashed());

        // Re-arming resets everything
        injector.arm();
        assert!(!injector.has_crashed());
        assert_eq!(injector.sync_count(), 0);
    }

    #[test]
    fn test_disarm_prevents_crash() {
        let injector = CrashInjector::new(CrashPoint::BeforeFirstSync);
        injector.arm();
        injector.disarm();

        // Should not crash when disarmed
        assert!(!injector.on_sync());
        assert!(!injector.has_crashed());
    }
}
