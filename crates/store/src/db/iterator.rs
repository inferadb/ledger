//! Streaming table iterator with buffered refill.
//!
//! [`TableIterator`] yields `(key, value)` pairs in key order while keeping
//! memory usage at `O(buffer_size)` instead of `O(n)`.

use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
};

use super::{Database, page_providers::CachingReadPageProvider};
use crate::{
    backend::StorageBackend,
    btree::{
        BTree,
        cursor::{Bound, Range},
    },
    error::{PageId, Result},
    page::Page,
    tables::Table,
};

/// Default number of entries to buffer per refill in the streaming iterator.
pub(super) const DEFAULT_BUFFER_SIZE: usize = 1000;

/// Streaming iterator over table entries.
///
/// Yields `(key, value)` pairs in key order using an internal buffer that is
/// lazily refilled from the B-tree. This keeps memory usage at
/// `O(buffer_size)` instead of `O(n)` regardless of table size.
///
/// # Memory characteristics
///
/// The iterator maintains a buffer of at most `buffer_size` entries (default:
/// 1000). When the buffer drains, a new B-tree range scan resumes from the
/// last key seen. For a 1000-entry buffer with 512-byte values, peak memory
/// is approximately 1 MB rather than scaling linearly with table size.
///
/// # Error handling
///
/// The `Iterator` trait implementation silently stops on B-tree errors
/// (returning `None`). Use [`next_entry`](Self::next_entry) for explicit
/// `Result`-based iteration when error handling matters.
pub struct TableIterator<'a, 'db, B: StorageBackend, T: Table> {
    db: &'db Database<B>,
    root: PageId,
    page_cache: &'a RefCell<HashMap<PageId, Page>>,
    start_bytes: Option<Vec<u8>>,
    end_bytes: Option<Vec<u8>>,
    /// Buffered entries awaiting consumption.
    pub(super) buffer: VecDeque<(Vec<u8>, Vec<u8>)>,
    /// The last key returned, used to resume scanning after the buffer drains.
    pub(super) last_key: Option<Vec<u8>>,
    /// True once the B-tree range has been fully consumed.
    pub(super) exhausted: bool,
    /// Maximum entries to fetch per refill.
    pub(super) buffer_size: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, 'db, B: StorageBackend, T: Table> TableIterator<'a, 'db, B, T> {
    pub(super) fn new(
        db: &'db Database<B>,
        root: PageId,
        page_cache: &'a RefCell<HashMap<PageId, Page>>,
    ) -> Result<Self> {
        Self::with_range(db, root, page_cache, None, None)
    }

    pub(super) fn with_range(
        db: &'db Database<B>,
        root: PageId,
        page_cache: &'a RefCell<HashMap<PageId, Page>>,
        start_bytes: Option<Vec<u8>>,
        end_bytes: Option<Vec<u8>>,
    ) -> Result<Self> {
        let mut iter = Self {
            db,
            root,
            page_cache,
            start_bytes,
            end_bytes,
            buffer: VecDeque::with_capacity(DEFAULT_BUFFER_SIZE),
            last_key: None,
            exhausted: false,
            buffer_size: DEFAULT_BUFFER_SIZE,
            _marker: std::marker::PhantomData,
        };

        iter.refill_buffer()?;

        Ok(iter)
    }

    /// Fill the internal buffer with the next batch of entries from the B-tree.
    ///
    /// After the first batch, subsequent refills start from an exclusive bound
    /// just past the last key returned so entries are never duplicated.
    pub(super) fn refill_buffer(&mut self) -> Result<()> {
        if self.exhausted || self.root == 0 {
            self.exhausted = true;
            return Ok(());
        }

        let provider = CachingReadPageProvider { db: self.db, page_cache: self.page_cache };
        let btree = BTree::new(self.root, provider);

        // Determine the lower bound for this scan.
        // On the first call we use the original start_bytes;
        // on subsequent calls we resume just past the last key returned.
        let start_bound: Bound<'_> = if let Some(ref last) = self.last_key {
            Bound::Excluded(last.as_slice())
        } else if let Some(ref start) = self.start_bytes {
            Bound::Included(start.as_slice())
        } else {
            Bound::Unbounded
        };

        let end_bound: Bound<'_> = match &self.end_bytes {
            Some(end) => Bound::Excluded(end.as_slice()),
            None => Bound::Unbounded,
        };

        let range = Range { start: start_bound, end: end_bound };

        let mut btree_iter = btree.range(range)?;
        let mut count = 0;

        while count < self.buffer_size {
            match btree_iter.next_entry()? {
                Some((k, v)) => {
                    self.buffer.push_back((k, v));
                    count += 1;
                },
                None => {
                    self.exhausted = true;
                    break;
                },
            }
        }

        Ok(())
    }

    /// Returns the next entry, returning an explicit `Result`.
    ///
    /// Prefer this over the `Iterator` trait when error handling is needed,
    /// as `Iterator::next` silently converts B-tree errors into `None`.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails while refilling the buffer.
    pub fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if let Some((k, v)) = self.buffer.pop_front() {
            self.last_key = Some(k.clone());
            return Ok(Some((k, v)));
        }

        if self.exhausted {
            return Ok(None);
        }

        self.refill_buffer()?;

        if let Some((k, v)) = self.buffer.pop_front() {
            self.last_key = Some(k.clone());
            Ok(Some((k, v)))
        } else {
            Ok(None)
        }
    }

    /// Collects all remaining entries into a `Vec`.
    pub fn collect_entries(mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        while let Ok(Some(entry)) = self.next_entry() {
            result.push(entry);
        }
        result
    }
}

impl<B: StorageBackend, T: Table> Iterator for TableIterator<'_, '_, B, T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // Silently convert errors to None to satisfy the Iterator trait.
        self.next_entry().ok().flatten()
    }
}
