//! File-backed segmented WAL with CRC integrity, rotation, and pre-allocation.
//!
//! Segment files are named `segment-NNNNNNNNN.wal` (9-digit zero-padded index).
//! Each frame is written as:
//! ```text
//! [shard_id:8LE][index:8LE][term:8LE][entry_len:4LE][data:variable][crc32:4LE]
//! ```
//! The CRC32 covers the header (shard_id + index + term + entry_len) and data bytes.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::wal_backend::{CHECKPOINT_SHARD_ID, CheckpointFrame, WalBackend, WalError, WalFrame};

/// Size of the frame header: 8 bytes shard_id + 8 bytes index + 8 bytes term + 4 bytes entry_len.
const HEADER_SIZE: usize = 28;
/// Size of the CRC32 trailer.
const CRC_SIZE: usize = 4;
/// Default maximum segment size in bytes (64 MB).
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
/// Pre-allocation threshold as a fraction of max segment size (75%).
const PREALLOC_THRESHOLD_NUM: u64 = 3;
const PREALLOC_THRESHOLD_DEN: u64 = 4;

/// A single WAL segment file.
struct Segment {
    /// File handle opened for append.
    file: File,
    /// Current write position (bytes written so far).
    write_pos: u64,
    /// Segment index, 1-based.
    index: u64,
}

/// File-backed WAL that writes to append-only segment files with CRC integrity.
///
/// Segments rotate when the active segment exceeds `max_segment_size` bytes.
/// The next segment file is pre-allocated when the current one reaches 75% capacity.
pub struct SegmentedWalBackend {
    /// Directory containing segment files.
    dir: PathBuf,
    /// Currently active segment for writes.
    active: Option<Segment>,
    /// Maximum size of a single segment file in bytes.
    pub max_segment_size: u64,
    /// Index to use for the next segment.
    next_segment_index: u64,
    /// Whether the next segment has been pre-allocated.
    next_preallocated: bool,
    /// Total number of frames written across all segments.
    total_frames_written: u64,
}

impl SegmentedWalBackend {
    /// Creates a new segmented WAL backend rooted at the given directory.
    ///
    /// On open, scans for existing segment files to resume from where we left off.
    pub fn open(dir: &Path) -> Result<Self, WalError> {
        fs::create_dir_all(dir)
            .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;

        let existing = list_segments(dir)?;
        let (next_index, active) = if let Some(&last_index) = existing.last() {
            let path = segment_path(dir, last_index);
            let metadata = fs::metadata(&path)
                .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
            let file = OpenOptions::new()
                .append(true)
                .open(&path)
                .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
            let segment = Segment { file, write_pos: metadata.len(), index: last_index };
            (last_index + 1, Some(segment))
        } else {
            (1, None)
        };

        Ok(Self {
            dir: dir.to_path_buf(),
            active,
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            next_segment_index: next_index,
            next_preallocated: false,
            total_frames_written: 0,
        })
    }

    /// Ensures there is an active segment, creating one if needed.
    fn ensure_active(&mut self) -> Result<(), WalError> {
        if self.active.is_none() {
            self.rotate()?;
        }
        Ok(())
    }

    /// Rotates to a new segment file, closing the current one.
    fn rotate(&mut self) -> Result<(), WalError> {
        let index = self.next_segment_index;
        let path = segment_path(&self.dir, index);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;

        self.active = Some(Segment { file, write_pos: 0, index });
        self.next_segment_index = index + 1;
        self.next_preallocated = false;
        Ok(())
    }

    /// Pre-allocates the next segment file if not already done.
    fn maybe_preallocate(&mut self) -> Result<(), WalError> {
        if self.next_preallocated {
            return Ok(());
        }
        let path = segment_path(&self.dir, self.next_segment_index);
        // Create empty file as pre-allocation hint.
        File::create(&path).map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
        self.next_preallocated = true;
        Ok(())
    }

    /// Writes a single frame to the active segment, returning the number of bytes written.
    fn write_frame(segment: &mut Segment, frame: &WalFrame) -> Result<u64, WalError> {
        let shard_bytes = frame.shard_id.0.to_le_bytes();
        let index_bytes = frame.index.to_le_bytes();
        let term_bytes = frame.term.to_le_bytes();
        let len_bytes = (frame.data.len() as u32).to_le_bytes();

        // Compute CRC over header + data.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&shard_bytes);
        hasher.update(&index_bytes);
        hasher.update(&term_bytes);
        hasher.update(&len_bytes);
        hasher.update(&frame.data);
        let crc = hasher.finalize();

        segment
            .file
            .write_all(&shard_bytes)
            .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
        segment
            .file
            .write_all(&index_bytes)
            .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
        segment
            .file
            .write_all(&term_bytes)
            .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
        segment
            .file
            .write_all(&len_bytes)
            .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
        segment
            .file
            .write_all(&frame.data)
            .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
        segment
            .file
            .write_all(&crc.to_le_bytes())
            .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;

        let written = (HEADER_SIZE + frame.data.len() + CRC_SIZE) as u64;
        segment.write_pos += written;
        Ok(written)
    }
}

impl WalBackend for SegmentedWalBackend {
    fn append(&mut self, frames: &[WalFrame]) -> Result<(), WalError> {
        for frame in frames {
            self.ensure_active()?;

            // Check if we need to rotate before writing.
            if let Some(ref seg) = self.active
                && seg.write_pos >= self.max_segment_size
            {
                self.rotate()?;
            }

            let segment = self.active.as_mut().ok_or(WalError::Io {
                kind: std::io::ErrorKind::Other,
                message: "no active segment after ensure_active".to_string(),
            })?;

            Self::write_frame(segment, frame)?;
            self.total_frames_written += 1;

            // Check pre-allocation threshold.
            if let Some(ref seg) = self.active {
                let threshold =
                    self.max_segment_size * PREALLOC_THRESHOLD_NUM / PREALLOC_THRESHOLD_DEN;
                if seg.write_pos >= threshold {
                    self.maybe_preallocate()?;
                }
            }
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<(), WalError> {
        if let Some(ref seg) = self.active {
            seg.file
                .sync_data()
                .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
        }
        Ok(())
    }

    fn read_frames(&self, _from_offset: u64) -> Result<Vec<WalFrame>, WalError> {
        let segment_indices = list_segments(&self.dir)?;
        let mut frames = Vec::new();

        for seg_index in segment_indices {
            let path = segment_path(&self.dir, seg_index);
            let file = File::open(&path)
                .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
            let mut reader = std::io::BufReader::new(file);
            let mut header_buf = [0u8; HEADER_SIZE];
            let mut byte_offset = 0u64;

            loop {
                match reader.read_exact(&mut header_buf) {
                    Ok(()) => {},
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(WalError::Io { kind: e.kind(), message: e.to_string() }),
                }

                let shard_id =
                    u64::from_le_bytes(header_buf[0..8].try_into().ok().ok_or(WalError::Io {
                        kind: std::io::ErrorKind::InvalidData,
                        message: "failed to read shard_id".to_string(),
                    })?);
                let index =
                    u64::from_le_bytes(header_buf[8..16].try_into().ok().ok_or(WalError::Io {
                        kind: std::io::ErrorKind::InvalidData,
                        message: "failed to read index".to_string(),
                    })?);
                let term =
                    u64::from_le_bytes(header_buf[16..24].try_into().ok().ok_or(WalError::Io {
                        kind: std::io::ErrorKind::InvalidData,
                        message: "failed to read term".to_string(),
                    })?);
                let entry_len =
                    u32::from_le_bytes(header_buf[24..28].try_into().ok().ok_or(WalError::Io {
                        kind: std::io::ErrorKind::InvalidData,
                        message: "failed to read entry_len".to_string(),
                    })?) as usize;

                // Read data bytes.
                let mut data = vec![0u8; entry_len];
                match reader.read_exact(&mut data) {
                    Ok(()) => {},
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(WalError::Io { kind: e.kind(), message: e.to_string() }),
                }

                // Read CRC trailer.
                let mut crc_buf = [0u8; CRC_SIZE];
                match reader.read_exact(&mut crc_buf) {
                    Ok(()) => {},
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(WalError::Io { kind: e.kind(), message: e.to_string() }),
                }
                let stored_crc = u32::from_le_bytes(crc_buf);

                // Verify CRC over header + data.
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&header_buf);
                hasher.update(&data);
                let computed_crc = hasher.finalize();

                if stored_crc != computed_crc {
                    return Err(WalError::Corrupted { offset: byte_offset });
                }

                frames.push(WalFrame {
                    shard_id: crate::types::ShardId(shard_id),
                    index,
                    term,
                    data: Arc::from(data.as_slice()),
                });

                byte_offset += (HEADER_SIZE + entry_len + CRC_SIZE) as u64;
            }
        }

        Ok(frames)
    }

    fn truncate_before(&mut self, offset: u64) -> Result<(), WalError> {
        if offset == 0 {
            return Ok(());
        }

        let segment_indices = list_segments(&self.dir)?;
        let mut remaining = offset;

        for seg_index in segment_indices {
            // Never delete the active segment.
            if let Some(ref active) = self.active
                && active.index == seg_index
            {
                break;
            }

            let frame_count = count_frames_in_segment(&self.dir, seg_index)?;
            if frame_count == 0 || remaining < frame_count {
                // This segment is not fully consumed — stop here.
                break;
            }

            // This segment is entirely within the truncation range — delete it.
            let path = segment_path(&self.dir, seg_index);
            fs::remove_file(&path)
                .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
            remaining -= frame_count;

            if remaining == 0 {
                break;
            }
        }

        Ok(())
    }

    fn write_checkpoint(&mut self, checkpoint: &CheckpointFrame) -> Result<(), WalError> {
        let frame = WalFrame {
            shard_id: CHECKPOINT_SHARD_ID,
            index: 0,
            term: 0,
            data: Arc::from(checkpoint.encode().as_slice()),
        };
        self.append(&[frame])
    }

    fn last_checkpoint(&self) -> Result<Option<CheckpointFrame>, WalError> {
        // Read frames from the last segment first for efficiency, then fall
        // back to earlier segments if needed.
        let segment_indices = list_segments(&self.dir)?;

        for &seg_index in segment_indices.iter().rev() {
            let path = segment_path(&self.dir, seg_index);
            let file = File::open(&path)
                .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
            let mut reader = std::io::BufReader::new(file);
            let mut header_buf = [0u8; HEADER_SIZE];

            // Collect checkpoint frames from this segment, then take the last.
            let mut last_cp: Option<CheckpointFrame> = None;

            loop {
                match reader.read_exact(&mut header_buf) {
                    Ok(()) => {},
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(WalError::Io { kind: e.kind(), message: e.to_string() }),
                }

                let shard_id =
                    u64::from_le_bytes(header_buf[0..8].try_into().ok().ok_or(WalError::Io {
                        kind: std::io::ErrorKind::InvalidData,
                        message: "failed to read shard_id".to_string(),
                    })?);
                let entry_len =
                    u32::from_le_bytes(header_buf[24..28].try_into().ok().ok_or(WalError::Io {
                        kind: std::io::ErrorKind::InvalidData,
                        message: "failed to read entry_len".to_string(),
                    })?) as u64;

                if shard_id == CHECKPOINT_SHARD_ID.0 {
                    // Read data for checkpoint frames.
                    let mut data = vec![0u8; entry_len as usize];
                    match reader.read_exact(&mut data) {
                        Ok(()) => {},
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => {
                            return Err(WalError::Io { kind: e.kind(), message: e.to_string() });
                        },
                    }
                    // Skip CRC — checkpoint lookup doesn't need verification.
                    let skip_crc = CRC_SIZE as u64;
                    std::io::copy(&mut reader.by_ref().take(skip_crc), &mut std::io::sink())
                        .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;

                    if let Some(cp) = CheckpointFrame::decode(&data) {
                        last_cp = Some(cp);
                    }
                } else {
                    // Skip data + CRC for non-checkpoint frames.
                    let skip = entry_len + CRC_SIZE as u64;
                    std::io::copy(&mut reader.by_ref().take(skip), &mut std::io::sink())
                        .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
                }
            }

            if last_cp.is_some() {
                return Ok(last_cp);
            }
        }

        Ok(None)
    }

    fn shred_frames(&mut self, shard_id: crate::types::ShardId) -> Result<u64, WalError> {
        let segment_indices = list_segments(&self.dir)?;
        let mut total_zeroed = 0u64;

        for index in segment_indices {
            let path = segment_path(&self.dir, index);
            let buf = {
                let mut f = File::open(&path)
                    .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
                let mut b = Vec::new();
                f.read_to_end(&mut b)
                    .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
                b
            };

            let mut file = OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;

            let mut pos = 0usize;
            while pos + HEADER_SIZE + CRC_SIZE <= buf.len() {
                let raw_shard_id = match buf[pos..pos + 8].try_into().ok().map(u64::from_le_bytes) {
                    Some(v) => v,
                    None => break,
                };
                let entry_len =
                    match buf[pos + 24..pos + 28].try_into().ok().map(u32::from_le_bytes) {
                        Some(v) => v as usize,
                        None => break,
                    };
                let frame_end = pos + HEADER_SIZE + entry_len + CRC_SIZE;
                if frame_end > buf.len() {
                    break;
                }

                if raw_shard_id == shard_id.0 && entry_len > 0 {
                    let data_offset = (pos + HEADER_SIZE) as u64;
                    file.seek(SeekFrom::Start(data_offset))
                        .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
                    file.write_all(&vec![0u8; entry_len])
                        .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
                    total_zeroed += 1;
                }

                pos = frame_end;
            }
        }

        Ok(total_zeroed)
    }
}

/// Counts the number of valid frames in a segment file.
fn count_frames_in_segment(dir: &Path, index: u64) -> Result<u64, WalError> {
    let path = segment_path(dir, index);
    let file =
        File::open(&path).map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
    let mut reader = std::io::BufReader::new(file);
    let mut header_buf = [0u8; HEADER_SIZE];
    let mut count = 0u64;

    loop {
        match reader.read_exact(&mut header_buf) {
            Ok(()) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(WalError::Io { kind: e.kind(), message: e.to_string() }),
        }
        let entry_len =
            u32::from_le_bytes(header_buf[24..28].try_into().ok().ok_or(WalError::Io {
                kind: std::io::ErrorKind::InvalidData,
                message: "failed to read entry_len".to_string(),
            })?) as u64;
        let skip = entry_len + CRC_SIZE as u64;
        std::io::copy(&mut reader.by_ref().take(skip), &mut std::io::sink())
            .map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
        count += 1;
    }

    Ok(count)
}

/// Builds the path for a segment file with the given index.
fn segment_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("segment-{index:09}.wal"))
}

/// Lists segment indices in the directory, sorted ascending.
fn list_segments(dir: &Path) -> Result<Vec<u64>, WalError> {
    let mut indices = Vec::new();

    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(indices),
        Err(e) => return Err(WalError::Io { kind: e.kind(), message: e.to_string() }),
    };

    for entry in entries {
        let entry = entry.map_err(|e| WalError::Io { kind: e.kind(), message: e.to_string() })?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(index) = parse_segment_name(&name_str) {
            indices.push(index);
        }
    }

    indices.sort_unstable();
    Ok(indices)
}

/// Parses a segment filename like `segment-000000001.wal` and returns the index.
fn parse_segment_name(name: &str) -> Option<u64> {
    let stripped = name.strip_prefix("segment-")?.strip_suffix(".wal")?;
    stripped.parse::<u64>().ok()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::types::ShardId;

    fn frame(shard: u64, data: &[u8]) -> WalFrame {
        WalFrame { shard_id: ShardId(shard), index: 0, term: 0, data: Arc::from(data) }
    }

    // --- append / read_frames ---

    #[test]
    fn test_append_multiple_frames_returns_all_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();

        wal.append(&[frame(1, b"hello"), frame(2, b"world")]).unwrap();
        wal.sync().unwrap();

        let frames = wal.read_frames(0).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].shard_id, ShardId(1));
        assert_eq!(&*frames[0].data, b"hello");
        assert_eq!(frames[1].shard_id, ShardId(2));
        assert_eq!(&*frames[1].data, b"world");
    }

    #[test]
    fn test_read_frames_empty_wal_returns_empty_vec() {
        let dir = tempfile::tempdir().unwrap();
        let wal = SegmentedWalBackend::open(dir.path()).unwrap();

        let frames = wal.read_frames(0).unwrap();
        assert!(frames.is_empty());
    }

    #[test]
    fn test_append_empty_data_frame_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();

        wal.append(&[frame(1, b"")]).unwrap();
        wal.sync().unwrap();

        let frames = wal.read_frames(0).unwrap();
        assert_eq!(frames.len(), 1);
        assert!(frames[0].data.is_empty());
        assert_eq!(frames[0].shard_id, ShardId(1));
    }

    #[test]
    fn test_append_empty_batch_succeeds_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();

        wal.append(&[]).unwrap();
        wal.sync().unwrap();

        let frames = wal.read_frames(0).unwrap();
        assert!(frames.is_empty());
    }

    #[test]
    fn test_append_preserves_index_and_term() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();

        let f =
            WalFrame { shard_id: ShardId(5), index: 42, term: 7, data: Arc::from(b"x".as_slice()) };
        wal.append(&[f]).unwrap();
        wal.sync().unwrap();

        let frames = wal.read_frames(0).unwrap();
        assert_eq!(frames[0].shard_id, ShardId(5));
        assert_eq!(frames[0].index, 42);
        assert_eq!(frames[0].term, 7);
        assert_eq!(&*frames[0].data, b"x");
    }

    // --- reopen / persistence ---

    #[test]
    fn test_reopen_reads_previously_written_frames() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
            wal.append(&[frame(1, b"persistent"), frame(3, b"data")]).unwrap();
            wal.sync().unwrap();
        }

        let wal = SegmentedWalBackend::open(dir.path()).unwrap();
        let frames = wal.read_frames(0).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(&*frames[0].data, b"persistent");
        assert_eq!(frames[1].shard_id, ShardId(3));
        assert_eq!(&*frames[1].data, b"data");
    }

    #[test]
    fn test_reopen_append_extends_existing_data() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
            wal.append(&[frame(1, b"first")]).unwrap();
            wal.sync().unwrap();
        }

        {
            let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
            wal.append(&[frame(1, b"second")]).unwrap();
            wal.sync().unwrap();
        }

        let wal = SegmentedWalBackend::open(dir.path()).unwrap();
        let frames = wal.read_frames(0).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(&*frames[0].data, b"first");
        assert_eq!(&*frames[1].data, b"second");
    }

    // --- CRC corruption detection ---

    #[test]
    fn test_read_frames_corrupted_data_returns_corrupted_error() {
        let dir = tempfile::tempdir().unwrap();

        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
        wal.append(&[frame(1, b"integrity")]).unwrap();
        wal.sync().unwrap();
        drop(wal);

        // Corrupt a data byte in the segment file.
        let seg_path = segment_path(dir.path(), 1);
        let mut contents = fs::read(&seg_path).unwrap();
        assert!(contents.len() > HEADER_SIZE);
        contents[HEADER_SIZE] ^= 0xFF;
        fs::write(&seg_path, &contents).unwrap();

        let wal = SegmentedWalBackend::open(dir.path()).unwrap();
        let result = wal.read_frames(0);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), WalError::Corrupted { .. }),
            "expected Corrupted error"
        );
    }

    #[test]
    fn test_read_frames_corrupted_crc_bytes_returns_corrupted_error() {
        let dir = tempfile::tempdir().unwrap();

        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
        wal.append(&[frame(1, b"checkme")]).unwrap();
        wal.sync().unwrap();
        drop(wal);

        let seg_path = segment_path(dir.path(), 1);
        let mut contents = fs::read(&seg_path).unwrap();

        // Corrupt the CRC trailer (last 4 bytes of the frame).
        let crc_offset = contents.len() - CRC_SIZE;
        contents[crc_offset] ^= 0xFF;
        fs::write(&seg_path, &contents).unwrap();

        let wal = SegmentedWalBackend::open(dir.path()).unwrap();
        let result = wal.read_frames(0);
        assert!(matches!(result.unwrap_err(), WalError::Corrupted { .. }));
    }

    #[test]
    fn test_read_frames_truncated_frame_skips_remainder() {
        let dir = tempfile::tempdir().unwrap();

        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
        wal.append(&[frame(1, b"complete"), frame(1, b"also-complete")]).unwrap();
        wal.sync().unwrap();
        drop(wal);

        // Truncate the segment file to cut the second frame in half.
        let seg_path = segment_path(dir.path(), 1);
        let mut contents = fs::read(&seg_path).unwrap();
        let first_frame_len = HEADER_SIZE + b"complete".len() + CRC_SIZE;
        // Keep first frame intact + partial second frame header.
        contents.truncate(first_frame_len + HEADER_SIZE / 2);
        fs::write(&seg_path, &contents).unwrap();

        let wal = SegmentedWalBackend::open(dir.path()).unwrap();
        let frames = wal.read_frames(0).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(&*frames[0].data, b"complete");
    }

    #[test]
    fn test_read_frames_corrupted_header_returns_corrupted_error() {
        let dir = tempfile::tempdir().unwrap();

        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
        wal.append(&[frame(1, b"header-test")]).unwrap();
        wal.sync().unwrap();
        drop(wal);

        // Corrupt the shard_id bytes (first 8 bytes).
        let seg_path = segment_path(dir.path(), 1);
        let mut contents = fs::read(&seg_path).unwrap();
        contents[0] ^= 0xFF;
        fs::write(&seg_path, &contents).unwrap();

        let wal = SegmentedWalBackend::open(dir.path()).unwrap();
        let result = wal.read_frames(0);
        // Header change means CRC mismatch.
        assert!(matches!(result.unwrap_err(), WalError::Corrupted { .. }));
    }

    // --- segment rotation ---

    #[test]
    fn test_rotation_creates_multiple_segments_all_readable() {
        let dir = tempfile::tempdir().unwrap();

        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
        wal.max_segment_size = 100;

        let num_frames = 20;
        for i in 0..num_frames {
            wal.append(&[frame(1, format!("frame-{i}").as_bytes())]).unwrap();
        }
        wal.sync().unwrap();

        let segments = list_segments(dir.path()).unwrap();
        assert!(segments.len() > 1, "expected multiple segments, got {}", segments.len());

        let frames = wal.read_frames(0).unwrap();
        assert_eq!(frames.len(), num_frames);
        for (i, f) in frames.iter().enumerate() {
            assert_eq!(&*f.data, format!("frame-{i}").as_bytes());
        }
    }

    #[test]
    fn test_rotation_preallocates_next_segment_file() {
        let dir = tempfile::tempdir().unwrap();

        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
        // Set max segment size small enough that writing a few frames triggers pre-allocation.
        wal.max_segment_size = 64;

        // Write enough to exceed 75% threshold.
        for _ in 0..3 {
            wal.append(&[frame(1, b"preallocate-trigger")]).unwrap();
        }
        wal.sync().unwrap();

        let segments = list_segments(dir.path()).unwrap();
        // At least the active segment + one pre-allocated or rotated segment.
        assert!(segments.len() >= 2, "expected pre-allocation, got {} segments", segments.len());
    }

    // --- truncate_before ---

    #[test]
    fn test_truncate_before_zero_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();

        wal.append(&[frame(1, b"keep-me")]).unwrap();
        wal.sync().unwrap();

        wal.truncate_before(0).unwrap();
        let frames = wal.read_frames(0).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(&*frames[0].data, b"keep-me");
    }

    #[test]
    fn test_truncate_before_deletes_fully_consumed_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
        wal.max_segment_size = 80; // Force rotation after ~1 frame.

        // Write frames across multiple segments.
        for i in 0..6 {
            wal.append(&[frame(1, format!("frame-{i}").as_bytes())]).unwrap();
        }
        wal.sync().unwrap();

        let segments_before = list_segments(dir.path()).unwrap();
        assert!(segments_before.len() > 1, "need multiple segments for this test");

        // Count frames in the first segment.
        let first_seg_frames = count_frames_in_segment(dir.path(), segments_before[0]).unwrap();
        assert!(first_seg_frames > 0);

        // Truncate exactly the first segment's worth of frames.
        wal.truncate_before(first_seg_frames).unwrap();

        let segments_after = list_segments(dir.path()).unwrap();
        assert_eq!(
            segments_after.len(),
            segments_before.len() - 1,
            "first segment should have been deleted"
        );
        assert!(!segments_after.contains(&segments_before[0]));
    }

    #[test]
    fn test_truncate_before_preserves_partially_consumed_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
        wal.max_segment_size = 80;

        for i in 0..6 {
            wal.append(&[frame(1, format!("frame-{i}").as_bytes())]).unwrap();
        }
        wal.sync().unwrap();

        let segments_before = list_segments(dir.path()).unwrap();
        let first_seg_frames = count_frames_in_segment(dir.path(), segments_before[0]).unwrap();

        // Truncate fewer frames than the first segment contains — nothing deleted.
        if first_seg_frames > 1 {
            wal.truncate_before(first_seg_frames - 1).unwrap();
            let segments_after = list_segments(dir.path()).unwrap();
            assert_eq!(segments_after.len(), segments_before.len());
        }
    }

    #[test]
    fn test_truncate_before_never_deletes_active_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();

        // Write a single frame — the only segment is the active one.
        wal.append(&[frame(1, b"active")]).unwrap();
        wal.sync().unwrap();

        let segments_before = list_segments(dir.path()).unwrap();

        // Attempt to truncate more frames than exist.
        wal.truncate_before(100).unwrap();

        let segments_after = list_segments(dir.path()).unwrap();
        assert_eq!(segments_after, segments_before, "active segment must not be deleted");
    }

    #[test]
    fn test_truncate_before_deletes_multiple_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
        wal.max_segment_size = 60; // Very small — each frame gets its own segment.

        for i in 0..10 {
            wal.append(&[frame(1, format!("frame-{i}").as_bytes())]).unwrap();
        }
        wal.sync().unwrap();

        let segments_before = list_segments(dir.path()).unwrap();
        assert!(segments_before.len() >= 3, "need several segments");

        // Count total frames in first two non-active segments.
        let mut total = 0u64;
        let mut deletable = 0usize;
        for &seg_idx in &segments_before {
            if Some(seg_idx) == wal.active.as_ref().map(|a| a.index) {
                break;
            }
            total += count_frames_in_segment(dir.path(), seg_idx).unwrap();
            deletable += 1;
        }

        if deletable >= 2 {
            wal.truncate_before(total).unwrap();
            let segments_after = list_segments(dir.path()).unwrap();
            assert_eq!(
                segments_after.len(),
                segments_before.len() - deletable,
                "expected {deletable} segments deleted"
            );
        }
    }

    #[test]
    fn test_truncate_before_empty_wal_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
        wal.truncate_before(10).unwrap();
        assert!(wal.read_frames(0).unwrap().is_empty());
    }

    // --- parse_segment_name ---

    #[test]
    fn test_parse_segment_name_valid_indices() {
        let cases = [
            ("segment-000000001.wal", Some(1u64)),
            ("segment-000000042.wal", Some(42)),
            ("segment-999999999.wal", Some(999_999_999)),
            ("segment-000000000.wal", Some(0)),
        ];
        for (input, expected) in cases {
            assert_eq!(parse_segment_name(input), expected, "input: {input}");
        }
    }

    #[test]
    fn test_parse_segment_name_invalid_returns_none() {
        let cases =
            ["not-a-segment.wal", "segment-abc.wal", "segment-000000001.log", "", "segment-.wal"];
        for input in cases {
            assert_eq!(parse_segment_name(input), None, "input: {input}");
        }
    }

    // --- checkpoint ---

    #[test]
    fn test_checkpoint_write_then_read_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();

        let cp = CheckpointFrame { committed_index: 42, term: 3, voted_for: None };
        wal.write_checkpoint(&cp).unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.last_checkpoint().unwrap(), Some(cp));
    }

    #[test]
    fn test_checkpoint_multiple_writes_returns_last() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();

        wal.write_checkpoint(&CheckpointFrame { committed_index: 1, term: 1, voted_for: None })
            .unwrap();
        wal.write_checkpoint(&CheckpointFrame { committed_index: 5, term: 2, voted_for: None })
            .unwrap();
        wal.write_checkpoint(&CheckpointFrame { committed_index: 10, term: 2, voted_for: None })
            .unwrap();
        wal.sync().unwrap();

        assert_eq!(
            wal.last_checkpoint().unwrap(),
            Some(CheckpointFrame { committed_index: 10, term: 2, voted_for: None })
        );
    }

    #[test]
    fn test_checkpoint_none_when_no_checkpoint_written() {
        let dir = tempfile::tempdir().unwrap();
        let wal = SegmentedWalBackend::open(dir.path()).unwrap();

        assert!(wal.last_checkpoint().unwrap().is_none());
    }

    #[test]
    fn test_checkpoint_interleaved_with_entries_returns_last() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();

        wal.append(&[frame(1, b"entry-1"), frame(2, b"entry-2")]).unwrap();
        wal.write_checkpoint(&CheckpointFrame { committed_index: 2, term: 1, voted_for: None })
            .unwrap();
        wal.append(&[frame(1, b"entry-3")]).unwrap();
        wal.write_checkpoint(&CheckpointFrame { committed_index: 3, term: 1, voted_for: None })
            .unwrap();
        wal.sync().unwrap();

        assert_eq!(
            wal.last_checkpoint().unwrap(),
            Some(CheckpointFrame { committed_index: 3, term: 1, voted_for: None })
        );
    }

    #[test]
    fn test_checkpoint_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
            wal.append(&[frame(1, b"entry-1")]).unwrap();
            wal.write_checkpoint(&CheckpointFrame { committed_index: 1, term: 1, voted_for: None })
                .unwrap();
            wal.sync().unwrap();
        }

        let wal = SegmentedWalBackend::open(dir.path()).unwrap();
        assert_eq!(
            wal.last_checkpoint().unwrap(),
            Some(CheckpointFrame { committed_index: 1, term: 1, voted_for: None })
        );
    }

    // --- recovery with corrupted WAL ---

    #[test]
    fn test_recover_from_wal_with_corrupted_frame_returns_error() {
        use std::collections::HashMap;

        use crate::wal_backend::CheckpointFrame;

        let dir = tempfile::tempdir().unwrap();

        {
            let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
            // Write two entry frames, then a checkpoint marking both committed.
            let f1 = WalFrame {
                shard_id: ShardId(1),
                index: 1,
                term: 1,
                data: Arc::from(b"entry-one".as_slice()),
            };
            let f2 = WalFrame {
                shard_id: ShardId(1),
                index: 2,
                term: 1,
                data: Arc::from(b"entry-two".as_slice()),
            };
            wal.append(&[f1, f2]).unwrap();
            wal.write_checkpoint(&CheckpointFrame { committed_index: 2, term: 1, voted_for: None })
                .unwrap();
            wal.sync().unwrap();
        }

        // Corrupt a byte in the data region of the first frame.
        let seg_path = segment_path(dir.path(), 1);
        let mut contents = fs::read(&seg_path).unwrap();
        assert!(contents.len() > HEADER_SIZE, "segment too small to corrupt");
        contents[HEADER_SIZE] ^= 0xFF;
        fs::write(&seg_path, &contents).unwrap();

        let wal = SegmentedWalBackend::open(dir.path()).unwrap();
        let result = crate::recovery::recover_from_wal(&wal, &HashMap::new());
        assert!(
            matches!(result.unwrap_err(), WalError::Corrupted { .. }),
            "expected Corrupted error propagated through recover_from_wal"
        );
    }

    // --- shred_frames ---

    #[test]
    fn test_shred_frames_zeros_matching_shard_preserves_others() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();

        wal.append(&[
            frame(1, b"shard-one-a"),
            frame(1, b"shard-one-b"),
            frame(1, b"shard-one-c"),
            frame(2, b"shard-two-a"),
            frame(2, b"shard-two-b"),
        ])
        .unwrap();
        wal.sync().unwrap();

        let zeroed = wal.shred_frames(ShardId(1)).unwrap();
        assert_eq!(zeroed, 3);

        // Verify by reading raw segment bytes (CRC won't match for zeroed frames).
        let seg_path = segment_path(dir.path(), 1);
        let contents = fs::read(&seg_path).unwrap();

        let mut pos = 0usize;
        let mut shard1_zeroed = 0u32;
        let mut shard2_intact = 0u32;
        while pos + HEADER_SIZE + CRC_SIZE <= contents.len() {
            let shard_id_raw = u64::from_le_bytes(contents[pos..pos + 8].try_into().unwrap());
            let entry_len =
                u32::from_le_bytes(contents[pos + 24..pos + 28].try_into().unwrap()) as usize;
            let frame_end = pos + HEADER_SIZE + entry_len + CRC_SIZE;
            if frame_end > contents.len() {
                break;
            }
            let data = &contents[pos + HEADER_SIZE..pos + HEADER_SIZE + entry_len];
            if shard_id_raw == 1 {
                assert!(data.iter().all(|&b| b == 0), "shard 1 data not zeroed at pos {pos}");
                shard1_zeroed += 1;
            } else if shard_id_raw == 2 {
                assert!(data.iter().any(|&b| b != 0), "shard 2 data was zeroed at pos {pos}");
                shard2_intact += 1;
            }
            pos = frame_end;
        }
        assert_eq!(shard1_zeroed, 3);
        assert_eq!(shard2_intact, 2);
    }

    #[test]
    fn test_shred_frames_unknown_shard_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();

        wal.append(&[frame(1, b"data")]).unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.shred_frames(ShardId(99)).unwrap(), 0);
    }

    #[test]
    fn test_shred_frames_across_multiple_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWalBackend::open(dir.path()).unwrap();
        wal.max_segment_size = 80;

        for i in 0..10 {
            wal.append(&[frame(1, format!("shard1-{i}").as_bytes())]).unwrap();
        }
        wal.sync().unwrap();

        let segments = list_segments(dir.path()).unwrap();
        assert!(segments.len() > 1, "expected multi-segment WAL");

        let zeroed = wal.shred_frames(ShardId(1)).unwrap();
        assert_eq!(zeroed, 10);
    }
}
