//! Audit logging framework for compliance-ready event tracking.
//!
//! Provides a trait-based audit logger with a file-based implementation that writes
//! JSON Lines (one JSON object per line) to disk with durable writes and log rotation.
//!
//! # Architecture
//!
//! - [`AuditLogger`] trait defines the interface for pluggable backends
//! - [`FileAuditLogger`] writes to disk with `fsync` for durability
//! - [`NullAuditLogger`] is a no-op for when audit logging is disabled
//!
//! # Durability
//!
//! SOC2/HIPAA compliance requires audit writes to be durable before the
//! operation response is returned. The file-based implementation calls
//! `sync_data()` after each write to ensure data reaches stable storage.
//!
//! # Log Rotation
//!
//! When the active log exceeds the configured size limit, it is rotated:
//! `audit.jsonl` → `audit.jsonl.1` → `audit.jsonl.2` → ... → deleted.
//! Rotation is atomic (rename) and protected by a mutex.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use inferadb_ledger_types::{audit::AuditEvent, config::AuditConfig};
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};

/// Audit logger trait for pluggable audit backends.
///
/// Implementations must be `Send + Sync` for use across gRPC service handlers.
/// The `log` method must ensure the event is durably persisted before returning.
pub trait AuditLogger: Send + Sync {
    /// Log an audit event.
    ///
    /// Must ensure the event is durably persisted before returning.
    /// Returns an error if the event cannot be persisted.
    fn log(&self, event: &AuditEvent) -> Result<(), AuditError>;
}

/// No-op audit logger for when audit logging is disabled.
///
/// Returns `Ok(())` immediately with zero overhead.
pub struct NullAuditLogger;

impl AuditLogger for NullAuditLogger {
    fn log(&self, _event: &AuditEvent) -> Result<(), AuditError> {
        Ok(())
    }
}

/// File-based audit logger with durable writes and log rotation.
///
/// Writes audit events as JSON Lines (one JSON object per line) to disk.
/// Each write is followed by `sync_data()` to ensure durability before
/// the operation response is returned to the client.
///
/// # Thread Safety
///
/// The logger is protected by a mutex. Writes and rotations are serialized
/// to prevent data corruption and ensure sequential event ordering.
pub struct FileAuditLogger {
    inner: Mutex<FileAuditLoggerInner>,
    config: AuditConfig,
}

struct FileAuditLoggerInner {
    file: File,
    bytes_written: u64,
}

impl FileAuditLogger {
    /// Creates a new file-based audit logger.
    ///
    /// Opens (or creates) the log file at the configured path. If the file
    /// already exists, new events are appended. The current file size is
    /// tracked for rotation decisions.
    ///
    /// # Errors
    ///
    /// Returns [`AuditError::Io`] if the file cannot be opened or created.
    pub fn new(config: AuditConfig) -> Result<Self, AuditError> {
        let path = Path::new(&config.path);

        // Create parent directories if needed
        if let Some(parent) = path.parent()
            && !parent.exists()
        {
            fs::create_dir_all(parent).context(IoSnafu { path: config.path.clone() })?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .context(IoSnafu { path: config.path.clone() })?;

        let bytes_written = file.metadata().context(IoSnafu { path: config.path.clone() })?.len();

        Ok(Self { inner: Mutex::new(FileAuditLoggerInner { file, bytes_written }), config })
    }

    /// Rotates log files when the active file exceeds the size limit.
    ///
    /// Rotation scheme: `audit.jsonl` → `audit.jsonl.1` → `audit.jsonl.2` → ...
    /// Files beyond `max_rotated_files` are deleted.
    fn rotate(&self, inner: &mut FileAuditLoggerInner) -> Result<(), AuditError> {
        let path = PathBuf::from(&self.config.path);

        // Delete the oldest file if it exceeds max_rotated_files
        let oldest = format!("{}.{}", self.config.path, self.config.max_rotated_files);
        let _ = fs::remove_file(&oldest);

        // Shift existing rotated files: .N → .N+1
        for i in (1..self.config.max_rotated_files).rev() {
            let from = format!("{}.{}", self.config.path, i);
            let to = format!("{}.{}", self.config.path, i + 1);
            if Path::new(&from).exists() {
                fs::rename(&from, &to).context(IoSnafu { path: from })?;
            }
        }

        // Rotate current file to .1
        let rotated = format!("{}.1", self.config.path);
        fs::rename(&path, &rotated).context(IoSnafu { path: self.config.path.clone() })?;

        // Open new file
        inner.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .context(IoSnafu { path: self.config.path.clone() })?;
        inner.bytes_written = 0;

        Ok(())
    }

    /// Returns the current file size in bytes (for testing).
    pub fn bytes_written(&self) -> u64 {
        self.inner.lock().bytes_written
    }
}

impl AuditLogger for FileAuditLogger {
    fn log(&self, event: &AuditEvent) -> Result<(), AuditError> {
        let mut line =
            serde_json::to_vec(event).context(SerializationSnafu { event_id: &event.event_id })?;
        line.push(b'\n');

        let mut inner = self.inner.lock();

        // Check if rotation is needed before writing
        if inner.bytes_written + line.len() as u64 > self.config.max_file_size_bytes {
            self.rotate(&mut inner)?;
        }

        inner.file.write_all(&line).context(IoSnafu { path: &self.config.path })?;
        inner.file.sync_data().context(IoSnafu { path: &self.config.path })?;
        inner.bytes_written += line.len() as u64;

        Ok(())
    }
}

/// Creates an audit logger based on configuration.
///
/// Returns a `FileAuditLogger` if config is `Some`, or a `NullAuditLogger` if `None`.
pub fn create_audit_logger(
    config: Option<AuditConfig>,
) -> Result<Arc<dyn AuditLogger>, AuditError> {
    match config {
        Some(config) => Ok(Arc::new(FileAuditLogger::new(config)?)),
        None => Ok(Arc::new(NullAuditLogger)),
    }
}

/// Audit logging error.
#[derive(Debug, Snafu)]
pub enum AuditError {
    /// I/O error during audit log write or rotation.
    #[snafu(display("Audit I/O error for {path}: {source}"))]
    Io {
        /// Path that caused the error.
        path: String,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// Serialization error when encoding audit event.
    #[snafu(display("Failed to serialize audit event {event_id}: {source}"))]
    Serialization {
        /// Event ID that failed to serialize.
        event_id: String,
        /// Underlying serialization error.
        source: serde_json::Error,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::io::Read;

    use inferadb_ledger_types::{
        audit::{AuditAction, AuditOutcome, AuditResource},
        types::{NamespaceId, VaultId},
    };

    use super::*;

    fn test_event(action: AuditAction) -> AuditEvent {
        AuditEvent {
            timestamp: "2025-01-15T10:30:00Z".to_string(),
            event_id: "test-event-001".to_string(),
            principal: "client:test-app".to_string(),
            action,
            resource: AuditResource::vault(NamespaceId::new(1), VaultId::new(2)),
            outcome: AuditOutcome::Success,
            node_id: Some(1),
            trace_id: Some("trace-123".to_string()),
            operations_count: Some(3),
        }
    }

    fn test_config(dir: &Path) -> AuditConfig {
        AuditConfig {
            path: dir.join("audit.jsonl").to_string_lossy().to_string(),
            max_file_size_bytes: 100 * 1024 * 1024,
            max_rotated_files: 3,
        }
    }

    #[test]
    fn test_null_audit_logger_always_succeeds() {
        let logger = NullAuditLogger;
        let event = test_event(AuditAction::Write);
        assert!(logger.log(&event).is_ok());
    }

    #[test]
    fn test_file_audit_logger_creates_file() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = test_config(dir.path());
        let logger = FileAuditLogger::new(config.clone()).expect("create logger");

        let event = test_event(AuditAction::Write);
        logger.log(&event).expect("log event");

        // Verify file exists and contains the event
        let mut content = String::new();
        File::open(&config.path).unwrap().read_to_string(&mut content).unwrap();
        assert!(content.contains("test-event-001"));
        assert!(content.contains("write"));
        assert!(content.ends_with('\n'));
    }

    #[test]
    fn test_file_audit_logger_appends_events() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = test_config(dir.path());
        let logger = FileAuditLogger::new(config.clone()).expect("create logger");

        logger.log(&test_event(AuditAction::Write)).expect("first");
        logger.log(&test_event(AuditAction::CreateNamespace)).expect("second");
        logger.log(&test_event(AuditAction::DeleteVault)).expect("third");

        let content = fs::read_to_string(&config.path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);

        // Each line is valid JSON
        for line in &lines {
            let _: AuditEvent = serde_json::from_str(line).expect("valid JSON");
        }
    }

    #[test]
    fn test_file_audit_logger_creates_parent_directories() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let nested = dir.path().join("deep").join("nested").join("dir");
        let config = AuditConfig {
            path: nested.join("audit.jsonl").to_string_lossy().to_string(),
            max_file_size_bytes: 100 * 1024 * 1024,
            max_rotated_files: 3,
        };
        let logger = FileAuditLogger::new(config).expect("create logger");
        logger.log(&test_event(AuditAction::Write)).expect("log event");
    }

    #[test]
    fn test_file_audit_logger_rotation() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("audit.jsonl");
        let config = AuditConfig {
            path: path.to_string_lossy().to_string(),
            max_file_size_bytes: 200, // Very small to trigger rotation
            max_rotated_files: 3,
        };
        let logger = FileAuditLogger::new(config.clone()).expect("create logger");

        // Write events until rotation occurs
        for _ in 0..20 {
            logger.log(&test_event(AuditAction::Write)).expect("log event");
        }

        // Check that rotated files exist
        let rotated_1 = format!("{}.1", config.path);
        assert!(Path::new(&rotated_1).exists(), "rotated file .1 should exist");
    }

    #[test]
    fn test_file_audit_logger_rotation_deletes_oldest() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("audit.jsonl");
        let config = AuditConfig {
            path: path.to_string_lossy().to_string(),
            max_file_size_bytes: 200,
            max_rotated_files: 2,
        };
        let logger = FileAuditLogger::new(config.clone()).expect("create logger");

        // Write enough to trigger multiple rotations
        for _ in 0..60 {
            logger.log(&test_event(AuditAction::Write)).expect("log event");
        }

        // .1 and .2 should exist, .3 should not
        assert!(Path::new(&format!("{}.1", config.path)).exists());
        assert!(Path::new(&format!("{}.2", config.path)).exists());
        assert!(!Path::new(&format!("{}.3", config.path)).exists());
    }

    #[test]
    fn test_file_audit_logger_bytes_written_tracking() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = test_config(dir.path());
        let logger = FileAuditLogger::new(config).expect("create logger");

        assert_eq!(logger.bytes_written(), 0);

        logger.log(&test_event(AuditAction::Write)).expect("log event");
        assert!(logger.bytes_written() > 0);
    }

    #[test]
    fn test_file_audit_logger_resumes_from_existing_file() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = test_config(dir.path());

        // Write one event
        {
            let logger = FileAuditLogger::new(config.clone()).expect("create logger");
            logger.log(&test_event(AuditAction::Write)).expect("first");
        }

        // Reopen the same file — should append
        {
            let logger = FileAuditLogger::new(config.clone()).expect("reopen logger");
            assert!(logger.bytes_written() > 0, "should track existing file size");
            logger.log(&test_event(AuditAction::CreateNamespace)).expect("second");
        }

        let content = fs::read_to_string(&config.path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_file_audit_logger_event_has_all_fields() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = test_config(dir.path());
        let logger = FileAuditLogger::new(config.clone()).expect("create logger");

        let event = AuditEvent {
            timestamp: "2025-01-15T10:30:00Z".to_string(),
            event_id: "full-event-id".to_string(),
            principal: "admin-user".to_string(),
            action: AuditAction::DeleteNamespace,
            resource: AuditResource::namespace(NamespaceId::new(42)).with_detail("prod-ns"),
            outcome: AuditOutcome::Failed {
                code: "NOT_FOUND".to_string(),
                detail: "namespace does not exist".to_string(),
            },
            node_id: Some(3),
            trace_id: Some("trace-456".to_string()),
            operations_count: None,
        };

        logger.log(&event).expect("log event");

        let content = fs::read_to_string(&config.path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        assert_eq!(parsed["event_id"], "full-event-id");
        assert_eq!(parsed["principal"], "admin-user");
        assert_eq!(parsed["action"], "delete_namespace");
        assert_eq!(parsed["resource"]["namespace_id"], 42);
        assert_eq!(parsed["resource"]["detail"], "prod-ns");
        assert_eq!(parsed["outcome"]["failed"]["code"], "NOT_FOUND");
        assert_eq!(parsed["node_id"], 3);
        assert_eq!(parsed["trace_id"], "trace-456");
        assert!(parsed.get("operations_count").is_none());
    }

    #[test]
    fn test_create_audit_logger_with_config() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = test_config(dir.path());
        let logger = create_audit_logger(Some(config));
        assert!(logger.is_ok());
    }

    #[test]
    fn test_create_audit_logger_without_config() {
        let logger = create_audit_logger(None);
        assert!(logger.is_ok());
        // NullAuditLogger should succeed
        let event = test_event(AuditAction::Write);
        logger.unwrap().log(&event).expect("null logger should succeed");
    }

    #[test]
    fn test_file_audit_logger_denied_outcome() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = test_config(dir.path());
        let logger = FileAuditLogger::new(config.clone()).expect("create logger");

        let event = AuditEvent {
            timestamp: "2025-01-15T10:30:00Z".to_string(),
            event_id: "denied-event".to_string(),
            principal: "bad-client".to_string(),
            action: AuditAction::Write,
            resource: AuditResource::vault(NamespaceId::new(1), VaultId::new(1)),
            outcome: AuditOutcome::Denied { reason: "rate_limited".to_string() },
            node_id: None,
            trace_id: None,
            operations_count: None,
        };

        logger.log(&event).expect("log denied event");

        let content = fs::read_to_string(&config.path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(parsed["outcome"]["denied"]["reason"], "rate_limited");
    }
}
