//! Background job telemetry context.
//!
//! `JobContext` is the background-job counterpart to `RequestContext`.
//! Create one at the start of a job cycle; set result and item counts
//! during execution. On Drop, it emits:
//!
//! - `ledger_background_job_runs_total{job, result}` counter
//! - `ledger_background_job_duration_seconds{job}` histogram
//! - `ledger_background_job_items_processed_total{job}` counter (if items > 0)
//! - A canonical log line to the `ledger::jobs` tracing target
//!
//! # Example
//!
//! ```no_run
//! use inferadb_ledger_raft::logging::JobContext;
//!
//! async fn run_maintenance_cycle() {
//!     let mut ctx = JobContext::new("token_maintenance", None);
//!     // ... do work ...
//!     ctx.record_items(42);
//!     // Metrics + log line emitted automatically on drop
//! }
//! ```

use std::{sync::Arc, time::Instant};

use inferadb_ledger_types::events::{EventAction, EventOutcome};

use crate::event_writer::EventEmitter;

/// Telemetry context for a single background job cycle.
///
/// Accumulates item counts and result state during a job cycle. On drop,
/// emits Prometheus counters/histograms and a structured log line to the
/// `ledger::jobs` tracing target.
///
/// The default result is `"success"`. Call [`set_failure`](Self::set_failure)
/// to record a failed cycle.
pub struct JobContext {
    job_name: &'static str,
    start: Instant,
    items_processed: u64,
    result: &'static str,
    event_handle: Option<Arc<dyn EventEmitter>>,
    detail_items: Vec<(&'static str, u64)>,
}

impl JobContext {
    /// Creates a new context for `job_name`, optionally with an event handle
    /// for business event emission.
    ///
    /// The result defaults to `"success"`. Call [`set_failure`](Self::set_failure)
    /// if the cycle encounters an error.
    pub fn new(job_name: &'static str, event_handle: Option<Arc<dyn EventEmitter>>) -> Self {
        Self {
            job_name,
            start: Instant::now(),
            items_processed: 0,
            result: "success",
            event_handle,
            detail_items: Vec::new(),
        }
    }

    /// Adds `count` to the total items processed this cycle.
    pub fn record_items(&mut self, count: u64) {
        self.items_processed = self.items_processed.saturating_add(count);
    }

    /// Records `count` items under a named category and adds them to the total.
    ///
    /// Use this when a job processes multiple distinct categories of work and
    /// you want to preserve the per-category breakdown (e.g., for log line
    /// introspection) while still rolling the total up for metrics.
    pub fn record_items_detail(&mut self, category: &'static str, count: u64) {
        self.detail_items.push((category, count));
        self.items_processed = self.items_processed.saturating_add(count);
    }

    /// Marks this cycle as failed.
    ///
    /// The `result` label emitted to Prometheus will be `"failure"` instead of
    /// the default `"success"`.
    pub fn set_failure(&mut self) {
        self.result = "failure";
    }

    /// Emits a handler-phase business event if an event handle was provided.
    ///
    /// Background jobs are treated as system-scope actors (no organization or
    /// vault context). Additional key-value pairs can be attached via `details`.
    ///
    /// This is a no-op when no event handle was provided at construction.
    pub fn record_event(&self, action: EventAction, details: &[(&str, &str)]) {
        let Some(handle) = &self.event_handle else {
            return;
        };

        let mut emitter = crate::event_writer::HandlerPhaseEmitter::for_system(action, 0)
            .outcome(EventOutcome::Success);

        for &(k, v) in details {
            emitter = emitter.detail(k, v);
        }

        let entry = emitter.build(90);
        handle.record_event(entry);
    }
}

impl Drop for JobContext {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_secs_f64();

        crate::metrics::record_background_job_run(self.job_name, self.result);
        crate::metrics::record_background_job_duration(self.job_name, elapsed);

        if self.items_processed > 0 {
            crate::metrics::record_background_job_items(self.job_name, self.items_processed);
        }

        tracing::info!(
            target: "ledger::jobs",
            job = self.job_name,
            result = self.result,
            duration_ms = (elapsed * 1000.0) as u64,
            items = self.items_processed,
            "background_job_complete",
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn job_context_defaults_to_success() {
        let ctx = JobContext::new("test_job", None);
        assert_eq!(ctx.result, "success");
    }

    #[test]
    fn job_context_set_failure() {
        let mut ctx = JobContext::new("test_job", None);
        ctx.set_failure();
        assert_eq!(ctx.result, "failure");
    }

    #[test]
    fn job_context_record_items() {
        let mut ctx = JobContext::new("test_job", None);
        ctx.record_items(10);
        ctx.record_items(5);
        assert_eq!(ctx.items_processed, 15);
    }

    #[test]
    fn job_context_record_items_detail() {
        let mut ctx = JobContext::new("test_job", None);
        ctx.record_items_detail("tokens", 5);
        assert_eq!(ctx.items_processed, 5);
        assert_eq!(ctx.detail_items.len(), 1);
        assert_eq!(ctx.detail_items[0], ("tokens", 5));
    }

    #[test]
    fn job_context_record_items_detail_adds_to_total() {
        let mut ctx = JobContext::new("test_job", None);
        ctx.record_items(3);
        ctx.record_items_detail("codes", 7);
        assert_eq!(ctx.items_processed, 10);
        assert_eq!(ctx.detail_items.len(), 1);
    }

    #[test]
    fn job_context_record_event_noop_without_handle() {
        let ctx = JobContext::new("test_job", None);
        // Should not panic
        ctx.record_event(
            inferadb_ledger_types::events::EventAction::RequestRateLimited,
            &[("reason", "test")],
        );
    }

    #[test]
    fn job_context_record_event_with_handle() {
        use std::sync::Mutex;

        use inferadb_ledger_types::events::EventEntry;

        struct TestEmitter {
            captured: Mutex<Option<EventEntry>>,
        }

        impl EventEmitter for TestEmitter {
            fn record_event(&self, entry: EventEntry) {
                *self.captured.lock().unwrap() = Some(entry);
            }
        }

        let emitter = Arc::new(TestEmitter { captured: Mutex::new(None) });

        let ctx = JobContext::new("test_job", Some(emitter.clone() as Arc<dyn EventEmitter>));
        ctx.record_event(
            inferadb_ledger_types::events::EventAction::RequestRateLimited,
            &[("level", "system")],
        );

        let entry = emitter.captured.lock().unwrap().take();
        assert!(entry.is_some(), "event should have been emitted");
        let entry = entry.unwrap();
        assert_eq!(entry.action, inferadb_ledger_types::events::EventAction::RequestRateLimited);
        assert_eq!(entry.details.get("level").map(String::as_str), Some("system"));
    }
}
