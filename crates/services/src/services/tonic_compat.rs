//! Tonic-shaped compatibility helpers for the wire transport.
//!
//! Provides a `MetadataMap`-only deadline extractor used by
//! [`super::wire_helpers::tonic_request_to_wire_context`] when synthesizing a
//! wire `RequestContext` from an inbound tonic request. New code should use
//! the wire-shaped sibling helpers in [`super::wire_helpers`] instead.

use std::time::Duration;

use tonic::metadata::MetadataMap;

/// Header used by tonic to encode the per-request deadline.
const GRPC_TIMEOUT_HEADER: &str = "grpc-timeout";

/// Parses a `grpc-timeout` header value into a [`Duration`].
fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    let bytes = value.as_bytes();
    if bytes.len() < 2 {
        return None;
    }
    let (digits, unit) = bytes.split_at(bytes.len() - 1);
    let n: u64 = std::str::from_utf8(digits).ok()?.parse().ok()?;
    match unit[0] {
        b'H' => Some(Duration::from_secs(n.checked_mul(3600)?)),
        b'M' => Some(Duration::from_secs(n.checked_mul(60)?)),
        b'S' => Some(Duration::from_secs(n)),
        b'm' => Some(Duration::from_millis(n)),
        b'u' => Some(Duration::from_micros(n)),
        b'n' => Some(Duration::from_nanos(n)),
        _ => None,
    }
}

/// Extracts the gRPC client deadline (as a remaining-time [`Duration`]) from a
/// tonic `MetadataMap`.
#[must_use]
pub(crate) fn extract_deadline_from_metadata(metadata: &MetadataMap) -> Option<Duration> {
    metadata.get(GRPC_TIMEOUT_HEADER).and_then(|v| v.to_str().ok()).and_then(parse_grpc_timeout)
}
