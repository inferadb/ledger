//! Bridge SDK [`TlsConfig`] → wire-transport
//! [`rustls::ClientConfig`].
//!
//! The SDK historically built a tonic [`ClientTlsConfig`](tonic::transport::ClientTlsConfig)
//! from its `TlsConfig`. Under the wire transport, the consumer is
//! [`inferadb_ledger_wire_transport::tls::client_config`] (which wraps a
//! [`rustls::ClientConfig`] into a [`quinn::ClientConfig`]). This module
//! converts the SDK's `TlsConfig` into a `rustls::ClientConfig` that the
//! wire path can use.
//!
//! # Behavior
//!
//! * `use_native_roots == true` — currently surfaces a config error. The workspace does not pull in
//!   `rustls-native-certs`, so callers asking for native roots on the wire path must supply an
//!   explicit CA cert via `ca_cert(...)` until this dependency is added (tracked separately).
//! * `ca_cert` set — every `CERTIFICATE` block in the supplied PEM is added as a root in the
//!   resulting [`rustls::RootCertStore`]. mTLS (`client_cert` + `client_key`) is not yet threaded
//!   through; the wire transport's `OpAuthEstablish` payload carries the bearer credential
//!   separately.
//! * No CA cert and no native roots — succeeds only when the `insecure-skip-verify` feature is
//!   enabled (returns the test-only no-op verifier). Otherwise returns a config error directing the
//!   caller to provide a CA cert.

use std::io::{BufReader, Cursor};

use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};

use crate::{
    config::{CertificateData, TlsConfig},
    error::{Result, SdkError},
};

/// Bridge an SDK [`TlsConfig`] (or its absence) into a
/// [`rustls::ClientConfig`] suitable for the wire transport.
///
/// `Some(tls)` produces a verifying config built from the supplied trust
/// material. `None` is allowed only when the `insecure-skip-verify` feature
/// is on — production builds without TLS material will fail at this point.
pub(crate) fn bridge_sdk_tls_to_wire(tls: Option<&TlsConfig>) -> Result<RustlsClientConfig> {
    match tls {
        Some(tls) => bridge_present_tls(tls),
        None => no_tls_fallback(),
    }
}

fn bridge_present_tls(tls: &TlsConfig) -> Result<RustlsClientConfig> {
    // Native roots: the workspace doesn't pull in `rustls-native-certs`,
    // so we can't materialize the system trust store yet. Surface a
    // structured error rather than silently swallowing the request and
    // building an empty root store (which would make every TLS handshake
    // fail far from the configuration site).
    if tls.use_native_roots() {
        return Err(SdkError::Config {
            message: "TLS use_native_roots is not yet supported on the wire transport; \
                      supply an explicit ca_cert(...) instead"
                .to_owned(),
        });
    }

    if let Some(ca) = tls.ca_cert() {
        let roots = build_root_store_from_ca(ca)?;
        return inferadb_ledger_wire_transport::tls::rustls_client_crypto(roots).map_err(|e| {
            SdkError::Config { message: format!("failed to build rustls client config: {e}") }
        });
    }

    // No CA + no native roots — only valid under the test-only feature.
    no_tls_fallback()
}

#[cfg(feature = "insecure-skip-verify")]
fn no_tls_fallback() -> Result<RustlsClientConfig> {
    Ok(inferadb_ledger_wire_transport::tls::rustls_client_crypto_skip_verify())
}

#[cfg(not(feature = "insecure-skip-verify"))]
fn no_tls_fallback() -> Result<RustlsClientConfig> {
    Err(SdkError::Config {
        message: "wire transport requires TLS configuration: supply tls(TlsConfig::builder()...) \
                  with a CA certificate, or build with the `insecure-skip-verify` feature for dev"
            .to_owned(),
    })
}

/// Parse every `CERTIFICATE` block from a PEM-or-DER cert source and add it
/// to a fresh [`RootCertStore`].
fn build_root_store_from_ca(ca: &CertificateData) -> Result<RootCertStore> {
    let pem_bytes = ca.to_pem();
    let mut reader = BufReader::new(Cursor::new(pem_bytes));
    let certs =
        rustls_pemfile::certs(&mut reader).collect::<std::result::Result<Vec<_>, _>>().map_err(
            |e| SdkError::Config { message: format!("failed to parse CA certificate PEM: {e}") },
        )?;

    if certs.is_empty() {
        return Err(SdkError::Config {
            message: "CA certificate PEM contained no CERTIFICATE blocks".to_owned(),
        });
    }

    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(|e| SdkError::Config {
            message: format!("rustls rejected CA certificate: {e}"),
        })?;
    }
    Ok(roots)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    /// `use_native_roots(true)` on the wire path is currently unsupported;
    /// surface a clear config error rather than building an empty root
    /// store.
    #[test]
    fn native_roots_returns_unsupported_error() {
        let tls = TlsConfig::with_native_roots().expect("valid config");
        let err = bridge_sdk_tls_to_wire(Some(&tls)).expect_err("native roots should error");
        match err {
            SdkError::Config { message } => {
                assert!(
                    message.contains("use_native_roots") && message.contains("ca_cert"),
                    "unexpected error: {message}"
                );
            },
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    /// Without TLS config and without the skip-verify feature, the bridge
    /// returns a clear config error pointing at the remediation.
    #[test]
    #[cfg(not(feature = "insecure-skip-verify"))]
    fn no_tls_without_skip_verify_errors() {
        let err = bridge_sdk_tls_to_wire(None).expect_err("no TLS without feature must error");
        match err {
            SdkError::Config { message } => {
                assert!(
                    message.contains("TLS configuration")
                        || message.contains("insecure-skip-verify"),
                    "unexpected error: {message}"
                );
            },
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    /// Under the skip-verify feature, the bridge succeeds with a no-op
    /// verifier. Tests and dev only.
    #[test]
    #[cfg(feature = "insecure-skip-verify")]
    fn no_tls_with_skip_verify_succeeds() {
        let _ = bridge_sdk_tls_to_wire(None).expect("skip-verify should succeed");
    }

    /// A CA cert in PEM form produces a verifying client config.
    #[test]
    fn ca_cert_pem_builds_client_config() {
        // Use rcgen to generate a valid self-signed cert.
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate self-signed");
        let pem = cert.cert.pem();
        let tls = TlsConfig::builder()
            .ca_cert(CertificateData::Pem(pem.into_bytes()))
            .build()
            .expect("valid TLS config");
        let _ = bridge_sdk_tls_to_wire(Some(&tls)).expect("bridge succeeds with valid CA");
    }

    /// Garbage PEM bytes surface a parse-style config error.
    #[test]
    fn garbage_ca_pem_errors() {
        let tls = TlsConfig::builder()
            .ca_cert(CertificateData::Pem(b"not a pem".to_vec()))
            .build()
            .expect("valid TLS config");
        let err = bridge_sdk_tls_to_wire(Some(&tls)).expect_err("garbage PEM must error");
        match err {
            SdkError::Config { message } => {
                assert!(
                    message.contains("no CERTIFICATE blocks")
                        || message.contains("parse")
                        || message.contains("rejected"),
                    "unexpected error: {message}"
                );
            },
            other => panic!("expected Config error, got {other:?}"),
        }
    }
}
