//! TLS plumbing for the wire transport bind path (F.1.d).
//!
//! `bootstrap_node` calls `load_wire_tls_server_config` when the
//! operator selects `TransportKind::Wire` (the F.1.d default). The
//! helper reads the operator-supplied PEM cert chain + key from disk,
//! hands the DER bytes to
//! `inferadb_ledger_wire_transport::tls::rustls_server_crypto`, and
//! surfaces a [`rustls::ServerConfig`] suitable for
//! `LedgerServer::with_wire_tls_config`.
//!
//! `load_wire_tls_client_config` mirrors the server side for the
//! inter-node Raft transport: bootstrap installs the resulting
//! `quinn::ClientConfig` on the shared `NodeConnectionRegistry` via
//! `WireClientTemplate` so peer connections established through
//! `set_peer_via_registry` reuse the cluster's TLS material.
//!
//! Production deployments are expected to share a single CA across the
//! cluster: the server cert proves the local node's identity to incoming
//! peer dialers, and the same cert (acting as a trust anchor for the
//! cluster's CA chain) is loaded into the client's
//! [`rustls::RootCertStore`] so this node can verify peer certs on
//! outgoing dials. For loopback / single-CA test fixtures the same leaf
//! cert can serve both roles. The two functions take the same `cert_path`
//! to make that pattern explicit.

use std::path::{Path, PathBuf};

use snafu::{ResultExt, Snafu};

/// Errors surfaced while loading PEM-encoded cert/key material from disk.
///
/// Each variant carries enough context (file path, parser stage) for an
/// operator to diagnose a misconfigured `--tls-cert` / `--tls-key` flag
/// without enabling debug logging.
#[derive(Debug, Snafu)]
pub enum TlsLoadError {
    /// Failed to open the PEM cert or key file. Typically a missing file,
    /// wrong path, or insufficient filesystem permissions.
    #[snafu(display("failed to open TLS PEM file at {}: {source}", path.display()))]
    OpenPem {
        /// The PEM file the operator supplied.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// Failed to parse a PEM block. Usually malformed PEM input.
    #[snafu(display("failed to parse PEM at {}: {source}", path.display()))]
    ParsePem {
        /// The PEM file that failed to parse.
        path: PathBuf,
        /// Underlying I/O error from the PEM parser.
        source: std::io::Error,
    },
    /// PEM file contained no certificate blocks.
    #[snafu(display("PEM at {} contained no CERTIFICATE blocks", path.display()))]
    NoCerts {
        /// The empty cert PEM file.
        path: PathBuf,
    },
    /// PEM file contained no private key.
    #[snafu(display("PEM at {} contained no private key", path.display()))]
    NoPrivateKey {
        /// The empty key PEM file.
        path: PathBuf,
    },
    /// `rustls` rejected the cert/key (mismatched key, unsupported curve,
    /// etc.).
    #[snafu(display("rustls rejected cert/key from {}: {source}", path.display()))]
    Rustls {
        /// The PEM file whose contents `rustls` could not load.
        path: PathBuf,
        /// Underlying rustls error.
        source: rustls::Error,
    },
}

/// Load a PEM cert chain + private key and produce a
/// [`rustls::ServerConfig`] suitable for the wire transport.
///
/// Both files are expected to be PEM-encoded:
///
/// - `cert_path`: one or more `CERTIFICATE` blocks (leaf first, then any intermediates).
/// - `key_path`: a single `PRIVATE KEY` (PKCS#8), `RSA PRIVATE KEY`, or `EC PRIVATE KEY` block.
///
/// The returned config has ALPN preset to the wire crate's canonical
/// list, so callers can hand it directly to
/// [`inferadb_ledger_services::LedgerServer::with_wire_tls_config`] without
/// further setup.
///
/// # Errors
///
/// Surfaces [`TlsLoadError`] for missing files, malformed PEM, empty PEM
/// blocks, or rustls validation failures.
pub fn load_wire_tls_server_config(
    cert_path: &Path,
    key_path: &Path,
) -> Result<rustls::ServerConfig, TlsLoadError> {
    let cert_chain = read_certs(cert_path)?;
    let key = read_private_key(key_path)?;
    inferadb_ledger_wire_transport::tls::rustls_server_crypto(cert_chain, key)
        .context(RustlsSnafu { path: cert_path.to_path_buf() })
}

/// Load a PEM cert chain (used as the trust anchor for outgoing peer
/// dials) and produce a [`rustls::ClientConfig`] suitable for inter-node
/// Raft over the wire transport.
///
/// In production the operator typically passes the cluster CA cert (or
/// the leaf cert in single-CA test fixtures) here. Every cert in the PEM
/// chain is added as a root in the resulting
/// [`rustls::RootCertStore`].
///
/// # Errors
///
/// Surfaces [`TlsLoadError`] for missing files, malformed PEM, empty PEM
/// blocks, or rustls validation failures (typically a malformed cert).
pub fn load_wire_tls_client_config(cert_path: &Path) -> Result<rustls::ClientConfig, TlsLoadError> {
    let cert_chain = read_certs(cert_path)?;
    let mut roots = rustls::RootCertStore::empty();
    for cert in cert_chain {
        roots.add(cert).context(RustlsSnafu { path: cert_path.to_path_buf() })?;
    }
    inferadb_ledger_wire_transport::tls::rustls_client_crypto(roots)
        .context(RustlsSnafu { path: cert_path.to_path_buf() })
}

/// Read every `CERTIFICATE` block out of a PEM file. Returns
/// [`TlsLoadError::NoCerts`] when the file parses cleanly but contains no
/// cert blocks (e.g. operator handed us a key file by mistake).
fn read_certs(
    path: &Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, TlsLoadError> {
    let file = std::fs::File::open(path).context(OpenPemSnafu { path: path.to_path_buf() })?;
    let mut reader = std::io::BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .context(ParsePemSnafu { path: path.to_path_buf() })?;
    if certs.is_empty() {
        return NoCertsSnafu { path: path.to_path_buf() }.fail();
    }
    Ok(certs)
}

/// Read the first private key out of a PEM file. Returns
/// [`TlsLoadError::NoPrivateKey`] when the file parses cleanly but
/// contains no key blocks.
fn read_private_key(
    path: &Path,
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, TlsLoadError> {
    let file = std::fs::File::open(path).context(OpenPemSnafu { path: path.to_path_buf() })?;
    let mut reader = std::io::BufReader::new(file);
    let key = rustls_pemfile::private_key(&mut reader)
        .context(ParsePemSnafu { path: path.to_path_buf() })?;
    key.ok_or_else(|| NoPrivateKeySnafu { path: path.to_path_buf() }.build())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::io::Write;

    use rcgen::generate_simple_self_signed;
    use tempfile::NamedTempFile;

    use super::*;

    /// Generate a self-signed cert + key pair in PEM form. Returns
    /// `(cert_pem_path, key_pem_path)` plus the temp-file guards (kept
    /// alive by the caller so the files persist for the duration of the
    /// test).
    fn write_pem_pair() -> (NamedTempFile, NamedTempFile) {
        let cert =
            generate_simple_self_signed(vec!["localhost".to_string()]).expect("rcgen self-signed");
        let cert_pem = cert.cert.pem();
        let key_pem = cert.key_pair.serialize_pem();

        let mut cert_file = NamedTempFile::new().expect("temp cert file");
        cert_file.write_all(cert_pem.as_bytes()).expect("write cert pem");
        let mut key_file = NamedTempFile::new().expect("temp key file");
        key_file.write_all(key_pem.as_bytes()).expect("write key pem");
        (cert_file, key_file)
    }

    #[test]
    fn load_wire_tls_server_config_loads_pem() {
        let (cert, key) = write_pem_pair();
        let server_config =
            load_wire_tls_server_config(cert.path(), key.path()).expect("loads PEM");
        // ALPN is set by `rustls_server_crypto`. Smoke-checking it here
        // catches a future regression where the bridge bypasses the
        // ALPN preset.
        assert_eq!(server_config.alpn_protocols.len(), 1);
    }

    #[test]
    fn load_wire_tls_server_config_missing_cert_errors_clearly() {
        let (_cert, key) = write_pem_pair();
        let bogus_cert = std::path::PathBuf::from("/nonexistent/wire-cert.pem");
        let err = load_wire_tls_server_config(&bogus_cert, key.path())
            .expect_err("missing cert file must error");
        assert!(
            matches!(err, TlsLoadError::OpenPem { .. }),
            "expected OpenPem error for missing cert path, got {err:?}"
        );
    }

    #[test]
    fn load_wire_tls_server_config_missing_key_errors_clearly() {
        let (cert, _key) = write_pem_pair();
        let bogus_key = std::path::PathBuf::from("/nonexistent/wire-key.pem");
        let err = load_wire_tls_server_config(cert.path(), &bogus_key)
            .expect_err("missing key file must error");
        assert!(
            matches!(err, TlsLoadError::OpenPem { .. }),
            "expected OpenPem error for missing key path, got {err:?}"
        );
    }

    #[test]
    fn load_wire_tls_server_config_invalid_pem_errors() {
        // Empty file — parses fine but yields no cert blocks. This is
        // the most common operator mistake (handed us the wrong file).
        let cert = NamedTempFile::new().expect("temp cert file");
        let (_real_cert, key) = write_pem_pair();
        let err = load_wire_tls_server_config(cert.path(), key.path())
            .expect_err("empty cert file must error");
        assert!(matches!(err, TlsLoadError::NoCerts { .. }), "expected NoCerts error, got {err:?}");
    }

    #[test]
    fn load_wire_tls_server_config_no_private_key_errors() {
        let (cert, _real_key) = write_pem_pair();
        let key = NamedTempFile::new().expect("temp key file");
        let err = load_wire_tls_server_config(cert.path(), key.path())
            .expect_err("empty key file must error");
        assert!(
            matches!(err, TlsLoadError::NoPrivateKey { .. }),
            "expected NoPrivateKey error, got {err:?}"
        );
    }

    #[test]
    fn load_wire_tls_server_config_garbage_pem_errors() {
        let mut cert = NamedTempFile::new().expect("temp cert file");
        cert.write_all(b"not a pem file\n").expect("write garbage");
        let (_real_cert, key) = write_pem_pair();
        let err = load_wire_tls_server_config(cert.path(), key.path())
            .expect_err("garbage cert file must error");
        // `rustls_pemfile::certs` skips non-PEM lines silently, so this
        // surfaces as `NoCerts` rather than `ParsePem` for free-form
        // garbage. Either is acceptable for the operator-facing error.
        assert!(
            matches!(err, TlsLoadError::NoCerts { .. } | TlsLoadError::ParsePem { .. }),
            "expected NoCerts or ParsePem for garbage input, got {err:?}"
        );
    }

    #[test]
    fn load_wire_tls_client_config_loads_pem() {
        let (cert, _key) = write_pem_pair();
        let client_config = load_wire_tls_client_config(cert.path()).expect("loads PEM");
        assert_eq!(client_config.alpn_protocols.len(), 1);
    }

    #[test]
    fn load_wire_tls_client_config_missing_cert_errors_clearly() {
        let bogus = std::path::PathBuf::from("/nonexistent/wire-ca.pem");
        let err = load_wire_tls_client_config(&bogus).expect_err("missing cert path must error");
        assert!(matches!(err, TlsLoadError::OpenPem { .. }), "expected OpenPem error, got {err:?}");
    }
}
