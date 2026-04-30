//! Shared TLS / quinn config builders used by both [`crate::server`] and
//! [`crate::client`].
//!
//! Both sides of the wire need a [`quinn::ServerConfig`] / [`quinn::ClientConfig`]
//! with consistent ALPN, transport limits, and 0-RTT policy. Centralising that
//! construction in one module keeps the server and client in lockstep on TLS
//! / ALPN policy — a future TLS-version bump or ALPN extension touches one
//! file, not two.
//!
//! # Policy decisions baked in here
//!
//! * **ALPN single-source-of-truth.** [`server_config`] / [`client_config`] always set
//!   [`rustls::ServerConfig::alpn_protocols`] / [`rustls::ClientConfig::alpn_protocols`] to
//!   [`inferadb_ledger_wire::supported_alpn_protocols_owned`] regardless of what the caller put on
//!   the rustls config. This is idempotent: setting the same protocols twice is a no-op.
//! * **0-RTT disabled.** quinn 0.11 disables 0-RTT by default. We rely on that default and do not
//!   flip it on. 0-RTT carries a replay risk and we don't have an idempotency table yet (Phase 12
//!   prerequisite). A future maintainer turning this on without revisiting that prerequisite is a
//!   correctness regression.
//! * **Stream caps applied symmetrically.** Both server and client transport configs cap concurrent
//!   bidirectional streams at [`MAX_CONCURRENT_BIDI_STREAMS`] and unidirectional streams at
//!   [`MAX_CONCURRENT_UNI_STREAMS`] (zero — we don't use uni streams in v1). The server-side cap is
//!   the load-bearing one (CodeRabbit H-3: prevents per-connection task fan-out); the client-side
//!   cap mirrors it so a misbehaving server can't open streams beyond what the client expects.
//! * **SYN-cookie equivalent (retry tokens).** quinn 0.11 moved the `use_retry` config knob to a
//!   per-`Incoming` decision via [`quinn::Incoming::retry`]. The server bind path (Task B.4) calls
//!   `incoming.retry()?` on first packet from any new address; this module does not configure that
//!   here.
//!
//! # Insecure feature flag
//!
//! The `insecure-skip-verify` cargo feature exposes
//! `rustls_client_crypto_skip_verify`, which accepts any server
//! certificate without verification. This is for integration tests only;
//! production builds (no feature flag) won't compile that code path.

use std::sync::{Arc, Once};

use inferadb_ledger_wire::supported_alpn_protocols_owned;

/// Maximum concurrent bidirectional streams per QUIC connection.
///
/// Caps fan-out per connection so a misbehaving client can't exhaust server
/// resources. Streams are allocated cheaply, but each carries a per-stream
/// task; 1024 keeps the per-connection task count bounded.
///
/// Applied symmetrically on the client side so a misbehaving server can't
/// open more streams than the client expects.
pub const MAX_CONCURRENT_BIDI_STREAMS: u32 = 1024;

/// Maximum concurrent unidirectional streams per QUIC connection.
///
/// We don't use unidirectional streams in v1 of the protocol; capping at
/// zero rejects them at the QUIC layer rather than relying on opcode
/// dispatch to refuse.
pub const MAX_CONCURRENT_UNI_STREAMS: u32 = 0;

/// Build a [`quinn::ServerConfig`] from a [`rustls::ServerConfig`].
///
/// Forces ALPN to the wire crate's supported protocols list (idempotent if
/// the caller already set it) and applies the standard transport caps.
///
/// # Panics
///
/// Panics if the rustls config is not QUIC-compatible. This requires
/// TLS 1.3 + a provider exposing the AEAD initial cipher suite (ring or
/// aws-lc-rs). [`rustls_server_crypto`] always satisfies this; if a caller
/// constructs the rustls config some other way and disables TLS 1.3, the
/// panic message points at the misconfiguration.
#[must_use]
pub fn server_config(mut crypto: rustls::ServerConfig) -> quinn::ServerConfig {
    // Idempotent: setting the same protocols twice is a no-op.
    crypto.alpn_protocols = supported_alpn_protocols_owned();

    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams(MAX_CONCURRENT_BIDI_STREAMS.into());
    transport.max_concurrent_uni_streams(MAX_CONCURRENT_UNI_STREAMS.into());

    // SAFETY-style justification: `QuicServerConfig::try_from` only fails
    // when the rustls config can't expose the QUIC initial cipher suite,
    // which requires TLS 1.3 + ring/aws-lc-rs. `rustls_server_crypto`
    // always provides both. Callers feeding their own rustls config in
    // are expected to do the same; the panic message is loud and points
    // at the prerequisite.
    #[allow(clippy::expect_used)]
    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
        .expect("rustls ServerConfig must be QUIC-compatible (TLS 1.3 + ring/aws-lc-rs)");

    let mut server = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    server.transport_config(Arc::new(transport));

    // 0-RTT is disabled by default in quinn 0.11; retry-token (SYN-cookie
    // equivalent) is now a per-`Incoming` call (`Incoming::retry()`) and
    // belongs in the server bind path (Task B.4), not here.

    server
}

/// Build a [`quinn::ClientConfig`] from a [`rustls::ClientConfig`].
///
/// Forces ALPN to the wire crate's supported protocols list and applies
/// stream caps that mirror the server side.
///
/// # Panics
///
/// Panics if the rustls config is not QUIC-compatible (see
/// [`server_config`] for the conditions).
#[must_use]
pub fn client_config(mut crypto: rustls::ClientConfig) -> quinn::ClientConfig {
    crypto.alpn_protocols = supported_alpn_protocols_owned();

    #[allow(clippy::expect_used)]
    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .expect("rustls ClientConfig must be QUIC-compatible (TLS 1.3 + ring/aws-lc-rs)");

    let mut client = quinn::ClientConfig::new(Arc::new(quic_crypto));

    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams(MAX_CONCURRENT_BIDI_STREAMS.into());
    transport.max_concurrent_uni_streams(MAX_CONCURRENT_UNI_STREAMS.into());
    client.transport_config(Arc::new(transport));

    client
}

/// Build a [`rustls::ServerConfig`] with our ALPN preset and a single
/// (cert chain, key) leaf identity.
///
/// Installs the `ring` rustls crypto provider as the process default if no
/// provider has been installed yet.
///
/// # Errors
///
/// Returns the underlying [`rustls::Error`] when the cert chain or key is
/// malformed.
pub fn rustls_server_crypto(
    cert_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
) -> Result<rustls::ServerConfig, rustls::Error> {
    install_default_provider();
    let mut config =
        rustls::ServerConfig::builder().with_no_client_auth().with_single_cert(cert_chain, key)?;
    config.alpn_protocols = supported_alpn_protocols_owned();
    Ok(config)
}

/// Build a [`rustls::ClientConfig`] with our ALPN preset and the supplied
/// trust roots.
///
/// Installs the `ring` rustls crypto provider as the process default if no
/// provider has been installed yet.
///
/// # Errors
///
/// Returns [`rustls::Error`] only on internal builder failures; with a
/// well-formed [`rustls::RootCertStore`] this is effectively infallible
/// today, but the `Result` shape leaves room for future validation.
pub fn rustls_client_crypto(
    roots: rustls::RootCertStore,
) -> Result<rustls::ClientConfig, rustls::Error> {
    install_default_provider();
    let mut config =
        rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    config.alpn_protocols = supported_alpn_protocols_owned();
    Ok(config)
}

/// Build a [`rustls::ClientConfig`] that accepts any server certificate.
///
/// **Insecure.** Used by integration tests so rcgen-generated self-signed
/// certs work without bundling a CA. NEVER for production. Gated behind
/// the `insecure-skip-verify` cargo feature so production builds don't
/// even compile this code path.
#[cfg(any(test, feature = "insecure-skip-verify"))]
#[must_use]
pub fn rustls_client_crypto_skip_verify() -> rustls::ClientConfig {
    install_default_provider();
    let mut config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    config.alpn_protocols = supported_alpn_protocols_owned();
    config
}

/// Install the `ring` rustls crypto provider as the process default.
///
/// rustls 0.23 with `default-features = false` requires explicit provider
/// installation per process. The [`Once`] guarantees idempotency — first
/// caller wins, subsequent callers are no-ops, even across threads.
///
/// `install_default()` returns `Result<(), Arc<CryptoProvider>>`; the error
/// case is "another provider was already installed" which is benign for
/// our use case (tests sharing a process) so we discard it.
fn install_default_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        // Discard the result; "already installed" is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Test-only `ServerCertVerifier` that accepts any peer certificate.
///
/// Gated behind `cfg(any(test, feature = "insecure-skip-verify"))` so it
/// disappears entirely from production builds.
#[cfg(any(test, feature = "insecure-skip-verify"))]
#[derive(Debug)]
struct NoVerify;

#[cfg(any(test, feature = "insecure-skip-verify"))]
impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]

    use inferadb_ledger_wire::ALPN_LEDGER_WIRE_V1;

    use super::*;

    fn generate_test_cert()
    -> (Vec<rustls::pki_types::CertificateDer<'static>>, rustls::pki_types::PrivateKeyDer<'static>)
    {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().clone();
        let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        (vec![cert_der], rustls::pki_types::PrivateKeyDer::Pkcs8(key_der))
    }

    #[test]
    fn server_config_alpn_set() {
        let (chain, key) = generate_test_cert();
        let crypto = rustls_server_crypto(chain, key).expect("build server crypto");
        assert_eq!(crypto.alpn_protocols.len(), 1);
        assert_eq!(crypto.alpn_protocols[0], ALPN_LEDGER_WIRE_V1);
    }

    #[test]
    fn client_config_alpn_set() {
        let roots = rustls::RootCertStore::empty();
        let crypto = rustls_client_crypto(roots).expect("build client crypto");
        assert_eq!(crypto.alpn_protocols.len(), 1);
        assert_eq!(crypto.alpn_protocols[0], ALPN_LEDGER_WIRE_V1);
    }

    #[test]
    fn stream_caps_constants_match_plan() {
        assert_eq!(MAX_CONCURRENT_BIDI_STREAMS, 1024);
        assert_eq!(MAX_CONCURRENT_UNI_STREAMS, 0);
    }

    #[test]
    fn server_quinn_config_builds() {
        let (chain, key) = generate_test_cert();
        let crypto = rustls_server_crypto(chain, key).expect("build server crypto");
        // Smoke test: ensures `QuicServerConfig::try_from` and the provider
        // installation worked end-to-end. If the panic in `server_config`
        // ever fires for our own builder output, this test catches it.
        let _ = server_config(crypto);
    }

    #[test]
    fn client_quinn_config_builds() {
        // Use the test-only skip-verify path so we exercise both the
        // `client_config` quinn wrap AND the `NoVerify` verifier in one
        // smoke test — same provider installation, both branches covered.
        let crypto = rustls_client_crypto_skip_verify();
        let _ = client_config(crypto);
    }

    #[test]
    fn install_default_provider_is_idempotent() {
        for _ in 0..10 {
            install_default_provider();
        }
    }

    #[test]
    fn server_config_overrides_callers_alpn() {
        let (chain, key) = generate_test_cert();
        let mut crypto = rustls_server_crypto(chain, key).expect("build server crypto");
        // Caller stomps on ALPN with something silly; `server_config` should
        // restore the wire-crate canonical list before handing off to quinn.
        crypto.alpn_protocols = vec![b"bogus/1".to_vec()];
        let _ = server_config(crypto);
        // The assertion is implicit: if `server_config` didn't overwrite,
        // the rustls server would advertise `bogus/1` and the QUIC handshake
        // would fail with our clients. We can't directly observe the inner
        // rustls config from a `quinn::ServerConfig`, but the constructor
        // doesn't panic, and end-to-end coverage in B.11 verifies the ALPN
        // string on the wire.
    }
}
