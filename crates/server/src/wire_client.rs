//! Operator-facing CLI helpers for dialing a running ledger node over
//! the wire transport.
//!
//! The `init`, `vaults`, and `admin` CLI subcommands all connect to a
//! single host (the operator-supplied `--host`) and issue an RPC against
//! one of the wire-protocol services. The wire transport is QUIC + TLS,
//! so the CLI accepts the same `--tls-cert` / `--tls-server-name` flags
//! the server itself uses; the cert acts as the trust anchor for the
//! dial.
//!
//! `--tls-cert` is required — there is no insecure / no-verify dial path
//! on the CLI. Dev clusters launched with `--dev` still generate a cert
//! at startup (the wire bind itself is gated on `--tls-cert`/`--tls-key`),
//! so the same PEM is reusable for `inferadb-ledger init --tls-cert ...`.
//!
//! Per-service builders (`build_admin_client`, `build_user_client`,
//! `build_vault_client`, `build_write_client`, `build_read_client`)
//! all delegate to a shared `build_wire_client` so cert loading and the
//! local QUIC bind happen exactly once per CLI invocation.

use std::{net::ToSocketAddrs, sync::Arc, time::Duration};

use bytes::Bytes;
use inferadb_ledger_wire_services::{
    AdminServiceClient, ReadServiceClient, UserServiceClient, VaultServiceClient,
    WriteServiceClient,
};
use inferadb_ledger_wire_transport::{
    ClientConfig as WireClientConfig, WireClient, tls as wire_tls,
};

use crate::config::TlsConfig;

/// Errors building a CLI-side wire client.
///
/// RPC-level failures are surfaced through the `RpcError` returned by
/// each service's client directly; this enum covers only the
/// construction path (TLS load, host resolution, QUIC client init).
#[derive(Debug)]
pub(crate) enum CliWireClientError {
    /// Operator omitted `--tls-cert`; required by the QUIC + TLS dial.
    MissingTlsCert,
    /// `--host` did not parse / resolve as `host:port`.
    InvalidHost { host: String, reason: String },
    /// Failed to load `--tls-cert` PEM material from disk.
    TlsLoad { source: crate::tls::TlsLoadError },
    /// `WireClient::new` failed (typically the local UDP-socket bind).
    WireClient { reason: String },
}

impl std::fmt::Display for CliWireClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingTlsCert => write!(
                f,
                "--tls-cert is required for CLI commands that dial a running node \
                 (the wire transport is QUIC + TLS); pass the same PEM the server \
                 was started with"
            ),
            Self::InvalidHost { host, reason } => {
                write!(f, "invalid --host '{host}': {reason}")
            },
            Self::TlsLoad { source } => write!(f, "failed to load TLS material: {source}"),
            Self::WireClient { reason } => write!(f, "failed to create wire client: {reason}"),
        }
    }
}

impl std::error::Error for CliWireClientError {}

/// Build a shared [`WireClient`] dialing `host` with operator-supplied
/// TLS material.
///
/// The returned `Arc<WireClient>` is the underlying QUIC connection
/// pool; per-service clients (`AdminServiceClient`, `WriteServiceClient`,
/// etc.) wrap it via `*Client::new(Arc::clone(&wire))` so a single CLI
/// invocation reuses one connection across multiple RPCs.
///
/// `tls.cert_path` is required and is loaded as the client trust anchor —
/// the same PEM the operator hands the server via `--tls-cert`.
///
/// `tls.server_name` (when set) is used as the SNI / cert-verification
/// name; otherwise we derive it from the host portion of `host` (or
/// fall back to `"localhost"` for IP-only addresses).
fn build_wire_client(host: &str, tls: TlsConfig) -> Result<Arc<WireClient>, CliWireClientError> {
    let server_addr = host
        .to_socket_addrs()
        .map_err(|e| CliWireClientError::InvalidHost {
            host: host.to_owned(),
            reason: format!("failed to resolve {host}: {e}"),
        })?
        .next()
        .ok_or_else(|| CliWireClientError::InvalidHost {
            host: host.to_owned(),
            reason: format!("no addresses for {host}"),
        })?;

    // Derive SNI name: explicit `--tls-server-name` wins; otherwise use the
    // host portion of `--host` (only when it's a hostname, not a raw IP);
    // otherwise fall back to `"localhost"`.
    let server_name = tls.server_name.clone().unwrap_or_else(|| {
        host.rsplit_once(':')
            .map(|(h, _)| h.trim_start_matches('[').trim_end_matches(']'))
            .filter(|h| h.parse::<std::net::IpAddr>().is_err())
            .map(|h| h.to_owned())
            .unwrap_or_else(|| "localhost".to_owned())
    });

    let cert_path = tls.cert_path.as_ref().ok_or(CliWireClientError::MissingTlsCert)?;
    let crypto = crate::tls::load_wire_tls_client_config(cert_path)
        .map_err(|source| CliWireClientError::TlsLoad { source })?;
    let quic = wire_tls::client_config(crypto);

    let wire_config = WireClientConfig {
        server_addr,
        server_name,
        quic,
        // Inter-CLI auth payload is empty: peer mTLS at the QUIC layer
        // already authenticates the connection. Future work can layer an
        // operator-identity token here.
        auth_payload: Bytes::new(),
        connect_timeout: Duration::from_secs(5),
    };

    let client = WireClient::new(wire_config)
        .map_err(|e| CliWireClientError::WireClient { reason: e.to_string() })?;
    Ok(Arc::new(client))
}

/// Build an [`AdminServiceClient`] dialing `host` with operator-supplied
/// TLS material.
pub(crate) fn build_admin_client(
    host: &str,
    tls: TlsConfig,
) -> Result<AdminServiceClient, CliWireClientError> {
    let wire = build_wire_client(host, tls)?;
    Ok(AdminServiceClient::new(wire))
}

/// Build a [`UserServiceClient`] dialing `host` with operator-supplied
/// TLS material.
pub(crate) fn build_user_client(
    host: &str,
    tls: TlsConfig,
) -> Result<UserServiceClient, CliWireClientError> {
    let wire = build_wire_client(host, tls)?;
    Ok(UserServiceClient::new(wire))
}

/// Build a [`VaultServiceClient`] dialing `host` with operator-supplied
/// TLS material.
pub(crate) fn build_vault_client(
    host: &str,
    tls: TlsConfig,
) -> Result<VaultServiceClient, CliWireClientError> {
    let wire = build_wire_client(host, tls)?;
    Ok(VaultServiceClient::new(wire))
}

/// Build a [`WriteServiceClient`] dialing `host` with operator-supplied
/// TLS material.
pub(crate) fn build_write_client(
    host: &str,
    tls: TlsConfig,
) -> Result<WriteServiceClient, CliWireClientError> {
    let wire = build_wire_client(host, tls)?;
    Ok(WriteServiceClient::new(wire))
}

/// Build a [`ReadServiceClient`] dialing `host` with operator-supplied
/// TLS material.
pub(crate) fn build_read_client(
    host: &str,
    tls: TlsConfig,
) -> Result<ReadServiceClient, CliWireClientError> {
    let wire = build_wire_client(host, tls)?;
    Ok(ReadServiceClient::new(wire))
}
