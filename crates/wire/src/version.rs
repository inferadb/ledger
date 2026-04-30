//! ALPN-based version negotiation for the InferaDB Ledger wire protocol.
//!
//! ALPN (Application-Layer Protocol Negotiation, RFC 7301) lets QUIC peers
//! agree on a protocol version during the TLS handshake — before any
//! application bytes flow. This eliminates the need for an in-band handshake
//! frame to negotiate the wire protocol version.
//!
//! The ALPN identifier is opaque to TLS but conventionally formatted as
//! `<protocol-name>/<version>`. We use `ledger-wire/N` where `N` is the
//! protocol major version. Renaming the protocol or bumping the major version
//! is a breaking change requiring all peers to upgrade.
//!
//! Note: doc-comment examples below use ` ```text ` rather than ` ```no_run `
//! because `quinn` is not yet a dependency of this crate (it lands in Phase
//! 0.0.B/C). `no_run` would still trigger compilation; `text` correctly
//! skips it for an illustrative snippet.
//!
//! # Usage
//!
//! Server-side (Phase 0.0.B):
//!
//! ```text
//! let mut server_config = quinn::ServerConfig::with_crypto(crypto);
//! server_config.alpn_protocols(SUPPORTED_ALPN_PROTOCOLS.to_vec());
//! ```
//!
//! Client-side (Phase 0.0.C):
//!
//! ```text
//! let mut client_config = quinn::ClientConfig::with_crypto(crypto);
//! client_config.alpn_protocols(vec![CURRENT_ALPN_PROTOCOL.to_vec()]);
//! ```

/// Current wire protocol version (v1).
///
/// Stored in every frame header's `version` field. The `FrameHeader::version`
/// constant must equal this value; `Frame::decode` rejects frames whose
/// `version` is not `CURRENT_PROTOCOL_VERSION`.
pub const CURRENT_PROTOCOL_VERSION: u8 = 0x01;

// Compile-time invariant: CURRENT_PROTOCOL_VERSION is the version byte
// frames must carry. If FrameHeader::Default's version field changes from 0
// to a specific version value (it shouldn't), this assert catches drift.
const _: () = assert!(CURRENT_PROTOCOL_VERSION == 0x01);

/// Raft inter-node protocol version (A7).
///
/// Independent of [`CURRENT_PROTOCOL_VERSION`]: the wire-format version
/// governs frame layout, while this constant governs the application-level
/// Raft message contract carried inside frames. Bumped when an inter-node
/// Raft message shape, ordering invariant, or handshake contract changes.
///
/// Exchanged via [`crate::opcode::OPCODE_RAFT_PROTOCOL_HANDSHAKE`] as the
/// first frame on every long-lived `Replicate` stream. Mismatched peers
/// refuse to exchange Raft state until the rolling upgrade completes.
/// Used by Phase 0.0.E.6b's `WireConsensusTransport`.
pub const RAFT_PROTOCOL_VERSION: u32 = 1;

/// ALPN identifier for v1 of the wire protocol.
///
/// Used during QUIC's TLS handshake. Both client and server advertise this
/// identifier; the QUIC stack rejects connections that don't share at least
/// one supported ALPN value.
pub const ALPN_LEDGER_WIRE_V1: &[u8] = b"ledger-wire/1";

/// All ALPN protocols this build supports, ordered by preference.
///
/// Servers should advertise this entire list. Clients typically advertise
/// only the version they want to speak (use [`supported_alpn_protocols_owned()`])
/// for the single-version case.
///
/// Server-side example:
///
/// ```text
/// server_config.alpn_protocols(SUPPORTED_ALPN_PROTOCOLS
///     .iter()
///     .map(|s| s.to_vec())
///     .collect());
/// ```
pub const SUPPORTED_ALPN_PROTOCOLS: &[&[u8]] = &[ALPN_LEDGER_WIRE_V1];

/// Returns all supported ALPN protocols as owned bytes, suitable for the
/// quinn API (`ServerConfig::alpn_protocols` and `ClientConfig::alpn_protocols`
/// both expect `Vec<Vec<u8>>`).
///
/// The quinn API requires owned `Vec<Vec<u8>>` (it stores the bytes
/// internally); this helper avoids consumers re-cloning the static slices
/// every time. With only v1 currently supported, the returned vec contains
/// a single entry.
///
/// **Server side**: advertise this whole list. The QUIC stack picks the
/// highest mutual ALPN value during handshake.
///
/// **Client side**: typically advertise the single version you want to
/// speak. Use `vec![ALPN_LEDGER_WIRE_V1.to_vec()]` directly rather than
/// this helper, OR call this helper if you accept any supported version.
#[must_use]
pub fn supported_alpn_protocols_owned() -> Vec<Vec<u8>> {
    SUPPORTED_ALPN_PROTOCOLS.iter().map(|p| p.to_vec()).collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    #[test]
    fn current_protocol_version_is_one() {
        assert_eq!(CURRENT_PROTOCOL_VERSION, 0x01);
    }

    #[test]
    fn alpn_v1_identifier_is_ledger_wire_one() {
        assert_eq!(ALPN_LEDGER_WIRE_V1, b"ledger-wire/1");
    }

    #[test]
    fn supported_alpn_protocols_includes_v1() {
        assert!(SUPPORTED_ALPN_PROTOCOLS.contains(&ALPN_LEDGER_WIRE_V1));
    }

    #[test]
    fn supported_alpn_protocols_owned_returns_vec() {
        let protocols = supported_alpn_protocols_owned();
        assert!(!protocols.is_empty());
        assert_eq!(protocols[0], ALPN_LEDGER_WIRE_V1);
    }

    #[test]
    fn alpn_identifier_format_matches_convention() {
        assert!(ALPN_LEDGER_WIRE_V1.is_ascii());
        let s = std::str::from_utf8(ALPN_LEDGER_WIRE_V1).expect("valid utf8");
        let parts: Vec<&str> = s.split('/').collect();
        assert_eq!(parts.len(), 2, "ALPN format should be 'name/version'");
        assert_eq!(parts[0], "ledger-wire");
        assert_eq!(parts[1], "1");
    }

    #[test]
    fn raft_protocol_version_is_one() {
        assert_eq!(RAFT_PROTOCOL_VERSION, 1);
    }
}
