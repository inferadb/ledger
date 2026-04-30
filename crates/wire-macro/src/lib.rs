//! `define_protocol!` macro — generates server trait + client struct +
//! dispatch fn + populated `OpCode` enum + opcode registry from a single
//! top-level DSL input. Phase 0.0.D of the SDK rewrite plan.
//!
//! D.1 implemented the parser and parse-time validation. D.2 wired in
//! server-side code generation — per-service traits, unary + streaming
//! dispatch fns, the populated `OpCode` enum, an `is_registered_opcode`
//! predicate, and an `op_reflect_registry()` snapshot helper. D.3 (this
//! commit) extends emission with client-side generation: a
//! `{Service}Client` struct per service whose methods wrap
//! `WireClient::unary` / `WireClient::server_stream` with the right
//! opcode plus postcard request encoding and response decoding, plus a
//! single shared `TypedResponseStream<T>` wrapper for streaming
//! responses. D.4 will validate the emitted code via `trybuild`.
//!
//! # Required dependencies
//!
//! Crates invoking `define_protocol!` must depend on the following
//! crates — the macro emits unqualified absolute paths (`::bytes::Bytes`,
//! `::postcard::to_allocvec`, etc.) that resolve in the consumer's
//! dependency graph rather than this proc-macro's:
//!
//! - `inferadb-ledger-wire`
//! - `inferadb-ledger-wire-transport`
//! - `bytes`
//! - `postcard`
//! - `serde` (with the `derive` feature)
//! - `futures` — the emitted `TypedResponseStream` impls `futures::Stream`
//! - `tokio` — transitively required by the transport's async fns
//!
//! All request and response types referenced from the protocol DSL must
//! derive `serde::{Serialize, Deserialize}`.

mod ast;
mod emit;
mod parse;

use proc_macro::TokenStream;
use syn::parse_macro_input;

use crate::ast::ProtocolAst;

/// Define the entire wire protocol surface: services, opcodes, request
/// and response types, and unary-vs-streaming shape. Cross-service
/// opcode uniqueness, per-service range bounds, and service-range
/// overlap are validated at macro expansion time. See the plan at
/// `docs/superpowers/plans/2026-04-30-region-aware-sdk-multiplexer.md`
/// § Phase 0.0.D for the full grammar.
#[proc_macro]
pub fn define_protocol(input: TokenStream) -> TokenStream {
    let protocol = parse_macro_input!(input as ProtocolAst);
    crate::emit::emit(&protocol).into()
}
