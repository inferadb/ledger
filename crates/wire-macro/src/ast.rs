//! AST types produced by the `define_protocol!` parser.
//!
//! These types capture the parsed shape of the protocol DSL. D.1 parses
//! the grammar into this AST and validates per-service / cross-service
//! invariants; D.2 consumes the same AST to emit server-side code
//! (traits + dispatch fns + `OpCode` enum + registry + reflection). D.3
//! will add client-side generation.

use syn::{Ident, Type};

/// The full parsed protocol — every `service { ... }` block in source
/// order.
#[derive(Debug, Clone)]
pub(crate) struct ProtocolAst {
    pub(crate) services: Vec<ServiceAst>,
}

/// A single `service Name { base: ..., rpc ...; rpc ...; }` block.
#[derive(Debug, Clone)]
pub(crate) struct ServiceAst {
    pub(crate) name: Ident,
    pub(crate) base: u16,
    /// Span of the `base` literal — surfaces error messages on the right
    /// token when validation rejects an overlapping or duplicate base.
    pub(crate) base_span: proc_macro2::Span,
    pub(crate) rpcs: Vec<RpcAst>,
}

/// A single `rpc <opcode> <name>(<Req>) -> <return spec>;` line.
#[derive(Debug, Clone)]
pub(crate) struct RpcAst {
    pub(crate) opcode: u16,
    /// Span of the opcode literal — surfaces "duplicate opcode" /
    /// "outside service range" errors on the offending token.
    pub(crate) opcode_span: proc_macro2::Span,
    pub(crate) name: Ident,
    pub(crate) request_type: Type,
    pub(crate) response_type: Type,
    /// `true` when the RPC was declared `-> stream T` (server-streaming);
    /// `false` for plain unary `-> T`.
    pub(crate) streaming: bool,
}
