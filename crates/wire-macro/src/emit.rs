//! Code emission for the `define_protocol!` macro.
//!
//! D.2 implements the server-side surface: per-service traits, unary +
//! streaming dispatch fns, the populated `OpCode` enum, an `is_registered_opcode`
//! const, and an `op_reflect_registry()` helper that snapshots every
//! registered RPC. D.3 extends this module with client-side generation —
//! a `{Service}Client` struct per service whose methods wrap
//! `WireClient::unary` / `WireClient::server_stream`, plus a single
//! shared `TypedResponseStream<T>` wrapper that decodes streaming chunks.
//! D.4 wires the macro into a test consumer crate to validate the
//! emitted code via `trybuild`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::ast::{ProtocolAst, RpcAst, ServiceAst};

/// Top-level entry point — fan out emission across helpers, the typed
/// stream wrapper, services (server traits + dispatch), clients, the
/// `OpCode` enum, the registry predicate, and the reflection table.
pub(crate) fn emit(protocol: &ProtocolAst) -> TokenStream {
    let helpers = emit_helpers();
    let typed_stream = emit_typed_response_stream();
    let services_code = protocol.services.iter().map(emit_service);
    let clients_code = protocol.services.iter().map(emit_client);
    let opcode_enum = emit_opcode_enum(protocol);
    let registry = emit_registry();
    let reflection = emit_op_reflect(protocol);

    quote! {
        #helpers
        #typed_stream
        #(#services_code)*
        #(#clients_code)*
        #opcode_enum
        #registry
        #reflection
    }
}

/// Emit the three internal `__decode_error`, `__encode_error`, and
/// `__unknown_opcode_error` helpers used by every dispatch arm. Marked
/// `#[doc(hidden)]` since they're macro-internal plumbing.
fn emit_helpers() -> TokenStream {
    quote! {
        #[doc(hidden)]
        #[inline]
        fn __decode_error(
            type_name: &'static str,
            err: ::postcard::Error,
        ) -> ::inferadb_ledger_wire::WireError {
            ::inferadb_ledger_wire::WireError {
                code: ::inferadb_ledger_wire::ErrorCode::InvalidArgument,
                message: ::std::format!("failed to decode {type_name}: {err}"),
                retryable: false,
                retry_after_ms: 0,
                context: ::std::collections::BTreeMap::new(),
                suggested_action: ::std::string::String::from(
                    "check request encoding matches the protocol schema",
                ),
            }
        }

        #[doc(hidden)]
        #[inline]
        fn __encode_error(
            type_name: &'static str,
            err: ::postcard::Error,
        ) -> ::inferadb_ledger_wire::WireError {
            ::inferadb_ledger_wire::WireError {
                code: ::inferadb_ledger_wire::ErrorCode::Internal,
                message: ::std::format!("failed to encode {type_name}: {err}"),
                retryable: false,
                retry_after_ms: 0,
                context: ::std::collections::BTreeMap::new(),
                suggested_action: ::std::string::String::from(
                    "internal server error; report bug",
                ),
            }
        }

        #[doc(hidden)]
        #[inline]
        fn __unknown_opcode_error(opcode: u16) -> ::inferadb_ledger_wire::WireError {
            ::inferadb_ledger_wire::WireError {
                code: ::inferadb_ledger_wire::ErrorCode::InvalidArgument,
                message: ::std::format!("unknown opcode {:#06x}", opcode),
                retryable: false,
                retry_after_ms: 0,
                context: ::std::collections::BTreeMap::new(),
                suggested_action: ::std::string::String::from(
                    "check protocol version compatibility",
                ),
            }
        }
    }
}

/// Emit the single shared `TypedResponseStream<T>` wrapper that decodes
/// each successful chunk's payload as `T` via postcard. Errors propagate
/// as [`::inferadb_ledger_wire_transport::RpcError`].
///
/// `ResponseStream` is `Unpin` (it boxes a pinned dyn-stream behind a
/// regular outer field) and `PhantomData<T>` is always `Unpin`, so we
/// declare `TypedResponseStream<T>: Unpin` explicitly. That gives
/// `Pin<&mut Self>` a `DerefMut` impl, which lets the safe
/// `Pin::new(&mut self.inner)` projection compile — no `unsafe` needed.
fn emit_typed_response_stream() -> TokenStream {
    quote! {
        /// Strongly-typed wrapper around
        /// [`::inferadb_ledger_wire_transport::ResponseStream`].
        ///
        /// Decodes each successful chunk's payload as `T` via postcard.
        /// Errors propagate as
        /// [`::inferadb_ledger_wire_transport::RpcError`].
        pub struct TypedResponseStream<T>
        where
            T: for<'de> ::serde::Deserialize<'de>,
        {
            inner: ::inferadb_ledger_wire_transport::ResponseStream,
            _phantom: ::core::marker::PhantomData<T>,
        }

        // `ResponseStream` is `Unpin` (its only field is a
        // `Pin<Box<dyn Stream + Send>>`, which is `Unpin`), and
        // `PhantomData<T>` is always `Unpin`. Declaring `Unpin`
        // explicitly gives `Pin<&mut Self>` access to its `DerefMut`
        // blanket impl so the `&mut self.inner` projection in
        // `poll_next` compiles without `unsafe`.
        impl<T> ::core::marker::Unpin for TypedResponseStream<T>
        where
            T: for<'de> ::serde::Deserialize<'de>,
        {}

        impl<T> TypedResponseStream<T>
        where
            T: for<'de> ::serde::Deserialize<'de>,
        {
            #[doc(hidden)]
            pub fn new(inner: ::inferadb_ledger_wire_transport::ResponseStream) -> Self {
                Self { inner, _phantom: ::core::marker::PhantomData }
            }
        }

        impl<T> ::futures::Stream for TypedResponseStream<T>
        where
            T: for<'de> ::serde::Deserialize<'de>,
        {
            type Item = ::core::result::Result<
                T,
                ::inferadb_ledger_wire_transport::RpcError,
            >;

            fn poll_next(
                mut self: ::core::pin::Pin<&mut Self>,
                cx: &mut ::core::task::Context<'_>,
            ) -> ::core::task::Poll<::core::option::Option<Self::Item>> {
                // `Self: Unpin` is declared above, so `Pin<&mut Self>`
                // implements `DerefMut`. The `&mut self.inner` reborrow
                // is therefore safe and the inner `Pin::new` projection
                // is valid.
                let pinned = ::core::pin::Pin::new(&mut self.inner);
                match ::futures::Stream::poll_next(pinned, cx) {
                    ::core::task::Poll::Pending => ::core::task::Poll::Pending,
                    ::core::task::Poll::Ready(::core::option::Option::None) => {
                        ::core::task::Poll::Ready(::core::option::Option::None)
                    }
                    ::core::task::Poll::Ready(::core::option::Option::Some(
                        ::core::result::Result::Err(err),
                    )) => ::core::task::Poll::Ready(::core::option::Option::Some(
                        ::core::result::Result::Err(err),
                    )),
                    ::core::task::Poll::Ready(::core::option::Option::Some(
                        ::core::result::Result::Ok(payload),
                    )) => {
                        let item = ::postcard::from_bytes::<T>(&payload).map_err(|err| {
                            ::inferadb_ledger_wire_transport::RpcError::ProtocolViolation(
                                ::std::format!("decode stream chunk: {err}"),
                            )
                        });
                        ::core::task::Poll::Ready(::core::option::Option::Some(item))
                    }
                }
            }
        }
    }
}

fn emit_service(service: &ServiceAst) -> TokenStream {
    let trait_def = emit_service_trait(service);
    let unary_dispatch = emit_unary_dispatch_fn(service);
    let stream_dispatch = emit_stream_dispatch_fn(service);
    quote! {
        #trait_def
        #unary_dispatch
        #stream_dispatch
    }
}

/// Emit a `{Service}Client` struct holding an `Arc<WireClient>` plus one
/// async method per RPC declared in the service. Unary methods return
/// the decoded response; streaming methods return a
/// `TypedResponseStream<Resp>`.
fn emit_client(service: &ServiceAst) -> TokenStream {
    let client_struct_name = format_ident!("{}Client", service.name);
    let methods = service.rpcs.iter().map(emit_client_method);

    quote! {
        #[derive(::core::clone::Clone)]
        #[allow(missing_docs)]
        pub struct #client_struct_name {
            client: ::std::sync::Arc<::inferadb_ledger_wire_transport::WireClient>,
        }

        #[allow(missing_docs)]
        impl #client_struct_name {
            pub fn new(
                client: ::std::sync::Arc<::inferadb_ledger_wire_transport::WireClient>,
            ) -> Self {
                Self { client }
            }

            #(#methods)*
        }
    }
}

/// Emit one async client method. Unary RPCs return `Result<Resp,
/// RpcError>`; streaming RPCs return `Result<TypedResponseStream<Resp>,
/// RpcError>`. Both encode the request via postcard and surface
/// encoding/decoding failures as `RpcError::ProtocolViolation`.
///
/// Idempotency token, trace context, and deadline default to zero — the
/// SDK layer (Phase 0.0.E) will provide higher-level builders that
/// thread those fields through.
fn emit_client_method(rpc: &RpcAst) -> TokenStream {
    let method_name = &rpc.name;
    let opcode = rpc.opcode;
    let request_type = &rpc.request_type;
    let response_type = &rpc.response_type;
    let request_type_str = quote!(#request_type).to_string();
    let response_type_str = quote!(#response_type).to_string();

    let unary_request_construction = quote! {
        let payload = ::postcard::to_allocvec(&request).map_err(|err| {
            ::inferadb_ledger_wire_transport::RpcError::ProtocolViolation(
                ::std::format!("encode {}: {}", #request_type_str, err),
            )
        })?;
        let unary = ::inferadb_ledger_wire_transport::UnaryRequest {
            opcode: #opcode,
            request_id,
            idempotency_token: [0u8; 16],
            trace_id: [0u8; 16],
            span_id: [0u8; 8],
            trace_flags: 0,
            causality_commit_index: 0,
            deadline_unix_nanos: 0,
            payload: ::bytes::Bytes::from(payload),
        };
    };

    if rpc.streaming {
        quote! {
            pub async fn #method_name(
                &self,
                request: #request_type,
                request_id: u128,
            ) -> ::core::result::Result<
                TypedResponseStream<#response_type>,
                ::inferadb_ledger_wire_transport::RpcError,
            > {
                #unary_request_construction
                let raw_stream = self.client.server_stream(unary).await?;
                ::core::result::Result::Ok(TypedResponseStream::new(raw_stream))
            }
        }
    } else {
        quote! {
            pub async fn #method_name(
                &self,
                request: #request_type,
                request_id: u128,
            ) -> ::core::result::Result<
                #response_type,
                ::inferadb_ledger_wire_transport::RpcError,
            > {
                #unary_request_construction
                let response_payload = self.client.unary(unary).await?;
                let response: #response_type = ::postcard::from_bytes(&response_payload)
                    .map_err(|err| {
                        ::inferadb_ledger_wire_transport::RpcError::ProtocolViolation(
                            ::std::format!("decode {}: {}", #response_type_str, err),
                        )
                    })?;
                ::core::result::Result::Ok(response)
            }
        }
    }
}

fn emit_service_trait(service: &ServiceAst) -> TokenStream {
    let trait_name = &service.name;
    let methods = service.rpcs.iter().map(emit_trait_method);
    quote! {
        #[allow(missing_docs)]
        pub trait #trait_name: ::core::marker::Send + ::core::marker::Sync + 'static {
            #(#methods)*
        }
    }
}

fn emit_trait_method(rpc: &RpcAst) -> TokenStream {
    let method_name = &rpc.name;
    let request_type = &rpc.request_type;
    let response_type = &rpc.response_type;

    if rpc.streaming {
        quote! {
            fn #method_name(
                &self,
                request: #request_type,
                ctx: ::inferadb_ledger_wire::RequestContext,
                sink: ::inferadb_ledger_wire_transport::StreamSink,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<(), ::inferadb_ledger_wire::WireError>,
            > + ::core::marker::Send;
        }
    } else {
        quote! {
            fn #method_name(
                &self,
                request: #request_type,
                ctx: ::inferadb_ledger_wire::RequestContext,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<#response_type, ::inferadb_ledger_wire::WireError>,
            > + ::core::marker::Send;
        }
    }
}

fn emit_unary_dispatch_fn(service: &ServiceAst) -> TokenStream {
    let dispatch_fn_name =
        format_ident!("dispatch_{}_unary", to_snake_case(&service.name.to_string()));
    let trait_name = &service.name;
    let arms = service.rpcs.iter().filter(|r| !r.streaming).map(|rpc| {
        let opcode = rpc.opcode;
        let method_name = &rpc.name;
        let request_type = &rpc.request_type;
        let response_type = &rpc.response_type;
        let request_type_str = quote!(#request_type).to_string();
        let response_type_str = quote!(#response_type).to_string();
        quote! {
            #opcode => {
                let request: #request_type = ::postcard::from_bytes(&request_payload)
                    .map_err(|err| __decode_error(#request_type_str, err))?;
                let response = service.#method_name(request, ctx).await?;
                let bytes = ::postcard::to_allocvec(&response)
                    .map_err(|err| __encode_error(#response_type_str, err))?;
                ::core::result::Result::Ok(::bytes::Bytes::from(bytes))
            }
        }
    });

    quote! {
        #[allow(missing_docs)]
        pub async fn #dispatch_fn_name<S: #trait_name>(
            service: &S,
            opcode: u16,
            request_payload: ::bytes::Bytes,
            ctx: ::inferadb_ledger_wire::RequestContext,
        ) -> ::core::result::Result<::bytes::Bytes, ::inferadb_ledger_wire::WireError> {
            match opcode {
                #(#arms)*
                other => ::core::result::Result::Err(__unknown_opcode_error(other)),
            }
        }
    }
}

fn emit_stream_dispatch_fn(service: &ServiceAst) -> TokenStream {
    let dispatch_fn_name =
        format_ident!("dispatch_{}_stream", to_snake_case(&service.name.to_string()));
    let trait_name = &service.name;
    let arms = service.rpcs.iter().filter(|r| r.streaming).map(|rpc| {
        let opcode = rpc.opcode;
        let method_name = &rpc.name;
        let request_type = &rpc.request_type;
        let request_type_str = quote!(#request_type).to_string();
        quote! {
            #opcode => {
                let request: #request_type = ::postcard::from_bytes(&request_payload)
                    .map_err(|err| __decode_error(#request_type_str, err))?;
                service.#method_name(request, ctx, sink).await
            }
        }
    });

    quote! {
        #[allow(missing_docs)]
        pub async fn #dispatch_fn_name<S: #trait_name>(
            service: &S,
            opcode: u16,
            request_payload: ::bytes::Bytes,
            ctx: ::inferadb_ledger_wire::RequestContext,
            sink: ::inferadb_ledger_wire_transport::StreamSink,
        ) -> ::core::result::Result<(), ::inferadb_ledger_wire::WireError> {
            match opcode {
                #(#arms)*
                other => ::core::result::Result::Err(__unknown_opcode_error(other)),
            }
        }
    }
}

/// Build `(variant_ident, opcode, streaming, service_name, method_name)`
/// quintuples for every RPC in source order — the shared raw material
/// for the `OpCode` enum and the reflection table.
struct OpCodeVariant<'a> {
    variant: proc_macro2::Ident,
    opcode: u16,
    streaming: bool,
    service_name: &'a syn::Ident,
    method_name: &'a syn::Ident,
}

fn collect_variants(protocol: &ProtocolAst) -> Vec<OpCodeVariant<'_>> {
    protocol
        .services
        .iter()
        .flat_map(|s| {
            s.rpcs.iter().map(move |r| OpCodeVariant {
                variant: format_ident!("{}{}", s.name, to_pascal_case(&r.name.to_string())),
                opcode: r.opcode,
                streaming: r.streaming,
                service_name: &s.name,
                method_name: &r.name,
            })
        })
        .collect()
}

fn emit_opcode_enum(protocol: &ProtocolAst) -> TokenStream {
    let variants = collect_variants(protocol);

    let variant_decls = variants.iter().map(|v| {
        let name = &v.variant;
        let opcode = v.opcode;
        quote! { #name = #opcode, }
    });
    let from_arms = variants.iter().map(|v| {
        let name = &v.variant;
        let opcode = v.opcode;
        quote! { #opcode => ::core::option::Option::Some(OpCode::#name), }
    });
    let name_arms = variants.iter().map(|v| {
        let name = &v.variant;
        let display = format!("{}::{}", v.service_name, v.method_name);
        quote! { OpCode::#name => #display, }
    });

    // `is_streaming` body — guard against the zero-streaming-RPCs case so
    // we don't emit the syntactically invalid `matches!(self, )`.
    let any_streaming = variants.iter().any(|v| v.streaming);
    let is_streaming_body = if any_streaming {
        let streaming_pats = variants.iter().filter(|v| v.streaming).map(|v| {
            let name = &v.variant;
            quote! { OpCode::#name }
        });
        quote! { ::core::matches!(self, #(#streaming_pats)|*) }
    } else {
        quote! { false }
    };

    // `name` body needs at least one arm to compile. If the protocol
    // declared no RPCs, `name_arms` is empty — fall back to an
    // `unreachable!` so the empty-enum case still produces valid Rust.
    // (D.1's parser rejects zero-service protocols, but a service with
    // zero RPCs is allowed.)
    let any_variants = !variants.is_empty();
    let name_body = if any_variants {
        quote! {
            match self {
                #(#name_arms)*
            }
        }
    } else {
        // No variants — `match self { }` on an empty enum is the canonical
        // "uninhabited" pattern in Rust, valid in const fn since 1.83 via
        // `#[allow]` because the lint flags it as suspicious. Use a
        // const-fn-friendly fallback that compiles even on stable.
        quote! { ::core::unreachable!() }
    };

    quote! {
        #[derive(
            ::core::fmt::Debug,
            ::core::clone::Clone,
            ::core::marker::Copy,
            ::core::cmp::PartialEq,
            ::core::cmp::Eq,
            ::core::hash::Hash,
        )]
        #[repr(u16)]
        #[allow(missing_docs)]
        pub enum OpCode {
            #(#variant_decls)*
        }

        #[allow(missing_docs)]
        impl OpCode {
            pub const fn opcode(self) -> u16 {
                self as u16
            }

            pub const fn from_opcode(opcode: u16) -> ::core::option::Option<Self> {
                match opcode {
                    #(#from_arms)*
                    _ => ::core::option::Option::None,
                }
            }

            pub const fn name(self) -> &'static str {
                #name_body
            }

            pub const fn is_streaming(self) -> bool {
                #is_streaming_body
            }
        }
    }
}

fn emit_registry() -> TokenStream {
    quote! {
        #[doc(hidden)]
        pub const fn is_registered_opcode(opcode: u16) -> bool {
            OpCode::from_opcode(opcode).is_some()
        }
    }
}

fn emit_op_reflect(protocol: &ProtocolAst) -> TokenStream {
    let entries = protocol.services.iter().flat_map(|s| {
        s.rpcs.iter().map(move |r| {
            let opcode = r.opcode;
            let display_name = format!("{}::{}", s.name, r.name);
            // `quote!` shorthand can't reach into a struct field
            // (`#r.request_type` would be parsed as `r . request_type`
            // outside the macro context), so bind a local reference first.
            let request_type = &r.request_type;
            let response_type = &r.response_type;
            let request_type_str = quote!(#request_type).to_string();
            let response_type_str = quote!(#response_type).to_string();
            let streaming = r.streaming;
            quote! {
                OpReflectEntry {
                    opcode: #opcode,
                    name: #display_name,
                    request_type: #request_type_str,
                    response_type: #response_type_str,
                    streaming: #streaming,
                }
            }
        })
    });

    quote! {
        #[derive(::core::fmt::Debug, ::core::clone::Clone, ::core::marker::Copy)]
        #[allow(missing_docs)]
        pub struct OpReflectEntry {
            pub opcode: u16,
            pub name: &'static str,
            pub request_type: &'static str,
            pub response_type: &'static str,
            pub streaming: bool,
        }

        #[allow(missing_docs)]
        pub fn op_reflect_registry() -> ::std::vec::Vec<OpReflectEntry> {
            ::std::vec![ #(#entries),* ]
        }
    }
}

/// `ReadService` -> `read_service`. We push an `_` before any uppercase
/// run that isn't at the start, lower-case each character — keep the
/// implementation simple since service names are short and ASCII.
fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// `watch_blocks` -> `WatchBlocks`. Capitalize the first character and
/// every character following an underscore; drop the underscores.
fn to_pascal_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut capitalize = true;
    for ch in s.chars() {
        if ch == '_' {
            capitalize = true;
        } else if capitalize {
            out.extend(ch.to_uppercase());
            capitalize = false;
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use proc_macro2::TokenStream;
    use quote::quote;

    use super::*;

    fn emit_string(ts: TokenStream) -> String {
        let proto: ProtocolAst = syn::parse2(ts).unwrap();
        emit(&proto).to_string()
    }

    #[test]
    fn emits_trait_for_unary_rpc() {
        let s = emit_string(quote! {
            service Foo {
                base: 0x0100,
                rpc 0x0100 hello(Req) -> Resp;
            }
        });
        assert!(s.contains("pub trait Foo"), "got: {s}");
        assert!(s.contains("fn hello"));
        assert!(s.contains("RequestContext"));
        assert!(s.contains("WireError"));
    }

    #[test]
    fn emits_streaming_method_with_sink() {
        let s = emit_string(quote! {
            service Foo {
                base: 0x0100,
                rpc 0x0100 watch(Req) -> stream Event;
            }
        });
        assert!(s.contains("StreamSink"));
        assert!(s.contains("dispatch_foo_stream"));
    }

    #[test]
    fn emits_dispatch_unary_match_arms() {
        let s = emit_string(quote! {
            service Foo {
                base: 0x0100,
                rpc 0x0100 a(R) -> S;
                rpc 0x0101 b(R) -> S;
            }
        });
        assert!(s.contains("256u16"), "expected 0x0100 (256u16); got: {s}");
        assert!(s.contains("257u16"), "expected 0x0101 (257u16); got: {s}");
    }

    #[test]
    fn emits_opcode_enum_with_variants() {
        let s = emit_string(quote! {
            service Foo {
                base: 0x0100,
                rpc 0x0100 hello(Req) -> Resp;
            }
        });
        assert!(s.contains("pub enum OpCode"));
        assert!(s.contains("FooHello"));
    }

    #[test]
    fn emits_op_reflect_registry() {
        let s = emit_string(quote! {
            service Foo {
                base: 0x0100,
                rpc 0x0100 hello(Req) -> Resp;
                rpc 0x0101 watch(WatchReq) -> stream Event;
            }
        });
        assert!(s.contains("op_reflect_registry"));
        assert!(s.contains("Foo :: hello") || s.contains("Foo::hello"));
        assert!(s.contains("Foo :: watch") || s.contains("Foo::watch"));
        assert!(s.contains("streaming : true"));
        assert!(s.contains("streaming : false"));
    }

    #[test]
    fn emits_is_registered_opcode() {
        let s = emit_string(quote! {
            service Foo { base: 0x0100, rpc 0x0100 hello(R) -> S; }
        });
        assert!(s.contains("is_registered_opcode"));
    }

    #[test]
    fn handles_zero_streaming_rpcs() {
        let s = emit_string(quote! {
            service Foo { base: 0x0100, rpc 0x0100 hello(R) -> S; }
        });
        assert!(s.contains("is_streaming"));
        // Zero streaming RPCs -> body is the literal `false`, not a
        // `matches!` invocation. Confirm we don't emit the empty
        // `matches!(self, )` form.
        assert!(!s.contains("matches ! (self ,)"), "got: {s}");
    }

    #[test]
    fn emits_dispatch_for_multiple_services() {
        // Pick ranges that don't overlap — 0x0100..=0x01FF and
        // 0x0200..=0x02FF.
        let s = emit_string(quote! {
            service ReadService {
                base: 0x0100,
                rpc 0x0100 read(R) -> S;
            }
            service WriteService {
                base: 0x0200,
                rpc 0x0200 write(R) -> S;
            }
        });
        assert!(s.contains("pub trait ReadService"));
        assert!(s.contains("pub trait WriteService"));
        assert!(s.contains("dispatch_read_service_unary"));
        assert!(s.contains("dispatch_write_service_unary"));
        assert!(s.contains("ReadServiceRead"));
        assert!(s.contains("WriteServiceWrite"));
    }

    #[test]
    fn snake_case_helper() {
        assert_eq!(to_snake_case("ReadService"), "read_service");
        assert_eq!(to_snake_case("APIService"), "a_p_i_service");
        assert_eq!(to_snake_case("foo"), "foo");
    }

    #[test]
    fn pascal_case_helper() {
        assert_eq!(to_pascal_case("hello"), "Hello");
        assert_eq!(to_pascal_case("hello_world"), "HelloWorld");
        assert_eq!(to_pascal_case("read_v2"), "ReadV2");
    }

    #[test]
    fn emits_client_struct_per_service() {
        let s = emit_string(quote! {
            service ReadService {
                base: 0x0010,
                rpc 0x0010 read(ReadRequest) -> ReadResponse;
            }
        });
        assert!(s.contains("pub struct ReadServiceClient"), "got: {s}");
        assert!(s.contains("pub fn new"));
        assert!(s.contains("pub async fn read"));
    }

    #[test]
    fn emits_unary_client_method_with_postcard() {
        let s = emit_string(quote! {
            service Foo {
                base: 0x0100,
                rpc 0x0100 hello(Req) -> Resp;
            }
        });
        assert!(s.contains("postcard :: to_allocvec"));
        assert!(s.contains("postcard :: from_bytes"));
        assert!(s.contains("self . client . unary"));
        // Unary-only protocol shouldn't reference server_stream from a
        // generated client method, but the shared `TypedResponseStream`
        // wrapper is always emitted, so a literal "server_stream"
        // substring is fine — what matters is that the unary method
        // doesn't call it.
        assert!(!s.contains("self . client . server_stream"));
    }

    #[test]
    fn emits_streaming_client_method_with_typed_stream() {
        let s = emit_string(quote! {
            service Foo {
                base: 0x0100,
                rpc 0x0100 watch(Req) -> stream Event;
            }
        });
        assert!(s.contains("TypedResponseStream < Event >"));
        assert!(s.contains("self . client . server_stream"));
        assert!(!s.contains("self . client . unary"));
    }

    #[test]
    fn emits_typed_response_stream_once() {
        let s = emit_string(quote! {
            service A { base: 0x0100, rpc 0x0100 a(R) -> stream E; }
            service B { base: 0x0200, rpc 0x0200 b(R) -> stream E2; }
        });
        let count = s.matches("pub struct TypedResponseStream").count();
        assert_eq!(count, 1, "TypedResponseStream emitted multiple times: {s}");
    }

    #[test]
    fn client_method_carries_correct_opcode() {
        let s = emit_string(quote! {
            service Foo {
                base: 0x0100,
                rpc 0x0142 do_thing(R) -> S;
            }
        });
        assert!(s.contains("opcode : 322u16"), "expected 0x0142 = 322, got: {s}");
    }
}
