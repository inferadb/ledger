//! `syn::Parse` implementations for the `define_protocol!` DSL.
//!
//! The grammar (informal):
//!
//! ```text
//! protocol     := service+
//! service      := "service" Ident "{" "base" ":" Lit "," rpc+ "}"
//! rpc          := "rpc" Lit Ident "(" Type ")" "->" return_type ";"
//! return_type  := "stream" Type | Type
//! ```
//!
//! Services are whitespace-separated at top level (no commas/semicolons
//! between them). RPCs inside a service body are semicolon-terminated.
//!
//! Validation performed at parse time:
//! - per-service: every RPC opcode must lie in `[base, base + 0xFF]`.
//! - per-service: no two RPCs share an opcode.
//! - cross-service: no two services have overlapping effective opcode ranges. A service's effective
//!   range is `[base, min(base + 0xFF, next_strictly_higher_base - 1)]` — the nominal 256-opcode
//!   block, clamped to avoid intruding on a higher-based service. This accommodates `ReadService`'s
//!   `base = 0x0010` next to `WriteService`'s `base = 0x0100`.
//! - cross-service: no two RPCs share the same opcode (subsumed by the range-overlap check, but
//!   kept as a defensive direct check).
//! - non-empty: at least one service is required.

use syn::{
    Ident, LitInt, Token, Type, braced, parenthesized,
    parse::{Parse, ParseStream, Result},
};

use crate::ast::{ProtocolAst, RpcAst, ServiceAst};

mod kw {
    syn::custom_keyword!(service);
    syn::custom_keyword!(base);
    syn::custom_keyword!(rpc);
    syn::custom_keyword!(stream);
}

impl Parse for ProtocolAst {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut services = Vec::new();
        while !input.is_empty() {
            services.push(input.parse::<ServiceAst>()?);
        }
        if services.is_empty() {
            return Err(input.error("define_protocol! requires at least one service"));
        }
        validate_cross_service(&services)?;
        Ok(ProtocolAst { services })
    }
}

impl Parse for ServiceAst {
    fn parse(input: ParseStream) -> Result<Self> {
        input.parse::<kw::service>()?;
        let name: Ident = input.parse()?;

        let body;
        braced!(body in input);

        body.parse::<kw::base>()?;
        body.parse::<Token![:]>()?;
        let base_lit: LitInt = body.parse()?;
        let base_span = base_lit.span();
        let base: u16 = base_lit.base10_parse()?;
        body.parse::<Token![,]>()?;

        let mut rpcs = Vec::new();
        while !body.is_empty() {
            let rpc = body.parse::<RpcAst>()?;
            // Per-service range bound: opcode must be in [base, base + 0xFF].
            if rpc.opcode < base || rpc.opcode > base.saturating_add(0xFF) {
                return Err(syn::Error::new(
                    rpc.opcode_span,
                    format!(
                        "rpc opcode {:#06x} is outside service `{}` range \
                         ({:#06x}..={:#06x})",
                        rpc.opcode,
                        name,
                        base,
                        base.saturating_add(0xFF)
                    ),
                ));
            }
            rpcs.push(rpc);
        }

        // Within-service uniqueness check.
        let mut seen = std::collections::HashSet::new();
        for rpc in &rpcs {
            if !seen.insert(rpc.opcode) {
                return Err(syn::Error::new(
                    rpc.opcode_span,
                    format!("duplicate rpc opcode {:#06x} within service `{}`", rpc.opcode, name),
                ));
            }
        }

        Ok(ServiceAst { name, base, base_span, rpcs })
    }
}

impl Parse for RpcAst {
    fn parse(input: ParseStream) -> Result<Self> {
        input.parse::<kw::rpc>()?;
        let opcode_lit: LitInt = input.parse()?;
        let opcode_span = opcode_lit.span();
        let opcode: u16 = opcode_lit.base10_parse()?;

        let name: Ident = input.parse()?;

        let request_args;
        parenthesized!(request_args in input);
        let request_type: Type = request_args.parse()?;
        if !request_args.is_empty() {
            return Err(request_args.error("rpc takes exactly one request type"));
        }

        input.parse::<Token![->]>()?;

        let streaming = input.peek(kw::stream);
        if streaming {
            input.parse::<kw::stream>()?;
        }
        let response_type: Type = input.parse()?;

        input.parse::<Token![;]>()?;

        Ok(RpcAst { opcode, opcode_span, name, request_type, response_type, streaming })
    }
}

fn validate_cross_service(services: &[ServiceAst]) -> Result<()> {
    // Cross-service opcode uniqueness. The range-overlap check below will
    // catch any service-level overlap that produces colliding opcodes,
    // but a direct check against the offending opcode's span gives a
    // sharper diagnostic.
    let mut seen: std::collections::HashMap<u16, &Ident> = std::collections::HashMap::new();
    for service in services {
        for rpc in &service.rpcs {
            if let Some(other_service) = seen.get(&rpc.opcode) {
                return Err(syn::Error::new(
                    rpc.opcode_span,
                    format!(
                        "rpc opcode {:#06x} (in service `{}`) collides with the \
                         same opcode in service `{}`",
                        rpc.opcode, service.name, other_service
                    ),
                ));
            }
            seen.insert(rpc.opcode, &service.name);
        }
    }

    // Cross-service range overlap. The effective end of a service is the
    // smaller of:
    //   * `base + 0xFF` — the macro's nominal 256-opcode allocation, and
    //   * `next_strictly_higher_base - 1` — the highest opcode that does not intrude on another
    //     service's declared base.
    //
    // The latter accommodates services like `ReadService` whose `base`
    // sits inside the first 256-opcode block alongside the reserved range
    // (`0x0000`–`0x000F`): its declared base is `0x0010`, but its
    // effective ceiling is `0x00FF` because the next service is
    // `WriteService` at `0x0100`. Without this clamp, the nominal
    // `[0x0010, 0x010F]` range would falsely overlap with `WriteService`.
    //
    // The clamp only considers *strictly higher* bases — services that
    // share a base still produce overlapping ranges and surface the
    // diagnostic, which matches the macro's defensive intent.
    //
    // Two services overlap when each one's base falls within the other's
    // effective range.
    let effective_end = |service: &ServiceAst| -> u16 {
        let nominal = service.base.saturating_add(0xFF);
        let next_higher =
            services.iter().filter(|other| other.base > service.base).map(|other| other.base).min();
        match next_higher {
            ::core::option::Option::Some(next) => nominal.min(next.saturating_sub(1)),
            ::core::option::Option::None => nominal,
        }
    };

    for (i, a) in services.iter().enumerate() {
        let a_end = effective_end(a);
        for b in services.iter().skip(i + 1) {
            let b_end = effective_end(b);
            let intersects = a.base <= b_end && b.base <= a_end;
            if intersects {
                return Err(syn::Error::new(
                    b.base_span,
                    format!(
                        "service `{}` range ({:#06x}..={:#06x}) overlaps with \
                         service `{}` range ({:#06x}..={:#06x})",
                        b.name, b.base, b_end, a.name, a.base, a_end,
                    ),
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use proc_macro2::TokenStream;
    use quote::quote;

    use super::*;

    fn parse(ts: TokenStream) -> syn::Result<ProtocolAst> {
        syn::parse2(ts)
    }

    #[test]
    fn parses_minimal_valid_protocol() {
        let ts = quote! {
            service Foo {
                base: 0x0100,
                rpc 0x0100 hello(Req) -> Resp;
            }
        };
        let proto = parse(ts).unwrap();
        assert_eq!(proto.services.len(), 1);
        let foo = &proto.services[0];
        assert_eq!(foo.name.to_string(), "Foo");
        assert_eq!(foo.base, 0x0100);
        assert_eq!(foo.rpcs.len(), 1);
        assert_eq!(foo.rpcs[0].opcode, 0x0100);
        assert_eq!(foo.rpcs[0].name.to_string(), "hello");
        assert!(!foo.rpcs[0].streaming);
    }

    #[test]
    fn parses_streaming_rpc() {
        let ts = quote! {
            service Foo {
                base: 0x0100,
                rpc 0x0101 watch(Req) -> stream Event;
            }
        };
        let proto = parse(ts).unwrap();
        assert!(proto.services[0].rpcs[0].streaming);
    }

    #[test]
    fn parses_multi_service() {
        let ts = quote! {
            service A { base: 0x0100, rpc 0x0100 m1(R) -> S; }
            service B { base: 0x0200, rpc 0x0200 m2(R) -> S; }
        };
        let proto = parse(ts).unwrap();
        assert_eq!(proto.services.len(), 2);
    }

    #[test]
    fn rejects_opcode_outside_service_range() {
        let ts = quote! {
            service Foo {
                base: 0x0100,
                rpc 0x0200 oops(R) -> S;
            }
        };
        let err = parse(ts).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("outside service"), "got: {msg}");
    }

    #[test]
    fn rejects_duplicate_opcode_within_service() {
        let ts = quote! {
            service Foo {
                base: 0x0100,
                rpc 0x0100 a(R) -> S;
                rpc 0x0100 b(R) -> S;
            }
        };
        let err = parse(ts).unwrap_err();
        assert!(err.to_string().contains("duplicate"), "got: {err}");
    }

    #[test]
    fn rejects_cross_service_opcode_collision() {
        let ts = quote! {
            service A { base: 0x0100, rpc 0x0100 m(R) -> S; }
            service B { base: 0x0100, rpc 0x0100 m(R) -> S; }
        };
        let err = parse(ts).unwrap_err();
        let s = err.to_string();
        assert!(s.contains("collides") || s.contains("overlaps"), "got: {s}");
    }

    #[test]
    fn rejects_overlapping_service_ranges() {
        // Two services declared with the same base have identical
        // effective ranges and overlap. The per-RPC opcodes are distinct
        // (`0x0100` vs `0x0101`), so the overlap check — not the direct
        // opcode-collision check — must surface the diagnostic.
        let ts = quote! {
            service A { base: 0x0100, rpc 0x0100 m(R) -> S; }
            service B { base: 0x0100, rpc 0x0101 m(R) -> S; }
        };
        let err = parse(ts).unwrap_err();
        assert!(err.to_string().contains("overlaps"), "got: {err}");
    }

    #[test]
    fn allows_neighbouring_service_with_truncated_range() {
        // `ReadService`'s real-world allocation: `base: 0x0010` with the
        // next service at `base: 0x0100`. The nominal range would be
        // `[0x0010, 0x010F]` and overlap with the neighbour, but the
        // effective range clamps at `0x00FF` so this configuration is
        // accepted.
        let ts = quote! {
            service ReadService {
                base: 0x0010,
                rpc 0x0010 read(R) -> S;
            }
            service WriteService {
                base: 0x0100,
                rpc 0x0100 write(R) -> S;
            }
        };
        parse(ts).unwrap();
    }

    #[test]
    fn rejects_empty_protocol() {
        let ts = quote! {};
        let err = parse(ts).unwrap_err();
        assert!(err.to_string().contains("at least one service"));
    }
}
