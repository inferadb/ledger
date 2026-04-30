//! End-to-end runtime test of the `define_protocol!` macro.
//!
//! Exercises the generated server traits and dispatch fns with a small
//! fake protocol + in-memory handlers, then asserts opcode routing,
//! decoder/encoder behaviour, and the `op_reflect_registry` contents.
//!
//! The trybuild tests prove emitted code COMPILES; this test proves it
//! BEHAVES correctly at runtime — request decoding, response encoding,
//! unknown-opcode rejection, malformed-payload rejection, streaming sink
//! integration, and the `OpCode` enum's round-trip.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, missing_docs)]

use std::{net::SocketAddr, sync::Arc, time::Instant};

use bytes::Bytes;
use inferadb_ledger_wire::{
    AuthMethod, CallerIdentity, ConnectionMetrics, RequestContext, WireError,
};
use inferadb_ledger_wire_transport::StreamSink;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct EchoRequest {
    pub value: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct EchoResponse {
    pub value: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct AddRequest {
    pub a: u32,
    pub b: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct AddResponse {
    pub sum: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct WatchRequest {
    pub limit: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct WatchEvent {
    pub n: u32,
}

inferadb_ledger_wire_macro::define_protocol! {
    service Math {
        base: 0x0100,
        rpc 0x0100 echo(EchoRequest) -> EchoResponse;
        rpc 0x0101 add(AddRequest) -> AddResponse;
    }
    service Counter {
        base: 0x0200,
        rpc 0x0200 watch(WatchRequest) -> stream WatchEvent;
    }
}

struct MathImpl;

impl Math for MathImpl {
    async fn echo(
        &self,
        request: EchoRequest,
        _ctx: RequestContext,
    ) -> Result<EchoResponse, WireError> {
        Ok(EchoResponse { value: request.value })
    }

    async fn add(
        &self,
        request: AddRequest,
        _ctx: RequestContext,
    ) -> Result<AddResponse, WireError> {
        Ok(AddResponse { sum: request.a + request.b })
    }
}

struct CounterImpl;

impl Counter for CounterImpl {
    async fn watch(
        &self,
        request: WatchRequest,
        _ctx: RequestContext,
        sink: StreamSink,
    ) -> Result<(), WireError> {
        for n in 0..request.limit {
            let event = WatchEvent { n };
            let bytes = postcard::to_allocvec(&event).unwrap();
            // Test sink is unbounded enough to never refuse.
            sink.send(Bytes::from(bytes)).await.ok();
        }
        Ok(())
    }
}

fn build_ctx() -> RequestContext {
    RequestContext {
        request_id: 0,
        idempotency_token: [0; 16],
        trace_id: [0; 16],
        span_id: [0; 8],
        trace_flags: 0,
        causality_commit_index: 0,
        deadline: None,
        caller_identity: CallerIdentity {
            user_id: None,
            app_id: None,
            organization_id: None,
            auth_method: AuthMethod::Anonymous,
            token_expiry: Instant::now(),
            revocation_epoch: 0,
        },
        peer_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        sdk_version: None,
        forwarded_for: None,
        correlation_id: 0,
        cancellation: CancellationToken::new(),
        conn_metrics: Arc::new(ConnectionMetrics),
    }
}

#[tokio::test]
async fn dispatch_routes_unary_opcodes() {
    let svc = MathImpl;

    // echo: 0x0100
    let request = postcard::to_allocvec(&EchoRequest { value: 42 }).unwrap();
    let response_bytes =
        dispatch_math_unary(&svc, 0x0100, Bytes::from(request), build_ctx()).await.unwrap();
    let response: EchoResponse = postcard::from_bytes(&response_bytes).unwrap();
    assert_eq!(response.value, 42);

    // add: 0x0101
    let request = postcard::to_allocvec(&AddRequest { a: 5, b: 7 }).unwrap();
    let response_bytes =
        dispatch_math_unary(&svc, 0x0101, Bytes::from(request), build_ctx()).await.unwrap();
    let response: AddResponse = postcard::from_bytes(&response_bytes).unwrap();
    assert_eq!(response.sum, 12);
}

#[tokio::test]
async fn dispatch_rejects_unknown_opcode() {
    let svc = MathImpl;
    let result = dispatch_math_unary(&svc, 0x9999, Bytes::new(), build_ctx()).await;
    let err = result.expect_err("unknown opcode must error");
    assert!(err.message.contains("unknown opcode"), "unexpected message: {}", err.message);
}

#[tokio::test]
async fn dispatch_rejects_malformed_payload() {
    let svc = MathImpl;
    // EchoRequest expects a `value: u32` encoded as a postcard varint.
    // A continuation byte (high bit set) with no following byte is an
    // incomplete varint; postcard returns `Error::DeserializeUnexpectedEnd`.
    let result = dispatch_math_unary(&svc, 0x0100, Bytes::from_static(b"\xff"), build_ctx()).await;
    let err = result.expect_err("malformed payload must error");
    assert!(
        err.message.contains("decode") || err.message.contains("EchoRequest"),
        "unexpected message: {}",
        err.message
    );
}

#[tokio::test]
async fn dispatch_streaming_routes_correctly() {
    let svc = CounterImpl;
    let (tx, mut rx) = mpsc::channel(16);
    let sink = StreamSink::new(tx);

    let request = postcard::to_allocvec(&WatchRequest { limit: 3 }).unwrap();
    let result =
        dispatch_counter_stream(&svc, 0x0200, Bytes::from(request), build_ctx(), sink).await;
    result.expect("stream dispatch returned Ok");

    // The sink was consumed by `dispatch_counter_stream`; once that future
    // resolved its sender was dropped, so the receiver will yield None
    // after draining the queued chunks.
    let mut received = Vec::new();
    while let Some(chunk) = rx.recv().await {
        let event: WatchEvent = postcard::from_bytes(&chunk).unwrap();
        received.push(event);
    }
    assert_eq!(received, vec![WatchEvent { n: 0 }, WatchEvent { n: 1 }, WatchEvent { n: 2 }]);
}

#[tokio::test]
async fn streaming_dispatch_rejects_unknown_opcode() {
    let svc = CounterImpl;
    let (tx, _rx) = mpsc::channel(16);
    let sink = StreamSink::new(tx);
    let result = dispatch_counter_stream(&svc, 0x9999, Bytes::new(), build_ctx(), sink).await;
    let err = result.expect_err("unknown stream opcode must error");
    assert!(err.message.contains("unknown opcode"), "unexpected message: {}", err.message);
}

#[test]
fn op_reflect_registry_contains_all_rpcs() {
    let entries = op_reflect_registry();
    assert_eq!(entries.len(), 3);

    let names: Vec<_> = entries.iter().map(|e| e.name).collect();
    assert!(names.contains(&"Math::echo"));
    assert!(names.contains(&"Math::add"));
    assert!(names.contains(&"Counter::watch"));

    let opcodes: Vec<u16> = entries.iter().map(|e| e.opcode).collect();
    assert!(opcodes.contains(&0x0100));
    assert!(opcodes.contains(&0x0101));
    assert!(opcodes.contains(&0x0200));

    // Streaming flag matches the DSL.
    let watch = entries.iter().find(|e| e.name == "Counter::watch").unwrap();
    assert!(watch.streaming);
    let echo = entries.iter().find(|e| e.name == "Math::echo").unwrap();
    assert!(!echo.streaming);
    let add = entries.iter().find(|e| e.name == "Math::add").unwrap();
    assert!(!add.streaming);
}

#[test]
fn opcode_enum_round_trip() {
    let echo = OpCode::from_opcode(0x0100).unwrap();
    assert_eq!(echo.opcode(), 0x0100);
    assert_eq!(echo.name(), "Math::echo");
    assert!(!echo.is_streaming());

    let add = OpCode::from_opcode(0x0101).unwrap();
    assert_eq!(add.name(), "Math::add");
    assert!(!add.is_streaming());

    let watch = OpCode::from_opcode(0x0200).unwrap();
    assert_eq!(watch.name(), "Counter::watch");
    assert!(watch.is_streaming());

    assert!(OpCode::from_opcode(0x9999).is_none());
}

#[test]
fn is_registered_opcode_matches_enum() {
    assert!(is_registered_opcode(0x0100));
    assert!(is_registered_opcode(0x0101));
    assert!(is_registered_opcode(0x0200));

    assert!(!is_registered_opcode(0x0102));
    assert!(!is_registered_opcode(0x0201));
    assert!(!is_registered_opcode(0x0000));
    assert!(!is_registered_opcode(0xFFFF));
}

/// Compile-time signature check: the generated `MathClient::echo`,
/// `MathClient::add`, and `CounterClient::watch` methods must accept
/// `(Req, u128)` and return a `Future<Output = Result<{Resp},
/// RpcError>>` (or `TypedResponseStream<Resp>` for streaming). This
/// forces the type checker to read the emitted method signatures even
/// though we can't drive a real `WireClient` here without a live
/// transport.
///
/// Marked `#[allow(dead_code)]` because the function is never invoked at
/// runtime — it exists only to drive type-checking of the body.
#[test]
fn client_method_signatures_compile() {
    #[allow(dead_code)]
    async fn _drive_math(client: MathClient) {
        let _: Result<EchoResponse, ::inferadb_ledger_wire_transport::RpcError> =
            client.echo(EchoRequest { value: 1 }, 0).await;
        let _: Result<AddResponse, ::inferadb_ledger_wire_transport::RpcError> =
            client.add(AddRequest { a: 1, b: 2 }, 0).await;
    }

    #[allow(dead_code)]
    async fn _drive_counter(client: CounterClient) {
        let _: Result<TypedResponseStream<WatchEvent>, ::inferadb_ledger_wire_transport::RpcError> =
            client.watch(WatchRequest { limit: 0 }, 0).await;
    }
}
