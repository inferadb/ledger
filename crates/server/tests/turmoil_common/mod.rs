//! Turmoil-based test harness for deterministic network simulation.
//!
//! This module provides utilities for testing distributed system behavior
//! under simulated network conditions like partitions, delays, and message loss.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]

use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use hyper_util::rt::TokioIo;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::Service;
use turmoil::net::{TcpListener, TcpStream};

/// Wrapper for turmoil's TcpStream for use with tonic servers.
///
/// This implements the traits needed by tonic's `serve_with_incoming`.
pub struct ServerStream(pub TcpStream);

impl tokio::io::AsyncRead for ServerStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for ServerStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl tonic::transport::server::Connected for ServerStream {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

/// Wrapper for turmoil's TcpStream for use with tonic clients.
///
/// This implements the hyper traits needed by the tower connector.
pub struct ClientStream(TokioIo<TcpStream>);

impl ClientStream {
    pub fn new(stream: TcpStream) -> Self {
        Self(TokioIo::new(stream))
    }
}

impl hyper::rt::Read for ClientStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for ClientStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl tonic::transport::server::Connected for ClientStream {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

/// Connector for tonic clients that uses turmoil's simulated network.
#[derive(Clone)]
pub struct TurmoilConnector;

impl Service<Uri> for TurmoilConnector {
    type Response = ClientStream;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        Box::pin(async move {
            let host = uri.host().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing host in URI")
            })?;
            let port = uri.port_u16().unwrap_or(9999);
            let addr = format!("{}:{}", host, port);

            let stream = TcpStream::connect(&addr).await?;
            Ok(ClientStream::new(stream))
        })
    }
}

/// Create a tonic channel that uses turmoil's simulated network.
pub async fn create_turmoil_channel(
    host: &str,
    port: u16,
) -> Result<Channel, tonic::transport::Error> {
    let uri = format!("http://{}:{}", host, port);
    Endpoint::new(uri)?.connect_with_connector(TurmoilConnector).await
}

/// Create a turmoil-compatible TCP listener stream for tonic servers.
pub fn incoming_stream(
    addr: SocketAddr,
) -> impl futures::Stream<Item = Result<ServerStream, io::Error>> {
    async_stream::stream! {
        let listener = TcpListener::bind(addr).await.expect("bind listener");
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    yield Ok(ServerStream(stream));
                }
                Err(e) => {
                    yield Err(e);
                }
            }
        }
    }
}

/// Node information for a simulated cluster.
pub struct SimulatedNode {
    pub id: u64,
    pub host: String,
    pub port: u16,
}

impl SimulatedNode {
    pub fn new(id: u64) -> Self {
        Self { id, host: format!("node{}", id), port: 9999 }
    }

    pub fn addr(&self) -> SocketAddr {
        format!("0.0.0.0:{}", self.port).parse().unwrap()
    }

    pub fn peer_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Configuration for a simulated cluster test.
pub struct SimulatedClusterConfig {
    pub num_nodes: usize,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
}

impl Default for SimulatedClusterConfig {
    fn default() -> Self {
        Self {
            num_nodes: 3,
            election_timeout: Duration::from_millis(150),
            heartbeat_interval: Duration::from_millis(50),
        }
    }
}
