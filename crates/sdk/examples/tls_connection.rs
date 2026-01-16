//! TLS connection example demonstrating secure connections.
//!
//! Run: `cargo run --example tls_connection -- --endpoint https://secure.example.com:443`
//!
//! This example shows:
//! - TLS configuration with CA certificates
//! - Mutual TLS (mTLS) with client certificates
//! - Using native root certificates
//! - Domain name override for certificate verification
//!
//! # Certificate Setup
//!
//! Before running this example, you'll need:
//!
//! ## For simple TLS (server verification only):
//! - CA certificate that signed the server's certificate (`ca.pem`)
//!
//! ## For mutual TLS (mTLS):
//! - CA certificate (`ca.pem`)
//! - Client certificate (`client.pem`)
//! - Client private key (`client.key`)
//!
//! ## Using native roots:
//! If your server uses a certificate signed by a well-known CA (like Let's Encrypt),
//! you can use the system's native root certificates instead of providing a CA file.

// Examples are allowed to use expect/unwrap for brevity
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]

use ledger_sdk::{ClientConfig, LedgerClient, Result, TlsConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let endpoint = args
        .iter()
        .position(|a| a == "--endpoint")
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
        .unwrap_or("https://localhost:50051");

    let ca_cert = args
        .iter()
        .position(|a| a == "--ca-cert")
        .and_then(|i| args.get(i + 1).cloned());

    let client_cert = args
        .iter()
        .position(|a| a == "--client-cert")
        .and_then(|i| args.get(i + 1).cloned());

    let client_key = args
        .iter()
        .position(|a| a == "--client-key")
        .and_then(|i| args.get(i + 1).cloned());

    let use_native_roots = args.iter().any(|a| a == "--native-roots");

    println!("=== TLS Connection Example ===\n");
    println!("Endpoint: {endpoint}");
    println!("CA cert: {}", ca_cert.as_deref().unwrap_or("(none)"));
    println!(
        "Client cert: {}",
        client_cert.as_deref().unwrap_or("(none)")
    );
    println!("Native roots: {use_native_roots}");
    println!();

    // -------------------------------------------------------------------------
    // Example 1: TLS with native root certificates
    // -------------------------------------------------------------------------
    // Use this when connecting to a server with a certificate signed by a
    // well-known CA (Let's Encrypt, DigiCert, etc.)

    if use_native_roots && ca_cert.is_none() {
        println!("--- Example: Using native root certificates ---");

        let tls = TlsConfig::with_native_roots();

        let config = ClientConfig::builder()
            .with_endpoint(endpoint)
            .with_client_id("tls-native-roots-example")
            .with_timeout(Duration::from_secs(10))
            .with_tls(tls)
            .build()?;

        println!("TLS configured with native roots");

        let client = LedgerClient::new(config).await?;
        println!("Connected successfully with native roots!\n");
        perform_health_check(&client).await?;
        return Ok(());
    }

    // -------------------------------------------------------------------------
    // Example 2: TLS with custom CA certificate
    // -------------------------------------------------------------------------
    // Use this for internal/private PKI or self-signed certificates

    if let Some(ref ca_path) = ca_cert {
        println!("--- Example: TLS with custom CA certificate ---");

        let mut tls = TlsConfig::new().with_ca_cert_pem(ca_path);

        // Optionally add native roots as fallback
        if use_native_roots {
            tls = TlsConfig::with_native_roots().with_ca_cert_pem(ca_path);
            println!("Using both custom CA and native roots");
        }

        // Add client certificate for mutual TLS if provided
        if let (Some(cert_path), Some(key_path)) = (&client_cert, &client_key) {
            println!("Configuring mutual TLS with client certificate");
            tls = tls.with_client_cert_pem(cert_path, key_path);
        }

        let config = ClientConfig::builder()
            .with_endpoint(endpoint)
            .with_client_id("tls-custom-ca-example")
            .with_timeout(Duration::from_secs(10))
            .with_tls(tls)
            .build()?;

        println!("TLS configuration created");

        let client = LedgerClient::new(config).await?;
        println!("Connected successfully with TLS!\n");
        perform_health_check(&client).await?;
        return Ok(());
    }

    // -------------------------------------------------------------------------
    // Example 3: Show all TLS options (documentation)
    // -------------------------------------------------------------------------

    println!("--- TLS Configuration Options ---\n");
    println!("No TLS configuration provided. Here are the available options:\n");

    println!("1. Native root certificates (for public CAs):");
    println!("   TlsConfig::with_native_roots()\n");

    println!("2. Custom CA certificate (PEM format):");
    println!("   TlsConfig::new().with_ca_cert_pem(\"/path/to/ca.pem\")\n");

    println!("3. Custom CA certificate (DER format):");
    println!("   TlsConfig::new().with_ca_cert_der(\"/path/to/ca.der\")\n");

    println!("4. From bytes (useful for embedded certs):");
    println!("   TlsConfig::new().with_ca_cert_pem_bytes(pem_bytes)\n");

    println!("5. Mutual TLS (mTLS) with client certificate:");
    println!("   TlsConfig::new()");
    println!("       .with_ca_cert_pem(\"/path/to/ca.pem\")");
    println!("       .with_client_cert_pem(\"/path/to/client.pem\", \"/path/to/client.key\")\n");

    println!("6. Domain name override (when cert CN doesn't match hostname):");
    println!("   TlsConfig::new()");
    println!("       .with_ca_cert_pem(\"/path/to/ca.pem\")");
    println!("       .with_domain_name(\"actual-domain.example.com\")\n");

    println!("Usage:");
    println!("  cargo run --example tls_connection -- --native-roots --endpoint https://...");
    println!("  cargo run --example tls_connection -- --ca-cert ca.pem --endpoint https://...");
    println!(
        "  cargo run --example tls_connection -- --ca-cert ca.pem --client-cert client.pem --client-key client.key"
    );

    Ok(())
}

/// Perform a simple health check to verify the connection works.
async fn perform_health_check(client: &LedgerClient) -> Result<()> {
    println!("Performing health check...");

    match client.health_check().await {
        Ok(healthy) => {
            if healthy {
                println!("✓ Server is healthy");
            } else {
                println!("⚠ Server is degraded");
            }
        }
        Err(e) => {
            println!("✗ Health check failed: {e}");
        }
    }

    // Get detailed health status
    match client.health_check_detailed().await {
        Ok(result) => {
            println!("\nDetailed health status:");
            println!("  Status: {:?}", result.status);
            if !result.message.is_empty() {
                println!("  Message: {}", result.message);
            }
            for (key, value) in &result.details {
                println!("  {key}: {value}");
            }
        }
        Err(e) => {
            println!("Could not get detailed health: {e}");
        }
    }

    Ok(())
}
