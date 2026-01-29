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

use std::time::Duration;

use inferadb_ledger_sdk::{
    CertificateData, ClientConfig, LedgerClient, Result, ServerSource, TlsConfig,
};

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

    let ca_cert = args.iter().position(|a| a == "--ca-cert").and_then(|i| args.get(i + 1).cloned());

    let client_cert =
        args.iter().position(|a| a == "--client-cert").and_then(|i| args.get(i + 1).cloned());

    let client_key =
        args.iter().position(|a| a == "--client-key").and_then(|i| args.get(i + 1).cloned());

    let use_native_roots = args.iter().any(|a| a == "--native-roots");

    println!("=== TLS Connection Example ===\n");
    println!("Endpoint: {endpoint}");
    println!("CA cert: {}", ca_cert.as_deref().unwrap_or("(none)"));
    println!("Client cert: {}", client_cert.as_deref().unwrap_or("(none)"));
    println!("Native roots: {use_native_roots}");
    println!();

    // -------------------------------------------------------------------------
    // Example 1: TLS with native root certificates
    // -------------------------------------------------------------------------
    // Use this when connecting to a server with a certificate signed by a
    // well-known CA (Let's Encrypt, DigiCert, etc.)

    if use_native_roots && ca_cert.is_none() {
        println!("--- Example: Using native root certificates ---");

        let tls = TlsConfig::with_native_roots()?;

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static([endpoint]))
            .client_id("tls-native-roots-example")
            .timeout(Duration::from_secs(10))
            .tls(tls)
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

        // Read the CA certificate from file
        let ca_data = std::fs::read(ca_path).expect("failed to read CA certificate");

        // Build TLS config based on options provided
        let tls = match (&client_cert, &client_key) {
            // Mutual TLS with client certificate
            (Some(cert_path), Some(key_path)) => {
                println!("Configuring mutual TLS with client certificate");
                let cert_data =
                    std::fs::read(cert_path).expect("failed to read client certificate");
                let key_data = std::fs::read(key_path).expect("failed to read client key");

                if use_native_roots {
                    println!("Using both custom CA and native roots");
                    TlsConfig::builder()
                        .ca_cert(CertificateData::Pem(ca_data))
                        .client_cert(CertificateData::Pem(cert_data))
                        .client_key(key_data)
                        .use_native_roots(true)
                        .build()?
                } else {
                    TlsConfig::builder()
                        .ca_cert(CertificateData::Pem(ca_data))
                        .client_cert(CertificateData::Pem(cert_data))
                        .client_key(key_data)
                        .build()?
                }
            },
            // Simple TLS (server verification only)
            _ => {
                if use_native_roots {
                    println!("Using both custom CA and native roots");
                    TlsConfig::builder()
                        .ca_cert(CertificateData::Pem(ca_data))
                        .use_native_roots(true)
                        .build()?
                } else {
                    TlsConfig::builder().ca_cert(CertificateData::Pem(ca_data)).build()?
                }
            },
        };

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static([endpoint]))
            .client_id("tls-custom-ca-example")
            .timeout(Duration::from_secs(10))
            .tls(tls)
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
    println!("   TlsConfig::with_native_roots()?;\n");

    println!("2. Custom CA certificate (from bytes):");
    println!("   let ca_data = std::fs::read(\"/path/to/ca.pem\")?;");
    println!("   TlsConfig::builder()");
    println!("       .ca_cert(CertificateData::Pem(ca_data))");
    println!("       .build()?;\n");

    println!("3. Custom CA certificate (DER format):");
    println!("   let ca_data = std::fs::read(\"/path/to/ca.der\")?;");
    println!("   TlsConfig::builder()");
    println!("       .ca_cert(CertificateData::Der(ca_data))");
    println!("       .build()?;\n");

    println!("4. Mutual TLS (mTLS) with client certificate:");
    println!("   let ca_data = std::fs::read(\"/path/to/ca.pem\")?;");
    println!("   let cert_data = std::fs::read(\"/path/to/client.pem\")?;");
    println!("   let key_data = std::fs::read(\"/path/to/client.key\")?;");
    println!("   TlsConfig::builder()");
    println!("       .ca_cert(CertificateData::Pem(ca_data))");
    println!("       .client_cert(CertificateData::Pem(cert_data))");
    println!("       .client_key(key_data)");
    println!("       .build()?;\n");

    println!("5. Domain name override (when cert CN doesn't match hostname):");
    println!("   TlsConfig::builder()");
    println!("       .ca_cert(CertificateData::Pem(ca_data))");
    println!("       .domain_name(\"actual-domain.example.com\")");
    println!("       .build()?;\n");

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
        },
        Err(e) => {
            println!("✗ Health check failed: {e}");
        },
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
        },
        Err(e) => {
            println!("Could not get detailed health: {e}");
        },
    }

    Ok(())
}
