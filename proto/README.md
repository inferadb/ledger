# Protocol Buffer Definitions

This directory contains the gRPC service and message definitions for InferaDB Ledger.

## Structure

```
proto/
├── buf.yaml          # Buf module configuration
├── buf.gen.yaml      # Code generation configuration
└── ledger/
    └── v1/
        └── ledger.proto  # Main proto file
```

## Prerequisites

Install the [Buf CLI](https://docs.buf.build/installation):

```bash
# macOS
brew install bufbuild/buf/buf

# Linux
curl -sSL https://github.com/bufbuild/buf/releases/latest/download/buf-Linux-x86_64 -o /usr/local/bin/buf
chmod +x /usr/local/bin/buf
```

## Code Generation

**Development builds** use `build.rs` with `tonic-prost-build` to compile protos at build time.

**Published crates** (crates.io) use pre-generated code in `crates/raft/src/generated/` since proto files aren't included in the package.

To update the pre-generated fallback file:

```bash
cargo build -p inferadb-ledger-raft
cp target/debug/build/inferadb-ledger-raft-*/out/ledger.v1.rs \
   crates/raft/src/generated/
```

## Linting and Breaking Change Detection

```bash
# Lint proto files
buf lint

# Check for breaking changes
buf breaking --against 'https://github.com/inferadb/ledger.git#subdir=proto'
```

## Services Defined

| Service                  | Description                                            |
| ------------------------ | ------------------------------------------------------ |
| `ReadService`            | Read operations (Get, List, VerifiedRead, WatchBlocks) |
| `WriteService`           | Write operations (Write, BatchWrite)                   |
| `AdminService`           | Administrative operations (Organization/Vault management) |
| `HealthService`          | Health checks                                          |
| `SystemDiscoveryService` | Peer discovery and cluster state                       |
