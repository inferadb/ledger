# inferadb-ledger-proto

Generated protobuf types, gRPC service traits, and domain type conversions for [InferaDB](https://github.com/inferadb) [Ledger](https://github.com/inferadb/ledger).

## Overview

This crate provides the wire-format layer for InferaDB Ledger:

- **Proto types**: Generated request/response messages, enums, and nested types
- **gRPC traits**: Service definitions for Read, Write, Admin, Health, and Discovery
- **Conversions**: Bidirectional `From`/`TryFrom` impls between proto and domain types

## Usage

```rust
use inferadb_ledger_proto::proto::{
    WriteRequest, ReadRequest, Operation,
    write_service_client::WriteServiceClient,
    read_service_client::ReadServiceClient,
};

// Convert between domain and proto types
use inferadb_ledger_types::Operation as DomainOp;
let domain_op = DomainOp::set_entity("key", b"value".to_vec());
let proto_op: Operation = domain_op.into();
```

## License

MIT OR Apache-2.0
