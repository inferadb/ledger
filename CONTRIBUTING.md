# Contributing to InferaDB Ledger

We welcome contributions! Please read the guidelines below.

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). Report unacceptable behavior to [open@inferadb.com](mailto:open@inferadb.com).

## Reporting Issues

- **Bugs**: Search existing issues first. Include version, reproduction steps, expected vs actual behavior, and logs.
- **Features**: Describe the use case, proposed solution, and alternatives.
- **Security**: Email [security@inferadb.com](mailto:security@inferadb.com). Do not open public issues.

## Pull Requests

1. Fork and branch from `main`
2. Follow [Conventional Commits](https://www.conventionalcommits.org/)
3. Run `just check` before submitting (format + lint + test)
4. Update documentation for API or behavior changes
5. Submit with a clear description

**PR title must follow Conventional Commits format** (validated by CI):

- `feat: add user authentication`
- `fix(api): handle empty requests`

## Development Setup

```bash
# Install tools
mise trust && mise install

# Build and test
just build
just test

# Pre-commit validation
just check

# See all commands
just
```

See [README.md](README.md) for prerequisites.

## Fuzzing

Fuzz tests use [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html) with libFuzzer to test parsing and deserialization code paths against arbitrary input.

### Setup

```bash
# Install cargo-fuzz (requires nightly Rust)
cargo install cargo-fuzz

# List available fuzz targets
cargo +nightly fuzz list --fuzz-dir fuzz
```

### Running Fuzz Targets

```bash
# Run a specific fuzz target (runs indefinitely until Ctrl+C)
cargo +nightly fuzz run fuzz_proto_convert --fuzz-dir fuzz

# Run for a limited time (300 seconds)
cargo +nightly fuzz run fuzz_postcard_codec --fuzz-dir fuzz -- -max_total_time=300

# Run all targets as a smoke test (60 seconds each)
for target in fuzz_proto_convert fuzz_postcard_codec fuzz_btree_keys fuzz_pagination_token; do
    cargo +nightly fuzz run "$target" --fuzz-dir fuzz -- -max_total_time=60
done
```

### Fuzz Targets

| Target                  | Attack Surface                           |
| ----------------------- | ---------------------------------------- |
| `fuzz_proto_convert`    | Protobuf deserialization (gRPC requests) |
| `fuzz_postcard_codec`   | Postcard codec for domain types          |
| `fuzz_btree_keys`       | B+ tree key encoding/decoding, varint    |
| `fuzz_pagination_token` | HMAC-signed pagination token parsing     |

### Investigating Crashes

```bash
# Reproduce a crash
cargo +nightly fuzz run fuzz_proto_convert --fuzz-dir fuzz fuzz/artifacts/fuzz_proto_convert/crash-*

# Minimize a crash input
cargo +nightly fuzz tmin fuzz_proto_convert --fuzz-dir fuzz fuzz/artifacts/fuzz_proto_convert/crash-*
```

### CI

Fuzz tests run nightly via GitHub Actions (`.github/workflows/fuzz.yml`), 5 minutes per target. Crash artifacts are uploaded on failure.

## Property Testing

Property tests use [proptest](https://proptest-rs.github.io/proptest/) to verify invariants across randomly generated inputs. Unlike fuzz tests (which target crash-inducing inputs), property tests assert logical properties that must hold for all valid inputs.

### Running Property Tests

```bash
# Run with default iterations (256 cases per test)
cargo test --workspace --lib -- proptest

# Run with high iteration count (10k cases per test)
just test-proptest

# Custom iteration count
just test-proptest 50000
```

### Test Suites

| Suite              | Crate   | Properties                                                                                                                                                            |
| ------------------ | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Codec roundtrip    | `types` | Encode/decode cycle preserves all serializable types (Operation, Entity, Relationship, Transaction, BlockHeader, VaultBlock, VaultEntry, ShardBlock, ChainCommitment) |
| Merkle proofs      | `types` | Valid proofs always verify, tampered proofs never verify, wrong roots never verify, root computation is deterministic                                                 |
| B+ tree invariants | `store` | Inserted keys retrievable, iteration returns sorted keys, deletes remove only target keys, updates overwrite correctly, get/iteration consistency                     |
| Raft log ordering  | `raft`  | Monotonically increasing indices, non-decreasing terms, LogId ordering consistency, no gaps within terms, LedgerRequest serialization roundtrip                       |

### Writing New Property Tests

1. Add strategies to `crates/test-utils/src/strategies.rs` for reusable input generators
2. Place property tests in a `proptest_*` inner module within the test module of the target crate
3. Name test functions with a `prop_` prefix so `cargo test -- proptest` filters correctly
4. Use `inferadb-ledger-test-utils` strategies where possible to avoid duplication

### CI

Property tests run at default iterations (256) on every PR via the standard CI workflow. A dedicated nightly workflow (`.github/workflows/proptest.yml`) runs 10,000 iterations per test to catch rare edge cases.

## Review Process

1. CI runs tests, linters, and formatters
2. A maintainer reviews your contribution
3. Address feedback
4. Maintainer merges on approval

## License

Contributions are dual-licensed under [Apache 2.0](LICENSE-APACHE) and [MIT](LICENSE-MIT).

## Questions?

- [Discord](https://discord.gg/inferadb)
- [open@inferadb.com](mailto:open@inferadb.com)
