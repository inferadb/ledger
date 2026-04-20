# Fuzzing

Fuzz tests use [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html) with libFuzzer to test parsing and deserialization code paths against arbitrary input.

## Setup

```bash
# Install cargo-fuzz (requires nightly Rust)
cargo install cargo-fuzz

# List available fuzz targets
cargo +nightly fuzz list --fuzz-dir fuzz
```

## Running Fuzz Targets

```bash
# Run a specific fuzz target (runs indefinitely until Ctrl+C)
cargo +nightly fuzz run fuzz_proto_convert --fuzz-dir fuzz

# Run for a limited time (300 seconds)
cargo +nightly fuzz run fuzz_postcard_codec --fuzz-dir fuzz -- -max_total_time=300

# Run all targets as a smoke test (60 seconds each)
for target in fuzz_proto_convert fuzz_postcard_codec fuzz_btree_keys fuzz_pagination_token fuzz_email_hmac fuzz_encrypted_page fuzz_region_serde; do
    cargo +nightly fuzz run "$target" --fuzz-dir fuzz -- -max_total_time=60
done
```

## Fuzz Targets

| Target                  | Attack Surface                                                              |
| ----------------------- | --------------------------------------------------------------------------- |
| `fuzz_proto_convert`    | Protobuf deserialization (gRPC requests)                                    |
| `fuzz_postcard_codec`   | Postcard codec for domain types                                             |
| `fuzz_btree_keys`       | B+ tree key encoding/decoding, varint                                       |
| `fuzz_pagination_token` | HMAC-signed pagination token parsing                                        |
| `fuzz_email_hmac`       | HMAC-SHA256 email blinding input handling                                   |
| `fuzz_encrypted_page`   | Page-level envelope-encryption decode + authentication tag validation       |
| `fuzz_region_serde`     | `Region` enum (de)serialization across stable wire + runtime representations |

## Investigating Crashes

```bash
# Reproduce a crash
cargo +nightly fuzz run fuzz_proto_convert --fuzz-dir fuzz fuzz/artifacts/fuzz_proto_convert/crash-*

# Minimize a crash input
cargo +nightly fuzz tmin fuzz_proto_convert --fuzz-dir fuzz fuzz/artifacts/fuzz_proto_convert/crash-*
```

## CI

Fuzz tests run nightly via GitHub Actions (`.github/workflows/fuzz.yml`), 5 minutes per target. Crash artifacts are uploaded on failure.
