# Testing Guide

How to run and write tests for Ledger.

## Running Tests

### Quick Test (Recommended for Development)

```bash
just test-fast
# or
cargo +1.92 test -p inferadb-ledger-types -p inferadb-ledger-store
```

Runs unit tests for core crates (~15 seconds).

### Standard Test Suite

```bash
just test
# or
cargo +1.92 test --workspace
```

Runs all tests except those marked `#[ignore]` (~30 seconds).

### Full Test Suite

```bash
just test-full
# or
cargo +1.92 test --workspace -- --include-ignored
```

Includes slow integration tests (~5 minutes).

### Single Test

```bash
cargo +1.92 test test_name -- --nocapture
```

The `--nocapture` flag shows println output.

### Specific Crate

```bash
cargo +1.92 test -p inferadb-ledger-state
```

## Test Organization

```
crates/
├── types/src/
│   └── *.rs          # Unit tests in same file
├── store/
│   ├── src/*.rs      # Unit tests
│   └── tests/        # Integration tests
├── state/
│   ├── src/*.rs      # Unit tests
│   └── tests/        # Integration tests
├── raft/
│   ├── src/*.rs      # Unit tests
│   └── tests/        # Integration tests (cluster scenarios)
└── server/
    └── tests/        # End-to-end tests
```

## Writing Tests

### Unit Tests

Place unit tests in the same file as the code:

```rust
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_something() {
        let result = do_something();
        assert_eq!(result, expected);
    }
}
```

The `#[allow(...)]` permits `.unwrap()` and `.expect()` in tests only.

### Integration Tests

Create files in `tests/` directory:

```rust
// crates/state/tests/entity_crud.rs
use inferadb_ledger_state::*;

#[test]
fn test_entity_create_read_update_delete() {
    let engine = create_test_engine();
    // ...
}
```

### Async Tests

Use `#[tokio::test]` for async tests:

```rust
#[tokio::test]
async fn test_async_operation() {
    let client = create_test_client().await;
    let result = client.write(...).await.unwrap();
    assert!(result.is_ok());
}
```

### Test Utilities

Use the builder pattern for test configuration:

```rust
use inferadb_ledger_server::config::Config;

#[test]
fn test_with_config() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = Config::for_test(1, 50051, temp_dir.path().to_path_buf());
    // ...
}
```

### Slow Tests

Mark slow tests with `#[ignore]`:

```rust
#[test]
#[ignore] // Slow: runs full cluster bootstrap
fn test_cluster_bootstrap() {
    // This test takes > 30 seconds
}
```

These run only with `--include-ignored`.

## Test Patterns

### Table-Driven Tests

```rust
#[test]
fn test_validation() {
    let cases = vec![
        ("valid", true),
        ("", false),
        ("too_long_".repeat(100).as_str(), false),
    ];

    for (input, expected) in cases {
        let result = validate(input);
        assert_eq!(result.is_ok(), expected, "input: {}", input);
    }
}
```

### Error Testing

```rust
#[test]
fn test_error_conditions() {
    let result = operation_that_should_fail();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, MyError::SpecificVariant { .. }));
}
```

### Temporary Files

Use `tempfile` for test data:

```rust
#[test]
fn test_with_temp_dir() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("test.db");

    // Test code using path...

    // temp automatically cleaned up on drop
}
```

### Mock Services

For gRPC service tests:

```rust
#[tokio::test]
async fn test_write_service() {
    let (server, addr) = start_test_server().await;
    let client = connect_client(addr).await;

    let response = client.write(request).await.unwrap();

    assert_eq!(response.status, Status::Ok);
}
```

## Coverage

### Running with Coverage

```bash
cargo +1.92 llvm-cov --workspace --html
open target/llvm-cov/html/index.html
```

### Coverage Targets

| Crate  | Target |
| ------ | ------ |
| types  | 90%+   |
| store  | 90%+   |
| state  | 85%+   |
| raft   | 80%+   |
| server | 70%+   |

## CI Integration

Tests run on every PR:

```yaml
# .github/workflows/ci.yml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@1.92
      - run: cargo test --workspace
```

### Pre-commit Hook

Run before pushing:

```bash
just check
# Runs: fmt + clippy + test
```

Or quick version:

```bash
just check-quick
# Runs: fmt + clippy only
```

## Debugging Tests

### Verbose Output

```bash
RUST_LOG=debug cargo test test_name -- --nocapture
```

### Run Single Threaded

For tests with shared state:

```bash
cargo test -- --test-threads=1
```

### Backtrace on Failure

```bash
RUST_BACKTRACE=1 cargo test
```

## Test Best Practices

1. **Test behavior, not implementation**: Focus on what the code does, not how.

2. **One assertion per test** (when practical): Makes failures clear.

3. **Descriptive test names**: `test_entity_update_with_stale_version_returns_conflict`

4. **Arrange-Act-Assert pattern**:

   ```rust
   #[test]
   fn test_example() {
       // Arrange
       let entity = create_test_entity();

       // Act
       let result = entity.update(new_value);

       // Assert
       assert!(result.is_ok());
   }
   ```

5. **Avoid test interdependence**: Each test should set up its own state.

6. **Clean up resources**: Use RAII (tempfile, drop) for cleanup.

7. **Don't test private functions directly**: Test through public API.
