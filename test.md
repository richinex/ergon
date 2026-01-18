# Testing Guide

This document contains all commands for running tests and checking coverage in the ergon project.

## Quick Test Commands

### Run All Tests
```bash
cargo test --all
```

### Run Library Tests Only
```bash
cargo test --lib
```

### Run Doc Tests Only
```bash
cargo test --doc
```

### Run Workspace Tests (with all features)
```bash
cargo test --workspace --all-features --verbose
```

### Run Specific Test
```bash
cargo test --lib test_name -- --exact
```

### Run Test with Output
```bash
cargo test --lib test_name -- --exact --nocapture
```

## Code Quality Checks

### Run Clippy (Linter)
```bash
cargo clippy --workspace --all-targets --all-features
```

### Run Clippy with Strict Warnings
```bash
cargo clippy --workspace --all-targets --all-features -- -W clippy::all -D clippy::correctness
```

### Check Code Formatting
```bash
cargo fmt --all -- --check
```

### Format Code
```bash
cargo fmt --all
```

## Build Commands

### Debug Build
```bash
cargo build --workspace --all-features --verbose
```

### Release Build
```bash
cargo build --workspace --all-features --release --verbose
```

### Check Examples Compile
```bash
cargo check --workspace --examples --all-features
```

## Coverage Commands

### Install Coverage Tools
```bash
# Install llvm-tools-preview component
rustup component add llvm-tools-preview

# Install cargo-llvm-cov
cargo install cargo-llvm-cov
```

### Generate Coverage Report (Summary Only)
```bash
cargo llvm-cov --lib --ignore-run-fail --summary-only
```

### Generate Coverage Report (Detailed)
```bash
cargo llvm-cov --lib --ignore-run-fail
```

### Generate Coverage Report (HTML)
```bash
cargo llvm-cov --lib --ignore-run-fail --html
# Opens in browser: open target/llvm-cov/html/index.html
```

### Generate Coverage Report (LCOV format)
```bash
cargo llvm-cov --lib --ignore-run-fail --lcov --output-path lcov.info
```

### Extract Coverage Percentage
```bash
cargo llvm-cov --lib --ignore-run-fail --summary-only | grep "TOTAL" | awk '{print $10}'
```

## Full CI Pipeline (Local)

Run all checks that run in CI:

```bash
# 1. Run tests
echo "Running tests..."
cargo test --workspace --all-features --verbose

# 2. Run doc tests
echo "Running doc tests..."
cargo test --workspace --doc --verbose

# 3. Check examples compile
echo "Checking examples..."
cargo check --workspace --examples --all-features

# 4. Run clippy
echo "Running clippy..."
cargo clippy --workspace --all-targets --all-features -- -W clippy::all -D clippy::correctness

# 5. Check formatting
echo "Checking format..."
cargo fmt --all -- --check

# 6. Build debug
echo "Building debug..."
cargo build --workspace --all-features --verbose

# 7. Build release
echo "Building release..."
cargo build --workspace --all-features --release --verbose

# 8. Generate coverage
echo "Generating coverage..."
cargo llvm-cov --lib --ignore-run-fail --summary-only
```

## Useful Test Flags

### Run Tests in Parallel
```bash
cargo test -- --test-threads=4
```

### Run Tests Serially
```bash
cargo test -- --test-threads=1
```

### Show Test Output
```bash
cargo test -- --nocapture
```

### Run Tests with Backtrace
```bash
RUST_BACKTRACE=1 cargo test
```

### Run Tests with Full Backtrace
```bash
RUST_BACKTRACE=full cargo test
```

## Filter Tests

### Run Tests Matching Pattern
```bash
cargo test pattern_name
```

### Run Tests in Specific Module
```bash
cargo test executor::timer::tests
```

### Run Tests Excluding Pattern
```bash
cargo test -- --skip pattern_to_skip
```

## Coverage Targets

Current coverage target: **72%+**

To check if coverage meets target:
```bash
COVERAGE=$(cargo llvm-cov --lib --ignore-run-fail --summary-only | grep "TOTAL" | awk '{print $10}' | sed 's/%//')
if (( $(echo "$COVERAGE >= 72" | bc -l) )); then
    echo "✅ Coverage is $COVERAGE% (meets 72% target)"
else
    echo "❌ Coverage is $COVERAGE% (below 72% target)"
fi
```

## Common Issues

### Tests Failing Due to Missing Invocation Records
Timer and signal tests require invocation records to exist before calling `log_timer()` or `log_signal()`. Always create the invocation first:

```rust
// Create invocation first
let params = InvocationStartParams {
    id: flow_id,
    step: 1,
    class_name: "TestFlow",
    method_name: "test_step()",
    status: InvocationStatus::Pending,
    parameters: &serialize_value(&()).unwrap(),
    retry_policy: None,
};
storage.log_invocation_start(params).await.unwrap();

// Then log timer/signal
storage.log_timer(flow_id, 1, fire_at, None).await.unwrap();
```

### Empty DAG Validation
Empty registries are invalid and will fail validation. Always add at least one step to the registry before calling `validate()` or `execute()`.
