# Procedural Macro Testing with Trybuild

This document explains how to test procedural macros in the `ergon_macros` crate using the `trybuild` test harness.

## Table of Contents

- [Why Test Macros?](#why-test-macros)
- [What is Trybuild?](#what-is-trybuild)
- [Setup](#setup)
- [Test Structure](#test-structure)
- [Writing Tests](#writing-tests)
- [Running Tests](#running-tests)
- [Understanding Test Output](#understanding-test-output)
- [Best Practices](#best-practices)
- [Common Patterns](#common-patterns)
- [Troubleshooting](#troubleshooting)

## Why Test Macros?

Traditional code coverage metrics are insufficient for procedural macros because:

1. **User Experience**: Macros generate code that users interact with. The quality of error messages is critical.
2. **Compiler Integration**: Macro errors appear at compile time, not runtime.
3. **Code Generation**: You need to verify that the generated code is correct and compiles.
4. **Error Messages**: Poor error messages frustrate users. You want to ensure errors point to the right location with helpful messages.

## What is Trybuild?

**Trybuild** is a test harness specifically designed for testing Rust compiler diagnostics. It's perfect for procedural macros because it:

- Compiles test cases and captures compiler output
- Compares actual error messages against expected ones
- Tests both successful compilation (pass tests) and compilation failures (compile-fail tests)
- Provides clear diffs when expectations don't match

## Setup

### 1. Add Dependencies

In `ergon_macros/Cargo.toml`:

```toml
[dev-dependencies]
trybuild = "1.0"
ergon = { path = "../ergon" }
tracing = "0.1"
serde = { version = "1.0", features = ["derive"] }
seahash = "4.1"
```

**Why these dependencies?**
- `trybuild`: The test harness
- `ergon`: The main crate (needed to use macros in tests)
- `tracing`, `serde`, `seahash`: Required by macro-generated code

### 2. Create Test Structure

```bash
ergon_macros/
├── tests/
│   ├── ui.rs              # Test harness entry point
│   └── ui/                # Test cases
│       ├── flow_valid.rs
│       ├── step_valid.rs
│       ├── flow_not_async.rs
│       ├── step_not_async.rs
│       ├── flow_not_async.stderr    # Expected error output
│       └── step_not_async.stderr
```

### 3. Create Test Harness

`ergon_macros/tests/ui.rs`:

```rust
#[test]
fn ui() {
    let t = trybuild::TestCases::new();

    // Pass tests - valid macro usage
    t.pass("tests/ui/flow_valid.rs");
    t.pass("tests/ui/step_valid.rs");

    // Compile-fail tests - invalid macro usage
    t.compile_fail("tests/ui/flow_not_async.rs");
    t.compile_fail("tests/ui/step_not_async.rs");
}
```

## Test Structure

### Pass Tests (Should Compile Successfully)

**Purpose**: Verify that valid macro usage compiles and runs correctly.

**Example**: `tests/ui/flow_valid.rs`

```rust
use ergon::prelude::*;
use ergon::executor::{ExecutionError, Retryable};
use ergon::core::RetryPolicy;

#[derive(FlowType, Serialize, Deserialize)]
struct TestFlow {
    value: i32,
}

impl TestFlow {
    #[flow]
    async fn simple_flow(self: Arc<Self>) -> Result<i32, ExecutionError> {
        Ok(self.value)
    }

    #[flow(retry = RetryPolicy::STANDARD)]
    async fn flow_with_retry(self: Arc<Self>) -> Result<String, ExecutionError> {
        Ok(format!("value: {}", self.value))
    }
}

fn main() {
    println!("Flow macros compile successfully");
}
```

**Requirements for Pass Tests**:
- Must compile without errors
- Must have a `main()` function
- `main()` must not panic when executed

### Compile-Fail Tests (Should Fail to Compile)

**Purpose**: Verify that invalid macro usage produces the correct error messages.

**Example**: `tests/ui/flow_not_async.rs`

```rust
use ergon::prelude::*;
use std::sync::Arc;

#[derive(FlowType)]
struct TestFlow {
    value: i32,
}

impl TestFlow {
    #[flow]
    fn not_async_flow(self: Arc<Self>) -> Result<i32, ExecutionError> {
        Ok(self.value)
    }
}

fn main() {}
```

**Expected Error**: `tests/ui/flow_not_async.stderr`

```
error: #[flow] can only be applied to async functions
  --> tests/ui/flow_not_async.rs:11:5
   |
11 |     fn not_async_flow(self: Arc<Self>) -> Result<i32, ExecutionError> {
   |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

warning: unused import: `std::sync::Arc`
 --> tests/ui/flow_not_async.rs:2:5
  |
2 | use std::sync::Arc;
  |     ^^^^^^^^^^^^^^
  |
  = note: `#[warn(unused_imports)]` on by default
```

## Writing Tests

### 1. Choose What to Test

Test cases should cover:

**Error Detection**:
- Invalid syntax (non-async functions, wrong receivers)
- Missing required attributes
- Invalid attribute arguments
- Type mismatches

**Code Generation**:
- Basic usage compiles
- Advanced features work
- Edge cases are handled
- Integration with the framework

**Error Messages**:
- Errors point to the correct location
- Messages are clear and actionable
- Suggestions are helpful

### 2. Write the Test Case

Create a `.rs` file in `tests/ui/` that demonstrates the scenario.

**Good test case**:
```rust
// tests/ui/step_invalid_receiver.rs
use ergon::prelude::*;

#[derive(FlowType, Serialize, Deserialize)]
struct TestFlow {
    value: i32,
}

impl TestFlow {
    #[step]
    async fn bad_receiver(&self) -> Result<i32, ExecutionError> {
        Ok(self.value)
    }
}

fn main() {}
```

### 3. Generate Expected Output

#### Method 1: Automatic Generation

1. Add the test to `tests/ui.rs`:
   ```rust
   t.compile_fail("tests/ui/step_invalid_receiver.rs");
   ```

2. Run the test:
   ```bash
   cargo test ui
   ```

3. Check the `wip/` directory for generated `.stderr` files:
   ```bash
   ls wip/
   ```

4. Move the file to `tests/ui/`:
   ```bash
   mv wip/step_invalid_receiver.stderr tests/ui/
   ```

#### Method 2: TRYBUILD=overwrite

Run with the overwrite flag:
```bash
TRYBUILD=overwrite cargo test ui
```

This writes `.stderr` files directly to `tests/ui/` without using `wip/`.

**⚠️ Warning**: Always review the generated `.stderr` files to ensure they match your expectations!

### 4. Review and Refine

Check the generated error message:
- Is it pointing to the right location?
- Is the message clear and helpful?
- Are there unnecessary warnings you should filter?

If the error message is poor, fix your macro's error handling and regenerate.

## Running Tests

### Run All UI Tests
```bash
cargo test ui
```

### Run Specific Test
```bash
cargo test ui -- flow_valid
```

### Run with Verbose Output
```bash
cargo test ui -- --nocapture
```

### Overwrite All Expected Outputs
```bash
TRYBUILD=overwrite cargo test ui
```

### Run Tests in CI
Tests automatically run in GitHub Actions as part of the standard test suite.

## Understanding Test Output

### Successful Test

```
test tests/ui/flow_valid.rs [should pass] ... ok
test tests/ui/flow_not_async.rs [should fail to compile] ... ok
```

### Failed Pass Test

When a test that should compile doesn't:

```
test tests/ui/flow_valid.rs [should pass] ... error
┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈
error[E0433]: failed to resolve: use of undeclared type `ExecutionError`
  --> tests/ui/flow_valid.rs:11:58
   |
11 |     async fn simple_flow(self: Arc<Self>) -> Result<i32, ExecutionError> {
   |                                                          ^^^^^^^^^^^^^^
```

**Action**: Fix the test case or the macro code.

### Failed Compile-Fail Test

When actual errors don't match expected:

```
test tests/ui/flow_not_async.rs [should fail to compile] ... mismatch
┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈

Expected (tests/ui/flow_not_async.stderr):
  error: #[flow] can only be applied to async functions

Actual:
  error: expected `async fn`
```

**Action**: Either fix the macro to produce the expected error, or update the `.stderr` file if the new error is better.

### Test That Should Fail But Compiles

```
test tests/ui/flow_not_async.rs [should fail to compile] ... ok
┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈
note: test did not fail to compile as expected
```

**Action**: This means your macro isn't detecting the error it should. Fix the macro validation.

## Best Practices

### 1. Test User-Facing Errors

Don't test every possible edge case. Focus on errors users are likely to encounter:

**Good**:
```rust
// tests/ui/flow_missing_async.rs - Common mistake
#[flow]
fn sync_flow() -> Result<i32> { Ok(42) }
```

**Not Worth Testing**:
```rust
// Extremely unlikely scenario that compiler catches anyway
#[flow(retry = "not a retry policy type")]
async fn weird_edge_case() {}
```

### 2. Keep Tests Simple

Each test should demonstrate one issue:

**Good**:
```rust
// tests/ui/step_wrong_receiver.rs - Tests one thing
impl TestFlow {
    #[step]
    async fn step(&self) -> Result<i32> { Ok(42) }
}
```

**Bad**:
```rust
// tests/ui/step_multiple_issues.rs - Too many issues
impl TestFlow {
    #[step]
    fn step(&self) -> i32 { 42 }  // Missing async, wrong return type, wrong receiver
}
```

### 3. Use Descriptive File Names

File names should clearly indicate what's being tested:

**Good**:
- `flow_not_async.rs`
- `step_missing_arc_receiver.rs`
- `dag_duplicate_step_names.rs`

**Bad**:
- `test1.rs`
- `broken.rs`
- `error_case.rs`

### 4. Document Complex Cases

Add comments explaining why a test exists:

```rust
// tests/ui/step_serialization_requirement.rs
//
// Steps require self to be serializable because we cache parameters.
// This test verifies that we produce a clear error when the type
// doesn't implement Serialize.

use ergon::prelude::*;

struct NotSerializable {
    // Complex type that can't be serialized
    callback: Box<dyn Fn()>,
}

impl NotSerializable {
    #[step]
    async fn will_fail(self: Arc<Self>) -> Result<()> {
        Ok(())
    }
}

fn main() {}
```

### 5. Review Error Messages

Always review generated `.stderr` files. Ask:

- Would this error help a user fix their code?
- Does it point to the right location?
- Is the message clear without being verbose?
- Are there helpful suggestions?

### 6. Version Control `.stderr` Files

**Always commit `.stderr` files** to version control. They are part of your tests.

### 7. Update Tests When Changing Macros

When you improve error messages:

1. Update the macro
2. Run `TRYBUILD=overwrite cargo test ui`
3. Review all changed `.stderr` files with `git diff`
4. Commit the improvements

## Common Patterns

### Testing Attribute Arguments

```rust
// tests/ui/flow_invalid_retry.rs
#[flow(retry = "not_a_policy")]
async fn bad_retry() -> Result<()> { Ok(()) }
```

### Testing Receiver Types

```rust
// tests/ui/step_borrowed_receiver.rs
#[step]
async fn borrowed_self(&self) -> Result<i32> { Ok(42) }

// tests/ui/step_mutable_receiver.rs
#[step]
async fn mutable_self(&mut self) -> Result<i32> { Ok(42) }

// tests/ui/step_owned_receiver.rs
#[step]
async fn owned_self(self) -> Result<i32> { Ok(42) }
```

### Testing Return Types

```rust
// tests/ui/flow_missing_result.rs
#[flow]
async fn no_result() -> i32 { 42 }

// tests/ui/flow_wrong_error_type.rs
#[flow]
async fn wrong_error() -> Result<i32, String> { Ok(42) }
```

### Testing Complex Scenarios

```rust
// tests/ui/dag_workflow_integration.rs
use ergon::prelude::*;

#[derive(FlowType, Serialize, Deserialize)]
struct ComplexFlow {
    config: Config,
}

impl ComplexFlow {
    #[flow]
    async fn main_flow(self: Arc<Self>) -> Result<Output, ExecutionError> {
        let data = self.fetch_data().await?;
        let processed = self.process_data(&data).await?;
        self.save_result(&processed).await
    }

    #[step]
    async fn fetch_data(self: Arc<Self>) -> Result<Data, ExecutionError> {
        // Implementation
    }

    #[step]
    async fn process_data(self: Arc<Self>, data: &Data) -> Result<Processed, ExecutionError> {
        // Implementation
    }

    #[step]
    async fn save_result(self: Arc<Self>, result: &Processed) -> Result<Output, ExecutionError> {
        // Implementation
    }
}

fn main() {
    println!("Complex workflow compiles");
}
```

## Troubleshooting

### Issue: Test compiles but shouldn't

**Symptom**:
```
test tests/ui/should_fail.rs [should fail to compile] ... ok
note: test did not fail to compile as expected
```

**Solution**: Your macro isn't validating the input. Add validation logic to detect the error.

### Issue: Error message points to wrong location

**Symptom**: Error appears on `#[flow]` instead of the actual problem location.

**Solution**: Use proper span handling in your macro:
```rust
// Bad
return Err(syn::Error::new(Span::call_site(), "error"));

// Good
return Err(syn::Error::new(input.span(), "error"));
```

### Issue: Missing dependencies in test

**Symptom**:
```
error[E0433]: failed to resolve: use of undeclared crate `serde`
```

**Solution**: Add the dependency to `[dev-dependencies]` in `ergon_macros/Cargo.toml`.

### Issue: Tests fail on Windows but pass on Linux

**Symptom**: Path or line-ending related failures.

**Solution**:
- Use `$WORKSPACE` instead of absolute paths in `.stderr` files
- Trybuild normalizes paths automatically
- Ensure test logic doesn't depend on platform-specific paths

### Issue: stderr file has platform-specific paths

**Symptom**:
```
Expected:
  --> tests/ui/flow.rs:10:5

Actual:
  --> /home/user/project/tests/ui/flow.rs:10:5
```

**Solution**: Trybuild automatically normalizes paths. If you're seeing this, you might be using an old version. Update trybuild:
```toml
[dev-dependencies]
trybuild = "1.0"
```

### Issue: Too many warnings in stderr

**Symptom**: `.stderr` files are cluttered with warnings that aren't relevant to the test.

**Solution**: Add `#![allow(warnings)]` to tests where warnings aren't important:
```rust
#![allow(warnings)]

use ergon::prelude::*;
// Test focuses on errors, not warnings
```

## Advanced Usage

### Testing Multiple Error Messages

```rust
// tests/ui/multiple_errors.rs
use ergon::prelude::*;

impl TestFlow {
    #[flow]
    fn first_error() -> Result<i32> { Ok(42) }  // Not async

    #[step]
    fn second_error() -> Result<i32> { Ok(42) }  // Not async
}

fn main() {}
```

The `.stderr` file will contain both errors.

### Testing Warnings

Trybuild captures warnings too:
```rust
// tests/ui/deprecation_warning.rs
use ergon::prelude::*;

#[deprecated]
#[flow]
async fn old_flow() -> Result<()> { Ok(()) }

fn main() {}
```

### Testing Macro Expansion

For debugging, you can inspect macro expansion:
```bash
cargo expand --test ui
```

Or for a specific test:
```bash
cargo expand --test ui --bin flow_valid
```

## Integration with CI

Trybuild tests run automatically in CI as part of `cargo test`:

```yaml
# .github/workflows/ci.yml
- name: Run tests
  run: cargo test --workspace --all-features --verbose
```

No special configuration needed!

## Summary

- **Trybuild** provides UI testing for macros
- **Pass tests** verify valid usage compiles
- **Compile-fail tests** verify error messages are correct
- **.stderr files** are the expected output (commit them!)
- **TRYBUILD=overwrite** regenerates expected output
- Focus on **user-facing errors** and **helpful messages**

## References

- [Trybuild Documentation](https://docs.rs/trybuild)
- [Trybuild GitHub](https://github.com/dtolnay/trybuild)
- [Procedural Macros Workshop](https://github.com/dtolnay/proc-macro-workshop)
- [The Little Book of Rust Macros](https://veykril.github.io/tlborm/)
