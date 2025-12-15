# Ergon Macros

[![Crates.io](https://img.shields.io/crates/v/ergon_macros.svg)](https://crates.io/crates/ergon_macros)
[![Documentation](https://docs.rs/ergon_macros/badge.svg)](https://docs.rs/ergon_macros)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](../LICENSE)

Procedural macros for the [Ergon](https://github.com/richinex/ergon) durable execution framework.

## Overview

This crate provides the core macro infrastructure that powers Ergon's declarative workflow API. These macros handle the complex code generation needed for durable execution, state management, and DAG coordination.

**Note**: Most users should depend on the `ergon` crate directly, which re-exports these macros. This crate is published separately to follow Rust's procedural macro requirements.

## Macros

### `#[flow]` - Define Workflow Entry Points

Marks an async method as a durable workflow entry point. Flows return `Result<T, ExecutionError>` and can invoke steps and child flows.

```rust
use ergon::prelude::*;

impl MyFlow {
    #[flow]
    async fn process_order(self: Arc<Self>) -> Result<Receipt, ExecutionError> {
        let payment = self.clone().charge_payment().await?;
        let shipping = self.clone().ship_order().await?;
        Ok(Receipt { payment, shipping })
    }

    #[flow(retry = RetryPolicy::STANDARD)]
    async fn process_with_retry(self: Arc<Self>) -> Result<Receipt, ExecutionError> {
        // Flow-level retry policy applies to the entire workflow
        let result = self.clone().external_operation().await?;
        Ok(result)
    }
}
```

**Flow attributes:**
- `retry`: Configure retry behavior for the entire flow (e.g., `RetryPolicy::STANDARD`, `RetryPolicy::AGGRESSIVE`)

**Generated code:**
- State management for flow execution
- Error handling and conversion
- Integration with the executor
- Automatic registration of steps
- Retry policy integration

<!-- TODO: Add flow lifecycle diagram here -->
![Flow Lifecycle](https://via.placeholder.com/800x300/2c3e50/ffffff?text=Flow+Lifecycle+and+State+Management)
*Visual representation of flow execution lifecycle*

### `#[step]` - Define Workflow Steps

Marks an async method as a cacheable workflow step. Steps return `Result<T, E>` where `E` implements standard error traits.

```rust
impl MyFlow {
    #[step]
    async fn charge_payment(self: Arc<Self>) -> Result<PaymentReceipt, String> {
        // Step logic - automatically cached on success
        self.payment_service.charge(self.amount).await
    }

    #[step(depends_on = "charge_payment")]
    async fn send_confirmation(self: Arc<Self>) -> Result<(), String> {
        // Sequential step - runs after charge_payment completes
        self.email_service.send_receipt().await
    }
}
```

**Step attributes:**
- `depends_on`: Specify dependencies for DAG execution (e.g., `depends_on = "step1"` or `depends_on = ["step1", "step2"]`)
- `inputs`: Wire dependency outputs to parameters (e.g., `inputs(data = "step1")`)
- `delay`: Delay before execution (e.g., `delay = 5, unit = "SECONDS"`)

**Generated code:**
- Result caching and memoization
- Error conversion
- Idempotency key generation
- Step registration
- Dependency tracking for DAG execution

<!-- TODO: Add step caching diagram here -->
![Step Caching](https://via.placeholder.com/800x250/34495e/ffffff?text=Step+Result+Caching+and+Replay)
*How steps are cached and replayed during retry*

### `dag!` - Declare DAG Execution

A macro for declaring Directed Acyclic Graph (DAG) execution of parallel steps. The macro analyzes dependencies and generates efficient parallel execution code.

```rust
#[flow]
async fn parallel_processing(self: Arc<Self>) -> Result<Summary, ExecutionError> {
    dag! {
        // Register independent steps (run in parallel)
        let user = self.register_validate_user();
        let inventory = self.register_check_inventory();
        let fraud = self.register_check_fraud();

        // Register dependent step (waits for all three)
        let result = self.register_finalize(user, inventory, fraud)
    }
}
```

**Features:**
- Automatic parallelization of independent steps
- Type-safe dependency tracking
- Compile-time DAG validation
- Efficient scheduling

**Generated code:**
- Step handle creation
- Dependency graph construction
- Parallel execution coordination
- Result collection and propagation

<!-- TODO: Add DAG execution visualization here -->
![DAG Execution](https://via.placeholder.com/800x400/27ae60/ffffff?text=DAG+Parallel+Step+Execution)
*Parallel execution of independent steps in a DAG*

### `#[derive(FlowType)]` - Implement Flow Trait

Automatically implements the `FlowType` trait required for flow execution. This enables runtime type information and serialization.

```rust
#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderFlow {
    order_id: String,
    customer_id: String,
    items: Vec<Item>,
}
```

**Requirements:**
- Type must implement `Clone`
- Type must implement `Serialize` and `Deserialize` (from serde)

**Generated code:**
- `FlowType::type_name()` implementation
- Runtime type identification
- Integration with storage layer

<!-- TODO: Add type hierarchy diagram here -->
![Flow Type System](https://via.placeholder.com/800x250/e74c3c/ffffff?text=Flow+Type+System+and+Serialization)
*Flow type system and serialization architecture*

## Code Generation Details

### Flow Macro Expansion

Input:
```rust
#[flow]
async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
    self.step1().await?;
    Ok("done".to_string())
}
```

Expands to (simplified):
```rust
async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
    // Setup execution context
    // Step invocation with caching
    // Error handling and conversion
    // Return result
}
```

### Step Macro Expansion

Input:
```rust
#[step]
async fn compute(self: Arc<Self>) -> Result<i32, String> {
    Ok(42)
}
```

Expands to (simplified):
```rust
async fn compute(self: Arc<Self>) -> Result<i32, String> {
    // Check cache
    // Execute if not cached
    // Store result
    // Return
}

// Generated registration method
fn register_compute(&self) -> StepHandle<i32, String> {
    // Create step handle with metadata
}
```

### DAG Macro Expansion

Input:
```rust
dag! {
    let h1 = self.register_step1();
    let h2 = self.register_step2(h1)
}
```

Expands to (simplified):
```rust
{
    // Create deferred registry
    // Register steps with dependencies
    // Build dependency graph
    // Execute in parallel
    // Collect results
}
```

<!-- TODO: Add macro expansion diagram here -->
![Macro Expansion](https://via.placeholder.com/800x350/3498db/ffffff?text=Macro+Code+Generation+Process)
*Overview of macro expansion and code generation*

## Macro Attributes

### Flow Attributes

#### `retry`

Specifies the retry policy for the entire flow:

```rust
#[flow(retry = RetryPolicy::NONE)]
async fn no_retry(self: Arc<Self>) -> Result<(), ExecutionError> { }

#[flow(retry = RetryPolicy::STANDARD)]
async fn standard_retry(self: Arc<Self>) -> Result<(), ExecutionError> { }

#[flow(retry = RetryPolicy::AGGRESSIVE)]
async fn aggressive_retry(self: Arc<Self>) -> Result<(), ExecutionError> { }

#[flow(retry = RetryPolicy::custom(5, Duration::from_secs(2)))]
async fn custom_retry(self: Arc<Self>) -> Result<(), ExecutionError> { }
```

### Step Attributes

#### `depends_on`

Declares dependencies for DAG execution:

```rust
#[step]
async fn step1(self: Arc<Self>) -> Result<String, String> { }

#[step(depends_on = "step1")]
async fn step2(self: Arc<Self>, data: String) -> Result<i32, String> { }

#[step(depends_on = "step1, step2")]
async fn step3(self: Arc<Self>, s: String, n: i32) -> Result<bool, String> { }
```

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
ergon = "0.1"  # Re-exports ergon_macros
```

Or directly:

```toml
[dependencies]
ergon_macros = "0.1"
```

Import macros:

```rust
use ergon::prelude::*;  // Recommended

// Or explicitly:
use ergon_macros::{flow, step, dag, FlowType};
```

## Implementation Notes

### Proc Macro Architecture

The macros are implemented using:
- [`syn`](https://docs.rs/syn) for parsing Rust syntax
- [`quote`](https://docs.rs/quote) for code generation
- [`proc-macro2`](https://docs.rs/proc-macro2) for token manipulation

### Error Handling

Macros generate compile-time errors for invalid usage:

```rust
// Error: Flow must return Result<T, ExecutionError>
#[flow]
async fn bad_flow(self: Arc<Self>) -> String { }

// Error: Step must return Result<T, E>
#[step]
async fn bad_step(self: Arc<Self>) -> i32 { }

// Error: Invalid retry attribute on flow
#[flow(retry = "invalid")]
async fn bad_retry(self: Arc<Self>) -> Result<(), ExecutionError> { }
```

### Type Safety

Macros preserve Rust's type safety:
- Flow and step signatures are type-checked
- Return types are validated
- Dependencies are type-checked in DAG macro
- Generic types are fully supported

## Examples

### Complete Workflow Example

```rust
use ergon::prelude::*;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct DataPipeline {
    source: String,
    destination: String,
}

impl DataPipeline {
    #[flow(retry = RetryPolicy::STANDARD)]
    async fn run(self: Arc<Self>) -> Result<Stats, ExecutionError> {
        dag! {
            // Parallel extraction
            let raw_data = self.register_extract();

            // Parallel transformation and validation
            let transformed = self.register_transform(raw_data);
            let validated = self.register_validate(raw_data);

            // Sequential load (depends on both)
            let stats = self.register_load(transformed, validated)
        }
    }

    #[step]
    async fn extract(self: Arc<Self>) -> Result<Vec<u8>, String> {
        // Fetch from source
        Ok(vec![])
    }

    #[step(depends_on = "extract")]
    async fn transform(self: Arc<Self>, data: Vec<u8>) -> Result<Vec<Record>, String> {
        Ok(vec![])
    }

    #[step(depends_on = "extract")]
    async fn validate(self: Arc<Self>, data: Vec<u8>) -> Result<ValidationResult, String> {
        Ok(ValidationResult::Valid)
    }

    #[step(depends_on = "transform, validate")]
    async fn load(
        self: Arc<Self>,
        records: Vec<Record>,
        validation: ValidationResult,
    ) -> Result<Stats, String> {
        Ok(Stats::default())
    }
}
```

## Development

### Building

```bash
cargo build -p ergon_macros
```

### Testing

```bash
cargo test -p ergon_macros
```

### Expanding Macros

Use `cargo expand` to see generated code:

```bash
cargo install cargo-expand
cargo expand --example your_example
```

## Troubleshooting

### Common Issues

**Issue**: Macro hygiene errors

```rust
error: cannot find type `ExecutionContext` in this scope
```

**Solution**: Ensure you have `use ergon::prelude::*;` in scope.

---

**Issue**: Type mismatch in flow return type

```rust
error: expected `Result<_, ExecutionError>`, found `Result<_, String>`
```

**Solution**: Flows must return `ExecutionError`, use `.map_err()` to convert.

---

**Issue**: Step dependency not found

```rust
error: step 'foo' not found for depends_on
```

**Solution**: Ensure the dependency step is defined before the dependent step.

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for development guidelines.

## License

MIT OR Apache-2.0 - see [LICENSE](../LICENSE) for details.

## Resources

- [Parent Crate Documentation](https://docs.rs/ergon)
- [GitHub Repository](https://github.com/richinex/ergon)
- [Macro Implementation](./src/lib.rs)

## Related Crates

- [`ergon`](../ergon) - Main durable execution framework
- [`syn`](https://docs.rs/syn) - Parser for Rust source code
- [`quote`](https://docs.rs/quote) - Quasi-quoting for proc macros
