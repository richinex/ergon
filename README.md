# Ergon

**Ergon** (ἔργον, Greek for "work" or "deed") is a durable execution framework for Rust, inspired by [Temporal](https://temporal.io/).

## Features

- **Durable execution**: Automatically persists execution state and resumes from failures
- **Step caching**: Memoizes step results for deterministic replay
- **DAG-based parallelization**: Automatically parallelizes independent steps
- **Retry logic**: Configurable retry with exponential backoff
- **External signals**: Wait for and respond to external events
- **Type-safe**: Full type safety with Rust's type system

## Quick Start

Add ergon to your `Cargo.toml`:

```toml
[dependencies]
ergon = { path = "../ergon/ergon" }
```

Or if published to crates.io:

```toml
[dependencies]
ergon = "0.1"
```

## Example

```rust
use ergon::prelude::*;

#[derive(Clone)]
struct MyFlow {
    name: String,
}

#[flow]
impl MyFlow {
    #[flow]
    async fn run(self: Arc<Self>) -> Result<String, String> {
        let greeting = self.greet().await?;
        let result = self.process(&greeting).await?;
        Ok(result)
    }

    #[step]
    async fn greet(self: Arc<Self>) -> Result<String, String> {
        Ok(format!("Hello, {}!", self.name))
    }

    #[step]
    async fn process(self: Arc<Self>, msg: &str) -> Result<String, String> {
        Ok(msg.to_uppercase())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(SqliteExecutionLog::new("my.db")?);
    let flow = Arc::new(MyFlow { name: "World".to_string() });
    let id = Uuid::new_v4();

    let instance = Ergon::new_flow(flow, id, storage);
    let result = instance.execute(|f| f.run()).await?;

    println!("Result: {}", result);
    Ok(())
}
```

## Architecture

Ergon follows Parnas's information hiding principles, with each module hiding specific design decisions:

- **core**: Foundation types and traits (hides serialization format)
- **storage**: Persistence layer (hides database choice and schema)
- **graph**: DAG structures (hides graph implementation)
- **executor**: Execution engine (hides execution strategy)

## Workspace Structure

```
ergon/
├── Cargo.toml          (workspace root)
├── ergon/              (main library)
│   ├── Cargo.toml
│   └── src/
└── ergon_macros/       (proc-macros)
    ├── Cargo.toml
    └── src/
```

## Building

Build the entire workspace:

```bash
cargo build --workspace
```

Run tests:

```bash
cargo test --workspace
```

Build release version:

```bash
cargo build --release
```

## Versioning

Ergon follows [Semantic Versioning 2.0.0](https://semver.org/).

### What Constitutes a Breaking Change

Breaking changes include:

- Removing or renaming public APIs (functions, types, modules)
- Changing function signatures in incompatible ways
- Changing trait definitions (adding required methods, removing methods)
- Adding fields to public structs without `#[non_exhaustive]`
- Adding variants to public enums without `#[non_exhaustive]`
- Changing the behavior of existing APIs in ways that could break user code
- Increasing MSRV (Minimum Supported Rust Version)

Non-breaking changes include:

- Adding new public APIs
- Adding new optional methods to traits with default implementations
- Adding fields to structs marked with `#[non_exhaustive]`
- Adding variants to enums marked with `#[non_exhaustive]`
- Deprecating APIs (with the `#[deprecated]` attribute)
- Performance improvements
- Bug fixes
- Documentation improvements

### Minimum Supported Rust Version (MSRV)

Ergon supports Rust 1.75.0 and later.

MSRV increases are considered breaking changes and will only occur with minor version bumps (0.x.0) before 1.0.0, and major version bumps (x.0.0) after 1.0.0.

### Feature Flags

Currently, Ergon does not use feature flags. All features are enabled by default.

Future versions may introduce optional features for:

- Different storage backends (PostgreSQL, in-memory only)
- Optional tracing/metrics integrations
- Performance vs binary size trade-offs

## License

MIT OR Apache-2.0
