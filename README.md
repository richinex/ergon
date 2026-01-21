# Ergon - Durable Execution Library

[![CI](https://github.com/richinex/ergon/workflows/CI/badge.svg)](https://github.com/richinex/ergon/actions)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B%20%7C%20edition%202021-orange.svg)](https://www.rust-lang.org)

**Ergon** (ἔργον, Greek for "work" or "deed") is a durable execution library for Rust, inspired by Gunnar Morling's [Persistasaurus](https://www.morling.dev/blog/building-durable-execution-engine-with-sqlite/). and several of Jack Vanlightly's blogs on [Durable Execution] (https://jack-vanlightly.com/blog/2025/11/24/demystifying-determinism-in-durable-execution).

It provides automatic state persistence, intelligent retry mechanisms, and distributed workflow orchestration.

Suffice to say that Ergon is a curiosity project for me because I wanted to learn the internals of many aspects of Rust e,g macros, async, typestate, autoref-specialization, to mention a few.
## Overview

Ergon enables you to write reliable, fault-tolerant workflows that survive process crashes, network failures, and infrastructure outages. Your business logic is expressed as simple async Rust code, while Ergon handles:

- **State persistence** - Automatic checkpointing of execution state
- **Automatic retry** - Smart retry with exponential backoff
- **Step caching** - Deterministic replay using cached results
- **DAG parallelization** - Automatic parallel execution of independent steps
- **Flow versioning** - Type-safe deployment versioning with compile-time checks
- **External signals** - Human-in-the-loop and external event coordination
- **Durable timers** - Long-running timers that survive crashes
- **Distributed workers** - Scale horizontally with multiple worker processes (Backed by Redis and Portgres)

## Features

### Durable Execution

```rust
use ergon::prelude::*;

#[flow]
async fn process_order(self: Arc<Self>) -> Result<Receipt, ExecutionError> {
    // Each step is automatically persisted
    let payment = self.clone().charge_payment().await?;
    let inventory = self.clone().reserve_inventory().await?;
    let shipping = self.clone().ship_order().await?;

    // If the process crashes here, it resumes from the last checkpoint
    Ok(Receipt { payment, inventory, shipping })
}
```


### Smart Retry with Custom Errors

```rust
use ergon::Retryable;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PaymentError {
    NetworkTimeout,      // Retryable
    InsufficientFunds,   // Not retryable
}

impl Retryable for PaymentError {
    fn is_retryable(&self) -> bool {
        matches!(self, PaymentError::NetworkTimeout)
    }
}

#[step]
async fn charge_card(self: Arc<Self>) -> Result<Receipt, PaymentError> {
    // Network timeouts retry automatically
    // Business rejections fail immediately
}
```

### External Signals & Human-in-the-Loop

```rust
#[step]
async fn await_manager_approval(
    self: Arc<Self>,
) -> Result<ApprovalDecision, ExecutionError> {
    // Flow suspends here until external signal arrives
    let decision = await_external_signal::<ApprovalDecision>().await?;
    Ok(decision)
}
```

### Durable Timers

```rust
#[step]
async fn wait_for_settlement(self: Arc<Self>) -> Result<(), ExecutionError> {
    // Timer survives process crashes
    schedule_timer(Duration::from_secs(3600)).await?;
    Ok(())
}
```

### Flow Versioning

```rust
// Configure scheduler with deployment version
let scheduler = Scheduler::new(storage)
    .with_version(env!("CARGO_PKG_VERSION"));  // Type-safe at compile time

// All flows get this version automatically
scheduler.schedule(order_flow).await?;

// Alternative: Read from environment
let scheduler = Scheduler::new(storage).from_env();  // DEPLOY_VERSION=v1.2.3

// Or explicitly unversioned
let scheduler = Scheduler::new(storage).unversioned();
```

### Distributed Workers

```rust
// Spawn multiple workers that share the queue
let worker1 = Worker::new(storage.clone(), "worker-1")
    .with_signals(signal_source.clone())
    .spawn();

let worker2 = Worker::new(storage.clone(), "worker-2")
    .with_signals(signal_source.clone())
    .spawn();
```


## Installation

Add ergon to your `Cargo.toml`:

```toml
[dependencies]
ergon = { version = "0.1", features = ["sqlite"] }
```

### Feature Flags

- **`sqlite`** (default): SQLite-based storage with bundled SQLite
- **`redis`**: Redis-based storage for distributed deployments

```toml
# Use Redis for distributed systems
ergon = { version = "0.1", default-features = false, features = ["redis"] }
```

## Quick Start

```rust
use ergon::prelude::*;
use std::sync::Arc;

// 1. Define your workflow
#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderFlow {
    order_id: String,
    amount: f64,
}

impl OrderFlow {
    // 2. Implement your business logic with #[flow] and #[step]
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        let payment = self.clone().charge_payment().await?;
        let shipping = self.clone().ship_order().await?;
        Ok(format!("Order {} complete", self.order_id))
    }

    #[step]
    async fn charge_payment(self: Arc<Self>) -> Result<String, String> {
        // Payment logic here
        Ok("payment_confirmed".to_string())
    }

    #[step]
    async fn ship_order(self: Arc<Self>) -> Result<String, String> {
        // Shipping logic here
        Ok("shipped".to_string())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 3. Setup storage
    let storage = Arc::new(SqliteExecutionLog::new("orders.db")?);

    // 4. Create and execute the flow
    let flow = OrderFlow {
        order_id: "ORDER-123".to_string(),
        amount: 99.99,
    };

    let executor = Executor::new(Uuid::new_v4(), flow, storage);

    match executor.execute(|f| Box::pin(f.process())).await {
        FlowOutcome::Completed(Ok(result)) => {
            println!("Success: {}", result);
        }
        FlowOutcome::Completed(Err(e)) => {
            eprintln!("Flow failed: {}", e);
        }
        FlowOutcome::Suspended(reason) => {
            println!("Flow suspended: {:?}", reason);
        }
    }

    Ok(())
}
```

## Module Organization

Ergon follows Parnas's information hiding principles:

- **`core`**: Foundation types and traits (serialization, retry policies)
- **`storage`**: Persistence layer (SQLite, Redis, in-memory)
- **`graph`**: DAG structures for parallel execution
- **`executor`**: Execution engine (scheduler, workers, error handling)

```rust
use ergon::prelude::*;            // Common types and macros
use ergon::core::RetryPolicy;     // Retry configuration
use ergon::executor::Scheduler;   // Flow scheduling with versioning
use ergon::executor::Worker;      // Distributed workers
use ergon::storage::ExecutionLog; // Storage abstraction
```

## Usage Patterns

### Pattern 1: Sequential Workflow

```rust
#[flow]
async fn sequential(self: Arc<Self>) -> Result<String, ExecutionError> {
    let a = self.clone().step1().await?;
    let b = self.clone().step2(&a).await?;
    let c = self.clone().step3(&b).await?;
    Ok(c)
}
```

### Pattern 2: Parallel DAG

```rust
#[flow]
async fn parallel(self: Arc<Self>) -> Result<Summary, ExecutionError> {
    dag! {
        let h1 = self.register_fetch_user();
        let h2 = self.register_fetch_orders();
        let summary = self.register_merge(h1, h2)
    }
}
```

### Pattern 3: Child Flow Invocation

```rust
#[step]
async fn process_items(self: Arc<Self>) -> Result<Vec<String>, String> {
    let mut results = vec![];

    for item in &self.items {
        let child = ItemFlow::new(item);
        let result = self.invoke(child).await?;
        results.push(result);
    }

    Ok(results)
}
```

### Pattern 4: Distributed Workers

```rust
// Create scheduler (configure once at startup)
let scheduler = Scheduler::new(storage.clone())
    .with_version(env!("CARGO_PKG_VERSION"));  // or .unversioned()

// Start multiple workers
let mut handles = vec![];

for i in 0..4 {
    let worker = Worker::new(storage.clone(), format!("worker-{}", i))
        .with_signals(signals.clone())
        .with_timers()
        .spawn();
    handles.push(worker);
}

// Schedule flows (version applied automatically)
scheduler.schedule(flow1).await?;
scheduler.schedule(flow2).await?;

// Workers automatically distribute the load
```

## Error Handling

Ergon provides fine-grained error control:

```rust
// Define custom errors with retry behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
enum BusinessError {
    Retryable(String),
    Permanent(String),
}

impl Retryable for BusinessError {
    fn is_retryable(&self) -> bool {
        matches!(self, BusinessError::Retryable(_))
    }
}

// Configure retry policies
#[step(retry_policy = RetryPolicy::AGGRESSIVE)]
async fn unstable_api(self: Arc<Self>) -> Result<Data, BusinessError> {
    // Retries with exponential backoff
}
```

## Storage Backends

### SQLite (Default)

```rust
let storage = Arc::new(SqliteExecutionLog::new("flows.db")?);
```

### Redis (Distributed)

```rust
let storage = Arc::new(RedisExecutionLog::new("redis://localhost:6379").await?);
```

### In-Memory (Testing)

```rust
let storage = Arc::new(InMemoryExecutionLog::new());
```

## Examples

The repository includes comprehensive examples:

- **Basic flows**: Simple sequential and parallel workflows
- **DAG execution**: Complex dependency graphs
- **Child flows**: Parent-child flow coordination
- **Custom errors**: Type-safe error handling with retry control
- **External signals**: Human-in-the-loop approval workflows
- **Durable timers**: Long-running timer coordination
- **Distributed workers**: Multi-worker deployments
- **Crash recovery**: Resilience testing

## Performance Considerations

- **Step granularity**: Balance between checkpointing overhead and replay cost
- **Parallel execution**: Use DAG for independent operations
- **Storage choice**: SQLite for single-process, Redis for distributed
- **Retry policies**: Configure appropriate backoff for your use case


## Acknowledgments

Inspired by:
- [Persistasaurus](https://www.morling.dev/blog/building-durable-execution-engine-with-sqlite/).
- [Temporal](https://temporal.io/) - Durable execution platform
- [Jack vanlightly] (https://jack-vanlightly.com/blog/2025/11/24/demystifying-determinism-in-durable-execution).
- [Dave Cheney](https://dave.cheney.net/) - Practical programming wisdom
- [Autoref Specialization] (https://github.com/dtolnay/case-studies/tree/master/autoref-specialization)
