# Ergon - Durable Execution Framework

[![Crates.io](https://img.shields.io/crates/v/ergon.svg)](https://crates.io/crates/ergon)
[![Documentation](https://docs.rs/ergon/badge.svg)](https://docs.rs/ergon)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](../LICENSE)

**Ergon** (ἔργον, Greek for "work" or "deed") is a durable execution framework for Rust, inspired by [Temporal](https://temporal.io/). It provides automatic state persistence, intelligent retry mechanisms, and distributed workflow orchestration.

## Overview

Ergon enables you to write reliable, fault-tolerant workflows that survive process crashes, network failures, and infrastructure outages. Your business logic is expressed as simple async Rust code, while Ergon handles:

- **State persistence** - Automatic checkpointing of execution state
- **Automatic retry** - Smart retry with exponential backoff
- **Step caching** - Deterministic replay using cached results
- **DAG parallelization** - Automatic parallel execution of independent steps
- **External signals** - Human-in-the-loop and external event coordination
- **Durable timers** - Long-running timers that survive crashes
- **Distributed workers** - Scale horizontally with multiple worker processes

<!-- TODO: Add architecture diagram here -->
![Architecture Overview](https://via.placeholder.com/800x400/2c3e50/ffffff?text=Ergon+Architecture+Diagram)
*Architecture diagram showing flow execution, storage, and worker coordination*

## Features

### 🔄 Durable Execution

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

### 📊 DAG-Based Parallelization

```rust
#[flow]
async fn parallel_processing(self: Arc<Self>) -> Result<Summary, ExecutionError> {
    dag! {
        // These run in parallel automatically
        let user_handle = self.register_validate_user();
        let inventory_handle = self.register_check_inventory();
        let fraud_handle = self.register_check_fraud();

        // This waits for all three to complete
        let final_handle = self.register_finalize(
            user_handle,
            inventory_handle,
            fraud_handle
        )
    }
}
```

<!-- TODO: Add DAG visualization here -->
![DAG Execution](https://via.placeholder.com/800x300/34495e/ffffff?text=DAG+Parallel+Execution+Flow)
*Visual representation of parallel step execution in a DAG*

### 🔁 Smart Retry with Custom Errors

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

### 📡 External Signals & Human-in-the-Loop

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

<!-- TODO: Add signal flow diagram here -->
![Signal Flow](https://via.placeholder.com/800x300/27ae60/ffffff?text=External+Signal+Coordination)
*Signal-based workflow coordination with external systems*

### ⏱️ Durable Timers

```rust
#[step]
async fn wait_for_settlement(self: Arc<Self>) -> Result<(), ExecutionError> {
    // Timer survives process crashes
    schedule_timer(Duration::from_secs(3600)).await?;
    Ok(())
}
```

### 👥 Distributed Workers

```rust
// Spawn multiple workers that share the queue
let worker1 = Worker::new("worker-1", storage.clone())
    .with_signals(signal_source.clone())
    .spawn();

let worker2 = Worker::new("worker-2", storage.clone())
    .with_signals(signal_source.clone())
    .spawn();
```

<!-- TODO: Add worker distribution diagram here -->
![Distributed Workers](https://via.placeholder.com/800x300/e74c3c/ffffff?text=Multi-Worker+Distribution)
*Multiple workers processing flows from shared queue*

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
- **`executor`**: Execution engine (worker loop, error handling)

```rust
use ergon::prelude::*;           // Common types and macros
use ergon::core::RetryPolicy;    // Retry configuration
use ergon::executor::Worker;     // Distributed workers
use ergon::storage::ExecutionLog;// Storage abstraction
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
// Start multiple workers
let mut handles = vec![];

for i in 0..4 {
    let worker = Worker::new(format!("worker-{}", i), storage.clone())
        .with_signals(signals.clone())
        .with_timers()
        .spawn();
    handles.push(worker);
}

// Schedule flows
scheduler.schedule(flow1).await?;
scheduler.schedule(flow2).await?;

// Workers automatically distribute the load
```

<!-- TODO: Add distributed execution diagram here -->
![Distributed Execution](https://via.placeholder.com/800x350/3498db/ffffff?text=Distributed+Worker+Execution)
*Load distribution across multiple worker processes*

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

See the [examples directory](./examples/) for complete, runnable examples.

## Performance Considerations

- **Step granularity**: Balance between checkpointing overhead and replay cost
- **Parallel execution**: Use DAG for independent operations
- **Storage choice**: SQLite for single-process, Redis for distributed
- **Worker count**: Scale based on queue depth and step duration
- **Retry policies**: Configure appropriate backoff for your use case

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for development guidelines.

## License

MIT OR Apache-2.0 - see [LICENSE](../LICENSE) for details.

## Resources

- [Documentation](https://docs.rs/ergon)
- [GitHub Repository](https://github.com/richinex/ergon)
- [Examples](./examples/)
- [Changelog](../CHANGELOG.md)

## Acknowledgments

Inspired by:
- [Temporal](https://temporal.io/) - Durable execution platform
- [Parnas](https://en.wikipedia.org/wiki/Information_hiding) - Information hiding principles
- [Dave Cheney](https://dave.cheney.net/) - Practical programming wisdom
