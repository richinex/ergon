# Hybrid AI Distributed Example

This example shows how to structure a **distributed ergon application** with separate scheduler and worker processes.

## Key Insight

**Ergon provides the abstraction (`Worker::new()`), YOU provide the implementation (your flow types).**

This is not infrastructure that ergon ships - it's the pattern YOU use in YOUR project.

## File Structure

```
hybrid_ai_distributed/
  flows.rs      ← Flow type definitions (shared module)
  scheduler.rs  ← Binary that schedules flows
  worker.rs     ← Binary that executes flows
```

Both `scheduler.rs` and `worker.rs` import from `flows.rs` - **no duplication**.

## How to Apply This to Your Project

In your company's project, you would structure it like:

```
my-company/
  flows/           ← Cargo.toml: [package] name = "my-company-flows"
    src/lib.rs     ← Your ContentFlow, OrderFlow, etc.

  scheduler/       ← Cargo.toml: [dependencies] my-company-flows = { path = "../flows" }
    src/main.rs    ← Schedules flows, delivers signals

  worker/          ← Cargo.toml: [dependencies] my-company-flows = { path = "../flows" }
    src/main.rs    ← Worker::new() + register your flows
    Dockerfile     ← Builds worker container
```

## Running This Example

See [HYBRID_AI_EXAMPLE.md](../../../HYBRID_AI_EXAMPLE.md) for full instructions.

Quick start:
```bash
# 1. Start Postgres + Worker
docker-compose -f docker-compose.hybrid-ai.yml up -d

# 2. Run scheduler
DATABASE_URL=postgres://ergon:ergon@localhost:5432/ergon \
cargo run -p ergon --example hybrid_ai_distributed_scheduler --features postgres
```

## What Ergon Provides

- `Worker::new()` - The worker abstraction
- `Scheduler::new()` - Flow scheduling
- Storage backends (Postgres, SQLite, Redis, Memory)
- `#[flow]` and `#[step]` macros
- Retry, caching, signal coordination

## What YOU Provide

- Your flow types (ContentFlow, OrderFlow, etc.)
- Your worker binary (imports ergon + your flows)
- Your scheduler/application logic

**The worker is user code, not ergon infrastructure.**
