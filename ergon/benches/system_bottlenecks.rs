use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ergon::executor::FlowOutcome;
use ergon::prelude::*;
use ergon::storage::TaskStatus;
use ergon::{Executor, Retryable};
use std::sync::Arc;

#[cfg(feature = "postgres")]
use ergon::PostgresExecutionLog;

#[cfg(feature = "redis")]
use ergon::RedisExecutionLog;

// =============================================================================
// Test Error Type
// =============================================================================

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BenchError {
    message: String,
}

impl std::fmt::Display for BenchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for BenchError {}

impl Retryable for BenchError {
    fn is_retryable(&self) -> bool {
        false // Non-retryable for benchmarking
    }
}

// =============================================================================
// Test Workflows
// =============================================================================

#[derive(Clone, serde::Serialize, serde::Deserialize, FlowType)]
struct SimpleFlow;

impl SimpleFlow {
    #[flow]
    async fn run(self: Arc<Self>) -> Result<String, BenchError> {
        let r1 = self.clone().step1().await?;
        let r2 = self.clone().step2(r1).await?;
        let r3 = self.step3(r2).await?;
        Ok(r3)
    }

    #[step]
    async fn step1(self: Arc<Self>) -> Result<String, BenchError> {
        Ok("step1".to_string())
    }

    #[step]
    async fn step2(self: Arc<Self>, input: String) -> Result<String, BenchError> {
        Ok(format!("{}-step2", input))
    }

    #[step]
    async fn step3(self: Arc<Self>, input: String) -> Result<String, BenchError> {
        Ok(format!("{}-step3", input))
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, FlowType)]
struct ComplexFlow;

impl ComplexFlow {
    #[flow]
    async fn run(self: Arc<Self>) -> Result<String, BenchError> {
        let mut results = Vec::new();
        for i in 0..10 {
            let r = self.clone().process_item(i).await?;
            results.push(r);
        }
        Ok(results.join(","))
    }

    #[step]
    async fn process_item(self: Arc<Self>, item: i32) -> Result<String, BenchError> {
        Ok(format!("item-{}", item))
    }
}

// =============================================================================
// Storage Benchmarks
// =============================================================================

fn bench_storage_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("storage");

    // InMemory: log_invocation_start
    group.bench_function("inmemory_log_invocation_start", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(InMemoryExecutionLog::new());
                let flow_id = uuid::Uuid::new_v4();
                let params = ergon::core::serialize_value(&()).unwrap();

                let result = storage
                    .log_invocation_start(ergon::storage::InvocationStartParams {
                        id: flow_id,
                        step: 0,
                        class_name: "TestFlow",
                        method_name: "run",
                        status: ergon::InvocationStatus::Pending,
                        parameters: &params,
                        retry_policy: None,
                    })
                    .await;

                black_box(result)
            })
        })
    });

    // SQLite: log_invocation_start
    group.bench_function("sqlite_log_invocation_start", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(SqliteExecutionLog::new(":memory:").await.unwrap());
                let flow_id = uuid::Uuid::new_v4();
                let params = ergon::core::serialize_value(&()).unwrap();

                let result = storage
                    .log_invocation_start(ergon::storage::InvocationStartParams {
                        id: flow_id,
                        step: 0,
                        class_name: "TestFlow",
                        method_name: "run",
                        status: ergon::InvocationStatus::Pending,
                        parameters: &params,
                        retry_policy: None,
                    })
                    .await;

                black_box(result)
            })
        })
    });

    // InMemory: get_invocations_for_flow
    group.bench_function("inmemory_get_invocations", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(InMemoryExecutionLog::new());
                let flow_id = uuid::Uuid::new_v4();
                let params = ergon::core::serialize_value(&()).unwrap();

                // Add 5 invocations
                for step in 0..5 {
                    storage
                        .log_invocation_start(ergon::storage::InvocationStartParams {
                            id: flow_id,
                            step,
                            class_name: "TestFlow",
                            method_name: "run",
                            status: ergon::InvocationStatus::Complete,
                            parameters: &params,
                            retry_policy: None,
                        })
                        .await
                        .unwrap();
                }

                // Benchmark the query
                let result = storage.get_invocations_for_flow(flow_id).await;
                black_box(result)
            })
        })
    });

    // SQLite: get_invocations_for_flow
    group.bench_function("sqlite_get_invocations", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(SqliteExecutionLog::new(":memory:").await.unwrap());
                let flow_id = uuid::Uuid::new_v4();
                let params = ergon::core::serialize_value(&()).unwrap();

                // Add 5 invocations
                for step in 0..5 {
                    storage
                        .log_invocation_start(ergon::storage::InvocationStartParams {
                            id: flow_id,
                            step,
                            class_name: "TestFlow",
                            method_name: "run",
                            status: ergon::InvocationStatus::Complete,
                            parameters: &params,
                            retry_policy: None,
                        })
                        .await
                        .unwrap();
                }

                // Benchmark the query
                let result = storage.get_invocations_for_flow(flow_id).await;
                black_box(result)
            })
        })
    });

    #[cfg(feature = "postgres")]
    {
        // Create connection pool ONCE (realistic production usage)
        let postgres_storage = Arc::new(rt.block_on(async {
            PostgresExecutionLog::new("postgres://postgres:postgres@localhost:5432/ergon")
                .await
                .unwrap()
        }));

        // Postgres: log_invocation_start
        group.bench_function("postgres_log_invocation_start", |b| {
            let storage = postgres_storage.clone();
            b.iter(|| {
                rt.block_on(async {
                    let flow_id = uuid::Uuid::new_v4();
                    let params = ergon::core::serialize_value(&()).unwrap();

                    let result = storage
                        .log_invocation_start(ergon::storage::InvocationStartParams {
                            id: flow_id,
                            step: 0,
                            class_name: "TestFlow",
                            method_name: "run",
                            status: ergon::InvocationStatus::Pending,
                            parameters: &params,
                            retry_policy: None,
                        })
                        .await;

                    black_box(result)
                })
            })
        });

        // Postgres: get_invocations_for_flow
        group.bench_function("postgres_get_invocations", |b| {
            let storage = postgres_storage.clone();
            b.iter(|| {
                rt.block_on(async {
                    let flow_id = uuid::Uuid::new_v4();
                    let params = ergon::core::serialize_value(&()).unwrap();

                    // Add 5 invocations
                    for step in 0..5 {
                        storage
                            .log_invocation_start(ergon::storage::InvocationStartParams {
                                id: flow_id,
                                step,
                                class_name: "TestFlow",
                                method_name: "run",
                                status: ergon::InvocationStatus::Complete,
                                parameters: &params,
                                retry_policy: None,
                            })
                            .await
                            .unwrap();
                    }

                    // Benchmark the query
                    let result = storage.get_invocations_for_flow(flow_id).await;
                    black_box(result)
                })
            })
        });
    }

    #[cfg(feature = "redis")]
    {
        // Create connection pool ONCE (realistic production usage)
        let redis_storage = Arc::new(rt.block_on(async {
            RedisExecutionLog::new("redis://127.0.0.1:6379")
                .await
                .unwrap()
        }));

        // Redis: log_invocation_start
        group.bench_function("redis_log_invocation_start", |b| {
            let storage = redis_storage.clone();
            b.iter(|| {
                rt.block_on(async {
                    let flow_id = uuid::Uuid::new_v4();
                    let params = ergon::core::serialize_value(&()).unwrap();

                    let result = storage
                        .log_invocation_start(ergon::storage::InvocationStartParams {
                            id: flow_id,
                            step: 0,
                            class_name: "TestFlow",
                            method_name: "run",
                            status: ergon::InvocationStatus::Pending,
                            parameters: &params,
                            retry_policy: None,
                        })
                        .await;

                    black_box(result)
                })
            })
        });

        // Redis: get_invocations_for_flow
        group.bench_function("redis_get_invocations", |b| {
            let storage = redis_storage.clone();
            b.iter(|| {
                rt.block_on(async {
                    let flow_id = uuid::Uuid::new_v4();
                    let params = ergon::core::serialize_value(&()).unwrap();

                    // Add 5 invocations
                    for step in 0..5 {
                        storage
                            .log_invocation_start(ergon::storage::InvocationStartParams {
                                id: flow_id,
                                step,
                                class_name: "TestFlow",
                                method_name: "run",
                                status: ergon::InvocationStatus::Complete,
                                parameters: &params,
                                retry_policy: None,
                            })
                            .await
                            .unwrap();
                    }

                    // Benchmark the query
                    let result = storage.get_invocations_for_flow(flow_id).await;
                    black_box(result)
                })
            })
        });
    }

    group.finish();
}

// =============================================================================
// Flow Execution Benchmarks
// =============================================================================

fn bench_flow_execution(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("flow_execution");

    // Simple flow (3 steps) - InMemory
    group.bench_function("simple_flow_inmemory", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(InMemoryExecutionLog::new());
                let flow = Arc::new(SimpleFlow);
                let flow_id = uuid::Uuid::new_v4();

                let executor = Executor::new(flow_id, flow.clone(), storage);
                let result = executor.execute(|f| Box::pin(f.clone().run())).await;

                black_box(result)
            })
        })
    });

    // Simple flow (3 steps) - SQLite
    group.bench_function("simple_flow_sqlite", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(SqliteExecutionLog::new(":memory:").await.unwrap());
                let flow = Arc::new(SimpleFlow);
                let flow_id = uuid::Uuid::new_v4();

                let executor = Executor::new(flow_id, flow.clone(), storage);
                let result = executor.execute(|f| Box::pin(f.clone().run())).await;

                black_box(result)
            })
        })
    });

    // Complex flow (10 steps) - InMemory
    group.bench_function("complex_flow_inmemory", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(InMemoryExecutionLog::new());
                let flow = Arc::new(ComplexFlow);
                let flow_id = uuid::Uuid::new_v4();

                let executor = Executor::new(flow_id, flow.clone(), storage);
                let result = executor.execute(|f| Box::pin(f.clone().run())).await;

                black_box(result)
            })
        })
    });

    // Complex flow (10 steps) - SQLite
    group.bench_function("complex_flow_sqlite", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(SqliteExecutionLog::new(":memory:").await.unwrap());
                let flow = Arc::new(ComplexFlow);
                let flow_id = uuid::Uuid::new_v4();

                let executor = Executor::new(flow_id, flow.clone(), storage);
                let result = executor.execute(|f| Box::pin(f.clone().run())).await;

                black_box(result)
            })
        })
    });

    // Cached replay - InMemory
    group.bench_function("cached_replay_inmemory", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(InMemoryExecutionLog::new());
                let flow = Arc::new(SimpleFlow);
                let flow_id = uuid::Uuid::new_v4();

                // First run to populate cache
                let executor = Executor::new(flow_id, flow.clone(), storage.clone());
                let _ = executor.execute(|f| Box::pin(f.clone().run())).await;

                // Second run (cached) - benchmark this
                let executor = Executor::new(flow_id, flow.clone(), storage);
                let result = executor.execute(|f| Box::pin(f.clone().run())).await;

                black_box(result)
            })
        })
    });

    group.finish();
}

// =============================================================================
// Worker Operations Benchmarks
// =============================================================================

fn bench_worker_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("worker");

    // Scheduler enqueue - InMemory
    group.bench_function("scheduler_enqueue_inmemory", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(InMemoryExecutionLog::new());
                let scheduler = Scheduler::new(storage).unversioned();
                let flow = SimpleFlow;

                let result = scheduler.schedule(flow).await;
                black_box(result)
            })
        })
    });

    // Scheduler enqueue - SQLite
    group.bench_function("scheduler_enqueue_sqlite", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(SqliteExecutionLog::new(":memory:").await.unwrap());
                let scheduler = Scheduler::new(storage).unversioned();
                let flow = SimpleFlow;

                let result = scheduler.schedule(flow).await;
                black_box(result)
            })
        })
    });

    // Worker dequeue - InMemory
    group.bench_function("worker_dequeue_inmemory", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(InMemoryExecutionLog::new());
                let scheduler = Scheduler::new(storage.clone()).unversioned();
                let flow = SimpleFlow;

                // Enqueue a flow
                scheduler.schedule(flow).await.unwrap();

                // Benchmark dequeue
                let result = storage.dequeue_flow("bench-worker").await;
                black_box(result)
            })
        })
    });

    // Worker dequeue - SQLite
    group.bench_function("worker_dequeue_sqlite", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(SqliteExecutionLog::new(":memory:").await.unwrap());
                let scheduler = Scheduler::new(storage.clone()).unversioned();
                let flow = SimpleFlow;

                // Enqueue a flow
                scheduler.schedule(flow).await.unwrap();

                // Benchmark dequeue
                let result = storage.dequeue_flow("bench-worker").await;
                black_box(result)
            })
        })
    });

    group.finish();
}

// =============================================================================
// End-to-End Benchmarks
// =============================================================================

fn bench_end_to_end(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("end_to_end");

    // Full workflow: Schedule + Execute + Complete (InMemory)
    group.bench_function("full_workflow_inmemory", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(InMemoryExecutionLog::new());

                // Schedule
                let scheduler = Scheduler::new(storage.clone()).unversioned();
                let _ = scheduler.schedule(SimpleFlow).await.unwrap();

                // Dequeue
                let scheduled_flow = storage.dequeue_flow("bench-worker").await.unwrap().unwrap();

                // Execute
                let flow = Arc::new(SimpleFlow);
                let executor = Executor::new(scheduled_flow.flow_id, flow.clone(), storage.clone());
                let result = executor.execute(|f| Box::pin(f.clone().run())).await;

                // Complete
                if matches!(result, FlowOutcome::Completed(_)) {
                    let _ = storage
                        .complete_flow(scheduled_flow.flow_id, TaskStatus::Complete, None)
                        .await;
                }

                black_box(result)
            })
        })
    });

    // Full workflow: Schedule + Execute + Complete (SQLite)
    group.bench_function("full_workflow_sqlite", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(SqliteExecutionLog::new(":memory:").await.unwrap());

                // Schedule
                let scheduler = Scheduler::new(storage.clone()).unversioned();
                let _ = scheduler.schedule(SimpleFlow).await.unwrap();

                // Dequeue
                let scheduled_flow = storage.dequeue_flow("bench-worker").await.unwrap().unwrap();

                // Execute
                let flow = Arc::new(SimpleFlow);
                let executor = Executor::new(scheduled_flow.flow_id, flow.clone(), storage.clone());
                let result = executor.execute(|f| Box::pin(f.clone().run())).await;

                // Complete
                if matches!(result, FlowOutcome::Completed(_)) {
                    let _ = storage
                        .complete_flow(scheduled_flow.flow_id, TaskStatus::Complete, None)
                        .await;
                }

                black_box(result)
            })
        })
    });

    group.finish();
}

// =============================================================================
// Criterion Configuration
// =============================================================================

criterion_group!(
    benches,
    bench_storage_operations,
    bench_flow_execution,
    bench_worker_operations,
    bench_end_to_end,
);
criterion_main!(benches);
