use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ergon::prelude::*;
use ergon::Retryable;
use std::sync::Arc;

// =============================================================================
// Mock Error Types for Benchmarking
// =============================================================================

#[derive(Debug, Clone)]
struct SimpleError {
    message: String,
}

impl std::fmt::Display for SimpleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SimpleError {}

impl Retryable for SimpleError {
    fn is_retryable(&self) -> bool {
        // Simple check - simulates cheap is_retryable()
        self.message.contains("timeout")
    }
}

#[derive(Debug, Clone)]
struct ComplexError {
    code: u32,
    category: String,
    details: Vec<String>,
}

impl std::fmt::Display for ComplexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.category)
    }
}

impl std::error::Error for ComplexError {}

impl Retryable for ComplexError {
    fn is_retryable(&self) -> bool {
        // Complex check - simulates expensive is_retryable()
        match self.category.as_str() {
            "network" => true,
            "timeout" => true,
            "rate_limit" => true,
            "validation" => false,
            "not_found" => false,
            "permission" => false,
            _ => self.details.iter().any(|d| d.contains("retry")),
        }
    }
}

// =============================================================================
// Benchmarks
// =============================================================================

fn bench_simple_is_retryable(c: &mut Criterion) {
    let error = SimpleError {
        message: "Network timeout".to_string(),
    };

    c.bench_function("simple_is_retryable_call", |b| {
        b.iter(|| black_box(error.is_retryable()))
    });
}

fn bench_complex_is_retryable(c: &mut Criterion) {
    let error = ComplexError {
        code: 500,
        category: "network".to_string(),
        details: vec!["Connection refused".to_string()],
    };

    c.bench_function("complex_is_retryable_call", |b| {
        b.iter(|| black_box(error.is_retryable()))
    });
}

fn bench_trait_dispatch_overhead(c: &mut Criterion) {
    let error = SimpleError {
        message: "Network timeout".to_string(),
    };
    let error_ref: &dyn Retryable = &error;

    c.bench_function("trait_object_is_retryable", |b| {
        b.iter(|| {
            // Call through trait object (dynamic dispatch)
            black_box(error_ref.is_retryable())
        })
    });
}

fn bench_storage_query_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_queries");

    // Use tokio runtime for async benchmarks
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Benchmark InMemoryExecutionLog (fast)
    group.bench_function("inmemory_get_invocations", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(ergon::InMemoryExecutionLog::new());
                let flow_id = uuid::Uuid::new_v4();

                // Simulate getting invocations (what has_inner_step_retryability_flag does)
                let invocations = storage.get_invocations_for_flow(flow_id).await;
                black_box(invocations)
            })
        })
    });

    // Benchmark SQLite (realistic production)
    group.bench_function("sqlite_get_invocations", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(ergon::SqliteExecutionLog::new(":memory:").await.unwrap());
                let flow_id = uuid::Uuid::new_v4();

                // Simulate getting invocations (what has_inner_step_retryability_flag does)
                let invocations = storage.get_invocations_for_flow(flow_id).await;
                black_box(invocations)
            })
        })
    });

    // Benchmark with actual data
    group.bench_function("sqlite_get_invocations_with_data", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = Arc::new(ergon::SqliteExecutionLog::new(":memory:").await.unwrap());
                let flow_id = uuid::Uuid::new_v4();

                // Add some invocations
                let params = ergon::core::serialize_value(&()).unwrap();
                for step in 0..5 {
                    let _ = storage
                        .log_invocation_start(ergon::storage::InvocationStartParams {
                            id: flow_id,
                            step,
                            class_name: "TestFlow",
                            method_name: "test_step",
                            status: ergon::InvocationStatus::Complete,
                            parameters: &params,
                            retry_policy: None,
                        })
                        .await;
                }

                // Now query (what has_inner_step_retryability_flag does)
                let invocations = storage.get_invocations_for_flow(flow_id).await.unwrap();

                // Simulate checking for flags
                let has_flag = invocations
                    .iter()
                    .filter(|inv| inv.step() > 0)
                    .any(|inv| inv.is_retryable().is_some());

                black_box(has_flag)
            })
        })
    });

    group.finish();
}

fn bench_redundant_vs_optimized(c: &mut Criterion) {
    let mut group = c.benchmark_group("redundant_vs_optimized");

    let error = SimpleError {
        message: "Network timeout".to_string(),
    };

    // Simulate redundant approach (step + flow both call)
    group.bench_function("redundant_two_calls", |b| {
        b.iter(|| {
            // Step macro call
            let is_retryable_1 = black_box(error.is_retryable());
            // Flow macro call (redundant)
            let is_retryable_2 = black_box(error.is_retryable());
            (is_retryable_1, is_retryable_2)
        })
    });

    // Simulate optimized approach (step calls, flow queries storage)
    group.bench_function("optimized_call_plus_query", |b| {
        b.iter(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let storage = Arc::new(ergon::InMemoryExecutionLog::new());
                let flow_id = uuid::Uuid::new_v4();

                // Step macro call
                let is_retryable = black_box(error.is_retryable());

                // Flow macro: Query storage instead of calling is_retryable()
                let invocations = storage.get_invocations_for_flow(flow_id).await.unwrap();
                let has_flag = invocations
                    .iter()
                    .filter(|inv| inv.step() > 0)
                    .any(|inv| inv.is_retryable().is_some());

                (is_retryable, has_flag)
            })
        })
    });

    // Simulate optimized with SQLite (more realistic)
    group.bench_function("optimized_call_plus_sqlite_query", |b| {
        b.iter(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let storage = Arc::new(ergon::SqliteExecutionLog::new(":memory:").await.unwrap());
                let flow_id = uuid::Uuid::new_v4();

                // Step macro call
                let is_retryable = black_box(error.is_retryable());

                // Flow macro: Query storage instead of calling is_retryable()
                let invocations = storage.get_invocations_for_flow(flow_id).await.unwrap();
                let has_flag = invocations
                    .iter()
                    .filter(|inv| inv.step() > 0)
                    .any(|inv| inv.is_retryable().is_some());

                (is_retryable, has_flag)
            })
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_is_retryable,
    bench_complex_is_retryable,
    bench_trait_dispatch_overhead,
    bench_storage_query_overhead,
    bench_redundant_vs_optimized,
);
criterion_main!(benches);
