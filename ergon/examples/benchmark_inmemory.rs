//! InMemory Storage Benchmark
//!
//! This example demonstrates:
//! - InMemory storage backend performance characteristics
//! - Multi-worker parallel flow processing
//! - Sequential data analysis pipeline with 4 steps
//! - Complex aggregations with inputs attribute
//! - Throughput measurement and performance metrics
//!
//! ## Scenario
//! - Process 20 data analysis flows concurrently
//! - Each flow: generate_dataset → compute_statistics → transform_data → aggregate_results
//! - 4 workers processing flows in parallel (5 flows per worker average)
//! - Dataset: 1000 random values per flow
//! - Statistics: mean, std_dev, min, max computed
//! - Transformations: coefficient of variation, range ratio, IQR, normalized mean
//! - Final score: weighted aggregation of all metrics
//! - Workers poll every 50ms for new flows
//!
//! ## Key Takeaways
//! - InMemory storage provides fastest performance (no I/O overhead)
//! - 4 workers achieve near-linear parallel speedup
//! - Each flow takes ~30ms total (5ms + 10ms + 8ms + 7ms per step)
//! - Sequential steps ensure data dependencies are respected
//! - inputs attribute automatically passes data between steps
//! - Throughput reported in flows/sec for performance comparison
//! - Best choice for development, testing, and single-process deployments
//!
//! ## Run with
//! ```bash
//! cargo run --example benchmark_inmemory
//! ```

use ergon::prelude::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

const NUM_FLOWS: usize = 20;
const SEED_BASE: u64 = 12345;

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct DataAnalysis {
    id: u64,
    size: usize,
}

impl DataAnalysis {
    #[flow]
    async fn analyze(self: Arc<Self>) -> Result<AnalysisResult, String> {
        let dataset = self.clone().generate_dataset().await?;
        let stats = self.clone().compute_statistics(dataset).await?;
        let transformed = self.clone().transform_data(stats).await?;
        let result = self.clone().aggregate_results(transformed).await?;
        Ok(result)
    }

    #[step]
    async fn generate_dataset(self: Arc<Self>) -> Result<Dataset, String> {
        let seed = SEED_BASE + self.id;
        let mut values = Vec::with_capacity(self.size);

        for i in 0..self.size {
            let value = ((seed * 1103515245 + i as u64 * 12345) % 2147483648) as f64 / 2147483648.0;
            values.push(value * 1000.0);
        }

        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok(Dataset {
            values,
            source_id: self.id,
        })
    }

    #[step(inputs(dataset = "generate_dataset"))]
    async fn compute_statistics(self: Arc<Self>, dataset: Dataset) -> Result<Statistics, String> {
        let sum: f64 = dataset.values.iter().sum();
        let mean = sum / dataset.values.len() as f64;
        let variance = dataset
            .values
            .iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>()
            / dataset.values.len() as f64;
        let std_dev = variance.sqrt();
        let min = dataset.values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max = dataset
            .values
            .iter()
            .fold(f64::NEG_INFINITY, |a, &b| a.max(b));

        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(Statistics {
            mean,
            std_dev,
            min,
            max,
            count: dataset.values.len(),
            source_id: dataset.source_id,
        })
    }

    #[step(inputs(stats = "compute_statistics"))]
    async fn transform_data(self: Arc<Self>, stats: Statistics) -> Result<TransformedData, String> {
        let cv = if stats.mean != 0.0 {
            (stats.std_dev / stats.mean) * 100.0
        } else {
            0.0
        };
        let range = stats.max - stats.min;
        let range_ratio = if stats.std_dev != 0.0 {
            range / stats.std_dev
        } else {
            0.0
        };
        let iqr = 1.349 * stats.std_dev;

        tokio::time::sleep(Duration::from_millis(8)).await;
        Ok(TransformedData {
            cv,
            range,
            range_ratio,
            iqr,
            norm_mean: stats.mean / stats.max,
            source_id: stats.source_id,
        })
    }

    #[step(inputs(transformed = "transform_data"))]
    async fn aggregate_results(
        self: Arc<Self>,
        transformed: TransformedData,
    ) -> Result<AnalysisResult, String> {
        let score = (transformed.cv * 0.3)
            + (transformed.range_ratio * 0.2)
            + (transformed.iqr * 0.2)
            + (transformed.norm_mean * 100.0 * 0.3);

        tokio::time::sleep(Duration::from_millis(7)).await;
        Ok(AnalysisResult {
            pipeline_id: self.id,
            final_score: score,
            cv: transformed.cv,
            range: transformed.range,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct Dataset {
    values: Vec<f64>,
    source_id: u64,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct Statistics {
    mean: f64,
    std_dev: f64,
    min: f64,
    max: f64,
    count: usize,
    source_id: u64,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct TransformedData {
    cv: f64,
    range: f64,
    range_ratio: f64,
    iqr: f64,
    norm_mean: f64,
    source_id: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct AnalysisResult {
    pipeline_id: u64,
    final_score: f64,
    cv: f64,
    range: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n========================================");
    println!("    InMemory Storage Benchmark");
    println!("========================================\n");

    let storage = Arc::new(InMemoryExecutionLog::new());
    let start = Instant::now();

    let scheduler = Scheduler::new(storage.clone());
    for i in 0..NUM_FLOWS {
        let pipeline = DataAnalysis {
            id: i as u64,
            size: 1000,
        };
        scheduler.schedule(pipeline, Uuid::new_v4()).await?;
    }

    let mut handles = vec![];
    for worker_id in 0..4 {
        let storage_clone = storage.clone();
        let id = format!("worker-{}", worker_id);

        let handle = tokio::spawn(async move {
            let worker = Worker::new(storage_clone.clone(), &id)
                .with_poll_interval(Duration::from_millis(50));
            worker
                .register(|flow: Arc<DataAnalysis>| flow.analyze())
                .await;
            let worker_handle = worker.start().await;

            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if let Ok(incomplete) = storage_clone.get_incomplete_flows().await {
                    if incomplete.is_empty() {
                        break;
                    }
                }
            }

            worker_handle.shutdown().await;
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    let duration = start.elapsed();

    println!("Results:");
    println!("  Flows:       {}", NUM_FLOWS);
    println!("  Workers:     4");
    println!("  Duration:    {:?}", duration);
    println!(
        "  Throughput:  {:.2} flows/sec\n",
        NUM_FLOWS as f64 / duration.as_secs_f64()
    );

    Ok(())
}
