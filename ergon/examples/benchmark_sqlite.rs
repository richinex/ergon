//! SQLite Storage Benchmark
//!
//! This example demonstrates:
//! - Benchmarking SQLite backend performance with complex workflows
//! - 20 parallel data analysis flows with 4 sequential steps each
//! - Data passing between steps using the inputs attribute
//! - Statistical computations and transformations
//! - Worker pool coordinating concurrent flow execution
//!
//! ## Scenario
//! Each flow generates synthetic datasets, computes statistics (mean, std dev, min, max),
//! transforms the data (coefficient of variation, range ratios), and aggregates final scores.
//! 4 workers process 20 flows in parallel, demonstrating SQLite's performance under concurrent load.
//!
//! ## Key Takeaways
//! - SQLite provides durable storage with good performance for moderate workloads
//! - Multiple workers can safely process flows concurrently with SQLite backend
//! - Complex data transformations benefit from step-level caching
//! - Each step receives outputs from previous steps via the inputs attribute
//! - Workers coordinate via shared SQLite database
//! - Temporary database file cleaned up automatically after benchmarking
//!
//! ## Run with
//! ```bash
//! cargo run --example benchmark_sqlite
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
    println!("\nSQLite Storage Benchmark");
    println!("========================\n");

    let db_path = "/tmp/ergon_benchmark_sqlite.db";
    let _ = std::fs::remove_file(db_path);

    let storage = Arc::new(SqliteExecutionLog::new(db_path)?);
    let start = Instant::now();

    let scheduler = FlowScheduler::new(storage.clone());
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
            let worker = FlowWorker::new(storage_clone.clone(), &id)
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
    let _ = std::fs::remove_file(db_path);

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
