//! Parallel File Processing Flow Example
//!
//! This example demonstrates:
//! - Concurrent file processing with multiple workers
//! - 3 workers processing 3 CSV files in parallel
//! - Each worker independently picking up flows from the queue
//! - Data passing between steps using the inputs attribute
//! - CSV parsing, validation, statistics computation, and report generation
//! - Output showing interleaved execution from different workers
//!
//! ## Scenario
//! Three CSV files (sales data for Q1, Q2, Q3) are processed concurrently by 3 workers.
//! Each flow: reads CSV, validates and cleans data, computes statistics, writes report.
//! Workers show interleaved output demonstrating true parallelism.
//!
//! ## Key Takeaways
//! - Multiple workers process different flows simultaneously
//! - Each flow progresses through 4 sequential steps with inputs attribute
//! - Data validation removes invalid records before statistics
//! - Results written to output directory with formatted reports
//! - Interleaved console output proves concurrent execution
//! - InMemory storage enables fast testing without database
//!
//! ## Run with
//! ```bash
//! cargo run --example file_processing_parallel
//! ```

use ergon::prelude::*;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// Global execution counters
static READ_CSV_COUNT: AtomicU32 = AtomicU32::new(0);
static VALIDATE_COUNT: AtomicU32 = AtomicU32::new(0);
static COMPUTE_STATS_COUNT: AtomicU32 = AtomicU32::new(0);
static WRITE_RESULTS_COUNT: AtomicU32 = AtomicU32::new(0);

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct FileProcessor {
    input_file: String,
    output_file: String,
}

impl FileProcessor {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<ProcessingReport, String> {
        println!("[Flow] Processing file: {}", self.input_file);

        // Step 1: Read and parse CSV file
        let records = self.clone().read_csv().await?;

        // Step 2: Validate and clean data (uses input from read_csv)
        let cleaned = self.clone().validate_and_clean(records).await?;

        // Step 3: Aggregate statistics (uses input from validate_and_clean)
        let stats = self.clone().compute_statistics(cleaned).await?;

        // Step 4: Write results to file (uses input from compute_statistics)
        let report = self.clone().write_results(stats).await?;

        println!(
            "[Flow] Completed: {} -> {}",
            self.input_file, self.output_file
        );
        Ok(report)
    }

    #[step]
    async fn read_csv(self: Arc<Self>) -> Result<Vec<Record>, String> {
        READ_CSV_COUNT.fetch_add(1, Ordering::Relaxed);
        println!("  [Step 1] Reading CSV file: {}", self.input_file);

        // Read file contents
        let contents = tokio::fs::read_to_string(&self.input_file)
            .await
            .map_err(|e| format!("Failed to read file: {}", e))?;

        // Parse CSV (simple comma-separated parsing)
        let mut records = Vec::new();
        for (line_num, line) in contents.lines().enumerate() {
            if line_num == 0 {
                continue; // Skip header
            }

            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() != 3 {
                return Err(format!("Invalid CSV format at line {}", line_num + 1));
            }

            let record = Record {
                id: parts[0].trim().to_string(),
                value: parts[1]
                    .trim()
                    .parse::<f64>()
                    .map_err(|_| format!("Invalid number at line {}", line_num + 1))?,
                category: parts[2].trim().to_string(),
            };

            records.push(record);
        }

        println!(
            "  [Step 1] Read {} records from {}",
            records.len(),
            self.input_file
        );

        // Simulate some processing time to make concurrency visible
        tokio::time::sleep(Duration::from_millis(200)).await;

        Ok(records)
    }

    #[step(inputs(records = "read_csv"))]
    async fn validate_and_clean(
        self: Arc<Self>,
        records: Vec<Record>,
    ) -> Result<Vec<Record>, String> {
        VALIDATE_COUNT.fetch_add(1, Ordering::Relaxed);
        println!(
            "  [Step 2] Validating {} records from {}",
            records.len(),
            self.input_file
        );

        let mut cleaned = Vec::new();
        let mut invalid_count = 0;

        for record in records {
            // Validate: value must be positive
            if record.value <= 0.0 {
                invalid_count += 1;
                continue;
            }

            // Validate: category must not be empty
            if record.category.is_empty() {
                invalid_count += 1;
                continue;
            }

            cleaned.push(record);
        }

        println!(
            "  [Step 2] Cleaned {}: {} valid, {} invalid",
            self.input_file,
            cleaned.len(),
            invalid_count
        );

        // Simulate processing time
        tokio::time::sleep(Duration::from_millis(150)).await;

        Ok(cleaned)
    }

    #[step(inputs(records = "validate_and_clean"))]
    async fn compute_statistics(
        self: Arc<Self>,
        records: Vec<Record>,
    ) -> Result<Statistics, String> {
        COMPUTE_STATS_COUNT.fetch_add(1, Ordering::Relaxed);
        println!("  [Step 3] Computing statistics for {}", self.input_file);

        let total = records.len();
        let sum: f64 = records.iter().map(|r| r.value).sum();
        let mean = if total > 0 { sum / total as f64 } else { 0.0 };

        let min = records
            .iter()
            .map(|r| r.value)
            .fold(f64::INFINITY, f64::min);
        let max = records
            .iter()
            .map(|r| r.value)
            .fold(f64::NEG_INFINITY, f64::max);

        // Group by category
        let mut category_counts = std::collections::HashMap::new();
        for record in &records {
            *category_counts.entry(record.category.clone()).or_insert(0) += 1;
        }

        println!(
            "  [Step 3] Stats for {}: count={}, mean={:.2}, min={:.2}, max={:.2}",
            self.input_file, total, mean, min, max
        );

        // Simulate processing time
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(Statistics {
            total,
            sum,
            mean,
            min,
            max,
            category_counts,
            records,
        })
    }

    #[step(inputs(stats = "compute_statistics"))]
    async fn write_results(self: Arc<Self>, stats: Statistics) -> Result<ProcessingReport, String> {
        WRITE_RESULTS_COUNT.fetch_add(1, Ordering::Relaxed);
        println!("  [Step 4] Writing results to: {}", self.output_file);

        // Create output directory if needed
        if let Some(parent) = Path::new(&self.output_file).parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| format!("Failed to create output directory: {}", e))?;
        }

        // Write summary report
        let mut output = String::new();
        output.push_str("# Processing Report\n\n");
        output.push_str(&format!("Input File: {}\n", self.input_file));
        output.push_str(&format!("Total Records: {}\n", stats.total));
        output.push_str(&format!("Sum: {:.2}\n", stats.sum));
        output.push_str(&format!("Mean: {:.2}\n", stats.mean));
        output.push_str(&format!("Min: {:.2}\n", stats.min));
        output.push_str(&format!("Max: {:.2}\n\n", stats.max));

        output.push_str("Categories:\n");
        for (category, count) in &stats.category_counts {
            output.push_str(&format!("  {}: {} records\n", category, count));
        }

        output.push_str("\nDetailed Records:\n");
        for record in &stats.records {
            output.push_str(&format!(
                "  {} | {:.2} | {}\n",
                record.id, record.value, record.category
            ));
        }

        tokio::fs::write(&self.output_file, output)
            .await
            .map_err(|e| format!("Failed to write output file: {}", e))?;

        println!("  [Step 4] Report written: {}", self.output_file);

        // Simulate processing time
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(ProcessingReport {
            input_file: self.input_file.clone(),
            output_file: self.output_file.clone(),
            records_processed: stats.total,
            mean_value: stats.mean,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct Record {
    id: String,
    value: f64,
    category: String,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct Statistics {
    total: usize,
    sum: f64,
    mean: f64,
    min: f64,
    max: f64,
    category_counts: std::collections::HashMap<String, usize>,
    records: Vec<Record>,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct ProcessingReport {
    input_file: String,
    output_file: String,
    records_processed: usize,
    mean_value: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║        Parallel File Processing Flow Example              ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Create sample input files
    setup_sample_files().await?;

    let storage = Arc::new(InMemoryExecutionLog::new());

    // ============================================================
    // PART 1: API Server / Scheduler Process
    // ============================================================
    // In production, this would be an HTTP endpoint that:
    //   POST /api/process-files -> schedules workflows -> returns 202 Accepted
    //
    // The scheduler does NOT wait for completion. It returns immediately.
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 1: Scheduling File Processing (API Server)           ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let scheduler = Scheduler::new(storage.clone());

    let files = vec![
        ("data/sales_2024_q1.csv", "output/sales_q1_report.txt"),
        ("data/sales_2024_q2.csv", "output/sales_q2_report.txt"),
        ("data/sales_2024_q3.csv", "output/sales_q3_report.txt"),
    ];

    let mut task_ids = Vec::new();
    for (input, output) in &files {
        let processor = FileProcessor {
            input_file: input.to_string(),
            output_file: output.to_string(),
        };
        let task_id = scheduler.schedule(processor, Uuid::new_v4()).await?;
        println!(
            "   ✓ {} scheduled (task_id: {})",
            input,
            &task_id.to_string()[..8]
        );
        task_ids.push(task_id);
    }

    println!("\n   → In production: Return HTTP 202 Accepted");
    println!(
        "   → Response body: {{\"task_ids\": [{:?}, ...]}}",
        &task_ids[0].to_string()[..8]
    );
    println!("   → Client polls GET /api/tasks/:id for status\n");

    // ============================================================
    // PART 2: Worker Service (Separate Process)
    // ============================================================
    // In production, workers run in separate pods/containers/services.
    // They continuously poll the shared storage for work.
    //
    // Workers are completely decoupled from the scheduler.
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 2: Starting Workers (Separate Service)               ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let start = Instant::now();

    // Start 3 workers to process flows in parallel
    let mut worker_handles = vec![];

    for worker_id in 1..=3 {
        let storage_clone = storage.clone();
        let worker_name = format!("worker-{}", worker_id);

        let handle = tokio::spawn(async move {
            let worker = Worker::new(storage_clone.clone(), &worker_name)
                .with_poll_interval(Duration::from_millis(50));

            worker
                .register(|flow: Arc<FileProcessor>| flow.process())
                .await;

            let worker_handle = worker.start().await;

            // Wait for completion
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

        worker_handles.push(handle);
    }

    println!("   ✓ 3 workers started and polling for work\n");

    // ============================================================
    // PART 3: Client Status Monitoring (Demo Only)
    // ============================================================
    // In production, the CLIENT would poll a status API endpoint:
    //   GET /api/tasks/:id -> returns {status: "pending|running|complete|failed"}
    //
    // This demonstrates that workflows actually execute, but in production
    // the scheduler process would NOT do this polling.
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 3: Monitoring Status (Client Would Poll API)         ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");
    println!("   → Simulating client polling GET /api/tasks/:id...\n");

    // Wait for all workers to complete
    for handle in worker_handles {
        handle.await?;
    }

    let duration = start.elapsed();

    println!("\n=== Summary ===\n");

    println!("Step Execution Counts:");
    println!(
        "  read_csv:         {}",
        READ_CSV_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  validate_clean:   {}",
        VALIDATE_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  compute_stats:    {}",
        COMPUTE_STATS_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  write_results:    {}",
        WRITE_RESULTS_COUNT.load(Ordering::Relaxed)
    );

    println!("\nExecution Metrics:");
    println!("  Files processed:  {}", files.len());
    println!("  Workers used:     3");
    println!("  Total time:       {:.2}s", duration.as_secs_f64());

    Ok(())
}

async fn setup_sample_files() -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting up sample data files...\n");

    // Create data directory
    tokio::fs::create_dir_all("data").await?;

    // Sample data for Q1
    let q1_data = "id,amount,category\n\
                   TXN001,150.50,Electronics\n\
                   TXN002,299.99,Clothing\n\
                   TXN003,45.00,Books\n\
                   TXN004,0.00,Electronics\n\
                   TXN005,125.75,Electronics\n\
                   TXN006,89.99,Books\n";

    // Sample data for Q2
    let q2_data = "id,amount,category\n\
                   TXN007,450.00,Electronics\n\
                   TXN008,199.99,Clothing\n\
                   TXN009,75.50,Books\n\
                   TXN010,320.00,Electronics\n\
                   TXN011,-10.00,Clothing\n\
                   TXN012,99.99,Books\n";

    // Sample data for Q3
    let q3_data = "id,amount,category\n\
                   TXN013,550.00,Electronics\n\
                   TXN014,399.99,Clothing\n\
                   TXN015,125.00,Books\n\
                   TXN016,0.00,Electronics\n\
                   TXN017,275.50,Clothing\n\
                   TXN018,149.99,Books\n";

    tokio::fs::write("data/sales_2024_q1.csv", q1_data).await?;
    tokio::fs::write("data/sales_2024_q2.csv", q2_data).await?;
    tokio::fs::write("data/sales_2024_q3.csv", q3_data).await?;

    println!("Created sample CSV files in 'data/' directory");
    Ok(())
}
