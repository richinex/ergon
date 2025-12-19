//! File Processing Flow Example
//!
//! This example demonstrates:
//! - Reading CSV files in a flow step
//! - Parsing and validating data
//! - Multi-step data transformation with `inputs` attribute
//! - Writing processed results to output files
//! - Sequential data pipeline with step dependencies
//!
//! ## Scenario
//! - Process 3 quarterly sales CSV files (Q1, Q2, Q3)
//! - Each file contains transaction records with ID, amount, and category
//! - 4-step pipeline: read CSV → validate/clean → compute statistics → write report
//! - Invalid records (negative amounts, empty categories) are filtered out
//! - Final reports include statistics and category breakdowns
//!
//! ## Key Takeaways
//! - The `inputs` attribute automatically wires step outputs to inputs
//! - Each step depends on the previous step's output (sequential pipeline)
//! - File I/O operations work seamlessly within flow steps
//! - Invalid data is gracefully handled and filtered during validation
//! - Reports are written to disk with detailed statistics and record details
//!
//! ## Run with
//! ```bash
//! cargo run --example file_processing_flow
//! ```

use ergon::prelude::*;
use std::path::Path;
use std::sync::Arc;

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

        println!("  [Step 1] Read {} records", records.len());
        Ok(records)
    }

    #[step(inputs(records = "read_csv"))]
    async fn validate_and_clean(
        self: Arc<Self>,
        records: Vec<Record>,
    ) -> Result<Vec<Record>, String> {
        println!(
            "  [Step 2] Validating and cleaning {} records",
            records.len()
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
            "  [Step 2] Cleaned: {} valid, {} invalid",
            cleaned.len(),
            invalid_count
        );
        Ok(cleaned)
    }

    #[step(inputs(records = "validate_and_clean"))]
    async fn compute_statistics(
        self: Arc<Self>,
        records: Vec<Record>,
    ) -> Result<Statistics, String> {
        println!("  [Step 3] Computing statistics");

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
            "  [Step 3] Stats: count={}, mean={:.2}, min={:.2}, max={:.2}",
            total, mean, min, max
        );

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

        println!("  [Step 4] Report written successfully");

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
    // Create sample input files
    setup_sample_files().await?;

    // Create storage
    let storage = Arc::new(InMemoryExecutionLog::new());

    // Schedule file processing flows
    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    let files = vec![
        ("data/sales_2024_q1.csv", "output/sales_q1_report.txt"),
        ("data/sales_2024_q2.csv", "output/sales_q2_report.txt"),
        ("data/sales_2024_q3.csv", "output/sales_q3_report.txt"),
    ];

    for (input, output) in files {
        let processor = FileProcessor {
            input_file: input.to_string(),
            output_file: output.to_string(),
        };
        scheduler.schedule_with(processor, Uuid::new_v4()).await?;
    }

    // Start worker to process flows
    let worker = Worker::new(storage.clone(), "file-worker");
    worker
        .register(|flow: Arc<FileProcessor>| flow.process())
        .await;

    let worker_handle = worker.start().await;

    // Wait for completion
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let incomplete = storage.get_incomplete_flows().await?;
        if incomplete.is_empty() {
            break;
        }
    }

    worker_handle.shutdown().await;

    Ok(())
}

async fn setup_sample_files() -> Result<(), Box<dyn std::error::Error>> {
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

    Ok(())
}
