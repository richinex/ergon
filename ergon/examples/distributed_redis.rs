//! Redis distributed worker example
//!
//! This example demonstrates:
//! - TRUE multi-machine distributed execution using Redis
//! - Workers can run on completely separate machines across the network
//! - Unlike SQLite which requires shared filesystem, Redis works over the network
//! - Separate scheduler and worker processes
//! - Multiple workers processing jobs in parallel across machines
//!
//! ## Scenario
//! - Machine A: Scheduler schedules 5 batch jobs and 3 email notifications
//! - Machine B: Worker-1 polls Redis and processes available jobs
//! - Machine C: Worker-2 polls Redis and processes available jobs
//! - Workers coordinate via Redis to avoid duplicate processing
//! - Each job is processed exactly once by one of the workers
//!
//! ## Key Takeaways
//! - Redis enables true distributed execution across network-connected machines
//! - Scheduler and workers are separate processes that can run anywhere
//! - Workers coordinate automatically via Redis (no duplicate processing)
//! - Optimistic concurrency control prevents race conditions
//! - Workers can be added/removed dynamically
//!
//! ## Prerequisites
//! Start Redis server:
//! ```bash
//! docker run -d -p 6379:6379 redis:latest
//! # or
//! redis-server
//! ```
//!
//! ## Run with
//! Scheduler (Machine A):
//! ```bash
//! cargo run --example distributed_redis --features redis -- --mode scheduler
//! ```
//!
//! Workers (Machines B, C, D...):
//! ```bash
//! cargo run --example distributed_redis --features redis -- --mode worker --id worker-1
//! cargo run --example distributed_redis --features redis -- --mode worker --id worker-2
//! ```
//!
//! ## Architecture
//! ```text
//! Machine A (Scheduler) --+
//!                         |
//! Machine B (Worker 1) ---+--> Redis Server (any machine)
//!                         |      - ergon:queue:pending (LIST)
//! Machine C (Worker 2) ---+      - ergon:flow:{id} (HASH)
//!                                - ergon:running (ZSET)
//! ```

use ergon::prelude::*;
use std::env;
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct DataProcessor {
    job_id: String,
    data_size_mb: u32,
    priority: String,
}

impl DataProcessor {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, String> {
        println!(
            "[Flow {}] Starting data processing ({} MB, priority: {})",
            self.job_id, self.data_size_mb, self.priority
        );

        // Step 1: Validate data
        self.clone().validate().await?;

        // Step 2: Transform data
        let transformed = self.clone().transform().await?;

        // Step 3: Save results
        let result = self.clone().save(transformed).await?;

        println!("[Flow {}] Completed: {}", self.job_id, result);
        Ok(result)
    }

    #[step]
    async fn validate(self: Arc<Self>) -> Result<bool, String> {
        println!("  [Validate] Checking data integrity for {}", self.job_id);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(true)
    }

    #[step]
    async fn transform(self: Arc<Self>) -> Result<String, String> {
        println!("  [Transform] Processing {} MB of data", self.data_size_mb);
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(format!("transformed_{}", self.job_id))
    }

    #[step]
    async fn save(self: Arc<Self>, transformed_id: String) -> Result<String, String> {
        println!("  [Save] Storing results: {}", transformed_id);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(format!("result_{}", transformed_id))
    }
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct EmailTask {
    recipient: String,
    subject: String,
    template: String,
}

impl EmailTask {
    #[flow]
    async fn send(self: Arc<Self>) -> Result<String, String> {
        println!("[Email] Sending to {}: {}", self.recipient, self.subject);

        // Generate deterministic email_id at flow level using hash of inputs
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.recipient.hash(&mut hasher);
        self.subject.hash(&mut hasher);
        self.template.hash(&mut hasher);
        let email_id = format!("email_id_{:x}", hasher.finish());

        let rendered = self.clone().render_template().await?;
        let result = self.clone().deliver(rendered, email_id).await?;

        println!("[Email] Sent successfully: {}", result);
        Ok(result)
    }

    #[step]
    async fn render_template(self: Arc<Self>) -> Result<String, String> {
        println!(
            "  [Render] Generating email from template: {}",
            self.template
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(format!("<html>Hello {}</html>", self.recipient))
    }

    #[step]
    async fn deliver(self: Arc<Self>, html: String, email_id: String) -> Result<String, String> {
        println!("  [Deliver] Sending via SMTP ({}...)", &html[..20]);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(email_id)
    }
}

async fn run_scheduler(redis_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Create Redis storage (network-accessible!)
    let storage = Arc::new(RedisExecutionLog::new(redis_url).await?);

    let scheduler = Scheduler::new(storage.clone());

    // Schedule data processing jobs
    for i in 1..=3 {
        let job = DataProcessor {
            job_id: format!("JOB-{:03}", i),
            data_size_mb: i * 50,
            priority: if i == 1 { "high" } else { "normal" }.to_string(),
        };

        let flow_id = Uuid::new_v4();
        scheduler.schedule(job, flow_id).await?;
    }

    // Schedule email tasks
    for i in 1..=2 {
        let email = EmailTask {
            recipient: format!("user{}@example.com", i),
            subject: format!("Weekly Report #{}", i),
            template: "weekly_report.html".to_string(),
        };

        let flow_id = Uuid::new_v4();
        scheduler.schedule(email, flow_id).await?;
    }

    Ok(())
}

async fn run_worker(redis_url: &str, worker_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Create Redis storage (same Redis, different machine!)
    let storage = Arc::new(RedisExecutionLog::new(redis_url).await?);

    // Create worker with faster polling for demo
    let worker =
        Worker::new(storage.clone(), worker_id).with_poll_interval(Duration::from_millis(100));

    // Register flow types this worker can handle
    worker
        .register(|flow: Arc<DataProcessor>| flow.process())
        .await;
    worker.register(|flow: Arc<EmailTask>| flow.send()).await;

    let handle = worker.start().await;

    // Run for 30 seconds (demo mode)
    tokio::time::sleep(Duration::from_secs(30)).await;

    handle.shutdown().await;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    // Parse command line arguments
    let mut mode = "scheduler"; // default mode
    let mut worker_id = "worker-1"; // default worker id
    let mut redis_url = "redis://127.0.0.1:6379"; // default Redis URL

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" => {
                if i + 1 < args.len() {
                    mode = &args[i + 1];
                    i += 2;
                } else {
                    eprintln!("Error: --mode requires a value");
                    std::process::exit(1);
                }
            }
            "--id" => {
                if i + 1 < args.len() {
                    worker_id = &args[i + 1];
                    i += 2;
                } else {
                    eprintln!("Error: --id requires a value");
                    std::process::exit(1);
                }
            }
            "--redis" => {
                if i + 1 < args.len() {
                    redis_url = &args[i + 1];
                    i += 2;
                } else {
                    eprintln!("Error: --redis requires a value");
                    std::process::exit(1);
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    println!("\n");
    println!("\nErgon Redis Distributed Execution");
    println!("==================================");
    println!();

    match mode {
        "scheduler" => run_scheduler(redis_url).await?,
        "worker" => run_worker(redis_url, worker_id).await?,
        _ => {
            eprintln!(
                "Error: Invalid mode '{}'. Use 'scheduler' or 'worker'",
                mode
            );
            eprintln!("\nUsage:");
            eprintln!("  Scheduler: cargo run --example distributed_redis --features redis -- --mode scheduler");
            eprintln!("  Worker:    cargo run --example distributed_redis --features redis -- --mode worker --id worker-1");
            std::process::exit(1);
        }
    }

    Ok(())
}
