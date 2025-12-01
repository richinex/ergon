//! Redis distributed worker example
//!
//! This example demonstrates TRUE multi-machine distributed execution using Redis.
//! Unlike SQLite which requires shared filesystem access, Redis workers can run
//! on completely separate machines across the network.
//!
//! # Setup
//!
//! 1. Start Redis server:
//!    ```bash
//!    docker run -d -p 6379:6379 redis:latest
//!    # or
//!    redis-server
//!    ```
//!
//! 2. Run scheduler (can be on Machine A):
//!    ```bash
//!    cargo run --example distributed_redis --features redis -- --mode scheduler
//!    ```
//!
//! 3. Run workers (can be on Machine B, C, D...):
//!    ```bash
//!    cargo run --example distributed_redis --features redis -- --mode worker --id worker-1
//!    cargo run --example distributed_redis --features redis -- --mode worker --id worker-2
//!    ```
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐
//! │  Machine A      │
//! │  Scheduler      │ ──┐
//! └─────────────────┘   │
//!                       ├──→ Redis Server (any machine)
//! ┌─────────────────┐   │    - ergon:queue:pending (LIST)
//! │  Machine B      │   │    - ergon:flow:{id} (HASH)
//! │  Worker 1       │ ──┤    - ergon:running (ZSET)
//! └─────────────────┘   │
//!                       │
//! ┌─────────────────┐   │
//! │  Machine C      │   │
//! │  Worker 2       │ ──┘
//! └─────────────────┘
//! ```
//!
//! Run with: cargo run --example distributed_redis --features redis

use ergon::prelude::*;
use std::env;
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone)]
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
        println!(
            "  [Transform] Processing {} MB of data",
            self.data_size_mb
        );
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

#[derive(Serialize, Deserialize, Clone)]
struct EmailTask {
    recipient: String,
    subject: String,
    template: String,
}

impl EmailTask {
    #[flow]
    async fn send(self: Arc<Self>) -> Result<String, String> {
        println!("[Email] Sending to {}: {}", self.recipient, self.subject);

        let rendered = self.clone().render_template().await?;
        let result = self.clone().deliver(rendered).await?;

        println!("[Email] Sent successfully: {}", result);
        Ok(result)
    }

    #[step]
    async fn render_template(self: Arc<Self>) -> Result<String, String> {
        println!("  [Render] Generating email from template: {}", self.template);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(format!("<html>Hello {}</html>", self.recipient))
    }

    #[step]
    async fn deliver(self: Arc<Self>, html: String) -> Result<String, String> {
        println!("  [Deliver] Sending via SMTP ({}...)", &html[..20]);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(format!("email_id_{}", uuid::Uuid::new_v4()))
    }
}

async fn run_scheduler(redis_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Ergon Redis Distributed Scheduler ===\n");
    println!("Redis URL: {}\n", redis_url);

    // Create Redis storage (network-accessible!)
    let storage = Arc::new(RedisExecutionLog::new(redis_url)?);

    println!("1. Creating scheduler...");
    let scheduler = FlowScheduler::new(storage.clone());

    println!("2. Scheduling jobs for distributed execution...\n");

    // Schedule data processing jobs
    for i in 1..=3 {
        let job = DataProcessor {
            job_id: format!("JOB-{:03}", i),
            data_size_mb: i * 50,
            priority: if i == 1 { "high" } else { "normal" }.to_string(),
        };

        let flow_id = Uuid::new_v4();
        let task_id = scheduler.schedule(job, flow_id).await?;
        println!(
            "   ✓ Scheduled JOB-{:03} (task_id: {})",
            i,
            task_id.to_string().split('-').next().unwrap_or("")
        );
    }

    // Schedule email tasks
    for i in 1..=2 {
        let email = EmailTask {
            recipient: format!("user{}@example.com", i),
            subject: format!("Weekly Report #{}", i),
            template: "weekly_report.html".to_string(),
        };

        let flow_id = Uuid::new_v4();
        let task_id = scheduler.schedule(email, flow_id).await?;
        println!(
            "   ✓ Scheduled email to user{} (task_id: {})",
            i,
            task_id.to_string().split('-').next().unwrap_or("")
        );
    }

    println!("\n3. Scheduler complete! Workers can now pick up jobs.\n");
    println!("Next steps:");
    println!("  - Start workers on any machine with network access to Redis");
    println!("  - Run: cargo run --example distributed_redis --features redis -- --mode worker --id <worker-name>\n");

    Ok(())
}

async fn run_worker(
    redis_url: &str,
    worker_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Ergon Redis Distributed Worker ===\n");
    println!("Worker ID: {}", worker_id);
    println!("Redis URL: {}\n", redis_url);

    // Create Redis storage (same Redis, different machine!)
    let storage = Arc::new(RedisExecutionLog::new(redis_url)?);

    println!("1. Starting worker...");

    // Create worker with faster polling for demo
    let worker = FlowWorker::new(storage.clone(), worker_id)
        .with_poll_interval(Duration::from_millis(100));

    // Register flow types this worker can handle
    worker
        .register(|flow: Arc<DataProcessor>| flow.process())
        .await;
    worker.register(|flow: Arc<EmailTask>| flow.send()).await;

    println!("2. Worker registered to handle:");
    println!("   - DataProcessor flows");
    println!("   - EmailTask flows");
    println!("\n3. Worker polling Redis queue...\n");

    let handle = worker.start().await;

    // Run for 30 seconds (demo mode)
    println!("Worker running for 30 seconds (Ctrl+C to stop)...\n");
    tokio::time::sleep(Duration::from_secs(30)).await;

    println!("\n4. Shutting down worker...");
    handle.shutdown().await;

    println!("   ✓ Worker stopped gracefully\n");

    // Show final status
    let incomplete = storage.get_incomplete_flows().await?;
    if incomplete.is_empty() {
        println!("=== All queued flows completed! ===");
    } else {
        println!("=== {} flows still pending ===", incomplete.len());
    }

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
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║         Ergon Redis Distributed Execution               ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();

    match mode {
        "scheduler" => run_scheduler(redis_url).await?,
        "worker" => run_worker(redis_url, worker_id).await?,
        _ => {
            eprintln!("Error: Invalid mode '{}'. Use 'scheduler' or 'worker'", mode);
            eprintln!("\nUsage:");
            eprintln!("  Scheduler: cargo run --example distributed_redis --features redis -- --mode scheduler");
            eprintln!("  Worker:    cargo run --example distributed_redis --features redis -- --mode worker --id worker-1");
            std::process::exit(1);
        }
    }

    Ok(())
}
