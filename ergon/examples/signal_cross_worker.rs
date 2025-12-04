//! Signal Resumption Example - Redis Backend (Distributed)
//!
//! Demonstrates signal API with two-terminal workflow using Redis.
//! Supports multiple workers processing the same queue.
//!
//! ## Prerequisites
//!
//! Make sure Redis is running: `redis-cli ping` (should return PONG)
//!
//! ## Two-Terminal Demo
//!
//! ```bash
//! # Terminal 1 - Start worker (keeps running)
//! cargo build --example signal_cross_worker --features=redis
//! ./target/debug/examples/signal_cross_worker redis start
//!
//! # Terminal 2 - Send commands while worker is running
//! ./target/debug/examples/signal_cross_worker redis schedule alice
//! ./target/debug/examples/signal_cross_worker redis schedule bob
//! ./target/debug/examples/signal_cross_worker redis list
//!
//! # Watch Terminal 1 - flows are waiting for signals!
//!
//! # Terminal 2 - Resume all waiting flows
//! ./target/debug/examples/signal_cross_worker redis resume
//!
//! # Watch Terminal 1 - flows complete!
//!
//! # Or test cancellation
//! ./target/debug/examples/signal_cross_worker redis schedule charlie
//! ./target/debug/examples/signal_cross_worker redis cancel
//! ```
//!
//! ## Commands
//!
//! - `start` - Start worker (Terminal 1 - keeps running)
//! - `schedule <name>` - Schedule a new flow (Terminal 2)
//! - `list` - Show all flow statuses (Terminal 2)
//! - `resume` - Resume all waiting flows (Terminal 2)
//! - `cancel` - Cancel all waiting flows (Terminal 2)
//! - `stop` - Stop the worker (Terminal 2)

use chrono::Utc;
use ergon::executor::schedule_timer;
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[cfg(not(feature = "redis"))]
fn main() {
    eprintln!("Error: This example requires the 'redis' feature");
    eprintln!("Run with: cargo build --example signal_cross_worker --features=redis");
    std::process::exit(1);
}

#[cfg(feature = "redis")]
#[derive(Debug)]
enum Backend {
    Redis,
}

#[cfg(feature = "redis")]
#[derive(Debug)]
enum Command {
    Start,
    Schedule,
    List,
    Resume,
    Cancel,
    Stop,
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[allow(dead_code)]
struct DistributedApprovalFlow {
    name: String,
    priority: i32,
}

impl DistributedApprovalFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!(
            "[{}] {} - Starting (priority: {})",
            format_time(),
            self.name,
            self.priority
        );

        // Step 1: Pre-approval work
        self.clone().pre_approval().await?;

        // Step 2: Wait for external approval
        self.clone().await_approval().await?;

        // Step 3: Post-approval work
        self.clone().post_approval().await?;

        println!("[{}] {} - Completed!", format_time(), self.name);
        Ok("Success".to_string())
    }

    #[step]
    async fn pre_approval(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!(
            "[{}] {} - Pre-approval processing...",
            format_time(),
            self.name
        );
        schedule_timer(Duration::from_millis(200)).await?;
        println!(
            "[{}] {} - Pre-approval complete, ready for approval",
            format_time(),
            self.name
        );
        Ok(())
    }

    #[step]
    async fn await_approval(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!(
            "[{}] {} - WAITING FOR APPROVAL SIGNAL...",
            format_time(),
            self.name
        );

        let decision: String = await_external_signal("approval").await?;

        // Check if it's a cancellation
        if decision.starts_with("CANCEL:") {
            let reason = decision.strip_prefix("CANCEL:").unwrap_or("cancelled");
            println!("[{}] {} - CANCELLED: {}", format_time(), self.name, reason);
            return Err(ExecutionError::Failed(format!(
                "Workflow cancelled: {}",
                reason
            )));
        }

        println!(
            "[{}] {} - APPROVAL RECEIVED: {}",
            format_time(),
            self.name,
            decision
        );
        Ok(())
    }

    #[step]
    async fn post_approval(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!(
            "[{}] {} - Post-approval processing...",
            format_time(),
            self.name
        );
        schedule_timer(Duration::from_millis(200)).await?;
        println!("[{}] {} - Post-approval complete", format_time(), self.name);
        Ok(())
    }
}

#[allow(dead_code)]
fn format_time() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

#[cfg(feature = "redis")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use ergon::executor::{Scheduler, Worker};
    use ergon::RedisExecutionLog;

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <backend> <command> [args...]", args[0]);
        eprintln!("  backend: redis");
        eprintln!("  command: start | schedule <name> | list | resume | cancel | stop");
        std::process::exit(1);
    }

    let _backend = match args[1].as_str() {
        "redis" => Backend::Redis,
        _ => {
            eprintln!("Unknown backend: {}", args[1]);
            eprintln!("Supported: redis");
            std::process::exit(1);
        }
    };

    let command = match args[2].as_str() {
        "start" => Command::Start,
        "schedule" => Command::Schedule,
        "list" => Command::List,
        "resume" => Command::Resume,
        "cancel" => Command::Cancel,
        "stop" => Command::Stop,
        _ => {
            eprintln!("Unknown command: {}", args[2]);
            eprintln!("Supported: start, schedule, list, resume, cancel, stop");
            std::process::exit(1);
        }
    };

    let storage = Arc::new(RedisExecutionLog::new("redis://localhost:6379").await?);

    match command {
        Command::Start => {
            println!("╔══════════════════════════════════════════════════╗");
            println!("║   Ergon Worker - Signal Example (Redis)         ║");
            println!("╚══════════════════════════════════════════════════╝");
            println!();
            println!("Worker starting...");

            let worker = Worker::new(storage.clone(), "redis-worker")
                .with_timers()
                .with_timer_interval(Duration::from_millis(100))
                .with_poll_interval(Duration::from_millis(300));

            worker
                .register(|flow: Arc<DistributedApprovalFlow>| flow.process())
                .await;

            let worker = worker.start().await;

            println!("✓ Worker ready and waiting for flows");
            println!();
            println!("Open another terminal and send commands:");
            println!(
                "  {} {} schedule alice   # Schedule a flow",
                args[0], args[1]
            );
            println!(
                "  {} {} list             # View flow status",
                args[0], args[1]
            );
            println!(
                "  {} {} resume           # Resume waiting flows",
                args[0], args[1]
            );
            println!(
                "  {} {} cancel           # Cancel waiting flows",
                args[0], args[1]
            );
            println!();
            println!("Press Ctrl+C to stop");
            println!();

            tokio::signal::ctrl_c().await?;
            println!();
            println!("Shutting down worker...");
            worker.shutdown().await;
            println!("✓ Worker stopped");
        }

        Command::Schedule => {
            let name = args
                .get(3)
                .ok_or("Missing flow name. Usage: schedule <name>")?;

            let scheduler = Scheduler::new(storage.clone());
            let flow = DistributedApprovalFlow {
                name: name.clone(),
                priority: 1,
            };
            let flow_id = uuid::Uuid::new_v4();
            let task_id = scheduler.schedule(flow, flow_id).await?;

            println!("✓ Flow scheduled: {}", name);
            println!("  Flow ID: {}", flow_id);
            println!("  Task ID: {}", task_id);
            println!();
            println!("Watch Terminal 1 - a worker will process this flow");
        }

        Command::List => {
            let incomplete = storage.get_incomplete_flows().await?;

            if incomplete.is_empty() {
                println!("No flows in the system");
                println!();
                println!("Schedule a flow: {} {} schedule <name>", args[0], args[1]);
                return Ok(());
            }

            println!("Flow Status:");
            println!();

            for inv in &incomplete {
                println!("  • Flow {}", inv.id());
                println!("    Step: {}", inv.step());
                println!("    Status: {:?}", inv.status());
                println!("    Method: {}.{}", inv.class_name(), inv.method_name());
                println!();
            }

            println!("Total: {} flow(s)", incomplete.len());
        }

        Command::Resume => {
            // Resume needs approval message from args
            let message = args
                .get(3)
                .cloned()
                .unwrap_or_else(|| "approved".to_string());

            // Find all flows waiting for signals using the new method
            let waiting_signals = storage.get_waiting_signals().await?;

            if waiting_signals.is_empty() {
                println!("No flows waiting for signals");
                println!();
                println!(
                    "Schedule a flow first: {} {} schedule <name>",
                    args[0], args[1]
                );
                return Ok(());
            }

            println!(
                "Sending approval signal to {} flow(s)...",
                waiting_signals.len()
            );
            println!();

            let decision_bytes = serde_json::to_vec(&message)?;
            let mut resumed_count = 0;

            for signal in &waiting_signals {
                storage
                    .store_signal_params(signal.flow_id, signal.step, &decision_bytes)
                    .await?;

                storage.resume_flow(signal.flow_id).await?;

                println!("  ✓ Resumed {} (step {})", signal.flow_id, signal.step);
                resumed_count += 1;
            }

            println!();
            println!("✓ Sent approval to {} flow(s)", resumed_count);
            println!("  Message: {}", message);
            println!();
            println!("Watch Terminal 1 - workers will process the flows!");
        }

        Command::Cancel => {
            // Cancel needs cancellation reason from args
            let reason = args
                .get(3)
                .cloned()
                .unwrap_or_else(|| "cancelled".to_string());

            // Find all flows waiting for signals using the new method
            let waiting_signals = storage.get_waiting_signals().await?;

            if waiting_signals.is_empty() {
                println!("No flows waiting for signals");
                println!();
                println!(
                    "Schedule a flow first: {} {} schedule <name>",
                    args[0], args[1]
                );
                return Ok(());
            }

            println!(
                "Sending cancellation signal to {} flow(s)...",
                waiting_signals.len()
            );
            println!();

            let cancel_message = format!("CANCEL:{}", reason);
            let cancel_bytes = serde_json::to_vec(&cancel_message)?;
            let mut cancelled_count = 0;

            for signal in &waiting_signals {
                storage
                    .store_signal_params(signal.flow_id, signal.step, &cancel_bytes)
                    .await?;

                storage.resume_flow(signal.flow_id).await?;

                println!("  ✓ Cancelled {} (step {})", signal.flow_id, signal.step);
                cancelled_count += 1;
            }

            println!();
            println!("✓ Sent cancellation to {} flow(s)", cancelled_count);
            println!("  Reason: {}", reason);
            println!();
            println!("Watch Terminal 1 - workers will fail the flows!");
        }

        Command::Stop => {
            println!("Stopping worker...");
            println!("(Use Ctrl+C on the worker process to stop it)");
            std::process::exit(0);
        }
    }

    Ok(())
}
