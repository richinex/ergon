//! Signal Resumption Example - SQLite Backend
//!
//! Demonstrates signal API with two-terminal workflow.
//!
//! ## Two-Terminal Demo
//!
//! ```bash
//! # Terminal 1 - Start worker (keeps running)
//! cargo build --example signal_cross_flow --features=sqlite
//! ./target/debug/examples/signal_cross_flow sqlite start
//!
//! # Terminal 2 - Send commands while worker is running
//! ./target/debug/examples/signal_cross_flow sqlite schedule alice
//! ./target/debug/examples/signal_cross_flow sqlite schedule bob
//! ./target/debug/examples/signal_cross_flow sqlite list
//!
//! # Watch Terminal 1 - flows are waiting for signals!
//!
//! # Terminal 2 - Resume all waiting flows
//! ./target/debug/examples/signal_cross_flow sqlite resume
//!
//! # Watch Terminal 1 - flows complete!
//!
//! # Or test cancellation
//! ./target/debug/examples/signal_cross_flow sqlite schedule charlie
//! ./target/debug/examples/signal_cross_flow sqlite cancel
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
use ergon::executor::{schedule_timer, Scheduler, Worker};
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// Custom Error Type
// ============================================================================

/// Approval workflow error type
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ApprovalError {
    // Cancellation - permanent
    Cancelled(String),

    // Infrastructure errors - transient
    Infrastructure(String),

    // Generic errors
    Failed(String),
}

impl std::fmt::Display for ApprovalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApprovalError::Cancelled(msg) => write!(f, "Workflow cancelled: {}", msg),
            ApprovalError::Infrastructure(msg) => write!(f, "Infrastructure error: {}", msg),
            ApprovalError::Failed(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ApprovalError {}

impl From<ApprovalError> for String {
    fn from(err: ApprovalError) -> Self {
        err.to_string()
    }
}

impl From<String> for ApprovalError {
    fn from(s: String) -> Self {
        ApprovalError::Failed(s)
    }
}

impl RetryableError for ApprovalError {
    fn is_retryable(&self) -> bool {
        matches!(self, ApprovalError::Infrastructure(_))
    }
}

#[derive(Debug)]
enum Backend {
    Sqlite,
}

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
struct ApprovalWorkflow {
    name: String,
}

impl ApprovalWorkflow {
    #[flow]
    async fn run(self: Arc<Self>) -> Result<String, ApprovalError> {
        println!("[{}] {} - Starting workflow", format_time(), self.name);

        // Step 1: Initial processing
        self.clone().initial_processing().await?;

        // Step 2: Wait for approval signal
        self.clone().wait_for_approval().await?;

        // Step 3: Final processing
        self.clone().final_processing().await?;

        println!("[{}] {} - Workflow completed!", format_time(), self.name);
        Ok("Success".to_string())
    }

    #[step]
    async fn initial_processing(self: Arc<Self>) -> Result<(), ApprovalError> {
        println!(
            "[{}] {} - Performing initial processing...",
            format_time(),
            self.name
        );
        schedule_timer(Duration::from_millis(100))
            .await
            .map_err(|e| ApprovalError::Infrastructure(e.to_string()))?;
        println!(
            "[{}] {} - Initial processing complete",
            format_time(),
            self.name
        );
        Ok(())
    }

    #[step]
    async fn wait_for_approval(self: Arc<Self>) -> Result<(), ApprovalError> {
        println!(
            "[{}] {} - Waiting for approval signal...",
            format_time(),
            self.name
        );

        // This will suspend the flow until signal is received
        let approval: String = await_external_signal("approval")
            .await
            .map_err(|e| ApprovalError::Infrastructure(e.to_string()))?;

        // Check if it's a cancellation
        if approval.starts_with("CANCEL:") {
            let reason = approval.strip_prefix("CANCEL:").unwrap_or("cancelled");
            println!("[{}] {} - CANCELLED: {}", format_time(), self.name, reason);
            return Err(ApprovalError::Cancelled(reason.to_string()));
        }

        println!(
            "[{}] {} - Approval received: {}",
            format_time(),
            self.name,
            approval
        );
        Ok(())
    }

    #[step]
    async fn final_processing(self: Arc<Self>) -> Result<(), ApprovalError> {
        println!(
            "[{}] {} - Performing final processing...",
            format_time(),
            self.name
        );
        schedule_timer(Duration::from_millis(100))
            .await
            .map_err(|e| ApprovalError::Infrastructure(e.to_string()))?;
        println!(
            "[{}] {} - Final processing complete",
            format_time(),
            self.name
        );
        Ok(())
    }
}

fn format_time() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <backend> <command> [args...]", args[0]);
        eprintln!("  backend: sqlite");
        eprintln!("  command: start | schedule <name> | list | resume | cancel | stop");
        std::process::exit(1);
    }

    let _backend = match args[1].as_str() {
        "sqlite" => Backend::Sqlite,
        _ => {
            eprintln!("Unknown backend: {}", args[1]);
            eprintln!("Supported: sqlite");
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

    let storage = Arc::new(SqliteExecutionLog::new("signal_demo.db").await?);

    match command {
        Command::Start => {
            println!("╔══════════════════════════════════════════════════╗");
            println!("║    Ergon Worker - Signal Example (SQLite)       ║");
            println!("╚══════════════════════════════════════════════════╝");
            println!();
            println!("Worker starting...");

            let worker = Worker::new(storage.clone(), "signal-demo-worker")
                .with_timers()
                .with_timer_interval(Duration::from_millis(100))
                .with_poll_interval(Duration::from_millis(500));

            worker
                .register(|flow: Arc<ApprovalWorkflow>| flow.run())
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

            // Keep worker running
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
            let flow = ApprovalWorkflow { name: name.clone() };
            let flow_id = uuid::Uuid::new_v4();
            let task_id = scheduler.schedule(flow, flow_id).await?;

            println!("✓ Flow scheduled: {}", name);
            println!("  Flow ID: {}", flow_id);
            println!("  Task ID: {}", task_id);
            println!();
            println!("Watch Terminal 1 - the worker will process this flow");
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

            // Send signal to each waiting flow
            let approval_bytes = serde_json::to_vec(&message)?;
            let mut resumed_count = 0;

            for signal in &waiting_signals {
                storage
                    .store_signal_params(signal.flow_id, signal.step, &approval_bytes)
                    .await?;

                match storage.resume_flow(signal.flow_id).await? {
                    true => {
                        println!("  ✓ Resumed flow {} (step {})", signal.flow_id, signal.step);
                        resumed_count += 1;
                    }
                    false => {
                        println!(
                            "  ⏳ Stored signal for flow {} (will resume when suspended)",
                            signal.flow_id
                        );
                        resumed_count += 1;
                    }
                }
            }

            println!();
            println!("✓ Sent approval to {} flow(s)", resumed_count);
            println!("  Message: {}", message);
            println!();
            println!("Watch Terminal 1 - flows will continue!");
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

            // Send cancellation signal (prefixed with CANCEL:)
            let cancel_message = format!("CANCEL:{}", reason);
            let cancel_bytes = serde_json::to_vec(&cancel_message)?;
            let mut cancelled_count = 0;

            for signal in &waiting_signals {
                storage
                    .store_signal_params(signal.flow_id, signal.step, &cancel_bytes)
                    .await?;

                match storage.resume_flow(signal.flow_id).await? {
                    true => {
                        println!(
                            "  ✓ Cancelled flow {} (step {})",
                            signal.flow_id, signal.step
                        );
                        cancelled_count += 1;
                    }
                    false => {
                        println!(
                            "  ⏳ Stored cancel signal for flow {} (will cancel when suspended)",
                            signal.flow_id
                        );
                        cancelled_count += 1;
                    }
                }
            }

            println!();
            println!("✓ Sent cancellation to {} flow(s)", cancelled_count);
            println!("  Reason: {}", reason);
            println!();
            println!("Watch Terminal 1 - flows will fail!");
        }

        Command::Stop => {
            println!("Stopping worker...");
            println!("(Use Ctrl+C on the worker process to stop it)");
            std::process::exit(0);
        }
    }

    Ok(())
}
