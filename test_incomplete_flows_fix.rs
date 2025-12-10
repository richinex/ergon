// Test to verify get_incomplete_flows() finds scheduled-but-not-started flows

use ergon::prelude::*;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct TestFlow {
    data: String,
}

impl TestFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, String> {
        Ok(format!("Processed: {}", self.data))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing get_incomplete_flows() with scheduled-but-not-started flows\n");

    let storage = Arc::new(SqliteExecutionLog::new("sqlite::memory:").await?);
    let scheduler = Scheduler::new(storage.clone());

    // Schedule 5 flows
    println!("Scheduling 5 flows...");
    for i in 1..=5 {
        let flow = TestFlow {
            data: format!("Flow {}", i),
        };
        scheduler.schedule(flow, Uuid::new_v4()).await?;
        println!("  ✓ Flow {} scheduled", i);
    }

    // DON'T start any workers - flows remain in queue, never execute
    println!("\nNOT starting any workers (flows stay in queue)\n");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Check incomplete flows - should find all 5!
    let incomplete = storage.get_incomplete_flows().await?;

    println!("Results:");
    println!("  Scheduled flows: 5");
    println!("  Incomplete flows found: {}", incomplete.len());

    if incomplete.len() == 5 {
        println!("\n✅ SUCCESS: get_incomplete_flows() correctly found all scheduled flows!");
        println!("\nFlow details:");
        for inv in &incomplete {
            println!("  - {} (step={}, status={:?})",
                inv.class_name(), inv.step(), inv.status());
        }
    } else {
        println!("\n❌ FAILURE: Expected 5 incomplete flows, found {}", incomplete.len());
        return Err("Bug still present!".into());
    }

    storage.close().await?;
    Ok(())
}
