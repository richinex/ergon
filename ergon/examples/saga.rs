//! Saga Pattern (Compensating Transactions)
//!
//! Demonstrates how to implement "Backward Recovery".
//! If a later step fails, we manually trigger "Undo" steps for
//! previous successes.
//!
//! Run with:
//! cargo run --example saga_compensation

use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::Notify;

// Track completions to exit cleanly
static COMPLETED_COUNT: AtomicU32 = AtomicU32::new(0);
static DONE_NOTIFIER: LazyLock<Arc<Notify>> = LazyLock::new(|| Arc::new(Notify::new()));

// =============================================================================
// DOMAIN LOGIC
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct HolidaySaga {
    destination: String, // "Paris" = Success, "Atlantis" = Car Failure
}

impl HolidaySaga {
    // --- FORWARD STEPS ---

    #[step]
    async fn book_flight(self: Arc<Self>) -> Result<String, String> {
        println!("   [1] ✈️  Booking flight to {}...", self.destination);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(format!("FLIGHT-{}", self.destination.to_uppercase()))
    }

    #[step]
    async fn book_hotel(self: Arc<Self>) -> Result<String, String> {
        println!("   [2] 🏨 Booking hotel in {}...", self.destination);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(format!("HOTEL-{}", self.destination.to_uppercase()))
    }

    #[step]
    async fn book_car(self: Arc<Self>) -> Result<String, String> {
        println!("   [3] 🚗 Attempting to book car...");
        tokio::time::sleep(Duration::from_millis(50)).await;

        if self.destination == "Atlantis" {
            println!("      ❌ Error: No cars available in Atlantis!");
            return Err("InventoryExhausted".to_string());
        }

        Ok(format!("CAR-{}", self.destination.to_uppercase()))
    }

    // --- COMPENSATING STEPS (UNDO) ---

    #[step]
    async fn cancel_hotel(self: Arc<Self>, hotel_id: String) -> Result<(), String> {
        println!("   [↩️] 🏨 Cancelling Hotel Reservation: {}", hotel_id);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    #[step]
    async fn cancel_flight(self: Arc<Self>, flight_id: String) -> Result<(), String> {
        println!("   [↩️] ✈️  Cancelling Flight Reservation: {}", flight_id);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    // --- ORCHESTRATOR ---

    /// Helper to increment counter when flow ends (Success or Failure)
    fn mark_complete() {
        let count = COMPLETED_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= 2 {
            // We expect 2 flows (Paris + Atlantis)
            DONE_NOTIFIER.notify_one();
        }
    }

    #[flow]
    async fn run_saga(self: Arc<Self>) -> Result<String, String> {
        println!("\n🚀 Starting Holiday Saga for: {}", self.destination);

        let result = self.clone().execute_logic().await;

        // Signal completion regardless of success/failure
        Self::mark_complete();
        result
    }

    // Extracted logic to make the run_saga wrapper cleaner
    async fn execute_logic(self: Arc<Self>) -> Result<String, String> {
        // 1. Book Flight
        let flight_id = self.clone().book_flight().await?;

        // 2. Book Hotel
        let hotel_id = match self.clone().book_hotel().await {
            Ok(id) => id,
            Err(e) => {
                println!("   ⚠️ Hotel failed. Compensating Flight...");
                self.clone().cancel_flight(flight_id).await?;
                return Err(format!("Hotel failed: {}", e));
            }
        };

        // 3. Book Car (Trip Wire)
        let car_id = match self.clone().book_car().await {
            Ok(id) => id,
            Err(e) => {
                println!("   ⚠️ Car failed. Initiating Rollback Sequence...");
                // Compensation Logic: Reverse Order
                self.clone().cancel_hotel(hotel_id).await?;
                self.clone().cancel_flight(flight_id).await?;
                return Err(format!("Saga Failed (Rolled Back): {}", e));
            }
        };

        let msg = format!("CONFIRMED: {} / {} / {}", flight_id, hotel_id, car_id);
        println!("   ✅ {}", msg);
        Ok(msg)
    }
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(ergon::storage::InMemoryExecutionLog::default());
    let scheduler = Scheduler::new(storage.clone());

    // Scenario 1: Success (Paris)
    let saga_success = HolidaySaga {
        destination: "Paris".to_string(),
    };
    scheduler
        .schedule(saga_success, uuid::Uuid::new_v4())
        .await?;

    // Scenario 2: Failure + Compensation (Atlantis)
    let saga_fail = HolidaySaga {
        destination: "Atlantis".to_string(),
    };
    scheduler.schedule(saga_fail, uuid::Uuid::new_v4()).await?;

    let worker = Worker::new(storage, "saga-worker").with_poll_interval(Duration::from_millis(100));

    worker.register(|f: Arc<HolidaySaga>| f.run_saga()).await;
    let handle = worker.start().await;

    // Wait until both flows finish (Success + Failure)
    DONE_NOTIFIER.notified().await;

    handle.shutdown().await;
    println!("\n✨ All sagas completed.");
    Ok(())
}

// This output is textbook perfect. It demonstrates exactly how a Saga works in a concurrent environment.

// Here is the play-by-play analysis of your logs:

// 1. Concurrency (Interleaved Execution)

// Notice how the logs for Paris and Atlantis are mixed together. You have one worker, but it is advancing both workflows simultaneously.

// code
// Text
// download
// content_copy
// expand_less
// [1] ✈️  Booking flight to Paris...
// [1] ✈️  Booking flight to Atlantis...
// [2] 🏨 Booking hotel in Atlantis...
// [2] 🏨 Booking hotel in Paris...
// 2. The Decision Point (The Fork)

// Both flows reach Step 3 (Car Booking).

// Paris: Passes the check.

// Atlantis: Hits the logic trap we set (if destination == "Atlantis").

// code
// Text
// download
// content_copy
// expand_less
// [3] 🚗 Attempting to book car...
// ❌ Error: No cars available in Atlantis!
// ⚠️ Car failed. Initiating Rollback Sequence...
// 3. The Happy Path Completion

// While Atlantis is starting to panic/rollback, Paris crosses the finish line.

// code
// Text
// download
// content_copy
// expand_less
// ✅ CONFIRMED: FLIGHT-PARIS / HOTEL-PARIS / CAR-PARIS
// 4. The Backward Recovery (The Saga)

// This is the money shot. The Atlantis flow didn't just crash; it entered the Err branch of your match statement and began executing the Compensating Transactions in reverse order.

// Undo Hotel:

// code
// Text
// download
// content_copy
// expand_less
// [↩️] 🏨 Cancelling Hotel Reservation: HOTEL-ATLANTIS

// Undo Flight:

// code
// Text
// download
// content_copy
// expand_less
// [↩️] ✈️  Cancelling Flight Reservation: FLIGHT-ATLANTIS
// Why this is a "Superpower"

// In standard Rust, you might write:

// code
// Rust
// download
// content_copy
// expand_less
// if let Err(_) = book_car() {
//     cancel_hotel().await; // <--- If the server crashes HERE...
//     cancel_flight().await;
// }

// If the server crashes exactly where I marked above:

// Standard Rust: The process dies. cancel_flight never runs. You have now paid for a flight to Atlantis that you aren't taking. (Financial Loss).

// Ergon: The crash happens. The process restarts. Ergon looks at the DB, sees that run_saga was executing cancel_hotel. It sees that step finished (idempotency required). It resumes execution at the next line: cancel_flight. (Consistency Guaranteed).

// You have successfully implemented the Saga Pattern with durable guarantees.
