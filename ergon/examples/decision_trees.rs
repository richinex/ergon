//! Dynamic Workflow with Decision Tree
//!
//! This example demonstrates:
//! - Dynamic Step Selection
//! - Branching Logic Based on Results
//! - Durable Workflow Guarantees
//!
//! Run with:
//! cargo run --example dynamic_workflow_decision_tree

use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

// Signal completion to test
static DONE_NOTIFIER: std::sync::LazyLock<Arc<Notify>> =
    std::sync::LazyLock::new(|| Arc::new(Notify::new()));

// =============================================================================
// DOMAIN LOGIC
// =============================================================================

/// The state of the flow after a decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum LoanApplicationState {
    Initial,
    CreditScoreChecked {
        score: u32,
    },
    ManualReviewStarted {
        score: u32,
        documents_requested: bool,
    },
    Approved,
    Rejected,
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct CreditApplication {
    application_id: String,
    credit_score: u32,
}

impl CreditApplication {
    #[step]
    async fn check_credit_score(self: Arc<Self>) -> Result<LoanApplicationState, String> {
        println!(
            "   [STEP] Checking Credit Score for {}",
            self.application_id
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(LoanApplicationState::CreditScoreChecked {
            score: self.credit_score,
        })
    }

    // This would be in an external system in real life.
    fn should_do_manual_review(score: u32) -> bool {
        score < 600
    }

    #[step(
        depends_on = "check_credit_score",
        inputs(state = "check_credit_score")
    )]
    async fn decide_workflow(
        self: Arc<Self>,
        state: LoanApplicationState,
    ) -> Result<LoanApplicationState, String> {
        // This step controls the branch
        println!("   [STEP] Deciding workflow based on credit score...");
        if let LoanApplicationState::CreditScoreChecked { score } = state {
            if Self::should_do_manual_review(score) {
                println!("   [DECISION] Manual Review Required");
                Ok(LoanApplicationState::ManualReviewStarted {
                    score,
                    documents_requested: false,
                })
            } else {
                println!("   [DECISION] Fast Track Approval");
                Ok(LoanApplicationState::Approved)
            }
        } else {
            Err("Invalid state for decision".to_string()) // Should never happen
        }
    }

    #[step(depends_on = "decide_workflow", inputs(state = "decide_workflow"))]
    async fn request_documents(
        self: Arc<Self>,
        state: LoanApplicationState,
    ) -> Result<LoanApplicationState, String> {
        if let LoanApplicationState::ManualReviewStarted { score, .. } = state {
            println!("   [STEP] Requesting documents for {}", self.application_id);
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(LoanApplicationState::ManualReviewStarted {
                score,
                documents_requested: true,
            })
        } else {
            // Skip this step if not required
            println!("   [SKIP] No document request needed.");
            Ok(state)
        }
    }

    #[step(depends_on = "request_documents", inputs(state = "request_documents"))]
    async fn approve_or_reject(
        self: Arc<Self>,
        state: LoanApplicationState,
    ) -> Result<LoanApplicationState, String> {
        println!("   [STEP] Final Decision");
        tokio::time::sleep(Duration::from_millis(50)).await;

        match state {
            LoanApplicationState::ManualReviewStarted {
                score,
                documents_requested,
            } => {
                if documents_requested {
                    println!("  [FINAL] Approved after manual review (score: {})!", score);
                    Ok(LoanApplicationState::Approved)
                } else {
                    println!("   [FINAL] Rejecting due to missing documents");
                    Ok(LoanApplicationState::Rejected)
                }
            }
            LoanApplicationState::Approved => {
                println!("   [FINAL] Approved via Fast Track");
                Ok(LoanApplicationState::Approved)
            }
            _ => {
                println!("   [FINAL] Rejecting due to low credit score");
                Ok(LoanApplicationState::Rejected)
            }
        }
    }

    #[flow]
    async fn run_application(self: Arc<Self>) -> Result<LoanApplicationState, String> {
        let mut state = self.clone().check_credit_score().await?;

        state = self.clone().decide_workflow(state).await?;

        // Conditional Step: Only run `request_documents` if a manual review is required
        if let LoanApplicationState::ManualReviewStarted { .. } = state {
            state = self.clone().request_documents(state).await?;
        }

        // Final approval or rejection.
        state = self.clone().approve_or_reject(state).await?;

        DONE_NOTIFIER.notify_one();
        Ok(state)
    }
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(ergon::storage::InMemoryExecutionLog::default());
    let scheduler = Scheduler::new(storage.clone()).unversioned();

    // --- Test 1: Good Credit ---
    let good_app = CreditApplication {
        application_id: "APP-001-GOOD".to_string(),
        credit_score: 700,
    };
    scheduler.schedule(good_app).await?;

    // --- Test 2: Bad Credit ---
    let bad_app = CreditApplication {
        application_id: "APP-002-BAD".to_string(),
        credit_score: 500,
    };
    scheduler.schedule(bad_app).await?;

    // --- Test 3: Borderline
    let border_app = CreditApplication {
        application_id: "APP-003-BORDER".to_string(),
        credit_score: 600,
    };
    scheduler.schedule(border_app).await?;

    let worker = Worker::new(storage, "loan-worker").with_poll_interval(Duration::from_millis(50));

    worker
        .register(|f: Arc<CreditApplication>| f.run_application())
        .await;
    let handle = worker.start().await;

    // Wait for completion (all three must finish)
    DONE_NOTIFIER.notified().await;

    handle.shutdown().await;
    Ok(())
}
