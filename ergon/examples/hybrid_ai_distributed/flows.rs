//! Flow type definitions for the Hybrid AI example.
//!
//! This module contains the flow types that are shared between the scheduler
//! and worker binaries. In a real application, this would be a separate
//! library crate that both your scheduler and worker depend on.
//!
//! Both binaries import this library, but use different parts:
//! - Scheduler: Creates and schedules ContentFlow instances
//! - Worker: Executes ContentFlow and AuditLogFlow
//!
//! This is normal for a shared library - not all exports are used by all consumers.
//! In a real multi-crate setup, library crates don't warn about unused exports.
#![allow(dead_code)]

use ergon::executor::InvokeChild;
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// DATA TYPES
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ModerationStatus {
    AutoApproved,
    AutoRejected,
    HumanApproved,
    HumanRejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HumanDecision {
    pub approved: bool,
}

// =============================================================================
// CHILD FLOW: Audit Archiver
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = ())]
pub struct AuditLogFlow {
    pub content_id: String,
    pub status: ModerationStatus,
    pub score: u32,
}

impl AuditLogFlow {
    #[flow]
    pub async fn run_audit(self: Arc<Self>) -> Result<(), String> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!(
            "   [AUDIT] Archived decision for {}: {:?} (Score: {})",
            self.content_id, self.status, self.score
        );
        Ok(())
    }
}

// =============================================================================
// PARENT FLOW: Content Pipeline
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
pub struct ContentFlow {
    pub content_id: String,
    pub text: String,
}

impl ContentFlow {
    #[step]
    pub async fn ai_analyze(self: Arc<Self>) -> Result<u32, String> {
        println!("   [AI] Analyzing content: \"{}\"", self.text);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let sum: u32 = self.text.bytes().map(|b| b as u32).sum();
        let score = sum % 100;

        println!("   [AI] Score calculated: {}/100", score);
        Ok(score)
    }

    #[step]
    pub async fn wait_for_human(self: Arc<Self>) -> Result<HumanDecision, String> {
        println!("   [FLOW] Score ambiguous. Suspending for Human Review...");

        let signal_name = format!("review:{}", self.content_id);

        let decision: HumanDecision = ergon::executor::await_external_signal(&signal_name)
            .await
            .map_err(|e| format!("Signal error: {}", e))?;

        Ok(decision)
    }

    #[flow]
    pub async fn process(self: Arc<Self>) -> Result<(), String> {
        println!("Processing upload: {}", self.content_id);

        let score = self.clone().ai_analyze().await?;
        let final_status: ModerationStatus;

        if score < 20 {
            println!("   [FLOW] Safe content. Auto-Approving.");
            final_status = ModerationStatus::AutoApproved;
        } else if score > 80 {
            println!("   [FLOW] Toxic content. Auto-Rejecting.");
            final_status = ModerationStatus::AutoRejected;
        } else {
            let decision = self.clone().wait_for_human().await?;
            if decision.approved {
                final_status = ModerationStatus::HumanApproved;
            } else {
                final_status = ModerationStatus::HumanRejected;
            }
        }

        self.invoke(AuditLogFlow {
            content_id: self.content_id.clone(),
            status: final_status.clone(),
            score,
        })
        .result()
        .await
        .map_err(|e| e.to_string())?;

        Ok(())
    }
}
