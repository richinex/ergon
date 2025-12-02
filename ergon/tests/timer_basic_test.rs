//! Basic timer functionality test
//!
//! This test verifies that:
//! 1. Timers can be scheduled
//! 2. Timer state is persisted to storage
//! 3. Timer processor can detect and fire expired timers
//! 4. Optimistic concurrency prevents duplicate firing

use chrono::Utc;
use ergon::prelude::*;
use ergon::storage::InMemoryExecutionLog;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn test_timer_storage_and_expiration() {
    // Create in-memory storage
    let storage = Arc::new(InMemoryExecutionLog::new());
    let flow_id = Uuid::new_v4();
    let step = 1;

    // First, create an invocation (timer needs an existing invocation)
    use ergon::core::InvocationStatus;
    use ergon::storage::InvocationStartParams;
    storage
        .log_invocation_start(InvocationStartParams {
            id: flow_id,
            step,
            class_name: "TestFlow",
            method_name: "wait_step",
            delay: None,
            status: InvocationStatus::Pending,
            parameters: &[],
            retry_policy: None,
        })
        .await
        .unwrap();

    // Schedule a timer that expires in 100ms
    let fire_at = Utc::now() + chrono::Duration::milliseconds(100);
    storage
        .log_timer(flow_id, step, fire_at, Some("test-timer"))
        .await
        .unwrap();

    // Timer should not be expired yet
    let expired = storage.get_expired_timers(Utc::now()).await.unwrap();
    assert_eq!(expired.len(), 0, "Timer should not be expired yet");

    // Wait for timer to expire
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Timer should now be expired
    let expired = storage.get_expired_timers(Utc::now()).await.unwrap();
    assert_eq!(expired.len(), 1, "Timer should be expired");
    assert_eq!(expired[0].flow_id, flow_id);
    assert_eq!(expired[0].step, step);
    assert_eq!(expired[0].timer_name, Some("test-timer".to_string()));
}

#[tokio::test]
async fn test_timer_optimistic_concurrency() {
    // This test verifies that only one worker can claim a timer
    let storage = Arc::new(InMemoryExecutionLog::new());
    let flow_id = Uuid::new_v4();
    let step = 1;

    // First, create an invocation
    use ergon::core::InvocationStatus;
    use ergon::storage::InvocationStartParams;
    storage
        .log_invocation_start(InvocationStartParams {
            id: flow_id,
            step,
            class_name: "TestFlow",
            method_name: "wait_step",
            delay: None,
            status: InvocationStatus::Pending,
            parameters: &[],
            retry_policy: None,
        })
        .await
        .unwrap();

    // Schedule a timer that's already expired
    let fire_at = Utc::now() - chrono::Duration::seconds(1);
    storage
        .log_timer(flow_id, step, fire_at, None)
        .await
        .unwrap();

    // Two workers try to claim it
    let claimed1 = storage.claim_timer(flow_id, step).await.unwrap();
    let claimed2 = storage.claim_timer(flow_id, step).await.unwrap();

    // Only ONE should succeed
    assert!(claimed1, "First worker should claim the timer");
    assert!(!claimed2, "Second worker should fail to claim");
}

#[tokio::test]
async fn test_multiple_timers() {
    let storage = Arc::new(InMemoryExecutionLog::new());
    use ergon::core::InvocationStatus;
    use ergon::storage::InvocationStartParams;

    // Schedule 3 timers
    let flow_id1 = Uuid::new_v4();
    let flow_id2 = Uuid::new_v4();
    let flow_id3 = Uuid::new_v4();

    // Create invocations first
    for flow_id in [flow_id1, flow_id2, flow_id3] {
        storage
            .log_invocation_start(InvocationStartParams {
                id: flow_id,
                step: 1,
                class_name: "TestFlow",
                method_name: "wait_step",
                delay: None,
                status: InvocationStatus::Pending,
                parameters: &[],
                retry_policy: None,
            })
            .await
            .unwrap();
    }

    let now = Utc::now();
    storage
        .log_timer(
            flow_id1,
            1,
            now - chrono::Duration::seconds(10),
            Some("timer1"),
        )
        .await
        .unwrap();
    storage
        .log_timer(
            flow_id2,
            1,
            now - chrono::Duration::seconds(5),
            Some("timer2"),
        )
        .await
        .unwrap();
    storage
        .log_timer(
            flow_id3,
            1,
            now + chrono::Duration::seconds(10),
            Some("timer3"),
        )
        .await
        .unwrap();

    // Get expired timers
    let expired = storage.get_expired_timers(now).await.unwrap();

    // Only first two should be expired
    assert_eq!(expired.len(), 2, "Should have 2 expired timers");

    // Verify they're in order (oldest first)
    assert_eq!(expired[0].timer_name, Some("timer1".to_string()));
    assert_eq!(expired[1].timer_name, Some("timer2".to_string()));
}
