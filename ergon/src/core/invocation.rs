use super::error::{CoreError, Result};
use super::retry::RetryPolicy;
use super::serialization::deserialize_value;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvocationStatus {
    Pending,
    WaitingForSignal,
    WaitingForTimer,
    Complete,
}

impl InvocationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            InvocationStatus::Pending => "PENDING",
            InvocationStatus::WaitingForSignal => "WAITING_FOR_SIGNAL",
            InvocationStatus::WaitingForTimer => "WAITING_FOR_TIMER",
            InvocationStatus::Complete => "COMPLETE",
        }
    }
}

impl FromStr for InvocationStatus {
    type Err = CoreError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "PENDING" => Ok(InvocationStatus::Pending),
            "WAITING_FOR_SIGNAL" => Ok(InvocationStatus::WaitingForSignal),
            "WAITING_FOR_TIMER" => Ok(InvocationStatus::WaitingForTimer),
            "COMPLETE" => Ok(InvocationStatus::Complete),
            _ => Err(CoreError::InvalidStatus(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallType {
    Run,
    Resume,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invocation {
    id: Uuid,
    step: i32,
    timestamp: DateTime<Utc>,
    class_name: String,
    method_name: String,
    status: InvocationStatus,
    attempts: i32,
    parameters: Vec<u8>,
    params_hash: u64,
    return_value: Option<Vec<u8>>,
    delay: Option<i64>,
    /// Retry policy for this invocation (None means no retries)
    #[serde(default)]
    retry_policy: Option<RetryPolicy>,
    /// Whether the cached error is retryable (None = not an error, true = retryable, false = permanent)
    #[serde(default)]
    is_retryable: Option<bool>,
    /// When the timer should fire (for WAITING_FOR_TIMER status)
    #[serde(default)]
    timer_fire_at: Option<DateTime<Utc>>,
    /// Optional timer name for debugging
    #[serde(default)]
    timer_name: Option<String>,
}

impl Invocation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Uuid,
        step: i32,
        timestamp: DateTime<Utc>,
        class_name: String,
        method_name: String,
        status: InvocationStatus,
        attempts: i32,
        parameters: Vec<u8>,
        params_hash: u64,
        return_value: Option<Vec<u8>>,
        delay: Option<i64>,
        retry_policy: Option<RetryPolicy>,
        is_retryable: Option<bool>,
    ) -> Self {
        Self {
            id,
            step,
            timestamp,
            class_name,
            method_name,
            status,
            attempts,
            parameters,
            params_hash,
            return_value,
            delay,
            retry_policy,
            is_retryable,
            timer_fire_at: None,
            timer_name: None,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn step(&self) -> i32 {
        self.step
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn class_name(&self) -> &str {
        &self.class_name
    }

    pub fn method_name(&self) -> &str {
        &self.method_name
    }

    pub fn status(&self) -> InvocationStatus {
        self.status
    }

    pub fn attempts(&self) -> i32 {
        self.attempts
    }

    pub fn parameters(&self) -> &[u8] {
        &self.parameters
    }

    pub fn params_hash(&self) -> u64 {
        self.params_hash
    }

    pub fn return_value(&self) -> Option<&[u8]> {
        self.return_value.as_deref()
    }

    pub fn delay(&self) -> Option<Duration> {
        self.delay.map(|ms| Duration::from_millis(ms as u64))
    }

    pub fn retry_policy(&self) -> Option<RetryPolicy> {
        self.retry_policy
    }

    pub fn is_flow(&self) -> bool {
        self.step == 0
    }

    pub fn is_retryable(&self) -> Option<bool> {
        self.is_retryable
    }

    pub fn set_status(&mut self, status: InvocationStatus) {
        self.status = status;
    }

    pub fn set_return_value(&mut self, return_value: Vec<u8>) {
        self.return_value = Some(return_value);
    }

    pub fn set_is_retryable(&mut self, is_retryable: Option<bool>) {
        self.is_retryable = is_retryable;
    }

    pub fn increment_attempts(&mut self) {
        self.attempts += 1;
    }

    pub fn deserialize_parameters<T: DeserializeOwned>(&self) -> Result<T> {
        deserialize_value(&self.parameters)
    }

    pub fn deserialize_return_value<T: DeserializeOwned>(&self) -> Result<Option<T>> {
        match &self.return_value {
            Some(bytes) => deserialize_value(bytes).map(Some),
            None => Ok(None),
        }
    }

    pub fn timer_fire_at(&self) -> Option<DateTime<Utc>> {
        self.timer_fire_at
    }

    pub fn timer_name(&self) -> Option<&str> {
        self.timer_name.as_deref()
    }

    pub fn is_timer_expired(&self) -> bool {
        match self.timer_fire_at {
            Some(ft) => Utc::now() >= ft,
            None => false,
        }
    }

    pub fn set_timer_fire_at(&mut self, fire_at: Option<DateTime<Utc>>) {
        self.timer_fire_at = fire_at;
    }

    pub fn set_timer_name(&mut self, name: Option<String>) {
        self.timer_name = name;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invocation_status_conversion() {
        assert_eq!(InvocationStatus::Pending.as_str(), "PENDING");
        assert_eq!(
            InvocationStatus::WaitingForSignal.as_str(),
            "WAITING_FOR_SIGNAL"
        );
        assert_eq!(
            InvocationStatus::WaitingForTimer.as_str(),
            "WAITING_FOR_TIMER"
        );
        assert_eq!(InvocationStatus::Complete.as_str(), "COMPLETE");

        assert_eq!(
            InvocationStatus::from_str("PENDING").unwrap(),
            InvocationStatus::Pending
        );
        assert_eq!(
            InvocationStatus::from_str("WAITING_FOR_SIGNAL").unwrap(),
            InvocationStatus::WaitingForSignal
        );
        assert_eq!(
            InvocationStatus::from_str("WAITING_FOR_TIMER").unwrap(),
            InvocationStatus::WaitingForTimer
        );
        assert_eq!(
            InvocationStatus::from_str("COMPLETE").unwrap(),
            InvocationStatus::Complete
        );
    }

    #[test]
    fn test_invocation_is_flow() {
        let inv = Invocation::new(
            Uuid::new_v4(),
            0,
            Utc::now(),
            "TestClass".to_string(),
            "testMethod".to_string(),
            InvocationStatus::Pending,
            1,
            vec![],
            0, // params_hash
            None,
            None,
            None, // retry_policy
            None, // is_retryable
        );
        assert!(inv.is_flow());

        let inv2 = Invocation::new(
            Uuid::new_v4(),
            1,
            Utc::now(),
            "TestClass".to_string(),
            "testMethod".to_string(),
            InvocationStatus::Pending,
            1,
            vec![],
            0, // params_hash
            None,
            None,
            None, // retry_policy
            None, // is_retryable
        );
        assert!(!inv2.is_flow());
    }

    #[test]
    fn test_invocation_retry_policy() {
        use super::super::retry::RetryPolicy;

        // Test with no retry policy
        let inv1 = Invocation::new(
            Uuid::new_v4(),
            1,
            Utc::now(),
            "TestClass".to_string(),
            "testMethod".to_string(),
            InvocationStatus::Pending,
            1,
            vec![],
            0,
            None,
            None,
            None,
            None,
        );
        assert_eq!(inv1.retry_policy(), None);

        // Test with retry policy
        let inv2 = Invocation::new(
            Uuid::new_v4(),
            1,
            Utc::now(),
            "TestClass".to_string(),
            "testMethod".to_string(),
            InvocationStatus::Pending,
            1,
            vec![],
            0,
            None,
            None,
            Some(RetryPolicy::STANDARD),
            None,
        );
        assert_eq!(inv2.retry_policy(), Some(RetryPolicy::STANDARD));

        // Test increment_attempts
        let mut inv3 = Invocation::new(
            Uuid::new_v4(),
            1,
            Utc::now(),
            "TestClass".to_string(),
            "testMethod".to_string(),
            InvocationStatus::Pending,
            1,
            vec![],
            0,
            None,
            None,
            Some(RetryPolicy::with_max_attempts(3)),
            None,
        );
        assert_eq!(inv3.attempts(), 1);
        inv3.increment_attempts();
        assert_eq!(inv3.attempts(), 2);
        inv3.increment_attempts();
        assert_eq!(inv3.attempts(), 3);
    }

    #[test]
    fn test_invocation_is_retryable() {
        // Test with retryable error
        let inv1 = Invocation::new(
            Uuid::new_v4(),
            1,
            Utc::now(),
            "TestClass".to_string(),
            "testMethod".to_string(),
            InvocationStatus::Complete,
            1,
            vec![],
            0,
            None,
            None,
            None,
            Some(true), // retryable error
        );
        assert_eq!(inv1.is_retryable(), Some(true));

        // Test with non-retryable error
        let inv2 = Invocation::new(
            Uuid::new_v4(),
            1,
            Utc::now(),
            "TestClass".to_string(),
            "testMethod".to_string(),
            InvocationStatus::Complete,
            1,
            vec![],
            0,
            None,
            None,
            None,
            Some(false), // non-retryable error
        );
        assert_eq!(inv2.is_retryable(), Some(false));

        // Test with no error (Ok result)
        let inv3 = Invocation::new(
            Uuid::new_v4(),
            1,
            Utc::now(),
            "TestClass".to_string(),
            "testMethod".to_string(),
            InvocationStatus::Complete,
            1,
            vec![],
            0,
            None,
            None,
            None,
            None, // not an error
        );
        assert_eq!(inv3.is_retryable(), None);
    }
}
