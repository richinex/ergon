use super::error::{Error, Result};
use super::serialization::deserialize_value;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvocationStatus {
    Pending,
    WaitingForSignal,
    Complete,
}

impl InvocationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            InvocationStatus::Pending => "PENDING",
            InvocationStatus::WaitingForSignal => "WAITING_FOR_SIGNAL",
            InvocationStatus::Complete => "COMPLETE",
        }
    }
}

impl FromStr for InvocationStatus {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "PENDING" => Ok(InvocationStatus::Pending),
            "WAITING_FOR_SIGNAL" => Ok(InvocationStatus::WaitingForSignal),
            "COMPLETE" => Ok(InvocationStatus::Complete),
            _ => Err(Error::InvalidStatus(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallType {
    Run,
    Await,
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

    pub fn is_flow(&self) -> bool {
        self.step == 0
    }

    pub fn set_status(&mut self, status: InvocationStatus) {
        self.status = status;
    }

    pub fn set_return_value(&mut self, return_value: Vec<u8>) {
        self.return_value = Some(return_value);
    }

    pub fn deserialize_parameters<T: for<'de> Deserialize<'de>>(&self) -> Result<T> {
        deserialize_value(&self.parameters)
    }

    pub fn deserialize_return_value<T: for<'de> Deserialize<'de>>(&self) -> Result<Option<T>> {
        match &self.return_value {
            Some(bytes) => deserialize_value(bytes).map(Some),
            None => Ok(None),
        }
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
        );
        assert!(!inv2.is_flow());
    }
}
