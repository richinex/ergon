//! Step identifier type
//!
//! This module defines the StepId type which uniquely identifies a step
//! within a flow. The step ID is based on the method name, which allows
//! for human-readable dependency declarations.
//!
//! # Design Decision
//!
//! We use method name as the primary identifier rather than a sequential
//! counter because:
//! 1. It's human-readable in `depends_on = "step_name"` declarations
//! 2. It survives code refactoring (reordering steps in code)
//! 3. It's stable across flow executions for replay

use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

/// Unique identifier for a step within a flow
///
/// The StepId is based on the method name of the step. For steps that may
/// be called multiple times with different parameters, a disambiguator can
/// be added.
///
/// # Examples
///
/// ```
/// use ergon::StepId;
///
/// let step = StepId::new("process_payment");
/// assert_eq!(step.name(), "process_payment");
/// ```
#[derive(Clone, Eq, Serialize, Deserialize)]
pub struct StepId {
    /// The method name of the step
    name: String,
    /// Optional disambiguator for multiple calls to same method
    /// (e.g., hash of parameters or explicit label)
    disambiguator: Option<String>,
}

impl StepId {
    /// Creates a new StepId from a method name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            disambiguator: None,
        }
    }

    /// Creates a StepId with a disambiguator
    ///
    /// Use this when the same step method may be called multiple times
    /// with different parameters.
    pub fn with_disambiguator(name: impl Into<String>, disambiguator: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            disambiguator: Some(disambiguator.into()),
        }
    }

    /// Returns the method name portion of the step ID
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the disambiguator if present
    pub fn disambiguator(&self) -> Option<&str> {
        self.disambiguator.as_deref()
    }

    /// Returns the full string representation of the step ID
    pub fn as_str(&self) -> String {
        match &self.disambiguator {
            Some(d) => format!("{}#{}", self.name, d),
            None => self.name.clone(),
        }
    }
}

impl PartialEq for StepId {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.disambiguator == other.disambiguator
    }
}

impl Hash for StepId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.disambiguator.hash(state);
    }
}

impl fmt::Display for StepId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.disambiguator {
            Some(d) => write!(f, "{}#{}", self.name, d),
            None => write!(f, "{}", self.name),
        }
    }
}

impl fmt::Debug for StepId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StepId({})", self)
    }
}

impl From<&str> for StepId {
    fn from(s: &str) -> Self {
        // Check if string contains disambiguator
        if let Some((name, disambiguator)) = s.split_once('#') {
            Self::with_disambiguator(name, disambiguator)
        } else {
            Self::new(s)
        }
    }
}

impl From<String> for StepId {
    fn from(s: String) -> Self {
        StepId::from(s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_step_id_creation() {
        let step = StepId::new("process_payment");
        assert_eq!(step.name(), "process_payment");
        assert!(step.disambiguator().is_none());
    }

    #[test]
    fn test_step_id_with_disambiguator() {
        let step = StepId::with_disambiguator("process_payment", "order_123");
        assert_eq!(step.name(), "process_payment");
        assert_eq!(step.disambiguator(), Some("order_123"));
        assert_eq!(step.as_str(), "process_payment#order_123");
    }

    #[test]
    fn test_step_id_equality() {
        let step1 = StepId::new("step_a");
        let step2 = StepId::new("step_a");
        let step3 = StepId::new("step_b");

        assert_eq!(step1, step2);
        assert_ne!(step1, step3);
    }

    #[test]
    fn test_step_id_from_string() {
        let step1: StepId = "simple_step".into();
        assert_eq!(step1.name(), "simple_step");
        assert!(step1.disambiguator().is_none());

        let step2: StepId = "step#disambig".into();
        assert_eq!(step2.name(), "step");
        assert_eq!(step2.disambiguator(), Some("disambig"));
    }

    #[test]
    fn test_step_id_display() {
        let step1 = StepId::new("my_step");
        assert_eq!(format!("{}", step1), "my_step");

        let step2 = StepId::with_disambiguator("my_step", "v2");
        assert_eq!(format!("{}", step2), "my_step#v2");
    }

    #[test]
    fn test_step_id_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(StepId::new("step_a"));
        set.insert(StepId::new("step_b"));
        set.insert(StepId::new("step_a")); // duplicate

        assert_eq!(set.len(), 2);
    }
}
