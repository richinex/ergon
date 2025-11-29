//! Error types for graph operations
//!
//! This module hides error representation details and provides
//! a unified error type for all graph operations.

use crate::StepId;
use thiserror::Error;

/// Result type for graph operations
pub type GraphResult<T> = Result<T, GraphError>;

/// Errors that can occur during graph operations
#[derive(Debug, Error, Clone)]
#[non_exhaustive]
pub enum GraphError {
    /// A cycle was detected in the dependency graph
    #[error("Cycle detected in dependency graph: {path}")]
    CycleDetected {
        /// Human-readable description of the cycle path
        path: String,
    },

    /// A step was not found in the graph
    #[error("Step not found: {step_id}")]
    StepNotFound {
        /// The step ID that was not found
        step_id: StepId,
    },

    /// A dependency references a non-existent step
    #[error("Dependency '{dependency}' for step '{step}' does not exist")]
    DependencyNotFound {
        /// The step that declared the dependency
        step: StepId,
        /// The dependency that was not found
        dependency: StepId,
    },

    /// A step was added with a duplicate ID
    #[error("Duplicate step ID: {step_id}")]
    DuplicateStep {
        /// The duplicate step ID
        step_id: StepId,
    },

    /// Self-dependency detected (step depends on itself)
    #[error("Step '{step_id}' cannot depend on itself")]
    SelfDependency {
        /// The step with self-dependency
        step_id: StepId,
    },

    /// Graph is empty (no steps defined)
    #[error("Graph is empty - no steps defined")]
    EmptyGraph,

    /// Attempted operation on an incomplete graph
    #[error("Graph validation failed: {reason}")]
    ValidationFailed {
        /// Reason for validation failure
        reason: String,
    },
}

impl GraphError {
    /// Creates a cycle detected error with the given path
    pub fn cycle(path: impl Into<String>) -> Self {
        Self::CycleDetected { path: path.into() }
    }

    /// Creates a step not found error
    pub fn step_not_found(step_id: StepId) -> Self {
        Self::StepNotFound { step_id }
    }

    /// Creates a dependency not found error
    pub fn dependency_not_found(step: StepId, dependency: StepId) -> Self {
        Self::DependencyNotFound { step, dependency }
    }

    /// Creates a duplicate step error
    pub fn duplicate_step(step_id: StepId) -> Self {
        Self::DuplicateStep { step_id }
    }

    /// Creates a self-dependency error
    pub fn self_dependency(step_id: StepId) -> Self {
        Self::SelfDependency { step_id }
    }

    /// Creates a validation failed error
    pub fn validation_failed(reason: impl Into<String>) -> Self {
        Self::ValidationFailed {
            reason: reason.into(),
        }
    }
}
