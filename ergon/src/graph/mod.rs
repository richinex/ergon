//! Dependency Graph for Durable Execution Flows
//!
//! This crate provides graph data structures and algorithms for managing step dependencies
//! in durable execution flows. It enables:
//!
//! - Explicit dependency declaration between steps
//! - Topological sorting for valid execution order
//! - Cycle detection to catch invalid dependency graphs
//! - Finding runnable steps (whose dependencies are satisfied)
//!
//! # Design Principles
//!
//! Following Parnas's information hiding principles:
//! - This module hides the graph representation (adjacency list vs matrix)
//! - Exposes only abstract operations: add_step, add_dependency, runnable, etc.
//!
//! # Algorithm References
//!
//! Based on implementations from Data Structures Using C (Tenenbaum et al.):
//! - Kahn's algorithm for topological sort (Exercise 8.3.7)

mod error;
mod flow_graph;
mod step_id;

pub use error::{GraphError, GraphResult};
pub use flow_graph::{FlowGraph, StepNode};
pub use step_id::StepId;
