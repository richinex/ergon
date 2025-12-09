//! Graph - Dependency graph for durable execution steps
//!
//! Following Dave Cheney's principle "The name of an identifier includes its package name,"
//! we use `Graph` instead of `FlowGraph` since the `ergon::` namespace already indicates
//! this is flow-related.
//!
//! This module provides the core data structures for representing step
//! dependencies as a directed acyclic graph (DAG).
//!
//! # Design
//!
//! The graph uses a bidirectional adjacency list representation:
//! - `successors`: steps that depend on this step (outgoing edges)
//! - `predecessors`: steps this step depends on (incoming edges)
//!
//! This allows O(1) access to both dependencies and dependents, which is
//! needed for efficient topological sort and ready_steps computation.
//!
//! # Algorithm Reference
//!
//! Based on Exercise 8.3.6 (SchedulingDAG) from Data Structures Using C,
//! which maintains both outgoing and incoming edge lists.

use super::error::{GraphError, GraphResult};
use super::StepId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

/// A node in the flow graph representing a step
///
/// This represents pure graph structure (topology) without execution state.
/// Execution state (which steps are running/completed) should be tracked
/// separately by the executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepNode {
    /// Unique identifier for this step
    id: StepId,
    /// Human-readable description (optional)
    description: Option<String>,
    /// Steps that must complete before this step can run (incoming edges)
    predecessors: Vec<StepId>,
    /// Steps that depend on this step (outgoing edges)
    successors: Vec<StepId>,
}

impl StepNode {
    /// Creates a new step node with no dependencies
    pub fn new(id: StepId) -> Self {
        Self {
            id,
            description: None,
            predecessors: Vec::new(),
            successors: Vec::new(),
        }
    }

    /// Creates a step node with a description
    pub fn with_description(id: StepId, description: impl Into<String>) -> Self {
        Self {
            id,
            description: Some(description.into()),
            predecessors: Vec::new(),
            successors: Vec::new(),
        }
    }

    /// Returns the step ID
    pub fn id(&self) -> &StepId {
        &self.id
    }

    /// Returns the description if present
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the predecessors (dependencies)
    pub fn predecessors(&self) -> &[StepId] {
        &self.predecessors
    }

    /// Returns the successors (dependents)
    pub fn successors(&self) -> &[StepId] {
        &self.successors
    }

    /// Returns the in-degree (number of dependencies)
    pub fn in_degree(&self) -> usize {
        self.predecessors.len()
    }

    /// Returns the out-degree (number of dependents)
    pub fn out_degree(&self) -> usize {
        self.successors.len()
    }
}

/// A directed acyclic graph representing step dependencies in a flow
///
/// The Graph maintains:
/// - All steps in the flow
/// - Dependencies between steps (edges)
/// - Bidirectional access to predecessors and successors
///
/// # Example
///
/// ```
/// use ergon::{Graph, StepId};
///
/// let mut graph = Graph::new();
///
/// // Add steps
/// graph.add_step(StepId::new("get_customer")).unwrap();
/// graph.add_step(StepId::new("validate_payment")).unwrap();
/// graph.add_step(StepId::new("process_order")).unwrap();
///
/// // Add dependencies: validate_payment depends on get_customer
/// graph.add_dependency(
///     StepId::new("validate_payment"),
///     StepId::new("get_customer")
/// ).unwrap();
///
/// // process_order depends on validate_payment
/// graph.add_dependency(
///     StepId::new("process_order"),
///     StepId::new("validate_payment")
/// ).unwrap();
///
/// // Get valid execution order
/// let order = graph.topological_sort().unwrap();
/// assert_eq!(order.len(), 3);
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Graph {
    /// Map from step ID to step node
    nodes: HashMap<StepId, StepNode>,
    /// Insertion order for deterministic iteration
    insertion_order: Vec<StepId>,
}

impl Graph {
    /// Creates a new empty flow graph
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            insertion_order: Vec::new(),
        }
    }

    /// Returns the number of steps in the graph
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns true if the graph has no steps
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Adds a step to the graph
    ///
    /// Returns an error if a step with the same ID already exists.
    pub fn add_step(&mut self, id: StepId) -> GraphResult<()> {
        if self.nodes.contains_key(&id) {
            return Err(GraphError::duplicate_step(id));
        }

        self.insertion_order.push(id.clone());
        self.nodes.insert(id.clone(), StepNode::new(id));
        Ok(())
    }

    /// Adds a step with a description
    pub fn add_step_with_description(
        &mut self,
        id: StepId,
        description: impl Into<String>,
    ) -> GraphResult<()> {
        if self.nodes.contains_key(&id) {
            return Err(GraphError::duplicate_step(id));
        }

        self.insertion_order.push(id.clone());
        self.nodes
            .insert(id.clone(), StepNode::with_description(id, description));
        Ok(())
    }

    /// Adds a dependency: `step` depends on `dependency`
    ///
    /// This means `dependency` must complete before `step` can run.
    ///
    /// Returns an error if:
    /// - Either step doesn't exist
    /// - Adding this dependency would create a cycle
    /// - Step depends on itself
    pub fn add_dependency(&mut self, step: StepId, dependency: StepId) -> GraphResult<()> {
        // Check for self-dependency
        if step == dependency {
            return Err(GraphError::self_dependency(step));
        }

        // Check that both steps exist
        if !self.nodes.contains_key(&step) {
            return Err(GraphError::step_not_found(step));
        }
        if !self.nodes.contains_key(&dependency) {
            return Err(GraphError::dependency_not_found(step, dependency));
        }

        // Check if dependency already exists
        if self.nodes[&step].predecessors.contains(&dependency) {
            return Ok(()); // Idempotent: already exists
        }

        // Add the edge (temporarily)
        // SAFETY: Both keys are guaranteed to exist due to checks at lines 204-209.
        // If these unwraps panic, it's a bug in this function's logic.
        self.nodes
            .get_mut(&step)
            .unwrap()
            .predecessors
            .push(dependency.clone());
        self.nodes
            .get_mut(&dependency)
            .unwrap()
            .successors
            .push(step.clone());

        // Check for cycles
        if self.has_cycle() {
            // Rollback the edge
            // SAFETY: Same as above - keys guaranteed to exist
            self.nodes.get_mut(&step).unwrap().predecessors.pop();
            self.nodes.get_mut(&dependency).unwrap().successors.pop();

            return Err(GraphError::cycle(format!(
                "Adding dependency {} -> {} would create a cycle",
                dependency, step
            )));
        }

        Ok(())
    }

    /// Adds multiple dependencies for a step at once
    pub fn add_dependencies(
        &mut self,
        step: StepId,
        dependencies: impl IntoIterator<Item = StepId>,
    ) -> GraphResult<()> {
        for dep in dependencies {
            self.add_dependency(step.clone(), dep)?;
        }
        Ok(())
    }

    /// Returns a reference to a step node
    pub fn get_step(&self, id: &StepId) -> Option<&StepNode> {
        self.nodes.get(id)
    }

    /// Returns a mutable reference to a step node
    pub fn get_step_mut(&mut self, id: &StepId) -> Option<&mut StepNode> {
        self.nodes.get_mut(id)
    }

    /// Returns true if the step exists in the graph
    pub fn contains_step(&self, id: &StepId) -> bool {
        self.nodes.contains_key(id)
    }

    /// Returns an iterator over all step IDs in insertion order
    pub fn step_ids(&self) -> impl Iterator<Item = &StepId> {
        self.insertion_order.iter()
    }

    /// Returns an iterator over all step nodes
    pub fn steps(&self) -> impl Iterator<Item = &StepNode> {
        self.nodes.values()
    }

    /// Returns steps with no dependencies (in-degree 0)
    ///
    /// These are the steps that can start immediately.
    pub fn root_steps(&self) -> Vec<StepId> {
        self.nodes
            .iter()
            .filter(|(_, node)| node.predecessors.is_empty())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Returns steps with no dependents (out-degree 0)
    ///
    /// These are the terminal steps of the flow.
    pub fn leaf_steps(&self) -> Vec<StepId> {
        self.nodes
            .iter()
            .filter(|(_, node)| node.successors.is_empty())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Returns the in-degree (number of dependencies) for a step
    pub fn in_degree(&self, id: &StepId) -> Option<usize> {
        self.nodes.get(id).map(|n| n.predecessors.len())
    }

    /// Returns the out-degree (number of dependents) for a step
    pub fn out_degree(&self, id: &StepId) -> Option<usize> {
        self.nodes.get(id).map(|n| n.successors.len())
    }

    /// Computes in-degrees for all nodes
    fn compute_in_degrees(&self) -> HashMap<StepId, usize> {
        self.nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.predecessors.len()))
            .collect()
    }

    /// Detects if the graph contains a cycle using DFS
    ///
    /// Uses three-color marking:
    /// - White (not visited): not in any set
    /// - Gray (visiting): in `on_stack`
    /// - Black (visited): in `visited` but not `on_stack`
    pub fn has_cycle(&self) -> bool {
        let mut visited = HashSet::new();
        let mut on_stack = HashSet::new();

        for id in self.nodes.keys() {
            if !visited.contains(id) && self.dfs_has_cycle(id, &mut visited, &mut on_stack) {
                return true;
            }
        }

        false
    }

    fn dfs_has_cycle(
        &self,
        node: &StepId,
        visited: &mut HashSet<StepId>,
        on_stack: &mut HashSet<StepId>,
    ) -> bool {
        visited.insert(node.clone());
        on_stack.insert(node.clone());

        if let Some(step_node) = self.nodes.get(node) {
            for successor in &step_node.successors {
                if !visited.contains(successor) {
                    if self.dfs_has_cycle(successor, visited, on_stack) {
                        return true;
                    }
                } else if on_stack.contains(successor) {
                    // Back edge found - cycle detected
                    return true;
                }
            }
        }

        on_stack.remove(node);
        false
    }

    /// Returns a valid topological ordering of the steps
    ///
    /// Uses Kahn's algorithm (BFS-based) for deterministic ordering.
    ///
    /// Returns an error if the graph contains a cycle.
    ///
    /// # Algorithm (from Exercise 8.3.7)
    ///
    /// 1. Compute in-degree for all nodes
    /// 2. Add all nodes with in-degree 0 to queue
    /// 3. While queue is not empty:
    ///    a. Remove node from queue, add to result
    ///    b. For each successor, decrement in-degree
    ///    c. If in-degree becomes 0, add to queue
    /// 4. If result.len() != node_count, graph has a cycle
    pub fn topological_sort(&self) -> GraphResult<Vec<StepId>> {
        if self.is_empty() {
            return Ok(Vec::new());
        }

        let mut in_degrees = self.compute_in_degrees();
        let mut queue: VecDeque<StepId> = VecDeque::new();
        let mut result = Vec::with_capacity(self.nodes.len());

        // Add all nodes with in-degree 0 to queue
        // Use insertion order for determinism among equal candidates
        for id in &self.insertion_order {
            if in_degrees.get(id) == Some(&0) {
                queue.push_back(id.clone());
            }
        }

        // Process nodes
        while let Some(node) = queue.pop_front() {
            result.push(node.clone());

            // Decrement in-degrees of successors
            if let Some(step_node) = self.nodes.get(&node) {
                // Sort successors by insertion order for determinism
                let mut successors: Vec<_> = step_node.successors.clone();
                successors.sort_by_key(|s| {
                    self.insertion_order
                        .iter()
                        .position(|x| x == s)
                        .unwrap_or(usize::MAX)
                });

                for successor in successors {
                    if let Some(degree) = in_degrees.get_mut(&successor) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(successor);
                        }
                    }
                }
            }
        }

        // Check for cycle
        if result.len() != self.nodes.len() {
            return Err(GraphError::cycle(
                "Graph contains a cycle - topological sort not possible",
            ));
        }

        Ok(result)
    }

    /// Returns steps that can run now given completed steps
    ///
    /// A step is runnable when all its dependencies (predecessors) have
    /// completed execution.
    ///
    /// # Arguments
    ///
    /// * `completed` - Set of step IDs that have completed execution
    ///
    /// # Returns
    ///
    /// Vector of step IDs that can run (all dependencies satisfied)
    pub fn runnable(&self, completed: &HashSet<StepId>) -> Vec<StepId> {
        let mut ready = Vec::new();

        // Use insertion order for determinism
        for id in &self.insertion_order {
            if completed.contains(id) {
                continue; // Already completed
            }

            if let Some(node) = self.nodes.get(id) {
                // Check if all predecessors are completed
                let all_deps_met = node.predecessors.iter().all(|dep| completed.contains(dep));
                if all_deps_met {
                    ready.push(id.clone());
                }
            }
        }

        ready
    }

    /// Validates the graph structure
    ///
    /// Checks:
    /// - No cycles
    /// - All referenced dependencies exist
    /// - At least one root step exists
    pub fn validate(&self) -> GraphResult<()> {
        if self.is_empty() {
            return Err(GraphError::EmptyGraph);
        }

        // Check for cycles
        if self.has_cycle() {
            return Err(GraphError::cycle("Graph contains a cycle"));
        }

        // Check that all dependencies reference existing steps
        for (id, node) in &self.nodes {
            for dep in &node.predecessors {
                if !self.nodes.contains_key(dep) {
                    return Err(GraphError::dependency_not_found(id.clone(), dep.clone()));
                }
            }
        }

        // Check that at least one root exists
        if self.root_steps().is_empty() {
            return Err(GraphError::validation_failed(
                "Graph has no root steps (all steps have dependencies)",
            ));
        }

        Ok(())
    }

    /// Clears all steps and edges from the graph
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.insertion_order.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_graph() {
        let graph = Graph::new();
        assert!(graph.is_empty());
        assert_eq!(graph.len(), 0);
    }

    #[test]
    fn test_add_step() {
        let mut graph = Graph::new();
        graph.add_step(StepId::new("step_a")).unwrap();
        graph.add_step(StepId::new("step_b")).unwrap();

        assert_eq!(graph.len(), 2);
        assert!(graph.contains_step(&StepId::new("step_a")));
        assert!(graph.contains_step(&StepId::new("step_b")));
    }

    #[test]
    fn test_duplicate_step_error() {
        let mut graph = Graph::new();
        graph.add_step(StepId::new("step_a")).unwrap();

        let result = graph.add_step(StepId::new("step_a"));
        assert!(matches!(result, Err(GraphError::DuplicateStep { .. })));
    }

    #[test]
    fn test_add_dependency() {
        let mut graph = Graph::new();
        graph.add_step(StepId::new("step_a")).unwrap();
        graph.add_step(StepId::new("step_b")).unwrap();

        // step_b depends on step_a
        graph
            .add_dependency(StepId::new("step_b"), StepId::new("step_a"))
            .unwrap();

        let step_a = graph.get_step(&StepId::new("step_a")).unwrap();
        let step_b = graph.get_step(&StepId::new("step_b")).unwrap();

        assert_eq!(step_a.successors(), &[StepId::new("step_b")]);
        assert_eq!(step_b.predecessors(), &[StepId::new("step_a")]);
    }

    #[test]
    fn test_self_dependency_error() {
        let mut graph = Graph::new();
        graph.add_step(StepId::new("step_a")).unwrap();

        let result = graph.add_dependency(StepId::new("step_a"), StepId::new("step_a"));
        assert!(matches!(result, Err(GraphError::SelfDependency { .. })));
    }

    #[test]
    fn test_cycle_detection() {
        let mut graph = Graph::new();
        graph.add_step(StepId::new("a")).unwrap();
        graph.add_step(StepId::new("b")).unwrap();
        graph.add_step(StepId::new("c")).unwrap();

        graph
            .add_dependency(StepId::new("b"), StepId::new("a"))
            .unwrap();
        graph
            .add_dependency(StepId::new("c"), StepId::new("b"))
            .unwrap();

        // This would create a cycle: a -> b -> c -> a
        let result = graph.add_dependency(StepId::new("a"), StepId::new("c"));
        assert!(matches!(result, Err(GraphError::CycleDetected { .. })));
    }

    #[test]
    fn test_topological_sort_linear() {
        let mut graph = Graph::new();
        graph.add_step(StepId::new("a")).unwrap();
        graph.add_step(StepId::new("b")).unwrap();
        graph.add_step(StepId::new("c")).unwrap();

        graph
            .add_dependency(StepId::new("b"), StepId::new("a"))
            .unwrap();
        graph
            .add_dependency(StepId::new("c"), StepId::new("b"))
            .unwrap();

        let order = graph.topological_sort().unwrap();
        assert_eq!(
            order,
            vec![StepId::new("a"), StepId::new("b"), StepId::new("c")]
        );
    }

    #[test]
    fn test_topological_sort_diamond() {
        let mut graph = Graph::new();
        graph.add_step(StepId::new("a")).unwrap();
        graph.add_step(StepId::new("b")).unwrap();
        graph.add_step(StepId::new("c")).unwrap();
        graph.add_step(StepId::new("d")).unwrap();

        // Diamond: a -> b -> d
        //          a -> c -> d
        graph
            .add_dependency(StepId::new("b"), StepId::new("a"))
            .unwrap();
        graph
            .add_dependency(StepId::new("c"), StepId::new("a"))
            .unwrap();
        graph
            .add_dependency(StepId::new("d"), StepId::new("b"))
            .unwrap();
        graph
            .add_dependency(StepId::new("d"), StepId::new("c"))
            .unwrap();

        let order = graph.topological_sort().unwrap();

        // a must be first, d must be last
        assert_eq!(order[0], StepId::new("a"));
        assert_eq!(order[3], StepId::new("d"));

        // b and c can be in either order
        let middle: HashSet<_> = vec![order[1].clone(), order[2].clone()]
            .into_iter()
            .collect();
        assert!(middle.contains(&StepId::new("b")));
        assert!(middle.contains(&StepId::new("c")));
    }

    #[test]
    fn test_runnable() {
        let mut graph = Graph::new();
        graph.add_step(StepId::new("a")).unwrap();
        graph.add_step(StepId::new("b")).unwrap();
        graph.add_step(StepId::new("c")).unwrap();
        graph.add_step(StepId::new("d")).unwrap();

        graph
            .add_dependency(StepId::new("b"), StepId::new("a"))
            .unwrap();
        graph
            .add_dependency(StepId::new("c"), StepId::new("a"))
            .unwrap();
        graph
            .add_dependency(StepId::new("d"), StepId::new("b"))
            .unwrap();
        graph
            .add_dependency(StepId::new("d"), StepId::new("c"))
            .unwrap();

        // Initially, only 'a' can run
        let completed = HashSet::new();
        let can_run = graph.runnable(&completed);
        assert_eq!(can_run, vec![StepId::new("a")]);

        // After 'a' completes, b and c can run
        let mut completed = HashSet::new();
        completed.insert(StepId::new("a"));
        let can_run = graph.runnable(&completed);
        assert!(can_run.contains(&StepId::new("b")));
        assert!(can_run.contains(&StepId::new("c")));
        assert!(!can_run.contains(&StepId::new("d")));

        // After b and c complete, d can run
        completed.insert(StepId::new("b"));
        completed.insert(StepId::new("c"));
        let can_run = graph.runnable(&completed);
        assert_eq!(can_run, vec![StepId::new("d")]);
    }

    #[test]
    fn test_root_and_leaf_steps() {
        let mut graph = Graph::new();
        graph.add_step(StepId::new("a")).unwrap();
        graph.add_step(StepId::new("b")).unwrap();
        graph.add_step(StepId::new("c")).unwrap();

        graph
            .add_dependency(StepId::new("b"), StepId::new("a"))
            .unwrap();
        graph
            .add_dependency(StepId::new("c"), StepId::new("b"))
            .unwrap();

        let roots = graph.root_steps();
        assert_eq!(roots, vec![StepId::new("a")]);

        let leaves = graph.leaf_steps();
        assert_eq!(leaves, vec![StepId::new("c")]);
    }

    #[test]
    fn test_validation() {
        let mut graph = Graph::new();
        graph.add_step(StepId::new("a")).unwrap();
        graph.add_step(StepId::new("b")).unwrap();
        graph
            .add_dependency(StepId::new("b"), StepId::new("a"))
            .unwrap();

        assert!(graph.validate().is_ok());
    }

    #[test]
    fn test_validation_empty_graph() {
        let graph = Graph::new();
        assert!(matches!(graph.validate(), Err(GraphError::EmptyGraph)));
    }
}
