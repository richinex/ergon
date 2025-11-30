//! DAG (Directed Acyclic Graph) Parallel Execution Module
//!
//! This module enables automatic parallel execution of independent steps by building
//! a dependency graph at runtime and executing steps in topological order.
//!
//! # Design Decision Hidden
//!
//! Following Parnas's information hiding principle, this module hides:
//! **"How parallel DAG execution works"**
//!
//! The implementation uses deferred handles and dependency tracking, but users
//! only see the high-level DAG flow abstraction.
//!
//! # How It Works
//!
//! 1. Steps return `StepHandle<T>` instead of `T` directly
//! 2. The handle contains step metadata and a factory function
//! 3. When handles are used as inputs to other steps, dependencies are tracked
//! 4. The `DeferredExecutor` builds a dependency graph (DAG)
//! 5. At flow completion, independent steps execute in parallel via tokio::spawn
//! 6. Dependent steps wait for their dependencies to complete first
//!
//! # Example
//!
//! ```ignore
//! #[flow]
//! async fn process_order(self: Arc<Self>) -> OrderResult {
//!     // These three steps are independent - execute in parallel
//!     let user = self.fetch_user();           // StepHandle<User>
//!     let inventory = self.check_inventory(); // StepHandle<Inventory>
//!     let pricing = self.get_pricing();       // StepHandle<Price>
//!
//!     // This step depends on all three above - waits for them
//!     let validated = self.validate_order(user, inventory, pricing);
//!
//!     // Final step depends on validation
//!     self.charge_card(validated)
//! }
//! ```
//!
//! # Parallelism
//!
//! - `fetch_user`, `check_inventory`, `get_pricing` run in parallel (no dependencies)
//! - `validate_order` waits for all three to complete
//! - `charge_card` waits for `validate_order`
//!
//! # Comparison with Sequential Execution
//!
//! - **Sequential flows** (`#[flow]`): Steps execute one at a time in order
//! - **DAG flows** (`#[flow]`): Independent steps execute in parallel
//!
//! Similar to how [Temporal](https://temporal.io) handles activities - they return
//! promises that are resolved by the workflow engine, not immediately.

use super::error::{ExecutionError, Result};
use crate::core::{deserialize_value, serialize_value};
use crate::graph::{FlowGraph, StepId};
use petgraph::dot::{Config, Dot};
use petgraph::graph::DiGraph;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

// Type aliases for complex step factory types
type StepInputs = HashMap<StepId, Vec<u8>>;
type StepOutput = Result<Vec<u8>>;
type StepFuture = Pin<Box<dyn Future<Output = StepOutput> + Send>>;
type StepFactory = Box<dyn FnOnce(StepInputs) -> StepFuture + Send>;

/// A handle representing a deferred step execution.
///
/// The step is not executed immediately - it's registered with the executor
/// and resolved later based on the dependency graph.
pub struct StepHandle<T> {
    /// Unique step identifier
    step_id: StepId,
    /// Dependencies (step IDs this step depends on)
    dependencies: Vec<StepId>,
    /// Receiver for the result (when resolved)
    receiver: oneshot::Receiver<Result<T>>,
    /// Marker for the result type
    _phantom: std::marker::PhantomData<T>,
}

impl<T> StepHandle<T> {
    /// Creates a new step handle
    pub fn new(
        step_id: StepId,
        dependencies: Vec<StepId>,
        receiver: oneshot::Receiver<Result<T>>,
    ) -> Self {
        Self {
            step_id,
            dependencies,
            receiver,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns the step ID
    pub fn step_id(&self) -> &StepId {
        &self.step_id
    }

    /// Returns the dependencies
    pub fn dependencies(&self) -> &[StepId] {
        &self.dependencies
    }

    /// Awaits the result (blocks until the step is resolved)
    pub async fn resolve(self) -> Result<T> {
        self.receiver
            .await
            .map_err(|_| ExecutionError::Failed("Step result channel closed".to_string()))?
    }
}

/// Internal representation of a deferred step for execution
pub struct DeferredStep {
    /// Step identifier
    pub step_id: StepId,
    /// Dependencies
    pub dependencies: Vec<StepId>,
    /// Factory function: takes serialized inputs, returns serialized output
    pub factory: StepFactory,
    /// Sender for the result
    pub result_sender: Box<dyn FnOnce(Result<Vec<u8>>) + Send>,
}

/// Registry for collecting deferred steps during flow execution
pub struct DeferredRegistry {
    /// Registered steps
    steps: Vec<DeferredStep>,
    /// Last registered step (for auto-chaining sequential execution)
    last_step: Option<StepId>,
}

impl DeferredRegistry {
    /// Creates a new registry
    pub fn new() -> Self {
        Self {
            steps: Vec::new(),
            last_step: None,
        }
    }

    /// Registers a deferred step and returns a handle
    ///
    /// By default, steps execute sequentially based on registration order.
    /// If `dependencies` is empty and there's a previously registered step,
    /// this step will automatically depend on it.
    ///
    /// To disable auto-chaining and run steps in parallel, use the special
    /// marker `__NO_AUTO_CHAIN__` as the first dependency (via `depends_on = []`).
    ///
    /// To run steps in parallel, explicitly specify dependencies with `depends_on`:
    /// ```ignore
    /// #[step]
    /// async fn root() -> i32 { 1 }
    ///
    /// #[step(depends_on = "root")]  // Explicit: runs after root
    /// async fn branch1() -> i32 { 2 }
    ///
    /// #[step(depends_on = "root")]  // Explicit: runs in PARALLEL with branch1
    /// async fn branch2() -> i32 { 3 }
    ///
    /// // Or disable auto-chaining for truly independent steps:
    /// #[step(depends_on = [])]  // No dependencies, runs in parallel with others
    /// async fn independent() -> i32 { 4 }
    /// ```
    pub fn register<T, F, Fut>(
        &mut self,
        step_name: &str,
        dependencies: &[&str],
        factory: F,
    ) -> StepHandle<T>
    where
        T: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + 'static,
        F: FnOnce(HashMap<StepId, Vec<u8>>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let step_id = StepId::new(step_name);

        // Check for special marker to disable auto-chaining
        let disable_auto_chain = dependencies
            .first()
            .is_some_and(|&d| d == "__NO_AUTO_CHAIN__");

        // Auto-chain: If no explicit dependencies and there's a previous step, depend on it
        let deps: Vec<StepId> = if disable_auto_chain {
            // Explicit opt-out of auto-chaining: no dependencies
            vec![]
        } else if dependencies.is_empty() {
            if let Some(ref last) = self.last_step {
                vec![last.clone()]
            } else {
                Vec::new()
            }
        } else {
            dependencies.iter().map(|d| StepId::new(*d)).collect()
        };

        // Create channel for result
        let (tx, rx) = oneshot::channel::<Result<T>>();

        // Wrap factory to serialize output
        let factory_boxed: StepFactory = Box::new(move |inputs| {
            Box::pin(async move {
                let result = factory(inputs).await?;
                serialize_value(&result).map_err(ExecutionError::Core)
            })
        });

        // Wrap sender to deserialize and send
        let sender_boxed: Box<dyn FnOnce(Result<Vec<u8>>) + Send> =
            Box::new(move |result: Result<Vec<u8>>| {
                let typed_result = result.and_then(|bytes: Vec<u8>| {
                    deserialize_value::<T>(&bytes).map_err(ExecutionError::Core)
                });
                let _ = tx.send(typed_result);
            });

        self.steps.push(DeferredStep {
            step_id: step_id.clone(),
            dependencies: deps.clone(),
            factory: factory_boxed,
            result_sender: sender_boxed,
        });

        // Update last_step for sequential chaining
        self.last_step = Some(step_id.clone());

        StepHandle::new(step_id, deps, rx)
    }

    /// Executes all registered steps with automatic parallelization
    pub async fn execute(self) -> Result<()> {
        let executor = DeferredExecutor::new();
        executor.execute(self.steps).await
    }

    /// Generates a DOT format representation of the DAG for Graphviz visualization
    ///
    /// Returns a string in DOT format that can be:
    /// - Saved to a .dot file
    /// - Rendered with Graphviz: `dot -Tpng graph.dot -o graph.png`
    /// - Viewed online at https://dreampuf.github.io/GraphvizOnline/
    ///
    /// Example:
    /// ```ignore
    /// let mut registry = DeferredRegistry::new();
    /// // ... register steps ...
    /// let dot = registry.to_dot();
    /// std::fs::write("graph.dot", dot).unwrap();
    /// ```
    pub fn to_dot(&self) -> String {
        let mut graph = DiGraph::<String, ()>::new();
        let mut node_indices = HashMap::new();

        // Create nodes for all steps
        for step in &self.steps {
            let node_idx = graph.add_node(step.step_id.to_string());
            node_indices.insert(step.step_id.clone(), node_idx);
        }

        // Create edges for dependencies
        for step in &self.steps {
            let target_idx = node_indices[&step.step_id];
            for dep in &step.dependencies {
                if let Some(&source_idx) = node_indices.get(dep) {
                    graph.add_edge(source_idx, target_idx, ());
                }
            }
        }

        format!("{:?}", Dot::with_config(&graph, &[Config::EdgeNoLabel]))
    }

    /// Saves the DAG visualization to a DOT file
    ///
    /// Example:
    /// ```ignore
    /// let mut registry = DeferredRegistry::new();
    /// // ... register steps ...
    /// registry.save_dot("flow_graph.dot").unwrap();
    /// // Then render: dot -Tpng flow_graph.dot -o flow_graph.png
    /// ```
    pub fn save_dot(&self, path: &str) -> std::io::Result<()> {
        std::fs::write(path, self.to_dot())
    }

    /// Prints an ASCII tree representation of the DAG to stdout
    ///
    /// Shows the dependency structure in a human-readable format.
    /// Steps at the same level can run in parallel.
    ///
    /// Example output:
    /// ```text
    /// DAG Structure (5 steps):
    /// get_customer
    ///   ├─ validate_payment
    ///   ├─ check_inventory
    ///     └─ process_order
    ///       └─ send_confirmation
    /// ```
    pub fn print_ascii_tree(&self) {
        println!("DAG Structure ({} steps):", self.steps.len());

        // Build dependency map (step -> steps that depend on it)
        let mut dep_map: HashMap<StepId, Vec<StepId>> = HashMap::new();

        for step in &self.steps {
            for dep in &step.dependencies {
                dep_map
                    .entry(dep.clone())
                    .or_default()
                    .push(step.step_id.clone());
            }
        }

        // Find root nodes (steps with no dependencies)
        let roots: Vec<StepId> = self
            .steps
            .iter()
            .filter(|s| s.dependencies.is_empty())
            .map(|s| s.step_id.clone())
            .collect();

        if roots.is_empty() {
            println!("  (No root nodes found - possible cycle or empty DAG)");
            return;
        }

        // Print tree recursively
        let mut visited = std::collections::HashSet::new();
        for (i, root) in roots.iter().enumerate() {
            let is_last = i == roots.len() - 1;
            Self::print_node(root, &dep_map, "", is_last, &mut visited);
        }
    }

    /// Prints a level-based graph view showing parallel execution levels
    ///
    /// Shows steps grouped by their execution level, making it clear
    /// which steps run in parallel.
    ///
    /// Example output:
    /// ```text
    /// Level 0: [get_customer]
    ///          ↓
    /// Level 1: [validate_payment] [check_inventory] (2 parallel steps)
    ///          ↓
    /// Level 2: [process_order]
    ///          ↓
    /// Level 3: [send_confirmation]
    /// ```
    pub fn print_level_graph(&self) {
        println!("DAG Execution Levels ({} steps):", self.steps.len());
        println!();

        // Calculate depths for all steps
        let mut depths: HashMap<StepId, usize> = HashMap::new();
        let mut changed = true;

        // Find roots
        let roots: Vec<&DeferredStep> = self
            .steps
            .iter()
            .filter(|s| s.dependencies.is_empty())
            .collect();

        // Initialize roots with depth 0
        for root in &roots {
            depths.insert(root.step_id.clone(), 0);
        }

        // Iteratively calculate depths
        while changed {
            changed = false;
            for step in &self.steps {
                let mut max_dep_depth = None;
                let mut all_deps_known = true;

                for dep in &step.dependencies {
                    if let Some(&dep_depth) = depths.get(dep) {
                        max_dep_depth = Some(max_dep_depth.unwrap_or(0).max(dep_depth));
                    } else {
                        all_deps_known = false;
                        break;
                    }
                }

                if all_deps_known {
                    if let Some(max_depth) = max_dep_depth {
                        let new_depth = max_depth + 1;
                        if depths.get(&step.step_id) != Some(&new_depth) {
                            depths.insert(step.step_id.clone(), new_depth);
                            changed = true;
                        }
                    }
                }
            }
        }

        // Group steps by level
        let max_level = depths.values().max().copied().unwrap_or(0);
        let mut levels: Vec<Vec<String>> = vec![Vec::new(); max_level + 1];

        for step in &self.steps {
            if let Some(&depth) = depths.get(&step.step_id) {
                levels[depth].push(step.step_id.to_string());
            }
        }

        // Print each level
        for (level, steps) in levels.iter().enumerate() {
            if steps.is_empty() {
                continue;
            }

            let parallel_note = if steps.len() > 1 {
                format!(" ({} parallel steps)", steps.len())
            } else {
                String::new()
            };

            println!("Level {}: [{}]{}", level, steps.join("] ["), parallel_note);

            if level < max_level {
                println!("         ↓");
            }
        }

        println!();
        println!("Steps at the same level run in parallel!");
    }

    /// Helper function to recursively print nodes in tree format
    fn print_node(
        node: &StepId,
        dep_map: &HashMap<StepId, Vec<StepId>>,
        prefix: &str,
        is_last: bool,
        visited: &mut std::collections::HashSet<StepId>,
    ) {
        let connector = if is_last { "└─ " } else { "├─ " };
        println!("{}{}{}", prefix, connector, node);

        if visited.contains(node) {
            return;
        }
        visited.insert(node.clone());

        if let Some(children) = dep_map.get(node) {
            let child_prefix = format!("{}{}  ", prefix, if is_last { "  " } else { "│ " });
            for (i, child) in children.iter().enumerate() {
                let is_last_child = i == children.len() - 1;
                Self::print_node(child, dep_map, &child_prefix, is_last_child, visited);
            }
        }
    }

    /// Returns a summary of the DAG structure
    ///
    /// Provides statistics about the graph including:
    /// - Total number of steps
    /// - Number of root nodes (no dependencies)
    /// - Number of leaf nodes (no dependents)
    /// - Maximum depth
    pub fn summary(&self) -> DagSummary {
        let total_steps = self.steps.len();

        // Find roots (steps with no dependencies)
        let roots: Vec<StepId> = self
            .steps
            .iter()
            .filter(|s| s.dependencies.is_empty())
            .map(|s| s.step_id.clone())
            .collect();

        // Find leaves (steps that no one depends on)
        let mut all_deps: std::collections::HashSet<StepId> = std::collections::HashSet::new();
        for step in &self.steps {
            for dep in &step.dependencies {
                all_deps.insert(dep.clone());
            }
        }

        let leaves: Vec<StepId> = self
            .steps
            .iter()
            .filter(|s| !all_deps.contains(&s.step_id))
            .map(|s| s.step_id.clone())
            .collect();

        // Calculate max depth (simple BFS)
        let max_depth = self.calculate_max_depth();

        DagSummary {
            total_steps,
            root_count: roots.len(),
            leaf_count: leaves.len(),
            max_depth,
            roots,
            leaves,
        }
    }

    /// Calculates the maximum depth of the DAG
    fn calculate_max_depth(&self) -> usize {
        let mut depths: HashMap<StepId, usize> = HashMap::new();
        let mut changed = true;

        // Find roots
        let roots: Vec<&DeferredStep> = self
            .steps
            .iter()
            .filter(|s| s.dependencies.is_empty())
            .collect();

        // Initialize roots with depth 0
        for root in &roots {
            depths.insert(root.step_id.clone(), 0);
        }

        // Iteratively calculate depths
        while changed {
            changed = false;
            for step in &self.steps {
                let mut max_dep_depth = None;
                let mut all_deps_known = true;

                for dep in &step.dependencies {
                    if let Some(&dep_depth) = depths.get(dep) {
                        max_dep_depth = Some(max_dep_depth.unwrap_or(0).max(dep_depth));
                    } else {
                        all_deps_known = false;
                        break;
                    }
                }

                if all_deps_known {
                    if let Some(max_depth) = max_dep_depth {
                        let new_depth = max_depth + 1;
                        if depths.get(&step.step_id) != Some(&new_depth) {
                            depths.insert(step.step_id.clone(), new_depth);
                            changed = true;
                        }
                    }
                }
            }
        }

        depths.values().max().copied().unwrap_or(0)
    }
}

/// Summary information about a DAG structure
#[derive(Debug)]
pub struct DagSummary {
    pub total_steps: usize,
    pub root_count: usize,
    pub leaf_count: usize,
    pub max_depth: usize,
    pub roots: Vec<StepId>,
    pub leaves: Vec<StepId>,
}

impl Default for DeferredRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Executor for deferred steps with automatic parallelization
pub struct DeferredExecutor;

impl DeferredExecutor {
    /// Creates a new executor
    pub fn new() -> Self {
        Self
    }

    /// Executes deferred steps in parallel based on dependencies
    pub async fn execute(&self, mut steps: Vec<DeferredStep>) -> Result<()> {
        let total = steps.len();
        let mut completed: HashSet<StepId> = HashSet::new();
        let results: Arc<Mutex<HashMap<StepId, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));

        // Build dependency graph using FlowGraph
        let mut flow_graph = FlowGraph::new();
        for step in &steps {
            flow_graph
                .add_step(step.step_id.clone())
                .map_err(|e| ExecutionError::Failed(format!("Failed to add step: {}", e)))?;
            for dep in &step.dependencies {
                flow_graph
                    .add_dependency(step.step_id.clone(), dep.clone())
                    .map_err(|e| {
                        ExecutionError::Failed(format!("Failed to add dependency: {}", e))
                    })?;
            }
        }

        // Validate graph for cycles
        flow_graph
            .validate()
            .map_err(|e| ExecutionError::Failed(format!("Invalid dependency graph: {}", e)))?;

        // Build step lookup
        let mut step_map: HashMap<StepId, DeferredStep> =
            steps.drain(..).map(|s| (s.step_id.clone(), s)).collect();

        while completed.len() < total {
            // Use FlowGraph to find ready steps (eliminates duplication)
            let ready = flow_graph.runnable(&completed);

            if ready.is_empty() && completed.len() < total {
                return Err(ExecutionError::Failed(
                    "Deadlock: no steps ready but not all completed".to_string(),
                ));
            }

            // Execute ready steps in parallel using futures::join_all
            // This runs futures concurrently in the SAME task, preserving task-local context
            let mut futures = Vec::new();

            for step_id in ready {
                if let Some(step) = step_map.remove(&step_id) {
                    let results_clone = Arc::clone(&results);
                    let step_id_clone = step_id.clone();

                    // Gather inputs
                    let inputs = {
                        let results_guard = results_clone.lock().await;
                        step.dependencies
                            .iter()
                            .filter_map(|dep| {
                                results_guard.get(dep).map(|v| (dep.clone(), v.clone()))
                            })
                            .collect::<HashMap<_, _>>()
                    };

                    // Create future (NO tokio::spawn - preserves context!)
                    let fut = async move {
                        let output = (step.factory)(inputs).await;
                        (step_id_clone, output, step.result_sender)
                    };

                    futures.push((step_id, fut));
                }
            }

            // Run all futures concurrently in the same task
            let step_results: Vec<_> = futures::future::join_all(
                futures
                    .into_iter()
                    .map(|(id, fut)| async move { (id, fut.await) }),
            )
            .await;

            // Process results
            for (_step_id, (id, output, sender)) in step_results {
                match output {
                    Ok(bytes) => {
                        {
                            let mut results_guard = results.lock().await;
                            results_guard.insert(id.clone(), bytes.clone());
                        }
                        sender(Ok(bytes));
                        completed.insert(id);
                    }
                    Err(e) => {
                        sender(Err(ExecutionError::Failed(format!("Step failed: {}", e))));
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }
}

impl Default for DeferredExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn test_deferred_basic() {
        let mut registry = DeferredRegistry::new();

        let handle_a = registry.register::<i32, _, _>("a", &[], |_| async { Ok(10) });
        let handle_b = registry.register::<i32, _, _>("b", &[], |_| async { Ok(20) });
        let handle_c = registry.register::<i32, _, _>("c", &["a", "b"], |inputs| async move {
            let a: i32 = deserialize_value(inputs.get(&StepId::new("a")).unwrap())
                .map_err(ExecutionError::Core)?;
            let b: i32 = deserialize_value(inputs.get(&StepId::new("b")).unwrap())
                .map_err(ExecutionError::Core)?;
            Ok(a + b)
        });

        // Execute all steps
        registry.execute().await.unwrap();

        // Resolve handles
        let a = handle_a.resolve().await.unwrap();
        let b = handle_b.resolve().await.unwrap();
        let c = handle_c.resolve().await.unwrap();

        assert_eq!(a, 10);
        assert_eq!(b, 20);
        assert_eq!(c, 30);
    }

    #[tokio::test]
    async fn test_deferred_parallel_timing() {
        let mut registry = DeferredRegistry::new();

        let task_duration = Duration::from_millis(100);
        let start = Instant::now();

        let _handle_a = registry.register::<String, _, _>("a", &[], |_| async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok("A".to_string())
        });

        let _handle_b = registry.register::<String, _, _>("b", &[], |_| async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok("B".to_string())
        });

        let handle_c = registry.register::<String, _, _>("c", &["a", "b"], |inputs| async move {
            let a: String = deserialize_value(inputs.get(&StepId::new("a")).unwrap())
                .map_err(ExecutionError::Core)?;
            let b: String = deserialize_value(inputs.get(&StepId::new("b")).unwrap())
                .map_err(ExecutionError::Core)?;
            Ok(format!("{}{}", a, b))
        });

        registry.execute().await.unwrap();

        let elapsed = start.elapsed();
        let result = handle_c.resolve().await.unwrap();

        assert_eq!(result, "AB");

        // Parallel execution should take roughly max(a, b) = 100ms
        // Sequential would take a + b = 200ms
        // Allow 120% overhead for scheduling/system variance (especially in CI)
        let max_parallel_time = task_duration.mul_f32(2.2);
        assert!(
            elapsed < max_parallel_time,
            "Expected parallel execution (< {:?}), got {:?}. \
             If this took > {:?}, tasks likely ran sequentially.",
            max_parallel_time,
            elapsed,
            task_duration.mul_f32(1.5)
        );
    }

    #[tokio::test]
    async fn test_deferred_diamond() {
        let mut registry = DeferredRegistry::new();

        let _handle_a = registry.register::<i32, _, _>("a", &[], |_| async { Ok(10) });
        let _handle_b = registry.register::<i32, _, _>("b", &["a"], |inputs| async move {
            let a: i32 = deserialize_value(inputs.get(&StepId::new("a")).unwrap())
                .map_err(ExecutionError::Core)?;
            Ok(a + 1)
        });
        let _handle_c = registry.register::<i32, _, _>("c", &["a"], |inputs| async move {
            let a: i32 = deserialize_value(inputs.get(&StepId::new("a")).unwrap())
                .map_err(ExecutionError::Core)?;
            Ok(a + 2)
        });
        let handle_d = registry.register::<i32, _, _>("d", &["b", "c"], |inputs| async move {
            let b: i32 = deserialize_value(inputs.get(&StepId::new("b")).unwrap())
                .map_err(ExecutionError::Core)?;
            let c: i32 = deserialize_value(inputs.get(&StepId::new("c")).unwrap())
                .map_err(ExecutionError::Core)?;
            Ok(b + c)
        });

        registry.execute().await.unwrap();

        let d = handle_d.resolve().await.unwrap();
        // a=10, b=11, c=12, d=23
        assert_eq!(d, 23);
    }
}
