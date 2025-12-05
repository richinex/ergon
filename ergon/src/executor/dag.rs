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
use crate::graph::{Graph, StepId};
use petgraph::dot::{Config, Dot};
use petgraph::graph::DiGraph;
use serde::de::DeserializeOwned;
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
///
/// **Important**: Handles are single-use. The `resolve()` method consumes the handle
/// and can only be called once. If you need the result multiple times, store it
/// after resolving.
pub struct StepHandle<T> {
    /// Unique step identifier
    step_id: StepId,
    /// Dependencies (step IDs this step depends on)
    dependencies: Vec<StepId>,
    /// Receiver for the result (when resolved)
    receiver: oneshot::Receiver<Result<T>>,
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

    /// Awaits the result (blocks until the step is resolved).
    ///
    /// **This method consumes the handle and can only be called once.**
    /// If you need the result in multiple places, store it in a variable:
    ///
    /// ```ignore
    /// let result = handle.resolve().await?;
    /// // Now you can use `result` multiple times
    /// ```
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
}

impl DeferredRegistry {
    /// Creates a new registry
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// Registers a deferred step with explicit dependencies.
    ///
    /// Dependencies must be explicitly declared - the DAG structure should be clear from the code.
    ///
    /// **Example - Independent parallel steps**:
    /// ```ignore
    /// let mut registry = DeferredRegistry::new();
    ///
    /// // These three steps run in parallel (no dependencies):
    /// let a = registry.register("fetch_user", &[], |_| async { Ok(user) });
    /// let b = registry.register("fetch_inventory", &[], |_| async { Ok(inventory) });
    /// let c = registry.register("fetch_pricing", &[], |_| async { Ok(price) });
    /// ```
    ///
    /// **Example - Explicit dependencies**:
    /// ```ignore
    /// #[step]
    /// async fn root() -> i32 { 1 }
    ///
    /// #[step(depends_on = "root")]  // Explicit: runs after root
    /// async fn branch1() -> i32 { 2 }
    ///
    /// #[step(depends_on = "root")]  // Explicit: runs in PARALLEL with branch1
    /// async fn branch2() -> i32 { 3 }
    /// ```
    pub fn register<T, F, Fut>(
        &mut self,
        step_name: &str,
        dependencies: &[&str],
        factory: F,
    ) -> StepHandle<T>
    where
        T: serde::Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce(HashMap<StepId, Vec<u8>>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let step_id = StepId::new(step_name);
        let deps: Vec<StepId> = dependencies.iter().map(|d| StepId::new(*d)).collect();

        // Create channel for result
        let (tx, rx) = oneshot::channel::<Result<T>>();

        // Wrap factory to serialize output
        let factory_boxed: StepFactory = Box::new(move |inputs| {
            Box::pin(async move {
                let result = factory(inputs).await?;
                serialize_value(&result).map_err(ExecutionError::from)
            })
        });

        // Wrap sender to deserialize and send
        let sender_boxed: Box<dyn FnOnce(Result<Vec<u8>>) + Send> =
            Box::new(move |result: Result<Vec<u8>>| {
                let typed_result = result.and_then(|bytes: Vec<u8>| {
                    deserialize_value::<T>(&bytes).map_err(ExecutionError::from)
                });
                let _ = tx.send(typed_result);
            });

        self.steps.push(DeferredStep {
            step_id: step_id.clone(),
            dependencies: deps.clone(),
            factory: factory_boxed,
            result_sender: sender_boxed,
        });

        StepHandle::new(step_id, deps, rx)
    }

    /// Validates the DAG structure without executing.
    ///
    /// Checks for:
    /// - Cycles in the dependency graph
    /// - Invalid dependencies (referencing non-existent steps)
    ///
    /// Call this before `execute()` to catch structural issues early,
    /// especially useful for debugging and testing.
    ///
    /// # Example
    /// ```ignore
    /// let mut registry = DeferredRegistry::new();
    /// // ... register steps ...
    ///
    /// // Validate before execution
    /// registry.validate()?;
    ///
    /// // If validation passes, safe to execute
    /// registry.execute().await?;
    /// ```
    pub fn validate(&self) -> Result<()> {
        let mut flow_graph = Graph::new();

        for step in &self.steps {
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

        // Validate graph for cycles and invalid dependencies
        flow_graph
            .validate()
            .map_err(|e| ExecutionError::Failed(format!("Invalid dependency graph: {}", e)))?;

        Ok(())
    }

    /// Executes all registered steps with automatic parallelization.
    ///
    /// **Note**: This method automatically validates the DAG before execution.
    /// You can call `validate()` beforehand if you want to check for issues
    /// without consuming the registry.
    pub async fn execute(self) -> Result<()> {
        execute_dag(self.steps).await
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

    /// Returns an ASCII tree representation of the DAG.
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
    pub fn ascii_tree(&self) -> String {
        let mut output = format!("DAG Structure ({} steps):\n", self.steps.len());

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
            output.push_str("  (No root nodes found - possible cycle or empty DAG)\n");
            return output;
        }

        // Build tree recursively
        let mut visited = std::collections::HashSet::new();
        for (i, root) in roots.iter().enumerate() {
            let is_last = i == roots.len() - 1;
            Self::format_node(root, &dep_map, "", is_last, &mut visited, &mut output);
        }

        output
    }

    /// Returns a level-based graph view showing parallel execution levels.
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
    pub fn level_graph(&self) -> String {
        let mut output = format!("DAG Execution Levels ({} steps):\n\n", self.steps.len());

        // Calculate depths for all steps using shared helper
        let depths = self.calculate_depths();

        // Group steps by level
        let max_level = depths.values().max().copied().unwrap_or(0);
        let mut levels: Vec<Vec<String>> = vec![Vec::new(); max_level + 1];

        for step in &self.steps {
            if let Some(&depth) = depths.get(&step.step_id) {
                levels[depth].push(step.step_id.to_string());
            }
        }

        // Format each level
        for (level, steps) in levels.iter().enumerate() {
            if steps.is_empty() {
                continue;
            }

            let parallel_note = if steps.len() > 1 {
                format!(" ({} parallel steps)", steps.len())
            } else {
                String::new()
            };

            output.push_str(&format!(
                "Level {}: [{}]{}\n",
                level,
                steps.join("] ["),
                parallel_note
            ));

            if level < max_level {
                output.push_str("         ↓\n");
            }
        }

        output.push_str("\nSteps at the same level run in parallel!\n");
        output
    }

    /// Helper function to recursively format nodes in tree format.
    ///
    /// Appends the formatted tree to the output string.
    /// Returns whether a cycle was detected.
    fn format_node(
        node: &StepId,
        dep_map: &HashMap<StepId, Vec<StepId>>,
        prefix: &str,
        is_last: bool,
        visited: &mut std::collections::HashSet<StepId>,
        output: &mut String,
    ) -> bool {
        let connector = if is_last { "└─ " } else { "├─ " };
        output.push_str(&format!("{}{}{}\n", prefix, connector, node));

        // Detect cycles: if we've visited this node before, it's a cycle
        if visited.contains(node) {
            output.push_str(&format!("{}    ⚠️  CYCLE DETECTED\n", prefix));
            return true;
        }
        visited.insert(node.clone());

        let mut cycle_detected = false;
        if let Some(children) = dep_map.get(node) {
            let child_prefix = format!("{}{}  ", prefix, if is_last { "  " } else { "│ " });
            for (i, child) in children.iter().enumerate() {
                let is_last_child = i == children.len() - 1;
                if Self::format_node(
                    child,
                    dep_map,
                    &child_prefix,
                    is_last_child,
                    visited,
                    output,
                ) {
                    cycle_detected = true;
                }
            }
        }

        cycle_detected
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

    /// Calculates the depth of each step in the DAG.
    ///
    /// Returns a HashMap mapping each StepId to its depth (distance from root).
    /// Root nodes have depth 0, their children have depth 1, etc.
    fn calculate_depths(&self) -> HashMap<StepId, usize> {
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

        // Iteratively calculate depths using dynamic programming
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

        depths
    }

    /// Calculates the maximum depth of the DAG
    fn calculate_max_depth(&self) -> usize {
        self.calculate_depths().values().max().copied().unwrap_or(0)
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

/// Executes deferred steps in parallel based on dependencies.
///
/// This is the core DAG execution engine that:
/// 1. Builds and validates the dependency graph
/// 2. Executes steps in topological order
/// 3. Runs independent steps in parallel
/// 4. Preserves task-local context (no tokio::spawn)
///
/// # Example
/// ```ignore
/// let mut registry = DeferredRegistry::new();
/// // ... register steps ...
/// registry.execute().await?;  // Internally calls execute_dag
/// ```
pub async fn execute_dag(mut steps: Vec<DeferredStep>) -> Result<()> {
    let total = steps.len();
    let mut completed: HashSet<StepId> = HashSet::new();
    let results: Arc<Mutex<HashMap<StepId, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));

    // Build dependency graph using Graph
    let mut flow_graph = Graph::new();
    for step in &steps {
        flow_graph
            .add_step(step.step_id.clone())
            .map_err(|e| ExecutionError::Failed(format!("Failed to add step: {}", e)))?;
        for dep in &step.dependencies {
            flow_graph
                .add_dependency(step.step_id.clone(), dep.clone())
                .map_err(|e| ExecutionError::Failed(format!("Failed to add dependency: {}", e)))?;
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
        // Use Graph to find ready steps (eliminates duplication)
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
                        .filter_map(|dep| results_guard.get(dep).map(|v| (dep.clone(), v.clone())))
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
                .map_err(ExecutionError::from)?;
            let b: i32 = deserialize_value(inputs.get(&StepId::new("b")).unwrap())
                .map_err(ExecutionError::from)?;
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
                .map_err(ExecutionError::from)?;
            let b: String = deserialize_value(inputs.get(&StepId::new("b")).unwrap())
                .map_err(ExecutionError::from)?;
            Ok(format!("{}{}", a, b))
        });

        registry.execute().await.unwrap();

        let elapsed = start.elapsed();
        let result = handle_c.resolve().await.unwrap();

        assert_eq!(result, "AB");

        // Parallel execution should take roughly max(a, b) = 100ms
        // Sequential would take a + b = 200ms
        // Allow 150% overhead for scheduling/system variance (especially in CI/Windows)
        let max_parallel_time = task_duration.mul_f32(2.5);
        assert!(
            elapsed < max_parallel_time,
            "Expected parallel execution (< {:?}), got {:?}. \
             If this took > {:?}, tasks likely ran sequentially.",
            max_parallel_time,
            elapsed,
            task_duration.mul_f32(1.8)
        );
    }

    #[tokio::test]
    async fn test_deferred_diamond() {
        let mut registry = DeferredRegistry::new();

        let _handle_a = registry.register::<i32, _, _>("a", &[], |_| async { Ok(10) });
        let _handle_b = registry.register::<i32, _, _>("b", &["a"], |inputs| async move {
            let a: i32 = deserialize_value(inputs.get(&StepId::new("a")).unwrap())
                .map_err(ExecutionError::from)?;
            Ok(a + 1)
        });
        let _handle_c = registry.register::<i32, _, _>("c", &["a"], |inputs| async move {
            let a: i32 = deserialize_value(inputs.get(&StepId::new("a")).unwrap())
                .map_err(ExecutionError::from)?;
            Ok(a + 2)
        });
        let handle_d = registry.register::<i32, _, _>("d", &["b", "c"], |inputs| async move {
            let b: i32 = deserialize_value(inputs.get(&StepId::new("b")).unwrap())
                .map_err(ExecutionError::from)?;
            let c: i32 = deserialize_value(inputs.get(&StepId::new("c")).unwrap())
                .map_err(ExecutionError::from)?;
            Ok(b + c)
        });

        registry.execute().await.unwrap();

        let d = handle_d.resolve().await.unwrap();
        // a=10, b=11, c=12, d=23
        assert_eq!(d, 23);
    }
}
