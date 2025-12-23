//! Procedural macros for the Ergon durable execution framework.
//!
//! This crate provides #[step] and #[flow] macros with:
//! - Explicit dependency graph management
//! - Automatic parallel execution of independent steps
//! - Execution context integration
//! - Input wiring for data flow
//! - Cache checking with non-determinism detection
//! - Invocation logging
//! - Delay support
//! - Autoref specialization for RetryableError
//!
//! # Module Organization (Following Parnas's Information Hiding)
//!
//! Each submodule hides a design decision:
//!
//! - [`parsing`]: How macro attributes are parsed and signature analysis
//! - [`step`]: How #[step] macro works and generates code
//! - [`flow`]: How #[flow] macro works and generates code
//! - [`dag`]: How dag! macro works and generates code
//! - [`determinism`]: How non-deterministic operations are detected
//!
//! This module (lib.rs) contains:
//! - Public macro entry points (#[step], #[flow], dag!)
//! - Re-exports from submodules

use proc_macro::TokenStream;

mod dag;
mod determinism;
mod flow;
mod flow_type;
mod parsing;
mod step;

/// Marks an async method as a durable execution step.
///
/// See [`step`](step::step_impl) module for details.
#[proc_macro_attribute]
pub fn step(attr: TokenStream, item: TokenStream) -> TokenStream {
    step::step_impl(attr, item)
}

/// Marks an async method as a flow orchestrator.
///
/// A `#[flow]` method orchestrates the execution of `#[step]` methods.
/// It should create a `DeferredRegistry`, register all steps, execute them,
/// and return the final result.
///
/// # Example
///
/// ```ignore
/// #[flow]
/// async fn process(&self) -> Result<OrderResult, String> {
///     let mut registry = DeferredRegistry::new();
///
///     let customer_handle = self.register_get_customer(&mut registry);
///     let validate_handle = self.register_validate(&mut registry);
///     let check_handle = self.register_check(&mut registry);
///     let final_handle = self.register_finalize(&mut registry);
///
///     registry.execute().await.map_err(|e| e.to_string())?;
///     final_handle.resolve().await.map_err(|e| e.to_string())?
/// }
/// ```
#[proc_macro_attribute]
pub fn flow(attr: TokenStream, item: TokenStream) -> TokenStream {
    flow::flow_impl(attr, item)
}

/// Derives the FlowType trait for a struct, providing a stable type identifier.
///
/// The type ID defaults to the struct name. For custom type IDs, use the `flow_type` attribute:
///
/// ```ignore
/// #[derive(FlowType)]
/// #[flow_type(id = "com.company.MyFlow.v1")]
/// struct MyFlow { }
/// ```
///
/// For child flows that can be invoked using the `invoke()` API, use the `invokable` attribute
/// to also implement the `InvokableFlow` trait:
///
/// ```ignore
/// #[derive(Clone, Serialize, Deserialize, FlowType)]
/// #[invokable(output = Label)]
/// struct Shipment {
///     order_id: String,
/// }
/// ```
///
/// This replaces the manual implementation:
/// ```ignore
/// impl InvokableFlow for Shipment {
///     type Output = Label;
/// }
/// ```
#[proc_macro_derive(FlowType, attributes(flow_type, invokable))]
pub fn derive_flow_type(input: TokenStream) -> TokenStream {
    flow_type::derive_flow_type_impl(input)
}

/// Helper macro for ergonomic DAG execution.
///
/// Automatically handles both parallel and sequential execution based on step dependencies.
///
/// Steps without dependencies run in parallel. Steps with dependencies run sequentially
/// after their dependencies complete. This is determined by the `depends_on` attribute
/// on your `#[step]` definitions.
///
/// # What it does
///
/// 1. Creates the DeferredRegistry
/// 2. Registers all steps
/// 3. Executes the DAG (parallel where possible, sequential where required)
/// 4. Resolves the final result
///
/// # Usage
///
/// ```ignore
/// #[flow]
/// async fn process(self: Arc<Self>) -> Result<OrderResult, FlowError> {
///     dag! {
///         self.register_get_customer();
///         self.register_validate_payment();
///         self.register_check_inventory();
///         self.register_process_order();
///         self.register_send_confirmation() // Last call becomes the return value
///     }
/// }
/// ```
///
/// This expands to:
/// ```ignore
/// {
///     let mut registry = DeferredRegistry::new();
///     self.register_get_customer(&mut registry);
///     self.register_validate_payment(&mut registry);
///     self.register_check_inventory(&mut registry);
///     self.register_process_order(&mut registry);
///     let final_handle = self.register_send_confirmation(&mut registry);
///     registry.execute().await.map_err(|e| e.into_flow_error())?;
///     final_handle.resolve().await.map_err(|e| e.into_flow_error())?
/// }
/// ```
///
/// See [`dag`](dag::dag_impl) module for implementation details.
#[proc_macro]
pub fn dag(input: TokenStream) -> TokenStream {
    dag::dag_impl(input)
}
