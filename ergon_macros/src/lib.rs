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
//! - [`determinism`]: How non-deterministic operations are detected
//!
//! This module (lib.rs) contains:
//! - Public macro entry points (#[step], #[flow], dag!)
//! - Re-exports from submodules

use proc_macro::TokenStream;

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
#[proc_macro]
pub fn dag(input: TokenStream) -> TokenStream {
    use proc_macro2::TokenStream as TokenStream2;
    use quote::quote;
    use syn::parse::{Parse, ParseStream};
    use syn::{parse_macro_input, Expr, Token};

    struct DagCalls {
        calls: Vec<Expr>,
    }

    impl Parse for DagCalls {
        fn parse(input: ParseStream) -> syn::Result<Self> {
            let mut calls = Vec::new();

            while !input.is_empty() {
                let call: Expr = input.parse()?;
                calls.push(call);

                // Allow optional trailing semicolon
                if input.peek(Token![;]) {
                    let _: Token![;] = input.parse()?;
                }
            }

            Ok(DagCalls { calls })
        }
    }

    let parsed = parse_macro_input!(input as DagCalls);

    if parsed.calls.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "dag! macro requires at least one step registration call",
        )
        .to_compile_error()
        .into();
    }

    let calls = &parsed.calls;
    let last_call = calls.last().unwrap();
    let intermediate_calls = &calls[..calls.len() - 1];

    // Transform each call to add &mut registry parameter
    // The input is like: self.register_foo()
    // We need to transform it to: self.register_foo(&mut __registry)
    let intermediate_stmts: Vec<TokenStream2> = intermediate_calls
        .iter()
        .map(|call| {
            if let Expr::MethodCall(ref method_call) = call {
                let receiver = &method_call.receiver;
                let method = &method_call.method;
                let turbofish = &method_call.turbofish;
                // Insert &mut __registry as first argument
                quote! { #receiver.#method #turbofish(&mut __registry); }
            } else {
                // Not a method call, pass through as-is
                quote! { #call; }
            }
        })
        .collect();

    // Handle the last call the same way
    let final_call_stmt = if let Expr::MethodCall(ref method_call) = last_call {
        let receiver = &method_call.receiver;
        let method = &method_call.method;
        let turbofish = &method_call.turbofish;
        quote! { #receiver.#method #turbofish(&mut __registry) }
    } else {
        quote! { #last_call }
    };

    let expanded = quote! {
        async {
            let mut __registry = ergon::DeferredRegistry::new();
            #(#intermediate_stmts)*
            let __final_handle = #final_call_stmt;

            // Execute registry and handle framework/infrastructure errors
            match __registry.execute().await {
                Ok(_) => {
                    // Handle is parameterized by Result<T, UserError>
                    // resolve() returns Result<Result<T, UserError>, ExecutionError>
                    // Flatten and convert user errors to ExecutionError::User with downcasting support
                    match __final_handle.resolve().await {
                        Ok(user_result) => {
                            user_result.map_err(|__user_error| {
                                // Convert user error to ExecutionError::User
                                #[allow(unused_imports)]
                                use ::ergon::kind::*;
                                let __is_retryable = (__user_error).error_kind().is_retryable(&__user_error);

                                ergon::ExecutionError::User {
                                    type_name: std::any::type_name_of_val(&__user_error).to_string(),
                                    message: __user_error.to_string(),
                                    retryable: __is_retryable,
                                }
                            })
                        }
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e),
            }
        }.await
    };

    TokenStream::from(expanded)
}
