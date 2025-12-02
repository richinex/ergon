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
//!
//! This module (lib.rs) contains:
//! - Public macro entry points (#[step], #[flow], dag!)
//! - Re-exports from submodules

use proc_macro::TokenStream;

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
/// async fn process(&self) -> OrderResult {
///     let mut registry = DeferredRegistry::new();
///
///     let customer_handle = self.register_get_customer(&mut registry);
///     let validate_handle = self.register_validate(&mut registry);
///     let check_handle = self.register_check(&mut registry);
///     let final_handle = self.register_finalize(&mut registry);
///
///     registry.execute().await.expect("DAG execution failed");
///
///     final_handle.resolve().await.expect("Failed to get result")
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
#[proc_macro_derive(FlowType, attributes(flow_type))]
pub fn derive_flow_type(input: TokenStream) -> TokenStream {
    flow_type::derive_flow_type_impl(input)
}

/// Helper macro for ergonomic DAG execution.
///
/// Eliminates boilerplate by automatically:
/// 1. Creating the DeferredRegistry
/// 2. Executing the registry
/// 3. Resolving the final handle
///
/// # Usage
///
/// ```ignore
/// #[flow]
/// async fn process(self: Arc<Self>) -> OrderResult {
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
///     registry.execute().await.expect("DAG execution failed");
///     final_handle.resolve().await.expect("Failed to get result")
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
    let intermediate_stmts: Vec<TokenStream2> = intermediate_calls
        .iter()
        .map(|call| {
            quote! { #call(&mut __registry); }
        })
        .collect();

    let expanded = quote! {
        {
            let mut __registry = ergon::DeferredRegistry::new();
            #(#intermediate_stmts)*
            let __final_handle = #last_call(&mut __registry);
            __registry.execute().await.expect("DAG execution failed");
            __final_handle.resolve().await.expect("Failed to resolve final result")
        }
    };

    TokenStream::from(expanded)
}
