//! DAG macro implementation for ergonomic parallel step execution.
//!
//! This module implements the `dag!` macro which provides a declarative way to
//! execute steps in a Directed Acyclic Graph (DAG) pattern. Steps without
//! dependencies run in parallel, while steps with dependencies run sequentially
//! after their dependencies complete.
//!
//! # Design Decision Hidden by This Module
//!
//! - How the dag! macro transforms user calls into DeferredRegistry operations
//! - How method calls are transformed to inject the registry parameter
//! - How the final result is extracted and errors are converted

use proc_macro::TokenStream;
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

pub fn dag_impl(input: TokenStream) -> TokenStream {
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

                                // Get type name before any potential boxing/erasure
                                // Using a helper function to capture the concrete type via inference
                                fn type_name_of<T: ?Sized>(_: &T) -> &'static str {
                                    std::any::type_name::<T>()
                                }
                                let __type_name = type_name_of(&__user_error);

                                ergon::ExecutionError::User {
                                    type_name: __type_name.to_string(),
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
