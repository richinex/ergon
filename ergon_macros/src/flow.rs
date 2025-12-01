//! Flow macro implementation.
//!
//! This module provides the #[flow] macro that orchestrates step execution.
//! It hides the complexity of:
//! - DAG registration auto-wrapping
//! - Flow instrumentation (context, caching, logging)
//! - Result handling for flows
//!
//! Following Parnas's information hiding principle, this module encapsulates
//! decisions about how flow methods are generated and instrumented.

use crate::parsing::{build_method_signature, is_result_type};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Block, Expr, FnArg, ItemFn, Pat, PatType, ReturnType, Stmt};

/// Arguments for the #[flow] macro
#[derive(Default)]
struct FlowArgs {
    /// Retry policy expression (e.g., RetryPolicy::STANDARD)
    retry: Option<proc_macro2::TokenStream>,
}

impl FlowArgs {
    fn parse_meta(&mut self, meta: syn::meta::ParseNestedMeta) -> syn::Result<()> {
        if meta.path.is_ident("retry") {
            // Parse: retry = RetryPolicy::STANDARD
            let value = meta.value()?;
            self.retry = Some(value.parse()?);
            Ok(())
        } else {
            Err(meta.error("unsupported flow attribute"))
        }
    }
}

/// Try to auto-wrap DAG registration calls.
///
/// If the block contains only `self.register_*()` method calls,
/// automatically wrap them with:
/// 1. Creating DeferredRegistry
/// 2. Calling each with &mut registry
/// 3. Executing registry
/// 4. Resolving final handle
///
/// Returns the original block if pattern doesn't match.
pub(crate) fn try_wrap_dag_registrations(
    block: &Block,
    _return_type: &TokenStream2,
) -> TokenStream2 {
    // Extract all statements from the block
    let stmts = &block.stmts;

    if stmts.is_empty() {
        return quote! { #block };
    }

    // Check if all statements are expression statements with register_* calls
    let mut register_calls = Vec::new();

    for stmt in stmts {
        // Extract the expression from the statement
        let expr = match stmt {
            Stmt::Expr(expr, _) => expr,
            Stmt::Macro(_) => {
                // Don't handle macro calls
                return quote! { #block };
            }
            _ => {
                // Other statement types, return original block
                return quote! { #block };
            }
        };

        // Check if this is a method call starting with "register_"
        if let Expr::MethodCall(method_call) = expr {
            let method_name = method_call.method.to_string();
            if method_name.starts_with("register_") {
                register_calls.push(expr.clone());
                continue;
            }
        }
        // If we hit a non-register call, return original block
        return quote! { #block };
    }

    // If we found no register calls, return original
    if register_calls.is_empty() {
        return quote! { #block };
    }

    // We have a pure registration block! Auto-wrap it
    // Transform each call: self.register_foo() becomes self.register_foo(&mut registry)
    let mut transformed_calls = Vec::new();

    for call_expr in &register_calls {
        if let Expr::MethodCall(method_call) = call_expr {
            // Clone the method call and add &mut registry as first argument
            let receiver = &method_call.receiver;
            let method = &method_call.method;
            let args: Vec<_> = method_call.args.iter().collect();
            let turbofish = &method_call.turbofish;

            // Build new call with &mut __registry prepended to args
            let transformed = if args.is_empty() {
                quote! {
                    #receiver.#method #turbofish(&mut __registry)
                }
            } else {
                quote! {
                    #receiver.#method #turbofish(&mut __registry, #(#args),*)
                }
            };
            transformed_calls.push(transformed);
        }
    }

    if transformed_calls.is_empty() {
        return quote! { #block };
    }

    let last_call = transformed_calls
        .last()
        .expect("transformed_calls not empty after check");
    let intermediate_calls = &transformed_calls[..transformed_calls.len() - 1];

    quote! {
        {
            let mut __registry = ergon::DeferredRegistry::new();
            #(#intermediate_calls;)*
            let __final_handle = #last_call;
            __registry.execute().await.map_err(|e| format!("DAG execution failed: {:?}", e))?;
            __final_handle.resolve().await.map_err(|e| format!("Failed to resolve result: {:?}", e))?
        }
    }
}

/// The `#[flow]` macro marks an async method as a DAG flow orchestrator.
///
/// This macro provides ALL the features of `#[flow]` - execution context,
/// caching, logging, etc. The flow body should create a `DeferredRegistry`,
/// register all steps, execute them, and return the result.
///
/// # Usage with Arc<Self>
///
/// When using `#[step]` methods with `self: Arc<Self>`, the flow method
/// should also use `self: Arc<Self>` for consistency. This enables clean,
/// method-style registration calls.
///
/// # Example
///
/// ```ignore
/// #[flow]
/// async fn process(self: Arc<Self>) -> OrderResult {
///     let mut registry = DeferredRegistry::new();
///
///     // Register steps using method-style calls
///     let customer_handle = self.register_get_customer(&mut registry);
///     let validate_handle = self.register_validate_payment(&mut registry);
///     let check_handle = self.register_check_inventory(&mut registry);
///     let final_handle = self.register_finalize(&mut registry);
///
///     registry.execute().await.expect("DAG execution failed");
///
///     final_handle.resolve().await.expect("Failed to get result")
/// }
/// ```
pub fn flow_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse flow arguments
    let mut args = FlowArgs::default();

    if !attr.is_empty() {
        let attr_parser = syn::meta::parser(|meta| args.parse_meta(meta));

        if let Err(err) = syn::parse::Parser::parse(attr_parser, attr) {
            return err.to_compile_error().into();
        }
    }

    let input = parse_macro_input!(item as ItemFn);

    // Build retry policy expression
    let retry_policy = if let Some(ref retry_expr) = args.retry {
        quote! { Some(#retry_expr) }
    } else {
        quote! { None }
    };

    let vis = &input.vis;
    let sig = &input.sig;
    let block = &input.block;
    let attrs = &input.attrs;

    // Check if function is async
    if sig.asyncness.is_none() {
        return syn::Error::new_spanned(sig, "#[flow] can only be applied to async functions")
            .to_compile_error()
            .into();
    }

    // Extract return type
    let return_type = match &sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    // Check if return type is Result
    let returns_result = is_result_type(&sig.output);

    // Build method signature
    let method_name_str = build_method_signature(sig);

    // Extract parameter names for serialization
    let param_names: Vec<_> = sig
        .inputs
        .iter()
        .filter_map(|arg| match arg {
            FnArg::Typed(PatType { pat, .. }) => {
                if let Pat::Ident(pat_ident) = &**pat {
                    Some(&pat_ident.ident)
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();

    // Try to detect if this is a simple DAG registration pattern
    // If the block contains only register_* calls, auto-wrap them
    let wrapped_block = try_wrap_dag_registrations(block, &return_type);

    // Generate completion logging code
    let log_completion_code = if !returns_result {
        quote! {
            let _ = __ctx.log_step_completion(__step, &__result).await;
        }
    } else {
        quote! {
            {
                #[allow(unused_imports)]
                use ::ergon::kind::*;

                let __should_cache = match __result.as_ref().err() {
                    Some(__e) => {
                        (__e).error_kind().should_cache(__e)
                    }
                    None => true,
                };

                if __should_cache {
                    let _ = __ctx.log_step_completion(__step, &__result).await;
                }
            }
        }
    };

    // Generate the flow method with full instrumentation
    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            // Try to get execution context
            match ergon::EXECUTION_CONTEXT.try_with(|ctx| ctx.clone()) {
                Ok(__ctx) => {
                    // Instrumented execution with context
                    let __step = __ctx.next_step();
                    let __class_name = std::any::type_name::<Self>();

                    // Serialize parameters
                    let __params = ( #(&#param_names,)* );

                    // Check cache for completed invocation
                    match __ctx.get_cached_result::<#return_type, _>(
                        __step, __class_name, #method_name_str, &__params
                    ).await {
                        Ok(Some(__cached_result)) => {
                            return __cached_result;
                        }
                        Ok(None) => {
                            // No cached result, continue
                        }
                        Err(__e) => {
                            panic!("Flow execution error at step {}: {}", __step, __e);
                        }
                    }

                    // Log invocation start
                    let _ = __ctx.log_step_start(
                        ergon::executor::LogStepStartParams {
                            step: __step,
                            class_name: __class_name,
                            method_name: #method_name_str,
                            delay: None,
                            status: ergon::InvocationStatus::Pending,
                            params: &__params,
                            retry_policy: #retry_policy,
                        }
                    ).await;

                    // Execute the user's DAG setup and execution block
                    let __result: #return_type = #wrapped_block;

                    // Log completion
                    #log_completion_code

                    __result
                }
                Err(_) => {
                    // Direct execution without context
                    #wrapped_block
                }
            }
        }
    };

    TokenStream::from(expanded)
}
