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
use quote::quote;
use syn::{parse_macro_input, FnArg, ItemFn, Pat, PatType, ReturnType};

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

    // Wrap the user's block in an immediately-invoked async closure
    // This prevents early returns from bypassing the completion logging code
    // The closure captures all parameters from the outer scope
    let wrapped_block = quote! {
        {
            let __executor = || async move { #block };
            __executor().await
        }
    };

    // Generate completion logging code
    let log_completion_code = if !returns_result {
        quote! {
            let _ = __ctx.log_step_completion(__step, &__result).await;
        }
    } else {
        quote! {
            {
                // Note: Don't call is_retryable() here - the step/child flow already did it
                // and stored the result. This avoids redundant trait method calls.
                let __should_cache = __result.is_ok();

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
                    // Use stable type_id from FlowType trait instead of std::any::type_name
                    // which is explicitly unstable across compiler versions
                    let __class_name = <Self as ergon::core::FlowType>::type_id();

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
                            // Distinguish between programmer mistakes and environment problems
                            match __e {
                                ergon::ExecutionError::Incompatible(_) => {
                                    // Non-determinism = programmer mistake (bug)
                                    // Continuing would corrupt flow state - must panic
                                    panic!("Non-determinism detected at step {}: {}", __step, __e);
                                }
                                _ => {
                                    // Environment problem (storage, deserialization)
                                    // Treat as cache miss - continue execution
                                    // Tradeoff: if storage is down, worst case is duplicate work
                                    // But steps should be idempotent anyway (best practice)
                                    // This is more resilient than blocking on storage errors
                                    tracing::warn!(
                                        "Storage error during cache check at step {}: {}. Treating as cache miss. \
                                         Ensure your steps are idempotent to handle potential duplicate execution.",
                                        __step, __e
                                    );
                                }
                            }
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

                    // Set enclosing step so that flow-level signals/timers/child invocations
                    // know they're being called from this flow (step 0)
                    __ctx.set_enclosing_step(__step);

                    // Execute the user's DAG setup and execution block
                    let __result: #return_type = #wrapped_block;

                    // Check if flow is suspending (child flow waiting, timer, signal)
                    // Suspension is framework machinery that should not be logged as completion
                    if !__ctx.has_suspend_reason() {
                        // Normal completion or error - log it
                        #log_completion_code
                    }

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
