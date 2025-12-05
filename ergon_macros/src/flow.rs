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
                #[allow(unused_imports)]
                use ::ergon::kind::*;

                let (__should_cache, __is_retryable_opt) = match __result.as_ref().err() {
                    Some(__e) => {
                        let __is_retryable = (__e).error_kind().is_retryable(__e);
                        (!__is_retryable, Some(__is_retryable)) // should_cache = !is_retryable
                    }
                    None => (true, None),
                };

                // Store the is_retryable flag before caching (for worker retry decision)
                if let Some(__is_ret) = __is_retryable_opt {
                    let _ = __ctx.update_step_retryability(__step, __is_ret).await;
                }

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

                    // Don't log completion if flow is suspending
                    // await_external_signal sets suspend_reason before returning its error
                    // This prevents caching the suspension error as a "completed" step
                    if !__ctx.has_suspend_reason() {
                        // Log completion (only for real completions, not suspensions)
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
