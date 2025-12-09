//! Step macro implementation.
//!
//! This module provides the #[step] macro that instruments individual workflow steps.
//! It hides the complexity of:
//! - Step signature analysis (Arc<Self>, parameters, return types)
//! - Registration function generation for DAG execution
//! - Executor function generation
//! - Direct execution function with instrumentation
//! - Input wiring and dependency extraction
//! - Cache checking and result handling
//! - Retry behavior with autoref specialization
//!
//! Following Parnas's information hiding principle, this module encapsulates
//! decisions about how step methods are transformed and instrumented.

use crate::parsing::{build_method_signature, is_result_type, DagStepArgs};
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, FnArg, ItemFn, Pat, ReturnType, Type};

/// The `#[step]` macro marks an async function as a step in a workflow.
///
/// Dependencies must be **explicitly declared** using `depends_on`:
/// - **No dependencies**: Steps without `depends_on` run in parallel
/// - **Explicit dependencies**: `depends_on = "step"` creates a sequential dependency
/// - **Multiple dependencies**: `depends_on = ["step_a", "step_b"]` waits for all
///
/// # Using self: Arc<Self>
///
/// To enable method-style calls, use `self: Arc<Self>` as the receiver:
/// - Allows clean registration: `self.register_foo(&mut registry)`
/// - Can access struct fields: `self.customer_id`
/// - Enables multi-threaded parallelism (satisfies 'static requirement)
/// - Standard Rust async pattern for spawned tasks
///
/// # Attributes
///
/// - `delay = <number>` - Delay before execution
/// - `unit = "MILLIS"|"SECONDS"|"MINUTES"|"HOURS"` - Time unit for delay
/// - `cache_errors` - Cache error results (default: use autoref specialization)
/// - `depends_on = "step_name"` - Single explicit dependency (enables parallelism)
/// - `depends_on = ["step_a", "step_b"]` - Multiple dependencies
/// - `inputs(param = "step")` - Wire dependency output to parameter
/// - `inputs(param1 = "step1", param2 = "step2")` - Multiple input wirings
///
/// # Execution Behavior
///
/// **Without `depends_on` attribute**: Step has no dependencies and runs immediately
/// in parallel with other independent steps.
///
/// **With explicit dependencies**: `depends_on = "step"` makes the step wait for the
/// named step to complete. Multiple steps with the same dependency run in parallel.
///
/// **Sequential execution**: To make steps run sequentially, explicitly declare each
/// dependency: `depends_on = "previous_step"`.
///
/// # Input Wiring
///
/// The `inputs` attribute enables automatic data flow between steps:
/// ```ignore
/// #[step]
/// async fn get_customer(self: Arc<Self>) -> Customer { ... }
///
/// // Before: Had to duplicate step name in depends_on and inputs
/// // #[step(depends_on = "get_customer", inputs(customer = "get_customer"))]
///
/// // Now: inputs automatically adds get_customer to dependencies!
/// #[step(inputs(customer = "get_customer"))]
/// async fn process_order(self: Arc<Self>, customer: Customer) -> OrderResult {
///     // customer is auto-deserialized from get_customer's output
/// }
/// ```
///
/// **Auto-Dependency Extraction**: Steps referenced in `inputs()` are automatically
/// added to the dependencies list. No need to duplicate in `depends_on`!
///
/// - Parameters in `inputs()` are deserialized from dependency outputs
/// - Referenced steps are automatically added to dependencies
/// - Other parameters are passed to the register function
///
/// # Generated Code
///
/// For a method `foo(self: Arc<Self>, x: X) -> T`, generates:
/// - `register_foo(&self, registry, x) -> StepHandle<T>` - Register with DAG
/// - `foo(self: Arc<Self>, x) -> T` - Original method for direct execution (with full instrumentation)
pub fn step_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse step arguments
    let mut args = DagStepArgs::default();

    if !attr.is_empty() {
        let attr_parser = syn::meta::parser(|meta| args.parse_meta(meta));

        if let Err(err) = syn::parse::Parser::parse(attr_parser, attr) {
            return err.to_compile_error().into();
        }
    }

    let input = parse_macro_input!(item as ItemFn);

    let vis = &input.vis;
    let sig = &input.sig;
    let block = &input.block;
    let attrs = &input.attrs;
    let fn_name = &sig.ident;

    // Check if function is async
    if sig.asyncness.is_none() {
        return syn::Error::new_spanned(sig, "#[step] can only be applied to async functions")
            .to_compile_error()
            .into();
    }

    // Check receiver type and determine if we have Arc<Self>
    let mut has_arc_self = false;
    let mut has_invalid_self = false;

    for arg in &sig.inputs {
        if let FnArg::Receiver(receiver) = arg {
            // Check if this is `self: Arc<Self>`
            if receiver.colon_token.is_some() {
                // Has explicit type: self: Type
                let ty = &receiver.ty;
                // Check if type is Arc<Self>
                if let Type::Path(type_path) = &**ty {
                    if let Some(segment) = type_path.path.segments.last() {
                        if segment.ident == "Arc" {
                            has_arc_self = true;
                        } else {
                            has_invalid_self = true;
                        }
                    } else {
                        has_invalid_self = true;
                    }
                } else {
                    has_invalid_self = true;
                }
            } else {
                // Plain &self or &mut self (no explicit type)
                has_invalid_self = true;
            }
        }
    }

    if has_invalid_self {
        return syn::Error::new_spanned(
            sig,
            "#[step] requires `self: Arc<Self>` instead of `&self` or `&mut self`. \
             Due to Rust's ownership rules, the closure needs to be 'static. \
             Use `self: Arc<Self>` as the receiver type to enable method-style calls. \
             Example: `async fn step(self: Arc<Self>) -> T`",
        )
        .to_compile_error()
        .into();
    }

    // Calculate delay
    let delay_ms = args.delay_ms();
    let delay_duration = if delay_ms > 0 {
        quote! { Some(std::time::Duration::from_millis(#delay_ms as u64)) }
    } else {
        quote! { None }
    };

    // Steps don't have retry policies - only flows do
    // Retry policy is stored at the flow level (step 0)
    let retry_policy_expr = quote! { None };

    // Extract return type
    let return_type = match &sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    // Check if return type is Result
    let returns_result = is_result_type(&sig.output);
    let cache_errors = args.cache_errors;

    // Step name and method signature
    let step_name_str = fn_name.to_string();
    let method_name_str = build_method_signature(sig);

    // Build dependencies array
    // Auto-include steps from 'inputs' as dependencies
    let mut all_deps: Vec<String> = args.depends_on.clone();

    // Add input step names to dependencies (if not already present)
    for input_step in args.inputs.values() {
        if !all_deps.contains(input_step) {
            all_deps.push(input_step.clone());
        }
    }

    // Build dependencies array - always use register() with explicit deps
    let deps_array = if all_deps.is_empty() {
        quote! { &[] }
    } else {
        quote! { &[#(#all_deps),*] }
    };

    // Generate step registration code for dependency graph
    let step_registration_code = if all_deps.is_empty() {
        quote! {
            let _ = __ctx.register_step(#step_name_str, &[]);
        }
    } else {
        quote! {
            let _ = __ctx.register_step(#step_name_str, &[#(#all_deps),*]);
        }
    };

    // Generate completion logging code with autoref specialization
    let log_completion_code = if cache_errors || !returns_result {
        quote! {
            let _ = __ctx.log_step_completion(__step, &__result).await;

            // If it's an error, also update the is_retryable flag
            if let Err(ref __e) = __result {
                #[allow(unused_imports)]
                use ::ergon::kind::*;
                let __is_retryable = (__e).error_kind().is_retryable(__e);
                let _ = __ctx.update_step_retryability(__step, __is_retryable).await;
            }
        }
    } else {
        quote! {
            {
                #[allow(unused_imports)]
                use ::ergon::kind::*;

                // Compute is_retryable and should_cache once
                let (__should_cache, __is_retryable_opt) = match __result.as_ref().err() {
                    Some(__e) => {
                        let __is_retryable = (__e).error_kind().is_retryable(__e);
                        (!__is_retryable, Some(__is_retryable)) // should_cache = !is_retryable
                    }
                    None => (true, None), // Ok result: cache it, no retryability
                };

                if __should_cache {
                    let _ = __ctx.log_step_completion(__step, &__result).await;

                    // If it's an error, update the is_retryable flag using the pre-computed value
                    if let Some(__is_retryable) = __is_retryable_opt {
                        let _ = __ctx.update_step_retryability(__step, __is_retryable).await;
                    }
                }
            }
        }
    };

    // Extract all parameters (no self)
    let params: Vec<_> = sig
        .inputs
        .iter()
        .filter_map(|arg| match arg {
            FnArg::Typed(pt) => Some(pt),
            _ => None,
        })
        .collect();

    // Generate parameter names
    let param_names: Vec<_> = params
        .iter()
        .filter_map(|pt| {
            if let Pat::Ident(pat_ident) = &*pt.pat {
                Some(&pat_ident.ident)
            } else {
                None
            }
        })
        .collect();

    // Separate parameters into two groups:
    // 1. params_from_inputs: Will be deserialized from dependency outputs
    // 2. params_from_register: Will be passed to the register function
    let mut params_from_inputs = Vec::new(); // (name, type, step_name)
    let mut params_from_register = Vec::new(); // (PatType)

    for (param, name) in params.iter().zip(param_names.iter()) {
        let name_str = name.to_string();
        if let Some(step_name) = args.inputs.get(&name_str) {
            // This parameter comes from a dependency output
            let ty = &param.ty;
            params_from_inputs.push((name, ty, step_name.clone()));
        } else {
            // This parameter is passed to register function
            params_from_register.push(*param);
        }
    }

    // Names for register parameters
    let register_param_names: Vec<_> = params_from_register
        .iter()
        .filter_map(|pt| {
            if let Pat::Ident(pat_ident) = &*pt.pat {
                Some(&pat_ident.ident)
            } else {
                None
            }
        })
        .collect();

    // Extract components from params_from_inputs for easier iteration in quote!
    let input_param_names: Vec<_> = params_from_inputs.iter().map(|(name, _, _)| name).collect();
    let input_param_types: Vec<_> = params_from_inputs.iter().map(|(_, ty, _)| ty).collect();
    let input_step_names: Vec<_> = params_from_inputs.iter().map(|(_, _, step)| step).collect();

    // Registration function name
    let register_fn_name = syn::Ident::new(&format!("register_{}", fn_name), fn_name.span());

    // Internal function name for direct execution
    let internal_fn_name = syn::Ident::new(&format!("__internal_{}", fn_name), fn_name.span());

    // Build original function parameters (including Arc<Self> if present)
    let original_fn_params = if has_arc_self {
        quote! { self: std::sync::Arc<Self>, #(#params),* }
    } else {
        quote! { #(#params),* }
    };

    // Build call arguments for original function
    let original_fn_call_args = if has_arc_self {
        quote! { self, #(#param_names),* }
    } else {
        quote! { #(#param_names),* }
    };

    // Create internal DAG executor function that takes Arc<Self> as a regular parameter
    // This allows the user's block to reference self naturally
    let dag_executor_fn_name = syn::Ident::new(&format!("__dag_exec_{}", fn_name), fn_name.span());

    // Build register function signature and executor call based on whether we have Arc<Self>
    let (register_sig, arc_setup_code, executor_call) = if has_arc_self {
        (
            quote! {
                #vis fn #register_fn_name(
                    self: &std::sync::Arc<Self>,
                    registry: &mut ergon::DeferredRegistry,
                    #(#params_from_register),*
                ) -> ergon::StepHandle<#return_type>
            },
            quote! {
                // Clone Arc for the factory closure
                let __arc_self = std::sync::Arc::clone(self);
            },
            quote! {
                Self::#dag_executor_fn_name(std::sync::Arc::clone(&__arc_self), #(#param_names.clone()),*)
            },
        )
    } else {
        (
            quote! {
                #vis fn #register_fn_name(
                    registry: &mut ergon::DeferredRegistry,
                    #(#params_from_register),*
                ) -> ergon::StepHandle<#return_type>
            },
            quote! {},
            quote! {
                Self::#dag_executor_fn_name(#(#param_names.clone()),*)
            },
        )
    };

    // Generate result handling code based on whether return type is Result
    // This matches the behavior of regular #[step] macro
    let result_handling_code = if returns_result {
        if cache_errors {
            // cache_errors: Always cache errors, return Ok with serialized error
            quote! {
                // Log completion (errors are cached)
                let _ = __ctx.log_step_completion(__step, &__result).await;
                Ok(__result)
            }
        } else {
            // Default or RetryableError: Check if error should be cached
            quote! {
                match &__result {
                    Ok(_) => {
                        // Success: log completion and return
                        let _ = __ctx.log_step_completion(__step, &__result).await;
                        Ok(__result)
                    }
                    Err(__e) => {
                        // Error: check if it's retryable using autoref specialization
                        #[allow(unused_imports)]
                        use ::ergon::kind::*;

                        let __should_cache = (__e).error_kind().should_cache(__e);

                        if __should_cache {
                            // Permanent error: cache it
                            let _ = __ctx.log_step_completion(__step, &__result).await;
                            Ok(__result)
                        } else {
                            // Transient error (or Suspend): DON'T cache, stop execution
                            // All framework decisions (suspend/retry) already made on original type
                            // This conversion just preserves error message for logging/debugging
                            Err(ergon::ExecutionError::Failed(
                                format!("Step {} failed (retryable): {:?}", #method_name_str, __e)
                            ))
                        }
                    }
                }
            }
        }
    } else {
        // Non-Result type: always log completion
        quote! {
            let _ = __ctx.log_step_completion(__step, &__result).await;
            Ok(__result)
        }
    };

    // Generate result handling for fallback (no context) case
    // Without context, we can't cache errors, so just return the result
    let fallback_result_handling = quote! {
        Ok(__result)
    };

    // Build params tuple for serialization
    // Include self when has_arc_self is true to capture struct fields in the hash
    // Use &* to dereference Arc and take a reference to the inner value
    // Include BOTH register parameters and input parameters (now in scope after deserialization)
    let params_tuple = if has_arc_self {
        quote! { ( &*__arc_self, #(&#register_param_names,)* #(&#input_param_names,)* ) }
    } else {
        quote! { ( #(&#register_param_names,)* #(&#input_param_names,)* ) }
    };

    // Build parameters for the executor function
    let executor_params = if has_arc_self {
        quote! { self: std::sync::Arc<Self>, #(#params),* }
    } else {
        quote! { #(#params),* }
    };

    // Generate the expanded code
    let expanded = quote! {
        /// Internal executor function that runs the user's block
        /// This function takes Arc<Self> (or no self) and parameters, allowing the user's
        /// code to reference self naturally
        async fn #dag_executor_fn_name(#executor_params) -> #return_type {
            #block
        }

        /// Registers this step with the DAG executor for parallel execution.
        /// Returns a StepHandle that will be resolved when the step completes.
        ///
        /// The step will be executed with full instrumentation:
        /// - Execution context integration
        /// - Cache checking with non-determinism detection
        /// - Invocation logging
        /// - Delay support
        /// - Autoref specialization for RetryableError
        #(#attrs)*
        #register_sig
        where
            #return_type: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
        {
            // Clone Arc<Self> if present
            #arc_setup_code

            // Clone register parameters for the closure (not input parameters - those come from dependencies)
            #(let #register_param_names = #register_param_names.clone();)*

            registry.register::<#return_type, _, _>(
                #step_name_str,
                #deps_array,
                move |__dep_inputs| {
                    // Clone Arc and register parameters into the async block
                    #(let #register_param_names = #register_param_names.clone();)*

                    async move {
                        // Deserialize input parameters from dependency outputs
                        // Note: Steps return Result<T, E> which gets serialized.
                        // We deserialize as Result<T, String> then unwrap to get T.
                        #(
                            let #input_param_names: #input_param_types = {
                                let __step_id = ergon::StepId::new(#input_step_names);
                                let __bytes = __dep_inputs.get(&__step_id)
                                    .ok_or_else(|| ergon::ExecutionError::Failed(
                                        format!("Missing dependency output for step: {}", #input_step_names)
                                    ))?;
                                // Try to deserialize as Result<T, String> first (steps return Result)
                                let __result: std::result::Result<#input_param_types, String> = ergon::deserialize_value(__bytes)
                                    .map_err(|e| ergon::ExecutionError::Core(e.to_string()))?;
                                // Extract the Ok value (step must have succeeded to be cached)
                                __result.map_err(|e| ergon::ExecutionError::Failed(
                                    format!("Dependency step {} returned error: {}", #input_step_names, e)
                                ))?
                            };
                        )*

                        // Try to get execution context
                        match ergon::EXECUTION_CONTEXT.try_with(|ctx| ctx.clone()) {
                            Ok(__ctx) => {
                                // Serialize parameters first - needed for step ID hash
                                let __params = #params_tuple;

                                // For DAG steps, use a stable step ID derived from step name AND parameters
                                // This ensures:
                                // 1. Consistent IDs across executions (hash-based, not counter-based)
                                // 2. Unique IDs when same step called with different parameters
                                // 3. Stable child flow step IDs (derived from stable parent step ID)
                                //
                                // Uses seahash for stable hashing across Rust versions.
                                // Masking with 0x7FFFFFFF ensures positive i32 without the i32::MIN.abs() bug.
                                let __step = {
                                    let __params_bytes = ergon::serialize_value(&__params)
                                        .expect("Failed to serialize step parameters");
                                    let mut __hash_input = #step_name_str.as_bytes().to_vec();
                                    __hash_input.extend_from_slice(&__params_bytes);
                                    (seahash::hash(&__hash_input) & 0x7FFFFFFF) as i32
                                };
                                let __class_name = "Step";

                                // Set enclosing step so invoke().result() and timers/signals know their parent
                                __ctx.set_enclosing_step(__step);

                                // Check cache first
                                match __ctx.get_cached_result::<#return_type, _>(
                                    __step, __class_name, #method_name_str, &__params
                                ).await {
                                    Ok(Some(__cached_result)) => {
                                        return Ok(__cached_result);
                                    }
                                    Ok(None) => {
                                        // No cached result, continue with execution
                                    }
                                    Err(__e) => {
                                        panic!("Step execution error at step {}: {}", __step, __e);
                                    }
                                }

                                // Log invocation start
                                let _ = __ctx.log_step_start(
                                    ergon::executor::LogStepStartParams {
                                        step: __step,
                                        class_name: __class_name,
                                        method_name: #method_name_str,
                                        delay: #delay_duration,
                                        status: ergon::InvocationStatus::Pending,
                                        params: &__params,
                                        retry_policy: #retry_policy_expr,
                                    }
                                ).await;

                                // Handle delay if specified
                                if let Some(__delay) = #delay_duration {
                                    tokio::time::sleep(__delay).await;
                                }

                                // Execute the step logic via executor function
                                let __result: #return_type = #executor_call.await;

                                // Apply result handling logic with autoref specialization
                                // This will log completion for Ok or permanent errors,
                                // and return Err for retryable errors
                                #result_handling_code
                            }
                            Err(_) => {
                                // Direct execution without context - execute via executor and check errors
                                let __result: #return_type = #executor_call.await;

                                // Apply fallback result handling (no logging)
                                #fallback_result_handling
                            }
                        }
                    }
                },
            )
        }

        /// The original step function for direct execution (with full instrumentation).
        ///
        /// This is the same as using #[step] - provides:
        /// - Execution context integration
        /// - Cache checking with non-determinism detection
        /// - Invocation logging
        /// - Delay support
        /// - Autoref specialization for RetryableError
        #(#attrs)*
        #vis fn #fn_name(#original_fn_params) -> ergon::StepFuture<impl std::future::Future<Output = Option<#return_type>>> {
            ergon::StepFuture::new(Self::#internal_fn_name(#original_fn_call_args))
        }

        /// Internal implementation with full instrumentation
        async fn #internal_fn_name(#original_fn_params) -> Option<#return_type> {
            // Try to get execution context
            match ergon::EXECUTION_CONTEXT.try_with(|ctx| ctx.clone()) {
                Ok(__ctx) => {
                    // Check call type
                    let __call_type = ergon::CALL_TYPE.try_with(|ct| *ct)
                        .unwrap_or(ergon::CallType::Run);

                    // Register step with dependency graph
                    #step_registration_code

                    // Serialize parameters first - needed for step ID hash
                    let __params = ( #(&#param_names,)* );

                    // Get stable step ID using hash of both method name AND parameters
                    // This ensures:
                    // 1. Consistent IDs across replays (hash-based, not counter-based)
                    // 2. Unique IDs when same step called with different parameters
                    // 3. Stable child flow step IDs (derived from stable parent step ID)
                    //
                    // Uses seahash for stable hashing across Rust versions.
                    // Masking with 0x7FFFFFFF ensures positive i32 without the i32::MIN.abs() bug.
                    let __step = {
                        let __params_bytes = ergon::serialize_value(&__params)
                            .expect("Failed to serialize step parameters");
                        let mut __hash_input = #step_name_str.as_bytes().to_vec();
                        __hash_input.extend_from_slice(&__params_bytes);
                        (seahash::hash(&__hash_input) & 0x7FFFFFFF) as i32
                    };
                    let __class_name = "Step";

                    // Set enclosing step so invoke().result() and timers/signals know their parent
                    __ctx.set_enclosing_step(__step);

                    // NEW: Check if this step has a pending child invocation
                    // This handles the replay case where:
                    // 1. Step called invoke().result() and suspended (WaitingForSignal)
                    // 2. Child completed and signaled the parent
                    // 3. Flow is being replayed - we need to skip re-executing the body
                    //    to avoid running side effects twice
                    if let Ok(Some(__child_step)) = __ctx.storage().get_child_step_for_parent(__ctx.flow_id(), __step).await {
                        // Check if the child's signal arrived
                        if let Ok(Some(_)) = __ctx.storage().get_signal_params(__ctx.flow_id(), __child_step).await {
                            // Child completed! Check if this step has cached result
                            match __ctx.get_cached_result::<#return_type, _>(
                                __step, __class_name, #method_name_str, &__params
                            ).await {
                                Ok(Some(__cached_result)) => {
                                    // Step was completed after child signaled, return cached result
                                    return Some(__cached_result);
                                }
                                Ok(None) | Err(_) => {
                                    // No cached result yet, continue to execute body
                                    // This happens when child signaled but step hasn't completed yet
                                }
                            }
                        }
                    }

                    // Handle AWAIT call type
                    if matches!(__call_type, ergon::CallType::Await) {
                        let _ = __ctx.log_step_start(
                            ergon::executor::LogStepStartParams {
                                step: __step,
                                class_name: __class_name,
                                method_name: #method_name_str,
                                delay: None,
                                status: ergon::InvocationStatus::WaitingForSignal,
                                params: &__params,
                                retry_policy: #retry_policy_expr,
                            }
                        ).await;
                        return None;
                    }

                    // Check cache (unless RESUME mode)
                    if !matches!(__call_type, ergon::CallType::Resume) {
                        match __ctx.get_cached_result::<#return_type, _>(
                            __step, __class_name, #method_name_str, &__params
                        ).await {
                            Ok(Some(__cached_result)) => {
                                return Some(__cached_result);
                            }
                            Ok(None) => {
                                // No cache, continue
                            }
                            Err(__e) => {
                                panic!("Step execution error at step {}: {}", __step, __e);
                            }
                        }
                    }

                    // Log invocation start
                    let _ = __ctx.log_step_start(
                        ergon::executor::LogStepStartParams {
                            step: __step,
                            class_name: __class_name,
                            method_name: #method_name_str,
                            delay: #delay_duration,
                            status: ergon::InvocationStatus::Pending,
                            params: &__params,
                            retry_policy: #retry_policy_expr,
                        }
                    ).await;

                    // Handle delay
                    if let Some(__delay) = #delay_duration {
                        tokio::time::sleep(__delay).await;
                    }

                                    // Execute the step logic (with automatic retry if policy is set)
                    let __result: #return_type = (async { #block }).await;

                    // Log completion conditionally
                    // For Result types, check if error should be cached
                    #log_completion_code

                    // Return Some to indicate result should be used
                    // (Factory will check if it was actually logged)
                    Some(__result)
                }
                Err(_) => {
                    // Direct execution without context
                    Some((async { #block }).await)
                }
            }
        }
    };

    TokenStream::from(expanded)
}
