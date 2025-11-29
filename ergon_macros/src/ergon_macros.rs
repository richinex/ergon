//! DAG-based Execution Macros with Sequential-by-Default Behavior
//!
//! This module provides `#[step]` and `#[flow]` macros that enable
//! both sequential and parallel execution of steps based on their dependencies.
//!
//! ## Execution Model
//!
//! **Sequential by Default**: Steps without explicit `depends_on` execute in
//! registration order, automatically chaining to the previous step.
//!
//! **Explicit Parallelism**: Use `depends_on` to specify exact dependencies,
//! enabling parallel execution when multiple steps depend on the same parent.
//!
//! ## Features:
//! - Sequential execution by default (safe and intuitive)
//! - Opt-in parallelism via explicit `depends_on` declarations
//! - Execution context integration
//! - Step registration with dependency graph
//! - Parameter serialization and `inputs` data wiring
//! - Cache check with non-determinism detection
//! - Invocation logging (start and completion)
//! - Delay support (`delay`, `unit` attributes)
//! - Autoref specialization for RetryableError
//! - Await/Resume support for external signals
//!
//! # Using Arc<Self> for Method-Style Calls
//!
//! Due to Rust's ownership rules with `tokio::spawn`, `#[step]` methods must use
//! `self: Arc<Self>` instead of `&self` to enable spawning on separate threads. This is
//! the standard Rust pattern for async methods that need 'static lifetime.
//!
//! Benefits of Arc<Self>:
//! - Clean method-style registration: `self.register_foo(&mut registry)`
//! - Access to struct fields naturally: `self.customer_id`
//! - True multi-threaded parallelism via tokio::spawn
//! - Standard Rust async pattern (used throughout tokio ecosystem)
//!
//! # Example: Sequential by Default
//!
//! ```ignore
//! struct OrderFlow { customer_id: String }
//!
//! impl OrderFlow {
//!     // Step 1: No depends_on - runs first
//!     #[step]
//!     async fn get_customer(self: Arc<Self>) -> Customer {
//!         Customer { id: self.customer_id.clone(), name: "John".into() }
//!     }
//!
//!     // Step 2: No depends_on - auto-depends on get_customer (sequential)
//!     #[step]
//!     async fn validate_order(self: Arc<Self>) -> bool {
//!         true
//!     }
//!
//!     // Step 3: No depends_on - auto-depends on validate_order (sequential)
//!     #[step]
//!     async fn process_payment(self: Arc<Self>) -> bool {
//!         true
//!     }
//!
//!     #[flow]
//!     async fn process(self: Arc<Self>) -> OrderResult {
//!         // Steps execute sequentially in registration order
//!         self.register_get_customer();
//!         self.register_validate_order();    // Waits for get_customer
//!         self.register_process_payment()    // Waits for validate_order
//!     }
//! }
//! ```
//!
//! # Example: Explicit Parallelism
//!
//! ```ignore
//! impl OrderFlow {
//!     #[step]
//!     async fn get_customer(self: Arc<Self>) -> Customer { ... }
//!
//!     // Explicit depends_on: runs after get_customer
//!     #[step(depends_on = "get_customer")]
//!     async fn validate_payment(self: Arc<Self>) -> bool { true }
//!
//!     // ALSO depends on get_customer - runs in PARALLEL with validate_payment!
//!     #[step(depends_on = "get_customer")]
//!     async fn check_inventory(self: Arc<Self>) -> bool { true }
//!
//!     // Depends on both - waits for parallel steps to complete
//!     #[step(depends_on = ["validate_payment", "check_inventory"])]
//!     async fn finalize(self: Arc<Self>) -> OrderResult { ... }
//!
//!     #[flow]
//!     async fn process(self: Arc<Self>) -> OrderResult {
//!         self.register_get_customer();
//!         self.register_validate_payment();  // Parallel with check_inventory
//!         self.register_check_inventory();   // Parallel with validate_payment
//!         self.register_finalize()           // Waits for both
//!     }
//! }
//! ```

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Block, Expr, FnArg, ItemFn, Pat, PatType, ReturnType, Stmt, Type};

use crate::{build_method_signature, is_result_type};

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
fn try_wrap_dag_registrations(block: &Block, _return_type: &TokenStream2) -> TokenStream2 {
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

/// Arguments for the #[dag_step] attribute macro
/// Supports all the same attributes as #[step] plus depends_on and inputs
#[derive(Default)]
pub(crate) struct DagStepArgs {
    /// Delay value (in units specified by `unit`)
    pub delay: Option<i64>,
    /// Time unit for delay (MILLIS, SECONDS, MINUTES, HOURS)
    pub unit: Option<String>,
    /// When true, Err results are cached like Ok results
    pub cache_errors: bool,
    /// List of step names this step depends on
    pub depends_on: Vec<String>,
    /// Input mappings: parameter name -> step name
    /// e.g., inputs(customer = "get_customer", payment = "validate_payment")
    pub inputs: std::collections::HashMap<String, String>,
    /// Track if depends_on was explicitly set to [] (to disable auto-chaining)
    pub has_explicit_empty_depends_on: bool,
}


impl DagStepArgs {
    /// Parse a single attribute using ParseNestedMeta
    pub fn parse_meta(&mut self, meta: syn::meta::ParseNestedMeta) -> syn::Result<()> {
        if meta.path.is_ident("delay") {
            self.delay = Some(meta.value()?.parse::<syn::LitInt>()?.base10_parse()?);
            Ok(())
        } else if meta.path.is_ident("unit") {
            self.unit = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            Ok(())
        } else if meta.path.is_ident("cache_errors") {
            self.cache_errors = true;
            Ok(())
        } else if meta.path.is_ident("depends_on") {
            let value = meta.value()?;

            if value.peek(syn::token::Bracket) {
                let content;
                syn::bracketed!(content in value);
                let deps: syn::punctuated::Punctuated<syn::LitStr, syn::Token![,]> =
                    content.parse_terminated(|input| input.parse(), syn::Token![,])?;

                // Check if it's explicitly empty
                if deps.is_empty() {
                    self.has_explicit_empty_depends_on = true;
                } else {
                    for dep in deps {
                        self.depends_on.push(dep.value());
                    }
                }
            } else {
                let dep: syn::LitStr = value.parse()?;
                self.depends_on.push(dep.value());
            }
            Ok(())
        } else if meta.path.is_ident("inputs") {
            // Parse inputs(param = "step", param2 = "step2")
            meta.parse_nested_meta(|nested| {
                let param_name = nested
                    .path
                    .get_ident()
                    .ok_or_else(|| nested.error("expected parameter name"))?
                    .to_string();
                let step_name: syn::LitStr = nested.value()?.parse()?;
                self.inputs.insert(param_name, step_name.value());
                Ok(())
            })?;
            Ok(())
        } else {
            Err(meta.error("expected `delay`, `unit`, `cache_errors`, `depends_on`, or `inputs`"))
        }
    }

    /// Calculate delay in milliseconds
    pub fn delay_ms(&self) -> i64 {
        if let Some(delay_value) = self.delay {
            let unit_str = self.unit.as_deref().unwrap_or("SECONDS");
            match unit_str {
                "MILLIS" => delay_value,
                "SECONDS" => delay_value * 1000,
                "MINUTES" => delay_value * 60 * 1000,
                "HOURS" => delay_value * 60 * 60 * 1000,
                _ => delay_value * 1000,
            }
        } else {
            0
        }
    }
}

/// The `#[step]` macro marks an async function as a step in a workflow.
///
/// Steps execute **sequentially by default** based on registration order.
/// Use explicit `depends_on` to enable parallel execution.
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
/// **Without `depends_on`**: Step auto-depends on the previously registered step
/// (sequential execution).
///
/// **With `depends_on`**: Step depends on explicitly named steps. Multiple steps
/// with the same dependency run in parallel.
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
pub fn dag_step_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse dag_step arguments
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
        return syn::Error::new_spanned(sig, "#[dag_step] can only be applied to async functions")
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
            "#[dag_step] requires `self: Arc<Self>` instead of `&self` or `&mut self`. \
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

    // Track if depends_on was explicitly set to empty (to disable auto-chain)
    let has_explicit_empty_depends_on = args.has_explicit_empty_depends_on;

    // Add input step names to dependencies (if not already present)
    for input_step in args.inputs.values() {
        if !all_deps.contains(input_step) {
            all_deps.push(input_step.clone());
        }
    }

    let deps_array = if all_deps.is_empty() && has_explicit_empty_depends_on {
        // Explicit depends_on = [] -> disable auto-chaining
        quote! { &["__NO_AUTO_CHAIN__"] }
    } else if all_deps.is_empty() {
        // No depends_on specified -> allow auto-chaining
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
                            // Transient error: DON'T cache, return Err so step retries
                            // Step remains PENDING, will retry on next execution
                            Err(ergon::ExecutionError::Failed(
                                format!("Step {} returned retryable error (will retry)", #method_name_str)
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
    // Same logic but without logging
    let fallback_result_handling = if returns_result {
        if cache_errors {
            quote! {
                // cache_errors: Always return Ok (can't log without context)
                Ok(__result)
            }
        } else {
            quote! {
                match &__result {
                    Ok(_) => Ok(__result),
                    Err(__e) => {
                        #[allow(unused_imports)]
                        use ::ergon::kind::*;

                        let __should_cache = (__e).error_kind().should_cache(__e);

                        if __should_cache {
                            Ok(__result)
                        } else {
                            Err(ergon::ExecutionError::Failed(
                                format!("Step {} returned retryable error (will retry)", #method_name_str)
                            ))
                        }
                    }
                }
            }
        }
    } else {
        quote! {
            Ok(__result)
        }
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
            #return_type: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + 'static,
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
                                    .map_err(ergon::ExecutionError::Core)?;
                                // Extract the Ok value (step must have succeeded to be cached)
                                __result.map_err(|e| ergon::ExecutionError::Failed(
                                    format!("Dependency step {} returned error: {}", #input_step_names, e)
                                ))?
                            };
                        )*

                        // Try to get execution context
                        match ergon::EXECUTION_CONTEXT.try_with(|ctx| ctx.clone()) {
                            Ok(__ctx) => {
                                // For DAG steps, use a stable step ID derived from the step name
                                // This ensures consistent IDs across executions even when parallel steps
                                // execute in different orders
                                let __step = {
                                    use std::collections::hash_map::DefaultHasher;
                                    use std::hash::{Hash, Hasher};
                                    let mut hasher = DefaultHasher::new();
                                    #step_name_str.hash(&mut hasher);
                                    (hasher.finish() as i32).abs()
                                };
                                let __class_name = "DagStep";

                                // Serialize parameters for cache check (includes self if present)
                                let __params = #params_tuple;

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
                                    __step,
                                    __class_name,
                                    #method_name_str,
                                    #delay_duration,
                                    ergon::InvocationStatus::Pending,
                                    &__params
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

                    // Get step number
                    let __step = __ctx.next_step();
                    let __class_name = "DagStep";

                    // Serialize parameters for caching
                    let __params = ( #(&#param_names,)* );

                    // Handle AWAIT call type
                    if matches!(__call_type, ergon::CallType::Await) {
                        let _ = __ctx.log_step_start(
                            __step,
                            __class_name,
                            #method_name_str,
                            None,
                            ergon::InvocationStatus::WaitingForSignal,
                            &__params
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
                        __step,
                        __class_name,
                        #method_name_str,
                        #delay_duration,
                        ergon::InvocationStatus::Pending,
                        &__params
                    ).await;

                    // Handle delay
                    if let Some(__delay) = #delay_duration {
                        tokio::time::sleep(__delay).await;
                    }

                    // Execute the step logic
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

/// The `#[dag_flow]` macro marks an async method as a DAG flow orchestrator.
///
/// This macro provides ALL the features of `#[flow]` - execution context,
/// caching, logging, etc. The flow body should create a `DeferredRegistry`,
/// register all steps, execute them, and return the result.
///
/// # Usage with Arc<Self>
///
/// When using `#[dag_step]` methods with `self: Arc<Self>`, the flow method
/// should also use `self: Arc<Self>` for consistency. This enables clean,
/// method-style registration calls.
///
/// # Example
///
/// ```ignore
/// #[dag_flow]
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
pub fn dag_flow_impl(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let vis = &input.vis;
    let sig = &input.sig;
    let block = &input.block;
    let attrs = &input.attrs;

    // Check if function is async
    if sig.asyncness.is_none() {
        return syn::Error::new_spanned(sig, "#[dag_flow] can only be applied to async functions")
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
                        __step,
                        __class_name,
                        #method_name_str,
                        None,
                        ergon::InvocationStatus::Pending,
                        &__params
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
