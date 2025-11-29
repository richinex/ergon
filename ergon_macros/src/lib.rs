use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, FnArg, ReturnType, Signature, Type};

mod ergon_macros;

/// Check if a return type is a Result type (Result<T, E>).
///
/// This is used to determine whether to generate code that conditionally
/// caches errors. By default, Result::Err values are NOT cached to allow
/// retry of transient failures.
fn is_result_type(return_type: &ReturnType) -> bool {
    match return_type {
        ReturnType::Default => false,
        ReturnType::Type(_, ty) => is_result_type_inner(ty),
    }
}

fn is_result_type_inner(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => {
            // Check if the last segment is "Result"
            if let Some(segment) = type_path.path.segments.last() {
                let ident = segment.ident.to_string();
                // Match both "Result" and full path like "std::result::Result"
                ident == "Result"
            } else {
                false
            }
        }
        // Handle parenthesized types like (Result<T, E>)
        Type::Paren(paren) => is_result_type_inner(&paren.elem),
        // Handle grouped types
        Type::Group(group) => is_result_type_inner(&group.elem),
        _ => false,
    }
}

/// Extract parameter types from a function signature and build a method signature string.
/// Format: "method_name(Type1, Type2, ...)" for better non-determinism detection.
/// This ensures that methods with the same name but different signatures are distinguishable.
fn build_method_signature(sig: &Signature) -> String {
    let fn_name = sig.ident.to_string();

    let param_types: Vec<String> = sig
        .inputs
        .iter()
        .filter_map(|arg| match arg {
            FnArg::Receiver(_) => None, // Skip self
            FnArg::Typed(pat_type) => {
                // Convert the type to a string representation
                let ty = &pat_type.ty;
                Some(quote!(#ty).to_string().replace(" ", ""))
            }
        })
        .collect();

    format!("{}({})", fn_name, param_types.join(","))
}
#[proc_macro_attribute]
pub fn step(attr: TokenStream, item: TokenStream) -> TokenStream {
    ergon_macros::dag_step_impl(attr, item)
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
    ergon_macros::dag_flow_impl(attr, item)
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
/// #[dag_flow]
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
    use syn::{Expr, Token};

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
            "dag! macro requires at least one step registration call"
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

