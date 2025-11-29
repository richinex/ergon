//! Attribute parsing for macro arguments.
//!
//! This module hides the complexity of parsing #[step] and #[flow] attributes,
//! including depends_on, inputs, delay, unit, and cache_errors.
//!
//! It also provides shared utilities for signature analysis.
//!
//! Following Parnas's information hiding principle, this module encapsulates
//! decisions about how macro attributes are parsed and validated.

use quote::quote;
use syn::{FnArg, ReturnType, Signature, Type};

/// Arguments for the #[step] attribute macro
///
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

/// Check if a return type is a Result type (Result<T, E>).
///
/// This is used to determine whether to generate code that conditionally
/// caches errors. By default, Result::Err values are NOT cached to allow
/// retry of transient failures.
pub(crate) fn is_result_type(return_type: &ReturnType) -> bool {
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
///
/// Format: "method_name(Type1, Type2, ...)" for better non-determinism detection.
/// This ensures that methods with the same name but different signatures are distinguishable.
pub(crate) fn build_method_signature(sig: &Signature) -> String {
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
