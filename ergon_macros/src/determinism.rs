//! Non-determinism detection for workflow steps.
//!
//! Following Parnas's information hiding principle:
//! - This module encapsulates "how we detect non-deterministic operations"
//! - Step/flow macros use the public API without knowing implementation details
//! - Detection strategy can change without affecting callers
//!
//! Uses `syn::visit::Visit` for proper AST traversal (type-safe, accurate spans).

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Block, Expr, ExprCall, ExprMethodCall, ExprPath};

/// Detects non-deterministic operations in function bodies.
///
/// This visitor walks the AST looking for:
/// - Function calls to non-deterministic APIs (Uuid::new_v4, SystemTime::now, etc.)
/// - Method calls on RNGs (gen, gen_range, sample)
pub struct NonDeterminismDetector {
    violations: Vec<(Span, String)>,
}

impl NonDeterminismDetector {
    pub fn new() -> Self {
        Self {
            violations: Vec::new(),
        }
    }

    /// Check if a path matches known non-deterministic functions.
    fn check_path(&mut self, path: &syn::Path, span: Span) {
        let segments: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();

        // Known non-deterministic function patterns
        // Format: (path_suffix, description)
        //
        // Note: Instant::now() is NOT included because it's for performance timing (monotonic clock),
        // not for generating business data. Users should be able to measure step duration.
        let non_det_functions: &[(&[&str], &str)] = &[
            (
                &["Uuid", "new_v4"],
                "Uuid::new_v4() - generates random UUIDs",
            ),
            (
                &["uuid", "Uuid", "new_v4"],
                "uuid::Uuid::new_v4() - generates random UUIDs",
            ),
            (
                &["SystemTime", "now"],
                "SystemTime::now() - returns wall-clock time",
            ),
            (
                &["std", "time", "SystemTime", "now"],
                "std::time::SystemTime::now() - returns wall-clock time",
            ),
            (
                &["Utc", "now"],
                "chrono::Utc::now() - returns current UTC time",
            ),
            (
                &["chrono", "Utc", "now"],
                "chrono::Utc::now() - returns current UTC time",
            ),
            (
                &["Local", "now"],
                "chrono::Local::now() - returns current local time",
            ),
            (
                &["chrono", "Local", "now"],
                "chrono::Local::now() - returns current local time",
            ),
            (
                &["thread_rng"],
                "rand::thread_rng() - creates random number generator",
            ),
            (
                &["rand", "thread_rng"],
                "rand::thread_rng() - creates random number generator",
            ),
            (&["random"], "rand::random() - generates random values"),
            (
                &["rand", "random"],
                "rand::random() - generates random values",
            ),
        ];

        for (pattern_segments, description) in non_det_functions {
            // Convert pattern to Vec<String> for comparison
            let pattern_owned: Vec<String> =
                pattern_segments.iter().map(|s| s.to_string()).collect();
            if segments.ends_with(&pattern_owned) {
                self.violations.push((span, description.to_string()));
                return;
            }
        }
    }
}

impl<'ast> Visit<'ast> for NonDeterminismDetector {
    /// Visit function calls like `Uuid::new_v4()`.
    fn visit_expr_call(&mut self, node: &'ast ExprCall) {
        if let Expr::Path(ExprPath { path, .. }) = &*node.func {
            self.check_path(path, node.func.span());
        }
        // Continue visiting nested expressions
        visit::visit_expr_call(self, node);
    }

    /// Visit method calls like `rng.gen()`, `rng.gen_range()`.
    fn visit_expr_method_call(&mut self, node: &'ast ExprMethodCall) {
        let method_name = node.method.to_string();

        // Random generation methods
        let non_det_methods = [
            ("gen", "RNG gen() method - generates random values"),
            (
                "gen_range",
                "RNG gen_range() method - generates random values in range",
            ),
            ("sample", "RNG sample() method - samples from distribution"),
            ("shuffle", "RNG shuffle() method - shuffles randomly"),
        ];

        for (pattern, description) in &non_det_methods {
            if method_name == *pattern {
                self.violations
                    .push((node.method.span(), description.to_string()));
                return;
            }
        }

        // Continue visiting
        visit::visit_expr_method_call(self, node);
    }
}

/// Check a function block for non-deterministic operations.
///
/// Uses a hybrid approach:
/// 1. AST visitor for precise detection (with exact spans)
/// 2. String-based fallback for macro-hidden violations
///
/// Returns the first violation found, or Ok if the block is deterministic.
///
/// # Usage
/// ```ignore
/// let block = &input.block;
/// if let Err(err) = check_determinism(block) {
///     return err.to_compile_error().into();
/// }
/// ```
pub fn check_determinism(block: &Block) -> Result<(), syn::Error> {
    // Phase 1: AST visitor (precise, with exact spans)
    let mut detector = NonDeterminismDetector::new();
    detector.visit_block(block);

    if let Some((span, description)) = detector.violations.into_iter().next() {
        return Err(syn::Error::new(
            span,
            format!(
                "#[step] functions must be deterministic for replay correctness.\n\
                 Found non-deterministic operation: {}\n\n\
                 Problem: On replay, this will produce different values, breaking determinism.\n\n\
                 Solutions:\n\
                 1. Generate the value BEFORE the step and pass as parameter:\n\
                    let id = Uuid::new_v4();\n\
                    self.my_step(id).await?;  // id is now a deterministic parameter\n\n\
                 2. Use deterministic alternatives (hash of inputs):\n\
                    use std::collections::hash_map::DefaultHasher;\n\
                    use std::hash::{{Hash, Hasher}};\n\
                    let mut hasher = DefaultHasher::new();\n\
                    self.order_id.hash(&mut hasher);\n\
                    let deterministic_id = hasher.finish();\n\n\
                 3. For timestamps, pass time as parameter from flow level:\n\
                    let now = SystemTime::now();\n\
                    self.process_with_timestamp(now).await?;",
                description
            ),
        ));
    }

    // Phase 2: String-based fallback (catches macro-hidden violations)
    // Proc macros can't see inside unexpanded macros like format!(), so we use
    // string matching as a fallback. This is less precise but catches hidden cases.
    use quote::quote;
    let block_str = quote! { #block }.to_string();
    let normalized = block_str.replace(" ", "");

    // String-based fallback patterns (for macro-hidden violations)
    // Note: Instant is NOT included - it's for timing/observability, not business logic
    let string_patterns = [
        ("new_v4", "Uuid::new_v4() - generates random UUIDs (detected via string matching - may be inside a macro)"),
        ("SystemTime", "SystemTime::now() - returns wall-clock time (detected via string matching - may be inside a macro)"),
        ("thread_rng", "rand::thread_rng() - creates RNG (detected via string matching - may be inside a macro)"),
    ];

    for (pattern, description) in &string_patterns {
        let pattern_normalized = pattern.replace(" ", "");
        if normalized.contains(&pattern_normalized) {
            return Err(syn::Error::new_spanned(
                block,
                format!(
                    "#[step] functions must be deterministic for replay correctness.\n\
                     Found non-deterministic operation: {}\n\n\
                     Note: This was detected via string matching because the operation may be\n\
                     hidden inside a macro (like format!()). Proc macros cannot see inside\n\
                     unexpanded macros, so the exact location cannot be pinpointed.\n\n\
                     Problem: On replay, this will produce different values, breaking determinism.\n\n\
                     Solutions:\n\
                     1. Generate the value BEFORE the step and pass as parameter:\n\
                        let id = Uuid::new_v4();\n\
                        self.my_step(id).await?;  // id is now a deterministic parameter\n\n\
                     2. Use deterministic alternatives (hash of inputs):\n\
                        use std::collections::hash_map::DefaultHasher;\n\
                        use std::hash::{{Hash, Hasher}};\n\
                        let mut hasher = DefaultHasher::new();\n\
                        self.order_id.hash(&mut hasher);\n\
                        let deterministic_id = hasher.finish();\n\n\
                     3. For timestamps, pass time as parameter from flow level:\n\
                        let now = SystemTime::now();\n\
                        self.process_with_timestamp(now).await?;",
                    description
                ),
            ));
        }
    }

    Ok(())
}
