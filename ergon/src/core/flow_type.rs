//! Stable flow type identification.
//!
//! This module provides the `FlowType` trait for stable, cross-version flow type
//! identification. Unlike `std::any::type_name()`, which is explicitly unstable
//! across compiler versions, `FlowType::type_id()` provides a stable identifier
//! that works reliably in distributed systems.
//!
//! # Problem with std::any::type_name()
//!
//! The Rust docs warn:
//! > The returned string must not be considered to be a unique identifier of a type...
//! > multiple types may map to the same type name, and the type name may change
//! > between compiler versions.
//!
//! This causes issues when:
//! - Scheduler and worker are compiled separately
//! - Different Rust compiler versions are used
//! - Deployment happens incrementally (old worker, new scheduler)
//!
//! # Solution
//!
//! Use `#[derive(FlowType)]` to generate a stable type identifier:
//!
//! ```ignore
//! use ergon::prelude::*;
//!
//! #[derive(FlowType, Serialize, Deserialize)]
//! struct OrderFlow {
//!     order_id: String,
//! }
//!
//! // Type ID is stable: "OrderFlow" (regardless of compiler version)
//! assert_eq!(OrderFlow::type_id(), "OrderFlow");
//! ```
//!
//! # Custom Type IDs
//!
//! Override the default with the `type_id` attribute:
//!
//! ```ignore
//! #[derive(FlowType)]
//! #[flow_type(id = "com.company.OrderFlow.v1")]
//! struct OrderFlow { }
//!
//! assert_eq!(OrderFlow::type_id(), "com.company.OrderFlow.v1");
//! ```

/// Provides a stable type identifier for flow types.
///
/// This trait should be derived using `#[derive(FlowType)]` rather than
/// implemented manually, to ensure consistency.
///
/// # Stability Guarantee
///
/// The type ID returned by `type_id()` is guaranteed to be stable across:
/// - Different Rust compiler versions
/// - Different build configurations
/// - Different crate versions (unless you explicitly change it)
///
/// This makes it safe for distributed execution where scheduler and worker
/// may be compiled separately.
pub trait FlowType {
    /// Returns a stable type identifier for this flow type.
    ///
    /// This identifier is used for:
    /// - Routing flows to the correct worker executor
    /// - Serialization/deserialization metadata
    /// - Monitoring and observability
    ///
    /// # Stability
    ///
    /// This value MUST be stable across compiler versions and builds.
    /// Do not use `std::any::type_name()` or other unstable sources.
    fn type_id() -> &'static str;
}
