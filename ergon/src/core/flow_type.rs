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

    /// Returns parent flow information if this is a child flow.
    ///
    /// This enables automatic parent signaling when the child completes.
    /// Child flows can implement this to specify their parent's ID and
    /// the method name the parent is waiting on.
    ///
    /// # Returns
    ///
    /// - `Some((parent_id, parent_method))` if this is a child flow
    /// - `None` if this is a top-level flow
    ///
    /// # Example
    ///
    /// ```ignore
    /// impl FlowType for InventoryCheckFlow {
    ///     fn type_id() -> &'static str { "InventoryCheckFlow" }
    ///
    ///     fn parent_info(&self) -> Option<(uuid::Uuid, String)> {
    ///         Some((self.parent_flow_id, "await_inventory_check".to_string()))
    ///     }
    /// }
    /// ```
    fn parent_info(&self) -> Option<(uuid::Uuid, String)> {
        None
    }
}

/// Marker trait for flows that can be invoked as child flows using the Level 3 API.
///
/// This trait extends `FlowType` with an associated `Output` type, enabling
/// compile-time type inference when invoking child flows.
///
/// # Example
///
/// ```ignore
/// use ergon::prelude::*;
///
/// #[derive(Clone, Serialize, Deserialize, FlowType)]
/// struct CheckInventory {
///     product_id: String,
/// }
///
/// impl InvokableFlow for CheckInventory {
///     type Output = InventoryStatus;
/// }
///
/// // Parent can invoke with full type inference:
/// let inventory = self.invoke(CheckInventory { product_id }).result().await?;
/// // ^^^^^^^^^^^ Type inferred as InventoryStatus!
/// ```
///
/// # Design
///
/// This trait is separate from `FlowType` because only flows that are invoked
/// as child flows need to declare their output type.
pub trait InvokableFlow: FlowType {
    /// The type returned by this flow's execution.
    ///
    /// This enables type-safe parent-child invocation where the compiler
    /// automatically infers the child's result type.
    type Output: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
}
