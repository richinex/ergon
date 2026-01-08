use crate::core::{InvocationStatus, RetryPolicy};
use uuid::Uuid;

/// Parameters for logging the start of a step invocation.
///
/// This struct groups all the parameters needed to log an invocation start,
/// making the API cleaner and more maintainable.
pub struct InvocationStartParams<'a> {
    /// Flow execution ID
    pub id: Uuid,
    /// Step number in the flow
    pub step: i32,
    /// Class name (for compatibility tracking)
    pub class_name: &'a str,
    /// Method name (for compatibility tracking)
    pub method_name: &'a str,
    /// Current invocation status
    pub status: InvocationStatus,
    /// Serialized parameters
    pub parameters: &'a [u8],
    /// Optional retry policy for this step
    pub retry_policy: Option<RetryPolicy>,
}
