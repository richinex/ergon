use ergon::prelude::*;
use std::sync::Arc;

#[derive(FlowType)]
struct TestFlow {
    value: i32,
}

impl TestFlow {
    #[flow]
    fn not_async_flow(self: Arc<Self>) -> Result<i32, ExecutionError> {
        Ok(self.value)
    }
}

fn main() {}
