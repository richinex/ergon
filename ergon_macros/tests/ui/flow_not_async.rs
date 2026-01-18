use ergon::prelude::*;

#[derive(FlowType)]
struct TestFlow {
    value: i32,
}

impl TestFlow {
    #[flow]
    fn not_async_flow(self) -> Result<i32, ExecutionError> {
        Ok(self.value)
    }
}

fn main() {}
