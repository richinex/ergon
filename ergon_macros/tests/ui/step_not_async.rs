use ergon::prelude::*;

#[derive(FlowType)]
struct TestFlow {
    value: i32,
}

impl TestFlow {
    #[step]
    fn not_async_step(&self) -> Result<i32, ExecutionError> {
        Ok(self.value)
    }
}

fn main() {}
