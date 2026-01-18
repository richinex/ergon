use ergon::prelude::*;
use ergon::executor::ExecutionError;

#[derive(FlowType, Serialize, Deserialize)]
struct TestFlow {
    value: i32,
}

impl TestFlow {
    #[step]
    async fn simple_step(self: Arc<Self>) -> Result<i32, ExecutionError> {
        Ok(self.value)
    }

    #[step]
    async fn step_with_arc(self: Arc<Self>) -> Result<i32, ExecutionError> {
        Ok(self.value * 2)
    }
}

fn main() {
    println!("Step macros compile successfully");
}
