use ergon::prelude::*;
use ergon::executor::{ExecutionError, Retryable};
use ergon::core::RetryPolicy;

#[derive(FlowType, Serialize, Deserialize)]
struct TestFlow {
    value: i32,
}

impl TestFlow {
    #[flow]
    async fn simple_flow(self: Arc<Self>) -> Result<i32, ExecutionError> {
        Ok(self.value)
    }

    #[flow(retry = RetryPolicy::STANDARD)]
    async fn flow_with_retry(self: Arc<Self>) -> Result<String, ExecutionError> {
        Ok(format!("value: {}", self.value))
    }
}

fn main() {
    println!("Flow macros compile successfully");
}
