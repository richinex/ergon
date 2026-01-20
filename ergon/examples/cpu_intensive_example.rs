//! Demonstrates the difference between blocking CPU work and using spawn_blocking.
//!
//! Blocking CPU work directly in async functions blocks executor threads,
//! preventing other tasks from running. Use tokio::task::spawn_blocking for
//! CPU-intensive work to avoid starving the async runtime.

use ergon::executor::{ExecutionError, Executor, FlowOutcome};
use ergon::prelude::*;
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct CpuFlow {
    mode: String,
}

impl CpuFlow {
    fn blocking_cpu_work(n: u64) -> u64 {
        let mut sum = 0u64;
        for i in 0..n {
            sum = sum.wrapping_add(std::hint::black_box(i * i));
        }
        std::hint::black_box(sum)
    }

    #[step]
    async fn bad_cpu_step1(self: Arc<Self>) -> Result<u64, String> {
        println!("  [BAD] Step 1 starting (blocks executor)");
        let result = Self::blocking_cpu_work(100_000_000);
        println!("  [BAD] Step 1 done");
        Ok(result)
    }

    #[step]
    async fn bad_cpu_step2(self: Arc<Self>) -> Result<u64, String> {
        println!("  [BAD] Step 2 starting (blocks executor)");
        let result = Self::blocking_cpu_work(100_000_000);
        println!("  [BAD] Step 2 done");
        Ok(result)
    }

    #[step]
    async fn bad_cpu_step3(self: Arc<Self>) -> Result<u64, String> {
        println!("  [BAD] Step 3 starting (blocks executor)");
        let result = Self::blocking_cpu_work(100_000_000);
        println!("  [BAD] Step 3 done");
        Ok(result)
    }

    #[step]
    async fn good_cpu_step1(self: Arc<Self>) -> Result<u64, String> {
        println!("  [GOOD] Step 1 starting (spawn_blocking)");
        let result = tokio::task::spawn_blocking(|| Self::blocking_cpu_work(100_000_000))
            .await
            .unwrap();
        println!("  [GOOD] Step 1 done");
        Ok(result)
    }

    #[step]
    async fn good_cpu_step2(self: Arc<Self>) -> Result<u64, String> {
        println!("  [GOOD] Step 2 starting (spawn_blocking)");
        let result = tokio::task::spawn_blocking(|| Self::blocking_cpu_work(100_000_000))
            .await
            .unwrap();
        println!("  [GOOD] Step 2 done");
        Ok(result)
    }

    #[step]
    async fn good_cpu_step3(self: Arc<Self>) -> Result<u64, String> {
        println!("  [GOOD] Step 3 starting (spawn_blocking)");
        let result = tokio::task::spawn_blocking(|| Self::blocking_cpu_work(100_000_000))
            .await
            .unwrap();
        println!("  [GOOD] Step 3 done");
        Ok(result)
    }

    #[flow]
    async fn run_bad(self: Arc<Self>) -> Result<u64, ExecutionError> {
        let r1 = self.clone().bad_cpu_step1().await?;
        let r2 = self.clone().bad_cpu_step2().await?;
        let r3 = self.bad_cpu_step3().await?;
        Ok(r1 + r2 + r3)
    }

    #[flow]
    async fn run_good(self: Arc<Self>) -> Result<u64, ExecutionError> {
        let r1 = self.clone().good_cpu_step1().await?;
        let r2 = self.clone().good_cpu_step2().await?;
        let r3 = self.good_cpu_step3().await?;
        Ok(r1 + r2 + r3)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== CPU-Intensive Work Example ===\n");

    println!("1. BAD: Blocking CPU work (blocks executor threads)\n");

    let storage = Arc::new(InMemoryExecutionLog::new());
    let workflow = Arc::new(CpuFlow { mode: "bad".into() });

    let start = Instant::now();
    let executor = Executor::new(Uuid::new_v4(), workflow.clone(), storage.clone());
    match executor.execute(|w| Box::pin(w.clone().run_bad())).await {
        FlowOutcome::Completed(result) => {
            let elapsed = start.elapsed();
            println!("\n  Result: {:?}", result);
            println!("  Time: {:?}\n", elapsed);
        }
        FlowOutcome::Suspended(reason) => return Err(format!("Suspended: {:?}", reason).into()),
    }

    println!("2. GOOD: Using spawn_blocking (dedicated thread pool)\n");

    storage.reset().await?;
    let workflow = Arc::new(CpuFlow {
        mode: "good".into(),
    });

    let start = Instant::now();
    let executor = Executor::new(Uuid::new_v4(), workflow.clone(), storage.clone());
    match executor.execute(|w| Box::pin(w.clone().run_good())).await {
        FlowOutcome::Completed(result) => {
            let elapsed = start.elapsed();
            println!("\n  Result: {:?}", result);
            println!("  Time: {:?}\n", elapsed);
        }
        FlowOutcome::Suspended(reason) => return Err(format!("Suspended: {:?}", reason).into()),
    }

    println!("3. CONCURRENT FLOWS: BAD (blocking) vs GOOD (spawn_blocking)\n");
    println!("  3a. BAD: 5 concurrent flows (blocking executor)");
    storage.reset().await?;

    let start = Instant::now();
    let mut handles = vec![];

    for i in 0..5 {
        let storage = storage.clone();
        let handle = tokio::spawn(async move {
            let workflow = Arc::new(CpuFlow {
                mode: format!("bad-{}", i),
            });
            let executor = Executor::new(Uuid::new_v4(), workflow.clone(), storage);
            executor.execute(|w| Box::pin(w.clone().run_bad())).await
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await?;
    }

    let elapsed = start.elapsed();
    println!("     Time for 5 flows (BAD): {:?}", elapsed);

    println!("\n  3b. GOOD: 5 concurrent flows (spawn_blocking)");
    storage.reset().await?;

    let start = Instant::now();
    let mut handles = vec![];

    for i in 0..5 {
        let storage = storage.clone();
        let handle = tokio::spawn(async move {
            let workflow = Arc::new(CpuFlow {
                mode: format!("good-{}", i),
            });
            let executor = Executor::new(Uuid::new_v4(), workflow.clone(), storage);
            executor.execute(|w| Box::pin(w.clone().run_good())).await
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await?;
    }

    let elapsed = start.elapsed();
    println!("     Time for 5 flows (GOOD): {:?}", elapsed);

    Ok(())
}
