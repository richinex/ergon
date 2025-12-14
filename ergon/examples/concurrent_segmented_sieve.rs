//! Concurrent Segmented Sieve with Ergon (Corrected)
//!
//! Benchmark Comparison:
//! - Go: Uses Goroutines + Channels + Mutex.
//! - Ergon: Uses Persistent Flows + Task Queue + Scatter/Gather.
//!
//! Scenario:
//! Find primes up to 10,000,000.
//! We split the work into chunks (Segments) and let 8 workers crunch them.
//!
//! Mathematical Fix applied: Sieve marking now starts at max(low, p*p).

use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Notify;

// =============================================================================
// GLOBAL CONFIG
// =============================================================================

const LARGEST_PRIME: u32 = 10_000_000;
const SEGMENT_SIZE: u32 = 1_000_000; // 10 chunks of 1M each

// To track completion
static COMPLETED_SEGMENTS: AtomicUsize = AtomicUsize::new(0);
static TOTAL_PRIMES_FOUND: AtomicUsize = AtomicUsize::new(0);
static DONE_NOTIFIER: std::sync::LazyLock<Arc<Notify>> =
    std::sync::LazyLock::new(|| Arc::new(Notify::new()));

// =============================================================================
// MATH LOGIC (Pure Rust)
// =============================================================================

/// Standard non-concurrent Sieve to get the "Base Primes" up to sqrt(N).
/// Needed to seed the segments.
fn simple_sieve(limit: u32) -> Vec<u32> {
    let mut is_prime = vec![true; (limit + 1) as usize];
    let mut primes = Vec::new();

    for p in 2..=limit {
        if is_prime[p as usize] {
            primes.push(p);
            let mut i = p * p;
            while i <= limit {
                is_prime[i as usize] = false;
                i += p;
            }
        }
    }
    primes
}

// =============================================================================
// ERGON FLOW: The Segment Calculator
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct PrimeSegmentFlow {
    low: u32,
    high: u32,
    base_primes: Vec<u32>, // Passed in so each worker is independent
}

impl PrimeSegmentFlow {
    #[step]
    async fn calculate(self: Arc<Self>) -> Result<usize, String> {
        let mut segment = vec![true; (self.high - self.low) as usize];

        // Sieve Logic adapted for Rust
        for &p in &self.base_primes {
            let p_sq = p * p;
            // Optimization: If p*p is beyond this segment, we can stop checking primes
            if p_sq >= self.high {
                break;
            }

            // Find the first multiple of p >= low
            let mut start = (self.low / p) * p;
            if start < self.low {
                start += p;
            }

            // --- LOGIC FIX ---
            // We must start marking at p*p.
            // 1. Optimization: Multiples < p*p are already handled by smaller primes.
            // 2. Correctness: Ensures we never mark 'p' itself as non-prime (since p*p > p).
            if start < p_sq {
                start = p_sq;
            }

            // Mark multiples
            let mut j = start;
            while j < self.high {
                segment[(j - self.low) as usize] = false;
                j += p;
            }
        }

        // Count primes in this segment
        let mut count = 0;
        for (i, &is_prime) in segment.iter().enumerate() {
            if is_prime {
                // Edge case: 0 and 1 are not prime
                let num = self.low + i as u32;
                if num > 1 {
                    count += 1;
                }
            }
        }

        // Update Global Stats (In a real app, we might write to DB)
        TOTAL_PRIMES_FOUND.fetch_add(count, Ordering::Relaxed);

        // Notify Orchestrator
        let finished = COMPLETED_SEGMENTS.fetch_add(1, Ordering::Relaxed) + 1;

        // Calculate expected number of segments
        let expected_segments = LARGEST_PRIME.div_ceil(SEGMENT_SIZE);
        if finished >= expected_segments as usize {
            DONE_NOTIFIER.notify_one();
        }

        Ok(count)
    }

    #[flow]
    async fn run(self: Arc<Self>) -> Result<(), String> {
        let _count = self.clone().calculate().await?;
        Ok(())
    }
}

// =============================================================================
// MAIN BENCHMARK
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup In-Memory Execution Log (Fastest for Benchmarking)
    let storage = Arc::new(ergon::storage::InMemoryExecutionLog::default());
    let scheduler = Scheduler::new(storage.clone());

    println!("⚡ STARTING ERGON PRIME SIEVE BENCHMARK");
    println!("   Target: {}", LARGEST_PRIME);
    println!("   Segment Size: {}", SEGMENT_SIZE);

    let start_time = Instant::now();

    // 2. Pre-calculate Base Primes (Sequential Step)
    // We need primes up to sqrt(10,000,000) ~= 3162
    let sqrt_limit = (LARGEST_PRIME as f64).sqrt() as u32;
    let base_primes = simple_sieve(sqrt_limit);
    println!("   Base Primes calculated: {}", base_primes.len());

    // 3. Scatter: Schedule Flows
    // This is equivalent to `go primesBetween(...)` in the Go example
    let mut low = 0;
    let mut segments_count = 0;

    while low < LARGEST_PRIME {
        let mut high = low + SEGMENT_SIZE;
        if high > LARGEST_PRIME {
            high = LARGEST_PRIME;
        }

        let flow = PrimeSegmentFlow {
            low,
            high,
            base_primes: base_primes.clone(),
        };

        scheduler.schedule(flow, uuid::Uuid::new_v4()).await?;

        low += SEGMENT_SIZE;
        segments_count += 1;
    }
    println!("   Scheduled {} concurrent segments.", segments_count);

    // 4. Start Workers (Simulating `runtime.NumCPU()` cores)
    // We spin up 8 concurrent workers to consume the segments
    let mut worker_handles = Vec::new();
    for i in 0..8 {
        let s = storage.clone();
        let w = Worker::new(s, format!("worker-{}", i))
            // Super aggressive polling for benchmark.
            // Note: Since we use InMemoryLog with the recent channel fix,
            // the workers will actually be pushed events instantly!
            .with_poll_interval(Duration::from_millis(1));

        w.register(|f: Arc<PrimeSegmentFlow>| f.run()).await;
        worker_handles.push(w.start().await);
    }

    // 5. Gather: Wait for completion
    DONE_NOTIFIER.notified().await;

    let elapsed = start_time.elapsed();

    // 6. Cleanup
    for h in worker_handles {
        h.shutdown().await;
    }

    println!("\n📊 RESULTS");
    println!("-----------------------------");
    println!("Total Time:       {:?}", elapsed);
    println!(
        "Primes Found:     {}",
        TOTAL_PRIMES_FOUND.load(Ordering::Relaxed)
    );

    // Validation
    // Known count for 10,000,000 is 664,579
    let found = TOTAL_PRIMES_FOUND.load(Ordering::Relaxed);
    if found == 664_579 {
        println!("Correctness:      ✅ PASS");
    } else {
        println!("Correctness:      ❌ FAIL (Expected 664,579)");
    }

    Ok(())
}
