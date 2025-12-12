# Bug Analysis: Flow Replay on Child Completion

## Summary

**Bug**: Flows are **replaying from the beginning** after child flow completion, instead of **resuming** from the suspension point.

**Severity**: High - causes unnecessary re-execution, confusing logs, and potential side effects

**Scope**: Affects both DAG and sequential flows that use `.invoke().result().await`

## Test Results

### Sequential Version (`test_sequential_multiple_invoke.rs`)

```
[14:39:53.583] FLOW: Starting sequential execution    ← ✅ 1st execution
[14:39:53.584] Step: validate ORD-001
[14:39:53.584] FLOW: Validate complete
[14:39:53.585] Step: invoking Payment child            ← Suspends here
[14:39:53.689]   CHILD: Processing payment $99.99      ← Child executes
[14:39:53.793] FLOW: Starting sequential execution    ← ❌ 2nd execution (BUG!)
[14:39:53.793] FLOW: Validate complete                 ← Returns cached result
[14:39:53.793] Step: invoking Payment child            ← ❌ Re-executes (BUG!)
[14:39:53.795] Step: got payment result: ...           ← Gets cached child result
[14:39:53.795] FLOW: Payment complete
[14:39:53.795] Step: invoking Shipment child           ← Suspends here
[14:39:53.846]   CHILD: Creating shipment               ← Child executes
[14:39:53.952] FLOW: Starting sequential execution    ← ❌ 3rd execution (BUG!)
[14:39:53.952] FLOW: Validate complete                 ← Cached
[14:39:53.953] FLOW: Payment complete                  ← Cached
[14:39:53.953] Step: invoking Shipment child           ← ❌ Re-executes (BUG!)
[14:39:53.954] Step: got shipment result: ...          ← Gets cached child result
[14:39:53.955] FLOW: Ship complete
```

**Pattern**: Flow executes → suspends at child → child completes → **FLOW REPLAYS FROM START**

### DAG Version (`test_dag_multiple_invoke.rs`)

```
[14:39:48.441] Step: invoking Shipment child           ← 1st parallel execution
[14:39:48.441] Step: validate ORD-001                  ← 1st parallel execution
[14:39:48.547]   CHILD: Creating shipment               ← Shipment child executes
[14:39:48.655] Step: invoking Payment child            ← ❌ 2nd execution (BUG!)
[14:39:48.655] Step: invoking Shipment child           ← ❌ 2nd execution (BUG!)
[14:39:48.707]   CHILD: Processing payment $99.99       ← Payment child executes
[14:39:48.812] Step: invoking Shipment child           ← ❌ 3rd execution (BUG!)
[14:39:48.812] Step: invoking Payment child            ← ❌ 3rd execution (BUG!)
[14:39:48.813] Step: got shipment result: ...          ← Gets cached result
[14:39:48.815] Step: got payment result: ...           ← Gets cached result
```

**Pattern**: Similar replay behavior, but harder to see due to parallel execution

## Root Cause Analysis

### Expected Behavior

When a flow suspends at `.invoke().result().await`:

1. Flow executes step
2. Step calls `.invoke().result().await` → **SUSPEND** (child not ready)
3. Child flow executes
4. Child completes
5. **Flow RESUMES at the await point** with child result
6. Flow continues from next line

### Actual Behavior

1. Flow executes step
2. Step calls `.invoke().result().await` → **SUSPEND**
3. Child flow executes
4. Child completes
5. **Flow REPLAYS FROM THE BEGINNING** ❌
6. All previous steps return cached results
7. When it reaches `.invoke().result().await` again, child is ready (cached)
8. Step gets cached result and continues

### Why This Happens

The flow is being treated like a **deterministic replay** system, not a **suspension/resumption** system.

**Hypothesis**: The `#[flow]` macro or executor is:
- Not preserving the execution state when a flow suspends
- Restarting the entire flow function instead of resuming at the suspension point
- Relying on step caching to make replay "correct"

**Evidence**:
- Sequential flow logs show `FLOW: Starting sequential execution` **three times**
- Each time it re-executes from line 1 of the flow function
- Steps return cached results (validate, payment)
- Final result is correct (thanks to caching)

## Impact

### Correct Behavior (Thanks to Caching)
✅ Final result is correct
✅ Child flows execute exactly once
✅ Idempotency is maintained
✅ Steps with caching don't re-execute side effects

### Problematic Behavior
❌ **Performance**: Unnecessary re-execution wastes CPU
❌ **Logs**: Confusing and misleading execution traces
❌ **Debugging**: Hard to understand what's actually happening
❌ **Side effects**: Any uncached operations in the flow body run multiple times
❌ **Conceptual model**: Violates the mental model of async/await

## Expected vs Actual Execution Count

| Operation | Expected | Actual | Note |
|-----------|----------|--------|------|
| Flow function body | 1 | 3 | ❌ Replays on each child completion |
| `validate` step execution | 1 | 1 | ✅ Cached after first run |
| `process_payment` step execution | 1 | 1 | ✅ Cached after first run |
| `ship` step execution | 1 | 1 | ✅ Cached after first run |
| `"Step: invoking Payment child"` log | 1 | 2 | ❌ Runs twice |
| `"Step: invoking Shipment child"` log | 1 | 3 | ❌ Runs three times |
| Payment child flow execution | 1 | 1 | ✅ Cached |
| Shipment child flow execution | 1 | 1 | ✅ Cached |

## Code Locations to Investigate

### 1. Flow Macro (`ergon_macros/src/flow.rs`)
Check how the `#[flow]` macro handles suspension/resumption.

**Question**: Does it generate a state machine or does it replay?

### 2. Executor Instance (`ergon/src/executor/instance.rs`)
Check how `Executor::execute()` handles flow suspension.

**Question**: When a flow suspends, does it store the suspension point or just restart?

### 3. Worker Loop (`ergon/src/executor/worker.rs`)
Check how the worker handles suspended flows.

**Question**: When resuming a flow, does it continue from suspension or restart?

### 4. Step Invocation (`ergon_macros/src/step.rs`)
Check how `.invoke().result().await` signals suspension.

**Question**: How does the step macro communicate suspension state?

## Reproduction

```bash
# Sequential version (clearer logs)
cargo run --example test_sequential_multiple_invoke --features=sqlite 2>&1 | grep -E "\[.*\]"

# DAG version (original bug report)
cargo run --example test_dag_multiple_invoke --features=sqlite 2>&1 | grep -E "\[.*\]"
```

## Expected Fix

The flow should **resume** from the suspension point, not replay:

```rust
// Current behavior (replay):
fn flow_body() {
    step1();  // ← Executes
    invoke_child().await;  // ← Suspends
    // Flow RESTARTS here when child completes ❌
    step1();  // ← Returns cached result
    invoke_child().await;  // ← Returns cached child result
    step2();  // ← Continues
}

// Expected behavior (resume):
fn flow_body() {
    step1();  // ← Executes
    invoke_child().await;  // ← Suspends
    // Flow RESUMES here when child completes ✅
    step2();  // ← Continues (no replay)
}
```

## Test Cases

### ✅ What Works
- Child flows execute exactly once
- Final results are correct
- Steps return cached results on replay
- Idempotency is maintained

### ❌ What's Broken
- Flow body executes multiple times
- Logs are confusing and misleading
- Any uncached code in flow body runs multiple times
- Performance waste from unnecessary replays

## Next Steps

1. Examine the `#[flow]` macro expansion
2. Check executor's suspension/resumption logic
3. Verify worker's flow state management
4. Fix the replay → resume behavior
5. Re-run tests to verify fix
6. Add test coverage for suspension counting

## Related Issues

This might be related to:
- Step caching mechanism (working correctly)
- Flow state persistence (might be missing suspension points)
- Async state machine generation in macro
- Worker's poll loop and task re-queueing
