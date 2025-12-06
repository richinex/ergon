#!/bin/bash

echo "╔══════════════════════════════════════════════════════════╗"
echo "║        COMPREHENSIVE REDIS DIAGNOSTIC TEST              ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

cd /home/richinex/Documents/devs/rust_projects/ergon

for run in 1 2 3 4 5 6 7 8 9 10; do
    echo "═══════════════════════════════════════════════════════════"
    echo "RUN #$run"
    echo "═══════════════════════════════════════════════════════════"

    # Flush Redis
    docker exec ergon-redis redis-cli FLUSHALL > /dev/null 2>&1

    # Run the example and capture ALL output
    output=$(RUST_LOG=error timeout 60 cargo run --example complex_1_worker_dag_redis --features=redis 2>&1)

    # Count how many times each order appears
    ord001_count=$(echo "$output" | grep -c "ORDER\[ORD-001\]" || echo "0")
    ord002_count=$(echo "$output" | grep -c "ORDER\[ORD-002\]" || echo "0")
    ord003_count=$(echo "$output" | grep -c "ORDER\[ORD-003\]" || echo "0")

    # Get completion counts
    completed=$(echo "$output" | grep -c "Fulfillment complete" || echo "0")

    # Get step counts from summary
    ord001_summary=$(echo "$output" | grep "ORD-001:" | head -1)
    ord002_summary=$(echo "$output" | grep "ORD-002:" | head -1)
    ord003_summary=$(echo "$output" | grep "ORD-003:" | head -1)

    echo "Order execution counts:"
    echo "  ORD-001: appeared $ord001_count times"
    echo "  ORD-002: appeared $ord002_count times"
    echo "  ORD-003: appeared $ord003_count times"
    echo ""
    echo "Completion: $completed/3 orders completed"
    echo ""
    echo "Step summaries:"
    echo "  $ord001_summary"
    echo "  $ord002_summary"
    echo "  $ord003_summary"

    # Check for failures
    if [ "$completed" != "3" ]; then
        echo ""
        echo "⚠️  FAILURE DETECTED! Diagnosing..."
        echo ""

        # Check Redis state
        echo "--- Processing Lists ---"
        processing_keys=$(docker exec ergon-redis redis-cli KEYS "ergon:processing:*")
        if [ -n "$processing_keys" ]; then
            echo "Processing lists exist:"
            for key in $processing_keys; do
                echo "  $key:"
                docker exec ergon-redis redis-cli LRANGE "$key" 0 -1 | while read task_id; do
                    if [ -n "$task_id" ]; then
                        echo "    Task: $task_id"
                        flow_data=$(docker exec ergon-redis redis-cli HGET "ergon:flow:$task_id" "flow_data")
                        status=$(docker exec ergon-redis redis-cli HGET "ergon:flow:$task_id" "status")
                        echo "      Status: $status"
                        echo "      Data: $flow_data"
                    fi
                done
            done
        else
            echo "  (none)"
        fi

        echo ""
        echo "--- Pending Queue ---"
        pending=$(docker exec ergon-redis redis-cli LRANGE "ergon:queue:pending" 0 -1)
        if [ -n "$pending" ]; then
            echo "Pending tasks:"
            echo "$pending" | while read task_id; do
                if [ -n "$task_id" ]; then
                    echo "  Task: $task_id"
                fi
            done
        else
            echo "  (empty)"
        fi

        echo ""
        echo "--- Running Set ---"
        running=$(docker exec ergon-redis redis-cli ZRANGE "ergon:running" 0 -1)
        if [ -n "$running" ]; then
            echo "Running tasks:"
            echo "$running" | while read task_id; do
                if [ -n "$task_id" ]; then
                    echo "  Task: $task_id"
                fi
            done
        else
            echo "  (empty)"
        fi

        echo ""
        echo "--- All Flow Keys ---"
        flow_count=$(docker exec ergon-redis redis-cli KEYS "ergon:flow:*" | wc -l)
        echo "Total flows in Redis: $flow_count"

        docker exec ergon-redis redis-cli KEYS "ergon:flow:*" | while read key; do
            flow_id=$(docker exec ergon-redis redis-cli HGET "$key" "flow_id")
            flow_type=$(docker exec ergon-redis redis-cli HGET "$key" "flow_type")
            status=$(docker exec ergon-redis redis-cli HGET "$key" "status")
            locked_by=$(docker exec ergon-redis redis-cli HGET "$key" "locked_by")
            echo "  $key:"
            echo "    flow_id: $flow_id"
            echo "    flow_type: $flow_type"
            echo "    status: $status"
            echo "    locked_by: $locked_by"
        done

        echo ""
        echo "--- Last 20 lines of output ---"
        echo "$output" | tail -20

        # Stop after first failure for detailed analysis
        break
    else
        echo "✅ SUCCESS - all 3 orders completed"
    fi

    echo ""
done

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                   DIAGNOSTIC COMPLETE                    ║"
echo "╚══════════════════════════════════════════════════════════╝"
