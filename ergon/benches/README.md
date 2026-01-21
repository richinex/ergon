# Ergon Benchmarks

## Running Benchmarks

### All Storage Backends

Make sure Docker services are running:
```bash
# Start Postgres and Redis
docker compose -f docker-compose.postgres.yml up -d
# Redis should already be running as ergon-redis

# Run all benchmarks
cargo bench --bench system_bottlenecks --features "postgres,redis"
```

### Individual Storage Backends

```bash
# InMemory + SQLite only (no Docker required)
cargo bench --bench system_bottlenecks

# With Postgres
cargo bench --bench system_bottlenecks --features postgres

# With Redis
cargo bench --bench system_bottlenecks --features redis
```

## Current Results Summary

**Storage Operation Performance:**

| Storage | Write (µs) | Read 5 items (µs) | Notes |
|---------|-----------|-------------------|-------|
| InMemory | ~5 | ~12 | In-process, no I/O |
| Redis | ~108 | ~970 | **Fastest persistent** - 12x faster than Postgres |
| SQLite | ~1,163 | ~1,427 | Local file, single connection |
| Postgres | ~1,270 | ~6,544 | Network + ACID, similar to SQLite |

**Critical Finding:**
- **Connection pooling matters!** Postgres improved 95.5% (28ms → 1.27ms) when fixed
- **Redis is 12x faster** than Postgres for single operations
- **Storage still dominates** 99%+ of execution time, but now we have realistic numbers
