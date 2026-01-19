# Load Balancing Test Harness

A runnable Node harness for benchmarking Jazz sync server performance with SQLite storage.

## Setup

From the repo root:

```bash
pnpm -C tests/load-balancing install
```

For the duration scenario, place a PDF in the assets folder:

```
tests/load-balancing/assets/sample.pdf
```

## Quick Start

Prepare both scenarios with default settings (10k items each):

```bash
pnpm -C tests/load-balancing run seed
```

Then run either scenario:

```bash
# Batch scenario - benchmark cold-cache map loading
pnpm -C tests/load-balancing run batch

# Duration scenario - sustained load test with files and maps
pnpm -C tests/load-balancing run duration
```

## Scenarios

### Batch Scenario

Loads a set of CoMaps across N workers, runs multiple iterations, and calculates throughput statistics. The sync server's coValue cache is cleared between runs to simulate cold-cache performance.

**Seed:**

```bash
pnpm -C tests/load-balancing run seed:batch -- --db ./batch.db --maps 1000 --minSize 100 --maxSize 1024
```

**Run:**

```bash
pnpm -C tests/load-balancing run batch -- --db ./batch.db --workers 8 --runs 50 --maps 500
```

Options:
- `--maps <n>` - Number of maps to create when seeding (default: 100)
- `--minSize <bytes>` - Minimum payload size (default: 100)
- `--maxSize <bytes>` - Maximum payload size (default: 1024)
- `--runs <n>` - Number of benchmark runs (default: 50)
- `--maps <n>` - Limit maps to load per run (default: all available)

### Duration Scenario

Generates sustained mixed load (files + maps) for a specified duration. Useful for testing throughput under continuous load.

**Seed:**

```bash
pnpm -C tests/load-balancing run seed:duration -- --db ./duration.db --items 100 --pdf ./assets/sample.pdf
```

**Run:**

```bash
pnpm -C tests/load-balancing run duration -- --db ./duration.db --workers 8 --durationMs 60000 --inflight 4 --mix 1f:1m
```

Options:
- `--items <n>` - Number of items to create (files + maps, default: 100)
- `--pdf <path>` - Path to PDF file for file streams (default: ./assets/sample.pdf)
- `--durationMs <ms>` - Duration to run the test (default: 60000)
- `--inflight <n>` - Max concurrent operations per worker (default: 4)
- `--mix <spec>` - Mix of files:maps, e.g. `1f:1m`, `2f:1m` (default: 1f:1m)
- `--mixMode <mode>` - `round_robin` or `randomized` (default: round_robin)

## Common Options

These options apply to both scenarios:

- `--db <path>` - Path to SQLite database
- `--workers <n>` - Number of worker threads (default: 8)
- `--host <host>` - Sync server host (default: 127.0.0.1)
- `--port <port>` - Sync server port (default: 4200)
- `--random-port` - Use a random available port

## Query Seeded Data

Inspect the seeded CoValues in a database:

```bash
pnpm -C tests/load-balancing run query -- --db ./batch.db --limit 50
```

## Metrics Dashboard

Both scenarios expose a Prometheus metrics endpoint. Open the dashboard in a browser:

```
http://127.0.0.1:4200/dashboard
```

The dashboard shows:
- Peers connected
- CoValues loaded (available/loading/unknown)
- Ingress/Egress bytes
- Message queue backlogs (incoming, outgoing, storage)
- Transaction size histogram
- Scenario-specific stats (throughput, run progress, etc.)

### Saving Results

The batch scenario dashboard includes a **Save Result** feature:
1. Enter a name for the result
2. Click "Save Result" to store it in localStorage
3. Saved results persist across page reloads
4. Delete saved results individually

This is useful for comparing performance across different configurations or code changes.

## Scaling Guidance- Start with small seeds (100-1000 items) and scale up
- Keep `--inflight` moderate (2-8) to avoid saturating the event loop
- Increase `--workers` to test concurrency
- For batch scenario, use `--maps` to limit items per run for faster iteration
- The batch scenario clears the cache between runs for consistent cold-cache measurements
