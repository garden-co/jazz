# Jazz Browser Performance Tests

A browser-based performance testing app for Jazz with automated benchmarking CLI.

## Scenarios

### Pixel Grid
Generate NxN pixel grids with random colors and configurable payload sizes for testing canvas rendering and data loading.

## Development

```bash
# Start the development server (web app + sync server)
pnpm dev

# Build for production
pnpm build
```

## Benchmark CLI

Run automated benchmarks with statistically rigorous analysis (confidence intervals, percentiles).

### Prerequisites

Before running benchmarks, start the servers:

```bash
pnpm build
pnpm preview --port 5173 &
pnpm sync --in-memory &
```

### Usage

```bash
pnpm bench <scenario> [options]
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--runs, -n <n>` | Number of benchmark runs | 50 |
| `--id <covalue-id>` | Use existing CoValue ID (skips fixture generation) | - |
| `--sync, -s <url>` | Sync server URL | ws://localhost:4200 |
| `--headful` | Run browser with visible window | false (headless) |
| `--cold-storage` | Clear browser storage between runs | false |

### Grid Scenario Options

| Option | Description | Default |
|--------|-------------|---------|
| `--size <n>` | Grid size NxN | 10 |
| `--min-padding <n>` | Minimum padding bytes per cell | 0 |
| `--max-padding <n>` | Maximum padding bytes per cell | 100 |

### Examples

```bash
# Run grid benchmark with 50 runs
pnpm bench grid --size 20 --runs 50

# Reuse an existing grid (skip generation)
pnpm bench grid --id co_z1234567890abcdef --runs 100

# Run with cold storage (fresh browser state each run)
pnpm bench grid --id co_z1234567890abcdef --cold-storage --runs 50
```

### Output

The benchmark outputs statistical analysis including:
- Mean and 95% confidence interval
- Median, p75, p90, p95, p99 percentiles
- Min/Max values

```
┌─────────────────────────────────────────────────────────────────┐
│  Grid Benchmark Results (50 runs)                               │
├─────────────────────────────────────────────────────────────────┤
│  CoValue ID: co_z1234567890abcdef                               │
│  Metric: loadTimeMs                                             │
│                                                                 │
│  Mean:     234.5ms                                              │
│  95% CI:   228.3ms - 240.7ms (±2.6%)                            │
│  Median:   232.0ms                                              │
│  p95:      289.0ms                                              │
│  Min/Max:  198.0ms - 312.0ms                                    │
└─────────────────────────────────────────────────────────────────┘
```

## Sync Server

By default, connects to `ws://localhost:4200`. You can:
- Pass a `?sync=wss://your-server.com` query parameter
- The app remembers recent connections in localStorage
