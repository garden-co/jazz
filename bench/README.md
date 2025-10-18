# Jazz Benchmarks

Benchmark suite for Jazz using [tinybench](https://github.com/tinylibs/tinybench) - a lightweight benchmarking library.

## 🚀 Quick Start

```bash
# Install dependencies
pnpm install

# Run all benchmarks
pnpm bench

# Run specific benchmark
pnpm bench:colist      # CoList operations
pnpm bench:comap       # CoMap operations
pnpm bench:comparison  # Quick comparison: NEW vs OLD

# View compaction statistics
pnpm stats
```

## 📊 Available Benchmarks

### 1. CoList Benchmarks (`colist.load.bench.ts`)

Tests CoList performance across different scenarios:

- **List Import**: Loading list data from storage
- **List Import + Content Load**: Loading and materializing list content
- **List Updating**: Appending items to lists

Each test compares:
- Current version (WASM crypto)
- Current version (NAPI crypto)
- Jazz v0.18.24 (baseline)

**Run**: `pnpm bench:colist`

### 2. CoMap Benchmarks (`comap.create.bench.ts`)

Tests CoMap performance:

- **Map Import**: Loading map data from storage
- **Map Import + Content Load**: Loading and materializing map content
- **Map Updating**: Setting values in maps

**Run**: `pnpm bench:comap`

### 3. Quick Comparison (`quick-comparison.ts`)

Comprehensive comparison of NEW vs OLD implementations across multiple scenarios:

- 10000 sequential appends
- 1000 sequential appends
- 500 sequential appends
- 100 random inserts
- 500 sequential + 50 random (mixed)

Shows detailed performance metrics including:
- Execution time
- Speedup/slowdown
- Compaction statistics
- Improvement percentage
- Percentiles (p75, p99, p99.5, p99.9)

**Run**: `pnpm bench:comparison`

### 4. Compaction Statistics (`compaction-stats.ts`)

Detailed analysis of CoList graph compaction optimization:

- Sequential appends (best case)
- Mixed operations
- Prepend operations
- Operations with deletions
- Cache performance comparison

**Run**: `pnpm stats`

## 📈 Understanding Results

Tinybench provides detailed metrics for each benchmark:

- **mean**: Average execution time per iteration (ms)
- **hz**: Operations per second
- **p75**: 75th percentile - 75% of operations complete within this time
- **p99**: 99th percentile - 99% of operations complete within this time
- **p99.5**: 99.5th percentile - 99.5% of operations complete within this time
- **p99.9**: 99.9th percentile - slowest 0.1% of operations
- **min/max**: Minimum and maximum execution times

Percentiles are important for understanding tail latencies. For example:
- **p99 = 2ms** means 99% of operations complete within 2ms
- A high p99 compared to mean indicates occasional slowdowns

Example output:
```
┌─────────┬────────────────────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
│ (index) │ Task                   │ Mean     │ p75      │ p99      │ p99.5    │ ops/sec  │
├─────────┼────────────────────────┼──────────┼──────────┼──────────┼──────────┼──────────┤
│ 0       │ 'current version'      │ '0.81ms' │ '0.82ms' │ '0.95ms' │ '1.03ms' │ '1,234'  │
│ 1       │ 'current version NAPI' │ '0.64ms' │ '0.64ms' │ '0.71ms' │ '0.78ms' │ '1,567'  │
│ 2       │ 'Jazz 0.18.18'         │ '0.92ms' │ '0.93ms' │ '1.08ms' │ '1.15ms' │ '1,089'  │
└─────────┴────────────────────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
```

All benchmarks now include detailed percentile information to help identify performance outliers and tail latencies.

## 🔧 Configuration

Benchmarks are configured with:

- **Iterations**: Number of times each benchmark runs
  - CoList/CoMap: 500-5000 iterations
  - Quick Comparison: 10 iterations
- **Warm-up**: Tinybench automatically handles warm-up phases

## 📁 Structure

```
bench/
├── utils.ts                        # Shared utilities
│   ├── formatTime()               # Format time values
│   └── displayBenchmarkResults()  # Display benchmark tables
├── colist.load.bench.ts           # CoList benchmarks
├── comap.create.bench.ts          # CoMap benchmarks
├── quick-comparison.colist.bench.ts # Comparison tests
├── compaction-stats.ts            # Compaction analysis
└── package.json                   # Dependencies & scripts
```

### Shared Utilities (`utils.ts`)

All benchmark files use shared utilities for consistent formatting:

- **`formatTime(ms)`**: Formats milliseconds as µs, ms, or s based on magnitude
- **`displayBenchmarkResults(bench, includeP999?)`**: Creates formatted tables with percentiles

## 📦 Dependencies

- **tinybench**: ^2.9.0 - Lightweight benchmarking library
- **tsx**: ^4.7.0 - TypeScript execution engine
- **cojson**: workspace:* - Current Jazz version
- **cojson-latest**: npm:cojson@0.18.24 - Baseline for comparison

## 🔄 Migration from Vitest

These benchmarks were migrated from Vitest to tinybench for:

1. **Better control**: More granular control over benchmark execution
2. **Lighter weight**: No test framework overhead
3. **Flexibility**: Easier to customize output and metrics
4. **Speed**: Faster startup and execution

## 📚 Additional Documentation

For CoList graph compaction optimization details, see:
- [README_COMPACTION.md](./README_COMPACTION.md) - Complete compaction documentation
- [OPTIMIZATION_SUMMARY.md](./OPTIMIZATION_SUMMARY.md) - Overview of optimizations
- [VISUAL_GUIDE.md](./VISUAL_GUIDE.md) - Visual examples and explanations

## 🎯 Best Practices

1. **Build first**: Always run `pnpm build:packages` before benchmarking
2. **Consistent environment**: Close other apps to reduce noise
3. **Multiple runs**: Run benchmarks multiple times for consistency
4. **Analyze trends**: Look at trends across multiple runs, not single values
5. **Compare carefully**: Ensure you're comparing like-for-like scenarios

## 🤝 Contributing

When adding new benchmarks:

1. Import utilities from `utils.ts`
2. Use tinybench's `Bench` class
3. Set appropriate iteration counts
4. Include comparison with baseline (Jazz 0.18.24)
5. Use `displayBenchmarkResults()` for consistent output
6. Update this README with benchmark description
7. Add script to `package.json`

Example:
```typescript
import { Bench } from "tinybench";
import { displayBenchmarkResults } from "./utils.js";

async function runMyBench() {
  console.log("\n📊 My Benchmark");
  console.log("=".repeat(50));

  const bench = new Bench({ iterations: 1000 });

  bench
    .add("test case 1", () => {
      // Your code here
    })
    .add("test case 2", () => {
      // Your code here
    });

  await bench.run();
  displayBenchmarkResults(bench);
}

runMyBench().catch(console.error);
```

### Utility Functions

Import shared utilities for consistent formatting:

```typescript
import { formatTime, displayBenchmarkResults } from "./utils.js";

// Format a time value
const formatted = formatTime(1.234); // "1.23ms"

// Display benchmark results with percentiles
await bench.run();
displayBenchmarkResults(bench); // Shows table with Mean, p75, p99, p99.5, ops/sec

// Include p99.9 for detailed analysis
displayBenchmarkResults(bench, true); // Adds p99.9 column
```

---

Happy benchmarking! 🚀

