# Storage Benchmarking Spike — TODO (This Week)

Establish baseline performance numbers for legacy storage and identify whether it's the right long-term storage backend.

## Motivation

We need to de-risk storage before building more on top. Specific concerns:

- legacy storage performance characteristics under realistic workloads are unknown
- Objects with long commit histories (many snapshots) may blow up storage/memory
- Memory duplication if similar snapshots are retained in history
- Alternative backends (fjall, LSM-based) may be better suited

## Benchmarks to Run

### 1. legacy storage baseline

- Write throughput: insert N objects of varying sizes
- Read throughput: random and sequential object_get
- Index operations: insert/remove/range scan under load
- Flush/snapshot timing (native + OPFS)

### 2. Long commit histories

- Create objects with 10, 100, 1000 commits
- Measure: storage size growth, read latency, snapshot size
- Check for memory duplication across similar snapshots

### 3. Comparison point

- Run same workloads against fjall (or its underlying LSM tree)
- Focus on: write amplification, read latency, space efficiency
- Don't need a full integration — standalone benchmarks are fine

## Expected Output

- Numbers in a markdown table (rough is fine, not publishable)
- Go/no-go recommendation: keep legacy storage, investigate alternatives, or both
- Identified bottlenecks to feed into `../b_mvp/benchmarks_and_performance.md`

## Non-Goals

- Publishable benchmarks (that's launch-phase)
- Full integration of an alternative backend
- Optimizing legacy storage (first understand, then decide)
