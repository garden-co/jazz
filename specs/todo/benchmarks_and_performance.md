# Benchmarks & Performance — TODO

Prove that Jazz is not a toy: measurable performance at realistic scale.

## Overview

The team needs to demonstrate that Jazz handles real-world data volumes and query patterns. Three complementary approaches:

### 1. Traditional DB Benchmarks

Run standard benchmarks (adapted for our SQL subset) to compare against Postgres, SQLite, etc.:

- Even 5x slower than Postgres is a strong result for a new distributed local-first database
- Publish results transparently (including where we're slower and why)
- Run internally first; decide whether to publish based on results

### 2. Large-Scale Example Apps

Build demo apps with realistic data volumes:

- 10K+ rows with complex queries, filters, sorts
- Show instant local query response + real-time sync
- Measure and display: local query time, sync latency, settlement time per tier
- Use these in the announcement post ("look how fast this is with 50K rows")

### 3. Distributed Query Tracing

Leverage `observability_first.md` to visualize performance across the full stack:

- Trace a single query from client → worker → edge → core → shard and back
- Show exactly where time is spent (local index lookup vs. network vs. server-side query)
- Use as a developer tool ("why is this query slow?") and as a demo ("look at our internals")

## Open Questions

- Which benchmarks? TPC-C (too transactional?), custom suite, or industry-standard local-first benchmarks?
- How to benchmark reactive query performance (subscription update latency)?
- Client-side benchmarks: WASM in browser, memory usage, OPFS throughput?
- Should benchmarks run in CI to catch performance regressions?
- How to fairly compare with systems that have very different consistency models?
