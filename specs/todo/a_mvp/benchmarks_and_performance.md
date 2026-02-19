# Benchmarks & Performance — TODO

Prove that Jazz is not a toy: measurable performance at realistic scale.

## Phasing

- **MVP**: Identify and run the most de-risking benchmarks internally. Focus on: "are we in the right ballpark?" Not publishable yet.
- **Launch**: Publishable benchmark results, large-scale demo apps, distributed query tracing visualization.

## MVP: De-Risking Benchmarks

Run internal benchmarks to validate core performance before onboarding adopters:

- **Local query throughput**: how fast are index lookups and query graph evaluation?
- **Sync throughput**: messages per second, bytes per second between tiers
- **OPFS performance**: read/write/flush timing in browser worker
- **Index write overhead**: cost of maintaining indices on insert/update

Goal: identify any show-stopping performance issues early. Even rough numbers (via `criterion` or simple timing harnesses) are valuable. Prioritize the benchmarks that would be most embarrassing if they revealed a problem.

## Launch: Publishable Results

### Traditional DB Benchmarks

Run standard benchmarks (adapted for our SQL subset) to compare against Postgres, SQLite:

- Even 5x slower than Postgres is a strong result for a local-first database
- Run internally first; decide whether to publish based on results

### Large-Scale Example Apps

Build demo apps with realistic data volumes (see `../b_launch/example_apps.md`):

- 10K+ rows with complex queries, filters, sorts
- Measure and display: local query time, sync latency, settlement time per tier

### Distributed Query Tracing

Leverage observability (`observability_first.md`) to visualize performance across the full stack.

## Open Questions

- Which benchmarks are most de-risking? (Query throughput? Sync latency? OPFS I/O?)
- Should benchmarks run in CI to catch performance regressions?
- How to fairly compare with systems that have very different consistency models?
- How to benchmark reactive query performance (subscription update latency)?
