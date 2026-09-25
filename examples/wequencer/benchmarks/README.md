# Wequencer benchmark variant

This is a self-contained native model of Wequencer's pattern-grid workload.
It mirrors the schema and key query shapes used by the app so a benchmark
remains intelligible without an application runtime.

The first workloads are intentionally small but realistic:

- read one ordered 16-step playhead window from a 64-step pattern;
- read one full track pattern ordered by step;
- apply a deterministic sequence of editor-shaped local writes, then prove the
  final values; and
- open the UI-shaped ordered subscription, edit one pad, and wait for its
  public subscription event; and
- fan that one edit out to 1, 8, or 32 independently maintained pattern-grid
  subscriptions; and
- read the latest transport observation through the session-scoped ordered
  query used by a second collaborator.

[metadata.ts](metadata.ts) owns the wall-clock descriptions and timing
boundaries for `benches/walltime.rs`, which CodSpeed runs on every
`benchmark`-labelled PR and nightly: opening the 16-track pattern grid (17
subscriptions) and toggling a pad while that grid is live. The other
workloads above are exercised by `tests/workloads.rs`.

Fixture setup is outside measured closures. Correctness tests assert exact
window ordering and convergence before CodSpeed measures the same APIs.
