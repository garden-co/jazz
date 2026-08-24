# Wequencer benchmark variant

This is a self-contained native model of Wequencer's pattern-grid workload.
It duplicates the schema, fixture, and query shapes used by the app so a
benchmark remains intelligible without an application runtime.

The first workloads are intentionally small but realistic:

- read one ordered 16-step playhead window from a 64-step pattern;
- read one full track pattern ordered by step;
- apply a burst of independent editor writes, then prove the final values; and
- open the UI-shaped ordered subscription, edit one pad, and wait for its
  public subscription event.

Fixture setup is outside measured closures. Correctness tests assert exact
window ordering and convergence before CodSpeed measures the same APIs.
