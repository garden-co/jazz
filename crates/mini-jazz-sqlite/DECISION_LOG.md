# Decision Log

## 2026-05-25 23:56 PDT

Started the continuous overnight implementation/discovery pass.

Goal: make executable progress rather than abstract answers. Priorities:

- add whole-system tests that expose missing Jazz parity and distributed invariants;
- implement enough generic runtime behavior to make the tests meaningful;
- improve architecture where the current mini crate is getting in its own way;
- keep timestamped notes from `date` before new entries;
- use subagents for parallel test/architecture scouting while local work stays on the critical path.

## 2026-05-25 23:58 PDT

Target stop time is 2026-05-26 06:00 PDT. Do not stop before then, including at
good stopping points. Treat green tests/commits as checkpoints; then pick the
next highest-leverage topic and continue.

First executable discovery target: generic update semantics and rejection repair.
Red tests show `update_row` currently requires full-row payloads, which is not
the desired Jazz-like patch behavior and also prevents rejection repair from
restoring a previous visible version cleanly.

## 2026-05-25 23:59 PDT

First slice green: generic updates now merge omitted fields from the current
visible row, and rejecting an update rebuilds projection so the previous visible
version reappears instead of disappearing. This makes transaction/read-set
tests closer to TS API semantics (`undefined`/omitted fields are no-ops).

Next target: query-scope resync when a row still matches the direct predicate
but becomes hidden through a policy dependency. Both scouts independently
flagged this as a likely gap in `export_query_where_eq` / query repair.

## 2026-05-26 00:05 PDT

Checkpoint: `cargo test -p mini-jazz-sqlite` is green with 123 tests.

Implemented:

- generic updates are patch-style and preserve omitted fields;
- patch updates choose their base from the effective visible row, including
  pinned branch base snapshots;
- rejecting an update rebuilds projection, restoring the previous visible
  version or pinned base instead of leaving a hole;
- added query-scope tests for policy-dependency changes and branch-local repair
  isolation.

Design lesson: write lowering needs an explicit "effective base row" concept.
That base is not always the row in the checked-out branch current projection:
it may be the sparse-overlay inherited main row, or a pinned historical snapshot.
This is a good candidate for an architecture cleanup boundary rather than more
ad hoc helpers inside `runtime.rs`.
