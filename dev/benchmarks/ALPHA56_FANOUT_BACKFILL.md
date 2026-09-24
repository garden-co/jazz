# alpha.56 historical subscription fan-out backfill

This permanent harness branch must not merge into main or any release. It
starts at released engine `8af685e184c8691a244da1171a51c745a7208e3b` (`v2.0.0-alpha.56`) and imports the W1
permissioned dashboard fan-out workload (#3231), added to main after alpha.56,
from harness source `91bd1bb97c6ad723ba92a8f73d6133f64bee1874`. Every released `crates/` entry is pinned by
`verify-alpha56-fanout-backfill.py`. Cargo.toml, Cargo.lock, dependency
versions and profiles are the release's own; the W1 package already exists in
the release workspace.

Dispatch `.github/workflows/codspeed.yml` on this branch; there are no inputs.
The single job runs on `codspeed-macro` in walltime mode with Rust 1.93.1,
cargo-codspeed 5.0.1 and CodSpeed action v5.0.3, and uploads a provenance
artifact before building. Like main's W1 job, it uses the system allocator and
builds on the macro runner.

## Harness changes

- `src/subscription_fanout.rs` is imported from the source SHA. Its only
  change replaces `compilation_counts()` with zeros: no release exposes a
  Db-level query-program compilation counter. The counter is a diagnostic
  receipt field; it never affects the settled condition or the work done.
- `src/lib.rs` gains `pub mod subscription_fanout;`.
- `benches/reads_memory_walltime.rs` keeps main's path and function identity,
  so CodSpeed files results under the same benchmark IDs, but contains only
  the four fan-out cases. Other W1 memory cases are not re-measured.

Schema, inherited SELECT policy, identities, row distribution, 60 fixed boards,
Core → scope-isolated relay → non-durable foreground topology, the 1024-turn
bound, the settled/exact-count condition and the divan cases and samples are
unchanged. `cargo test -p jazz-example-benchmark-w1 --lib subscription_fanout`
(exact membership, live edit and revocation for local-first and strict
remote reads) passes against this released engine.

## Not portable: `first_sync_local_relay_27518_rocksdb`

The permissioned-resources revision-2 harness admits the relay through
`Node::accept_scope_isolated_relay_subscriber_for_test`. Commit 3dc586449
added that testing wrapper to the engine after alpha.56. In this release the
Node-level admission is `pub(crate)`, and the only public route is the serving
shell (a different topology). Adding the wrapper would change pinned engine
bytes, so no alpha.56 measurement is produced.

## Registration

After CI, inspect successful job output and provenance, resolve exact CodSpeed
run/result IDs, and register only actually measured results in main's
`docs/lib/perf-timeline/backfills.ts`. Effective date `2026-09-21T00:48:24.669Z` is npm
`jazz-tools` time[2.0.0-alpha.56]. Keep the actual CodSpeed measurement
timestamp, harness SHA, source SHA, engine SHA and workflow link. Never
fabricate a historical timestamp or include carried-forward/skipped results.
