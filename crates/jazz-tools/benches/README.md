# Benchmark porting status

This directory contains both active Criterion benches and legacy benchmark source
material while the old `jazz-tools` engine is being replaced.

## Active benches

The active bench harness is the explicit `[[bench]]` list in
`crates/jazz-tools/Cargo.toml`:

- `observer_write_path`
- `direct_core_benchmark`
- `direct_authorization_scope_benchmark`
- `realistic_phase1_direct`
- `insert_benchmark`
- `update_benchmark`
- `subscription_benchmark`

All active Criterion benches now exercise the new `jazz_core`/`jazz` facade
directly instead of going through the legacy
`jazz-tools::runtime_core::RuntimeCore` stack.

Two direct ports intentionally measure the nearest direct-core semantics rather
than old helper behavior:

- `insert_benchmark` models team/folder authorization as a folder-access join
  policy instead of old `INHERITS SELECT VIA folder_id` session recursion.
- `subscription_benchmark` uses `Db::mergeable_tx()` for the batch case so the
  direct-core benchmark measures one transaction-shaped subscription delta.
- `realistic_phase1_direct` is a smallest useful active slice of the old
  realistic suite. It hard-codes the S profile and covers single-DB memory
  project-board CRUD, mixed reads, a hot-task comment/activity history workload
  with multiple direct subscriptions, subscribed writes, and a direct
  writer-DB -> server-DB -> reader-DB sync fanout with a reader subscription
  through `jazz::db::Db` directly.

## Intended next ports

Next ports should prioritize the inactive deep-internal scenarios before they
are reintroduced:

- `memory_benchmark`
- `realistic_phase1`

Inactive legacy benches should remain in the tree as source material for
scenarios, scale factors, fixtures, and expected measurement coverage. Port them
by rebuilding the scenarios against the public direct-core API rather than by
reviving the old RuntimeCore internals.

The old `memory_benchmark` file was removed rather than left as a broken
RuntimeCore path. Reintroduce it after the direct `Db` facade exposes retained
memory metrics comparable to the old SyncManager/QueryManager breakdown.
