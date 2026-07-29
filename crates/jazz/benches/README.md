# Benchmark porting status

This directory contains active Criterion benches for the core benchmark
path. Old deep-internal `RuntimeCore` benchmark sources are not retained as
source material here.

## Active benches

The active bench harness is the explicit `[[bench]]` list in
`crates/jazz-tools/Cargo.toml`:

- `observer_write_path`
- `db_benchmark`
- `authorization_scope_benchmark`
- `realistic_phase1`
- `insert_benchmark`
- `update_benchmark`
- `subscription_benchmark`

All active Criterion benches now exercise the workspace `jazz` engine facade
directly instead of going through the legacy
`jazz-tools::runtime_core::RuntimeCore` stack.

Two core ports intentionally measure the nearest core semantics rather
than old helper behavior:

- `insert_benchmark` models team/folder authorization as a folder-access join
  policy instead of old `INHERITS SELECT VIA folder_id` session recursion.
- `subscription_benchmark` uses `Db::mergeable_tx()` for the batch case so the
  core benchmark measures one transaction-shaped subscription delta.
- `realistic_phase1` is a smallest useful active slice of the old
  realistic suite. It hard-codes the S profile and covers single-DB memory
  project-board CRUD, mixed reads, a RocksDB project-board cold-load
  reopen/prepare/first-read scenario, a hot-task comment/activity history workload
  with multiple core subscriptions, subscribed writes, and a core
  writer-DB -> server-DB -> reader-DB sync fanout with a reader subscription
  through `jazz::db::Db` directly. It also includes a byte-wire reconnect/resume
  canary that serves current task rows once, resumes after a disconnected
  upstream write is ingested by the server, and checks that the catch-up payload
  is smaller than the full snapshot. The `r12_recursive_permissions` group ports
  the spirit of the old R5 recursive permission benchmark to the public
  `Db` APIs with a `docs`/`teams`/`doc_access`/`team_edges` schema, prepared
  recursive read-policy query/subscription visibility. A scoped
  `r13_permission_filtered_resume` reproducer in the same file combines the
  byte-wire session/resume path with that recursive read policy: a reader first
  sees direct and inherited docs, disconnects, then resumes after one inherited
  grant is revoked and another is added. It is intentionally not registered in
  the default green Criterion group yet, because the resumed client still keeps
  the revoked doc visible. Recursive write-policy settlement is covered in the
  `jazz` policy tests with global/settled support rows; local-only support rows
  correctly do not authorize writes.

## Intended next ports

Next ports should rebuild any missing measurement intent against the public
core API before reintroducing it:

- `memory_benchmark`

The old `server_authorization_scope_benchmark` file was removed after its
measurement intent was ported to `authorization_scope_benchmark`.

The old `memory_benchmark` file was removed rather than left as a broken
RuntimeCore path. Reintroduce it after the `Db` facade exposes retained
memory metrics comparable to the old SyncManager/QueryManager breakdown.
