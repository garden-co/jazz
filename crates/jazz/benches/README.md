# Benchmark porting status

This directory contains active Criterion benches for the core benchmark
path. Old deep-internal `RuntimeCore` benchmark sources are not retained as
source material here.

## Active benches

The active bench harness is the explicit `[[bench]]` list in
`crates/jazz/Cargo.toml`. It includes the retained core/simulation receipts and
the facade-level Criterion and realistic lanes, including:

- `observer_write_path`
- `db_benchmark`
- `authorization_scope_benchmark`
- `realistic_phase1`
- `insert_benchmark`
- `update_benchmark`
- `subscription_benchmark`
- `validation`
- `sync`
- `cold_subscription`
- `relation_include_delivery`
- `selective_global_hydration`

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
  `r13_permission_filtered_resume` matrix in the same file combines the
  byte-wire session/resume path with recursive membership and claim policies. It
  covers unchanged authorization, grant-only, revoke-only, simultaneous
  grant/revoke, claim revoke, and claim restore while disconnected. Every lane
  gates the exact resumed subscription transition and records reconnect-only
  time plus full/resume response bytes. The retained receipt is in
  `dev/benchmarks/POLICY_CHURN_RECONNECT_RECEIPT_20260805.md`. The `jazz` policy
  tests cover recursive write-policy settlement with global/settled support
  rows; local-only support rows correctly do not authorize writes.

`selective_global_hydration` is a Divan/CodSpeed wall-time benchmark over the
persisted Global maintained-subscription hydration path. It measures fixed
10k- and 100k-row tables while keeping the selected team and result page
constant. Fixture seeding, reopening, preparation, and an exact result/read-
bound validation pass are outside the timed closure. The full JSONL read-count
receipt, including its 1M-row diagnostic rung, remains available by setting
`JAZZ_SELECTIVE_HYDRATION_RECEIPT=1`.

## Intended next ports

Next ports should rebuild any missing measurement intent against the public
core API before reintroducing it:

- `memory_benchmark`

The old `server_authorization_scope_benchmark` file was removed after its
measurement intent was ported to `authorization_scope_benchmark`.

The old `memory_benchmark` file was removed rather than left as a broken
RuntimeCore path. Reintroduce it after the `Db` facade exposes retained
memory metrics comparable to the old SyncManager/QueryManager breakdown.
