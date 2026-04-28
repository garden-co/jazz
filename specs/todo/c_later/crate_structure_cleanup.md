# Crate Structure Cleanup — TODO (Later)

Non-blocking refactor of `crates/jazz-tools/` aimed at making the crate easier to
read end-to-end. No functional change. Each phase is independently mergeable and
ordered so later work depends on earlier moves.

The motivation is captured in a runtime-first walkthrough done against `main`
at `fa52406b6`: the runtime layer itself is in good shape (a sync state machine
plus a thin tokio wrapper), but the top-level `src/` directory has become a
junk drawer, a few files have outgrown their seams, and a handful of types live
inside `RuntimeCore` that conceptually belong elsewhere.

## Goals

- Top-level `src/` only contains entry points and true cross-subsystem
  primitives.
- Each subsystem owns its own helpers, test support, and platform splits.
- `RuntimeCore` shrinks to orchestration; bookkeeping moves to dedicated types.
- No file over ~3K LOC except as a deliberate, documented exception.

## Non-goals

- No change to public API of `jazz-tools` consumed by `jazz-napi`, `jazz-wasm`,
  `jazz-rn`, or the TS client. Re-exports in `lib.rs` continue to expose the
  same names from their new paths.
- No change to wire format, storage format, sync semantics, or query
  evaluation.
- No new features. Hypothetical future requirements do not justify abstractions
  introduced here.
- No tests rewritten. Tests move with their code; their assertions stay.

## Phase 1 — Junk-drawer relocation

Pure mechanical moves. Each one is a single PR.

| Move                | From                  | To                                                                                   |
| ------------------- | --------------------- | ------------------------------------------------------------------------------------ |
| Sync clock          | `monotonic_clock.rs`  | `sync_manager/clock.rs`                                                              |
| Batch lifecycle     | `batch_fate.rs`       | `sync_manager/batch_fate.rs`                                                         |
| Sync test recorder  | `sync_tracer.rs`      | `sync_manager/test_support.rs` (gated `#[cfg(any(test, feature = "test-support"))]`) |
| HTTP route surface  | `routes.rs`           | `server/routes.rs`                                                                   |
| Native binding glue | `binding_support.rs`  | `query_manager/bindings.rs`                                                          |
| Test row history    | `test_row_history.rs` | `row_histories/tests.rs` (gated `#[cfg(test)]`)                                      |

Top-level files that **stay** (true primitives, used by ≥3 subsystems):
`commit.rs`, `digest.rs`, `identity.rs`, `metadata.rs`, `object.rs`,
`row_format.rs`, `wire_types.rs`, `catalogue.rs`, `otel.rs`.

Acceptance:

- `cargo build --all-features` and `cargo test --all-features` pass.
- `pnpm build` and `pnpm test` pass (covers napi/wasm/rn re-export paths).
- No `pub use` from `lib.rs` changes name; only the path on the right-hand
  side of each re-export shifts.

## Phase 2 — Entry-point dedup

Small, independent fixes that remove parallel construction logic between
`client.rs` and `server/builder.rs`.

1. Extract `build_schema_manager(storage, context, role)` from the duplicated
   blocks at `client.rs:64-82` and `server/builder.rs:217-238`.
2. Extract `resolve_node_env()` from `main.rs:24-36` and `builder.rs:305-309`
   into a single helper (likely `server/env.rs`).
3. Move JWT key resolution from `main.rs:38-59` into `commands/server.rs` so
   the library no longer carries CLI-shaped logic.
4. Reuse one `reqwest::Client` between the main server (`builder.rs:155`) and
   the JWKS loader (`builder.rs:338-342`).
5. Promote `allow_local_first_auth: true` to `AuthConfig::default()` and drop
   the hardcoded copies in `builder.rs:73-76` and `server/testing.rs:105`.
6. Delete `#[allow(dead_code)] pub app_id` on `ServerState` (`server/mod.rs:165`)
   — either use it in a routed handler or remove the field.

## Phase 3 — Builder collapse

Replace the four storage builder variants (`with_persistent_storage`,
`with_in_memory_storage`, `with_sqlite_storage`, `with_rocksdb_storage` —
`server/builder.rs:86-146`) with a single `.with_storage(StorageBackend)`
where `StorageBackend` is an enum:

```rust
pub enum StorageBackend {
    InMemory,
    Sqlite { path: PathBuf },
    RocksDb { path: PathBuf },
    Persistent { path: PathBuf }, // current default-shape
}
```

The four call sites in tests, `commands/server.rs`, and `server/testing.rs`
are mechanical replacements. Old methods are removed (no deprecation shim —
this is prototype-stage code, per CLAUDE.md).

Acceptance: `cargo build --all-features` clean; tests unchanged in spirit.

## Phase 4 — Storage trait split

`storage/mod.rs` is ~8K LOC: the `Storage` trait definition (~2K) plus the
in-memory implementation (~6K). Split into:

```
storage/
  mod.rs           re-exports + module docs
  trait.rs         the Storage trait + associated types
  memory.rs        MemoryStorage impl
  sqlite.rs        (existing)
  rocksdb.rs       (existing)
  storage_core.rs  (existing)
  key_codec.rs     (existing)
  conformance.rs   (existing)
  opfs_btree/
    mod.rs
    native.rs      #[cfg(not(target_arch = "wasm32"))] body
    wasm.rs        #[cfg(target_arch = "wasm32")] body
```

Acceptance: every existing import keeps resolving via `storage::*`. The
`opfs_btree` cfg-fork now lives at module boundary, not as `#[cfg]` blocks
threaded through one file.

## Phase 5 — `RuntimeCore` decomposition

`RuntimeCore` (`runtime_core.rs:282-332`) holds 15+ fields blending
unrelated concerns. Extract three focused owners:

- **`DurabilityTracker`** — owns `ack_watchers` and `rejected_batch_ids`.
  Provides `register_watcher`, `record_ack`, `record_rejection`,
  `drain_rejected`. Used by `runtime_core/writes.rs`.
- **`SyncInbox`** — owns `parked_sync_messages`,
  `parked_sync_messages_by_server_seq`, `next_expected_server_seq`,
  `last_applied_server_seq`. Replaces dual-buffer code at
  `runtime_core/ticks.rs:622-684` with one keyed buffer
  (`Option<seq>`). Provides `push`, `apply_ready(&mut SyncManager)`.
- **`SubscriptionRegistry`** — owns `subscriptions`, `subscription_reverse`,
  `pending_subscriptions`, `pending_one_shot_queries`. Encloses the
  one-shot leak window in `runtime_core/ticks.rs:390-475` by carrying the
  `sub_id` on `PendingOneShotQuery` so failure paths clean up
  consistently.

Done correctly, `RuntimeCore` becomes:

```rust
pub struct RuntimeCore<S, Sch> {
    schema_manager: SchemaManager,
    storage: S,
    scheduler: Sch,
    transport: Option<TransportHandle>,
    sync_sender: Option<Box<dyn SyncSender>>,
    inbox: SyncInbox,
    durability: DurabilityTracker,
    subscriptions: SubscriptionRegistry,
    tier_label: &'static str,
    sync_tracer: Option<SyncTracer>,
    auth_failure_callback: Option<AuthFailureCallback>,
}
```

The Scheduler / SyncSender trait boundaries do not move. The FFI scheduler
glue in `jazz-napi` and `jazz-wasm` continues to talk to the same traits.

Acceptance: existing `runtime_core/tests.rs` passes unchanged. Behavior of
`immediate_tick` and `batched_tick` is identical (verified by integration
tests, not internal mocks — see CLAUDE.md TDD note).

## Phase 6 — Subscribe as typed builder

The two-phase subscribe path (`runtime_core/subscriptions.rs:179-266`)
requires the caller to call `create_subscription` followed by
`execute_subscription`. The pair is enforced by convention only.

Replace with a state-machine type:

```rust
let pending = runtime.subscribe(query); // -> PendingSubscription
let handle  = pending.execute(callback); // consumes pending
```

`PendingSubscription` is `#[must_use]` and only exposes `execute(...)`.
Forgetting to call `execute` becomes a compiler warning; calling it twice
is impossible.

Acceptance: every existing call site updated in the same PR; no
`pub fn create_subscription` / `pub fn execute_subscription` remain on
`RuntimeCore`.

## Phase 7 — Centralize storage-flush flag

`mark_storage_write_pending_flush()` is called 21× in
`runtime_core/writes.rs` and 3× in `runtime_core/ticks.rs`. The flag is
set defensively after any mutation that touches the storage layer.

Replace with a `WriteGuard` returned by mutation entry points; the guard
sets the flag on construction and releases it normally. Mutation
functions stop knowing about the flag at all.

Acceptance: zero direct calls to `mark_storage_write_pending_flush` from
`writes.rs`; `batched_tick` flushes whenever the guard registry shows
unflushed work.

## Phase 8 — `query_manager/graph.rs` split

`graph.rs` is 4.6K LOC and conflates two phases:

- **compile** — turn relation IR into `QueryGraph` nodes; pure transform
- **execute** — `QueryGraph::settle()` and friends; row I/O via closure

Split into `graph_compile.rs` and `graph_execute.rs` with `graph/mod.rs`
re-exporting. `policy_eval` logic that today lives in `policy.rs` (3.1K)
folds into the existing `graph_nodes/` per-operator files where possible;
shared evaluation primitives stay in `policy.rs` but shrink.

Acceptance: no public type renamed; `cargo bench` (if present for query
hot paths) is within ±2% of pre-split numbers.

## Out of scope (capture only)

These came up in the audit but should be separate specs if pursued:

- WASM/native callback duplication in `subscriptions.rs:79-148` — needs
  thinking about whether the `Send` bound should be at the trait or the
  call site.
- Rejected-batch notification channel — currently polled via
  `drain_rejected_batch_ids`; piggybacking on the subscription delta
  stream is appealing but is a behavioral change, not cleanup.
- `query_manager/graph_nodes/magic_columns.rs` (47 LOC) — possibly inline
  into `graph_nodes/mod.rs`, but trivially low value on its own.

## Invariants

- All phases preserve the public API of `jazz-tools`.
- All phases preserve wire format, storage format, sync semantics.
- Tests are not rewritten; they move with their code. Per CLAUDE.md, an
  unexpectedly failing test is treated as a signal, not as something to
  edit out.
- Each phase is its own PR. Phases 1–4 are independent of one another;
  phases 5–8 build on the cleaner module layout from 1–4.

## Testing Strategy

- Each phase relies on the existing `cargo test --all-features` and
  `pnpm test` suites. No new test files are required for the moves
  themselves.
- For Phase 5 (RuntimeCore decomposition), the existing
  `runtime_core/tests.rs` exercises `immediate_tick`/`batched_tick`
  end-to-end and is the load-bearing safety net.
- For Phase 6 (typed subscribe), removing the old API at compile time
  is the test — no path can call `create_subscription` without the
  compiler complaining.
- Per CLAUDE.md, prefer e2e checks over unit tests added during the
  refactor; do not introduce internal-helper tests for the new types
  (`DurabilityTracker`, `SyncInbox`, `SubscriptionRegistry`) unless they
  contain non-trivial pure logic worth pinning.

## Phase ordering rationale

```
1 Junk drawer ────┐
                  ├── independent, mergeable in any order
2 Entry-point   ──┤
                  │
3 Builder enum ───┤
                  │
4 Storage split ──┘
                  │
                  v
5 RuntimeCore decomposition  (depends on 1: sync_tracer + clock have moved)
                  │
                  v
6 Typed subscribe            (depends on 5: SubscriptionRegistry exists)
                  │
                  v
7 WriteGuard                 (depends on 5: DurabilityTracker exists)
                  │
                  v
8 graph.rs split             (independent; sequenced last to avoid
                              merge conflicts with hot-path PRs)
```
