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

- No change to wire format, storage format, sync semantics, or query
  evaluation.
- No new features. Hypothetical future requirements do not justify abstractions
  introduced here.
- No tests rewritten. Tests move with their code; their assertions stay.

## Public API impact

Most phases preserve the public API of `jazz-tools` consumed by `jazz-napi`,
`jazz-wasm`, `jazz-rn`, and the TS client — either because they touch internal
code only, or because re-exports in `lib.rs` keep the existing import paths
resolving from the new module locations.

Two phases are **deliberate API breaks** coordinated with binding updates in
the same PR:

- **Phase 3 (builder collapse)** — replaces four public `ServerBuilder::with_*_storage`
  methods. Call sites in `crates/jazz-napi/src/lib.rs:1585, 1754, 1756` must
  be updated in the same PR.
- **Phase 6 (typed subscribe)** — replaces public
  `RuntimeCore::{create_subscription, execute_subscription}`. Call sites in
  `crates/jazz-napi/src/lib.rs:941, 983`, `crates/jazz-wasm/src/runtime.rs:1588, 1612`,
  and `crates/jazz-rn/rust/src/lib.rs:713, 755` (plus the regenerated UniFFI
  C++ shim under `crates/jazz-rn/cpp/`) must be updated in the same PR.

## Phase 1 — Junk-drawer relocation

Pure mechanical moves. Each one is a single PR.

| Move                | From                  | To                                                                                 |
| ------------------- | --------------------- | ---------------------------------------------------------------------------------- |
| Sync clock          | `monotonic_clock.rs`  | `sync_manager/clock.rs`                                                            |
| Batch lifecycle     | `batch_fate.rs`       | `sync_manager/batch_fate.rs`                                                       |
| Sync test recorder  | `sync_tracer.rs`      | `sync_manager/test_support.rs` (gated `#[cfg(any(test, feature = "test-utils"))]`) |
| HTTP route surface  | `routes.rs`           | `server/routes.rs`                                                                 |
| Native binding glue | `binding_support.rs`  | `query_manager/bindings.rs`                                                        |
| Test row history    | `test_row_history.rs` | `row_histories/tests.rs` (gated `#[cfg(test)]`)                                    |

Notes:

- The sync-test-recorder gate uses the existing `test-utils` feature defined
  in `crates/jazz-tools/Cargo.toml` (alongside `test`). No new feature is
  introduced.
- `binding_support` is currently `pub mod binding_support;` in `lib.rs:2` and
  imported as `jazz_tools::binding_support::*` from all three FFI crates
  (e.g. `jazz-napi/src/lib.rs:25`, `jazz-wasm/src/runtime.rs:21, 62`,
  `jazz-rn/rust/src/lib.rs:14`). The move keeps that import path stable via
  a `pub use crate::query_manager::bindings as binding_support;` re-export in
  `lib.rs` — so binding crates see no source change.

Top-level files that **stay** (true primitives, used by ≥3 subsystems):
`commit.rs`, `digest.rs`, `identity.rs`, `metadata.rs`, `object.rs`,
`row_format.rs`, `wire_types.rs`, `catalogue.rs`, `otel.rs`.

Acceptance:

- `cargo build --all-features` and `cargo test --all-features` pass.
- `pnpm build` and `pnpm test` pass (covers napi/wasm/rn re-export paths).
- All `jazz_tools::binding_support::*` imports in the three FFI crates resolve
  unchanged. No `pub use` from `lib.rs` is renamed; only the right-hand
  side of each re-export shifts.

## Phase 2 — Entry-point dedup

Small, independent fixes that remove parallel construction logic between
`client.rs` and `server/builder.rs`.

1. Share the `NODE_ENV` → "are we in prod?" classification between
   `main.rs::resolve_node_env_mode()` (`main.rs:24-29`) and
   `builder.rs::should_allow_unprivileged_schema_catalogue_writes()`
   (`builder.rs:305-310`). The two functions do not do the same thing — one
   gates auth defaults, the other gates unprivileged catalogue writes — but
   they share the same env-var match (`eq_ignore_ascii_case("production")`,
   anything else treated as dev). Extract just the classifier (e.g. a
   pub `node_env_mode() -> NodeEnvMode` in `server/env.rs`) and have both
   policy functions call it. The policy decisions on top stay separate.
   Scope is ~3 lines of true dedup; "leave it alone" is also defensible.
2. Move JWT key resolution from `main.rs:38-59` into `commands/server.rs` so
   the library no longer carries CLI-shaped logic.
3. Reuse one `reqwest::Client` between the main server (`builder.rs:155`) and
   the JWKS loader (`builder.rs:338-342`).
4. Delete `#[allow(dead_code)] pub app_id` on `ServerState` (`server/mod.rs:165`)
   — either use it in a routed handler or remove the field.

(An earlier draft proposed extracting a shared `build_schema_manager` from
`client.rs:64-82` and `server/builder.rs:217-238`. On closer reading those
two functions are not duplicates: the client uses an empty `SyncManager`,
always constructs `SchemaManager::new(...)` with the declared schema, uses
the `"client"` tier label, and always rehydrates from the catalogue. The
server uses `server_sync_manager()` (Edge+Global tiers, plus optional
unprivileged catalogue writes when `NODE_ENV != production`), splits on
Dynamic vs Fixed schema mode, uses `"prod"`, only rehydrates in Dynamic
mode, calls `require_authorization_schema()` afterwards to fail closed,
and returns `String` errors. Sharing them would require a function with
five orthogonal parameters that obscures rather than clarifies. Left as
two separate construction policies.)

(Promoting `allow_local_first_auth: true` into `AuthConfig::default()` was
considered but moved to _Out of scope_ below: `AuthConfig` derives `Default`
today, so the bool defaults to `false`, and `crates/jazz-tools/src/middleware/auth.rs:1134, 1617`
plus `crates/jazz-tools/tests/auth_test.rs:228` rely on that. Flipping it is
a behavior change, not cleanup.)

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

**This is a public API break.** `with_persistent_storage`,
`with_in_memory_storage`, `with_sqlite_storage`, and `with_rocksdb_storage`
are public on `ServerBuilder` (`crates/jazz-tools/src/server/builder.rs:106-137`)
and called directly by `crates/jazz-napi/src/lib.rs:1585, 1754, 1756`. The
napi crate must be updated in the same PR. WASM and RN do not currently call
these.

Acceptance:

- `cargo build --all-features` clean.
- `crates/jazz-napi/src/lib.rs` updated to the new `with_storage(...)` API in
  the same PR; `pnpm build` and `pnpm test` for the napi package pass.
- Tests unchanged in spirit (only the call shape rewritten).

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
  (`Option<seq>`). Exact API to be settled during implementation, but it
  must support: parking new entries, draining ready entries with their
  metadata (notably whether each entry writes storage — see
  `runtime_core/sync.rs:47` and `runtime_core/ticks.rs:632`), and reporting
  whether further work is pending. Orchestration concerns (marking
  storage-flush state, scheduling the next tick) stay in `RuntimeCore` —
  the inbox returns enough information for the orchestrator to do them.
- **`SubscriptionRegistry`** — owns `subscriptions`, `subscription_reverse`,
  `pending_subscriptions`, `pending_one_shot_queries`. Pure extraction;
  the one-shot leak claim from earlier audit notes is stale —
  `PendingOneShotQuery` already stores `subscription_id`
  (`runtime_core.rs:273-274`, populated at `subscriptions.rs:396`).

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

**This is a public API break.** `create_subscription` and
`execute_subscription` are public on `RuntimeCore`
(`crates/jazz-tools/src/runtime_core/subscriptions.rs:179-231`) and called
from all three FFI crates: `crates/jazz-napi/src/lib.rs:941, 983`,
`crates/jazz-wasm/src/runtime.rs:1588, 1612`,
`crates/jazz-rn/rust/src/lib.rs:713, 755` (with regenerated UniFFI bindings
under `crates/jazz-rn/cpp/`).

Two viable shapes:

- **Coordinated rewrite** — change all three FFI crates in the same PR to
  use the new state-machine API. Largest blast radius but cleanest end state.
- **FFI-internal forwarding** — keep the new `subscribe(...).execute(cb)` API
  at the `RuntimeCore` level but have each FFI wrapper expose a flat
  `create_subscription` / `execute_subscription` pair internally to preserve
  its own JS/UniFFI signatures. The "no-pair-misuse" guarantee then applies
  inside `jazz-tools`; the FFI surface keeps its current shape until each
  binding's API is updated independently.

The coordinated rewrite is preferred unless the FFI breakage is judged too
costly at the time of implementation.

Acceptance: every Rust call site updated in the same PR; no
`pub fn create_subscription` / `pub fn execute_subscription` remain on
`RuntimeCore`. If the FFI-internal-forwarding shape is chosen, document
that explicitly in each binding's wrapper.

## Phase 7 — Centralize storage-flush flag

`mark_storage_write_pending_flush()` is currently called from four files:

| File                     | Calls                            |
| ------------------------ | -------------------------------- |
| `runtime_core/writes.rs` | 12                               |
| `runtime_core.rs`        | 5 (incl. the setter at line 427) |
| `runtime_core/ticks.rs`  | 3                                |
| `runtime_core/sync.rs`   | 1                                |

The flag is set defensively after any mutation that touches the storage
layer. Replace with a `WriteGuard` returned by mutation entry points; the
guard sets the flag on construction and releases it normally. Mutation
functions stop knowing about the flag at all.

Acceptance:

- The only direct callers of the flag setter are the helper that constructs
  a `WriteGuard` and (if still useful) `runtime_core.rs` itself for cases
  that genuinely have no guard to attach to. No direct calls remain in
  `writes.rs`, `ticks.rs`, or `sync.rs`.
- `batched_tick` flushes whenever the guard registry shows unflushed work.

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

- **`AuthConfig::default()` flipping `allow_local_first_auth` to `true`** —
  this is a behavior change, not cleanup. Today the field defaults to `false`
  via the derived `Default`, and tests and runtime paths in
  `crates/jazz-tools/src/middleware/auth.rs:1134, 1617` plus
  `crates/jazz-tools/tests/auth_test.rs:228` rely on that. If the new-app
  default should be `true`, that needs its own spec and a sweep of all
  callers that construct `AuthConfig::default()` for "no creds" scenarios.
- WASM/native callback duplication in `subscriptions.rs:79-148` — needs
  thinking about whether the `Send` bound should be at the trait or the
  call site.
- Rejected-batch notification channel — currently polled via
  `drain_rejected_batch_ids`; piggybacking on the subscription delta
  stream is appealing but is a behavioral change, not cleanup.
- `query_manager/graph_nodes/magic_columns.rs` (47 LOC) — possibly inline
  into `graph_nodes/mod.rs`, but trivially low value on its own.

## Invariants

- Phases 1, 2, 4, 5, 7, 8 preserve the public API of `jazz-tools`. Phases
  3 and 6 are deliberate API breaks; they coordinate the corresponding
  binding updates in the same PR (see _Public API impact_ above).
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
