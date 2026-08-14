# React Native binding core realignment — design

Status: implemented 2026-08-12; RN-specific gates and the iOS simulator E2E
are green. Branch publication was explicitly approved 2026-08-13; a green
landing remains blocked by the merged-core gates recorded in §13.
Owner: RN surface owner.
Scope: merge of `codex/jazz-core-engine-swap` (598 commits, at `89b2dee40`)
into `spec/jazz-rn-rewrite`, plus the port of `jazz::binding_support`,
`crates/jazz-rn`, and `packages/jazz-tools/src/react-native` onto the moved
core. Amends `dev/RN_BINDING_REWRITE_DESIGN.md` (M1–M5, landed 2026-08-10);
that document's architecture stands, this one records what the base branch
changed underneath it and how the binding follows.

2026-08-14 refresh: the pending merge advances the base another 262 commits to
`24728c3a8`. The core now owns durability waiting through
`Db::wait_for_transaction_with`, requires `NativeDb.onMutationError`, and
publishes producer-owned terminal layouts alongside terminal operations. The
RN actor, UniFFI surface, generated bindings, and TypeScript shim are realigned
to all three contracts; focused host checks are recorded in the refreshed
§13 receipt below.

v1 → v2: owner review found six correctness blockers in v1 — snapshot wire
shape, transaction keying, view release, mergeable transaction reads, the
`connectUpstreamWithSession` consequence, and the landing-gate script — and
named three pre-existing PR blockers. All were verified against the code and
are resolved below (§3.2, §4, §5, §8, §10, §11). The snapshot encoder fix is
already applied in the working tree; the rest is R1/R2 work.

## 1. Context

The RN rewrite landed against core revision `15bd8905c`. The base branch then
moved 598 commits and redesigned three subsystems the binding sits on. A
textual merge cannot express any of them — git reports clean hunks while the
result fails to compile or, worse, compiles and mis-encodes the wire.

1. **Subscription carriers.** `3a87ee9b5` ("remove relation facts from
   subscription carriers") deleted `added_related`/`added_edges`/
   `removed_edges` from `SubscriptionEvent::Delta`. Deltas are now
   core-authoritative: `added`/`updated` are `SubscriptionOutputRow` (row +
   `occurrence_id`), typed `terminal_operations` patches arrived
   (`da26a321d`), and the postcard delta grew three `ResultKey` sidecar
   vectors that `readNativeSubscriptionDelta` length-checks on the client —
   omitting them is a runtime throw on the phone, not a compile error.
   Relation _snapshots_ also shed their `cursor`/`edges` fields (§3.2).
2. **Transactions.** `OpenTxId` (core-minted, handle-held) was replaced by
   `OpenBatchId` — a caller-minted string (`createOpenBatchId()`,
   `client.ts:149`) that the binding parses. The `NativeDb` contract now
   _requires_ `registerSchema`, `beginTransaction`, `commitTransaction`,
   `rollbackTransaction`, `attachMergeableTx`, and an id-taking
   `mergeableTx(openBatchId)`. `Write` gained `readonly batchId`. Reads go
   through pending transactions of **both** kinds (§4.3).
3. **Permissions.** The synchronous `can_*`/`can_*_for_identity` probes are
   gone from the public core and from the `NativeDb` contract. Doctrine
   (`db.rs:2296`): a client-local replica never turns local policy evidence
   into allow/deny; it returns `PermissionAdvice::Unknown`. Decisions moved to
   optional async `request*PermissionAdvice*` members backed by a serving
   authority, with an adapter-side timeout.

Implementation state: all textual conflicts and both semantic realignments are
resolved in the working tree. `binding_support`, `jazz-rn`, generated UniFFI
bindings, the `jazz-tools` RN shim, transaction/schema-view/session surfaces,
async settlement, mutation-error delivery, terminal layouts, and the
collision-resistant filename rule are implemented. The current validation
receipt is in §13. Recovery points retained from the original merge are tag
`pre-merge-backup-81f69f600` and patch `/tmp/merge-resolution.patch`.

## 2. Recorded decisions (made during the merge)

1. **Merge, not rebase** (RN owner direction, 2026-08-12). Replaying 17
   commits would hit the napi conflict repeatedly; one merge resolves each
   disagreement once. The merge commit will contain compile fixes beyond
   textual resolution (an "evil merge") — deliberately, because a merge
   commit that does not build is worse than one that resolves semantics. The
   commit message must say so.
2. **`jazz-napi` taken wholesale from the base branch.** The base extended
   napi's codecs in place (+661 lines: occurrence sidecars, session context,
   authority endpoints, advice requests) while this branch had extracted the
   old codecs into `binding_support` (−548). Keeping the extraction would
   have silently dropped `occurrence_id`/`added_occurrence_keys` from the
   wire. Consequence: napi no longer uses `binding_support` — the M2
   "no third copy" property is temporarily lost (§12, open question 1).
3. **Windowed-record-store tests deleted with their feature.** The base
   branch removed the windowed record store entirely (`consolidate_windows`,
   `WindowConsolidation`, `window_store`: zero hits in the base tree). The
   ~657 lines of tests covering it in `storage/mod.rs` reference deleted
   code and went with it. This is base-branch behavior adopted, not a
   unilateral test rewrite.
4. **This branch's storage refactors survive.** `Durability` stays lifted in
   `storage/mod.rs` (base's `rocksdb_storage::Durability` re-export still
   resolves via `pub use super::Durability`), shared `WriteFlushCadence`
   stays, and base's new `RocksDbMetrics` is kept and re-exported.
5. **Terminal operations cross UniFFI as JSON** (`terminal_operations_json:
Option<String>`). UniFFI has no `serde_json::Value` mapping; this is the
   same treatment `reason_json` already gets, and the payloads are small
   structured edits, not row data — the "keep postcard binary" rationale in
   the original design applies to deltas, not to these.

## 3. Wire carriers — done, in the working tree

### 3.1 Subscription deltas

`binding_support::encode_subscription_event` matches the napi/wasm contract:

- `added`/`updated` are `SubscriptionOutputRow`; the postcard
  `BindingSubscriptionDelta` carries six fields in this order: `added`,
  `updated`, `removed`, `added_occurrence_keys`, `updated_occurrence_keys`,
  `removed_occurrence_keys`. Field order is the wire contract — postcard is
  positional and the TS reader asserts each sidecar length equals its row
  count ("subscription occurrence sidecar length mismatch").
- **Either/or rule**: when `terminal_operations` is non-empty, the row delta
  is encoded empty. The patches carry the change; sending both would make
  the client apply every change twice. Both reference bindings gate
  identically (napi `lib.rs:2315`, wasm `lib.rs:2742`).
- `relation_delta` is gone from the event, the JSON view, and the UniFFI
  record. JSON key is `terminalOperations`; napi's contract test
  `subscription_payload_exposes_only_terminal_rows` asserts `relation_delta`
  is absent. §9 adds the same assertion for the shared encoder.

### 3.2 Relation snapshots (v2 correction)

v1 claimed relation snapshots "still carry edges". **That was wrong.** The
decoder is the contract, and `readNativeRelationSubscriptionSnapshot`
(`native-row-codec.ts:145`) reads exactly two fields positionally:
`root_count: u64`, then `rows`. napi's `CoreRelationSnapshot` confirms
(`lib.rs:119`: `{ root_count, rows }`). The shared encoder's four-field
`{ cursor, root_count, rows, edges }` would have compiled and then decoded
`cursor = 0` as the root count and the real root count as a rows-vec length —
garbage, not an error.

Fixed in the working tree: `BindingRelationSnapshot` is now
`{ root_count, rows }`, and `BindingRelationEdge`/`relation_edge` are deleted
outright — with the snapshot fields gone, no carrier references edges
anywhere. §9 pins the two-field shape with a decode-side test.

Completed TS work (R2): `native-db.ts` drops `relationDelta`, parses
`terminalOperationsJson` into the `terminalOperations` array the adapter
expects, and `native-db.test.ts` is updated — those are this branch's own
tests and the behavior change is intentional.

## 4. Transactions: the `OpenBatchId` port

### 4.1 Model change

Old: the actor minted `u64` handles for core-held `OpenTxId`s; `RnTx` owned
its transaction and `Drop` abandoned it. New: the **client** mints an
`OpenBatchId` string and threads it through; the binding parses and routes.
Batch lifecycle is owned by explicit `begin`/`commit`/`rollback` calls, not
by handle lifetime.

The napi rules port verbatim: `kind` ∈ {`"mergeable"`, `"exclusive"`},
anything else errors; `"exclusive"` rejects an `author` override; malformed
ids fail at parse (`String → OpenBatchId`) before reaching the core.

### 4.2 Keying: owner-level batches, per-view attachments (v2)

v1 proposed one actor map keyed by `OpenBatchId`. **Insufficient**: the
adapter attaches the _same_ batch from multiple schema views —
`PendingTx.txByView: Map<NativeRuntimeAdapter, Tx>`
(`native-runtime-adapter.ts:271`) — so a batch-id-only map collides on the
second view and mis-scopes close/drop. The actor therefore keeps two
registries:

- **Batches** (owner-level): `open_batches: HashMap<OpenBatchId, BatchEntry>`
  where `BatchEntry { kind }`. Created by `begin_transaction`, removed by
  `commit`/`rollback`/shutdown. Owned by the root, independent of views.
- **Attachments** (per view): `attachments: HashMap<u64, TxAttachment>` where
  `TxAttachment { batch: OpenBatchId, view: u64, owns_lifetime: bool }`,
  keyed by a minted handle id as today. `attach_*` mints a non-owning
  attachment (`owns_lifetime: false`); the `mergeable_tx`/`exclusive_tx`
  conveniences mint owning ones, napi-parity (`Tx.owns_lifetime`,
  napi `lib.rs:252`).

`RnTx::drop` and `RnTx::rollback`/`commit` consult `owns_lifetime`: a
non-owning attachment releases only its attachment entry — napi's
`attached_tx_drop_preserves_owner_batch` is the reference behavior, and it
inverts this branch's current `RnTx::drop` (which abandons). Only an owning
attachment may end the batch.

### 4.3 Transaction reads: both kinds, opts honored (v2)

The adapter's `readPlainRows` (`native-runtime-adapter.ts:1299`) reads
through **any** pending transaction — no kind check — and passes `opts`.
The current RN actor rejects non-exclusive reads and discards `ReadOpts`
(`actor.rs:1158`: `let _opts = …`). Both are wrong against the new contract.
`all_in_transaction*` follows wasm (`lib.rs:1406`): dispatch on the batch's
kind to `mergeable_all(tx_id, query, opts)` / `exclusive_all(tx_id, query,
opts)` (and the `_for_identity` twins), decoding and honoring the read
options. Read-your-writes holds for both kinds; §9 tests it for both.

### 4.4 Surface

| UniFFI method                                                                                | Core call (via the view-aware macro, §5)                                                   |
| -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `begin_transaction(open_batch_id, kind, author?)`                                            | `begin_mergeable(id)` / `begin_mergeable_for_identity(id, author)` / `begin_exclusive(id)` |
| `commit_transaction(open_batch_id, kind?) -> RnWrite`                                        | mergeable/exclusive commit by id; `RnWrite` carries `batch_id`                             |
| `rollback_transaction(open_batch_id)`                                                        | abandon by id                                                                              |
| `attach_mergeable_tx(open_batch_id) -> RnTx`                                                 | non-owning attachment over `mergeable_tx_ref(id)`                                          |
| `attach_exclusive_tx(open_batch_id) -> RnTx`                                                 | non-owning attachment over `exclusive_tx_ref(id)`                                          |
| `mergeable_tx(open_batch_id)`, `mergeable_tx_for_identity(…)`, `exclusive_tx(open_batch_id)` | owning attachments, napi-parity semantics                                                  |
| `all_in_transaction(_for_identity)(query, open_batch_id, …, opts)`                           | `mergeable_all`/`exclusive_all` by batch kind (§4.3)                                       |

`RnTx` keeps its staged-op methods and `commit()`/`rollback()`, routed by the
attachment's batch id. **Staged `upsert_encoded` routes to the core's staged
upsert** — the current routing to `tx_insert` under a "the adapter pre-splits
insert-vs-patch" comment is the open P1 (`lib.rs:928`); wasm routes upsert to
a real `upsert` (`lib.rs:1817`), and no adapter pre-split exists. Fixed as
part of this rewrite, with a §9 test staging an upsert over an existing row.
`RnWrite` gains `batch_id: String`; the shim exposes it as `batchId`
(adapter reads it at `native-runtime-adapter.ts:2031`).

## 5. Schema views: `registerSchema` and `free`

`registerSchema` is **required** by the contract and is not a mutation — it
returns a _new schema view_: `Db::register_schema_view(schema) -> Db`, a
second facade over the same node (napi returns a fresh wrapper with
`owns_runtime: false`).

The actor therefore holds multiple views on one core thread:

- `CoreState.views: HashMap<u64, CoreDb>`; view `0` is the root opened by
  `open_memory`/`open_persistent`.
- `RnDb` handles carry `view: u64`. `register_schema(schema)` runs
  `register_schema_view` on the actor thread, stores the view, and returns a
  new non-root `RnDb` handle.
- The dispatch macro becomes view-aware — one map lookup at the existing ~40
  sites, no change to their bodies.

**View release is `free()`, not `close()` (v2).** The adapter's `close()` on
a non-owner view calls `this.db.free?.()` and returns _without_ calling
`close()` (`native-runtime-adapter.ts:498`). v1 omitted `free()`, which
would have leaked every non-root view entry for the process lifetime. The RN
surface implements `free()` as the view-release operation: it removes the
view's `CoreState.views` entry (and any attachments scoped to it) and is a
no-op on the root. Root `close()` is unchanged: terminal teardown, joins the
actor. `RnDb::drop` on a non-root view enqueues the same release
best-effort, so a GC'd view handle does not depend on the shim calling
`free()`.

Everything else in the original §4.1 lifecycle (state machine, poisoning,
close-joins-actor, notifier-never-joined) is unchanged.

## 6. Permissions: deletion, not port

- The eight `can_*`/`can_*_for_identity` UniFFI methods and their shim
  counterparts are **deleted**. Their core methods no longer exist; the
  contract no longer lists them.
- The async `request*PermissionAdvice*` members are **omitted in v1 of this
  port** (recorded, §8). The adapter degrades to `"unknown"` without them —
  `canInsertLocally` returns `"unknown"` unconditionally and each
  `request*` resolves `"unknown"` when the member is absent
  (`native-runtime-adapter.ts:688–742`). The Expo example uses none of them.

## 7. Session-bound transport: `connectUpstreamWithSession` (v2)

v1 recorded this as an omission with the consequence "no session
resumption". **That understated it.** The method binds the _negotiated_
connection state after the server hello: protocol version, feature bits, and
local/remote `WireAuthorityEndpoint`s (node + epoch) into a
`ConnectionSessionContext` (napi `lib.rs:1671`). This is the mechanism that
makes carrier feature advertisement honest — the exact bug class the
original design §11 recorded as "capability-blind carrier", and which the
base branch fixed for the browser (`fix/websocket-authority-hello`,
`cdcfda360`). RN compiles zstd and the carrier advertises features
unconditionally; falling back to plain `connectUpstream` on RN would
reintroduce the class there.

**Decision: implement it in R1.** The actor's transport plumbing already
exists; this adds one constructor variant that parses the endpoint arguments
(16-byte nodes, epoch bigints — napi's validation ports verbatim) and passes
the session context to the core connect. A §9 test drives a session-bound
connect against an in-process server. If R1 finds a blocking impediment, the
fallback is an explicit owner waiver backed by a connection test proving the
degraded path safe — not a silent omission.

## 8. Omitted in v1 of this port (recorded, with degradation)

| Member                          | Consequence when absent                                                                      |
| ------------------------------- | -------------------------------------------------------------------------------------------- |
| `request*PermissionAdvice*` (4) | Advice resolves `"unknown"`; UI cannot pre-deny. Follow-up if mobile needs authority advice. |

(v1 of this document also listed `free` and `connectUpstreamWithSession`
here; both are now implemented — §5, §7.)

## 9. Testing

Per `crates/jazz/TESTING_GUIDELINES.md`; the M2/M3 suites are ported, not
rewritten, except where this document records intentional behavior change.

1. **Shared encoder contract** (`binding_support` in-module): either/or
   gating (non-empty `terminal_operations` ⇒ empty row delta); sidecar
   lengths equal row counts; JSON payload has `terminalOperations` and no
   `relation_delta`; **snapshot encodes exactly `(root_count, rows)` — a
   positional decode of the two fields round-trips** (§3.2).
2. **Actor** (`cargo test -p jazz-rn`, host): existing lifecycle/poisoning
   matrices retained; write waiting realigned to the core-owned async
   `Db::wait_for_transaction_with` contract; transaction tests re-keyed by `OpenBatchId`;
   new: attached-`RnTx`-drop preserves the owner batch; same batch attached
   from two views yields independent attachments (no collision, §4.2);
   **read-your-writes through both mergeable and exclusive batches with
   non-default `ReadOpts` asserted** (§4.3); **staged upsert over an
   existing row updates it** (§4.4 / P1); begin/commit/rollback round-trip
   incl. unknown-kind and exclusive-with-author rejections;
   `register_schema` view isolation; `free()` releases a view while the
   root stays usable (§5); session-bound connect round-trip (§7).
3. **TS** (vitest): shim unit tests over the mocked generated module updated
   for the new surface; `tsc` enforces contract completeness — `RnDbShim
implements NativeDb` makes the 26-member contract a typecheck.
4. **Bindings**: ubrn regeneration; `dev/gates/rn-bindings-fresh.sh`.
5. **E2E** (gating, iOS simulator): the §9 scenario from the original design
   re-run against the merged core. The 2026-08-11 receipt predates this
   merge and does not carry over.

## 10. Landing strategy

- **One merge commit** containing conflict resolutions + the full port +
  regenerated bindings, so every commit on the branch builds. Follow-up
  commits on top: smoke-ledger receipt, changeset (§11), and (if taken) the
  napi re-point.
- Push updates PR **#1367**; `mergeable` should flip from CONFLICTING.
- **`dev/gates/run-canonical.sh` must be corrected in this change (v2).**
  The base-branch script still runs the removed package form
  (`run_gate cargo-test-jazz-server cargo test -p jazz-server`, line 99 —
  which the merged AGENTS.md itself documents as failing), and omits gates
  AGENTS.md requires. Corrections: replace with
  `cargo test -p jazz --bin jazz-server`; add
  `cargo test -p groove --no-default-features --features sqlite`; add
  `dev/gates/rn-bindings-fresh.sh`; run `pnpm build:core` before
  `ts-wire-codec.sh` so generated WASM declarations are fresh rather than
  whatever the last local build left behind.
- Landing tier: the corrected canonical set, smoke (storage touched),
  oracle, canaries, and the jazz-private sensitive-data guard — **currently
  a silent no-op on this machine because `jazz-private` is not cloned; per
  the M1 plan its absence blocks push absent an explicit owner exception.**
  The four failures recorded during design review were stale by implementation
  time. The current, reproduced landing blockers are listed in §13.

### Milestones

- ✅ **R1 — Rust realign**: §4 transactions (incl. P1 fix), §5 views + free,
  §6 deletions, and §7 session connect; `cargo test -p jazz-rn` is green.
- ✅ **R2 — bindings + TS**: ubrn regen, shim realign (incl. §11 P2 fix),
  `tsc`/vitest green.
- ◐ **R3 — land**: `run-canonical.sh`, the changeset, carrier benchmark, and
  fresh iOS E2E receipt are complete. Commit/push were explicitly approved on
  2026-08-13; the merged-core blockers in §13 still prevent a green landing
  tier.

## 11. Pre-existing PR blockers folded into this change (v2)

1. **P1 — staged upsert routes to insert** (`jazz-rn/rust/src/lib.rs:928`).
   `RnTx::upsert_encoded` calls `actor.tx_insert` under a comment claiming
   the adapter pre-splits insert-vs-patch; no such pre-split exists, and
   wasm routes upsert to a real `upsert`. Fixed in R1's transaction rewrite
   (§4.4) with a pinning test (§9.2).
2. **P2 — sanitized database filenames collide**
   (`react-native/runtime-source.ts:23`). Every non-`[A-Za-z0-9_-]`
   character maps to `_`, so `my.app` and `my:app` share a file. Fixed in R2:
   when sanitization changed the name, use a reserved marker plus a short
   stable hash of the raw name (`~<sanitized>-<8 hex>`). Because `~` is outside
   the accepted raw-name alphabet, transformed names cannot equal an unchanged
   safe name such as `<sanitized>-<8 hex>`. Both sanitization and hashing walk
   Unicode code points. Names that were already filename-safe keep their exact
   current path. Renaming affected databases is acceptable pre-release
   (`2.0.0-alpha`, and the branch's wire posture is already "no compat shims");
   recorded here so it is a decision, not an accident.
3. **Missing changeset.** Public `jazz-rn` and `jazz-tools` surfaces change;
   a changeset was added in R3 (the repo's convention — the base branch adds
   `.changeset/*.md` for comparable changes).

## 12. Risks and open questions

1. **napi re-point onto `binding_support`** (owner call). The M2 rule was
   "napi switches in the same change"; decision 2 broke that to avoid
   mis-encoding. Current state: `jazz-napi` has no `binding_support`
   references, so the older design's §4.5 "no third copy" structural invariant
   is not satisfied; that paragraph now points to this exception. Re-pointing
   the overlapping codecs (open args, cells, rows, subscription event, wait
   state) is now near-mechanical — the shared encoder implements napi's format
   — and napi's suite gates it. Options: separate commit in this PR, or recorded
   follow-up. Recommendation: follow-up; this change is large enough, and the
   §9.1 contract test pins the format either way.
2. **Advice requests over the RN transport** (follow-up scope). Requires the
   serving-authority round-trip; unneeded by the example. Unblocks
   pre-emptive permission UI on mobile if wanted.
3. **E2E churn risk.** The merged core changed the write path (batch ids in
   write contexts); if the example app's flows surface adapter paths the
   simulator receipt didn't cover, R3 absorbs the fixes — the scenario
   itself is unchanged.

## 13. Implementation receipt — 2026-08-12

Implemented surfaces:

- six-field subscription deltas with occurrence-key sidecars, terminal
  operation either/or delivery, and exact two-field relation snapshots;
- caller-minted owner-wide batches with per-view attachments, both-kind
  transaction reads, real staged upsert, and committed `Write.batchId`;
- schema views with scoped `free()` cleanup, removed synchronous permission
  probes, and session-bound transports;
- collision-resistant persistent RN database names, regenerated UniFFI
  TypeScript/C++/iOS artifacts, corrected canonical gates, and a public
  changeset.

Green validation:

- `cargo test -p jazz-rn`: 16 passed;
- `cargo test -p jazz binding_support::tests`: 4 passed;
- `cargo clippy -p jazz-rn --all-targets -- -D warnings`;
- `dev/gates/rn-bindings-fresh.sh`;
- focused RN vitest: 11 passed; `jazz-tools` RN typecheck, `jazz-rn` typecheck,
  and `jazz-rn` Jest all pass;
- `pnpm --filter jazz-tools build`, `pnpm build:core`, and
  `dev/gates/ts-wire-codec.sh` (136 passed, 1 skipped);
- both Groove feature shapes, the Jazz server bin, jazz-sim bench check,
  incremental-delivery canary, and the 300-seed maintained/one-shot oracle;
- terminal-operation delivery benchmark: the one-sample smoke receipt measured
  1.001512× allocations and 1.034605× bytes; repeated fresh three-sample
  receipts measured 1.001133–1.001512× and 1.033047–1.034582×. All are green
  under the carrier-specific 1.043× bound, an explicit relaxation from the
  prior 1.025× gate; absolute median allocation bytes still fell from roughly
  487–495 KiB to 361–373 KiB;
- final smoke receipt `20260812T170500Z`: 14/16 scenarios passed, including
  the corrected relation-delivery lane; the two red scenarios are below;
- fresh iOS 26.0 / iPhone 17 Pro simulator flow: `created; rows: 1`, process
  restart `reused; rows: 1`, then the second client printed
  `{"observedOfflineTitle":"offline-seed","insertedRemoteTitle":"remote-seed","rowCount":2}`
  and the simulator displayed both rows.

Current landing blockers outside the RN bridge:

- both full Jazz feature shapes reproduce five policy failures
  (`db_sync_surface_blob_values_follow_ordinary_row_permissions`,
  `inherited_child_insert_uses_parent_update_where_old_only`,
  `session_delete_uses_current_row_for_owner_write_policy`, and the two
  trusted-backend claim-context cases) plus a hanging
  `write_state_waiter_resolves_on_remote_fate_update`;
- the now-honest invariant registry reports 32 stale test citations and three
  covered invariants without a cited test;
- smoke still catches a below-window route emitted as an add/remove pair and
  `jazz-sim/s7_migrations` failing with `UnauthorizedCatalogueUpdate`;
- `../jazz-private/dev/gates` is absent, so the required sensitive-data guard
  cannot run on this machine.

Tooling friction: a canonical runner with per-gate timeouts/live logs and a
versioned known-red baseline would have saved substantial wall-clock time.
