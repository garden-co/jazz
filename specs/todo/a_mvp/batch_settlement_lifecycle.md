# Batch Settlement Lifecycle — TODO (MVP)

This spec is written for readers who know the status quo on `main`, in particular
[Batches — Status Quo](../../status-quo/batches.md) and
[Sync Manager — Status Quo](../../status-quo/sync_manager.md).

Today there is no single answer to the question "is this batch settled?". Four
call sites answer it four different ways, and for a local-tier browser worker
none of the four is ever satisfiable. Every direct batch the app has ever
committed therefore stays "pending" forever. The pending set grows monotonically
with total writes, the worker→main reload replay grows with it, and recovering
batch membership for the pending set degrades into repeated full row-store
scans. The result is the observed reload stall: a `todo-react` stress dataset
takes seconds (or never finishes) to come back after refresh, while deleting
browser storage and re-syncing from a server makes loading instant — because
server-synced data arrives with no local submissions or fates at all.

This spec defines one settlement lifecycle: a single per-runtime **settlement
target**, atomic **retirement** of batch bookkeeping when a batch reaches it,
persisted **membership** until retirement, and `wait(tier)` semantics that are
resolvable by construction or fail fast.

## Related

- [Batches — Status Quo](../../status-quo/batches.md)
- [Sync Manager — Status Quo](../../status-quo/sync_manager.md)
- [Batched Tick Orchestration — Status Quo](../../status-quo/batched_tick_orchestration.md)
- [Duplicated batch bookkeeping storage](../issues/duplicated-batch-bookkeeping-storage.md)
- [Upstream-connected signalling is optimistic](../issues/worker-upstream-connected-is-optimistic.md)
- [Unsealed pending rows cleanup](../issues/unsealed-pending-rows-cleanup.md)

## Why this exists

### Four competing terminal predicates

1. **`sealed_batch_still_needs_edge_reconciliation`**
   (`crates/jazz-tools/src/runtime_core/writes.rs:803`) — hard-codes
   `confirmed_tier < EdgeServer`. A `DurableDirect { confirmed_tier: Local }`
   fate therefore _always_ "still needs reconciliation", with or without a
   configured server. This gate selects the worker→main reload replay set
   (`local_batch_records_for_worker_sync`, `writes.rs:818`) and feeds
   `pending_batch_ids_needing_reconciliation`.

2. **`local_batch_record_needs_fate_reconciliation`**
   (`crates/jazz-wasm/src/worker_bridge.rs:64`) — the same hard-coded
   `< EdgeServer` gate, duplicated on the main side.

3. **`retained_batch_terminal_tier`**
   (`crates/jazz-tools/src/runtime_core/sync.rs:4`) — dynamic: `GlobalServer`
   when any server is _registered_ (`has_connected_servers` is
   `!servers.is_empty()`, `crates/jazz-tools/src/sync_manager/mod.rs:310` —
   socket health is irrelevant), otherwise the runtime's own max tier. Main
   always registers the worker as its upstream server
   (`crates/jazz-wasm/src/worker_bridge.rs:144`), so on main this is always
   `GlobalServer`. On the worker, a stale `serverUrl` (e.g. a leftover
   `VITE_JAZZ_SERVER_URL` while `jazzPlugin({ server: false })`) also flips it
   to `GlobalServer`.

4. **Submission pruning on settlement**
   (`crates/jazz-tools/src/sync_manager/inbox.rs:965`) — prunes only on
   `Rejected` or `confirmed_tier >= GlobalServer`.

A browser worker's durability tier is `Local`. Gates 1, 2, and 4 are
unsatisfiable for it by construction; gate 3 is unsatisfiable whenever any
upstream is registered, reachable or not.

### The pending set is immortal _and_ its membership is expensive to recover

When the worker settles a sealed batch received from main, `settle_sealed_batch`
deletes the sealed submission (`inbox.rs:908-912`) but keeps the fate at
`Local` — which gate 1 says is non-terminal. On the next reload,
`local_batch_records_for_worker_sync` walks every such fate
(`writes.rs:857-879`) and calls `local_batch_rows(batch_id)` for each
(`writes.rs:864`). With the submission deleted, the in-memory cache empty after
restart, and no persisted `LocalBatchRecord` on the worker (normal writes never
persist one — the only `upsert_local_batch_record` caller is main-side
hydration), `local_batch_rows` falls through to its last resort: a full scan of
every row locator and every history row
(`crates/jazz-tools/src/runtime_core/ticks.rs:138-168`). That is
O(batches × total rows) at every worker startup. The same fallback is reachable
from the `retain` at the end of `pending_batch_ids_needing_reconciliation`
(`sync.rs:99`), which itself re-derives the pending set with whole-database
scans on every `add_server`.

The two halves of the lifecycle sabotage each other: settlement deletes the
membership artifact that the never-terminating reconciliation path then has to
reconstruct forensically.

### `wait({ tier: "local" })` can hang forever

A batch waiter resolves only when a `BatchFate` with a sufficient
`confirmed_tier` is observed (`runtime_core/durability.rs:69`). There are
exactly two producers:

- **Self-confirmation at commit** (`writes.rs:1091` via
  `local_write_confirmed_tier`, `writes.rs:164`) — disabled when
  `set_non_durable_client_runtime` was called (`runtime_core/mod.rs:511`),
  which is how browser main runs (`crates/jazz-wasm/src/runtime.rs:1443`).
- **A durable peer settling the seal** — but if the settling peer has no
  durability tiers, `settle_sealed_batch` silently `return`s without producing
  any fate (`inbox.rs:879-881`).

So in any configuration where the committing runtime is non-durable and no
tiered peer answers (serverless test, worker missing or untiered), the waiter
is structurally unresolvable. No timeout, no error.

## Before / After

### Before (`main`)

| Topic                       | Status quo                                                                  |
| --------------------------- | --------------------------------------------------------------------------- |
| Terminal predicate          | four divergent definitions, three hard-coded to `EdgeServer`/`GlobalServer` |
| Local-only batch settlement | unreachable; submissions/fates retained forever                             |
| Pending-set membership      | reconstructed via fallback chain ending in full row-store scan              |
| Pending-set derivation      | re-derived by whole-DB scans on every `add_server`                          |
| Worker→main reload replay   | every batch ever committed                                                  |
| `wait(tier)`                | can hang forever when no producer exists for the requested tier             |
| Upstream presence           | `!servers.is_empty()` — registration, not connectivity or configuration     |

### After (this spec)

| Topic                       | Proposed                                                                |
| --------------------------- | ----------------------------------------------------------------------- |
| Terminal predicate          | one per-runtime **settlement target**, consumed by all four call sites  |
| Local-only batch settlement | settles and retires at commit (target = own tier)                       |
| Pending-set membership      | persisted until retirement; full-scan fallback removal deferred         |
| Pending-set derivation      | maintained incrementally; `add_server` reads it, never re-derives it    |
| Worker→main reload replay   | pending (unsettled) batches only — empty on a healthy local-only reload |
| `wait(tier)`                | resolvable by construction, or an immediate error when no path exists   |
| Upstream presence           | derived from configuration intent, validated once                       |

## Design

### 1. One settlement target per runtime

Each runtime derives a single `settlement_target: DurabilityTier` from
configuration at startup (and re-derives it only on explicit configuration
change, not per call):

- an upstream server is configured → `GlobalServer`
  (or `EdgeServer` if that is the strongest configured upstream);
- no upstream configured → the runtime's own max durability tier
  (`Local` for a browser worker);
- a non-durable client (browser main) inherits the target of its durable peer
  (the worker), since that peer is the only fate producer it has.

All four predicates above become consumers of this one value. The hard-coded
`< EdgeServer` comparisons in `writes.rs:803` and `worker_bridge.rs:64` are
deleted. `retained_batch_terminal_tier` becomes the accessor for the stored
target rather than a per-call inference from `servers.is_empty()`.

Whether an upstream "is configured" is a configuration-validation question, not
a live-socket question. A `serverUrl` that fails validation (the stale
`VITE_JAZZ_SERVER_URL` case) must surface as a startup error/warning rather
than silently raising the settlement target to a tier the runtime will never
reach.

### 2. Settlement is atomic retirement

When a batch's fate reaches `settlement_target`, all of its pending-set
bookkeeping retires together, in one storage transaction:

- delete the `SealedBatchSubmission`;
- delete the pending membership record (see §3);
- remove the batch from the in-memory pending set and
  `local_batch_record_cache`;
- keep the `BatchFate` row as a terminal tombstone (idempotency: a late
  duplicate seal or fate must not resurrect the batch), but mark or partition
  it so pending-set scans never visit settled fates.

The asymmetric deletion in `settle_sealed_batch` (`inbox.rs:908` deletes the
submission while the gate at `inbox.rs:965` would have kept it) disappears:
there is exactly one retirement routine and both paths call it.

### 3. Membership is persisted until retirement, never reconstructed

The pending set owns a durable `batch_id → members` record from seal time to
retirement. Commit already has the full member list in hand
(`writes.rs:1087-1110`); persisting it there (or persisting the existing
`LocalBatchRecord`, which already carries members + submission + fate — see
[Duplicated batch bookkeeping storage](../issues/duplicated-batch-bookkeeping-storage.md)
for collapsing the redundancy) makes membership recovery a point lookup.

Consequences:

- the fallback chain in `local_batch_rows` (`ticks.rs:27-194`) is demoted from
  the hot path: with submissions retained until settlement, pending-batch
  membership resolves from the submission, and the reload/reconnect paths no
  longer reach the terminal full-scan arm for new data. **Status: the
  full-scan arm itself is retained** for legacy storages and the late-server
  promotion pass; deleting it (and turning "no membership record for a pending
  batch" into an asserted integrity error) is deferred until the §6
  watermark/migration pass lands;
- `pending_batch_ids_needing_reconciliation` (`sync.rs:15-101`) stops
  re-deriving the pending set from whole-DB scans; it reads the maintained set.
  Its third loop (`sync.rs:59-78`, the `VisibleDirect`-rows-without-fate sweep)
  is demoted to an explicit one-shot repair/migration pass (§6), not something
  that runs on every `add_server`;
- `local_batch_records_for_worker_sync` (`writes.rs:818`) becomes "read the
  pending set" — no fate-table sweep, no per-fate membership reconstruction.

### 4. `wait(tier)` resolves or fails fast

Semantics: `wait(tier)` waits for the batch's fate to reach `tier`, and is only
accepted when the runtime has a configured path to `tier`.

- a durable runtime resolves `tier <= own tier` at commit (today's
  self-confirmation, kept);
- a non-durable client resolves `local` on the first fate from its durable
  peer;
- `tier > settlement_target` is rejected immediately with a descriptive error
  (e.g. `wait({ tier: "global" })` on a runtime with no server configured),
  instead of registering a structurally unresolvable waiter;
- the silent no-tier `return` in `settle_sealed_batch` (`inbox.rs:879-881`)
  must eventually become an explicit error response to the origin client — a
  peer that cannot settle should say so rather than drop the seal. **Status:
  deferred** (it requires a new wire message); the implementation ships a
  `tracing::warn!` at the dropping peer, and the origin side is protected by
  the `wait(tier)` fail-fast guard, which refuses waits no configured producer
  can resolve.

A timeout knob on `wait` is still worth having for network flakiness, but it is
not the fix; the fix is that no waiter can be created that nothing can resolve.

### 5. Worker→main replay carries the pending set only

`sync_retained_local_batch_records` (`crates/jazz-wasm/src/worker_host.rs`) and
`hydrate_worker_local_batch_record` (`worker_bridge.rs:604`) keep their current
protocol shape, but the payload is now the worker's pending set — bounded by
genuinely in-flight work. On a healthy local-only reload it is empty; after an
offline session it is proportional to unsynced batches, which is real work
replay must do anyway. When the worker later retires a batch, the existing fate
forwarding (`queue_batch_fate_to_client`, `inbox.rs:927-936`) tells main, and
main retires its hydrated mirror the same way (§2).

Whether main needs full `LocalBatchRecord`s at all (vs. a flat
`batch_id → row_ids + tier` map for query visibility) is a worthwhile follow-up
simplification, but out of scope here; this spec only shrinks the set.

### 6. Tier promotion when an upstream appears later

Lowering the retirement gate creates one new obligation the status quo gets for
free by never retiring anything: a batch retired at `Local` must still be
syncable if a server is configured next week.

Proposal:

- each runtime persists a **promotion watermark**: the settlement target its
  retired batches have reached;
- when configuration raises `settlement_target` above the watermark (first
  server registration on a previously local-only runtime), run a **one-shot
  promotion pass**: scan the row store for `VisibleDirect` rows whose batches
  have terminal fates below the new target, regenerate seals from row history
  (`direct_sealed_submission_from_local_batch_rows`, `writes.rs:866`, already
  does exactly this), and re-enter those batches into the pending set;
- the server side already supports promoting a previously local direct fate
  (`try_accept_completed_sealed_batch_from_client` explicitly continues into
  seal validation for `confirmed_tier < authority tier`,
  `inbox.rs:953-963`), so no protocol change is needed;
- after the pass completes, advance the watermark.

This converts today's permanent whole-DB reconciliation scan into a bounded
migration that runs once per configuration change, which is also the natural
home for the repair sweep that `sync.rs:59-78` currently runs on every
`add_server`.

## Invariants

These should each be enforceable by a black-box integration test through the
public API:

1. **Bounded pending set.** After `commit` + fate at `settlement_target`, the
   batch has no sealed submission, no pending membership record, and does not
   appear in the worker→main replay payload after reload.
2. **Local-only apps settle locally.** With no server configured, N inserts +
   commits followed by a worker restart replay zero retained batch records, and
   reload time does not grow with N.
3. **No full-scan membership recovery (partially delivered — see §3 status).**
   `local_batch_rows` for new retained submissions is a point lookup; turning a
   pending batch without a membership record into a reported integrity error
   instead of a fallback scan is deferred.
4. **`wait` totality.** For every (runtime config, tier) pair, `wait(tier)`
   either can be resolved by a producer that exists in that config, or returns
   an error immediately. `commit().wait({ tier: "local" })` with no server
   resolves on a durable runtime and resolves via the worker in the browser
   setup; it never hangs.
5. **Promotion completeness.** Local-only writes → retire → configure a server →
   promotion pass → server holds the rows and fates reach `GlobalServer`;
   afterwards the pending set is empty again.
6. **Idempotent retirement.** Re-delivering a seal or fate for a retired batch
   is a no-op (tombstone), and never re-creates pending-set entries.

## Migration / repair

Existing storages already contain the accumulated state this spec eliminates:
`Local`-tier fates without submissions, submissions without reachable terminal
fates, and `VisibleDirect` rows referencing both. On first startup with the new
lifecycle, run the §6 promotion/repair pass once against the _current_
settlement target:

- target = `Local` (no server configured): retire everything that has any
  non-rejected fate;
- target = `GlobalServer`: rebuild pending membership records (one final full
  scan, amortized once) and leave genuinely unsynced batches pending.

## Open questions

- Where should `settlement_target` live for the browser pair — decided in the
  worker and mirrored to main over the init wire, or passed to both from TS
  config? (Worker-decided seems right: main's only producer is the worker.)
- Does retiring a batch's fate need a real tombstone row, or is "fate exists at
  terminal tier + no membership record" sufficient to reject resurrection?
- `EdgeServer` in a three-tier deployment: does an edge-confirmed batch retire
  on the client while the edge still owes global reconciliation upstream
  (per-hop targets), or does the client keep it pending until global? Per-hop
  targets keep each runtime's pending set local to its own obligations and
  match how fates already propagate, but this needs a deliberate decision.
- Should the promotion watermark be per-upstream rather than a single tier, to
  handle multi-server configurations?
