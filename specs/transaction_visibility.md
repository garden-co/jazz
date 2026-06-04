# Transaction Visibility

## Intro

End state: users only think in terms of **transactions**.

```ts
db.transaction(async (tx) => {
  tx.insert(todos, { title: "Ship it" });
});
```

By default, a transaction becomes visible when the highest relevant authority accepts it:

```ts
// With server authority: default visibleAt is "global"
db.transaction(async (tx) => {}, { visibleAt: "global" });

// With no remote authority but local durable authority: default visibleAt is "local"
db.transaction(async (tx) => {}, { visibleAt: "local" });

// With no remote authority and no local durable authority (i.e. in-memory DB): default visibleAt is "immediate"
db.transaction(async (tx) => {}, { visibleAt: "immediate" });
```

Explicit optimistic visibility is allowed:

```ts
// visible after the runtime accepts the write in memory
db.transaction(async (tx) => {}, { visibleAt: "immediate" });
// visible after the write is durably persisted/accepted by local authority
db.transaction(async (tx) => {}, { visibleAt: "local" });
// visible after edge authority acceptance
db.transaction(async (tx) => {}, { visibleAt: "edge" });
```

Manual transactions use the same option:

```ts
const tx = db.beginTransaction({ visibleAt: "global" });
tx.insert(todos, { title: "Carefully" });
await tx.commit();
```

Existing direct writes/direct batches become implementation details: internally they are transactions visible at `immediate` acceptance. Public `batch` wording should be deprecated or aliased, but not kept as a separate semantic concept.

Core invariant:

```text
A transaction's rows are ordinary-read visible only when:
visible_at == immediate
or confirmed_tier >= visible_at
```

Offline transactions stay staged/pending until the required authority fate arrives.

## Implementation Plan

1. Add a transaction visibility type.

   Introduce one Rust enum, probably near `batch_fate`:

   ```rust
   pub enum TransactionVisibility {
       Immediate,
       Local,
       EdgeServer,
       GlobalServer,
   }
   ```

   `Local`, `EdgeServer`, and `GlobalServer` correspond to durability/authority tiers (see `DurabilityTier`). `Immediate` is not a durability tier; it means publish after in-memory runtime acceptance. In TypeScript, expose:

   ```ts
   export type TransactionVisibleAt = "immediate" | "local" | "edge" | "global";
   ```

2. Extend transaction metadata.

   Add `visible_at` to:

   ```rust
   SealedBatchSubmission {
       visible_at: TransactionVisibility,
       ...
   }
   ```

   and:

   ```rust
   BatchFate::AcceptedTransaction {
       confirmed_tier: DurabilityTier,
       visible_at: TransactionVisibility,
   }
   ```

   Keep `DurableDirect` unchanged, as it is used for durable waits (and it doesn't make sense to wait for an "immediate" write).

3. Bind `visible_at` into the sealed manifest.

   Update `SealedBatchSubmission::compute_batch_digest` so `visible_at` participates in the digest. Two submissions with the same rows but different visibility policy should not verify as the same sealed transaction.

   Also validate that if a node sees both a submission and an accepted fate, their `visible_at` values agree. Mismatch should reject or ignore the stale/inconsistent fate.

4. Bump storage formats.

   Update codecs/storage descriptors for:
   - sealed batch submission storage
   - authoritative batch fate storage
   - local batch record, indirectly, if its encoded form embeds or reconstructs sealed submission/fate

   Old records can decode with a default:

   ```text
   old AcceptedTransaction without visible_at => visible_at = confirmed_tier
   ```

   or, if we prefer new semantics only for new stores:

   ```text
   old transactional records => visible_at = immediate
   ```

   Prefer the least surprising migration for existing data: preserve current behavior for old records, then apply new defaults only to newly created transactions.

5. Update sync protocol serialization.

   `SyncPayload::SealBatch` will carry the new submission field automatically if `SealedBatchSubmission` changes.

   `SyncPayload::BatchFate` must carry `visible_at` for `AcceptedTransaction`, because many peers receive fate-only messages and never get the sealed submission.

   Update Rust serde, NAPI/WASM bindings, React Native adapter, and TypeScript `BatchFate` type.

6. Change row publication logic.

   Today an accepted transaction immediately publishes rows. Change that reducer to:

   ```rust
   match AcceptedTransaction { confirmed_tier, visible_at, .. } {
       if visible_at == Immediate => publish VisibleTransactional
       else if confirmed_tier >= visible_at => publish VisibleTransactional
       else => keep StagingPending, but persist fate
   }
   ```

   This affects the `apply_transactional_batch_fate_to_rows` path.

   Lower-tier acceptance should still wake durability waiters for that lower tier, but should not make rows visible unless it satisfies `visible_at`. `immediate` visibility is the special case: rows publish before any durable authority acknowledgement, matching current direct-batch behavior.

7. Retain sealed submissions until terminal enough.

   Do not delete sealed submissions after any non-missing accepted fate. Delete only when:

   ```text
   Rejected
   or confirmed_tier >= required final reconciliation tier
   ```

   For a `visibleAt: "global"` transaction accepted at edge, keep enough sealed submission data to continue global reconciliation after reconnect/restart.

8. Update default resolution.

   Add a single resolver for transaction visibility defaults:

   ```text
   if explicit visibleAt: use it
   else if remote/global authority configured: global
   else if local durable authority exists: local
   else: immediate
   ```

   Use that resolver in both:

   ```ts
   db.transaction(...)
   db.beginTransaction(...)
   ```

   Ordinary direct writes should map to `visibleAt: "immediate"` internally so current batch/direct-write ergonomics remain intact.

9. Public API changes.

   Extend transaction methods with an optional options object at the end:

   ```ts
   db.transaction(callback)
   db.transaction(callback, options?)

   db.beginTransaction(options?)
   ```

   Proposed final shape:

   ```ts
   interface TransactionOptions {
     visibleAt?: "immediate" | "local" | "edge" | "global";
   }
   ```

   Keep `commit()` simple initially:

   ```ts
   await tx.commit();
   ```

   Do not start with `tx.commit({ visibleAt })`; choosing at begin time is easier to persist, reason about, and document.

10. Tests.

    Add red/green tests around the real semantics:
    - `visibleAt: immediate`: row becomes visible after in-memory write acceptance, before local durability
    - `visibleAt: local`: row stays invisible until local durable acceptance
    - `visibleAt: global`, accepted at edge: row stays invisible
    - later accepted at global: row becomes visible
    - `visibleAt: edge`, accepted at edge: row becomes visible
    - offline with `visibleAt: global`: row stays invisible/pending
    - fate arrives before rows: fate's `visible_at` controls later row materialization
    - `BatchFateNeeded` response includes `visible_at`
    - restart/reconnect preserves `visible_at`
    - sealed submission retained after edge acceptance when global visibility/reconciliation is still needed
    - mismatch between submission `visible_at` and fate `visible_at` is handled deterministically

The key implementation principle: `visible_at` is transaction metadata, not row state. Rows remain staged or visible; transaction/fate metadata decides when staged rows are allowed to become visible.

## Ordered Implementation Tasks

1. [x] Add failing storage/codec tests for the new transaction visibility metadata:
   - `SealedBatchSubmission` round-trips `visible_at`
   - `BatchFate::AcceptedTransaction` round-trips `visible_at`
   - old accepted-transaction records decode with the chosen compatibility default
   - old sealed submissions decode with the chosen compatibility default

2. [x] Add the Rust `TransactionVisibility` type near transaction fate/submission code, including parsing, storage encoding helpers, serde support, and comparison helpers against `DurabilityTier`.

3. [x] Extend `SealedBatchSubmission` with `visible_at`, including storage encoding/decoding, conformance tests, and old-record defaults.

4. [x] Bind `visible_at` into `SealedBatchSubmission::compute_batch_digest`.

5. [x] Extend `BatchFate::AcceptedTransaction` with `visible_at`, including merge behavior, storage encoding/decoding, conformance tests, and old-record defaults.

6. [ ] Add validation for mismatches between a sealed submission's `visible_at` and the matching accepted fate's `visible_at`.

7. [x] Update sync protocol serialization/deserialization so:
   - `SyncPayload::SealBatch` carries `visible_at` through the sealed submission
   - `SyncPayload::BatchFate` carries `visible_at` for `AcceptedTransaction`
   - `BatchFateNeeded` responses include `visible_at`
   - fate-only delivery is sufficient for peers that never received the sealed submission

8. [x] Update WASM/NAPI bindings, React Native adapter types, and TypeScript `BatchFate` serialization for the new `visible_at` / `visibleAt` field.

9. [x] Update sealed-submission retention so lower-tier acceptance does not delete the submission while later global reconciliation is still required.

10. [x] Change the row publication reducer so staged transaction rows become ordinary-read visible only when:

```text
visible_at == immediate
or confirmed_tier >= visible_at
```

11. [x] Keep lower-tier accepted fates durable and replayable without publishing rows when they do not satisfy `visible_at`.

12. [x] Add sync-ordering tests where fate arrives before row data and where row data arrives before fate.

13. [ ] Add restart/reconnect tests proving `visible_at` survives persistence and still controls row publication after replay.

14. [ ] Add failing end-to-end row-visibility tests for:
    - `visibleAt: "immediate"` publishes after in-memory runtime acceptance
    - `visibleAt: "local"` waits for local durable acceptance
    - `visibleAt: "edge"` waits for edge acceptance
    - `visibleAt: "global"` waits for global acceptance
    - edge acceptance of a global-visible transaction does not publish rows
    - offline global-visible transactions remain staged

15. [x] Add one default resolver for transaction visibility:
    - explicit `visibleAt` wins
    - remote/global authority defaults to `global`
    - durable local authority without remote authority defaults to `local`
    - no durable authority defaults to `immediate`

16. [ ] Map existing direct writes/direct batches internally to transaction writes with `visibleAt: "immediate"`.

17. [x] Add the TypeScript `TransactionVisibleAt` type and public `TransactionOptions` type.

18. [x] Thread `visibleAt` through the public transaction APIs:
    - `db.transaction(callback, options?)`
    - `db.beginTransaction(options?)`
    - runtime/client transaction creation
    - internal batch/write context

19. [x] Add user-facing API tests for explicit `visibleAt` and default resolution:
    - no remote authority but durable local authority defaults to local visibility
    - no remote authority and no durable local authority defaults to immediate visibility

20. [ ] Update status-quo docs and this spec with any final names chosen during implementation.

## Compatibility/Deprecation

### First Compatibility Window

Rename gradually, not all at once.

Internally we can keep `BatchId`, `BatchFate`, and `SealedBatchSubmission` while landing behavior. Then do terminology cleanup separately:

```text
user-facing BatchId -> TransactionId later
BatchFate -> TransactionFate later
SealedBatchSubmission -> SealedTransactionSubmission later
```

This avoids mixing semantic changes with a giant rename.

Keep old public batch/direct APIs as aliases for one release window or until launch cleanup:

```ts
db.batch(cb);
```

Make this equivalent to `visibleAt: "immediate"`, but move docs and examples to `transaction`.

### Proper Cleanup Later

Once the visibility semantics are in place, do a deliberate terminology cleanup instead of leaving
"batch" as the shared word for unrelated concepts. The target vocabulary should be:

- `transaction`: user-facing write group whose rows become visible according to `visibleAt`
- `transaction fate` or `transaction settlement`: replayable acceptance/rejection state for a transaction

Do not blindly rename every occurrence of `batch`. Keep the word only where it means an operational
bulk grouping for performance. Remove it from transaction lifecycle names.

Public TypeScript/API cleanup:

- Remove `db.batch(...)` and `db.beginBatch()` after the compatibility window.
- Remove `JazzClient.batch(...)`, `JazzClient.beginBatch()`, and `beginBatchInternal()` from the public/runtime surface.
- Replace `DirectBatch`, `BatchScope`, `DbDirectBatch`, `DbBatchScope`, and imported `RuntimeDirectBatch` with transaction handles using `{ visibleAt: "immediate" }`.
- Rename user-facing `batchId` on write handles/results to `transactionId` for transaction APIs.
- Rename `waitForBatch(batchId, tier)` to a transaction-oriented wait, probably `waitForTransaction(transactionId, tier)`.
- Rename `batchFate(...)`, `loadBatchFate(...)`, `sealBatch(...)`, `discardLocalBatch(...)`, and `acknowledgeRejectedBatch(...)` to transaction names or make them internal.
- Rename `BatchMode = "direct" | "transactional"` away. Ideally delete it; otherwise replace it with a visibility/transaction mode that distinguishes `immediate` from authority-validated transactions.
- Rename mutation error payloads from `batch` / `LocalBatchRecord` to transaction terminology.

Rust transaction-lifecycle cleanup:

- `batch_fate.rs` should become a transaction settlement/fate module.
- `BatchFate` -> `TransactionFate` or `TransactionSettlement`.
- `BatchFate::AcceptedTransaction` -> `Accepted` or `AcceptedTransaction` with `confirmed_tier` and `visible_at`.
- `BatchFate::Rejected` / `Missing` can keep the variant names under the new enum.
- `BatchFate::DurableDirect` should be removed if direct batches disappear. If durable waits still need a non-transaction ack, split that into a separate durability acknowledgement instead of keeping it as a transaction fate.
- `LocalBatchRecord` -> `LocalTransactionRecord`.
- `LocalBatchMember` -> `LocalTransactionMember`.
- `SealedBatchSubmission` -> `SealedTransactionSubmission`.
- `SealedBatchMember` -> `SealedTransactionMember`.
- `CapturedFrontierMember` can stay conceptually, but should live under transaction submission naming.
- Runtime/core methods such as `local_batch_record`, `local_batch_records_for_worker_sync`, `hydrate_local_batch_record`, `reconcile_local_batch_with_server`, `upsert_local_batch_record`, and `load_authoritative_batch_fate` should become transaction names.

Sync protocol cleanup:

- `SyncPayload::SealBatch` -> `SealTransaction`.
- `SyncPayload::BatchFate` -> `TransactionFate` or `TransactionSettlement`.
- `SyncPayload::BatchFateNeeded` -> `TransactionFateNeeded` or `TransactionStatusNeeded`.
- `batch_fate_interest`, `pending_batch_fates`, and `pending_client_batch_fates` should become transaction fate/status names.

Out of scope for this cleanup:

- Do not rename row-history `BatchId`, `StoredRowBatch`, `QueryRowBatch`, `RowBatchCreated`, `RowBatchNeeded`, or physical `_jazz_*batch*` columns as part of this transaction visibility cleanup.

System storage cleanup:

- `__local_batch_record` -> `__local_transaction_record`.
- `__sealed_batch_submission` -> `__sealed_transaction_submission`.
- `__authoritative_batch_settlement` -> `__transaction_settlement` or `__authoritative_transaction_fate`.
- `__acknowledged_rejected_batch` -> `__acknowledged_rejected_transaction`.
- Storage keys currently shaped as `batch:<id>` should become `transaction:<id>` for transaction lifecycle tables.
- Update storage kind constants and descriptors together with table names; this is a format migration, not just a symbol rename.

Binding/docs/test cleanup:

- WASM/NAPI/React Native bindings should expose transaction names and keep batch aliases only during the compatibility window.
- Generated/browser-facing JSON should use `transactionId` for transaction APIs.
- `specs/status-quo/batches.md` should be replaced or split so transaction lifecycle docs no longer share terminology with unrelated batching concepts.
- Update `specs/status-quo/sync_manager.md`, `specs/status-quo/row_histories.md`, `specs/status-quo/storage.md`, and query/subscription docs so they no longer use `batch` for three different concepts.
- Rename transaction visibility/fate tests under transaction naming.
