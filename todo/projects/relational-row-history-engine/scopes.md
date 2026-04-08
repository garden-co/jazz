# Scopes

```text
┌──────────────────────────────────────────────────────┐
│ Scope 1: Row-Region Engine Replacement              │
│ replace today's object/commit substrate end to end  │
└────────────────────────┬─────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────┐
│ Scope 2: Transactions, Authorities, and Fate        │
│ add staging, acceptance/rejection, and tx semantics │
└────────────────────────┬─────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────┐
│ Scope 3: Public History, As-Of, and Branch Views    │
│ expose the historical query surface                 │
└──────────────────────────────────────────────────────┘

Each scope is a standalone PR.
Scope 2 assumes Scope 1 has already replaced the runtime path.
Scope 3 assumes the storage model and transaction semantics are both stable.
```

## Row-Region Engine Replacement — replace today's engine with row regions

- [ ] Add row-region storage primitives and key layout to `Storage`, with one physical keyed table space per user table, a visible region keyed by `($branch, $row_id)`, and a history region keyed by `($row_id, $version_id)`
- [ ] Reuse the existing row encoding and fast reproject machinery so system columns and user columns share one canonical encoded row format
- [ ] Rename the row-history subsystem to `row_histories` so the code reads like row-local DAG semantics rather than a vague "region" abstraction
- [ ] Extract the generic binary row codec out of `query_manager::encoding` into a neutral `row_format` module used by row histories, storage, query materialization, and catalogue rows
- [ ] Define the reserved system columns for encoded row versions:
      `$row_id`, `$branch`, `$version_id`, `$created_by`, `$updated_by`, `$parents`, `$generation`, optional history-only `$tx_id`, `$state`, `$confirmed_tier`, `$is_deleted`, `$metadata`
- [ ] Derive `$updated_at` from `$version_id` rather than keeping a separate required hot-path timestamp column
- [ ] Preserve one DAG per logical row in the history region, with parent pointers referencing only version ids and generation numbers available for ancestor/MRCA work
- [ ] Introduce a compact visible-entry shape with one `current_version_id`, optional tier winner ids for `worker` / `edge` / `global`, and one `current_data` payload instead of duplicating full visible rows per durability tier
- [ ] Make those tier winner pointers sparse by defaulting `worker -> current`, `edge -> worker`, and `global -> edge`
- [ ] Treat better optional-column encoding as part of Slice 1 so nullable `$tx_id` and sparse tier pointers do not silently add avoidable entropy
- [ ] Keep branch-oriented history scans available through raw-table access paths or indexes without making branch-local ancestry the semantic model
- [ ] Move row-history transition logic into a dedicated reducer module inside `row_histories`, leaving backend-specific persistence concerns in `storage`
- [ ] Replace the remaining production `ObjectManager` row path with storage-backed row-apply / row-patch helpers plus a tiny monotonic clock in `RuntimeCore`
- [ ] Remove the leftover legacy `Commit`, `StoredState`, `CommitAckState`, and object/branch container types that only exist to bridge from the old model
- [ ] Implement the new row-region storage path in `MemoryStorage`
- [ ] Implement the same row-region storage path in `FjallStorage`
- [ ] Get the full `MemoryStorage` and `FjallStorage` test surface green on the new engine before touching the other durable backends
- [ ] Run the first serious benchmark comparisons on the new engine using `MemoryStorage` and `FjallStorage`; treat this as the main de-risking checkpoint for the architecture
- [ ] After those tests and benchmarks validate the direction, adapt the remaining storage backends to the same row-region model
- [ ] Replace direct visible writes so they append encoded row versions to history and upsert encoded current rows to visible
- [ ] Replace direct-write durability bookkeeping so reconnect/restart recovers from persisted row metadata rather than commit-id reconstruction
- [ ] Replace ordinary query execution so current reads and subscriptions target the visible region rather than reconstructing rows from object graphs
- [ ] Replace current sync payloads and replay semantics so user-row replication is row-version and row-metadata based instead of object/commit based
- [ ] Preserve the current external direct-write product semantics while the substrate underneath is replaced
- [ ] Preserve the current supported query shapes for ordinary current-state reads
- [ ] Remove the old object-manager hot path for user rows instead of keeping a hybrid forever architecture
- [ ] Delete production user-row references to `ObjectManager` entirely rather than just renaming the type
- [ ] Add SchemaManager and RuntimeCore integration tests covering current reads, writes, restart, multi-tier sync, and deletion semantics on the new engine
- [ ] Add benchmark comparisons against `main` for point reads, visible scans, direct writes, restart cost, sync payload size, and on-disk size

## Transactions, Authorities, and Fate — add staging and accepted/rejected transaction semantics

- [ ] Introduce cross-row transactions as the write unit across multiple rows while keeping the row-region storage model unchanged
- [ ] Add opt-in transactional writes that append staging row versions into history without touching visible state
- [ ] Introduce authority handling for transaction validation and exactly-one terminal fate per transaction, assuming one global authority identity for now
- [ ] Treat the central server as both the global durability tier owner and the global transaction authority in this scope
- [ ] Reuse the optional `$tx_id` placeholder from Scope 1 as the real shared transaction identity for every row version participating in one transaction
- [ ] Add accepted/rejected fate as row-metadata state transitions rather than as a second bespoke mechanism
- [ ] Patch accepted transactional history rows in place and publish the corresponding visible rows
- [ ] Patch rejected transactional history rows in place while leaving visible state unchanged
- [ ] Add replayable reconnect semantics for pending and settled transactional work
- [ ] Add transaction-aware durability semantics on top of transaction-level confirmed tier
- [ ] Preserve the row-level version DAG semantics so accepted transactional publishes still have precise per-row merge meaning and ancestor structure
- [ ] Expose transaction outcomes through runtime/subscription semantics without yet introducing the public historical query APIs
- [ ] Keep Slice 1 direct-write behavior working cleanly beside the new transactional path
- [ ] Add SchemaManager and RuntimeCore integration tests for accepted/rejected multi-row writes, replay, restart, and durability/fate behavior
- [ ] Add benchmarks for wide-transaction acceptance/rejection fan-out and tier advancement costs

## Public History, As-Of, and Branch Views — expose the historical query surface

- [ ] Add `query.history()` as a public query mode targeting the history region
- [ ] Add `query.as_of(ts)` as a public query mode that reconstructs the latest visible row versions at or before a timestamp
- [ ] Add `query.branch_view(branch)` as a public query mode for non-default branch images
- [ ] Support combinations such as `query.history().branch_view(branch)` and `query.as_of(ts).branch_view(branch)`
- [ ] Expose a curated first public surface for system-column-derived information rather than making every reserved system column directly queryable at once
- [ ] Decide which curated metadata becomes first-class in the public API, and how users opt into selecting/filtering on it
- [ ] Extend query planning and execution so historical modes operate over the same encoded row format as ordinary current reads
- [ ] Ensure accepted/rejected/staged transactional states are represented coherently in historical query results
- [ ] Add query-planning tests for region selection, branch selection, and as-of reconstruction
- [ ] Add SchemaManager and RuntimeCore integration tests for history, as-of, and branch-view queries over realistic row histories
- [ ] Add performance tests for history scans, as-of reconstruction, and branch-view queries on realistic histories
