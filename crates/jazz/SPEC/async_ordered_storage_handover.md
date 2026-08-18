# Async storage consolidation handover

This note records the deliberately pushed, temporarily non-green checkpoint on
`experiment/async-ordered-storage`. The design and ordered implementation plan
remain in [async_ordered_storage.md](async_ordered_storage.md), especially
“Addendum: evolve the owners instead of grafting parallel async facades” and
its numbered steps 1–8.

## Current checkpoint

- `DbCore` has been deleted. `Db` is the sole high-level Jazz database owner,
  owns the demand-driven runtime, and exposes async reads and writes.
- Typed schemas are inert `DbSchemaView` tokens; short-lived `DbView<'_>` values
  borrow the sole owner instead of owning a second runtime.
- The old synchronous transaction handles and traits were removed. Callers use
  caller-owned `OpenBatchId`s with async begin/stage/commit operations.
- Production `cargo check --workspace` passed after the consolidation. The
  all-target/test migration is intentionally incomplete at this checkpoint.
- Dead synchronous NodeState commit/read helpers found during the migration
  were removed rather than retained as compatibility paths.

The local commit sequence at handover is:

1. `bf941dd46` — consolidate database ownership in async `Db`
2. `3cbce048b` — remove superseded synchronous database paths
3. `c6cafa76d` — adapt publication benchmarks to mutable state
4. `f6151c067` — adapt the CLI dry-run client pump

An unintegrated, preserved caller-migration checkpoint exists at `467a89a84`
(`wip: migrate async integration callers`). Inspect or cherry-pick it rather
than repeating those mechanical edits blindly.

## Work in progress when paused

Step 1 of the addendum (“Make `Db` the sole high-level owner”) was in its final
caller/test cleanup. The main remaining work was:

1. Finish compiling all Jazz test and benchmark targets against `&mut Db` and
   the explicit async transaction API. Do not restore synchronous handles or a
   compatibility owner.
2. Clean mechanically introduced `unused_mut` warnings in internal Db tests.
3. Remove or relocate the now-unused prepared-query graph handles. Their fields
   and `plan_for_tier` currently only support test diagnostics, so default
   clippy reports them as dead. Prefer an async-owner-native plan cache over
   retaining dead handles in the public prepared query value.
4. Investigate the behavior failure in
   `maintained_authorization_restores_an_ordered_page_after_scope_reentry`:
   `update_for_identity` returned `Ok` where the existing assertion requires a
   write-only principal to be rejected. Treat this as a possible authorization
   regression; do not weaken the test to finish migration.
5. Run the immediate-visibility proof matrix from the main design: resident
   direct writes must synchronously publish callbacks and subsequent one-shot
   reads, while unloaded joins/includes may suspend as explicitly allowed.
6. Run full workspace checks/tests, clippy, format, diff checks, and an
   adversarial review before replacing this checkpoint with a green commit.

After step 1 is green, continue the numbered plan in the main document: make
`Node` the sole node owner, finish schema-view consolidation, make Groove
`Database` its sole owner, unify constructors/opening, share the operation
driver, and only then revisit publication receipts.

## Guardrails

- There is one core, not synchronous and asynchronous modes. A backend that
  completes immediately naturally gives its callers immediate completion.
- Memory is immediate but never claims `Local` durability; RocksDB may be both
  immediate and locally durable.
- Simulated latency and IndexedDB must exercise the same async storage
  abstraction as production.
- Local visibility is guaranteed for the rows directly written when their
  required inputs are resident. A cold referenced row needed only by a join or
  include may complete asynchronously.
- Do not reintroduce `DbCore`, storage-generic high-level `Db` owners,
  synchronous transaction handle traits, or test-only alternate database
  modes to make callers compile.
