# Attempt 3 Decision Log

## 2026-05-25 16:43 PDT

Attempt 3 starts from `SPEC.md` as the working contract.

Setup decisions:

- All stores, including memory-only nodes, use SQLite. In-memory means
  in-memory SQLite, not a fake store.
- The harness should test mixed topologies: in-memory local/main-thread-like
  nodes, durable worker/tab-broker-like nodes, optional edge nodes, and a durable
  global authority.
- Policies and lenses are in scope for this attempt, even if the first versions
  are narrow.
- Attempt 2 code and old specs were archived under `reference/attempt2/`.
- First implementation focus: whole-system invariant tests and a boring
  SQLite-backed runtime boundary.

## 2026-05-25 16:45 PDT

Implementation structure guardrail: split the crate into modules from the first
real code pass. Avoid recreating Attempt 2's large `store.rs` center of gravity.

Initial module plan:

- `types`: public domain structs and ids
- `storage`: SQLite connection/opening/migrations
- `tx`: transaction records, outcomes, receipts
- `schema`: fixed Attempt 3 project/todo schema and DDL helpers
- `sync`: query-scoped bundles and apply/export
- `runtime`: product-shaped facade used by whole-system tests

## 2026-05-25 16:49 PDT

First Attempt 3 implementation slice is green.

Added product-shaped whole-system tests proving:

- in-memory SQLite runtime writes through history/current projection
- query-scoped bundle sync converges an in-memory node and durable SQLite node
- rejected transactions remain in history but disappear from current reads
- durable SQLite nodes survive reopen while fresh in-memory SQLite nodes start
  empty

Implementation is deliberately fixed to the `projects`/`todos` harness schema
for now and split into modules:

- `error`
- `runtime`
- `schema`
- `storage`
- `sync`
- `tx`
- `types`

Learning: starting with SQLite from the first test is not slowing us down. The
boundary is already cleaner than Attempt 2 because storage opening, DDL, tx
facts, sync payloads, and runtime facade are separate from the beginning.

## 2026-05-25 16:49 PDT

Next target: complete more of the transaction/history/sync spine before moving
to policy/branches.

Planned tests:

- explicit transaction seals multiple mutations
- rebuilding current projection from history matches current reads
- delete appends history and hides current row
- applying the same bundle twice is idempotent
- replicas can use different physical ids for the same public ids

## 2026-05-25 16:51 PDT

Second spine slice is green: 9 whole-system tests pass.

Added/proved:

- explicit transaction builder seals project + multiple todos under one tx id
- projection rebuild from history restores current reads
- delete appends a history row and removes current visibility
- applying the same bundle twice is idempotent
- two replicas can use different physical row ids for the same public row ids

Important early catch: public tx ids cannot be based on local physical
`node_num`, because two replicas may both assign the writer physical node `1`.
Attempt 3 tx ids now include the public node id plus local epoch.

## 2026-05-25 16:52 PDT

Next target: start authority/scope semantics.

Planned tests:

- query-scoped sync is not table replication
- authority acceptance enriches an existing public transaction id with global
  epoch/receipt instead of replacing it

## 2026-05-25 16:53 PDT

Authority/scope slice is green: 11 whole-system tests pass.

Added/proved:

- open-todo query scope exports only referenced projects, not unrelated table
  rows
- global authority acceptance mutates/enriches the existing tx row with
  `global_epoch`
- global receipt state is separate from transaction identity and observable
  through transaction info

Learning: query-scoped export already forced us to stop treating "all history"
as the easy sync payload. The test is tiny, but it protects the core product
claim that Jazz syncs query scope rather than tables.

## 2026-05-25 16:54 PDT

Structural cleanup trigger: `runtime.rs` reached ~750 lines after only 11 tests.
Before adding policy/branches/lenses, split helper-heavy behavior out so the
runtime facade stays product-shaped.

Target split:

- `projection`: projection clearing/rebuild and rejection repair helpers
- `bundle`: query-scoped export/apply helpers
- `rows`: row-id and fixed-schema insert helpers

## 2026-05-25 16:56 PDT

Course correction before adding features: the runtime must not be hardcoded to
the todo fixture. The fixture should be a schema used by tests, not knowledge
baked into the runtime.

Next structural goal:

- introduce generic table/schema definitions
- generate history/current tables from schema
- route writes and projection rebuild through table definitions
- keep product-shaped `projects`/`todos` helpers only as test-fixture
  convenience over generic runtime operations

This is worth doing before policy/branches/lenses so those features attach to
the real architecture instead of to hardcoded tables.
