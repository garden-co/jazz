# Attempt 1: SQLite Core Speedrun

Started: 2026-05-24 22:10 PDT.

Goal: implement as much of the parity ladder as possible from the current spec,
prioritizing learning over polish. When the spec is fuzzy, make a local
decision, write it down here, and keep moving.

## Ground Rules

- Keep commits at meaningful checkpoints.
- Prefer executable semantics over microbenchmarks.
- Use the deterministic harness as the integration spine.
- Do not preserve compatibility with old Jazz internals.

## Decisions and Discoveries

### 2026-05-24 22:12 PDT

Starting with a minimal native Rust SQLite storage slice:

- one hard-coded `todos` table shape
- `jazz_node`, `jazz_tx`, `todos__schema_v1_history`,
  `todos__schema_v1_current`
- insert, current query, subscription-ish rerun/diff later
- inline JSONB columns represented as text JSON-ish placeholders at first

Decision: use `rusqlite` directly in the prototype. The spec wants a minimal
replaceable interface eventually, but a direct implementation should expose the
right seams faster.

Open fuzziness:

- SQLite JSONB construction from Rust values is not important for the first
  semantics slice; stable text payloads in BLOB/TEXT-shaped columns are enough
  to exercise the row flow.
- Conflict metadata starts as a single tx id array represented as a string.

### 2026-05-24 22:15 PDT

First concrete SQLite learning: bare `$systemColumn` identifiers fail because
SQLite treats `$name` as parameter syntax in many contexts. For implementation I
encoded system columns as snake_case (`tx_id`, `created_at`, etc.) while keeping
`$` names in the spec as semantic notation. Later lowering needs a single
identifier codec instead of hand-written names.

### 2026-05-24 22:18 PDT

Reached first vertical slice:

- deterministic harness skeleton
- pure model types for transaction status/vector visibility/read-write sets
- SQLite schema bootstrap
- insert/update/delete on hard-coded `todos`
- current query over user and system columns
- result-scope locators
- local subscription rerun+diff for added/updated/removed

Discovery: the hard-coded table is a good forcing function. Generic schema
lowering would be premature; the current useful pressure is on transaction
metadata, read/write sets, and deterministic projection rebuilding.

Subscription decision: callback-free polling API for tests. This avoids async
runtime choices and still exercises the semantic loop:

```text
write -> rerun SQL -> full-row diff -> update stored result
```

### 2026-05-24 22:20 PDT

Added a persistence/reopen check and byte-for-byte current projection rebuild.

Discovery: update and delete history rows must carry immutable creation
metadata (`created_at`, and eventually `created_by`) as well as updated
metadata. If history rows only store the new values, rebuilding the main current
projection from history can drift even when ordinary current reads look correct.

For now the prototype preserves `created_at` across update/delete rows. It still
uses the write actor as `created_by` on update/delete history rows because the
read model does not expose `created_by` yet; that should be corrected before
this becomes a real projection invariant.

### 2026-05-24 22:32 PDT

Added model-level acceptance mapping:

- local/edge durable transaction keeps its `tx_id`
- authority maps `(tx_id, node, local_epoch)` to `global_epoch`
- accepted transaction remains addressable by both old local coordinates and
  new global epoch

This supports the "txids first, epoch indexes later if useful" direction while
still making the local-to-global upgrade explicit.

Added first storage-level snapshot read:

- `query_todos_at_local_epoch(query, node_id, local_epoch)`
- implemented as a pure history query, not a projection table
- chooses the latest non-rejected version per row at or below the requested
  local epoch
- delete rows suppress the row at that snapshot

Fuzziness: this is intentionally only a same-node local snapshot. It does not
yet evaluate a full dotted version vector across global base, local bases, and
explicit tx includes. The SQL shape is useful, though: "latest visible version
per row" can be expressed with history joins and `NOT EXISTS`, and the next
step is to replace the same-node predicate with a visibility relation.

### 2026-05-24 22:25 PDT

Added storage-level global acceptance:

- `accept_tx(tx_id, global_epoch)` updates `jazz_tx`
- accepted transactions become `global_durable_accepted`
- global snapshot reads only include accepted transactions at or below the
  requested global epoch

Discovery: the current projection and historical snapshot queries now
deliberately answer different questions. `todos__schema_v1_current` shows the
local optimistic main-branch state, including local pending writes. A global
snapshot query is authority-shaped and only sees globally accepted writes. This
seems right, but the API needs to make the read mode explicit so callers do not
confuse "current local" with "globally durable at epoch N".

Open fuzziness:

- `accept_tx` currently mutates the transaction row directly. The real system
  may want an append-only authority receipt table, with `jazz_tx` holding the
  denormalized current acceptance state.

Added storage-level rejection:

- `reject_tx(tx_id, reason_json)` marks pending/edge transactions rejected
- rejected history remains stored
- local and global snapshot reads filter rejected transactions out

Discovery: this validates the "no vector excludes for rejected txs" instinct.
Rejected txs can remain in history and simply fail the visibility predicate.
The hard remaining problem is not historical visibility; it is repairing the
optimistic current projection after a local write is later rejected.

### 2026-05-24 23:05 PDT

Recommended next five rungs after the CRUD/query/subscription/snapshot basics,
ordered for learning value:

1. Full snapshot vector visibility over history.
   Replace the same-node local snapshot predicate with the spec's closed vector
   semantics: `globalBase`, sorted `localBases`, explicit `include` dots, and
   rejected-transaction filtering. This should be the next pressure test because
   branches, sync, reconnect, and read sets all depend on exactly the same
   visibility relation.

2. Two-node authority acceptance and fate propagation in the deterministic
   harness.
   Create local transactions on an "alice" node, forward them to an authority,
   assign global epochs, broadcast the mapping, and assert that `$txId` remains
   stable while compact coordinates become available. Include a rejected
   transaction case so current projections and snapshots learn to remove or
   ignore rejected local effects.

3. Branch metadata and branch-local reads/writes.
   Add `jazz_branch`/`jazz_branch_history`, create a branch from a closed vector,
   write rows on that branch, and prove those globally accepted rows stay hidden
   from `main` until a metadata-only merge updates the target branch head. This
   is the smallest branch slice that tests the core "global history is not main
   visibility" rule.

4. Two-table joins/includes with result dependency scope.
   Add a second hard-coded table, likely `projects`, and lower a realistic
   todos-with-project query. Capture both result locators and dependency
   locators, then make subscription diffs and sync-scope output prove that a
   reproduced result has every row version it needs.

5. Multi-write transactions plus per-column merge candidates.
   Let one sealed transaction touch multiple rows/tables, record row+column
   write sets, and run a small concurrent update scenario where `title` and
   `done` can merge independently. Store resolved current values with ordered
   conflict candidate tx ids so byte-for-byte projection rebuilds start covering
   the merge contract instead of only last-write projection.

Uncertainties to settle while implementing:

- Whether decoded snapshot vectors should be represented as temp tables first
  or compiled directly into generated predicates. Temp tables look slower but
  will make the visibility contract easier to test and reuse.
- What canonical ordering to use for mixed include dots before global mappings
  exist. `$txId`-only includes may be the most stable prototype choice even if
  not the final compact form.
- How rejected local transactions should be undone in `main` current without
  overfitting to the one-row CRUD path. A projection rebuild after rejection is
  semantically clean; incremental repair can follow.
- Whether branch merge metadata should be represented before app-level branch
  rows. For this prototype, system metadata first is enough to test visibility,
  but it leaves the permission-anchor story unexercised.
- How much of read/write-set JSON needs to be durable in this pass. The useful
  minimum is exact previous visible row version for writes plus column masks;
  range reads can wait until exclusive validation is being modeled.

### 2026-05-24 22:36 PDT

Added a first storage `SnapshotVector`:

- `global_base`
- `local_bases`
- `include_tx_ids`

and `query_todos_at_snapshot(query, snapshot)`, which first resolves visible
transaction ids, then queries history rows for the latest visible row versions.

Discovery: txid includes are a very simple first representation. They avoid
local-to-global coordinate rewrite inside serialized vectors and can be made to
work before compact epoch encodings exist.

Bad smell: the first implementation loops once per visible tx and uses dynamic
`IN (?, ?, ...)` predicates. This is acceptable as an executable spec, but the
real implementation should likely materialize the resolved visible tx set into a
temporary table or use a generated CTE so SQLite can plan the whole snapshot
query at once.

Open fuzziness: ordering concurrent visible versions of the same row is not
solved. The prototype only suppresses older versions from the same node. If two
visible transactions from different nodes both write the same row, this should
become conflict-candidate output rather than "pick one latest row".

### 2026-05-24 22:45 PDT

Added the smallest branch metadata slice:

- `jazz_branch`
- `jazz_branch_history`
- `create_branch`
- `insert_todo_in_branch`

Validated that a branch-local data transaction can be globally accepted while
remaining isolated from `main`. This matches the important rule: global history
is not the same as visibility on every branch.

Major semantic gap: branch reads do not yet inherit rows from their base
branches. A branch created at `main@globalBase=1` should see main rows at that
base plus branch-local rows. The current spike only proves branch id isolation
for branch-local writes. To model real branch reads, a query probably needs a
branch visibility source like:

```text
(source_branch_id, snapshot_vector)
```

for each base/provenance component, plus the branch's own head vector.

### 2026-05-24 22:50 PDT

Made rejected local inserts repair `main` current by rebuilding the current
projection from non-rejected history after `reject_tx`.

Discovery: full projection rebuild is the cleanest first implementation for
rejection repair. It is obviously too broad for a hot path, but it keeps the
semantic invariant simple:

```text
main current = fold(non-rejected main history rows in deterministic order)
```

That invariant is more valuable right now than incremental cleverness. Later,
rejection repair can be narrowed to affected rows using write sets.

### 2026-05-24 22:56 PDT

Closed the first branch-read gap crudely with `query_todos_on_branch`:

- reads base rows from `main` at the branch's stored `head_global_epoch`
- reads branch-local rows from the branch at the requested global epoch
- lets branch-local rows shadow same-row base rows

Discovery: this shape makes branch provenance matter immediately. A branch head
is not just a single vector; it is a set of source branch/vector components plus
local branch history. The current implementation hard-codes "one main base plus
branch-local rows" in Rust. The real lowering probably needs a SQL-visible
branch source relation before joins, pagination, and sync scopes can be correct.

### 2026-05-24 23:04 PDT

Added first sync bundle shape:

- `export_tx(tx_id)`
- `import_tx(bundle)`
- bundle includes one `jazz_tx` row with stable `node_id/local_epoch`
- bundle includes associated `todos` history rows

Validated a client-to-authority path:

```text
alice local write -> export bundle -> authority import -> authority accept -> global snapshot read
```

Discovery: storing `node_num` as a local surrogate is fine as long as every sync
boundary exports stable `node_id` and rehydrates the local surrogate on import.
This mirrors the likely production shape: compact local integer keys inside one
SQLite database, stable string identities on the wire.

Open fuzziness: authority acceptance is still a direct mutation of the imported
`jazz_tx` row. If acceptance receipts become append-only, import/export needs to
carry both proposed local transaction state and authority fate events.

### 2026-05-24 23:10 PDT

Added authority fate propagation back to a client:

```text
client exports local tx -> authority imports/accepts -> authority exports same tx
-> client imports fate -> client global snapshot sees accepted tx
```

Discovery: `import_tx` cannot be insert-only. It has to upsert transaction fate
fields (`status`, `global_epoch`, rejection reason, etc.) while keeping the
stable local identity. This is where "local versions turn into global ones"
starts to become concrete: the row version's public `$txid` stays fixed, but
its coordinate metadata is enriched when the authority response arrives.

### 2026-05-24 23:15 PDT

Added remote rejection propagation:

```text
client optimistic write -> authority imports/rejects -> authority exports same tx
-> client imports fate -> client repairs main current
```

Discovery: import needs side effects, not just row insertion. If an incoming
bundle marks an existing transaction rejected, the recipient has to repair any
derived projections that may have included that optimistic transaction. The
prototype uses full `main` rebuild again; write-set-driven repair remains the
obvious optimization path.

### 2026-05-24 23:22 PDT

Added a first multi-write transaction:

- one `jazz_tx`
- two `todos` history rows
- one exported/imported bundle
- one authority acceptance

Discovery: the schema shape is genuinely transaction-shaped enough for this.
Export/import naturally carries multiple history rows under one transaction.
This also reinforces that write sets should be transaction-level metadata, not
row metadata; row history can stay simple as long as the transaction row
contains the durable read/write contract.
