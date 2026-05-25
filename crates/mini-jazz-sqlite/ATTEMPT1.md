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
