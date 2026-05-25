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
