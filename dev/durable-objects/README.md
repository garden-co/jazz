# jazz-db on Durable Object SQLite

A de-risk spike: can jazz-db run on Cloudflare Durable Object SQLite
(`ctx.storage.sql`)? Measured in a
local workerd (`workerd 2026-07-30`, via Miniflare 4), which enforces the
same authorizer and limits as production.

**Verdict: yes, with no design change to the data model.** Every statement
jazz-db issues across the whole workspace test suite (2.4M statements,
17k distinct texts, 1,624 connections) prepares on Durable Object SQLite
with the fixes now in jazz-db (the `durable-object-limits` feature), except
the transaction statements, which need a scoped transaction API (below). The work left is a storage
seam, not a redesign.

## How it was checked

1. `probe.js`: what DO SQLite accepts (transactions, PRAGMAs, schema
   features, functions, limits, value types, exec cost).
2. `JAZZ_SQL_TRACE=<file>` with jazz's `sql-trace` feature logs every
   top-level statement jazz-db runs (connection, text, bound-parameter
   count, largest value, integers beyond 2^53).
3. `replay-prep.py` + `replay.js` replay each connection's trace in its own
   Durable Object: DDL runs, every other distinct statement is prepared with
   `EXPLAIN`. This catches the authorizer (functions, PRAGMAs, TEMP) and
   every prepare-time limit.
4. jazz's `durable-object-limits` feature applies DO's limits to native
   SQLite (`sqlite3_limit`), so the whole Rust suite runs as if on DO,
   including runtime limits (value size) that a prepare cannot show.

```sh
# 1. probe (needs miniflare from npm)
node run-probe.mjs
# 2. trace
JAZZ_SQL_TRACE=/tmp/trace.jsonl cargo test --workspace --features jazz/sql-trace
# 3. replay
python3 replay-prep.py /tmp/trace.jsonl /tmp/jobs.json
node run-replay.mjs /tmp/jobs.json /tmp/failures.json
# 4. the suite under DO limits
cargo test --workspace --features jazz/durable-object-limits
```

Before the fixes: 225 distinct statements failed on DO (all over the
bound-parameter cap), schema DDL over 100 KB failed to open
(permissioned-resources), and 10 tests failed under the limits. After: none
(the replay's remaining "no such table" errors are reopened file stores
whose DDL ran on an earlier connection; the two suite failures,
`ordered_page_preserves_id_ties_and_refills_after_deletions` and
`predicate_json_and_postcard_encodings_are_pinned`, fail on the base branch
too).

## What DO SQLite gives and refuses

| Area                                                                                                                                  | DO SQLite                                                                                                                                                                                          | jazz-db today                                                                                                                                                         |
| ------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Transactions                                                                                                                          | `BEGIN`/`SAVEPOINT`/`RELEASE`/`ROLLBACK` refused. `transactionSync(fn)` nests (inner throw rolls back only the inner scope). Async `transaction(fn)` holds across `await` and rolls back on throw. | 11 savepoint sites, 39% of all statements run. All lexical except `SAVEPOINT tx`, held across the async Jazz transaction callback.                                    |
| PRAGMAs                                                                                                                               | `journal_mode`, `synchronous`, `cache_size`, `user_version`, `temp_store`, `page_count` refused. `foreign_keys`, `recursive_triggers`, `table_info`, `optimize`, `quick_check` allowed.            | Only the file store sets PRAGMAs.                                                                                                                                     |
| TEMP, ATTACH, rtree                                                                                                                   | Refused                                                                                                                                                                                            | Not used                                                                                                                                                              |
| Triggers, WITHOUT ROWID, STRICT, partial and expression indexes, FTS5, window functions, recursive CTEs, `RETURNING`, upsert, ANALYZE | Allowed                                                                                                                                                                                            | Triggers, WITHOUT ROWID, expression indexes, recursive CTEs used                                                                                                      |
| Functions                                                                                                                             | Allowlist. `sqlite_version`, `subtype`, `load_extension` refused; no `generate_series`, `carray`. `json_each`, `unhex`, `instr`, `substr` allowed.                                                 | Uses only `cast`, `coalesce`, `count`, `max`, `instr`, `substr` (+ `json_each`, `unhex` now). No custom functions or collations since the vanilla large-value format. |
| Bound parameters                                                                                                                      | **100** per statement, `?NNN` up to `?100`                                                                                                                                                         | Up to 10,002 (IN lists, policy batches of 500 ids). **Fixed.**                                                                                                        |
| Statement length                                                                                                                      | 100,000 bytes, for the whole text of one `exec` (multi-statement too)                                                                                                                              | Schema DDL batches past 100 KB. **Fixed.**                                                                                                                            |
| Expression depth                                                                                                                      | 100 (a chain of n AND/OR terms is n deep)                                                                                                                                                          | Flat AND/OR chains. **Fixed** (balanced past 32 terms).                                                                                                               |
| Columns per table                                                                                                                     | 100                                                                                                                                                                                                | 5 system columns (`rid`, `id`, `_b`, `_seq`, `_del`), so 95 for the app. Not fixed: an explicit error for wider tables is the honest answer.                          |
| Compound SELECT                                                                                                                       | 5 terms                                                                                                                                                                                            | 2 at most                                                                                                                                                             |
| LIKE pattern                                                                                                                          | 50 bytes                                                                                                                                                                                           | Not used (`contains` is `instr`)                                                                                                                                      |
| Value / row size                                                                                                                      | 2 MiB                                                                                                                                                                                              | Large values are chunked; largest traced value well under.                                                                                                            |
| Integers                                                                                                                              | Read back as JS numbers (exact to 2^53); BigInt cannot be bound. Text bound against an INTEGER column compares exactly.                                                                            | Local seqs from 2^62 and scratch slots from 64 hash bits. **Fixed** (2^52 base, 52-bit slots). User `I64`/`U64` columns still need the bridge below.                  |
| Internal tables                                                                                                                       | `_cf_*` names reserved; `_cf_KV` appears in the file                                                                                                                                               | The "fresh store" check counted non-`_jazz_` tables. **Fixed.**                                                                                                       |
| Exec cost                                                                                                                             | ~2.3 us per `exec` with a repeated text (workerd caches statements), ~7.4 us for a new text                                                                                                        | rusqlite statement cache, 512 entries                                                                                                                                 |

## Fixes in jazz-db

- **Long IN lists bind one JSON array.** Past 16 values, `x IN (?1, …, ?n)`
  becomes `x IN (SELECT value FROM json_each(?k))` (`unhex(value)` for
  blobs) in the query compiler, the policy batch check (`versions_in`) and
  the fan-out order-key read. One placeholder, one cached statement for
  every length. Cost natively (Python sqlite3, 100k-row table, encoding
  included): +40% at 20 ids, +15% at 500, +70% at 10,000 on that one
  statement. Reals and blobs mixed with other types keep placeholders.
- **DDL runs one statement at a time** (`execute_script`), split at
  `sqlite3_complete` boundaries so trigger bodies stay whole.
- **AND/OR chains nest in halves past 32 terms**, so depth grows with
  log n. SQLite splits nested AND/OR terms the same as a flat chain.
- **Engine integers stay below 2^53**: `LOCAL_BASE` is 2^52,
  `NON_DURABLE_BASE` (a worker's tabs) is 1.5 · 2^52, scratch slots are 52
  bits.
- **The fresh-store check ignores `_cf_*` tables** and escapes `_` in its
  LIKE pattern.

## What still needs a seam (the next step)

None of these change the data model; they decide the shape of a storage
trait jazz-db could adopt (rusqlite natively and in the browser,
`ctx.storage.sql` through a JS bridge on DO).

1. **Transactions as scopes, not statements.** Replace the 11
   `execute_batch("SAVEPOINT …")` sites with `storage.scope(|| …)`:
   `SAVEPOINT`/`RELEASE`/`ROLLBACK TO` on rusqlite, nested
   `transactionSync` on DO (an `Err` becomes a throw, which rolls back that
   scope only). Ten sites are already lexical.
2. **The async Jazz transaction holds `SAVEPOINT tx` across `await`.** On DO
   the callback-shaped `transaction()` can't be opened from Rust and closed
   later without its release slipping to a microtask. Principled fix: stage
   a mergeable transaction's writes the way open transaction batches
   already are, and apply them in one scope at commit (reads-your-writes
   through the existing overlay scope). Fallback: run the callback inside
   `transactionSync` and refuse with "awaiting I/O inside a transaction is
   not supported on Durable Objects yet" if it does not complete in one
   poll.
3. **Commit and rollback hooks** (the large-value chunk cache pins nodes
   until commit) become "outermost scope ended" callbacks from (1).
4. **No reader connection.** The file store's second, read-only connection
   reads chunks while the writer is mid-statement. On DO there is one
   connection; read chunks through it, and give the chunk cache a byte
   budget (a DO isolate has 128 MB, shared by its objects), as file stores
   have.
5. **No PRAGMAs**: a DO store is a durable store without the file store's
   PRAGMAs (durability and caching are DO's).
6. **Exact 64-bit integers.** The bridge binds integers beyond 2^53 as
   decimal text (exact against INTEGER columns), and the compiler projects
   `I64`/`U64` columns as `CAST(… AS TEXT)` on DO so the decoder, which
   already decodes by declared type, parses them exactly.
7. **The statement surface is small.** jazz-db uses `prepare_cached`
   (25 sites), `query_row` (25), `query_map` (13), `execute` (33),
   `execute_batch` (41, mostly savepoints and DDL) and reads `ValueRef`
   cells. A trait with `exec(sql, params) -> rows`, `scope(f)` and
   `on_commit` covers it; DO's per-text statement cache stands in for
   rusqlite's.

Risks to keep an eye on:

- **Expression depth 100** also bounds nested `EXISTS` (multi-hop
  policies), `CASE` nesting from large-value comparisons and unrolled
  `Inherits`. Every current test passes under the limit; a compile-time
  depth check that answers "not supported yet" would make it explicit.
- **95 app columns per table.**
- **Per-statement overhead**: ~2.3 us per `exec` plus the wasm-to-JS
  crossing, against ~1 us natively. Savepoints are 39% of statements today;
  as `transactionSync` scopes they cost a JS call each, not an `exec`.

A CI job running `cargo test --workspace --features jazz/durable-object-limits`
keeps all of this from regressing.
