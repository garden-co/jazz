# jazz-db on Durable Object SQLite

A de-risk spike: can jazz-db (the SQLite engine on the SQLite-only branch)
run on Cloudflare Durable Object SQLite (`ctx.storage.sql`)? Measured in a
local workerd (`workerd 2026-07-30`, via Miniflare 4), which enforces the
same authorizer and limits as production.

**Verdict: yes, with no design change to the data model.** Every statement
jazz-db issues across the whole workspace test suite (2.4M statements,
17k distinct texts, 1,624 connections) prepares on Durable Object SQLite
once the fixes in this branch are in, except the transaction statements,
which need a scoped transaction API (below). The work left is a storage
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

## Fixes in this branch

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
- **Engine integers stay below 2^53**: `LOCAL_BASE` is 2^52, scratch slots
  are 52 bits.
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

## Single-object ceiling (an unsharded app on one object)

`doc-shelf-do-ceiling` (in `examples/doc-shelf/benchmarks`) models a
collaborative task app. Tasks live in projects of `PROJECT` rows (200 by
default), with `GROUP` connected users per project (5), each subscribed
to its project. A user write is one transaction updating `ROWS` tasks
(10), so it fans out to the project's other users. The authority is a
file store of `STORE` tasks. The bench times only the authority's tick,
because that is the object's single thread. It runs natively: jazz-db
does not run inside workerd until the storage seam exists, so the
Durable Object numbers below are estimates built from native
measurements and the DO per-statement costs measured above.

Native authority, release build, on one core of a cloud dev container
(10-row writes, groups of 5, one write per tick):

| connected sessions |     stored rows | authority time per write | writes/s at 100% | idle tick |
| -----------------: | --------------: | -----------------------: | ---------------: | --------: |
|                 10 | 10k / 100k / 1M |       1.9 / 2.0 / 1.8 ms |  535 / 500 / 544 |  16-28 us |
|                100 | 10k / 100k / 1M |       2.6 / 2.3 / 2.2 ms |  386 / 427 / 459 |  34-37 us |
|              1,000 |       100k / 1M |             5.4 / 4.9 ms |        186 / 203 |  60-72 us |
|              3,000 |            400k |                  11.1 ms |               90 |    296 us |
|             10,000 |            400k |                  37.1 ms |               27 |    918 us |

- **Store size does not matter** from 10k to 1M rows (7 MB to 279 MB).
  Point reads and index maintenance stay flat. The tick p99 is 11-17 ms
  at every size, from WAL checkpoints in the file store.
- **Rows per write dominate**: a 1-row write costs 0.54 ms (1,850/s).
  A 10-row write costs about 2 ms: about 153 statements (125 of them
  reads), roughly 45% of the time in SQLite and the rest in the engine.
- **Fan-out width**: 50 users on one project costs 8.3 ms per write
  (255 KB of images out), against 2.9 ms with nobody else watching. That
  is about 110 us per recipient.
- **Batching does not amortize the write itself**: 10 or 50 writes per
  tick still cost 2.0 ms each. It does amortize the per-session cost below.
- **Every connected session costs about 3.5 us per tick** whether or not
  the write concerns it (the tick walks every session). At 1,000 sessions
  that is more than the write itself. Skipping sessions with nothing
  pending is an engine fix this branch does not have yet.
- **Memory is about 1.25 KB per (session, subscribed row)**: the
  session's watch keys and held-row maps. 1,000 sessions over 1,000-row
  projects take 1.3 GB, 1,000 sessions over 10-row projects take 48 MB.

### On a Durable Object (estimate)

- **CPU per 10-row write**: the SQLite work itself (~1 ms), plus 153 ×
  ~2.3 us per `exec` through `ctx.storage.sql` (~0.35 ms) and the
  wasm-to-JS crossings, plus the engine at an assumed 1.5-2× native in
  wasm (~2-2.6 ms). That is about 3.5-4 ms, so ~250-280 writes/s with
  the thread at 100%. To keep queueing latency low (under ~60% busy),
  plan on **~150 sustained 10-row writes/s**, minus ~6 us per connected
  session per tick.
- **Memory binds first**: an isolate has 128 MB, shared with the
  object's other state. At 1.25 KB per (session, row), with 200-row
  subscriptions, that is **~350 concurrent sessions** in ~90 MB.
- **Storage** (10 GB per object) is far away: 1M tasks are 279 MB.

What can differ in production: production CPUs and wasm speed, and
WebSocket hibernation (it frees an idle session's memory, but a woken
session pays its subscription again: ~1.3 ms per 200-row session here).
The output gate delays replies until writes are durable, which adds
latency but not CPU. The isolate's 128 MB is shared by the objects
placed in it.
