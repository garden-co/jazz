# Jazz on Durable Object SQLite

Jazz's authority can use a Durable Object's own `ctx.storage.sql`, without a
page VFS. The refresh found a query-compilation limit the earlier spike missed;
not every current query shape fits yet. This directory contains compatibility probes
rather than a production DO provider. Implementation
work is described in [PLAN.md](PLAN.md).

## Compatibility baseline (2026-11-04)

The refreshed probes run on **Miniflare 4.20260730.0 / workerd 1.20260730.1**,
with compatibility date `2026-07-01`. This is the version currently available
from the npm registry used here; it is not a verification of November production
workerd. Versions are pinned in `package-lock.json` and printed by the runner.

| Area                         | Observed behavior / current Jazz implications                                                                                                                                                                                                                                  |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Transactions                 | SQL BEGIN, SAVEPOINT, RELEASE, COMMIT and ROLLBACK are refused. Nested `transactionSync` rolls back an inner throw independently. Async `transaction` rolls back across await; SQL issued by an awaited helper participates, on both success and failure.                      |
| Columns                      | 100 per table. Current rows have 8 structural columns (`rid`, `id`, `_branch`, `_clock`, `_writer`, `_state`, `_base`, `_touched`) plus 4 provenance columns: **88 physical app columns**. Retained migration columns count. History tables omit `rid`.                        |
| Bound parameters             | 100 accepted, 101 refused. Long eligible IN lists still use one JSON parameter; mixed-type shapes may retain placeholders.                                                                                                                                                     |
| SQL length                   | Exactly 100,000 ASCII bytes accepted; 100,001 refused. The limit applies to the whole `exec` input. The schema DDL helper splits at complete-statement boundaries, including trigger bodies.                                                                                   |
| Expression / compound limits | Flat AND chain of 101 refused (depth limit 100); 5 compound SELECT terms accepted, 6 refused. Jazz balances long AND/OR chains, but nested policy/query shapes can still exceed limits.                                                                                        |
| Function arguments           | This workerd accepts 127 and refuses 128. Cloudflare documents **32**; native `durable-object-limits` now uses the conservative documented value.                                                                                                                              |
| Values                       | Large values remain chunked. The probe accepts a BLOB just below 2 MiB and rejects one just below 3 MiB; the SQLite length limit also covers a whole row.                                                                                                                      |
| Integers                     | Reading `9007199254740993` as a JS number loses precision; BigInt bindings are refused. Decimal text bound to a current affinity-free app column stays TEXT and changes equality. `CAST(? AS INTEGER)` plus `CAST(value AS TEXT)` round-trips signed 64-bit endpoints exactly. |
| Storage classes              | Current app columns have no type affinity. Inline text/JSON is TEXT, bytes are BLOB, and chunked cells are INTEGER epoch markers. Integral REAL values retain REAL in SQL even though JS cannot distinguish them from integers.                                                |
| Internal tables              | KV use creates `_cf_KV` in this workerd, which current fresh-store checks ignore. Creating `_cf_x` is refused.                                                                                                                                                                 |
| PRAGMAs                      | journal mode, synchronous, cache size, user version, temp store and page count are refused. Foreign keys, recursive triggers, table info, optimize, data version and quick check work.                                                                                         |
| SQL features                 | Triggers, WITHOUT ROWID, STRICT, partial/expression indexes, FTS5, ANALYZE, JSON functions and unhex work. TEMP, ATTACH, rtree and sqlite_version are refused. LIKE patterns accept 50 bytes and reject 51.                                                                    |

The column diagnostics are enabled by the native
`durable-object-limits` feature. Schema admission checks the physical union
before DDL, including retained columns, and names the table and limit in a
`Schema` error. Other SQLite failures retain their existing error classification.
This feature is a compatibility test profile, not the production storage interface.

References: [Cloudflare SQL limits](https://developers.cloudflare.com/durable-objects/platform/limits/),
[SQL storage and transaction APIs](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/).
The documented function-argument limit and measured runtime limit differ; do
not silently treat the more permissive local result as the production contract.

The pinned [workerd security profile](https://github.com/cloudflare/workerd/blob/v1.20260730.1/src/workerd/util/sqlite.c%2B%2B#L1353)
also limits compiled VM instruction allocation to 25,000, trigger depth to 10,
attached databases to zero, and worker threads to zero. These are now included
in the native profile. Its length limit is 2,200,000 bytes; the native profile
retains the more conservative 2 MiB limit. Instruction allocation grows in
chunks, so fewer than 25,000 emitted instructions can still exhaust its budget.
The probe accepts 5,000 literal IN members and rejects 8,000 with SQLITE_NOMEM.

## Run the probes and replay

From the repository root:

```sh
npm ci --prefix dev/durable-objects
node dev/durable-objects/run-probe.mjs

# Use a new trace path for each run: the tracer appends.
JAZZ_SQL_TRACE=/tmp/jazz-do-trace.jsonl cargo test --workspace \
  --features jazz/durable-object-limits,jazz/sql-trace --no-fail-fast
python3 dev/durable-objects/replay-prep.py /tmp/jazz-do-trace.jsonl /tmp/jazz-do-jobs.json
node dev/durable-objects/run-replay.mjs /tmp/jazz-do-jobs.json /tmp/jazz-do-failures.json

# Native boundary regression tests.
cargo test -p jazz --features durable-object-limits --test integration durable_object_limits
```

The probe runner asserts transaction outcomes, exact casts, and key limit
boundaries, and exits nonzero on a mismatch. `--bench` separately enables the
old exec timing probe; ordinary compatibility runs do not measure performance.

Replay executes DDL and prepares other statements with EXPLAIN QUERY PLAN and NULL
bindings. It skips transaction statements and PRAGMAs; probes cover those
separately. It does not execute the traced writes, prove result correctness,
or reconstruct a reopened file's earlier connections. Full EXPLAIN cursor
enumeration hung after schema changes in the pinned runtime; QUERY PLAN avoids
bytecode rendering while still preparing SQL and draining each cursor. The
runner saves progress, bounds each job to 30 seconds, records incomplete jobs,
and exits nonzero for any failure. `START`/`END` select a job range and
`DEBUG_JOB` prints its operations. Missing-table failures
must be inspected rather than counted as platform incompatibility. Failed
native prepares do not enter the SQL trace. Native limits do not emulate the
DO authorizer, JS value bridge, or callback transaction lifetime.

## Validation of this refresh

- Probe assertions pass on the pinned runtime, including async helper commit
  and rollback, exact integer casts, storage classes, and limit boundaries.
- The final workspace suite with all 12 native limits reports 2,644 passed,
  2 failed, and 11 ignored.
  Both failures are `durable_file` memory tests that read `/proc/self/status`,
  which is unavailable on the macOS host. Their expectations were not changed.
- A subsequent Jazz integration run, including the new fresh-schema,
  schema-evolution, and retained-column boundary tests, reports 908 passed,
  0 failed, and 4 ignored.
- The new trace contains 5,376,911 statements, 14,360 distinct SQL texts, and
  5,717 connections. This capture preceded adding the VM/trigger limits.
  Replay preparation produces 2,142 jobs / 223,444 operations.
  No traced binding exceeds 100 parameters or the scanned 2 MiB value bound.
  Wide integer literals/bindings remain present and require the typed bridge.
- All 2,142 replay jobs complete with 978 errors: 977 missing-table errors
  (coverage gaps in per-connection schema reconstruction) and one reproducible
  `SQLITE_NOMEM` on the S1 SaaS policy query. Missing-table errors are not passes.
  There are no other error categories or timed-out jobs in the QUERY PLAN run.

The S1 failure combines history-cut unions with nested membership/inheritance
policy joins. Its full EXPLAIN contains 32,106 instructions on SQLite 3.51.3
and 31,459 on Jazz's bundled SQLite 3.53.2. Setting the 25,000 VM allocation
limit reproduces the failure on both versions. The native S1 scenario tests
still pass, so they do not establish that this exact emitted shape fits. This
is a query-shape restriction, not evidence of exhausted isolate heap. The
fixture contains only the existing simulator's synthetic DDL and the failing
query; it fails in a fresh workerd with no application rows:

```sh
node dev/durable-objects/run-replay.mjs \
  dev/durable-objects/fixtures/s1-policy-prepare.json /tmp/s1-policy-failures.json
# Expected today: one SQLITE_NOMEM failure and a nonzero exit.
```

The existing S1 expectations remain unchanged. This baseline does not resolve
policy-query expansion or establish support for the failing shape. Do not turn
an arbitrary SQLITE_NOMEM into an unsupported-query error: real allocation
failures share that code and need to remain distinguishable.

## Historical evidence

The October spike captured 2.4 million statements, 17,000 distinct SQL texts,
and 1,624 connections. It found and fixed long IN bindings, oversized DDL
batches, deep AND/OR chains, engine integer ranges, and fresh-store detection.
Those counts and its reported suite results describe that snapshot, not the
current tree. Its 89/95 app-column figures and large-value TEXT-marker advice
are obsolete. The subsequent #4579 experiment ran sync over WebSockets with
10–500 sessions, but its timing and memory estimates are not measurements of
this refreshed build.
