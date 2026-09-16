# Partial-parent lookup and compiled payload projection

Follow-up to [the native baseline](native-local-batch-profile.md), using the
same synthetic fixture, optimized profile, clock-aligned Rust sampling and
MemoryStorage/RocksDB backends. These are native core phase measurements,
not browser results or complete app startup latency.

## Partial-parent misses

PR #2790 (`74b6f8e463`) avoids loading retained siblings when an exact parent
coordinate is missing from a known partial transaction. Such siblings cannot
establish invalidity; the existing pending coordinate constraint is retained.
Exact parent reads and complete-transaction rejection retain their semantics.

| Fresh foreground ingestion after reopening |   Before |  After |
| ------------------------------------------ | -------: | -----: |
| 1,500 rows, 50% updated, memory            | 2,151 ms | 120 ms |
| 1,500 rows, 90% updated, memory            |   887 ms | 130 ms |
| 1,500 rows, 100% updated, memory           |   135 ms | 134 ms |
| 3,000 rows, 90% updated, memory            | 3,376 ms | 275 ms |

The 50% case drops from 2,259,793 counted storage reads/index entries to
8,293. History-index entries drop from 1,126,500 to 1,500. These counters
include in-memory probes and index entries, not disk I/O. At 90%, the count
falls from 822,793 to 10,093. Doubling the input now approximately doubles
this phase, instead of roughly quadrupling it.

The regression covers cold and resident caches at two transaction widths.
Restoring the old implementation makes 32 misses perform 64 whole-transaction
loads and fails the assertion. All 1,999 active library tests passed.

## Compiled current-result payload projection

The next profile showed repeated descriptor construction and value conversion.
The current-result encoder rebuilt its schema for each row, materialized selected
values, encoded them, rebound and decoded the result, then encoded it again to
check its own output. It also serialized type descriptors for every row.

The replacement compiles a field mapping once per source descriptor in a single
terminal delta batch. Groove's existing registry-rebinding projector copies
encoded field spans and reconstructs framing. Compatibility is checked when
compiling the plan. Descriptor interning handles are cache keys, never ordered
or serialized identities. The cache cannot outlive or cross its terminal schema.
No storage or wire format changes. Publication descriptor bytes still accompany
the runtime payload; this slice does not redesign that representation.

After timings are medians of three uninstrumented runs, compared with the
immediately preceding partial-parent-only run, at 1,500 rows/1,350 updates:

| Publication phase | Parent fix only | Plus compiled projection |
| ----------------- | --------------: | -----------------------: |
| Initial, memory   |        215.8 ms |                 194.9 ms |
| Updated, memory   |        162.6 ms |                 135.5 ms |
| Initial, RocksDB  |        224.0 ms |                 206.4 ms |
| Updated, RocksDB  |        167.1 ms |                 136.8 ms |

Across five separately profiled runs, memory batch publication has 830 samples
before and 691 after. Samples containing the payload encoder/plan fall from
113 to 3. This agrees with the phase improvement, rather than relying only on
small timing differences. Some outer async frames remain unresolved/truncated.

All 2,000 active library tests passed (2 ignored). An internal byte-compatibility
test compares projected bytes with independently encoded selected fields across
32 rows, reordered inputs, nested enum registry rebinding, arrays and nulls.
Deliberately selecting an unrelated payload field makes that test fail; the
mutation was restored. The optimized harness also verifies exact row IDs and
updated-row sets on both backends.

## Remaining bottlenecks and interpretation

Memory worker ingestion remains approximately 229 ms for 1,350 updates;
this publication change does not improve that phase. In its final profile:

- 364/1,161 samples contain a record encode/decode or descriptor construction
  frame (union of those frames, counted once per sample).
- 189 contain `BorrowedRecord::to_values`; 160 contain `decode_value`.
- 285 contain an IVM `update_one_node` frame. This overlaps conversion work.
- 206 have memory copy as the leaf frame.

These figures overlap and must not be summed. They support eliminating repeated
representations, not merely tuning a serializer's primitive encoding speed.
`VersionRow::from_wire_with_schema_version` reconstructs storage tables,
materializes row values, then constructs and encodes a storage record per row.
The next candidate is a per-table conversion plan that retains encoded payloads
where wire/storage layouts permit it and encodes only changing metadata.

Within IVM, `materialize_record_attempt` expands all values even when it
ultimately returns the original bytes because nothing was indirect. A compiled
layout-aware fast path is another candidate. Known receipt ingestion still
reconstructs stored versions for conflict comparison. These remaining paths
are tracked in #2789; publication projection is only the first slice.

Tooling: optimized native core rebuilds took about 1m30–1m40 with warm caches.
The inner loop required no WASM build. The first new byte test used a tuple
with variable-width members, which Groove correctly rejects; its final fixture
uses a nested record. Native timing and profile collection ran after builds
and tests completed.

## Second target for this iteration: permissioned graph loads

Alongside `local_batch_phases`, track `jazz-sim`'s `customer_cold_start` at
`JAZZ_CUSTOMER_SCALE=1.0`: the anonymized large-load
workload historically described as approximately 20.5k rows with recursive group membership, resource access edges and inherited
child permissions. The fixture subscribes across 39 tables. Use the emitted
`expected_rows`/`rows_materialized` for exact counts rather than treating the
historical headline size as a fixed assertion.

Track member `cold` and `warm` separately. Cold starts with an empty relay;
warm primes, closes and reopens its RocksDB state, then connects a fresh
client. Warm therefore measures persisted relay reuse, not a retained browser
foreground. Keep admin cold and unauthorized (`spy`) cold as permission
controls. Preserve exact visibility assertions and compare phase, per-table
readiness, transport and memory metrics alongside total time.

Build the native benchmark once under `--profile perf`, then invoke its
executable directly with `JAZZ_CUSTOMER_IDENTITY=member`,
`JAZZ_CUSTOMER_PHASES=cold,warm`, `JAZZ_CUSTOMER_SCALE=1.0` and
`JAZZ_CUSTOMER_MAX_TICKS=200000`. The existing `repro-customer.sh` identifies the
historical matrix, but its output pipeline suppresses command failures; use
direct commands and inspect exit status for new receipts. Do not reuse old
seed databases across storage revisions without checking their compatibility.

The first current-tip cold receipt and the warm reopen blocker are recorded in
[permissioned-load-current.md](permissioned-load-current.md).
