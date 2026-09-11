# Permissioned cold load: structural profile and three improvements

This iteration profiles the full-scale, anonymized permissioned resource graph on
native Core → relay → client, with RocksDB and in-process semantic messages.
The relay and client start empty; Core uses its populated seed cache. This is
not a browser timing or a reopen of a populated client. Seed creation is excluded.
All runs below deliver 27,518 expected rows across 39 subscriptions, including
23,831 visible children out of 43,000 candidates in the dominant table.

## Measured improvement

| Runtime state                                       | Correct subscription readiness |
| --------------------------------------------------- | -----------------------------: |
| Before this iteration (#2793)                       |                       51.914 s |
| Skip unconstrained completed-parent history (#2794) |                       41.829 s |
| Skip fresh-occurrence order scans (#2795)           |                       38.718 s |
| Probe inline values without decoding (#2796)        |                       35.863 s |

These are individual optimized native measurements, not medians. The final
CPU-sampled repeat reached readiness in 36.351 s. The ordinary final run's
settle loop took 35.365 s and complete harness wall time was 37.634 s. Final
one-shot verification and diagnostics extend wall time beyond readiness.
The three changes save about 31%; the approximately 5 s target remains unmet.

Run a clean optimized timing with:

```sh
cargo build -p jazz-sim --bench customer_cold_start --profile perf
JAZZ_CUSTOMER_IDENTITY=member JAZZ_CUSTOMER_PHASES=cold \
JAZZ_CUSTOMER_SCALE=1.0 JAZZ_CUSTOMER_MAX_TICKS=200000 \
cargo bench -p jazz-sim --bench customer_cold_start --profile perf
```

Use the Cargo-reported executable directly for profiling so samples exclude
Cargo/build work. CPU samples used `perf record --clockid mono -e task-clock
-F 499 --call-graph dwarf,32768`; a 65528-byte stack capture was also inspected.
Some libc callers still fail to unwind, so inclusive percentages are incomplete
and overlapping. Do not add them or treat their differences as exact wall time.

## Why completed parents were expensive

`settle_completed_parent_batch` invalidated transaction caches, then loaded each
parent's row versions before finding whether any child needed them. A cold
`query_versions_for_tx` lacked a table hint and probed history indexes across
unrelated tables. The fixture has a separate seed transaction per row.

The new path scans pending constraints once for the batch and groups only the
relevant parents. No waiting child means no parent-history reload. Complete
wrong-coordinate parents still reject pending children; partial/missing parents
remain inconclusive. The regression includes an unrelated waiting child.
Restoring the original implementation fails with 32 history reloads instead of
zero. Existing receiver/reopen cases remain covered.

## What the growing order vector represents

The maintained view stores occurrence records in a map and their collector-defined
positions in a vector. Distinct occurrences may refer to the same public row.
An insert can replace an existing occurrence, so its old position must be removed
before reinsertion. Removing every matching key before every fresh insert was
unnecessary: 23,831 distinct inserts performed about 284 million comparisons.

The existing occurrence map now determines whether that removal is needed. Fresh
keys take the direct insertion path; replacement, removal and move behavior remain.
A 2,000-occurrence test detects both always scanning (1,999,000 comparisons) and
never removing a replacement (2,001 occurrences instead of 2,000).

## How 27,518 results become 2,061,039 projection inputs

These counters count a record once per projection operator it visits, not once
per distinct row, storage read, or byte-wire message.

| Node                      | Projection input visits |
| ------------------------- | ----------------------: |
| Core                      |                 522,580 |
| Relay                     |               1,118,743 |
| Client                    |                 419,716 |
| Total during settle ticks |               2,061,039 |

A temporary per-operator trace reproduces those totals exactly. The dominant
child query accounts for 473,715 visits on Core and 979,250 on the relay:

- Core: 11 distinct projection operators each process 43,000 child records,
  plus 11 × 65 supporting parent records.
- Relay: 10 distinct operators process 43,000 candidates; 22 distinct operators
  process 23,831 visible records; smaller parent/access/group relations add work.
- Client: 14 distinct operators each process a 23,831-record batch, alongside
  other tables and transaction-key processing.

For these large batches, this is pipeline depth, not repeated invocation of the
same operator. The projections adapt physical/history fields, expose application
metadata, attach permission claims and coverage, prepare collector fields, and
produce supporting-version/membership inputs. For example, a full child record
is copied into a collector-prefixed layout and another layout adds a closure root.
The final one-shot verification queries add 137,590 projection visits outside the
settle ticks; those are not included in the 2,061,039 total.

320,362 visits have identity-shaped field mappings, making them candidates for
sharing encoded bytes. This is an upper-bound candidate count: type/layout
compatibility still needs checking before any copy can actually be removed.
The old dominant-child attribution bucket fails to recognize the lowered graph;
its zero must not be interpreted as absence of dominant-table work.

## Inline materialization

Previously the materializer decoded every field to allocated Values, recursively
visited nested values, and returned the original bytes when nothing changed.
It now inspects descriptor-directed tags and offsets before allocating Values.
Inline records pass through; real chunk references retain the existing blocking,
loading and retry path. Selected-field materialization ignores unselected chunks.
Ordinary String and Bytes are recognized as indirect-capable, matching their codec.

Tests cover bytes, strings and JSON-backed scalars under nullable/array/enum/record
boundaries, missing chunks, retry, invalid field indexes and unselected references.
Always-skip and always-decode mutations both fail. The ordinary materialization
path fell from approximately 5% of initial CPU samples to below 1% afterward.
No storage or wire encoding changes are involved.

## Allocation profile and remaining thesis

The current workload reports approximately 485.6 million Rust allocation requests
and 295.7 GB of cumulative requested bytes. A run sampled every 32,768 allocations
captured 14,818 stacks without reaching the cap; its totals agree with a denser
run to within a few hundred requests. These are allocation churn, not peak RAM,
retained data size, actual bytes copied, or C++/RocksDB allocation totals. The
instrumented allocator includes growth requests and must not define a normal
wall-time receipt. Counting ends after final verification/storage diagnostics.

Across all 14,818 sampled allocation stacks, 26.7% pass through schema/descriptor
construction, 17.1% through author conversion, 12.6% through supporting-version
reconstruction, 8.1% through VersionRecord cloning and 4.7% through join processing.
These are overlapping allocation-stack shares, not exclusive CPU-time shares.
Symbols were resolved against the executable's PIE virtual addresses (not ELF
file offsets), with inline frames included. The first capped sampler's top-stack
list is not used for these whole-run percentages.

Code walks confirm per-row work that should be schema- or batch-level:

- `update_table_source_from_inputs` calls `record_schema_for_variant` inside its
  stored-row loop; that rebuilds descriptor field vectors.
- `VersionRow::from_wire_with_schema_version` constructs a history storage table
  while preparing individual records.
- `RowAuthor::value_type`, `to_value`, and `from_value` reconstruct the same nested
  account/identity descriptor structure repeatedly.
- `decode_history_owned_record` walks logical table mappings and formats physical
  table names to reverse-resolve a known physical storage table.
- Join processing creates temporary encoded keys/buckets, and supporting-version
  conversion rebuilds records from allocated cells and author metadata.

The next structural direction is to prepare descriptors and physical mappings once
per schema/batch, then retain encoded records through more of the query pipeline.
Projection fusion or shared backing bytes may remove entire passes; counters alone
do not establish which permission stages are redundant. Durability relaxation
previously showed no measurable improvement and remains a lower-priority lever.

## Verification and limits

#2794: 2,001 Jazz library tests pass, 2 ignored. #2795 and #2796: 2,002 pass,
2 ignored. #2796 additionally passes 734 Groove library tests, 2 ignored.
All three changes have mutation-sensitive regressions and successful full-scale
cold fixture counts. These are library/focused receipts, not CI-equivalent gates
or browser/device acceptance. The PRs remain drafts in stack #2773.

Broad Cargo tests require the runner's 4 MiB thread stack and sufficient file
descriptors (`RUST_MIN_STACK=4194304`, `ulimit -n 65536` here). Smaller defaults
caused a stack abort and RocksDB descriptor-exhaustion failures before clean reruns.
Diagnostic instrumentation was preserved separately and removed from production
changes. The existing warm-relay reopen failure remains tracked in #2792.
