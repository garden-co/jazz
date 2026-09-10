# Next performance slice: known-transaction row matching

Investigation baseline: ancestry runtime ebf279ce49 (PR #2785), measured as
3bfdd6d264 in an isolated checkout with identical runtime files. Optimized WASM,
synthetic persistent-browser workload: 1,500 two-field rows, 1,350 updates,
active subscription, local durability. Runtime reopen in an already loaded
browser; not browser startup, forced disk-cache eviction or percentile data.
Unprofiled batch 1,166.78 ms; initial reopen read 965.07 ms; post-batch reopen
read 1,010.54 ms. Separate sampled CPU runs include profiling overhead.

## Findings

Inclusive times overlap; do not sum parent and child totals or page and worker.

| Path                                         | Initial read |  Batch | Post-batch read |
| -------------------------------------------- | -----------: | -----: | --------------: |
| Worker supporting-view construction          |       343 ms | 342 ms |          308 ms |
| Page known-transaction ingestion             |       224 ms | 223 ms |          183 ms |
| Worker transaction ingestion/current indexes |            — | 455 ms |               — |

Known-transaction ingestion in `crates/jazz/src/node/ingest/commit_bundles.rs`
loads stored versions and then performs a linear `find` for every incoming
version. `view_version_key_for_ingest` reconstructs owned table/branch keys and
reads row identity and deletion state for comparisons. Matching two wide copies
of a transaction can therefore perform quadratic comparisons. Unlike ancestry,
this operation does need the whole relevant transaction; it does not need a
repeated search through it.

Implementation: use Groove's batch-scoped `ensure_exact` on each incoming
physical history record. Storage compares encoded values without returning old
row bodies; Groove stages only missing records and retains the lookup result
for delta computation. Exact duplicates add no history writes. The batch is
bound to its database/resident revision and a conflict invalidates it. Jazz
retains transaction identity, partial cardinality, fate and visibility rules.
This removes the repeated search rather than indexing all stored rows. No
storage or wire encoding changes. Updated timings are pending.

Other candidates for subsequent slices:

- Record field access: `BorrowedRecord::field_bytes` accounts for 123 ms in the
  initial-read worker and 124 ms in the batch worker. Inspect repeated access
  and layout lookup rather than assuming every bounds check is unnecessary.
- Descriptor construction: `from_logical_fields` accounts for 47–89 ms per
  measured runtime/phase; structured author type construction contributes
  27–48 ms within those totals. Inspect reuse of immutable descriptors.
- Page `VersionRecord::deletion` costs 81 ms on initial read and 96 ms in the
  batch, including children. It is used by repeated row-key construction, so
  measure again after removing that repetition before optimizing the accessor.
- Worker page encoding and internal-shape validation have 60 ms and 31 ms self
  time in the batch profile. These are smaller than current publication and
  ingestion costs and should not lead the next slice.

This note records the baseline; no new performance improvement is claimed until remeasurement.

### Receiver batching boundary found during browser acceptance

The first optimized browser run rejected a post-update reopen: two authoring
nodes appeared in one received snapshot, and registration of the second node's
alias published metadata after the first row's exact-match preparation. A native
multi-author receiver test reproduces this failure. Receiver batching now resolves
all author, parent and schema aliases before preparing rows.

Known complete transactions can also publish through their fate-update path.
They are handled after the prepared receiver batch commits, rather than publishing
inside it. A regression covers a new row alongside a previously pending known
transaction becoming accepted. Removing the deferral reproduces the stale-batch
failure; the fixed path preserves both rows and the accepted fate.

The failed intermediate browser run is not a valid performance measurement.
The final optimized build and fresh timing/profile round are pending.
