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
The corrected optimized build passed the fresh timing/profile round below.

### Final measurements

Implementation revision: `e05e400dd5`; measurement checkout: `64de44ea7e`.
All changed Rust files were byte-identical. Hosted CI passed on the implementation
revision. Local Jazz library tests: 1,998 passed, 2 ignored; Groove: 732 passed,
2 ignored. Backend comparison tests and the mutation checks described above pass.

Same optimized WASM and persistent-browser harness as the previous slice:

| Operation, 1,500 rows                          | Previous slice | This slice, two quiet runs |
| ---------------------------------------------- | -------------: | -------------------------: |
| First read after runtime reopen                |         965 ms |                 778–808 ms |
| 1,350 updates, commit through local durability |       1,167 ms |             1,116–1,181 ms |
| First read after post-update runtime reopen    |       1,011 ms |                 856–865 ms |

These are runtime reopens in a loaded Chromium session, not browser-process cold
starts. The first-read improvement is roughly 16–19%, and post-update read improvement
is 14–15%; bulk-write latency is effectively
unchanged in this small sample. The four-size batch sweep measured 119, 226, 541,
and 1,181 ms at 150, 300, 750, and 1,500 rows respectively. Browser correctness:
four-size sweep 4 passed, single-size repeat 1 passed, profiling run 1 passed.

Rust-function CPU profiles show the intended path improved substantially:
foreground `ingest_known_transaction` fell from 224 to 43 ms on initial read,
223 to 46 ms during the batch, and 183 to 39 ms on post-update reopen. Whole-unit
fate handling still legitimately reads transaction history; immutable matching
no longer adds a second full-transaction read.

The next bottlenecks are outside exact matching. During the batch, the worker
profile attributes about 340 ms to supporting-row update construction, 132 ms
to record field-byte access, 153 ms to Groove batch application and 116 ms to page
encoding. Allocation/deallocation account for about 191 ms of self time across
these operations. Worker `ensure_exact` is about 7 ms, including about 2 ms in
backend comparison. These are overlapping inclusive categories except the stated
allocator self time; do not add them together or add foreground and worker totals.

The next thesis is to carry shared batch records through current-index maintenance
and publication instead of repeatedly extracting fields and reconstructing records
and descriptors. Further tuning of the exact comparison alone cannot deliver the
requested order-of-magnitude end-to-end gains. No further optimization is included
in this slice.
