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

Next implementation hypothesis: build a coordinate index once, compute each
incoming key once, and preserve exact byte comparison for matches, conflict
rejection, missing-version ingestion, partial cardinality and fate updates.
Keep this separate from storage and protocol changes. Pin work scaling with a
deterministic regression and confirm sensitivity before repeating measurements.
The full 224 ms is an inclusive upper bound, not a promised saving.

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

No runtime change or measured improvement beyond #2785 is claimed by this note.
