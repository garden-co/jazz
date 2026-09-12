# Prepared physical field accessor trial

This tests an execution-only change on the existing lowered graphs: resolve
fixed field ranges and variable offset-table slots while preparing a projection,
then reuse those constants across input rows. The existing encoded layout and
record validity checks are preserved. Nested fields prepare one accessor for
each level; constant/evaluation errors remain lazy.

Unlike #2797, this prepares physical span access, not just field names, types and
nested descriptor paths. It does not repeat the rejected fresh-join constructor
#2864. General descriptor field access outside prepared projections is unchanged.

Initial validation: full Groove library suite passes (757 passed, 2 ignored).
An internal byte-boundary oracle compares prepared spans with the general
field-span implementation across fixed/mixed/variable records, every truncation,
extra trailing bytes and invalid offset-table entries. Existing projection tests
cover nested fields, nullability, mixed evaluation and rollback.

Completed measurements are below. No speedup or release readiness is claimed.

## Result: no established end-to-end improvement

Five pairs, alternating execution order, optimized native, default member
fixture, RocksDB Core/Edge/Client. Release gate paused at a partition boundary;
no compilation overlapped measurements. Each run verified 27,518 expected rows.
Times cover connect + subscribe + settle, excluding seed setup and subsequent
one-shot verification/storage accounting.

| Round  | Baseline ms | Candidate ms |
| ------ | ----------: | -----------: |
| 0      |       11216 |        11144 |
| 1      |       11611 |        11519 |
| 2      |       11174 |        11521 |
| 3      |       11249 |        11239 |
| 4      |       11160 |        11285 |
| Median |       11216 |        11285 |

Candidate median is 0.62% slower. Pair directions are mixed and ranges overlap;
this does not establish either a useful gain or a robust regression. It is not
recommended for the release candidate.

Separate scoped 499Hz cpu-clock profile: projection 0.255 CPU-seconds (2.21%),
fresh join-index construction 0.539s (4.69%), collect-by update 0.513s (4.46%),
graph deduplication/addition 0.429s (3.73%). These are inclusive sampled paths,
not additive exclusive buckets. The old baseline profiles suggested larger
projection cost, but were not contemporaneous paired profiles; do not quote
their difference as a precisely measured local speedup.

Whole-process copy leaves remain 1.216s (10.57%), comparison leaves 0.657s
(5.71%) and allocator clock paths 0.990s (8.60%). They are not entirely Groove
and overlap inclusive caller costs. Host clocksource remains HPET. No hardware
or allocator settings changed. The profile does not isolate a dominant single
operator or justify another round of the rejected #2864 constructor.

Mutation sensitivity: shift prepared constant offsets by one byte; the new
oracle fails with span1..9 versus0..8. Restore source and rerun before claiming
validation. Full-suite baseline candidate checks were757passed/2ignored.

Provenance: baseline f56e5f3b49; candidate Rust source is4ce927fb4b. Candidate
build began before commit, so its embedded dirty-source receipt is retained
rather than rewritten; source was unchanged between build and commit. Binaries,
build JSON, all10 receipts and profile are preserved outside target under
`/home/ubuntu/jazz-debug-evidence/permissioned-profile/prepared-accessors/`.
