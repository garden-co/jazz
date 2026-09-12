# Compact join bucket trial

Follow-up to the sorted arrangement replay (#2900), isolated from the release
checkpoint. Immutable per-key buckets use sorted record/weight vectors; the outer
key map, incremental overlays and shared snapshots remain.

Three quiet alternating native comparisons, unchanged default anonymized fixture,
RocksDB / RocksDB / RocksDB, mimalloc, optimized perf profile:

| Pair   | Parent ready time | Trial ready time |
| ------ | ----------------: | ---------------: |
| 1      |         11,364 ms |        11,226 ms |
| 2      |         11,763 ms |        11,103 ms |
| 3      |         11,334 ms |        11,205 ms |
| Median |         11,364 ms |        11,205 ms |

Readiness is connect + subscribe + settle. Each process verified 27,518 outputs.
The 1.4% median difference is much smaller than the roughly 581ms replay update
saving. These three pairs do not establish a precise product speedup. The hybrid
retains map/allocation machinery absent from the fully sorted replay; that is a
plausible explanation, not an attributed finding. Do not infer that full-record
hashing alone explains the replay benefit.

Correctness: 757 Groove tests passed, two ignored. The new private container test
covers signed-weight consolidation and snapshot isolation. Removing zero-weight
elimination makes it fail; restored source passes. Broad Jazz/browser acceptance
has not been run for this trial, and the change is not called ready or retained.

Receipts outside target: `permissioned-profile/compact-buckets/` contains exact
parent/trial binaries, source, build logs, raw runs, and test/mutation receipts.
The candidate binary was built before commit; its source snapshot is preserved.
