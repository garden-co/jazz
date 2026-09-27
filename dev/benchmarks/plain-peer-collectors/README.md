# Omit unused application collectors for plain peers

## Why ship

Peer publication sends query membership and immutable version facts. The receiver constructs its own application rows. Eligible serving peers therefore do not need another application collector retaining the same result.

The existing optimization skipped that execution sink only for queries selecting a Bytes column. This change removes that column-kind restriction. Ordinary semantic lowering still runs before the unused sink is removed, preserving compiler handoff, source requirements and authority/coverage checks. It adds no cache or protocol representation.

## Host-wake startup receipt

The anonymous `publication_fairness` mixed-local fixture opens 37 staged Local subscriptions over a preseeded owner and an initially empty foreground, both backed by independent memory stores. Queries include points, empty results, filtered/ordered pages, background queries and a full list. The schema has 17 tables; main rows contain 36 text fields with 256-character suffixes. Every run checks exact identities and all selected values.

The endpoint runs from staged query startup through all results. Seeding/opening is reported separately and excluded from the endpoint. Browser transport, IndexedDB, rendering and account authentication are outside this receipt. Host-owned query wakeups are explicitly activated and asserted in every sample; the benchmark still drives ticks manually.

ABBA then BAAB, four fresh endpoints per process: 16 observations per arm and row count. No observations or warmups discarded; no compiler, test or profiler ran during timing. See `host-wake-receipt.json` for provenance/summary, `host-wake-observations.jsonl` for every raw observation and `host-wake-samples.csv` for phase timings.

| Main rows | Control median | Candidate median |         Time saved |
| --------- | -------------: | ---------------: | -----------------: |
| 600       |     414.563 ms |       400.959 ms |  13.604 ms / 3.28% |
| 6,000     |   1,877.373 ms |     1,730.070 ms | 147.303 ms / 7.85% |

The two orders independently improve the 6,000-row endpoint by **4.4% and 7.8%**. There is substantial process variation: control process medians span 1,789–1,929 ms and candidate process medians 1,687–1,856 ms. The pooled median is a local estimate, not a guaranteed production speedup.

Six-thousand-row phase medians:

| Phase                       |      Control |    Candidate |
| --------------------------- | -----------: | -----------: |
| Owner ticks                 |   815.190 ms |   669.281 ms |
| Foreground ticks            |   949.118 ms |   952.309 ms |
| Subscribe                   |    75.500 ms |    75.092 ms |
| Prepare                     |     1.424 ms |     1.455 ms |
| Result extraction           |    31.453 ms |    32.533 ms |
| Seed/open, outside endpoint | 1,437.430 ms | 1,426.536 ms |

Phase medians need not sum to the median endpoint. The receiver still performs its work. Owner runtime nodes fall from 1,770 to 1,629 (141 fewer); foreground nodes and arrangement rows/encoded bytes are identical. These runtime counters are not process RSS; encoded-byte counters can include shared backing ownership. No total-memory reduction is claimed.

Control: retained integration `2f9f288ef71eaa17c21a92cd472116e1f512a363`, including the two chunk-decoding improvements and prior unmerged work, excluding #3611. The fixture is restored from #3600. Candidate adds only the plain-peer guard/comment patch. This is **not an exact-main comparison or an app startup claim**.

- Control executable SHA256: `36d2b851c9b8399ffa1e26ab4c2f2e1081459825421612af6ddc413641892c1a`.
- Candidate executable SHA256: `2eb87f232446f21f8018707f9574466949ee94c27d7429976d6e1c78faf3b76e`.
- Runtime patch SHA256: `f5f6f994e9e299110040541eed2e765f70ad774109548f9fe1f542f3b4b90a09`.
- Rust 1.93.1, `[profile.perf]`, no default features, `testing,transport-compression-zstd`.

```sh
JAZZ_FAIR_LAYOUT=mixed-local JAZZ_FAIR_ROWS=6000 \
JAZZ_FAIR_MIXED_WIDTH=256 JAZZ_FAIR_HOST_SCHEDULER=1 JAZZ_FAIR_REPEATS=4 \
cargo bench -p jazz --profile perf --no-default-features \
  --features testing,transport-compression-zstd --bench publication_fairness
```

## Initial manual-tick qualification

The earlier restored fixture did not contain the host-scheduler hook: setting `JAZZ_FAIR_HOST_SCHEDULER` had no effect. That omission was caught during review, and the complete balanced comparison above was repeated with the hook restored and an explicit output assertion. The initial observations remain in `observations.jsonl` and `samples.csv`, with distinct source/binary hashes in `receipt.json`.

That initial control was `cc5a303f1a2bfd0b56c8d36f1544464f6e23eba7` plus the restored fixture, before the leaf-codec change. Its candidate added the same guard patch. Six hundred rows: 388.118 → 367.763 ms (5.24%). Six thousand rows: 1,877.372 → 1,786.769 ms (4.83%). Both orders improved. Do not pool those observations with the host-wake receipt or describe them as host-wake measurements.

## Correctness and limits

Eligibility remains peer consumers, the active schema and the default read view. Aggregates, relations, flat joins, array subqueries, includes, joins, reachability, inheritance and explicit policy branches retain their existing path. Application and authorization-support consumers retain their collectors. Older-schema readers retain compatibility projection, including exclusion of newly introduced enum cases. Storage and wire encodings are unchanged.

The two new tests use real resident databases and public schema/query builders plus `row_input!`. They check three fresh coverage/reopening cycles, exact multibyte text and literal JSON, hidden projection fields, ordering by an unselected key with offset/limit, and live replacement/deletion. The owner-only reconstruction counter requires internal topology because public clients cannot attribute reconstruction to the serving peer. No graph or terminal state is forged. Required source facts, memberships and replacement witnesses remain; this does not eliminate all duplicated database state.

Both lifecycle tests pass on the retained integration and separate #3600-based review branch. The combined Jazz run with CI's 4 MiB test stack passes 2,259 library tests, 229 integration tests, three delivery canaries, five benchmark smoke cases and 29 doc tests. Its only assertion failure is the existing test expecting one retained peer application row. A representation-only patch awaits user approval; all delivered-row, update, delete and restore assertions are preserved. Existing ignores remain ignored. The bounded five-seed maintained/one-shot oracle at churn 10/1000 also passes.

The default-stack run aborts in existing deep catalogue and prepared-claim tests; it is not counted as passing. Full standalone/hosted validation status belongs in the PR description. This is not a CI-equivalent receipt.

Tooling-friction: retain the host scheduler and its explicit output flag when restoring native fixtures; assert that intended workload options actually activate.
