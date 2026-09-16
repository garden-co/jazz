# Borrowed collection and sort fields

Implementation `6a3554f3ee73cffaa178375d76a0898a7c1642dd`, on #2807.
Nine collection/sort sites previously decoded complete records into owned Value
vectors before selecting and cloning a few fields. They now borrow the record
and decode only selected fields. Empty sort-key lists perform no field decoding.
Nullable unwrapping, ordering, multiplicity and encoded outputs are preserved.

## Native permissioned fixture

Same 39-subscription anonymized member fixture, 27,518 visible rows, preseeded
Core and empty relay/client. Optimized build with phase attribution, no concurrent
build/test during timings. Seed and final verification queries are excluded from
settling. Both implementation and parent measurements use clean committed trees.

| Measure                       |  Parent | This slice |
| ----------------------------- | ------: | ---------: |
| Settling                      | 27.394s |    25.693s |
| Dominant subscription ready   | 27.882s |    26.181s |
| Collection/results, exclusive |  3.032s |     1.661s |
| IVM updates, exclusive        |  3.190s |     2.819s |
| Storage apply, exclusive      |  3.021s |     2.993s |
| Ingestion, exclusive          |  2.562s |     2.597s |
| Output decoding, exclusive    |  2.516s |     2.496s |

About 6.2% less settling time. The targeted phase reduction (1.74s) accounts for
the observed overall reduction (1.70s). Single-run comparisons, not confidence
intervals. The 5s target remains unmet.

## Native todo checkpoint

1,500 rows, exactly 1,350 batch updates; RocksDB worker, memory foreground.
The previous measured tip was #2804, so this comparison also includes #2806 and
#2807. It does not isolate the collection change. No browser/IndexedDB, JS,
real network or external auth is included.

| Measure                               |    #2804 | Current stack |
| ------------------------------------- | -------: | ------------: |
| Measured batch-update roundtrip       | 378.65ms |      364.47ms |
| Batch authoring                       |  37.06ms |       36.90ms |
| Initial publication                   | 142.29ms |      116.68ms |
| Initial receiver ingestion            |  67.89ms |       54.19ms |
| Initial receiver query                |  14.97ms |       13.76ms |
| Fresh receiver ingestion after update |  61.71ms |       64.09ms |

The previous batch repeats ranged 371–387ms; treat small differences cautiously.
All exact visible-result checks pass.

Validation: Groove library 743 passed, 2 ignored; all three scaling canaries and
two-seed maintained/one-shot oracle at depths 10 and 1,000 passed. Scoped Clippy
and formatting pass. Full canonical gates, browser acceptance and independent
review are not claimed.

Tooling friction: native optimized rebuilds still take about 1–2 minutes when
Jazz/Groove changes. Keep timing runs isolated from those builds.
