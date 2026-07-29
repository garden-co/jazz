# R3 fused ahead-current recovery

This receipt follows `R3_BOUNDED_GLOBAL_SEQ_RECOVERY.md`. It removes a duplicate
startup pass over every ahead-current row and bulk-reconstructs the in-memory
ahead-current indexes from the existing layout-validation scan.

Previously, recovery decoded every global-current and ahead-current row to
validate its stored layout. Startup then scanned every ahead-current table
again, decoded the row keys again, and inserted 952,620 entries one at a time
into three B-tree indexes.

Recovery now:

1. validates each ahead-current row;
2. retains its table, layer, row, time, and node key;
3. sorts and deduplicates those keys once;
4. bulk-constructs the full-key, touched-row, and latest-key indexes.

The indexes are complete before unclean-close cleanup runs, preserving the
existing cleanup ordering.

## Correctness boundary

An internal recovery regression persists two pending versions of one row and
reopens the node. It verifies that:

- the receipt counts both ahead-current entries;
- the newer version is selected after reopen;
- rejecting the newer version exposes the older pending version.

The fallback assertion exercises both the recovered full-key set and the
per-row latest-key index.

## Initial local receipt

Three-sample warm-cache clean-close medians on `anselm-devbox`, 2026-07-29,
using the same generated `m` workload:

| phase                              |    #1176 | fused + bulk |               delta |
| ---------------------------------- | -------: | -----------: | ------------------: |
| Jazz open                          | 1,104 ms |       832 ms |              -24.6% |
| validate and rebuild current state | 1,103 ms |       831 ms |              -24.7% |
| standalone ahead-current rebuild   |   763 ms |            0 |          eliminated |
| first read                         |   345 ms |       306 ms | within run variance |
| total reopen plus first read       | 1,459 ms |     1,149 ms |              -21.2% |

The fused-only intermediate measured 967 ms for Jazz open. Bulk reconstruction
then removed another 135 ms by avoiding three ordered-tree mutations per row.
Both results consumed the same 952,620 ahead-current entries.

This optimization still performs work proportional to ahead-current entries
and temporarily retains their decoded keys while constructing the indexes.
Persisting or lazily materializing those indexes remains a separate design
option if startup must avoid the full pass entirely.

## Command

```sh
JAZZ_R3_PROFILES=m \
JAZZ_R3_CACHE_MODES=warm \
JAZZ_R3_CLOSE_MODES=clean \
JAZZ_R3_PHASE_SAMPLES=3 \
JAZZ_R3_PHASE_ONLY=1 \
  cargo bench -p jazz-tools --features r3-open-attribution \
    --bench realistic_phase1 -- realistic_phase1/r3_rocksdb_cold_load
```
