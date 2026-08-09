# R3 bounded global-sequence recovery

This receipt follows `R3_JAZZ_OPEN_ATTRIBUTION.md`. It changes startup recovery
to range-scan only non-null entries in `jazz_transactions/by_global_seq`,
instead of scanning the nullable index from its empty prefix.

The previous scan visited every transaction, including local pending and
rejected transactions whose `global_seq` is null. Recovery still filters for
accepted fate, sorts and deduplicates the recovered sequences, and rebuilds the
same contiguous watermark plus above-watermark gap set.

## Correctness boundary

An internal recovery regression covers accepted global sequences `{1, 3}`,
64 pending transactions, and one rejected transaction. Reopen must:

- scan only the two sequenced transactions;
- recover watermark `1`, above-watermark set `{3}`, and next sequence `4`;
- advance to watermark `3` when sequence `2` subsequently arrives.

The test is internal because the distinction between contiguous watermark and
above-watermark gaps is not exposed through the public `Db` API.

## Initial local receipt

Three-sample warm-cache clean-close medians on `anselm-devbox`, 2026-07-29,
using the same generated `m` workload:

| phase                         |   before |    after |                  delta |
| ----------------------------- | -------: | -------: | ---------------------: |
| Jazz open                     | 2,919 ms | 1,104 ms |                 -62.2% |
| recover global sequences      | 1,648 ms |    31 us | effectively eliminated |
| recover storage               | 1,984 ms |   340 ms |                 -82.9% |
| rebuild ahead-current indexes |   946 ms |   763 ms |                 -19.3% |
| validate current rows         |   337 ms |   340 ms |              unchanged |
| first read                    |   359 ms |   345 ms |    within run variance |

The transaction-index work counter falls from 952,620 scanned records to zero
because this fixture has no accepted global sequences. The optimization is
proportional to sequenced transactions rather than total transactions; the
gap regression demonstrates the non-zero path.

Ahead-current reconstruction is now the dominant open phase, followed by
current-row validation. Those remain separate optimization lanes.

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

Tooling friction: an opt-in reusable seeded fixture would reduce the several
minutes spent rebuilding unchanged `m` data for every recovery iteration.
