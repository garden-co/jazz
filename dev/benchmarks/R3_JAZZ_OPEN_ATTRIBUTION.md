# R3 Jazz open attribution

This receipt drills into the `jazz_open` phase established by
`R3_PERSISTED_READ_RECEIPT.md`. It is local characterization, not an acceptance
target or an optimization.

The diagnostic API is available only through Jazz's `testing` feature. The
ordinary `rocksdb` benchmark path remains unchanged and does not collect
internal timings.

## Initial local receipt

Warm-cache `m` profile on `anselm-devbox`, 2026-07-29. Values are three-sample
medians over one shared generated workload.

| phase | clean close | unclean drop |
| --- | ---: | ---: |
| recover storage | 1,984 ms | 2,082 ms |
| rebuild ahead-current indexes | 946 ms | 938 ms |
| recover known-state facts | <1 ms | 28 ms |
| all other measured open work | <1 ms | <1 ms |
| **Jazz open** | **2,919 ms** | **3,045 ms** |

`recover_storage` breaks down as:

| recovery child | clean close | unclean drop | observed work |
| --- | ---: | ---: | --- |
| recover global sequences | 1,648 ms | 1,507 ms | 952,620 transaction-index records scanned; zero accepted global sequences recovered |
| validate current rows | 337 ms | 338 ms | 952,620 global/ahead current rows decoded |
| unclean-close cleanup | 0 ms | 221 ms | scoped crash recovery |
| pending/rejected recovery | 2 ms | 2 ms | no dominant contribution |
| catalogue/clock recovery | <1 ms | <1 ms | no dominant contribution |

Ahead-current index reconstruction then scans and inserts 952,620 entries
again. Startup therefore performs multiple whole-dataset passes before the
first read. The leading optimization question is whether global-sequence
recovery can seek directly to persisted watermark metadata (or otherwise avoid
scanning transactions that cannot contribute a global sequence). The clean
lifecycle still spends about 2.9 seconds in Jazz open, so crash recovery is not
the primary problem. The second question is whether current-row validation and
ahead-current reconstruction can share one pass or recover a persisted
index/checkpoint.

Clean-close and unclean-drop modes use the same node identity and stored
fixture. Lifecycle setup and `Db::close` happen outside the measured window.
The clean marker is therefore valid for every clean sample, while every
unclean sample deliberately enters crash recovery.

## Command

```sh
JAZZ_R3_PROFILES=m \
JAZZ_R3_CACHE_MODES=warm \
JAZZ_R3_CLOSE_MODES=clean,unclean \
JAZZ_R3_PHASE_SAMPLES=3 \
JAZZ_R3_PHASE_ONLY=1 \
  cargo bench -p jazz --features r3-open-attribution \
    --bench realistic_phase1 -- realistic_phase1/r3_rocksdb_cold_load
```

Tooling friction: retaining an opt-in seeded fixture directory would avoid
repeating several minutes of public-API fixture construction for attribution
changes that do not alter stored data.
