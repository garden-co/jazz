# R3 persisted reopen/read receipt

`realistic_phase1/r3_rocksdb_cold_load` measures a process-cold RocksDB/Jazz
reopen followed by preparation and first materialization of a project-board
query. Fixture seeding, page-cache eviction, and result validation are outside
the measured phases.

The historical Criterion measurement remains
`realistic_phase1/r3_rocksdb_cold_load/project_board_s/<tasks>` and times the
combined reopen/prepare/read operation. The additional JSON receipt attributes
that operation to:

1. RocksDB storage open;
2. Jazz `Db` open;
3. query preparation;
4. first query materialization.

The first-materialization figure is also attributed internally to:

1. settled-view/prepared-plan resolution;
2. one-shot program compilation, when a prepared plan is unavailable;
3. executable-plan selection and query-context lookup;
4. Groove plan execution;
5. output decode and Jazz row materialization;
6. includes, ordering, offset, and limit processing;
7. output projection.

The receipt emits the facade/clock residual as `first_read_unattributed_p50_us`
so the internal phases can be checked against the existing end-to-end
`first_read_p50_us`. Ordinary reads do not collect these clocks; only the R3
diagnostic call does.

## Workload ladder

Profiles match the realistic benchmark definitions:

| profile | users | orgs | projects |   tasks | comments | watchers/task | activity |
| ------- | ----: | ---: | -------: | ------: | -------: | ------------: | -------: |
| `ci`    |     4 |    2 |        8 |     120 |      360 |             1 |      240 |
| `s`     |    10 |    3 |       30 |   3,000 |   12,000 |             1 |    9,000 |
| `m`     |   100 |   20 |      500 | 100,000 |  400,000 |             2 |  250,000 |

Each task is assigned round-robin to a project, so the selected project returns
15, 100, and 200 rows respectively. The exact result count is asserted for
every sample.

## Cache modes

- `warm`: creates a fresh RocksDB/Jazz instance over the seeded files without
  controlling the OS page cache.
- `evicted` (Linux): after closing the previous instance, opens every regular
  RocksDB file and calls `posix_fadvise(POSIX_FADV_DONTNEED)` before the sample.
  This is a scoped best-effort eviction hint, not a machine-wide page-cache
  drop. Eviction is outside the measurement window.

Both modes are process-cold. RocksDB's per-instance block cache is recreated on
every sample.

## Initial local receipt

Three-sample medians on `anselm-devbox`, 2026-07-29, Rust 1.93.1:

| profile | cache   | storage open |  Jazz open | prepare | first read | result rows |
| ------- | ------- | -----------: | ---------: | ------: | ---------: | ----------: |
| `ci`    | warm    |       8.0 ms |     3.2 ms |    7 us |     0.6 ms |          15 |
| `ci`    | evicted |       8.9 ms |     6.1 ms |    7 us |     0.6 ms |          15 |
| `s`     | warm    |       8.5 ms |    68.2 ms |   21 us |     7.6 ms |         100 |
| `s`     | evicted |      10.1 ms |    83.3 ms |   22 us |     7.5 ms |         100 |
| `m`     | warm    |      12.6 ms | 3,059.6 ms |   27 us |   355.4 ms |         200 |
| `m`     | evicted |      34.6 ms | 3,219.2 ms |   28 us |   339.9 ms |         200 |

The primary scale signal is Jazz open, not RocksDB open. First materialization
also grows much faster than result size, consistent with the existing gap around
filtered/indexed persisted reads. These figures are a local receipt, not an
acceptance target.

## Commands

Default historical Criterion benchmark plus a warm CI receipt:

```sh
cargo bench -p jazz-tools --features rocksdb --bench realistic_phase1 -- \
  realistic_phase1/r3_rocksdb_cold_load
```

Full local phase ladder:

```sh
JAZZ_R3_PROFILES=ci,s,m \
JAZZ_R3_CACHE_MODES=warm,evicted \
JAZZ_R3_PHASE_SAMPLES=3 \
JAZZ_R3_PHASE_ONLY=1 \
  cargo bench -p jazz-tools --features rocksdb --bench realistic_phase1 -- \
  realistic_phase1/r3_rocksdb_cold_load
```

The M fixture seeds through the public API and takes several minutes. It remains
opt-in; the default path is unchanged at `ci`.
