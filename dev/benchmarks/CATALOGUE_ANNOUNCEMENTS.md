# Catalogue announcement checks

`send_catalogue_snapshot_if_needed` runs before outgoing sync messages and on trusted subscriber turns. Previously it cloned the schema catalogue, serialized the complete snapshot and hashed the bytes before discovering that this peer had already received the same catalogue.

The node now remembers only that process-local fingerprint. An unchanged peer compares 32 bytes. The first check, a mutation, or a peer requiring a snapshot still uses the existing snapshot construction and validation. The fingerprint input and all wire/storage bytes remain unchanged.

All mutable catalogue access passes through a private wrapper that clears the fingerprint before returning the mutable state. This includes planning clones, permission-only changes, mapping changes and rollback. Clones have independent cache cells. Mutating derived catalogue caches can conservatively cause recomputation too. There is no retained second copy of the schema or snapshot.

## Native phase measurement

Four runs per arm in control/candidate/candidate/control/control/candidate/candidate/control order, optimized `perf` profile, same public schema builders and trusted Node/Transport boundary. Each schema has 16 text columns per table. The observer received exactly one snapshot per run.

| Tables | Repeated turns | Parent median | Candidate median |
| -----: | -------------: | ------------: | ---------------: |
|      1 |          1,000 |     16.718 ms |         1.565 ms |
|     16 |          1,000 |    205.585 ms |         1.336 ms |
|     64 |          1,000 |    810.099 ms |         1.382 ms |

The 64-table initial announcement was 0.852 / 0.837 ms; schema construction and open were 65.044 / 63.573 ms. This measures the repeated catalogue-check phase, **not application startup or general query throughput**. The browser profile that motivated the change attributed 57.7 ms of worker CPU to this function; a fresh browser comparison is required to establish an app latency gain.

Control engine: `f0df83effd1d351e43e7fb4e2840c8dacbe00dee` plus the new benchmark. Binary SHA-256: `205454b32a07943e91395f73de7ec45a0ee0a68fa723d55572ec4c99c16a4b07`. The benchmark was formatted between builds; its operations and inputs are identical. Candidate binary SHA-256: `9515f0d3e99d2d58b7a9bfe5b0438330eb300f3bc2f9ec638f5a8d11bc3d350b`. Both exact source forms and the eight observations are retained with the local receipt.

```sh
CARGO_INCREMENTAL=0 cargo bench -p jazz --bench catalogue_announcements \
  --profile perf --no-default-features \
  --features testing,transport-compression-zstd
```

`JAZZ_CATALOGUE_TURNS` changes only the number of repeated turns. The receipt separates setup, first announcement and repetition. Correctness lives in `tests/catalogue_announcements.rs`, which checks migration/permission updates through a relay, announcement state per peer, and retry after backpressure. Omitting fingerprint invalidation makes the migration test fail.

Tooling friction: the benchmark smoke script needed an explicit `-` stdin operand for BSD `paste`; using separate feature sets also rebuilt native dependency products before measurement.
