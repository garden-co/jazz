# Reuse authenticated chunk leases

## Mechanism

An owned chunk provider authenticates immutable bytes against the requested object hash before issuing a lease. Previously, evaluation retained the bytes but discarded that verification evidence; the descriptor-bound decoder hashed the same object again on each visit.

The lease now retains the exact 32-byte hash it verified. The decoder can reuse that evidence only when it matches the requested object. Logical hashing, canonical node decoding, format/kind checks, size ceilings, descriptor metrics, traversal limits and authority/coverage checks remain in place. Direct unverified installations still authenticate their bytes.

This adds 32 bytes per active lease. It adds no payload cache or retained decoded-node state and changes no storage/wire encoding or public API. Evicting cache ownership does not invalidate a live immutable lease. Replacing an evaluation input drops that lease and its proof.

## Whole-read receipt

Anonymous native `local_blob_reads`, `[profile.perf]`, two independently preloaded memory databases, account identity with eight claims, 32 schema tables, foreign-key references, and 36 settled background queries. Every timed full-row read waits for ordinary peer coverage and checks the exact returned bytes. This measures a resident local read; browser transport, IndexedDB, rendering and cold transfer are excluded.

Two balanced process orders, ABBA then BAAB; nine reads per size/process, first discarded as warmup: **32 measured reads per arm and size**. No compiler or profiler ran during timing. All raw observations, including warmups, are in `receipt.json`.

| Bytes   | Control median | Candidate median |             Time saved |
| ------- | -------------: | ---------------: | ---------------------: |
| 32 KiB  |       3.545 ms |         3.519 ms | 0.7% (no useful claim) |
| 256 KiB |       4.570 ms |         4.485 ms | 1.9% (no useful claim) |
| 4 MiB   |      20.002 ms |        14.847 ms |      **25.8% / 1.35x** |
| 16 MiB  |     110.934 ms |        90.859 ms |      **18.1% / 1.22x** |

Both process orders independently improved 4 MiB and 16 MiB. The small cases changed sign between rounds; do not claim a benefit for those. The 4 MiB read-poll and owner-tick phases both improve. Full materializations stay at two; this removes redundant authentication rather than eliminating result work.

The control is the retained integration `7a7bac7f2ece4a952f0b54d2b3a5104ac5ae3aa0`, which includes earlier unmerged performance fixes and excludes #3611. The candidate adds only the three-file runtime proof patch. Tests and formatting were added after freezing the measured executable. This is **not an exact-main comparison or an application startup claim**; the standalone PR's hosted/main and browser gates remain pending.

- Control executable SHA256: `0f9fdccf352ced376e20fecff3692c92ce184b0e0a326e367e6ccedcefa9a0a7`.
- Candidate executable SHA256: `39c50d6589445783153d373ccb240500f3b9a56dac7277a4520ccd7726f90f02`.
- Measured runtime patch SHA256: `616e2caf619e653dc074f84cc5b150f8945cbf021aa73255cb9bd405784d6667`.
- Features: `testing,transport-compression-zstd`, no default features; Rust 1.93.1.

```sh
JAZZ_BLOB_COVERAGE=1 JAZZ_BLOB_REFERENCE=1 JAZZ_BLOB_ACCOUNT=1 \
JAZZ_BLOB_SCHEMA_TABLES=32 JAZZ_BLOB_BACKGROUND=36 \
JAZZ_BLOB_BYTES=32768,262144,4194304,16777216 JAZZ_BLOB_REPEATS=9 \
cargo bench -p jazz --profile perf --no-default-features \
  --features testing,transport-compression-zstd --bench local_blob_reads
```

For attribution-only protocol receipts, add `JAZZ_BLOB_PROTOCOL_TRACE=1`; never pool these with timing results.

## Correctness and tradeoffs

- Valid bytes: cold provider verification occurs once; cache hits and later decoder visits reuse only the object-hash proof. String and JSON chunks use the same path as byte chunks.
- Wrong object: installing a valid lease under another object's request cannot reuse its proof; ordinary authentication rejects the mismatch.
- Replacement: replacing a lease with raw input drops the proof; corrupted bytes reject, valid bytes are hashed again.
- Descriptor mismatch: a verified object with the wrong descriptor format, semantic kind or logical hash still fails.
- Malformed object: a hash-valid truncated node still fails canonical decoding.
- Coalescing/eviction: concurrent cold readers get independent verified leases; zero cache budget still releases all active leases and does not retain payload ownership.

The internal test counter is needed because public results cannot expose redundant hashing or construct a misinstalled private lease. It is absent from production. Existing public query, chunk-provider and streaming suites remain unchanged.

Retained integration validation: 870 Groove unit tests, 209 integration tests, 33 additional tests and doc tests pass; four existing ignored tests remain ignored. Five new tests specifically cover the proof boundary. Full standalone CI and hosted performance are pending; this receipt is not CI-equivalent.

Tooling-friction: frozen native executables and per-phase receipts allow balanced comparisons without rebuilding bindings or reinstalling the app for each trial.
