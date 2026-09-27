# Resident peer reads: omit unused ordering payloads

## Finding and change

A resident read between two Jazz databases still opens a subscription, serves a maintained snapshot, installs coverage, and evaluates the foreground result. The independent in-memory reproduction rules out IndexedDB and browser transport as necessary causes of this cost.

For a 4 MiB cached file with a reference, the read sends RegisterShape (87 bytes), Subscribe (72 bytes, no known-state declaration), and ViewUpdate (3,988 bytes). Cleanup sends Unsubscribe (52 bytes). Both stores already contain every chunk. The small message still triggers three full blob reconstructions: auxiliary ordering snapshot, maintained version witness, and public foreground result. Counts and sizes are canonical Postcard semantic payloads, excluding framing/compression. The timing path passes SyncMessage objects directly and does not serialize/deserialize them.

`order_terminal_snapshot` consumes only the first field of its auxiliary ordering rows. Hydration now materializes only that field, subject to the existing root-value policy. If that node is also a published sink, its normal materialization policy wins. Operator input/memo state, sort evaluation, public results, permissions, source closure, wire/storage encodings and incremental maintenance are unchanged.

This applies to both disposable first results and retained subscription openings. It does not remove a replica or substitute weaker freshness. It does not yet remove full version-witness reconstruction or reuse payload knowledge across subscriptions.

## Controlled native integration comparison

Integration parent: `6b14a7ed5`; only the runtime change differs between frozen executables. Both executables use the same extended benchmark harness with tracing **disabled**. This parent includes other performance work and the browser experiment, but the reproduction explicitly instantiates two independent native databases; no browser binding participates. These are integration measurements, not a main-base or exact review-base claim.

Fixture: two preloaded MemoryStorage databases, ordinary Local/Full read coverage, 32 schema tables, 36 settled background subscriptions, eight identity claims, file-to-folder reference, exact returned bytes asserted. Release-like `perf` profile, Rust 1.93.1, incremental compilation disabled. No concurrent builds/tests during timing.

Run order is control/candidate/candidate/control, 16 reads of each size per process. Discard repetition zero from each size/process, leaving 30 warm observations per arm/size. Aggregate the two process medians. Raw records and executable SHA-256 hashes are in `receipts/ordering-hydration/`. These are descriptive local measurements, not confidence intervals.

| Blob size     |   Control | Candidate | Speedup |      Latency reduction |
| ------------- | --------: | --------: | ------: | ---------------------: |
| 32 KiB inline |   3.55 ms |   3.46 ms |   1.02× | 2.4%, effectively flat |
| 256 KiB       |   5.24 ms |   4.63 ms |   1.13× |                  11.6% |
| 4 MiB         |  26.96 ms |  19.99 ms |   1.35× |                  25.8% |
| 16 MiB        | 139.64 ms | 111.35 ms |   1.25× |                  20.3% |

For 4 MiB, owner tick time falls from 18.10 to 11.07 ms; foreground read polling remains 8.70 vs 8.71 ms. For 16 MiB, owner time falls from 85.14 to 56.77 ms while foreground read polling remains 54.30 vs 54.31 ms. Individual phase medians need not sum to the median total.

Every indirect-blob case reconstructs the full value **three times before and twice after**. Both arms use three timed messages and three turns. Separate trace runs verify identical message variants, sizes, known-state declarations and membership summaries. Trace-mode records are explicitly marked attribution-only and excluded from timing.

The earlier 28.5 → 20.0 ms first comparison is superseded by this matched reverse-order measurement. Browser UI, Core service latency, cold transfer and first-use/startup are not measured here.

## Correctness and edge cases

New public GraphBuilder/Database regressions check:

- An ordered id-only projection returns while omitted blob chunks are unavailable, with no request for those chunks, for both subscription lifetimes.
- An ordered payload projection returns exact bytes in exact order and reconstructs each published payload once.
- A shared node used as both an ordering helper and a public wide output still waits for required chunks and returns its full payload.
- A large logical ordering identity is still fetched and materialized.

The first two tests fail against the unchanged implementation: unnecessary chunk suspension, and four instead of two reconstructions for two returned rows. No existing test is rewritten. The new regressions plus existing large-value, TopBy identity and root-rank suites pass: 58 tests on the integration candidate.

A diagnostic demanding a physical indirect first-field key failed on the control with `UnsupportedJoinKey`; it was an invalid assumption about current support, not an optimization regression. The patch respects existing physical-root policy. Broader key support requires separate qualification.

## Reproduction

Build each arm independently and freeze its executable before alternating runs:

```sh
CARGO_INCREMENTAL=0 cargo +1.93.1 bench -p jazz --no-default-features \
  --features testing,transport-compression-zstd --profile perf \
  --bench local_blob_reads --no-run
JAZZ_BLOB_COVERAGE=1 JAZZ_BLOB_REFERENCE=1 JAZZ_BLOB_ACCOUNT=1 \
  JAZZ_BLOB_SCHEMA_TABLES=32 JAZZ_BLOB_BACKGROUND=36 \
  JAZZ_BLOB_BYTES=32768,262144,4194304,16777216 JAZZ_BLOB_REPEATS=16 \
  path/to/frozen-local_blob_reads
```

Set `JAZZ_BLOB_PROTOCOL_TRACE=1` only for a separate attribution run. This adds serializer size counting and JSON construction inside send; do not compare its elapsed times.

Focused checks are not the full canonical gate or CI-equivalent validation. Exact review-head qualification and hosted checks are reported on the draft PR. Remaining work belongs in GitHub Issues; this report is an experiment receipt.

Tooling-friction: permanent per-root materialization attribution would avoid temporary tracing and repeated full perf relinks; the new protocol trace makes message/byte attribution repeatable.
