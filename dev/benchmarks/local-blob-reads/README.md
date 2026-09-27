# Cached peer blob reads

## Work removed

A foreground read with ordinary coverage opens a maintained publication on its
storage owner. For a plain blob row on the current write schema, that publication
needs membership and source/version facts. The receiver builds the application
row. Previously, the owner also installed an application collector whose result
was discarded, reconstructing the same large value once more.

The candidate preserves normal query planning, then omits that unused execution
terminal. Application terminals, metadata-only queries, older schema readers,
strict authority relay inputs, non-default read views, and compound queries keep
their existing paths. The query still obtains fresh owner coverage. Updates,
deletes, implicit references, and exact bytes remain covered by tests.

## Native receipt

Control source: `552be68a526f4ed59d949d338989b56347db0579`.
The benchmark source is identical in all arms (SHA-256
`76fc25eedaa6352b1e332f2b2ab263d68d421914f90a70f070c88d6c3fc470b1`).
The production source delta is limited to the maintained peer terminal selection
in `node/query_eval.rs` and its application caller in `maintained_views.rs`.

Synthetic independent preloaded memory stores, one asset with a foreign key,
ordinary Local/Full coverage, and six derived SYSTEM binding claims. Each read
opens and releases a fresh attachment. This is not signed-in policy evaluation,
browser transport, IndexedDB, a cold file transfer, or image/PDF rendering.

Four processes per arm and size, eight reads per process, balanced order
`A B C C B A C A B B A C`. A is control, B removes the collector before lowering,
C removes it after lowering. Discard only each process's first read; report the
median of the remaining 28 reads. No builds or test suites ran during timing.

| Blob    |    Control |  Candidate | Time reduction | Speedup |
| ------- | ---------: | ---------: | -------------: | ------: |
| 32 KiB  |   2.921 ms |   2.641 ms |           9.6% |   1.11x |
| 256 KiB |   4.991 ms |   4.259 ms |          14.7% |   1.17x |
| 4 MiB   |  34.700 ms |  26.616 ms |          23.3% |   1.30x |
| 16 MiB  | 171.544 ms | 138.640 ms |          19.2% |   1.24x |

For indirect blobs, full reconstructions fall from four to three. The owner
phase accounts for the reduction: at 4 MiB its median falls from 25.931 to
17.602 ms; foreground read polling stays about 8.6 ms. Message counts are
unchanged, and every run verifies exact bytes. At 32 KiB the bytes are inline,
so the reconstruction counter remains zero in both arms.

Reproduce a process with:

```sh
JAZZ_BLOB_COVERAGE=1 JAZZ_BLOB_REFERENCE=1 JAZZ_BLOB_CLAIMS=1 \
JAZZ_BLOB_BYTES=4194304 JAZZ_BLOB_REPEATS=8 \
cargo bench -p jazz --no-default-features \
  --features testing,transport-compression-zstd --profile perf \
  --bench local_blob_reads
```

Use separately preserved executables for an interleaved comparison; compile them
before timing. The harness emits elapsed time, owner ticks, foreground ticks,
read polling, full reconstructions, and message counts on every read.

## Why pruning happens after lowering

Removing `request.output.app_rows` before compilation prevented reuse of the
already admitted program. It slowed 32 KiB reads from 2.921 to 3.339 ms in the
same comparison. A separate instrumented two-read run reported four compilations
for control, six for early pruning, and four for late pruning. Instrumented
timings were excluded. Preserving semantic lowering also avoids changing the
compiler's demand analysis.

An earlier broader omission broke older enum projection. The final guard retains
the original path for older schemas, with a new live catalogue-evolution test.
An additional canonical policy-key carry trial gave no useful incremental native
endpoint gain in this SYSTEM-only fixture and was excluded. A later account-bound
fixture motivated the separate [immutable claims experiment](CLAIMS.md).

## Correctness and remaining qualification

- Library: 2,255 passed, four ignored, with `RUST_MIN_STACK=4194304`.
- All three incremental delivery canaries passed.
- Differential oracle: five seeds, churn depths 10 and 1,000, passed.
- Affected benchmark compile passed.
- New tests cover repeated fresh coverage, exact bytes, a referenced row, live
  blob replacement, delete delivery, and old-reader enum compatibility after
  catalogue activation. Existing tests were not rewritten.

The raw default-stack library invocation hits the separately tracked stack issue
#3331. The above is an iteration receipt, not full canonical or CI-equivalent
qualification. A matched browser/application comparison found no repeatable improvement for
small image/PDF reads. The native large-blob numbers do not establish an
application image or PDF loading improvement.

Tooling friction: preserving native executables and phase-level receipts made
the compiler-reuse regression visible before paying for another WASM build.
