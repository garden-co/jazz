# Node sync latency probe

Runs synthetic indexed CRUD operations against an isolated local Jazz server,
using the normal authenticated sync and durability paths. No cloud credentials
are needed. The server, TCP proxy, and client are shut down after the run.

```sh
node dev/benchmarks/sync-latency/probe.mjs \
  --sdk-root /absolute/path/to/built/jazz-tools \
  --server-storage persistent --client-storage memory \
  --rtt-ms 10 --repeats 7 --output-dir /tmp/jazz-sync-results
```

The SDK root must contain `dist/` and resolve its matching NAPI dependency.
`--rtt-ms` adds half that delay in each direction over real TCP; it approximates
propagation delay, without modeling bandwidth, loss, regional routing, or cloud
server load. Both proxy sockets disable Nagle's algorithm.

Use `--tier local` to measure local durability. Cleanup still waits for global
durability before the next sample. Use `--trace` for transport timestamps,
frame sizes, and client runtime phases. Trace instrumentation can affect timing;
compare untraced runs for latency. `--pump-debounce-ms 0` is an optional
experiment that changes only the background pump scheduling delay.

Receipts contain all samples, medians, SDK/native fingerprints, and optional
trace events. Warmup and cleanup are excluded from operation medians.
Each sample deletes its 1,001 rows, retaining their history and deletion
registers until server teardown. Later samples therefore include more prior
deletions. Compare the same repeat count and inspect individual samples when
checking whether read latency grows with unrelated history.

For select-only comparisons, seed once and keep the dataset fixed throughout
the timed loop:

```sh
node dev/benchmarks/sync-latency/probe.mjs \
  --sdk-root /absolute/path/to/built/jazz-tools \
  --select-only --matching-rows 1000 --unrelated-rows 0 --deleted-rows 10000 \
  --server-storage persistent --rtt-ms 10 --repeats 35 \
  --output-dir /tmp/jazz-select-results
```

`--matching-rows` controls the selected bucket (at least 10 rows).
`--unrelated-rows` and `--deleted-rows` populate separate buckets; all seeding
and deletions settle globally before timing starts. No cleanup runs between
the timed reads. The client that seeded the rows also reads them, matching the
CRUD benchmark's warm writer/reader topology. Exact Top-N UUIDs and ordinals,
point contents, and the default UUID-ordered page are checked on every iteration.
Use `--composite-index` to add `(runId, ordinal)` alongside the two single-column
indexes. This is a separate schema experiment and is recorded in each receipt.
Use `--ordered-select` to order `select10` by ascending ordinal; `selectTopN`
always uses descending ordinal. Use `--fresh-reader` to replace the seeding
client with an empty client before measuring reads. Its connection and auth
startup are warmed by an untimed point read of the warmup row. Page rows are
initially absent locally and become cached across repetitions. Both options
are recorded in the receipt and filename.

Use `--seed-batch-size 1` to create the same dataset with one row per transaction
instead of the default 1,000-row batches. Seeding waits for each transaction's
Global durability before starting the next, outside the select timings. Receipts
record the batch size and total seed duration; this allows isolating transaction
history size without changing query shape, row count, or deletion history.

`updateTopN` includes a global read followed by a global write when the global
tier is selected, so its critical path includes two serial network round trips.

Run trials sequentially without concurrent builds. Local results identify
mechanisms; end-to-end performance claims require same-base CodSpeed runs per
the repository performance rules.

## CodSpeed global SELECT coverage

The `Node global SELECT wall time` job in `.github/workflows/codspeed.yml`
runs this same probe with `--codspeed` on the macro runner. It runs on main
pushes, `benchmark`-labeled PRs, and manual merge-suite dispatches. At each of
1,000 and 10,000 matching rows it records three separate benchmarks:

- `global_select10_<rows>_rows`: ascending ordinal, limit 10.
- `global_selectTopN_<rows>_rows`: descending ordinal, limit 10.
- `global_getById_<rows>_rows`: a primary-key read as a control.

Each size has an isolated persistent server and a fresh memory Node reader.
An authenticated writer seeds 1,000-row transactions and waits for Global
durability, then shuts down. The reader warms its connection/authentication with
an unrelated point read. A `(runId, ordinal)` composite index serves the pages;
there are no deleted rows, extra data buckets, or injected network delays.
Every timed call still uses the ordinary Global durability and sync path over
loopback WebSocket, including NAPI and JavaScript row materialization.

Tinybench measures 60 awaited calls per operation, in serial operation groups.
Seeding, startup, teardown, and exact row checks are outside the per-call timer.
There is no query warmup: the first ascending/descending page read hydrates its
rows, which remain cached for the subsequent calls. This measures repeated
Global reads on a warm connection, not a new connection or an empty reader on
every sample. The diagnostic mode without `--codspeed` retains its original
interleaved operation order.

`--codspeed` rejects incompatible topology, query, tracing, delay, and seeding
flags so these benchmark IDs cannot silently acquire different meanings. Wrong
IDs, values, counts, or ordering fail the run. The uploaded receipt retains all
latency samples and the native fingerprint. Native profiles for async Node
benchmarks may be incomplete; elapsed latency includes the awaited server work.
Tinybench sorts each operation's samples by latency; the receipt labels this
order explicitly, so its sample indices do not denote invocation order.

Reproduce one case locally with a matching built SDK:

```sh
node dev/benchmarks/sync-latency/probe.mjs \
  --sdk-root /absolute/path/to/built/jazz-tools --codspeed \
  --select-only --fresh-reader --composite-index --ordered-select \
  --server-storage persistent --client-storage memory --tier global \
  --matching-rows 10000 --deleted-rows 0 --unrelated-rows 0 \
  --seed-batch-size 1000 --rtt-ms 0 --repeats 60 \
  --output-dir /tmp/jazz-codspeed-select
```

CI builds the release NAPI client/server and normal TypeScript SDK off the macro
runner. Fast WASM is only an SDK build prerequisite and is not measured. The
archive is checked against its content hash, source commit, workflow run,
Node version, architecture, platform, and lockfile before measurement. This
release-profile CI run is distinct from the earlier local `perf`-profile trials.

New benchmark IDs need a baseline with the same harness. A first head-only
result establishes coverage; it is not evidence of a speedup over main. Compare
the same source base, native build profile, and fixture before claiming one.
