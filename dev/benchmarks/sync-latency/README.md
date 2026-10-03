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

`updateTopN` includes a global read followed by a global write when the global
tier is selected, so its critical path includes two serial network round trips.

Run trials sequentially without concurrent builds. Local results identify
mechanisms; end-to-end performance claims require same-base CodSpeed runs per
the repository performance rules.
