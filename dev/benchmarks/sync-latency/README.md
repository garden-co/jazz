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
`updateTopN` includes a global read followed by a global write when the global
tier is selected, so its critical path includes two serial network round trips.

Run trials sequentially without concurrent builds. Local results identify
mechanisms; end-to-end performance claims require same-base CodSpeed runs per
the repository performance rules.
