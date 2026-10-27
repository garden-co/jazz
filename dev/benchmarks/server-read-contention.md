# Server read contention

`crates/jazz-native-transport/examples/server_read_contention.rs` measures
independent requests to one file-backed Jazz server. It is a local diagnostic
for server scheduling, with authenticated public clients and real WebSockets.
The engine, permissions, sync, write acknowledgements and storage settings are
the normal implementation. No reader-pool implementation is included.

The existing example CodSpeed cases measure operation cost. Some have many
subscriptions in flight, but their engine fixtures execute on one thread. The
`jazz-sim` scheduler explicitly ticks nodes in declaration order, and the browser
B2 read scenario awaits each request. Those cases do not isolate interference
between an expensive server query and unrelated concurrent requests.

## Run

Correctness-only smoke (small fixture, no useful performance numbers):

```sh
cargo run --locked -p jazz-native-transport --example server_read_contention -- --smoke
```

Build once, then run with no compilation or profiling running alongside it:

```sh
cargo build --locked --release -p jazz-native-transport --example server_read_contention
target/release/examples/server_read_contention --revision <source-label> > receipt.jsonl
```

Options: `--rows`, `--payload-bytes` (128..2048), `--readers`, `--requests`,
`--background`, `--rounds`, and `--revision`. Defaults are 16,000 fixture rows,
2,048 payload bytes, four point readers, one writer, one background client,
40 requests per foreground client and four rounds. Reader concurrency is
bounded by `--readers`; each client has at most one request in flight.

The two arms run in ABBA order: idle, contended, contended, idle, then repeat.
Both arms have simultaneous point readers and a writer. The contended arm
also continuously issues a text `contains` filter over synthetic inline text.
A long matching prefix ending in an absent character makes SQLite do real
work; the expected result is empty and bounded by `LIMIT 1`. Each invocation
has a distinct binding so an existing live result cannot answer it. There is
no sleep, injected SQL function, disabled permission check or runtime hook.
Increase `--rows` to examine longer queries; query duration is measured, not
assumed to be 500 ms.

## Measurement and checks

Each arm opens fresh in-memory clients against the same durable WAL server.
Fixture construction uses the public `Db` API with a fixed row-id seed and
finishes before the server opens the file. Account registration, connection,
point-read warm-up and an isolated background-query sample are outside timing.
Foreground timing ends before client shutdown and before waiting for the last
background request to finish.

Receipts contain raw latency samples and p50/p95/p99/max for remote point reads,
globally acknowledged writes, and background reads. Request timing includes
client work, transport, server queueing, execution and the return path; it does
not separately attribute SQL execution time. Foreground throughput excludes
background work. Background samples include the final request that can finish
after the foreground interval.

Every point response must contain the exact seeded row and value. Every
background response must be empty; a positive query first proves that its
client can read the fixture. Each measured insert waits for `GlobalServer`,
then an independent verifier checks every written value exactly once.
A failed request, timeout or oracle makes the process fail. Discard receipts
without the final `oracles: passed` record. Debug builds refuse timing runs
unless `--smoke` is supplied.

For comparisons, retain the runtime base revision, benchmark-source SHA-256,
binary SHA-256, toolchain, allocator, machine, command and raw output. Use the
identical harness and parameters on both implementations and vary reader
counts (for example 1, 4 and 8) and background counts. Local contention receipts
establish a baseline; an end-to-end performance-win claim also needs same-base
CodSpeed runs on a `benchmark`-labelled PR, per the repository rules.

## Baseline on main, 2026-10-27

Runtime base `5b2fc14d17a226c00a88c7824f04466d6eedf160`, release build, system
allocator, Apple M1 Max (10 logical CPUs), macOS 27.0.1, Rust 1.93.1.
Only this diagnostic example was added; runtime sources were unchanged.
Default parameters, with the reader count varied, produced:

| Readers | Remote read p99, idle | Remote read p99, contended | Global write p99, idle | Global write p99, contended |
| ------- | --------------------- | -------------------------- | ---------------------- | --------------------------- |
| 1       | 2.07 ms               | 163.72 ms                  | 2.16 ms                | 164.05 ms                   |
| 4       | 2.55 ms               | 164.02 ms                  | 2.35 ms                | 164.18 ms                   |
| 8       | 2.76 ms               | 155.55 ms                  | 3.00 ms                | 155.48 ms                   |

Percentiles combine raw samples from both phases of each mode, not the
per-phase percentiles. Each mode includes 80 globally acknowledged writes and
80, 320 or 640 point reads, respectively. All 480 writes across the three
runs were independently verified. The isolated expensive requests took
approximately 142–153 ms; these are request durations, not isolated SQL times.

The result establishes that this workload delays otherwise cheap reads and
writes on current main. It does not measure a reader-pool implementation or
establish the benefit of one. It is a fixed-concurrency local diagnostic,
not an open-loop load test or a hosted CodSpeed performance comparison.

Raw samples and source/binary hashes are retained in
[the baseline receipts](receipts/server-read-contention-20261027/provenance.json),
alongside `readers-1.jsonl`, `readers-4.jsonl`, and `readers-8.jsonl`.
