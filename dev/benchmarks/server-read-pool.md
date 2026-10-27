# Native WAL reader comparison, 2026-10-27

This local diagnostic compares the serial server with two native WAL readers
for subscription openings. One expensive opening leaves the owner available
for writes and another worker available for fresh point queries. Two expensive
openings occupy the pool and queue point queries, while writes still progress.
Dispatching even cheap openings adds overhead in the idle arm.

These are developer-machine request latencies, not a hosted CodSpeed result
or evidence of a general application throughput improvement.

## Source and method

The control runtime is `afb7e3d3a471c831d989a91364fe164af6407c56`, the
benchmark-only commit on main `5b2fc14d17a226c00a88c7824f04466d6eedf160`.
Its source was exported with `git archive`; only the benchmark example was
replaced with the candidate's identical harness. The candidate runtime and
harness are `4e045b4186fc95f6e42539e9ef3cd2409fe2c5ca`.

The harness SHA-256 is
`9526e892f1d216eb478d6bc01fcbb7c90b5dcae0c05bae48fe3ea6867e942794`.
Every point query uses the same indexed fixture id and a distinct, true bound
on its known value. That distinct binding prevents both warm-up reuse and
sharing a live result between readers: each point invocation needs fresh SQL.
The earlier [baseline receipts](server-read-contention.md) used a repeated
point binding and established owner interference; the comparisons here rebuild
both runtimes with the stronger harness.

Both binaries use release optimization, the system allocator and Rust 1.93.1
on an Apple M1 Max with 10 logical CPUs, arm64, macOS 27.0.1. Other desktop
applications remained active. No builds or test suites ran during timing.
Each run seeds 16,000 synthetic rows with 2,048-byte payloads, then measures
authenticated public clients over real WebSockets against the normal durable
WAL/FULL server. Policies, sync and global write acknowledgements are enabled.

For each reader count the process order is control, candidate, candidate,
control. Every process internally runs idle, contended, contended, idle. The
single-background runs use 40 requests per foreground client per phase; the
two-background boundary check uses 12. Percentiles use nearest rank over the
combined raw samples, without averaging phase percentiles.

## One expensive query

The contended arm adds one continuously requesting expensive client to the
point readers and one writer present in both arms. Each implementation and
reader count contributes 160 global writes per mode, plus 160, 640 or 1,280
point reads per mode. Expensive isolated request durations were approximately
140 ms; they include transport and scheduling, not just SQL execution.

| Point readers | Read p99 before | Read p99 after | Global write p99 before | Global write p99 after |
| ------------- | --------------- | -------------- | ----------------------- | ---------------------- |
| 1             | 148.27 ms       | 4.13 ms        | 148.27 ms               | 3.53 ms                |
| 4             | 146.71 ms       | 3.43 ms        | 146.52 ms               | 3.06 ms                |
| 8             | 151.36 ms       | 4.59 ms        | 151.17 ms               | 3.72 ms                |

The idle arm exposes the dispatch cost:

| Point readers | Read p50 before / after | Read p99 before / after | Global write p99 before / after | Foreground ops/s before / after |
| ------------- | ----------------------- | ----------------------- | ------------------------------- | ------------------------------- |
| 1             | 1.14 / 1.31 ms          | 1.87 / 2.26 ms          | 2.09 / 2.30 ms                  | 1,670 / 1,467                   |
| 4             | 1.68 / 1.83 ms          | 2.48 / 3.30 ms          | 2.13 / 2.65 ms                  | 2,926 / 2,606                   |
| 8             | 2.30 / 2.66 ms          | 3.49 / 5.36 ms          | 2.98 / 5.07 ms                  | 3,845 / 3,187                   |

Foreground throughput counts point reads and global writes only. It excludes
the background requests and cannot establish overall server throughput.

## Both workers occupied

With four point readers and two distinct expensive queries, contended point-read
p99 changes from **286.08 ms to 153.08 ms**, while global-write p99 changes from
**286.01 ms to 3.70 ms**. Each implementation contributes 192 point reads and
48 global writes per mode. The pool bounds concurrency; it does not reserve a
worker for a cheap query or provide unlimited readers.

## Reproduction and limits

Build the control runtime with the identical harness copied into its source
export, and build the candidate. Use separate Cargo target directories or
clean affected release packages between source trees; preserve each executable
under its own filename before building the other. Run binaries directly with
the same arguments, without compilation or profiling alongside them:

```sh
CONTROL --rows 16000 --payload-bytes 2048 --readers 4 --requests 40 --background 1 --rounds 4 --revision control
CANDIDATE --rows 16000 --payload-bytes 2048 --readers 4 --requests 40 --background 1 --rounds 4 --revision candidate
```

Repeat in reverse process order, sweep `--readers 1` and `--readers 8`, and
check the two-background boundary with `--background 2 --requests 12`.

The [provenance and raw receipts](receipts/server-read-pool-20261027/provenance.json)
retain executable hashes, source revisions, parameters, process order,
all latency samples and aggregate summaries. All 16 processes passed the
point-row, empty-filter and independent global-write oracles; all **2,112**
measured writes were verified exactly once. A receipt without its final
`oracles: passed` record is invalid.

This is a fixed-iteration, closed-loop workload. In the single-background
candidate arm, foreground work can finish before the first expensive request
completes; joining that final request is outside foreground timing. That arm
demonstrates short-request interference, not sustained saturation. The
two-background run and deterministic worker-barrier tests exercise the bound.
Repeated relevant writes can invalidate an opening and delay it further.

The implementation covers native file-backed authority subscription openings.
Direct local reads, transactions, nested array assembly and already-open
subscription maintenance still execute on the owner. Worker threads, snapshots
and caches add per-app resource costs; memory was not measured here. Same-base
hosted CodSpeed runs remain required before a general performance-win claim.
