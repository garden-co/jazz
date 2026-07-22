# Storage benchmark: Jazz vs. native engines, and behavior under slow disks

An analysis of ingest throughput, write amplification, and query latency for the
Jazz relational stack versus the raw storage engines underneath it, plus how each
behaves under injected disk latency and an EBS-like throttle.

Benchmark code: `dev/benchmarks/jazz-ingest/` (crate `jazz-ingest-bench`).

## TL;DR

- **Jazz adds ~100× write time and ~15–25× on-disk size over the raw engine** on
  the same data. Almost all of it is Jazz's per-cell history/register/current
  encoding and transaction path — not the storage engine.
- **RocksDB and SlateDB are both excellent as raw KV stores** here: sub-second
  ingest of 93k rows, and on-disk footprints _smaller_ than the raw input once
  compression is on.
- **SlateDB's default scan path is ~15× slower than RocksDB's** and stays
  disk-bound under latency, for two concrete, tunable reasons (async per-row
  iteration; `ScanOptions { cache_blocks: false }`).
- **Enabling Zstd on SlateDB cut its footprint 5.4× (raw) / 4.1× (Jazz).** It is
  off by default.
- **Under a slow disk, the write path is barely affected** (buffered, no-sync
  writes) while **reads and durability barriers pay the full latency.** RocksDB's
  block cache hides read latency after warm-up; SlateDB's scans re-expose it.

## What the benchmark does

Ingests the public-domain [USDA PLANTS checklist][usda] (93,157 rows:
`symbol, synonym_symbol, scientific_name, common_name, family`) into one flat
table (five string columns, no secondary index) and measures three things:

1. **Write time** — wall-clock to insert all rows in batched transactions/write-
   batches (1000 rows/batch), plus flush/close cost.
2. **Write amplification** — physical on-disk bytes (`du`) ÷ logical bytes,
   against both the raw CSV payload (5.3 MiB) and Jazz's own encoded size.
3. **Cold-load query latency** — close + reopen from disk, then time a fixed
   query set. Each query is timed **cold** (first read), **warm** (second), and
   **hot** (best of 10 further reads).

It runs at two altitudes on the identical dataset and metrics:

- **`--storage`** (Jazz layer): through the public `jazz::db::Db` API — real
  schema, batched mergeable transactions, `db.read` queries — over a chosen
  adapter (`memory`, `rocksdb`, `btree`, `slatedb`).
- **`--raw`** (native layer): straight through each engine's own crate API
  (`rocksdb`, `slatedb`), tuned for bulk ingest, bypassing Jazz and the groove
  seam. One flat KV pair per row; key = `symbol\0synonym_symbol`, value = the
  five fields joined by `0x1F`.

The eight queries (identical row counts across all layers, so latencies are
comparable): `point_by_key`, `prefix_scan_AB`, `filter_family_Malvaceae`,
`full_scan`, `contains_scientific_Carex`, `common_name_present`, `family_in_set`,
`top_100_by_symbol`.

**Topology note:** the Jazz layer is a single standalone local `Db` node — no
server, sync, relay/client, or policies. It isolates storage cost, not
replication.

[usda]: https://plants.sc.egov.usda.gov/DocumentLibrary/Txt/plantlst.txt

## Results — write & storage (93,157 rows, Apple-silicon host)

| run             | write time |    throughput | encoded (Jazz) | physical on disk | amplification vs raw |
| --------------- | ---------: | ------------: | -------------: | ---------------: | -------------------: |
| **raw:slatedb** |    0.038 s |  2.5 M rows/s |              — |      **2.9 MiB** |            **0.55×** |
| **raw:rocksdb** |    0.016 s |  5.9 M rows/s |              — |          4.1 MiB |                0.77× |
| jazz:slatedb    |      8.2 s | 11.4 k rows/s |        ~87 MiB |         30.4 MiB |                5.74× |
| jazz:rocksdb    |      2.0 s | 47.1 k rows/s |         87 MiB |         76.0 MiB |                14.4× |

Raw input payload = 5.3 MiB. Both native engines compress _below_ the input
(highly repetitive plant taxonomy text). Jazz's encoded size is ~16× the payload
before the engine even compresses it.

## Results — query latency, cold / warm / hot (ms)

| query             |    jazz:rocksdb |    jazz:slatedb |              raw:rocksdb |             raw:slatedb |
| ----------------- | --------------: | --------------: | -----------------------: | ----------------------: |
| point_by_key      | 226 / 218 / 213 | 467 / 460 / 459 | 0.01 / 0.001 / **0.000** | 0.30 / 0.02 / **0.009** |
| prefix_scan_AB    | 216 / 217 / 214 | 467 / 462 / 462 |  0.04 / 0.02 / **0.019** | 0.29 / 0.26 / **0.238** |
| full_scan         | 211 / 212 / 209 | 465 / 470 / 455 |      9.0 / 9.0 / **8.8** |     138 / 139 / **135** |
| filter_family     | 211 / 209 / 208 | 456 / 458 / 455 |   15.4 / 12.1 / **11.6** |     140 / 139 / **138** |
| top_100_by_symbol | 287 / 286 / 285 | 536 / 542 / 531 |  0.01 / 0.01 / **0.009** | 0.16 / 0.15 / **0.144** |

Two clusters per engine: **indexed** queries (point/prefix/top-N) seek on the
ordered keyspace → microseconds native; **full-value scans** (the filter/count
queries) walk all 93k rows because there is no secondary index → flat ~12 ms
(RocksDB) / ~138 ms (SlateDB) regardless of selectivity.

## Root-cause analysis

### Jazz overhead (the ~100× write, ~15× size gap)

Jazz stores each cell across history + register + current storage classes with
versioning metadata, giving ~16× amplification (87 MiB encoded from 5.3 MiB
payload) before storage compression. Writes go through the mergeable-transaction
path, which is CPU-bound in Jazz, not disk-bound. And `db.read` **materializes
the whole relation** to answer any query, so every query — even a 1-row point
lookup — costs ~210 ms (RocksDB) / ~460 ms (SlateDB); `top_100_by_symbol` is the
worst case because it sorts-then-limits the full relation. These are the price of
Jazz's semantics (history, merge, policies, IVM), which the raw layer does not
provide.

### Why SlateDB scans are ~15× slower than RocksDB

Two independent, tunable reasons (both verified in `slatedb-0.14.1` source):

1. **Async per-row iteration.** `DbIterator::next().await` is polled once per row
   through an LSM k-way merge (`db_iter.rs`). 93k async awaits + `Bytes` clones
   vs. RocksDB's synchronous native C++ iterator. ~1.5 µs/row vs ~0.1 µs/row.
2. **Scans bypass the block cache by default.** `ScanOptions { cache_blocks:
false }` (`config.rs:373`) vs. `ReadOptions { cache_blocks: true }`
   (`:301`). Point lookups populate and hit SlateDB's 512 MiB block cache
   (fast warm/hot); range scans stream SSTs from `object_store` every time and
   never consult it. This is a sensible default for its target medium (object
   storage, huge scans) but penalizes a small dataset scanned repeatedly.

SlateDB is an LSM designed for **object storage (S3)**, where per-op latency is a
network round-trip that dwarfs this overhead. On a local, warm benchmark that
overhead is fully exposed.

### jazz:slatedb runs a sync-forced path

The two SlateDB numbers are not the same engine driven two ways. **raw:slatedb**
drives SlateDB's native async API directly. **jazz:slatedb** reaches it through
groove's `SlateDbStorage`, which wraps the natively-async engine in
`SyncBridgeStorage` — a synchronous façade that runs SlateDB on a dedicated
worker thread and _blocks the caller on every operation_. Each storage call
becomes a thread round-trip, so the async engine's concurrency is serialized and
per-op overhead is multiplied. That is why jazz:slatedb (8.2 s ingest, ~460 ms
queries) is far slower than raw:slatedb (0.04 s, ~138 ms) beyond Jazz's own
encoding cost — and why it degrades worst under I/O latency: the many small,
individually-blocking ops each pay the disk latency in series rather than
overlapping.

### Compression

SlateDB defaults to `compression_codec: None`. Enabling Zstd (one line in
groove's `SlateDbStorage`, plus the `zstd` crate feature) cut the footprint:
raw:slatedb 15.8 → 2.9 MiB (5.4×), jazz:slatedb 124.8 → 30.4 MiB (4.1×). The
benchmark's raw path and groove's adapter now both enable it.

## Behavior under an EBS-like disk (Docker, arm64 Linux)

Engine data sits on a loop-backed ext4 volume, rate-limited on **real device
I/O** by cgroup v2 `io.max` — IOPS + bandwidth caps (gp3 baseline = 3000 IOPS,
125 MiB/s) — while the page cache serves warm reads for free. This throttles
actual device I/O the way an EBS volume does, respecting the cache. (Docker
Desktop's kernel lacks `dm-delay`, so the cgroup throttle is the faithful
mechanism available.) Verified: an 8 MiB/s cap makes a 64 MiB write+fsync take
exactly 8 s. See `docker/ebs-run.sh`.

| metric                        | host (no throttle) | gp3 (3000/125) | constrained (500/30) |
| ----------------------------- | -----------------: | -------------: | -------------------: |
| raw:rocksdb · write           |            0.016 s |        0.017 s |              0.019 s |
| raw:rocksdb · flush           |            0.019 s |        0.023 s |          **0.111 s** |
| raw:slatedb · full_scan (hot) |             135 ms |         251 ms |               254 ms |
| jazz:rocksdb · cold reopen    |             0.25 s |         0.34 s |           **1.00 s** |

Both throttled runs are containerized; the throttle's effect is cleanest in the
gp3 → constrained delta (host → gp3 also carries container + loop-device
overhead).

**EBS latency lands on reads and durability barriers, not buffered writes.** Both
engines use no-sync / deferred-durability writes, so writes hit the page cache
and writeback is async — on a dataset this size the OS absorbs it, and even the
constrained tier barely moves `write` (0.016 → 0.019 s). What pays the throttle:

- **RocksDB's flush fsync** — 0.02 → 0.11 s at 500 IOPS (~6×), its one true
  device wait.
- **jazz:rocksdb cold reopen** — 0.25 → 0.34 → 1.00 s, scaling monotonically with
  the throttle as it reads SST index/blocks back off the volume.
- **SlateDB's uncached scans** — 135 → ~254 ms, directly exposed because they
  re-read from the device every time (the `cache_blocks: false` default), while
  RocksDB's cached scans (6 ms) are untouched.

Caveat: `io.max` models EBS's throughput/IOPS **ceilings**, not its ~1 ms base
per-op latency. And the write-path effect is muted only because the dataset is
small; a sustained-write workload would saturate the throughput cap.

## Methodology caveats & fairness notes

- **"Cold" is process-cold, not OS-cold.** Close+reopen drops the in-process
  cache, but the OS page cache still holds just-written data (dropping it needs
  root). So cold-load numbers read as _warm in-memory query cost_ unless run in
  the throttled/`drop_caches` container.
- **Per-query "cold" is cache-order-dependent.** The block cache persists across
  queries on one DB instance, so only the first query to touch a block range is
  truly cold; later "cold" numbers are warm-cache hits. `--cold-per-query`
  reopens before each query to give a genuine cold read per query.
- **Jazz `db.read` materializes rows; the raw path counts.** The Jazz query does
  strictly more work, so the jazz-vs-raw _query_ gap is partly API shape, not
  pure storage speed. Amplification and write-time comparisons don't have this.
- **The raw layer is a floor, not feature parity.** One flat KV per row — no
  history, versioning, secondary indexes, or policies. "Raw is 100× faster" is
  "storage engine with none of Jazz's semantics."
- **Config symmetry:** RocksDB is given an explicit 128 MiB block cache + LZ4/Zstd;
  SlateDB uses its default 512 MiB cache + (now) Zstd. Both are tuned to their
  strengths, but the two engines' cache _policies_ differ (see scan caching).
- **Repeatability** is tight: physical sizes byte-identical across runs, timings
  within ~2%, no cross-run contamination (each engine gets its own tempdir/DB).

## How to reproduce

```bash
# Local, full comparison
cargo run --release -p jazz-ingest-bench -- --storage rocksdb,slatedb --raw rocksdb,slatedb

# True cold per query (reopen before each)
cargo run --release -p jazz-ingest-bench -- --raw rocksdb,slatedb --cold-per-query

# EBS-like throttle (container, gp3 baseline)
docker build -f dev/benchmarks/jazz-ingest/docker/Dockerfile -t jazz-ingest-bench .
docker run --rm --privileged -v "$PWD/dev/benchmarks/jazz-ingest/docker/ebs-run.sh:/ebs-run.sh" \
  -e EBS_IOPS=3000 -e EBS_BPS=131072000 jazz-ingest-bench 'sh /ebs-run.sh --raw rocksdb,slatedb'

# Tighter tier (constrained volume)
docker run --rm --privileged -v "$PWD/dev/benchmarks/jazz-ingest/docker/ebs-run.sh:/ebs-run.sh" \
  -e EBS_IOPS=500 -e EBS_BPS=31457280 jazz-ingest-bench 'sh /ebs-run.sh --raw rocksdb,slatedb'
```

The Docker build caps parallelism (`-j 2`) so the memory-heavy jazz crate fits
the VM's RAM.

## Recommendations / open questions

- **Enable Zstd on groove's `SlateDbStorage`** (done in this branch) — 4×
  smaller for free.
- **If SlateDB scans matter, set `ScanOptions { cache_blocks: true }`** for
  repeated scans over a working set that fits the cache; expect scan latency to
  drop toward RocksDB's once warm.
- **jazz:slatedb through `SyncBridgeStorage` issues many small write syscalls** —
  worth profiling the bridge's write batching before considering SlateDB for
  latency-sensitive (EBS/network) storage.
- Jazz's ~16× encoding amplification and full-relation `db.read` materialization
  are the dominant costs vs. the raw engines; both are inherent to current Jazz
  semantics, not the storage layer.
