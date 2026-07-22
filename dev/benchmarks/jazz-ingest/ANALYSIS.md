# Storage benchmark: Jazz vs. native engines, and SlateDB write architectures

Ingest throughput, write amplification, and query latency for the Jazz relational
stack across storage adapters — including a local-WAL SlateDB variant that closes
most of the gap to RocksDB.

Benchmark code: `dev/benchmarks/jazz-ingest/` (crate `jazz-ingest-bench`).

## TL;DR

- **Jazz adds ~100× write time and ~15× on-disk size over the raw engine** — its
  per-cell history/register/current encoding and transaction path, not the
  storage engine.
- **The object-store WAL is what makes `jazz:slatedb` slow.** Routing SlateDB's
  WAL through `object_store` behind a sync bridge costs one blocking round-trip
  per write batch: 8.3 s ingest, ~475 ms queries.
- **`slatedb-localwal` fixes it** with a local append-only WAL + an in-memory hot
  tier: **1.7 s write (faster than RocksDB), ~217 ms queries (matching RocksDB)**,
  with the object-store cost deferred to a checkpoint at close.
- **Zstd cuts SlateDB's footprint ~4×** (125 → 30 MiB) and composes with
  `localwal`, so `slatedb-localwal` ends up **both smaller and faster to write
  than `jazz:rocksdb`**.

## What the benchmark measures

Ingests the public-domain [USDA PLANTS checklist][usda] (93,157 rows:
`symbol, synonym_symbol, scientific_name, common_name, family`) into one flat
table (five string columns, no secondary index) and measures:

1. **Write time** — insert all rows in batched transactions (1000 rows/batch).
   **write+close** adds the flush/checkpoint cost that makes the data fully
   durable in its final store.
2. **Write amplification** — physical on-disk bytes (`du`) ÷ logical bytes.
3. **Cold-load query latency** — close + reopen from disk, then time a fixed
   query set cold (first read) and warm (second).

Two altitudes: `--storage <adapter>` (through the Jazz `Db` API) and
`--raw <engine>` (the engine's native crate API, bypassing Jazz). Adapters:
`memory`, `rocksdb`, `btree`, `slatedb`, `slatedb-localwal`,
`slatedb-localwal-sync`, `sqlite`, `redb`, `postgres`.

**Topology:** a single standalone local `Db` node — no server, sync, or policies.
It isolates storage cost, not replication.

[usda]: https://plants.sc.egov.usda.gov/DocumentLibrary/Txt/plantlst.txt

## Results — Jazz adapters (93,157 rows, Apple-silicon host, Zstd on)

point/full-scan are warm; `write+close` = write + flush/checkpoint.

| adapter                      |      write | write+close | cold reopen | point (warm) | full scan |      on disk | amp vs raw |
| ---------------------------- | ---------: | ----------: | ----------: | -----------: | --------: | -----------: | ---------: |
| `jazz:rocksdb`               |     2.03 s |      2.03 s |      0.26 s |       224 ms |    214 ms |     76.0 MiB |      14.4× |
| `jazz:slatedb`               |     8.31 s |      8.31 s |      0.74 s |       479 ms |    474 ms |     30.4 MiB |      5.74× |
| **`jazz:slatedb-localwal`**  | **1.69 s** |      2.63 s |      1.14 s |   **217 ms** |    217 ms | **30.3 MiB** |      5.73× |
| `jazz:slatedb-localwal-sync` |     1.97 s |      2.90 s |      1.15 s |       216 ms |    221 ms |     30.3 MiB |      5.73× |

Raw input payload = 5.3 MiB. Raw-engine floor for reference: `raw:rocksdb`
0.016 s / 4.1 MiB, `raw:slatedb` 0.038 s / 2.9 MiB — ~100× faster than through
Jazz, because the raw layer stores one flat KV per row with none of Jazz's
history/versioning/transactions.

## How `localwal` gets faster

The two SlateDB numbers are the _same engine wired two ways_.

**`jazz:slatedb` — WAL on object storage, behind a sync bridge.** groove's
`SlateDbStorage` runs the natively-async SlateDB on a worker thread and presents a
synchronous seam (`SyncBridgeStorage`). Every write batch is a **blocking thread
round-trip**, and SlateDB appends its WAL to the `object_store` (here a local
filesystem, but treated as an object API — small immutable objects, no in-place
append). So each of the ~93 batches pays: bridge hand-off + an object-store WAL
write. That serialization is the 8.3 s.

**`slatedb-localwal` — local WAL + in-memory hot tier + deferred checkpoint.**
`LocalWalSlateDbStorage` splits the store into three parts:

- **`hot`** — a `MemoryStorage` holding the whole dataset in RAM. Every read
  (`get`, scans) is served from memory, so queries are ~217 ms (matching RocksDB)
  instead of ~475 ms.
- **`wal`** — a single local append-only file. Each write batch is serialized and
  `write_all`-appended (the `-sync` variant also `fsync`s it). No object store, no
  bridge round-trip on the write path.
- **`checkpoint`** — the object-store SlateDB, written **only at close/flush**:
  the buffered operations are applied in one `write_many` and the store is closed
  (the Zstd-compressed SSTs land here). This is the 0.94 s `write+close` tail.

So the write path collapses to _append-to-local-file + update-memory + buffer_ —
1.7 s instead of 8.3 s — and the expensive object-store I/O is batched once at the
end. This is the Neon/Aurora **disaggregated-storage** pattern: a cheap durable
local WAL on the hot path, with object-store SST materialization pushed to a
background/close checkpoint.

**Cost of the trick:** `hot` keeps the entire working set in RAM (~74 MiB encoded
here), so memory scales with the dataset; and cold reopen is slower (1.14 s vs
RocksDB 0.26 s) because it replays the checkpoint back into memory on open. On a
full-durability basis `write+close` (2.63 s) is close to RocksDB (2.03 s) — the
win is the **write hot-path latency** and the disk footprint, not raw
end-to-end throughput.

## Root-cause analysis

### Jazz overhead

Jazz stores each cell across history + register + current classes with versioning,
giving ~16× amplification (87 MiB encoded from 5.3 MiB) before compression. Writes
are CPU-bound in the mergeable-transaction path, and `db.read` **materializes the
whole relation** for any query, so every query — even a 1-row point lookup — costs
~210 ms (RocksDB) and full scans cost the same regardless of selectivity.

### Why default SlateDB queries are slow

Two tunable reasons (verified in `slatedb-0.14.1`): **async per-row iteration**
(`DbIterator::next().await` polled once per row through an LSM merge) and
**scans bypass the block cache** (`ScanOptions { cache_blocks: false }` vs.
`ReadOptions { cache_blocks: true }`). `localwal` sidesteps both by serving reads
from the in-memory `hot` tier.

### Compression

SlateDB defaults to `compression_codec: None`. Enabling Zstd on groove's
`SlateDbStorage` (one line) cut every SlateDB variant from ~125 MiB to ~30 MiB.

## Simulated EBS / safekeeper latency

`slatedb-localwal-sync` models a disaggregated durable write **in process**: it
`fsync`s the local WAL per commit, then sleeps for a configurable local-disk
("EBS") delay and a remote-ack ("safekeeper") delay before returning. The knobs:

```
--ebs-delay-ms <n>         fixed local-WAL fsync delay
--ebs-jitter-ms <n>        deterministic 0..n ms fsync jitter per sync batch
--safekeeper-delay-ms <n>  fixed remote-ack delay
--safekeeper-jitter-ms <n> deterministic 0..n ms remote-ack jitter per sync batch
```

This throttles exactly the durability path (per-commit WAL sync + remote ack)
without touching reads or the checkpoint, which is the right shape for reasoning
about a Neon-style deployment. (A cgroup `io.max` harness that throttles _real_
device I/O for any adapter also lives in `docker/ebs-run.sh`, for whole-volume
IOPS/bandwidth ceilings rather than per-commit latency.)

## Caveats & fairness notes

- **"Cold" is process-cold, not OS-cold.** Close+reopen drops in-process caches;
  the OS page cache still holds just-written data. Numbers read as warm in-memory
  query cost unless run under the throttle harness.
- **`localwal` trades memory for speed** — the `hot` tier is a full in-memory copy
  of the dataset. Its query speed is really "everything is already in RAM."
- **Jazz `db.read` materializes rows; the raw path counts** — the jazz-vs-raw
  query gap is partly API shape, not pure storage speed.
- **The raw layer is a floor, not feature parity** (one flat KV per row, no
  history/index/versioning/policies).
- **Repeatability** is tight: physical sizes byte-identical across runs, timings
  within ~2%.

## How to reproduce

```bash
# Jazz adapter matrix
cargo run --release -p jazz-ingest-bench -- \
  --storage rocksdb,slatedb,slatedb-localwal,slatedb-localwal-sync

# localwal-sync under simulated EBS + safekeeper latency
cargo run --release -p jazz-ingest-bench -- \
  --storage slatedb-localwal-sync --ebs-delay-ms 2 --safekeeper-delay-ms 5

# Also available: sqlite, redb, postgres adapters; the --raw native layer;
# and a cgroup io.max EBS throttle in docker/ebs-run.sh.
```

## Recommendations

- **Ship Zstd on groove's `SlateDbStorage`** — 4× smaller for free, composes with
  everything.
- **`slatedb-localwal` is the interesting SlateDB shape for Jazz** — it recovers
  RocksDB-class write latency and query latency while keeping SlateDB's small
  compressed footprint. The open question is the memory cost of the in-RAM hot
  tier at scale, and cold-reopen replay time.
- The default object-store-WAL `SlateDbStorage` is the wrong fit for a
  latency-sensitive local `Db`; its cost is the per-write bridge + object-store
  round-trip, not SlateDB itself.
