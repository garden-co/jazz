# Jazz ingestion / cold-load storage benchmark

A single-dataset ingestion/cold-load benchmark with **two layers** you can run
side by side on identical data and identical metrics:

- **`--storage`** — through the **public `jazz::db::Db` API** (real schema,
  transactions, queries) over a Jazz storage adapter. Measures _Jazz plus the
  selected storage_: the transaction/encoding path and the footprint Jazz
  actually produces.
- **`--raw`** — straight through a storage engine's **own native crate API**
  (no Jazz, no groove seam), tuned for bulk ingest. Measures the engine itself.

Running both answers "how much of the write time / amplification / query latency
is Jazz versus the storage engine underneath?" Unlike `dev/benchmarks/storage/`
(synthetic op-streams behind the `BenchEngine` contract), this bench ingests a
real dataset end to end.

## Dataset

`data/plantlst.txt` — the public-domain [USDA PLANTS checklist][usda] (~93k
rows): `Symbol, Synonym Symbol, Scientific Name with Author, Common Name,
Family`. It is ingested into one `plants` table with five string columns. The
`(symbol, synonym_symbol)` pair is unique in the data and backs the point-lookup
query; `family` (550 distinct values) backs the filter query.

[usda]: https://plants.sc.egov.usda.gov/DocumentLibrary/Txt/plantlst.txt

## What it measures

1. **Write time** — wall-clock to insert every record in batched transactions,
   plus the flush/close cost to make it durable.
2. **Write amplification** — physical on-disk bytes (`du` of the backend's data
   directory/file) divided by the logical bytes ingested, reported against both
   the raw CSV payload and Jazz's own `encoded_storage_bytes_for_test()`.
3. **Cold-load query latency** — the DB is closed and reopened from disk with
   cold caches; the reopen cost and a fixed query set are timed reading from
   cold storage. Each query runs twice so the cold (first) and warm (second)
   reads are visible. The same eight queries run on both layers and return
   identical row counts, so latencies are directly comparable:

   | query                       | access pattern                       |
   | --------------------------- | ------------------------------------ |
   | `point_by_key`              | exact key lookup                     |
   | `prefix_scan_AB`            | ordered range `[AB, AC)`             |
   | `filter_family_Malvaceae`   | non-key equality (full scan)         |
   | `full_scan`                 | count all rows                       |
   | `contains_scientific_Carex` | substring match on `scientific_name` |
   | `common_name_present`       | non-empty-column filter              |
   | `family_in_set`             | membership in a set of families      |
   | `top_100_by_symbol`         | first N ordered by `symbol`          |

## Layers and engines

`--storage` (Jazz layer) and `--raw` (native layer) both take a comma-separated
list and can be combined in one run:

| `--storage` (via Jazz `Db`) | backend                           | cold-load |
| --------------------------- | --------------------------------- | --------- |
| `rocksdb`                   | `RocksDbStorage` (WalNoSync)      | yes       |
| `btree`                     | `NativeBtreeStorage` (opfs-btree) | yes       |
| `slatedb`                   | `SlateDbStorage` (LSM, prototype) | yes       |
| `memory`                    | `MemoryStorage` (baseline)        | no (warm) |

| `--raw` (native crate API) | engine                                                            | cold-load |
| -------------------------- | ----------------------------------------------------------------- | --------- |
| `rocksdb`                  | `rocksdb` crate: WriteBatch + LZ4/Zstd, block cache               | yes       |
| `slatedb`                  | `slatedb` crate: async WriteBatch, Zstd SSTs, over `object_store` | yes       |

Both native engines are configured to their strengths so the comparison is
engine-vs-engine, not config-vs-config: RocksDB uses LZ4 + bottommost Zstd; the
`slatedb` path enables Zstd SST compression (`--raw slatedb`). SlateDB also
retains a WAL that its background GC reclaims only after `min_age`; the optional
`--slatedb-settle-ms <n>` waits before sizing so that GC can run. With Zstd on
the compacted SST dominates and the WAL is no longer significant, so it defaults
to `0` (off).

Each run is labelled `jazz:<adapter>` or `raw:<engine>`, and failures are
isolated so one backend can't abort the comparison.

## Running

```bash
# Default (no flags): the direct engines — raw:rocksdb and raw:slatedb.
cargo run --release -p jazz-ingest-bench

# Jazz-vs-native head to head on the same dataset.
cargo run --release -p jazz-ingest-bench -- --storage rocksdb,slatedb --raw rocksdb,slatedb

# All Jazz adapters on a 20k-row slice, with JSON output.
cargo run --release -p jazz-ingest-bench -- \
  --storage memory,rocksdb,btree,slatedb --limit 20000 --json

# Options:
#   --storage <list>  Jazz adapters:  memory,rocksdb,btree,slatedb
#   --raw <list>      native engines: rocksdb,slatedb   (default when no flags)
#   --input <path>    CSV dataset                       (default bundled file)
#   --batch-size <n>  rows per transaction/write-batch  (default 1000)
#   --limit <n>       ingest only the first n rows
#   --json            also emit one JSON line per run
```

## Caveats

- Cold-load drops Jazz's in-process caches (close + reopen) but does **not**
  drop the OS page cache — that needs elevated privileges. Numbers are
  cold-relative-to-process, which is what a fresh `Db::open` sees in practice.
- `SlateDbStorage` is a prototype backend; treat its numbers as indicative.
- `encoded_storage_bytes_for_test` is gated by the `jazz` `testing` feature,
  which this crate enables (as the jazz-sim benches do).
