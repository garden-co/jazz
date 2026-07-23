# plants-rocksdb-bench

A RocksDB-focused ingestion + point-read benchmark on the USDA PLANTS checklist
(~93k rows). Every plant is assigned a stable UUID, ingested three ways, and then
the same 500 random plants are fetched back by that id.

## Topologies

| name     | write path                                                                    | 500-by-id read                     |
| -------- | ----------------------------------------------------------------------------- | ---------------------------------- |
| `raw`    | RocksDB directly — one `put` per row, no Jazz                                 | 500 native point `get`s            |
| `jazz`   | `jazz::db::Db<RocksDbStorage>`, batched transactions                          | one `in_list` read + per-id probe¹ |
| `server` | Jazz client → **Jazz Server** (both RocksDB) over a real localhost WebSocket² | client-local `in_list` after sync  |

¹ Jazz's local read ignores indexes and full-scans, so 500 sequential point
queries are impractical (~100 s). The bench fetches all 500 with a single
membership (`in_list`) read and separately probes the per-id point-lookup cost.

² The server is `jazz_server::LoopbackWebSocketServer` backed by RocksDB
(`persistent_data_dir`). The client writes locally, connects upstream, and ships
every row to the server; the run then reopens the server's RocksDB directory and
reports how many rows durably landed (`synced to server N / M`).

> **Scale ceiling — cap the `server` topology at `--limit 25000`.** The server's
> WebSocket sync _ingestion_ is super-linear in rows already stored (its history
> consolidation re-runs as the table grows), so per-batch cost keeps climbing:
>
> | batch (×1000 rows) | 5   | 10  | 15  | 20   | 25   |
> | ------------------ | --- | --- | --- | ---- | ---- |
> | elapsed            | 8s  | 37s | 87s | 163s | 267s |
>
> 25k rows takes ~4.5 min and fully syncs (25000/25000); the full ~93k would take
> ~40 min, so it is not run by default. `raw` and `jazz` handle the full dataset
> in well under a second. This per-batch cost is the server ingest path itself,
> not this harness — a genuine finding, not a bug. The `server` write time
> includes syncing every row through to the server's durable RocksDB (verified by
> reopening it: `synced N/M`).
>
> Reference numbers at `--limit 25000` (Apple silicon, release build):
>
> | topology | write all                  | get 500 by id                              |
> | -------- | -------------------------- | ------------------------------------------ |
> | `raw`    | 0.01 s                     | 1.2 ms (500 point gets)                    |
> | `jazz`   | 0.53 s                     | 969 ms cold `in_list` · 55 ms/point-lookup |
> | `server` | 267 s (25000/25000 synced) | 973 ms warm · 60 ms/point-lookup           |

## Usage

```sh
# Download the dataset (NOT committed to the repo).
dev/benchmarks/plants-rocksdb/scripts/setup.sh

# Local topologies over the full dataset (seconds).
cargo run --release -p plants-rocksdb-bench -- --topology raw,jazz

# The server topology at its practical ceiling (~4.5 min; see above).
cargo run --release -p plants-rocksdb-bench -- --topology server --limit 25000

# Watch server-sync progress on the slow topology.
JZ_PROGRESS=1 cargo run --release -p plants-rocksdb-bench -- --topology server --limit 5000
```

The default (no `--topology`) runs all three; at the full dataset the `server`
stage will take minutes, so prefer the split commands above.

### Options

| flag            | default                     | meaning                          |
| --------------- | --------------------------- | -------------------------------- |
| `--limit <n>`   | all rows                    | ingest only the first `n` plants |
| `--batch <n>`   | 1000                        | rows per commit batch            |
| `--sample <n>`  | 500                         | random ids to fetch back         |
| `--seed <n>`    | `0x5eed`                    | RNG seed for the id sample       |
| `--topology`    | `raw,jazz,server`           | comma-separated subset           |
| `--data <path>` | `<crate>/data/plantlst.txt` | dataset path                     |
