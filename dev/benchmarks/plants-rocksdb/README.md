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

> **Scale ceiling.** The server's WebSocket sync _ingestion_ is slow — roughly a
> few seconds per 1000-row batch (apparently super-linear), so the full ~93k
> dataset would take ~10 minutes. Run the `server` topology with a small
> `--limit` (a few thousand rows); `raw` and `jazz` handle the full dataset in
> seconds. The per-batch cost is the server ingest path itself, not this harness
> — it's a genuine finding, not a bug. The write time reported for `server`
> includes syncing every row through to the server's durable RocksDB.

## Synthetic EBS delay

`--ebs-delay-ms N` charges a fixed latency per durable commit batch, modelling a
network-attached volume. Applied identically to every topology's write loop.

## Usage

```sh
# Download the dataset (NOT committed to the repo).
dev/benchmarks/plants-rocksdb/scripts/setup.sh

# Local topologies over the full dataset (seconds).
cargo run --release -p plants-rocksdb-bench -- --topology raw,jazz

# The server topology at a practical scale (see the scale ceiling above).
cargo run --release -p plants-rocksdb-bench -- --topology server --limit 5000

# All three, with a 2 ms/commit synthetic EBS delay.
cargo run --release -p plants-rocksdb-bench -- --limit 5000 --ebs-delay-ms 2

# Watch server-sync progress on the slow topology.
JZ_PROGRESS=1 cargo run --release -p plants-rocksdb-bench -- --topology server --limit 5000
```

The default (no `--topology`) runs all three; at the full dataset the `server`
stage will take minutes, so prefer the split commands above.

### Options

| flag             | default                     | meaning                                   |
| ---------------- | --------------------------- | ----------------------------------------- |
| `--limit <n>`    | all rows                    | ingest only the first `n` plants          |
| `--batch <n>`    | 1000                        | rows per commit batch                     |
| `--ebs-delay-ms` | 0                           | synthetic per-batch durable-write latency |
| `--sample <n>`   | 500                         | random ids to fetch back                  |
| `--seed <n>`     | `0x5eed`                    | RNG seed for the id sample                |
| `--topology`     | `raw,jazz,server`           | comma-separated subset                    |
| `--data <path>`  | `<crate>/data/plantlst.txt` | dataset path                              |
