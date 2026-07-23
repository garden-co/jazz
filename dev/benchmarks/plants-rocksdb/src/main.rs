//! RocksDB-focused storage benchmark on the USDA PLANTS checklist.
//!
//! Every plant is assigned a stable UUID and ingested three ways, then the same
//! 500 random plants are fetched back by that id:
//!
//! 1. **`raw`** — RocksDB driven directly (no Jazz): one `put` per row, 500
//!    native point `get`s for the read. See [`raw`].
//! 2. **`jazz`** — `jazz::db::Db<RocksDbStorage>`: batched transactions, then the
//!    500 ids fetched with one membership read (Jazz's local read ignores
//!    indexes and full-scans, so 500 sequential point queries are impractical —
//!    a small point-lookup probe reports that per-id cost separately). See [`jazz`].
//! 3. **`server`** — a `jazz::db::Db<RocksDbStorage>` **client** that syncs over a
//!    real localhost WebSocket into a RocksDB-backed **Jazz Server**
//!    (`jazz_server::LoopbackWebSocketServer`). See [`server`]. Cap it at
//!    `--limit 25000` (~4.5 min); the full 93k would take ~40 min because the
//!    server ingest path is super-linear.
//!
//! An optional synthetic **EBS** write delay (`--ebs-delay-ms`) models a
//! network-attached volume by charging a fixed latency per durable commit batch.
//!
//! ```text
//! dev/benchmarks/plants-rocksdb/scripts/setup.sh                 # download the dataset
//! cargo run --release -p plants-rocksdb-bench -- --topology raw,jazz          # full 93k, seconds
//! cargo run --release -p plants-rocksdb-bench -- --topology server --limit 25000  # ~4.5 min
//! cargo run --release -p plants-rocksdb-bench -- --limit 5000 --ebs-delay-ms 2     # all three
//! ```

mod bench;
mod dataset;
mod jazz;
mod raw;
mod server;
mod ws_transport;

use std::path::PathBuf;

use crate::bench::EbsDelay;
use crate::dataset::{load_dataset, sample_ids};
use crate::jazz::run_jazz;
use crate::raw::run_raw;
use crate::server::run_server;

fn main() {
    let mut limit: Option<usize> = None;
    let mut batch = 1000usize;
    let mut ebs_ms = 0u64;
    let mut sample = 500usize;
    let mut seed = 0x5eedu64;
    let mut topologies = vec!["raw".to_owned(), "jazz".to_owned(), "server".to_owned()];
    let default_data = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/data/plantlst.txt"));
    let mut data = default_data;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--limit" => limit = Some(parse_next(&mut args, "--limit")),
            "--batch" => batch = parse_next(&mut args, "--batch"),
            "--ebs-delay-ms" => ebs_ms = parse_next(&mut args, "--ebs-delay-ms"),
            "--sample" => sample = parse_next(&mut args, "--sample"),
            "--seed" => seed = parse_next(&mut args, "--seed"),
            "--data" => {
                data = PathBuf::from(args.next().expect("--data needs a path"));
            }
            "--topology" => {
                let value: String = args.next().expect("--topology needs a value");
                topologies = value.split(',').map(|s| s.trim().to_owned()).collect();
            }
            "--help" | "-h" => {
                print_help();
                return;
            }
            other => panic!("unknown argument: {other}"),
        }
    }

    let plants = load_dataset(&data, limit);
    assert!(!plants.is_empty(), "dataset is empty: {}", data.display());
    let ids = sample_ids(&plants, sample, seed);
    let ebs = EbsDelay::new(ebs_ms);

    println!(
        "dataset {} rows | batch {} | sample {} | ebs-delay {} ms/batch\n",
        plants.len(),
        batch,
        ids.len(),
        ebs_ms
    );

    for topology in &topologies {
        let metrics = match topology.as_str() {
            "raw" => run_raw(&plants, &ids, batch, ebs),
            "jazz" => run_jazz(&plants, &ids, batch, ebs),
            "server" => run_server(&plants, &ids, batch, ebs),
            other => panic!("unknown topology: {other} (expected raw|jazz|server)"),
        };
        metrics.print();
        println!();
    }
}

fn parse_next<T: std::str::FromStr>(args: &mut impl Iterator<Item = String>, flag: &str) -> T
where
    T::Err: std::fmt::Display,
{
    let raw = args
        .next()
        .unwrap_or_else(|| panic!("{flag} needs a value"));
    raw.parse()
        .unwrap_or_else(|e| panic!("{flag}: invalid value {raw:?}: {e}"))
}

fn print_help() {
    println!(
        "plants-rocksdb-bench — RocksDB ingestion + 500-by-id read on the USDA plants dataset\n\n\
         Options:\n\
         \x20 --limit <n>          only ingest the first n plants\n\
         \x20 --batch <n>          rows per commit batch (default 1000)\n\
         \x20 --ebs-delay-ms <n>   synthetic per-batch durable-write latency (default 0)\n\
         \x20 --sample <n>         number of random ids to fetch back (default 500)\n\
         \x20 --seed <n>           RNG seed for the id sample (default 0x5eed)\n\
         \x20 --topology <list>    comma-separated: raw,jazz,server (default all)\n\
         \x20 --data <path>        dataset path (default <crate>/data/plantlst.txt)\n"
    );
}
