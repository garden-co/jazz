//! RocksDB-focused ingestion + point-read benchmark on the USDA plants dataset,
//! across three topologies, all RocksDB-backed:
//!
//! * `raw` — RocksDB driven directly (no Jazz).
//! * `local` — a serverless `jazz_tools::JazzClient` (no upstream).
//! * `server` — a `JazzClient` synced over a real localhost websocket into a
//!   `jazz_tools::server::JazzServer` (the real deployment path).
//!
//! Each plant is assigned a UUID; the same sampled ids are fetched back by id.
//! Reports write time, throughput, point-lookup latency, and on-disk size + write
//! amplification. Configured from the environment (`N`, `ONLY`, `JZ_PROGRESS`);
//! the server topology needs `JAZZ_TOOLS_WAIT_FOR_BATCH_TIMEOUT_SECS=300`.

mod config;
mod dataset;
mod jazz;
mod report;
mod topologies;

use crate::config::{Config, SAMPLE};
use crate::dataset::{load_plants, logical_bytes, sample_ids};

async fn run() {
    let cfg = Config::from_env();
    let plants = load_plants(cfg.rows);
    let ids = sample_ids(&plants, SAMPLE);
    let logical = logical_bytes(&plants);
    println!(
        "dataset {} rows | logical payload {:.2} MB | sample {}\n",
        plants.len(),
        logical as f64 / 1e6,
        ids.len()
    );
    let rows = topologies::run_selected(&cfg, &plants, &ids, logical).await;
    report::render(&rows);
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tokio::task::LocalSet::new().run_until(run()).await;
}
