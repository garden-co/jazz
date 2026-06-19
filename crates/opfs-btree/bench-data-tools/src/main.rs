//! Normalizes the vendored benchmark datasets (committed under `datasets/`) into
//! the canonical `.kv`/`.ops` static files consumed by the in-browser benchmark.
mod normalize;
mod ops;
mod sources;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Normalize vendored benchmark datasets into .kv/.ops files")]
struct Cli {
    /// Profiles to build (comma-separated): objects,wikipedia,all
    #[arg(long, default_value = "all")]
    profiles: String,
    /// Max records per profile (overrides per-profile defaults)
    #[arg(long)]
    count: Option<usize>,
    /// Directory of committed source datasets (`*.gz`)
    #[arg(long, default_value = "crates/opfs-btree/wasm-bench/datasets")]
    datasets: PathBuf,
    /// Output directory for `.kv`/`.ops`/`.license` files
    #[arg(long, default_value = "crates/opfs-btree/wasm-bench/bench-data")]
    out: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    std::fs::create_dir_all(&cli.out)?;
    for profile in sources::selected_profiles(&cli.profiles)? {
        sources::build_profile(profile, cli.count, &cli.datasets, &cli.out)?;
    }
    Ok(())
}
