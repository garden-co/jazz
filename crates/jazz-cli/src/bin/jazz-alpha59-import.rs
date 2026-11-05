//! `jazz-alpha59-import`: the import half of moving a self-hosted alpha.59
//! server data dir to the current format. It reads the dump that
//! `jazz-alpha59-export` writes. Both ship as GitHub release assets (see
//! `.github/workflows/alpha59-exporter.yml`), since the npm `jazz-tools`
//! carries no native binary.
//!
//! ```text
//! jazz-alpha59-import import --dump app.dump --data-dir ./data-new --app-id <app id>
//! jazz-alpha59-import verify --dump app.dump --data-dir ./data-new --app-id <app id>
//! ```

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    // The JSON summary goes to stdout, so logs go to stderr.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("jazz=info".parse().unwrap())
                .add_directive("jazz_tools=info".parse().unwrap()),
        )
        .init();
    let code = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to start the async runtime")
        .block_on(jazz_cli::migrate_alpha59::main(
            "jazz-alpha59-import",
            std::env::args().skip(1).collect(),
        ));
    std::process::exit(code);
}
