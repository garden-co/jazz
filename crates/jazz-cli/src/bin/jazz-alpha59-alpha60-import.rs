//! `jazz-alpha59-alpha60-import`: the import half of moving a self-hosted alpha.59
//! server data dir to the current format. It reads the dump that
//! `jazz-alpha59-alpha60-export` writes. Both ship as GitHub release assets (see
//! `.github/workflows/alpha59-alpha60-exporter.yml`), since the npm `jazz-tools`
//! carries no native binary.
//!
//! ```text
//! jazz-alpha59-alpha60-import import --dump app.dump --data-dir ./data-new --app-id <app id>
//! jazz-alpha59-alpha60-import verify --dump app.dump --data-dir ./data-new --app-id <app id>
//! ```

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    // The JSON summary goes to stdout, so logs go to stderr.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            // A target `RUST_LOG` names takes the level it gives there.
            tracing_subscriber::EnvFilter::new(format!(
                "jazz=info,jazz_tools=info,{}",
                std::env::var("RUST_LOG").unwrap_or_default().trim()
            )),
        )
        .init();
    let code = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to start the async runtime")
        .block_on(jazz_cli::alpha59_alpha60::main(
            "jazz-alpha59-alpha60-import",
            std::env::args().skip(1).collect(),
        ));
    std::process::exit(code);
}
