mod server;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "jazz-cloud-server")]
#[command(about = "Jazz multi-tenant sync server")]
struct Cli {
    /// Port to listen on.
    #[arg(short, long, default_value = "1625")]
    port: u16,

    /// Root data directory for all app runtimes.
    #[arg(long, default_value = "./data-multi")]
    data_root: String,

    /// Internal API secret for app provisioning routes.
    #[arg(long, env = "JAZZ_INTERNAL_API_SECRET")]
    internal_api_secret: String,

    /// Key used to hash backend/admin secrets in meta-app storage.
    #[arg(long, env = "JAZZ_SECRET_HASH_KEY")]
    secret_hash_key: String,

    /// Number of worker threads used for app placement and fairness scheduling.
    #[arg(long)]
    worker_threads: Option<usize>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("jazz_cloud_server=info".parse().unwrap())
                .add_directive("tower_http=debug".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    if cli.internal_api_secret.is_empty() {
        eprintln!(
            "Missing internal API secret. Set --internal-api-secret or JAZZ_INTERNAL_API_SECRET."
        );
        std::process::exit(1);
    }
    if cli.secret_hash_key.is_empty() {
        eprintln!("Missing secret hash key. Set --secret-hash-key or JAZZ_SECRET_HASH_KEY.");
        std::process::exit(1);
    }

    let worker_threads = cli.worker_threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });

    let config = server::ServerConfig {
        port: cli.port,
        data_root: cli.data_root,
        internal_api_secret: cli.internal_api_secret,
        secret_hash_key: cli.secret_hash_key,
        worker_threads,
    };

    if let Err(err) = server::run(config).await {
        eprintln!("Server error: {err}");
        std::process::exit(1);
    }
}
