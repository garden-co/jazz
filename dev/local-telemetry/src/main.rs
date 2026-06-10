use anyhow::{Context, Result};
use clap::Parser;
use local_telemetry::collector::{self, CollectorConfig};
use local_telemetry::http;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    #[arg(long, default_value = "127.0.0.1")]
    otlp_host: String,

    #[arg(long, default_value_t = 4318)]
    otlp_port: u16,

    #[arg(long, default_value = "127.0.0.1")]
    http_host: String,

    #[arg(long, default_value_t = 4319)]
    http_port: u16,

    #[arg(long, default_value_t = 2)]
    retention_days: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let data_dir = std::fs::canonicalize({
        std::fs::create_dir_all(&args.data_dir).context("create data dir")?;
        &args.data_dir
    })
    .context("resolve data dir")?;
    tracing::info!("data dir: {}", data_dir.display());
    if args.retention_days != 0 {
        tracing::warn!(
            retention_days = args.retention_days,
            "Rotel JSON file exporter does not support day-based retention; flag accepted for compatibility"
        );
    }

    let public_otlp_http_endpoint: SocketAddr = format!("{}:{}", args.otlp_host, args.otlp_port)
        .parse()
        .context("parse OTLP HTTP endpoint")?;
    let rotel_otlp_http_endpoint: SocketAddr = "127.0.0.1:0"
        .parse()
        .context("parse internal Rotel OTLP HTTP endpoint")?;
    let rotel_otlp_grpc_endpoint: SocketAddr = "127.0.0.1:0"
        .parse()
        .context("parse internal Rotel OTLP gRPC endpoint")?;
    let collector = collector::bind(CollectorConfig {
        data_dir: data_dir.clone(),
        otlp_http_endpoint: rotel_otlp_http_endpoint,
        otlp_grpc_endpoint: rotel_otlp_grpc_endpoint,
    })?;
    let rotel_otlp_http_endpoint = collector.otlp_http_bound_endpoint;

    let shutdown = CancellationToken::new();
    let collector_shutdown = shutdown.clone();
    let otlp_proxy_shutdown = shutdown.clone();
    let http_shutdown = shutdown.clone();
    let web_dist = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("web")
        .join("dist");

    let collector_task = tokio::spawn(collector::run(collector, collector_shutdown));
    let otlp_proxy_task = tokio::spawn(http::serve_otlp_proxy(
        public_otlp_http_endpoint.ip().to_string(),
        public_otlp_http_endpoint.port(),
        rotel_otlp_http_endpoint,
        otlp_proxy_shutdown,
    ));
    let http_task = tokio::spawn(http::serve(
        args.http_host,
        args.http_port,
        data_dir,
        web_dist,
        http_shutdown,
    ));

    tokio::select! {
        result = collector_task => {
            shutdown.cancel();
            result??;
        }
        result = otlp_proxy_task => {
            shutdown.cancel();
            result??;
        }
        result = http_task => {
            shutdown.cancel();
            result??;
        }
        result = tokio::signal::ctrl_c() => {
            result.context("listen for ctrl-c")?;
            shutdown.cancel();
        }
    }

    Ok(())
}
