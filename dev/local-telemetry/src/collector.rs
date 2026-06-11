use anyhow::Result;
use rotel::init::agent::Agent;
use rotel::init::args::{AgentRun, Exporter, Receiver};
use rotel::init::file_exporter::FileExporterFormat;
use rotel::listener::Listener;
use rotel::topology::flush_control::FlushBroadcast;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const SENDING_QUEUE_SIZE: usize = 1_000;

#[derive(Clone, Debug)]
pub struct CollectorConfig {
    pub data_dir: PathBuf,
    pub otlp_http_endpoint: SocketAddr,
    pub otlp_grpc_endpoint: SocketAddr,
}

pub struct BoundCollector {
    config: CollectorConfig,
    port_map: HashMap<SocketAddr, Listener>,
    pub otlp_http_bound_endpoint: SocketAddr,
}

pub fn bind(config: CollectorConfig) -> Result<BoundCollector> {
    let grpc_listener = Listener::listen_std(config.otlp_grpc_endpoint)
        .map_err(|err| anyhow::anyhow!("bind Rotel OTLP gRPC endpoint: {err}"))?;
    let otlp_grpc_bound_endpoint = grpc_listener
        .bound_address()
        .map_err(|err| anyhow::anyhow!("read Rotel OTLP gRPC listener address: {err}"))?;
    let http_listener = Listener::listen_std(config.otlp_http_endpoint)
        .map_err(|err| anyhow::anyhow!("bind Rotel OTLP HTTP endpoint: {err}"))?;
    let otlp_http_bound_endpoint = http_listener
        .bound_address()
        .map_err(|err| anyhow::anyhow!("read Rotel OTLP HTTP listener address: {err}"))?;

    let mut port_map = HashMap::new();
    port_map.insert(otlp_grpc_bound_endpoint, grpc_listener);
    port_map.insert(otlp_http_bound_endpoint, http_listener);

    Ok(BoundCollector {
        config: CollectorConfig {
            otlp_grpc_endpoint: otlp_grpc_bound_endpoint,
            otlp_http_endpoint: otlp_http_bound_endpoint,
            ..config
        },
        port_map,
        otlp_http_bound_endpoint,
    })
}

pub async fn run(bound: BoundCollector, shutdown: CancellationToken) -> Result<()> {
    let mut args = AgentRun {
        receiver: Some(Receiver::Otlp),
        exporter: Some(Exporter::File),
        ..Default::default()
    };
    args.otlp_receiver.otlp_http_endpoint = bound.config.otlp_http_endpoint;
    args.otlp_receiver.otlp_grpc_endpoint = bound.config.otlp_grpc_endpoint;
    args.batch.batch_timeout = Duration::from_millis(200);
    args.file_exporter.file_format = FileExporterFormat::Json;
    args.file_exporter.output_dir = bound.config.data_dir;
    args.file_exporter.flush_interval = Duration::from_millis(200);

    let (_pipeline_flush_tx, pipeline_flush_sub) = FlushBroadcast::new().into_parts();
    let (_exporters_flush_tx, exporters_flush_sub) = FlushBroadcast::new().into_parts();

    Agent::new(
        Box::new(args),
        bound.port_map,
        SENDING_QUEUE_SIZE,
        "dev".to_string(),
    )
    .with_pipeline_flush(pipeline_flush_sub)
    .with_exporters_flush(exporters_flush_sub)
    .run(shutdown)
    .await
    .map_err(|err| anyhow::anyhow!(err))
}
