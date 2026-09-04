//! Native HTTP/WebSocket server adapter for Jazz's semantic runtime.

pub mod loopback;
pub mod middleware;
pub mod server;

pub use middleware::AuthConfig;
pub use server::{
    BuiltServer, EdgeUpstreamHealth, ServerBuilder, ServerState, ServerTopology,
    ShutdownController, ShutdownPhase, StorageBackend,
};
#[cfg(feature = "embedded-server")]
pub use server::{
    JazzServer, JazzServerBuilder, ServerDataDir, TEST_JWT_AUDIENCE, TEST_JWT_ISSUER,
    TestJwtIssuer, TestJwtOptions, push_catalogue_in_memory,
};

use std::net::SocketAddr;
use std::time::Duration;

use axum::serve;
use jazz::node::EdgeCacheBudget;
use jazz::tools::AppId;
use jazz::tools::native_transport_connector::NativeTransportConnector;
use tokio::task::JoinHandle;
use tracing::info;

const STANDALONE_INSPECTOR_URL: &str = "https://jazz2-inspector.vercel.app/";

/// Run the native Jazz server process shell.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    app_id_str: &str,
    port: u16,
    data_dir: &str,
    in_memory: bool,
    auth_config: AuthConfig,
    upstream_url: Option<String>,
    edge_cache_budget: Option<EdgeCacheBudget>,
    bound_port_file: Option<String>,
    shutdown_timeout: Duration,
    native_transport: std::sync::Arc<dyn NativeTransportConnector>,
) -> Result<(), Box<dyn std::error::Error>> {
    let app_id = AppId::from_string(app_id_str)?;
    let app_id_string = app_id.to_string();
    let admin_secret = auth_config.admin_secret.clone();
    info!("Starting Jazz server for app: {}", app_id);
    if in_memory {
        info!("Storage mode: in-memory");
    } else {
        info!("Data directory: {}", data_dir);
    }
    let builder = ServerBuilder::new(app_id)
        .with_auth_config(auth_config)
        .with_shutdown_timeout(shutdown_timeout)
        .with_native_transport_connector(native_transport);
    let builder = match upstream_url {
        Some(url) => builder.with_upstream_url(url),
        None => builder,
    };
    let builder = match edge_cache_budget {
        Some(budget) => builder.with_edge_cache_budget(budget),
        None => builder,
    };
    let built = if in_memory {
        builder.with_storage(StorageBackend::InMemory).build().await
    } else {
        builder
            .with_storage_factory(std::sync::Arc::new(
                jazz_storage_rocksdb::RocksDbStorageFactory,
            ))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.into(),
            })
            .build()
            .await
    }
    .map_err(|error| format!("failed to build server: {error}"))?;
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?;
    let bound_addr = listener.local_addr()?;
    let shutdown = built.state.shutdown.clone();
    let mut sigterm_task = install_signal_before_readiness(
        || spawn_sigterm_shutdown_task(shutdown.clone()),
        || {
            bound_port_file.map_or(Ok(()), |path| {
                std::fs::write(&path, bound_addr.port().to_string()).map_err(|error| {
                    std::io::Error::new(
                        error.kind(),
                        format!("failed to write bound port file {path}: {error}"),
                    )
                })
            })
        },
    )?;
    info!("Listening on http://{}", bound_addr);
    info!(
        "Open the inspector: {}",
        build_inspector_link(bound_addr.port(), &app_id_string)
    );
    if admin_secret.is_some() {
        info!("Enter your admin secret in the inspector to publish schemas and policies.");
    }
    #[cfg(feature = "otel")]
    let _active_ws_gauge = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .is_ok()
        .then(|| {
            let meter = opentelemetry::global::meter("jazz-server");
            let shutdown = built.state.shutdown.clone();
            jazz_otel::register_active_websockets_gauge(&meter, move || {
                shutdown.active_websockets() as u64
            })
        });
    let shutdown_budget = shutdown_timeout
        .saturating_mul(2)
        .saturating_add(Duration::from_secs(5));
    let (serve_shutdown_tx, serve_shutdown_rx) = tokio::sync::oneshot::channel();
    let state = built.state.clone();
    let mut shutdown_task = tokio::spawn(async move {
        state.shutdown.wait_requested().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let phase = state.run_shutdown_finalization().await;
        let _ = serve_shutdown_tx.send(());
        phase
    });
    let mut serve_task = tokio::spawn(async move {
        serve(listener, built.app)
            .with_graceful_shutdown(async {
                let _ = serve_shutdown_rx.await;
            })
            .await
    });
    let mut forced_shutdown = false;
    let serve_join_result = tokio::select! {
        result = &mut serve_task => result,
        _ = async { shutdown.wait_requested().await; tokio::time::sleep(shutdown_budget).await; } => { forced_shutdown = true; serve_task.abort(); match tokio::time::timeout(Duration::from_millis(50), &mut serve_task).await { Ok(result) => result, Err(_) => Ok(Ok(())) } }
    };
    let serve_result = match serve_join_result {
        Ok(result) => result,
        Err(error) if forced_shutdown && error.is_cancelled() => Ok(()),
        Err(error) => {
            abort_task(&mut shutdown_task).await;
            abort_task(&mut sigterm_task).await;
            return Err(Box::new(error));
        }
    };
    if let Err(error) = serve_result {
        abort_task(&mut shutdown_task).await;
        abort_task(&mut sigterm_task).await;
        return Err(Box::new(error));
    }
    if forced_shutdown {
        abort_task(&mut sigterm_task).await;
        return Err("server shutdown timed out while waiting for active requests to finish".into());
    }
    if shutdown.is_shutting_down() {
        match tokio::time::timeout(Duration::from_secs(5), &mut shutdown_task).await {
            Ok(Ok(ShutdownPhase::Failed)) => {
                abort_task(&mut sigterm_task).await;
                return Err("server shutdown finalization failed".into());
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                abort_task(&mut sigterm_task).await;
                return Err(Box::new(error));
            }
            Err(_) => {
                abort_task(&mut shutdown_task).await;
                abort_task(&mut sigterm_task).await;
                return Err("server shutdown finalization did not finish".into());
            }
        }
    } else {
        abort_task(&mut shutdown_task).await;
    }
    abort_task(&mut sigterm_task).await;
    Ok(())
}

fn install_signal_before_readiness<T, E>(
    install_signal: impl FnOnce() -> Result<T, E>,
    publish_readiness: impl FnOnce() -> Result<(), E>,
) -> Result<T, E> {
    let signal = install_signal()?;
    publish_readiness()?;
    Ok(signal)
}
#[cfg(unix)]
fn spawn_sigterm_shutdown_task(
    shutdown: ShutdownController,
) -> Result<JoinHandle<()>, std::io::Error> {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    Ok(tokio::spawn(async move {
        if sigterm.recv().await.is_some() {
            if shutdown.request_shutdown() {
                info!("Received SIGTERM; starting controlled shutdown");
            } else {
                info!("Received SIGTERM; controlled shutdown is already in progress");
            }
        }
    }))
}
#[cfg(not(unix))]
fn spawn_sigterm_shutdown_task(_: ShutdownController) -> Result<JoinHandle<()>, std::io::Error> {
    Ok(tokio::spawn(async { std::future::pending::<()>().await }))
}
async fn abort_task<T>(task: &mut JoinHandle<T>) {
    task.abort();
    let _ = tokio::time::timeout(Duration::from_millis(50), task).await;
}
fn build_inspector_link(port: u16, app_id: &str) -> String {
    let server_url = format!("http://localhost:{port}");
    format!(
        "{STANDALONE_INSPECTOR_URL}#serverUrl={}&appId={}",
        percent_encode_fragment_value(&server_url),
        percent_encode_fragment_value(app_id)
    )
}
fn percent_encode_fragment_value(input: &str) -> String {
    input.bytes().fold(String::new(), |mut encoded, byte| {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
        encoded
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::{build_inspector_link, install_signal_before_readiness};

    #[test]
    fn signal_registration_precedes_readiness_publication() {
        let events = RefCell::new(Vec::new());
        let installed = install_signal_before_readiness(
            || {
                events.borrow_mut().push("signal-installed");
                Ok::<_, ()>(7)
            },
            || {
                events.borrow_mut().push("readiness-published");
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(installed, 7);
        assert_eq!(
            events.into_inner(),
            ["signal-installed", "readiness-published"]
        );
    }

    #[test]
    fn failed_signal_registration_never_publishes_readiness() {
        let readiness_published = RefCell::new(false);
        let result = install_signal_before_readiness(
            || Err::<(), _>("signal registration failed"),
            || {
                *readiness_published.borrow_mut() = true;
                Ok(())
            },
        );

        assert_eq!(result, Err("signal registration failed"));
        assert!(!readiness_published.into_inner());
    }

    #[test]
    fn inspector_link_percent_encodes_fragment_values() {
        let link = build_inspector_link(4200, "app:one/two");

        assert_eq!(
            link,
            "https://jazz2-inspector.vercel.app/#serverUrl=http%3A%2F%2Flocalhost%3A4200&appId=app%3Aone%2Ftwo"
        );
    }

    #[test]
    fn inspector_link_does_not_include_admin_secret() {
        let link = build_inspector_link(4200, "my-app");

        assert!(
            !link.contains("adminSecret"),
            "admin secret must not appear in logged link"
        );
    }

    #[test]
    fn semantic_runtime_facade_is_consumable_without_core_state_access() {
        let _boundary = |runtime: &jazz::serving::ServerRuntimeHandle| {
            let _activity = runtime.subscribe_activity();
            runtime.notify_activity();
        };
    }
}
