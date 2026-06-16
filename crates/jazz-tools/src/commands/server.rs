//! Server command implementation.

use std::net::SocketAddr;
use std::time::Duration;

use jazz_tools::middleware::AuthConfig;
use jazz_tools::schema_manager::AppId;
use jazz_tools::server::{ServerBuilder, ShutdownPhase, StorageBackend};
use tracing::info;

const STANDALONE_INSPECTOR_URL: &str = "https://jazz2-inspector.vercel.app/";

/// Run the Jazz server.
pub async fn run(
    app_id_str: &str,
    port: u16,
    data_dir: &str,
    in_memory: bool,
    auth_config: AuthConfig,
    upstream_url: Option<String>,
    bound_port_file: Option<String>,
    shutdown_timeout: Duration,
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
        .with_shutdown_timeout(shutdown_timeout);
    let builder = match upstream_url {
        Some(upstream_url) => builder.with_upstream_url(upstream_url),
        None => builder,
    };
    let built = if in_memory {
        builder.with_storage(StorageBackend::InMemory).build().await
    } else {
        builder
            .with_storage(StorageBackend::Persistent {
                path: data_dir.into(),
            })
            .build()
            .await
    }
    .map_err(|e| format!("failed to build server: {e}"))?;

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound_addr = listener.local_addr()?;
    let inspector_link = build_inspector_link(bound_addr.port(), &app_id_string);

    if let Some(path) = bound_port_file {
        std::fs::write(&path, bound_addr.port().to_string())
            .map_err(|e| format!("failed to write bound port file {path}: {e}"))?;
    }

    info!("Listening on http://{}", bound_addr);
    info!("Open the inspector: {}", inspector_link);
    if admin_secret.is_some() {
        info!("Enter your admin secret in the inspector to publish schemas and policies.");
    }

    let state = built.state.clone();
    let shutdown = state.shutdown.clone();
    // Report the current inbound WebSocket count as an OTel gauge. Held for the
    // server's lifetime; dropped when `run` returns. Only active when otel is
    // compiled in and an OTLP endpoint is configured.
    #[cfg(feature = "otel")]
    let _active_ws_gauge = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .is_ok()
        .then(|| {
            let meter = opentelemetry::global::meter("jazz-server");
            let shutdown = state.shutdown.clone();
            jazz_tools::otel::register_active_websockets_gauge(&meter, move || {
                shutdown.active_websockets() as u64
            })
        });
    let shutdown_budget = shutdown_timeout
        .saturating_mul(2)
        .saturating_add(Duration::from_secs(5));
    let (serve_shutdown_tx, serve_shutdown_rx) = tokio::sync::oneshot::channel();
    let mut shutdown_task = tokio::spawn(async move {
        state.shutdown.wait_requested().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let phase = state.run_shutdown_finalization().await;
        let _ = serve_shutdown_tx.send(());
        phase
    });

    let mut serve_task = tokio::spawn(async move {
        axum::serve(listener, built.app)
            .with_graceful_shutdown(async {
                let _ = serve_shutdown_rx.await;
            })
            .await
    });

    let mut forced_shutdown = false;
    let serve_join_result = tokio::select! {
        result = &mut serve_task => result,
        _ = async {
            shutdown.wait_requested().await;
            tokio::time::sleep(shutdown_budget).await;
        } => {
            forced_shutdown = true;
            serve_task.abort();
            match tokio::time::timeout(Duration::from_millis(50), &mut serve_task).await {
                Ok(result) => result,
                Err(_) => Ok(Ok(())),
            }
        }
    };

    let serve_result = match serve_join_result {
        Ok(result) => result,
        Err(error) if forced_shutdown && error.is_cancelled() => Ok(()),
        Err(error) => {
            shutdown_task.abort();
            let _ = tokio::time::timeout(Duration::from_millis(50), shutdown_task).await;
            return Err(Box::new(error));
        }
    };

    if let Err(error) = serve_result {
        shutdown_task.abort();
        let _ = tokio::time::timeout(Duration::from_millis(50), shutdown_task).await;
        return Err(Box::new(error));
    }

    if forced_shutdown {
        return Err("server shutdown timed out while waiting for active requests to finish".into());
    }

    if shutdown.is_shutting_down() {
        let final_phase =
            match tokio::time::timeout(Duration::from_secs(5), &mut shutdown_task).await {
                Ok(Ok(phase)) => phase,
                Ok(Err(error)) => return Err(Box::new(error)),
                Err(_) => {
                    shutdown_task.abort();
                    return Err("server shutdown finalization did not finish".into());
                }
            };
        if final_phase == ShutdownPhase::Failed {
            return Err("server shutdown finalization failed".into());
        }
    } else {
        shutdown_task.abort();
        let _ = tokio::time::timeout(Duration::from_millis(50), shutdown_task).await;
    }

    Ok(())
}

fn build_inspector_link(port: u16, app_id: &str) -> String {
    let server_url = format!("http://localhost:{port}");
    format!(
        "{STANDALONE_INSPECTOR_URL}#serverUrl={}&appId={}",
        percent_encode_fragment_value(&server_url),
        percent_encode_fragment_value(app_id),
    )
}

fn percent_encode_fragment_value(input: &str) -> String {
    let mut encoded = String::with_capacity(input.len());
    for byte in input.bytes() {
        let is_unreserved =
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~');
        if is_unreserved {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::build_inspector_link;

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
}
