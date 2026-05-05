//! Server command implementation.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use jazz_tools::middleware::AuthConfig;
use jazz_tools::schema_manager::AppId;
use jazz_tools::server::{CatalogueAuthorityMode, ServerBuilder, ServerState};
use jazz_tools::transport_manager::AuthConfig as WsAuthConfig;
use tracing::{error, info, warn};

const STANDALONE_INSPECTOR_URL: &str = "https://jazz2-inspector.vercel.app/";
const UPSTREAM_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Configuration for peering with an upstream Jazz server.
pub struct UpstreamPeerConfig {
    pub url: String,
    pub admin_secret: String,
}

/// Run the Jazz server.
pub async fn run(
    app_id_str: &str,
    port: u16,
    data_dir: &str,
    in_memory: bool,
    auth_config: AuthConfig,
    catalogue_authority: CatalogueAuthorityMode,
    bound_port_file: Option<String>,
    upstream: Option<UpstreamPeerConfig>,
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
        .with_catalogue_authority(catalogue_authority);
    let built = if in_memory {
        builder.with_in_memory_storage().build().await
    } else {
        builder.with_persistent_storage(data_dir).build().await
    }
    .map_err(|e| format!("failed to build server: {e}"))?;

    // Start upstream peering if configured
    if let Some(upstream_config) = upstream {
        info!("Peering with upstream: {}", upstream_config.url);
        let state = Arc::clone(&built.state);
        let app_id = app_id_string.clone();
        tokio::spawn(async move {
            if let Err(e) = run_upstream_peer(state, upstream_config, app_id).await {
                error!("Upstream peer error: {}", e);
            }
        });
    }

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound_addr = listener.local_addr()?;
    let inspector_link =
        build_inspector_link(bound_addr.port(), &app_id_string, admin_secret.as_deref());

    if let Some(path) = bound_port_file {
        std::fs::write(&path, bound_addr.port().to_string())
            .map_err(|e| format!("failed to write bound port file {path}: {e}"))?;
    }

    info!("Listening on http://{}", bound_addr);
    info!("Open the inspector: {}", inspector_link);
    axum::serve(listener, built.app).await?;

    Ok(())
}

/// Manages the upstream peering connection.
///
/// The current server transport surface is the app-scoped WebSocket route.
/// Installing the transport in the runtime handles catalogue seeding, inbound
/// sync, outbound Destination::Server payloads, and reconnects.
async fn run_upstream_peer(
    state: Arc<ServerState>,
    config: UpstreamPeerConfig,
    app_id: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let upstream_ws_url = upstream_url_to_ws(&config.url, &app_id)?;
    info!("Peering with upstream WebSocket: {}", upstream_ws_url);
    let auth = WsAuthConfig {
        jwt_token: None,
        backend_secret: None,
        admin_secret: Some(config.admin_secret),
        backend_session: None,
    };

    state.runtime.connect(upstream_ws_url, auth);
    match tokio::time::timeout(UPSTREAM_HANDSHAKE_TIMEOUT, async {
        loop {
            if state.runtime.transport_ever_connected() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    {
        Ok(true) => info!("upstream WebSocket transport connected"),
        Ok(false) => {
            warn!("upstream WebSocket transport closed before completing its first handshake")
        }
        Err(_) => warn!(
            "upstream WebSocket transport did not complete its first handshake within {:?}",
            UPSTREAM_HANDSHAKE_TIMEOUT
        ),
    }

    Ok(())
}

fn upstream_url_to_ws(upstream_url: &str, app_id: &str) -> Result<String, String> {
    let trimmed = upstream_url.trim().trim_end_matches('/');
    let app_ws_suffix = format!("/apps/{app_id}/ws");

    if let Some(rest) = trimmed.strip_prefix("https://") {
        return Ok(format!("wss://{rest}{app_ws_suffix}"));
    }
    if let Some(rest) = trimmed.strip_prefix("http://") {
        return Ok(format!("ws://{rest}{app_ws_suffix}"));
    }
    if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        let without_ws_suffix = trimmed.strip_suffix("/ws").unwrap_or(trimmed);
        return Ok(format!("{without_ws_suffix}{app_ws_suffix}"));
    }

    Err(format!(
        "invalid upstream URL '{upstream_url}': expected http://, https://, ws://, or wss://"
    ))
}

fn build_inspector_link(port: u16, app_id: &str, admin_secret: Option<&str>) -> String {
    let server_url = format!("http://localhost:{port}");
    let admin_secret_value = admin_secret.unwrap_or("");
    format!(
        "{STANDALONE_INSPECTOR_URL}#serverUrl={}&appId={}&adminSecret={}",
        percent_encode_fragment_value(&server_url),
        percent_encode_fragment_value(app_id),
        percent_encode_fragment_value(admin_secret_value),
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
    use super::upstream_url_to_ws;

    #[test]
    fn inspector_link_percent_encodes_fragment_values() {
        let link = build_inspector_link(4200, "app:one/two", Some("secret with spaces"));

        assert_eq!(
            link,
            "https://jazz2-inspector.vercel.app/#serverUrl=http%3A%2F%2Flocalhost%3A4200&appId=app%3Aone%2Ftwo&adminSecret=secret%20with%20spaces"
        );
    }

    #[test]
    fn upstream_url_to_ws_targets_app_scoped_ws_route() {
        assert_eq!(
            upstream_url_to_ws("https://host.example", "app:one").unwrap(),
            "wss://host.example/apps/app:one/ws"
        );
        assert_eq!(
            upstream_url_to_ws("http://localhost:4200/", "demo").unwrap(),
            "ws://localhost:4200/apps/demo/ws"
        );
        assert_eq!(
            upstream_url_to_ws("ws://localhost:4200/ws", "demo").unwrap(),
            "ws://localhost:4200/apps/demo/ws"
        );
    }

    #[test]
    fn upstream_url_to_ws_rejects_urls_without_network_scheme() {
        assert!(upstream_url_to_ws("localhost:4200", "demo").is_err());
    }
}
