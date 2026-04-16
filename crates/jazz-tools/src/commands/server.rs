//! Server command implementation.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use jazz_tools::ServerEvent;
use jazz_tools::jazz_transport::SyncBatchResponse;
use jazz_tools::middleware::AuthConfig;
use jazz_tools::schema_manager::AppId;
use jazz_tools::server::{CatalogueAuthorityMode, ServerBuilder, ServerState};
use jazz_tools::sync_manager::{ClientId, InboxEntry, ServerId, Source, SyncPayload};
use tokio::sync::oneshot;
use tracing::{error, info, warn};

const STANDALONE_INSPECTOR_URL: &str = "https://jazz2-inspector.vercel.app/";
const UPSTREAM_RETRY_DELAY: Duration = Duration::from_secs(5);

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
        tokio::spawn(async move {
            if let Err(e) = run_upstream_peer(state, upstream_config).await {
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
/// 1. Registers the upstream as a server in our runtime
/// 2. Connects to the upstream's /events stream to receive incoming data
/// 3. Polls our runtime's outbox for Server-destined messages and POSTs to /sync
async fn run_upstream_peer(
    state: Arc<ServerState>,
    config: UpstreamPeerConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let upstream_server_id = ServerId::new();
    let our_client_id = ClientId::new();
    let http_client = reqwest::Client::new();

    // Spawn the event listener (incoming from upstream)
    let events_state = Arc::clone(&state);
    let events_url = format!(
        "{}?client_id={}",
        url_join(&config.url, "/events"),
        our_client_id.0
    );
    let events_secret = config.admin_secret.clone();
    let events_client = http_client.clone();
    let events_server_id = upstream_server_id;
    let (initial_ready_tx, initial_ready_rx) = oneshot::channel();

    tokio::spawn(async move {
        let mut initial_ready_tx = Some(initial_ready_tx);
        loop {
            match connect_upstream_events(
                &events_client,
                &events_url,
                &events_secret,
                events_server_id,
                &events_state,
                &mut initial_ready_tx,
            )
            .await
            {
                Ok(()) => {
                    info!("upstream event stream ended, reconnecting in 5s");
                }
                Err(e) => {
                    warn!("upstream event stream error: {}, reconnecting in 5s", e);
                }
            }
            tokio::time::sleep(UPSTREAM_RETRY_DELAY).await;
        }
    });

    let initial_catalogue_state_hash = initial_ready_rx
        .await
        .map_err(|_| "upstream event stream ended before sending Connected")?;

    state.runtime.add_server_with_catalogue_state_hash(
        upstream_server_id,
        initial_catalogue_state_hash.as_deref(),
    )?;
    info!(
        %upstream_server_id,
        catalogue_state_hash = initial_catalogue_state_hash.as_deref(),
        "registered upstream server in runtime"
    );

    // Spawn the outbox poller (outgoing to upstream)
    let sync_url = url_join(&config.url, "/sync");
    let sync_secret = config.admin_secret.clone();

    // Take the server outbox receiver — messages arrive here when the
    // runtime produces Destination::Server entries.
    let mut outbox_rx = state
        .server_outbox_rx
        .lock()
        .unwrap()
        .take()
        .expect("server_outbox_rx should only be taken once");

    // Batch and forward outgoing messages to the upstream
    let mut batch: Vec<SyncPayload> = Vec::new();
    loop {
        if batch.is_empty() {
            match outbox_rx.recv().await {
                Some((sid, payload)) if sid == upstream_server_id => {
                    batch.push(payload);
                }
                Some(_) => continue, // Message for a different server, skip
                None => break,       // Channel closed
            }

            while let Ok((sid, payload)) = outbox_rx.try_recv() {
                if sid == upstream_server_id {
                    batch.push(payload);
                }
            }
        }

        match send_to_upstream(&http_client, &sync_url, &sync_secret, our_client_id, &batch).await {
            Ok(UpstreamSendOutcome::Delivered) => {
                batch.clear();
            }
            Ok(UpstreamSendOutcome::RetryFailedPayloads { payloads, errors }) => {
                warn!(
                    "upstream rejected {} of {} sync payloads: {}",
                    payloads.len(),
                    batch.len(),
                    errors.join("; ")
                );
                batch = payloads;
                tokio::time::sleep(UPSTREAM_RETRY_DELAY).await;
            }
            Err(e) => {
                warn!("failed to send {} messages to upstream: {}", batch.len(), e);
                tokio::time::sleep(UPSTREAM_RETRY_DELAY).await;
            }
        }
    }

    Ok(())
}

/// Connect to the upstream's /events SSE stream and deliver incoming
/// messages to our runtime.
async fn connect_upstream_events(
    client: &reqwest::Client,
    url: &str,
    admin_secret: &str,
    server_id: ServerId,
    state: &Arc<ServerState>,
    initial_ready_tx: &mut Option<oneshot::Sender<Option<String>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The /events endpoint authenticates via X-Jazz-Backend-Secret (no session needed).
    // We use the upstream's admin secret which doubles as backend auth for peering.
    let resp = client
        .get(url)
        .header("X-Jazz-Backend-Secret", admin_secret)
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(format!("upstream events returned {}", resp.status()).into());
    }

    info!("connected to upstream events stream");

    let mut resp = resp;
    let mut buf = Vec::new();

    while let Some(chunk) = resp.chunk().await? {
        buf.extend_from_slice(&chunk);

        // Parse length-prefixed frames: [4 bytes u32 BE length][JSON payload]
        while buf.len() >= 4 {
            let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if buf.len() < 4 + len {
                break; // Need more data
            }

            let frame = &buf[4..4 + len];
            match serde_json::from_slice::<ServerEvent>(frame) {
                Ok(event) => {
                    handle_upstream_server_event(event, server_id, state, initial_ready_tx)?;
                }
                Err(e) => {
                    warn!("failed to parse upstream event frame: {}", e);
                }
            }

            buf.drain(..4 + len);
        }
    }

    Ok(())
}

fn handle_upstream_server_event(
    event: ServerEvent,
    server_id: ServerId,
    state: &Arc<ServerState>,
    initial_ready_tx: &mut Option<oneshot::Sender<Option<String>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match event {
        ServerEvent::Connected {
            next_sync_seq,
            catalogue_state_hash,
            ..
        } => {
            info!(
                catalogue_state_hash = catalogue_state_hash.as_deref(),
                next_sync_seq, "upstream peer connected"
            );
            if let Some(next_sequence) = next_sync_seq {
                state
                    .runtime
                    .set_server_next_sequence(server_id, next_sequence)?;
            }
            if let Some(tx) = initial_ready_tx.take() {
                let _ = tx.send(catalogue_state_hash);
            }
        }
        ServerEvent::SyncUpdate { seq, payload } => {
            if let Some(ref tracer) = state.sync_tracer {
                tracer.record_incoming(&Source::Server(server_id), "server", &payload);
            }
            let entry = InboxEntry {
                source: Source::Server(server_id),
                payload: *payload,
            };
            if let Some(sequence) = seq {
                state
                    .runtime
                    .push_sync_inbox_with_sequence(entry, sequence)?;
            } else {
                state.runtime.push_sync_inbox(entry)?;
            }
        }
        ServerEvent::Error { message, code } => {
            warn!("upstream error {:?}: {}", code, message);
        }
        ServerEvent::Subscribed { .. } | ServerEvent::Heartbeat => {}
    }

    Ok(())
}

/// Send sync payloads to the upstream server.
enum UpstreamSendOutcome {
    Delivered,
    RetryFailedPayloads {
        payloads: Vec<SyncPayload>,
        errors: Vec<String>,
    },
}

async fn send_to_upstream(
    client: &reqwest::Client,
    url: &str,
    admin_secret: &str,
    client_id: ClientId,
    payloads: &[SyncPayload],
) -> Result<UpstreamSendOutcome, Box<dyn std::error::Error + Send + Sync>> {
    let body = serde_json::json!({
        "payloads": payloads,
        "client_id": client_id,
    });

    let resp = client
        .post(url)
        .header("X-Jazz-Admin-Secret", admin_secret)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("upstream sync returned {}: {}", status, body).into());
    }

    let response: SyncBatchResponse = resp.json().await?;
    if response.results.len() != payloads.len() {
        return Err(format!(
            "upstream sync returned {} results for {} payloads",
            response.results.len(),
            payloads.len()
        )
        .into());
    }

    let mut failed_payloads = Vec::new();
    let mut errors = Vec::new();
    for (payload, result) in payloads.iter().zip(response.results.into_iter()) {
        if result.ok {
            continue;
        }
        failed_payloads.push(payload.clone());
        errors.push(
            result
                .error
                .unwrap_or_else(|| "unknown upstream sync error".to_string()),
        );
    }

    if failed_payloads.is_empty() {
        Ok(UpstreamSendOutcome::Delivered)
    } else {
        Ok(UpstreamSendOutcome::RetryFailedPayloads {
            payloads: failed_payloads,
            errors,
        })
    }
}

fn url_join(base: &str, path: &str) -> String {
    let base = base.trim_end_matches('/');
    format!("{}{}", base, path)
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
    use super::handle_upstream_server_event;
    use jazz_tools::ServerEvent;
    use jazz_tools::jazz_transport::ConnectionId;
    use jazz_tools::middleware::AuthConfig;
    use jazz_tools::schema_manager::AppId;
    use jazz_tools::server::ServerBuilder;
    use jazz_tools::sync_manager::ServerId;
    use tokio::sync::oneshot;

    #[test]
    fn inspector_link_percent_encodes_fragment_values() {
        let link = build_inspector_link(4200, "app:one/two", Some("secret with spaces"));

        assert_eq!(
            link,
            "https://jazz2-inspector.vercel.app/#serverUrl=http%3A%2F%2Flocalhost%3A4200&appId=app%3Aone%2Ftwo&adminSecret=secret%20with%20spaces"
        );
    }

    #[tokio::test]
    async fn connected_upstream_event_sends_initial_catalogue_hash() {
        let state = ServerBuilder::new(AppId::from_name("peer-test"))
            .with_auth_config(AuthConfig {
                allow_local_first_auth: true,
                ..Default::default()
            })
            .with_in_memory_storage()
            .build()
            .await
            .expect("build server")
            .state;

        let (tx, rx) = oneshot::channel();
        let mut initial_ready_tx = Some(tx);
        handle_upstream_server_event(
            ServerEvent::Connected {
                connection_id: ConnectionId(1),
                client_id: "peer-client".to_string(),
                next_sync_seq: Some(7),
                catalogue_state_hash: Some("digest-123".to_string()),
            },
            ServerId::new(),
            &state,
            &mut initial_ready_tx,
        )
        .expect("handle connected event");

        assert_eq!(
            rx.await.expect("receive initial hash"),
            Some("digest-123".to_string())
        );
        assert!(initial_ready_tx.is_none());
    }
}
