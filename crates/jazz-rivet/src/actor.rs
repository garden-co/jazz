//! RivetKit owns actor lifecycle, routing and SQLite. Jazz owns admitted operations.
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use jazz_server::{AuthConfig, EmbeddedOutgoing, EmbeddedServer, ServerBuilder, StorageBackend};
use rivetkit::{Actor, Ctx, Registry, Request, Response, WebSocket, WsMessage};
use std::{collections::HashMap, sync::Arc, time::Duration};

#[cfg(feature = "testing")]
static STARTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
/// Lifecycle observation for the real-engine integration test.
#[cfg(feature = "testing")]
pub fn test_start_count() -> usize {
    STARTS.load(std::sync::atomic::Ordering::SeqCst)
}

pub struct JazzActor {
    core: EmbeddedServer,
    app_id: String,
    secret: String,
}

fn headers(request: &Request) -> Vec<(String, String)> {
    request
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.to_string(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect()
}

#[async_trait]
impl Actor for JazzActor {
    type Input = ();
    type Actions = ();
    type Events = ();
    type ConnParams = ();
    type Action = rivetkit::action::Raw;
    const CONCURRENT_HTTP_CALLBACKS: bool = true;

    async fn create(ctx: &Ctx<Self>) -> Result<Self> {
        let secret = std::env::var("JAZZ_CORE_SECRET").context("JAZZ_CORE_SECRET is required")?;
        ensure!(
            secret.len() >= 32,
            "JAZZ_CORE_SECRET must have at least 32 characters"
        );
        let app_id = crate::app_id_for_actor(ctx.actor_id());
        let failure_ctx = ctx.clone();
        let storage = crate::worker_store(
            ctx.sql().clone(),
            tokio::runtime::Handle::current(),
            move |message| {
                let _ = failure_ctx.stop_with_error(format!("Jazz storage failed: {message}"));
            },
        );
        let auth = AuthConfig {
            admin_secret: Some(secret.clone()),
            backend_secret: Some(secret.clone()),
            allow_local_first_auth: true,
            ..Default::default()
        };
        let runtime = tokio::runtime::Handle::current();
        let core = tokio::task::spawn_blocking(move || {
            runtime.block_on(
                ServerBuilder::new(app_id)
                    .with_auth_config(auth)
                    .with_storage(StorageBackend::Worker(storage))
                    .build(),
            )
        })
        .await?
        .map_err(anyhow::Error::msg)?;
        #[cfg(feature = "testing")]
        STARTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Self {
            core: EmbeddedServer::new(core),
            app_id: app_id.to_string(),
            secret,
        })
    }

    async fn on_fetch(self: Arc<Self>, ctx: Ctx<Self>, request: Request) -> Result<Response> {
        let _awake = ctx.keep_awake_region();
        let path = request
            .uri()
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/");
        let path = path.strip_prefix("/request").unwrap_or(path).to_owned();
        let headers = headers(&request);
        if path == "/__jazz/host" {
            let auth = AuthConfig {
                admin_secret: Some(self.secret.clone()),
                ..Default::default()
            };
            let credential = headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("x-jazz-backend-credential"))
                .map(|(_, v)| v.as_str());
            if jazz_server::middleware::auth::validate_admin_secret(credential, &auth).is_err() {
                return Response::from_parts(401, HashMap::new(), b"unauthorized".to_vec());
            }
            return Response::from_parts(
                200,
                HashMap::new(),
                serde_json::to_vec(&serde_json::json!({
                    "appId": self.app_id,
                    "artifact": std::env::var("JAZZ_HOST_ARTIFACT").unwrap_or_else(|_| "development".into())
                }))?,
            );
        }
        const MAX_BODY: usize = 8 << 20;
        let mut body = request.body().clone();
        if body.len() > MAX_BODY {
            return Response::from_parts(413, HashMap::new(), Vec::new());
        }
        if let Some(mut stream) = request.take_body_stream() {
            while let Some(chunk) = stream.recv().await? {
                if chunk.len() > MAX_BODY - body.len() {
                    return Response::from_parts(413, HashMap::new(), Vec::new());
                }
                body.extend_from_slice(&chunk);
            }
        }
        let (status, headers, body) = self
            .core
            .handle_parts(request.method().as_str(), &path, &headers, body)
            .await
            .map_err(anyhow::Error::msg)?;
        Response::from_parts(status, headers.into_iter().collect(), body)
    }

    async fn on_websocket(
        self: Arc<Self>,
        ctx: Ctx<Self>,
        ws: WebSocket,
        request: Request,
    ) -> Result<()> {
        let path = request.uri().path();
        ensure!(
            path.strip_prefix("/websocket").unwrap_or(path) == format!("/apps/{}/ws", self.app_id),
            "unknown Jazz socket route"
        );
        let mut peer = self
            .core
            .open_socket_with(&headers(&request))
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let input = peer.sender();
        let close = input.clone();
        // Rivet's public message callback is synchronous. Bound incoming work;
        // a slow peer reconnects through normal Jazz sync rather than lose frames.
        let (send, mut receive) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
        ws.configure_message_event_callback(Some(Arc::new(move |message, _| {
            let bytes = match message {
                WsMessage::Binary(bytes) => bytes,
                WsMessage::Text(text) => text.into_bytes(),
            };
            if send.try_send(bytes).is_err() {
                close.close();
            }
            Ok(())
        })));
        let close = input.clone();
        ws.configure_close_event_callback(Some(Arc::new(move |_, _, _| {
            close.close();
            Box::pin(async { Ok(()) })
        })));
        let awake = ctx.keep_awake_region();
        ctx.register_task(async move {
            let _awake = awake;
            let incoming = async {
                while let Some(bytes) = receive.recv().await {
                    if !input.send_with_room(bytes).await {
                        break;
                    }
                }
            };
            let outgoing = async {
                while let Some(message) = peer.recv().await {
                    match message {
                        EmbeddedOutgoing::Binary(bytes) => ws.send(WsMessage::Binary(bytes)),
                        EmbeddedOutgoing::Close { code, reason } => {
                            ws.close(Some(code), Some(reason)).await;
                            return;
                        }
                    }
                }
            };
            tokio::select! { _ = incoming => {}, _ = outgoing => {} }
            input.close();
            ws.close(Some(1000), None).await;
        });
        Ok(())
    }
    async fn on_sleep(self: Arc<Self>, _ctx: Ctx<Self>) -> Result<()> {
        self.shutdown().await
    }
    async fn on_destroy(self: Arc<Self>, _ctx: Ctx<Self>) -> Result<()> {
        self.shutdown().await
    }
}
impl JazzActor {
    async fn shutdown(&self) -> Result<()> {
        ensure!(
            self.core.shutdown().await == jazz_server::ShutdownPhase::StorageClosed,
            "Jazz storage did not drain"
        );
        Ok(())
    }
}

pub fn registry() -> Registry {
    let mut registry = Registry::new();
    registry.register_actor_with::<JazzActor>(
        "jazz",
        rivetkit::ActorConfig {
            sleep_timeout: Duration::from_secs(300),
            ..Default::default()
        },
    );
    registry
}
