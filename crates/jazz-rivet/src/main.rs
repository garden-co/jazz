//! Jazz actor host using Rivet callbacks and its unmodified async SQLite worker.
mod transport;
use anyhow::{Context, Result, ensure};
use jazz_server::{AuthConfig, EmbeddedOutgoing, EmbeddedServer, ServerBuilder, StorageBackend};
use rivet_envoy_client::{
    callbacks::{BorrowedBoxFuture, BoxFuture, EnvoyCallbacks},
    config::{ActorName, EnvoyConfig},
    handle::EnvoyHandle,
    http::{HttpRequest, HttpResponse},
    protocol as p,
    websocket::{WebSocketHandler, WebSocketSender},
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

struct Running {
    generation: u32,
    app_id: String,
    core: EmbeddedServer,
    busy: AtomicUsize,
    touched: Mutex<Instant>,
}
struct Activity(Arc<Running>);
impl Activity {
    fn new(actor: Arc<Running>) -> Self {
        actor.busy.fetch_add(1, Ordering::SeqCst);
        Self(actor)
    }
}
impl Drop for Activity {
    fn drop(&mut self) {
        *self.0.touched.lock().unwrap() = Instant::now();
        self.0.busy.fetch_sub(1, Ordering::SeqCst);
    }
}
#[derive(Default)]
struct Actors {
    running: HashMap<String, Arc<Running>>,
    starting: HashMap<String, Starting>,
}
struct Starting {
    generation: u32,
    done: tokio::sync::watch::Receiver<bool>,
}
// An incomplete startup releases its reservation and wakes a waiting stop,
// including on failure. Callback cancellation cannot abandon the owned startup.
struct StartLease(Arc<Mutex<Actors>>, String, tokio::sync::watch::Sender<bool>);
impl Drop for StartLease {
    fn drop(&mut self) {
        self.0.lock().unwrap().starting.remove(&self.1);
        self.2.send_replace(true);
    }
}
#[derive(Clone)]
struct Host {
    actors: Arc<Mutex<Actors>>,
    secret: String,
    idle: Duration,
}
impl Host {
    fn actor(&self, id: &str) -> Result<Arc<Running>> {
        self.actors
            .lock()
            .unwrap()
            .running
            .get(id)
            .cloned()
            .context("actor is not running")
    }
}
fn reply(status: u16, body: impl Into<Vec<u8>>) -> HttpResponse {
    HttpResponse {
        status,
        headers: HashMap::new(),
        body: Some(body.into()),
        body_stream: None,
    }
}
impl EnvoyCallbacks for Host {
    fn on_actor_start(
        &self,
        handle: EnvoyHandle,
        id: String,
        generation: u32,
        config: p::ActorConfig,
        _: Option<p::PreloadedKv>,
    ) -> BoxFuture<Result<()>> {
        let host = self.clone();
        Box::pin(async move {
            tokio::spawn(async move {
                ensure!(config.name == "jazz", "unknown actor type");
                let (finished, done) = tokio::sync::watch::channel(false);
                {
                    let mut actors = host.actors.lock().unwrap();
                    ensure!(
                        !actors.running.contains_key(&id) && !actors.starting.contains_key(&id),
                        "prior actor generation is still running or starting"
                    );
                    actors
                        .starting
                        .insert(id.clone(), Starting { generation, done });
                }
                let _startup = StartLease(host.actors.clone(), id.clone(), finished);
                let storage = jazz_rivet::worker_store(
                    Arc::new(transport::Transport(handle.clone())),
                    id.clone(),
                    generation.into(),
                    tokio::runtime::Handle::current(),
                );
                let app = jazz_rivet::app_id_for_actor(&id);
                let auth = AuthConfig {
                    admin_secret: Some(host.secret.clone()),
                    backend_secret: Some(host.secret.clone()),
                    allow_local_first_auth: true,
                    ..Default::default()
                };
                // ServerBuilder starts a dedicated owner thread and synchronously waits
                // for storage there; don't occupy a Tokio I/O worker while it opens.
                let runtime = tokio::runtime::Handle::current();
                let core = tokio::task::spawn_blocking(move || {
                    runtime.block_on(
                        ServerBuilder::new(app)
                            .with_auth_config(auth)
                            .with_storage(StorageBackend::Worker(storage))
                            .build(),
                    )
                })
                .await?
                .map_err(anyhow::Error::msg)?;
                let actor = Arc::new(Running {
                    generation,
                    app_id: app.to_string(),
                    core: EmbeddedServer::new(core),
                    busy: AtomicUsize::new(0),
                    touched: Mutex::new(Instant::now()),
                });
                host.actors
                    .lock()
                    .unwrap()
                    .running
                    .insert(id.clone(), actor.clone());
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        let alive = host
                            .actors
                            .lock()
                            .unwrap()
                            .running
                            .get(&id)
                            .is_some_and(|a| Arc::ptr_eq(a, &actor));
                        if !alive {
                            break;
                        }
                        if actor.busy.load(Ordering::SeqCst) == 0
                            && actor.touched.lock().unwrap().elapsed() >= host.idle
                        {
                            handle.sleep_actor(id.clone(), Some(generation), None);
                            break;
                        }
                    }
                });
                Ok(())
            })
            .await?
        })
    }
    fn on_actor_stop(
        &self,
        _: EnvoyHandle,
        id: String,
        generation: u32,
        _: p::StopActorReason,
    ) -> BoxFuture<Result<()>> {
        let host = self.clone();
        Box::pin(async move {
            tokio::spawn(async move {
                let pending = host
                    .actors
                    .lock()
                    .unwrap()
                    .starting
                    .get(&id)
                    .filter(|start| start.generation == generation)
                    .map(|start| start.done.clone());
                if let Some(mut pending) = pending {
                    pending.wait_for(|done| *done).await?;
                }
                let actor = host
                    .actors
                    .lock()
                    .unwrap()
                    .running
                    .get(&id)
                    .filter(|actor| actor.generation == generation)
                    .cloned();
                if let Some(actor) = actor {
                    ensure!(
                        actor.core.shutdown().await == jazz_server::ShutdownPhase::StorageClosed,
                        "actor storage shutdown failed"
                    );
                    let mut actors = host.actors.lock().unwrap();
                    if actors
                        .running
                        .get(&id)
                        .is_some_and(|current| Arc::ptr_eq(current, &actor))
                    {
                        actors.running.remove(&id);
                    }
                }
                Ok(())
            })
            .await?
        })
    }
    fn on_shutdown(&self) {}
    fn fetch(
        &self,
        _: EnvoyHandle,
        id: String,
        _: p::GatewayId,
        _: p::RequestId,
        mut request: HttpRequest,
    ) -> BoxFuture<Result<HttpResponse>> {
        let actor = self.actor(&id);
        let secret = self.secret.clone();
        Box::pin(async move {
            let actor = actor?;
            let _activity = Activity::new(actor.clone());
            let path = request
                .path
                .strip_prefix("/request")
                .unwrap_or(&request.path);
            if path == "/__jazz/host" {
                let auth = AuthConfig {
                    admin_secret: Some(secret),
                    ..Default::default()
                };
                let credential = request
                    .headers
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case("x-jazz-backend-credential"))
                    .map(|(_, value)| value.as_str());
                if jazz_server::middleware::auth::validate_admin_secret(credential, &auth).is_err()
                {
                    return Ok(reply(401, "unauthorized"));
                }
                return Ok(reply(
                    200,
                    serde_json::to_vec(
                        &serde_json::json!({"appId":actor.app_id,"artifact":std::env::var("JAZZ_HOST_ARTIFACT").unwrap_or_else(|_| "development".into())}),
                    )?,
                ));
            }
            const MAX_BODY: usize = 8 << 20;
            let mut body = request.body.take().unwrap_or_default();
            if body.len() > MAX_BODY {
                return Ok(reply(413, "request too large"));
            }
            if let Some(mut stream) = request.body_stream.take() {
                while let Some(chunk) = stream.recv().await? {
                    if chunk.len() > MAX_BODY - body.len() {
                        return Ok(reply(413, "request too large"));
                    }
                    body.extend_from_slice(&chunk);
                }
            }
            let headers = request.headers.into_iter().collect::<Vec<_>>();
            let (status, headers, body) = actor
                .core
                .handle_parts(&request.method, path, &headers, body)
                .await
                .map_err(anyhow::Error::msg)?;
            Ok(HttpResponse {
                status,
                headers: headers.into_iter().collect(),
                body: Some(body),
                body_stream: None,
            })
        })
    }
    fn websocket(
        &self,
        _: EnvoyHandle,
        id: String,
        _: p::GatewayId,
        _: p::RequestId,
        _: HttpRequest,
        path: String,
        headers: HashMap<String, String>,
        _: bool,
        restoring: bool,
        sender: WebSocketSender,
    ) -> BoxFuture<Result<WebSocketHandler>> {
        let actor = self.actor(&id);
        Box::pin(async move {
            let actor = actor?;
            ensure!(!restoring, "Jazz socket hibernation is not supported");
            let expected = format!("/apps/{}/ws", actor.app_id);
            ensure!(
                path.strip_prefix("/websocket").unwrap_or(&path) == expected,
                "unknown Jazz socket route"
            );
            let activity = Activity::new(actor.clone());
            let mut peer = actor
                .core
                .open_socket_with(&headers.into_iter().collect::<Vec<_>>())
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let input = peer.sender();
            let close = input.clone();
            tokio::spawn(async move {
                let _activity = activity;
                while let Some(message) = peer.recv().await {
                    match message {
                        EmbeddedOutgoing::Binary(bytes) => {
                            sender.send(bytes, true);
                            sender.flush().await;
                        }
                        EmbeddedOutgoing::Close { code, reason } => {
                            sender.close(Some(code), Some(reason));
                            break;
                        }
                    }
                }
            });
            Ok(WebSocketHandler {
                on_message: Box::new(move |message| {
                    let input = input.clone();
                    Box::pin(async move {
                        if !input.send_with_room(message.data).await {
                            input.close();
                        }
                    })
                }),
                on_close: Box::new(move |_, _| {
                    close.close();
                    Box::pin(async {})
                }),
                on_open: None,
            })
        })
    }
    fn can_hibernate<'a>(
        &'a self,
        _: &'a str,
        _: &'a p::GatewayId,
        _: &'a p::RequestId,
        _: &'a HttpRequest,
    ) -> BorrowedBoxFuture<'a, Result<bool>> {
        Box::pin(async { Ok(false) })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let secret = std::env::var("JAZZ_CORE_SECRET").context("JAZZ_CORE_SECRET is required")?;
    ensure!(
        secret.len() >= 32,
        "JAZZ_CORE_SECRET must have at least 32 characters"
    );
    sqlite_embedded::vfs::verify_batch_atomic_writes_at_process_startup().await?;
    let endpoint = std::env::var("RIVET_ENDPOINT").context("RIVET_ENDPOINT is required")?;
    let handle = rivet_envoy_client::envoy::start_envoy(EnvoyConfig {
        version: std::env::var("RIVET_NODE_VERSION")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1),
        endpoint,
        token: std::env::var("RIVET_TOKEN").ok(),
        namespace: std::env::var("RIVET_NAMESPACE").unwrap_or_else(|_| "default".into()),
        pool_name: std::env::var("RIVET_POOL").unwrap_or_else(|_| "jazz".into()),
        prepopulate_actor_names: HashMap::from([(
            "jazz".into(),
            ActorName {
                metadata: serde_json::json!({}),
            },
        )]),
        metadata: Some(serde_json::json!({"jazz":"rivet-async-sqlite-v1"})),
        not_global: true,
        debug_latency_ms: None,
        stopped_generation_floor_ttl: Duration::from_secs(120),
        stopped_generation_floor_capacity: 65536,
        max_queued_requests_per_actor: 64,
        callbacks: Arc::new(Host {
            actors: Default::default(),
            secret,
            idle: Duration::from_secs(300),
        }),
        connector: None,
    })
    .await?;
    let health = handle.clone();
    let router = axum::Router::new().route(
        "/",
        axum::routing::get(move || {
            let health = health.clone();
            async move {
                if health.is_ping_healthy() {
                    axum::http::StatusCode::OK
                } else {
                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                }
            }
        }),
    );
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tokio::select! {
        result = axum::serve(listener, router) => { result?; }
        result = shutdown_signal() => { result?; }
        _ = handle.wait_stopped() => {}
    }
    handle.shutdown_gracefully().await;
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => { result?; }
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
