//! RivetKit owns actor lifecycle, routing and SQLite. Jazz owns admitted operations.
use async_trait::async_trait;
use jazz_server::{AuthConfig, EmbeddedOutgoing, EmbeddedServer, ServerBuilder, StorageBackend};
use rivetkit::prelude::{Result, anyhow};
use rivetkit::{Actor, Ctx, Registry, Request, Response, WebSocket, WsMessage};
use std::{collections::HashMap, sync::Arc};

pub struct JazzActor {
    auth: AuthConfig,
    store: tokio::sync::Mutex<Store>,
}

struct Store {
    destination: crate::blobs::Destination,
    core: Option<Arc<EmbeddedServer>>,
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
        let secret = std::env::var("JAZZ_CORE_SECRET")
            .map_err(|e| anyhow!(e).context("JAZZ_CORE_SECRET is required"))?;
        if secret.len() < 32 {
            return Err(anyhow!("JAZZ_CORE_SECRET must have at least 32 characters"));
        }
        let destination = crate::blobs::Destination::load(ctx.sql()).await?;
        Ok(Self {
            auth: AuthConfig {
                admin_secret: Some(secret),
                ..Default::default()
            },
            store: tokio::sync::Mutex::new(Store {
                destination,
                core: None,
            }),
        })
    }

    async fn on_fetch(self: Arc<Self>, ctx: Ctx<Self>, mut request: Request) -> Result<Response> {
        let _awake = ctx.keep_awake_region();
        let path = request
            .uri()
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/");
        let path = path.strip_prefix("/request").unwrap_or(path).to_owned();
        let configure = path == "/app/blobs/v1" && request.method().as_str() == "PUT";
        if path == "/app" || configure {
            let credential = request
                .headers()
                .get("x-jazz-backend-credential")
                .and_then(|value| value.to_str().ok());
            if jazz_server::middleware::auth::validate_admin_secret(credential, &self.auth).is_err()
            {
                return Response::from_parts(401, HashMap::new(), b"unauthorized".to_vec());
            }
        } else if !path.starts_with("/apps/") {
            return Response::from_parts(404, HashMap::new(), Vec::new());
        }
        // The SDK can supply either a buffered body or an initial buffer and stream.
        let limit = if configure { 8192 } else { 8 << 20 };
        let mut body = std::mem::take(request.body_mut());
        if body.len() > limit {
            return Response::from_parts(413, HashMap::new(), Vec::new());
        }
        if let Some(mut stream) = request.take_body_stream() {
            while let Some(chunk) = stream.recv().await? {
                if chunk.len() > limit - body.len() {
                    return Response::from_parts(413, HashMap::new(), Vec::new());
                }
                body.extend_from_slice(&chunk);
            }
        }
        if path == "/app" || configure {
            let requested = if configure {
                match serde_json::from_slice(&body) {
                    Ok(dest) => Some(dest),
                    Err(_) => {
                        return Response::from_parts(
                            400,
                            HashMap::new(),
                            b"invalid blob destination".to_vec(),
                        );
                    }
                }
            } else {
                None
            };
            let core = match self.open(&ctx, requested).await {
                Ok(core) => core,
                Err(error) => {
                    return Response::from_parts(
                        409,
                        HashMap::new(),
                        error.to_string().into_bytes(),
                    );
                }
            };
            return Response::from_parts(
                200,
                HashMap::new(),
                serde_json::to_vec(&serde_json::json!({
                    "appId": core.state().app_id.to_string()
                }))?,
            );
        }
        let (method, _, headers, _) = request.to_parts();
        let headers: Vec<_> = headers.into_iter().collect();
        let (status, headers, body) = self
            .open(&ctx, None)
            .await?
            .handle_parts(&method, &path, &headers, body)
            .await
            .map_err(|e| anyhow!(e))?;
        Response::from_parts(status, headers.into_iter().collect(), body)
    }

    async fn on_websocket(
        self: Arc<Self>,
        ctx: Ctx<Self>,
        ws: WebSocket,
        request: Request,
    ) -> Result<()> {
        let core = self.open(&ctx, None).await?;
        let path = request.uri().path();
        if path.strip_prefix("/websocket").unwrap_or(path)
            != format!("/apps/{}/ws", core.state().app_id)
        {
            return Err(anyhow!("unknown Jazz socket route"));
        }
        let mut upgrade = request.headers().clone();
        jazz_server::middleware::workload::admit_backend_headers(core.state(), &mut upgrade)
            .await
            .map_err(|e| anyhow!(e))?;
        let mut peer = core.open_socket(upgrade).map_err(|e| anyhow!(e))?;
        let input = peer.sender();
        ws.configure_message_event_callback(Some(Arc::new(move |message, _| {
            let bytes = match message {
                WsMessage::Binary(bytes) => bytes,
                WsMessage::Text(text) => text.into_bytes(),
            };
            input.send(bytes);
            Ok(())
        })));
        let close = peer.sender();
        ws.configure_close_event_callback(Some(Arc::new(move |_, _, _| {
            close.close();
            Box::pin(async { Ok(()) })
        })));
        let awake = ctx.keep_awake_region();
        ctx.register_task(async move {
            let _awake = awake;
            while let Some(message) = peer.recv().await {
                match message {
                    EmbeddedOutgoing::Binary(bytes) => ws.send(WsMessage::Binary(bytes)),
                    EmbeddedOutgoing::Close { code, reason } => {
                        ws.close(Some(code), Some(reason)).await;
                        return;
                    }
                }
            }
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
    async fn open(
        &self,
        ctx: &Ctx<Self>,
        requested: Option<crate::blobs::Destination>,
    ) -> Result<Arc<EmbeddedServer>> {
        let mut state = self.store.lock().await;
        if requested.is_none()
            && let Some(core) = &state.core
        {
            return Ok(core.clone());
        }
        let app_id = jazz::tools::AppId::from_name(ctx.actor_id());
        let owner = app_id.to_string();
        let (destination, blobs) = requested
            .as_ref()
            .unwrap_or(&state.destination)
            .resolve(&owner)
            .map_err(|e| anyhow!(e))?;
        let changing = destination != state.destination;
        if changing && state.destination != crate::blobs::Destination::Sqlite {
            return Err(anyhow!(
                "this actor already stores its large values in a different blob destination"
            ));
        }
        if !changing && let Some(core) = &state.core {
            return Ok(core.clone());
        }
        let db = ctx.sql().clone();
        let runtime = tokio::runtime::Handle::current();
        let owner = ctx.clone();
        let connection = crate::storage::Connection::new(db, runtime.clone(), move |reason| {
            let _ = owner.stop_with_error(reason);
        });
        let storage = jazz_server::runtime::ManagedStore(Arc::new(move || {
            jazz::storage::Storage::from_backend(connection.clone())
        }));
        let prepared = ServerBuilder::new(app_id)
            .with_auth_config(self.auth.clone())
            .with_storage(StorageBackend::Managed(storage))
            .with_blob_store(blobs)
            .prepare_storage()
            .await
            .map_err(|e| anyhow!(e))?;
        if changing {
            if let Some(core) = &state.core {
                drain(core).await?;
            }
            state.core = None;
            // Record the resolved destination before any chunk can be externalized.
            if let Err(error) = destination.pin(ctx.sql()).await {
                let _ = ctx.stop_with_error("reload the persisted blob destination");
                return Err(error);
            }
            state.destination = destination;
        }
        let built = tokio::task::spawn_blocking(move || {
            let _entered = runtime.enter();
            prepared.build()
        })
        .await?
        .map_err(|e| anyhow!(e))?;
        let core = Arc::new(EmbeddedServer::new(built));
        state.core = Some(core.clone());
        Ok(core)
    }

    async fn shutdown(&self) -> Result<()> {
        if let Some(core) = &self.store.lock().await.core {
            drain(core).await?;
        }
        Ok(())
    }
}

async fn drain(core: &EmbeddedServer) -> Result<()> {
    if core.shutdown().await != jazz_server::ShutdownPhase::StorageClosed {
        return Err(anyhow!("Jazz storage did not drain"));
    }
    Ok(())
}

pub fn registry() -> Registry {
    let mut registry = Registry::new();
    registry.register_actor::<JazzActor>("jazz");
    registry
}
