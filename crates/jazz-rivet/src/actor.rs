//! RivetKit owns actor lifecycle, routing and SQLite. Jazz owns admitted operations.
use async_trait::async_trait;
use jazz_server::{AuthConfig, EmbeddedOutgoing, EmbeddedServer, ServerBuilder, StorageBackend};
use rivetkit::prelude::{Result, anyhow};
use rivetkit::{Actor, Ctx, Registry, Request, Response, WebSocket, WsMessage};
use std::{collections::HashMap, sync::Arc};

pub struct JazzActor {
    core: EmbeddedServer,
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
        let app_id = jazz::tools::AppId::from_name(ctx.actor_id());
        let db = ctx.sql().clone();
        let runtime = tokio::runtime::Handle::current();
        let owner = ctx.clone();
        let connection = crate::storage::Connection::new(db, runtime.clone(), move |reason| {
            let _ = owner.stop_with_error(reason);
        });
        let storage = jazz_server::runtime::ManagedStore(Arc::new(move || {
            jazz::storage::Storage::from_backend(connection.clone())
        }));
        let auth = AuthConfig {
            admin_secret: Some(secret),
            ..Default::default()
        };
        let core = tokio::task::spawn_blocking(move || {
            runtime.block_on(
                ServerBuilder::new(app_id)
                    .with_auth_config(auth)
                    .with_storage(StorageBackend::Managed(storage))
                    .build(),
            )
        })
        .await?
        .map_err(|e| anyhow!(e))?;
        Ok(Self {
            core: EmbeddedServer::new(core),
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
        if path == "/app" {
            let credential = request
                .headers()
                .get("x-jazz-backend-credential")
                .and_then(|value| value.to_str().ok());
            if jazz_server::middleware::auth::validate_admin_secret(
                credential,
                &self.core.state().auth_config,
            )
            .is_err()
            {
                return Response::from_parts(401, HashMap::new(), b"unauthorized".to_vec());
            }
            return Response::from_parts(
                200,
                HashMap::new(),
                serde_json::to_vec(&serde_json::json!({
                    "appId": self.core.state().app_id.to_string()
                }))?,
            );
        }
        if !path.starts_with("/apps/") {
            return Response::from_parts(404, HashMap::new(), Vec::new());
        }
        const MAX_BODY: usize = 8 << 20;
        if request.body().len() > MAX_BODY {
            return Response::from_parts(413, HashMap::new(), Vec::new());
        }
        let (method, _, headers, mut body) = request.to_parts();
        let headers: Vec<_> = headers.into_iter().collect();
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
        let path = request.uri().path();
        if path.strip_prefix("/websocket").unwrap_or(path)
            != format!("/apps/{}/ws", self.core.state().app_id)
        {
            return Err(anyhow!("unknown Jazz socket route"));
        }
        let mut upgrade = request.headers().clone();
        jazz_server::middleware::workload::admit_backend_headers(self.core.state(), &mut upgrade)
            .await
            .map_err(|e| anyhow!(e))?;
        let mut peer = self.core.open_socket(upgrade).map_err(|e| anyhow!(e))?;
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
    async fn shutdown(&self) -> Result<()> {
        if self.core.shutdown().await != jazz_server::ShutdownPhase::StorageClosed {
            return Err(anyhow!("Jazz storage did not drain"));
        }
        Ok(())
    }
}

pub fn registry() -> Registry {
    let mut registry = Registry::new();
    registry.register_actor::<JazzActor>("jazz");
    registry
}
