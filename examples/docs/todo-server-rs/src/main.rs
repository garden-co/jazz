//! Todo Server - Example backend using the `jazz-app` crate.
//!
//! Demonstrates a simple todo list API over a Jazz app, run as a backend
//! client of the app's TypeScript main backend (`createJazz`): local
//! persistence, and sync through the main backend's /jazz.
//!
//! # Running
//!
//! ```bash
//! # The main backend's /jazz, its app id, and a machine credential it
//! # trusts with `trustWorkload` (an OIDC token):
//! export JAZZ_SERVER_URL=https://todo.example/jazz JAZZ_APP_ID=…
//! export JAZZ_TOKEN_FILE=/var/run/secrets/tokens/jazz   # or JAZZ_TOKEN / JAZZ_TOKEN_COMMAND
//!
//! # Then run the todo backend
//! cargo run -p todo-server-docs
//! ```
//!
//! # API
//!
//! | Route | Method | Description |
//! |-------|--------|-------------|
//! | `/todos` | GET | List all todo items |
//! | `/todos` | POST | Create new item |
//! | `/todos/:id` | GET | Get one item |
//! | `/todos/:id` | PUT | Update item |
//! | `/todos/:id` | DELETE | Delete item |
//! | `/todos/live` | GET | SSE stream of full-list snapshots |

use std::future::IntoFuture;
use std::net::SocketAddr;
use std::time::Duration;

use ::jazz_app::{AppId, CLOSE_TIMEOUT, Config, Storage};
use futures_util::FutureExt;
use todo_server_docs::{credential::Credential, jazz_app, router_until};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

/// Resolves on Ctrl-C, or SIGTERM (what a container runtime sends).
async fn signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("listen for SIGTERM");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn env(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("set {name}"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("todo_server_docs=info".parse().unwrap()),
        )
        .init();

    // Configuration from the environment
    let app_id = env("JAZZ_APP_ID")?;
    let server_url = env("JAZZ_SERVER_URL")?;
    let credential = Credential::from_env()?;
    let data_dir = std::env::var("TODO_DATA_DIR").unwrap_or_else(|_| "./todo-data".to_string());
    let port: u16 = std::env::var("TODO_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

    // #region context-setup-rust-backend-client
    // This service is a backend client of the app's main backend: it syncs
    // through the main backend's /jazz with its machine credential, and the
    // main backend decides who it is (`trustWorkload` in `createJazz`):
    // scoped as one user, with that user's permissions, or trusted.
    let app_id = AppId::from_string(&app_id).map_err(|_| {
        format!("JAZZ_APP_ID `{app_id}` is not an app id (createJazz's jazz.appId)")
    })?;
    let config = Config::new(app_id)
        .server(&server_url)
        .storage(Storage::Dir(data_dir.into()))
        .credential(credential.provider());

    // `open` is generated from jazz.app.json: it embeds the app's schema.
    let jazz = jazz_app::open(config).await?;
    // #endregion context-setup-rust-backend-client
    info!(
        "Connected to {server_url} as a {:?} backend client",
        jazz.backend_client()
    );

    // Ctrl-C or SIGTERM: stop accepting, end the live streams, let the
    // requests in flight finish (for up to 10 seconds), then stop the
    // client, so the writes already made finish and the store closes.
    let stopping = signal().boxed().shared();
    let app = router_until(jazz.clone(), stopping.clone())
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive());
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let serving = axum::serve(listener, app)
        .with_graceful_shutdown(stopping.clone())
        .into_future();
    tokio::pin!(serving);
    tokio::select! {
        served = &mut serving => served?,
        () = async {
            stopping.await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        } => info!("Requests still open after 10 s; stopping anyway"),
    }
    info!("Shutting down");
    jazz.close(CLOSE_TIMEOUT).await?;
    Ok(())
}
