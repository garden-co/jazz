//! Todo Server: a todo list API over a Jazz app, run as the app's backend.
//!
//! ```bash
//! # A Jazz server for the app, with its admin and backend secrets:
//! export JAZZ_APP_ID=… JAZZ_SERVER_URL=http://localhost:1625
//! export JAZZ_ADMIN_SECRET=… JAZZ_BACKEND_SECRET=…
//! cargo run -p todo-server
//! ```
//!
//! | Route | Method | Description |
//! |-------|--------|-------------|
//! | `/todos` | GET | List all todo items |
//! | `/todos` | POST | Create new item |
//! | `/todos/:id` | GET | Get one item |
//! | `/todos/:id` | PUT | Update item |
//! | `/todos/:id` | DELETE | Delete item |
//! | `/todos/live` | GET | SSE stream of full-list snapshots |

// mimalloc replaces the system allocator for ~25% throughput on Rust-side
// allocation-heavy paths (query/insert/observer).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::net::SocketAddr;

use ::jazz_app::{AppId, Backend, Config, Storage};
use std::future::IntoFuture;
use std::time::Duration;

use futures_util::FutureExt;
use todo_server::{jazz_app, router_until};
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
                .add_directive("todo_server=info".parse().unwrap()),
        )
        .init();

    let app_id = std::env::var("JAZZ_APP_ID")
        .unwrap_or_else(|_| "019d4349-2460-73fc-9e98-5d67c9697598".to_string());
    let server_url =
        std::env::var("JAZZ_SERVER_URL").unwrap_or_else(|_| "http://localhost:1625".to_string());
    let user_id = std::env::var("TODO_USER_ID").unwrap_or_else(|_| "todo-service".to_string());
    let data_dir = std::env::var("TODO_DATA_DIR").unwrap_or_else(|_| "./todo-data".to_string());
    let port: u16 = std::env::var("TODO_PORT")
        .ok()
        .and_then(|port| port.parse().ok())
        .unwrap_or(3000);

    // This service is the app's backend: it publishes the app's
    // permissions bundle on start (with the admin secret) and acts as one
    // user of the app (vouched for with the backend secret), in a session
    // pinned to that bundle.
    let backend = Backend::new(
        env("JAZZ_BACKEND_SECRET")?,
        env("JAZZ_ADMIN_SECRET")?,
        user_id,
    )
    .permissions(include_bytes!("../jazz.permissions.json").as_slice());
    let config =
        Config::new(AppId::from_string(&app_id).unwrap_or_else(|_| AppId::from_name(&app_id)))
            .server(&server_url)
            .storage(Storage::Dir(data_dir.into()))
            .backend(backend);

    info!("Opening the todo app on {server_url}");
    let jazz = jazz_app::open(config).await?;
    info!("Published bundle {}", jazz.bundle().unwrap_or("-"));

    // Ctrl-C or SIGTERM: stop accepting, end the live streams, let the
    // requests in flight finish (for up to 10 seconds), then stop the
    // client. With a persistent store, call shutdown before exiting: the
    // writes already made finish and the store closes.
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
    jazz.shutdown().await?;
    Ok(())
}
