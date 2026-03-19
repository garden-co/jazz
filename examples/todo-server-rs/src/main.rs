//! Todo Server - Example backend using jazz-tools.
//!
//! Demonstrates a simple todo list API backed by Jazz for local persistence
//! and server sync.
//!
//! # Running
//!
//! ```bash
//! # First, create an app and start the Jazz server
//! jazz-tools create app --name todo-app
//! jazz-tools server <APP_ID> --port 1625
//!
//! # Then run the todo backend
//! cargo run -p todo-server
//! ```
//!
//! # API
//!
//! | Route | Method | Description |
//! |-------|--------|-------------|
//! | `/todos` | GET | List all todo items |
//! | `/todos` | POST | Create new item |
//! | `/todos/:id` | PUT | Update item |
//! | `/todos/:id` | DELETE | Delete item |
//! | `/updates` | GET | SSE stream of add/remove events |

mod routes;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use axum::Router;
use jazz_tools::{AppContext, AppId, JazzClient, Schema};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use routes::Todo;

/// Application state shared across request handlers.
pub struct AppState {
    pub client: JazzClient,
    /// Broadcast channel for SSE updates. Sends the full list of todos.
    pub sse_tx: broadcast::Sender<Vec<Todo>>,
}

fn load_schema_from_cli(schema_dir: &str) -> Result<Schema, Box<dyn std::error::Error>> {
    let jazz_tools_bin = std::env::var("JAZZ_TOOLS_BIN").unwrap_or_else(|_| "jazz-tools".into());
    let output = Command::new(jazz_tools_bin)
        .args([
            "schema",
            "export",
            "--schema-dir",
            schema_dir,
            "--format",
            "json",
        ])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("schema export failed: {stderr}").into());
    }

    Ok(serde_json::from_slice(&output.stdout)?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("todo_server=info".parse().unwrap())
                .add_directive("jazz_rs=debug".parse().unwrap()),
        )
        .init();

    // Configuration from environment or defaults
    let app_id = std::env::var("JAZZ_APP_ID").unwrap_or_else(|_| "todo-app".to_string());
    let server_url =
        std::env::var("JAZZ_SERVER_URL").unwrap_or_else(|_| "http://localhost:1625".to_string());
    let data_dir = std::env::var("TODO_DATA_DIR").unwrap_or_else(|_| "./todo-data".to_string());
    let port: u16 = std::env::var("TODO_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

    info!("Starting todo server");
    info!("App ID: {}", app_id);
    info!("Jazz server: {}", server_url);
    info!("Data directory: {}", data_dir);

    let schema_root = env!("CARGO_MANIFEST_DIR");
    let schema = load_schema_from_cli(schema_root)?;
    info!(
        "Loaded schema from {schema_root}/schema.ts ({} tables)",
        schema.len()
    );

    // Create Jazz client
    let context = AppContext {
        app_id: AppId::from_name(&app_id),
        client_id: None,
        schema,
        server_url,
        data_dir: PathBuf::from(data_dir),
        jwt_token: None,
        backend_secret: None,
        admin_secret: None,
    };

    let client = JazzClient::connect(context).await?;
    info!("Connected to Jazz");

    // Create broadcast channel for SSE updates
    let (sse_tx, _) = broadcast::channel::<Vec<Todo>>(16);

    // Build application state
    let state = Arc::new(AppState { client, sse_tx });

    // Build router
    let app = Router::new()
        .nest("/", routes::create_router())
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
