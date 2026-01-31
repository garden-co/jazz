//! Todo Server - Example backend using jazz-rs.
//!
//! Demonstrates a simple todo list API backed by Jazz for local persistence
//! and server sync.
//!
//! # Running
//!
//! ```bash
//! # First, create an app and start the Jazz server
//! jazz create app --name todo-app
//! jazz server <APP_ID> --port 1625
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
use std::sync::Arc;

use axum::Router;
use jazz_rs::{AppContext, AppId, ColumnType, JazzClient, SchemaBuilder, TableSchema};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

/// Application state shared across request handlers.
pub struct AppState {
    pub client: JazzClient,
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
    let app_name = std::env::var("TODO_APP_NAME").unwrap_or_else(|_| "todo-app".to_string());
    let server_url =
        std::env::var("JAZZ_SERVER_URL").unwrap_or_else(|_| "http://localhost:1625".to_string());
    let data_dir = std::env::var("TODO_DATA_DIR").unwrap_or_else(|_| "./todo-data".to_string());
    let port: u16 = std::env::var("TODO_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

    info!("Starting todo server");
    info!("App name: {}", app_name);
    info!("Jazz server: {}", server_url);
    info!("Data directory: {}", data_dir);

    // Define schema for todos
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build();

    // Create Jazz client
    let context = AppContext {
        app_id: AppId::from_name(&app_name),
        client_id: None,
        schema,
        server_url,
        data_dir: PathBuf::from(data_dir),
    };

    let client = JazzClient::connect(context).await?;
    info!("Connected to Jazz");

    // Build application state
    let state = Arc::new(AppState { client });

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
