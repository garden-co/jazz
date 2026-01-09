//! Sync server binary.
//!
//! A standalone HTTP server for Jazz sync protocol.
//!
//! Usage:
//!   cargo run --bin sync_server --features sync-server -- [OPTIONS]
//!
//! Options:
//!   --host HOST     Host to bind to (default: 127.0.0.1)
//!   --port PORT     Port to listen on (default: 8080)

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use tokio::net::TcpListener;

use groove::MemoryEnvironment;
use groove::sync::{sync_router, AcceptAllTokens, AppState};

#[tokio::main]
async fn main() {
    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 8080;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--host" => {
                if i + 1 < args.len() {
                    host = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: --host requires an argument");
                    std::process::exit(1);
                }
            }
            "--port" => {
                if i + 1 < args.len() {
                    port = args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Error: --port must be a number");
                        std::process::exit(1);
                    });
                    i += 2;
                } else {
                    eprintln!("Error: --port requires an argument");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => {
                println!("Jazz Sync Server");
                println!();
                println!("Usage: sync_server [OPTIONS]");
                println!();
                println!("Options:");
                println!("  --host HOST     Host to bind to (default: 127.0.0.1)");
                println!("  --port PORT     Port to listen on (default: 8080)");
                println!("  --help, -h      Show this help message");
                std::process::exit(0);
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                eprintln!("Use --help for usage information");
                std::process::exit(1);
            }
        }
    }

    // Create in-memory environment (for MVP - production would use persistent storage)
    let env = Arc::new(MemoryEnvironment::new());

    // Accept all tokens for MVP (production would validate against auth service)
    let token_validator = Arc::new(AcceptAllTokens);

    // Create app state
    let state = Arc::new(AppState::new(env, token_validator));

    // Build router
    let app: Router = sync_router().with_state(state);

    // Bind and serve
    let addr: SocketAddr = format!("{}:{}", host, port).parse().unwrap_or_else(|_| {
        eprintln!("Error: Invalid address {}:{}", host, port);
        std::process::exit(1);
    });

    println!("Jazz Sync Server starting on http://{}", addr);
    println!("Endpoints:");
    println!("  POST /sync/subscribe   - Subscribe to a query (SSE stream)");
    println!("  POST /sync/unsubscribe - Unsubscribe from a query");
    println!("  POST /sync/push        - Push commits for an object");
    println!("  POST /sync/reconcile   - Request full reconciliation");

    let listener = TcpListener::bind(addr).await.unwrap_or_else(|e| {
        eprintln!("Error binding to {}: {}", addr, e);
        std::process::exit(1);
    });

    axum::serve(listener, app).await.unwrap_or_else(|e| {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    });
}
