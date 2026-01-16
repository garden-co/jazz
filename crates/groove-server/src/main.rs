//! Sync server binary using hyper.
//!
//! A standalone HTTP server for Jazz sync protocol.
//! Runs single-threaded for simplicity and compatibility with Rc-based internals.
//!
//! Usage:
//!   cargo run -p groove-server -- [OPTIONS]
//!
//! Options:
//!   --config FILE   Load configuration from TOML file
//!   --host HOST     Host to bind to (default: 0.0.0.0)
//!   --port PORT     Port to listen on (default: 8080)

use std::net::SocketAddr;
use std::rc::Rc;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::task::LocalSet;

use groove::MemoryEnvironment;
use groove::sync::jwt::JwtTokenValidator;
use groove::sync::{AcceptAllTokens, TokenValidator};
use groove_server::{
    AppState, AuthProvider, InMemoryUserResolver, ProvisioningTokenValidator, ServerConfig,
    SyncResponse, handle_request,
};

/// Create a token validator based on the server configuration.
fn create_token_validator(config: &ServerConfig) -> Rc<dyn TokenValidator> {
    let resolver = InMemoryUserResolver::new();
    let auto_provision = config.auth.provisioning.auto_provision;

    match config.auth.provider {
        AuthProvider::AcceptAll => {
            if auto_provision {
                Rc::new(ProvisioningTokenValidator::with_auto_provision(
                    AcceptAllTokens,
                    resolver,
                ))
            } else {
                Rc::new(AcceptAllTokens)
            }
        }
        AuthProvider::BetterAuth | AuthProvider::WorkOS | AuthProvider::Jwt => {
            let jwt_config = config.auth.jwt.to_jwt_config();
            let jwt_validator = JwtTokenValidator::new(jwt_config);

            if auto_provision {
                Rc::new(ProvisioningTokenValidator::with_auto_provision(
                    jwt_validator,
                    resolver,
                ))
            } else {
                Rc::new(ProvisioningTokenValidator::without_auto_provision(
                    jwt_validator,
                    resolver,
                ))
            }
        }
    }
}

fn main() {
    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let mut config_path: Option<String> = None;
    let mut host_override: Option<String> = None;
    let mut port_override: Option<u16> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" | "-c" => {
                if i + 1 < args.len() {
                    config_path = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: --config requires a file path");
                    std::process::exit(1);
                }
            }
            "--host" => {
                if i + 1 < args.len() {
                    host_override = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: --host requires an argument");
                    std::process::exit(1);
                }
            }
            "--port" | "-p" => {
                if i + 1 < args.len() {
                    port_override = Some(args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Error: --port must be a number");
                        std::process::exit(1);
                    }));
                    i += 2;
                } else {
                    eprintln!("Error: --port requires an argument");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => {
                println!("Jazz Sync Server");
                println!();
                println!("Usage: groove-server [OPTIONS]");
                println!();
                println!("Options:");
                println!("  --config, -c FILE   Load configuration from TOML file");
                println!("  --host HOST         Host to bind to (default: 0.0.0.0)");
                println!("  --port, -p PORT     Port to listen on (default: 8080)");
                println!("  --help, -h          Show this help message");
                println!();
                println!("Configuration:");
                println!("  The server looks for groove-server.toml in the current directory");
                println!("  if no --config option is specified.");
                std::process::exit(0);
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                eprintln!("Use --help for usage information");
                std::process::exit(1);
            }
        }
    }

    // Load configuration
    let mut config = if let Some(path) = config_path {
        ServerConfig::from_file(&path).unwrap_or_else(|e| {
            eprintln!("Error loading config from '{}': {}", path, e);
            std::process::exit(1);
        })
    } else {
        ServerConfig::load()
    };

    // Apply command-line overrides
    if let Some(host) = host_override {
        config.host = host;
    }
    if let Some(port) = port_override {
        config.port = port;
    }

    // Create single-threaded runtime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Run the server in a LocalSet for spawn_local support
    rt.block_on(async {
        let local = LocalSet::new();
        local.run_until(run_server(config)).await;
    });
}

async fn run_server(config: ServerConfig) {
    // Create in-memory environment
    let env = Rc::new(MemoryEnvironment::new());

    // Create token validator
    let token_validator = create_token_validator(&config);

    // Create app state
    let state = Rc::new(AppState::new(env, token_validator));

    // Bind to address
    let addr: SocketAddr = config.socket_addr().parse().unwrap_or_else(|_| {
        eprintln!("Error: Invalid address {}", config.socket_addr());
        std::process::exit(1);
    });

    let listener = TcpListener::bind(addr).await.unwrap_or_else(|e| {
        eprintln!("Error binding to {}: {}", addr, e);
        std::process::exit(1);
    });

    println!("Jazz Sync Server starting on http://{}", addr);
    println!("Auth provider: {:?}", config.auth.provider);
    if config.auth.provisioning.auto_provision {
        println!("Auto-provisioning: enabled");
    }
    println!();
    println!("Endpoints:");
    println!("  POST /sync/subscribe   - Subscribe to a query (SSE stream)");
    println!("  POST /sync/unsubscribe - Unsubscribe from a query");
    println!("  POST /sync/push        - Push commits for an object");
    println!("  POST /sync/reconcile   - Request full reconciliation");
    println!("  GET  /sync/events      - SSE event stream");

    // Accept connections
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("Accept error: {}", e);
                continue;
            }
        };

        let io = TokioIo::new(stream);
        let state = Rc::clone(&state);

        // Handle connection in a local task (not spawned across threads)
        tokio::task::spawn_local(async move {
            let service = service_fn(|req| {
                let state = Rc::clone(&state);
                async move {
                    let response = handle_request(state, req).await;
                    // Convert SyncResponse to a type hyper can use
                    match response {
                        SyncResponse::Regular(resp) => Ok::<_, std::convert::Infallible>(
                            resp.map(|b| http_body_util::Either::Left(b)),
                        ),
                        SyncResponse::Sse(resp) => {
                            Ok(resp.map(|b| http_body_util::Either::Right(b)))
                        }
                    }
                }
            });

            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}
