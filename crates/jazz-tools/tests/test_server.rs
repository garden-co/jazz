#![cfg(feature = "cli")]

//! Test server infrastructure for self-spawning integration tests.
//!
//! Spawns the jazz-tools binary as a subprocess and waits for it to become ready.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use axum::{Json, Router, extract::State, routing::get};
use base64::Engine;
use serde_json::{Value, json};
use tempfile::TempDir;

const JWT_SECRET: &str = "test-jwt-secret-for-integration";
const JWT_KID: &str = "test-jwks-kid";

#[derive(Clone)]
struct JwksState {
    kid: String,
    secret_b64: String,
}

struct JwksServer {
    task: tokio::task::JoinHandle<()>,
    url: String,
}

impl JwksServer {
    async fn start(kid: &str, secret: &str) -> Self {
        let state = JwksState {
            kid: kid.to_string(),
            secret_b64: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret.as_bytes()),
        };

        let app = Router::new()
            .route("/jwks", get(jwks_handler))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind JWKS server");
        let addr = listener.local_addr().expect("JWKS local addr");
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve JWKS");
        });

        Self {
            task,
            url: format!("http://{addr}/jwks"),
        }
    }
}

impl Drop for JwksServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Test server handle - kills process on drop.
pub struct TestServer {
    process: Child,
    pub port: u16,
    #[allow(dead_code)]
    data_dir: TempDir,
    #[allow(dead_code)]
    jwks_server: JwksServer,
}

impl TestServer {
    /// Start a test server on a free port.
    pub async fn start() -> Self {
        let port = get_free_port();
        Self::start_on_port(port).await
    }

    /// Start a test server on the given port.
    pub async fn start_on_port(port: u16) -> Self {
        let data_dir = TempDir::new().expect("create temp dir");
        let jwks_server = JwksServer::start(JWT_KID, JWT_SECRET).await;

        // Use a deterministic UUID app ID for testing
        let app_id = "00000000-0000-0000-0000-000000000001";

        let jazz_binary = Self::find_jazz_binary();

        let process = Command::new(&jazz_binary)
            .args([
                "server",
                app_id,
                "--port",
                &port.to_string(),
                "--data-dir",
                data_dir.path().to_str().unwrap(),
            ])
            .env(
                "JAZZ_BACKEND_SECRET",
                "backend-secret-for-integration-tests",
            )
            .env("JAZZ_ADMIN_SECRET", "admin-secret-for-integration-tests")
            .env("JAZZ_JWKS_URL", &jwks_server.url)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to spawn jazz server at {:?}: {}", jazz_binary, e));

        let server = Self {
            process,
            port,
            data_dir,
            jwks_server,
        };

        // Wait for server to be ready
        server.wait_ready().await;

        server
    }

    /// Find the jazz-tools binary in cargo's target directory.
    fn find_jazz_binary() -> PathBuf {
        // Get the path to the test binary, which gives us the target directory
        let exe = std::env::current_exe().expect("get current exe");
        let target_dir = exe
            .parent() // deps
            .and_then(|p| p.parent()) // debug or release
            .expect("find target dir");

        let jazz_path = target_dir.join("jazz-tools");
        if jazz_path.exists() {
            return jazz_path;
        }

        // Try building if not found (useful for first run)
        panic!(
            "jazz binary not found at {:?}. Run `cargo build -p jazz-tools --bin jazz-tools --features cli` first.",
            jazz_path
        );
    }

    /// Get the base URL for this server.
    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Wait for the server to become ready by polling /health.
    async fn wait_ready(&self) {
        let client = reqwest::Client::new();
        let url = format!("{}/health", self.base_url());

        for i in 0..50 {
            match client.get(&url).send().await {
                Ok(_) => return,
                Err(e) => {
                    if i == 49 {
                        eprintln!("Last error: {:?}", e);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("Server failed to become ready within 5 seconds");
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// Find an available port by binding to port 0.
fn get_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind to port 0");
    listener.local_addr().unwrap().port()
}

async fn jwks_handler(State(state): State<JwksState>) -> Json<Value> {
    Json(json!({
        "keys": [
            {
                "kty": "oct",
                "kid": state.kid,
                "alg": "HS256",
                "k": state.secret_b64,
            }
        ]
    }))
}
