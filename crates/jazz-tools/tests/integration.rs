#![cfg(feature = "test")]

//! E2E integration tests for jazz-tools server.
//!
//! These tests spawn the actual `jazz-tools` binary and interact via HTTP
//! and WebSocket with binary transport frames.

use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use futures::{SinkExt as _, StreamExt as _};
use jazz::wire::{
    FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD, WireFrame, WireHello, WirePeerRole,
    decode_frame, encode_frame,
};
use jazz_tools::transport_auth::AuthConfig;
use jazz_tools::{ColumnType, SchemaBuilder, TableSchema};
use reqwest::Client;
use serde_json::json;
use tempfile::TempDir;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const TEST_APP_ID: &str = "00000000-0000-0000-0000-000000000001";
const TEST_ADMIN_SECRET: &str = "test-admin-secret";
const TEST_SEED: [u8; 32] = [42u8; 32];
const DIRECT_WS_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS;

fn mint_test_token(audience: &str) -> String {
    jazz_tools::identity::mint_jazz_self_signed_token(
        &TEST_SEED,
        jazz_tools::identity::LOCAL_FIRST_ISSUER,
        audience,
        3600,
    )
    .unwrap()
}

fn test_peer_identity() -> String {
    let user_id = jazz_tools::identity::derive_user_id(&TEST_SEED);
    hex::encode(user_id.as_bytes())
}

fn direct_ws_prelude(jwt_token: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "peer_identity": test_peer_identity(),
        "auth": AuthConfig {
            jwt_token: Some(jwt_token.to_string()),
            ..Default::default()
        },
    }))
    .expect("serialize direct ws prelude")
}

fn direct_ws_client_hello_batch() -> Vec<u8> {
    let hello = WireFrame::Hello(WireHello::current(WirePeerRole::Client, DIRECT_WS_FEATURES));
    let encoded = vec![encode_frame(&hello).expect("encode direct client hello")];
    postcard::to_allocvec(&encoded).expect("encode direct hello batch")
}

async fn direct_ws_handshake(
    port: u16,
    jwt_token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let ws_url = format!("ws://127.0.0.1:{port}/apps/{TEST_APP_ID}/ws");
    let (mut ws, _) = connect_async(&ws_url).await.expect("connect direct ws");

    ws.send(Message::Binary(direct_ws_prelude(jwt_token).into()))
        .await
        .expect("send direct ws auth prelude");
    ws.send(Message::Binary(direct_ws_client_hello_batch().into()))
        .await
        .expect("send direct ws hello");

    ws
}

async fn expect_direct_server_hello(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout waiting for direct server hello")
        .expect("stream ended")
        .expect("ws recv error");
    let Message::Binary(response) = response else {
        panic!("expected binary direct server hello, got {response:?}");
    };
    let frames: Vec<Vec<u8>> =
        postcard::from_bytes(&response).expect("decode direct ws response batch");
    assert_eq!(frames.len(), 1);
    let WireFrame::Hello(server_hello) =
        decode_frame(&frames[0]).expect("decode direct server hello")
    else {
        panic!("expected direct server hello");
    };
    assert_eq!(server_hello.role, WirePeerRole::Core);
    assert_eq!(server_hello.features, DIRECT_WS_FEATURES);
}

async fn publish_test_schema(server: &TestServer) {
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("todos").column("title", ColumnType::Text))
        .build();
    let response = Client::new()
        .post(format!(
            "{}/apps/{TEST_APP_ID}/admin/schemas",
            server.base_url()
        ))
        .header("X-Jazz-Admin-Secret", TEST_ADMIN_SECRET)
        .json(&json!({ "schema": schema, "permissions": null }))
        .send()
        .await
        .expect("publish test schema");
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
}

/// Test server handle - kills process on drop.
struct TestServer {
    process: Child,
    port: u16,
    bound_port_file: PathBuf,
    #[allow(dead_code)]
    data_dir: TempDir,
    configured_data_dir: PathBuf,
}

impl TestServer {
    /// Start a test server on the given port.
    async fn start(port: u16) -> Self {
        let data_dir = TempDir::new().expect("create temp dir");
        let configured_data_dir = data_dir.path().to_path_buf();
        let bound_port_file = data_dir.path().join("bound-port");

        // Use a deterministic UUID app ID for testing
        let jazz_binary = Self::find_jazz_binary();

        let process = Command::new(&jazz_binary)
            .args([
                "server",
                TEST_APP_ID,
                "--port",
                &port.to_string(),
                "--data-dir",
                configured_data_dir.to_str().unwrap(),
                "--admin-secret",
                TEST_ADMIN_SECRET,
            ])
            .env("JAZZ_BOUND_PORT_FILE", &bound_port_file)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to spawn jazz-tools server at {:?}: {}",
                    jazz_binary, e
                )
            });

        let mut server = Self {
            process,
            port,
            bound_port_file,
            data_dir,
            configured_data_dir,
        };

        // Wait for server to be ready
        server.wait_ready().await;

        server
    }

    /// Start a test server with the CLI `--in-memory` flag enabled.
    async fn start_in_memory(port: u16) -> Self {
        let data_dir = TempDir::new().expect("create temp dir");
        let configured_data_dir = data_dir.path().join("should-not-exist");
        let bound_port_file = data_dir.path().join("bound-port");

        let jazz_binary = Self::find_jazz_binary();

        let process = Command::new(&jazz_binary)
            .args([
                "server",
                TEST_APP_ID,
                "--port",
                &port.to_string(),
                "--data-dir",
                configured_data_dir.to_str().unwrap(),
                "--in-memory",
                "--admin-secret",
                TEST_ADMIN_SECRET,
            ])
            .env("JAZZ_BOUND_PORT_FILE", &bound_port_file)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to spawn jazz-tools server at {:?}: {}",
                    jazz_binary, e
                )
            });

        let mut server = Self {
            process,
            port,
            bound_port_file,
            data_dir,
            configured_data_dir,
        };

        server.wait_ready().await;
        server
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn find_jazz_binary() -> PathBuf {
        // Get the path to the test binary, which gives us the target directory.
        let exe = std::env::current_exe().expect("get current exe");
        let target_dir = exe
            .parent() // deps
            .and_then(|p| p.parent()) // debug or release
            .expect("find target dir");

        let jazz_path = target_dir.join("jazz-tools");
        if jazz_path.exists() {
            return jazz_path;
        }

        panic!(
            "jazz-tools binary not found at {:?}. Run `cargo build -p jazz-tools --bin jazz-tools --features cli` first.",
            jazz_path
        );
    }

    async fn wait_ready(&mut self) {
        let client = Client::new();

        for i in 0..200 {
            self.maybe_update_bound_port();
            if let Some(status) = self.process.try_wait().expect("poll jazz-tools server") {
                panic!(
                    "jazz-tools server exited before becoming ready: {status}{}",
                    self.process_output_summary()
                );
            }
            if self.port != 0 {
                let url = format!("{}/health", self.base_url());
                match client.get(&url).send().await {
                    Ok(_) => return,
                    Err(e) => {
                        if i == 199 {
                            eprintln!("Last error: {:?}", e);
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "Server failed to become ready within 20 seconds{}",
            self.process_output_summary()
        );
    }

    fn maybe_update_bound_port(&mut self) {
        let Ok(contents) = std::fs::read_to_string(&self.bound_port_file) else {
            return;
        };
        let Ok(port) = contents.trim().parse::<u16>() else {
            return;
        };
        self.port = port;
    }

    fn process_output_summary(&mut self) -> String {
        let stdout = take_pipe_text(&mut self.process.stdout);
        let stderr = take_pipe_text(&mut self.process.stderr);
        if stdout.is_empty() && stderr.is_empty() {
            return String::new();
        }

        format!(
            "\nstdout:\n{}\nstderr:\n{}",
            if stdout.is_empty() {
                "<empty>"
            } else {
                stdout.trim()
            },
            if stderr.is_empty() {
                "<empty>"
            } else {
                stderr.trim()
            }
        )
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn take_pipe_text<T: Read>(pipe: &mut Option<T>) -> String {
    let Some(mut pipe) = pipe.take() else {
        return String::new();
    };

    let mut bytes = Vec::new();
    let _ = pipe.read_to_end(&mut bytes);
    String::from_utf8_lossy(&bytes).into_owned()
}

#[tokio::test]
async fn test_server_health_check() {
    let server = TestServer::start(0).await;

    let client = Client::new();
    let resp = client
        .get(format!("{}/health", server.base_url()))
        .send()
        .await
        .expect("health check");

    assert!(resp.status().is_success());

    let body: serde_json::Value = resp.json().await.expect("parse json");
    assert_eq!(body["status"], "healthy");
}

#[tokio::test]
async fn test_server_health_check_in_memory_does_not_create_data_dir() {
    let server = TestServer::start_in_memory(0).await;

    let client = Client::new();
    let resp = client
        .get(format!("{}/health", server.base_url()))
        .send()
        .await
        .expect("health check");

    assert!(resp.status().is_success());
    assert!(
        !server.configured_data_dir.exists(),
        "--in-memory should not create the configured data directory"
    );
}

#[tokio::test]
async fn test_ws_connection_receives_server_hello() {
    let server = TestServer::start(0).await;
    publish_test_schema(&server).await;

    let token = mint_test_token("00000000-0000-0000-0000-000000000001");
    let mut ws = direct_ws_handshake(server.port, &token).await;

    expect_direct_server_hello(&mut ws).await;
}

#[tokio::test]
async fn test_ws_connection_stays_open_after_handshake() {
    let server = TestServer::start(0).await;
    publish_test_schema(&server).await;

    let token = mint_test_token("00000000-0000-0000-0000-000000000001");
    let mut ws = direct_ws_handshake(server.port, &token).await;

    expect_direct_server_hello(&mut ws).await;

    // Drain frames for 100ms, confirming the connection stays open the whole time.
    // The server may push direct WireFrame batches immediately after negotiation;
    // those are expected and should not fail this test.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Err(_timeout) => break, // deadline reached — connection stayed open
            Ok(Some(Ok(Message::Binary(_)))) => { /* catalogue sync or other update — expected */
            }
            Ok(Some(Ok(Message::Ping(_)))) | Ok(Some(Ok(Message::Pong(_)))) => { /* keep-alive */ }
            Ok(Some(Ok(Message::Close(_)))) => panic!("connection closed unexpectedly"),
            Ok(Some(Ok(other))) => panic!("unexpected WS frame: {other:?}"),
            Ok(Some(Err(e))) => panic!("ws error: {e}"),
            Ok(None) => panic!("ws stream ended unexpectedly"),
        }
    }
}
