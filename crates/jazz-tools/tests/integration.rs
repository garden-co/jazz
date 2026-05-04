#![cfg(feature = "test")]

//! E2E integration tests for jazz-tools server.
//!
//! These tests spawn the actual `jazz-tools` binary and interact via HTTP
//! and WebSocket with binary length-prefixed frames.

use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use futures::{SinkExt as _, StreamExt as _};
use jazz_tools::sync_manager::ClientId;
use jazz_tools::transport_manager::{
    AuthConfig, AuthHandshake, ConnectedResponse, SYNC_PROTOCOL_VERSION,
};
use reqwest::Client;
use tempfile::TempDir;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const TEST_APP_ID: &str = "00000000-0000-0000-0000-000000000001";

fn mint_test_token(audience: &str) -> String {
    let seed = [42u8; 32];
    jazz_tools::identity::mint_jazz_self_signed_token(
        &seed,
        jazz_tools::identity::LOCAL_FIRST_ISSUER,
        audience,
        3600,
    )
    .unwrap()
}

/// Encode a 4-byte big-endian length-prefixed frame.
fn frame_encode(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode a 4-byte big-endian length-prefixed frame, returning the payload slice.
fn frame_decode(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + len {
        return None;
    }
    Some(&data[4..4 + len])
}

/// Perform a WS handshake against `ws://host/apps/<appId>/ws` using a local-first JWT token.
///
/// Returns `Ok(ConnectedResponse)` on success, or `Err(message)` on failure.
async fn ws_handshake(port: u16, jwt_token: &str) -> Result<ConnectedResponse, String> {
    let ws_url = format!("ws://127.0.0.1:{port}/apps/{TEST_APP_ID}/ws");
    let (mut ws, _) = connect_async(&ws_url)
        .await
        .map_err(|e| format!("ws connect failed: {e}"))?;

    let handshake = AuthHandshake {
        sync_protocol_version: SYNC_PROTOCOL_VERSION,
        client_id: ClientId::new().to_string(),
        auth: AuthConfig {
            jwt_token: Some(jwt_token.to_string()),
            ..Default::default()
        },
        catalogue_state_hash: None,
        declared_schema_hash: None,
    };
    let payload = serde_json::to_vec(&handshake).expect("serialize AuthHandshake");
    ws.send(Message::Binary(frame_encode(&payload).into()))
        .await
        .map_err(|e| format!("ws send failed: {e}"))?;

    match ws.next().await {
        Some(Ok(Message::Binary(bytes))) => {
            let inner = frame_decode(&bytes).ok_or("malformed response frame")?;
            if let Ok(connected) = serde_json::from_slice::<ConnectedResponse>(inner) {
                return Ok(connected);
            }
            let msg = serde_json::from_slice::<serde_json::Value>(inner)
                .ok()
                .and_then(|v| {
                    v.get("message")
                        .and_then(|m| m.as_str())
                        .map(str::to_string)
                })
                .unwrap_or_else(|| "auth rejected".to_string());
            Err(msg)
        }
        Some(Ok(Message::Close(_))) | None => Err("server closed connection".to_string()),
        Some(Ok(other)) => Err(format!("unexpected WS message: {other:?}")),
        Some(Err(e)) => Err(format!("ws recv error: {e}")),
    }
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
async fn test_ws_connection_receives_connected_response() {
    let server = TestServer::start(0).await;

    let token = mint_test_token("00000000-0000-0000-0000-000000000001");
    let resp = tokio::time::timeout(Duration::from_secs(5), ws_handshake(server.port, &token))
        .await
        .expect("timeout waiting for ConnectedResponse")
        .expect("WS handshake failed");

    assert!(
        !resp.connection_id.is_empty(),
        "connection_id should be non-empty"
    );
    assert!(!resp.client_id.is_empty(), "client_id should be non-empty");
    assert!(
        resp.catalogue_state_hash.is_some(),
        "ConnectedResponse should include catalogue_state_hash"
    );
}

#[tokio::test]
async fn test_ws_connection_stays_open_after_handshake() {
    let server = TestServer::start(0).await;

    let token = mint_test_token("00000000-0000-0000-0000-000000000001");
    let ws_url = format!("ws://127.0.0.1:{}/apps/{TEST_APP_ID}/ws", server.port);
    let (mut ws, _) = connect_async(&ws_url).await.expect("ws connect");

    let handshake = AuthHandshake {
        sync_protocol_version: SYNC_PROTOCOL_VERSION,
        client_id: ClientId::new().to_string(),
        auth: AuthConfig {
            jwt_token: Some(token),
            ..Default::default()
        },
        catalogue_state_hash: None,
        declared_schema_hash: None,
    };
    let payload = serde_json::to_vec(&handshake).expect("serialize AuthHandshake");
    ws.send(Message::Binary(frame_encode(&payload).into()))
        .await
        .expect("ws send handshake");

    // Read the ConnectedResponse frame.
    let first = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout waiting for ConnectedResponse")
        .expect("stream ended")
        .expect("ws recv error");
    assert!(
        matches!(first, Message::Binary(_)),
        "expected Binary ConnectedResponse frame"
    );

    // Drain frames for 100ms, confirming the connection stays open the whole time.
    // The server may push SyncUpdate frames (e.g. initial catalogue sync) immediately
    // after the handshake — those are expected and should not fail this test.
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

#[tokio::test]
async fn test_ws_handshake_returns_valid_connection_id() {
    let server = TestServer::start(0).await;

    let token = mint_test_token("00000000-0000-0000-0000-000000000001");
    let resp = tokio::time::timeout(Duration::from_secs(5), ws_handshake(server.port, &token))
        .await
        .expect("timeout waiting for ConnectedResponse")
        .expect("WS handshake failed");

    // connection_id is a non-empty String UUID assigned by the server.
    assert!(
        !resp.connection_id.is_empty(),
        "server should assign a valid connection_id"
    );
    // client_id echoed back must match the UUID format (non-empty string).
    assert!(
        !resp.client_id.is_empty(),
        "server should echo back the client_id"
    );
}
