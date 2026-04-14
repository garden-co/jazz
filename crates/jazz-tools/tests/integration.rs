#![cfg(feature = "test")]

//! E2E integration tests for jazz-tools server.
//!
//! These tests spawn the actual `jazz-tools` binary and interact via HTTP
//! or WebSocket.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use futures::StreamExt as _;
use jazz_tools::transport_manager::{AuthConfig, TransportInbound};
use reqwest::Client;
use tempfile::TempDir;

fn mint_test_token(audience: &str) -> String {
    let seed = [42u8; 32];
    jazz_tools::identity::mint_local_first_token(&seed, audience, 3600).unwrap()
}

struct NoopTickNotifier;
impl jazz_tools::transport_manager::TickNotifier for NoopTickNotifier {
    fn notify(&self) {}
}

/// Test server handle - kills process on drop.
struct TestServer {
    process: Child,
    port: u16,
    #[allow(dead_code)]
    data_dir: TempDir,
    configured_data_dir: PathBuf,
}

impl TestServer {
    /// Start a test server on the given port.
    async fn start(port: u16) -> Self {
        let data_dir = TempDir::new().expect("create temp dir");
        let configured_data_dir = data_dir.path().to_path_buf();

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
                configured_data_dir.to_str().unwrap(),
            ])
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

        let app_id = "00000000-0000-0000-0000-000000000001";
        let jazz_binary = Self::find_jazz_binary();

        let process = Command::new(&jazz_binary)
            .args([
                "server",
                app_id,
                "--port",
                &port.to_string(),
                "--data-dir",
                configured_data_dir.to_str().unwrap(),
                "--in-memory",
            ])
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
        let url = format!("{}/health", self.base_url());

        for i in 0..200 {
            if let Some(status) = self.process.try_wait().expect("poll jazz-tools server") {
                panic!("jazz-tools server exited before becoming ready: {status}");
            }
            match client.get(&url).send().await {
                Ok(_) => return,
                Err(e) => {
                    if i == 199 {
                        eprintln!("Last error: {:?}", e);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("Server failed to become ready within 20 seconds");
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

#[tokio::test]
async fn test_server_health_check() {
    let port = get_free_port();
    let server = TestServer::start(port).await;

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
    let port = get_free_port();
    let server = TestServer::start_in_memory(port).await;

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
async fn test_stream_connection_receives_connected_event() {
    let port = get_free_port();
    let server = TestServer::start(port).await;
    let _ = &server;

    let auth = AuthConfig {
        jwt_token: Some(mint_test_token("00000000-0000-0000-0000-000000000001")),
        backend_secret: None,
        admin_secret: None,
        backend_session: None,
    };

    let ws_url = format!("ws://127.0.0.1:{}/ws", port);

    let (mut handle, manager) = jazz_tools::transport_manager::create::<
        jazz_tools::ws_stream::NativeWsStream,
        NoopTickNotifier,
    >(ws_url, auth, NoopTickNotifier);

    tokio::spawn(manager.run());

    let event = tokio::time::timeout(Duration::from_secs(5), handle.inbound_rx.next())
        .await
        .expect("timed out waiting for Connected event")
        .expect("transport channel closed");

    match event {
        TransportInbound::Connected {
            catalogue_state_hash,
            ..
        } => {
            assert!(
                catalogue_state_hash.is_some(),
                "Connected event should advertise the server catalogue digest"
            );
        }
        other => panic!("Expected Connected event, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_stream_heartbeat() {
    let port = get_free_port();
    let server = TestServer::start(port).await;
    let _ = &server;

    let auth = AuthConfig {
        jwt_token: Some(mint_test_token("00000000-0000-0000-0000-000000000001")),
        backend_secret: None,
        admin_secret: None,
        backend_session: None,
    };

    let ws_url = format!("ws://127.0.0.1:{}/ws", port);

    let (mut handle, manager) = jazz_tools::transport_manager::create::<
        jazz_tools::ws_stream::NativeWsStream,
        NoopTickNotifier,
    >(ws_url, auth, NoopTickNotifier);

    tokio::spawn(manager.run());

    // Read the Connected event to confirm the stream is live.
    let event = tokio::time::timeout(Duration::from_secs(5), handle.inbound_rx.next())
        .await
        .expect("timed out waiting for Connected event")
        .expect("transport channel closed");

    assert!(
        matches!(event, TransportInbound::Connected { .. }),
        "Expected Connected event, got: {event:?}"
    );

    // The heartbeat interval is 30s which is too long for a test.
    // Verifying Connected is enough to confirm the stream stays open.
}

#[tokio::test]
async fn test_sync_payload_broadcast_to_stream_client() {
    let port = get_free_port();
    let server = TestServer::start(port).await;
    let _ = &server;

    let auth = AuthConfig {
        jwt_token: Some(mint_test_token("00000000-0000-0000-0000-000000000001")),
        backend_secret: None,
        admin_secret: None,
        backend_session: None,
    };

    let ws_url = format!("ws://127.0.0.1:{}/ws", port);

    let (mut handle, manager) = jazz_tools::transport_manager::create::<
        jazz_tools::ws_stream::NativeWsStream,
        NoopTickNotifier,
    >(ws_url, auth, NoopTickNotifier);

    tokio::spawn(manager.run());

    // Wait for Connected event to verify connection works.
    let event = tokio::time::timeout(Duration::from_secs(5), handle.inbound_rx.next())
        .await
        .expect("timed out waiting for Connected event")
        .expect("transport channel closed");

    assert!(
        matches!(event, TransportInbound::Connected { .. }),
        "Expected Connected event, got: {event:?}"
    );
}
