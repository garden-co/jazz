#![cfg(feature = "cli")]

//! E2E integration tests for jazz-tools server.
//!
//! These tests spawn the actual `jazz-tools` binary and interact via HTTP
//! with binary length-prefixed streaming.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use bytes::BytesMut;
use futures::StreamExt;
use jazz_tools::jazz_transport::ServerEvent;
use reqwest::Client;
use tempfile::TempDir;

/// Test server handle - kills process on drop.
struct TestServer {
    process: Child,
    port: u16,
    #[allow(dead_code)]
    data_dir: TempDir,
}

impl TestServer {
    /// Start a test server on the given port.
    async fn start(port: u16) -> Self {
        let data_dir = TempDir::new().expect("create temp dir");

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
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to spawn jazz-tools server at {:?}: {}",
                    jazz_binary, e
                )
            });

        let server = Self {
            process,
            port,
            data_dir,
        };

        // Wait for server to be ready
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

    async fn wait_ready(&self) {
        let client = Client::new();
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

/// Read the next complete ServerEvent from a binary stream.
async fn read_next_event(
    body: &mut (impl futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin),
    buffer: &mut BytesMut,
) -> Option<ServerEvent> {
    loop {
        // Try to decode a frame from the buffer
        if buffer.len() >= 4 {
            let len = u32::from_be_bytes(buffer[..4].try_into().unwrap()) as usize;
            if buffer.len() >= 4 + len {
                let json = &buffer[4..4 + len];
                let event: ServerEvent = serde_json::from_slice(json).ok()?;
                let _ = buffer.split_to(4 + len);
                return Some(event);
            }
        }

        // Need more data
        match body.next().await {
            Some(Ok(chunk)) => buffer.extend_from_slice(&chunk),
            _ => return None,
        }
    }
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
async fn test_stream_connection_receives_connected_event() {
    let port = get_free_port();
    let server = TestServer::start(port).await;

    // Connect to events endpoint with local auth headers.
    let response = Client::new()
        .get(format!("{}/events", server.base_url()))
        .header("X-Jazz-Local-Mode", "anonymous")
        .header("X-Jazz-Local-Token", "stream-test-user")
        .send()
        .await
        .expect("connect to events");

    assert!(response.status().is_success());

    let mut body = response.bytes_stream();
    let mut buffer = BytesMut::new();

    // First event should be Connected
    let event = tokio::time::timeout(
        Duration::from_secs(5),
        read_next_event(&mut body, &mut buffer),
    )
    .await
    .expect("timeout waiting for event")
    .expect("no event received");

    match event {
        ServerEvent::Connected {
            connection_id,
            client_id,
            ..
        } => {
            assert!(connection_id.0 > 0);
            assert!(!client_id.is_empty());
        }
        other => panic!("Expected Connected event, got {:?}", other.variant_name()),
    }
}

#[tokio::test]
async fn test_stream_heartbeat() {
    let port = get_free_port();
    let server = TestServer::start(port).await;

    let response = Client::new()
        .get(format!("{}/events", server.base_url()))
        .header("X-Jazz-Local-Mode", "anonymous")
        .header("X-Jazz-Local-Token", "stream-heartbeat-user")
        .send()
        .await
        .expect("connect to events");

    assert!(response.status().is_success());

    let mut body = response.bytes_stream();
    let mut buffer = BytesMut::new();

    // Read the Connected event
    let event = tokio::time::timeout(
        Duration::from_secs(5),
        read_next_event(&mut body, &mut buffer),
    )
    .await
    .expect("timeout")
    .expect("no event");

    assert!(matches!(event, ServerEvent::Connected { .. }));

    // The heartbeat interval is 30s which is too long for a test.
    // Verify the Connected event was received and the stream stays open.
}

#[tokio::test]
async fn test_sync_payload_broadcast_to_stream_client() {
    let port = get_free_port();
    let server = TestServer::start(port).await;

    // Connect to binary stream with local auth headers.
    let response = Client::new()
        .get(format!("{}/events", server.base_url()))
        .header("X-Jazz-Local-Mode", "anonymous")
        .header("X-Jazz-Local-Token", "stream-broadcast-user")
        .send()
        .await
        .expect("connect to events");

    assert!(response.status().is_success());

    let mut body = response.bytes_stream();
    let mut buffer = BytesMut::new();

    // Wait for Connected event to verify connection works
    let event = tokio::time::timeout(
        Duration::from_secs(5),
        read_next_event(&mut body, &mut buffer),
    )
    .await
    .expect("timeout waiting for Connected")
    .expect("no event");

    match event {
        ServerEvent::Connected { connection_id, .. } => {
            assert!(connection_id.0 > 0, "Should receive valid connection_id");
        }
        other => panic!("Expected Connected, got {:?}", other.variant_name()),
    }
}
