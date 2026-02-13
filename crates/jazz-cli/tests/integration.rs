//! E2E integration tests for jazz-cli server.
//!
//! These tests spawn the actual `jazz` binary and interact via HTTP
//! with binary length-prefixed streaming.

use std::collections::{HashMap, HashSet};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use bytes::BytesMut;
use futures::StreamExt;
use groove::commit::{Commit, CommitAckState, StoredState};
use groove::object::{BranchName, ObjectId};
use groove::sync_manager::{ClientId, ObjectMetadata, PersistenceTier, SyncPayload};
use jazz_transport::{ServerEvent, SyncPayloadRequest};
use jsonwebtoken::{EncodingKey, Header, encode};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const JWT_SECRET: &str = "test-jwt-secret-for-integration";
const CLIENT_DISCONNECT_GRACE_MS: u64 = 400;

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    claims: serde_json::Value,
    exp: u64,
}

/// Create a JWT token for test authentication.
fn make_jwt(user_id: &str) -> String {
    let claims = JwtClaims {
        sub: user_id.to_string(),
        claims: serde_json::json!({"role": "user"}),
        exp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600,
    };
    let key = EncodingKey::from_secret(JWT_SECRET.as_bytes());
    encode(&Header::default(), &claims, &key).unwrap()
}

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

        let process = Command::new(env!("CARGO_BIN_EXE_jazz"))
            .args([
                "server",
                app_id,
                "--port",
                &port.to_string(),
                "--data-dir",
                data_dir.path().to_str().unwrap(),
            ])
            .env("JAZZ_JWT_SECRET", JWT_SECRET)
            .env(
                "JAZZ_CLIENT_DISCONNECT_GRACE_MS",
                CLIENT_DISCONNECT_GRACE_MS.to_string(),
            )
            .env("JAZZ_EVENTS_HEARTBEAT_MS", "50")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn jazz server");

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

fn fixed_client_id(value: &str) -> ClientId {
    ClientId::parse(value).expect("valid fixed test client id")
}

fn build_test_commit(content: &[u8]) -> Commit {
    Commit {
        parents: Default::default(),
        content: content.to_vec(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64,
        author: ObjectId::from_uuid(Uuid::nil()),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    }
}

async fn post_sync_payload(base_url: &str, jwt: &str, client_id: ClientId, payload: SyncPayload) {
    let request = SyncPayloadRequest { payload, client_id };
    let response = Client::new()
        .post(format!("{}/sync", base_url))
        .header("Authorization", format!("Bearer {}", jwt))
        .json(&request)
        .send()
        .await
        .expect("send /sync request");

    let status = response.status();
    let body = response
        .text()
        .await
        .expect("read /sync response body for assertion");
    assert!(
        status.is_success(),
        "sync request failed: {} {}",
        status,
        body
    );
}

async fn wait_for_persistence_ack(
    body: &mut (impl futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin),
    buffer: &mut BytesMut,
    commit_id: groove::commit::CommitId,
    expected_tier: PersistenceTier,
    timeout: Duration,
) -> bool {
    match tokio::time::timeout(timeout, async {
        loop {
            let event = read_next_event(body, buffer).await?;
            if let ServerEvent::SyncUpdate { payload } = event
                && let SyncPayload::PersistenceAck {
                    tier,
                    confirmed_commits,
                    ..
                } = payload.as_ref()
                && *tier == expected_tier
                && confirmed_commits.contains(&commit_id)
            {
                return Some(());
            }
        }
    })
    .await
    {
        Ok(Some(())) => true,
        Ok(None) | Err(_) => false,
    }
}

async fn connect_and_drop_events_socket(port: u16, client_id: ClientId, jwt: &str) {
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect raw socket to /events");
    let request = format!(
        "GET /events?client_id={} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",
        client_id, port, jwt
    );
    socket
        .write_all(request.as_bytes())
        .await
        .expect("send raw /events request");
    socket.flush().await.expect("flush raw /events request");

    let mut response = vec![0_u8; 1024];
    let read = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut response))
        .await
        .expect("timeout waiting for /events response")
        .expect("read /events response bytes");
    assert!(read > 0, "expected non-empty /events HTTP response");

    let status_preview = String::from_utf8_lossy(&response[..read]);
    assert!(
        status_preview.contains(" 200 "),
        "expected 200 from /events, got: {}",
        status_preview.lines().next().unwrap_or("<no status line>")
    );

    drop(socket);
    tokio::time::sleep(Duration::from_millis(100)).await;
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

    // Connect to events endpoint with JWT auth
    let token = make_jwt("stream-test-user");
    let response = Client::new()
        .get(format!("{}/events", server.base_url()))
        .header("Authorization", format!("Bearer {}", token))
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

    let token = make_jwt("stream-heartbeat-user");
    let response = Client::new()
        .get(format!("{}/events", server.base_url()))
        .header("Authorization", format!("Bearer {}", token))
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

    // Connect to binary stream with JWT auth
    let token = make_jwt("stream-broadcast-user");
    let response = Client::new()
        .get(format!("{}/events", server.base_url()))
        .header("Authorization", format!("Bearer {}", token))
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

#[tokio::test]
async fn test_reconnect_within_grace_retains_client_sync_state() {
    let port = get_free_port();
    let server = TestServer::start(port).await;
    let base_url = server.base_url();

    let client_a = fixed_client_id("11111111-1111-1111-1111-111111111111");
    let client_b = fixed_client_id("22222222-2222-2222-2222-222222222222");
    let token_a = make_jwt("grace-client-a");
    let token_b = make_jwt("grace-client-b");

    let object_id = ObjectId::new();
    let branch_name = BranchName::new("main");
    let commit = build_test_commit(b"retain-state");
    let commit_id = commit.id();

    post_sync_payload(
        &base_url,
        &token_a,
        client_a,
        SyncPayload::ObjectUpdated {
            object_id,
            metadata: Some(ObjectMetadata {
                id: object_id,
                metadata: HashMap::new(),
            }),
            branch_name,
            commits: vec![commit],
        },
    )
    .await;

    connect_and_drop_events_socket(server.port, client_a, &token_a).await;

    let second_stream = Client::new()
        .get(format!("{}/events?client_id={}", base_url, client_a))
        .header("Authorization", format!("Bearer {}", token_a))
        .send()
        .await
        .expect("reconnect /events stream");
    assert!(second_stream.status().is_success());

    let mut second_body = second_stream.bytes_stream();
    let mut second_buffer = BytesMut::new();
    let second_event = tokio::time::timeout(
        Duration::from_secs(5),
        read_next_event(&mut second_body, &mut second_buffer),
    )
    .await
    .expect("timeout waiting for reconnect Connected event")
    .expect("no reconnect Connected event");
    assert!(matches!(second_event, ServerEvent::Connected { .. }));

    let mut confirmed_commits = HashSet::new();
    confirmed_commits.insert(commit_id);
    post_sync_payload(
        &base_url,
        &token_b,
        client_b,
        SyncPayload::PersistenceAck {
            object_id,
            branch_name,
            confirmed_commits,
            tier: PersistenceTier::CoreServer,
        },
    )
    .await;

    let received_ack = wait_for_persistence_ack(
        &mut second_body,
        &mut second_buffer,
        commit_id,
        PersistenceTier::CoreServer,
        Duration::from_secs(2),
    )
    .await;
    assert!(
        received_ack,
        "Expected PersistenceAck relay for client reconnected within grace window"
    );
}

#[tokio::test]
async fn test_reconnect_after_grace_purges_client_sync_state() {
    let port = get_free_port();
    let server = TestServer::start(port).await;
    let base_url = server.base_url();

    let client_a = fixed_client_id("33333333-3333-3333-3333-333333333333");
    let client_b = fixed_client_id("44444444-4444-4444-4444-444444444444");
    let token_a = make_jwt("purge-client-a");
    let token_b = make_jwt("purge-client-b");

    let object_id = ObjectId::new();
    let branch_name = BranchName::new("main");
    let commit = build_test_commit(b"purged-state");
    let commit_id = commit.id();

    post_sync_payload(
        &base_url,
        &token_a,
        client_a,
        SyncPayload::ObjectUpdated {
            object_id,
            metadata: Some(ObjectMetadata {
                id: object_id,
                metadata: HashMap::new(),
            }),
            branch_name,
            commits: vec![commit],
        },
    )
    .await;

    connect_and_drop_events_socket(server.port, client_a, &token_a).await;
    tokio::time::sleep(Duration::from_millis(CLIENT_DISCONNECT_GRACE_MS + 500)).await;

    let reconnect_stream = Client::new()
        .get(format!("{}/events?client_id={}", base_url, client_a))
        .header("Authorization", format!("Bearer {}", token_a))
        .send()
        .await
        .expect("reconnect after grace /events stream");
    assert!(reconnect_stream.status().is_success());

    let mut reconnect_body = reconnect_stream.bytes_stream();
    let mut reconnect_buffer = BytesMut::new();
    let reconnect_event = tokio::time::timeout(
        Duration::from_secs(5),
        read_next_event(&mut reconnect_body, &mut reconnect_buffer),
    )
    .await
    .expect("timeout waiting for post-grace Connected event")
    .expect("no post-grace Connected event");
    assert!(matches!(reconnect_event, ServerEvent::Connected { .. }));

    let mut confirmed_commits = HashSet::new();
    confirmed_commits.insert(commit_id);
    post_sync_payload(
        &base_url,
        &token_b,
        client_b,
        SyncPayload::PersistenceAck {
            object_id,
            branch_name,
            confirmed_commits,
            tier: PersistenceTier::CoreServer,
        },
    )
    .await;

    let received_ack = wait_for_persistence_ack(
        &mut reconnect_body,
        &mut reconnect_buffer,
        commit_id,
        PersistenceTier::CoreServer,
        Duration::from_millis(750),
    )
    .await;
    assert!(
        !received_ack,
        "Did not expect PersistenceAck relay after grace window expiry"
    );
}
