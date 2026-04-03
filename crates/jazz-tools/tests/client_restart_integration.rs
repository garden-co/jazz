#![cfg(feature = "test")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{Json, Router, routing::get};
use base64::Engine;
#[cfg(feature = "rocksdb")]
use jazz_tools::storage::RocksDBStorage;
use jazz_tools::storage::Storage;
use jazz_tools::{
    AppContext, AppId, ClientId, ClientStorage, ColumnType, DurabilityTier, JazzClient,
    QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;

const APP_ID_STR: &str = "00000000-0000-0000-0000-000000000001";
const BACKEND_SECRET: &str = "backend-secret-for-integration-tests";
const ADMIN_SECRET: &str = "admin-secret-for-integration-tests";
const JWT_KID: &str = "test-jwks-kid";
const JWT_SECRET: &str = "test-jwt-secret-for-integration";

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    claims: JsonValue,
    exp: u64,
}

struct JwksServer {
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl JwksServer {
    async fn start() -> Self {
        let app = Router::new().route("/jwks", get(jwks_handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind jwks server");
        let addr = listener.local_addr().expect("jwks local addr");
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve jwks");
        });
        Self { addr, task }
    }

    fn endpoint(&self) -> String {
        format!("http://{}/jwks", self.addr)
    }
}

impl Drop for JwksServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn jwks_handler() -> Json<JsonValue> {
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(JWT_SECRET.as_bytes());
    Json(json!({
        "keys": [
            {
                "kty": "oct",
                "kid": JWT_KID,
                "alg": "HS256",
                "k": encoded,
            }
        ]
    }))
}

struct ServerProcess {
    process: Child,
    port: u16,
    client: reqwest::Client,
}

impl ServerProcess {
    async fn start(port: u16, data_dir: &Path, jwks_endpoint: &str) -> Self {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_jazz-tools"));
        cmd.args([
            "server",
            APP_ID_STR,
            "--port",
            &port.to_string(),
            "--data-dir",
            data_dir.to_str().expect("data dir path"),
        ])
        .env("JAZZ_JWKS_URL", jwks_endpoint)
        .env("JAZZ_BACKEND_SECRET", BACKEND_SECRET)
        .env("JAZZ_ADMIN_SECRET", ADMIN_SECRET)
        .stdout(Stdio::null());

        if std::env::var("JAZZ_TEST_SERVER_LOGS").is_ok() {
            cmd.stderr(Stdio::inherit());
        } else {
            cmd.stderr(Stdio::null());
        }

        let process = cmd.spawn().expect("spawn jazz-tools server");
        let server = Self {
            process,
            port,
            client: reqwest::Client::new(),
        };
        server.wait_ready().await;
        server
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    async fn wait_ready(&self) {
        let health_url = format!("{}/health", self.base_url());
        for _ in 0..80 {
            if let Ok(response) = self.client.get(&health_url).send().await
                && response.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("jazz-tools server did not become ready in time");
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if self.process.try_wait().ok().flatten().is_some() {
            return;
        }

        #[cfg(unix)]
        {
            let _ = Command::new("kill")
                .args(["-TERM", &self.process.id().to_string()])
                .status();
        }

        for _ in 0..100 {
            if self.process.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn get_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind port 0");
    listener.local_addr().expect("local addr").port()
}

fn make_jwt(sub: &str) -> String {
    let claims = JwtClaims {
        sub: sub.to_string(),
        claims: json!({"role": "user"}),
        exp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_secs()
            + 3600,
    };

    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(JWT_KID.to_string());

    encode(
        &header,
        &claims,
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    )
    .expect("encode jwt")
}

fn test_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

fn make_context(
    app_id: AppId,
    server_url: String,
    data_dir: PathBuf,
    jwt_token: String,
) -> AppContext {
    AppContext {
        app_id,
        client_id: None,
        schema: test_schema(),
        server_url,
        data_dir,
        storage: ClientStorage::Persistent,
        jwt_token: Some(jwt_token),
        backend_secret: Some(BACKEND_SECRET.to_string()),
        admin_secret: Some(ADMIN_SECRET.to_string()),
    }
}

async fn wait_for_todos_count(
    client: &JazzClient,
    expected_count: usize,
    timeout: Duration,
    durability_tier: Option<DurabilityTier>,
) -> Vec<(jazz_tools::ObjectId, Vec<Value>)> {
    let query = QueryBuilder::new("todos").build();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = Vec::new();

    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(rows)) = tokio::time::timeout(
            Duration::from_secs(8),
            client.query(query.clone(), durability_tier),
        )
        .await
        {
            if rows.len() == expected_count {
                return rows;
            }
            last = rows;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    panic!(
        "timed out waiting for todos count {expected_count}, last_count={}",
        last.len()
    );
}

async fn wait_for_edge_query_ready(client: &JazzClient, timeout: Duration) {
    let query = QueryBuilder::new("todos").build();
    let deadline = tokio::time::Instant::now() + timeout;

    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(_)) = tokio::time::timeout(
            Duration::from_secs(8),
            client.query(query.clone(), Some(DurabilityTier::EdgeServer)),
        )
        .await
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    panic!("timed out waiting for EdgeServer query readiness");
}

async fn wait_for_catalogue_manifest_schema_count_on_disk(
    app_id: AppId,
    data_root: &Path,
    expected_min_count: usize,
    timeout: Duration,
) {
    #[cfg(feature = "rocksdb")]
    let db_path = data_root.join("jazz.rocksdb");
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_count = 0usize;

    while tokio::time::Instant::now() < deadline {
        #[cfg(feature = "rocksdb")]
        let storage_result = if db_path.exists() {
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024).ok()
        } else {
            None
        };
        if let Some(storage) = storage_result {
            let manifest = storage
                .load_catalogue_manifest(app_id.as_object_id())
                .ok()
                .flatten();
            last_count = manifest.map(|m| m.schema_seen.len()).unwrap_or(0);
            let _ = storage.close();
            if last_count >= expected_min_count {
                return;
            }
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!(
        "timed out waiting for schema manifest count >= {expected_min_count}, last_count={last_count}"
    );
}

#[tokio::test]
async fn jazz_tools_cli_existing_client_keeps_working_after_server_restart_without_catalogue_resync()
 {
    let user_id = "restart-no-catalogue-resync-cli";
    let jwks_server = JwksServer::start().await;
    let server_data = TempDir::new().expect("temp server dir");
    let app_id = AppId::from_string(APP_ID_STR).expect("parse app id");
    let port = get_free_port();

    let server = ServerProcess::start(port, server_data.path(), &jwks_server.endpoint()).await;

    let client_dir = TempDir::new().expect("client dir");
    let client = JazzClient::connect(make_context(
        app_id,
        server.base_url(),
        client_dir.path().to_path_buf(),
        make_jwt(user_id),
    ))
    .await
    .expect("connect client");

    wait_for_edge_query_ready(&client, Duration::from_secs(30)).await;

    client
        .create(
            "todos",
            HashMap::from([
                (
                    "title".to_string(),
                    Value::Text("before-restart".to_string()),
                ),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("create before restart");

    let _ = wait_for_todos_count(
        &client,
        1,
        Duration::from_secs(20),
        Some(DurabilityTier::EdgeServer),
    )
    .await;

    drop(server);
    wait_for_catalogue_manifest_schema_count_on_disk(
        app_id,
        server_data.path(),
        1,
        Duration::from_secs(20),
    )
    .await;

    let restarted = ServerProcess::start(port, server_data.path(), &jwks_server.endpoint()).await;

    let rows_after_restart = wait_for_todos_count(
        &client,
        1,
        Duration::from_secs(25),
        Some(DurabilityTier::EdgeServer),
    )
    .await;
    assert_eq!(
        rows_after_restart.len(),
        1,
        "existing client should continue serving Edge-settled queries after server restart"
    );

    client
        .create(
            "todos",
            HashMap::from([
                (
                    "title".to_string(),
                    Value::Text("after-restart".to_string()),
                ),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("create after restart");

    let rows_after_create = wait_for_todos_count(
        &client,
        2,
        Duration::from_secs(25),
        Some(DurabilityTier::EdgeServer),
    )
    .await;
    assert_eq!(
        rows_after_create.len(),
        2,
        "mutations after restart should still settle at Edge without explicit catalogue re-sync"
    );

    client.shutdown().await.expect("shutdown client");
    drop(restarted);
}

#[tokio::test]
async fn memory_storage_client_does_not_persist_local_state_to_disk() {
    let data_dir = TempDir::new().expect("temp client dir");
    let context = AppContext {
        app_id: AppId::from_string(APP_ID_STR).expect("parse app id"),
        client_id: Some(ClientId::new()),
        schema: test_schema(),
        server_url: String::new(),
        data_dir: data_dir.path().to_path_buf(),
        storage: ClientStorage::Memory,
        jwt_token: None,
        backend_secret: None,
        admin_secret: None,
    };

    let client = JazzClient::connect(context.clone())
        .await
        .expect("connect memory client");

    client
        .create(
            "todos",
            HashMap::from([
                (
                    "title".to_string(),
                    Value::Text("only-in-memory".to_string()),
                ),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("create todo");

    let initial_rows = client
        .query(QueryBuilder::new("todos").build(), None)
        .await
        .expect("query rows before restart");
    assert_eq!(
        initial_rows.len(),
        1,
        "memory client should serve local rows"
    );

    client.shutdown().await.expect("shutdown memory client");

    assert!(
        !data_dir.path().join("jazz.rocksdb").exists(),
        "memory storage should not create a RocksDB database on disk"
    );
    assert!(
        !data_dir.path().join("client_id").exists(),
        "memory storage should not persist a client_id file"
    );

    let restarted = JazzClient::connect(context)
        .await
        .expect("reconnect memory client");
    let rows_after_restart = restarted
        .query(QueryBuilder::new("todos").build(), None)
        .await
        .expect("query rows after restart");
    assert_eq!(
        rows_after_restart.len(),
        0,
        "memory storage should not retain rows across reconnects"
    );
    restarted
        .shutdown()
        .await
        .expect("shutdown restarted memory client");
}
