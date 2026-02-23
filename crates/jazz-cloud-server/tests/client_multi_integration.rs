use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{Json, Router, routing::get};
use base64::Engine;
use groove::object::BranchName;
use groove::query_manager::types::{ComposedBranchName, SchemaHash};
use groove::storage::{Storage, SurrealKvStorage};
use groove::{
    AppContext, AppId, ColumnType, JazzClient, PersistenceTier, QueryBuilder, SchemaBuilder,
    TableSchema, Value,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;

const INTERNAL_API_SECRET: &str = "integration-internal-secret";
const SECRET_HASH_KEY: &str = "integration-secret-hash-key";
const ADMIN_SECRET: &str = "integration-admin-secret";
const BACKEND_SECRET: &str = "integration-backend-secret";
const JWT_KID: &str = "integration-kid";
const JWT_SECRET: &str = "integration-jwt-secret";

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    claims: JsonValue,
    exp: u64,
}

#[derive(Debug, Deserialize)]
struct CreateAppResponse {
    app_id: String,
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
    client: Client,
}

impl ServerProcess {
    async fn start(data_root: &Path) -> Self {
        let port = get_free_port();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_jazz-cloud-server"));
        cmd.args([
            "--port",
            &port.to_string(),
            "--data-root",
            data_root.to_str().expect("data root path"),
            "--internal-api-secret",
            INTERNAL_API_SECRET,
            "--secret-hash-key",
            SECRET_HASH_KEY,
            "--worker-threads",
            "1",
        ])
        .stdout(Stdio::null());

        if std::env::var("JAZZ_TEST_SERVER_LOGS").is_ok() {
            cmd.stderr(Stdio::inherit());
        } else {
            cmd.stderr(Stdio::null());
        }

        let process = cmd.spawn().expect("spawn jazz-cloud-server");

        let server = Self {
            process,
            port,
            client: Client::new(),
        };
        server.wait_ready().await;
        server
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    async fn wait_ready(&self) {
        let health_url = format!("{}/health", self.base_url());
        for _ in 0..60 {
            if let Ok(response) = self.client.get(&health_url).send().await
                && response.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("jazz-cloud-server did not become ready in time");
    }

    async fn create_app(&self, jwks_endpoint: &str) -> CreateAppResponse {
        let response = self
            .client
            .post(format!("{}/internal/apps", self.base_url()))
            .header("X-Jazz-Internal-Secret", INTERNAL_API_SECRET)
            .json(&json!({
                "app_name": "client-e2e-app",
                "jwks_endpoint": jwks_endpoint,
                "backend_secret": BACKEND_SECRET,
                "admin_secret": ADMIN_SECRET,
            }))
            .send()
            .await
            .expect("create app request");

        let status = response.status();
        let body = response.text().await.expect("create app body");
        assert!(
            status.is_success(),
            "create app failed: status={status}, body={body}"
        );
        serde_json::from_str(&body).expect("create app json")
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

fn test_schema() -> groove::Schema {
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
        server_url: format!("{server_url}/apps/{app_id}"),
        data_dir,
        jwt_token: Some(jwt_token),
        backend_secret: Some(BACKEND_SECRET.to_string()),
        admin_secret: Some(ADMIN_SECRET.to_string()),
    }
}

fn sender_delay_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct ScopedEnvVar {
    key: &'static str,
    previous: Option<String>,
}

impl ScopedEnvVar {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        // SAFETY: Test-only env mutation scoped by Drop restore.
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => {
                // SAFETY: restoring previous test-only env value.
                unsafe { std::env::set_var(self.key, value) };
            }
            None => {
                // SAFETY: removing test-only env value.
                unsafe { std::env::remove_var(self.key) };
            }
        }
    }
}

async fn wait_for_todos_count(
    client: &JazzClient,
    expected_count: usize,
    timeout: Duration,
    settled_tier: Option<PersistenceTier>,
) -> Vec<(groove::ObjectId, Vec<Value>)> {
    let query = QueryBuilder::new("todos").build();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = Vec::new();

    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(rows)) = tokio::time::timeout(
            Duration::from_secs(8),
            client.query(query.clone(), settled_tier),
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
            client.query(query.clone(), Some(PersistenceTier::EdgeServer)),
        )
        .await
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    panic!("timed out waiting for EdgeServer query readiness");
}

async fn wait_for_todos_count_on_disk(
    app_id: AppId,
    data_root: &Path,
    expected_count: usize,
    timeout: Duration,
) {
    let db_path = data_root
        .join("apps")
        .join(app_id.to_string())
        .join("groove.surrealkv");
    let schema_hash = SchemaHash::compute(&test_schema());
    let branch = ComposedBranchName::new("client", schema_hash, "main")
        .to_branch_name()
        .to_string();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_count = 0usize;

    while tokio::time::Instant::now() < deadline {
        if db_path.exists()
            && let Ok(storage) = SurrealKvStorage::open(&db_path, 64 * 1024 * 1024)
        {
            let branch_name = BranchName::new(&branch);
            let row_ids = storage.index_scan_all("todos", "_id", &branch);
            let mut materialized = 0usize;
            for row_id in row_ids {
                let has_metadata = storage
                    .load_object_metadata(row_id)
                    .ok()
                    .flatten()
                    .is_some();
                let has_content = storage
                    .load_branch(row_id, &branch_name)
                    .ok()
                    .flatten()
                    .map(|loaded| {
                        loaded
                            .commits
                            .iter()
                            .any(|commit| !commit.content.is_empty())
                    })
                    .unwrap_or(false);
                if has_metadata && has_content {
                    materialized += 1;
                }
            }
            last_count = materialized;
            let _ = storage.close();
            if last_count == expected_count {
                return;
            }
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!("timed out waiting for on-disk todos count {expected_count}, last_count={last_count}");
}

#[tokio::test]
async fn jazz_tools_clients_sync_queries_and_mutations_over_cloud_server() {
    let jwks_server = JwksServer::start().await;
    let server_data = TempDir::new().expect("temp server dir");
    let server = ServerProcess::start(server_data.path()).await;
    let app = server.create_app(&jwks_server.endpoint()).await;
    let app_id = AppId::from_string(&app.app_id).expect("parse app id");

    let client_a_dir = TempDir::new().expect("client a dir");
    let client_a = JazzClient::connect(make_context(
        app_id,
        server.base_url(),
        client_a_dir.path().to_path_buf(),
        make_jwt("client-a"),
    ))
    .await
    .expect("connect client a");

    // Warm up auth/JWKS + schema/catalogue sync before first row write.
    wait_for_edge_query_ready(&client_a, Duration::from_secs(30)).await;

    let row_id = client_a
        .create(
            "todos",
            vec![
                Value::Text("from-client-a".to_string()),
                Value::Boolean(false),
            ],
        )
        .await
        .expect("client a create todo");

    // Ensure local state sees the insert first.
    let _ = wait_for_todos_count(&client_a, 1, Duration::from_secs(10), None).await;

    let _ = wait_for_todos_count(
        &client_a,
        1,
        Duration::from_secs(20),
        Some(PersistenceTier::EdgeServer),
    )
    .await;

    let client_b_dir = TempDir::new().expect("client b dir");
    let client_b = JazzClient::connect(make_context(
        app_id,
        server.base_url(),
        client_b_dir.path().to_path_buf(),
        make_jwt("client-b"),
    ))
    .await
    .expect("connect client b");

    // Ensure query path is fully ready before asserting cross-client sync.
    wait_for_edge_query_ready(&client_b, Duration::from_secs(30)).await;

    let rows = wait_for_todos_count(
        &client_b,
        1,
        Duration::from_secs(30),
        Some(PersistenceTier::EdgeServer),
    )
    .await;
    assert_eq!(rows[0].0, row_id);
    assert_eq!(rows[0].1[0], Value::Text("from-client-a".to_string()));

    client_a
        .update(
            row_id,
            vec![("completed".to_string(), Value::Boolean(true))],
        )
        .await
        .expect("client a update todo");

    let query = QueryBuilder::new("todos").build();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut saw_update = false;
    while tokio::time::Instant::now() < deadline {
        if let Ok(rows) = client_b
            .query(query.clone(), Some(PersistenceTier::EdgeServer))
            .await
            && rows.len() == 1
            && rows[0].1[1] == Value::Boolean(true)
        {
            saw_update = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(saw_update, "client b should observe mutation from client a");

    client_a.delete(row_id).await.expect("client a delete todo");
    let rows_after_delete = wait_for_todos_count(&client_b, 0, Duration::from_secs(15), None).await;
    assert!(rows_after_delete.is_empty());

    client_a.shutdown().await.expect("shutdown client a");
    client_b.shutdown().await.expect("shutdown client b");
}

#[tokio::test]
async fn jazz_tools_sender_side_objectupdated_delay_should_not_return_stale_settled_rows() {
    let _env_lock = sender_delay_env_lock()
        .lock()
        .expect("lock sender delay env");

    let jwks_server = JwksServer::start().await;
    let server_data = TempDir::new().expect("temp server dir");
    let seed_server = ServerProcess::start(server_data.path()).await;
    let app = seed_server.create_app(&jwks_server.endpoint()).await;
    let app_id = AppId::from_string(&app.app_id).expect("parse app id");

    // Phase 1: seed persisted row without artificial delay.
    let client_a_dir = TempDir::new().expect("client a dir");
    let client_a = JazzClient::connect(make_context(
        app_id,
        seed_server.base_url(),
        client_a_dir.path().to_path_buf(),
        make_jwt("sender-delay-client-a"),
    ))
    .await
    .expect("connect client a");

    wait_for_edge_query_ready(&client_a, Duration::from_secs(30)).await;

    client_a
        .create(
            "todos",
            vec![
                Value::Text("ordering-precision-seed".to_string()),
                Value::Boolean(false),
            ],
        )
        .await
        .expect("client a create todo");

    let _ = wait_for_todos_count(
        &client_a,
        1,
        Duration::from_secs(20),
        Some(PersistenceTier::EdgeServer),
    )
    .await;
    client_a.shutdown().await.expect("shutdown client a");
    drop(seed_server);

    // Phase 2: restart with delayed server->client ObjectUpdated sends.
    let _delay = ScopedEnvVar::set("JAZZ_TEST_DELAY_SERVER_SEND_OBJECT_UPDATED_MS", "1400-1800");
    let _every = ScopedEnvVar::set("JAZZ_TEST_DELAY_SERVER_SEND_OBJECT_UPDATED_EVERY", "1");
    let delayed_server = ServerProcess::start(server_data.path()).await;

    let client_b_dir = TempDir::new().expect("client b dir");
    let client_b = JazzClient::connect(make_context(
        app_id,
        delayed_server.base_url(),
        client_b_dir.path().to_path_buf(),
        make_jwt("sender-delay-client-b"),
    ))
    .await
    .expect("connect client b");

    let query = QueryBuilder::new("todos").build();
    let mut rows = None;
    for _ in 0..3 {
        match tokio::time::timeout(
            Duration::from_secs(8),
            client_b.query(query.clone(), Some(PersistenceTier::EdgeServer)),
        )
        .await
        {
            Ok(Ok(result_rows)) => {
                rows = Some(result_rows);
                break;
            }
            Ok(Err(err)) => panic!("client b query error: {err}"),
            Err(_) => {
                // Stream can race startup; retry to exercise ordering once connected.
                continue;
            }
        }
    }
    let rows = rows.expect("client b query timeout after retries");

    assert_eq!(
        rows.len(),
        1,
        "query settled at EdgeServer should include already-persisted row"
    );

    client_b.shutdown().await.expect("shutdown client b");
}

#[tokio::test]
async fn jazz_tools_client_resyncs_after_server_restart_with_persisted_app_data() {
    let user_id = "resync-user";
    let jwks_server = JwksServer::start().await;
    let server_data = TempDir::new().expect("temp server dir");
    let app = {
        let server = ServerProcess::start(server_data.path()).await;
        let app = server.create_app(&jwks_server.endpoint()).await;
        let app_id = AppId::from_string(&app.app_id).expect("parse app id");

        let writer_dir = TempDir::new().expect("writer dir");
        let writer = JazzClient::connect(make_context(
            app_id,
            server.base_url(),
            writer_dir.path().to_path_buf(),
            make_jwt(user_id),
        ))
        .await
        .expect("connect writer");

        wait_for_edge_query_ready(&writer, Duration::from_secs(30)).await;

        writer
            .create(
                "todos",
                vec![
                    Value::Text("persisted-on-server".to_string()),
                    Value::Boolean(false),
                ],
            )
            .await
            .expect("writer create");

        wait_for_todos_count(
            &writer,
            1,
            Duration::from_secs(10),
            Some(PersistenceTier::EdgeServer),
        )
        .await;
        writer.shutdown().await.expect("shutdown writer");
        app
    };

    let app_id = AppId::from_string(&app.app_id).expect("parse app id");
    wait_for_todos_count_on_disk(app_id, server_data.path(), 1, Duration::from_secs(20)).await;
    let restarted = ServerProcess::start(server_data.path()).await;

    let reader_dir = TempDir::new().expect("reader dir");
    {
        let reader = JazzClient::connect(make_context(
            app_id,
            restarted.base_url(),
            reader_dir.path().to_path_buf(),
            make_jwt(user_id),
        ))
        .await
        .expect("connect reader");

        wait_for_edge_query_ready(&reader, Duration::from_secs(30)).await;
        reader.shutdown().await.expect("shutdown reader");
    }

    drop(restarted);
    wait_for_todos_count_on_disk(app_id, server_data.path(), 1, Duration::from_secs(20)).await;
}
