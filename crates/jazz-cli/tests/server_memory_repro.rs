#![cfg(feature = "test")]
//! Scratch repro: measure server RSS while one client imports N rows and a
//! fresh client then reads them all back.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::{Json, Router, routing::get};
use base64::Engine;
use jazz::tools::{
    AppContext, AppId, ClientStorage, ColumnType, DurabilityTier, JazzClient, SchemaBuilder,
    TableSchema, Value,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;

mod support;
use support::{cargo_binary, publish_allow_all_permissions};

const APP_ID_STR: &str = "00000000-0000-0000-0000-000000000001";
const ADMIN_SECRET: &str = "admin-secret-for-integration-tests";
const BACKEND_SECRET: &str = "backend-secret-for-integration-tests";
const JWT_KID: &str = "test-jwks-kid";
const JWT_SECRET: &str = "test-jwt-secret-for-integration";
const JWT_ISSUER: &str = "https://issuer.jazz.test";

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    iss: String,
    aud: String,
    claims: JsonValue,
    exp: u64,
}

async fn jwks_handler() -> Json<JsonValue> {
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(JWT_SECRET.as_bytes());
    Json(json!({"keys": [{"kty": "oct","kid": JWT_KID,"alg": "HS256","k": encoded}]}))
}

fn make_jwt(sub: &str) -> String {
    let claims = JwtClaims {
        sub: sub.to_string(),
        iss: JWT_ISSUER.to_owned(),
        aud: APP_ID_STR.to_owned(),
        claims: json!({"role": "user"}),
        exp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
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
    .unwrap()
}

fn schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("title", ColumnType::Text)
                .column("body", ColumnType::Text)
                .column("n", ColumnType::Integer),
        )
        .build()
}

fn proc_status(pid: u32) -> (u64, u64) {
    let s = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    let get = |k: &str| {
        s.lines()
            .find(|l| l.starts_with(k))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
            / 1024
    };
    (get("VmRSS:"), get("VmHWM:"))
}

fn reset_peak(pid: u32) {
    let _ = std::fs::write(format!("/proc/{pid}/clear_refs"), "5");
}

fn report(pid: u32, label: &str) {
    let (rss, hwm) = proc_status(pid);
    eprintln!("MEM {label}: rss={rss}MiB peak={hwm}MiB");
}

struct Server {
    child: Child,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn start_server(data_dir: &Path, jwks: &str) -> Server {
    let bound = data_dir.join("bound-port");
    let bin = std::env::var("JAZZ_REPRO_SERVER_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|_| cargo_binary("jazz-tools"));
    let child = Command::new(bin)
        .args(["server", APP_ID_STR, "--port", "0", "--data-dir"])
        .arg(data_dir.join("db"))
        .env("JAZZ_JWKS_URL", jwks)
        .env("JAZZ_JWT_ISSUER", JWT_ISSUER)
        .env("JAZZ_JWT_AUDIENCE", APP_ID_STR)
        .env("JAZZ_BACKEND_SECRET", BACKEND_SECRET)
        .env("JAZZ_ADMIN_SECRET", ADMIN_SECRET)
        .env("JAZZ_BOUND_PORT_FILE", &bound)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut port = 0;
    for _ in 0..300 {
        if let Ok(c) = std::fs::read_to_string(&bound)
            && let Ok(p) = c.trim().parse::<u16>()
        {
            port = p;
            if Client::new()
                .get(format!("http://127.0.0.1:{p}/health"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Server { child, port }
}

fn context(url: String, dir: PathBuf, user: &str) -> AppContext {
    AppContext {
        app_id: AppId::from_string(APP_ID_STR).unwrap(),
        client_id: None,
        schema: schema(),
        server_url: url,
        data_dir: dir,
        storage: ClientStorage::Persistent,
        storage_factory: Some(std::sync::Arc::new(
            jazz_storage_rocksdb::RocksDbStorageFactory,
        )),
        account_id: None,
        jwt_token: Some(make_jwt(user)),
        backend_secret: None,
        admin_secret: None,
    }
}

async fn connect(url: String, dir: PathBuf, user: &str) -> JazzClient {
    let mut ctx = context(url, dir, user);
    jazz_testkit::enroll_test_context(&mut ctx).await.unwrap();
    JazzClient::connect_with_native_transport(
        ctx,
        std::sync::Arc::new(jazz_native_transport::NativeWebSocketConnector),
    )
    .await
    .unwrap()
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::test]
async fn server_memory_repro() {
    tokio::task::LocalSet::new().run_until(repro()).await
}

async fn repro() {
    let rows = env_usize("JAZZ_REPRO_ROWS", 20_000);
    let body_len = env_usize("JAZZ_REPRO_BODY", 1_000);
    let batch = env_usize("JAZZ_REPRO_BATCH", 500);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let jwks_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/jwks", get(jwks_handler)))
            .await
            .unwrap()
    });
    let jwks = format!("http://{jwks_addr}/jwks");

    let server_dir = TempDir::new().unwrap();
    let server = start_server(server_dir.path(), &jwks).await;
    let pid = server.child.id();
    let url = format!("http://127.0.0.1:{}", server.port);
    let app_id = AppId::from_string(APP_ID_STR).unwrap();
    let r = Client::new()
        .post(format!("{url}/apps/{APP_ID_STR}/admin/schemas"))
        .header("X-Jazz-Admin-Secret", ADMIN_SECRET)
        .json(&json!({ "schema": schema(), "permissions": null }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), reqwest::StatusCode::CREATED);
    publish_allow_all_permissions(&url, app_id, ADMIN_SECRET, &schema()).await;
    report(pid, "baseline");
    reset_peak(pid);

    let writer_dir = TempDir::new().unwrap();
    let writer = connect(
        url.clone(),
        writer_dir.path().to_path_buf(),
        "919ba477-426f-5b7d-9d27-f50e07d1e8ef",
    )
    .await;
    let started = Instant::now();
    let filler: String = "x".repeat(body_len);
    let mut last = None;
    for i in 0..rows {
        let (_, _, tx) = writer
            .insert(
                "items",
                HashMap::from([
                    ("title".to_string(), Value::Text(format!("item {i}"))),
                    ("body".to_string(), Value::Text(format!("{i}:{filler}"))),
                    ("n".to_string(), Value::Integer(i as i32)),
                ]),
            )
            .unwrap();
        last = tx;
        if (i + 1) % batch == 0 {
            if let Some(tx) = last.take() {
                writer
                    .wait_for_transaction(tx, DurabilityTier::GlobalServer)
                    .await
                    .unwrap();
            }
            if (i + 1) % (batch * 10) == 0 {
                report(pid, &format!("import {}", i + 1));
            }
        }
    }
    if let Some(tx) = last.take() {
        writer
            .wait_for_transaction(tx, DurabilityTier::GlobalServer)
            .await
            .unwrap();
    }
    eprintln!("import took {:?}", started.elapsed());
    report(pid, "after import");
    writer.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    report(pid, "writer gone");
    let du = Command::new("du")
        .arg("-sm")
        .arg(server_dir.path())
        .output()
        .unwrap();
    eprintln!(
        "server disk: {}",
        String::from_utf8_lossy(&du.stdout).trim()
    );
    reset_peak(pid);

    if std::env::var("JAZZ_REPRO_RESTART").is_ok() {
        drop(server);
        let server = start_server(server_dir.path(), &jwks).await;
        let pid = server.child.id();
        let url = format!("http://127.0.0.1:{}", server.port);
        report(pid, "restarted");
        reset_peak(pid);
        read_back(pid, url, rows).await;
        return;
    }
    read_back(pid, url, rows).await;
}

async fn read_back(pid: u32, url: String, rows: usize) {
    let reader_dir = TempDir::new().unwrap();
    let reader = connect(
        url,
        reader_dir.path().to_path_buf(),
        "7a9e1d53-4a7c-5c56-8ad3-0e5d1a4c2e11",
    )
    .await;
    let started = Instant::now();
    let sampler_pid = pid;
    let sampler = std::thread::spawn(move || {
        let mut max = 0;
        for _ in 0..1200 {
            let (rss, _) = proc_status(sampler_pid);
            max = max.max(rss);
            std::thread::sleep(Duration::from_millis(50));
            if std::fs::metadata(format!("/proc/{sampler_pid}")).is_err() {
                break;
            }
            if std::path::Path::new("/tmp/claude-0/stop-sampler").exists() {
                break;
            }
        }
        max
    });
    let _ = std::fs::remove_file("/tmp/claude-0/stop-sampler");
    let result = reader
        .query(
            jazz::query::Query::from("items"),
            jazz::tools::ReadTier::Remote,
        )
        .await
        .unwrap();
    eprintln!(
        "reader got {} rows (expected {rows}) in {:?}",
        result.len(),
        started.elapsed()
    );
    report(pid, "after remote read");
    tokio::time::sleep(Duration::from_secs(3)).await;
    report(pid, "reader idle 3s");
    reader.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    report(pid, "reader gone");
    std::fs::write("/tmp/claude-0/stop-sampler", "").unwrap();
    eprintln!(
        "sampled max rss during read: {}MiB",
        sampler.join().unwrap()
    );
}
