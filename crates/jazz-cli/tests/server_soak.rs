#![cfg(feature = "test")]
//! Long-running single-client soak against a real `jazz-tools server`
//! process, sampling the server's RSS between rounds.
//!
//! Ignored by default. Run it against a release server binary:
//!
//! ```sh
//! cargo build --profile release-server -p jazz-cli --bin jazz-tools
//! JAZZ_SOAK_SERVER_BIN=$PWD/target/release-server/jazz-tools \
//!   JAZZ_SOAK_ROUNDS=40 JAZZ_SOAK_ITERATIONS=200 \
//!   cargo test -p jazz-cli --features test --test server_soak -- --ignored --nocapture
//! ```
//!
//! Each iteration writes (insert or update over a bounded set of rows),
//! opens and drops a filtered subscription, and runs a one-shot remote
//! query. Every `JAZZ_SOAK_RECONNECT_EVERY` rounds the client shuts down and
//! a fresh one connects. The row set stays bounded, so server memory should
//! plateau after warm-up; steady growth per round is a leak.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use jazz::tools::Session;
use jazz::tools::{
    AppContext, AppId, ClientStorage, ColumnType, DurabilityTier, JazzClient, ObjectId,
    SchemaBuilder, TableSchema, Value,
};
use serde_json::json;
use tempfile::TempDir;

mod support;

use support::{cargo_binary, publish_allow_all_permissions};

const APP_ID_STR: &str = "00000000-0000-0000-0000-000000000001";
const BACKEND_SECRET: &str = "backend-secret-for-soak";
const ADMIN_SECRET: &str = "admin-secret-for-soak";
const JWT_ISSUER: &str = "https://issuer.jazz.test";

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

struct ServerProcess {
    process: Child,
    port: u16,
}

impl ServerProcess {
    async fn start(data_dir: &Path) -> Self {
        let bound_port_file = data_dir.join("bound-port");
        let binary = std::env::var_os("JAZZ_SOAK_SERVER_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| cargo_binary("jazz-tools"));
        let mut cmd = Command::new(binary);
        cmd.args([
            "server",
            APP_ID_STR,
            "--port",
            "0",
            "--data-dir",
            data_dir.to_str().expect("data dir path"),
        ])
        .env("JAZZ_BACKEND_SECRET", BACKEND_SECRET)
        .env("JAZZ_ADMIN_SECRET", ADMIN_SECRET)
        .env("JAZZ_BOUND_PORT_FILE", &bound_port_file)
        .stdout(Stdio::null());
        if std::env::var("JAZZ_TEST_SERVER_LOGS").is_ok() {
            cmd.stderr(Stdio::inherit());
        } else {
            cmd.stderr(Stdio::null());
        }
        let mut process = cmd.spawn().expect("spawn jazz-tools server");
        let http = reqwest::Client::new();
        for _ in 0..300 {
            assert!(
                process.try_wait().expect("poll server").is_none(),
                "server exited before becoming ready"
            );
            if let Some(port) = std::fs::read_to_string(&bound_port_file)
                .ok()
                .and_then(|text| text.trim().parse::<u16>().ok())
                && let Ok(response) = http
                    .get(format!("http://127.0.0.1:{port}/health"))
                    .send()
                    .await
                && response.status().is_success()
            {
                return Self { process, port };
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("server did not become ready");
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn rss_kib(&self) -> u64 {
        proc_status_kib(self.process.id(), "VmRSS:")
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn proc_status_kib(pid: u32, field: &str) -> u64 {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    status
        .lines()
        .find(|line| line.starts_with(field))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("bucket", ColumnType::Integer)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

async fn connect(
    server: &ServerProcess,
    bundle: &jazz_testkit::PublishedBundle,
    data_dir: PathBuf,
) -> JazzClient {
    let app_id = AppId::from_string(APP_ID_STR).expect("app id");
    let mut context = AppContext {
        app_id,
        client_id: None,
        schema: schema(),
        server_url: server.base_url(),
        data_dir,
        storage: ClientStorage::Memory,
        account_id: None,
        jwt_token: None,
        backend_secret: Some(BACKEND_SECRET.to_owned()),
        backend_session: Some(
            Session::new(JWT_ISSUER, "919ba477-426f-5b7d-9d27-f50e07d1e8ef")
                .with_claims(json!({ "role": "user" })),
        ),
        bundle: Some(bundle.bundle.clone()),
        admin_secret: None,
        migrations: Default::default(),
    };
    jazz_testkit::enroll_test_context_with_admin_secret(ADMIN_SECRET, &mut context)
        .await
        .expect("enroll soak client");
    JazzClient::connect_with_native_transport(
        context,
        std::sync::Arc::new(jazz_native_transport::NativeWebSocketConnector),
    )
    .await
    .expect("connect soak client")
}

fn row(i: usize, bucket: i32, body: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("title".to_owned(), Value::Text(format!("todo {i} {body}"))),
        ("bucket".to_owned(), Value::Integer(bucket)),
        ("completed".to_owned(), Value::Boolean(i % 2 == 0)),
    ])
}

#[tokio::test]
#[ignore = "long-running soak; run explicitly with --ignored"]
async fn single_client_soak_server_memory_plateaus() {
    tokio::task::LocalSet::new()
        .run_until(single_client_soak_impl())
        .await
}

async fn single_client_soak_impl() {
    let rounds = env_usize("JAZZ_SOAK_ROUNDS", 10);
    let iterations = env_usize("JAZZ_SOAK_ITERATIONS", 100);
    let max_rows = env_usize("JAZZ_SOAK_MAX_ROWS", 200);
    let reconnect_every = env_usize("JAZZ_SOAK_RECONNECT_EVERY", 5);
    let body = "x".repeat(env_usize("JAZZ_SOAK_BODY_BYTES", 256));

    let server_data = TempDir::new().expect("server dir");
    let server = ServerProcess::start(server_data.path()).await;
    let app_id = AppId::from_string(APP_ID_STR).expect("app id");
    let bundle =
        publish_allow_all_permissions(&server.base_url(), app_id, ADMIN_SECRET, &schema()).await;
    eprintln!(
        "soak: server pid {} rss {} KiB at start",
        server.process.id(),
        server.rss_kib()
    );

    let client_root = TempDir::new().expect("client dir");
    let mut generation = 0usize;
    let mut client = connect(&server, &bundle, client_root.path().join("c0")).await;
    let mut ids: Vec<ObjectId> = Vec::new();
    let mut counter = 0usize;
    let started = Instant::now();
    let mut samples = Vec::new();

    for round in 0..rounds {
        if round > 0 && reconnect_every > 0 && round % reconnect_every == 0 {
            client.shutdown().await.expect("shutdown client");
            generation += 1;
            client = connect(
                &server,
                &bundle,
                client_root.path().join(format!("c{generation}")),
            )
            .await;
            // A fresh client must load the rows it is about to update.
            client
                .query(
                    jazz::query::Query::from("todos"),
                    jazz::tools::ReadTier::Remote,
                )
                .await
                .expect("reload rows after reconnect");
        }
        for _ in 0..iterations {
            counter += 1;
            let bucket = (counter % 17) as i32;
            let tx = if ids.len() < max_rows {
                let (id, _, tx) = client
                    .insert("todos", row(counter, bucket, &body))
                    .expect("insert");
                ids.push(id);
                tx
            } else {
                let id = ids[counter % ids.len()];
                client
                    .update(
                        "todos",
                        id,
                        vec![
                            (
                                "title".to_owned(),
                                Value::Text(format!("todo {counter} {body}")),
                            ),
                            ("bucket".to_owned(), Value::Integer(bucket)),
                        ],
                    )
                    .expect("update")
            };
            client
                .wait_for_transaction(tx.expect("commits"), DurabilityTier::GlobalServer)
                .await
                .expect("write reaches server");

            // A view opens and closes, like a component mounting and
            // unmounting.
            let mut stream = client
                .subscribe(jazz::query::Query::from("todos").filter(jazz::query::eq(
                    jazz::query::col("bucket"),
                    jazz::query::lit(bucket),
                )))
                .await
                .expect("subscribe");
            tokio::time::timeout(Duration::from_secs(20), stream.next())
                .await
                .expect("first delivery in time")
                .expect("stream open");
            drop(stream);

            tokio::time::timeout(
                Duration::from_secs(20),
                client.query(
                    jazz::query::Query::from("todos").limit(20),
                    jazz::tools::ReadTier::Remote,
                ),
            )
            .await
            .expect("remote query in time")
            .expect("remote query");
        }
        // Let the server settle before sampling.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let rss = server.rss_kib();
        samples.push(rss);
        eprintln!(
            "soak: round {round:>3} ops {counter:>6} t {:>6.0}s rss {:>8} KiB hwm {:>8} KiB",
            started.elapsed().as_secs_f64(),
            rss,
            proc_status_kib(server.process.id(), "VmHWM:"),
        );
    }

    let idle_secs = env_usize("JAZZ_SOAK_IDLE_SECS", 0);
    if idle_secs > 0 {
        tokio::time::sleep(Duration::from_secs(idle_secs as u64)).await;
        eprintln!("soak: after {idle_secs}s idle rss {} KiB", server.rss_kib());
    }
    client.shutdown().await.expect("shutdown client");
    tokio::time::sleep(Duration::from_secs(2)).await;
    eprintln!("soak: after final disconnect rss {} KiB", server.rss_kib());

    // Compare the second half against the first half after warm-up: a
    // plateauing server grows by much less than the warm-up did.
    if samples.len() >= 6 {
        let warm = samples[samples.len() / 3];
        let last = *samples.last().unwrap();
        eprintln!(
            "soak: growth after warm-up {} KiB over {} rounds",
            last as i64 - warm as i64,
            samples.len() - samples.len() / 3 - 1
        );
    }
}
