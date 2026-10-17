//! Local-only Core harness for Android/iOS installed-artifact acceptance.
//! The device retains its own accounts; the host supplies only endpoint/control metadata.

use std::{
    collections::HashMap,
    io::{BufRead, Write},
    time::Duration,
};

#[path = "support/device_fixture.rs"]
mod device_fixture;

use jazz::query::Query;

use jazz::tools::{AppContext, AppId, ClientStorage, DurabilityTier, Value};
use jazz_native_relay as _;
use jazz_server::{JazzServer, TestJwtIssuer};
use jazz_testkit::{connect, wait_for_query};

fn main() {
    if std::env::args().any(|arg| arg == "--print-fixture") {
        println!(
            "{}",
            serde_json::to_string_pretty(&device_fixture::fixture()).unwrap()
        );
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build local Core harness runtime")
        .block_on(tokio::task::LocalSet::new().run_until(run()));
}

async fn run() {
    let run_nonce = std::env::var("JAZZ_DEVICE_RUN_NONCE").expect("host run nonce is required");
    let title = format!("high-level-foreground-row:{run_nonce}");
    let issuer = TestJwtIssuer::start().await;
    let app_id = AppId::from_name("jazz-device-acceptance");
    let schema = device_fixture::schema();
    let server_storage = tempfile::tempdir().expect("persistent Core storage");
    let core = JazzServer::builder()
        .with_app_id(app_id)
        .with_data_dir(server_storage.path())
        .with_storage_factory(std::sync::Arc::new(
            jazz_storage_rocksdb::RocksDbStorageFactory,
        ))
        .with_schema(schema.clone())
        .with_jwks_url(issuer.endpoint())
        .start()
        .await
        .expect("start test server");
    // A fresh, read-only observer attaches directly to Core, never device SQLite. It cannot seed the acceptance marker itself. The device
    // is the only writer of this run-specific high-level foreground title.
    let observer_storage = tempfile::tempdir().expect("observer scratch directory");
    let observer = connect(AppContext {
        app_id: core.app_id(),
        client_id: None,
        schema: schema.clone(),
        server_url: core.base_url(),
        data_dir: observer_storage.path().to_owned(),
        storage: ClientStorage::Memory,
        storage_factory: None,
        account_id: None,
        jwt_token: Some(TestJwtIssuer::jwt_for_user("rn-device-core-observer")),
        backend_secret: None,
        admin_secret: Some(core.admin_secret().to_owned()),
    })
    .await
    .expect("connect read-only Core observer");

    // Android reaches the host loopback listener through 10.0.2.2. The
    // driver derives that endpoint from `server_port`; keeping it out of this
    // process means the same harness remains usable by non-Android hosts.
    let receipt = serde_json::json!({
        "server_port": core.port(),
    });
    println!("JAZZ_RN_SERVER_SESSION {receipt}");
    std::io::stdout().flush().expect("flush harness receipt");

    let row_id = wait_for_query(
        &observer,
        Query::from("todos"),
        jazz::tools::ReadTier::Remote,
        Duration::from_secs(60),
        "device run marker at Core",
        |rows| {
            rows.into_iter()
                .find_map(|(id, values)| values.contains(&Value::Text(title.clone())).then_some(id))
        },
    )
    .await;
    let observation = serde_json::json!({
        "source": "core",
        "runNonce": run_nonce,
        "title": title,
        "rowId": row_id.to_string(),
    });
    println!("JAZZ_RN_CORE_OBSERVATION {observation}");
    std::io::stdout().flush().expect("flush Core observation");

    // The installed-app driver owns this narrow control protocol. It lets the
    // existing foreground stay alive while Core is genuinely absent, then
    // reopens Core from the same persistent store at the identical configured endpoint.
    let mut line = tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        std::io::stdin().lock().read_line(&mut line).unwrap();
        line
    })
    .await
    .expect("join control reader");
    assert_eq!(line.trim(), "interrupt-server");
    let server_port = core.port();
    let admin_secret = core.admin_secret().to_owned();
    assert_eq!(
        core.shutdown().await,
        jazz_server::ShutdownPhase::StorageClosed
    );
    println!("JAZZ_RN_SERVER_INTERRUPTED {{\"server_port\":{server_port}}}");
    std::io::stdout()
        .flush()
        .expect("flush server interruption");
    line = tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        std::io::stdin().lock().read_line(&mut line).unwrap();
        line
    })
    .await
    .expect("join control reader");
    assert_eq!(line.trim(), "recover-server");
    let core = JazzServer::builder()
        .with_port(server_port)
        .with_app_id(app_id)
        .with_data_dir(server_storage.path())
        .with_storage_factory(std::sync::Arc::new(
            jazz_storage_rocksdb::RocksDbStorageFactory,
        ))
        .with_schema(schema.clone())
        .with_jwks_url(issuer.endpoint())
        .with_admin_secret(admin_secret)
        .start()
        .await
        .expect("reopen persistent Core before the recovery write");
    let writer_storage = tempfile::tempdir().expect("Core writer scratch directory");
    let core_writer = connect(AppContext {
        app_id: core.app_id(),
        client_id: None,
        schema: schema.clone(),
        server_url: core.base_url(),
        data_dir: writer_storage.path().to_owned(),
        storage: ClientStorage::Memory,
        storage_factory: None,
        account_id: None,
        jwt_token: Some(TestJwtIssuer::jwt_for_user(
            "rn-device-core-recovery-writer",
        )),
        backend_secret: None,
        admin_secret: Some(core.admin_secret().to_owned()),
    })
    .await
    .expect("connect Core recovery writer");
    let (_, _, transaction) = core_writer
        .insert(
            "todos",
            HashMap::from([(
                "title".to_owned(),
                Value::Text(format!("{title}:recovered-by-core")),
            )]),
        )
        .expect("Core writer writes post-recovery marker");
    core_writer
        .wait_for_transaction(
            transaction.expect("Core write owns a transaction"),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("Core commits post-recovery marker");
    println!("JAZZ_RN_SERVER_RECOVERED {{\"server_port\":{server_port}}}");
    std::io::stdout().flush().expect("flush server recovery");

    // The parent owns process lifetime. It kills this local-only fixture after
    // both installed-app launches, which also ensures credentials cannot be
    // accidentally reused by a later driver invocation.
    std::future::pending::<()>().await;
}
