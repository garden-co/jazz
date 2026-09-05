//! Local-only Edge/Core session issuer for the Android/iOS installed-artifact
//! acceptance driver. It deliberately prints endpoint and short-lived bearer
//! material only to its direct parent process, never to a checked-in fixture.

use std::{io::Write, time::Duration};

#[path = "support/device_fixture.rs"]
mod device_fixture;

use jazz::query::Query;

use jazz::tools::{AppContext, AppId, ClientStorage, DurabilityTier, Value};
use jazz_native_relay as _;
use jazz_server::{EdgeUpstreamHealth, JazzServer, TestJwtIssuer};
use jazz_testkit::{connect, native_connector, wait_for_query};

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
        .expect("build local Edge/Core harness runtime")
        .block_on(tokio::task::LocalSet::new().run_until(run()));
}

async fn run() {
    let run_nonce = std::env::var("JAZZ_DEVICE_RUN_NONCE").expect("host run nonce is required");
    let title = format!("high-level-foreground-row:{run_nonce}");
    let issuer = TestJwtIssuer::start().await;
    let app_id = AppId::from_name("jazz-device-acceptance");
    let schema = device_fixture::schema();
    let core = JazzServer::builder()
        .with_app_id(app_id)
        .with_schema(schema.clone())
        .with_jwks_url(issuer.endpoint())
        .with_native_transport_connector(native_connector())
        .start()
        .await;
    let edge = JazzServer::builder()
        .with_app_id(core.app_id())
        .with_schema(schema.clone())
        .with_jwks_url(issuer.endpoint())
        .with_admin_secret(core.admin_secret().to_owned())
        .with_upstream_url(core.base_url())
        .with_native_transport_connector(native_connector())
        .start()
        .await;

    for _ in 0..300 {
        if edge.server_state().edge_upstream_health() == EdgeUpstreamHealth::Connected {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        edge.server_state().edge_upstream_health(),
        EdgeUpstreamHealth::Connected,
        "local Edge must attach Core before credentials are handed to a device fixture"
    );

    // A fresh, read-only observer attaches directly to Core, never Edge or
    // device SQLite. It cannot seed the acceptance marker itself. The device
    // is the only writer of this run-specific high-level foreground title.
    let observer_storage = tempfile::tempdir().expect("observer scratch directory");
    let observer = connect(AppContext {
        app_id: core.app_id(),
        client_id: None,
        schema,
        server_url: core.base_url(),
        data_dir: observer_storage.path().to_owned(),
        storage: ClientStorage::Memory,
        storage_factory: None,
        jwt_token: Some(TestJwtIssuer::jwt_for_user("rn-device-core-observer")),
        backend_secret: None,
        admin_secret: Some(core.admin_secret().to_owned()),
    })
    .await
    .expect("connect read-only Core observer");

    // Android reaches the host loopback listener through 10.0.2.2. The
    // driver derives that endpoint from `edge_port`; keeping it out of this
    // process means the same harness remains usable by non-Android hosts.
    let receipt = serde_json::json!({
        "edge_port": edge.port(),
        "app_id": core.app_id().to_string(),
        "bearer_a": TestJwtIssuer::jwt_for_user("rn-device-private-a"),
        "bearer_b": TestJwtIssuer::jwt_for_user("rn-device-private-b"),
    });
    println!("JAZZ_RN_EDGE_SESSION {receipt}");
    std::io::stdout().flush().expect("flush harness receipt");

    let row_id = wait_for_query(
        &observer,
        Query::from("todos"),
        Some(DurabilityTier::GlobalServer),
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

    // The parent owns process lifetime. It kills this local-only fixture after
    // both installed-app launches, which also ensures credentials cannot be
    // accidentally reused by a later driver invocation.
    std::future::pending::<()>().await;
}
