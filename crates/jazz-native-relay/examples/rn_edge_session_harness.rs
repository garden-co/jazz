//! Local-only Edge/Core session issuer for the Android installed-artifact
//! acceptance driver. It deliberately prints endpoint and short-lived bearer
//! material only to its direct parent process, never to a checked-in fixture.

use std::io::Write;

use jazz::tools::{AppId, ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_native_relay as _;
use jazz_server::{EdgeUpstreamHealth, JazzServer, TestJwtIssuer};
use jazz_testkit::native_connector;

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build local Edge/Core harness runtime")
        .block_on(run());
}

async fn run() {
    let issuer = TestJwtIssuer::start().await;
    let app_id = AppId::from_name("jazz-device-acceptance");
    let schema = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
        .build();
    let core = JazzServer::builder()
        .with_app_id(app_id)
        .with_schema(schema.clone())
        .with_jwks_url(issuer.endpoint())
        .with_native_transport_connector(native_connector())
        .start()
        .await;
    let edge = JazzServer::builder()
        .with_app_id(core.app_id())
        .with_schema(schema)
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

    // The parent owns process lifetime. It kills this local-only fixture after
    // both installed-app launches, which also ensures credentials cannot be
    // accidentally reused by a later driver invocation.
    std::future::pending::<()>().await;
}
