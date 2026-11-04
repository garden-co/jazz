use std::time::Duration;

use jazz::query::Query;
use jazz::tools::{
    AppContext, ClientStorage, ColumnType, DurabilityTier, SchemaBuilder, Session, TableSchema,
    Value,
};
use jazz_server::JazzServer;

use client_worker::TodoClient;

/// The service, as the app's backend, acts as Alice from two native workers:
/// both resolve her one account, and a write on the first reaches the second
/// through the server with the example's open policies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backend_acts_as_one_user_across_workers() {
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("todos").column("title", ColumnType::Text))
        .build();
    let server = JazzServer::builder()
        .with_app_id(jazz::tools::AppId::random())
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start isolated Jazz server");
    permissions_support::publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;
    let context = AppContext {
        app_id: server.app_id(),
        client_id: None,
        schema,
        server_url: server.base_url(),
        data_dir: std::env::temp_dir(),
        storage: ClientStorage::Memory,
        account_id: None,
        jwt_token: None,
        backend_secret: Some(server.backend_secret().to_owned()),
        backend_session: Some(Session::new(
            format!("jazz:app:{}", server.app_id()),
            "alice",
        )),
        admin_secret: None,
        migrations: Default::default(),
    };

    let writer = TodoClient::connect(context.clone())
        .await
        .expect("act as Alice on the first worker");
    let (id, _, _) = writer
        .insert("todos", jazz::row_input!("title" => "Native user smoke"))
        .await
        .expect("write as Alice");
    let reader = TodoClient::connect(context)
        .await
        .expect("resolve Alice's account again on the second worker");
    let rows = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = reader
                .query(Query::from("todos"), Some(DurabilityTier::GlobalServer))
                .await
                .expect("query synced rows");
            if !rows.is_empty() {
                break rows;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Alice's write reaches her other worker");
    assert_eq!(
        rows,
        vec![(id, vec![Value::Text("Native user smoke".into())])]
    );

    drop(reader);
    drop(writer);
    server.shutdown().await;
}
