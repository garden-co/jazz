use std::time::Duration;

use jazz::query::Query;
use jazz::tools::{
    AppContext, ClientStorage, ColumnType, DurabilityTier, SchemaBuilder, TableSchema, Value,
};
use jazz_server::{JazzServer, TestJwtIssuer};

use client_worker::TodoClient;

/// Alice enrols two native devices using only her JWT; a write on the first
/// device reaches the second through the server with the example's open policies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_user_enrols_retries_and_syncs_without_privileged_credentials() {
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
        storage_factory: None,
        account_id: None,
        jwt_token: Some(TestJwtIssuer::jwt_for_user("alice")),
        backend_secret: None,
        admin_secret: None,
    };

    let writer = TodoClient::connect(context.clone())
        .await
        .expect("enrol first device");
    let (id, _, _) = writer
        .insert("todos", jazz::row_input!("title" => "Native user smoke"))
        .await
        .expect("write as ordinary user");
    let reader = TodoClient::connect(context)
        .await
        .expect("retry enrolment on second device");
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
    .expect("ordinary-user write reaches another native worker");
    assert_eq!(
        rows,
        vec![(id, vec![Value::Text("Native user smoke".into())])]
    );

    drop(reader);
    drop(writer);
    server.shutdown().await;
}
