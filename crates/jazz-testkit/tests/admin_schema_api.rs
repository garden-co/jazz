use jazz::tools::{ColumnType, Schema, SchemaBuilder, TableSchema};
use jazz_server::JazzServer;
use serde_json::{Value, json};

const ADMIN_SECRET: &str = "secret";
const MAX_ADMIN_REQUEST_BODY_BYTES: usize = 8 << 20;

async fn start_server() -> JazzServer {
    JazzServer::builder()
        .with_admin_secret(ADMIN_SECRET)
        .start()
        .await
}

fn app_url(server: &JazzServer, path: &str) -> String {
    format!("{}/apps/{}{}", server.base_url(), server.app_id(), path)
}

fn admin_request(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    request.header("X-Jazz-Admin-Secret", ADMIN_SECRET)
}

async fn json_response(response: reqwest::Response) -> Value {
    response.json().await.expect("response body is JSON")
}

fn schema_with_column(table: &str, column: &str, column_type: ColumnType) -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder(table).column(column, column_type))
        .build()
}

fn publish_body(schema: Schema) -> Value {
    json!({ "schema": schema, "permissions": null })
}

#[tokio::test]
async fn admin_schema_api_requires_secret_and_rejects_permissions() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let url = app_url(&server, "/admin/schemas");
    let body = publish_body(SchemaBuilder::new().build());

    assert_eq!(
        client.post(&url).json(&body).send().await.unwrap().status(),
        401
    );
    assert_eq!(
        client
            .post(&url)
            .header("X-Jazz-Admin-Secret", "wrong")
            .json(&body)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );

    let rejected = admin_request(client.post(&url))
        .json(&json!({ "schema": SchemaBuilder::new().build(), "permissions": {} }))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 400);

    server.shutdown().await;
}

#[tokio::test]
async fn admin_schema_api_rejects_oversized_bodies_before_the_handler_runs() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let response = admin_request(client.post(app_url(&server, "/admin/schemas")))
        .header("content-type", "application/json")
        .body(vec![b' '; MAX_ADMIN_REQUEST_BODY_BYTES + 1])
        .send()
        .await
        .expect("send oversized schema request");

    assert_eq!(response.status(), 413);

    server.shutdown().await;
}

#[tokio::test]
async fn admin_schema_api_publishes_lists_and_gets_schema_json() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let publish_url = app_url(&server, "/admin/schemas");
    let published = admin_request(client.post(&publish_url))
        .json(&publish_body(schema_with_column(
            "todos",
            "title",
            ColumnType::Text,
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(published.status(), 201);
    let published = json_response(published).await;
    let hash = published["hash"].as_str().expect("schema hash").to_owned();
    assert_eq!(hash.len(), 64);
    assert!(
        !published["objectId"]
            .as_str()
            .expect("object id")
            .is_empty()
    );

    let schemas_url = app_url(&server, "/schemas");
    assert_eq!(client.get(&schemas_url).send().await.unwrap().status(), 401);
    let list = admin_request(client.get(&schemas_url))
        .send()
        .await
        .unwrap();
    assert_eq!(list.status(), 200);
    let list = json_response(list).await;
    assert_eq!(list["hashes"], json!([hash]));

    let fetched = admin_request(client.get(app_url(&server, &format!("/schema/{hash}"))))
        .send()
        .await
        .unwrap();
    assert_eq!(fetched.status(), 200);
    let fetched = json_response(fetched).await;
    assert!(fetched["publishedAt"].as_u64().expect("publishedAt") > 0);
    assert!(fetched["schema"]["tables"]["todos"].is_object());

    let missing = admin_request(client.get(app_url(
        &server,
        "/schema/0000000000000000000000000000000000000000000000000000000000000000",
    )))
    .send()
    .await
    .unwrap();
    assert_eq!(missing.status(), 404);

    server.shutdown().await;
}

#[tokio::test]
async fn admin_schema_api_rejects_noncanonical_schema_payloads() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let url = app_url(&server, "/admin/schemas");

    // These intentionally bypass the schema builders: the HTTP boundary must
    // reject old/noncanonical JSON that no public Rust schema API can create.
    for schema in [
        json!({
            "todos": {
                "columns": [{ "name": "title", "column_type": "Text" }]
            }
        }),
        json!({
            "tables": {
                "todos": {
                    "columns": [{
                        "name": "metadata",
                        "column_type": { "type": "Removed" },
                        "nullable": false
                    }]
                }
            }
        }),
    ] {
        let response = admin_request(client.post(&url))
            .json(&json!({ "schema": schema, "permissions": null }))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_client_error(), "{}", response.status());
    }

    server.shutdown().await;
}

#[tokio::test]
async fn admin_schema_api_accepts_public_schema_tables_wrapper() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let response = admin_request(client.post(app_url(&server, "/admin/schemas")))
        .json(&publish_body(
            SchemaBuilder::new()
                .table(
                    TableSchema::builder("events")
                        .column("id", ColumnType::Uuid)
                        .column("seenAt", ColumnType::Timestamp)
                        .column("score", ColumnType::Double),
                )
                .build(),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);

    server.shutdown().await;
}

#[tokio::test]
async fn admin_schema_api_persists_catalogue_in_the_production_storage_backend() {
    let data_dir = tempfile::tempdir().expect("temporary server data directory");
    let app_id = JazzServer::default_app_id();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .with_admin_secret(ADMIN_SECRET)
        .with_data_dir(data_dir.path())
        .with_storage_factory(jazz_testkit::persistent_storage_factory())
        .start()
        .await;
    let client = reqwest::Client::new();
    let published = admin_request(client.post(app_url(&server, "/admin/schemas")))
        .json(&publish_body(schema_with_column(
            "notes",
            "body",
            ColumnType::Text,
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(published.status(), 201);
    let hash = json_response(published).await["hash"]
        .as_str()
        .expect("schema hash")
        .to_owned();
    server.shutdown().await;

    let restarted = JazzServer::builder()
        .with_app_id(app_id)
        .with_admin_secret(ADMIN_SECRET)
        .with_data_dir(data_dir.path())
        .with_storage_factory(jazz_testkit::persistent_storage_factory())
        .start()
        .await;
    let list = admin_request(client.get(app_url(&restarted, "/schemas")))
        .send()
        .await
        .unwrap();
    assert_eq!(list.status(), 200);
    assert_eq!(json_response(list).await["hashes"], json!([hash]));

    restarted.shutdown().await;
}
