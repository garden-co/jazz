//! Exercise the real HTTP router and persistent server. The
//! runtime snapshot assertions are internal because HTTP graph inventory cannot
//! establish which physical lineage the core runtime installed.
use super::super::{ServerBuilder, StorageBackend, routes::create_router};
use super::*;
use crate::middleware::AuthConfig;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use jazz::tools::{
    AppId,
    public_schema::{ColumnType, PolicyExpr, SchemaBuilder, TableSchema},
};
use serde_json::{Value, json};
use tower::ServiceExt;

fn schema(columns: &[&str]) -> Schema {
    let mut table = TableSchema::builder("notes").column("title", ColumnType::Text);
    for column in columns {
        table = table.column(*column, ColumnType::Text);
    }
    SchemaBuilder::new().table(table).build()
}
fn app_id() -> AppId {
    AppId::from_name("deployment-tests")
}
fn builder(path: Option<&std::path::Path>) -> ServerBuilder {
    let builder = ServerBuilder::new(app_id()).with_auth_config(AuthConfig {
        admin_secret: Some("test-admin".into()),
        ..Default::default()
    });
    match path {
        Some(path) => builder
            .with_storage(StorageBackend::Persistent {
                path: path.to_path_buf(),
            })
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory)),
        None => builder.with_storage(StorageBackend::InMemory),
    }
}
fn request(target: &Schema, schemas: &[&Schema], migrations: Vec<Value>) -> Value {
    json!({"targetSchemaHash":SchemaHash::compute(target).to_string(),
        "schemas":schemas.iter().map(|schema|json!({"hash":SchemaHash::compute(schema).to_string(),"schema":schema})).collect::<Vec<_>>(),
        "migrations":migrations,"permissions":HashMap::<TableName,TablePolicies>::new()})
}
fn migration(source: &Schema, target: &Schema, added: &[&str]) -> Value {
    json!({"fromHash":SchemaHash::compute(source).to_string(),"toHash":SchemaHash::compute(target).to_string(),
        "forward":[{"table":"notes","operations":added.iter().map(|column|json!({"type":"introduce","column":column,"column_type":ColumnType::Text,"value":jazz::tools::public_schema::Value::Text("default".into())})).collect::<Vec<_>>()}]})
}
async fn http(router: &Router, method: &str, path: &str, body: Value) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(format!("/apps/{}/admin{path}", app_id()))
                .header("X-Jazz-Admin-Secret", "test-admin")
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value =
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)));
    (status, value)
}
async fn deploy_ok(router: &Router, request: Value) -> Value {
    let (status, body) = http(router, "POST", "/deploy", request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}
async fn graph(router: &Router) -> Value {
    let (status, body) = http(router, "GET", "/migrations/graph", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

#[tokio::test]
async fn deploy_initial_permission_only_and_unchanged_requests() {
    let server = builder(None).build().await.unwrap();
    let router = create_router(server.state.clone());
    let base = schema(&[]);
    let initial = request(&base, &[&base], vec![]);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/apps/{}/admin/deploy", app_id()))
                .header("Content-Type", "application/json")
                .body(Body::from(initial.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let mut missing = initial.clone();
    missing.as_object_mut().unwrap().remove("permissions");
    assert_eq!(
        http(&router, "POST", "/deploy", missing).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(graph(&router).await["schemas"], json!([]));
    let result = deploy_ok(&router, initial.clone()).await;
    assert_eq!(result["changed"], true);
    assert_eq!(
        result["published"]["schemas"],
        json!([SchemaHash::compute(&base).to_string()])
    );
    let head = http(&router, "GET", "/permissions/head", Value::Null)
        .await
        .1;
    assert_eq!(deploy_ok(&router, initial).await["changed"], false);
    assert_eq!(
        http(&router, "GET", "/permissions/head", Value::Null)
            .await
            .1,
        head
    );
    let mut allow_read = request(&base, &[], vec![]);
    allow_read["permissions"] = serde_json::to_value(HashMap::from([(
        TableName::new("notes"),
        TablePolicies::new().with_select(PolicyExpr::True),
    )]))
    .unwrap();
    let result = deploy_ok(&router, allow_read.clone()).await;
    assert_eq!(
        result,
        json!({"changed":true,"published":{"schemas":[],"migrations":[]}})
    );
    assert_eq!(deploy_ok(&router, allow_read).await["changed"], false);
    let stored = http(&router, "GET", "/permissions", Value::Null).await.1;
    assert!(stored["permissions"]["notes"].is_object(), "{stored}");
    server.shutdown().await;
}

#[tokio::test]
async fn deploy_complete_diamond_reopens_and_rejects_incomplete_history() {
    let dir = tempfile::tempdir().unwrap();
    let server = builder(Some(dir.path())).build().await.unwrap();
    let router = create_router(server.state.clone());
    let (a, b, c, d) = (
        schema(&[]),
        schema(&["a"]),
        schema(&["b"]),
        schema(&["a", "b"]),
    );
    deploy_ok(&router, request(&a, &[&a], vec![])).await;
    // Publish B first, then merge C and D in one deployment using the stored A/B.
    deploy_ok(&router, request(&b, &[&b], vec![migration(&a, &b, &["a"])])).await;
    let before = graph(&router).await;
    // The sibling-detour case must fail: B has no forward migration to D.
    let incomplete = request(
        &d,
        &[&c, &d],
        vec![migration(&a, &c, &["b"]), migration(&c, &d, &["a"])],
    );
    let (status, error) = http(&router, "POST", "/deploy", incomplete.clone()).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{error}");
    assert_eq!(error["code"], "non_convergent_graph");
    assert_eq!(graph(&router).await, before);
    let mut complete = incomplete;
    complete["migrations"]
        .as_array_mut()
        .unwrap()
        .push(migration(&b, &d, &["b"]));
    let result = deploy_ok(&router, complete).await;
    assert_eq!(result["published"]["schemas"].as_array().unwrap().len(), 2);
    assert_eq!(
        result["published"]["migrations"].as_array().unwrap().len(),
        3
    );
    let expected = graph(&router).await;
    assert_eq!(
        expected["activeSchemaHash"],
        SchemaHash::compute(&d).to_string()
    );
    assert_eq!(expected["schemas"].as_array().unwrap().len(), 4);
    let snapshot = server
        .state
        .runtime()
        .unwrap()
        .trusted_catalogue_snapshot()
        .await
        .unwrap();
    assert_eq!(
        snapshot.current_write_schema.schema,
        JazzSchema::new(&d).unwrap().version_id()
    );
    assert_eq!(
        snapshot
            .lineages
            .iter()
            .find(|(_, p)| p.schema.id == snapshot.current_write_schema.schema)
            .unwrap()
            .1
            .predecessors
            .len(),
        2
    );
    assert_eq!(
        http(&router, "POST", "/deploy", request(&b, &[], vec![]))
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    server.shutdown().await;
    drop(router);
    drop(server);
    let reopened = builder(Some(dir.path())).build().await.unwrap();
    let router = create_router(reopened.state.clone());
    assert_eq!(graph(&router).await, expected);
    assert_eq!(
        deploy_ok(&router, request(&d, &[], vec![])).await["changed"],
        false
    );
    reopened.shutdown().await;
}

#[tokio::test]
async fn deploy_validation_failure_leaves_no_durable_partial_catalogue() {
    let dir = tempfile::tempdir().unwrap();
    let server = builder(Some(dir.path())).build().await.unwrap();
    let router = create_router(server.state.clone());
    let (a, b) = (schema(&[]), schema(&["a"]));
    let mut invalid = request(&b, &[&a, &b], vec![migration(&a, &b, &["a"])]);
    invalid["permissions"] = serde_json::to_value(HashMap::from([(
        TableName::new("missing"),
        TablePolicies::new().with_select(PolicyExpr::True),
    )]))
    .unwrap();
    let invalid_schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .column("title", ColumnType::Integer),
        )
        .build();
    let mut invalid_hash = request(&a, &[&a], vec![]);
    invalid_hash["schemas"][0]["hash"] = json!(SchemaHash::compute(&b).to_string());
    for (body, code) in [
        (invalid, "invalid_permissions"),
        (
            request(&invalid_schema, &[&invalid_schema], vec![]),
            "invalid_schema",
        ),
        (invalid_hash, "schema_hash_mismatch"),
        (
            request(&b, &[&a, &b], vec![migration(&a, &b, &[])]),
            "invalid_migration",
        ),
        (request(&b, &[&a, &b], vec![]), "non_convergent_graph"),
    ] {
        let (status, error) = http(&router, "POST", "/deploy", body).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{error}");
        assert_eq!(error["code"], code, "{error}");
        assert_eq!(graph(&router).await["schemas"], json!([]));
    }
    server.shutdown().await;
    drop(router);
    drop(server);
    let reopened = builder(Some(dir.path())).build().await.unwrap();
    let router = create_router(reopened.state.clone());
    assert_eq!(graph(&router).await["schemas"], json!([]));
    deploy_ok(&router, request(&a, &[&a], vec![])).await;
    reopened.shutdown().await;
}

#[tokio::test]
async fn deploy_concurrent_branches_revalidate_against_the_committed_graph() {
    let server = builder(None).build().await.unwrap();
    let router = create_router(server.state.clone());
    let (a, b, c) = (schema(&[]), schema(&["a"]), schema(&["b"]));
    deploy_ok(&router, request(&a, &[&a], vec![])).await;
    let (left, right) = tokio::join!(
        http(
            &router,
            "POST",
            "/deploy",
            request(&b, &[&b], vec![migration(&a, &b, &["a"])])
        ),
        http(
            &router,
            "POST",
            "/deploy",
            request(&c, &[&c], vec![migration(&a, &c, &["b"])])
        ),
    );
    let (accepted, rejected) = if left.0 == StatusCode::OK {
        (b, right)
    } else {
        assert_eq!(right.0, StatusCode::OK, "{}", right.1);
        (c, left)
    };
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert_eq!(rejected.1["code"], "non_convergent_graph");
    let graph = graph(&router).await;
    assert_eq!(
        graph["activeSchemaHash"],
        SchemaHash::compute(&accepted).to_string()
    );
    assert_eq!(graph["schemas"].as_array().unwrap().len(), 2);
    server.shutdown().await;
}

#[tokio::test]
async fn deploy_explicit_empty_schema_is_not_a_legacy_initialization_sentinel() {
    let dir = tempfile::tempdir().unwrap();
    let server = builder(Some(dir.path())).build().await.unwrap();
    let router = create_router(server.state.clone());
    let empty = SchemaBuilder::new().build();
    deploy_ok(&router, request(&empty, &[&empty], vec![])).await;
    server.shutdown().await;
    drop(router);
    drop(server);
    let reopened = builder(Some(dir.path())).build().await.unwrap();
    let router = create_router(reopened.state.clone());
    assert_eq!(
        graph(&router).await["activeSchemaHash"],
        SchemaHash::compute(&empty).to_string()
    );
    assert_eq!(
        deploy_ok(&router, request(&empty, &[], vec![])).await["changed"],
        false
    );
    reopened.shutdown().await;
}

#[tokio::test]
async fn deploy_runtime_predecessor_validation_precedes_storage_writes() {
    let server = builder(None).build().await.unwrap();
    let router = create_router(server.state.clone());
    let (a, b, c, d) = (
        schema(&[]),
        schema(&["a"]),
        schema(&["b"]),
        schema(&["a", "b"]),
    );
    deploy_ok(
        &router,
        request(
            &d,
            &[&a, &b, &d],
            vec![migration(&a, &b, &["a"]), migration(&b, &d, &["b"])],
        ),
    )
    .await;
    let before = graph(&router).await;
    // The combined graph is valid, but D's already-published physical lineage
    // cannot acquire another predecessor. C must not be stored on rejection.
    let (status, error) = http(
        &router,
        "POST",
        "/deploy",
        request(
            &d,
            &[&c],
            vec![migration(&a, &c, &["b"]), migration(&c, &d, &["a"])],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{error}");
    assert!(
        error["error"]
            .as_str()
            .unwrap()
            .contains("different predecessors"),
        "{error}"
    );
    assert_eq!(graph(&router).await, before);
    server.shutdown().await;
}

/// An administrator deploys compatible revisions without migration files, then
/// adds a column with an explicit migration and uploads an older compatible snapshot.
/// Reopening and repeating the deployment must preserve the same graph.
#[tokio::test]
async fn compatible_deployment_preserves_hashes_and_history() {
    let dir = tempfile::tempdir().unwrap();
    let server = builder(Some(dir.path())).build().await.unwrap();
    let router = create_router(server.state.clone());
    let a = schema(&["body"]);
    let b = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("body", ColumnType::Text)
                .column_with_default(
                    "title",
                    ColumnType::Text,
                    jazz::tools::Value::Text("draft".into()),
                )
                .index_only(["title"]),
        )
        .build();
    deploy_ok(&router, request(&a, &[&a], vec![])).await;
    deploy_ok(&router, request(&b, &[&b], vec![])).await;
    let c = schema(&["body", "extra"]);
    deploy_ok(
        &router,
        request(&c, &[&c], vec![migration(&b, &c, &["extra"])]),
    )
    .await;
    let historical = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column_with_default(
                    "title",
                    ColumnType::Text,
                    jazz::tools::Value::Text("historical".into()),
                )
                .column("body", ColumnType::Text),
        )
        .build();
    deploy_ok(&router, request(&c, &[&historical], vec![])).await;
    let inventory = graph(&router).await;
    assert_eq!(
        inventory["activeSchemaHash"],
        SchemaHash::compute(&c).to_string()
    );
    assert_eq!(inventory["schemas"].as_array().unwrap().len(), 4);
    let edges = inventory["migrations"].as_array().unwrap();
    assert_eq!(
        edges
            .iter()
            .filter(|edge| edge["automatic"] == true)
            .count(),
        2
    );
    assert!(edges.iter().any(
        |edge| edge["fromHash"] == SchemaHash::compute(&a).to_string()
            && edge["toHash"] == SchemaHash::compute(&b).to_string()
            && edge["automatic"] == true
    ));
    assert_eq!(
        deploy_ok(&router, request(&c, &[], vec![])).await["changed"],
        false
    );
    server.shutdown().await;
    drop(router);
    drop(server);
    let reopened = builder(Some(dir.path())).build().await.unwrap();
    assert_eq!(
        graph(&create_router(reopened.state.clone())).await,
        inventory
    );
    reopened.shutdown().await;
}

/// An administrator changes defaults and indexes on both genesis and descendant
/// schemas; metadata that shares a runtime ID must not create a self-lineage.
#[tokio::test]
async fn compatible_defaults_and_indexes_share_runtime_identity() {
    let server = builder(None).build().await.unwrap();
    let router = create_router(server.state.clone());
    let a = schema(&[]);
    let b = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column_with_default(
                    "title",
                    ColumnType::Text,
                    jazz::tools::Value::Text("draft".into()),
                )
                .index_only(std::iter::empty::<&str>()),
        )
        .build();
    deploy_ok(&router, request(&a, &[&a], vec![])).await;
    deploy_ok(&router, request(&b, &[&b], vec![])).await;
    let snapshot = server
        .state
        .runtime()
        .unwrap()
        .trusted_catalogue_snapshot()
        .await
        .unwrap();
    assert_eq!(snapshot.schemas.len(), 1);
    assert!(snapshot.lineages.is_empty());
    let c = schema(&["extra"]);
    let before = graph(&router).await;
    assert_eq!(
        http(&router, "POST", "/deploy", request(&c, &[&c], vec![]))
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(graph(&router).await, before);
    server.shutdown().await;
}

/// The administrator cannot bypass immutable branch defaults or publish
/// incompatible parallel projections through compatible target definitions.
#[tokio::test]
async fn compatible_connections_preserve_branch_and_convergence_validation() {
    let server = builder(None).build().await.unwrap();
    let router = create_router(server.state.clone());
    let branch = |byte| {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("notes")
                    .column_with_default(
                        "branch",
                        ColumnType::Uuid,
                        jazz::tools::Value::Uuid(jazz::tools::ObjectId::from_uuid(
                            uuid::Uuid::from_bytes([byte; 16]),
                        )),
                    )
                    .column("title", ColumnType::Text)
                    .branch_by("branch"),
            )
            .build()
    };
    let a = branch(1);
    let b = branch(2);
    deploy_ok(&router, request(&a, &[&a], vec![])).await;
    let before = graph(&router).await;
    assert_eq!(
        http(&router, "POST", "/deploy", request(&b, &[&b], vec![]))
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(graph(&router).await, before);
    server.shutdown().await;

    let server = builder(None).build().await.unwrap();
    let router = create_router(server.state.clone());
    let a = schema(&[]);
    let b = schema(&["body"]);
    let c = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("body", ColumnType::Text)
                .column("title", ColumnType::Text),
        )
        .build();
    let mut conflicting = migration(&a, &c, &["body"]);
    conflicting["forward"][0]["operations"][0]["value"] =
        json!(jazz::tools::Value::Text("different".into()));
    let (status, error) = http(
        &router,
        "POST",
        "/deploy",
        request(
            &c,
            &[&a, &b, &c],
            vec![migration(&a, &b, &["body"]), conflicting],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(error["code"], "conflicting_paths");
    assert_eq!(graph(&router).await["schemas"], json!([]));
    server.shutdown().await;
}
