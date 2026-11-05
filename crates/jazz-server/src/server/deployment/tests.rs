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
        Some(path) => builder.with_storage(StorageBackend::Persistent {
            path: path.to_path_buf(),
        }),
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
    let (status, body) = http(router, "POST", "/bundles", request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}
/// Publish a deployment as a bundle, activating nothing (createJazz).
async fn publish_ok(router: &Router, request: Value) -> Value {
    let (status, body) = http(router, "POST", "/bundles", request).await;
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
                .uri(format!("/apps/{}/admin/bundles", app_id()))
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
        http(&router, "POST", "/bundles", missing).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(graph(&router).await["schemas"], json!([]));
    let result = deploy_ok(&router, initial.clone()).await;
    assert_eq!(result["changed"], true);
    assert_eq!(
        result["published"]["schemas"],
        json!([SchemaHash::compute(&base).to_string()])
    );
    let first = result["bundle"].clone();
    let again = deploy_ok(&router, initial).await;
    assert_eq!(again["changed"], false);
    assert_eq!(again["bundle"], first);
    let mut allow_read = request(&base, &[], vec![]);
    allow_read["permissions"] = serde_json::to_value(HashMap::from([(
        TableName::new("notes"),
        TablePolicies::new().with_select(PolicyExpr::True),
    )]))
    .unwrap();
    let result = deploy_ok(&router, allow_read.clone()).await;
    assert_eq!(result["changed"], true);
    assert_eq!(
        result["published"],
        json!({"schemas":[],"migrations":[]}),
        "a permission-only change publishes only its bundle"
    );
    assert_ne!(result["bundle"], first);
    let again = deploy_ok(&router, allow_read).await;
    assert_eq!(again["changed"], false);
    assert_eq!(again["bundle"], result["bundle"]);
    server.shutdown().await;
}

#[tokio::test]
async fn deploy_complete_diamond_reopens_and_rejects_lossy_history() {
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
    let incomplete = request(
        &d,
        &[&c, &d],
        vec![migration(&a, &c, &["b"]), migration(&c, &d, &["a"])],
    );
    // Completing it with B -> D introducing `b` leaves the two lenses into D
    // disagreeing on `a` (C's introduces it, B's keeps B's): not supported yet.
    let mut lossy = incomplete;
    lossy["migrations"]
        .as_array_mut()
        .unwrap()
        .push(migration(&b, &d, &["b"]));
    let (status, error) = http(&router, "POST", "/bundles", lossy).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{error}");
    assert!(
        error["error"]
            .as_str()
            .unwrap()
            .contains("not supported yet"),
        "{error}"
    );
    assert_eq!(graph(&router).await, before);
    // A diamond whose branches both keep B's `a` merges.
    let wide = schema(&["a", "tmp"]);
    let mut narrow = migration(&wide, &d, &["b"]);
    narrow["forward"][0]["operations"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"drop","column":"tmp","column_type":ColumnType::Text,"value":jazz::tools::public_schema::Value::Text("default".into())}));
    let complete = request(
        &d,
        &[&wide, &d],
        vec![
            migration(&b, &wide, &["tmp"]),
            narrow,
            migration(&b, &d, &["b"]),
        ],
    );
    let result = deploy_ok(&router, complete).await;
    assert_eq!(result["published"]["schemas"].as_array().unwrap().len(), 2);
    assert_eq!(
        result["published"]["migrations"].as_array().unwrap().len(),
        3
    );
    let expected = graph(&router).await;
    assert_eq!(expected["schemas"].as_array().unwrap().len(), 4);
    // D is admitted, reached by both branches of the diamond.
    let catalogue = server.state.runtime().unwrap().catalogue().await.unwrap();
    let merged = catalogue
        .schemas
        .iter()
        .find(|schema| schema.version == Some(JazzSchema::new(&d).unwrap().version_id()))
        .expect("D is admitted");
    assert_eq!(
        catalogue
            .lenses
            .iter()
            .filter(|lens| lens.target == merged.hash)
            .count(),
        2
    );
    // Publishing B's bundle again (a rollback) leaves the graph as it is.
    deploy_ok(&router, request(&b, &[], vec![])).await;
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
        (
            request(&b, &[&a, &b], vec![]),
            "disconnected_migration_graph",
        ),
    ] {
        let (status, error) = http(&router, "POST", "/bundles", body).await;
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
async fn deploy_concurrent_branches_both_publish() {
    let server = builder(None).build().await.unwrap();
    let router = create_router(server.state.clone());
    let (a, b, c) = (schema(&[]), schema(&["a"]), schema(&["b"]));
    deploy_ok(&router, request(&a, &[&a], vec![])).await;
    let (left, right) = tokio::join!(
        http(
            &router,
            "POST",
            "/bundles",
            request(&b, &[&b], vec![migration(&a, &b, &["a"])])
        ),
        http(
            &router,
            "POST",
            "/bundles",
            request(&c, &[&c], vec![migration(&a, &c, &["b"])])
        ),
    );
    // Two backends publishing sibling branches of A serve side by side.
    assert_eq!(left.0, StatusCode::OK, "{}", left.1);
    assert_eq!(right.0, StatusCode::OK, "{}", right.1);
    let graph = graph(&router).await;
    let mut schemas = graph["schemas"]
        .as_array()
        .unwrap()
        .iter()
        .map(|schema| schema.as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    schemas.sort();
    let mut expected = [&a, &b, &c]
        .map(|schema| SchemaHash::compute(schema).to_string())
        .to_vec();
    expected.sort();
    assert_eq!(schemas, expected);
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
        graph(&router).await["schemas"].as_array().unwrap().len(),
        1,
        "the empty schema outlives a restart"
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
        "/bundles",
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

/// A lens from another hash of an admitted predecessor (an index does not
/// change the version) is the same edge, so it does not count as a new
/// predecessor of an admitted target.
#[tokio::test]
async fn deploy_accepts_a_lens_from_an_equivalent_predecessor_hash() {
    let server = builder(None).build().await.unwrap();
    let router = create_router(server.state.clone());
    let (a, b) = (schema(&[]), schema(&["a"]));
    deploy_ok(
        &router,
        request(&b, &[&a, &b], vec![migration(&a, &b, &["a"])]),
    )
    .await;
    let indexed = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .index_only(["title"]),
        )
        .build();
    assert_ne!(SchemaHash::compute(&indexed), SchemaHash::compute(&a));
    deploy_ok(
        &router,
        request(&b, &[&indexed], vec![migration(&indexed, &b, &["a"])]),
    )
    .await;
    server.shutdown().await;
}

/// The same edge from an equivalent predecessor hash must carry the same lens:
/// one that introduces the column with another default would change how rows
/// of that version read under the admitted target, so it is refused. Path
/// validation sees the disagreement first; `prepare_catalogue_change` refuses
/// it again on its own (its unit tests).
#[tokio::test]
async fn deploy_refuses_a_different_lens_from_an_equivalent_predecessor_hash() {
    let server = builder(None).build().await.unwrap();
    let router = create_router(server.state.clone());
    let (a, b) = (schema(&[]), schema(&["a"]));
    deploy_ok(
        &router,
        request(&b, &[&a, &b], vec![migration(&a, &b, &["a"])]),
    )
    .await;
    let before = graph(&router).await;
    let indexed = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .index_only(["title"]),
        )
        .build();
    let mut different = migration(&indexed, &b, &["a"]);
    different["forward"][0]["operations"][0]["value"] =
        json!(jazz::tools::public_schema::Value::Text("other".into()));
    let (status, error) = http(
        &router,
        "POST",
        "/bundles",
        request(&b, &[&indexed], vec![different]),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{error}");
    assert_eq!(error["code"], "conflicting_paths", "{error}");
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
    let catalogue = server.state.runtime().unwrap().catalogue().await.unwrap();
    assert_eq!(
        catalogue
            .schemas
            .iter()
            .filter(|schema| schema.version.is_some())
            .count(),
        1,
        "one admitted version for both definitions"
    );
    let c = schema(&["extra"]);
    let before = graph(&router).await;
    assert_eq!(
        http(&router, "POST", "/bundles", request(&c, &[&c], vec![]))
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
        http(&router, "POST", "/bundles", request(&b, &[&b], vec![]))
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
        "/bundles",
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

/// Switching `notes` to latest-only over HTTP is refused with
/// `drops_history` naming the table, and nothing changes; the same request
/// with `allowDroppingHistory` deploys it.
#[tokio::test]
async fn switching_a_table_to_latest_only_needs_allow_dropping_history() {
    let server = builder(None).build().await.unwrap();
    let router = create_router(server.state.clone());
    let full = schema(&[]);
    let mut latest = full.clone();
    latest.get_mut(&TableName::new("notes")).unwrap().history =
        jazz::tools::HistoryRetention::Latest;
    deploy_ok(&router, request(&full, &[&full], vec![])).await;
    let before = graph(&router).await;

    let switch = request(&latest, &[&latest], vec![]);
    let (status, error) = http(&router, "POST", "/bundles", switch.clone()).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{error}");
    assert_eq!(error["code"], "drops_history", "{error}");
    assert_eq!(error["details"]["tables"], json!(["notes"]));
    assert_eq!(graph(&router).await, before);

    let mut confirmed = switch;
    confirmed["allowDroppingHistory"] = json!(true);
    assert_eq!(deploy_ok(&router, confirmed).await["changed"], true);
    server.shutdown().await;
}

/// A deployment published to `/bundles` publishes its permissions as a
/// bundle, by content: the same bundle again (a rollback) changes nothing,
/// and the current permissions stay as they were.
/// `GET /bundles` lists both bundles, each with its write schema and
/// permissions; a rollback adds no third.
async fn assert_published_bundles(
    router: &Router,
    open: &str,
    closed: &str,
    base: &Schema,
    next: &Schema,
) {
    let (status, body) = http(router, "GET", "/bundles", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut listed = body["bundles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|bundle| {
            (
                bundle["bundle"].as_str().unwrap().to_owned(),
                bundle["schemaHash"].as_str().unwrap().to_owned(),
                bundle["permissions"]["notes"]["select"]["using"].to_string(),
            )
        })
        .collect::<Vec<_>>();
    listed.sort();
    let mut expected = vec![
        (
            open.to_owned(),
            SchemaHash::compute(base).to_string(),
            serde_json::to_value(PolicyExpr::True).unwrap().to_string(),
        ),
        (
            closed.to_owned(),
            SchemaHash::compute(next).to_string(),
            serde_json::to_value(PolicyExpr::False).unwrap().to_string(),
        ),
    ];
    expected.sort();
    assert_eq!(listed, expected, "{body}");
}

#[tokio::test]
async fn publish_only_deployments_publish_content_addressed_bundles() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let base = schema(&[]);
    let next = schema(&["body"]);
    let with = |mut request: Value, read: PolicyExpr| {
        request["permissions"] = serde_json::to_value(HashMap::from([(
            TableName::new("notes"),
            TablePolicies::new().with_select(read),
        )]))
        .unwrap();
        request
    };
    let (open, closed) = {
        let server = builder(Some(&path)).build().await.unwrap();
        let router = create_router(server.state.clone());
        let first = publish_ok(
            &router,
            with(request(&base, &[&base], vec![]), PolicyExpr::True),
        )
        .await;
        assert_eq!(first["changed"], true);
        let open = first["bundle"].as_str().unwrap().to_owned();
        let again = publish_ok(&router, with(request(&base, &[], vec![]), PolicyExpr::True)).await;
        assert_eq!(again["changed"], false);
        assert_eq!(again["bundle"], json!(open));

        // A newer deployment with another schema and policies.
        let upgraded = publish_ok(
            &router,
            with(
                request(&next, &[&next], vec![migration(&base, &next, &["body"])]),
                PolicyExpr::False,
            ),
        )
        .await;
        assert_eq!(upgraded["changed"], true);
        let closed = upgraded["bundle"].as_str().unwrap().to_owned();
        assert_ne!(closed, open);

        // Rolling back to the first deployment publishes its bundle again.
        let rollback =
            publish_ok(&router, with(request(&base, &[], vec![]), PolicyExpr::True)).await;
        assert_eq!(rollback["changed"], false, "{rollback}");
        assert_eq!(rollback["bundle"], json!(open));

        assert_published_bundles(&router, &open, &closed, &base, &next).await;
        server.shutdown().await;
        (open, closed)
    };
    // Both bundles, and both write schemas, outlive a restart.
    let server = builder(Some(&path)).build().await.unwrap();
    let router = create_router(server.state.clone());
    assert_published_bundles(&router, &open, &closed, &base, &next).await;
    let rows = server.state.runtime().unwrap().catalogue().await.unwrap();
    let mut bundles = rows
        .bundles
        .iter()
        .map(|bundle| bundle.id.to_hex())
        .collect::<Vec<_>>();
    bundles.sort();
    let mut expected = vec![open, closed];
    expected.sort();
    assert_eq!(bundles, expected);
    assert!(
        rows.schemas
            .iter()
            .filter(|schema| schema.version.is_some())
            .count()
            >= 2,
        "the upgraded write schema is admitted"
    );
    server.shutdown().await;
}

/// A deployment publishes a bundle and admits its schemas, but no schema or
/// permissions become current, before or after the core reopens: sessions
/// write by the bundle they pin to.
#[tokio::test]
async fn deployments_never_make_a_schema_current() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (schema(&[]), schema(&["body"]));
    {
        let server = builder(Some(dir.path())).build().await.unwrap();
        let router = create_router(server.state.clone());
        deploy_ok(&router, request(&a, &[&a], vec![])).await;
        deploy_ok(
            &router,
            request(&b, &[&a, &b], vec![migration(&a, &b, &["body"])]),
        )
        .await;
        let catalogue = server.state.runtime().unwrap().catalogue().await.unwrap();
        assert!(catalogue.head().is_none());
        assert!(catalogue.permissions.is_empty());
        assert_eq!(catalogue.bundles.len(), 2);
        assert_eq!(
            graph(&router).await["latestBundleSchemaHash"],
            json!(SchemaHash::compute(&b).to_string())
        );
        server.shutdown().await;
    }
    let reopened = builder(Some(dir.path())).build().await.unwrap();
    let catalogue = reopened.state.runtime().unwrap().catalogue().await.unwrap();
    assert!(catalogue.head().is_none());
    assert_eq!(catalogue.bundles.len(), 2);
    reopened.shutdown().await;
}

/// The migration graph's latest bundle is the one published last, not the
/// newest schema: a bundle for an older schema published after a newer one
/// is the latest. Publishing an existing bundle again moves nothing.
#[tokio::test]
async fn the_latest_bundle_is_the_last_one_published() {
    let server = builder(None).build().await.unwrap();
    let router = create_router(server.state.clone());
    let (base, next) = (schema(&[]), schema(&["body"]));
    let with = |mut request: Value, read: PolicyExpr| {
        request["permissions"] = serde_json::to_value(HashMap::from([(
            TableName::new("notes"),
            TablePolicies::new().with_select(read),
        )]))
        .unwrap();
        request
    };
    let latest = |router: Router| async move {
        graph(&router).await["latestBundleSchemaHash"]
            .as_str()
            .map(str::to_owned)
    };
    let hash = |schema: &Schema| Some(SchemaHash::compute(schema).to_string());

    publish_ok(
        &router,
        with(request(&base, &[&base], vec![]), PolicyExpr::True),
    )
    .await;
    assert_eq!(latest(router.clone()).await, hash(&base));
    let upgrade = with(
        request(&next, &[&next], vec![migration(&base, &next, &["body"])]),
        PolicyExpr::False,
    );
    publish_ok(&router, upgrade.clone()).await;
    assert_eq!(latest(router.clone()).await, hash(&next));
    // New policies for the older schema, published right after: within the
    // same millisecond or not, this bundle is the latest.
    let older = publish_ok(
        &router,
        with(request(&base, &[], vec![]), PolicyExpr::False),
    )
    .await;
    assert_eq!(older["changed"], true, "{older}");
    assert_eq!(latest(router.clone()).await, hash(&base));
    let again = publish_ok(&router, upgrade).await;
    assert_eq!(again["changed"], false, "{again}");
    assert_eq!(latest(router.clone()).await, hash(&base));
    server.shutdown().await;
}
