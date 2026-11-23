//! An encrypted app changed through `migrations create`, the command an app
//! author runs, then deployed through the real HTTP router: the version the
//! command moves into `history` keeps its encryption declarations, so the
//! deploy's checks between versions see the same encrypted columns on both
//! sides.
//!
//! ```text
//! v1 files ──generate──► bindings ──(edit tables)──► migrations create
//!     │                                                   │
//!     ▼ POST /bundles                                     ▼ POST /bundles
//!   server keeps v1's declarations ──────────► v2 checked against them
//! ```
use super::super::{ServerBuilder, StorageBackend, routes::create_router};
use crate::middleware::AuthConfig;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use jazz::tools::AppId;
use jazz::tools::deployment::artifact;
use jazz_codegen::Lang;
use jazz_codegen::project::{self, Selected};
use serde_json::Value;
use tower::ServiceExt;

fn app_id() -> AppId {
    AppId::from_name("migrations-create-deployment-tests")
}

async fn router() -> Router {
    let server = ServerBuilder::new(app_id())
        .with_auth_config(AuthConfig {
            admin_secret: Some("test-admin".into()),
            ..Default::default()
        })
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .unwrap();
    create_router(server.state.clone())
}

async fn publish(router: &Router, body: Value) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/apps/{}/admin/bundles", app_id()))
                .header("X-Jazz-Admin-Secret", "test-admin")
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Two encrypted tables (each project is its own space, its tasks live in
/// it) and a plain one.
const APP_V1: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
  "tables": {
    "boards": { "columns": { "title": "string" } },
    "projects": {
      "columns": { "name": "bytes", "ownerId": "uuid" },
      "encryption": { "space": "id", "scope": "projects", "columns": { "name": "string" } }
    },
    "tasks": {
      "columns": {
        "projectId": "uuid",
        "title": "bytes",
        "__e2ee_eq_title": "bytes",
        "notes": "bytes",
        "done": "boolean"
      },
      "relations": { "project": { "rel": "projects", "column": "projectId" } },
      "encryption": {
        "space": "projectId",
        "scope": "projects",
        "columns": { "title": "string", "notes": "string?" },
        "equality": ["title"]
      }
    }
  }
}
"#;

const PERMISSIONS: &str = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
    "tables": {
        "boards": { "read": "true" },
        "projects": {
            "read": "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)",
            "insert": "$createdBy.account = session.user",
            "update": "ownerId = session.user"
        },
        "tasks": { "read": "allowedTo.read(project)", "insert": "allowedTo.update(project)" }
    } }"#;

fn deployment_body(dir: &std::path::Path) -> Value {
    let app = std::fs::read(dir.join(artifact::APP_FILE)).unwrap();
    artifact::load(&app, PERMISSIONS.as_bytes())
        .unwrap_or_else(|error| panic!("the app file loads: {error}"))
        .deployment_body(false)
}

/// Generates `lang`'s bindings of [`APP_V1`] and deploys it, adds an
/// optional int column to `table` with no other edit, runs
/// `migrations create`, and deploys the file it wrote.
async fn add_a_column_and_deploy(lang: Lang, table: &str) {
    let dir = tempfile::tempdir().unwrap();
    let app_path = dir.path().join(artifact::APP_FILE);
    std::fs::write(&app_path, APP_V1).unwrap();
    std::fs::write(dir.path().join("jazz.permissions.json"), PERMISSIONS).unwrap();
    let langs = [Selected::from(lang)];
    project::generate(dir.path(), &langs, false).unwrap();
    let router = router().await;
    let (status, response) = publish(&router, deployment_body(dir.path())).await;
    assert_eq!(status, StatusCode::OK, "v1: {response}");

    let mut app: Value = serde_json::from_str(APP_V1).unwrap();
    app["tables"][table]["columns"]["priority"] = Value::from("int?");
    std::fs::write(&app_path, serde_json::to_string_pretty(&app).unwrap()).unwrap();
    let created = project::migrations_create(dir.path(), &langs, "add-priority")
        .unwrap_or_else(|error| panic!("{lang:?}, {table}: migrations create: {error}"));
    assert!(created.changed, "{lang:?}, {table}: {:?}", created.notes);
    assert!(
        created.todos.is_empty(),
        "{lang:?}, {table}: {:?}",
        created.todos
    );

    let written: Value =
        serde_json::from_str(&std::fs::read_to_string(&app_path).unwrap()).unwrap();
    let history = written["history"].as_object().unwrap();
    assert_eq!(history.len(), 1);
    let earlier = &history.values().next().unwrap()["tables"];
    for (encrypted, column) in [("projects", "name"), ("tasks", "title"), ("tasks", "notes")] {
        assert!(
            earlier[encrypted]["encryption"]["columns"][column].is_string(),
            "{lang:?}, {table}: the earlier version keeps {encrypted}.{column}'s declaration: \
             {earlier}"
        );
    }
    assert_eq!(earlier["tasks"]["encryption"]["equality"][0], "title");

    let (status, response) = publish(&router, deployment_body(dir.path())).await;
    assert_eq!(status, StatusCode::OK, "{lang:?}, {table}: v2: {response}");
}

/// A change to a plain table leaves every encrypted declaration as it was.
#[tokio::test]
async fn migrations_create_keeps_declarations_for_a_change_elsewhere() {
    for lang in [Lang::TypeScript, Lang::Rust] {
        add_a_column_and_deploy(lang, "boards").await;
    }
}

/// A plain column added to an encrypted table, and to a table that is its
/// own space.
#[tokio::test]
async fn migrations_create_keeps_declarations_for_a_change_to_an_encrypted_table() {
    for lang in [Lang::TypeScript, Lang::Rust] {
        add_a_column_and_deploy(lang, "tasks").await;
        add_a_column_and_deploy(lang, "projects").await;
    }
}
