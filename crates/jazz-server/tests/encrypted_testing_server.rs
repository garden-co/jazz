//! The testing server boots an app that declares encryption the way a
//! backend deploys it: the schema's permissions and the app's encryption
//! declarations go out together, so the server derives the system tables'
//! policies and keeps the declarations for the version.
//!
//! ```text
//! jazz.app.json + jazz.permissions.json ──load──► alice's app
//!        │                                            │
//!        │ with_schema + with_encryption              │ deployment_body
//!        ▼                                            ▼
//!   testing server boots ──── bundle ════ same bundle ── backend deploys
//!                                         (nothing changes)
//! ```

use jazz::tools::deployment::artifact::{self, LoadedApp};
use jazz::tools::public_schema::Schema;
use jazz_server::JazzServer;
use serde_json::Value;

/// Projects are spaces; a task's title is encrypted in its project's space.
const APP: &str = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
    "tables": {
        "projects": { "columns": { "name": "string", "ownerId": "uuid" } },
        "tasks": {
            "columns": { "projectId": "uuid", "title": "bytes", "__e2ee_eq_title": "bytes" },
            "relations": { "project": { "rel": "projects", "column": "projectId" } },
            "encryption": { "space": "projectId", "scope": "projects",
                "columns": { "title": "string" }, "equality": ["title"] }
        }
    } }"#;

const PERMISSIONS: &str = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
    "tables": {
        "projects": {
            "read": "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)",
            "insert": "$createdBy.account = session.user",
            "update": "ownerId = session.user"
        },
        "tasks": { "read": "allowedTo.read(project)", "insert": "allowedTo.update(project)" }
    } }"#;

fn alices_app() -> LoadedApp {
    artifact::load(APP.as_bytes(), PERMISSIONS.as_bytes()).unwrap_or_else(|e| panic!("{e}"))
}

/// The app's current schema carrying its own permissions as table
/// policies: what a test hands the testing server.
fn source(app: &LoadedApp) -> Schema {
    let mut source = app.current_schema().clone();
    for (table, policies) in app.permissions() {
        source
            .get_mut(table)
            .expect("permissions name the app's tables")
            .policies = policies.clone();
    }
    source
}

/// alice's encrypted app boots on the testing server with its declarations,
/// and the deployment her backend then publishes from the same files finds
/// it already live: the same bundle, with nothing changed and nothing
/// refused for naming other declarations than the version keeps.
#[tokio::test]
async fn the_testing_server_boots_an_encrypted_app() {
    let app = alices_app();
    let server = JazzServer::builder()
        .with_schema(source(&app))
        .with_encryption(app.encryption().clone())
        .start()
        .await
        .expect("an encrypted app boots on the testing server");

    let response = reqwest::Client::new()
        .post(format!(
            "{}/apps/{}/admin/bundles",
            server.base_url(),
            server.app_id()
        ))
        .header("X-Jazz-Admin-Secret", server.admin_secret())
        .json(&app.deployment_body(false))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body: Value = response.json().await.unwrap();
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["bundle"], app.bundle().to_hex(), "{body}");
    assert_eq!(body["changed"], false, "{body}");
}
