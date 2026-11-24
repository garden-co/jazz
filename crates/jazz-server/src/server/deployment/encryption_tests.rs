//! Deploying an app that declares encryption through the real HTTP router:
//! the body a backend publishes from its loaded files, the server's own
//! derivation of the system tables' policies, and its refusals.
//!
//! ```text
//! jazz.app.json + jazz.permissions.json ──load──► deployment body
//!                                                     │ POST /bundles
//!                                                     ▼
//!                       server: system tables checked, read rules checked,
//!                               policies derived ──► the backend's bundle id
//! ```
use super::super::{ServerBuilder, StorageBackend, routes::create_router};
use crate::middleware::AuthConfig;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use jazz::tools::AppId;
use jazz::tools::deployment::artifact::{self, LoadedApp};
use jazz::tools::public_schema::{ColumnType, SchemaBuilder, SchemaHash, TableSchema};
use serde_json::{Value, json};
use tower::ServiceExt;

fn app_id() -> AppId {
    AppId::from_name("encrypted-deployment-tests")
}

async fn router() -> Router {
    router_and_state().await.0
}

async fn router_and_state() -> (Router, std::sync::Arc<super::super::ServerState>) {
    let server = ServerBuilder::new(app_id())
        .with_auth_config(AuthConfig {
            admin_secret: Some("test-admin".into()),
            ..Default::default()
        })
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .unwrap();
    (create_router(server.state.clone()), server.state)
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
    let value =
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)));
    (status, value)
}

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

fn permissions(project_read: &str, project_update: &str) -> String {
    json!({
        "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": {
            "projects": {
                "read": project_read,
                "insert": "$createdBy.account = session.user",
                "update": project_update,
            },
            "tasks": { "read": "allowedTo.read(project)", "insert": "allowedTo.update(project)" },
        },
    })
    .to_string()
}

const MEMBERS_RULE: &str = "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)";

fn loaded(project_update: &str) -> LoadedApp {
    artifact::load(
        APP.as_bytes(),
        permissions(MEMBERS_RULE, project_update).as_bytes(),
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

/// A backend's deployment of an encrypted app publishes the bundle its
/// own loader computed: the server derives the same system policies.
#[tokio::test]
async fn an_app_with_encryption_declarations_deploys() {
    let router = router().await;
    let app = loaded("ownerId = session.user");
    let (status, body) = publish(&router, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["bundle"], app.bundle().to_hex());
    assert!(body.get("warnings").is_none(), "{body}");

    // The same deployment again changes nothing.
    let (status, body) = publish(&router, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["changed"], false);
}

/// The server warns of a broad space-table update rule it can see.
#[tokio::test]
async fn the_deploy_answers_with_its_warnings() {
    let router = router().await;
    let app = loaded("true");
    let (status, body) = publish(&router, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let warnings = body["warnings"].to_string();
    assert!(
        warnings.contains("space table projects: its update rule is broad"),
        "{body}"
    );
}

/// The server refuses a space table whose read rule doesn't come from the
/// members table, whatever the backend's loader did, and a body whose
/// system tables don't match its declarations.
#[tokio::test]
async fn a_wrong_space_read_rule_is_refused() {
    let router = router().await;
    let app = loaded("ownerId = session.user");
    let owner_only = artifact::load(
        APP.as_bytes(),
        permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
    )
    .unwrap()
    .permissions()
    .get(&jazz::tools::public_schema::TableName::from("projects"))
    .unwrap()
    .update
    .clone();

    let mut body = app.deployment_body(false);
    body["permissions"]["projects"]["select"] = serde_json::to_value(&owner_only).unwrap();
    let (status, refusal) = publish(&router, body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("space table projects: its read rule must come from the members table"),
        "{refusal}"
    );

    let mut undeclared = app.deployment_body(false);
    undeclared.as_object_mut().unwrap().remove("encryption");
    let (status, refusal) = publish(&router, undeclared).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("has Jazz's system tables but declares no encryption"),
        "{refusal}"
    );

    let mut own_policies = app.deployment_body(false);
    own_policies["permissions"]["__e2ee_members"] = json!({});
    let (status, refusal) = publish(&router, own_policies).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
}

/// An encrypted column keeps its type and nullability on redeploy.
#[tokio::test]
async fn a_redeployment_cannot_change_an_encrypted_columns_type_or_nullability() {
    let router = router().await;
    let app = loaded("ownerId = session.user");
    let (status, body) = publish(&router, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let retyped = APP.replace(r#""title": "string""#, r#""title": "int""#);
    let edited = artifact::load(
        retyped.as_bytes(),
        permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
    )
    .unwrap();
    assert_eq!(edited.current_hash(), app.current_hash(), "the same schema");
    let (status, refusal) = publish(&router, edited.deployment_body(false)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("deployed with other encryption declarations"),
        "{refusal}"
    );

    let widened = APP.replace(
        r#""title": "string""#,
        r#""title": { "type": "string?", "sealedType": "string" }"#,
    );
    let widened = artifact::load(
        widened.as_bytes(),
        permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
    )
    .unwrap();
    let (status, refusal) = publish(&router, widened.deployment_body(false)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"].as_str().unwrap().contains("tasks.title"),
        "{refusal}"
    );
}

/// On her first deploy Alice can pin the declared type, including its nullability.
#[tokio::test]
async fn a_first_declaration_can_pin_only_the_columns_declared_type() {
    let router = router().await;
    let app = loaded("ownerId = session.user");
    let mut body = app.deployment_body(false);
    let hash = app.current_hash().to_string();
    let title = &mut body["encryption"]["versions"][&hash]["tasks"]["columns"]["title"];
    title["sealedType"] = title["type"].clone();
    title["type"]["nullable"] = json!(true);
    let (status, refusal) = publish(&router, body.clone()).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    let reason = refusal["error"].as_str().unwrap();
    assert!(
        reason.contains("tasks.title") && reason.contains("sealedType only pins an earlier type"),
        "{refusal}"
    );

    body["encryption"]["versions"][&hash]["tasks"]["columns"]["title"]["sealedType"]["nullable"] =
        json!(true);
    let (status, response) = publish(&router, body).await;
    assert_eq!(status, StatusCode::OK, "{response}");
}

#[tokio::test]
async fn a_redeployment_cannot_append_an_encrypted_enum_variant_to_the_same_schema_hash() {
    let router = router().await;
    let app = loaded("ownerId = session.user");
    let hash = app.current_hash().to_string();
    let mut body = app.deployment_body(false);
    let original =
        json!({ "columnType": { "type": "Enum", "variants": ["a"] }, "nullable": false });
    body["encryption"]["versions"][&hash]["tasks"]["columns"]["title"] =
        json!({ "type": original });
    let (status, response) = publish(&router, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let (status, response) = publish(&router, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{response}");

    let title = &mut body["encryption"]["versions"][&hash]["tasks"]["columns"]["title"];
    title["sealedType"] = original;
    title["type"]["columnType"]["variants"] = json!(["a", "b"]);
    let (status, refusal) = publish(&router, body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"].as_str().unwrap().contains("immutable"),
        "{refusal}"
    );
}

/// Both project references already belong to the stored tasks table. A
/// new physical schema can reorder them without changing their identities.
fn standalone_tasks() -> (LoadedApp, LoadedApp) {
    let permissions = permissions(MEMBERS_RULE, "ownerId = session.user");
    let source = APP.replace(
        r#""projectId": "uuid", "title": "bytes""#,
        r#""projectId": "uuid", "otherProject": { "type": "uuid", "references": "projects" }, "caption": "bytes", "title": "bytes", "plainTitle": "string?""#,
    );
    let reordered = source.replace(
        r#""title": "bytes", "plainTitle": "string?""#,
        r#""plainTitle": "string?", "title": "bytes""#,
    );
    let load = |s: &str| artifact::load(s.as_bytes(), permissions.as_bytes()).unwrap();
    let versions = (load(&source), load(&reordered));
    assert_ne!(versions.0.current_hash(), versions.1.current_hash());
    versions
}

async fn refuses_standalone_change(edit: impl FnOnce(&mut Value), reason: &str) {
    let (first, next) = standalone_tasks();
    let router = router().await;
    let (status, response) = publish(&router, first.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let mut body = next.deployment_body(false);
    let hash = next.current_hash().to_string();
    edit(&mut body["encryption"]["versions"][&hash]["tasks"]);
    // Unknown deploy options cannot override the transition checks.
    body["confirmDecrypt"] = json!(["tasks.title"]);
    let (status, refusal) = publish(&router, body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    let message = refusal["error"].as_str().unwrap();
    assert!(
        message.contains("tasks") && message.contains(reason),
        "{refusal}"
    );
}

/// Alice cannot redirect existing ciphertext to another project by deploying a standalone version.
#[tokio::test]
async fn standalone_encryption_preserves_the_stored_space() {
    refuses_standalone_change(
        |e| e["space"] = json!("otherProject"),
        "changes its space column",
    )
    .await;
}

/// Alice must keep a stored ciphertext column declared until she drops it.
#[tokio::test]
async fn standalone_encryption_keeps_ciphertext_columns_declared() {
    refuses_standalone_change(
        |e| {
            e["columns"] = json!({});
            e["equality"] = json!([]);
        },
        "holds ciphertext",
    )
    .await;
}

/// When Alice keeps the stored equality token column, its writers must still populate it.
#[tokio::test]
async fn standalone_encryption_keeps_equality_while_the_token_column_remains() {
    refuses_standalone_change(|e| e["equality"] = json!([]), "token column").await;
}

/// Alice can locate an unknown declaration field and see which fields are accepted.
#[tokio::test]
async fn an_unknown_deployment_field_names_its_location_and_allowed_fields() {
    for (key, value, column) in [
        ("from", Value::Null, true),
        ("typo", Value::Null, true),
        ("backfill", json!([]), false),
        ("typo", json!([]), false),
    ] {
        let source = APP.replace(
            r#""title": "bytes""#,
            r#""oldTitle": "string", "title": "bytes""#,
        );
        let app = artifact::load(
            source.as_bytes(),
            permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
        )
        .unwrap();
        let hash = app.current_hash().to_string();
        let mut body = app.deployment_body(false);
        let entry = &mut body["encryption"]["versions"][&hash]["tasks"];
        if column {
            entry["columns"]["title"][key] = value;
        } else {
            entry[key] = value;
        }
        let (status, refusal) = publish(&router().await, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{key}: {refusal}");
        let error = refusal["error"].as_str().unwrap();
        assert!(
            error.contains("tasks")
                && (!column || error.contains("title"))
                && error.contains(key)
                && error.contains("expected")
                && error.contains(if column { "sealedType" } else { "equality" })
                && !error.contains("docs/e2ee/encrypting-existing-data"),
            "{key}: {refusal}"
        );
    }
}

/// Alice renames an encrypted column but replaces its token with a fresh bytes column.
/// The new token has the expected public name, but contains none of the stored tokens.
#[tokio::test]
async fn a_renamed_encrypted_column_cannot_replace_its_token() {
    let (current, history) = APP_V2.split_once(r#""history":"#).unwrap();
    let current = current.replace("title", "caption");
    let history = history.replace(
        r#""done": { "add""#,
        r#""caption": { "renameFrom": "title" },
           "__e2ee_eq_title": { "drop": "bytes", "backwardsDefault": "" },
           "__e2ee_eq_caption": { "add": "bytes", "default": "" }, "done": { "add""#,
    );
    // Build the physical migration first so the server checks the declarations itself.
    let file = format!(r#"{current}"history":{history}"#)
        .replace(r#", "equality": ["title"]"#, "")
        .replace(r#", "equality": ["caption"]"#, "");
    let app = artifact::load(
        file.as_bytes(),
        permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
    )
    .unwrap();
    let mut body = app.deployment_body(false);
    let from = body["migrations"][0]["fromHash"]
        .as_str()
        .unwrap()
        .to_owned();
    body["encryption"]["versions"][&from]["tasks"]["equality"] = json!(["title"]);
    body["encryption"]["versions"][app.current_hash().to_string()]["tasks"]["equality"] =
        json!(["caption"]);
    let (status, response) = publish(&router().await, body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{response}");
    let error = response["error"].as_str().unwrap();
    assert!(
        error.contains("tasks.caption") && error.contains("__e2ee_eq_title to __e2ee_eq_caption"),
        "{response}"
    );
}

/// Alice renames the projects table. Both names refer to the same stored scope for tasks.
#[tokio::test]
async fn a_renamed_scope_table_keeps_its_stored_identity() {
    let (router, state) = router_and_state().await;
    let original = loaded("ownerId = session.user");
    let (status, response) = publish(&router, original.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{response}");

    // Another deployed version shares the stored scope without a direct migration to rooms.
    let sibling = APP.replace(
        r#""name": "string", "ownerId": "uuid""#,
        r#""ownerId": "uuid", "name": "string""#,
    );
    let policy = permissions(MEMBERS_RULE, "ownerId = session.user");
    let sibling = artifact::load(sibling.as_bytes(), policy.as_bytes()).unwrap();
    assert_ne!(sibling.current_hash(), original.current_hash());
    let (status, response) = publish(&router, sibling.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{response}");

    let (current, history) = APP_V2.split_once(r#""history":"#).unwrap();
    let current = current.replace("projects", "rooms");
    let history = history.replace(
        r#""from": "v1", "migrate""#,
        r#""from": "v1", "renameTables": { "rooms": "projects" }, "migrate""#,
    );
    let file = format!(r#"{current}"history":{history}"#);
    let policy = policy.replace("projects", "rooms");
    let renamed = artifact::load(file.as_bytes(), policy.as_bytes()).unwrap();
    let (status, response) = publish(&router, renamed.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{response}");

    // Read the admitted catalogue to check the identities the cell context uses.
    let (from, to) = (original.current_hash(), renamed.current_hash());
    let (before, after) = state
        .runtime()
        .unwrap()
        .with_db_for_test(move |db| {
            let names = |hash| {
                jazz::db::block_on(db.catalogue_stored_names(&hash)).map_err(|err| err.to_string())
            };
            Ok((names(from)?, names(to)?))
        })
        .await
        .unwrap();
    assert_eq!(before.tables["projects"].0, after.tables["rooms"].0);
    assert_eq!(
        before.tables["tasks"].1["projectId"],
        after.tables["tasks"].1["projectId"]
    );
}

/// Alice renames the project reference. The same stored column still selects the space.
#[tokio::test]
async fn a_renamed_space_column_keeps_its_stored_identity() {
    let router = router().await;
    let first = loaded("ownerId = session.user");
    let (status, response) = publish(&router, first.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let (current, history) = APP_V2.split_once(r#""history":"#).unwrap();
    let current = current.replace("projectId", "roomId");
    let history = history.replace(
        r#""done": { "add""#,
        r#""roomId": { "renameFrom": "projectId" }, "done": { "add""#,
    );
    let source = format!(r#"{current}"history":{history}"#);
    let renamed = artifact::load(
        source.as_bytes(),
        permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
    )
    .unwrap();
    let (status, response) = publish(&router, renamed.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{response}");
}

/// Alice cannot declare a re-home from the old title into a new encrypted caption on either path.
#[tokio::test]
async fn an_explicit_rehome_is_refused_with_or_without_a_migration() {
    let (first, next) = standalone_tasks();
    let from = first.current_hash().to_string();
    let to = next.current_hash().to_string();
    let first = first.deployment_body(false);
    let mut standalone = next.deployment_body(false);
    let entry = &mut standalone["encryption"]["versions"][&to]["tasks"];
    entry["space"] = json!("otherProject");
    entry["rehome"] = json!({ "space": "projectId", "scope": "projects" });
    entry["columns"] = json!({ "caption": {
        "type": entry["columns"]["title"]["type"], "from": "title"
    } });
    entry["equality"] = json!([]);
    let mut migrated = standalone.clone();
    migrated["migrations"] = json!([{ "fromHash": from, "toHash": to, "forward": [] }]);
    migrated["encryption"]["versions"][&from] = first["encryption"]["versions"][&from].clone();
    for candidate in [migrated, standalone] {
        let router = router().await;
        let (status, response) = publish(&router, first.clone()).await;
        assert_eq!(status, StatusCode::OK, "{response}");
        let (status, response) = publish(&router, candidate).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    }
}

/// A later version can reuse the public name of a dropped token as an ordinary bytes column.
#[tokio::test]
async fn a_token_dropped_with_equality_can_be_replaced_by_an_unindexed_column() {
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "title": "bytes", "__e2ee_eq_title": "bytes", "done": "boolean" },
            "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } },
        "history": {
            "v1": { "tables": { "notes": {
                "columns": { "title": "bytes", "__e2ee_eq_title": "bytes" },
                "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" }, "equality": ["title"] } } } },
            "v2": { "tables": { "notes": {
                "columns": { "title": "bytes" },
                "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } } } },
        "migrations": [
            { "from": "v1", "to": "v2", "migrate": { "notes": { "__e2ee_eq_title": { "drop": "bytes", "backwardsDefault": "" } } } },
            { "from": "v2", "migrate": { "notes": { "__e2ee_eq_title": { "add": "bytes", "default": "" }, "done": { "add": "boolean", "default": false } } } }] }"#;
    let permissions = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let app = artifact::load(file.as_bytes(), permissions.as_bytes()).unwrap();
    let (status, response) = publish(&router().await, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{response}");
}

/// Alice drops equality, then tries to keep the old token storage under its original or a new name.
#[tokio::test]
async fn removing_equality_drops_its_stored_token_in_files_and_raw_deployments() {
    let permissions = permissions(MEMBERS_RULE, "ownerId = session.user");
    let load = |file: &str| artifact::load(file.as_bytes(), permissions.as_bytes());
    let original = load(APP).unwrap();
    let from = original.current_hash().to_string();
    let (current, history) = APP_V2.split_once(r#""history":"#).unwrap();
    let equality = r#", "equality": ["title"]"#;
    let mut results = Vec::new();
    for (name, tag, operations) in [
        (
            "caption",
            "__e2ee_eq_title",
            r#""caption":{"renameFrom":"title"},"#,
        ),
        (
            "caption",
            "formerTag",
            r#""caption":{"renameFrom":"title"},"formerTag":{"renameFrom":"__e2ee_eq_title"},"#,
        ),
        (
            "",
            "__e2ee_eq_title",
            r#""title":{"drop":"bytes","backwardsDefault":""},"#,
        ),
    ] {
        let next = current
            .replace(equality, "")
            .replace("__e2ee_eq_title", tag);
        let next = if name.is_empty() {
            next.replace(r#""title": "bytes", "#, "")
                .replace(r#""title": "string""#, "")
        } else {
            next.replace(r#""title""#, &format!(r#""{name}""#))
        };
        let history = history.replace(
            r#""done": { "add""#,
            &format!(r#"{operations}"done": {{ "add""#),
        );
        let file = format!(r#"{next}"history":{history}"#);
        let file_error = load(&file)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        // Build the physical migration independently of the source declaration, then post the real declaration.
        let mut raw = load(&file.replace(equality, ""))
            .unwrap()
            .deployment_body(false);
        raw["encryption"]["versions"][&from] =
            original.deployment_body(false)["encryption"]["versions"][&from].clone();
        let (status, response) = publish(&router().await, raw).await;
        results.push((tag, file_error, status, response));
    }
    for (tag, file_error, status, response) in &results {
        assert!(
            file_error.contains("token column") && file_error.contains(tag),
            "{results:?}"
        );
        assert_eq!(*status, StatusCode::UNPROCESSABLE_ENTITY, "{results:?}");
        assert!(
            response["error"].as_str().unwrap().contains(tag),
            "{response}"
        );
    }
}

/// Alice's logical default is read only when the migration supplies an empty stored cell for old rows.
#[tokio::test]
async fn adding_an_encrypted_column_requires_an_empty_lens_default() {
    let permissions = permissions(MEMBERS_RULE, "ownerId = session.user");
    let load = |file: &str| artifact::load(file.as_bytes(), permissions.as_bytes());
    let (current, history) = APP_V2.split_once(r#""history":"#).unwrap();
    let current = current
        .replace(r#""done": "boolean""#, r#""body": "bytes""#)
        .replace(
            r#""title": "string""#,
            r#""title":"string","body":{"type":"string","default":"none"}"#,
        );
    let op = r#""body": { "add": "bytes", "default": "" }"#;
    let history = history.replace(r#""done": { "add": "boolean", "default": false }"#, op);
    let file = format!(r#"{current}"history":{history}"#);
    let mut raw = load(&file).unwrap().deployment_body(false);
    let (status, response) = publish(&router().await, raw.clone()).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let invalid = file.replace(op, r#""body":{"add":"bytes","default":"AQ=="}"#);
    let file_error = load(&invalid)
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    raw["migrations"][0]["forward"][0]["operations"][0]["value"] =
        json!({"type":"Bytea","value":[1]});
    let (status, response) = publish(&router().await, raw).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "file error={file_error:?}, raw={response}"
    );
    for error in [&file_error, response["error"].as_str().unwrap()] {
        assert!(
            error.contains("tasks.body") && error.contains("empty"),
            "{error}"
        );
    }
}

/// Alice drops the last encrypted column and its token. Only empty bytes may be supplied to older clients.
#[tokio::test]
async fn dropping_encrypted_storage_requires_empty_backwards_defaults() {
    let file = json!({
        "$schema":"https://jazz.tools/schemas/jazz.app.v1.json",
        "tables":{"notes":{"columns":{"label":"string"}}},
        "history":{"v1":{"tables":{"notes":{
            "columns":{"label":"string","title":"bytes","__e2ee_eq_title":"bytes","digest":"bytes"},
            "encryption":{"space":"id","scope":"notes","columns":{"title":"string"},"equality":["title"]}
        }}}},
        "migrations":[{"from":"v1","migrate":{"notes":{
            "title":{"drop":"bytes","backwardsDefault":""},
            "__e2ee_eq_title":{"drop":"bytes","backwardsDefault":""},
            "digest":{"drop":"bytes","backwardsDefault":"AQ=="}
        }}}]
    });
    let permissions =
        br#"{"$schema":"https://jazz.tools/schemas/jazz.permissions.v1.json","tables":{"notes":{"read":"exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)"}}}"#;
    let load = |file: &Value| artifact::load(file.to_string().as_bytes(), permissions);
    let body = load(&file).unwrap().deployment_body(false);
    let (status, response) = publish(&router().await, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{response}");

    for column in ["title", "__e2ee_eq_title"] {
        let mut invalid = file.clone();
        invalid["migrations"][0]["migrate"]["notes"][column]["backwardsDefault"] = json!("AQ==");
        let file_error = load(&invalid).err().map(|e| e.to_string());
        let mut raw = body.clone();
        let operations = raw["migrations"][0]["forward"][0]["operations"]
            .as_array_mut()
            .unwrap();
        let drop = operations
            .iter_mut()
            .find(|op| op["column"] == column)
            .unwrap();
        drop["value"] = json!({"type":"Bytea","value":[1]});
        let (status, response) = publish(&router().await, raw).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{column}: file error={file_error:?}, raw={response}"
        );
        for error in [
            file_error.as_deref().unwrap(),
            response["error"].as_str().unwrap(),
        ] {
            assert!(
                error.contains(&format!("notes.{column}"))
                    && error.contains("backwardsDefault")
                    && error.contains("empty byte string"),
                "{error}"
            );
        }
    }
}

/// When a migration introduces token storage on an existing table, earlier rows cannot receive a token from its lens.
#[tokio::test]
async fn an_introduced_token_cannot_have_nonempty_stored_bytes() {
    let permissions = permissions(MEMBERS_RULE, "ownerId = session.user");
    let load = |file: &str| artifact::load(file.as_bytes(), permissions.as_bytes());
    let (current, history) = APP_V2.split_once(r#""history":"#).unwrap();
    let current = current
        .replace(
            r#""done": "boolean""#,
            r#""body": "bytes", "__e2ee_eq_body": "bytes""#,
        )
        .replace(
            r#""title": "string""#,
            r#""title":"string","body":"string?""#,
        );
    let history = history.replace(
        r#""done": { "add": "boolean", "default": false }"#,
        r#""body":{"add":"bytes","default":""},"__e2ee_eq_body":{"add":"bytes","default":"AQ=="}"#,
    );
    let file = format!(r#"{current}"history":{history}"#);
    let base = load(&file).unwrap();
    let mut raw = base.deployment_body(false);
    let hash = base.current_hash().to_string();
    raw["encryption"]["versions"][&hash]["tasks"]["equality"] = json!(["title", "body"]);
    let file = file.replacen(
        r#""equality": ["title"]"#,
        r#""equality":["title","body"]"#,
        1,
    );
    let error = load(&file).unwrap_err().to_string();
    let (status, response) = publish(&router().await, raw).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{response}");
    for reason in [&error, response["error"].as_str().unwrap()] {
        assert!(
            reason.contains("tasks.__e2ee_eq_body") && reason.contains("empty byte string"),
            "{reason}"
        );
    }
}

/// Alice's encrypted array declarations check each element type. A plain schema keeps its usual reader.
#[tokio::test]
async fn only_encrypted_declarations_refuse_unknown_scalar_type_fields() {
    let mut plain = plain_body();
    let column = &mut plain["schemas"][0]["schema"]["tables"]["tasks"]["columns"][0]["column_type"];
    assert_eq!(column["type"], "Uuid");
    column["from"] = json!("oldTitle");
    let (status, response) = publish(&router().await, plain).await;
    assert_eq!(status, StatusCode::OK, "{response}");

    let app = loaded("ownerId = session.user");
    let hash = app.current_hash().to_string();
    let mut results = Vec::new();
    for (field, key) in [
        ("type", "from"),
        ("type", "typo"),
        ("sealedType", "reverse"),
    ] {
        let mut raw = app.deployment_body(false);
        let column = &mut raw["encryption"]["versions"][&hash]["tasks"]["columns"]["title"];
        let array = json!({"type":"Array","element":{"type":"Array","element":{"type":"Text"}}});
        column["type"]["columnType"] = array.clone();
        column[field] = json!({"columnType":array,"nullable":false});
        column[field]["columnType"]["element"]["element"][key] = json!("oldTitle");
        let (status, response) = publish(&router().await, raw).await;
        results.push((key, status, response));
    }
    for (key, status, response) in &results {
        assert_eq!(*status, StatusCode::BAD_REQUEST, "{results:?}");
        let error = response["error"].as_str().unwrap();
        assert!(
            error.contains(key)
                && error.contains("tasks")
                && error.contains("title")
                && error.contains("element")
                && error.contains("expected `type`")
                && !error.contains("docs/e2ee/encrypting-existing-data"),
            "{response}"
        );
    }
}

/// Alice gets the same refusal for an unreadable encrypted type in an app file and at the raw router.
#[tokio::test]
async fn unreadable_encrypted_types_are_refused_before_publication() {
    let permissions = permissions(MEMBERS_RULE, "ownerId = session.user");
    let source = APP.replace(r#", "equality": ["title"]"#, "");
    let load = |file: &str| artifact::load(file.as_bytes(), permissions.as_bytes());
    let base = load(&source).unwrap();
    let hash = base.current_hash().to_string();
    let payload = json!({"enum":{"pending":{"reason":"string"}}});
    let raw_payload = json!({"type":"EnumPayload","cases":[{"name":"pending","fields":[
        {"name":"reason","column_type":{"type":"Text"},"nullable":false}
    ]}]});
    let mut results = Vec::new();
    for (file_type, raw_type, reason) in [
        (payload.clone(), raw_payload.clone(), "payload enum"),
        (
            json!({"array":payload}),
            json!({"type":"Array","element":raw_payload}),
            "payload enum",
        ),
        (
            json!({"json":{"minimum":0.5}}),
            json!({"type":"Json","schema":{"minimum":0.5}}),
            "non-integer",
        ),
    ] {
        let file = source.replace(r#""title": "string""#, &format!(r#""title":{file_type}"#));
        let file_error = load(&file).err().map(|e| e.to_string());
        let mut raw = base.deployment_body(false);
        raw["encryption"]["versions"][&hash]["tasks"]["columns"]["title"]["type"]["columnType"] =
            raw_type;
        let (status, response) = publish(&router().await, raw).await;
        results.push((reason, file_error, status, response));
    }
    for (reason, file_error, status, response) in &results {
        assert_eq!(*status, StatusCode::UNPROCESSABLE_ENTITY, "{results:?}");
        for error in [
            file_error.as_deref().unwrap(),
            response["error"].as_str().unwrap(),
        ] {
            assert!(
                error.contains("tasks") && error.contains("title") && error.contains(reason),
                "{error}"
            );
        }
    }
}

/// Alice can deploy a migration that drops an unreadable historical declaration.
#[tokio::test]
async fn a_historical_encrypted_payload_enum_can_be_dropped() {
    let file = r#"{"$schema":"https://jazz.tools/schemas/jazz.app.v1.json",
        "tables":{"notes":{"columns":{"summary":"bytes"},"encryption":{"space":"id","scope":"notes","columns":{"summary":"string?"}}}},
        "history":{"old":{"tables":{"notes":{"columns":{"detail":"bytes"},"encryption":{"space":"id","scope":"notes","columns":{"detail":{"enum":{"pending":{"reason":"string"}}}}}}}}},
        "migrations":[{"from":"old","migrate":{"notes":{"detail":{"drop":"bytes","backwardsDefault":""},"summary":{"add":"bytes","default":""}}}}]}"#;
    let policy = r#"{"$schema":"https://jazz.tools/schemas/jazz.permissions.v1.json","tables":{"notes":{"read":"exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)"}}}"#;
    let app = artifact::load(file.as_bytes(), policy.as_bytes()).unwrap();
    let (status, response) = publish(&router().await, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{response}");
}

/// Alice can declare null only on an optional encrypted column.
#[tokio::test]
async fn defaults_in_files_and_raw_deployments_must_match_the_encrypted_type() {
    let permissions = permissions(MEMBERS_RULE, "ownerId = session.user");
    let load = |file: &str| artifact::load(file.as_bytes(), permissions.as_bytes());
    let app = load(APP).unwrap();
    let hash = app.current_hash().to_string();
    for (ty, value, nullable, expected) in [
        (
            "string",
            json!({"type":"Null"}),
            false,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        ("string?", json!({"type":"Null"}), true, StatusCode::OK),
        (
            "string",
            json!({"type":"Integer","value":7}),
            false,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let default = if value["type"] == "Integer" {
            "7"
        } else {
            "null"
        };
        let file = APP.replace(
            r#""title": "string""#,
            &format!(r#""title": {{"type":"{ty}","default":{default}}}"#),
        );
        let file_result = load(&file);
        let mut raw = app.deployment_body(false);
        let plain = &mut raw["encryption"]["versions"][&hash]["tasks"]["columns"]["title"]["type"];
        plain["nullable"] = json!(nullable);
        plain["default"] = value;
        let (status, response) = publish(&router().await, raw).await;
        assert_eq!(
            status,
            expected,
            "file accepted={}, raw={response}",
            file_result.is_ok()
        );
        assert_eq!(file_result.is_ok(), expected == StatusCode::OK);
        if status != StatusCode::OK {
            let error = response["error"].as_str().unwrap();
            assert!(
                error.contains("title") && error.contains("default"),
                "{response}"
            );
        }
    }
    let mut raw = app.deployment_body(false);
    let column = &mut raw["encryption"]["versions"][&hash]["tasks"]["columns"]["title"];
    column["sealedType"] = column["type"].clone();
    column["sealedType"]["default"] =
        json!({"type":"Text","value":"a default belongs on the column"});
    let (status, response) = publish(&router().await, raw).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{response}");
    assert!(
        response["error"].as_str().unwrap().contains("sealedType"),
        "{response}"
    );
}

fn with_sibling(target: &LoadedApp, other: &LoadedApp) -> Value {
    let mut body = target.deployment_body(false);
    let other_hash = other.current_hash().to_string();
    let other = other.deployment_body(false);
    body["schemas"].as_array_mut().unwrap().push(
        other["schemas"]
            .as_array()
            .unwrap()
            .iter()
            .find(|schema| schema["hash"] == other_hash)
            .unwrap()
            .clone(),
    );
    body["encryption"]["versions"][&other_hash] =
        other["encryption"]["versions"][&other_hash].clone();
    body["migrations"]
        .as_array_mut()
        .unwrap()
        .push(other["migrations"][0].clone());
    body
}

/// Alice's sibling enums follow one append-only sequence, whichever version she publishes first.
#[tokio::test]
async fn sibling_versions_accept_comparable_enum_prefixes_in_either_submission_order() {
    let permissions = permissions(MEMBERS_RULE, "ownerId = session.user");
    let root_type = r#""title": {"enum":["a"]}"#;
    let load = |s: &str| artifact::load(s.as_bytes(), permissions.as_bytes()).unwrap();
    let root = load(&APP.replace(r#""title": "string""#, root_type));
    let sibling = |column: &str, variants: &[&str]| {
        let source = APP_V2.replace("done", column);
        let (current, history) = source.split_once(r#""history":"#).unwrap();
        let variants = serde_json::to_string(variants).unwrap();
        let new_type =
            format!(r#""title": {{"type":{{"enum":{variants}}},"sealedType":{{"enum":["a"]}}}}"#);
        let current = current.replace(r#""title": "string""#, &new_type);
        let history = history.replace(r#""title": "string""#, root_type);
        load(&format!(r#"{current}"history":{history}"#))
    };

    for (left_variants, right_variants, expected) in [
        (&["a", "b"][..], &["a", "b"][..], StatusCode::OK),
        (&["a"][..], &["a", "b"][..], StatusCode::OK),
        (
            &["a", "b"][..],
            &["a", "c"][..],
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let left = sibling("completed", left_variants);
        let right = sibling("archived", right_variants);
        for (target, other) in [(&left, &right), (&right, &left)] {
            for together in [true, false] {
                let router = router().await;
                let (status, response) = publish(&router, root.deployment_body(false)).await;
                assert_eq!(status, StatusCode::OK, "{response}");
                let body = if together {
                    with_sibling(target, other)
                } else {
                    let (status, response) = publish(&router, target.deployment_body(false)).await;
                    assert_eq!(status, StatusCode::OK, "{response}");
                    other.deployment_body(false)
                };
                let (status, response) = publish(&router, body).await;
                assert_eq!(status, expected, "{response}");
                if expected == StatusCode::UNPROCESSABLE_ENTITY {
                    assert!(
                        response["error"]
                            .as_str()
                            .unwrap()
                            .contains("enum variants"),
                        "{response}"
                    );
                }
            }
        }
    }
}

fn enum_migration_chain(first_variants: &[&str], next_variants: &[&str]) -> (Value, Value) {
    let mut second: Value = serde_json::from_str(APP_V2).unwrap();
    let root = json!({"enum":["a"]});
    let widened = |variants: &[&str]| json!({"type":{"enum":variants},"sealedType":root});
    let first = &mut second["history"]["v1"]["tables"]["tasks"];
    first["encryption"]["columns"]["title"] = root.clone();
    second["tables"]["tasks"]["encryption"]["columns"]["title"] = widened(first_variants);
    let mut third = second.clone();
    let mut previous = second["history"]["v1"].clone();
    previous["tables"]["tasks"]["columns"]["done"] = json!("boolean");
    previous["tables"]["tasks"]["encryption"]["columns"]["title"] = widened(first_variants);
    third["history"]["v2"] = previous;
    third["tables"]["tasks"]["columns"]["archived"] = json!("boolean");
    third["tables"]["tasks"]["encryption"]["columns"]["title"] = widened(next_variants);
    third["migrations"][0]["to"] = json!("v2");
    third["migrations"].as_array_mut().unwrap().push(
        json!({"from":"v2","migrate":{"tasks":{"archived":{"add":"boolean","default":false}}}}),
    );
    (second, third)
}

/// An old declaration can still be reached through two migrations that each append enum variants.
#[tokio::test]
async fn an_encrypted_enum_can_widen_again_after_an_intermediate_version() {
    let permissions = permissions(MEMBERS_RULE, "ownerId = session.user");
    let load =
        |file: &Value| artifact::load(file.to_string().as_bytes(), permissions.as_bytes()).unwrap();
    let (second, third) = enum_migration_chain(&["a", "b"], &["a", "b", "c"]);
    let latest = load(&third);
    for in_steps in [false, true] {
        let server = router().await;
        if in_steps {
            let (status, response) = publish(&server, load(&second).deployment_body(false)).await;
            assert_eq!(status, StatusCode::OK, "{response}");
        }
        let (status, response) = publish(&server, latest.deployment_body(false)).await;
        assert_eq!(status, StatusCode::OK, "{response}");
    }
}

/// Alice can append on one branch after deploying two siblings that both kept the root enum.
#[tokio::test]
async fn an_enum_can_grow_after_both_siblings_kept_the_root_variants() {
    let (left, wider) = enum_migration_chain(&["a"], &["a", "b"]);
    let right = left.to_string().replace("done", "reviewed");
    let permissions = permissions(MEMBERS_RULE, "ownerId = session.user");
    let server = router().await;
    for file in [left.to_string(), right, wider.to_string()] {
        let app = artifact::load(file.as_bytes(), permissions.as_bytes()).unwrap();
        let (status, response) = publish(&server, app.deployment_body(false)).await;
        assert_eq!(status, StatusCode::OK, "{response}");
    }
}

/// Alice may later branch from the root with either prefix of an enum already widened on another branch.
#[tokio::test]
async fn a_later_root_branch_can_use_either_prefix_of_a_stored_enum() {
    let (_, wider) = enum_migration_chain(&["a"], &["a", "b"]);
    let permissions = permissions(MEMBERS_RULE, "ownerId = session.user");
    for variants in [&["a", "b"][..], &["a"]] {
        let (branch, _) = enum_migration_chain(variants, variants);
        let server = router().await;
        for file in [
            wider.to_string(),
            branch.to_string().replace("done", "reviewed"),
        ] {
            let app = artifact::load(file.as_bytes(), permissions.as_bytes()).unwrap();
            let (status, response) = publish(&server, app.deployment_body(false)).await;
            assert_eq!(status, StatusCode::OK, "{variants:?}: {response}");
        }
    }
}

/// The version that drops a column cannot erase the declarations of versions that still store it.
#[tokio::test]
async fn a_branch_cannot_diverge_from_an_enum_after_another_branch_dropped_it() {
    let permissions = permissions(MEMBERS_RULE, "ownerId = session.user");
    let load = |file: &str| artifact::load(file.as_bytes(), permissions.as_bytes()).unwrap();
    let root_type = r#""title": {"enum":["a"]}"#;
    let wide_type = r#""title": {"type":{"enum":["a","b"]},"sealedType":{"enum":["a"]}}"#;
    let (tables, history) = APP_V2.split_once(r#""history":"#).unwrap();
    let history = history.replace(r#""title": "string""#, root_type);
    let wide = load(&format!(
        r#"{}"history":{history}"#,
        tables.replace(r#""title": "string""#, wide_type)
    ));
    let router = router().await;
    let (status, response) = publish(&router, wide.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{response}");

    let dropped = load(
        r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": {
            "projects":{"columns":{"name":"string","ownerId":"uuid"}},
            "tasks":{"columns":{"projectId":"uuid","done":"boolean"},
                "relations":{"project":{"rel":"projects","column":"projectId"}},
                "encryption":{"space":"projectId","scope":"projects","columns":{}}}
        },
        "history":{"wide":{"tables":{
            "projects":{"columns":{"name":"string","ownerId":"uuid"}},
            "tasks":{"columns":{"projectId":{"type":"uuid","references":"projects"},
                        "title":"bytes","__e2ee_eq_title":"bytes","done":"boolean"},
                "encryption":{"space":"projectId","scope":"projects","columns":{
                    "title":{"type":{"enum":["a","b"]},"sealedType":{"enum":["a"]}}
                },"equality":["title"]}}
        }}},
        "migrations":[{"from":"wide","migrate":{"tasks":{
            "title":{"drop":"bytes","backwardsDefault":""},
            "__e2ee_eq_title":{"drop":"bytes","backwardsDefault":""}
        }}}] }"#,
    );
    let (status, response) = publish(&router, dropped.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{response}");

    // This sibling still uses the original stored title. Its own migration starts at ["a"].
    let divergent = load(&format!(
        r#"{}"history":{history}"#,
        tables.replace("done", "archived").replace(
            r#""title": "string""#,
            &wide_type.replace(r#""b""#, r#""c""#)
        ),
        history = history.replace("done", "archived")
    ));
    let (status, response) = publish(&router, divergent.deployment_body(false)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{response}");
    let reason = response["error"].as_str().unwrap();
    assert!(
        reason.contains("tasks.title") && reason.contains("enum variants"),
        "{response}"
    );
}

/// Endpoints that agree cannot conceal a different ciphertext encoding in the middle version.
#[tokio::test]
async fn encryption_refuses_a_nullability_flip_flop_in_one_deployment() {
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": {"notes":{"columns":{"title":"bytes","done":"boolean","pinned":"boolean"},
            "encryption":{"space":"id","scope":"notes","columns":{"title":"string"}}}},
        "history": {
            "first":{"tables":{"notes":{"columns":{"title":"bytes"},
                "encryption":{"space":"id","scope":"notes","columns":{"title":"string"}}}}},
            "middle":{"tables":{"notes":{"columns":{"title":"bytes","done":"boolean"},
                "encryption":{"space":"id","scope":"notes","columns":{"title":"string"}}}}}
        },
        "migrations": [
            {"from":"first","to":"middle","migrate":{"notes":{"done":{"add":"boolean","default":false}}}},
            {"from":"middle","migrate":{"notes":{"pinned":{"add":"boolean","default":false}}}}
        ] }"#;
    let policy = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let app = artifact::load(file.as_bytes(), policy.as_bytes()).unwrap();
    let mut body = app.deployment_body(false);
    let middle = body["migrations"][0]["toHash"].as_str().unwrap().to_owned();
    assert_ne!(middle, app.current_hash().to_string());
    body["encryption"]["versions"][&middle]["notes"]["columns"]["title"]["type"]["nullable"] =
        json!(true);
    let (status, refusal) = publish(&router().await, body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"].as_str().unwrap().contains("notes.title"),
        "{refusal}"
    );
}

/// Alice appends an enum variant on a standalone version. A migration from
/// her original version renames the column and must preserve that variant.
#[tokio::test]
async fn a_renamed_enum_keeps_variants_from_deployments_outside_its_migration() {
    let router = router().await;
    let permissions = permissions(MEMBERS_RULE, "ownerId = session.user");
    let load = |source: &str| artifact::load(source.as_bytes(), permissions.as_bytes()).unwrap();
    let original = r#""title": {"enum":["a"]}"#;
    let v1 = load(&APP.replace(r#""title": "string""#, original));
    let (status, body) = publish(&router, v1.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let branch = APP
        .replace(
            r#""projectId": "uuid", "title": "bytes""#,
            r#""title": "bytes", "projectId": "uuid""#,
        )
        .replace(
            r#""title": "string""#,
            r#""title": {"type":{"enum":["a","b"]},"sealedType":{"enum":["a"]}}"#,
        );
    let branch = load(&branch);
    assert_ne!(branch.current_hash(), v1.current_hash());
    let (status, body) = publish(&router, branch.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (current, history) = APP_V2.split_once(r#""history":"#).unwrap();
    let current = current.replace("title", "caption");
    let history = history.replace(r#""title": "string""#, original).replace(
        r#""done": { "add""#,
        r#""caption": { "renameFrom": "title" }, "__e2ee_eq_caption": { "renameFrom": "__e2ee_eq_title" }, "done": { "add""#,
    );
    let renamed = |variants| {
        let declaration =
            format!(r#""caption": {{"type":{{"enum":{variants}}},"sealedType":{{"enum":["a"]}}}}"#);
        let current = current.replace(r#""caption": "string""#, &declaration);
        load(&format!(r#"{current}"history":{history}"#))
    };
    let candidate = renamed(r#"["a","c"]"#);
    let (status, refusal) = publish(&router, candidate.deployment_body(false)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    let reason = refusal["error"].as_str().unwrap();
    assert!(
        reason.contains("tasks.caption") && reason.contains("enum variants"),
        "{refusal}"
    );
    let compatible = renamed(r#"["a","b"]"#);
    assert_eq!(candidate.current_hash(), compatible.current_hash());
    let (status, body) = publish(&router, compatible.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = publish(&router, v1.deployment_body(false)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "replaying the original declaration: {body}"
    );
}

/// Alice cannot convert into a fresh stored column that reuses an old name.
#[tokio::test]
async fn a_new_column_reusing_a_name_still_refuses_an_unknown_source_field() {
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": {
            "columns": { "oldTitle": "bytes", "title": { "type": "bytes", "default": "" } },
            "encryption": { "space": "id", "scope": "notes",
                "columns": { "title": { "type": "string?", "from": "oldTitle" } } } } },
        "history": {
            "v1": { "tables": { "notes": { "columns": { "title": "bytes" },
                "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } } },
            "v2": { "tables": { "notes": { "columns": { "oldTitle": "bytes" },
                "encryption": { "space": "id", "scope": "notes", "columns": { "oldTitle": "string" } } } } } },
        "migrations": [
            { "from": "v1", "to": "v2", "migrate": { "notes": { "oldTitle": { "renameFrom": "title" } } } },
            { "from": "v2", "migrate": { "notes": { "title": { "add": "bytes", "default": "" } } } }] }"#;
    let permissions = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let error = artifact::load(file.as_bytes(), permissions.as_bytes())
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("tables.notes.encryption.columns.title")
            && error.contains("unknown key `from`"),
        "{error}"
    );
}

/// Redeclaring an encrypted column checks its fields and preserves equality
/// while its token column stays.
#[tokio::test]
async fn a_redeclaration_refuses_unknown_fields_and_keeps_required_equality() {
    let router = router().await;
    let app = loaded("ownerId = session.user");
    let (status, body) = publish(&router, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let hash = app.current_hash().to_string();

    let mut converted = app.deployment_body(false);
    converted["encryption"]["versions"][&hash]["tasks"]["columns"]["title"] = json!({
        "type": { "columnType": { "type": "Integer" }, "nullable": false },
        "from": "projectId",
        "convert": "parse",
    });
    let (status, refusal) = publish(&router, converted).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refusal}");
    assert!(
        refusal["error"].as_str().unwrap().contains("unknown field"),
        "{refusal}"
    );

    let mut untagged = app.deployment_body(false);
    untagged["encryption"]["versions"][&hash]["tasks"]
        .as_object_mut()
        .unwrap()
        .remove("equality");
    let (status, refusal) = publish(&router, untagged).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"].as_str().unwrap().contains("token column"),
        "{refusal}"
    );
}

/// Declarations kept for a version are never replaced: keeping the same
/// ones again changes nothing, and keeping different ones is refused
/// instead of ignored.
#[tokio::test]
async fn kept_declarations_are_never_replaced() {
    let server = ServerBuilder::new(app_id())
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .unwrap();
    let kept = server
        .state
        .store
        .as_ref()
        .unwrap()
        .kv(super::DECLARATIONS)
        .unwrap();
    let version = jazz::tools::public_schema::SchemaHash::compute(
        &jazz::tools::public_schema::SchemaBuilder::new().build(),
    );
    super::keep_declarations(Some(&kept), &[(version, b"first".to_vec())]).unwrap();
    super::keep_declarations(Some(&kept), &[(version, b"first".to_vec())])
        .expect("the same declarations again");
    let refusal = super::keep_declarations(Some(&kept), &[(version, b"other".to_vec())])
        .expect_err("other declarations for a kept version");
    assert_eq!(refusal.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(kept.get(&version.0).unwrap(), Some(b"first".to_vec()));
}

/// A deployment that changes the catalogue keeps its first-declared
/// versions' declarations with it, in the same transaction: once the
/// deploy answers, the store holds them, and a repost of the same
/// declarations changes nothing.
#[tokio::test]
async fn a_deployment_keeps_its_declarations_with_the_catalogue_change() {
    let server = ServerBuilder::new(app_id())
        .with_auth_config(AuthConfig {
            admin_secret: Some("test-admin".into()),
            ..Default::default()
        })
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .unwrap();
    let router = create_router(server.state.clone());
    let app = loaded("ownerId = session.user");
    let kept = server
        .state
        .store
        .as_ref()
        .unwrap()
        .kv(super::DECLARATIONS)
        .unwrap();
    assert_eq!(kept.get(&app.current_hash().0).unwrap(), None);
    let (status, body) = publish(&router, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored = kept.get(&app.current_hash().0).unwrap().expect("kept");
    let entries = jazz::tools::deployment::artifact::encryption::decode_stored(&stored).unwrap();
    assert_eq!(
        entries,
        *app.encryption()
            .entries(&app.current_hash().to_string())
            .unwrap()
    );
    let (status, body) = publish(&router, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(kept.get(&app.current_hash().0).unwrap(), Some(stored));
}

/// Alice prepares the first deployment, then Bob publishes different declarations for
/// its schema. Alice's failed start leaves the store without a catalogue or runtime.
#[tokio::test]
async fn a_conflicting_first_deployment_rolls_back_initialization() {
    let (_, state) = router_and_state().await;
    let store = state.store.as_ref().unwrap();
    let app = loaded("ownerId = session.user");
    let hash = app.current_hash();
    let body = serde_json::from_value(app.deployment_body(false)).unwrap();
    let index = state.catalogue_store.index().unwrap();
    let deployment =
        crate::portable::deployment::prepare_encrypted(&index, body, 1_000, |_| Ok(None))
            .unwrap_or_else(|error| panic!("{}", error.body));
    let kept = store.kv(super::DECLARATIONS).unwrap();
    kept.put_if_absent(&hash.0, b"another deployment").unwrap();
    let catalogue = store.catalogue().unwrap();
    let before = catalogue.read().unwrap();
    let change = deployment.catalogue_change(&before).unwrap();
    let values = deployment
        .first_declared
        .iter()
        .map(|(version, bytes)| (super::DECLARATIONS, version.0.to_vec(), bytes.clone()))
        .collect();
    let result = store.start_deployment(deployment.genesis().unwrap(), change, values);
    assert!(matches!(
        result,
        Err(crate::runtime::CatalogueApplyError::Rejected(_))
    ));
    assert_eq!(catalogue.read().unwrap(), before);
    assert_eq!(
        kept.get(&hash.0).unwrap().as_deref(),
        Some(b"another deployment".as_slice())
    );
}

/// Two instances sharing a store deploy the same new version with
/// different declarations: the second prepared its deployment before the
/// first kept its own, so its check saw nothing kept. It is refused when
/// the catalogue change applies, and none of its catalogue rows land.
///
/// ```text
/// B prepares v2 (nothing kept yet)
///   A keeps v2's declarations
/// B applies v2 + its declarations ──✗ conflict: refused, v2 not in the catalogue
/// ```
#[tokio::test]
async fn a_racing_deploy_with_conflicting_declarations_is_refused_without_its_catalogue() {
    let server = ServerBuilder::new(app_id())
        .with_auth_config(AuthConfig {
            admin_secret: Some("test-admin".into()),
            ..Default::default()
        })
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .unwrap();
    let state = server.state.clone();
    let router = create_router(state.clone());
    let v1 = loaded("ownerId = session.user");
    let (status, body) = publish(&router, v1.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let v2 = artifact::load(
        APP_V2.as_bytes(),
        permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let body: crate::portable::deployment::DeployBody =
        serde_json::from_value(v2.deployment_body(false)).unwrap();

    // B prepares while the store holds v1's declarations but nothing for v2.
    let kept = state
        .store
        .as_ref()
        .unwrap()
        .kv(super::DECLARATIONS)
        .unwrap();
    let index = state.catalogue_store.index().unwrap();
    let deployment =
        crate::portable::deployment::prepare_encrypted(&index, body, 1_000, |version| {
            kept.get(&version.0)
        })
        .unwrap_or_else(|error| panic!("{}", error.body));
    let v2_hash = v2.current_hash();
    assert!(
        deployment
            .first_declared
            .iter()
            .any(|(version, _)| *version == v2_hash)
    );
    // A keeps different declarations for v2 meanwhile.
    kept.put_if_absent(&v2_hash.0, b"declarations another instance kept")
        .unwrap();

    let runtime = state.runtime().expect("a runtime after the first deploy");
    let change = super::change(&runtime, &deployment).await.unwrap();
    let kept_values = deployment
        .first_declared
        .iter()
        .map(|(version, stored)| (super::DECLARATIONS, version.0.to_vec(), stored.clone()))
        .collect();
    let refused = runtime
        .apply_catalogue_keeping(change, deployment.staged.new_bundle(), kept_values)
        .await
        .expect_err("a conflicting kept declaration");
    assert!(
        matches!(refused, crate::runtime::CatalogueApplyError::Rejected(_)),
        "{refused}"
    );
    state.catalogue_store.refresh().unwrap();
    let schemas = state
        .catalogue_store
        .index()
        .unwrap()
        .deployment_catalogue()
        .schemas
        .len();
    assert_eq!(
        schemas, 1,
        "only v1 is in the catalogue: v2 was not applied"
    );
    assert_eq!(
        kept.get(&v2_hash.0).unwrap(),
        Some(b"declarations another instance kept".to_vec())
    );
}

/// A deploy over the real route whose kept declarations conflict at the
/// moment the catalogue change applies (another instance kept different
/// ones after this deploy checked) is refused with the catalogue untouched:
/// the declarations are kept in the transaction that commits the change.
/// Keeping them after the commit would leave the version live and the deploy
/// erroring.
///
/// ```text
/// deploy v2: checked (nothing kept) ─┐ the store thread is busy
///                                    │  another instance keeps v2's declarations
///                                    └─ the change applies ──✗ refused, v2 not live
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deploy_whose_declarations_conflict_at_apply_leaves_nothing_live() {
    use jazz::host::HostTables;
    use jazz::tools::deployment::artifact::encryption::encode_stored;
    use jazz::tools::public_schema::ColumnType;
    use std::sync::mpsc;
    use std::time::Duration;
    let server = ServerBuilder::new(app_id())
        .with_auth_config(AuthConfig {
            admin_secret: Some("test-admin".into()),
            ..Default::default()
        })
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .unwrap();
    let state = server.state.clone();
    let router = create_router(state.clone());
    let v1 = loaded("ownerId = session.user");
    let (status, body) = publish(&router, v1.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let v2 = artifact::load(
        APP_V2.as_bytes(),
        permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let kept = state
        .store
        .as_ref()
        .unwrap()
        .kv(super::DECLARATIONS)
        .unwrap();
    let runtime = state.runtime().expect("a runtime after the first deploy");

    // Declarations another instance keeps for v2: well formed, and different
    // from the deploy's (the title's type), so a deploy that sees them is
    // refused at its check and one that applies over them at its apply.
    let v2_hash = v2.current_hash();
    let mut other_entries = v2
        .encryption()
        .entries(&v2_hash.to_string())
        .unwrap()
        .clone();
    other_entries
        .get_mut("tasks")
        .unwrap()
        .columns
        .get_mut("title")
        .unwrap()
        .plain
        .column_type = ColumnType::Integer;
    let other = encode_stored(&other_entries).unwrap();

    // Hold the store thread (job H1). The deploy's read of the kept
    // declarations is host work, which waits for it and then runs ahead of
    // queued runtime work, so a second job H2 queued meanwhile, which keeps
    // the other instance's declarations, runs after that read and before the
    // deploy's own catalogue job: the declarations conflict only when the
    // change applies.
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let h1 = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .with_db_for_test(move |_| {
                    entered_tx.send(()).unwrap();
                    let _ = release_rx.recv();
                    Ok(())
                })
                .await
        })
    };
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();
    let deploying = {
        let router = router.clone();
        let body = v2.deployment_body(false);
        tokio::spawn(async move { publish(&router, body).await })
    };
    // No signal says the deploy's read is queued, so wait for it. A slow
    // machine can only make H2 run before that read, which then refuses the
    // deploy at its check (the same 422, v2 not live): the test never fails
    // for being slow, it only stops reaching the apply.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let h2 = {
        let runtime = runtime.clone();
        let other = other.clone();
        tokio::spawn(async move {
            runtime
                .with_db_for_test(move |db| {
                    jazz::db::block_on(db.host_put_if_absent(
                        super::DECLARATIONS,
                        &v2_hash.0,
                        &other,
                    ))
                    .map_err(|error| error.to_string())?;
                    Ok(())
                })
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    release_tx.send(()).unwrap();
    h1.await.unwrap().unwrap();
    h2.await.unwrap().unwrap();
    let (status, refusal) = deploying.await.unwrap();
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    state.catalogue_store.refresh().unwrap();
    let schemas = state
        .catalogue_store
        .index()
        .unwrap()
        .deployment_catalogue()
        .schemas
        .len();
    assert_eq!(schemas, 1, "v2 is not live: only v1 is in the catalogue");
    assert_eq!(kept.get(&v2_hash.0).unwrap(), Some(other));
}

/// An app whose current version declares no encryption but follows one that
/// does keeps Jazz's system tables: the loader appends them, the server's
/// check requires them, and the client and the server compute the same
/// schema hash and bundle.
#[tokio::test]
async fn a_later_version_without_declarations_deploys_with_the_system_tables() {
    let router = router().await;
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "done": "boolean" } } },
        "history": { "v1": { "tables": { "notes": {
            "columns": { "title": "bytes", "done": "boolean" },
            "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } } } },
        "migrations": [{ "from": "v1", "migrate": { "notes": {
            "title": { "drop": "bytes", "backwardsDefault": "" } } } }] }"#;
    let permissions = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let app =
        artifact::load(file.as_bytes(), permissions.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    let (status, body) = publish(&router, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["bundle"], app.bundle().to_hex());
    let (client_schema, _, _) = artifact::load_client_encrypted(
        file.as_bytes(),
        jazz::tools::AppId::from_name("notes-app"),
        "production",
    )
    .unwrap();
    assert_eq!(
        jazz::tools::public_schema::SchemaHash::compute(&client_schema),
        app.current_hash(),
        "the client and the server compute the same hash"
    );
    assert!(
        body["published"]["schemas"]
            .as_array()
            .unwrap()
            .iter()
            .any(|hash| *hash == json!(app.current_hash().to_string()))
    );
}

fn space_app_files() -> (Value, Value) {
    let file: Value = serde_json::from_str(include_str!(
        "../../../../jazz/tests/fixtures/drop_space_table.json"
    ))
    .unwrap();
    let permissions = json!({
        "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "true" } }
    });
    (file, permissions)
}

fn load_space_app(file: &Value, permissions: &Value) -> LoadedApp {
    artifact::load(
        file.to_string().as_bytes(),
        permissions.to_string().as_bytes(),
    )
    .unwrap()
}

/// Alice drops rooms without declaring a replacement space in the deployment body.
#[tokio::test]
async fn dropping_an_inherited_space_table_is_an_invalid_schema() {
    let (file, permissions) = space_app_files();
    let app = load_space_app(&file, &permissions);
    let mut inherited = app.deployment_body(false);
    inherited["encryption"]["versions"]
        .as_object_mut()
        .unwrap()
        .remove(&app.current_hash().to_string());
    let (status, response) = publish(&router().await, inherited).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{response}");
    assert_eq!(response["code"], "invalid_schema", "{response}");
    let error = response["error"].as_str().unwrap();
    assert!(
        error.contains("space table rooms is not in this version (dropped or renamed)"),
        "{response}"
    );
}

/// Alice drops rooms by declaring notes as a space with no encrypted columns.
#[tokio::test]
async fn dropping_an_inherited_space_table_with_an_empty_column_entry_deploys() {
    let (file, permissions) = space_app_files();
    let app = load_space_app(&file, &permissions);
    let (status, response) = publish(&router().await, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["bundle"], app.bundle().to_hex());
}

/// Alice keeps the rooms space and its membership rule while dropping messages.
#[tokio::test]
async fn keeping_an_inherited_space_table_deploys() {
    let (mut file, mut permissions) = space_app_files();
    file["tables"]["notes"]
        .as_object_mut()
        .unwrap()
        .remove("encryption");
    file["tables"]["rooms"] = file["history"]["v1"]["tables"]["rooms"].clone();
    file["migrations"][0]["dropTables"] = json!(["messages"]);
    permissions["tables"]["rooms"] = json!({ "read": MEMBERS_RULE.replace("projects", "rooms") });
    let kept = load_space_app(&file, &permissions);
    let (status, response) = publish(&router().await, kept.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["bundle"], kept.bundle().to_hex());
}

/// Alice can declare encryption on the first deployment of a schema that
/// the host already knows as its unpublished genesis.
#[test]
fn the_first_deployment_of_an_unpublished_genesis_can_declare_encryption() {
    use crate::portable::catalogue::CatalogueIndex;
    use crate::portable::deployment::prepare_encrypted;

    let app = loaded("ownerId = session.user");
    let index = CatalogueIndex::from_rows(&Default::default(), Some(app.current_schema()));
    let body = serde_json::from_value(app.deployment_body(false)).unwrap();
    let deployment = prepare_encrypted(&index, body, 1_000, |_| Ok(None))
        .unwrap_or_else(|error| panic!("{}", error.body));
    assert_eq!(deployment.first_declared.len(), 1);
    assert_eq!(deployment.first_declared[0].0, app.current_hash());
}

/// When Alice redeploys a schema that previously declared no encryption,
/// its existing plaintext columns must stay plaintext.
///
/// ```text
/// Alice deploys v1 (encrypted title) and v2 (plaintext body)
///   └─► redeploy v2 declaring body encrypted in place ──✗ refused
/// ```
#[tokio::test]
async fn a_version_deployed_without_declarations_cannot_encrypt_in_place() {
    let (router, state) = router_and_state().await;
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "body": "bytes" } } },
        "history": { "v1": { "tables": { "notes": {
            "columns": { "title": "bytes", "body": "bytes" },
            "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } } } },
        "migrations": [{ "from": "v1", "migrate": { "notes": {
            "title": { "drop": "bytes", "backwardsDefault": "" } } } }] }"#;
    let permissions = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let app =
        artifact::load(file.as_bytes(), permissions.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    let mut deployment = app.deployment_body(false);
    let (status, body) = publish(&router, deployment.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = publish(&router, deployment.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The migration is already stored. Redeploy the same schema hash
    // without submitting a new migration that would independently refuse it.
    deployment["migrations"] = json!([]);
    let versions = deployment["encryption"]["versions"]
        .as_object_mut()
        .unwrap();
    let mut entry = versions.values().next().unwrap()["notes"].clone();
    entry["columns"] = json!({ "body": entry["columns"]["title"] });
    versions.insert(app.current_hash().to_string(), json!({ "notes": entry }));
    let (status, refusal) = publish(&router, deployment.clone()).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("held plaintext"),
        "{refusal}"
    );

    // Inspect storage directly to pin the durable record for an empty set.
    let kept = state
        .store
        .as_ref()
        .unwrap()
        .kv(super::DECLARATIONS)
        .unwrap();
    let target = app.current_hash();
    assert_eq!(
        kept.get(&target.0).unwrap().as_deref(),
        Some(b"jazz-e2ee-declarations-v1\n{}".as_slice())
    );

    // A catalogue created before empty records were written must also
    // reject the request, even though its store has no record for v2.
    let index = state.catalogue_store.index().unwrap();
    let Err(legacy_refusal) = crate::portable::deployment::prepare_encrypted(
        &index,
        serde_json::from_value(deployment).unwrap(),
        1_000,
        |version| {
            if *version == target {
                Ok(None)
            } else {
                kept.get(&version.0)
            }
        },
    ) else {
        panic!("the existing plaintext column cannot be encrypted in place");
    };
    assert_eq!(legacy_refusal.body, refusal);
}

/// A policy of a later version that declares nothing may read a column that
/// is plaintext in it, though an earlier version had one of that name
/// encrypted: the server's check refuses only the columns the target itself
/// declares encrypted.
#[tokio::test]
async fn a_policy_may_read_a_column_that_the_target_keeps_in_plaintext() {
    let router = router().await;
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "title": "string?" } } },
        "history": {
            "v1": { "tables": { "notes": {
                "columns": { "title": "bytes" },
                "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } } },
            "v2": { "tables": { "notes": {
                "columns": { "title": "bytes", "titleText": "string?" },
                "encryption": { "space": "id", "scope": "notes",
                    "columns": { "title": "string" } } } } },
            "v3": { "tables": { "notes": { "columns": { "titleText": "string?" } } } } },
        "migrations": [
            { "from": "v1", "to": "v2", "migrate": { "notes": { "titleText": { "add": "string?", "default": null } } } },
            { "from": "v2", "to": "v3", "migrate": { "notes": { "title": { "drop": "bytes", "backwardsDefault": "" } } } },
            { "from": "v3", "migrate": { "notes": { "title": { "renameFrom": "titleText" } } } }] }"#;
    let permissions = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user) and title = 'public'" } } }"#;
    let app =
        artifact::load(file.as_bytes(), permissions.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    let body = app.deployment_body(false);
    let (status, deployed) = publish(&router, body).await;
    assert_eq!(status, StatusCode::OK, "{deployed}");
}

/// On a memory store, a second connection to the shared in-memory database
/// is locked out while the store's connection holds a write, so the
/// deployment reads the kept declarations on the store's own thread. A
/// deploy that arrives while the store is inside a write waits for it and
/// succeeds, instead of failing with a locked database.
///
/// ```text
/// store thread: write in progress ─────────────┐
/// deploy v2: reads kept declarations ─ waits ──┴─► ✓ deployed
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_memory_store_deploy_reads_kept_declarations_while_a_write_is_open() {
    use jazz::host::HostTables;
    use std::sync::mpsc;
    use std::time::Duration;
    let server = ServerBuilder::new(app_id())
        .with_auth_config(AuthConfig {
            admin_secret: Some("test-admin".into()),
            ..Default::default()
        })
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .unwrap();
    let state = server.state.clone();
    let router = create_router(state.clone());
    // An encrypted app deploys to a fresh memory store.
    let v1 = loaded("ownerId = session.user");
    let (status, body) = publish(&router, v1.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let v2 = artifact::load(
        APP_V2.as_bytes(),
        permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let runtime = state.runtime().expect("a runtime after the first deploy");
    let v2_hash = v2.current_hash();

    // The store's connection is inside a write (a host write's savepoint).
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let held = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .with_db_for_test(move |db| {
                    jazz::db::block_on(db.host_sql(&mut async |conn: &jazz::storage::Storage| {
                        conn.execute_batch(
                            "CREATE TABLE IF NOT EXISTS _jazz_host_e2ee_declarations \
                             (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID; \
                             INSERT OR REPLACE INTO _jazz_host_e2ee_declarations \
                             VALUES (x'01', x'02')",
                        )
                        .await
                        .map_err(|error| {
                            jazz::db::Error::new(jazz::db::ErrorCode::Storage, error.to_string())
                        })?;
                        entered_tx.send(()).unwrap();
                        let _ = release_rx.recv();
                        Ok(())
                    }))
                    .map_err(|error| error.to_string())
                })
                .await
        })
    };
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();
    let deploying = {
        let router = router.clone();
        let body = v2.deployment_body(false);
        tokio::spawn(async move { publish(&router, body).await })
    };
    // The write stays open longer than a second connection would wait for it
    // (SQLite's busy timeout is 10 seconds): the deploy's read of the kept
    // declarations, made on the store's own thread, waits for it and
    // succeeds, where one made on another connection fails as locked.
    let kept = state
        .store
        .as_ref()
        .unwrap()
        .kv(super::DECLARATIONS)
        .unwrap();
    let reading = tokio::task::spawn_blocking(move || kept.get_on_store(&v2_hash.0));
    tokio::time::sleep(Duration::from_secs(11)).await;
    release_tx.send(()).unwrap();
    held.await.unwrap().unwrap();
    let during = reading.await.unwrap();
    assert!(during.is_ok(), "{during:?}");
    let (status, deployed) = deploying.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{deployed}");
}

/// The loader gives the system tables only to versions a migration leads to
/// from one that has them, as the server follows the migrations: a plain
/// version listed after the encrypting one stays plain, and a later plain
/// version that hangs off it is refused by the server plainly (a stored
/// version has the tables, and this one leads to none), not given the tables
/// by the loader.
///
/// ```text
/// v1 ──► v2 (encrypts) ──► current        have the tables
/// v1 ──► v3 (plain, listed after v2)      stays plain
/// v3 ──► v4 (new, plain)                  ✗ refused by the server
/// ```
#[tokio::test]
async fn a_plain_branch_listed_after_an_encrypting_version_stays_plain() {
    use jazz::tools::public_schema::TableName;
    let router = router().await;
    let permissions = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let file = |extra: &str, extra_migration: &str| {
        format!(
            r#"{{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": {{ "notes": {{ "columns": {{ "done": "boolean" }} }} }},
        "history": {{
            "v1": {{ "tables": {{ "notes": {{ "columns": {{ "done": "boolean" }} }} }} }},
            "v2": {{ "tables": {{ "notes": {{
                "columns": {{ "done": "boolean", "title": "bytes" }},
                "encryption": {{ "space": "id", "scope": "notes",
                    "columns": {{ "title": {{ "type": "string", "default": "" }} }} }} }} }} }},
            "v3": {{ "tables": {{ "notes": {{ "columns": {{ "done": "boolean", "extra": "boolean?" }} }} }} }}{extra} }},
        "migrations": [
            {{ "from": "v1", "to": "v2", "migrate": {{ "notes": {{ "title": {{ "add": "bytes", "default": "" }} }} }} }},
            {{ "from": "v2", "migrate": {{ "notes": {{ "title": {{ "drop": "bytes", "backwardsDefault": "" }} }} }} }},
            {{ "from": "v1", "to": "v3", "migrate": {{ "notes": {{ "extra": {{ "add": "boolean?", "default": null }} }} }} }}{extra_migration}] }}"#
        )
    };
    let members = TableName::from("__e2ee_members");
    let first = artifact::load(file("", "").as_bytes(), permissions.as_bytes())
        .unwrap_or_else(|e| panic!("{e}"));
    let carrying: Vec<bool> = first
        .schemas()
        .iter()
        .map(|(_, schema)| schema.contains_key(&members))
        .collect();
    assert_eq!(
        carrying.iter().filter(|carries| **carries).count(),
        2,
        "v2 and the current version, not the plain v3 listed after v2: {carrying:?}"
    );
    let (status, body) = publish(&router, first.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A new plain version hanging off the plain one, leading to no version
    // that has the tables.
    let sibling = file(
        r#",
            "v4": { "tables": { "notes": { "columns": { "done": "boolean", "extra": "boolean?", "other": "string?" } } } }"#,
        r#",
            { "from": "v3", "to": "v4", "migrate": { "notes": { "other": { "add": "string?", "default": null } } } }"#,
    );
    let second = artifact::load(sibling.as_bytes(), permissions.as_bytes())
        .unwrap_or_else(|e| panic!("{e}"));
    let (status, refusal) = publish(&router, second.deployment_body(false)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("a stored version has Jazz's system tables and this one lacks them"),
        "{refusal}"
    );
}

/// A deployment can't drop a stored version's declarations to slip past
/// the checks they bring: reposting the deployed version with no
/// `encryption` (and nothing to submit) and a read rule that doesn't come
/// from the members table is refused, as is one that keeps the
/// declarations but breaks the rule.
#[tokio::test]
async fn a_repost_without_a_versions_declarations_is_refused() {
    let router = router().await;
    let app = loaded("ownerId = session.user");
    let (status, body) = publish(&router, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let owner_only = serde_json::to_value(
        &app.permissions()
            .get(&jazz::tools::public_schema::TableName::from("projects"))
            .unwrap()
            .update,
    )
    .unwrap();

    let mut repost = app.deployment_body(false);
    repost.as_object_mut().unwrap().remove("encryption");
    repost["schemas"] = json!([]);
    repost["permissions"]["projects"]["select"] = owner_only.clone();
    let (status, refusal) = publish(&router, repost).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("was deployed with encryption declarations"),
        "{refusal}"
    );

    let mut declared = app.deployment_body(false);
    declared["schemas"] = json!([]);
    declared["permissions"]["projects"]["select"] = owner_only;
    let (status, refusal) = publish(&router, declared).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("must come from the members table"),
        "{refusal}"
    );
}

/// The app at a second version: tasks gain a plaintext `done`, by a
/// migration from the deployed first version.
const APP_V2: &str = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
    "tables": {
        "projects": { "columns": { "name": "string", "ownerId": "uuid" } },
        "tasks": {
            "columns": { "projectId": "uuid", "title": "bytes", "__e2ee_eq_title": "bytes", "done": "boolean" },
            "relations": { "project": { "rel": "projects", "column": "projectId" } },
            "encryption": { "space": "projectId", "scope": "projects",
                "columns": { "title": "string" }, "equality": ["title"] }
        }
    },
    "history": { "v1": { "tables": {
        "projects": { "columns": { "name": "string", "ownerId": "uuid" } },
        "tasks": {
            "columns": { "projectId": { "type": "uuid", "references": "projects" }, "title": "bytes", "__e2ee_eq_title": "bytes" },
            "encryption": { "space": "projectId", "scope": "projects",
                "columns": { "title": "string" }, "equality": ["title"] }
        }
    } } },
    "migrations": [{ "from": "v1", "migrate": { "tasks": { "done": { "add": "boolean", "default": false } } } }] }"#;

/// A migration from a deployed encrypted version is judged by the
/// declarations that version was deployed with: leaving them out of the
/// body, which would let the migration treat ciphertext as plaintext, is
/// refused; sending them, the migration deploys.
#[tokio::test]
async fn a_migration_from_a_stored_version_keeps_its_declarations() {
    let router = router().await;
    let v1 = loaded("ownerId = session.user");
    let (status, body) = publish(&router, v1.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let v2 = artifact::load(
        APP_V2.as_bytes(),
        permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert!(
        v2.schemas()
            .iter()
            .any(|(hash, _)| *hash == v1.current_hash()),
        "the first version is the one deployed"
    );

    let mut omitted = v2.deployment_body(false);
    omitted["encryption"]["versions"]
        .as_object_mut()
        .unwrap()
        .remove(&v1.current_hash().to_string());
    let (status, refusal) = publish(&router, omitted).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("was deployed with encryption declarations"),
        "{refusal}"
    );

    let (status, body) = publish(&router, v2.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// An app's own permissions may not hold the policy nodes Jazz derives
/// for its system tables.
#[tokio::test]
async fn an_app_policy_with_a_system_only_node_is_refused() {
    let router = router().await;
    let mut body = loaded("ownerId = session.user").deployment_body(false);
    body["permissions"]["tasks"]["insert"]["with_check"] =
        json!({ "type": "InsertedByThisTransaction" });
    let (status, refusal) = publish(&router, body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("for Jazz's system tables only"),
        "{refusal}"
    );
}

/// A migration from a deployed encrypted version is judged against the
/// declarations of both of its versions: one that retypes an encrypted
/// column in place would make the old cells unreadable, so the
/// deploy refuses it, naming the column, while the same migration with the
/// declarations the app's files give deploys.
///
/// ```text
/// first version deployed ──► ✓
/// migration whose declarations retype tasks.title (string → int) ──► ✗
/// the migration as the app's files declare it                    ──► ✓
/// ```
#[tokio::test]
async fn a_migration_that_retypes_an_encrypted_column_is_refused() {
    let router = router().await;
    let v1 = loaded("ownerId = session.user");
    let (status, body) = publish(&router, v1.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let v2 = artifact::load(
        APP_V2.as_bytes(),
        permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let mut retyped = v2.deployment_body(false);
    let second = retyped["migrations"][0]["toHash"]
        .as_str()
        .unwrap()
        .to_owned();
    retyped["encryption"]["versions"][&second]["tasks"]["columns"]["title"]["type"]["columnType"] =
        json!({ "type": "Integer" });
    let (status, refusal) = publish(&router, retyped).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    let error = refusal["error"].as_str().unwrap();
    assert!(
        error.contains("tasks.title: the encrypted column's type changes"),
        "{refusal}"
    );

    let (status, body) = publish(&router, v2.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// A raw deploy request that defines a table with the reserved prefix
/// which is not one of Jazz's system tables is refused, while the same
/// schema under an ordinary name deploys.
///
/// ```text
/// mallory ──{ __e2ee_notes }──► server ✗ reserved prefix
/// mallory ──{ notes }─────────► server ✓
/// ```
#[tokio::test]
async fn a_raw_deploy_of_a_foreign_system_table_is_refused() {
    let router = router().await;
    let body = |table: &str| {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder(table).column("body", ColumnType::Text))
            .build();
        let hash = SchemaHash::compute(&schema).to_string();
        json!({
            "targetSchemaHash": hash,
            "schemas": [{ "hash": hash, "schema": schema }],
            "migrations": [],
            "permissions": {},
        })
    };
    let (status, refusal) = publish(&router, body("__e2ee_notes")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("reserved prefix"),
        "{refusal}"
    );
    let (status, deployed) = publish(&router, body("notes")).await;
    assert_eq!(status, StatusCode::OK, "{deployed}");
}

/// The app's tables without Jazz's system tables.
fn plain_app() -> jazz::tools::public_schema::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("projects").column("ownerId", ColumnType::Uuid))
        .table(
            TableSchema::builder("tasks")
                .fk_column("projectId", "projects")
                .column("title", ColumnType::Bytea),
        )
        .build()
}

/// The app before it has a title to encrypt: the tables the plain server
/// runs, to which a later deployment adds the encrypted title.
fn before_app() -> jazz::tools::public_schema::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("projects").column("ownerId", ColumnType::Uuid))
        .table(TableSchema::builder("tasks").fk_column("projectId", "projects"))
        .build()
}

/// The app with Jazz's system tables, and the declarations that go with
/// it: tasks encrypt their title in their project's space.
fn encrypted_app() -> (
    jazz::tools::public_schema::Schema,
    jazz::tools::deployment::artifact::DeclaredEncryption,
) {
    use jazz::tools::deployment::artifact::DeclaredEncryption;
    use jazz::tools::deployment::artifact::system_tables::with_e2ee_fragment;
    use jazz::tools::public_schema::{EncryptedColumn, EncryptionEntry, PlainType};
    use std::collections::BTreeMap;
    let schema = with_e2ee_fragment(&plain_app());
    let hash = SchemaHash::compute(&schema).to_string();
    let entries = BTreeMap::from([(
        "tasks".to_owned(),
        EncryptionEntry {
            space: "projectId".to_owned(),
            scope: "projects".to_owned(),
            columns: BTreeMap::from([(
                "title".to_owned(),
                EncryptedColumn {
                    plain: PlainType {
                        column_type: ColumnType::Text,
                        nullable: false,
                        // A required encrypted column a migration adds
                        // declares the default older rows read.
                        default: Some(jazz::tools::public_schema::Value::Text(String::new())),
                    },
                    sealed_type: None,
                },
            )]),
            equality: Vec::new(),
        },
    )]);
    let declared = DeclaredEncryption {
        settings: Default::default(),
        versions: BTreeMap::from([(hash, entries)]),
    };
    (schema, declared)
}

/// An encrypted schema as a deploy body: tasks encrypt their title in their
/// project's space, whose read rule comes from the members table.
fn encrypted_body() -> Value {
    loaded("ownerId = session.user").deployment_body(false)
}

/// The plain app as a deploy body.
fn plain_body() -> Value {
    let schema = before_app();
    let hash = SchemaHash::compute(&schema).to_string();
    json!({
        "targetSchemaHash": hash,
        "schemas": [{ "hash": hash, "schema": schema }],
        "migrations": [],
        "permissions": {},
    })
}

/// The encrypted app as a deploy body over the plain one, which the
/// server already runs: the migration adds Jazz's system tables.
fn encryption_turned_on_body() -> Value {
    use jazz::tools::deployment::artifact::system_tables::is_system_table;
    let (schema, declared) = encrypted_app();
    let hash = SchemaHash::compute(&schema).to_string();
    let first = SchemaHash::compute(&before_app()).to_string();
    let mut added: Vec<Value> = schema
        .keys()
        .filter(|name| is_system_table(name.as_str()))
        .map(|name| {
            json!({
                "table": name.as_str(),
                "added": true,
                "renamedFrom": null,
                "operations": [],
            })
        })
        .collect();
    // The encrypted title is a new column of the existing tasks table.
    added.push(json!({
        "table": "tasks",
        "added": false,
        "renamedFrom": null,
        "operations": [{
            "type": "introduce",
            "column": "title",
            "column_type": ColumnType::Bytea,
            "value": jazz::tools::public_schema::Value::Bytea(Vec::new()),
        }],
    }));
    json!({
        "targetSchemaHash": hash,
        "schemas": [{ "hash": hash, "schema": schema }],
        "migrations": [{ "fromHash": first, "toHash": hash, "forward": added }],
        // The space table reads through the members table.
        "permissions": encrypted_body()["permissions"].clone(),
        "encryption": declared,
    })
}

/// A server with its account registry deploys an encrypted schema to a
/// fresh store.
#[tokio::test]
async fn an_encrypted_schema_deploys_to_a_server_with_its_registry() {
    let router = router().await;
    let (status, deployed) = publish(&router, encrypted_body()).await;
    assert_eq!(status, StatusCode::OK, "{deployed}");
}

/// A deployment that brings an encrypted schema to a runtime without the
/// account registry is refused before anything is applied, and one that
/// brings an ordinary schema is not.
///
/// ```text
/// runtime (no registry) ◄── encrypted schema ──✗ refused
/// runtime (no registry) ◄── ordinary schema ──✓
/// ```
#[tokio::test]
async fn an_encrypted_schema_is_not_deployed_to_a_runtime_without_the_registry() {
    use crate::runtime::ServerRuntimeHandle;
    use crate::runtime::StorageConfig;
    let server = ServerBuilder::new(app_id())
        .with_auth_config(AuthConfig {
            admin_secret: Some("test-admin".into()),
            ..Default::default()
        })
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .unwrap();
    // A standalone runtime reads no host tables, so it has no registry.
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("notes").column("body", ColumnType::Text))
        .build();
    *server.state.core_server_shell.write().unwrap() = Some(
        ServerRuntimeHandle::start_with_storage(
            jazz::schema::JazzSchema::new(&schema).unwrap(),
            StorageConfig::InMemory,
        )
        .unwrap(),
    );
    let router = create_router(server.state.clone());
    let (status, refusal) = publish(&router, encrypted_body()).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("account registry"),
        "{refusal}"
    );
    let hash = SchemaHash::compute(&schema).to_string();
    let (status, deployed) = publish(
        &router,
        json!({
            "targetSchemaHash": hash,
            "schemas": [{ "hash": hash, "schema": schema }],
            "migrations": [],
            "permissions": {},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{deployed}");
}

/// A host without the native store (a Durable Object) deploys through the
/// portable route with no declarations, which refuses Jazz's system tables:
/// a schema that has them must declare encryption.
#[test]
fn the_portable_deploy_refuses_system_tables_without_declarations() {
    use crate::portable::catalogue::CatalogueIndex;
    use crate::portable::deployment::{DeployRequest, DeploySchema, prepare};
    use jazz::tools::deployment::artifact::system_tables::with_e2ee_fragment;
    let schema = with_e2ee_fragment(
        &SchemaBuilder::new()
            .table(TableSchema::builder("notes").column("body", ColumnType::Text))
            .build(),
    );
    let hash = SchemaHash::compute(&schema).to_string();
    let index = CatalogueIndex::from_rows(&Default::default(), None);
    let Err(refusal) = prepare(
        &index,
        DeployRequest {
            target_schema_hash: hash.clone(),
            schemas: vec![DeploySchema { hash, schema }],
            migrations: Vec::new(),
            permissions: Default::default(),
            allow_dropping_history: false,
        },
        1_000,
    ) else {
        panic!("a schema with system tables and no declarations is refused");
    };
    assert!(
        refusal.body.to_string().contains("declares no encryption"),
        "{}",
        refusal.body
    );
}

/// An encrypting app serves device accounts founded for its own app id
/// only: the registry keeps no record of the founding namespace, so an
/// account founded for another audience could not be told from the app's
/// own. Before the app encrypts, any audience founds.
///
/// ```text
/// found-local-first {audience: other} ──► plain app ✓
/// deploy encrypted schema
/// found-local-first {audience: other} ──► ✗
/// found-local-first {}                ──► ✓   found-local-first {audience: app id} ──► ✓
/// ```
#[tokio::test]
async fn an_encrypting_app_founds_device_accounts_for_its_own_audience_only() {
    let router = router().await;
    let found = |audience: Option<String>, subject: &str| {
        let router = router.clone();
        let mut body = json!({
            "identity": {
                "issuer": jazz::tools::identity::DEVICE_ACCOUNT_ISSUER,
                "subject": subject,
            },
        });
        if let Some(audience) = audience {
            body["audience"] = audience.into();
        }
        async move {
            let response = router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!(
                            "/apps/{}/admin/accounts/found-local-first",
                            app_id()
                        ))
                        .header("X-Jazz-Admin-Secret", "test-admin")
                        .header("Content-Type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            response.status()
        }
    };
    let subject = |n: u8| uuid::Uuid::from_bytes([n; 16]).to_string();
    let other = Some("https://other.example/jazz".to_owned());
    assert_eq!(found(other.clone(), &subject(1)).await, StatusCode::OK);

    let (status, deployed) = publish(&router, encrypted_body()).await;
    assert_eq!(status, StatusCode::OK, "{deployed}");
    assert_eq!(
        found(other, &subject(2)).await,
        StatusCode::BAD_REQUEST,
        "another audience"
    );
    assert_eq!(found(None, &subject(3)).await, StatusCode::OK);
    assert_eq!(
        found(Some(app_id().to_string()), &subject(4)).await,
        StatusCode::OK,
        "the app's own id as the audience"
    );
}

/// A local-first guest's first admission founds its account in the namespace
/// of the audience its proof names, and an encrypting app refuses a guest
/// whose audience is not its own.
#[tokio::test]
async fn an_encrypting_app_admits_a_device_account_founder_of_its_own_audience_only() {
    use crate::server::routes::accounts::admit_local_founder;
    use axum::http::HeaderMap;
    let server = ServerBuilder::new(app_id())
        .with_auth_config(AuthConfig {
            admin_secret: Some("test-admin".into()),
            backend_secret: Some("test-backend".into()),
            ..Default::default()
        })
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .unwrap();
    let router = create_router(server.state.clone());
    let guest = |n: u8| jazz::account_registry::Principal {
        issuer: jazz::tools::identity::DEVICE_ACCOUNT_ISSUER.into(),
        subject: uuid::Uuid::from_bytes([n; 16]).to_string(),
    };
    let stating = |audience: &str| {
        let mut headers = HeaderMap::new();
        headers.insert(
            crate::middleware::auth::DEVICE_ACCOUNT_AUDIENCE_HEADER,
            audience.parse().unwrap(),
        );
        headers.insert("X-Jazz-Backend-Secret", "test-backend".parse().unwrap());
        headers
    };
    let other = "https://other.example/jazz";
    admit_local_founder(&server.state, &guest(1), &stating(other))
        .await
        .expect("before the app encrypts, any audience founds");

    let (status, deployed) = publish(&router, encrypted_body()).await;
    assert_eq!(status, StatusCode::OK, "{deployed}");
    admit_local_founder(&server.state, &guest(2), &stating(other))
        .await
        .expect_err("another audience");
    admit_local_founder(&server.state, &guest(3), &HeaderMap::new())
        .await
        .expect("no audience stated: the app's own");
    admit_local_founder(&server.state, &guest(4), &stating(&app_id().to_string()))
        .await
        .expect("the app's own id, stated");
}

/// A running server keeps its first schema while deployments admit later
/// ones, so what it knows of encryption follows the catalogue: an app that
/// turns encryption on in a later version, deployed to a server running its
/// plain first version, refuses another audience's founders at once, not
/// after a restart.
#[tokio::test]
async fn encryption_turned_on_by_a_later_deployment_refuses_foreign_founders_at_once() {
    let router = router().await;
    let found = |audience: Option<String>, subject: u8| {
        let router = router.clone();
        let mut body = json!({
            "identity": {
                "issuer": jazz::tools::identity::DEVICE_ACCOUNT_ISSUER,
                "subject": uuid::Uuid::from_bytes([subject; 16]).to_string(),
            },
        });
        if let Some(audience) = audience {
            body["audience"] = audience.into();
        }
        async move {
            router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!(
                            "/apps/{}/admin/accounts/found-local-first",
                            app_id()
                        ))
                        .header("X-Jazz-Admin-Secret", "test-admin")
                        .header("Content-Type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
        }
    };
    let other = Some("https://other.example/jazz".to_owned());
    let (status, deployed) = publish(&router, plain_body()).await;
    assert_eq!(status, StatusCode::OK, "{deployed}");
    assert_eq!(found(other.clone(), 1).await, StatusCode::OK);

    let (status, deployed) = publish(&router, encryption_turned_on_body()).await;
    assert_eq!(status, StatusCode::OK, "{deployed}");
    assert_eq!(
        found(other, 2).await,
        StatusCode::BAD_REQUEST,
        "the running server follows the catalogue"
    );
    assert_eq!(found(None, 3).await, StatusCode::OK);
}

/// Alice publishes a legacy app to a fresh server, upgrades, then tries the
/// old bundle again. Another fresh server can install the whole history at once.
#[tokio::test]
async fn system_table_upgrade_accepts_legacy_history_and_refuses_rollback() {
    let old = loaded("ownerId = session.user");
    let perms = permissions(MEMBERS_RULE, "ownerId = session.user");
    let file = APP_V2.replacen(r#""tables""#, r#""systemTablesVersion": 2, "tables""#, 1);
    let new = artifact::load(file.as_bytes(), perms.as_bytes()).unwrap();
    assert_eq!(new.schemas()[0].0, old.current_hash());
    let canonical = artifact::rewrite(file.as_bytes(), perms.as_bytes(), None).unwrap();
    let round_trip =
        artifact::load(canonical.app.as_bytes(), canonical.permissions.as_bytes()).unwrap();
    assert_eq!(round_trip.bundle(), new.bundle());
    let fresh = router().await;
    for app in [&old, &new] {
        let (status, body) = publish(&fresh, app.deployment_body(false)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["bundle"], app.bundle().to_hex());
    }
    let (status, body) = publish(&fresh, old.deployment_body(true)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["error"].as_str().unwrap().contains("cannot roll back"),
        "{body}"
    );
    let (status, body) = publish(&router().await, new.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A client can post raw lenses independently of the source loader.
    let mut forged = new.deployment_body(false);
    let operations = forged["migrations"][0]["forward"].as_array_mut().unwrap();
    let members = operations
        .iter_mut()
        .find(|op| op["table"] == "__e2ee_members")
        .unwrap();
    members["operations"][0]["value"] =
        serde_json::to_value(jazz::tools::Value::Text("forged".into())).unwrap();
    let (status, body) = publish(&router().await, forged).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["error"].as_str().unwrap().contains("may not change"),
        "{body}"
    );
}
