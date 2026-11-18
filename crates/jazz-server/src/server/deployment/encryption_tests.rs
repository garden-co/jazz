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

/// The server keeps each version's declarations from its first
/// deployment: editing an encrypted column's type in place, with the
/// schema unchanged, is refused, while pinning the old type for a
/// widening is not (migrations §3.1, §4.3).
#[tokio::test]
async fn a_deployed_versions_declarations_change_only_compatibly() {
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
    let (status, body) = publish(&router, widened.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// The server's kept declarations judge a redeployed version like a
/// migration: with the schema unchanged, a conversion may not write over a
/// column that is already encrypted (its cells are sealed under the old
/// type), and equality can't be dropped while its token column stays.
#[tokio::test]
async fn a_redeclaration_may_not_convert_over_a_column_or_drop_its_equality() {
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
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("was already encrypted"),
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

/// Decrypting a column puts its plaintext on the server, so the deploy
/// asks for an explicit confirmation naming it (migrations §4.2).
#[tokio::test]
async fn a_decrypt_needs_the_deployers_confirmation() {
    let router = router().await;
    let decrypting = APP
        .replace(
            r#""__e2ee_eq_title": "bytes" }"#,
            r#""__e2ee_eq_title": "bytes", "notes": "bytes", "notesText": "string?" }"#,
        )
        .replace(
            r#""columns": { "title": "string" }"#,
            r#""columns": { "title": "string", "notes": { "type": "string", "to": "notesText" } }"#,
        );
    let app = artifact::load(
        decrypting.as_bytes(),
        permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let (status, refusal) = publish(&router, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{refusal}");
    assert!(
        refusal["error"].as_str().unwrap().contains("tasks.notes"),
        "{refusal}"
    );
    let mut confirmed = app.deployment_body(false);
    confirmed["confirmDecrypt"] = json!(["tasks.notes"]);
    let (status, body) = publish(&router, confirmed).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // Confirmed once, the version's decrypt is known.
    let (status, body) = publish(&router, app.deployment_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// Confirming one decrypt does not confirm another: a deploy that decrypts
/// `tasks.notes` is refused when it names only `tasks.title`, or nothing,
/// and deploys when it names `tasks.notes` among others.
#[tokio::test]
async fn a_decrypt_is_confirmed_by_naming_its_column() {
    let router = router().await;
    let decrypting = APP
        .replace(
            r#""__e2ee_eq_title": "bytes" }"#,
            r#""__e2ee_eq_title": "bytes", "notes": "bytes", "notesText": "string?" }"#,
        )
        .replace(
            r#""columns": { "title": "string" }"#,
            r#""columns": { "title": "string", "notes": { "type": "string", "to": "notesText" } }"#,
        );
    let app = artifact::load(
        decrypting.as_bytes(),
        permissions(MEMBERS_RULE, "ownerId = session.user").as_bytes(),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    for confirmed in [json!([]), json!(["tasks.title"])] {
        let mut body = app.deployment_body(false);
        body["confirmDecrypt"] = confirmed.clone();
        let (status, refusal) = publish(&router, body).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{confirmed}");
        assert!(
            refusal["error"].as_str().unwrap().contains("tasks.notes"),
            "{refusal}"
        );
    }
    let mut body = app.deployment_body(false);
    body["confirmDecrypt"] = json!(["tasks.title", "tasks.notes"]);
    let (status, body) = publish(&router, body).await;
    assert_eq!(status, StatusCode::OK, "{body}");
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
/// column without a conversion would make the old cells unreadable, so the
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

/// An encrypted schema as a deploy body: tasks encrypt their title in their
/// project's space.
fn encrypted_body() -> Value {
    use jazz::tools::deployment::artifact::DeclaredEncryption;
    use jazz::tools::deployment::artifact::system_tables::with_e2ee_fragment;
    use jazz::tools::public_schema::{EncryptedColumn, EncryptionEntry, PlainType, SweepBy};
    use std::collections::BTreeMap;
    let schema = with_e2ee_fragment(
        &SchemaBuilder::new()
            .table(TableSchema::builder("projects").column("ownerId", ColumnType::Uuid))
            .table(
                TableSchema::builder("tasks")
                    .fk_column("projectId", "projects")
                    .column("title", ColumnType::Bytea),
            )
            .build(),
    );
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
                    },
                    sealed_type: None,
                    from: None,
                    to: None,
                    convert: None,
                    reverse: None,
                },
            )]),
            equality: Vec::new(),
            backfill: Vec::new(),
            rehome: None,
            sweep: SweepBy::Updaters,
        },
    )]);
    let declared = DeclaredEncryption {
        settings: Default::default(),
        versions: BTreeMap::from([(hash.clone(), entries)]),
    };
    json!({
        "targetSchemaHash": hash,
        "schemas": [{ "hash": hash, "schema": schema }],
        "migrations": [],
        "permissions": {},
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

/// An encrypting app serves local-first accounts founded for its own app id
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
async fn an_encrypting_app_founds_local_first_accounts_for_its_own_audience_only() {
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
async fn an_encrypting_app_admits_a_local_first_founder_of_its_own_audience_only() {
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
