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
                    db.host_put_if_absent(super::DECLARATIONS, &v2_hash.0, &other)
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
/// its existing plaintext columns still require an encryption conversion.
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
        panic!("the legacy deployment must require a conversion");
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
                    "columns": { "title": { "type": "string", "to": "titleText" } } } } } },
            "v3": { "tables": { "notes": { "columns": { "titleText": "string?" } } } } },
        "migrations": [
            { "from": "v1", "to": "v2", "migrate": { "notes": { "titleText": { "add": "string?", "default": null } } } },
            { "from": "v2", "to": "v3", "migrate": { "notes": { "title": { "drop": "bytes", "backwardsDefault": "" } } } },
            { "from": "v3", "migrate": { "notes": { "title": { "renameFrom": "titleText" } } } }] }"#;
    let permissions = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user) and title = 'public'" } } }"#;
    let app =
        artifact::load(file.as_bytes(), permissions.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    let mut body = app.deployment_body(false);
    body["confirmDecrypt"] = json!(["notes.title"]);
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
                    db.host_sql(&mut |conn| {
                        conn.execute_batch(
                            "CREATE TABLE IF NOT EXISTS _jazz_host_e2ee_declarations \
                             (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID; \
                             INSERT OR REPLACE INTO _jazz_host_e2ee_declarations \
                             VALUES (x'01', x'02')",
                        )
                        .map_err(|error| {
                            jazz::db::Error::new(jazz::db::ErrorCode::Storage, error.to_string())
                        })?;
                        entered_tx.send(()).unwrap();
                        let _ = release_rx.recv();
                        Ok(())
                    })
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
    use jazz::tools::public_schema::{EncryptedColumn, EncryptionEntry, PlainType, SweepBy};
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
