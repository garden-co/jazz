//! Deploying through the real HTTP router a schema that holds tables with
//! Jazz's reserved `__e2ee_` prefix: the server accepts only Jazz's own
//! system tables, whatever the backend's loader did.
//!
//! ```text
//! raw deploy body ──POST /bundles──► server: system tables checked ──► refused
//! ```
use super::super::{ServerBuilder, StorageBackend, routes::create_router};
use crate::middleware::AuthConfig;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use jazz::tools::AppId;
use jazz::tools::public_schema::{ColumnType, SchemaBuilder, SchemaHash, TableSchema};
use serde_json::{Value, json};
use tower::ServiceExt;

fn app_id() -> AppId {
    AppId::from_name("system-table-deployment-tests")
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
