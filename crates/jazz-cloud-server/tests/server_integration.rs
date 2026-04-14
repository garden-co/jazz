use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use base64::Engine;
use jazz_tools::commit::CommitId;
use jazz_tools::metadata::{MetadataKey, ObjectType};
use jazz_tools::query_manager::session::Session;
use jazz_tools::query_manager::types::{ColumnType, SchemaBuilder, SchemaHash, TableSchema};
use jazz_tools::row_histories::{RowState, StoredRowVersion};
use jazz_tools::schema_manager::encode_schema;
use jazz_tools::sync_manager::{ClientId, SyncPayload};
use jazz_tools::transport_protocol::SyncBatchRequest;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use tempfile::TempDir;
use uuid::Uuid;

const INTERNAL_API_SECRET: &str = "integration-internal-secret";
const SECRET_HASH_KEY: &str = "integration-secret-hash-key";

#[derive(Debug, Deserialize)]
struct AppSummaryResponse {
    app_id: String,
    app_name: String,
    jwks_endpoint: String,
    jwks_cache_ttl_secs: u64,
    jwks_max_stale_secs: u64,
    allow_anonymous: bool,
    allow_demo: bool,
    status: String,
}

#[derive(Debug, Deserialize)]
struct CreateAppResponse {
    app_id: String,
    app_name: String,
    jwks_endpoint: String,
    jwks_cache_ttl_secs: u64,
    jwks_max_stale_secs: u64,
    backend_secret: String,
    admin_secret: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct UpdateAppResponse {
    app_id: String,
    jwks_cache_ttl_secs: u64,
    jwks_max_stale_secs: u64,
    status: String,
    backend_secret: Option<String>,
    admin_secret: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ManageAdminSecretResponse {
    app_id: String,
    admin_secret: Option<String>,
}

struct ServerProcess {
    process: Child,
    port: u16,
    client: Client,
}

impl ServerProcess {
    async fn start(data_root: &Path) -> Self {
        let port = get_free_port();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_jazz-cloud-server"));
        cmd.args([
            "--port",
            &port.to_string(),
            "--data-root",
            data_root.to_str().expect("data root path"),
            "--internal-api-secret",
            INTERNAL_API_SECRET,
            "--secret-hash-key",
            SECRET_HASH_KEY,
            "--worker-threads",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());

        let process = cmd.spawn().expect("spawn jazz-cloud-server");

        let mut server = Self {
            process,
            port,
            client: Client::new(),
        };
        server.wait_ready().await;
        server
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    async fn wait_ready(&mut self) {
        let health_url = format!("{}/health", self.base_url());
        for _ in 0..200 {
            if let Some(status) = self.process.try_wait().expect("poll jazz-cloud-server") {
                panic!("jazz-cloud-server exited before becoming ready: {status}");
            }
            if let Ok(response) = self.client.get(&health_url).send().await
                && response.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("jazz-cloud-server did not become ready in time");
    }

    async fn create_app(
        &self,
        app_name: &str,
        jwks_endpoint: &str,
        backend_secret: Option<&str>,
        admin_secret: Option<&str>,
    ) -> CreateAppResponse {
        let mut payload = json!({
            "app_name": app_name,
            "jwks_endpoint": jwks_endpoint,
        });

        if let Some(secret) = backend_secret {
            payload["backend_secret"] = Value::String(secret.to_string());
        }
        if let Some(secret) = admin_secret {
            payload["admin_secret"] = Value::String(secret.to_string());
        }

        let response = self
            .client
            .post(format!("{}/internal/apps", self.base_url()))
            .header("X-Jazz-Internal-Secret", INTERNAL_API_SECRET)
            .json(&payload)
            .send()
            .await
            .expect("create app request");

        let status = response.status();
        let body = response.text().await.expect("create app body");
        assert!(
            status.is_success(),
            "create app failed: status={status}, body={body}"
        );
        serde_json::from_str(&body).expect("create app response json")
    }

    async fn list_apps(&self) -> Vec<AppSummaryResponse> {
        let response = self
            .client
            .get(format!("{}/internal/apps", self.base_url()))
            .header("X-Jazz-Internal-Secret", INTERNAL_API_SECRET)
            .send()
            .await
            .expect("list apps request");
        let status = response.status();
        let body = response.text().await.expect("list apps body");
        assert!(
            status.is_success(),
            "list apps failed: status={status}, body={body}"
        );
        serde_json::from_str(&body).expect("list apps response json")
    }

    async fn get_app(&self, app_id: &str) -> reqwest::Response {
        self.client
            .get(format!("{}/internal/apps/{app_id}", self.base_url()))
            .header("X-Jazz-Internal-Secret", INTERNAL_API_SECRET)
            .send()
            .await
            .expect("get app request")
    }

    async fn update_app(&self, app_id: &str, payload: Value) -> UpdateAppResponse {
        let response = self
            .client
            .patch(format!("{}/internal/apps/{app_id}", self.base_url()))
            .header("X-Jazz-Internal-Secret", INTERNAL_API_SECRET)
            .json(&payload)
            .send()
            .await
            .expect("update app request");
        let status = response.status();
        let body = response.text().await.expect("update app body");
        assert!(
            status.is_success(),
            "update app failed: status={status}, body={body}"
        );
        serde_json::from_str(&body).expect("update app response json")
    }

    async fn sync_with_backend_session(
        &self,
        app_id: &str,
        backend_secret: &str,
        user_id: &str,
    ) -> reqwest::Response {
        self.client
            .post(format!("{}/apps/{app_id}/sync", self.base_url()))
            .header("X-Jazz-Backend-Secret", backend_secret)
            .header("X-Jazz-Session", encode_session(user_id))
            .json(&sync_body())
            .send()
            .await
            .expect("sync request")
    }

    async fn sync_with_admin_payload(
        &self,
        app_id: &str,
        admin_secret: &str,
        payload: Value,
    ) -> reqwest::Response {
        self.client
            .post(format!("{}/apps/{app_id}/sync", self.base_url()))
            .header("X-Jazz-Admin-Secret", admin_secret)
            .json(&payload)
            .send()
            .await
            .expect("admin sync request")
    }

    async fn get_schema_by_hash(
        &self,
        app_id: &str,
        admin_secret: &str,
        schema_hash: &str,
    ) -> reqwest::Response {
        self.client
            .get(format!(
                "{}/apps/{app_id}/schema/{schema_hash}",
                self.base_url()
            ))
            .header("X-Jazz-Admin-Secret", admin_secret)
            .send()
            .await
            .expect("schema fetch request")
    }

    async fn get_schema_hashes(&self, app_id: &str, admin_secret: &str) -> reqwest::Response {
        self.client
            .get(format!("{}/apps/{app_id}/schemas", self.base_url()))
            .header("X-Jazz-Admin-Secret", admin_secret)
            .send()
            .await
            .expect("schema hashes fetch request")
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn encode_session(user_id: &str) -> String {
    let session = Session::new(user_id);
    let json = serde_json::to_string(&session).expect("serialize session");
    base64::engine::general_purpose::STANDARD.encode(json.as_bytes())
}

fn basic_auth_header(username: &str, password: &str) -> String {
    let raw = format!("{username}:{password}");
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(raw.as_bytes())
    )
}

fn sync_body() -> SyncBatchRequest {
    let row_id = jazz_tools::ObjectId::new();
    let row = StoredRowVersion::new(
        row_id,
        "main",
        Vec::<CommitId>::new(),
        b"alice".to_vec(),
        jazz_tools::metadata::RowProvenance::for_insert(row_id.to_string(), 1_000),
        Default::default(),
        RowState::VisibleDirect,
        None,
    );

    SyncBatchRequest {
        payloads: vec![SyncPayload::RowBatchCreated {
            metadata: None,
            row,
        }],
        client_id: ClientId::new(),
    }
}

fn get_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind port 0");
    listener.local_addr().expect("local addr").port()
}

#[tokio::test]
async fn internal_api_secret_is_required_for_provisioning_routes() {
    let temp_dir = TempDir::new().expect("temp dir");
    let server = ServerProcess::start(temp_dir.path()).await;

    let response = server
        .client
        .post(format!("{}/internal/apps", server.base_url()))
        .json(&json!({
            "app_name": "missing-secret",
            "jwks_endpoint": "http://example.invalid/jwks",
        }))
        .send()
        .await
        .expect("request without secret");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn backend_session_auth_works_and_secret_rotation_is_enforced() {
    let temp_dir = TempDir::new().expect("temp dir");
    let server = ServerProcess::start(temp_dir.path()).await;

    let created = server
        .create_app(
            "lifecycle-app",
            "http://example.invalid/jwks",
            Some("backend-v1"),
            Some("admin-v1"),
        )
        .await;

    assert_eq!(created.app_name, "lifecycle-app");
    assert_eq!(created.jwks_endpoint, "http://example.invalid/jwks");
    assert_eq!(created.jwks_cache_ttl_secs, 300);
    assert_eq!(created.jwks_max_stale_secs, 300);
    assert_eq!(created.backend_secret, "backend-v1");
    assert_eq!(created.admin_secret, "admin-v1");
    assert_eq!(created.status, "active");

    let listed = server.list_apps().await;
    assert!(
        listed.iter().any(|app| app.app_id == created.app_id),
        "newly created app should appear in app listing"
    );

    let fetched = server.get_app(&created.app_id).await;
    assert_eq!(fetched.status(), StatusCode::OK);
    let summary: AppSummaryResponse = fetched.json().await.expect("summary json");
    assert_eq!(summary.app_id, created.app_id);
    assert_eq!(summary.app_name, "lifecycle-app");
    assert_eq!(summary.jwks_cache_ttl_secs, 300);
    assert_eq!(summary.jwks_max_stale_secs, 300);
    assert_eq!(summary.status, "active");

    let sync_ok = server
        .sync_with_backend_session(&created.app_id, "backend-v1", "backend-user")
        .await;
    assert_ne!(
        sync_ok.status(),
        StatusCode::UNAUTHORIZED,
        "backend session auth should be accepted before rotation"
    );

    let updated = server
        .update_app(
            &created.app_id,
            json!({
                "status": "disabled",
                "rotate_backend_secret": true,
                "rotate_admin_secret": true,
            }),
        )
        .await;

    assert_eq!(updated.status, "disabled");
    assert_eq!(updated.app_id, created.app_id);
    let new_backend = updated
        .backend_secret
        .expect("rotating backend secret should return new secret");
    let new_admin = updated
        .admin_secret
        .expect("rotating admin secret should return new secret");
    assert_ne!(new_backend, "backend-v1");
    assert_ne!(new_admin, "admin-v1");

    let disabled_sync = server
        .sync_with_backend_session(&created.app_id, &new_backend, "backend-user")
        .await;
    assert_eq!(
        disabled_sync.status(),
        StatusCode::FORBIDDEN,
        "disabled app should reject sync traffic"
    );

    let reenabled = server
        .update_app(&created.app_id, json!({ "status": "active" }))
        .await;
    assert_eq!(reenabled.status, "active");

    let old_secret_sync = server
        .sync_with_backend_session(&created.app_id, "backend-v1", "backend-user")
        .await;
    assert_eq!(
        old_secret_sync.status(),
        StatusCode::UNAUTHORIZED,
        "old backend secret should be rejected after rotation"
    );

    let new_secret_sync = server
        .sync_with_backend_session(&created.app_id, &new_backend, "backend-user")
        .await;
    assert_ne!(
        new_secret_sync.status(),
        StatusCode::UNAUTHORIZED,
        "new backend secret should be accepted after rotation"
    );
}

#[tokio::test]
async fn unknown_and_invalid_app_ids_return_expected_statuses() {
    let temp_dir = TempDir::new().expect("temp dir");
    let server = ServerProcess::start(temp_dir.path()).await;

    let unknown_app_id = Uuid::new_v4().to_string();
    let unknown_sync = server
        .sync_with_backend_session(&unknown_app_id, "unused-secret", "backend-user")
        .await;
    assert_eq!(unknown_sync.status(), StatusCode::NOT_FOUND);

    let invalid_get = server.get_app("not-a-uuid").await;
    assert_eq!(invalid_get.status(), StatusCode::BAD_REQUEST);

    let unknown_get = server.get_app(&unknown_app_id).await;
    assert_eq!(unknown_get.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn app_registry_and_auth_survive_server_restart() {
    let temp_dir = TempDir::new().expect("temp dir");
    let data_root = temp_dir.path().to_path_buf();

    let app_id = {
        let server = ServerProcess::start(&data_root).await;
        let created = server
            .create_app(
                "persistent-app",
                "http://example.invalid/jwks",
                Some("backend-persist"),
                Some("admin-persist"),
            )
            .await;

        let sync_ok = server
            .sync_with_backend_session(&created.app_id, "backend-persist", "backend-user")
            .await;
        assert_ne!(sync_ok.status(), StatusCode::UNAUTHORIZED);

        created.app_id
    };

    let restarted = ServerProcess::start(&data_root).await;
    let apps = restarted.list_apps().await;
    assert!(
        apps.iter().any(|app| app.app_id == app_id),
        "meta app registry should survive server restart"
    );

    let sync_after_restart = restarted
        .sync_with_backend_session(&app_id, "backend-persist", "backend-user")
        .await;
    assert_ne!(
        sync_after_restart.status(),
        StatusCode::UNAUTHORIZED,
        "backend secret auth should still work after restart"
    );
}

#[tokio::test]
async fn management_routes_are_enabled_and_require_basic_auth() {
    let temp_dir = TempDir::new().expect("temp dir");
    let server = ServerProcess::start(temp_dir.path()).await;

    let response = server
        .client
        .get(format!("{}/manage", server.base_url()))
        .send()
        .await
        .expect("management page request");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn management_routes_require_valid_basic_auth() {
    let temp_dir = TempDir::new().expect("temp dir");
    let server = ServerProcess::start(temp_dir.path()).await;
    let url = format!("{}/manage", server.base_url());

    let missing_auth = server
        .client
        .get(&url)
        .send()
        .await
        .expect("missing auth request");
    assert_eq!(missing_auth.status(), StatusCode::UNAUTHORIZED);
    let challenge = missing_auth
        .headers()
        .get("WWW-Authenticate")
        .and_then(|v| v.to_str().ok())
        .expect("missing WWW-Authenticate challenge header");
    assert!(
        challenge.starts_with("Basic "),
        "expected basic challenge, got: {challenge}"
    );

    let wrong_auth = server
        .client
        .get(&url)
        .header(
            "Authorization",
            basic_auth_header("admin", "wrong-password"),
        )
        .send()
        .await
        .expect("wrong auth request");
    assert_eq!(wrong_auth.status(), StatusCode::UNAUTHORIZED);

    let ok_auth = server
        .client
        .get(&url)
        .header(
            "Authorization",
            basic_auth_header("admin", INTERNAL_API_SECRET),
        )
        .send()
        .await
        .expect("valid auth request");
    assert_eq!(ok_auth.status(), StatusCode::OK);
    let body = ok_auth.text().await.expect("management page body");
    assert!(body.contains("Jazz Cloud Server Management"));
}

#[tokio::test]
async fn management_api_create_list_and_status_update_work() {
    let temp_dir = TempDir::new().expect("temp dir");
    let server = ServerProcess::start(temp_dir.path()).await;
    let auth_header = basic_auth_header("admin", INTERNAL_API_SECRET);

    let create_response = server
        .client
        .post(format!("{}/manage/api/apps", server.base_url()))
        .header("Authorization", &auth_header)
        .json(&json!({
            "app_name": "managed-app",
            "jwks_endpoint": "http://example.invalid/jwks",
            "jwks_cache_ttl_secs": 90,
            "jwks_max_stale_secs": 30,
            "allow_anonymous": false,
            "allow_demo": true,
            "backend_secret": "managed-backend-secret",
            "admin_secret": "managed-admin-secret"
        }))
        .send()
        .await
        .expect("manage create app request");
    assert_eq!(create_response.status(), StatusCode::OK);
    let created: CreateAppResponse = create_response
        .json()
        .await
        .expect("parse manage create app response");
    assert_eq!(created.app_name, "managed-app");
    assert_eq!(created.jwks_cache_ttl_secs, 90);
    assert_eq!(created.jwks_max_stale_secs, 30);
    assert_eq!(created.status, "active");

    let reveal_response = server
        .client
        .get(format!(
            "{}/manage/api/apps/{}/admin-secret",
            server.base_url(),
            created.app_id
        ))
        .header("Authorization", &auth_header)
        .send()
        .await
        .expect("manage reveal admin secret request");
    assert_eq!(reveal_response.status(), StatusCode::OK);
    let revealed: ManageAdminSecretResponse = reveal_response
        .json()
        .await
        .expect("parse revealed admin secret response");
    assert_eq!(revealed.app_id, created.app_id);
    assert_eq!(
        revealed.admin_secret.as_deref(),
        Some("managed-admin-secret")
    );

    let auth_update_response = server
        .client
        .patch(format!(
            "{}/manage/api/apps/{}/auth",
            server.base_url(),
            created.app_id
        ))
        .header("Authorization", &auth_header)
        .json(&json!({
            "allow_anonymous": true,
            "allow_demo": false,
            "jwks_endpoint": "",
            "jwks_cache_ttl_secs": 10,
            "jwks_max_stale_secs": 5
        }))
        .send()
        .await
        .expect("manage update auth request");
    assert_eq!(auth_update_response.status(), StatusCode::OK);
    let auth_updated: UpdateAppResponse = auth_update_response
        .json()
        .await
        .expect("parse auth update response");
    assert_eq!(auth_updated.app_id, created.app_id);
    assert_eq!(auth_updated.jwks_cache_ttl_secs, 10);
    assert_eq!(auth_updated.jwks_max_stale_secs, 5);
    assert_eq!(auth_updated.status, "active");
    assert!(auth_updated.backend_secret.is_none());
    assert!(auth_updated.admin_secret.is_none());

    let list_response = server
        .client
        .get(format!("{}/manage/api/apps", server.base_url()))
        .header("Authorization", &auth_header)
        .send()
        .await
        .expect("manage list apps request");
    assert_eq!(list_response.status(), StatusCode::OK);
    let listed: Vec<AppSummaryResponse> = list_response.json().await.expect("parse app list");
    let listed_created = listed
        .iter()
        .find(|app| app.app_id == created.app_id)
        .expect("created app should exist in management list");
    assert_eq!(listed_created.jwks_endpoint, "");
    assert_eq!(listed_created.jwks_cache_ttl_secs, 10);
    assert_eq!(listed_created.jwks_max_stale_secs, 5);
    assert!(listed_created.allow_anonymous);
    assert!(!listed_created.allow_demo);
    assert_eq!(listed_created.status, "active");

    let update_response = server
        .client
        .post(format!(
            "{}/manage/api/apps/{}/status",
            server.base_url(),
            created.app_id
        ))
        .header("Authorization", &auth_header)
        .json(&json!({ "status": "disabled" }))
        .send()
        .await
        .expect("manage update status request");
    assert_eq!(update_response.status(), StatusCode::OK);
    let updated: UpdateAppResponse = update_response
        .json()
        .await
        .expect("parse update status response");
    assert_eq!(updated.status, "disabled");

    let rotate_response = server
        .client
        .post(format!(
            "{}/manage/api/apps/{}/admin-secret/rotate",
            server.base_url(),
            created.app_id
        ))
        .header("Authorization", &auth_header)
        .send()
        .await
        .expect("manage rotate admin secret request");
    assert_eq!(rotate_response.status(), StatusCode::OK);
    let rotated: UpdateAppResponse = rotate_response
        .json()
        .await
        .expect("parse rotate admin secret response");
    let rotated_secret = rotated
        .admin_secret
        .expect("rotating admin secret should return secret value");
    assert_ne!(rotated_secret, "managed-admin-secret");

    let reveal_after_rotate_response = server
        .client
        .get(format!(
            "{}/manage/api/apps/{}/admin-secret",
            server.base_url(),
            created.app_id
        ))
        .header("Authorization", &auth_header)
        .send()
        .await
        .expect("manage reveal after rotate request");
    assert_eq!(reveal_after_rotate_response.status(), StatusCode::OK);
    let revealed_after_rotate: ManageAdminSecretResponse = reveal_after_rotate_response
        .json()
        .await
        .expect("parse reveal after rotate response");
    assert_eq!(
        revealed_after_rotate.admin_secret.as_deref(),
        Some(rotated_secret.as_str())
    );

    let listed_after_update = server
        .client
        .get(format!("{}/manage/api/apps", server.base_url()))
        .header("Authorization", &auth_header)
        .send()
        .await
        .expect("manage list after update request");
    assert_eq!(listed_after_update.status(), StatusCode::OK);
    let listed_after_update: Vec<AppSummaryResponse> = listed_after_update
        .json()
        .await
        .expect("parse app list after update");
    let updated_created = listed_after_update
        .iter()
        .find(|app| app.app_id == created.app_id)
        .expect("updated app should exist in management list");
    assert_eq!(updated_created.status, "disabled");
}

#[tokio::test]
async fn schema_catalogue_sync_and_retrieval_round_trip() {
    let temp_dir = TempDir::new().expect("temp dir");
    let server = ServerProcess::start(temp_dir.path()).await;

    let created = server
        .create_app(
            "schema-catalogue-app",
            "http://example.invalid/jwks",
            Some("backend-secret"),
            Some("admin-secret"),
        )
        .await;

    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let schema_hash = SchemaHash::compute(&schema);
    let encoded_schema = encode_schema(&schema);
    let object_id = schema_hash.to_object_id().to_string();
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        MetadataKey::Type.as_str().to_string(),
        ObjectType::CatalogueSchema.as_str().to_string(),
    );
    metadata.insert(
        MetadataKey::AppId.as_str().to_string(),
        created.app_id.clone(),
    );
    metadata.insert(
        MetadataKey::SchemaHash.as_str().to_string(),
        hex::encode(schema_hash.as_bytes()),
    );

    let sync_payload = json!({
        "client_id": Uuid::new_v4().to_string(),
        "payloads": [{
            "CatalogueEntryUpdated": {
                "entry": {
                "object_id": object_id,
                "metadata": metadata,
                "content": encoded_schema
                }
            }
        }]
    });

    let sync_response = server
        .sync_with_admin_payload(&created.app_id, "admin-secret", sync_payload)
        .await;
    assert_eq!(
        sync_response.status(),
        StatusCode::OK,
        "admin catalogue sync should succeed"
    );

    let schema_hashes_response = server
        .get_schema_hashes(&created.app_id, "admin-secret")
        .await;
    assert_eq!(schema_hashes_response.status(), StatusCode::OK);
    let schema_hashes_json: Value = schema_hashes_response
        .json()
        .await
        .expect("schema hashes json");
    let expected_hash = schema_hash.to_string();
    assert!(
        schema_hashes_json["hashes"]
            .as_array()
            .is_some_and(|hashes| hashes
                .iter()
                .any(|hash| hash.as_str() == Some(&expected_hash)))
    );

    let schema_response = server
        .get_schema_by_hash(&created.app_id, "admin-secret", &expected_hash)
        .await;
    assert_eq!(schema_response.status(), StatusCode::OK);

    let schema_json: Value = schema_response.json().await.expect("schema json");
    let expected_schema_json = serde_json::to_value(schema.clone()).expect("expected schema json");
    assert_eq!(schema_json, expected_schema_json);
}
