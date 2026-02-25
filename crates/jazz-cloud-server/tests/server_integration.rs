use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use base64::Engine;
use jazz_tools::query_manager::session::Session;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use tempfile::TempDir;
use uuid::Uuid;

const INTERNAL_API_SECRET: &str = "integration-internal-secret";
const SECRET_HASH_KEY: &str = "integration-secret-hash-key";
const MANAGEMENT_PASSWORD: &str = "integration-management-password";

#[derive(Debug, Deserialize)]
struct AppSummaryResponse {
    app_id: String,
    app_name: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct CreateAppResponse {
    app_id: String,
    app_name: String,
    jwks_endpoint: String,
    backend_secret: String,
    admin_secret: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct UpdateAppResponse {
    app_id: String,
    status: String,
    backend_secret: Option<String>,
    admin_secret: Option<String>,
}

struct ServerProcess {
    process: Child,
    port: u16,
    client: Client,
}

impl ServerProcess {
    async fn start(data_root: &Path) -> Self {
        Self::start_with_management_password(data_root, None).await
    }

    async fn start_with_management_password(data_root: &Path, password: Option<&str>) -> Self {
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

        if let Some(password) = password {
            cmd.env("JAZZ_MANAGEMENT_PASSWORD", password);
        }

        let process = cmd.spawn().expect("spawn jazz-cloud-server");

        let server = Self {
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

    async fn wait_ready(&self) {
        let health_url = format!("{}/health", self.base_url());
        for _ in 0..60 {
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

fn sync_body() -> Value {
    json!({
        "client_id": "01234567-89ab-cdef-0123-456789abcdef",
        "payload": {
            "ObjectUpdated": {
                "object_id": "01234567-89ab-cdef-0123-456789abcdef",
                "metadata": null,
                "branch_name": "main",
                "commits": []
            }
        }
    })
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
async fn management_routes_are_disabled_when_password_not_set() {
    let temp_dir = TempDir::new().expect("temp dir");
    let server = ServerProcess::start(temp_dir.path()).await;

    let response = server
        .client
        .get(format!("{}/manage", server.base_url()))
        .send()
        .await
        .expect("management page request");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn management_routes_require_valid_basic_auth() {
    let temp_dir = TempDir::new().expect("temp dir");
    let server =
        ServerProcess::start_with_management_password(temp_dir.path(), Some(MANAGEMENT_PASSWORD))
            .await;
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
            basic_auth_header("admin", MANAGEMENT_PASSWORD),
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
    let server =
        ServerProcess::start_with_management_password(temp_dir.path(), Some(MANAGEMENT_PASSWORD))
            .await;
    let auth_header = basic_auth_header("admin", MANAGEMENT_PASSWORD);

    let create_response = server
        .client
        .post(format!("{}/manage/api/apps", server.base_url()))
        .header("Authorization", &auth_header)
        .json(&json!({
            "app_name": "managed-app",
            "jwks_endpoint": "http://example.invalid/jwks",
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
    assert_eq!(created.status, "active");

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
