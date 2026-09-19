//! Short-lived, app-scoped Inspector authority. Never shares account-JWT keys.
use crate::server::ServerState;
use axum::{
    Json,
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub(super) const READ: &str = "inspector:read";
const EDIT: &str = "inspector:edit";
const ADMIN: &str = "inspector:admin";
const TTL: u64 = 900;
const AUDIENCE: &str = "jazz-inspector-v1";

pub(super) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Claims {
    iss: String,
    aud: String,
    app: String,
    sub: String,
    iat: u64,
    pub(super) exp: u64,
    capabilities: Vec<String>,
}
impl Claims {
    pub(super) fn allows(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|value| value == capability)
    }
    pub(super) fn edit(&self) -> bool {
        self.allows(EDIT)
    }
    pub(super) fn operator_key(&self) -> [u8; 32] {
        Sha256::digest(self.sub.as_bytes()).into()
    }
}

fn issuer(state: &ServerState) -> String {
    format!("jazz-inspector-v1:{}", state.app_id)
}
fn key(state: &ServerState) -> Result<Vec<u8>, ()> {
    let secret = state.auth_config.admin_secret.as_deref().ok_or(())?;
    let mut hash = Sha256::new();
    hash.update(b"jazz-inspector-signing-key-v1\0");
    hash.update(secret.as_bytes());
    Ok(hash.finalize().to_vec())
}
fn valid_capabilities(capabilities: &[String]) -> bool {
    capabilities.iter().any(|value| value == READ)
        && capabilities.len() <= 3
        && capabilities
            .iter()
            .all(|value| [READ, EDIT, ADMIN].contains(&value.as_str()))
        && capabilities
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == capabilities.len()
}

pub(super) fn verify(token: &str, state: &ServerState) -> Result<Claims, ()> {
    if token.len() > 8192 {
        return Err(());
    }
    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 0;
    validation.set_audience(&[AUDIENCE]);
    validation.set_issuer(&[issuer(state)]);
    let claims =
        jsonwebtoken::decode::<Claims>(token, &DecodingKey::from_secret(&key(state)?), &validation)
            .map_err(|_| ())?
            .claims;
    let now = now();
    if claims.app != state.app_id.to_string()
        || claims.sub.is_empty()
        || claims.sub.len() > 256
        || claims.iat > now
        || claims.exp <= now
        || claims.exp.saturating_sub(claims.iat) > TTL
        || !valid_capabilities(&claims.capabilities)
    {
        return Err(());
    }
    Ok(claims)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Exchange {
    operator: String,
    #[serde(default = "default_capabilities")]
    capabilities: Vec<String>,
    #[serde(default = "default_ttl")]
    expires_in: u64,
}
fn default_capabilities() -> Vec<String> {
    vec![READ.to_owned()]
}
fn default_ttl() -> u64 {
    TTL
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Session {
    access_token: String,
    expires_at: u64,
    app_id: String,
    capabilities: Vec<String>,
}
fn failure(status: StatusCode, error: &str) -> Response {
    (
        status,
        [("cache-control", "no-store")],
        Json(serde_json::json!({"error":error})),
    )
        .into_response()
}

pub(super) async fn exchange(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    request: Result<Json<Exchange>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if crate::middleware::auth::validate_admin_secret(
        headers
            .get("x-jazz-admin-secret")
            .and_then(|value| value.to_str().ok()),
        &state.auth_config,
    )
    .is_err()
    {
        return failure(StatusCode::UNAUTHORIZED, "invalid_admin_credential");
    }
    let Ok(Json(request)) = request else {
        return failure(StatusCode::BAD_REQUEST, "invalid_request");
    };
    if request.operator.is_empty()
        || request.operator.len() > 256
        || request.operator.chars().any(char::is_control)
        || !valid_capabilities(&request.capabilities)
        || request.expires_in == 0
        || request.expires_in > TTL
    {
        return failure(StatusCode::BAD_REQUEST, "invalid_request");
    }
    let issued = now();
    let claims = Claims {
        iss: issuer(&state),
        aud: AUDIENCE.to_owned(),
        app: state.app_id.to_string(),
        sub: request.operator,
        iat: issued,
        exp: issued + request.expires_in,
        capabilities: request.capabilities,
    };
    let Ok(key) = key(&state) else {
        return failure(StatusCode::UNAUTHORIZED, "invalid_admin_credential");
    };
    let Ok(token) = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(&key),
    ) else {
        return failure(StatusCode::INTERNAL_SERVER_ERROR, "exchange_failed");
    };
    (
        [("cache-control", "no-store")],
        Json(Session {
            access_token: token,
            expires_at: claims.exp,
            app_id: claims.app,
            capabilities: claims.capabilities,
        }),
    )
        .into_response()
}

/// Adapt only an explicit catalogue-operation allowlist to existing handlers.
/// Root authority is server-local after token validation; it is never returned
/// to the client. Account administration and exchange cannot pass this gate.
pub(super) async fn authorize_http(
    State(state): State<Arc<ServerState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let Some(token) = request.headers().get("x-jazz-inspector-token") else {
        return next.run(request).await;
    };
    if request.headers().contains_key("x-jazz-admin-secret") {
        return failure(StatusCode::BAD_REQUEST, "ambiguous_credentials");
    }
    let Ok(claims) = token
        .to_str()
        .map_err(|_| ())
        .and_then(|token| verify(token, &state))
    else {
        return failure(StatusCode::UNAUTHORIZED, "invalid_inspector_credential");
    };
    let path = request.uri().path();
    let prefix = format!("/apps/{}/", state.app_id);
    let route = path
        .strip_prefix(&prefix)
        .unwrap_or(path.trim_start_matches('/'));
    let required = match (request.method().as_str(), route) {
        (
            "GET",
            "schemas"
            | "admin/permissions"
            | "admin/permissions/head"
            | "admin/schema-connectivity"
            | "admin/introspection/subscriptions",
        ) => READ,
        ("GET", route) if route.starts_with("schema/") && !route[7..].contains('/') => READ,
        ("POST", "admin/schemas" | "admin/permissions" | "admin/migrations") => ADMIN,
        _ => return failure(StatusCode::FORBIDDEN, "inspector_operation_denied"),
    };
    if !claims.allows(required) {
        return failure(StatusCode::FORBIDDEN, "inspector_capability_denied");
    }
    let Some(secret) = state
        .auth_config
        .admin_secret
        .as_ref()
        .and_then(|secret| secret.parse().ok())
    else {
        return failure(StatusCode::UNAUTHORIZED, "invalid_inspector_credential");
    };
    request.headers_mut().remove("x-jazz-inspector-token");
    request.headers_mut().insert("x-jazz-admin-secret", secret);
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AuthConfig,
        server::{ServerBuilder, StorageBackend},
    };
    use jazz::tools::{
        AppId,
        public_schema::{ColumnType, SchemaBuilder, TableSchema},
    };
    use tower::ServiceExt;
    async fn state(app: &str) -> Arc<ServerState> {
        ServerBuilder::new(AppId::from_name(app))
            .with_auth_config(AuthConfig {
                admin_secret: Some("inspector-test-root".into()),
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .with_schema(
                SchemaBuilder::new()
                    .table(TableSchema::builder("items").column("title", ColumnType::Text))
                    .build(),
            )
            .build()
            .await
            .unwrap()
            .state
    }
    async fn request(
        state: Arc<ServerState>,
        method: &str,
        route: &str,
        credential: Option<(&str, &str)>,
        body: serde_json::Value,
    ) -> Response {
        let mut request = Request::builder()
            .method(method)
            .uri(format!("/apps/{}/{}", state.app_id, route))
            .header("content-type", "application/json");
        if let Some((name, value)) = credential {
            request = request.header(name, value);
        }
        super::super::create_router(state)
            .oneshot(request.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap()
    }
    async fn session(state: Arc<ServerState>, capabilities: Vec<&str>) -> serde_json::Value {
        let response = request(
            state,
            "POST",
            "admin/inspector/sessions",
            Some(("x-jazz-admin-secret", "inspector-test-root")),
            serde_json::json!({"operator":"test-operator", "capabilities": capabilities}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "no-store");
        serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 16384)
                .await
                .unwrap(),
        )
        .unwrap()
    }
    #[tokio::test]
    async fn inspector_http_exchange_enforces_app_and_operation_capabilities() {
        let state = state("inspector-http").await;
        let issued = session(state.clone(), vec![READ]).await;
        let token = issued["accessToken"].as_str().unwrap();
        let read = request(
            state.clone(),
            "GET",
            "schemas",
            Some(("x-jazz-inspector-token", token)),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(read.status(), StatusCode::OK);
        for route in [
            "admin/schemas",
            "admin/permissions",
            "admin/migrations",
            "admin/accounts/resolve",
            "admin/inspector/sessions",
        ] {
            let response = request(
                state.clone(),
                "POST",
                route,
                Some(("x-jazz-inspector-token", token)),
                serde_json::json!({}),
            )
            .await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{route}");
        }
        let other = self::state("inspector-other").await;
        let cross_app = request(
            other,
            "GET",
            "schemas",
            Some(("x-jazz-inspector-token", token)),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(cross_app.status(), StatusCode::UNAUTHORIZED);
        let root_slot = request(
            state.clone(),
            "GET",
            "schemas",
            Some(("x-jazz-admin-secret", token)),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(root_slot.status(), StatusCode::UNAUTHORIZED);
        let no_root = request(
            state.clone(),
            "POST",
            "admin/inspector/sessions",
            None,
            serde_json::json!({"operator":"test"}),
        )
        .await;
        assert_eq!(no_root.status(), StatusCode::UNAUTHORIZED);
        let admin = session(state.clone(), vec![READ, ADMIN]).await;
        let allowed = request(
            state,
            "POST",
            "admin/schemas",
            Some((
                "x-jazz-inspector-token",
                admin["accessToken"].as_str().unwrap(),
            )),
            serde_json::json!({}),
        )
        .await;
        assert_eq!(
            allowed.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "explicit admin reaches schema validation"
        );
    }
    #[tokio::test]
    async fn inspector_exchange_rejects_invalid_scope_ttl_and_operator() {
        let state = state("inspector-invalid-exchange").await;
        for body in [
            serde_json::json!({"operator":"x", "expiresIn":"must-not-reflect-credential-material"}),
            serde_json::json!({"operator":"x", "capabilities":["account:admin"]}),
            serde_json::json!({"operator":"x", "capabilities":[EDIT]}),
            serde_json::json!({"operator":"x", "expiresIn":901}),
            serde_json::json!({"operator":"", "expiresIn":900}),
        ] {
            let response = request(
                state.clone(),
                "POST",
                "admin/inspector/sessions",
                Some(("x-jazz-admin-secret", "inspector-test-root")),
                body,
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                serde_json::json!({"error":"invalid_request"})
            );
        }
    }
    // Internal token mutation is necessary to exercise signed-but-wrong claims:
    // the public exchange deliberately cannot mint them. Admission remains HTTP.
    #[tokio::test]
    async fn inspector_http_rejects_signed_wrong_audience_scope_issuer_and_expiry() {
        let state = state("inspector-invalid-claims").await;
        for field in ["aud", "iss", "app", "exp", "capabilities"] {
            let mut claims = Claims {
                iss: issuer(&state),
                aud: AUDIENCE.into(),
                app: state.app_id.to_string(),
                sub: "test".into(),
                iat: now(),
                exp: now() + 900,
                capabilities: default_capabilities(),
            };
            match field {
                "aud" => claims.aud = "account-jwt".into(),
                "iss" => claims.iss = "another-issuer".into(),
                "app" => claims.app = AppId::random().to_string(),
                "exp" => claims.exp = now(),
                _ => claims.capabilities = vec![EDIT.into()],
            }
            let token = jsonwebtoken::encode(
                &Header::new(Algorithm::HS256),
                &claims,
                &EncodingKey::from_secret(&key(&state).unwrap()),
            )
            .unwrap();
            let response = request(
                state.clone(),
                "GET",
                "schemas",
                Some(("x-jazz-inspector-token", &token)),
                serde_json::Value::Null,
            )
            .await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{field}");
        }
    }
}
