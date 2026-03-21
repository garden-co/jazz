//! HTTP/SSE transport for server communication.

use crate::jazz_transport::SyncBatchRequest;
use crate::query_manager::session::Session;
use crate::sync_manager::{ClientId, SyncPayload};
use base64::Engine;
use reqwest::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};

use crate::{JazzError, Result};

/// Authentication configuration for server requests.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// JWT token for frontend authentication.
    pub jwt_token: Option<String>,
    /// Backend secret for session impersonation.
    pub backend_secret: Option<String>,
    /// Admin secret for schema/policy sync.
    pub admin_secret: Option<String>,
}

impl AuthConfig {
    /// Create auth config from an AppContext.
    pub fn from_context(context: &crate::AppContext) -> Self {
        Self {
            jwt_token: context.jwt_token.clone(),
            backend_secret: context.backend_secret.clone(),
            admin_secret: context.admin_secret.clone(),
        }
    }
}

/// Connection to a Jazz server.
pub struct ServerConnection {
    client: Client,
    base_url: String,
    route_prefix: String,
    auth: AuthConfig,
}

impl ServerConnection {
    /// Connect to a Jazz server.
    pub async fn connect(base_url: &str, auth: AuthConfig) -> Result<Self> {
        let client = Client::new();
        let (base_url, route_prefix) = split_base_url(base_url)?;

        // Test connection with health check
        let health_url = format!("{}/health", base_url);
        client
            .get(&health_url)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JazzError::Connection(e.to_string()))?;

        Ok(Self {
            client,
            base_url,
            route_prefix,
            auth,
        })
    }

    /// Build headers for a request.
    ///
    /// If a session is provided (for backend impersonation), includes:
    /// - `X-Jazz-Backend-Secret` header
    /// - `X-Jazz-Session` header (base64-encoded JSON)
    ///
    /// Otherwise, if JWT token is configured, includes:
    /// - `Authorization: Bearer <token>` header
    fn build_headers(&self, session: Option<&Session>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        // Priority 1: Backend impersonation
        if let (Some(session), Some(secret)) = (session, &self.auth.backend_secret) {
            if let Ok(secret_value) = HeaderValue::from_str(secret) {
                headers.insert("X-Jazz-Backend-Secret", secret_value);
            }
            if let Ok(session_json) = serde_json::to_string(session) {
                let session_b64 =
                    base64::engine::general_purpose::STANDARD.encode(session_json.as_bytes());
                if let Ok(session_value) = HeaderValue::from_str(&session_b64) {
                    headers.insert("X-Jazz-Session", session_value);
                }
            }
        }
        // Priority 2: Frontend JWT auth
        else if let Some(jwt) = &self.auth.jwt_token {
            let auth_value = format!("Bearer {}", jwt);
            if let Ok(header_value) = HeaderValue::from_str(&auth_value) {
                headers.insert(AUTHORIZATION, header_value);
            }
        }

        headers
    }

    /// Build headers for admin operations (catalogue sync).
    ///
    /// Includes admin secret AND session auth (JWT or backend) so the server
    /// can both promote the client to Admin and bind a session.
    fn build_admin_headers(&self) -> HeaderMap {
        let mut headers = self.build_headers(None);

        if let Some(secret) = &self.auth.admin_secret
            && let Ok(secret_value) = HeaderValue::from_str(secret)
        {
            headers.insert("X-Jazz-Admin-Secret", secret_value);
        }

        headers
    }

    /// Build auth headers for the binary streaming connection.
    ///
    /// Same auth as `build_headers` but without Content-Type.
    pub fn build_stream_headers(&self) -> HeaderMap {
        let mut headers = self.build_headers(None);
        headers.remove(CONTENT_TYPE);
        headers
    }

    /// Push a sync payload to the server.
    pub async fn push_sync(&self, payload: SyncPayload, client_id: ClientId) -> Result<()> {
        let url = self.endpoint_url("/sync");

        // Check if this is a catalogue object - use admin headers
        let headers = if is_catalogue_payload(&payload) {
            self.build_admin_headers()
        } else {
            self.build_headers(None)
        };

        let request = SyncBatchRequest {
            payloads: vec![payload],
            client_id,
        };

        self.client
            .post(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JazzError::Sync(e.to_string()))?;

        Ok(())
    }

    /// Get the events stream URL for the given client id.
    pub fn stream_url(&self, client_id: &str) -> String {
        format!("{}?client_id={}", self.endpoint_url("/events"), client_id)
    }

    fn endpoint_url(&self, endpoint: &str) -> String {
        format!("{}{}{}", self.base_url, self.route_prefix, endpoint)
    }
}

fn split_base_url(input: &str) -> Result<(String, String)> {
    let parsed = reqwest::Url::parse(input)
        .map_err(|e| JazzError::Connection(format!("invalid server URL '{input}': {e}")))?;

    let mut origin = parsed.clone();
    origin.set_path("");
    origin.set_query(None);
    origin.set_fragment(None);

    let base_url = origin.as_str().trim_end_matches('/').to_string();
    let route_prefix = normalize_route_prefix(parsed.path());

    Ok((base_url, route_prefix))
}

fn normalize_route_prefix(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return String::new();
    }

    let trimmed = trimmed.trim_end_matches('/');
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

/// Check if a sync payload is for a catalogue object.
fn is_catalogue_payload(payload: &SyncPayload) -> bool {
    match payload {
        SyncPayload::ObjectUpdated { metadata, .. } => {
            if let Some(meta) = metadata
                && let Some(type_str) = meta
                    .metadata
                    .get(crate::metadata::MetadataKey::Type.as_str())
            {
                return type_str == crate::metadata::ObjectType::CatalogueSchema.as_str()
                    || type_str == crate::metadata::ObjectType::CatalogueLens.as_str()
                    || type_str == crate::metadata::ObjectType::CataloguePermissions.as_str();
            }
            false
        }
        _ => false,
    }
}
