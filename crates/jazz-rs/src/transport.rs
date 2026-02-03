//! HTTP/SSE transport for server communication.

#![allow(dead_code)]

use base64::Engine;
use groove::query_manager::session::Session;
use groove::sync_manager::{ClientId, SyncPayload};
use jazz_transport::{
    ConnectionId, CreateObjectRequest, CreateObjectResponse, DeleteObjectRequest,
    SubscribeRequest, SubscribeResponse, SyncPayloadRequest, UnsubscribeRequest,
    UpdateObjectRequest,
};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;

use crate::{JazzError, ObjectId, Result, Value};

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
    connection_id: Option<ConnectionId>,
    auth: AuthConfig,
}

impl ServerConnection {
    /// Connect to a Jazz server.
    pub async fn connect(base_url: &str, auth: AuthConfig) -> Result<Self> {
        let client = Client::new();

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
            base_url: base_url.to_string(),
            connection_id: None,
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
    fn build_admin_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        if let Some(secret) = &self.auth.admin_secret {
            if let Ok(secret_value) = HeaderValue::from_str(secret) {
                headers.insert("X-Jazz-Admin-Secret", secret_value);
            }
        }

        headers
    }

    /// Subscribe to a query on the server.
    ///
    /// Session context is determined by configured auth (JWT token or backend secret).
    pub async fn subscribe(&self, request: SubscribeRequest) -> Result<SubscribeResponse> {
        let url = format!("{}/sync/subscribe", self.base_url);
        let headers = self.build_headers(None);

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JazzError::Sync(e.to_string()))?;

        let result: SubscribeResponse = response.json().await?;
        Ok(result)
    }

    /// Subscribe with explicit session (for backend impersonation).
    pub async fn subscribe_with_session(
        &self,
        request: SubscribeRequest,
        session: &Session,
    ) -> Result<SubscribeResponse> {
        let url = format!("{}/sync/subscribe", self.base_url);
        let headers = self.build_headers(Some(session));

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JazzError::Sync(e.to_string()))?;

        let result: SubscribeResponse = response.json().await?;
        Ok(result)
    }

    /// Unsubscribe from a query.
    pub async fn unsubscribe(&self, request: UnsubscribeRequest) -> Result<()> {
        let url = format!("{}/sync/unsubscribe", self.base_url);
        let headers = self.build_headers(None);

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

    /// Create an object on the server.
    ///
    /// Session context is determined by configured auth (JWT token or backend secret).
    pub async fn create_object(
        &self,
        request: CreateObjectRequest,
    ) -> Result<CreateObjectResponse> {
        let url = format!("{}/sync/object", self.base_url);
        let headers = self.build_headers(None);

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JazzError::Sync(e.to_string()))?;

        let result: CreateObjectResponse = response.json().await?;
        Ok(result)
    }

    /// Create an object with explicit session (for backend impersonation).
    pub async fn create_object_with_session(
        &self,
        table: &str,
        values: Vec<Value>,
        session: &Session,
        schema_context: groove::schema_manager::QuerySchemaContext,
    ) -> Result<ObjectId> {
        let url = format!("{}/sync/object", self.base_url);
        let headers = self.build_headers(Some(session));

        let request = CreateObjectRequest {
            table: table.to_string(),
            values,
            schema_context,
        };

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JazzError::Sync(e.to_string()))?;

        let result: CreateObjectResponse = response.json().await?;
        Ok(result.object_id)
    }

    /// Update an object with explicit session (for backend impersonation).
    pub async fn update_object_with_session(
        &self,
        object_id: ObjectId,
        updates: Vec<(String, Value)>,
        session: &Session,
        schema_context: groove::schema_manager::QuerySchemaContext,
    ) -> Result<()> {
        let url = format!("{}/sync/object", self.base_url);
        let headers = self.build_headers(Some(session));

        let request = UpdateObjectRequest {
            object_id,
            updates,
            schema_context,
        };

        self.client
            .put(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JazzError::Sync(e.to_string()))?;

        Ok(())
    }

    /// Delete an object with explicit session (for backend impersonation).
    pub async fn delete_object_with_session(
        &self,
        object_id: ObjectId,
        session: &Session,
        schema_context: groove::schema_manager::QuerySchemaContext,
    ) -> Result<()> {
        let url = format!("{}/sync/object/delete", self.base_url);
        let headers = self.build_headers(Some(session));

        let request = DeleteObjectRequest {
            object_id,
            schema_context,
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

    /// Push a sync payload to the server.
    pub async fn push_sync(&self, payload: SyncPayload, client_id: ClientId) -> Result<()> {
        let url = format!("{}/sync", self.base_url);

        // Check if this is a catalogue object - use admin headers
        let headers = if is_catalogue_payload(&payload) {
            self.build_admin_headers()
        } else {
            self.build_headers(None)
        };

        let request = SyncPayloadRequest { payload, client_id };

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

    /// Get the connection ID (once connected via SSE).
    pub fn connection_id(&self) -> Option<ConnectionId> {
        self.connection_id
    }

    /// Get the base URL for this connection.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Check if backend secret is configured.
    pub fn has_backend_secret(&self) -> bool {
        self.auth.backend_secret.is_some()
    }
}

/// Check if a sync payload is for a catalogue object (schema or lens).
fn is_catalogue_payload(payload: &SyncPayload) -> bool {
    match payload {
        SyncPayload::ObjectUpdated { metadata, .. } => {
            if let Some(meta) = metadata {
                if let Some(type_str) = meta.metadata.get("type") {
                    return type_str == "catalogue_schema" || type_str == "catalogue_lens";
                }
            }
            false
        }
        _ => false,
    }
}
