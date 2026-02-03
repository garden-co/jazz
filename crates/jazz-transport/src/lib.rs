//! HTTP/SSE transport protocol types for Jazz.
//!
//! This crate defines the wire format for communication between Jazz clients
//! and servers over HTTP and Server-Sent Events (SSE).
//!
//! # Protocol Overview
//!
//! - Clients connect to a single SSE endpoint (`/events`) for all subscription updates
//! - Mutations and subscriptions use REST endpoints with JSON bodies
//! - Server pushes all sync updates and query notifications via SSE
//!
//! # Endpoints
//!
//! | Route | Method | Description |
//! |-------|--------|-------------|
//! | `/events` | GET | SSE stream for all subscription updates |
//! | `/sync/subscribe` | POST | Subscribe to a query |
//! | `/sync/unsubscribe` | POST | Unsubscribe from a query |
//! | `/sync/object` | POST/PUT | Create or write to objects |

use serde::{Deserialize, Serialize};

use groove::object::ObjectId;
use groove::query_manager::query::Query;
use groove::query_manager::types::Value;
use groove::schema_manager::QuerySchemaContext;
use groove::sync_manager::{ClientId, QueryId, SyncPayload};

/// Unique identifier for a client's SSE connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectionId(pub u64);

// ============================================================================
// Client -> Server Requests
// ============================================================================

/// Request to subscribe to a query.
///
/// Session context for policy evaluation comes from HTTP headers (JWT or backend impersonation),
/// not from the request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeRequest {
    /// The query to subscribe to.
    pub query: Query,
    /// Schema context for query execution.
    /// Tells the server which schema version the client uses.
    pub schema_context: QuerySchemaContext,
}

/// Response to a subscribe request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeResponse {
    /// ID for this subscription (used to unsubscribe).
    pub query_id: QueryId,
}

/// Request to unsubscribe from a query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
    /// ID of the subscription to cancel.
    pub query_id: QueryId,
}

/// Request to create a new object/row.
///
/// Session context for policy evaluation comes from HTTP headers (JWT or backend impersonation),
/// not from the request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateObjectRequest {
    /// Table name.
    pub table: String,
    /// Column values for the new row.
    pub values: Vec<Value>,
    /// Schema context for mutation execution.
    /// Tells the server which schema version the client uses.
    pub schema_context: QuerySchemaContext,
}

/// Response to a create request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateObjectResponse {
    /// ID of the created object.
    pub object_id: ObjectId,
}

/// Request to update an existing object/row.
///
/// Session context for policy evaluation comes from HTTP headers (JWT or backend impersonation),
/// not from the request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateObjectRequest {
    /// ID of the object to update.
    pub object_id: ObjectId,
    /// Column name -> new value pairs.
    pub updates: Vec<(String, Value)>,
    /// Schema context for mutation execution.
    /// Tells the server which schema version the client uses.
    pub schema_context: QuerySchemaContext,
}

/// Request to delete an object/row.
///
/// Session context for policy evaluation comes from HTTP headers (JWT or backend impersonation),
/// not from the request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteObjectRequest {
    /// ID of the object to delete.
    pub object_id: ObjectId,
    /// Schema context for mutation execution.
    /// Tells the server which schema version the client uses.
    pub schema_context: QuerySchemaContext,
}

/// Request to sync object data (for peer-to-peer sync).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncObjectRequest {
    /// The sync payload containing commits.
    pub payload: SyncPayload,
}

/// Request to push a sync payload to the server's inbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPayloadRequest {
    /// The sync payload from the client's outbox.
    pub payload: SyncPayload,
    /// Client ID for source tracking.
    pub client_id: ClientId,
}

// ============================================================================
// Server -> Client Events (SSE)
// ============================================================================

/// Event sent over SSE stream.
///
/// Note: Query results are NOT sent here directly. The server syncs the
/// underlying objects, and the client's local QueryManager handles query
/// notifications based on the synced data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerEvent {
    /// Connection established, server sends connection ID and confirms client ID.
    Connected {
        connection_id: ConnectionId,
        /// The client ID the server is using for this connection.
        client_id: String,
    },

    /// Subscription created successfully.
    Subscribed { query_id: QueryId },

    /// Sync update - object data changed.
    SyncUpdate { payload: SyncPayload },

    /// Error response.
    Error { message: String, code: ErrorCode },

    /// Heartbeat to keep connection alive.
    Heartbeat,
}

/// Error codes for server errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    /// Invalid request format.
    BadRequest,
    /// Authentication required or failed.
    Unauthorized,
    /// Permission denied by policy.
    Forbidden,
    /// Resource not found.
    NotFound,
    /// Internal server error.
    Internal,
    /// Rate limit exceeded.
    RateLimited,
}

// ============================================================================
// HTTP Response Types
// ============================================================================

/// Generic success response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuccessResponse {
    pub success: bool,
}

impl Default for SuccessResponse {
    fn default() -> Self {
        Self { success: true }
    }
}

/// Generic error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: ErrorCode,
}

impl ErrorResponse {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::BadRequest,
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::Unauthorized,
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::Forbidden,
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::NotFound,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::Internal,
        }
    }
}

// ============================================================================
// SSE Encoding Helpers
// ============================================================================

impl ServerEvent {
    /// Encode as SSE data line.
    ///
    /// Returns a string in the format: `data: {json}\n\n`
    pub fn to_sse_data(&self) -> String {
        let json = serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string());
        format!("data: {}\n\n", json)
    }

    /// Parse from SSE data line.
    ///
    /// Expects input in the format: `data: {json}` (without trailing newlines)
    pub fn from_sse_data(line: &str) -> Option<Self> {
        let json = line.strip_prefix("data: ")?;
        serde_json::from_str(json).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_event_sse_encoding() {
        let event = ServerEvent::Connected {
            connection_id: ConnectionId(42),
            client_id: "test-client-id".to_string(),
        };

        let sse = event.to_sse_data();
        assert!(sse.starts_with("data: "));
        assert!(sse.ends_with("\n\n"));
        assert!(sse.contains("Connected"));
        assert!(sse.contains("42"));
        assert!(sse.contains("test-client-id"));
    }

    #[test]
    fn test_server_event_sse_decoding() {
        let line = r#"data: {"type":"Heartbeat"}"#;
        let event = ServerEvent::from_sse_data(line).unwrap();
        assert!(matches!(event, ServerEvent::Heartbeat));
    }

    #[test]
    fn test_error_response_constructors() {
        let err = ErrorResponse::bad_request("invalid query");
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert_eq!(err.error, "invalid query");

        let err = ErrorResponse::forbidden("not allowed");
        assert_eq!(err.code, ErrorCode::Forbidden);
    }

    #[test]
    fn test_subscribe_request_serialization() {
        use groove::query_manager::query::QueryBuilder;
        use groove::query_manager::types::{SchemaHash, TableName};

        let query = QueryBuilder::new(TableName::new("users")).build();
        let schema_context =
            QuerySchemaContext::new("dev", SchemaHash::from_bytes([0; 32]), "main");
        let request = SubscribeRequest {
            query,
            schema_context,
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("users"));
        assert!(json.contains("schema_context"));

        let parsed: SubscribeRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.schema_context.env, "dev");
    }
}
