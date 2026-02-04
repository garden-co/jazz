//! HTTP/SSE transport protocol types for Jazz.
//!
//! This crate defines the wire format for communication between Jazz clients
//! and servers over HTTP and Server-Sent Events (SSE).
//!
//! # Protocol Overview
//!
//! - Clients connect to `/events` (SSE) for all subscription updates
//! - All client→server communication flows through a single `/sync` endpoint
//! - Session is bound at connection time via HTTP headers
//!
//! # Endpoints
//!
//! | Route | Method | Description |
//! |-------|--------|-------------|
//! | `/events` | GET | SSE stream for all subscription updates |
//! | `/sync` | POST | Unified sync endpoint for all SyncPayload variants |

use serde::{Deserialize, Serialize};

use groove::sync_manager::{ClientId, QueryId, SyncPayload};

/// Unique identifier for a client's SSE connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectionId(pub u64);

// ============================================================================
// Client -> Server Requests
// ============================================================================

/// Request to push a sync payload to the server's inbox.
///
/// This is the unified request type for all client→server communication.
/// Session context is extracted from HTTP headers at connection time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPayloadRequest {
    /// The sync payload from the client's outbox.
    /// Can be any SyncPayload variant: ObjectUpdated, QuerySubscription, etc.
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

impl ServerEvent {
    /// Get the variant name for debugging.
    pub fn variant_name(&self) -> &'static str {
        match self {
            ServerEvent::Connected { .. } => "Connected",
            ServerEvent::Subscribed { .. } => "Subscribed",
            ServerEvent::SyncUpdate { .. } => "SyncUpdate",
            ServerEvent::Error { .. } => "Error",
            ServerEvent::Heartbeat => "Heartbeat",
        }
    }
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
    fn test_sync_payload_request_serialization() {
        use groove::object::BranchName;
        use groove::object::ObjectId;
        use groove::sync_manager::ClientId;

        let payload = SyncPayload::ObjectUpdated {
            object_id: ObjectId::new(),
            metadata: None,
            branch_name: BranchName::new("main"),
            commits: vec![],
        };
        let request = SyncPayloadRequest {
            payload,
            client_id: ClientId::new(),
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("ObjectUpdated"));
        assert!(json.contains("main"));

        let parsed: SyncPayloadRequest = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed.payload, SyncPayload::ObjectUpdated { .. }));
    }
}
