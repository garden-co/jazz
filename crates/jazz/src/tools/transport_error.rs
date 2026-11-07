use serde::{Deserialize, Serialize};

/// Error codes returned by runtime-facing HTTP endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Invalid request format.
    BadRequest,
    /// Client and server do not share a compatible sync protocol version.
    IncompatibleProtocol,
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

/// Generic error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: ErrorCode,
}

impl ErrorResponse {
    fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code,
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::BadRequest, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthorized, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }
}

/// Auth failure reasons returned by runtime-facing HTTP endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnauthenticatedCode {
    Expired,
    Missing,
    Invalid,
    Disabled,
    /// Authentication could not be checked because its key service is unavailable.
    Unavailable,
}

/// Structured unauthenticated response for runtime-facing HTTP endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnauthenticatedResponse {
    pub error: &'static str,
    pub code: UnauthenticatedCode,
    pub message: String,
}

impl UnauthenticatedResponse {
    fn new(code: UnauthenticatedCode, message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code,
            message: message.into(),
        }
    }

    pub fn expired(message: impl Into<String>) -> Self {
        Self::new(UnauthenticatedCode::Expired, message)
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(UnauthenticatedCode::Invalid, message)
    }

    pub fn disabled(message: impl Into<String>) -> Self {
        Self::new(UnauthenticatedCode::Disabled, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(UnauthenticatedCode::Unavailable, message)
    }
}
