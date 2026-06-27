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

/// Auth failure reasons returned by runtime-facing HTTP endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnauthenticatedCode {
    Expired,
    Missing,
    Invalid,
    Disabled,
}

/// Structured unauthenticated response for runtime-facing HTTP endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnauthenticatedResponse {
    pub error: &'static str,
    pub code: UnauthenticatedCode,
    pub message: String,
}

impl UnauthenticatedResponse {
    pub fn expired(message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code: UnauthenticatedCode::Expired,
            message: message.into(),
        }
    }

    pub fn missing(message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code: UnauthenticatedCode::Missing,
            message: message.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code: UnauthenticatedCode::Invalid,
            message: message.into(),
        }
    }

    pub fn disabled(message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code: UnauthenticatedCode::Disabled,
            message: message.into(),
        }
    }
}
