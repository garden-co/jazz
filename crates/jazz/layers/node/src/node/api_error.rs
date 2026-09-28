//! The public database API error with stable machine-readable codes.
//! Re-exported from `crate::db`.

/// Public API error with stable machine-readable codes.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Error {
    /// Stable error code.
    pub code: ErrorCode,
    /// Human-readable detail.
    pub message: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.code {
            ErrorCode::TransactionConflict => {
                write!(formatter, "(transaction_conflict): {}", self.message)
            }
            _ => write!(formatter, "{:?}: {}", self.code, self.message),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    #[doc(hidden)]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Stable API error code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ErrorCode {
    /// Schema validation failed.
    Schema,
    /// Query validation or binding failed.
    Query,
    /// Write was rejected.
    WriteRejected,
    /// An exclusive transaction's fixed snapshot was invalidated locally.
    TransactionConflict,
    /// Storage failed.
    Storage,
    /// Protocol or local node operation failed.
    Protocol,
    /// Local transport queue is full and the operation should be retried later.
    Backpressure,
    /// Requested observation is not locally available in this slice.
    NotObserved,
    /// Historical read must be evaluated by a complete-history server.
    HistoricalReadRequiresServer,
}

impl From<crate::node::Error> for Error {
    fn from(error: crate::node::Error) -> Self {
        let code = match &error {
            crate::node::Error::HistoricalReadRequiresServer => {
                ErrorCode::HistoricalReadRequiresServer
            }
            crate::node::Error::Storage(_) | crate::node::Error::Groove(_) => ErrorCode::Storage,
            crate::node::Error::Query(_) => ErrorCode::Query,
            crate::node::Error::TransactionConflict => ErrorCode::TransactionConflict,
            crate::node::Error::TableNotFound(_)
            | crate::node::Error::UnsupportedColumnType(_)
            | crate::node::Error::InvalidMergeableCommit(_) => ErrorCode::Schema,
            _ => ErrorCode::Protocol,
        };
        Self::new(code, error.to_string())
    }
}

impl From<crate::query::QueryError> for Error {
    fn from(error: crate::query::QueryError) -> Self {
        Self::new(ErrorCode::Query, error.to_string())
    }
}
