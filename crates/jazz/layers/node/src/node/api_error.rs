//! The public database API error with stable machine-readable codes.
//! Re-exported from `crate::db`.

/// Public API error with stable machine-readable codes.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Error {
    /// Stable error code.
    pub code: ErrorCode,
    /// Human-readable detail.
    pub message: String,
    /// The codec-family difference behind an
    /// [`ErrorCode::UnsupportedStorageCodecs`] refusal. Present exactly when
    /// `code` is that code, so bindings can report it as structured fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_codecs: Option<StorageCodecMismatch>,
}

/// Which codec families an existing store and the opening build disagree on.
///
/// Codec IDs carry their format version (`name.vN`): `missing` names formats
/// this build requires that the store lacks, and `unknown` names formats the
/// store was written with that this build does not read.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct StorageCodecMismatch {
    /// Storage epoch the refused root declares.
    pub epoch: u16,
    /// Codec families this build requires and the root lacks.
    pub missing: Vec<String>,
    /// Codec families the root declares and this build does not read.
    pub unknown: Vec<String>,
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
            storage_codecs: None,
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
    /// An existing local store was written with a storage format this build
    /// does not read. The store is left untouched; there is no migration, so
    /// the application deletes the local store and resyncs from Core.
    UnsupportedStorageCodecs,
}

impl ErrorCode {
    /// The stable, snake_case name of this code.
    ///
    /// This string is a durable contract: bindings expose it to JavaScript as
    /// `error.code`, and mutation-error events report it as their `code`. Every
    /// spelling is written out here rather than derived from `Debug`, so
    /// renaming a variant cannot silently change it. The table is pinned by the
    /// `error_code_strings` integration test.
    pub const fn as_str(self) -> &'static str {
        match self {
            ErrorCode::Schema => "schema",
            ErrorCode::Query => "query",
            ErrorCode::WriteRejected => "write_rejected",
            ErrorCode::TransactionConflict => "transaction_conflict",
            ErrorCode::Storage => "storage",
            ErrorCode::Protocol => "protocol",
            ErrorCode::Backpressure => "backpressure",
            ErrorCode::NotObserved => "not_observed",
            ErrorCode::HistoricalReadRequiresServer => "historical_read_requires_server",
            ErrorCode::UnsupportedStorageCodecs => "unsupported_storage_codecs",
        }
    }
}

impl From<groove::storage::Error> for Error {
    fn from(error: groove::storage::Error) -> Self {
        let message = error.to_string();
        match error {
            groove::storage::Error::UnsupportedStorageCodecs {
                epoch,
                missing,
                unknown,
            } => Self {
                code: ErrorCode::UnsupportedStorageCodecs,
                message,
                storage_codecs: Some(StorageCodecMismatch {
                    epoch,
                    missing,
                    unknown,
                }),
            },
            _ => Self::new(ErrorCode::Storage, message),
        }
    }
}

impl From<crate::node::Error> for Error {
    fn from(error: crate::node::Error) -> Self {
        if let crate::node::Error::Storage(storage) = error {
            return storage.into();
        }
        let code = match &error {
            crate::node::Error::HistoricalReadRequiresServer => {
                ErrorCode::HistoricalReadRequiresServer
            }
            crate::node::Error::Groove(_) => ErrorCode::Storage,
            crate::node::Error::Query(_) | crate::node::Error::UnsupportedExclusiveRead(_) => {
                ErrorCode::Query
            }
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
