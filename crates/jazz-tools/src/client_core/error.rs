#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientErrorCode {
    InvalidConfig,
    InvalidSchema,
    InvalidQuery,
    WriteRejected,
    BatchRejected,
    UnsupportedRuntimeFeature,
    TransportError,
    AuthFailure,
    StorageError,
    RuntimeError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientError {
    pub code: ClientErrorCode,
    pub message: String,
    pub batch_id: Option<String>,
    pub table: Option<String>,
    pub object_id: Option<String>,
}

impl ClientError {
    pub fn new(code: ClientErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            batch_id: None,
            table: None,
            object_id: None,
        }
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for ClientError {}
