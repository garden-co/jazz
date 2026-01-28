use thiserror::Error;

#[derive(Error, Debug)]
pub enum CoJsonCoreError {
    #[error("Transaction not found at index {0}")]
    TransactionNotFound(u32),

    #[error("Invalid encrypted prefix in transaction")]
    InvalidEncryptedPrefix,

    #[error("Base64 decoding failed")]
    Base64Decode(#[from] base64::DecodeError),

    #[error("UTF-8 conversion failed")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("JSON deserialization failed")]
    Json(#[from] serde_json::Error),

    #[error("Signature verification failed: (hash: {0})")]
    SignatureVerification(String),

    #[error("Invalid decoding prefix")]
    InvalidDecodingPrefix,

    #[error("Invalid key length")]
    InvalidKeyLength(usize, usize),

    #[error("Invalid base58")]
    InvalidBase58(#[from] bs58::decode::Error),

    #[error("Transaction too large to be synced: {0} bytes > {1} bytes limit")]
    TransactionTooLarge(usize, usize),
}
