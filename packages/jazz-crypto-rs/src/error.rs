use std::fmt;

#[derive(Debug)]
pub enum CryptoError {
    InvalidKeyLength,
    InvalidNonceLength,
    InvalidSignatureLength,
    InvalidVerifyingKey(String),
    WrongTag,
    CipherError,
}

impl fmt::Display for CryptoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CryptoError::InvalidKeyLength => write!(f, "Invalid key length"),
            CryptoError::InvalidNonceLength => write!(f, "Invalid nonce length"),
            CryptoError::InvalidSignatureLength => write!(f, "Invalid signature length"),
            CryptoError::InvalidVerifyingKey(e) => write!(f, "Invalid verifying key: {}", e),
            CryptoError::WrongTag => write!(f, "Wrong tag"),
            CryptoError::CipherError => write!(f, "Failed to create cipher"),
        }
    }
}

impl std::error::Error for CryptoError {}
