use thiserror::Error;

#[derive(Debug, Error)]
pub enum CryptoError {
  #[error("Invalid key length (expected {0}, got {1})")]
  InvalidKeyLength(usize, usize),
  #[error("Invalid nonce length")]
  InvalidNonceLength,
  #[error("Invalid sealer secret format: must start with 'sealerSecret_z'")]
  InvalidSealerSecretFormat,
  #[error("Invalid signature length")]
  InvalidSignatureLength,
  #[error("Invalid verifying key: {0}")]
  InvalidVerifyingKey(String),
  #[error("Invalid public key: {0}")]
  InvalidPublicKey(String),
  #[error("Wrong tag")]
  WrongTag,
  #[error("Failed to create cipher")]
  CipherError,
  #[error("Invalid prefix: {0} must start with '{1}'")]
  InvalidPrefix(&'static str, &'static str),
  #[error("Invalid base58: {0}")]
  Base58Error(String),
  #[error("JSON parsing failed: {0}")]
  JsonParse(#[from] serde_json::Error),
}

impl From<CryptoError> for String {
    fn from(err: CryptoError) -> Self {
      err.to_string()
    }
}
