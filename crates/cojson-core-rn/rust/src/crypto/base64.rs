use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD},
    Engine,
};

use super::ed25519::CryptoErrorUniffi;

/// Encodes bytes to a base64url string (with padding to match JS implementation)
#[uniffi::export]
pub fn bytes_to_base64url(bytes: Vec<u8>) -> String {
    URL_SAFE.encode(&bytes)
}

/// Encodes bytes to a standard base64 string (with padding)
/// Use this for data URLs and other contexts requiring standard base64.
#[uniffi::export]
pub fn bytes_to_base64(bytes: Vec<u8>) -> String {
    STANDARD.encode(&bytes)
}

/// Decodes a base64url string to bytes (handles both padded and unpadded)
#[uniffi::export]
pub fn base64url_to_bytes(base64: String) -> Result<Vec<u8>, CryptoErrorUniffi> {
    // Try with padding first, then without padding as fallback
    URL_SAFE
        .decode(&base64)
        .or_else(|_| URL_SAFE_NO_PAD.decode(&base64))
        .map_err(|e| CryptoErrorUniffi::Base64DecodeError(e.to_string()))
}
