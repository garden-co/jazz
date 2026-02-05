use base64::{
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
    Engine,
};

/// Encodes bytes to a base64url string (with padding to match JS implementation)
#[uniffi::export]
pub fn bytes_to_base64url(bytes: Vec<u8>) -> String {
    URL_SAFE.encode(&bytes)
}

/// Decodes a base64url string to bytes (handles both padded and unpadded)
#[uniffi::export]
pub fn base64url_to_bytes(base64: String) -> Vec<u8> {
    // Try with padding first, then without padding as fallback
    URL_SAFE
        .decode(&base64)
        .or_else(|_| URL_SAFE_NO_PAD.decode(&base64))
        .unwrap_or_default()
}
