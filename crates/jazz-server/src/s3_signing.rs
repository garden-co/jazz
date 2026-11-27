//! S3 request signing shared by the native and Durable Object hosts.
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// One set of AWS keys.
#[derive(Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Keys {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keys")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// The SigV4 `Authorization` header for a request to S3. `headers` holds
/// every header to sign, `x-amz-date` among them, and is sorted in place;
/// `query` is the canonical query string.
pub fn sign(
    method: &str,
    path: &str,
    query: &str,
    headers: &mut [(String, String)],
    payload_hash: &str,
    keys: &Keys,
    region: &str,
) -> String {
    headers.sort_by(|a, b| a.0.cmp(&b.0));
    let date_time = headers
        .iter()
        .find(|(name, _)| name == "x-amz-date")
        .map(|(_, value)| value.clone())
        .expect("x-amz-date is signed");
    let date = &date_time[..8];
    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", value.trim()))
        .collect();
    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request =
        format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{date_time}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let key = [region.as_bytes(), b"s3", b"aws4_request"].iter().fold(
        hmac(
            format!("AWS4{}", keys.secret_access_key).as_bytes(),
            date.as_bytes(),
        ),
        |key, part| hmac(&key, part),
    );
    let signature = hex::encode(hmac(&key, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        keys.access_key_id
    )
}
