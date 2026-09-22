use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha512};
use uuid::Uuid;

const KEY_NAMESPACE: Uuid = Uuid::from_bytes([
    0x6a, 0x61, 0x7a, 0x7a, 0x2d, 0x61, 0x75, 0x74, 0x68, 0x2d, 0x6b, 0x65, 0x79, 0x2d, 0x76, 0x31,
]);

const SIGN_DOMAIN: &str = "jazz-auth-sign-v1";

pub const LOCAL_FIRST_ISSUER: &str = "urn:jazz:local-first";
pub const ANONYMOUS_ISSUER: &str = "urn:jazz:anonymous";

#[derive(serde::Serialize)]
struct LocalFirstClaims<'a> {
    iss: &'a str,
    sub: &'a str,
    aud: &'a str,
    jazz_pub_key: &'a str,
    iat: u64,
    exp: u64,
}

#[derive(serde::Serialize)]
struct JwtHeader<'a> {
    alg: &'a str,
    typ: &'a str,
}

pub fn mint_jazz_self_signed_token_at(
    seed: &[u8; 32],
    issuer: &'static str,
    audience: &str,
    ttl_seconds: u64,
    now_seconds: u64,
) -> Result<String, String> {
    let signing_key = derive_signing_key(seed);
    let verifying_key = signing_key.verifying_key();
    let user_id = user_id_from_public_key(verifying_key.as_bytes());
    let user_id_str = user_id.to_string();
    let pub_key_b64 = URL_SAFE_NO_PAD.encode(verifying_key.as_bytes());

    let normalized_aud = match Uuid::parse_str(audience) {
        Ok(uuid) => uuid.to_string(),
        Err(_) => Uuid::new_v5(&Uuid::NAMESPACE_DNS, audience.as_bytes()).to_string(),
    };

    let header = JwtHeader {
        alg: "EdDSA",
        typ: "JWT",
    };
    let header_json = serde_json::to_string(&header).map_err(|e| e.to_string())?;
    let header_b64 = URL_SAFE_NO_PAD.encode(header_json.as_bytes());

    let claims = LocalFirstClaims {
        iss: issuer,
        sub: &user_id_str,
        aud: &normalized_aud,
        jazz_pub_key: &pub_key_b64,
        iat: now_seconds,
        exp: now_seconds + ttl_seconds,
    };
    let claims_json = serde_json::to_string(&claims).map_err(|e| e.to_string())?;
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims_json.as_bytes());

    let signing_input = format!("{}.{}", header_b64, claims_b64);
    let signature = signing_key.sign(signing_input.as_bytes());
    let signature_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    Ok(format!("{}.{}", signing_input, signature_b64))
}

pub fn derive_user_id(seed: &[u8; 32]) -> Uuid {
    let verifying_key = derive_signing_key(seed).verifying_key();
    user_id_from_public_key(verifying_key.as_bytes())
}

fn derive_signing_key(seed: &[u8; 32]) -> SigningKey {
    let mut hasher = Sha512::new();
    hasher.update(SIGN_DOMAIN.as_bytes());
    hasher.update(seed);
    let hash = hasher.finalize();
    let key_bytes: [u8; 32] = hash[..32].try_into().expect("SHA-512 output is 64 bytes");
    SigningKey::from_bytes(&key_bytes)
}

fn user_id_from_public_key(pub_key_bytes: &[u8; 32]) -> Uuid {
    Uuid::new_v5(&KEY_NAMESPACE, pub_key_bytes)
}
