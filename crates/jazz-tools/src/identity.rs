use ed25519_dalek::{SigningKey, VerifyingKey};
use sha2::{Digest, Sha512};
use uuid::Uuid;

const KEY_NAMESPACE: Uuid = Uuid::from_bytes([
    0x6a, 0x61, 0x7a, 0x7a, 0x2d, 0x61, 0x75, 0x74, 0x68, 0x2d, 0x6b, 0x65, 0x79, 0x2d, 0x76, 0x31,
]);

const SIGN_DOMAIN: &str = "jazz-auth-sign-v1";

/// Derive a signing key from a 32-byte seed and a domain string.
/// Uses SHA-512(domain || seed), taking the first 32 bytes as the Ed25519 key material.
pub fn derive_signing_key(seed: &[u8; 32], domain: &str) -> SigningKey {
    let mut hasher = Sha512::new();
    hasher.update(domain.as_bytes());
    hasher.update(seed);
    let hash = hasher.finalize();
    let key_bytes: [u8; 32] = hash[..32].try_into().expect("SHA-512 output is 64 bytes");
    SigningKey::from_bytes(&key_bytes)
}

/// Derive the verifying (public) key from a 32-byte seed using the standard sign domain.
pub fn derive_verifying_key(seed: &[u8; 32]) -> VerifyingKey {
    derive_signing_key(seed, SIGN_DOMAIN).verifying_key()
}

/// Derive a stable UUIDv5 user identity from a 32-byte seed.
/// Derives the signing key for the sign domain, extracts the public key,
/// then produces UUIDv5(KEY_NAMESPACE, public_key_bytes).
pub fn derive_user_id(seed: &[u8; 32]) -> Uuid {
    let verifying_key = derive_verifying_key(seed);
    Uuid::new_v5(&KEY_NAMESPACE, verifying_key.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alice_seed() -> [u8; 32] {
        let mut seed = [0u8; 32];
        seed[0] = 0xAA;
        seed[31] = 0x01;
        seed
    }

    fn bob_seed() -> [u8; 32] {
        let mut seed = [0u8; 32];
        seed[0] = 0xBB;
        seed[31] = 0x02;
        seed
    }

    #[test]
    fn same_seed_produces_same_user_id() {
        let id1 = derive_user_id(&alice_seed());
        let id2 = derive_user_id(&alice_seed());
        assert_eq!(id1, id2);
    }

    #[test]
    fn different_seeds_produce_different_user_ids() {
        let alice_id = derive_user_id(&alice_seed());
        let bob_id = derive_user_id(&bob_seed());
        assert_ne!(alice_id, bob_id);
    }

    #[test]
    fn user_id_is_uuid_v5() {
        let id = derive_user_id(&alice_seed());
        assert_eq!(id.get_version_num(), 5);
    }

    #[test]
    fn derive_verifying_key_is_deterministic() {
        let key1 = derive_verifying_key(&alice_seed());
        let key2 = derive_verifying_key(&alice_seed());
        assert_eq!(key1.as_bytes(), key2.as_bytes());
    }
}
