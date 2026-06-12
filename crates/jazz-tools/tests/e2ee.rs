//! Black-box tests for the E2EE crypto core (public API only).

use jazz_tools::e2ee::{E2eePublicKey, derive_e2ee_keypair};
use jazz_tools::identity::derive_verifying_key;

#[test]
fn e2ee_keypair_is_deterministic_per_seed() {
    let seed = [7u8; 32];
    let a = derive_e2ee_keypair(&seed);
    let b = derive_e2ee_keypair(&seed);
    assert_eq!(a.public.as_bytes(), b.public.as_bytes());

    let other = derive_e2ee_keypair(&[8u8; 32]);
    assert_ne!(a.public.as_bytes(), other.public.as_bytes());
}

#[test]
fn e2ee_key_is_domain_separated_from_signing_key() {
    let seed = [7u8; 32];
    let e2ee = derive_e2ee_keypair(&seed);
    let signing = derive_verifying_key(&seed);
    assert_ne!(e2ee.public.as_bytes(), signing.as_bytes());
}

#[test]
fn public_key_base64url_round_trips() {
    let seed = [9u8; 32];
    let pk = derive_e2ee_keypair(&seed).public;
    let encoded = pk.to_base64url();
    // 32 bytes -> 43 base64url chars, no padding
    assert_eq!(encoded.len(), 43);
    assert!(!encoded.contains('='));
    let decoded = E2eePublicKey::from_base64url(&encoded).unwrap();
    assert_eq!(decoded.as_bytes(), pk.as_bytes());
}

#[test]
fn public_key_rejects_malformed_input() {
    assert!(E2eePublicKey::from_base64url("not base64!!!").is_err());
    assert!(E2eePublicKey::from_base64url("AAAA").is_err()); // wrong length
}
