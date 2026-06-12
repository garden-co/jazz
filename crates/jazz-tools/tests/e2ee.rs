//! Black-box tests for the E2EE crypto core (public API only).

use jazz_tools::e2ee::{
    E2eeError, E2eePublicKey, SEALED_KEY_LEN, SpaceKey, derive_e2ee_keypair, seal_space_key,
    unseal_space_key,
};
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

#[test]
fn seal_unseal_round_trips() {
    let recipient = derive_e2ee_keypair(&[1u8; 32]);
    let space_key = SpaceKey::generate();
    let sealed = seal_space_key(&recipient.public, &space_key).unwrap();
    assert_eq!(sealed.len(), SEALED_KEY_LEN); // 81 bytes: alg(1) + encapped(32) + ct(48)
    assert_eq!(sealed[0], 1); // alg_id 1 = HPKE X25519
    let unsealed = unseal_space_key(&recipient, &sealed).unwrap();
    assert_eq!(unsealed.as_bytes(), space_key.as_bytes());
}

#[test]
fn unseal_with_wrong_recipient_fails() {
    let recipient = derive_e2ee_keypair(&[1u8; 32]);
    let attacker = derive_e2ee_keypair(&[2u8; 32]);
    let sealed = seal_space_key(&recipient.public, &SpaceKey::generate()).unwrap();
    assert!(matches!(
        unseal_space_key(&attacker, &sealed),
        Err(E2eeError::Unseal(_))
    ));
}

#[test]
fn unseal_rejects_tampered_blob() {
    let recipient = derive_e2ee_keypair(&[1u8; 32]);
    let mut sealed = seal_space_key(&recipient.public, &SpaceKey::generate()).unwrap();
    let last = sealed.len() - 1;
    sealed[last] ^= 0xff;
    assert!(unseal_space_key(&recipient, &sealed).is_err());
}

#[test]
fn unseal_rejects_malformed_blobs() {
    let recipient = derive_e2ee_keypair(&[1u8; 32]);
    assert_eq!(
        unseal_space_key(&recipient, &[]),
        Err(E2eeError::MalformedSealedKey)
    );
    assert_eq!(
        unseal_space_key(&recipient, &[1u8; 10]),
        Err(E2eeError::MalformedSealedKey)
    );
    // unknown algorithm id
    let mut sealed = seal_space_key(&recipient.public, &SpaceKey::generate()).unwrap();
    sealed[0] = 99;
    assert_eq!(
        unseal_space_key(&recipient, &sealed),
        Err(E2eeError::UnsupportedAlgorithm(99))
    );
}

#[test]
fn space_keys_are_random() {
    assert_ne!(
        SpaceKey::generate().as_bytes(),
        SpaceKey::generate().as_bytes()
    );
}
