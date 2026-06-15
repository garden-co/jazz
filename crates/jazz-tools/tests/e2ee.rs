//! Black-box tests for the E2EE crypto core (public API only).

use jazz_tools::e2ee::{
    E2eeError, E2eePublicKey, ENVELOPE_HEADER_LEN, EncryptionContext, SEALED_KEY_LEN, SpaceKey,
    decrypt_value, derive_e2ee_keypair, encrypt_value, envelope_key_id, seal_space_key,
    unseal_space_key,
};
use jazz_tools::identity::derive_verifying_key;
use uuid::Uuid;

fn ctx<'a>(table: &'a str, column: &'a str, row_id: &'a [u8]) -> EncryptionContext<'a> {
    EncryptionContext {
        table,
        column,
        row_id,
    }
}

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

#[test]
fn value_encryption_round_trips() {
    let key = SpaceKey::generate();
    let key_id = Uuid::new_v4();
    let context = ctx("todos", "title", b"row-1");
    let envelope = encrypt_value(&key, &key_id, &context, b"secret title").unwrap();
    assert_eq!(envelope[0], 1); // alg_id 1 = XChaCha20-Poly1305
    assert_eq!(envelope_key_id(&envelope).unwrap(), key_id);
    let plaintext = decrypt_value(&key, &context, &envelope).unwrap();
    assert_eq!(plaintext, b"secret title");
}

#[test]
fn nonces_are_unique_per_encryption() {
    let key = SpaceKey::generate();
    let key_id = Uuid::new_v4();
    let context = ctx("todos", "title", b"row-1");
    let a = encrypt_value(&key, &key_id, &context, b"same").unwrap();
    let b = encrypt_value(&key, &key_id, &context, b"same").unwrap();
    // nonce lives at bytes [17..41]
    assert_ne!(a[17..41], b[17..41]);
    assert_ne!(a, b);
}

#[test]
fn decrypt_fails_when_any_context_field_differs() {
    let key = SpaceKey::generate();
    let key_id = Uuid::new_v4();
    let envelope = encrypt_value(&key, &key_id, &ctx("todos", "title", b"row-1"), b"x").unwrap();

    for wrong in [
        ctx("notes", "title", b"row-1"), // different table
        ctx("todos", "body", b"row-1"),  // different column
        ctx("todos", "title", b"row-2"), // different row
    ] {
        assert!(matches!(
            decrypt_value(&key, &wrong, &envelope),
            Err(E2eeError::Decrypt(_))
        ));
    }
}

#[test]
fn context_encoding_is_unambiguous_across_field_boundaries() {
    // ("ab", "c") and ("a", "bc") must not produce the same AAD.
    let key = SpaceKey::generate();
    let key_id = Uuid::new_v4();
    let envelope = encrypt_value(&key, &key_id, &ctx("ab", "c", b"r"), b"x").unwrap();
    assert!(decrypt_value(&key, &ctx("a", "bc", b"r"), &envelope).is_err());
}

#[test]
fn decrypt_fails_with_wrong_key() {
    let context = ctx("todos", "title", b"row-1");
    let envelope = encrypt_value(&SpaceKey::generate(), &Uuid::new_v4(), &context, b"x").unwrap();
    assert!(decrypt_value(&SpaceKey::generate(), &context, &envelope).is_err());
}

#[test]
fn decrypt_binds_key_id_via_aad() {
    // Same key, same context, but envelope key_id swapped to a different uuid:
    // authentication must fail because key_id is part of the AAD.
    let key = SpaceKey::generate();
    let context = ctx("todos", "title", b"row-1");
    let mut envelope = encrypt_value(&key, &Uuid::new_v4(), &context, b"x").unwrap();
    let other = Uuid::new_v4();
    envelope[1..17].copy_from_slice(other.as_bytes());
    assert!(decrypt_value(&key, &context, &envelope).is_err());
}

#[test]
fn decrypt_rejects_malformed_envelopes() {
    let key = SpaceKey::generate();
    let context = ctx("todos", "title", b"row-1");
    assert_eq!(
        decrypt_value(&key, &context, &[]),
        Err(E2eeError::MalformedEnvelope)
    );
    assert_eq!(
        decrypt_value(&key, &context, &[1u8; ENVELOPE_HEADER_LEN - 1]),
        Err(E2eeError::MalformedEnvelope)
    );
    let mut envelope = encrypt_value(&key, &Uuid::new_v4(), &context, b"x").unwrap();
    envelope[0] = 99;
    assert_eq!(
        decrypt_value(&key, &context, &envelope),
        Err(E2eeError::UnsupportedAlgorithm(99))
    );
}

#[test]
fn empty_plaintext_round_trips() {
    let key = SpaceKey::generate();
    let context = ctx("todos", "title", b"row-1");
    let envelope = encrypt_value(&key, &Uuid::new_v4(), &context, b"").unwrap();
    assert_eq!(decrypt_value(&key, &context, &envelope).unwrap(), b"");
}
