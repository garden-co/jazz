//! Cross-checks every primitive against libsodium 1.0.22 on random inputs.
//! libsodium is a dev-dependency only.

mod common;

use std::ptr::{null, null_mut};

use common::{FixedRandom, TestRandom};
use jazz_e2ee::Error;
use jazz_e2ee::context::frame_record;
use jazz_e2ee::mechanisms::stream;
use jazz_e2ee::primitives::{aead, hash, sign};
use libsodium_sys as ffi;

const ROUNDS: usize = 300;

fn init() {
    // SAFETY: sodium_init is thread-safe and idempotent.
    assert!(unsafe { ffi::sodium_init() } >= 0);
}

fn sodium_hash(key: &[u8], input: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    // SAFETY: every pointer covers its stated length.
    let result = unsafe {
        ffi::crypto_generichash(
            out.as_mut_ptr(),
            32,
            input.as_ptr(),
            input.len() as u64,
            key.as_ptr(),
            key.len(),
        )
    };
    assert_eq!(result, 0);
    out
}

#[test]
fn blake2b_matches() {
    init();
    let mut rng = TestRandom(11);
    for round in 0..ROUNDS {
        let key = rng.array::<32>();
        let input = if round == 0 {
            Vec::new()
        } else {
            rng.sized(600)
        };
        assert_eq!(*hash::keyed_hash(&key, &input), sodium_hash(&key, &input));
        let split = rng.below(input.len() + 1);
        assert_eq!(
            *hash::keyed_hash_parts(&key, &[&input[..split], &input[split..]]),
            sodium_hash(&key, &input)
        );
    }
}

fn sodium_encrypt(key: &[u8; 32], nonce: &[u8; 24], aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; plaintext.len() + 16];
    // SAFETY: out has room for the tag; inputs cover their lengths.
    let result = unsafe {
        ffi::crypto_aead_xchacha20poly1305_ietf_encrypt(
            out.as_mut_ptr(),
            null_mut(),
            plaintext.as_ptr(),
            plaintext.len() as u64,
            aad.as_ptr(),
            aad.len() as u64,
            null(),
            nonce.as_ptr(),
            key.as_ptr(),
        )
    };
    assert_eq!(result, 0);
    out
}

fn sodium_decrypt(
    key: &[u8; 32],
    nonce: &[u8; 24],
    aad: &[u8],
    ciphertext: &[u8],
) -> Option<Vec<u8>> {
    let mut out = vec![0u8; ciphertext.len().checked_sub(16)?];
    // SAFETY: as above; the ciphertext includes its tag.
    let result = unsafe {
        ffi::crypto_aead_xchacha20poly1305_ietf_decrypt(
            out.as_mut_ptr(),
            null_mut(),
            null_mut(),
            ciphertext.as_ptr(),
            ciphertext.len() as u64,
            aad.as_ptr(),
            aad.len() as u64,
            nonce.as_ptr(),
            key.as_ptr(),
        )
    };
    (result == 0).then_some(out)
}

#[test]
fn xchacha20poly1305_matches() {
    init();
    let mut rng = TestRandom(12);
    for _ in 0..ROUNDS {
        let key = rng.array::<32>();
        let nonce = rng.array::<24>();
        let aad = rng.sized(80);
        let plaintext = rng.sized(600);
        let ours = aead::encrypt(&key, &nonce, &aad, &plaintext).unwrap();
        assert_eq!(ours, sodium_encrypt(&key, &nonce, &aad, &plaintext));
        assert_eq!(
            *aead::decrypt(&key, &nonce, &aad, &ours).unwrap(),
            plaintext
        );
        let mut tampered = ours.clone();
        rng.flip(&mut tampered);
        assert_eq!(
            aead::decrypt(&key, &nonce, &aad, &tampered).map(|_| ()),
            Err(Error::InvalidCiphertext)
        );
        assert!(sodium_decrypt(&key, &nonce, &aad, &tampered).is_none());
    }
    assert_eq!(
        aead::decrypt(&[0; 32], &[0; 24], &[], &[0; 15]).map(|_| ()),
        Err(Error::InvalidCiphertext)
    );
}

fn sodium_seed_keypair(seed: &[u8; 32]) -> ([u8; 32], [u8; 64]) {
    let (mut public, mut secret) = ([0u8; 32], [0u8; 64]);
    // SAFETY: fixed-size Ed25519 buffers.
    assert_eq!(
        unsafe {
            ffi::crypto_sign_seed_keypair(public.as_mut_ptr(), secret.as_mut_ptr(), seed.as_ptr())
        },
        0
    );
    (public, secret)
}

fn sodium_sign(secret: &[u8; 64], message: &[u8]) -> [u8; 64] {
    let mut signature = [0u8; 64];
    // SAFETY: fixed-size key and signature; message covers its length.
    let result = unsafe {
        ffi::crypto_sign_detached(
            signature.as_mut_ptr(),
            null_mut(),
            message.as_ptr(),
            message.len() as u64,
            secret.as_ptr(),
        )
    };
    assert_eq!(result, 0);
    signature
}

fn sodium_verify(public: &[u8; 32], message: &[u8], signature: &[u8; 64]) -> bool {
    // SAFETY: fixed-size key and signature; message covers its length.
    unsafe {
        ffi::crypto_sign_verify_detached(
            signature.as_ptr(),
            message.as_ptr(),
            message.len() as u64,
            public.as_ptr(),
        ) == 0
    }
}

#[test]
fn ed25519_matches() {
    init();
    let mut rng = TestRandom(13);
    for _ in 0..ROUNDS {
        let seed = rng.array::<32>();
        let ours = sign::SigningKeyPair::from_seed(&seed);
        let (public, secret) = sodium_seed_keypair(&seed);
        assert_eq!(ours.public_key, public);
        assert_eq!(*ours.secret_key, secret);
        let message = rng.sized(300);
        let signature = sign::sign(&ours.secret_key, &message).unwrap();
        assert_eq!(signature, sodium_sign(&secret, &message));
        assert!(sign::verify(&public, &message, &signature));
        let mut changed = signature;
        rng.flip(&mut changed);
        assert_eq!(
            sign::verify(&public, &message, &changed),
            sodium_verify(&public, &message, &changed)
        );
        // Random keys and signatures: both reject alike.
        let (key, garbage) = (rng.array::<32>(), {
            let mut bytes = rng.array::<64>();
            bytes[63] &= 0x0f;
            bytes
        });
        assert_eq!(
            sign::verify(&key, &message, &garbage),
            sodium_verify(&key, &message, &garbage)
        );
    }
}

/// Encodings libsodium treats specially: small-order and non-canonical keys
/// and `R`, and non-canonical `S`.
#[test]
fn ed25519_edge_cases_match() {
    init();
    let pair = sign::SigningKeyPair::from_seed(&[1; 32]);
    let message = b"edge";
    let valid = sign::sign(&pair.secret_key, message).unwrap();
    // The group order L, little-endian.
    let order: [u8; 32] = [
        0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58, 0xd6, 0x9c, 0xf7, 0xa2, 0xde, 0xf9, 0xde,
        0x14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10,
    ];
    let mut non_canonical_s = valid;
    let mut carry = 0u16;
    for index in 0..32 {
        let sum = u16::from(non_canonical_s[32 + index]) + u16::from(order[index]) + carry;
        non_canonical_s[32 + index] = sum as u8;
        carry = sum >> 8;
    }
    let mut small_order = vec![
        // Identity, order-2 point and an order-8 point, plus non-canonical
        // encodings (y >= p).
        common::hex("0100000000000000000000000000000000000000000000000000000000000000"),
        common::hex("ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f"),
        common::hex("c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a"),
        common::hex("26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc05"),
        common::hex("eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f"),
        common::hex("edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f"),
        common::hex("0000000000000000000000000000000000000000000000000000000000000080"),
    ];
    // A valid key with its sign bit set on a non-canonical y.
    small_order.push({
        let mut key = pair.public_key.to_vec();
        key[31] ^= 0x80;
        key
    });
    let mut cases: Vec<([u8; 32], [u8; 64])> =
        vec![(pair.public_key, valid), (pair.public_key, non_canonical_s)];
    for encoding in &small_order {
        let encoding: [u8; 32] = encoding.as_slice().try_into().unwrap();
        cases.push((encoding, valid));
        let mut r = valid;
        r[..32].copy_from_slice(&encoding);
        cases.push((pair.public_key, r));
        let mut zero_s = [0u8; 64];
        zero_s[..32].copy_from_slice(&encoding);
        cases.push((encoding, zero_s));
    }
    for (index, (public, signature)) in cases.iter().enumerate() {
        assert_eq!(
            sign::verify(public, message, signature),
            sodium_verify(public, message, signature),
            "case {index}"
        );
    }
}

/// The stream mechanism rebuilt from libsodium's XChaCha20-Poly1305 with
/// explicitly constructed STREAM nonces: prefix, u32be index, last flag.
fn sodium_stream(key: &[u8; 32], context: &[u8], prefix: &[u8; 19], plaintext: &[u8]) -> Vec<u8> {
    let header = stream::MECHANISM.header().unwrap();
    let aad = frame_record(&[&header, context]).unwrap();
    let derived = sodium_hash(key, &aad);
    let mut out = [header.as_slice(), prefix].concat();
    let mut chunks: Vec<&[u8]> = plaintext.chunks(stream::RECORD_BYTES).collect();
    chunks.push(&[]);
    let last = chunks.len() - 1;
    for (index, chunk) in chunks.into_iter().enumerate() {
        let mut nonce = [0u8; 24];
        nonce[..19].copy_from_slice(prefix);
        nonce[19..23].copy_from_slice(&(index as u32).to_be_bytes());
        nonce[23] = u8::from(index == last);
        let record = sodium_encrypt(&derived, &nonce, &aad, chunk);
        out.extend_from_slice(&(record.len() as u32).to_be_bytes());
        out.extend_from_slice(&record);
    }
    out
}

#[test]
fn stream_matches_libsodium_construction() {
    init();
    let mut rng = TestRandom(15);
    for size in [
        0,
        1,
        100,
        stream::RECORD_BYTES - 1,
        stream::RECORD_BYTES,
        stream::RECORD_BYTES + 1,
        200_000,
    ] {
        for _ in 0..3 {
            let key = rng.array::<32>();
            let context = rng.sized(60);
            let prefix = rng.array::<19>();
            let plaintext = rng.bytes(size);
            let ours = stream::encrypt(
                &key,
                &context,
                &plaintext,
                &mut FixedRandom(prefix.to_vec()),
            )
            .unwrap();
            let theirs = sodium_stream(&key, &context, &prefix, &plaintext);
            assert_eq!(ours, theirs, "size {size}");
            assert_eq!(
                *stream::decrypt(&key, &context, &theirs).unwrap(),
                plaintext
            );
        }
    }
}
