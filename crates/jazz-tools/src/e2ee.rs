//! E2EE crypto core: identity derivation, space-key sealing, value encryption.
//!
//! Pure functions over byte slices; no runtime or storage coupling. Formats are
//! specified in docs/superpowers/specs/2026-06-12-e2ee-shared-keys-design.md.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use hpke::aead::ChaCha20Poly1305;
use hpke::kdf::HkdfSha256;
use hpke::kem::X25519HkdfSha256;
use hpke::{Deserializable, Kem as KemTrait, OpModeR, OpModeS, Serializable};
use hpke::{single_shot_open, single_shot_seal};
use rand::RngCore;
use rand::rngs::OsRng;
use uuid::Uuid;

use crate::identity::derive_key_material;

/// Domain string for deriving the E2EE encryption identity from the auth seed.
/// Sibling of `jazz-auth-sign-v1` in `identity.rs`.
pub const E2EE_SEAL_DOMAIN: &str = "jazz-e2ee-seal-v1";

type Kem = X25519HkdfSha256;

#[derive(Debug, PartialEq, Eq)]
pub enum E2eeError {
    MalformedPublicKey,
    MalformedSealedKey,
    MalformedEnvelope,
    UnsupportedAlgorithm(u8),
    Seal(String),
    Unseal(String),
    Encrypt(String),
    Decrypt(String),
}

impl std::fmt::Display for E2eeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            E2eeError::MalformedPublicKey => write!(f, "malformed E2EE public key"),
            E2eeError::MalformedSealedKey => write!(f, "malformed sealed key blob"),
            E2eeError::MalformedEnvelope => write!(f, "malformed ciphertext envelope"),
            E2eeError::UnsupportedAlgorithm(id) => write!(f, "unsupported algorithm id {id}"),
            E2eeError::Seal(e) => write!(f, "seal failed: {e}"),
            E2eeError::Unseal(e) => write!(f, "unseal failed: {e}"),
            E2eeError::Encrypt(e) => write!(f, "encrypt failed: {e}"),
            E2eeError::Decrypt(e) => write!(f, "decrypt failed: {e}"),
        }
    }
}

impl std::error::Error for E2eeError {}

/// X25519 public key of an E2EE identity (what apps publish to their directory).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct E2eePublicKey([u8; 32]);

impl E2eePublicKey {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn to_base64url(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0)
    }

    pub fn from_base64url(encoded: &str) -> Result<Self, E2eeError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| E2eeError::MalformedPublicKey)?;
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| E2eeError::MalformedPublicKey)?;
        Ok(Self(bytes))
    }
}

/// An E2EE identity keypair derived from the 32-byte LocalFirst Auth seed.
pub struct E2eeKeypair {
    secret: <Kem as KemTrait>::PrivateKey,
    pub public: E2eePublicKey,
}

/// Derive the E2EE keypair from a 32-byte seed.
/// Key material = `identity::derive_key_material` (same construction as the
/// signing key, different domain), fed as IKM into the HPKE KEM's deterministic
/// keypair derivation.
pub fn derive_e2ee_keypair(seed: &[u8; 32]) -> E2eeKeypair {
    let ikm = derive_key_material(seed, E2EE_SEAL_DOMAIN);
    let (secret, public) = Kem::derive_keypair(&ikm);
    let public_bytes: [u8; 32] = public
        .to_bytes()
        .as_slice()
        .try_into()
        .expect("X25519 public key is 32 bytes");
    E2eeKeypair {
        secret,
        public: E2eePublicKey(public_bytes),
    }
}

/// Algorithm id for HPKE Base mode, DHKEM(X25519) + HKDF-SHA256 + ChaCha20-Poly1305.
pub const SEALED_ALG_HPKE_X25519: u8 = 1;
/// Sealed blob layout: [alg_id: 1][encapped_key: 32][ciphertext: 32 key + 16 tag].
pub const SEALED_KEY_LEN: usize = 1 + 32 + 48;

/// 32-byte symmetric key owned by an encryption-space row.
#[derive(Clone, PartialEq, Eq)]
pub struct SpaceKey([u8; 32]);

// Redacting Debug: tests compare Result<SpaceKey, _> with assert_eq!, but key
// bytes must never end up in logs or panic messages.
impl std::fmt::Debug for SpaceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpaceKey(..)")
    }
}

impl SpaceKey {
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// Seal a space key to a recipient's public key. Output: 81-byte blob.
pub fn seal_space_key(
    recipient: &E2eePublicKey,
    space_key: &SpaceKey,
) -> Result<Vec<u8>, E2eeError> {
    let recipient_pk = <Kem as KemTrait>::PublicKey::from_bytes(recipient.as_bytes())
        .map_err(|e| E2eeError::Seal(e.to_string()))?;
    let (encapped, ciphertext) = single_shot_seal::<ChaCha20Poly1305, HkdfSha256, Kem, _>(
        &OpModeS::Base,
        &recipient_pk,
        E2EE_SEAL_DOMAIN.as_bytes(),
        space_key.as_bytes(),
        &[],
        &mut OsRng,
    )
    .map_err(|e| E2eeError::Seal(e.to_string()))?;

    let mut blob = Vec::with_capacity(SEALED_KEY_LEN);
    blob.push(SEALED_ALG_HPKE_X25519);
    blob.extend_from_slice(encapped.to_bytes().as_slice());
    blob.extend_from_slice(&ciphertext);
    debug_assert_eq!(blob.len(), SEALED_KEY_LEN);
    Ok(blob)
}

/// Unseal a space key with the recipient's keypair. Dispatches on the leading
/// algorithm id; each algorithm owns its payload layout.
pub fn unseal_space_key(keypair: &E2eeKeypair, sealed: &[u8]) -> Result<SpaceKey, E2eeError> {
    let (&alg, payload) = sealed.split_first().ok_or(E2eeError::MalformedSealedKey)?;
    match alg {
        SEALED_ALG_HPKE_X25519 => unseal_hpke_x25519(keypair, payload),
        other => Err(E2eeError::UnsupportedAlgorithm(other)),
    }
}

/// Payload layout for `SEALED_ALG_HPKE_X25519`: [encapped_key: 32][ciphertext: 48].
fn unseal_hpke_x25519(keypair: &E2eeKeypair, payload: &[u8]) -> Result<SpaceKey, E2eeError> {
    if payload.len() != SEALED_KEY_LEN - 1 {
        return Err(E2eeError::MalformedSealedKey);
    }
    let (encapped_bytes, ciphertext) = payload.split_at(32);
    let encapped = <Kem as KemTrait>::EncappedKey::from_bytes(encapped_bytes)
        .map_err(|_| E2eeError::MalformedSealedKey)?;
    let plaintext = single_shot_open::<ChaCha20Poly1305, HkdfSha256, Kem>(
        &OpModeR::Base,
        &keypair.secret,
        &encapped,
        E2EE_SEAL_DOMAIN.as_bytes(),
        ciphertext,
        &[],
    )
    .map_err(|e| E2eeError::Unseal(e.to_string()))?;
    let bytes: [u8; 32] = plaintext
        .try_into()
        .map_err(|_| E2eeError::Unseal("unexpected plaintext length".to_string()))?;
    Ok(SpaceKey::from_bytes(bytes))
}

/// Algorithm id for XChaCha20-Poly1305 value encryption.
pub const ENVELOPE_ALG_XCHACHA20POLY1305: u8 = 1;
/// Envelope layout: [alg_id: 1][key_id: 16][nonce: 24][ciphertext+tag].
pub const ENVELOPE_HEADER_LEN: usize = 1 + 16 + 24;

const AAD_DOMAIN: &str = "jazz-e2ee-aad-v1";

/// Identifies where a ciphertext lives; bound into the AAD so ciphertext cannot
/// be grafted between rows or columns.
#[derive(Debug, Clone, Copy)]
pub struct EncryptionContext<'a> {
    pub table: &'a str,
    pub column: &'a str,
    pub row_id: &'a [u8],
}

/// Length-prefixed field encoding: no two distinct (table, column, row_id, key_id)
/// tuples can produce the same AAD bytes.
fn build_aad(context: &EncryptionContext<'_>, key_id: &Uuid) -> Vec<u8> {
    let mut aad = Vec::with_capacity(
        AAD_DOMAIN.len()
            + context.table.len()
            + context.column.len()
            + context.row_id.len()
            + 16
            + 5 * 8,
    );
    for field in [
        AAD_DOMAIN.as_bytes(),
        context.table.as_bytes(),
        context.column.as_bytes(),
        context.row_id,
        key_id.as_bytes().as_slice(),
    ] {
        aad.extend_from_slice(&(field.len() as u64).to_be_bytes());
        aad.extend_from_slice(field);
    }
    aad
}

/// Encrypt a serialized column value under a space key. Output: envelope bytes.
pub fn encrypt_value(
    key: &SpaceKey,
    key_id: &Uuid,
    context: &EncryptionContext<'_>,
    plaintext: &[u8],
) -> Result<Vec<u8>, E2eeError> {
    let cipher = XChaCha20Poly1305::new(key.as_bytes().into());
    let mut nonce = [0u8; 24];
    OsRng.fill_bytes(&mut nonce);
    let aad = build_aad(context, key_id);
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|e| E2eeError::Encrypt(e.to_string()))?;

    let mut envelope = Vec::with_capacity(ENVELOPE_HEADER_LEN + ciphertext.len());
    envelope.push(ENVELOPE_ALG_XCHACHA20POLY1305);
    envelope.extend_from_slice(key_id.as_bytes());
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(&ciphertext);
    Ok(envelope)
}

/// Parsed view of an envelope; the single place that knows the header layout.
struct EnvelopeParts<'a> {
    key_id: Uuid,
    nonce: &'a [u8],
    ciphertext: &'a [u8],
}

fn parse_envelope(envelope: &[u8]) -> Result<EnvelopeParts<'_>, E2eeError> {
    if envelope.len() < ENVELOPE_HEADER_LEN {
        return Err(E2eeError::MalformedEnvelope);
    }
    let alg = envelope[0];
    if alg != ENVELOPE_ALG_XCHACHA20POLY1305 {
        return Err(E2eeError::UnsupportedAlgorithm(alg));
    }
    let key_id_bytes: [u8; 16] = envelope[1..17].try_into().expect("slice length checked");
    Ok(EnvelopeParts {
        key_id: Uuid::from_bytes(key_id_bytes),
        nonce: &envelope[17..ENVELOPE_HEADER_LEN],
        ciphertext: &envelope[ENVELOPE_HEADER_LEN..],
    })
}

/// Read the key id from an envelope without decrypting (the runtime uses this to
/// pick the right space key from its cache).
pub fn envelope_key_id(envelope: &[u8]) -> Result<Uuid, E2eeError> {
    Ok(parse_envelope(envelope)?.key_id)
}

/// Decrypt an envelope under a space key. The key id inside the envelope is part
/// of the AAD, so a swapped key id fails authentication.
pub fn decrypt_value(
    key: &SpaceKey,
    context: &EncryptionContext<'_>,
    envelope: &[u8],
) -> Result<Vec<u8>, E2eeError> {
    let EnvelopeParts {
        key_id,
        nonce,
        ciphertext,
    } = parse_envelope(envelope)?;
    let aad = build_aad(context, &key_id);
    let cipher = XChaCha20Poly1305::new(key.as_bytes().into());
    cipher
        .decrypt(
            XNonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|e| E2eeError::Decrypt(e.to_string()))
}
