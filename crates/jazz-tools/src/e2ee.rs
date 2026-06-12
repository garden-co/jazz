//! E2EE crypto core: identity derivation, space-key sealing, value encryption.
//!
//! Pure functions over byte slices; no runtime or storage coupling. Formats are
//! specified in docs/superpowers/specs/2026-06-12-e2ee-shared-keys-design.md.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hpke::aead::ChaCha20Poly1305;
use hpke::kdf::HkdfSha256;
use hpke::kem::X25519HkdfSha256;
use hpke::{Deserializable, Kem as KemTrait, OpModeR, OpModeS, Serializable};
use hpke::{single_shot_open, single_shot_seal};
use rand::RngCore;
use rand::rngs::OsRng;
use sha2::{Digest, Sha512};

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

impl E2eeKeypair {
    fn secret(&self) -> &<Kem as KemTrait>::PrivateKey {
        &self.secret
    }
}

/// Derive the E2EE keypair from a 32-byte seed.
/// Key material = SHA-512(domain || seed)[..32], mirroring `identity::derive_signing_key`,
/// fed as IKM into the HPKE KEM's deterministic keypair derivation.
pub fn derive_e2ee_keypair(seed: &[u8; 32]) -> E2eeKeypair {
    let mut hasher = Sha512::new();
    hasher.update(E2EE_SEAL_DOMAIN.as_bytes());
    hasher.update(seed);
    let hash = hasher.finalize();
    let ikm: [u8; 32] = hash[..32].try_into().expect("SHA-512 output is 64 bytes");
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

/// Unseal a space key with the recipient's keypair.
pub fn unseal_space_key(keypair: &E2eeKeypair, sealed: &[u8]) -> Result<SpaceKey, E2eeError> {
    if sealed.is_empty() {
        return Err(E2eeError::MalformedSealedKey);
    }
    let alg = sealed[0];
    if alg != SEALED_ALG_HPKE_X25519 {
        // Length check only applies to known layouts; unknown alg wins when long
        // enough to plausibly carry one, otherwise report malformed.
        if sealed.len() == SEALED_KEY_LEN {
            return Err(E2eeError::UnsupportedAlgorithm(alg));
        }
        return Err(E2eeError::MalformedSealedKey);
    }
    if sealed.len() != SEALED_KEY_LEN {
        return Err(E2eeError::MalformedSealedKey);
    }
    let encapped = <Kem as KemTrait>::EncappedKey::from_bytes(&sealed[1..33])
        .map_err(|_| E2eeError::MalformedSealedKey)?;
    let plaintext = single_shot_open::<ChaCha20Poly1305, HkdfSha256, Kem>(
        &OpModeR::Base,
        keypair.secret(),
        &encapped,
        E2EE_SEAL_DOMAIN.as_bytes(),
        &sealed[33..],
        &[],
    )
    .map_err(|e| E2eeError::Unseal(e.to_string()))?;
    let bytes: [u8; 32] = plaintext
        .try_into()
        .map_err(|_| E2eeError::Unseal("unexpected plaintext length".to_string()))?;
    Ok(SpaceKey::from_bytes(bytes))
}
