//! E2EE crypto core: identity derivation, space-key sealing, value encryption.
//!
//! Pure functions over byte slices; no runtime or storage coupling. Formats are
//! specified in docs/superpowers/specs/2026-06-12-e2ee-shared-keys-design.md.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hpke::kem::X25519HkdfSha256;
use hpke::{Kem as KemTrait, Serializable};
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
    #[allow(dead_code)]
    secret: <Kem as KemTrait>::PrivateKey,
    pub public: E2eePublicKey,
}

impl E2eeKeypair {
    #[allow(dead_code)]
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
