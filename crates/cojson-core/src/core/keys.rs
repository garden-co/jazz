use crate::core::CoJsonCoreError;
use ed25519_dalek::{Signature as Ed25519Signature, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

/// A unique identifier for a signer, derived from its public key.
/// Encoded as "signer_z" followed by base58-encoded public key bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SignerID(pub String);

impl From<VerifyingKey> for SignerID {
    fn from(key: VerifyingKey) -> Self {
        SignerID(format!(
            "signer_z{}",
            bs58::encode(key.to_bytes()).into_string()
        ))
    }
}

/// A secret signing key, encoded as "signerSecret_z" followed by base58-encoded private key bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SignerSecret(pub String);

impl From<SigningKey> for SignerSecret {
    fn from(key: SigningKey) -> Self {
        SignerSecret(format!(
            "signerSecret_z{}",
            bs58::encode(key.to_bytes()).into_string()
        ))
    }
}

impl TryFrom<&SignerSecret> for SigningKey {
    type Error = CoJsonCoreError;
    fn try_from(val: &SignerSecret) -> Result<Self, Self::Error> {
        let key_bytes = decode_z(&val.0)?;
        Ok(SigningKey::from_bytes(&key_bytes.try_into().map_err(
            |e: Vec<u8>| CoJsonCoreError::InvalidKeyLength(32, e.len()),
        )?))
    }
}

/// A cryptographic signature, encoded as "signature_z" followed by base58-encoded signature bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Signature(pub String);

impl From<Ed25519Signature> for Signature {
    fn from(signature: Ed25519Signature) -> Self {
        Signature(format!(
            "signature_z{}",
            bs58::encode(signature.to_bytes()).into_string()
        ))
    }
}

impl TryFrom<&Signature> for Ed25519Signature {
    type Error = CoJsonCoreError;
    fn try_from(val: &Signature) -> Result<Self, Self::Error> {
        let signature_bytes = decode_z(&val.0)?;
        Ok(Ed25519Signature::from_bytes(
            &signature_bytes
                .try_into()
                .map_err(|e: Vec<u8>| CoJsonCoreError::InvalidKeyLength(64, e.len()))?,
        ))
    }
}

/// A cryptographic hash, encoded as "hash_z" followed by base58-encoded hash bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash(pub String);

impl From<blake3::Hash> for Hash {
    fn from(hash: blake3::Hash) -> Self {
        Hash(format!(
            "hash_z{}",
            bs58::encode(hash.as_bytes()).into_string()
        ))
    }
}

/// A unique identifier for an encryption key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeyID(pub String);

/// A secret encryption key.
/// Encoded as "keySecret_z" followed by base58-encoded key bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeySecret(pub String);

impl TryFrom<&KeySecret> for [u8; 32] {
    type Error = CoJsonCoreError;
    fn try_from(val: &KeySecret) -> Result<Self, Self::Error> {
        let key_bytes = decode_z(&val.0)?;
        key_bytes
            .try_into()
            .map_err(|e: Vec<u8>| CoJsonCoreError::InvalidKeyLength(32, e.len()))
    }
}

/// A unique identifier for a CoValue.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CoID(pub String);

/// Decode a base58 string with a "_z" prefix.
/// Used for decoding keys and other encoded values.
pub(crate) fn decode_z(value: &str) -> Result<Vec<u8>, CoJsonCoreError> {
    let prefix_end = value
        .find("_z")
        .ok_or(CoJsonCoreError::InvalidDecodingPrefix)?
        + 2;
    bs58::decode(&value[prefix_end..])
        .into_vec()
        .map_err(CoJsonCoreError::InvalidBase58)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, Verifier};
    use rand_core::OsRng;

    #[test]
    fn test_signer_id_conversion() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let verifying_key = signing_key.verifying_key();

        let signer_id = SignerID::from(verifying_key);
        assert!(signer_id.0.starts_with("signer_z"));

        // Verify we can decode it back
        let decoded_bytes = decode_z(&signer_id.0).unwrap();
        let reconstructed_key = VerifyingKey::try_from(decoded_bytes.as_slice()).unwrap();
        assert_eq!(reconstructed_key, verifying_key);
    }

    #[test]
    fn test_signer_secret_conversion() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let verifying_key = signing_key.verifying_key();

        let signer_secret = SignerSecret::from(signing_key);
        assert!(signer_secret.0.starts_with("signerSecret_z"));

        // Verify we can decode it back
        let reconstructed_key = SigningKey::try_from(&signer_secret).unwrap();
        assert_eq!(reconstructed_key.verifying_key(), verifying_key);
    }

    #[test]
    fn test_signature_conversion() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let verifying_key = signing_key.verifying_key();

        let message = b"test message";
        let ed25519_signature = signing_key.sign(message);

        let signature = Signature::from(ed25519_signature);
        assert!(signature.0.starts_with("signature_z"));

        // Verify we can decode it back and verify
        let reconstructed_signature = Ed25519Signature::try_from(&signature).unwrap();
        assert!(verifying_key
            .verify(message, &reconstructed_signature)
            .is_ok());
    }

    #[test]
    fn test_hash_conversion() {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"test data");
        let blake3_hash = hasher.finalize();

        let hash = Hash::from(blake3_hash);
        assert!(hash.0.starts_with("hash_z"));

        // Verify we can decode it back
        let decoded_bytes = decode_z(&hash.0).unwrap();
        assert_eq!(decoded_bytes, blake3_hash.as_bytes());
    }
}
