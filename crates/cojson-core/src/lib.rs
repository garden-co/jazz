use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use bs58;
use ed25519_dalek::{Signature as Ed25519Signature, Signer, SigningKey, Verifier, VerifyingKey};
use salsa20::{
    cipher::{KeyIvInit, StreamCipher},
    XSalsa20,
};
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Number, Value as JsonValue};
use thiserror::Error;

// Re-export lzy for convenience
#[cfg(feature = "lzy")]
pub use lzy;

// Re-export crypto modules
pub mod crypto;
pub mod error;
pub mod hash;

// Re-export specific functions for convenience
pub use crypto::seal::{seal_internal, unseal_internal};
pub use crypto::x25519::{x25519_public_key, x25519_diffie_hellman, get_sealer_id_internal};
pub use crypto::xsalsa20::{encrypt_xsalsa20, decrypt_xsalsa20, encrypt_xsalsa20_poly1305, decrypt_xsalsa20_poly1305};
pub use error::CryptoError;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionID(pub String);

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

impl Into<SigningKey> for &SignerSecret {
    fn into(self) -> SigningKey {
        let key_bytes = decode_z(&self.0).expect("Invalid key secret");
        SigningKey::from_bytes(&key_bytes.try_into().expect("Invalid key secret length"))
    }
}

impl SignerSecret {
    /// Safely convert SignerSecret to SigningKey with proper error handling
    pub fn try_into_signing_key(&self) -> Result<SigningKey, CoJsonCoreError> {
        // Handle both formats: "signerSecret_z..." and "sealerSecret_z.../signerSecret_z..."
        let signer_secret_part = if self.0.contains('/') {
            // AgentSecret format: "sealerSecret_z.../signerSecret_z..."
            let parts: Vec<&str> = self.0.split('/').collect();
            if parts.len() != 2 {
                return Err(CoJsonCoreError::InvalidSignerSecret(format!(
                    "AgentSecret must have exactly 2 parts separated by '/', got: '{}'",
                    self.0
                )));
            }
            parts[1] // Take the signerSecret part
        } else {
            // Direct SignerSecret format: "signerSecret_z..."
            &self.0
        };

        if !signer_secret_part.starts_with("signerSecret_z") {
            return Err(CoJsonCoreError::InvalidSignerSecret(format!(
                "signer part must start with 'signerSecret_z', got: '{}'",
                signer_secret_part
            )));
        }

        let base58_part = &signer_secret_part["signerSecret_z".len()..];
        let key_bytes = bs58::decode(base58_part).into_vec()?;

        if key_bytes.len() != 32 {
            return Err(CoJsonCoreError::InvalidSignerSecret(format!(
                "expected 32 bytes, got {}",
                key_bytes.len()
            )));
        }

        let key_array: [u8; 32] = key_bytes.try_into().map_err(|_| {
            CoJsonCoreError::InvalidSignerSecret("failed to convert to 32-byte array".to_string())
        })?;

        // SigningKey::from_bytes can panic, so we wrap it in a catch_unwind for safety
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            SigningKey::from_bytes(&key_array)
        }))
        .map_err(|_| CoJsonCoreError::InvalidSignerSecret("Invalid signing key bytes".to_string()))
    }
}

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

impl Into<Ed25519Signature> for &Signature {
    fn into(self) -> Ed25519Signature {
        let signature_bytes = decode_z(&self.0).expect("Invalid signature");
        Ed25519Signature::from_bytes(
            &signature_bytes
                .try_into()
                .expect("Invalid signature length"),
        )
    }
}

impl Signature {
    /// Safely convert Signature to Ed25519Signature with proper error handling
    pub fn try_into_ed25519_signature(&self) -> Result<Ed25519Signature, CoJsonCoreError> {
        let signature_bytes = decode_z(&self.0).map_err(|e| {
            CoJsonCoreError::InvalidSignerSecret(format!("Invalid signature format: {}", e))
        })?;

        if signature_bytes.len() != 64 {
            return Err(CoJsonCoreError::InvalidSignerSecret(format!(
                "Invalid signature length: expected 64 bytes, got {}",
                signature_bytes.len()
            )));
        }

        let signature_array: [u8; 64] = signature_bytes.try_into().map_err(|_| {
            CoJsonCoreError::InvalidSignerSecret("failed to convert to 64-byte array".to_string())
        })?;

        Ok(Ed25519Signature::from_bytes(&signature_array))
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeyID(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeySecret(pub String);

impl Into<[u8; 32]> for &KeySecret {
    fn into(self) -> [u8; 32] {
        let key_bytes = decode_z(&self.0).expect("Invalid key secret");
        key_bytes.try_into().expect("Invalid key secret length")
    }
}

impl KeySecret {
    /// Safely convert KeySecret to [u8; 32] with proper error handling
    pub fn try_into_bytes(&self) -> Result<[u8; 32], CoJsonCoreError> {
        let key_bytes = decode_z(&self.0).map_err(|e| {
            CoJsonCoreError::InvalidKeySecret(format!("base58 decode failed: {}", e))
        })?;

        if key_bytes.len() != 32 {
            return Err(CoJsonCoreError::InvalidKeySecret(format!(
                "expected 32 bytes, got {}",
                key_bytes.len()
            )));
        }

        key_bytes.try_into().map_err(|_| {
            CoJsonCoreError::InvalidKeySecret("failed to convert to 32-byte array".to_string())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CoID(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionID {
    #[serde(rename = "sessionID")]
    pub session_id: SessionID,
    #[serde(rename = "txIndex")]
    pub tx_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Encrypted<T> {
    pub value: String,
    _phantom: std::marker::PhantomData<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateTransaction {
    #[serde(rename = "encryptedChanges")]
    pub encrypted_changes: Encrypted<JsonValue>,
    #[serde(rename = "keyUsed")]
    pub key_used: KeyID,
    #[serde(rename = "madeAt")]
    pub made_at: Number,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Encrypted<JsonValue>>,
    pub privacy: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustingTransaction {
    pub changes: String,
    #[serde(rename = "madeAt")]
    pub made_at: Number,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<String>,
    pub privacy: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Transaction {
    Private(PrivateTransaction),
    Trusting(TrustingTransaction),
}

pub enum TransactionMode {
    Private {
        key_id: KeyID,
        key_secret: KeySecret,
    },
    Trusting,
}

#[derive(Error, Debug)]
pub enum CoJsonCoreError {
    #[error("Transaction not found at index {0}")]
    TransactionNotFound(u32),

    #[error("Invalid encrypted prefix in transaction")]
    InvalidEncryptedPrefix,

    #[error("Base64 decoding failed")]
    Base64Decode(#[from] base64::DecodeError),

    #[error("UTF-8 conversion failed")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("JSON deserialization failed")]
    Json(#[from] serde_json::Error),

    #[error("Signature verification failed: (hash: {0})")]
    SignatureVerification(String),

    #[error("Invalid signer secret format: {0}")]
    InvalidSignerSecret(String),

    #[error("Invalid signer ID format: {0}")]
    InvalidSignerID(String),

    #[error("Invalid key secret format: {0}")]
    InvalidKeySecret(String),

    #[error("Base58 decoding failed: {0}")]
    Base58Decode(#[from] bs58::decode::Error),

    #[error("Ed25519 key error: {0}")]
    Ed25519Key(#[from] ed25519_dalek::SignatureError),
}

#[derive(Clone)]
pub struct SessionLogInternal {
    co_id: CoID,
    session_id: SessionID,
    public_key: Option<VerifyingKey>,
    hasher: blake3::Hasher,
    transactions_json: Vec<String>,
    last_signature: Option<Signature>,
}

impl SessionLogInternal {
    pub fn new(
        co_id: CoID,
        session_id: SessionID,
        signer_id: Option<SignerID>,
    ) -> Result<Self, CoJsonCoreError> {
        let hasher = blake3::Hasher::new();

        let (public_key, last_signature) = match signer_id {
            Some(signer_id) => match Self::validate_and_extract_public_key(signer_id) {
                Ok(key) => (Some(key), None),
                Err(e) => return Err(e),
            },
            None => (None, None),
        };

        Ok(SessionLogInternal {
            co_id,
            session_id,
            public_key,
            last_signature,
            hasher,
            transactions_json: Vec::new(),
        })
    }

    /// Convenience method to create a session without signature verification
    pub fn new_unsigned(co_id: CoID, session_id: SessionID) -> Result<Self, CoJsonCoreError> {
        Self::new(co_id, session_id, None)
    }

    /// Validate and extract public key from SignerID
    fn validate_and_extract_public_key(
        signer_id: SignerID,
    ) -> Result<VerifyingKey, CoJsonCoreError> {
        // Validate signer_id is not empty
        if signer_id.0.is_empty() {
            return Err(CoJsonCoreError::InvalidSignerID(
                "SignerID cannot be empty".to_string(),
            ));
        }

        // Validate signer_id has correct format and prefix
        if !signer_id.0.starts_with("signer_z") {
            return Err(CoJsonCoreError::InvalidSignerID(format!(
                "Invalid SignerID format: '{}' - must start with 'signer_z'",
                signer_id.0
            )));
        }

        let decoded_bytes = decode_z(&signer_id.0).map_err(|e| {
            CoJsonCoreError::InvalidSignerID(format!(
                "Failed to decode SignerID '{}': {}",
                signer_id.0, e
            ))
        })?;

        if decoded_bytes.len() != 32 {
            return Err(CoJsonCoreError::InvalidSignerID(format!(
                "Invalid public key length in SignerID '{}': expected 32 bytes, got {}",
                signer_id.0,
                decoded_bytes.len()
            )));
        }

        VerifyingKey::try_from(decoded_bytes.as_slice()).map_err(|e| {
            CoJsonCoreError::InvalidSignerID(format!(
                "Invalid public key in SignerID '{}': {}",
                signer_id.0, e
            ))
        })
    }

    pub fn transactions_json(&self) -> &Vec<String> {
        &self.transactions_json
    }

    pub fn last_signature(&self) -> Option<&Signature> {
        self.last_signature.as_ref()
    }

    fn expected_hash_after(&self, transactions: &[Box<RawValue>]) -> blake3::Hash {
        let mut hasher = self.hasher.clone();
        for tx in transactions {
            hasher.update(tx.get().as_bytes());
        }

        hasher.finalize()
    }

    pub fn test_expected_hash_after(&self, transactions: &[Box<RawValue>]) -> String {
        let new_hash = self.expected_hash_after(transactions);
        format!("hash_z{}", bs58::encode(new_hash.as_bytes()).into_string())
    }

    pub fn try_add(
        &mut self,
        transactions: Vec<Box<RawValue>>,
        new_signature: &Signature,
        skip_verify: bool,
    ) -> Result<(), CoJsonCoreError> {
        if !skip_verify {
            let new_hash = self.expected_hash_after(&transactions);
            let new_hash_encoded_stringified = format!(
                "\"hash_z{}\"",
                bs58::encode(new_hash.as_bytes()).into_string()
            );

            match &self.public_key {
                Some(public_key) => {
                    let ed25519_signature = new_signature.try_into_ed25519_signature()
                        .map_err(|_| CoJsonCoreError::SignatureVerification(
                            new_hash_encoded_stringified.replace("\"", "")
                        ))?;
                    
                    match public_key.verify(
                        new_hash_encoded_stringified.as_bytes(),
                        &ed25519_signature,
                    ) {
                        Ok(()) => {}
                        Err(_) => {
                            return Err(CoJsonCoreError::SignatureVerification(
                                new_hash_encoded_stringified.replace("\"", ""),
                            ));
                        }
                    }
                }
                None => {
                    return Err(CoJsonCoreError::InvalidSignerID(
                        "No public key available for signature verification".to_string(),
                    ));
                }
            }
        }

        for tx in transactions {
            let tx_json = tx.get();
            self.hasher.update(tx_json.as_bytes());
            self.transactions_json.push(tx_json.to_string());
        }

        self.last_signature = Some(new_signature.clone());

        Ok(())
    }

    pub fn add_new_transaction(
        &mut self,
        changes_json: &str,
        mode: TransactionMode,
        signer_secret: &SignerSecret,
        made_at: u64,
        meta: Option<String>,
    ) -> (Signature, Transaction) {
        // Use the fallible version and panic on error to maintain API compatibility
        self.try_add_new_transaction(changes_json, mode, signer_secret, made_at, meta)
            .expect("Transaction creation failed")
    }

    /// Safely create a new transaction with proper error handling
    pub fn try_add_new_transaction(
        &mut self,
        changes_json: &str,
        mode: TransactionMode,
        signer_secret: &SignerSecret,
        made_at: u64,
        meta: Option<String>,
    ) -> Result<(Signature, Transaction), CoJsonCoreError> {
        let new_tx = match mode {
            TransactionMode::Private { key_id, key_secret } => {
                let tx_index = self.transactions_json.len() as u32;

                let nonce_material = self.generate_nonce_material(tx_index);

                let nonce = self.try_generate_json_nonce(&nonce_material)?;

                let secret_key_bytes = key_secret.try_into_bytes()?;

                let mut ciphertext = changes_json.as_bytes().to_vec();
                let mut cipher = XSalsa20::new(&secret_key_bytes.into(), &nonce.into());
                cipher.apply_keystream(&mut ciphertext);
                let encrypted_str = format!("encrypted_U{}", URL_SAFE.encode(&ciphertext));

                let encrypted_meta = meta.map(|meta| {
                    let mut ciphertext = meta.as_bytes().to_vec();
                    let mut cipher = XSalsa20::new(&secret_key_bytes.into(), &nonce.into());
                    cipher.apply_keystream(&mut ciphertext);

                    let encrypted_meta = format!("encrypted_U{}", URL_SAFE.encode(&ciphertext));

                    return Encrypted {
                        value: encrypted_meta,
                        _phantom: std::marker::PhantomData,
                    };
                });

                Transaction::Private(PrivateTransaction {
                    encrypted_changes: Encrypted {
                        value: encrypted_str,
                        _phantom: std::marker::PhantomData,
                    },
                    key_used: key_id.clone(),
                    made_at: Number::from(made_at),
                    meta: encrypted_meta,
                    privacy: "private".to_string(),
                })
            }
            TransactionMode::Trusting => Transaction::Trusting(TrustingTransaction {
                changes: changes_json.to_string(),
                made_at: Number::from(made_at),
                meta,
                privacy: "trusting".to_string(),
            }),
        };

        let tx_json = serde_json::to_string(&new_tx)?;
        self.hasher.update(tx_json.as_bytes());
        self.transactions_json.push(tx_json);

        let new_hash = self.hasher.finalize();
        let new_hash_encoded_stringified = format!(
            "\"hash_z{}\"",
            bs58::encode(new_hash.as_bytes()).into_string()
        );
        let signing_key = signer_secret.try_into_signing_key()?;
        let new_signature: Signature = signing_key
            .sign(new_hash_encoded_stringified.as_bytes())
            .into();

        self.last_signature = Some(new_signature.clone());

        Ok((new_signature, new_tx))
    }

    pub fn decrypt_next_transaction_changes_json(
        &self,
        tx_index: u32,
        key_secret: KeySecret,
    ) -> Result<String, CoJsonCoreError> {
        let tx_json = self
            .transactions_json
            .get(tx_index as usize)
            .ok_or(CoJsonCoreError::TransactionNotFound(tx_index))?;
        let tx: Transaction = serde_json::from_str(tx_json)?;

        match tx {
            Transaction::Private(private_tx) => {
                let nonce_material = self.generate_nonce_material(tx_index);
                let nonce = self.try_generate_json_nonce(&nonce_material)?;

                let encrypted_val = private_tx.encrypted_changes.value;
                let prefix = "encrypted_U";
                if !encrypted_val.starts_with(prefix) {
                    return Err(CoJsonCoreError::InvalidEncryptedPrefix);
                }

                let ciphertext_b64 = &encrypted_val[prefix.len()..];
                let mut ciphertext = URL_SAFE.decode(ciphertext_b64)?;

                let secret_key_bytes = key_secret.try_into_bytes()?;
                let mut cipher = XSalsa20::new((&secret_key_bytes).into(), &nonce.into());
                cipher.apply_keystream(&mut ciphertext);

                Ok(String::from_utf8(ciphertext)?)
            }
            Transaction::Trusting(trusting_tx) => Ok(trusting_tx.changes),
        }
    }

    pub fn decrypt_next_transaction_meta_json(
        &self,
        tx_index: u32,
        key_secret: KeySecret,
    ) -> Result<Option<String>, CoJsonCoreError> {
        let tx_json = self
            .transactions_json
            .get(tx_index as usize)
            .ok_or(CoJsonCoreError::TransactionNotFound(tx_index))?;
        let tx: Transaction = serde_json::from_str(tx_json)?;

        match tx {
            Transaction::Private(private_tx) => {
                if let Some(encrypted_meta) = private_tx.meta {
                    let nonce_material = self.generate_nonce_material(tx_index);
                    let nonce = self.try_generate_json_nonce(&nonce_material)?;

                    let encrypted_val = encrypted_meta.value;
                    let prefix = "encrypted_U";
                    if !encrypted_val.starts_with(prefix) {
                        return Err(CoJsonCoreError::InvalidEncryptedPrefix);
                    }

                    let ciphertext_b64 = &encrypted_val[prefix.len()..];
                    let mut ciphertext = URL_SAFE.decode(ciphertext_b64)?;

                    let secret_key_bytes = key_secret.try_into_bytes()?;
                    let mut cipher = XSalsa20::new((&secret_key_bytes).into(), &nonce.into());
                    cipher.apply_keystream(&mut ciphertext);

                    Ok(Some(String::from_utf8(ciphertext)?))
                } else {
                    Ok(None)
                }
            }
            Transaction::Trusting(trusting_tx) => Ok(trusting_tx.meta),
        }
    }

    fn generate_nonce_material(&self, tx_index: u32) -> JsonValue {
        let nonce_material = JsonValue::Object(serde_json::Map::from_iter(vec![
            ("in".to_string(), JsonValue::String(self.co_id.0.clone())),
            (
                "tx".to_string(),
                serde_json::to_value(TransactionID {
                    session_id: self.session_id.clone(),
                    tx_index,
                })
                .expect("TransactionID serialization should not fail"),
            ),
        ]));

        return nonce_material;
    }

    fn generate_nonce(&self, material: &[u8]) -> [u8; 24] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(material);
        let mut output = [0u8; 24];
        let mut output_reader = hasher.finalize_xof();
        output_reader.fill(&mut output);
        output
    }

    /// Safely generate JSON nonce with proper error handling
    fn try_generate_json_nonce(&self, material: &JsonValue) -> Result<[u8; 24], CoJsonCoreError> {
        let stable_json = serde_json::to_string(&material)?;
        Ok(self.generate_nonce(stable_json.as_bytes()))
    }
}

pub fn decode_z(value: &str) -> Result<Vec<u8>, String> {
    let prefix_end = value.find("_z").ok_or("Invalid prefix")? + 2;
    bs58::decode(&value[prefix_end..])
        .into_vec()
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand_core::OsRng;
    use std::{collections::HashMap, fs};

    #[test]
    fn it_works() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let verifying_key = signing_key.verifying_key();

        let session = SessionLogInternal::new(
            CoID("co_test1".to_string()),
            SessionID("session_test1".to_string()),
            Some(verifying_key.into()),
        )
        .unwrap();

        assert!(session.last_signature.is_none());
    }

    #[test]
    fn test_add_from_example_json() {
        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        struct TestSession<'a> {
            last_signature: Signature,
            #[serde(borrow)]
            transactions: Vec<&'a RawValue>,
            last_hash: String,
        }

        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        struct Root<'a> {
            #[serde(borrow)]
            example_base: HashMap<String, TestSession<'a>>,
            #[serde(rename = "signerID")]
            signer_id: SignerID,
        }

        let data = fs::read_to_string("data/singleTxSession.json")
            .expect("Unable to read singleTxSession.json");
        let root: Root = serde_json::from_str(&data).unwrap();

        let (session_id_str, example) = root.example_base.into_iter().next().unwrap();
        let session_id = SessionID(session_id_str.clone());
        let co_id = CoID(
            session_id_str
                .split("_session_")
                .next()
                .unwrap()
                .to_string(),
        );

        let mut session = SessionLogInternal::new(co_id, session_id, Some(root.signer_id)).unwrap();

        let new_signature = example.last_signature;
        let transactions_vec: Vec<Box<RawValue>> = example
            .transactions
            .into_iter()
            .map(|tx| tx.to_owned())
            .collect();

        let result = session.try_add(transactions_vec, &new_signature, true);

        match result {
            Ok(_) => {
                let final_hash = session.hasher.finalize();
                let final_hash_encoded = format!(
                    "hash_z{}",
                    bs58::encode(final_hash.as_bytes()).into_string()
                );

                assert_eq!(final_hash_encoded, example.last_hash);
                assert_eq!(session.last_signature, Some(new_signature));
            }
            Err(CoJsonCoreError::SignatureVerification(new_hash_encoded)) => {
                assert_eq!(new_hash_encoded, example.last_hash);
                panic!("Signature verification failed despite same hash");
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_add_from_example_json_multi_tx() {
        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        struct TestSession<'a> {
            last_signature: Signature,
            #[serde(borrow)]
            transactions: Vec<&'a RawValue>,
            last_hash: String,
        }

        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        struct Root<'a> {
            #[serde(borrow)]
            example_base: HashMap<String, TestSession<'a>>,
            #[serde(rename = "signerID")]
            signer_id: SignerID,
        }

        let data = fs::read_to_string("data/multiTxSession.json")
            .expect("Unable to read multiTxSession.json");
        let root: Root = serde_json::from_str(&data).unwrap();

        let (session_id_str, example) = root.example_base.into_iter().next().unwrap();
        let session_id = SessionID(session_id_str.clone());
        let co_id = CoID(
            session_id_str
                .split("_session_")
                .next()
                .unwrap()
                .to_string(),
        );

        let mut session = SessionLogInternal::new(co_id, session_id, Some(root.signer_id)).unwrap();

        let new_signature = example.last_signature;
        let transactions_vec: Vec<Box<RawValue>> = example
            .transactions
            .into_iter()
            .map(|tx| tx.to_owned())
            .collect();

        let result = session.try_add(transactions_vec, &new_signature, true);

        match result {
            Ok(_) => {
                let final_hash = session.hasher.finalize();
                let final_hash_encoded = format!(
                    "hash_z{}",
                    bs58::encode(final_hash.as_bytes()).into_string()
                );

                assert_eq!(final_hash_encoded, example.last_hash);
                assert_eq!(session.last_signature, Some(new_signature));
            }
            Err(CoJsonCoreError::SignatureVerification(new_hash_encoded)) => {
                assert_eq!(new_hash_encoded, example.last_hash);
                panic!("Signature verification failed despite same hash");
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_add_new_transaction() {
        // Load the example data to get all the pieces we need
        let data = fs::read_to_string("data/singleTxSession.json")
            .expect("Unable to read singleTxSession.json");
        let root: serde_json::Value = serde_json::from_str(&data).unwrap();
        let session_data =
            &root["exampleBase"]["co_zkNajJ1BhLzR962jpzvXxx917ZB_session_zXzrQLTtp8rR"];
        let tx_from_example = &session_data["transactions"][0];
        let known_key = &root["knownKeys"][0];

        // Since we don't have the original private key, we generate a new one for this test.
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let public_key = signing_key.verifying_key();

        // Initialize an empty session
        let mut session = SessionLogInternal::new(
            CoID(root["coID"].as_str().unwrap().to_string()),
            SessionID("co_zkNajJ1BhLzR962jpzvXxx917ZB_session_zXzrQLTtp8rR".to_string()),
            Some(public_key.into()),
        )
        .unwrap();

        // The plaintext changes we want to add
        let changes_json =
            r#"[{"after":"start","op":"app","value":"co_zMphsnYN6GU8nn2HDY5suvyGufY"}]"#;

        // Extract all the necessary components from the example data
        let key_secret = KeySecret(known_key["secret"].as_str().unwrap().to_string());
        let key_id = KeyID(known_key["id"].as_str().unwrap().to_string());
        let made_at = tx_from_example["madeAt"].as_u64().unwrap();

        // Call the function we are testing
        let (new_signature, _new_tx) = session.add_new_transaction(
            changes_json,
            TransactionMode::Private {
                key_id: key_id,
                key_secret: key_secret,
            },
            &signing_key.into(),
            made_at,
            None,
        );

        // 1. Check that we successfully created a transaction
        assert_eq!(session.transactions_json.len(), 1);
        let created_tx_json = &session.transactions_json[0];
        
        // Parse the created transaction to verify it has the expected structure
        let created_tx: serde_json::Value = serde_json::from_str(created_tx_json).unwrap();
        assert!(created_tx.get("encryptedChanges").is_some());
        assert!(created_tx.get("keyUsed").is_some());
        assert!(created_tx.get("madeAt").is_some());
        assert_eq!(created_tx["privacy"], "private");

        // 2. Check that we have a valid final hash
        let final_hash = session.hasher.finalize();
        let final_hash_encoded = format!(
            "hash_z{}",
            bs58::encode(final_hash.as_bytes()).into_string()
        );
        // Note: We don't compare against example data since we're using a different signing key

        let final_hash_encoded_stringified = format!("\"{}\"", final_hash_encoded);

        // 3. Check that the signature is valid for our generated key
        assert!(session
            .public_key
            .expect("Public key should be available")
            .verify(
                final_hash_encoded_stringified.as_bytes(),
                &(&new_signature).into()
            )
            .is_ok());
        assert_eq!(session.last_signature, Some(new_signature.clone()));

        let mut session2 = SessionLogInternal::new(
            CoID(root["coID"].as_str().unwrap().to_string()),
            SessionID("co_zkNajJ1BhLzR962jpzvXxx917ZB_session_zXzrQLTtp8rR".to_string()),
            Some(public_key.into()),
        ).expect("Failed to create SessionLogInternal in test");

        session2
            .try_add(
                vec![serde_json::from_str(&created_tx_json).unwrap()],
                &new_signature,
                false,
            )
            .unwrap();

        assert_eq!(session2.transactions_json, session.transactions_json);
    }

    #[test]
    fn test_decrypt_from_example_json() {
        #[derive(Deserialize, Debug)]
        struct KnownKey {
            secret: String,
        }

        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        #[serde(bound(deserialize = "'de: 'a"))]
        struct TestSession<'a> {
            last_signature: String,
            #[serde(borrow)]
            transactions: Vec<&'a RawValue>,
        }

        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        #[serde(bound(deserialize = "'de: 'a"))]
        struct Root<'a> {
            #[serde(borrow)]
            example_base: HashMap<String, TestSession<'a>>,
            #[serde(rename = "signerID")]
            signer_id: SignerID,
            known_keys: Vec<KnownKey>,
            #[serde(rename = "coID")]
            co_id: CoID,
        }

        let data = fs::read_to_string("data/singleTxSession.json")
            .expect("Unable to read singleTxSession.json");
        let root: Root = serde_json::from_str(&data).unwrap();

        let (session_id_str, example) = root.example_base.into_iter().next().unwrap();
        let session_id = SessionID(session_id_str.clone());

        let public_key =
            VerifyingKey::from_bytes(&decode_z(&root.signer_id.0).unwrap().try_into().unwrap())
                .unwrap();

        let mut session =
            SessionLogInternal::new(root.co_id, session_id, Some(public_key.into())).unwrap();

        let new_signature = Signature(example.last_signature);

        session
            .try_add(
                example
                    .transactions
                    .into_iter()
                    .map(|v| v.to_owned())
                    .collect(),
                &new_signature,
                true, // Skipping verification because we don't have the right initial state
            )
            .unwrap();

        let key_secret = KeySecret(root.known_keys[0].secret.clone());

        let decrypted = session
            .decrypt_next_transaction_changes_json(0, key_secret)
            .unwrap();

        assert_eq!(
            decrypted,
            r#"[{"after":"start","op":"app","value":"co_zMphsnYN6GU8nn2HDY5suvyGufY"}]"#
        );
    }

    #[test]
    fn test_signer_id_validation() {
        let co_id = CoID("co_test".to_string());
        let session_id = SessionID("session_test".to_string());

        // Test empty SignerID
        let result = SessionLogInternal::new(
            co_id.clone(),
            session_id.clone(),
            Some(SignerID("".to_string())),
        );
        assert!(matches!(result, Err(CoJsonCoreError::InvalidSignerID(_))));

        // Test invalid prefix
        let result = SessionLogInternal::new(
            co_id.clone(),
            session_id.clone(),
            Some(SignerID("invalid_prefix".to_string())),
        );
        assert!(matches!(result, Err(CoJsonCoreError::InvalidSignerID(_))));

        // Test invalid base58
        let result = SessionLogInternal::new(
            co_id.clone(),
            session_id.clone(),
            Some(SignerID("signer_z0OIl".to_string())), // Invalid base58 characters
        );
        assert!(matches!(result, Err(CoJsonCoreError::InvalidSignerID(_))));

        // Test None signer_id (should succeed)
        let result = SessionLogInternal::new(co_id.clone(), session_id.clone(), None);
        assert!(result.is_ok());
        let session = result.unwrap();
        assert!(session.last_signature.is_none());

        // Test new_unsigned convenience method
        let result = SessionLogInternal::new_unsigned(co_id, session_id);
        assert!(result.is_ok());
        let session = result.unwrap();
        assert!(session.last_signature.is_none());
    }

    #[test]
    fn test_valid_signer_id() {
        use rand_core::OsRng;

        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let verifying_key = signing_key.verifying_key();
        let signer_id: SignerID = verifying_key.into();

        let co_id = CoID("co_test".to_string());
        let session_id = SessionID("session_test".to_string());

        // Test valid SignerID
        let result = SessionLogInternal::new(co_id, session_id, Some(signer_id));
        assert!(result.is_ok());

        let session = result.unwrap();
        assert_eq!(
            session.public_key.expect("Public key should be available"),
            verifying_key
        );
        assert!(session.last_signature.is_none()); // New session should not have a signature yet
    }

    #[test]
    fn test_signature_verification_without_public_key() {
        use rand_core::OsRng;
        use serde_json::value::RawValue;

        let co_id = CoID("co_test".to_string());
        let session_id = SessionID("session_test".to_string());

        // Create session without signer_id
        let mut session = SessionLogInternal::new_unsigned(co_id, session_id).unwrap();
        assert!(session.last_signature.is_none());

        // Try to add a transaction with signature verification (should fail)
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let dummy_signature: Signature = signing_key.sign(b"dummy").into();

        let tx_json = r#"{"changes":"test","madeAt":123,"privacy":"trusting"}"#;
        let raw_tx = RawValue::from_string(tx_json.to_string()).unwrap();

        let result = session.try_add(vec![raw_tx], &dummy_signature, false);
        assert!(matches!(result, Err(CoJsonCoreError::InvalidSignerID(_))));

        // But should succeed with skip_verify=true
        let tx_json2 = r#"{"changes":"test2","madeAt":124,"privacy":"trusting"}"#;
        let raw_tx2 = RawValue::from_string(tx_json2.to_string()).unwrap();

        let result = session.try_add(vec![raw_tx2], &dummy_signature, true);
        assert!(result.is_ok());
    }
}
