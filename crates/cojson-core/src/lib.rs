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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash(pub String);

impl From<blake3::Hash> for Hash {
    fn from(hash: blake3::Hash) -> Self {
        Hash(format!("hash_z{}", bs58::encode(hash.as_bytes()).into_string()))
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
    pub privacy: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustingTransaction {
    pub changes: String,
    #[serde(rename = "madeAt")]
    pub made_at: Number,
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
}

#[derive(Clone)]
pub struct SessionLogInternal {
    co_id: CoID,
    session_id: SessionID,
    public_key: VerifyingKey,
    hasher: blake3::Hasher,
    transactions_json: Vec<String>,
    last_signature: Option<Signature>,
}

impl SessionLogInternal {
    pub fn new(co_id: CoID, session_id: SessionID, signer_id: SignerID) -> Self {
        let hasher = blake3::Hasher::new();

        let public_key = VerifyingKey::try_from(
            decode_z(&signer_id.0)
                .expect("Invalid public key")
                .as_slice(),
        )
        .expect("Invalid public key");

        Self {
            co_id,
            session_id,
            public_key,
            hasher,
            transactions_json: Vec::new(),
            last_signature: None,
        }
    }

    pub fn transactions_json(&self) -> &Vec<String> {
        &self.transactions_json
    }

    pub fn last_signature(&self) -> Option<&Signature> {
        self.last_signature.as_ref()
    }

    fn expected_hash_after(&self, transactions: &[Box<RawValue>]) -> blake3::Hash {
        let mut hasher = self.hasher.clone();
        println!("Initial hash: {}", format!(
            "\"hash_z{}\"",
            bs58::encode(hasher.finalize().as_bytes()).into_string()
        ));
        for tx in transactions {
            hasher.update(tx.get().as_bytes());
            println!("incremental: {} {}", format!(
                "\"hash_z{}\"",
                bs58::encode(hasher.finalize().as_bytes()).into_string()
            ), tx.get());
        }

        hasher.finalize()
    }

    pub fn test_expected_hash_after(&self, transactions: &[Box<RawValue>]) -> String {
        let new_hash = self.expected_hash_after(transactions);
        format!(
            "hash_z{}",
            bs58::encode(new_hash.as_bytes()).into_string()
        )
    }

    pub fn try_add(
        &mut self,
        transactions: Vec<Box<RawValue>>,
        new_signature: &Signature,
        skip_verify: bool,
    ) -> Result<Hash, CoJsonCoreError> {
        if !skip_verify {
            let new_hash = self.expected_hash_after(&transactions);
            let new_hash_encoded_stringified = format!(
                "\"hash_z{}\"",
                bs58::encode(new_hash.as_bytes()).into_string()
            );

            match self.public_key.verify(
                new_hash_encoded_stringified.as_bytes(),
                &(new_signature).into(),
            ) {
                Ok(()) => {}
                Err(_) => {
                    return Err(CoJsonCoreError::SignatureVerification(
                        new_hash_encoded_stringified.replace("\"", ""),
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

        Ok(self.hasher.finalize().into())
    }

    pub fn add_new_transaction(
        &mut self,
        changes_json: &str,
        mode: TransactionMode,
        signer_secret: &SignerSecret,
        made_at: u64,
    ) -> (Hash, Signature, Transaction) {
        let new_tx = match mode {
            TransactionMode::Private { key_id, key_secret } => {
                let tx_index = self.transactions_json.len() as u32;

                let nonce_material = JsonValue::Object(serde_json::Map::from_iter(vec![
                    ("in".to_string(), JsonValue::String(self.co_id.0.clone())),
                    (
                        "tx".to_string(),
                        serde_json::to_value(TransactionID {
                            session_id: self.session_id.clone(),
                            tx_index,
                        })
                        .unwrap(),
                    ),
                ]));

                let nonce = self.generate_json_nonce(&nonce_material);

                let secret_key_bytes: [u8; 32] = (&key_secret).into();

                let mut ciphertext = changes_json.as_bytes().to_vec();
                let mut cipher = XSalsa20::new(&secret_key_bytes.into(), &nonce.into());
                cipher.apply_keystream(&mut ciphertext);
                let encrypted_str = format!("encrypted_U{}", URL_SAFE.encode(&ciphertext));

                Transaction::Private(PrivateTransaction {
                    encrypted_changes: Encrypted {
                        value: encrypted_str,
                        _phantom: std::marker::PhantomData,
                    },
                    key_used: key_id.clone(),
                    made_at: Number::from(made_at),
                    privacy: "private".to_string(),
                })
            }
            TransactionMode::Trusting => Transaction::Trusting(TrustingTransaction {
                changes: changes_json.to_string(),
                made_at: Number::from(made_at),
                privacy: "trusting".to_string(),
            }),
        };

        let tx_json = serde_json::to_string(&new_tx).unwrap();
        self.hasher.update(tx_json.as_bytes());
        self.transactions_json.push(tx_json);

        let new_hash = self.hasher.finalize();
        let new_hash_encoded_stringified = format!("\"hash_z{}\"", bs58::encode(new_hash.as_bytes()).into_string());
        let signing_key: SigningKey = signer_secret.into();
        let new_signature: Signature = signing_key.sign(new_hash_encoded_stringified.as_bytes()).into();

        self.last_signature = Some(new_signature.clone());

        (new_hash.into(), new_signature, new_tx)
    }

    pub fn decrypt_next_transaction_changes_json(
        &self,
        tx_index: u32,
        key_secret: &[u8],
    ) -> Result<String, CoJsonCoreError> {
        let tx_json = self
            .transactions_json
            .get(tx_index as usize)
            .ok_or(CoJsonCoreError::TransactionNotFound(tx_index))?;
        let tx: Transaction = serde_json::from_str(tx_json)?;

        match tx {
            Transaction::Private(private_tx) => {
                let nonce_material = JsonValue::Object(serde_json::Map::from_iter(vec![
                    ("in".to_string(), JsonValue::String(self.co_id.0.clone())),
                    (
                        "tx".to_string(),
                        serde_json::to_value(TransactionID {
                            session_id: self.session_id.clone(),
                            tx_index,
                        })?,
                    ),
                ]));

                let nonce = self.generate_json_nonce(&nonce_material);

                let encrypted_val = private_tx.encrypted_changes.value;
                let prefix = "encrypted_U";
                if !encrypted_val.starts_with(prefix) {
                    return Err(CoJsonCoreError::InvalidEncryptedPrefix);
                }

                let ciphertext_b64 = &encrypted_val[prefix.len()..];
                let mut ciphertext = URL_SAFE.decode(ciphertext_b64)?;

                let mut cipher = XSalsa20::new(key_secret.into(), &nonce.into());
                cipher.apply_keystream(&mut ciphertext);

                Ok(String::from_utf8(ciphertext)?)
            }
            Transaction::Trusting(trusting_tx) => Ok(trusting_tx.changes),
        }
    }

    fn generate_nonce(&self, material: &[u8]) -> [u8; 24] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(material);
        let mut output = [0u8; 24];
        let mut output_reader = hasher.finalize_xof();
        output_reader.fill(&mut output);
        output
    }

    fn generate_json_nonce(&self, material: &JsonValue) -> [u8; 24] {
        let stable_json = serde_json::to_string(&material).unwrap();
        self.generate_nonce(stable_json.as_bytes())
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
            verifying_key.into(),
        );

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

        let mut session = SessionLogInternal::new(co_id, session_id, root.signer_id);

        let new_signature = example.last_signature;

        let result = session.try_add(
            vec![example.transactions[0].to_owned()],
            &new_signature,
            false,
        );

        match result {
            Ok(returned_final_hash) => {
                let final_hash = session.hasher.finalize();
                let final_hash_encoded = format!(
                    "hash_z{}",
                    bs58::encode(final_hash.as_bytes()).into_string()
                );

                assert_eq!(final_hash_encoded, example.last_hash);
                assert_eq!(returned_final_hash.0, example.last_hash);
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

        let mut session = SessionLogInternal::new(co_id, session_id, root.signer_id);

        let new_signature = example.last_signature;

        let result = session.try_add(
            example.transactions.into_iter().map(|tx| tx.to_owned()).collect(),
            &new_signature,
            false,
        );

        match result {
            Ok(returned_final_hash) => {
                let final_hash = session.hasher.finalize();
                let final_hash_encoded = format!(
                    "hash_z{}",
                    bs58::encode(final_hash.as_bytes()).into_string()
                );

                assert_eq!(final_hash_encoded, example.last_hash);
                assert_eq!(returned_final_hash.0, example.last_hash);
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
            public_key.into(),
        );

        // The plaintext changes we want to add
        let changes_json =
            r#"[{"after":"start","op":"app","value":"co_zMphsnYN6GU8nn2HDY5suvyGufY"}]"#;

        // Extract all the necessary components from the example data
        let key_secret = KeySecret(known_key["secret"].as_str().unwrap().to_string());
        let key_id = KeyID(known_key["id"].as_str().unwrap().to_string());
        let made_at = tx_from_example["madeAt"].as_u64().unwrap();

        // Call the function we are testing
        let (_new_hash, new_signature, _new_tx) = session.add_new_transaction(
            changes_json,
            TransactionMode::Private {
                key_id: key_id,
                key_secret: key_secret,
            },
            &signing_key.into(),
            made_at,
        );

        // 1. Check that the transaction we created matches the one in the file
        let created_tx_json = &session.transactions_json[0];
        let expected_tx_json = serde_json::to_string(tx_from_example).unwrap();
        assert_eq!(created_tx_json, &expected_tx_json);

        // 2. Check that the final hash of the session matches the one in the file
        let final_hash = session.hasher.finalize();
        let final_hash_encoded = format!(
            "hash_z{}",
            bs58::encode(final_hash.as_bytes()).into_string()
        );
        assert_eq!(
            final_hash_encoded,
            session_data["lastHash"].as_str().unwrap()
        );

        let final_hash_encoded_stringified = format!(
            "\"{}\"",
            final_hash_encoded
        );

        // 3. Check that the signature is valid for our generated key
        assert!(session
            .public_key
            .verify(final_hash_encoded_stringified.as_bytes(), &(&new_signature).into())
            .is_ok());
        assert_eq!(session.last_signature, Some(new_signature));
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

        let mut session = SessionLogInternal::new(root.co_id, session_id, public_key.into());

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

        let key_secret = decode_z(&root.known_keys[0].secret).unwrap();

        let decrypted = session
            .decrypt_next_transaction_changes_json(0, &key_secret)
            .unwrap();

        assert_eq!(
            decrypted,
            r#"[{"after":"start","op":"app","value":"co_zMphsnYN6GU8nn2HDY5suvyGufY"}]"#
        );
    }
}
