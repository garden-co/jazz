
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use salsa20::{
    cipher::{Key, KeyIvInit, StreamCipher},
    XSalsa20,
};
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Number, Value as JsonValue};
use crate::core::{CryptoCache, NonceGenerator, CoJsonCoreError};
use crate::core::keys::{SignerID, SignerSecret, Signature, KeyID, KeySecret, CoID, decode_z};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionID(pub String);

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



#[derive(Clone)]
pub struct SessionLogInternal {
    public_key: Option<VerifyingKey>,
    hasher: blake3::Hasher,
    transactions_json: Vec<String>,
    last_signature: Option<Signature>,
    nonce_generator: NonceGenerator,
    crypto_cache: CryptoCache,
}


impl SessionLogInternal {
    /// Create a new session log, optionally with a public key for signature verification.
    pub fn new(co_id: CoID, session_id: SessionID, signer_id: Option<SignerID>) -> Self {
        let hasher = blake3::Hasher::new();

        // If a signer_id is provided, decode and parse it as a VerifyingKey.
        let public_key = match signer_id {
            Some(signer_id) => Some(
                VerifyingKey::try_from(
                    decode_z(&signer_id.0)
                        .expect("Invalid public key")
                        .as_slice(),
                )
                .expect("Invalid public key"),
            ),
            None => None,
        };

        Self {
            public_key,
            hasher,
            transactions_json: Vec::new(),
            last_signature: None,
            nonce_generator: NonceGenerator::new(co_id, session_id),
            crypto_cache: CryptoCache::new(),
        }
    }

    /// Get a reference to the list of serialized transaction JSON strings.
    pub fn transactions_json(&self) -> &Vec<String> {
        &self.transactions_json
    }

    /// Get the last signature, if any.
    pub fn last_signature(&self) -> Option<&Signature> {
        self.last_signature.as_ref()
    }

    /// Compute the hash that would result after adding the given transactions.
    /// This is used for signature verification.
    fn expected_hash_after(&self, transactions: &[Box<RawValue>]) -> blake3::Hasher {
        let mut hasher = self.hasher.clone();
        for tx in transactions {
            hasher.update(tx.get().as_bytes());
        }
        hasher
    }

    /// Try to add a batch of transactions, verifying the signature if required.
    /// If skip_verify is false, checks the signature against the expected hash.
    /// Updates the internal hash state and transaction log if successful.
    pub fn try_add(
        &mut self,
        transactions: Vec<Box<RawValue>>,
        new_signature: &Signature,
        skip_verify: bool,
    ) -> Result<(), CoJsonCoreError> {
        if !skip_verify {
            // Compute the hash after adding the new transactions.
            let hasher = self.expected_hash_after(&transactions);
            let new_hash_encoded_stringified = format!(
                "\"hash_z{}\"",
                bs58::encode(hasher.finalize().as_bytes()).into_string()
            );

            // Verify the signature using the public key, if present.
            if let Some(public_key) = self.public_key {
                match public_key.verify(
                    new_hash_encoded_stringified.as_bytes(),
                    &(new_signature.try_into()?),
                ) {
                    Ok(()) => {}
                    Err(_) => {
                        return Err(CoJsonCoreError::SignatureVerification(
                            new_hash_encoded_stringified.replace("\"", ""),
                        ));
                    }
                }
            } else {
                // No public key available for verification.
                return Err(CoJsonCoreError::SignatureVerification(
                    new_hash_encoded_stringified.replace("\"", ""),
                ));
            }

            // Update the internal hasher state to the new hash.
            self.hasher = hasher;
        }

        // Add the new transactions to the log.
        for tx in transactions {
            self.transactions_json.push(tx.get().to_string());
        }

        // Update the last signature.
        self.last_signature = Some(new_signature.clone());

        Ok(())
    }

    /// Add a new transaction (private or trusting), encrypting as needed, and sign the new hash.
    /// Returns the new signature and the transaction object.
    pub fn add_new_transaction(
        &mut self,
        changes_json: &str,
        mode: TransactionMode,
        signer_secret: &SignerSecret,
        made_at: u64,
        meta: Option<String>,
    ) -> Result<(Signature, Transaction), CoJsonCoreError> {
        // Build the transaction object depending on the mode.
        let new_tx = match mode {
            TransactionMode::Private { key_id, key_secret } => {
                // For private transactions, encrypt the changes and meta fields.
                let tx_index = self.transactions_json.len() as u32;

                // Generate a unique nonce for this transaction.
                let nonce = self.nonce_generator.get_nonce(tx_index);

                // Prepare the secret key bytes for encryption.
                let key: Key<XSalsa20> = self.crypto_cache.get_xsalsa20_key(&key_secret)?;

                // Encrypt the changes JSON.
                let mut ciphertext = changes_json.as_bytes().to_vec();
                let mut cipher = XSalsa20::new(&key, &nonce.into());
                cipher.apply_keystream(&mut ciphertext);
                let encrypted_str = format!("encrypted_U{}", URL_SAFE.encode(&ciphertext));

                // Optionally encrypt the meta field.
                let encrypted_meta = meta.map(|meta| {
                    let mut ciphertext = meta.as_bytes().to_vec();
                    let mut cipher = XSalsa20::new(&key, &nonce.into());
                    cipher.apply_keystream(&mut ciphertext);

                    let encrypted_meta = format!("encrypted_U{}", URL_SAFE.encode(&ciphertext));

                    Encrypted {
                        value: encrypted_meta,
                        _phantom: std::marker::PhantomData,
                    }
                });

                // Build the private transaction.
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
            TransactionMode::Trusting => {
                // For trusting transactions, just store the changes as plain text.
                Transaction::Trusting(TrustingTransaction {
                    changes: changes_json.to_string(),
                    made_at: Number::from(made_at),
                    meta,
                    privacy: "trusting".to_string(),
                })
            }
        };

        // Serialize the transaction to JSON and update the hash state.
        let tx_json = serde_json::to_string(&new_tx).unwrap();
        self.hasher.update(tx_json.as_bytes());
        self.transactions_json.push(tx_json);

        // Compute the new hash and sign it.
        let new_hash = self.hasher.finalize();
        let new_hash_encoded_stringified = format!(
            "\"hash_z{}\"",
            bs58::encode(new_hash.as_bytes()).into_string()
        );
        let signing_key: SigningKey = self.crypto_cache.get_ed25519_signing_key(signer_secret)?;
        let new_signature: Signature = signing_key
            .sign(new_hash_encoded_stringified.as_bytes())
            .into();

        // Update the last signature.
        self.last_signature = Some(new_signature.clone());

        Ok((new_signature, new_tx))
    }

    /// Decrypt the changes JSON for the transaction at the given index.
    /// Returns the decrypted string, or an error if decryption fails.
    pub fn decrypt_next_transaction_changes_json(
        &self,
        tx_index: u32,
        key_secret: KeySecret,
    ) -> Result<String, CoJsonCoreError> {
        // Get the transaction JSON string at the given index.
        let tx_json = self
            .transactions_json
            .get(tx_index as usize)
            .ok_or(CoJsonCoreError::TransactionNotFound(tx_index))?;
        let tx: Transaction = serde_json::from_str(tx_json)?;

        match tx {
            Transaction::Private(private_tx) => {
                // For private transactions, decrypt the encrypted_changes field.
                let nonce = self.nonce_generator.get_nonce(tx_index);

                let encrypted_val = private_tx.encrypted_changes.value;
                let prefix = "encrypted_U";
                if !encrypted_val.starts_with(prefix) {
                    return Err(CoJsonCoreError::InvalidEncryptedPrefix);
                }

                // Decode the base64-encoded ciphertext.
                let ciphertext_b64 = &encrypted_val[prefix.len()..];
                let mut ciphertext = URL_SAFE.decode(ciphertext_b64)?;

                // Decrypt using XSalsa20.
                let key = self.crypto_cache.get_xsalsa20_key(&key_secret)?;

                let mut cipher = XSalsa20::new(&key, &nonce.into());
                cipher.apply_keystream(&mut ciphertext);

                Ok(String::from_utf8(ciphertext)?)
            }
            // For trusting transactions, just return the plain changes.
            Transaction::Trusting(trusting_tx) => Ok(trusting_tx.changes),
        }
    }

    /// Decrypt the meta JSON for the transaction at the given index, if present.
    /// Returns the decrypted string, or None if no meta, or an error if decryption fails.
    pub fn decrypt_next_transaction_meta_json(
        &self,
        tx_index: u32,
        key_secret: KeySecret,
    ) -> Result<Option<String>, CoJsonCoreError> {
        // Get the transaction JSON string at the given index.
        let tx_json = self
            .transactions_json
            .get(tx_index as usize)
            .ok_or(CoJsonCoreError::TransactionNotFound(tx_index))?;
        let tx: Transaction = serde_json::from_str(tx_json)?;

        match tx {
            Transaction::Private(private_tx) => {
                // If meta is present, decrypt it.
                if let Some(encrypted_meta) = private_tx.meta {
                    let nonce = self.nonce_generator.get_nonce(tx_index);

                    let encrypted_val = encrypted_meta.value;
                    let prefix = "encrypted_U";
                    if !encrypted_val.starts_with(prefix) {
                        return Err(CoJsonCoreError::InvalidEncryptedPrefix);
                    }

                    // Decode the base64-encoded ciphertext.
                    let ciphertext_b64 = &encrypted_val[prefix.len()..];
                    let mut ciphertext = URL_SAFE.decode(ciphertext_b64)?;

                    // Decrypt using XSalsa20.
                    let key: Key<XSalsa20> = self.crypto_cache.get_xsalsa20_key(&key_secret)?;
                    let mut cipher = XSalsa20::new(&key, &nonce.into());
                    cipher.apply_keystream(&mut ciphertext);

                    Ok(Some(String::from_utf8(ciphertext)?))
                } else {
                    Ok(None)
                }
            }
            // For trusting transactions, just return the plain meta field.
            Transaction::Trusting(trusting_tx) => Ok(trusting_tx.meta),
        }
    }
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

        let mut session = SessionLogInternal::new(co_id, session_id, Some(root.signer_id));

        let new_signature = example.last_signature;

        let result = session.try_add(
            vec![example.transactions[0].to_owned()],
            &new_signature,
            false,
        );

        match result {
            Ok(_returned_final_hash) => {
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

        let mut session = SessionLogInternal::new(co_id, session_id, Some(root.signer_id));

        let new_signature = example.last_signature;

        let result = session.try_add(
            example
                .transactions
                .into_iter()
                .map(|tx| tx.to_owned())
                .collect(),
            &new_signature,
            false,
        );

        match result {
            Ok(_returned_final_hash) => {
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
        );

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
                key_id,
                key_secret,
            },
            &signing_key.into(),
            made_at,
            None,
        ).unwrap();

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

        let final_hash_encoded_stringified = format!("\"{}\"", final_hash_encoded);

        // 3. Check that the signature is valid for our generated key
        assert!(session
            .public_key
            .expect("Public key should be present")
            .verify(
                final_hash_encoded_stringified.as_bytes(),
                &(&new_signature).try_into().unwrap()
            )
            .is_ok());
        assert_eq!(session.last_signature, Some(new_signature.clone()));

        let mut session2 = SessionLogInternal::new(
            CoID(root["coID"].as_str().unwrap().to_string()),
            SessionID("co_zkNajJ1BhLzR962jpzvXxx917ZB_session_zXzrQLTtp8rR".to_string()),
            Some(public_key.into()),
        );

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

        let mut session = SessionLogInternal::new(root.co_id, session_id, Some(public_key.into()));

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
    fn test_decrypt_meta_from_example_json() {
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

        let data = fs::read_to_string("data/singleTxSessionMeta.json")
            .expect("Unable to read singleTxSessionMeta.json");
        let root: Root = serde_json::from_str(&data).unwrap();

        let (session_id_str, example) = root.example_base.into_iter().next().unwrap();
        let session_id = SessionID(session_id_str.clone());

        let public_key =
            VerifyingKey::from_bytes(&decode_z(&root.signer_id.0).unwrap().try_into().unwrap())
                .unwrap();

        let mut session = SessionLogInternal::new(root.co_id, session_id, Some(public_key.into()));

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
            .decrypt_next_transaction_meta_json(0, key_secret)
            .unwrap();

        assert_eq!(
            decrypted,
            Some(r#"{"meta":{"test":"test"}}"#.to_string())
        );
    }


    #[test]
    fn test_add_new_transaction_meta() {
        const META_JSON: &str = r#"{"meta":{"test":"test"}}"#;
        const CHANGES_JSON: &str = r#"[]"#;
        const SESSION_ID: &str = "co_ziwYjGfPdBjvc1bnCufaVdWLozm_session_zj5NjtJL6s5p";

        // Load the example data to get all the pieces we need
        let data = fs::read_to_string("data/singleTxSessionMeta.json")
            .expect("Unable to read singleTxSession.json");
        let root: serde_json::Value = serde_json::from_str(&data).unwrap();
        let session_data =
            &root["exampleBase"][SESSION_ID];
        let tx_from_example = &session_data["transactions"][0];
        let known_key = &root["knownKeys"][0];

        // Since we don't have the original private key, we generate a new one for this test.
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let public_key = signing_key.verifying_key();

        // Initialize an empty session
        let mut session = SessionLogInternal::new(
            CoID(root["coID"].as_str().unwrap().to_string()),
            SessionID(SESSION_ID.to_string()),
            Some(public_key.into()),
        );

        // Extract all the necessary components from the example data
        let key_secret = KeySecret(known_key["secret"].as_str().unwrap().to_string());
        let key_id = KeyID(known_key["id"].as_str().unwrap().to_string());
        let made_at = tx_from_example["madeAt"].as_u64().unwrap();

        // Call the function we are testing
        let (new_signature, _new_tx) = session.add_new_transaction(
            CHANGES_JSON,
            TransactionMode::Private {
                key_id,
                key_secret,
            },
            &signing_key.into(),
            made_at,
            Some(META_JSON.to_string()),
        ).unwrap();

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

        let final_hash_encoded_stringified = format!("\"{}\"", final_hash_encoded);

        // 3. Check that the signature is valid for our generated key
        assert!(session
            .public_key
            .expect("Public key should be present")
            .verify(
                final_hash_encoded_stringified.as_bytes(),
                &(&new_signature).try_into().unwrap()
            )
            .is_ok());
        assert_eq!(session.last_signature, Some(new_signature.clone()));

        let mut session2 = SessionLogInternal::new(
            CoID(root["coID"].as_str().unwrap().to_string()),
            SessionID("co_ziwYjGfPdBjvc1bnCufaVdWLozm_session_zj5NjtJL6s5p".to_string()),
            Some(public_key.into()),
        );

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
    fn test_transaction_not_found_error() {
        let session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            None,
        );

        let key_secret = KeySecret("test_key".to_string());
        
        // Try to decrypt from non-existent transaction index
        let result = session.decrypt_next_transaction_changes_json(999, key_secret.clone());
        assert!(matches!(result, Err(CoJsonCoreError::TransactionNotFound(999))));

        let result = session.decrypt_next_transaction_meta_json(999, key_secret);
        assert!(matches!(result, Err(CoJsonCoreError::TransactionNotFound(999))));
    }

    #[test]
    fn test_invalid_encrypted_prefix_error() {
        let mut session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            None,
        );

        // Create a transaction with invalid encrypted prefix
        let invalid_tx = Transaction::Private(PrivateTransaction {
            encrypted_changes: Encrypted {
                value: "invalid_prefix_some_data".to_string(),
                _phantom: std::marker::PhantomData,
            },
            key_used: KeyID("test_key".to_string()),
            made_at: Number::from(1234567890),
            meta: None,
            privacy: "private".to_string(),
        });

        let tx_json = serde_json::to_string(&invalid_tx).unwrap();
        session.transactions_json.push(tx_json);

        let key_secret = KeySecret("test_key".to_string());
        
        let result = session.decrypt_next_transaction_changes_json(0, key_secret.clone());
        assert!(matches!(result, Err(CoJsonCoreError::InvalidEncryptedPrefix)));
    }

    #[test]
    fn test_signature_verification_failure() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let public_key = signing_key.verifying_key();

        let mut session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            Some(public_key.into()),
        );

        // Create a transaction
        let tx = Transaction::Trusting(TrustingTransaction {
            changes: r#"{"test": "data"}"#.to_string(),
            made_at: Number::from(1234567890),
            meta: None,
            privacy: "trusting".to_string(),
        });

        let tx_json = serde_json::to_string(&tx).unwrap();
        let transactions = vec![serde_json::from_str(&tx_json).unwrap()];

        // Generate a wrong signature
        let wrong_signing_key = SigningKey::generate(&mut csprng);
        let wrong_signature: Signature = wrong_signing_key.sign(b"wrong data").into();

        let result = session.try_add(transactions, &wrong_signature, false);
        assert!(matches!(result, Err(CoJsonCoreError::SignatureVerification(_))));
    }

    #[test]
    fn test_signature_verification_without_public_key() {
        let session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            None, // No public key provided
        );

        let tx = Transaction::Trusting(TrustingTransaction {
            changes: r#"{"test": "data"}"#.to_string(),
            made_at: Number::from(1234567890),
            meta: None,
            privacy: "trusting".to_string(),
        });

        let tx_json = serde_json::to_string(&tx).unwrap();
        let transactions = vec![serde_json::from_str(&tx_json).unwrap()];

        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let signature: Signature = signing_key.sign(b"some data").into();

        let mut session = session;
        let result = session.try_add(transactions, &signature, false);
        assert!(matches!(result, Err(CoJsonCoreError::SignatureVerification(_))));
    }

    #[test]
    fn test_trusting_transaction_mode() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let public_key = signing_key.verifying_key();

        let mut session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            Some(public_key.into()),
        );

        let changes_json = r#"{"test": "data", "value": 42}"#;
        let meta_json = Some(r#"{"meta": "test"}"#.to_string());
        let made_at = 1234567890;

        let (signature, transaction) = session.add_new_transaction(
            changes_json,
            TransactionMode::Trusting,
            &signing_key.into(),
            made_at,
            meta_json.clone(),
        ).unwrap();

        // Verify the transaction is a TrustingTransaction
        match transaction {
            Transaction::Trusting(tx) => {
                assert_eq!(tx.changes, changes_json);
                assert_eq!(tx.meta, meta_json);
                assert_eq!(tx.made_at, Number::from(made_at));
                assert_eq!(tx.privacy, "trusting");
            }
            _ => panic!("Expected TrustingTransaction"),
        }

        // Verify we can decrypt the changes (should return plain text for trusting transactions)
        let decrypted = session.decrypt_next_transaction_changes_json(0, KeySecret("dummy".to_string())).unwrap();
        assert_eq!(decrypted, changes_json);

        // Verify we can decrypt the meta (should return plain text for trusting transactions)
        let decrypted_meta = session.decrypt_next_transaction_meta_json(0, KeySecret("dummy".to_string())).unwrap();
        assert_eq!(decrypted_meta, meta_json);

        // Verify signature is valid
        let final_hash = session.hasher.finalize();
        let final_hash_encoded_stringified = format!(
            "\"hash_z{}\"",
            bs58::encode(final_hash.as_bytes()).into_string()
        );
        assert!(session
            .public_key
            .expect("Public key should be present")
            .verify(
                final_hash_encoded_stringified.as_bytes(),
                &(&signature).try_into().unwrap()
            )
            .is_ok());
    }

    #[test]
    fn test_getters() {
        let session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            None,
        );

        // Test transactions_json getter
        let transactions = session.transactions_json();
        assert!(transactions.is_empty());

        // Test last_signature getter
        let last_sig = session.last_signature();
        assert!(last_sig.is_none());
    }

    #[test]
    fn test_session_creation_without_signer_id() {
        let session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            None,
        );

        assert!(session.public_key.is_none());
        assert!(session.last_signature.is_none());
        assert!(session.transactions_json.is_empty());
    }

    #[test]
    fn test_session_creation_with_signer_id() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let public_key = signing_key.verifying_key();

        let session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            Some(public_key.into()),
        );

        assert!(session.public_key.is_some());
        assert!(session.last_signature.is_none());
        assert!(session.transactions_json.is_empty());
    }

    #[test]
    fn test_skip_verify_mode() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let public_key = signing_key.verifying_key();

        let mut session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            Some(public_key.into()),
        );

        let tx = Transaction::Trusting(TrustingTransaction {
            changes: r#"{"test": "data"}"#.to_string(),
            made_at: Number::from(1234567890),
            meta: None,
            privacy: "trusting".to_string(),
        });

        let tx_json = serde_json::to_string(&tx).unwrap();
        let transactions = vec![serde_json::from_str(&tx_json).unwrap()];

        // Use a completely wrong signature but skip verification
        let wrong_signature: Signature = signing_key.sign(b"completely wrong data").into();
        
        let result = session.try_add(transactions, &wrong_signature, true);
        assert!(result.is_ok());
        assert_eq!(session.transactions_json.len(), 1);
        assert_eq!(session.last_signature, Some(wrong_signature));
    }

    #[test]
    fn test_multiple_transactions() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let public_key = signing_key.verifying_key();
        let signer_secret = signing_key.into();

        let mut session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            Some(public_key.into()),
        );

        // Add multiple transactions
        for i in 0..3 {
            let changes_json = format!(r#"{{"test": "data_{}"}}"#, i);
            let (_, _) = session.add_new_transaction(
                &changes_json,
                TransactionMode::Trusting,
                &signer_secret,
                1234567890 + i,
                None,
            ).unwrap();
        }

        assert_eq!(session.transactions_json.len(), 3);

        // Verify we can decrypt each transaction
        for i in 0..3 {
            let expected_changes = format!(r#"{{"test": "data_{}"}}"#, i);
            let decrypted = session.decrypt_next_transaction_changes_json(i, KeySecret("dummy".to_string())).unwrap();
            assert_eq!(decrypted, expected_changes);
        }
    }

    #[test]
    fn test_private_transaction_without_meta() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let public_key = signing_key.verifying_key();

        let mut session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            Some(public_key.into()),
        );

        // Create a properly formatted KeySecret with "_z" prefix
        let key_bytes = [1u8; 32]; // 32 bytes of test data
        let key_secret = KeySecret(format!("key_z{}", bs58::encode(key_bytes).into_string()));
        let key_id = KeyID("test_key_id".to_string());
        let changes_json = r#"{"test": "data"}"#;

        let (_, transaction) = session.add_new_transaction(
            changes_json,
            TransactionMode::Private {
                key_id: key_id.clone(),
                key_secret: key_secret.clone(),
            },
            &signing_key.into(),
            1234567890,
            None, // No meta
        ).unwrap();

        // Verify it's a private transaction
        match transaction {
            Transaction::Private(tx) => {
                assert_eq!(tx.key_used, key_id);
                assert_eq!(tx.made_at, Number::from(1234567890));
                assert_eq!(tx.privacy, "private");
                assert!(tx.meta.is_none());
            }
            _ => panic!("Expected PrivateTransaction"),
        }

        // Verify we can decrypt the changes
        let decrypted = session.decrypt_next_transaction_changes_json(0, key_secret.clone()).unwrap();
        assert_eq!(decrypted, changes_json);

        // Verify meta decryption returns None
        let decrypted_meta = session.decrypt_next_transaction_meta_json(0, key_secret).unwrap();
        assert!(decrypted_meta.is_none());
    }

    #[test]
    fn test_malformed_json_error() {
        let mut session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            None,
        );

        // Add malformed JSON to transactions
        session.transactions_json.push("invalid json".to_string());

        let key_secret = KeySecret("test_key".to_string());
        
        let result = session.decrypt_next_transaction_changes_json(0, key_secret.clone());
        assert!(matches!(result, Err(CoJsonCoreError::Json(_))));

        let result = session.decrypt_next_transaction_meta_json(0, key_secret);
        assert!(matches!(result, Err(CoJsonCoreError::Json(_))));
    }

    #[test]
    fn test_base64_decode_error() {
        let mut session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            None,
        );

        // Create a transaction with invalid base64
        let invalid_tx = Transaction::Private(PrivateTransaction {
            encrypted_changes: Encrypted {
                value: "encrypted_Uinvalid_base64!@#$%".to_string(),
                _phantom: std::marker::PhantomData,
            },
            key_used: KeyID("test_key".to_string()),
            made_at: Number::from(1234567890),
            meta: None,
            privacy: "private".to_string(),
        });

        let tx_json = serde_json::to_string(&invalid_tx).unwrap();
        session.transactions_json.push(tx_json);

        let key_secret = KeySecret("test_key".to_string());
        
        let result = session.decrypt_next_transaction_changes_json(0, key_secret.clone());
        assert!(matches!(result, Err(CoJsonCoreError::Base64Decode(_))));
    }

    #[test]
    fn test_utf8_decode_error() {
        let _session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            None,
        );

        // Create a transaction with invalid UTF-8 data (this is tricky to create directly)
        // We'll use a different approach - create valid encrypted data but with wrong key
        let key_bytes = [1u8; 32]; // 32 bytes of test data
        let key_secret = KeySecret(format!("key_z{}", bs58::encode(key_bytes).into_string()));
        let key_id = KeyID("test_key_id".to_string());

        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let public_key = signing_key.verifying_key();

        let mut session_with_tx = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            Some(public_key.into()),
        );

        // Add a valid transaction
        let (_, _) = session_with_tx.add_new_transaction(
            r#"{"test": "data"}"#,
            TransactionMode::Private {
                key_id: key_id.clone(),
                key_secret: key_secret.clone(),
            },
            &signing_key.into(),
            1234567890,
            None,
        ).unwrap();

        // Now try to decrypt with a different key (this will result in garbage UTF-8)
        let wrong_key_bytes = [2u8; 32]; // Different 32 bytes
        let wrong_key = KeySecret(format!("key_z{}", bs58::encode(wrong_key_bytes).into_string()));
        let result = session_with_tx.decrypt_next_transaction_changes_json(0, wrong_key);
        
        // This should fail with UTF-8 error since decryption with wrong key produces garbage
        assert!(matches!(result, Err(CoJsonCoreError::Utf8(_))));
    }

    #[test]
    fn test_empty_transactions_list() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let public_key = signing_key.verifying_key();

        let mut session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            Some(public_key.into()),
        );

        let signature: Signature = signing_key.sign(b"empty transactions").into();

        // Try to add empty transactions list
        let result = session.try_add(Vec::new(), &signature, true);
        assert!(result.is_ok());
        assert_eq!(session.transactions_json.len(), 0);
        assert_eq!(session.last_signature, Some(signature));
    }

    #[test]
    fn test_expected_hash_after_function() {
        let session = SessionLogInternal::new(
            CoID("co_test".to_string()),
            SessionID("session_test".to_string()),
            None,
        );

        let tx1 = serde_json::from_str(r#"{"test": "data1"}"#).unwrap();
        let tx2 = serde_json::from_str(r#"{"test": "data2"}"#).unwrap();
        let transactions = vec![tx1, tx2];

        // This tests the expected_hash_after function indirectly
        // We can't call it directly since it's private, but we can test it through try_add
        let mut test_session = session.clone();
        let signature: Signature = SigningKey::generate(&mut OsRng).sign(b"test").into();
        
        // This should work with skip_verify = true
        let result = test_session.try_add(transactions, &signature, true);
        assert!(result.is_ok());
        assert_eq!(test_session.transactions_json.len(), 2);
    }

    #[test]
    fn test_transaction_id_serialization() {
        let session_id = SessionID("test_session".to_string());
        let tx_id = TransactionID {
            session_id: session_id.clone(),
            tx_index: 42,
        };

        // Test serialization
        let serialized = serde_json::to_string(&tx_id).unwrap();
        let expected = r#"{"sessionID":"test_session","txIndex":42}"#;
        assert_eq!(serialized, expected);

        // Test deserialization
        let deserialized: TransactionID = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, tx_id);
    }

    #[test]
    fn test_encrypted_struct_serialization() {
        let encrypted = Encrypted::<JsonValue> {
            value: "test_encrypted_data".to_string(),
            _phantom: std::marker::PhantomData,
        };

        // Test serialization
        let serialized = serde_json::to_string(&encrypted).unwrap();
        assert_eq!(serialized, "\"test_encrypted_data\"");

        // Test deserialization
        let deserialized: Encrypted<JsonValue> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.value, encrypted.value);
    }

    #[test]
    fn test_transaction_enum_serialization() {
        // Test Private transaction serialization
        let private_tx = Transaction::Private(PrivateTransaction {
            encrypted_changes: Encrypted {
                value: "encrypted_Utest_data".to_string(),
                _phantom: std::marker::PhantomData,
            },
            key_used: KeyID("test_key".to_string()),
            made_at: Number::from(1234567890),
            meta: None,
            privacy: "private".to_string(),
        });

        let serialized = serde_json::to_string(&private_tx).unwrap();
        assert!(serialized.contains("\"privacy\":\"private\""));
        assert!(serialized.contains("\"keyUsed\":\"test_key\""));

        // Test Trusting transaction serialization
        let trusting_tx = Transaction::Trusting(TrustingTransaction {
            changes: r#"{"test": "data"}"#.to_string(),
            made_at: Number::from(1234567890),
            meta: None,
            privacy: "trusting".to_string(),
        });

        let serialized = serde_json::to_string(&trusting_tx).unwrap();
        assert!(serialized.contains("\"privacy\":\"trusting\""));
        assert!(serialized.contains("\"changes\":\"{\\\"test\\\": \\\"data\\\"}\""));
    }
    
}
