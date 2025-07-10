use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Number, Value as JsonValue};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey, Verifier};
use bs58;
use salsa20::{
    cipher::{KeyIvInit, StreamCipher},
    XSalsa20,
};
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use std::error::Error;

// Re-export lzy for convenience
pub use lzy;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionID(pub String);
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SignerID(pub String);
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeyID(pub String);
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

pub const APPEND_OK: u32 = 0;
pub const APPEND_INVALID_SIGNATURE: u32 = 1;

pub type AppendResult = u32;

pub struct SessionLogInternal {
    co_id: CoID,
    session_id: SessionID,
    public_key: VerifyingKey,
    hasher: blake3::Hasher,
    transactions_json: Vec<String>,
    last_signature: Option<Signature>,
}

impl SessionLogInternal {
    pub fn new(co_id: CoID, session_id: SessionID, public_key: VerifyingKey) -> Self {
        let hasher = blake3::Hasher::new();
        Self {
            co_id,
            session_id,
            public_key,
            hasher,
            transactions_json: Vec::new(),
            last_signature: None,
        }
    }

    pub fn try_add(
        &mut self,
        transactions: Vec<Box<RawValue>>,
        new_signature: Signature,
        skip_verify: bool,
    ) -> Result<(), String> {
        if !skip_verify {
            let mut hasher = self.hasher.clone();
            for tx in &transactions {
                hasher.update(tx.get().as_bytes());
            }

            let new_hash = hasher.finalize();
            let new_hash_encoded = format!("hash_z{}", bs58::encode(new_hash.as_bytes()).into_string());

            if self
                .public_key
                .verify(new_hash_encoded.as_bytes(), &new_signature)
                .is_err()
            {
                return Err(new_hash_encoded);
            }
        }

        for tx in transactions {
            let tx_json = tx.get();
            self.hasher.update(tx_json.as_bytes());
            self.transactions_json.push(tx_json.to_string());
        }

        self.last_signature = Some(new_signature);

        Ok(())
    }

    pub fn add_new_transaction(
        &mut self,
        changes_json: &str,
        signing_key: &SigningKey,
        encryption_key: &[u8],
        key_id: KeyID,
        made_at: u64,
    ) -> Signature {
        let tx_index = self.transactions_json.len() as u32;

        let nonce_material = JsonValue::Object(serde_json::Map::from_iter(vec![
            ("in".to_string(), JsonValue::String(self.co_id.0.clone())),
            ("tx".to_string(), serde_json::to_value(TransactionID {
                session_id: self.session_id.clone(),
                tx_index,
            }).unwrap()),
        ]));

        println!("Nonce material for add_new_transaction: {}", serde_json::to_string(&nonce_material).unwrap());
        let nonce = self.generate_json_nonce(&nonce_material);

        let mut ciphertext = changes_json.as_bytes().to_vec();
        let mut cipher = XSalsa20::new(encryption_key.into(), &nonce.into());
        cipher.apply_keystream(&mut ciphertext);
        let encrypted_str = format!("encrypted_U{}", URL_SAFE.encode(&ciphertext));

        let new_tx = Transaction::Private(PrivateTransaction {
            encrypted_changes: Encrypted {
                value: encrypted_str,
                _phantom: std::marker::PhantomData,
            },
            key_used: key_id,
            made_at: Number::from(made_at),
            privacy: "private".to_string(),
        });

        let tx_json = serde_json::to_string(&new_tx).unwrap();
        self.hasher.update(tx_json.as_bytes());
        self.transactions_json.push(tx_json);

        let new_hash = self.hasher.finalize();
        let new_hash_encoded = format!("hash_z{}", bs58::encode(new_hash.as_bytes()).into_string());
        let new_signature = signing_key.sign(new_hash_encoded.as_bytes());

        self.last_signature = Some(new_signature);

        new_signature
    }

    pub fn decrypt_next_transaction_changes_json(
        &self,
        tx_index: u32,
        key_secret: &[u8],
    ) -> Result<String, Box<dyn Error>> {
        let tx_json = self.transactions_json.get(tx_index as usize).ok_or("Transaction not found")?;
        let tx: Transaction = serde_json::from_str(tx_json)?;

        match tx {
            Transaction::Private(private_tx) => {
                let nonce_material = JsonValue::Object(serde_json::Map::from_iter(vec![
                    ("in".to_string(), JsonValue::String(self.co_id.0.clone())),
                    ("tx".to_string(), serde_json::to_value(TransactionID {
                        session_id: self.session_id.clone(),
                        tx_index,
                    })?),
                ]));

                let nonce = self.generate_json_nonce(&nonce_material);

                let encrypted_val = private_tx.encrypted_changes.value;
                let prefix = "encrypted_U";
                if !encrypted_val.starts_with(prefix) {
                    return Err("Invalid encrypted prefix".into());
                }

                let ciphertext_b64 = &encrypted_val[prefix.len()..];
                let mut ciphertext = URL_SAFE.decode(ciphertext_b64)?;

                let mut cipher = XSalsa20::new(key_secret.into(), &nonce.into());
                cipher.apply_keystream(&mut ciphertext);

                Ok(String::from_utf8(ciphertext)?)
            }
            Transaction::Trusting(trusting_tx) => {
                Ok(trusting_tx.changes)
            }
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


#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Signer;
    use rand_core::OsRng;
    use std::{collections::HashMap, fs};

    fn decode_z(value: &str) -> Result<Vec<u8>, String> {
        let prefix_end = value.find("_z").ok_or("Invalid prefix")? + 2;
        bs58::decode(&value[prefix_end..]).into_vec().map_err(|e| e.to_string())
    }

    #[test]
    fn it_works() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let verifying_key = signing_key.verifying_key();

        let session = SessionLogInternal::new(
            CoID("co_test1".to_string()),
            SessionID("session_test1".to_string()),
            verifying_key,
        );

        assert!(session.last_signature.is_none());
    }

    #[test]
    fn test_add_from_example_json() {
        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        struct TestSession<'a> {
            last_signature: String,
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

        let data = fs::read_to_string("data/exampleSessions.json").expect("Unable to read exampleSessions.json");
        let root: Root = serde_json::from_str(&data).unwrap();

        let (session_id_str, example) = root.example_base.into_iter().next().unwrap();
        let session_id = SessionID(session_id_str.clone());
        let co_id = CoID(session_id_str.split("_session_").next().unwrap().to_string());

        let public_key = VerifyingKey::from_bytes(&decode_z(&root.signer_id.0).unwrap().try_into().unwrap()).unwrap();

        let mut session = SessionLogInternal::new(
            co_id,
            session_id,
            public_key,
        );

        let new_signature = Signature::from_bytes(&decode_z(&example.last_signature).unwrap().try_into().unwrap());

        let result = session.try_add(
            vec![example.transactions[0].to_owned()],
            new_signature,
            false,
        );

        match result {
            Ok(()) => {
                let final_hash = session.hasher.finalize();
                let final_hash_encoded = format!("hash_z{}", bs58::encode(final_hash.as_bytes()).into_string());

                assert_eq!(final_hash_encoded, example.last_hash);
                assert_eq!(session.last_signature, Some(new_signature));
            }
            Err(new_hash_encoded) => {
                assert_eq!(new_hash_encoded, example.last_hash);
            }
        }
    }

    #[test]
    fn test_add_new_transaction() {
        // Load the example data to get all the pieces we need
        let data = fs::read_to_string("data/exampleSessions.json").expect("Unable to read exampleSessions.json");
        let root: serde_json::Value = serde_json::from_str(&data).unwrap();
        let session_data = &root["exampleBase"]["co_zkNajJ1BhLzR962jpzvXxx917ZB_session_zXzrQLTtp8rR"];
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
            public_key,
        );

        // The plaintext changes we want to add
        let changes_json = r#"[{"after":"start","op":"app","value":"co_zMphsnYN6GU8nn2HDY5suvyGufY"}]"#;

        // Extract all the necessary components from the example data
        let key_secret = decode_z(known_key["secret"].as_str().unwrap()).unwrap();
        let key_id = KeyID(known_key["id"].as_str().unwrap().to_string());
        let made_at = tx_from_example["madeAt"].as_u64().unwrap();

        // Call the function we are testing
        let new_signature = session.add_new_transaction(
            changes_json,
            &signing_key,
            &key_secret,
            key_id,
            made_at,
        );

        // 1. Check that the transaction we created matches the one in the file
        let created_tx_json = &session.transactions_json[0];
        let expected_tx_json = serde_json::to_string(tx_from_example).unwrap();
        assert_eq!(created_tx_json, &expected_tx_json);

        // 2. Check that the final hash of the session matches the one in the file
        let final_hash = session.hasher.finalize();
        let final_hash_encoded = format!("hash_z{}", bs58::encode(final_hash.as_bytes()).into_string());
        assert_eq!(final_hash_encoded, session_data["lastHash"].as_str().unwrap());

        // 3. Check that the signature is valid for our generated key
        assert!(session.public_key.verify(final_hash_encoded.as_bytes(), &new_signature).is_ok());
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
            last_hash: String,
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

        let data = fs::read_to_string("data/exampleSessions.json").expect("Unable to read exampleSessions.json");
        let root: Root = serde_json::from_str(&data).unwrap();

        let (session_id_str, example) = root.example_base.into_iter().next().unwrap();
        let session_id = SessionID(session_id_str.clone());

        let public_key = VerifyingKey::from_bytes(&decode_z(&root.signer_id.0).unwrap().try_into().unwrap()).unwrap();

        let mut session = SessionLogInternal::new(
            root.co_id,
            session_id,
            public_key,
        );

        let new_signature = Signature::from_bytes(&decode_z(&example.last_signature).unwrap().try_into().unwrap());

        session.try_add(
            example
                .transactions
                .into_iter()
                .map(|v| v.to_owned())
                .collect(),
            new_signature,
            true, // Skipping verification because we don't have the right initial state
        ).unwrap();

        let key_secret = decode_z(&root.known_keys[0].secret).unwrap();

        let nonce_material_for_decryption = JsonValue::Object(serde_json::Map::from_iter(vec![
            ("in".to_string(), JsonValue::String(session.co_id.0.clone())),
            ("tx".to_string(), serde_json::to_value(TransactionID {
                session_id: session.session_id.clone(),
                tx_index: 0,
            }).unwrap()),
        ]));

        let decrypted = session.decrypt_next_transaction_changes_json(0, &key_secret).unwrap();

        assert_eq!(decrypted, r#"[{"after":"start","op":"app","value":"co_zMphsnYN6GU8nn2HDY5suvyGufY"}]"#);
    }
}
