use cojson_core::{
    CoID, CoJsonCoreError, KeyID, KeySecret, SessionID, SessionLogInternal, Signature, SignerSecret, TransactionMode, decode_z
};
use serde_json::value::RawValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use std::collections::HashMap;
use std::sync::Mutex;
use once_cell::sync::Lazy;
use ed25519_dalek::VerifyingKey;

mod error;
pub use error::CryptoError;

pub mod hash {
    pub mod blake3;
    pub use blake3::*;
}

pub mod crypto {
    pub mod ed25519;
    pub mod encrypt;
    pub mod seal;
    pub mod sign;
    pub mod x25519;
    pub mod xsalsa20;

    pub use ed25519::*;
    pub use encrypt::*;
    pub use seal::*;
    pub use sign::*;
    pub use x25519::*;
    pub use xsalsa20::*;
}

#[derive(Error, Debug)]
pub enum CojsonCoreWasmError {
    #[error(transparent)]
    CoJson(#[from] CoJsonCoreError),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    SerdeWasmBindgen(#[from] serde_wasm_bindgen::Error),
    #[error("JsValue Error: {0:?}")]
    Js(JsValue),
}

impl From<CojsonCoreWasmError> for JsValue {
    fn from(err: CojsonCoreWasmError) -> Self {
        JsValue::from_str(&err.to_string())
    }
}

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
#[derive(Clone)]
pub struct SessionLog {
    internal: SessionLogInternal,
}

#[derive(Serialize, Deserialize)]
struct PrivateTransactionResult {
    signature: String,
    encrypted_changes: String,
}

#[wasm_bindgen]
impl SessionLog {
    #[wasm_bindgen(constructor)]
    pub fn new(co_id: String, session_id: String, signer_id: String) -> SessionLog {
        let co_id = CoID(co_id);
        let session_id = SessionID(session_id);
        
        // Get the public key from the KeyChain, or create a default one if not found
        let public_key = keychain_get_signer_id(signer_id.clone());

        let internal = SessionLogInternal::new(co_id, session_id, public_key);

        SessionLog { internal }
    }

    #[wasm_bindgen(js_name = clone)]
    pub fn clone_js(&self) -> SessionLog {
        self.clone()
    }

    #[wasm_bindgen(js_name = tryAdd)]
    pub fn try_add(
        &mut self,
        transactions_json: Vec<String>,
        new_signature_str: String,
        skip_verify: bool,
    ) -> Result<(), CojsonCoreWasmError> {
        let transactions: Vec<Box<RawValue>> = transactions_json
            .into_iter()
            .map(|s| {
                serde_json::from_str(&s).map_err(|e| {
                    CojsonCoreWasmError::Js(JsValue::from(format!(
                        "Failed to parse transaction string: {}",
                        e
                    )))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let new_signature = Signature(new_signature_str);

        self.internal
            .try_add(transactions, &new_signature, skip_verify)?;

        Ok(())
    }

    #[wasm_bindgen(js_name = addNewPrivateTransaction)]
    pub fn add_new_private_transaction(
        &mut self,
        changes_json: &str,
        signer_secret: String,
        encryption_key: String,
        key_id: String,
        made_at: f64,
    ) -> Result<String, CojsonCoreWasmError> {
        let key_secret = keychain_get_key_secret(encryption_key)
            .ok_or_else(|| CojsonCoreWasmError::Js(JsValue::from_str("Failed to get key secret")))?;
        
        let signer_secret = keychain_get_signer_secret(signer_secret)
            .ok_or_else(|| CojsonCoreWasmError::Js(JsValue::from_str("Failed to get signer secret")))?;

        let (signature, transaction) = self.internal.add_new_transaction(
            changes_json,
            TransactionMode::Private{key_id: KeyID(key_id), key_secret},
            &signer_secret,
            made_at as u64,
        );

        // Extract encrypted_changes from the private transaction
        let encrypted_changes = match transaction {
            cojson_core::Transaction::Private(private_tx) => private_tx.encrypted_changes.value,
            _ => return Err(CojsonCoreWasmError::Js(JsValue::from_str("Expected private transaction"))),
        };

        let result = PrivateTransactionResult{
            signature: signature.0,
            encrypted_changes,
        };

        Ok(serde_json::to_string(&result)?)
    }

    #[wasm_bindgen(js_name = addNewTrustingTransaction)]
    pub fn add_new_trusting_transaction(
        &mut self,
        changes_json: &str,
        signer_secret: String,
        made_at: f64,
    ) -> Result<String, CojsonCoreWasmError> {
        let signer_secret = keychain_get_signer_secret(signer_secret)
            .ok_or_else(|| CojsonCoreWasmError::Js(JsValue::from_str("Failed to get signer secret")))?;

        let (signature, _) = self.internal.add_new_transaction(
            changes_json,
            TransactionMode::Trusting,
            &signer_secret,
            made_at as u64,
        );

        Ok(signature.0)
    }

    #[wasm_bindgen(js_name = decryptNextTransactionChangesJson)]
    pub fn decrypt_next_transaction_changes_json(
        &self,
        tx_index: u32,
        encryption_key: String,
    ) -> Result<String, CojsonCoreWasmError> {
        let key_secret = keychain_get_key_secret(encryption_key)
            .ok_or_else(|| CojsonCoreWasmError::Js(JsValue::from_str("Failed to get key secret")))?;

        Ok(self
            .internal
            .decrypt_next_transaction_changes_json(tx_index, key_secret)?)
    }
}

static KEYCHAIN: Lazy<Mutex<KeyChain>> = Lazy::new(|| Mutex::new(KeyChain::new()));

#[derive(Clone)]
struct KeyChain {
    key_secrets: HashMap<String, KeySecret>,
    signer_secrets: HashMap<String, SignerSecret>,
    signer_ids: HashMap<String, VerifyingKey>,
}

impl KeyChain {
    fn new() -> Self {
        Self {
            key_secrets: HashMap::new(),
            signer_secrets: HashMap::new(),
            signer_ids: HashMap::new(),
        }
    }

    fn get_key_secret(&mut self, raw_secret: &str) -> KeySecret {
        if let Some(key_secret) = self.key_secrets.get(raw_secret) {
            key_secret.clone()
        } else {
            let key_secret = KeySecret(raw_secret.to_string());
            self.key_secrets.insert(raw_secret.to_string(), key_secret.clone());
            key_secret
        }
    }

    fn get_signer_secret(&mut self, raw_secret: &str) -> SignerSecret {
        if let Some(signer_secret) = self.signer_secrets.get(raw_secret) {
            signer_secret.clone()
        } else {
            let signer_secret = SignerSecret(raw_secret.to_string());
            self.signer_secrets.insert(raw_secret.to_string(), signer_secret.clone());
            signer_secret
        }
    }

    fn get_signer_id(&mut self, raw_id: &str) -> VerifyingKey {
        if let Some(signer_id) = self.signer_ids.get(raw_id) {
            signer_id.clone()
        } else {
            let public_key = VerifyingKey::try_from(
                decode_z(&raw_id)
                    .expect("Invalid public key")
                    .as_slice(),
            )
            .expect("Invalid public key");
            self.signer_ids.insert(raw_id.to_string(), public_key.clone());
            public_key
        }
    }

    fn clear_all(&mut self) {
        self.key_secrets.clear();
        self.signer_secrets.clear();
        self.signer_ids.clear();
    }
}

// Public functions to interact with the singleton KeyChain
pub fn keychain_get_key_secret(raw_secret: String) -> Option<KeySecret> {
    if let Ok(mut keychain) = KEYCHAIN.lock() {
        Some(keychain.get_key_secret(&raw_secret))
    } else {
        None
    }
}

pub fn keychain_get_signer_secret(raw_secret: String) -> Option<SignerSecret> {
    if let Ok(mut keychain) = KEYCHAIN.lock() {
        Some(keychain.get_signer_secret(&raw_secret))
    } else {
        None
    }
}

pub fn keychain_get_signer_id(signer_id: String) -> VerifyingKey {
    if let Ok(mut keychain) = KEYCHAIN.lock() {
        keychain.get_signer_id(&signer_id)
    } else {
        VerifyingKey::try_from(
            decode_z(&signer_id)
                .expect("Invalid public key")
                .as_slice(),
        )
        .expect("Invalid public key")
    }
}

pub fn keychain_clear_all() {
    if let Ok(mut keychain) = KEYCHAIN.lock() {
        keychain.clear_all();
    }
}
