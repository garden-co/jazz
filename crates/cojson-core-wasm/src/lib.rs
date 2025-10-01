use cojson_core::{
    CoID, CoJsonCoreError, KeyID, KeySecret, SessionID, SessionLogInternal, Signature, SignerID, SignerSecret, TransactionMode
};
use serde_json::value::RawValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use wasm_bindgen::prelude::*;

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

#[wasm_bindgen]
#[derive(Clone)]
pub struct SessionLog {
    internal: SessionLogInternal,
}

#[derive(Serialize, Deserialize)]
struct PrivateTransactionResult {
    signature: String,
    encrypted_changes: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<String>,
}

#[wasm_bindgen]
impl SessionLog {
    #[wasm_bindgen(constructor)]
    pub fn new(co_id: String, session_id: String, signer_id: Option<String>) -> SessionLog {
        let co_id = CoID(co_id);
        let session_id = SessionID(session_id);
        let signer_id = signer_id.map(|id| SignerID(id));

        let internal = SessionLogInternal::new(co_id, session_id, signer_id);

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
        meta: Option<String>,
    ) -> Result<String, CojsonCoreWasmError> {
        let (signature, transaction) = self.internal.add_new_transaction(
            changes_json,
            TransactionMode::Private{key_id: KeyID(key_id), key_secret: KeySecret(encryption_key)},
            &SignerSecret(signer_secret),
            made_at as u64,
            meta,
        );

        // Extract encrypted_changes from the private transaction
        let result = match transaction {
            cojson_core::Transaction::Private(private_tx) => PrivateTransactionResult{
                signature: signature.0,
                encrypted_changes: private_tx.encrypted_changes.value,
                meta: private_tx.meta.map(|meta| meta.value),
            },
            _ => return Err(CojsonCoreWasmError::Js(JsValue::from_str("Expected private transaction"))),
        };

        Ok(serde_json::to_string(&result)?)
    }

    #[wasm_bindgen(js_name = addNewTrustingTransaction)]
    pub fn add_new_trusting_transaction(
        &mut self,
        changes_json: &str,
        signer_secret: String,
        made_at: f64,
        meta: Option<String>,
    ) -> Result<String, CojsonCoreWasmError> {
        let (signature, _) = self.internal.add_new_transaction(
            changes_json,
            TransactionMode::Trusting,
            &SignerSecret(signer_secret),
            made_at as u64,
            meta,
        );

        Ok(signature.0)
    }

    #[wasm_bindgen(js_name = decryptNextTransactionChangesJson)]
    pub fn decrypt_next_transaction_changes_json(
        &self,
        tx_index: u32,
        encryption_key: String,
    ) -> Result<String, CojsonCoreWasmError> {
        Ok(self
            .internal
            .decrypt_next_transaction_changes_json(tx_index, KeySecret(encryption_key))?)
    }

    #[wasm_bindgen(js_name = decryptNextTransactionMetaJson)]
    pub fn decrypt_next_transaction_meta_json(
        &self,
        tx_index: u32,
        encryption_key: String,
    ) -> Result<Option<String>, CojsonCoreWasmError> {
        Ok(self.internal.decrypt_next_transaction_meta_json(tx_index, KeySecret(encryption_key))?)
    }
}
