use cojson_core::{
    CoID, CoJsonCoreError, KeyID, KeySecret, SessionID, SessionLogInternal, Signature, SignerID, SignerSecret, Transaction, TransactionMode
};
use serde_json::value::RawValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use wasm_bindgen::prelude::*;

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
struct MakeTransactionResult {
    signature: String,
    transaction: Transaction,
    hash: String,
}

#[wasm_bindgen]
impl SessionLog {
    #[wasm_bindgen(constructor)]
    pub fn new(co_id: String, session_id: String, signer_id: String) -> SessionLog {
        let co_id = CoID(co_id);
        let session_id = SessionID(session_id);
        let signer_id = SignerID(signer_id);

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
    ) -> Result<String, CojsonCoreWasmError> {
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

        let new_hash = self.internal
            .try_add(transactions, &new_signature, skip_verify)?;

        Ok(new_hash.0)
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
        let (hash, signature, transaction) = self.internal.add_new_transaction(
            changes_json,
            TransactionMode::Private{key_id: KeyID(key_id), key_secret: KeySecret(encryption_key)},
            &SignerSecret(signer_secret),
            made_at as u64,
        );

        let result = MakeTransactionResult{
            signature: signature.0,
            transaction,
            hash: hash.0,
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
        let (hash, signature, transaction) = self.internal.add_new_transaction(
            changes_json,
            TransactionMode::Trusting,
            &SignerSecret(signer_secret),
            made_at as u64,
        );

        let result = MakeTransactionResult{
            signature: signature.0,
            transaction,
            hash: hash.0,
        };

        Ok(serde_json::to_string(&result)?)
    }

    #[wasm_bindgen(js_name = testExpectedHashAfter)]
    pub fn test_expected_hash_after(&self, transactions_js: JsValue) -> Result<String, CojsonCoreWasmError> {
        let transactions_str: Vec<String> = serde_wasm_bindgen::from_value(transactions_js)?;
        let transactions: Vec<Box<RawValue>> = transactions_str
            .into_iter()
            .map(|s| serde_json::from_str(&s).unwrap())
            .collect();

        Ok(self.internal.test_expected_hash_after(&transactions))
    }

    #[wasm_bindgen(js_name = decryptNextTransactionChangesJson)]
    pub fn decrypt_next_transaction_changes_json(
        &self,
        tx_index: u32,
        key_secret: &[u8],
    ) -> Result<String, CojsonCoreWasmError> {
        Ok(self
            .internal
            .decrypt_next_transaction_changes_json(tx_index, key_secret)?)
    }
}