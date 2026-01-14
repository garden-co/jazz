use cojson_core::core::{
    CoID, CoJsonCoreError, Encrypted, KeyID, KeySecret, PrivateTransaction, SessionID,
    SessionLogInternal, Signature, SignerID, SignerSecret, Transaction, TransactionMode,
    TrustingTransaction,
};
use serde::{Deserialize, Serialize};
use serde_json::Number;
use thiserror::Error;
use wasm_bindgen::prelude::*;

pub mod hash {
    pub mod blake3;
    pub use blake3::*;
}

pub mod crypto {
    pub mod ed25519;
    pub mod encrypt;
    pub mod seal;
    pub mod signature;
    pub mod x25519;
    pub mod xsalsa20;

    pub use ed25519::*;
    pub use encrypt::*;
    pub use seal::*;
    pub use signature::*;
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

/// WASM-compatible FFI Transaction struct.
/// Can be passed directly from JavaScript without JSON serialization.
#[wasm_bindgen(getter_with_clone)]
pub struct WasmFfiTransaction {
    /// "private" or "trusting"
    pub privacy: String,
    /// For private transactions: the key ID used for encryption
    pub key_used: Option<String>,
    /// Transaction payload:
    /// - for private transactions: the encrypted changes string (e.g., "encrypted_U...")
    /// - for trusting transactions: the stringified changes JSON
    pub changes: String,
    /// Timestamp when the transaction was made (milliseconds)
    pub made_at: u64,
    /// Optional meta (encrypted for private, stringified for trusting)
    pub meta: Option<String>,
}

#[wasm_bindgen]
impl WasmFfiTransaction {
    #[wasm_bindgen(constructor)]
    pub fn new(
        privacy: String,
        key_used: Option<String>,
        changes: String,
        made_at: u64,
        meta: Option<String>,
    ) -> WasmFfiTransaction {
        WasmFfiTransaction {
            privacy,
            key_used,
            changes,
            made_at,
            meta,
        }
    }
}

/// Convert WasmFfiTransaction to internal Transaction type.
/// Maps directly to PrivateTransaction or TrustingTransaction based on privacy field.
fn to_transaction(wasm: WasmFfiTransaction) -> Result<Transaction, CojsonCoreWasmError> {
    match wasm.privacy.as_str() {
        "private" => {
            let key_used = wasm.key_used.ok_or_else(|| {
                CojsonCoreWasmError::Js(JsValue::from_str(
                    "Missing key_used for private transaction",
                ))
            })?;

            Ok(Transaction::Private(PrivateTransaction {
                encrypted_changes: Encrypted::new(wasm.changes),
                key_used: KeyID(key_used),
                made_at: Number::from(wasm.made_at),
                meta: wasm.meta.map(Encrypted::new),
                privacy: "private".to_string(),
            }))
        }
        "trusting" => Ok(Transaction::Trusting(TrustingTransaction {
            changes: wasm.changes,
            made_at: Number::from(wasm.made_at),
            meta: wasm.meta,
            privacy: "trusting".to_string(),
        })),
        _ => Err(CojsonCoreWasmError::Js(JsValue::from_str(&format!(
            "Invalid privacy type: {}",
            wasm.privacy
        )))),
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
        let signer_id = signer_id.map(SignerID);

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
        let new_signature = Signature(new_signature_str);

        self.internal
            .try_add(transactions_json, &new_signature, skip_verify)?;

        Ok(())
    }

    /// FFI-optimized version of tryAdd that accepts typed transaction structs.
    /// Avoids JSON.stringify on the JavaScript side by accepting WasmFfiTransaction objects.
    #[wasm_bindgen(js_name = tryAddFfi)]
    pub fn try_add_ffi(
        &mut self,
        transactions: Vec<WasmFfiTransaction>,
        new_signature_str: String,
        skip_verify: bool,
    ) -> Result<(), CojsonCoreWasmError> {
        let new_signature = Signature(new_signature_str);

        // Convert WasmFfiTransaction objects directly to internal Transaction type
        let transactions: Vec<Transaction> = transactions
            .into_iter()
            .map(to_transaction)
            .collect::<Result<Vec<_>, _>>()?;

        self.internal
            .try_add_transactions(transactions, &new_signature, skip_verify)?;

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
        let (signature, transaction) = self
            .internal
            .add_new_transaction(
                changes_json,
                TransactionMode::Private {
                    key_id: KeyID(key_id),
                    key_secret: KeySecret(encryption_key),
                },
                &SignerSecret(signer_secret),
                made_at as u64,
                meta,
            )
            .map_err(CojsonCoreWasmError::CoJson)?;

        // Extract encrypted_changes from the private transaction
        let result = match transaction {
            Transaction::Private(private_tx) => PrivateTransactionResult {
                signature: signature.0,
                encrypted_changes: private_tx.encrypted_changes.value,
                meta: private_tx.meta.map(|meta| meta.value),
            },
            _ => {
                return Err(CojsonCoreWasmError::Js(JsValue::from_str(
                    "Expected private transaction",
                )))
            }
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
        let (signature, _) = self
            .internal
            .add_new_transaction(
                changes_json,
                TransactionMode::Trusting,
                &SignerSecret(signer_secret),
                made_at as u64,
                meta,
            )
            .map_err(CojsonCoreWasmError::CoJson)?;

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
        Ok(self
            .internal
            .decrypt_next_transaction_meta_json(tx_index, KeySecret(encryption_key))?)
    }
}
