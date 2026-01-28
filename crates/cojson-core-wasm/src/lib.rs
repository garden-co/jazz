use cojson_core::core::{
    CoID, CoJsonCoreError, KeyID, KeySecret, SessionID, SessionLogInternal, SessionMapImpl,
    Signature, SignerID, SignerSecret, Transaction, TransactionMode,
};
use serde::{Deserialize, Serialize};
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

    /// Add an existing private transaction to the staging area.
    /// The transaction is NOT committed until commitTransactions() succeeds.
    /// Note: made_at uses f64 because JavaScript's number type is f64.
    #[wasm_bindgen(js_name = addExistingPrivateTransaction)]
    pub fn add_existing_private_transaction(
        &mut self,
        encrypted_changes: String,
        key_used: String,
        made_at: f64,
        meta: Option<String>,
    ) -> Result<(), CojsonCoreWasmError> {
        self.internal
            .add_existing_private_transaction(encrypted_changes, key_used, made_at as u64, meta)
            .map_err(CojsonCoreWasmError::CoJson)
    }

    /// Add an existing trusting transaction to the staging area.
    /// The transaction is NOT committed until commitTransactions() succeeds.
    /// Note: made_at uses f64 because JavaScript's number type is f64.
    #[wasm_bindgen(js_name = addExistingTrustingTransaction)]
    pub fn add_existing_trusting_transaction(
        &mut self,
        changes: String,
        made_at: f64,
        meta: Option<String>,
    ) -> Result<(), CojsonCoreWasmError> {
        self.internal
            .add_existing_trusting_transaction(changes, made_at as u64, meta)
            .map_err(CojsonCoreWasmError::CoJson)
    }

    /// Commit pending transactions to the main state.
    /// If skip_validate is false, validates the signature first.
    /// If skip_validate is true, commits without validation.
    #[wasm_bindgen(js_name = commitTransactions)]
    pub fn commit_transactions(
        &mut self,
        new_signature_str: String,
        skip_validate: bool,
    ) -> Result<(), CojsonCoreWasmError> {
        let new_signature = Signature(new_signature_str);
        self.internal
            .commit_transactions(&new_signature, skip_validate)
            .map_err(CojsonCoreWasmError::CoJson)
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

// ============================================================================
// SessionMap - WASM wrapper for SessionMapImpl
// ============================================================================

#[wasm_bindgen]
pub struct SessionMap {
    internal: SessionMapImpl,
}

#[wasm_bindgen]
impl SessionMap {
    /// Create a new SessionMap for a CoValue
    /// `max_tx_size` is the threshold for recording in-between signatures (default: 100KB)
    #[wasm_bindgen(constructor)]
    pub fn new(
        co_id: String,
        header_json: String,
        max_tx_size: Option<u32>,
    ) -> Result<SessionMap, CojsonCoreWasmError> {
        let internal = SessionMapImpl::new(&co_id, &header_json, max_tx_size)
            .map_err(|e| CojsonCoreWasmError::Js(JsValue::from_str(&e.to_string())))?;
        Ok(SessionMap { internal })
    }

    // === Header ===

    /// Get the header as JSON
    #[wasm_bindgen(js_name = getHeader)]
    pub fn get_header(&self) -> String {
        self.internal.get_header()
    }

    // === Transaction Operations ===

    /// Add transactions to a session
    #[wasm_bindgen(js_name = addTransactions)]
    pub fn add_transactions(
        &mut self,
        session_id: String,
        signer_id: Option<String>,
        transactions_json: String,
        signature: String,
        skip_verify: bool,
    ) -> Result<(), CojsonCoreWasmError> {
        self.internal
            .add_transactions(
                &session_id,
                signer_id.as_deref(),
                &transactions_json,
                &signature,
                skip_verify,
            )
            .map_err(|e| CojsonCoreWasmError::Js(JsValue::from_str(&e.to_string())))
    }

    /// Create new private transaction (for local writes)
    /// Returns JSON: { signature: string, transaction: Transaction }
    #[wasm_bindgen(js_name = makeNewPrivateTransaction)]
    pub fn make_new_private_transaction(
        &mut self,
        session_id: String,
        signer_secret: String,
        changes_json: String,
        key_id: String,
        key_secret: String,
        meta_json: Option<String>,
        made_at: f64,
    ) -> Result<String, CojsonCoreWasmError> {
        self.internal
            .make_new_private_transaction(
                &session_id,
                &signer_secret,
                &changes_json,
                &key_id,
                &key_secret,
                meta_json.as_deref(),
                made_at as u64,
            )
            .map_err(|e| CojsonCoreWasmError::Js(JsValue::from_str(&e.to_string())))
    }

    /// Create new trusting transaction (for local writes)
    /// Returns JSON: { signature: string, transaction: Transaction }
    #[wasm_bindgen(js_name = makeNewTrustingTransaction)]
    pub fn make_new_trusting_transaction(
        &mut self,
        session_id: String,
        signer_secret: String,
        changes_json: String,
        meta_json: Option<String>,
        made_at: f64,
    ) -> Result<String, CojsonCoreWasmError> {
        self.internal
            .make_new_trusting_transaction(
                &session_id,
                &signer_secret,
                &changes_json,
                meta_json.as_deref(),
                made_at as u64,
            )
            .map_err(|e| CojsonCoreWasmError::Js(JsValue::from_str(&e.to_string())))
    }

    // === Session Queries ===

    /// Get all session IDs as native array
    #[wasm_bindgen(js_name = getSessionIds)]
    pub fn get_session_ids(&self) -> Vec<String> {
        self.internal.get_session_ids()
    }

    /// Get transaction count for a session (returns -1 if session not found)
    #[wasm_bindgen(js_name = getTransactionCount)]
    pub fn get_transaction_count(&self, session_id: String) -> i32 {
        self.internal
            .get_transaction_count(&session_id)
            .map(|c| c as i32)
            .unwrap_or(-1)
    }

    /// Get single transaction by index (returns undefined if not found)
    #[wasm_bindgen(js_name = getTransaction)]
    pub fn get_transaction(&self, session_id: String, tx_index: u32) -> Option<String> {
        self.internal.get_transaction(&session_id, tx_index)
    }

    /// Get transactions for a session from index (returns undefined if session not found)
    #[wasm_bindgen(js_name = getSessionTransactions)]
    pub fn get_session_transactions(
        &self,
        session_id: String,
        from_index: u32,
    ) -> Option<Vec<String>> {
        self.internal
            .get_session_transactions(&session_id, from_index)
    }

    /// Get last signature for a session (returns undefined if session not found)
    #[wasm_bindgen(js_name = getLastSignature)]
    pub fn get_last_signature(&self, session_id: String) -> Option<String> {
        self.internal.get_last_signature(&session_id)
    }

    /// Get signature after specific transaction index
    #[wasm_bindgen(js_name = getSignatureAfter)]
    pub fn get_signature_after(&self, session_id: String, tx_index: u32) -> Option<String> {
        self.internal.get_signature_after(&session_id, tx_index)
    }

    /// Get the last signature checkpoint index (-1 if no checkpoints, undefined if session not found)
    #[wasm_bindgen(js_name = getLastSignatureCheckpoint)]
    pub fn get_last_signature_checkpoint(&self, session_id: String) -> Option<i32> {
        self.internal.get_last_signature_checkpoint(&session_id)
    }

    // === Known State ===

    /// Get the known state as a native JavaScript object
    #[wasm_bindgen(js_name = getKnownState)]
    pub fn get_known_state(&self) -> JsValue {
        // Use serialize_maps_as_objects to convert BTreeMap to JS object instead of Map
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        self.internal
            .get_known_state()
            .serialize(&serializer)
            .expect("KnownState serialization should not fail")
    }

    /// Get the known state with streaming as a native JavaScript object
    #[wasm_bindgen(js_name = getKnownStateWithStreaming)]
    pub fn get_known_state_with_streaming(&self) -> JsValue {
        match self.internal.get_known_state_with_streaming() {
            Some(ks) => {
                // Use serialize_maps_as_objects to convert BTreeMap to JS object instead of Map
                let serializer =
                    serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
                ks.serialize(&serializer)
                    .expect("KnownState serialization should not fail")
            }
            None => JsValue::undefined(),
        }
    }

    /// Set streaming known state
    #[wasm_bindgen(js_name = setStreamingKnownState)]
    pub fn set_streaming_known_state(
        &mut self,
        streaming_json: String,
    ) -> Result<(), CojsonCoreWasmError> {
        self.internal
            .set_streaming_known_state(&streaming_json)
            .map_err(|e| CojsonCoreWasmError::Js(JsValue::from_str(&e.to_string())))
    }

    // === Deletion ===

    /// Mark this CoValue as deleted
    #[wasm_bindgen(js_name = markAsDeleted)]
    pub fn mark_as_deleted(&mut self) {
        self.internal.mark_as_deleted();
    }

    /// Check if this CoValue is deleted
    #[wasm_bindgen(js_name = isDeleted)]
    pub fn is_deleted(&self) -> bool {
        self.internal.is_deleted()
    }

    // === Decryption ===

    /// Decrypt transaction changes
    #[wasm_bindgen(js_name = decryptTransaction)]
    pub fn decrypt_transaction(
        &self,
        session_id: String,
        tx_index: u32,
        key_secret: String,
    ) -> Result<Option<String>, CojsonCoreWasmError> {
        self.internal
            .decrypt_transaction(&session_id, tx_index, &key_secret)
            .map_err(|e| CojsonCoreWasmError::Js(JsValue::from_str(&e.to_string())))
    }

    /// Decrypt transaction meta
    #[wasm_bindgen(js_name = decryptTransactionMeta)]
    pub fn decrypt_transaction_meta(
        &self,
        session_id: String,
        tx_index: u32,
        key_secret: String,
    ) -> Result<Option<String>, CojsonCoreWasmError> {
        self.internal
            .decrypt_transaction_meta(&session_id, tx_index, &key_secret)
            .map_err(|e| CojsonCoreWasmError::Js(JsValue::from_str(&e.to_string())))
    }
}
