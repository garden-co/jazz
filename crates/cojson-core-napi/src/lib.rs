use cojson_core::core::{
  CoID, CoJsonCoreError, KeyID, KeySecret, SessionID, SessionLogInternal, SessionMapImpl,
  Signature, SignerID, SignerSecret, Transaction, TransactionMode,
};
use napi_derive::napi;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
pub enum CojsonCoreError {
  #[error(transparent)]
  CoJson(#[from] CoJsonCoreError),
  #[error(transparent)]
  Serde(#[from] serde_json::Error),
  #[error("String Error: {0:?}")]
  Js(String),
}

impl From<CojsonCoreError> for String {
  fn from(err: CojsonCoreError) -> Self {
    err.to_string()
  }
}

#[napi]
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

#[napi]
impl SessionLog {
  #[napi(constructor)]
  pub fn new(co_id: String, session_id: String, signer_id: Option<String>) -> SessionLog {
    let co_id = CoID(co_id);
    let session_id = SessionID(session_id);
    let signer_id = signer_id.map(SignerID);

    let internal = SessionLogInternal::new(co_id, session_id, signer_id);

    SessionLog { internal }
  }

  #[napi(js_name = "clone")]
  pub fn clone_js(&self) -> SessionLog {
    self.clone()
  }

  #[napi]
  pub fn add_new_private_transaction(
    &mut self,
    changes_json: String,
    signer_secret: String,
    encryption_key: String,
    key_id: String,
    made_at: f64,
    meta: Option<String>,
  ) -> napi::Result<String> {
    let (signature, transaction) = self
      .internal
      .add_new_transaction(
        &changes_json,
        TransactionMode::Private {
          key_id: KeyID(key_id),
          key_secret: KeySecret(encryption_key),
        },
        &SignerSecret(signer_secret),
        made_at as u64,
        meta,
      )
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?;

    // Extract encrypted_changes from the private transaction
    let result = match transaction {
      Transaction::Private(private_tx) => PrivateTransactionResult {
        signature: signature.0,
        encrypted_changes: private_tx.encrypted_changes.value,
        meta: private_tx.meta.map(|meta| meta.value),
      },
      _ => {
        return napi::Result::Err(napi::Error::new(
          napi::Status::GenericFailure,
          "Expected a private transaction".to_string(),
        ))
      }
    };

    Ok(serde_json::to_string(&result)?)
  }

  #[napi]
  pub fn add_new_trusting_transaction(
    &mut self,
    changes_json: String,
    signer_secret: String,
    made_at: f64,
    meta: Option<String>,
  ) -> napi::Result<String> {
    let (signature, _) = self
      .internal
      .add_new_transaction(
        &changes_json,
        TransactionMode::Trusting,
        &SignerSecret(signer_secret),
        made_at as u64,
        meta,
      )
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?;

    Ok(signature.0)
  }

  /// Add an existing private transaction to the staging area.
  /// The transaction is NOT committed until commitTransactions() succeeds.
  /// Note: made_at uses f64 because JavaScript's number type is f64.
  #[napi]
  pub fn add_existing_private_transaction(
    &mut self,
    encrypted_changes: String,
    key_used: String,
    made_at: f64,
    meta: Option<String>,
  ) -> napi::Result<()> {
    self
      .internal
      .add_existing_private_transaction(encrypted_changes, key_used, made_at as u64, meta)
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
  }

  /// Add an existing trusting transaction to the staging area.
  /// The transaction is NOT committed until commitTransactions() succeeds.
  /// Note: made_at uses f64 because JavaScript's number type is f64.
  #[napi]
  pub fn add_existing_trusting_transaction(
    &mut self,
    changes: String,
    made_at: f64,
    meta: Option<String>,
  ) -> napi::Result<()> {
    self
      .internal
      .add_existing_trusting_transaction(changes, made_at as u64, meta)
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
  }

  /// Commit pending transactions to the main state.
  /// If skip_validate is false, validates the signature first.
  /// If skip_validate is true, commits without validation.
  #[napi]
  pub fn commit_transactions(
    &mut self,
    new_signature_str: String,
    skip_validate: bool,
  ) -> napi::Result<()> {
    let new_signature = Signature(new_signature_str);
    self
      .internal
      .commit_transactions(&new_signature, skip_validate)
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
  }

  #[napi]
  pub fn decrypt_next_transaction_changes_json(
    &self,
    tx_index: u32,
    encryption_key: String,
  ) -> napi::Result<String> {
    self
      .internal
      .decrypt_next_transaction_changes_json(tx_index, KeySecret(encryption_key))
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
  }

  #[napi]
  pub fn decrypt_next_transaction_meta_json(
    &self,
    tx_index: u32,
    encryption_key: String,
  ) -> napi::Result<Option<String>> {
    self
      .internal
      .decrypt_next_transaction_meta_json(tx_index, KeySecret(encryption_key))
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
  }
}

// ============================================================================
// SessionMap - NAPI wrapper for SessionMapImpl
// ============================================================================

#[napi]
pub struct SessionMap {
  internal: SessionMapImpl,
}

#[napi]
impl SessionMap {
  /// Create a new SessionMap for a CoValue
  #[napi(constructor)]
  pub fn new(co_id: String, header_json: String) -> napi::Result<SessionMap> {
    let internal = SessionMapImpl::new(&co_id, &header_json)
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?;
    Ok(SessionMap { internal })
  }

  // === Header ===

  /// Get the header as JSON
  #[napi]
  pub fn get_header(&self) -> String {
    self.internal.get_header()
  }

  // === Transaction Operations ===

  /// Add transactions to a session
  #[napi]
  pub fn add_transactions(
    &mut self,
    session_id: String,
    signer_id: Option<String>,
    transactions_json: String,
    signature: String,
    skip_verify: bool,
  ) -> napi::Result<()> {
    self
      .internal
      .add_transactions(
        &session_id,
        signer_id.as_deref(),
        &transactions_json,
        &signature,
        skip_verify,
      )
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
  }

  /// Create new private transaction (for local writes)
  /// Returns JSON: { signature: string, transaction: Transaction }
  #[napi]
  pub fn make_new_private_transaction(
    &mut self,
    session_id: String,
    signer_secret: String,
    changes_json: String,
    key_id: String,
    key_secret: String,
    meta_json: Option<String>,
    made_at: f64,
  ) -> napi::Result<String> {
    self
      .internal
      .make_new_private_transaction(
        &session_id,
        &signer_secret,
        &changes_json,
        &key_id,
        &key_secret,
        meta_json.as_deref(),
        made_at as u64,
      )
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
  }

  /// Create new trusting transaction (for local writes)
  /// Returns JSON: { signature: string, transaction: Transaction }
  #[napi]
  pub fn make_new_trusting_transaction(
    &mut self,
    session_id: String,
    signer_secret: String,
    changes_json: String,
    meta_json: Option<String>,
    made_at: f64,
  ) -> napi::Result<String> {
    self
      .internal
      .make_new_trusting_transaction(
        &session_id,
        &signer_secret,
        &changes_json,
        meta_json.as_deref(),
        made_at as u64,
      )
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
  }

  // === Session Queries ===

  /// Get all session IDs as JSON array
  #[napi]
  pub fn get_session_ids(&self) -> napi::Result<String> {
    let ids = self.internal.get_session_ids();
    serde_json::to_string(&ids)
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
  }

  /// Get transaction count for a session (returns -1 if session not found)
  #[napi]
  pub fn get_transaction_count(&self, session_id: String) -> i32 {
    self
      .internal
      .get_transaction_count(&session_id)
      .map(|c| c as i32)
      .unwrap_or(-1)
  }

  /// Get single transaction by index (returns undefined if not found)
  #[napi]
  pub fn get_transaction(&self, session_id: String, tx_index: u32) -> Option<String> {
    self.internal.get_transaction(&session_id, tx_index)
  }

  /// Get transactions for a session from index (returns undefined if session not found)
  #[napi]
  pub fn get_session_transactions(&self, session_id: String, from_index: u32) -> Option<String> {
    self.internal.get_session_transactions(&session_id, from_index)
  }

  /// Get last signature for a session (returns undefined if session not found)
  #[napi]
  pub fn get_last_signature(&self, session_id: String) -> Option<String> {
    self.internal.get_last_signature(&session_id)
  }

  /// Get signature after specific transaction index
  #[napi]
  pub fn get_signature_after(&self, session_id: String, tx_index: u32) -> Option<String> {
    self.internal.get_signature_after(&session_id, tx_index)
  }

  /// Get the last signature checkpoint index (-1 if no checkpoints, undefined if session not found)
  #[napi]
  pub fn get_last_signature_checkpoint(&self, session_id: String) -> Option<i32> {
    self.internal.get_last_signature_checkpoint(&session_id)
  }

  // === Known State ===

  /// Get the known state as JSON
  #[napi]
  pub fn get_known_state(&self) -> String {
    self.internal.get_known_state()
  }

  /// Get the known state with streaming as JSON (returns undefined if no streaming)
  #[napi]
  pub fn get_known_state_with_streaming(&self) -> Option<String> {
    self.internal.get_known_state_with_streaming()
  }

  /// Set streaming known state
  #[napi]
  pub fn set_streaming_known_state(&mut self, streaming_json: String) -> napi::Result<()> {
    self
      .internal
      .set_streaming_known_state(&streaming_json)
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
  }

  // === Deletion ===

  /// Mark this CoValue as deleted
  #[napi]
  pub fn mark_as_deleted(&mut self) {
    self.internal.mark_as_deleted();
  }

  /// Check if this CoValue is deleted
  #[napi]
  pub fn is_deleted(&self) -> bool {
    self.internal.is_deleted()
  }

  // === Decryption ===

  /// Decrypt transaction changes
  #[napi]
  pub fn decrypt_transaction(
    &self,
    session_id: String,
    tx_index: u32,
    key_secret: String,
  ) -> napi::Result<Option<String>> {
    self
      .internal
      .decrypt_transaction(&session_id, tx_index, &key_secret)
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
  }

  /// Decrypt transaction meta
  #[napi]
  pub fn decrypt_transaction_meta(
    &self,
    session_id: String,
    tx_index: u32,
    key_secret: String,
  ) -> napi::Result<Option<String>> {
    self
      .internal
      .decrypt_transaction_meta(&session_id, tx_index, &key_secret)
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
  }
}
