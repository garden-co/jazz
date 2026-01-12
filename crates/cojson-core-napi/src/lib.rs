use cojson_core::core::{
  CoID, CoJsonCoreError, Encrypted, KeyID, KeySecret, PrivateTransaction, SessionID,
  SessionLogInternal, Signature, SignerID, SignerSecret, Transaction, TransactionMode,
  TrustingTransaction,
};
use napi::bindgen_prelude::BigInt;
use napi_derive::napi;
use serde::{Deserialize, Serialize};
use serde_json::Number;
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

#[napi(object)]
pub struct NapiFfiTransaction {
  /// "private" or "trusting"
  pub privacy: String,
  /// For private transactions
  pub encrypted_changes: Option<String>,
  /// For private transactions
  pub key_used: Option<String>,
  /// For trusting transactions
  pub changes: Option<String>,
  /// Timestamp (milliseconds) - BigInt for full u64 support
  pub made_at: BigInt,
  /// Optional meta (encrypted or stringified)
  pub meta: Option<String>,
}

fn to_transaction(tx: NapiFfiTransaction) -> napi::Result<Transaction> {
  // Extract u64 from BigInt (returns (sign, value) tuple, we take the value)
  let made_at = tx.made_at.get_u64().1;

  match tx.privacy.as_str() {
    "private" => {
      let encrypted_changes = tx.encrypted_changes.ok_or_else(|| {
        napi::Error::new(
          napi::Status::InvalidArg,
          "Missing encrypted_changes for private transaction".to_string(),
        )
      })?;
      let key_used = tx.key_used.ok_or_else(|| {
        napi::Error::new(
          napi::Status::InvalidArg,
          "Missing key_used for private transaction".to_string(),
        )
      })?;

      Ok(Transaction::Private(PrivateTransaction {
        encrypted_changes: Encrypted::new(encrypted_changes),
        key_used: KeyID(key_used),
        made_at: Number::from(made_at),
        meta: tx.meta.map(Encrypted::new),
        privacy: "private".to_string(),
      }))
    }
    "trusting" => {
      let changes = tx.changes.ok_or_else(|| {
        napi::Error::new(
          napi::Status::InvalidArg,
          "Missing changes for trusting transaction".to_string(),
        )
      })?;

      Ok(Transaction::Trusting(TrustingTransaction {
        changes,
        made_at: Number::from(made_at),
        meta: tx.meta,
        privacy: "trusting".to_string(),
      }))
    }
    other => Err(napi::Error::new(
      napi::Status::InvalidArg,
      format!("Invalid privacy type: {other}"),
    )),
  }
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
  pub fn try_add(
    &mut self,
    transactions_json: Vec<String>,
    new_signature_str: String,
    skip_verify: bool,
  ) -> napi::Result<()> {
    let new_signature = Signature(new_signature_str);

    self
      .internal
      .try_add(transactions_json, &new_signature, skip_verify)
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?;

    Ok(())
  }

  #[napi(js_name = "tryAddFfi")]
  pub fn try_add_ffi(
    &mut self,
    transactions: Vec<NapiFfiTransaction>,
    new_signature_str: String,
    skip_verify: bool,
  ) -> napi::Result<()> {
    let new_signature = Signature(new_signature_str);

    let transactions: Vec<Transaction> = transactions
      .into_iter()
      .map(to_transaction)
      .collect::<napi::Result<_>>()?;

    self
      .internal
      .try_add_transactions(transactions, &new_signature, skip_verify)
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?;

    Ok(())
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
