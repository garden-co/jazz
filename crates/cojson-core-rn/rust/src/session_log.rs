use cojson_core::core::{
    CoID, CoJsonCoreError, KeyID, KeySecret, SessionID, SessionLogInternal, Signature, SignerID,
    SignerSecret, Transaction, TransactionMode,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug, uniffi::Error)]
pub enum SessionLogError {
    #[error("CoJson error: {0}")]
    CoJson(String),
    #[error("Serialization error: {0}")]
    Serde(String),
    #[error("Error: {0}")]
    Generic(String),
    #[error("Failed to acquire lock")]
    LockError,
}

impl From<CoJsonCoreError> for SessionLogError {
    fn from(err: CoJsonCoreError) -> Self {
        SessionLogError::CoJson(err.to_string())
    }
}

impl From<serde_json::Error> for SessionLogError {
    fn from(err: serde_json::Error) -> Self {
        SessionLogError::Serde(err.to_string())
    }
}

#[derive(Serialize, Deserialize)]
struct PrivateTransactionResult {
    signature: String,
    encrypted_changes: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<String>,
}

#[derive(uniffi::Object)]
pub struct SessionLog {
    internal: std::sync::Mutex<SessionLogInternal>,
}

#[uniffi::export]
impl SessionLog {
    #[uniffi::constructor]
    pub fn new(co_id: String, session_id: String, signer_id: Option<String>) -> Self {
        let co_id = CoID(co_id);
        let session_id = SessionID(session_id);
        let signer_id = signer_id.map(SignerID);

        let internal = SessionLogInternal::new(co_id, session_id, signer_id);

        SessionLog {
            internal: std::sync::Mutex::new(internal),
        }
    }

    pub fn clone_session_log(&self) -> Result<Self, SessionLogError> {
        if let Ok(internal) = self.internal.lock() {
            Ok(SessionLog {
                internal: std::sync::Mutex::new(internal.clone()),
            })
        } else {
            Err(SessionLogError::LockError)
        }
    }

    pub fn try_add(
        &self,
        transactions_json: Vec<String>,
        new_signature_str: String,
        skip_verify: bool,
    ) -> Result<(), SessionLogError> {

        let new_signature = Signature(new_signature_str);

        if let Ok(mut internal) = self.internal.lock() {
            internal
                .try_add(transactions_json, &new_signature, skip_verify)
                .map_err(Into::into)
        } else {
            Err(SessionLogError::LockError)
        }
    }

    pub fn add_new_private_transaction(
        &self,
        changes_json: String,
        signer_secret: String,
        encryption_key: String,
        key_id: String,
        made_at: f64,
        meta: Option<String>,
    ) -> Result<String, SessionLogError> {
        if let Ok(mut internal) = self.internal.lock() {
            let (signature, transaction) = internal.add_new_transaction(
                &changes_json,
                TransactionMode::Private {
                    key_id: KeyID(key_id),
                    key_secret: KeySecret(encryption_key),
                },
                &SignerSecret(signer_secret),
                made_at as u64,
                meta,
            )?;

            // Extract encrypted_changes from the private transaction
            let result = match transaction {
                Transaction::Private(private_tx) => PrivateTransactionResult {
                    signature: signature.0,
                    encrypted_changes: private_tx.encrypted_changes.value,
                    meta: private_tx.meta.map(|meta| meta.value),
                },
                _ => {
                    return Err(SessionLogError::Generic(
                        "Expected a private transaction".to_string(),
                    ))
                }
            };

            Ok(serde_json::to_string(&result)?)
        } else {
            Err(SessionLogError::LockError)
        }
    }

    pub fn add_new_trusting_transaction(
        &self,
        changes_json: String,
        signer_secret: String,
        made_at: f64,
        meta: Option<String>,
    ) -> Result<String, SessionLogError> {
        if let Ok(mut internal) = self.internal.lock() {
            let (signature, _) = internal.add_new_transaction(
                &changes_json,
                TransactionMode::Trusting,
                &SignerSecret(signer_secret),
                made_at as u64,
                meta,
            )?;

            Ok(signature.0)
        } else {
            Err(SessionLogError::Generic(
                "Failed to acquire lock".to_string(),
            ))
        }
    }

    pub fn decrypt_next_transaction_changes_json(
        &self,
        tx_index: u32,
        encryption_key: String,
    ) -> Result<String, SessionLogError> {
        if let Ok(internal) = self.internal.lock() {
            internal
                .decrypt_next_transaction_changes_json(tx_index, KeySecret(encryption_key))
                .map_err(Into::into)
        } else {
            Err(SessionLogError::LockError)
        }
    }

    pub fn decrypt_next_transaction_meta_json(
        &self,
        tx_index: u32,
        encryption_key: String,
    ) -> Result<Option<String>, SessionLogError> {
        if let Ok(internal) = self.internal.lock() {
            internal
                .decrypt_next_transaction_meta_json(tx_index, KeySecret(encryption_key))
                .map_err(Into::into)
        } else {
            Err(SessionLogError::LockError)
        }
    }
}

