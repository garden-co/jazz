use cojson_core::{
    CoID, KeyID, KeySecret, SessionID, SessionLogInternal, Signature, SignerID, SignerSecret,
    TransactionMode,
};
use cojson_core::{seal_internal, unseal_internal};
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

#[cxx::bridge]
mod ffi {
    // Shared types between Rust and C++
    struct SessionLogHandle {
        id: u64,
    }

    struct TransactionResult {
        success: bool,
        result: String,
        error: String,
    }

    #[derive(Serialize)]
    struct MakeTransactionResult {
        signature: String,
        transaction: String,
        hash: String,
    }

    struct U8VecResult {
        success: bool,
        data: Vec<u8>,
        error: String,
    }

    // Rust functions exposed to C++
    extern "Rust" {
        fn create_session_log(
            co_id: String,
            session_id: String,
            signer_id: String,
        ) -> SessionLogHandle;
        fn clone_session_log(handle: &SessionLogHandle) -> SessionLogHandle;
        fn try_add_transactions(
            handle: &SessionLogHandle,
            transactions_json: Vec<String>,
            new_signature: String,
            skip_verify: bool,
        ) -> TransactionResult;
        fn add_new_private_transaction(
            handle: &SessionLogHandle,
            changes_json: String,
            signer_secret: String,
            encryption_key: String,
            key_id: String,
            made_at: f64,
            meta: String,
        ) -> TransactionResult;
        fn add_new_trusting_transaction(
            handle: &SessionLogHandle,
            changes_json: String,
            signer_secret: String,
            made_at: f64,
            meta: String,
        ) -> TransactionResult;
        fn test_expected_hash_after(
            handle: &SessionLogHandle,
            transactions_json: Vec<String>,
        ) -> TransactionResult;
        fn decrypt_next_transaction_changes_json(
            handle: &SessionLogHandle,
            tx_index: u32,
            key_secret: Vec<u8>,
        ) -> TransactionResult;
        fn seal_message(
            message: Vec<u8>,
            sender_secret: String,
            recipient_id: String,
            nonce_material: Vec<u8>,
        ) -> U8VecResult;
        fn unseal_message(
            sealed_message: Vec<u8>,
            recipient_secret: String,
            sender_id: String,
            nonce_material: Vec<u8>,
        ) -> U8VecResult;
        fn destroy_session_log(handle: &SessionLogHandle);
    }
}

// Internal storage for SessionLog instances - thread-safe
static SESSION_LOGS: OnceLock<Mutex<HashMap<u64, SessionLogInternal>>> = OnceLock::new();
static NEXT_ID: Mutex<u64> = Mutex::new(1);

// Initialize the storage
fn ensure_storage() -> &'static Mutex<HashMap<u64, SessionLogInternal>> {
    SESSION_LOGS.get_or_init(|| Mutex::new(HashMap::new()))
}

// Get the next unique ID
fn get_next_id() -> u64 {
    let mut id = NEXT_ID.lock().unwrap();
    let current = *id;
    *id += 1;
    current
}

// Helper function to create error result
fn error_result(error: String) -> ffi::TransactionResult {
    ffi::TransactionResult {
        success: false,
        result: String::new(),
        error,
    }
}

// Helper function to create success result
fn success_result(result: String) -> ffi::TransactionResult {
    ffi::TransactionResult {
        success: true,
        result,
        error: String::new(),
    }
}

// Helper function to create U8VecResult success
fn u8vec_success_result(data: Vec<u8>) -> ffi::U8VecResult {
    ffi::U8VecResult {
        success: true,
        data,
        error: String::new(),
    }
}

// Helper function to create U8VecResult error
fn u8vec_error_result(error: String) -> ffi::U8VecResult {
    ffi::U8VecResult {
        success: false,
        data: Vec::new(),
        error,
    }
}

// FFI function implementations
pub fn create_session_log(
    co_id: String,
    session_id: String,
    signer_id: String,
) -> ffi::SessionLogHandle {
    let co_id = CoID(co_id);
    let session_id = SessionID(session_id);
    let signer_id = SignerID(signer_id);

    let internal = match SessionLogInternal::new(co_id, session_id, Some(signer_id)) {
        Ok(internal) => internal,
        Err(_) => {
            // Return a handle with id 0 to indicate error - this will be handled by the caller
            return ffi::SessionLogHandle { id: 0 };
        }
    };
    let id = get_next_id();

    let storage = ensure_storage();
    let mut logs = storage.lock().unwrap();
    logs.insert(id, internal);

    ffi::SessionLogHandle { id }
}

pub fn clone_session_log(handle: &ffi::SessionLogHandle) -> ffi::SessionLogHandle {
    let storage = ensure_storage();
    let mut logs = storage.lock().unwrap();

    if let Some(log) = logs.get(&handle.id) {
        let cloned = log.clone();
        let new_id = get_next_id();
        logs.insert(new_id, cloned);
        return ffi::SessionLogHandle { id: new_id };
    }

    // Return invalid handle if not found
    ffi::SessionLogHandle { id: 0 }
}

pub fn try_add_transactions(
    handle: &ffi::SessionLogHandle,
    transactions_json: Vec<String>,
    new_signature: String,
    skip_verify: bool,
) -> ffi::TransactionResult {
    let storage = ensure_storage();
    let mut logs = storage.lock().unwrap();

    if let Some(log) = logs.get_mut(&handle.id) {
        let transactions: Result<Vec<Box<RawValue>>, _> = transactions_json
            .into_iter()
            .map(|s| serde_json::from_str(&s))
            .collect();

        match transactions {
            Ok(transactions) => {
                let signature = Signature(new_signature);
                match log.try_add(transactions, &signature, skip_verify) {
                    Ok(()) => success_result("success".to_string()),
                    Err(e) => error_result(format!("Failed to add transactions: {}", e)),
                }
            }
            Err(e) => error_result(format!("Failed to parse transactions: {}", e)),
        }
    } else {
        error_result("Invalid session log handle".to_string())
    }
}

pub fn add_new_private_transaction(
    handle: &ffi::SessionLogHandle,
    changes_json: String,
    signer_secret: String,
    encryption_key: String,
    key_id: String,
    made_at: f64,
    meta: String,
) -> ffi::TransactionResult {
    let storage = ensure_storage();
    
    // Use safe lock handling to prevent panic on poisoned mutex
    let mut logs = match storage.lock() {
        Ok(logs) => logs,
        Err(poisoned) => {
            // Recover from poisoned mutex by taking the guard anyway
            poisoned.into_inner()
        }
    };

    if let Some(log) = logs.get_mut(&handle.id) {
        match log.try_add_new_transaction(
            &changes_json,
            TransactionMode::Private {
                key_id: KeyID(key_id),
                key_secret: KeySecret(encryption_key),
            },
            &SignerSecret(signer_secret),
            made_at as u64,
            if meta.is_empty() { None } else { Some(meta) },
        ) {
            Ok((signature, transaction)) => {
                match serde_json::to_string(&serde_json::json!({
                    "signature": signature.0,
                    "transaction": transaction
                })) {
                    Ok(json) => success_result(json),
                    Err(e) => error_result(format!("Failed to serialize result: {}", e)),
                }
            }
            Err(e) => error_result(format!("Transaction creation failed: {}", e)),
        }
    } else {
        error_result("Invalid session log handle".to_string())
    }
}

pub fn add_new_trusting_transaction(
    handle: &ffi::SessionLogHandle,
    changes_json: String,
    signer_secret: String,
    made_at: f64,
    meta: String,
) -> ffi::TransactionResult {
    let storage = ensure_storage();
    
    // Use safe lock handling to prevent panic on poisoned mutex
    let mut logs = match storage.lock() {
        Ok(logs) => logs,
        Err(poisoned) => {
            // Recover from poisoned mutex by taking the guard anyway
            poisoned.into_inner()
        }
    };

    if let Some(log) = logs.get_mut(&handle.id) {
        match log.try_add_new_transaction(
            &changes_json,
            TransactionMode::Trusting,
            &SignerSecret(signer_secret),
            made_at as u64,
            if meta.is_empty() { None } else { Some(meta) },
        ) {
            Ok((signature, transaction)) => {
                match serde_json::to_string(&serde_json::json!({
                    "signature": signature.0,
                    "transaction": transaction
                })) {
                    Ok(json) => success_result(json),
                    Err(e) => error_result(format!("Failed to serialize result: {}", e)),
                }
            }
            Err(e) => error_result(format!("Transaction creation failed: {}", e)),
        }
    } else {
        error_result("Invalid session log handle".to_string())
    }
}

pub fn test_expected_hash_after(
    handle: &ffi::SessionLogHandle,
    transactions_json: Vec<String>,
) -> ffi::TransactionResult {
    let storage = ensure_storage();
    let logs = storage.lock().unwrap();

    if let Some(log) = logs.get(&handle.id) {
        let transactions: Result<Vec<Box<RawValue>>, _> = transactions_json
            .into_iter()
            .map(|s| serde_json::from_str(&s))
            .collect();

        match transactions {
            Ok(transactions) => {
                let hash = log.test_expected_hash_after(&transactions);
                success_result(hash)
            }
            Err(e) => error_result(format!("Failed to parse transactions: {}", e)),
        }
    } else {
        error_result("Invalid session log handle".to_string())
    }
}

pub fn decrypt_next_transaction_changes_json(
    handle: &ffi::SessionLogHandle,
    tx_index: u32,
    key_secret: Vec<u8>,
) -> ffi::TransactionResult {
    let storage = ensure_storage();
    let logs = storage.lock().unwrap();

    if let Some(log) = logs.get(&handle.id) {
        let key_secret = KeySecret(String::from_utf8_lossy(&key_secret).to_string());
        match log.decrypt_next_transaction_changes_json(tx_index, key_secret) {
            Ok(changes) => success_result(changes),
            Err(e) => error_result(format!("Failed to decrypt transaction: {}", e)),
        }
    } else {
        error_result("Invalid session log handle".to_string())
    }
}

pub fn destroy_session_log(handle: &ffi::SessionLogHandle) {
    let storage = ensure_storage();
    let mut logs = storage.lock().unwrap();
    logs.remove(&handle.id);
}

pub fn seal_message(
    message: Vec<u8>,
    sender_secret: String,
    recipient_id: String,
    nonce_material: Vec<u8>,
) -> ffi::U8VecResult {
    match seal_internal(&message, &sender_secret, &recipient_id, &nonce_material) {
        Ok(sealed_data) => u8vec_success_result(sealed_data),
        Err(e) => u8vec_error_result(format!("Failed to seal message: {}", e)),
    }
}

pub fn unseal_message(
    sealed_message: Vec<u8>,
    recipient_secret: String,
    sender_id: String,
    nonce_material: Vec<u8>,
) -> ffi::U8VecResult {
    match unseal_internal(&sealed_message, &recipient_secret, &sender_id, &nonce_material) {
        Ok(unsealed_data) => u8vec_success_result((*unsealed_data).to_vec()),
        Err(e) => u8vec_error_result(format!("Failed to unseal message: {}", e)),
    }
}
