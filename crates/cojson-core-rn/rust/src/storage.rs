//! Native storage bindings for React Native (UniFFI).
//!
//! This module provides UniFFI bindings for the native storage backend,
//! allowing React Native apps to use high-performance Rust storage
//! with file-based persistence.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cojson_storage::bftree::{BTreeConfig, BTreeStorage};
use cojson_storage::{
    CoValueHeader, SessionRow, SessionUpdate,
    StorageBackend, StoredSessionRow,
    SyncStateUpdate, Transaction, RulesetDef, Uniqueness,
};
use thiserror::Error;

/// Error type for storage operations.
#[derive(Error, Debug, uniffi::Error)]
pub enum NativeStorageError {
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Failed to acquire lock")]
    LockError,
}

impl From<cojson_storage::StorageError> for NativeStorageError {
    fn from(err: cojson_storage::StorageError) -> Self {
        NativeStorageError::Storage(err.to_string())
    }
}

impl From<serde_json::Error> for NativeStorageError {
    fn from(err: serde_json::Error) -> Self {
        NativeStorageError::Serialization(err.to_string())
    }
}

/// Stored CoValue row.
#[derive(uniffi::Record, Clone, Debug)]
pub struct StoredCoValue {
    pub row_id: i64,
    pub id: String,
    pub header_json: String,
}

/// Stored session row.
#[derive(uniffi::Record, Clone, Debug)]
pub struct StoredSession {
    pub row_id: i64,
    pub covalue: i64,
    pub session_id: String,
    pub last_idx: i64,
    pub last_signature: String,
    pub bytes_since_last_signature: Option<i64>,
}

/// Transaction row.
#[derive(uniffi::Record, Clone, Debug)]
pub struct StoredTransaction {
    pub ses: i64,
    pub idx: i64,
    pub tx_json: String,
}

/// Signature row.
#[derive(uniffi::Record, Clone, Debug)]
pub struct StoredSignature {
    pub ses: i64,
    pub idx: i64,
    pub signature: String,
}

/// Known state for a CoValue.
#[derive(uniffi::Record, Clone, Debug)]
pub struct StorageKnownState {
    pub id: String,
    pub header: bool,
    pub sessions: HashMap<String, i64>,
}

/// Sync state update.
#[derive(uniffi::Record, Clone, Debug)]
pub struct SyncUpdate {
    pub id: String,
    pub peer_id: String,
    pub synced: bool,
}

/// Storage statistics.
#[derive(uniffi::Record, Clone, Debug)]
pub struct StorageStatistics {
    pub covalue_count: i64,
    pub session_count: i64,
    pub transaction_count: i64,
}

// Helper to convert CoValueHeader from JSON
fn parse_header(json: &str) -> Result<CoValueHeader, serde_json::Error> {
    #[derive(serde::Deserialize)]
    struct JsHeader {
        #[serde(rename = "type")]
        covalue_type: String,
        ruleset: JsRuleset,
        meta: Option<serde_json::Map<String, serde_json::Value>>,
        uniqueness: Option<serde_json::Value>,
        #[serde(rename = "createdAt")]
        created_at: Option<String>,
    }

    #[derive(serde::Deserialize)]
    struct JsRuleset {
        #[serde(rename = "type")]
        ruleset_type: String,
        group: Option<String>,
    }

    let js: JsHeader = serde_json::from_str(json)?;

    let ruleset = match js.ruleset.ruleset_type.as_str() {
        "unsafeAllowAll" => RulesetDef::UnsafeAllowAll,
        "ownedByGroup" => RulesetDef::OwnedByGroup {
            group: js.ruleset.group.unwrap_or_default(),
        },
        "group" => RulesetDef::Group,
        "account" => RulesetDef::Account,
        _ => RulesetDef::UnsafeAllowAll,
    };

    let uniqueness = js
        .uniqueness
        .map(|v| match v {
            serde_json::Value::Null => Uniqueness::Null,
            serde_json::Value::Bool(b) => Uniqueness::Bool(b),
            serde_json::Value::String(s) => Uniqueness::String(s),
            serde_json::Value::Object(o) => {
                let map: HashMap<String, String> = o
                    .into_iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k, s.to_string())))
                    .collect();
                Uniqueness::Object(map)
            }
            _ => Uniqueness::Null,
        })
        .unwrap_or(Uniqueness::Null);

    Ok(CoValueHeader {
        covalue_type: js.covalue_type,
        ruleset,
        meta: js.meta,
        uniqueness,
        created_at: js.created_at,
    })
}

// Helper to serialize header to JSON
fn header_to_json(header: &CoValueHeader) -> String {
    #[derive(serde::Serialize)]
    struct JsHeader<'a> {
        #[serde(rename = "type")]
        covalue_type: &'a str,
        ruleset: JsRuleset<'a>,
        #[serde(skip_serializing_if = "Option::is_none")]
        meta: &'a Option<serde_json::Map<String, serde_json::Value>>,
        uniqueness: serde_json::Value,
        #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
        created_at: &'a Option<String>,
    }

    #[derive(serde::Serialize)]
    struct JsRuleset<'a> {
        #[serde(rename = "type")]
        ruleset_type: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        group: Option<&'a str>,
    }

    let (ruleset_type, group) = match &header.ruleset {
        RulesetDef::UnsafeAllowAll => ("unsafeAllowAll", None),
        RulesetDef::OwnedByGroup { group } => ("ownedByGroup", Some(group.as_str())),
        RulesetDef::Group => ("group", None),
        RulesetDef::Account => ("account", None),
    };

    let uniqueness = match &header.uniqueness {
        Uniqueness::Null => serde_json::Value::Null,
        Uniqueness::Bool(b) => serde_json::Value::Bool(*b),
        Uniqueness::String(s) => serde_json::Value::String(s.clone()),
        Uniqueness::Object(o) => serde_json::to_value(o).unwrap_or(serde_json::Value::Null),
    };

    let js = JsHeader {
        covalue_type: &header.covalue_type,
        ruleset: JsRuleset {
            ruleset_type,
            group,
        },
        meta: &header.meta,
        uniqueness,
        created_at: &header.created_at,
    };

    serde_json::to_string(&js).unwrap_or_default()
}

/// Native storage backend for React Native.
///
/// This provides a high-performance storage backend using BTreeMap-based
/// indexes. On React Native, data is stored in the app's documents directory.
#[derive(uniffi::Object)]
pub struct NativeStorage {
    inner: Mutex<Arc<BTreeStorage>>,
    path: Option<String>,
}

#[uniffi::export]
impl NativeStorage {
    /// Create an in-memory storage instance.
    #[uniffi::constructor]
    pub fn in_memory() -> Self {
        let config = BTreeConfig::default();
        let storage = BTreeStorage::new(config);

        NativeStorage {
            inner: Mutex::new(Arc::new(storage)),
            path: None,
        }
    }

    /// Create a storage instance with a file path.
    ///
    /// The path should be in the app's documents directory.
    #[uniffi::constructor]
    pub fn with_path(path: String) -> Self {
        let config = BTreeConfig {
            persist: true,
            ..Default::default()
        };
        let storage = BTreeStorage::new(config);

        NativeStorage {
            inner: Mutex::new(Arc::new(storage)),
            path: Some(path),
        }
    }

    /// Get the storage path.
    pub fn get_path(&self) -> Option<String> {
        self.path.clone()
    }

    // =========================================================================
    // CoValue Operations
    // =========================================================================

    /// Get a CoValue by its ID.
    pub fn get_covalue(&self, co_value_id: String) -> Result<Option<StoredCoValue>, NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        
        Ok(inner.get_covalue(&co_value_id).map(|row| StoredCoValue {
            row_id: row.row_id as i64,
            id: row.id,
            header_json: header_to_json(&row.header),
        }))
    }

    /// Insert or update a CoValue.
    pub fn upsert_covalue(
        &self,
        id: String,
        header_json: Option<String>,
    ) -> Result<Option<i64>, NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        
        let header = header_json
            .as_ref()
            .map(|json| parse_header(json))
            .transpose()?;
        
        Ok(inner.upsert_covalue(&id, header.as_ref()).map(|id| id as i64))
    }

    /// Get all sessions for a CoValue.
    pub fn get_covalue_sessions(
        &self,
        co_value_row_id: i64,
    ) -> Result<Vec<StoredSession>, NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        
        Ok(inner
            .get_covalue_sessions(co_value_row_id as u64)
            .iter()
            .map(|s| StoredSession {
                row_id: s.row_id as i64,
                covalue: s.covalue as i64,
                session_id: s.session_id.clone(),
                last_idx: s.last_idx as i64,
                last_signature: s.last_signature.clone(),
                bytes_since_last_signature: s.bytes_since_last_signature.map(|b| b as i64),
            })
            .collect())
    }

    /// Get transactions in a session within a range.
    pub fn get_new_transaction_in_session(
        &self,
        session_row_id: i64,
        from_idx: i64,
        to_idx: i64,
    ) -> Result<Vec<StoredTransaction>, NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        
        Ok(inner
            .get_new_transaction_in_session(session_row_id as u64, from_idx as u64, to_idx as u64)
            .iter()
            .map(|t| StoredTransaction {
                ses: t.ses as i64,
                idx: t.idx as i64,
                tx_json: serde_json::to_string(&t.tx).unwrap_or_default(),
            })
            .collect())
    }

    /// Get signatures after a given transaction index.
    pub fn get_signatures(
        &self,
        session_row_id: i64,
        first_new_tx_idx: i64,
    ) -> Result<Vec<StoredSignature>, NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        
        Ok(inner
            .get_signatures(session_row_id as u64, first_new_tx_idx as u64)
            .iter()
            .map(|s| StoredSignature {
                ses: s.ses as i64,
                idx: s.idx as i64,
                signature: s.signature.clone(),
            })
            .collect())
    }

    /// Get the known state for a CoValue.
    pub fn get_covalue_known_state(
        &self,
        co_value_id: String,
    ) -> Result<Option<StorageKnownState>, NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        
        Ok(inner.get_covalue_known_state(&co_value_id).map(|ks| StorageKnownState {
            id: ks.id,
            header: ks.header,
            sessions: ks.sessions.iter().map(|(k, v)| (k.clone(), *v as i64)).collect(),
        }))
    }

    // =========================================================================
    // Sync State
    // =========================================================================

    /// Track sync state for multiple CoValues.
    pub fn track_covalues_sync_state(
        &self,
        updates: Vec<SyncUpdate>,
    ) -> Result<(), NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        
        let rust_updates: Vec<SyncStateUpdate> = updates
            .iter()
            .map(|u| SyncStateUpdate {
                id: u.id.clone(),
                peer_id: u.peer_id.clone(),
                synced: u.synced,
            })
            .collect();
        
        inner.track_covalues_sync_state(&rust_updates);
        Ok(())
    }

    /// Get all CoValue IDs that have unsynced peers.
    pub fn get_unsynced_covalue_ids(&self) -> Result<Vec<String>, NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        Ok(inner.get_unsynced_covalue_ids())
    }

    /// Stop tracking sync state for a CoValue.
    pub fn stop_tracking_sync_state(&self, id: String) -> Result<(), NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        inner.stop_tracking_sync_state(&id);
        Ok(())
    }

    // =========================================================================
    // Deletion
    // =========================================================================

    /// Get all CoValue IDs waiting for deletion.
    pub fn get_all_covalues_waiting_for_delete(&self) -> Result<Vec<String>, NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        Ok(inner.get_all_covalues_waiting_for_delete())
    }

    /// Erase a CoValue but keep its tombstone.
    pub fn erase_covalue_but_keep_tombstone(
        &self,
        co_value_id: String,
    ) -> Result<(), NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        inner.erase_covalue_but_keep_tombstone(&co_value_id)?;
        Ok(())
    }

    // =========================================================================
    // Transaction Operations
    // =========================================================================

    /// Add or update a session.
    pub fn add_session(
        &self,
        covalue_row_id: i64,
        session_id: String,
        last_idx: i64,
        last_signature: String,
        bytes_since_last_signature: Option<i64>,
        existing_row_id: Option<i64>,
    ) -> Result<i64, NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        
        inner
            .transaction(|tx| {
                let session_row = existing_row_id.map(|id| StoredSessionRow {
                    row_id: id as u64,
                    covalue: covalue_row_id as u64,
                    session_id: session_id.clone(),
                    last_idx: last_idx as u64,
                    last_signature: last_signature.clone(),
                    bytes_since_last_signature: bytes_since_last_signature.map(|b| b as u64),
                });

                let update = SessionUpdate {
                    session_update: SessionRow {
                        covalue: covalue_row_id as u64,
                        session_id,
                        last_idx: last_idx as u64,
                        last_signature,
                        bytes_since_last_signature: bytes_since_last_signature.map(|b| b as u64),
                    },
                    session_row,
                };

                tx.add_session_update(&update)
            })
            .map(|id| id as i64)
            .map_err(|e| NativeStorageError::Storage(e.to_string()))
    }

    /// Add a transaction to a session.
    pub fn add_transaction(
        &self,
        session_row_id: i64,
        idx: i64,
        tx_json: String,
    ) -> Result<i64, NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        
        let tx: Transaction = serde_json::from_str(&tx_json)?;
        
        inner
            .transaction(|t| t.add_transaction(session_row_id as u64, idx as u64, &tx))
            .map(|n| n as i64)
            .map_err(|e| NativeStorageError::Storage(e.to_string()))
    }

    /// Add a signature checkpoint.
    pub fn add_signature_after(
        &self,
        session_row_id: i64,
        idx: i64,
        signature: String,
    ) -> Result<(), NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        
        inner
            .transaction(|tx| tx.add_signature_after(session_row_id as u64, idx as u64, &signature))
            .map_err(|e| NativeStorageError::Storage(e.to_string()))
    }

    /// Mark a CoValue as deleted.
    pub fn mark_covalue_as_deleted(&self, id: String) -> Result<(), NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        
        inner
            .transaction(|tx| tx.mark_covalue_as_deleted(&id))
            .map_err(|e| NativeStorageError::Storage(e.to_string()))
    }

    /// Get a single session for a CoValue.
    pub fn get_single_covalue_session(
        &self,
        co_value_row_id: i64,
        session_id: String,
    ) -> Result<Option<StoredSession>, NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        
        let result = inner
            .transaction(|tx| Ok(tx.get_single_covalue_session(co_value_row_id as u64, &session_id)))
            .map_err(|e| NativeStorageError::Storage(e.to_string()))?;
        
        Ok(result.map(|s| StoredSession {
            row_id: s.row_id as i64,
            covalue: s.covalue as i64,
            session_id: s.session_id,
            last_idx: s.last_idx as i64,
            last_signature: s.last_signature,
            bytes_since_last_signature: s.bytes_since_last_signature.map(|b| b as i64),
        }))
    }

    // =========================================================================
    // Statistics
    // =========================================================================

    /// Get storage statistics.
    pub fn get_stats(&self) -> Result<StorageStatistics, NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        let stats = inner.stats();
        
        Ok(StorageStatistics {
            covalue_count: stats.covalue_count as i64,
            session_count: stats.session_count as i64,
            transaction_count: stats.transaction_count as i64,
        })
    }

    /// Clear all storage data.
    pub fn clear(&self) -> Result<(), NativeStorageError> {
        let inner = self.inner.lock().map_err(|_| NativeStorageError::LockError)?;
        inner.clear();
        Ok(())
    }
}
