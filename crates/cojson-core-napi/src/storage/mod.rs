//! Native storage bindings for Node.js.
//!
//! This module provides NAPI bindings for the native storage backend,
//! allowing JavaScript code to use the high-performance Rust storage
//! directly.

use std::collections::HashMap;
use std::sync::Arc;

use cojson_storage::bftree::{BTreeConfig, BTreeStorage};
use cojson_storage::{
    CoValueHeader, CoValueKnownState, SessionRow, SessionUpdate,
    SignatureAfterRow, StorageBackend, StoredCoValueRow, StoredSessionRow,
    SyncStateUpdate, Transaction, TransactionRow, RulesetDef, Uniqueness,
};
use napi_derive::napi;

/// Native storage backend for Node.js.
///
/// This provides a high-performance storage backend using BTreeMap-based
/// indexes with optional file persistence.
///
/// @example
/// ```javascript
/// const storage = new NativeStorage('./data/jazz-storage');
///
/// // Store a CoValue
/// const rowId = storage.upsertCoValue('co_zTest123', {
///   type: 'comap',
///   ruleset: { type: 'unsafeAllowAll' },
///   uniqueness: null
/// });
///
/// // Retrieve it
/// const stored = storage.getCoValue('co_zTest123');
/// ```
#[napi]
pub struct NativeStorage {
    inner: Arc<BTreeStorage>,
    path: Option<String>,
}

/// CoValue header as a JavaScript object.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct JsCoValueHeader {
    /// The type of the CoValue (e.g., "comap", "colist", "costream")
    #[napi(js_name = "type")]
    pub covalue_type: String,
    /// The ruleset defining permissions
    pub ruleset: JsRuleset,
    /// Optional metadata (as JSON string)
    pub meta: Option<String>,
    /// Uniqueness value for deduplication (as JSON string)
    pub uniqueness: Option<String>,
    /// Creation timestamp (ISO 8601 format)
    pub created_at: Option<String>,
}

/// Ruleset definition as a JavaScript object.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct JsRuleset {
    /// The ruleset type: "unsafeAllowAll", "ownedByGroup", "group", or "account"
    #[napi(js_name = "type")]
    pub ruleset_type: String,
    /// Group ID (only for "ownedByGroup" type)
    pub group: Option<String>,
}

/// Stored CoValue row as a JavaScript object.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct JsStoredCoValueRow {
    /// Database row ID
    pub row_id: i64,
    /// The CoValue ID
    pub id: String,
    /// The CoValue header
    pub header: JsCoValueHeader,
}

/// Session row as a JavaScript object.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct JsStoredSessionRow {
    /// Database row ID
    pub row_id: i64,
    /// Foreign key to CoValues table
    pub covalue: i64,
    /// The session ID
    pub session_id: String,
    /// Index of the last transaction
    pub last_idx: i64,
    /// Signature of the last transaction
    pub last_signature: String,
    /// Bytes since the last signature checkpoint
    pub bytes_since_last_signature: Option<i64>,
}

/// Transaction row as a JavaScript object.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct JsTransactionRow {
    /// Foreign key to Sessions table
    pub ses: i64,
    /// Transaction index within the session
    pub idx: i64,
    /// The transaction data as JSON string
    pub tx: String,
}

/// Signature row as a JavaScript object.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct JsSignatureAfterRow {
    /// Foreign key to Sessions table
    pub ses: i64,
    /// Transaction index this signature covers
    pub idx: i64,
    /// The signature
    pub signature: String,
}

/// Known state as a JavaScript object.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct JsCoValueKnownState {
    /// The CoValue ID
    pub id: String,
    /// Whether the header is known
    pub header: bool,
    /// Session states (session ID -> transaction count)
    pub sessions: HashMap<String, i64>,
}

/// Sync state update as a JavaScript object.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct JsSyncStateUpdate {
    /// The CoValue ID
    pub id: String,
    /// The peer ID
    pub peer_id: String,
    /// Whether the peer has synced
    pub synced: bool,
}

// Conversion helpers

fn js_header_to_rust(header: &JsCoValueHeader) -> CoValueHeader {
    let ruleset = match header.ruleset.ruleset_type.as_str() {
        "unsafeAllowAll" => RulesetDef::UnsafeAllowAll,
        "ownedByGroup" => RulesetDef::OwnedByGroup {
            group: header.ruleset.group.clone().unwrap_or_default(),
        },
        "group" => RulesetDef::Group,
        "account" => RulesetDef::Account,
        _ => RulesetDef::UnsafeAllowAll,
    };

    let uniqueness = header
        .uniqueness
        .as_ref()
        .map(|s| {
            if s == "null" || s.is_empty() {
                Uniqueness::Null
            } else if s == "true" {
                Uniqueness::Bool(true)
            } else if s == "false" {
                Uniqueness::Bool(false)
            } else if s.starts_with('{') {
                // Try to parse as object
                serde_json::from_str(s).unwrap_or(Uniqueness::String(s.clone()))
            } else {
                Uniqueness::String(s.clone())
            }
        })
        .unwrap_or(Uniqueness::Null);

    CoValueHeader {
        covalue_type: header.covalue_type.clone(),
        ruleset,
        meta: header.meta.as_ref().and_then(|s| serde_json::from_str(s).ok()),
        uniqueness,
        created_at: header.created_at.clone(),
    }
}

fn rust_header_to_js(header: &CoValueHeader) -> JsCoValueHeader {
    let (ruleset_type, group) = match &header.ruleset {
        RulesetDef::UnsafeAllowAll => ("unsafeAllowAll".to_string(), None),
        RulesetDef::OwnedByGroup { group } => ("ownedByGroup".to_string(), Some(group.clone())),
        RulesetDef::Group => ("group".to_string(), None),
        RulesetDef::Account => ("account".to_string(), None),
    };

    let uniqueness = match &header.uniqueness {
        Uniqueness::Null => None,
        Uniqueness::Bool(b) => Some(b.to_string()),
        Uniqueness::String(s) => Some(s.clone()),
        Uniqueness::Object(o) => serde_json::to_string(o).ok(),
    };

    JsCoValueHeader {
        covalue_type: header.covalue_type.clone(),
        ruleset: JsRuleset {
            ruleset_type,
            group,
        },
        meta: header.meta.as_ref().and_then(|m| serde_json::to_string(m).ok()),
        uniqueness,
        created_at: header.created_at.clone(),
    }
}

fn rust_stored_row_to_js(row: &StoredCoValueRow) -> JsStoredCoValueRow {
    JsStoredCoValueRow {
        row_id: row.row_id as i64,
        id: row.id.clone(),
        header: rust_header_to_js(&row.header),
    }
}

fn rust_session_to_js(row: &StoredSessionRow) -> JsStoredSessionRow {
    JsStoredSessionRow {
        row_id: row.row_id as i64,
        covalue: row.covalue as i64,
        session_id: row.session_id.clone(),
        last_idx: row.last_idx as i64,
        last_signature: row.last_signature.clone(),
        bytes_since_last_signature: row.bytes_since_last_signature.map(|b| b as i64),
    }
}

fn rust_tx_to_js(row: &TransactionRow) -> JsTransactionRow {
    let tx_json = serde_json::to_string(&row.tx).unwrap_or_default();
    JsTransactionRow {
        ses: row.ses as i64,
        idx: row.idx as i64,
        tx: tx_json,
    }
}

fn rust_sig_to_js(row: &SignatureAfterRow) -> JsSignatureAfterRow {
    JsSignatureAfterRow {
        ses: row.ses as i64,
        idx: row.idx as i64,
        signature: row.signature.clone(),
    }
}

fn rust_known_state_to_js(ks: &CoValueKnownState) -> JsCoValueKnownState {
    JsCoValueKnownState {
        id: ks.id.clone(),
        header: ks.header,
        sessions: ks.sessions.iter().map(|(k, v)| (k.clone(), *v as i64)).collect(),
    }
}

#[napi]
impl NativeStorage {
    /// Create a new native storage instance.
    ///
    /// @param path - Optional path for file-based persistence (Node.js only)
    #[napi(constructor)]
    pub fn new(path: Option<String>) -> napi::Result<Self> {
        let config = BTreeConfig::default();
        let storage = BTreeStorage::new(config);

        Ok(Self {
            inner: Arc::new(storage),
            path,
        })
    }

    /// Create a storage instance with custom configuration.
    ///
    /// @param path - Optional path for file-based persistence
    /// @param maxMemoryBytes - Maximum memory usage before flushing (default: 64MB)
    #[napi(factory)]
    pub fn with_config(
        path: Option<String>,
        max_memory_bytes: Option<i64>,
    ) -> napi::Result<Self> {
        let config = BTreeConfig {
            persist: path.is_some(),
            max_memory_bytes: max_memory_bytes.unwrap_or(64 * 1024 * 1024) as usize,
            ..Default::default()
        };
        let storage = BTreeStorage::new(config);

        Ok(Self {
            inner: Arc::new(storage),
            path,
        })
    }

    /// Get the storage path (if configured).
    #[napi(getter)]
    pub fn path(&self) -> Option<String> {
        self.path.clone()
    }

    // =========================================================================
    // CoValue Operations
    // =========================================================================

    /// Get a CoValue by its ID.
    ///
    /// @param coValueId - The CoValue ID to look up
    /// @returns The stored CoValue row, or undefined if not found
    #[napi]
    pub fn get_co_value(&self, co_value_id: String) -> Option<JsStoredCoValueRow> {
        self.inner.get_covalue(&co_value_id).map(|r| rust_stored_row_to_js(&r))
    }

    /// Insert or update a CoValue.
    ///
    /// @param id - The CoValue ID
    /// @param header - The CoValue header (optional for updates)
    /// @returns The row ID of the CoValue, or undefined if insert failed
    #[napi]
    pub fn upsert_co_value(&self, id: String, header: Option<JsCoValueHeader>) -> Option<i64> {
        let rust_header = header.as_ref().map(js_header_to_rust);
        self.inner.upsert_covalue(&id, rust_header.as_ref()).map(|id| id as i64)
    }

    /// Get all sessions for a CoValue.
    ///
    /// @param coValueRowId - The CoValue's row ID
    /// @returns Array of session rows
    #[napi]
    pub fn get_co_value_sessions(&self, co_value_row_id: i64) -> Vec<JsStoredSessionRow> {
        self.inner
            .get_covalue_sessions(co_value_row_id as u64)
            .iter()
            .map(rust_session_to_js)
            .collect()
    }

    /// Get transactions in a session within a range.
    ///
    /// @param sessionRowId - The session's row ID
    /// @param fromIdx - Start index (inclusive)
    /// @param toIdx - End index (exclusive)
    /// @returns Array of transaction rows
    #[napi]
    pub fn get_new_transaction_in_session(
        &self,
        session_row_id: i64,
        from_idx: i64,
        to_idx: i64,
    ) -> Vec<JsTransactionRow> {
        self.inner
            .get_new_transaction_in_session(session_row_id as u64, from_idx as u64, to_idx as u64)
            .iter()
            .map(rust_tx_to_js)
            .collect()
    }

    /// Get signatures after a given transaction index.
    ///
    /// @param sessionRowId - The session's row ID
    /// @param firstNewTxIdx - First transaction index to get signatures for
    /// @returns Array of signature rows
    #[napi]
    pub fn get_signatures(&self, session_row_id: i64, first_new_tx_idx: i64) -> Vec<JsSignatureAfterRow> {
        self.inner
            .get_signatures(session_row_id as u64, first_new_tx_idx as u64)
            .iter()
            .map(rust_sig_to_js)
            .collect()
    }

    /// Get the known state for a CoValue.
    ///
    /// @param coValueId - The CoValue ID
    /// @returns The known state, or undefined if not found
    #[napi]
    pub fn get_co_value_known_state(&self, co_value_id: String) -> Option<JsCoValueKnownState> {
        self.inner.get_covalue_known_state(&co_value_id).map(|ks| rust_known_state_to_js(&ks))
    }

    // =========================================================================
    // Sync State
    // =========================================================================

    /// Track sync state for multiple CoValues.
    ///
    /// @param updates - Array of sync state updates
    #[napi]
    pub fn track_co_values_sync_state(&self, updates: Vec<JsSyncStateUpdate>) {
        let rust_updates: Vec<SyncStateUpdate> = updates
            .iter()
            .map(|u| SyncStateUpdate {
                id: u.id.clone(),
                peer_id: u.peer_id.clone(),
                synced: u.synced,
            })
            .collect();
        self.inner.track_covalues_sync_state(&rust_updates);
    }

    /// Get all CoValue IDs that have unsynced peers.
    ///
    /// @returns Array of CoValue IDs
    #[napi]
    pub fn get_unsynced_co_value_ids(&self) -> Vec<String> {
        self.inner.get_unsynced_covalue_ids()
    }

    /// Stop tracking sync state for a CoValue.
    ///
    /// @param id - The CoValue ID
    #[napi]
    pub fn stop_tracking_sync_state(&self, id: String) {
        self.inner.stop_tracking_sync_state(&id);
    }

    // =========================================================================
    // Deletion
    // =========================================================================

    /// Get all CoValue IDs waiting for deletion.
    ///
    /// @returns Array of CoValue IDs with pending deletion
    #[napi]
    pub fn get_all_co_values_waiting_for_delete(&self) -> Vec<String> {
        self.inner.get_all_covalues_waiting_for_delete()
    }

    /// Erase a CoValue but keep its tombstone.
    ///
    /// @param coValueId - The CoValue ID to erase
    #[napi]
    pub fn erase_co_value_but_keep_tombstone(&self, co_value_id: String) -> napi::Result<()> {
        self.inner
            .erase_covalue_but_keep_tombstone(&co_value_id)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
    }

    // =========================================================================
    // Transaction Operations
    // =========================================================================

    /// Execute operations within a transaction.
    ///
    /// Note: For Node.js, this is a simplified synchronous transaction.
    /// For true async operations, use the async variants.
    #[napi]
    pub fn add_session(
        &self,
        covalue_row_id: i64,
        session_id: String,
        last_idx: i64,
        last_signature: String,
        bytes_since_last_signature: Option<i64>,
        existing_row_id: Option<i64>,
    ) -> napi::Result<i64> {
        self.inner
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
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
    }

    /// Add a transaction to a session.
    ///
    /// @param sessionRowId - The session's row ID
    /// @param idx - Transaction index
    /// @param txJson - Transaction data as JSON string
    #[napi]
    pub fn add_transaction(
        &self,
        session_row_id: i64,
        idx: i64,
        tx_json: String,
    ) -> napi::Result<i64> {
        let tx: Transaction = serde_json::from_str(&tx_json)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?;

        self.inner
            .transaction(|t| t.add_transaction(session_row_id as u64, idx as u64, &tx))
            .map(|n| n as i64)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
    }

    /// Add a signature checkpoint.
    ///
    /// @param sessionRowId - The session's row ID
    /// @param idx - Transaction index the signature covers
    /// @param signature - The signature
    #[napi]
    pub fn add_signature_after(
        &self,
        session_row_id: i64,
        idx: i64,
        signature: String,
    ) -> napi::Result<()> {
        self.inner
            .transaction(|tx| tx.add_signature_after(session_row_id as u64, idx as u64, &signature))
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
    }

    /// Mark a CoValue as deleted.
    ///
    /// @param id - The CoValue ID to mark as deleted
    #[napi]
    pub fn mark_co_value_as_deleted(&self, id: String) -> napi::Result<()> {
        self.inner
            .transaction(|tx| tx.mark_covalue_as_deleted(&id))
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))
    }

    /// Get a single session for a CoValue.
    ///
    /// @param coValueRowId - The CoValue's row ID
    /// @param sessionId - The session ID
    /// @returns The session row, or undefined if not found
    #[napi]
    pub fn get_single_co_value_session(
        &self,
        co_value_row_id: i64,
        session_id: String,
    ) -> Option<JsStoredSessionRow> {
        self.inner
            .transaction(|tx| Ok(tx.get_single_covalue_session(co_value_row_id as u64, &session_id)))
            .ok()
            .flatten()
            .map(|r| rust_session_to_js(&r))
    }

    // =========================================================================
    // Statistics
    // =========================================================================

    /// Get storage statistics.
    ///
    /// @returns Object with coValueCount, sessionCount, transactionCount
    #[napi]
    pub fn get_stats(&self) -> napi::Result<serde_json::Value> {
        let stats = self.inner.stats();
        Ok(serde_json::json!({
            "coValueCount": stats.covalue_count,
            "sessionCount": stats.session_count,
            "transactionCount": stats.transaction_count,
            "totalSizeBytes": stats.total_size_bytes
        }))
    }

    /// Clear all storage data.
    #[napi]
    pub fn clear(&self) {
        self.inner.clear();
    }
}
