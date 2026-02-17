//! Native storage bindings for WASM (browsers and Cloudflare Workers).
//!
//! This module provides WASM bindings for the native storage backend,
//! allowing JavaScript code to use the high-performance Rust storage
//! in browser and worker environments.
//!
//! # Storage Modes
//!
//! - **In-Memory**: Default mode, suitable for Cloudflare Workers
//! - **OPFS**: For browsers with persistent storage (requires Web Worker for sync access)

use std::collections::HashMap;
use std::sync::Arc;

use cojson_storage::bftree::{BTreeConfig, BTreeStorage};
use cojson_storage::{
    CoValueHeader, SessionRow, SessionUpdate,
    StorageBackend, StoredSessionRow,
    SyncStateUpdate, Transaction, RulesetDef, Uniqueness,
};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

/// Native storage backend for WASM environments.
///
/// This provides a high-performance storage backend using BTreeMap-based
/// indexes. In browser environments with OPFS support, data can be persisted.
/// In Cloudflare Workers, data persists for the worker instance lifetime.
///
/// @example
/// ```javascript
/// // Create in-memory storage (Workers or browser)
/// const storage = NativeStorage.inMemory();
///
/// // Store a CoValue
/// const rowId = storage.upsertCoValue('co_zTest123', JSON.stringify({
///   type: 'comap',
///   ruleset: { type: 'unsafeAllowAll' },
///   uniqueness: null
/// }));
///
/// // Retrieve it
/// const stored = storage.getCoValue('co_zTest123');
/// ```
#[wasm_bindgen]
pub struct NativeStorage {
    inner: Arc<BTreeStorage>,
}

/// Error type for storage operations
#[wasm_bindgen]
pub struct StorageError {
    message: String,
}

#[wasm_bindgen]
impl StorageError {
    #[wasm_bindgen(getter)]
    pub fn message(&self) -> String {
        self.message.clone()
    }
}

impl From<cojson_storage::StorageError> for StorageError {
    fn from(err: cojson_storage::StorageError) -> Self {
        StorageError {
            message: err.to_string(),
        }
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(err: serde_json::Error) -> Self {
        StorageError {
            message: err.to_string(),
        }
    }
}

// Helper to convert CoValueHeader from JSON
fn parse_header(json: &str) -> Result<CoValueHeader, serde_json::Error> {
    #[derive(Deserialize)]
    struct JsHeader {
        #[serde(rename = "type")]
        covalue_type: String,
        ruleset: JsRuleset,
        meta: Option<serde_json::Map<String, serde_json::Value>>,
        uniqueness: Option<serde_json::Value>,
        #[serde(rename = "createdAt")]
        created_at: Option<String>,
    }

    #[derive(Deserialize)]
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
    #[derive(Serialize)]
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

    #[derive(Serialize)]
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

#[wasm_bindgen]
impl NativeStorage {
    /// Create an in-memory storage instance.
    ///
    /// This is the recommended mode for Cloudflare Workers where data
    /// persists for the worker instance lifetime. Jazz sync provides
    /// durability by replicating to servers with persistent storage.
    #[wasm_bindgen(js_name = inMemory)]
    pub fn in_memory() -> NativeStorage {
        let config = BTreeConfig::default();
        let storage = BTreeStorage::new(config);

        NativeStorage {
            inner: Arc::new(storage),
        }
    }

    /// Check if the current environment supports OPFS.
    ///
    /// Returns true if running in a browser with OPFS support.
    /// Note: OPFS Synchronous Access Handle is only available in Web Workers.
    #[wasm_bindgen(js_name = supportsOpfs)]
    pub fn supports_opfs() -> bool {
        // Check if navigator.storage.getDirectory exists
        js_sys::eval("typeof navigator !== 'undefined' && typeof navigator.storage !== 'undefined' && typeof navigator.storage.getDirectory === 'function'")
            .map(|v| v.as_bool().unwrap_or(false))
            .unwrap_or(false)
    }

    /// Check if running in a Web Worker context.
    ///
    /// OPFS Synchronous Access Handle is only available in Workers.
    #[wasm_bindgen(js_name = isInWorker)]
    pub fn is_in_worker() -> bool {
        js_sys::eval("typeof WorkerGlobalScope !== 'undefined' && self instanceof WorkerGlobalScope")
            .map(|v| v.as_bool().unwrap_or(false))
            .unwrap_or(false)
    }

    // =========================================================================
    // CoValue Operations
    // =========================================================================

    /// Get a CoValue by its ID.
    ///
    /// Returns the stored CoValue as JSON, or undefined if not found.
    #[wasm_bindgen(js_name = getCoValue)]
    pub fn get_covalue(&self, co_value_id: &str) -> Option<String> {
        self.inner.get_covalue(co_value_id).map(|row| {
            serde_json::json!({
                "rowId": row.row_id,
                "id": row.id,
                "header": serde_json::from_str::<serde_json::Value>(&header_to_json(&row.header)).unwrap_or_default()
            }).to_string()
        })
    }

    /// Insert or update a CoValue.
    ///
    /// @param id - The CoValue ID
    /// @param headerJson - The CoValue header as JSON (optional for updates)
    /// @returns The row ID of the CoValue, or -1 if insert failed
    #[wasm_bindgen(js_name = upsertCoValue)]
    pub fn upsert_covalue(&self, id: &str, header_json: Option<String>) -> i64 {
        let header = header_json.as_ref().and_then(|json| parse_header(json).ok());
        self.inner
            .upsert_covalue(id, header.as_ref())
            .map(|id| id as i64)
            .unwrap_or(-1)
    }

    /// Get all sessions for a CoValue.
    ///
    /// @param coValueRowId - The CoValue's row ID
    /// @returns Array of session rows as JSON string
    #[wasm_bindgen(js_name = getCoValueSessions)]
    pub fn get_covalue_sessions(&self, co_value_row_id: i64) -> String {
        let sessions: Vec<serde_json::Value> = self
            .inner
            .get_covalue_sessions(co_value_row_id as u64)
            .iter()
            .map(|s| {
                serde_json::json!({
                    "rowId": s.row_id,
                    "covalue": s.covalue,
                    "sessionId": s.session_id,
                    "lastIdx": s.last_idx,
                    "lastSignature": s.last_signature,
                    "bytesSinceLastSignature": s.bytes_since_last_signature
                })
            })
            .collect();
        serde_json::to_string(&sessions).unwrap_or_default()
    }

    /// Get transactions in a session within a range.
    ///
    /// @param sessionRowId - The session's row ID
    /// @param fromIdx - Start index (inclusive)
    /// @param toIdx - End index (exclusive)
    /// @returns Array of transaction rows as JSON string
    #[wasm_bindgen(js_name = getNewTransactionInSession)]
    pub fn get_new_transaction_in_session(
        &self,
        session_row_id: i64,
        from_idx: i64,
        to_idx: i64,
    ) -> String {
        let txs: Vec<serde_json::Value> = self
            .inner
            .get_new_transaction_in_session(session_row_id as u64, from_idx as u64, to_idx as u64)
            .iter()
            .map(|t| {
                serde_json::json!({
                    "ses": t.ses,
                    "idx": t.idx,
                    "tx": serde_json::to_value(&t.tx).unwrap_or_default()
                })
            })
            .collect();
        serde_json::to_string(&txs).unwrap_or_default()
    }

    /// Get signatures after a given transaction index.
    ///
    /// @param sessionRowId - The session's row ID
    /// @param firstNewTxIdx - First transaction index to get signatures for
    /// @returns Array of signature rows as JSON string
    #[wasm_bindgen(js_name = getSignatures)]
    pub fn get_signatures(&self, session_row_id: i64, first_new_tx_idx: i64) -> String {
        let sigs: Vec<serde_json::Value> = self
            .inner
            .get_signatures(session_row_id as u64, first_new_tx_idx as u64)
            .iter()
            .map(|s| {
                serde_json::json!({
                    "ses": s.ses,
                    "idx": s.idx,
                    "signature": s.signature
                })
            })
            .collect();
        serde_json::to_string(&sigs).unwrap_or_default()
    }

    /// Get the known state for a CoValue.
    ///
    /// @param coValueId - The CoValue ID
    /// @returns The known state as JSON, or undefined if not found
    #[wasm_bindgen(js_name = getCoValueKnownState)]
    pub fn get_covalue_known_state(&self, co_value_id: &str) -> Option<String> {
        self.inner.get_covalue_known_state(co_value_id).map(|ks| {
            serde_json::json!({
                "id": ks.id,
                "header": ks.header,
                "sessions": ks.sessions
            }).to_string()
        })
    }

    // =========================================================================
    // Sync State
    // =========================================================================

    /// Track sync state for multiple CoValues.
    ///
    /// @param updatesJson - Array of sync state updates as JSON string
    #[wasm_bindgen(js_name = trackCoValuesSyncState)]
    pub fn track_covalues_sync_state(&self, updates_json: &str) {
        #[derive(Deserialize)]
        struct JsUpdate {
            id: String,
            #[serde(rename = "peerId")]
            peer_id: String,
            synced: bool,
        }

        if let Ok(updates) = serde_json::from_str::<Vec<JsUpdate>>(updates_json) {
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
    }

    /// Get all CoValue IDs that have unsynced peers.
    ///
    /// @returns Array of CoValue IDs as JSON string
    #[wasm_bindgen(js_name = getUnsyncedCoValueIds)]
    pub fn get_unsynced_covalue_ids(&self) -> String {
        serde_json::to_string(&self.inner.get_unsynced_covalue_ids()).unwrap_or_default()
    }

    /// Stop tracking sync state for a CoValue.
    ///
    /// @param id - The CoValue ID
    #[wasm_bindgen(js_name = stopTrackingSyncState)]
    pub fn stop_tracking_sync_state(&self, id: &str) {
        self.inner.stop_tracking_sync_state(id);
    }

    // =========================================================================
    // Deletion
    // =========================================================================

    /// Get all CoValue IDs waiting for deletion.
    ///
    /// @returns Array of CoValue IDs as JSON string
    #[wasm_bindgen(js_name = getAllCoValuesWaitingForDelete)]
    pub fn get_all_covalues_waiting_for_delete(&self) -> String {
        serde_json::to_string(&self.inner.get_all_covalues_waiting_for_delete()).unwrap_or_default()
    }

    /// Erase a CoValue but keep its tombstone.
    ///
    /// @param coValueId - The CoValue ID to erase
    #[wasm_bindgen(js_name = eraseCoValueButKeepTombstone)]
    pub fn erase_covalue_but_keep_tombstone(&self, co_value_id: &str) -> Result<(), JsValue> {
        self.inner
            .erase_covalue_but_keep_tombstone(co_value_id)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    // =========================================================================
    // Transaction Operations
    // =========================================================================

    /// Add or update a session.
    ///
    /// @returns The row ID of the session, or -1 on error
    #[wasm_bindgen(js_name = addSession)]
    pub fn add_session(
        &self,
        covalue_row_id: i64,
        session_id: &str,
        last_idx: i64,
        last_signature: &str,
        bytes_since_last_signature: Option<i64>,
        existing_row_id: Option<i64>,
    ) -> i64 {
        self.inner
            .transaction(|tx| {
                let session_row = existing_row_id.map(|id| StoredSessionRow {
                    row_id: id as u64,
                    covalue: covalue_row_id as u64,
                    session_id: session_id.to_string(),
                    last_idx: last_idx as u64,
                    last_signature: last_signature.to_string(),
                    bytes_since_last_signature: bytes_since_last_signature.map(|b| b as u64),
                });

                let update = SessionUpdate {
                    session_update: SessionRow {
                        covalue: covalue_row_id as u64,
                        session_id: session_id.to_string(),
                        last_idx: last_idx as u64,
                        last_signature: last_signature.to_string(),
                        bytes_since_last_signature: bytes_since_last_signature.map(|b| b as u64),
                    },
                    session_row,
                };

                tx.add_session_update(&update)
            })
            .map(|id| id as i64)
            .unwrap_or(-1)
    }

    /// Add a transaction to a session.
    ///
    /// @param sessionRowId - The session's row ID
    /// @param idx - Transaction index
    /// @param txJson - Transaction data as JSON string
    /// @returns 1 on success, -1 on error
    #[wasm_bindgen(js_name = addTransaction)]
    pub fn add_transaction(&self, session_row_id: i64, idx: i64, tx_json: &str) -> i64 {
        let tx: Result<Transaction, _> = serde_json::from_str(tx_json);
        match tx {
            Ok(tx) => self
                .inner
                .transaction(|t| t.add_transaction(session_row_id as u64, idx as u64, &tx))
                .map(|n| n as i64)
                .unwrap_or(-1),
            Err(_) => -1,
        }
    }

    /// Add a signature checkpoint.
    ///
    /// @param sessionRowId - The session's row ID
    /// @param idx - Transaction index the signature covers
    /// @param signature - The signature
    #[wasm_bindgen(js_name = addSignatureAfter)]
    pub fn add_signature_after(
        &self,
        session_row_id: i64,
        idx: i64,
        signature: &str,
    ) -> Result<(), JsValue> {
        self.inner
            .transaction(|tx| tx.add_signature_after(session_row_id as u64, idx as u64, &signature.to_string()))
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Mark a CoValue as deleted.
    ///
    /// @param id - The CoValue ID to mark as deleted
    #[wasm_bindgen(js_name = markCoValueAsDeleted)]
    pub fn mark_covalue_as_deleted(&self, id: &str) -> Result<(), JsValue> {
        self.inner
            .transaction(|tx| tx.mark_covalue_as_deleted(id))
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get a single session for a CoValue.
    ///
    /// @param coValueRowId - The CoValue's row ID
    /// @param sessionId - The session ID
    /// @returns The session row as JSON, or undefined if not found
    #[wasm_bindgen(js_name = getSingleCoValueSession)]
    pub fn get_single_covalue_session(
        &self,
        co_value_row_id: i64,
        session_id: &str,
    ) -> Option<String> {
        self.inner
            .transaction(|tx| Ok(tx.get_single_covalue_session(co_value_row_id as u64, session_id)))
            .ok()
            .flatten()
            .map(|s| {
                serde_json::json!({
                    "rowId": s.row_id,
                    "covalue": s.covalue,
                    "sessionId": s.session_id,
                    "lastIdx": s.last_idx,
                    "lastSignature": s.last_signature,
                    "bytesSinceLastSignature": s.bytes_since_last_signature
                }).to_string()
            })
    }

    // =========================================================================
    // Statistics
    // =========================================================================

    /// Get storage statistics.
    ///
    /// @returns JSON object with coValueCount, sessionCount, transactionCount
    #[wasm_bindgen(js_name = getStats)]
    pub fn get_stats(&self) -> String {
        let stats = self.inner.stats();
        serde_json::json!({
            "coValueCount": stats.covalue_count,
            "sessionCount": stats.session_count,
            "transactionCount": stats.transaction_count,
            "totalSizeBytes": stats.total_size_bytes
        }).to_string()
    }

    /// Clear all storage data.
    #[wasm_bindgen]
    pub fn clear(&self) {
        self.inner.clear();
    }
}
