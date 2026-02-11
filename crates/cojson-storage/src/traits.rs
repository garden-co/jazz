//! Core storage traits for synchronous operations.
//!
//! These traits correspond to the TypeScript `DBClientInterfaceSync` and
//! `DBTransactionInterfaceSync` interfaces in `packages/cojson/src/storage/types.ts`.

use crate::error::StorageResult;
use crate::types::*;

/// A synchronous storage backend for CoValues.
///
/// This trait mirrors the TypeScript `DBClientInterfaceSync` interface.
/// Implementations must be thread-safe (Send + Sync).
///
/// # Example
///
/// ```rust,ignore
/// use cojson_storage::{StorageBackend, StoredCoValueRow, CoValueHeader};
///
/// struct MyStorage { /* ... */ }
///
/// impl StorageBackend for MyStorage {
///     fn get_covalue(&self, id: &str) -> Option<StoredCoValueRow> {
///         // Look up CoValue by ID
///         None
///     }
///     // ... implement other methods
/// }
/// ```
pub trait StorageBackend: Send + Sync {
    /// Get a CoValue by its ID.
    ///
    /// Returns `Some(StoredCoValueRow)` if found, `None` otherwise.
    fn get_covalue(&self, covalue_id: &str) -> Option<StoredCoValueRow>;

    /// Insert or update a CoValue.
    ///
    /// If the CoValue doesn't exist, inserts it with the given header.
    /// If it exists and header is `None`, returns the existing row ID.
    /// If it exists and header is `Some`, updates the header.
    ///
    /// Returns the row ID of the CoValue.
    fn upsert_covalue(&self, id: &str, header: Option<&CoValueHeader>) -> Option<u64>;

    /// Get all sessions for a CoValue.
    ///
    /// Returns all sessions associated with the given CoValue row ID.
    fn get_covalue_sessions(&self, covalue_row_id: u64) -> Vec<StoredSessionRow>;

    /// Get transactions in a session within a range.
    ///
    /// Returns transactions with indices in `[from_idx, to_idx)`.
    fn get_new_transaction_in_session(
        &self,
        session_row_id: u64,
        from_idx: u64,
        to_idx: u64,
    ) -> Vec<TransactionRow>;

    /// Get signatures after a given transaction index.
    ///
    /// Returns all signature checkpoints with index >= `first_new_tx_idx`.
    fn get_signatures(&self, session_row_id: u64, first_new_tx_idx: u64) -> Vec<SignatureAfterRow>;

    /// Execute a transaction with a callback.
    ///
    /// The callback receives a `StorageTransaction` that can perform
    /// multiple operations atomically. If the callback returns an error,
    /// the transaction is rolled back.
    fn transaction<F, R>(&self, callback: F) -> StorageResult<R>
    where
        F: FnOnce(&dyn StorageTransaction) -> StorageResult<R>;

    /// Track sync state for multiple CoValues.
    ///
    /// Records which peers have synced which CoValues.
    fn track_covalues_sync_state(&self, updates: &[SyncStateUpdate]);

    /// Get all CoValue IDs that have at least one unsynced peer.
    fn get_unsynced_covalue_ids(&self) -> Vec<RawCoID>;

    /// Stop tracking sync state for a CoValue.
    ///
    /// Removes all peer entries for the given CoValue.
    fn stop_tracking_sync_state(&self, id: &str);

    /// Enumerate all CoValue IDs pending deletion.
    ///
    /// Returns IDs in the deletion work queue with status `Pending`.
    fn get_all_covalues_waiting_for_delete(&self) -> Vec<RawCoID>;

    /// Erase a CoValue's data but keep the tombstone.
    ///
    /// Deletes all transactions and non-delete sessions while preserving
    /// the header and delete session(s) as a tombstone.
    fn erase_covalue_but_keep_tombstone(&self, covalue_id: &str) -> StorageResult<()>;

    /// Get the known state for a CoValue without loading transactions.
    ///
    /// Returns `None` if the CoValue doesn't exist.
    fn get_covalue_known_state(&self, covalue_id: &str) -> Option<CoValueKnownState>;
}

/// A storage transaction for atomic operations.
///
/// This trait mirrors the TypeScript `DBTransactionInterfaceSync` interface.
/// Implementations should ensure operations are atomic within the transaction.
pub trait StorageTransaction: Send + Sync {
    /// Get a single session for a CoValue.
    ///
    /// Returns the session with the given session ID for the CoValue.
    fn get_single_covalue_session(
        &self,
        covalue_row_id: u64,
        session_id: &str,
    ) -> Option<StoredSessionRow>;

    /// Mark a CoValue as deleted.
    ///
    /// Adds the CoValue to the deletion work queue with status `Pending`.
    /// This is idempotent - calling multiple times has no additional effect.
    fn mark_covalue_as_deleted(&self, id: &str) -> StorageResult<()>;

    /// Add or update a session.
    ///
    /// If `session_row` is `None`, creates a new session.
    /// If `session_row` is `Some`, updates the existing session.
    ///
    /// Returns the row ID of the session.
    fn add_session_update(&self, update: &SessionUpdate) -> StorageResult<u64>;

    /// Add a transaction to a session.
    ///
    /// Returns the number of transactions added (typically 1).
    fn add_transaction(
        &self,
        session_row_id: u64,
        idx: u64,
        new_transaction: &Transaction,
    ) -> StorageResult<u64>;

    /// Add a signature checkpoint after a transaction.
    ///
    /// Records that a signature covers transactions up to and including `idx`.
    fn add_signature_after(
        &self,
        session_row_id: u64,
        idx: u64,
        signature: &Signature,
    ) -> StorageResult<()>;
}

/// Extension trait for additional storage operations.
///
/// These are optional operations that may not be supported by all backends.
pub trait StorageBackendExt: StorageBackend {
    /// Close the storage backend and release resources.
    ///
    /// After calling this, the storage should not be used.
    fn close(&self) -> StorageResult<()> {
        Ok(())
    }

    /// Check if the storage backend is healthy.
    ///
    /// Returns `true` if the backend is operational.
    fn is_healthy(&self) -> bool {
        true
    }

    /// Get storage statistics.
    ///
    /// Returns information about storage usage, performance, etc.
    fn get_stats(&self) -> StorageStats {
        StorageStats::default()
    }
}

/// Storage statistics.
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Number of CoValues stored
    pub covalue_count: u64,
    /// Number of sessions stored
    pub session_count: u64,
    /// Number of transactions stored
    pub transaction_count: u64,
    /// Total storage size in bytes (if available)
    pub total_size_bytes: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::RwLock;

    /// A simple in-memory storage implementation for testing.
    struct InMemoryStorage {
        covalues: RwLock<HashMap<String, (u64, CoValueHeader)>>,
        sessions: RwLock<HashMap<u64, Vec<StoredSessionRow>>>,
        transactions: RwLock<HashMap<u64, Vec<TransactionRow>>>,
        signatures: RwLock<HashMap<u64, Vec<SignatureAfterRow>>>,
        sync_state: RwLock<HashMap<(String, String), bool>>,
        deletions: RwLock<HashMap<String, DeletionStatus>>,
        next_covalue_id: RwLock<u64>,
        next_session_id: RwLock<u64>,
    }

    impl InMemoryStorage {
        fn new() -> Self {
            Self {
                covalues: RwLock::new(HashMap::new()),
                sessions: RwLock::new(HashMap::new()),
                transactions: RwLock::new(HashMap::new()),
                signatures: RwLock::new(HashMap::new()),
                sync_state: RwLock::new(HashMap::new()),
                deletions: RwLock::new(HashMap::new()),
                next_covalue_id: RwLock::new(1),
                next_session_id: RwLock::new(1),
            }
        }
    }

    impl StorageBackend for InMemoryStorage {
        fn get_covalue(&self, covalue_id: &str) -> Option<StoredCoValueRow> {
            let covalues = self.covalues.read().unwrap();
            covalues.get(covalue_id).map(|(row_id, header)| StoredCoValueRow {
                row_id: *row_id,
                id: covalue_id.to_string(),
                header: header.clone(),
            })
        }

        fn upsert_covalue(&self, id: &str, header: Option<&CoValueHeader>) -> Option<u64> {
            let mut covalues = self.covalues.write().unwrap();
            if let Some((row_id, _)) = covalues.get(id) {
                Some(*row_id)
            } else if let Some(h) = header {
                let mut next_id = self.next_covalue_id.write().unwrap();
                let row_id = *next_id;
                *next_id += 1;
                covalues.insert(id.to_string(), (row_id, h.clone()));
                Some(row_id)
            } else {
                None
            }
        }

        fn get_covalue_sessions(&self, covalue_row_id: u64) -> Vec<StoredSessionRow> {
            let sessions = self.sessions.read().unwrap();
            sessions.get(&covalue_row_id).cloned().unwrap_or_default()
        }

        fn get_new_transaction_in_session(
            &self,
            session_row_id: u64,
            from_idx: u64,
            to_idx: u64,
        ) -> Vec<TransactionRow> {
            let transactions = self.transactions.read().unwrap();
            transactions
                .get(&session_row_id)
                .map(|txs| {
                    txs.iter()
                        .filter(|tx| tx.idx >= from_idx && tx.idx < to_idx)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default()
        }

        fn get_signatures(&self, session_row_id: u64, first_new_tx_idx: u64) -> Vec<SignatureAfterRow> {
            let signatures = self.signatures.read().unwrap();
            signatures
                .get(&session_row_id)
                .map(|sigs| {
                    sigs.iter()
                        .filter(|sig| sig.idx >= first_new_tx_idx)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default()
        }

        fn transaction<F, R>(&self, callback: F) -> StorageResult<R>
        where
            F: FnOnce(&dyn StorageTransaction) -> StorageResult<R>,
        {
            // Simple implementation: just run the callback
            // A real implementation would handle rollback on error
            struct InMemoryTx<'a> {
                storage: &'a InMemoryStorage,
            }

            impl<'a> StorageTransaction for InMemoryTx<'a> {
                fn get_single_covalue_session(
                    &self,
                    covalue_row_id: u64,
                    session_id: &str,
                ) -> Option<StoredSessionRow> {
                    let sessions = self.storage.sessions.read().unwrap();
                    sessions.get(&covalue_row_id).and_then(|s| {
                        s.iter().find(|sess| sess.session_id == session_id).cloned()
                    })
                }

                fn mark_covalue_as_deleted(&self, id: &str) -> StorageResult<()> {
                    let mut deletions = self.storage.deletions.write().unwrap();
                    deletions.insert(id.to_string(), DeletionStatus::Pending);
                    Ok(())
                }

                fn add_session_update(&self, update: &SessionUpdate) -> StorageResult<u64> {
                    let mut sessions = self.storage.sessions.write().unwrap();
                    let mut next_id = self.storage.next_session_id.write().unwrap();

                    let row_id = if let Some(existing) = &update.session_row {
                        existing.row_id
                    } else {
                        let id = *next_id;
                        *next_id += 1;
                        id
                    };

                    let stored = StoredSessionRow {
                        row_id,
                        covalue: update.session_update.covalue,
                        session_id: update.session_update.session_id.clone(),
                        last_idx: update.session_update.last_idx,
                        last_signature: update.session_update.last_signature.clone(),
                        bytes_since_last_signature: update.session_update.bytes_since_last_signature,
                    };

                    sessions
                        .entry(update.session_update.covalue)
                        .or_insert_with(Vec::new)
                        .retain(|s| s.row_id != row_id);
                    sessions
                        .get_mut(&update.session_update.covalue)
                        .unwrap()
                        .push(stored);

                    Ok(row_id)
                }

                fn add_transaction(
                    &self,
                    session_row_id: u64,
                    idx: u64,
                    new_transaction: &Transaction,
                ) -> StorageResult<u64> {
                    let mut transactions = self.storage.transactions.write().unwrap();
                    let row = TransactionRow {
                        ses: session_row_id,
                        idx,
                        tx: new_transaction.clone(),
                    };
                    transactions
                        .entry(session_row_id)
                        .or_insert_with(Vec::new)
                        .push(row);
                    Ok(1)
                }

                fn add_signature_after(
                    &self,
                    session_row_id: u64,
                    idx: u64,
                    signature: &Signature,
                ) -> StorageResult<()> {
                    let mut signatures = self.storage.signatures.write().unwrap();
                    let row = SignatureAfterRow {
                        ses: session_row_id,
                        idx,
                        signature: signature.clone(),
                    };
                    signatures
                        .entry(session_row_id)
                        .or_insert_with(Vec::new)
                        .push(row);
                    Ok(())
                }
            }

            let tx = InMemoryTx { storage: self };
            callback(&tx)
        }

        fn track_covalues_sync_state(&self, updates: &[SyncStateUpdate]) {
            let mut sync_state = self.sync_state.write().unwrap();
            for update in updates {
                sync_state.insert(
                    (update.id.clone(), update.peer_id.clone()),
                    update.synced,
                );
            }
        }

        fn get_unsynced_covalue_ids(&self) -> Vec<RawCoID> {
            let sync_state = self.sync_state.read().unwrap();
            let mut unsynced = std::collections::HashSet::new();
            for ((id, _), synced) in sync_state.iter() {
                if !*synced {
                    unsynced.insert(id.clone());
                }
            }
            unsynced.into_iter().collect()
        }

        fn stop_tracking_sync_state(&self, id: &str) {
            let mut sync_state = self.sync_state.write().unwrap();
            sync_state.retain(|(covalue_id, _), _| covalue_id != id);
        }

        fn get_all_covalues_waiting_for_delete(&self) -> Vec<RawCoID> {
            let deletions = self.deletions.read().unwrap();
            deletions
                .iter()
                .filter_map(|(id, status)| {
                    if *status == DeletionStatus::Pending {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect()
        }

        fn erase_covalue_but_keep_tombstone(&self, covalue_id: &str) -> StorageResult<()> {
            // In a real implementation, this would delete transactions/sessions
            // but keep the header and delete session
            let mut deletions = self.deletions.write().unwrap();
            if let Some(status) = deletions.get_mut(covalue_id) {
                *status = DeletionStatus::Done;
            }
            Ok(())
        }

        fn get_covalue_known_state(&self, covalue_id: &str) -> Option<CoValueKnownState> {
            let covalues = self.covalues.read().unwrap();
            if covalues.contains_key(covalue_id) {
                let sessions = self.sessions.read().unwrap();
                let covalue_row = self.get_covalue(covalue_id)?;
                let mut known_sessions = HashMap::new();

                if let Some(sess_list) = sessions.get(&covalue_row.row_id) {
                    for sess in sess_list {
                        known_sessions.insert(sess.session_id.clone(), sess.last_idx + 1);
                    }
                }

                Some(CoValueKnownState {
                    id: covalue_id.to_string(),
                    header: true,
                    sessions: known_sessions,
                })
            } else {
                None
            }
        }
    }

    #[test]
    fn test_in_memory_storage_basic_operations() {
        let storage = InMemoryStorage::new();

        // Test upsert and get
        let header = CoValueHeader {
            covalue_type: "comap".to_string(),
            ..Default::default()
        };
        let row_id = storage.upsert_covalue("co_test", Some(&header)).unwrap();
        assert_eq!(row_id, 1);

        let retrieved = storage.get_covalue("co_test").unwrap();
        assert_eq!(retrieved.id, "co_test");
        assert_eq!(retrieved.header.covalue_type, "comap");

        // Test that non-existent CoValue returns None
        assert!(storage.get_covalue("co_nonexistent").is_none());
    }

    #[test]
    fn test_in_memory_storage_transactions() {
        let storage = InMemoryStorage::new();

        let header = CoValueHeader::default();
        let covalue_row_id = storage.upsert_covalue("co_test", Some(&header)).unwrap();

        storage
            .transaction(|tx| {
                let session_update = SessionUpdate {
                    session_update: SessionRow {
                        covalue: covalue_row_id,
                        session_id: "session_1".to_string(),
                        last_idx: 0,
                        last_signature: "sig_123".to_string(),
                        bytes_since_last_signature: None,
                    },
                    session_row: None,
                };

                let session_row_id = tx.add_session_update(&session_update)?;

                let transaction = Transaction::Trusting(TrustingTransaction {
                    privacy: TrustingTransactionPrivacy::Trusting,
                    made_at: 1234567890,
                    changes: "[]".to_string(),
                    meta: None,
                });

                tx.add_transaction(session_row_id, 0, &transaction)?;
                tx.add_signature_after(session_row_id, 0, &"sig_abc".to_string())?;

                Ok(())
            })
            .unwrap();

        let sessions = storage.get_covalue_sessions(covalue_row_id);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_id, "session_1");
    }

    #[test]
    fn test_in_memory_storage_sync_state() {
        let storage = InMemoryStorage::new();

        storage.track_covalues_sync_state(&[
            SyncStateUpdate {
                id: "co_1".to_string(),
                peer_id: "peer_a".to_string(),
                synced: false,
            },
            SyncStateUpdate {
                id: "co_2".to_string(),
                peer_id: "peer_a".to_string(),
                synced: true,
            },
        ]);

        let unsynced = storage.get_unsynced_covalue_ids();
        assert_eq!(unsynced.len(), 1);
        assert!(unsynced.contains(&"co_1".to_string()));

        storage.stop_tracking_sync_state("co_1");
        let unsynced = storage.get_unsynced_covalue_ids();
        assert!(unsynced.is_empty());
    }

    #[test]
    fn test_in_memory_storage_deletion() {
        let storage = InMemoryStorage::new();

        storage
            .transaction(|tx| {
                tx.mark_covalue_as_deleted("co_to_delete")?;
                Ok(())
            })
            .unwrap();

        let pending = storage.get_all_covalues_waiting_for_delete();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0], "co_to_delete");

        storage.erase_covalue_but_keep_tombstone("co_to_delete").unwrap();

        let pending = storage.get_all_covalues_waiting_for_delete();
        assert!(pending.is_empty());
    }
}
