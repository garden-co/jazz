//! BTree-based storage backend implementation.
//!
//! This provides a storage backend using Rust's BTreeMap data structures.
//! It implements the StorageBackend trait and can be used as a drop-in
//! replacement for other storage backends.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{atomic::{AtomicU64, Ordering}, RwLock};

use crate::error::{StorageError, StorageResult};
use crate::traits::{StorageBackend, StorageTransaction, StorageStats};
use crate::types::*;

use super::BTreeConfig;

/// BTree-based storage backend.
///
/// This is a high-performance in-memory storage backend that uses BTreeMap
/// for efficient range queries and sorted iteration.
///
/// # Example
///
/// ```rust
/// use cojson_storage::bftree::{BTreeStorage, BTreeConfig};
/// use cojson_storage::{StorageBackend, CoValueHeader};
///
/// let storage = BTreeStorage::new(BTreeConfig::default());
///
/// // Store a CoValue
/// let header = CoValueHeader::default();
/// let row_id = storage.upsert_covalue("co_test123", Some(&header)).unwrap();
///
/// // Retrieve it
/// let stored = storage.get_covalue("co_test123").unwrap();
/// assert_eq!(stored.id, "co_test123");
/// ```
pub struct BTreeStorage {
    /// Configuration
    config: BTreeConfig,

    /// CoValue storage: id -> (row_id, header)
    covalues: RwLock<BTreeMap<String, (u64, CoValueHeader)>>,

    /// Session storage: (covalue_row_id, session_id) -> StoredSessionRow
    sessions: RwLock<BTreeMap<(u64, String), StoredSessionRow>>,

    /// Transaction storage: (session_row_id, idx) -> TransactionRow
    transactions: RwLock<BTreeMap<(u64, u64), TransactionRow>>,

    /// Signature storage: (session_row_id, idx) -> SignatureAfterRow
    signatures: RwLock<BTreeMap<(u64, u64), SignatureAfterRow>>,

    /// Sync state: (covalue_id, peer_id) -> synced
    sync_state: RwLock<HashMap<(String, String), bool>>,

    /// Deletion queue: covalue_id -> status
    deletions: RwLock<HashMap<String, DeletionStatus>>,

    /// Next row IDs
    next_covalue_id: AtomicU64,
    next_session_id: AtomicU64,
}

impl BTreeStorage {
    /// Create a new BTree storage with the given configuration.
    pub fn new(config: BTreeConfig) -> Self {
        Self {
            config,
            covalues: RwLock::new(BTreeMap::new()),
            sessions: RwLock::new(BTreeMap::new()),
            transactions: RwLock::new(BTreeMap::new()),
            signatures: RwLock::new(BTreeMap::new()),
            sync_state: RwLock::new(HashMap::new()),
            deletions: RwLock::new(HashMap::new()),
            next_covalue_id: AtomicU64::new(1),
            next_session_id: AtomicU64::new(1),
        }
    }

    /// Create a new BTree storage with default configuration.
    pub fn new_default() -> Self {
        Self::new(BTreeConfig::default())
    }

    /// Get storage statistics.
    pub fn stats(&self) -> StorageStats {
        let covalues = self.covalues.read().unwrap();
        let sessions = self.sessions.read().unwrap();
        let transactions = self.transactions.read().unwrap();

        StorageStats {
            covalue_count: covalues.len() as u64,
            session_count: sessions.len() as u64,
            transaction_count: transactions.len() as u64,
            total_size_bytes: None, // Would need serialization to compute
        }
    }

    /// Clear all data from storage.
    pub fn clear(&self) {
        self.covalues.write().unwrap().clear();
        self.sessions.write().unwrap().clear();
        self.transactions.write().unwrap().clear();
        self.signatures.write().unwrap().clear();
        self.sync_state.write().unwrap().clear();
        self.deletions.write().unwrap().clear();
    }

    /// Get the configuration.
    pub fn config(&self) -> &BTreeConfig {
        &self.config
    }
}

impl StorageBackend for BTreeStorage {
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
            // Existing CoValue - return its row ID
            Some(*row_id)
        } else if let Some(h) = header {
            // New CoValue with header
            let row_id = self.next_covalue_id.fetch_add(1, Ordering::SeqCst);
            covalues.insert(id.to_string(), (row_id, h.clone()));
            Some(row_id)
        } else {
            // No header provided and CoValue doesn't exist
            None
        }
    }

    fn get_covalue_sessions(&self, covalue_row_id: u64) -> Vec<StoredSessionRow> {
        let sessions = self.sessions.read().unwrap();

        // Range query: get all sessions for this CoValue
        sessions
            .range((covalue_row_id, String::new())..)
            .take_while(|((cv_id, _), _)| *cv_id == covalue_row_id)
            .map(|(_, session)| session.clone())
            .collect()
    }

    fn get_new_transaction_in_session(
        &self,
        session_row_id: u64,
        from_idx: u64,
        to_idx: u64,
    ) -> Vec<TransactionRow> {
        let transactions = self.transactions.read().unwrap();

        // Range query: get transactions in the specified range
        transactions
            .range((session_row_id, from_idx)..(session_row_id, to_idx))
            .map(|(_, tx)| tx.clone())
            .collect()
    }

    fn get_signatures(&self, session_row_id: u64, first_new_tx_idx: u64) -> Vec<SignatureAfterRow> {
        let signatures = self.signatures.read().unwrap();

        // Range query: get signatures from the specified index
        signatures
            .range((session_row_id, first_new_tx_idx)..)
            .take_while(|((ses_id, _), _)| *ses_id == session_row_id)
            .map(|(_, sig)| sig.clone())
            .collect()
    }

    fn transaction<F, R>(&self, callback: F) -> StorageResult<R>
    where
        F: FnOnce(&dyn StorageTransaction) -> StorageResult<R>,
    {
        // Create a transaction context
        let tx = BTreeTransaction {
            storage: self,
        };

        // Execute the callback
        // Note: In a real implementation, we'd need proper rollback support
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

        let mut unsynced = HashSet::new();
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
        // Get the CoValue row ID
        let covalue = self.get_covalue(covalue_id)
            .ok_or_else(|| StorageError::NotFound(covalue_id.to_string()))?;

        let covalue_row_id = covalue.row_id;

        // Get all sessions for this CoValue
        let sessions_to_process: Vec<StoredSessionRow> = {
            let sessions = self.sessions.read().unwrap();
            sessions
                .range((covalue_row_id, String::new())..)
                .take_while(|((cv_id, _), _)| *cv_id == covalue_row_id)
                .map(|(_, s)| s.clone())
                .collect()
        };

        // Identify delete sessions (keep these) and regular sessions (delete these)
        let delete_session_prefix = "delete_";

        for session in sessions_to_process {
            let is_delete_session = session.session_id.starts_with(delete_session_prefix);

            if !is_delete_session {
                // Remove all transactions for this session
                {
                    let mut transactions = self.transactions.write().unwrap();
                    transactions.retain(|(ses_id, _), _| *ses_id != session.row_id);
                }

                // Remove all signatures for this session
                {
                    let mut signatures = self.signatures.write().unwrap();
                    signatures.retain(|(ses_id, _), _| *ses_id != session.row_id);
                }

                // Remove the session itself
                {
                    let mut sessions = self.sessions.write().unwrap();
                    sessions.remove(&(covalue_row_id, session.session_id));
                }
            }
        }

        // Mark deletion as complete
        {
            let mut deletions = self.deletions.write().unwrap();
            deletions.insert(covalue_id.to_string(), DeletionStatus::Done);
        }

        Ok(())
    }

    fn get_covalue_known_state(&self, covalue_id: &str) -> Option<CoValueKnownState> {
        let covalues = self.covalues.read().unwrap();

        if let Some((row_id, _)) = covalues.get(covalue_id) {
            let sessions = self.sessions.read().unwrap();

            let mut known_sessions = HashMap::new();

            // Get all sessions for this CoValue
            for ((cv_id, _), session) in sessions.range((*row_id, String::new())..) {
                if *cv_id != *row_id {
                    break;
                }
                // Transaction count is last_idx + 1
                known_sessions.insert(session.session_id.clone(), session.last_idx + 1);
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

/// Transaction context for BTree storage.
struct BTreeTransaction<'a> {
    storage: &'a BTreeStorage,
}

impl<'a> StorageTransaction for BTreeTransaction<'a> {
    fn get_single_covalue_session(
        &self,
        covalue_row_id: u64,
        session_id: &str,
    ) -> Option<StoredSessionRow> {
        let sessions = self.storage.sessions.read().unwrap();
        sessions.get(&(covalue_row_id, session_id.to_string())).cloned()
    }

    fn mark_covalue_as_deleted(&self, id: &str) -> StorageResult<()> {
        let mut deletions = self.storage.deletions.write().unwrap();
        deletions.insert(id.to_string(), DeletionStatus::Pending);
        Ok(())
    }

    fn add_session_update(&self, update: &SessionUpdate) -> StorageResult<u64> {
        let mut sessions = self.storage.sessions.write().unwrap();

        let row_id = if let Some(existing) = &update.session_row {
            existing.row_id
        } else {
            self.storage.next_session_id.fetch_add(1, Ordering::SeqCst)
        };

        let stored = StoredSessionRow {
            row_id,
            covalue: update.session_update.covalue,
            session_id: update.session_update.session_id.clone(),
            last_idx: update.session_update.last_idx,
            last_signature: update.session_update.last_signature.clone(),
            bytes_since_last_signature: update.session_update.bytes_since_last_signature,
        };

        sessions.insert(
            (update.session_update.covalue, update.session_update.session_id.clone()),
            stored,
        );

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

        transactions.insert((session_row_id, idx), row);

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

        signatures.insert((session_row_id, idx), row);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_storage_basic_operations() {
        let storage = BTreeStorage::new_default();

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
    fn test_btree_storage_transactions() {
        let storage = BTreeStorage::new_default();

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
    fn test_btree_storage_range_queries() {
        let storage = BTreeStorage::new_default();

        let header = CoValueHeader::default();
        let covalue_row_id = storage.upsert_covalue("co_test", Some(&header)).unwrap();

        // Add multiple sessions and transactions
        storage
            .transaction(|tx| {
                // Session 1 with 5 transactions
                let session1 = SessionUpdate {
                    session_update: SessionRow {
                        covalue: covalue_row_id,
                        session_id: "session_1".to_string(),
                        last_idx: 4,
                        last_signature: "sig_1".to_string(),
                        bytes_since_last_signature: None,
                    },
                    session_row: None,
                };
                let sess1_id = tx.add_session_update(&session1)?;

                for i in 0..5 {
                    let transaction = Transaction::Trusting(TrustingTransaction {
                        privacy: TrustingTransactionPrivacy::Trusting,
                        made_at: 1000 + i as i64,
                        changes: format!("[{{\"idx\": {}}}]", i),
                        meta: None,
                    });
                    tx.add_transaction(sess1_id, i, &transaction)?;
                }

                // Session 2 with 3 transactions
                let session2 = SessionUpdate {
                    session_update: SessionRow {
                        covalue: covalue_row_id,
                        session_id: "session_2".to_string(),
                        last_idx: 2,
                        last_signature: "sig_2".to_string(),
                        bytes_since_last_signature: None,
                    },
                    session_row: None,
                };
                let sess2_id = tx.add_session_update(&session2)?;

                for i in 0..3 {
                    let transaction = Transaction::Trusting(TrustingTransaction {
                        privacy: TrustingTransactionPrivacy::Trusting,
                        made_at: 2000 + i as i64,
                        changes: format!("[{{\"idx\": {}}}]", i),
                        meta: None,
                    });
                    tx.add_transaction(sess2_id, i, &transaction)?;
                }

                Ok(())
            })
            .unwrap();

        // Test range query for transactions
        let sessions = storage.get_covalue_sessions(covalue_row_id);
        assert_eq!(sessions.len(), 2);

        let sess1 = sessions.iter().find(|s| s.session_id == "session_1").unwrap();
        let txs = storage.get_new_transaction_in_session(sess1.row_id, 2, 4);
        assert_eq!(txs.len(), 2);
        assert_eq!(txs[0].idx, 2);
        assert_eq!(txs[1].idx, 3);
    }

    #[test]
    fn test_btree_storage_sync_state() {
        let storage = BTreeStorage::new_default();

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
            SyncStateUpdate {
                id: "co_1".to_string(),
                peer_id: "peer_b".to_string(),
                synced: true,
            },
        ]);

        let unsynced = storage.get_unsynced_covalue_ids();
        assert_eq!(unsynced.len(), 1);
        assert!(unsynced.contains(&"co_1".to_string()));

        // Mark as synced
        storage.track_covalues_sync_state(&[SyncStateUpdate {
            id: "co_1".to_string(),
            peer_id: "peer_a".to_string(),
            synced: true,
        }]);

        let unsynced = storage.get_unsynced_covalue_ids();
        assert!(unsynced.is_empty());
    }

    #[test]
    fn test_btree_storage_deletion() {
        let storage = BTreeStorage::new_default();

        let header = CoValueHeader::default();
        let covalue_row_id = storage.upsert_covalue("co_to_delete", Some(&header)).unwrap();

        // Add sessions and transactions
        storage
            .transaction(|tx| {
                let session = SessionUpdate {
                    session_update: SessionRow {
                        covalue: covalue_row_id,
                        session_id: "regular_session".to_string(),
                        last_idx: 0,
                        last_signature: "sig".to_string(),
                        bytes_since_last_signature: None,
                    },
                    session_row: None,
                };
                let sess_id = tx.add_session_update(&session)?;

                let transaction = Transaction::Trusting(TrustingTransaction {
                    privacy: TrustingTransactionPrivacy::Trusting,
                    made_at: 1000,
                    changes: "[]".to_string(),
                    meta: None,
                });
                tx.add_transaction(sess_id, 0, &transaction)?;

                // Mark for deletion
                tx.mark_covalue_as_deleted("co_to_delete")?;

                Ok(())
            })
            .unwrap();

        // Check pending deletions
        let pending = storage.get_all_covalues_waiting_for_delete();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0], "co_to_delete");

        // Erase but keep tombstone
        storage.erase_covalue_but_keep_tombstone("co_to_delete").unwrap();

        // Check deletion complete
        let pending = storage.get_all_covalues_waiting_for_delete();
        assert!(pending.is_empty());

        // CoValue should still exist (tombstone)
        assert!(storage.get_covalue("co_to_delete").is_some());

        // But sessions should be gone
        let sessions = storage.get_covalue_sessions(covalue_row_id);
        assert!(sessions.is_empty());
    }

    #[test]
    fn test_btree_storage_known_state() {
        let storage = BTreeStorage::new_default();

        // Non-existent CoValue
        assert!(storage.get_covalue_known_state("co_nonexistent").is_none());

        // Create CoValue with sessions
        let header = CoValueHeader::default();
        let covalue_row_id = storage.upsert_covalue("co_test", Some(&header)).unwrap();

        storage
            .transaction(|tx| {
                let session = SessionUpdate {
                    session_update: SessionRow {
                        covalue: covalue_row_id,
                        session_id: "session_1".to_string(),
                        last_idx: 4, // 5 transactions (0-4)
                        last_signature: "sig".to_string(),
                        bytes_since_last_signature: None,
                    },
                    session_row: None,
                };
                tx.add_session_update(&session)?;
                Ok(())
            })
            .unwrap();

        let known_state = storage.get_covalue_known_state("co_test").unwrap();
        assert_eq!(known_state.id, "co_test");
        assert!(known_state.header);
        assert_eq!(known_state.sessions.get("session_1"), Some(&5));
    }

    #[test]
    fn test_btree_storage_stats() {
        let storage = BTreeStorage::new_default();

        let stats = storage.stats();
        assert_eq!(stats.covalue_count, 0);
        assert_eq!(stats.session_count, 0);
        assert_eq!(stats.transaction_count, 0);

        let header = CoValueHeader::default();
        storage.upsert_covalue("co_1", Some(&header));
        storage.upsert_covalue("co_2", Some(&header));

        let stats = storage.stats();
        assert_eq!(stats.covalue_count, 2);
    }
}
