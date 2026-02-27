//! Async storage traits for asynchronous operations.
//!
//! These traits correspond to the TypeScript `DBClientInterfaceAsync` and
//! `DBTransactionInterfaceAsync` interfaces in `packages/cojson/src/storage/types.ts`.
//!
//! Enable with the `async` feature flag.

use crate::error::StorageResult;
use crate::types::*;
use std::future::Future;
use std::pin::Pin;

/// Type alias for boxed futures (for trait object compatibility).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An asynchronous storage backend for CoValues.
///
/// This trait mirrors the TypeScript `DBClientInterfaceAsync` interface.
/// Implementations must be thread-safe (Send + Sync).
///
/// # Example
///
/// ```rust,ignore
/// use cojson_storage::{StorageBackendAsync, StoredCoValueRow, CoValueHeader};
/// use std::future::Future;
///
/// struct MyAsyncStorage { /* ... */ }
///
/// impl StorageBackendAsync for MyAsyncStorage {
///     async fn get_covalue(&self, id: &str) -> Option<StoredCoValueRow> {
///         // Look up CoValue by ID asynchronously
///         None
///     }
///     // ... implement other methods
/// }
/// ```
pub trait StorageBackendAsync: Send + Sync {
    /// Get a CoValue by its ID.
    ///
    /// Returns `Some(StoredCoValueRow)` if found, `None` otherwise.
    fn get_covalue(&self, covalue_id: &str) -> BoxFuture<'_, Option<StoredCoValueRow>>;

    /// Insert or update a CoValue.
    ///
    /// If the CoValue doesn't exist, inserts it with the given header.
    /// If it exists and header is `None`, returns the existing row ID.
    /// If it exists and header is `Some`, updates the header.
    ///
    /// Returns the row ID of the CoValue.
    fn upsert_covalue<'a>(
        &'a self,
        id: &'a str,
        header: Option<&'a CoValueHeader>,
    ) -> BoxFuture<'a, Option<u64>>;

    /// Get all sessions for a CoValue.
    ///
    /// Returns all sessions associated with the given CoValue row ID.
    fn get_covalue_sessions(&self, covalue_row_id: u64) -> BoxFuture<'_, Vec<StoredSessionRow>>;

    /// Get transactions in a session within a range.
    ///
    /// Returns transactions with indices in `[from_idx, to_idx)`.
    fn get_new_transaction_in_session(
        &self,
        session_row_id: u64,
        from_idx: u64,
        to_idx: u64,
    ) -> BoxFuture<'_, Vec<TransactionRow>>;

    /// Get signatures after a given transaction index.
    ///
    /// Returns all signature checkpoints with index >= `first_new_tx_idx`.
    fn get_signatures(
        &self,
        session_row_id: u64,
        first_new_tx_idx: u64,
    ) -> BoxFuture<'_, Vec<SignatureAfterRow>>;

    /// Execute a transaction with a callback.
    ///
    /// The callback receives a `StorageTransactionAsync` that can perform
    /// multiple operations atomically. If the callback returns an error,
    /// the transaction is rolled back.
    fn transaction<'a, F, Fut, R>(&'a self, callback: F) -> BoxFuture<'a, StorageResult<R>>
    where
        F: FnOnce(Box<dyn StorageTransactionAsync + 'a>) -> Fut + Send + 'a,
        Fut: Future<Output = StorageResult<R>> + Send + 'a,
        R: Send + 'a;

    /// Track sync state for multiple CoValues.
    ///
    /// Records which peers have synced which CoValues.
    fn track_covalues_sync_state<'a>(
        &'a self,
        updates: &'a [SyncStateUpdate],
    ) -> BoxFuture<'a, ()>;

    /// Get all CoValue IDs that have at least one unsynced peer.
    fn get_unsynced_covalue_ids(&self) -> BoxFuture<'_, Vec<RawCoID>>;

    /// Stop tracking sync state for a CoValue.
    ///
    /// Removes all peer entries for the given CoValue.
    fn stop_tracking_sync_state<'a>(&'a self, id: &'a str) -> BoxFuture<'a, ()>;

    /// Enumerate all CoValue IDs pending deletion.
    ///
    /// Returns IDs in the deletion work queue with status `Pending`.
    fn get_all_covalues_waiting_for_delete(&self) -> BoxFuture<'_, Vec<RawCoID>>;

    /// Erase a CoValue's data but keep the tombstone.
    ///
    /// Deletes all transactions and non-delete sessions while preserving
    /// the header and delete session(s) as a tombstone.
    fn erase_covalue_but_keep_tombstone<'a>(
        &'a self,
        covalue_id: &'a str,
    ) -> BoxFuture<'a, StorageResult<()>>;

    /// Get the known state for a CoValue without loading transactions.
    ///
    /// Returns `None` if the CoValue doesn't exist.
    fn get_covalue_known_state<'a>(
        &'a self,
        covalue_id: &'a str,
    ) -> BoxFuture<'a, Option<CoValueKnownState>>;
}

/// An asynchronous storage transaction for atomic operations.
///
/// This trait mirrors the TypeScript `DBTransactionInterfaceAsync` interface.
/// Implementations should ensure operations are atomic within the transaction.
pub trait StorageTransactionAsync: Send + Sync {
    /// Get a single session for a CoValue.
    ///
    /// Returns the session with the given session ID for the CoValue.
    fn get_single_covalue_session<'a>(
        &'a self,
        covalue_row_id: u64,
        session_id: &'a str,
    ) -> BoxFuture<'a, Option<StoredSessionRow>>;

    /// Mark a CoValue as deleted.
    ///
    /// Adds the CoValue to the deletion work queue with status `Pending`.
    /// This is idempotent - calling multiple times has no additional effect.
    fn mark_covalue_as_deleted<'a>(&'a self, id: &'a str) -> BoxFuture<'a, StorageResult<()>>;

    /// Add or update a session.
    ///
    /// If `session_row` is `None`, creates a new session.
    /// If `session_row` is `Some`, updates the existing session.
    ///
    /// Returns the row ID of the session.
    fn add_session_update<'a>(
        &'a self,
        update: &'a SessionUpdate,
    ) -> BoxFuture<'a, StorageResult<u64>>;

    /// Add a transaction to a session.
    ///
    /// Returns the number of transactions added (typically 1).
    fn add_transaction<'a>(
        &'a self,
        session_row_id: u64,
        idx: u64,
        new_transaction: &'a Transaction,
    ) -> BoxFuture<'a, StorageResult<u64>>;

    /// Add a signature checkpoint after a transaction.
    ///
    /// Records that a signature covers transactions up to and including `idx`.
    fn add_signature_after<'a>(
        &'a self,
        session_row_id: u64,
        idx: u64,
        signature: &'a Signature,
    ) -> BoxFuture<'a, StorageResult<()>>;
}

/// Extension trait for additional async storage operations.
///
/// These are optional operations that may not be supported by all backends.
pub trait StorageBackendAsyncExt: StorageBackendAsync {
    /// Close the storage backend and release resources.
    ///
    /// After calling this, the storage should not be used.
    fn close(&self) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async { Ok(()) })
    }

    /// Check if the storage backend is healthy.
    ///
    /// Returns `true` if the backend is operational.
    fn is_healthy(&self) -> BoxFuture<'_, bool> {
        Box::pin(async { true })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    /// A simple async in-memory storage implementation for testing.
    struct InMemoryAsyncStorage {
        covalues: Arc<RwLock<HashMap<String, (u64, CoValueHeader)>>>,
        sessions: Arc<RwLock<HashMap<u64, Vec<StoredSessionRow>>>>,
        transactions: Arc<RwLock<HashMap<u64, Vec<TransactionRow>>>>,
        signatures: Arc<RwLock<HashMap<u64, Vec<SignatureAfterRow>>>>,
        sync_state: Arc<RwLock<HashMap<(String, String), bool>>>,
        deletions: Arc<RwLock<HashMap<String, DeletionStatus>>>,
        next_covalue_id: Arc<RwLock<u64>>,
        next_session_id: Arc<RwLock<u64>>,
    }

    impl InMemoryAsyncStorage {
        fn new() -> Self {
            Self {
                covalues: Arc::new(RwLock::new(HashMap::new())),
                sessions: Arc::new(RwLock::new(HashMap::new())),
                transactions: Arc::new(RwLock::new(HashMap::new())),
                signatures: Arc::new(RwLock::new(HashMap::new())),
                sync_state: Arc::new(RwLock::new(HashMap::new())),
                deletions: Arc::new(RwLock::new(HashMap::new())),
                next_covalue_id: Arc::new(RwLock::new(1)),
                next_session_id: Arc::new(RwLock::new(1)),
            }
        }
    }

    impl StorageBackendAsync for InMemoryAsyncStorage {
        fn get_covalue(&self, covalue_id: &str) -> BoxFuture<'_, Option<StoredCoValueRow>> {
            let covalue_id = covalue_id.to_string();
            Box::pin(async move {
                let covalues = self.covalues.read().await;
                covalues.get(&covalue_id).map(|(row_id, header)| StoredCoValueRow {
                    row_id: *row_id,
                    id: covalue_id,
                    header: header.clone(),
                })
            })
        }

        fn upsert_covalue<'a>(
            &'a self,
            id: &'a str,
            header: Option<&'a CoValueHeader>,
        ) -> BoxFuture<'a, Option<u64>> {
            Box::pin(async move {
                let mut covalues = self.covalues.write().await;
                if let Some((row_id, _)) = covalues.get(id) {
                    Some(*row_id)
                } else if let Some(h) = header {
                    let mut next_id = self.next_covalue_id.write().await;
                    let row_id = *next_id;
                    *next_id += 1;
                    covalues.insert(id.to_string(), (row_id, h.clone()));
                    Some(row_id)
                } else {
                    None
                }
            })
        }

        fn get_covalue_sessions(&self, covalue_row_id: u64) -> BoxFuture<'_, Vec<StoredSessionRow>> {
            Box::pin(async move {
                let sessions = self.sessions.read().await;
                sessions.get(&covalue_row_id).cloned().unwrap_or_default()
            })
        }

        fn get_new_transaction_in_session(
            &self,
            session_row_id: u64,
            from_idx: u64,
            to_idx: u64,
        ) -> BoxFuture<'_, Vec<TransactionRow>> {
            Box::pin(async move {
                let transactions = self.transactions.read().await;
                transactions
                    .get(&session_row_id)
                    .map(|txs| {
                        txs.iter()
                            .filter(|tx| tx.idx >= from_idx && tx.idx < to_idx)
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default()
            })
        }

        fn get_signatures(
            &self,
            session_row_id: u64,
            first_new_tx_idx: u64,
        ) -> BoxFuture<'_, Vec<SignatureAfterRow>> {
            Box::pin(async move {
                let signatures = self.signatures.read().await;
                signatures
                    .get(&session_row_id)
                    .map(|sigs| {
                        sigs.iter()
                            .filter(|sig| sig.idx >= first_new_tx_idx)
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default()
            })
        }

        fn transaction<'a, F, Fut, R>(&'a self, callback: F) -> BoxFuture<'a, StorageResult<R>>
        where
            F: FnOnce(Box<dyn StorageTransactionAsync + 'a>) -> Fut + Send + 'a,
            Fut: Future<Output = StorageResult<R>> + Send + 'a,
            R: Send + 'a,
        {
            Box::pin(async move {
                // Create transaction handle
                struct InMemoryAsyncTx<'a> {
                    storage: &'a InMemoryAsyncStorage,
                }

                impl<'a> StorageTransactionAsync for InMemoryAsyncTx<'a> {
                    fn get_single_covalue_session<'b>(
                        &'b self,
                        covalue_row_id: u64,
                        session_id: &'b str,
                    ) -> BoxFuture<'b, Option<StoredSessionRow>> {
                        let session_id = session_id.to_string();
                        Box::pin(async move {
                            let sessions = self.storage.sessions.read().await;
                            sessions.get(&covalue_row_id).and_then(|s| {
                                s.iter().find(|sess| sess.session_id == session_id).cloned()
                            })
                        })
                    }

                    fn mark_covalue_as_deleted<'b>(
                        &'b self,
                        id: &'b str,
                    ) -> BoxFuture<'b, StorageResult<()>> {
                        Box::pin(async move {
                            let mut deletions = self.storage.deletions.write().await;
                            deletions.insert(id.to_string(), DeletionStatus::Pending);
                            Ok(())
                        })
                    }

                    fn add_session_update<'b>(
                        &'b self,
                        update: &'b SessionUpdate,
                    ) -> BoxFuture<'b, StorageResult<u64>> {
                        Box::pin(async move {
                            let mut sessions = self.storage.sessions.write().await;
                            let mut next_id = self.storage.next_session_id.write().await;

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
                                bytes_since_last_signature: update
                                    .session_update
                                    .bytes_since_last_signature,
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
                        })
                    }

                    fn add_transaction<'b>(
                        &'b self,
                        session_row_id: u64,
                        idx: u64,
                        new_transaction: &'b Transaction,
                    ) -> BoxFuture<'b, StorageResult<u64>> {
                        let tx = new_transaction.clone();
                        Box::pin(async move {
                            let mut transactions = self.storage.transactions.write().await;
                            let row = TransactionRow {
                                ses: session_row_id,
                                idx,
                                tx,
                            };
                            transactions
                                .entry(session_row_id)
                                .or_insert_with(Vec::new)
                                .push(row);
                            Ok(1)
                        })
                    }

                    fn add_signature_after<'b>(
                        &'b self,
                        session_row_id: u64,
                        idx: u64,
                        signature: &'b Signature,
                    ) -> BoxFuture<'b, StorageResult<()>> {
                        let sig = signature.clone();
                        Box::pin(async move {
                            let mut signatures = self.storage.signatures.write().await;
                            let row = SignatureAfterRow {
                                ses: session_row_id,
                                idx,
                                signature: sig,
                            };
                            signatures
                                .entry(session_row_id)
                                .or_insert_with(Vec::new)
                                .push(row);
                            Ok(())
                        })
                    }
                }

                let tx = Box::new(InMemoryAsyncTx { storage: self });
                callback(tx).await
            })
        }

        fn track_covalues_sync_state<'a>(
            &'a self,
            updates: &'a [SyncStateUpdate],
        ) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                let mut sync_state = self.sync_state.write().await;
                for update in updates {
                    sync_state.insert(
                        (update.id.clone(), update.peer_id.clone()),
                        update.synced,
                    );
                }
            })
        }

        fn get_unsynced_covalue_ids(&self) -> BoxFuture<'_, Vec<RawCoID>> {
            Box::pin(async move {
                let sync_state = self.sync_state.read().await;
                let mut unsynced = std::collections::HashSet::new();
                for ((id, _), synced) in sync_state.iter() {
                    if !*synced {
                        unsynced.insert(id.clone());
                    }
                }
                unsynced.into_iter().collect()
            })
        }

        fn stop_tracking_sync_state<'a>(&'a self, id: &'a str) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                let mut sync_state = self.sync_state.write().await;
                sync_state.retain(|(covalue_id, _), _| covalue_id != id);
            })
        }

        fn get_all_covalues_waiting_for_delete(&self) -> BoxFuture<'_, Vec<RawCoID>> {
            Box::pin(async move {
                let deletions = self.deletions.read().await;
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
            })
        }

        fn erase_covalue_but_keep_tombstone<'a>(
            &'a self,
            covalue_id: &'a str,
        ) -> BoxFuture<'a, StorageResult<()>> {
            Box::pin(async move {
                let mut deletions = self.deletions.write().await;
                if let Some(status) = deletions.get_mut(covalue_id) {
                    *status = DeletionStatus::Done;
                }
                Ok(())
            })
        }

        fn get_covalue_known_state<'a>(
            &'a self,
            covalue_id: &'a str,
        ) -> BoxFuture<'a, Option<CoValueKnownState>> {
            Box::pin(async move {
                let covalues = self.covalues.read().await;
                if covalues.contains_key(covalue_id) {
                    let sessions = self.sessions.read().await;
                    let covalue_row = {
                        let (row_id, header) = covalues.get(covalue_id)?;
                        StoredCoValueRow {
                            row_id: *row_id,
                            id: covalue_id.to_string(),
                            header: header.clone(),
                        }
                    };
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
            })
        }
    }

    #[tokio::test]
    async fn test_async_storage_basic_operations() {
        let storage = InMemoryAsyncStorage::new();

        // Test upsert and get
        let header = CoValueHeader {
            covalue_type: "comap".to_string(),
            ..Default::default()
        };
        let row_id = storage.upsert_covalue("co_test", Some(&header)).await.unwrap();
        assert_eq!(row_id, 1);

        let retrieved = storage.get_covalue("co_test").await.unwrap();
        assert_eq!(retrieved.id, "co_test");
        assert_eq!(retrieved.header.covalue_type, "comap");

        // Test that non-existent CoValue returns None
        assert!(storage.get_covalue("co_nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn test_async_storage_transactions() {
        let storage = InMemoryAsyncStorage::new();

        let header = CoValueHeader::default();
        let covalue_row_id = storage.upsert_covalue("co_test", Some(&header)).await.unwrap();

        storage
            .transaction(|tx| async move {
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

                let session_row_id = tx.add_session_update(&session_update).await?;

                let transaction = Transaction::Trusting(TrustingTransaction {
                    privacy: TrustingTransactionPrivacy::Trusting,
                    made_at: 1234567890,
                    changes: "[]".to_string(),
                    meta: None,
                });

                tx.add_transaction(session_row_id, 0, &transaction).await?;
                tx.add_signature_after(session_row_id, 0, &"sig_abc".to_string()).await?;

                Ok(())
            })
            .await
            .unwrap();

        let sessions = storage.get_covalue_sessions(covalue_row_id).await;
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_id, "session_1");
    }
}
