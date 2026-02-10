pub mod keys;

use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use keys::*;

#[derive(Debug, thiserror::Error)]
pub enum FjallStorageError {
    #[error("Fjall error: {0}")]
    Fjall(#[from] fjall::Error),

    #[error("Data corruption: {0}")]
    DataCorruption(String),
}

pub type Result<T> = std::result::Result<T, FjallStorageError>;

/// Result of a CoValue lookup.
#[derive(Debug, Clone)]
pub struct CoValueResult {
    pub row_id: u64,
    pub header_json: String,
}

/// Result of a session lookup.
#[derive(Debug, Clone)]
pub struct SessionResult {
    pub row_id: u64,
    pub co_value: u64,
    pub session_id: String,
    pub last_idx: u32,
    pub last_signature: String,
    pub bytes_since_last_signature: u32,
}

/// Result of a transaction lookup.
#[derive(Debug, Clone)]
pub struct TransactionResult {
    pub ses: u64,
    pub idx: u32,
    pub tx: String,
}

/// Result of a signature lookup.
#[derive(Debug, Clone)]
pub struct SignatureResult {
    pub idx: u32,
    pub signature: String,
}

/// Known state for a CoValue (header presence + session counters).
#[derive(Debug, Clone)]
pub struct KnownStateResult {
    pub id: String,
    /// Vec of (sessionID, lastIdx)
    pub sessions: Vec<(String, u32)>,
}

/// Session value encoding:
///   [0..8]   rowID (u64 BE)
///   [8..12]  lastIdx (u32 BE)
///   [12..16] bytesSinceLastSignature (u32 BE)
///   [16..]   lastSignature (UTF-8)
const SESSION_HEADER_SIZE: usize = 16;

fn encode_session_value(
    row_id: u64,
    last_idx: u32,
    bytes_since_last_sig: u32,
    last_signature: &str,
) -> Vec<u8> {
    let mut val = Vec::with_capacity(SESSION_HEADER_SIZE + last_signature.len());
    val.extend_from_slice(&encode_u64(row_id));
    val.extend_from_slice(&encode_u32(last_idx));
    val.extend_from_slice(&encode_u32(bytes_since_last_sig));
    val.extend_from_slice(last_signature.as_bytes());
    val
}

fn decode_session_value(
    key: &[u8],
    value: &[u8],
    co_value_row_id_from_key: u64,
) -> Result<SessionResult> {
    if value.len() < SESSION_HEADER_SIZE {
        return Err(FjallStorageError::DataCorruption(
            "Session value too short".into(),
        ));
    }
    let row_id = decode_u64(&value[..8]);
    let last_idx = decode_u32(&value[8..12]);
    let bytes_since_last_sig = decode_u32(&value[12..16]);
    let last_signature = std::str::from_utf8(&value[16..])
        .map_err(|e| FjallStorageError::DataCorruption(format!("Invalid UTF-8 in signature: {e}")))?
        .to_string();

    // sessionID is the part of the key after the 8-byte coValueRowID prefix
    let session_id = std::str::from_utf8(&key[8..])
        .map_err(|e| {
            FjallStorageError::DataCorruption(format!("Invalid UTF-8 in sessionID: {e}"))
        })?
        .to_string();

    Ok(SessionResult {
        row_id,
        co_value: co_value_row_id_from_key,
        session_id,
        last_idx,
        last_signature,
        bytes_since_last_signature: bytes_since_last_sig,
    })
}

/// CoValue value encoding:
///   [0..8]  rowID (u64 BE)
///   [8..]   header JSON (UTF-8)
fn encode_covalue_value(row_id: u64, header_json: &str) -> Vec<u8> {
    let mut val = Vec::with_capacity(8 + header_json.len());
    val.extend_from_slice(&encode_u64(row_id));
    val.extend_from_slice(header_json.as_bytes());
    val
}

/// The core fjall-based storage engine for Jazz.
///
/// Maps the relational SQLite schema to fjall keyspaces
/// with composite key encoding for efficient prefix/range scans.
pub struct FjallStorage {
    db: Database,
    covalue_by_id: Keyspace,
    covalue_by_row: Keyspace,
    session_by_cv_sid: Keyspace,
    session_by_row: Keyspace,
    transactions: Keyspace,
    signature_after: Keyspace,
    unsynced: Keyspace,
    deleted: Keyspace,
    meta: Keyspace,
}

impl FjallStorage {
    /// Open or create a fjall storage at the given path.
    pub fn open(path: &str) -> Result<Self> {
        let db = Database::builder(path).open()?;
        let ks = KeyspaceCreateOptions::default;

        Ok(Self {
            covalue_by_id: db.keyspace("covalue_by_id", ks)?,
            covalue_by_row: db.keyspace("covalue_by_row", ks)?,
            session_by_cv_sid: db.keyspace("session_by_cv_sid", ks)?,
            session_by_row: db.keyspace("session_by_row", ks)?,
            transactions: db.keyspace("transactions", ks)?,
            signature_after: db.keyspace("signature_after", ks)?,
            unsynced: db.keyspace("unsynced", ks)?,
            deleted: db.keyspace("deleted", ks)?,
            meta: db.keyspace("meta", ks)?,
            db,
        })
    }

    /// Close the storage, persisting all pending writes.
    pub fn close(&self) -> Result<()> {
        self.db.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }

    // ──────────────────────────────────────────────
    // Row ID generation
    // ──────────────────────────────────────────────

    fn next_row_id(&self, counter_key: &[u8]) -> Result<u64> {
        let current = self
            .meta
            .get(counter_key)?
            .map(|v| decode_u64(&v))
            .unwrap_or(1);
        let next = current + 1;
        self.meta.insert(counter_key, encode_u64(next))?;
        Ok(current)
    }

    fn next_covalue_row_id(&self) -> Result<u64> {
        self.next_row_id(b"next_covalue_row_id")
    }

    fn next_session_row_id(&self) -> Result<u64> {
        self.next_row_id(b"next_session_row_id")
    }

    // ──────────────────────────────────────────────
    // CoValue operations
    // ──────────────────────────────────────────────

    /// Get a CoValue by its ID. Returns None if not found.
    pub fn get_co_value(&self, co_value_id: &str) -> Result<Option<CoValueResult>> {
        let val = self.covalue_by_id.get(co_value_id.as_bytes())?;
        match val {
            None => Ok(None),
            Some(v) => {
                if v.len() < 8 {
                    return Err(FjallStorageError::DataCorruption(
                        "CoValue value too short".into(),
                    ));
                }
                let row_id = decode_u64(&v[..8]);
                let header_json = std::str::from_utf8(&v[8..])
                    .map_err(|e| {
                        FjallStorageError::DataCorruption(format!(
                            "Invalid UTF-8 in header: {e}"
                        ))
                    })?
                    .to_string();
                Ok(Some(CoValueResult {
                    row_id,
                    header_json,
                }))
            }
        }
    }

    /// Get a CoValue's rowID by its string ID.
    fn get_co_value_row_id(&self, id: &str) -> Result<Option<u64>> {
        let val = self.covalue_by_id.get(id.as_bytes())?;
        match val {
            None => Ok(None),
            Some(v) => {
                if v.len() < 8 {
                    return Err(FjallStorageError::DataCorruption(
                        "CoValue value too short".into(),
                    ));
                }
                Ok(Some(decode_u64(&v[..8])))
            }
        }
    }

    /// Upsert a CoValue. If header is None, just look up the existing rowID.
    /// Returns the rowID, or None if not found and no header provided.
    pub fn upsert_co_value(
        &self,
        id: &str,
        header_json: Option<&str>,
    ) -> Result<Option<u64>> {
        match header_json {
            None => self.get_co_value_row_id(id),
            Some(header) => {
                // Check if already exists (ON CONFLICT DO NOTHING behavior)
                if let Some(row_id) = self.get_co_value_row_id(id)? {
                    return Ok(Some(row_id));
                }
                // Allocate new rowID and insert
                let row_id = self.next_covalue_row_id()?;
                let val = encode_covalue_value(row_id, header);
                self.covalue_by_id.insert(id.as_bytes(), val)?;
                self.covalue_by_row
                    .insert(encode_u64(row_id), id.as_bytes())?;
                Ok(Some(row_id))
            }
        }
    }

    // ──────────────────────────────────────────────
    // Session operations
    // ──────────────────────────────────────────────

    /// Get all sessions for a CoValue.
    pub fn get_co_value_sessions(&self, co_value_row_id: u64) -> Result<Vec<SessionResult>> {
        let prefix = encode_u64(co_value_row_id);
        let mut sessions = Vec::new();
        for guard in self.session_by_cv_sid.prefix(prefix) {
            let (key, value) = guard.into_inner()?;
            sessions.push(decode_session_value(&key, &value, co_value_row_id)?);
        }
        Ok(sessions)
    }

    /// Get a single session for a CoValue + sessionID.
    pub fn get_single_co_value_session(
        &self,
        co_value_row_id: u64,
        session_id: &str,
    ) -> Result<Option<SessionResult>> {
        let key = encode_u64_suffix(co_value_row_id, session_id.as_bytes());
        let val = self.session_by_cv_sid.get(&key)?;
        match val {
            None => Ok(None),
            Some(v) => Ok(Some(decode_session_value(
                &key,
                &v,
                co_value_row_id,
            )?)),
        }
    }

    /// Upsert a session. Returns the session rowID.
    pub fn add_session_update(
        &self,
        co_value_row_id: u64,
        session_id: &str,
        last_idx: u32,
        last_signature: &str,
        bytes_since_last_signature: u32,
    ) -> Result<u64> {
        let key = encode_u64_suffix(co_value_row_id, session_id.as_bytes());

        // Check if session exists to get/reuse its rowID
        let session_row_id = match self.session_by_cv_sid.get(&key)? {
            Some(existing) => {
                if existing.len() < 8 {
                    return Err(FjallStorageError::DataCorruption(
                        "Session value too short".into(),
                    ));
                }
                decode_u64(&existing[..8])
            }
            None => {
                // New session — allocate rowID and write reverse index
                let new_id = self.next_session_row_id()?;
                self.session_by_row.insert(encode_u64(new_id), &key)?;
                new_id
            }
        };

        // Write updated session value
        let val = encode_session_value(
            session_row_id,
            last_idx,
            bytes_since_last_signature,
            last_signature,
        );
        self.session_by_cv_sid.insert(&key, val)?;

        Ok(session_row_id)
    }

    // ──────────────────────────────────────────────
    // Transaction operations
    // ──────────────────────────────────────────────

    /// Get transactions in a session within a range [from_idx, to_idx] inclusive.
    pub fn get_new_transaction_in_session(
        &self,
        session_row_id: u64,
        from_idx: u32,
        to_idx: u32,
    ) -> Result<Vec<TransactionResult>> {
        let start = encode_tx_key(session_row_id, from_idx);
        let end = encode_tx_key(session_row_id, to_idx);
        let mut txs = Vec::new();
        for guard in self.transactions.range(start..=end) {
            let (key, value) = guard.into_inner()?;
            if key.len() < 12 {
                continue;
            }
            let idx = decode_u32(&key[8..12]);
            let tx = std::str::from_utf8(&value)
                .map_err(|e| {
                    FjallStorageError::DataCorruption(format!("Invalid UTF-8 in tx: {e}"))
                })?
                .to_string();
            txs.push(TransactionResult {
                ses: session_row_id,
                idx,
                tx,
            });
        }
        Ok(txs)
    }

    /// Add a transaction to a session.
    pub fn add_transaction(
        &self,
        session_row_id: u64,
        idx: u32,
        tx_json: &str,
    ) -> Result<()> {
        let key = encode_tx_key(session_row_id, idx);
        self.transactions.insert(key, tx_json.as_bytes())?;
        Ok(())
    }

    // ──────────────────────────────────────────────
    // Signature operations
    // ──────────────────────────────────────────────

    /// Get signatures for a session starting from first_new_tx_idx.
    pub fn get_signatures(
        &self,
        session_row_id: u64,
        first_new_tx_idx: u32,
    ) -> Result<Vec<SignatureResult>> {
        // Use prefix scan on the session, then filter by idx
        let prefix = encode_u64(session_row_id);
        let mut sigs = Vec::new();
        for guard in self.signature_after.prefix(prefix) {
            let (key, value) = guard.into_inner()?;
            if key.len() < 12 {
                continue;
            }
            let idx = decode_u32(&key[8..12]);
            if idx < first_new_tx_idx {
                continue;
            }
            let signature = std::str::from_utf8(&value)
                .map_err(|e| {
                    FjallStorageError::DataCorruption(format!(
                        "Invalid UTF-8 in signature: {e}"
                    ))
                })?
                .to_string();
            sigs.push(SignatureResult { idx, signature });
        }
        Ok(sigs)
    }

    /// Add a signature checkpoint after a transaction.
    pub fn add_signature_after(
        &self,
        session_row_id: u64,
        idx: u32,
        signature: &str,
    ) -> Result<()> {
        let key = encode_tx_key(session_row_id, idx);
        self.signature_after.insert(key, signature.as_bytes())?;
        Ok(())
    }

    // ──────────────────────────────────────────────
    // Deletion operations
    // ──────────────────────────────────────────────

    /// Mark a CoValue as deleted (enqueue for background erasure).
    /// Idempotent.
    pub fn mark_co_value_as_deleted(&self, co_value_id: &str) -> Result<()> {
        // Only insert if not already present (preserve Done status)
        if self.deleted.get(co_value_id.as_bytes())?.is_none() {
            self.deleted
                .insert(co_value_id.as_bytes(), &[0u8])?; // 0 = Pending
        }
        Ok(())
    }

    /// Erase all data for a deleted CoValue, preserving the tombstone
    /// (header + delete sessions with sessionID ending in '$').
    /// Uses an atomic write batch.
    pub fn erase_co_value_but_keep_tombstone(&self, co_value_id: &str) -> Result<()> {
        let co_value_row_id = match self.get_co_value_row_id(co_value_id)? {
            Some(id) => id,
            None => return Ok(()), // Nothing to erase
        };

        let mut batch = self.db.batch();

        // Find all sessions for this CoValue
        let prefix = encode_u64(co_value_row_id);

        // Collect session data first (can't iterate and batch-modify simultaneously)
        let mut sessions_to_delete: Vec<(Vec<u8>, u64)> = Vec::new();
        for guard in self.session_by_cv_sid.prefix(&prefix) {
            let (key, value) = guard.into_inner()?;

            // Extract sessionID from key (after 8-byte prefix)
            let session_id = std::str::from_utf8(&key[8..]).unwrap_or("");

            // Keep tombstone sessions (sessionID ends with '$')
            if session_id.ends_with('$') {
                continue;
            }

            let session_row_id = if value.len() >= 8 {
                decode_u64(&value[..8])
            } else {
                continue;
            };

            sessions_to_delete.push((key.to_vec(), session_row_id));
        }

        // Now process deletions
        for (session_key, session_row_id) in &sessions_to_delete {
            let tx_prefix = encode_u64(*session_row_id);

            // Delete all transactions for this session
            for guard in self.transactions.prefix(&tx_prefix) {
                let (tx_key, _) = guard.into_inner()?;
                batch.remove(&self.transactions, tx_key);
            }

            // Delete all signatures for this session
            for guard in self.signature_after.prefix(&tx_prefix) {
                let (sig_key, _) = guard.into_inner()?;
                batch.remove(&self.signature_after, sig_key);
            }

            // Delete reverse session index
            batch.remove(&self.session_by_row, encode_u64(*session_row_id));

            // Delete the session itself
            batch.remove(&self.session_by_cv_sid, session_key.as_slice());
        }

        // Mark deletion as Done
        batch.insert(&self.deleted, co_value_id.as_bytes(), &[1u8]);

        batch.commit()?;
        Ok(())
    }

    /// Get all CoValue IDs waiting for deletion (status = Pending = 0).
    pub fn get_all_co_values_waiting_for_delete(&self) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        for guard in self.deleted.iter() {
            let (key, value) = guard.into_inner()?;
            // Status 0 = Pending
            if value.first() == Some(&0u8) {
                if let Ok(id) = std::str::from_utf8(&key) {
                    ids.push(id.to_string());
                }
            }
        }
        Ok(ids)
    }

    // ──────────────────────────────────────────────
    // Sync tracking operations
    // ──────────────────────────────────────────────

    /// Track sync state for CoValue/peer pairs.
    pub fn track_co_values_sync_state(
        &self,
        updates: &[(&str, &str, bool)], // (co_value_id, peer_id, synced)
    ) -> Result<()> {
        for &(co_value_id, peer_id, synced) in updates {
            let key = encode_unsynced_key(co_value_id, peer_id);
            if synced {
                self.unsynced.remove(&key)?;
            } else {
                self.unsynced.insert(&key, &[])?;
            }
        }
        Ok(())
    }

    /// Get all distinct CoValue IDs that have at least one unsynced peer.
    pub fn get_unsynced_co_value_ids(&self) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        let mut last_id: Option<String> = None;
        for guard in self.unsynced.iter() {
            let (key, _) = guard.into_inner()?;
            if let Some(id) = decode_unsynced_co_value_id(&key) {
                if last_id.as_deref() != Some(id) {
                    let owned = id.to_string();
                    last_id = Some(owned.clone());
                    ids.push(owned);
                }
            }
        }
        Ok(ids)
    }

    /// Stop tracking sync state for a CoValue (remove all peer entries).
    pub fn stop_tracking_sync_state(&self, co_value_id: &str) -> Result<()> {
        let prefix = {
            let mut p = Vec::with_capacity(co_value_id.len() + 1);
            p.extend_from_slice(co_value_id.as_bytes());
            p.push(0x00);
            p
        };
        let keys_to_remove: Vec<Vec<u8>> = self
            .unsynced
            .prefix(&prefix)
            .filter_map(|guard| guard.into_inner().ok().map(|(k, _)| k.to_vec()))
            .collect();
        for key in keys_to_remove {
            self.unsynced.remove(&key)?;
        }
        Ok(())
    }

    // ──────────────────────────────────────────────
    // Known state
    // ──────────────────────────────────────────────

    /// Get the known state for a CoValue without loading transactions.
    /// Returns None if the CoValue doesn't exist.
    pub fn get_co_value_known_state(&self, co_value_id: &str) -> Result<Option<KnownStateResult>> {
        let co_value_row_id = match self.get_co_value_row_id(co_value_id)? {
            Some(id) => id,
            None => return Ok(None),
        };

        let prefix = encode_u64(co_value_row_id);
        let mut sessions = Vec::new();
        for guard in self.session_by_cv_sid.prefix(prefix) {
            let (key, value) = guard.into_inner()?;
            if value.len() < SESSION_HEADER_SIZE {
                continue;
            }
            let session_id = std::str::from_utf8(&key[8..])
                .unwrap_or("")
                .to_string();
            let last_idx = decode_u32(&value[8..12]);
            sessions.push((session_id, last_idx));
        }

        Ok(Some(KnownStateResult {
            id: co_value_id.to_string(),
            sessions,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn open_temp() -> FjallStorage {
        let dir = tempdir().unwrap();
        FjallStorage::open(dir.path().to_str().unwrap()).unwrap()
    }

    #[test]
    fn covalue_roundtrip() {
        let s = open_temp();
        let row_id = s.upsert_co_value("co_abc", Some(r#"{"type":"comap"}"#)).unwrap();
        assert!(row_id.is_some());

        let result = s.get_co_value("co_abc").unwrap().unwrap();
        assert_eq!(result.header_json, r#"{"type":"comap"}"#);
        assert_eq!(result.row_id, row_id.unwrap());
    }

    #[test]
    fn upsert_no_header_not_found() {
        let s = open_temp();
        assert!(s.upsert_co_value("co_missing", None).unwrap().is_none());
    }

    #[test]
    fn upsert_idempotent() {
        let s = open_temp();
        let id1 = s.upsert_co_value("co_x", Some(r#"{"type":"a"}"#)).unwrap().unwrap();
        let id2 = s.upsert_co_value("co_x", Some(r#"{"type":"b"}"#)).unwrap().unwrap();
        // Should return same rowID, not overwrite
        assert_eq!(id1, id2);
        // Header should be the original
        let result = s.get_co_value("co_x").unwrap().unwrap();
        assert_eq!(result.header_json, r#"{"type":"a"}"#);
    }

    #[test]
    fn session_crud() {
        let s = open_temp();
        let cv_row = s.upsert_co_value("co_1", Some(r#"{"type":"comap"}"#)).unwrap().unwrap();

        // Add session
        let ses_row = s.add_session_update(cv_row, "session_1", 5, "sig_abc", 100).unwrap();
        assert!(ses_row > 0);

        // Get single session
        let session = s.get_single_co_value_session(cv_row, "session_1").unwrap().unwrap();
        assert_eq!(session.last_idx, 5);
        assert_eq!(session.last_signature, "sig_abc");
        assert_eq!(session.bytes_since_last_signature, 100);

        // Get all sessions
        let sessions = s.get_co_value_sessions(cv_row).unwrap();
        assert_eq!(sessions.len(), 1);

        // Add another session
        s.add_session_update(cv_row, "session_2", 3, "sig_def", 50).unwrap();
        let sessions = s.get_co_value_sessions(cv_row).unwrap();
        assert_eq!(sessions.len(), 2);

        // Update existing session
        s.add_session_update(cv_row, "session_1", 10, "sig_ghi", 200).unwrap();
        let session = s.get_single_co_value_session(cv_row, "session_1").unwrap().unwrap();
        assert_eq!(session.last_idx, 10);
        assert_eq!(session.last_signature, "sig_ghi");
        // rowID should be preserved
        assert_eq!(session.row_id, ses_row);
    }

    #[test]
    fn transaction_range_query() {
        let s = open_temp();
        let cv_row = s.upsert_co_value("co_1", Some(r#"{}"#)).unwrap().unwrap();
        let ses_row = s.add_session_update(cv_row, "s1", 0, "", 0).unwrap();

        // Insert 10 transactions
        for i in 0..10u32 {
            s.add_transaction(ses_row, i, &format!(r#"{{"idx":{i}}}"#)).unwrap();
        }

        // Query range [3, 7]
        let txs = s.get_new_transaction_in_session(ses_row, 3, 7).unwrap();
        assert_eq!(txs.len(), 5);
        assert_eq!(txs[0].idx, 3);
        assert_eq!(txs[4].idx, 7);
    }

    #[test]
    fn signature_operations() {
        let s = open_temp();
        let cv_row = s.upsert_co_value("co_1", Some(r#"{}"#)).unwrap().unwrap();
        let ses_row = s.add_session_update(cv_row, "s1", 0, "", 0).unwrap();

        s.add_signature_after(ses_row, 5, "sig_5").unwrap();
        s.add_signature_after(ses_row, 10, "sig_10").unwrap();
        s.add_signature_after(ses_row, 15, "sig_15").unwrap();

        // Query from idx 5
        let sigs = s.get_signatures(ses_row, 5).unwrap();
        assert_eq!(sigs.len(), 3);
        assert_eq!(sigs[0].idx, 5);
        assert_eq!(sigs[0].signature, "sig_5");

        // Query from idx 8
        let sigs = s.get_signatures(ses_row, 8).unwrap();
        assert_eq!(sigs.len(), 2);
        assert_eq!(sigs[0].idx, 10);
    }

    #[test]
    fn deletion_workflow() {
        let s = open_temp();
        let cv_row = s.upsert_co_value("co_del", Some(r#"{"type":"comap"}"#)).unwrap().unwrap();

        // Add a normal session and a delete session (ends with '$')
        let ses_normal = s.add_session_update(cv_row, "normal_session", 3, "sig_n", 0).unwrap();
        let _ses_delete = s.add_session_update(cv_row, "delete_session$", 1, "sig_d", 0).unwrap();

        // Add transactions to the normal session
        s.add_transaction(ses_normal, 0, r#"{"data":"normal"}"#).unwrap();
        s.add_transaction(ses_normal, 1, r#"{"data":"normal2"}"#).unwrap();

        // Mark as deleted
        s.mark_co_value_as_deleted("co_del").unwrap();

        // Should appear in waiting list
        let waiting = s.get_all_co_values_waiting_for_delete().unwrap();
        assert!(waiting.contains(&"co_del".to_string()));

        // Erase but keep tombstone
        s.erase_co_value_but_keep_tombstone("co_del").unwrap();

        // Normal session should be gone, delete session preserved
        let sessions = s.get_co_value_sessions(cv_row).unwrap();
        assert_eq!(sessions.len(), 1);
        assert!(sessions[0].session_id.ends_with('$'));

        // Normal transactions should be gone
        let txs = s.get_new_transaction_in_session(ses_normal, 0, 10).unwrap();
        assert!(txs.is_empty());

        // Should no longer be in pending list (status = Done)
        let waiting = s.get_all_co_values_waiting_for_delete().unwrap();
        assert!(!waiting.contains(&"co_del".to_string()));
    }

    #[test]
    fn mark_deleted_idempotent() {
        let s = open_temp();
        s.mark_co_value_as_deleted("co_x").unwrap();
        s.mark_co_value_as_deleted("co_x").unwrap();
        let waiting = s.get_all_co_values_waiting_for_delete().unwrap();
        assert_eq!(waiting.iter().filter(|id| *id == "co_x").count(), 1);
    }

    #[test]
    fn sync_tracking() {
        let s = open_temp();

        // Track as unsynced
        s.track_co_values_sync_state(&[
            ("co_1", "peer_a", false),
            ("co_1", "peer_b", false),
            ("co_2", "peer_a", false),
        ]).unwrap();

        let ids = s.get_unsynced_co_value_ids().unwrap();
        assert!(ids.contains(&"co_1".to_string()));
        assert!(ids.contains(&"co_2".to_string()));

        // Mark co_1/peer_a as synced
        s.track_co_values_sync_state(&[("co_1", "peer_a", true)]).unwrap();

        // co_1 should still appear (peer_b still unsynced)
        let ids = s.get_unsynced_co_value_ids().unwrap();
        assert!(ids.contains(&"co_1".to_string()));

        // Mark co_1/peer_b as synced
        s.track_co_values_sync_state(&[("co_1", "peer_b", true)]).unwrap();

        // co_1 should no longer appear
        let ids = s.get_unsynced_co_value_ids().unwrap();
        assert!(!ids.contains(&"co_1".to_string()));
        assert!(ids.contains(&"co_2".to_string()));
    }

    #[test]
    fn stop_tracking_sync_state() {
        let s = open_temp();
        s.track_co_values_sync_state(&[
            ("co_1", "peer_a", false),
            ("co_1", "peer_b", false),
        ]).unwrap();

        s.stop_tracking_sync_state("co_1").unwrap();
        let ids = s.get_unsynced_co_value_ids().unwrap();
        assert!(!ids.contains(&"co_1".to_string()));
    }

    #[test]
    fn known_state() {
        let s = open_temp();
        let cv_row = s.upsert_co_value("co_ks", Some(r#"{"type":"comap"}"#)).unwrap().unwrap();

        s.add_session_update(cv_row, "s1", 5, "sig1", 0).unwrap();
        s.add_session_update(cv_row, "s2", 10, "sig2", 0).unwrap();

        let ks = s.get_co_value_known_state("co_ks").unwrap().unwrap();
        assert_eq!(ks.id, "co_ks");
        assert_eq!(ks.sessions.len(), 2);

        let s1 = ks.sessions.iter().find(|(id, _)| id == "s1").unwrap();
        assert_eq!(s1.1, 5);
        let s2 = ks.sessions.iter().find(|(id, _)| id == "s2").unwrap();
        assert_eq!(s2.1, 10);
    }

    #[test]
    fn known_state_not_found() {
        let s = open_temp();
        assert!(s.get_co_value_known_state("co_missing").unwrap().is_none());
    }
}
