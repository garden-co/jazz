use std::collections::HashMap;

use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{ReadTxn, StateVector, Transact, Update};

use crate::object::ObjectId;
use crate::row_doc::RowDoc;
use crate::storage::Storage;

// ============================================================================
// Error
// ============================================================================

#[derive(Debug)]
pub enum Error {
    DocNotFound(ObjectId),
    YrsError(String),
    BranchNotFound(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DocNotFound(id) => write!(f, "doc not found: {id}"),
            Error::YrsError(msg) => write!(f, "yrs error: {msg}"),
            Error::BranchNotFound(name) => write!(f, "branch not found: {name}"),
        }
    }
}

impl std::error::Error for Error {}

// ============================================================================
// DocManager
// ============================================================================

pub struct DocManager {
    docs: HashMap<ObjectId, RowDoc>,
    storage: Box<dyn Storage>,
}

impl DocManager {
    pub fn new(storage: Box<dyn Storage>) -> Self {
        Self {
            docs: HashMap::new(),
            storage,
        }
    }

    /// Create a new RowDoc with a fresh ObjectId and return that id.
    pub fn create(&mut self, metadata: HashMap<String, String>) -> ObjectId {
        let id = ObjectId::new();
        let row_doc = RowDoc::new(id, metadata);
        self.docs.insert(id, row_doc);
        id
    }

    /// Create a RowDoc with a specific id.
    pub fn create_with_id(&mut self, id: ObjectId, metadata: HashMap<String, String>) {
        let row_doc = RowDoc::new(id, metadata);
        self.docs.insert(id, row_doc);
    }

    pub fn get(&self, id: ObjectId) -> Option<&RowDoc> {
        self.docs.get(&id)
    }

    pub fn get_mut(&mut self, id: ObjectId) -> Option<&mut RowDoc> {
        self.docs.get_mut(&id)
    }

    /// Decode a v1 update and apply it to the doc identified by `id`.
    pub fn apply_update(&mut self, id: ObjectId, update: &[u8]) -> Result<(), Error> {
        let row_doc = self.docs.get_mut(&id).ok_or(Error::DocNotFound(id))?;
        let decoded = Update::decode_v1(update).map_err(|e| Error::YrsError(e.to_string()))?;
        let mut txn = row_doc.doc.transact_mut();
        txn.apply_update(decoded)
            .map_err(|e| Error::YrsError(e.to_string()))
    }

    /// Encode the doc's current StateVector (v1).
    pub fn get_state_vector(&self, id: ObjectId) -> Result<Vec<u8>, Error> {
        let row_doc = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
        let sv = row_doc.doc.transact().state_vector();
        Ok(sv.encode_v1())
    }

    /// Encode the full doc state as a snapshot and save it to storage.
    /// Creates doc metadata in storage if not yet persisted.
    pub fn persist(&mut self, id: ObjectId) -> Result<(), Error> {
        let row_doc = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
        let txn = row_doc.doc.transact();
        let snapshot = txn.encode_state_as_update_v1(&StateVector::default());
        let metadata = row_doc.metadata.clone();

        let existing = self
            .storage
            .load_doc_metadata(id)
            .map_err(|e| Error::YrsError(e.to_string()))?;
        if existing.is_none() {
            self.storage
                .create_doc(id, &metadata)
                .map_err(|e| Error::YrsError(e.to_string()))?;
        }
        self.storage
            .save_snapshot(id, &snapshot)
            .map_err(|e| Error::YrsError(e.to_string()))?;
        self.storage
            .clear_updates(id)
            .map_err(|e| Error::YrsError(e.to_string()))?;
        Ok(())
    }

    /// Remove the doc from the in-memory map.
    pub fn evict(&mut self, id: ObjectId) {
        self.docs.remove(&id);
    }

    /// Return the doc if already loaded, otherwise load from storage (snapshot + updates).
    pub fn get_or_load(&mut self, id: ObjectId) -> Result<&RowDoc, Error> {
        if self.docs.contains_key(&id) {
            return Ok(self.docs.get(&id).unwrap());
        }

        let metadata = self
            .storage
            .load_doc_metadata(id)
            .map_err(|e| Error::YrsError(e.to_string()))?
            .ok_or(Error::DocNotFound(id))?;
        let row_doc = RowDoc::new(id, metadata);

        if let Some(snapshot) = self
            .storage
            .load_snapshot(id)
            .map_err(|e| Error::YrsError(e.to_string()))?
        {
            let update =
                Update::decode_v1(&snapshot).map_err(|e| Error::YrsError(e.to_string()))?;
            row_doc
                .doc
                .transact_mut()
                .apply_update(update)
                .map_err(|e| Error::YrsError(e.to_string()))?;
        }

        for update_bytes in self
            .storage
            .load_updates(id)
            .map_err(|e| Error::YrsError(e.to_string()))?
        {
            let update =
                Update::decode_v1(&update_bytes).map_err(|e| Error::YrsError(e.to_string()))?;
            row_doc
                .doc
                .transact_mut()
                .apply_update(update)
                .map_err(|e| Error::YrsError(e.to_string()))?;
        }

        self.docs.insert(id, row_doc);
        Ok(self.docs.get(&id).unwrap())
    }

    /// Append an incremental update to storage (currently encodes full state).
    pub fn persist_update(&mut self, id: ObjectId) -> Result<(), Error> {
        let row_doc = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
        let txn = row_doc.doc.transact();
        let update = txn.encode_state_as_update_v1(&StateVector::default());
        self.storage
            .append_update(id, &update)
            .map_err(|e| Error::YrsError(e.to_string()))?;
        Ok(())
    }

    /// Encode the diff between the doc's current state and `remote_sv` (v1).
    pub fn encode_diff(&self, id: ObjectId, remote_sv: &[u8]) -> Result<Vec<u8>, Error> {
        let row_doc = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
        let sv = StateVector::decode_v1(remote_sv).map_err(|e| Error::YrsError(e.to_string()))?;
        let diff = row_doc.doc.transact().encode_diff_v1(&sv);
        Ok(diff)
    }
}

mod branches;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
