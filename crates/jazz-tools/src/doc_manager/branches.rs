use std::collections::HashMap;

use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{ReadTxn, StateVector, Transact, Update};

use crate::object::ObjectId;
use crate::row_doc::RowDoc;

use super::{DocManager, Error};

impl DocManager {
    /// Fork doc `id` into a new branch named `branch_name`.
    ///
    /// The branch doc is initialised with the parent's full state. Its
    /// `origin` field records the parent id and the parent's state vector at
    /// the time of the fork (used later to compute the diff during merge).
    /// The parent's `branches` map is updated to record the new branch id.
    pub fn fork(&mut self, id: ObjectId, branch_name: &str) -> Result<ObjectId, Error> {
        // 1. Capture the parent's full state as a v1 update.
        let (state_bytes, sv_bytes) = {
            let parent = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
            let txn = parent.doc.transact();
            let state_bytes = txn.encode_state_as_update_v1(&StateVector::default());
            let sv_bytes = txn.state_vector().encode_v1();
            (state_bytes, sv_bytes)
        };

        // 2. Build the branch RowDoc.
        let branch_id = ObjectId::new();
        let mut branch = RowDoc::new(branch_id, HashMap::new());
        branch.origin = Some((id, sv_bytes));

        // 3. Apply parent state to branch.
        {
            let decoded =
                Update::decode_v1(&state_bytes).map_err(|e| Error::YrsError(e.to_string()))?;
            let mut txn = branch.doc.transact_mut();
            txn.apply_update(decoded)
                .map_err(|e| Error::YrsError(e.to_string()))?;
        }

        // 4. Register the branch in self.docs.
        self.docs.insert(branch_id, branch);

        // 5. Register the branch name in the parent.
        let parent = self.docs.get_mut(&id).ok_or(Error::DocNotFound(id))?;
        parent.branches.insert(branch_name.to_string(), branch_id);

        Ok(branch_id)
    }

    /// Merge `source_id` into `target_id`.
    ///
    /// Encodes everything the source knows that the target doesn't, then
    /// applies it to the target. Yrs CRDT semantics handle concurrent edits
    /// to the same field deterministically (last-write wins by logical clock).
    pub fn merge(&mut self, source_id: ObjectId, target_id: ObjectId) -> Result<(), Error> {
        // 1. Get the target's current state vector.
        let target_sv = {
            let target = self
                .docs
                .get(&target_id)
                .ok_or(Error::DocNotFound(target_id))?;
            target.doc.transact().state_vector()
        };

        // 2. Encode the diff from source relative to target's SV.
        let diff = {
            let source = self
                .docs
                .get(&source_id)
                .ok_or(Error::DocNotFound(source_id))?;
            source.doc.transact().encode_diff_v1(&target_sv)
        };

        // 3. Apply the diff to the target.
        let target = self
            .docs
            .get_mut(&target_id)
            .ok_or(Error::DocNotFound(target_id))?;
        let decoded = Update::decode_v1(&diff).map_err(|e| Error::YrsError(e.to_string()))?;
        let mut txn = target.doc.transact_mut();
        txn.apply_update(decoded)
            .map_err(|e| Error::YrsError(e.to_string()))
    }

    /// Return the branches map for doc `id`.
    pub fn list_branches(&self, id: ObjectId) -> Result<&HashMap<String, ObjectId>, Error> {
        let doc = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
        Ok(&doc.branches)
    }

    /// Remove a branch by name from both the parent's registry and `self.docs`.
    pub fn delete_branch(&mut self, id: ObjectId, branch_name: &str) -> Result<(), Error> {
        let branch_id = {
            let parent = self.docs.get_mut(&id).ok_or(Error::DocNotFound(id))?;
            parent
                .branches
                .remove(branch_name)
                .ok_or_else(|| Error::BranchNotFound(branch_name.to_string()))?
        };
        self.docs.remove(&branch_id);
        Ok(())
    }
}
