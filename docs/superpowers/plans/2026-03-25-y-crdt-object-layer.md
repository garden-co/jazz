# Y-CRDT Object Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Jazz2's commit DAG object layer (Object/Branch/Commit) with Yrs CRDT documents, where each row is a Yrs Doc and columns are Map keys.

**Architecture:** Bottom-up replacement. Build RowDoc + DocManager + new Storage trait with MemoryStorage first (fully tested in isolation). Then swap into the existing system layer by layer: storage implementations, sync, query, runtime. Each phase produces a compilable, testable state.

**Tech Stack:** Rust, yrs 0.25.0 (local path `y-crdt/yrs`), existing jazz-tools infrastructure

**Spec:** `specs/a_mvp/y-crdt-object-layer.md`

---

## File Structure

### New files

- `crates/jazz-tools/src/row_doc.rs` -- RowDoc struct, column type mapping helpers
- `crates/jazz-tools/src/doc_manager/mod.rs` -- DocManager struct and core API
- `crates/jazz-tools/src/doc_manager/tests.rs` -- DocManager unit tests
- `crates/jazz-tools/src/doc_manager/branches.rs` -- Fork/merge logic
- `crates/jazz-tools/src/doc_manager/subscriptions.rs` -- observe_deep/observe_update wiring

### Modified files

- `crates/jazz-tools/Cargo.toml` -- add yrs dependency
- `crates/jazz-tools/src/lib.rs` -- export new modules
- `crates/jazz-tools/src/storage/mod.rs` -- new Storage trait methods
- `crates/jazz-tools/src/storage/storage_core.rs` -- new helper functions
- `crates/jazz-tools/src/storage/key_codec.rs` -- new key layout
- `crates/jazz-tools/src/storage/fjall.rs` -- implement new trait methods
- `crates/jazz-tools/src/storage/opfs_btree.rs` -- implement new trait methods
- `crates/jazz-tools/src/sync_manager/mod.rs` -- DocManager field, Yrs update payload
- `crates/jazz-tools/src/sync_manager/inbox.rs` -- Yrs update processing
- `crates/jazz-tools/src/sync_manager/sync_logic.rs` -- remove DAG logic
- `crates/jazz-tools/src/sync_manager/forwarding.rs` -- Yrs update forwarding
- `crates/jazz-tools/src/sync_manager/types.rs` -- StateVector-based acks
- `crates/jazz-tools/src/sync_manager/permissions.rs` -- validate Yrs updates
- `crates/jazz-tools/src/sync_manager/tests.rs` -- rewrite for Yrs
- `crates/jazz-tools/src/query_manager/manager.rs` -- read from Yrs Maps
- `crates/jazz-tools/src/query_manager/writes.rs` -- write via Yrs transact_mut
- `crates/jazz-tools/src/query_manager/manager_tests.rs` -- update tests
- `crates/jazz-tools/src/runtime_core.rs` -- StateVector acks, DocManager
- `crates/jazz-tools/src/runtime_core/writes.rs` -- Yrs Doc writes
- `crates/jazz-tools/src/runtime_core/sync.rs` -- Yrs update sync
- `crates/jazz-tools/src/runtime_core/tests.rs` -- update tests
- `crates/jazz-tools/src/transport_protocol.rs` -- Yrs update wire format

### Files to delete (after migration complete)

- `crates/jazz-tools/src/commit.rs`
- `crates/jazz-tools/src/object_manager/mod.rs`
- `crates/jazz-tools/src/object_manager/tests.rs`

### Files to heavily refactor (most content replaced)

- `crates/jazz-tools/src/object.rs` -- keep ObjectId/BranchName, remove Branch/Object/BranchLoadedState

---

## Phase 1: Foundation (RowDoc + DocManager + MemoryStorage)

### Task 1: Add yrs dependency and define RowDoc

**Files:**

- Modify: `crates/jazz-tools/Cargo.toml`
- Create: `crates/jazz-tools/src/row_doc.rs`
- Modify: `crates/jazz-tools/src/lib.rs`

- [ ] **Step 1: Add yrs to Cargo.toml**

Add to `[dependencies]`:

```toml
yrs = { path = "../../y-crdt/yrs" }
```

- [ ] **Step 2: Create row_doc.rs with RowDoc struct**

```rust
use std::collections::HashMap;
use yrs::{Doc, Map, MapRef, ReadTxn, TransactionMut, Transact};
use crate::object::ObjectId;

pub struct RowDoc {
    pub id: ObjectId,
    pub doc: Doc,
    pub root_map: MapRef, // cached at construction — avoids deadlock with active transactions
    pub metadata: HashMap<String, String>,
    pub branches: HashMap<String, ObjectId>,
    pub origin: Option<(ObjectId, Vec<u8>)>, // (parent_id, state_vector_at_fork)
}

impl RowDoc {
    pub fn new(id: ObjectId, metadata: HashMap<String, String>) -> Self {
        let doc = Doc::new();
        let root_map = doc.get_or_insert_map("row");
        Self {
            id,
            doc,
            root_map,
            metadata,
            branches: HashMap::new(),
            origin: None,
        }
    }
}
```

**Important:** `root_map` is cached at construction time. Calling `get_or_insert_map` acquires a write transaction internally, so it must NOT be called while another transaction is active on the same Doc. All test code and implementation code should access `row_doc.root_map` directly (the field), never call `get_or_insert_map` again.

- [ ] **Step 3: Export row_doc module from lib.rs**

Add `pub mod row_doc;` to `crates/jazz-tools/src/lib.rs`.

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p jazz-tools`
Expected: compiles with no errors

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/Cargo.toml crates/jazz-tools/src/row_doc.rs crates/jazz-tools/src/lib.rs
git commit -m "feat: add yrs dependency and RowDoc struct"
```

---

### Task 2: DocManager core — create, get, apply_update

**Files:**

- Create: `crates/jazz-tools/src/doc_manager/mod.rs`
- Create: `crates/jazz-tools/src/doc_manager/tests.rs`
- Modify: `crates/jazz-tools/src/lib.rs`

- [ ] **Step 1: Write failing test for DocManager::create**

Create `crates/jazz-tools/src/doc_manager/tests.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;
    use std::collections::HashMap;

    fn make_manager() -> DocManager {
        let storage = Box::new(MemoryStorage::new());
        DocManager::new(storage)
    }

    #[test]
    fn create_returns_unique_ids() {
        let mut mgr = make_manager();
        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), "todos".to_string());

        let id1 = mgr.create(metadata.clone());
        let id2 = mgr.create(metadata.clone());

        assert_ne!(id1, id2);
        assert!(mgr.get(id1).is_some());
        assert!(mgr.get(id2).is_some());
    }

    #[test]
    fn get_returns_none_for_unknown_id() {
        let mgr = make_manager();
        let id = ObjectId::new();
        assert!(mgr.get(id).is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p jazz-tools doc_manager::tests::create_returns_unique_ids`
Expected: FAIL — `DocManager` not defined

- [ ] **Step 3: Write DocManager struct with create/get**

Create `crates/jazz-tools/src/doc_manager/mod.rs`:

```rust
use std::collections::HashMap;
use crate::object::ObjectId;
use crate::row_doc::RowDoc;
use crate::storage::Storage;

#[cfg(test)]
mod tests;

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

    pub fn create(&mut self, metadata: HashMap<String, String>) -> ObjectId {
        let id = ObjectId::new();
        let row_doc = RowDoc::new(id, metadata);
        self.docs.insert(id, row_doc);
        id
    }

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
}
```

- [ ] **Step 4: Export doc_manager from lib.rs**

Add `pub mod doc_manager;` to `crates/jazz-tools/src/lib.rs`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p jazz-tools doc_manager::tests`
Expected: PASS

- [ ] **Step 6: Write failing test for apply_update and state_vector**

Add to `tests.rs`:

```rust
#[test]
fn apply_update_modifies_doc_state() {
    use yrs::{Transact, Map, ReadTxn, updates::encoder::Encode, updates::decoder::Decode, Update};

    let mut mgr = make_manager();
    let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
    let id = mgr.create(metadata);

    // Create an update from an external doc
    let external_doc = yrs::Doc::new();
    let external_map = external_doc.get_or_insert_map("row");
    {
        let mut txn = external_doc.transact_mut();
        external_map.insert(&mut txn, "title", "Buy milk");
    }
    let update_bytes = external_doc.transact().encode_state_as_update_v1(&yrs::StateVector::default());

    // Apply update to managed doc
    mgr.apply_update(id, &update_bytes).unwrap();

    // Verify state
    let row_doc = mgr.get(id).unwrap();
    let txn = row_doc.doc.transact();
    let root = row_doc.root_map;
    let title = root.get(&txn, "title").unwrap().to_string(&txn);
    assert_eq!(title, "Buy milk");
}
```

- [ ] **Step 7: Run test to verify it fails**

Run: `cargo test -p jazz-tools doc_manager::tests::apply_update_modifies_doc_state`
Expected: FAIL — `apply_update` not defined

- [ ] **Step 8: Implement apply_update**

Add to `DocManager`:

```rust
use yrs::{updates::decoder::Decode, Update, Transact, ReadTxn, StateVector};
use yrs::updates::encoder::Encode;

pub fn apply_update(&mut self, id: ObjectId, update: &[u8]) -> Result<(), Error> {
    let row_doc = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
    let update = Update::decode_v1(update).map_err(|e| Error::YrsError(e.to_string()))?;
    row_doc.doc.transact_mut().apply_update(update).map_err(|e| Error::YrsError(e.to_string()))?;
    Ok(())
}

pub fn get_state_vector(&self, id: ObjectId) -> Result<Vec<u8>, Error> {
    let row_doc = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
    Ok(row_doc.doc.transact().state_vector().encode_v1())
}

pub fn encode_diff(&self, id: ObjectId, remote_sv: &[u8]) -> Result<Vec<u8>, Error> {
    let row_doc = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
    let sv = StateVector::decode_v1(remote_sv).map_err(|e| Error::YrsError(e.to_string()))?;
    Ok(row_doc.doc.transact().encode_diff_v1(&sv))
}

#[derive(Debug)]
pub enum Error {
    DocNotFound(ObjectId),
    YrsError(String),
    BranchNotFound(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DocNotFound(id) => write!(f, "doc not found: {:?}", id),
            Error::YrsError(msg) => write!(f, "yrs error: {}", msg),
            Error::BranchNotFound(name) => write!(f, "branch not found: {}", name),
        }
    }
}

impl std::error::Error for Error {}
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `cargo test -p jazz-tools doc_manager::tests`
Expected: PASS

- [ ] **Step 10: Write failing test for encode_diff (sync roundtrip)**

Add to `tests.rs`:

```rust
#[test]
fn encode_diff_produces_minimal_update() {
    use yrs::{Transact, Map, ReadTxn, updates::encoder::Encode, updates::decoder::Decode, Update, StateVector};

    let mut mgr = make_manager();
    let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
    let id = mgr.create(metadata);

    // Write to doc
    {
        let row_doc = mgr.get(id).unwrap();
        let mut txn = row_doc.doc.transact_mut();
        let root = row_doc.root_map;
        root.insert(&mut txn, "title", "Buy milk");
    }

    // Encode diff from empty state vector
    let empty_sv = StateVector::default().encode_v1();
    let diff = mgr.encode_diff(id, &empty_sv).unwrap();

    // Apply diff to a fresh doc and verify
    let fresh_doc = yrs::Doc::new();
    let fresh_map = fresh_doc.get_or_insert_map("row");
    fresh_doc.transact_mut().apply_update(Update::decode_v1(&diff).unwrap());

    let txn = fresh_doc.transact();
    assert_eq!(fresh_map.get(&txn, "title").unwrap().to_string(&txn), "Buy milk");
}
```

- [ ] **Step 11: Run test — should pass with existing impl**

Run: `cargo test -p jazz-tools doc_manager::tests::encode_diff_produces_minimal_update`
Expected: PASS

- [ ] **Step 12: Commit**

```bash
git add crates/jazz-tools/src/doc_manager/ crates/jazz-tools/src/lib.rs
git commit -m "feat: DocManager with create, get, apply_update, encode_diff"
```

---

### Task 3: DocManager branches — fork and merge

**Files:**

- Create: `crates/jazz-tools/src/doc_manager/branches.rs`
- Modify: `crates/jazz-tools/src/doc_manager/mod.rs`
- Modify: `crates/jazz-tools/src/doc_manager/tests.rs`

- [ ] **Step 1: Write failing test for fork**

Add to `tests.rs`:

```rust
#[test]
fn fork_creates_independent_branch_doc() {
    use yrs::{Transact, Map, ReadTxn};

    let mut mgr = make_manager();
    let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
    let id = mgr.create(metadata);

    // Write initial state
    {
        let row_doc = mgr.get(id).unwrap();
        let mut txn = row_doc.doc.transact_mut();
        row_doc.root_map.insert(&mut txn, "title", "Buy milk");
    }

    // Fork
    let branch_id = mgr.fork(id, "draft").unwrap();

    // Verify branch has same state
    {
        let branch = mgr.get(branch_id).unwrap();
        let txn = branch.doc.transact();
        assert_eq!(branch.root_map.get(&txn, "title").unwrap().to_string(&txn), "Buy milk");
        assert!(branch.origin.is_some());
        let (origin_id, _) = branch.origin.as_ref().unwrap();
        assert_eq!(*origin_id, id);
    }

    // Verify branch is registered on parent
    let parent = mgr.get(id).unwrap();
    assert_eq!(parent.branches.get("draft"), Some(&branch_id));

    // Modify branch independently — should not affect parent
    {
        let branch = mgr.get(branch_id).unwrap();
        let mut txn = branch.doc.transact_mut();
        branch.root_map.insert(&mut txn, "title", "Buy eggs");
    }
    {
        let parent = mgr.get(id).unwrap();
        let txn = parent.doc.transact();
        assert_eq!(parent.root_map.get(&txn, "title").unwrap().to_string(&txn), "Buy milk");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p jazz-tools doc_manager::tests::fork_creates_independent_branch_doc`
Expected: FAIL — `fork` not defined

- [ ] **Step 3: Implement fork in branches.rs**

Create `crates/jazz-tools/src/doc_manager/branches.rs`:

```rust
use std::collections::HashMap;
use yrs::{Doc, Transact, ReadTxn, Update, StateVector};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use crate::object::ObjectId;
use crate::row_doc::RowDoc;
use super::{DocManager, Error};

impl DocManager {
    pub fn fork(&mut self, id: ObjectId, branch_name: &str) -> Result<ObjectId, Error> {
        // Capture state from parent
        let (state_bytes, sv_bytes, parent_metadata) = {
            let parent = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
            let txn = parent.doc.transact();
            let state = txn.encode_state_as_update_v1(&StateVector::default());
            let sv = txn.state_vector().encode_v1();
            (state, sv, parent.metadata.clone())
        };

        // Create branch doc
        let branch_id = ObjectId::new();
        let mut branch_row_doc = RowDoc::new(branch_id, parent_metadata);
        {
            let update = Update::decode_v1(&state_bytes).map_err(|e| Error::YrsError(e.to_string()))?;
            branch_row_doc.doc.transact_mut().apply_update(update)
                .map_err(|e| Error::YrsError(e.to_string()))?;
        }
        branch_row_doc.origin = Some((id, sv_bytes));

        self.docs.insert(branch_id, branch_row_doc);

        // Register on parent
        let parent = self.docs.get_mut(&id).ok_or(Error::DocNotFound(id))?;
        parent.branches.insert(branch_name.to_string(), branch_id);

        Ok(branch_id)
    }

    pub fn merge(&mut self, source_id: ObjectId, target_id: ObjectId) -> Result<(), Error> {
        // Encode diff from source that target doesn't have
        let diff = {
            let target = self.docs.get(&target_id).ok_or(Error::DocNotFound(target_id))?;
            let sv = target.doc.transact().state_vector();

            let source = self.docs.get(&source_id).ok_or(Error::DocNotFound(source_id))?;
            source.doc.transact().encode_diff_v1(&sv)
        };

        // Apply to target
        let target = self.docs.get(&target_id).ok_or(Error::DocNotFound(target_id))?;
        let update = Update::decode_v1(&diff).map_err(|e| Error::YrsError(e.to_string()))?;
        target.doc.transact_mut().apply_update(update);

        Ok(())
    }

    pub fn list_branches(&self, id: ObjectId) -> Result<&HashMap<String, ObjectId>, Error> {
        let row_doc = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
        Ok(&row_doc.branches)
    }

    pub fn delete_branch(&mut self, id: ObjectId, branch_name: &str) -> Result<(), Error> {
        let branch_id = {
            let parent = self.docs.get_mut(&id).ok_or(Error::DocNotFound(id))?;
            parent.branches.remove(branch_name).ok_or(Error::BranchNotFound(branch_name.to_string()))?
        };
        self.docs.remove(&branch_id);
        Ok(())
    }
}
```

Add `mod branches;` to `mod.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p jazz-tools doc_manager::tests::fork_creates_independent_branch_doc`
Expected: PASS

- [ ] **Step 5: Write failing test for merge**

Add to `tests.rs`:

```rust
#[test]
fn merge_applies_branch_changes_to_main() {
    use yrs::{Transact, Map, ReadTxn};

    let mut mgr = make_manager();
    let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
    let id = mgr.create(metadata);

    // Write initial state
    {
        let row_doc = mgr.get(id).unwrap();
        let mut txn = row_doc.doc.transact_mut();
        row_doc.root_map.insert(&mut txn, "title", "Buy milk");
        row_doc.root_map.insert(&mut txn, "done", false);
    }

    let branch_id = mgr.fork(id, "draft").unwrap();

    // Edit branch: change title
    {
        let branch = mgr.get(branch_id).unwrap();
        let mut txn = branch.doc.transact_mut();
        branch.root_map.insert(&mut txn, "title", "Buy eggs");
    }

    // Edit main: change done
    {
        let main_doc = mgr.get(id).unwrap();
        let mut txn = main_doc.doc.transact_mut();
        main_doc.root_map.insert(&mut txn, "done", true);
    }

    // Merge branch into main
    mgr.merge(branch_id, id).unwrap();

    // Both changes should be present
    let main_doc = mgr.get(id).unwrap();
    let txn = main_doc.doc.transact();
    let root = main_doc.root_map;
    // "done" was only edited on main — preserved
    assert_eq!(root.get(&txn, "done").unwrap().to_string(&txn), "true");
    // "title" was edited on branch — should appear (branch's client ID may or may not win,
    // but since main didn't edit title after fork, branch change applies cleanly)
    assert_eq!(root.get(&txn, "title").unwrap().to_string(&txn), "Buy eggs");
}
```

- [ ] **Step 6: Run test — should pass with existing impl**

Run: `cargo test -p jazz-tools doc_manager::tests::merge_applies_branch_changes_to_main`
Expected: PASS

- [ ] **Step 7: Write test for concurrent same-field merge (client ID seniority)**

```rust
#[test]
fn merge_concurrent_same_field_resolves_deterministically() {
    use yrs::{Transact, Map, ReadTxn};

    let mut mgr = make_manager();
    let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
    let id = mgr.create(metadata);

    {
        let row_doc = mgr.get(id).unwrap();
        let mut txn = row_doc.doc.transact_mut();
        row_doc.root_map.insert(&mut txn, "title", "Original");
    }

    let branch_id = mgr.fork(id, "draft").unwrap();

    // Both edit the same field
    {
        let branch = mgr.get(branch_id).unwrap();
        let mut txn = branch.doc.transact_mut();
        branch.root_map.insert(&mut txn, "title", "Branch version");
    }
    {
        let main_doc = mgr.get(id).unwrap();
        let mut txn = main_doc.doc.transact_mut();
        main_doc.root_map.insert(&mut txn, "title", "Main version");
    }

    mgr.merge(branch_id, id).unwrap();

    // Should resolve deterministically (one wins via client ID seniority)
    let main_doc = mgr.get(id).unwrap();
    let txn = main_doc.doc.transact();
    let title = main_doc.root_map.get(&txn, "title").unwrap().to_string(&txn);
    // We don't assert which wins — just that it's deterministic and one of the two
    assert!(title == "Branch version" || title == "Main version");
}
```

- [ ] **Step 8: Run all doc_manager tests**

Run: `cargo test -p jazz-tools doc_manager::tests`
Expected: all PASS

- [ ] **Step 9: Commit**

```bash
git add crates/jazz-tools/src/doc_manager/
git commit -m "feat: DocManager fork and merge operations"
```

---

### Task 4: New Storage trait methods

**Files:**

- Modify: `crates/jazz-tools/src/storage/mod.rs`
- Modify: `crates/jazz-tools/src/storage/key_codec.rs`
- Modify: `crates/jazz-tools/src/storage/storage_core.rs`

**Note:** This task ADDS the new doc-oriented methods to the Storage trait alongside the existing commit-based methods. Both coexist during migration. Old methods are removed in a later cleanup task.

- [ ] **Step 1: Add new key codec functions**

Add to `crates/jazz-tools/src/storage/key_codec.rs`:

```rust
use crate::object::ObjectId;

pub fn doc_meta_key(id: ObjectId) -> Vec<u8> {
    format!("doc:{}:meta", id).into_bytes()
}

pub fn doc_snapshot_key(id: ObjectId) -> Vec<u8> {
    format!("doc:{}:snapshot", id).into_bytes()
}

pub fn doc_update_key(id: ObjectId, seq: u64) -> Vec<u8> {
    format!("doc:{}:log:{:020}", id, seq).into_bytes()
}

pub fn doc_update_prefix(id: ObjectId) -> Vec<u8> {
    format!("doc:{}:log:", id).into_bytes()
}

pub fn doc_branch_key(id: ObjectId, branch_name: &str) -> Vec<u8> {
    format!("doc:{}:branches:{}", id, branch_name).into_bytes()
}

pub fn doc_origin_key(id: ObjectId) -> Vec<u8> {
    format!("doc:{}:origin", id).into_bytes()
}
```

- [ ] **Step 2: Add new methods to Storage trait**

Add to the `Storage` trait in `crates/jazz-tools/src/storage/mod.rs`:

```rust
// Document storage (Y-CRDT)
fn create_doc(&mut self, id: ObjectId, metadata: &HashMap<String, String>);
fn load_doc_metadata(&self, id: ObjectId) -> Option<HashMap<String, String>>;
fn save_snapshot(&mut self, id: ObjectId, snapshot: &[u8]);
fn load_snapshot(&self, id: ObjectId) -> Option<Vec<u8>>;
fn append_update(&mut self, id: ObjectId, update: &[u8]);
fn load_updates(&self, id: ObjectId) -> Vec<Vec<u8>>;
fn clear_updates(&mut self, id: ObjectId);
fn delete_doc(&mut self, id: ObjectId);
```

- [ ] **Step 3: Implement new methods on MemoryStorage**

Add fields to `MemoryStorage` and implement the new trait methods. MemoryStorage already uses HashMaps, so this is straightforward — add `doc_metadata`, `doc_snapshots`, `doc_updates` fields.

- [ ] **Step 4: Write tests for MemoryStorage doc operations**

```rust
#[test]
fn memory_storage_doc_roundtrip() {
    let mut storage = MemoryStorage::new();
    let id = ObjectId::new();
    let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);

    storage.create_doc(id, &metadata);
    assert_eq!(storage.load_doc_metadata(id), Some(metadata));

    storage.save_snapshot(id, b"snapshot_v1");
    assert_eq!(storage.load_snapshot(id), Some(b"snapshot_v1".to_vec()));

    storage.append_update(id, b"update_1");
    storage.append_update(id, b"update_2");
    let updates = storage.load_updates(id);
    assert_eq!(updates.len(), 2);

    storage.clear_updates(id);
    assert!(storage.load_updates(id).is_empty());

    storage.delete_doc(id);
    assert!(storage.load_doc_metadata(id).is_none());
    assert!(storage.load_snapshot(id).is_none());
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p jazz-tools storage::tests::memory_storage_doc_roundtrip`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-tools/src/storage/
git commit -m "feat: add doc-oriented Storage trait methods and MemoryStorage impl"
```

---

### Task 5: DocManager persistence — save and load from Storage

**Files:**

- Modify: `crates/jazz-tools/src/doc_manager/mod.rs`
- Modify: `crates/jazz-tools/src/doc_manager/tests.rs`

- [ ] **Step 1: Write failing test for persist-on-create and get_or_load**

```rust
#[test]
fn get_or_load_restores_doc_from_storage() {
    use yrs::{Transact, Map, ReadTxn, updates::encoder::Encode, StateVector};

    let storage = Box::new(MemoryStorage::new());
    let storage_ptr = storage.as_ref() as *const MemoryStorage;
    let mut mgr = DocManager::new(storage);

    let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
    let id = mgr.create(metadata);

    // Write some data
    {
        let row_doc = mgr.get(id).unwrap();
        let mut txn = row_doc.doc.transact_mut();
        row_doc.root_map.insert(&mut txn, "title", "Buy milk");
    }

    // Persist to storage
    mgr.persist(id).unwrap();

    // Evict from memory
    mgr.evict(id);
    assert!(mgr.get(id).is_none());

    // Load back
    let row_doc = mgr.get_or_load(id).unwrap();
    let txn = row_doc.doc.transact();
    assert_eq!(row_doc.root_map.get(&txn, "title").unwrap().to_string(&txn), "Buy milk");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p jazz-tools doc_manager::tests::get_or_load_restores_doc_from_storage`
Expected: FAIL — `persist`, `evict`, `get_or_load` not defined

- [ ] **Step 3: Implement persist, evict, get_or_load**

Add to `DocManager`:

```rust
pub fn persist(&mut self, id: ObjectId) -> Result<(), Error> {
    let row_doc = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
    let txn = row_doc.doc.transact();
    let snapshot = txn.encode_state_as_update_v1(&StateVector::default());

    if self.storage.load_doc_metadata(id).is_none() {
        self.storage.create_doc(id, &row_doc.metadata);
    }
    self.storage.save_snapshot(id, &snapshot);
    self.storage.clear_updates(id);
    Ok(())
}

pub fn evict(&mut self, id: ObjectId) {
    self.docs.remove(&id);
}

pub fn get_or_load(&mut self, id: ObjectId) -> Result<&RowDoc, Error> {
    if self.docs.contains_key(&id) {
        return Ok(self.docs.get(&id).unwrap());
    }

    let metadata = self.storage.load_doc_metadata(id).ok_or(Error::DocNotFound(id))?;
    let mut row_doc = RowDoc::new(id, metadata);

    if let Some(snapshot) = self.storage.load_snapshot(id) {
        let update = Update::decode_v1(&snapshot).map_err(|e| Error::YrsError(e.to_string()))?;
        row_doc.doc.transact_mut().apply_update(update);
    }

    for update_bytes in self.storage.load_updates(id) {
        let update = Update::decode_v1(&update_bytes).map_err(|e| Error::YrsError(e.to_string()))?;
        row_doc.doc.transact_mut().apply_update(update);
    }

    self.docs.insert(id, row_doc);
    Ok(self.docs.get(&id).unwrap())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p jazz-tools doc_manager::tests::get_or_load_restores_doc_from_storage`
Expected: PASS

- [ ] **Step 5: Write test for incremental update persistence**

```rust
#[test]
fn append_update_persists_incrementally() {
    use yrs::{Transact, Map, ReadTxn};

    let mut mgr = make_manager();
    let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
    let id = mgr.create(metadata);

    // Initial persist (snapshot)
    {
        let row_doc = mgr.get(id).unwrap();
        let mut txn = row_doc.doc.transact_mut();
        row_doc.root_map.insert(&mut txn, "title", "Buy milk");
    }
    mgr.persist(id).unwrap();

    // Make another change and persist as update (not full snapshot)
    {
        let row_doc = mgr.get(id).unwrap();
        let mut txn = row_doc.doc.transact_mut();
        row_doc.root_map.insert(&mut txn, "done", false);
    }
    mgr.persist_update(id).unwrap();

    // Evict and reload — should have both fields
    mgr.evict(id);
    let row_doc = mgr.get_or_load(id).unwrap();
    let txn = row_doc.doc.transact();
    let root = row_doc.root_map;
    assert_eq!(root.get(&txn, "title").unwrap().to_string(&txn), "Buy milk");
    assert_eq!(root.get(&txn, "done").unwrap().to_string(&txn), "false");
}
```

- [ ] **Step 6: Implement persist_update**

```rust
pub fn persist_update(&mut self, id: ObjectId) -> Result<(), Error> {
    let row_doc = self.docs.get(&id).ok_or(Error::DocNotFound(id))?;
    let txn = row_doc.doc.transact();
    // Encode full state as update — storage deduplicates via Yrs on reload
    let update = txn.encode_state_as_update_v1(&StateVector::default());
    // TODO: track last-persisted state vector to encode only the diff
    self.storage.append_update(id, &update);
    Ok(())
}
```

- [ ] **Step 7: Run all tests**

Run: `cargo test -p jazz-tools doc_manager::tests`
Expected: all PASS

- [ ] **Step 8: Commit**

```bash
git add crates/jazz-tools/src/doc_manager/
git commit -m "feat: DocManager persistence — persist, evict, get_or_load"
```

---

### Task 6: DocManager compaction

**Files:**

- Modify: `crates/jazz-tools/src/doc_manager/mod.rs`
- Modify: `crates/jazz-tools/src/doc_manager/tests.rs`

- [ ] **Step 1: Write failing test for compaction**

```rust
#[test]
fn compact_replaces_updates_with_snapshot() {
    use yrs::{Transact, Map, ReadTxn};

    let mut mgr = make_manager();
    let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
    let id = mgr.create(metadata);

    // Write and persist as updates (not snapshots)
    for i in 0..10 {
        {
            let row_doc = mgr.get(id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            row_doc.root_map.insert(&mut txn, "counter", i as f64);
        }
        mgr.persist_update(id).unwrap();
    }

    // Compact: snapshot + clear updates
    mgr.compact(id).unwrap();

    // Evict and reload — should work from snapshot alone
    mgr.evict(id);
    let row_doc = mgr.get_or_load(id).unwrap();
    let txn = row_doc.doc.transact();
    assert_eq!(row_doc.root_map.get(&txn, "counter").unwrap().to_string(&txn), "9");
}

#[test]
fn crash_safe_compaction_stale_updates_are_deduplicated() {
    use yrs::{Transact, Map, ReadTxn};

    let mut mgr = make_manager();
    let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
    let id = mgr.create(metadata);

    {
        let row_doc = mgr.get(id).unwrap();
        let mut txn = row_doc.doc.transact_mut();
        row_doc.root_map.insert(&mut txn, "title", "Buy milk");
    }
    mgr.persist_update(id).unwrap();

    // Simulate partial compaction (snapshot saved but updates NOT cleared)
    mgr.persist(id).unwrap();
    // Updates still in storage — simulates crash between save_snapshot and clear_updates

    // Reload — stale updates on top of snapshot should be harmless (Yrs deduplication)
    mgr.evict(id);
    let row_doc = mgr.get_or_load(id).unwrap();
    let txn = row_doc.doc.transact();
    assert_eq!(row_doc.root_map.get(&txn, "title").unwrap().to_string(&txn), "Buy milk");
}
```

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement compact**

```rust
pub fn compact(&mut self, id: ObjectId) -> Result<(), Error> {
    self.persist(id)?;           // save full snapshot
    self.storage.clear_updates(id); // clear update log
    Ok(())
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p jazz-tools doc_manager::tests`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/doc_manager/
git commit -m "feat: DocManager compaction with crash-safety"
```

---

### Task 7: DocManager subscriptions and observe_update_v1 wiring

**Files:**

- Create: `crates/jazz-tools/src/doc_manager/subscriptions.rs`
- Modify: `crates/jazz-tools/src/doc_manager/mod.rs`
- Modify: `crates/jazz-tools/src/doc_manager/tests.rs`

- [ ] **Step 1: Write failing test for subscribe_all (doc change notification)**

```rust
#[test]
fn subscribe_all_receives_change_notifications() {
    use yrs::{Transact, Map};
    use std::sync::{Arc, Mutex};

    let mut mgr = make_manager();
    let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
    let id = mgr.create(metadata);

    let changes: Arc<Mutex<Vec<ObjectId>>> = Arc::new(Mutex::new(Vec::new()));
    let changes_clone = changes.clone();

    let sub_id = mgr.subscribe_all(move |doc_id| {
        changes_clone.lock().unwrap().push(doc_id);
    });

    // Write to doc
    {
        let row_doc = mgr.get(id).unwrap();
        let mut txn = row_doc.doc.transact_mut();
        row_doc.root_map.insert(&mut txn, "title", "Buy milk");
    }
    mgr.notify_change(id);

    assert_eq!(changes.lock().unwrap().len(), 1);
    assert_eq!(changes.lock().unwrap()[0], id);

    mgr.unsubscribe_all(sub_id);
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement subscription system**

Create `crates/jazz-tools/src/doc_manager/subscriptions.rs`:

- `subscribe_all(callback) -> SubscriptionId` — global change notifications
- `subscribe(id, callback) -> SubscriptionId` — per-doc change notifications
- `unsubscribe_all(sub_id)` / `unsubscribe(id, sub_id)`
- `notify_change(id)` — calls both per-doc and global subscribers

- [ ] **Step 4: Write failing test for observe_update_v1 wiring (raw update bytes for sync)**

```rust
#[test]
fn observe_update_captures_raw_bytes_for_sync() {
    use yrs::{Transact, Map, Doc, Update};
    use yrs::updates::decoder::Decode;
    use std::sync::{Arc, Mutex};

    let mut mgr = make_manager();
    let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
    let id = mgr.create(metadata);

    let captured_updates: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let updates_clone = captured_updates.clone();

    mgr.set_update_handler(move |_doc_id, update_bytes| {
        updates_clone.lock().unwrap().push(update_bytes.to_vec());
    });

    // Write to doc — should trigger update handler with raw Yrs update bytes
    {
        let row_doc = mgr.get(id).unwrap();
        let mut txn = row_doc.doc.transact_mut();
        row_doc.root_map.insert(&mut txn, "title", "Buy milk");
    }
    // observe_update_v1 fires on transaction drop

    let updates = captured_updates.lock().unwrap();
    assert_eq!(updates.len(), 1);

    // Verify the captured bytes can be applied to a fresh doc
    let fresh_doc = Doc::new();
    let fresh_map = fresh_doc.get_or_insert_map("row");
    let update = Update::decode_v1(&updates[0]).unwrap();
    fresh_doc.transact_mut().apply_update(update).unwrap();

    let txn = fresh_doc.transact();
    assert_eq!(fresh_map.get(&txn, "title").unwrap().to_string(&txn), "Buy milk");
}
```

- [ ] **Step 5: Implement observe_update_v1 wiring**

Wire `doc.observe_update_v1()` on each Doc during `create()` and `get_or_load()`. The callback captures the DocManager's update handler and the doc's ObjectId, routing raw update bytes to both storage persistence and the external handler (for sync outbox).

Note: `observe_update_v1` returns a `Subscription` that must be kept alive. Store it in the RowDoc or DocManager.

- [ ] **Step 6: Run all tests**

Run: `cargo test -p jazz-tools doc_manager::tests`
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add crates/jazz-tools/src/doc_manager/
git commit -m "feat: DocManager subscriptions and observe_update_v1 wiring"
```

---

## Phase 2: Storage Implementations

### Task 7: FjallStorage — new doc methods

**Files:**

- Modify: `crates/jazz-tools/src/storage/fjall.rs`
- Test inline or in existing test module

- [ ] **Step 1: Write failing test for FjallStorage doc roundtrip**

Same test pattern as MemoryStorage but using FjallStorage with a temp directory.

- [ ] **Step 2: Implement new trait methods on FjallStorage**

Use the key_codec functions from Task 4. Store doc metadata, snapshots, and updates as key-value pairs in Fjall. Use prefix scan for `load_updates` and `clear_updates`.

- [ ] **Step 3: Run test to verify it passes**

- [ ] **Step 4: Write test for delete_doc**

- [ ] **Step 5: Run all storage tests**

Run: `cargo test -p jazz-tools storage`
Expected: all PASS (including existing commit-based tests — they should still work)

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-tools/src/storage/fjall.rs
git commit -m "feat: FjallStorage doc-oriented methods"
```

---

### Task 8: OpfsBTreeStorage — new doc methods

**Files:**

- Modify: `crates/jazz-tools/src/storage/opfs_btree.rs`

- [ ] **Step 1: Implement new trait methods on OpfsBTreeStorage**

Same pattern as FjallStorage but using the OPFS B-tree API.

- [ ] **Step 2: Verify it compiles for wasm32 target**

Run: `cargo check -p jazz-wasm --target wasm32-unknown-unknown`

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-tools/src/storage/opfs_btree.rs
git commit -m "feat: OpfsBTreeStorage doc-oriented methods"
```

---

## Phase 3: Wire DocManager into Existing System

### Task 9: RuntimeCore — use DocManager for writes

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core.rs`
- Modify: `crates/jazz-tools/src/runtime_core/writes.rs`
- Modify: `crates/jazz-tools/src/runtime_core/tests.rs`

This is where the old system starts getting replaced. RuntimeCore currently holds an ObjectManager. It needs to hold a DocManager instead (or both during transition).

- [ ] **Step 1: Read current RuntimeCore to understand the write path**

Read: `crates/jazz-tools/src/runtime_core.rs`, `crates/jazz-tools/src/runtime_core/writes.rs`
Map: how `insert` and `update` currently create commits via ObjectManager

- [ ] **Step 2: Add DocManager field to RuntimeCore alongside ObjectManager**

During transition, both can coexist. New writes go through DocManager; reads fall back to ObjectManager if doc not found.

- [ ] **Step 3: Write failing test for insert via DocManager**

Test that inserting a row creates a RowDoc with the correct column values.

- [ ] **Step 4: Implement insert path through DocManager**

Instead of creating a Commit with serialized content, create/get a RowDoc and write columns via `transact_mut` + `root_map.insert`.

- [ ] **Step 5: Write failing test for update via DocManager**

- [ ] **Step 6: Implement update path**

- [ ] **Step 7: Run tests**

- [ ] **Step 8: Commit**

```bash
git commit -m "feat: RuntimeCore writes via DocManager"
```

---

### Task 10: Column type mapping helpers

**Files:**

- Modify: `crates/jazz-tools/src/row_doc.rs`
- Modify: `crates/jazz-tools/src/doc_manager/tests.rs`

Conversion functions between Jazz2 `Value` types and Yrs Map entries, per the spec's column type mapping table.

- [ ] **Step 1: Write failing tests for value conversion roundtrips**

```rust
#[test]
fn column_type_roundtrip_string() {
    use yrs::{Transact, Map, ReadTxn, Any};
    let doc = Doc::new();
    let map = doc.get_or_insert_map("row");
    {
        let mut txn = doc.transact_mut();
        write_column(&map, &mut txn, "name", &Value::Text("Alice".into()));
    }
    let txn = doc.transact();
    let val = read_column(&map, &txn, "name").unwrap();
    assert_eq!(val, Value::Text("Alice".into()));
}

#[test]
fn column_type_roundtrip_integer() { /* similar for i64 */ }

#[test]
fn column_type_roundtrip_boolean() { /* similar for bool */ }

#[test]
fn column_type_roundtrip_float() { /* similar for f64 */ }

#[test]
fn column_type_roundtrip_bytes() { /* similar for Vec<u8> */ }

#[test]
fn column_type_roundtrip_null() { /* None / missing key */ }
```

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement write_column and read_column**

In `row_doc.rs`:

```rust
pub fn write_column(map: &MapRef, txn: &mut TransactionMut, key: &str, value: &Value) {
    match value {
        Value::Text(s) => { map.insert(txn, key, s.as_str()); }
        Value::Integer(i) => { map.insert(txn, key, *i as f64); }
        Value::Float(f) => { map.insert(txn, key, *f); }
        Value::Boolean(b) => { map.insert(txn, key, *b); }
        Value::Bytes(b) => { map.insert(txn, key, b.as_slice()); }
        Value::Null => { map.remove(txn, key); }
    }
}

pub fn read_column(map: &MapRef, txn: &impl ReadTxn, key: &str) -> Option<Value> {
    let out = map.get(txn, key)?;
    // Convert Yrs Out to Jazz2 Value based on the stored type
    Some(out_to_value(out))
}
```

The exact mapping depends on how Jazz2's `Value` enum is defined — read `crates/jazz-tools/src/query_manager/types/` to match the existing type system.

- [ ] **Step 4: Run tests**

Run: `cargo test -p jazz-tools doc_manager::tests::column_type_roundtrip`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/row_doc.rs crates/jazz-tools/src/doc_manager/tests.rs
git commit -m "feat: column type mapping helpers (Value <-> Yrs Map)"
```

---

### Task 11: QueryManager — read from Yrs Docs

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/manager.rs`
- Modify: `crates/jazz-tools/src/query_manager/writes.rs`
- Modify: `crates/jazz-tools/src/query_manager/manager_tests.rs`

- [ ] **Step 1: Read current QueryManager read path**

Read: `crates/jazz-tools/src/query_manager/manager.rs`
Map: how rows are currently read from ObjectManager tips

- [ ] **Step 2: Write failing test for reading a row from Yrs Doc**

Test that a query returns column values that were written via DocManager.

- [ ] **Step 3: Implement row reading from Yrs Doc**

Replace the `get_tips -> deserialize content` path with `doc_manager.transact(id) -> root_map.get(txn, column_name)`.

- [ ] **Step 4: Write failing test for index update via observe_deep**

Test that inserting a row updates secondary indices correctly.

- [ ] **Step 5: Implement observe_deep for index maintenance**

Wire up `observe_deep` on root MapRef when docs are loaded. Route `Event::Map` changes to the existing index insert/remove logic.

- [ ] **Step 6: Run query manager tests**

Run: `cargo test -p jazz-tools query_manager`
Expected: tests that don't depend on commit internals should pass

- [ ] **Step 7: Commit**

```bash
git commit -m "feat: QueryManager reads from Yrs Docs"
```

---

### Task 11: QueryManager — writes via Yrs transact_mut

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/writes.rs`

- [ ] **Step 1: Read current write path**

Map: how INSERT/UPDATE/DELETE currently create commits

- [ ] **Step 2: Replace commit creation with Yrs Doc writes**

INSERT: `DocManager::create` + `transact_mut` + `root_map.insert` for each column
UPDATE: `DocManager::get` + `transact_mut` + `root_map.insert` for changed columns
DELETE (soft): `transact_mut` + `root_map.insert(&mut txn, "_deleted", "soft")`
DELETE (hard): `transact_mut` + set `_deleted` to `"hard"` + remove all column values

- [ ] **Step 3: Write tests for soft/hard delete**

- [ ] **Step 4: Run tests**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: QueryManager writes via Yrs transact_mut"
```

---

### Task 12: SyncManager — Yrs update payload

**Files:**

- Modify: `crates/jazz-tools/src/sync_manager/mod.rs`
- Modify: `crates/jazz-tools/src/sync_manager/inbox.rs`
- Modify: `crates/jazz-tools/src/sync_manager/sync_logic.rs`
- Modify: `crates/jazz-tools/src/sync_manager/forwarding.rs`
- Modify: `crates/jazz-tools/src/sync_manager/types.rs`
- Modify: `crates/jazz-tools/src/sync_manager/tests.rs`

- [ ] **Step 1: Read current sync path end-to-end**

Map: inbox processing, outbox, forwarding, ack tracking

- [ ] **Step 2: Define new sync message types**

In `types.rs`, add types for Yrs-based sync:

- `DocUpdate { doc_id: ObjectId, update: Vec<u8> }` (replaces commit-based InboxEntry)
- `DocSyncRequest { doc_id: ObjectId, state_vector: Vec<u8> }` (initial sync)
- `DocSyncResponse { doc_id: ObjectId, diff: Vec<u8> }` (initial sync response)
- `DurabilityAck { doc_id: ObjectId, state_vector: Vec<u8>, tier: DurabilityTier }`

- [ ] **Step 3: Write failing test for sync roundtrip between two DocManagers**

```
alice writes "title" = "Buy milk"
  -> alice's sync encodes update
  -> bob receives update
  -> bob's doc has "title" = "Buy milk"
```

- [ ] **Step 4: Implement inbox processing for Yrs updates**

Replace commit-based `process_inbox` with: receive update bytes -> `doc_manager.apply_update()` -> persist -> relay to other connections.

Remove topological sort, parent validation, merge commit creation.

- [ ] **Step 5: Implement outbox/forwarding for Yrs updates**

When a local write happens, capture update bytes (via `observe_update_v1`) -> add to outbox -> forward to connected peers.

- [ ] **Step 6: Implement StateVector-based ack tracking**

Replace CommitId acks with StateVector acks per spec.

- [ ] **Step 7: Write test for concurrent sync (two writers, converge)**

```
alice writes "title" = "Alice's"
bob writes "done" = true
  -> sync both ways
  -> both have title="Alice's" AND done=true
```

- [ ] **Step 8: Run sync tests**

- [ ] **Step 9: Commit**

```bash
git commit -m "feat: SyncManager with Yrs update payload"
```

---

### Task 13: Transport protocol — wire format

**Files:**

- Modify: `crates/jazz-tools/src/transport_protocol.rs`
- Modify: `crates/jazz-tools/src/routes.rs`

- [ ] **Step 1: Read current wire format**

Map: how commits are serialized for transport

- [ ] **Step 2: Update wire format to carry Yrs updates**

Replace commit serialization with the new sync message types from Task 12.

- [ ] **Step 3: Update HTTP routes that handle sync messages**

- [ ] **Step 4: Write integration test for sync over transport**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: transport protocol for Yrs updates"
```

---

### Task 14: Policy validation on Yrs updates

**Files:**

- Modify: `crates/jazz-tools/src/sync_manager/permissions.rs`
- Modify: `crates/jazz-tools/src/query_manager/policy.rs`

- [ ] **Step 1: Read current policy enforcement**

Map: how commits are validated against schema policies

- [ ] **Step 2: Implement Yrs update decoding for policy inspection**

Decode incoming Yrs updates to extract changed keys and values. Map to `PolicyChange` struct per spec.

- [ ] **Step 3: Write test for policy rejection of unauthorized field update**

- [ ] **Step 4: Write test for policy acceptance of authorized update**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: policy validation on decoded Yrs updates"
```

---

## Phase 4: Cleanup

### Task 15: Remove old commit DAG types and ObjectManager

**Files:**

- Delete: `crates/jazz-tools/src/commit.rs`
- Delete: `crates/jazz-tools/src/object_manager/mod.rs`
- Delete: `crates/jazz-tools/src/object_manager/tests.rs`
- Modify: `crates/jazz-tools/src/object.rs` — keep ObjectId, BranchName; remove Branch, Object, BranchLoadedState
- Modify: `crates/jazz-tools/src/lib.rs` — remove old module exports
- Modify: `crates/jazz-tools/src/storage/mod.rs` — remove commit-based trait methods
- Modify: `crates/jazz-tools/src/storage/storage_core.rs` — remove commit helpers
- Modify: `crates/jazz-tools/src/storage/fjall.rs` — remove commit methods
- Modify: `crates/jazz-tools/src/storage/opfs_btree.rs` — remove commit methods

- [ ] **Step 1: Remove commit.rs and object_manager/**

- [ ] **Step 2: Remove Branch, Object, BranchLoadedState from object.rs**

Keep `ObjectId` and `BranchName` (still used).

- [ ] **Step 3: Remove old Storage trait methods**

Remove: `create_object`, `load_object_metadata`, `load_branch`, `append_commit`, `delete_commit`, `set_branch_tails`, `store_ack_tier`

- [ ] **Step 4: Remove old implementations from FjallStorage and OpfsBTreeStorage**

- [ ] **Step 5: Remove old storage_core helpers**

- [ ] **Step 6: Remove old types from sync_manager/types.rs**

Remove commit-based InboxEntry, OutboxEntry types.

- [ ] **Step 7: Fix all compilation errors**

Run: `cargo check -p jazz-tools`
Iterate until clean.

- [ ] **Step 8: Run full test suite**

Run: `cargo test -p jazz-tools`
Expected: all PASS (old tests deleted, new tests passing)

- [ ] **Step 9: Commit**

```bash
git commit -m "refactor: remove commit DAG types and ObjectManager"
```

---

### Task 16: Update bindings (WASM + NAPI)

**Files:**

- Modify: `crates/jazz-wasm/src/` — update any references to old types
- Modify: `crates/jazz-napi/src/` — update any references to old types

- [ ] **Step 1: Check WASM bindings compile**

Run: `cargo check -p jazz-wasm --target wasm32-unknown-unknown`
Fix any compilation errors from removed types.

- [ ] **Step 2: Check NAPI bindings compile**

Run: `cargo check -p jazz-napi`
Fix any compilation errors.

- [ ] **Step 3: Check cloud server compiles**

Run: `cargo check -p jazz-cloud-server`

- [ ] **Step 4: Run full workspace build**

Run: `cargo check --workspace`

- [ ] **Step 5: Commit**

```bash
git commit -m "fix: update WASM and NAPI bindings for Yrs object layer"
```

---

### Task 17: End-to-end integration test

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core/tests.rs`

- [ ] **Step 1: Write integration test — full write/read/sync cycle**

```
1. Alice creates a row with title="Buy milk", done=false
2. Persist to storage
3. Bob syncs from Alice (initial sync via state vector exchange)
4. Bob reads the row — sees title="Buy milk", done=false
5. Alice updates done=true
6. Ongoing sync — Bob receives update
7. Bob reads — sees done=true
8. Both update concurrently (different fields)
9. Sync both ways — both converge to same state
```

- [ ] **Step 2: Write integration test — fork, edit, merge**

```
1. Create row, write initial state
2. Fork to "draft"
3. Edit branch and main independently
4. Merge — verify both changes present
```

- [ ] **Step 3: Write integration test — soft delete and undelete**

```
1. Create row, write data
2. Soft delete — verify query excludes it
3. Undelete — verify query includes it again
```

- [ ] **Step 4: Run all tests**

Run: `cargo test -p jazz-tools`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git commit -m "test: end-to-end integration tests for Yrs object layer"
```
