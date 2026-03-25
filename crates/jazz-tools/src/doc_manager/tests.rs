#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use yrs::updates::decoder::Decode;
    use yrs::updates::encoder::Encode;
    use yrs::{Doc, Map, ReadTxn, StateVector, Transact};

    use crate::doc_manager::DocManager;
    use crate::storage::MemoryStorage;

    fn make_manager() -> DocManager {
        DocManager::new(Box::new(MemoryStorage::new()))
    }

    #[test]
    fn create_returns_unique_ids() {
        let mut mgr = make_manager();
        let id1 = mgr.create(HashMap::new());
        let id2 = mgr.create(HashMap::new());
        assert_ne!(id1, id2);
        assert!(mgr.get(id1).is_some());
        assert!(mgr.get(id2).is_some());
    }

    #[test]
    fn get_returns_none_for_unknown_id() {
        let mgr = make_manager();
        use crate::object::ObjectId;
        let unknown = ObjectId::new();
        assert!(mgr.get(unknown).is_none());
    }

    #[test]
    fn apply_update_modifies_doc_state() {
        let mut mgr = make_manager();
        let id = mgr.create(HashMap::new());

        // Build an external Yrs doc with "title" = "Buy milk"
        let external = Doc::new();
        let root = external.get_or_insert_map("row");
        {
            let mut txn = external.transact_mut();
            root.insert(&mut txn, "title", "Buy milk");
        }

        // Encode its full state as an update
        let update = external.transact().encode_diff_v1(&StateVector::default());

        // Apply to the managed doc
        mgr.apply_update(id, &update)
            .expect("apply_update should succeed");

        // Verify the managed doc now has "title" = "Buy milk"
        let row_doc = mgr.get(id).expect("doc should exist");
        let txn = row_doc.doc.transact();
        let title = row_doc.root_map.get(&txn, "title");
        assert!(
            title.is_some(),
            "expected 'title' key to exist in managed doc"
        );
        match title.unwrap() {
            yrs::Out::Any(yrs::Any::String(s)) => {
                assert_eq!(s.as_ref(), "Buy milk");
            }
            other => panic!("unexpected value: {:?}", other),
        }
    }

    // -------------------------------------------------------------------------
    // Branch tests
    // -------------------------------------------------------------------------

    fn write_str(mgr: &mut DocManager, id: crate::object::ObjectId, key: &str, value: &str) {
        let row_doc = mgr.get_mut(id).expect("doc should exist");
        let mut txn = row_doc.doc.transact_mut();
        row_doc.root_map.insert(&mut txn, key, value);
    }

    fn write_bool(mgr: &mut DocManager, id: crate::object::ObjectId, key: &str, value: bool) {
        let row_doc = mgr.get_mut(id).expect("doc should exist");
        let mut txn = row_doc.doc.transact_mut();
        row_doc.root_map.insert(&mut txn, key, value);
    }

    fn read_str(mgr: &DocManager, id: crate::object::ObjectId, key: &str) -> Option<String> {
        let row_doc = mgr.get(id)?;
        let txn = row_doc.doc.transact();
        match row_doc.root_map.get(&txn, key)? {
            yrs::Out::Any(yrs::Any::String(s)) => Some(s.to_string()),
            _ => None,
        }
    }

    #[test]
    fn fork_creates_independent_branch_doc() {
        let mut mgr = make_manager();
        let parent_id = mgr.create(HashMap::new());
        write_str(&mut mgr, parent_id, "title", "Buy milk");

        let branch_id = mgr.fork(parent_id, "draft").expect("fork should succeed");

        // Branch has same state
        assert_eq!(
            read_str(&mgr, branch_id, "title").as_deref(),
            Some("Buy milk")
        );

        // Branch origin points to parent
        let branch = mgr.get(branch_id).expect("branch doc should exist");
        assert!(branch.origin.is_some(), "branch.origin should be Some");
        assert_eq!(branch.origin.as_ref().unwrap().0, parent_id);

        // Parent.branches contains "draft" -> branch_id
        let parent = mgr.get(parent_id).expect("parent doc should exist");
        assert_eq!(parent.branches.get("draft"), Some(&branch_id));

        // Modify branch, parent is unaffected
        write_str(&mut mgr, branch_id, "title", "Buy eggs");
        assert_eq!(
            read_str(&mgr, parent_id, "title").as_deref(),
            Some("Buy milk")
        );
        assert_eq!(
            read_str(&mgr, branch_id, "title").as_deref(),
            Some("Buy eggs")
        );
    }

    #[test]
    fn merge_applies_branch_changes_to_main() {
        let mut mgr = make_manager();
        let main_id = mgr.create(HashMap::new());
        write_str(&mut mgr, main_id, "title", "Buy milk");
        write_bool(&mut mgr, main_id, "done", false);

        let branch_id = mgr.fork(main_id, "draft").expect("fork should succeed");

        // Edit branch: title = "Buy eggs"
        write_str(&mut mgr, branch_id, "title", "Buy eggs");

        // Edit main: done = true
        write_bool(&mut mgr, main_id, "done", true);

        // Merge branch into main
        mgr.merge(branch_id, main_id).expect("merge should succeed");

        // Main now has both changes
        assert_eq!(
            read_str(&mgr, main_id, "title").as_deref(),
            Some("Buy eggs")
        );
        let main_doc = mgr.get(main_id).expect("main doc should exist");
        let txn = main_doc.doc.transact();
        let done = main_doc.root_map.get(&txn, "done");
        match done {
            Some(yrs::Out::Any(yrs::Any::Bool(b))) => assert!(b, "done should be true"),
            other => panic!("unexpected value for 'done': {:?}", other),
        }
    }

    #[test]
    fn merge_concurrent_same_field_resolves_deterministically() {
        let mut mgr = make_manager();
        let main_id = mgr.create(HashMap::new());
        write_str(&mut mgr, main_id, "title", "Original");

        let branch_id = mgr.fork(main_id, "draft").expect("fork should succeed");

        // Both sides edit "title"
        write_str(&mut mgr, branch_id, "title", "Branch version");
        write_str(&mut mgr, main_id, "title", "Main version");

        mgr.merge(branch_id, main_id).expect("merge should succeed");

        let result = read_str(&mgr, main_id, "title").expect("title should exist");
        assert!(
            result == "Branch version" || result == "Main version",
            "expected one of the two concurrent values, got: {result}"
        );
    }

    #[test]
    fn get_or_load_restores_doc_from_storage() {
        use yrs::{Map, Transact};

        let mut mgr = make_manager();
        let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
        let id = mgr.create(metadata);

        // Write some data
        {
            let row_doc = mgr.get_mut(id).unwrap();
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
        let title = row_doc.root_map.get(&txn, "title");
        match title {
            Some(yrs::Out::Any(yrs::Any::String(s))) => assert_eq!(s.as_ref(), "Buy milk"),
            other => panic!("expected String 'Buy milk', got: {:?}", other),
        }
    }

    #[test]
    fn append_update_persists_incrementally() {
        use yrs::{Map, Transact};

        let mut mgr = make_manager();
        let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
        let id = mgr.create(metadata);

        // Initial persist (snapshot)
        {
            let row_doc = mgr.get_mut(id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            row_doc.root_map.insert(&mut txn, "title", "Buy milk");
        }
        mgr.persist(id).unwrap();

        // Make another change and persist as update (not full snapshot)
        {
            let row_doc = mgr.get_mut(id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            row_doc.root_map.insert(&mut txn, "done", false);
        }
        mgr.persist_update(id).unwrap();

        // Evict and reload — should have both fields
        mgr.evict(id);
        let row_doc = mgr.get_or_load(id).unwrap();
        let txn = row_doc.doc.transact();
        let title = row_doc.root_map.get(&txn, "title");
        match title {
            Some(yrs::Out::Any(yrs::Any::String(s))) => assert_eq!(s.as_ref(), "Buy milk"),
            other => panic!("expected String 'Buy milk', got: {:?}", other),
        }
        let done = row_doc.root_map.get(&txn, "done");
        match done {
            Some(yrs::Out::Any(yrs::Any::Bool(b))) => assert!(!b, "done should be false"),
            other => panic!("expected Bool false, got: {:?}", other),
        }
    }

    #[test]
    fn encode_diff_produces_minimal_update() {
        let mut mgr = make_manager();
        let id = mgr.create(HashMap::new());

        // Write "title"="Buy milk" directly into the managed doc
        {
            let row_doc = mgr.get_mut(id).expect("doc should exist");
            let mut txn = row_doc.doc.transact_mut();
            row_doc.root_map.insert(&mut txn, "title", "Buy milk");
        }

        // Encode diff from empty StateVector
        let diff = mgr
            .encode_diff(id, &StateVector::default().encode_v1())
            .expect("encode_diff should succeed");

        // Apply diff to a fresh Doc and verify it has the data
        let fresh = Doc::new();
        let root = fresh.get_or_insert_map("row");
        {
            let mut txn = fresh.transact_mut();
            txn.apply_update(yrs::Update::decode_v1(&diff).expect("decode should succeed"))
                .expect("apply should succeed");
        }

        let txn = fresh.transact();
        let title = root.get(&txn, "title");
        assert!(title.is_some(), "expected 'title' key in fresh doc");
        match title.unwrap() {
            yrs::Out::Any(yrs::Any::String(s)) => {
                assert_eq!(s.as_ref(), "Buy milk");
            }
            other => panic!("unexpected value: {:?}", other),
        }
    }
}
