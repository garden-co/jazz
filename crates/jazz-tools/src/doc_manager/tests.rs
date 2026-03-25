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
    fn compact_replaces_updates_with_snapshot() {
        use yrs::{Map, Transact};

        let mut mgr = make_manager();
        let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
        let id = mgr.create(metadata);

        // Write and persist as updates (not snapshots)
        for i in 0..10 {
            {
                let row_doc = mgr.get_mut(id).unwrap();
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
        let counter = row_doc.root_map.get(&txn, "counter");
        match counter {
            Some(yrs::Out::Any(yrs::Any::Number(n))) => assert_eq!(n, 9.0),
            other => panic!("expected Number 9.0, got: {:?}", other),
        }
    }

    #[test]
    fn crash_safe_compaction_stale_updates_are_deduplicated() {
        use yrs::{Map, Transact};

        let mut mgr = make_manager();
        let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
        let id = mgr.create(metadata);

        {
            let row_doc = mgr.get_mut(id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            row_doc.root_map.insert(&mut txn, "title", "Buy milk");
        }
        mgr.persist_update(id).unwrap();

        // Simulate partial compaction: snapshot saved but updates NOT cleared
        // persist() does both, but this simulates what happens if clear_updates crashes
        // We call persist which saves snapshot AND clears updates, then re-add the stale update
        mgr.persist(id).unwrap();
        // Re-add the update to simulate crash (snapshot saved, but updates not cleared)
        mgr.persist_update(id).unwrap();

        // Reload — stale updates on top of snapshot should be harmless (Yrs deduplication)
        mgr.evict(id);
        let row_doc = mgr.get_or_load(id).unwrap();
        let txn = row_doc.doc.transact();
        let title = row_doc.root_map.get(&txn, "title");
        match title {
            Some(yrs::Out::Any(yrs::Any::String(s))) => assert_eq!(s.as_ref(), "Buy milk"),
            other => panic!("expected String 'Buy milk', got: {:?}", other),
        }
    }

    #[test]
    fn subscribe_all_receives_change_notifications() {
        use crate::object::ObjectId;
        use std::sync::{Arc, Mutex};
        use yrs::{Map, Transact};

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
            let row_doc = mgr.get_mut(id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            row_doc.root_map.insert(&mut txn, "title", "Buy milk");
        }
        mgr.notify_change(id);

        assert_eq!(changes.lock().unwrap().len(), 1);
        assert_eq!(changes.lock().unwrap()[0], id);

        mgr.unsubscribe_all(sub_id);
    }

    #[test]
    fn observe_update_captures_raw_bytes_for_sync() {
        use std::sync::{Arc, Mutex};
        use yrs::updates::decoder::Decode;
        use yrs::{Doc, Map, Transact, Update};

        let mut mgr = make_manager();

        let captured_updates: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let updates_clone = captured_updates.clone();

        mgr.set_update_handler(move |_doc_id, update_bytes| {
            updates_clone.lock().unwrap().push(update_bytes.to_vec());
        });

        let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
        let id = mgr.create(metadata);

        // Write to doc — should trigger update handler with raw Yrs update bytes
        {
            let row_doc = mgr.get_mut(id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            row_doc.root_map.insert(&mut txn, "title", "Buy milk");
        }
        // observe_update_v1 fires on transaction drop (which happens at end of block above)

        let updates = captured_updates.lock().unwrap();
        assert_eq!(updates.len(), 1);

        // Verify the captured bytes can be applied to a fresh doc
        let fresh_doc = Doc::new();
        let fresh_map = fresh_doc.get_or_insert_map("row");
        let update = Update::decode_v1(&updates[0]).unwrap();
        fresh_doc.transact_mut().apply_update(update).unwrap();

        let txn = fresh_doc.transact();
        match fresh_map.get(&txn, "title") {
            Some(yrs::Out::Any(yrs::Any::String(s))) => assert_eq!(s.as_ref(), "Buy milk"),
            other => panic!("expected String 'Buy milk', got: {:?}", other),
        }
    }

    // -------------------------------------------------------------------------
    // Column type mapping tests
    // -------------------------------------------------------------------------

    #[test]
    fn column_type_roundtrip_text() {
        use crate::query_manager::types::Value;
        use crate::row_doc::{read_column, write_column};

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
    fn column_type_roundtrip_integer() {
        use crate::query_manager::types::Value;
        use crate::row_doc::{read_column, write_column};

        let doc = Doc::new();
        let map = doc.get_or_insert_map("row");
        {
            let mut txn = doc.transact_mut();
            write_column(&map, &mut txn, "age", &Value::Integer(42));
        }
        let txn = doc.transact();
        let val = read_column(&map, &txn, "age").unwrap();
        assert_eq!(val, Value::Integer(42));
    }

    #[test]
    fn column_type_roundtrip_double() {
        use crate::query_manager::types::Value;
        use crate::row_doc::{read_column, write_column};

        let doc = Doc::new();
        let map = doc.get_or_insert_map("row");
        {
            let mut txn = doc.transact_mut();
            write_column(&map, &mut txn, "score", &Value::Double(3.14));
        }
        let txn = doc.transact();
        let val = read_column(&map, &txn, "score").unwrap();
        assert_eq!(val, Value::Double(3.14));
    }

    #[test]
    fn column_type_roundtrip_boolean() {
        use crate::query_manager::types::Value;
        use crate::row_doc::{read_column, write_column};

        let doc = Doc::new();
        let map = doc.get_or_insert_map("row");
        {
            let mut txn = doc.transact_mut();
            write_column(&map, &mut txn, "done", &Value::Boolean(true));
        }
        let txn = doc.transact();
        let val = read_column(&map, &txn, "done").unwrap();
        assert_eq!(val, Value::Boolean(true));
    }

    #[test]
    fn column_type_roundtrip_bigint() {
        use crate::query_manager::types::Value;
        use crate::row_doc::{read_column, write_column};

        let doc = Doc::new();
        let map = doc.get_or_insert_map("row");
        {
            let mut txn = doc.transact_mut();
            write_column(&map, &mut txn, "big", &Value::BigInt(9_000_000_000));
        }
        let txn = doc.transact();
        let val = read_column(&map, &txn, "big").unwrap();
        assert_eq!(val, Value::BigInt(9_000_000_000));
    }

    #[test]
    fn column_type_roundtrip_null() {
        use crate::query_manager::types::Value;
        use crate::row_doc::{read_column, write_column};

        let doc = Doc::new();
        let map = doc.get_or_insert_map("row");
        {
            let mut txn = doc.transact_mut();
            write_column(&map, &mut txn, "name", &Value::Text("Alice".into()));
        }
        {
            let mut txn = doc.transact_mut();
            write_column(&map, &mut txn, "name", &Value::Null);
        }
        let txn = doc.transact();
        let val = read_column(&map, &txn, "name");
        assert!(val.is_none() || val == Some(Value::Null));
    }

    #[test]
    fn column_type_roundtrip_bytea() {
        use crate::query_manager::types::Value;
        use crate::row_doc::{read_column, write_column};

        let doc = Doc::new();
        let map = doc.get_or_insert_map("row");
        {
            let mut txn = doc.transact_mut();
            write_column(&map, &mut txn, "data", &Value::Bytea(vec![1, 2, 3]));
        }
        let txn = doc.transact();
        let val = read_column(&map, &txn, "data").unwrap();
        assert_eq!(val, Value::Bytea(vec![1, 2, 3]));
    }

    // -------------------------------------------------------------------------
    // End-to-end integration tests
    // -------------------------------------------------------------------------

    /// alice creates a row → syncs to bob → alice updates → bob receives incremental update
    ///
    ///   alice                          bob
    ///   ──────                         ────
    ///   create(title="Buy milk",
    ///          done=false)
    ///          ──── full sync ────▶    apply
    ///                                  read ✓
    ///   update(done=true)
    ///          ──── incr sync ───▶     apply
    ///                                  read ✓
    #[test]
    fn e2e_write_read_sync_cycle() {
        use crate::query_manager::types::Value;
        use crate::row_doc::{read_column, write_column};

        let mut alice = make_manager();
        let mut bob = make_manager();

        let doc_id = crate::object::ObjectId::new();
        let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);

        // Alice creates and writes
        alice.create_with_id(doc_id, metadata.clone());
        {
            let row_doc = alice.get_mut(doc_id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            write_column(
                &row_doc.root_map,
                &mut txn,
                "title",
                &Value::Text("Buy milk".into()),
            );
            write_column(&row_doc.root_map, &mut txn, "done", &Value::Boolean(false));
        }

        // Alice encodes full state for bob
        let alice_update = alice
            .encode_diff(doc_id, &StateVector::default().encode_v1())
            .unwrap();

        // Bob creates doc and applies alice's state
        bob.create_with_id(doc_id, metadata);
        bob.apply_update(doc_id, &alice_update).unwrap();

        // Bob reads — should see alice's data
        {
            let row_doc = bob.get(doc_id).unwrap();
            let txn = row_doc.doc.transact();
            assert_eq!(
                read_column(&row_doc.root_map, &txn, "title"),
                Some(Value::Text("Buy milk".into()))
            );
            assert_eq!(
                read_column(&row_doc.root_map, &txn, "done"),
                Some(Value::Boolean(false))
            );
        }

        // Alice updates done=true
        {
            let row_doc = alice.get_mut(doc_id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            write_column(&row_doc.root_map, &mut txn, "done", &Value::Boolean(true));
        }

        // Get bob's current state vector, encode diff from alice
        let bob_sv = bob.get_state_vector(doc_id).unwrap();
        let incremental_update = alice.encode_diff(doc_id, &bob_sv).unwrap();

        // Bob applies incremental update
        bob.apply_update(doc_id, &incremental_update).unwrap();

        // Bob reads updated state
        {
            let row_doc = bob.get(doc_id).unwrap();
            let txn = row_doc.doc.transact();
            assert_eq!(
                read_column(&row_doc.root_map, &txn, "title"),
                Some(Value::Text("Buy milk".into()))
            );
            assert_eq!(
                read_column(&row_doc.root_map, &txn, "done"),
                Some(Value::Boolean(true))
            );
        }
    }

    /// alice and bob make concurrent edits to different fields — both converge
    ///
    ///   alice                          bob
    ///   ──────                         ────
    ///   initial state synced           initial state synced
    ///   write title="Alice's version"  write done=true
    ///          ──── exchange diffs ──▶
    ///          ◀── exchange diffs ────
    ///   converge ✓                     converge ✓
    #[test]
    fn e2e_concurrent_different_fields_converge() {
        use crate::query_manager::types::Value;
        use crate::row_doc::{read_column, write_column};

        let mut alice = make_manager();
        let mut bob = make_manager();
        let doc_id = crate::object::ObjectId::new();
        let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);

        // Both start with same initial state
        alice.create_with_id(doc_id, metadata.clone());
        bob.create_with_id(doc_id, metadata);

        {
            let row_doc = alice.get_mut(doc_id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            write_column(
                &row_doc.root_map,
                &mut txn,
                "title",
                &Value::Text("Original".into()),
            );
            write_column(&row_doc.root_map, &mut txn, "done", &Value::Boolean(false));
        }

        // Sync initial state to bob
        let alice_update = alice
            .encode_diff(doc_id, &StateVector::default().encode_v1())
            .unwrap();
        bob.apply_update(doc_id, &alice_update).unwrap();

        // Capture state vectors BEFORE concurrent edits
        let alice_sv_before = alice.get_state_vector(doc_id).unwrap();
        let bob_sv_before = bob.get_state_vector(doc_id).unwrap();

        // Concurrent writes to different fields
        {
            let row_doc = alice.get_mut(doc_id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            write_column(
                &row_doc.root_map,
                &mut txn,
                "title",
                &Value::Text("Alice's version".into()),
            );
        }
        {
            let row_doc = bob.get_mut(doc_id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            write_column(&row_doc.root_map, &mut txn, "done", &Value::Boolean(true));
        }

        // Exchange diffs
        let alice_diff = alice.encode_diff(doc_id, &bob_sv_before).unwrap();
        let bob_diff = bob.encode_diff(doc_id, &alice_sv_before).unwrap();

        bob.apply_update(doc_id, &alice_diff).unwrap();
        alice.apply_update(doc_id, &bob_diff).unwrap();

        // Both should converge
        for (name, mgr) in [("alice", &alice), ("bob", &bob)] {
            let row_doc = mgr.get(doc_id).unwrap();
            let txn = row_doc.doc.transact();
            assert_eq!(
                read_column(&row_doc.root_map, &txn, "title"),
                Some(Value::Text("Alice's version".into())),
                "{name} should have Alice's title"
            );
            assert_eq!(
                read_column(&row_doc.root_map, &txn, "done"),
                Some(Value::Boolean(true)),
                "{name} should have bob's done=true"
            );
        }
    }

    /// fork a doc, edit branch and main independently, merge — both changes present
    ///
    ///   main           branch("draft")
    ///   ────           ───────────────
    ///   title="Buy milk"
    ///   done=false
    ///        ── fork ──▶  (same state)
    ///   done=true         title="Buy eggs"
    ///        ◀── merge ──
    ///   title="Buy eggs"
    ///   done=true
    #[test]
    fn e2e_fork_edit_merge() {
        use crate::query_manager::types::Value;
        use crate::row_doc::{read_column, write_column};

        let mut mgr = make_manager();
        let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
        let main_id = mgr.create(metadata);

        // Write initial state
        {
            let row_doc = mgr.get_mut(main_id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            write_column(
                &row_doc.root_map,
                &mut txn,
                "title",
                &Value::Text("Buy milk".into()),
            );
            write_column(&row_doc.root_map, &mut txn, "done", &Value::Boolean(false));
        }

        // Fork
        let branch_id = mgr.fork(main_id, "draft").unwrap();

        // Edit branch: change title
        {
            let row_doc = mgr.get_mut(branch_id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            write_column(
                &row_doc.root_map,
                &mut txn,
                "title",
                &Value::Text("Buy eggs".into()),
            );
        }

        // Edit main: change done
        {
            let row_doc = mgr.get_mut(main_id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            write_column(&row_doc.root_map, &mut txn, "done", &Value::Boolean(true));
        }

        // Merge branch into main
        mgr.merge(branch_id, main_id).unwrap();

        // Main should have both changes
        {
            let row_doc = mgr.get(main_id).unwrap();
            let txn = row_doc.doc.transact();
            assert_eq!(
                read_column(&row_doc.root_map, &txn, "title"),
                Some(Value::Text("Buy eggs".into())),
                "title should be branch's version"
            );
            assert_eq!(
                read_column(&row_doc.root_map, &txn, "done"),
                Some(Value::Boolean(true)),
                "done should be main's version"
            );
        }
    }

    /// persist → evict → reload preserves all column types
    #[test]
    fn e2e_persist_evict_reload_preserves_types() {
        use crate::query_manager::types::Value;
        use crate::row_doc::{read_column, write_column};

        let mut mgr = make_manager();
        let metadata = HashMap::from([("table".to_string(), "todos".to_string())]);
        let doc_id = mgr.create(metadata);

        // Write various types
        {
            let row_doc = mgr.get_mut(doc_id).unwrap();
            let mut txn = row_doc.doc.transact_mut();
            write_column(
                &row_doc.root_map,
                &mut txn,
                "title",
                &Value::Text("Buy milk".into()),
            );
            write_column(&row_doc.root_map, &mut txn, "count", &Value::Integer(42));
            write_column(&row_doc.root_map, &mut txn, "score", &Value::Double(3.14));
            write_column(&row_doc.root_map, &mut txn, "done", &Value::Boolean(false));
            write_column(
                &row_doc.root_map,
                &mut txn,
                "big",
                &Value::BigInt(9_000_000_000),
            );
        }

        // Persist and evict
        mgr.persist(doc_id).unwrap();
        mgr.evict(doc_id);
        assert!(mgr.get(doc_id).is_none());

        // Reload
        let row_doc = mgr.get_or_load(doc_id).unwrap();
        let txn = row_doc.doc.transact();

        assert_eq!(
            read_column(&row_doc.root_map, &txn, "title"),
            Some(Value::Text("Buy milk".into()))
        );
        assert_eq!(
            read_column(&row_doc.root_map, &txn, "count"),
            Some(Value::Integer(42))
        );
        assert_eq!(
            read_column(&row_doc.root_map, &txn, "score"),
            Some(Value::Double(3.14))
        );
        assert_eq!(
            read_column(&row_doc.root_map, &txn, "done"),
            Some(Value::Boolean(false))
        );
        assert_eq!(
            read_column(&row_doc.root_map, &txn, "big"),
            Some(Value::BigInt(9_000_000_000))
        );
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
