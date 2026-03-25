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
