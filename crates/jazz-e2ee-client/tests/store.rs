//! The private store contract (`E2EE_PROTOCOL.md` §6.1; core hooks note H9),
//! on the in-memory store every test uses: a save replaces a document only
//! at the revision it names, a scope is deleted whole, and a writer that
//! opened a scope before it was deleted never writes into it again.

use jazz::db::block_on;
use jazz_e2ee_client::store::is_document_name;
use jazz_e2ee_client::{MemoryStore, PrivateStore, StoreError};

fn compare_and_swap(store: &dyn PrivateStore) {
    let scope = r#"["app","test","alice"]"#;
    let at = block_on(store.open_scope(scope)).unwrap();
    assert_eq!(block_on(store.open_scope(scope)).unwrap(), at);
    let other = block_on(store.open_scope("other scope")).unwrap();
    assert_eq!(block_on(store.load(scope, "device")).unwrap(), None);
    assert_eq!(
        block_on(store.save(scope, at, "device", b"one", None)).unwrap(),
        1
    );
    // A writer that did not see revision 1 does not overwrite it.
    assert_eq!(
        block_on(store.save(scope, at, "device", b"stale", None)),
        Err(StoreError::Conflict)
    );
    assert_eq!(
        block_on(store.save(scope, at, "device", b"stale", Some(2))),
        Err(StoreError::Conflict)
    );
    assert_eq!(
        block_on(store.save(scope, at, "device", b"two", Some(1))).unwrap(),
        2
    );
    let stored = block_on(store.load(scope, "device")).unwrap().unwrap();
    assert_eq!((stored.bytes.as_slice(), stored.revision), (&b"two"[..], 2));
    block_on(store.save(scope, at, "held-a", b"keys", None)).unwrap();
    block_on(store.save("other scope", other, "device", b"other", None)).unwrap();
    assert_eq!(block_on(store.list(scope)).unwrap(), ["device", "held-a"]);
    assert!(block_on(store.save(scope, at, "../escape", b"x", None)).is_err());

    block_on(store.delete(scope)).unwrap();
    assert_eq!(
        block_on(store.save(scope, at, "device", b"late", None)),
        Err(StoreError::Wiped),
        "a save never recreates a deleted scope"
    );
    assert_eq!(block_on(store.list(scope)).unwrap(), Vec::<String>::new());
    assert_eq!(block_on(store.load(scope, "device")).unwrap(), None);
    block_on(store.delete(scope)).unwrap();
    assert_eq!(
        block_on(store.load("other scope", "device"))
            .unwrap()
            .unwrap()
            .bytes,
        b"other"
    );
}

/// Contract: a store saves by compare and swap on the document's revision
/// (a writer that did not see the current revision gets a conflict and
/// writes nothing), lists a scope's documents, and deletes a scope whole
/// without touching another.
#[test]
fn saves_are_compare_and_swap_and_scopes_delete_whole() {
    compare_and_swap(&MemoryStore::new());
    assert!(is_document_name(
        "floor-a11ce000-0000-5000-8000-00000000a11c"
    ));
    assert!(!is_document_name("Held"));
    assert!(!is_document_name(""));
}

fn stale_writer_after_a_wipe(store: &dyn PrivateStore) {
    let scope = "scope";
    let stale = block_on(store.open_scope(scope)).unwrap();
    assert_eq!(
        block_on(store.save(scope, stale, "held-a", b"old", None)),
        Ok(1)
    );
    block_on(store.delete(scope)).unwrap();
    let fresh = block_on(store.open_scope(scope)).unwrap();
    assert_ne!(fresh, stale);
    assert_eq!(
        block_on(store.save(scope, fresh, "held-a", b"new", None)),
        Ok(1)
    );
    for expected in [Some(1), None] {
        assert_eq!(
            block_on(store.save(scope, stale, "held-a", b"stale", expected)),
            Err(StoreError::Wiped)
        );
    }
    assert_eq!(
        block_on(store.load(scope, "held-a"))
            .unwrap()
            .unwrap()
            .bytes,
        b"new"
    );
}

/// Contract: a writer that opened a scope before it was deleted never
/// writes into it again, even at the revision a document recreated since
/// has: the scope's incarnation changed, so its save is refused and the
/// new document stays.
#[test]
fn a_stale_writer_never_writes_into_a_recreated_scope() {
    stale_writer_after_a_wipe(&MemoryStore::new());
}
