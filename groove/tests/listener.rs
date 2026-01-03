//! Integration tests for ObjectListenerRegistry.

use groove::{Branch, Commit, ContentRef, Environment, MemoryEnvironment, ObjectKey, ObjectListenerRegistry, ObjectState, ObjectId};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

fn make_env() -> Arc<dyn Environment> {
    Arc::new(MemoryEnvironment::new())
}

fn make_branch_with_commit(content: &[u8]) -> (Arc<RwLock<Branch>>, groove::CommitId) {
    let mut branch = Branch::new("main");
    let commit = Commit {
        parents: vec![],
        content: ContentRef::inline(content.to_vec()),
        author: "alice".to_string(),
        timestamp: 1000,
        meta: None,
    };
    let id = branch.add_commit(commit);
    (Arc::new(RwLock::new(branch)), id)
}

#[test]
fn basic_subscribe_and_notify() {
    let registry = ObjectListenerRegistry::new();
    let env = make_env();
    let (branch, id) = make_branch_with_commit(b"hello");
    let key = ObjectKey::new(ObjectId::new(1), "main");

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let listener_id = registry.subscribe(key.clone(), env, Box::new(move |_state| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
    }));

    // No initial call since there's no cached state yet
    assert_eq!(call_count.load(Ordering::SeqCst), 0);

    // Notify - should call the callback
    registry.notify(&key, vec![id], branch);
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Unsubscribe
    assert!(registry.unsubscribe(&key, listener_id));
    assert_eq!(registry.listener_count(&key), 0);
}

#[test]
fn new_subscriber_gets_current_state() {
    let registry = ObjectListenerRegistry::new();
    let env = make_env();
    let (branch, id) = make_branch_with_commit(b"hello");
    let key = ObjectKey::new(ObjectId::new(1), "main");

    // First subscriber
    let _id1 = registry.subscribe(key.clone(), env.clone(), Box::new(|_| {}));

    // Notify to set initial state
    registry.notify(&key, vec![id], branch);

    // Second subscriber should get called immediately with current state
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();
    let _id2 = registry.subscribe(key.clone(), env, Box::new(move |_state| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
    }));

    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

#[test]
fn multiple_listeners_all_called() {
    let registry = ObjectListenerRegistry::new();
    let env = make_env();
    let (branch, id) = make_branch_with_commit(b"hello");
    let key = ObjectKey::new(ObjectId::new(1), "main");

    let count1 = Arc::new(AtomicUsize::new(0));
    let count2 = Arc::new(AtomicUsize::new(0));
    let count1_clone = count1.clone();
    let count2_clone = count2.clone();

    let _id1 = registry.subscribe(key.clone(), env.clone(), Box::new(move |_| {
        count1_clone.fetch_add(1, Ordering::SeqCst);
    }));
    let _id2 = registry.subscribe(key.clone(), env, Box::new(move |_| {
        count2_clone.fetch_add(1, Ordering::SeqCst);
    }));

    registry.notify(&key, vec![id], branch);

    assert_eq!(count1.load(Ordering::SeqCst), 1);
    assert_eq!(count2.load(Ordering::SeqCst), 1);
}

#[test]
fn state_tracks_previous_tips() {
    let registry = ObjectListenerRegistry::new();
    let env = make_env();
    let (branch, id1) = make_branch_with_commit(b"first");
    let key = ObjectKey::new(ObjectId::new(1), "main");

    let has_previous = Arc::new(RwLock::new(Vec::new()));
    let has_previous_clone = has_previous.clone();

    let _id = registry.subscribe(key.clone(), env, Box::new(move |state| {
        has_previous_clone.write().unwrap().push(state.has_previous());
    }));

    // First notify - no previous
    registry.notify(&key, vec![id1], branch.clone());

    // Add second commit
    let id2 = {
        let mut b = branch.write().unwrap();
        let commit = Commit {
            parents: vec![id1],
            content: ContentRef::inline(b"second".to_vec()),
            author: "alice".to_string(),
            timestamp: 2000,
            meta: None,
        };
        b.add_commit(commit)
    };

    // Second notify - should have previous
    registry.notify(&key, vec![id2], branch);

    let results = has_previous.read().unwrap();
    assert_eq!(*results, vec![false, true]);
}

#[test]
fn get_current_state() {
    let registry = ObjectListenerRegistry::new();
    let env = make_env();
    let (branch, id) = make_branch_with_commit(b"hello");
    let key = ObjectKey::new(ObjectId::new(1), "main");

    // No state yet
    assert!(registry.get_current(&key).is_none());

    let _id = registry.subscribe(key.clone(), env, Box::new(|_| {}));
    registry.notify(&key, vec![id], branch);

    // Now should have state
    let state = registry.get_current(&key).unwrap();
    assert_eq!(state.tips.len(), 1);
    let content = state.read_content().unwrap();
    assert_eq!(&content[..], b"hello");
}

#[test]
fn unsubscribe_removes_listener() {
    let registry = ObjectListenerRegistry::new();
    let env = make_env();
    let (branch, id) = make_branch_with_commit(b"hello");
    let key = ObjectKey::new(ObjectId::new(1), "main");

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let listener_id = registry.subscribe(key.clone(), env, Box::new(move |_| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
    }));

    registry.notify(&key, vec![id], branch.clone());
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Unsubscribe
    registry.unsubscribe(&key, listener_id);

    // Add another commit
    let id2 = {
        let mut b = branch.write().unwrap();
        b.add_commit(Commit {
            parents: vec![],
            content: ContentRef::inline(b"world".to_vec()),
            author: "alice".to_string(),
            timestamp: 2000,
            meta: None,
        })
    };

    // Notify again - should not call unsubscribed listener
    registry.notify(&key, vec![id2], branch);
    assert_eq!(call_count.load(Ordering::SeqCst), 1); // Still 1
}

#[test]
fn diff_raw_detects_changes() {
    let env = make_env();
    let (branch, id1) = make_branch_with_commit(b"hello");

    // No previous - should be Initial
    let state = ObjectState::new(vec![id1], branch.clone(), env.clone());
    assert!(state.diff_raw().is_initial());

    // Add commit with same content
    let id2 = {
        let mut b = branch.write().unwrap();
        b.add_commit(Commit {
            parents: vec![id1],
            content: ContentRef::inline(b"hello".to_vec()),
            author: "alice".to_string(),
            timestamp: 2000,
            meta: None,
        })
    };

    // With previous, unchanged
    let state = ObjectState::with_previous(Some(vec![id1]), vec![id2], branch.clone(), env.clone());
    assert!(state.diff_raw().is_unchanged());

    // Add commit with different content
    let id3 = {
        let mut b = branch.write().unwrap();
        b.add_commit(Commit {
            parents: vec![id2],
            content: ContentRef::inline(b"world".to_vec()),
            author: "alice".to_string(),
            timestamp: 3000,
            meta: None,
        })
    };

    // With previous, changed
    let state = ObjectState::with_previous(Some(vec![id2]), vec![id3], branch, env);
    assert!(state.diff_raw().is_changed());
}
