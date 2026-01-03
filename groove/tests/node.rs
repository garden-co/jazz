//! Integration tests for LocalNode.

use groove::{generate_object_id, LocalNode, ObjectId};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

#[test]
fn local_node_create_and_get_objects() {
    let node = LocalNode::in_memory();

    let id1 = node.create_object("chat");
    let id2 = node.create_object("message");

    assert!(node.get_object(id1).is_some());
    assert!(node.get_object(id2).is_some());
    assert!(node.get_object(ObjectId::new(999)).is_none());

    assert_eq!(node.get_object(id1).unwrap().read().unwrap().prefix, "chat");
    assert_eq!(
        node.get_object(id2).unwrap().read().unwrap().prefix,
        "message"
    );
}

#[test]
fn uuidv7_is_unique_and_ordered() {
    let id1 = generate_object_id();
    std::thread::sleep(std::time::Duration::from_millis(1));
    let id2 = generate_object_id();

    assert_ne!(id1, id2);
    assert!(id2 > id1);
}

#[test]
fn local_node_uses_uuidv7() {
    let node = LocalNode::in_memory();

    let id1 = node.create_object("test1");
    std::thread::sleep(std::time::Duration::from_millis(1));
    let id2 = node.create_object("test2");

    assert!(id2 > id1);
    assert!(id1 > ObjectId::new(1000));
}

#[test]
fn subscribe_to_empty_object() {
    let node = LocalNode::in_memory();
    let id = node.create_object("test");

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let _listener_id = node
        .subscribe(
            id,
            "main",
            Box::new(move |state| {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                // Empty branch has no tips
                assert!(state.tips.is_empty());
            }),
        )
        .unwrap();

    // Should be called once for initial state
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

#[test]
fn subscribe_nonexistent_object_errors() {
    let node = LocalNode::in_memory();

    let result = node.subscribe(ObjectId::new(999), "main", Box::new(|_| {}));
    assert!(result.is_err());
}

#[test]
fn subscribe_nonexistent_branch_errors() {
    let node = LocalNode::in_memory();
    let id = node.create_object("test");

    let result = node.subscribe(id, "nonexistent", Box::new(|_| {}));
    assert!(result.is_err());
}

#[test]
fn write_sync_notifies_listener() {
    let node = LocalNode::in_memory();
    let id = node.create_object("test");

    let call_count = Arc::new(AtomicUsize::new(0));
    let tip_counts = Arc::new(RwLock::new(Vec::new()));
    let call_count_clone = call_count.clone();
    let tip_counts_clone = tip_counts.clone();

    let _listener_id = node
        .subscribe(
            id,
            "main",
            Box::new(move |state| {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                tip_counts_clone.write().unwrap().push(state.tips.len());
            }),
        )
        .unwrap();

    // Initial call
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(*tip_counts.read().unwrap(), vec![0]); // empty initially

    // Write through node (auto-notifies)
    node.write_sync(id, "main", b"hello", "alice", 1000)
        .unwrap();

    // Callback should be called synchronously
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
    assert_eq!(*tip_counts.read().unwrap(), vec![0, 1]); // now has 1 tip

    // Write again
    node.write_sync(id, "main", b"world", "alice", 2000)
        .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 3);
    assert_eq!(*tip_counts.read().unwrap(), vec![0, 1, 1]); // still 1 tip
}

#[test]
fn write_without_subscriber() {
    let node = LocalNode::in_memory();
    let id = node.create_object("test");

    // Write without subscribing - should not error
    let commit_id = node
        .write_sync(id, "main", b"hello", "alice", 1000)
        .unwrap();

    // Now subscribe and verify content in callback
    let received_tips = Arc::new(RwLock::new(Vec::new()));
    let received_tips_clone = received_tips.clone();

    let _listener_id = node
        .subscribe(
            id,
            "main",
            Box::new(move |state| {
                received_tips_clone
                    .write()
                    .unwrap()
                    .extend(state.tips.clone());
            }),
        )
        .unwrap();

    let tips = received_tips.read().unwrap();
    assert_eq!(tips.len(), 1);
    assert_eq!(tips[0], commit_id);
}

#[test]
fn notify_object() {
    let node = LocalNode::in_memory();
    let id = node.create_object("test");

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let _listener_id = node
        .subscribe(
            id,
            "main",
            Box::new(move |_state| {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        )
        .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 1); // initial

    // Write directly to object (bypassing node's write method)
    {
        let obj_lock = node.get_object(id).unwrap();
        let obj = obj_lock.read().unwrap();
        obj.write_sync("main", b"direct write", "alice", 1000);
    }

    // Listener not notified yet
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Now notify
    node.notify_object(id);

    // Listener should be notified
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[test]
fn multiple_subscribers_all_notified() {
    let node = LocalNode::in_memory();
    let id = node.create_object("test");

    let count1 = Arc::new(AtomicUsize::new(0));
    let count2 = Arc::new(AtomicUsize::new(0));
    let count1_clone = count1.clone();
    let count2_clone = count2.clone();

    let _id1 = node
        .subscribe(
            id,
            "main",
            Box::new(move |_| {
                count1_clone.fetch_add(1, Ordering::SeqCst);
            }),
        )
        .unwrap();
    let _id2 = node
        .subscribe(
            id,
            "main",
            Box::new(move |_| {
                count2_clone.fetch_add(1, Ordering::SeqCst);
            }),
        )
        .unwrap();

    // Both called for initial state
    assert_eq!(count1.load(Ordering::SeqCst), 1);
    assert_eq!(count2.load(Ordering::SeqCst), 1);

    // Write
    node.write_sync(id, "main", b"hello", "alice", 1000)
        .unwrap();

    // Both should be notified
    assert_eq!(count1.load(Ordering::SeqCst), 2);
    assert_eq!(count2.load(Ordering::SeqCst), 2);
}

#[test]
fn read_write_roundtrip() {
    let node = LocalNode::in_memory();
    let id = node.create_object("test");

    node.write_sync(id, "main", b"hello world", "alice", 1000)
        .unwrap();

    let content = node.read_sync(id, "main").unwrap().unwrap();
    assert_eq!(content, b"hello world");
}

#[test]
fn unsubscribe_stops_notifications() {
    let node = LocalNode::in_memory();
    let id = node.create_object("test");

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let listener_id = node
        .subscribe(
            id,
            "main",
            Box::new(move |_| {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
            }),
        )
        .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 1); // initial

    node.write_sync(id, "main", b"hello", "alice", 1000)
        .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 2);

    // Unsubscribe
    assert!(node.unsubscribe(id, "main", listener_id));

    // Write again - should not notify
    node.write_sync(id, "main", b"world", "alice", 2000)
        .unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 2); // still 2
}

#[test]
fn callback_called_synchronously() {
    use std::sync::atomic::AtomicBool;

    let node = LocalNode::in_memory();
    let id = node.create_object("test");

    let was_called = Arc::new(AtomicBool::new(false));
    let was_called_clone = was_called.clone();

    let _listener_id = node
        .subscribe(
            id,
            "main",
            Box::new(move |_| {
                was_called_clone.store(true, Ordering::SeqCst);
            }),
        )
        .unwrap();

    was_called.store(false, Ordering::SeqCst);

    // Write - callback should be called SYNCHRONOUSLY (before write_sync returns)
    node.write_sync(id, "main", b"test", "alice", 1000).unwrap();

    // This assertion happens IMMEDIATELY after write_sync returns
    // If callback was async, this would fail
    assert!(
        was_called.load(Ordering::SeqCst),
        "Callback must be called synchronously"
    );
}
