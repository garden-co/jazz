//! Integration tests for ObjectManager.

use groove::{ObjectId, ObjectManager, generate_object_id};

#[test]
fn local_node_create_and_get_objects() {
    let node = ObjectManager::in_memory();

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
    let node = ObjectManager::in_memory();

    let id1 = node.create_object("test1");
    std::thread::sleep(std::time::Duration::from_millis(1));
    let id2 = node.create_object("test2");

    assert!(id2 > id1);
    assert!(id1 > ObjectId::new(1000));
}

#[test]
fn read_write_roundtrip() {
    let node = ObjectManager::in_memory();
    let id = node.create_object("test");

    node.write(id, "main", b"hello world", "alice", 1000)
        .unwrap();

    let content = node.read(id, "main").unwrap().unwrap();
    assert_eq!(content, b"hello world");
}
