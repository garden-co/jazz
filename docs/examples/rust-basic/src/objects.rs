//! Working with Objects and ObjectIds

use groove::object::ObjectId;

/// ObjectId examples
pub fn object_id_examples() {
    // Generate a new ObjectId (UUIDv7 in Crockford Base32)
    let id = ObjectId::new();
    println!("New ObjectId: {}", id);

    // Parse from string
    let parsed: ObjectId = "01HXYZABC123456789DEFGHJ".parse().unwrap();
    println!("Parsed ObjectId: {}", parsed);

    // ObjectIds are sortable by creation time
    let id1 = ObjectId::new();
    std::thread::sleep(std::time::Duration::from_millis(1));
    let id2 = ObjectId::new();

    assert!(id2 > id1, "Later ObjectIds are lexicographically greater");

    // Get the timestamp from an ObjectId
    let timestamp = id.timestamp();
    println!("Created at: {:?}", timestamp);
}

/// Working with the object commit graph
pub fn commit_graph_examples() {
    use groove::node::LocalNode;

    // Create a local node for managing objects
    let mut node = LocalNode::new();

    // Create a new object
    let obj_id = node.create_object();
    println!("Created object: {}", obj_id);

    // Make a change (creates a commit)
    // node.update(&obj_id, data)?;

    // Get the current head commit
    // let head = node.head(&obj_id)?;

    // View commit history
    // let history = node.commits(&obj_id);
    // for commit in history {
    //     println!("Commit: {} at {}", commit.id, commit.timestamp);
    // }
}
