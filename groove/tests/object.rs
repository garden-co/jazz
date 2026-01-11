//! Integration tests for Object.

use groove::{Commit, CommitId, LastWriterWins, Object, ObjectId};

fn make_commit(content: &[u8], parents: Vec<CommitId>) -> Commit {
    Commit {
        parents,
        content: content.to_vec().into_boxed_slice(),
        author: "test-author".to_string(),
        timestamp: 1000,
        meta: None,
    }
}

#[test]
fn object_has_main_branch() {
    let obj = Object::new(ObjectId::new(1), "test");

    assert_eq!(obj.branch_names(), vec!["main"]);
    assert!(obj.branch("main").unwrap().is_empty());
}

#[test]
fn object_create_branch() {
    let mut obj = Object::new(ObjectId::new(1), "test");

    // Add a commit to main
    let commit = make_commit(b"initial", vec![]);
    let commit_id = obj.branch_mut("main").unwrap().add_commit(commit);

    // Create a feature branch from that commit
    obj.create_branch("feature", "main", &commit_id).unwrap();

    assert_eq!(obj.branch_names().len(), 2);
    assert!(obj.branch_names().contains(&"main"));
    assert!(obj.branch_names().contains(&"feature"));

    // Feature branch should have the same commit
    let feature = obj.branch("feature").unwrap();
    assert_eq!(feature.len(), 1);
    assert!(feature.get_commit(&commit_id).is_some());
}

#[test]
fn object_branch_errors() {
    let mut obj = Object::new(ObjectId::new(1), "test");
    let fake_id = CommitId::from_bytes([0; 32]);

    // Can't create branch from non-existent commit
    assert!(obj.create_branch("feature", "main", &fake_id).is_err());

    // Can't create branch from non-existent source branch
    assert!(
        obj.create_branch("feature", "nonexistent", &fake_id)
            .is_err()
    );

    // Add a commit and create a branch
    let commit = make_commit(b"test", vec![]);
    let id = obj.branch_mut("main").unwrap().add_commit(commit);
    obj.create_branch("feature", "main", &id).unwrap();

    // Can't create duplicate branch
    assert!(obj.create_branch("feature", "main", &id).is_err());
}

#[test]
fn merge_branches_simple() {
    let mut obj = Object::new(ObjectId::new(1), "test");

    // Add initial commit to main
    let c1 = make_commit(b"initial", vec![]);
    let id1 = obj.branch_mut("main").unwrap().add_commit(c1);

    // Create feature branch
    obj.create_branch("feature", "main", &id1).unwrap();

    // Add commit to main
    let c2 = Commit {
        parents: vec![id1],
        content: b"main-change".to_vec().into_boxed_slice(),
        author: "alice".to_string(),
        timestamp: 2000,
        meta: None,
    };
    obj.branch_mut("main").unwrap().add_commit(c2);

    // Add commit to feature
    let c3 = Commit {
        parents: vec![id1],
        content: b"feature-change".to_vec().into_boxed_slice(),
        author: "bob".to_string(),
        timestamp: 2001,
        meta: None,
    };
    obj.branch_mut("feature").unwrap().add_commit(c3);

    // Merge feature into main
    let strategy = LastWriterWins;
    let merge_id = obj
        .merge_branches("main", "feature", &strategy, "alice", 3000)
        .unwrap();

    // Main should now have single tip (the merge commit)
    assert!(!obj.branch("main").unwrap().needs_merge());
    assert_eq!(obj.branch("main").unwrap().frontier(), &[merge_id]);

    // Merge commit should have 2 parents
    let main = obj.branch("main").unwrap();
    let merge_commit = main.get_commit(&merge_id).unwrap();
    assert_eq!(merge_commit.parents.len(), 2);
}

#[test]
fn write_and_read() {
    let obj = Object::new(ObjectId::new(1), "test");

    // Write some content
    let id = obj.write_sync("main", b"hello world", "alice", 1000);

    // Read it back
    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, b"hello world");

    // Write more content
    obj.write_sync("main", b"updated", "alice", 2000);

    // Read the updated content
    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, b"updated");

    // Verify commit chain
    let main = obj.branch("main").unwrap();
    assert_eq!(main.len(), 2);
    let latest = main.get_commit(&main.frontier()[0]).unwrap();
    assert_eq!(latest.parents.len(), 1);
    assert_eq!(latest.parents[0], id);
}

#[test]
fn read_empty_branch_returns_none() {
    let obj = Object::new(ObjectId::new(1), "test");
    assert!(obj.read_sync("main").is_none());
}

#[test]
fn write_large_content() {
    let obj = Object::new(ObjectId::new(1), "test");

    // Large content is stored directly (no chunking at commit level)
    let large_content: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
    obj.write_sync("main", &large_content, "alice", 1000);

    // Should be readable
    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, large_content);
}

#[test]
fn branch_ref_access() {
    let obj = Object::new(ObjectId::new(1), "test");

    // Can get branch ref
    let branch_ref = obj.branch_ref("main").unwrap();
    assert!(branch_ref.read().unwrap().is_empty());

    // Write through regular API
    obj.write_sync("main", b"hello", "alice", 1000);

    // Branch ref sees the change
    assert_eq!(branch_ref.read().unwrap().len(), 1);
}
