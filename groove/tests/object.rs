//! Integration tests for Object.

use bytes::Bytes;
use futures::executor::block_on;
use futures::io::AllowStdIo;
use futures::stream::StreamExt;
use groove::{Commit, CommitId, LastWriterWins, Object, ObjectId};
use std::io::Cursor;

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
    assert!(obj
        .create_branch("feature", "nonexistent", &fake_id)
        .is_err());

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
fn sync_write_and_read() {
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
fn read_sync_empty_branch_returns_none() {
    let obj = Object::new(ObjectId::new(1), "test");
    assert!(obj.read_sync("main").is_none());
}

#[test]
fn sync_write_large_content() {
    let obj = Object::new(ObjectId::new(1), "test");

    // Large content is now stored directly (no chunking at commit level)
    let large_content: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
    obj.write_sync("main", &large_content, "alice", 1000);

    // Should be readable
    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, large_content);
}

#[test]
fn async_write_content() {
    let obj = Object::new(ObjectId::new(1), "test");

    // Write content
    block_on(async {
        obj.write("main", b"hello", "alice", 1000).await;
    });

    // Read back
    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, b"hello");
}

#[test]
fn async_write_large_content() {
    let obj = Object::new(ObjectId::new(1), "test");

    // Large content stored directly
    let large_content: Vec<u8> = (0..1024 * 1024 * 3).map(|i| (i % 256) as u8).collect();

    block_on(async {
        obj.write("main", &large_content, "alice", 1000).await;
    });

    // Should be readable via sync (no chunking anymore)
    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, large_content);

    // Async read should also work
    let content = block_on(async { obj.read("main").await });
    assert_eq!(content.unwrap(), large_content);
}

#[test]
fn async_read_content() {
    let obj = Object::new(ObjectId::new(1), "test");

    // Write using sync
    obj.write_sync("main", b"hello", "alice", 1000);

    // Read using async should also work
    let content = block_on(async { obj.read("main").await });
    assert_eq!(content.unwrap(), b"hello");
}

#[test]
fn stream_write_content() {
    let obj = Object::new(ObjectId::new(1), "test");

    let data = b"hello streaming world";
    let cursor = AllowStdIo::new(Cursor::new(data.to_vec()));

    block_on(async {
        obj.write_stream("main", cursor, "alice", 1000)
            .await
            .unwrap();
    });

    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, data);
}

#[test]
fn stream_write_large_content() {
    let obj = Object::new(ObjectId::new(1), "test");

    let large_content: Vec<u8> = (0..1024 * 1024 * 5).map(|i| (i % 256) as u8).collect();
    let cursor = AllowStdIo::new(Cursor::new(large_content.clone()));

    block_on(async {
        obj.write_stream("main", cursor, "alice", 1000)
            .await
            .unwrap();
    });

    // Should be readable (content stored directly, no chunking)
    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, large_content);
}

#[test]
fn stream_write_empty_content() {
    let obj = Object::new(ObjectId::new(1), "test");

    let cursor = AllowStdIo::new(Cursor::new(Vec::<u8>::new()));

    block_on(async {
        obj.write_stream("main", cursor, "alice", 1000)
            .await
            .unwrap();
    });

    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, b"");
}

#[test]
fn stream_read_content() {
    let obj = Object::new(ObjectId::new(1), "test");

    // Write content
    obj.write_sync("main", b"hello", "alice", 1000);

    // Stream read should work (returns single chunk now)
    let chunks: Vec<Bytes> = block_on(async {
        let stream = obj.read_stream("main").unwrap();
        stream.collect().await
    });

    assert_eq!(chunks.len(), 1);
    assert_eq!(&chunks[0][..], b"hello");
}

#[test]
fn stream_read_large_content() {
    let obj = Object::new(ObjectId::new(1), "test");

    // Write large content
    let large_content: Vec<u8> = (0..1024 * 1024 * 3).map(|i| (i % 256) as u8).collect();
    obj.write_sync("main", &large_content, "alice", 1000);

    // Stream read yields single chunk (no chunking at commit level)
    let chunks: Vec<Bytes> = block_on(async {
        let stream = obj.read_stream("main").unwrap();
        stream.collect().await
    });

    // Single chunk containing all content
    assert_eq!(chunks.len(), 1);
    assert_eq!(&chunks[0][..], &large_content[..]);
}

#[test]
fn stream_read_empty_branch_returns_none() {
    let obj = Object::new(ObjectId::new(1), "test");
    assert!(obj.read_stream("main").is_none());
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
