//! Integration tests for Object.

use bytes::Bytes;
use futures::executor::block_on;
use futures::io::AllowStdIo;
use futures::stream::StreamExt;
use groove::{
    Commit, CommitId, ContentRef, LastWriterWins, MemoryContentStore, Object, INLINE_THRESHOLD,
};
use std::io::Cursor;

fn make_commit(content: &[u8], parents: Vec<CommitId>) -> Commit {
    Commit {
        parents,
        content: ContentRef::inline(content.to_vec()),
        author: "test-author".to_string(),
        timestamp: 1000,
        meta: None,
    }
}

#[test]
fn object_has_main_branch() {
    let obj = Object::new(1, "test");

    assert_eq!(obj.branch_names(), vec!["main"]);
    assert!(obj.branch("main").unwrap().is_empty());
}

#[test]
fn object_create_branch() {
    let mut obj = Object::new(1, "test");

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
    let mut obj = Object::new(1, "test");
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
    let mut obj = Object::new(1, "test");

    // Add initial commit to main
    let c1 = make_commit(b"initial", vec![]);
    let id1 = obj.branch_mut("main").unwrap().add_commit(c1);

    // Create feature branch
    obj.create_branch("feature", "main", &id1).unwrap();

    // Add commit to main
    let c2 = Commit {
        parents: vec![id1],
        content: ContentRef::inline(b"main-change".to_vec()),
        author: "alice".to_string(),
        timestamp: 2000,
        meta: None,
    };
    obj.branch_mut("main").unwrap().add_commit(c2);

    // Add commit to feature
    let c3 = Commit {
        parents: vec![id1],
        content: ContentRef::inline(b"feature-change".to_vec()),
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
    let obj = Object::new(1, "test");

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
    let obj = Object::new(1, "test");
    assert!(obj.read_sync("main").is_none());
}

#[test]
#[should_panic(expected = "content exceeds INLINE_THRESHOLD")]
fn write_sync_panics_on_large_content() {
    let obj = Object::new(1, "test");
    let large_content = vec![0u8; INLINE_THRESHOLD + 1];
    obj.write_sync("main", &large_content, "alice", 1000);
}

#[test]
fn async_write_small_content() {
    let obj = Object::new(1, "test");
    let store = MemoryContentStore::new();

    // Write small content (should be inline)
    block_on(async {
        obj.write("main", b"hello", "alice", 1000, &store).await;
    });

    // Read back
    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, b"hello");
}

#[test]
fn async_write_large_content() {
    let obj = Object::new(1, "test");
    let store = MemoryContentStore::new();

    // Write large content (should be chunked)
    let large_content: Vec<u8> = (0..INLINE_THRESHOLD * 3).map(|i| (i % 256) as u8).collect();

    block_on(async {
        obj.write("main", &large_content, "alice", 1000, &store)
            .await;
    });

    // Read sync should return None (content is chunked)
    assert!(obj.read_sync("main").is_none());

    // Read async should work
    let content = block_on(async { obj.read("main", &store).await });
    assert_eq!(content.unwrap(), large_content);
}

#[test]
fn async_read_inline_content() {
    let obj = Object::new(1, "test");
    let store = MemoryContentStore::new();

    // Write using sync (inline)
    obj.write_sync("main", b"hello", "alice", 1000);

    // Read using async should also work
    let content = block_on(async { obj.read("main", &store).await });
    assert_eq!(content.unwrap(), b"hello");
}

#[test]
fn stream_write_small_content() {
    let obj = Object::new(1, "test");
    let store = MemoryContentStore::new();

    // Small content should be inlined
    let data = b"hello streaming world";
    let cursor = AllowStdIo::new(Cursor::new(data.to_vec()));

    block_on(async {
        obj.write_stream("main", cursor, "alice", 1000, &store)
            .await
            .unwrap();
    });

    // Should be readable via sync (inline)
    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, data);
}

#[test]
fn stream_write_large_content() {
    let obj = Object::new(1, "test");
    let store = MemoryContentStore::new();

    // Large content should be chunked
    let large_content: Vec<u8> = (0..INLINE_THRESHOLD * 5).map(|i| (i % 256) as u8).collect();
    let cursor = AllowStdIo::new(Cursor::new(large_content.clone()));

    block_on(async {
        obj.write_stream("main", cursor, "alice", 1000, &store)
            .await
            .unwrap();
    });

    // Should NOT be readable via sync (chunked)
    assert!(obj.read_sync("main").is_none());

    // But should be readable via async
    let content = block_on(async { obj.read("main", &store).await });
    assert_eq!(content.unwrap(), large_content);
}

#[test]
fn stream_write_empty_content() {
    let obj = Object::new(1, "test");
    let store = MemoryContentStore::new();

    let cursor = AllowStdIo::new(Cursor::new(Vec::<u8>::new()));

    block_on(async {
        obj.write_stream("main", cursor, "alice", 1000, &store)
            .await
            .unwrap();
    });

    // Empty content should be inline
    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, b"");
}

#[test]
fn stream_read_inline_content() {
    let obj = Object::new(1, "test");
    let store = MemoryContentStore::new();

    // Write inline content
    obj.write_sync("main", b"hello", "alice", 1000);

    // Stream read should work
    let chunks: Vec<Bytes> = block_on(async {
        let stream = obj.read_stream("main", &store).unwrap();
        stream.collect().await
    });

    assert_eq!(chunks.len(), 1);
    assert_eq!(&chunks[0][..], b"hello");
}

#[test]
fn stream_read_chunked_content() {
    let obj = Object::new(1, "test");
    let store = MemoryContentStore::new();

    // Write chunked content
    let large_content: Vec<u8> = (0..INLINE_THRESHOLD * 3).map(|i| (i % 256) as u8).collect();

    block_on(async {
        obj.write("main", &large_content, "alice", 1000, &store)
            .await;
    });

    // Stream read should yield multiple chunks
    let chunks: Vec<Bytes> = block_on(async {
        let stream = obj.read_stream("main", &store).unwrap();
        stream.collect().await
    });

    // Should have 3 chunks
    assert_eq!(chunks.len(), 3);

    // Concatenated should equal original
    let reassembled: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
    assert_eq!(reassembled, large_content);
}

#[test]
fn stream_read_empty_branch_returns_none() {
    let obj = Object::new(1, "test");
    let store = MemoryContentStore::new();

    assert!(obj.read_stream("main", &store).is_none());
}

#[test]
fn stream_roundtrip_exact_threshold() {
    let obj = Object::new(1, "test");
    let store = MemoryContentStore::new();

    // Content exactly at threshold should be inline
    let data: Vec<u8> = (0..INLINE_THRESHOLD).map(|i| (i % 256) as u8).collect();
    let cursor = AllowStdIo::new(Cursor::new(data.clone()));

    block_on(async {
        obj.write_stream("main", cursor, "alice", 1000, &store)
            .await
            .unwrap();
    });

    // Should be inline
    let content = obj.read_sync("main").unwrap();
    assert_eq!(content, data);
}

#[test]
fn stream_roundtrip_just_over_threshold() {
    let obj = Object::new(1, "test");
    let store = MemoryContentStore::new();

    // Content just over threshold should be chunked
    let data: Vec<u8> = (0..INLINE_THRESHOLD + 1).map(|i| (i % 256) as u8).collect();
    let cursor = AllowStdIo::new(Cursor::new(data.clone()));

    block_on(async {
        obj.write_stream("main", cursor, "alice", 1000, &store)
            .await
            .unwrap();
    });

    // Should be chunked (not readable via sync)
    assert!(obj.read_sync("main").is_none());

    // But readable via stream
    let chunks: Vec<Bytes> = block_on(async {
        let stream = obj.read_stream("main", &store).unwrap();
        stream.collect().await
    });

    let reassembled: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
    assert_eq!(reassembled, data);
}

#[test]
fn branch_ref_access() {
    let obj = Object::new(1, "test");

    // Can get branch ref
    let branch_ref = obj.branch_ref("main").unwrap();
    assert!(branch_ref.read().unwrap().is_empty());

    // Write through regular API
    obj.write_sync("main", b"hello", "alice", 1000);

    // Branch ref sees the change
    assert_eq!(branch_ref.read().unwrap().len(), 1);
}
