//! Integration tests for Storage types.

use futures::executor::block_on;
use futures::stream::StreamExt;
use groove::{ChunkHash, Commit, CommitId, CommitStore, ContentRef, MemoryEnvironment};

#[test]
fn chunk_hash_deterministic() {
    let data = b"hello world";
    let hash1 = ChunkHash::compute(data);
    let hash2 = ChunkHash::compute(data);
    assert_eq!(hash1, hash2);
}

#[test]
fn chunk_hash_different_for_different_data() {
    let hash1 = ChunkHash::compute(b"hello");
    let hash2 = ChunkHash::compute(b"world");
    assert_ne!(hash1, hash2);
}

#[test]
fn content_ref_inline() {
    let content = ContentRef::inline(b"small data".to_vec());
    assert!(content.is_inline());
    assert_eq!(content.as_inline(), Some(b"small data".as_slice()));
    assert!(content.as_chunks().is_none());
}

#[test]
fn content_ref_chunked() {
    let hashes = vec![
        ChunkHash::compute(b"chunk1"),
        ChunkHash::compute(b"chunk2"),
    ];
    let content = ContentRef::chunked(hashes.clone());
    assert!(!content.is_inline());
    assert!(content.as_inline().is_none());
    assert_eq!(content.as_chunks(), Some(hashes.as_slice()));
}

#[test]
fn list_commits_returns_all_commits_not_just_frontier() {
    let env = MemoryEnvironment::new();
    let object_id: u128 = 1;
    let branch = "main";

    // Create a chain of 3 commits: c1 <- c2 <- c3
    let c1 = Commit {
        parents: vec![],
        content: ContentRef::inline(b"first".to_vec()),
        author: "alice".to_string(),
        timestamp: 1000,
        meta: None,
    };
    let id1 = block_on(env.put_commit(&c1));

    let c2 = Commit {
        parents: vec![id1],
        content: ContentRef::inline(b"second".to_vec()),
        author: "alice".to_string(),
        timestamp: 2000,
        meta: None,
    };
    let id2 = block_on(env.put_commit(&c2));

    let c3 = Commit {
        parents: vec![id2],
        content: ContentRef::inline(b"third".to_vec()),
        author: "alice".to_string(),
        timestamp: 3000,
        meta: None,
    };
    let id3 = block_on(env.put_commit(&c3));

    // Set frontier to just the tip (c3)
    block_on(env.set_frontier(object_id, branch, &[id3]));

    // list_commits should return ALL commits, not just the frontier
    let commits: Vec<CommitId> = block_on(async { env.list_commits(object_id, branch).collect().await });

    assert_eq!(
        commits.len(),
        3,
        "should return all 3 commits, not just frontier"
    );
    assert!(commits.contains(&id1), "should contain first commit");
    assert!(commits.contains(&id2), "should contain second commit");
    assert!(
        commits.contains(&id3),
        "should contain third commit (frontier)"
    );
}
