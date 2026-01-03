//! Integration tests for Commit.

use groove::{ChunkHash, Commit, CommitId, ContentRef};

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
fn commit_id_is_deterministic() {
    let commit1 = make_commit(b"hello", vec![]);
    let commit2 = make_commit(b"hello", vec![]);

    assert_eq!(commit1.compute_id(), commit2.compute_id());
}

#[test]
fn different_content_different_id() {
    let commit1 = make_commit(b"hello", vec![]);
    let commit2 = make_commit(b"world", vec![]);

    assert_ne!(commit1.compute_id(), commit2.compute_id());
}

#[test]
fn inline_vs_chunked_different_id() {
    let data = b"hello";
    let commit_inline = Commit {
        parents: vec![],
        content: ContentRef::inline(data.to_vec()),
        author: "test".to_string(),
        timestamp: 1000,
        meta: None,
    };

    // Same content but as a single "chunk"
    let chunk_hash = ChunkHash::compute(data);
    let commit_chunked = Commit {
        parents: vec![],
        content: ContentRef::chunked(vec![chunk_hash]),
        author: "test".to_string(),
        timestamp: 1000,
        meta: None,
    };

    // These should have different IDs because the storage format differs
    assert_ne!(commit_inline.compute_id(), commit_chunked.compute_id());
}
