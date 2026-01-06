//! Integration tests for Commit.

use groove::{Commit, CommitId};

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
fn different_author_different_id() {
    let commit1 = Commit {
        parents: vec![],
        content: b"hello".to_vec().into_boxed_slice(),
        author: "alice".to_string(),
        timestamp: 1000,
        meta: None,
    };

    let commit2 = Commit {
        parents: vec![],
        content: b"hello".to_vec().into_boxed_slice(),
        author: "bob".to_string(),
        timestamp: 1000,
        meta: None,
    };

    assert_ne!(commit1.compute_id(), commit2.compute_id());
}

#[test]
fn different_timestamp_different_id() {
    let commit1 = Commit {
        parents: vec![],
        content: b"hello".to_vec().into_boxed_slice(),
        author: "test".to_string(),
        timestamp: 1000,
        meta: None,
    };

    let commit2 = Commit {
        parents: vec![],
        content: b"hello".to_vec().into_boxed_slice(),
        author: "test".to_string(),
        timestamp: 2000,
        meta: None,
    };

    assert_ne!(commit1.compute_id(), commit2.compute_id());
}
