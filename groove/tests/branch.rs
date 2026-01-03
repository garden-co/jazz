//! Integration tests for Branch.

use groove::{Branch, Commit, CommitId, ContentRef};

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
fn branch_single_commit() {
    let mut branch = Branch::new("main");
    let commit = make_commit(b"initial", vec![]);
    let id = branch.add_commit(commit);

    assert_eq!(branch.len(), 1);
    assert_eq!(branch.frontier(), &[id]);
    assert!(!branch.needs_merge());
}

#[test]
fn branch_sequential_commits() {
    let mut branch = Branch::new("main");

    let c1 = make_commit(b"first", vec![]);
    let id1 = branch.add_commit(c1);

    let c2 = make_commit(b"second", vec![id1]);
    let id2 = branch.add_commit(c2);

    let c3 = make_commit(b"third", vec![id2]);
    let id3 = branch.add_commit(c3);

    assert_eq!(branch.len(), 3);
    assert_eq!(branch.frontier(), &[id3]);
    assert!(!branch.needs_merge());

    // Check parent-child relationships
    assert_eq!(branch.children_of(&id1), Some(&vec![id2]));
    assert_eq!(branch.children_of(&id2), Some(&vec![id3]));
    assert_eq!(branch.children_of(&id3), None);
}

#[test]
fn branch_concurrent_commits_need_merge() {
    let mut branch = Branch::new("main");

    // Root commit
    let root = make_commit(b"root", vec![]);
    let root_id = branch.add_commit(root);

    // Two concurrent commits from root
    let c1 = Commit {
        parents: vec![root_id],
        content: ContentRef::inline(b"branch-a".to_vec()),
        author: "alice".to_string(),
        timestamp: 2000,
        meta: None,
    };
    let id1 = branch.add_commit(c1);

    let c2 = Commit {
        parents: vec![root_id],
        content: ContentRef::inline(b"branch-b".to_vec()),
        author: "bob".to_string(),
        timestamp: 2001,
        meta: None,
    };
    let id2 = branch.add_commit(c2);

    assert_eq!(branch.len(), 3);
    assert!(branch.needs_merge());
    assert_eq!(branch.frontier().len(), 2);
    assert!(branch.frontier().contains(&id1));
    assert!(branch.frontier().contains(&id2));
}

#[test]
fn branch_merge_commit() {
    let mut branch = Branch::new("main");

    let root = make_commit(b"root", vec![]);
    let root_id = branch.add_commit(root);

    // Two concurrent branches
    let c1 = Commit {
        parents: vec![root_id],
        content: ContentRef::inline(b"a".to_vec()),
        author: "alice".to_string(),
        timestamp: 2000,
        meta: None,
    };
    let id1 = branch.add_commit(c1);

    let c2 = Commit {
        parents: vec![root_id],
        content: ContentRef::inline(b"b".to_vec()),
        author: "bob".to_string(),
        timestamp: 2001,
        meta: None,
    };
    let id2 = branch.add_commit(c2);

    assert!(branch.needs_merge());

    // Merge commit
    let merge = Commit {
        parents: vec![id1, id2],
        content: ContentRef::inline(b"merged".to_vec()),
        author: "alice".to_string(),
        timestamp: 3000,
        meta: None,
    };
    let merge_id = branch.add_commit(merge);

    assert!(!branch.needs_merge());
    assert_eq!(branch.frontier(), &[merge_id]);
}

#[test]
fn find_lca_same_commit() {
    let mut branch = Branch::new("main");
    let c = make_commit(b"only", vec![]);
    let id = branch.add_commit(c);

    let lca = branch.find_lca(&id, &id);
    assert_eq!(lca, vec![id]);
}

#[test]
fn find_lca_linear_history() {
    let mut branch = Branch::new("main");

    let c1 = make_commit(b"first", vec![]);
    let id1 = branch.add_commit(c1);

    let c2 = make_commit(b"second", vec![id1]);
    let id2 = branch.add_commit(c2);

    let c3 = make_commit(b"third", vec![id2]);
    let id3 = branch.add_commit(c3);

    // LCA of id2 and id3 should be id2
    let lca = branch.find_lca(&id2, &id3);
    assert_eq!(lca, vec![id2]);

    // LCA of id1 and id3 should be id1
    let lca = branch.find_lca(&id1, &id3);
    assert_eq!(lca, vec![id1]);
}

#[test]
fn find_lca_diamond() {
    let mut branch = Branch::new("main");

    //     c1
    //    /  \
    //   c2   c3
    //    \  /
    //     c4

    let c1 = make_commit(b"root", vec![]);
    let id1 = branch.add_commit(c1);

    let c2 = Commit {
        parents: vec![id1],
        content: ContentRef::inline(b"left".to_vec()),
        author: "alice".to_string(),
        timestamp: 2000,
        meta: None,
    };
    let id2 = branch.add_commit(c2);

    let c3 = Commit {
        parents: vec![id1],
        content: ContentRef::inline(b"right".to_vec()),
        author: "bob".to_string(),
        timestamp: 2001,
        meta: None,
    };
    let id3 = branch.add_commit(c3);

    // LCA of c2 and c3 should be c1
    let lca = branch.find_lca(&id2, &id3);
    assert_eq!(lca, vec![id1]);

    // Now merge them
    let c4 = Commit {
        parents: vec![id2, id3],
        content: ContentRef::inline(b"merged".to_vec()),
        author: "alice".to_string(),
        timestamp: 3000,
        meta: None,
    };
    let id4 = branch.add_commit(c4);

    // LCA of c2 and c4 should be c2
    let lca = branch.find_lca(&id2, &id4);
    assert_eq!(lca, vec![id2]);
}

#[test]
fn is_ancestor() {
    let mut branch = Branch::new("main");

    let c1 = make_commit(b"first", vec![]);
    let id1 = branch.add_commit(c1);

    let c2 = make_commit(b"second", vec![id1]);
    let id2 = branch.add_commit(c2);

    let c3 = make_commit(b"third", vec![id2]);
    let id3 = branch.add_commit(c3);

    assert!(branch.is_ancestor(&id1, &id1)); // self
    assert!(branch.is_ancestor(&id1, &id2));
    assert!(branch.is_ancestor(&id1, &id3));
    assert!(branch.is_ancestor(&id2, &id3));
    assert!(!branch.is_ancestor(&id3, &id1));
    assert!(!branch.is_ancestor(&id2, &id1));
}
