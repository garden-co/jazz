//! Integration tests for Branch.

use groove::{Branch, BranchError, Commit, CommitId, ContentRef};

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

// ========== Truncation Tests ==========

#[test]
fn truncate_at_single_commit() {
    let mut branch = Branch::new("main");

    let c1 = make_commit(b"first", vec![]);
    let id1 = branch.add_commit(c1);

    let c2 = make_commit(b"second", vec![id1]);
    let id2 = branch.add_commit(c2);

    // Truncate at first commit
    let result = branch.truncate_at(id1);
    assert!(result.is_ok());
    assert_eq!(branch.truncation(), Some(id1));

    // Can still add commit with parent after truncation point
    let c3 = make_commit(b"third", vec![id2]);
    let id3 = branch.try_add_commit(c3);
    assert!(id3.is_ok());
}

#[test]
fn truncate_at_nonexistent_commit() {
    let mut branch = Branch::new("main");

    let c1 = make_commit(b"first", vec![]);
    branch.add_commit(c1);

    // Try to truncate at nonexistent commit
    let fake_id = CommitId::from_bytes([0u8; 32]);
    let result = branch.truncate_at(fake_id);
    assert_eq!(result, Err(BranchError::CommitNotFound(fake_id)));
}

#[test]
fn truncate_rejects_non_ancestor_of_frontier() {
    let mut branch = Branch::new("main");

    // Create two divergent branches
    let root = make_commit(b"root", vec![]);
    let root_id = branch.add_commit(root);

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
    let _id2 = branch.add_commit(c2);

    // Now branch has two tips; can truncate at root (ancestor of both)
    let result = branch.truncate_at(root_id);
    assert!(result.is_ok());

    // Cannot truncate at id1 (not ancestor of id2)
    let result = branch.truncate_at(id1);
    assert_eq!(result, Err(BranchError::InvalidTruncationPoint(id1)));
}

#[test]
fn add_commit_rejects_pre_truncation_parents() {
    let mut branch = Branch::new("main");

    let c1 = make_commit(b"first", vec![]);
    let id1 = branch.add_commit(c1);

    let c2 = make_commit(b"second", vec![id1]);
    let id2 = branch.add_commit(c2);

    let c3 = make_commit(b"third", vec![id2]);
    let id3 = branch.add_commit(c3);

    // Truncate at second commit
    branch.truncate_at(id2).unwrap();

    // Try to add commit with parent before truncation
    let c4 = make_commit(b"invalid", vec![id1]);
    let result = branch.try_add_commit(c4);
    assert_eq!(result, Err(BranchError::ParentsBeforeTruncation));

    // But can add commit with parent at or after truncation
    let c5 = make_commit(b"valid-from-truncation", vec![id2]);
    let result = branch.try_add_commit(c5);
    assert!(result.is_ok());

    let c6 = make_commit(b"valid-from-after", vec![id3]);
    let result = branch.try_add_commit(c6);
    assert!(result.is_ok());
}

#[test]
fn truncation_can_only_move_forward() {
    let mut branch = Branch::new("main");

    let c1 = make_commit(b"first", vec![]);
    let id1 = branch.add_commit(c1);

    let c2 = make_commit(b"second", vec![id1]);
    let id2 = branch.add_commit(c2);

    let c3 = make_commit(b"third", vec![id2]);
    let id3 = branch.add_commit(c3);

    let c4 = make_commit(b"fourth", vec![id3]);
    let id4 = branch.add_commit(c4);

    assert_eq!(branch.len(), 4);

    // Truncate at second commit - prunes id1
    let pruned = branch.truncate_at(id2).unwrap();
    assert_eq!(pruned, 1);
    assert_eq!(branch.truncation(), Some(id2));
    assert_eq!(branch.len(), 3); // id2, id3, id4 remain

    // Can move forward - prunes id2
    let pruned = branch.truncate_at(id3).unwrap();
    assert_eq!(pruned, 1);
    assert_eq!(branch.truncation(), Some(id3));
    assert_eq!(branch.len(), 2); // id3, id4 remain

    // Cannot move backward - id1 was pruned, so CommitNotFound
    let result = branch.truncate_at(id1);
    assert_eq!(result, Err(BranchError::CommitNotFound(id1)));

    // Cannot even move back to the old truncation point (it was pruned)
    let result = branch.truncate_at(id2);
    assert_eq!(result, Err(BranchError::CommitNotFound(id2)));

    // Can still move forward
    let pruned = branch.truncate_at(id4).unwrap();
    assert_eq!(pruned, 1);
    assert_eq!(branch.len(), 1); // only id4 remains
}

#[test]
fn truncation_prunes_commits_from_memory() {
    let mut branch = Branch::new("main");

    // Create a chain: c1 -> c2 -> c3 -> c4 -> c5
    let c1 = make_commit(b"first", vec![]);
    let id1 = branch.add_commit(c1);

    let c2 = make_commit(b"second", vec![id1]);
    let id2 = branch.add_commit(c2);

    let c3 = make_commit(b"third", vec![id2]);
    let id3 = branch.add_commit(c3);

    let c4 = make_commit(b"fourth", vec![id3]);
    let id4 = branch.add_commit(c4);

    let c5 = make_commit(b"fifth", vec![id4]);
    let id5 = branch.add_commit(c5);

    assert_eq!(branch.len(), 5);

    // All commits exist
    assert!(branch.get_commit(&id1).is_some());
    assert!(branch.get_commit(&id2).is_some());
    assert!(branch.get_commit(&id3).is_some());
    assert!(branch.get_commit(&id4).is_some());
    assert!(branch.get_commit(&id5).is_some());

    // Truncate at id3 - should prune id1 and id2
    let pruned = branch.truncate_at(id3).unwrap();
    assert_eq!(pruned, 2);
    assert_eq!(branch.len(), 3);

    // id1 and id2 no longer exist
    assert!(branch.get_commit(&id1).is_none());
    assert!(branch.get_commit(&id2).is_none());

    // id3, id4, id5 still exist
    assert!(branch.get_commit(&id3).is_some());
    assert!(branch.get_commit(&id4).is_some());
    assert!(branch.get_commit(&id5).is_some());

    // id3 now has no parents (they were pruned)
    let truncation_commit = branch.get_commit(&id3).unwrap();
    assert!(truncation_commit.parents.is_empty());

    // Frontier still works
    assert_eq!(branch.frontier(), &[id5]);
}

#[test]
fn find_frontier_lca() {
    let mut branch = Branch::new("main");

    // Empty branch has no LCA
    assert_eq!(branch.find_frontier_lca(), None);

    // Single commit: LCA is that commit
    let c1 = make_commit(b"root", vec![]);
    let id1 = branch.add_commit(c1);
    assert_eq!(branch.find_frontier_lca(), Some(id1));

    // Two divergent branches: LCA is the common ancestor
    let c2 = Commit {
        parents: vec![id1],
        content: ContentRef::inline(b"branch-a".to_vec()),
        author: "alice".to_string(),
        timestamp: 2000,
        meta: None,
    };
    let _id2 = branch.add_commit(c2);

    let c3 = Commit {
        parents: vec![id1],
        content: ContentRef::inline(b"branch-b".to_vec()),
        author: "bob".to_string(),
        timestamp: 2001,
        meta: None,
    };
    let _id3 = branch.add_commit(c3);

    assert_eq!(branch.find_frontier_lca(), Some(id1));
}
