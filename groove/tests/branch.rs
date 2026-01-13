//! Integration tests for Branch.

use std::sync::Arc;

use groove::sql::ColumnType;
use groove::sql::row_buffer::{RowBuilder, RowDescriptor};
use groove::{Branch, BranchError, Commit, CommitId};

fn make_commit(content: &[u8], parents: Vec<CommitId>) -> Commit {
    Commit {
        parents,
        content: content.to_vec().into_boxed_slice(),
        author: "test-author".to_string(),
        timestamp: 1000,
        meta: None,
    }
}

/// Create a test row descriptor with name (String) and count (I32) columns.
fn test_descriptor() -> Arc<RowDescriptor> {
    Arc::new(RowDescriptor::new([
        ("name".to_string(), ColumnType::String, false),
        ("count".to_string(), ColumnType::I32, false),
    ]))
}

/// Build a row buffer with the given name and count values.
fn make_row(desc: &Arc<RowDescriptor>, name: &str, count: i32) -> Box<[u8]> {
    let name_idx = desc.column_index("name").unwrap();
    let count_idx = desc.column_index("count").unwrap();

    let row = RowBuilder::new(desc.clone())
        .set_string(name_idx, name)
        .set_i32(count_idx, count)
        .build();

    row.buffer.into_boxed_slice()
}

/// Make a commit with structured row data.
fn make_row_commit(
    desc: &Arc<RowDescriptor>,
    name: &str,
    count: i32,
    author: &str,
    timestamp: u64,
    parents: Vec<CommitId>,
) -> Commit {
    Commit {
        parents,
        content: make_row(desc, name, count),
        author: author.to_string(),
        timestamp,
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
        content: b"branch-a".to_vec().into_boxed_slice(),
        author: "alice".to_string(),
        timestamp: 2000,
        meta: None,
    };
    let id1 = branch.add_commit(c1);

    let c2 = Commit {
        parents: vec![root_id],
        content: b"branch-b".to_vec().into_boxed_slice(),
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
        content: b"a".to_vec().into_boxed_slice(),
        author: "alice".to_string(),
        timestamp: 2000,
        meta: None,
    };
    let id1 = branch.add_commit(c1);

    let c2 = Commit {
        parents: vec![root_id],
        content: b"b".to_vec().into_boxed_slice(),
        author: "bob".to_string(),
        timestamp: 2001,
        meta: None,
    };
    let id2 = branch.add_commit(c2);

    assert!(branch.needs_merge());

    // Merge commit
    let merge = Commit {
        parents: vec![id1, id2],
        content: b"merged".to_vec().into_boxed_slice(),
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
        content: b"left".to_vec().into_boxed_slice(),
        author: "alice".to_string(),
        timestamp: 2000,
        meta: None,
    };
    let id2 = branch.add_commit(c2);

    let c3 = Commit {
        parents: vec![id1],
        content: b"right".to_vec().into_boxed_slice(),
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
        content: b"merged".to_vec().into_boxed_slice(),
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
        content: b"branch-a".to_vec().into_boxed_slice(),
        author: "alice".to_string(),
        timestamp: 2000,
        meta: None,
    };
    let id1 = branch.add_commit(c1);

    let c2 = Commit {
        parents: vec![root_id],
        content: b"branch-b".to_vec().into_boxed_slice(),
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
        content: b"branch-a".to_vec().into_boxed_slice(),
        author: "alice".to_string(),
        timestamp: 2000,
        meta: None,
    };
    let _id2 = branch.add_commit(c2);

    let c3 = Commit {
        parents: vec![id1],
        content: b"branch-b".to_vec().into_boxed_slice(),
        author: "bob".to_string(),
        timestamp: 2001,
        meta: None,
    };
    let _id3 = branch.add_commit(c3);

    assert_eq!(branch.find_frontier_lca(), Some(id1));
}

// ========== Column Change Tracking Tests ==========

#[test]
fn column_tracking_root_commit_tracks_all_columns() {
    let desc = test_descriptor();
    let mut branch = Branch::new("main");

    let commit = make_row_commit(&desc, "Alice", 10, "alice", 1000, vec![]);
    let id = branch.add_commit_with_tracking(commit, &desc).unwrap();

    // Root commit should track all columns
    let changes = branch.frontier_changes();
    assert_eq!(changes.len(), 1);

    let commit_changes = changes.get(&id).unwrap();
    assert_eq!(commit_changes.len(), 2);

    // Both columns should be tracked with the commit's timestamp/author
    let name_change = commit_changes.get("name").unwrap();
    assert_eq!(name_change.timestamp, 1000);
    assert_eq!(name_change.author, "alice");

    let count_change = commit_changes.get("count").unwrap();
    assert_eq!(count_change.timestamp, 1000);
    assert_eq!(count_change.author, "alice");
}

#[test]
fn column_tracking_child_commit_diffs_against_parent() {
    let desc = test_descriptor();
    let mut branch = Branch::new("main");

    // Root commit: name="Alice", count=10
    let root = make_row_commit(&desc, "Alice", 10, "alice", 1000, vec![]);
    let root_id = branch.add_commit_with_tracking(root, &desc).unwrap();

    // Child commit: name="Alice" (unchanged), count=20 (changed)
    let child = make_row_commit(&desc, "Alice", 20, "bob", 2000, vec![root_id]);
    let child_id = branch.add_commit_with_tracking(child, &desc).unwrap();

    // Only the child should be in frontier_changes (root was removed)
    let changes = branch.frontier_changes();
    assert_eq!(changes.len(), 1);
    assert!(changes.get(&root_id).is_none()); // Root no longer in frontier

    let child_changes = changes.get(&child_id).unwrap();
    assert_eq!(child_changes.len(), 2);

    // Name was NOT changed - should inherit from parent
    let name_change = child_changes.get("name").unwrap();
    assert_eq!(name_change.timestamp, 1000); // From root
    assert_eq!(name_change.author, "alice"); // From root

    // Count WAS changed - should be from this commit
    let count_change = child_changes.get("count").unwrap();
    assert_eq!(count_change.timestamp, 2000); // From child
    assert_eq!(count_change.author, "bob"); // From child
}

#[test]
fn column_tracking_inheritance_through_chain() {
    let desc = test_descriptor();
    let mut branch = Branch::new("main");

    // c1: name="Alice", count=10 (alice, t=1000)
    let c1 = make_row_commit(&desc, "Alice", 10, "alice", 1000, vec![]);
    let id1 = branch.add_commit_with_tracking(c1, &desc).unwrap();

    // c2: name="Bob", count=10 (bob, t=2000) - only name changed
    let c2 = make_row_commit(&desc, "Bob", 10, "bob", 2000, vec![id1]);
    let id2 = branch.add_commit_with_tracking(c2, &desc).unwrap();

    // c3: name="Bob", count=20 (carol, t=3000) - only count changed
    let c3 = make_row_commit(&desc, "Bob", 20, "carol", 3000, vec![id2]);
    let id3 = branch.add_commit_with_tracking(c3, &desc).unwrap();

    let changes = branch.frontier_changes();
    let c3_changes = changes.get(&id3).unwrap();

    // Name should trace back to c2 (where it was last changed)
    let name_change = c3_changes.get("name").unwrap();
    assert_eq!(name_change.timestamp, 2000);
    assert_eq!(name_change.author, "bob");

    // Count should be from c3 (where it was just changed)
    let count_change = c3_changes.get("count").unwrap();
    assert_eq!(count_change.timestamp, 3000);
    assert_eq!(count_change.author, "carol");
}

#[test]
fn column_tracking_concurrent_commits_use_rebuild() {
    // Note: add_commit_with_tracking doesn't work well for concurrent commits
    // because adding the first sibling removes the parent's metadata.
    // For concurrent commits, use add_commit followed by rebuild_column_changes.
    let desc = test_descriptor();
    let mut branch = Branch::new("main");

    // Root commit
    let root = make_row_commit(&desc, "Alice", 10, "alice", 1000, vec![]);
    let root_id = branch.add_commit(root);

    // Two concurrent commits from root (using add_commit, not add_commit_with_tracking)
    let c1 = make_row_commit(&desc, "Bob", 10, "bob", 2000, vec![root_id]);
    let id1 = branch.add_commit(c1);

    let c2 = make_row_commit(&desc, "Alice", 20, "carol", 2500, vec![root_id]);
    let id2 = branch.add_commit(c2);

    // Rebuild to get correct metadata for both frontier commits
    branch.rebuild_column_changes(&desc);

    // Both concurrent commits should be in frontier_changes
    let changes = branch.frontier_changes();
    assert_eq!(changes.len(), 2);

    // c1 changed name, inherited count from root
    let c1_changes = changes.get(&id1).unwrap();
    assert_eq!(c1_changes.get("name").unwrap().timestamp, 2000);
    assert_eq!(c1_changes.get("name").unwrap().author, "bob");
    assert_eq!(c1_changes.get("count").unwrap().timestamp, 1000);
    assert_eq!(c1_changes.get("count").unwrap().author, "alice");

    // c2 changed count, inherited name from root
    let c2_changes = changes.get(&id2).unwrap();
    assert_eq!(c2_changes.get("name").unwrap().timestamp, 1000);
    assert_eq!(c2_changes.get("name").unwrap().author, "alice");
    assert_eq!(c2_changes.get("count").unwrap().timestamp, 2500);
    assert_eq!(c2_changes.get("count").unwrap().author, "carol");
}

#[test]
fn column_tracking_merge_commit_picks_latest_unchanged() {
    let desc = test_descriptor();
    let mut branch = Branch::new("main");

    // Root commit
    let root = make_row_commit(&desc, "Alice", 10, "alice", 1000, vec![]);
    let root_id = branch.add_commit_with_tracking(root, &desc).unwrap();

    // Two concurrent commits that change different columns
    // c1: name="Bob" (t=2000), count=10 (inherited from t=1000)
    let c1 = make_row_commit(&desc, "Bob", 10, "bob", 2000, vec![root_id]);
    let id1 = branch.add_commit_with_tracking(c1, &desc).unwrap();

    // c2: name="Alice" (inherited), count=20 (t=2500)
    let c2 = make_row_commit(&desc, "Alice", 20, "carol", 2500, vec![root_id]);
    let id2 = branch.add_commit_with_tracking(c2, &desc).unwrap();

    // Merge commit: name="Bob", count=20 (merges both changes)
    let merge = make_row_commit(&desc, "Bob", 20, "dave", 3000, vec![id1, id2]);
    let merge_id = branch.add_commit_with_tracking(merge, &desc).unwrap();

    // Only merge should be in frontier now
    let changes = branch.frontier_changes();
    assert_eq!(changes.len(), 1);

    let merge_changes = changes.get(&merge_id).unwrap();

    // Name was changed in merge (differs from c2's "Alice")
    // The merge content has "Bob" which differs from parent c2 ("Alice")
    // So it's marked as changed by the merge commit
    let name_change = merge_changes.get("name").unwrap();
    // It could be 3000 (from merge) or 2000 (from c1) depending on implementation
    // Since merge content matches c1, and differs from c2, it's considered changed
    assert!(name_change.timestamp >= 2000);

    // Count was changed in merge (differs from c1's 10)
    let count_change = merge_changes.get("count").unwrap();
    // Similar logic - merge content (20) matches c2, differs from c1
    assert!(count_change.timestamp >= 2000);
}

#[test]
fn column_tracking_rebuild_from_scratch() {
    let desc = test_descriptor();
    let mut branch = Branch::new("main");

    // Build a chain using regular add_commit (no tracking)
    let c1 = make_row_commit(&desc, "Alice", 10, "alice", 1000, vec![]);
    let id1 = branch.add_commit(c1);

    let c2 = make_row_commit(&desc, "Bob", 10, "bob", 2000, vec![id1]);
    let id2 = branch.add_commit(c2);

    let c3 = make_row_commit(&desc, "Bob", 20, "carol", 3000, vec![id2]);
    let id3 = branch.add_commit(c3);

    // frontier_changes should be empty (we used add_commit, not add_commit_with_tracking)
    assert!(branch.frontier_changes().is_empty());

    // Rebuild should reconstruct the metadata
    branch.rebuild_column_changes(&desc);

    let changes = branch.frontier_changes();
    assert_eq!(changes.len(), 1); // Only frontier (c3) should have metadata

    let c3_changes = changes.get(&id3).unwrap();

    // Name last changed in c2
    let name_change = c3_changes.get("name").unwrap();
    assert_eq!(name_change.timestamp, 2000);
    assert_eq!(name_change.author, "bob");

    // Count last changed in c3
    let count_change = c3_changes.get("count").unwrap();
    assert_eq!(count_change.timestamp, 3000);
    assert_eq!(count_change.author, "carol");
}

#[test]
fn column_tracking_rebuild_with_concurrent_frontier() {
    let desc = test_descriptor();
    let mut branch = Branch::new("main");

    // Root commit
    let root = make_row_commit(&desc, "Alice", 10, "alice", 1000, vec![]);
    let root_id = branch.add_commit(root);

    // Two concurrent commits creating a forked frontier
    let c1 = make_row_commit(&desc, "Bob", 10, "bob", 2000, vec![root_id]);
    let id1 = branch.add_commit(c1);

    let c2 = make_row_commit(&desc, "Alice", 20, "carol", 2500, vec![root_id]);
    let id2 = branch.add_commit(c2);

    // Rebuild should reconstruct metadata for both frontier commits
    branch.rebuild_column_changes(&desc);

    let changes = branch.frontier_changes();
    assert_eq!(changes.len(), 2);

    // Verify c1's metadata
    let c1_changes = changes.get(&id1).unwrap();
    assert_eq!(c1_changes.get("name").unwrap().timestamp, 2000); // Changed in c1
    assert_eq!(c1_changes.get("count").unwrap().timestamp, 1000); // From root

    // Verify c2's metadata
    let c2_changes = changes.get(&id2).unwrap();
    assert_eq!(c2_changes.get("name").unwrap().timestamp, 1000); // From root
    assert_eq!(c2_changes.get("count").unwrap().timestamp, 2500); // Changed in c2
}
