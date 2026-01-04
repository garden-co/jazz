# Deletes and History Truncation

## Implementation Status

**Implemented:**
- Soft delete via metadata marker (`deleted=true`)
- History truncation at any commit that is an LCA of the frontier
- Automatic pruning of commits before truncation point from memory
- `DELETE FROM table WHERE ...` SQL statement (soft delete)
- `DELETE FROM table WHERE ... HARD` SQL statement (soft delete + truncate)
- Truncation validation (must be ancestor of all frontier commits)
- Rejection of commits with parents before truncation point

**Not yet implemented:**
- Distributed GC protocol for content cleanup after truncation
- More graceful handling of orphan commits (rebase instead of reject)
- Filtering deleted rows from SELECT queries (currently still visible)

## Overview

In a distributed commit-graph database, traditional "hard delete" is problematic because other nodes may have references to deleted data. This spec describes two complementary mechanisms:

1. **Soft Delete**: A commit that marks a row as deleted, preserving history and allowing undo
2. **History Truncation**: Pruning old commits from memory to reclaim resources

Combined, these enable a range of delete semantics from fully reversible to "as hard as possible in a distributed system."

## Soft Delete

### Representation

A delete is a regular commit with a metadata marker:

```rust
// In commit metadata:
meta.insert("deleted".to_string(), "true".to_string());
```

The commit content is empty (`[]`), and the `deleted` flag in metadata distinguishes this from a regular empty-content commit.

### Behavior

- A row is considered "deleted" when its latest frontier commit(s) have `meta["deleted"] = "true"`
- Creating a new commit with content (without the deleted marker) "undeletes" the row
- Delete commits participate in normal merge/conflict resolution

### Why Metadata?

- Uses existing Commit structure (no new types)
- Backward compatible with existing code
- Clear semantic meaning in the commit history
- Affects CommitId hash (delete is a distinct state from empty content)

### Example Timeline

```
c1 [content: "Alice"]
    ↓
c2 [content: "Alice Smith"]
    ↓
c3 [content: "", meta: {deleted: true}]  ← Row appears deleted
    ↓
c4 [content: "Alice Jones"]               ← Row undeleted with new content
```

## History Truncation

### Representation

Branch tracks an optional truncation point:

```rust
pub struct Branch {
    pub name: String,
    pub(crate) commits: HashMap<CommitId, Commit>,
    pub(crate) children: HashMap<CommitId, Vec<CommitId>>,
    pub(crate) frontier: Vec<CommitId>,
    pub(crate) truncation: Option<CommitId>,  // Truncation point
}
```

### Invariant

If `truncation` is set, it must be an ancestor of all frontier commits.

### Behavior

When `truncate_at(commit_id)` is called:

1. **Validation**: Verify commit exists and is an ancestor of all frontier tips
2. **Prune**: Remove all commits that are ancestors of the truncation point (but not the truncation point itself)
3. **Clear Parents**: The truncation point commit has its parents list cleared
4. **Reject Future Orphans**: Future commits with parents before the truncation point are rejected

### Pruning Details

```rust
impl Branch {
    pub fn truncate_at(&mut self, commit_id: CommitId) -> Result<usize, BranchError> {
        // 1. Verify commit exists
        if !self.commits.contains_key(&commit_id) {
            return Err(BranchError::CommitNotFound(commit_id));
        }

        // 2. Verify it's an ancestor of all frontier commits
        for tip in &self.frontier {
            if !self.is_ancestor(&commit_id, tip) {
                return Err(BranchError::InvalidTruncationPoint(commit_id));
            }
        }

        // 3. Collect commits to prune (ancestors of truncation point)
        let truncation_commit = self.commits.get(&commit_id).unwrap();
        let mut to_prune: HashSet<CommitId> = HashSet::new();
        // ... walk parents recursively

        // 4. Remove pruned commits from memory
        for id in &to_prune {
            self.commits.remove(id);
            self.children.remove(id);
        }

        // 5. Clear parents of truncation point
        if let Some(commit) = self.commits.get_mut(&commit_id) {
            commit.parents.clear();
        }

        self.truncation = Some(commit_id);
        Ok(to_prune.len())
    }
}
```

### Truncation Can Only Move Forward

Once truncation is set, it can only be moved to a descendant of the current truncation point:

```
Before: c1 -> c2 -> c3 -> c4 (frontier)
               ↑ truncation

After truncate_at(c3):
        c3 -> c4 (frontier)
        ↑ truncation (c1, c2 pruned from memory)

Cannot: truncate_at(c2) - c2 no longer exists
Cannot: truncate_at(c1) - c1 no longer exists
Can:    truncate_at(c4) - moves forward
```

## SQL Interface

### DELETE Statement

Two forms of DELETE are supported:

```sql
-- Soft delete: creates delete commit, preserves history
DELETE FROM users WHERE id = '0000000000000034NBSM938NKR';

-- Hard delete: soft delete + truncate at the delete commit
DELETE FROM users WHERE id = '0000000000000034NBSM938NKR' HARD;
```

### Parser Changes

```rust
pub struct Delete {
    pub table: String,
    pub where_clause: Vec<Condition>,
    pub hard: bool,  // true if HARD keyword present
}
```

### Execution

```rust
fn delete_impl(&self, table: &str, id: ObjectId, hard: bool) -> Result<bool, DatabaseError> {
    // Create delete metadata marker
    let mut meta = BTreeMap::new();
    meta.insert("deleted".to_string(), "true".to_string());

    // Write soft delete commit
    let commit_id = self.node.write_sync_with_meta(
        id, "main", &[], "system", timestamp, Some(meta)
    )?;

    // If hard delete, truncate history at the delete commit
    if hard {
        self.node.truncate_at(id, "main", commit_id)?;
    }

    Ok(true)
}
```

## Sync Considerations

### Truncation Convergence

When syncing two nodes with different truncation points:

1. Exchange (frontier, truncation) pairs
2. If truncation points differ:
   - If one is ancestor of the other → use the descendant (later truncation wins)
   - If neither is ancestor → conflict (shouldn't happen if truncation requires LCA)
3. Reject incoming commits whose parents are before local truncation

### Storage Interface

```rust
pub trait CommitStore {
    // ... existing methods ...

    async fn get_truncation(&self, object_id: u128, branch: &str) -> Option<CommitId>;
    async fn set_truncation(&self, object_id: u128, branch: &str, truncation: Option<CommitId>);
}
```

## Error Types

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum BranchError {
    /// The specified commit is not in this branch
    CommitNotFound(CommitId),
    /// The commit is not a valid truncation point (not an LCA of frontier)
    InvalidTruncationPoint(CommitId),
    /// A commit has parents that are before the truncation point
    ParentsBeforeTruncation,
}
```

## Edge Cases

### Delete Then Undelete Then Delete

Each operation creates a new commit; the latest frontier determines the current state:

```
c1 [content: "data"]
c2 [deleted: true]      ← deleted
c3 [content: "data"]    ← undeleted
c4 [deleted: true]      ← deleted again
```

### Merge of Deleted and Non-Deleted Branches

When one branch deletes and another modifies:

```
     c1
    /  \
   c2   c3 [deleted: true]
    \  /
     c4 (merge)
```

The merge strategy decides the outcome. With LastWriterWins based on timestamp, the later commit wins.

### Truncation at a Merge Commit

Valid if the merge commit is an LCA of the current frontier:

```
     c1
    /  \
   c2   c3
    \  /
     c4 (merge) ← Valid truncation point if frontier is [c4] or descendants
```

### Multiple Truncations

Each truncation must be at a descendant of the previous truncation point:

```
Initial:  c1 -> c2 -> c3 -> c4 -> c5 (frontier)

truncate_at(c2): c2 -> c3 -> c4 -> c5    (c1 pruned)
truncate_at(c4): c4 -> c5                 (c2, c3 pruned)
truncate_at(c3): ERROR - c3 no longer exists
```

## Future Work

1. **Graceful Orphan Handling**: Instead of rejecting commits with parents before truncation, consider rebasing them onto the truncation point

2. **Distributed GC Protocol**: Coordinate content cleanup across nodes after truncation convergence

3. **Query Filtering**: Automatically exclude deleted rows from SELECT results (currently they're still returned)

4. **Per-Table Truncation Policies**: Configure automatic truncation based on age or commit count
