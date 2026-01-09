use std::collections::{HashMap, HashSet, VecDeque};

use crate::commit::{Commit, CommitId};

/// Error type for branch operations.
#[derive(Debug, Clone, PartialEq)]
pub enum BranchError {
    /// The specified commit is not in this branch.
    CommitNotFound(CommitId),
    /// The commit is not a valid truncation point (not an LCA of frontier).
    InvalidTruncationPoint(CommitId),
    /// A commit has parents that are before the truncation point.
    ParentsBeforeTruncation,
}

impl std::fmt::Display for BranchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BranchError::CommitNotFound(id) => write!(f, "commit not found: {:?}", id),
            BranchError::InvalidTruncationPoint(id) => {
                write!(f, "invalid truncation point: {:?}", id)
            }
            BranchError::ParentsBeforeTruncation => {
                write!(f, "commit has parents before truncation point")
            }
        }
    }
}

impl std::error::Error for BranchError {}

/// A named branch within an object.
#[derive(Debug)]
pub struct Branch {
    /// Branch name (e.g., "main", "migration-v2")
    pub name: String,
    /// All commits in this branch, keyed by their ID
    pub(crate) commits: HashMap<CommitId, Commit>,
    /// Reverse index: parent -> children
    pub(crate) children: HashMap<CommitId, Vec<CommitId>>,
    /// Current frontier (tip commits with no children in this branch)
    pub(crate) frontier: Vec<CommitId>,
    /// Truncation point: if set, commits with parents before this point are rejected.
    /// The truncation point itself is kept; commits before it are considered pruned.
    pub(crate) truncation: Option<CommitId>,
}

impl Branch {
    /// Create a new empty branch with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Branch {
            name: name.into(),
            commits: HashMap::new(),
            children: HashMap::new(),
            frontier: Vec::new(),
            truncation: None,
        }
    }

    /// Add a commit to this branch. Returns the commit ID.
    /// Updates frontier: removes parents from frontier, adds new commit if it has no children.
    ///
    /// # Panics
    /// Panics if the commit has parents before the truncation point.
    /// Use `try_add_commit` for a fallible version.
    pub fn add_commit(&mut self, commit: Commit) -> CommitId {
        self.try_add_commit(commit)
            .expect("commit has parents before truncation point")
    }

    /// Restore a commit to this branch without updating frontier.
    /// Used for loading commits from storage where frontier will be set separately.
    pub fn restore_commit(&mut self, commit: Commit) -> CommitId {
        let id = commit.compute_id();

        // Update parent->child relationships
        for parent_id in &commit.parents {
            self.children
                .entry(*parent_id)
                .or_default()
                .push(id);
        }

        self.commits.insert(id, commit);
        id
    }

    /// Set the frontier explicitly. Used for restoring from storage.
    pub fn set_frontier(&mut self, frontier: Vec<CommitId>) {
        self.frontier = frontier;
    }

    /// Try to add a commit to this branch. Returns the commit ID or an error.
    /// Updates frontier: removes parents from frontier, adds new commit if it has no children.
    ///
    /// Returns an error if any parent is before the truncation point.
    pub fn try_add_commit(&mut self, commit: Commit) -> Result<CommitId, BranchError> {
        let id = commit.compute_id();

        // Validate parents against truncation point
        if let Some(truncation) = &self.truncation {
            for parent_id in &commit.parents {
                // Parent must be either:
                // 1. The truncation point itself, OR
                // 2. A descendant of the truncation point (in our commit set and reachable)
                if parent_id != truncation && !self.is_ancestor(truncation, parent_id) {
                    return Err(BranchError::ParentsBeforeTruncation);
                }
            }
        }

        // Update children index for each parent
        for parent_id in &commit.parents {
            self.children
                .entry(*parent_id)
                .or_default()
                .push(id);

            // Remove parent from frontier (it now has a child)
            self.frontier.retain(|f| f != parent_id);
        }

        // Add to frontier (new commit has no children yet)
        self.frontier.push(id);

        // Store the commit
        self.commits.insert(id, commit);

        Ok(id)
    }

    /// Get a commit by its ID.
    pub fn get_commit(&self, id: &CommitId) -> Option<&Commit> {
        self.commits.get(id)
    }

    /// Get the current frontier (tip commits).
    pub fn frontier(&self) -> &[CommitId] {
        &self.frontier
    }

    /// Check if the branch has multiple tips (needs merge).
    pub fn needs_merge(&self) -> bool {
        self.frontier.len() > 1
    }

    /// Get all commits in the branch.
    pub fn commits(&self) -> &HashMap<CommitId, Commit> {
        &self.commits
    }

    /// Get children of a commit.
    pub fn children_of(&self, id: &CommitId) -> Option<&Vec<CommitId>> {
        self.children.get(id)
    }

    /// Check if the branch is empty.
    pub fn is_empty(&self) -> bool {
        self.commits.is_empty()
    }

    /// Get the number of commits.
    pub fn len(&self) -> usize {
        self.commits.len()
    }

    /// Find the Lowest Common Ancestors (LCA) of two commits.
    /// Returns all commits that are ancestors of both and have no descendants
    /// that are also common ancestors.
    pub fn find_lca(&self, a: &CommitId, b: &CommitId) -> Vec<CommitId> {
        if a == b {
            return vec![*a];
        }

        // Get all ancestors of each commit (including themselves)
        let ancestors_a = self.ancestors(a);
        let ancestors_b = self.ancestors(b);

        // Find common ancestors
        let common: HashSet<_> = ancestors_a.intersection(&ancestors_b).copied().collect();

        if common.is_empty() {
            return vec![];
        }

        // Filter to only those with no descendants that are also common ancestors
        // (i.e., find the "lowest" common ancestors)
        // A commit is an LCA if no other common ancestor is a descendant of it
        let mut lca = Vec::new();
        for &candidate in &common {
            // Check if any other common ancestor has this candidate as an ancestor
            // (meaning that other commit is "lower" / closer to the tips)
            let dominated = common.iter().any(|&other| {
                if other == candidate {
                    return false;
                }
                // other dominates candidate if candidate is an ancestor of other
                self.ancestors(&other).contains(&candidate)
            });
            if !dominated {
                lca.push(candidate);
            }
        }

        lca
    }

    /// Get all ancestors of a commit (including itself).
    pub fn ancestors(&self, id: &CommitId) -> HashSet<CommitId> {
        let mut result = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(*id);

        while let Some(current) = queue.pop_front() {
            if result.contains(&current) {
                continue;
            }
            result.insert(current);

            if let Some(commit) = self.commits.get(&current) {
                for parent in &commit.parents {
                    if !result.contains(parent) {
                        queue.push_back(*parent);
                    }
                }
            }
        }

        result
    }

    /// Check if commit `ancestor` is an ancestor of commit `descendant`.
    pub fn is_ancestor(&self, ancestor: &CommitId, descendant: &CommitId) -> bool {
        if ancestor == descendant {
            return true;
        }
        self.ancestors(descendant).contains(ancestor)
    }

    /// Get the current truncation point, if any.
    pub fn truncation(&self) -> Option<CommitId> {
        self.truncation
    }

    /// Set the truncation point and prune all commits before it.
    ///
    /// The truncation point must be an ancestor of all frontier commits.
    /// After truncation:
    /// - All commits before the truncation point are removed from memory
    /// - The truncation point commit is kept but its parents are cleared
    /// - Future commits with parents before the truncation point will be rejected
    ///
    /// Returns the number of commits pruned, or an error if invalid.
    pub fn truncate_at(&mut self, commit_id: CommitId) -> Result<usize, BranchError> {
        // Verify the commit exists
        if !self.commits.contains_key(&commit_id) {
            return Err(BranchError::CommitNotFound(commit_id));
        }

        // Verify commit is an ancestor of all frontier commits
        for tip in &self.frontier {
            if !self.is_ancestor(&commit_id, tip) {
                return Err(BranchError::InvalidTruncationPoint(commit_id));
            }
        }

        // If there's an existing truncation point, the new one must be a descendant of it
        // (i.e., we can only move truncation forward, not backward)
        if let Some(existing) = &self.truncation {
            if !self.is_ancestor(existing, &commit_id) {
                return Err(BranchError::InvalidTruncationPoint(commit_id));
            }
        }

        // Find all commits to prune (ancestors of truncation point, excluding itself)
        let truncation_commit = self.commits.get(&commit_id).unwrap();
        let mut to_prune: HashSet<CommitId> = HashSet::new();
        let mut queue: VecDeque<CommitId> = truncation_commit.parents.iter().copied().collect();

        while let Some(id) = queue.pop_front() {
            if to_prune.contains(&id) {
                continue;
            }
            if let Some(commit) = self.commits.get(&id) {
                to_prune.insert(id);
                for parent in &commit.parents {
                    queue.push_back(*parent);
                }
            }
        }

        // Remove pruned commits
        let pruned_count = to_prune.len();
        for id in &to_prune {
            self.commits.remove(id);
            self.children.remove(id);
        }

        // Clear parents of the truncation point commit (they're now pruned)
        if let Some(commit) = self.commits.get_mut(&commit_id) {
            commit.parents.clear();
        }

        // Clean up children index for pruned commits
        for children_list in self.children.values_mut() {
            children_list.retain(|id| !to_prune.contains(id));
        }

        self.truncation = Some(commit_id);
        Ok(pruned_count)
    }

    /// Find the LCA of all frontier commits.
    /// Returns None if the frontier is empty.
    /// Returns a single commit if there's only one tip or all tips share a single LCA.
    pub fn find_frontier_lca(&self) -> Option<CommitId> {
        if self.frontier.is_empty() {
            return None;
        }
        if self.frontier.len() == 1 {
            return Some(self.frontier[0]);
        }

        // Find LCA of first two tips
        let mut lca_set: HashSet<CommitId> = self
            .find_lca(&self.frontier[0], &self.frontier[1])
            .into_iter()
            .collect();

        // Intersect with LCAs of remaining tips
        for tip in &self.frontier[2..] {
            let new_lca_set: HashSet<CommitId> = lca_set
                .iter()
                .flat_map(|lca| self.find_lca(lca, tip))
                .collect();
            lca_set = new_lca_set;
        }

        // Return the "lowest" LCA (the one that is a descendant of all others)
        // If there are multiple, we pick one arbitrarily (shouldn't happen with proper DAG)
        lca_set.into_iter().next()
    }

    /// Set truncation directly (for deserialization/sync).
    /// Does NOT validate - use truncate_at() for validated truncation.
    pub fn set_truncation(&mut self, truncation: Option<CommitId>) {
        self.truncation = truncation;
    }
}

// Tests have been moved to tests/branch.rs
