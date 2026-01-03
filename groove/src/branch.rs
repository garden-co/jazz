use std::collections::{HashMap, HashSet, VecDeque};

use crate::commit::{Commit, CommitId};

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
}

impl Branch {
    /// Create a new empty branch with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Branch {
            name: name.into(),
            commits: HashMap::new(),
            children: HashMap::new(),
            frontier: Vec::new(),
        }
    }

    /// Add a commit to this branch. Returns the commit ID.
    /// Updates frontier: removes parents from frontier, adds new commit if it has no children.
    pub fn add_commit(&mut self, commit: Commit) -> CommitId {
        let id = commit.compute_id();

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

        id
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
    pub(crate) fn ancestors(&self, id: &CommitId) -> HashSet<CommitId> {
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
}

// Tests have been moved to tests/branch.rs
