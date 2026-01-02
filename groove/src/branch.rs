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

#[cfg(test)]
mod tests {
    use super::*;

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
}
