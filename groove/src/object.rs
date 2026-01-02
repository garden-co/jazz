use std::collections::{BTreeMap, HashMap, HashSet};

use crate::branch::Branch;
use crate::commit::{Commit, CommitId};
use crate::merge::MergeStrategy;

/// An object (CoValue) with its commit graph.
#[derive(Debug)]
pub struct Object {
    /// Unique object ID (UUIDv7)
    pub id: u128,
    /// Type prefix (e.g., "chat", "message")
    pub prefix: String,
    /// Named branches
    branches: HashMap<String, Branch>,
    /// Object-level metadata
    pub meta: Option<BTreeMap<String, String>>,
}

impl Object {
    /// Create a new object with the given ID and prefix.
    /// Automatically creates a "main" branch.
    pub fn new(id: u128, prefix: impl Into<String>) -> Self {
        let mut branches = HashMap::new();
        branches.insert("main".to_string(), Branch::new("main"));

        Object {
            id,
            prefix: prefix.into(),
            branches,
            meta: None,
        }
    }

    /// Get the main branch.
    pub fn main_branch(&self) -> &Branch {
        self.branches.get("main").expect("main branch always exists")
    }

    /// Get the main branch mutably.
    pub fn main_branch_mut(&mut self) -> &mut Branch {
        self.branches.get_mut("main").expect("main branch always exists")
    }

    /// Get a branch by name.
    pub fn branch(&self, name: &str) -> Option<&Branch> {
        self.branches.get(name)
    }

    /// Get a branch by name mutably.
    pub fn branch_mut(&mut self, name: &str) -> Option<&mut Branch> {
        self.branches.get_mut(name)
    }

    /// Create a new branch starting from a commit in an existing branch.
    /// Returns error if the source branch or commit doesn't exist.
    pub fn create_branch(
        &mut self,
        name: impl Into<String>,
        from_branch: &str,
        from_commit: &CommitId,
    ) -> Result<(), &'static str> {
        let name = name.into();

        if self.branches.contains_key(&name) {
            return Err("branch already exists");
        }

        let source = self.branches.get(from_branch).ok_or("source branch not found")?;

        if !source.commits.contains_key(from_commit) {
            return Err("commit not found in source branch");
        }

        // Create new branch with commits up to and including from_commit
        let mut new_branch = Branch::new(&name);

        // Copy all ancestors of from_commit (including itself)
        let mut to_copy = vec![*from_commit];
        let mut copied = HashSet::new();

        while let Some(id) = to_copy.pop() {
            if copied.contains(&id) {
                continue;
            }
            if let Some(commit) = source.commits.get(&id) {
                // Add parents to copy list
                for parent in &commit.parents {
                    if !copied.contains(parent) {
                        to_copy.push(*parent);
                    }
                }
                // Copy commit (re-add to build proper indices)
                new_branch.commits.insert(id, commit.clone());
                copied.insert(id);

                // Rebuild children index
                for parent in &commit.parents {
                    new_branch.children.entry(*parent).or_default().push(id);
                }
            }
        }

        // Set frontier to just the starting commit
        new_branch.frontier = vec![*from_commit];

        self.branches.insert(name, new_branch);
        Ok(())
    }

    /// List all branch names.
    pub fn branch_names(&self) -> Vec<&str> {
        self.branches.keys().map(|s| s.as_str()).collect()
    }

    /// Merge a source branch into a target branch.
    /// Creates a merge commit in the target branch that combines the tips of both.
    /// Returns the new merge commit ID.
    pub fn merge_branches(
        &mut self,
        target_branch: &str,
        source_branch: &str,
        strategy: &dyn MergeStrategy,
        author: &str,
        timestamp: u64,
    ) -> Result<CommitId, &'static str> {
        // Get source frontier
        let source_frontier: Vec<CommitId> = self
            .branches
            .get(source_branch)
            .ok_or("source branch not found")?
            .frontier()
            .to_vec();

        let source_commits: HashMap<CommitId, Commit> = self
            .branches
            .get(source_branch)
            .ok_or("source branch not found")?
            .commits()
            .clone();

        // Get target branch
        let target = self
            .branches
            .get_mut(target_branch)
            .ok_or("target branch not found")?;

        let target_frontier = target.frontier().to_vec();

        if target_frontier.is_empty() {
            return Err("target branch is empty");
        }
        if source_frontier.is_empty() {
            return Err("source branch is empty");
        }

        // First, copy any commits from source that aren't in target
        for (id, commit) in &source_commits {
            if !target.commits.contains_key(id) {
                target.commits.insert(*id, commit.clone());
                for parent in &commit.parents {
                    target.children.entry(*parent).or_default().push(*id);
                }
            }
        }

        // Collect all tips we need to merge
        let mut all_tips: Vec<CommitId> = target_frontier.clone();
        for tip in &source_frontier {
            if !all_tips.contains(tip) {
                all_tips.push(*tip);
            }
        }

        // If there's only one unique tip, nothing to merge
        if all_tips.len() == 1 {
            return Ok(all_tips[0]);
        }

        // Find LCA of first two tips, then extend
        let mut lca_commits = target.find_lca(&all_tips[0], &all_tips[1]);
        for tip in all_tips.iter().skip(2) {
            // Find LCA with each additional tip
            if let Some(first_lca) = lca_commits.first() {
                lca_commits = target.find_lca(first_lca, tip);
            }
        }

        // Get base content (from first LCA if exists)
        let base_content: Option<Box<[u8]>> = lca_commits
            .first()
            .and_then(|id| target.commits.get(id))
            .map(|c| c.content.clone());

        // Collect tip contents
        let tip_contents: Vec<&[u8]> = all_tips
            .iter()
            .filter_map(|id| target.commits.get(id))
            .map(|c| c.content.as_ref())
            .collect();

        // Perform merge
        let merged_content = strategy.merge(base_content.as_deref(), &tip_contents)?;

        // Create merge commit
        let merge_commit = Commit {
            parents: all_tips,
            content: merged_content,
            author: author.to_string(),
            timestamp,
            meta: None,
        };

        // Manually handle frontier update for merge
        let merge_id = merge_commit.compute_id();

        // Remove all merged tips from frontier
        for parent in &merge_commit.parents {
            target.frontier.retain(|f| f != parent);
            target.children.entry(*parent).or_default().push(merge_id);
        }

        target.frontier.push(merge_id);
        target.commits.insert(merge_id, merge_commit);

        Ok(merge_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge::LastWriterWins;

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
    fn object_has_main_branch() {
        let obj = Object::new(1, "test");

        assert_eq!(obj.branch_names(), vec!["main"]);
        assert!(obj.main_branch().is_empty());
    }

    #[test]
    fn object_create_branch() {
        let mut obj = Object::new(1, "test");

        // Add a commit to main
        let commit = make_commit(b"initial", vec![]);
        let commit_id = obj.main_branch_mut().add_commit(commit);

        // Create a feature branch from that commit
        obj.create_branch("feature", "main", &commit_id).unwrap();

        assert_eq!(obj.branch_names().len(), 2);
        assert!(obj.branch_names().contains(&"main"));
        assert!(obj.branch_names().contains(&"feature"));

        // Feature branch should have the same commit
        let feature = obj.branch("feature").unwrap();
        assert_eq!(feature.len(), 1);
        assert!(feature.get_commit(&commit_id).is_some());
    }

    #[test]
    fn object_branch_errors() {
        let mut obj = Object::new(1, "test");
        let fake_id = CommitId::from_bytes([0; 32]);

        // Can't create branch from non-existent commit
        assert!(obj.create_branch("feature", "main", &fake_id).is_err());

        // Can't create branch from non-existent source branch
        assert!(obj.create_branch("feature", "nonexistent", &fake_id).is_err());

        // Add a commit and create a branch
        let commit = make_commit(b"test", vec![]);
        let id = obj.main_branch_mut().add_commit(commit);
        obj.create_branch("feature", "main", &id).unwrap();

        // Can't create duplicate branch
        assert!(obj.create_branch("feature", "main", &id).is_err());
    }

    #[test]
    fn merge_branches_simple() {
        let mut obj = Object::new(1, "test");

        // Add initial commit to main
        let c1 = make_commit(b"initial", vec![]);
        let id1 = obj.main_branch_mut().add_commit(c1);

        // Create feature branch
        obj.create_branch("feature", "main", &id1).unwrap();

        // Add commit to main
        let c2 = Commit {
            parents: vec![id1],
            content: b"main-change".to_vec().into_boxed_slice(),
            author: "alice".to_string(),
            timestamp: 2000,
            meta: None,
        };
        obj.main_branch_mut().add_commit(c2);

        // Add commit to feature
        let c3 = Commit {
            parents: vec![id1],
            content: b"feature-change".to_vec().into_boxed_slice(),
            author: "bob".to_string(),
            timestamp: 2001,
            meta: None,
        };
        obj.branch_mut("feature").unwrap().add_commit(c3);

        // Merge feature into main
        let strategy = LastWriterWins;
        let merge_id = obj
            .merge_branches("main", "feature", &strategy, "alice", 3000)
            .unwrap();

        // Main should now have single tip (the merge commit)
        assert!(!obj.main_branch().needs_merge());
        assert_eq!(obj.main_branch().frontier(), &[merge_id]);

        // Merge commit should have 2 parents
        let merge_commit = obj.main_branch().get_commit(&merge_id).unwrap();
        assert_eq!(merge_commit.parents.len(), 2);
    }
}
