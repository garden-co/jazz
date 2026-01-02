use std::collections::{BTreeMap, HashMap, HashSet};

use crate::branch::Branch;
use crate::commit::{Commit, CommitId};
use crate::merge::MergeStrategy;
use crate::storage::{ContentRef, ContentStore, INLINE_THRESHOLD};

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
        // Note: This only works with inline content. Chunked content would need async loading.
        let base_content: Option<Vec<u8>> = lca_commits
            .first()
            .and_then(|id| target.commits.get(id))
            .and_then(|c| c.content.as_inline().map(|b| b.to_vec()));

        // Collect tip contents (only inline supported for sync merge)
        let tip_contents: Vec<Vec<u8>> = all_tips
            .iter()
            .filter_map(|id| target.commits.get(id))
            .filter_map(|c| c.content.as_inline().map(|b| b.to_vec()))
            .collect();

        if tip_contents.len() != all_tips.len() {
            return Err("cannot merge: some commits have chunked content (use async merge)");
        }

        let tip_refs: Vec<&[u8]> = tip_contents.iter().map(|v| v.as_slice()).collect();

        // Perform merge
        let merged_content = strategy.merge(base_content.as_deref(), &tip_refs)?;

        // Create merge commit
        let merge_commit = Commit {
            parents: all_tips,
            content: ContentRef::inline(merged_content),
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

    // ========== Sync Read/Write Methods ==========

    /// Read content from the frontier of the main branch (sync).
    /// Returns None if the branch is empty or content is not inline.
    pub fn read_sync(&self) -> Option<&[u8]> {
        self.read_sync_branch("main")
    }

    /// Read content from the frontier of a specific branch (sync).
    /// Returns None if the branch is empty, has multiple tips, or content is not inline.
    pub fn read_sync_branch(&self, branch: &str) -> Option<&[u8]> {
        let branch = self.branches.get(branch)?;
        let frontier = branch.frontier();

        // Only return content if there's exactly one tip
        if frontier.len() != 1 {
            return None;
        }

        let commit = branch.get_commit(&frontier[0])?;
        commit.content.as_inline()
    }

    /// Write content to the main branch (sync).
    /// Panics if content exceeds INLINE_THRESHOLD.
    /// Returns the new commit ID.
    pub fn write_sync(
        &mut self,
        content: &[u8],
        author: &str,
        timestamp: u64,
    ) -> CommitId {
        self.write_sync_branch("main", content, author, timestamp)
    }

    /// Write content to a specific branch (sync).
    /// Panics if content exceeds INLINE_THRESHOLD.
    /// Returns the new commit ID.
    pub fn write_sync_branch(
        &mut self,
        branch_name: &str,
        content: &[u8],
        author: &str,
        timestamp: u64,
    ) -> CommitId {
        assert!(
            content.len() <= INLINE_THRESHOLD,
            "content exceeds INLINE_THRESHOLD ({} bytes), use write() for large content",
            INLINE_THRESHOLD
        );

        let branch = self
            .branches
            .get_mut(branch_name)
            .expect("branch not found");

        let parents = branch.frontier().to_vec();

        let commit = Commit {
            parents,
            content: ContentRef::inline(content.to_vec()),
            author: author.to_string(),
            timestamp,
            meta: None,
        };

        branch.add_commit(commit)
    }

    // ========== Async Read/Write Methods ==========

    /// Read content from the frontier of the main branch (async).
    /// Loads chunked content from storage if needed.
    pub async fn read(&self, store: &dyn ContentStore) -> Option<Vec<u8>> {
        self.read_branch("main", store).await
    }

    /// Read content from the frontier of a specific branch (async).
    /// Loads chunked content from storage if needed.
    pub async fn read_branch(&self, branch_name: &str, store: &dyn ContentStore) -> Option<Vec<u8>> {
        let branch = self.branches.get(branch_name)?;
        let frontier = branch.frontier();

        // Only return content if there's exactly one tip
        if frontier.len() != 1 {
            return None;
        }

        let commit = branch.get_commit(&frontier[0])?;

        match &commit.content {
            ContentRef::Inline(data) => Some(data.to_vec()),
            ContentRef::Chunked(hashes) => {
                // Load all chunks and concatenate
                let mut result = Vec::new();
                for hash in hashes {
                    let chunk = store.get_chunk(hash).await?;
                    result.extend_from_slice(&chunk);
                }
                Some(result)
            }
        }
    }

    /// Write content to the main branch (async).
    /// Automatically chunks content that exceeds INLINE_THRESHOLD.
    pub async fn write(
        &mut self,
        content: &[u8],
        author: &str,
        timestamp: u64,
        store: &dyn ContentStore,
    ) -> CommitId {
        self.write_branch("main", content, author, timestamp, store).await
    }

    /// Write content to a specific branch (async).
    /// Automatically chunks content that exceeds INLINE_THRESHOLD.
    pub async fn write_branch(
        &mut self,
        branch_name: &str,
        content: &[u8],
        author: &str,
        timestamp: u64,
        store: &dyn ContentStore,
    ) -> CommitId {
        let content_ref = if content.len() <= INLINE_THRESHOLD {
            ContentRef::inline(content.to_vec())
        } else {
            // For now, simple fixed-size chunking
            // TODO: Use FastCDC for content-defined chunking
            let mut hashes = Vec::new();
            for chunk in content.chunks(INLINE_THRESHOLD) {
                let hash = store.put_chunk(chunk.to_vec().into()).await;
                hashes.push(hash);
            }
            ContentRef::chunked(hashes)
        };

        let branch = self
            .branches
            .get_mut(branch_name)
            .expect("branch not found");

        let parents = branch.frontier().to_vec();

        let commit = Commit {
            parents,
            content: content_ref,
            author: author.to_string(),
            timestamp,
            meta: None,
        };

        branch.add_commit(commit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge::LastWriterWins;

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
            content: ContentRef::inline(b"main-change".to_vec()),
            author: "alice".to_string(),
            timestamp: 2000,
            meta: None,
        };
        obj.main_branch_mut().add_commit(c2);

        // Add commit to feature
        let c3 = Commit {
            parents: vec![id1],
            content: ContentRef::inline(b"feature-change".to_vec()),
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

    #[test]
    fn sync_write_and_read() {
        let mut obj = Object::new(1, "test");

        // Write some content
        let id = obj.write_sync(b"hello world", "alice", 1000);

        // Read it back
        let content = obj.read_sync().unwrap();
        assert_eq!(content, b"hello world");

        // Write more content
        obj.write_sync(b"updated", "alice", 2000);

        // Read the updated content
        let content = obj.read_sync().unwrap();
        assert_eq!(content, b"updated");

        // Verify commit chain
        assert_eq!(obj.main_branch().len(), 2);
        let latest = obj.main_branch().get_commit(&obj.main_branch().frontier()[0]).unwrap();
        assert_eq!(latest.parents.len(), 1);
        assert_eq!(latest.parents[0], id);
    }

    #[test]
    fn read_sync_empty_branch_returns_none() {
        let obj = Object::new(1, "test");
        assert!(obj.read_sync().is_none());
    }

    #[test]
    #[should_panic(expected = "content exceeds INLINE_THRESHOLD")]
    fn write_sync_panics_on_large_content() {
        let mut obj = Object::new(1, "test");
        let large_content = vec![0u8; INLINE_THRESHOLD + 1];
        obj.write_sync(&large_content, "alice", 1000);
    }

    // Async tests using futures executor
    use crate::storage::MemoryContentStore;
    use futures::executor::block_on;

    #[test]
    fn async_write_small_content() {
        let mut obj = Object::new(1, "test");
        let store = MemoryContentStore::new();

        // Write small content (should be inline)
        block_on(async {
            obj.write(b"hello", "alice", 1000, &store).await;
        });

        // Read back
        let content = obj.read_sync().unwrap();
        assert_eq!(content, b"hello");
    }

    #[test]
    fn async_write_large_content() {
        let mut obj = Object::new(1, "test");
        let store = MemoryContentStore::new();

        // Write large content (should be chunked)
        let large_content: Vec<u8> = (0..INLINE_THRESHOLD * 3)
            .map(|i| (i % 256) as u8)
            .collect();

        block_on(async {
            obj.write(&large_content, "alice", 1000, &store).await;
        });

        // Read sync should return None (content is chunked)
        assert!(obj.read_sync().is_none());

        // Read async should work
        let content = block_on(async {
            obj.read(&store).await
        });
        assert_eq!(content.unwrap(), large_content);
    }

    #[test]
    fn async_read_inline_content() {
        let mut obj = Object::new(1, "test");
        let store = MemoryContentStore::new();

        // Write using sync (inline)
        obj.write_sync(b"hello", "alice", 1000);

        // Read using async should also work
        let content = block_on(async {
            obj.read(&store).await
        });
        assert_eq!(content.unwrap(), b"hello");
    }
}
