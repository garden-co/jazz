use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

/// A commit ID is the BLAKE3 hash of the commit's canonical representation.
/// Using full 256-bit hash for now (naive implementation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommitId([u8; 32]);

impl CommitId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        CommitId(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A commit in the object's history.
/// Contains a full snapshot of the object state (naive, uncompressed for now).
#[derive(Debug, Clone)]
pub struct Commit {
    /// Parent commit IDs (empty for root commits, multiple for merge commits)
    pub parents: Vec<CommitId>,
    /// Full snapshot of the object state
    pub content: Box<[u8]>,
    /// Author identifier (account/device ID)
    pub author: String,
    /// Timestamp (milliseconds since epoch)
    pub timestamp: u64,
    /// Optional metadata
    pub meta: Option<BTreeMap<String, String>>,
}

impl Commit {
    /// Compute the commit ID by hashing the commit's canonical representation.
    pub fn compute_id(&self) -> CommitId {
        let mut hasher = blake3::Hasher::new();

        // Hash parents
        hasher.update(&(self.parents.len() as u64).to_le_bytes());
        for parent in &self.parents {
            hasher.update(parent.as_bytes());
        }

        // Hash content
        hasher.update(&(self.content.len() as u64).to_le_bytes());
        hasher.update(&self.content);

        // Hash author
        hasher.update(&(self.author.len() as u64).to_le_bytes());
        hasher.update(self.author.as_bytes());

        // Hash timestamp
        hasher.update(&self.timestamp.to_le_bytes());

        // Hash metadata (simplified: just presence for now)
        hasher.update(&[self.meta.is_some() as u8]);
        if let Some(meta) = &self.meta {
            hasher.update(&(meta.len() as u64).to_le_bytes());
            for (k, v) in meta {
                hasher.update(&(k.len() as u64).to_le_bytes());
                hasher.update(k.as_bytes());
                hasher.update(&(v.len() as u64).to_le_bytes());
                hasher.update(v.as_bytes());
            }
        }

        CommitId(*hasher.finalize().as_bytes())
    }
}

/// A named branch within an object.
#[derive(Debug)]
pub struct Branch {
    /// Branch name (e.g., "main", "migration-v2")
    pub name: String,
    /// All commits in this branch, keyed by their ID
    commits: HashMap<CommitId, Commit>,
    /// Reverse index: parent -> children
    children: HashMap<CommitId, Vec<CommitId>>,
    /// Current frontier (tip commits with no children in this branch)
    frontier: Vec<CommitId>,
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
    fn ancestors(&self, id: &CommitId) -> HashSet<CommitId> {
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

/// Merge strategy for combining divergent branches.
pub trait MergeStrategy {
    /// Merge multiple tip contents into one.
    /// `base` is the LCA content (if any), `tips` are the divergent tip contents.
    fn merge(&self, base: Option<&[u8]>, tips: &[&[u8]]) -> Result<Box<[u8]>, &'static str>;
}

/// Simple "last writer wins" merge strategy based on timestamp.
pub struct LastWriterWins;

impl MergeStrategy for LastWriterWins {
    fn merge(&self, _base: Option<&[u8]>, tips: &[&[u8]]) -> Result<Box<[u8]>, &'static str> {
        // Just pick the last tip (caller should sort by timestamp)
        tips.last()
            .map(|t| t.to_vec().into_boxed_slice())
            .ok_or("no tips to merge")
    }
}

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

/// Generate a new UUIDv7 as u128.
pub fn generate_object_id() -> u128 {
    uuid::Uuid::now_v7().as_u128()
}

/// A local node managing multiple objects.
#[derive(Debug, Default)]
pub struct LocalNode {
    objects: BTreeMap<u128, Object>,
}

impl LocalNode {
    pub fn new() -> Self {
        LocalNode {
            objects: BTreeMap::new(),
        }
    }

    /// Create a new object with the given prefix. Returns the object ID.
    pub fn create_object(&mut self, prefix: impl Into<String>) -> u128 {
        let id = generate_object_id();
        let object = Object::new(id, prefix);
        self.objects.insert(id, object);
        id
    }

    /// Get an object by ID.
    pub fn get_object(&self, id: u128) -> Option<&Object> {
        self.objects.get(&id)
    }

    /// Get an object by ID mutably.
    pub fn get_object_mut(&mut self, id: u128) -> Option<&mut Object> {
        self.objects.get_mut(&id)
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
        let fake_id = CommitId([0; 32]);

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
    fn local_node_create_and_get_objects() {
        let mut node = LocalNode::new();

        let id1 = node.create_object("chat");
        let id2 = node.create_object("message");

        assert!(node.get_object(id1).is_some());
        assert!(node.get_object(id2).is_some());
        assert!(node.get_object(999).is_none());

        assert_eq!(node.get_object(id1).unwrap().prefix, "chat");
        assert_eq!(node.get_object(id2).unwrap().prefix, "message");
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

    #[test]
    fn uuidv7_is_unique_and_ordered() {
        let id1 = generate_object_id();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = generate_object_id();

        assert_ne!(id1, id2);
        // UUIDv7 should be roughly time-ordered
        assert!(id2 > id1);
    }

    #[test]
    fn local_node_uses_uuidv7() {
        let mut node = LocalNode::new();

        let id1 = node.create_object("test1");
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = node.create_object("test2");

        // IDs should be valid UUIDv7 (time-ordered)
        assert!(id2 > id1);

        // Should be large numbers (not sequential 1, 2, 3...)
        assert!(id1 > 1000);
    }
}
