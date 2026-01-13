use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;

use crate::commit::{Commit, CommitId};
use crate::object::ObjectId;
use crate::sql::DescriptorId;
use crate::sql::row_buffer::{RowDescriptor, diff_columns};

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

/// Metadata about when a column was last changed.
///
/// This is tracked per-column for each frontier commit, enabling
/// efficient per-column LWW merge without LCA computation.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnChange {
    /// Timestamp when this column was last modified.
    pub timestamp: u64,
    /// Author who made the change.
    pub author: String,
}

/// Per-column change tracking for a commit.
///
/// Maps column name to its change metadata (when it was last changed and by whom).
/// This is maintained for all frontier commits and rebuilt on load/sync.
pub type ColumnChanges = HashMap<String, ColumnChange>;

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
    /// Per-frontier-commit column change metadata.
    ///
    /// Only maintained for current frontier commits (not all historical commits).
    /// For each frontier commit, tracks when each column was last changed and by whom.
    /// Rebuilt on load/sync by replaying history in topological order.
    pub(crate) frontier_changes: HashMap<CommitId, ColumnChanges>,
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
            frontier_changes: HashMap::new(),
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
            self.children.entry(*parent_id).or_default().push(id);
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
            self.children.entry(*parent_id).or_default().push(id);

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
        if let Some(existing) = &self.truncation
            && !self.is_ancestor(existing, &commit_id)
        {
            return Err(BranchError::InvalidTruncationPoint(commit_id));
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

    /// Get the column change metadata for a frontier commit.
    pub fn frontier_changes(&self) -> &HashMap<CommitId, ColumnChanges> {
        &self.frontier_changes
    }

    /// Add a commit and track per-column change metadata.
    ///
    /// This method:
    /// 1. Computes which columns changed vs parent(s) using diff_columns
    /// 2. For changed columns: records (timestamp, author) from this commit
    /// 3. For unchanged columns: inherits metadata from parent
    /// 4. Updates frontier_changes (removes old frontier metadata, adds new)
    ///
    /// The descriptor is needed to interpret the row buffer for diffing.
    pub fn add_commit_with_tracking(
        &mut self,
        commit: Commit,
        descriptor: &RowDescriptor,
    ) -> Result<CommitId, BranchError> {
        // Build the column changes metadata before adding the commit
        let mut changes = ColumnChanges::new();

        if commit.parents.is_empty() {
            // Root commit: all columns are "changed" by this commit
            for col in &descriptor.columns {
                changes.insert(
                    col.name.clone(),
                    ColumnChange {
                        timestamp: commit.timestamp,
                        author: commit.author.clone(),
                    },
                );
            }
        } else {
            // For commits with parents, we need to:
            // 1. Diff against parent(s) to find changed columns
            // 2. For changed columns: use this commit's (timestamp, author)
            // 3. For unchanged columns: inherit from parent

            // Collect all changed columns from all parents
            let mut all_changed_columns = HashSet::new();
            for parent_id in &commit.parents {
                if let Some(parent_commit) = self.commits.get(parent_id) {
                    let changed = diff_columns(&parent_commit.content, &commit.content, descriptor);
                    for col in changed {
                        all_changed_columns.insert(col);
                    }
                }
            }

            // For changed columns: use this commit's metadata
            for col_name in &all_changed_columns {
                changes.insert(
                    col_name.clone(),
                    ColumnChange {
                        timestamp: commit.timestamp,
                        author: commit.author.clone(),
                    },
                );
            }

            // For unchanged columns: inherit from parent with latest timestamp
            // (In case of multiple parents with differing metadata, pick the latest)
            for col in &descriptor.columns {
                if !all_changed_columns.contains(&col.name) {
                    // Find the parent with the most recent change for this column
                    let mut best_change: Option<ColumnChange> = None;

                    for parent_id in &commit.parents {
                        if let Some(parent_changes) = self.frontier_changes.get(parent_id)
                            && let Some(parent_col_change) = parent_changes.get(&col.name)
                        {
                            match &best_change {
                                None => {
                                    best_change = Some(parent_col_change.clone());
                                }
                                Some(current_best)
                                    if parent_col_change.timestamp > current_best.timestamp =>
                                {
                                    best_change = Some(parent_col_change.clone());
                                }
                                _ => {}
                            }
                        }
                    }

                    // If we found parent metadata, use it; otherwise this column hasn't been tracked
                    // (This can happen during partial rebuilds or schema changes)
                    if let Some(change) = best_change {
                        changes.insert(col.name.clone(), change);
                    }
                }
            }
        }

        // Store parents before adding (we need them for cleanup)
        let parents = commit.parents.clone();

        // Add the commit using existing method
        let commit_id = self.try_add_commit(commit)?;

        // Update frontier_changes:
        // - Remove metadata for parents that are no longer in frontier
        // - Add metadata for the new commit
        for parent_id in &parents {
            if !self.frontier.contains(parent_id) {
                self.frontier_changes.remove(parent_id);
            }
        }
        self.frontier_changes.insert(commit_id, changes);

        Ok(commit_id)
    }

    /// Rebuild column change metadata for all frontier commits.
    ///
    /// Called after loading from storage or receiving sync data.
    /// Replays history in topological order to reconstruct the per-column
    /// change tracking metadata for current frontier commits.
    pub fn rebuild_column_changes(&mut self, descriptor: &RowDescriptor) {
        self.frontier_changes.clear();

        if self.commits.is_empty() {
            return;
        }

        // We need to process commits in topological order (parents before children)
        // Build in-degree map for commits in this branch
        let mut in_degree: HashMap<CommitId, usize> = HashMap::new();
        for (id, commit) in &self.commits {
            in_degree.entry(*id).or_insert(0);
            for parent in &commit.parents {
                // Only count parents that are in our commit set
                if self.commits.contains_key(parent) {
                    *in_degree.entry(*id).or_insert(0) += 1;
                }
            }
        }

        // Kahn's algorithm: start with commits that have no in-branch parents
        let mut queue: VecDeque<CommitId> = in_degree
            .iter()
            .filter(|&(_, deg)| *deg == 0)
            .map(|(&id, _)| id)
            .collect();

        // Temporary storage for all changes (not just frontier)
        let mut all_changes: HashMap<CommitId, ColumnChanges> = HashMap::new();

        while let Some(commit_id) = queue.pop_front() {
            let commit = self.commits.get(&commit_id).unwrap();

            let mut changes = ColumnChanges::new();

            if commit.parents.is_empty()
                || commit.parents.iter().all(|p| !self.commits.contains_key(p))
            {
                // Root commit (or all parents are outside our truncated view):
                // all columns are "changed" by this commit
                for col in &descriptor.columns {
                    changes.insert(
                        col.name.clone(),
                        ColumnChange {
                            timestamp: commit.timestamp,
                            author: commit.author.clone(),
                        },
                    );
                }
            } else {
                // Compute changed columns vs parents
                let mut changed_columns = HashSet::new();
                for parent_id in &commit.parents {
                    if let Some(parent_commit) = self.commits.get(parent_id) {
                        let changed =
                            diff_columns(&parent_commit.content, &commit.content, descriptor);
                        for col in changed {
                            changed_columns.insert(col);
                        }
                    }
                }

                // For changed columns: use this commit's metadata
                for col_name in &changed_columns {
                    changes.insert(
                        col_name.clone(),
                        ColumnChange {
                            timestamp: commit.timestamp,
                            author: commit.author.clone(),
                        },
                    );
                }

                // For unchanged columns: inherit from parent with latest timestamp
                for col in &descriptor.columns {
                    if !changed_columns.contains(&col.name) {
                        let mut best_change: Option<ColumnChange> = None;

                        for parent_id in &commit.parents {
                            if let Some(parent_changes) = all_changes.get(parent_id)
                                && let Some(parent_col_change) = parent_changes.get(&col.name)
                            {
                                match &best_change {
                                    None => {
                                        best_change = Some(parent_col_change.clone());
                                    }
                                    Some(current_best)
                                        if parent_col_change.timestamp > current_best.timestamp =>
                                    {
                                        best_change = Some(parent_col_change.clone());
                                    }
                                    _ => {}
                                }
                            }
                        }

                        if let Some(change) = best_change {
                            changes.insert(col.name.clone(), change);
                        }
                    }
                }
            }

            all_changes.insert(commit_id, changes);

            // Process children
            if let Some(children) = self.children.get(&commit_id) {
                for &child_id in children {
                    if let Some(deg) = in_degree.get_mut(&child_id) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(child_id);
                        }
                    }
                }
            }
        }

        // Only keep metadata for frontier commits
        for &frontier_id in &self.frontier {
            if let Some(changes) = all_changes.remove(&frontier_id) {
                self.frontier_changes.insert(frontier_id, changes);
            }
        }
    }
}

// =============================================================================
// Schema-Aware Branch Naming
// =============================================================================

/// Default environment for local development.
pub const DEFAULT_ENV: &str = "dev";

/// Default user branch name.
pub const DEFAULT_USER_BRANCH: &str = "main";

/// A schema-aware branch name with the format `[env]-[schemaVersion]-[userBranch]`.
///
/// Examples:
/// - `prod-01JGXYZ123ABC456DEF789GHIJ-main` (production, full schema ID, main branch)
/// - `dev-01JGXYZ123ABC456DEF789GHIJ-feature-x` (development, full schema ID, feature-x branch)
/// - `main` (legacy format, interpreted as dev-<no-schema>-main)
///
/// The schema version is the full DescriptorId (26-char Crockford Base32 ObjectId).
///
/// TODO(GCO-1090): This struct provides the naming convention, but branch forking is not yet
/// wired to row operations. When a schema migration deploys, existing rows should be forked
/// to the new schema branch (applying lenses to transform data).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaBranchName {
    /// Environment (e.g., "dev", "staging", "prod")
    pub env: String,
    /// Schema version (full 26-char Crockford Base32 ObjectId)
    pub schema_version: String,
    /// User-visible branch name (e.g., "main", "feature-x")
    pub user_branch: String,
}

impl SchemaBranchName {
    /// Create a new schema branch name.
    pub fn new(
        env: impl Into<String>,
        schema_version: impl Into<String>,
        user_branch: impl Into<String>,
    ) -> Self {
        SchemaBranchName {
            env: env.into(),
            schema_version: schema_version.into(),
            user_branch: user_branch.into(),
        }
    }

    /// Create a schema branch name from a DescriptorId.
    pub fn from_descriptor(
        env: impl Into<String>,
        descriptor_id: &DescriptorId,
        user_branch: impl Into<String>,
    ) -> Self {
        SchemaBranchName {
            env: env.into(),
            schema_version: descriptor_id.to_string(),
            user_branch: user_branch.into(),
        }
    }

    /// Create the default branch for a given environment and schema.
    pub fn default_for_env(env: impl Into<String>, descriptor_id: &DescriptorId) -> Self {
        Self::from_descriptor(env, descriptor_id, DEFAULT_USER_BRANCH)
    }

    /// Parse a branch name string into a SchemaBranchName.
    ///
    /// Handles both new format (`env-schema-branch`) and legacy format (`branch`).
    /// Legacy format is interpreted as `dev-<empty>-<branch>`.
    pub fn parse(name: &str) -> Self {
        let parts: Vec<&str> = name.splitn(3, '-').collect();

        match parts.len() {
            3 => SchemaBranchName {
                env: parts[0].to_string(),
                schema_version: parts[1].to_string(),
                user_branch: parts[2].to_string(),
            },
            2 => {
                // Could be "env-branch" (no schema) or "schema-branch" (no env)
                // We assume "env-branch" format for backwards compat
                SchemaBranchName {
                    env: parts[0].to_string(),
                    schema_version: String::new(),
                    user_branch: parts[1].to_string(),
                }
            }
            _ => {
                // Legacy format: just a branch name
                SchemaBranchName {
                    env: DEFAULT_ENV.to_string(),
                    schema_version: String::new(),
                    user_branch: name.to_string(),
                }
            }
        }
    }

    /// Format as the full branch name string.
    fn format_name(&self) -> String {
        if self.schema_version.is_empty() {
            if self.env == DEFAULT_ENV {
                // Legacy format for backwards compat
                self.user_branch.clone()
            } else {
                format!("{}-{}", self.env, self.user_branch)
            }
        } else {
            format!("{}-{}-{}", self.env, self.schema_version, self.user_branch)
        }
    }

    /// Check if this is a legacy branch name (no schema version).
    pub fn is_legacy(&self) -> bool {
        self.schema_version.is_empty()
    }

    /// Get the descriptor ID for this branch's schema version.
    ///
    /// Returns None for legacy branches without a schema version.
    pub fn descriptor_id(&self) -> Option<DescriptorId> {
        if self.schema_version.is_empty() {
            return None;
        }
        ObjectId::from_str(&self.schema_version)
            .ok()
            .map(DescriptorId::from_object_id)
    }

    /// Create a new branch name for a different schema version.
    /// Preserves the environment and user branch.
    pub fn with_schema_version(&self, descriptor_id: &DescriptorId) -> Self {
        SchemaBranchName {
            env: self.env.clone(),
            schema_version: descriptor_id.to_string(),
            user_branch: self.user_branch.clone(),
        }
    }

    /// Create a new branch name for a different user branch.
    /// Preserves the environment and schema version.
    pub fn with_user_branch(&self, user_branch: impl Into<String>) -> Self {
        SchemaBranchName {
            env: self.env.clone(),
            schema_version: self.schema_version.clone(),
            user_branch: user_branch.into(),
        }
    }

    /// Create a new branch name for a different environment.
    /// Preserves the schema version and user branch.
    pub fn with_env(&self, env: impl Into<String>) -> Self {
        SchemaBranchName {
            env: env.into(),
            schema_version: self.schema_version.clone(),
            user_branch: self.user_branch.clone(),
        }
    }

    /// Check if this branch matches a given environment.
    pub fn matches_env(&self, env: &str) -> bool {
        self.env == env
    }

    /// Check if this branch matches a given schema version (prefix match).
    pub fn matches_schema(&self, schema_prefix: &str) -> bool {
        self.schema_version.starts_with(schema_prefix)
            || schema_prefix.starts_with(&self.schema_version)
    }
}

impl std::fmt::Display for SchemaBranchName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format_name())
    }
}

impl From<&str> for SchemaBranchName {
    fn from(s: &str) -> Self {
        Self::parse(s)
    }
}

impl From<String> for SchemaBranchName {
    fn from(s: String) -> Self {
        Self::parse(&s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_branch_name_new() {
        let name = SchemaBranchName::new("prod", "abc123def456", "main");
        assert_eq!(name.env, "prod");
        assert_eq!(name.schema_version, "abc123def456");
        assert_eq!(name.user_branch, "main");
    }

    #[test]
    fn test_schema_branch_name_from_descriptor() {
        use crate::object::ObjectId;
        let descriptor_id = DescriptorId::from_object_id(ObjectId::new(0x123456789abc));
        let name = SchemaBranchName::from_descriptor("prod", &descriptor_id, "main");
        assert_eq!(name.env, "prod");
        // Full 26-char Crockford Base32 ObjectId
        assert_eq!(name.schema_version.len(), 26);
        assert_eq!(name.user_branch, "main");
    }

    #[test]
    fn test_schema_branch_name_parse_full() {
        let name = SchemaBranchName::parse("prod-abc123-feature-x");
        assert_eq!(name.env, "prod");
        assert_eq!(name.schema_version, "abc123");
        assert_eq!(name.user_branch, "feature-x");
    }

    #[test]
    fn test_schema_branch_name_parse_legacy() {
        let name = SchemaBranchName::parse("main");
        assert_eq!(name.env, "dev");
        assert_eq!(name.schema_version, "");
        assert_eq!(name.user_branch, "main");
        assert!(name.is_legacy());
    }

    #[test]
    fn test_schema_branch_name_to_string_full() {
        let name = SchemaBranchName::new("prod", "abc123", "main");
        assert_eq!(name.to_string(), "prod-abc123-main");
    }

    #[test]
    fn test_schema_branch_name_to_string_legacy() {
        let name = SchemaBranchName::new("dev", "", "main");
        assert_eq!(name.to_string(), "main");
    }

    #[test]
    fn test_schema_branch_name_to_string_env_no_schema() {
        let name = SchemaBranchName::new("prod", "", "main");
        assert_eq!(name.to_string(), "prod-main");
    }

    #[test]
    fn test_schema_branch_name_roundtrip() {
        let original = SchemaBranchName::new("staging", "def456", "feature-branch");
        let serialized = original.to_string();
        let parsed = SchemaBranchName::parse(&serialized);
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_schema_branch_name_with_schema_version() {
        use crate::object::ObjectId;
        let name = SchemaBranchName::new("prod", "abc123", "main");
        let new_descriptor = DescriptorId::from_object_id(ObjectId::new(0xdef456789abc));
        let updated = name.with_schema_version(&new_descriptor);
        assert_eq!(updated.env, "prod");
        // Full 26-char Crockford Base32 ObjectId
        assert_eq!(updated.schema_version.len(), 26);
        assert_eq!(updated.user_branch, "main");
    }

    #[test]
    fn test_schema_branch_name_matches_env() {
        let name = SchemaBranchName::new("prod", "abc123", "main");
        assert!(name.matches_env("prod"));
        assert!(!name.matches_env("dev"));
    }

    #[test]
    fn test_schema_branch_name_matches_schema() {
        let name = SchemaBranchName::new("prod", "abc123def456", "main");
        assert!(name.matches_schema("abc123"));
        assert!(name.matches_schema("abc123def456"));
        assert!(!name.matches_schema("xyz"));
    }
}

// Tests have been moved to tests/branch.rs
