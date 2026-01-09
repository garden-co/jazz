//! Sync negotiation logic for determining which commits to exchange.
//!
//! The sync negotiation process compares frontiers between peers to determine
//! which commits need to be sent. The algorithm handles three cases:
//! - Server ahead: server sends missing commits to client
//! - Client ahead: client sends missing commits to server
//! - Diverged: both sides exchange commits, resulting in multi-tip frontier

use std::collections::{HashSet, VecDeque};

use crate::branch::Branch;
use crate::commit::{Commit, CommitId};

/// Result of comparing two frontiers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontierComparison {
    /// Frontiers are identical - no sync needed
    Identical,
    /// Local is ahead - local has commits remote doesn't have
    LocalAhead,
    /// Remote is ahead - remote has commits local doesn't have
    RemoteAhead,
    /// Diverged - both sides have unique commits
    Diverged,
}

/// Compare two frontiers to determine their relationship.
///
/// This is a quick check that doesn't require access to the full commit graph.
/// It only tells you IF sync is needed, not what commits to exchange.
pub fn compare_frontiers(local_frontier: &[CommitId], remote_frontier: &[CommitId]) -> FrontierComparison {
    let local_set: HashSet<_> = local_frontier.iter().collect();
    let remote_set: HashSet<_> = remote_frontier.iter().collect();

    if local_set == remote_set {
        return FrontierComparison::Identical;
    }

    let local_has_remote = remote_frontier.iter().all(|id| local_set.contains(id));
    let remote_has_local = local_frontier.iter().all(|id| remote_set.contains(id));

    match (local_has_remote, remote_has_local) {
        (true, false) => FrontierComparison::LocalAhead,
        (false, true) => FrontierComparison::RemoteAhead,
        _ => FrontierComparison::Diverged,
    }
}

/// Find commits that are in the local branch but not known to the remote peer.
///
/// Given the local branch and the remote's known frontier, returns all commits
/// that need to be sent, in topological order (parents before children).
///
/// # Arguments
/// * `branch` - The local branch containing all commits
/// * `local_frontier` - The local frontier (tip commits)
/// * `remote_frontier` - The remote's frontier (what they claim to have)
///
/// # Returns
/// Commits to send, topologically sorted (parents first).
pub fn commits_to_send(
    branch: &Branch,
    local_frontier: &[CommitId],
    remote_frontier: &[CommitId],
) -> Vec<Commit> {
    // If frontiers are identical, nothing to send
    if compare_frontiers(local_frontier, remote_frontier) == FrontierComparison::Identical {
        return vec![];
    }

    // Get all ancestors of remote frontier (commits they already have)
    let remote_known: HashSet<CommitId> = remote_frontier
        .iter()
        .flat_map(|id| branch.ancestors(id))
        .collect();

    // Walk from local frontier, collecting commits not in remote_known
    let mut to_send = Vec::new();
    let mut visited = HashSet::new();
    let mut queue: VecDeque<CommitId> = local_frontier.iter().copied().collect();

    while let Some(id) = queue.pop_front() {
        if visited.contains(&id) || remote_known.contains(&id) {
            continue;
        }
        visited.insert(id);

        if let Some(commit) = branch.get_commit(&id) {
            to_send.push((id, commit.clone()));

            // Add parents to queue
            for parent in &commit.parents {
                if !visited.contains(parent) && !remote_known.contains(parent) {
                    queue.push_back(*parent);
                }
            }
        }
    }

    // Topologically sort: parents before children
    topological_sort(to_send)
}

/// Find commits that we need but don't have, given the remote's frontier.
///
/// Returns commit IDs that we should request from the remote.
///
/// # Arguments
/// * `branch` - The local branch (may not have all commits)
/// * `local_frontier` - Our current frontier
/// * `remote_frontier` - The remote's frontier
///
/// # Returns
/// Commit IDs we're missing (need to request from remote).
pub fn missing_commit_ids(
    branch: &Branch,
    local_frontier: &[CommitId],
    remote_frontier: &[CommitId],
) -> Vec<CommitId> {
    // Commits in remote frontier that we don't have
    let local_known: HashSet<CommitId> = local_frontier
        .iter()
        .flat_map(|id| branch.ancestors(id))
        .collect();

    remote_frontier
        .iter()
        .filter(|id| !local_known.contains(id) && branch.get_commit(id).is_none())
        .copied()
        .collect()
}

/// Topologically sort commits so parents come before children.
fn topological_sort(commits: Vec<(CommitId, Commit)>) -> Vec<Commit> {
    if commits.is_empty() {
        return vec![];
    }

    let commit_map: std::collections::HashMap<CommitId, Commit> =
        commits.iter().cloned().collect();
    let ids: HashSet<CommitId> = commits.iter().map(|(id, _)| *id).collect();

    let mut result = Vec::with_capacity(commits.len());
    let mut visited = HashSet::new();
    let mut in_stack = HashSet::new();

    fn visit(
        id: &CommitId,
        commit_map: &std::collections::HashMap<CommitId, Commit>,
        ids: &HashSet<CommitId>,
        visited: &mut HashSet<CommitId>,
        in_stack: &mut HashSet<CommitId>,
        result: &mut Vec<Commit>,
    ) {
        if visited.contains(id) {
            return;
        }
        if in_stack.contains(id) {
            // Cycle detected (shouldn't happen in a valid DAG)
            return;
        }

        in_stack.insert(*id);

        if let Some(commit) = commit_map.get(id) {
            // Visit parents first (that are in our set)
            for parent in &commit.parents {
                if ids.contains(parent) {
                    visit(parent, commit_map, ids, visited, in_stack, result);
                }
            }

            visited.insert(*id);
            result.push(commit.clone());
        }

        in_stack.remove(id);
    }

    for (id, _) in &commits {
        visit(id, &commit_map, &ids, &mut visited, &mut in_stack, &mut result);
    }

    result
}

/// Compute the combined frontier after merging commits from both sides.
///
/// When both local and remote have diverged, the result is a multi-tip frontier
/// containing all tips that have no children in the combined set.
pub fn merged_frontier(
    branch: &Branch,
    local_frontier: &[CommitId],
    remote_frontier: &[CommitId],
) -> Vec<CommitId> {
    let mut all_tips: HashSet<CommitId> = local_frontier.iter().copied().collect();
    all_tips.extend(remote_frontier.iter().copied());

    // Remove any tip that is an ancestor of another tip
    let mut result: Vec<CommitId> = Vec::new();
    for &tip in &all_tips {
        let is_ancestor_of_another = all_tips.iter().any(|&other| {
            other != tip && branch.is_ancestor(&tip, &other)
        });
        if !is_ancestor_of_another {
            result.push(tip);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_commit(parents: Vec<CommitId>, content: &str, timestamp: u64) -> Commit {
        Commit {
            parents,
            content: content.as_bytes().to_vec().into_boxed_slice(),
            author: "test".to_string(),
            timestamp,
            meta: None,
        }
    }

    #[test]
    fn test_compare_frontiers_identical() {
        let id1 = CommitId::from_bytes([1u8; 32]);
        let id2 = CommitId::from_bytes([2u8; 32]);

        assert_eq!(
            compare_frontiers(&[id1, id2], &[id2, id1]),
            FrontierComparison::Identical
        );
    }

    #[test]
    fn test_compare_frontiers_local_ahead() {
        let id1 = CommitId::from_bytes([1u8; 32]);
        let id2 = CommitId::from_bytes([2u8; 32]);

        // Local has id1 and id2, remote only has id1
        // This means local has id2 that remote doesn't have
        // BUT this simple check just looks at frontier membership
        // So if remote frontier is [id1] and local frontier is [id1, id2],
        // remote's frontier is a subset of local's
        assert_eq!(
            compare_frontiers(&[id1, id2], &[id1]),
            FrontierComparison::LocalAhead
        );
    }

    #[test]
    fn test_compare_frontiers_remote_ahead() {
        let id1 = CommitId::from_bytes([1u8; 32]);
        let id2 = CommitId::from_bytes([2u8; 32]);

        assert_eq!(
            compare_frontiers(&[id1], &[id1, id2]),
            FrontierComparison::RemoteAhead
        );
    }

    #[test]
    fn test_compare_frontiers_diverged() {
        let id1 = CommitId::from_bytes([1u8; 32]);
        let id2 = CommitId::from_bytes([2u8; 32]);

        assert_eq!(
            compare_frontiers(&[id1], &[id2]),
            FrontierComparison::Diverged
        );
    }

    #[test]
    fn test_commits_to_send_identical() {
        let mut branch = Branch::new("main");

        let c1 = make_commit(vec![], "initial", 1);
        let id1 = branch.add_commit(c1);

        // Same frontier - nothing to send
        let commits = commits_to_send(&branch, &[id1], &[id1]);
        assert!(commits.is_empty());
    }

    #[test]
    fn test_commits_to_send_local_ahead() {
        let mut branch = Branch::new("main");

        let c1 = make_commit(vec![], "commit 1", 1);
        let id1 = branch.add_commit(c1);

        let c2 = make_commit(vec![id1], "commit 2", 2);
        let id2 = branch.add_commit(c2);

        let c3 = make_commit(vec![id2], "commit 3", 3);
        let id3 = branch.add_commit(c3);

        // Local at id3, remote at id1 - should send c2 and c3
        let commits = commits_to_send(&branch, &[id3], &[id1]);
        assert_eq!(commits.len(), 2);
        // Should be topologically sorted: c2 before c3
        assert_eq!(commits[0].timestamp, 2);
        assert_eq!(commits[1].timestamp, 3);
    }

    #[test]
    fn test_commits_to_send_diverged() {
        let mut branch = Branch::new("main");

        // Create a fork:
        //     c1
        //    /  \
        //   c2   c3
        let c1 = make_commit(vec![], "commit 1", 1);
        let id1 = branch.add_commit(c1);

        let c2 = make_commit(vec![id1], "commit 2", 2);
        let id2 = branch.add_commit(c2);

        let c3 = make_commit(vec![id1], "commit 3", 3);
        let id3 = branch.add_commit(c3);

        // Local has c2, remote has c3 - should send c2
        let commits = commits_to_send(&branch, &[id2], &[id3]);
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].timestamp, 2);

        // Remote has c2, local has c3 - should send c3
        let commits = commits_to_send(&branch, &[id3], &[id2]);
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].timestamp, 3);
    }

    #[test]
    fn test_commits_to_send_complex_dag() {
        let mut branch = Branch::new("main");

        // Create a more complex DAG:
        //       c1
        //      /  \
        //     c2   c3
        //      \  /
        //       c4 (merge)
        //        |
        //       c5
        let c1 = make_commit(vec![], "commit 1", 1);
        let id1 = branch.add_commit(c1);

        let c2 = make_commit(vec![id1], "commit 2", 2);
        let id2 = branch.add_commit(c2);

        let c3 = make_commit(vec![id1], "commit 3", 3);
        let id3 = branch.add_commit(c3);

        let c4 = make_commit(vec![id2, id3], "commit 4 (merge)", 4);
        let id4 = branch.add_commit(c4);

        let c5 = make_commit(vec![id4], "commit 5", 5);
        let id5 = branch.add_commit(c5);

        // Local at c5, remote at c1 - should send c2, c3, c4, c5
        let commits = commits_to_send(&branch, &[id5], &[id1]);
        assert_eq!(commits.len(), 4);

        // Verify topological order: parents before children
        let positions: std::collections::HashMap<u64, usize> = commits
            .iter()
            .enumerate()
            .map(|(i, c)| (c.timestamp, i))
            .collect();

        // c2 and c3 must come before c4
        assert!(positions[&2] < positions[&4]);
        assert!(positions[&3] < positions[&4]);
        // c4 must come before c5
        assert!(positions[&4] < positions[&5]);
    }

    #[test]
    fn test_merged_frontier_no_overlap() {
        let mut branch = Branch::new("main");

        let c1 = make_commit(vec![], "commit 1", 1);
        let id1 = branch.add_commit(c1);

        let c2 = make_commit(vec![id1], "commit 2", 2);
        let id2 = branch.add_commit(c2);

        let c3 = make_commit(vec![id1], "commit 3", 3);
        let id3 = branch.add_commit(c3);

        // Merge frontiers [id2] and [id3] - both are tips
        let frontier = merged_frontier(&branch, &[id2], &[id3]);
        assert_eq!(frontier.len(), 2);
        assert!(frontier.contains(&id2));
        assert!(frontier.contains(&id3));
    }

    #[test]
    fn test_merged_frontier_one_ahead() {
        let mut branch = Branch::new("main");

        let c1 = make_commit(vec![], "commit 1", 1);
        let id1 = branch.add_commit(c1);

        let c2 = make_commit(vec![id1], "commit 2", 2);
        let id2 = branch.add_commit(c2);

        // Merge frontiers [id1] and [id2] - id1 is ancestor of id2
        let frontier = merged_frontier(&branch, &[id1], &[id2]);
        assert_eq!(frontier.len(), 1);
        assert!(frontier.contains(&id2));
    }

    #[test]
    fn test_missing_commit_ids() {
        let mut branch = Branch::new("main");

        let c1 = make_commit(vec![], "commit 1", 1);
        let id1 = branch.add_commit(c1);

        // Remote claims to have id2 which we don't have
        let id2 = CommitId::from_bytes([99u8; 32]);
        let missing = missing_commit_ids(&branch, &[id1], &[id2]);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0], id2);
    }
}
