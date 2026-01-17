use std::collections::{HashMap, HashSet};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};

/// How deeply to load a branch from storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadDepth {
    /// Just CommitIds of tips.
    TipIdsOnly,
    /// Full Commit structs for tips.
    TipsOnly,
    /// All commits in branch.
    AllCommits,
}

/// Errors from storage operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    NotFound,
    IoError(String),
}

/// Request to the storage driver.
#[derive(Debug, Clone)]
pub enum StorageRequest {
    CreateObject {
        id: ObjectId,
        metadata: HashMap<String, String>,
    },
    AppendCommit {
        object_id: ObjectId,
        branch_name: BranchName,
        commit: Commit,
    },
    LoadObjectBranch {
        object_id: ObjectId,
        branch_name: BranchName,
        depth: LoadDepth,
    },
}

/// Branch data loaded from storage.
#[derive(Debug, Clone)]
pub struct LoadedBranch {
    pub tips: HashSet<CommitId>,
    /// May be partial based on LoadDepth.
    pub commits: HashMap<CommitId, Commit>,
}

/// Response from the storage driver.
#[derive(Debug, Clone)]
pub enum StorageResponse {
    CreateObject {
        id: ObjectId,
        result: Result<(), StorageError>,
    },
    AppendCommit {
        object_id: ObjectId,
        commit_id: CommitId,
        result: Result<(), StorageError>,
    },
    LoadObjectBranch {
        object_id: ObjectId,
        branch_name: BranchName,
        result: Result<LoadedBranch, StorageError>,
    },
}
