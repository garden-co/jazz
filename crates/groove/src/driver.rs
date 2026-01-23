use std::collections::{HashMap, HashSet};

use crate::commit::{Commit, CommitId, StoredState};
use crate::object::{BranchName, ObjectId};
use crate::storage::{
    BlobAssociation, ContentHash, LoadDepth, LoadedBranch, StorageError, StorageRequest,
    StorageResponse,
};

/// Trait for storage drivers that process storage requests.
pub trait Driver {
    fn process(&mut self, requests: Vec<StorageRequest>) -> Vec<StorageResponse>;
}

/// In-memory storage for testing.
#[derive(Debug, Clone, Default)]
pub struct TestDriver {
    pub objects: HashMap<ObjectId, StoredObject>,
    /// Blobs by content hash.
    pub blobs: HashMap<ContentHash, Vec<u8>>,
    /// Blob associations for GC.
    pub blob_associations: HashMap<ContentHash, Vec<BlobAssociation>>,
    /// Index pages by (table, column, page_id).
    pub index_pages: HashMap<(String, String, u64), Vec<u8>>,
    /// Index metadata by (table, column).
    pub index_meta: HashMap<(String, String), Vec<u8>>,
}

/// An object stored by TestDriver.
#[derive(Debug, Clone, Default)]
pub struct StoredObject {
    pub metadata: HashMap<String, String>,
    pub branches: HashMap<BranchName, StoredBranch>,
}

/// A branch stored by TestDriver.
#[derive(Debug, Clone, Default)]
pub struct StoredBranch {
    pub commits: HashMap<CommitId, Commit>,
    pub tips: HashSet<CommitId>,
    pub tails: Option<HashSet<CommitId>>,
}

impl TestDriver {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Driver for TestDriver {
    fn process(&mut self, requests: Vec<StorageRequest>) -> Vec<StorageResponse> {
        requests
            .into_iter()
            .map(|req| self.process_one(req))
            .collect()
    }
}

impl TestDriver {
    fn process_one(&mut self, request: StorageRequest) -> StorageResponse {
        match request {
            StorageRequest::CreateObject { id, metadata } => {
                self.objects.insert(
                    id,
                    StoredObject {
                        metadata,
                        branches: HashMap::new(),
                    },
                );
                StorageResponse::CreateObject { id, result: Ok(()) }
            }
            StorageRequest::AppendCommit {
                object_id,
                branch_name,
                mut commit,
            } => {
                let commit_id = commit.id();

                let result = if let Some(obj) = self.objects.get_mut(&object_id) {
                    let branch = obj.branches.entry(branch_name).or_default();

                    // Update tips: remove parents, add new commit
                    for parent in &commit.parents {
                        branch.tips.remove(parent);
                    }
                    branch.tips.insert(commit_id);

                    // Mark as stored and insert
                    commit.stored_state = StoredState::Stored;
                    branch.commits.insert(commit_id, commit);

                    Ok(())
                } else {
                    Err(StorageError::NotFound)
                };

                StorageResponse::AppendCommit {
                    object_id,
                    commit_id,
                    result,
                }
            }
            StorageRequest::LoadObjectBranch {
                object_id,
                branch_name,
                depth,
            } => {
                let result = if let Some(obj) = self.objects.get(&object_id) {
                    if let Some(branch) = obj.branches.get(&branch_name) {
                        let commits = match depth {
                            LoadDepth::TipIdsOnly => HashMap::new(),
                            LoadDepth::TipsOnly => branch
                                .tips
                                .iter()
                                .filter_map(|id| branch.commits.get(id).map(|c| (*id, c.clone())))
                                .collect(),
                            LoadDepth::AllCommits => branch.commits.clone(),
                        };

                        Ok(LoadedBranch {
                            tips: branch.tips.clone(),
                            tails: branch.tails.clone(),
                            commits,
                        })
                    } else {
                        Err(StorageError::NotFound)
                    }
                } else {
                    Err(StorageError::NotFound)
                };

                StorageResponse::LoadObjectBranch {
                    object_id,
                    branch_name,
                    result,
                }
            }
            StorageRequest::StoreBlob { content_hash, data } => {
                self.blobs.insert(content_hash, data);
                StorageResponse::StoreBlob {
                    content_hash,
                    result: Ok(()),
                }
            }
            StorageRequest::LoadBlob { content_hash } => {
                let result = self
                    .blobs
                    .get(&content_hash)
                    .cloned()
                    .ok_or(StorageError::NotFound);
                StorageResponse::LoadBlob {
                    content_hash,
                    result,
                }
            }
            StorageRequest::AssociateBlob {
                content_hash,
                object_id,
                branch_name,
                commit_id,
            } => {
                self.blob_associations
                    .entry(content_hash)
                    .or_default()
                    .push(BlobAssociation {
                        object_id,
                        branch_name,
                        commit_id,
                    });
                StorageResponse::AssociateBlob {
                    content_hash,
                    result: Ok(()),
                }
            }
            StorageRequest::LoadBlobAssociations { content_hash } => {
                let result = self
                    .blob_associations
                    .get(&content_hash)
                    .cloned()
                    .ok_or(StorageError::NotFound);
                StorageResponse::LoadBlobAssociations {
                    content_hash,
                    result,
                }
            }
            StorageRequest::DeleteCommit {
                object_id,
                branch_name,
                commit_id,
            } => {
                let result = if let Some(obj) = self.objects.get_mut(&object_id) {
                    if let Some(branch) = obj.branches.get_mut(&branch_name) {
                        branch.commits.remove(&commit_id);
                        branch.tips.remove(&commit_id);
                        Ok(())
                    } else {
                        Err(StorageError::NotFound)
                    }
                } else {
                    Err(StorageError::NotFound)
                };
                StorageResponse::DeleteCommit {
                    object_id,
                    branch_name,
                    commit_id,
                    result,
                }
            }
            StorageRequest::DissociateAndMaybeDeleteBlob {
                content_hash,
                object_id,
                branch_name,
                commit_id,
            } => {
                // Remove association
                let mut blob_deleted = false;
                if let Some(associations) = self.blob_associations.get_mut(&content_hash) {
                    associations.retain(|a| {
                        !(a.object_id == object_id
                            && a.branch_name == branch_name
                            && a.commit_id == commit_id)
                    });
                    // If no associations remain, delete the blob
                    if associations.is_empty() {
                        self.blob_associations.remove(&content_hash);
                        self.blobs.remove(&content_hash);
                        blob_deleted = true;
                    }
                }
                StorageResponse::DissociateAndMaybeDeleteBlob {
                    content_hash,
                    object_id,
                    branch_name,
                    commit_id,
                    blob_deleted: Ok(blob_deleted),
                }
            }
            StorageRequest::SetBranchTails {
                object_id,
                branch_name,
                tails,
            } => {
                let result = if let Some(obj) = self.objects.get_mut(&object_id) {
                    if let Some(branch) = obj.branches.get_mut(&branch_name) {
                        branch.tails = tails;
                        Ok(())
                    } else {
                        Err(StorageError::NotFound)
                    }
                } else {
                    Err(StorageError::NotFound)
                };
                StorageResponse::SetBranchTails {
                    object_id,
                    branch_name,
                    result,
                }
            }

            // Index page storage
            StorageRequest::LoadIndexPage {
                table,
                column,
                page_id,
            } => {
                let data = self
                    .index_pages
                    .get(&(table.clone(), column.clone(), page_id))
                    .cloned();
                StorageResponse::LoadIndexPage {
                    table,
                    column,
                    page_id,
                    result: Ok(data),
                }
            }
            StorageRequest::StoreIndexPage {
                table,
                column,
                page_id,
                data,
            } => {
                self.index_pages
                    .insert((table.clone(), column.clone(), page_id), data);
                StorageResponse::StoreIndexPage {
                    table,
                    column,
                    page_id,
                    result: Ok(()),
                }
            }
            StorageRequest::DeleteIndexPage {
                table,
                column,
                page_id,
            } => {
                self.index_pages
                    .remove(&(table.clone(), column.clone(), page_id));
                StorageResponse::DeleteIndexPage {
                    table,
                    column,
                    page_id,
                    result: Ok(()),
                }
            }
            StorageRequest::LoadIndexMeta { table, column } => {
                let data = self
                    .index_meta
                    .get(&(table.clone(), column.clone()))
                    .cloned();
                StorageResponse::LoadIndexMeta {
                    table,
                    column,
                    result: Ok(data),
                }
            }
            StorageRequest::StoreIndexMeta {
                table,
                column,
                data,
            } => {
                self.index_meta
                    .insert((table.clone(), column.clone()), data);
                StorageResponse::StoreIndexMeta {
                    table,
                    column,
                    result: Ok(()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    #[test]
    fn test_driver_creates_object() {
        let mut driver = TestDriver::new();
        let id = ObjectId::new();

        let responses = driver.process(vec![StorageRequest::CreateObject {
            id,
            metadata: HashMap::new(),
        }]);

        assert_eq!(responses.len(), 1);
        assert!(matches!(
            &responses[0],
            StorageResponse::CreateObject { id: resp_id, result: Ok(()) }
            if *resp_id == id
        ));
        assert!(driver.objects.contains_key(&id));
    }

    #[test]
    fn test_driver_appends_commit() {
        let mut driver = TestDriver::new();
        let object_id = ObjectId::new();
        let author = ObjectId::new();

        // Create object first
        driver.process(vec![StorageRequest::CreateObject {
            id: object_id,
            metadata: HashMap::new(),
        }]);

        let commit = Commit {
            parents: smallvec![],
            content: b"test".to_vec(),
            timestamp: 123,
            author,
            metadata: None,
            stored_state: StoredState::Pending,
        };
        let commit_id = commit.id();

        let responses = driver.process(vec![StorageRequest::AppendCommit {
            object_id,
            branch_name: BranchName::new("main"),
            commit,
        }]);

        assert_eq!(responses.len(), 1);
        assert!(matches!(
            &responses[0],
            StorageResponse::AppendCommit { commit_id: cid, result: Ok(()), .. }
            if *cid == commit_id
        ));

        // Verify stored
        let branch = &driver.objects[&object_id].branches[&BranchName::new("main")];
        assert!(branch.commits.contains_key(&commit_id));
        assert!(branch.tips.contains(&commit_id));
    }

    #[test]
    fn test_driver_loads_branch() {
        let mut driver = TestDriver::new();
        let object_id = ObjectId::new();
        let author = ObjectId::new();

        // Create object and commit
        driver.process(vec![StorageRequest::CreateObject {
            id: object_id,
            metadata: HashMap::new(),
        }]);

        let commit = Commit {
            parents: smallvec![],
            content: b"test".to_vec(),
            timestamp: 123,
            author,
            metadata: None,
            stored_state: StoredState::Pending,
        };
        let commit_id = commit.id();

        driver.process(vec![StorageRequest::AppendCommit {
            object_id,
            branch_name: BranchName::new("main"),
            commit,
        }]);

        // Load branch
        let responses = driver.process(vec![StorageRequest::LoadObjectBranch {
            object_id,
            branch_name: BranchName::new("main"),
            depth: LoadDepth::AllCommits,
        }]);

        assert_eq!(responses.len(), 1);
        if let StorageResponse::LoadObjectBranch {
            result: Ok(loaded), ..
        } = &responses[0]
        {
            assert!(loaded.tips.contains(&commit_id));
            assert!(loaded.commits.contains_key(&commit_id));
        } else {
            panic!("Expected LoadObjectBranch response");
        }
    }
}
