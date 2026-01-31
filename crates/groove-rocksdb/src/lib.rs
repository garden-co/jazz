//! RocksDB-backed storage driver for Groove.
//!
//! Provides persistent storage using RocksDB with column families for:
//! - `objects` - Object metadata (key: ObjectId bytes)
//! - `branches` - Branch tip/tail data (key: ObjectId + BranchName)
//! - `commits` - Commit content (key: ObjectId + BranchName + CommitId)
//! - `blobs` - Blob content by hash
//! - `blob_refs` - Blob associations for GC
//! - `index_pages` - B-tree index pages
//! - `index_meta` - Index metadata

use std::collections::{HashMap, HashSet};
use std::path::Path;

use groove::commit::{Commit, CommitId, StoredState};
use groove::driver::Driver;
use groove::object::{BranchName, ObjectId};
use groove::storage::{
    BlobAssociation, ContentHash, LoadDepth, LoadedBranch, StorageError, StorageRequest,
    StorageResponse,
};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, Options};
use serde::{Deserialize, Serialize};

/// Column family names.
const CF_OBJECTS: &str = "objects";
const CF_BRANCHES: &str = "branches";
const CF_COMMITS: &str = "commits";
const CF_BLOBS: &str = "blobs";
const CF_BLOB_REFS: &str = "blob_refs";
const CF_INDEX_PAGES: &str = "index_pages";
const CF_INDEX_META: &str = "index_meta";

/// Stored branch metadata (tips and tails).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredBranchMeta {
    tips: Vec<[u8; 32]>,
    tails: Option<Vec<[u8; 32]>>,
}

/// RocksDB-backed implementation of the Driver trait.
pub struct RocksDbDriver {
    rocksdb: DB,
}

impl RocksDbDriver {
    /// Open or create a RocksDB database at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_names = [
            CF_OBJECTS,
            CF_BRANCHES,
            CF_COMMITS,
            CF_BLOBS,
            CF_BLOB_REFS,
            CF_INDEX_PAGES,
            CF_INDEX_META,
        ];

        let cfs: Vec<ColumnFamilyDescriptor> = cf_names
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        let rocksdb = DB::open_cf_descriptors(&opts, path, cfs)?;
        Ok(Self { rocksdb })
    }

    /// Get a column family handle.
    fn cf(&self, name: &str) -> &ColumnFamily {
        self.rocksdb
            .cf_handle(name)
            .expect("Column family should exist")
    }

    /// Create a key for branch metadata: ObjectId bytes + BranchName.
    fn branch_key(object_id: ObjectId, branch_name: &BranchName) -> Vec<u8> {
        let mut key = Vec::with_capacity(16 + branch_name.as_str().len());
        key.extend_from_slice(object_id.uuid().as_bytes());
        key.extend_from_slice(branch_name.as_str().as_bytes());
        key
    }

    /// Create a key for commit: ObjectId bytes + BranchName bytes + CommitId bytes.
    fn commit_key(object_id: ObjectId, branch_name: &BranchName, commit_id: CommitId) -> Vec<u8> {
        let mut key = Vec::with_capacity(16 + branch_name.as_str().len() + 32);
        key.extend_from_slice(object_id.uuid().as_bytes());
        key.extend_from_slice(branch_name.as_str().as_bytes());
        key.push(0); // Separator
        key.extend_from_slice(&commit_id.0);
        key
    }

    /// Create a prefix for scanning commits on a branch.
    fn commit_prefix(object_id: ObjectId, branch_name: &BranchName) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(16 + branch_name.as_str().len() + 1);
        prefix.extend_from_slice(object_id.uuid().as_bytes());
        prefix.extend_from_slice(branch_name.as_str().as_bytes());
        prefix.push(0); // Separator
        prefix
    }

    /// Create a key for index page: table + column + page_id.
    fn index_page_key(table: &str, column: &str, page_id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(table.len() + column.len() + 10);
        key.extend_from_slice(table.as_bytes());
        key.push(0); // Separator
        key.extend_from_slice(column.as_bytes());
        key.push(0); // Separator
        key.extend_from_slice(&page_id.to_be_bytes());
        key
    }

    /// Create a key for index meta: table + column.
    fn index_meta_key(table: &str, column: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(table.len() + column.len() + 1);
        key.extend_from_slice(table.as_bytes());
        key.push(0); // Separator
        key.extend_from_slice(column.as_bytes());
        key
    }

    fn process_one(&self, request: StorageRequest) -> StorageResponse {
        match request {
            StorageRequest::CreateObject { id, metadata } => {
                let result = self.create_object(id, metadata);
                StorageResponse::CreateObject { id, result }
            }

            StorageRequest::AppendCommit {
                object_id,
                branch_name,
                mut commit,
            } => {
                let commit_id = commit.id();
                let result = self.append_commit(object_id, branch_name, &mut commit);
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
                let result = self.load_object_branch(object_id, branch_name, depth);
                StorageResponse::LoadObjectBranch {
                    object_id,
                    branch_name,
                    result,
                }
            }

            StorageRequest::StoreBlob { content_hash, data } => {
                let result = self.store_blob(content_hash, &data);
                StorageResponse::StoreBlob {
                    content_hash,
                    result,
                }
            }

            StorageRequest::LoadBlob { content_hash } => {
                let result = self.load_blob(content_hash);
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
                let result = self.associate_blob(content_hash, object_id, branch_name, commit_id);
                StorageResponse::AssociateBlob {
                    content_hash,
                    result,
                }
            }

            StorageRequest::LoadBlobAssociations { content_hash } => {
                let result = self.load_blob_associations(content_hash);
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
                let result = self.delete_commit(object_id, branch_name, commit_id);
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
                let result = self.dissociate_and_maybe_delete_blob(
                    content_hash,
                    object_id,
                    branch_name,
                    commit_id,
                );
                StorageResponse::DissociateAndMaybeDeleteBlob {
                    content_hash,
                    object_id,
                    branch_name,
                    commit_id,
                    blob_deleted: result,
                }
            }

            StorageRequest::SetBranchTails {
                object_id,
                branch_name,
                tails,
            } => {
                let result = self.set_branch_tails(object_id, branch_name, tails);
                StorageResponse::SetBranchTails {
                    object_id,
                    branch_name,
                    result,
                }
            }

            StorageRequest::LoadIndexPage {
                table,
                column,
                page_id,
            } => {
                let result = self.load_index_page(&table, &column, page_id);
                StorageResponse::LoadIndexPage {
                    table,
                    column,
                    page_id,
                    result,
                }
            }

            StorageRequest::StoreIndexPage {
                table,
                column,
                page_id,
                data,
            } => {
                let result = self.store_index_page(&table, &column, page_id, &data);
                StorageResponse::StoreIndexPage {
                    table,
                    column,
                    page_id,
                    result,
                }
            }

            StorageRequest::DeleteIndexPage {
                table,
                column,
                page_id,
            } => {
                let result = self.delete_index_page(&table, &column, page_id);
                StorageResponse::DeleteIndexPage {
                    table,
                    column,
                    page_id,
                    result,
                }
            }

            StorageRequest::LoadIndexMeta { table, column } => {
                let result = self.load_index_meta(&table, &column);
                StorageResponse::LoadIndexMeta {
                    table,
                    column,
                    result,
                }
            }

            StorageRequest::StoreIndexMeta {
                table,
                column,
                data,
            } => {
                let result = self.store_index_meta(&table, &column, &data);
                StorageResponse::StoreIndexMeta {
                    table,
                    column,
                    result,
                }
            }
        }
    }

    // ========================================================================
    // Object Operations
    // ========================================================================

    fn create_object(
        &self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        let key = id.uuid().as_bytes();
        let value =
            serde_json::to_vec(&metadata).map_err(|e| StorageError::IoError(e.to_string()))?;
        self.rocksdb
            .put_cf(self.cf(CF_OBJECTS), key, value)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn append_commit(
        &self,
        object_id: ObjectId,
        branch_name: BranchName,
        commit: &mut Commit,
    ) -> Result<(), StorageError> {
        let commit_id = commit.id();

        // Check object exists
        let obj_key = object_id.uuid().as_bytes();
        if self
            .rocksdb
            .get_cf(self.cf(CF_OBJECTS), obj_key)
            .map_err(|e| StorageError::IoError(e.to_string()))?
            .is_none()
        {
            return Err(StorageError::NotFound);
        }

        // Load or create branch metadata
        let branch_key = Self::branch_key(object_id, &branch_name);
        let mut branch_meta = if let Some(data) = self
            .rocksdb
            .get_cf(self.cf(CF_BRANCHES), &branch_key)
            .map_err(|e| StorageError::IoError(e.to_string()))?
        {
            serde_json::from_slice::<StoredBranchMeta>(&data)
                .map_err(|e| StorageError::IoError(e.to_string()))?
        } else {
            StoredBranchMeta {
                tips: Vec::new(),
                tails: None,
            }
        };

        // Update tips: remove parents, add new commit
        let parent_hashes: HashSet<[u8; 32]> = commit.parents.iter().map(|p| p.0).collect();
        branch_meta.tips.retain(|t| !parent_hashes.contains(t));
        branch_meta.tips.push(commit_id.0);

        // Store commit
        commit.stored_state = StoredState::Stored;
        let commit_key = Self::commit_key(object_id, &branch_name, commit_id);
        let commit_data =
            serde_json::to_vec(commit).map_err(|e| StorageError::IoError(e.to_string()))?;
        self.rocksdb
            .put_cf(self.cf(CF_COMMITS), &commit_key, commit_data)
            .map_err(|e| StorageError::IoError(e.to_string()))?;

        // Store updated branch metadata
        let branch_data =
            serde_json::to_vec(&branch_meta).map_err(|e| StorageError::IoError(e.to_string()))?;
        self.rocksdb
            .put_cf(self.cf(CF_BRANCHES), &branch_key, branch_data)
            .map_err(|e| StorageError::IoError(e.to_string()))?;

        Ok(())
    }

    fn load_object_branch(
        &self,
        object_id: ObjectId,
        branch_name: BranchName,
        depth: LoadDepth,
    ) -> Result<LoadedBranch, StorageError> {
        // Load branch metadata
        let branch_key = Self::branch_key(object_id, &branch_name);
        let branch_meta = if let Some(data) = self
            .rocksdb
            .get_cf(self.cf(CF_BRANCHES), &branch_key)
            .map_err(|e| StorageError::IoError(e.to_string()))?
        {
            serde_json::from_slice::<StoredBranchMeta>(&data)
                .map_err(|e| StorageError::IoError(e.to_string()))?
        } else {
            return Err(StorageError::NotFound);
        };

        let tips: HashSet<CommitId> = branch_meta.tips.iter().map(|h| CommitId(*h)).collect();
        let tails: Option<HashSet<CommitId>> = branch_meta
            .tails
            .map(|t| t.iter().map(|h| CommitId(*h)).collect());

        let commits = match depth {
            LoadDepth::TipIdsOnly => HashMap::new(),
            LoadDepth::TipsOnly => {
                let mut commits = HashMap::new();
                for tip_id in &tips {
                    if let Some(commit) = self.load_commit(object_id, branch_name, *tip_id)? {
                        commits.insert(*tip_id, commit);
                    }
                }
                commits
            }
            LoadDepth::AllCommits => {
                let mut commits = HashMap::new();
                let prefix = Self::commit_prefix(object_id, &branch_name);
                let iter = self
                    .rocksdb
                    .prefix_iterator_cf(self.cf(CF_COMMITS), &prefix);

                for item in iter {
                    let (key, value) = item.map_err(|e| StorageError::IoError(e.to_string()))?;
                    // Check key starts with our prefix
                    if !key.starts_with(&prefix) {
                        break;
                    }
                    let mut commit: Commit = serde_json::from_slice(&value)
                        .map_err(|e| StorageError::IoError(e.to_string()))?;
                    // Mark as stored since it came from persistent storage
                    commit.stored_state = StoredState::Stored;
                    let commit_id = commit.id();
                    commits.insert(commit_id, commit);
                }
                commits
            }
        };

        Ok(LoadedBranch {
            tips,
            tails,
            commits,
        })
    }

    fn load_commit(
        &self,
        object_id: ObjectId,
        branch_name: BranchName,
        commit_id: CommitId,
    ) -> Result<Option<Commit>, StorageError> {
        let key = Self::commit_key(object_id, &branch_name, commit_id);
        match self
            .rocksdb
            .get_cf(self.cf(CF_COMMITS), &key)
            .map_err(|e| StorageError::IoError(e.to_string()))?
        {
            Some(data) => {
                let mut commit: Commit = serde_json::from_slice(&data)
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                // Mark as stored since it came from persistent storage
                commit.stored_state = StoredState::Stored;
                Ok(Some(commit))
            }
            None => Ok(None),
        }
    }

    fn delete_commit(
        &self,
        object_id: ObjectId,
        branch_name: BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        // Remove commit
        let commit_key = Self::commit_key(object_id, &branch_name, commit_id);
        self.rocksdb
            .delete_cf(self.cf(CF_COMMITS), &commit_key)
            .map_err(|e| StorageError::IoError(e.to_string()))?;

        // Update branch metadata to remove from tips if present
        let branch_key = Self::branch_key(object_id, &branch_name);
        if let Some(data) = self
            .rocksdb
            .get_cf(self.cf(CF_BRANCHES), &branch_key)
            .map_err(|e| StorageError::IoError(e.to_string()))?
        {
            let mut branch_meta: StoredBranchMeta =
                serde_json::from_slice(&data).map_err(|e| StorageError::IoError(e.to_string()))?;
            branch_meta.tips.retain(|t| *t != commit_id.0);
            let branch_data = serde_json::to_vec(&branch_meta)
                .map_err(|e| StorageError::IoError(e.to_string()))?;
            self.rocksdb
                .put_cf(self.cf(CF_BRANCHES), &branch_key, branch_data)
                .map_err(|e| StorageError::IoError(e.to_string()))?;
        }

        Ok(())
    }

    fn set_branch_tails(
        &self,
        object_id: ObjectId,
        branch_name: BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        let branch_key = Self::branch_key(object_id, &branch_name);
        if let Some(data) = self
            .rocksdb
            .get_cf(self.cf(CF_BRANCHES), &branch_key)
            .map_err(|e| StorageError::IoError(e.to_string()))?
        {
            let mut branch_meta: StoredBranchMeta =
                serde_json::from_slice(&data).map_err(|e| StorageError::IoError(e.to_string()))?;
            branch_meta.tails = tails.map(|t| t.iter().map(|c| c.0).collect());
            let branch_data = serde_json::to_vec(&branch_meta)
                .map_err(|e| StorageError::IoError(e.to_string()))?;
            self.rocksdb
                .put_cf(self.cf(CF_BRANCHES), &branch_key, branch_data)
                .map_err(|e| StorageError::IoError(e.to_string()))?;
            Ok(())
        } else {
            Err(StorageError::NotFound)
        }
    }

    // ========================================================================
    // Blob Operations
    // ========================================================================

    fn store_blob(&self, content_hash: ContentHash, data: &[u8]) -> Result<(), StorageError> {
        self.rocksdb
            .put_cf(self.cf(CF_BLOBS), &content_hash.0, data)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn load_blob(&self, content_hash: ContentHash) -> Result<Vec<u8>, StorageError> {
        self.rocksdb
            .get_cf(self.cf(CF_BLOBS), &content_hash.0)
            .map_err(|e| StorageError::IoError(e.to_string()))?
            .ok_or(StorageError::NotFound)
    }

    fn associate_blob(
        &self,
        content_hash: ContentHash,
        object_id: ObjectId,
        branch_name: BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        // Load existing associations
        let mut associations = self.get_blob_associations(content_hash)?;

        // Add new association
        associations.push(BlobAssociation {
            object_id,
            branch_name,
            commit_id,
        });

        // Store updated associations
        let data =
            serde_json::to_vec(&associations).map_err(|e| StorageError::IoError(e.to_string()))?;
        self.rocksdb
            .put_cf(self.cf(CF_BLOB_REFS), &content_hash.0, data)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn load_blob_associations(
        &self,
        content_hash: ContentHash,
    ) -> Result<Vec<BlobAssociation>, StorageError> {
        let associations = self.get_blob_associations(content_hash)?;
        if associations.is_empty() {
            Err(StorageError::NotFound)
        } else {
            Ok(associations)
        }
    }

    fn get_blob_associations(
        &self,
        content_hash: ContentHash,
    ) -> Result<Vec<BlobAssociation>, StorageError> {
        match self
            .rocksdb
            .get_cf(self.cf(CF_BLOB_REFS), &content_hash.0)
            .map_err(|e| StorageError::IoError(e.to_string()))?
        {
            Some(data) => {
                serde_json::from_slice(&data).map_err(|e| StorageError::IoError(e.to_string()))
            }
            None => Ok(Vec::new()),
        }
    }

    fn dissociate_and_maybe_delete_blob(
        &self,
        content_hash: ContentHash,
        object_id: ObjectId,
        branch_name: BranchName,
        commit_id: CommitId,
    ) -> Result<bool, StorageError> {
        let mut associations = self.get_blob_associations(content_hash)?;

        // Remove matching association
        associations.retain(|a| {
            !(a.object_id == object_id && a.branch_name == branch_name && a.commit_id == commit_id)
        });

        if associations.is_empty() {
            // Delete both associations and blob
            self.rocksdb
                .delete_cf(self.cf(CF_BLOB_REFS), &content_hash.0)
                .map_err(|e| StorageError::IoError(e.to_string()))?;
            self.rocksdb
                .delete_cf(self.cf(CF_BLOBS), &content_hash.0)
                .map_err(|e| StorageError::IoError(e.to_string()))?;
            Ok(true)
        } else {
            // Store updated associations
            let data = serde_json::to_vec(&associations)
                .map_err(|e| StorageError::IoError(e.to_string()))?;
            self.rocksdb
                .put_cf(self.cf(CF_BLOB_REFS), &content_hash.0, data)
                .map_err(|e| StorageError::IoError(e.to_string()))?;
            Ok(false)
        }
    }

    // ========================================================================
    // Index Operations
    // ========================================================================

    fn load_index_page(
        &self,
        table: &str,
        column: &str,
        page_id: u64,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let key = Self::index_page_key(table, column, page_id);
        self.rocksdb
            .get_cf(self.cf(CF_INDEX_PAGES), &key)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn store_index_page(
        &self,
        table: &str,
        column: &str,
        page_id: u64,
        data: &[u8],
    ) -> Result<(), StorageError> {
        let key = Self::index_page_key(table, column, page_id);
        self.rocksdb
            .put_cf(self.cf(CF_INDEX_PAGES), &key, data)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn delete_index_page(
        &self,
        table: &str,
        column: &str,
        page_id: u64,
    ) -> Result<(), StorageError> {
        let key = Self::index_page_key(table, column, page_id);
        self.rocksdb
            .delete_cf(self.cf(CF_INDEX_PAGES), &key)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn load_index_meta(&self, table: &str, column: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let key = Self::index_meta_key(table, column);
        self.rocksdb
            .get_cf(self.cf(CF_INDEX_META), &key)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn store_index_meta(&self, table: &str, column: &str, data: &[u8]) -> Result<(), StorageError> {
        let key = Self::index_meta_key(table, column);
        self.rocksdb
            .put_cf(self.cf(CF_INDEX_META), &key, data)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }
}

impl Driver for RocksDbDriver {
    fn process(&mut self, requests: Vec<StorageRequest>) -> Vec<StorageResponse> {
        requests
            .into_iter()
            .map(|req| self.process_one(req))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use groove::commit::StoredState;
    use smallvec::smallvec;
    use tempfile::tempdir;

    fn create_test_driver() -> (RocksDbDriver, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let driver = RocksDbDriver::open(dir.path()).unwrap();
        (driver, dir)
    }

    #[test]
    fn test_driver_creates_object() {
        let (mut driver, _dir) = create_test_driver();
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
    }

    #[test]
    fn test_driver_appends_commit() {
        let (mut driver, _dir) = create_test_driver();
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
    }

    #[test]
    fn test_driver_loads_branch() {
        let (mut driver, _dir) = create_test_driver();
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

    #[test]
    fn test_driver_blob_operations() {
        let (mut driver, _dir) = create_test_driver();
        let hash = ContentHash([1u8; 32]);
        let data = b"hello blob".to_vec();

        // Store blob
        let responses = driver.process(vec![StorageRequest::StoreBlob {
            content_hash: hash,
            data: data.clone(),
        }]);
        assert!(matches!(
            &responses[0],
            StorageResponse::StoreBlob { result: Ok(()), .. }
        ));

        // Load blob
        let responses = driver.process(vec![StorageRequest::LoadBlob { content_hash: hash }]);
        if let StorageResponse::LoadBlob {
            result: Ok(loaded), ..
        } = &responses[0]
        {
            assert_eq!(loaded, &data);
        } else {
            panic!("Expected LoadBlob response");
        }
    }

    #[test]
    fn test_driver_blob_associations() {
        let (mut driver, _dir) = create_test_driver();
        let hash = ContentHash([2u8; 32]);
        let object_id = ObjectId::new();
        let branch_name = BranchName::new("main");
        let commit_id = CommitId([3u8; 32]);

        // Store blob first
        driver.process(vec![StorageRequest::StoreBlob {
            content_hash: hash,
            data: b"blob data".to_vec(),
        }]);

        // Associate blob
        let responses = driver.process(vec![StorageRequest::AssociateBlob {
            content_hash: hash,
            object_id,
            branch_name,
            commit_id,
        }]);
        assert!(matches!(
            &responses[0],
            StorageResponse::AssociateBlob { result: Ok(()), .. }
        ));

        // Load associations
        let responses = driver.process(vec![StorageRequest::LoadBlobAssociations {
            content_hash: hash,
        }]);
        if let StorageResponse::LoadBlobAssociations {
            result: Ok(associations),
            ..
        } = &responses[0]
        {
            assert_eq!(associations.len(), 1);
            assert_eq!(associations[0].object_id, object_id);
        } else {
            panic!("Expected LoadBlobAssociations response");
        }

        // Dissociate and delete
        let responses = driver.process(vec![StorageRequest::DissociateAndMaybeDeleteBlob {
            content_hash: hash,
            object_id,
            branch_name,
            commit_id,
        }]);
        if let StorageResponse::DissociateAndMaybeDeleteBlob {
            blob_deleted: Ok(deleted),
            ..
        } = &responses[0]
        {
            assert!(
                *deleted,
                "Blob should be deleted when no associations remain"
            );
        } else {
            panic!("Expected DissociateAndMaybeDeleteBlob response");
        }

        // Verify blob is gone
        let responses = driver.process(vec![StorageRequest::LoadBlob { content_hash: hash }]);
        assert!(matches!(
            &responses[0],
            StorageResponse::LoadBlob {
                result: Err(StorageError::NotFound),
                ..
            }
        ));
    }

    #[test]
    fn test_driver_index_operations() {
        let (mut driver, _dir) = create_test_driver();
        let table = "users";
        let column = "email";
        let page_id = 42;
        let data = b"index page data".to_vec();

        // Store index page
        let responses = driver.process(vec![StorageRequest::StoreIndexPage {
            table: table.to_string(),
            column: column.to_string(),
            page_id,
            data: data.clone(),
        }]);
        assert!(matches!(
            &responses[0],
            StorageResponse::StoreIndexPage { result: Ok(()), .. }
        ));

        // Load index page
        let responses = driver.process(vec![StorageRequest::LoadIndexPage {
            table: table.to_string(),
            column: column.to_string(),
            page_id,
        }]);
        if let StorageResponse::LoadIndexPage {
            result: Ok(Some(loaded)),
            ..
        } = &responses[0]
        {
            assert_eq!(loaded, &data);
        } else {
            panic!("Expected LoadIndexPage response with data");
        }

        // Delete index page
        let responses = driver.process(vec![StorageRequest::DeleteIndexPage {
            table: table.to_string(),
            column: column.to_string(),
            page_id,
        }]);
        assert!(matches!(
            &responses[0],
            StorageResponse::DeleteIndexPage { result: Ok(()), .. }
        ));

        // Verify deleted
        let responses = driver.process(vec![StorageRequest::LoadIndexPage {
            table: table.to_string(),
            column: column.to_string(),
            page_id,
        }]);
        assert!(matches!(
            &responses[0],
            StorageResponse::LoadIndexPage {
                result: Ok(None),
                ..
            }
        ));
    }

    #[test]
    fn test_driver_index_meta() {
        let (mut driver, _dir) = create_test_driver();
        let table = "users";
        let column = "email";
        let meta_data = b"root_page=1,count=100".to_vec();

        // Store index meta
        let responses = driver.process(vec![StorageRequest::StoreIndexMeta {
            table: table.to_string(),
            column: column.to_string(),
            data: meta_data.clone(),
        }]);
        assert!(matches!(
            &responses[0],
            StorageResponse::StoreIndexMeta { result: Ok(()), .. }
        ));

        // Load index meta
        let responses = driver.process(vec![StorageRequest::LoadIndexMeta {
            table: table.to_string(),
            column: column.to_string(),
        }]);
        if let StorageResponse::LoadIndexMeta {
            result: Ok(Some(loaded)),
            ..
        } = &responses[0]
        {
            assert_eq!(loaded, &meta_data);
        } else {
            panic!("Expected LoadIndexMeta response with data");
        }
    }

    #[test]
    fn test_driver_persistence() {
        let dir = tempdir().unwrap();
        let object_id = ObjectId::new();
        let author = ObjectId::new();

        // Create and populate
        {
            let mut driver = RocksDbDriver::open(dir.path()).unwrap();
            driver.process(vec![StorageRequest::CreateObject {
                id: object_id,
                metadata: HashMap::new(),
            }]);

            let commit = Commit {
                parents: smallvec![],
                content: b"persistent data".to_vec(),
                timestamp: 12345,
                author,
                metadata: None,
                stored_state: StoredState::Pending,
            };

            driver.process(vec![StorageRequest::AppendCommit {
                object_id,
                branch_name: BranchName::new("main"),
                commit,
            }]);
        }

        // Reopen and verify
        {
            let mut driver = RocksDbDriver::open(dir.path()).unwrap();
            let responses = driver.process(vec![StorageRequest::LoadObjectBranch {
                object_id,
                branch_name: BranchName::new("main"),
                depth: LoadDepth::AllCommits,
            }]);

            if let StorageResponse::LoadObjectBranch {
                result: Ok(loaded), ..
            } = &responses[0]
            {
                assert_eq!(loaded.commits.len(), 1);
                let commit = loaded.commits.values().next().unwrap();
                assert_eq!(commit.content, b"persistent data".to_vec());
            } else {
                panic!("Expected LoadObjectBranch response after reopen");
            }
        }
    }
}
