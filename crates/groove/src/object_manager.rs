use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use blake3::Hasher;

use crate::commit::{Commit, CommitId, StoredState};
use crate::object::{Branch, BranchLoadedState, BranchName, Object, ObjectId, ObjectState};
use crate::storage::{
    BlobAssociation, ContentHash, LoadDepth, StorageError, StorageRequest, StorageResponse,
};

/// Unique identifier for a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

/// Internal tracking of a subscription.
#[derive(Debug, Clone)]
struct Subscription {
    object_id: ObjectId,
    branch_name: BranchName,
}

/// Update sent to subscribers when commits are added or loaded.
///
/// Contains the current frontier (tips) sorted by timestamp (oldest first).
/// When branches diverge, you'll see multiple commits in the frontier.
/// When they merge, the frontier consolidates back to one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionUpdate {
    pub subscription_id: SubscriptionId,
    pub object_id: ObjectId,
    pub branch_name: BranchName,
    /// Current frontier commit IDs, sorted by timestamp (oldest first).
    pub commit_ids: Vec<CommitId>,
}

/// Full blob identifier (for addressing within commit context).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlobId {
    pub object_id: ObjectId,
    pub branch_name: BranchName,
    pub commit_id: CommitId,
    pub content_hash: ContentHash,
}

/// State of a blob in memory.
#[derive(Debug, Clone)]
enum BlobState {
    /// Data in memory, storage state tracked.
    Available {
        data: Vec<u8>,
        stored_state: StoredState,
    },
    /// Load requested, waiting for response.
    Loading,
    /// Blob not found in storage (permanent error).
    NotFound,
}

/// Errors that can occur when managing objects and commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ObjectNotFound(ObjectId),
    BranchNotFound(BranchName),
    ParentNotFound(CommitId),
    /// Object is in Creating or Loading state.
    ObjectNotReady(ObjectId),
    /// Branch data not yet loaded, need to poll again.
    BranchNotLoaded(BranchName),
    /// Storage operation failed.
    StorageError(StorageError),
    /// Blob not yet loaded, need to poll again.
    BlobNotLoaded(ContentHash),
    /// Blob not found (permanent error).
    BlobNotFound(ContentHash),
}

/// Manages a collection of objects.
#[derive(Debug, Clone, Default)]
pub struct ObjectManager {
    pub objects: HashMap<ObjectId, ObjectState>,
    pub outbox: Vec<StorageRequest>,
    pub inbox: Vec<StorageResponse>,
    next_subscription_id: u64,
    subscriptions: HashMap<SubscriptionId, Subscription>,
    /// Index: (ObjectId, BranchName) → set of SubscriptionIds
    branch_subscribers: HashMap<(ObjectId, BranchName), HashSet<SubscriptionId>>,
    pub subscription_outbox: Vec<SubscriptionUpdate>,
    /// Last timestamp used, for monotonic ordering.
    last_timestamp: u64,
    /// Blobs by content hash (deduplicated storage).
    blobs: HashMap<ContentHash, BlobState>,
    /// Track which commits reference each blob (for GC).
    blob_associations: HashMap<ContentHash, Vec<BlobAssociation>>,
}

impl ObjectManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get next monotonic timestamp (microseconds since epoch).
    /// Guarantees each call returns a value greater than the previous.
    fn next_timestamp(&mut self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_micros() as u64;

        self.last_timestamp = if now > self.last_timestamp {
            now
        } else {
            self.last_timestamp + 1
        };

        self.last_timestamp
    }

    /// Create a new object with optional metadata, returning its id.
    /// Queues a CreateObject request to the outbox.
    pub fn create(&mut self, metadata: Option<HashMap<String, String>>) -> ObjectId {
        let object = Object::new(metadata.clone());
        let id = object.id;

        self.outbox.push(StorageRequest::CreateObject {
            id,
            metadata: metadata.unwrap_or_default(),
        });

        self.objects.insert(id, ObjectState::Creating(object));
        id
    }

    /// Get an object by id (only if Creating or Available).
    pub fn get(&self, id: ObjectId) -> Option<&Object> {
        match self.objects.get(&id)? {
            ObjectState::Creating(obj) | ObjectState::Available(obj) => Some(obj),
            ObjectState::Loading => None,
        }
    }

    /// Get mutable object by id (only if Creating or Available).
    fn get_mut(&mut self, id: ObjectId) -> Option<&mut Object> {
        match self.objects.get_mut(&id)? {
            ObjectState::Creating(obj) | ObjectState::Available(obj) => Some(obj),
            ObjectState::Loading => None,
        }
    }

    /// Check if an object is in Loading state.
    pub fn is_loading(&self, id: ObjectId) -> bool {
        matches!(self.objects.get(&id), Some(ObjectState::Loading))
    }

    /// Add a commit to an object's branch.
    ///
    /// - Creates the branch automatically if parents is empty
    /// - Rejects if object doesn't exist or is Loading
    /// - Rejects if parents are specified but branch doesn't exist
    /// - Rejects if any parent doesn't exist in the branch
    /// - Updates tips: removes parents from tips, adds new commit as tip
    /// - Queues an AppendCommit request to the outbox
    pub fn add_commit(
        &mut self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        parents: Vec<CommitId>,
        content: Vec<u8>,
        author: ObjectId,
        metadata: Option<BTreeMap<String, String>>,
    ) -> Result<CommitId, Error> {
        let branch_name = branch_name.into();

        // Check object state first
        if self.is_loading(object_id) {
            return Err(Error::ObjectNotReady(object_id));
        }

        // Validate object exists and check parents
        {
            let object = self
                .get(object_id)
                .ok_or(Error::ObjectNotFound(object_id))?;

            // If parents is non-empty, branch must exist and contain all parents
            if !parents.is_empty() {
                let branch = object
                    .branches
                    .get(&branch_name)
                    .ok_or_else(|| Error::BranchNotFound(branch_name.clone()))?;

                for parent in &parents {
                    if !branch.commits.contains_key(parent) {
                        return Err(Error::ParentNotFound(*parent));
                    }
                }
            }
        }

        let timestamp = self.next_timestamp();

        let commit = Commit {
            parents: parents.clone(),
            content,
            timestamp,
            author,
            metadata,
            stored_state: StoredState::Pending,
        };

        let commit_id = commit.id();

        // Queue storage request (before mutable borrow of object)
        self.outbox.push(StorageRequest::AppendCommit {
            object_id,
            branch_name: branch_name.clone(),
            commit: commit.clone(),
        });

        // Now mutably borrow and update
        let object = self
            .get_mut(object_id)
            .expect("object existence already validated");

        // Create branch if it doesn't exist (only valid for parentless commits)
        let branch = object
            .branches
            .entry(branch_name.clone())
            .or_insert_with(|| Branch {
                loaded_state: BranchLoadedState::AllCommits,
                ..Default::default()
            });

        // Update tips: remove parents, add new commit
        for parent in &parents {
            branch.tips.remove(parent);
        }
        branch.tips.insert(commit_id);

        branch.commits.insert(commit_id, commit);

        // Notify subscribers of updated frontier
        self.notify_subscribers_of_commit(object_id, &branch_name);

        Ok(commit_id)
    }

    /// Get tip IDs for a branch.
    /// Requires at least TipIdsOnly load depth for Loading objects.
    pub fn get_tip_ids(
        &mut self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> Result<&HashSet<CommitId>, Error> {
        let branch_name = branch_name.into();

        // For Loading state, check if we have sufficient depth
        if let Some(ObjectState::Loading) = self.objects.get(&object_id) {
            // Queue a load request and return not loaded
            self.outbox.push(StorageRequest::LoadObjectBranch {
                object_id,
                branch_name: branch_name.clone(),
                depth: LoadDepth::TipIdsOnly,
            });
            return Err(Error::BranchNotLoaded(branch_name));
        }

        let object = self
            .get(object_id)
            .ok_or(Error::ObjectNotFound(object_id))?;

        let branch = object
            .branches
            .get(&branch_name)
            .ok_or(Error::BranchNotFound(branch_name.clone()))?;

        // For Loading objects, check load depth
        if branch.loaded_state == BranchLoadedState::NotLoaded {
            return Err(Error::BranchNotLoaded(branch_name));
        }

        Ok(&branch.tips)
    }

    /// Get the tips (frontier commits) as full Commit structs for a branch.
    /// Requires at least TipsOnly load depth for Loading objects.
    pub fn get_tips(
        &mut self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> Result<HashMap<CommitId, &Commit>, Error> {
        let branch_name = branch_name.into();

        // For Loading state, check if we have sufficient depth
        if let Some(ObjectState::Loading) = self.objects.get(&object_id) {
            self.outbox.push(StorageRequest::LoadObjectBranch {
                object_id,
                branch_name: branch_name.clone(),
                depth: LoadDepth::TipsOnly,
            });
            return Err(Error::BranchNotLoaded(branch_name));
        }

        let object = self
            .get(object_id)
            .ok_or(Error::ObjectNotFound(object_id))?;

        let branch = object
            .branches
            .get(&branch_name)
            .ok_or(Error::BranchNotFound(branch_name.clone()))?;

        // Check sufficient load depth
        match branch.loaded_state {
            BranchLoadedState::NotLoaded | BranchLoadedState::TipIdsOnly => {
                return Err(Error::BranchNotLoaded(branch_name));
            }
            _ => {}
        }

        let tips: HashMap<CommitId, &Commit> = branch
            .tips
            .iter()
            .filter_map(|id| branch.commits.get(id).map(|c| (*id, c)))
            .collect();

        Ok(tips)
    }

    /// Get all commits in a branch.
    /// Requires AllCommits load depth for Loading objects.
    pub fn get_commits(
        &mut self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> Result<&HashMap<CommitId, Commit>, Error> {
        let branch_name = branch_name.into();

        // For Loading state, queue request
        if let Some(ObjectState::Loading) = self.objects.get(&object_id) {
            self.outbox.push(StorageRequest::LoadObjectBranch {
                object_id,
                branch_name: branch_name.clone(),
                depth: LoadDepth::AllCommits,
            });
            return Err(Error::BranchNotLoaded(branch_name));
        }

        let object = self
            .get(object_id)
            .ok_or(Error::ObjectNotFound(object_id))?;

        let branch = object
            .branches
            .get(&branch_name)
            .ok_or(Error::BranchNotFound(branch_name.clone()))?;

        // Check sufficient load depth
        if branch.loaded_state != BranchLoadedState::AllCommits {
            return Err(Error::BranchNotLoaded(branch_name));
        }

        Ok(&branch.commits)
    }

    /// Associate a blob with a commit, storing the data if new.
    ///
    /// Deduplicates by content hash. Returns full BlobId for addressing.
    /// Queues StoreBlob (if new) and AssociateBlob requests.
    pub fn associate_blob(
        &mut self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        commit_id: CommitId,
        data: Vec<u8>,
    ) -> BlobId {
        let branch_name = branch_name.into();

        // Compute content hash
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let content_hash = ContentHash(*hasher.finalize().as_bytes());

        // Create association
        let association = BlobAssociation {
            object_id,
            branch_name: branch_name.clone(),
            commit_id,
        };

        // Check if blob already exists
        if let std::collections::hash_map::Entry::Vacant(e) = self.blobs.entry(content_hash) {
            // New blob - store data and add association
            e.insert(BlobState::Available {
                data: data.clone(),
                stored_state: StoredState::Pending,
            });

            // Queue store request
            self.outbox
                .push(StorageRequest::StoreBlob { content_hash, data });
        }

        // Add association (whether blob was new or existing)
        self.blob_associations
            .entry(content_hash)
            .or_default()
            .push(association.clone());

        // Always queue association request
        self.outbox.push(StorageRequest::AssociateBlob {
            content_hash,
            object_id: association.object_id,
            branch_name: association.branch_name,
            commit_id: association.commit_id,
        });

        BlobId {
            object_id,
            branch_name,
            commit_id,
            content_hash,
        }
    }

    /// Load a blob by its identifier.
    ///
    /// Returns the data if available, or triggers a load if not present.
    pub fn load_blob(&mut self, blob_id: &BlobId) -> Result<&[u8], Error> {
        use std::collections::hash_map::Entry;

        let content_hash = blob_id.content_hash;

        // Check if blob exists, if not start loading
        if let Entry::Vacant(e) = self.blobs.entry(content_hash) {
            e.insert(BlobState::Loading);
            self.outbox.push(StorageRequest::LoadBlob { content_hash });
            return Err(Error::BlobNotLoaded(content_hash));
        }

        match self.blobs.get(&content_hash) {
            Some(BlobState::Available { data, .. }) => Ok(data),
            Some(BlobState::Loading) => Err(Error::BlobNotLoaded(content_hash)),
            Some(BlobState::NotFound) => Err(Error::BlobNotFound(content_hash)),
            None => unreachable!(), // Entry was occupied
        }
    }

    /// Process responses from the inbox, updating object/commit states.
    pub fn process_storage_responses(&mut self) {
        let responses = std::mem::take(&mut self.inbox);

        for response in responses {
            match response {
                StorageResponse::CreateObject { id, result } => {
                    if let Ok(()) = result {
                        // Transition Creating -> Available
                        if let Some(ObjectState::Creating(obj)) = self.objects.remove(&id) {
                            self.objects.insert(id, ObjectState::Available(obj));
                        }
                    }
                    // On error, object stays in Creating state
                }
                StorageResponse::AppendCommit {
                    object_id,
                    commit_id,
                    result,
                } => {
                    if let Some(object) = self.get_mut(object_id) {
                        for branch in object.branches.values_mut() {
                            if let Some(commit) = branch.commits.get_mut(&commit_id) {
                                commit.stored_state = match result {
                                    Ok(()) => StoredState::Stored,
                                    Err(ref e) => StoredState::Errored(format!("{:?}", e)),
                                };
                            }
                        }
                    }
                }
                StorageResponse::LoadObjectBranch {
                    object_id,
                    branch_name,
                    result,
                } => {
                    if let Ok(loaded_branch) = result {
                        let commits = loaded_branch.commits.clone();
                        let tips = loaded_branch.tips.clone();

                        // If object was Loading, create it as Available
                        if matches!(self.objects.get(&object_id), Some(ObjectState::Loading)) {
                            let mut object = Object {
                                id: object_id,
                                metadata: HashMap::new(),
                                branches: HashMap::new(),
                            };
                            object.branches.insert(
                                branch_name.clone(),
                                Branch {
                                    commits: loaded_branch.commits,
                                    tips: loaded_branch.tips,
                                    loaded_state: BranchLoadedState::AllCommits,
                                },
                            );
                            self.objects
                                .insert(object_id, ObjectState::Available(object));
                        } else if let Some(object) = self.get_mut(object_id) {
                            // Merge loaded branch data
                            let branch = object
                                .branches
                                .entry(branch_name.clone())
                                .or_insert_with(Branch::default);
                            branch.tips = loaded_branch.tips;
                            branch.commits.extend(loaded_branch.commits);
                            branch.loaded_state = BranchLoadedState::AllCommits;
                        }

                        // Notify subscribers of loaded commits
                        self.notify_subscribers_of_load(object_id, &branch_name, &commits, &tips);
                    }
                }
                StorageResponse::StoreBlob {
                    content_hash,
                    result,
                } => {
                    if let Some(BlobState::Available { stored_state, .. }) =
                        self.blobs.get_mut(&content_hash)
                    {
                        *stored_state = match result {
                            Ok(()) => StoredState::Stored,
                            Err(ref e) => StoredState::Errored(format!("{:?}", e)),
                        };
                    }
                }
                StorageResponse::LoadBlob {
                    content_hash,
                    result,
                } => match result {
                    Ok(data) => {
                        self.blobs.insert(
                            content_hash,
                            BlobState::Available {
                                data,
                                stored_state: StoredState::Stored,
                            },
                        );
                    }
                    Err(StorageError::NotFound) => {
                        // Mark as not found - subsequent load_blob will return BlobNotFound
                        self.blobs.insert(content_hash, BlobState::NotFound);
                    }
                    Err(_) => {
                        // Other errors - keep as Loading, could retry
                    }
                },
                StorageResponse::AssociateBlob { .. } => {
                    // Could track association state, but for now just ignore
                }
                StorageResponse::LoadBlobAssociations {
                    content_hash,
                    result,
                } => {
                    if let Ok(associations) = result {
                        // Merge loaded associations
                        let entry = self.blob_associations.entry(content_hash).or_default();
                        for assoc in associations {
                            if !entry.contains(&assoc) {
                                entry.push(assoc);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Take all pending requests from the outbox for the driver to process.
    pub fn take_requests(&mut self) -> Vec<StorageRequest> {
        std::mem::take(&mut self.outbox)
    }

    /// Add a response to the inbox (used by drivers).
    pub fn push_response(&mut self, response: StorageResponse) {
        self.inbox.push(response);
    }

    /// Start loading an object from storage.
    pub fn start_loading(&mut self, object_id: ObjectId) {
        self.objects.insert(object_id, ObjectState::Loading);
    }

    /// Subscribe to updates on a branch.
    ///
    /// If the branch is already loaded at sufficient depth, queues an immediate
    /// update with existing commits. Otherwise, queues a load request.
    pub fn subscribe(
        &mut self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        depth: LoadDepth,
    ) -> SubscriptionId {
        let branch_name = branch_name.into();
        let id = SubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;

        let subscription = Subscription {
            object_id,
            branch_name: branch_name.clone(),
        };
        self.subscriptions.insert(id, subscription);

        self.branch_subscribers
            .entry((object_id, branch_name.clone()))
            .or_default()
            .insert(id);

        // Check if branch is already loaded at sufficient depth
        if let Some(object) = self.get(object_id)
            && let Some(branch) = object.branches.get(&branch_name)
            && Self::has_sufficient_depth(branch.loaded_state, depth)
        {
            let commit_ids = Self::tips_by_timestamp(&branch.commits, &branch.tips);
            self.subscription_outbox.push(SubscriptionUpdate {
                subscription_id: id,
                object_id,
                branch_name,
                commit_ids,
            });
            return id;
        }

        // Not loaded or insufficient depth - queue load request
        self.outbox.push(StorageRequest::LoadObjectBranch {
            object_id,
            branch_name,
            depth,
        });

        id
    }

    /// Unsubscribe from updates.
    ///
    /// Removes the subscription and any pending updates for it.
    pub fn unsubscribe(&mut self, subscription_id: SubscriptionId) {
        if let Some(sub) = self.subscriptions.remove(&subscription_id)
            && let Some(subscribers) = self
                .branch_subscribers
                .get_mut(&(sub.object_id, sub.branch_name))
        {
            subscribers.remove(&subscription_id);
        }

        // Remove pending updates for this subscription
        self.subscription_outbox
            .retain(|u| u.subscription_id != subscription_id);
    }

    /// Take all pending subscription updates.
    pub fn take_subscription_updates(&mut self) -> Vec<SubscriptionUpdate> {
        std::mem::take(&mut self.subscription_outbox)
    }

    /// Check if loaded_state satisfies the required depth.
    fn has_sufficient_depth(loaded_state: BranchLoadedState, required: LoadDepth) -> bool {
        match required {
            LoadDepth::TipIdsOnly => loaded_state != BranchLoadedState::NotLoaded,
            LoadDepth::TipsOnly => matches!(
                loaded_state,
                BranchLoadedState::TipsOnly | BranchLoadedState::AllCommits
            ),
            LoadDepth::AllCommits => loaded_state == BranchLoadedState::AllCommits,
        }
    }

    /// Get the current frontier (tips) sorted by timestamp (oldest first).
    fn tips_by_timestamp(
        commits: &HashMap<CommitId, Commit>,
        tips: &HashSet<CommitId>,
    ) -> Vec<CommitId> {
        let mut tip_vec: Vec<_> = tips.iter().copied().collect();
        tip_vec.sort_by_key(|id| commits.get(id).map(|c| c.timestamp).unwrap_or(0));
        tip_vec
    }

    /// Notify subscribers about a commit change - sends current frontier sorted by timestamp.
    fn notify_subscribers_of_commit(&mut self, object_id: ObjectId, branch_name: &BranchName) {
        let key = (object_id, branch_name.clone());
        if let Some(subscriber_ids) = self.branch_subscribers.get(&key).cloned() {
            // Get current tips from the branch
            let commit_ids = if let Some(object) = self.get(object_id) {
                if let Some(branch) = object.branches.get(branch_name) {
                    Self::tips_by_timestamp(&branch.commits, &branch.tips)
                } else {
                    vec![]
                }
            } else {
                vec![]
            };

            for sub_id in subscriber_ids {
                self.subscription_outbox.push(SubscriptionUpdate {
                    subscription_id: sub_id,
                    object_id,
                    branch_name: branch_name.clone(),
                    commit_ids: commit_ids.clone(),
                });
            }
        }
    }

    /// Notify subscribers about loaded commits - sends current frontier sorted by timestamp.
    fn notify_subscribers_of_load(
        &mut self,
        object_id: ObjectId,
        branch_name: &BranchName,
        commits: &HashMap<CommitId, Commit>,
        tips: &HashSet<CommitId>,
    ) {
        let key = (object_id, branch_name.clone());
        if let Some(subscriber_ids) = self.branch_subscribers.get(&key).cloned() {
            let commit_ids = Self::tips_by_timestamp(commits, tips);
            for sub_id in subscriber_ids {
                self.subscription_outbox.push(SubscriptionUpdate {
                    subscription_id: sub_id,
                    object_id,
                    branch_name: branch_name.clone(),
                    commit_ids: commit_ids.clone(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{ContentHash, StorageError};

    #[test]
    fn create_object_without_metadata() {
        let mut manager = ObjectManager::new();
        let id = manager.create(None);

        let object = manager.get(id).expect("object should exist");
        assert_eq!(object.id, id);
        assert!(object.metadata.is_empty());
        assert!(object.branches.is_empty());
    }

    #[test]
    fn create_object_with_metadata() {
        let mut manager = ObjectManager::new();
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "test".to_string());

        let id = manager.create(Some(metadata));

        let object = manager.get(id).expect("object should exist");
        assert_eq!(object.metadata.get("name"), Some(&"test".to_string()));
    }

    #[test]
    fn get_nonexistent_object_returns_none() {
        let manager = ObjectManager::new();
        let fake_id = ObjectId::new();

        assert!(manager.get(fake_id).is_none());
    }

    // --- add_commit tests ---

    #[test]
    fn add_commit_rejects_unknown_object() {
        let mut manager = ObjectManager::new();
        let fake_object_id = ObjectId::new();
        let author = ObjectId::new();

        let result = manager.add_commit(
            fake_object_id,
            "main",
            vec![],
            b"content".to_vec(),
            author,
            None,
        );

        assert_eq!(result, Err(Error::ObjectNotFound(fake_object_id)));
    }

    #[test]
    fn add_commit_creates_branch_for_parentless_commit() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let commit_id = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .expect("should succeed");

        let object = manager.get(object_id).unwrap();
        assert!(object.branches.contains_key(&BranchName::new("main")));

        let branch = &object.branches[&BranchName::new("main")];
        assert!(branch.commits.contains_key(&commit_id));
    }

    #[test]
    fn add_commit_rejects_unknown_branch_with_parents() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();
        let fake_parent = CommitId([0u8; 32]);

        let result = manager.add_commit(
            object_id,
            "nonexistent",
            vec![fake_parent],
            b"content".to_vec(),
            author,
            None,
        );

        assert_eq!(
            result,
            Err(Error::BranchNotFound(BranchName::new("nonexistent")))
        );
    }

    #[test]
    fn add_commit_rejects_unknown_parent() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        // Create branch with initial commit
        manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        let fake_parent = CommitId([0u8; 32]);
        let result = manager.add_commit(
            object_id,
            "main",
            vec![fake_parent],
            b"child".to_vec(),
            author,
            None,
        );

        assert_eq!(result, Err(Error::ParentNotFound(fake_parent)));
    }

    #[test]
    fn add_commit_with_valid_parent_succeeds() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let parent_id = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        let child_id = manager
            .add_commit(
                object_id,
                "main",
                vec![parent_id],
                b"child".to_vec(),
                author,
                None,
            )
            .expect("should succeed");

        let commits = manager.get_commits(object_id, "main").unwrap();
        assert!(commits.contains_key(&child_id));
        assert_eq!(commits[&child_id].parents, vec![parent_id]);
    }

    // --- tips management tests ---

    #[test]
    fn parentless_commit_becomes_tip() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let commit_id = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        let tip_ids = manager.get_tip_ids(object_id, "main").unwrap();
        assert_eq!(tip_ids.len(), 1);
        assert!(tip_ids.contains(&commit_id));
    }

    #[test]
    fn child_commit_replaces_parent_in_tips() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let parent_id = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        let child_id = manager
            .add_commit(
                object_id,
                "main",
                vec![parent_id],
                b"child".to_vec(),
                author,
                None,
            )
            .unwrap();

        let tip_ids = manager.get_tip_ids(object_id, "main").unwrap();
        assert_eq!(tip_ids.len(), 1);
        assert!(!tip_ids.contains(&parent_id));
        assert!(tip_ids.contains(&child_id));
    }

    #[test]
    fn diverging_commits_create_multiple_tips() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let root = manager
            .add_commit(object_id, "main", vec![], b"root".to_vec(), author, None)
            .unwrap();

        let branch_a = manager
            .add_commit(
                object_id,
                "main",
                vec![root],
                b"branch_a".to_vec(),
                author,
                None,
            )
            .unwrap();

        let branch_b = manager
            .add_commit(
                object_id,
                "main",
                vec![root],
                b"branch_b".to_vec(),
                author,
                None,
            )
            .unwrap();

        let tip_ids = manager.get_tip_ids(object_id, "main").unwrap();
        assert_eq!(tip_ids.len(), 2);
        assert!(tip_ids.contains(&branch_a));
        assert!(tip_ids.contains(&branch_b));
    }

    #[test]
    fn merge_commit_consolidates_tips() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let root = manager
            .add_commit(object_id, "main", vec![], b"root".to_vec(), author, None)
            .unwrap();

        let branch_a = manager
            .add_commit(
                object_id,
                "main",
                vec![root],
                b"branch_a".to_vec(),
                author,
                None,
            )
            .unwrap();

        let branch_b = manager
            .add_commit(
                object_id,
                "main",
                vec![root],
                b"branch_b".to_vec(),
                author,
                None,
            )
            .unwrap();

        // Merge both branches
        let merge = manager
            .add_commit(
                object_id,
                "main",
                vec![branch_a, branch_b],
                b"merge".to_vec(),
                author,
                None,
            )
            .unwrap();

        let tip_ids = manager.get_tip_ids(object_id, "main").unwrap();
        assert_eq!(tip_ids.len(), 1);
        assert!(tip_ids.contains(&merge));
    }

    #[test]
    fn multiple_roots_create_multiple_tips() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let root1 = manager
            .add_commit(object_id, "main", vec![], b"root1".to_vec(), author, None)
            .unwrap();

        let root2 = manager
            .add_commit(object_id, "main", vec![], b"root2".to_vec(), author, None)
            .unwrap();

        let tip_ids = manager.get_tip_ids(object_id, "main").unwrap();
        assert_eq!(tip_ids.len(), 2);
        assert!(tip_ids.contains(&root1));
        assert!(tip_ids.contains(&root2));
    }

    // --- getter tests ---

    #[test]
    fn get_tip_ids_rejects_unknown_object() {
        let mut manager = ObjectManager::new();
        let fake_id = ObjectId::new();

        let result = manager.get_tip_ids(fake_id, "main");
        assert_eq!(result, Err(Error::ObjectNotFound(fake_id)));
    }

    #[test]
    fn get_tip_ids_rejects_unknown_branch() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);

        let result = manager.get_tip_ids(object_id, "nonexistent");
        assert_eq!(
            result,
            Err(Error::BranchNotFound(BranchName::new("nonexistent")))
        );
    }

    #[test]
    fn get_tips_returns_commit_structs() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let commit_id = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        let tips = manager.get_tips(object_id, "main").unwrap();
        assert_eq!(tips.len(), 1);
        assert!(tips.contains_key(&commit_id));
        assert_eq!(tips[&commit_id].content, b"initial".to_vec());
    }

    #[test]
    fn get_commits_returns_all_commits() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let c1 = manager
            .add_commit(object_id, "main", vec![], b"first".to_vec(), author, None)
            .unwrap();

        let c2 = manager
            .add_commit(
                object_id,
                "main",
                vec![c1],
                b"second".to_vec(),
                author,
                None,
            )
            .unwrap();

        let c3 = manager
            .add_commit(object_id, "main", vec![c2], b"third".to_vec(), author, None)
            .unwrap();

        let commits = manager.get_commits(object_id, "main").unwrap();
        assert_eq!(commits.len(), 3);
        assert!(commits.contains_key(&c1));
        assert!(commits.contains_key(&c2));
        assert!(commits.contains_key(&c3));
    }

    #[test]
    fn get_commits_rejects_unknown_object() {
        let mut manager = ObjectManager::new();
        let fake_id = ObjectId::new();

        let result = manager.get_commits(fake_id, "main");
        assert!(matches!(result, Err(Error::ObjectNotFound(id)) if id == fake_id));
    }

    #[test]
    fn get_commits_rejects_unknown_branch() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);

        let result = manager.get_commits(object_id, "nonexistent");
        assert!(matches!(result, Err(Error::BranchNotFound(ref name)) if name.0 == "nonexistent"));
    }

    // --- persistence tests ---

    #[test]
    fn create_queues_storage_request() {
        let mut manager = ObjectManager::new();
        let id = manager.create(None);

        let requests = manager.take_requests();
        assert_eq!(requests.len(), 1);
        assert!(
            matches!(&requests[0], StorageRequest::CreateObject { id: req_id, .. } if *req_id == id)
        );
    }

    #[test]
    fn add_commit_queues_storage_request() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        // Clear the create request
        manager.take_requests();

        let commit_id = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        let requests = manager.take_requests();
        assert_eq!(requests.len(), 1);
        assert!(matches!(
            &requests[0],
            StorageRequest::AppendCommit { object_id: oid, branch_name, commit }
            if *oid == object_id && branch_name.0 == "main" && commit.id() == commit_id
        ));
    }

    #[test]
    fn process_create_response_transitions_to_available() {
        let mut manager = ObjectManager::new();
        let id = manager.create(None);

        // Object starts in Creating state
        assert!(matches!(
            manager.objects.get(&id),
            Some(ObjectState::Creating(_))
        ));

        // Process successful response
        manager.push_response(StorageResponse::CreateObject { id, result: Ok(()) });
        manager.process_storage_responses();

        // Object should now be Available
        assert!(matches!(
            manager.objects.get(&id),
            Some(ObjectState::Available(_))
        ));
    }

    #[test]
    fn process_commit_response_updates_stored_state() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let commit_id = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        // Commit starts as Pending
        let commit =
            &manager.get(object_id).unwrap().branches[&BranchName::new("main")].commits[&commit_id];
        assert_eq!(commit.stored_state, StoredState::Pending);

        // Process successful response
        manager.push_response(StorageResponse::AppendCommit {
            object_id,
            commit_id,
            result: Ok(()),
        });
        manager.process_storage_responses();

        // Commit should now be Stored
        let commit =
            &manager.get(object_id).unwrap().branches[&BranchName::new("main")].commits[&commit_id];
        assert_eq!(commit.stored_state, StoredState::Stored);
    }

    #[test]
    fn loading_object_returns_not_loaded_error() {
        let mut manager = ObjectManager::new();
        let object_id = ObjectId::new();

        manager.start_loading(object_id);

        let result = manager.get_tip_ids(object_id, "main");
        assert!(matches!(result, Err(Error::BranchNotLoaded(_))));

        // Should have queued a load request
        let requests = manager.take_requests();
        assert_eq!(requests.len(), 1);
        assert!(matches!(
            &requests[0],
            StorageRequest::LoadObjectBranch { object_id: oid, depth: LoadDepth::TipIdsOnly, .. }
            if *oid == object_id
        ));
    }

    // --- subscription tests ---

    #[test]
    fn subscribe_to_loaded_branch_gets_immediate_update_with_frontier() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let c1 = manager
            .add_commit(object_id, "main", vec![], b"first".to_vec(), author, None)
            .unwrap();
        let c2 = manager
            .add_commit(
                object_id,
                "main",
                vec![c1],
                b"second".to_vec(),
                author,
                None,
            )
            .unwrap();

        // Clear any updates from add_commit (no subscribers yet)
        manager.take_subscription_updates();

        let sub_id = manager.subscribe(object_id, "main", LoadDepth::AllCommits);

        let updates = manager.take_subscription_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(updates[0].object_id, object_id);
        assert_eq!(updates[0].branch_name, BranchName::new("main"));
        // Only the current frontier (tip), not all commits
        assert_eq!(updates[0].commit_ids, vec![c2]);
    }

    #[test]
    fn subscribe_to_unloaded_branch_triggers_load_request() {
        let mut manager = ObjectManager::new();
        let object_id = ObjectId::new();
        manager.start_loading(object_id);

        manager.take_requests(); // Clear any previous requests

        let _sub_id = manager.subscribe(object_id, "main", LoadDepth::AllCommits);

        // Should get no immediate update
        let updates = manager.take_subscription_updates();
        assert!(updates.is_empty());

        // Should have queued a load request
        let requests = manager.take_requests();
        assert_eq!(requests.len(), 1);
        assert!(matches!(
            &requests[0],
            StorageRequest::LoadObjectBranch { object_id: oid, branch_name, depth: LoadDepth::AllCommits }
            if *oid == object_id && branch_name.0 == "main"
        ));
    }

    #[test]
    fn subscriber_gets_update_on_load_response() {
        use crate::storage::LoadedBranch;

        let mut manager = ObjectManager::new();
        let object_id = ObjectId::new();
        manager.start_loading(object_id);

        let sub_id = manager.subscribe(object_id, "main", LoadDepth::AllCommits);
        manager.take_requests();
        manager.take_subscription_updates();

        // Create test commits for the loaded branch
        let commit = Commit {
            parents: vec![],
            content: b"loaded".to_vec(),
            timestamp: 12345,
            author: ObjectId::new(),
            metadata: None,
            stored_state: StoredState::Stored,
        };
        let commit_id = commit.id();
        let mut commits = HashMap::new();
        commits.insert(commit_id, commit);
        let mut tips = HashSet::new();
        tips.insert(commit_id);

        manager.push_response(StorageResponse::LoadObjectBranch {
            object_id,
            branch_name: BranchName::new("main"),
            result: Ok(LoadedBranch { tips, commits }),
        });
        manager.process_storage_responses();

        let updates = manager.take_subscription_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(updates[0].commit_ids, vec![commit_id]);
    }

    #[test]
    fn add_commit_notifies_subscriber() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let c1 = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        // Subscribe after initial commit
        let sub_id = manager.subscribe(object_id, "main", LoadDepth::AllCommits);
        manager.take_subscription_updates(); // Clear initial update

        // Add another commit
        let c2 = manager
            .add_commit(
                object_id,
                "main",
                vec![c1],
                b"second".to_vec(),
                author,
                None,
            )
            .unwrap();

        let updates = manager.take_subscription_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].subscription_id, sub_id);
        assert_eq!(updates[0].commit_ids, vec![c2]);
    }

    #[test]
    fn multiple_subscribers_each_get_updates() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let c1 = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        let sub1 = manager.subscribe(object_id, "main", LoadDepth::AllCommits);
        let sub2 = manager.subscribe(object_id, "main", LoadDepth::AllCommits);
        manager.take_subscription_updates(); // Clear initial updates

        let c2 = manager
            .add_commit(
                object_id,
                "main",
                vec![c1],
                b"second".to_vec(),
                author,
                None,
            )
            .unwrap();

        let updates = manager.take_subscription_updates();
        assert_eq!(updates.len(), 2);

        let sub_ids: HashSet<_> = updates.iter().map(|u| u.subscription_id).collect();
        assert!(sub_ids.contains(&sub1));
        assert!(sub_ids.contains(&sub2));

        for update in &updates {
            assert_eq!(update.commit_ids, vec![c2]);
        }
    }

    #[test]
    fn unsubscribe_stops_updates() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let c1 = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        let sub_id = manager.subscribe(object_id, "main", LoadDepth::AllCommits);
        manager.take_subscription_updates();

        manager.unsubscribe(sub_id);

        // Add a commit after unsubscribing
        manager
            .add_commit(
                object_id,
                "main",
                vec![c1],
                b"second".to_vec(),
                author,
                None,
            )
            .unwrap();

        let updates = manager.take_subscription_updates();
        assert!(updates.is_empty());
    }

    #[test]
    fn unsubscribe_clears_pending_updates() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        let sub_id = manager.subscribe(object_id, "main", LoadDepth::AllCommits);
        // Don't take updates yet - they're pending

        manager.unsubscribe(sub_id);

        let updates = manager.take_subscription_updates();
        assert!(updates.is_empty());
    }

    #[test]
    fn subscribe_tips_only_gets_only_tips() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let c1 = manager
            .add_commit(object_id, "main", vec![], b"first".to_vec(), author, None)
            .unwrap();
        let c2 = manager
            .add_commit(
                object_id,
                "main",
                vec![c1],
                b"second".to_vec(),
                author,
                None,
            )
            .unwrap();

        let _sub_id = manager.subscribe(object_id, "main", LoadDepth::TipsOnly);

        let updates = manager.take_subscription_updates();
        assert_eq!(updates.len(), 1);
        // Only the tip commit, not all commits
        assert_eq!(updates[0].commit_ids.len(), 1);
        assert!(updates[0].commit_ids.contains(&c2));
        assert!(!updates[0].commit_ids.contains(&c1));
    }

    #[test]
    fn frontier_evolves_through_diamond_graph() {
        // Test frontier evolution: root -> (a, b) -> merge
        // Subscriber should see frontier evolve as commits are added
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let root = manager
            .add_commit(object_id, "main", vec![], b"root".to_vec(), author, None)
            .unwrap();

        // Subscribe after root
        let _sub_id = manager.subscribe(object_id, "main", LoadDepth::AllCommits);

        // Initial update should show [root] as frontier
        let updates = manager.take_subscription_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].commit_ids, vec![root]);

        // Add 'a' - frontier becomes [a]
        let a = manager
            .add_commit(object_id, "main", vec![root], b"a".to_vec(), author, None)
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].commit_ids, vec![a]);

        // Add 'b' (also from root) - frontier becomes [a, b] sorted by timestamp
        let b = manager
            .add_commit(object_id, "main", vec![root], b"b".to_vec(), author, None)
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].commit_ids.len(), 2);
        // 'a' should come before 'b' (earlier timestamp, monotonic)
        assert_eq!(updates[0].commit_ids[0], a);
        assert_eq!(updates[0].commit_ids[1], b);

        // Merge a and b - frontier becomes [merge]
        let merge = manager
            .add_commit(
                object_id,
                "main",
                vec![a, b],
                b"merge".to_vec(),
                author,
                None,
            )
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].commit_ids, vec![merge]);
    }

    #[test]
    fn subscription_ids_are_unique() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);

        let sub1 = manager.subscribe(object_id, "main", LoadDepth::AllCommits);
        let sub2 = manager.subscribe(object_id, "main", LoadDepth::AllCommits);
        let sub3 = manager.subscribe(object_id, "other", LoadDepth::TipsOnly);

        assert_ne!(sub1, sub2);
        assert_ne!(sub2, sub3);
        assert_ne!(sub1, sub3);
    }

    #[test]
    fn frontier_with_extended_divergence() {
        // Test: root -> a1 -> a2 -> a3
        //            -> b1 -> b2
        // Then merge. Frontier peels forward by timestamp.
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let root = manager
            .add_commit(object_id, "main", vec![], b"root".to_vec(), author, None)
            .unwrap();

        let _sub_id = manager.subscribe(object_id, "main", LoadDepth::AllCommits);
        manager.take_subscription_updates(); // Clear [root]

        // a1 from root
        let a1 = manager
            .add_commit(object_id, "main", vec![root], b"a1".to_vec(), author, None)
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids, vec![a1]);

        // b1 from root (diverge)
        let b1 = manager
            .add_commit(object_id, "main", vec![root], b"b1".to_vec(), author, None)
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids.len(), 2);
        assert_eq!(updates[0].commit_ids[0], a1); // a1 earlier
        assert_eq!(updates[0].commit_ids[1], b1);

        // a2 extends a branch
        let a2 = manager
            .add_commit(object_id, "main", vec![a1], b"a2".to_vec(), author, None)
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids.len(), 2);
        // b1 is earlier than a2, so order is [b1, a2]
        assert_eq!(updates[0].commit_ids[0], b1);
        assert_eq!(updates[0].commit_ids[1], a2);

        // b2 extends b branch
        let b2 = manager
            .add_commit(object_id, "main", vec![b1], b"b2".to_vec(), author, None)
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids.len(), 2);
        // a2 is earlier than b2
        assert_eq!(updates[0].commit_ids[0], a2);
        assert_eq!(updates[0].commit_ids[1], b2);

        // a3 extends a branch
        let a3 = manager
            .add_commit(object_id, "main", vec![a2], b"a3".to_vec(), author, None)
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids.len(), 2);
        // b2 is earlier than a3
        assert_eq!(updates[0].commit_ids[0], b2);
        assert_eq!(updates[0].commit_ids[1], a3);

        // Merge a3 and b2
        let merge = manager
            .add_commit(
                object_id,
                "main",
                vec![a3, b2],
                b"merge".to_vec(),
                author,
                None,
            )
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids, vec![merge]);
    }

    #[test]
    fn frontier_with_three_way_divergence() {
        // Test: root -> a1 -> a2
        //            -> b1
        //            -> c1 -> c2
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let root = manager
            .add_commit(object_id, "main", vec![], b"root".to_vec(), author, None)
            .unwrap();

        let _sub_id = manager.subscribe(object_id, "main", LoadDepth::AllCommits);
        manager.take_subscription_updates();

        // First branch
        let a1 = manager
            .add_commit(object_id, "main", vec![root], b"a1".to_vec(), author, None)
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids, vec![a1]);

        // Second branch diverges
        let b1 = manager
            .add_commit(object_id, "main", vec![root], b"b1".to_vec(), author, None)
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids.len(), 2);

        // Third branch diverges - now three concurrent tips
        let c1 = manager
            .add_commit(object_id, "main", vec![root], b"c1".to_vec(), author, None)
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids.len(), 3);
        assert!(updates[0].commit_ids.contains(&a1));
        assert!(updates[0].commit_ids.contains(&b1));
        assert!(updates[0].commit_ids.contains(&c1));
        // Verify timestamp order: a1 < b1 < c1
        assert_eq!(updates[0].commit_ids[0], a1);
        assert_eq!(updates[0].commit_ids[1], b1);
        assert_eq!(updates[0].commit_ids[2], c1);

        // Extend a and c branches
        let a2 = manager
            .add_commit(object_id, "main", vec![a1], b"a2".to_vec(), author, None)
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids.len(), 3);
        // b1 and c1 are earlier than a2
        assert!(updates[0].commit_ids.contains(&b1));
        assert!(updates[0].commit_ids.contains(&c1));
        assert!(updates[0].commit_ids.contains(&a2));

        let c2 = manager
            .add_commit(object_id, "main", vec![c1], b"c2".to_vec(), author, None)
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids.len(), 3);
        assert!(updates[0].commit_ids.contains(&b1));
        assert!(updates[0].commit_ids.contains(&a2));
        assert!(updates[0].commit_ids.contains(&c2));

        // Partial merge: merge a2 and b1
        let merge_ab = manager
            .add_commit(
                object_id,
                "main",
                vec![a2, b1],
                b"merge_ab".to_vec(),
                author,
                None,
            )
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids.len(), 2);
        assert!(updates[0].commit_ids.contains(&c2));
        assert!(updates[0].commit_ids.contains(&merge_ab));

        // Final merge
        let merge_all = manager
            .add_commit(
                object_id,
                "main",
                vec![merge_ab, c2],
                b"merge_all".to_vec(),
                author,
                None,
            )
            .unwrap();
        let updates = manager.take_subscription_updates();
        assert_eq!(updates[0].commit_ids, vec![merge_all]);
    }

    // --- blob tests ---

    #[test]
    fn associate_blob_stores_and_returns_blob_id() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let commit_id = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        manager.take_requests(); // Clear previous requests

        let data = b"hello blob".to_vec();
        let blob_id = manager.associate_blob(object_id, "main", commit_id, data.clone());

        // Verify blob_id has correct fields
        assert_eq!(blob_id.object_id, object_id);
        assert_eq!(blob_id.branch_name, BranchName::new("main"));
        assert_eq!(blob_id.commit_id, commit_id);

        // Verify requests queued
        let requests = manager.take_requests();
        assert_eq!(requests.len(), 2);
        assert!(matches!(
            &requests[0],
            StorageRequest::StoreBlob { content_hash, data: d }
            if *content_hash == blob_id.content_hash && *d == data
        ));
        assert!(matches!(
            &requests[1],
            StorageRequest::AssociateBlob { content_hash, .. }
            if *content_hash == blob_id.content_hash
        ));
    }

    #[test]
    fn associate_blob_deduplicates_by_content() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let c1 = manager
            .add_commit(object_id, "main", vec![], b"first".to_vec(), author, None)
            .unwrap();
        let c2 = manager
            .add_commit(
                object_id,
                "main",
                vec![c1],
                b"second".to_vec(),
                author,
                None,
            )
            .unwrap();

        manager.take_requests();

        let data = b"same data".to_vec();

        // First association
        let blob_id1 = manager.associate_blob(object_id, "main", c1, data.clone());
        let requests1 = manager.take_requests();

        // Second association with same data
        let blob_id2 = manager.associate_blob(object_id, "main", c2, data.clone());
        let requests2 = manager.take_requests();

        // Same content hash
        assert_eq!(blob_id1.content_hash, blob_id2.content_hash);

        // First should have StoreBlob + AssociateBlob
        assert_eq!(requests1.len(), 2);
        assert!(matches!(&requests1[0], StorageRequest::StoreBlob { .. }));

        // Second should only have AssociateBlob (no duplicate store)
        assert_eq!(requests2.len(), 1);
        assert!(matches!(
            &requests2[0],
            StorageRequest::AssociateBlob { .. }
        ));
    }

    #[test]
    fn load_blob_returns_available_data() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let commit_id = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        let data = b"blob content".to_vec();
        let blob_id = manager.associate_blob(object_id, "main", commit_id, data.clone());

        // Blob should be immediately available
        let result = manager.load_blob(&blob_id);
        assert_eq!(result.unwrap(), data.as_slice());
    }

    #[test]
    fn load_blob_triggers_load_for_unknown_blob() {
        let mut manager = ObjectManager::new();

        let blob_id = BlobId {
            object_id: ObjectId::new(),
            branch_name: BranchName::new("main"),
            commit_id: CommitId([0u8; 32]),
            content_hash: ContentHash([42u8; 32]),
        };

        manager.take_requests();

        let result = manager.load_blob(&blob_id);
        assert!(matches!(result, Err(Error::BlobNotLoaded(_))));

        let requests = manager.take_requests();
        assert_eq!(requests.len(), 1);
        assert!(matches!(
            &requests[0],
            StorageRequest::LoadBlob { content_hash }
            if *content_hash == blob_id.content_hash
        ));
    }

    #[test]
    fn load_blob_returns_data_after_load_response() {
        let mut manager = ObjectManager::new();

        let content_hash = ContentHash([42u8; 32]);
        let blob_id = BlobId {
            object_id: ObjectId::new(),
            branch_name: BranchName::new("main"),
            commit_id: CommitId([0u8; 32]),
            content_hash,
        };

        // Trigger load
        let _ = manager.load_blob(&blob_id);

        // Simulate storage response
        let data = b"loaded data".to_vec();
        manager.push_response(StorageResponse::LoadBlob {
            content_hash,
            result: Ok(data.clone()),
        });
        manager.process_storage_responses();

        // Now should return data
        let result = manager.load_blob(&blob_id);
        assert_eq!(result.unwrap(), data.as_slice());
    }

    #[test]
    fn load_blob_returns_not_found_after_not_found_response() {
        let mut manager = ObjectManager::new();

        let content_hash = ContentHash([42u8; 32]);
        let blob_id = BlobId {
            object_id: ObjectId::new(),
            branch_name: BranchName::new("main"),
            commit_id: CommitId([0u8; 32]),
            content_hash,
        };

        // Trigger load
        let _ = manager.load_blob(&blob_id);

        // Simulate not found response
        manager.push_response(StorageResponse::LoadBlob {
            content_hash,
            result: Err(StorageError::NotFound),
        });
        manager.process_storage_responses();

        // Should return BlobNotFound
        let result = manager.load_blob(&blob_id);
        assert!(matches!(result, Err(Error::BlobNotFound(_))));
    }

    #[test]
    fn store_blob_response_updates_stored_state() {
        let mut manager = ObjectManager::new();
        let object_id = manager.create(None);
        let author = ObjectId::new();

        let commit_id = manager
            .add_commit(object_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        let data = b"blob content".to_vec();
        let blob_id = manager.associate_blob(object_id, "main", commit_id, data);

        // Simulate successful store response
        manager.push_response(StorageResponse::StoreBlob {
            content_hash: blob_id.content_hash,
            result: Ok(()),
        });
        manager.process_storage_responses();

        // Blob should still be loadable
        let result = manager.load_blob(&blob_id);
        assert!(result.is_ok());
    }
}
