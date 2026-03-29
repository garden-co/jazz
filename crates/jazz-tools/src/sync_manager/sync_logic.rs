use super::*;
use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::BatchBranchKey;
use std::collections::{HashMap, HashSet};

impl SyncManager {
    pub(super) fn is_catalogue_metadata(metadata: &HashMap<String, String>) -> bool {
        matches!(
            metadata
                .get(crate::metadata::MetadataKey::Type.as_str())
                .map(|value| value.as_str()),
            Some(kind) if crate::metadata::ObjectType::is_catalogue_type_str(kind)
        )
    }

    pub(super) fn track_catalogue_object(
        &mut self,
        object_id: ObjectId,
        metadata: &HashMap<String, String>,
    ) {
        if Self::is_catalogue_metadata(metadata) {
            self.catalogue_objects.insert(object_id);
        } else {
            self.catalogue_objects.remove(&object_id);
        }
    }

    pub(super) fn object_is_catalogue(&self, object_id: ObjectId) -> bool {
        self.catalogue_objects.contains(&object_id)
    }

    /// Mark all existing catalogue objects as already sent for this server.
    ///
    /// This is used when the upstream server reports the same catalogue digest
    /// during the connect handshake, allowing us to skip replaying schema/lens
    /// objects while still performing the normal full sync for row data.
    pub(super) fn mark_catalogue_sent_for_server(&mut self, server_id: ServerId) {
        let Some(_server) = self.servers.get(&server_id) else {
            return;
        };

        let mut sent_metadata = HashSet::new();
        let mut sent_tips = Vec::new();

        for object_id in self.catalogue_objects.iter().copied() {
            let Some(object) = self.object_manager.objects.get(&object_id) else {
                continue;
            };

            sent_metadata.insert(object_id);
            for (branch_name, branch) in object.branches.iter() {
                sent_tips.push((
                    object_id,
                    branch_name,
                    branch.tips.iter().copied().collect::<HashSet<_>>(),
                ));
            }
        }

        let Some(server) = self.servers.get_mut(&server_id) else {
            return;
        };
        server.sent_metadata.extend(sent_metadata);
        for (object_id, branch_name, tips) in sent_tips {
            server.sent_tips.insert(
                (object_id, BatchBranchKey::from_branch_name(branch_name)),
                tips,
            );
        }
    }

    /// Queue all existing objects to sync to a new server.
    pub(super) fn queue_full_sync_to_server(&mut self, server_id: ServerId) {
        let _span = tracing::debug_span!("queue_full_sync_to_server", %server_id).entered();
        // Collect all object/branch/tips we need to sync
        let mut to_sync: Vec<BranchSyncData> = Vec::new();

        for (object_id, object) in &self.object_manager.objects {
            for (branch_name, branch) in object.branches.iter() {
                to_sync.push((
                    *object_id,
                    object.metadata.clone(),
                    branch_name,
                    branch.tips.iter().copied().collect(),
                ));
            }
        }

        // Now queue messages (borrowing self.servers mutably)
        for (object_id, metadata, branch_name, tips) in to_sync {
            self.queue_tips_to_server(server_id, object_id, metadata, branch_name, tips);
        }
    }

    /// Queue all existing catalogue objects to sync to a new client.
    pub(super) fn queue_catalogue_sync_to_client(&mut self, client_id: ClientId) {
        let mut to_sync: Vec<BranchSyncData> = Vec::new();

        for object_id in self.catalogue_objects.iter().copied() {
            let Some(object) = self.object_manager.objects.get(&object_id) else {
                continue;
            };
            for (branch_name, branch) in object.branches.iter() {
                to_sync.push((
                    object_id,
                    object.metadata.clone(),
                    branch_name,
                    branch.tips.iter().copied().collect(),
                ));
            }
        }

        for (object_id, metadata, branch_name, tips) in to_sync {
            self.queue_tips_to_client_unscoped(client_id, object_id, metadata, branch_name, tips);
        }
    }

    /// Queue tips to a server, including metadata if first time.
    pub(super) fn queue_tips_to_server(
        &mut self,
        server_id: ServerId,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        branch_name: BranchName,
        tips: HashSet<CommitId>,
    ) {
        let Some(branch_name) = Self::normalize_branch_name(branch_name) else {
            return;
        };
        let _span = tracing::debug_span!("queue_tips_to_server", %server_id, %object_id, %branch_name, tips = tips.len()).entered();
        // Skip objects marked as nosync (local-only, e.g., index nodes)
        if metadata
            .get(crate::metadata::MetadataKey::NoSync.as_str())
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return;
        }

        // Extract needed info without holding mutable borrow
        let (include_metadata, already_sent) = {
            let Some(server) = self.servers.get(&server_id) else {
                return;
            };
            let include_metadata = !server.sent_metadata.contains(&object_id);
            let branch_key = BatchBranchKey::from_branch_name(branch_name);
            let already_sent = server
                .sent_tips
                .get(&(object_id, branch_key))
                .cloned()
                .unwrap_or_default();
            (include_metadata, already_sent)
        };

        // Collect commits we need to send
        let commits = self.collect_commits_to_send(object_id, &branch_name, &already_sent, &tips);

        if commits.is_empty() && !include_metadata {
            return; // Nothing new to send
        }

        // Now update server state
        let server = self.servers.get_mut(&server_id).unwrap();
        if include_metadata {
            server.sent_metadata.insert(object_id);
        }
        server.sent_tips.insert(
            (object_id, BatchBranchKey::from_branch_name(branch_name)),
            tips,
        );
        let payload_branch_name = Self::display_branch_name(branch_name);

        self.outbox.push(OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::ObjectUpdated {
                object_id,
                metadata: if include_metadata {
                    Some(ObjectMetadata {
                        id: object_id,
                        metadata,
                    })
                } else {
                    None
                },
                branch_name: payload_branch_name,
                commits,
            },
        });
    }

    /// Queue initial sync to a client for a newly visible object/branch.
    pub(super) fn queue_initial_sync_to_client(
        &mut self,
        client_id: ClientId,
        object_id: ObjectId,
        branch_name: BranchName,
    ) {
        let Some(branch_name) = Self::normalize_branch_name(branch_name) else {
            return;
        };
        // Get current tips from object manager
        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        let Some(branch) = object.branches.get(&branch_name) else {
            return;
        };
        let tips: HashSet<CommitId> = branch.tips.iter().copied().collect();
        let metadata = object.metadata.clone();

        self.queue_tips_to_client(client_id, object_id, metadata, branch_name, tips);
    }

    /// Queue tips to a client, including metadata if first time.
    pub(super) fn queue_tips_to_client(
        &mut self,
        client_id: ClientId,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        branch_name: BranchName,
        tips: HashSet<CommitId>,
    ) {
        self.queue_tips_to_client_inner(client_id, object_id, metadata, branch_name, tips, true);
    }

    pub(super) fn queue_tips_to_client_unscoped(
        &mut self,
        client_id: ClientId,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        branch_name: BranchName,
        tips: HashSet<CommitId>,
    ) {
        self.queue_tips_to_client_inner(client_id, object_id, metadata, branch_name, tips, false);
    }

    fn queue_tips_to_client_inner(
        &mut self,
        client_id: ClientId,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        branch_name: BranchName,
        tips: HashSet<CommitId>,
        require_scope: bool,
    ) {
        let Some(branch_name) = Self::normalize_branch_name(branch_name) else {
            return;
        };
        // Skip objects marked as nosync (local-only, e.g., index nodes)
        if metadata
            .get(crate::metadata::MetadataKey::NoSync.as_str())
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return;
        }

        // Extract needed info without holding mutable borrow
        let (in_scope, include_metadata, already_sent) = {
            let Some(client) = self.clients.get(&client_id) else {
                return;
            };

            // Check if in scope
            let in_scope = !require_scope || client.is_in_scope(object_id, &branch_name);

            let include_metadata = !client.sent_metadata.contains(&object_id);
            let branch_key = BatchBranchKey::from_branch_name(branch_name);

            let already_sent = client
                .sent_tips
                .get(&(object_id, branch_key))
                .cloned()
                .unwrap_or_default();

            (in_scope, include_metadata, already_sent)
        };

        if !in_scope {
            return;
        }

        // Collect commits
        let commits = self.collect_commits_to_send(object_id, &branch_name, &already_sent, &tips);

        if commits.is_empty() && !include_metadata {
            return;
        }

        // Now update client state
        let client = self.clients.get_mut(&client_id).unwrap();
        if include_metadata {
            client.sent_metadata.insert(object_id);
        }
        client.sent_tips.insert(
            (object_id, BatchBranchKey::from_branch_name(branch_name)),
            tips,
        );
        let payload_branch_name = Self::display_branch_name(branch_name);

        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id,
                metadata: if include_metadata {
                    Some(ObjectMetadata {
                        id: object_id,
                        metadata,
                    })
                } else {
                    None
                },
                branch_name: payload_branch_name,
                commits,
            },
        });
    }

    /// Collect commits needed to bring destination from already_sent to new_tips.
    /// Returns commits in topological order (parents first).
    pub(super) fn collect_commits_to_send(
        &self,
        object_id: ObjectId,
        branch_name: &BranchName,
        already_sent: &HashSet<CommitId>,
        new_tips: &HashSet<CommitId>,
    ) -> Vec<Commit> {
        let Some(branch_name) = Self::normalize_branch_name(*branch_name) else {
            return Vec::new();
        };
        let Some(object) = self.object_manager.get(object_id) else {
            return Vec::new();
        };
        let Some(branch) = object.branches.get(&branch_name) else {
            return Vec::new();
        };

        // If no commits yet sent, send all commits reachable from tips
        // If commits were sent, send only new commits (those not in ancestry of already_sent)

        let mut to_send: HashSet<CommitId> = HashSet::new();
        let mut to_visit: Vec<CommitId> = new_tips.iter().copied().collect();
        let mut visited: HashSet<CommitId> = HashSet::new();

        while let Some(commit_id) = to_visit.pop() {
            if visited.contains(&commit_id) {
                continue;
            }
            visited.insert(commit_id);

            // If already sent this commit (or its descendant), stop traversal
            if already_sent.contains(&commit_id) {
                continue;
            }

            to_send.insert(commit_id);

            // Visit parents
            if let Some(commit) = branch.commits.get(&commit_id) {
                for parent in &commit.parents {
                    if !visited.contains(parent) {
                        to_visit.push(*parent);
                    }
                }
            }
        }

        // Sort topologically (parents before children)
        self.topological_sort(&branch.commits, to_send)
    }

    /// Sort commits topologically (parents first).
    pub(super) fn topological_sort(
        &self,
        all_commits: &HashMap<CommitId, Commit>,
        to_sort: HashSet<CommitId>,
    ) -> Vec<Commit> {
        let mut result = Vec::new();
        let mut remaining: HashSet<CommitId> = to_sort.clone();
        let mut added: HashSet<CommitId> = HashSet::new();

        // Simple iterative approach: repeatedly add commits whose parents are all added
        while !remaining.is_empty() {
            let mut progress = false;
            let current: Vec<CommitId> = remaining.iter().copied().collect();

            for commit_id in current {
                let Some(commit) = all_commits.get(&commit_id) else {
                    // Commit not found, skip
                    remaining.remove(&commit_id);
                    progress = true;
                    continue;
                };

                // Check if all parents in to_sort are already added
                let parents_ready = commit
                    .parents
                    .iter()
                    .all(|p| !to_sort.contains(p) || added.contains(p));

                if parents_ready {
                    result.push(commit.clone());
                    added.insert(commit_id);
                    remaining.remove(&commit_id);
                    progress = true;
                }
            }

            if !progress {
                // Cycle detected or missing parents, break to avoid infinite loop
                break;
            }
        }

        result
    }
}
