use super::*;
use crate::commit::CommitId;
use crate::object::{BranchName, ObjectId};
use std::collections::HashSet;
use uuid::Uuid;

impl SyncManager {
    /// Forward an update to all servers.
    ///
    /// Call this after local writes to sync changes to connected servers.
    pub fn forward_update_to_servers(&mut self, object_id: ObjectId, branch_name: BranchName) {
        let Some(branch_name) = Self::normalize_branch_name(branch_name) else {
            return;
        };
        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();
        if !server_ids.is_empty() {
            tracing::trace!(%object_id, %branch_name, servers = server_ids.len(), "forwarding to servers");
        }

        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        let Some(branch) = object.branches.get(&branch_name) else {
            return;
        };
        let tips: HashSet<CommitId> = branch.tips.iter().copied().collect();
        let metadata = object.metadata.clone();

        for server_id in server_ids {
            self.queue_tips_to_server(
                server_id,
                object_id,
                metadata.clone(),
                branch_name,
                tips.clone(),
            );
        }
    }

    /// Forward an update to clients whose scope includes this object/branch.
    pub(super) fn forward_update_to_clients(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
    ) {
        self.forward_update_to_clients_except(object_id, branch_name, ClientId(Uuid::nil()));
    }

    /// Forward an update to clients except the specified one.
    pub(super) fn forward_update_to_clients_except(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        except: ClientId,
    ) {
        let Some(branch_name) = Self::normalize_branch_name(branch_name) else {
            return;
        };
        let is_catalogue = self.object_is_catalogue(object_id);
        let client_ids: Vec<ClientId> = self
            .clients
            .iter()
            .filter(|(id, client)| {
                **id != except && (is_catalogue || client.is_in_scope(object_id, &branch_name))
            })
            .map(|(id, _)| *id)
            .collect();

        let _span = tracing::debug_span!("forward_update_to_clients", %object_id, %branch_name, client_count = client_ids.len()).entered();

        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        let Some(branch) = object.branches.get(&branch_name) else {
            return;
        };
        let tips: HashSet<CommitId> = branch.tips.iter().copied().collect();
        let metadata = object.metadata.clone();

        for client_id in &client_ids {
            tracing::trace!(%client_id, "queuing tips to client");
            if is_catalogue {
                self.queue_tips_to_client_unscoped(
                    *client_id,
                    object_id,
                    metadata.clone(),
                    branch_name,
                    tips.clone(),
                );
            } else {
                self.queue_tips_to_client(
                    *client_id,
                    object_id,
                    metadata.clone(),
                    branch_name,
                    tips.clone(),
                );
            }
        }
    }

    /// Forward a truncation to all servers.
    pub(super) fn forward_truncation_to_servers(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        tails: HashSet<CommitId>,
    ) {
        let Some(branch_name) = Self::normalize_branch_name(branch_name) else {
            return;
        };
        // Skip objects marked as nosync (local-only, e.g., index nodes)
        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        if object
            .metadata
            .get(crate::metadata::MetadataKey::NoSync.as_str())
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return;
        }

        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();
        for server_id in server_ids {
            self.outbox.push(OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::ObjectTruncated {
                    object_id,
                    branch_name,
                    tails: tails.clone(),
                },
            });
        }
    }

    /// Forward a truncation to clients whose scope includes this object/branch.
    pub(super) fn forward_truncation_to_clients(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        tails: HashSet<CommitId>,
    ) {
        self.forward_truncation_to_clients_except(
            object_id,
            branch_name,
            tails,
            ClientId(Uuid::nil()),
        );
    }

    /// Forward a truncation to clients except the specified one.
    pub(super) fn forward_truncation_to_clients_except(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        tails: HashSet<CommitId>,
        except: ClientId,
    ) {
        let Some(branch_name) = Self::normalize_branch_name(branch_name) else {
            return;
        };
        // Skip objects marked as nosync (local-only, e.g., index nodes)
        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        if object
            .metadata
            .get(crate::metadata::MetadataKey::NoSync.as_str())
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return;
        }

        let is_catalogue = Self::is_catalogue_metadata(&object.metadata);
        let client_ids: Vec<ClientId> = self
            .clients
            .iter()
            .filter(|(id, client)| {
                **id != except && (is_catalogue || client.is_in_scope(object_id, &branch_name))
            })
            .map(|(id, _)| *id)
            .collect();
        for client_id in client_ids {
            self.outbox.push(OutboxEntry {
                destination: Destination::Client(client_id),
                payload: SyncPayload::ObjectTruncated {
                    object_id,
                    branch_name,
                    tails: tails.clone(),
                },
            });
        }
    }
}
