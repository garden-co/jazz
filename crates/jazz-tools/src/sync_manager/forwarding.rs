use super::*;
use crate::commit::CommitId;
use crate::object::{BranchName, ObjectId};
use crate::row_regions::{HistoryScan, StoredRowVersion};
use std::collections::HashSet;
use uuid::Uuid;

impl SyncManager {
    pub(super) fn load_current_row_from_storage<H: crate::storage::Storage + ?Sized>(
        &self,
        storage: &H,
        object_id: ObjectId,
        branch_name: &BranchName,
        metadata: &HashMap<String, String>,
    ) -> Option<StoredRowVersion> {
        let table = metadata.get(crate::metadata::MetadataKey::Table.as_str())?;

        if let Ok(Some(row)) =
            storage.load_visible_region_row(table, branch_name.as_str(), object_id)
        {
            return Some(row);
        }

        storage
            .scan_history_region(
                table,
                branch_name.as_str(),
                HistoryScan::Row { row_id: object_id },
            )
            .ok()?
            .into_iter()
            .filter(|row| row.state.is_visible())
            .max_by_key(|row| (row.updated_at, row.version_id()))
    }

    /// Forward an update to all servers.
    ///
    /// Call this after local writes to sync changes to connected servers.
    pub fn forward_update_to_servers(&mut self, object_id: ObjectId, branch_name: BranchName) {
        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();
        if !server_ids.is_empty() {
            tracing::trace!(%object_id, %branch_name, servers = server_ids.len(), "forwarding to servers");
        }

        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        let metadata = object.metadata.clone();

        if let Some(row) = self.object_manager.visible_row(object_id, branch_name) {
            for server_id in server_ids {
                self.queue_row_to_server(server_id, object_id, metadata.clone(), row.clone());
            }
            return;
        }

        let Some(branch) = object.branches.get(&branch_name) else {
            return;
        };
        let tips: HashSet<CommitId> = branch.tips.iter().copied().collect();

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

    #[cfg(test)]
    pub fn forward_update_to_servers_with_storage<H: crate::storage::Storage>(
        &mut self,
        storage: &H,
        object_id: ObjectId,
        branch_name: BranchName,
    ) {
        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();
        if !server_ids.is_empty() {
            tracing::trace!(%object_id, %branch_name, servers = server_ids.len(), "forwarding to servers");
        }

        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        if let Some(row) =
            self.load_current_row_from_storage(storage, object_id, &branch_name, &object.metadata)
        {
            let metadata = object.metadata.clone();
            for server_id in server_ids {
                self.queue_row_to_server(server_id, object_id, metadata.clone(), row.clone());
            }
            return;
        }
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

    pub(crate) fn forward_row_version_to_servers(
        &mut self,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        row: StoredRowVersion,
    ) {
        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();
        if !server_ids.is_empty() {
            tracing::trace!(
                %object_id,
                branch = row.branch,
                servers = server_ids.len(),
                "forwarding row version to servers"
            );
        }

        for server_id in server_ids {
            self.queue_row_to_server(server_id, object_id, metadata.clone(), row.clone());
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

    pub(super) fn forward_update_to_clients_with_storage(
        &mut self,
        storage: &impl crate::storage::Storage,
        object_id: ObjectId,
        branch_name: BranchName,
    ) {
        self.forward_update_to_clients_except_with_storage(
            storage,
            object_id,
            branch_name,
            ClientId(Uuid::nil()),
        );
    }

    /// Forward an update to clients except the specified one.
    pub(super) fn forward_update_to_clients_except(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        except: ClientId,
    ) {
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
        let metadata = object.metadata.clone();

        if let Some(row) = self.object_manager.visible_row(object_id, branch_name) {
            for client_id in &client_ids {
                tracing::trace!(%client_id, "queuing row update to client");
                if is_catalogue {
                    self.queue_row_to_client_unscoped(
                        *client_id,
                        object_id,
                        metadata.clone(),
                        row.clone(),
                    );
                } else {
                    self.queue_row_to_client(*client_id, object_id, metadata.clone(), row.clone());
                }
            }
            return;
        }

        let Some(branch) = object.branches.get(&branch_name) else {
            return;
        };
        let tips: HashSet<CommitId> = branch.tips.iter().copied().collect();

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

    pub(super) fn forward_update_to_clients_except_with_storage<H: crate::storage::Storage>(
        &mut self,
        storage: &H,
        object_id: ObjectId,
        branch_name: BranchName,
        except: ClientId,
    ) {
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
        if let Some(row) =
            self.load_current_row_from_storage(storage, object_id, &branch_name, &object.metadata)
        {
            let metadata = object.metadata.clone();
            for client_id in &client_ids {
                tracing::trace!(%client_id, "queuing row update to client");
                if is_catalogue {
                    self.queue_row_to_client_unscoped(
                        *client_id,
                        object_id,
                        metadata.clone(),
                        row.clone(),
                    );
                } else {
                    self.queue_row_to_client(*client_id, object_id, metadata.clone(), row.clone());
                }
            }
            return;
        }
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
