use super::*;
use crate::commit::CommitId;
use crate::object::{BranchName, ObjectId};
use std::collections::HashSet;
use uuid::Uuid;

impl SyncManager {
    /// Forward an update to all servers.
    ///
    /// Call this after local writes to sync changes to connected servers.
    /// Now uses DocManager for Yrs-based sync.
    pub fn forward_update_to_servers(&mut self, object_id: ObjectId, _branch_name: BranchName) {
        let Some(row_doc) = self.doc_manager.get(object_id) else {
            return;
        };

        let metadata = row_doc.metadata.clone();

        // Skip nosync objects
        if metadata
            .get(crate::metadata::MetadataKey::NoSync.as_str())
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return;
        }

        // Get the full doc state as an update
        let update = {
            let txn = row_doc.doc.transact();
            use yrs::ReadTxn;
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();
        if !server_ids.is_empty() {
            tracing::trace!(%object_id, servers = server_ids.len(), "forwarding doc to servers");
        }

        for server_id in server_ids {
            let include_metadata = {
                let Some(server) = self.servers.get(&server_id) else {
                    continue;
                };
                !server.sent_metadata.contains(&object_id)
            };

            if let Some(server) = self.servers.get_mut(&server_id)
                && include_metadata
            {
                server.sent_metadata.insert(object_id);
            }

            self.outbox.push(OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::DocUpdated {
                    doc_id: object_id,
                    update: update.clone(),
                    metadata: if include_metadata {
                        Some(ObjectMetadata {
                            id: object_id,
                            metadata: metadata.clone(),
                        })
                    } else {
                        None
                    },
                },
            });
        }
    }

    /// Forward an update to clients whose scope includes this object/branch.
    #[allow(dead_code)]
    pub(super) fn forward_update_to_clients(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
    ) {
        self.forward_update_to_clients_except(object_id, branch_name, ClientId(Uuid::nil()));
    }

    /// Forward an update to clients except the specified one.
    #[allow(dead_code)]
    pub(super) fn forward_update_to_clients_except(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        except: ClientId,
    ) {
        let Some(row_doc) = self.doc_manager.get(object_id) else {
            return;
        };

        let metadata = row_doc.metadata.clone();

        // Skip nosync objects
        if metadata
            .get(crate::metadata::MetadataKey::NoSync.as_str())
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return;
        }

        let is_catalogue = Self::is_catalogue_metadata(&metadata);

        let client_ids: Vec<ClientId> = self
            .clients
            .iter()
            .filter(|(id, client)| {
                **id != except && (is_catalogue || client.is_in_scope(object_id, &branch_name))
            })
            .map(|(id, _)| *id)
            .collect();

        let _span = tracing::debug_span!("forward_update_to_clients", %object_id, %branch_name, client_count = client_ids.len()).entered();

        let update = {
            let txn = row_doc.doc.transact();
            use yrs::ReadTxn;
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        for client_id in &client_ids {
            let include_metadata = {
                let Some(client) = self.clients.get(client_id) else {
                    continue;
                };
                !client.sent_metadata.contains(&object_id)
            };

            if let Some(client) = self.clients.get_mut(client_id)
                && include_metadata
            {
                client.sent_metadata.insert(object_id);
            }

            self.outbox.push(OutboxEntry {
                destination: Destination::Client(*client_id),
                payload: SyncPayload::DocUpdated {
                    doc_id: object_id,
                    update: update.clone(),
                    metadata: if include_metadata {
                        Some(ObjectMetadata {
                            id: object_id,
                            metadata: metadata.clone(),
                        })
                    } else {
                        None
                    },
                },
            });
        }
    }

    /// Forward a truncation to all servers.
    #[allow(dead_code)]
    pub(super) fn forward_truncation_to_servers(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        _tails: HashSet<CommitId>,
    ) {
        // TODO(task-17): Legacy commit-based truncation forwarding.
        // With DocManager, truncation is handled by Yrs compaction.
        tracing::debug!(%object_id, %branch_name, "forward_truncation_to_servers (legacy, no-op)");
    }

    /// Forward a truncation to clients whose scope includes this object/branch.
    #[allow(dead_code)]
    pub(super) fn forward_truncation_to_clients(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        _tails: HashSet<CommitId>,
    ) {
        // TODO(task-17): Legacy commit-based truncation forwarding.
        tracing::debug!(%object_id, %branch_name, "forward_truncation_to_clients (legacy, no-op)");
    }

    /// Forward a truncation to clients except the specified one.
    #[allow(dead_code)]
    pub(super) fn forward_truncation_to_clients_except(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        _tails: HashSet<CommitId>,
        _except: ClientId,
    ) {
        // TODO(task-17): Legacy commit-based truncation forwarding.
        tracing::debug!(%object_id, %branch_name, "forward_truncation_to_clients_except (legacy, no-op)");
    }

    /// Forward a doc update to all connected servers.
    pub(super) fn forward_doc_update_to_servers(
        &mut self,
        doc_id: ObjectId,
        update: Vec<u8>,
        metadata: Option<ObjectMetadata>,
    ) {
        for &server_id in self.servers.keys() {
            self.outbox.push(OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::DocUpdated {
                    doc_id,
                    update: update.clone(),
                    metadata: metadata.clone(),
                },
            });
        }
    }
}
