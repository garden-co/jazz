use super::*;
use crate::batch_fate::BatchSettlement;
use crate::object::{BranchName, ObjectId};
use crate::row_histories::{BatchId, HistoryScan, StoredRowBatch};
use crate::storage::{RowLocator, Storage, metadata_from_row_locator};
use std::collections::HashSet;
use uuid::Uuid;

impl SyncManager {
    pub(super) fn load_current_row_from_storage<H: crate::storage::Storage + ?Sized>(
        &self,
        storage: &H,
        object_id: ObjectId,
        branch_name: &BranchName,
        row_locator: &RowLocator,
    ) -> Option<StoredRowBatch> {
        let table = row_locator.table.as_str();

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
            .max_by_key(|row| (row.updated_at, row.batch_id()))
    }

    pub(super) fn load_current_batch_settlement_from_storage<
        H: crate::storage::Storage + ?Sized,
    >(
        &self,
        storage: &H,
        object_id: ObjectId,
        branch_name: &BranchName,
        row_locator: &RowLocator,
    ) -> Option<BatchSettlement> {
        let row =
            self.load_current_row_from_storage(storage, object_id, branch_name, row_locator)?;
        if row.branch != branch_name.as_str() {
            return None;
        }
        match row.state {
            crate::row_histories::RowState::VisibleDirect
            | crate::row_histories::RowState::VisibleTransactional => {
                self.load_batch_settlement_by_batch_id_from_storage(storage, row.batch_id)
            }
            crate::row_histories::RowState::StagingPending
            | crate::row_histories::RowState::Superseded
            | crate::row_histories::RowState::Rejected => None,
        }
    }

    pub(super) fn load_batch_settlement_by_batch_id_from_storage<
        H: crate::storage::Storage + ?Sized,
    >(
        &self,
        storage: &H,
        batch_id: BatchId,
    ) -> Option<BatchSettlement> {
        storage
            .load_local_batch_record(batch_id)
            .ok()
            .flatten()
            .and_then(|record| record.latest_settlement)
            .or_else(|| {
                storage
                    .load_authoritative_batch_settlement(batch_id)
                    .ok()
                    .flatten()
            })
    }

    pub(super) fn queue_batch_settlement_to_client(
        &mut self,
        client_id: ClientId,
        settlement: BatchSettlement,
    ) {
        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::BatchSettlement { settlement },
        });
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

        let Some(row_locator) = storage.load_row_locator(object_id).ok().flatten() else {
            return;
        };
        if let Some(row) =
            self.load_current_row_from_storage(storage, object_id, &branch_name, &row_locator)
        {
            let metadata = metadata_from_row_locator(&row_locator);
            self.forward_row_batch_to_servers_with_storage(
                storage,
                row_locator.table.as_str(),
                object_id,
                metadata,
                row,
            );
            return;
        }
    }

    pub(crate) fn forward_row_batch_to_servers_with_storage<H: Storage>(
        &mut self,
        storage: &H,
        table: &str,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        row: StoredRowBatch,
    ) {
        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();
        if !server_ids.is_empty() {
            tracing::trace!(
                %object_id,
                branch = row.branch.as_str(),
                servers = server_ids.len(),
                "forwarding row batch entry with parent closure to servers"
            );
        }

        for server_id in server_ids {
            let mut visiting = HashSet::new();
            self.queue_row_to_server_with_missing_parents(
                storage,
                table,
                server_id,
                metadata.clone(),
                row.clone(),
                &mut visiting,
            );
        }
    }

    pub(super) fn queue_row_to_server_with_missing_parents<H: Storage>(
        &mut self,
        storage: &H,
        table: &str,
        server_id: ServerId,
        metadata: HashMap<String, String>,
        row: StoredRowBatch,
        visiting: &mut HashSet<BatchId>,
    ) {
        let object_id = row.row_id;
        let branch_name = BranchName::new(&row.branch);
        let already_sent = self
            .servers
            .get(&server_id)
            .and_then(|server| {
                server
                    .sent_batch_ids
                    .get(&(object_id, branch_name))
                    .cloned()
            })
            .unwrap_or_default();

        for parent_batch_id in row.parents.iter().copied() {
            if already_sent.contains(&parent_batch_id) {
                continue;
            }
            if !visiting.insert(parent_batch_id) {
                continue;
            }

            match storage.load_history_row_batch(
                table,
                row.branch.as_str(),
                object_id,
                parent_batch_id,
            ) {
                Ok(Some(parent_row)) => self.queue_row_to_server_with_missing_parents(
                    storage,
                    table,
                    server_id,
                    metadata.clone(),
                    parent_row,
                    visiting,
                ),
                Ok(None) => tracing::warn!(
                    %server_id,
                    %object_id,
                    %branch_name,
                    ?parent_batch_id,
                    "missing parent row batch in local storage while queueing row batch to server"
                ),
                Err(error) => tracing::warn!(
                    %server_id,
                    %object_id,
                    %branch_name,
                    ?parent_batch_id,
                    %error,
                    "failed to load parent row batch while queueing row batch to server"
                ),
            }

            visiting.remove(&parent_batch_id);
        }

        self.queue_row_to_server(server_id, object_id, metadata, row);
    }

    pub(crate) fn forward_row_batch_to_servers(
        &mut self,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        row: StoredRowBatch,
    ) {
        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();
        if !server_ids.is_empty() {
            tracing::trace!(
                %object_id,
                branch = row.branch.as_str(),
                servers = server_ids.len(),
                "forwarding row batch entry to servers"
            );
        }

        for server_id in server_ids {
            self.queue_row_to_server(server_id, object_id, metadata.clone(), row.clone());
        }
    }

    pub(crate) fn force_row_batch_to_servers(
        &mut self,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        row: StoredRowBatch,
    ) {
        let branch_name = BranchName::new(&row.branch);
        let batch_id = row.batch_id;
        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();

        for server_id in server_ids {
            if let Some(server) = self.servers.get_mut(&server_id) {
                server.sent_metadata.remove(&object_id);
                if let Some(sent_batches) = server.sent_batch_ids.get_mut(&(object_id, branch_name))
                {
                    sent_batches.remove(&batch_id);
                    if sent_batches.is_empty() {
                        server.sent_batch_ids.remove(&(object_id, branch_name));
                    }
                }
            }

            self.queue_row_to_server(server_id, object_id, metadata.clone(), row.clone());
        }
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

    pub(super) fn forward_update_to_clients_except_with_storage<H: crate::storage::Storage>(
        &mut self,
        storage: &H,
        object_id: ObjectId,
        branch_name: BranchName,
        except: ClientId,
    ) {
        let client_ids: Vec<ClientId> = self
            .clients
            .iter()
            .filter(|(id, client)| **id != except && client.is_in_scope(object_id, &branch_name))
            .map(|(id, _)| *id)
            .collect();

        let _span = tracing::debug_span!("forward_update_to_clients", %object_id, %branch_name, client_count = client_ids.len()).entered();

        let Some(row_locator) = storage.load_row_locator(object_id).ok().flatten() else {
            return;
        };
        if let Some(row) =
            self.load_current_row_from_storage(storage, object_id, &branch_name, &row_locator)
        {
            let metadata = metadata_from_row_locator(&row_locator);
            for client_id in &client_ids {
                tracing::trace!(%client_id, "queuing row update to client");
                self.queue_row_to_client(
                    *client_id,
                    object_id,
                    metadata.clone(),
                    row.clone(),
                    false,
                );
                if let Some(settlement) = self.load_current_batch_settlement_from_storage(
                    storage,
                    object_id,
                    &branch_name,
                    &row_locator,
                ) {
                    self.queue_batch_settlement_to_client(*client_id, settlement);
                }
            }
        }
    }
}
