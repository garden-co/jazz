use super::*;
use crate::batch_fate::{BatchSettlement, VisibleBatchMember};
use crate::object::{BranchName, ObjectId};
use crate::row_histories::{BatchId, HistoryScan, StoredRowVersion};
use crate::storage::{RowLocator, metadata_from_row_locator};
use std::collections::HashSet;
use uuid::Uuid;

impl SyncManager {
    pub(super) fn load_current_row_from_storage<H: crate::storage::Storage + ?Sized>(
        &self,
        storage: &H,
        object_id: ObjectId,
        branch_name: &BranchName,
        row_locator: &RowLocator,
    ) -> Option<StoredRowVersion> {
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
            .max_by_key(|row| (row.updated_at, row.version_id()))
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
        let confirmed_tier = row.confirmed_tier?;
        let visible_members = vec![VisibleBatchMember {
            object_id,
            branch_name: *branch_name,
            batch_id: row.batch_id,
        }];
        match row.state {
            crate::row_histories::RowState::VisibleDirect => Some(BatchSettlement::DurableDirect {
                batch_id: row.batch_id,
                confirmed_tier,
                visible_members,
            }),
            crate::row_histories::RowState::VisibleTransactional => {
                Some(BatchSettlement::AcceptedTransaction {
                    batch_id: row.batch_id,
                    confirmed_tier,
                    visible_members,
                })
            }
            crate::row_histories::RowState::StagingPending
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
        let row_locators = storage.scan_row_locators().ok()?;
        let mut visible_members = Vec::new();
        let mut batch_kind: Option<crate::row_histories::RowState> = None;
        let mut confirmed_tier: Option<DurabilityTier> = None;

        for (object_id, row_locator) in row_locators {
            let Ok(history_rows) =
                storage.scan_history_row_versions(row_locator.table.as_str(), object_id)
            else {
                continue;
            };

            let branch_names: HashSet<_> = history_rows
                .into_iter()
                .filter(|row| row.batch_id == batch_id)
                .map(|row| BranchName::new(&row.branch))
                .collect();

            for branch_name in branch_names {
                let Some(row) = self.load_current_row_from_storage(
                    storage,
                    object_id,
                    &branch_name,
                    &row_locator,
                ) else {
                    continue;
                };
                if row.batch_id != batch_id || !row.state.is_visible() {
                    continue;
                }
                if !matches!(
                    row.state,
                    crate::row_histories::RowState::VisibleDirect
                        | crate::row_histories::RowState::VisibleTransactional
                ) {
                    continue;
                }

                if let Some(existing_kind) = batch_kind {
                    if existing_kind != row.state {
                        tracing::warn!(
                            ?batch_id,
                            object_id = %object_id,
                            branch = %branch_name,
                            existing_state = ?existing_kind,
                            row_state = ?row.state,
                            "batch has mixed visible states; skipping settlement reconstruction"
                        );
                        return None;
                    }
                } else {
                    batch_kind = Some(row.state);
                }

                let row_tier = row.confirmed_tier?;
                confirmed_tier = Some(match confirmed_tier {
                    Some(existing) => existing.min(row_tier),
                    None => row_tier,
                });
                visible_members.push(VisibleBatchMember {
                    object_id,
                    branch_name,
                    batch_id,
                });
            }
        }

        let state = batch_kind?;
        let confirmed_tier = confirmed_tier?;
        if visible_members.is_empty() {
            return None;
        }

        match state {
            crate::row_histories::RowState::VisibleDirect => Some(BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier,
                visible_members,
            }),
            crate::row_histories::RowState::VisibleTransactional => {
                Some(BatchSettlement::AcceptedTransaction {
                    batch_id,
                    confirmed_tier,
                    visible_members,
                })
            }
            crate::row_histories::RowState::StagingPending
            | crate::row_histories::RowState::Rejected => None,
        }
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
            for server_id in server_ids {
                self.queue_row_to_server(server_id, object_id, metadata.clone(), row.clone());
            }
            return;
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
                branch = row.branch.as_str(),
                servers = server_ids.len(),
                "forwarding row version to servers"
            );
        }

        for server_id in server_ids {
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
