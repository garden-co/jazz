use super::*;
use crate::catalogue::CatalogueEntry;
use crate::object::{BranchName, ObjectId};
use crate::row_histories::{RowState, StoredRowVersion};
use crate::storage::{RowLocator, metadata_from_row_locator};
use std::collections::HashMap;

type RowSyncData = (ObjectId, HashMap<String, String>, StoredRowVersion);

impl SyncManager {
    pub(super) fn queue_catalogue_sync_to_server_from_storage<H: Storage>(
        &mut self,
        server_id: ServerId,
        storage: &H,
    ) {
        let Ok(entries) = storage.scan_catalogue_entries() else {
            return;
        };
        for entry in entries {
            self.catalogue_entries
                .insert(entry.object_id, entry.clone());
            self.queue_catalogue_entry_to_server(server_id, entry);
        }
    }

    /// Queue all existing objects to sync to a new server using storage as the
    /// source of truth for row history and current state.
    pub(super) fn queue_full_sync_to_server_from_storage<H: Storage>(
        &mut self,
        server_id: ServerId,
        storage: &H,
    ) {
        let _span =
            tracing::debug_span!("queue_full_sync_to_server_from_storage", %server_id).entered();
        let Ok(row_locators) = storage.scan_row_locators() else {
            return;
        };

        let mut row_sync: Vec<RowSyncData> = Vec::new();

        for (object_id, row_locator) in row_locators {
            self.collect_row_sync_versions(storage, object_id, &row_locator, &mut row_sync);
        }

        for (object_id, metadata, row) in row_sync {
            self.queue_row_to_server(server_id, object_id, metadata, row);
        }
    }

    fn collect_row_sync_versions<H: Storage>(
        &self,
        storage: &H,
        object_id: ObjectId,
        row_locator: &RowLocator,
        row_sync: &mut Vec<RowSyncData>,
    ) {
        let metadata = metadata_from_row_locator(row_locator);
        let Ok(rows) = storage.scan_history_row_versions(row_locator.table.as_str(), object_id)
        else {
            return;
        };

        for row in rows
            .into_iter()
            .filter(|row| !matches!(row.state, RowState::StagingPending))
        {
            row_sync.push((object_id, metadata.clone(), row));
        }
    }

    pub(super) fn queue_catalogue_sync_to_client_from_storage<H: Storage>(
        &mut self,
        client_id: ClientId,
        storage: &H,
    ) {
        let Ok(entries) = storage.scan_catalogue_entries() else {
            return;
        };

        for entry in entries {
            self.catalogue_entries
                .insert(entry.object_id, entry.clone());
            self.queue_catalogue_entry_to_client(client_id, entry);
        }
    }

    pub fn upsert_catalogue_entry<H: Storage>(&mut self, storage: &mut H, entry: CatalogueEntry) {
        let changed = self.persist_catalogue_entry(storage, entry.clone());
        if !changed {
            return;
        }

        self.forward_catalogue_entry_to_servers(entry.clone());
        self.forward_catalogue_entry_to_clients(entry, None);
    }

    pub(super) fn persist_catalogue_entry<H: Storage>(
        &mut self,
        storage: &mut H,
        entry: CatalogueEntry,
    ) -> bool {
        let existing = self
            .catalogue_entries
            .get(&entry.object_id)
            .cloned()
            .or_else(|| storage.load_catalogue_entry(entry.object_id).ok().flatten());

        if existing.as_ref() == Some(&entry) {
            self.catalogue_entries.insert(entry.object_id, entry);
            return false;
        }

        if let Err(error) = storage.upsert_catalogue_entry(&entry) {
            tracing::warn!(
                object_id = %entry.object_id,
                %error,
                "failed to persist catalogue entry"
            );
        }

        self.catalogue_entries.insert(entry.object_id, entry);
        true
    }

    fn queue_catalogue_entry_to_server(&mut self, server_id: ServerId, entry: CatalogueEntry) {
        self.outbox.push(OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::CatalogueEntryUpdated { entry },
        });
    }

    fn queue_catalogue_entry_to_client(&mut self, client_id: ClientId, entry: CatalogueEntry) {
        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::CatalogueEntryUpdated { entry },
        });
    }

    pub(super) fn forward_catalogue_entry_to_servers(&mut self, entry: CatalogueEntry) {
        let server_ids: Vec<_> = self.servers.keys().copied().collect();
        for server_id in server_ids {
            self.queue_catalogue_entry_to_server(server_id, entry.clone());
        }
    }

    pub(super) fn forward_catalogue_entry_to_clients(
        &mut self,
        entry: CatalogueEntry,
        except: Option<ClientId>,
    ) {
        let client_ids: Vec<_> = self
            .clients
            .keys()
            .copied()
            .filter(|client_id| except != Some(*client_id))
            .collect();
        for client_id in client_ids {
            self.queue_catalogue_entry_to_client(client_id, entry.clone());
        }
    }

    pub(super) fn queue_row_to_server(
        &mut self,
        server_id: ServerId,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        row: StoredRowVersion,
    ) {
        if metadata
            .get(crate::metadata::MetadataKey::NoSync.as_str())
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return;
        }

        let branch_name = BranchName::new(&row.branch);
        let version_id = row.version_id();

        let (include_metadata, already_sent) = {
            let Some(server) = self.servers.get(&server_id) else {
                return;
            };
            let include_metadata = !server.sent_metadata.contains(&object_id);
            let already_sent = server
                .sent_row_versions
                .get(&(object_id, branch_name))
                .cloned()
                .unwrap_or_default();
            (include_metadata, already_sent)
        };

        if already_sent.contains(&version_id) && !include_metadata {
            return;
        }

        let Some(server) = self.servers.get_mut(&server_id) else {
            return;
        };
        if include_metadata {
            server.sent_metadata.insert(object_id);
        }
        server
            .sent_row_versions
            .entry((object_id, branch_name))
            .or_default()
            .insert(version_id);

        self.outbox.push(OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::RowVersionCreated {
                metadata: include_metadata.then_some(RowMetadata {
                    id: object_id,
                    metadata,
                }),
                row,
            },
        });
    }

    pub(super) fn queue_initial_sync_to_client_with_storage<H: Storage + ?Sized>(
        &mut self,
        storage: &H,
        client_id: ClientId,
        object_id: ObjectId,
        branch_name: BranchName,
    ) {
        let Some(row_locator) = storage.load_row_locator(object_id).ok().flatten() else {
            return;
        };
        if let Some(row) =
            self.load_current_row_from_storage(storage, object_id, &branch_name, &row_locator)
        {
            self.queue_row_to_client(
                client_id,
                object_id,
                metadata_from_row_locator(&row_locator),
                row,
            );
        }
    }

    pub(super) fn queue_row_to_client(
        &mut self,
        client_id: ClientId,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        row: StoredRowVersion,
    ) {
        if metadata
            .get(crate::metadata::MetadataKey::NoSync.as_str())
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return;
        }

        let branch_name = BranchName::new(&row.branch);
        let version_id = row.version_id();

        let (in_scope, include_metadata, already_sent) = {
            let Some(client) = self.clients.get(&client_id) else {
                return;
            };
            let in_scope = client.is_in_scope(object_id, &branch_name);
            let include_metadata = !client.sent_metadata.contains(&object_id);
            let already_sent = client
                .sent_row_versions
                .get(&(object_id, branch_name))
                .cloned()
                .unwrap_or_default();
            (in_scope, include_metadata, already_sent)
        };

        if !in_scope {
            return;
        }

        if already_sent.contains(&version_id) && !include_metadata {
            return;
        }

        let Some(client) = self.clients.get_mut(&client_id) else {
            return;
        };
        if include_metadata {
            client.sent_metadata.insert(object_id);
        }
        client
            .sent_row_versions
            .entry((object_id, branch_name))
            .or_default()
            .insert(version_id);
        self.row_version_interest
            .entry(RowVersionKey::new(object_id, branch_name, version_id))
            .or_default()
            .insert(client_id);

        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::RowVersionNeeded {
                metadata: include_metadata.then_some(RowMetadata {
                    id: object_id,
                    metadata,
                }),
                row,
            },
        });
    }
}
