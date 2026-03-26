use super::*;
use crate::object::{BranchName, ObjectId};
use std::collections::HashMap;

impl SyncManager {
    pub fn is_catalogue_metadata(metadata: &HashMap<String, String>) -> bool {
        matches!(
            metadata
                .get(crate::metadata::MetadataKey::Type.as_str())
                .map(|value| value.as_str()),
            Some(kind)
                if kind == crate::metadata::ObjectType::CatalogueSchema.as_str()
                    || kind == crate::metadata::ObjectType::CatalogueLens.as_str()
        )
    }

    pub fn track_catalogue_object(
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

    #[allow(dead_code)]
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

        for object_id in self.catalogue_objects.iter().copied() {
            // Check DocManager for catalogue objects
            if self.doc_manager.get(object_id).is_some() {
                sent_metadata.insert(object_id);
            }
        }

        let Some(server) = self.servers.get_mut(&server_id) else {
            return;
        };
        server.sent_metadata.extend(sent_metadata);
    }

    /// Queue all existing doc-based objects to sync to a new server.
    pub(super) fn queue_full_sync_to_server(&mut self, server_id: ServerId) {
        let _span = tracing::debug_span!("queue_full_sync_to_server", %server_id).entered();

        // Collect doc IDs and their metadata
        let docs: Vec<(ObjectId, HashMap<String, String>)> = self
            .doc_manager
            .all_docs()
            .map(|(id, doc)| (id, doc.metadata.clone()))
            .collect();

        // Check which catalogue objects are already marked as sent for this server
        let already_sent: HashSet<ObjectId> = self
            .servers
            .get(&server_id)
            .map(|s| s.sent_metadata.clone())
            .unwrap_or_default();

        for (doc_id, metadata) in docs {
            // Skip nosync objects
            if metadata
                .get(crate::metadata::MetadataKey::NoSync.as_str())
                .map(|v| v == "true")
                .unwrap_or(false)
            {
                continue;
            }

            // Skip catalogue objects that were already marked as sent
            // (happens when the upstream advertises a matching catalogue state hash)
            if self.catalogue_objects.contains(&doc_id) && already_sent.contains(&doc_id) {
                continue;
            }

            self.queue_doc_sync_to_server(server_id, doc_id, metadata);
        }
    }

    /// Queue a doc update to a server using Yrs sync.
    fn queue_doc_sync_to_server(
        &mut self,
        server_id: ServerId,
        doc_id: ObjectId,
        metadata: HashMap<String, String>,
    ) {
        let Some(row_doc) = self.doc_manager.get(doc_id) else {
            return;
        };

        // Get the full doc state as an update
        let update = {
            let txn = row_doc.doc.transact();
            use yrs::ReadTxn;
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        let include_metadata = {
            let Some(server) = self.servers.get(&server_id) else {
                return;
            };
            !server.sent_metadata.contains(&doc_id)
        };

        // Update server state
        if let Some(server) = self.servers.get_mut(&server_id)
            && include_metadata
        {
            server.sent_metadata.insert(doc_id);
        }

        self.outbox.push(OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::DocUpdated {
                doc_id,
                update,
                metadata: if include_metadata {
                    Some(ObjectMetadata {
                        id: doc_id,
                        metadata,
                    })
                } else {
                    None
                },
            },
        });
    }

    /// Queue all existing catalogue objects to sync to a new client.
    pub(super) fn queue_catalogue_sync_to_client(&mut self, client_id: ClientId) {
        let catalogue_ids: Vec<ObjectId> = self.catalogue_objects.iter().copied().collect();

        for object_id in catalogue_ids {
            let Some(row_doc) = self.doc_manager.get(object_id) else {
                continue;
            };
            let metadata = row_doc.metadata.clone();

            let update = {
                let txn = row_doc.doc.transact();
                use yrs::ReadTxn;
                txn.encode_state_as_update_v1(&yrs::StateVector::default())
            };

            let include_metadata = {
                let Some(client) = self.clients.get(&client_id) else {
                    return;
                };
                !client.sent_metadata.contains(&object_id)
            };

            if let Some(client) = self.clients.get_mut(&client_id)
                && include_metadata
            {
                client.sent_metadata.insert(object_id);
            }

            self.outbox.push(OutboxEntry {
                destination: Destination::Client(client_id),
                payload: SyncPayload::DocUpdated {
                    doc_id: object_id,
                    update,
                    metadata: if include_metadata {
                        Some(ObjectMetadata {
                            id: object_id,
                            metadata,
                        })
                    } else {
                        None
                    },
                },
            });
        }
    }

    /// Queue initial sync to a client for a newly visible object/branch.
    pub(super) fn queue_initial_sync_to_client(
        &mut self,
        client_id: ClientId,
        object_id: ObjectId,
        _branch_name: BranchName,
    ) {
        let Some(row_doc) = self.doc_manager.get(object_id) else {
            return;
        };
        let metadata = row_doc.metadata.clone();

        let update = {
            let txn = row_doc.doc.transact();
            use yrs::ReadTxn;
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        let include_metadata = {
            let Some(client) = self.clients.get(&client_id) else {
                return;
            };
            !client.sent_metadata.contains(&object_id)
        };

        // Skip nosync objects
        if metadata
            .get(crate::metadata::MetadataKey::NoSync.as_str())
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return;
        }

        // Check scope
        {
            let Some(client) = self.clients.get(&client_id) else {
                return;
            };
            if !client.is_in_scope(object_id, &_branch_name) {
                return;
            }
        }

        if let Some(client) = self.clients.get_mut(&client_id)
            && include_metadata
        {
            client.sent_metadata.insert(object_id);
        }

        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::DocUpdated {
                doc_id: object_id,
                update,
                metadata: if include_metadata {
                    Some(ObjectMetadata {
                        id: object_id,
                        metadata,
                    })
                } else {
                    None
                },
            },
        });
    }
}
