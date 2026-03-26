use super::*;
use crate::object::ObjectId;
use crate::query_manager::policy::Operation;
use crate::storage::Storage;
use std::collections::HashSet;

impl SyncManager {
    /// Process a single inbox entry.
    pub(super) fn process_inbox_entry<H: Storage>(&mut self, storage: &mut H, entry: InboxEntry) {
        tracing::trace!(source = ?entry.source, payload = entry.payload.variant_name(), "processing inbox entry");
        match entry.source {
            Source::Server(server_id) => {
                self.process_from_server(storage, server_id, entry.payload)
            }
            Source::Client(client_id) => {
                self.process_from_client(storage, client_id, entry.payload)
            }
        }
    }

    /// Process a payload from a server.
    pub(super) fn process_from_server<H: Storage>(
        &mut self,
        storage: &mut H,
        server_id: ServerId,
        payload: SyncPayload,
    ) {
        let _span = tracing::debug_span!("process_from_server", %server_id, payload = payload.variant_name()).entered();
        match payload {
            SyncPayload::ObjectUpdated {
                object_id,
                branch_name,
                commits,
                ..
            } => {
                // TODO(task-17): Legacy commit-based sync — no longer applied.
                // Peers should migrate to DocUpdated.
                tracing::debug!(%object_id, %branch_name, commits = commits.len(),
                    "server→ObjectUpdated (legacy, ignored)");
            }
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                ..
            } => {
                // TODO(task-17): Legacy commit-based truncation — no longer applied.
                tracing::debug!(%object_id, %branch_name, "server→ObjectTruncated (legacy, ignored)");
            }
            SyncPayload::PersistenceAck {
                object_id,
                branch_name,
                confirmed_commits,
                tier,
            } => {
                tracing::debug!(%object_id, ?tier, commits = confirmed_commits.len(), "server→PersistenceAck");
                // Persist ack state
                for &commit_id in &confirmed_commits {
                    let _ = storage.store_ack_tier(commit_id, tier);
                    // Notify RuntimeCore of received ack
                    self.received_acks.push((commit_id, tier));
                }
                // Relay to interested clients
                let mut interested = HashSet::new();
                for &commit_id in &confirmed_commits {
                    if let Some(clients) = self.commit_interest.get(&commit_id) {
                        interested.extend(clients);
                    }
                }
                for cid in interested {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Client(cid),
                        payload: SyncPayload::PersistenceAck {
                            object_id,
                            branch_name,
                            confirmed_commits: confirmed_commits.clone(),
                            tier,
                        },
                    });
                }
            }
            SyncPayload::QuerySettled {
                query_id,
                tier,
                through_seq,
            } => {
                tracing::debug!(?query_id, ?tier, "server→QuerySettled");
                // Queue for local QueryManager to process
                self.pending_query_settled.push((query_id, tier));

                // Relay to interested clients
                if let Some(clients) = self.query_origin.get(&query_id) {
                    for &cid in clients {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(cid),
                            payload: SyncPayload::QuerySettled {
                                query_id,
                                tier,
                                through_seq,
                            },
                        });
                    }
                }
            }
            SyncPayload::Error(err) => {
                // Log or handle server error
                eprintln!("Error from server {:?}: {:?}", server_id, err);
            }
            SyncPayload::DocUpdated {
                doc_id,
                update,
                metadata,
            } => {
                tracing::debug!(%doc_id, update_len = update.len(), "server→DocUpdated");
                let meta = metadata
                    .as_ref()
                    .map(|m| m.metadata.clone())
                    .unwrap_or_default();
                // Track catalogue objects so they get replayed to new clients
                self.track_catalogue_object(doc_id, &meta);
                if self.doc_manager.get(doc_id).is_none() {
                    self.doc_manager.create_with_id(doc_id, meta);
                }
                if let Err(e) = self.doc_manager.apply_update(doc_id, &update) {
                    tracing::warn!(%doc_id, error = %e, "failed to apply doc update");
                }
                self.synced_doc_ids.push(doc_id);
                // Forward to clients in scope
                self.forward_update_to_clients(doc_id, "main".into());
            }
            SyncPayload::DocSyncRequest {
                doc_id,
                state_vector,
            } => {
                // TODO: compute diff and respond
                tracing::debug!(%doc_id, sv_len = state_vector.len(), "DocSyncRequest received (not yet implemented)");
            }
            SyncPayload::DocSyncResponse {
                doc_id,
                diff,
                metadata,
            } => {
                tracing::debug!(%doc_id, diff_len = diff.len(), "server→DocSyncResponse");
                if self.doc_manager.get(doc_id).is_none() {
                    let meta = metadata
                        .as_ref()
                        .map(|m| m.metadata.clone())
                        .unwrap_or_default();
                    self.doc_manager.create_with_id(doc_id, meta);
                }
                if let Err(e) = self.doc_manager.apply_update(doc_id, &diff) {
                    tracing::warn!(%doc_id, error = %e, "failed to apply doc sync response");
                }
            }
            SyncPayload::DocPersistenceAck { doc_id, tier, .. } => {
                tracing::debug!(%doc_id, ?tier, "server→DocPersistenceAck");
                self.received_doc_acks.push((doc_id, tier));
                // Relay to interested clients
                for &cid in self.clients.keys() {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Client(cid),
                        payload: SyncPayload::DocPersistenceAck {
                            doc_id,
                            state_vector: Vec::new(),
                            tier,
                        },
                    });
                }
            }
            // Servers shouldn't send these to us
            SyncPayload::QuerySubscription { .. } | SyncPayload::QueryUnsubscription { .. } => {}
        }
    }

    /// Process a payload from a client.
    pub(super) fn process_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        payload: SyncPayload,
    ) {
        let _span = tracing::debug_span!("process_from_client", %client_id, payload = payload.variant_name()).entered();
        let Some(client) = self.clients.get(&client_id) else {
            tracing::warn!(%client_id, "message from unknown client, ignoring");
            return;
        };
        tracing::trace!(%client_id, role = ?client.role, payload = payload.variant_name(), "client→payload");

        match &payload {
            SyncPayload::ObjectUpdated {
                object_id,
                branch_name,
                ..
            } => {
                let object_id = *object_id;
                let branch_name = *branch_name;
                // TODO(task-17): Legacy commit-based sync from client — ignored.
                tracing::debug!(%object_id, %branch_name, "client→ObjectUpdated (legacy, ignored)");
            }
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                ..
            } => {
                let object_id = *object_id;
                let branch_name = *branch_name;
                // TODO(task-17): Legacy commit-based truncation from client — ignored.
                tracing::debug!(%object_id, %branch_name, "client→ObjectTruncated (legacy, ignored)");
            }
            // Handle query subscription with full Query struct
            // Queue for QueryManager to process (SyncManager doesn't know about QueryGraph)
            SyncPayload::QuerySubscription {
                query_id,
                query,
                session,
                propagation,
            } => {
                // Warn if the payload carries a session that differs from the one established
                // during the SSE handshake — this would indicate a spoofing attempt.
                if let (Some(payload_session), Some(client_session)) = (session, &client.session)
                    && payload_session != client_session
                {
                    tracing::warn!(
                        %client_id,
                        "QuerySubscription payload session does not match client session; using client session"
                    );
                }
                // Prefer the server-established session (set from validated auth headers
                // during the SSE handshake) over whatever the client claims in the payload.
                // Fall back to the payload only for anonymous/demo clients whose
                // client.session is None.  Note: despite the name, client.session is
                // server-owned state — not a value the client can supply directly.
                let effective_session = client.session.clone().or_else(|| session.clone());
                // Track origin for QuerySettled relay
                self.query_origin
                    .entry(*query_id)
                    .or_default()
                    .insert(client_id);
                self.pending_query_subscriptions
                    .push(PendingQuerySubscription {
                        client_id,
                        query_id: *query_id,
                        query: query.as_ref().clone(),
                        session: effective_session,
                        propagation: *propagation,
                    });
            }
            // Handle query unsubscription
            // Queue for QueryManager to process (remove server-side QueryGraph, forward upstream)
            SyncPayload::QueryUnsubscription { query_id } => {
                if let Some(client) = self.clients.get_mut(&client_id) {
                    client.queries.remove(query_id);
                }
                // Clean up query origin
                if let Some(clients) = self.query_origin.get_mut(query_id) {
                    clients.remove(&client_id);
                    if clients.is_empty() {
                        self.query_origin.remove(query_id);
                    }
                }
                self.pending_query_unsubscriptions
                    .push(PendingQueryUnsubscription {
                        client_id,
                        query_id: *query_id,
                    });
            }
            SyncPayload::PersistenceAck {
                object_id,
                branch_name,
                confirmed_commits,
                tier,
            } => {
                let object_id = *object_id;
                let branch_name = *branch_name;
                let tier = *tier;
                let confirmed_commits = confirmed_commits.clone();
                // A client relaying an ack (e.g. from a further-upstream tier)
                // Persist ack state
                for &commit_id in &confirmed_commits {
                    let _ = storage.store_ack_tier(commit_id, tier);
                    // Notify RuntimeCore of received ack
                    self.received_acks.push((commit_id, tier));
                }
                // Relay to interested clients (excluding the sender)
                let mut interested = HashSet::new();
                for &commit_id in &confirmed_commits {
                    if let Some(clients) = self.commit_interest.get(&commit_id) {
                        interested.extend(clients);
                    }
                }
                interested.remove(&client_id);
                for cid in interested {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Client(cid),
                        payload: SyncPayload::PersistenceAck {
                            object_id,
                            branch_name,
                            confirmed_commits: confirmed_commits.clone(),
                            tier,
                        },
                    });
                }
            }
            SyncPayload::QuerySettled {
                query_id,
                tier,
                through_seq: _,
            } => {
                // Client relaying a QuerySettled from downstream
                self.pending_query_settled.push((*query_id, *tier));
            }
            SyncPayload::DocUpdated {
                doc_id,
                update,
                metadata,
            } => {
                let doc_id = *doc_id;
                tracing::debug!(%doc_id, update_len = update.len(), "client→DocUpdated");
                match client.role {
                    ClientRole::Peer | ClientRole::Admin => {
                        // Trusted — apply directly
                        self.apply_doc_update_from_client(client_id, doc_id, update, metadata);
                    }
                    ClientRole::Backend => {
                        if payload.is_catalogue() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id: doc_id,
                                    branch_name: "main".into(),
                                }),
                            });
                            return;
                        }
                        self.apply_doc_update_from_client(client_id, doc_id, update, metadata);
                    }
                    ClientRole::User => {
                        let Some(session) = &client.session else {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::SessionRequired {
                                    object_id: doc_id,
                                    branch_name: "main".into(),
                                }),
                            });
                            return;
                        };
                        if payload.is_catalogue() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id: doc_id,
                                    branch_name: "main".into(),
                                }),
                            });
                            return;
                        }
                        let payload_metadata = metadata
                            .as_ref()
                            .map(|m| m.metadata.clone())
                            .unwrap_or_default();
                        let stored_metadata = self
                            .doc_manager
                            .get(doc_id)
                            .map(|d| d.metadata.clone())
                            .unwrap_or_default();
                        let operation = if self.doc_manager.get(doc_id).is_some() {
                            Operation::Update
                        } else {
                            Operation::Insert
                        };
                        // Use payload metadata for new docs, stored metadata for existing
                        let meta = if stored_metadata.is_empty() {
                            payload_metadata
                        } else {
                            stored_metadata
                        };
                        // old_content / new_content: policy evaluator will decode from Yrs
                        // update or read DocManager directly. We pass None here.
                        self.queue_for_permission_check(
                            client_id,
                            payload,
                            session.clone(),
                            meta,
                            None,
                            None,
                            operation,
                        );
                    }
                }
            }
            SyncPayload::DocSyncRequest {
                doc_id,
                state_vector,
            } => {
                let doc_id = *doc_id;
                // TODO: compute diff and respond
                tracing::debug!(%doc_id, sv_len = state_vector.len(), "client→DocSyncRequest (not yet implemented)");
            }
            SyncPayload::DocSyncResponse {
                doc_id,
                diff,
                metadata,
            } => {
                let doc_id = *doc_id;
                tracing::debug!(%doc_id, diff_len = diff.len(), "client→DocSyncResponse");
                if self.doc_manager.get(doc_id).is_none() {
                    let meta = metadata
                        .as_ref()
                        .map(|m| m.metadata.clone())
                        .unwrap_or_default();
                    self.doc_manager.create_with_id(doc_id, meta);
                }
                if let Err(e) = self.doc_manager.apply_update(doc_id, diff) {
                    tracing::warn!(%doc_id, error = %e, "failed to apply doc sync response from client");
                }
            }
            SyncPayload::DocPersistenceAck { doc_id, tier, .. } => {
                let doc_id = *doc_id;
                let tier = *tier;
                tracing::debug!(%doc_id, ?tier, "client→DocPersistenceAck");
                self.received_doc_acks.push((doc_id, tier));
            }
            // Clients shouldn't send these
            SyncPayload::Error(_) => {}
        }
    }

    /// Apply a payload from a client (either directly or after approval).
    pub(super) fn apply_payload_from_client<H: Storage>(
        &mut self,
        _storage: &mut H,
        client_id: ClientId,
        payload: SyncPayload,
        _was_pending: bool,
    ) {
        match payload {
            SyncPayload::ObjectUpdated {
                object_id,
                branch_name,
                ..
            } => {
                // TODO(task-17): Legacy commit-based sync — no longer applied.
                tracing::debug!(%object_id, %branch_name, "apply_payload_from_client: ObjectUpdated (legacy, ignored)");
            }
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                ..
            } => {
                // TODO(task-17): Legacy commit-based truncation — no longer applied.
                tracing::debug!(%object_id, %branch_name, "apply_payload_from_client: ObjectTruncated (legacy, ignored)");
            }
            SyncPayload::DocUpdated {
                doc_id,
                update,
                metadata,
            } => {
                self.apply_doc_update_from_client(client_id, doc_id, &update, &metadata);
            }
            _ => {}
        }
    }

    /// Apply a DocUpdated payload from a client: create doc if needed, apply update, forward.
    fn apply_doc_update_from_client(
        &mut self,
        client_id: ClientId,
        doc_id: ObjectId,
        update: &[u8],
        metadata: &Option<ObjectMetadata>,
    ) {
        if self.doc_manager.get(doc_id).is_none() {
            let meta = metadata
                .as_ref()
                .map(|m| m.metadata.clone())
                .unwrap_or_default();
            self.doc_manager.create_with_id(doc_id, meta);
        }
        if let Err(e) = self.doc_manager.apply_update(doc_id, update) {
            tracing::warn!(%doc_id, error = %e, "failed to apply doc update from client");
        }
        self.synced_doc_ids.push(doc_id);

        // Emit doc-level persistence acks for each of this node's durability tiers
        for &tier in &self.my_tiers.clone() {
            // Ack back to the sending client
            self.outbox.push(OutboxEntry {
                destination: Destination::Client(client_id),
                payload: SyncPayload::DocPersistenceAck {
                    doc_id,
                    state_vector: Vec::new(), // Placeholder — full SV not needed for ack matching
                    tier,
                },
            });
            // Also notify RuntimeCore locally for relay scenarios
            self.received_doc_acks.push((doc_id, tier));
        }

        // Forward to servers
        self.forward_doc_update_to_servers(doc_id, update.to_vec(), metadata.clone());
        // TODO: forward to other clients
    }
}
