use super::*;
use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
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
                metadata,
                branch_name,
                commits,
            } => {
                tracing::debug!(%object_id, %branch_name, commits = commits.len(), "server→ObjectUpdated");
                let persisted =
                    self.apply_object_updated(storage, object_id, metadata, branch_name, commits);

                // Emit ack back to server for each local durability identity.
                if !persisted.is_empty() {
                    for tier in self.my_tiers.iter().copied() {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Server(server_id),
                            payload: SyncPayload::PersistenceAck {
                                object_id,
                                branch_name,
                                confirmed_commits: persisted.clone(),
                                tier,
                            },
                        });
                    }
                }

                // Forward to clients whose scope includes this object/branch
                self.forward_update_to_clients(object_id, branch_name);
            }
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                tails,
            } => {
                // Apply truncation locally
                let _ = self.object_manager.truncate_branch(
                    storage,
                    object_id,
                    branch_name,
                    tails.clone(),
                );

                // Forward to clients
                self.forward_truncation_to_clients(object_id, branch_name, tails);
            }
            SyncPayload::PersistenceAck {
                object_id,
                branch_name,
                confirmed_commits,
                tier,
            } => {
                tracing::debug!(%object_id, ?tier, commits = confirmed_commits.len(), "server→PersistenceAck");
                // Persist ack state and update in-memory
                for &commit_id in &confirmed_commits {
                    let _ = storage.store_ack_tier(commit_id, tier);
                    if let Some(commit) =
                        self.object_manager
                            .get_commit_mut(object_id, &branch_name, commit_id)
                    {
                        commit.ack_state.confirmed_tiers.insert(tier);
                    }
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
                commits,
                ..
            } => {
                let object_id = *object_id;
                let branch_name = *branch_name;
                match client.role {
                    ClientRole::Peer | ClientRole::Admin => {
                        // Trusted — apply directly
                        self.apply_payload_from_client(storage, client_id, payload, false);
                    }
                    ClientRole::Backend => {
                        if payload.is_catalogue() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        }
                        self.apply_payload_from_client(storage, client_id, payload, false);
                    }
                    ClientRole::User => {
                        // User requires session
                        let Some(session) = &client.session else {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::SessionRequired {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        };
                        // User cannot write catalogue objects
                        if payload.is_catalogue() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        }
                        // Row data — queue for ReBAC permission check
                        let (metadata, old_content) = self
                            .object_manager
                            .get(object_id)
                            .map(|obj| {
                                let old = obj
                                    .branches
                                    .get(&branch_name)
                                    .and_then(|branch| {
                                        branch
                                            .tips
                                            .iter()
                                            .next()
                                            .and_then(|tip_id| branch.commits.get(tip_id))
                                    })
                                    .map(|commit| commit.content.clone());
                                (obj.metadata.clone(), old)
                            })
                            .unwrap_or_default();
                        let new_content = commits.last().map(|c| c.content.clone());
                        let operation = if old_content.is_some() {
                            Operation::Update
                        } else {
                            Operation::Insert
                        };
                        self.queue_for_permission_check(
                            client_id,
                            payload,
                            session.clone(),
                            metadata,
                            old_content,
                            new_content,
                            operation,
                        );
                    }
                }
            }
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                ..
            } => {
                let object_id = *object_id;
                let branch_name = *branch_name;
                match client.role {
                    ClientRole::Peer | ClientRole::Admin => {
                        self.apply_payload_from_client(storage, client_id, payload, false);
                    }
                    ClientRole::Backend => {
                        if payload.is_catalogue() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        }
                        self.apply_payload_from_client(storage, client_id, payload, false);
                    }
                    ClientRole::User => {
                        let Some(session) = &client.session else {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::SessionRequired {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        };
                        let (metadata, old_content) = self
                            .object_manager
                            .get(object_id)
                            .map(|obj| {
                                let old = obj
                                    .branches
                                    .get(&branch_name)
                                    .and_then(|branch| {
                                        branch
                                            .tips
                                            .iter()
                                            .next()
                                            .and_then(|tip_id| branch.commits.get(tip_id))
                                    })
                                    .map(|commit| commit.content.clone());
                                (obj.metadata.clone(), old)
                            })
                            .unwrap_or_default();
                        self.queue_for_permission_check(
                            client_id,
                            payload,
                            session.clone(),
                            metadata,
                            old_content,
                            None,
                            Operation::Delete,
                        );
                    }
                }
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
                // Persist ack state and update in-memory
                for &commit_id in &confirmed_commits {
                    let _ = storage.store_ack_tier(commit_id, tier);
                    if let Some(commit) =
                        self.object_manager
                            .get_commit_mut(object_id, &branch_name, commit_id)
                    {
                        commit.ack_state.confirmed_tiers.insert(tier);
                    }
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
            // Clients shouldn't send these
            SyncPayload::Error(_) => {}
        }
    }

    /// Apply a payload from a client (either directly or after approval).
    pub(super) fn apply_payload_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        payload: SyncPayload,
        _was_pending: bool,
    ) {
        match payload {
            SyncPayload::ObjectUpdated {
                object_id,
                metadata,
                branch_name,
                commits,
            } => {
                // Track client interest for ack relay
                for commit in &commits {
                    self.commit_interest
                        .entry(commit.id())
                        .or_default()
                        .insert(client_id);
                }

                let persisted =
                    self.apply_object_updated(storage, object_id, metadata, branch_name, commits);

                // Emit ack back to client for each local durability identity.
                if !persisted.is_empty() {
                    for tier in self.my_tiers.iter().copied() {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(client_id),
                            payload: SyncPayload::PersistenceAck {
                                object_id,
                                branch_name,
                                confirmed_commits: persisted.clone(),
                                tier,
                            },
                        });
                    }
                }

                // Forward to servers
                self.forward_update_to_servers(object_id, branch_name);

                // Forward to other clients (not the sender)
                self.forward_update_to_clients_except(object_id, branch_name, client_id);
            }
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                tails,
            } => {
                let _ = self.object_manager.truncate_branch(
                    storage,
                    object_id,
                    branch_name,
                    tails.clone(),
                );

                // Forward to servers
                self.forward_truncation_to_servers(object_id, branch_name, tails.clone());

                // Forward to other clients
                self.forward_truncation_to_clients_except(object_id, branch_name, tails, client_id);
            }
            _ => {}
        }
    }

    /// Apply an ObjectUpdated payload to the local ObjectManager.
    /// Returns the set of newly persisted commit IDs (excludes duplicates).
    pub(super) fn apply_object_updated<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        metadata: Option<ObjectMetadata>,
        branch_name: BranchName,
        commits: Vec<Commit>,
    ) -> HashSet<CommitId> {
        // If we don't have this object yet and metadata is provided, create it
        if self.object_manager.get(object_id).is_none() {
            if let Some(meta) = metadata {
                self.object_manager
                    .receive_object(storage, object_id, meta.metadata);
            } else {
                return HashSet::new();
            }
        }

        let mut persisted = HashSet::new();
        for commit in commits {
            let commit_id = commit.id();
            // Check if commit already exists before applying
            let already_exists = self
                .object_manager
                .get(object_id)
                .and_then(|obj| obj.branches.get(&branch_name))
                .is_some_and(|branch| branch.commits.contains_key(&commit_id));

            if self
                .object_manager
                .receive_commit(storage, object_id, branch_name, commit)
                .is_ok()
                && !already_exists
            {
                persisted.insert(commit_id);
            }
        }
        persisted
    }
}
