use super::*;
use crate::commit::{Commit, CommitId};
use crate::metadata::MetadataKey;
use crate::object::{BranchName, Object, ObjectId};
use crate::query_manager::policy::Operation;
use crate::row_regions::{BatchId, RowState, StoredRowVersion};
use crate::storage::Storage;
use std::collections::{HashMap, HashSet};

fn short_hash(hash: &impl ToString) -> String {
    hash.to_string().chars().take(12).collect()
}

fn log_schema_warning(origin: &str, warning: &SchemaWarning) {
    tracing::warn!(
        origin,
        query_id = warning.query_id.0,
        table = warning.table_name,
        row_count = warning.row_count,
        from_hash = %warning.from_hash,
        to_hash = %warning.to_hash,
        "Detected {} rows of {} with differing schema versions. To ensure data visibility and forward/backward compatibility please create a new migration with `npx jazz-tools migrations create {} {}`",
        warning.row_count,
        warning.table_name,
        short_hash(&warning.from_hash),
        short_hash(&warning.to_hash),
    );
}

impl SyncManager {
    fn ensure_object_metadata<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
    ) {
        if self.object_manager.get(object_id).is_none() {
            if storage
                .load_object_metadata(object_id)
                .ok()
                .flatten()
                .is_none()
            {
                let _ = storage.create_object(object_id, metadata.clone());
            }

            self.object_manager.objects.insert(
                object_id,
                Object {
                    id: object_id,
                    metadata,
                    branches: HashMap::new(),
                },
            );
            return;
        }

        if let Some(object) = self.object_manager.objects.get_mut(&object_id)
            && !metadata.is_empty()
        {
            object.metadata = metadata;
        }
    }

    fn row_metadata_from_payload<H: Storage>(
        &self,
        storage: &H,
        row: &StoredRowVersion,
        metadata: Option<&ObjectMetadata>,
    ) -> Option<HashMap<String, String>> {
        if let Some(metadata) = metadata {
            return Some(metadata.metadata.clone());
        }

        self.object_manager
            .get(row.row_id)
            .map(|object| object.metadata.clone())
            .or_else(|| storage.load_object_metadata(row.row_id).ok().flatten())
    }

    fn apply_row_updated<H: Storage>(
        &mut self,
        storage: &mut H,
        metadata: Option<ObjectMetadata>,
        row: StoredRowVersion,
    ) -> Option<RowUpdateEvent> {
        let metadata = self.row_metadata_from_payload(storage, &row, metadata.as_ref())?;
        let table = metadata.get(MetadataKey::Table.as_str())?.clone();
        let branch_name = row.branch.clone();
        let previous_row = storage
            .load_visible_region_row(&table, &branch_name, row.row_id)
            .ok()
            .flatten();
        let is_new_object = self.object_manager.get(row.row_id).is_none()
            && storage
                .load_object_metadata(row.row_id)
                .ok()
                .flatten()
                .is_none();

        self.ensure_object_metadata(storage, row.row_id, metadata.clone());

        if let Err(error) = storage.append_history_region_rows(&table, std::slice::from_ref(&row)) {
            tracing::warn!(
                table,
                branch = branch_name,
                row_id = %row.row_id,
                %error,
                "failed to append synced history row"
            );
        }

        if let Err(error) = storage.upsert_visible_region_rows(&table, std::slice::from_ref(&row)) {
            tracing::warn!(
                table,
                branch = branch_name,
                row_id = %row.row_id,
                %error,
                "failed to upsert synced visible row"
            );
        }

        Some(RowUpdateEvent {
            object_id: row.row_id,
            metadata,
            row,
            previous_row,
            is_new_object,
        })
    }

    fn table_for_object_id<H: Storage>(&self, storage: &H, object_id: ObjectId) -> Option<String> {
        self.object_manager
            .get(object_id)
            .and_then(|object| object.metadata.get(MetadataKey::Table.as_str()).cloned())
            .or_else(|| {
                storage
                    .load_object_metadata(object_id)
                    .ok()
                    .flatten()
                    .and_then(|metadata| metadata.get(MetadataKey::Table.as_str()).cloned())
            })
    }

    fn patch_row_region_state<H: Storage>(
        &self,
        storage: &mut H,
        row_id: ObjectId,
        version_id: CommitId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) {
        let Some(table) = self.table_for_object_id(storage, row_id) else {
            return;
        };

        if let Err(error) = storage.patch_row_region_rows_by_batch(
            &table,
            BatchId::from_commit_id(version_id),
            state,
            confirmed_tier,
        ) {
            tracing::warn!(
                %row_id,
                ?version_id,
                table,
                ?state,
                ?confirmed_tier,
                %error,
                "failed to patch row-region system state"
            );
        }
    }

    fn apply_row_version_state_changed<H: Storage>(
        &mut self,
        storage: &mut H,
        row_id: ObjectId,
        branch_name: BranchName,
        version_id: CommitId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) {
        if confirmed_tier.is_none() && state.is_none() {
            return;
        }

        if let Some(tier) = confirmed_tier {
            let _ = storage.store_ack_tier(version_id, tier);
        }
        self.patch_row_region_state(storage, row_id, version_id, state, confirmed_tier);

        if let Some(tier) = confirmed_tier {
            if let Some(commit) =
                self.object_manager
                    .get_commit_mut(row_id, &branch_name, version_id)
            {
                commit.ack_state.confirmed_tiers.insert(tier);
            }
            self.received_acks.push((version_id, tier));
        }
    }

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
            SyncPayload::RowVersionCreated { metadata, row }
            | SyncPayload::RowVersionNeeded { metadata, row } => {
                let object_id = row.row_id;
                let branch_name = BranchName::new(&row.branch);
                tracing::debug!(
                    %object_id,
                    %branch_name,
                    "server→row-version payload"
                );
                let persisted = self
                    .apply_row_updated(storage, metadata, row.clone())
                    .into_iter()
                    .inspect(|update| self.pending_row_updates.push(update.clone()))
                    .map(|update| update.row.version_id())
                    .collect::<HashSet<_>>();

                if !persisted.is_empty() {
                    for version_id in persisted {
                        for tier in self.my_tiers.iter().copied() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Server(server_id),
                                payload: SyncPayload::RowVersionStateChanged {
                                    row_id: object_id,
                                    branch_name,
                                    version_id,
                                    state: None,
                                    confirmed_tier: Some(tier),
                                },
                            });
                        }
                    }
                }

                self.forward_update_to_clients_with_storage(storage, object_id, branch_name);
            }
            SyncPayload::RowVersionStateChanged {
                row_id,
                branch_name,
                version_id,
                state,
                confirmed_tier,
            } => {
                tracing::debug!(
                    %row_id,
                    %branch_name,
                    ?version_id,
                    ?state,
                    ?confirmed_tier,
                    "server→RowVersionStateChanged"
                );
                self.apply_row_version_state_changed(
                    storage,
                    row_id,
                    branch_name,
                    version_id,
                    state,
                    confirmed_tier,
                );

                let mut interested = HashSet::new();
                if let Some(clients) = self.commit_interest.get(&version_id) {
                    interested.extend(clients);
                }
                for cid in interested {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Client(cid),
                        payload: SyncPayload::RowVersionStateChanged {
                            row_id,
                            branch_name,
                            version_id,
                            state,
                            confirmed_tier,
                        },
                    });
                }
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
                    self.patch_row_region_state(storage, object_id, commit_id, None, Some(tier));
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
            SyncPayload::SchemaWarning(warning) => {
                log_schema_warning("server", &warning);

                if let Some(clients) = self.query_origin.get(&warning.query_id) {
                    for &cid in clients {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(cid),
                            payload: SyncPayload::SchemaWarning(warning.clone()),
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
                metadata,
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
                        // User cannot write catalogue objects, except for
                        // development-only structural schema auto-push.
                        if payload.is_catalogue() {
                            if self.allow_unprivileged_schema_catalogue_writes
                                && payload.is_structural_schema_catalogue()
                            {
                                self.apply_payload_from_client(storage, client_id, payload, false);
                                return;
                            }
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
                        let payload_metadata = metadata
                            .as_ref()
                            .map(|meta| meta.metadata.clone())
                            .unwrap_or_default();
                        let (stored_metadata, old_content) = self
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
                        // For brand-new rows, metadata may only be present in the inbound payload
                        // because the object has not been applied to ObjectManager yet.
                        let metadata = if old_content.is_none() && stored_metadata.is_empty() {
                            payload_metadata
                        } else {
                            stored_metadata
                        };
                        let is_delete = Self::is_deleted_update(commits);
                        let new_content = if is_delete {
                            None
                        } else {
                            commits.last().map(|c| c.content.clone())
                        };
                        let operation = if is_delete {
                            Operation::Delete
                        } else if old_content.is_some() {
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
            SyncPayload::RowVersionCreated { metadata, row }
            | SyncPayload::RowVersionNeeded { metadata, row } => {
                let object_id = row.row_id;
                let branch_name = BranchName::new(&row.branch);
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
                        if payload.is_catalogue() {
                            if self.allow_unprivileged_schema_catalogue_writes
                                && payload.is_structural_schema_catalogue()
                            {
                                self.apply_payload_from_client(storage, client_id, payload, false);
                                return;
                            }
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        }

                        let payload_metadata = metadata
                            .as_ref()
                            .map(|meta| meta.metadata.clone())
                            .unwrap_or_default();
                        let (stored_metadata, old_content) = self
                            .row_metadata_from_payload(storage, row, metadata.as_ref())
                            .and_then(|stored_metadata| {
                                let table =
                                    stored_metadata.get(MetadataKey::Table.as_str())?.clone();
                                let old_content = storage
                                    .load_visible_region_row(&table, &row.branch, row.row_id)
                                    .ok()
                                    .flatten()
                                    .map(|previous| previous.data);
                                Some((stored_metadata, old_content))
                            })
                            .unwrap_or_else(|| (HashMap::new(), None));
                        let metadata = if old_content.is_none() && stored_metadata.is_empty() {
                            payload_metadata
                        } else {
                            stored_metadata
                        };
                        let new_content = (!row.is_deleted).then_some(row.data.clone());
                        let operation = if row.is_deleted {
                            Operation::Delete
                        } else if old_content.is_some() {
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
                // Build effective session: identity (user_id) comes from the
                // server-established session (set during the SSE auth handshake) and
                // cannot be overridden by the payload. However, ephemeral per-subscription
                // claims supplied in the payload — such as a join_code for invite flows —
                // are merged in when the user_id matches, so that policy conditions like
                // `claims.join_code` evaluate correctly for this subscription.
                let effective_session = match (&client.session, session) {
                    (Some(client_session), Some(payload_session)) => {
                        if client_session.user_id != payload_session.user_id {
                            tracing::warn!(
                                %client_id,
                                "QuerySubscription payload session user_id does not match client session; ignoring payload session"
                            );
                            Some(client_session.clone())
                        } else {
                            // Same user: merge claims. Payload provides ephemeral claims
                            // (e.g. join_code); client session claims take precedence so
                            // auth-established values cannot be spoofed.
                            let merged_claims = if let (
                                serde_json::Value::Object(client_map),
                                serde_json::Value::Object(payload_map),
                            ) =
                                (&client_session.claims, &payload_session.claims)
                            {
                                let mut merged = payload_map.clone();
                                merged.extend(client_map.clone());
                                serde_json::Value::Object(merged)
                            } else {
                                client_session.claims.clone()
                            };
                            Some(Session {
                                user_id: client_session.user_id.clone(),
                                claims: merged_claims,
                            })
                        }
                    }
                    (Some(client_session), None) => Some(client_session.clone()),
                    (None, payload_session) => payload_session.clone(),
                };
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
                    self.patch_row_region_state(storage, object_id, commit_id, None, Some(tier));
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
            SyncPayload::RowVersionStateChanged {
                row_id,
                branch_name,
                version_id,
                state,
                confirmed_tier,
            } => {
                self.apply_row_version_state_changed(
                    storage,
                    *row_id,
                    *branch_name,
                    *version_id,
                    *state,
                    *confirmed_tier,
                );
                let mut interested = HashSet::new();
                if let Some(clients) = self.commit_interest.get(version_id) {
                    interested.extend(clients);
                }
                interested.remove(&client_id);
                for cid in interested {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Client(cid),
                        payload: SyncPayload::RowVersionStateChanged {
                            row_id: *row_id,
                            branch_name: *branch_name,
                            version_id: *version_id,
                            state: *state,
                            confirmed_tier: *confirmed_tier,
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
            SyncPayload::SchemaWarning(warning) => {
                tracing::warn!(
                    %client_id,
                    query_id = warning.query_id.0,
                    "client attempted to send SchemaWarning payload; ignoring"
                );
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
            SyncPayload::RowVersionCreated { metadata, row }
            | SyncPayload::RowVersionNeeded { metadata, row } => {
                let object_id = row.row_id;
                let branch_name = BranchName::new(&row.branch);
                let version_id = row.version_id();
                self.commit_interest
                    .entry(version_id)
                    .or_default()
                    .insert(client_id);

                let persisted = self
                    .apply_row_updated(storage, metadata, row)
                    .into_iter()
                    .inspect(|update| self.pending_row_updates.push(update.clone()))
                    .map(|update| update.row.version_id())
                    .collect::<HashSet<_>>();

                if !persisted.is_empty() {
                    for version_id in persisted {
                        for tier in self.my_tiers.iter().copied() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::RowVersionStateChanged {
                                    row_id: object_id,
                                    branch_name,
                                    version_id,
                                    state: None,
                                    confirmed_tier: Some(tier),
                                },
                            });
                        }
                    }
                }

                self.forward_update_to_servers_with_storage(storage, object_id, branch_name);
                self.forward_update_to_clients_except_with_storage(
                    storage,
                    object_id,
                    branch_name,
                    client_id,
                );
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
        if let Some(meta) = metadata.as_ref() {
            self.track_catalogue_object(object_id, &meta.metadata);
        }

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

    /// Soft deletes travel over sync as `ObjectUpdated`; we infer them from the
    /// newest commit carrying delete metadata on the payload's tip.
    fn is_deleted_update(commits: &[Commit]) -> bool {
        commits.last().is_some_and(|commit| {
            commit
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.contains_key(MetadataKey::Delete.as_str()))
        })
    }
}
