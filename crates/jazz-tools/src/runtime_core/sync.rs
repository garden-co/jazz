use super::*;

use crate::batch_fate::BatchFate;
use crate::sync_manager::SyncManager;

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    pub(crate) fn settlement_target(&self) -> DurabilityTier {
        self.schema_manager
            .query_manager()
            .sync_manager()
            .settlement_target()
    }

    /// One terminal predicate for batch settlement
    /// (spec: batch_settlement_lifecycle §1), evaluated against this
    /// runtime's settlement target.
    pub(crate) fn batch_needs_settlement(&self, fate: Option<&BatchFate>) -> bool {
        self.schema_manager
            .query_manager()
            .sync_manager()
            .batch_needs_settlement(fate)
    }

    fn pending_batch_ids_needing_reconciliation(&self) -> Vec<crate::row_histories::BatchId> {
        let target = self.settlement_target();
        let fates_by_batch: HashMap<crate::row_histories::BatchId, BatchFate> = self
            .storage
            .scan_authoritative_batch_fates()
            .unwrap_or_default()
            .into_iter()
            .map(|fate| (fate.batch_id(), fate))
            .collect();

        let mut batch_ids = self
            .storage
            .scan_sealed_batch_submissions()
            .unwrap_or_default()
            .into_iter()
            .filter(|submission| {
                let fate = fates_by_batch.get(&submission.batch_id);
                // A stored `Missing` fate still owes the server a retransmit
                // of rows + seal. The live-delivery path retransmits
                // immediately, but after a restart this pending-set derivation
                // is the only thing that re-offers the batch.
                matches!(fate, Some(BatchFate::Missing { .. }))
                    || SyncManager::fate_needs_settlement_at(fate, target)
            })
            .map(|submission| submission.batch_id)
            .collect::<Vec<_>>();

        batch_ids.extend(
            fates_by_batch
                .values()
                .filter(|fate| SyncManager::fate_needs_settlement_at(Some(fate), target))
                .map(|fate| fate.batch_id()),
        );
        // No row-history sweep here: every committed direct batch durably
        // carries a fate (self-confirming runtimes write one at commit) or a
        // sealed submission (retained until the settlement target is reached),
        // so the submission/fate scans above already cover the pending set. A
        // `VisibleDirect` row with neither is a storage inconsistency to
        // repair explicitly, not a reason to scan all row history on every
        // server (re)connect (spec: batch_settlement_lifecycle §4/§6).
        batch_ids.extend(self.local_batch_record_cache.values().filter_map(|record| {
            let latest_fate = fates_by_batch
                .get(&record.batch_id)
                .or(record.latest_fate.as_ref());
            SyncManager::fate_needs_settlement_at(latest_fate, target).then_some(record.batch_id)
        }));
        batch_ids.sort();
        batch_ids.dedup();
        batch_ids.retain(|batch_id| !self.local_batch_rows(*batch_id).is_empty());
        batch_ids
    }

    #[cfg(test)]
    pub(crate) fn pending_batch_ids_needing_reconciliation_for_test(
        &self,
    ) -> Vec<crate::row_histories::BatchId> {
        self.pending_batch_ids_needing_reconciliation()
    }

    /// Retire a batch's pending bookkeeping once its fate reaches the
    /// settlement target. The terminal fate row stays behind as the tombstone;
    /// row history is untouched.
    pub(crate) fn retire_settled_batch(&mut self, batch_id: crate::row_histories::BatchId) {
        // Servers broadcast fates to every interested client and may
        // re-deliver them, so most settled fates arriving here describe
        // batches this node never tracked. Probe before deleting to avoid
        // dirtying storage (and forcing a flush) when there is nothing to
        // retire.
        let had_cached_record = self.local_batch_record_cache.remove(&batch_id).is_some();
        let has_stored_bookkeeping = had_cached_record
            || self
                .storage
                .load_sealed_batch_submission(batch_id)
                .ok()
                .flatten()
                .is_some()
            || self
                .storage
                .load_local_batch_record(batch_id)
                .ok()
                .flatten()
                .is_some();
        if !has_stored_bookkeeping {
            return;
        }
        if let Err(error) = self.storage.delete_sealed_batch_submission(batch_id) {
            tracing::warn!(?batch_id, %error, "failed to retire sealed batch submission");
        }
        if let Err(error) = self.storage.delete_local_batch_record(batch_id) {
            tracing::warn!(?batch_id, %error, "failed to retire local batch record");
        }
        self.mark_storage_write_pending_flush();
    }

    // =========================================================================
    // Sync Operations
    // =========================================================================

    /// Push a sync message to the inbox (from network).
    pub fn push_sync_inbox(&mut self, entry: InboxEntry) {
        if matches!(
            entry.payload,
            crate::sync_manager::SyncPayload::CatalogueEntryUpdated { .. }
        ) {
            self.mark_transport_catalogue_state_hash_dirty();
        }
        if entry.payload.writes_storage() {
            self.mark_storage_write_pending_flush();
        }
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(entry);
    }

    /// Add a server connection.
    pub fn add_server(&mut self, server_id: ServerId) {
        self.add_server_with_catalogue_state_hash(server_id, None);
    }

    /// Add a server connection, optionally comparing the upstream catalogue
    /// digest first so unchanged catalogue objects are not replayed.
    pub fn add_server_with_catalogue_state_hash(
        &mut self,
        server_id: ServerId,
        remote_catalogue_state_hash: Option<&str>,
    ) {
        self.add_server_with_catalogue_state_hash_and_permission(
            server_id,
            remote_catalogue_state_hash,
            true,
        );
    }

    pub fn add_server_with_catalogue_state_hash_and_permission(
        &mut self,
        server_id: ServerId,
        remote_catalogue_state_hash: Option<&str>,
        can_publish_catalogue: bool,
    ) {
        info!(%server_id, "adding server");
        let local_catalogue_state_hash = self.schema_manager.catalogue_state_hash();
        let skip_catalogue_sync = !can_publish_catalogue
            || remote_catalogue_state_hash
                .is_some_and(|remote_hash| remote_hash == local_catalogue_state_hash);
        self.schema_manager
            .query_manager_mut()
            .add_server_with_storage(&self.storage, server_id, skip_catalogue_sync);
        let pending_batch_ids = self.pending_batch_ids_needing_reconciliation();
        for batch_id in &pending_batch_ids {
            self.retransmit_local_batch_to_servers(*batch_id);
        }
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .request_batch_fates_from_server(server_id, pending_batch_ids);
        self.immediate_tick();
    }

    pub fn reconcile_local_batch_with_server(&mut self, batch_id: crate::row_histories::BatchId) {
        let Some(server_id) = self
            .schema_manager
            .query_manager()
            .sync_manager()
            .server_ids()
            .next()
        else {
            return;
        };
        self.retransmit_local_batch_to_servers(batch_id);
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .request_batch_fates_from_server(server_id, vec![batch_id]);
        self.immediate_tick();
    }

    /// Remove a server connection.
    pub fn remove_server(&mut self, server_id: ServerId) {
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .remove_server(server_id);
        self.parked_sync_messages_by_server_seq.remove(&server_id);
        self.next_expected_server_seq.remove(&server_id);
        self.last_applied_server_seq.remove(&server_id);
    }

    /// Add a client connection.
    pub fn add_client(&mut self, client_id: ClientId, session: Option<Session>) {
        info!(%client_id, has_session = session.is_some(), "adding client");
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        sm.add_client_with_storage(&self.storage, client_id);
        if let Some(s) = session {
            sm.set_client_session(client_id, s);
        }
        self.immediate_tick();
    }

    /// Ensure a client exists with the given session.
    ///
    /// If the client already exists, updates the session. This is idempotent —
    /// calling with the same session is a no-op. Calling with a new session
    /// updates it in place without resetting the client's role or other state.
    ///
    /// A session is always required — callers must authenticate before
    /// registering a client.
    pub fn ensure_client_with_session(&mut self, client_id: ClientId, session: Session) {
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        if sm.get_client(client_id).is_some() {
            sm.set_client_session(client_id, session);
        } else {
            sm.add_client_with_storage(&self.storage, client_id);
            sm.set_client_session(client_id, session);
            self.immediate_tick();
        }
    }

    /// Ensure an authenticated session client exists, replaying catalogue
    /// entries only when the client's digest is missing or stale.
    pub fn ensure_client_with_session_and_catalogue_state_hash(
        &mut self,
        client_id: ClientId,
        session: Session,
        remote_catalogue_state_hash: Option<&str>,
    ) {
        use crate::sync_manager::ClientRole;

        self.ensure_client_with_role_and_catalogue_state_hash(
            client_id,
            ClientRole::User,
            Some(session),
            remote_catalogue_state_hash,
        );
    }

    /// Remove a client connection.
    ///
    /// Returns `false` if the client has unprocessed messages — either
    /// parked in RuntimeCore (pre-inbox, from `push_sync_inbox`) or
    /// already in SyncManager's inbox. The caller should retry later.
    pub fn remove_client(&mut self, client_id: ClientId) -> bool {
        use crate::sync_manager::Source;

        let has_parked = self
            .parked_sync_messages
            .iter()
            .any(|e| e.source == Source::Client(client_id));
        if has_parked {
            tracing::warn!(
                %client_id,
                "skipping reap: client has parked sync messages"
            );
            return false;
        }

        self.schema_manager
            .query_manager_mut()
            .remove_client(client_id)
    }

    /// Promote a client to Admin role (full access, no ReBAC).
    pub fn set_client_admin(&mut self, client_id: ClientId) {
        use crate::sync_manager::ClientRole;
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(client_id, ClientRole::Admin);
    }

    /// Ensure a client exists and is marked as Admin without resetting state.
    pub fn ensure_client_as_admin(&mut self, client_id: ClientId) {
        use crate::sync_manager::ClientRole;
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        if sm.get_client(client_id).is_some() {
            sm.set_client_role(client_id, ClientRole::Admin);
        } else {
            sm.add_client_with_storage(&self.storage, client_id);
            sm.set_client_role(client_id, ClientRole::Admin);
            self.immediate_tick();
        }
    }

    /// Ensure an admin client exists, replaying catalogue entries only when
    /// its digest is missing or stale.
    pub fn ensure_client_as_admin_with_catalogue_state_hash(
        &mut self,
        client_id: ClientId,
        remote_catalogue_state_hash: Option<&str>,
    ) {
        use crate::sync_manager::ClientRole;

        self.ensure_client_with_role_and_catalogue_state_hash(
            client_id,
            ClientRole::Admin,
            None,
            remote_catalogue_state_hash,
        );
    }

    /// Promote a client to Backend role (row access, no catalogue writes).
    pub fn set_client_backend(&mut self, client_id: ClientId) {
        use crate::sync_manager::ClientRole;
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(client_id, ClientRole::Backend);
    }

    /// Ensure a client exists and is marked as Backend without resetting state.
    pub fn ensure_client_as_backend(&mut self, client_id: ClientId) {
        use crate::sync_manager::ClientRole;
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        if sm.get_client(client_id).is_some() {
            sm.set_client_role(client_id, ClientRole::Backend);
        } else {
            sm.add_client_with_storage(&self.storage, client_id);
            sm.set_client_role(client_id, ClientRole::Backend);
            self.immediate_tick();
        }
    }

    /// Ensure a backend client exists, replaying catalogue entries only when
    /// its digest is missing or stale.
    pub fn ensure_client_as_backend_with_catalogue_state_hash(
        &mut self,
        client_id: ClientId,
        remote_catalogue_state_hash: Option<&str>,
    ) {
        use crate::sync_manager::ClientRole;

        self.ensure_client_with_role_and_catalogue_state_hash(
            client_id,
            ClientRole::Backend,
            None,
            remote_catalogue_state_hash,
        );
    }

    /// Ensure a client exists and is marked as Peer without resetting state.
    pub fn ensure_client_as_peer(&mut self, client_id: ClientId) {
        self.ensure_client_as_peer_with_catalogue_state_hash(client_id, None);
    }

    /// Ensure a peer client exists, then replay catalogue entries only when
    /// the peer's catalogue digest is missing or stale.
    pub fn ensure_client_as_peer_with_catalogue_state_hash(
        &mut self,
        client_id: ClientId,
        remote_catalogue_state_hash: Option<&str>,
    ) {
        use crate::sync_manager::ClientRole;

        let local_catalogue_state_hash = self.schema_manager.catalogue_state_hash();
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        let client_existed = sm.get_client(client_id).is_some();

        if !client_existed {
            sm.add_client(client_id);
        }
        sm.set_client_role(client_id, ClientRole::Peer);

        let queued_catalogue_replay = sm.queue_catalogue_sync_to_client_if_hash_mismatch(
            &self.storage,
            client_id,
            remote_catalogue_state_hash,
            &local_catalogue_state_hash,
        );
        if queued_catalogue_replay {
            self.immediate_tick();
        }
    }

    fn ensure_client_with_role_and_catalogue_state_hash(
        &mut self,
        client_id: ClientId,
        role: crate::sync_manager::ClientRole,
        session: Option<Session>,
        remote_catalogue_state_hash: Option<&str>,
    ) {
        let local_catalogue_state_hash = self.schema_manager.catalogue_state_hash();
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();

        if sm.get_client(client_id).is_none() {
            sm.add_client(client_id);
        }
        sm.set_client_role(client_id, role);
        if let Some(session) = session {
            sm.set_client_session(client_id, session);
        }

        let queued_catalogue_replay = sm.queue_catalogue_sync_to_client_if_hash_mismatch(
            &self.storage,
            client_id,
            remote_catalogue_state_hash,
            &local_catalogue_state_hash,
        );
        if queued_catalogue_replay {
            self.immediate_tick();
        }
    }

    /// Set a client's role.
    pub fn set_client_role_by_name(
        &mut self,
        client_id: ClientId,
        role: crate::sync_manager::ClientRole,
    ) {
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(client_id, role);
    }
}
