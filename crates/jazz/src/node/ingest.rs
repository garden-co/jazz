//! Commit, fate, and sync-message ingestion for a storage-backed
//! node. This module owns mutation paths that validate incoming transactions,
//! apply authority fates, park/unpark causally blocked units, and write node
//! state into groove; read-only global derivations live in [`super::global_state`],
//! policy evaluation in [`super::policy`], and byte-level record construction in
//! [`super::codec`]. It is the node layer's write side below the `Db` facade and
//! protocol sync loop. Trusted catalogue snapshot activation lives in the
//! sibling [`super::catalogue_ingest`] module.

use super::*;
use crate::protocol::{CatalogueAck, LensOp, SchemaLineagePublication, VersionBundleRef};
use crate::protocol_limits::{
    commit_unit_limit_violation, validate_known_state_declaration, validate_shape_ast_size,
};
use crate::schema::{ColumnSchema, MERGE_HEADS_TABLE};
use crate::tx::BranchLineage;
use groove::records::ValueType;

pub(super) const MAX_SCHEMA_LINEAGE_DECLARATIONS: usize = 4096;
pub(super) const MAX_SCHEMA_LINEAGE_NAME_BYTES: usize = 1024;
pub(super) const MAX_SCHEMA_LINEAGE_OPS: usize = 16_384;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CommitUnitParkMode {
    ingest_context: Option<CommitUnitIngestContext>,
    ingress_role: ParkedIngressRole,
}

impl Default for CommitUnitParkMode {
    fn default() -> Self {
        Self {
            ingest_context: None,
            ingress_role: ParkedIngressRole::Authority,
        }
    }
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Apply one sync message and return any outgoing sync messages.
    pub fn apply_sync_message(&mut self, message: SyncMessage) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.apply_sync_message_with_ingest_context(message, None)
    }

    /// Apply a catalogue mutation from the trusted local administrative lane.
    pub fn apply_trusted_catalogue_message(
        &mut self,
        message: SyncMessage,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.apply_sync_message_with_ingest_context(
            message,
            Some(CommitUnitIngestContext {
                identity: AuthorId::SYSTEM,
                trust: CommitUnitTrust::TrustedBackend,
                edge_authority: false,
            }),
        )
    }

    /// Apply one sync message from a connection-authenticated upload path.
    pub fn apply_sync_message_with_ingest_context(
        &mut self,
        message: SyncMessage,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        // A dynamic edge has exactly one admissible pre-ready transition: the
        // authenticated upstream invokes `apply_trusted_catalogue_snapshot`
        // directly.  Incremental catalogue/data/branch traffic has no
        // authority lineage to validate against and must not leave durable
        // pending rows that poison a later reopen.
        self.require_catalogue_ready()?;
        if self.catalogue_activation_failed {
            return Err(Error::CatalogueActivationFailed);
        }
        let message = message
            .expand_version_carriers_for_receive()
            .map_err(|_| Error::UnsupportedSyncMessage("malformed version-bundle run"))?;
        match message {
            SyncMessage::BranchMetadata(metadata) => {
                self.admit_branch_metadata(metadata)?;
                self.drain_parked_commit_units()
            }
            SyncMessage::FetchBranchMetadata { .. } => Err(Error::UnsupportedSyncMessage(
                "branch metadata repair must be served by peer state",
            )),
            SyncMessage::SessionClaims { identity, claims } => {
                if let Some(context) = ingest_context
                    && context.trust == CommitUnitTrust::TrustedBackend
                {
                    self.set_session_claims(identity, claims);
                }
                Ok(Vec::new())
            }
            SyncMessage::CommitUnit { tx, versions } => self.ingest_commit_unit_with_context(
                tx,
                versions,
                u64::MAX - SKEW_TOLERANCE_MS,
                ingest_context,
            ),
            SyncMessage::FateUpdate {
                tx_id,
                fate,
                global_seq,
                durability,
            } => {
                validate_received_fate_update_global_seq_durability(global_seq, durability)?;
                self.apply_fate_update(tx_id, fate, global_seq, durability)?;
                self.drain_parked_commit_units()
            }
            SyncMessage::ViewUpdate {
                subscription,
                settled_through,
                reset_result_set,
                version_carriers,
                version_bundles,
                peer_payload_inventory,
                result_member_adds,
                result_member_removes,
                terminal_operations,
                program_fact_adds,
                program_fact_removes,
            } => {
                self.apply_view_update(ViewUpdateParts {
                    subscription,
                    settled_through,
                    defer_settlement: false,
                    reset_result_set,
                    version_carriers,
                    version_bundles,
                    peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
                    authorization_progress: peer_payload_inventory.authorization_progress,
                    opening_pending: peer_payload_inventory.opening_pending,
                    result_member_adds,
                    result_member_removes,
                    terminal_operations,
                    program_fact_adds,
                    program_fact_removes,
                })?;
                Ok(Vec::new())
            }
            SyncMessage::RegisterShape {
                shape_id,
                ast,
                opts: _,
            } => {
                validate_shape_ast_size(&ast)
                    .map_err(|_| Error::UnsupportedSyncMessage("shape AST exceeds byte limit"))?;
                self.register_shape(shape_id, ast)?;
                Ok(Vec::new())
            }
            SyncMessage::FetchRowVersions { .. } => Err(Error::UnsupportedSyncMessage(
                "row-version repair fetch must be served by peer state",
            )),
            SyncMessage::RowVersionPayloads { .. } => Err(Error::UnsupportedSyncMessage(
                "row-version repair payload requires outstanding request context",
            )),
            SyncMessage::CatalogueSnapshot(_) => Err(Error::UnsupportedSyncMessage(
                "catalogue snapshot requires a trusted upstream link",
            )),
            SyncMessage::Subscribe(subscribe) => {
                validate_known_state_declaration(&subscribe.known_state).map_err(|_| {
                    Error::UnsupportedSyncMessage("known-state declaration exceeds limit")
                })?;
                self.apply_subscribe(subscribe)?;
                Ok(Vec::new())
            }
            SyncMessage::SubscribeRejected { .. } => Err(Error::UnsupportedSyncMessage(
                "subscription rejection requires subscription stream context",
            )),
            SyncMessage::Unsubscribe { subscription } => {
                self.apply_unsubscribe(subscription);
                Ok(Vec::new())
            }
            SyncMessage::PublishSchema { author, schema } => {
                self.apply_publish_schema(author, ingest_context, *schema)
            }
            SyncMessage::PublishSchemaWithLens {
                author,
                catalogue_seq,
                publication,
            } => self.apply_publish_schema_with_lens(
                author,
                ingest_context,
                catalogue_seq,
                *publication,
            ),
            SyncMessage::PublishLens { author, lens } => {
                self.apply_publish_lens(author, ingest_context, lens)
            }
            SyncMessage::SetCurrentWriteSchema { author, pointer } => {
                self.apply_set_current_write_schema(author, ingest_context, pointer)
            }
            SyncMessage::CatalogueAck(_) => Ok(Vec::new()),
            SyncMessage::PermissionAdviceRequest { .. }
            | SyncMessage::PermissionAdviceResponse { .. }
            | SyncMessage::AuthorizationScopeSubscribe { .. }
            | SyncMessage::AuthorizationScopeReceipt { .. }
            | SyncMessage::AuthorizationScopeIntent { .. }
            | SyncMessage::AuthorizationScopeView { .. }
            | SyncMessage::AuthorizationScopeAggregateReceipt { .. }
            | SyncMessage::AuthorizationScopeUnavailable { .. }
            | SyncMessage::AuthorizationScopeDecision { .. } => Err(Error::UnsupportedSyncMessage(
                "permission advice requires authenticated link context",
            )),
        }
    }

    fn apply_publish_schema(
        &mut self,
        author: AuthorId,
        ingest_context: Option<CommitUnitIngestContext>,
        schema: SchemaVersion,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_admin(author, ingest_context)?;
        if schema.id != schema.schema.version_id() {
            return Err(Error::InvalidCatalogueUpdate(
                "schema id does not match schema payload",
            ));
        }
        if !self.catalogue.catalogue_schemas.contains_key(&schema.id) {
            return Err(Error::InvalidCatalogueUpdate(
                "non-genesis schema requires lineage publication",
            ));
        }
        let active_schema_changed = schema.id == self.catalogue.current_write_schema.schema
            && self
                .catalogue
                .catalogue_schemas
                .get(&schema.id)
                .is_some_and(|current| current.schema != schema.schema);
        self.catalogue
            .catalogue_schemas
            .insert(schema.id, schema.clone());
        self.query.version_storage_sources_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
        if schema.id == self.catalogue.current_schema_version_id {
            self.catalogue.schema = schema.schema.clone();
        }
        self.persist_catalogue_schema(&schema)?;
        self.ensure_provisional_physical_mapping(schema.id)?;
        self.ensure_schema_version_alias(schema.id)?;
        self.synchronize_physical_version_tables()?;
        if active_schema_changed {
            // Policy declarations are intentionally outside the schema version
            // identity. Invalidate maintained handles when that same-version
            // payload changes so live subscriptions rebuild their authorization
            // graph without reopening storage through the old catalogue row.
            self.groove_runtime_token = next_groove_runtime_token();
        }
        let updates = self.drain_parked_commit_units()?;
        self.drain_parked_relay_commit_units()?;
        self.drain_parked_shape_registrations()?;
        let mut out = vec![SyncMessage::CatalogueAck(CatalogueAck {
            revision: None,
            schema: Some(schema.id),
            lens: None,
            applied: true,
        })];
        out.extend(updates);
        Ok(out)
    }

    fn apply_publish_schema_with_lens(
        &mut self,
        author: AuthorId,
        ingest_context: Option<CommitUnitIngestContext>,
        catalogue_seq: u64,
        publication: SchemaLineagePublication,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_admin(author, ingest_context)?;
        Self::validate_schema_lineage_publication(&publication)?;
        if catalogue_seq == 0 {
            return Err(Error::InvalidCatalogueUpdate(
                "schema lineage catalogue sequence must be nonzero",
            ));
        }
        let schema = &publication.schema;
        let lens = &publication.lens;
        if let Some(existing) = self.catalogue.active_lineages_by_target.get(&schema.id) {
            if existing.publication != publication || existing.catalogue_seq != catalogue_seq {
                return Err(Error::InvalidCatalogueUpdate(
                    "schema lineage publication conflicts with catalogue",
                ));
            }
            return Ok(vec![SyncMessage::CatalogueAck(CatalogueAck {
                revision: None,
                schema: Some(schema.id),
                lens: Some(lens.id),
                applied: true,
            })]);
        }

        if catalogue_seq <= self.catalogue.active_catalogue_seq {
            return Err(Error::InvalidCatalogueUpdate(
                "schema lineage catalogue sequence conflicts with active catalogue",
            ));
        }
        if let Some(existing) = self.catalogue.pending_lineages.get(&catalogue_seq) {
            if existing.publication != publication {
                return Err(Error::InvalidCatalogueUpdate(
                    "schema lineage catalogue sequence conflict",
                ));
            }
        } else {
            if let Some(source) = self.catalogue.catalogue_schemas.get(&lens.source) {
                Self::validate_migration_lens_between(lens, source, schema)?;
                Self::validate_lineage_table_partition(
                    &source.schema,
                    &schema.schema,
                    lens,
                    &publication.new_tables,
                    &publication.dropped_tables,
                )?;
            }
            if self
                .catalogue
                .pending_lineages
                .values()
                .any(|pending| pending.publication.schema.id == schema.id)
                || self
                    .catalogue
                    .staged_lineages
                    .values()
                    .any(|staged| staged.publication.schema.id == schema.id)
            {
                return Err(Error::InvalidCatalogueUpdate(
                    "schema lineage target is already reserved",
                ));
            }
            let pending = PendingSchemaLineage {
                catalogue_seq,
                publication,
            };
            self.persist_pending_schema_lineage(&pending)?;
            self.catalogue
                .pending_lineages
                .insert(catalogue_seq, pending);
        }
        self.drain_pending_schema_lineages()
    }

    pub(super) fn drain_pending_schema_lineages(&mut self) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        let mut out = Vec::new();
        loop {
            let next = self.catalogue.active_catalogue_seq.saturating_add(1);
            let Some(pending) = self.catalogue.pending_lineages.get(&next).cloned() else {
                break;
            };
            let publication = pending.publication;
            let Some(source) = self
                .catalogue
                .catalogue_schemas
                .get(&publication.lens.source)
                .cloned()
            else {
                break;
            };
            let validation = Self::validate_migration_lens_between(
                &publication.lens,
                &source,
                &publication.schema,
            )
            .and_then(|()| {
                Self::validate_lineage_table_partition(
                    &source.schema,
                    &publication.schema.schema,
                    &publication.lens,
                    &publication.new_tables,
                    &publication.dropped_tables,
                )
            });
            if validation.is_err() {
                self.remove_pending_schema_lineage(next, publication.id)?;
                break;
            }
            if self
                .catalogue
                .active_lineages_by_target
                .contains_key(&publication.schema.id)
            {
                return Err(Error::InvalidCatalogueUpdate(
                    "schema lineage target already has an active bundle",
                ));
            }
            let staged = if let Some(staged) = self.catalogue.staged_lineages.get(&next) {
                if staged.publication != publication {
                    return Err(Error::InvalidCatalogueUpdate(
                        "staged schema lineage conflicts with pending bundle",
                    ));
                }
                staged.clone()
            } else {
                let fresh = allocate_provisional_physical_mapping(
                    &publication.schema.schema,
                    &mut self.catalogue.next_physical_table_id,
                    &mut self.catalogue.next_physical_column_id,
                )?;
                let mapping = self.reconcile_physical_mapping_for_lens_payload(
                    &publication.lens,
                    &publication.schema,
                    &fresh,
                )?;
                let staged = StagedSchemaLineage {
                    catalogue_seq: next,
                    publication: publication.clone(),
                    alias: self.next_schema_version_alias()?,
                    mapping,
                };
                self.persist_catalogue_schema_lineage(&staged)?;
                self.catalogue.staged_lineages.insert(next, staged.clone());
                staged
            };

            // A new logical column widens the shared physical current-row
            // descriptor. Existing prepared/maintained graphs embed the old
            // fixed projection output, so they must be rebuilt after the
            // activation commits. A pure new variant over the same physical
            // columns remains safe to refresh in place.
            let widens_shared_current_descriptor = staged.mapping.tables.values().any(|target| {
                let existing_columns = self
                    .catalogue
                    .physical_mappings
                    .values()
                    .flat_map(|mapping| mapping.tables.values())
                    .filter(|existing| existing.table_id == target.table_id)
                    .flat_map(|existing| existing.columns.values().copied())
                    .collect::<BTreeSet<_>>();
                !existing_columns.is_empty()
                    && target
                        .columns
                        .values()
                        .any(|column| !existing_columns.contains(column))
            });

            #[cfg(test)]
            if self.catalogue_activation_failpoint
                == Some(CatalogueActivationFailpoint::AfterStaged)
            {
                self.catalogue_activation_failpoint = None;
                self.catalogue_activation_failed = true;
                return Err(Error::CatalogueActivationFailed);
            }

            self.install_staged_schema_lineage_in_memory(&staged);
            // A widened lineage adds new variant cases to existing physical
            // current tables. Install those cases in the live registry rather
            // than reopening the database: a reopen drops active history and
            // maintained-subscription receivers even when their output shape
            // remains compatible.
            if self.synchronize_physical_version_tables().is_err() {
                self.remove_staged_schema_lineage_from_memory(&staged);
                self.catalogue_activation_failed = true;
                return Err(Error::CatalogueActivationFailed);
            }
            // Closed raw receivers can otherwise make a cold server look live
            // until a later non-empty notification. Prune them explicitly;
            // then rebuild only when no observable subscription handle would
            // be disconnected. Live handles use the in-place cases above.
            if self.database.prune_dropped_subscriptions().is_err() {
                self.remove_staged_schema_lineage_from_memory(&staged);
                self.catalogue_activation_failed = true;
                return Err(Error::CatalogueActivationFailed);
            }
            let rebuild_cold_runtime = self.database.runtime_stats().active_subscriptions == 0;
            if rebuild_cold_runtime && self.rebuild_database_slot().is_err() {
                self.remove_staged_schema_lineage_from_memory(&staged);
                self.catalogue_activation_failed = true;
                return Err(Error::CatalogueActivationFailed);
            }
            if widens_shared_current_descriptor && !rebuild_cold_runtime {
                // Peer-serving and maintained caches are compiled against the
                // shared current-row descriptor too. Retire those handles now;
                // their owners rebuild from the new runtime token below. Raw
                // history subscriptions remain attached to the live Groove
                // registry and keep receiving compatible projected rows.
                self.invalidate_runtime_handles_after_database_rebuild();
            }
            #[cfg(test)]
            if self.catalogue_activation_failpoint
                == Some(CatalogueActivationFailpoint::AfterRegistration)
            {
                self.catalogue_activation_failpoint = None;
                self.remove_staged_schema_lineage_from_memory(&staged);
                self.catalogue_activation_failed = true;
                return Err(Error::CatalogueActivationFailed);
            }
            let mut batch = self.database.open_batch();
            Self::write_active_schema_lineage_to_batch(&mut batch, &staged)?;
            if self.database.commit_batch(batch).is_err() {
                self.remove_staged_schema_lineage_from_memory(&staged);
                self.catalogue_activation_failed = true;
                return Err(Error::CatalogueActivationFailed);
            }
            self.catalogue.staged_lineages.remove(&next);
            self.catalogue.pending_lineages.remove(&next);
            self.catalogue
                .active_lineages_by_target
                .insert(staged.publication.schema.id, staged.clone());
            self.catalogue.active_catalogue_seq = next;
            if widens_shared_current_descriptor {
                self.groove_runtime_token = next_groove_runtime_token();
            }
            out.push(SyncMessage::CatalogueAck(CatalogueAck {
                revision: Some(next),
                schema: Some(staged.publication.schema.id),
                lens: Some(staged.publication.lens.id),
                applied: true,
            }));
            out.extend(self.drain_parked_commit_units()?);
            self.drain_parked_relay_commit_units()?;
            self.drain_parked_shape_registrations()?;
            out.extend(self.drain_pending_catalogue_pointers()?);
        }
        Ok(out)
    }

    fn install_staged_schema_lineage_in_memory(&mut self, staged: &StagedSchemaLineage) {
        self.catalogue.catalogue_schemas.insert(
            staged.publication.schema.id,
            staged.publication.schema.clone(),
        );
        self.catalogue
            .catalogue_lenses
            .insert(staged.publication.lens.id, staged.publication.lens.clone());
        self.catalogue
            .schema_version_aliases
            .insert(staged.publication.schema.id, staged.alias);
        self.catalogue
            .physical_mappings
            .insert(staged.publication.schema.id, staged.mapping.clone());
        self.catalogue.lens_path_cache.clear();
        self.catalogue.compiled_lens_cache.clear();
        self.query.version_storage_sources_cache.clear();
        self.query.query_shape_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
    }

    fn remove_staged_schema_lineage_from_memory(&mut self, staged: &StagedSchemaLineage) {
        self.catalogue
            .catalogue_schemas
            .remove(&staged.publication.schema.id);
        self.catalogue
            .catalogue_lenses
            .remove(&staged.publication.lens.id);
        self.catalogue
            .schema_version_aliases
            .remove(&staged.publication.schema.id);
        self.catalogue
            .physical_mappings
            .remove(&staged.publication.schema.id);
        self.catalogue.lens_path_cache.clear();
        self.catalogue.compiled_lens_cache.clear();
        self.query.version_storage_sources_cache.clear();
        self.query.query_shape_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
    }

    fn apply_publish_lens(
        &mut self,
        author: AuthorId,
        ingest_context: Option<CommitUnitIngestContext>,
        lens: MigrationLens,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_admin(author, ingest_context)?;
        if lens.id != lens.content_id() {
            return Err(Error::InvalidCatalogueUpdate(
                "lens id does not match lens payload",
            ));
        }
        if !self.catalogue.catalogue_schemas.contains_key(&lens.source)
            || !self.catalogue.catalogue_schemas.contains_key(&lens.target)
        {
            return Err(Error::InvalidCatalogueUpdate("lens endpoint is unknown"));
        }
        self.validate_migration_lens(&lens)?;
        let installed = !self.catalogue.catalogue_lenses.contains_key(&lens.id);
        if installed {
            let candidate = self.reconcile_physical_mapping_for_lens(&lens)?;
            let authoritative = self.catalogue.physical_mappings.get(&lens.target).ok_or(
                Error::InvalidStoredValue("authoritative physical mapping missing"),
            )?;
            if candidate != *authoritative {
                return Err(Error::InvalidCatalogueUpdate(
                    "cross-lens conflicts with authoritative physical mapping",
                ));
            }
        }
        self.persist_catalogue_lens_with_physical_metadata(&lens, None)?;
        if installed {
            self.catalogue
                .catalogue_lenses
                .insert(lens.id, lens.clone());
        }
        self.catalogue.lens_path_cache.clear();
        self.catalogue.compiled_lens_cache.clear();
        self.query.version_storage_sources_cache.clear();
        self.query.query_shape_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
        // Both endpoint schemas are already Active and their agreeing physical
        // projection cases were registered during activation. A cross-lens adds
        // a catalogue path only; re-registering those cases is unnecessary and
        // Groove rejects it as a duplicate variant projection.
        Ok(vec![SyncMessage::CatalogueAck(CatalogueAck {
            revision: None,
            schema: None,
            lens: Some(lens.id),
            applied: true,
        })])
    }

    fn apply_set_current_write_schema(
        &mut self,
        author: AuthorId,
        ingest_context: Option<CommitUnitIngestContext>,
        pointer: CurrentWriteSchema,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_admin(author, ingest_context)?;
        if !self
            .catalogue
            .catalogue_schemas
            .contains_key(&pointer.schema)
        {
            self.persist_pending_catalogue_pointer(pointer)?;
            self.catalogue
                .pending_write_pointers
                .insert(pointer.revision, pointer);
            return Ok(Vec::new());
        }
        Ok(vec![self.apply_active_catalogue_pointer(pointer)?])
    }

    fn apply_active_catalogue_pointer(
        &mut self,
        pointer: CurrentWriteSchema,
    ) -> Result<SyncMessage, Error> {
        let applied = pointer.revision > self.catalogue.current_write_schema.revision;
        if applied {
            self.catalogue.current_write_schema = pointer;
            self.persist_catalogue_pointer(pointer)?;
            self.query.version_storage_sources_cache.clear();
            self.query.read_policy_authorization_request_cache.clear();
            self.query.policy_authorization_graph_cache.clear();
            let active_schema = self
                .catalogue
                .catalogue_schemas
                .get(&pointer.schema)
                .ok_or(Error::InvalidStoredValue(
                    "current write schema payload missing",
                ))?;
            if pointer.schema == self.catalogue.current_schema_version_id {
                self.catalogue.schema = active_schema.schema.clone();
            }
        }
        Ok(SyncMessage::CatalogueAck(CatalogueAck {
            revision: Some(pointer.revision),
            schema: Some(pointer.schema),
            lens: None,
            applied,
        }))
    }

    pub(super) fn drain_pending_catalogue_pointers(&mut self) -> Result<Vec<SyncMessage>, Error> {
        let ready = self
            .catalogue
            .pending_write_pointers
            .iter()
            .filter(|(_, pointer)| {
                self.catalogue
                    .catalogue_schemas
                    .contains_key(&pointer.schema)
            })
            .map(|(revision, pointer)| (*revision, *pointer))
            .collect::<Vec<_>>();
        let mut out = Vec::new();
        for (revision, pointer) in ready {
            out.push(self.apply_active_catalogue_pointer(pointer)?);
            self.catalogue.pending_write_pointers.remove(&revision);
        }
        Ok(out)
    }

    fn require_catalogue_admin(
        &self,
        _claimed_author: AuthorId,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<(), Error> {
        if matches!(
            ingest_context,
            Some(context)
                if context.identity == AuthorId::SYSTEM
                    && context.trust == CommitUnitTrust::TrustedBackend
        ) {
            Ok(())
        } else {
            Err(Error::UnauthorizedCatalogueUpdate)
        }
    }

    fn validate_migration_lens(&self, lens: &MigrationLens) -> Result<(), Error> {
        let source = self
            .catalogue
            .catalogue_schemas
            .get(&lens.source)
            .ok_or(Error::InvalidCatalogueUpdate("lens endpoint is unknown"))?;
        let target = self
            .catalogue
            .catalogue_schemas
            .get(&lens.target)
            .ok_or(Error::InvalidCatalogueUpdate("lens endpoint is unknown"))?;
        Self::validate_migration_lens_between(lens, source, target)
    }

    pub(super) fn validate_migration_lens_between(
        lens: &MigrationLens,
        source: &SchemaVersion,
        target: &SchemaVersion,
    ) -> Result<(), Error> {
        for table_lens in &lens.table_lenses {
            let source_table = source
                .schema
                .tables
                .iter()
                .find(|table| table.name == table_lens.source_table)
                .ok_or(Error::InvalidCatalogueUpdate("table lens is unknown"))?;
            let target_table = target
                .schema
                .tables
                .iter()
                .find(|table| table.name == table_lens.target_table)
                .ok_or(Error::InvalidCatalogueUpdate("table lens is unknown"))?;
            let mut columns = source_table
                .columns
                .iter()
                .cloned()
                .map(|column| (column.name.clone(), column))
                .collect::<BTreeMap<_, _>>();
            let mut saw_table_rename = source_table.name == target_table.name;
            for op in &table_lens.ops {
                match op {
                    LensOp::RenameTable { from, to } => {
                        if saw_table_rename
                            || from != &source_table.name
                            || to != &target_table.name
                        {
                            return Err(Error::InvalidCatalogueUpdate(
                                "table rename does not match lens endpoints",
                            ));
                        }
                        saw_table_rename = true;
                    }
                    LensOp::RenameColumn { from, to } => {
                        if columns.contains_key(to) {
                            return Err(Error::InvalidCatalogueUpdate(
                                "column rename collides with existing column",
                            ));
                        }
                        let mut column = columns.remove(from).ok_or(
                            Error::InvalidCatalogueUpdate("column rename source is unknown"),
                        )?;
                        column.name = to.clone();
                        columns.insert(to.clone(), column);
                    }
                    LensOp::CopyColumn { from, to } => {
                        if columns.contains_key(to) {
                            return Err(Error::InvalidCatalogueUpdate(
                                "column copy collides with existing column",
                            ));
                        }
                        let mut column =
                            columns
                                .get(from)
                                .cloned()
                                .ok_or(Error::InvalidCatalogueUpdate(
                                    "column copy source is unknown",
                                ))?;
                        column.name = to.clone();
                        columns.insert(to.clone(), column);
                    }
                    LensOp::AddColumn { column, .. } => {
                        if columns.contains_key(column) {
                            return Err(Error::InvalidCatalogueUpdate(
                                "added column already exists",
                            ));
                        }
                        let target_column = target_table
                            .columns
                            .iter()
                            .find(|candidate| candidate.name == *column)
                            .cloned()
                            .ok_or(Error::InvalidCatalogueUpdate(
                                "added column is absent from target",
                            ))?;
                        columns.insert(column.clone(), target_column);
                    }
                    LensOp::DropColumn { column, .. } => {
                        if columns.remove(column).is_none() {
                            return Err(Error::InvalidCatalogueUpdate(
                                "dropped column is absent from source",
                            ));
                        }
                    }
                    LensOp::TransformColumn { column, transform } => {
                        validate_transform_column(columns.get(column), transform)?;
                        let source_column =
                            columns.get(column).ok_or(Error::InvalidCatalogueUpdate(
                                "transformed column is absent from source",
                            ))?;
                        let target_column = target_table
                            .columns
                            .iter()
                            .find(|candidate| candidate.name == *column)
                            .ok_or(Error::InvalidCatalogueUpdate(
                                "transformed column is absent from target",
                            ))?;
                        if !physical_value_epoch_is_compatible(
                            &source_column.column_type,
                            &target_column.column_type,
                        ) {
                            return Err(Error::InvalidCatalogueUpdate(
                                "column transform changes variant registry non-additively",
                            ));
                        }
                        columns.insert(column.clone(), target_column.clone());
                    }
                    LensOp::RejectSourceDelta { .. } => {}
                }
            }
            if !saw_table_rename {
                return Err(Error::InvalidCatalogueUpdate(
                    "renamed table requires an explicit RenameTable operation",
                ));
            }
            let target_columns = target_table
                .columns
                .iter()
                .cloned()
                .map(|column| (column.name.clone(), column))
                .collect::<BTreeMap<_, _>>();
            if columns != target_columns {
                return Err(Error::InvalidCatalogueUpdate(
                    "lens operations do not reproduce target columns",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn validate_lineage_table_partition(
        source: &JazzSchema,
        target: &JazzSchema,
        lens: &MigrationLens,
        new_tables: &[String],
        dropped_tables: &[String],
    ) -> Result<(), Error> {
        let source_tables = source
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<BTreeSet<_>>();
        let target_tables = target
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<BTreeSet<_>>();
        let related_source = lens
            .table_lenses
            .iter()
            .map(|table| table.source_table.clone())
            .collect::<BTreeSet<_>>();
        let related_target = lens
            .table_lenses
            .iter()
            .map(|table| table.target_table.clone())
            .collect::<BTreeSet<_>>();
        let new = new_tables.iter().cloned().collect::<BTreeSet<_>>();
        let dropped = dropped_tables.iter().cloned().collect::<BTreeSet<_>>();
        if related_source.len() != lens.table_lenses.len()
            || related_target.len() != lens.table_lenses.len()
            || new.len() != new_tables.len()
            || dropped.len() != dropped_tables.len()
            || !related_source.is_disjoint(&dropped)
            || !related_target.is_disjoint(&new)
            || related_source
                .union(&dropped)
                .cloned()
                .collect::<BTreeSet<_>>()
                != source_tables
            || related_target.union(&new).cloned().collect::<BTreeSet<_>>() != target_tables
        {
            return Err(Error::InvalidCatalogueUpdate(
                "lineage table declarations do not partition schemas",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_schema_lineage_publication_bounds(
        publication: &SchemaLineagePublication,
    ) -> Result<(), Error> {
        let declaration_count = publication
            .lens
            .table_lenses
            .len()
            .saturating_add(publication.new_tables.len())
            .saturating_add(publication.dropped_tables.len());
        let operation_count = publication
            .lens
            .table_lenses
            .iter()
            .map(|table| table.ops.len())
            .sum::<usize>();
        let names_in_bounds = publication
            .new_tables
            .iter()
            .chain(&publication.dropped_tables)
            .chain(
                publication
                    .lens
                    .table_lenses
                    .iter()
                    .flat_map(|table| [&table.source_table, &table.target_table]),
            )
            .all(|name| !name.is_empty() && name.len() <= MAX_SCHEMA_LINEAGE_NAME_BYTES);
        if declaration_count > MAX_SCHEMA_LINEAGE_DECLARATIONS
            || operation_count > MAX_SCHEMA_LINEAGE_OPS
            || !names_in_bounds
        {
            return Err(Error::InvalidCatalogueUpdate(
                "schema lineage publication exceeds structural limits",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_schema_lineage_publication(
        publication: &SchemaLineagePublication,
    ) -> Result<(), Error> {
        Self::validate_schema_lineage_publication_bounds(publication)?;
        if publication.id != publication.content_id() {
            return Err(Error::InvalidCatalogueUpdate(
                "schema lineage publication id mismatch",
            ));
        }
        if publication.schema.id != publication.schema.schema.version_id() {
            return Err(Error::InvalidCatalogueUpdate(
                "schema id does not match schema payload",
            ));
        }
        if publication.lens.id != publication.lens.content_id() {
            return Err(Error::InvalidCatalogueUpdate(
                "lens id does not match lens payload",
            ));
        }
        if publication.lens.target != publication.schema.id {
            return Err(Error::InvalidCatalogueUpdate(
                "lineage lens target does not match schema",
            ));
        }
        Ok(())
    }

    /// Prepare physical branch storage only after bounded structural checks and
    /// catalogue dependencies are known to be satisfiable. Missing catalogue
    /// schemas are left for the ordinary parking path.
    fn prepare_branch_target_partitions_if_ready(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
    ) -> Result<(), Error>
    where
        S: ReopenableStorage,
    {
        let crate::tx::BranchLineage::Branch(branch_id) = tx.target_lineage else {
            return Ok(());
        };
        if !self.branches.branches.contains_key(&branch_id)
            || !commit_unit_write_count_matches(tx, versions.len())
            || versions.iter().any(|version| {
                !self
                    .catalogue
                    .catalogue_schemas
                    .contains_key(&version.schema_version())
            })
        {
            return Ok(());
        }
        self.ensure_branch_target_partitions(branch_id, versions)
    }

    /// Ingest a commit unit as fate authority.
    pub fn ingest_commit_unit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        self.ingest_commit_unit_with_context(tx, versions, now_ms, None)
    }

    /// Ingest a commit unit as fate authority with an optional authenticated
    /// connection identity. SPEC/7 §7.2 evaluates policy against the connection
    /// subject; `made_by` is provenance unless the link is an untrusted session.
    pub fn ingest_commit_unit_with_context(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        if let Some(reason) = commit_unit_limit_violation(&versions) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(reason));
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if commit_unit_write_count_matches(&tx, versions.len())
            && let Some(reason) = self.malformed_authored_version_reason(&versions)
        {
            return self.reject_malformed_commit(tx, reason);
        }
        self.prepare_branch_target_partitions_if_ready(&tx, &versions)?;
        let mut updates = self.ingest_commit_unit_once(tx, versions, now_ms, ingest_context)?;
        updates.extend(self.drain_parked_commit_units()?);
        Ok(updates)
    }

    /// Ingest a mergeable commit unit as an edge authority.
    ///
    /// This applies the same structural and write-policy checks as the normal
    /// authority path, but records only edge durability: no global sequence is
    /// allocated until core later finalizes the edge-accepted unit.
    pub fn ingest_edge_authority_mergeable_commit_unit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        if commit_unit_limit_violation(&versions).is_none()
            && commit_unit_write_count_matches(&tx, versions.len())
            && let Some(reason) = self.malformed_authored_version_reason(&versions)
        {
            return self.reject_malformed_commit(tx, reason);
        }
        if commit_unit_limit_violation(&versions).is_none()
            && commit_unit_write_count_matches(&tx, versions.len())
        {
            self.prepare_branch_target_partitions_if_ready(&tx, &versions)?;
        }
        let mut updates =
            self.ingest_edge_authority_mergeable_commit_unit_once(tx, versions, now_ms, None)?;
        updates.extend(self.drain_parked_commit_units()?);
        Ok(updates)
    }

    /// Ingest a mergeable commit unit as an edge authority using an
    /// authenticated permission subject while preserving `made_by` provenance.
    pub fn ingest_edge_authority_mergeable_commit_unit_with_identity(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        identity: AuthorId,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        if commit_unit_limit_violation(&versions).is_none()
            && commit_unit_write_count_matches(&tx, versions.len())
            && let Some(reason) = self.malformed_authored_version_reason(&versions)
        {
            return self.reject_malformed_commit(tx, reason);
        }
        if commit_unit_limit_violation(&versions).is_none()
            && commit_unit_write_count_matches(&tx, versions.len())
        {
            self.prepare_branch_target_partitions_if_ready(&tx, &versions)?;
        }
        let ingest_context = Some(CommitUnitIngestContext {
            identity,
            trust: CommitUnitTrust::TrustedBackend,
            edge_authority: true,
        });
        let mut updates = self.ingest_edge_authority_mergeable_commit_unit_once(
            tx,
            versions,
            now_ms,
            ingest_context,
        )?;
        updates.extend(self.drain_parked_commit_units()?);
        Ok(updates)
    }

    /// Finalize a locally-authored pending mergeable commit as the global
    /// authority: assign the next global sequence and mark it Accepted/Global.
    ///
    /// This is the authority's self-acceptance of its own write — the path a
    /// `Core` `Db` takes when it commits through the facade (a client instead
    /// commits Pending/Local and learns its fate from upstream). It reuses the
    /// stored versions and does not re-run the
    /// authority validation the node already performed when it authored the
    /// commit. Idempotent: a non-pending transaction is left untouched.
    pub fn finalize_local_mergeable_commit(&mut self, tx_id: TxId) -> Result<(), Error> {
        self.require_catalogue_ready()?;
        let stored = self
            .query_transaction(tx_id)?
            .ok_or(Error::MissingTransaction(tx_id))?;
        if stored.tx.kind != TxKind::Mergeable {
            return Err(Error::UnsupportedCommitUnit(
                "self-finalize is mergeable-only",
            ));
        }
        if !matches!(stored.fate, Fate::Pending) {
            return Ok(());
        }
        let records = self
            .query_versions_for_tx(tx_id)?
            .into_iter()
            .map(|stored| self.version_record_from_row(&stored))
            .collect::<Result<Vec<_>, Error>>()?;
        let permission_subject = self
            .open_tx
            .local_permission_subjects
            .get(&tx_id)
            .copied()
            .unwrap_or(stored.tx.made_by);
        if !self.commit_unit_satisfies_write_policies(
            &Transaction {
                permission_subject: Some(permission_subject),
                ..stored.tx.clone()
            },
            &records,
            None,
        )? {
            let fate = Fate::Rejected(RejectionReason::AuthorizationDenied);
            self.ingest_rejected_transaction(stored.tx, fate)?;
            return Ok(());
        }
        let global_seq = self.clock.allocate_global_seq()?;
        self.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(global_seq),
            Some(DurabilityTier::Global),
        )?;
        if stored.tx.target_lineage == crate::tx::BranchLineage::Root {
            self.create_merge_versions_for(&records)?;
        }
        Ok(())
    }

    /// Finalize a locally-authored pending exclusive commit as the global
    /// authority, returning the accepted or rejected fate.
    ///
    /// Validation runs against the in-memory commit unit (`tx` + `versions`),
    /// NOT a re-query of the stored transaction: the stored transaction record
    /// does not persist `base_snapshot` or the read sets (they travel only on
    /// the commit unit), so re-querying would drop the §3.7 read evidence and
    /// spuriously reject. This mirrors the foreign authority path, which
    /// validates the arriving commit unit before it is ingested.
    pub fn finalize_local_exclusive_commit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
    ) -> Result<Fate, Error> {
        self.require_catalogue_ready()?;
        let tx_id = tx.tx_id;
        if tx.kind != TxKind::Exclusive {
            return Err(Error::UnsupportedCommitUnit(
                "exclusive self-finalize requires an exclusive transaction",
            ));
        }
        let stored = self
            .query_transaction(tx_id)?
            .ok_or(Error::MissingTransaction(tx_id))?;
        if !matches!(stored.fate, Fate::Pending) {
            return Ok(stored.fate);
        }
        // Validate through the SAME authority path the core uses for an incoming
        // exclusive commit unit (§3.7): row/absent/predicate reads (INV-TX-16/17/18)
        // AND per-write first-committer-wins (INV-TX-20). Do not reimplement.
        if !self.validate_exclusive_commit_unit(&tx, &versions)? {
            let fate = Fate::Rejected(RejectionReason::ExclusiveConflict);
            self.ingest_rejected_transaction(tx, fate.clone())?;
            return Ok(fate);
        }
        let global_seq = self.clock.allocate_global_seq()?;
        self.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(global_seq),
            Some(DurabilityTier::Global),
        )?;
        if tx.target_lineage == crate::tx::BranchLineage::Root {
            self.create_merge_versions_for(&versions)?;
        }
        Ok(Fate::Accepted)
    }

    pub(super) fn finalize_edge_accepted_mergeable_commit_unit_once(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) -> Result<Vec<SyncMessage>, Error> {
        let versions = canonical_versions(versions);
        let mut memo = IngestMemo::default();
        if tx.kind != TxKind::Mergeable {
            return Err(Error::UnsupportedCommitUnit(
                "edge-accepted finalization is mergeable-only",
            ));
        }
        if let Some(reason) = commit_unit_limit_violation(&versions) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(reason));
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if !commit_unit_write_count_matches(&tx, versions.len()) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(
                "commit unit version count does not match transaction n_total_writes".to_owned(),
            ));
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if let Some(reason) = self.malformed_authored_version_reason(&versions) {
            return self.reject_malformed_commit(tx, reason);
        }
        if let Some(existing) = self.query_transaction(tx.tx_id)? {
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id)?
                .into_iter()
                .map(|stored| self.version_record_from_row(&stored))
                .collect::<Result<Vec<_>, Error>>()?;
            existing_versions.sort();
            if !known_transaction_payload_matches(&existing.tx, &tx)
                || existing_versions != versions
            {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            if matches!(existing.fate, Fate::Accepted)
                && existing.global_seq.is_some()
                && existing.durability >= DurabilityTier::Global
            {
                return Ok(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_seq: existing.global_seq,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]);
            }
            if matches!(existing.fate, Fate::Rejected(_)) {
                return Ok(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_seq: existing.global_seq,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]);
            }
        }
        if self.park_commit_unit_if_missing_branch_metadata_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingress_role: ParkedIngressRole::EdgeAccepted,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        if self.park_commit_unit_if_missing_schema_versions_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingress_role: ParkedIngressRole::EdgeAccepted,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        if self.park_commit_unit_if_missing_parents_with_mode(
            &tx,
            &versions,
            now_ms,
            &mut memo,
            CommitUnitParkMode {
                ingress_role: ParkedIngressRole::EdgeAccepted,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        if !self.commit_unit_satisfies_clock_condition(&tx, &versions, &mut memo)? {
            let fate = Fate::Rejected(RejectionReason::CausalityViolation);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if tx.tx_id.time.physical_ms() > now_ms.saturating_add(SKEW_TOLERANCE_MS) {
            let fate = Fate::Rejected(RejectionReason::ClientClockTooFarAhead);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if let Some(root) = self.cascade_root_for_versions(&versions) {
            let fate = Fate::Rejected(RejectionReason::Cascade { root });
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            return Ok(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }]);
        }
        let global_seq = self.clock.allocate_global_seq()?;
        let fate = Fate::Accepted;
        let durability = DurabilityTier::Global;
        let root_target = tx.target_lineage == crate::tx::BranchLineage::Root;
        let merge_rows = if root_target {
            self.merge_rows_for_versions(&versions)?
        } else {
            Vec::new()
        };
        self.ingest_known_transaction(
            tx.clone(),
            versions,
            fate.clone(),
            Some(global_seq),
            durability,
        )?;
        debug_assert_eq!(self.clock.applied_global_watermark, global_seq);
        if root_target {
            self.create_merge_versions_for_rows(merge_rows)?;
        }
        Ok(vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate,
            global_seq: Some(global_seq),
            durability: Some(durability),
        }])
    }

    /// Ingest an unfated commit unit at a Local relay without assigning fate.
    pub fn ingest_relay_commit_unit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
    ) -> Result<(), Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        if commit_unit_limit_violation(&versions).is_some()
            || !commit_unit_write_count_matches(&tx, versions.len())
        {
            return Err(Error::UnsupportedCommitUnit("malformed relay commit unit"));
        }
        if self.malformed_authored_version_reason(&versions).is_some() {
            return Err(Error::UnsupportedCommitUnit("malformed relay commit unit"));
        }
        self.prepare_branch_target_partitions_if_ready(&tx, &versions)?;
        self.ingest_relay_commit_unit_once(tx, versions)?;
        self.drain_parked_relay_commit_units()?;
        Ok(())
    }

    pub(super) fn ingest_relay_commit_unit_once(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
    ) -> Result<(), Error> {
        if tx.kind != TxKind::Mergeable && tx.kind != TxKind::Exclusive {
            return Err(Error::UnsupportedCommitUnit("unsupported commit unit kind"));
        }
        let versions = canonical_versions(versions);
        if let Some(existing) = self.query_transaction(tx.tx_id)? {
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id)?
                .into_iter()
                .map(|stored| self.version_record_from_row(&stored))
                .collect::<Result<Vec<_>, Error>>()?;
            existing_versions.sort();
            if !known_transaction_payload_matches(&existing.tx, &tx)
                || existing_versions != versions
            {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            return Ok(());
        }

        if !commit_unit_write_count_matches(&tx, versions.len()) {
            return Err(Error::UnsupportedCommitUnit(
                "commit unit version count does not match transaction n_total_writes",
            ));
        }
        let relay_mode = CommitUnitParkMode {
            ingress_role: ParkedIngressRole::Relay,
            ..CommitUnitParkMode::default()
        };
        if self.park_commit_unit_if_missing_branch_metadata_with_mode(
            &tx,
            &versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            relay_mode,
        )? {
            return Ok(());
        }
        if self.park_commit_unit_if_missing_schema_versions_with_mode(
            &tx,
            &versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            relay_mode,
        )? {
            return Ok(());
        }
        self.prepare_authored_schema_variants_for_commit(&versions)?;

        let mut memo = IngestMemo::default();
        if self.park_commit_unit_if_missing_parents_with_mode(
            &tx,
            &versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            &mut memo,
            relay_mode,
        )? {
            return Ok(());
        }
        self.ingest_transaction_and_versions(
            tx,
            versions,
            Fate::Pending,
            None,
            DurabilityTier::Local,
        )
    }

    pub(super) fn ingest_commit_unit_once(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<Vec<SyncMessage>, Error> {
        let versions = canonical_versions(versions);
        let mut memo = IngestMemo::default();
        if !commit_unit_write_count_matches(&tx, versions.len()) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(
                "commit unit version count does not match transaction n_total_writes".to_owned(),
            ));
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if let Some(reason) = self.malformed_authored_version_reason(&versions) {
            return self.reject_malformed_commit(tx, reason);
        }
        if let Some(existing) = self.query_transaction(tx.tx_id)? {
            if tx.kind == TxKind::Exclusive || matches!(existing.fate, Fate::Rejected(_)) {
                if !known_transaction_payload_matches(&existing.tx, &tx) {
                    return Err(Error::ConflictingCommitUnit(tx.tx_id));
                }
                return Ok(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_seq: existing.global_seq,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]);
            }
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id)?
                .into_iter()
                .map(|stored| self.version_record_from_row(&stored))
                .collect::<Result<Vec<_>, Error>>()?;
            existing_versions.sort();
            if !known_transaction_payload_matches(&existing.tx, &tx)
                || existing_versions != versions
            {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            if tx.kind == TxKind::Mergeable && matches!(existing.fate, Fate::Pending) {
                // Edge fate assignment can relay a mergeable unit as pending
                // before its permission scope settles, then re-enter authority
                // validation once that link-local subscription has hydrated.
            } else {
                return Ok(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_seq: existing.global_seq,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]);
            }
        }
        if self.park_commit_unit_if_missing_branch_metadata_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingest_context,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        if self.park_commit_unit_if_missing_schema_versions_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingest_context,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        self.prepare_authored_schema_variants_for_commit(&versions)?;
        if self.park_commit_unit_if_missing_parents_with_mode(
            &tx,
            &versions,
            now_ms,
            &mut memo,
            CommitUnitParkMode {
                ingest_context,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        if !self.commit_unit_satisfies_clock_condition(&tx, &versions, &mut memo)? {
            let fate = Fate::Rejected(RejectionReason::CausalityViolation);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if tx.tx_id.time.physical_ms() > now_ms.saturating_add(SKEW_TOLERANCE_MS) {
            let fate = Fate::Rejected(RejectionReason::ClientClockTooFarAhead);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }

        if let Some(root) = self.cascade_root_for_versions(&versions) {
            let fate = Fate::Rejected(RejectionReason::Cascade { root });
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            return Ok(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }]);
        }
        if !self.commit_unit_satisfies_write_policies(&tx, &versions, ingest_context)? {
            let fate = Fate::Rejected(RejectionReason::AuthorizationDenied);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if tx.kind == TxKind::Exclusive && !self.validate_exclusive_commit_unit(&tx, &versions)? {
            let fate = Fate::Rejected(RejectionReason::ExclusiveConflict);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            // This is a newly observed authority-side rejection. No stored
            // descendant can already point at it: descendants delivered before
            // the parent would park on the missing parent instead of entering
            // history. Later descendants will cascade when their parent state
            // is checked, so scanning all stored history here is redundant.
            return Ok(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }]);
        }
        if tx.kind != TxKind::Mergeable && tx.kind != TxKind::Exclusive {
            return Err(Error::UnsupportedCommitUnit("unsupported commit unit kind"));
        }
        let global_seq = self.clock.allocate_global_seq()?;
        let fate = Fate::Accepted;
        let durability = DurabilityTier::Global;
        let root_target = tx.target_lineage == crate::tx::BranchLineage::Root;
        let merge_rows = if root_target {
            self.merge_rows_for_versions(&versions)?
        } else {
            Vec::new()
        };
        self.ingest_known_transaction(
            tx.clone(),
            versions,
            fate.clone(),
            Some(global_seq),
            durability,
        )?;
        debug_assert_eq!(self.clock.applied_global_watermark, global_seq);
        if root_target {
            self.create_merge_versions_for_rows(merge_rows)?;
        }
        Ok(vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate,
            global_seq: Some(global_seq),
            durability: Some(durability),
        }])
    }

    pub(super) fn ingest_edge_authority_mergeable_commit_unit_once(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<Vec<SyncMessage>, Error> {
        let versions = canonical_versions(versions);
        let mut memo = IngestMemo::default();
        if tx.kind != TxKind::Mergeable {
            return Err(Error::UnsupportedCommitUnit(
                "edge authority only supports mergeable commit units",
            ));
        }
        if let Some(reason) = commit_unit_limit_violation(&versions) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(reason));
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if !commit_unit_write_count_matches(&tx, versions.len()) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(
                "commit unit version count does not match transaction n_total_writes".to_owned(),
            ));
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if let Some(reason) = self.malformed_authored_version_reason(&versions) {
            return self.reject_malformed_commit(tx, reason);
        }
        if let Some(existing) = self.query_transaction(tx.tx_id)? {
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id)?
                .into_iter()
                .map(|stored| self.version_record_from_row(&stored))
                .collect::<Result<Vec<_>, Error>>()?;
            existing_versions.sort();
            if !known_transaction_payload_matches(&existing.tx, &tx)
                || existing_versions != versions
            {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            if !matches!(existing.fate, Fate::Pending) {
                return Ok(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_seq: existing.global_seq,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]);
            }
        }
        if self.park_commit_unit_if_missing_branch_metadata_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingest_context,
                ingress_role: ParkedIngressRole::EdgeAuthority,
            },
        )? {
            return Ok(Vec::new());
        }
        if self.park_commit_unit_if_missing_schema_versions_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingest_context,
                ingress_role: ParkedIngressRole::EdgeAuthority,
            },
        )? {
            return Ok(Vec::new());
        }
        self.prepare_authored_schema_variants_for_commit(&versions)?;
        if self.park_commit_unit_if_missing_parents_with_mode(
            &tx,
            &versions,
            now_ms,
            &mut memo,
            CommitUnitParkMode {
                ingest_context,
                ingress_role: ParkedIngressRole::EdgeAuthority,
            },
        )? {
            return Ok(Vec::new());
        }
        if !self.commit_unit_satisfies_clock_condition(&tx, &versions, &mut memo)? {
            let fate = Fate::Rejected(RejectionReason::CausalityViolation);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if tx.tx_id.time.physical_ms() > now_ms.saturating_add(SKEW_TOLERANCE_MS) {
            let fate = Fate::Rejected(RejectionReason::ClientClockTooFarAhead);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if let Some(root) = self.cascade_root_for_versions(&versions) {
            let fate = Fate::Rejected(RejectionReason::Cascade { root });
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            return Ok(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }]);
        }
        if !self.commit_unit_satisfies_write_policies(&tx, &versions, ingest_context)? {
            let fate = Fate::Rejected(RejectionReason::AuthorizationDenied);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }

        let fate = Fate::Accepted;
        let durability = DurabilityTier::Edge;
        self.ingest_known_transaction(tx.clone(), versions, fate.clone(), None, durability)?;
        Ok(vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate,
            global_seq: None,
            durability: Some(durability),
        }])
    }

    pub(super) fn ingest_known_transaction(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: DurabilityTier,
    ) -> Result<(), Error> {
        self.require_catalogue_ready()?;
        debug_assert!(
            global_seq.is_none() || durability == DurabilityTier::Global,
            "a global sequence requires Global durability"
        );
        self.merge_tx_time(tx.tx_id.time);
        let versions = canonical_versions(versions);
        self.prepare_authored_schema_variants_for_commit(&versions)?;
        if let Some(existing) = self.query_transaction(tx.tx_id)? {
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id)?
                .into_iter()
                .map(|stored| self.version_record_from_row(&stored))
                .collect::<Result<Vec<_>, Error>>()?;
            existing_versions.sort();
            if !known_transaction_payload_matches(&existing.tx, &tx) {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            let mut version_bundles = Vec::new();
            for version in versions {
                match existing_versions.iter().find(|existing| {
                    view_version_key_for_ingest(existing) == view_version_key_for_ingest(&version)
                }) {
                    Some(existing) if existing != &version => {
                        return Err(Error::ConflictingCommitUnit(tx.tx_id));
                    }
                    Some(_) => {}
                    None => version_bundles.push(version),
                }
            }
            if version_bundles.is_empty() {
                self.apply_fate_update(tx.tx_id, fate, global_seq, Some(durability))?;
                return Ok(());
            }
            return self.ingest_transaction_and_versions(
                tx,
                version_bundles,
                fate,
                global_seq,
                durability,
            );
        }
        self.ingest_transaction_and_versions(tx, versions, fate, global_seq, durability)
    }

    pub(super) fn stage_known_transaction(
        &mut self,
        batch: &mut DatabaseBatch,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: DurabilityTier,
        staged_global_seqs: &mut Vec<GlobalSeq>,
    ) -> Result<(), Error> {
        debug_assert!(
            global_seq.is_none() || durability == DurabilityTier::Global,
            "a global sequence requires Global durability"
        );
        let versions = canonical_versions(versions);
        // This is entered by the batched ViewUpdate path, which has no
        // authority role and therefore cannot synthesize a fate. The caller
        // validates its entire frame before staging, and this is the central
        // storage-ingress backstop for other direct callers.
        self.validate_view_payload_versions(&versions)?;
        self.merge_tx_time(tx.tx_id.time);
        if self.query_transaction(tx.tx_id)?.is_some() {
            return self.ingest_known_transaction(tx, versions, fate, global_seq, durability);
        }
        self.stage_transaction_and_versions_with_current_indexes(
            batch,
            tx.clone(),
            versions,
            fate.clone(),
            global_seq,
            durability,
            true,
        )?;
        self.finalize_staged_transaction_ingest(
            batch,
            tx.tx_id,
            fate,
            global_seq,
            staged_global_seqs,
        )
    }

    pub(super) fn ingest_reset_view_bundle_refs_in_bulk(
        &mut self,
        bundles: &[VersionBundleRef<'_>],
    ) -> Result<BTreeSet<TxId>, Error> {
        let mut bundles_by_tx = BTreeMap::<TxId, Vec<VersionBundleRef<'_>>>::new();
        for bundle in bundles {
            validate_received_view_bundle_global_seq_durability(
                bundle.global_seq,
                bundle.durability,
            )?;
            bundles_by_tx
                .entry(bundle.tx.tx_id)
                .or_default()
                .push(*bundle);
        }
        let mut eligible = Vec::new();
        let mut loaded_tx_ids = BTreeSet::new();
        for (tx_id, tx_bundles) in bundles_by_tx {
            let first = tx_bundles[0];
            if tx_bundles.iter().any(|bundle| {
                bundle.tx != first.tx
                    || bundle.fate != first.fate
                    || bundle.global_seq != first.global_seq
                    || bundle.durability != first.durability
            }) {
                continue;
            }
            if *first.fate != Fate::Accepted {
                continue;
            }
            if first.global_seq.is_none() {
                continue;
            }
            if first.tx.kind != TxKind::Mergeable && first.tx.kind != TxKind::Exclusive {
                continue;
            }
            let mut unique_versions = BTreeMap::<
                (String, RowUuid, crate::ids::SchemaVersionId, bool),
                &VersionRecord,
            >::new();
            let mut duplicate_conflict = false;
            for bundle in &tx_bundles {
                for version in bundle.versions {
                    let key = (
                        version.table().to_owned(),
                        version.row_uuid(),
                        version.schema_version(),
                        version.deletion().is_some(),
                    );
                    match unique_versions.get(&key) {
                        Some(existing) if existing.record().raw() != version.record().raw() => {
                            duplicate_conflict = true;
                            break;
                        }
                        Some(_) => {}
                        None => {
                            unique_versions.insert(key, version);
                        }
                    }
                }
                if duplicate_conflict {
                    break;
                }
            }
            if duplicate_conflict {
                continue;
            }
            let version_count = unique_versions.len();
            if first.tx.kind == TxKind::Exclusive
                && usize::try_from(first.tx.n_total_writes).ok() != Some(version_count)
            {
                continue;
            }
            if self.query_transaction(tx_id)?.is_some() {
                continue;
            }
            let mut missing_refs = false;
            for bundle in &tx_bundles {
                if !self.missing_parent_refs(bundle.versions)?.is_empty() {
                    missing_refs = true;
                    break;
                }
            }
            if missing_refs {
                continue;
            }
            if loaded_tx_ids.insert(tx_id) {
                eligible.push(tx_bundles);
            }
        }
        if eligible.is_empty() {
            return Ok(loaded_tx_ids);
        }
        let eligible_versions = eligible
            .iter()
            .flat_map(|tx_bundles| tx_bundles.iter().flat_map(|bundle| bundle.versions))
            .cloned()
            .collect::<Vec<_>>();
        self.prepare_authored_schema_variants_for_commit(&eligible_versions)?;
        self.sync_metrics.receiver_bulk_ingest_commits += 1;
        self.sync_metrics.receiver_bulk_bundle_ingests += eligible.len() as u64;

        let mut batch = self.database.open_batch();
        let version_count = eligible
            .iter()
            .flatten()
            .map(|bundle| bundle.versions.len())
            .sum::<usize>();
        batch.reserve(eligible.len() + version_count.saturating_mul(2));
        let mut current_updates =
            BTreeMap::<(String, RowUuid, VersionLayer), (VersionRow, GlobalSeq)>::new();
        let mut content_versions = Vec::new();
        #[cfg(test)]
        let mut content_rows = BTreeSet::<(String, RowUuid)>::new();
        let mut applied_global_seqs = Vec::with_capacity(eligible.len());

        for tx_bundles in eligible {
            let first = tx_bundles[0];
            let tx = first.tx;
            let tx_node_alias = self.ensure_node_alias(tx.tx_id.node)?;
            let global_seq = first.global_seq.expect("checked above");
            applied_global_seqs.push(global_seq);
            batch.insert(
                "jazz_transactions",
                transaction_values(
                    tx_node_alias,
                    tx,
                    (*first.fate).clone(),
                    first.global_seq,
                    first.durability,
                ),
            );

            let mut unique_versions = BTreeMap::<
                (String, RowUuid, crate::ids::SchemaVersionId, bool),
                &VersionRecord,
            >::new();
            for bundle in &tx_bundles {
                for version in bundle.versions {
                    unique_versions
                        .entry((
                            version.table().to_owned(),
                            version.row_uuid(),
                            version.schema_version(),
                            version.deletion().is_some(),
                        ))
                        .or_insert(version);
                }
            }
            let mut versions = unique_versions.into_values().collect::<Vec<_>>();
            versions.sort();
            for version in versions {
                let author_schema = version.schema_version();
                let source_table_schema = self.table_in_schema(version.table(), author_schema)?;
                let schema_version_alias = self.ensure_schema_version_alias(author_schema)?;
                let stored = VersionRow::from_wire_with_schema_version(
                    &source_table_schema,
                    version,
                    tx_node_alias,
                    schema_version_alias,
                    tx.tx_id.time,
                    (author_schema != self.catalogue.current_schema_version_id)
                        .then_some(author_schema),
                )?;
                let (history_table, groove_record) = self.version_storage_write_binding(&stored)?;
                batch.insert_raw(
                    history_table.as_ref(),
                    self.version_storage_primary_key(&stored, BranchLineage::Root)?,
                    groove_record,
                );
                if stored.layer() == VersionLayer::Content {
                    content_versions.push(stored.clone());
                    #[cfg(test)]
                    content_rows.insert((stored.table().to_owned(), stored.row_uuid()));
                }

                let key = (stored.table().to_owned(), stored.row_uuid(), stored.layer());
                let existing_winner = current_updates.get(&key).map(|(previous, _)| {
                    (
                        previous,
                        self.version_tx_id(previous).expect("valid version tx id"),
                        previous.tx_time(),
                    )
                });
                if version_wins_over_open_winner(&stored, tx.tx_id, tx.tx_id.time, existing_winner)
                {
                    current_updates.insert(key, (stored, global_seq));
                }
            }
        }

        for (stored, global_seq) in current_updates.values() {
            self.write_global_current_update(&mut batch, stored, *global_seq)?;
        }
        self.write_merge_heads_for_bulk_content_versions(&mut batch, &content_versions)?;

        #[cfg(test)]
        let current_update_versions = current_updates
            .values()
            .map(|(stored, global_seq)| (stored.clone(), *global_seq))
            .collect::<Vec<_>>();
        self.database.commit_batch(batch)?;
        if let Some(tx_time) = loaded_tx_ids.iter().map(|tx_id| tx_id.time).max() {
            self.persist_storage_consistency_marker_through(tx_time)?;
        }
        #[cfg(test)]
        {
            if std::env::var_os("JAZZ_SKIP_BULK_INGEST_ASSERTS").is_none() {
                self.assert_merge_head_rows_match_history_for_test(&content_rows)?;
                self.assert_global_current_updates_match_history_for_test(
                    &current_update_versions,
                )?;
            }
        }
        for tx_id in &loaded_tx_ids {
            self.invalidate_tx_version_tables_cache(*tx_id);
        }
        for global_seq in applied_global_seqs {
            self.record_applied_global_seq(global_seq);
        }
        Ok(loaded_tx_ids)
    }

    /// Apply an upstream fate update.
    pub fn apply_fate_update(
        &mut self,
        tx_id: TxId,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: Option<DurabilityTier>,
    ) -> Result<(), Error> {
        self.require_catalogue_ready()?;
        debug_assert!(
            global_seq.is_none() || durability == Some(DurabilityTier::Global),
            "a global sequence requires Global durability"
        );
        let mut terminal_fate_persisted = false;
        let result = self.apply_fate_update_once(
            tx_id,
            fate,
            global_seq,
            durability,
            &mut terminal_fate_persisted,
        );
        if terminal_fate_persisted {
            self.open_tx.local_permission_subjects.remove(&tx_id);
        }
        result
    }

    fn apply_fate_update_once(
        &mut self,
        tx_id: TxId,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: Option<DurabilityTier>,
        terminal_fate_persisted: &mut bool,
    ) -> Result<(), Error> {
        let mut stored = self
            .query_transaction(tx_id)?
            .ok_or(Error::MissingTransaction(tx_id))?;
        if let (Some(current), Some(next)) = (stored.global_seq, global_seq)
            && next < current
        {
            return Err(Error::NonMonotoneState("global seq cannot move backwards"));
        }
        stored.fate = next_fate(&stored.fate, fate)?;
        stored.global_seq = global_seq.or(stored.global_seq);
        if let Some(durability) = durability {
            stored.durability = stored.durability.max(durability);
        }
        let advanced_global_seqs = if matches!(stored.fate, Fate::Accepted)
            && let Some(global_seq) = stored.global_seq
        {
            self.record_applied_global_seq(global_seq)
        } else {
            Vec::new()
        };

        let root_target = stored.tx.target_lineage == crate::tx::BranchLineage::Root;
        let mut batch = self.database.open_batch();
        let mut global_current_updates = Vec::new();
        let cleanup_rejected_versions = matches!(stored.fate, Fate::Rejected(_));
        let tx_versions = self.query_versions_for_tx(tx_id)?;
        let content_versions = tx_versions
            .iter()
            .filter(|version| version.layer() == VersionLayer::Content)
            .cloned()
            .collect::<Vec<_>>();
        if root_target && matches!(stored.fate, Fate::Accepted) && stored.global_seq.is_some() {
            global_current_updates =
                self.global_current_updates_for_versions(tx_id, &tx_versions)?;
        }
        if let Some(child_alias) = self.node_aliases.get(&tx_id.node).copied() {
            for raw in self.database.primary_key_scan_raw(
                "jazz_pending_edges",
                &[Value::U64(tx_id.time.0), Value::U64(child_alias.0)],
            )? {
                let record = raw.record();
                let parent_alias =
                    NodeAlias(record.get_u64(PendingEdgeRowRecord::FIELD_PARENT_NODE_ID_IDX)?);
                let parent = TxId::new(
                    TxTime(record.get_u64(PendingEdgeRowRecord::FIELD_PARENT_TIME_IDX)?),
                    self.node_for_alias(parent_alias)
                        .ok_or(Error::InvalidStoredValue(
                            "pending edge parent alias must exist",
                        ))?,
                );
                batch.delete(
                    "jazz_pending_edges",
                    pending_edge_primary_key(child_alias, tx_id, parent_alias, parent),
                );
            }
        }
        batch.update(
            "jazz_transactions",
            transaction_values(
                stored.node_alias,
                &stored.tx,
                stored.fate.clone(),
                stored.global_seq,
                stored.durability,
            ),
        );
        if root_target && !matches!(stored.fate, Fate::Rejected(_)) {
            for version in &content_versions {
                self.update_merge_heads_for_content_version(&mut batch, version)?;
            }
        }
        if let Some(global_seq) = stored.global_seq {
            for version in &global_current_updates {
                self.write_global_current_update(&mut batch, version, global_seq)?;
            }
        }
        #[cfg(test)]
        let global_current_update_versions = stored
            .global_seq
            .map(|global_seq| {
                global_current_updates
                    .iter()
                    .cloned()
                    .map(|version| (version, global_seq))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if root_target && (matches!(stored.fate, Fate::Rejected(_)) || stored.global_seq.is_some())
        {
            self.cleanup_fated_ahead_current_for_versions(&mut batch, &tx_versions)?;
        }
        for global_seq in advanced_global_seqs
            .iter()
            .copied()
            .filter(|global_seq| Some(*global_seq) != stored.global_seq)
        {
            self.prune_ahead_current_for_global_seq(&mut batch, global_seq)?;
        }
        let rejected_payload = if root_target && cleanup_rejected_versions {
            self.remove_rejected_local_versions(tx_id, &stored, &mut batch)?
        } else {
            None
        };
        self.database.commit_batch(batch)?;
        *terminal_fate_persisted = !matches!(stored.fate, Fate::Pending);
        if matches!(stored.fate, Fate::Rejected(_)) || stored.global_seq.is_some() {
            self.persist_storage_consistency_marker_through(tx_id.time)?;
        }
        #[cfg(test)]
        {
            if root_target {
                let rows = content_versions
                    .iter()
                    .map(|version| (version.table().to_owned(), version.row_uuid()))
                    .collect::<BTreeSet<_>>();
                self.assert_merge_head_rows_match_history_for_test(&rows)?;
                self.assert_global_current_updates_match_history_for_test(
                    &global_current_update_versions,
                )?;
            }
        }
        if let Some(rejected_payload) = rejected_payload {
            let tx_id = rejected_payload.tx_id();
            self.rejections
                .rejected_transactions
                .insert(tx_id, rejected_payload);
        }
        let accepted_final = matches!(stored.fate, Fate::Accepted);
        let rejected_root = rejected_root_for(&stored.fate, tx_id);
        if accepted_final {
            self.rejections.child_txs_by_parent.remove(&tx_id);
            self.prune_child_edges(tx_id);
        } else if let Some(root) = rejected_root {
            self.prune_child_edges(tx_id);
            let cascades = self.local_cascade_descendants(tx_id, root)?;
            for descendant in cascades {
                // Authority-side parking resolves parents before children, so
                // a locally cascaded descendant should still be speculative.
                let descendant_fate = self.query_transaction(descendant)?.map(|tx| tx.fate);
                debug_assert!(
                    matches!(descendant_fate.as_ref(), Some(Fate::Pending))
                        || matches!(
                            descendant_fate.as_ref(),
                            Some(Fate::Rejected(RejectionReason::Cascade { root: existing }))
                                if *existing == root
                        )
                );
                self.apply_fate_update(
                    descendant,
                    Fate::Rejected(RejectionReason::Cascade { root }),
                    None,
                    None,
                )?;
            }
        }
        Ok(())
    }

    /// Return locally visible current cells for one row.
    pub(super) fn validate_exclusive_commit_unit(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
    ) -> Result<bool, Error> {
        let Some(base_snapshot) = &tx.base_snapshot else {
            return Ok(false);
        };
        let mut visible_content_memo = BTreeMap::<(String, RowUuid), Option<TxId>>::new();
        for read in tx.row_read_set.as_deref().unwrap_or(&[]) {
            let current = self.visible_global_content_tx_id_now_memoized(
                &read.table,
                read.row_uuid,
                &mut visible_content_memo,
            );
            if current != Some(read.version) {
                return Ok(false);
            }
        }
        for absent in tx.absent_read_set.as_deref().unwrap_or(&[]) {
            let current = self.visible_global_content_tx_id_now_memoized(
                &absent.table,
                absent.row_uuid,
                &mut visible_content_memo,
            );
            if current.is_some() {
                return Ok(false);
            }
        }
        for predicate in tx.predicate_read_set.as_deref().unwrap_or(&[]) {
            if self.predicate_read_is_degenerate_whole_table(predicate)? {
                if self
                    .global_currency_changed_after(&predicate.table, base_snapshot.global_base)?
                {
                    return Ok(false);
                }
            } else if self.shape_predicate_changed_after(predicate, base_snapshot.global_base)? {
                return Ok(false);
            }
        }
        for version in versions {
            self.table_in_schema(version.table(), version.schema_version())?;
            let current = self.visible_global_content_tx_id_now_memoized(
                version.table(),
                version.row_uuid(),
                &mut visible_content_memo,
            );
            let parents = version.parents();
            let parent = match parents.as_slice() {
                [] => None,
                [parent] => Some(*parent),
                _ => return Ok(false),
            };
            if current != parent {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn visible_global_content_tx_id_now_memoized(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        memo: &mut BTreeMap<(String, RowUuid), Option<TxId>>,
    ) -> Option<TxId> {
        if let Some(current) = memo.get(&(table.to_owned(), row_uuid)) {
            return *current;
        }
        let current = self.visible_global_content_tx_id_now(table, row_uuid);
        memo.insert((table.to_owned(), row_uuid), current);
        current
    }

    pub(super) fn predicate_read_is_degenerate_whole_table(
        &self,
        predicate: &PredicateRead,
    ) -> Result<bool, Error> {
        let shape = crate::query::Query::from(&predicate.table).validate(&self.catalogue.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        Ok(predicate.shape_id == shape.shape_id() && predicate.binding_id == binding.binding_id())
    }

    pub(super) fn shape_predicate_changed_after(
        &mut self,
        predicate: &PredicateRead,
        global_base: GlobalSeq,
    ) -> Result<bool, Error> {
        let shape = predicate.shape.validate(&self.catalogue.schema)?;
        if shape.shape_id() != predicate.shape_id {
            return Ok(true);
        }
        let binding = shape.bind(predicate.binding_values.clone())?;
        if binding.binding_id() != predicate.binding_id {
            return Ok(true);
        }
        let at_base = self.shape_output_tx_set_at_global_base(&shape, &binding, global_base)?;
        let at_now = self.shape_output_tx_set_now(&shape, &binding)?;
        Ok(at_base != at_now)
    }

    fn shape_output_tx_set_now(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<BTreeSet<(RowUuid, TxId)>, Error> {
        let table = shape.query().table.clone();
        let mut set = BTreeSet::new();
        for row in self.query_rows(shape, binding, DurabilityTier::Global)? {
            if let Some(tx_id) = self.visible_global_content_tx_id_now(&table, row.row_uuid()) {
                set.insert((row.row_uuid(), tx_id));
            }
        }
        Ok(set)
    }

    fn shape_output_tx_set_at_global_base(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        global_base: GlobalSeq,
    ) -> Result<BTreeSet<(RowUuid, TxId)>, Error> {
        let table = shape.query().table.clone();
        let rows = self.query_rows_at(shape, binding, global_base)?;
        rows.into_iter()
            .map(|row| {
                let row_uuid = row.row_uuid();
                let Some(tx_id) =
                    self.visible_global_content_tx_id_at(&table, row_uuid, global_base)?
                else {
                    return Err(Error::InvalidStoredValue(
                        "historical query output row must have visible content",
                    ));
                };
                Ok((row_uuid, tx_id))
            })
            .collect()
    }

    pub(super) fn commit_unit_satisfies_write_policies(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<bool, Error> {
        let permission_subject = match ingest_context {
            Some(context) => {
                if context.trust == CommitUnitTrust::Session && tx.made_by != context.identity {
                    return Ok(false);
                }
                match context.trust {
                    CommitUnitTrust::Session => context.identity,
                    CommitUnitTrust::TrustedBackend => tx.permission_subject.unwrap_or(tx.made_by),
                }
            }
            None => tx.permission_subject.unwrap_or(tx.made_by),
        };
        if let crate::tx::BranchLineage::Branch(branch_id) = tx.target_lineage {
            let branch = self
                .branches
                .branches
                .get(&branch_id)
                .cloned()
                .ok_or(Error::BranchNotFound(branch_id))?;
            if branch.state != codec::BranchState::Open {
                return Ok(false);
            }
            if !self.branch_write_policy_allows(branch_id, permission_subject)? {
                return Ok(false);
            }
            for version in versions {
                let table = self.table_in_schema(version.table(), version.schema_version())?;
                if !self.branch_table_write_policy_allows_version_record(
                    &branch,
                    &table,
                    version,
                    permission_subject,
                )? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        for version in versions {
            if !self.version_satisfies_write_policy(version, permission_subject)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn version_satisfies_write_policy(
        &mut self,
        version: &VersionRecord,
        author: AuthorId,
    ) -> Result<bool, Error> {
        self.write_policy_allows_version_record(version, author)
    }

    pub(super) fn cascade_root_for_versions(&mut self, versions: &[VersionRecord]) -> Option<TxId> {
        for parent in versions.iter().flat_map(|version| version.parents()) {
            if let Some(root) = self.cascade_root_for_tx(parent) {
                return Some(root);
            }
        }
        None
    }

    pub(super) fn park_commit_unit_if_missing_parents_with_mode(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        now_ms: u64,
        memo: &mut IngestMemo,
        mode: CommitUnitParkMode,
    ) -> Result<bool, Error> {
        if self.missing_parent_refs_memo(versions, memo)?.is_empty() {
            return Ok(false);
        }
        if let Some(existing) = self.parking.parked_commit_units.get_mut(&tx.tx_id) {
            if existing.tx != *tx || existing.versions != versions {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            if existing.ingest_context != mode.ingest_context {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            existing.ingress_role = existing.ingress_role.strongest(mode.ingress_role);
            return Ok(true);
        }
        self.sync_metrics.parked_orphans += 1;
        self.parking.parked_commit_units.insert(
            tx.tx_id,
            ParkedCommitUnit {
                tx: tx.clone(),
                versions: versions.to_vec(),
                now_ms,
                ingest_context: mode.ingest_context,
                ingress_role: mode.ingress_role,
            },
        );
        Ok(true)
    }

    pub(super) fn park_commit_unit_if_missing_schema_versions_with_mode(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        now_ms: u64,
        mode: CommitUnitParkMode,
    ) -> Result<bool, Error> {
        if versions.iter().all(|version| {
            self.catalogue
                .catalogue_schemas
                .contains_key(&version.schema_version())
        }) {
            return Ok(false);
        }
        if let Some(existing) = self.parking.parked_commit_units.get_mut(&tx.tx_id) {
            if existing.tx != *tx || existing.versions != versions {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            if existing.ingest_context != mode.ingest_context {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            existing.ingress_role = existing.ingress_role.strongest(mode.ingress_role);
            return Ok(true);
        }
        self.sync_metrics.parked_orphans += 1;
        self.sync_metrics.parked_catalogue_orphans += 1;
        self.parking.parked_catalogue_commit_units.insert(tx.tx_id);
        self.parking.parked_commit_units.insert(
            tx.tx_id,
            ParkedCommitUnit {
                tx: tx.clone(),
                versions: versions.to_vec(),
                now_ms,
                ingest_context: mode.ingest_context,
                ingress_role: mode.ingress_role,
            },
        );
        Ok(true)
    }

    /// Park a branch-targeted unit until the authenticated routing record has
    /// arrived.  Branch metadata is a transport prerequisite, not a synthetic
    /// transaction parent, so this deliberately shares the ordinary bounded
    /// orphan queue and its idempotence/conflict checks.
    pub(super) fn park_commit_unit_if_missing_branch_metadata_with_mode(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        now_ms: u64,
        mode: CommitUnitParkMode,
    ) -> Result<bool, Error> {
        let crate::tx::BranchLineage::Branch(branch_id) = tx.target_lineage else {
            return Ok(false);
        };
        if self.branches.branches.contains_key(&branch_id) {
            return Ok(false);
        }
        if let Some(existing) = self.parking.parked_commit_units.get_mut(&tx.tx_id) {
            if existing.tx != *tx
                || existing.versions != versions
                || existing.ingest_context != mode.ingest_context
            {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            existing.ingress_role = existing.ingress_role.strongest(mode.ingress_role);
            return Ok(true);
        }
        self.sync_metrics.parked_orphans += 1;
        self.parking.parked_commit_units.insert(
            tx.tx_id,
            ParkedCommitUnit {
                tx: tx.clone(),
                versions: versions.to_vec(),
                now_ms,
                ingest_context: mode.ingest_context,
                ingress_role: mode.ingress_role,
            },
        );
        Ok(true)
    }

    pub(super) fn missing_parent_refs(
        &mut self,
        versions: &[VersionRecord],
    ) -> Result<BTreeSet<TxId>, Error> {
        let mut memo = IngestMemo::default();
        self.missing_parent_refs_memo(versions, &mut memo)
    }

    pub(super) fn missing_parent_refs_memo(
        &mut self,
        versions: &[VersionRecord],
        memo: &mut IngestMemo,
    ) -> Result<BTreeSet<TxId>, Error> {
        let mut missing = BTreeSet::new();
        for parent in versions.iter().flat_map(|version| version.parents()) {
            if !self.transaction_exists_memo(parent, memo)? {
                missing.insert(parent);
            }
        }
        Ok(missing)
    }

    pub(super) fn commit_unit_satisfies_clock_condition(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        memo: &mut IngestMemo,
    ) -> Result<bool, Error> {
        for version in versions {
            for parent in version.parents() {
                let Some(parent_made_at) = self.transaction_made_at_memo(parent, memo)? else {
                    return Ok(false);
                };
                if tx.tx_id.time <= parent_made_at {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    pub(super) fn drain_parked_commit_units(&mut self) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        let mut updates = Vec::new();
        loop {
            let parked = self
                .parking
                .parked_commit_units
                .iter()
                .filter(|(_, unit)| unit.ingress_role != ParkedIngressRole::Relay)
                .map(|(tx_id, unit)| (*tx_id, unit.versions.clone()))
                .collect::<Vec<_>>();
            let mut ready = Vec::new();
            for (tx_id, versions) in parked {
                if versions.iter().all(|version| {
                    self.catalogue
                        .catalogue_schemas
                        .contains_key(&version.schema_version())
                }) && branch_metadata_available(
                    self,
                    &self.parking.parked_commit_units[&tx_id].tx,
                ) && self.missing_parent_refs(&versions)?.is_empty()
                {
                    ready.push(tx_id);
                }
            }
            if ready.is_empty() {
                break;
            }
            for tx_id in ready {
                if let Some(unit) = self.parking.parked_commit_units.get(&tx_id).cloned() {
                    self.prepare_branch_target_partitions_if_ready(&unit.tx, &unit.versions)?;
                }
                let Some(unit) = self.parking.parked_commit_units.remove(&tx_id) else {
                    continue;
                };
                self.sync_metrics.parked_orphans_resolved += 1;
                if self.parking.parked_catalogue_commit_units.remove(&tx_id) {
                    self.sync_metrics.parked_catalogue_orphans_resolved += 1;
                }
                if unit.ingress_role == ParkedIngressRole::EdgeAccepted {
                    updates.extend(self.finalize_edge_accepted_mergeable_commit_unit_once(
                        unit.tx,
                        unit.versions,
                        unit.now_ms,
                    )?);
                } else if unit.ingress_role == ParkedIngressRole::EdgeAuthority {
                    updates.extend(self.ingest_edge_authority_mergeable_commit_unit_once(
                        unit.tx,
                        unit.versions,
                        unit.now_ms,
                        unit.ingest_context,
                    )?);
                } else {
                    updates.extend(self.ingest_commit_unit_once(
                        unit.tx,
                        unit.versions,
                        unit.now_ms,
                        unit.ingest_context,
                    )?);
                }
            }
        }
        Ok(updates)
    }

    pub(super) fn drain_parked_relay_commit_units(&mut self) -> Result<(), Error>
    where
        S: ReopenableStorage,
    {
        loop {
            let parked = self
                .parking
                .parked_commit_units
                .iter()
                .filter(|(_, unit)| unit.ingress_role == ParkedIngressRole::Relay)
                .map(|(tx_id, unit)| (*tx_id, unit.versions.clone()))
                .collect::<Vec<_>>();
            let mut ready = Vec::new();
            for (tx_id, versions) in parked {
                if versions.iter().all(|version| {
                    self.catalogue
                        .catalogue_schemas
                        .contains_key(&version.schema_version())
                }) && branch_metadata_available(
                    self,
                    &self.parking.parked_commit_units[&tx_id].tx,
                ) && self.missing_parent_refs(&versions)?.is_empty()
                {
                    ready.push(tx_id);
                }
            }
            if ready.is_empty() {
                break;
            }
            for tx_id in ready {
                let Some(unit) = self.parking.parked_commit_units.remove(&tx_id) else {
                    continue;
                };
                // A relay has no fate authority. Once its deferred schema is
                // known, an incomplete row record has a terminal local
                // disposition: discard it without writing a synthetic rejected
                // transaction or failing the catalogue publication that made
                // the violation observable.
                if self
                    .malformed_authored_version_reason(&unit.versions)
                    .is_some()
                {
                    self.parking.parked_catalogue_commit_units.remove(&tx_id);
                    self.sync_metrics.dropped_malformed_relay_commit_units += 1;
                    continue;
                }
                self.prepare_branch_target_partitions_if_ready(&unit.tx, &unit.versions)?;
                self.sync_metrics.parked_orphans_resolved += 1;
                if self.parking.parked_catalogue_commit_units.remove(&tx_id) {
                    self.sync_metrics.parked_catalogue_orphans_resolved += 1;
                }
                self.ingest_relay_commit_unit_once(unit.tx, unit.versions)?;
            }
        }
        Ok(())
    }

    pub(super) fn cascade_root_for_tx(&mut self, tx_id: TxId) -> Option<TxId> {
        let mut stack = vec![tx_id];
        let mut seen = BTreeSet::new();
        while let Some(current) = stack.pop() {
            if !seen.insert(current) {
                continue;
            }
            if let Ok(Some(tx)) = self.query_transaction(current)
                && let Some(root) = rejected_root_for(&tx.fate, current)
            {
                return Some(root);
            }
            if let Ok(Some(tx)) = self.query_transaction(current)
                && matches!(tx.fate, Fate::Accepted)
            {
                continue;
            }
            let Ok(versions) = self.query_versions_for_tx(current) else {
                return None;
            };
            stack.extend(versions.iter().flat_map(|version| version.parents()));
        }
        None
    }

    pub(super) fn cascade_rejections_from(
        &mut self,
        rejected: TxId,
    ) -> Result<Vec<SyncMessage>, Error> {
        let Some(root) = self.cascade_root_for_tx(rejected).or(Some(rejected)) else {
            return Ok(Vec::new());
        };
        let descendants = self.local_cascade_descendants(rejected, root)?;
        let mut updates = Vec::new();
        for descendant in descendants {
            let fate = Fate::Rejected(RejectionReason::Cascade { root });
            self.apply_fate_update(descendant, fate.clone(), None, None)?;
            updates.push(SyncMessage::FateUpdate {
                tx_id: descendant,
                fate,
                global_seq: None,
                durability: None,
            });
        }
        Ok(updates)
    }

    #[cfg(test)]
    pub(crate) fn transaction_ids(&self) -> Result<Vec<TxId>, Error> {
        let mut tx_ids = Vec::new();
        for raw in self
            .database
            .primary_key_scan_raw("jazz_transactions", &[])?
        {
            let record = raw.record();
            let time = TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?);
            let alias = NodeAlias(record.get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)?);
            let node = self.node_for_alias(alias).ok_or(Error::InvalidStoredValue(
                "transaction node alias must exist",
            ))?;
            tx_ids.push(TxId::new(time, node));
        }
        tx_ids.sort();
        tx_ids.dedup();
        Ok(tx_ids)
    }

    pub(super) fn local_cascade_descendants(
        &mut self,
        rejected: TxId,
        root: TxId,
    ) -> Result<Vec<TxId>, Error> {
        let mut descendants = BTreeSet::new();
        let mut stack = self
            .rejections
            .child_txs_by_parent
            .remove(&rejected)
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        let mut seen = BTreeSet::new();
        while let Some(tx_id) = stack.pop() {
            if !seen.insert(tx_id) {
                continue;
            }
            let Some(tx) = self.query_transaction(tx_id)? else {
                continue;
            };
            let eligible = !matches!(tx.fate, Fate::Rejected(_))
                || matches!(
                    tx.fate,
                    Fate::Rejected(RejectionReason::Cascade { root: existing }) if existing == root
                );
            if eligible {
                descendants.insert(tx_id);
                if let Some(children) = self.rejections.child_txs_by_parent.get(&tx_id) {
                    stack.extend(children.iter().copied());
                }
            }
        }
        Ok(descendants.into_iter().collect())
    }

    pub(super) fn remove_rejected_local_versions(
        &mut self,
        tx_id: TxId,
        tx: &StoredTransaction,
        batch: &mut DatabaseBatch,
    ) -> Result<Option<RejectedTransaction>, Error> {
        let rejected = self.query_versions_for_tx(tx_id)?;
        if rejected.is_empty() {
            return Ok(None);
        }
        let affected = rejected
            .iter()
            .map(|version| (version.table, version.row_uuid(), version.layer()))
            .collect::<BTreeSet<_>>();
        let affected_content_rows = rejected
            .iter()
            .filter(|version| version.layer() == VersionLayer::Content)
            .map(|version| {
                Ok((
                    self.physical_table_id_for_version(version)?,
                    version.table().to_owned(),
                    version.row_uuid(),
                ))
            })
            .collect::<Result<BTreeSet<_>, Error>>()?;
        let mut rejected_payload = None;
        if tx_id.node == self.node_uuid
            && let Fate::Rejected(reason) = &tx.fate
        {
            let rejected_tx_values =
                rejected_transaction_values(tx.node_alias, &tx.tx, reason.clone());
            batch.insert("jazz_rejected_transactions", rejected_tx_values.clone());
            let rejected_tx_table = self
                .catalogue
                .schema
                .storage_tables()
                .into_iter()
                .find(|table| table.name == "jazz_rejected_transactions")
                .ok_or(Error::InvalidStoredValue(
                    "missing rejected transaction table",
                ))?;
            let rejected_tx_record =
                owned_record_from_storage_values(&rejected_tx_table, rejected_tx_values)?;
            let mut rejected_versions = Vec::new();
            for version in &rejected {
                let schema_version = self
                    .schema_version_for_alias(version.schema_version_alias())
                    .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
                let table_schema = self.table_in_schema(version.table(), schema_version)?;
                let rejected_version_table = table_schema.rejected_versions_storage_table();
                let rejected_version_values = rejected_version_values(&table_schema, version)?;
                let rejected_version_record = owned_record_from_storage_values(
                    &rejected_version_table,
                    rejected_version_values,
                )?;
                let (storage_table, storage_record) =
                    self.rejected_version_storage_write_binding(version, &rejected_version_record)?;
                batch.insert(storage_table.as_ref(), storage_record);
                rejected_versions.push(RejectedVersion::new(
                    version.table().to_owned(),
                    rejected_version_record,
                ));
            }
            rejected_versions.sort_by_key(|version| {
                (
                    version.table(),
                    version.row_uuid(),
                    version.deletion().is_some(),
                )
            });
            rejected_payload = Some(RejectedTransaction::new(
                tx_id,
                rejected_tx_record,
                rejected_versions,
            ));
        }
        for version in &rejected {
            self.write_ahead_current_delete(batch, version)?;
            let history_table = self.version_storage_table_for_row(version)?;
            batch.delete(
                history_table.as_ref(),
                self.version_storage_primary_key(version, tx.tx.target_lineage)?,
            );
        }
        for (table_id, table, row_uuid) in affected_content_rows {
            self.rewrite_merge_heads_excluding_tx(batch, table_id, &table, row_uuid, tx_id)?;
        }
        self.invalidate_tx_version_tables_cache(tx_id);
        let _ = affected;
        Ok(rejected_payload)
    }

    pub(super) fn create_merge_versions_for(
        &mut self,
        records: &[VersionRecord],
    ) -> Result<(), Error> {
        let rows = self.merge_rows_for_versions(records)?;
        self.create_merge_versions_for_rows(rows)
    }

    fn merge_rows_for_versions(
        &mut self,
        records: &[VersionRecord],
    ) -> Result<Vec<(String, RowUuid)>, Error> {
        let mut rows = Vec::with_capacity(records.len());
        for record in records {
            if record.deletion().is_some() {
                continue;
            }
            let (projected_schema, table) = self.translate_cells_to_current_write_schema(
                record.schema_version(),
                record.table(),
                &mut BTreeMap::new(),
            )?;
            // Synthetic merge versions are authored in the current write
            // schema. An otherwise valid version in an unreconciled schema
            // has its own physical lineage and merge-head set, but cannot be
            // semantically merged into the write schema until a lens exists.
            if projected_schema != self.catalogue.current_write_schema.schema {
                continue;
            }
            rows.push((table, record.row_uuid()));
        }
        rows.sort_unstable();
        rows.dedup();
        Ok(rows)
    }

    pub(super) fn create_merge_versions_for_rows(
        &mut self,
        rows: Vec<(String, RowUuid)>,
    ) -> Result<(), Error> {
        for (table, row_uuid) in rows {
            self.create_merge_version_if_needed(&table, row_uuid)?;
        }
        Ok(())
    }

    pub(super) fn create_merge_version_if_needed(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<(), Error> {
        let table_id =
            self.physical_table_id_for_schema(self.catalogue.current_write_schema.schema, table)?;
        let head_tx_ids = self.merge_head_tx_ids(table_id, row_uuid)?;
        let table_schema =
            self.table_in_schema(table, self.catalogue.current_write_schema.schema)?;
        let has_gset_column = table_schema
            .columns
            .iter()
            .any(|column| table_schema.merge_strategy(&column.name) == MergeStrategy::GSet);
        if head_tx_ids.len() < 2 && !has_gset_column {
            return Ok(());
        }
        let row_versions = self.query_physical_content_row_versions(table_id, table, row_uuid)?;
        let mut row_versions_by_tx = BTreeMap::new();
        for version in row_versions {
            row_versions_by_tx.insert(self.version_tx_id(&version)?, version);
        }
        let head_tx_ids = head_tx_ids.into_iter().collect::<Vec<_>>();
        let raw_head_tx_ids = raw_merge_head_tx_ids(&row_versions_by_tx, &head_tx_ids)?;
        let mut parents = raw_head_tx_ids.clone();
        parents.sort();
        if row_versions_by_tx.values().any(|version| {
            version.layer() == VersionLayer::Content && {
                let mut existing = version.parents();
                existing.sort();
                existing == parents
            }
        }) {
            return Ok(());
        }

        let raw_heads = raw_head_tx_ids
            .iter()
            .map(|tx_id| {
                row_versions_by_tx
                    .get(tx_id)
                    .cloned()
                    .ok_or(Error::MissingTransaction(*tx_id))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let cells = self.merge_cells_for_heads(&table_schema, &raw_heads, &row_versions_by_tx)?;
        if raw_heads.len() == 1
            && has_gset_column
            && !gset_cells_need_materialization(&table_schema, &raw_heads[0], &cells)?
        {
            return Ok(());
        }
        if cells.is_empty() {
            return Ok(());
        }
        let made_at = raw_heads
            .iter()
            .map(|version| self.version_made_at(version))
            .collect::<Result<Vec<_>, Error>>()?
            .into_iter()
            .max_by_key(|made_at| made_at.sort_key(self.node_uuid))
            .map(TxTime::tick_after)
            .ok_or(Error::InvalidStoredValue("merge requires heads"))?;
        self.merge_tx_time(made_at);
        let merge_tx_id = TxId::new(made_at, self.node_uuid);
        if self.query_transaction(merge_tx_id)?.is_some() {
            return Ok(());
        }
        let merge_commit = MergeableCommit::new(table, row_uuid, made_at.physical_ms())
            .parents(parents)
            .cells(cells);
        let merge_tx = self.commit_mergeable_at(merge_commit, made_at)?;
        let global_seq = self.clock.allocate_global_seq()?;
        self.apply_fate_update(
            merge_tx,
            Fate::Accepted,
            Some(global_seq),
            Some(DurabilityTier::Global),
        )?;
        debug_assert_eq!(self.clock.applied_global_watermark, global_seq);
        Ok(())
    }

    fn merge_cells_for_heads(
        &mut self,
        table_schema: &TableSchema,
        heads: &[VersionRow],
        row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    ) -> Result<BTreeMap<String, Value>, Error> {
        let mut cells = BTreeMap::new();
        for column in &table_schema.columns {
            match table_schema.merge_strategy(&column.name) {
                MergeStrategy::Lww => {
                    let mut best: Option<(crate::time::TxTimeSortKey, Value)> = None;
                    for version in heads {
                        if version
                            .authored_columns(table_schema)?
                            .is_some_and(|columns| !columns.contains(&column.name))
                        {
                            continue;
                        }
                        let Some(value) = version.cell(table_schema, &column.name)? else {
                            continue;
                        };
                        let tx_id = self.version_tx_id(version)?;
                        let made_at = self.version_made_at(version)?;
                        let key = made_at.sort_key(tx_id.node);
                        if best.as_ref().is_none_or(|(best_key, _)| key > *best_key) {
                            best = Some((key, value));
                        }
                    }
                    if best.is_none() {
                        let parent_union = heads
                            .iter()
                            .flat_map(VersionRow::parents)
                            .collect::<BTreeSet<_>>();
                        for parent in parent_union {
                            let Some(version) = row_versions_by_tx.get(&parent) else {
                                continue;
                            };
                            let Some(value) = version.cell(table_schema, &column.name)? else {
                                continue;
                            };
                            let tx_id = self.version_tx_id(version)?;
                            let made_at = self.version_made_at(version)?;
                            let key = made_at.sort_key(tx_id.node);
                            if best.as_ref().is_none_or(|(best_key, _)| key > *best_key) {
                                best = Some((key, value));
                            }
                        }
                    }
                    if let Some((_, value)) = best {
                        cells.insert(column.name.clone(), value);
                    }
                }
                MergeStrategy::Counter => {
                    let mut memo = BTreeMap::new();
                    let value = counter_merge_value(
                        table_schema,
                        &column.name,
                        row_versions_by_tx,
                        &heads
                            .iter()
                            .map(|version| self.version_tx_id(version))
                            .collect::<Result<Vec<_>, Error>>()?,
                        &mut memo,
                    )?;
                    cells.insert(
                        column.name.clone(),
                        counter_value_from_i128(&column.column_type, value)?,
                    );
                }
                MergeStrategy::GSet => {
                    let value = gset_merge_value(
                        table_schema,
                        &column.name,
                        row_versions_by_tx,
                        &heads
                            .iter()
                            .map(|version| self.version_tx_id(version))
                            .collect::<Result<Vec<_>, Error>>()?,
                    )?;
                    cells.insert(column.name.clone(), value);
                }
            }
        }
        Ok(cells)
    }

    fn encode_merge_heads(heads: &BTreeSet<TxId>) -> Result<Vec<u8>, Error> {
        postcard::to_allocvec(&heads.iter().copied().collect::<Vec<_>>())
            .map_err(|_| Error::InvalidStoredValue("merge head set failed to encode"))
    }

    fn decode_merge_heads(bytes: &[u8]) -> Result<BTreeSet<TxId>, Error> {
        let heads: Vec<TxId> = postcard::from_bytes(bytes)
            .map_err(|_| Error::InvalidStoredValue("merge head set failed to decode"))?;
        Ok(heads.into_iter().collect())
    }

    fn read_merge_heads(
        &mut self,
        table_id: PhysicalTableId,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeSet<TxId>>, Error> {
        let row = self.database.primary_key_get_raw(
            MERGE_HEADS_TABLE,
            &[Value::U64(table_id.0), Value::Uuid(row_uuid.0)],
        )?;
        let Some(row) = row else {
            return Ok(None);
        };
        let heads = row.record().get_bytes(2)?;
        Self::decode_merge_heads(heads).map(Some)
    }

    fn read_merge_heads_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        table_id: PhysicalTableId,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeSet<TxId>>, Error> {
        let row = self.database.primary_key_get_raw_in_batch(
            batch,
            MERGE_HEADS_TABLE,
            &[Value::U64(table_id.0), Value::Uuid(row_uuid.0)],
        )?;
        let Some(row) = row else {
            return Ok(None);
        };
        let heads = row.record().get_bytes(2)?;
        Self::decode_merge_heads(heads).map(Some)
    }

    fn require_merge_heads(
        &mut self,
        table_id: PhysicalTableId,
        row_uuid: RowUuid,
    ) -> Result<BTreeSet<TxId>, Error> {
        self.read_merge_heads(table_id, row_uuid)?
            .ok_or(Error::InvalidStoredValue(
                "merge head set missing for existing global current row",
            ))
    }

    fn write_merge_heads(
        batch: &mut DatabaseBatch,
        table_id: PhysicalTableId,
        row_uuid: RowUuid,
        heads: &BTreeSet<TxId>,
    ) -> Result<(), Error> {
        batch.update(
            MERGE_HEADS_TABLE,
            vec![
                Value::U64(table_id.0),
                Value::Uuid(row_uuid.0),
                Value::Bytes(Self::encode_merge_heads(heads)?),
            ],
        );
        Ok(())
    }

    fn query_physical_content_row_versions(
        &mut self,
        table_id: PhysicalTableId,
        requested_table: &str,
        row_uuid: RowUuid,
    ) -> Result<Vec<VersionRow>, Error> {
        let storage_table = physical_history_table_name(table_id);
        let raws = self
            .database
            .primary_key_scan_raw(&storage_table, &[Value::Uuid(row_uuid.0)])?
            .into_iter()
            .map(|raw| raw.owned_record())
            .collect::<Vec<_>>();
        let mut versions = raws
            .into_iter()
            .map(|record| self.decode_history_owned_record(requested_table, &storage_table, record))
            .collect::<Result<Vec<_>, Error>>()?;
        let aliases = self.node_aliases.clone();
        versions.sort_by_key(|version| {
            version_tx_id_from_aliases(version, &aliases).expect("valid version tx id")
        });
        Ok(versions)
    }

    fn query_physical_content_layer_winner(
        &mut self,
        table_id: PhysicalTableId,
        requested_table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<VersionRow>, Error> {
        let mut winner = None;
        for version in
            self.query_physical_content_row_versions(table_id, requested_table, row_uuid)?
        {
            let candidate_tx = self.version_tx_id(&version)?;
            let replaces_winner = winner.as_ref().is_none_or(|existing: &VersionRow| {
                let existing_tx = self.version_tx_id(existing).expect("valid version tx id");
                candidate_tx.time.sort_key(candidate_tx.node)
                    > existing_tx.time.sort_key(existing_tx.node)
            });
            if replaces_winner {
                winner = Some(version);
            }
        }
        Ok(winner)
    }

    pub(super) fn update_merge_heads_for_content_version(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<(), Error> {
        if version.layer() != VersionLayer::Content {
            return Ok(());
        }
        let table_id = self.physical_table_id_for_version(version)?;
        let new_tx = self.version_tx_id(version)?;
        let mut heads = match self.read_merge_heads(table_id, version.row_uuid())? {
            Some(existing) => existing,
            None => {
                if let Some(previous) = self.query_physical_content_layer_winner(
                    table_id,
                    version.table(),
                    version.row_uuid(),
                )? {
                    let previous_tx = self.version_tx_id(&previous)?;
                    if previous_tx != new_tx {
                        return Err(Error::InvalidStoredValue(
                            "merge head set missing for existing content row",
                        ));
                    }
                }
                BTreeSet::new()
            }
        };
        for parent in version.parents() {
            heads.remove(&parent);
        }
        let dominated_by_existing_head = heads
            .iter()
            .copied()
            .map(|head| {
                self.content_version_reaches_tx_in_batch(
                    batch,
                    table_id,
                    version.table(),
                    version.row_uuid(),
                    head,
                    new_tx,
                )
            })
            .collect::<Result<Vec<_>, Error>>()?
            .into_iter()
            .any(|reaches| reaches);
        if !dominated_by_existing_head {
            heads.insert(new_tx);
        }
        Self::write_merge_heads(batch, table_id, version.row_uuid(), &heads)
    }

    fn update_merge_heads_for_content_version_in_batch(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<(), Error> {
        if version.layer() != VersionLayer::Content {
            return Ok(());
        }
        let table_id = self.physical_table_id_for_version(version)?;
        let new_tx = self.version_tx_id(version)?;
        let mut heads = match self.read_merge_heads_in_batch(batch, table_id, version.row_uuid())? {
            Some(existing) => existing,
            None => {
                if let Some(previous) = self.query_physical_content_layer_winner(
                    table_id,
                    version.table(),
                    version.row_uuid(),
                )? {
                    let previous_tx = self.version_tx_id(&previous)?;
                    if previous_tx != new_tx {
                        return Err(Error::InvalidStoredValue(
                            "merge head set missing for existing content row",
                        ));
                    }
                }
                BTreeSet::new()
            }
        };
        for parent in version.parents() {
            heads.remove(&parent);
        }
        let dominated_by_existing_head = heads
            .iter()
            .copied()
            .map(|head| self.content_version_reaches_tx(table_id, version.row_uuid(), head, new_tx))
            .collect::<Result<Vec<_>, Error>>()?
            .into_iter()
            .any(|reaches| reaches);
        if !dominated_by_existing_head {
            heads.insert(new_tx);
        }
        Self::write_merge_heads(batch, table_id, version.row_uuid(), &heads)
    }

    fn query_global_layer_winner_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        let schema_version = if self
            .table_in_schema(table, self.catalogue.current_schema_version_id)
            .is_ok()
        {
            self.catalogue.current_schema_version_id
        } else {
            self.table_in_schema(table, self.catalogue.current_write_schema.schema)?;
            self.catalogue.current_write_schema.schema
        };
        let current_table = self.physical_current_table_for_schema(
            schema_version,
            table,
            layer,
            PhysicalCurrentClass::Global,
        )?;
        let raw = self.database.primary_key_get_raw_in_batch(
            batch,
            &current_table,
            &[Value::Uuid(row_uuid.0)],
        )?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let record = raw.record();
        let tx_time = TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?);
        let tx_node_alias =
            NodeAlias(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
        self.query_version_by_alias_in_batch(batch, table, row_uuid, layer, tx_time, tx_node_alias)
    }

    fn query_version_by_alias_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<VersionRow>, Error> {
        for storage_table in self.version_storage_sources_for_layer(table, layer)? {
            let key = if storage_table == SHARED_DELETION_HISTORY_TABLE {
                let mut key =
                    self.deletion_storage_prefix(table, BranchLineage::Root, Some(row_uuid))?;
                key.extend([Value::U64(tx_time.0), Value::U64(tx_node_alias.0)]);
                key
            } else {
                vec![
                    Value::Uuid(row_uuid.0),
                    Value::U64(tx_time.0),
                    Value::U64(tx_node_alias.0),
                ]
            };
            let raw = self
                .database
                .primary_key_get_raw_in_batch(batch, &storage_table, &key)?;
            let record = raw.map(|raw| raw.owned_record());
            let Some(record) = record else {
                continue;
            };
            return self
                .decode_history_owned_record(table, &storage_table, record)
                .map(Some);
        }
        Ok(None)
    }

    fn write_merge_heads_for_bulk_content_versions(
        &mut self,
        batch: &mut DatabaseBatch,
        versions: &[VersionRow],
    ) -> Result<(), Error> {
        let mut by_row = BTreeMap::<(PhysicalTableId, RowUuid), Vec<&VersionRow>>::new();
        for version in versions {
            if version.layer() == VersionLayer::Content {
                let table_id = self.physical_table_id_for_version(version)?;
                by_row
                    .entry((table_id, version.row_uuid()))
                    .or_default()
                    .push(version);
            }
        }
        for ((table_id, row_uuid), mut row_versions) in by_row {
            row_versions.sort_by_key(|version| {
                let tx_id = self
                    .version_tx_id(version)
                    .expect("bulk content version must have node alias");
                tx_id.time.sort_key(tx_id.node)
            });
            let mut heads = self
                .read_merge_heads(table_id, row_uuid)?
                .unwrap_or_default();
            let mut staged_parents = BTreeMap::<TxId, Vec<TxId>>::new();
            for version in &row_versions {
                staged_parents.insert(self.version_tx_id(version)?, version.parents());
            }
            for version in row_versions {
                let new_tx = self.version_tx_id(version)?;
                for parent in version.parents() {
                    heads.remove(&parent);
                }
                let dominated_by_existing_head = heads
                    .iter()
                    .copied()
                    .map(|head| {
                        content_version_reaches_tx_in_staged_parents(head, new_tx, &staged_parents)
                            .map_or_else(
                                || {
                                    self.content_version_reaches_tx(
                                        table_id, row_uuid, head, new_tx,
                                    )
                                },
                                Ok,
                            )
                    })
                    .collect::<Result<Vec<_>, Error>>()?
                    .into_iter()
                    .any(|reaches| reaches);
                if !dominated_by_existing_head {
                    heads.insert(new_tx);
                }
            }
            Self::write_merge_heads(batch, table_id, row_uuid, &heads)?;
        }
        Ok(())
    }

    fn content_version_reaches_tx(
        &mut self,
        table_id: PhysicalTableId,
        row_uuid: RowUuid,
        start: TxId,
        target: TxId,
    ) -> Result<bool, Error> {
        let mut stack = vec![start];
        let mut seen = BTreeSet::new();
        while let Some(tx_id) = stack.pop() {
            if tx_id == target {
                return Ok(true);
            }
            if !seen.insert(tx_id) {
                continue;
            }
            for version in self.query_versions_for_tx(tx_id)? {
                if self.physical_table_id_for_version(&version)? == table_id
                    && version.row_uuid() == row_uuid
                    && version.layer() == VersionLayer::Content
                {
                    stack.extend(version.parents());
                }
            }
        }
        Ok(false)
    }

    fn content_version_reaches_tx_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        table_id: PhysicalTableId,
        table: &str,
        row_uuid: RowUuid,
        start: TxId,
        target: TxId,
    ) -> Result<bool, Error> {
        let mut stack = vec![start];
        let mut seen = BTreeSet::new();
        while let Some(tx_id) = stack.pop() {
            if tx_id == target {
                return Ok(true);
            }
            if !seen.insert(tx_id) {
                continue;
            }
            for version in self
                .query_versions_for_tx_in_batch_for_row(batch, tx_id, table_id, table, row_uuid)?
            {
                if self.physical_table_id_for_version(&version)? == table_id
                    && version.row_uuid() == row_uuid
                    && version.layer() == VersionLayer::Content
                {
                    stack.extend(version.parents());
                }
            }
        }
        Ok(false)
    }

    fn query_versions_for_tx_in_batch_for_row(
        &mut self,
        batch: &DatabaseBatch,
        tx_id: TxId,
        table_id: PhysicalTableId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Vec<VersionRow>, Error> {
        let mut versions = Vec::new();
        let Some(tx_node_alias) = self.node_aliases.get(&tx_id.node).copied() else {
            return Ok(versions);
        };
        let storage_table = physical_history_table_name(table_id);
        if let Some(raw) = self.database.primary_key_get_raw_in_batch(
            batch,
            &storage_table,
            &[
                Value::Uuid(row_uuid.0),
                Value::U64(tx_id.time.0),
                Value::U64(tx_node_alias.0),
            ],
        )? {
            versions.push(self.decode_history_owned_record(
                table,
                &storage_table,
                raw.owned_record(),
            )?);
        }
        Ok(versions)
    }

    fn rewrite_merge_heads_excluding_tx(
        &mut self,
        batch: &mut DatabaseBatch,
        table_id: PhysicalTableId,
        table: &str,
        row_uuid: RowUuid,
        excluded_tx: TxId,
    ) -> Result<(), Error> {
        let versions = self.query_physical_content_row_versions(table_id, table, row_uuid)?;
        let candidate_indices = versions
            .iter()
            .enumerate()
            .filter(|(_, version)| {
                version.layer() == VersionLayer::Content
                    && self.version_tx_id(version).ok() != Some(excluded_tx)
            })
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();
        let head_indices = content_head_indices(&versions, &candidate_indices, &self.node_aliases);
        let mut heads = BTreeSet::new();
        for idx in head_indices {
            heads.insert(self.version_tx_id(&versions[idx])?);
        }
        Self::write_merge_heads(batch, table_id, row_uuid, &heads)
    }

    fn merge_head_tx_ids(
        &mut self,
        table_id: PhysicalTableId,
        row_uuid: RowUuid,
    ) -> Result<BTreeSet<TxId>, Error> {
        self.require_merge_heads(table_id, row_uuid)
    }

    #[cfg(test)]
    fn recomputed_merge_heads_from_history_for_test(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<BTreeSet<TxId>, Error> {
        let table_id =
            self.physical_table_id_for_schema(self.catalogue.current_write_schema.schema, table)?;
        let versions = self.query_physical_content_row_versions(table_id, table, row_uuid)?;
        let mut candidate_indices = Vec::new();
        for (idx, version) in versions.iter().enumerate() {
            if version.layer() != VersionLayer::Content {
                continue;
            }
            let tx_id = self.version_tx_id(version)?;
            let Some(tx) = self.query_transaction(tx_id)? else {
                continue;
            };
            if matches!(tx.fate, Fate::Pending | Fate::Accepted) {
                candidate_indices.push(idx);
            }
        }
        let head_indices = content_head_indices(&versions, &candidate_indices, &self.node_aliases);
        let mut heads = BTreeSet::new();
        for idx in head_indices {
            heads.insert(self.version_tx_id(&versions[idx])?);
        }
        Ok(heads)
    }

    #[cfg(test)]
    pub(super) fn rebuild_merge_heads_from_history_for_test(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<(), Error> {
        let heads = self.recomputed_merge_heads_from_history_for_test(table, row_uuid)?;
        let table_id =
            self.physical_table_id_for_schema(self.catalogue.current_write_schema.schema, table)?;
        let mut batch = self.database.open_batch();
        Self::write_merge_heads(&mut batch, table_id, row_uuid, &heads)?;
        self.database.commit_batch(batch)?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn assert_merge_heads_match_history_for_test(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<(), Error> {
        let expected = self.recomputed_merge_heads_from_history_for_test(table, row_uuid)?;
        let table_id =
            self.physical_table_id_for_schema(self.catalogue.current_write_schema.schema, table)?;
        let actual = self.require_merge_heads(table_id, row_uuid)?;
        if actual != expected {
            let versions = self
                .query_row_versions(table, row_uuid)?
                .into_iter()
                .map(|version| {
                    let tx_id = self.version_tx_id(&version)?;
                    let fate = self
                        .query_transaction(tx_id)?
                        .map(|tx| tx.fate)
                        .unwrap_or(Fate::Pending);
                    Ok(format!(
                        "{tx_id:?} layer={:?} parents={:?} fate={fate:?}",
                        version.layer(),
                        version.parents()
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            panic!(
                "stored merge heads diverged from history for {table}/{row_uuid:?}: expected {expected:?}, actual {actual:?}, versions={versions:?}"
            );
        }
        Ok(())
    }

    #[cfg(test)]
    fn assert_merge_head_rows_match_history_for_test(
        &mut self,
        rows: &BTreeSet<(String, RowUuid)>,
    ) -> Result<(), Error> {
        for (table, row_uuid) in rows {
            self.assert_merge_heads_match_history_for_test(table, *row_uuid)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn recomputed_global_layer_winner_from_history_for_test(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        let mut winner = None::<(VersionRow, TxId, TxTime)>;
        for version in self
            .query_row_versions(table, row_uuid)?
            .into_iter()
            .filter(|version| version.layer() == layer)
        {
            let tx_id = self.version_tx_id(&version)?;
            let Some(tx) = self.query_transaction(tx_id)? else {
                continue;
            };
            if !matches!(tx.fate, Fate::Accepted) || tx.global_seq.is_none() {
                continue;
            }
            let made_at = self.version_made_at(&version)?;
            let previous = winner
                .as_ref()
                .map(|(version, tx_id, made_at)| (version, *tx_id, *made_at));
            if version_wins_over_open_winner(&version, tx_id, made_at, previous) {
                winner = Some((version, tx_id, made_at));
            }
        }
        Ok(winner.map(|(version, _, _)| version))
    }

    #[cfg(test)]
    fn assert_global_current_updates_match_history_for_test(
        &mut self,
        updates: &[(VersionRow, GlobalSeq)],
    ) -> Result<(), Error> {
        for (version, global_seq) in updates {
            let Some(expected) = self.recomputed_global_layer_winner_from_history_for_test(
                version.table(),
                version.row_uuid(),
                version.layer(),
            )?
            else {
                panic!(
                    "global-current update has no accepted history winner for {}/ {:?} {:?}",
                    version.table(),
                    version.row_uuid(),
                    version.layer()
                );
            };
            let expected_tx = self.version_tx_id(&expected)?;
            let actual_tx = self.version_tx_id(version)?;
            if expected_tx != actual_tx {
                panic!(
                    "global-current update diverged from history for {}/{:?} {:?}: expected winner {:?}, actual update {:?}",
                    version.table(),
                    version.row_uuid(),
                    version.layer(),
                    expected_tx,
                    actual_tx
                );
            }
            self.assert_global_current_row_matches_version_for_test(version, *global_seq)?;
            self.assert_global_change_row_matches_version_for_test(version, *global_seq)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn assert_global_current_row_matches_version_for_test(
        &mut self,
        version: &VersionRow,
        global_seq: GlobalSeq,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let table = self
            .table_in_schema(version.table(), schema_version)?
            .clone();
        let storage_tables = table.global_current_storage_tables();
        let (current_table, current_schema, expected_values) = match version.layer() {
            VersionLayer::Content => (
                groove::Intern::new(self.physical_current_table_for_schema(
                    schema_version,
                    version.table(),
                    VersionLayer::Content,
                    PhysicalCurrentClass::Global,
                )?),
                &storage_tables[0],
                self.public_current_values(&table, version, Some(global_seq))?,
            ),
            VersionLayer::Deletion => (
                groove::Intern::new(self.physical_current_table_for_schema(
                    schema_version,
                    version.table(),
                    VersionLayer::Deletion,
                    PhysicalCurrentClass::Global,
                )?),
                &storage_tables[1],
                register_global_current_values(version, Some(global_seq)),
            ),
        };
        let rows = self
            .database
            .primary_key_scan_raw(current_table.as_ref(), &[Value::Uuid(version.row_uuid().0)])?;
        let actual = rows.first().map(|row| row.record().raw().to_vec());
        let expected = owned_record_from_storage_values(current_schema, expected_values)?
            .raw()
            .to_vec();
        if actual.as_deref() != Some(expected.as_slice()) {
            panic!(
                "global-current row diverged for {}/{:?} {:?}: expected {:?}, actual {:?}",
                version.table(),
                version.row_uuid(),
                version.layer(),
                expected,
                actual
            );
        }
        Ok(())
    }

    #[cfg(test)]
    fn assert_global_change_row_matches_version_for_test(
        &mut self,
        version: &VersionRow,
        global_seq: GlobalSeq,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let table_id = self.physical_table_id_for_schema(schema_version, version.table())?;
        let rows = self.database.primary_key_scan_raw(
            "jazz_global_changes",
            &[
                Value::U64(table_id.0),
                Value::Uuid(version.row_uuid().0),
                Value::Bytes(version_layer_string(version.layer()).into_bytes()),
                Value::U64(global_seq.0),
            ],
        )?;
        let Some(row) = rows.first() else {
            panic!(
                "missing global-change row for {}/{:?} {:?} at {:?}",
                version.table(),
                version.row_uuid(),
                version.layer(),
                global_seq
            );
        };
        let record = row.record();
        let expected_deletion = version.deletion();
        let actual_deletion =
            nullable_value(record.get_idx(GlobalChangeRowRecord::FIELD__DELETION_IDX)?)?
                .map(deletion_event_from_value)
                .transpose()?;
        let actual_tx = TxId::new(
            TxTime(record.get_u64(GlobalChangeRowRecord::FIELD_TX_TIME_IDX)?),
            self.node_for_alias(NodeAlias(
                record.get_u64(GlobalChangeRowRecord::FIELD_TX_NODE_ID_IDX)?,
            ))
            .ok_or(Error::InvalidStoredValue(
                "global-change tx node alias must exist",
            ))?,
        );
        let expected_tx = self.version_tx_id(version)?;
        if actual_tx != expected_tx || actual_deletion != expected_deletion {
            panic!(
                "global-change row diverged for {}/{:?} {:?} at {:?}: expected tx {:?} deletion {:?}, actual tx {:?} deletion {:?}",
                version.table(),
                version.row_uuid(),
                version.layer(),
                global_seq,
                expected_tx,
                expected_deletion,
                actual_tx,
                actual_deletion
            );
        }
        Ok(())
    }

    pub(super) fn write_global_current_update(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
        global_seq: GlobalSeq,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        match version.layer() {
            VersionLayer::Content => {
                let table = self.table_in_schema(version.table(), schema_version)?;
                let binding = physical_current_binding(
                    &self.catalogue.catalogue_schemas,
                    &self.catalogue.physical_mappings,
                    schema_version,
                    version.table(),
                    PhysicalCurrentClass::Global,
                )?;
                let logical = owned_record_from_storage_values(
                    &table.global_current_storage_tables()[0],
                    self.public_current_values(&table, version, Some(global_seq))?,
                )
                .expect("valid global current row");
                let mapping = self
                    .catalogue
                    .physical_mappings
                    .get(&schema_version)
                    .and_then(|mapping| mapping.tables.get(version.table()))
                    .cloned()
                    .ok_or(Error::InvalidStoredValue(
                        "physical global-current table mapping missing",
                    ))?;
                let physical_table = self.database.table_schema(&binding.storage_table)?.clone();
                let descriptor = physical_write_descriptor(
                    &table.global_current_storage_tables()[0].record_schema(),
                    &physical_current_field_names(&table, &mapping)?,
                    &physical_table,
                )?;
                let mut values = logical.to_values()?;
                self.remap_authored_enum_cells_for_physical(
                    &mut values,
                    &table,
                    &mapping,
                    &physical_table,
                    GlobalCurrentRowRecord::USER_CELLS,
                )?;
                let physical = OwnedRecord::new(descriptor.create(&values)?, descriptor);
                batch.update_raw(
                    binding.storage_table,
                    global_current_primary_key(version.row_uuid()),
                    groove::records::VariantRecord::new(
                        u32::try_from(version.schema_version_alias().0)
                            .expect("schema aliases are allocated in Groove's variant-tag space"),
                        physical,
                    ),
                );
            }
            VersionLayer::Deletion => batch.update_raw(
                self.physical_current_table_for_schema(
                    schema_version,
                    version.table(),
                    VersionLayer::Deletion,
                    PhysicalCurrentClass::Global,
                )?,
                global_current_primary_key(version.row_uuid()),
                version.bind_groove_record(
                    owned_record_from_storage_values(
                        &self
                            .table_in_schema(version.table(), schema_version)?
                            .global_current_storage_tables()[1],
                        register_global_current_values(version, Some(global_seq)),
                    )
                    .expect("valid register global current row"),
                ),
            ),
        }
        batch.update(
            "jazz_global_changes",
            global_change_values(
                self.physical_table_id_for_schema(schema_version, version.table())?,
                version,
                global_seq,
            ),
        );
        Ok(())
    }

    pub(super) fn write_ahead_current_insert(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<(), Error> {
        // A peer may replay a transaction that is already present locally
        // (notably while a fresh browser relay hydrates from its persistent
        // worker). History ingestion verifies that replay is byte-identical;
        // its pending-current projection must be idempotent too. Otherwise a
        // self-referential schema can visit the same version twice and try to
        // insert its exact current primary key again.
        if self.ahead_current_keys.contains(&(
            version.table().to_owned(),
            version.layer(),
            version.row_uuid(),
            version.tx_time(),
            version.tx_node_alias(),
        )) {
            return Ok(());
        }
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        match version.layer() {
            VersionLayer::Content => {
                let table = self.table_in_schema(version.table(), schema_version)?;
                let binding = physical_current_binding(
                    &self.catalogue.catalogue_schemas,
                    &self.catalogue.physical_mappings,
                    schema_version,
                    version.table(),
                    PhysicalCurrentClass::Ahead,
                )?;
                let logical = owned_record_from_storage_values(
                    &table.ahead_current_storage_tables()[0],
                    self.public_current_values(&table, version, None)?,
                )
                .expect("valid ahead current row");
                let mapping = self
                    .catalogue
                    .physical_mappings
                    .get(&schema_version)
                    .and_then(|mapping| mapping.tables.get(version.table()))
                    .cloned()
                    .ok_or(Error::InvalidStoredValue(
                        "physical ahead-current table mapping missing",
                    ))?;
                let physical_table = self.database.table_schema(&binding.storage_table)?.clone();
                let descriptor = physical_write_descriptor(
                    &table.ahead_current_storage_tables()[0].record_schema(),
                    &physical_current_field_names(&table, &mapping)?,
                    &physical_table,
                )?;
                let mut values = logical.to_values()?;
                self.remap_authored_enum_cells_for_physical(
                    &mut values,
                    &table,
                    &mapping,
                    &physical_table,
                    GlobalCurrentRowRecord::USER_CELLS,
                )?;
                let physical = OwnedRecord::new(descriptor.create(&values)?, descriptor);
                batch.insert_raw(
                    binding.storage_table,
                    history_primary_key(version),
                    groove::records::VariantRecord::new(
                        u32::try_from(version.schema_version_alias().0)
                            .expect("schema aliases are allocated in Groove's variant-tag space"),
                        physical,
                    ),
                );
            }
            VersionLayer::Deletion => batch.insert_raw(
                self.physical_current_table_for_schema(
                    schema_version,
                    version.table(),
                    VersionLayer::Deletion,
                    PhysicalCurrentClass::Ahead,
                )?,
                history_primary_key(version),
                version.bind_groove_record(
                    owned_record_from_storage_values(
                        &self
                            .table_in_schema(version.table(), schema_version)?
                            .ahead_current_storage_tables()[1],
                        register_global_current_values(version, None),
                    )
                    .expect("valid register ahead current row"),
                ),
            ),
        }
        self.insert_ahead_current_key(
            version.table().to_owned(),
            version.layer(),
            version.row_uuid(),
            version.tx_time(),
            version.tx_node_alias(),
        );
        Ok(())
    }

    /// Build the physical current-source carrier consumed by Groove terminals.
    fn public_current_values(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
        global_seq: Option<GlobalSeq>,
    ) -> Result<Vec<Value>, Error> {
        global_current_values(table, version, global_seq)
    }

    pub(super) fn write_ahead_current_delete(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let table = self.physical_current_table_for_schema(
            schema_version,
            version.table(),
            version.layer(),
            PhysicalCurrentClass::Ahead,
        )?;
        batch.delete(table, history_primary_key(version));
        self.remove_ahead_current_key(
            version.table(),
            version.layer(),
            version.row_uuid(),
            version.tx_time(),
            version.tx_node_alias(),
        );
        Ok(())
    }

    /// Once a transaction is rejected or globally settled, it must not remain
    /// in the ahead-current overlay: accepted global effects live in current
    /// tables, and rejected effects are no longer visible. Edge-accepted
    /// no-global transactions intentionally stay ahead-visible at Edge tier.
    /// Outbox/redelivery may keep the commit unit until fate arrives, so
    /// callers invoke this strictly after the cleanup-triggering fate is durable.
    pub(super) fn cleanup_fated_ahead_current_for_tx(
        &mut self,
        batch: &mut DatabaseBatch,
        tx_id: TxId,
    ) -> Result<(), Error> {
        let versions = self.query_versions_for_tx(tx_id)?;
        self.cleanup_fated_ahead_current_for_versions(batch, &versions)
    }

    fn cleanup_fated_ahead_current_for_versions(
        &mut self,
        batch: &mut DatabaseBatch,
        versions: &[VersionRow],
    ) -> Result<(), Error> {
        for version in versions {
            self.write_ahead_current_delete(batch, &version)?;
        }
        Ok(())
    }

    pub(super) fn cleanup_settled_ahead_current_leftovers(
        &mut self,
        already_consistent_through: Option<TxTime>,
    ) -> Result<(), Error> {
        let mut tx_ids = Vec::new();
        for raw in self
            .database
            .primary_key_scan_raw("jazz_transactions", &[])?
        {
            let record = raw.record();
            let fate = fate_from_encoded_fields(record)?;
            let global_seq = record.get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_SEQ_IDX)?;
            if !matches!(fate, Fate::Rejected(_)) && global_seq.is_none() {
                continue;
            }
            let tx_time = TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?);
            if already_consistent_through.is_some_and(|through| tx_time <= through) {
                continue;
            }
            let node_alias = NodeAlias(record.get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)?);
            let node = self
                .node_for_alias(node_alias)
                .ok_or(Error::InvalidStoredValue(
                    "transaction node alias must exist",
                ))?;
            tx_ids.push(TxId::new(tx_time, node));
        }
        if tx_ids.is_empty() {
            return Ok(());
        }
        let mut batch = self.database.open_batch();
        for tx_id in &tx_ids {
            self.cleanup_fated_ahead_current_for_tx(&mut batch, *tx_id)?;
        }
        self.database.commit_batch(batch)?;
        if let Some(tx_time) = tx_ids.into_iter().map(|tx_id| tx_id.time).max() {
            self.persist_storage_consistency_marker_through(tx_time)?;
        }
        Ok(())
    }

    fn prune_ahead_current_for_global_seq(
        &mut self,
        batch: &mut DatabaseBatch,
        global_seq: GlobalSeq,
    ) -> Result<(), Error> {
        let mut tx_ids = Vec::new();
        for raw in self.database.index_scan_raw(
            "jazz_transactions",
            "by_global_seq",
            &[Value::U64(global_seq.0)],
        )? {
            let record = raw.record();
            tx_ids.push(TxId::new(
                TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?),
                self.node_for_alias(NodeAlias(
                    record.get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)?,
                ))
                .ok_or(Error::InvalidStoredValue(
                    "transaction node alias must exist",
                ))?,
            ));
        }
        for tx_id in tx_ids {
            for version in self.query_versions_for_tx(tx_id)? {
                self.write_ahead_current_delete(batch, &version)?;
            }
        }
        Ok(())
    }

    pub(super) fn ingest_transaction_and_versions(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: DurabilityTier,
    ) -> Result<(), Error> {
        self.ingest_transaction_and_versions_with_current_indexes(
            tx, versions, fate, global_seq, durability, true,
        )
    }

    pub(super) fn ingest_transaction_fragment_without_current_indexes(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: DurabilityTier,
    ) -> Result<(), Error> {
        self.ingest_transaction_and_versions_with_current_indexes(
            tx, versions, fate, global_seq, durability, false,
        )
    }

    fn ingest_transaction_and_versions_with_current_indexes(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: DurabilityTier,
        update_current_indexes: bool,
    ) -> Result<(), Error> {
        let tx_id = tx.tx_id;
        let mut batch = self.database.open_batch();
        self.stage_transaction_and_versions_with_current_indexes(
            &mut batch,
            tx,
            versions,
            fate.clone(),
            global_seq,
            durability,
            update_current_indexes,
        )?;
        self.database.commit_batch(batch)?;
        let mut staged_global_seqs = Vec::new();
        let mut cleanup_batch = self.database.open_batch();
        self.finalize_staged_transaction_ingest(
            &mut cleanup_batch,
            tx_id,
            fate,
            global_seq,
            &mut staged_global_seqs,
        )?;
        if !cleanup_batch.is_empty() {
            self.database.commit_batch(cleanup_batch)?;
            self.persist_storage_consistency_marker_through(tx_id.time)?;
        }
        Ok(())
    }

    fn stage_transaction_and_versions_with_current_indexes(
        &mut self,
        batch: &mut DatabaseBatch,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: DurabilityTier,
        update_current_indexes: bool,
    ) -> Result<(), Error> {
        self.merge_tx_time(tx.tx_id.time);
        let update_current_indexes =
            update_current_indexes && tx.target_lineage == crate::tx::BranchLineage::Root;
        let tx_node_alias = self.ensure_node_alias(tx.tx_id.node)?;
        let tx_already_known = self.query_transaction(tx.tx_id)?.is_some();
        let tx_values =
            transaction_values(tx_node_alias, &tx, fate.clone(), global_seq, durability);
        if tx_already_known {
            batch.update("jazz_transactions", tx_values);
        } else {
            batch.insert("jazz_transactions", tx_values);
        }

        let parent_edges = versions
            .iter()
            .flat_map(|version| version.parents())
            .collect::<BTreeSet<_>>();
        let pending_edge_rows = if matches!(fate, Fate::Pending) {
            parent_edges
                .iter()
                .map(|parent| {
                    let parent_alias = self.node_aliases.get(&parent.node).copied().ok_or(
                        Error::InvalidStoredValue("pending edge parent alias must exist"),
                    )?;
                    Ok((*parent, parent_alias))
                })
                .collect::<Result<Vec<_>, Error>>()?
        } else {
            Vec::new()
        };
        let mut pending_global_updates =
            BTreeMap::<(String, RowUuid, VersionLayer), VersionRow>::new();
        let mut content_versions = Vec::new();
        let mut stored_versions = Vec::new();
        for version in versions {
            let author_schema = version.schema_version();
            let source_table_schema = self.table_in_schema(version.table(), author_schema)?;
            let table_schema = source_table_schema;
            let schema_version_alias = self.ensure_schema_version_alias(author_schema)?;
            let stored = VersionRow::from_wire_with_schema_version(
                &table_schema,
                &version,
                tx_node_alias,
                schema_version_alias,
                tx.tx_id.time,
                (author_schema != self.catalogue.current_schema_version_id)
                    .then_some(author_schema),
            )?;
            let layer = VersionLayer::for_record(&version);
            let previous_current =
                self.query_local_layer_winner(&table_schema.name, version.row_uuid(), layer)?;
            let previous_winner = if let Some(previous) = previous_current.as_ref() {
                let previous_tx_id = self.version_tx_id(previous)?;
                let previous_made_at = if previous_tx_id == tx.tx_id {
                    tx.tx_id.time
                } else {
                    self.version_made_at(previous)?
                };
                Some((previous, previous_tx_id, previous_made_at))
            } else {
                None
            };
            let new_is_current =
                version_wins_over_open_winner(&stored, tx.tx_id, tx.tx_id.time, previous_winner);
            debug_assert!(
                new_is_current || previous_current.is_some(),
                "clock condition violated: local winner after insert must be the previous winner or inserted version"
            );
            let _ = (new_is_current, previous_current);
            if !matches!(fate, Fate::Rejected(_)) && stored.layer() == VersionLayer::Content {
                content_versions.push(stored.clone());
            }
            stored_versions.push(stored.clone());
            if update_current_indexes && matches!(fate, Fate::Accepted) {
                if global_seq.is_some() {
                    let previous_global_current = self.query_global_layer_winner_in_batch(
                        batch,
                        &table_schema.name,
                        stored.row_uuid(),
                        stored.layer(),
                    )?;
                    let previous_global_winner =
                        if let Some(previous) = previous_global_current.as_ref() {
                            Some((previous, self.version_tx_id(previous)?, previous.tx_time()))
                        } else {
                            None
                        };
                    let new_is_global_current = version_wins_over_open_winner(
                        &stored,
                        tx.tx_id,
                        tx.tx_id.time,
                        previous_global_winner,
                    );
                    debug_assert!(
                        new_is_global_current || previous_global_current.is_some(),
                        "clock condition violated: global winner after insert must be the previous winner or inserted version"
                    );
                    if new_is_global_current {
                        pending_global_updates.insert(
                            (stored.table().to_owned(), stored.row_uuid(), stored.layer()),
                            stored.clone(),
                        );
                    }
                }
            }
            let (history_table, groove_record) = match tx.target_lineage {
                crate::tx::BranchLineage::Root => self.version_storage_write_binding(&stored)?,
                crate::tx::BranchLineage::Branch(branch_id) => {
                    self.branch_version_storage_write_binding(&stored, branch_id)?
                }
            };
            let storage_key = self.version_storage_primary_key(&stored, tx.target_lineage)?;
            if tx_already_known {
                let existing = self.database.primary_key_get_raw_in_batch(
                    batch,
                    history_table.as_ref(),
                    &self.version_storage_primary_key_values(&stored, tx.target_lineage)?,
                )?;
                if let Some(existing) = existing {
                    if existing.record().raw() != groove_record.record().raw() {
                        return Err(Error::ConflictingCommitUnit(tx.tx_id));
                    }
                } else {
                    batch.insert_raw(history_table.as_ref(), storage_key, groove_record);
                }
            } else {
                batch.insert_raw_fresh(history_table.as_ref(), storage_key, groove_record);
            }
            if update_current_indexes && !matches!(fate, Fate::Rejected(_)) && global_seq.is_none()
            {
                self.write_ahead_current_insert(batch, &stored)?;
            }
        }
        if update_current_indexes && !matches!(fate, Fate::Rejected(_)) {
            for stored in &content_versions {
                self.update_merge_heads_for_content_version_in_batch(batch, stored)?;
            }
        }
        if update_current_indexes && matches!(fate, Fate::Accepted) {
            if let Some(global_seq) = global_seq {
                for stored in pending_global_updates.values() {
                    self.write_global_current_update(batch, stored, global_seq)?;
                }
            }
        }
        for (parent, parent_alias) in &pending_edge_rows {
            let values = pending_edge_values(tx_node_alias, tx.tx_id, *parent_alias, *parent);
            if tx_already_known {
                batch.update("jazz_pending_edges", values);
            } else {
                batch.insert("jazz_pending_edges", values);
            }
        }
        if matches!(fate, Fate::Accepted) {
            self.rejections.child_txs_by_parent.remove(&tx.tx_id);
            self.prune_child_edges(tx.tx_id);
        } else if matches!(fate, Fate::Pending) {
            self.record_child_edges(tx.tx_id, parent_edges);
        }
        self.cache_tx_versions(tx.tx_id, stored_versions);
        Ok(())
    }

    fn finalize_staged_transaction_ingest(
        &mut self,
        batch: &mut DatabaseBatch,
        tx_id: TxId,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        staged_global_seqs: &mut Vec<GlobalSeq>,
    ) -> Result<(), Error> {
        self.invalidate_tx_version_table_names_cache(tx_id);
        if matches!(fate, Fate::Accepted)
            && let Some(global_seq) = global_seq
        {
            staged_global_seqs.push(global_seq);
            let advanced_global_seqs = self.record_applied_global_seq(global_seq);
            self.cleanup_fated_ahead_current_for_tx(batch, tx_id)?;
            if !advanced_global_seqs.is_empty() {
                for advanced in advanced_global_seqs
                    .into_iter()
                    .filter(|advanced| *advanced != global_seq)
                {
                    self.prune_ahead_current_for_global_seq(batch, advanced)?;
                }
            }
        }
        Ok(())
    }

    fn translate_cells_to_current_write_schema(
        &mut self,
        source: SchemaVersionId,
        table: &str,
        cells: &mut BTreeMap<String, Value>,
    ) -> Result<(SchemaVersionId, String), Error> {
        let target = self.catalogue.current_write_schema.schema;
        if source == target {
            return Ok((source, table.to_owned()));
        }
        for direction in [LensPathDirection::Forward, LensPathDirection::Reverse] {
            if let Some(path) = self.compiled_lens_path(source, target, direction, table)? {
                return Ok((target, apply_compiled_lens_path(&path, cells)));
            }
        }
        Ok((source, table.to_owned()))
    }

    /// A wire row version is a complete row under the schema id it declares.
    /// An unknown schema cannot be checked until its catalogue value arrives,
    /// but a known schema must never accept a descriptor borrowed from another
    /// version: that would make the omitted trailing columns indistinguishable
    /// from an authored value and reintroduce partial-row sync semantics.
    fn malformed_authored_version_reason(&self, versions: &[VersionRecord]) -> Option<String> {
        for version in versions {
            let Some(schema) = self
                .catalogue
                .catalogue_schemas
                .get(&version.schema_version())
            else {
                continue;
            };
            let Some(table) = schema
                .schema
                .tables
                .iter()
                .find(|table| table.name == version.table())
            else {
                return Some(format!(
                    "row version table '{}' is absent from its authored schema",
                    version.table()
                ));
            };
            if version.record().descriptor() != &table.wire_record_descriptor() {
                return Some(format!(
                    "row version for table '{}' does not carry the complete descriptor of its authored schema",
                    version.table()
                ));
            }
        }
        None
    }

    /// Validate row versions carried by a view or repair payload before that
    /// payload may advance local receiver state. View payloads cannot park for
    /// a missing catalogue entry: unlike an authored commit unit, they have no
    /// protocol disposition that can defer a partial application of the frame.
    pub(super) fn validate_view_payload_versions(
        &self,
        versions: &[VersionRecord],
    ) -> Result<(), Error> {
        for version in versions {
            let schema = self
                .catalogue
                .catalogue_schemas
                .get(&version.schema_version())
                .ok_or(Error::MalformedViewUpdate(
                    "row version names an unknown authored schema",
                ))?;
            let table = schema
                .schema
                .tables
                .iter()
                .find(|table| table.name == version.table())
                .ok_or(Error::MalformedViewUpdate(
                    "row version table is absent from its authored schema",
                ))?;
            if version.record().descriptor() != &table.wire_record_descriptor() {
                return Err(Error::MalformedViewUpdate(
                    "row version does not carry the complete descriptor of its authored schema",
                ));
            }
        }
        Ok(())
    }

    fn reject_malformed_commit(
        &mut self,
        tx: Transaction,
        reason: String,
    ) -> Result<Vec<SyncMessage>, Error> {
        let fate = Fate::Rejected(RejectionReason::MalformedCommit(reason));
        self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
        let mut updates = vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate,
            global_seq: None,
            durability: None,
        }];
        updates.extend(self.cascade_rejections_from(tx.tx_id)?);
        Ok(updates)
    }

    /// Ensure every known authored schema named by an arriving commit has a
    /// local alias and registered shared-storage variant. Unknown schemas stay
    /// parked until their catalogue lineage arrives and re-enters this path.
    fn prepare_authored_schema_variants_for_commit(
        &mut self,
        versions: &[VersionRecord],
    ) -> Result<(), Error> {
        if self.malformed_authored_version_reason(versions).is_some() {
            return Err(Error::InvalidStoredValue(
                "wire version record does not match authored schema",
            ));
        }
        if versions.iter().any(|version| {
            !self
                .catalogue
                .catalogue_schemas
                .contains_key(&version.schema_version())
        }) {
            return Ok(());
        }

        let authored_variants = versions
            .iter()
            .map(|version| (version.table().to_owned(), version.schema_version()))
            .collect::<BTreeSet<_>>();
        let mut registered_mapping = false;
        for (table, schema_version) in authored_variants {
            self.table_in_schema(&table, schema_version)?;
            registered_mapping |= !self
                .catalogue
                .schema_version_aliases
                .contains_key(&schema_version)
                || !self
                    .catalogue
                    .physical_mappings
                    .contains_key(&schema_version);
            self.ensure_schema_version_alias(schema_version)?;
        }
        if registered_mapping {
            self.synchronize_physical_version_tables()?;
        }
        Ok(())
    }

    pub(super) fn ingest_rejected_transaction(
        &mut self,
        tx: Transaction,
        fate: Fate,
    ) -> Result<(), Error> {
        if self.query_transaction(tx.tx_id)?.is_some() {
            return self.apply_fate_update(tx.tx_id, fate, None, None);
        }
        let tx_node_alias = self.ensure_node_alias(tx.tx_id.node)?;
        let mut batch = self.database.open_batch();
        batch.insert(
            "jazz_transactions",
            transaction_values(
                tx_node_alias,
                &tx,
                fate.clone(),
                None,
                DurabilityTier::Local,
            ),
        );
        self.database.commit_batch(batch)?;
        Ok(())
    }
}

/// A sequence is the global-authority receipt. Peer payloads which pair it
/// with a weaker durability must be rejected before they can reach storage.
pub(super) fn validate_received_fate_update_global_seq_durability(
    global_seq: Option<GlobalSeq>,
    durability: Option<DurabilityTier>,
) -> Result<(), Error> {
    if global_seq.is_some() && durability != Some(DurabilityTier::Global) {
        return Err(Error::UnsupportedSyncMessage(
            "global sequence requires Global durability",
        ));
    }
    Ok(())
}

/// View bundles are peer payloads too, including reset bundles eligible for
/// bulk persistence.
pub(super) fn validate_received_view_bundle_global_seq_durability(
    global_seq: Option<GlobalSeq>,
    durability: DurabilityTier,
) -> Result<(), Error> {
    if global_seq.is_some() && durability != DurabilityTier::Global {
        return Err(Error::MalformedViewUpdate(
            "global sequence requires Global durability",
        ));
    }
    Ok(())
}

fn validate_transform_column(column: Option<&ColumnSchema>, transform: &str) -> Result<(), Error> {
    validate_registered_transform(transform)?;
    let Some(_) = column else {
        return Err(Error::InvalidCatalogueUpdate("transform column is unknown"));
    };
    Ok(())
}

fn fate_update_durability_claim(fate: &Fate, durability: DurabilityTier) -> Option<DurabilityTier> {
    match fate {
        Fate::Rejected(_) => None,
        Fate::Pending | Fate::Accepted => Some(durability),
    }
}

fn commit_unit_write_count_matches(tx: &Transaction, version_count: usize) -> bool {
    usize::try_from(tx.n_total_writes) == Ok(version_count)
}

fn view_version_key_for_ingest(version: &VersionRecord) -> (String, RowUuid, VersionLayer) {
    (
        version.table().to_owned(),
        version.row_uuid(),
        VersionLayer::for_record(version),
    )
}

fn content_version_reaches_tx_in_staged_parents(
    start: TxId,
    target: TxId,
    parents_by_tx: &BTreeMap<TxId, Vec<TxId>>,
) -> Option<bool> {
    if !parents_by_tx.contains_key(&start) {
        return None;
    }
    let mut stack = vec![start];
    let mut seen = BTreeSet::new();
    while let Some(tx_id) = stack.pop() {
        if tx_id == target {
            return Some(true);
        }
        if !seen.insert(tx_id) {
            continue;
        }
        let Some(parents) = parents_by_tx.get(&tx_id) else {
            continue;
        };
        stack.extend(parents.iter().copied());
    }
    Some(false)
}

fn counter_merge_value(
    table_schema: &TableSchema,
    column: &str,
    row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    tx_ids: &[TxId],
    memo: &mut BTreeMap<Vec<TxId>, i128>,
) -> Result<i128, Error> {
    let mut key = tx_ids.to_vec();
    key.sort();
    key.dedup();
    key = counter_head_tx_ids(row_versions_by_tx, &key);
    if key.is_empty() {
        return Ok(0);
    }
    if let Some(value) = memo.get(&key) {
        return Ok(*value);
    }

    let parent_union = key
        .iter()
        .map(|tx_id| {
            row_versions_by_tx
                .get(tx_id)
                .ok_or(Error::MissingTransaction(*tx_id))
        })
        .collect::<Result<Vec<_>, Error>>()?
        .into_iter()
        .flat_map(VersionRow::parents)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut merged = counter_merge_value(
        table_schema,
        column,
        row_versions_by_tx,
        &parent_union,
        memo,
    )?;

    for tx_id in &key {
        let version = row_versions_by_tx
            .get(tx_id)
            .ok_or(Error::MissingTransaction(*tx_id))?;
        let Some(value) = version.cell(table_schema, column)? else {
            continue;
        };
        let parent_value = counter_merge_value(
            table_schema,
            column,
            row_versions_by_tx,
            &version.parents(),
            memo,
        )?;
        merged += counter_value_to_i128(&value)? - parent_value;
    }
    memo.insert(key, merged);
    Ok(merged)
}

fn counter_head_tx_ids(
    row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    tx_ids: &[TxId],
) -> Vec<TxId> {
    let present = tx_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut dominated = BTreeSet::new();
    for tx_id in tx_ids {
        let Some(version) = row_versions_by_tx.get(tx_id) else {
            continue;
        };
        let mut stack = version.parents();
        let mut seen = BTreeSet::new();
        while let Some(parent) = stack.pop() {
            if !seen.insert(parent) {
                continue;
            }
            if present.contains(&parent) {
                dominated.insert(parent);
            }
            if let Some(parent_version) = row_versions_by_tx.get(&parent) {
                stack.extend(parent_version.parents());
            }
        }
    }
    tx_ids
        .iter()
        .copied()
        .filter(|tx_id| !dominated.contains(tx_id))
        .collect()
}

/// Merge every array value reachable from the current heads.  A GSet is
/// deliberately history-based rather than last-write-based: omitting an
/// element in a later write cannot remove an element introduced by any parent.
/// Elements are keyed and ordered by Groove's deterministic record encoding;
/// this preserves distinct valid float bit patterns such as `+0.0` and `-0.0`.
fn gset_merge_value(
    table_schema: &TableSchema,
    column: &str,
    row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    head_tx_ids: &[TxId],
) -> Result<Value, Error> {
    let column_schema = table_schema
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .ok_or(Error::InvalidStoredValue(
            "g-set column is missing from schema",
        ))?;
    let ValueType::Array(element_type) = &column_schema.column_type else {
        return Err(Error::InvalidStoredValue(
            "g-set merge strategy requires an array column",
        ));
    };
    let element_descriptor =
        records::RecordDescriptor::new([("element", element_type.as_ref().clone())]);

    let mut pending = head_tx_ids.to_vec();
    let mut visited = BTreeSet::new();
    let mut elements = BTreeMap::<Vec<u8>, Value>::new();
    while let Some(tx_id) = pending.pop() {
        if !visited.insert(tx_id) {
            continue;
        }
        let version = row_versions_by_tx
            .get(&tx_id)
            .ok_or(Error::MissingTransaction(tx_id))?;
        pending.extend(version.parents());
        let Some(Value::Array(values)) = version.cell(table_schema, column)? else {
            continue;
        };
        for value in values {
            let key = element_descriptor.create(std::slice::from_ref(&value))?;
            elements.entry(key).or_insert(value);
        }
    }
    Ok(Value::Array(elements.into_values().collect()))
}

/// A linear write is materialized only when its GSet cells differ from the
/// union of their ancestry. This prevents a no-op merge version from chaining
/// forever while making an attempted removal immediately restore prior values.
fn gset_cells_need_materialization(
    table_schema: &TableSchema,
    head: &VersionRow,
    merged_cells: &BTreeMap<String, Value>,
) -> Result<bool, Error> {
    for column in table_schema
        .columns
        .iter()
        .filter(|column| table_schema.merge_strategy(&column.name) == MergeStrategy::GSet)
    {
        let Some(current) = head.cell(table_schema, &column.name)? else {
            return Ok(true);
        };
        let Some(merged) = merged_cells.get(&column.name) else {
            return Ok(true);
        };
        let descriptor = records::RecordDescriptor::new([("cell", column.column_type.clone())]);
        if descriptor.create(std::slice::from_ref(&current))?
            != descriptor.create(std::slice::from_ref(merged))?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn raw_merge_head_tx_ids(
    row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    tx_ids: &[TxId],
) -> Result<Vec<TxId>, Error> {
    let mut raw = BTreeSet::new();
    let mut stack = tx_ids.to_vec();
    while let Some(tx_id) = stack.pop() {
        let version = row_versions_by_tx
            .get(&tx_id)
            .ok_or(Error::MissingTransaction(tx_id))?;
        let parents = version.parents();
        if parents.len() >= 2 {
            stack.extend(parents);
        } else {
            raw.insert(tx_id);
        }
    }
    Ok(counter_head_tx_ids(
        row_versions_by_tx,
        &raw.into_iter().collect::<Vec<_>>(),
    ))
}

fn counter_value_to_i128(value: &Value) -> Result<i128, Error> {
    match value {
        Value::U8(value) => Ok(i128::from(*value)),
        Value::U16(value) => Ok(i128::from(*value)),
        Value::U32(value) => Ok(i128::from(*value)),
        Value::U64(value) => Ok(i128::from(*value)),
        Value::I32(value) => Ok(i128::from(*value)),
        Value::I64(value) => Ok(i128::from(*value)),
        _ => Err(Error::InvalidStoredValue("counter value must be integer")),
    }
}

fn counter_value_from_i128(
    column_type: &groove::schema::ColumnType,
    value: i128,
) -> Result<Value, Error> {
    match column_type {
        groove::schema::ColumnType::U8 => u8::try_from(value)
            .map(Value::U8)
            .map_err(|_| Error::InvalidStoredValue("counter value out of range")),
        groove::schema::ColumnType::U16 => u16::try_from(value)
            .map(Value::U16)
            .map_err(|_| Error::InvalidStoredValue("counter value out of range")),
        groove::schema::ColumnType::U32 => u32::try_from(value)
            .map(Value::U32)
            .map_err(|_| Error::InvalidStoredValue("counter value out of range")),
        groove::schema::ColumnType::U64 => u64::try_from(value)
            .map(Value::U64)
            .map_err(|_| Error::InvalidStoredValue("counter value out of range")),
        groove::schema::ColumnType::I32 => i32::try_from(value)
            .map(Value::I32)
            .map_err(|_| Error::InvalidStoredValue("counter value out of range")),
        groove::schema::ColumnType::I64 => i64::try_from(value)
            .map(Value::I64)
            .map_err(|_| Error::InvalidStoredValue("counter value out of range")),
        _ => Err(Error::InvalidStoredValue(
            "counter strategy requires integer column",
        )),
    }
}

fn branch_metadata_available<S: OrderedKvStorage>(node: &NodeState<S>, tx: &Transaction) -> bool {
    match tx.target_lineage {
        crate::tx::BranchLineage::Root => true,
        crate::tx::BranchLineage::Branch(branch) => node.branches.branches.contains_key(&branch),
    }
}
