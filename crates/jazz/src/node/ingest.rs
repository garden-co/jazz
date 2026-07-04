//! Commit, fate, catalogue, and sync-message ingestion for a storage-backed
//! node. This module owns mutation paths that validate incoming transactions,
//! apply authority fates, park/unpark causally blocked units, and write node
//! state into groove; read-only global derivations live in [`super::global_state`],
//! policy evaluation in [`super::policy`], and byte-level record construction in
//! [`super::codec`]. It is the node layer's write side below the `Db` facade and
//! protocol sync loop.

use super::*;
use crate::merge_strategy::{MergeSide, MergeStrategyInput, materialize_strategy_output};
use crate::protocol::{CatalogueAck, ContentExtent, LensOp, VersionBundle};
use crate::protocol_limits::{
    commit_unit_limit_violation, validate_content_extents, validate_known_state_declaration,
    validate_shape_ast_size,
};
use crate::schema::LargeValueKind;
use crate::schema::{ColumnSchema, MERGE_HEADS_TABLE, no_text_merge_spec_hash};
use crate::text_merge::{
    EventId as TextEventId, TextEvent, TextEventGraph, TieBreak as TextTieBreak,
};
use crate::time::TxTimeSortKey;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct CommitUnitParkMode {
    ingest_context: Option<CommitUnitIngestContext>,
    edge_authority_mergeable: bool,
    edge_accepted_mergeable: bool,
}

struct LargeValueMergeCell {
    value: Value,
    strategy: RecordedMergeStrategy,
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Apply bulk-lane content extent payloads and drain any parked units whose
    /// text-op refs are now locally readable.
    pub fn apply_content_extents(
        &mut self,
        extents: Vec<ContentExtent>,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        for content in extents {
            if content.bytes.len() > crate::protocol_limits::MAX_CONTENT_EXTENT_BYTES {
                return Err(Error::UnsupportedSyncMessage(
                    "content extent exceeds byte limit",
                ));
            }
            self.content_store()
                .put_extent(&content.extent, &content.bytes)?;
        }
        self.drain_parked_commit_units()
    }

    /// True when this extent is named by a visible op-log version for `row`.
    pub fn content_extent_visible_to(
        &mut self,
        row: RowUuid,
        extent: &content_store::Extent,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        if extent.row != row {
            return Ok(false);
        }
        for tx_id in self.transaction_ids()? {
            for version in self.query_versions_for_tx(tx_id)? {
                if version.row_uuid() != row || version.layer() != VersionLayer::Content {
                    continue;
                }
                let table_name = version.table().to_owned();
                if !self.content_extent_owner_visible_to(&table_name, row, identity)? {
                    continue;
                }
                let table = self.table(&table_name)?.clone();
                if self.version_references_content_extent(&table, &version, extent)? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn content_extent_owner_visible_to(
        &mut self,
        table_name: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        let shape = crate::query::Query::from(table_name)
            .filter(crate::query::eq(
                crate::query::col("id"),
                crate::query::lit(Value::Uuid(row.0)),
            ))
            .validate(&self.catalogue.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        Ok(!self
            .query_rows_for_link(&shape, &binding, DurabilityTier::Global, identity)?
            .is_empty())
    }

    /// Apply one sync message and return any outgoing sync messages.
    pub fn apply_sync_message(&mut self, message: SyncMessage) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.apply_sync_message_with_ingest_context(message, None)
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
        self.apply_sync_message_with_ingest_context_and_encoded_len(message, ingest_context, None)
    }

    pub(crate) fn apply_sync_message_with_ingest_context_and_encoded_len(
        &mut self,
        message: SyncMessage,
        ingest_context: Option<CommitUnitIngestContext>,
        encoded_len: Option<usize>,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        match message {
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
                encoded_len,
            ),
            SyncMessage::FateUpdate {
                tx_id,
                fate,
                global_seq,
                durability,
            } => {
                self.apply_fate_update(tx_id, fate, global_seq, durability)?;
                self.drain_parked_commit_units()
            }
            SyncMessage::ViewUpdate {
                subscription,
                settled_through,
                reset_result_set,
                version_bundles,
                peer_payload_inventory,
                result_member_adds,
                result_member_removes,
                program_fact_adds,
                program_fact_removes,
            } => {
                self.apply_view_update(ViewUpdateParts {
                    subscription,
                    settled_through,
                    reset_result_set,
                    version_bundles,
                    peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
                    result_member_adds,
                    result_member_removes,
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
            SyncMessage::Subscribe(subscribe) => {
                validate_known_state_declaration(&subscribe.known_state).map_err(|_| {
                    Error::UnsupportedSyncMessage("known-state declaration exceeds limit")
                })?;
                self.apply_subscribe(subscribe)?;
                Ok(Vec::new())
            }
            SyncMessage::SubscribeRejected { .. } => Ok(Vec::new()),
            SyncMessage::Unsubscribe { subscription } => {
                self.apply_unsubscribe(subscription);
                Ok(Vec::new())
            }
            SyncMessage::PublishSchema { author, schema } => {
                self.apply_publish_schema(author, *schema)
            }
            SyncMessage::PublishLens { author, lens } => self.apply_publish_lens(author, lens),
            SyncMessage::SetCurrentWriteSchema { author, pointer } => {
                self.apply_set_current_write_schema(author, pointer)
            }
            SyncMessage::CatalogueAck(_) => Ok(Vec::new()),
            SyncMessage::FetchContentExtent { .. } => {
                Err(Error::UnsupportedSyncMessage("content extent fetch"))
            }
            SyncMessage::ContentExtents { extents } => {
                validate_content_extents(&extents).map_err(|_| {
                    Error::UnsupportedSyncMessage("content extent exceeds byte limit")
                })?;
                self.apply_content_extents(extents)
            }
        }
    }

    fn apply_publish_schema(
        &mut self,
        author: AuthorId,
        schema: SchemaVersion,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_admin(author)?;
        if schema.id != schema.schema.version_id() {
            return Err(Error::InvalidCatalogueUpdate(
                "schema id does not match schema payload",
            ));
        }
        self.catalogue
            .catalogue_schemas
            .insert(schema.id, schema.clone());
        if schema.id == self.catalogue.current_schema_version_id {
            self.catalogue.schema = schema.schema.clone();
            self.query.current_row_graphs = current_row_graphs(&self.catalogue.schema);
        }
        self.persist_catalogue_schema(&schema)?;
        self.ensure_schema_version_alias(schema.id)?;
        if schema.id != self.catalogue.current_schema_version_id
            && self.parking.parked_commit_units.values().any(|parked| {
                parked
                    .versions
                    .iter()
                    .any(|version| version.schema_version() == schema.id)
            })
        {
            let mut added_partition = false;
            for table in &schema.schema.tables {
                added_partition |= self.persist_partition(table.name.clone(), schema.id)?;
            }
            if added_partition {
                self.rebuild_database_slot()?;
            }
        }
        let updates = self.drain_parked_commit_units()?;
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

    fn apply_publish_lens(
        &mut self,
        author: AuthorId,
        lens: MigrationLens,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.require_catalogue_admin(author)?;
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
        self.catalogue
            .catalogue_lenses
            .entry(lens.id)
            .or_insert(lens.clone());
        self.catalogue.lens_path_cache.clear();
        self.catalogue.compiled_lens_cache.clear();
        self.persist_catalogue_lens(&lens)?;
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
        pointer: CurrentWriteSchema,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_admin(author)?;
        if !self
            .catalogue
            .catalogue_schemas
            .contains_key(&pointer.schema)
        {
            return Err(Error::InvalidCatalogueUpdate(
                "current write schema is unknown",
            ));
        }
        let applied = pointer.revision > self.catalogue.current_write_schema.revision;
        if applied {
            self.catalogue.current_write_schema = pointer;
            self.persist_catalogue_pointer(pointer)?;
            let active_schema = self
                .catalogue
                .catalogue_schemas
                .get(&pointer.schema)
                .ok_or(Error::InvalidStoredValue(
                    "current write schema payload missing",
                ))?;
            if pointer.schema == self.catalogue.current_schema_version_id {
                self.catalogue.schema = active_schema.schema.clone();
                self.query.current_row_graphs = current_row_graphs(&self.catalogue.schema);
            }
            let tables = active_schema
                .schema
                .tables
                .iter()
                .map(|table| table.name.clone())
                .collect::<Vec<_>>();
            let mut added_partition = false;
            for table in tables {
                added_partition |= self.persist_partition(table, pointer.schema)?;
            }
            if added_partition {
                self.rebuild_database_slot()?;
            }
        }
        Ok(vec![SyncMessage::CatalogueAck(CatalogueAck {
            revision: Some(pointer.revision),
            schema: Some(pointer.schema),
            lens: None,
            applied,
        })])
    }

    fn require_catalogue_admin(&self, author: AuthorId) -> Result<(), Error> {
        if author == AuthorId::SYSTEM {
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
            for op in &table_lens.ops {
                match op {
                    LensOp::RenameTable { .. } => {}
                    LensOp::RenameColumn { from, to } => {
                        if let Some(mut column) = columns.remove(from) {
                            column.name = to.clone();
                            columns.insert(to.clone(), column);
                        }
                    }
                    LensOp::CopyColumn { from, to } => {
                        if let Some(mut column) = columns.get(from).cloned() {
                            column.name = to.clone();
                            columns.insert(to.clone(), column);
                        }
                    }
                    LensOp::AddColumn { column, .. } => {
                        if let Some(target_column) = target_table
                            .columns
                            .iter()
                            .find(|candidate| candidate.name == *column)
                            .cloned()
                        {
                            columns.insert(column.clone(), target_column);
                        }
                    }
                    LensOp::DropColumn { column, .. } => {
                        columns.remove(column);
                    }
                    LensOp::TransformColumn { column, transform } => {
                        validate_transform_column(columns.get(column), transform)?;
                    }
                    LensOp::RejectSourceDelta { .. } => {}
                }
            }
        }
        Ok(())
    }

    /// Ingest a commit unit as fate authority.
    pub fn ingest_commit_unit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.ingest_commit_unit_with_context(tx, versions, now_ms, None, None)
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
        encoded_len: Option<usize>,
    ) -> Result<Vec<SyncMessage>, Error> {
        if let Some(reason) = commit_unit_limit_violation(&tx, &versions, encoded_len) {
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
    ) -> Result<Vec<SyncMessage>, Error> {
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
    ) -> Result<Vec<SyncMessage>, Error> {
        let ingest_context = Some(CommitUnitIngestContext {
            identity,
            trust: CommitUnitTrust::TrustedBackend,
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
    /// stored versions, so it is large-value-safe and does not re-run the
    /// authority validation the node already performed when it authored the
    /// commit. Idempotent: a non-pending transaction is left untouched.
    pub fn finalize_local_mergeable_commit(&mut self, tx_id: TxId) -> Result<(), Error> {
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
            .remove(&tx_id)
            .unwrap_or(stored.tx.made_by);
        for version in &records {
            if !self.version_satisfies_write_policy(version, permission_subject) {
                let fate = Fate::Rejected(RejectionReason::AuthorizationDenied);
                self.ingest_rejected_transaction(stored.tx, fate)?;
                return Ok(());
            }
        }
        let global_seq = self.clock.next_global_seq;
        self.clock.next_global_seq = self.clock.next_global_seq.next();
        self.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(global_seq),
            Some(DurabilityTier::Global),
        )?;
        self.create_merge_versions_for(&records)?;
        self.checkpoint_large_values_for_tx(tx_id)?;
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
        let global_seq = self.clock.next_global_seq;
        self.clock.next_global_seq = self.clock.next_global_seq.next();
        self.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(global_seq),
            Some(DurabilityTier::Global),
        )?;
        self.create_merge_versions_for(&versions)?;
        self.checkpoint_large_values_for_tx(tx_id)?;
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
        if let Some(reason) = commit_unit_limit_violation(&tx, &versions, None) {
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
        if self.park_commit_unit_if_missing_schema_versions_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                edge_accepted_mergeable: true,
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
                edge_accepted_mergeable: true,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        if self.park_commit_unit_if_missing_content_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                edge_accepted_mergeable: true,
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
        if let Some(reason) = self.reject_source_delta_reason(&versions) {
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

        let global_seq = self.clock.next_global_seq;
        self.clock.next_global_seq = self.clock.next_global_seq.next();
        let fate = Fate::Accepted;
        let durability = DurabilityTier::Global;
        let merge_rows = merge_rows_for_versions(&versions);
        self.ingest_known_transaction(
            tx.clone(),
            versions,
            fate.clone(),
            Some(global_seq),
            durability,
        )?;
        debug_assert_eq!(self.clock.applied_global_watermark, global_seq);
        self.create_merge_versions_for_rows(merge_rows)?;
        self.checkpoint_large_values_for_tx(tx.tx_id)?;
        Ok(vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate,
            global_seq: Some(global_seq),
            durability: Some(durability),
        }])
    }

    /// Ingest a commit unit as a relay without assigning fate.
    pub fn ingest_relay_commit_unit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
    ) -> Result<(), Error> {
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
        if self.park_commit_unit_if_missing_schema_versions(
            &tx,
            &versions,
            u64::MAX - SKEW_TOLERANCE_MS,
        )? {
            return Ok(());
        }

        let mut memo = IngestMemo::default();
        if self.park_commit_unit_if_missing_parents(
            &tx,
            &versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            &mut memo,
        )? {
            return Ok(());
        }
        if self.park_commit_unit_if_missing_content(&tx, &versions, u64::MAX - SKEW_TOLERANCE_MS)? {
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
        if self.park_commit_unit_if_missing_content_with_mode(
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
        if let Some(reason) = self.reject_source_delta_reason(&versions) {
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
        let global_seq = self.clock.next_global_seq;
        self.clock.next_global_seq = self.clock.next_global_seq.next();
        let fate = Fate::Accepted;
        let durability = DurabilityTier::Global;
        let merge_rows = merge_rows_for_versions(&versions);
        self.ingest_known_transaction(
            tx.clone(),
            versions,
            fate.clone(),
            Some(global_seq),
            durability,
        )?;
        debug_assert_eq!(self.clock.applied_global_watermark, global_seq);
        self.create_merge_versions_for_rows(merge_rows)?;
        self.checkpoint_large_values_for_tx(tx.tx_id)?;
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
        if let Some(reason) = commit_unit_limit_violation(&tx, &versions, None) {
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
        if self.park_commit_unit_if_missing_schema_versions_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingest_context,
                edge_authority_mergeable: true,
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
                ingest_context,
                edge_authority_mergeable: true,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        if self.park_commit_unit_if_missing_content_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingest_context,
                edge_authority_mergeable: true,
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
        if let Some(reason) = self.reject_source_delta_reason(&versions) {
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
        self.checkpoint_large_values_for_tx(tx.tx_id)?;
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
        self.merge_tx_time(tx.tx_id.time);
        let versions = canonical_versions(versions);
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
        self.merge_tx_time(tx.tx_id.time);
        let versions = canonical_versions(versions);
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

    pub(super) fn ingest_reset_view_bundles_in_bulk(
        &mut self,
        bundles: &[VersionBundle],
    ) -> Result<BTreeSet<TxId>, Error> {
        let mut eligible = Vec::new();
        let mut loaded_tx_ids = BTreeSet::new();
        for bundle in bundles {
            if bundle.fate != Fate::Accepted || bundle.global_seq.is_none() {
                continue;
            }
            if bundle.tx.kind != TxKind::Mergeable && bundle.tx.kind != TxKind::Exclusive {
                continue;
            }
            if bundle.tx.kind == TxKind::Exclusive
                && usize::try_from(bundle.tx.n_total_writes).ok() != Some(bundle.versions.len())
            {
                continue;
            }
            if self.query_transaction(bundle.tx.tx_id)?.is_some() {
                continue;
            }
            if loaded_tx_ids.insert(bundle.tx.tx_id) {
                eligible.push(bundle);
            }
        }
        if eligible.is_empty() {
            return Ok(loaded_tx_ids);
        }
        self.sync_metrics.receiver_bulk_ingest_commits += 1;
        self.sync_metrics.receiver_bulk_bundle_ingests += eligible.len() as u64;

        let mut batch = self.database.open_batch();
        let version_count = eligible
            .iter()
            .map(|bundle| bundle.versions.len())
            .sum::<usize>();
        batch.reserve(eligible.len() + version_count.saturating_mul(2));
        let mut current_updates =
            BTreeMap::<(String, RowUuid, VersionLayer), (VersionRow, GlobalSeq)>::new();
        let mut content_versions = Vec::new();
        #[cfg(test)]
        let mut content_rows = BTreeSet::<(String, RowUuid)>::new();
        let mut applied_global_seqs = Vec::with_capacity(eligible.len());

        for bundle in eligible {
            let tx = &bundle.tx;
            let tx_node_alias = self.ensure_node_alias(tx.tx_id.node)?;
            let global_seq = bundle.global_seq.expect("checked above");
            applied_global_seqs.push(global_seq);
            batch.insert(
                "jazz_transactions",
                transaction_values(
                    tx_node_alias,
                    tx,
                    bundle.fate.clone(),
                    bundle.global_seq,
                    bundle.durability,
                ),
            );

            let mut versions = bundle.versions.iter().collect::<Vec<_>>();
            versions.sort();
            for version in versions {
                let author_schema = version.schema_version();
                let source_table_schema = self.table_in_schema(version.table(), author_schema)?;
                let has_forward_lens = author_schema != self.catalogue.current_write_schema.schema
                    && self.has_forward_lens_path(
                        author_schema,
                        self.catalogue.current_write_schema.schema,
                    );
                let (table_schema, target_schema, stored) = if has_forward_lens {
                    let mut target_table = version.table().to_owned();
                    let mut target_cells = source_table_schema
                        .columns
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, column)| {
                            version
                                .optional_cell_at(idx)
                                .map(|value| (column.name.clone(), value))
                        })
                        .collect::<BTreeMap<_, _>>();
                    let target_schema = self.catalogue.current_write_schema.schema;
                    target_table = self.translate_cells_forward(
                        author_schema,
                        target_schema,
                        &target_table,
                        &mut target_cells,
                    )?;
                    let table_schema = self.table_in_schema(&target_table, target_schema)?;
                    let schema_version_alias = self.ensure_schema_version_alias(target_schema)?;
                    let stored = VersionRow::from_parts_with_schema_version(
                        &table_schema,
                        VersionRowParts {
                            table: target_table,
                            row_uuid: version.row_uuid(),
                            tx_node_alias,
                            schema_version_alias,
                            tx_time: tx.tx_id.time,
                            parents: version.parents(),
                            created_by: version.created_by(),
                            created_at: version.created_at(),
                            updated_by: version.updated_by(),
                            updated_at: version.updated_at(),
                            cells: target_cells,
                            deletion: version.deletion(),
                        },
                        (target_schema != self.catalogue.current_schema_version_id)
                            .then_some(target_schema),
                    )?;
                    (table_schema, target_schema, stored)
                } else {
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
                    (source_table_schema, author_schema, stored)
                };
                let history_table = version_storage_table_name_for_schema(
                    &table_schema.name,
                    stored.layer(),
                    target_schema,
                    self.catalogue.current_schema_version_id,
                );
                batch.insert_raw(
                    history_table,
                    history_primary_key(&stored),
                    stored.record.raw().to_vec(),
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
        #[cfg(test)]
        {
            self.assert_merge_head_rows_match_history_for_test(&content_rows)?;
            self.assert_global_current_updates_match_history_for_test(&current_update_versions)?;
        }
        for bundle in bundles {
            if !loaded_tx_ids.contains(&bundle.tx.tx_id) {
                continue;
            }
            self.invalidate_tx_version_tables_cache(bundle.tx.tx_id);
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

        let mut batch = self.database.open_batch();
        let mut global_current_updates = Vec::new();
        let cleanup_rejected_versions = matches!(stored.fate, Fate::Rejected(_));
        let content_versions = self
            .query_versions_for_tx(tx_id)?
            .into_iter()
            .filter(|version| version.layer() == VersionLayer::Content)
            .collect::<Vec<_>>();
        if matches!(stored.fate, Fate::Accepted) && stored.global_seq.is_some() {
            global_current_updates = self.global_current_updates(tx_id)?;
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
        if !matches!(stored.fate, Fate::Rejected(_)) {
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
        if matches!(stored.fate, Fate::Rejected(_)) || stored.global_seq.is_some() {
            self.cleanup_fated_ahead_current_for_tx(&mut batch, tx_id)?;
        }
        for global_seq in advanced_global_seqs
            .iter()
            .copied()
            .filter(|global_seq| Some(*global_seq) != stored.global_seq)
        {
            self.prune_ahead_current_for_global_seq(&mut batch, global_seq)?;
        }
        let rejected_payload = if cleanup_rejected_versions {
            self.remove_rejected_local_versions(tx_id, &stored, &mut batch)?
        } else {
            None
        };
        self.database.commit_batch(batch)?;
        #[cfg(test)]
        {
            let rows = content_versions
                .iter()
                .map(|version| (version.table().to_owned(), version.row_uuid()))
                .collect::<BTreeSet<_>>();
            self.assert_merge_head_rows_match_history_for_test(&rows)?;
            self.assert_global_current_updates_match_history_for_test(
                &global_current_update_versions,
            )?;
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
            self.checkpoint_large_values_for_tx(tx_id)?;
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
        for version in versions {
            if !self.version_satisfies_write_policy(version, permission_subject) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn version_satisfies_write_policy(
        &mut self,
        version: &VersionRecord,
        author: AuthorId,
    ) -> bool {
        match self.write_policy_allows_version_record(version, author) {
            Ok(allowed) => allowed,
            Err(_) => false,
        }
    }

    pub(super) fn cascade_root_for_versions(&mut self, versions: &[VersionRecord]) -> Option<TxId> {
        for parent in versions.iter().flat_map(|version| version.parents()) {
            if let Some(root) = self.cascade_root_for_tx(parent) {
                return Some(root);
            }
        }
        None
    }

    pub(super) fn park_commit_unit_if_missing_parents(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        now_ms: u64,
        memo: &mut IngestMemo,
    ) -> Result<bool, Error> {
        self.park_commit_unit_if_missing_parents_with_mode(
            tx,
            versions,
            now_ms,
            memo,
            CommitUnitParkMode::default(),
        )
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
            existing.edge_authority_mergeable |= mode.edge_authority_mergeable;
            existing.edge_accepted_mergeable |= mode.edge_accepted_mergeable;
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
                edge_authority_mergeable: mode.edge_authority_mergeable,
                edge_accepted_mergeable: mode.edge_accepted_mergeable,
            },
        );
        Ok(true)
    }

    pub(super) fn park_commit_unit_if_missing_schema_versions(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        now_ms: u64,
    ) -> Result<bool, Error> {
        self.park_commit_unit_if_missing_schema_versions_with_mode(
            tx,
            versions,
            now_ms,
            CommitUnitParkMode::default(),
        )
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
            existing.edge_authority_mergeable |= mode.edge_authority_mergeable;
            existing.edge_accepted_mergeable |= mode.edge_accepted_mergeable;
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
                edge_authority_mergeable: mode.edge_authority_mergeable,
                edge_accepted_mergeable: mode.edge_accepted_mergeable,
            },
        );
        Ok(true)
    }

    pub(super) fn park_commit_unit_if_missing_content(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        now_ms: u64,
    ) -> Result<bool, Error> {
        self.park_commit_unit_if_missing_content_with_mode(
            tx,
            versions,
            now_ms,
            CommitUnitParkMode::default(),
        )
    }

    pub(super) fn park_commit_unit_if_missing_content_with_mode(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        now_ms: u64,
        mode: CommitUnitParkMode,
    ) -> Result<bool, Error> {
        if self.missing_content_refs(versions)?.is_empty() {
            return Ok(false);
        }
        if let Some(existing) = self.parking.parked_commit_units.get_mut(&tx.tx_id) {
            if existing.tx != *tx || existing.versions != versions {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            if existing.ingest_context != mode.ingest_context {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            existing.edge_authority_mergeable |= mode.edge_authority_mergeable;
            existing.edge_accepted_mergeable |= mode.edge_accepted_mergeable;
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
                edge_authority_mergeable: mode.edge_authority_mergeable,
                edge_accepted_mergeable: mode.edge_accepted_mergeable,
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

    pub(super) fn missing_content_refs(
        &mut self,
        versions: &[VersionRecord],
    ) -> Result<BTreeSet<content_store::Extent>, Error> {
        let mut missing = BTreeSet::new();
        for extent in self.content_refs_in_version_records(versions)? {
            if !self.content_store().contains(&extent)? {
                missing.insert(extent);
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

    pub(super) fn drain_parked_commit_units(&mut self) -> Result<Vec<SyncMessage>, Error> {
        let mut updates = Vec::new();
        loop {
            let parked = self
                .parking
                .parked_commit_units
                .iter()
                .map(|(tx_id, unit)| (*tx_id, unit.versions.clone()))
                .collect::<Vec<_>>();
            let mut ready = Vec::new();
            for (tx_id, versions) in parked {
                if versions.iter().all(|version| {
                    self.catalogue
                        .catalogue_schemas
                        .contains_key(&version.schema_version())
                }) && self.missing_parent_refs(&versions)?.is_empty()
                    && self.missing_content_refs(&versions)?.is_empty()
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
                self.sync_metrics.parked_orphans_resolved += 1;
                if self.parking.parked_catalogue_commit_units.remove(&tx_id) {
                    self.sync_metrics.parked_catalogue_orphans_resolved += 1;
                }
                if unit.edge_accepted_mergeable {
                    updates.extend(self.finalize_edge_accepted_mergeable_commit_unit_once(
                        unit.tx,
                        unit.versions,
                        unit.now_ms,
                    )?);
                } else if unit.edge_authority_mergeable {
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

    pub(super) fn drain_parked_relay_commit_units(&mut self) -> Result<(), Error> {
        loop {
            let parked = self
                .parking
                .parked_commit_units
                .iter()
                .map(|(tx_id, unit)| (*tx_id, unit.versions.clone()))
                .collect::<Vec<_>>();
            let mut ready = Vec::new();
            for (tx_id, versions) in parked {
                if versions.iter().all(|version| {
                    self.catalogue
                        .catalogue_schemas
                        .contains_key(&version.schema_version())
                }) && self.missing_parent_refs(&versions)?.is_empty()
                    && self.missing_content_refs(&versions)?.is_empty()
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

    fn version_references_content_extent(
        &self,
        table: &TableSchema,
        version: &VersionRow,
        target: &content_store::Extent,
    ) -> Result<bool, Error> {
        for column in table
            .columns
            .iter()
            .filter(|column| column.large_value.is_some())
        {
            let Some(Value::Bytes(payload)) = version.cell(table, &column.name)? else {
                continue;
            };
            let extent_payload = match column.large_value {
                Some(LargeValueKind::Text) => {
                    let Some(extent_payload) = payload.strip_prefix(super::TEXT_EXTENT_OPS_MAGIC)
                    else {
                        continue;
                    };
                    extent_payload
                }
                Some(LargeValueKind::Blob) => &payload,
                None => continue,
            };
            for extent in content_refs_in_ops(text_oplog::decode(extent_payload)?) {
                if &extent == target {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub(crate) fn content_refs_in_sync_message(
        &self,
        message: &SyncMessage,
    ) -> Result<BTreeSet<content_store::Extent>, Error> {
        match message {
            SyncMessage::CommitUnit { versions, .. } => {
                self.content_refs_in_version_records(versions)
            }
            SyncMessage::ViewUpdate {
                version_bundles, ..
            }
            | SyncMessage::RowVersionPayloads {
                version_bundles, ..
            } => {
                let mut refs = BTreeSet::new();
                for bundle in version_bundles {
                    refs.extend(self.content_refs_in_version_records(&bundle.versions)?);
                }
                Ok(refs)
            }
            _ => Ok(BTreeSet::new()),
        }
    }

    fn content_refs_in_version_records(
        &self,
        versions: &[VersionRecord],
    ) -> Result<BTreeSet<content_store::Extent>, Error> {
        let mut refs = BTreeSet::new();
        for version in versions {
            let table = self.table_in_schema(version.table(), version.schema_version())?;
            for (idx, column) in table.columns.iter().enumerate() {
                if column.large_value.is_none() {
                    continue;
                }
                let Some(Value::Bytes(payload)) = version.optional_cell_at(idx) else {
                    continue;
                };
                match column.large_value {
                    Some(LargeValueKind::Text) => {
                        if let Some(extent_payload) =
                            payload.strip_prefix(super::TEXT_EXTENT_OPS_MAGIC)
                        {
                            refs.extend(content_refs_in_ops(text_oplog::decode(extent_payload)?));
                        }
                    }
                    Some(LargeValueKind::Blob) => {
                        refs.extend(content_refs_in_ops(text_oplog::decode(&payload)?));
                    }
                    None => {}
                }
            }
        }
        Ok(refs)
    }

    fn transaction_ids(&self) -> Result<Vec<TxId>, Error> {
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
            .map(|version| (version.table().to_owned(), version.row_uuid()))
            .collect::<BTreeSet<_>>();
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
                batch.insert(
                    rejected_versions_table_name(version.table()),
                    rejected_version_values.clone(),
                );
                rejected_versions.push(RejectedVersion::new(
                    version.table().to_owned(),
                    owned_record_from_storage_values(
                        &rejected_version_table,
                        rejected_version_values,
                    )?,
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
            batch.delete(
                version_storage_table_name(&version.table, version.layer()),
                history_primary_key(version),
            );
        }
        for (table, row_uuid) in affected_content_rows {
            self.rewrite_merge_heads_excluding_tx(batch, &table, row_uuid, tx_id)?;
        }
        self.invalidate_tx_version_tables_cache(tx_id);
        let _ = affected;
        Ok(rejected_payload)
    }

    pub(super) fn create_merge_versions_for(
        &mut self,
        records: &[VersionRecord],
    ) -> Result<(), Error> {
        self.create_merge_versions_for_rows(merge_rows_for_versions(records))
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
        let head_tx_ids = self.merge_head_tx_ids(table, row_uuid)?;
        if head_tx_ids.len() < 2 {
            return Ok(());
        }
        let table_schema = self.table(table)?.clone();
        let row_versions = self.query_row_versions(table, row_uuid)?;
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
        let (cells, recorded_strategy) =
            self.merge_cells_for_heads(&table_schema, &raw_heads, &row_versions_by_tx)?;
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
        let mut merge_commit = MergeableCommit::new(table, row_uuid, made_at.physical_ms())
            .parents(parents)
            .cells(cells);
        if let Some(strategy) = recorded_strategy {
            merge_commit = merge_commit.merge_strategy(strategy);
        }
        let merge_tx = self.commit_mergeable_at(merge_commit, made_at)?;
        let global_seq = self.clock.next_global_seq;
        self.clock.next_global_seq = self.clock.next_global_seq.next();
        self.apply_fate_update(
            merge_tx,
            Fate::Accepted,
            Some(global_seq),
            Some(DurabilityTier::Global),
        )?;
        debug_assert_eq!(self.clock.applied_global_watermark, global_seq);
        self.checkpoint_large_values_for_tx(merge_tx)?;
        Ok(())
    }

    fn merge_cells_for_heads(
        &mut self,
        table_schema: &TableSchema,
        heads: &[VersionRow],
        row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    ) -> Result<(BTreeMap<String, Value>, Option<RecordedMergeStrategy>), Error> {
        let mut cells = BTreeMap::new();
        let mut recorded_strategy = None;
        for column in &table_schema.columns {
            match table_schema.merge_strategy(&column.name) {
                MergeStrategy::Lww => {
                    if column.large_value.is_some()
                        && let Some(merged) = self.merge_large_value_cell_for_heads(
                            table_schema,
                            &column.name,
                            heads,
                            row_versions_by_tx,
                        )?
                    {
                        if recorded_strategy.is_none() {
                            recorded_strategy = Some(merged.strategy);
                        }
                        cells.insert(column.name.clone(), merged.value);
                        continue;
                    }
                    let mut best: Option<(crate::time::TxTimeSortKey, Value)> = None;
                    for version in heads {
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
            }
        }
        Ok((cells, recorded_strategy))
    }

    fn merge_large_value_cell_for_heads(
        &mut self,
        table_schema: &TableSchema,
        column: &str,
        heads: &[VersionRow],
        row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    ) -> Result<Option<LargeValueMergeCell>, Error> {
        let column_heads = heads
            .iter()
            .filter(|version| version.cell(table_schema, column).transpose().is_some())
            .map(|version| self.version_tx_id(version))
            .collect::<Result<Vec<_>, Error>>()?;
        if column_heads.len() < 2 {
            return Ok(None);
        }
        if column_large_value_kind(table_schema, column)? == LargeValueKind::Text {
            return self.merge_text_value_cell_for_heads(
                table_schema,
                column,
                column_heads,
                row_versions_by_tx,
            );
        }
        let keyed_column_heads = column_heads
            .into_iter()
            .map(|tx_id| {
                let made_at = self
                    .transaction_made_at(tx_id)?
                    .ok_or(Error::MissingTransaction(tx_id))?;
                Ok((made_at.sort_key(tx_id.node), tx_id))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let lca = self.large_value_lca(
            &keyed_column_heads
                .iter()
                .map(|(_, tx_id)| *tx_id)
                .collect::<Vec<_>>(),
            row_versions_by_tx,
        )?;
        let lca_value = match lca {
            Some(lca) => {
                let lca_version = row_versions_by_tx
                    .get(&lca)
                    .ok_or(Error::MissingTransaction(lca))?;
                self.materialize_large_value_column(table_schema, lca_version, column)?
            }
            None => Vec::new(),
        };
        let mut head_ops = Vec::new();
        for (key, head) in &keyed_column_heads {
            head_ops.push((
                *key,
                self.large_value_ops_since_lca(table_schema, column, *head, lca)?,
                self.large_value_merge_origin(*head)?,
            ));
        }
        let merged = merge_large_value_head_ops(&lca_value, head_ops);

        let primary = self.large_value_primary_head(
            &keyed_column_heads
                .iter()
                .map(|(_, tx_id)| *tx_id)
                .collect::<Vec<_>>(),
        )?;
        let primary_version = row_versions_by_tx
            .get(&primary)
            .ok_or(Error::MissingTransaction(primary))?;
        let primary_value =
            self.materialize_large_value_column(table_schema, primary_version, column)?;
        let ops = text_oplog::diff(&primary_value, &merged);
        let ops = self.extent_back_text_ops(
            AuthorId(self.node_uuid.0),
            primary_version.row_uuid(),
            column,
            ops,
        )?;
        Ok(Some(LargeValueMergeCell {
            value: Value::Bytes(text_oplog::encode(&ops)),
            strategy: RecordedMergeStrategy {
                id: "builtin.large-value-oplog-v1".to_owned(),
                version: 1,
                column_spec_hash: no_text_merge_spec_hash(),
            },
        }))
    }

    fn merge_text_value_cell_for_heads(
        &mut self,
        table_schema: &TableSchema,
        column: &str,
        column_heads: Vec<TxId>,
        row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    ) -> Result<Option<LargeValueMergeCell>, Error> {
        let keyed_column_heads = column_heads
            .into_iter()
            .map(|tx_id| {
                let made_at = self
                    .transaction_made_at(tx_id)?
                    .ok_or(Error::MissingTransaction(tx_id))?;
                Ok((made_at.sort_key(tx_id.node), tx_id))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let lca = self.large_value_lca(
            &keyed_column_heads
                .iter()
                .map(|(_, tx_id)| *tx_id)
                .collect::<Vec<_>>(),
            row_versions_by_tx,
        )?;
        let lca_value = match lca {
            Some(lca) => {
                let lca_version = row_versions_by_tx
                    .get(&lca)
                    .ok_or(Error::MissingTransaction(lca))?;
                self.materialize_large_value_column(table_schema, lca_version, column)?
            }
            None => Vec::new(),
        };

        let mut chains = Vec::new();
        let mut tie_tx_ids = BTreeSet::new();
        for (_, head) in &keyed_column_heads {
            let chain = self.plain_text_ops_since_lca(table_schema, column, *head, lca)?;
            for (tx_id, _) in &chain {
                tie_tx_ids.insert(*tx_id);
            }
            chains.push((*head, chain));
        }
        let tie_breaks = tie_tx_ids
            .into_iter()
            .enumerate()
            .map(|(idx, tx_id)| (tx_id, TextTieBreak((idx + 1) as u64)))
            .collect::<BTreeMap<_, _>>();

        let mut graph = TextEventGraph::default();
        let root = TextEventId(0);
        graph
            .insert(TextEvent {
                id: root,
                parents: Vec::new(),
                op: crate::text_merge::TextOp::identity(),
                tie_break: TextTieBreak(0),
            })
            .map_err(|_| Error::InvalidStoredValue("text merge graph insert failed"))?;

        let mut ids = BTreeMap::new();
        if let Some(lca) = lca {
            ids.insert(lca, root);
        }
        let mut next_id = 1u64;
        let mut heads = Vec::new();
        for (_, chain) in chains {
            let mut parent = root;
            for (tx_id, op) in chain {
                let id = *ids.entry(tx_id).or_insert_with(|| {
                    let id = TextEventId(next_id);
                    next_id += 1;
                    id
                });
                let _ = graph.insert(TextEvent {
                    id,
                    parents: vec![parent],
                    op,
                    tie_break: tie_breaks[&tx_id],
                });
                parent = id;
            }
            heads.push(parent);
        }
        let merged = graph
            .merge_heads(root, &lca_value, &heads)
            .map_err(|_| Error::InvalidStoredValue("text merge graph walk failed"))?;
        let mut recorded_strategy = RecordedMergeStrategy {
            id: "builtin.text-rle-v1".to_owned(),
            version: 1,
            column_spec_hash: table_schema
                .columns
                .iter()
                .find(|candidate| candidate.name == column)
                .and_then(|column| column.text_merge_spec.as_ref())
                .map_or_else(no_text_merge_spec_hash, |spec| spec.spec_hash()),
        };
        let merged = self.rung3_text_merge_if_triggered(
            table_schema,
            column,
            &keyed_column_heads,
            &lca_value,
            lca,
            row_versions_by_tx,
            merged,
            &mut recorded_strategy,
        )?;
        let primary = self.large_value_primary_head(
            &keyed_column_heads
                .iter()
                .map(|(_, tx_id)| *tx_id)
                .collect::<Vec<_>>(),
        )?;
        let primary_version = row_versions_by_tx
            .get(&primary)
            .ok_or(Error::MissingTransaction(primary))?;
        let primary_value =
            self.materialize_large_value_column(table_schema, primary_version, column)?;
        let mut merge_ops = vec![TextOp::Delete {
            pos: 0,
            len: primary_value.len(),
        }];
        if merged.len() <= crate::protocol_limits::MAX_CONTENT_EXTENT_BYTES {
            merge_ops.push(TextOp::Insert {
                pos: 0,
                content: TextContent::Inline(merged),
            });
        } else {
            merge_ops.extend(self.extent_back_text_ops(
                AuthorId(self.node_uuid.0),
                primary_version.row_uuid(),
                column,
                vec![TextOp::Insert {
                    pos: 0,
                    content: TextContent::Inline(merged),
                }],
            )?);
        }
        Ok(Some(LargeValueMergeCell {
            value: Value::Bytes(encode_extent_text_ops(&merge_ops)),
            strategy: recorded_strategy,
        }))
    }

    fn rung3_text_merge_if_triggered(
        &mut self,
        table_schema: &TableSchema,
        column: &str,
        keyed_column_heads: &[(TxTimeSortKey, TxId)],
        base: &[u8],
        lca: Option<TxId>,
        row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
        rung2_merged: Vec<u8>,
        recorded_strategy: &mut RecordedMergeStrategy,
    ) -> Result<Vec<u8>, Error> {
        let Some(column_schema) = table_schema
            .columns
            .iter()
            .find(|candidate| candidate.name == column)
        else {
            return Ok(rung2_merged);
        };
        let Some(spec) = column_schema.text_merge_spec.clone() else {
            return Ok(rung2_merged);
        };
        if keyed_column_heads.len() != 2 {
            return Ok(rung2_merged);
        }
        let Some(strategy) = self
            .text_merge_strategies
            .get(&(spec.strategy_id.clone(), spec.strategy_version))
            .cloned()
        else {
            return Ok(rung2_merged);
        };

        let mut ordered_heads = keyed_column_heads
            .iter()
            .map(|(_, tx_id)| *tx_id)
            .collect::<Vec<_>>();
        ordered_heads.sort();
        let left = self.merge_strategy_side(
            table_schema,
            column,
            ordered_heads[0],
            lca,
            row_versions_by_tx,
        )?;
        let right = self.merge_strategy_side(
            table_schema,
            column,
            ordered_heads[1],
            lca,
            row_versions_by_tx,
        )?;
        let input = MergeStrategyInput {
            schema_version: self.catalogue.current_write_schema.schema,
            table: table_schema.name.clone(),
            column: column.to_owned(),
            spec_hash: spec.spec_hash(),
            spec,
            base: base.to_vec(),
            left,
            right,
        };
        if !strategy.structural_proximity(&input) {
            return Ok(rung2_merged);
        }
        let Ok(output) = strategy.merge(&input) else {
            self.sync_metrics.rung3_text_merge_fallbacks = self
                .sync_metrics
                .rung3_text_merge_fallbacks
                .saturating_add(1);
            return Ok(rung2_merged);
        };
        if output.strategy_id != strategy.id() || output.strategy_version != strategy.version() {
            self.sync_metrics.rung3_text_merge_fallbacks = self
                .sync_metrics
                .rung3_text_merge_fallbacks
                .saturating_add(1);
            return Ok(rung2_merged);
        }
        let Ok(materialized) = materialize_strategy_output(&input, &output) else {
            self.sync_metrics.rung3_text_merge_fallbacks = self
                .sync_metrics
                .rung3_text_merge_fallbacks
                .saturating_add(1);
            return Ok(rung2_merged);
        };
        *recorded_strategy = RecordedMergeStrategy {
            id: output.strategy_id,
            version: output.strategy_version,
            column_spec_hash: input.spec_hash,
        };
        Ok(materialized)
    }

    fn merge_strategy_side(
        &mut self,
        table_schema: &TableSchema,
        column: &str,
        head: TxId,
        lca: Option<TxId>,
        row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    ) -> Result<MergeSide, Error> {
        let head_version = row_versions_by_tx
            .get(&head)
            .ok_or(Error::MissingTransaction(head))?;
        Ok(MergeSide {
            head,
            materialized: self.materialize_large_value_column(
                table_schema,
                head_version,
                column,
            )?,
            ops: self.plain_text_ops_since_lca(table_schema, column, head, lca)?,
        })
    }

    fn large_value_lca(
        &mut self,
        heads: &[TxId],
        row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    ) -> Result<Option<TxId>, Error> {
        let mut common: Option<BTreeSet<TxId>> = None;
        for head in heads {
            let ancestors = self.large_value_ancestors(*head, row_versions_by_tx)?;
            common = Some(match common {
                Some(common) => common.intersection(&ancestors).copied().collect(),
                None => ancestors,
            });
        }
        common
            .unwrap_or_default()
            .into_iter()
            .map(|tx_id| {
                let made_at = self
                    .transaction_made_at(tx_id)?
                    .ok_or(Error::MissingTransaction(tx_id))?;
                Ok((made_at.sort_key(tx_id.node), tx_id))
            })
            .collect::<Result<Vec<_>, Error>>()?
            .into_iter()
            .max_by_key(|(key, _)| *key)
            .map(|(_, tx_id)| tx_id)
            .map_or(Ok(None), |tx_id| Ok(Some(tx_id)))
    }

    fn large_value_ancestors(
        &mut self,
        head: TxId,
        row_versions_by_tx: &BTreeMap<TxId, VersionRow>,
    ) -> Result<BTreeSet<TxId>, Error> {
        let mut ancestors = BTreeSet::new();
        let mut stack = vec![head];
        while let Some(tx_id) = stack.pop() {
            if !ancestors.insert(tx_id) {
                continue;
            }
            let Some(version) = row_versions_by_tx.get(&tx_id) else {
                continue;
            };
            stack.extend(version.parents());
        }
        Ok(ancestors)
    }

    fn large_value_ops_since_lca(
        &mut self,
        table_schema: &TableSchema,
        column: &str,
        head: TxId,
        lca: Option<TxId>,
    ) -> Result<Vec<TextOp>, Error> {
        let mut chain = Vec::new();
        let mut current = Some(head);
        while let Some(tx_id) = current {
            if Some(tx_id) == lca {
                break;
            }
            let version = self
                .query_versions_for_tx(tx_id)?
                .into_iter()
                .find(|version| {
                    version.table() == table_schema.name && version.layer() == VersionLayer::Content
                })
                .ok_or(Error::MissingTransaction(tx_id))?;
            current = match version.parents().as_slice() {
                [] => None,
                [parent] => Some(*parent),
                parents => Some(self.large_value_primary_parent(parents)?),
            };
            chain.push(version);
        }
        chain.reverse();

        let mut ops = Vec::new();
        for version in chain {
            let Some(Value::Bytes(payload)) = version.cell(table_schema, column)? else {
                continue;
            };
            ops.extend(self.resolve_text_op_refs(text_oplog::decode(&payload)?)?);
        }
        Ok(ops)
    }

    fn plain_text_ops_since_lca(
        &mut self,
        table_schema: &TableSchema,
        column: &str,
        head: TxId,
        lca: Option<TxId>,
    ) -> Result<Vec<(TxId, crate::text_merge::TextOp)>, Error> {
        let mut chain = Vec::new();
        let mut current = Some(head);
        while let Some(tx_id) = current {
            if Some(tx_id) == lca {
                break;
            }
            let version = self
                .query_versions_for_tx(tx_id)?
                .into_iter()
                .find(|version| {
                    version.table() == table_schema.name && version.layer() == VersionLayer::Content
                })
                .ok_or(Error::MissingTransaction(tx_id))?;
            current = match version.parents().as_slice() {
                [] => None,
                [parent] => Some(*parent),
                parents => Some(self.large_value_primary_parent(parents)?),
            };
            chain.push((tx_id, version));
        }
        chain.reverse();

        let mut ops = Vec::new();
        for (tx_id, version) in chain {
            let Some(Value::Bytes(payload)) = version.cell(table_schema, column)? else {
                continue;
            };
            ops.push((tx_id, self.decode_text_storage_op(&payload)?));
        }
        Ok(ops)
    }

    fn large_value_merge_origin(&mut self, tx_id: TxId) -> Result<text_oplog::MergeOrigin, Error> {
        let tx = self
            .query_transaction(tx_id)?
            .ok_or(Error::MissingTransaction(tx_id))?;
        Ok(text_oplog::MergeOrigin {
            tx_time: tx.tx.tx_id.time,
            author: tx.tx.made_by,
            node: tx_id.node,
        })
    }

    fn large_value_primary_head(&mut self, heads: &[TxId]) -> Result<TxId, Error> {
        heads
            .iter()
            .copied()
            .map(|tx_id| {
                let made_at = self
                    .transaction_made_at(tx_id)?
                    .ok_or(Error::MissingTransaction(tx_id))?;
                Ok((made_at.sort_key(tx_id.node), tx_id))
            })
            .collect::<Result<Vec<_>, Error>>()?
            .into_iter()
            .max_by_key(|(key, _)| *key)
            .map(|(_, tx_id)| tx_id)
            .ok_or(Error::InvalidStoredValue(
                "large value merge requires heads",
            ))
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
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeSet<TxId>>, Error> {
        let rows = self.database.primary_key_scan_raw(
            MERGE_HEADS_TABLE,
            &[
                Value::Bytes(table.as_bytes().to_vec()),
                Value::Uuid(row_uuid.0),
            ],
        )?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let heads = row.record().get_bytes(2)?;
        Self::decode_merge_heads(heads).map(Some)
    }

    fn read_merge_heads_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeSet<TxId>>, Error> {
        let rows = self.database.primary_key_scan_raw_in_batch(
            batch,
            MERGE_HEADS_TABLE,
            &[
                Value::Bytes(table.as_bytes().to_vec()),
                Value::Uuid(row_uuid.0),
            ],
        )?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let heads = row.record().get_bytes(2)?;
        Self::decode_merge_heads(heads).map(Some)
    }

    fn require_merge_heads(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<BTreeSet<TxId>, Error> {
        self.read_merge_heads(table, row_uuid)?
            .ok_or(Error::InvalidStoredValue(
                "merge head set missing for existing global current row",
            ))
    }

    fn write_merge_heads(
        batch: &mut DatabaseBatch,
        table: &str,
        row_uuid: RowUuid,
        heads: &BTreeSet<TxId>,
    ) -> Result<(), Error> {
        batch.update(
            MERGE_HEADS_TABLE,
            vec![
                Value::Bytes(table.as_bytes().to_vec()),
                Value::Uuid(row_uuid.0),
                Value::Bytes(Self::encode_merge_heads(heads)?),
            ],
        );
        Ok(())
    }

    pub(super) fn update_merge_heads_for_content_version(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<(), Error> {
        if version.layer() != VersionLayer::Content {
            return Ok(());
        }
        let new_tx = self.version_tx_id(version)?;
        let mut heads = match self.read_merge_heads(version.table(), version.row_uuid())? {
            Some(existing) => existing,
            None => {
                if let Some(previous) = self.query_local_layer_winner(
                    version.table(),
                    version.row_uuid(),
                    VersionLayer::Content,
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
        Self::write_merge_heads(batch, version.table(), version.row_uuid(), &heads)
    }

    fn update_merge_heads_for_content_version_in_batch(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<(), Error> {
        if version.layer() != VersionLayer::Content {
            return Ok(());
        }
        let new_tx = self.version_tx_id(version)?;
        let mut heads =
            match self.read_merge_heads_in_batch(batch, version.table(), version.row_uuid())? {
                Some(existing) => existing,
                None => {
                    if let Some(previous) = self.query_local_layer_winner(
                        version.table(),
                        version.row_uuid(),
                        VersionLayer::Content,
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
                self.content_version_reaches_tx(version.table(), version.row_uuid(), head, new_tx)
            })
            .collect::<Result<Vec<_>, Error>>()?
            .into_iter()
            .any(|reaches| reaches);
        if !dominated_by_existing_head {
            heads.insert(new_tx);
        }
        Self::write_merge_heads(batch, version.table(), version.row_uuid(), &heads)
    }

    fn query_global_layer_winner_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        let (schema_version, base_for_current_names) = if self
            .table_in_schema(table, self.catalogue.current_schema_version_id)
            .is_ok()
        {
            (
                self.catalogue.current_schema_version_id,
                self.catalogue.current_schema_version_id,
            )
        } else {
            self.table_in_schema(table, self.catalogue.current_write_schema.schema)?;
            (
                self.catalogue.current_write_schema.schema,
                self.catalogue.current_write_schema.schema,
            )
        };
        let current_table = match layer {
            VersionLayer::Content => {
                global_current_table_name_for_schema(table, schema_version, base_for_current_names)
            }
            VersionLayer::Deletion => register_global_current_table_name_for_schema(
                table,
                schema_version,
                base_for_current_names,
            ),
        };
        let raw = self.database.primary_key_scan_raw_in_batch(
            batch,
            &current_table,
            &[Value::Uuid(row_uuid.0)],
        )?;
        let Some(raw) = raw.first() else {
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
        for (storage_table, descriptor) in self.version_storage_sources_for_layer(table, layer)? {
            let raw = self
                .database
                .primary_key_scan_raw_in_batch(
                    batch,
                    &storage_table,
                    &[
                        Value::Uuid(row_uuid.0),
                        Value::U64(tx_time.0),
                        Value::U64(tx_node_alias.0),
                    ],
                )?
                .first()
                .map(|raw| raw.raw().to_vec());
            let Some(raw) = raw else {
                continue;
            };
            return self
                .decode_history_record(table, BorrowedRecord::new(&raw, &descriptor))
                .map(Some);
        }
        Ok(None)
    }

    fn write_merge_heads_for_bulk_content_versions(
        &mut self,
        batch: &mut DatabaseBatch,
        versions: &[VersionRow],
    ) -> Result<(), Error> {
        let mut by_row = BTreeMap::<(String, RowUuid), Vec<&VersionRow>>::new();
        for version in versions {
            if version.layer() == VersionLayer::Content {
                by_row
                    .entry((version.table().to_owned(), version.row_uuid()))
                    .or_default()
                    .push(version);
            }
        }
        for ((table, row_uuid), mut row_versions) in by_row {
            row_versions.sort_by_key(|version| {
                let tx_id = self
                    .version_tx_id(version)
                    .expect("bulk content version must have node alias");
                tx_id.time.sort_key(tx_id.node)
            });
            let mut heads = self.read_merge_heads(&table, row_uuid)?.unwrap_or_default();
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
                                || self.content_version_reaches_tx(&table, row_uuid, head, new_tx),
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
            Self::write_merge_heads(batch, &table, row_uuid, &heads)?;
        }
        Ok(())
    }

    fn content_version_reaches_tx(
        &mut self,
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
            for version in self.query_versions_for_tx(tx_id)? {
                if version.table() == table
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
            for version in
                self.query_versions_for_tx_in_batch_for_row(batch, tx_id, table, row_uuid)?
            {
                if version.row_uuid() == row_uuid && version.layer() == VersionLayer::Content {
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
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Vec<VersionRow>, Error> {
        let mut versions = Vec::new();
        for (storage_table, descriptor) in
            self.version_storage_sources_for_layer(table, VersionLayer::Content)?
        {
            let raws = self
                .database
                .primary_key_scan_raw_in_batch(batch, &storage_table, &[Value::Uuid(row_uuid.0)])?
                .into_iter()
                .map(|raw| raw.raw().to_vec())
                .collect::<Vec<_>>();
            for raw in raws {
                let version =
                    self.decode_history_record(table, BorrowedRecord::new(&raw, &descriptor))?;
                if self.version_tx_id(&version)? == tx_id {
                    versions.push(version);
                }
            }
        }
        Ok(versions)
    }

    fn rewrite_merge_heads_excluding_tx(
        &mut self,
        batch: &mut DatabaseBatch,
        table: &str,
        row_uuid: RowUuid,
        excluded_tx: TxId,
    ) -> Result<(), Error> {
        let versions = self.query_row_versions(table, row_uuid)?;
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
        Self::write_merge_heads(batch, table, row_uuid, &heads)
    }

    fn merge_head_tx_ids(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<BTreeSet<TxId>, Error> {
        self.require_merge_heads(table, row_uuid)
    }

    #[cfg(test)]
    fn recomputed_merge_heads_from_history_for_test(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<BTreeSet<TxId>, Error> {
        let versions = self.query_row_versions(table, row_uuid)?;
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
        let mut batch = self.database.open_batch();
        Self::write_merge_heads(&mut batch, table, row_uuid, &heads)?;
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
        let actual = self.require_merge_heads(table, row_uuid)?;
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
        let base_for_current_names = if self
            .table_in_schema(version.table(), self.catalogue.current_schema_version_id)
            .is_ok()
        {
            self.catalogue.current_schema_version_id
        } else {
            schema_version
        };
        let table = self.table_in_schema(version.table(), schema_version)?;
        let storage_tables = table.global_current_storage_tables();
        let (current_table, current_schema, expected_values) = match version.layer() {
            VersionLayer::Content => (
                global_current_table_name_for_schema(
                    version.table(),
                    schema_version,
                    base_for_current_names,
                ),
                &storage_tables[0],
                global_current_values(&table, version, Some(global_seq))?,
            ),
            VersionLayer::Deletion => (
                register_global_current_table_name_for_schema(
                    version.table(),
                    schema_version,
                    base_for_current_names,
                ),
                &storage_tables[1],
                register_global_current_values(version, Some(global_seq)),
            ),
        };
        let rows = self
            .database
            .primary_key_scan_raw(&current_table, &[Value::Uuid(version.row_uuid().0)])?;
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
        let rows = self.database.primary_key_scan_raw(
            "jazz_global_changes",
            &[
                Value::Bytes(version.table().as_bytes().to_vec()),
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
        &self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
        global_seq: GlobalSeq,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let base_for_current_names = if self
            .table_in_schema(version.table(), self.catalogue.current_schema_version_id)
            .is_ok()
        {
            self.catalogue.current_schema_version_id
        } else {
            schema_version
        };
        match version.layer() {
            VersionLayer::Content => {
                let table = self.table_in_schema(version.table(), schema_version)?;
                batch.update_raw(
                    global_current_table_name_for_schema(
                        version.table(),
                        schema_version,
                        base_for_current_names,
                    ),
                    global_current_primary_key(version.row_uuid()),
                    owned_record_from_storage_values(
                        &table.global_current_storage_tables()[0],
                        global_current_values(&table, version, Some(global_seq))
                            .expect("valid global current values"),
                    )
                    .expect("valid global current row")
                    .raw()
                    .to_vec(),
                );
            }
            VersionLayer::Deletion => batch.update_raw(
                register_global_current_table_name_for_schema(
                    version.table(),
                    schema_version,
                    base_for_current_names,
                ),
                global_current_primary_key(version.row_uuid()),
                owned_record_from_storage_values(
                    &self
                        .table_in_schema(version.table(), schema_version)?
                        .global_current_storage_tables()[1],
                    register_global_current_values(version, Some(global_seq)),
                )
                .expect("valid register global current row")
                .raw()
                .to_vec(),
            ),
        }
        batch.update(
            "jazz_global_changes",
            global_change_values(version, global_seq),
        );
        Ok(())
    }

    pub(super) fn write_ahead_current_insert(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let base_for_current_names = if self
            .table_in_schema(version.table(), self.catalogue.current_schema_version_id)
            .is_ok()
        {
            self.catalogue.current_schema_version_id
        } else {
            schema_version
        };
        match version.layer() {
            VersionLayer::Content => {
                let table = self.table_in_schema(version.table(), schema_version)?;
                batch.insert_raw(
                    ahead_current_table_name_for_schema(
                        version.table(),
                        schema_version,
                        base_for_current_names,
                    ),
                    history_primary_key(version),
                    owned_record_from_storage_values(
                        &table.ahead_current_storage_tables()[0],
                        global_current_values(&table, version, None)
                            .expect("valid ahead current values"),
                    )
                    .expect("valid ahead current row")
                    .raw()
                    .to_vec(),
                );
            }
            VersionLayer::Deletion => batch.insert_raw(
                register_ahead_current_table_name_for_schema(
                    version.table(),
                    schema_version,
                    base_for_current_names,
                ),
                history_primary_key(version),
                owned_record_from_storage_values(
                    &self
                        .table_in_schema(version.table(), schema_version)?
                        .ahead_current_storage_tables()[1],
                    register_global_current_values(version, None),
                )
                .expect("valid register ahead current row")
                .raw()
                .to_vec(),
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

    pub(super) fn write_ahead_current_delete(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let base_for_current_names = if self
            .table_in_schema(version.table(), self.catalogue.current_schema_version_id)
            .is_ok()
        {
            self.catalogue.current_schema_version_id
        } else {
            schema_version
        };
        let table = match version.layer() {
            VersionLayer::Content => ahead_current_table_name_for_schema(
                version.table(),
                schema_version,
                base_for_current_names,
            ),
            VersionLayer::Deletion => register_ahead_current_table_name_for_schema(
                version.table(),
                schema_version,
                base_for_current_names,
            ),
        };
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
        for version in self.query_versions_for_tx(tx_id)? {
            self.write_ahead_current_delete(batch, &version)?;
        }
        Ok(())
    }

    pub(super) fn cleanup_settled_ahead_current_leftovers(&mut self) -> Result<(), Error> {
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
            let node_alias = NodeAlias(record.get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)?);
            let node = self
                .node_for_alias(node_alias)
                .ok_or(Error::InvalidStoredValue(
                    "transaction node alias must exist",
                ))?;
            tx_ids.push(TxId::new(
                TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?),
                node,
            ));
        }
        if tx_ids.is_empty() {
            return Ok(());
        }
        let mut batch = self.database.open_batch();
        for tx_id in tx_ids {
            self.cleanup_fated_ahead_current_for_tx(&mut batch, tx_id)?;
        }
        self.database.commit_batch(batch)?;
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
        for version in versions {
            let author_schema = version.schema_version();
            let source_table_schema = self.table_in_schema(version.table(), author_schema)?;
            let mut target_table = version.table().to_owned();
            let mut target_cells = source_table_schema
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, column)| {
                    version
                        .optional_cell_at(idx)
                        .map(|value| (column.name.clone(), value))
                })
                .collect::<BTreeMap<_, _>>();
            let mut target_schema = author_schema;
            if author_schema != self.catalogue.current_write_schema.schema
                && self.has_forward_lens_path(
                    author_schema,
                    self.catalogue.current_write_schema.schema,
                )
            {
                target_schema = self.catalogue.current_write_schema.schema;
                target_table = self.translate_cells_forward(
                    author_schema,
                    target_schema,
                    &target_table,
                    &mut target_cells,
                )?;
            }
            let table_schema = self.table_in_schema(&target_table, target_schema)?;
            let schema_version_alias = self.ensure_schema_version_alias(target_schema)?;
            let layer = VersionLayer::for_record(&version);
            let previous_current =
                self.query_local_layer_winner(&table_schema.name, version.row_uuid(), layer)?;
            let stored = VersionRow::from_parts_with_schema_version(
                &table_schema,
                VersionRowParts {
                    table: target_table,
                    row_uuid: version.row_uuid(),
                    tx_node_alias,
                    schema_version_alias,
                    tx_time: tx.tx_id.time,
                    parents: version.parents(),
                    created_by: version.created_by(),
                    created_at: version.created_at(),
                    updated_by: version.updated_by(),
                    updated_at: version.updated_at(),
                    cells: target_cells,
                    deletion: version.deletion(),
                },
                (target_schema != self.catalogue.current_schema_version_id)
                    .then_some(target_schema),
            )?;
            let previous_winner = if let Some(previous) = previous_current.as_ref() {
                Some((
                    previous,
                    self.version_tx_id(previous)?,
                    self.version_made_at(previous)?,
                ))
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
            let history_table = version_storage_table_name_for_schema(
                &table_schema.name,
                stored.layer(),
                target_schema,
                self.catalogue.current_schema_version_id,
            );
            let existing = self.database.primary_key_scan_raw_in_batch(
                batch,
                &history_table,
                &[
                    Value::Uuid(stored.row_uuid().0),
                    Value::U64(stored.tx_time().0),
                    Value::U64(stored.tx_node_alias().0),
                ],
            )?;
            if let Some(existing) = existing.first() {
                if existing.record().raw() != stored.record.raw() {
                    return Err(Error::ConflictingCommitUnit(tx.tx_id));
                }
            } else {
                batch.insert_raw(
                    history_table,
                    history_primary_key(&stored),
                    stored.record.raw().to_vec(),
                );
            }
            if update_current_indexes && !matches!(fate, Fate::Rejected(_)) && global_seq.is_none()
            {
                self.write_ahead_current_insert(batch, &stored)?;
            }
        }
        if !matches!(fate, Fate::Rejected(_)) {
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
        self.invalidate_tx_version_tables_cache(tx_id);
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

    fn translate_cells_forward(
        &mut self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        table: &str,
        cells: &mut BTreeMap<String, Value>,
    ) -> Result<String, Error> {
        if source == target {
            return Ok(table.to_owned());
        }
        let path = self
            .compiled_lens_path(source, target, LensPathDirection::Forward, table)?
            .ok_or(Error::InvalidCatalogueUpdate("lens chain is unknown"))?;
        Ok(apply_compiled_lens_path(&path, cells))
    }

    fn reject_source_delta_reason(&mut self, versions: &[VersionRecord]) -> Option<String> {
        for version in versions {
            let target_schema = self.catalogue.current_write_schema.schema;
            if version.schema_version() == target_schema {
                continue;
            }
            let mut current_table = version.table().to_owned();
            let Some(path) = self.shortest_lens_path_ids_cached(
                version.schema_version(),
                target_schema,
                LensPathDirection::Forward,
            ) else {
                continue;
            };
            for lens_id in path {
                let lens = self.catalogue.catalogue_lenses.get(&lens_id)?;
                let table_lens = lens
                    .table_lenses
                    .iter()
                    .find(|candidate| candidate.source_table == current_table)?;
                for op in &table_lens.ops {
                    match op {
                        LensOp::RejectSourceDelta { reason } => return Some(reason.clone()),
                        LensOp::TransformColumn { transform, .. } => {
                            if validate_registered_transform(transform).is_err() {
                                return Some("transform column is not registered".to_owned());
                            }
                        }
                        _ => {}
                    }
                }
                current_table = table_lens.target_table.clone();
            }
        }
        None
    }

    fn has_forward_lens_path(&mut self, source: SchemaVersionId, target: SchemaVersionId) -> bool {
        self.shortest_lens_path_ids_cached(source, target, LensPathDirection::Forward)
            .is_some()
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

fn merge_large_value_head_ops(
    lca_value: &[u8],
    mut head_ops: Vec<(TxTimeSortKey, Vec<TextOp>, text_oplog::MergeOrigin)>,
) -> Vec<u8> {
    head_ops.sort_by_key(|(key, _, _)| *key);

    let mut merged = lca_value.to_vec();
    let mut merged_origin = None;
    let mut seen_ops = BTreeSet::new();
    for (_, ops, origin) in head_ops {
        let ops = ops
            .into_iter()
            .filter(|op| seen_ops.insert((origin, text_oplog::encode(std::slice::from_ref(op)))))
            .collect::<Vec<_>>();
        if ops.is_empty() {
            continue;
        }
        let accumulator_origin = merged_origin.unwrap_or(origin);
        merged = text_oplog::merge_since_lca(
            lca_value,
            (&text_oplog::diff(lca_value, &merged), accumulator_origin),
            (&ops, origin),
        );
        // INV-HIST-15/16: heads are folded in causal order, so the
        // accumulator's same-position tie-break identity is the greatest origin
        // already folded, not an all-zero sentinel.
        merged_origin = Some(accumulator_origin.max(origin));
    }
    merged
}

fn content_refs_in_ops(ops: Vec<TextOp>) -> Vec<content_store::Extent> {
    ops.into_iter()
        .filter_map(|op| match op {
            TextOp::Insert {
                content: TextContent::Ref(extent),
                ..
            } => Some(extent),
            TextOp::Insert { .. } | TextOp::Delete { .. } => None,
        })
        .collect()
}

fn validate_transform_column(column: Option<&ColumnSchema>, transform: &str) -> Result<(), Error> {
    validate_registered_transform(transform)?;
    let Some(column) = column else {
        return Err(Error::InvalidCatalogueUpdate("transform column is unknown"));
    };
    if column.large_value.is_some() {
        return Err(Error::InvalidCatalogueUpdate(
            "large-value columns cannot be content-transformed",
        ));
    }
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

fn merge_rows_for_versions(records: &[VersionRecord]) -> Vec<(String, RowUuid)> {
    let mut rows = Vec::with_capacity(records.len());
    for record in records {
        if record.deletion().is_none() {
            rows.push((record.table().to_owned(), record.row_uuid()));
        }
    }
    rows.sort_unstable();
    rows.dedup();
    rows
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
        _ => Err(Error::InvalidStoredValue(
            "counter strategy requires integer column",
        )),
    }
}

#[cfg(test)]
mod large_value_merge_tests {
    use super::*;

    #[test]
    fn three_head_large_value_fold_is_input_order_deterministic() {
        let ancestor = b"abc";
        let first = head_insert(20, 0x82, 0xa1, b"AAA");
        let second = head_insert(21, 0x83, 0xa2, b"BBB");
        let third = head_insert(22, 0x84, 0xa3, b"CCC");

        let ascending = merge_large_value_head_ops(
            ancestor,
            vec![first.clone(), second.clone(), third.clone()],
        );
        let descending = merge_large_value_head_ops(ancestor, vec![third, second, first]);

        assert_eq!(ascending, b"aAAABBBCCCbc".to_vec());
        assert_eq!(ascending, descending);
    }

    #[test]
    fn overlapping_large_value_raw_op_is_applied_once() {
        let ancestor = b"abc";
        let shared = head_insert(20, 0x82, 0xa1, b"AAA");
        let distinct = head_insert(21, 0x83, 0xa2, b"BBB");

        let merged = merge_large_value_head_ops(ancestor, vec![shared.clone(), distinct, shared]);

        assert_eq!(merged, b"aAAABBBbc".to_vec());
    }

    fn head_insert(
        tx_time: u64,
        node: u8,
        author: u8,
        bytes: &[u8],
    ) -> (TxTimeSortKey, Vec<TextOp>, text_oplog::MergeOrigin) {
        let tx_time = TxTime::from(tx_time);
        let node = NodeUuid::from_bytes([node; 16]);
        (
            tx_time.sort_key(node),
            vec![TextOp::Insert {
                pos: 1,
                content: TextContent::Inline(bytes.to_vec()),
            }],
            text_oplog::MergeOrigin {
                tx_time,
                author: AuthorId::from_bytes([author; 16]),
                node,
            },
        )
    }
}
