pub(crate) struct PreparedFateUpdate {
    tx_id: TxId,
    stored: StoredTransaction,
    next_clock: Clock,
    batch: groove::db::PreparedDatabaseBatch,
    rejected_payload: Option<RejectedTransaction>,
    #[cfg(test)]
    content_versions: Vec<VersionRow>,
    #[cfg(test)]
    global_current_update_versions: Vec<(VersionRow, GlobalSeq)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FateUpdateRequest {
    pub(super) tx_id: TxId,
    pub(super) fate: Fate,
    pub(super) global_seq: Option<GlobalSeq>,
    pub(super) durability: Option<DurabilityTier>,
}

impl<S> NodeState<S>
where
    S: ResidentStorage,
{
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
        let mut pending = VecDeque::from([FateUpdateRequest {
            tx_id,
            fate,
            global_seq,
            durability,
        }]);
        while let Some(request) = pending.pop_front() {
            let prepared = self.prepare_fate_update_with_storage(request, false)?;
            pending.extend(self.publish_prepared_fate_update(prepared)?);
        }
        Ok(())
    }

    pub(crate) fn prepare_fate_update(
        &mut self,
        request: FateUpdateRequest,
    ) -> Result<PreparedFateUpdate, Error> {
        self.prepare_fate_update_with_storage(request, true)
    }

    fn prepare_fate_update_with_storage(
        &mut self,
        request: FateUpdateRequest,
        acquire_tick_storage: bool,
    ) -> Result<PreparedFateUpdate, Error> {
        self.require_catalogue_ready()?;
        let FateUpdateRequest {
            tx_id,
            fate,
            global_seq,
            durability,
        } = request;
        debug_assert!(
            global_seq.is_none() || durability == Some(DurabilityTier::Global),
            "a global sequence requires Global durability"
        );
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
        if matches!(stored.fate, Fate::Rejected(_)) {
            self.ensure_child_edge_closure_loaded(tx_id)?;
        }
        let mut next_clock = self.clock.clone();
        let advanced_global_seqs = if matches!(stored.fate, Fate::Accepted)
            && let Some(global_seq) = stored.global_seq
        {
            next_clock.record_applied_global_seq(global_seq)
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
        #[cfg(test)]
        for (table, row_uuid) in content_versions
            .iter()
            .map(|version| (version.table(), version.row_uuid()))
            .collect::<BTreeSet<_>>()
        {
            // The post-publication invariant checks below deliberately rescan
            // complete row history. A demand-loaded test runtime must acquire
            // those diagnostic-only inputs before crossing publication just
            // like production inputs; assertions may never introduce a cold
            // read after resident state has advanced.
            self.query_row_versions(table, row_uuid)?;
        }
        self.stage_recovery_checkpoint_for_clock(&mut batch, self.clock.tx_time, &next_clock);
        if matches!(stored.fate, Fate::Rejected(_)) || stored.global_seq.is_some() {
            self.stage_storage_consistency_marker_through(&mut batch, tx_id.time)?;
        }
        let batch = if acquire_tick_storage {
            self.database.prepare_batch_storage_inputs(&batch)?
        } else {
            self.database.prepare_resident_batch(&batch)?
        };
        Ok(PreparedFateUpdate {
            tx_id,
            stored,
            next_clock,
            batch,
            rejected_payload,
            #[cfg(test)]
            content_versions,
            #[cfg(test)]
            global_current_update_versions,
        })
    }

    pub(crate) fn publish_prepared_fate_update(
        &mut self,
        prepared: PreparedFateUpdate,
    ) -> Result<Vec<FateUpdateRequest>, Error> {
        let PreparedFateUpdate {
            tx_id,
            stored,
            next_clock,
            batch,
            rejected_payload,
            #[cfg(test)]
            content_versions,
            #[cfg(test)]
            global_current_update_versions,
        } = prepared;
        self.publish_prepared_database_batch(batch)?;
        self.clock = next_clock;
        if !matches!(stored.fate, Fate::Pending) {
            self.open_tx.local_permission_subjects.remove(&tx_id);
        }
        #[cfg(test)]
        {
            if stored.tx.target_lineage == crate::tx::BranchLineage::Root {
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
        let mut cascades = Vec::new();
        if accepted_final {
            self.rejections.child_txs_by_parent.remove(&tx_id);
            self.prune_child_edges(tx_id);
        } else if let Some(root) = rejected_root {
            self.prune_child_edges(tx_id);
            let descendants = self.local_cascade_descendants(tx_id, root)?;
            for descendant in descendants {
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
                cascades.push(FateUpdateRequest {
                    tx_id: descendant,
                    fate: Fate::Rejected(RejectionReason::Cascade { root }),
                    global_seq: None,
                    durability: None,
                });
            }
        }
        Ok(cascades)
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

    /// Admit the exact durable child-edge closure that a rejection may walk
    /// during its non-suspending publication phase.
    pub(super) fn prepare_rejection_cascade_inputs(&mut self, root: TxId) -> Result<(), Error> {
        let mut pending = VecDeque::from([root]);
        let mut visited = BTreeSet::new();
        while let Some(parent) = pending.pop_front() {
            if !visited.insert(parent) {
                continue;
            }
            self.ensure_child_edge_closure_loaded(parent)?;
            pending.extend(
                self.rejections
                    .child_txs_by_parent
                    .get(&parent)
                    .into_iter()
                    .flatten()
                    .copied(),
            );
        }
        Ok(())
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
        self.ensure_child_edges_loaded(rejected)?;
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
                self.ensure_child_edges_loaded(tx_id)?;
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

}
