impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Apply an upstream fate update.
    pub async fn apply_fate_update(
        &mut self,
        tx_id: TxId,
        fate: Fate,
        global_time: Option<GlobalTime>,
        durability: Option<DurabilityTier>,
    ) -> Result<(), Error> {
        self.apply_fate_update_with_cascade(tx_id, fate, global_time, durability, true)
            .await
    }

    async fn apply_fate_update_with_cascade(
        &mut self,
        tx_id: TxId,
        fate: Fate,
        global_time: Option<GlobalTime>,
        durability: Option<DurabilityTier>,
        cascade_descendants: bool,
    ) -> Result<(), Error> {
        self.require_catalogue_ready()?;
        debug_assert!(
            global_time.is_none() || durability == Some(DurabilityTier::Global),
            "a global timestamp requires Global durability"
        );
        let mut terminal_fate_persisted = false;
        let result = self.apply_fate_update_once(
            tx_id,
            fate,
            global_time,
            durability,
            &mut terminal_fate_persisted,
            cascade_descendants,
        ).await;
        if terminal_fate_persisted {
            self.open_tx.local_permission_subjects.remove(&tx_id);
        }
        result
    }

    async fn apply_fate_update_once(
        &mut self,
        tx_id: TxId,
        fate: Fate,
        global_time: Option<GlobalTime>,
        durability: Option<DurabilityTier>,
        terminal_fate_persisted: &mut bool,
        cascade_descendants: bool,
    ) -> Result<(), Error> {
        let mut stored = self
            .query_transaction(tx_id).await?
            .ok_or(Error::MissingTransaction(tx_id))?;
        if let (Some(current), Some(next)) = (stored.global_time, global_time)
            && next < current
        {
            return Err(Error::NonMonotoneState("global seq cannot move backwards"));
        }
        stored.fate = next_fate(&stored.fate, fate)?;
        stored.global_time = global_time.or(stored.global_time);
        if let Some(durability) = durability {
            stored.durability = stored.durability.max(durability);
        }
        let advanced_global_times = if matches!(stored.fate, Fate::Accepted)
            && let Some(global_time) = stored.global_time
        {
            self.record_applied_global_time(global_time)
        } else {
            Vec::new()
        };

        let mut batch = self.database.open_batch();
        let mut global_current_updates = Vec::new();
        let cleanup_rejected_versions = matches!(stored.fate, Fate::Rejected(_));
        let tx_versions = self.query_versions_for_tx(tx_id).await?;
        let content_versions = tx_versions
            .iter()
            .filter(|version| version.layer() == VersionLayer::Content)
            .cloned()
            .collect::<Vec<_>>();
        if matches!(stored.fate, Fate::Accepted) && stored.global_time.is_some() {
            global_current_updates =
                self.global_current_updates_for_versions(tx_id, &tx_versions).await?;
        }
        if let Some(child_alias) = self.node_aliases.get(&tx_id.node).copied() {
            for raw in self.database.primary_key_scan_raw(
                "jazz_pending_edges",
                &[Value::U64(tx_id.time.0), Value::U64(child_alias.0)],
            ).await? {
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
            transaction_values_with_cardinality_scope(
                stored.node_alias,
                &stored.tx,
                stored.fate.clone(),
                stored.global_time,
                stored.durability,
                stored.view_scoped_cardinality,
            ),
        );
        if !matches!(stored.fate, Fate::Rejected(_)) {
            for version in &content_versions {
                self.update_merge_heads_for_content_version(&mut batch, version, false)
                    .await?;
            }
        }
        if let Some(global_time) = stored.global_time {
            for version in &global_current_updates {
                self.write_global_current_update(&mut batch, version, global_time)?;
            }
        }
        #[cfg(test)]
        let global_current_update_versions = stored
            .global_time
            .map(|global_time| {
                global_current_updates
                    .iter()
                    .cloned()
                    .map(|version| (version, global_time))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if matches!(stored.fate, Fate::Rejected(_)) || stored.global_time.is_some() {
            self.cleanup_fated_ahead_current_for_versions(&mut batch, &tx_versions)?;
        }
        for global_time in advanced_global_times
            .iter()
            .copied()
            .filter(|global_time| Some(*global_time) != stored.global_time)
        {
            self.prune_ahead_current_for_global_time(&mut batch, global_time)
                .await?;
        }
        let rejected_payload = if cleanup_rejected_versions {
            self.remove_rejected_local_versions(tx_id, &stored, &mut batch).await?
        } else {
            None
        };
        let applied = self.database.apply_batch(batch).await?;
        let persisted = applied.persist().await;
        self.database.finish_persistence(persisted)?;
        *terminal_fate_persisted = !matches!(stored.fate, Fate::Pending);
        if matches!(stored.fate, Fate::Rejected(_)) || stored.global_time.is_some() {
            self.persist_storage_consistency_marker_through(tx_id.time)
                .await?;
        }
        #[cfg(test)]
        {
            let rows = content_versions
                .iter()
                .map(|version| {
                    (
                        version.table().to_owned(),
                        version.branch_key().clone(),
                        version.row_uuid(),
                    )
                })
                .collect::<BTreeSet<_>>();
            self.assert_merge_head_rows_match_history_for_test(&rows)
                .await?;
            self.assert_global_current_updates_match_history_for_test(
                &global_current_update_versions,
            )
            .await?;
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
            if !cascade_descendants {
                self.rejections.child_txs_by_parent.remove(&tx_id);
                return Ok(());
            }
            let cascades = self.local_cascade_descendants(tx_id, root).await?;
            for descendant in cascades {
                // Authority-side parking resolves parents before children, so
                // a locally cascaded descendant should still be speculative.
                let descendant_fate = self.query_transaction(descendant).await?.map(|tx| tx.fate);
                debug_assert!(
                    matches!(descendant_fate.as_ref(), Some(Fate::Pending))
                        || matches!(
                            descendant_fate.as_ref(),
                            Some(Fate::Rejected(RejectionReason::Cascade { root: existing }))
                                if *existing == root
                        )
                );
                Box::pin(self.apply_fate_update_with_cascade(
                    descendant,
                    Fate::Rejected(RejectionReason::Cascade { root }),
                    None,
                    None,
                    false,
                ))
                .await?;
            }
        }
        Ok(())
    }

    /// Return locally visible current cells for one row.
    pub(super) async fn validate_exclusive_commit_unit(
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
            ).await;
            if current != Some(read.version) {
                return Ok(false);
            }
        }
        for absent in tx.absent_read_set.as_deref().unwrap_or(&[]) {
            let current = self.visible_global_content_tx_id_now_memoized(
                &absent.table,
                absent.row_uuid,
                &mut visible_content_memo,
            ).await;
            if current.is_some() {
                return Ok(false);
            }
        }
        for predicate in tx.predicate_read_set.as_deref().unwrap_or(&[]) {
            if self.predicate_read_is_degenerate_whole_table(predicate)? {
                if self
                    .global_currency_changed_outside_snapshot(&predicate.table, base_snapshot)
                    .await?
                {
                    return Ok(false);
                }
            } else if self
                .shape_predicate_changed_after(predicate, base_snapshot)
                .await?
            {
                return Ok(false);
            }
        }
        for version in versions {
            self.table_in_schema(version.table(), version.schema_version())?;
            let current = self.visible_global_content_tx_id_now_memoized(
                version.table(),
                version.row_uuid(),
                &mut visible_content_memo,
            ).await;
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

    async fn visible_global_content_tx_id_now_memoized(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        memo: &mut BTreeMap<(String, RowUuid), Option<TxId>>,
    ) -> Option<TxId> {
        if let Some(current) = memo.get(&(table.to_owned(), row_uuid)) {
            return *current;
        }
        let current = self.visible_global_content_tx_id_now(table, row_uuid).await;
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

    pub(super) async fn shape_predicate_changed_after(
        &mut self,
        predicate: &PredicateRead,
        snapshot: &Snapshot,
    ) -> Result<bool, Error> {
        let shape = predicate.shape.validate(&self.catalogue.schema)?;
        if shape.shape_id() != predicate.shape_id {
            return Ok(true);
        }
        let binding = shape.bind(predicate.binding_values.clone())?;
        if binding.binding_id() != predicate.binding_id {
            return Ok(true);
        }
        let at_base = self
            .shape_output_tx_set_at_snapshot(&shape, &binding, snapshot)
            .await?;
        let at_now = self.shape_output_tx_set_now(&shape, &binding).await?;
        Ok(at_base != at_now)
    }

    async fn shape_output_tx_set_now(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<BTreeSet<(RowUuid, TxId)>, Error> {
        let table = shape.query().table.clone();
        let mut set = BTreeSet::new();
        for row in self
            .query_rows(shape, binding, DurabilityTier::Global)
            .await?
        {
            if let Some(tx_id) = self.visible_global_content_tx_id_now(&table, row.row_uuid()).await {
                set.insert((row.row_uuid(), tx_id));
            }
        }
        Ok(set)
    }

    async fn shape_output_tx_set_at_snapshot(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        snapshot: &Snapshot,
    ) -> Result<BTreeSet<(RowUuid, TxId)>, Error> {
        // An origin snapshot has no visible application rows.  Do not ask the
        // historical compiler for provenance columns in that empty state: such
        // columns require a historical storage cut even though the result set
        // is necessarily empty (notably after a transaction reads its own
        // staged insert before the first authority receipt).
        if snapshot.global_base == GlobalTime(0)
            && snapshot.local_base == TxTime(0)
            && snapshot.dots.is_empty()
        {
            return Ok(BTreeSet::new());
        }
        let table = shape.query().table.clone();
        let rows = self
            .query_rows_at_snapshot(shape, binding, snapshot)
            .await?;
        let mut set = BTreeSet::new();
        for row in rows {
            let row_uuid = row.row_uuid();
            let Some(tx_id) = self
                .snapshot_content_witness(&table, row_uuid, snapshot)
                .await
            else {
                    return Err(Error::InvalidStoredValue(
                        "historical query output row must have visible content",
                    ));
            };
            set.insert((row_uuid, tx_id));
        }
        Ok(set)
    }

    pub(super) async fn commit_unit_satisfies_write_policies(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<bool, Error> {
        if ingest_context.is_some_and(|context| context.trust == CommitUnitTrust::TrustedAdmin) {
            return Ok(true);
        }
        let permission_subject = match ingest_context {
            Some(context) => {
                if context.trust == CommitUnitTrust::Session && tx.made_by != context.identity {
                    return Ok(false);
                }
                match context.trust {
                    CommitUnitTrust::Session => context.identity,
                    CommitUnitTrust::TrustedBackend => tx.permission_subject.unwrap_or(tx.made_by),
                    CommitUnitTrust::TrustedAdmin => unreachable!("handled above"),
                }
            }
            None => tx.permission_subject.unwrap_or(tx.made_by),
        };
        for version in versions {
            if !self
                .version_satisfies_write_policy(version, permission_subject, tx.tx_id)
                .await?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) async fn version_satisfies_write_policy(
        &mut self,
        version: &VersionRecord,
        author: AuthorSubject,
        candidate_tx_id: TxId,
    ) -> Result<bool, Error> {
        self.write_policy_allows_version_record(version, author, Some(candidate_tx_id))
            .await
    }

    pub(super) async fn cascade_root_for_versions(
        &mut self,
        versions: &[VersionRecord],
    ) -> Option<TxId> {
        for parent in versions.iter().flat_map(|version| version.parents()) {
            if let Some(root) = self.cascade_root_for_tx(parent).await {
                return Some(root);
            }
        }
        None
    }

    pub(super) async fn park_commit_unit_if_missing_parents_with_mode(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        now_ms: u64,
        memo: &mut IngestMemo,
        mode: CommitUnitParkMode,
    ) -> Result<bool, Error> {
        if self.missing_parent_refs_memo(versions, memo).await?.is_empty() {
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

    pub(super) async fn missing_parent_refs(
        &mut self,
        versions: &[VersionRecord],
    ) -> Result<BTreeSet<TxId>, Error> {
        let mut memo = IngestMemo::default();
        self.missing_parent_refs_memo(versions, &mut memo).await
    }

    pub(super) async fn missing_parent_refs_memo(
        &mut self,
        versions: &[VersionRecord],
        memo: &mut IngestMemo,
    ) -> Result<BTreeSet<TxId>, Error> {
        let mut missing = BTreeSet::new();
        for parent in versions.iter().flat_map(|version| version.parents()) {
            if !self.transaction_exists_memo(parent, memo).await? {
                missing.insert(parent);
            }
        }
        Ok(missing)
    }

    pub(super) async fn commit_unit_satisfies_clock_condition(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        memo: &mut IngestMemo,
    ) -> Result<bool, Error> {
        for version in versions {
            for parent in version.parents() {
                let Some(parent_made_at) = self.transaction_made_at_memo(parent, memo).await? else {
                    return Ok(false);
                };
                if tx.tx_id.time <= parent_made_at {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    pub(super) async fn drain_parked_commit_units(
        &mut self,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: ReopenableStorage,
    {
        let mut updates = PublicationOutcome::settled(Vec::new());
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
                }) && self.missing_parent_refs(&versions).await?.is_empty()
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
                if unit.ingress_role == ParkedIngressRole::EdgeAccepted {
                    updates.extend(self.finalize_edge_accepted_mergeable_commit_unit_once(
                        unit.tx,
                        unit.versions,
                        unit.now_ms,
                    ).await?);
                } else if unit.ingress_role == ParkedIngressRole::EdgeAuthority {
                    updates.extend(self.ingest_edge_authority_mergeable_commit_unit_once(
                        unit.tx,
                        unit.versions,
                        unit.now_ms,
                        unit.ingest_context,
                    ).await?);
                } else {
                    updates.extend(self.ingest_commit_unit_once(
                        unit.tx,
                        unit.versions,
                        unit.now_ms,
                        unit.ingest_context,
                    ).await?);
                }
            }
        }
        Ok(updates)
    }

    pub(super) async fn drain_parked_relay_commit_units(&mut self) -> Result<(), Error>
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
                }) && self.missing_parent_refs(&versions).await?.is_empty()
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
                self.sync_metrics.parked_orphans_resolved += 1;
                if self.parking.parked_catalogue_commit_units.remove(&tx_id) {
                    self.sync_metrics.parked_catalogue_orphans_resolved += 1;
                }
                self.ingest_relay_commit_unit_once(unit.tx, unit.versions).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn cascade_root_for_tx(&mut self, tx_id: TxId) -> Option<TxId> {
        let mut stack = vec![tx_id];
        let mut seen = BTreeSet::new();
        while let Some(current) = stack.pop() {
            if !seen.insert(current) {
                continue;
            }
            if let Ok(Some(tx)) = self.query_transaction(current).await
                && let Some(root) = rejected_root_for(&tx.fate, current)
            {
                return Some(root);
            }
            if let Ok(Some(tx)) = self.query_transaction(current).await
                && matches!(tx.fate, Fate::Accepted)
            {
                continue;
            }
            let Ok(versions) = self.query_versions_for_tx(current).await else {
                return None;
            };
            stack.extend(versions.iter().flat_map(|version| version.parents()));
        }
        None
    }

    pub(super) async fn cascade_rejections_from(
        &mut self,
        rejected: TxId,
    ) -> Result<Vec<SyncMessage>, Error> {
        let Some(root) = self.cascade_root_for_tx(rejected).await.or(Some(rejected)) else {
            return Ok(Vec::new());
        };
        let descendants = self.local_cascade_descendants(rejected, root).await?;
        let mut updates = Vec::new();
        for descendant in descendants {
            let fate = Fate::Rejected(RejectionReason::Cascade { root });
            self.apply_fate_update_with_cascade(descendant, fate.clone(), None, None, false)
                .await?;
            updates.push(SyncMessage::FateUpdate {
                tx_id: descendant,
                fate,
                global_time: None,
                durability: None,
            });
        }
        Ok(updates)
    }

    #[cfg(test)]
    pub(crate) async fn transaction_ids(&self) -> Result<Vec<TxId>, Error> {
        let mut tx_ids = Vec::new();
        for raw in self
            .database
            .primary_key_scan_raw("jazz_transactions", &[])
            .await?
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

    pub(super) async fn local_cascade_descendants(
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
            let Some(tx) = self.query_transaction(tx_id).await? else {
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

    pub(super) async fn remove_rejected_local_versions(
        &mut self,
        tx_id: TxId,
        tx: &StoredTransaction,
        batch: &mut DatabaseBatch,
    ) -> Result<Option<RejectedTransaction>, Error> {
        let rejected = self.query_versions_for_tx(tx_id).await?;
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
                    version.branch_key().clone(),
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
                self.version_storage_primary_key(version)?,
            );
        }
        for (table_id, table, branch_key, row_uuid) in affected_content_rows {
            self.rewrite_merge_heads_excluding_tx(
                batch,
                table_id,
                &table,
                &branch_key,
                row_uuid,
                tx_id,
            )
            .await?;
        }
        self.invalidate_tx_version_tables_cache(tx_id);
        let _ = affected;
        Ok(rejected_payload)
    }

}
