impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Reconstruct an edge's coherent upload from accepted canonical history.
    ///
    /// Call only after settling edge admission and generated merge publications.
    /// Globally acknowledged parents already exist at core, so traversal stops
    /// there rather than walking a row's complete historical ancestry.
    pub async fn edge_authority_publication_for(
        &mut self,
        tx_id: TxId,
    ) -> Result<crate::protocol::AuthorityPublication, Error> {
        let original = self.query_versions_for_tx(tx_id).await?;
        let records = original
            .iter()
            .map(|row| self.version_record_from_row(row))
            .collect::<Result<Vec<_>, _>>()?;
        let rows = self.merge_rows_for_versions(&records)?;
        let mut pending = vec![tx_id];
        for (table, branch, row) in rows {
            let table_id = self
                .physical_table_id_for_schema(self.catalogue.current_write_schema.schema, &table)?;
            for head in self.merge_head_tx_ids(table_id, &branch, row).await? {
                let state = self
                    .query_transaction(head)
                    .await?
                    .ok_or(Error::MissingTransaction(head))?;
                if state.fate == Fate::Accepted && state.durability == DurabilityTier::Edge {
                    pending.push(head);
                }
            }
        }
        let mut commits = BTreeMap::new();
        while let Some(current) = pending.pop() {
            if commits.contains_key(&current) {
                continue;
            }
            let stored = self
                .query_transaction(current)
                .await?
                .ok_or(Error::MissingTransaction(current))?;
            if stored.fate != Fate::Accepted || stored.durability < DurabilityTier::Edge {
                return Err(Error::InvalidStoredValue(
                    "authority publication requires accepted persisted transactions",
                ));
            }
            let tx = stored.tx;
            let versions = self
                .query_versions_for_tx(current)
                .await?
                .iter()
                .map(|row| self.version_record_from_row(row))
                .collect::<Result<Vec<_>, _>>()?;
            for version in &versions {
                for parent in version.parents() {
                    let state = self
                        .query_transaction(parent)
                        .await?
                        .ok_or(Error::MissingTransaction(parent))?;
                    if state.durability < DurabilityTier::Global {
                        pending.push(parent);
                    }
                }
            }
            commits.insert(
                current,
                crate::protocol::AuthorityCommitUnit { tx, versions },
            );
        }
        Ok(crate::protocol::AuthorityPublication {
            tx_id,
            commits: commits.into_values().collect(),
        })
    }

    /// Finalize one publication from a host-authenticated edge authority.
    ///
    /// This trusted-host API is not permission admission for an ordinary
    /// client or relay. Network callers must prove authority capability first.
    /// Individual transactions retain their own fates/global sequence numbers.
    /// Their canonical state persists in one existing Groove batch, before
    /// frontier reconciliation, so cancellation cannot admit only a prefix.
    pub async fn ingest_edge_authority_publication(
        &mut self,
        publication: crate::protocol::AuthorityPublication,
        now_ms: u64,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error> {
        self.require_catalogue_ready()?;
        if publication.commits.is_empty()
            || !publication
                .commits
                .iter()
                .any(|unit| unit.tx.tx_id == publication.tx_id)
            || publication
                .commits
                .windows(2)
                .any(|pair| pair[0].tx.tx_id >= pair[1].tx.tx_id)
        {
            return Err(Error::InvalidStoredValue(
                "authority publication must have an anchor and strictly ordered unique transactions",
            ));
        }
        let mut known = BTreeSet::new();
        let mut affected_rows = BTreeSet::new();
        let units_by_tx = publication
            .commits
            .iter()
            .map(|unit| (unit.tx.tx_id, unit))
            .collect::<BTreeMap<_, _>>();
        for unit in &publication.commits {
            if unit.tx.kind != TxKind::Mergeable
                || !commit_unit_write_count_matches(&unit.tx, unit.versions.len())
                || commit_unit_limit_violation(&unit.versions).is_some()
                || crate::protocol::validate_version_records(&unit.versions).is_err()
                || self
                    .malformed_authored_version_reason(&unit.versions)
                    .is_some()
            {
                return Err(Error::InvalidStoredValue(
                    "authority publication contains an invalid complete mergeable transaction",
                ));
            }
            self.validate_contribution_merge_operation_identities(&unit.tx)?;
            if unit.tx.tx_id.time.physical_ms() > now_ms.saturating_add(SKEW_TOLERANCE_MS) {
                return Err(Error::InvalidStoredValue(
                    "authority publication exceeds permitted clock skew",
                ));
            }
            if let Some(existing) = self.query_transaction(unit.tx.tx_id).await? {
                let mut versions = self
                    .query_versions_for_tx(unit.tx.tx_id)
                    .await?
                    .iter()
                    .map(|row| self.version_record_from_row(row))
                    .collect::<Result<Vec<_>, _>>()?;
                versions.sort();
                if !known_transaction_payload_matches(&existing.tx, &unit.tx)
                    || versions != canonical_versions(unit.versions.clone())
                {
                    return Err(Error::ConflictingCommitUnit(unit.tx.tx_id));
                }
                if matches!(existing.fate, Fate::Rejected(_)) {
                    return Err(Error::ConflictingFate);
                }
            }
            for version in &unit.versions {
                // Never park a member separately: the sender must replay the
                // whole publication after supplying its catalogue/dependencies.
                if !self
                    .catalogue
                    .catalogue_schemas
                    .contains_key(&version.schema_version())
                {
                    return Err(Error::InvalidCatalogueUpdate(
                        "authority publication requires its complete schema context",
                    ));
                }
                for parent in version.parents() {
                    if unit.tx.tx_id.time <= parent.time {
                        return Err(Error::InvalidStoredValue(
                            "authority publication violates parent clock order",
                        ));
                    }
                    if !known.contains(&parent) {
                        let state = self
                            .query_transaction(parent)
                            .await?
                            .ok_or(Error::MissingTransaction(parent))?;
                        if state.fate != Fate::Accepted
                            || state.durability != DurabilityTier::Global
                        {
                            return Err(Error::InvalidStoredValue(
                                "authority publication omits an unsettled parent",
                            ));
                        }
                    }
                    let coordinate = ParentCoordinate {
                        physical_table_id: self.physical_table_id_for_schema(
                            version.schema_version(),
                            version.table(),
                        )?,
                        branch_key: version.branch_key().clone(),
                        row_uuid: version.row_uuid(),
                        layer: VersionLayer::for_record(version),
                    };
                    if let Some(parent_unit) = units_by_tx.get(&parent) {
                        let mut matched = false;
                        for candidate in &parent_unit.versions {
                            matched |= self
                                .version_record_matches_parent_coordinate(candidate, &coordinate)?;
                        }
                        if !matched {
                            return Err(Error::InvalidMergeableCommit(
                                "authority publication parent belongs to another row, branch, or layer",
                            ));
                        }
                    } else if self
                        .validate_known_parent_coordinate(parent, &coordinate)
                        .await?
                        != ParentCoordinateValidation::Exact
                    {
                        return Err(Error::InvalidMergeableCommit(
                            "authority publication requires a complete parent coordinate",
                        ));
                    }
                }
            }
            affected_rows.extend(self.merge_rows_for_versions(&unit.versions)?);
            known.insert(unit.tx.tx_id);
        }
        let versions = publication
            .commits
            .iter()
            .flat_map(|unit| &unit.versions)
            .cloned()
            .collect::<Vec<_>>();
        self.prepare_authored_schema_variants_for_commit(&versions)
            .await?;
        let complete_parents = publication
            .commits
            .iter()
            .map(|unit| (unit.tx.tx_id, unit.versions.clone()))
            .collect::<Vec<_>>();
        let mut batch = self.database.open_batch();
        self.preflight_complete_parent_batch(&mut batch, &complete_parents)
            .await?;
        let mut staged_tx_ids = BTreeSet::new();
        let mut staged_global_times = Vec::new();
        let mut content_versions = Vec::new();
        let mut outcome = PublicationOutcome::settled(Vec::new());
        for unit in publication.commits {
            let tx_id = unit.tx.tx_id;
            if let Some(existing) = self.query_transaction(tx_id).await?
                && existing.fate == Fate::Accepted
                && existing.durability == DurabilityTier::Global
            {
                outcome.value.push(SyncMessage::FateUpdate {
                    tx_id,
                    fate: Fate::Accepted,
                    global_time: existing.global_time,
                    durability: Some(DurabilityTier::Global),
                });
                continue;
            }
            let global_time = self
                .clock
                .allocate_global_time(GlobalTime::authority_now_ms(
                    now_ms,
                    tx_id.time.physical_ms(),
                ))?;
            let staged_versions = self
                .stage_transaction_and_versions_with_current_indexes(
                    &mut batch,
                    unit.tx,
                    canonical_versions(unit.versions),
                    Fate::Accepted,
                    Some(global_time),
                    DurabilityTier::Global,
                    true,
                    false,
                    Some(&mut content_versions),
                )
                .await?;
            self.finalize_staged_transaction_ingest(
                &mut batch,
                Fate::Accepted,
                Some(global_time),
                &mut staged_global_times,
                &staged_versions,
            )
            .await?;
            staged_tx_ids.insert(tx_id);
            outcome.value.push(SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Accepted,
                global_time: Some(global_time),
                durability: Some(DurabilityTier::Global),
            });
        }
        self.write_merge_heads_for_bulk_content_versions(&mut batch, &content_versions)
            .await?;
        if !batch.is_empty() {
            batch.deliver_notifications(groove::db::NotificationTiming::AfterPersistence);
            let applied = self.database.apply_batch(batch).await?;
            let persisted = applied.persist().await;
            self.database.finish_persistence(persisted)?;
            for tx_id in &staged_tx_ids {
                self.invalidate_tx_version_tables_cache(*tx_id);
            }
            self.settle_completed_parent_batch(&staged_tx_ids).await?;
            if let Some(time) = staged_tx_ids.iter().map(|tx_id| tx_id.time).max() {
                self.persist_storage_consistency_marker_through(time)
                    .await?;
            }
        }
        outcome.append_outcome(
            self.create_merge_versions_for_rows(
                affected_rows.into_iter().collect(),
                MergeAuthority::Core,
            )
            .await?,
        );
        Ok(outcome)
    }
}
