impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Capture each accepted content row's global current state before a
    /// Core decision, so [`Self::create_fold_versions_for`] can check the
    /// linear fold afterwards.
    pub(super) async fn fold_candidates_for_versions(
        &mut self,
        records: &[VersionRecord],
    ) -> Result<Vec<FoldCandidate>, Error> {
        let mut candidates = Vec::with_capacity(records.len());
        for record in records {
            let (projected_schema, table) = self.translate_cells_to_current_write_schema(
                record.schema_version(),
                record.table(),
                &mut BTreeMap::new(),
            )?;
            // Fold versions are authored in the current write schema. A
            // version in an unreconciled schema has its own physical lineage
            // and cannot be folded into the write schema until a lens exists.
            if projected_schema != self.catalogue.active_schema.schema {
                continue;
            }
            let previous = self
                .query_global_winner_in_branch(
                    &table,
                    record.branch_key(),
                    record.row_uuid(),)
                .await?;
            candidates.push(FoldCandidate {
                table,
                branch_key: record.branch_key().clone(),
                row_uuid: record.row_uuid(),
                previous,
            });
        }
        Ok(candidates)
    }

    /// Linear history: an accepted write applies exactly its authored columns
    /// on top of the row Core held before it. Winner selection installs whole
    /// versions, so when a concurrent or late write leaves the installed row
    /// different from that fold, Core mints one parentless fold version that
    /// carries the folded row. Nothing is minted in the common, uncontended
    /// case.
    pub(super) async fn create_fold_versions_for(
        &mut self,
        tx_id: TxId,
        candidates: Vec<FoldCandidate>,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error> {
        let mut outcome = PublicationOutcome::settled(Vec::new());
        if candidates.is_empty() {
            return Ok(outcome);
        }
        let authored = self.query_versions_for_tx(tx_id).await?;
        for candidate in candidates {
            let Some(version) = authored.iter().find(|version| {
                version.row_uuid() == candidate.row_uuid
                    && version.branch_key() == &candidate.branch_key
            }) else {
                continue;
            };
            let created =
                Box::pin(self.create_fold_version_if_needed(candidate, version.clone())).await?;
            outcome.append_outcome(created);
        }
        Ok(outcome)
    }

    async fn create_fold_version_if_needed(
        &mut self,
        candidate: FoldCandidate,
        authored: VersionRow,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error> {
        let FoldCandidate {
            table,
            branch_key,
            row_uuid,
            previous,
        } = candidate;
        let table_schema = self.table_in_schema(&table, self.catalogue.active_schema.schema)?;
        let Some(installed) = self
            .query_global_winner_in_branch(&table, &branch_key, row_uuid)
            .await?
        else {
            return Ok(PublicationOutcome::settled(Vec::new()));
        };
        let authored_columns = self.authored_columns_for_version(&authored)?;
        let mut folded = match previous.as_ref() {
            Some(previous) => previous.cells(&table_schema)?,
            None => BTreeMap::new(),
        };
        for column in &table_schema.columns {
            if authored_columns
                .as_ref()
                .is_some_and(|columns| !columns.contains(&column.name))
            {
                continue;
            }
            if let Some(value) = authored.cell(&table_schema, &column.name)? {
                folded.insert(column.name.clone(), value);
            }
        }
        // `_deletion` folds like any other cell: only a delete/restore
        // authors it, so a concurrent content write cannot resurrect a row.
        let authors_deletion = authored_columns
            .as_ref()
            .is_none_or(|columns| columns.contains(DELETION_COLUMN_NAME));
        let folded_deleted = if authors_deletion {
            authored.is_deleted()
        } else {
            previous.as_ref().is_some_and(VersionRow::is_deleted)
        };
        let installed_cells = installed.cells(&table_schema)?;
        let differing = table_schema
            .columns
            .iter()
            .filter(|column| folded.get(&column.name) != installed_cells.get(&column.name))
            .map(|column| column.name.clone())
            .collect::<BTreeSet<_>>();
        let deletion_differs = folded_deleted != installed.is_deleted();
        if (differing.is_empty() || folded.is_empty()) && !deletion_differs {
            return Ok(PublicationOutcome::settled(Vec::new()));
        }
        let installed_at = self.version_made_at(&installed).await?;
        let authored_at = self.version_made_at(&authored).await?;
        let made_at = installed_at.max(authored_at).tick_after()?;
        self.merge_tx_time(made_at);
        let fold_tx_id = TxId::new(made_at, self.node_uuid);
        if self.query_transaction(fold_tx_id).await?.is_some() {
            return Ok(PublicationOutcome::settled(Vec::new()));
        }
        let schema = &self
            .catalogue
            .catalogue_schemas
            .get(&self.catalogue.active_schema.schema)
            .expect("current write schema exists")
            .schema;
        let branch = schema
            .branch_selector_for_key(&table_schema, &branch_key)
            .map_err(Error::InvalidBranchKey)?;
        let mut fold_commit = MergeableCommit::new(&table, row_uuid, made_at.physical_ms())
            .branch(branch)
            .cells(folded)
            .authored_columns(differing);
        if deletion_differs {
            fold_commit = fold_commit.deletion(if folded_deleted {
                DeletionEvent::Deleted
            } else {
                DeletionEvent::Restored
            });
        }
        let publication = Box::pin(self.commit_mergeable_at(fold_commit, made_at)).await?;
        let work = Box::pin(self.resident_commit_unit(publication.tx_id)).await?;
        Ok(PublicationOutcome::published_then(Vec::new(), publication, work))
    }


    async fn query_global_winner_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,) -> Result<Option<VersionRow>, Error> {
        let current_table = self.physical_current_table_for_schema(
            schema_version,
            table,
            PhysicalCurrentClass::Global,)?;
        let raw = self.database.primary_key_get_raw_in_batch(
            batch,
            &current_table,
            &[
                Value::Bytes(branch_key.canonical_bytes()),
                Value::Uuid(row_uuid.0),
            ],
        )
        .await?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let current = raw.owned_record();
        if let Some(winner) =
            self.history_image_from_current_record(schema_version, table, current.borrowed())?
        {
            return Ok(Some(winner));
        }
        let record = current.borrowed();
        let tx_time = TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?);
        let tx_node_alias =
            NodeAlias(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
        self.query_version_by_alias_in_batch(
            batch,
            schema_version,
            table,
            branch_key,
            row_uuid,
            tx_time,
            tx_node_alias,)
        .await
    }

    async fn query_version_by_alias_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,) -> Result<Option<VersionRow>, Error> {
        for storage_table in self.version_storage_sources(table)? {
            let _ = schema_version;
            let key = vec![
                Value::Bytes(branch_key.canonical_bytes()),
                Value::Uuid(row_uuid.0),
                Value::U64(tx_time.0),
                Value::U64(tx_node_alias.0),
            ];
            let raw = self
                .database
                .primary_key_get_raw_in_batch(batch, &storage_table, &key).await?;
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

    #[cfg(test)]
    async fn recomputed_global_winner_from_history_for_test(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,) -> Result<Option<VersionRow>, Error> {
        let mut winner = None::<(VersionRow, TxId, TxTime)>;
        for version in self
            .query_row_versions_in_branch(table, branch_key, row_uuid)
            .await?
            .into_iter()
        {
            let tx_id = self.version_tx_id(&version)?;
            let Some(tx) = self.query_transaction(tx_id).await? else {
                continue;
            };
            if !matches!(tx.fate, Fate::Accepted) || tx.global_time.is_none() {
                continue;
            }
            let made_at = self.version_made_at(&version).await?;
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
    async fn assert_global_current_updates_match_history_for_test(
        &mut self,
        updates: &[(VersionRow, GlobalTime)],
    ) -> Result<(), Error> {
        for (version, global_time) in updates {
            let Some(expected) = self.recomputed_global_winner_from_history_for_test(
                version.table(),
                version.branch_key(),
                version.row_uuid(),)
            .await?
            else {
                panic!(
                    "global-current update has no accepted history winner for {}/ {:?}",
                    version.table(),
                    version.row_uuid(),
                );
            };
            let expected_tx = self.version_tx_id(&expected)?;
            let actual_tx = self.version_tx_id(version)?;
            if expected_tx != actual_tx {
                panic!(
                    "global-current update diverged from history for {}/{:?}: expected winner {:?}, actual update {:?}",
                    version.table(),
                    version.row_uuid(),
                    expected_tx,
                    actual_tx
                );
            }
            self.assert_global_current_row_matches_version_for_test(version, *global_time)
                .await?;
            self.assert_global_change_row_matches_version_for_test(version, *global_time)
                .await?;
        }
        Ok(())
    }

    #[cfg(test)]
    async fn assert_global_current_row_matches_version_for_test(
        &mut self,
        version: &VersionRow,
        global_time: GlobalTime,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let table = self
            .table_in_schema(version.table(), schema_version)?
            .clone();
        let current_schema = table.global_current_storage_table();
        let current_table = groove::Intern::new(self.physical_current_table_for_schema(
            schema_version,
            version.table(),
            PhysicalCurrentClass::Global,
        )?);
        let expected_values = self.public_current_values(&table, version, Some(global_time))?;
        let rows = self
            .database
            .primary_key_scan_raw(
                current_table.as_ref(),
                &[
                    Value::Bytes(version.branch_key().canonical_bytes()),
                    Value::Uuid(version.row_uuid().0),
                ],
            )
            .await?;
        let actual = rows.first().map(|row| row.record().raw().to_vec());
        let expected = owned_record_from_storage_values(&current_schema, expected_values)?
            .raw()
            .to_vec();
        if actual.as_deref() != Some(expected.as_slice()) {
            panic!(
                "global-current row diverged for {}/{:?}: expected {:?}, actual {:?}",
                version.table(),
                version.row_uuid(),
                expected,
                actual
            );
        }
        Ok(())
    }

    #[cfg(test)]
    async fn assert_global_change_row_matches_version_for_test(
        &mut self,
        version: &VersionRow,
        global_time: GlobalTime,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let table_id = self.physical_table_id_for_schema(schema_version, version.table())?;
        let rows = self.database.primary_key_scan_raw(
            "jazz_global_changes",
            &[
                Value::U64(table_id.0),
                Value::Bytes(version.branch_key().canonical_bytes()),
                Value::Uuid(version.row_uuid().0),
                Value::U64(global_time.0),
            ],
        )
        .await?;
        let Some(row) = rows.first() else {
            panic!(
                "missing global-change row for {}/{:?} at {:?}",
                version.table(),
                version.row_uuid(),
                global_time
            );
        };
        let record = row.record();
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
        if actual_tx != expected_tx {
            panic!(
                "global-change row diverged for {}/{:?} at {:?}: expected tx {:?}, actual tx {:?}",
                version.table(),
                version.row_uuid(),
                global_time,
                expected_tx,
                actual_tx,
            );
        }
        Ok(())
    }

    pub(super) fn write_global_current_update(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
        global_time: GlobalTime,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let plan = self.prepared_physical_write_plan(
            schema_version,
            version.table(),
            PhysicalWriteTarget::GlobalCurrent,
        )?;
        // Validate node-local authored column aliases before deriving
        // the current carrier; encoding itself retains trusted bytes.
        let _ = self.authored_columns_for_version(version)?;
        let physical = self.encode_physical_version_record(&plan, version, Some(global_time))?;
        batch.update_raw(
            plan.storage_table.clone(),
            global_current_primary_key(version.branch_key(), version.row_uuid()),
            physical,
        );
        batch.update(
            "jazz_global_changes",
            global_change_values(
                self.physical_table_id_for_schema(schema_version, version.table())?,
                version,
                global_time,
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
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let physical_table_id =
            self.physical_table_id_for_schema(schema_version, version.table())?;
        let encoded_primary_key = history_primary_key(version).into_bytes();
        if self
            .ahead_current_keys
            .contains(&(physical_table_id, encoded_primary_key.clone()))
        {
            return Ok(());
        }
        let plan = self.prepared_physical_write_plan(
            schema_version,
            version.table(),
            PhysicalWriteTarget::AheadCurrent,
        )?;
        let _ = self.authored_columns_for_version(version)?;
        let physical = self.encode_physical_version_record(&plan, version, None)?;
        batch.insert_raw(
            plan.storage_table.clone(),
            history_primary_key(version),
            physical,
        );
        self.insert_ahead_current_key(
            physical_table_id,
            encoded_primary_key,);
        Ok(())
    }

    /// Build the physical current-source carrier consumed by Groove terminals.
    #[cfg(test)]
    fn public_current_values(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
        global_time: Option<GlobalTime>,
    ) -> Result<Vec<Value>, Error> {
        // Current carriers are derived durable state. Validate the compact
        // authored-column ids against this row's exact authored schema/table
        // before copying them into either ahead or global current storage.
        // The returned logical names are not stored here; this is solely the
        // local-alias integrity boundary.
        let _ = self.authored_columns_for_version(version)?;
        global_current_values(table, version, global_time)
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
            PhysicalCurrentClass::Ahead,)?;
        batch.delete(table, history_primary_key(version));
        self.remove_ahead_current_key(
            self.physical_table_id_for_schema(schema_version, version.table())?,
            history_primary_key(version).into_bytes(),);
        Ok(())
    }

    /// Once a transaction is rejected or globally settled, it must not remain
    /// in the ahead-current overlay: accepted global effects live in current
    /// tables, and rejected effects are no longer visible. Pending local
    /// transactions remain in this overlay until Core supplies a final fate.
    /// Outbox/redelivery may keep the commit unit until fate arrives, so
    /// callers invoke this strictly after the cleanup-triggering fate is durable.
    pub(super) async fn cleanup_fated_ahead_current_for_tx(
        &mut self,
        batch: &mut DatabaseBatch,
        tx_id: TxId,
    ) -> Result<(), Error> {
        let versions = self.query_versions_for_tx(tx_id).await?;
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

    pub(super) async fn cleanup_settled_ahead_current_leftovers(
        &mut self,
        already_consistent_through: Option<TxTime>,
    ) -> Result<(), Error> {
        let mut tx_ids = Vec::new();
        for raw in self
            .database
            .primary_key_scan_raw("jazz_transactions", &[])
            .await?
        {
            let record = raw.record();
            let fate = fate_from_encoded_fields(record)?;
            let global_time = record.get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_TIME_IDX)?;
            if !matches!(fate, Fate::Rejected(_)) && global_time.is_none() {
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
            self.cleanup_fated_ahead_current_for_tx(&mut batch, *tx_id)
                .await?;
        }
        let applied = self.database.apply_batch(batch).await?;
let persisted = applied.persist().await;
self.database.finish_persistence(persisted)?;
        if let Some(tx_time) = tx_ids.into_iter().map(|tx_id| tx_id.time).max() {
            self.persist_storage_consistency_marker_through(tx_time)
                .await?;
        }
        Ok(())
    }

    async fn prune_ahead_current_for_global_time(
        &mut self,
        batch: &mut DatabaseBatch,
        global_time: GlobalTime,
    ) -> Result<(), Error> {
        let mut tx_ids = Vec::new();
        for raw in self.database.index_scan_raw(
            "jazz_transactions",
            "by_global_time",
            &[Value::U64(global_time.0)],
        )
        .await?
        {
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
            for version in self.query_versions_for_tx(tx_id).await? {
                self.write_ahead_current_delete(batch, &version)?;
            }
        }
        Ok(())
    }

}
