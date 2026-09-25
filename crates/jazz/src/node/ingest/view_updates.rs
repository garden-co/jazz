impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Apply an accepted write to the row's post-image, one column at a time.
    ///
    /// Plain columns are last-writer-wins per column: a write sets each column
    /// it authored unless a later write (by `(tx_time, node)`) already set that
    /// column. `_deletion` is one more column. The row carries the identity
    /// and update provenance of its newest write. Returns `None` when the
    /// post-image does not change.
    pub(super) async fn merged_global_post_image(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table_schema: &TableSchema,
        incoming: &VersionRow,
        incoming_tx: TxId,
    ) -> Result<Option<VersionRow>, Error> {
        let Some(previous) = self
            .query_global_winner_in_batch(
                batch,
                schema_version,
                &table_schema.name,
                incoming.branch_key(),
                incoming.row_uuid(),
            )
            .await?
        else {
            return Ok(Some(incoming.clone()));
        };
        let previous_tx = self.version_tx_id(&previous)?;
        if previous_tx == incoming_tx {
            return Ok(None);
        }
        let incoming_is_newest = incoming_tx > previous_tx;
        if previous.schema_version_alias() != incoming.schema_version_alias() {
            // Different authored layouts: keep whole-row last-writer-wins
            // until post-images are stored in physical form.
            return Ok(incoming_is_newest.then(|| incoming.clone()));
        }
        let authored = self.authored_columns_for_version(incoming)?;
        let authors = |name: &str| authored.as_ref().is_none_or(|columns| columns.contains(name));
        // Columns set by writes newer than this one keep their value. Only a
        // late write needs this history scan.
        let mut newer_sets = BTreeSet::<String>::new();
        let mut newer_sets_everything = false;
        if !incoming_is_newest {
            for version in self
                .query_row_versions_in_branch(
                    &table_schema.name,
                    incoming.branch_key(),
                    incoming.row_uuid(),
                )
                .await?
            {
                if self.version_tx_id(&version)? <= incoming_tx {
                    continue;
                }
                match self.authored_columns_for_version(&version)? {
                    Some(columns) => newer_sets.extend(columns),
                    None => newer_sets_everything = true,
                }
            }
        }
        let wins = |name: &str| {
            authors(name) && !newer_sets_everything && !newer_sets.contains(name)
        };
        // The post-image keeps the incoming write's identity: it is the row
        // as of this write's seq. Only cells that a newer write set keep
        // their value from the previous image.
        let mut merged = incoming.record.to_values()?;
        let previous_values = previous.record.to_values()?;
        let keep = |merged: &mut Vec<Value>, index: usize| {
            merged[index] = previous_values[index].clone();
        };
        if !wins(DELETION_COLUMN_NAME) {
            keep(&mut merged, HistoryRowRecord::FIELD__DELETION_IDX);
        }
        for (index, column) in table_schema.columns.iter().enumerate() {
            let index = HistoryRowRecord::USER_CELLS + index;
            let strategy = table_schema.merge_strategy(&column.name);
            if strategy != crate::schema::MergeStrategy::Lww {
                // Merge columns are ops: they apply in seq order whatever
                // their stamps.
                merged[index] = if authors(&column.name) {
                    // History cells are stored nullable.
                    Value::Nullable(Some(Box::new(crate::node::merge_ops::apply_merge_op(
                        strategy,
                        &column.column_type,
                        &previous_values[index],
                        &merged[index],
                    )?)))
                } else {
                    previous_values[index].clone()
                };
            } else if !wins(&column.name) {
                keep(&mut merged, index);
            }
        }
        if !incoming_is_newest {
            keep(&mut merged, HistoryRowRecord::FIELD_UPDATED_BY_IDX);
            keep(&mut merged, HistoryRowRecord::FIELD_UPDATED_AT_IDX);
        }
        for index in [
            HistoryRowRecord::FIELD_CREATED_BY_IDX,
            HistoryRowRecord::FIELD_CREATED_AT_IDX,
        ] {
            keep(&mut merged, index);
        }
        incoming.with_record_values(merged).map(Some)
    }

    pub(super) async fn global_current_seq_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<GlobalTime>, Error> {
        let current_table = self.physical_current_table_for_schema(
            schema_version,
            table,
            PhysicalCurrentClass::Global,
        )?;
        let Some(raw) = self
            .database
            .primary_key_get_raw_in_batch(
                batch,
                &current_table,
                &[
                    Value::Bytes(branch_key.canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                ],
            )
            .await?
        else {
            return Ok(None);
        };
        Ok(
            match raw.record().get_idx(GlobalCurrentRowRecord::FIELD_GLOBAL_TIME_IDX)? {
                Value::U64(seq) => Some(GlobalTime(seq)),
                _ => None,
            },
        )
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
        // History holds post-images: the accepted image with the newest seq
        // is the row.
        let mut winner = None::<(VersionRow, GlobalTime)>;
        for version in self
            .query_row_versions_in_branch(table, branch_key, row_uuid)
            .await?
            .into_iter()
        {
            let tx_id = self.version_tx_id(&version)?;
            let Some(tx) = self.query_transaction(tx_id).await? else {
                continue;
            };
            let (Fate::Accepted, Some(global_time)) = (&tx.fate, tx.global_time) else {
                continue;
            };
            if winner.as_ref().is_none_or(|(_, best)| *best < global_time) {
                winner = Some((version, global_time));
            }
        }
        Ok(winner.map(|(version, _)| version))
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

    pub(super) fn write_history_post_image(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<(), Error> {
        let (history_table, record) = self.version_storage_write_binding(version)?;
        batch.update_raw(
            history_table.as_ref(),
            self.version_storage_primary_key(version)?,
            record,
        );
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
        let overlay_key = self.ahead_overlay_key(version)?;
        if self.ahead_current_keys.contains_key(&overlay_key) {
            self.mark_ahead_shadow_dirty(schema_version, version);
        }
        Ok(())
    }

    fn mark_ahead_shadow_dirty(&mut self, schema_version: SchemaVersionId, version: &VersionRow) {
        self.ahead_shadow_dirty.push((
            schema_version,
            version.table().to_owned(),
            version.branch_key().clone(),
            version.row_uuid(),
        ));
    }

    /// Bring the shadow copy of every row touched in this batch in line:
    /// a row with an overlay shadows its synced image (as the batch leaves
    /// it), a row without one has no shadow.
    pub(in crate::node) async fn flush_ahead_shadows(
        &mut self,
        batch: &mut DatabaseBatch,
    ) -> Result<(), Error> {
        if self.ahead_shadow_dirty.is_empty() {
            return Ok(());
        }
        let mut dirty = std::mem::take(&mut self.ahead_shadow_dirty);
        dirty.sort_by(|a, b| (&a.1, &a.2, a.3).cmp(&(&b.1, &b.2, b.3)));
        dirty.dedup_by(|a, b| a.1 == b.1 && a.2 == b.2 && a.3 == b.3);
        for (schema_version, table, branch_key, row_uuid) in dirty {
            let table_id = self.physical_table_id_for_schema(schema_version, &table)?;
            let primary_key = global_current_primary_key(&branch_key, row_uuid);
            let shadow = self.physical_current_table_for_schema(
                schema_version,
                &table,
                PhysicalCurrentClass::AheadShadow,
            )?;
            if !self
                .ahead_current_keys
                .contains_key(&(table_id, primary_key.clone().into_bytes()))
            {
                batch.delete(shadow, primary_key);
                continue;
            }
            let global = self.physical_current_table_for_schema(
                schema_version,
                &table,
                PhysicalCurrentClass::Global,
            )?;
            let raw = self
                .database
                .primary_key_get_raw_in_batch(
                    batch,
                    &global,
                    &[
                        Value::Bytes(branch_key.canonical_bytes()),
                        Value::Uuid(row_uuid.0),
                    ],
                )
                .await?;
            match raw {
                Some(raw) => {
                    let (_, record) = raw.into_variant_parts();
                    batch.update_raw(shadow, primary_key, record);
                }
                None => batch.delete(shadow, primary_key),
            }
        }
        Ok(())
    }

    /// The client overlay holds one row per row: the newest pending local
    /// image. Each local image is already built over the previous visible row
    /// (overlay or synced), so the newest pending image is the fold of every
    /// pending patch for that row. Replays and older images are ignored.
    pub(super) fn write_ahead_current_insert(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let physical_table_id =
            self.physical_table_id_for_schema(schema_version, version.table())?;
        let tx_id = self.version_tx_id(version)?;
        let overlay_key = (
            physical_table_id,
            global_current_primary_key(version.branch_key(), version.row_uuid()).into_bytes(),
        );
        if self
            .ahead_current_keys
            .get(&overlay_key)
            .is_some_and(|existing| *existing >= tx_id)
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
        batch.update_raw(
            plan.storage_table.clone(),
            global_current_primary_key(version.branch_key(), version.row_uuid()),
            physical,
        );
        if self.ahead_current_keys.insert(overlay_key, tx_id).is_none() {
            self.mark_ahead_shadow_dirty(schema_version, version);
        }
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

    /// Drop the overlay row when it holds this version's image. An overlay
    /// holding a newer pending image already includes this one's effect.
    /// Returns whether the overlay was dropped.
    pub(super) fn write_ahead_current_delete(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<bool, Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let physical_table_id =
            self.physical_table_id_for_schema(schema_version, version.table())?;
        let tx_id = self.version_tx_id(version)?;
        let primary_key = global_current_primary_key(version.branch_key(), version.row_uuid());
        let overlay_key = (physical_table_id, primary_key.clone().into_bytes());
        if self.ahead_current_keys.get(&overlay_key) != Some(&tx_id) {
            return Ok(false);
        }
        let table = self.physical_current_table_for_schema(
            schema_version,
            version.table(),
            PhysicalCurrentClass::Ahead,
        )?;
        batch.delete(table, primary_key);
        self.ahead_current_keys.remove(&overlay_key);
        self.mark_ahead_shadow_dirty(schema_version, version);
        Ok(true)
    }

    /// After a rejected image leaves the overlay, the newest remaining
    /// pending image for the row (if any) takes its place.
    pub(super) async fn restore_ahead_overlay_after_reject(
        &mut self,
        batch: &mut DatabaseBatch,
        rejected: &VersionRow,
    ) -> Result<(), Error> {
        let settling = BTreeSet::from([self.version_tx_id(rejected)?]);
        self.recompute_ahead_overlay(batch, rejected, &settling).await
    }

    /// Rebuild one row's ahead overlay: its synced image with the row's
    /// still-pending local patches folded on top in tx order. Pending patches
    /// store merge columns as ops, so the overlay is never a patch copy.
    pub(super) async fn recompute_ahead_overlay(
        &mut self,
        batch: &mut DatabaseBatch,
        row: &VersionRow,
        settling: &BTreeSet<TxId>,
    ) -> Result<(), Error> {
        let mut pending = Vec::new();
        for version in self
            .query_row_versions_in_branch(row.table(), row.branch_key(), row.row_uuid())
            .await?
        {
            let tx_id = self.version_tx_id(&version)?;
            if !settling.contains(&tx_id)
                && matches!(
                    self.query_transaction_state(tx_id).await?,
                    Some((Fate::Pending, None, _))
                )
            {
                pending.push((tx_id, version));
            }
        }
        pending.sort_by_key(|(tx_id, _)| *tx_id);
        let Some((_, first)) = pending.first() else {
            return Ok(());
        };
        let schema_version = self
            .schema_version_for_alias(first.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "pending version schema alias must exist",
            ))?;
        // A renamed table names the row differently in the pending patch's
        // schema than in the settling version's.
        let table_schema = self.table_in_schema(first.table(), schema_version)?;
        let mut image = self
            .query_global_winner_in_batch(
                batch,
                schema_version,
                &table_schema.name,
                row.branch_key(),
                row.row_uuid(),
            )
            .await?;
        for (_, patch) in pending {
            image = Some(match image {
                Some(base) if base.schema_version_alias() == patch.schema_version_alias() => {
                    self.fold_pending_patch(&table_schema, &base, &patch)?
                }
                _ => patch,
            });
        }
        if let Some(image) = image {
            let key = self.ahead_overlay_key(&image)?;
            self.ahead_current_keys.remove(&key);
            self.write_ahead_current_insert(batch, &image)?;
        }
        Ok(())
    }

    /// A new synced image rebases any other pending local patches on its row.
    pub(super) async fn rebase_ahead_overlays(
        &mut self,
        batch: &mut DatabaseBatch,
        synced: &[VersionRow],
        settling: &BTreeSet<TxId>,
    ) -> Result<(), Error> {
        for version in synced {
            if self.has_foreign_ahead_overlay(version, self.version_tx_id(version)?)? {
                self.recompute_ahead_overlay(batch, version, settling).await?;
            }
        }
        Ok(())
    }

    fn ahead_overlay_key(&self, version: &VersionRow) -> Result<(crate::ids::PhysicalTableId, Vec<u8>), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let physical_table_id =
            self.physical_table_id_for_schema(schema_version, version.table())?;
        Ok((
            physical_table_id,
            global_current_primary_key(version.branch_key(), version.row_uuid()).into_bytes(),
        ))
    }

    /// Whether a pending overlay row other than `tx_id`'s own image covers
    /// this row, so a new synced image must be rebased under it.
    pub(super) fn has_foreign_ahead_overlay(&self, version: &VersionRow, tx_id: TxId) -> Result<bool, Error> {
        let key = self.ahead_overlay_key(version)?;
        Ok(self.ahead_current_keys.get(&key).is_some_and(|overlay| *overlay != tx_id))
    }

    /// Apply one pending patch over an image of the same layout: authored
    /// plain columns replace, merge columns apply their op.
    fn fold_pending_patch(
        &self,
        table_schema: &TableSchema,
        base: &VersionRow,
        patch: &VersionRow,
    ) -> Result<VersionRow, Error> {
        let authored = self.authored_columns_for_version(patch)?;
        let authors = |name: &str| authored.as_ref().is_none_or(|columns| columns.contains(name));
        let mut folded = patch.record.to_values()?;
        let base_values = base.record.to_values()?;
        if !authors(DELETION_COLUMN_NAME) {
            folded[HistoryRowRecord::FIELD__DELETION_IDX] =
                base_values[HistoryRowRecord::FIELD__DELETION_IDX].clone();
        }
        for (index, column) in table_schema.columns.iter().enumerate() {
            let index = HistoryRowRecord::USER_CELLS + index;
            let strategy = table_schema.merge_strategy(&column.name);
            folded[index] = match (authors(&column.name), strategy) {
                (false, _) => base_values[index].clone(),
                (true, crate::schema::MergeStrategy::Lww) => continue,
                (true, strategy) => Value::Nullable(Some(Box::new(
                    crate::node::merge_ops::apply_merge_op(
                        strategy,
                        &column.column_type,
                        &base_values[index],
                        &folded[index],
                    )?,
                ))),
            };
        }
        for index in [
            HistoryRowRecord::FIELD_CREATED_BY_IDX,
            HistoryRowRecord::FIELD_CREATED_AT_IDX,
        ] {
            folded[index] = base_values[index].clone();
        }
        patch.with_record_values(folded)
    }

    /// Drop the overlays a fated transaction owns, refolding any other
    /// still-pending patches on those rows over the synced image.
    pub(super) async fn cleanup_fated_ahead_current_for_versions(
        &mut self,
        batch: &mut DatabaseBatch,
        versions: &[VersionRow],
    ) -> Result<(), Error> {
        let mut settling = BTreeSet::new();
        for version in versions {
            settling.insert(self.version_tx_id(version)?);
        }
        for version in versions {
            if self.write_ahead_current_delete(batch, version)? {
                self.recompute_ahead_overlay(batch, version, &settling)
                    .await?;
            }
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
        // Recovery runs before the open path indexes the overlay rows, and
        // the delete below only drops an overlay this transaction owns.
        self.rebuild_ahead_current_keys().await?;
        let settling = tx_ids.iter().copied().collect::<BTreeSet<_>>();
        let mut batch = self.database.open_batch();
        for tx_id in &tx_ids {
            for version in self.query_versions_for_tx(*tx_id).await? {
                if self.write_ahead_current_delete(&mut batch, &version)? {
                    // Other still-pending patches on the row keep an overlay.
                    self.recompute_ahead_overlay(&mut batch, &version, &settling)
                        .await?;
                }
            }
        }
        self.flush_ahead_shadows(&mut batch).await?;
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
