impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Apply an accepted write to the row's post-image, one column at a time.
    ///
    /// Plain columns are last-writer-wins per column by stamp. The write's
    /// stamp is its transaction time clamped to the seq it was accepted at:
    /// `min(tx physical ms, seq physical ms)`. Core mints the seq from its own
    /// clock when it receives the write, so this is Core's zero-tolerance
    /// clamp; every node replaying the same seq computes the same stamp. A
    /// write sets each plain column it authored iff its stamp is at least the
    /// column's stored stamp; ties go to the higher seq (writes apply in seq
    /// order, so normally the incoming write). `_deletion` is one more stamped
    /// column. Merge columns apply their op whatever the stamps. The row
    /// keeps this write's identity (it is the row as of this seq); its
    /// `updated_by`/`updated_at` follow the write with the highest stamp.
    /// Returns `None` when the post-image does not change.
    pub(super) async fn merged_global_post_image(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table_schema: &TableSchema,
        incoming: &VersionRow,
        incoming_tx: TxId,
        global_time: GlobalTime,
    ) -> Result<Option<VersionRow>, Error> {
        use crate::node::col_stamps::{ColumnStamps, StampSlots};

        let stamp = incoming
            .tx_time()
            .physical_ms()
            .min(global_time.physical_ms());
        let slots = StampSlots::for_table(table_schema);
        let authored = self.authored_columns_for_version(incoming)?;
        let authors = |name: &str| authored.as_ref().is_none_or(|columns| columns.contains(name));
        let incoming_descriptor = incoming.record.descriptor();
        let Some((previous, previous_seq)) = self
            .query_global_winner_with_seq_in_batch(
                batch,
                schema_version,
                &table_schema.name,
                incoming.branch_key(),
                incoming.row_uuid(),
            )
            .await?
        else {
            // The first image of a row: the columns this write authored
            // carry its stamp; columns nobody has set yet carry 0.
            let mut stamps = ColumnStamps::uniform(&slots, 0);
            for (index, column) in table_schema.columns.iter().enumerate() {
                if let Some(slot) = slots.column(index)
                    && authors(&column.name)
                {
                    stamps.set(slot, stamp);
                }
            }
            if authors(DELETION_COLUMN_NAME) {
                stamps.set(slots.deletion(), stamp);
            }
            // Over no image, each counter cell's low bits are the written
            // value itself (a delta from zero): the image keeps the cells.
            let mut values = incoming.record.to_values()?;
            stamps.write_values(&mut values, &incoming_descriptor)?;
            crate::node::merge_ops::clear_counter_signs(&mut values, &incoming_descriptor);
            return incoming.with_record_values(values).map(Some);
        };
        let previous_tx = self.version_tx_id(&previous)?;
        if previous_tx == incoming_tx {
            return Ok(None);
        }
        // Ties go to the later seq. Writes apply in seq order, so this write
        // is normally later than every write already in the row; a node that
        // applies an older seq late (an out-of-order fate) lets it lose ties.
        let applies_later = previous_seq.is_none_or(|previous_seq| global_time > previous_seq);
        let beats = |stored: u64| stamp > stored || (stamp == stored && applies_later);
        // An unstamped previous image (legacy, or a lens-translated payload)
        // counts as stamp 0 everywhere: any stamped write may replace it.
        let previous_row_stamp = previous.max_col_stamp()?;
        let incoming_is_newest = beats(previous_row_stamp);
        if previous.schema_version_alias() != incoming.schema_version_alias() {
            // Different authored layouts: keep whole-row last-writer-wins
            // until post-images are stored in physical form. The winner's
            // stamp covers every column of its image.
            if !incoming_is_newest {
                return Ok(None);
            }
            let mut values = incoming.record.to_values()?;
            // A merge column the write did not author holds the writer's
            // snapshot, not an op: it takes the row's settled value, so ops
            // Core accepted since that snapshot survive (`INV-HIST-10`).
            let carried = self
                .cross_schema_settled_merge_cells(
                    schema_version,
                    table_schema,
                    authored.as_ref(),
                    &previous,
                )?
                .map_err(|_| {
                    Error::InvalidStoredValue(
                        "accepted cross-schema write cannot carry a merge column's settled value",
                    )
                })?;
            for (field, value) in carried {
                values[field] = value;
            }
            ColumnStamps::uniform(&slots, stamp).write_values(&mut values, &incoming_descriptor)?;
            crate::node::merge_ops::clear_counter_signs(&mut values, &incoming_descriptor);
            return incoming.with_record_values(values).map(Some);
        }
        let mut stamps = previous
            .col_stamps(table_schema)?
            .unwrap_or_else(|| ColumnStamps::uniform(&slots, 0));
        // A plain column takes this write's value iff the write authored it
        // and its stamp is at least the column's stamp.
        let mut wins = |name: &str, slot: usize| {
            let wins = authors(name) && beats(stamps.get(slot));
            if wins {
                stamps.set(slot, stamp);
            }
            wins
        };
        // The post-image keeps the incoming write's identity: it is the row
        // as of this write's seq. Cells this write did not win keep their
        // value from the previous image.
        let mut merged = incoming.record.to_values()?;
        let previous_values = previous.record.to_values()?;
        let counter_signs = incoming.counter_signs()?;
        let keep = |merged: &mut Vec<Value>, index: usize| {
            merged[index] = previous_values[index].clone();
        };
        if !wins(DELETION_COLUMN_NAME, slots.deletion()) {
            keep(&mut merged, HistoryRowRecord::FIELD__DELETION_IDX);
        }
        for (index, column) in table_schema.columns.iter().enumerate() {
            let field = HistoryRowRecord::USER_CELLS + index;
            match slots.column(index) {
                Some(slot) => {
                    if !wins(&column.name, slot) {
                        keep(&mut merged, field);
                    }
                }
                None => {
                    // Merge columns are ops: they apply in seq order
                    // whatever their stamps.
                    let strategy = table_schema.merge_strategy(&column.name);
                    merged[field] = if authors(&column.name) {
                        // History cells are stored nullable.
                        Value::Nullable(Some(Box::new(
                            crate::node::merge_ops::apply_merge_op(
                                strategy,
                                &column.column_type,
                                &previous_values[field],
                                &merged[field],
                                crate::node::merge_ops::counter_sign(
                                    table_schema,
                                    &counter_signs,
                                    index,
                                ),
                            )?,
                        )))
                    } else {
                        previous_values[field].clone()
                    };
                }
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
        stamps.write_values(&mut merged, &incoming_descriptor)?;
        crate::node::merge_ops::clear_counter_signs(&mut merged, &incoming_descriptor);
        incoming.with_record_values(merged).map(Some)
    }

    /// The authority refuses a write it cannot merge into the row without
    /// losing or inventing merge-column state:
    ///
    /// - A merge-column write over a row whose current image is stored under
    ///   another schema version. Across layouts the row is still whole-row
    ///   last-writer-wins (`merged_global_post_image`), so the write's op (a
    ///   counter delta, a g-set's added elements) would be stored as the
    ///   column's value when the write wins, and dropped when it loses.
    ///   Applying ops across layouts needs Core's current row in one
    ///   physical layout (#3899).
    /// - A write across schema versions that leaves a merge column alone
    ///   when that column's settled value cannot be carried into the write's
    ///   layout unambiguously (`cross_schema_settled_merge_cells`).
    /// - A counter op that would take the column outside its type's range
    ///   over the row's current value. The op is not wrapped.
    pub(super) async fn merge_op_rejection(
        &mut self,
        versions: &[VersionRecord],
    ) -> Result<Option<RejectionReason>, Error> {
        let batch = self.database.open_batch();
        for version in versions {
            let schema_version = version.schema_version();
            let table_schema = self.table_in_schema(version.table(), schema_version)?;
            let authored = version.authored_columns();
            let authors =
                |name: &str| authored.is_none_or(|columns| columns.contains(name));
            let is_merge = |column: &ColumnSchema| {
                table_schema.merge_strategy(&column.name) != crate::schema::MergeStrategy::Lww
            };
            if !table_schema.columns.iter().any(is_merge) {
                continue;
            }
            let Some((current, _)) = self
                .query_global_winner_with_seq_in_batch(
                    &batch,
                    schema_version,
                    version.table(),
                    version.branch_key(),
                    version.row_uuid(),
                )
                .await?
            else {
                continue;
            };
            let not_supported = |column: &str| {
                RejectionReason::MalformedCommit(format!(
                    "merge column '{}.{column}' cannot be merged with the row's current image, which is stored under a different schema version; merge-column writes across schema versions are not supported yet (#3899)",
                    version.table(),
                ))
            };
            if self.schema_version_for_alias(current.schema_version_alias()) != Some(schema_version) {
                if let Some(column) = table_schema
                    .columns
                    .iter()
                    .find(|column| is_merge(column) && authors(&column.name))
                {
                    return Ok(Some(not_supported(&column.name)));
                }
                if let Err(column) = self.cross_schema_settled_merge_cells(
                    schema_version,
                    &table_schema,
                    authored,
                    &current,
                )? {
                    return Ok(Some(not_supported(&column)));
                }
                continue;
            }
            for (index, column) in table_schema.columns.iter().enumerate() {
                if table_schema.merge_strategy(&column.name) != crate::schema::MergeStrategy::Counter
                    || !authors(&column.name)
                {
                    continue;
                }
                let Some(op) = version.optional_cell_at(index) else {
                    continue;
                };
                let negative = crate::node::merge_ops::counter_sign(
                    &table_schema,
                    version.counter_signs(),
                    index,
                );
                let previous = current
                    .cell(&table_schema, &column.name)?
                    .unwrap_or(Value::Nullable(None));
                if crate::node::merge_ops::counter_after_op(
                    &column.column_type,
                    &previous,
                    &op,
                    negative,
                )?
                .is_none()
                {
                    return Ok(Some(RejectionReason::MalformedCommit(format!(
                        "counter '{}.{}' would be out of range for its {:?} column: {} {:+} leaves the type's range, so the write is rejected rather than wrapped",
                        version.table(),
                        column.name,
                        column.column_type,
                        crate::node::merge_ops::counter_to_i128(&previous)?,
                        crate::node::merge_ops::counter_op_delta(
                            &column.column_type,
                            &op,
                            negative,
                        )?,
                    ))));
                }
            }
        }
        Ok(None)
    }

    /// The row's settled values of the merge columns a write across schema
    /// versions did not author, as `(history field, value)` in the write's
    /// layout. The write's own cells there are the writer's snapshot, which
    /// may predate ops Core has accepted since; whole-row last-writer-wins
    /// would silently replace them.
    ///
    /// A column carries the current image's value when the image's schema
    /// has a column of the same name, type and merge strategy that the lens
    /// path between the two versions leaves alone; a column the image's
    /// schema lacks, and that no lens renames or copies into, carries the
    /// lens default (no op can have touched it under the image's layout).
    /// Any other mapping is `Err(column)`: the value cannot be carried
    /// without guessing, and the authority refuses the write.
    fn cross_schema_settled_merge_cells(
        &mut self,
        schema_version: SchemaVersionId,
        table_schema: &TableSchema,
        authored: Option<&BTreeSet<String>>,
        previous: &VersionRow,
    ) -> Result<Result<Vec<(usize, Value)>, String>, Error> {
        let unauthored = table_schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| {
                table_schema.merge_strategy(&column.name) != crate::schema::MergeStrategy::Lww
                    && authored.is_some_and(|columns| !columns.contains(&column.name))
            })
            .collect::<Vec<_>>();
        let Some((_, first)) = unauthored.first() else {
            return Ok(Ok(Vec::new()));
        };
        let Some(previous_schema) = self.schema_version_for_alias(previous.schema_version_alias())
        else {
            return Ok(Err(first.name.clone()));
        };
        let previous_table = self.table_in_schema(previous.table(), previous_schema)?;
        let Some(path) =
            self.compiled_lens_path(previous_schema, schema_version, previous.table())?
        else {
            return Ok(Err(first.name.clone()));
        };
        if path.target_table != table_schema.name {
            return Ok(Err(first.name.clone()));
        }
        let previous_cells = previous.cells(&previous_table)?;
        let mut translated = previous_cells.clone();
        apply_compiled_lens_path(&path, &mut translated);
        let mut carried = Vec::with_capacity(unauthored.len());
        for (index, column) in unauthored {
            let name = column.name.as_str();
            let mut touched = false;
            let mut mapped_from_another_column = false;
            for op in &path.ops {
                match op {
                    CompiledLensOp::Rename { from, to } | CompiledLensOp::Copy { from, to } => {
                        if from == name || to == name {
                            touched = true;
                            mapped_from_another_column = true;
                        }
                    }
                    CompiledLensOp::Add { column, .. } | CompiledLensOp::Drop { column } => {
                        touched |= column == name;
                    }
                }
            }
            let same_column = previous_table.columns.iter().find(|candidate| candidate.name == name);
            let value = match same_column {
                Some(previous_column)
                    if !touched
                        && previous_column.column_type == column.column_type
                        && previous_table.merge_strategy(name)
                            == table_schema.merge_strategy(name) =>
                {
                    previous_cells.get(name)
                }
                None if !mapped_from_another_column => translated.get(name),
                _ => return Ok(Err(column.name.clone())),
            };
            // History cells are stored nullable.
            carried.push((
                HistoryRowRecord::USER_CELLS + index,
                Value::Nullable(value.cloned().map(Box::new)),
            ));
        }
        Ok(Ok(carried))
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
        // `global_time` is a nullable field: read it as one.
        Ok(raw
            .record()
            .get_nullable_u64(GlobalCurrentRowRecord::FIELD_GLOBAL_TIME_IDX)?
            .map(GlobalTime))
    }

    async fn query_global_winner_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<VersionRow>, Error> {
        Ok(self
            .query_global_winner_with_seq_in_batch(batch, schema_version, table, branch_key, row_uuid)
            .await?
            .map(|(winner, _)| winner))
    }

    /// The row's global post-image together with the seq it is the row at.
    async fn query_global_winner_with_seq_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<(VersionRow, Option<GlobalTime>)>, Error> {
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
        let variant_tag = raw.variant_tag();
        let current = raw.owned_record();
        let seq = current
            .borrowed()
            .get_nullable_u64(GlobalCurrentRowRecord::FIELD_GLOBAL_TIME_IDX)?
            .map(GlobalTime);
        if let Some(winner) =
            self.history_image_from_current_record(
                schema_version,
                table,
                variant_tag,
                current.borrowed(),
            )?
        {
            return Ok(Some((winner, seq)));
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
        .map(|winner| winner.map(|winner| (winner, seq)))
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
                .decode_stored_history_record(Some(batch), table, &storage_table, record)
                .await
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
            .table_in_schema_ref(version.table(), schema_version)?
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
        tx_author: AuthorSubject,
    ) -> Result<(), Error> {
        let (history_table, record) = self.version_storage_write_binding(version, tx_author)?;
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
        let physical = self.encode_physical_version_record(&plan, version, Some(global_time), None)?;
        batch.update_raw(
            plan.storage_table.clone(),
            global_current_primary_key(version.branch_key(), version.row_uuid()),
            physical,
        );
        // A node that has seen a column stamp must stamp its own later
        // writes at least as high, so an edit made after observing a value
        // is never older than that value. The row's identity is the write at
        // its seq, which need not carry the row's highest stamp.
        let observed = version.max_col_stamp()?;
        self.merge_tx_time(TxTime::from_physical_ms(observed).map_err(|_| {
            Error::InvalidStoredValue("column stamp exceeds the packed HLC range")
        })?);
        let overlay_key = self.ahead_overlay_key(version)?;
        if self.ahead_current_keys.contains_key(&overlay_key) {
            self.mark_ahead_shadow_dirty(schema_version, version, true);
        }
        Ok(())
    }

    /// `shadow_may_exist` is false only when the row just gained its overlay:
    /// a row without an overlay has no shadow, so there is nothing to delete
    /// if its synced image is also absent.
    fn mark_ahead_shadow_dirty(
        &mut self,
        schema_version: SchemaVersionId,
        version: &VersionRow,
        shadow_may_exist: bool,
    ) {
        self.ahead_shadow_dirty.push((
            schema_version,
            version.table().to_owned(),
            version.branch_key().clone(),
            version.row_uuid(),
            shadow_may_exist,
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
        dirty.dedup_by(|later, kept| {
            let same_row = later.1 == kept.1 && later.2 == kept.2 && later.3 == kept.3;
            if same_row {
                kept.4 |= later.4;
            }
            same_row
        });
        for (schema_version, table, branch_key, row_uuid, shadow_may_exist) in dirty {
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
                // No overlay before this batch means no shadow to remove.
                None if !shadow_may_exist => {}
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
        let physical = self.encode_physical_version_record(&plan, version, None, None)?;
        batch.update_raw(
            plan.storage_table.clone(),
            global_current_primary_key(version.branch_key(), version.row_uuid()),
            physical,
        );
        if self.ahead_current_keys.insert(overlay_key, tx_id).is_none() {
            self.mark_ahead_shadow_dirty(schema_version, version, false);
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
        self.mark_ahead_shadow_dirty(schema_version, version, true);
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
            let had_overlay = self.ahead_current_keys.remove(&key).is_some();
            self.write_ahead_current_insert(batch, &image)?;
            if had_overlay {
                // The replaced overlay may have left a shadow behind.
                let image_schema_version = self
                    .schema_version_for_alias(image.schema_version_alias())
                    .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
                self.mark_ahead_shadow_dirty(image_schema_version, &image, true);
            }
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

    pub(super) fn ahead_overlay_key(&self, version: &VersionRow) -> Result<(crate::ids::PhysicalTableId, Vec<u8>), Error> {
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
        let counter_signs = patch.counter_signs()?;
        if !authors(DELETION_COLUMN_NAME) {
            folded[HistoryRowRecord::FIELD__DELETION_IDX] =
                base_values[HistoryRowRecord::FIELD__DELETION_IDX].clone();
        }
        for (position, column) in table_schema.columns.iter().enumerate() {
            let index = HistoryRowRecord::USER_CELLS + position;
            let strategy = table_schema.merge_strategy(&column.name);
            folded[index] = match (authors(&column.name), strategy) {
                (false, _) => base_values[index].clone(),
                (true, crate::schema::MergeStrategy::Lww) => continue,
                (true, strategy) => Value::Nullable(Some(Box::new(
                    crate::node::merge_ops::apply_pending_merge_op(
                        strategy,
                        &column.column_type,
                        &base_values[index],
                        &folded[index],
                        crate::node::merge_ops::counter_sign(
                            table_schema,
                            &counter_signs,
                            position,
                        ),
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
        let descriptor = patch.record.descriptor();
        crate::node::merge_ops::clear_counter_signs(&mut folded, &descriptor);
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
