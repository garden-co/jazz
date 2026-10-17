impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Apply an accepted write to the row's post-image as a patch, one
    /// physical column at a time (SPEC 4 §4.6, "Patch rule" and "Across
    /// schema versions").
    ///
    /// Every plain cell the write authored, and `_deletion` when authored,
    /// takes the write's value: the last write Core accepts wins. Merge
    /// columns apply their op to the row's value. Every other column of the
    /// table's lineage keeps the row's value, whatever schema version the
    /// write was authored under; a column the row's image does not hold yet
    /// takes what a reader of that column's schema saw through the lens. The
    /// post-image is laid out in the lineage's newest schema version, with
    /// the lineage's other columns as union cells (SPEC 10 §10.3, "Union row
    /// cases"). It keeps the write's identity, takes `updated_by`/`updated_at`
    /// from the write, and keeps `created_by`/`created_at`. The write's base
    /// takes no part (the authority validated it before minting the seq).
    /// Returns `None` when the post-image does not change.
    ///
    /// Only the node that minted `global_time` (`authority`) derives the
    /// row's post-image. Any other node merging an accepted write here is an
    /// originator settling its own write when its fate arrives, and Core's
    /// image at that seq is authoritative: the local merge is only a
    /// prediction of it, made by applying the write's patch over whatever
    /// image the node holds, without resolving its base. So a non-authority
    /// keeps what it holds, and takes Core's post-image when it arrives
    /// (SPEC 4 §4.6), whenever
    ///
    /// - it already holds Core's image at a later seq, which counts this
    ///   write already (Core applies writes in seq order; merging the write
    ///   again would apply its merge ops twice and move the row back to an
    ///   older seq), or
    /// - the prediction cannot be made over the image it holds: a counter
    ///   op leaves its type's range over a stale value. Core accepted the
    ///   write, so this is the local image's staleness, never a reason to
    ///   fail the fate.
    ///
    /// At the authority those are invariant violations (seqs are minted in
    /// order, and `merge_op_rejection` refuses such writes before a seq is
    /// minted) and remain errors.
    pub(super) async fn merged_global_post_image(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table_schema: &TableSchema,
        incoming: &VersionRow,
        incoming_tx: TxId,
        global_time: GlobalTime,
        authority: bool,
    ) -> Result<Option<VersionRow>, Error> {
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
            // The first image of a row. Over no image, each counter cell's
            // low bits are the written value itself (a delta from zero): the
            // image keeps the cells.
            let mut values = incoming.record.to_values()?;
            crate::node::merge_ops::clear_counter_signs(&mut values, &incoming_descriptor);
            return incoming
                .with_record_values(values)?
                .with_merge_fields(global_time, incoming.base()?)
                .map(Some);
        };
        let previous_tx = self.version_tx_id(&previous)?;
        if previous_tx == incoming_tx {
            return Ok(None);
        }
        if previous_seq.is_some_and(|previous_seq| previous_seq > global_time) {
            // Core's image at a later seq already counts this write. The
            // authority mints seqs in order and never gets here.
            debug_assert!(!authority, "the authority merged a write below the row's seq");
            return Ok(None);
        }
        match self
            .union_merge(schema_version, table_schema, incoming, &previous)
            .await?
        {
            Ok(merge) => self
                .union_post_image(incoming, &previous, merge, global_time)
                .map(Some),
            // A prediction over a stale local image.
            Err(_) if !authority => Ok(None),
            Err(UnionMergeRefusal::CounterOutOfRange { .. }) => Err(Error::InvalidStoredValue(
                "accepted counter op leaves the column's range",
            )),
            Err(UnionMergeRefusal::NotSupported(_)) => Err(Error::InvalidStoredValue(
                "accepted write cannot be merged into the row's image",
            )),
        }
    }

    /// The authority refuses a write it cannot merge into the row:
    ///
    /// - a counter op that would take the column outside its type's range
    ///   over the row's current value (the op is not wrapped), and
    /// - a cell the merge cannot derive without guessing
    ///   (`complete_union_cells`).
    ///
    /// It also refuses a write whose base it cannot name exactly
    /// (`INV-HIST-20`).
    pub(super) async fn merge_op_rejection(
        &mut self,
        tx_id: TxId,
        versions: &[VersionRecord],
    ) -> Result<Option<RejectionReason>, Error> {
        let batch = self.database.open_batch();
        // Writes of the unit's node that applying it passes over, recorded
        // once the unit passes every check here (`passed_over_predecessor`).
        let mut passed_over = Vec::new();
        for version in versions {
            let schema_version = version.schema_version();
            let (current_seq, current_writer) = self
                .global_current_seq_and_writer_in_batch(
                    &batch,
                    schema_version,
                    version.table(),
                    version.branch_key(),
                    version.row_uuid(),
                )
                .await?
                .unwrap_or((None, None));
            if let Some(reason) = self
                .late_write_refusal(
                    &batch,
                    schema_version,
                    version.table(),
                    version.branch_key(),
                    version.row_uuid(),
                    version.base(),
                    tx_id,
                    current_seq,
                    current_writer,
                )
                .await?
            {
                return Ok(Some(RejectionReason::MalformedCommit(reason)));
            }
            if !version.base().is_empty() {
                if let Err(reason) = self
                    .validate_write_base(
                        &batch,
                        schema_version,
                        version.table(),
                        version.branch_key(),
                        version.row_uuid(),
                        version.base(),
                        tx_id,
                        current_seq,
                    )
                    .await?
                {
                    return Ok(Some(RejectionReason::MalformedCommit(reason)));
                }
            }
            if let Some(through) = self
                .passed_over_predecessor(version.base(), tx_id, current_writer)
                .await?
            {
                passed_over.push((version, through));
            }
            // Without a merge column anywhere and with a single schema
            // version, every write merges.
            if !self.any_known_merge_column(version.table())
                && !self.lineage_has_several_schemas(schema_version, version.table())?
            {
                continue;
            }
            let Some(alias) = self
                .catalogue
                .schema_version_aliases
                .get(&schema_version)
                .copied()
            else {
                continue;
            };
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
            let table_schema = self.table_in_schema(version.table(), schema_version)?;
            let authored = self.authored_column_ids_for_names(
                schema_version,
                version.table(),
                version.authored_columns(),
            )?;
            // The write's cells as a row; its identity takes no part.
            let incoming = VersionRow::from_wire_with_schema_version(
                &table_schema,
                version,
                authored,
                NodeAlias(0),
                alias,
                tx_id.time,
                GlobalTime(0),
                None,
            )?;
            if let Some(reason) = self
                .union_merge_rejection(schema_version, &table_schema, &incoming, &current)
                .await?
            {
                return Ok(Some(reason));
            }
        }
        drop(batch);
        for (version, through) in passed_over {
            self.record_superseded_writes(
                self.physical_table_id_for_schema(version.schema_version(), version.table())?,
                version.branch_key(),
                version.row_uuid(),
                tx_id.node,
                through,
            )
            .await?;
        }
        Ok(None)
    }

    /// Whether more than one schema version maps `table`'s physical lineage.
    fn lineage_has_several_schemas(
        &self,
        schema_version: SchemaVersionId,
        table: &str,
    ) -> Result<bool, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, table)?;
        Ok(self
            .catalogue
            .physical_mappings
            .values()
            .filter(|mapping| {
                mapping
                    .tables
                    .values()
                    .any(|candidate| candidate.table_id == table_id)
            })
            .nth(1)
            .is_some())
    }

    /// Whether a row of `table` can have an image holding merge state under
    /// some schema version this node knows: some version gives a table of
    /// that name a merge column, or gives any table one while some lens
    /// renames a table (the image may then be stored under another name).
    fn any_known_merge_column(&self, table: &str) -> bool {
        let has_merge = |schema: &JazzSchema, name: Option<&str>| {
            schema
                .tables
                .iter()
                .filter(|candidate| name.is_none_or(|name| candidate.name == name))
                .any(|candidate| {
                    candidate.columns.iter().any(|column| {
                        candidate.merge_strategy(&column.name)
                            != crate::schema::MergeStrategy::Lww
                    })
                })
        };
        let any_schema = |name: Option<&str>| {
            has_merge(&self.catalogue.schema, name)
                || has_merge(&self.catalogue.active_schema.compiled, name)
                || self
                    .catalogue
                    .catalogue_schemas
                    .values()
                    .any(|schema| has_merge(&schema.schema, name))
        };
        let renames_a_table = || {
            self.catalogue.catalogue_lenses.values().any(|lens| {
                lens.table_lenses
                    .iter()
                    .any(|table_lens| table_lens.source_table != table_lens.target_table)
            })
        };
        any_schema(Some(table)) || (renames_a_table() && any_schema(None))
    }

    pub(super) async fn global_current_seq_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<GlobalTime>, Error> {
        Ok(self
            .global_current_seq_and_writer_in_batch(
                batch,
                schema_version,
                table,
                branch_key,
                row_uuid,
            )
            .await?
            .and_then(|(seq, _)| seq))
    }

    /// The row's current accepted seq and the transaction that wrote its
    /// current image, read from the one global-current record. `None` when
    /// the row has no global-current record.
    pub(super) async fn global_current_seq_and_writer_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<(Option<GlobalTime>, Option<TxId>)>, Error> {
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
        let record = raw.record();
        // `global_time` is a nullable field: read it as one.
        let seq = record
            .get_nullable_u64(GlobalCurrentRowRecord::FIELD_GLOBAL_TIME_IDX)?
            .map(GlobalTime);
        let tx_time = TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?);
        let tx_node_alias =
            NodeAlias(record.get_u32(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
        let writer = self
            .node_for_alias(tx_node_alias)
            .map(|node| TxId::new(tx_time, node));
        Ok(Some((seq, writer)))
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
            NodeAlias(record.get_u32(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
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
        let seqs = history_seq_candidates(
            self.history_tx_seq(Some(batch), tx_time, tx_node_alias).await?,
        );
        for storage_table in self.version_storage_sources(table)? {
            let _ = schema_version;
            let record = self
                .stored_image_record(
                    Some(batch),
                    &storage_table,
                    &branch_key.canonical_bytes(),
                    row_uuid,
                    &seqs,
                    tx_time,
                    tx_node_alias,
                )
                .await?;
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
            // History holds only superseded images: the current image has no
            // history copy (SPEC 2 §2.7.1).
            let schema_version = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
            let history_table = physical_history_table_name(
                self.physical_table_id_for_schema(schema_version, version.table())?,
            );
            let key = history_key_values(
                &version.branch_key().canonical_bytes(),
                version.row_uuid(),
                *global_time,
                version.tx_time(),
                version.tx_node_alias(),
            );
            let duplicated = self
                .database
                .primary_key_get_raw(&history_table, &key)
                .await?
                .is_some();
            let current_converts = self
                .current_image_records(
                    &history_table,
                    &[
                        Value::Bytes(version.branch_key().canonical_bytes()),
                        Value::Uuid(version.row_uuid().0),
                    ],
                )
                .await?
                .len()
                == 1;
            if duplicated && current_converts {
                panic!(
                    "history duplicates the current image of {}/{:?} at seq {:?}",
                    version.table(),
                    version.row_uuid(),
                    global_time
                );
            }
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
        // A union row's current record carries its extras after the
        // schema's own fields, as its write plan lays them out.
        let descriptor = if version.union_extra_cells()?.is_empty() {
            current_schema.record_schema()
        } else {
            self.prepared_physical_write_plan_for_row(version, PhysicalWriteTarget::GlobalCurrent)?
                .logical_descriptor
                .clone()
        };
        let expected = owned_record_from_storage_values_with_descriptor(descriptor, expected_values)?
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

    /// Store an accepted write's record in history at its seq, moving its
    /// pending record (if any) there. Used for an accepted write whose image
    /// is not the row's current image: history holds only superseded images
    /// (SPEC 2 §2.7.1). When the row's current image is this write's at the
    /// same seq, current already holds it, and only the pending record goes.
    pub(super) async fn write_superseded_history_record(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<(), Error> {
        let (history_table, record) = self.version_storage_write_binding(version)?;
        let seq = version.seq()?;
        if seq != GlobalTime(0) {
            batch.delete(
                history_table.as_ref(),
                self.version_storage_primary_key(&version.with_seq(GlobalTime(0))?)?,
            );
            let schema_version = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
            let current = self
                .global_current_seq_and_writer_in_batch(
                    batch,
                    schema_version,
                    version.table(),
                    version.branch_key(),
                    version.row_uuid(),
                )
                .await?;
            if current == Some((Some(seq), Some(self.version_tx_id(version)?))) {
                return Ok(());
            }
        }
        batch.update_raw(
            history_table.as_ref(),
            self.version_storage_primary_key(version)?,
            record,
        );
        Ok(())
    }

    /// Move the row's global-current image into history at its own seq when
    /// `version` at `global_time` supersedes it: the image is at a lower seq
    /// and was written by another transaction. The history record is built
    /// from the current record's bytes, so the image and its base move
    /// verbatim. A current layout that cannot be converted kept its history
    /// copy when it was written, so nothing moves.
    async fn supersede_global_current_in_batch(
        &mut self,
        batch: &mut DatabaseBatch,
        schema_version: SchemaVersionId,
        history_table: &str,
        version: &VersionRow,
        global_time: GlobalTime,
    ) -> Result<(), Error> {
        let current_table = self.physical_current_table_for_schema(
            schema_version,
            version.table(),
            PhysicalCurrentClass::Global,
        )?;
        let Some((variant_tag, current)) = self
            .database
            .primary_key_get_raw_in_batch(
                batch,
                &current_table,
                &[
                    Value::Bytes(version.branch_key().canonical_bytes()),
                    Value::Uuid(version.row_uuid().0),
                ],
            )
            .await?
            .map(|raw| (raw.variant_tag(), raw.owned_record()))
        else {
            return Ok(());
        };
        let record = current.borrowed();
        let Some(previous_seq) =
            record.get_nullable_u64(GlobalCurrentRowRecord::FIELD_GLOBAL_TIME_IDX)?
        else {
            return Ok(());
        };
        let previous_writer = (
            record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?,
            record.get_u32(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?,
        );
        if previous_seq >= global_time.0
            || previous_writer == (version.tx_time().0, version.tx_node_alias().0)
        {
            return Ok(());
        }
        let Some(history_record) =
            self.history_record_from_current(history_table, variant_tag, record)?
        else {
            return Ok(());
        };
        let (branch_key, row_uuid, seq, tx_time, tx_node_alias) =
            history_record_identity(&history_record)?;
        batch.update_raw(
            history_table.to_owned(),
            PrimaryKeyValue::Composite(vec![
                PrimaryKeyValue::Bytes(branch_key),
                PrimaryKeyValue::Uuid(row_uuid.0),
                PrimaryKeyValue::U64(seq),
                PrimaryKeyValue::U64(tx_time),
                PrimaryKeyValue::U32(tx_node_alias),
            ]),
            groove::records::VariantRecord::new(variant_tag, history_record),
        );
        Ok(())
    }

    /// Make `version` the row's global-current image at `global_time`.
    ///
    /// History holds only superseded images; the latest is in current alone
    /// (SPEC 2 §2.7.1). So the row's previous current image, when this write
    /// supersedes it (it is at a lower seq), first moves into history at its
    /// own seq, whole and with its base. Then current is overwritten, and any
    /// history copy of the new image (the record an ingest path staged at
    /// its seq, or its pending record at seq 0) is dropped from this batch.
    pub(super) async fn write_global_current_update(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
        global_time: GlobalTime,
    ) -> Result<(), Error> {
        self.write_global_current_update_over(batch, version, global_time, true)
            .await
    }

    /// [`Self::write_global_current_update`]; `may_have_current` is false
    /// only when the caller proved the current table holds no rows, so there
    /// is no image to supersede and the cold load path reads nothing.
    pub(super) async fn write_global_current_update_over(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
        global_time: GlobalTime,
        may_have_current: bool,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        let table_id = self.physical_table_id_for_schema(schema_version, version.table())?;
        let history_table = physical_history_table_name(table_id);
        // The image may live only in current: list the row for its
        // transaction like any history write.
        self.mark_tx_touched_row(table_id, version);
        if may_have_current {
            self.supersede_global_current_in_batch(
                batch,
                schema_version,
                &history_table,
                version,
                global_time,
            )
            .await?;
        }
        let plan =
            self.prepared_physical_write_plan_for_row(version, PhysicalWriteTarget::GlobalCurrent)?;
        // Validate node-local authored column aliases before deriving
        // the current carrier; encoding itself retains trusted bytes.
        let _ = self.authored_columns_for_version(version)?;
        let at_seq = version.with_seq(global_time)?;
        let physical = self.encode_physical_version_record(&plan, &at_seq, Some(global_time))?;
        // Current keeps the only copy of the latest image when it can give
        // the history record back; a layout it cannot convert keeps its
        // history copy as well.
        let converts = self
            .history_record_from_current(
                &history_table,
                physical.variant_tag(),
                physical.record().borrowed(),
            )?
            .is_some();
        batch.update_raw(
            plan.storage_table.clone(),
            global_current_primary_key(version.branch_key(), version.row_uuid()),
            physical,
        );
        if converts {
            batch.delete(
                history_table.clone(),
                self.version_storage_primary_key(&at_seq)?,
            );
            batch.delete(
                history_table,
                self.version_storage_primary_key(&at_seq.with_seq(GlobalTime(0))?)?,
            );
        } else {
            // Not convertible: history keeps the image at its seq.
            let (history_table, record) = self.version_storage_write_binding(&at_seq)?;
            batch.delete(
                history_table.as_ref(),
                self.version_storage_primary_key(&at_seq.with_seq(GlobalTime(0))?)?,
            );
            batch.update_raw(
                history_table.as_ref(),
                self.version_storage_primary_key(&at_seq)?,
                record,
            );
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
        let plan =
            self.prepared_physical_write_plan_for_row(version, PhysicalWriteTarget::AheadCurrent)?;
        let _ = self.authored_columns_for_version(version)?;
        let physical = self.encode_physical_version_record(&plan, version, None)?;
        batch.update_raw(
            plan.storage_table.clone(),
            global_current_primary_key(version.branch_key(), version.row_uuid()),
            physical,
        );
        self.ahead_current_keys.insert(overlay_key, tx_id);
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
                // An image of another layout: the patch merges into it per
                // physical column, as Core will merge it (SPEC 4 §4.6,
                // "Across schema versions"). A patch that cannot be merged
                // over this image shows whole until Core's image arrives.
                Some(base) => match self
                    .union_merge(schema_version, &table_schema, &patch, &base)
                    .await?
                {
                    Ok(merge) => self.union_pending_image(&patch, &base, merge)?,
                    Err(_) => patch,
                },
                None => patch,
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
                    record.get_u32(TransactionRowRecord::FIELD_NODE_ID_IDX)?,
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
