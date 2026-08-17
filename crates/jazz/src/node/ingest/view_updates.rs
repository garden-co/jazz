impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
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
    fn physical_table_id_for_authored_test_table(
        &self,
        table: &str,
    ) -> Result<PhysicalTableId, Error> {
        let candidates = self
            .catalogue
            .physical_mappings
            .values()
            .filter_map(|mapping| mapping.tables.get(table).map(|table| table.table_id))
            .collect::<BTreeSet<_>>();
        match candidates.iter().copied().collect::<Vec<_>>().as_slice() {
            [table_id] => Ok(*table_id),
            [] => Err(Error::TableNotFound(table.to_owned())),
            _ => Err(Error::InvalidStoredValue(
                "authored test table name maps to multiple physical lineages",
            )),
        }
    }

    #[cfg(test)]
    fn recomputed_merge_heads_from_history_for_test(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<BTreeSet<TxId>, Error> {
        let table_id = self.physical_table_id_for_authored_test_table(table)?;
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
        let table_id = self.physical_table_id_for_authored_test_table(table)?;
        let mut batch = self.database.open_batch();
        Self::write_merge_heads(&mut batch, table_id, row_uuid, &heads)?;
        self.commit_database_batch(batch)?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn assert_merge_heads_match_history_for_test(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<(), Error> {
        let expected = self.recomputed_merge_heads_from_history_for_test(table, row_uuid)?;
        let table_id = self.physical_table_id_for_authored_test_table(table)?;
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
        self.commit_database_batch(batch)?;
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

}
