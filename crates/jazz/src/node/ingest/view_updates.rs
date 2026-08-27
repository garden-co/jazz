impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) async fn create_merge_versions_for(
        &mut self,
        records: &[VersionRecord],
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error> {
        let rows = self.merge_rows_for_versions(records)?;
        self.create_merge_versions_for_rows(rows).await
    }

    fn merge_rows_for_versions(
        &mut self,
        records: &[VersionRecord],
    ) -> Result<Vec<(String, BranchKey, RowUuid)>, Error> {
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
            rows.push((table, record.branch_key().clone(), record.row_uuid()));
        }
        rows.sort_unstable();
        rows.dedup();
        Ok(rows)
    }

    pub(super) async fn create_merge_versions_for_rows(
        &mut self,
        rows: Vec<(String, BranchKey, RowUuid)>,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error> {
        let mut outcome = PublicationOutcome::settled(Vec::new());
        for (table, branch_key, row_uuid) in rows {
            let created = self
                .create_merge_version_if_needed_in_branch(&table, &branch_key, row_uuid)
                .await?;
            outcome.append_outcome(created);
        }
        Ok(outcome)
    }

    #[cfg(test)]
    pub(super) async fn create_merge_version_if_needed(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error> {
        self.create_merge_version_if_needed_in_branch(table, &BranchKey::default(), row_uuid)
            .await
    }

    async fn create_merge_version_if_needed_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error> {
        let table_id =
            self.physical_table_id_for_schema(self.catalogue.current_write_schema.schema, table)?;
        let head_tx_ids = self
            .merge_head_tx_ids(table_id, branch_key, row_uuid)
            .await?;
        let table_schema =
            self.table_in_schema(table, self.catalogue.current_write_schema.schema)?;
        let has_gset_column = table_schema
            .columns
            .iter()
            .any(|column| table_schema.merge_strategy(&column.name) == MergeStrategy::GSet);
        if head_tx_ids.len() < 2 && !has_gset_column {
            return Ok(PublicationOutcome::settled(Vec::new()));
        }
        let row_versions = self.query_physical_content_row_versions(
            table_id,
            table,
            branch_key,
            row_uuid,
        )
        .await?;
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
            return Ok(PublicationOutcome::settled(Vec::new()));
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
        let cells = self
            .merge_cells_for_heads(&table_schema, &raw_heads, &row_versions_by_tx)
            .await?;
        if raw_heads.len() == 1
            && has_gset_column
            && !gset_cells_need_materialization(&table_schema, &raw_heads[0], &cells)?
        {
            return Ok(PublicationOutcome::settled(Vec::new()));
        }
        if cells.is_empty() {
            return Ok(PublicationOutcome::settled(Vec::new()));
        }
        let mut head_times = Vec::with_capacity(raw_heads.len());
        for version in &raw_heads {
            head_times.push(self.version_made_at(version).await?);
        }
        let made_at = head_times
            .into_iter()
            .max_by_key(|made_at| made_at.sort_key(self.node_uuid))
            .map(TxTime::tick_after)
            .transpose()?
            .ok_or(Error::InvalidStoredValue("merge requires heads"))?;
        self.merge_tx_time(made_at);
        let merge_tx_id = TxId::new(made_at, self.node_uuid);
        if self.query_transaction(merge_tx_id).await?.is_some() {
            return Ok(PublicationOutcome::settled(Vec::new()));
        }
        let schema = &self
            .catalogue
            .catalogue_schemas
            .get(&self.catalogue.current_write_schema.schema)
            .expect("current write schema exists")
            .schema;
        let branch = schema
            .branch_selector_for_key(&table_schema, branch_key)
            .map_err(Error::InvalidBranchKey)?;
        let merge_commit = MergeableCommit::new(table, row_uuid, made_at.physical_ms())
            .branch(branch)
            .parents(parents)
            .cells(cells);
        let publication = self.commit_mergeable_at(merge_commit, made_at).await?;
        let merge_tx = publication.tx_id;
        let unit = self.resident_commit_unit(Transaction {
            tx_id: merge_tx,
            kind: TxKind::Mergeable,
            n_total_writes: 1,
            made_by: AuthorSubject::SYSTEM,
            permission_subject: None,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: None,
            contribution_merge: None,
        })?;
        Ok(PublicationOutcome::published_then(
            Vec::new(),
            publication,
            unit,
        ))
    }

    async fn merge_cells_for_heads(
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
                        let made_at = self.version_made_at(version).await?;
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
                            let made_at = self.version_made_at(version).await?;
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

    async fn read_merge_heads(
        &mut self,
        table_id: PhysicalTableId,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeSet<TxId>>, Error> {
        let row = self.database.primary_key_get_raw(
            MERGE_HEADS_TABLE,
            &[
                Value::U64(table_id.0),
                Value::Bytes(branch_key.canonical_bytes()),
                Value::Uuid(row_uuid.0),
            ],
        )
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let heads = row.record().get_bytes(3)?;
        Self::decode_merge_heads(heads).map(Some)
    }

    async fn read_merge_heads_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        table_id: PhysicalTableId,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeSet<TxId>>, Error> {
        let row = self.database.primary_key_get_raw_in_batch(
            batch,
            MERGE_HEADS_TABLE,
            &[
                Value::U64(table_id.0),
                Value::Bytes(branch_key.canonical_bytes()),
                Value::Uuid(row_uuid.0),
            ],
        )
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let heads = row.record().get_bytes(3)?;
        Self::decode_merge_heads(heads).map(Some)
    }

    async fn require_merge_heads(
        &mut self,
        table_id: PhysicalTableId,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<BTreeSet<TxId>, Error> {
        self.read_merge_heads(table_id, branch_key, row_uuid).await?
            .ok_or(Error::InvalidStoredValue(
                "merge head set missing for existing global current row",
            ))
    }

    fn write_merge_heads(
        batch: &mut DatabaseBatch,
        table_id: PhysicalTableId,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        heads: &BTreeSet<TxId>,
    ) -> Result<(), Error> {
        batch.update(
            MERGE_HEADS_TABLE,
            vec![
                Value::U64(table_id.0),
                Value::Bytes(branch_key.canonical_bytes()),
                Value::Uuid(row_uuid.0),
                Value::Bytes(Self::encode_merge_heads(heads)?),
            ],
        );
        Ok(())
    }

    async fn query_physical_content_row_versions(
        &mut self,
        table_id: PhysicalTableId,
        requested_table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Vec<VersionRow>, Error> {
        let storage_table = physical_history_table_name(table_id);
        let raws = self
            .database
            .primary_key_scan_raw(
                &storage_table,
                &[
                    Value::Bytes(branch_key.canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                ],
            )
            .await?
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

    async fn recompute_merge_heads_from_persisted_history(
        &mut self,
        table_id: PhysicalTableId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<BTreeSet<TxId>, Error> {
        let versions =
            self.query_physical_content_row_versions(table_id, table, branch_key, row_uuid)
                .await?;
        let mut candidate_indices = Vec::new();
        for (idx, version) in versions.iter().enumerate() {
            let tx_id = self.version_tx_id(version)?;
            let Some(tx) = self.query_transaction(tx_id).await? else {
                continue;
            };
            if matches!(tx.fate, Fate::Pending | Fate::Accepted) {
                candidate_indices.push(idx);
            }
        }
        let head_indices = content_head_indices(&versions, &candidate_indices, &self.node_aliases);
        head_indices
            .into_iter()
            .map(|idx| self.version_tx_id(&versions[idx]))
            .collect()
    }

    pub(super) async fn update_merge_heads_for_content_version(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
        known_first_local_version: bool,
    ) -> Result<(), Error> {
        if version.layer() != VersionLayer::Content {
            return Ok(());
        }
        let table_id = self.physical_table_id_for_version(version)?;
        let new_tx = self.version_tx_id(version)?;
        // Authored commits may skip the derived-index bootstrap only when the
        // caller's physical-history lookup proved that this branch-local row
        // has no persisted content version and this is its first occurrence
        // in the still-uncommitted database batch.
        let mut heads = if known_first_local_version {
            BTreeSet::new()
        } else {
            match self
                .read_merge_heads(table_id, version.branch_key(), version.row_uuid())
                .await?
            {
                Some(existing) => existing,
                // A redacted exclusive view fragment intentionally persists
                // history without current indexes. If a later visible mergeable
                // version reaches this replica, bootstrap the derived head index
                // from every locally known eligible version before advancing it.
                None => {
                    self.recompute_merge_heads_from_persisted_history(
                        table_id,
                        version.table(),
                        version.branch_key(),
                        version.row_uuid(),
                    )
                    .await?
                }
            }
        };
        for parent in version.parents() {
            heads.remove(&parent);
        }
        let mut dominated_by_existing_head = false;
        for head in heads.iter().copied() {
            if self
                .content_version_reaches_tx_in_batch(
                    batch,
                    table_id,
                    version.table(),
                    version.branch_key(),
                    version.row_uuid(),
                    head,
                    new_tx,
                )
                .await?
            {
                dominated_by_existing_head = true;
                break;
            }
        }
        if !dominated_by_existing_head {
            heads.insert(new_tx);
        }
        Self::write_merge_heads(
            batch,
            table_id,
            version.branch_key(),
            version.row_uuid(),
            &heads,
        )
    }

    async fn update_merge_heads_for_content_version_in_batch(
        &mut self,
        batch: &mut DatabaseBatch,
        version: &VersionRow,
    ) -> Result<(), Error> {
        if version.layer() != VersionLayer::Content {
            return Ok(());
        }
        let table_id = self.physical_table_id_for_version(version)?;
        let new_tx = self.version_tx_id(version)?;
        let mut heads = match self.read_merge_heads_in_batch(
            batch,
            table_id,
            version.branch_key(),
            version.row_uuid(),
        ).await? {
            Some(existing) => existing,
            // See the non-batched path above: missing derived state is valid
            // after partial view-scoped history ingress.
            None => self.recompute_merge_heads_from_persisted_history(
                table_id,
                version.table(),
                version.branch_key(),
                version.row_uuid(),
            ).await?,
        };
        for parent in version.parents() {
            heads.remove(&parent);
        }
        let mut dominated_by_existing_head = false;
        for head in heads.iter().copied() {
            if self
                .content_version_reaches_tx(
                    table_id,
                    version.branch_key(),
                    version.row_uuid(),
                    head,
                    new_tx,
                )
                .await?
            {
                dominated_by_existing_head = true;
                break;
            }
        }
        if !dominated_by_existing_head {
            heads.insert(new_tx);
        }
        Self::write_merge_heads(
            batch,
            table_id,
            version.branch_key(),
            version.row_uuid(),
            &heads,
        )
    }

    async fn query_global_layer_winner_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        table: &str,
        branch_key: &BranchKey,
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
            &[
                Value::Bytes(branch_key.canonical_bytes()),
                Value::Uuid(row_uuid.0),
            ],
        )
        .await?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let record = raw.record();
        let tx_time = TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?);
        let tx_node_alias =
            NodeAlias(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
        self.query_version_by_alias_in_batch(
            batch,
            table,
            branch_key,
            row_uuid,
            layer,
            tx_time,
            tx_node_alias,
        )
        .await
    }

    async fn query_version_by_alias_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        layer: VersionLayer,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<VersionRow>, Error> {
        for storage_table in self.version_storage_sources_for_layer(table, layer)? {
            let key = if storage_table == SHARED_DELETION_HISTORY_TABLE {
                let mut key = self.deletion_storage_prefix_in_branch(
                    table,
                    branch_key,
                    Some(row_uuid),
                )?;
                key.extend([Value::U64(tx_time.0), Value::U64(tx_node_alias.0)]);
                key
            } else {
                vec![
                    Value::Bytes(branch_key.canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                    Value::U64(tx_time.0),
                    Value::U64(tx_node_alias.0),
                ]
            };
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

    pub(crate) async fn write_merge_heads_for_bulk_content_versions(
        &mut self,
        batch: &mut DatabaseBatch,
        versions: &[VersionRow],
    ) -> Result<(), Error> {
        let mut by_row = BTreeMap::<(PhysicalTableId, BranchKey, RowUuid), Vec<&VersionRow>>::new();
        for version in versions {
            if version.layer() == VersionLayer::Content {
                let table_id = self.physical_table_id_for_version(version)?;
                by_row
                    .entry((table_id, version.branch_key().clone(), version.row_uuid()))
                    .or_default()
                    .push(version);
            }
        }
        for ((table_id, branch_key, row_uuid), mut row_versions) in by_row {
            row_versions.sort_by_key(|version| {
                let tx_id = self
                    .version_tx_id(version)
                    .expect("bulk content version must have node alias");
                tx_id.time.sort_key(tx_id.node)
            });
            let mut heads = self
                .read_merge_heads(table_id, &branch_key, row_uuid)
                .await?
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
                let mut ancestors_of_new = Vec::new();
                for head in heads.iter().copied() {
                    let reaches = match content_version_reaches_tx_in_staged_parents(
                        new_tx,
                        head,
                        &staged_parents,
                    ) {
                        Some(reaches) => reaches,
                        None => {
                            self.content_version_reaches_tx(
                                table_id,
                                &branch_key,
                                row_uuid,
                                new_tx,
                                head,
                            )
                            .await?
                        }
                    };
                    ancestors_of_new.push((head, reaches));
                }
                for (head, is_ancestor) in ancestors_of_new {
                    if is_ancestor {
                        heads.remove(&head);
                    }
                }
                let mut dominated_by_existing_head = false;
                for head in heads.iter().copied() {
                    let reaches = match content_version_reaches_tx_in_staged_parents(
                        head,
                        new_tx,
                        &staged_parents,
                    ) {
                        Some(reaches) => reaches,
                        None => {
                            self.content_version_reaches_tx(
                                table_id,
                                &branch_key,
                                row_uuid,
                                head,
                                new_tx,
                            )
                            .await?
                        }
                    };
                    if reaches {
                        dominated_by_existing_head = true;
                        break;
                    }
                }
                if !dominated_by_existing_head {
                    heads.insert(new_tx);
                }
            }
            Self::write_merge_heads(batch, table_id, &branch_key, row_uuid, &heads)?;
        }
        Ok(())
    }

    pub(crate) async fn rebuild_merge_heads_after_history_commit(
        &mut self,
        rows: &BTreeSet<(PhysicalTableId, String, BranchKey, RowUuid)>,
    ) -> Result<(), Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut batch = self.database.open_batch();
        for (table_id, table, branch_key, row_uuid) in rows {
            let heads = self.recompute_merge_heads_from_persisted_history(
                *table_id,
                table,
                branch_key,
                *row_uuid,
            )
            .await?;
            Self::write_merge_heads(&mut batch, *table_id, branch_key, *row_uuid, &heads)?;
        }
        let applied = self.database.apply_batch(batch).await?;
        let persisted = applied.persist().await;
        self.database.finish_persistence(persisted)?;
        Ok(())
    }

    async fn content_version_reaches_tx(
        &mut self,
        table_id: PhysicalTableId,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        start: TxId,
        target: TxId,
    ) -> Result<bool, Error> {
        #[cfg(test)]
        MERGE_HEAD_REACHABILITY_WALKS.fetch_add(1, Ordering::Relaxed);
        let mut stack = vec![start];
        let mut seen = BTreeSet::new();
        while let Some(tx_id) = stack.pop() {
            if tx_id == target {
                return Ok(true);
            }
            if !seen.insert(tx_id) {
                continue;
            }
            for version in self.query_versions_for_tx(tx_id).await? {
                if self.physical_table_id_for_version(&version)? == table_id
                    && version.branch_key() == branch_key
                    && version.row_uuid() == row_uuid
                    && version.layer() == VersionLayer::Content
                {
                    stack.extend(version.parents());
                }
            }
        }
        Ok(false)
    }

    async fn content_version_reaches_tx_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        table_id: PhysicalTableId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        start: TxId,
        target: TxId,
    ) -> Result<bool, Error> {
        #[cfg(test)]
        MERGE_HEAD_REACHABILITY_WALKS.fetch_add(1, Ordering::Relaxed);
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
                .query_versions_for_tx_in_batch_for_row(
                    batch,
                    tx_id,
                    table_id,
                    table,
                    branch_key,
                    row_uuid,
                )
                .await?
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

    async fn query_versions_for_tx_in_batch_for_row(
        &mut self,
        batch: &DatabaseBatch,
        tx_id: TxId,
        table_id: PhysicalTableId,
        table: &str,
        branch_key: &BranchKey,
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
                Value::Bytes(branch_key.canonical_bytes()),
                Value::Uuid(row_uuid.0),
                Value::U64(tx_id.time.0),
                Value::U64(tx_node_alias.0),
            ],
        ).await? {
            versions.push(self.decode_history_owned_record(
                table,
                &storage_table,
                raw.owned_record(),
            )?);
        }
        Ok(versions)
    }

    async fn rewrite_merge_heads_excluding_tx(
        &mut self,
        batch: &mut DatabaseBatch,
        table_id: PhysicalTableId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        excluded_tx: TxId,
    ) -> Result<(), Error> {
        let versions = self.query_physical_content_row_versions(
            table_id,
            table,
            branch_key,
            row_uuid,
        )
        .await?;
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
        Self::write_merge_heads(batch, table_id, branch_key, row_uuid, &heads)
    }

    async fn merge_head_tx_ids(
        &mut self,
        table_id: PhysicalTableId,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<BTreeSet<TxId>, Error> {
        self.require_merge_heads(table_id, branch_key, row_uuid).await
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
    async fn recomputed_merge_heads_from_history_for_test(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<BTreeSet<TxId>, Error> {
        let table_id = self.physical_table_id_for_authored_test_table(table)?;
        let versions = self.query_physical_content_row_versions(
            table_id,
            table,
            branch_key,
            row_uuid,
        )
        .await?;
        let mut candidate_indices = Vec::new();
        for (idx, version) in versions.iter().enumerate() {
            if version.layer() != VersionLayer::Content {
                continue;
            }
            let tx_id = self.version_tx_id(version)?;
            let Some(tx) = self.query_transaction(tx_id).await? else {
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
    pub(super) async fn rebuild_merge_heads_from_history_for_test(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<(), Error> {
        let branch_key = BranchKey::default();
        let heads = self
            .recomputed_merge_heads_from_history_for_test(table, &branch_key, row_uuid)
            .await?;
        let table_id = self.physical_table_id_for_authored_test_table(table)?;
        let mut batch = self.database.open_batch();
        Self::write_merge_heads(
            &mut batch,
            table_id,
            &branch_key,
            row_uuid,
            &heads,
        )?;
        let applied = self.database.apply_batch(batch).await?;
        let persisted = applied.persist().await;
        self.database.finish_persistence(persisted)?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn assert_merge_heads_match_history_for_test(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<(), Error> {
        self.assert_merge_heads_match_history_in_branch_for_test(
            table,
            &BranchKey::default(),
            row_uuid,
        )
        .await
    }

    #[cfg(test)]
    async fn assert_merge_heads_match_history_in_branch_for_test(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<(), Error> {
        let expected = self
            .recomputed_merge_heads_from_history_for_test(table, branch_key, row_uuid)
            .await?;
        let table_id = self.physical_table_id_for_authored_test_table(table)?;
        let actual = self
            .require_merge_heads(table_id, branch_key, row_uuid)
            .await?;
        if actual != expected {
            let stored_versions = self
                .query_physical_content_row_versions(table_id, table, branch_key, row_uuid)
                .await?;
            let mut versions = Vec::with_capacity(stored_versions.len());
            for version in stored_versions {
                let tx_id = self.version_tx_id(&version)?;
                let fate = self
                    .query_transaction(tx_id)
                    .await?
                    .map(|tx| tx.fate)
                    .unwrap_or(Fate::Pending);
                versions.push(format!(
                    "{tx_id:?} layer={:?} parents={:?} fate={fate:?}",
                    version.layer(),
                    version.parents()
                ));
            }
            panic!(
                "stored merge heads diverged from history for {table}/{branch_key:?}/{row_uuid:?}: expected {expected:?}, actual {actual:?}, versions={versions:?}"
            );
        }
        Ok(())
    }

    #[cfg(test)]
    async fn assert_merge_head_rows_match_history_for_test(
        &mut self,
        rows: &BTreeSet<(String, BranchKey, RowUuid)>,
    ) -> Result<(), Error> {
        for (table, branch_key, row_uuid) in rows {
            self.assert_merge_heads_match_history_in_branch_for_test(
                table,
                branch_key,
                *row_uuid,
            )
            .await?;
        }
        Ok(())
    }

    #[cfg(test)]
    async fn recomputed_global_layer_winner_from_history_for_test(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        let mut winner = None::<(VersionRow, TxId, TxTime)>;
        for version in self
            .query_row_versions_in_branch(table, branch_key, row_uuid)
            .await?
            .into_iter()
            .filter(|version| version.layer() == layer)
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
            let Some(expected) = self.recomputed_global_layer_winner_from_history_for_test(
                version.table(),
                version.branch_key(),
                version.row_uuid(),
                version.layer(),
            )
            .await?
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
                self.public_current_values(&table, version, Some(global_time))?,
            ),
            VersionLayer::Deletion => (
                groove::Intern::new(self.physical_current_table_for_schema(
                    schema_version,
                    version.table(),
                    VersionLayer::Deletion,
                    PhysicalCurrentClass::Global,
                )?),
                &storage_tables[1],
                register_global_current_values(version, Some(global_time)),
            ),
        };
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
                Value::Bytes(version_layer_string(version.layer()).into_bytes()),
                Value::U64(global_time.0),
            ],
        )
        .await?;
        let Some(row) = rows.first() else {
            panic!(
                "missing global-change row for {}/{:?} {:?} at {:?}",
                version.table(),
                version.row_uuid(),
                version.layer(),
                global_time
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
                global_time,
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
        global_time: GlobalTime,
    ) -> Result<(), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
        match version.layer() {
            VersionLayer::Content => {
                let plan = self.prepared_physical_write_plan(
                    schema_version,
                    version.table(),
                    PhysicalWriteTarget::GlobalCurrent,
                )?;
                let mut values = self.public_current_values(
                    &plan.source_table,
                    version,
                    Some(global_time),
                )?;
                self.remap_authored_enum_cells_for_physical(
                    &mut values,
                    &plan.source_table,
                    &plan.source_mapping,
                    &plan.physical_table,
                    GlobalCurrentRowRecord::USER_CELLS,
                )?;
                let physical = OwnedRecord::new(
                    plan.physical_descriptor.create(&values)?,
                    plan.physical_descriptor,
                );
                batch.update_raw(
                    plan.storage_table.clone(),
                    global_current_primary_key(version.branch_key(), version.row_uuid()),
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
                global_current_primary_key(version.branch_key(), version.row_uuid()),
                version.bind_groove_record(
                    owned_record_from_storage_values(
                        &self
                            .table_in_schema(version.table(), schema_version)?
                            .global_current_storage_tables()[1],
                        register_global_current_values(version, Some(global_time)),
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
        if self.ahead_current_keys.contains(&(
            physical_table_id,
            version.layer(),
            encoded_primary_key.clone(),
        )) {
            return Ok(());
        }
        match version.layer() {
            VersionLayer::Content => {
                let plan = self.prepared_physical_write_plan(
                    schema_version,
                    version.table(),
                    PhysicalWriteTarget::AheadCurrent,
                )?;
                let mut values =
                    self.public_current_values(&plan.source_table, version, None)?;
                self.remap_authored_enum_cells_for_physical(
                    &mut values,
                    &plan.source_table,
                    &plan.source_mapping,
                    &plan.physical_table,
                    GlobalCurrentRowRecord::USER_CELLS,
                )?;
                let physical = OwnedRecord::new(
                    plan.physical_descriptor.create(&values)?,
                    plan.physical_descriptor,
                );
                batch.insert_raw(
                    plan.storage_table.clone(),
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
            physical_table_id,
            version.layer(),
            encoded_primary_key,
        );
        Ok(())
    }

    /// Build the physical current-source carrier consumed by Groove terminals.
    fn public_current_values(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
        global_time: Option<GlobalTime>,
    ) -> Result<Vec<Value>, Error> {
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
            version.layer(),
            PhysicalCurrentClass::Ahead,
        )?;
        batch.delete(table, history_primary_key(version));
        self.remove_ahead_current_key(
            self.physical_table_id_for_schema(schema_version, version.table())?,
            version.layer(),
            history_primary_key(version).into_bytes(),
        );
        Ok(())
    }

    /// Once a transaction is rejected or globally settled, it must not remain
    /// in the ahead-current overlay: accepted global effects live in current
    /// tables, and rejected effects are no longer visible. Edge-accepted
    /// no-global transactions intentionally stay ahead-visible at Edge tier.
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
