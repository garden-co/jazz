impl<S> NodeState<S>
where
    S: ResidentStorage,
{
    /// Commit a local mergeable write and leave its fate pending.
    pub fn commit_mergeable(&mut self, commit: MergeableCommit) -> Result<TxId, Error> {
        commit.validate()?;
        let made_at = self.preview_mergeable_tx_time(std::slice::from_ref(&commit), commit.now_ms);
        self.commit_mergeable_at(commit, made_at)
    }

    /// Commit one local mergeable write under an admitted authored schema.
    ///
    /// Client database handles retain the schema they were opened with even
    /// when an authority later advances its separate current-write pointer.
    /// Their canonical versions must retain that authored schema so receivers
    /// can reconstruct through the ordered catalogue lineage.
    pub(crate) fn commit_mergeable_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        commit: MergeableCommit,
    ) -> Result<TxId, Error> {
        self.commit_mergeable_many_in_schema(schema_version, vec![commit])
    }

    /// Commit multiple local mergeable writes as one transaction.
    pub fn commit_mergeable_many(&mut self, commits: Vec<MergeableCommit>) -> Result<TxId, Error> {
        if commits.is_empty() {
            return Err(Error::InvalidMergeableCommit(
                "mergeable transaction requires at least one write",
            ));
        }
        for commit in &commits {
            commit.validate()?;
            if commit.effective_permission_subject() != commits[0].effective_permission_subject() {
                return Err(Error::InvalidMergeableCommit(
                    "mergeable transaction permission subjects must match",
                ));
            }
        }
        let made_at = self.preview_mergeable_tx_time(&commits, commits[0].now_ms);
        self.commit_mergeable_many_at(commits, made_at)
    }

    /// Commit local mergeable writes under one admitted authored schema.
    pub(crate) fn commit_mergeable_many_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        commits: Vec<MergeableCommit>,
    ) -> Result<TxId, Error> {
        self.require_catalogue_ready()?;
        if !self
            .catalogue
            .catalogue_schemas
            .contains_key(&schema_version)
        {
            return Err(Error::InvalidMergeableCommit(
                "authored schema version is not admitted",
            ));
        }
        if commits.is_empty() {
            return Err(Error::InvalidMergeableCommit(
                "mergeable transaction requires at least one write",
            ));
        }
        for commit in &commits {
            commit.validate()?;
            if commit.effective_permission_subject() != commits[0].effective_permission_subject() {
                return Err(Error::InvalidMergeableCommit(
                    "mergeable transaction permission subjects must match",
                ));
            }
        }
        let made_at = self.preview_mergeable_tx_time(&commits, commits[0].now_ms);
        self.commit_mergeable_many_at_with_schema_versions(
            commits
                .into_iter()
                .map(|commit| (schema_version, commit))
                .collect(),
            made_at,
            None,
        )
    }

    pub(crate) fn preview_mergeable_tx_time(
        &self,
        commits: &[MergeableCommit],
        now_ms: u64,
    ) -> TxTime {
        let observed = commits
            .iter()
            .flat_map(|commit| commit.parents.iter().map(|parent| parent.time))
            .fold(self.clock.tx_time, TxTime::max);
        TxTime::tick(observed, now_ms)
    }

    pub(crate) fn prepare_current_mergeable_commit(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<PreparedMergeableCommit, Error> {
        commit.validate()?;
        let made_at = self.preview_mergeable_tx_time(std::slice::from_ref(&commit), commit.now_ms);
        let schema = self.catalogue.current_write_schema.schema;
        self.prepare_mergeable_many_at_with_schema_versions(
            vec![(schema, commit)],
            made_at,
            None,
        )
    }

    fn commit_mergeable_at(
        &mut self,
        commit: MergeableCommit,
        made_at: TxTime,
    ) -> Result<TxId, Error> {
        self.commit_mergeable_many_at(vec![commit], made_at)
    }

    fn commit_mergeable_many_at(
        &mut self,
        commits: Vec<MergeableCommit>,
        made_at: TxTime,
    ) -> Result<TxId, Error> {
        self.commit_mergeable_many_at_with_branch_merge(commits, made_at, None)
    }

    pub(super) fn commit_mergeable_many_at_with_branch_merge(
        &mut self,
        commits: Vec<MergeableCommit>,
        made_at: TxTime,
        branch_merge: Option<BranchMergeProvenance>,
    ) -> Result<TxId, Error> {
        self.require_catalogue_ready()?;
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let commits = commits
            .into_iter()
            .map(|commit| (write_schema_version, commit))
            .collect();
        self.commit_mergeable_many_at_with_schema_versions(commits, made_at, branch_merge)
    }

    pub(super) fn commit_mergeable_many_at_with_schema_versions(
        &mut self,
        commits: Vec<(SchemaVersionId, MergeableCommit)>,
        made_at: TxTime,
        branch_merge: Option<BranchMergeProvenance>,
    ) -> Result<TxId, Error> {
        let prepared = self.prepare_mergeable_many_at_with_schema_versions(
            commits,
            made_at,
            branch_merge,
        )?;
        self.publish_prepared_mergeable_commit(prepared)
    }

    fn prepare_mergeable_many_at_with_schema_versions(
        &mut self,
        commits: Vec<(SchemaVersionId, MergeableCommit)>,
        made_at: TxTime,
        branch_merge: Option<BranchMergeProvenance>,
    ) -> Result<PreparedMergeableCommit, Error> {
        let tx_id = TxId::new(made_at, self.node_uuid);
        let made_by = commits[0].1.made_by;
        let permission_subject = commits[0].1.effective_permission_subject();
        let user_metadata_json = commits[0].1.user_metadata_json.clone();
        let tx = Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: commits.len().try_into().map_err(|_| {
                Error::InvalidMergeableCommit("transaction write count exceeds u32")
            })?,
            made_by,
            permission_subject: commits[0].1.permission_subject,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json,
            target_lineage: crate::tx::BranchLineage::Root,
            branch_merge,
        };
        let tx_node_alias = self
            .node_aliases
            .get(&tx_id.node)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "local node alias must be installed before write preparation",
            ))?;
        let mut batch = self.database.open_batch();
        batch.insert(
            "jazz_transactions",
            transaction_values(
                tx_node_alias,
                &tx,
                Fate::Pending,
                None,
                self.authored_commit_durability,
            ),
        );
        let mut stored_versions = Vec::new();
        let mut pending_parents = BTreeSet::new();
        for (write_schema_version, commit) in commits {
            let schema_version_alias = self
                .catalogue
                .schema_version_aliases
                .get(&write_schema_version)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "authored schema alias must be installed before write preparation",
                ))?;
            let table_schema = self.table_in_schema(&commit.table, write_schema_version)?;
            let layer = VersionLayer::for_commit(&commit);
            let previous_current =
                match self.query_local_layer_winner(&table_schema.name, commit.row_uuid, layer)? {
                    Some(previous) => Some(previous),
                    None => {
                        self.query_global_layer_winner(&table_schema.name, commit.row_uuid, layer)?
                    }
                };
            let creator_source = if let Some(previous) = previous_current.as_ref() {
                Some(previous.clone())
            } else if layer == VersionLayer::Deletion {
                match self.query_local_layer_winner(
                    &table_schema.name,
                    commit.row_uuid,
                    VersionLayer::Content,
                )? {
                    Some(previous) => Some(previous),
                    None => self.query_global_layer_winner(
                        &table_schema.name,
                        commit.row_uuid,
                        VersionLayer::Content,
                    )?,
                }
            } else {
                None
            };
            let (created_by, created_at) = creator_source
                .as_ref()
                .map(|version| (version.created_by(), version.created_at()))
                .unwrap_or((commit.made_by, TxTime(commit.now_ms)));

            let parents = if commit.parents.is_empty() {
                Vec::new()
            } else {
                commit.parents
            };
            let cells = commit.cells;
            let authored_columns = Some(
                commit
                    .authored_columns
                    .clone()
                    .unwrap_or_else(|| cells.keys().cloned().collect()),
            );
            let stored = VersionRow::from_parts_with_schema_version(
                &table_schema,
                VersionRowParts {
                    table: commit.table,
                    row_uuid: commit.row_uuid,
                    tx_node_alias,
                    schema_version_alias,
                    tx_time: made_at,
                    parents,
                    created_by,
                    created_at,
                    updated_by: commit.made_by,
                    updated_at: TxTime(commit.now_ms),
                    cells,
                    authored_columns,
                    deletion: commit.deletion,
                },
                (write_schema_version != self.catalogue.current_schema_version_id)
                    .then_some(write_schema_version),
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
                version_wins_over_open_winner(&stored, tx_id, made_at, previous_winner);
            let _ = (new_is_current, previous_current);
            let (history_table, groove_record) = self.version_storage_write_binding(&stored)?;
            batch.insert_raw(
                history_table.as_ref(),
                self.version_storage_primary_key(&stored, BranchLineage::Root)?,
                groove_record,
            );
            self.update_merge_heads_for_content_version(&mut batch, &stored)?;
            self.write_ahead_current_insert(&mut batch, &stored)?;
            pending_parents.extend(stored.parents());
            stored_versions.push(stored);
        }
        for parent in pending_parents {
            if let Some(parent_alias) = self.node_aliases.get(&parent.node).copied() {
                batch.insert(
                    "jazz_pending_edges",
                    pending_edge_values(tx_node_alias, tx_id, parent_alias, parent),
                );
            }
        }
        self.stage_recovery_checkpoint(&mut batch, made_at);
        let batch = self.database.prepare_batch_storage_inputs(&batch)?;
        Ok(PreparedMergeableCommit {
            tx_id,
            batch,
            stored_versions,
            made_by,
            permission_subject,
        })
    }

    pub(crate) fn publish_prepared_mergeable_commit(
        &mut self,
        prepared: PreparedMergeableCommit,
    ) -> Result<TxId, Error> {
        let PreparedMergeableCommit {
            tx_id,
            batch,
            stored_versions,
            made_by,
            permission_subject,
        } = prepared;
        self.clock.tx_time = self.clock.tx_time.max(tx_id.time);
        self.publish_prepared_database_batch(batch)?;
        self.cache_tx_versions(tx_id, stored_versions.clone());
        if permission_subject != made_by {
            self.open_tx
                .local_permission_subjects
                .insert(tx_id, permission_subject);
        }
        for stored in &stored_versions {
            self.record_child_edges(tx_id, stored.parents());
        }
        Ok(tx_id)
    }

    /// Commit a local mergeable write and return its sync commit unit.
    pub fn commit_mergeable_unit(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<(TxId, SyncMessage), Error> {
        let tx_id = self.commit_mergeable(commit)?;
        Ok((tx_id, self.commit_unit_for(tx_id)?))
    }

    /// Rebuild the sync commit unit for an already-committed local transaction
    /// from its stored versions.
    ///
    /// Used by the `Db` sync surface to upload a client's local writes upstream
    /// on a connection. Unlike [`NodeState::commit_mergeable_unit`] this reads the
    /// stored versions, so the shipped
    /// unit matches what the author actually stored.
    pub fn commit_unit_for(&mut self, tx_id: TxId) -> Result<SyncMessage, Error> {
        let tx = self
            .query_transaction(tx_id)?
            .ok_or(Error::MissingTransaction(tx_id))?
            .tx
            .clone();
        let versions = self
            .query_versions_for_tx(tx_id)?
            .into_iter()
            .map(|row| self.version_record_from_row(&row))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(SyncMessage::CommitUnit { tx, versions })
    }

    /// Open an exclusive transaction over the current snapshot.
    pub fn visible_current_cells(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        Ok(self
            .current_rows(table, DurabilityTier::Local)?
            .into_iter()
            .find(|row| row.row_uuid() == row_uuid)
            .map(|row| {
                let table_schema = self.table(table).expect("table exists");
                table_schema
                    .columns
                    .iter()
                    .filter_map(|column| {
                        row.cell(table_schema, &column.name)
                            .map(|value| (column.name.clone(), value))
                    })
                    .collect()
            }))
    }

    /// Return current rows at the requested durability tier.
    pub fn current_rows(
        &mut self,
        table: &str,
        settled: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.require_catalogue_ready()?;
        let shape = crate::query::Query::from(table).validate(&self.catalogue.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.query_rows(&shape, &binding, settled)
    }

    fn local_layer_winner_tx_id(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<TxId>, Error> {
        self.query_local_layer_winner(table, row_uuid, layer)?
            .as_ref()
            .map(|version| self.version_tx_id(version))
            .transpose()
    }

    pub(crate) fn local_content_winner_tx_id(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        self.local_layer_winner_tx_id(table, row_uuid, VersionLayer::Content)
    }

    pub(crate) fn local_deletion_winner_tx_id(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        self.local_layer_winner_tx_id(table, row_uuid, VersionLayer::Deletion)
    }

    pub(super) fn cached_tx_version_tables(&self, tx_id: TxId) -> Option<BTreeSet<String>> {
        self.query.tx_version_tables_cache.get(&tx_id).cloned()
    }

    pub(super) fn cached_tx_versions(&self, tx_id: TxId) -> Option<Vec<VersionRow>> {
        self.query.tx_versions_cache.get(&tx_id).cloned()
    }

    pub(super) fn cache_tx_version_tables(&mut self, tx_id: TxId, tables: BTreeSet<String>) {
        self.touch_tx_version_cache_entry(tx_id);
        self.query.tx_version_tables_cache.insert(tx_id, tables);
        self.bound_tx_version_cache();
    }

    pub(super) fn cache_tx_versions(&mut self, tx_id: TxId, versions: Vec<VersionRow>) {
        self.touch_tx_version_cache_entry(tx_id);
        self.query.tx_versions_cache.insert(tx_id, versions);
        self.bound_tx_version_cache();
    }

    fn touch_tx_version_cache_entry(&mut self, tx_id: TxId) {
        if self.query.tx_version_tables_cache_order_set.insert(tx_id) {
            self.query.tx_version_tables_cache_order.push_back(tx_id);
        }
    }

    fn bound_tx_version_cache(&mut self) {
        while self.query.tx_version_tables_cache.len() > TX_VERSION_TABLE_CACHE_MAX_ENTRIES
            || self.query.tx_versions_cache.len() > TX_VERSION_TABLE_CACHE_MAX_ENTRIES
        {
            let Some(oldest) = self.query.tx_version_tables_cache_order.pop_front() else {
                break;
            };
            if !self.query.tx_version_tables_cache_order_set.remove(&oldest) {
                continue;
            }
            self.query.tx_version_tables_cache.remove(&oldest);
            self.query.tx_versions_cache.remove(&oldest);
        }
    }

    pub(super) fn invalidate_tx_version_tables_cache(&mut self, tx_id: TxId) {
        self.query.tx_version_tables_cache.remove(&tx_id);
        self.query.tx_versions_cache.remove(&tx_id);
        self.query.tx_version_tables_cache_order_set.remove(&tx_id);
    }

    pub(super) fn invalidate_tx_version_table_names_cache(&mut self, tx_id: TxId) {
        self.query.tx_version_tables_cache.remove(&tx_id);
    }

    fn materialize_current_row(
        &mut self,
        _table: &TableSchema,
        row: CurrentRow,
    ) -> Result<CurrentRow, Error> {
        Ok(row)
    }

    fn current_row_from_materialized_version(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
    ) -> Result<CurrentRow, Error> {
        current_row_from_version_projection(table, version)
    }

    fn materialized_cells_for_version(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
    ) -> Result<BTreeMap<String, Value>, Error> {
        version.cells(table)
    }

    pub(crate) fn local_current_row(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        self.local_current_row_in_schema(
            table,
            row_uuid,
            self.catalogue.current_write_schema.schema,
        )
    }

    pub(crate) fn local_current_row_in_schema(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        schema_version: SchemaVersionId,
    ) -> Result<Option<CurrentRow>, Error> {
        let table_schema = self.table_in_schema(table, schema_version)?;
        let content =
            self.local_current_content_row_candidate(&table_schema, row_uuid, schema_version)?;
        let deletion =
            self.local_current_deletion_candidate(&table_schema, row_uuid, schema_version)?;
        if let (Some((_, content_tx)), Some((deletion, deletion_tx))) = (&content, &deletion)
            && deletion_tx > content_tx
            && *deletion == DeletionEvent::Deleted
        {
            return Ok(None);
        }
        content
            .map(|(row, _)| self.materialize_current_row(&table_schema, row))
            .transpose()
    }

    fn local_current_content_row_candidate(
        &mut self,
        table: &TableSchema,
        row_uuid: RowUuid,
        schema_version: SchemaVersionId,
    ) -> Result<Option<(CurrentRow, (TxTime, NodeUuid))>, Error> {
        let prefix = vec![groove::ivm::LiteralValue::from(Value::Uuid(row_uuid.0))];
        let global = self.physical_current_source_scan_graph(
            schema_version,
            &table.name,
            PhysicalCurrentClass::Global,
            groove::ivm::StaticScanSpec::Point(prefix.clone()),
        )?;
        let ahead = self.physical_current_source_scan_graph(
            schema_version,
            &table.name,
            PhysicalCurrentClass::Ahead,
            groove::ivm::StaticScanSpec::Prefix(prefix),
        )?;
        let result = self
            .database
            .query_graph(GraphBuilder::arg_max_by(
                GraphBuilder::union([global, ahead]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            ))
            .map_err(|error| Self::malformed_current_query_error(&table.name, row_uuid, error))?;
        let Some(delta) = result.deltas.into_iter().find(|delta| delta.weight > 0) else {
            return Ok(None);
        };
        let record = BorrowedRecord::new(&delta.record, &result.descriptor);
        let tx = self.current_record_sort_key(&table.name, row_uuid, record)?;
        Ok(Some((decode_current_row(table, record)?, tx)))
    }

    fn local_current_deletion_candidate(
        &mut self,
        table: &TableSchema,
        row_uuid: RowUuid,
        schema_version: SchemaVersionId,
    ) -> Result<Option<(DeletionEvent, (TxTime, NodeUuid))>, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, &table.name)?;
        let prefix = vec![groove::ivm::LiteralValue::from(Value::Uuid(row_uuid.0))];
        let global = GraphBuilder::table_scan(
            physical_register_global_current_table_name(table_id),
            groove::ivm::StaticScanSpec::Point(prefix.clone()),
        );
        let ahead = GraphBuilder::table_scan(
            physical_register_ahead_current_table_name(table_id),
            groove::ivm::StaticScanSpec::Prefix(prefix),
        );
        let result = self
            .database
            .query_graph(GraphBuilder::arg_max_by(
                GraphBuilder::union([global, ahead]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            ))
            .map_err(|error| Self::malformed_current_query_error(&table.name, row_uuid, error))?;
        let Some(delta) = result.deltas.into_iter().find(|delta| delta.weight > 0) else {
            return Ok(None);
        };
        let record = BorrowedRecord::new(&delta.record, &result.descriptor);
        Ok(Some((
            deletion_event_from_value(
                record.get_idx(RegisterGlobalCurrentRowRecord::FIELD__DELETION_IDX)?,
            )?,
            self.current_record_sort_key(&table.name, row_uuid, record)?,
        )))
    }

    fn current_record_sort_key(
        &self,
        table: &str,
        row_uuid: RowUuid,
        record: BorrowedRecord<'_>,
    ) -> Result<(TxTime, NodeUuid), Error> {
        let malformed = |source| {
            Error::MalformedCurrentRow(Box::new(MalformedCurrentRow {
                table: table.to_owned(),
                row_uuid,
                source,
            }))
        };
        let tx_time = TxTime(
            record
                .get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)
                .map_err(malformed)?,
        );
        let tx_node_alias = NodeAlias(
            record
                .get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)
                .map_err(malformed)?,
        );
        let tx_node = self
            .node_aliases
            .iter()
            .find_map(|(node, alias)| (*alias == tx_node_alias).then_some(*node))
            .ok_or(Error::InvalidStoredValue(
                "current row references unknown node alias",
            ))?;
        Ok((tx_time, tx_node))
    }

    fn malformed_current_query_error(
        table: &str,
        row_uuid: RowUuid,
        error: GrooveDbError,
    ) -> Error {
        let source = match error {
            GrooveDbError::RecordEncoding(source)
            | GrooveDbError::IvmRuntime(groove::ivm::IvmRuntimeError::RecordEncoding(source)) => {
                source
            }
            error => return Error::Groove(error),
        };
        Error::MalformedCurrentRow(Box::new(MalformedCurrentRow {
            table: table.to_owned(),
            row_uuid,
            source,
        }))
    }

}
