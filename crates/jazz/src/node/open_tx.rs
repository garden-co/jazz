//! Open transaction lifecycle and snapshot-overlay reads. This module owns
//! `tx_read`, `tx_query`, mergeable/exclusive write staging, and commit-unit
//! construction for open transactions; authority-side validation and fate
//! assignment live in [`super::ingest`], while query execution helpers live in
//! [`super::query_eval`]. It is the node API layer used by the `Db` facade before
//! writes become protocol commit units.

use super::*;

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Open an exclusive transaction over the current snapshot.
    pub fn open_exclusive(&mut self, id: OpenBatchId) -> Result<(), Error> {
        self.open_transaction(id, OpenTransactionKind::Exclusive)
    }

    /// Open a mergeable transaction over the current snapshot.
    pub(crate) fn open_mergeable(
        &mut self,
        id: OpenBatchId,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
    ) -> Result<(), Error> {
        self.open_transaction(
            id,
            OpenTransactionKind::Mergeable {
                made_by,
                permission_subject,
            },
        )
    }

    fn open_transaction(
        &mut self,
        id: OpenBatchId,
        kind: OpenTransactionKind,
    ) -> Result<(), Error> {
        if self.open_tx.open_transactions.contains_key(&id)
            || self.open_tx.closed_batches.contains(&id)
        {
            return Err(Error::DuplicateOpenBatch(id));
        }
        let local_base = self.clock.tx_time;
        let mut dots = Vec::new();
        for tx_id in self.transaction_ids()? {
            let Some(stored) = self.query_transaction(tx_id)? else {
                continue;
            };
            if !matches!(stored.fate, Fate::Rejected(_))
                && stored
                    .global_seq
                    .is_some_and(|global_seq| global_seq > self.clock.applied_global_watermark)
            {
                dots.push(tx_id);
            }
        }
        let base_snapshot = Snapshot::exclusive_base(
            self.node_uuid,
            self.clock.applied_global_watermark,
            local_base,
            dots,
        )
        .map_err(Error::InvalidStoredValue)?;
        self.open_tx.open_transactions.insert(
            id,
            OpenTransaction {
                kind,
                base_snapshot,
                base_snapshot_rows: BTreeMap::new(),
                row_reads: Vec::new(),
                absent_reads: Vec::new(),
                predicate_reads: Vec::new(),
                writes: Vec::new(),
                user_metadata_json: None,
            },
        );
        Ok(())
    }

    /// Read a row inside an exclusive transaction.
    pub fn tx_read(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        self.table(table)?;
        self.tx_read_unchecked(
            tx_id,
            self.catalogue.current_write_schema.schema,
            table,
            row_uuid,
        )
    }

    /// Read a row through an explicit registered schema view.
    pub fn tx_read_in_schema(
        &mut self,
        tx_id: OpenBatchId,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        self.table_in_schema(table, schema_version)?;
        self.tx_read_unchecked(tx_id, schema_version, table, row_uuid)
    }

    fn tx_read_unchecked(
        &mut self,
        tx_id: OpenBatchId,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let snapshot = self.open_tx(tx_id)?.base_snapshot.clone();
        let snapshot_row =
            self.snapshot_row_in_schema(schema_version, table, row_uuid, &snapshot)?;
        self.open_tx_mut(tx_id)?.base_snapshot_rows.insert(
            (schema_version, table.to_owned(), row_uuid),
            snapshot_row.clone(),
        );
        let result = self.overlay_pending_writes_in_schema(
            tx_id,
            schema_version,
            table,
            row_uuid,
            snapshot_row.clone(),
        )?;
        if let Some(version) = snapshot_row.read_version {
            let open_tx = self.open_tx_mut(tx_id)?;
            if !open_tx.row_reads.iter().any(|read| {
                read.table == table && read.row_uuid == row_uuid && read.version == version
            }) {
                open_tx.row_reads.push(RowRead {
                    table: table.to_owned(),
                    row_uuid,
                    version,
                });
            }
        } else {
            let open_tx = self.open_tx_mut(tx_id)?;
            if !open_tx
                .absent_reads
                .iter()
                .any(|read| read.table == table && read.row_uuid == row_uuid)
            {
                open_tx.absent_reads.push(AbsentRead {
                    table: table.to_owned(),
                    row_uuid,
                });
            }
        }
        Ok(result)
    }

    /// Read all current rows inside an exclusive transaction.
    pub fn tx_current_rows(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
    ) -> Result<Vec<CurrentRow>, Error> {
        let schema_version = self.catalogue.current_write_schema.schema;
        let table_schema = self.table(table)?.clone();
        self.tx_current_rows_with_table(tx_id, schema_version, table, table_schema)
    }

    /// Read current rows through an explicit registered schema view.
    pub fn tx_current_rows_in_schema(
        &mut self,
        tx_id: OpenBatchId,
        schema_version: SchemaVersionId,
        table: &str,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table_schema = self.table_in_schema(table, schema_version)?;
        self.tx_current_rows_with_table(tx_id, schema_version, table, table_schema)
    }

    fn tx_current_rows_with_table(
        &mut self,
        tx_id: OpenBatchId,
        schema_version: SchemaVersionId,
        table: &str,
        table_schema: TableSchema,
    ) -> Result<Vec<CurrentRow>, Error> {
        let snapshot = self.open_tx(tx_id)?.base_snapshot.clone();
        let rows = self
            .query_table_versions(table)?
            .iter()
            .filter(|version| version.table() == table)
            .map(|version| version.row_uuid())
            .chain(
                self.open_tx(tx_id)?
                    .writes
                    .iter()
                    .filter(|write| write.table == table)
                    .map(|write| write.row_uuid),
            )
            .collect::<BTreeSet<_>>();
        let mut current = Vec::new();
        for row_uuid in rows {
            let snapshot_row =
                self.snapshot_row_in_schema(schema_version, table, row_uuid, &snapshot)?;
            if let Some(cells) = self.overlay_pending_writes_with_table(
                tx_id,
                &table_schema,
                table,
                row_uuid,
                snapshot_row,
            )? {
                let cells = positional_cells_from_map(&table_schema, &cells)?;
                current.push(current_row_from_positional_cells(
                    &table_schema,
                    row_uuid,
                    &cells,
                )?);
            }
        }
        sort_current_rows(&mut current);
        let schema = self
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue("transaction schema is unknown"))?;
        let shape = crate::query::Query::from(table).validate(&schema.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.open_tx_mut(tx_id)?
            .predicate_reads
            .push(PredicateRead {
                table: table.to_owned(),
                shape_id: shape.shape_id(),
                shape: shape.query().clone(),
                binding_id: binding.binding_id(),
                binding_values: binding.values().clone(),
            });
        Ok(current)
    }

    /// Stage a row write inside an exclusive transaction.
    pub fn tx_write<V: Into<Value>>(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row_uuid: RowUuid,
        cells: BTreeMap<String, V>,
        deletion: Option<DeletionEvent>,
    ) -> Result<(), Error> {
        self.tx_write_in_schema(
            tx_id,
            self.catalogue.current_write_schema.schema,
            table,
            row_uuid,
            cells,
            deletion,
        )
    }

    /// Stage a row write through an explicit registered schema view.
    pub fn tx_write_in_schema<V: Into<Value>>(
        &mut self,
        tx_id: OpenBatchId,
        write_schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        cells: BTreeMap<String, V>,
        deletion: Option<DeletionEvent>,
    ) -> Result<(), Error> {
        if !matches!(self.open_tx(tx_id)?.kind, OpenTransactionKind::Exclusive) {
            return Err(Error::InvalidMergeableCommit(
                "open transaction is not exclusive",
            ));
        }
        let table_schema = self.table_in_schema(table, write_schema_version)?;
        let cells = cells
            .into_iter()
            .map(|(column, value)| (column, value.into()))
            .collect::<BTreeMap<_, _>>();
        validate_mergeable_write_shape(cells.is_empty(), deletion.is_some())?;
        let cache_key = (write_schema_version, table.to_owned(), row_uuid);
        let snapshot_row = if let Some(snapshot_row) = self
            .open_tx(tx_id)?
            .base_snapshot_rows
            .get(&cache_key)
            .cloned()
        {
            snapshot_row
        } else {
            let snapshot = self.open_tx(tx_id)?.base_snapshot.clone();
            self.snapshot_row_in_schema(write_schema_version, table, row_uuid, &snapshot)?
        };
        let parent = if snapshot_row.deleted {
            None
        } else {
            snapshot_row.content_version
        };
        positional_cells_from_map(&table_schema, &cells)?;
        let pending = PendingWrite {
            table: table.to_owned(),
            row_uuid,
            schema_version: write_schema_version,
            cells: PendingCells::Replace(cells),
            deletion,
            parents: parent.into_iter().collect(),
            now_ms: None,
            refresh_parents_at_commit: false,
        };
        let open_tx = self.open_tx_mut(tx_id)?;
        open_tx
            .base_snapshot_rows
            .retain(|(_, cached_table, cached_row), _| {
                cached_table != table || *cached_row != row_uuid
            });
        if let Some(existing) = open_tx.writes.iter_mut().find(|write| {
            write.table == pending.table
                && write.row_uuid == pending.row_uuid
                && write.deletion.is_some() == pending.deletion.is_some()
        }) {
            *existing = pending;
        } else {
            open_tx.writes.push(pending);
        }
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn tx_write_mergeable(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row_uuid: RowUuid,
        cells: BTreeMap<String, Value>,
        deletion: Option<DeletionEvent>,
        parents: Vec<TxId>,
        now_ms: Option<u64>,
        refresh_parents_at_commit: bool,
    ) -> Result<(), Error> {
        self.tx_write_mergeable_in_schema(
            tx_id,
            self.catalogue.current_write_schema.schema,
            table,
            row_uuid,
            cells,
            deletion,
            parents,
            now_ms,
            refresh_parents_at_commit,
        )
    }

    pub(crate) fn tx_write_mergeable_in_schema(
        &mut self,
        tx_id: OpenBatchId,
        write_schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        cells: BTreeMap<String, Value>,
        deletion: Option<DeletionEvent>,
        parents: Vec<TxId>,
        now_ms: Option<u64>,
        refresh_parents_at_commit: bool,
    ) -> Result<(), Error> {
        if !matches!(
            self.open_tx(tx_id)?.kind,
            OpenTransactionKind::Mergeable { .. }
        ) {
            return Err(Error::InvalidMergeableCommit(
                "open transaction is not mergeable",
            ));
        }
        validate_mergeable_write_shape(cells.is_empty(), deletion.is_some())?;
        let table_schema = self.table_in_schema(table, write_schema_version)?;
        positional_cells_from_map(&table_schema, &cells)?;
        self.stage_mergeable_write(
            tx_id,
            PendingWrite {
                table: table.to_owned(),
                row_uuid,
                schema_version: write_schema_version,
                cells: PendingCells::Replace(cells),
                deletion,
                parents,
                now_ms,
                refresh_parents_at_commit,
            },
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn tx_patch_mergeable(
        &mut self,
        tx_id: OpenBatchId,
        table: &str,
        row_uuid: RowUuid,
        patch: BTreeMap<String, Value>,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.tx_patch_mergeable_in_schema(
            tx_id,
            self.catalogue.current_write_schema.schema,
            table,
            row_uuid,
            patch,
            now_ms,
        )
    }

    pub(crate) fn tx_patch_mergeable_in_schema(
        &mut self,
        tx_id: OpenBatchId,
        write_schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        patch: BTreeMap<String, Value>,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        if !matches!(
            self.open_tx(tx_id)?.kind,
            OpenTransactionKind::Mergeable { .. }
        ) {
            return Err(Error::InvalidMergeableCommit(
                "open transaction is not mergeable",
            ));
        }
        let mut staged_cells = self
            .tx_read_in_schema(tx_id, write_schema_version, table, row_uuid)?
            .unwrap_or_default();
        staged_cells.extend(patch.clone());
        validate_mergeable_write_shape(staged_cells.is_empty(), false)?;
        let table_schema = self.table_in_schema(table, write_schema_version)?;
        positional_cells_from_map(&table_schema, &patch)?;
        self.stage_mergeable_write(
            tx_id,
            PendingWrite {
                table: table.to_owned(),
                row_uuid,
                schema_version: write_schema_version,
                cells: PendingCells::Patch(patch),
                deletion: None,
                parents: Vec::new(),
                now_ms,
                refresh_parents_at_commit: false,
            },
        )
    }

    fn stage_mergeable_write(
        &mut self,
        tx_id: OpenBatchId,
        pending: PendingWrite,
    ) -> Result<(), Error> {
        let open_tx = self.open_tx_mut(tx_id)?;
        open_tx
            .base_snapshot_rows
            .retain(|(_, table, row), _| table != &pending.table || *row != pending.row_uuid);
        if pending.deletion.is_none() {
            if let Some(existing) = open_tx.writes.iter_mut().find(|write| {
                write.table == pending.table
                    && write.row_uuid == pending.row_uuid
                    && write.deletion.is_none()
            }) {
                let cells = match (&existing.cells, &pending.cells) {
                    (PendingCells::Replace(existing), PendingCells::Patch(patch)) => {
                        let mut cells = existing.clone();
                        cells.extend(patch.clone());
                        PendingCells::Replace(cells)
                    }
                    (PendingCells::Patch(existing), PendingCells::Patch(patch)) => {
                        let mut cells = existing.clone();
                        cells.extend(patch.clone());
                        PendingCells::Patch(cells)
                    }
                    (_, PendingCells::Replace(cells)) => PendingCells::Replace(cells.clone()),
                };
                *existing = PendingWrite { cells, ..pending };
            } else {
                open_tx.writes.push(pending);
            }
            return Ok(());
        }

        open_tx.writes.retain(|existing| {
            existing.table != pending.table
                || existing.row_uuid != pending.row_uuid
                || match pending.deletion {
                    Some(DeletionEvent::Deleted) => false,
                    Some(DeletionEvent::Restored) => existing.deletion.is_none(),
                    None => true,
                }
        });
        open_tx.writes.push(pending);
        Ok(())
    }

    /// Attach application metadata to an open transaction.
    pub fn tx_set_metadata(&mut self, tx_id: OpenBatchId, json: String) -> Result<(), Error> {
        self.open_tx_mut(tx_id)?.user_metadata_json = Some(json);
        Ok(())
    }

    /// Commit an exclusive transaction and return its sync commit unit.
    pub fn commit_exclusive(
        &mut self,
        open_batch_id: OpenBatchId,
        made_by: AuthorId,
        now_ms: u64,
    ) -> Result<(TxId, SyncMessage), Error> {
        if !matches!(
            self.open_tx(open_batch_id)?.kind,
            OpenTransactionKind::Exclusive
        ) {
            return Err(Error::InvalidMergeableCommit(
                "open transaction is not exclusive",
            ));
        }
        if !self.open_exclusive_is_locally_serializable(open_batch_id)? {
            return Err(Error::TransactionConflict);
        }
        let open_tx = self
            .open_tx
            .open_transactions
            .get(&open_batch_id)
            .cloned()
            .ok_or(Error::MissingOpenBatch(open_batch_id))?;
        for parent in open_tx.writes.iter().flat_map(|write| write.parents.iter()) {
            self.merge_tx_time(parent.time);
        }
        let made_at = self.mint_tx_time(now_ms);
        let tx_id = TxId::new(made_at, self.node_uuid);
        let versions = open_tx
            .writes
            .into_iter()
            .map(|write| {
                let table_schema = self.table_in_schema(&write.table, write.schema_version)?;
                let PendingCells::Replace(cells) = write.cells else {
                    return Err(Error::InvalidMergeableCommit(
                        "exclusive transaction cannot contain update patches",
                    ));
                };
                let cells = positional_cells_from_map(&table_schema, &cells)?;
                Ok(VersionRecord::encode(
                    &table_schema,
                    write.schema_version,
                    write.row_uuid,
                    write.parents,
                    made_by,
                    made_at,
                    made_by,
                    made_at,
                    &cells,
                    write.deletion,
                )?)
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let tx = Transaction {
            tx_id,
            kind: TxKind::Exclusive,
            n_total_writes: versions.len().try_into().map_err(|_| {
                Error::InvalidMergeableCommit("transaction write count exceeds u32")
            })?,
            made_by,
            permission_subject: None,
            base_snapshot: Some(open_tx.base_snapshot),
            row_read_set: Some(open_tx.row_reads),
            absent_read_set: Some(open_tx.absent_reads),
            predicate_read_set: Some(open_tx.predicate_reads),
            user_metadata_json: open_tx.user_metadata_json,
            target_lineage: crate::tx::BranchLineage::Root,
            branch_merge: None,
            merge_strategy: None,
        };
        self.ingest_transaction_and_versions(
            tx.clone(),
            versions.clone(),
            Fate::Pending,
            None,
            DurabilityTier::Local,
        )?;
        self.open_tx.open_transactions.remove(&open_batch_id);
        self.open_tx.closed_batches.insert(open_batch_id);
        Ok((tx_id, SyncMessage::CommitUnit { tx, versions }))
    }

    fn open_exclusive_is_locally_serializable(
        &mut self,
        open_batch_id: OpenBatchId,
    ) -> Result<bool, Error> {
        let open_tx = self.open_tx(open_batch_id)?.clone();
        for read in &open_tx.row_reads {
            for version in self.query_row_versions(&read.table, read.row_uuid)? {
                let tx_id = self.version_tx_id(&version)?;
                let visible = self
                    .query_transaction(tx_id)?
                    .is_some_and(|stored| !matches!(stored.fate, Fate::Rejected(_)));
                if visible && !self.snapshot_covers(tx_id, &open_tx.base_snapshot) {
                    return Ok(false);
                }
            }
        }
        for absent in &open_tx.absent_reads {
            for version in self.query_row_versions(&absent.table, absent.row_uuid)? {
                let tx_id = self.version_tx_id(&version)?;
                let visible = self
                    .query_transaction(tx_id)?
                    .is_some_and(|stored| !matches!(stored.fate, Fate::Rejected(_)));
                if visible && !self.snapshot_covers(tx_id, &open_tx.base_snapshot) {
                    return Ok(false);
                }
            }
        }
        for write in &open_tx.writes {
            for version in self.query_row_versions(&write.table, write.row_uuid)? {
                let tx_id = self.version_tx_id(&version)?;
                let visible = self
                    .query_transaction(tx_id)?
                    .is_some_and(|stored| !matches!(stored.fate, Fate::Rejected(_)));
                if visible && !self.snapshot_covers(tx_id, &open_tx.base_snapshot) {
                    return Ok(false);
                }
            }
        }
        for predicate in &open_tx.predicate_reads {
            for version in self.query_table_versions(&predicate.table)? {
                let tx_id = self.version_tx_id(&version)?;
                let visible = self
                    .query_transaction(tx_id)?
                    .is_some_and(|stored| !matches!(stored.fate, Fate::Rejected(_)));
                if visible && !self.snapshot_covers(tx_id, &open_tx.base_snapshot) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Commit a mergeable open transaction through the ordinary mergeable batch path.
    pub(crate) fn commit_mergeable_open(
        &mut self,
        open_batch_id: OpenBatchId,
        mut next_now_ms: impl FnMut() -> u64,
    ) -> Result<TxId, Error> {
        if !matches!(
            self.open_tx(open_batch_id)?.kind,
            OpenTransactionKind::Mergeable { .. }
        ) {
            return Err(Error::InvalidMergeableCommit(
                "open transaction is not mergeable",
            ));
        }
        let open_tx = self
            .open_tx
            .open_transactions
            .get(&open_batch_id)
            .cloned()
            .ok_or(Error::MissingOpenBatch(open_batch_id))?;
        let OpenTransactionKind::Mergeable {
            made_by,
            permission_subject,
        } = open_tx.kind
        else {
            return Err(Error::InvalidMergeableCommit(
                "open transaction is not mergeable",
            ));
        };
        let mut commits = Vec::with_capacity(open_tx.writes.len());
        for (index, write) in open_tx.writes.into_iter().enumerate() {
            for parent in &write.parents {
                self.merge_tx_time(parent.time);
            }
            let parents = if write.refresh_parents_at_commit {
                if write.deletion.is_none() {
                    self.local_content_winner_tx_id(&write.table, write.row_uuid)?
                } else {
                    self.local_deletion_winner_tx_id(&write.table, write.row_uuid)?
                }
                .into_iter()
                .collect()
            } else {
                write.parents
            };
            let (cells, authored_columns) = match write.cells {
                PendingCells::Replace(cells) => (cells, None),
                PendingCells::Patch(patch) => {
                    let table_schema = self.table_in_schema(&write.table, write.schema_version)?;
                    let mut cells = BTreeMap::new();
                    if let Some(existing) = self
                        .current_rows_for_schema(
                            &write.table,
                            write.schema_version,
                            DurabilityTier::Local,
                        )?
                        .into_iter()
                        .find(|row| row.row_uuid() == write.row_uuid)
                    {
                        for column in &table_schema.columns {
                            if let Some(value) = existing.cell(&table_schema, &column.name) {
                                cells.insert(column.name.clone(), value);
                            }
                        }
                    }
                    let authored_columns = patch.keys().cloned().collect();
                    cells.extend(patch);
                    (cells, Some(authored_columns))
                }
            };
            let mut commit = MergeableCommit::new(
                &write.table,
                write.row_uuid,
                write.now_ms.unwrap_or_else(&mut next_now_ms),
            )
            .made_by(made_by)
            .parents(parents)
            .cells(cells);
            if let Some(authored_columns) = authored_columns {
                commit = commit.authored_columns(authored_columns);
            }
            if let Some(subject) = permission_subject {
                commit = commit.permission_subject(subject);
            }
            if let Some(deletion) = write.deletion {
                commit = commit.deletion(deletion);
            }
            if index == 0
                && let Some(metadata) = open_tx.user_metadata_json.as_ref()
            {
                commit = commit.user_metadata(metadata.clone());
            }
            commits.push((write.schema_version, commit));
        }
        let first = commits.first().ok_or(Error::InvalidMergeableCommit(
            "mergeable transaction requires at least one write",
        ))?;
        let made_at = self.mint_tx_time(first.1.now_ms);
        let committed =
            self.commit_mergeable_many_at_with_schema_versions(commits, made_at, None)?;
        self.open_tx.open_transactions.remove(&open_batch_id);
        self.open_tx.closed_batches.insert(open_batch_id);
        Ok(committed)
    }

    /// Abandon an open transaction.
    pub fn abandon_tx(&mut self, tx_id: OpenBatchId) -> Result<(), Error> {
        self.open_tx
            .open_transactions
            .remove(&tx_id)
            .ok_or(Error::MissingOpenBatch(tx_id))?;
        self.open_tx.closed_batches.insert(tx_id);
        Ok(())
    }

    /// Return whether local transaction time advanced after this transaction opened.
    pub fn open_exclusive_snapshot_moved(&self, tx_id: OpenBatchId) -> Result<bool, Error> {
        Ok(self.clock.tx_time > self.open_tx(tx_id)?.base_snapshot.local_base)
    }

    pub(super) fn open_tx(&self, tx_id: OpenBatchId) -> Result<&OpenTransaction, Error> {
        self.open_tx
            .open_transactions
            .get(&tx_id)
            .ok_or(Error::MissingOpenBatch(tx_id))
    }

    pub(super) fn open_tx_mut(
        &mut self,
        tx_id: OpenBatchId,
    ) -> Result<&mut OpenTransaction, Error> {
        self.open_tx
            .open_transactions
            .get_mut(&tx_id)
            .ok_or(Error::MissingOpenBatch(tx_id))
    }

    pub(super) fn record_applied_global_seq(&mut self, global_seq: GlobalSeq) -> Vec<GlobalSeq> {
        self.clock.next_global_seq = self.clock.next_global_seq.max(global_seq.next());
        if global_seq <= self.clock.applied_global_watermark {
            return Vec::new();
        }
        self.clock.applied_global_above_watermark.insert(global_seq);
        let mut advanced = Vec::new();
        while let Some(next) = self
            .clock
            .applied_global_watermark
            .0
            .checked_add(1)
            .map(GlobalSeq)
            && self.clock.applied_global_above_watermark.remove(&next)
        {
            self.clock.applied_global_watermark = next;
            advanced.push(next);
        }
        advanced
    }

    pub(super) fn snapshot_covers(&mut self, tx_id: TxId, snapshot: &Snapshot) -> bool {
        self.query_transaction(tx_id)
            .ok()
            .flatten()
            .is_some_and(|stored| {
                stored
                    .global_seq
                    .is_some_and(|global_seq| global_seq <= snapshot.global_base)
                    || (tx_id.node == snapshot.owner && tx_id.time <= snapshot.local_base)
                    || snapshot.dots.contains(&tx_id)
            })
    }

    pub(super) fn snapshot_row_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        snapshot: &Snapshot,
    ) -> Result<SnapshotRow, Error> {
        let content = self.snapshot_layer_winner(table, row_uuid, VersionLayer::Content, snapshot);
        let deletion =
            self.snapshot_layer_winner(table, row_uuid, VersionLayer::Deletion, snapshot);
        let deleted = matches!(
            deletion.as_ref().and_then(|version| version.deletion()),
            Some(DeletionEvent::Deleted)
        );
        let target_table = self.table_in_schema(table, schema_version)?;
        let content_cells = if let Some(version) = content.as_ref() {
            let source_schema = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history schema version alias must exist",
                ))?;
            let source_table = self.table_in_schema(version.table(), source_schema)?;
            let mut cells = self.materialized_cells_for_version(&source_table, version)?;
            let projected_table =
                self.translate_cells(source_schema, schema_version, version.table(), &mut cells)?;
            if projected_table.as_deref() == Some(table) {
                Some(
                    target_table
                        .columns
                        .iter()
                        .map(|column| cells.get(&column.name).cloned())
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            }
        } else {
            None
        };
        Ok(SnapshotRow {
            content_cells,
            content_version: content
                .as_ref()
                .and_then(|version| self.version_tx_id(version).ok()),
            read_version: if deleted {
                deletion
                    .as_ref()
                    .and_then(|version| self.version_tx_id(version).ok())
            } else {
                content
                    .as_ref()
                    .and_then(|version| self.version_tx_id(version).ok())
            },
            deleted,
        })
    }

    pub(super) fn snapshot_layer_winner(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        snapshot: &Snapshot,
    ) -> Option<VersionRow> {
        // Snapshot reads must be stable for the whole transaction lifetime.
        // Intervals can REOPEN when a late arrival shifts the DAG winner, so
        // they cannot serve snapshot reads; domination over the fixed member
        // set depends only on immutable payload and is stable by construction.
        let versions = self.query_row_versions(table, row_uuid).ok()?;
        let mut candidate_indices = Vec::new();
        for (idx, version) in versions.iter().enumerate() {
            let tx_id = self.version_tx_id(version).ok()?;
            if version.layer() == layer && self.snapshot_covers(tx_id, snapshot) {
                candidate_indices.push(idx);
            }
        }
        current_version_index(&versions, &candidate_indices, layer, &self.node_aliases)
            .map(|idx| versions[idx].clone())
    }

    fn overlay_pending_writes_in_schema(
        &self,
        tx_id: OpenBatchId,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        snapshot_row: SnapshotRow,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let table_schema = self.table_in_schema(table, schema_version)?;
        self.overlay_pending_writes_with_table(tx_id, &table_schema, table, row_uuid, snapshot_row)
    }

    fn overlay_pending_writes_with_table(
        &self,
        tx_id: OpenBatchId,
        table_schema: &TableSchema,
        table: &str,
        row_uuid: RowUuid,
        snapshot_row: SnapshotRow,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let mut cells = snapshot_row
            .content_cells
            .map(|cells| cells_from_positional(table_schema, &cells));
        let mut deleted = snapshot_row.deleted;
        for write in self
            .open_tx(tx_id)?
            .writes
            .iter()
            .filter(|write| write.table == table && write.row_uuid == row_uuid)
        {
            match &write.cells {
                PendingCells::Replace(replacement) if !replacement.is_empty() => {
                    cells = Some(replacement.clone());
                }
                PendingCells::Patch(patch) => {
                    cells.get_or_insert_default().extend(patch.clone());
                }
                PendingCells::Replace(_) => {}
            }
            match write.deletion {
                Some(DeletionEvent::Deleted) => deleted = true,
                Some(DeletionEvent::Restored) => deleted = false,
                None => {}
            }
        }
        Ok(if deleted { None } else { cells })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OpenTransactionKind {
    Exclusive,
    Mergeable {
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
    },
}

#[derive(Clone)]
pub(super) struct OpenTransaction {
    /// Commit semantics and attribution carried by this open transaction.
    pub(super) kind: OpenTransactionKind,
    /// Snapshot captured when the transaction opened.
    pub(super) base_snapshot: Snapshot,
    /// Base snapshot row derivations observed by point reads in this transaction.
    pub(super) base_snapshot_rows: BTreeMap<(SchemaVersionId, String, RowUuid), SnapshotRow>,
    /// Point reads recorded by the transaction.
    pub(super) row_reads: Vec<RowRead>,
    /// Absent-row reads recorded by the transaction.
    pub(super) absent_reads: Vec<AbsentRead>,
    /// Predicate reads recorded by the transaction.
    pub(super) predicate_reads: Vec<PredicateRead>,
    /// Pending writes staged by the transaction.
    pub(super) writes: Vec<PendingWrite>,
    /// Optional application metadata.
    pub(super) user_metadata_json: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
enum PendingCells {
    Replace(BTreeMap<String, Value>),
    Patch(BTreeMap<String, Value>),
}

#[derive(Clone, Debug, PartialEq)]
/// Pending row write inside an open transaction.
pub(super) struct PendingWrite {
    /// Target table.
    pub(super) table: String,
    /// Target row.
    pub(super) row_uuid: RowUuid,
    /// Schema version used to encode staged cells.
    pub(super) schema_version: SchemaVersionId,
    /// Replacement cells or an update patch resolved when the transaction commits.
    cells: PendingCells,
    /// Deletion-register event, if any.
    pub(super) deletion: Option<DeletionEvent>,
    /// Parent vector carried by the staged write.
    pub(super) parents: Vec<TxId>,
    /// Per-write provenance time, or `None` for a commit-time clock value.
    pub(super) now_ms: Option<u64>,
    /// Whether restore parents must follow the current layer winner at commit time.
    pub(super) refresh_parents_at_commit: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct SnapshotRow {
    content_cells: Option<Vec<Option<Value>>>,
    content_version: Option<TxId>,
    read_version: Option<TxId>,
    deleted: bool,
}
