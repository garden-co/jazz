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
    pub fn open_exclusive(&mut self) -> Result<OpenTxId, Error> {
        self.open_transaction(OpenTransactionKind::Exclusive)
    }

    /// Open a mergeable transaction over the current snapshot.
    pub(crate) fn open_mergeable(
        &mut self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
    ) -> Result<OpenTxId, Error> {
        self.open_transaction(OpenTransactionKind::Mergeable {
            made_by,
            permission_subject,
        })
    }

    fn open_transaction(&mut self, kind: OpenTransactionKind) -> Result<OpenTxId, Error> {
        let id = OpenTxId(self.open_tx.next_open_tx_id);
        self.open_tx.next_open_tx_id = self
            .open_tx
            .next_open_tx_id
            .checked_add(1)
            .ok_or(Error::InvalidStoredValue("open tx id overflow"))?;
        let local_base = self.clock.tx_time;
        let base_snapshot = Snapshot::exclusive_base(
            self.node_uuid,
            self.clock.applied_global_watermark,
            local_base,
            Vec::new(),
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
        Ok(id)
    }

    /// Read a row inside an exclusive transaction.
    pub fn tx_read(
        &mut self,
        tx_id: OpenTxId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let Some(mut cells) = self.tx_read_raw(tx_id, table, row_uuid)? else {
            return Ok(None);
        };
        let table_schema = self.table(table)?.clone();
        for column in table_schema
            .columns
            .iter()
            .filter(|column| column.large_value.is_some())
        {
            match self.tx_large_value_cell(tx_id, table, row_uuid, &column.name)? {
                Some(OpenTxLargeValueCell::Authored(bytes))
                | Some(OpenTxLargeValueCell::SnapshotHandle(bytes)) => {
                    cells.insert(column.name.clone(), Value::Bytes(bytes));
                }
                None => {
                    cells.remove(&column.name);
                }
            }
        }
        Ok(Some(cells))
    }

    /// Read a row overlay without resolving large-value cells.
    pub(crate) fn tx_read_raw(
        &mut self,
        tx_id: OpenTxId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        self.table(table)?;
        let snapshot = self.open_tx(tx_id)?.base_snapshot.clone();
        let snapshot_row = self.snapshot_row(table, row_uuid, &snapshot);
        self.open_tx_mut(tx_id)?
            .base_snapshot_rows
            .insert((table.to_owned(), row_uuid), snapshot_row.clone());
        let result = self.overlay_pending_writes(tx_id, table, row_uuid, snapshot_row.clone())?;
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
        tx_id: OpenTxId,
        table: &str,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = self.tx_current_rows_for_query_source(tx_id, table)?;
        self.resolve_tx_large_value_rows(tx_id, table, None, &mut rows)?;
        Ok(rows)
    }

    /// Replace raw transaction-query large-value cells with authored bytes or
    /// snapshot handles after filtering, ordering, and pagination.
    pub(super) fn resolve_tx_large_value_rows(
        &mut self,
        tx_id: OpenTxId,
        table: &str,
        selected_columns: Option<&[String]>,
        rows: &mut [CurrentRow],
    ) -> Result<(), Error> {
        let table_schema = self.table(table)?.clone();
        let selected_columns = selected_columns
            .map(|columns| columns.iter().map(String::as_str).collect::<BTreeSet<_>>());
        let large_columns = table_schema
            .columns
            .iter()
            .filter(|column| {
                column.large_value.is_some()
                    && selected_columns
                        .as_ref()
                        .is_none_or(|selected| selected.contains(column.name.as_str()))
            })
            .cloned()
            .collect::<Vec<_>>();
        if large_columns.is_empty() {
            return Ok(());
        }
        for row in rows {
            let row_uuid = row.row_uuid();
            let mut cells = table_schema
                .columns
                .iter()
                .filter_map(|column| {
                    row.cell(&table_schema, &column.name)
                        .map(|value| (column.name.clone(), value))
                })
                .collect::<BTreeMap<_, _>>();
            for column in &large_columns {
                match self.tx_large_value_cell(tx_id, table, row_uuid, &column.name)? {
                    Some(OpenTxLargeValueCell::Authored(bytes))
                    | Some(OpenTxLargeValueCell::SnapshotHandle(bytes)) => {
                        cells.insert(column.name.clone(), Value::Bytes(bytes));
                    }
                    None => {
                        cells.remove(&column.name);
                    }
                }
            }
            let cells = positional_cells_from_map(&table_schema, &cells)?;
            *row = current_row_from_positional_cells(&table_schema, row_uuid, &cells)?;
        }
        Ok(())
    }

    /// Build raw snapshot-plus-overlay rows for query evaluation. Large-value
    /// cells stay as stored payloads or authored bytes here; selected output
    /// cells are resolved lazily after filtering and pagination.
    pub(super) fn tx_current_rows_for_query_source(
        &mut self,
        tx_id: OpenTxId,
        table: &str,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.table(table)?;
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
        let table_schema = self.table(table)?.clone();
        for row_uuid in rows {
            let mut snapshot_row = self.snapshot_row(table, row_uuid, &snapshot);
            if snapshot_row.content_version.is_some()
                && let Some(cells) = snapshot_row.content_cells.as_mut()
            {
                for (index, column) in table_schema.columns.iter().enumerate() {
                    if column.large_value.is_some() && cells.get(index).is_some_and(Option::is_none)
                    {
                        cells[index] = Some(Value::Bytes(Vec::new()));
                    }
                }
            }
            if let Some(cells) =
                self.overlay_pending_writes(tx_id, table, row_uuid, snapshot_row)?
            {
                let cells = positional_cells_from_map(&table_schema, &cells)?;
                current.push(current_row_from_positional_cells(
                    &table_schema,
                    row_uuid,
                    &cells,
                )?);
            }
        }
        sort_current_rows(&mut current);
        let shape = crate::query::Query::from(table).validate(&self.catalogue.schema)?;
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

    /// Resolve one large-value cell with explicit transaction provenance.
    /// Pending authored bytes never pass through handle decoding; snapshot
    /// values become handles only when a returned cell asks for them.
    pub(crate) fn tx_large_value_cell(
        &mut self,
        tx_id: OpenTxId,
        table: &str,
        row_uuid: RowUuid,
        column: &str,
    ) -> Result<Option<OpenTxLargeValueCell>, Error> {
        let table_schema = self.table(table)?.clone();
        let column_schema = table_schema
            .columns
            .iter()
            .find(|candidate| candidate.name == column)
            .ok_or(Error::InvalidStoredValue(
                "open transaction large-value column is unknown",
            ))?;
        let Some(kind) = column_schema.large_value else {
            return Err(Error::InvalidStoredValue(
                "open transaction cell is not a large value",
            ));
        };

        enum PendingResolution {
            NoOverride,
            Value(Option<Vec<u8>>),
        }
        let pending = self
            .open_tx(tx_id)?
            .writes
            .iter()
            .rev()
            .filter(|write| {
                write.table == table && write.row_uuid == row_uuid && write.deletion.is_none()
            })
            .find_map(|write| match &write.cells {
                PendingCells::Replace(cells) => Some(PendingResolution::Value(
                    cells.get(column).and_then(authored_large_value_bytes),
                )),
                PendingCells::Patch(patch) => patch
                    .get(column)
                    .map(|value| PendingResolution::Value(authored_large_value_bytes(value))),
            })
            .unwrap_or(PendingResolution::NoOverride);
        match pending {
            PendingResolution::Value(Some(bytes)) => {
                return Ok(Some(OpenTxLargeValueCell::Authored(bytes)));
            }
            PendingResolution::Value(None) => return Ok(None),
            PendingResolution::NoOverride => {}
        }

        let snapshot = self.open_tx(tx_id)?.base_snapshot.clone();
        let Some(version) =
            self.snapshot_layer_winner(table, row_uuid, VersionLayer::Content, &snapshot)
        else {
            return Ok(None);
        };
        if matches!(
            column_schema.column_type,
            crate::groove::schema::ColumnType::Nullable(_)
        ) && self.snapshot_large_value_is_null(&table_schema, &version, column)?
        {
            return Ok(None);
        }
        Ok(Some(OpenTxLargeValueCell::SnapshotHandle(
            self.large_value_handle_for_version(
                &table_schema,
                &version,
                &column_schema.name,
                kind,
            )?,
        )))
    }

    fn snapshot_large_value_is_null(
        &mut self,
        table: &TableSchema,
        winner: &VersionRow,
        column: &str,
    ) -> Result<bool, Error> {
        let mut current = self.version_tx_id(winner)?;
        loop {
            let version = self
                .query_versions_for_tx(current)?
                .into_iter()
                .find(|version| {
                    version.table() == table.name
                        && version.row_uuid() == winner.row_uuid()
                        && version.layer() == VersionLayer::Content
                })
                .ok_or(Error::MissingTransaction(current))?;
            match version.cell(table, column)? {
                Some(Value::Nullable(None)) => return Ok(true),
                Some(Value::Nullable(Some(value))) if matches!(*value, Value::Bytes(_)) => {
                    return Ok(false);
                }
                Some(Value::Bytes(_)) => return Ok(false),
                Some(_) => {
                    return Err(Error::InvalidStoredValue(
                        "nullable large-value column has an invalid stored value",
                    ));
                }
                None => {}
            }
            match version.parents().as_slice() {
                [] => return Ok(true),
                [parent] => current = *parent,
                parents => current = self.large_value_primary_parent(parents)?,
            }
        }
    }

    /// Stage a row write inside an exclusive transaction.
    pub fn tx_write<V: Into<Value>>(
        &mut self,
        tx_id: OpenTxId,
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
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let table_schema = self.table_in_schema(table, write_schema_version)?;
        let cells = cells
            .into_iter()
            .map(|(column, value)| (column, value.into()))
            .collect::<BTreeMap<_, _>>();
        validate_mergeable_write_shape(cells.is_empty(), deletion.is_some())?;
        let cache_key = (table.to_owned(), row_uuid);
        let snapshot_row = if let Some(snapshot_row) = self
            .open_tx(tx_id)?
            .base_snapshot_rows
            .get(&cache_key)
            .cloned()
        {
            snapshot_row
        } else {
            let snapshot = self.open_tx(tx_id)?.base_snapshot.clone();
            self.snapshot_row(table, row_uuid, &snapshot)
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
        open_tx.base_snapshot_rows.remove(&cache_key);
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

    /// Stage a partial row update inside an exclusive transaction.
    ///
    /// Keeping the patch distinct from a replacement matters for large-value
    /// columns: an untouched column inherits through the content parent instead
    /// of materializing and re-encoding its stored operation payload.
    pub(crate) fn tx_patch_exclusive(
        &mut self,
        tx_id: OpenTxId,
        table: &str,
        row_uuid: RowUuid,
        patch: BTreeMap<String, Value>,
    ) -> Result<bool, Error> {
        if !matches!(self.open_tx(tx_id)?.kind, OpenTransactionKind::Exclusive) {
            return Err(Error::InvalidMergeableCommit(
                "open transaction is not exclusive",
            ));
        }
        if self.tx_read_raw(tx_id, table, row_uuid)?.is_none() {
            return Ok(false);
        }
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let table_schema = self.table_in_schema(table, write_schema_version)?;
        positional_cells_from_map(&table_schema, &patch)?;
        let cache_key = (table.to_owned(), row_uuid);
        let snapshot_row = self
            .open_tx(tx_id)?
            .base_snapshot_rows
            .get(&cache_key)
            .cloned()
            .ok_or(Error::InvalidStoredValue(
                "exclusive update did not retain its snapshot row",
            ))?;
        let pending = PendingWrite {
            table: table.to_owned(),
            row_uuid,
            schema_version: write_schema_version,
            cells: PendingCells::Patch(patch),
            deletion: None,
            parents: snapshot_row.content_version.into_iter().collect(),
            now_ms: None,
            refresh_parents_at_commit: false,
        };
        let open_tx = self.open_tx_mut(tx_id)?;
        open_tx.base_snapshot_rows.remove(&cache_key);
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
                _ => unreachable!("exclusive update always stages a patch"),
            };
            *existing = PendingWrite { cells, ..pending };
        } else {
            open_tx.writes.push(pending);
        }
        Ok(true)
    }

    pub(crate) fn tx_write_mergeable(
        &mut self,
        tx_id: OpenTxId,
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
        let write_schema_version = self.catalogue.current_write_schema.schema;
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

    pub(crate) fn tx_patch_mergeable(
        &mut self,
        tx_id: OpenTxId,
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
        let mut staged_cells = self.tx_read(tx_id, table, row_uuid)?.unwrap_or_default();
        staged_cells.extend(patch.clone());
        validate_mergeable_write_shape(staged_cells.is_empty(), false)?;
        let write_schema_version = self.catalogue.current_write_schema.schema;
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
        tx_id: OpenTxId,
        pending: PendingWrite,
    ) -> Result<(), Error> {
        let cache_key = (pending.table.clone(), pending.row_uuid);
        let open_tx = self.open_tx_mut(tx_id)?;
        open_tx.base_snapshot_rows.remove(&cache_key);
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
    pub fn tx_set_metadata(&mut self, tx_id: OpenTxId, json: String) -> Result<(), Error> {
        self.open_tx_mut(tx_id)?.user_metadata_json = Some(json);
        Ok(())
    }

    /// Commit an exclusive transaction and return its sync commit unit.
    pub fn commit_exclusive(
        &mut self,
        tx_id: OpenTxId,
        made_by: AuthorId,
        now_ms: u64,
    ) -> Result<(TxId, SyncMessage), Error> {
        if !matches!(self.open_tx(tx_id)?.kind, OpenTransactionKind::Exclusive) {
            return Err(Error::InvalidMergeableCommit(
                "open transaction is not exclusive",
            ));
        }
        let open_tx = self
            .open_tx
            .open_transactions
            .remove(&tx_id)
            .ok_or(Error::MissingOpenTx(tx_id))?;
        for parent in open_tx.writes.iter().flat_map(|write| write.parents.iter()) {
            self.merge_tx_time(parent.time);
        }
        let made_at = self.mint_tx_time(now_ms);
        let tx_id = TxId::new(made_at, self.node_uuid);
        let mut versions = Vec::with_capacity(open_tx.writes.len());
        for write in open_tx.writes {
            let table_schema = self
                .table_in_schema(&write.table, write.schema_version)?
                .clone();
            let parent = match write.parents.first().copied() {
                Some(parent) => Some(
                    self.query_versions_for_tx(parent)?
                        .into_iter()
                        .find(|version| {
                            version.table() == write.table
                                && version.row_uuid() == write.row_uuid
                                && version.layer() == VersionLayer::Content
                        })
                        .ok_or(Error::MissingTransaction(parent))?,
                ),
                None => None,
            };
            let mut cells = match write.cells {
                PendingCells::Replace(cells) => cells,
                PendingCells::Patch(patch) => {
                    let parent = parent.as_ref().ok_or(Error::InvalidMergeableCommit(
                        "exclusive update patch requires a content parent",
                    ))?;
                    let mut cells = BTreeMap::new();
                    for column in table_schema
                        .columns
                        .iter()
                        .filter(|column| column.large_value.is_none())
                    {
                        if let Some(value) = parent.cell(&table_schema, &column.name)? {
                            cells.insert(column.name.clone(), value);
                        }
                    }
                    cells.extend(patch);
                    cells
                }
            };
            cells = self.encode_large_value_cells(
                &table_schema,
                write.schema_version,
                write.row_uuid,
                made_by,
                cells,
                parent.as_ref(),
            )?;
            let cells = positional_cells_from_map(&table_schema, &cells)?;
            versions.push(VersionRecord::encode(
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
            )?);
        }
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
            source_branch: None,
            merge_strategy: None,
        };
        self.ingest_transaction_and_versions(
            tx.clone(),
            versions.clone(),
            Fate::Pending,
            None,
            DurabilityTier::Local,
        )?;
        Ok((tx_id, SyncMessage::CommitUnit { tx, versions }))
    }

    /// Commit a mergeable open transaction through the ordinary mergeable batch path.
    pub(crate) fn commit_mergeable_open(
        &mut self,
        tx_id: OpenTxId,
        mut next_now_ms: impl FnMut() -> u64,
    ) -> Result<TxId, Error> {
        if !matches!(
            self.open_tx(tx_id)?.kind,
            OpenTransactionKind::Mergeable { .. }
        ) {
            return Err(Error::InvalidMergeableCommit(
                "open transaction is not mergeable",
            ));
        }
        let open_tx = self
            .open_tx
            .open_transactions
            .remove(&tx_id)
            .ok_or(Error::MissingOpenTx(tx_id))?;
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
            let cells = match write.cells {
                PendingCells::Replace(cells) => cells,
                PendingCells::Patch(patch) => {
                    let table_schema = self.table(&write.table)?.clone();
                    let mut cells = BTreeMap::new();
                    if let Some(existing) = self.local_current_row(&write.table, write.row_uuid)? {
                        for column in &table_schema.columns {
                            if let Some(value) = existing.cell(&table_schema, &column.name) {
                                cells.insert(column.name.clone(), value);
                            }
                        }
                    }
                    cells.extend(patch);
                    cells
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
            commits.push(commit);
        }
        self.commit_mergeable_many(commits)
    }

    /// Abandon an open transaction.
    pub fn abandon_tx(&mut self, tx_id: OpenTxId) -> Result<(), Error> {
        self.open_tx
            .open_transactions
            .remove(&tx_id)
            .ok_or(Error::MissingOpenTx(tx_id))?;
        Ok(())
    }

    /// Return whether local transaction time advanced after this transaction opened.
    pub fn open_exclusive_snapshot_moved(&self, tx_id: OpenTxId) -> Result<bool, Error> {
        Ok(self.clock.tx_time > self.open_tx(tx_id)?.base_snapshot.local_base)
    }

    pub(super) fn open_tx(&self, tx_id: OpenTxId) -> Result<&OpenTransaction, Error> {
        self.open_tx
            .open_transactions
            .get(&tx_id)
            .ok_or(Error::MissingOpenTx(tx_id))
    }

    pub(super) fn open_tx_mut(&mut self, tx_id: OpenTxId) -> Result<&mut OpenTransaction, Error> {
        self.open_tx
            .open_transactions
            .get_mut(&tx_id)
            .ok_or(Error::MissingOpenTx(tx_id))
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

    pub(super) fn snapshot_row(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        snapshot: &Snapshot,
    ) -> SnapshotRow {
        let content = self.snapshot_layer_winner(table, row_uuid, VersionLayer::Content, snapshot);
        let deletion =
            self.snapshot_layer_winner(table, row_uuid, VersionLayer::Deletion, snapshot);
        let deleted = matches!(
            deletion.as_ref().and_then(|version| version.deletion()),
            Some(DeletionEvent::Deleted)
        );
        let table_schema = self.table(table).ok();
        SnapshotRow {
            content_cells: content.as_ref().and_then(|version| {
                table_schema.map(|schema| {
                    schema
                        .columns
                        .iter()
                        .map(|column| version.peek_cell(schema, &column.name).ok().flatten())
                        .collect::<Vec<_>>()
                })
            }),
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
        }
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

    pub(super) fn overlay_pending_writes(
        &self,
        tx_id: OpenTxId,
        table: &str,
        row_uuid: RowUuid,
        snapshot_row: SnapshotRow,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let table_schema = self.table(table)?;
        let mut cells = snapshot_row
            .content_cells
            .map(|cells| cells_from_positional(&table_schema, &cells));
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

fn authored_large_value_bytes(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Bytes(bytes) => Some(bytes.clone()),
        Value::Nullable(Some(value)) => authored_large_value_bytes(value),
        Value::Nullable(None)
        | Value::U8(_)
        | Value::U16(_)
        | Value::U32(_)
        | Value::U64(_)
        | Value::I32(_)
        | Value::I64(_)
        | Value::F64(_)
        | Value::Bool(_)
        | Value::String(_)
        | Value::Uuid(_)
        | Value::Enum(_)
        | Value::Tuple(_)
        | Value::Array(_) => None,
    }
}

pub(super) struct OpenTransaction {
    /// Commit semantics and attribution carried by this open transaction.
    pub(super) kind: OpenTransactionKind,
    /// Snapshot captured when the transaction opened.
    pub(super) base_snapshot: Snapshot,
    /// Base snapshot row derivations observed by point reads in this transaction.
    pub(super) base_snapshot_rows: BTreeMap<(String, RowUuid), SnapshotRow>,
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
