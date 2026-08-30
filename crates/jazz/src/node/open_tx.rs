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
    pub async fn open_exclusive(&mut self, id: OpenTransactionId) -> Result<(), Error> {
        self.open_transaction(
            id,
            OpenTransactionKind::Exclusive {
                bound_author: Some(AuthorSubject::SYSTEM),
            },
            AuthorSubject::SYSTEM,
        )
        .await
    }

    #[cfg(feature = "testing")]
    /// Open an exclusive transaction for a synthetic test identity.
    pub async fn open_exclusive_for_test(
        &mut self,
        id: OpenTransactionId,
        made_by: AuthorSubject,
    ) -> Result<(), Error> {
        self.open_exclusive_for_identity(id, made_by).await
    }

    pub(crate) async fn open_exclusive_for_identity(
        &mut self,
        id: OpenTransactionId,
        made_by: AuthorSubject,
    ) -> Result<(), Error> {
        self.open_transaction(
            id,
            OpenTransactionKind::Exclusive {
                bound_author: Some(made_by),
            },
            made_by,
        )
        .await
    }

    /// Open a mergeable transaction over the current snapshot.
    pub(crate) async fn open_mergeable(
        &mut self,
        id: OpenTransactionId,
        made_by: AuthorSubject,
        permission_subject: Option<AuthorSubject>,
    ) -> Result<(), Error> {
        self.open_transaction(
            id,
            OpenTransactionKind::Mergeable {
                made_by,
                permission_subject,
            },
            made_by,
        )
        .await
    }

    /// Whether an open mergeable batch separates durable provenance from its
    /// permission subject. Such batches are deliberately root-only until
    /// branch attribution has a complete representation.
    pub(crate) fn mergeable_transaction_is_attributed(
        &self,
        id: OpenTransactionId,
    ) -> Result<bool, Error> {
        match self.open_tx(id)?.kind {
            OpenTransactionKind::Mergeable {
                made_by,
                permission_subject: Some(subject),
            } => Ok(subject != made_by),
            OpenTransactionKind::Mergeable { .. } | OpenTransactionKind::Exclusive { .. } => {
                Ok(false)
            }
        }
    }

    /// Return the policy identity bound to an open mergeable transaction.
    pub(crate) fn mergeable_transaction_permission_subject(
        &self,
        id: OpenTransactionId,
    ) -> Result<Option<AuthorSubject>, Error> {
        match self.open_tx(id)?.kind {
            OpenTransactionKind::Mergeable {
                permission_subject, ..
            } => Ok(permission_subject),
            OpenTransactionKind::Exclusive { .. } => Err(Error::InvalidMergeableCommit(
                "open transaction is not mergeable",
            )),
        }
    }

    /// Return the identity capability bound to an open exclusive transaction.
    pub(crate) fn exclusive_transaction_bound_author(
        &self,
        id: OpenTransactionId,
    ) -> Result<AuthorSubject, Error> {
        match self.open_tx(id)?.kind {
            OpenTransactionKind::Exclusive {
                bound_author: Some(author),
            } => Ok(author),
            OpenTransactionKind::Exclusive { bound_author: None } => {
                Ok(self.open_tx(id)?.provisional_author)
            }
            OpenTransactionKind::Mergeable { .. } => Err(Error::InvalidMergeableCommit(
                "open transaction is not exclusive",
            )),
        }
    }

    async fn open_transaction(
        &mut self,
        id: OpenTransactionId,
        kind: OpenTransactionKind,
        provisional_author: AuthorSubject,
    ) -> Result<(), Error> {
        self.require_catalogue_ready()?;
        if self.open_tx.open_transactions.contains_key(&id)
            || self.open_tx.closed_batches.contains(&id)
        {
            return Err(Error::DuplicateOpenBatch(id));
        }
        let local_base = self.tx_time_high_water();
        let mut dots = Vec::with_capacity(self.clock.applied_global_times_after_frontier.len());
        for global_time in self.clock.applied_global_times_after_frontier.clone() {
            dots.extend(self.transaction_ids_for_global_time(global_time).await?);
        }
        let base_snapshot = Snapshot::exclusive_base(
            self.node_uuid,
            self.clock.committed_global_time,
            local_base,
            dots,
        )
        .map_err(Error::InvalidStoredValue)?;
        self.open_tx.open_transactions.insert(
            id,
            OpenTransaction {
                kind,
                provisional_author,
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
    pub async fn tx_read(
        &mut self,
        tx_id: OpenTransactionId,
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
        .await
    }

    /// Read a row through an explicit registered schema view.
    pub async fn tx_read_in_schema(
        &mut self,
        tx_id: OpenTransactionId,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        self.table_in_schema(table, schema_version)?;
        self.tx_read_unchecked(tx_id, schema_version, table, row_uuid)
            .await
    }

    async fn tx_read_unchecked(
        &mut self,
        tx_id: OpenTransactionId,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let snapshot = self.open_tx(tx_id)?.base_snapshot.clone();
        let snapshot_row = self
            .snapshot_row_in_schema(schema_version, table, row_uuid, &snapshot)
            .await?;
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
    pub async fn tx_current_rows(
        &mut self,
        tx_id: OpenTransactionId,
        table: &str,
    ) -> Result<Vec<CurrentRow>, Error> {
        let schema_version = self.catalogue.current_write_schema.schema;
        let table_schema = self.table(table)?.clone();
        self.tx_current_rows_with_table(tx_id, schema_version, table, table_schema, false)
            .await
    }

    /// Read current rows through an explicit registered schema view.
    pub async fn tx_current_rows_in_schema(
        &mut self,
        tx_id: OpenTransactionId,
        schema_version: SchemaVersionId,
        table: &str,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table_schema = self.table_in_schema(table, schema_version)?;
        self.tx_current_rows_with_table(tx_id, schema_version, table, table_schema, false)
            .await
    }

    /// Read transaction rows through a registered schema view, optionally
    /// retaining root rows whose deletion register wins.
    pub(crate) async fn tx_current_rows_in_schema_with_options(
        &mut self,
        tx_id: OpenTransactionId,
        schema_version: SchemaVersionId,
        table: &str,
        include_deleted: bool,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table_schema = self.table_in_schema(table, schema_version)?;
        self.tx_current_rows_with_table(tx_id, schema_version, table, table_schema, include_deleted)
            .await
    }

    async fn tx_current_rows_with_table(
        &mut self,
        tx_id: OpenTransactionId,
        schema_version: SchemaVersionId,
        table: &str,
        table_schema: TableSchema,
        include_deleted: bool,
    ) -> Result<Vec<CurrentRow>, Error> {
        let snapshot = self.open_tx(tx_id)?.base_snapshot.clone();
        let rows = self
            .query_table_versions(table)
            .await?
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
            let snapshot_row = self
                .snapshot_row_in_schema(schema_version, table, row_uuid, &snapshot)
                .await?;
            let snapshot_provenance = snapshot_row.provenance.clone();
            let open_tx = self.open_tx(tx_id)?;
            let provisional_author = open_tx.provisional_author;
            let pending_writes = open_tx
                .writes
                .iter()
                .filter(|write| write.table == table && write.row_uuid == row_uuid)
                .cloned()
                .collect::<Vec<_>>();
            let (cells, deleted) = self.overlay_pending_cells_and_deletion_with_table(
                tx_id,
                &table_schema,
                table,
                row_uuid,
                snapshot_row,
            )?;
            if let Some(cells) = cells
                && (!deleted || include_deleted)
            {
                let snapshot_projection = snapshot_provenance.as_ref().map(|(created, updated)| {
                    (
                        RowProvenance {
                            created_by: created.created_by(),
                            created_at: created.created_at().physical_ms(),
                            updated_by: updated.updated_by(),
                            updated_at: updated.updated_at().physical_ms(),
                        },
                        (updated.tx_time(), updated.tx_node_alias()),
                    )
                });
                let mut provenance = snapshot_projection.map(|(provenance, _)| provenance);
                for write in &pending_writes {
                    let Some(now_ms) = write.now_ms else {
                        continue;
                    };
                    let updated_at = now_ms;
                    provenance = Some(match provenance {
                        Some(existing) => RowProvenance {
                            updated_by: provisional_author,
                            updated_at,
                            ..existing
                        },
                        None => RowProvenance {
                            created_by: provisional_author,
                            created_at: updated_at,
                            updated_by: provisional_author,
                            updated_at,
                        },
                    });
                }
                let row = if let Some(provenance) = provenance {
                    current_row_from_cells_with_explicit_provenance(
                        &table_schema,
                        row_uuid,
                        &cells,
                        provenance,
                        snapshot_projection.map(|(_, projected)| projected),
                    )?
                } else {
                    let cells = positional_cells_from_map(&table_schema, &cells)?;
                    current_row_from_positional_cells(&table_schema, row_uuid, &cells)?
                };
                current.push(if deleted { row.into_deleted() } else { row });
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
    pub async fn tx_write<V: Into<Value>>(
        &mut self,
        tx_id: OpenTransactionId,
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
        .await
    }

    /// Stage a row write through an explicit registered schema view.
    pub async fn tx_write_in_schema<V: Into<Value>>(
        &mut self,
        tx_id: OpenTransactionId,
        write_schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        cells: BTreeMap<String, V>,
        deletion: Option<DeletionEvent>,
    ) -> Result<(), Error> {
        self.tx_write_in_schema_at_ms(
            tx_id,
            write_schema_version,
            table,
            row_uuid,
            cells,
            deletion,
            None,
        )
        .await
    }

    pub(crate) async fn tx_write_in_schema_at_ms<V: Into<Value>>(
        &mut self,
        tx_id: OpenTransactionId,
        write_schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        cells: BTreeMap<String, V>,
        deletion: Option<DeletionEvent>,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        if !matches!(
            self.open_tx(tx_id)?.kind,
            OpenTransactionKind::Exclusive { .. }
        ) {
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
            self.snapshot_row_in_schema(write_schema_version, table, row_uuid, &snapshot)
                .await?
        };
        // Content and deletion are independent history registers.  A version
        // parent is ancestry for the register being written, never a generic
        // causal dependency on whichever row version happened to be read.
        let parent = match deletion {
            Some(_) => snapshot_row.deletion_version,
            None => snapshot_row.content_version,
        };
        positional_cells_from_map(&table_schema, &cells)?;
        let pending = PendingWrite {
            table: table.to_owned(),
            row_uuid,
            schema_version: write_schema_version,
            branch: BranchSelector::default(),
            cells: PendingCells::Replace(cells),
            deletion,
            parents: parent.into_iter().collect(),
            now_ms,
            refresh_parents_at_commit: false,
            known_fresh_row: false,
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
    pub(crate) async fn tx_write_mergeable(
        &mut self,
        tx_id: OpenTransactionId,
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
            false,
        )
        .await
    }

    pub(crate) async fn tx_write_mergeable_in_schema(
        &mut self,
        tx_id: OpenTransactionId,
        write_schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        cells: BTreeMap<String, Value>,
        deletion: Option<DeletionEvent>,
        parents: Vec<TxId>,
        now_ms: Option<u64>,
        refresh_parents_at_commit: bool,
        known_fresh_row: bool,
    ) -> Result<(), Error> {
        self.tx_write_mergeable_in_schema_and_branch(
            tx_id,
            write_schema_version,
            table,
            row_uuid,
            cells,
            deletion,
            parents,
            now_ms,
            refresh_parents_at_commit,
            BranchSelector::default(),
            known_fresh_row,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn tx_write_mergeable_in_schema_and_branch(
        &mut self,
        tx_id: OpenTransactionId,
        write_schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        cells: BTreeMap<String, Value>,
        deletion: Option<DeletionEvent>,
        parents: Vec<TxId>,
        now_ms: Option<u64>,
        refresh_parents_at_commit: bool,
        branch: BranchSelector,
        known_fresh_row: bool,
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
                branch,
                cells: PendingCells::Replace(cells),
                deletion,
                parents,
                now_ms,
                refresh_parents_at_commit,
                known_fresh_row,
            },
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn tx_patch_mergeable(
        &mut self,
        tx_id: OpenTransactionId,
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
        .await
    }

    pub(crate) async fn tx_patch_mergeable_in_schema(
        &mut self,
        tx_id: OpenTransactionId,
        write_schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        patch: BTreeMap<String, Value>,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.tx_patch_mergeable_in_schema_and_branch(
            tx_id,
            write_schema_version,
            table,
            row_uuid,
            patch,
            now_ms,
            BranchSelector::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn tx_patch_mergeable_in_schema_and_branch(
        &mut self,
        tx_id: OpenTransactionId,
        write_schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        patch: BTreeMap<String, Value>,
        now_ms: Option<u64>,
        branch: BranchSelector,
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
            .visible_current_cells_in_branch(table, &branch, row_uuid)
            .await?
            .unwrap_or_default();
        for write in self.open_tx(tx_id)?.writes.iter().filter(|write| {
            write.table == table && write.row_uuid == row_uuid && write.branch == branch
        }) {
            match &write.cells {
                PendingCells::Replace(cells) => staged_cells = cells.clone(),
                PendingCells::Patch(patch) => staged_cells.extend(patch.clone()),
            }
        }
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
                branch,
                cells: PendingCells::Patch(patch),
                deletion: None,
                parents: Vec::new(),
                now_ms,
                refresh_parents_at_commit: false,
                known_fresh_row: false,
            },
        )
    }

    fn stage_mergeable_write(
        &mut self,
        tx_id: OpenTransactionId,
        mut pending: PendingWrite,
    ) -> Result<(), Error> {
        let open_tx = self.open_tx_mut(tx_id)?;
        open_tx
            .base_snapshot_rows
            .retain(|(_, table, row), _| table != &pending.table || *row != pending.row_uuid);
        if pending.deletion.is_none() {
            if let Some(existing) = open_tx.writes.iter_mut().find(|write| {
                write.table == pending.table
                    && write.row_uuid == pending.row_uuid
                    && write.branch == pending.branch
                    && write.deletion.is_none()
            }) {
                pending.known_fresh_row |= existing.known_fresh_row;
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
                || existing.branch != pending.branch
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
    pub fn tx_set_metadata(&mut self, tx_id: OpenTransactionId, json: String) -> Result<(), Error> {
        self.open_tx_mut(tx_id)?.user_metadata_json = Some(json);
        Ok(())
    }

    /// Commit an exclusive transaction and return its sync commit unit.
    pub(crate) async fn commit_exclusive_bound(
        &mut self,
        open_batch_id: OpenTransactionId,
        now_ms: u64,
    ) -> Result<(PublishedTransaction, SyncMessage), Error> {
        let OpenTransactionKind::Exclusive {
            bound_author: Some(author),
        } = self.open_tx(open_batch_id)?.kind
        else {
            return Err(Error::OpenTransactionIdentityMismatch);
        };
        self.commit_exclusive(open_batch_id, author, now_ms).await
    }

    /// Commit an exclusive transaction and return its sync commit unit.
    pub async fn commit_exclusive(
        &mut self,
        open_batch_id: OpenTransactionId,
        made_by: AuthorSubject,
        now_ms: u64,
    ) -> Result<(PublishedTransaction, SyncMessage), Error> {
        let made_by = match self.open_tx(open_batch_id)?.kind {
            OpenTransactionKind::Exclusive {
                bound_author: Some(bound_author),
            } if bound_author != made_by => return Err(Error::OpenTransactionIdentityMismatch),
            OpenTransactionKind::Exclusive {
                bound_author: Some(bound_author),
            } => bound_author,
            OpenTransactionKind::Exclusive { bound_author: None } => made_by,
            OpenTransactionKind::Mergeable { .. } => {
                return Err(Error::InvalidMergeableCommit(
                    "open transaction is not exclusive",
                ));
            }
        };
        if !self
            .open_exclusive_is_locally_serializable(open_batch_id)
            .await?
        {
            return Err(Error::TransactionConflict);
        }
        let open_tx = self
            .open_tx
            .open_transactions
            .get(&open_batch_id)
            .cloned()
            .ok_or(Error::MissingOpenBatch(open_batch_id))?;
        for write in &open_tx.writes {
            if let Some(provenance_ms) = write.now_ms {
                TxTime::from_physical_ms(provenance_ms).map_err(|_| {
                    Error::InvalidMergeableCommit(
                        "exclusive write now_ms exceeds packed HLC physical-millisecond range",
                    )
                })?;
            }
        }
        for parent in open_tx.writes.iter().flat_map(|write| write.parents.iter()) {
            self.merge_tx_time(parent.time);
        }
        let made_at = self.mint_tx_time(now_ms)?;
        let tx_id = TxId::new(made_at, self.node_uuid);
        let provenance_snapshot = open_tx.base_snapshot.clone();
        let mut versions = Vec::with_capacity(open_tx.writes.len());
        for write in open_tx.writes {
            let snapshot_content = self
                .snapshot_layer_winner(
                    &write.table,
                    write.row_uuid,
                    VersionLayer::Content,
                    &provenance_snapshot,
                )
                .await;
            let table_schema = self.table_in_schema(&write.table, write.schema_version)?;
            let PendingCells::Replace(mut cells) = write.cells else {
                return Err(Error::InvalidMergeableCommit(
                    "exclusive transaction cannot contain update patches",
                ));
            };
            let snapshot_row = self
                .snapshot_row_in_schema(
                    write.schema_version,
                    &write.table,
                    write.row_uuid,
                    &provenance_snapshot,
                )
                .await?;
            let inherited = table_schema
                .columns
                .iter()
                .zip(snapshot_row.content_cells.unwrap_or_default())
                .filter_map(|(column, value)| value.map(|value| (column.name.clone(), value)))
                .collect::<BTreeMap<_, _>>();
            for (column, value) in &cells {
                if value_contains_indirect_descriptor(value) && inherited.get(column) != Some(value)
                {
                    return Err(Error::InvalidMergeableCommit(
                        "exclusive transaction contains an unverified large-value descriptor",
                    ));
                }
            }
            for (column, value) in &mut cells {
                let semantic_kind = table_schema
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == *column)
                    .map(|column| column.large_value_kind)
                    .unwrap_or(crate::schema::LargeValueSemanticKind::NotLarge);
                self.prepare_and_stage_large_scalar(value, semantic_kind)
                    .await?;
            }
            let cells = positional_cells_from_map(&table_schema, &cells)?;
            let provenance_at =
                TxTime::from_physical_ms(write.now_ms.unwrap_or(now_ms)).map_err(|_| {
                    Error::InvalidMergeableCommit(
                        "exclusive write now_ms exceeds packed HLC physical-millisecond range",
                    )
                })?;
            let (created_by, created_at) = snapshot_content
                .as_ref()
                .map(|version| (version.created_by(), version.created_at()))
                .unwrap_or((made_by, provenance_at));
            versions.push(VersionRecord::encode(
                &table_schema,
                write.schema_version,
                write.row_uuid,
                write.parents,
                created_by,
                created_at.physical_ms(),
                made_by,
                provenance_at.physical_ms(),
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
            // Exclusive writes carry their trusted open-session identity
            // explicitly, just like immediate mergeable session writes. This
            // keeps authority policy evaluation independent from the transport
            // link's SYSTEM credential.
            permission_subject: Some(made_by),
            base_snapshot: Some(open_tx.base_snapshot),
            row_read_set: Some(open_tx.row_reads),
            absent_read_set: Some(open_tx.absent_reads),
            predicate_read_set: Some(open_tx.predicate_reads),
            user_metadata_json: open_tx.user_metadata_json,
            contribution_merge: None,
        };
        let publication = self
            .publish_pending_transaction_and_versions(
                tx.clone(),
                versions.clone(),
                self.authored_commit_durability,
            )
            .await?;
        self.open_tx.open_transactions.remove(&open_batch_id);
        self.open_tx.closed_batches.insert(open_batch_id);
        Ok((publication, SyncMessage::CommitUnit { tx, versions }))
    }

    async fn open_exclusive_is_locally_serializable(
        &mut self,
        open_batch_id: OpenTransactionId,
    ) -> Result<bool, Error> {
        let open_tx = self.open_tx(open_batch_id)?.clone();
        for read in &open_tx.row_reads {
            for version in self.query_row_versions(&read.table, read.row_uuid).await? {
                let tx_id = self.version_tx_id(&version)?;
                let visible = self
                    .query_transaction(tx_id)
                    .await?
                    .is_some_and(|stored| !matches!(stored.fate, Fate::Rejected(_)));
                if visible && !self.snapshot_covers(tx_id, &open_tx.base_snapshot).await {
                    return Ok(false);
                }
            }
        }
        for absent in &open_tx.absent_reads {
            for version in self
                .query_row_versions(&absent.table, absent.row_uuid)
                .await?
            {
                let tx_id = self.version_tx_id(&version)?;
                let visible = self
                    .query_transaction(tx_id)
                    .await?
                    .is_some_and(|stored| !matches!(stored.fate, Fate::Rejected(_)));
                if visible && !self.snapshot_covers(tx_id, &open_tx.base_snapshot).await {
                    return Ok(false);
                }
            }
        }
        for write in &open_tx.writes {
            for version in self
                .query_row_versions(&write.table, write.row_uuid)
                .await?
            {
                let tx_id = self.version_tx_id(&version)?;
                let visible = self
                    .query_transaction(tx_id)
                    .await?
                    .is_some_and(|stored| !matches!(stored.fate, Fate::Rejected(_)));
                if visible && !self.snapshot_covers(tx_id, &open_tx.base_snapshot).await {
                    return Ok(false);
                }
            }
        }
        for predicate in &open_tx.predicate_reads {
            for version in self.query_table_versions(&predicate.table).await? {
                let tx_id = self.version_tx_id(&version)?;
                let visible = self
                    .query_transaction(tx_id)
                    .await?
                    .is_some_and(|stored| !matches!(stored.fate, Fate::Rejected(_)));
                if visible && !self.snapshot_covers(tx_id, &open_tx.base_snapshot).await {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Commit a mergeable open transaction through the ordinary mergeable batch path.
    pub(crate) async fn commit_mergeable_open(
        &mut self,
        open_batch_id: OpenTransactionId,
        mut next_now_ms: impl FnMut() -> u64,
    ) -> Result<PublishedTransaction, Error> {
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
            let parents = if write.refresh_parents_at_commit {
                if write.deletion.is_none() {
                    self.local_content_winner_tx_id_in_branch(
                        &write.table,
                        &write.branch,
                        write.row_uuid,
                    )
                    .await?
                } else {
                    self.local_deletion_winner_tx_id_in_branch(
                        &write.table,
                        &write.branch,
                        write.row_uuid,
                    )
                    .await?
                }
                .into_iter()
                .collect()
            } else {
                write.parents
            };
            let (cells, authored_columns) = match write.cells {
                PendingCells::Replace(cells) => (cells, None),
                PendingCells::Patch(patch) => {
                    let mut cells = BTreeMap::new();
                    if let Some(existing) = self
                        .visible_current_cells_in_branch(
                            &write.table,
                            &write.branch,
                            write.row_uuid,
                        )
                        .await?
                    {
                        cells.extend(existing);
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
            .branch(write.branch)
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
            if write.known_fresh_row {
                commit = commit.known_fresh_row();
            }
            if index == 0
                && let Some(metadata) = open_tx.user_metadata_json.as_ref()
            {
                commit = commit.user_metadata(metadata.clone());
            }
            commits.push((write.schema_version, commit));
        }
        // Constructing an open batch may require snapshot reads, but it must
        // not advance HLC/parent state until *every* lowered write is valid.
        // In particular, a later invalid public provenance value must not make
        // an otherwise valid first write observably consume a clock position.
        for (_, commit) in &commits {
            commit.validate()?;
        }
        let first = commits.first().ok_or(Error::InvalidMergeableCommit(
            "mergeable transaction requires at least one write",
        ))?;
        for (_, commit) in &commits {
            for parent in &commit.parents {
                self.merge_tx_time(parent.time);
            }
        }
        let made_at = self.mint_tx_time(first.1.now_ms)?;
        let committed = self
            .commit_mergeable_many_at_with_schema_versions(commits, made_at)
            .await?;
        self.open_tx.open_transactions.remove(&open_batch_id);
        self.open_tx.closed_batches.insert(open_batch_id);
        Ok(committed)
    }

    /// Abandon an open transaction.
    pub fn abandon_tx(&mut self, tx_id: OpenTransactionId) -> Result<(), Error> {
        self.open_tx
            .open_transactions
            .remove(&tx_id)
            .ok_or(Error::MissingOpenBatch(tx_id))?;
        self.open_tx.closed_batches.insert(tx_id);
        Ok(())
    }

    /// Return whether local transaction time advanced after this transaction opened.
    pub fn open_exclusive_snapshot_moved(&self, tx_id: OpenTransactionId) -> Result<bool, Error> {
        Ok(self.tx_time_high_water() > self.open_tx(tx_id)?.base_snapshot.local_base)
    }

    pub(super) fn open_tx(&self, tx_id: OpenTransactionId) -> Result<&OpenTransaction, Error> {
        self.open_tx
            .open_transactions
            .get(&tx_id)
            .ok_or(Error::MissingOpenBatch(tx_id))
    }

    pub(super) fn open_tx_mut(
        &mut self,
        tx_id: OpenTransactionId,
    ) -> Result<&mut OpenTransaction, Error> {
        self.open_tx
            .open_transactions
            .get_mut(&tx_id)
            .ok_or(Error::MissingOpenBatch(tx_id))
    }

    pub(super) fn record_applied_global_time(
        &mut self,
        global_time: GlobalTime,
    ) -> Vec<GlobalTime> {
        self.clock.global_time_register = self.clock.global_time_register.max(global_time);
        if global_time <= self.clock.committed_global_time {
            return Vec::new();
        }
        let newly_applied = self
            .clock
            .applied_global_times_after_frontier
            .insert(global_time);
        let locally_minted = self.clock.locally_minted_global_times.remove(&global_time);
        if self.history_complete || locally_minted {
            self.clock.committed_global_time = global_time;
            self.clock
                .applied_global_times_after_frontier
                .retain(|applied| *applied > global_time);
        }
        newly_applied.then_some(global_time).into_iter().collect()
    }

    pub(super) async fn transaction_ids_for_global_time(
        &mut self,
        global_time: GlobalTime,
    ) -> Result<Vec<TxId>, Error> {
        let mut tx_ids = Vec::new();
        for raw in self
            .database
            .index_scan_raw(
                "jazz_transactions",
                "by_global_time",
                &[Value::Nullable(Some(Box::new(Value::U64(global_time.0))))],
            )
            .await?
        {
            let record = raw.record();
            let node_alias = NodeAlias(record.get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)?);
            let node = self
                .node_for_alias(node_alias)
                .ok_or(Error::InvalidStoredValue(
                    "transaction node alias must exist",
                ))?;
            tx_ids.push(TxId::new(
                TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?),
                node,
            ));
        }
        Ok(tx_ids)
    }

    pub(super) async fn snapshot_covers(&mut self, tx_id: TxId, snapshot: &Snapshot) -> bool {
        self.query_transaction(tx_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|stored| {
                stored
                    .global_time
                    .is_some_and(|global_time| global_time <= snapshot.global_base)
                    || (tx_id.node == snapshot.owner && tx_id.time <= snapshot.local_base)
                    || snapshot.dots.contains(&tx_id)
            })
    }

    pub(super) async fn snapshot_row_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        snapshot: &Snapshot,
    ) -> Result<SnapshotRow, Error> {
        let content = self
            .snapshot_layer_winner(table, row_uuid, VersionLayer::Content, snapshot)
            .await;
        let deletion = self
            .snapshot_layer_winner(table, row_uuid, VersionLayer::Deletion, snapshot)
            .await;
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
        let provenance = if let Some(content) = content.as_ref() {
            let content_tx = self.version_tx_id(content)?;
            let updated = if let Some(deletion) = deletion.as_ref() {
                let deletion_tx = self.version_tx_id(deletion)?;
                if deletion.tx_time().sort_key(deletion_tx.node)
                    > content.tx_time().sort_key(content_tx.node)
                {
                    deletion
                } else {
                    content
                }
            } else {
                content
            };
            Some((content.clone(), updated.clone()))
        } else {
            None
        };
        Ok(SnapshotRow {
            content_cells,
            content_version: content
                .as_ref()
                .and_then(|version| self.version_tx_id(version).ok()),
            deletion_version: deletion
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
            provenance,
        })
    }

    pub(super) async fn snapshot_layer_winner(
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
        let versions = self.query_row_versions(table, row_uuid).await.ok()?;
        let mut candidate_indices = Vec::new();
        for (idx, version) in versions.iter().enumerate() {
            let tx_id = self.version_tx_id(version).ok()?;
            if version.layer() == layer && self.snapshot_covers(tx_id, snapshot).await {
                candidate_indices.push(idx);
            }
        }
        current_version_index(&versions, &candidate_indices, layer, &self.node_aliases)
            .map(|idx| versions[idx].clone())
    }

    pub(super) async fn snapshot_content_witness(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        snapshot: &Snapshot,
    ) -> Option<TxId> {
        let version = self
            .snapshot_layer_winner(table, row_uuid, VersionLayer::Content, snapshot)
            .await?;
        self.version_tx_id(&version).ok()
    }

    fn overlay_pending_writes_in_schema(
        &self,
        tx_id: OpenTransactionId,
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
        tx_id: OpenTransactionId,
        table_schema: &TableSchema,
        table: &str,
        row_uuid: RowUuid,
        snapshot_row: SnapshotRow,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let (cells, deleted) = self.overlay_pending_cells_and_deletion_with_table(
            tx_id,
            table_schema,
            table,
            row_uuid,
            snapshot_row,
        )?;
        Ok(if deleted { None } else { cells })
    }

    fn overlay_pending_cells_and_deletion_with_table(
        &self,
        tx_id: OpenTransactionId,
        table_schema: &TableSchema,
        table: &str,
        row_uuid: RowUuid,
        snapshot_row: SnapshotRow,
    ) -> Result<(Option<BTreeMap<String, Value>>, bool), Error> {
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
        Ok((cells, deleted))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OpenTransactionKind {
    Exclusive {
        bound_author: Option<AuthorSubject>,
    },
    Mergeable {
        made_by: AuthorSubject,
        permission_subject: Option<AuthorSubject>,
    },
}

#[derive(Clone)]
pub(super) struct OpenTransaction {
    /// Commit semantics and attribution carried by this open transaction.
    pub(super) kind: OpenTransactionKind,
    /// Author reflected by transaction-local provenance before commit.
    pub(super) provisional_author: AuthorSubject,
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
    /// Exact branch coordinate of this row branch-local row.
    pub(super) branch: BranchSelector,
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
    /// The production UUID source generated this staged insert's id, so it may
    /// use the trusted fresh-coordinate fast path.
    pub(super) known_fresh_row: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct SnapshotRow {
    content_cells: Option<Vec<Option<Value>>>,
    content_version: Option<TxId>,
    deletion_version: Option<TxId>,
    read_version: Option<TxId>,
    deleted: bool,
    provenance: Option<(VersionRow, VersionRow)>,
}
