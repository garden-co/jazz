//! Open exclusive transaction lifecycle and snapshot-overlay reads. This module
//! owns `tx_read`, `tx_query`, write buffering, and commit-unit construction for
//! exclusive transactions described in `jazz/SPEC/6_queries.md`; authority-side
//! validation and fate assignment live in [`super::ingest`], while query
//! execution helpers live in [`super::query_eval`]. It is the node API layer used
//! by the `Db` facade before writes become protocol commit units.

use super::*;

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Open an exclusive transaction over the current snapshot.
    pub fn open_exclusive(&mut self) -> Result<OpenTxId, Error> {
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
        self.open_tx.open_exclusive.insert(
            id,
            OpenExclusive {
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
        let table_schema = self.table(table)?.clone();
        let snapshot = self.open_tx(tx_id)?.base_snapshot.clone();
        let snapshot_row = self.snapshot_row(table, row_uuid, &snapshot);
        self.open_tx_mut(tx_id)?
            .base_snapshot_rows
            .insert((table.to_owned(), row_uuid), snapshot_row.clone());
        let result = self
            .overlay_pending_writes(tx_id, table, row_uuid, snapshot_row.clone())?
            .map(|cells| cells_from_positional(&table_schema, &cells));
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
            let snapshot_row = self.snapshot_row(table, row_uuid, &snapshot);
            if let Some(cells) =
                self.overlay_pending_writes(tx_id, table, row_uuid, snapshot_row)?
            {
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

    /// Stage a row write inside an exclusive transaction.
    pub fn tx_write<V: Into<Value>>(
        &mut self,
        tx_id: OpenTxId,
        table: &str,
        row_uuid: RowUuid,
        cells: BTreeMap<String, V>,
        deletion: Option<DeletionEvent>,
    ) -> Result<(), Error> {
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
        let pending = PendingWrite {
            table: table.to_owned(),
            row_uuid,
            schema_version: write_schema_version,
            cells: positional_cells_from_map(&table_schema, &cells)?,
            deletion,
            parent,
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

    /// Attach application metadata to an exclusive transaction.
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
        let open_tx = self
            .open_tx
            .open_exclusive
            .remove(&tx_id)
            .ok_or(Error::MissingOpenTx(tx_id))?;
        for parent in open_tx.writes.iter().filter_map(|write| write.parent) {
            self.merge_tx_time(parent.time);
        }
        let made_at = self.mint_tx_time(now_ms);
        let tx_id = TxId::new(made_at, self.node_uuid);
        let versions = open_tx
            .writes
            .into_iter()
            .map(|write| {
                let table_schema = self.table_in_schema(&write.table, write.schema_version)?;
                Ok(VersionRecord::encode(
                    &table_schema,
                    write.schema_version,
                    write.row_uuid,
                    write.parent.into_iter().collect(),
                    made_by,
                    made_at,
                    made_by,
                    made_at,
                    &write.cells,
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

    /// Abandon an open exclusive transaction.
    pub fn abandon_tx(&mut self, tx_id: OpenTxId) -> Result<(), Error> {
        self.open_tx
            .open_exclusive
            .remove(&tx_id)
            .ok_or(Error::MissingOpenTx(tx_id))?;
        Ok(())
    }

    /// Return whether local transaction time advanced after this transaction opened.
    pub fn open_exclusive_snapshot_moved(&self, tx_id: OpenTxId) -> Result<bool, Error> {
        Ok(self.clock.tx_time > self.open_tx(tx_id)?.base_snapshot.local_base)
    }

    pub(super) fn open_tx(&self, tx_id: OpenTxId) -> Result<&OpenExclusive, Error> {
        self.open_tx
            .open_exclusive
            .get(&tx_id)
            .ok_or(Error::MissingOpenTx(tx_id))
    }

    pub(super) fn open_tx_mut(&mut self, tx_id: OpenTxId) -> Result<&mut OpenExclusive, Error> {
        self.open_tx
            .open_exclusive
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
    ) -> Result<Option<Vec<Option<Value>>>, Error> {
        let mut cells = snapshot_row.content_cells;
        let mut deleted = snapshot_row.deleted;
        for write in self
            .open_tx(tx_id)?
            .writes
            .iter()
            .filter(|write| write.table == table && write.row_uuid == row_uuid)
        {
            if write.cells.iter().any(Option::is_some) {
                cells = Some(write.cells.clone());
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

pub struct OpenExclusive {
    /// Snapshot captured when the transaction opened.
    pub base_snapshot: Snapshot,
    /// Base snapshot row derivations observed by point reads in this transaction.
    pub(super) base_snapshot_rows: BTreeMap<(String, RowUuid), SnapshotRow>,
    /// Point reads recorded by the transaction.
    pub row_reads: Vec<RowRead>,
    /// Absent-row reads recorded by the transaction.
    pub absent_reads: Vec<AbsentRead>,
    /// Predicate reads recorded by the transaction.
    pub predicate_reads: Vec<PredicateRead>,
    /// Pending writes staged by the transaction.
    pub writes: Vec<PendingWrite>,
    /// Optional application metadata.
    pub user_metadata_json: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
/// Pending row write inside an exclusive transaction.
pub struct PendingWrite {
    /// Target table.
    pub table: String,
    /// Target row.
    pub row_uuid: RowUuid,
    /// Schema version used to encode staged cells.
    pub schema_version: SchemaVersionId,
    /// User cells to write.
    pub cells: Vec<Option<Value>>,
    /// Deletion-register event, if any.
    pub deletion: Option<DeletionEvent>,
    /// Parent content version, if any.
    pub parent: Option<TxId>,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct SnapshotRow {
    content_cells: Option<Vec<Option<Value>>>,
    content_version: Option<TxId>,
    read_version: Option<TxId>,
    deleted: bool,
}
