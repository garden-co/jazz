/// Plain cells of an image keyed by physical column id: each cell's column
/// type and value (`None` is null). `_deletion` is keyed by
/// `DELETION_COLUMN_ID`.
pub(super) type PhysicalCells = BTreeMap<PhysicalColumnId, (ValueType, Option<Value>)>;

/// The `_deletion` cell's type: the same in every layout.
fn deletion_cell_type() -> ValueType {
    ValueType::Nullable(Box::new(ValueType::Bool))
}

/// What a node's accepted history says about one accepted write (SPEC 4
/// §4.6, "Maybe conflicting"). Derived on demand; nothing of it is stored.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::node) struct WriteConflict {
    /// The write's resolved base seq: the larger of its base seq and its
    /// pending predecessor's resolved seq. `None` for a write without a
    /// base, and for a chain whose every predecessor Core rejected.
    pub(in crate::node) resolved_base: Option<GlobalTime>,
    /// The seq of the row's accepted record immediately before the write.
    pub(in crate::node) previous: Option<GlobalTime>,
    /// The write has a base, and it does not resolve to `previous`.
    pub(in crate::node) maybe_conflicting: bool,
    /// The conflict analysis: the accepted writes after the resolved base
    /// and before the write whose authored columns overlap the write's, in
    /// seq order.
    pub(in crate::node) overlapping: Vec<TxId>,
}

/// Whether two writes' authored columns overlap; `None` authors every
/// column, `_deletion` included.
#[cfg_attr(not(test), allow(dead_code))]
fn authored_overlap(
    left: Option<&BTreeSet<PhysicalColumnId>>,
    right: Option<&BTreeSet<PhysicalColumnId>>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => !left.is_disjoint(right),
        (Some(columns), None) | (None, Some(columns)) => !columns.is_empty(),
        (None, None) => true,
    }
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Check the base of a write by `writer` to a row whose current image is
    /// at `current_seq` (SPEC 4 §4.6, "Validating the base at Core").
    /// `Ok(Err(reason))` refuses the write: the authority accepts a base it
    /// can name exactly or not at all (`INV-HIST-20`). The base takes no part
    /// in the merge itself: every accepted write applies as a patch.
    ///
    /// It reads at most the row's history record at the base seq.
    pub(super) async fn validate_write_base(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        base: RowBase,
        writer: TxId,
        current_seq: Option<GlobalTime>,
    ) -> Result<Result<(), String>, Error> {
        if base.is_empty() {
            return Ok(Ok(()));
        }
        let not_supported = |detail: String| {
            format!(
                "the base of the write to '{table}' row {} cannot be resolved: {detail}; writes over such a base are not supported yet",
                row_uuid.0
            )
        };
        if base.seq == Some(GlobalTime(0)) {
            // Seq 0 is where history keeps writes without an accepted fate;
            // no accepted image is at it.
            return Ok(Err(not_supported(
                "base seq 0 names no accepted write".to_owned(),
            )));
        }
        if let Some(seq) = base.seq
            && current_seq.is_none_or(|current| seq > current)
        {
            return Ok(Err(not_supported(format!(
                "base seq {} is above the row's current seq",
                seq.0
            ))));
        }
        if let Some(pending) = base.pending {
            if pending.node != writer.node {
                return Ok(Err(not_supported(
                    "its pending predecessor was written by another node".to_owned(),
                )));
            }
            if pending.time >= writer.time {
                return Ok(Err(not_supported(
                    "its pending predecessor is not older than the write".to_owned(),
                )));
            }
            // An unknown or still pending predecessor is answered with
            // `RetryLater` before this check (`commit_unit_awaited_predecessor`);
            // reaching either here means no fate will come.
            match self.query_transaction(pending).await? {
                None => {
                    return Ok(Err(not_supported(
                        "its pending predecessor is unknown here".to_owned(),
                    )));
                }
                Some(stored) if matches!(stored.fate, Fate::Pending) => {
                    return Ok(Err(not_supported(
                        "its pending predecessor has no fate here yet".to_owned(),
                    )));
                }
                Some(_) => {}
            }
        }
        let Some(seq) = base.seq else {
            return Ok(Ok(()));
        };
        if Some(seq) == current_seq {
            // The row's current image is its accepted write at that seq.
            return Ok(Ok(()));
        }
        // The base seq must name an accepted write of this row.
        let history_table =
            physical_history_table_name(self.physical_table_id_for_schema(schema_version, table)?);
        let found = !self
            .database
            .primary_key_scan_raw_in_batch(
                batch,
                &history_table,
                &[
                    Value::Bytes(branch_key.canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                    Value::U64(seq.0),
                ],
            )
            .await?
            .is_empty();
        if !found {
            return Ok(Err(not_supported(format!(
                "no accepted write of the row is at base seq {}",
                seq.0
            ))));
        }
        Ok(Ok(()))
    }

    /// Whether the accepted write `tx` to a row is maybe conflicting, and
    /// its conflict analysis (SPEC 4 §4.6, "Maybe conflicting"). `None` when
    /// the node holds no accepted record of `tx` for the row.
    #[cfg_attr(not(test), allow(dead_code))] // Derived on demand; tests read it.
    pub(in crate::node) async fn write_conflict(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        tx: TxId,
    ) -> Result<Option<WriteConflict>, Error> {
        Ok(self
            .row_write_conflicts(table, branch_key, row_uuid)
            .await?
            .into_iter()
            .find_map(|(id, conflict)| (id == tx).then_some(conflict)))
    }

    /// [`Self::write_conflict`] for every accepted write of a row this node
    /// holds, in seq order: one walk of the row's history records, derived
    /// on demand and never stored.
    #[cfg_attr(not(test), allow(dead_code))] // Derived on demand; tests read it.
    pub(in crate::node) async fn row_write_conflicts(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Vec<(TxId, WriteConflict)>, Error> {
        let prefix = [
            Value::Bytes(branch_key.canonical_bytes()),
            Value::Uuid(row_uuid.0),
        ];
        // The row's accepted writes: seq, transaction, base, authored columns.
        let mut history = Vec::new();
        for history_table in self.version_storage_sources(table)? {
            let records = self
                .database
                .primary_key_scan_raw(&history_table, &prefix)
                .await?
                .into_iter()
                .map(|raw| raw.owned_record())
                .collect::<Vec<_>>();
            for record in records {
                let version = self.decode_history_owned_record("", &history_table, record)?;
                let seq = version.seq()?;
                if seq == GlobalTime(0) {
                    // A write without an accepted fate yet.
                    continue;
                }
                history.push((
                    seq,
                    self.version_tx_id(&version)?,
                    version.base()?,
                    version.authored_column_ids()?,
                ));
            }
        }
        history.sort_by_key(|(seq, ..)| *seq);
        let mut conflicts = Vec::with_capacity(history.len());
        for (index, (_, tx, base, authored)) in history.iter().enumerate() {
            let earlier = &history[..index];
            let previous = earlier.last().map(|(seq, ..)| *seq);
            if base.is_empty() {
                // An insert or a blind update.
                conflicts.push((
                    *tx,
                    WriteConflict {
                        resolved_base: None,
                        previous,
                        maybe_conflicting: false,
                        overlapping: Vec::new(),
                    },
                ));
                continue;
            }
            // A writer's writes to a row reach Core in its own order, and a
            // write Core rejected is not in history: the pending predecessor
            // resolves to its own seq when Core accepted it, and otherwise to
            // the newest accepted write of the same node before it (its own
            // resolved base).
            let predecessor = base.pending.and_then(|pending| {
                earlier
                    .iter()
                    .rev()
                    .find(|(_, id, ..)| id.node == pending.node && id.time <= pending.time)
                    .map(|(seq, ..)| *seq)
            });
            let resolved_base = base.seq.max(predecessor);
            let after_base =
                resolved_base.map_or(0, |base| earlier.partition_point(|(seq, ..)| *seq <= base));
            let overlapping = earlier[after_base..]
                .iter()
                .filter(|(.., other)| authored_overlap(authored.as_ref(), other.as_ref()))
                .map(|(_, id, ..)| *id)
                .collect();
            conflicts.push((
                *tx,
                WriteConflict {
                    resolved_base,
                    previous,
                    maybe_conflicting: resolved_base != previous,
                    overlapping,
                },
            ));
        }
        Ok(conflicts)
    }

    /// This node's newest write to the row that has no fate yet, as a
    /// writer's base names it (SPEC 4 §4.6, "Base of a write"). History keeps
    /// such writes at seq 0; foreign pending writes this node holds (as a
    /// relay) are skipped.
    pub(in crate::node) async fn newest_own_pending_write_in_batch(
        &mut self,
        batch: &DatabaseBatch,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        let Some(own_alias) = self.self_node_alias else {
            return Ok(None);
        };
        let prefix = [
            Value::Bytes(branch_key.canonical_bytes()),
            Value::Uuid(row_uuid.0),
            Value::U64(0),
        ];
        let mut newest: Option<TxTime> = None;
        for history_table in self.version_storage_sources(table)? {
            for raw in self
                .database
                .primary_key_scan_raw_in_batch(batch, &history_table, &prefix)
                .await?
            {
                let record = raw.record();
                if NodeAlias(record.get_u64(HistoryRowRecord::FIELD_TX_NODE_ID_IDX)?) != own_alias {
                    continue;
                }
                let time = TxTime(record.get_u64(HistoryRowRecord::FIELD_TX_TIME_IDX)?);
                newest = newest.max(Some(time));
            }
        }
        Ok(newest.map(|time| TxId::new(time, self.node_uuid)))
    }

    /// Every plain cell and `_deletion` of a row image, keyed by physical
    /// column id.
    pub(super) fn image_physical_cells(&self, image: &VersionRow) -> Result<PhysicalCells, Error> {
        let schema_version = self
            .schema_version_for_alias(image.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "history schema version alias must exist",
            ))?;
        let table = self.table_in_schema_ref(image.table(), schema_version)?;
        let mapping = self
            .catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| mapping.tables.get(image.table()))
            .map(|mapping| &mapping.columns)
            .ok_or(Error::InvalidStoredValue(
                "history physical table mapping missing",
            ))?;
        let record = image.record.borrowed();
        let mut cells = PhysicalCells::new();
        for (index, column) in table.columns.iter().enumerate() {
            if table.merge_strategy(&column.name) != crate::schema::MergeStrategy::Lww {
                continue;
            }
            let id = *mapping.get(&column.name).ok_or(Error::InvalidStoredValue(
                "history column physical mapping missing",
            ))?;
            let value = nullable_value(record.get_idx(HistoryRowRecord::USER_CELLS + index)?)?;
            cells.insert(id, (column.column_type.clone(), value));
        }
        let deletion = nullable_value(record.get_idx(HistoryRowRecord::FIELD__DELETION_IDX)?)?;
        cells.insert(DELETION_COLUMN_ID, (deletion_cell_type(), deletion));
        Ok(cells)
    }
}
