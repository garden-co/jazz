/// Plain cells of an image or patch keyed by physical column id: each cell's
/// column type and value (`None` is null). `_deletion` is keyed by
/// `DELETION_COLUMN_ID`.
pub(super) type PhysicalCells = BTreeMap<PhysicalColumnId, (ValueType, Option<Value>)>;

/// What the authority knows of the image a write was made over: its ancestor
/// (SPEC 4 §4.6, "Ancestor at Core").
pub(super) enum WriteAncestor {
    /// Nothing changed the row since the writer's image on any cell the
    /// write can author, or the write has no ancestor (an insert or a blind
    /// update): every authored cell applies.
    AllApply,
    /// The ancestor's cells. A cell absent here is unknown, and applies.
    Cells(PhysicalCells),
}

/// The `_deletion` cell's comparison type: the same in every layout.
fn deletion_cell_type() -> ValueType {
    ValueType::Nullable(Box::new(ValueType::Bool))
}

#[cfg(test)]
thread_local! {
    /// Test hook: resolve every based write through the full ancestor rule,
    /// skipping both fast paths, to check they agree with it.
    pub(super) static FULL_ANCESTOR_RULE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn fast_paths_enabled() -> bool {
    #[cfg(test)]
    if FULL_ANCESTOR_RULE.with(std::cell::Cell::get) {
        return false;
    }
    true
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Resolve the ancestor of a write by `writer` to a row whose current
    /// image is at `current_seq`, from the write's `base`, on the cells in
    /// `authored` (every cell when `None`). `Ok(Err(reason))` refuses the
    /// write: the authority resolves a base exactly or not at all
    /// (`INV-HIST-20`).
    ///
    /// It reads the row's accepted writes after the base seq (one range read
    /// of that row's history) when the base names a pending predecessor, and
    /// otherwise only the record at the base seq. Chained writes are read
    /// newest first and only until every authored cell is known.
    pub(super) async fn resolve_write_ancestor(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        base: RowBase,
        writer: TxId,
        current_seq: Option<GlobalTime>,
        authored: Option<&BTreeSet<PhysicalColumnId>>,
    ) -> Result<Result<WriteAncestor, String>, Error> {
        if base.is_empty() {
            return Ok(Ok(WriteAncestor::AllApply));
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
            // An unknown or still pending predecessor is parked before
            // resolution (`park_commit_unit_awaiting_predecessor`); reaching
            // either here means no fate will come.
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
        } else if base.seq == current_seq && fast_paths_enabled() {
            // Fast path: the writer saw the current image.
            return Ok(Ok(WriteAncestor::AllApply));
        }
        let history_table =
            physical_history_table_name(self.physical_table_id_for_schema(schema_version, table)?);
        let branch = Value::Bytes(branch_key.canonical_bytes());
        let row = Value::Uuid(row_uuid.0);

        // The root must be an accepted write of the row at the base seq.
        let root = match base.seq {
            None => None,
            Some(seq) => {
                let Some(record) = self
                    .database
                    .primary_key_scan_raw_in_batch(
                        batch,
                        &history_table,
                        &[branch.clone(), row.clone(), Value::U64(seq.0)],
                    )
                    .await?
                    .into_iter()
                    .next_back()
                else {
                    return Ok(Err(not_supported(format!(
                        "no accepted write of the row is at base seq {}",
                        seq.0
                    ))));
                };
                Some(record.owned_record())
            }
        };

        let mut ancestor = PhysicalCells::new();
        // Authored cells the ancestor does not hold yet; `None` is all.
        let mut open: Option<BTreeSet<PhysicalColumnId>> = authored.cloned();
        let all_known = |open: &Option<BTreeSet<PhysicalColumnId>>| {
            open.as_ref().is_some_and(BTreeSet::is_empty)
        };
        if let Some(pending) = base.pending {
            // The chain: the writer's own accepted writes to the row after
            // the root, up to its pending predecessor. Any other write after
            // the root is concurrent with this one.
            let writer_alias = self.node_aliases.get(&pending.node).copied();
            let after = base.seq.map_or(1, |seq| seq.0.saturating_add(1));
            let raws = self
                .database
                .primary_key_scan_range_raw_in_batch(
                    batch,
                    &history_table,
                    &[
                        branch.clone(),
                        row.clone(),
                        Value::U64(after),
                        Value::U64(0),
                        Value::U64(0),
                    ],
                    &[
                        branch.clone(),
                        row.clone(),
                        Value::U64(u64::MAX),
                        Value::U64(u64::MAX),
                        Value::U64(u64::MAX),
                    ],
                )
                .await?;
            let mut chain = Vec::new();
            let mut concurrent = false;
            let mut chain_lost = false;
            for raw in raws {
                let record = raw.record();
                let tx_time = TxTime(record.get_u64(HistoryRowRecord::FIELD_TX_TIME_IDX)?);
                let tx_node = NodeAlias(record.get_u64(HistoryRowRecord::FIELD_TX_NODE_ID_IDX)?);
                if Some(tx_node) == writer_alias && tx_time <= pending.time {
                    let lost = record
                        .descriptor()
                        .field_index(crate::schema::LOST_CELLS_FIELD)
                        .ok_or(Error::InvalidStoredValue("history record lacks lost_cells"))?;
                    match record.get_idx(lost)? {
                        Value::Bytes(bytes) => chain_lost |= !bytes.is_empty(),
                        _ => return Err(Error::InvalidStoredValue("history lost_cells must be bytes")),
                    }
                    chain.push(raw.owned_record());
                } else {
                    concurrent = true;
                }
            }
            if !concurrent && !chain_lost && fast_paths_enabled() {
                // Fast path: every write since the root is the writer's own
                // and none of them lost a cell, so the current image is the
                // ancestor wherever the root or a chained write holds a cell.
                // A chained write that lost a cell put its own value, not
                // the current one, into the ancestor (INV-HIST-21).
                return Ok(Ok(WriteAncestor::AllApply));
            }
            // Newest first: the last chained write to author a cell holds
            // the writer's value of it.
            for record in chain.into_iter().rev() {
                if all_known(&open) {
                    break;
                }
                let version = self
                    .decode_stored_history_record(Some(batch), "", &history_table, record)
                    .await?;
                for (id, cell) in self.own_patch_physical_cells(&version)? {
                    if ancestor.contains_key(&id) {
                        continue;
                    }
                    if let Some(open) = open.as_mut() {
                        if !open.remove(&id) {
                            continue;
                        }
                    }
                    ancestor.insert(id, cell);
                }
            }
        }
        if let Some(root) = root
            && !all_known(&open)
        {
            let root = self
                .decode_stored_history_record(Some(batch), "", &history_table, root)
                .await?;
            for (id, cell) in self.image_physical_cells(&root)? {
                if open.as_ref().is_none_or(|open| open.contains(&id)) {
                    ancestor.entry(id).or_insert(cell);
                }
            }
        }
        Ok(Ok(WriteAncestor::Cells(ancestor)))
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

    /// Every plain cell and `_deletion` of a row image.
    pub(super) fn image_physical_cells(&self, image: &VersionRow) -> Result<PhysicalCells, Error> {
        self.physical_cells(image, None, &BTreeMap::new())
    }

    /// The cells a write authored, as its writer wrote them: its history
    /// post-image restricted to its `authored_columns`, with its lost cells
    /// overriding (`INV-HIST-21`). Lost cells are stored with the record's
    /// authored enum tags; they are re-tagged to the physical registry the
    /// record's own cells use before they are compared with any image.
    pub(super) fn own_patch_physical_cells(
        &mut self,
        version: &VersionRow,
    ) -> Result<PhysicalCells, Error> {
        let authored = version.authored_column_ids()?;
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("history schema version alias must exist"))?;
        let (table, mapping) = self.version_table_and_mapping(version)?;
        let table = table.clone();
        let mapping = mapping.clone();
        let logical = super::codec::history_record_descriptor(&table);
        let column_of = |id: u64| -> Result<Option<usize>, Error> {
            if PhysicalColumnId(id) == DELETION_COLUMN_ID {
                return Ok(None);
            }
            let name = mapping
                .iter()
                .find_map(|(name, column)| (column.0 == id).then_some(name))
                .ok_or(Error::InvalidStoredValue("lost cell column id is unmapped"))?;
            table
                .columns
                .iter()
                .position(|column| &column.name == name)
                .map(Some)
                .ok_or(Error::InvalidStoredValue("lost cell column missing"))
        };
        let decoded = super::lost_cells::decode(&version.lost_cells_raw()?, |id| {
            let field = match column_of(id)? {
                None => HistoryRowRecord::FIELD__DELETION_IDX,
                Some(index) => HistoryRowRecord::USER_CELLS + index,
            };
            Ok(logical.fields()[field].value_type.clone())
        })?;
        let mut lost = BTreeMap::new();
        for (id, value) in decoded {
            let value = match column_of(id)? {
                None => value,
                Some(index) => {
                    self.authored_cell_to_physical(schema_version, &table.name, index, value)?
                }
            };
            lost.insert(PhysicalColumnId(id), nullable_value(value)?);
        }
        self.physical_cells(
            version,
            Some(authored.unwrap_or_else(|| {
                mapping
                    .values()
                    .copied()
                    .chain(std::iter::once(DELETION_COLUMN_ID))
                    .collect()
            })),
            &lost,
        )
    }

    fn version_table_and_mapping(
        &self,
        version: &VersionRow,
    ) -> Result<(&TableSchema, &BTreeMap<String, PhysicalColumnId>), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("history schema version alias must exist"))?;
        let table = self.table_in_schema_ref(version.table(), schema_version)?;
        let mapping = self
            .catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| mapping.tables.get(version.table()))
            .map(|mapping| &mapping.columns)
            .ok_or(Error::InvalidStoredValue("history physical table mapping missing"))?;
        Ok((table, mapping))
    }

    fn physical_cells(
        &self,
        version: &VersionRow,
        only: Option<BTreeSet<PhysicalColumnId>>,
        overrides: &BTreeMap<PhysicalColumnId, Option<Value>>,
    ) -> Result<PhysicalCells, Error> {
        let (table, mapping) = self.version_table_and_mapping(version)?;
        let record = version.record.borrowed();
        let wanted = |id: PhysicalColumnId| only.as_ref().is_none_or(|only| only.contains(&id));
        let mut cells = PhysicalCells::new();
        for (index, column) in table.columns.iter().enumerate() {
            if table.merge_strategy(&column.name) != crate::schema::MergeStrategy::Lww {
                continue;
            }
            let id = *mapping
                .get(&column.name)
                .ok_or(Error::InvalidStoredValue("history column physical mapping missing"))?;
            if !wanted(id) {
                continue;
            }
            let value = match overrides.get(&id) {
                Some(value) => value.clone(),
                None => nullable_value(record.get_idx(HistoryRowRecord::USER_CELLS + index)?)?,
            };
            cells.insert(id, (column.column_type.clone(), value));
        }
        if wanted(DELETION_COLUMN_ID) {
            let value = match overrides.get(&DELETION_COLUMN_ID) {
                Some(value) => value.clone(),
                None => nullable_value(record.get_idx(HistoryRowRecord::FIELD__DELETION_IDX)?)?,
            };
            cells.insert(DELETION_COLUMN_ID, (deletion_cell_type(), value));
        }
        Ok(cells)
    }
}
