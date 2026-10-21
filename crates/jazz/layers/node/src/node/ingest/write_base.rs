/// What a node's accepted history says about one accepted write (SPEC 4
/// §4.6, "Maybe conflicting"). Derived on demand; nothing of it is stored.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::node) struct WriteConflict {
    /// The write's resolved base seq: the newest accepted record of the row
    /// its writer had seen. That is its seen seq (the larger of its own base
    /// seq and its chain root's settled base seq), moved forward over the
    /// writer's own chain records that directly follow it. `None` for a
    /// write without a base, and for a chain with no settled image under it
    /// whose first accepted record is not the writer's own.
    pub(in crate::node) resolved_base: Option<GlobalTime>,
    /// The seq of the row's accepted record immediately before the write.
    pub(in crate::node) previous: Option<GlobalTime>,
    /// The write has a base, and another writer's accepted record lies
    /// between its seen seq and the write; equivalently, `resolved_base`
    /// is not `previous`.
    pub(in crate::node) maybe_conflicting: bool,
    /// The conflict analysis: the other writers' accepted writes after the
    /// seen seq and before the write whose authored columns overlap the
    /// write's, in seq order. The writer's own chain records never appear.
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

/// Why Core cannot chain a write over the pending predecessor its base names,
/// or `None` when it can: the predecessor is an older transaction of the
/// writer's own node (`INV-HIST-20`).
///
/// This is the only place cross-writer chains are refused. Nothing stored or
/// sent encodes the rule (a pending base is a full `TxId`), so a peer remote
/// whose writes chain over another node's pending write needs only this
/// check relaxed, together with what SPEC 4 §4.7 lists for peers. The
/// conflict walk's counterpart is [`in_writers_chain`].
pub(super) fn pending_predecessor_refusal(
    predecessor: TxId,
    writer: TxId,
) -> Option<&'static str> {
    if predecessor.node != writer.node {
        return Some("its pending predecessor was written by another node");
    }
    if predecessor.time >= writer.time {
        return Some("its pending predecessor is not older than the write");
    }
    None
}

/// Whether the accepted write `record` belongs to the chain a write's
/// pending predecessor `pending` names: the predecessor's node's accepted
/// writes no newer than it (`INV-HIST-21`). Such records never count against
/// the write.
///
/// Together with [`pending_predecessor_refusal`] this is the only place the
/// conflict analysis assumes a chain is one writer's own. A peer chain (the
/// chain the writer saw, of any author) would replace this one predicate.
fn in_writers_chain(pending: TxId, record: TxId) -> bool {
    record.node == pending.node && record.time <= pending.time
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
            if let Some(refusal) = pending_predecessor_refusal(pending, writer) {
                return Ok(Err(not_supported(refusal.to_owned())));
            }
            // The predecessor's fate is not looked up. A writer's writes
            // reach Core in their authored order, so a predecessor Core has
            // not decided by now never arrives: like one Core rejected, it
            // is not in history, and the write applies as written over the
            // row's current image (`INV-HIST-20`).
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

    /// Why Core refuses `writer`'s write to a row as arriving after a later
    /// write of its own node that Core already accepted there, or `None`.
    /// A writer's writes reach Core in their authored order (SPEC 14), so
    /// such a write is a protocol violation; applying it would put an older
    /// image over a newer one (`INV-HIST-23`).
    ///
    /// Its reads are bounded by the writes accepted since the writer's base,
    /// never by the row's whole history:
    ///
    /// - The row's current image names its newest write. When that write is
    ///   the writer's own, or is the write's base, nothing more is read.
    /// - A write with a base seq `B` under a current seq `C`: the history
    ///   key is `(branch, row, seq, tx_time, node)`, so one range read per
    ///   history table covers exactly the records at seqs `B+1..=C`, and only
    ///   their key fields are read. A later write of the writer's node lies
    ///   there if anywhere, since the writer had seen `B` when it wrote.
    /// - A write without a base seq: one point read of the writer's
    ///   superseded-writes marker for the row (see
    ///   [`Self::passed_over_predecessor`]).
    #[allow(clippy::too_many_arguments)]
    pub(in crate::node) async fn late_write_refusal(
        &mut self,
        batch: &DatabaseBatch,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        base: RowBase,
        writer: TxId,
        current_seq: Option<GlobalTime>,
        current_writer: Option<TxId>,
    ) -> Result<Option<String>, Error> {
        let late = || {
            format!(
                "the write to '{table}' row {} arrived after a later write of its writer; a writer's writes must reach the authority in the order they were written",
                row_uuid.0
            )
        };
        let Some(current_seq) = current_seq else {
            // No accepted write of the row.
            return Ok(None);
        };
        if let Some(current_writer) = current_writer
            && current_writer.node == writer.node
        {
            // The writer's newest accepted write of the row is the current
            // image's: its writes before that were accepted in order.
            return Ok((current_writer.time > writer.time).then(late));
        }
        let is_late = match base.seq {
            // Nothing was accepted after the image the writer had seen.
            Some(seq) if seq == current_seq => false,
            // A base above the row's current seq is refused by
            // `validate_write_base`.
            Some(seq) if seq > current_seq => false,
            Some(seq) => {
                self.own_write_accepted_between(
                    batch,
                    table,
                    branch_key,
                    row_uuid,
                    writer,
                    seq,
                    current_seq,
                )
                .await?
            }
            None => self
                .superseded_through(
                    self.physical_table_id_for_schema(schema_version, table)?,
                    branch_key,
                    row_uuid,
                    writer.node,
                )
                .await?
                .is_some_and(|through| writer.time <= through),
        };
        Ok(is_late.then(late))
    }

    /// Whether the row holds an accepted record of `writer`'s node newer
    /// than `writer` at a seq in `after+1..=through`. One range read per
    /// history table of the row's table; only key fields are read.
    #[allow(clippy::too_many_arguments)]
    async fn own_write_accepted_between(
        &mut self,
        batch: &DatabaseBatch,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        writer: TxId,
        after: GlobalTime,
        through: GlobalTime,
    ) -> Result<bool, Error> {
        let bound = |seq: u64| {
            [
                Value::Bytes(branch_key.canonical_bytes()),
                Value::Uuid(row_uuid.0),
                Value::U64(seq),
                Value::U64(0),
                Value::U64(0),
            ]
        };
        let (start, end) = (
            bound(after.0.saturating_add(1)),
            bound(through.0.saturating_add(1)),
        );
        for history_table in self.version_storage_sources(table)? {
            for raw in self
                .database
                .primary_key_scan_range_raw_in_batch(batch, &history_table, &start, &end)
                .await?
            {
                let record = raw.record();
                let time = TxTime(record.get_u64(HistoryRowRecord::FIELD_TX_TIME_IDX)?);
                if time <= writer.time {
                    continue;
                }
                let alias = NodeAlias(record.get_u64(HistoryRowRecord::FIELD_TX_NODE_ID_IDX)?);
                if self.node_for_alias(alias) == Some(writer.node) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// The superseded-writes marker key of `node` for a row. It names the
    /// row's table by its physical id, which a table rename keeps, so a
    /// marker recorded under one schema version guards writes made under
    /// another.
    fn superseded_writes_key(
        table: PhysicalTableId,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        node: NodeUuid,
    ) -> [Value; 4] {
        [
            Value::U64(table.0),
            Value::Bytes(branch_key.canonical_bytes()),
            Value::Uuid(row_uuid.0),
            Value::Uuid(node.0),
        ]
    }

    /// The newest transaction time of `node`'s writes to the row that Core
    /// passed over (see [`Self::passed_over_predecessor`]): one point read.
    async fn superseded_through(
        &self,
        table: PhysicalTableId,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        node: NodeUuid,
    ) -> Result<Option<TxTime>, Error> {
        let store = self.database.direct_record_store(SUPERSEDED_WRITES_STORE)?;
        let Some(record) = store
            .get(&Self::superseded_writes_key(table, branch_key, row_uuid, node))
            .await?
        else {
            return Ok(None);
        };
        match record.get_idx(0)? {
            Value::U64(time) => Ok(Some(TxTime(time))),
            _ => Err(Error::InvalidStoredValue(
                "superseded-writes marker is not a transaction time",
            )),
        }
    }

    /// The writes of `writer`'s node to a row that Core passes over by
    /// applying `writer`'s write: through its pending predecessor's time,
    /// when Core holds no decision for that predecessor (it never arrived,
    /// or is stored Pending). `None` when the write passes nothing over.
    ///
    /// A writer's writes reach Core in the order they were written
    /// (`INV-HIST-23`), so once a write chained on `P` is decided, none of
    /// its node's writes at or before `P` arrives for a decision any more;
    /// Core records the marker before it applies the write, and also when it
    /// refuses one (see [`Self::record_superseded_by_refusal`]). It is what
    /// makes the late check of a write without a base seq one point read:
    /// when such a write `Y` arrives after Core accepted a later write of its
    /// node to the row, the first of those, `Z`, was written while `Y` was
    /// still pending at its writer, so `Z` is chained on `Y` or on a later
    /// write `X` that Core had not accepted either. If Core held no decision
    /// for `X` (or `X` is `Y`), applying `Z` recorded a marker at or after
    /// `Y`. If Core had refused `X`, the same holds one link down the chain:
    /// refusing `X` recorded a marker at or after `Y` unless `X`'s own
    /// predecessor had been refused before it, and so on down to the write
    /// chained directly on `Y`, which Core decided while it held no
    /// decision for `Y`. A predecessor Core already decided (accepted or
    /// refused) therefore writes nothing: a redelivery of it is deduplicated
    /// before this check, and the writes before it are covered by the
    /// marker written when it was decided, if any was needed. Writing
    /// happens only when a write passes over a predecessor Core never saw,
    /// which takes a transport fault or a parked predecessor; the ordinary
    /// chain writes nothing and reads at most the predecessor's transaction.
    pub(super) async fn passed_over_predecessor(
        &mut self,
        base: RowBase,
        writer: TxId,
        current_writer: Option<TxId>,
    ) -> Result<Option<TxTime>, Error> {
        let Some(pending) = base.pending else {
            return Ok(None);
        };
        if current_writer
            .is_some_and(|current| current.node == writer.node && current.time >= pending.time)
        {
            // The row's image is the predecessor's, or a newer write of its
            // node Core accepted after it.
            return Ok(None);
        }
        self.undecided_predecessor(pending, writer).await
    }

    /// `pending`'s time when it is a well-formed predecessor of `writer`
    /// that Core holds no decision for: not stored, or stored Pending.
    async fn undecided_predecessor(
        &mut self,
        pending: TxId,
        writer: TxId,
    ) -> Result<Option<TxTime>, Error> {
        if pending_predecessor_refusal(pending, writer).is_some() {
            return Ok(None);
        }
        let undecided = self
            .query_transaction(pending)
            .await?
            .is_none_or(|stored| matches!(stored.fate, Fate::Pending));
        Ok(undecided.then_some(pending.time))
    }

    /// Record the marker a refused write `writer` leaves for each row it
    /// addressed whose pending predecessor Core holds no decision for: a
    /// later write chained on `writer` passes over the refused write without
    /// a marker (see [`Self::passed_over_predecessor`]), so the refusal
    /// records the writes it passed over itself. A version whose table this
    /// node cannot resolve leaves none.
    pub(super) async fn record_superseded_by_refusal(
        &mut self,
        writer: TxId,
        predecessors: Vec<(SchemaVersionId, String, BranchKey, RowUuid, TxId)>,
    ) -> Result<(), Error> {
        for (schema_version, table, branch_key, row_uuid, pending) in predecessors {
            let Ok(physical_table) = self.physical_table_id_for_schema(schema_version, &table)
            else {
                continue;
            };
            if let Some(through) = self.undecided_predecessor(pending, writer).await? {
                self.record_superseded_writes(
                    physical_table,
                    &branch_key,
                    row_uuid,
                    writer.node,
                    through,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Record that Core passed over `node`'s writes to a row through
    /// `through` (see [`Self::passed_over_predecessor`]). Markers only grow.
    pub(super) async fn record_superseded_writes(
        &mut self,
        table: PhysicalTableId,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        node: NodeUuid,
        through: TxTime,
    ) -> Result<(), Error> {
        if self
            .superseded_through(table, branch_key, row_uuid, node)
            .await?
            .is_some_and(|recorded| recorded >= through)
        {
            return Ok(());
        }
        self.database
            .direct_record_store(SUPERSEDED_WRITES_STORE)?
            .set(
                &Self::superseded_writes_key(table, branch_key, row_uuid, node),
                &[Value::U64(through.0)],
            )
            .await?;
        Ok(())
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
        // Per record, the seq its writer had seen everything up to: the
        // larger of its own base seq (the settled image its node held) and
        // its chain root's settled base seq. `None` for an empty base.
        let mut seen: Vec<Option<GlobalTime>> = Vec::with_capacity(history.len());
        let mut conflicts = Vec::with_capacity(history.len());
        for (index, (_, tx, base, authored)) in history.iter().enumerate() {
            let earlier = &history[..index];
            let previous = earlier.last().map(|(seq, ..)| *seq);
            if base.is_empty() {
                // An insert or a blind update.
                seen.push(None);
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
            // is the newest accepted write of the same node no newer than it
            // (itself when Core accepted it, otherwise the rejected
            // predecessor's own predecessor). The writer saw the settled
            // image its chain rests on, never a predecessor's later seq: a
            // write another writer got accepted before the predecessor
            // landed was not in that image.
            let predecessor = base.pending.and_then(|pending| {
                earlier
                    .iter()
                    .rposition(|(_, id, ..)| in_writers_chain(pending, *id))
            });
            let seen_seq = base.seq.max(predecessor.and_then(|at| seen[at]));
            seen.push(seen_seq);
            // The writer's own chain: its accepted writes no newer than the
            // pending predecessor. They never count against the write.
            let own_chain = |id: &TxId| {
                base.pending
                    .is_some_and(|pending| in_writers_chain(pending, *id))
            };
            let after_seen =
                seen_seq.map_or(0, |seen| earlier.partition_point(|(seq, ..)| *seq <= seen));
            let walk = &earlier[after_seen..];
            let first_foreign = walk.iter().position(|(_, id, ..)| !own_chain(id));
            let resolved_base = match first_foreign.unwrap_or(walk.len()) {
                0 => seen_seq,
                own => Some(walk[own - 1].0),
            };
            let overlapping = walk
                .iter()
                .filter(|(_, id, _, other)| {
                    !own_chain(id) && authored_overlap(authored.as_ref(), other.as_ref())
                })
                .map(|(_, id, ..)| *id)
                .collect();
            conflicts.push((
                *tx,
                WriteConflict {
                    resolved_base,
                    previous,
                    maybe_conflicting: first_foreign.is_some(),
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
}
