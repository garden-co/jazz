//! Whether an accepted write maybe conflicts (SPEC 4 §4.6, INV-HIST-21).
//!
//! Every uploaded write names the image its writer made it over (its base:
//! the settled image the writer held and its own newest unsettled write to
//! the row). The authority keeps, beside each row version an upload wrote,
//! that base, the writer's (node, tx) and the columns the write authored
//! (`_jazz_writes`, as a bitmask over per-table column codes that
//! `_jazz_columns` interns). From those and the row's accepted history it derives on
//! demand whether the write overrode values its writer never saw, and which
//! intervening writes it overlapped. Nothing derived is stored.
//!
//! The authority's own writes are not recorded: it writes over the row's
//! current image, so they never maybe conflict, and in another write's
//! analysis they author every column (a full image).

use super::*;

/// What the authority's accepted history says about one accepted write of a
/// row. Derived on demand; nothing of it is stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteConflict {
    /// The seq the write landed at.
    pub seq: u64,
    /// Its writer's (node, tx): the session commit's, through a relay.
    pub writer: ([u8; 16], u64),
    /// The newest accepted version of the row its writer had seen: its
    /// seen seq, moved forward over the versions it also saw that directly
    /// follow it (its own chain's, and a relayed version it was shipped). `None` for a write without a base, and for a
    /// chain with no settled image under it whose first accepted version is
    /// not the writer's own.
    pub resolved_base: Option<u64>,
    /// The seq of the row's accepted version immediately before the write.
    pub previous: Option<u64>,
    /// The write has a base, and another writer's accepted version lies
    /// between its seen seq and the write; equivalently, `resolved_base` is
    /// not `previous`.
    pub maybe_conflicting: bool,
    /// The conflict analysis: the other writers' accepted writes after the
    /// seen seq and before the write whose authored columns overlap the
    /// write's, as (seq, writer), in seq order. The writer's own chain never
    /// appears.
    pub overlapping: Vec<(u64, ([u8; 16], u64))>,
}

/// One accepted version of a row, as the conflict walk reads it.
struct Version {
    seq: u64,
    writer: ([u8; 16], u64),
    base_seq: Option<u64>,
    /// `base_seq` is where a relay's unsettled version the writer was
    /// shipped landed: the writer saw that version and what its write was
    /// made over.
    base_via: bool,
    base_pending: Option<([u8; 16], u64)>,
    /// Bitmask over the table's column codes; `None` authors every
    /// column, `_del` included.
    authored: Option<Vec<u8>>,
}

/// One row of the accepted-versions read: a version and, when an upload
/// wrote it, what `_jazz_writes` kept of it.
struct Recorded {
    seq: u64,
    node: Option<Vec<u8>>,
    tx: Option<i64>,
    base_seq: Option<i64>,
    base_via: bool,
    pending_node: Option<Vec<u8>>,
    pending_tx: Option<i64>,
    authored: Option<Vec<u8>>,
}

impl Version {
    fn has_base(&self) -> bool {
        self.base_seq.is_some() || self.base_pending.is_some()
    }
}

/// What the authority keeps of each row version an accepted upload wrote,
/// and the per-table codes its authored-column bitmasks use (assigned in
/// order of first use, never reused).
const WRITES_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_writes (t TEXT NOT NULL, \
     id BLOB NOT NULL, k BLOB NOT NULL, seq INTEGER NOT NULL, node BLOB NOT NULL, \
     tx INTEGER NOT NULL, base_seq INTEGER, base_via INTEGER, pending_node BLOB, \
     pending_tx INTEGER, \
     authored BLOB, \
     PRIMARY KEY (t, id, k, seq)) WITHOUT ROWID; \
     CREATE TABLE IF NOT EXISTS _jazz_columns (t TEXT NOT NULL, name TEXT NOT NULL, \
     code INTEGER NOT NULL, PRIMARY KEY (t, name)) WITHOUT ROWID;";

/// The authored-column name a deletion or restore writes.
const DELETION: &str = "_del";

/// Whether two writes' authored-column bitmasks overlap; `None` authors
/// every column, `_del` included.
fn authored_overlap(left: Option<&[u8]>, right: Option<&[u8]>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.iter().zip(right).any(|(l, r)| l & r != 0),
        (Some(columns), None) | (None, Some(columns)) => columns.iter().any(|&b| b != 0),
        (None, None) => true,
    }
}

/// Whether accepted version `record` belongs to the chain a write's pending
/// predecessor `pending` names: the predecessor's node's accepted writes no
/// newer than it (INV-HIST-21). Such versions never count against the write.
/// The only place the analysis assumes a chain is one writer's own
/// (INV-REF-4): a peer chain would replace this one predicate.
fn in_writers_chain(pending: ([u8; 16], u64), record: ([u8; 16], u64)) -> bool {
    record.0 == pending.0 && record.1 <= pending.1
}

impl Inner {
    /// The index of the first of `ops` that writes stored table `table`'s
    /// row `row`: how a base names its row.
    pub(super) fn op_writing(&self, ops: &[StagedOp], table: &str, row: RowUuid) -> Option<u32> {
        ops.iter()
            .position(|op| {
                op.row() == Some(row)
                    && self
                        .schema
                        .stored_table(op.table())
                        .is_ok_and(|stored| stored == table)
            })
            .map(|index| index as u32)
    }

    /// The code `_jazz_columns` gives stored column `name` of `table`,
    /// assigned on first use.
    fn column_code(&self, table: &str, name: &str) -> Result<u32, Error> {
        let known = self
            .conn
            .cached_row(
                "SELECT code FROM _jazz_columns WHERE t = ?1 AND name = ?2",
                rusqlite::params![table, name],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage)?;
        if let Some(code) = known {
            return Ok(code as u32);
        }
        self.exec(
            "INSERT INTO _jazz_columns (t, name, code) \
             SELECT ?1, ?2, count(*) FROM _jazz_columns WHERE t = ?1",
            rusqlite::params![table, name],
        )
        .map_err(storage)?;
        self.column_code(table, name)
    }

    /// The stored columns `ops` author on stored table `table`'s row `row`,
    /// as a bitmask over their codes; `None` when one of them writes a full
    /// image (an insert or a move).
    fn authored(
        &self,
        ops: &[StagedOp],
        table: &str,
        row: RowUuid,
    ) -> Result<Option<Vec<u8>>, Error> {
        let mut authored = BTreeSet::new();
        for op in ops {
            if op.row() != Some(row) {
                continue;
            }
            let schema_table = self.schema.table(op.table())?;
            if schema_table.stored != table {
                continue;
            }
            let stored = |cells: &BTreeMap<String, Value>| {
                cells
                    .keys()
                    // A text write's base edits are not a column.
                    .filter(|name| crate::text_merge::based_column(name).is_none())
                    .map(|name| schema_table.stored_name(name).to_owned())
                    .collect::<Vec<_>>()
            };
            match op {
                StagedOp::Insert(..) | StagedOp::Move(..) => return Ok(None),
                StagedOp::Update(_, _, cells, _) | StagedOp::Upsert(_, _, cells, _) => {
                    authored.extend(stored(cells));
                }
                StagedOp::Delete(..) => {
                    authored.insert(DELETION.to_owned());
                }
                StagedOp::Restore(_, _, cells, _) => {
                    authored.insert(DELETION.to_owned());
                    if let Some(cells) = cells {
                        authored.extend(stored(cells));
                    }
                }
            }
        }
        let mut mask = Vec::new();
        for name in &authored {
            let code = self.column_code(table, name)? as usize;
            if mask.len() <= code / 8 {
                mask.resize(code / 8 + 1, 0);
            }
            mask[code / 8] |= 1 << (code % 8);
        }
        Ok(Some(mask))
    }

    /// Keep, beside each row version an accepted upload wrote at `seq`, its
    /// writer, its base and the columns it authored (`_jazz_writes`).
    pub(super) fn record_writes(
        &self,
        ops: &[StagedOp],
        bases: &[WireBase],
        (sender, writer): ([u8; 16], ([u8; 16], u64)),
        seq: u64,
        rows: &[(String, RowUuid, Vec<u8>)],
    ) -> Result<(), Error> {
        if !self.writes_table.get() {
            self.conn.execute_batch(WRITES_DDL).map_err(storage)?;
        }
        for (table, row, key) in rows {
            let op = self.op_writing(ops, table, *row);
            let base = bases
                .iter()
                .find(|base| Some(base.op) == op && base.key == *key);
            let (base_seq, via) = match base.and_then(|base| base.seq) {
                Some(seen) => self.resolve_seen(sender, seen)?,
                None => (None, false),
            };
            let pending = base
                .and_then(|base| base.pending)
                .and_then(|pending| pending.resolve(writer));
            let authored = self.authored(ops, table, *row)?;
            self.exec(
                "INSERT OR REPLACE INTO _jazz_writes \
                 (t, id, k, seq, node, tx, base_seq, base_via, pending_node, pending_tx, authored) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![
                    table,
                    row.0.as_bytes().to_vec(),
                    key,
                    seq as i64,
                    writer.0.to_vec(),
                    writer.1 as i64,
                    base_seq.map(|seq| seq as i64),
                    via.then_some(1),
                    // The writer's own node is not repeated.
                    pending
                        .filter(|(node, _)| *node != writer.0)
                        .map(|(node, _)| node.to_vec()),
                    pending.map(|(_, tx)| tx as i64),
                    authored,
                ],
            )
            .map_err(storage)?;
        }
        Ok(())
    }

    /// A base's seen seq in this store's history, and whether it names a
    /// relayed version. A seq below [`LOCAL_BASE`] is one this store
    /// accepted. Above it, it is the sender's unsettled version at that
    /// commit tx, which the sender (a relay) shipped its session: the
    /// writer saw that version and what its commit was made over, so it
    /// resolves to where that commit landed, flagged. A sender's commit that
    /// never landed leaves the writer having seen no accepted version (seq
    /// 0), so every earlier version counts against it.
    fn resolve_seen(&self, sender: [u8; 16], seen: u64) -> Result<(Option<u64>, bool), Error> {
        if seen < LOCAL_BASE {
            return Ok((Some(seen), false));
        }
        Ok(match self.settled(sender, seen)? {
            Some(Some(landed)) => (Some(landed), true),
            _ => (Some(0), false),
        })
    }

    /// The accepted versions of a row, in seq order, with what
    /// `_jazz_writes` kept of each.
    fn accepted_versions(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
    ) -> Result<Vec<Version>, Error> {
        if !self.writes_table.get() {
            let exists = self
                .conn
                .cached_row(
                    "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = '_jazz_writes'",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .map_err(storage)?
                > 0;
            self.writes_table.set(exists);
        }
        let recorded = if self.writes_table.get() {
            "_jazz_writes"
        } else {
            // Nothing was recorded: every version reads as unrecorded.
            "(SELECT NULL AS t, NULL AS id, NULL AS k, NULL AS seq, NULL AS node, NULL AS tx, \
             NULL AS base_seq, NULL AS base_via, NULL AS pending_node, NULL AS pending_tx, \
             NULL AS authored)"
        };
        let sql = format!(
            "SELECT v.s, w.node, w.tx, w.base_seq, w.base_via, w.pending_node, w.pending_tx, \
             w.authored \
             FROM (SELECT _seq AS s FROM {} WHERE id = ?1 AND _b = ?2 \
             UNION SELECT _seq FROM {} WHERE id = ?1 AND _b = ?2) AS v \
             LEFT JOIN {recorded} AS w ON w.t = ?3 AND w.id = ?1 AND w.k = ?2 AND w.seq = v.s \
             ORDER BY v.s",
            quote(table),
            quote(&format!("{table}__h"))
        );
        let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
        let rows = statement
            .query_map(
                rusqlite::params![row.0.as_bytes().to_vec(), key, table],
                |r| {
                    Ok(Recorded {
                        seq: r.get::<_, i64>(0)? as u64,
                        node: r.get(1)?,
                        tx: r.get(2)?,
                        base_seq: r.get(3)?,
                        base_via: r.get::<_, Option<i64>>(4)?.is_some(),
                        pending_node: r.get(5)?,
                        pending_tx: r.get(6)?,
                        authored: r.get(7)?,
                    })
                },
            )
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        drop(statement);
        // Versions no upload wrote: who committed them.
        let unrecorded: BTreeSet<u64> = rows
            .iter()
            .filter(|recorded| recorded.node.is_none())
            .map(|recorded| recorded.seq)
            .collect();
        let origins: BTreeMap<u64, ([u8; 16], u64)> = self
            .origins(&unrecorded)?
            .into_iter()
            .map(|(seq, node, tx)| (seq, (node, tx)))
            .collect();
        let corrupt = || Error::new(ErrorCode::Storage, "a recorded write is malformed");
        rows.into_iter()
            .map(|recorded| {
                let Some(node) = recorded.node else {
                    return Ok(Version {
                        seq: recorded.seq,
                        writer: origins[&recorded.seq],
                        base_seq: None,
                        base_via: false,
                        base_pending: None,
                        authored: None,
                    });
                };
                let node: [u8; 16] = node.try_into().map_err(|_| corrupt())?;
                let tx = recorded.tx.ok_or_else(corrupt)? as u64;
                Ok(Version {
                    seq: recorded.seq,
                    writer: (node, tx),
                    base_seq: recorded.base_seq.map(|seq| seq as u64),
                    base_via: recorded.base_via,
                    // No pending node is the writer's own.
                    base_pending: match (recorded.pending_node, recorded.pending_tx) {
                        (Some(node), Some(tx)) => {
                            Some((node.try_into().map_err(|_| corrupt())?, tx as u64))
                        }
                        (None, Some(tx)) => Some((node, tx as u64)),
                        _ => None,
                    },
                    authored: recorded.authored,
                })
            })
            .collect()
    }

    /// Whether each accepted write of a row maybe conflicts, and its
    /// conflict analysis (SPEC 4 §4.6, "Maybe conflicting"), in seq order:
    /// one walk of the row's accepted versions, derived on demand and never
    /// stored.
    #[cfg_attr(not(feature = "testing"), allow(dead_code))] // Derived on demand; tests read it.
    pub(super) fn row_write_conflicts(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
    ) -> Result<Vec<WriteConflict>, Error> {
        let history = self.accepted_versions(table, row, key)?;
        // Per version, the seq its writer had seen everything up to: the
        // larger of its own base seq (the settled image its node held) and
        // its chain root's settled base seq. `None` for an empty base.
        let mut seen: Vec<Option<u64>> = Vec::with_capacity(history.len());
        let mut conflicts = Vec::with_capacity(history.len());
        for (index, version) in history.iter().enumerate() {
            let earlier = &history[..index];
            let previous = earlier.last().map(|v| v.seq);
            if !version.has_base() {
                // An insert or a blind write.
                seen.push(None);
                conflicts.push(WriteConflict {
                    seq: version.seq,
                    writer: version.writer,
                    resolved_base: None,
                    previous,
                    maybe_conflicting: false,
                    overlapping: Vec::new(),
                });
                continue;
            }
            // The writer's own chain: its accepted writes no newer than its
            // pending predecessor. They never count against the write.
            let own_chain = |record: ([u8; 16], u64)| {
                version
                    .base_pending
                    .is_some_and(|pending| in_writers_chain(pending, record))
            };
            // A writer's writes reach the authority in its own order, and a
            // rejected write is not in history: the pending predecessor is
            // the newest accepted write of the chain (itself when accepted,
            // otherwise the rejected one's own predecessor). The writer saw
            // the settled image its chain rests on, never a predecessor's
            // later seq: a write another writer got accepted before the
            // predecessor landed was not in that image.
            let predecessor = earlier.iter().rposition(|v| own_chain(v.writer));
            // A relayed version the writer was shipped: it saw that version
            // and what that version's writer had seen, nothing between.
            let (base_seen, shipped) = if version.base_via {
                let at = version
                    .base_seq
                    .and_then(|landed| earlier.iter().position(|v| v.seq == landed));
                (at.and_then(|at| seen[at]), at.map(|at| earlier[at].seq))
            } else {
                (version.base_seq, None)
            };
            let seen_seq = base_seen.max(predecessor.and_then(|at| seen[at]));
            seen.push(seen_seq);
            // Versions after the seen seq the writer still saw.
            let saw = |v: &Version| own_chain(v.writer) || Some(v.seq) == shipped;
            let after_seen = seen_seq.map_or(0, |seen| earlier.partition_point(|v| v.seq <= seen));
            let walk = &earlier[after_seen..];
            let first_foreign = walk.iter().position(|v| !saw(v));
            let resolved_base = match first_foreign.unwrap_or(walk.len()) {
                // Seq 0: the writer saw no accepted version.
                0 => seen_seq.filter(|&seen| seen > 0),
                own => Some(walk[own - 1].seq),
            };
            let overlapping = walk
                .iter()
                .filter(|v| {
                    !saw(v) && authored_overlap(version.authored.as_deref(), v.authored.as_deref())
                })
                .map(|v| (v.seq, v.writer))
                .collect();
            conflicts.push(WriteConflict {
                seq: version.seq,
                writer: version.writer,
                resolved_base,
                previous,
                maybe_conflicting: first_foreign.is_some(),
                overlapping,
            });
        }
        Ok(conflicts)
    }
}

impl Db {
    /// Whether each accepted write of `table`'s row `row` (on `branch`,
    /// `None` unbranched) maybe conflicts, and its conflict analysis, in seq
    /// order (SPEC 4 §4.6, INV-HIST-21). Derived from this store's accepted
    /// history on demand; meaningful on the authority.
    #[cfg(feature = "testing")]
    pub fn write_conflicts(
        &self,
        table: &str,
        row: RowUuid,
        branch: Option<&BranchSelector>,
    ) -> Result<Vec<WriteConflict>, Error> {
        let inner = self.inner();
        let stored = inner.schema.stored_table(table)?.to_owned();
        let key = branch.map_or_else(Vec::new, |branch| crate::values::branch_key(&branch.values));
        inner.row_write_conflicts(&stored, row, &key)
    }
}
