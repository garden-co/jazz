//! What an accepted write was built on, and whether it maybe conflicts
//! (SPEC 4 §4.6, INV-HIST-21).
//!
//! Every uploaded write names, for each row it writes over a held image,
//! the version of the row that image was ([`BuiltOn`]): an accepted version, or
//! an unsettled commit of its sender's. A relay renames each base into
//! terms its upstream knows (its own commits), so the authority only ever
//! reads names of its own history and of its senders' commits.
//!
//! The authority resolves each base before it applies the write: an
//! accepted seq, or where the named commit landed. A write built on a
//! rejected commit, or on one it never received, is rejected too. It keeps,
//! in the version the write made (the row table and history alike), `_base`
//! (what it was built on, [`Built::stored`]) and `_cols`, the columns it
//! authored (a bitmask over the columns' positions in the row table, which
//! only ever grows by `ADD COLUMN`). From those and the row's history it
//! derives on demand whether the write overrode a version its writer never
//! saw, and which; nothing derived is stored. Text merging reads the same
//! base.
//!
//! The authority's own writes keep nothing: made over the current image,
//! they never maybe conflict, and in another write's analysis they author
//! every column.

use super::*;

/// What the authority's accepted history says about one accepted write of a
/// row. Derived on demand; nothing of it is stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteConflict {
    /// The seq the write landed at.
    pub seq: u64,
    /// The commit that wrote it, as the authority received it: (sender
    /// node, sender tx); a relayed write's is the relay's.
    pub writer: ([u8; 16], u64),
    /// The newest accepted version of the row its writer had seen with
    /// every version before it: `None` for a write with no base, and for
    /// one whose writer saw no accepted version before the first it missed.
    pub resolved_base: Option<u64>,
    /// The seq of the row's accepted version immediately before the write.
    pub previous: Option<u64>,
    /// The write has a base, and an accepted version before it is one its
    /// writer never saw.
    pub maybe_conflicting: bool,
    /// The conflict analysis: the versions before the write its writer
    /// never saw whose authored columns overlap the write's, as (seq,
    /// writer), in seq order.
    pub overlapping: Vec<(u64, ([u8; 16], u64))>,
}

/// What an accepted write was built on, at the authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Built {
    /// The row's accepted version at this seq.
    Seq(u64),
    /// The version an unsettled commit wrote, which landed at this seq: its
    /// writer saw it as it was sent, and what it was built on.
    On(u64),
}

impl Built {
    /// As `_base` keeps it: an accepted seq as is, a landed commit's seq
    /// negated (seqs start at 1, so the two never meet).
    fn stored(self) -> i64 {
        match self {
            Built::Seq(seq) => seq as i64,
            Built::On(seq) => -(seq as i64),
        }
    }

    fn from_stored(stored: i64) -> Self {
        if stored < 0 {
            Built::On(stored.unsigned_abs())
        } else {
            Built::Seq(stored as u64)
        }
    }
}

/// One accepted version of a row, as the conflict walk reads it.
struct Version {
    seq: u64,
    built: Option<Built>,
    /// Bitmask over the table's column codes; `None` authors every
    /// column, `_del` included.
    cols: Option<Vec<u8>>,
}

/// What a write's writer saw of its row: every version up to `floor`, and
/// the versions in `also`.
struct Knowledge {
    floor: u64,
    also: BTreeSet<u64>,
}

impl Knowledge {
    /// What a write built on `built` saw: an accepted version and every
    /// version before it, or an unsettled commit's version and what that
    /// was built on, down the chain.
    fn of(built: Built, versions: &[Version]) -> Self {
        let mut also = BTreeSet::new();
        let mut at = built;
        loop {
            match at {
                Built::Seq(floor) => return Self { floor, also },
                Built::On(on) => {
                    also.insert(on);
                    // A chain only runs back: a base newer than its write
                    // is not followed.
                    match versions.iter().find(|v| v.seq == on).and_then(|v| v.built) {
                        Some(next @ Built::Seq(seq) | next @ Built::On(seq)) if seq < on => {
                            at = next;
                        }
                        _ => return Self { floor: 0, also },
                    }
                }
            }
        }
    }

    fn saw(&self, seq: u64) -> bool {
        seq <= self.floor || self.also.contains(&seq)
    }
}

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

/// Whether a bitmask authors column `code`; `None` authors every column.
fn authors(cols: Option<&[u8]>, code: u32) -> bool {
    cols.is_none_or(|cols| {
        cols.get(code as usize / 8)
            .is_some_and(|byte| byte & (1 << (code % 8)) != 0)
    })
}

fn predecessor_rejected() -> Error {
    Error::with_reason(
        ErrorCode::WriteRejected,
        Reason::PredecessorRejected,
        "the write was built on a write the authority rejected",
    )
}

/// A base that names something no base may: a version outside the
/// receiver's history, an op the commit lacks, or another node's commit.
fn unknown_base(what: &str) -> Error {
    Error::new(
        ErrorCode::Protocol,
        format!("a write's base names {what} this store cannot resolve"),
    )
}

/// A commit's predecessor as the receiver recorded it: accepted (at a
/// seq), rejected, or never received, which a sender only names after its
/// predecessor was lost, so the write is rejected as over a rejected one.
fn predecessor(commit: Option<Option<u64>>) -> Result<u64, Error> {
    commit.flatten().ok_or_else(predecessor_rejected)
}

/// Which of a commit's ops write each (stored table, row), in op order.
pub(super) type RowOps = HashMap<(String, RowUuid), Vec<usize>>;

impl Inner {
    /// Which of `ops` write each row, indexed once per commit; a base
    /// names its row by the first.
    pub(super) fn row_ops(&self, ops: &[StagedOp]) -> RowOps {
        let mut by_row = RowOps::new();
        for (index, op) in ops.iter().enumerate() {
            if let (Some(row), Ok(table)) = (op.row(), self.schema.stored_table(op.table())) {
                by_row
                    .entry((table.to_owned(), row))
                    .or_default()
                    .push(index);
            }
        }
        by_row
    }

    /// The code authored-column bitmasks give stored column `name` of
    /// `table`: its position in the row table, which only ever grows by
    /// `ADD COLUMN`, so a code never moves or is reused.
    fn column_code(&self, table: &str, name: &str) -> Result<u32, Error> {
        if let Some(code) = self
            .column_codes
            .borrow()
            .get(table)
            .and_then(|t| t.get(name))
        {
            return Ok(*code);
        }
        let codes: HashMap<String, u32> = self
            .conn
            .prepare_cached(&format!("PRAGMA table_info({})", quote(table)))
            .and_then(|mut statement| {
                statement
                    .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, u32>(0)?)))?
                    .collect()
            })
            .map_err(storage)?;
        let code = codes
            .get(name)
            .copied()
            .ok_or_else(|| unknown_base("a column"))?;
        self.column_codes
            .borrow_mut()
            .insert(table.to_owned(), codes);
        Ok(code)
    }

    /// The stored columns `ops` (the ones at `indexes`) author on stored
    /// table `table`'s row, as a bitmask over their codes; `None` when one
    /// of them writes a full image (an insert or a move).
    fn authored(
        &self,
        ops: &[StagedOp],
        indexes: &[usize],
        table: &str,
    ) -> Result<Option<Vec<u8>>, Error> {
        let mut authored = BTreeSet::new();
        for op in indexes.iter().map(|&index| &ops[index]) {
            let schema_table = self.schema.table(op.table())?;
            let stored = |cells: &BTreeMap<String, Value>| {
                cells
                    .keys()
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

    /// A session's bases, renamed into this relay's terms for its upstream
    /// (SPEC 4 §4.6): an accepted upstream seq stays; a commit of this
    /// relay's (one of its own unsettled versions it shipped the session, or
    /// the commit carrying one of the session's) is named as this relay's.
    /// A base on a commit this relay rejected, or never received, rejects
    /// the write. A version in this relay's local range that is not one of
    /// its own commits (an upstream relay's, or one from before a reset)
    /// cannot be named upstream: the write counts as built on the newest
    /// version of the row this relay holds accepted, so it may be flagged
    /// over a version its writer saw, never the other way round.
    pub(super) fn relay_bases(
        &self,
        sender: ([u8; 16], u64),
        ops: &[StagedOp],
        bases: Vec<WireBase>,
    ) -> Result<Vec<WireBase>, Error> {
        let me = *self.identity.node.as_bytes();
        let pending = &self.client.as_ref().expect("a relay is a client").pending;
        // Where a commit this relay carries landed: still in its own local
        // range (named as its commit), or already accepted upstream.
        let carried = |seq: u64| {
            if seq >= LOCAL_BASE {
                BuiltOn::Commit { node: me, tx: seq }
            } else {
                BuiltOn::Seq(seq)
            }
        };
        bases
            .into_iter()
            .map(|base| {
                let on = match base.on {
                    BuiltOn::Seq(seq) if seq < LOCAL_BASE => BuiltOn::Seq(seq),
                    BuiltOn::Seq(seq) if seq < NON_DURABLE_BASE => {
                        let (table, row) = self.based_row(ops, &base)?;
                        let row_key = (table.to_owned(), row, base.key.clone());
                        let own_pending = pending
                            .by_row
                            .get(&row_key)
                            .is_some_and(|txs| txs.contains(&seq));
                        match self.settled(me, seq)? {
                            _ if own_pending => BuiltOn::Commit { node: me, tx: seq },
                            Some(Some(_)) => BuiltOn::Commit { node: me, tx: seq },
                            Some(None) => return Err(predecessor_rejected()),
                            None => BuiltOn::Seq(
                                self.newest_accepted(table, row, &base.key)?.unwrap_or(0),
                            ),
                        }
                    }
                    BuiltOn::Seq(_) => return Err(unknown_base("a version")),
                    BuiltOn::Own { back } => {
                        let tx = sender
                            .1
                            .checked_sub(back)
                            .ok_or_else(|| unknown_base("a commit"))?;
                        carried(predecessor(self.settled(sender.0, tx)?)?)
                    }
                    BuiltOn::Commit { node, tx } if node == sender.0 => {
                        carried(predecessor(self.settled(node, tx)?)?)
                    }
                    BuiltOn::Commit { .. } => return Err(unknown_base("another node's commit")),
                };
                Ok(WireBase { on, ..base })
            })
            .collect()
    }

    /// The stored table and row a base names, by its op.
    fn based_row(&self, ops: &[StagedOp], base: &WireBase) -> Result<(&str, RowUuid), Error> {
        let op = ops
            .get(base.op as usize)
            .ok_or_else(|| unknown_base("an op"))?;
        match (op.row(), self.schema.stored_table(op.table())) {
            (Some(row), Ok(table)) => Ok((table, row)),
            _ => Err(unknown_base("an op")),
        }
    }

    /// The newest version of a row this store holds accepted upstream.
    fn newest_accepted(&self, table: &str, row: RowUuid, key: &[u8]) -> Result<Option<u64>, Error> {
        Ok(self
            .accepted_versions(table, row, key)?
            .iter()
            .rev()
            .map(|v| v.seq)
            .find(|&seq| seq < LOCAL_BASE))
    }

    /// What each based row of an upload was built on, in this authority's
    /// history, keyed by the row: an accepted seq, or where the commit it
    /// names landed. A base on a commit rejected or never received rejects
    /// the upload.
    pub(super) fn resolve_bases(
        &self,
        sender: ([u8; 16], u64),
        ops: &[StagedOp],
        bases: &[WireBase],
    ) -> Result<HashMap<RowKey, Built>, Error> {
        let mut built = HashMap::new();
        for base in bases {
            let (table, row) = self.based_row(ops, base)?;
            let on = match base.on {
                BuiltOn::Seq(seq) if seq < LOCAL_BASE => Built::Seq(seq),
                BuiltOn::Seq(_) => return Err(unknown_base("a version")),
                BuiltOn::Own { back } => {
                    let tx = sender
                        .1
                        .checked_sub(back)
                        .ok_or_else(|| unknown_base("a commit"))?;
                    Built::On(predecessor(self.settled(sender.0, tx)?)?)
                }
                BuiltOn::Commit { node, tx } if node == sender.0 => {
                    Built::On(predecessor(self.settled(node, tx)?)?)
                }
                BuiltOn::Commit { .. } => return Err(unknown_base("another node's commit")),
            };
            built.insert((table.to_owned(), row, base.key.clone()), on);
        }
        Ok(built)
    }

    /// Keep, in each row version an accepted upload wrote at `seq`, what it
    /// was built on and the columns it authored.
    pub(super) fn record_bases(
        &self,
        ops: &[StagedOp],
        by_row: &RowOps,
        built: &HashMap<RowKey, Built>,
        seq: u64,
        rows: &[RowKey],
    ) -> Result<(), Error> {
        for row_key @ (table, row, key) in rows {
            let indexes = by_row
                .get(&(table.clone(), *row))
                .map_or(&[][..], Vec::as_slice);
            self.exec(
                &format!(
                    "UPDATE {} SET _base = ?1, _cols = ?2 WHERE id = ?3 AND _b = ?4 AND _seq = ?5",
                    quote(table)
                ),
                rusqlite::params![
                    built.get(row_key).map(|built| built.stored()),
                    self.authored(ops, indexes, table)?,
                    row.0.as_bytes().to_vec(),
                    key,
                    seq as i64
                ],
            )
            .map_err(storage)?;
        }
        Ok(())
    }

    /// The accepted versions of a row, in seq order.
    fn accepted_versions(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
    ) -> Result<Vec<Version>, Error> {
        let sql = format!(
            "SELECT _seq, _base, _cols FROM {} WHERE id = ?1 AND _b = ?2 \
             UNION ALL SELECT _seq, _base, _cols FROM {} WHERE id = ?1 AND _b = ?2 \
             ORDER BY 1",
            quote(table),
            quote(&format!("{table}__h"))
        );
        let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
        let versions = statement
            .query_map(rusqlite::params![row.0.as_bytes().to_vec(), key], |r| {
                Ok(Version {
                    seq: r.get::<_, i64>(0)? as u64,
                    built: r.get::<_, Option<i64>>(1)?.map(Built::from_stored),
                    cols: r.get(2)?,
                })
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        Ok(versions)
    }

    /// Whether each accepted write of a row maybe conflicts, and its
    /// conflict analysis (SPEC 4 §4.6, "Maybe conflicting"), in seq order:
    /// derived on demand and never stored.
    #[cfg_attr(not(feature = "testing"), allow(dead_code))] // Derived on demand; tests read it.
    pub(super) fn row_write_conflicts(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
    ) -> Result<Vec<WriteConflict>, Error> {
        let history = self.accepted_versions(table, row, key)?;
        let writers: BTreeMap<u64, ([u8; 16], u64)> = self
            .origins(&history.iter().map(|v| v.seq).collect())?
            .into_iter()
            .map(|(seq, node, tx)| (seq, (node, tx)))
            .collect();
        let writer = |seq: u64| writers.get(&seq).copied().unwrap_or(([0; 16], seq));
        let mut conflicts = Vec::with_capacity(history.len());
        for (index, version) in history.iter().enumerate() {
            let earlier = &history[..index];
            let previous = earlier.last().map(|v| v.seq);
            let Some(built) = version.built else {
                // An insert, a blind write, or the authority's own.
                conflicts.push(WriteConflict {
                    seq: version.seq,
                    writer: writer(version.seq),
                    resolved_base: None,
                    previous,
                    maybe_conflicting: false,
                    overlapping: Vec::new(),
                });
                continue;
            };
            let knew = Knowledge::of(built, &history);
            let seen = earlier.iter().take_while(|v| knew.saw(v.seq)).count();
            let missed: Vec<&Version> = earlier.iter().filter(|v| !knew.saw(v.seq)).collect();
            conflicts.push(WriteConflict {
                seq: version.seq,
                writer: writer(version.seq),
                resolved_base: seen.checked_sub(1).map(|last| earlier[last].seq),
                previous,
                maybe_conflicting: !missed.is_empty(),
                overlapping: missed
                    .iter()
                    .filter(|v| authored_overlap(version.cols.as_deref(), v.cols.as_deref()))
                    .map(|v| (v.seq, writer(v.seq)))
                    .collect(),
            });
        }
        Ok(conflicts)
    }

    /// The value stored column `column` of a row had in the version a write
    /// landing after `before` was built on, when merging the write needs it
    /// (SPEC 4 §4.6): a version before `before` the writer never saw
    /// authored the column. `None` takes the fast path: nothing to merge
    /// with, so the write's value stands as sent. Only `_base` and `_cols`
    /// are read to decide; the value is read only when a merge is needed.
    ///
    /// The value is the newest one the writer's chain gave the column: an
    /// unsettled link that authored it (its version as accepted, with the
    /// link's seq, so the caller can take it back to the value as sent),
    /// or else the version the chain's floor names. A link that left the
    /// column alone holds whatever the authority held, which its writer
    /// never saw, so the walk passes it by.
    pub(crate) fn merge_base(
        &self,
        (table, row, key): (&str, RowUuid, &[u8]),
        built: Built,
        before: u64,
        column: &str,
    ) -> Result<Option<(Sql, Option<u64>)>, Error> {
        let history = self.accepted_versions(table, row, key)?;
        let knew = Knowledge::of(built, &history);
        let code = self.column_code(table, column)?;
        let missed = history
            .iter()
            .take_while(|v| v.seq < before)
            .any(|v| !knew.saw(v.seq) && authors(v.cols.as_deref(), code));
        if !missed {
            return Ok(None);
        }
        let mut link = built;
        let (at, sent) = loop {
            match link {
                Built::Seq(floor) => {
                    break (
                        history.iter().rev().find(|v| v.seq <= floor).map(|v| v.seq),
                        None,
                    );
                }
                Built::On(on) => {
                    let version = history.iter().find(|v| v.seq == on);
                    if version.is_none_or(|v| authors(v.cols.as_deref(), code)) {
                        break (Some(on), Some(on));
                    }
                    // As in [`Knowledge::of`], a chain only runs back.
                    match version.and_then(|v| v.built) {
                        Some(next @ (Built::Seq(seq) | Built::On(seq))) if seq < on => link = next,
                        _ => break (None, None),
                    }
                }
            }
        };
        let Some(at) = at else {
            return Ok(None);
        };
        let sql = format!(
            "SELECT {column} FROM {} WHERE id = ?1 AND _b = ?2 AND _seq = ?3 \
             UNION ALL SELECT {column} FROM {} WHERE id = ?1 AND _b = ?2 AND _seq = ?3",
            quote(table),
            quote(&format!("{table}__h")),
            column = quote(column),
        );
        let cell = self
            .conn
            .cached_row(
                &sql,
                rusqlite::params![row.0.as_bytes().to_vec(), key, at as i64],
                |r| r.get::<_, Sql>(0),
            )
            .optional()
            .map_err(storage)?;
        Ok(cell.map(|cell| (cell, sent)))
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

    /// The value column `column` of `table`'s unbranched row `row` had in
    /// the version the accepted write at `seq` was built on, when merging
    /// that write needed it (SPEC 4 §4.6); `None` when it took the fast path
    /// or kept no base.
    #[cfg(feature = "testing")]
    pub fn merge_base(
        &self,
        table: &str,
        row: RowUuid,
        seq: u64,
        column: &str,
    ) -> Result<Option<Value>, Error> {
        let inner = self.inner();
        let schema_table = inner.schema.table(table)?;
        let stored = schema_table.stored.clone();
        let stored_column = schema_table.stored_name(column).to_owned();
        let built = inner
            .accepted_versions(&stored, row, &[])?
            .into_iter()
            .find(|v| v.seq == seq)
            .and_then(|v| v.built);
        let Some(built) = built else {
            return Ok(None);
        };
        let Some((cell, _)) = inner.merge_base((&stored, row, &[]), built, seq, &stored_column)?
        else {
            return Ok(None);
        };
        let ty = &schema_table
            .column(column)
            .ok_or_else(|| unknown_base("a column"))?
            .ty;
        crate::values::from_sql(ty, cell.as_ref(), &inner.chunks)
    }

    /// How many bytes column `column` of `table`'s unbranched row `row`
    /// stores inline now: a chunked value keeps only its descriptor.
    #[cfg(feature = "testing")]
    pub fn stored_bytes_for_test(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
    ) -> Result<u64, Error> {
        let inner = self.inner();
        let schema_table = inner.schema.table(table)?;
        let sql = format!(
            "SELECT length(CAST({} AS BLOB)) FROM {} WHERE id = ?1 AND _b = ?2",
            quote(schema_table.stored_name(column)),
            quote(&schema_table.stored),
        );
        inner
            .conn
            .cached_row(
                &sql,
                rusqlite::params![row.0.as_bytes().to_vec(), Vec::<u8>::new()],
                |r| r.get::<_, i64>(0).map(|n| n as u64),
            )
            .map_err(storage)
    }
}
