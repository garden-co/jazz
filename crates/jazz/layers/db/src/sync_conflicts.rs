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
//! rejected commit is rejected too. It keeps, in the version the write
//! made (the row table and history alike), `_base` (the accepted seq it was
//! built on) or `_on` (the seq the commit it was built on landed at), and
//! `_cols`, the columns it authored (a bitmask over per-table codes that
//! `_jazz_columns` interns). From those and the row's history it derives on
//! demand whether the write overrode a version its writer never saw, and
//! which; nothing derived is stored. Text merging reads the same base.
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

/// The per-table codes authored-column bitmasks use, assigned in order of
/// first use and never reused.
const COLUMNS_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_columns (t TEXT NOT NULL, \
     name TEXT NOT NULL, code INTEGER NOT NULL, PRIMARY KEY (t, name)) WITHOUT ROWID;";

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

/// A relay's base it cannot rename: named nothing it received.
fn unknown_base(what: &str) -> Error {
    Error::new(
        ErrorCode::Protocol,
        format!("a write's base names {what} this store never received"),
    )
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
        // Created in the caller's transaction on first use; known to exist
        // once that commits ([`Self::note_columns_table`]).
        if !self.columns_table.get() {
            self.conn.execute_batch(COLUMNS_DDL).map_err(storage)?;
        }
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

    /// Remember, once an upload's savepoint is released, whether
    /// `_jazz_columns` now exists.
    pub(super) fn note_columns_table(&self) -> Result<(), Error> {
        if !self.columns_table.get() {
            let exists = self
                .conn
                .cached_row(
                    "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = '_jazz_columns'",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .map_err(storage)?;
            self.columns_table.set(exists > 0);
        }
        Ok(())
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

    /// A session's bases, renamed into this relay's terms for its upstream:
    /// an accepted upstream seq stays; this relay's own unsettled version
    /// (one it shipped the session) and a commit of the session's (or of a
    /// relay below it) become the commit of this relay's that carries it.
    /// A base on a commit this relay rejected rejects the write.
    pub(super) fn relay_bases(
        &self,
        sender: ([u8; 16], u64),
        bases: Vec<WireBase>,
    ) -> Result<Vec<WireBase>, Error> {
        let me = *self.identity.node.as_bytes();
        let carried = |commit: Option<Option<u64>>| match commit {
            Some(Some(seq)) if seq >= LOCAL_BASE => Ok(BuiltOn::Commit { node: me, tx: seq }),
            Some(Some(_)) => Err(unsupported(
                "a base on a session's commit an authority without an upstream accepted",
            )),
            Some(None) => Err(predecessor_rejected()),
            None => Err(unknown_base("a commit")),
        };
        bases
            .into_iter()
            .map(|base| {
                let on = match base.on {
                    BuiltOn::Seq(seq) if seq < LOCAL_BASE => BuiltOn::Seq(seq),
                    BuiltOn::Seq(seq) if seq < NON_DURABLE_BASE => {
                        BuiltOn::Commit { node: me, tx: seq }
                    }
                    BuiltOn::Seq(_) => return Err(unknown_base("a version")),
                    BuiltOn::Own { back } => {
                        let tx = sender
                            .1
                            .checked_sub(back)
                            .ok_or_else(|| unknown_base("a commit"))?;
                        carried(self.settled(sender.0, tx)?)?
                    }
                    BuiltOn::Commit { node, tx } => carried(self.settled(node, tx)?)?,
                };
                Ok(WireBase { on, ..base })
            })
            .collect()
    }

    /// What each based row of an upload was built on, in this authority's
    /// history, keyed by the row: an accepted seq, or where the commit it
    /// names landed. A base on a rejected commit rejects the upload.
    pub(super) fn resolve_bases(
        &self,
        sender: ([u8; 16], u64),
        ops: &[StagedOp],
        bases: &[WireBase],
    ) -> Result<HashMap<RowKey, Built>, Error> {
        let landed = |commit: Option<Option<u64>>| match commit {
            Some(Some(seq)) => Ok(Built::On(seq)),
            Some(None) => Err(predecessor_rejected()),
            None => Err(unknown_base("a commit")),
        };
        let mut built = HashMap::new();
        for base in bases {
            let Some(op) = ops.get(base.op as usize) else {
                return Err(unknown_base("an op"));
            };
            let (Some(row), Ok(table)) = (op.row(), self.schema.stored_table(op.table())) else {
                return Err(unknown_base("an op"));
            };
            let on = match base.on {
                BuiltOn::Seq(seq) if seq < LOCAL_BASE => Built::Seq(seq),
                BuiltOn::Seq(_) => return Err(unknown_base("a version")),
                BuiltOn::Own { back } => {
                    let tx = sender
                        .1
                        .checked_sub(back)
                        .ok_or_else(|| unknown_base("a commit"))?;
                    landed(self.settled(sender.0, tx)?)?
                }
                BuiltOn::Commit { node, tx } => landed(self.settled(node, tx)?)?,
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
        built: &HashMap<RowKey, Built>,
        seq: u64,
        rows: &[RowKey],
    ) -> Result<(), Error> {
        for row_key @ (table, row, key) in rows {
            let (base, on) = match built.get(row_key) {
                Some(Built::Seq(seq)) => (Some(*seq as i64), None),
                Some(Built::On(seq)) => (None, Some(*seq as i64)),
                None => (None, None),
            };
            self.exec(
                &format!(
                    "UPDATE {} SET _base = ?1, _on = ?2, _cols = ?3 \
                     WHERE id = ?4 AND _b = ?5 AND _seq = ?6",
                    quote(table)
                ),
                rusqlite::params![
                    base,
                    on,
                    self.authored(ops, table, *row)?,
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
            "SELECT _seq, _base, _on, _cols FROM {} WHERE id = ?1 AND _b = ?2 \
             UNION ALL SELECT _seq, _base, _on, _cols FROM {} WHERE id = ?1 AND _b = ?2 \
             ORDER BY 1",
            quote(table),
            quote(&format!("{table}__h"))
        );
        let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
        let versions = statement
            .query_map(rusqlite::params![row.0.as_bytes().to_vec(), key], |r| {
                let base = r
                    .get::<_, Option<i64>>(1)?
                    .map(|seq| Built::Seq(seq as u64));
                let on = r.get::<_, Option<i64>>(2)?.map(|seq| Built::On(seq as u64));
                Ok(Version {
                    seq: r.get::<_, i64>(0)? as u64,
                    built: base.or(on),
                    cols: r.get(3)?,
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
    /// with, so the write's value stands as sent. A base on an unsettled
    /// commit gives that commit's version as accepted.
    #[cfg_attr(not(feature = "testing"), allow(dead_code))] // Text merging reads it.
    pub(crate) fn merge_base(
        &self,
        (table, row, key): (&str, RowUuid, &[u8]),
        built: Built,
        before: u64,
        column: &str,
    ) -> Result<Option<rusqlite::types::Value>, Error> {
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
        let at = match built {
            Built::Seq(seq) => history.iter().rev().find(|v| v.seq <= seq).map(|v| v.seq),
            Built::On(seq) => Some(seq),
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
        self.conn
            .cached_row(
                &sql,
                rusqlite::params![row.0.as_bytes().to_vec(), key, at as i64],
                |r| r.get::<_, rusqlite::types::Value>(0),
            )
            .optional()
            .map_err(storage)
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
        let Some(cell) = inner.merge_base((&stored, row, &[]), built, seq, &stored_column)? else {
            return Ok(None);
        };
        let ty = &schema_table
            .column(column)
            .ok_or_else(|| unknown_base("a column"))?
            .ty;
        crate::values::from_sql(ty, (&cell).into(), &inner.chunks)
    }
}
