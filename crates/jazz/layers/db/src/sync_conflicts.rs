//! Whether an accepted write maybe conflicts (SPEC 4 §4.6, INV-HIST-21).
//!
//! Every uploaded write names the image its writer made it over (its base:
//! the settled image the writer held and its own newest unsettled write to
//! the row). The authority keeps, beside each row version an upload wrote,
//! that base, the writer's (node, tx) and the columns the write authored
//! (`_jazz_writes`). From those and the row's accepted history it derives on
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
    /// seen seq, moved forward over the writer's own chain versions that
    /// directly follow it. `None` for a write without a base, and for a
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
    base_pending: Option<([u8; 16], u64)>,
    /// Stored column names; `None` authors every column, `_del` included.
    authored: Option<BTreeSet<String>>,
}

impl Version {
    fn has_base(&self) -> bool {
        self.base_seq.is_some() || self.base_pending.is_some()
    }
}

/// What the authority keeps of each row version an accepted upload wrote.
const WRITES_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_writes (t TEXT NOT NULL, \
     id BLOB NOT NULL, k BLOB NOT NULL, seq INTEGER NOT NULL, node BLOB NOT NULL, \
     tx INTEGER NOT NULL, base_seq INTEGER, pending_node BLOB, pending_tx INTEGER, \
     authored BLOB, \
     PRIMARY KEY (t, id, k, seq)) WITHOUT ROWID;";

/// The authored-column name a deletion or restore writes.
const DELETION: &str = "_del";

/// Whether two writes' authored columns overlap; `None` authors every
/// column, `_del` included.
fn authored_overlap(left: Option<&BTreeSet<String>>, right: Option<&BTreeSet<String>>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => !left.is_disjoint(right),
        (Some(columns), None) | (None, Some(columns)) => !columns.is_empty(),
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
    /// The name `ops` give stored table `table` for row `row`: a commit
    /// names its tables as the schema it was staged under does.
    pub(super) fn op_table_name(
        &self,
        ops: &[StagedOp],
        table: &str,
        row: RowUuid,
    ) -> Option<String> {
        ops.iter()
            .find(|op| {
                op.row() == Some(row)
                    && self
                        .schema
                        .stored_table(op.table())
                        .is_ok_and(|stored| stored == table)
            })
            .map(|op| op.table().to_owned())
    }

    /// The stored columns `ops` author on stored table `table`'s row `row`;
    /// `None` when one of them writes a full image (an insert or a move).
    fn authored(
        &self,
        ops: &[StagedOp],
        table: &str,
        row: RowUuid,
    ) -> Result<Option<BTreeSet<String>>, Error> {
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
        Ok(Some(authored))
    }

    /// Keep, beside each row version an accepted upload wrote at `seq`, its
    /// writer, its base and the columns it authored (`_jazz_writes`).
    pub(super) fn record_writes(
        &self,
        ops: &[StagedOp],
        bases: &[WireBase],
        writer: ([u8; 16], u64),
        seq: u64,
        rows: &[(String, RowUuid, Vec<u8>)],
    ) -> Result<(), Error> {
        if !self.writes_table.get() {
            self.conn.execute_batch(WRITES_DDL).map_err(storage)?;
        }
        for (table, row, key) in rows {
            let base = bases.iter().find(|base| {
                base.id == *row.0.as_bytes()
                    && base.key == *key
                    && self
                        .schema
                        .stored_table(&base.table)
                        .is_ok_and(|stored| stored == table)
            });
            let authored = self
                .authored(ops, table, *row)?
                .map(|columns| encode(&columns.into_iter().collect::<Vec<_>>()));
            self.exec(
                "INSERT OR REPLACE INTO _jazz_writes \
                 (t, id, k, seq, node, tx, base_seq, pending_node, pending_tx, authored) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                rusqlite::params![
                    table,
                    row.0.as_bytes().to_vec(),
                    key,
                    seq as i64,
                    writer.0.to_vec(),
                    writer.1 as i64,
                    base.and_then(|base| base.seq).map(|seq| seq as i64),
                    base.and_then(|base| base.pending)
                        .map(|(node, _)| node.to_vec()),
                    base.and_then(|base| base.pending).map(|(_, tx)| tx as i64),
                    authored,
                ],
            )
            .map_err(storage)?;
        }
        // Set only once they went in: this is an accepted upload's last
        // step, so its savepoint, which holds the table, is then kept.
        self.writes_table.set(true);
        Ok(())
    }

    /// A session's bases as this relay uploads them. A session behind a
    /// relay may have been shipped the relay's unsettled version of a row:
    /// what it saw of the row's accepted history is then the settled
    /// version that version was made over, never that version's later
    /// accepted seq (another writer's write accepted in between was not in
    /// it). Its pending predecessor already names the session's own
    /// commit, and the upload names the session as its origin.
    pub(super) fn relayed_bases(&self, bases: Vec<WireBase>) -> Result<Vec<WireBase>, Error> {
        let me = *self.identity.node.as_bytes();
        let mut out = Vec::with_capacity(bases.len());
        for mut base in bases {
            if let Some(seen) = base.seq
                && seen >= LOCAL_BASE
            {
                let table = self.schema.stored_table(&base.table)?.to_owned();
                // Where the relay's version landed upstream, once it has.
                let below = match self.settled(me, seen)? {
                    Some(Some(accepted)) => accepted.min(LOCAL_BASE),
                    _ => LOCAL_BASE,
                };
                base.seq = self.newest_version_below(&table, &base.id, &base.key, below)?;
            }
            out.push(base);
        }
        Ok(out)
    }

    /// The newest version of a row below `below`, current or superseded.
    fn newest_version_below(
        &self,
        table: &str,
        id: &[u8; 16],
        key: &[u8],
        below: u64,
    ) -> Result<Option<u64>, Error> {
        let sql = format!(
            "SELECT MAX(s) FROM (SELECT _seq AS s FROM {} WHERE id = ?1 AND _b = ?2 AND _seq < ?3 \
             UNION ALL SELECT _seq FROM {} WHERE id = ?1 AND _b = ?2 AND _seq < ?3)",
            quote(table),
            quote(&format!("{table}__h"))
        );
        self.conn
            .cached_row(
                &sql,
                rusqlite::params![id.to_vec(), key, below as i64],
                |r| r.get::<_, Option<i64>>(0),
            )
            .map(|seq| seq.map(|seq| seq as u64))
            .map_err(storage)
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
             NULL AS base_seq, NULL AS pending_node, NULL AS pending_tx, NULL AS authored)"
        };
        let sql = format!(
            "SELECT v.s, w.node, w.tx, w.base_seq, w.pending_node, w.pending_tx, w.authored \
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
                    Ok((
                        r.get::<_, i64>(0)? as u64,
                        r.get::<_, Option<Vec<u8>>>(1)?,
                        r.get::<_, Option<i64>>(2)?,
                        r.get::<_, Option<i64>>(3)?,
                        r.get::<_, Option<Vec<u8>>>(4)?,
                        r.get::<_, Option<i64>>(5)?,
                        r.get::<_, Option<Vec<u8>>>(6)?,
                    ))
                },
            )
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        drop(statement);
        // Versions no upload wrote: who committed them.
        let unrecorded: BTreeSet<u64> = rows
            .iter()
            .filter(|(_, node, ..)| node.is_none())
            .map(|(seq, ..)| *seq)
            .collect();
        let origins: BTreeMap<u64, ([u8; 16], u64)> = self
            .origins(&unrecorded)?
            .into_iter()
            .map(|(seq, node, tx)| (seq, (node, tx)))
            .collect();
        let corrupt = || Error::new(ErrorCode::Storage, "a recorded write is malformed");
        rows.into_iter()
            .map(
                |(seq, node, tx, base_seq, pending_node, pending_tx, authored)| {
                    let Some(node) = node else {
                        return Ok(Version {
                            seq,
                            writer: origins[&seq],
                            base_seq: None,
                            base_pending: None,
                            authored: None,
                        });
                    };
                    let authored = authored
                        .map(|bytes| postcard::from_bytes::<Vec<String>>(&bytes))
                        .transpose()
                        .map_err(|_| corrupt())?
                        .map(|columns| columns.into_iter().collect());
                    Ok(Version {
                        seq,
                        writer: (
                            node.try_into().map_err(|_| corrupt())?,
                            tx.ok_or_else(corrupt)? as u64,
                        ),
                        base_seq: base_seq.map(|seq| seq as u64),
                        base_pending: match (pending_node, pending_tx) {
                            (Some(node), Some(tx)) => {
                                Some((node.try_into().map_err(|_| corrupt())?, tx as u64))
                            }
                            _ => None,
                        },
                        authored,
                    })
                },
            )
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
            let seen_seq = version.base_seq.max(predecessor.and_then(|at| seen[at]));
            seen.push(seen_seq);
            let after_seen = seen_seq.map_or(0, |seen| earlier.partition_point(|v| v.seq <= seen));
            let walk = &earlier[after_seen..];
            let first_foreign = walk.iter().position(|v| !own_chain(v.writer));
            let resolved_base = match first_foreign.unwrap_or(walk.len()) {
                0 => seen_seq,
                own => Some(walk[own - 1].seq),
            };
            let overlapping = walk
                .iter()
                .filter(|v| {
                    !own_chain(v.writer)
                        && authored_overlap(version.authored.as_ref(), v.authored.as_ref())
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
    /// Whether each accepted write of `table`'s row `row` (in branch `key`,
    /// `&[]` unbranched) maybe conflicts, and its conflict analysis, in seq
    /// order (SPEC 4 §4.6, INV-HIST-21). Derived from this store's accepted
    /// history on demand; meaningful on the authority.
    #[cfg(feature = "testing")]
    pub fn write_conflicts(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
    ) -> Result<Vec<WriteConflict>, Error> {
        let inner = self.inner();
        let stored = inner.schema.stored_table(table)?.to_owned();
        inner.row_write_conflicts(&stored, row, key)
    }
}
