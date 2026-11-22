//! Sealing writes with the client's encryption hooks ([`crate::e2ee`])
//! before anything stores, chunks or uploads them.

use std::collections::BTreeMap;

use super::large_ops::{StreamedSeal, sealed_elsewhere};
use super::{
    BatchKind, Db, Inner, InsertOptions, LargeEdits, Located, StagedOp, Target, TxRef,
    UpdateOptions, WriteHandle, WriteTarget, column_from_sql, quote, rejected, storage,
};
use crate::db::Pos;
use crate::e2ee::{
    E2eeHooks, E2eeRefusal, SealError, StreamSealer, StreamTarget, WriteCells, WriteKind,
};
use crate::ids::RowUuid;
use crate::options::{Error, ErrorCode};
use crate::records::Value;
use crate::tools::OpenTransactionId;
use crate::values::{CachedRow, Sql};

impl Db {
    /// Counts retained staging state that a row read cannot observe.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn staged_checks_and_chunks_for_test(&self, id: OpenTransactionId) -> (usize, usize) {
        let inner = self.inner.staging();
        let batch = &inner.batches[&id];
        (
            batch.first_checks.len(),
            batch.edits.derived.len() + batch.edits.pending_nodes.len(),
        )
    }

    /// Stage `op` in batch `id` sealed by the encryption hooks, as `kind`
    /// when given. The internal rows the hooks stage with it go first; if any of them or
    /// the write fails, none stays staged.
    pub(crate) fn stage_sealed(
        &self,
        id: OpenTransactionId,
        mut op: StagedOp,
        edits: LargeEdits,
        kind: Option<WriteKind>,
        streamed: Option<StreamedSeal<'_>>,
    ) -> Result<(), Error> {
        let companions =
            self.in_overlay(id, None, |inner, _| inner.seal_op(&mut op, kind, streamed))?;
        if companions.is_empty() {
            return self.stage_chunked(id, op, edits);
        }
        let names: Vec<(String, RowUuid)> = companions
            .iter()
            .filter_map(|companion| match companion {
                StagedOp::Insert(table, _, opts) => opts.row_id.map(|row| (table.clone(), row)),
                _ => None,
            })
            .collect();
        let (prefix, rows, inserted, claims, new_check_keys) = {
            let inner = self.inner.staging();
            let batch = inner
                .batches
                .get(&id)
                .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?;
            // Only absent entries can be added by these inserts. An unknown
            // table fails staging before it can record a check.
            let new_check_keys: Vec<_> = names
                .iter()
                .filter_map(|(table, row)| {
                    let stored = inner.schema.stored_table(table).ok()?;
                    let key = (stored.to_owned(), *row.0.as_bytes());
                    (!batch.first_checks.contains_key(&key)).then_some(key)
                })
                .collect();
            (
                batch.prefix(),
                batch.rows.clone(),
                batch.inserted.clone(),
                batch.claims.clone(),
                new_check_keys,
            )
        };
        let staged = companions
            .into_iter()
            .try_for_each(|companion| self.stage_chunked(id, companion, LargeEdits::default()))
            .and_then(|()| self.stage_chunked(id, op, edits));
        let hooks = {
            let mut inner = self.inner.staging();
            if staged.is_err() {
                // The overlay may hold the companions: the next stage replays
                // what stays staged.
                inner.settle_overlay();
                if let Some(batch) = inner.batches.get_mut(&id) {
                    batch.ops.truncate(prefix.ops);
                    batch.edits.derived.truncate(prefix.derived);
                    batch.edits.pending_nodes.truncate(prefix.pending_nodes);
                    for key in new_check_keys {
                        batch.first_checks.remove(&key);
                    }
                    batch.rows = rows;
                    batch.inserted = inserted;
                    batch.claims = claims;
                }
                inner.e2ee.clone()
            } else {
                let hooks = inner.e2ee.clone();
                if let Some(batch) = inner.batches.get_mut(&id) {
                    batch.companions.stage(hooks, names.iter().cloned());
                }
                None
            }
        };
        // Told once nothing borrows the store: the hooks never call it.
        if let Some(hooks) = hooks {
            hooks.companions_dropped(&names);
        }
        staged
    }
}

/// The companion rows the sealed writes of one open transaction staged:
/// unless the transaction commits them, the hooks are told they were
/// dropped ([`E2eeHooks::companions_dropped`]) when it goes.
#[derive(Default)]
pub(crate) struct StagedCompanions {
    hooks: Option<std::rc::Rc<dyn E2eeHooks>>,
    rows: Vec<(String, RowUuid)>,
    committed: std::cell::Cell<bool>,
}

impl StagedCompanions {
    fn stage(
        &mut self,
        hooks: Option<std::rc::Rc<dyn E2eeHooks>>,
        rows: impl IntoIterator<Item = (String, RowUuid)>,
    ) {
        self.hooks = self.hooks.take().or(hooks);
        self.rows.extend(rows);
    }

    /// The transaction committed its rows: nothing was dropped.
    pub(crate) fn committed(&self) {
        self.committed.set(true);
    }
}

impl Drop for StagedCompanions {
    fn drop(&mut self) {
        if self.committed.get() || self.rows.is_empty() {
            return;
        }
        if let Some(hooks) = &self.hooks {
            hooks.companions_dropped(&self.rows);
        }
    }
}

impl TxRef<'_> {
    /// Move `row` of encrypted `table` to the space `target_space`
    /// (migrations §4.4): one update that sets the row's space column and
    /// seals every encrypted column again under the target space. `cells`
    /// holds the plaintext of every encrypted column; the hooks refuse a
    /// partial move. Only a serializable transaction moves a row.
    pub async fn move_to_space(
        &self,
        table: &str,
        row: RowUuid,
        mut cells: BTreeMap<String, Value>,
        target_space: Value,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        let space = {
            let inner = self.db.inner.staging();
            let serializable = matches!(
                inner.batches.get(&self.id).map(|batch| &batch.kind),
                Some(BatchKind::Serializable { .. })
            );
            if !serializable {
                return Err(Error::new(
                    ErrorCode::Query,
                    "a row moves between spaces only in a serializable transaction",
                ));
            }
            let entry = inner
                .e2ee
                .as_ref()
                .and_then(|hooks| hooks.declarations().tables.get(table).cloned())
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::Query,
                        format!("`{table}` is not an encrypted table"),
                    )
                })?;
            if entry.space == "id" {
                return Err(Error::new(
                    ErrorCode::Query,
                    format!("a row of `{table}` is its own space and cannot move"),
                ));
            }
            entry.space
        };
        cells.insert(space, target_space.clone());
        let op = StagedOp::Update(table.to_owned(), row, cells, opts);
        self.db
            .fetching_with(op, |op| {
                self.db.stage_sealed(
                    self.id,
                    op,
                    LargeEdits::default(),
                    Some(WriteKind::Move {
                        target_space: target_space.clone(),
                    }),
                    None,
                )
            })
            .await?;
        Ok(WriteHandle {
            row,
            seq: Pos::default(),
            tx: 0,
        })
    }
}

impl Inner {
    /// Seal `op`, a write this database authors, before anything stores or
    /// chunks it. Returns the internal rows the write needs, as inserts to
    /// apply before it in the same transaction. `kind` overrides the kind
    /// `op` implies (a move between spaces). `streamed` names the one column
    /// of this write whose value was sealed as it streamed: it is stored as
    /// it is. A branch move carries sealed cells as they are.
    pub(crate) fn seal_op(
        &self,
        op: &mut StagedOp,
        kind: Option<WriteKind>,
        streamed: Option<StreamedSeal<'_>>,
    ) -> Result<Vec<StagedOp>, Error> {
        let Some(hooks) = self.e2ee.clone() else {
            return Ok(Vec::new());
        };
        let identity = op.identity();
        let mut none = BTreeMap::new();
        let (table, row, implied, cells, target) = match op {
            StagedOp::Insert(table, cells, opts) => {
                let row = opts.row_id.ok_or_else(|| {
                    Error::new(
                        ErrorCode::Protocol,
                        "an insert is sealed once its row id is drawn",
                    )
                })?;
                (table.clone(), row, WriteKind::Insert, cells, None)
            }
            StagedOp::Update(table, row, cells, opts) => (
                table.clone(),
                *row,
                WriteKind::Update,
                cells,
                Some((Target::of(&opts.target, self)?, false)),
            ),
            StagedOp::Upsert(table, row, cells, _, opts) => (
                table.clone(),
                *row,
                WriteKind::Upsert,
                cells,
                Some((Target::of(&opts.target, self)?, false)),
            ),
            StagedOp::Delete(table, row, _) => {
                (table.clone(), *row, WriteKind::Delete, &mut none, None)
            }
            // A restore finds the row deleted: its space is the one the row
            // had when it was deleted.
            StagedOp::Restore(table, row, content, opts) => (
                table.clone(),
                *row,
                WriteKind::Restore,
                content.as_mut().unwrap_or(&mut none),
                Some((Target::exact(&opts.target), true)),
            ),
            StagedOp::Move(..) => return Ok(Vec::new()),
        };
        let kind = kind.unwrap_or(implied);
        let stored_space = match target {
            Some((target, deleted)) => self.stored_space(&*hooks, &table, row, &target, deleted)?,
            None => None,
        };
        // A value sealed as it streamed is stored as it is, and only in the
        // row and space it was sealed for, as this write finds them: its
        // transaction's own writes included.
        if let Some(seal) = streamed {
            let space = self
                .named_space(&table, row, cells)
                .or_else(|| stored_space.clone());
            if seal.row != row || space.as_ref() != seal.space {
                return Err(sealed_elsewhere());
            }
        }
        let streamed = streamed.and_then(|seal| cells.remove_entry(seal.column));
        let mut companions = Vec::new();
        let sealed = hooks.seal_write(&mut WriteCells {
            table: &table,
            row,
            kind,
            cells,
            streamed_column: streamed.as_ref().map(|(name, _)| name.as_str()),
            stored_space: stored_space.as_ref(),
            companion_rows: &mut companions,
        });
        if let Some((column, value)) = streamed {
            cells.insert(column, value);
        }
        match sealed {
            Ok(()) => {}
            Err(SealError::NeedsKey { space }) => {
                return Err(E2eeRefusal::NeedsKey { space }.error());
            }
            Err(SealError::SpacePending { space }) => {
                return Err(E2eeRefusal::SpacePending { space }.error());
            }
            Err(SealError::Refused(error)) => return Err(error),
        }
        Ok(companions
            .into_iter()
            .map(|(table, row, cells)| {
                StagedOp::Insert(
                    table,
                    cells,
                    InsertOptions {
                        row_id: Some(row),
                        identity: identity.unwrap_or_default(),
                        ..InsertOptions::default()
                    },
                )
            })
            .collect())
    }

    /// The space cell of `row` of `table` as a write to `target` finds
    /// it, live or (for a restore, `deleted`) deleted: `None` for a table
    /// without an encryption entry, or no such row.
    fn stored_space(
        &self,
        hooks: &dyn E2eeHooks,
        table: &str,
        row: RowUuid,
        target: &Target,
        deleted: bool,
    ) -> Result<Option<Value>, Error> {
        let Some(entry) = hooks.declarations().tables.get(table) else {
            return Ok(None);
        };
        let t = self.schema.table(table)?;
        let key = match self.locate(t, row, target)? {
            Located::Absent { .. } => return Ok(None),
            Located::Row { deleted: is, .. } if is != deleted => return Ok(None),
            Located::Row { key, .. } => key,
            Located::Inherited { from, .. } => from,
        };
        if entry.space == "id" {
            return Ok(Some(Value::Uuid(row.0)));
        }
        let column = t.column(&entry.space).ok_or_else(|| {
            Error::new(
                ErrorCode::Schema,
                format!("`{table}` has no space column `{}`", entry.space),
            )
        })?;
        let sql = format!(
            "SELECT {} FROM {} WHERE id = ?1 AND _branch = ?2",
            quote(&column.stored),
            quote(&t.stored)
        );
        let cell: Sql = self
            .conn
            .cached_row(
                &sql,
                rusqlite::params![row.0.as_bytes().to_vec(), key],
                |r| r.get::<_, Sql>(0),
            )
            .map_err(storage)?;
        column_from_sql(column, cell.as_ref(), &self.chunks)
    }

    /// Refuse a partial read or edit (a range, a JSON pointer, a delta, a
    /// splice, an append, a streamed value) of an encrypted column: its
    /// cell is sealed and opened whole.
    pub(crate) fn refuse_encrypted_part(&self, table: &str, column: &str) -> Result<(), Error> {
        let encrypted = self.e2ee.as_ref().is_some_and(|hooks| {
            hooks
                .declarations()
                .tables
                .get(table)
                .is_some_and(|entry| entry.columns.contains_key(column))
        });
        if encrypted {
            return Err(Error::new(
                ErrorCode::Query,
                format!(
                    "reading or writing part of encrypted column `{table}.{column}` is not supported yet"
                ),
            ));
        }
        Ok(())
    }

    /// The sealer of a streamed value of `column` of row `row` of `table`,
    /// written to `target`, when the column is encrypted: the hooks check
    /// the column and choose the key before any source byte is read
    /// (protocol §5.5). `None` for a column the hooks leave alone.
    pub(crate) fn stream_sealer(
        &self,
        table: &str,
        row: RowUuid,
        cells: &BTreeMap<String, Value>,
        column: &str,
        target: &WriteTarget,
    ) -> Result<Option<(Box<dyn StreamSealer>, Option<Value>)>, Error> {
        let Some(hooks) = self.e2ee.clone() else {
            return Ok(None);
        };
        let encrypted = hooks
            .declarations()
            .tables
            .get(table)
            .is_some_and(|entry| entry.columns.contains_key(column));
        if !encrypted {
            return Ok(None);
        }
        let space = self.stream_space(table, row, cells, target)?;
        let sealer = hooks.seal_stream(&StreamTarget {
            table,
            row,
            column,
            space: space.as_ref(),
        });
        match sealer {
            Ok(sealer) => Ok(Some((sealer, space))),
            Err(SealError::NeedsKey { space }) => Err(E2eeRefusal::NeedsKey { space }.error()),
            Err(SealError::SpacePending { space }) => {
                Err(E2eeRefusal::SpacePending { space }.error())
            }
            Err(SealError::Refused(error)) => Err(error),
        }
    }

    /// The space of a streamed value's row: the one the write names
    /// ([`Inner::named_space`]), or the stored row's as a write to `target`
    /// finds it.
    pub(crate) fn stream_space(
        &self,
        table: &str,
        row: RowUuid,
        cells: &BTreeMap<String, Value>,
        target: &WriteTarget,
    ) -> Result<Option<Value>, Error> {
        if let Some(space) = self.named_space(table, row, cells) {
            return Ok(Some(space));
        }
        let Some(hooks) = self.e2ee.clone() else {
            return Ok(None);
        };
        self.stored_space(&*hooks, table, row, &Target::of(target, self)?, false)
    }

    /// The space a write of `row` names: the row itself, for a table whose
    /// rows are their own spaces, else its space cell; `None` when the cells
    /// leave it to the stored row. Beginning a stream, finishing it and
    /// sealing its write all ask this, so they cannot disagree.
    pub(crate) fn named_space(
        &self,
        table: &str,
        row: RowUuid,
        cells: &BTreeMap<String, Value>,
    ) -> Option<Value> {
        let hooks = self.e2ee.as_ref()?;
        let entry = hooks.declarations().tables.get(table)?;
        if entry.space == "id" {
            return Some(Value::Uuid(row.0));
        }
        cells.get(&entry.space).cloned()
    }
}
