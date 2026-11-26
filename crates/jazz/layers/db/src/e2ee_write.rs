//! Sealing writes with the client's encryption hooks ([`crate::e2ee`])
//! before anything stores, chunks or uploads them.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use jazz_model::model::public_api::types::equality_column;

use super::large_ops::{SealedFor, StreamedSeal, sealed_elsewhere};
use super::{
    Batch, BatchPrefix, Cells, Db, GroundedRows, InsertOptions, InsertedRows, LargeEdits, Located,
    PreparedWrite, StagedOp, Store, StreamingMutationKind, StreamingValueUpload, Target,
    WriteTarget, column_from_sql, quote, rejected, storage,
};
use crate::e2ee::{E2eeHooks, SealError, StreamTarget, WriteCells, WriteKind};
use crate::ids::RowUuid;
use crate::options::{Error, ErrorCode, WriteIdentity};
use crate::records::Value;
use crate::tools::OpenTransactionId;
use crate::values::Schema;
use crate::values::{CachedRow, Sql};

impl Db {
    /// Counts retained staging state that a row read cannot observe.
    #[cfg(all(feature = "e2ee-client", any(test, feature = "testing")))]
    #[doc(hidden)]
    pub fn staged_checks_and_chunks_for_test(&self, id: OpenTransactionId) -> (usize, usize) {
        let batches = self.inner.batches.borrow();
        let batch = &batches[&id];
        (
            batch.first_checks.len(),
            batch.edits.derived.len() + batch.edits.pending_nodes.len(),
        )
    }

    /// Stage `op` in batch `id` sealed by the encryption hooks. The internal rows the hooks stage with it go first; if any of them or
    /// the write fails, none stays staged.
    pub(crate) async fn stage_sealed(
        &self,
        id: OpenTransactionId,
        mut op: StagedOp,
        edits: LargeEdits,
        streamed: Option<StreamedSeal<'_>>,
    ) -> Result<(), Error> {
        let schema = self.operation_schema();
        let companions = self
            .in_overlay(id, None, async |inner, _| {
                inner.seal_op(&schema, &mut op, streamed).await
            })
            .await?;
        if companions.is_empty() {
            return self.stage_chunked(id, op, edits).await;
        }
        let names = companion_names(&companions);
        let checkpoint = self
            .inner
            .batches
            .borrow()
            .get(&id)
            .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?
            .checkpoint(&schema, &names);
        let staged = async {
            for companion in companions {
                self.stage_chunked(id, companion, LargeEdits::default())
                    .await?;
            }
            self.stage_chunked(id, op, edits).await
        }
        .await;
        let hooks = {
            let inner = self.inner.as_ref();
            if staged.is_err() {
                // The overlay may hold the companions: the next stage replays
                // what stays staged.
                inner.settle_overlay();
                if let Some(batch) = inner.batches.borrow_mut().get_mut(&id) {
                    batch.restore(checkpoint);
                }
                inner.e2ee_hooks().cloned()
            } else {
                let hooks = inner.e2ee_hooks().cloned();
                if let Some(batch) = inner.batches.borrow_mut().get_mut(&id) {
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

struct BatchCheckpoint {
    prefix: BatchPrefix,
    rows: std::sync::Arc<GroundedRows>,
    inserted: std::sync::Arc<InsertedRows>,
    claims: Option<BTreeMap<String, Value>>,
    new_check_keys: Vec<(String, [u8; 16])>,
}

impl Batch {
    fn checkpoint(&self, schema: &Schema, names: &[(String, RowUuid)]) -> BatchCheckpoint {
        // Only absent entries can be added by these inserts. An unknown
        // table fails staging before it can record a check.
        let new_check_keys = names
            .iter()
            .filter_map(|(table, row)| {
                let stored = schema.stored_table(table).ok()?;
                let key = (stored.to_owned(), *row.0.as_bytes());
                (!self.first_checks.contains_key(&key)).then_some(key)
            })
            .collect();
        BatchCheckpoint {
            prefix: self.prefix(),
            rows: self.rows.clone(),
            inserted: self.inserted.clone(),
            claims: self.claims.clone(),
            new_check_keys,
        }
    }

    fn restore(&mut self, checkpoint: BatchCheckpoint) {
        self.restore_prefix(checkpoint.prefix);
        for key in checkpoint.new_check_keys {
            self.first_checks.remove(&key);
        }
        self.rows = checkpoint.rows;
        self.inserted = checkpoint.inserted;
        self.claims = checkpoint.claims;
    }
}

pub(super) fn companion_names(companions: &[StagedOp]) -> Vec<(String, RowUuid)> {
    companions
        .iter()
        .filter_map(|companion| match companion {
            StagedOp::Insert(table, _, opts) => opts.row_id.map(|row| (table.clone(), row)),
            _ => None,
        })
        .collect()
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

fn seal_error(error: SealError) -> Error {
    match error {
        SealError::Pending(refusal) => refusal.error(),
        SealError::Refused(error) => error,
    }
}

impl From<StreamingMutationKind> for WriteKind {
    fn from(kind: StreamingMutationKind) -> Self {
        match kind {
            StreamingMutationKind::Insert => Self::Insert,
            StreamingMutationKind::Update => Self::Update,
            StreamingMutationKind::Upsert => Self::Upsert,
        }
    }
}

impl Store {
    pub(super) async fn creator_member(
        &self,
        schema: &Schema,
        table: &str,
        options: &InsertOptions,
    ) -> Result<Option<StagedOp>, Error> {
        if !self.add_creator_as_member || options.add_creator_as_member == Some(false) {
            return Ok(None);
        }
        let Some(hooks) = self.e2ee_hooks() else {
            return Ok(None);
        };
        if !hooks
            .declarations()
            .tables
            .values()
            .any(|entry| entry.scope == table)
        {
            return Ok(None);
        }
        let author = match options.identity {
            WriteIdentity::Database => self.stamp_author(),
            WriteIdentity::Session(author) => author,
            WriteIdentity::Attribution(_) => return Ok(None),
        };
        let Some(account) = author.account_id() else {
            return Ok(None);
        };
        let row = options.row_id.expect("authored inserts have a row id");
        let version = schema.jazz.version_id();
        let namespace = self.namespace().await?;
        let (Some(namespace), true) = (namespace, self.names_confirmed(&version)) else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "this store has not received its authority's catalogue for this schema version yet",
            ));
        };
        let scope = crate::names::identity(namespace, &schema.table(table)?.stored, None);
        Ok(Some(StagedOp::Insert(
            jazz_model::schema::MEMBERS_TABLE.to_owned(),
            BTreeMap::from([
                ("scopeId".to_owned(), Value::Uuid(scope)),
                ("identifier".to_owned(), Value::Uuid(row.0)),
                ("accountId".to_owned(), Value::Uuid(account.0)),
            ]),
            InsertOptions {
                row_id: Some(self.ids.borrow_mut().next_row_id()),
                identity: options.identity,
                updated_at_ms: options.updated_at_ms,
                ..Default::default()
            },
        )))
    }

    pub(super) async fn creator_member_write(
        &self,
        schema: Rc<Schema>,
        mut member: StagedOp,
    ) -> Result<PreparedWrite, Error> {
        self.seal_op(&schema, &mut member, None).await?;
        self.prepare_write(schema, member)
    }

    pub(super) async fn add_creator_members(&self, batch: &mut Batch) -> Result<(), Error> {
        if !self.add_creator_as_member {
            return Ok(());
        }
        let members = jazz_model::schema::MEMBERS_TABLE;
        let mut explicit = BTreeSet::new();
        let mut deleted = BTreeSet::new();
        for write in &batch.ops {
            match &write.op {
                StagedOp::Insert(_, cells, _) if write.layout.source.name == members => {
                    explicit.insert(member_address(cells, |name| {
                        write.layout.source.stored_name(name)
                    }));
                }
                StagedOp::Delete(table, row, _) => {
                    deleted.insert((table.as_str(), *row));
                }
                _ => {}
            }
        }
        let mut generated = Vec::new();
        for write in &batch.ops {
            let StagedOp::Insert(table, _, options) = &write.op else {
                continue;
            };
            if options
                .row_id
                .is_some_and(|row| deleted.contains(&(table.as_str(), row)))
            {
                continue;
            }
            let schema = &write.layout.policies;
            let Some(member) = self
                .creator_member(schema, &write.layout.source.name, options)
                .await?
            else {
                continue;
            };
            let StagedOp::Insert(_, cells, _) = &member else {
                unreachable!()
            };
            if explicit.insert(member_address(cells, |name| name)) {
                generated.push(self.creator_member_write(schema.clone(), member).await?);
            }
        }
        batch.ops.extend(generated);
        Ok(())
    }

    /// Seal `op`, a write this database authors under `schema` (named as its
    /// caller named it), before anything stores or chunks it. Returns the
    /// internal rows the write needs, as inserts to apply before it in the
    /// same transaction. A streamed write already holds its sealed cells.
    /// Check its space and claim any root it needs here.
    pub(crate) async fn seal_op(
        &self,
        schema: &Rc<Schema>,
        op: &mut StagedOp,
        streamed: Option<StreamedSeal<'_>>,
    ) -> Result<Vec<StagedOp>, Error> {
        let Some(hooks) = self.e2ee_hooks().cloned() else {
            return Ok(Vec::new());
        };
        let identity = op.identity();
        let table = op.table();
        let stored_space = if hooks.declarations().tables.contains_key(table) {
            let located = self.prepare_write(schema.clone(), op.clone())?;
            match &located.op {
                StagedOp::Update(_, row, _, opts) => {
                    self.found_space(&*hooks, table, &located.layout, *row, &opts.target)
                        .await?
                }
                StagedOp::Upsert(_, row, _, _, opts) => {
                    self.found_space(&*hooks, table, &located.layout, *row, &opts.target)
                        .await?
                }
                // A restore finds the row deleted and uses its previous space.
                StagedOp::Restore(_, row, _, opts) => {
                    self.stored_space(
                        &*hooks,
                        table,
                        &located.layout,
                        *row,
                        &Target::exact(&opts.target),
                        true,
                    )
                    .await?
                }
                _ => None,
            }
        } else {
            None
        };
        let mut none = BTreeMap::new();
        let (table, row, kind, cells) = match op {
            StagedOp::Insert(table, cells, opts) => {
                let row = opts.row_id.ok_or_else(|| {
                    Error::new(
                        ErrorCode::Protocol,
                        "an insert is sealed once its row id is drawn",
                    )
                })?;
                (table.clone(), row, WriteKind::Insert, cells)
            }
            StagedOp::Update(table, row, cells, _) => {
                (table.clone(), *row, WriteKind::Update, cells)
            }
            StagedOp::Upsert(table, row, cells, _, _) => {
                (table.clone(), *row, WriteKind::Upsert, cells)
            }
            StagedOp::Delete(table, row, _) => (table.clone(), *row, WriteKind::Delete, &mut none),
            StagedOp::Restore(table, row, content, _) => (
                table.clone(),
                *row,
                WriteKind::Restore,
                content.as_mut().unwrap_or(&mut none),
            ),
            StagedOp::Move(..) => return Ok(Vec::new()),
        };
        let mut companions = Vec::new();
        let mut insert_only = BTreeMap::new();
        if let Some(seal) = streamed {
            let space = self
                .stream_space(&table, row, cells, stored_space.as_ref())
                .map_err(|_| sealed_elsewhere())?;
            if seal.row != row || space != seal.space {
                return Err(sealed_elsewhere());
            }
            if kind == WriteKind::Upsert && stored_space.is_some() != seal.existed {
                return Err(rejected(
                    "the row of a streamed upsert appeared or disappeared during the upload",
                ));
            }
            companions.clone_from(&seal.companions);
            insert_only.clone_from(&seal.insert_only);
            hooks
                .claim_stream_companions(
                    &table,
                    space.as_ref(),
                    seal.root.as_deref(),
                    &mut companions,
                )
                .map_err(seal_error)?;
        } else {
            hooks
                .seal_write(&mut WriteCells {
                    table: &table,
                    row,
                    kind,
                    cells,
                    insert_only: &mut insert_only,
                    streamed_column: None,
                    stream_root: None,
                    stored_space: stored_space.as_ref(),
                    companion_rows: &mut companions,
                })
                .map_err(seal_error)?;
        }
        if let StagedOp::Upsert(_, _, _, created, _) = op {
            created.extend(insert_only);
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

    async fn found_space(
        &self,
        hooks: &dyn E2eeHooks,
        table: &str,
        layout: &super::WriteTable,
        row: RowUuid,
        target: &WriteTarget,
    ) -> Result<Option<Value>, Error> {
        let target = Target::of(target, self).await?;
        self.stored_space(hooks, table, layout, row, &target, false)
            .await
    }

    /// The space cell of `row` of `table` as a write
    /// to `target` (in stored names) finds it, live or (for a restore,
    /// `deleted`) deleted: `None` for a table without an encryption entry,
    /// or no such row.
    async fn stored_space(
        &self,
        hooks: &dyn E2eeHooks,
        table: &str,
        layout: &super::WriteTable,
        row: RowUuid,
        target: &Target,
        deleted: bool,
    ) -> Result<Option<Value>, Error> {
        let Some(entry) = hooks.declarations().tables.get(table) else {
            return Ok(None);
        };
        let key = match self.locate(&layout.table, row, target).await? {
            Located::Absent { .. } => return Ok(None),
            Located::Row { deleted: is, .. } if is != deleted => return Ok(None),
            Located::Row { key, .. } => key,
            Located::Inherited { from, .. } => from,
        };
        if entry.is_own_space() {
            return Ok(Some(Value::Uuid(row.0)));
        }
        let column = layout.source.column(&entry.space).ok_or_else(|| {
            Error::new(
                ErrorCode::Schema,
                format!("`{table}` has no space column `{}`", entry.space),
            )
        })?;
        let sql = format!(
            "SELECT {} FROM {} WHERE id = ?1 AND _branch = ?2",
            quote(&column.stored),
            quote(&layout.source.stored)
        );
        let cell: Sql = self
            .conn
            .cached_row(
                &sql,
                crate::storage::params![row.0.as_bytes().to_vec(), key],
                |r| r.get::<_, Sql>(0),
            )
            .await
            .map_err(storage)?;
        column_from_sql(column, cell.as_ref(), &self.chunks).await
    }

    /// Refuse a partial read or edit (a range, a JSON pointer, a delta, a
    /// splice, an append, a streamed value) of an encrypted column: its
    /// cell is sealed and opened whole.
    pub(crate) fn refuse_encrypted_part(&self, table: &str, column: &str) -> Result<(), Error> {
        let encrypted = self.e2ee_hooks().is_some_and(|hooks| {
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

    /// Plain siblings of an encrypted row are checked before the source runs.
    /// The hooks check encrypted siblings against their declared plaintext types.
    pub(super) fn validate_stream_siblings(
        &self,
        schema: &Schema,
        table: &str,
        cells: &BTreeMap<String, Value>,
    ) -> Result<(), Error> {
        let Some(entry) = self
            .e2ee_hooks()
            .and_then(|hooks| hooks.declarations().tables.get(table))
        else {
            return Ok(());
        };
        if entry.columns.is_empty() {
            return Ok(());
        }
        let t = schema.table(table)?;
        self.validate_named_cells(t, cells.iter().map(|(name, value)| (name.as_str(), value)))?;
        for (name, value) in cells {
            if entry.columns.contains_key(name) {
                continue;
            }
            let column = t.column(name).expect("validated above");
            if let Value::Large(large) = Self::check_cell(column, value, Cells::Authored)? {
                Self::check_large_kind(column, large)?;
            }
        }
        Ok(())
    }

    pub(super) fn check_rowless_stream(
        &self,
        table: &str,
        cells: &BTreeMap<String, Value>,
        mutation: Option<StreamingMutationKind>,
    ) -> Result<(), Error> {
        let Some(entry) = self
            .e2ee_hooks()
            .and_then(|hooks| hooks.declarations().tables.get(table))
        else {
            return Ok(());
        };
        if let Some(column) = cells.keys().find(|name| entry.columns.contains_key(*name)) {
            return Err(Error::new(
                ErrorCode::Query,
                format!(
                    "Writing encrypted column `{table}.{column}` in a rowless upload is not supported. Begin the upload with its row and mutation kind."
                ),
            ));
        }
        if matches!(
            mutation,
            Some(StreamingMutationKind::Insert | StreamingMutationKind::Upsert)
        ) {
            return Err(Error::new(
                ErrorCode::Query,
                format!(
                    "Inserting or upserting into encrypted table `{table}` in a rowless upload is not supported. Begin the upload with its row and mutation kind."
                ),
            ));
        }
        Ok(())
    }

    /// Prepare the row's encrypted metadata and its stream together, while the
    /// same store overlay and key are current and before the source can run.
    pub(super) async fn prepare_stream(
        &self,
        schema: &Rc<Schema>,
        upload: &mut StreamingValueUpload,
        mutation: StreamingMutationKind,
        row: RowUuid,
        cells: &mut BTreeMap<String, Value>,
        target: &WriteTarget,
    ) -> Result<(), Error> {
        let Some(hooks) = self.e2ee_hooks() else {
            return Ok(());
        };
        let table = &upload.table;
        let column = &upload.column;
        let Some(entry) = hooks.declarations().tables.get(table) else {
            return Ok(());
        };
        let stored = match mutation {
            StreamingMutationKind::Insert => None,
            _ => {
                let source = schema.table(table)?;
                let layout = self.write_layout(schema, source);
                let stored;
                let target = if schema.stored_names {
                    target
                } else {
                    stored = source.stored_write_target(target.clone());
                    &stored
                };
                self.found_space(&**hooks, table, &layout, row, target)
                    .await?
            }
        };
        // An upsert of a row this store does not hold inserts it; publication
        // refuses it if the row appeared since, so the same check applies.
        let inserts = mutation == StreamingMutationKind::Insert
            || (mutation == StreamingMutationKind::Upsert && stored.is_none());
        if inserts && !entry.columns.is_empty() {
            let schema = schema.table(table)?;
            let branch_cells = match target {
                WriteTarget::BranchView { head, .. } => schema.selector_cells(head)?,
                WriteTarget::Root => BTreeMap::new(),
            };
            // The branch selector supplies its columns, and sealing supplies equality tokens.
            let tokens: std::collections::BTreeSet<_> = entry
                .equality
                .iter()
                .map(|name| equality_column(name))
                .collect();
            for sibling in &schema.columns {
                let name = &sibling.name;
                if name == column
                    || cells.contains_key(name)
                    || branch_cells.contains_key(name)
                    || tokens.contains(name)
                {
                    continue;
                }
                let required = match entry.columns.get(name) {
                    Some(declared) => !declared.plain.nullable && declared.plain.default.is_none(),
                    None => !sibling.nullable() && sibling.default.is_none(),
                };
                if required {
                    return Err(super::missing_required(table, name));
                }
            }
        }
        let space = self.stream_space(table, row, cells, stored.as_ref())?;
        if entry.columns.contains_key(column) {
            upload.sealer = Some(
                hooks
                    .seal_stream(&StreamTarget {
                        table,
                        row,
                        column,
                        space: space.as_ref(),
                    })
                    .map_err(seal_error)?,
            );
        }
        let mut companions = Vec::new();
        let mut insert_only = BTreeMap::new();
        let mut metadata = WriteCells {
            table,
            row,
            kind: mutation.into(),
            cells,
            insert_only: &mut insert_only,
            streamed_column: Some(column),
            stream_root: None,
            stored_space: stored.as_ref(),
            companion_rows: &mut companions,
        };
        hooks.seal_write(&mut metadata).map_err(seal_error)?;
        let root = metadata.stream_root;
        upload.sealed_for = Some(SealedFor {
            row,
            mutation,
            target: target.clone(),
            space,
            existed: stored.is_some(),
            cells: std::mem::take(cells),
            insert_only,
            companions,
            root,
        });
        Ok(())
    }

    fn stream_space(
        &self,
        table: &str,
        row: RowUuid,
        cells: &BTreeMap<String, Value>,
        stored: Option<&Value>,
    ) -> Result<Option<Value>, Error> {
        let named = self.named_space(table, row, cells);
        if let (Some(stored), Some(named)) = (stored, &named)
            && stored != named
        {
            return Err(rejected(
                "an encrypted row cannot change its space through a streamed write",
            ));
        }
        Ok(stored.cloned().or(named))
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
        let hooks = self.e2ee_hooks()?;
        let entry = hooks.declarations().tables.get(table)?;
        if entry.is_own_space() {
            return Some(Value::Uuid(row.0));
        }
        cells.get(&entry.space).cloned()
    }
}

fn member_address<'a>(
    cells: &BTreeMap<String, Value>,
    column: impl Fn(&'static str) -> &'a str,
) -> [Option<uuid::Uuid>; 3] {
    ["scopeId", "identifier", "accountId"].map(|name| match cells.get(column(name)) {
        Some(Value::Uuid(id)) => Some(*id),
        _ => None,
    })
}
