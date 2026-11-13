//! Range reads and in-place edits of `string`, `json` and `bytes` cells.
//!
//! Inline and chunked cells share these APIs. A chunked cell reads only the
//! chunks its range covers, and an append or splice adds one entry to the
//! value's bounded edit tail instead of rewriting it; when the tail is full
//! the value is consolidated, rechunking only until content boundaries
//! resynchronize (see `jazz_types::large_values`). Every edit is published as one
//! ordinary row update, so policies, history and sync see a new version.

use super::*;
use crate::large::Logical;
use jazz_types::large_values as lv;
use serde::{Deserialize, Serialize};

/// Coordinate space of a partial large-value update.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LargeValueUpdatePage {
    /// Byte page of a bytes value.
    Bytes {
        /// Inclusive byte offset.
        from: u64,
        /// Exclusive byte offset.
        to: u64,
    },
    /// UTF-16 code-unit page of a text value.
    TextUtf16 {
        /// Inclusive UTF-16 offset.
        from: u64,
        /// Exclusive UTF-16 offset.
        to: u64,
    },
    /// UTF-8 byte page of a text value.
    TextUtf8 {
        /// Inclusive UTF-8 byte offset.
        from: u64,
        /// Exclusive UTF-8 byte offset.
        to: u64,
    },
}

/// One sequential splice, in the coordinates of its page.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LargeValueUpdateSplice {
    /// Page-relative start.
    pub at: u64,
    /// Page units to remove.
    pub delete: u64,
    /// Replacement bytes, UTF-8 for text pages.
    pub insert: Vec<u8>,
}

pub use jazz_model::model::public_api::types::LargeValueDeltaSplice;

/// An RFC 6901 JSON replacement of an existing member or array element.
/// Missing parents are never created, so a typo cannot change a document's
/// shape.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct JsonSetEdit {
    /// RFC 6901 path.
    pub at: String,
    /// Replacement value.
    pub value: serde_json::Value,
}

/// One partial update of a large-capable column.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LargeValueUpdate {
    /// Sequential splices within one page.
    Splice {
        /// Target column.
        column: String,
        /// Coordinate page shared by the splices.
        within: LargeValueUpdatePage,
        /// Ordered replacements.
        splices: Vec<LargeValueUpdateSplice>,
    },
    /// JSON replacements by pointer.
    JsonSet {
        /// Target column.
        column: String,
        /// Ordered replacements.
        edits: Vec<JsonSetEdit>,
    },
    /// A text-merge column's new value, `text`, made by editing
    /// `edited_from`: the value its writer (an editor) last saw. When the
    /// column moved on since (a merged value this store took in that the
    /// editor has not shown yet), `text` is merged over the column's value
    /// as the authority merges, so the write keeps what it did not see.
    TextEdit {
        /// Target column.
        column: String,
        /// The column's new value as the writer made it.
        text: String,
        /// The value the writer made it from.
        edited_from: String,
    },
}

impl LargeValueUpdate {
    fn column(&self) -> &str {
        match self {
            Self::Splice { column, .. }
            | Self::JsonSet { column, .. }
            | Self::TextEdit { column, .. } => column,
        }
    }
}

/// One byte-coordinate edit.
#[derive(Clone)]
pub(crate) enum Edit {
    Append(Vec<u8>),
    Splice {
        offset: u64,
        delete: u64,
        insert: Vec<u8>,
    },
}

fn query(what: impl std::fmt::Display) -> Error {
    Error::new(ErrorCode::Query, what.to_string())
}

fn not_large(table: &str, column: &str) -> Error {
    Error::new(
        ErrorCode::Schema,
        format!("{table}.{column} is not a bytes or string cell"),
    )
}

/// `range` of `len` units, checked.
fn check_range(range: &std::ops::Range<u64>, len: u64) -> Result<(), Error> {
    if range.start > range.end || range.end > len {
        return Err(query("value range is out of bounds"));
    }
    Ok(())
}

/// The byte offset of UTF-16 position `at` in `text`, or `None` inside a
/// surrogate pair or past the end.
fn utf16_byte(text: &str, at: u64) -> Option<usize> {
    let mut units = 0_u64;
    for (byte, character) in text.char_indices() {
        if units == at {
            return Some(byte);
        }
        units += character.len_utf16() as u64;
    }
    (units == at).then_some(text.len())
}

impl Inner {
    async fn stage_upload(
        &self,
        id: &[u8; 32],
        staged: &[lv::StagedChunk],
        replica: bool,
    ) -> Result<(), Error> {
        let cache = self.conn.sql().transaction_open() || self.chunks.in_memory();
        crate::values::in_savepoint_async(&self.conn, "jazz_lv_upload", async || {
            crate::large::stage_upload(
                &mut self.conn.sql(),
                &self.chunks,
                id,
                staged,
                replica,
                cache,
            )
            .await
        })
        .await
    }

    /// Drop queues contain only this owner's uploads. SQL errors retain the
    /// receipt for the next maintenance pass; crash recovery uses lease expiry.
    pub(super) async fn release_dropped_uploads(&self) -> Result<(), Error> {
        // Removing an in-memory receipt must not outlive a SQL rollback.
        // Callback transactions can tick while suspended; defer cleanup until
        // its deletes can commit independently of that application unit.
        if self.conn.sql().transaction_open() {
            return Ok(());
        }
        let mut cleanup = self.upload_cleanup.borrow_mut();
        while let Some(id) = cleanup.last() {
            crate::large::end_upload(&mut self.conn.sql(), id).await?;
            cleanup.pop();
        }
        Ok(())
    }

    /// Forget what the finished write's edits derived.
    pub(crate) fn clear_derived(&mut self) {
        self.pending_nodes.clear();
        self.derived.clear();
    }

    /// Whose read policy a cell-level read or edit runs under: the store's
    /// own author (the system author reads everything).
    fn reader(&self) -> Option<AuthorSubject> {
        let author = self.identity.author;
        (author != AuthorSubject::SYSTEM).then_some(author)
    }

    /// The stored cell of `column` in the row visible to `identity`, with
    /// its column. Errors when the row is not visible.
    async fn physical_cell(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        identity: Option<AuthorSubject>,
    ) -> Result<(crate::values::Column, Sql), Error> {
        let t = self.schema.table(table)?;
        if !t.branch_by.is_empty() {
            return Err(unsupported(
                "large-value range reads and edits on branched tables",
            ));
        }
        let col = t
            .column(column)
            .filter(|c| crate::large::large_capable(c))
            .ok_or_else(|| not_large(table, column))?
            .clone();
        let query = Query::from(table)
            .filter(crate::query::eq(
                crate::query::col("id"),
                crate::query::lit(Value::Uuid(row.0)),
            ))
            .select([column]);
        let prepared = PreparedQuery {
            query,
            binding: BTreeMap::new(),
            shape: Shape([0; 16]),
            binding_id: Binding([0; 16]),
            request_claims: None,
            // Read under the schema this handle serves (a pinned session's
            // view and its bundle's policies), as its own reads are.
            schema: Some(self.schema.clone()),
        };
        let compiled = self.compile(&prepared, identity, &ReadSpec::default())?;
        let params: Vec<_> = compiled.params.iter().map(Sql::as_param).collect();
        let cell: Option<Sql> = self
            .conn
            .sql()
            .query_optional(&compiled.sql, &params, |r| r.get(3))
            .await
            .map_err(storage)?;
        let cell = cell.ok_or_else(|| {
            Error::new(ErrorCode::NotObserved, "large-value cell is not observed")
        })?;
        Ok((col, cell))
    }

    async fn read_range(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        range: std::ops::Range<u64>,
        utf16: bool,
    ) -> Result<Vec<u8>, Error> {
        let (col, cell) = self
            .physical_cell(table, row, column, self.reader())
            .await?;
        if !large_cell(&cell) {
            return Err(not_large(table, column));
        }
        if utf16 && crate::large::kind(&col) == lv::LargeValueKind::Bytes {
            return Err(not_large(table, column));
        }
        self.cell_range(&mut self.conn.sql(), cell.as_ref(), range, utf16)
            .await
    }

    /// `delta(a, b)` (SPEC 19.6) between the values `column` of `row` holds
    /// in its versions `from` and `to`, for a reader allowed to read both by
    /// `policies` (a pinned session's bundle, or the current schema).
    /// Only the chunks in changed regions are read.
    pub(crate) async fn value_delta(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        (from, to): (sync::Rank, sync::Rank),
        reader: Option<AuthorSubject>,
        policies: &Schema,
    ) -> Result<Vec<LargeValueDeltaSplice>, Error> {
        let t = self.schema.table(table)?;
        let governed = policies.table(table)?;
        if !t.branch_by.is_empty() {
            return Err(unsupported("large-value deltas on branched tables"));
        }
        let col = t
            .column(column)
            .filter(|c| crate::large::large_capable(c))
            .ok_or_else(|| not_large(table, column))?;
        let text = crate::large::kind(col) != lv::LargeValueKind::Bytes;
        // Losing read access takes effect at once, for earlier versions too.
        if let Some(author) = reader
            && !self
                .check_in(policies, governed, row, &[], author, Operation::Select)
                .await?
        {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "the row is not readable".to_owned(),
            ));
        }
        let mut cells = Vec::with_capacity(2);
        for at in [from, to] {
            if let Some(author) = reader
                && !self
                    .check_version(policies, governed, row, &[], at, author, Operation::Select)
                    .await?
            {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    format!("version {} of the row is not readable", at.0.seq),
                ));
            }
            cells.push(self.version_cell(t, row, &col.stored, at).await?);
        }
        async fn logical<'a>(
            conn: &crate::host::DbConnection,
            chunks: &crate::large::Chunks,
            cell: &'a Sql,
        ) -> Result<Logical<'a>, Error> {
            match cell {
                Sql::Null => Ok(Logical::Inline(&[][..])),
                cell => crate::large::logical(&mut conn.sql(), chunks, cell.as_ref()).await,
            }
        }
        let (a, b) = (
            logical(&self.conn, &self.chunks, &cells[0]).await?,
            logical(&self.conn, &self.chunks, &cells[1]).await?,
        );
        if let (Logical::Chunked(a), Logical::Chunked(b)) = (&a, &b) {
            let edits = crate::large::read(
                &mut self.conn.sql(),
                &self.chunks,
                |inputs| lv::delta_attempt(a, b, inputs),
                None,
            )
            .await?;
            return Ok(edits.into_iter().map(Into::into).collect());
        }
        if let (Logical::Inline(a), Logical::Inline(b)) = (&a, &b) {
            return Ok(byte_splices(a, b, text));
        }
        // One inline side, one chunked: the value crossed the chunking
        // threshold, so it changed; one replace of the whole value.
        let (delete, delete_utf16) = match &a {
            Logical::Inline(bytes) => (bytes.len() as u64, utf16_len(bytes, text)?),
            Logical::Chunked(value) => (value.byte_length, value.utf16_length.unwrap_or(0)),
        };
        let insert = match b {
            Logical::Inline(bytes) => bytes.to_vec(),
            Logical::Chunked(value) => {
                crate::large::value_bytes(&mut self.conn.sql(), &self.chunks, &value).await?
            }
        };
        if delete == 0 && insert.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![LargeValueDeltaSplice {
            at: 0,
            delete,
            insert,
            at_utf16: 0,
            delete_utf16,
        }])
    }

    /// The stored cell `stored` of `row`'s version at `at`, current or in
    /// history: a pending one by its writer too, when given, as two
    /// writers' can share a position.
    async fn version_cell(
        &self,
        t: &Table,
        row: RowUuid,
        stored: &str,
        at: sync::Rank,
    ) -> Result<Sql, Error> {
        let writer = at.1.filter(|_| !at.0.accepted());
        for table in [t.stored.clone(), format!("{}__h", t.stored)] {
            let id = row.0.as_bytes().to_vec();
            let cell: Option<Sql> = match writer {
                Some((node, tx)) => self.conn.sql().query_optional(&format!(
                        "SELECT {} FROM {} WHERE id = ?1 AND _branch = ?2 AND _clock = ?3 AND _writer = ?4",
                        quote(stored),
                        quote(&table)
                    ), jazz_sql::params![id, Vec::<u8>::new(), at.0.t(), sync::writer_bytes(node, tx)], |r| r.get(0)).await,
                None => self.conn.sql().query_optional(&format!(
                        "SELECT {} FROM {} WHERE id = ?1 AND _branch = ?2 AND _clock = ?3 \
                         ORDER BY _writer DESC LIMIT 1",
                        quote(stored),
                        quote(&table)
                    ), jazz_sql::params![id, Vec::<u8>::new(), at.0.t()], |r| r.get(0)).await,
            }
            .map_err(storage)?;
            if let Some(cell) = cell {
                return Ok(cell);
            }
        }
        Err(Error::new(
            ErrorCode::NotObserved,
            format!("version {} of the row is not held", at.0.seq),
        ))
    }

    /// `range` of a cell's logical value (UTF-16 code units when `utf16`,
    /// else bytes), reading only the chunks it covers.
    pub(crate) async fn cell_range<S: jazz_sql::Sql>(
        &self,
        conn: &mut S,
        cell: SqlCell<'_>,
        range: std::ops::Range<u64>,
        utf16: bool,
    ) -> Result<Vec<u8>, Error>
    where
        S::Error: std::fmt::Display,
    {
        match crate::large::logical(conn, &self.chunks, cell).await? {
            Logical::Inline(bytes) if utf16 => {
                let text = std::str::from_utf8(bytes).map_err(|_| query("cell is not text"))?;
                let len = text.encode_utf16().count() as u64;
                check_range(&range, len)?;
                let (Some(start), Some(end)) =
                    (utf16_byte(text, range.start), utf16_byte(text, range.end))
                else {
                    return Err(query("UTF-16 range splits a surrogate pair"));
                };
                Ok(bytes[start..end].to_vec())
            }
            Logical::Inline(bytes) => {
                check_range(&range, bytes.len() as u64)?;
                Ok(bytes[range.start as usize..range.end as usize].to_vec())
            }
            Logical::Chunked(value) => {
                let len = if utf16 {
                    value.utf16_length.unwrap_or(0)
                } else {
                    value.byte_length
                };
                check_range(&range, len)?;
                if utf16 {
                    // A UTF-16 code unit is one to three UTF-8 bytes, so
                    // these bytes hold the range: the pull asks for them.
                    let bytes = range.start.min(value.byte_length)
                        ..range.end.saturating_mul(3).min(value.byte_length);
                    crate::large::read(
                        conn,
                        &self.chunks,
                        |inputs| lv::utf16_range_attempt(&value, range.clone(), inputs),
                        Some((&value, bytes)),
                    )
                    .await
                } else {
                    crate::large::read(
                        conn,
                        &self.chunks,
                        |inputs| lv::byte_range_attempt(&value, range.clone(), inputs),
                        Some((&value, range.clone())),
                    )
                    .await
                }
            }
        }
    }

    /// A selected cell read only in `part` (SPEC 19.12): only the chunks
    /// the part covers are read.
    pub(crate) async fn partial_cell<S: jazz_sql::Sql>(
        &self,
        conn: &mut S,
        ty: &ValueType,
        cell: SqlCell<'_>,
        part: &crate::query::Part,
    ) -> Result<Option<Value>, Error>
    where
        S::Error: std::fmt::Display,
    {
        use crate::query::Part;
        if let ValueType::Nullable(inner) = ty {
            return Ok(Some(match cell {
                SqlCell::Null => Value::Nullable(None),
                cell => Value::Nullable(
                    Box::pin(self.partial_cell(conn, inner, cell, part))
                        .await?
                        .map(Box::new),
                ),
            }));
        }
        let text = |bytes: Vec<u8>, split: &str| {
            String::from_utf8(bytes)
                .map(Value::String)
                .map_err(|_| query(split))
        };
        Ok(Some(match part {
            Part::Bytes { from, to } => {
                Value::Bytes(self.cell_range(conn, cell, *from..*to, false).await?)
            }
            Part::TextUtf16 { from, to } => text(
                self.cell_range(conn, cell, *from..*to, true).await?,
                "UTF-16 range splits a surrogate pair",
            )?,
            Part::TextUtf8 { from, to } => text(
                self.cell_range(conn, cell, *from..*to, false).await?,
                "UTF-8 range splits a code point",
            )?,
            Part::JsonPointer(pointer) => {
                let found = self
                    .cell_pointer(conn, cell, pointer)
                    .await?
                    .ok_or_else(|| query("JSON pointer path does not exist"))?;
                Value::String(found.to_string())
            }
        }))
    }

    /// The subtree at JSON pointer `pointer` of a JSON cell, reading the
    /// document only as far as the pointer needs when it can.
    pub(crate) async fn cell_pointer<S: jazz_sql::Sql>(
        &self,
        conn: &mut S,
        cell: SqlCell<'_>,
        pointer: &str,
    ) -> Result<Option<serde_json::Value>, Error>
    where
        S::Error: std::fmt::Display,
    {
        let source = match crate::large::logical(conn, &self.chunks, cell).await? {
            Logical::Inline(bytes) => bytes.to_vec(),
            Logical::Chunked(value) => {
                let mut source = Vec::new();
                let mut offset = 0;
                while offset < value.byte_length {
                    let end = (offset + lv::LEAF_MIN_BYTES as u64).min(value.byte_length);
                    source.extend(
                        crate::large::read(
                            conn,
                            &self.chunks,
                            |inputs| lv::byte_range_attempt(&value, offset..end, inputs),
                            Some((&value, offset..end)),
                        )
                        .await?,
                    );
                    offset = end;
                    match lv::json_pointer_prefix(&source, pointer)
                        .map_err(|e| query(e.to_string()))?
                    {
                        lv::JsonPointerPrefix::Found(found) => return Ok(found),
                        lv::JsonPointerPrefix::NeedMore => {}
                        lv::JsonPointerPrefix::RequiresFullDocument => {
                            if offset < value.byte_length {
                                let rest = offset..value.byte_length;
                                source.extend(
                                    crate::large::read(
                                        conn,
                                        &self.chunks,
                                        |inputs| {
                                            lv::byte_range_attempt(&value, rest.clone(), inputs)
                                        },
                                        Some((&value, rest.clone())),
                                    )
                                    .await?,
                                );
                            }
                            break;
                        }
                    }
                }
                source
            }
        };
        let json: serde_json::Value =
            serde_json::from_slice(&source).map_err(|e| query(format!("stored JSON: {e}")))?;
        Ok(json.pointer(pointer).cloned())
    }

    /// A whole write of `logical` over a chunked value as the edit that
    /// makes it: the one splice between the held value and the new one,
    /// which costs its epoch a few bytes and travels as an edit, where the
    /// whole value would be a new tree. `None` keeps the write whole: the
    /// held value is inline or cannot be read here (its chunks not fetched,
    /// a branched table), or the change inserts more than an epoch edit
    /// holds. The edit's derived value and any nodes it wrote are left in
    /// `derived` and `pending_nodes` for the caller to take.
    pub(crate) async fn whole_write_as_edit(
        &mut self,
        table: &str,
        row: RowUuid,
        column: &crate::values::Column,
        logical: &[u8],
        identity: WriteIdentity,
    ) -> Option<Value> {
        let (col, cell) = self
            .physical_cell(table, row, &column.name, identity_of(identity))
            .await
            .ok()?;
        self.as_edit_of(table, &col, cell, logical).await
    }

    /// `logical` as an edit of the chunked `cell` ([`Self::whole_write_as_edit`]);
    /// a value equal to the held one is the held value, so rewriting a
    /// document unchanged costs nothing.
    pub(crate) async fn as_edit_of(
        &mut self,
        table: &str,
        column: &crate::values::Column,
        cell: Sql,
        logical: &[u8],
    ) -> Option<Value> {
        let Ok(Logical::Chunked(held)) =
            crate::large::logical(&mut self.conn.sql(), &self.chunks, cell.as_ref()).await
        else {
            return None;
        };
        let current = crate::large::value_bytes(&mut self.conn.sql(), &self.chunks, &held)
            .await
            .ok()?;
        let text = crate::large::kind(column) != lv::LargeValueKind::Bytes;
        let Some(splice) = byte_splices(&current, logical, text).pop() else {
            self.derived.push((*held).clone());
            return Some(Value::Large(held));
        };
        // A longer insertion materializes a new tree either way (SPEC 19.4),
        // which the whole write builds as well.
        if splice.insert.len() > lv::epoch::EPOCH_INSERT_BYTES {
            return None;
        }
        let edit = Edit::Splice {
            offset: splice.at,
            delete: splice.delete,
            insert: splice.insert,
        };
        self.edited(table, column, cell, edit).await.ok()
    }

    /// The value `edit` makes of the current cell. A chunked value gains a
    /// tail entry, or is consolidated when its tail is full; the nodes a
    /// consolidation writes are stored with the row update.
    pub(crate) async fn edited(
        &mut self,
        table: &str,
        column: &crate::values::Column,
        cell: Sql,
        edit: Edit,
    ) -> Result<Value, Error> {
        let kind = crate::large::kind(column);
        if !large_cell(&cell) {
            return Err(not_large(table, &column.name));
        }
        let value =
            match crate::large::logical(&mut self.conn.sql(), &self.chunks, cell.as_ref()).await? {
                Logical::Inline(bytes) => {
                    let mut bytes = bytes.to_vec();
                    match edit {
                        Edit::Append(suffix) => bytes.extend(suffix),
                        Edit::Splice {
                            offset,
                            delete,
                            insert,
                        } => {
                            let end = offset
                                .checked_add(delete)
                                .ok_or_else(|| query("splice range overflows"))?;
                            check_range(&(offset..end), bytes.len() as u64)?;
                            bytes.splice(offset as usize..end as usize, insert);
                        }
                    }
                    return logical_value(kind, bytes);
                }
                Logical::Chunked(value) => value,
            };
        let outcome = match edit {
            Edit::Append(suffix) => {
                lv::append_tail(&value, suffix).map_err(|e| rejected(e.to_string()))?
            }
            Edit::Splice {
                offset,
                delete,
                insert,
            } => {
                crate::large::write(&mut self.conn.sql(), &self.chunks, |inputs| {
                    lv::replace_tail_attempt(&value, offset, delete, insert.clone(), inputs)
                })
                .await?
            }
        };
        let next = match outcome {
            lv::TailOutcome::Updated(next) => {
                self.derived.push(next.clone());
                next
            }
            lv::TailOutcome::ConsolidationRequired(transient) => {
                let mut continuation = lv::ConsolidationContinuation::new(transient)
                    .map_err(|e| rejected(e.to_string()))?;
                let prepared = loop {
                    if let Some(prepared) = crate::large::read(
                        &mut self.conn.sql(),
                        &self.chunks,
                        |inputs| continuation.step(inputs),
                        None,
                    )
                    .await?
                    {
                        break prepared;
                    }
                };
                let next = prepared.value_ref.clone();
                // A later edit of the same write reads the new tree before
                // the row update stores it.
                self.chunks.cache_pinned(&prepared);
                self.pending_nodes.push(prepared);
                next
            }
        };
        Ok(Value::Large(Box::new(next)))
    }
}

/// An edited inline value, checked for its kind.
fn logical_value(kind: lv::LargeValueKind, bytes: Vec<u8>) -> Result<Value, Error> {
    Ok(match kind {
        lv::LargeValueKind::Bytes => Value::Bytes(bytes),
        lv::LargeValueKind::String => Value::String(
            String::from_utf8(bytes).map_err(|_| rejected("the edited string is not UTF-8"))?,
        ),
        lv::LargeValueKind::Json => {
            let text =
                String::from_utf8(bytes).map_err(|_| rejected("the edited JSON is not UTF-8"))?;
            serde_json::from_str::<serde_json::Value>(&text)
                .map_err(|_| rejected("the edited JSON is not valid JSON"))?;
            Value::String(text)
        }
    })
}

fn nullable(column: &crate::values::Column, value: Value) -> Value {
    if column.nullable() {
        Value::Nullable(Some(Box::new(value)))
    } else {
        value
    }
}

/// The session an edit's read of the current cell is checked as: a
/// trusted session's, or none for local staging, which (like every local
/// write) the authority checks on upload.
fn identity_of(identity: WriteIdentity) -> Option<AuthorSubject> {
    match who(identity) {
        Who::Session(author) => Some(author),
        Who::Local | Who::Attributed(_) => None,
    }
}

impl Db {
    /// A byte range of a bytes or string cell. Only the chunks the range
    /// covers are read.
    pub async fn read_value_range(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, Error> {
        self.fetching(async || {
            self.inner()
                .await
                .read_range(table, row, column, range.clone(), false)
                .await
        })
        .await
    }

    /// `delta(a, b)` (SPEC 19.6): the edits that turn the value `column` of
    /// `row` holds in version `from` into its value in version `to`, for a
    /// reader allowed to read both versions. Only changed regions are read.
    pub async fn value_delta(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        from: Pos,
        to: Pos,
    ) -> Result<Vec<LargeValueDeltaSplice>, Error> {
        self.fetching(async || {
            let inner = self.inner().await;
            inner
                .value_delta(
                    table,
                    row,
                    column,
                    ((from, None), (to, None)),
                    inner.reader(),
                    &inner.schema,
                )
                .await
        })
        .await
    }

    /// A UTF-16 code-unit range of a string cell. A boundary inside a
    /// surrogate pair fails.
    pub async fn read_text_utf16_range(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        range: std::ops::Range<u64>,
    ) -> Result<String, Error> {
        let bytes = self
            .fetching(async || {
                self.inner()
                    .await
                    .read_range(table, row, column, range.clone(), true)
                    .await
            })
            .await?;
        String::from_utf8(bytes).map_err(|_| query("UTF-16 range splits a surrogate pair"))
    }

    /// The value at a JSON pointer of a JSON cell, reading the document
    /// only as far as the pointer needs when it starts with an array index.
    pub async fn read_json_pointer(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        pointer: &str,
    ) -> Result<Option<serde_json::Value>, Error> {
        self.fetching(async || self.json_pointer(table, row, column, pointer).await)
            .await
    }

    async fn json_pointer(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        pointer: &str,
    ) -> Result<Option<serde_json::Value>, Error> {
        let inner = self.inner().await;
        let reader = inner.reader();
        let (col, cell) = inner.physical_cell(table, row, column, reader).await?;
        if crate::large::kind(&col) != lv::LargeValueKind::Json {
            return Err(Error::new(
                ErrorCode::Schema,
                "JSON pointer selection requires a JSON column",
            ));
        }
        if !large_cell(&cell) {
            return Ok(None);
        }
        inner
            .cell_pointer(&mut inner.conn.sql(), cell.as_ref(), pointer)
            .await
    }

    /// Append bytes (UTF-8 for a string) to a cell, as one row update.
    pub async fn append_value(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        bytes: Vec<u8>,
    ) -> Result<WriteHandle, Error> {
        self.append_value_with(table, row, column, bytes, UpdateOptions::default())
            .await
    }

    /// [`Self::append_value`] with update options.
    pub async fn append_value_with(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        bytes: Vec<u8>,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        self.fetching_with((Edit::Append(bytes), opts), async |(edit, opts)| {
            self.edit_cell(table, row, column, edit, opts).await
        })
        .await
    }

    /// Replace `splice.delete` bytes at byte offset `splice.at` of a cell
    /// with `splice.insert`, as one row update.
    pub async fn splice_value(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        splice: LargeValueUpdateSplice,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        let edit = Edit::Splice {
            offset: splice.at,
            delete: splice.delete,
            insert: splice.insert,
        };
        self.fetching_with((edit, opts), async |(edit, opts)| {
            self.edit_cell(table, row, column, edit, opts).await
        })
        .await
    }

    async fn edit_cell(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        edit: Edit,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        let value = {
            let mut inner = self.inner().await;
            inner.require_mutation_admission()?;
            let edited = (async {
                let (col, cell) = inner
                    .physical_cell(table, row, column, identity_of(opts.identity))
                    .await?;
                Ok(nullable(&col, inner.edited(table, &col, cell, edit).await?))
            })
            .await;
            match edited {
                Ok(value) => value,
                Err(error) => {
                    // Nothing derived survives a failed edit (which may be
                    // retried once its chunks are fetched).
                    inner.clear_derived();
                    return Err(error);
                }
            }
        };
        let cells = BTreeMap::from([(column.to_owned(), value)]);
        let result = self
            .write_op(
                StagedOp::Update(table.to_owned(), row, cells, opts.clone()),
                who(opts.identity),
            )
            .await;
        self.inner().await.clear_derived();
        result
    }

    /// Apply partial large-value updates together with an ordinary patch,
    /// as one row update. A failing update leaves the row unchanged.
    pub async fn update_with_large_value_mutations(
        &self,
        table: &str,
        row: RowUuid,
        patch: BTreeMap<String, Value>,
        mutations: Vec<LargeValueUpdate>,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        self.fetching_with(
            (patch, mutations, opts),
            async |(patch, mutations, opts)| {
                self.update_with_mutations(table, row, patch, mutations, opts)
                    .await
            },
        )
        .await
    }

    async fn update_with_mutations(
        &self,
        table: &str,
        row: RowUuid,
        mut patch: BTreeMap<String, Value>,
        mutations: Vec<LargeValueUpdate>,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        let lowered = {
            let mut inner = self.inner().await;
            inner.require_mutation_admission()?;
            let identity = identity_of(opts.identity);
            inner
                .lower_large_mutations(table, row, &mut patch, mutations, identity)
                .await
        };
        if let Err(error) = lowered {
            self.inner().await.clear_derived();
            return Err(error);
        }
        let result = self
            .write_op(
                StagedOp::Update(table.to_owned(), row, patch, opts.clone()),
                who(opts.identity),
            )
            .await;
        self.inner().await.clear_derived();
        result
    }

    /// Insert a row whose `column` is read from `reader`, chunked as it
    /// streams: the logical value is never held whole. Each finalized node
    /// is stored as it is produced, held by the upload's lease until the
    /// row commits (see [`Self::begin_streaming_value_upload`]).
    pub async fn insert_streaming_value<R: std::io::Read>(
        &self,
        table: &str,
        cells: BTreeMap<String, Value>,
        column: &str,
        reader: R,
    ) -> Result<WriteHandle, Error> {
        let row = self.inner().await.ids.next_row_id();
        self.insert_streaming_value_with_id(table, row, cells, column, reader)
            .await
    }

    /// [`Self::insert_streaming_value`] with a caller-chosen row id.
    pub async fn insert_streaming_value_with_id<R: std::io::Read>(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        column: &str,
        reader: R,
    ) -> Result<WriteHandle, Error> {
        self.write_streaming_value_with_id(
            StreamingMutationKind::Insert,
            table,
            row,
            cells,
            column,
            reader,
            None,
            None,
            None,
            None,
        )
        .await
    }

    /// Complete an insert, update or upsert whose `column` is read from
    /// `reader`, with the identity, timestamp and branch view the ordinary
    /// mutation would take.
    #[allow(clippy::too_many_arguments)]
    pub async fn write_streaming_value_with_id<R: std::io::Read>(
        &self,
        mutation: StreamingMutationKind,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        column: &str,
        mut reader: R,
        identity: Option<AuthorSubject>,
        now_ms: Option<u64>,
        head: Option<BranchSelector>,
        base: Option<BranchViewBase>,
    ) -> Result<WriteHandle, Error> {
        let mut upload = self.begin_streaming_value_upload(table, &cells, column)?;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    self.push_streaming_value_upload(&mut upload, &buffer[..read])
                        .await?
                }
                Err(error) => {
                    self.abort_streaming_value_upload(upload).await?;
                    return Err(Error::new(
                        ErrorCode::WriteRejected,
                        format!("streaming value reader failed: {error}"),
                    ));
                }
            }
        }
        self.finish_streaming_value_upload(
            upload,
            mutation,
            table,
            row,
            cells,
            column,
            identity.map_or(WriteIdentity::Database, WriteIdentity::Session),
            now_ms,
            head,
            base,
        )
        .await
    }

    /// Begin a resumable upload of one large value, for hosts that feed it
    /// in pieces (such as an asynchronous browser stream). The handle is bound
    /// to this database, exact schema view, table and column. Nothing is stored
    /// until the first push.
    pub fn begin_streaming_value_upload(
        &self,
        table: &str,
        cells: &BTreeMap<String, Value>,
        column: &str,
    ) -> Result<StreamingValueUpload, Error> {
        self.metadata().require_mutation_admission()?;
        let col = streaming_column(&self.metadata(), table, column)?;
        if cells.contains_key(column) {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("{table}.{column} is streamed, so the other cells cannot also set it"),
            ));
        }
        let kind = crate::large::kind(&col);
        let emitted = Rc::new(RefCell::new(Vec::new()));
        let stage = Rc::clone(&emitted);
        let id = upload_id();
        let schema_instance = {
            let inner = self.inner.metadata();
            Rc::downgrade(self.view.as_ref().unwrap_or(&inner.owner))
        };
        Ok(StreamingValueUpload {
            id,
            owner: Rc::downgrade(&self.inner),
            schema: self.schema_view_id(),
            schema_instance,
            table: table.to_owned(),
            column: column.to_owned(),
            _lease: UploadLease {
                id,
                cleanup: self.metadata().upload_cleanup.clone(),
            },
            kind,
            initialized: false,
            preparation: Some(lv::PushStreamingPreparation::new(
                kind,
                Box::new(move |chunk| {
                    stage.borrow_mut().push(chunk);
                    Ok(())
                }),
            )),
            emitted,
        })
    }

    /// Feed one piece of an upload, storing every node it finalizes. A
    /// piece over the staging policy's ingress budget, or one that fails
    /// validation, closes the upload and releases what it stored.
    pub async fn push_streaming_value_upload(
        &self,
        upload: &mut StreamingValueUpload,
        bytes: &[u8],
    ) -> Result<(), Error> {
        self.check_streaming_upload(upload)?;
        let inner = self.inner().await;
        inner.require_mutation_admission()?;
        let now = streaming_now_ms();
        if upload.preparation.is_none() {
            return Err(closed());
        }
        if !upload.initialized {
            crate::large::begin_upload(&mut inner.conn.sql(), &upload.id, now).await?;
            upload.initialized = true;
        } else if !crate::large::touch_upload(&mut inner.conn.sql(), &upload.id, now).await? {
            upload.preparation.take();
            return Err(expired());
        }
        let Some(preparation) = upload.preparation.as_mut() else {
            return Err(closed());
        };
        let staged = (async {
            let staged = preparation
                .push(bytes)
                .map_err(|e| rejected(e.to_string()))
                .map(|()| std::mem::take(&mut *upload.emitted.borrow_mut()))?;
            {
                admit_ingress(&inner, &staged, now)?;
                inner
                    .stage_upload(&upload.id, &staged, inner.client.is_some())
                    .await
            }
        })
        .await;
        if let Err(error) = staged {
            upload.preparation.take();
            crate::large::end_upload(&mut inner.conn.sql(), &upload.id).await?;
            return Err(error);
        }
        Ok(())
    }

    /// Abandon an upload and release what it stored.
    pub async fn abort_streaming_value_upload(
        &self,
        mut upload: StreamingValueUpload,
    ) -> Result<(), Error> {
        self.check_streaming_upload(&mut upload)?;
        upload.preparation.take();
        crate::large::end_upload(&mut self.inner().await.conn.sql(), &upload.id).await
    }

    fn check_streaming_upload(&self, upload: &mut StreamingValueUpload) -> Result<(), Error> {
        let mismatch = || schema_error("streaming upload belongs to another database or schema");
        if !upload.owner.ptr_eq(&Rc::downgrade(&self.inner)) {
            return Err(mismatch());
        }
        let inner = self.inner.metadata();
        let schema = self.view.as_ref().unwrap_or(&inner.owner);
        let instance = Rc::downgrade(schema);
        if !upload.schema_instance.ptr_eq(&instance) {
            // An equivalent registered handle is allowed. Remember it so each
            // subsequent chunk does not serialize the whole schema again.
            if upload.schema != SchemaViewId::for_schema(&schema.jazz) {
                return Err(mismatch());
            }
            upload.schema_instance = instance;
        }
        Ok(())
    }

    /// Finish an upload and publish its row mutation. A committed row holds
    /// its value from then on. Consuming a failed upload schedules any remaining
    /// lease cleanup for the next maintenance pass.
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_streaming_value_upload(
        &self,
        mut upload: StreamingValueUpload,
        mutation: StreamingMutationKind,
        table: &str,
        row: RowUuid,
        mut cells: BTreeMap<String, Value>,
        column: &str,
        identity: WriteIdentity,
        now_ms: Option<u64>,
        head: Option<BranchSelector>,
        base: Option<BranchViewBase>,
    ) -> Result<WriteHandle, Error> {
        if let Some(value) = self
            .seal_streaming_upload(&mut upload, table, &mut cells, column)
            .await?
        {
            self.inner().await.derived.push(value);
        }
        let result = match mutation {
            StreamingMutationKind::Insert if base.is_some() => Err(Error::new(
                ErrorCode::WriteRejected,
                "a streamed insert writes one exact branch, not a branch view",
            )),
            StreamingMutationKind::Insert => {
                self.insert(
                    table,
                    cells,
                    InsertOptions {
                        row_id: Some(row),
                        identity,
                        target: head.map_or(ExactWriteTarget::Root, ExactWriteTarget::Branch),
                        updated_at_ms: now_ms,
                    },
                )
                .await
            }
            StreamingMutationKind::Update => {
                let target = view_target(head, base);
                self.update(
                    table,
                    row,
                    cells,
                    UpdateOptions {
                        identity,
                        target,
                        updated_at_ms: now_ms,
                    },
                )
                .await
            }
            StreamingMutationKind::Upsert => {
                let target = view_target(head, base);
                self.upsert(
                    table,
                    row,
                    cells,
                    UpsertOptions {
                        identity,
                        target,
                        updated_at_ms: now_ms,
                    },
                )
                .await
            }
        };
        let mut inner = self.inner().await;
        inner.clear_derived();
        crate::large::end_upload(&mut inner.conn.sql(), &upload.id).await?;
        result
    }

    /// Finish `upload`'s value and set it as `column` of `cells`: inline when
    /// it fits, else the chunked descriptor, returned for the write to trust.
    /// A failed upload is ended here.
    async fn seal_streaming_upload(
        &self,
        upload: &mut StreamingValueUpload,
        table: &str,
        cells: &mut BTreeMap<String, Value>,
        column: &str,
    ) -> Result<Option<lv::LargeValueRef>, Error> {
        self.inner().await.require_mutation_admission()?;
        self.check_streaming_upload(upload)?;
        if upload.table != table || upload.column != column {
            return Err(schema_error(
                "streaming upload target differs from its original table or column",
            ));
        }
        if cells.contains_key(column) {
            return Err(rejected(
                "the streamed column is also set in the other cells",
            ));
        }
        let value = {
            let inner = self.inner().await;
            let now = streaming_now_ms();
            let alive = if upload.initialized {
                crate::large::touch_upload(&mut inner.conn.sql(), &upload.id, now).await?
            } else {
                crate::large::begin_upload(&mut inner.conn.sql(), &upload.id, now).await?;
                true
            };
            let finished = match upload.preparation.take() {
                _ if !alive => Err(expired()),
                None => Err(closed()),
                Some(preparation) => preparation.finish().map_err(|e| rejected(e.to_string())),
            };
            let staged = std::mem::take(&mut *upload.emitted.borrow_mut());
            let value = (async {
                let (value, _) = finished?;
                {
                    let col = streaming_column(&inner, table, column)?;
                    if crate::large::kind(&col) != upload.kind {
                        return Err(not_large(table, column));
                    }
                    admit_ingress(&inner, &staged, now)?;
                    inner
                        .stage_upload(&upload.id, &staged, inner.client.is_some())
                        .await?;
                    crate::large::finish_upload(&mut inner.conn.sql(), &value).await?;
                    Ok((col, value))
                }
            })
            .await;
            match value {
                Ok(value) => value,
                Err(error) => {
                    crate::large::end_upload(&mut inner.conn.sql(), &upload.id).await?;
                    return Err(error);
                }
            }
        };
        let (col, value) = value;
        // A value that fits inline is written as an ordinary inline cell, as
        // the store keeps it: its commit carries the bytes, not a
        // descriptor of chunks the store then reclaims.
        let (logical, large) = if value.byte_length <= crate::large::inline_max(&col) as u64 {
            let bytes = {
                let inner = self.inner().await;
                crate::large::value_bytes(&mut inner.conn.sql(), &inner.chunks, &value).await?
            };
            (logical_value(value.kind, bytes)?, None)
        } else {
            (Value::Large(Box::new(value.clone())), Some(value))
        };
        cells.insert(column.to_owned(), nullable(&col, logical));
        Ok(large)
    }

    /// Replace the limits on local streaming uploads.
    pub fn set_large_value_staging_policy(&self, policy: LargeValueStagingPolicy) {
        self.metadata().staging = policy;
    }

    /// End every streaming upload left untouched past the staging policy's
    /// maximum age, and every pushed-node receipt past it; returns how
    /// many uploads ended.
    pub async fn evict_expired_staged_large_values(&self) -> Result<usize, Error> {
        let inner = self.inner().await;
        let now = streaming_now_ms();
        let max_age = i64::try_from(inner.staging.max_age_ms).unwrap_or(i64::MAX);
        inner.release_dropped_uploads().await?;
        crate::large::expire_receipts(&mut inner.conn.sql(), now).await?;
        crate::large::expire_uploads(&mut inner.conn.sql(), now.saturating_sub(max_age)).await
    }
}

/// How a finished streaming upload publishes its row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamingMutationKind {
    /// Create a new row.
    Insert,
    /// Patch an existing row visible in the selected branch view.
    Update,
    /// Patch the visible row or create it when absent.
    Upsert,
}

/// Limits on local streaming uploads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LargeValueStagingPolicy {
    /// Encoded node bytes all uploads may store per window.
    pub incoming_bytes_per_window: u64,
    /// The fixed rate-limit window, in milliseconds.
    pub window_ms: u64,
    /// How long an untouched upload (or pushed node awaiting its commit)
    /// is held before maintenance releases it.
    pub max_age_ms: u64,
}

impl Default for LargeValueStagingPolicy {
    fn default() -> Self {
        Self {
            // One maximum-size logical wire message per second.
            incoming_bytes_per_window: 256 * 1024 * 1024,
            window_ms: 1_000,
            max_age_ms: 10 * 60 * 1_000,
        }
    }
}

type Stage = Box<dyn FnMut(lv::StagedChunk) -> Result<(), lv::Error>>;

/// A fresh upload lease id: random, so leases never collide.
fn upload_id() -> [u8; 32] {
    let mut id = [0; 32];
    id[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    id[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    id
}

/// A resumable upload of one large value: the tree under construction and
/// the lease that holds its stored nodes. Dropping a handle schedules its
/// remaining lease for cleanup by the original owner, never by another store.
pub struct StreamingValueUpload {
    id: [u8; 32],
    owner: std::rc::Weak<Store>,
    schema: SchemaViewId,
    schema_instance: std::rc::Weak<Schema>,
    table: String,
    column: String,
    _lease: UploadLease,
    kind: lv::LargeValueKind,
    initialized: bool,
    preparation: Option<lv::PushStreamingPreparation<Stage>>,
    emitted: Rc<RefCell<Vec<lv::StagedChunk>>>,
}

/// Drop schedules cleanup on the next maintenance tick without borrowing the
/// database recursively. The existing persisted lease handles crash recovery.
pub(crate) struct UploadLease {
    id: [u8; 32],
    cleanup: Rc<RefCell<Vec<[u8; 32]>>>,
}

impl Drop for UploadLease {
    fn drop(&mut self) {
        self.cleanup.borrow_mut().push(self.id);
    }
}

/// Renew the leases a batch holds before it commits; a lease that expired
/// while the batch was open refuses the commit, as its chunks may be gone.
pub(crate) async fn renew_upload_leases(
    inner: &Inner,
    leases: &[UploadLease],
) -> Result<(), Error> {
    let now = streaming_now_ms();
    for lease in leases {
        if !crate::large::touch_upload(&mut inner.conn.sql(), &lease.id, now).await? {
            return Err(expired());
        }
    }
    Ok(())
}

fn streaming_column(
    inner: &Inner,
    table: &str,
    column: &str,
) -> Result<crate::values::Column, Error> {
    inner
        .schema
        .table(table)?
        .column(column)
        .filter(|c| crate::large::large_capable(c))
        .cloned()
        .ok_or_else(|| not_large(table, column))
}

fn view_target(head: Option<BranchSelector>, base: Option<BranchViewBase>) -> WriteTarget {
    match head {
        Some(head) => WriteTarget::BranchView { head, base },
        None => WriteTarget::Root,
    }
}

/// Charge `staged` to the staging policy's ingress window.
fn admit_ingress(inner: &Inner, staged: &[lv::StagedChunk], now: i64) -> Result<(), Error> {
    let bytes: u64 = staged.iter().map(|chunk| chunk.encoded.len() as u64).sum();
    if bytes == 0 {
        return Ok(());
    }
    let policy = inner.staging;
    let mut window = inner.ingress.borrow_mut();
    let now = now.max(0) as u64;
    if now.saturating_sub(window.0) >= policy.window_ms {
        *window = (now, 0);
    }
    if window.1.saturating_add(bytes) > policy.incoming_bytes_per_window {
        return Err(Error::new(
            ErrorCode::WriteRejected,
            "streaming upload exceeds the large-value ingress rate limit",
        ));
    }
    window.1 += bytes;
    Ok(())
}

fn streaming_now_ms() -> i64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

fn expired() -> Error {
    Error::new(
        ErrorCode::WriteRejected,
        "streaming upload expired: it went untouched past the staging policy's maximum age",
    )
}

fn closed() -> Error {
    Error::new(
        ErrorCode::WriteRejected,
        "streaming upload is closed: an earlier push or finish ended it",
    )
}

/// Splices in a page's coordinates, lowered to byte coordinates of the
/// current value. Each later splice is relative to the value the earlier
/// ones produced.
async fn lower_splices(
    inner: &Inner,
    column: &crate::values::Column,
    cell: &Sql,
    within: &LargeValueUpdatePage,
    splices: Vec<LargeValueUpdateSplice>,
) -> Result<Vec<Edit>, Error> {
    let kind = crate::large::kind(column);
    if !large_cell(cell) {
        return Err(query("partial splice requires a bytes or string cell"));
    }
    let raw = cell.as_ref();
    let (from, to, utf16) = match *within {
        LargeValueUpdatePage::Bytes { from, to } if kind == lv::LargeValueKind::Bytes => {
            (from, to, false)
        }
        LargeValueUpdatePage::TextUtf8 { from, to } if kind != lv::LargeValueKind::Bytes => {
            (from, to, false)
        }
        LargeValueUpdatePage::TextUtf16 { from, to } if kind != lv::LargeValueKind::Bytes => {
            (from, to, true)
        }
        _ => return Err(query("splice page does not match the column's kind")),
    };
    let current = crate::large::logical(&mut inner.conn.sql(), &inner.chunks, raw).await?;
    let len = match (&current, utf16) {
        (Logical::Chunked(value), true) => value.utf16_length.unwrap_or(0),
        (Logical::Inline(bytes), true) => std::str::from_utf8(bytes)
            .map_err(|_| query("cell is not text"))?
            .encode_utf16()
            .count() as u64,
        (value, false) => value.len(),
    };
    check_range(&(from..to), len)?;
    if utf16 && !splices.is_empty() {
        // UTF-16 positions shift with every splice; lower them one at a time
        // against the text each produced.
        let mut text = String::from_utf8(
            crate::large::materialize(&mut inner.conn.sql(), &inner.chunks, raw).await?,
        )
        .map_err(|_| query("cell is not text"))?;
        let mut page_len = to - from;
        let mut edits = Vec::new();
        for splice in splices {
            let end = splice
                .at
                .checked_add(splice.delete)
                .ok_or_else(|| query("splice range overflows"))?;
            if end > page_len {
                return Err(query("splice deletion leaves the selected page"));
            }
            let (Some(start), Some(stop)) = (
                utf16_byte(&text, from + splice.at),
                utf16_byte(&text, from + end),
            ) else {
                return Err(query("UTF-16 splice splits a surrogate pair"));
            };
            let insert = String::from_utf8(splice.insert)
                .map_err(|_| query("text splice is not valid UTF-8"))?;
            let inserted = insert.encode_utf16().count() as u64;
            text.replace_range(start..stop, &insert);
            page_len = page_len - splice.delete + inserted;
            edits.push(Edit::Splice {
                offset: start as u64,
                delete: (stop - start) as u64,
                insert: insert.into_bytes(),
            });
        }
        return Ok(edits);
    }
    let mut page_len = to - from;
    let mut edits = Vec::new();
    for splice in splices {
        let end = splice
            .at
            .checked_add(splice.delete)
            .ok_or_else(|| query("splice range overflows"))?;
        if end > page_len {
            return Err(query("splice deletion leaves the selected page"));
        }
        page_len = page_len - splice.delete + splice.insert.len() as u64;
        edits.push(Edit::Splice {
            offset: from + splice.at,
            delete: splice.delete,
            insert: splice.insert,
        });
    }
    Ok(edits)
}

impl Inner {
    /// Lower partial large-value updates into `patch` against the row as
    /// this store sees it now (inside a batch: with its staged writes).
    /// What the edits build is left in `derived` and `pending_nodes`.
    pub(crate) async fn lower_large_mutations(
        &mut self,
        table: &str,
        row: RowUuid,
        patch: &mut BTreeMap<String, Value>,
        mutations: Vec<LargeValueUpdate>,
        identity: Option<AuthorSubject>,
    ) -> Result<(), Error> {
        for mutation in &mutations {
            let column = mutation.column();
            if patch.contains_key(column) {
                return Err(query(format!(
                    "`{column}` is both patched and partially updated"
                )));
            }
        }
        for mutation in mutations {
            let column = mutation.column().to_owned();
            let (col, mut cell) = self.physical_cell(table, row, &column, identity).await?;
            let value = match mutation {
                LargeValueUpdate::Splice {
                    within, splices, ..
                } => {
                    let mut value = None;
                    for splice in lower_splices(self, &col, &cell, &within, splices).await? {
                        let next = self.edited(table, &col, cell.clone(), splice).await?;
                        cell = match &next {
                            Value::Large(large) => {
                                self.large_cell(large, crate::large::inline_max(&col))
                                    .await?
                            }
                            other => to_sql(other)?.ok_or_else(|| unsupported("NaN"))?,
                        };
                        value = Some(next);
                    }
                    match value {
                        Some(value) => value,
                        None => continue,
                    }
                }
                LargeValueUpdate::JsonSet { edits, .. } => {
                    if crate::large::kind(&col) != lv::LargeValueKind::Json {
                        return Err(Error::new(
                            ErrorCode::Schema,
                            format!("{table}.{column} is not a JSON column"),
                        ));
                    }
                    if !large_cell(&cell) {
                        return Err(query(format!("{table}.{column} is null")));
                    }
                    let mut json: serde_json::Value = serde_json::from_slice(
                        &crate::large::materialize(
                            &mut self.conn.sql(),
                            &self.chunks,
                            cell.as_ref(),
                        )
                        .await?,
                    )
                    .map_err(|e| query(format!("stored JSON: {e}")))?;
                    for edit in edits {
                        let slot = json.pointer_mut(&edit.at).ok_or_else(|| {
                            query(format!(
                                "JSON pointer `{}` names no existing value",
                                edit.at
                            ))
                        })?;
                        *slot = edit.value;
                    }
                    Value::String(json.to_string())
                }
                LargeValueUpdate::TextEdit {
                    text, edited_from, ..
                } => {
                    let Some(format) = col.merge.text_format() else {
                        return Err(Error::new(
                            ErrorCode::Schema,
                            format!("{table}.{column} does not merge text"),
                        ));
                    };
                    let current = if large_cell(&cell) {
                        String::from_utf8(
                            crate::large::materialize(
                                &mut self.conn.sql(),
                                &self.chunks,
                                cell.as_ref(),
                            )
                            .await?,
                        )
                        .map_err(|_| query(format!("{table}.{column} is not text")))?
                    } else {
                        String::new()
                    };
                    let value = if current == edited_from {
                        text
                    } else {
                        jazz_text::merge3_within(
                            format,
                            &edited_from,
                            &current,
                            &text,
                            self.merge_budget,
                        )
                    };
                    Value::String(value)
                }
            };
            patch.insert(column, nullable(&col, value));
        }
        Ok(())
    }
}

impl TxRef<'_> {
    /// [`Db::finish_streaming_value_upload`] staged in an open batch. The
    /// batch holds the upload's lease, so the streamed chunks stay until it
    /// commits or is abandoned; a commit after the lease expired (untouched
    /// past the staging policy's maximum age) is refused.
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_streaming_value_upload(
        &self,
        mut upload: StreamingValueUpload,
        mutation: StreamingMutationKind,
        table: &str,
        row: RowUuid,
        mut cells: BTreeMap<String, Value>,
        column: &str,
        identity: WriteIdentity,
        now_ms: Option<u64>,
        head: Option<BranchSelector>,
        base: Option<BranchViewBase>,
    ) -> Result<WriteHandle, Error> {
        if mutation == StreamingMutationKind::Insert && base.is_some() {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                "a streamed insert writes one exact branch, not a branch view",
            ));
        }
        let large = self
            .db
            .seal_streaming_upload(&mut upload, table, &mut cells, column)
            .await?;
        let op = match mutation {
            StreamingMutationKind::Insert => StagedOp::Insert(
                table.to_owned(),
                cells,
                InsertOptions {
                    row_id: Some(row),
                    identity,
                    target: head.map_or(ExactWriteTarget::Root, ExactWriteTarget::Branch),
                    updated_at_ms: now_ms,
                },
            ),
            StreamingMutationKind::Update => StagedOp::Update(
                table.to_owned(),
                row,
                cells,
                UpdateOptions {
                    identity,
                    target: view_target(head, base),
                    updated_at_ms: now_ms,
                },
            ),
            StreamingMutationKind::Upsert => StagedOp::Upsert(
                table.to_owned(),
                row,
                cells,
                BTreeMap::new(),
                UpsertOptions {
                    identity,
                    target: view_target(head, base),
                    updated_at_ms: now_ms,
                },
            ),
        };
        let held = large.is_some();
        let edits = crate::db::LargeEdits {
            derived: large.into_iter().collect(),
            pending_nodes: Vec::new(),
        };
        self.db.stage_with_edits(self.id, op, edits).await?;
        // An inline value is in its cell: its upload ends as the handle drops.
        if held {
            self.db.hold_upload(self.id, upload._lease)?;
        }
        Ok(WriteHandle {
            row,
            seq: Pos::default(),
            tx: 0,
        })
    }

    /// [`Db::update_with_large_value_mutations`] staged in an open batch:
    /// the edits read the row with the batch's earlier writes applied.
    pub async fn update_with_large_value_mutations(
        &self,
        table: &str,
        row: RowUuid,
        mut patch: BTreeMap<String, Value>,
        mutations: Vec<LargeValueUpdate>,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        self.db.inner().await.require_mutation_admission()?;
        let edits = self
            .db
            .in_overlay(self.id, None, async |inner, _| {
                let identity = identity_of(opts.identity);
                let lowered = inner
                    .lower_large_mutations(table, row, &mut patch, mutations, identity)
                    .await;
                let edits = crate::db::LargeEdits {
                    derived: std::mem::take(&mut inner.derived),
                    pending_nodes: std::mem::take(&mut inner.pending_nodes),
                };
                lowered.map(|()| edits)
            })
            .await?;
        self.db
            .stage_with_edits(
                self.id,
                StagedOp::Update(table.to_owned(), row, patch, opts),
                edits,
            )
            .await?;
        Ok(WriteHandle {
            row,
            seq: Pos::default(),
            tx: 0,
        })
    }
}

/// Whether `cell` holds a string, JSON or bytes value: inline or chunked.
fn large_cell(cell: &Sql) -> bool {
    matches!(cell, Sql::Text(_) | Sql::Blob(_) | Sql::Integer(_))
}

/// The splices that turn `a` into `b`: none when they are equal, else one
/// replace of the bytes between their common prefix and suffix, kept on
/// character boundaries for text.
pub(crate) fn byte_splices(a: &[u8], b: &[u8], text: bool) -> Vec<LargeValueDeltaSplice> {
    let boundary = |bytes: &[u8], at: usize| at == bytes.len() || bytes[at] & 0xc0 != 0x80;
    let mut prefix = a.iter().zip(b).take_while(|(x, y)| x == y).count();
    let mut suffix = a[prefix..]
        .iter()
        .rev()
        .zip(b[prefix..].iter().rev())
        .take_while(|(x, y)| x == y)
        .count();
    if text {
        while prefix > 0 && !boundary(a, prefix) {
            prefix -= 1;
        }
        while suffix > 0 && !boundary(a, a.len() - suffix) {
            suffix -= 1;
        }
    }
    let (deleted, inserted) = (&a[prefix..a.len() - suffix], &b[prefix..b.len() - suffix]);
    if deleted.is_empty() && inserted.is_empty() {
        return Vec::new();
    }
    let units = |bytes: &[u8]| {
        if text {
            std::str::from_utf8(bytes).map_or(0, |s| s.encode_utf16().count() as u64)
        } else {
            0
        }
    };
    vec![LargeValueDeltaSplice {
        at: prefix as u64,
        delete: deleted.len() as u64,
        insert: inserted.to_vec(),
        at_utf16: units(&a[..prefix]),
        delete_utf16: units(deleted),
    }]
}

/// The UTF-16 length of inline text `bytes`; zero for bytes columns.
fn utf16_len(bytes: &[u8], text: bool) -> Result<u64, Error> {
    if !text {
        return Ok(0);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| query("cell is not text"))?;
    Ok(text.encode_utf16().count() as u64)
}
