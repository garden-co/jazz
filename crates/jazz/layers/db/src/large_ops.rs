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
use crate::storage::OptionalExtension;
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

impl LargeValueUpdatePage {
    /// The UTF-8 byte page `range` of a text value: offsets are `str`
    /// indices, and each splice in it must start and end on a char
    /// boundary.
    pub fn text(range: std::ops::Range<usize>) -> Self {
        Self::TextUtf8 {
            from: range.start as u64,
            to: range.end as u64,
        }
    }

    /// The byte page `range` of a bytes value.
    pub fn bytes(range: std::ops::Range<usize>) -> Self {
        Self::Bytes {
            from: range.start as u64,
            to: range.end as u64,
        }
    }
}

/// One sequential splice, in the coordinates of its page.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LargeValueUpdateSplice {
    /// Page-relative start, in the page's units (bytes, UTF-8 bytes or
    /// UTF-16 code units).
    pub at: u64,
    /// Page units to remove.
    pub delete: u64,
    /// Replacement bytes, UTF-8 for text pages.
    pub insert: Vec<u8>,
}

impl LargeValueUpdateSplice {
    /// Replaces `delete` units at `at` with `insert`: UTF-8 bytes in a
    /// [`LargeValueUpdatePage::text`] page.
    pub fn text(at: usize, delete: usize, insert: &str) -> Self {
        Self {
            at: at as u64,
            delete: delete as u64,
            insert: insert.as_bytes().to_vec(),
        }
    }
}

/// How the error for a UTF-8 text splice offset that is not on a char
/// boundary begins.
pub const NOT_CHAR_BOUNDARY: &str = "UTF-8 splice offset is not on a char boundary";

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

/// `range` clamped to `len` units (SPEC 19.12): the part of the range the
/// value covers, empty at the end when the range starts past it. An
/// inverted range still fails.
fn clamp_range(range: std::ops::Range<u64>, len: u64) -> Result<std::ops::Range<u64>, Error> {
    if range.start > range.end {
        return Err(query("value range is out of bounds"));
    }
    let end = range.end.min(len);
    Ok(range.start.min(end)..end)
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

#[derive(Clone)]
pub(super) struct PreparedCell {
    query: PreparedQuery,
    column: crate::values::Column,
}

impl PreparedCell {
    pub(super) fn new(
        schema: Rc<Schema>,
        table: &str,
        row: RowUuid,
        column: &str,
    ) -> Result<Self, Error> {
        let t = schema.table(table)?;
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
            schema: Some(schema),
            planned: None,
        };
        Ok(Self {
            query: prepared,
            column: col,
        })
    }
}

impl Store {
    /// Drop queues contain only this owner's uploads. SQL errors retain the
    /// receipt for the next maintenance pass; crash recovery uses lease expiry.
    pub(super) async fn release_dropped_uploads(&self, conn: &Storage) -> Result<(), Error> {
        // Removing an in-memory receipt must not outlive a SQL rollback.
        // Callback transactions can tick while suspended; defer cleanup until
        // its deletes can commit independently of that application unit.
        if !conn.is_autocommit() {
            return Ok(());
        }
        loop {
            let next = self
                .upload_cleanup
                .borrow()
                .iter()
                .copied()
                .enumerate()
                .next_back();
            let Some((index, id)) = next else { break };
            crate::large::end_upload(conn, &id).await?;
            // Dropped handles may have appended other receipts while SQL ran.
            self.upload_cleanup.borrow_mut().swap_remove(index);
        }
        Ok(())
    }
}

impl Store {
    /// Forget what the finished write's edits derived.
    pub(crate) fn clear_derived(&self) {
        self.pending_nodes.borrow_mut().clear();
        self.derived.borrow_mut().clear();
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
        cell: &PreparedCell,
        identity: Option<AuthorSubject>,
    ) -> Result<(crate::values::Column, Sql), Error> {
        whole_only(self, &cell.query.query.table, &cell.column.name)?;
        let compiled = self.compile(
            &cell.query,
            identity,
            &ReadSpec::default(),
            self.read_namespace(&cell.query).await?,
        )?;
        let value: Option<Sql> = (async {
            let mut statement = self.conn.prepare_cached(&compiled.sql)?;
            {
                statement
                    .query_row(params_from_iter(compiled.params.iter()), |r| r.get(3))
                    .await
                    .optional()
            }
        })
        .await
        .map_err(storage)?;
        let value = value.ok_or_else(|| {
            Error::new(ErrorCode::NotObserved, "large-value cell is not observed")
        })?;
        Ok((cell.column.clone(), value))
    }

    async fn read_range(
        &self,
        prepared: &PreparedCell,
        range: std::ops::Range<u64>,
        utf16: bool,
    ) -> Result<Vec<u8>, Error> {
        let (col, cell) = self.physical_cell(prepared, self.reader()).await?;
        if !large_cell(&cell) {
            return Err(not_large(
                &prepared.query.query.table,
                &prepared.column.name,
            ));
        }
        if utf16 && crate::large::kind(&col) == lv::LargeValueKind::Bytes {
            return Err(not_large(
                &prepared.query.query.table,
                &prepared.column.name,
            ));
        }
        self.cell_range(cell.as_ref(), range, utf16, false).await
    }

    /// `delta(a, b)` (SPEC 19.6) between the values `column` of `row` holds
    /// in its versions `from` and `to`, for a reader allowed to read both by
    /// `policies` (a pinned session's bundle, or the current schema).
    /// Only the chunks in changed regions are read.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn value_delta(
        &self,
        t: &Table,
        row: RowUuid,
        column: &str,
        (from, to): (sync::Rank, sync::Rank),
        reader: Option<AuthorSubject>,
        policies: &Schema,
    ) -> Result<Vec<LargeValueDeltaSplice>, Error> {
        let table = &t.name;
        whole_only(self, table, column)?;
        let governed = policies.by_stored(&t.stored).ok_or_else(|| {
            Error::new(ErrorCode::NotObserved, "the row is not readable".to_owned())
        })?;
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
            chunks: &crate::large::Chunks,
            cell: &'a Sql,
        ) -> Result<Logical<'a>, Error> {
            match cell {
                Sql::Null => Ok(Logical::Inline(&[][..])),
                cell => chunks.logical(cell.as_ref()).await,
            }
        }
        let (a, b) = (
            logical(&self.chunks, &cells[0]).await?,
            logical(&self.chunks, &cells[1]).await?,
        );
        if let (Logical::Chunked(a), Logical::Chunked(b)) = (&a, &b) {
            let edits = self
                .chunks
                .run(|inputs| lv::delta_attempt(a, b, inputs))
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
            Logical::Chunked(value) => self.chunks.value_bytes(&value).await?,
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
                Some((node, tx)) => self.conn.cached_row(
                    &format!(
                        "SELECT {} FROM {} WHERE id = ?1 AND _branch = ?2 AND _clock = ?3 AND _writer = ?4",
                        quote(stored),
                        quote(&table)
                    ),
                    crate::storage::params![id, Vec::<u8>::new(), at.0.t(), sync::writer_bytes(node, tx)],
                    |r| r.get(0),
                ).await,
                None => self.conn.cached_row(
                    &format!(
                        "SELECT {} FROM {} WHERE id = ?1 AND _branch = ?2 AND _clock = ?3 \
                         ORDER BY _writer DESC LIMIT 1",
                        quote(stored),
                        quote(&table)
                    ),
                    crate::storage::params![id, Vec::<u8>::new(), at.0.t()],
                    |r| r.get(0),
                ).await,
            }
            .optional()
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
    /// else bytes), reading only the chunks it covers. With `clamp`, a range
    /// past the value's end reads the part the value covers (a query's
    /// partial selection, SPEC 19.12); otherwise it fails.
    pub(crate) async fn cell_range(
        &self,
        cell: ValueRef<'_>,
        range: std::ops::Range<u64>,
        utf16: bool,
        clamp: bool,
    ) -> Result<Vec<u8>, Error> {
        #[cfg(feature = "testing")]
        crate::large::CELL_RANGES.with(|count| count.set(count.get() + 1));
        let fit = |range: std::ops::Range<u64>, len: u64| {
            if clamp {
                clamp_range(range, len)
            } else {
                check_range(&range, len).map(|()| range)
            }
        };
        match self.chunks.logical(cell).await? {
            Logical::Inline(bytes) if utf16 => {
                let text = std::str::from_utf8(bytes).map_err(|_| query("cell is not text"))?;
                let len = text.encode_utf16().count() as u64;
                let range = fit(range, len)?;
                let (Some(start), Some(end)) =
                    (utf16_byte(text, range.start), utf16_byte(text, range.end))
                else {
                    return Err(query("UTF-16 range splits a surrogate pair"));
                };
                Ok(bytes[start..end].to_vec())
            }
            Logical::Inline(bytes) => {
                let range = fit(range, bytes.len() as u64)?;
                Ok(bytes[range.start as usize..range.end as usize].to_vec())
            }
            Logical::Chunked(value) => {
                let len = if utf16 {
                    value.utf16_length.unwrap_or(0)
                } else {
                    value.byte_length
                };
                let range = fit(range, len)?;
                if utf16 {
                    // A UTF-16 code unit is one to three UTF-8 bytes, so
                    // these bytes hold the range: the pull asks for them.
                    let bytes = range.start.min(value.byte_length)
                        ..range.end.saturating_mul(3).min(value.byte_length);
                    self.chunks
                        .run_range(&value, bytes, |inputs| {
                            lv::utf16_range_attempt(&value, range.clone(), inputs)
                        })
                        .await
                } else {
                    self.chunks
                        .run_range(&value, range.clone(), |inputs| {
                            lv::byte_range_attempt(&value, range.clone(), inputs)
                        })
                        .await
                }
            }
        }
    }

    /// Whether [`Store::partial_cell`] would refuse `part` of `cell`, as far
    /// as that is decided without reading a chunk: an inline value is read
    /// as it would be (it is in the cell), a chunked one is checked against
    /// the lengths in its descriptor. What only a chunked value's content
    /// decides (a range splitting a code point or a surrogate pair, a JSON
    /// pointer that is not there) is not checked.
    pub(crate) async fn check_part(
        &self,
        ty: &ValueType,
        cell: ValueRef<'_>,
        part: &crate::query::Part,
    ) -> Result<(), Error> {
        use crate::query::Part;
        if let ValueType::Nullable(inner) = ty {
            return match cell {
                ValueRef::Null => Ok(()),
                cell => Box::pin(self.check_part(inner, cell, part)).await,
            };
        }
        let value = match self.chunks.logical(cell).await? {
            Logical::Inline(_) => return self.partial_cell(ty, cell, part).await.map(drop),
            Logical::Chunked(value) => value,
        };
        // A query's partial selection is clamped to the value
        // ([`Store::partial_cell`]): only an inverted range refuses.
        match part {
            Part::Bytes { from, to } | Part::TextUtf8 { from, to } => {
                clamp_range(*from..*to, value.byte_length).map(drop)
            }
            Part::TextUtf16 { from, to } => {
                clamp_range(*from..*to, value.utf16_length.unwrap_or(0)).map(drop)
            }
            Part::JsonPointer(_) => Ok(()),
        }
    }

    /// A selected cell read only in `part` (SPEC 19.12): only the chunks
    /// the part covers are read.
    pub(crate) async fn partial_cell(
        &self,
        ty: &ValueType,
        cell: ValueRef<'_>,
        part: &crate::query::Part,
    ) -> Result<Option<Value>, Error> {
        use crate::query::Part;
        if let ValueType::Nullable(inner) = ty {
            return Ok(Some(match cell {
                ValueRef::Null => Value::Nullable(None),
                cell => Value::Nullable(
                    Box::pin(self.partial_cell(inner, cell, part))
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
                Value::Bytes(self.cell_range(cell, *from..*to, false, true).await?)
            }
            Part::TextUtf16 { from, to } => text(
                self.cell_range(cell, *from..*to, true, true).await?,
                "UTF-16 range splits a surrogate pair",
            )?,
            Part::TextUtf8 { from, to } => text(
                self.cell_range(cell, *from..*to, false, true).await?,
                "UTF-8 range splits a code point",
            )?,
            Part::JsonPointer(pointer) => {
                let found = self
                    .cell_pointer(cell, pointer)
                    .await?
                    .ok_or_else(|| query("JSON pointer path does not exist"))?;
                Value::String(found.to_string())
            }
        }))
    }

    /// The subtree at JSON pointer `pointer` of a JSON cell, reading the
    /// document only as far as the pointer needs when it can.
    pub(crate) async fn cell_pointer(
        &self,
        cell: ValueRef<'_>,
        pointer: &str,
    ) -> Result<Option<serde_json::Value>, Error> {
        let source = match self.chunks.logical(cell).await? {
            Logical::Inline(bytes) => bytes.to_vec(),
            Logical::Chunked(value) => {
                let mut source = Vec::new();
                let mut offset = 0;
                while offset < value.byte_length {
                    let end = (offset + lv::LEAF_MIN_BYTES as u64).min(value.byte_length);
                    source.extend(
                        self.chunks
                            .run_range(&value, offset..end, |inputs| {
                                lv::byte_range_attempt(&value, offset..end, inputs)
                            })
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
                                    self.chunks
                                        .run_range(&value, rest.clone(), |inputs| {
                                            lv::byte_range_attempt(&value, rest.clone(), inputs)
                                        })
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
        &self,
        prepared: &PreparedCell,
        logical: &[u8],
        identity: WriteIdentity,
    ) -> Option<Value> {
        let (col, cell) = self
            .physical_cell(prepared, identity_of(identity))
            .await
            .ok()?;
        self.as_edit_of(&prepared.query.query.table, &col, cell, logical)
            .await
    }

    /// `logical` as an edit of the chunked `cell` ([`Self::whole_write_as_edit`]);
    /// a value equal to the held one is the held value, so rewriting a
    /// document unchanged costs nothing.
    pub(crate) async fn as_edit_of(
        &self,
        table: &str,
        column: &crate::values::Column,
        cell: Sql,
        logical: &[u8],
    ) -> Option<Value> {
        let Ok(Logical::Chunked(held)) = self.chunks.logical(cell.as_ref()).await else {
            return None;
        };
        let current = self.chunks.value_bytes(&held).await.ok()?;
        let text = crate::large::kind(column) != lv::LargeValueKind::Bytes;
        let Some(splice) = byte_splices(&current, logical, text).pop() else {
            self.derived.borrow_mut().push((*held).clone());
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
        &self,
        table: &str,
        column: &crate::values::Column,
        cell: Sql,
        edit: Edit,
    ) -> Result<Value, Error> {
        let kind = crate::large::kind(column);
        if !large_cell(&cell) {
            return Err(not_large(table, &column.name));
        }
        let value = match self.chunks.logical(cell.as_ref()).await? {
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
                self.chunks
                    .run_write(|inputs| {
                        lv::replace_tail_attempt(&value, offset, delete, insert.clone(), inputs)
                    })
                    .await?
            }
        };
        let next = match outcome {
            lv::TailOutcome::Updated(next) => {
                self.derived.borrow_mut().push(next.clone());
                next
            }
            lv::TailOutcome::ConsolidationRequired(transient) => {
                let mut continuation = lv::ConsolidationContinuation::new(transient)
                    .map_err(|e| rejected(e.to_string()))?;
                let prepared = loop {
                    if let Some(prepared) =
                        self.chunks.run(|inputs| continuation.step(inputs)).await?
                    {
                        break prepared;
                    }
                };
                let next = prepared.value_ref.clone();
                // A later edit of the same write reads the new tree before
                // the row update stores it.
                self.chunks.cache_pinned(&prepared);
                self.pending_nodes.borrow_mut().push(Rc::new(prepared));
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
        let prepared = PreparedCell::new(self.operation_schema(), table, row, column)?;
        self.fetching(&[], async || {
            self.inner
                .without_overlay()
                .read_range(&prepared, range.clone(), false)
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
        let schema = self.operation_schema();
        let table = schema.table(table)?;
        self.fetching(&[], async || {
            let inner = self.inner.without_overlay();
            inner
                .value_delta(
                    table,
                    row,
                    column,
                    ((from, None), (to, None)),
                    inner.reader(),
                    schema.policy_schema(),
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
        let prepared = PreparedCell::new(self.operation_schema(), table, row, column)?;
        let bytes = self
            .fetching(&[], async || {
                self.inner
                    .without_overlay()
                    .read_range(&prepared, range.clone(), true)
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
        self.fetching(&[], async || {
            self.json_pointer(table, row, column, pointer).await
        })
        .await
    }

    async fn json_pointer(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        pointer: &str,
    ) -> Result<Option<serde_json::Value>, Error> {
        let inner = self.inner.without_overlay();
        let reader = inner.reader();
        let prepared =
            PreparedCell::new(self.execution_schema(&inner.owner()), table, row, column)?;
        let (col, cell) = inner.physical_cell(&prepared, reader).await?;
        if crate::large::kind(&col) != lv::LargeValueKind::Json {
            return Err(Error::new(
                ErrorCode::Schema,
                "JSON pointer selection requires a JSON column",
            ));
        }
        if !large_cell(&cell) {
            return Ok(None);
        }
        inner.cell_pointer(cell.as_ref(), pointer).await
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
        self.fetching_with(&[], (Edit::Append(bytes), opts), async |(edit, opts)| {
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
        self.fetching_with(&[], (edit, opts), async |(edit, opts)| {
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
            let inner = self.inner.without_overlay();
            inner.require_mutation_admission()?;
            let edited = (async {
                let (col, cell) = inner
                    .physical_cell(
                        &PreparedCell::new(
                            self.execution_schema(&inner.owner()),
                            table,
                            row,
                            column,
                        )?,
                        identity_of(opts.identity),
                    )
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
        // The outer edit already owns chunk-fetch retries.
        let result = self
            .write_edited(
                StagedOp::Update(table.to_owned(), row, cells, opts.clone()),
                who(opts.identity),
            )
            .await;
        self.inner.without_overlay().clear_derived();
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
        let mutations = prepare_mutations(self.operation_schema(), table, row, &patch, mutations)?;
        self.fetching_with(
            &[],
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
        mutations: Vec<(PreparedCell, LargeValueUpdate)>,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        let lowered = {
            let inner = self.inner.without_overlay();
            inner.require_mutation_admission()?;
            let identity = identity_of(opts.identity);
            inner
                .lower_large_mutations(&mut patch, mutations, identity)
                .await
        };
        if let Err(error) = lowered {
            self.inner.without_overlay().clear_derived();
            return Err(error);
        }
        // The outer edit already owns chunk-fetch retries.
        let result = self
            .write_edited(
                StagedOp::Update(table.to_owned(), row, patch, opts.clone()),
                who(opts.identity),
            )
            .await;
        self.inner.without_overlay().clear_derived();
        result
    }

    /// Write `op`, an update an edit built, as its caller named it: with
    /// encryption hooks it is sealed like any update, its encrypted cells
    /// sealed and its space checked before anything stores it.
    async fn write_edited(&self, op: StagedOp, who: Who) -> Result<WriteHandle, Error> {
        if self.inner.e2ee_hooks().is_some() {
            return self.write_sealed(op, who, None).await;
        }
        let prepared = self.prepare_write(op)?;
        self.write_prepared(prepared, who).await
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
        let row = self.inner.ids.borrow_mut().next_row_id();
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
        reader: R,
        identity: Option<AuthorSubject>,
        now_ms: Option<u64>,
        head: Option<BranchSelector>,
        base: Option<BranchViewBase>,
    ) -> Result<WriteHandle, Error> {
        let target = view_target(head.clone(), base.clone());
        let mut cells = cells;
        let upload = self
            .begin_streaming_value_upload_for(mutation, table, row, &mut cells, column, &target)
            .await?;
        let upload = self
            .feed_streaming_value_from_reader(upload, reader)
            .await?;
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

    /// Feed an open upload from a one-shot reader. The caller can prepare
    /// the encryption key and retry opening before passing the reader here.
    pub async fn feed_streaming_value_from_reader<R: std::io::Read>(
        &self,
        mut upload: StreamingValueUpload,
        mut reader: R,
    ) -> Result<StreamingValueUpload, Error> {
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
        Ok(upload)
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
        if self.inner.mutation_admission_closed.get() {
            return Err(Error::new(
                ErrorCode::Storage,
                "database is closing or closed",
            ));
        }
        whole_only(&self.inner, table, column)?;
        self.inner.check_rowless_stream(table, cells, None)?;
        self.begin_upload(table, cells, column)
    }

    /// [`Self::begin_streaming_value_upload`] of the value of `column` of
    /// row `row`, whose write goes to `target`. With encryption hooks, the
    /// value of an encrypted column is sealed as it streams: the hooks
    /// choose the key and seal sibling cells before any source byte is read
    /// (protocol §5.5). For an encrypted table the upload takes the metadata
    /// from `cells`. Pass the remaining map to finish.
    pub async fn begin_streaming_value_upload_for(
        &self,
        mutation: StreamingMutationKind,
        table: &str,
        row: RowUuid,
        cells: &mut BTreeMap<String, Value>,
        column: &str,
        target: &WriteTarget,
    ) -> Result<StreamingValueUpload, Error> {
        self.begin_row_upload(None, mutation, table, row, cells, column, target)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn begin_row_upload(
        &self,
        transaction: Option<OpenTransactionId>,
        mutation: StreamingMutationKind,
        table: &str,
        row: RowUuid,
        cells: &mut BTreeMap<String, Value>,
        column: &str,
        target: &WriteTarget,
    ) -> Result<StreamingValueUpload, Error> {
        if self.inner.mutation_admission_closed.get() {
            return Err(Error::new(
                ErrorCode::Storage,
                "database is closing or closed",
            ));
        }
        let mut upload = self.begin_upload(table, cells, column)?;
        if cfg!(feature = "e2ee-client") {
            let schema = self.operation_schema();
            let mut prepare = async |inner: &Store| {
                inner
                    .prepare_stream(&schema, &mut upload, mutation, row, cells, target)
                    .await
            };
            let _operation = self.write_operation().await;
            match transaction {
                Some(id) => {
                    self.in_overlay(id, None, async |inner, _| prepare(inner).await)
                        .await?
                }
                None => prepare(self.inner.without_overlay()).await?,
            }
        }
        Ok(upload)
    }

    fn begin_upload(
        &self,
        table: &str,
        cells: &BTreeMap<String, Value>,
        column: &str,
    ) -> Result<StreamingValueUpload, Error> {
        let col = streaming_column(self.operation_schema().table(table)?, column)?;
        if cells.contains_key(column) {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("{table}.{column} is streamed, so the other cells cannot also set it"),
            ));
        }
        self.inner
            .validate_stream_siblings(&self.operation_schema(), table, cells)?;
        let kind = crate::large::kind(&col);
        let emitted = Rc::new(RefCell::new(Vec::new()));
        let stage = Rc::clone(&emitted);
        let id = upload_id();
        let schema_instance = {
            Rc::downgrade(
                self.view
                    .as_ref()
                    .unwrap_or(&self.inner.published_schema.borrow()),
            )
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
                cleanup: self.inner.upload_cleanup.clone(),
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
            sealer: None,
            sealed_for: None,
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
        let _operation = self.write_operation().await;
        // An encrypted value is sealed before the chunker sees it.
        if let Some(sealer) = upload.sealer.as_mut() {
            let sealed = sealer.push(bytes)?;
            return self.stage_streaming_bytes(upload, &sealed).await;
        }
        self.stage_streaming_bytes(upload, bytes).await
    }

    /// Feed stored bytes to an upload's chunker, within the caller's
    /// operation.
    async fn stage_streaming_bytes(
        &self,
        upload: &mut StreamingValueUpload,
        bytes: &[u8],
    ) -> Result<(), Error> {
        self.check_streaming_upload(upload)?;
        let inner = self.inner.without_overlay();
        inner.require_mutation_admission()?;
        let now = streaming_now_ms();
        if upload.preparation.is_none() {
            return Err(closed());
        }
        if !upload.initialized {
            crate::large::begin_upload(&inner.conn, &upload.id, now).await?;
            upload.initialized = true;
        } else if !crate::large::touch_upload(&inner.conn, &upload.id, now).await? {
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
                admit_ingress(&self.inner, &staged, now)?;
                crate::large::stage_upload(
                    &inner.conn,
                    &inner.chunks,
                    &upload.id,
                    &staged,
                    inner.client().is_some(),
                )
                .await
            }
        })
        .await;
        if let Err(error) = staged {
            upload.preparation.take();
            crate::large::end_upload(&inner.conn, &upload.id).await?;
            return Err(error);
        }
        Ok(())
    }

    /// Abandon an upload and release what it stored.
    pub async fn abort_streaming_value_upload(
        &self,
        mut upload: StreamingValueUpload,
    ) -> Result<(), Error> {
        let _operation = self.write_operation().await;
        self.check_streaming_upload(&mut upload)?;
        upload.preparation.take();
        crate::large::end_upload(&self.inner.without_overlay().conn, &upload.id).await
    }

    fn check_streaming_upload(&self, upload: &mut StreamingValueUpload) -> Result<(), Error> {
        let mismatch = || schema_error("streaming upload belongs to another database or schema");
        if !upload.owner.ptr_eq(&Rc::downgrade(&self.inner)) {
            return Err(mismatch());
        }
        let inner = self.inner.without_overlay();
        let owner_schema = inner.owner();
        let schema = self.view.as_ref().unwrap_or(&owner_schema);
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
        let target = view_target(head.clone(), base.clone());
        let large = {
            let _operation = self.write_operation().await;
            self.seal_streaming_upload(
                &mut upload,
                mutation,
                table,
                row,
                &mut cells,
                column,
                &target,
            )
            .await?
        };
        // The value was sealed as it streamed: this write, and no other,
        // leaves the column to the hooks as it is, in the row and space it
        // was sealed for.
        let sealed_for = upload.sealed_for.take();
        let result = async {
            if mutation == StreamingMutationKind::Insert && base.is_some() {
                return Err(rejected(
                    "a streamed insert writes one exact branch, not a branch view",
                ));
            }
            let op = mutation.staged(table, row, cells, identity, now_ms, head, base);
            if self.inner.e2ee_hooks().is_some() {
                return self
                    .fetching_with(&[], op, async |op| {
                        self.inner
                            .without_overlay()
                            .derived
                            .borrow_mut()
                            .extend(large.iter().cloned());
                        let result = self
                            .write_sealed(op, who(identity), sealed_for.as_ref())
                            .await;
                        self.inner.without_overlay().clear_derived();
                        result
                    })
                    .await;
            }
            let prepared = {
                let _operation = self.write_operation().await;
                self.prepare_write(op)?
            };
            self.fetching_with(&[], prepared, async |op| {
                self.inner
                    .without_overlay()
                    .derived
                    .borrow_mut()
                    .extend(large.iter().cloned());
                let result = self.write_prepared(op, who(identity)).await;
                self.inner.without_overlay().clear_derived();
                result
            })
            .await
        }
        .await;
        let _operation = self.write_operation().await;
        let inner = self.inner.without_overlay();
        crate::large::end_upload(&inner.conn, &upload.id).await?;
        result
    }

    /// Finish `upload`'s value and set it as `column` of `cells`: inline when
    /// it fits, else the chunked descriptor, returned for the write to trust.
    /// A failed upload is ended here.
    #[allow(clippy::too_many_arguments)]
    async fn seal_streaming_upload(
        &self,
        upload: &mut StreamingValueUpload,
        mutation: StreamingMutationKind,
        table: &str,
        row: RowUuid,
        cells: &mut BTreeMap<String, Value>,
        column: &str,
        target: &WriteTarget,
    ) -> Result<Option<lv::LargeValueRef>, Error> {
        let active_schema = self.operation_schema();
        self.inner.without_overlay().require_mutation_admission()?;
        self.check_streaming_upload(upload)?;
        if upload.table != table || upload.column != column {
            return Err(schema_error(
                "streaming upload target differs from its original table or column",
            ));
        }
        // A value sealed as it streamed is bound to the row, target and
        // space it was sealed for: published anywhere else, it would never
        // open, and nothing would seal it again.
        // Refused here early when the finish names another row, target or
        // space; the write's staging, which sees its transaction's own
        // writes, checks the space it is sealed in (`Store::seal_op`).
        if let Some(sealed) = &mut upload.sealed_for {
            if mutation != sealed.mutation {
                return Err(rejected(
                    "a sealed streaming upload must finish with its original mutation kind",
                ));
            }
            let named = self.inner.named_space(table, row, cells);
            if sealed.row != row
                || sealed.target != *target
                || named.is_some_and(|named| Some(named) != sealed.space)
            {
                return Err(sealed_elsewhere());
            }
            if cells.iter().any(|(k, v)| sealed.cells.get(k) != Some(v)) {
                return Err(rejected(
                    "a sealed streaming upload takes its metadata at begin",
                ));
            }
            *cells = std::mem::take(&mut sealed.cells);
        } else {
            self.inner
                .check_rowless_stream(table, cells, Some(mutation))?;
        }
        if cells.contains_key(column) {
            return Err(rejected(
                "the streamed column is also set in the other cells",
            ));
        }
        if let Some(sealer) = upload.sealer.take() {
            let tail = sealer.finish()?;
            self.stage_streaming_bytes(upload, &tail).await?;
        }
        let value = {
            let inner = self.inner.without_overlay();
            let now = streaming_now_ms();
            let alive = if upload.initialized {
                crate::large::touch_upload(&inner.conn, &upload.id, now).await?
            } else {
                crate::large::begin_upload(&inner.conn, &upload.id, now).await?;
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
                    let col = streaming_column(active_schema.table(table)?, column)?;
                    if crate::large::kind(&col) != upload.kind {
                        return Err(not_large(table, column));
                    }
                    admit_ingress(&self.inner, &staged, now)?;
                    crate::large::stage_upload(
                        &inner.conn,
                        &inner.chunks,
                        &upload.id,
                        &staged,
                        inner.client().is_some(),
                    )
                    .await?;
                    crate::large::finish_upload(&inner.conn, &value).await?;
                    Ok((col, value))
                }
            })
            .await;
            match value {
                Ok(value) => value,
                Err(error) => {
                    crate::large::end_upload(&inner.conn, &upload.id).await?;
                    return Err(error);
                }
            }
        };
        let (col, value) = value;
        // A value that fits inline is written as an ordinary inline cell, as
        // the store keeps it: its commit carries the bytes, not a
        // descriptor of chunks the store then reclaims.
        let (logical, large) = if value.byte_length <= crate::large::inline_max(&col) as u64 {
            let bytes = self
                .inner
                .without_overlay()
                .chunks
                .value_bytes(&value)
                .await?;
            (logical_value(value.kind, bytes)?, None)
        } else {
            (Value::Large(Box::new(value.clone())), Some(value))
        };
        cells.insert(column.to_owned(), nullable(&col, logical));
        Ok(large)
    }

    /// Replace the limits on local streaming uploads.
    pub fn set_large_value_staging_policy(&self, policy: LargeValueStagingPolicy) {
        self.inner.staging_policy.set(policy);
    }

    /// End every streaming upload left untouched past the staging policy's
    /// maximum age, and every pushed-node receipt past it; returns how
    /// many uploads ended.
    pub async fn evict_expired_staged_large_values(&self) -> Result<usize, Error> {
        let _operation = self.write_operation().await;
        let conn = self.inner.without_overlay().conn.clone();
        let now = streaming_now_ms();
        let max_age = i64::try_from(self.inner.staging_policy.get().max_age_ms).unwrap_or(i64::MAX);
        self.inner.release_dropped_uploads(&conn).await?;
        crate::large::expire_receipts(&conn, now).await?;
        crate::large::expire_uploads(&conn, now.saturating_sub(max_age)).await
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

impl StreamingMutationKind {
    #[allow(clippy::too_many_arguments)]
    fn staged(
        self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        identity: WriteIdentity,
        now_ms: Option<u64>,
        head: Option<BranchSelector>,
        base: Option<BranchViewBase>,
    ) -> StagedOp {
        match self {
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
        }
    }
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
    pub(super) table: String,
    pub(super) column: String,
    _lease: UploadLease,
    kind: lv::LargeValueKind,
    initialized: bool,
    preparation: Option<lv::PushStreamingPreparation<Stage>>,
    emitted: Rc<RefCell<Vec<lv::StagedChunk>>>,
    /// Seals an encrypted column's value as it streams.
    pub(super) sealer: Option<Box<dyn crate::e2ee::StreamSealer>>,
    /// The row, target and space the value was sealed for as it streamed:
    /// only a write of that row, to that target, in that space publishes
    /// it, and leaves it to the hooks as it is.
    pub(super) sealed_for: Option<SealedFor>,
}

pub(crate) type StreamedSeal<'a> = &'a SealedFor;

/// The refusal of a sealed stream published anywhere but where it was
/// sealed for: there it would never open, and nothing would seal it again.
pub(crate) fn sealed_elsewhere() -> Error {
    schema_error(
        "a sealed streaming upload is published to the row, target and space it was sealed for",
    )
}

/// What a sealed stream was sealed for (see
/// [`Db::begin_streaming_value_upload_for`]).
pub(crate) struct SealedFor {
    pub(super) row: RowUuid,
    pub(super) mutation: StreamingMutationKind,
    pub(super) target: WriteTarget,
    pub(super) space: Option<Value>,
    pub(super) existed: bool,
    pub(super) root: Option<String>,
    pub(super) cells: BTreeMap<String, Value>,
    pub(super) insert_only: BTreeMap<String, Value>,
    pub(super) companions: Vec<(String, RowUuid, BTreeMap<String, Value>)>,
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
    inner: &Store,
    leases: &[UploadLease],
) -> Result<(), Error> {
    let now = streaming_now_ms();
    for lease in leases {
        if !crate::large::touch_upload(&inner.conn, &lease.id, now).await? {
            return Err(expired());
        }
    }
    Ok(())
}

/// Refuse to read or write `column` of `table` other than whole when it is
/// encrypted: a range, a JSON pointer, a delta, an edit or a streamed
/// value (protocol §5.5).
fn whole_only(inner: &Store, table: &str, column: &str) -> Result<(), Error> {
    inner.refuse_encrypted_part(table, column)
}

fn streaming_column(table: &Table, column: &str) -> Result<crate::values::Column, Error> {
    table
        .column(column)
        .filter(|c| crate::large::large_capable(c))
        .cloned()
        .ok_or_else(|| not_large(&table.name, column))
}

fn view_target(head: Option<BranchSelector>, base: Option<BranchViewBase>) -> WriteTarget {
    match head {
        Some(head) => WriteTarget::BranchView { head, base },
        None => WriteTarget::Root,
    }
}

/// Charge `staged` to the staging policy's ingress window.
fn admit_ingress(store: &Store, staged: &[lv::StagedChunk], now: i64) -> Result<(), Error> {
    let bytes: u64 = staged.iter().map(|chunk| chunk.encoded.len() as u64).sum();
    if bytes == 0 {
        return Ok(());
    }
    let policy = store.staging_policy.get();
    let mut window = store.ingress.get();
    let now = now.max(0) as u64;
    if now.saturating_sub(window.0) >= policy.window_ms {
        window = (now, 0);
        store.ingress.set(window);
    }
    if window.1.saturating_add(bytes) > policy.incoming_bytes_per_window {
        return Err(Error::new(
            ErrorCode::WriteRejected,
            "streaming upload exceeds the large-value ingress rate limit",
        ));
    }
    store.ingress.set((window.0, window.1 + bytes));
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

/// Whether byte `offset` of the text that `edits` (byte splices, in order)
/// made from `raw`, `len` bytes long, is a char boundary: its end, or a byte
/// that doesn't continue a code point. Reads at most one byte of `raw`.
async fn utf8_boundary(
    inner: &Store,
    raw: ValueRef<'_>,
    edits: &[Edit],
    mut offset: u64,
    len: u64,
) -> Result<bool, Error> {
    if offset >= len {
        return Ok(offset == len);
    }
    let continues = |byte: u8| byte & 0b1100_0000 == 0b1000_0000;
    for edit in edits.iter().rev() {
        let Edit::Splice {
            offset: at,
            delete,
            insert,
        } = edit
        else {
            continue;
        };
        if offset < *at {
            continue;
        }
        let inserted = insert.len() as u64;
        if offset < at + inserted {
            return Ok(!continues(insert[(offset - at) as usize]));
        }
        offset = offset - inserted + delete;
    }
    let byte = inner
        .cell_range(raw, offset..offset + 1, false, false)
        .await?;
    Ok(byte.first().is_none_or(|byte| !continues(*byte)))
}

/// Splices in a page's coordinates, lowered to byte coordinates of the
/// current value. Each later splice is relative to the value the earlier
/// ones produced.
async fn lower_splices(
    inner: &Store,
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
    let current = inner.chunks.logical(raw).await?;
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
        let mut text = String::from_utf8(inner.chunks.materialize(raw).await?)
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
    let utf8_text = matches!(within, LargeValueUpdatePage::TextUtf8 { .. });
    let mut value_len = len;
    let mut page_len = to - from;
    let mut edits: Vec<Edit> = Vec::new();
    for splice in splices {
        let end = splice
            .at
            .checked_add(splice.delete)
            .ok_or_else(|| query("splice range overflows"))?;
        if end > page_len {
            return Err(query("splice deletion leaves the selected page"));
        }
        if utf8_text {
            // A UTF-8 offset is refused off a char boundary rather than
            // splitting a code point. Only the byte at each end is read.
            for offset in [from + splice.at, from + end] {
                if !utf8_boundary(inner, raw, &edits, offset, value_len).await? {
                    return Err(query(format!("{NOT_CHAR_BOUNDARY}: {offset}")));
                }
            }
            std::str::from_utf8(&splice.insert)
                .map_err(|_| query("text splice is not valid UTF-8"))?;
        }
        value_len = value_len - splice.delete + splice.insert.len() as u64;
        page_len = page_len - splice.delete + splice.insert.len() as u64;
        edits.push(Edit::Splice {
            offset: from + splice.at,
            delete: splice.delete,
            insert: splice.insert,
        });
    }
    Ok(edits)
}

fn prepare_mutations(
    schema: Rc<Schema>,
    table: &str,
    row: RowUuid,
    patch: &BTreeMap<String, Value>,
    mutations: Vec<LargeValueUpdate>,
) -> Result<Vec<(PreparedCell, LargeValueUpdate)>, Error> {
    for mutation in &mutations {
        let column = mutation.column();
        if patch.contains_key(column) {
            return Err(query(format!(
                "`{column}` is both patched and partially updated"
            )));
        }
    }
    mutations
        .into_iter()
        .map(|mutation| {
            let cell = PreparedCell::new(schema.clone(), table, row, mutation.column())?;
            Ok((cell, mutation))
        })
        .collect()
}

impl Store {
    /// Lower partial large-value updates into `patch` against the row as
    /// this store sees it now (inside a batch: with its staged writes).
    /// What the edits build is left in `derived` and `pending_nodes`.
    pub(crate) async fn lower_large_mutations(
        &self,
        patch: &mut BTreeMap<String, Value>,
        mutations: Vec<(PreparedCell, LargeValueUpdate)>,
        identity: Option<AuthorSubject>,
    ) -> Result<(), Error> {
        for (prepared, mutation) in mutations {
            let table = &prepared.query.query.table;
            let column = prepared.column.name.clone();
            let (col, mut cell) = self.physical_cell(&prepared, identity).await?;
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
                    let mut json: serde_json::Value =
                        serde_json::from_slice(&self.chunks.materialize(cell.as_ref()).await?)
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
                        String::from_utf8(self.chunks.materialize(cell.as_ref()).await?)
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
                            self.merge_budget.get(),
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
    /// [`Db::begin_streaming_value_upload_for`] in this transaction: the
    /// row's space, when the cells leave it to the stored row, is the one
    /// the transaction sees, its own writes included, so a value can be
    /// streamed into a row the transaction inserted.
    pub async fn begin_streaming_value_upload_for(
        &self,
        mutation: StreamingMutationKind,
        table: &str,
        row: RowUuid,
        cells: &mut BTreeMap<String, Value>,
        column: &str,
        target: &WriteTarget,
    ) -> Result<StreamingValueUpload, Error> {
        self.db
            .begin_row_upload(Some(self.id), mutation, table, row, cells, column, target)
            .await
    }

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
        let _operation = self.db.write_operation().await;
        let large = self
            .db
            .seal_streaming_upload(
                &mut upload,
                mutation,
                table,
                row,
                &mut cells,
                column,
                &view_target(head.clone(), base.clone()),
            )
            .await?;
        let op = mutation.staged(table, row, cells, identity, now_ms, head, base);
        let held = large.is_some();
        let edits = crate::db::LargeEdits {
            derived: large.into_iter().collect(),
            pending_nodes: Vec::new(),
        };
        // The value was sealed as it streamed: this write, and no other,
        // leaves the column to the hooks as it is, in the row and space it
        // was sealed for.
        let sealed_for = upload.sealed_for.take();
        let streamed = sealed_for.as_ref();
        self.db.stage_streamed(self.id, op, edits, streamed).await?;
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
        let _operation = self.db.write_operation().await;
        let mutations =
            prepare_mutations(self.db.operation_schema(), table, row, &patch, mutations)?;
        self.db
            .inner
            .without_overlay()
            .require_mutation_admission()?;
        let edits = self
            .db
            .in_overlay(self.id, None, async |inner, _| {
                let identity = identity_of(opts.identity);
                let lowered = inner
                    .lower_large_mutations(&mut patch, mutations, identity)
                    .await;
                let edits = crate::db::LargeEdits {
                    derived: inner.derived.take(),
                    pending_nodes: inner.pending_nodes.take(),
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
