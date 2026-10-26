//! Range reads and in-place edits of `string`, `json` and `bytes` cells.
//!
//! Inline and chunked cells share these APIs. A chunked cell reads only the
//! chunks its range covers, and an append or splice adds one entry to the
//! value's bounded edit tail instead of rewriting it; when the tail is full
//! the value is consolidated, rechunking only until content boundaries
//! resynchronize (Groove SPEC ch. 9 §9.3). Every edit is published as one
//! ordinary row update, so policies, history and sync see a new version.

use super::*;
use crate::large::{Logical, logical};
use jazz_types::large_values as lv;
use rusqlite::OptionalExtension;
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
}

impl LargeValueUpdate {
    fn column(&self) -> &str {
        match self {
            Self::Splice { column, .. } | Self::JsonSet { column, .. } => column,
        }
    }
}

/// One byte-coordinate edit.
#[derive(Clone)]
enum Edit {
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
    /// Drop queues contain only this owner's uploads. SQL errors retain the
    /// receipt for the next maintenance pass; crash recovery uses lease expiry.
    pub(super) fn release_dropped_uploads(&self) -> Result<(), Error> {
        let mut cleanup = self.upload_cleanup.borrow_mut();
        while let Some(id) = cleanup.last() {
            crate::large::end_upload(&self.conn, id)?;
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
    fn physical_cell(
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
            authority_policies: false,
            schema: None,
        };
        let compiled = self.compile(&prepared, identity, &ReadSpec::default())?;
        let cell: Option<Sql> = self.with_preps(&compiled.preps, |_| {
            let mut statement = self.conn.prepare_cached(&compiled.sql).map_err(storage)?;
            statement
                .query_row(params_from_iter(compiled.params.iter()), |r| r.get(3))
                .optional()
                .map_err(storage)
        })?;
        let cell = cell.ok_or_else(|| {
            Error::new(ErrorCode::NotObserved, "large-value cell is not observed")
        })?;
        Ok((col, cell))
    }

    fn read_range(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        range: std::ops::Range<u64>,
        utf16: bool,
    ) -> Result<Vec<u8>, Error> {
        let (col, cell) = self.physical_cell(table, row, column, self.reader())?;
        let Sql::Text(cell) = cell else {
            return Err(not_large(table, column));
        };
        if utf16 && crate::large::kind(&col) == lv::LargeValueKind::Bytes {
            return Err(not_large(table, column));
        }
        match logical(&cell)? {
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
                self.chunks.run(|inputs| {
                    if utf16 {
                        lv::utf16_range_attempt(&value, range.clone(), inputs)
                    } else {
                        lv::byte_range_attempt(&value, range.clone(), inputs)
                    }
                })
            }
        }
    }

    /// The value `edit` makes of the current cell. A chunked value gains a
    /// tail entry, or is consolidated when its tail is full; the nodes a
    /// consolidation writes are stored with the row update.
    fn edited(
        &mut self,
        table: &str,
        column: &crate::values::Column,
        cell: Sql,
        edit: Edit,
    ) -> Result<Value, Error> {
        let kind = crate::large::kind(column);
        let Sql::Text(cell) = cell else {
            return Err(not_large(table, &column.name));
        };
        let value = match logical(&cell)? {
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
            } => self.chunks.run(|inputs| {
                lv::replace_tail_attempt(&value, offset, delete, insert.clone(), inputs)
            })?,
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
                    if let Some(prepared) = self.chunks.run(|inputs| continuation.step(inputs))? {
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
        self.inner().read_range(table, row, column, range, false)
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
        let bytes = self.inner().read_range(table, row, column, range, true)?;
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
        let inner = self.inner();
        let reader = inner.reader();
        let (col, cell) = inner.physical_cell(table, row, column, reader)?;
        if crate::large::kind(&col) != lv::LargeValueKind::Json {
            return Err(Error::new(
                ErrorCode::Schema,
                "JSON pointer selection requires a JSON column",
            ));
        }
        let Sql::Text(cell) = cell else {
            return Ok(None);
        };
        let source = match logical(&cell)? {
            Logical::Inline(bytes) => bytes.to_vec(),
            Logical::Chunked(value) => {
                let mut source = Vec::new();
                let mut offset = 0;
                while offset < value.byte_length {
                    let end = (offset + lv::LEAF_MIN_BYTES as u64).min(value.byte_length);
                    source.extend(
                        inner
                            .chunks
                            .run(|inputs| lv::byte_range_attempt(&value, offset..end, inputs))?,
                    );
                    offset = end;
                    match lv::json_pointer_prefix(&source, pointer)
                        .map_err(|e| query(e.to_string()))?
                    {
                        lv::JsonPointerPrefix::Found(found) => return Ok(found),
                        lv::JsonPointerPrefix::NeedMore => {}
                        lv::JsonPointerPrefix::RequiresFullDocument => {
                            if offset < value.byte_length {
                                source.extend(inner.chunks.run(|inputs| {
                                    lv::byte_range_attempt(
                                        &value,
                                        offset..value.byte_length,
                                        inputs,
                                    )
                                })?);
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
        self.edit_cell(table, row, column, Edit::Append(bytes), opts)
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
        self.edit_cell(table, row, column, edit, opts)
    }

    fn edit_cell(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        edit: Edit,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        self.inner().require_mutation_admission()?;
        let value = {
            let mut inner = self.inner();
            let (col, cell) =
                inner.physical_cell(table, row, column, identity_of(opts.identity))?;
            let value = inner.edited(table, &col, cell, edit)?;
            nullable(&col, value)
        };
        let cells = BTreeMap::from([(column.to_owned(), value)]);
        let result = self.write_op(
            StagedOp::Update(table.to_owned(), row, cells, opts.clone()),
            who(opts.identity),
        );
        self.inner().clear_derived();
        result
    }

    /// Apply partial large-value updates together with an ordinary patch,
    /// as one row update. A failing update leaves the row unchanged.
    pub async fn update_with_large_value_mutations(
        &self,
        table: &str,
        row: RowUuid,
        mut patch: BTreeMap<String, Value>,
        mutations: Vec<LargeValueUpdate>,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        self.inner().require_mutation_admission()?;
        let lowered = {
            let mut inner = self.inner();
            let identity = identity_of(opts.identity);
            inner.lower_large_mutations(table, row, &mut patch, mutations, identity)
        };
        if let Err(error) = lowered {
            self.inner().clear_derived();
            return Err(error);
        }
        let result = self.write_op(
            StagedOp::Update(table.to_owned(), row, patch, opts.clone()),
            who(opts.identity),
        );
        self.inner().clear_derived();
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
        let row = self.inner().ids.next_row_id();
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
    /// in pieces (such as an asynchronous browser stream). Nothing is
    /// stored until the first push.
    pub fn begin_streaming_value_upload(
        &self,
        table: &str,
        cells: &BTreeMap<String, Value>,
        column: &str,
    ) -> Result<StreamingValueUpload, Error> {
        self.inner().require_mutation_admission()?;
        let col = streaming_column(&self.inner(), table, column)?;
        if cells.contains_key(column) {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("{table}.{column} is streamed, so the other cells cannot also set it"),
            ));
        }
        let kind = crate::large::kind(&col);
        let emitted = Rc::new(RefCell::new(Vec::new()));
        let stage = Rc::clone(&emitted);
        let id = *lv::Locator::random().as_bytes();
        Ok(StreamingValueUpload {
            id,
            owner: Rc::downgrade(&self.inner),
            schema: self.schema_view_id(),
            table: table.to_owned(),
            column: column.to_owned(),
            lease: Rc::new(UploadLease {
                id,
                cleanup: self.inner().upload_cleanup.clone(),
            }),
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
        self.inner().require_mutation_admission()?;
        self.check_streaming_upload(upload)?;
        let inner = self.inner();
        let now = streaming_now_ms();
        if upload.preparation.is_none() {
            return Err(closed());
        }
        if !upload.initialized {
            crate::large::begin_upload(&inner.conn, &upload.id, now)?;
            upload.initialized = true;
        } else if !crate::large::touch_upload(&inner.conn, &upload.id, now)? {
            upload.preparation.take();
            return Err(expired());
        }
        let Some(preparation) = upload.preparation.as_mut() else {
            return Err(closed());
        };
        let staged = preparation
            .push(bytes)
            .map_err(|e| rejected(e.to_string()))
            .map(|()| std::mem::take(&mut *upload.emitted.borrow_mut()))
            .and_then(|staged| {
                admit_ingress(&inner, &staged, now)?;
                crate::large::stage_upload(
                    &inner.conn,
                    &inner.chunks,
                    &upload.id,
                    &staged,
                    inner.client.is_some(),
                )
            });
        if let Err(error) = staged {
            upload.preparation.take();
            crate::large::end_upload(&inner.conn, &upload.id)?;
            return Err(error);
        }
        Ok(())
    }

    /// Abandon an upload and release what it stored.
    pub async fn abort_streaming_value_upload(
        &self,
        mut upload: StreamingValueUpload,
    ) -> Result<(), Error> {
        self.check_streaming_upload(&upload)?;
        upload.preparation.take();
        crate::large::end_upload(&self.inner().conn, &upload.id)
    }

    fn check_streaming_upload(&self, upload: &StreamingValueUpload) -> Result<(), Error> {
        if !upload.owner.ptr_eq(&Rc::downgrade(&self.inner))
            || upload.schema != self.schema_view_id()
        {
            return Err(schema_error(
                "streaming upload belongs to another database or schema",
            ));
        }
        Ok(())
    }

    /// Finish an upload and publish its row mutation. Whether or not the
    /// write succeeds, the upload's lease ends: a committed row holds the
    /// value from then on, and anything else is reclaimable.
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
        self.inner().require_mutation_admission()?;
        if cells.contains_key(column) {
            return Err(rejected(
                "the streamed column is also set in the other cells",
            ));
        }
        let (col, value) = self.finalize_streaming_upload(&mut upload, table, column)?;
        // A value that fits inline is written as an ordinary inline cell, as
        // the store keeps it: its commit carries the bytes, not a
        // descriptor of chunks the store then reclaims.
        let logical = if value.byte_length <= lv::INLINE_VALUE_MAX_BYTES as u64 {
            let bytes = self.inner().chunks.value_bytes(&value)?;
            logical_value(value.kind, bytes)?
        } else {
            self.inner().derived.push(value.clone());
            Value::Large(Box::new(value))
        };
        cells.insert(column.to_owned(), nullable(&col, logical));
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
        let mut inner = self.inner();
        inner.clear_derived();
        crate::large::end_upload(&inner.conn, &upload.id)?;
        result
    }

    fn finalize_streaming_upload(
        &self,
        upload: &mut StreamingValueUpload,
        table: &str,
        column: &str,
    ) -> Result<(crate::values::Column, lv::LargeValueRef), Error> {
        self.check_streaming_upload(upload)?;
        if upload.table != table || upload.column != column {
            return Err(schema_error(
                "streaming upload target differs from its original table or column",
            ));
        }
        let value = {
            let inner = self.inner();
            let now = streaming_now_ms();
            let alive = if upload.initialized {
                crate::large::touch_upload(&inner.conn, &upload.id, now)?
            } else {
                crate::large::begin_upload(&inner.conn, &upload.id, now)?;
                true
            };
            let finished = match upload.preparation.take() {
                _ if !alive => Err(expired()),
                None => Err(closed()),
                Some(preparation) => preparation.finish().map_err(|e| rejected(e.to_string())),
            };
            let staged = std::mem::take(&mut *upload.emitted.borrow_mut());
            let value = finished.and_then(|(value, _)| {
                let col = streaming_column(&inner, table, column)?;
                if crate::large::kind(&col) != upload.kind {
                    return Err(not_large(table, column));
                }
                admit_ingress(&inner, &staged, now)?;
                crate::large::stage_upload(
                    &inner.conn,
                    &inner.chunks,
                    &upload.id,
                    &staged,
                    inner.client.is_some(),
                )?;
                crate::large::finish_upload(&inner.conn, &value)?;
                Ok((col, value))
            });
            match value {
                Ok(value) => value,
                Err(error) => {
                    crate::large::end_upload(&inner.conn, &upload.id)?;
                    return Err(error);
                }
            }
        };
        Ok(value)
    }

    /// Finish staging without publishing a row. The opaque, single-use
    /// capability binds the mutation, schema and trusted identity. Its chunks
    /// stay under the upload's existing lease until attachment or abandonment.
    #[allow(clippy::too_many_arguments)]
    pub async fn stage_streaming_value_upload(
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
    ) -> Result<StagedStreamingValue, Error> {
        self.inner().require_mutation_admission()?;
        self.check_streaming_upload(&upload)?;
        if head.is_some()
            || base.is_some()
            || !self.inner().schema.table(table)?.branch_by.is_empty()
        {
            return Err(schema_error("staged streaming requires a root-view target"));
        }
        if matches!(identity, WriteIdentity::Attribution(_)) {
            self.inner().require_attribution_backend()?;
        }
        if cells.contains_key(column) {
            return Err(rejected(
                "the streamed column is also set in the other cells",
            ));
        }
        let (col, value) = self.finalize_streaming_upload(&mut upload, table, column)?;
        cells.insert(
            column.to_owned(),
            nullable(&col, Value::Large(Box::new(value))),
        );
        let op = match mutation {
            StreamingMutationKind::Insert => StagedOp::Insert(
                table.to_owned(),
                cells,
                InsertOptions {
                    row_id: Some(row),
                    identity,
                    updated_at_ms: now_ms,
                    ..Default::default()
                },
            ),
            StreamingMutationKind::Update => StagedOp::Update(
                table.to_owned(),
                row,
                cells,
                UpdateOptions {
                    identity,
                    updated_at_ms: now_ms,
                    ..Default::default()
                },
            ),
            StreamingMutationKind::Upsert => StagedOp::Upsert(
                table.to_owned(),
                row,
                cells,
                UpsertOptions {
                    identity,
                    updated_at_ms: now_ms,
                    ..Default::default()
                },
            ),
        };
        Ok(StagedStreamingValue {
            owner: upload.owner.clone(),
            schema: upload.schema,
            pending: Some(StagedStreamingMutation {
                op,
                lease: upload.lease.clone(),
            }),
        })
    }

    /// Attach to an open serializable batch. Every attempt consumes the
    /// capability, including a rejected attachment; the batch takes over its
    /// lease only after the normal transaction overlay validates the write.
    pub async fn attach_staged_streaming_value(
        &self,
        tx: OpenTransactionId,
        staged: &mut StagedStreamingValue,
    ) -> Result<(), Error> {
        let pending = staged
            .pending
            .take()
            .ok_or_else(|| schema_error("staged streaming upload is closed"))?;
        self.inner().require_mutation_admission()?;
        self.check_staged_streaming_owner(staged)?;
        {
            let inner = self.inner();
            let batch = inner
                .batches
                .get(&tx)
                .ok_or_else(|| rejected(format!("transaction {tx:?} is not open")))?;
            if !matches!(batch.kind, BatchKind::Serializable { .. }) {
                return Err(unsupported(
                    "staged streaming attachment to a mergeable transaction",
                ));
            }
            if Some(batch.identity) != pending.op.identity() {
                return Err(rejected(
                    "staged upload identity differs from its transaction",
                ));
            }
            if !crate::large::touch_upload(&inner.conn, &pending.lease.id, streaming_now_ms())? {
                return Err(expired());
            }
        }
        self.stage(tx, pending.op)?;
        self.inner()
            .batches
            .get_mut(&tx)
            .expect("validated")
            .uploads
            .push(pending.lease);
        Ok(())
    }

    fn check_staged_streaming_owner(&self, staged: &StagedStreamingValue) -> Result<(), Error> {
        if !staged.owner.ptr_eq(&Rc::downgrade(&self.inner))
            || staged.schema != self.schema_view_id()
        {
            return Err(schema_error(
                "staged upload belongs to another database or schema",
            ));
        }
        Ok(())
    }

    /// Release an unattached capability. Consumed capabilities are harmless;
    /// history and open batches keep their own references to committed chunks.
    pub async fn abort_staged_streaming_value(
        &self,
        staged: &mut StagedStreamingValue,
    ) -> Result<bool, Error> {
        self.check_staged_streaming_owner(staged)?;
        let Some(pending) = staged.pending.take() else {
            return Ok(false);
        };
        crate::large::end_upload(&self.inner().conn, &pending.lease.id)?;
        Ok(true)
    }

    /// Replace the limits on local streaming uploads.
    pub fn set_large_value_staging_policy(&self, policy: LargeValueStagingPolicy) {
        self.inner().staging = policy;
    }

    /// Persisted local upload leases, including dropped handles awaiting
    /// maintenance. Observing the inventory must not perform that cleanup.
    #[cfg(any(test, feature = "testing"))]
    pub async fn pending_upload_count_for_test(&self) -> Result<usize, Error> {
        self.inner
            .borrow()
            .conn
            .query_row("SELECT count(*) FROM _jazz_lv_upload", [], |row| {
                let count: i64 = row.get(0)?;
                usize::try_from(count)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, count))
            })
            .map_err(storage)
    }

    /// End every streaming upload left untouched past the staging policy's
    /// maximum age, and every pushed-node receipt past it; returns how
    /// many uploads ended.
    pub async fn evict_expired_staged_large_values(&self) -> Result<usize, Error> {
        let inner = self.inner();
        let now = streaming_now_ms();
        let max_age = i64::try_from(inner.staging.max_age_ms).unwrap_or(i64::MAX);
        inner.release_dropped_uploads()?;
        crate::large::expire_receipts(&inner.conn, now)?;
        let pinned = inner
            .batches
            .values()
            .flat_map(|batch| batch.uploads.iter().map(|lease| lease.id))
            .collect();
        crate::large::expire_uploads_except(&inner.conn, now.saturating_sub(max_age), &pinned)
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

/// A resumable upload of one large value: the tree under construction and
/// the lease that holds its stored nodes.
pub struct StreamingValueUpload {
    id: [u8; 32],
    owner: std::rc::Weak<RefCell<Inner>>,
    schema: SchemaViewId,
    table: String,
    column: String,
    lease: Rc<UploadLease>,
    kind: lv::LargeValueKind,
    initialized: bool,
    preparation: Option<lv::PushStreamingPreparation<fn(lv::ContentHash) -> lv::Locator, Stage>>,
    emitted: Rc<RefCell<Vec<lv::StagedChunk>>>,
}

/// An opaque capability for one completed upload. It cannot expose or change
/// its descriptor, mutation target or identity after staging.
pub struct StagedStreamingValue {
    owner: std::rc::Weak<RefCell<Inner>>,
    schema: SchemaViewId,
    pending: Option<StagedStreamingMutation>,
}

struct StagedStreamingMutation {
    op: StagedOp,
    lease: Rc<UploadLease>,
}

/// A capability or batch owns this lease; Drop schedules bounded cleanup on
/// the next maintenance tick without borrowing the database recursively.
pub(super) struct UploadLease {
    pub(super) id: [u8; 32],
    cleanup: Rc<RefCell<Vec<[u8; 32]>>>,
}

impl Drop for UploadLease {
    fn drop(&mut self) {
        self.cleanup.borrow_mut().push(self.id);
    }
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
/// ones produced, as in main's `apply_inline_splices`.
fn lower_splices(
    inner: &Inner,
    column: &crate::values::Column,
    cell: &Sql,
    within: &LargeValueUpdatePage,
    splices: Vec<LargeValueUpdateSplice>,
) -> Result<Vec<Edit>, Error> {
    let kind = crate::large::kind(column);
    let Sql::Text(raw) = cell else {
        return Err(query("partial splice requires a bytes or string cell"));
    };
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
    let current = logical(raw)?;
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
        let mut text = String::from_utf8(inner.chunks.materialize(raw)?)
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
    pub(crate) fn lower_large_mutations(
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
            let (col, mut cell) = self.physical_cell(table, row, &column, identity)?;
            let value = match mutation {
                LargeValueUpdate::Splice {
                    within, splices, ..
                } => {
                    let mut value = None;
                    for splice in lower_splices(self, &col, &cell, &within, splices)? {
                        let next = self.edited(table, &col, cell.clone(), splice)?;
                        cell = match &next {
                            Value::Large(large) => self.large_cell(large)?,
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
                    let Sql::Text(raw) = &cell else {
                        return Err(query(format!("{table}.{column} is null")));
                    };
                    let mut json: serde_json::Value =
                        serde_json::from_slice(&self.chunks.materialize(raw)?)
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
            };
            patch.insert(column, nullable(&col, value));
        }
        Ok(())
    }
}

impl TxRef<'_> {
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
        self.db.inner().require_mutation_admission()?;
        let edits = self.db.in_overlay(self.id, None, |inner, _| {
            let batch = inner.batches.get(&self.id).expect("validated overlay");
            if opts.identity != WriteIdentity::Database && opts.identity != batch.identity {
                return Err(rejected("write identity differs from its transaction"));
            }
            let identity = identity_of(batch.identity);
            let lowered = inner.lower_large_mutations(table, row, &mut patch, mutations, identity);
            let edits = crate::db::LargeEdits {
                derived: std::mem::take(&mut inner.derived),
                pending_nodes: std::mem::take(&mut inner.pending_nodes),
            };
            lowered.map(|()| edits)
        })?;
        self.db.stage_with_edits(
            self.id,
            StagedOp::Update(table.to_owned(), row, patch, opts),
            edits,
        )?;
        Ok(WriteHandle { row, seq: 0 })
    }
}
