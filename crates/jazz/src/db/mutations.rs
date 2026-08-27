//! Row insertion, update, deletion, restoration, and authorization.

use super::*;
use crate::node::{ContributionMergeRequest, ContributionMergeRow};
use crate::protocol::{BranchSelector, BranchViewBase};

/// Ordinary row mutation completed with one Groove-staged scalar.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamingMutationKind {
    /// Create a new branch-local row.
    Insert,
    /// Patch an existing row visible in the selected branch view.
    Update,
    /// Patch the visible row or create it when absent.
    Upsert,
}

type PushPreparation = groove::large_values::PushStreamingPreparation<
    fn(groove::large_values::ContentHash) -> groove::large_values::Locator,
    Box<dyn FnMut(groove::large_values::StagedChunk) -> Result<(), groove::large_values::Error>>,
>;

const MAX_PHYSICAL_TIMESTAMP_MS: u64 = crate::time::HLC_MAX_PHYSICAL_MS;

pub(super) fn validate_updated_at_ms(updated_at_ms: Option<u64>) -> Result<(), Error> {
    if let Some(updated_at_ms) = updated_at_ms
        && updated_at_ms > MAX_PHYSICAL_TIMESTAMP_MS
    {
        return Err(Error::new(
            ErrorCode::WriteRejected,
            format!(
                "updated_at_ms {updated_at_ms} exceeds the packed-HLC physical millisecond range"
            ),
        ));
    }
    Ok(())
}

/// Resumable host-driven upload used by asynchronous bindings such as WASM.
pub struct StreamingValueUpload {
    id: groove::large_values::StagedLargeValueId,
    kind: groove::large_values::LargeValueKind,
    initialized: bool,
    preparation: Option<PushPreparation>,
    emitted: Rc<RefCell<Vec<groove::large_values::StagedChunk>>>,
}

fn large_value_cell_type_error(table: &str, column: &str) -> Error {
    Error::new(
        ErrorCode::Schema,
        format!("{table}.{column} is not a bytes or string cell"),
    )
}

fn checked_usize(value: u64, what: &str) -> Result<usize, Error> {
    usize::try_from(value).map_err(|_| {
        Error::new(
            ErrorCode::Query,
            format!("{what} exceeds addressable memory"),
        )
    })
}

fn owned_byte_range(bytes: Vec<u8>, range: std::ops::Range<u64>) -> Result<Vec<u8>, Error> {
    let start = checked_usize(range.start, "range start")?;
    let end = checked_usize(range.end, "range end")?;
    if start > end || end > bytes.len() {
        return Err(Error::new(ErrorCode::Query, "value range is out of bounds"));
    }
    Ok(bytes[start..end].to_vec())
}

fn owned_utf16_range(text: &str, range: std::ops::Range<u64>) -> Result<String, Error> {
    let total = text.encode_utf16().count() as u64;
    if range.start > range.end || range.end > total {
        return Err(Error::new(
            ErrorCode::Query,
            "UTF-16 range is out of bounds",
        ));
    }
    let byte_at = |target: u64| {
        if target == total {
            return Some(text.len());
        }
        text.char_indices()
            .scan(0_u64, |offset, (byte, character)| {
                let current = *offset;
                *offset += character.len_utf16() as u64;
                Some((current, byte))
            })
            .find_map(|(offset, byte)| (offset == target).then_some(byte))
    };
    let Some(start) = byte_at(range.start) else {
        return Err(Error::new(
            ErrorCode::Query,
            "UTF-16 range starts inside a surrogate pair",
        ));
    };
    let Some(end) = byte_at(range.end) else {
        return Err(Error::new(
            ErrorCode::Query,
            "UTF-16 range ends inside a surrogate pair",
        ));
    };
    Ok(text[start..end].to_owned())
}

fn splice_owned_bytes(
    bytes: &mut Vec<u8>,
    offset: u64,
    delete_length: u64,
    insert: Vec<u8>,
) -> Result<(), Error> {
    let start = checked_usize(offset, "splice offset")?;
    let end_u64 = offset
        .checked_add(delete_length)
        .ok_or_else(|| Error::new(ErrorCode::Query, "splice range overflows"))?;
    let end = checked_usize(end_u64, "splice end")?;
    if start > end || end > bytes.len() {
        return Err(Error::new(
            ErrorCode::Query,
            "splice range is out of bounds",
        ));
    }
    bytes.splice(start..end, insert);
    Ok(())
}

fn unwrap_present_nullable(value: Value) -> (Value, bool) {
    match value {
        Value::Nullable(Some(value)) => (*value, true),
        value => (value, false),
    }
}

fn preserve_nullable(value: Value, nullable: bool) -> Value {
    if nullable {
        Value::Nullable(Some(Box::new(value)))
    } else {
        value
    }
}

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Read a byte range from an ordinary bytes or string cell without
    /// exposing its physical representation. Inline and indirect cells share
    /// this API; only the intersecting chunk paths are requested.
    pub async fn read_value_range(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, Error> {
        let (value, _) =
            unwrap_present_nullable(self.authorized_physical_cell(table, row, column).await?);
        match value {
            Value::Bytes(bytes) => owned_byte_range(bytes, range),
            Value::String(text) => owned_byte_range(text.into_bytes(), range),
            Value::Large(value_ref) => Ok(self
                .node
                .node
                .lock()
                .await
                .read_large_value_range(&value_ref, range)
                .await?),
            _ => Err(large_value_cell_type_error(table, column)),
        }
    }

    /// Read a UTF-16 code-unit range from an ordinary string cell. Boundaries
    /// that split a surrogate pair fail safely.
    pub async fn read_text_utf16_range(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        range: std::ops::Range<u64>,
    ) -> Result<String, Error> {
        let (value, _) =
            unwrap_present_nullable(self.authorized_physical_cell(table, row, column).await?);
        match value {
            Value::String(text) => owned_utf16_range(&text, range),
            Value::Large(value_ref) => Ok(self
                .node
                .node
                .lock()
                .await
                .read_large_text_utf16_range(&value_ref, range)
                .await?),
            _ => Err(large_value_cell_type_error(table, column)),
        }
    }

    /// Resolve a JSON Pointer against the literal source in a string/JSON
    /// cell, returning an owned host-safe value.
    pub async fn read_json_pointer(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        pointer: &str,
    ) -> Result<Option<serde_json::Value>, Error> {
        let (value, _) =
            unwrap_present_nullable(self.authorized_physical_cell(table, row, column).await?);
        match value {
            Value::String(text) => {
                let value: serde_json::Value = serde_json::from_str(&text).map_err(|error| {
                    Error::new(ErrorCode::Query, format!("invalid stored JSON: {error}"))
                })?;
                Ok(value.pointer(pointer).cloned())
            }
            Value::Large(value_ref) => Ok(self
                .node
                .node
                .lock()
                .await
                .read_large_json_pointer(&value_ref, pointer)
                .await?),
            _ => Err(large_value_cell_type_error(table, column)),
        }
    }

    /// Append encoded bytes to an ordinary bytes or UTF-8 string cell and
    /// publish the resulting descriptor as one ordinary authorized row update.
    pub async fn append_value(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        bytes: Vec<u8>,
    ) -> Result<WriteHandle<S>, Error> {
        let (value, nullable) =
            unwrap_present_nullable(self.authorized_physical_cell(table, row, column).await?);
        match value {
            Value::Bytes(mut current) => {
                current.extend(bytes);
                self.update(
                    table,
                    row,
                    BTreeMap::from([(
                        column.to_owned(),
                        preserve_nullable(Value::Bytes(current), nullable),
                    )]),
                    Default::default(),
                )
                .await
            }
            Value::String(mut current) => {
                let suffix = String::from_utf8(bytes).map_err(|_| {
                    Error::new(ErrorCode::Schema, "string append is not valid UTF-8")
                })?;
                current.push_str(&suffix);
                self.update(
                    table,
                    row,
                    BTreeMap::from([(
                        column.to_owned(),
                        preserve_nullable(Value::String(current), nullable),
                    )]),
                    Default::default(),
                )
                .await
            }
            Value::Large(value_ref) => {
                let staged = self
                    .node
                    .node
                    .lock()
                    .await
                    .append_and_stage_large_value(value_ref, bytes)
                    .await?;
                self.write_staged_large_value_update(table, row, column, staged, nullable)
                    .await
            }
            _ => Err(large_value_cell_type_error(table, column)),
        }
    }

    /// Apply a byte-coordinate splice to an ordinary bytes or UTF-8 string
    /// cell and publish it through the normal Jazz row-write lifecycle.
    pub async fn splice_value(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        offset: u64,
        delete_length: u64,
        insert: Vec<u8>,
    ) -> Result<WriteHandle<S>, Error> {
        let (value, nullable) =
            unwrap_present_nullable(self.authorized_physical_cell(table, row, column).await?);
        match value {
            Value::Bytes(mut current) => {
                splice_owned_bytes(&mut current, offset, delete_length, insert)?;
                self.update(
                    table,
                    row,
                    BTreeMap::from([(
                        column.to_owned(),
                        preserve_nullable(Value::Bytes(current), nullable),
                    )]),
                    Default::default(),
                )
                .await
            }
            Value::String(current) => {
                let mut bytes = current.into_bytes();
                splice_owned_bytes(&mut bytes, offset, delete_length, insert)?;
                let text = String::from_utf8(bytes).map_err(|_| {
                    Error::new(ErrorCode::Schema, "string splice is not valid UTF-8")
                })?;
                self.update(
                    table,
                    row,
                    BTreeMap::from([(
                        column.to_owned(),
                        preserve_nullable(Value::String(text), nullable),
                    )]),
                    Default::default(),
                )
                .await
            }
            Value::Large(value_ref) => {
                let staged = self
                    .node
                    .node
                    .lock()
                    .await
                    .edit_and_stage_large_value(value_ref, offset, delete_length, insert)
                    .await?;
                self.write_staged_large_value_update(table, row, column, staged, nullable)
                    .await
            }
            _ => Err(large_value_cell_type_error(table, column)),
        }
    }

    async fn authorized_physical_cell(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
    ) -> Result<Value, Error> {
        let table_schema = self.table_schema(table)?;
        if !table_schema
            .columns
            .iter()
            .any(|candidate| candidate.name == column)
        {
            return Err(Error::new(
                ErrorCode::Schema,
                format!("unknown column {table}.{column}"),
            ));
        }
        if self.authorize_read_for_identity(table, row, self.identity.author)?
            != PermissionAdvice::Allowed
        {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "large-value cell is not observed",
            ));
        }
        self.node
            .node
            .lock()
            .await
            .current_physical_cell_in_schema(self.schema_version_id, table, row, column)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::NotObserved, "large-value cell is not observed"))
    }

    async fn write_staged_large_value_update(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        staged: groove::large_values::StagedLargeValue,
        nullable: bool,
    ) -> Result<WriteHandle<S>, Error> {
        let published = {
            let mut node = self.node.node.lock().await;
            let mut cells = node
                .current_physical_cells_in_schema(self.schema_version_id, table, row)
                .await?
                .ok_or_else(|| {
                    Error::new(ErrorCode::NotObserved, "large-value row is not observed")
                })?;
            cells.insert(
                column.to_owned(),
                preserve_nullable(Value::Large(staged.value_ref.clone()), nullable),
            );
            let parents = node
                .local_content_winner_tx_id_in_schema(self.schema_version_id, table, row)
                .await?
                .into_iter()
                .collect();
            let commit = MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(self.identity.author)
                .parents(parents)
                .cells(cells)
                .authored_columns(BTreeSet::from([column.to_owned()]));
            let commit = node
                .seal_large_value_update(commit, column, staged, self.schema_version_id)
                .await?;
            node.commit_mergeable_in_schema(self.schema_version_id, commit)
                .await?
        };
        self.finish_published_write(row, published).await
    }

    /// Calculate and commit novel contributions from one exact branch key into
    /// another. This requires a history-complete database and emits an ordinary
    /// mergeable transaction when the target does not already represent every
    /// selected contribution.
    pub async fn merge_branch_contributions(
        &self,
        source: BranchSelector,
        target: BranchSelector,
        rows: impl IntoIterator<Item = ContributionMergeRow>,
    ) -> Result<Option<TxId>, Error> {
        let rows = rows.into_iter().collect::<Vec<_>>();
        let representative_row = rows.first().map(|row| row.row_uuid);
        let tx_id = self
            .node
            .node
            .lock()
            .await
            .merge_branch_contributions(ContributionMergeRequest {
                source,
                target,
                rows,
                made_by: self.identity.author,
                permission_subject: Some(self.identity.author),
                now_ms: self.next_now_ms(),
            })
            .await?;
        let Some(published) = tx_id else {
            return Ok(None);
        };
        let tx_id = published.tx_id;
        self.finish_published_write(
            representative_row.expect("a published contribution merge has at least one row"),
            published,
        )
        .await?;
        Ok(Some(tx_id))
    }

    /// Insert a row locally.
    ///
    /// By default this uses the database identity, root branch, current
    /// timestamp, and a generated UUIDv7 row id. Every supported variation is
    /// selected through [`InsertOptions`]; there are no parallel insert paths.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// let write = block_on(db.insert(
    ///     "todos",
    ///     jazz::row! { title: "new todo", done: false },
    ///     Default::default(),
    /// ))?;
    /// let row = write.row_uuid();
    /// block_on(write.wait(DurabilityTier::Local))?;
    ///
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.read(&todos)?.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn insert(
        &self,
        table: &str,
        cells: RowCells,
        options: InsertOptions,
    ) -> Result<WriteHandle<S>, Error> {
        let InsertOptions {
            row_id,
            identity,
            target,
            updated_at_ms,
        } = options;
        self.reject_attributed_branch_target(
            identity,
            matches!(target, ExactWriteTarget::Branch(_)),
        )?;
        validate_updated_at_ms(updated_at_ms)?;
        let supplied_row_id = row_id.is_some();
        let row = row_id.unwrap_or_else(|| self.row_id_source.borrow_mut().next_row_id());
        let (made_by, permission_subject) = self.resolve_write_identity(identity)?;
        let branch = target.branch();

        if supplied_row_id {
            match target {
                ExactWriteTarget::Root => {
                    self.ensure_row_absent(table, row, permission_subject.unwrap_or(made_by))
                        .await?
                }
                ExactWriteTarget::Branch(_) => {
                    self.ensure_exact_branch_row_absent(table, &branch, row)
                        .await?
                }
            }
        }

        self.write_mergeable_at_ms_with_authorship_in_branch(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            Vec::new(),
            None,
            None,
            updated_at_ms.unwrap_or_else(|| self.next_now_ms()),
            branch,
        )
        .await
    }

    /// Trusted-backend-only root insert retaining backend admission while
    /// recording `made_by` as external provenance.
    #[doc(hidden)]
    pub async fn insert_with_id_attributed(
        &self,
        made_by: AuthorSubject,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.insert(
            table,
            cells,
            InsertOptions {
                row_id: Some(row),
                identity: WriteIdentity::Attribution(made_by),
                ..Default::default()
            },
        )
        .await
    }

    /// Stream one large scalar into a newly inserted row without retaining the
    /// complete logical value in Jazz memory.
    ///
    /// `cells` contains the other row fields; `column` is inserted from
    /// `reader`. Groove incrementally constructs and stages the immutable tree
    /// with bounded buffering. Jazz publishes the resulting descriptor only
    /// after the reader reaches EOF and text/JSON validation succeeds. A write
    /// rejected by row policy leaves only the ordinary expiring staging claim.
    ///
    /// This native-reader API is not available on WebAssembly. Browser hosts
    /// require an asynchronous stream adapter rather than `std::io::Read`.
    #[cfg(not(target_family = "wasm"))]
    pub async fn insert_streaming_value<R>(
        &self,
        table: &str,
        cells: RowCells,
        column: &str,
        reader: R,
    ) -> Result<WriteHandle<S>, Error>
    where
        R: std::io::Read + Send + 'static,
    {
        let row = self.row_id_source.borrow_mut().next_row_id();
        self.insert_streaming_value_with_id(table, row, cells, column, reader)
            .await
    }

    /// Stream one large scalar into a newly inserted row with an explicit row
    /// id. Binding adapters use this to choose the public id before consuming
    /// an asynchronous host stream.
    #[cfg(not(target_family = "wasm"))]
    pub async fn insert_streaming_value_with_id<R>(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        column: &str,
        reader: R,
    ) -> Result<WriteHandle<S>, Error>
    where
        R: std::io::Read + Send + 'static,
    {
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

    /// Complete an insert, update, or upsert with one streamed scalar.
    /// Binding adapters supply the same identity, timestamp, and branch view
    /// that their ordinary mutation path would use.
    #[cfg(not(target_family = "wasm"))]
    #[allow(clippy::too_many_arguments)]
    pub async fn write_streaming_value_with_id<R>(
        &self,
        mutation: StreamingMutationKind,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        column: &str,
        reader: R,
        identity: Option<AuthorSubject>,
        now_ms: Option<u64>,
        head: Option<BranchSelector>,
        base: Option<BranchViewBase>,
    ) -> Result<WriteHandle<S>, Error>
    where
        R: std::io::Read + Send + 'static,
    {
        use futures::{SinkExt, StreamExt};

        let mut upload = self.begin_streaming_value_upload(table, &cells, column)?;
        let (mut bytes_tx, mut bytes_rx) = futures::channel::mpsc::channel::<Vec<u8>>(8);
        let (result_tx, result_rx) = futures::channel::oneshot::channel();
        std::thread::spawn(move || {
            let mut reader = reader;
            let mut buffer = vec![0_u8; 64 * 1024];
            let result = loop {
                match std::io::Read::read(&mut reader, &mut buffer) {
                    Ok(0) => break Ok(()),
                    Ok(read) => {
                        if futures::executor::block_on(bytes_tx.send(buffer[..read].to_vec()))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(_) => break Err(groove::large_values::Error::MalformedScalar),
                }
            };
            drop(bytes_tx);
            let _ = result_tx.send(result);
        });

        while let Some(bytes) = bytes_rx.next().await {
            if let Err(error) = self.push_streaming_value_upload(&mut upload, &bytes).await {
                drop(bytes_rx);
                return Err(error);
            }
        }

        match result_rx.await {
            Ok(Ok(())) => {
                self.finish_streaming_value_upload(
                    upload, mutation, table, row, cells, column, identity, now_ms, head, base, None,
                )
                .await
            }
            Ok(Err(error)) => {
                self.abort_streaming_value_upload(upload).await?;
                Err(crate::node::Error::from(error).into())
            }
            Err(_) => {
                self.abort_streaming_value_upload(upload).await?;
                Err(crate::node::Error::from(groove::large_values::Error::MalformedScalar).into())
            }
        }
    }

    /// Begin a resumable push preparation without retaining the logical value.
    pub fn begin_streaming_value_upload(
        &self,
        table: &str,
        cells: &RowCells,
        column: &str,
    ) -> Result<StreamingValueUpload, Error> {
        let (kind, _) = self.validate_streaming_column(table, cells, column)?;
        let emitted = Rc::new(RefCell::new(Vec::new()));
        let emitted_for_stage = Rc::clone(&emitted);
        let preparation = groove::large_values::PushStreamingPreparation::new(
            kind,
            Box::new(move |chunk| {
                emitted_for_stage.borrow_mut().push(chunk);
                Ok(())
            }) as Box<dyn FnMut(_) -> _>,
        );
        Ok(StreamingValueUpload {
            id: groove::large_values::StagedLargeValueId(*uuid::Uuid::new_v4().as_bytes()),
            kind,
            initialized: false,
            preparation: Some(preparation),
            emitted,
        })
    }

    /// Feed one host chunk and durably stage every tree node finalized by it.
    pub async fn push_streaming_value_upload(
        &self,
        upload: &mut StreamingValueUpload,
        bytes: &[u8],
    ) -> Result<(), Error> {
        let initialized_now = !upload.initialized;
        if !upload.initialized {
            self.node
                .node
                .lock()
                .await
                .begin_streaming_large_value_upload(upload.id, upload.kind)
                .await?;
            upload.initialized = true;
        }
        let push_result = upload
            .preparation
            .as_mut()
            .ok_or_else(|| Error::new(ErrorCode::Schema, "streaming upload is closed"))?
            .push(bytes);
        if let Err(error) = push_result {
            upload.preparation.take();
            self.node
                .node
                .lock()
                .await
                .evict_pending_large_value_upload(upload.id)
                .await?;
            return Err(crate::node::Error::from(error).into());
        }
        let chunks = std::mem::take(&mut *upload.emitted.borrow_mut());
        // The begin operation already persisted the journal under the same
        // node lock. Avoid immediately re-admitting an empty first flush;
        // every subsequent push, including an empty one, proves liveness.
        if initialized_now && chunks.is_empty() {
            return Ok(());
        }
        let stage_result = {
            let node = self.node.node.lock().await;
            node.stage_large_value_chunk_batch(upload.id, upload.kind, chunks)
                .await
        };
        if let Err(error) = stage_result {
            upload.preparation.take();
            let _ = self
                .node
                .node
                .lock()
                .await
                .evict_pending_large_value_upload(upload.id)
                .await;
            return Err(error.into());
        }
        Ok(())
    }

    /// Abort a resumable upload and release its persisted pending retainers.
    pub async fn abort_streaming_value_upload(
        &self,
        mut upload: StreamingValueUpload,
    ) -> Result<(), Error> {
        upload.preparation.take();
        self.node
            .node
            .lock()
            .await
            .evict_pending_large_value_upload(upload.id)
            .await?;
        Ok(())
    }

    /// Finish a resumable upload and publish its ordinary row mutation.
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_streaming_value_upload(
        &self,
        mut upload: StreamingValueUpload,
        mutation: StreamingMutationKind,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        column: &str,
        identity: Option<AuthorSubject>,
        now_ms: Option<u64>,
        head: Option<BranchSelector>,
        base: Option<BranchViewBase>,
        attribution: Option<AuthorSubject>,
    ) -> Result<WriteHandle<S>, Error> {
        let attribution_rejection = match attribution {
            Some(author) if author != self.identity.author && !self.backend_attribution => {
                Some("attribution requires a trusted serving node")
            }
            Some(_) if identity.is_some() => Some(
                "backend-attributed streaming mutations cannot override backend admission identity",
            ),
            Some(_) if head.is_some() || base.is_some() => {
                Some("backend-attributed streaming mutations do not support branch targets")
            }
            _ => None,
        };
        if let Some(message) = attribution_rejection {
            self.abort_streaming_value_upload(upload).await?;
            return Err(Error::new(ErrorCode::WriteRejected, message));
        }
        // Provenance is external, but trusted backend admission remains this
        // Db's identity all the way through the final policy-bearing commit.
        let identity = attribution.map(|_| self.identity.author).or(identity);
        if !upload.initialized {
            self.node
                .node
                .lock()
                .await
                .begin_streaming_large_value_upload(upload.id, upload.kind)
                .await?;
            upload.initialized = true;
        }
        let preparation = upload
            .preparation
            .take()
            .ok_or_else(|| Error::new(ErrorCode::Schema, "streaming upload is closed"))?;
        let (value_ref, _) = match preparation.finish() {
            Ok(finished) => finished,
            Err(error) => {
                let _ = self
                    .node
                    .node
                    .lock()
                    .await
                    .evict_pending_large_value_upload(upload.id)
                    .await;
                return Err(crate::node::Error::from(error).into());
            }
        };
        let chunks = std::mem::take(&mut *upload.emitted.borrow_mut());
        let staged_result = {
            let node = self.node.node.lock().await;
            match node
                .stage_large_value_chunk_batch(upload.id, upload.kind, chunks)
                .await
            {
                Ok(()) => node.finalize_large_value_upload(upload.id, value_ref).await,
                Err(error) => Err(error),
            }
        };
        let staged = match staged_result {
            Ok(staged) => staged,
            Err(error) => {
                // Cleanup is best-effort here so the terminal operation reports
                // its original staging/finalization failure.
                let _ = self
                    .node
                    .node
                    .lock()
                    .await
                    .evict_pending_large_value_upload(upload.id)
                    .await;
                return Err(error.into());
            }
        };
        let nullable = match self.validate_streaming_column(table, &cells, column) {
            Ok((expected_kind, nullable)) if expected_kind == staged.value_ref.kind => nullable,
            Ok(_) => {
                let _ = self
                    .node
                    .node
                    .lock()
                    .await
                    .evict_staged_large_value(staged.id)
                    .await;
                return Err(large_value_cell_type_error(table, column));
            }
            Err(error) => {
                let _ = self
                    .node
                    .node
                    .lock()
                    .await
                    .evict_staged_large_value(staged.id)
                    .await;
                return Err(error);
            }
        };
        let staged_id = staged.id;
        let published = self
            .publish_streaming_value_with_id(
                mutation,
                table,
                row,
                cells,
                column,
                staged,
                nullable,
                identity,
                now_ms,
                head,
                base,
                attribution,
            )
            .await;
        if published.is_err() {
            // Finalization transfers the pending journal into a staged root,
            // but publication is still fallible (for example a duplicate
            // insert or an invalid branch view). Do not make cleanup mask the
            // caller-visible admission error.
            let _ = self
                .node
                .node
                .lock()
                .await
                .evict_staged_large_value(staged_id)
                .await;
        }
        published
    }

    fn validate_streaming_column(
        &self,
        table: &str,
        cells: &RowCells,
        column: &str,
    ) -> Result<(groove::large_values::LargeValueKind, bool), Error> {
        if cells.contains_key(column) {
            return Err(Error::new(
                ErrorCode::Schema,
                format!("streamed column {table}.{column} was also supplied in row cells"),
            ));
        }
        let table_schema = self.table_schema(table)?;
        let column = table_schema
            .columns
            .iter()
            .find(|candidate| candidate.name == column)
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::Schema,
                    format!("unknown streamed column {table}.{column}"),
                )
            })?;
        let (column_type, nullable) = match &column.column_type {
            groove::records::ValueType::Nullable(inner) => (inner.as_ref(), true),
            column_type => (column_type, false),
        };
        let kind = match column.large_value_kind {
            crate::schema::LargeValueSemanticKind::Bytes
                if matches!(column_type, groove::records::ValueType::Bytes) =>
            {
                groove::large_values::LargeValueKind::Bytes
            }
            crate::schema::LargeValueSemanticKind::String
                if matches!(column_type, groove::records::ValueType::String) =>
            {
                groove::large_values::LargeValueKind::String
            }
            crate::schema::LargeValueSemanticKind::Json
                if matches!(column_type, groove::records::ValueType::String) =>
            {
                groove::large_values::LargeValueKind::Json
            }
            _ => return Err(large_value_cell_type_error(table, &column.name)),
        };
        Ok((kind, nullable))
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_streaming_value_with_id(
        &self,
        mutation: StreamingMutationKind,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        column: &str,
        staged: groove::large_values::StagedLargeValue,
        nullable: bool,
        identity: Option<AuthorSubject>,
        now_ms: Option<u64>,
        head: Option<BranchSelector>,
        base: Option<BranchViewBase>,
        attribution: Option<AuthorSubject>,
    ) -> Result<WriteHandle<S>, Error> {
        let made_by = attribution.or(identity).unwrap_or(self.identity.author);
        let permission_subject = identity;
        let branch = head.clone().unwrap_or_default();
        let (mut cells, parents, authored_columns, inserting) = match mutation {
            StreamingMutationKind::Insert => {
                if head.is_some() {
                    self.ensure_exact_branch_row_absent(table, &branch, row)
                        .await?;
                } else {
                    self.ensure_row_absent(table, row, identity.unwrap_or(self.identity.author))
                        .await?;
                }
                (cells, Vec::new(), None, true)
            }
            StreamingMutationKind::Update => {
                let authored = cells
                    .keys()
                    .cloned()
                    .chain(std::iter::once(column.to_owned()))
                    .collect();
                if let Some(head) = head.as_ref() {
                    let visible_to_session = match identity {
                        Some(identity) => Some(
                            self.visible_branch_view_cells_for_identity(
                                table,
                                head,
                                base.as_ref(),
                                row,
                                identity,
                            )
                            .await?
                            .ok_or_else(|| read_for_write_denied("UPDATE", table))?,
                        ),
                        None => None,
                    };
                    let mut node = self.node.node.lock().await;
                    if let Some(mut current) = node
                        .visible_current_cells_in_branch(table, head, row)
                        .await?
                    {
                        let parent = node
                            .local_content_winner_tx_id_in_branch(table, head, row)
                            .await?;
                        drop(node);
                        if let Some(visible_to_session) = visible_to_session {
                            current = visible_to_session;
                        }
                        current.extend(cells);
                        (current, parent.into_iter().collect(), Some(authored), false)
                    } else {
                        let Some(mut inherited) = node
                            .visible_current_cells_in_branch_view(table, head, base.as_ref(), row)
                            .await?
                        else {
                            return Err(Error::new(
                                ErrorCode::NotObserved,
                                format!("row is not visible in branch view: {}", row.0),
                            ));
                        };
                        drop(node);
                        if let Some(visible_to_session) = visible_to_session {
                            inherited = visible_to_session;
                        }
                        inherited.extend(cells);
                        (inherited, Vec::new(), Some(authored), true)
                    }
                } else {
                    let (merged, parent, _) = if let Some(identity) = identity {
                        self.merge_existing_cells_for_identity(table, row, cells, identity)
                            .await?
                    } else {
                        self.merge_existing_cells(table, row, cells).await?
                    };
                    (merged, parent.into_iter().collect(), Some(authored), false)
                }
            }
            StreamingMutationKind::Upsert => {
                let authored = cells
                    .keys()
                    .cloned()
                    .chain(std::iter::once(column.to_owned()))
                    .collect();
                if let Some(head) = head.as_ref() {
                    let visible_to_session = match identity {
                        Some(identity) => {
                            self.visible_branch_view_cells_for_identity(
                                table,
                                head,
                                base.as_ref(),
                                row,
                                identity,
                            )
                            .await?
                        }
                        None => None,
                    };
                    let mut node = self.node.node.lock().await;
                    if let Some(mut current) = node
                        .visible_current_cells_in_branch(table, head, row)
                        .await?
                    {
                        let parent = node
                            .local_content_winner_tx_id_in_branch(table, head, row)
                            .await?;
                        drop(node);
                        if identity.is_some() && visible_to_session.is_none() {
                            return Err(read_for_write_denied("UPSERT", table));
                        }
                        if let Some(visible_to_session) = visible_to_session {
                            current = visible_to_session;
                        }
                        current.extend(cells);
                        (current, parent.into_iter().collect(), Some(authored), false)
                    } else if let Some(mut inherited) = node
                        .visible_current_cells_in_branch_view(table, head, base.as_ref(), row)
                        .await?
                    {
                        drop(node);
                        if identity.is_some() && visible_to_session.is_none() {
                            return Err(read_for_write_denied("UPSERT", table));
                        }
                        if let Some(visible_to_session) = visible_to_session {
                            inherited = visible_to_session;
                        }
                        inherited.extend(cells);
                        (inherited, Vec::new(), Some(authored), true)
                    } else {
                        drop(node);
                        (cells, Vec::new(), None, true)
                    }
                } else {
                    self.ensure_row_not_deleted(table, row).await?;
                    let exists = if let Some(identity) = identity {
                        self.upsert_target_for_trusted_identity(table, row, identity)
                            .await?
                            .is_some()
                    } else {
                        self.upsert_target_for_client_identity(table, row, self.identity.author)
                            .await?
                            .is_some()
                    };
                    if exists {
                        let (merged, parent, _) = if let Some(identity) = identity {
                            self.merge_existing_cells_for_identity(table, row, cells, identity)
                                .await?
                        } else {
                            self.merge_existing_cells(table, row, cells).await?
                        };
                        (merged, parent.into_iter().collect(), Some(authored), false)
                    } else {
                        (cells, Vec::new(), None, true)
                    }
                }
            }
        };
        if inserting {
            cells = self.apply_insert_defaults(table, cells)?;
        }
        let mut commit =
            MergeableCommit::new(table, row, now_ms.unwrap_or_else(|| self.next_now_ms()))
                .branch(branch)
                .made_by(made_by)
                .parents(parents)
                .cells(cells);
        if let Some(authored_columns) = authored_columns {
            commit = commit.authored_columns(authored_columns);
        }
        if let Some(permission_subject) = permission_subject {
            commit = commit.permission_subject(permission_subject);
        }
        let published = {
            let mut node = self.node.node.lock().await;
            let commit = node
                .seal_inherited_large_values(commit, self.schema_version_id, true)
                .await?
                .staged_large_cell(column, staged, nullable);
            node.commit_mergeable_in_schema(self.schema_version_id, commit)
                .await?
        };
        self.finish_published_write(row, published).await
    }

    /// Advise whether an insert may be allowed.
    ///
    /// A `Db` is ordinarily a client-local replica, whose policy evidence may
    /// be incomplete. It therefore never turns a local policy evaluation into
    /// an allow/deny result. Use an explicitly trusted serving authority for a
    /// final decision.
    pub fn can_insert(&self, _table: &str, _cells: RowCells) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    /// Evaluate an insert for a test-only serving-path probe without writing.
    #[cfg(test)]
    pub(crate) async fn authorize_insert_for_identity(
        &self,
        table: &str,
        cells: RowCells,
        identity: AuthorSubject,
    ) -> Result<PermissionAdvice, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_mergeable_write_allows_for_view(
                &self.schema,
                MergeableCommit::new(table, RowUuid::from_bytes([0; 16]), 0)
                    .made_by(identity)
                    .permission_subject(identity)
                    .cells(cells),
            )
            .await
            .map(|allowed| {
                if allowed {
                    PermissionAdvice::Allowed
                } else {
                    PermissionAdvice::Denied
                }
            })
            .map_err(Into::into)
    }

    /// Update one row, optionally through a branch view.
    pub async fn update(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        options: UpdateOptions,
    ) -> Result<WriteHandle<S>, Error> {
        let UpdateOptions {
            identity,
            target,
            updated_at_ms,
        } = options;
        self.reject_attributed_branch_target(
            identity,
            matches!(target, WriteTarget::BranchView { .. }),
        )?;
        validate_updated_at_ms(updated_at_ms)?;
        let (made_by, permission_subject) = self.resolve_write_identity(identity)?;
        let now_ms = updated_at_ms.unwrap_or_else(|| self.next_now_ms());

        match target {
            WriteTarget::Root => {
                if patch.is_empty() {
                    return match identity {
                        WriteIdentity::Database | WriteIdentity::Attribution(_) => {
                            self.no_op_update_handle_for_client(
                                table,
                                row,
                                permission_subject.unwrap_or(made_by),
                            )
                            .await
                        }
                        WriteIdentity::Session(author) => {
                            self.no_op_update_handle_for_identity(table, row, author)
                                .await
                        }
                    };
                }
                let (cells, parent, authored_columns) = match identity {
                    WriteIdentity::Database | WriteIdentity::Attribution(_) => {
                        self.merge_existing_cells(table, row, patch).await?
                    }
                    WriteIdentity::Session(author) => {
                        self.merge_existing_cells_for_identity(table, row, patch, author)
                            .await?
                    }
                };
                self.write_mergeable_at_ms_with_authorship(
                    made_by,
                    permission_subject,
                    table,
                    row,
                    cells,
                    parent.into_iter().collect(),
                    None,
                    Some(authored_columns),
                    now_ms,
                )
                .await
            }
            WriteTarget::BranchView { head, base } => {
                if patch.is_empty() {
                    return Err(Error::new(
                        ErrorCode::Schema,
                        "branch update requires at least one authored column",
                    ));
                }
                let visible_to_session = match identity {
                    WriteIdentity::Session(author) => Some(
                        self.visible_branch_view_cells_for_identity(
                            table,
                            &head,
                            base.as_ref(),
                            row,
                            author,
                        )
                        .await?
                        .ok_or_else(|| read_for_write_denied("UPDATE", table))?,
                    ),
                    WriteIdentity::Database | WriteIdentity::Attribution(_) => None,
                };
                let local = self
                    .node
                    .node
                    .lock()
                    .await
                    .visible_current_cells_in_branch(table, &head, row)
                    .await?;
                let (mut cells, parents, authored_columns) = if let Some(cells) = local {
                    let parent = self
                        .node
                        .node
                        .lock()
                        .await
                        .local_content_winner_tx_id_in_branch(table, &head, row)
                        .await?;
                    (
                        visible_to_session.unwrap_or(cells),
                        parent.into_iter().collect(),
                        Some(patch.keys().cloned().collect()),
                    )
                } else {
                    let inherited = self
                        .node
                        .node
                        .lock()
                        .await
                        .visible_current_cells_in_branch_view(table, &head, base.as_ref(), row)
                        .await?
                        .ok_or_else(|| {
                            Error::new(
                                ErrorCode::NotObserved,
                                format!("row is not visible in branch view: {}", row.0),
                            )
                        })?;
                    (visible_to_session.unwrap_or(inherited), Vec::new(), None)
                };
                cells.extend(patch);
                self.write_mergeable_at_ms_with_authorship_in_branch(
                    made_by,
                    permission_subject,
                    table,
                    row,
                    cells,
                    parents,
                    None,
                    authored_columns,
                    now_ms,
                    head,
                )
                .await
            }
        }
    }

    #[doc(hidden)]
    pub async fn update_attributed(
        &self,
        made_by: AuthorSubject,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.update(
            table,
            row,
            patch,
            UpdateOptions {
                identity: WriteIdentity::Attribution(made_by),
                ..Default::default()
            },
        )
        .await
    }

    /// Insert or update one row through a single implementation.
    pub async fn upsert(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        options: UpsertOptions,
    ) -> Result<WriteHandle<S>, Error> {
        let UpsertOptions {
            identity,
            target,
            updated_at_ms,
        } = options;
        self.reject_attributed_branch_target(
            identity,
            matches!(target, ExactWriteTarget::Branch(_)),
        )?;
        validate_updated_at_ms(updated_at_ms)?;
        let (made_by, permission_subject) = self.resolve_write_identity(identity)?;
        let now_ms = updated_at_ms.unwrap_or_else(|| self.next_now_ms());
        let branch = target.branch();

        let (cells, parents, authored_columns) = match target {
            ExactWriteTarget::Root => {
                self.ensure_row_not_deleted(table, row).await?;
                let exists = match identity {
                    WriteIdentity::Database | WriteIdentity::Attribution(_) => self
                        .upsert_target_for_client_identity(
                            table,
                            row,
                            permission_subject.unwrap_or(made_by),
                        )
                        .await?
                        .is_some(),
                    WriteIdentity::Session(author) => self
                        .upsert_target_for_trusted_identity(table, row, author)
                        .await?
                        .is_some(),
                };
                if exists {
                    let (cells, parent, authored_columns) = match identity {
                        WriteIdentity::Database | WriteIdentity::Attribution(_) => {
                            self.merge_existing_cells(table, row, cells).await?
                        }
                        WriteIdentity::Session(author) => {
                            self.merge_existing_cells_for_identity(table, row, cells, author)
                                .await?
                        }
                    };
                    (cells, parent.into_iter().collect(), Some(authored_columns))
                } else {
                    (cells, Vec::new(), None)
                }
            }
            ExactWriteTarget::Branch(_) => {
                let visible_to_session = match identity {
                    WriteIdentity::Session(author) => {
                        self.visible_branch_view_cells_for_identity(
                            table, &branch, None, row, author,
                        )
                        .await?
                    }
                    WriteIdentity::Database | WriteIdentity::Attribution(_) => None,
                };
                let mut node = self.node.node.lock().await;
                let existing = node
                    .visible_current_cells_in_branch(table, &branch, row)
                    .await?;
                let parent = if existing.is_some() {
                    node.local_content_winner_tx_id_in_branch(table, &branch, row)
                        .await?
                } else {
                    None
                };
                drop(node);
                if let Some(mut existing) = existing {
                    if matches!(identity, WriteIdentity::Session(_)) && visible_to_session.is_none()
                    {
                        return Err(read_for_write_denied("UPSERT", table));
                    }
                    if cells.is_empty() {
                        return Err(Error::new(
                            ErrorCode::Schema,
                            "branch upsert update requires at least one authored column",
                        ));
                    }
                    if let Some(visible_to_session) = visible_to_session {
                        existing = visible_to_session;
                    }
                    let authored_columns = cells.keys().cloned().collect();
                    existing.extend(cells);
                    (
                        existing,
                        parent.into_iter().collect(),
                        Some(authored_columns),
                    )
                } else {
                    (cells, Vec::new(), None)
                }
            }
        };

        self.write_mergeable_at_ms_with_authorship_in_branch(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            now_ms,
            branch,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn upsert_attributed(
        &self,
        made_by: AuthorSubject,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.upsert(
            table,
            row,
            cells,
            UpsertOptions {
                identity: WriteIdentity::Attribution(made_by),
                ..Default::default()
            },
        )
        .await
    }

    /// Soft-delete one row, optionally through a branch view.
    pub async fn delete(
        &self,
        table: &str,
        row: RowUuid,
        options: DeleteOptions,
    ) -> Result<WriteHandle<S>, Error> {
        let DeleteOptions {
            identity,
            target,
            updated_at_ms,
        } = options;
        self.reject_attributed_branch_target(
            identity,
            matches!(target, WriteTarget::BranchView { .. }),
        )?;
        validate_updated_at_ms(updated_at_ms)?;
        let (made_by, permission_subject) = self.resolve_write_identity(identity)?;
        let now_ms = updated_at_ms.unwrap_or_else(|| self.next_now_ms());

        let (branch, parents) = match target {
            WriteTarget::Root => {
                self.ensure_row_not_deleted(table, row).await?;
                let (parents, _) = self.row_layer_parents(table, row).await?;
                (BranchSelector::default(), parents)
            }
            WriteTarget::BranchView { head, base } => {
                let mut node = self.node.node.lock().await;
                let local = node
                    .visible_current_cells_in_branch(table, &head, row)
                    .await?;
                let parents = if local.is_some() {
                    let deletion = node
                        .local_deletion_winner_tx_id_in_branch(table, &head, row)
                        .await?;
                    let content = node
                        .local_content_winner_tx_id_in_branch(table, &head, row)
                        .await?;
                    deletion.or(content).into_iter().collect()
                } else {
                    if node
                        .visible_current_cells_in_branch_view(table, &head, base.as_ref(), row)
                        .await?
                        .is_none()
                    {
                        return Err(Error::new(
                            ErrorCode::NotObserved,
                            format!("row is not visible in branch view: {}", row.0),
                        ));
                    }
                    node.local_deletion_winner_tx_id_in_branch(table, &head, row)
                        .await?
                        .into_iter()
                        .collect()
                };
                (head, parents)
            }
        };

        self.write_mergeable_at_ms_with_authorship_in_branch(
            made_by,
            permission_subject,
            table,
            row,
            BTreeMap::new(),
            parents,
            Some(DeletionEvent::Deleted),
            None,
            now_ms,
            branch,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn delete_attributed(
        &self,
        made_by: AuthorSubject,
        table: &str,
        row: RowUuid,
    ) -> Result<WriteHandle<S>, Error> {
        self.delete(
            table,
            row,
            DeleteOptions {
                identity: WriteIdentity::Attribution(made_by),
                ..Default::default()
            },
        )
        .await
    }

    /// Advise whether a read may be allowed. Client-local replicas return
    /// `Unknown` rather than using locally available rows as policy evidence.
    pub fn can_read(&self, _table: &str, _row: RowUuid) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    /// Evaluate a read for the serving path without disclosing data.
    pub(crate) fn authorize_read_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorSubject,
    ) -> Result<PermissionAdvice, Error> {
        self.table_schema(table)?;
        crate::db::block_on(
            self.node
                .node
                .borrow_mut()
                .dry_run_read_current_allows(table, row, author),
        )
        .map(|allowed| {
            if allowed {
                PermissionAdvice::Allowed
            } else {
                PermissionAdvice::Denied
            }
        })
        .map_err(Into::into)
    }

    /// Advise whether an update may be allowed. Client-local replicas return
    /// `Unknown` rather than using locally available rows as policy evidence.
    pub fn can_update(&self, _table: &str, _row: RowUuid) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    /// Attach process-local auth claims for `identity`.
    pub fn set_identity_claims(&self, identity: AuthorSubject, claims: BTreeMap<String, Value>) {
        let changed = {
            let mut node = self.node.node.borrow_mut();
            let previous_revision = node.session_claim_revision(identity);
            node.set_session_claims(identity, claims);
            node.session_claim_revision(identity) != previous_revision
        };
        if changed {
            self.node.schedule_tick(TickUrgency::Deferred);
        }
    }

    #[cfg(test)]
    pub(crate) fn set_test_provider_claims(
        &self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) {
        let changed = {
            let mut node = self.node.node.borrow_mut();
            let previous_revision = node.session_claim_revision(identity);
            node.set_test_provider_claims(identity, claims);
            node.session_claim_revision(identity) != previous_revision
        };
        if changed {
            self.node.schedule_tick(TickUrgency::Deferred);
        }
    }

    /// Advise whether a delete may be allowed. Client-local replicas return
    /// `Unknown` rather than using locally available rows as policy evidence.
    pub fn can_delete(&self, _table: &str, _row: RowUuid) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    /// Evaluate a delete for a test-only serving-path probe without writing.
    #[cfg(test)]
    pub(crate) async fn authorize_delete_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorSubject,
    ) -> Result<PermissionAdvice, Error> {
        self.table_schema(table)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_delete_current_allows(table, row, author)
            .await
            .map(|allowed| {
                if allowed {
                    PermissionAdvice::Allowed
                } else {
                    PermissionAdvice::Denied
                }
            })
            .map_err(Into::into)
    }

    /// Restore a deleted row through one root-or-branch implementation.
    ///
    /// `cells` replaces the content atomically when present. `None` only
    /// restores the deletion register and therefore preserves existing content.
    pub async fn restore(
        &self,
        table: &str,
        row: RowUuid,
        cells: Option<RowCells>,
        options: RestoreOptions,
    ) -> Result<WriteHandle<S>, Error> {
        let RestoreOptions {
            identity,
            target,
            updated_at_ms,
        } = options;
        self.reject_attributed_branch_target(
            identity,
            matches!(target, ExactWriteTarget::Branch(_)),
        )?;
        validate_updated_at_ms(updated_at_ms)?;
        let (made_by, permission_subject) = self.resolve_write_identity(identity)?;
        let branch = target.branch();
        let now_ms = updated_at_ms.unwrap_or_else(|| self.next_now_ms());
        let cells = cells
            .map(|cells| self.apply_insert_defaults(table, cells))
            .transpose()?;
        let (content_parents, deletion_parents) = match target {
            ExactWriteTarget::Root => {
                self.ensure_row_deleted(table, row, permission_subject.unwrap_or(made_by))
                    .await?;
                self.row_layer_parents(table, row).await?
            }
            ExactWriteTarget::Branch(_) => {
                let mut node = self.node.node.lock().await;
                let deletion = node
                    .local_deletion_winner_tx_id_in_branch(table, &branch, row)
                    .await?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorCode::NotObserved,
                            format!("branch deletion not observed: {}", row.0),
                        )
                    })?;
                let content = node
                    .local_content_winner_tx_id_in_branch(table, &branch, row)
                    .await?
                    .into_iter()
                    .collect();
                (content, vec![deletion])
            }
        };

        let with_subject = |commit: MergeableCommit| match permission_subject {
            Some(subject) => commit.permission_subject(subject),
            None => commit,
        };
        let mut commits = Vec::with_capacity(if cells.is_some() { 2 } else { 1 });
        if let Some(cells) = cells {
            commits.push(with_subject(
                MergeableCommit::new(table, row, now_ms)
                    .branch(branch.clone())
                    .made_by(made_by)
                    .parents(content_parents)
                    .cells(cells),
            ));
        }
        commits.push(with_subject(
            MergeableCommit::new(table, row, now_ms)
                .branch(branch)
                .made_by(made_by)
                .parents(deletion_parents)
                .cells(BTreeMap::<String, Value>::new())
                .deletion(DeletionEvent::Restored),
        ));
        let published = self
            .node
            .node
            .lock()
            .await
            .commit_mergeable_many_in_schema(self.schema_version_id, commits)
            .await?;
        self.finish_published_write(row, published).await
    }

    #[doc(hidden)]
    pub async fn restore_attributed(
        &self,
        made_by: AuthorSubject,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.restore(
            table,
            row,
            Some(cells),
            RestoreOptions {
                identity: WriteIdentity::Attribution(made_by),
                ..Default::default()
            },
        )
        .await
    }

    async fn write_mergeable_at_ms_with_authorship(
        &self,
        made_by: AuthorSubject,
        permission_subject: Option<AuthorSubject>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        authored_columns: Option<BTreeSet<String>>,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.write_mergeable_at_ms_with_authorship_in_branch(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            parents,
            deletion,
            authored_columns,
            now_ms,
            BranchSelector::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_mergeable_at_ms_with_authorship_in_branch(
        &self,
        made_by: AuthorSubject,
        permission_subject: Option<AuthorSubject>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        authored_columns: Option<BTreeSet<String>>,
        now_ms: u64,
        branch: BranchSelector,
    ) -> Result<WriteHandle<S>, Error> {
        let operation = if deletion == Some(DeletionEvent::Deleted) {
            "DELETE"
        } else if parents.is_empty() {
            "INSERT"
        } else {
            "UPDATE"
        };
        let cells = if operation == "INSERT" {
            self.apply_insert_defaults(table, cells)?
        } else {
            cells
        };
        let allow_inherited_descriptors = !authored_columns.as_ref().is_some_and(|authored| {
            self.table_schema(table).is_ok_and(|schema| {
                schema
                    .columns
                    .iter()
                    .all(|column| authored.contains(&column.name))
            })
        });
        let mut commit = MergeableCommit::new(table, row, now_ms)
            .branch(branch)
            .made_by(made_by)
            .parents(parents)
            .cells(cells);
        if let Some(authored_columns) = authored_columns {
            commit = commit.authored_columns(authored_columns);
        }
        if let Some(subject) = permission_subject {
            commit = commit.permission_subject(subject);
        }
        if let Some(deletion) = deletion {
            commit = commit.deletion(deletion);
        }
        // Db is an untrusted client: structurally valid writes are staged and
        // sent optimistically. A serving authority assigns the policy fate.
        let published = {
            let mut node = self.node.node.lock().await;
            let commit = node
                .seal_inherited_large_values(
                    commit,
                    self.schema_version_id,
                    allow_inherited_descriptors,
                )
                .await?;
            node.commit_mergeable_in_schema(self.schema_version_id, commit)
                .await?
        };
        self.finish_published_write(row, published).await
    }

    async fn finish_published_write(
        &self,
        row: RowUuid,
        published: PublishedTransaction,
    ) -> Result<WriteHandle<S>, Error> {
        let tx_id = published.tx_id;
        let local_tier = if self.node.defer_local_persistence.get() {
            // Publication is the synchronous visibility boundary. Refresh
            // resident subscribers before returning, then let the host tick
            // own suspendable persistence and later peer visibility.
            self.refresh_subscriptions().await?;
            self.node.queue_local_publication(published, None);
            DurabilityTier::None
        } else {
            self.finish_publication_outcome(PublicationOutcome::published((), published))
                .await?;
            self.finalize_local_commit(tx_id)?
        };
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    pub(super) fn finish_publication_outcome<'a, T: 'a>(
        &'a self,
        outcome: PublicationOutcome<T>,
    ) -> Pin<Box<dyn Future<Output = Result<T, Error>> + 'a>> {
        Box::pin(async move {
            let PublicationOutcome {
                value,
                mut publications,
                mut post_settlement_work,
            } = outcome;
            loop {
                if !publications.is_empty() {
                    self.refresh_subscriptions().await?;
                    let mut persisted = Vec::with_capacity(publications.len());
                    for publication in &publications {
                        persisted.push((publication.tx_id(), publication.persist().await));
                    }
                    let mut node = self.node.node.lock().await;
                    for (tx_id, persistence) in persisted {
                        node.settle_published_transaction(tx_id, persistence)?;
                    }
                }
                let Some(message) = post_settlement_work.pop_front() else {
                    break;
                };
                let mut outcome = self
                    .node
                    .node
                    .lock()
                    .await
                    .apply_sync_message_with_ingest_context(
                        message,
                        Some(CommitUnitIngestContext {
                            identity: AuthorSubject::SYSTEM,
                            trust: CommitUnitTrust::TrustedBackend,
                            edge_authority: false,
                        }),
                    )
                    .await?;
                publications = outcome.publications;
                post_settlement_work.append(&mut outcome.post_settlement_work);
            }
            Ok(value)
        })
    }

    fn resolve_write_identity(
        &self,
        identity: WriteIdentity,
    ) -> Result<(AuthorSubject, Option<AuthorSubject>), Error> {
        match identity {
            WriteIdentity::Database => Ok((self.identity.author, None)),
            WriteIdentity::Session(author) => Ok((author, Some(author))),
            WriteIdentity::Attribution(author)
                if author == self.identity.author || self.backend_attribution =>
            {
                Ok((author, Some(self.identity.author)))
            }
            WriteIdentity::Attribution(_) => Err(Error::new(
                ErrorCode::WriteRejected,
                "attribution requires a trusted serving node",
            )),
        }
    }

    fn reject_attributed_branch_target(
        &self,
        identity: WriteIdentity,
        targets_branch: bool,
    ) -> Result<(), Error> {
        if matches!(identity, WriteIdentity::Attribution(_)) && targets_branch {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                "backend-attributed writes do not support branch targets",
            ));
        }
        Ok(())
    }

    pub(super) fn check_catalogue_admin(&self) -> Result<(), Error> {
        if self.identity.author == AuthorSubject::SYSTEM {
            return Ok(());
        }
        Err(Error::new(
            ErrorCode::Protocol,
            "catalogue updates require a serving Node",
        ))
    }

    /// Client writes stay pending at this runtime's authored durability until
    /// peer durability or fate updates arrive over a connection.
    pub(super) fn finalize_local_commit(&self, tx_id: TxId) -> Result<DurabilityTier, Error> {
        self.node.queue_pending_upload(tx_id, None);
        Ok(self.node.node.borrow().authored_commit_durability())
    }

    pub(super) fn next_now_ms(&self) -> u64 {
        let next = self.next_now_ms.get();
        self.next_now_ms.set(next + 1);
        next
    }

    pub(super) fn current_write_schema_for_query(
        &self,
    ) -> Result<(JazzSchema, SchemaVersionId), Error> {
        if self.schema_view_is_fixed {
            return Ok((self.schema.clone(), self.schema_version_id));
        }
        let node = self.node.node.borrow();
        let current = node.current_write_schema().map_err(Error::from)?;
        if current.schema == self.schema_version_id {
            return Ok((self.schema.clone(), self.schema_version_id));
        }
        node.catalogue_schemas()
            .get(&current.schema)
            .map(|schema| (schema.schema.clone(), current.schema))
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::Schema,
                    format!(
                        "current write schema {:?} is missing from catalogue",
                        current.schema
                    ),
                )
            })
    }

    pub(super) fn table_schema(&self, table: &str) -> Result<&TableSchema, Error> {
        self.schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| Error::new(ErrorCode::Schema, format!("unknown table {table}")))
    }

    pub(super) fn apply_insert_defaults(
        &self,
        table: &str,
        mut cells: RowCells,
    ) -> Result<RowCells, Error> {
        let table_schema = self.table_schema(table)?;
        for column in &table_schema.columns {
            if !cells.contains_key(&column.name) {
                if let Some(default) = &column.default {
                    cells.insert(
                        column.name.clone(),
                        default_cell_for_column_type(&column.column_type, default),
                    );
                }
            }
        }
        Ok(cells)
    }

    async fn upsert_target_for_client_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorSubject,
    ) -> Result<Option<CurrentRow>, Error> {
        let target = self
            .local_row_for_client_identity(table, row, identity)
            .await?;
        if target.is_some() {
            return Ok(target);
        }
        // A policy-filtered point read cannot by itself distinguish an absent
        // row from an existing row hidden from this identity. Upsert needs
        // exactly that distinction: a genuinely absent target follows INSERT
        // policy and does not require read permission, while merging into an
        // existing target must not expose or copy hidden cells.
        if self.local_current_row(table, row).await?.is_none() {
            return Ok(None);
        }
        if identity == AuthorSubject::SYSTEM || self.table_schema(table)?.read_policy.is_none() {
            return Ok(None);
        }
        Err(read_for_write_denied("UPSERT", table))
    }

    async fn upsert_target_for_trusted_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorSubject,
    ) -> Result<Option<CurrentRow>, Error> {
        let target = self
            .local_row_for_trusted_identity(table, row, identity)
            .await?;
        if target.is_some() {
            return Ok(target);
        }
        // Trusted serving evaluates the identity's real read policy before
        // merging an existing row. A hidden existing row must not be treated
        // as an insert target.
        if self.local_current_row(table, row).await?.is_none() {
            return Ok(None);
        }
        if identity == AuthorSubject::SYSTEM || self.table_schema(table)?.read_policy.is_none() {
            return Ok(None);
        }
        Err(read_for_write_denied("UPSERT", table))
    }

    /// Read one locally-current row by primary key without evaluating a table
    /// query. This backend-scoped helper is used by import/upsert bridges that
    /// already operate with database authority and need an O(row) existence
    /// check before staging a write.
    pub async fn local_current_row(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        self.table_schema(table)?;
        Ok(self
            .node
            .node
            .lock()
            .await
            .local_current_row(table, row)
            .await?)
    }

    async fn ensure_row_absent(
        &self,
        table: &str,
        row: RowUuid,
        _identity: AuthorSubject,
    ) -> Result<(), Error> {
        self.table_schema(table)?;
        let (content_parent, deletion_parent) = {
            let mut node = self.node.node.lock().await;
            (
                node.local_content_winner_tx_id_in_schema(self.schema_version_id, table, row)
                    .await?,
                node.local_deletion_winner_tx_id_in_schema(self.schema_version_id, table, row)
                    .await?,
            )
        };
        if deletion_parent.is_some() {
            return Err(row_already_deleted(row));
        }
        if content_parent.is_some() {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("encoding error: object already exists: {}", row.0),
            ));
        }
        Ok(())
    }

    async fn ensure_exact_branch_row_absent(
        &self,
        table: &str,
        branch: &BranchSelector,
        row: RowUuid,
    ) -> Result<(), Error> {
        self.table_schema(table)?;
        let mut node = self.node.node.lock().await;
        let content = node
            .local_content_winner_tx_id_in_branch(table, branch, row)
            .await?;
        let deletion = node
            .local_deletion_winner_tx_id_in_branch(table, branch, row)
            .await?;
        if deletion.is_some() {
            return Err(row_already_deleted(row));
        }
        if content.is_some() {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("encoding error: branch-local row already exists: {}", row.0),
            ));
        }
        Ok(())
    }

    async fn ensure_row_deleted(
        &self,
        table: &str,
        row: RowUuid,
        _identity: AuthorSubject,
    ) -> Result<(), Error> {
        self.table_schema(table)?;
        let deleted = self
            .node
            .node
            .lock()
            .await
            .local_deletion_winner_tx_id_in_schema(self.schema_version_id, table, row)
            .await?
            .is_some();
        if deleted {
            Ok(())
        } else {
            Err(Error::new(
                ErrorCode::WriteRejected,
                format!("row not deleted: {}", row.0),
            ))
        }
    }

    async fn ensure_row_not_deleted(&self, table: &str, row: RowUuid) -> Result<(), Error> {
        self.table_schema(table)?;
        let deleted = self
            .node
            .node
            .lock()
            .await
            .local_deletion_winner_tx_id_in_schema(self.schema_version_id, table, row)
            .await?
            .is_some();
        if deleted {
            Err(row_already_deleted(row))
        } else {
            Ok(())
        }
    }

    async fn row_layer_parents(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<(Vec<TxId>, Vec<TxId>), Error> {
        let mut node = self.node.node.lock().await;
        let content_parents = node
            .local_content_winner_tx_id_in_schema(self.schema_version_id, table, row)
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        let deletion_parents = node
            .local_deletion_winner_tx_id_in_schema(self.schema_version_id, table, row)
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        Ok((content_parents, deletion_parents))
    }

    async fn local_row_for_client_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorSubject,
    ) -> Result<Option<CurrentRow>, Error> {
        // A policy-free table cannot hide the preimage from this client. Use
        // the point lookup directly instead of building and hydrating an
        // unbounded policy query only to select one row afterward.
        if self.table_schema(table)?.read_policy.is_none() {
            return self.local_current_row(table, row).await;
        }
        let query = self.prepare_query(&Query::from(table))?;
        Ok(self
            .node
            .node
            .lock()
            .await
            .query_rows_for_client_physical_row(
                &query.shape,
                &query.binding,
                DurabilityTier::Local,
                identity,
                row,
            )
            .await?
            .into_iter()
            .next())
    }

    pub(super) async fn local_row_for_trusted_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorSubject,
    ) -> Result<Option<CurrentRow>, Error> {
        let query = self.prepare_query(&Query::from(table))?;
        Ok(self
            .node
            .node
            .lock()
            .await
            .query_rows_for_link_physical_row(
                &query.shape,
                &query.binding,
                DurabilityTier::Local,
                identity,
                row,
            )
            .await?
            .into_iter()
            .next())
    }

    pub(super) async fn visible_branch_view_cells_for_identity(
        &self,
        table: &str,
        head: &BranchSelector,
        base: Option<&BranchViewBase>,
        row: RowUuid,
        identity: AuthorSubject,
    ) -> Result<Option<RowCells>, Error> {
        let table_schema = self.table_schema(table)?.clone();
        let query = self.prepare_query(&Query::from(table))?;
        let opts = ReadOpts {
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        }
        .branch_view(head.clone(), base.cloned());
        Ok(self
            .all_for_identity(&query, opts, identity)
            .await?
            .into_iter()
            .find(|candidate| candidate.row_uuid() == row)
            .map(|candidate| {
                table_schema
                    .columns
                    .iter()
                    .filter_map(|column| {
                        candidate
                            .cell(&table_schema, &column.name)
                            .map(|value| (column.name.clone(), value))
                    })
                    .collect()
            }))
    }

    async fn no_op_update_handle_for_client(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorSubject,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row).await?;
        let existing = self
            .local_row_for_client_identity(table, row, identity)
            .await?
            .ok_or_else(|| read_for_write_denied("UPDATE", table))?;
        let tx_id = self
            .node
            .node
            .lock()
            .await
            .current_row_tx_id(&existing)
            .await
            .ok_or_else(|| Error::new(ErrorCode::NotObserved, "current row has no transaction"))?;
        let local_tier = self.write_state(tx_id)?.durability;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    async fn no_op_update_handle_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorSubject,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row).await?;
        let existing = self
            .local_row_for_trusted_identity(table, row, identity)
            .await?
            .ok_or_else(|| read_for_write_denied("UPDATE", table))?;
        let tx_id = self
            .node
            .node
            .lock()
            .await
            .current_row_tx_id(&existing)
            .await
            .ok_or_else(|| Error::new(ErrorCode::NotObserved, "current row has no transaction"))?;
        let local_tier = self.write_state(tx_id)?.durability;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    async fn merge_existing_cells(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<(RowCells, Option<TxId>, BTreeSet<String>), Error> {
        self.merge_existing_cells_for_client_identity(table, row, patch, self.identity.author)
            .await
    }

    async fn merge_existing_cells_for_client_identity(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        identity: AuthorSubject,
    ) -> Result<(RowCells, Option<TxId>, BTreeSet<String>), Error> {
        let table_schema = self.table_schema(table)?;
        self.ensure_row_not_deleted(table, row).await?;
        let is_partial = table_schema
            .columns
            .iter()
            .any(|column| !patch.contains_key(&column.name));
        if is_partial && (identity == AuthorSubject::SYSTEM || table_schema.read_policy.is_none()) {
            // The serving query below proves that a partial writer may observe
            // the cells it inherits. SYSTEM and policy-free tables are
            // unconditionally visible, so that query cannot change the answer.
            // Preserve indirect descriptors by reading the physical winner.
            let (mut cells, parent) = {
                let mut node = self.node.node.lock().await;
                let (cells, parent) = node
                    .current_physical_cells_and_winner_in_schema(self.schema_version_id, table, row)
                    .await?
                    .ok_or_else(|| read_for_write_denied("partial UPDATE", table))?;
                (cells, Some(parent))
            };
            let authored_columns = patch.keys().cloned().collect();
            cells.extend(patch);
            return Ok((cells, parent, authored_columns));
        }
        let existing = self
            .local_row_for_client_identity(table, row, identity)
            .await?
            .ok_or_else(|| read_for_write_denied("UPDATE", table))?;
        let (mut cells, parent) = {
            let mut node = self.node.node.lock().await;
            let cells = node
                .current_physical_cells_in_schema(self.schema_version_id, table, row)
                .await?
                .ok_or_else(|| read_for_write_denied("UPDATE", table))?;
            let parent = node.current_row_tx_id(&existing).await;
            (cells, parent)
        };
        let authored_columns = patch.keys().cloned().collect();
        cells.extend(patch);
        Ok((cells, parent, authored_columns))
    }

    async fn merge_existing_cells_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        identity: AuthorSubject,
    ) -> Result<(RowCells, Option<TxId>, BTreeSet<String>), Error> {
        self.ensure_row_not_deleted(table, row).await?;
        if self.authorize_read_for_identity(table, row, identity)? != PermissionAdvice::Allowed {
            return Err(read_for_write_denied("UPDATE", table));
        }
        let authored_columns = patch.keys().cloned().collect();
        // A complete replacement still requires read permission, proved by
        // the point dry-run above, but it does not consume any preimage cells.
        // Avoid materializing a second policy query here: besides doing
        // unnecessary work, that read can install transient coverage whose
        // lifetime races a subsequent authorship-based visibility handoff.
        if self
            .table_schema(table)?
            .columns
            .iter()
            .all(|column| patch.contains_key(&column.name))
        {
            let parent = match self.local_current_row(table, row).await? {
                Some(existing) => {
                    self.node
                        .node
                        .lock()
                        .await
                        .current_row_tx_id(&existing)
                        .await
                }
                None => None,
            };
            return Ok((patch, parent, authored_columns));
        }
        let (mut cells, parent) = {
            let mut node = self.node.node.lock().await;
            let (cells, parent) = node
                .current_physical_cells_and_winner_in_schema(self.schema_version_id, table, row)
                .await?
                .ok_or_else(|| read_for_write_denied("partial UPDATE", table))?;
            (cells, Some(parent))
        };
        cells.extend(patch);
        Ok((cells, parent, authored_columns))
    }
}
