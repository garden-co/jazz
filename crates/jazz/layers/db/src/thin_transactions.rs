//! Thin-client transactions served outside the core (SPEC 8.14).
//!
//! A thin client's transaction runs in a client [`Db`] that its session's
//! backend holds for it, linked to the core as that session. The backend
//! routes the session's transaction requests here: `Begin`, `Commit`,
//! `Rollback`, and every read, write and large-value upload that names an
//! open transaction. Its other requests go to the core as before.
//!
//! The transaction stages and commits in this client like any other client
//! transaction, so the core never holds one open between calls: it only
//! accepts the finished upload, as the session, checking its writes (and a
//! serializable transaction's reads) like any other.
//!
//! What a thin client sees stays what it saw when the core ran its
//! transactions:
//! - A mergeable read inside a transaction reads the core's current rows
//!   with the transaction's own staged writes: it fetches from the core
//!   first, since a client's mergeable transaction reads only what its
//!   replica holds. A serializable read hydrates from the core at the
//!   transaction's snapshot, as on any client.
//! - `Commit` answers once the core accepted the upload, or with the
//!   refusal that rejected it; never at the local commit.
//! - Open transactions end with the session ([`ThinTransactions::release`]).

use std::collections::{BTreeMap, BTreeSet};

use jazz_model::model::transaction::OpenTransactionId;
use jazz_model::query::{Query, col, eq, lit};
use jazz_model::tx::DurabilityTier;
use jazz_protocol::protocol::{BranchSelector, BranchViewBase};
use jazz_protocol::thin::{
    ThinAssembler, ThinBody, ThinOp, ThinRequest, ThinResponse, thin_wire_frames,
};
use jazz_protocol::wire::{WireFrame, decode_frame, decode_postcard_frame, encode_frame};
use jazz_types::ids::{AuthorSubject, RowUuid};
use jazz_types::records::Value;

use crate::binding_codec;
use crate::binding_host::{
    HostError, TransactionKind, decode_cells, delete_options, insert_options, open_transaction,
    read_opts, restore_options, update_options, upsert_options,
};
use crate::db::{
    Db, Error, ErrorCode, LargeValueUpdate, ReadOpts, ReadTier, Reason, SerializedReadResult,
    StreamingMutationKind, StreamingValueUpload, UpdateOptions, WriteHandle, WriteIdentity,
};

/// A transaction the session began here and has not ended.
struct Open {
    serializable: bool,
    /// The first staged write refused for its row's state: the commit
    /// reports it instead of committing.
    refused: Option<Error>,
}

/// A large value the session is uploading in chunks into an open
/// transaction.
struct Stream {
    upload: StreamingValueUpload,
    mutation: StreamingMutationKind,
    table: String,
    row: RowUuid,
    cells: BTreeMap<String, Value>,
    column: String,
    updated_at_ms: Option<u64>,
    head: Option<BranchSelector>,
    base: Option<BranchViewBase>,
    transaction: OpenTransactionId,
}

/// One thin session's transactions, served from a client [`Db`] linked to
/// the core as that session.
pub struct ThinTransactions {
    /// The session: every staged write runs as it.
    identity: AuthorSubject,
    open: BTreeMap<OpenTransactionId, Open>,
    /// Uploads begun inside a transaction, by the id of the request that
    /// began them.
    streams: BTreeMap<u64, Stream>,
}

impl ThinTransactions {
    pub fn new(identity: AuthorSubject) -> Self {
        Self {
            identity,
            open: BTreeMap::new(),
            streams: BTreeMap::new(),
        }
    }

    /// Whether request `op` is served here rather than by the core: it
    /// begins, commits or rolls back a transaction, names one, or continues
    /// an upload begun in one.
    pub fn serves(&self, op: &ThinOp) -> bool {
        match op {
            ThinOp::StreamChunk { stream, .. }
            | ThinOp::FinishStream { stream }
            | ThinOp::AbortStream { stream } => self.streams.contains_key(stream),
            op => names_transaction(op),
        }
    }

    /// Whether any transaction or upload is open.
    pub fn is_idle(&self) -> bool {
        self.open.is_empty() && self.streams.is_empty()
    }

    /// Serve request `id`, one [`Self::serves`] accepts, against `db`. The
    /// session's requests are served one at a time, in order: a commit's
    /// answer waits for the core's verdict.
    pub async fn serve(&mut self, db: &Db, id: u64, op: &ThinOp) -> ThinBody {
        self.serve_op(db, id, op).await.unwrap_or_else(|body| body)
    }

    /// End what the session left open: its transactions roll back (nothing
    /// of them reached the core), its uploads are dropped.
    pub async fn release(self, db: &Db) {
        for id in self.open.into_keys() {
            let _ = db.abandon_transaction_handle(id);
        }
        for stream in self.streams.into_values() {
            let _ = db.abort_streaming_value_upload(stream.upload).await;
        }
    }

    fn transaction(
        &mut self,
        transaction: &str,
    ) -> Result<(OpenTransactionId, &mut Open), ThinBody> {
        let id = open_transaction(transaction).map_err(host_error)?;
        match self.open.get_mut(&id) {
            Some(open) => Ok((id, open)),
            None => Err(argument(format!(
                "transaction {transaction} is not open in this session"
            ))),
        }
    }

    /// Load row `row` of `table` into the client before transaction `open`
    /// writes to it, as the core held every row its own transactions wrote
    /// to: a client stages a write to an existing row only over the row it
    /// holds. A serializable transaction reads it from the core at its
    /// snapshot (the core validates the rows it writes in any case); a
    /// mergeable one fetches it unless the client already holds it.
    async fn load_row(
        &self,
        db: &Db,
        open: OpenTransactionId,
        table: &str,
        row: RowUuid,
    ) -> Result<(), ThinBody> {
        let serializable = self.open.get(&open).is_some_and(|open| open.serializable);
        let query = Query::from(table).filter(eq(col("id"), lit(Value::Uuid(row.0))));
        let query = postcard::to_allocvec(&query)
            .map_err(|error| argument(format!("encode row read: {error}")))?;
        let mut opts = ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        };
        opts.read_view.include_deleted = true;
        if !serializable {
            let held = db
                .all_serialized_query(&query, opts.clone(), None, None, None)
                .await
                .map_err(|error| error_body(&error))?;
            if !result_is_empty(&held) {
                return Ok(());
            }
        }
        let remote = ReadOpts {
            tier: ReadTier::Remote,
            ..opts
        };
        let in_transaction = serializable.then_some(open);
        db.all_serialized_query(&query, remote, in_transaction, None, None)
            .await
            .map_err(|error| error_body(&error))?;
        Ok(())
    }

    async fn serve_op(&mut self, db: &Db, id: u64, op: &ThinOp) -> Result<ThinBody, ThinBody> {
        let identity = self.identity;
        Ok(match op {
            ThinOp::Begin { transaction, kind } => {
                let open = open_transaction(transaction).map_err(host_error)?;
                if self.open.contains_key(&open) {
                    return Err(argument(format!(
                        "transaction {transaction} is already open in this session"
                    )));
                }
                let serializable = matches!(
                    TransactionKind::parse(kind).map_err(host_error)?,
                    TransactionKind::Serializable
                );
                let begun = if serializable {
                    db.begin_serializable(open).await
                } else {
                    db.begin_mergeable(open).await
                };
                match begun {
                    Ok(()) => {
                        self.open.insert(
                            open,
                            Open {
                                serializable,
                                refused: None,
                            },
                        );
                        ThinBody::Ok
                    }
                    Err(error) => error_body(&error),
                }
            }
            ThinOp::Commit { transaction } => {
                let open = self.transaction(transaction)?.0;
                let state = self.open.remove(&open).expect("checked above");
                // Uploads begun in it can no longer finish into it.
                self.streams.retain(|_, stream| stream.transaction != open);
                if let Some(error) = state.refused {
                    let _ = db.abandon_transaction_handle(open);
                    return Err(error_body(&error));
                }
                let receipt = if state.serializable {
                    db.commit_serializable_handle(open).await
                } else {
                    db.commit_mergeable_handle(open).await
                };
                let receipt = receipt.map_err(|error| error_body(&error))?;
                // The core's verdict, not the local commit, ends it.
                match db
                    .wait_for_transaction(receipt.0, DurabilityTier::Global)
                    .await
                {
                    Ok(_) => ThinBody::Written {
                        row: [0; 16],
                        seq: receipt.0,
                    },
                    // The core refuses an upload whose serializable reads no
                    // longer hold as a rejected write; a thin client was
                    // always told that as the conflict a commit it ran on
                    // the core reported.
                    Err(error) if error.reason == Some(Reason::SerializationConflict) => {
                        ThinBody::Error {
                            code: ErrorCode::TransactionConflict.as_str().to_owned(),
                            message: error.message.clone(),
                        }
                    }
                    Err(error) => error_body(&error),
                }
            }
            ThinOp::Rollback { transaction } => {
                let open = self.transaction(transaction)?.0;
                self.open.remove(&open);
                self.streams.retain(|_, stream| stream.transaction != open);
                match db.abandon_transaction_handle(open) {
                    Ok(()) => ThinBody::Ok,
                    Err(error) => error_body(&error),
                }
            }
            ThinOp::Query {
                query,
                options,
                transaction: Some(transaction),
            } => {
                let opts = read_opts(&options_json(options)?).map_err(host_error)?;
                let (open, state) = self.transaction(transaction)?;
                let serializable = state.serializable;
                // Read what the core holds now, as the core's own read in a
                // transaction did: a mergeable transaction reads its
                // replica, so the rows are fetched into it first; a
                // serializable one hydrates from the core at its snapshot.
                let remote = ReadOpts {
                    tier: ReadTier::Remote,
                    ..opts.clone()
                };
                if !serializable {
                    db.all_serialized_query(query, remote.clone(), None, None, None)
                        .await
                        .map_err(|error| error_body(&error))?;
                }
                let read = db
                    .all_serialized_query(query, remote, Some(open), None, None)
                    .await
                    .map_err(|error| error_body(&error))?;
                let encoded = match read {
                    SerializedReadResult::Rows(rows) => {
                        binding_codec::encode_rows(&rows).map(|rows| (rows, false))
                    }
                    SerializedReadResult::Relation(snapshot) => {
                        binding_codec::encode_relation_snapshot(&snapshot).map(|rows| (rows, true))
                    }
                };
                match encoded {
                    Ok((rows, relation)) => ThinBody::Rows { rows, relation },
                    Err(error) => argument(format!("encode result: {error}")),
                }
            }
            ThinOp::Insert {
                table,
                cells,
                options,
                transaction: Some(transaction),
            } => {
                let cells = decode_cells(cells).map_err(host_error)?;
                let json = staged_options(options)?;
                let mut options = insert_options(&json).map_err(host_error)?;
                as_session(&mut options.identity, identity)?;
                let row = *options.row_id.get_or_insert_with(|| db.next_row_id());
                let (open, state) = self.transaction(transaction)?;
                staged(
                    state,
                    row,
                    db.tx_ref(open).insert(table, cells, options).await,
                )
            }
            ThinOp::Update {
                table,
                row,
                patch,
                options,
                transaction: Some(transaction),
            } => {
                let row = RowUuid::from_bytes(*row);
                let patch = decode_cells(patch).map_err(host_error)?;
                let json = staged_options(options)?;
                let mut options = update_options(&json).map_err(host_error)?;
                as_session(&mut options.identity, identity)?;
                let open = self.transaction(transaction)?.0;
                self.load_row(db, open, table, row).await?;
                let state = self.open.get_mut(&open).expect("checked above");
                staged(
                    state,
                    row,
                    db.tx_ref(open).update(table, row, patch, options).await,
                )
            }
            ThinOp::Upsert {
                table,
                row,
                cells,
                options,
                transaction: Some(transaction),
            } => {
                let row = RowUuid::from_bytes(*row);
                let json = staged_options(options)?;
                let mut options = upsert_options(&json).map_err(host_error)?;
                let cells = decode_cells(cells).map_err(host_error)?;
                as_session(&mut options.identity, identity)?;
                let open = self.transaction(transaction)?.0;
                self.load_row(db, open, table, row).await?;
                let state = self.open.get_mut(&open).expect("checked above");
                staged(
                    state,
                    row,
                    db.tx_ref(open).upsert(table, row, cells, options).await,
                )
            }
            ThinOp::Delete {
                table,
                row,
                options,
                transaction: Some(transaction),
            } => {
                let row = RowUuid::from_bytes(*row);
                let json = staged_options(options)?;
                let mut options = delete_options(&json).map_err(host_error)?;
                as_session(&mut options.identity, identity)?;
                let open = self.transaction(transaction)?.0;
                self.load_row(db, open, table, row).await?;
                let state = self.open.get_mut(&open).expect("checked above");
                staged(
                    state,
                    row,
                    db.tx_ref(open).delete(table, row, options).await,
                )
            }
            ThinOp::Restore {
                table,
                row,
                cells,
                options,
                transaction: Some(transaction),
            } => {
                let row = RowUuid::from_bytes(*row);
                let cells = cells
                    .as_ref()
                    .map(|cells| decode_cells(cells))
                    .transpose()
                    .map_err(host_error)?;
                let json = staged_options(options)?;
                let mut options = restore_options(&json).map_err(host_error)?;
                as_session(&mut options.identity, identity)?;
                let open = self.transaction(transaction)?.0;
                self.load_row(db, open, table, row).await?;
                let state = self.open.get_mut(&open).expect("checked above");
                staged(
                    state,
                    row,
                    db.tx_ref(open).restore(table, row, cells, options).await,
                )
            }
            ThinOp::UpdateLargeValues {
                table,
                row,
                patch,
                mutations,
                updated_at_ms,
                transaction: Some(transaction),
            } => {
                let row = RowUuid::from_bytes(*row);
                let patch = decode_cells(patch).map_err(host_error)?;
                let mutations: Vec<LargeValueUpdate> =
                    serde_json::from_slice(mutations).map_err(|error| {
                        argument(format!("invalid partial-value update descriptor: {error}"))
                    })?;
                let options = UpdateOptions {
                    identity: WriteIdentity::Session(identity),
                    updated_at_ms: *updated_at_ms,
                    ..UpdateOptions::default()
                };
                let open = self.transaction(transaction)?.0;
                self.load_row(db, open, table, row).await?;
                let state = self.open.get_mut(&open).expect("checked above");
                staged(
                    state,
                    row,
                    db.tx_ref(open)
                        .update_with_large_value_mutations(table, row, patch, mutations, options)
                        .await,
                )
            }
            ThinOp::BeginStream {
                table,
                row,
                cells,
                column,
                mutation,
                options,
                transaction: Some(transaction),
            } => {
                if self.streams.contains_key(&id) {
                    return Err(argument("an upload with this id is already open"));
                }
                let transaction = self.transaction(transaction)?.0;
                let mutation = streaming_mutation(mutation)?;
                let (updated_at_ms, head, base) = stream_options(&options_json(options)?)?;
                let cells = decode_cells(cells).map_err(host_error)?;
                match db.begin_streaming_value_upload(table, &cells, column) {
                    Ok(upload) => {
                        self.streams.insert(
                            id,
                            Stream {
                                upload,
                                mutation,
                                table: table.clone(),
                                row: RowUuid::from_bytes(*row),
                                cells,
                                column: column.clone(),
                                updated_at_ms,
                                head,
                                base,
                                transaction,
                            },
                        );
                        ThinBody::Ok
                    }
                    Err(error) => error_body(&error),
                }
            }
            ThinOp::StreamChunk { stream, chunk } => {
                let Some(open) = self.streams.get_mut(stream) else {
                    return Err(argument("no upload with this id is open"));
                };
                match db
                    .push_streaming_value_upload(&mut open.upload, chunk)
                    .await
                {
                    Ok(()) => ThinBody::Ok,
                    Err(error) => {
                        // A failed push ends the upload.
                        let open = self.streams.remove(stream).expect("checked above");
                        let _ = db.abort_streaming_value_upload(open.upload).await;
                        error_body(&error)
                    }
                }
            }
            ThinOp::FinishStream { stream } => {
                let Some(open) = self.streams.remove(stream) else {
                    return Err(argument("no upload with this id is open"));
                };
                if !self.open.contains_key(&open.transaction) {
                    let _ = db.abort_streaming_value_upload(open.upload).await;
                    return Err(argument("the upload's transaction is no longer open"));
                }
                if !matches!(open.mutation, StreamingMutationKind::Insert) {
                    self.load_row(db, open.transaction, &open.table, open.row)
                        .await?;
                }
                let state = self.open.get_mut(&open.transaction).expect("checked above");
                let write = db
                    .tx_ref(open.transaction)
                    .finish_streaming_value_upload(
                        open.upload,
                        open.mutation,
                        &open.table,
                        open.row,
                        open.cells,
                        &open.column,
                        WriteIdentity::Session(identity),
                        open.updated_at_ms,
                        open.head,
                        open.base,
                    )
                    .await;
                staged(state, open.row, write)
            }
            ThinOp::AbortStream { stream } => {
                let Some(open) = self.streams.remove(stream) else {
                    return Err(argument("no upload with this id is open"));
                };
                match db.abort_streaming_value_upload(open.upload).await {
                    Ok(()) => ThinBody::Ok,
                    Err(error) => error_body(&error),
                }
            }
            _ => return Err(argument("not a transaction request")),
        })
    }
}

/// A core error's answer, by its stable snake_case code, as the core's
/// own thin answers carry it.
fn result_is_empty(result: &SerializedReadResult) -> bool {
    match result {
        SerializedReadResult::Rows(rows) => rows.is_empty(),
        SerializedReadResult::Relation(snapshot) => snapshot.rows.is_empty(),
    }
}

fn error_body(error: &Error) -> ThinBody {
    ThinBody::Error {
        code: error.code.as_str().to_owned(),
        message: error.message.clone(),
    }
}

fn argument(message: impl Into<String>) -> ThinBody {
    ThinBody::Error {
        code: "Argument".to_owned(),
        message: message.into(),
    }
}

fn host_error(error: HostError) -> ThinBody {
    match error {
        HostError::Engine(error) => error_body(&error),
        HostError::Argument(message) => argument(message),
    }
}

/// Options as the bindings take them; empty bytes are no options.
fn options_json(options: &[u8]) -> Result<serde_json::Value, ThinBody> {
    if options.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_slice(options).map_err(|error| argument(format!("invalid options: {error}")))
}

/// A staged write's options: a `writeKey` names a standalone write.
fn staged_options(options: &[u8]) -> Result<serde_json::Value, ThinBody> {
    let json = options_json(options)?;
    if json.get("writeKey").is_some_and(|key| !key.is_null()) {
        return Err(argument(
            "a writeKey names a standalone write, not a staged one",
        ));
    }
    Ok(json)
}

/// Every thin write runs as the session: options may not name an author
/// or attribution of their own.
fn as_session(identity: &mut WriteIdentity, session: AuthorSubject) -> Result<(), ThinBody> {
    if *identity != WriteIdentity::Database {
        return Err(argument(
            "thin writes run as the session; options name no author or attribution",
        ));
    }
    *identity = WriteIdentity::Session(session);
    Ok(())
}

/// A staged write's answer. A refusal decided by the row's stored state, or
/// a serializable conflict, does not fail the call: the transaction stays
/// open and its commit reports the first one (as for the bindings).
fn staged(state: &mut Open, row: RowUuid, write: Result<WriteHandle, Error>) -> ThinBody {
    match write {
        Ok(_) => ThinBody::Staged {
            row: *row.as_bytes(),
        },
        Err(error)
            if error.is_apply_time_rejection() || error.code == ErrorCode::TransactionConflict =>
        {
            state.refused.get_or_insert(error);
            ThinBody::Staged {
                row: *row.as_bytes(),
            }
        }
        Err(error) => error_body(&error),
    }
}

fn streaming_mutation(mutation: &str) -> Result<StreamingMutationKind, ThinBody> {
    match mutation {
        "insert" => Ok(StreamingMutationKind::Insert),
        "update" => Ok(StreamingMutationKind::Update),
        "upsert" => Ok(StreamingMutationKind::Upsert),
        _ => Err(argument("unknown streaming mutation kind")),
    }
}

/// `updatedAtMs`, `head` and `base` of a streaming mutation.
type StreamOptions = (Option<u64>, Option<BranchSelector>, Option<BranchViewBase>);

/// `{ updatedAtMs?, head?, base? }` of a streaming mutation.
fn stream_options(options: &serde_json::Value) -> Result<StreamOptions, ThinBody> {
    let field = |name: &str| options.get(name).filter(|value| !value.is_null()).cloned();
    if field("author").is_some() || field("attribution").is_some() {
        return Err(argument(
            "thin writes run as the session; options name no author or attribution",
        ));
    }
    let updated_at_ms = match field("updatedAtMs") {
        Some(value) => Some(
            value
                .as_u64()
                .ok_or_else(|| argument("updatedAtMs must be a nonnegative integer"))?,
        ),
        None => None,
    };
    let head = field("head")
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| argument(format!("invalid head: {error}")))?;
    let base = field("base")
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| argument(format!("invalid base: {error}")))?;
    if base.is_some() && head.is_none() {
        return Err(argument("streaming mutation base requires a branch head"));
    }
    Ok((updated_at_ms, head, base))
}

/// Whether request `op` belongs to a transaction, ignoring uploads (see
/// [`ThinRouter`]): it begins, commits or rolls one back, or names one.
pub fn names_transaction(op: &ThinOp) -> bool {
    match op {
        ThinOp::Begin { .. } | ThinOp::Commit { .. } | ThinOp::Rollback { .. } => true,
        ThinOp::Query { transaction, .. }
        | ThinOp::Insert { transaction, .. }
        | ThinOp::Update { transaction, .. }
        | ThinOp::Upsert { transaction, .. }
        | ThinOp::Delete { transaction, .. }
        | ThinOp::Restore { transaction, .. }
        | ThinOp::UpdateLargeValues { transaction, .. }
        | ThinOp::BeginStream { transaction, .. } => transaction.is_some(),
        ThinOp::StreamChunk { .. }
        | ThinOp::FinishStream { .. }
        | ThinOp::AbortStream { .. }
        | ThinOp::Schema { .. }
        | ThinOp::Subscribe { .. }
        | ThinOp::Unsubscribe { .. }
        | ThinOp::CaptureSnapshot => false,
    }
}

/// One thin connection's client-to-core frames, split between the core
/// and the session's backend: the backend serves the session's transaction
/// requests ([`ThinTransactions`]) and passes everything else to the core
/// unchanged. Carrier-agnostic: the caller moves the frames.
///
/// A split request (its [`ThinPart`]s) is joined before it is routed, and
/// goes on as the same parts when it is the core's.
#[derive(Debug, Default)]
pub struct ThinRouter {
    assembler: ThinAssembler,
    /// The parts of the request being joined, as they came.
    parts: Vec<Vec<u8>>,
    /// Uploads begun in a transaction, by the id of the request that began
    /// them: their chunks, finish and abort are the backend's too.
    streams: BTreeSet<u64>,
}

/// Where a batch of client frames goes.
#[derive(Debug, Default, PartialEq)]
pub struct Routed {
    /// Frames for the core, in order, unchanged.
    pub to_core: Vec<Vec<u8>>,
    /// Requests the backend serves, in order.
    pub to_backend: Vec<ThinRequest>,
}

impl ThinRouter {
    /// Route the frames of one client message. `Err` for a frame that does
    /// not decode or a part that does not join: the connection is broken.
    pub fn route(&mut self, frames: Vec<Vec<u8>>) -> Result<Routed, String> {
        let mut routed = Routed::default();
        for frame in frames {
            match decode_frame(&frame).map_err(|error| format!("undecodable frame: {error}"))? {
                WireFrame::ThinPart(part) => {
                    self.parts.push(frame);
                    let Some(joined) = self.assembler.push(part)? else {
                        continue;
                    };
                    let parts = std::mem::take(&mut self.parts);
                    match decode_postcard_frame(&joined)
                        .map_err(|error| format!("undecodable joined frame: {error}"))?
                    {
                        WireFrame::ThinRequest(request) if self.is_backends(&request) => {
                            routed.to_backend.push(request);
                        }
                        _ => routed.to_core.extend(parts),
                    }
                }
                WireFrame::ThinRequest(request) if self.is_backends(&request) => {
                    routed.to_backend.push(request);
                }
                _ => routed.to_core.push(frame),
            }
        }
        Ok(routed)
    }

    /// Whether the backend serves `request`, noting the uploads it begins
    /// (whatever the backend answers: a refused one's chunks are refused
    /// there too) or ends.
    fn is_backends(&mut self, request: &ThinRequest) -> bool {
        match &request.op {
            ThinOp::StreamChunk { stream, .. } => self.streams.contains(stream),
            ThinOp::FinishStream { stream } | ThinOp::AbortStream { stream } => {
                self.streams.remove(stream)
            }
            ThinOp::BeginStream {
                transaction: Some(_),
                ..
            } => {
                self.streams.insert(request.id);
                true
            }
            op => names_transaction(op),
        }
    }
}

/// The client frames that carry the backend's `answer`: a whole frame, or
/// its parts when it is larger than one. An answer past
/// [`jazz_protocol::thin::MAX_THIN_MESSAGE_BYTES`] is answered with that
/// error instead.
pub fn answer_frames(answer: ThinResponse) -> Vec<Vec<u8>> {
    let id = answer.id;
    let encode = |response: &ThinResponse| {
        encode_frame(&WireFrame::ThinResponse(response.clone())).expect("a thin answer encodes")
    };
    match thin_wire_frames(encode(&answer)) {
        Ok(frames) => frames,
        Err(size) => vec![encode(&ThinResponse {
            id,
            body: argument(format!(
                "the answer of {size} bytes is over the most one may take"
            )),
        })],
    }
}
