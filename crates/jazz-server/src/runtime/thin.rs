//! Thin sessions (SPEC 8.14): connections that hold no replica. The owner
//! evaluates their queries under the session's identity and claims, sends
//! finished results, and runs their writes as that session.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jazz::binding_codec;
use jazz::binding_host::{
    EventChunk, TransactionKind, decode_cells, decode_schema, delete_options, event_chunk,
    insert_options, open_transaction, read_opts, restore_options, update_options, upsert_options,
};
use jazz::db::{
    Db, Error, ErrorCode, LargeValueUpdate, SerializedReadResult, StreamingMutationKind,
    StreamingValueUpload, SubscriptionEvent, SubscriptionStream, UpdateOptions, WriteHandle,
    WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, RowUuid, SchemaVersionId};
use jazz::model::transaction::OpenTransactionId;
use jazz::protocol::{BranchSelector, BranchViewBase};
use jazz::records::Value;
use jazz::thin::{ThinBody, ThinOp, ThinRequest, ThinResponse};
use jazz::wire::{WireErrorCode, WireFrame, WireRetry, encode_frame};
use tokio::sync::mpsc as tokio_mpsc;

use super::{Outbound, Owner, SESSION_QUEUE_BUDGET, ServerSession};

/// A transaction a thin session began and has not ended.
struct ThinTransaction {
    serializable: bool,
    /// The first staged write refused for its row's state: the commit
    /// reports it instead of committing.
    refused: Option<Error>,
}

/// A large value a thin session is uploading in chunks.
struct ThinStream {
    upload: StreamingValueUpload,
    mutation: StreamingMutationKind,
    table: String,
    row: RowUuid,
    cells: BTreeMap<String, Value>,
    column: String,
    updated_at_ms: Option<u64>,
    head: Option<BranchSelector>,
    base: Option<BranchViewBase>,
    transaction: Option<OpenTransactionId>,
}

pub(super) struct ThinSession {
    identity: AuthorSubject,
    /// The identity's claims, bound to every read; `None` for SYSTEM.
    claims: Option<BTreeMap<String, Value>>,
    /// A session under its own identity (not a trusted backend link).
    untrusted: bool,
    /// The published permissions bundle its backend pinned it to: its
    /// reads and writes are governed by that bundle's policies.
    bundle: Option<jazz::catalogue::BundleId>,
    /// The schema version the client declared.
    version: Option<SchemaVersionId>,
    /// Live queries, by the id of the request that opened them.
    subscriptions: BTreeMap<u64, SubscriptionStream>,
    /// Open transactions; they end with the session.
    transactions: BTreeMap<OpenTransactionId, ThinTransaction>,
    /// Large-value uploads, by the id of the request that began them.
    streams: BTreeMap<u64, ThinStream>,
    /// Answers produced while the session's egress budget is spent.
    held: Vec<ThinResponse>,
    /// The live query the next pump drains first, so one busy query
    /// cannot keep the others' events waiting.
    next_subscription: u64,
    output: tokio_mpsc::UnboundedSender<Outbound>,
    egress: Arc<AtomicUsize>,
}

impl ThinSession {
    pub(super) fn new(
        identity: AuthorSubject,
        claims: Option<BTreeMap<String, Value>>,
        bundle: Option<jazz::catalogue::BundleId>,
        output: tokio_mpsc::UnboundedSender<Outbound>,
        egress: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            identity,
            claims,
            untrusted: identity != AuthorSubject::SYSTEM,
            bundle,
            version: None,
            subscriptions: BTreeMap::new(),
            transactions: BTreeMap::new(),
            streams: BTreeMap::new(),
            held: Vec::new(),
            next_subscription: 0,
            output,
            egress,
        }
    }

    fn has_room(&self) -> bool {
        self.egress.load(Ordering::Acquire) < SESSION_QUEUE_BUDGET
    }

    /// Send `responses` as one batch. False once the socket side is gone.
    fn send(&self, responses: Vec<ThinResponse>) -> bool {
        let mut frames = Vec::with_capacity(responses.len());
        for response in responses {
            match self.encode(response) {
                Some(frame) => frames.push(frame),
                None => return false,
            }
        }
        self.send_frames(frames)
    }

    /// `response` on the wire; `None` once the session was closed because
    /// it could not be encoded.
    fn encode(&self, response: ThinResponse) -> Option<Vec<u8>> {
        encode_response(&self.output, response)
    }

    /// Send encoded `frames` as one batch, counted against the egress
    /// budget. False once the socket side is gone.
    fn send_frames(&self, frames: Vec<Vec<u8>>) -> bool {
        if frames.is_empty() {
            return true;
        }
        let bytes = frames.iter().map(Vec::len).sum::<usize>();
        self.egress.fetch_add(bytes, Ordering::AcqRel);
        self.output.send((Ok(frames), bytes)).is_ok()
    }

    /// End what the session left open: its transactions roll back, its
    /// uploads are dropped.
    fn release(self, db: &Db) {
        for id in self.transactions.into_keys() {
            let _ = db.abandon_transaction_handle(id);
        }
        for stream in self.streams.into_values() {
            let _ = block_on(db.abort_streaming_value_upload(stream.upload));
        }
    }
}

fn error_body(error: &Error) -> ThinBody {
    ThinBody::Error {
        code: format!("{:?}", error.code),
        message: error.message.clone(),
    }
}

fn argument(message: impl Into<String>) -> ThinBody {
    ThinBody::Error {
        code: "Argument".to_owned(),
        message: message.into(),
    }
}

fn host_error(error: jazz::binding_host::HostError) -> ThinBody {
    match error {
        jazz::binding_host::HostError::Engine(error) => error_body(&error),
        jazz::binding_host::HostError::Argument(message) => argument(message),
    }
}

/// Options as the bindings take them; empty bytes are no options.
fn options_json(options: &[u8]) -> Result<serde_json::Value, ThinBody> {
    if options.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_slice(options).map_err(|error| argument(format!("invalid options: {error}")))
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

fn written(write: Result<WriteHandle, Error>) -> ThinBody {
    match write {
        Ok(write) => ThinBody::Written {
            row: *write.row_uuid().as_bytes(),
            seq: write.mergeable_tx_id(),
        },
        Err(error) => error_body(&error),
    }
}

/// A staged write's answer. A refusal decided by the row's stored state, or
/// a serializable conflict, does not fail the call: the transaction stays
/// open and its commit reports the first one (as for the bindings).
fn staged(
    transaction: &mut ThinTransaction,
    row: RowUuid,
    write: Result<WriteHandle, Error>,
) -> ThinBody {
    match write {
        Ok(_) => ThinBody::Staged {
            row: *row.as_bytes(),
        },
        Err(error)
            if error.is_apply_time_rejection() || error.code == ErrorCode::TransactionConflict =>
        {
            transaction.refused.get_or_insert(error);
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

impl Owner {
    pub(super) fn open_thin_session(
        &mut self,
        session: ServerSession,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        bundle: Option<jazz::catalogue::BundleId>,
        output: tokio_mpsc::UnboundedSender<Outbound>,
        egress: Arc<AtomicUsize>,
    ) -> Result<(), jazz::db::Error> {
        // Admitted on the same terms as a sync session: a pinned bundle must
        // resolve here, and a createJazz app serves only pinned sessions.
        self.db.check_session_pin(identity, bundle.as_ref())?;
        let claims = (identity != AuthorSubject::SYSTEM).then(|| {
            self.db.set_identity_claims(identity, claims.clone());
            claims
        });
        self.thin.insert(
            session,
            ThinSession::new(identity, claims, bundle, output, egress),
        );
        Ok(())
    }

    /// End every thin session pinned to a bundle `ends` names, telling it
    /// `error` first. How many ended.
    pub(super) fn end_pinned_thin_sessions(
        &mut self,
        ends: &dyn Fn(&jazz::catalogue::BundleId) -> bool,
        error: &jazz::wire::WireError,
    ) -> usize {
        let ending: Vec<ServerSession> = self
            .thin
            .iter()
            .filter(|(_, thin)| thin.bundle.as_ref().is_some_and(ends))
            .map(|(id, _)| *id)
            .collect();
        for id in &ending {
            let _ = self.thin[id].output.send((Err(error.clone()), 0));
            self.close_thin_session(*id);
        }
        ending.len()
    }

    pub(super) fn close_thin_session(&mut self, session: ServerSession) {
        // Dropping the streams ends the subscriptions.
        if let Some(thin) = self.thin.remove(&session) {
            thin.release(&self.db);
        }
    }

    pub(super) fn thin_request(&mut self, session: ServerSession, request: ThinRequest) {
        let Some(thin) = self.thin.get(&session) else {
            return;
        };
        // A pinned session's bundle is published already: it does not wait
        // for the current permissions.
        let body = if self.awaiting_permissions && thin.untrusted && thin.bundle.is_none() {
            ThinBody::Error {
                code: "NotReady".to_owned(),
                message: "no permissions are published for this app yet; deploy them before \
                          connecting thin clients"
                    .to_owned(),
            }
        } else {
            match self.serve_thin(session, request.id, request.op) {
                Some(body) => body,
                None => return,
            }
        };
        let response = ThinResponse {
            id: request.id,
            body,
        };
        let thin = self.thin.get_mut(&session).expect("checked above");
        if thin.has_room() && thin.held.is_empty() {
            if !thin.send(vec![response]) {
                self.close_thin_session(session);
            }
        } else {
            thin.held.push(response);
        }
    }

    /// Serve one request; `None` when it has no immediate answer (a live
    /// query answers from [`Owner::pump_thin`]).
    fn serve_thin(&mut self, session: ServerSession, id: u64, op: ThinOp) -> Option<ThinBody> {
        let thin = self.thin.get_mut(&session)?;
        if let ThinOp::Schema { schema } = op {
            // The version a replica built against this schema would declare.
            let version = match decode_schema(&schema) {
                Ok(schema) => schema.version_id(),
                Err(error) => return Some(host_error(error)),
            };
            return Some(match self.db.pinned_view(version, thin.bundle.as_ref()) {
                Err(error) => error_body(&error),
                Ok(Some(_)) => {
                    thin.version = Some(version);
                    ThinBody::Ok
                }
                Ok(None) => ThinBody::Error {
                    code: "SchemaNotDeployed".to_owned(),
                    message: "the client's schema version is not deployed to this server"
                        .to_owned(),
                },
            });
        }
        if let ThinOp::Unsubscribe { subscription } = op {
            let removed = thin.subscriptions.remove(&subscription).is_some();
            // The live query's own id hears that it ended.
            if removed && subscription != id {
                thin.held.push(ThinResponse {
                    id: subscription,
                    body: ThinBody::Closed,
                });
            }
            return Some(ThinBody::Ok);
        }
        let Some(version) = thin.version else {
            return Some(argument("a thin session declares its schema version first"));
        };
        // Resolved per request: an activation changes the policies that
        // govern the declared version (a pinned bundle's never change).
        let db = match self.db.pinned_view(version, thin.bundle.as_ref()) {
            Ok(db) => db,
            Err(error) => return Some(error_body(&error)),
        };
        let Some(db) = db else {
            return Some(ThinBody::Error {
                code: "SchemaNotDeployed".to_owned(),
                message: "the client's schema version is no longer deployed".to_owned(),
            });
        };
        match serve_thin_op(&db, thin, id, op) {
            Ok(body) => body,
            Err(body) => Some(body),
        }
    }

    /// After a tick: every thin session's held answers and its live
    /// queries' new events, up to its egress budget. What does not fit
    /// waits (answers in `held`, events in their streams). A drain only
    /// stops early once the encoded batch fills the budget, so the egress
    /// it leaves is over budget and the client draining its socket below
    /// it queues the next tick ([`super::ServerRuntimeFrameStream::recv`]).
    pub(super) fn pump_thin(&mut self) {
        let mut gone = Vec::new();
        for (session, thin) in &mut self.thin {
            if !thin.has_room() {
                continue;
            }
            let sent = drain_thin(thin).is_some_and(|frames| thin.send_frames(frames));
            if !sent {
                gone.push(*session);
            }
        }
        for session in gone {
            self.close_thin_session(session);
        }
    }
}

/// `thin`'s held answers, then its live queries' events in turn from
/// [`ThinSession::next_subscription`], encoded until the batch fills the
/// session's egress budget. Answers that do not fit stay held. `None` once
/// the session was closed over a response it could not encode.
fn drain_thin(thin: &mut ThinSession) -> Option<Vec<Vec<u8>>> {
    let room = SESSION_QUEUE_BUDGET.saturating_sub(thin.egress.load(Ordering::Acquire));
    let mut frames = Vec::new();
    let mut bytes = 0;
    let mut held = std::mem::take(&mut thin.held).into_iter();
    for response in held.by_ref() {
        let frame = thin.encode(response)?;
        bytes += frame.len();
        frames.push(frame);
        if bytes >= room {
            break;
        }
    }
    thin.held = held.collect();
    if !thin.held.is_empty() {
        return Some(frames);
    }
    let cursor = thin.next_subscription;
    let order: Vec<u64> = thin
        .subscriptions
        .range(cursor..)
        .chain(thin.subscriptions.range(..cursor))
        .map(|(id, _)| *id)
        .collect();
    let mut ended = Vec::new();
    'streams: for id in order {
        let stream = thin.subscriptions.get_mut(&id).expect("listed above");
        while bytes < room {
            let Some(event) = stream.try_next_event() else {
                continue 'streams;
            };
            let (body, last) = event_body(event);
            if last {
                ended.push(id);
            }
            let frame = encode_response(&thin.output, ThinResponse { id, body })?;
            bytes += frame.len();
            frames.push(frame);
            if last {
                continue 'streams;
            }
        }
        // Full: the next drain starts after this query.
        thin.next_subscription = id + 1;
        break;
    }
    for id in ended {
        thin.subscriptions.remove(&id);
    }
    Some(frames)
}

/// `response` on the wire. On failure the session is told it closed, and
/// `None` is returned.
fn encode_response(
    output: &tokio_mpsc::UnboundedSender<Outbound>,
    response: ThinResponse,
) -> Option<Vec<u8>> {
    match encode_frame(&WireFrame::ThinResponse(response)) {
        Ok(frame) => Some(frame),
        Err(error) => {
            tracing::error!(%error, "encode thin response");
            let _ = output.send((
                Err(jazz::wire::WireError::new(
                    WireErrorCode::Internal,
                    WireRetry::Never,
                    format!("thin session closed: {error}"),
                )),
                0,
            ));
            None
        }
    }
}

/// A live query's event as a response body, and whether it ends the query.
fn event_body(event: SubscriptionEvent) -> (ThinBody, bool) {
    match event_chunk(event) {
        Ok(EventChunk::Delta {
            delta,
            reset,
            settled,
            tier,
        }) => (
            ThinBody::Event {
                delta,
                reset,
                settled,
                tier,
            },
            false,
        ),
        Ok(EventChunk::Closed) => (ThinBody::Closed, true),
        Ok(EventChunk::Rejected { detail }) => (
            ThinBody::Error {
                code: "Rejected".to_owned(),
                message: detail,
            },
            true,
        ),
        Err(error) => (argument(format!("{error:?}")), true),
    }
}

/// The open transaction `transaction` names, if this session began it.
fn session_transaction<'a>(
    thin: &'a mut ThinSession,
    transaction: &str,
) -> Result<(OpenTransactionId, &'a mut ThinTransaction), ThinBody> {
    let id = open_transaction(transaction).map_err(host_error)?;
    match thin.transactions.get_mut(&id) {
        Some(open) => Ok((id, open)),
        None => Err(argument(format!(
            "transaction {transaction} is not open in this session"
        ))),
    }
}

/// One request against the session's governed view; `Ok(None)` when a live
/// query answers later.
fn serve_thin_op(
    db: &Db,
    thin: &mut ThinSession,
    id: u64,
    op: ThinOp,
) -> Result<Option<ThinBody>, ThinBody> {
    let identity = thin.identity;
    let scope = thin.claims.clone().map(|claims| (identity, claims));
    let body = match op {
        ThinOp::Schema { .. } | ThinOp::Unsubscribe { .. } => unreachable!("served by the owner"),
        ThinOp::Subscribe { query, options } => {
            if thin.subscriptions.contains_key(&id) {
                return Err(argument("a live query with this id is already open"));
            }
            let opts = read_opts(&options_json(&options)?).map_err(host_error)?;
            let query: jazz::query::Query = postcard::from_bytes(&query)
                .map_err(|error| argument(format!("undecodable query: {error}")))?;
            let opened = db.prepare_query(&query).and_then(|mut prepared| {
                if let Some((author, claims)) = scope {
                    prepared = prepared.with_identity_claims(author, claims);
                }
                block_on(db.subscribe_for_identity(&prepared, opts, identity))
            });
            match opened {
                Ok(stream) => {
                    thin.subscriptions.insert(id, stream);
                    return Ok(None);
                }
                Err(error) => error_body(&error),
            }
        }
        ThinOp::Query {
            query,
            options,
            transaction,
        } => {
            let opts = read_opts(&options_json(&options)?).map_err(host_error)?;
            let open = match transaction {
                Some(transaction) => Some(session_transaction(thin, &transaction)?.0),
                None => None,
            };
            // The authority holds everything a read can ask for: it never
            // waits.
            let read = block_on(db.all_serialized_query(&query, opts, open, scope, None));
            let encoded = match read {
                Ok(SerializedReadResult::Rows(rows)) => {
                    binding_codec::encode_rows(&rows).map(|rows| (rows, false))
                }
                Ok(SerializedReadResult::Relation(snapshot)) => {
                    binding_codec::encode_relation_snapshot(&snapshot).map(|rows| (rows, true))
                }
                Err(error) => return Err(error_body(&error)),
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
            transaction,
        } => {
            let cells = decode_cells(&cells).map_err(host_error)?;
            let mut options = insert_options(&options_json(&options)?).map_err(host_error)?;
            as_session(&mut options.identity, identity)?;
            match transaction {
                Some(transaction) => {
                    let (open, state) = session_transaction(thin, &transaction)?;
                    let row = *options.row_id.get_or_insert_with(|| db.next_row_id());
                    staged(
                        state,
                        row,
                        block_on(db.tx_ref(open).insert(&table, cells, options)),
                    )
                }
                None => written(block_on(db.insert(&table, cells, options))),
            }
        }
        ThinOp::Update {
            table,
            row,
            patch,
            options,
            transaction,
        } => {
            let row = RowUuid::from_bytes(row);
            let patch = decode_cells(&patch).map_err(host_error)?;
            let mut options = update_options(&options_json(&options)?).map_err(host_error)?;
            as_session(&mut options.identity, identity)?;
            match transaction {
                Some(transaction) => {
                    let (open, state) = session_transaction(thin, &transaction)?;
                    staged(
                        state,
                        row,
                        block_on(db.tx_ref(open).update(&table, row, patch, options)),
                    )
                }
                None => written(block_on(db.update(&table, row, patch, options))),
            }
        }
        ThinOp::Upsert {
            table,
            row,
            cells,
            options,
            transaction,
        } => {
            let row = RowUuid::from_bytes(row);
            let mut options = upsert_options(&options_json(&options)?).map_err(host_error)?;
            let cells = decode_cells(&cells).map_err(host_error)?;
            as_session(&mut options.identity, identity)?;
            match transaction {
                Some(transaction) => {
                    let (open, state) = session_transaction(thin, &transaction)?;
                    staged(
                        state,
                        row,
                        block_on(db.tx_ref(open).upsert(&table, row, cells, options)),
                    )
                }
                None => written(block_on(db.upsert(&table, row, cells, options))),
            }
        }
        ThinOp::Delete {
            table,
            row,
            options,
            transaction,
        } => {
            let row = RowUuid::from_bytes(row);
            let mut options = delete_options(&options_json(&options)?).map_err(host_error)?;
            as_session(&mut options.identity, identity)?;
            match transaction {
                Some(transaction) => {
                    let (open, state) = session_transaction(thin, &transaction)?;
                    staged(
                        state,
                        row,
                        block_on(db.tx_ref(open).delete(&table, row, options)),
                    )
                }
                None => written(block_on(db.delete(&table, row, options))),
            }
        }
        ThinOp::Restore {
            table,
            row,
            cells,
            options,
            transaction,
        } => {
            let row = RowUuid::from_bytes(row);
            let cells = cells
                .map(|cells| decode_cells(&cells))
                .transpose()
                .map_err(host_error)?;
            let mut options = restore_options(&options_json(&options)?).map_err(host_error)?;
            as_session(&mut options.identity, identity)?;
            match transaction {
                Some(transaction) => {
                    let (open, state) = session_transaction(thin, &transaction)?;
                    staged(
                        state,
                        row,
                        block_on(db.tx_ref(open).restore(&table, row, cells, options)),
                    )
                }
                None => written(block_on(db.restore(&table, row, cells, options))),
            }
        }
        ThinOp::UpdateLargeValues {
            table,
            row,
            patch,
            mutations,
            updated_at_ms,
            transaction,
        } => {
            let row = RowUuid::from_bytes(row);
            let patch = decode_cells(&patch).map_err(host_error)?;
            let mutations: Vec<LargeValueUpdate> =
                serde_json::from_slice(&mutations).map_err(|error| {
                    argument(format!("invalid partial-value update descriptor: {error}"))
                })?;
            let options = UpdateOptions {
                identity: WriteIdentity::Session(identity),
                updated_at_ms,
                ..UpdateOptions::default()
            };
            match transaction {
                Some(transaction) => {
                    let (open, state) = session_transaction(thin, &transaction)?;
                    staged(
                        state,
                        row,
                        block_on(db.tx_ref(open).update_with_large_value_mutations(
                            &table, row, patch, mutations, options,
                        )),
                    )
                }
                None => written(block_on(db.update_with_large_value_mutations(
                    &table, row, patch, mutations, options,
                ))),
            }
        }
        ThinOp::Begin { transaction, kind } => {
            let open = open_transaction(&transaction).map_err(host_error)?;
            let kind = TransactionKind::parse(&kind).map_err(host_error)?;
            let serializable = matches!(kind, TransactionKind::Serializable);
            let begun = if serializable {
                block_on(db.begin_serializable(open))
            } else {
                block_on(db.begin_mergeable(open))
            };
            match begun {
                Ok(()) => {
                    thin.transactions.insert(
                        open,
                        ThinTransaction {
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
            let open = session_transaction(thin, &transaction)?.0;
            let state = thin.transactions.remove(&open).expect("checked above");
            if let Some(error) = state.refused {
                let _ = db.abandon_transaction_handle(open);
                return Err(error_body(&error));
            }
            let receipt = if state.serializable {
                block_on(db.commit_serializable_handle(open))
            } else {
                block_on(db.commit_mergeable_handle(open))
            };
            match receipt {
                Ok(receipt) => ThinBody::Written {
                    row: [0; 16],
                    seq: receipt.0,
                },
                Err(error) => error_body(&error),
            }
        }
        ThinOp::Rollback { transaction } => {
            let open = session_transaction(thin, &transaction)?.0;
            thin.transactions.remove(&open);
            match db.abandon_transaction_handle(open) {
                Ok(()) => ThinBody::Ok,
                Err(error) => error_body(&error),
            }
        }
        ThinOp::CaptureSnapshot => match block_on(db.capture_snapshot()) {
            Ok(snapshot) => ThinBody::Snapshot {
                snapshot: snapshot.to_persisted().into_bytes(),
            },
            Err(error) => error_body(&error),
        },
        ThinOp::BeginStream {
            table,
            row,
            cells,
            column,
            mutation,
            options,
            transaction,
        } => {
            if thin.streams.contains_key(&id) {
                return Err(argument("an upload with this id is already open"));
            }
            let transaction = match transaction {
                Some(transaction) => Some(session_transaction(thin, &transaction)?.0),
                None => None,
            };
            let mutation = streaming_mutation(&mutation)?;
            let (updated_at_ms, head, base) = stream_options(&options_json(&options)?)?;
            let cells = decode_cells(&cells).map_err(host_error)?;
            match db.begin_streaming_value_upload(&table, &cells, &column) {
                Ok(upload) => {
                    thin.streams.insert(
                        id,
                        ThinStream {
                            upload,
                            mutation,
                            table,
                            row: RowUuid::from_bytes(row),
                            cells,
                            column,
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
            let Some(open) = thin.streams.get_mut(&stream) else {
                return Err(argument("no upload with this id is open"));
            };
            match block_on(db.push_streaming_value_upload(&mut open.upload, &chunk)) {
                Ok(()) => ThinBody::Ok,
                Err(error) => {
                    // A failed push ends the upload.
                    let open = thin.streams.remove(&stream).expect("checked above");
                    let _ = block_on(db.abort_streaming_value_upload(open.upload));
                    error_body(&error)
                }
            }
        }
        ThinOp::FinishStream { stream } => {
            let Some(open) = thin.streams.remove(&stream) else {
                return Err(argument("no upload with this id is open"));
            };
            if let Some(transaction) = open.transaction {
                let Some(state) = thin.transactions.get_mut(&transaction) else {
                    return Err(argument("the upload's transaction is no longer open"));
                };
                let write = block_on(db.tx_ref(transaction).finish_streaming_value_upload(
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
                ));
                return Ok(Some(staged(state, open.row, write)));
            }
            written(block_on(db.finish_streaming_value_upload(
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
            )))
        }
        ThinOp::AbortStream { stream } => {
            let Some(open) = thin.streams.remove(&stream) else {
                return Err(argument("no upload with this id is open"));
            };
            match block_on(db.abort_streaming_value_upload(open.upload)) {
                Ok(()) => ThinBody::Ok,
                Err(error) => error_body(&error),
            }
        }
    };
    Ok(Some(body))
}
